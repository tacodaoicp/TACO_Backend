import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";

import CanisterIds "../helper/CanisterIds";

shared (deployer) persistent actor class Rewards() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Types for our rewards calculations
  public type PriceType = {
    #ICP;
    #USD;
  };

  // Detailed checkpoint data for each calculation point
  public type CheckpointData = {
    timestamp: Int;
    allocations: [Allocation]; // Active allocations at this point
    tokenValues: [(Principal, Float)]; // Per-token values (allocation % × token price)
    totalPortfolioValue: Float; // Sum of all token values
    pricesUsed: [(Principal, PriceInfo)]; // Prices used for this calculation
  };

  public type PerformanceResult = {
    neuronId: Blob;
    startTime: Int;
    endTime: Int;
    initialValue: Float; // Always 1.0 (first checkpoint)
    finalValue: Float;   // Final portfolio value (last checkpoint)
    performanceScore: Float; // finalValue / initialValue
    allocationChanges: Nat; // Number of rebalances in the period
    checkpoints: [CheckpointData]; // Detailed data for each calculation point
    preTimespanAllocation: ?NeuronAllocationChangeBlockData; // Most recent allocation before timespan
    inTimespanChanges: [NeuronAllocationChangeBlockData]; // All changes within timespan
  };

  public type RewardsError = {
    #NeuronNotFound;
    #InvalidTimeRange;
    #PriceDataMissing: {token: Principal; timestamp: Int};
    #AllocationDataMissing;
    #SystemError: Text;
  };

  // External canister interfaces
  private transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  
  private transient let DAO_NEURON_ALLOCATION_ARCHIVE_ID = canister_ids.getCanisterId(#dao_neuron_allocation_archive);
  private transient let PRICE_ARCHIVE_ID = canister_ids.getCanisterId(#price_archive);

  // Import the external canister types (we'll need to define these)
  type NeuronAllocationChangeBlockData = {
    neuronId: Blob;
    newAllocations: [Allocation];
    oldAllocations: [Allocation];
    timestamp: Int;
    changeType: AllocationChangeType;
    id: Nat;
    maker: Principal;
    reason: ?Text;
    votingPower: Nat;
  };

  type Allocation = {
    token: Principal;
    basisPoints: Nat; // Out of 10,000 (100%)
  };

  type AllocationChangeType = {
    #FollowAction: {followedUser: Principal};
    #UserUpdate: {userInitiated: Bool};
    #SystemRebalance;
    #VotingPowerChange;
  };

  type ArchiveError = {
    #BlockNotFound;
    #InvalidBlockType;
    #InvalidData;
    #InvalidTimeRange;
    #NotAuthorized;
    #StorageFull;
    #SystemError: Text;
  };

  type PriceInfo = {
    icpPrice: Nat;
    usdPrice: Float;
    timestamp: Int;
  };

  // External canister interfaces
  type NeuronAllocationArchive = actor {
    getNeuronAllocationChangesByNeuronInTimeRange: (Blob, Int, Int, Nat) -> async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError>;
    getNeuronAllocationChangesWithContext: (Blob, Int, Int, Nat) -> async Result.Result<{
      preTimespanAllocation: ?NeuronAllocationChangeBlockData;
      inTimespanChanges: [NeuronAllocationChangeBlockData];
    }, ArchiveError>;
  };

  type PriceArchive = actor {
    getPriceAtTime: (Principal, Int) -> async Result.Result<?PriceInfo, ArchiveError>;
  };

  private transient let neuronAllocationArchive : NeuronAllocationArchive = actor (Principal.toText(DAO_NEURON_ALLOCATION_ARCHIVE_ID));
  private transient let priceArchive : PriceArchive = actor (Principal.toText(PRICE_ARCHIVE_ID));

  //=========================================================================
  // Main Query Method
  //=========================================================================

  public shared func calculateNeuronPerformance(
    neuronId: Blob,
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async Result.Result<PerformanceResult, RewardsError> {
    
    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };

    // Get allocation data from the neuron allocation archive
    let allocationResult = await neuronAllocationArchive.getNeuronAllocationChangesWithContext(
      neuronId, startTime, endTime, 100
    );

    let allocationData = switch (allocationResult) {
      case (#ok(data)) { data };
      case (#err(error)) {
        return #err(#SystemError("Failed to get allocation data: " # debug_show(error)));
      };
    };

    // Determine the active allocation at start time
    let startAllocation = switch (allocationData.preTimespanAllocation) {
      case (?preAlloc) { preAlloc.newAllocations };
      case (null) {
        // Check if there's a change exactly at start time
        switch (Array.find(allocationData.inTimespanChanges, func(change: NeuronAllocationChangeBlockData) : Bool {
          change.timestamp == startTime
        })) {
          case (?exactChange) { exactChange.oldAllocations };
          case (null) {
            // No allocation data found
            return #err(#NeuronNotFound);
          };
        };
      };
    };

    // Build timeline of allocation changes
    let timelineBuffer = Buffer.Buffer<(Int, [Allocation])>(10);
    timelineBuffer.add((startTime, startAllocation));
    
    // Add all in-timespan changes
    for (change in allocationData.inTimespanChanges.vals()) {
      timelineBuffer.add((change.timestamp, change.newAllocations));
    };
    
    // Add end time if it's different from the last change
    let lastChangeTime = if (timelineBuffer.size() == 0) { 
      startTime 
    } else { 
      timelineBuffer.get(timelineBuffer.size() - 1).0 
    };
    if (lastChangeTime != endTime) {
      let endAllocations = if (timelineBuffer.size() == 0) { 
        startAllocation 
      } else { 
        timelineBuffer.get(timelineBuffer.size() - 1).1 
      };
      timelineBuffer.add((endTime, endAllocations));
    };

    let timeline = Buffer.toArray(timelineBuffer);

    // Calculate checkpoints for each point in timeline
    let checkpointsBuffer = Buffer.Buffer<CheckpointData>(timeline.size());
    var currentValue : Float = 1.0;

    for (i in timeline.keys()) {
      let (timestamp, allocations) = timeline[i];
      
      // Get prices for all tokens at this timestamp
      let tokenPricesBuffer = Buffer.Buffer<(Principal, PriceInfo)>(allocations.size());
      let tokenValuesBuffer = Buffer.Buffer<(Principal, Float)>(allocations.size());
      var totalValue : Float = 0.0;

      for (allocation in allocations.vals()) {
        let priceResult = await priceArchive.getPriceAtTime(allocation.token, timestamp);
        switch (priceResult) {
          case (#ok(?priceInfo)) {
            let tokenPrice = getPriceValue(priceInfo, priceType);
            let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
            let tokenValue = currentValue * allocationPercent * tokenPrice;
            
            tokenPricesBuffer.add((allocation.token, priceInfo));
            tokenValuesBuffer.add((allocation.token, tokenValue));
            totalValue += tokenValue;
          };
          case (#ok(null)) {
            return #err(#PriceDataMissing({token = allocation.token; timestamp = timestamp}));
          };
          case (#err(_)) {
            return #err(#PriceDataMissing({token = allocation.token; timestamp = timestamp}));
          };
        };
      };

      // Create checkpoint
      let checkpoint : CheckpointData = {
        timestamp = timestamp;
        allocations = allocations;
        tokenValues = Buffer.toArray(tokenValuesBuffer);
        totalPortfolioValue = totalValue;
        pricesUsed = Buffer.toArray(tokenPricesBuffer);
      };
      
      checkpointsBuffer.add(checkpoint);
      
      // Update current value for next iteration (except for the last checkpoint)
      let isLastCheckpoint = (Int.abs(i) + 1 == Int.abs(timeline.size()));
      if (not isLastCheckpoint) {
        currentValue := totalValue;
      };
    };

    let checkpoints = Buffer.toArray(checkpointsBuffer);

    // Calculate final results
    let finalValue = if (checkpoints.size() == 0) { 
      1.0 
    } else { 
      checkpoints[checkpoints.size() - 1].totalPortfolioValue 
    };

    #ok({
      neuronId = neuronId;
      startTime = startTime;
      endTime = endTime;
      initialValue = 1.0;
      finalValue = finalValue;
      performanceScore = finalValue / 1.0;
      allocationChanges = Array.size(allocationData.inTimespanChanges);
      checkpoints = checkpoints;
      preTimespanAllocation = allocationData.preTimespanAllocation;
      inTimespanChanges = allocationData.inTimespanChanges;
    });
  };

  //=========================================================================
  // Helper Functions (for when we implement the full logic)
  //=========================================================================

  // Convert allocation basis points to percentage (0.0 to 1.0)
  private func basisPointsToPercentage(basisPoints: Nat) : Float {
    Float.fromInt(basisPoints) / 10000.0;
  };

  // Get price value based on price type
  private func getPriceValue(priceInfo: PriceInfo, priceType: PriceType) : Float {
    switch (priceType) {
      case (#ICP) { Float.fromInt(priceInfo.icpPrice) / 100000000.0 }; // Convert from e8s
      case (#USD) { priceInfo.usdPrice };
    };
  };

  // Calculate portfolio value given allocations and prices
  private func calculatePortfolioValue(
    allocations: [Allocation], 
    prices: [(Principal, PriceInfo)],
    priceType: PriceType,
    baseValue: Float
  ) : Float {
    var totalValue : Float = 0.0;
    
    for (allocation in allocations.vals()) {
      let percentage = basisPointsToPercentage(allocation.basisPoints);
      let tokenValue = baseValue * percentage;
      
      // Find the price for this token
      var priceMultiplier : Float = 1.0;
      for ((token, priceInfo) in prices.vals()) {
        if (Principal.equal(token, allocation.token)) {
          // This would need to be adjusted based on how we handle price changes
          // For now, we assume prices are relative to initial prices
          priceMultiplier := getPriceValue(priceInfo, priceType);
        };
      };
      
      totalValue += tokenValue * priceMultiplier;
    };
    
    totalValue;
  };

  //=========================================================================
  // Admin Functions
  //=========================================================================

  public shared ({ caller }) func getCanisterStatus() : async {
    neuronAllocationArchiveId: Principal;
    priceArchiveId: Principal;
    environment: Text;
  } {
    {
      neuronAllocationArchiveId = DAO_NEURON_ALLOCATION_ARCHIVE_ID;
      priceArchiveId = PRICE_ARCHIVE_ID;
      environment = switch (canister_ids.getEnvironment()) {
        case (#Staging) { "staging" };
        case (#Production) { "production" };
      };
    };
  };
}
