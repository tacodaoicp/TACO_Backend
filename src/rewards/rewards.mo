import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Float "mo:base/Float";
import Int "mo:base/Int";
import _Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Timer "mo:base/Timer";
import Map "mo:map/Map";
import Vector "mo:vector";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Text "mo:base/Text";
import Error "mo:base/Error";

import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import ICRC "../helper/icrc.types";
import NeuronSnapshot "../neuron_snapshot/ns_types";

shared (deployer) persistent actor class Rewards() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Logger instance
  private transient var logger = Logger.Logger();

  // TACO token constants
  private let TACO_DECIMALS : Nat = 8;
  private let TACO_SATOSHIS_PER_TOKEN : Nat = 100_000_000; // 10^8
  private let TACO_LEDGER_CANISTER_ID : Text = "kknbx-zyaaa-aaaaq-aae4a-cai";
  private let TACO_WITHDRAWAL_FEE : Nat = 10_000; // 0.0001 TACO in satoshis

  private let SNS_GOVERNANCE_CANISTER_ID : Principal = Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai");

  // Helper functions for TACO amount conversions
  private func tacoTokensToSatoshis(tokens: Nat) : Nat {
    tokens * TACO_SATOSHIS_PER_TOKEN
  };

  private func tacoSatoshisToTokens(satoshis: Nat) : Float {
    Float.fromInt(satoshis) / Float.fromInt(TACO_SATOSHIS_PER_TOKEN)
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
    maker: ?Principal; // The principal responsible for the allocation at this checkpoint
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
    #DistributionInProgress;
    #InsufficientRewardPot;
    #NotAuthorized;
  };

  // Periodic distribution types
  public type NeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Nat; // Reward amount in TACO satoshis (integer)
    checkpoints: [CheckpointData]; // Include checkpoints to access maker information
  };

  public type FailedNeuron = {
    neuronId: Blob;
    errorMessage: Text;
  };

  public type DistributionStatus = {
    #InProgress: {currentNeuron: Nat; totalNeurons: Nat};
    #Completed;
    #Failed: Text;
    #PartiallyCompleted: {successfulNeurons: Nat; failedNeurons: Nat};
  };

  public type DistributionRecord = {
    id: Nat;
    startTime: Int;
    endTime: Int;
    distributionTime: Int;
    totalRewardPot: Nat; // Reward pot in whole TACO tokens
    actualDistributed: Nat; // Actual amount distributed in TACO satoshis
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [NeuronReward];
    failedNeurons: [FailedNeuron];
    status: DistributionStatus;
  };



  public type DistributionError = {
    #SystemError: Text;
    #InProgress;
    #NotAuthorized;
  };

  // Withdrawal types
  public type WithdrawalRecord = {
    id: Nat;
    caller: Principal;
    neuronWithdrawals: [(Blob, Nat)]; // Array of (neuronId, amount withdrawn from it)
    totalAmount: Nat; // Total from all neurons (including fee)
    amountSent: Nat; // Amount actually sent (total - fee)
    fee: Nat; // Fee deducted
    targetAccount: ICRC.Account;
    timestamp: Int;
    transactionId: ?Nat; // ICRC1 transaction ID if successful
  };

  // Configuration
  stable var distributionPeriodNS : Nat = 604_800_000_000_000; // 7 days in nanoseconds
  stable var periodicRewardPot : Nat = 1000; // Default reward pot in whole TACO tokens
  stable var maxDistributionHistory : Nat = 52; // Keep 1 year of periodic distributions
  stable var distributionEnabled : Bool = true;
  stable var performanceScorePower : Float = 1.0; // Power to raise performance scores to (0 = no effect, 1 = linear, 2 = quadratic, etc.)
  stable var votingPowerPower : Float = 1.0; // Power to raise voting power to (0 = no effect, 1 = linear, 2 = quadratic, etc.)

  // Distribution state
  stable var distributionCounter : Nat = 0;
  stable var currentDistributionId : ?Nat = null;
  stable var lastDistributionTime : Int = 0;
  stable var nextScheduledDistributionTime : ?Int = null; // When the next distribution is scheduled to run
  private transient var distributionTimerId : ?Nat = null;
  
  // Reward tracking
  private transient let { phash; bhash } = Map;
  stable var neuronRewardBalances = Map.new<Blob, Nat>(); // neuronId -> accumulated rewards in TACO satoshis
  stable var totalDistributed : Nat = 0; // Total amount distributed to users in TACO satoshis (for balance validation)
  
  // Withdrawal tracking
  stable var totalWithdrawn : Nat = 0; // Total amount withdrawn by users in TACO satoshis
  stable var totalWithdrawals : Nat = 0; // Total number of withdrawal transactions
  stable var withdrawalCounter : Nat = 0; // Counter for withdrawal IDs
  
  // Distribution history (circular buffer using Vector)
  private stable let distributionHistory = Vector.new<DistributionRecord>();
  
  // Withdrawal history (circular buffer using Vector)
  private stable let withdrawalHistory = Vector.new<WithdrawalRecord>();

  // External canister interfaces
  private transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  
  private transient let DAO_NEURON_ALLOCATION_ARCHIVE_ID = canister_ids.getCanisterId(#dao_neuron_allocation_archive);
  private transient let PRICE_ARCHIVE_ID = canister_ids.getCanisterId(#price_archive);
  private transient let DAO_ID = canister_ids.getCanisterId(#DAO_backend);

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
    getPricesAtTime: ([Principal], Int) -> async Result.Result<[(Principal, ?PriceInfo)], ArchiveError>;
  };

  type NeuronAllocation = {
    allocations: [Allocation];
    lastUpdate: Int;
    votingPower: Nat;
    lastAllocationMaker: Principal;
  };

  type DAOCanister = actor {
    admin_getNeuronAllocations: () -> async [(Blob, NeuronAllocation)];
  };

  private transient let neuronAllocationArchive : NeuronAllocationArchive = actor (Principal.toText(DAO_NEURON_ALLOCATION_ARCHIVE_ID));
  private transient let priceArchive : PriceArchive = actor (Principal.toText(PRICE_ARCHIVE_ID));
  private transient let daoCanister : DAOCanister = actor (Principal.toText(DAO_ID));

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

    // Build timeline of allocation changes with makers
    let timelineBuffer = Buffer.Buffer<(Int, [Allocation], ?Principal)>(10);
    
    // For start time, get maker from preTimespanAllocation if available
    let startMaker = switch (allocationData.preTimespanAllocation) {
      case (?preAlloc) { ?preAlloc.maker };
      case (null) {
        // Check if there's a change exactly at start time
        switch (Array.find(allocationData.inTimespanChanges, func(change: NeuronAllocationChangeBlockData) : Bool {
          change.timestamp == startTime
        })) {
          case (?exactChange) { ?exactChange.maker };
          case (null) { null };
        };
      };
    };
    timelineBuffer.add((startTime, startAllocation, startMaker));
    
    // Add all in-timespan changes with their makers
    for (change in allocationData.inTimespanChanges.vals()) {
      timelineBuffer.add((change.timestamp, change.newAllocations, ?change.maker));
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
      // For end time, use the maker from the last change
      let endMaker = if (timelineBuffer.size() == 0) {
        startMaker
      } else {
        timelineBuffer.get(timelineBuffer.size() - 1).2
      };
      timelineBuffer.add((endTime, endAllocations, endMaker));
    };

    let timeline = Buffer.toArray(timelineBuffer);

    // Calculate checkpoints for each point in timeline
    let checkpointsBuffer = Buffer.Buffer<CheckpointData>(timeline.size());
    
    // Track asset values between checkpoints
    var assetValues = Buffer.Buffer<(Principal, Float)>(10); // (token, current_value)
    var previousPrices = Buffer.Buffer<(Principal, Float)>(10); // (token, price)
    
    for (i in timeline.keys()) {
      let (timestamp, allocations, maker) = timeline[i];
      
      // Get prices for all tokens at this timestamp
      let tokenPricesBuffer = Buffer.Buffer<(Principal, PriceInfo)>(allocations.size());
      let tokenValuesBuffer = Buffer.Buffer<(Principal, Float)>(allocations.size());
      
      if (i == 0) {
        // First checkpoint: Initialize with 1.0 total value, distributed by allocation
        var totalValue : Float = 1.0;
        
        // Collect all tokens for batch price request
        let tokens = Array.map<Allocation, Principal>(allocations, func(allocation) { allocation.token });
        
        // Get prices for all tokens at once
        let batchPriceResult = await priceArchive.getPricesAtTime(tokens, timestamp);
        let tokenPrices = switch (batchPriceResult) {
          case (#ok(prices)) { prices };
          case (#err(_)) {
            return #err(#SystemError("Failed to get price data"));
          };
        };
        
        // Process each allocation with its corresponding price
        for (allocation in allocations.vals()) {
          // Find the price for this token
          let priceEntry = Array.find<(Principal, ?PriceInfo)>(tokenPrices, func((token, _)) {
            Principal.equal(token, allocation.token)
          });
          
          switch (priceEntry) {
            case (?(_, ?priceInfo)) {
              let tokenPrice = getPriceValue(priceInfo, priceType);
              let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
              let tokenValue = totalValue * allocationPercent; // Just allocation percentage of 1.0
              
              tokenPricesBuffer.add((allocation.token, priceInfo));
              tokenValuesBuffer.add((allocation.token, tokenValue));
              
              // Store initial asset values and prices
              assetValues.add((allocation.token, tokenValue));
              previousPrices.add((allocation.token, tokenPrice));
            };
            case (?(_, null)) {
              return #err(#PriceDataMissing({token = allocation.token; timestamp = timestamp}));
            };
            case null {
              return #err(#PriceDataMissing({token = allocation.token; timestamp = timestamp}));
            };
          };
        };
        
        // Create first checkpoint
        let checkpoint : CheckpointData = {
          timestamp = timestamp;
          allocations = allocations;
          tokenValues = Buffer.toArray(tokenValuesBuffer);
          totalPortfolioValue = 1.0;
          pricesUsed = Buffer.toArray(tokenPricesBuffer);
          maker = maker;
        };
        checkpointsBuffer.add(checkpoint);
        
      } else {
        // Subsequent checkpoints: Apply price changes to existing asset values
        
        // Collect all unique tokens we need prices for (both existing assets and new allocations)
        let existingTokens = Array.map<(Principal, Float), Principal>(Buffer.toArray(assetValues), func((token, _)) { token });
        let newTokens = Array.map<Allocation, Principal>(allocations, func(allocation) { allocation.token });
        
        // Combine and deduplicate tokens
        let allTokensBuffer = Buffer.Buffer<Principal>(existingTokens.size() + newTokens.size());
        for (token in existingTokens.vals()) {
          allTokensBuffer.add(token);
        };
        for (token in newTokens.vals()) {
          let isDuplicate = Array.find<Principal>(existingTokens, func(t) { Principal.equal(t, token) });
          switch (isDuplicate) {
            case null { allTokensBuffer.add(token); };
            case _ {}; // Already added
          };
        };
        let allTokens = Buffer.toArray(allTokensBuffer);
        
        // Get prices for all tokens at once
        let batchPriceResult = await priceArchive.getPricesAtTime(allTokens, timestamp);
        let tokenPrices = switch (batchPriceResult) {
          case (#ok(prices)) { prices };
          case (#err(_)) {
            return #err(#SystemError("Failed to get price data"));
          };
        };
        
        // Helper function to find price for a token
        let findPrice = func(token: Principal) : ?PriceInfo {
          let priceEntry = Array.find<(Principal, ?PriceInfo)>(tokenPrices, func((t, _)) {
            Principal.equal(t, token)
          });
          switch (priceEntry) {
            case (?(_, price)) { price };
            case null { null };
          };
        };
        
        // Step 1: Update asset values based on price changes
        let updatedAssetValues = Buffer.Buffer<(Principal, Float)>(assetValues.size());
        let updatedPrices = Buffer.Buffer<(Principal, Float)>(previousPrices.size());
        
        for (j in Iter.range(0, assetValues.size() - 1)) {
          let (token, oldValue) = assetValues.get(j);
          let (_, oldPrice) = previousPrices.get(j);
          
          // Get new price for this token from batch result
          switch (findPrice(token)) {
            case (?priceInfo) {
              let newPrice = getPriceValue(priceInfo, priceType);
              let priceRatio = newPrice / oldPrice; // This is the key fix!
              let newValue = oldValue * priceRatio;
              
              updatedAssetValues.add((token, newValue));
              updatedPrices.add((token, newPrice));
            };
            case null {
              return #err(#PriceDataMissing({token = token; timestamp = timestamp}));
            };
          };
        };
        
        // Step 2: Calculate total portfolio value after price changes
        var totalValueAfterPriceChanges : Float = 0.0;
        for (j in Iter.range(0, updatedAssetValues.size() - 1)) {
          let (_, value) = updatedAssetValues.get(j);
          totalValueAfterPriceChanges += value;
        };
        
        // Step 3: Rebalance to new allocations using the updated total value
        for (allocation in allocations.vals()) {
          let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
          let tokenValue = totalValueAfterPriceChanges * allocationPercent;
          
          // Get price info for this token from batch result
          switch (findPrice(allocation.token)) {
            case (?priceInfo) {
              tokenPricesBuffer.add((allocation.token, priceInfo));
              tokenValuesBuffer.add((allocation.token, tokenValue));
            };
            case null {
              return #err(#PriceDataMissing({token = allocation.token; timestamp = timestamp}));
            };
          };
        };
        
        // Create checkpoint
        let checkpoint : CheckpointData = {
          timestamp = timestamp;
          allocations = allocations;
          tokenValues = Buffer.toArray(tokenValuesBuffer);
          totalPortfolioValue = totalValueAfterPriceChanges;
          pricesUsed = Buffer.toArray(tokenPricesBuffer);
          maker = maker;
        };
        checkpointsBuffer.add(checkpoint);
        
        // Step 4: Update asset values for next iteration (rebalanced values)
        assetValues := Buffer.Buffer<(Principal, Float)>(allocations.size());
        previousPrices := updatedPrices;
        
        for (allocation in allocations.vals()) {
          let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
          let tokenValue = totalValueAfterPriceChanges * allocationPercent;
          assetValues.add((allocation.token, tokenValue));
        };
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
  private func _calculatePortfolioValue(
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
  // Periodic Distribution System
  //=========================================================================

  // Start the recurring distribution timer
  public shared ({ caller }) func startDistributionTimer() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    let now = Time.now();
    let nextRunTime = now + distributionPeriodNS;
    await startDistributionTimerAt(nextRunTime)
  };

  // Start distribution timer with a specific target datetime
  public shared ({ caller }) func startDistributionTimerAt(targetTime: Int) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    let now = Time.now();
    
    // Validate target time is in the future
    if (targetTime <= now) {
      return #err(#SystemError("Target time must be in the future"));
    };

    // Cancel existing timer if any
    switch (distributionTimerId) {
      case (?id) { Timer.cancelTimer(id); };
      case null { };
    };

    // Enable distributions and store the scheduled time
    distributionEnabled := true;
    nextScheduledDistributionTime := ?targetTime;
    
    // Calculate delay until target time
    let delayNS = Int.abs(targetTime - now);
    
    // Start timer with calculated delay
    await* scheduleDistributionAt<system>(delayNS);
    
    logger.info("Distribution", "Distribution timer started, next run scheduled for: " # Int.toText(targetTime), "startDistributionTimerAt");
    #ok("Distribution timer started");
  };

  // Stop the distribution timer
  public shared ({ caller }) func stopDistributionTimer() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    // Cancel existing timer
    switch (distributionTimerId) {
      case (?id) {
        Timer.cancelTimer(id);
        distributionTimerId := null;
      };
      case null { };
    };

    // Disable distributions and clear scheduled time
    distributionEnabled := false;
    nextScheduledDistributionTime := null;
    
    logger.info("Distribution", "Distribution timer stopped and disabled", "stopDistributionTimer");
    #ok("Distribution timer stopped");
  };

  // Schedule a distribution at a specific delay from now
  private func scheduleDistributionAt<system>(delayNS: Nat) : async* () {
    distributionTimerId := ?Timer.setTimer<system>(
      #nanoseconds(delayNS),
      func() : async () {
        // Update the last distribution time when timer fires
        lastDistributionTime := Time.now();
        
        // Start distribution asynchronously (0-second timer to avoid blocking)
        ignore Timer.setTimer<system>(
          #nanoseconds(0),
          func() : async () {
            await* runPeriodicDistribution<system>();
          }
        );
        
        // Schedule next distribution if still enabled
        if (distributionEnabled) {
          let nextRunTime = Time.now() + distributionPeriodNS;
          nextScheduledDistributionTime := ?nextRunTime;
          await* scheduleDistributionAt<system>(distributionPeriodNS);
        } else {
          nextScheduledDistributionTime := null;
        };
      }
    );
  };

  // Legacy method for backward compatibility
  private func scheduleNextDistribution<system>() : async* () {
    if (not distributionEnabled) {
      return;
    };
    await* scheduleDistributionAt<system>(distributionPeriodNS);
  };

  // Main distribution function
  private func runPeriodicDistribution<system>() : async* () {
    // Check if distribution is already in progress
    switch (currentDistributionId) {
      case (?_) {
        logger.warn("Distribution", "Distribution already in progress, skipping", "runPeriodicDistribution");
        return;
      };
      case null { };
    };

    let now = Time.now();
    let endTime = now;
    let startTime = now - distributionPeriodNS;
    
    await* runDistributionWithParams<system>(startTime, endTime, #USD);
  };

  // Custom distribution function with specified parameters
  private func runCustomDistribution<system>(
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async* () {
    // Check if distribution is already in progress
    switch (currentDistributionId) {
      case (?_) {
        logger.warn("Distribution", "Distribution already in progress, skipping", "runCustomDistribution");
        return;
      };
      case null { };
    };

    await* runDistributionWithParams<system>(startTime, endTime, priceType);
  };

  // Core distribution logic with parameters
  private func runDistributionWithParams<system>(
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async* () {
    distributionCounter += 1;
    currentDistributionId := ?distributionCounter;
    
    logger.info("Distribution", "Starting distribution #" # Nat.toText(distributionCounter) # " from " # Int.toText(startTime) # " to " # Int.toText(endTime), "runDistributionWithParams");

    // Create initial distribution record
    let now = Time.now();
    let initialRecord : DistributionRecord = {
      id = distributionCounter;
      startTime = startTime;
      endTime = endTime;
      distributionTime = now;
      totalRewardPot = periodicRewardPot;
      actualDistributed = 0; // Will be updated when distribution completes
      totalRewardScore = 0.0;
      neuronsProcessed = 0;
      neuronRewards = [];
      failedNeurons = [];
      status = #InProgress({currentNeuron = 0; totalNeurons = 0});
    };

    // Add to history (using treasury pattern for consistency)
    Vector.add(distributionHistory, initialRecord);
    if (Vector.size(distributionHistory) > maxDistributionHistory) {
      Vector.reverse(distributionHistory);
      while (Vector.size(distributionHistory) > maxDistributionHistory) {
        ignore Vector.removeLast(distributionHistory);
      };
      Vector.reverse(distributionHistory);
    };

    // Validate that we have enough available balance for the distribution
    let availableBalance = await getAvailableBalance();
    let rewardPotSatoshis = tacoTokensToSatoshis(periodicRewardPot);
    
    if (availableBalance < rewardPotSatoshis) {
      let errorMsg = "Insufficient available balance for distribution. Available: " # 
                     Nat.toText(availableBalance) # " satoshis, Required: " # 
                     Nat.toText(rewardPotSatoshis) # " satoshis";
      logger.error("Distribution", errorMsg, "runDistributionWithParams");
      await* completeDistribution(distributionCounter, ([] : [NeuronReward]), ([] : [FailedNeuron]), 0.0, errorMsg);
      return;
    };

    logger.info("Distribution", "Balance validation passed. Available: " # 
                Nat.toText(availableBalance) # " satoshis, Required: " # 
                Nat.toText(rewardPotSatoshis) # " satoshis", "runDistributionWithParams");

    // Get all neurons from DAO
    try {
      let neuronsResult = await daoCanister.admin_getNeuronAllocations();
      let neurons = neuronsResult;
      
      logger.info("Distribution", "Found " # Nat.toText(neurons.size()) # " neurons to process", "runPeriodicDistribution");
      
      if (neurons.size() == 0) {
        await* completeDistribution(distributionCounter, ([] : [NeuronReward]), ([] : [FailedNeuron]), 0.0, "No neurons found");
        return;
      };

      // Update status with total neuron count
      let updatedRecord = {
        initialRecord with
        status = #InProgress({currentNeuron = 0; totalNeurons = neurons.size()});
      };
      Vector.put(distributionHistory, Vector.size(distributionHistory) - 1, updatedRecord);

      // Start processing neurons one by one
      await* processNeuronsSequentially<system>(distributionCounter, neurons, 0, ([] : [NeuronReward]), ([] : [FailedNeuron]), 0.0, startTime, endTime, priceType);
      
    } catch (error) {
      logger.error("Distribution", "Failed to get neurons: " # "Error occurred", "runPeriodicDistribution");
      await* completeDistribution(distributionCounter, ([] : [NeuronReward]), ([] : [FailedNeuron]), 0.0, "Failed to get neurons: " # "Error occurred");
    };
  };

  // Process neurons sequentially using timer pattern to avoid timeouts
  private func processNeuronsSequentially<system>(
    distributionId: Nat,
    neurons: [(Blob, NeuronAllocation)],
    currentIndex: Nat,
    neuronRewards: [NeuronReward],
    failedNeurons: [FailedNeuron],
    totalRewardScore: Float,
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async* () {
    
    // Check if this distribution was cancelled
    switch (currentDistributionId) {
      case (?id) {
        if (id != distributionId) {
          logger.warn("Distribution", "Distribution " # Nat.toText(distributionId) # " was cancelled", "processNeuronsSequentially");
          return;
        };
      };
      case null {
        logger.warn("Distribution", "No current distribution, stopping", "processNeuronsSequentially");
        return;
      };
    };

    // Check if we've processed all neurons
    if (currentIndex >= neurons.size()) {
      await* calculateAndDistributeRewards(distributionId, neuronRewards, failedNeurons, totalRewardScore);
      return;
    };

    let (neuronId, neuronAllocation) = neurons[currentIndex];
    
    logger.info("Distribution", "Processing neuron " # Nat.toText(currentIndex + 1) # "/" # Nat.toText(neurons.size()), "processNeuronsSequentially");

    // Update distribution status
    switch (Vector.get(distributionHistory, Vector.size(distributionHistory) - 1)) {
      case (record) {
        let updatedRecord = {
          record with
          neuronsProcessed = currentIndex + 1;
          status = #InProgress({currentNeuron = currentIndex + 1; totalNeurons = neurons.size()});
        };
        Vector.put(distributionHistory, Vector.size(distributionHistory) - 1, updatedRecord);
      };
    };

    try {
      // Calculate performance for this neuron
      let performanceResult = await calculateNeuronPerformance(
        neuronId,
        startTime,
        endTime,
        priceType
      );

      switch (performanceResult) {
        case (#ok(performance)) {
          // Get voting power - use the most recent (oldest in time) available
          let votingPower = getVotingPowerFromPerformance(performance);
          // Apply power factors to performance score and voting power
          let adjustedPerformanceScore = Float.pow(performance.performanceScore, performanceScorePower);
          let adjustedVotingPower = Float.pow(Float.fromInt(votingPower), votingPowerPower);
          let rewardScore = adjustedPerformanceScore * adjustedVotingPower;
          
          let neuronReward : NeuronReward = {
            neuronId = neuronId;
            performanceScore = performance.performanceScore;
            votingPower = votingPower;
            rewardScore = rewardScore;
            rewardAmount = 0; // Will be calculated later
            checkpoints = performance.checkpoints;
          };

          let updatedRewards = Array.flatten([neuronRewards, [neuronReward]]);
          let updatedTotalScore = totalRewardScore + rewardScore;

          // Schedule next neuron processing with 0 delay (timer pattern)
          ignore Timer.setTimer<system>(
            #nanoseconds(0),
            func() : async () {
              await* processNeuronsSequentially<system>(
                distributionId,
                neurons,
                currentIndex + 1,
                updatedRewards,
                failedNeurons, // Keep same failed neurons list
                updatedTotalScore,
                startTime,
                endTime,
                priceType
              );
            }
          );
        };
        case (#err(error)) {
          let errorMsg = debug_show(error);
          logger.warn("Distribution", "Failed to calculate performance for neuron: " # errorMsg, "processNeuronsSequentially");
          
          // Add this neuron to the failed list
          let failedNeuron : FailedNeuron = {
            neuronId = neuronId;
            errorMessage = errorMsg;
          };
          let updatedFailedNeurons = Array.flatten([failedNeurons, [failedNeuron]]);
          
          // Continue with next neuron
          ignore Timer.setTimer<system>(
            #nanoseconds(0),
            func() : async () {
              await* processNeuronsSequentially<system>(
                distributionId,
                neurons,
                currentIndex + 1,
                neuronRewards,
                updatedFailedNeurons,
                totalRewardScore,
                startTime,
                endTime,
                priceType
              );
            }
          );
        };
      };
    } catch (error) {
      let errorMsg = Error.message(error);
      logger.error("Distribution", "Error processing neuron: " # errorMsg, "processNeuronsSequentially");
      
      // Add this neuron to the failed list
      let failedNeuron : FailedNeuron = {
        neuronId = neuronId;
        errorMessage = "System error: " # errorMsg;
      };
      let updatedFailedNeurons = Array.flatten([failedNeurons, [failedNeuron]]);
      
      // Continue with next neuron
      ignore Timer.setTimer<system>(
        #nanoseconds(0),
        func() : async () {
          await* processNeuronsSequentially<system>(
            distributionId,
            neurons,
            currentIndex + 1,
            neuronRewards,
            updatedFailedNeurons,
            totalRewardScore,
            startTime,
            endTime,
            priceType
          );
        }
      );
    };
  };

  // Calculate final reward amounts and distribute
  private func calculateAndDistributeRewards(
    distributionId: Nat,
    neuronRewards: [NeuronReward],
    failedNeurons: [FailedNeuron],
    totalRewardScore: Float
  ) : async* () {
    
    if (totalRewardScore == 0.0) {
      await* completeDistribution(distributionId, neuronRewards, failedNeurons, totalRewardScore, "No valid reward scores");
      return;
    };

    // Calculate individual reward amounts
    let periodicRewardPotSatoshis = tacoTokensToSatoshis(periodicRewardPot);
    let finalRewards = Array.map<NeuronReward, NeuronReward>(neuronRewards, func(reward) {
      // Calculate as float first, then convert to integer satoshis using floor
      let rewardAmountFloat = (reward.rewardScore / totalRewardScore) * Float.fromInt(periodicRewardPotSatoshis);
      let rewardAmount = Int.abs(Float.toInt(Float.floor(rewardAmountFloat))); // Use floor to never exceed pot
      { reward with rewardAmount = rewardAmount }
    });

    // Credit rewards to neuron balances
    for (reward in finalRewards.vals()) {
      let currentBalance = switch (Map.get(neuronRewardBalances, bhash, reward.neuronId)) {
        case (?balance) { balance };
        case null { 0 };
      };
      Map.set(neuronRewardBalances, bhash, reward.neuronId, currentBalance + reward.rewardAmount);
    };

    await* completeDistribution(distributionId, finalRewards, failedNeurons, totalRewardScore, "");
    
    logger.info("Distribution", "Distribution " # Nat.toText(distributionId) # " completed. Processed " # Nat.toText(finalRewards.size()) # " neurons, distributed " # Nat.toText(periodicRewardPot) # " TACO tokens", "calculateAndDistributeRewards");
  };

  // Complete the distribution and update records
  private func completeDistribution(
    distributionId: Nat,
    neuronRewards: [NeuronReward],
    failedNeurons: [FailedNeuron],
    totalRewardScore: Float,
    errorMessage: Text
  ) : async* () {
    
    currentDistributionId := null;
    lastDistributionTime := Time.now();

    // Update the distribution record
    let historyIndex = Vector.size(distributionHistory) - 1;
    switch (Vector.get(distributionHistory, historyIndex)) {
      case (record) {
        let status = if (errorMessage != "") {
          #Failed(errorMessage)
        } else if (failedNeurons.size() > 0) {
          #PartiallyCompleted({
            successfulNeurons = neuronRewards.size();
            failedNeurons = failedNeurons.size();
          })
        } else {
          #Completed
        };

        // Calculate total amount distributed in this distribution
        let distributedAmount = Array.foldLeft<NeuronReward, Nat>(
          neuronRewards, 
          0, 
          func(acc, reward) { acc + reward.rewardAmount }
        );
        
        // Update the total distributed amount
        totalDistributed += distributedAmount;

        let finalRecord = {
          record with
          totalRewardScore = totalRewardScore;
          actualDistributed = distributedAmount;
          neuronsProcessed = neuronRewards.size() + failedNeurons.size();
          neuronRewards = neuronRewards;
          failedNeurons = failedNeurons;
          status = status;
        };
        Vector.put(distributionHistory, historyIndex, finalRecord);
      };
    };
  };

  // Helper function to extract voting power from performance result
  private func getVotingPowerFromPerformance(performance: PerformanceResult) : Nat {
    // Use the oldest voting power available (from preTimespanAllocation or first in-timespan change)
    switch (performance.preTimespanAllocation) {
      case (?preAlloc) { preAlloc.votingPower };
      case null {
        if (performance.inTimespanChanges.size() > 0) {
          performance.inTimespanChanges[0].votingPower
        } else {
          0 // No voting power data available
        };
      };
    };
  };

  // Manual distribution trigger (for testing/admin)
  public shared ({ caller }) func triggerDistribution() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    switch (currentDistributionId) {
      case (?_) {
        return #err(#DistributionInProgress);
      };
      case null {
        try {
          await* runPeriodicDistribution<system>();
          #ok("Distribution triggered successfully");
        } catch (error) {
          #err(#SystemError("Failed to trigger distribution: " # Error.message(error)));
        };
      };
    };
  };

  // Manual distribution trigger with custom parameters (for testing/admin)
  public shared ({ caller }) func triggerDistributionCustom(
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };

    switch (currentDistributionId) {
      case (?_) {
        return #err(#DistributionInProgress);
      };
      case null {
        try {
          await* runCustomDistribution<system>(startTime, endTime, priceType);
          #ok("Custom distribution triggered successfully");
        } catch (error) {
          #err(#SystemError("Failed to trigger custom distribution: " # Error.message(error)));
        };
      };
    };
  };

  //=========================================================================
  // Reward Claim Functions
  //=========================================================================

  // Get neuron reward balance (returns TACO satoshis)
  public query func getNeuronRewardBalance(neuronId: Blob) : async Nat {
    switch (Map.get(neuronRewardBalances, bhash, neuronId)) {
      case (?balance) { balance };
      case null { 0 };
    };
  };

  // Get multiple neuron reward balances (returns array of neuronId-balance tuples)
  public query func getNeuronRewardBalances(neuronIds: [Blob]) : async [(Blob, Nat)] {
    Array.map<Blob, (Blob, Nat)>(neuronIds, func(neuronId) {
      let balance = switch (Map.get(neuronRewardBalances, bhash, neuronId)) {
        case (?bal) { bal };
        case null { 0 };
      };
      (neuronId, balance)
    });
  };

  // Get total distributed amount (returns TACO satoshis)
  public query func getTotalDistributed() : async Nat {
    totalDistributed
  };

  // Get current TACO balance of this canister (returns TACO satoshis)
  public func getTacoBalance() : async Nat {
    let tacoLedger : ICRC.Self = actor(TACO_LEDGER_CANISTER_ID);
    let account : ICRC.Account = {
      owner = this_canister_id();
      subaccount = null;
    };
    
    try {
      await tacoLedger.icrc1_balance_of(account)
    } catch (error) {
      logger.error("TacoBalance", "Failed to get TACO balance: " # Error.message(error), "getTacoBalance");
      0 // Return 0 on error
    }
  };

  // Get all neuron reward balances (admin only) - returns TACO satoshis
  public shared query ({ caller }) func getAllNeuronRewardBalances() : async [(Blob, Nat)] {
    if (not isAdmin(caller)) {
      return [];
    };
    Iter.toArray(Map.entries(neuronRewardBalances));
  };

  // TODO: Implement reward claiming mechanism
  // This would transfer rewards to user wallets and deduct from balances

  //=========================================================================
  // Distribution History and Status
  //=========================================================================

  // Get distribution history
  public query func getDistributionHistory(limit: ?Nat) : async [DistributionRecord] {
    let actualLimit = switch (limit) {
      case (?l) { Nat.min(l, Vector.size(distributionHistory)) };
      case null { Vector.size(distributionHistory) };
    };
    
    let historyArray = Vector.toArray(distributionHistory);
    let startIndex = if (historyArray.size() > actualLimit) {
      historyArray.size() - actualLimit
    } else {
      0
    };
    
    Array.subArray(historyArray, startIndex, actualLimit);
  };

  // Get current distribution status
  public query func getCurrentDistributionStatus() : async {
    inProgress: Bool;
    currentDistributionId: ?Nat;
    lastDistributionTime: Int;
    nextDistributionTime: Int;
    distributionEnabled: Bool;
  } {
    {
      inProgress = switch (currentDistributionId) { case (?_) { true }; case null { false } };
      currentDistributionId = currentDistributionId;
      lastDistributionTime = lastDistributionTime;
      nextDistributionTime = lastDistributionTime + distributionPeriodNS;
      distributionEnabled = distributionEnabled;
    };
  };

  //=========================================================================
  // Archive Integration - "Since" Methods
  //=========================================================================

  // Get distribution records since a timestamp for archive import
  public shared query ({ caller }) func getDistributionsSince(sinceTimestamp: Int, limit: Nat) : async Result.Result<{
    distributions: [DistributionRecord];
  }, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    if (limit > 1000) {
      return #err(#SystemError("Limit cannot exceed 1000"));
    };

    let historyArray = Vector.toArray(distributionHistory);
    let filteredDistributions = Array.filter<DistributionRecord>(historyArray, func(dist) {
      dist.distributionTime > sinceTimestamp
    });

    // Sort by timestamp (oldest first) for proper archival order
    let sortedDistributions = Array.sort<DistributionRecord>(filteredDistributions, func(a, b) {
      Int.compare(a.distributionTime, b.distributionTime)
    });

    // Apply limit
    let limitedDistributions = if (sortedDistributions.size() > limit) {
      Array.subArray(sortedDistributions, 0, limit)
    } else {
      sortedDistributions
    };

    #ok({
      distributions = limitedDistributions;
    });
  };

  // Get withdrawal records since a timestamp for archive import
  public shared query ({ caller }) func getWithdrawalsSince(sinceTimestamp: Int, limit: Nat) : async Result.Result<{
    withdrawals: [WithdrawalRecord];
  }, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    if (limit > 1000) {
      return #err(#SystemError("Limit cannot exceed 1000"));
    };

    let historyArray = Vector.toArray(withdrawalHistory);
    let filteredWithdrawals = Array.filter<WithdrawalRecord>(historyArray, func(withdrawal) {
      withdrawal.timestamp > sinceTimestamp
    });

    // Sort by timestamp (oldest first) for proper archival order
    let sortedWithdrawals = Array.sort<WithdrawalRecord>(filteredWithdrawals, func(a, b) {
      Int.compare(a.timestamp, b.timestamp)
    });

    // Apply limit
    let limitedWithdrawals = if (sortedWithdrawals.size() > limit) {
      Array.subArray(sortedWithdrawals, 0, limit)
    } else {
      sortedWithdrawals
    };

    #ok({
      withdrawals = limitedWithdrawals;
    });
  };

  //=========================================================================
  // Admin Configuration Functions
  //=========================================================================

  // Set distribution period
  public shared ({ caller }) func setDistributionPeriod(periodNS: Nat) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    distributionPeriodNS := periodNS;
    logger.info("Config", "Distribution period set to " # Nat.toText(periodNS / 1_000_000_000) # " seconds", "setDistributionPeriod");
    #ok("Distribution period updated");
  };

  // Set periodic reward pot (amount in whole TACO tokens)
  public shared ({ caller }) func setPeriodicRewardPot(amount: Nat) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    periodicRewardPot := amount;
    logger.info("Config", "Periodic reward pot set to " # Nat.toText(amount) # " TACO tokens", "setPeriodicRewardPot");
    #ok("Periodic reward pot updated");
  };

  // Enable/disable distribution
  public shared ({ caller }) func setDistributionEnabled(enabled: Bool) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    distributionEnabled := enabled;
    logger.info("Config", "Distribution " # (if (enabled) { "enabled" } else { "disabled" }), "setDistributionEnabled");
    #ok("Distribution status updated");
  };

  // Set performance score power factor
  public shared ({ caller }) func setPerformanceScorePower(power: Float) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    if (power < 0.0) {
      return #err(#SystemError("Performance score power must be >= 0"));
    };
    
    performanceScorePower := power;
    logger.info("Config", "Performance score power set to " # Float.toText(power), "setPerformanceScorePower");
    #ok("Performance score power updated");
  };

  // Set voting power power factor
  public shared ({ caller }) func setVotingPowerPower(power: Float) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    if (power < 0.0) {
      return #err(#SystemError("Voting power power must be >= 0"));
    };
    
    votingPowerPower := power;
    logger.info("Config", "Voting power power set to " # Float.toText(power), "setVotingPowerPower");
    #ok("Voting power power updated");
  };

  // Get configuration
  public query func getConfiguration() : async {
    distributionPeriodNS: Nat;
    periodicRewardPot: Nat; // Reward pot in whole TACO tokens
    maxDistributionHistory: Nat;
    distributionEnabled: Bool;
    performanceScorePower: Float;
    votingPowerPower: Float;
    timerRunning: Bool;
    nextScheduledDistribution: ?Int;
    lastDistributionTime: Int;
    totalDistributions: Nat;
  } {
    {
      distributionPeriodNS = distributionPeriodNS;
      periodicRewardPot = periodicRewardPot;
      maxDistributionHistory = maxDistributionHistory;
      distributionEnabled = distributionEnabled;
      performanceScorePower = performanceScorePower;
      votingPowerPower = votingPowerPower;
      timerRunning = switch (distributionTimerId) { case (?_) { true }; case null { false } };
      nextScheduledDistribution = nextScheduledDistributionTime;
      lastDistributionTime = lastDistributionTime;
      totalDistributions = distributionCounter;
    };
  };

  // Restore timer after canister upgrade
  system func postupgrade() {
    // Restore timer if we have a scheduled time
    switch (nextScheduledDistributionTime) {
      case (?scheduledTime) {
        let now = Time.now();
        
        if (scheduledTime > now) {
          // Scheduled time is in the future, restore timer with remaining delay
          let delayNS = Int.abs(scheduledTime - now);
          distributionTimerId := ?Timer.setTimer<system>(
            #nanoseconds(delayNS),
            func() : async () {
              lastDistributionTime := Time.now();
              ignore Timer.setTimer<system>(
                #nanoseconds(0),
                func() : async () {
                  await* runPeriodicDistribution<system>();
                }
              );
              
              if (distributionEnabled) {
                let nextRunTime = Time.now() + distributionPeriodNS;
                nextScheduledDistributionTime := ?nextRunTime;
                await* scheduleDistributionAt<system>(distributionPeriodNS);
              } else {
                nextScheduledDistributionTime := null;
              };
            }
          );
          logger.info("Postupgrade", "Timer restored for scheduled time: " # Int.toText(scheduledTime), "postupgrade");
        } else {
          // Scheduled time is in the past, find next valid time
          var nextValidTime = scheduledTime;
          while (nextValidTime <= now) {
            nextValidTime += distributionPeriodNS;
          };
          
          // Update stored time and start timer
          nextScheduledDistributionTime := ?nextValidTime;
          let delayNS = Int.abs(nextValidTime - now);
          distributionTimerId := ?Timer.setTimer<system>(
            #nanoseconds(delayNS),
            func() : async () {
              lastDistributionTime := Time.now();
              ignore Timer.setTimer<system>(
                #nanoseconds(0),
                func() : async () {
                  await* runPeriodicDistribution<system>();
                }
              );
              
              if (distributionEnabled) {
                let nextRunTime = Time.now() + distributionPeriodNS;
                nextScheduledDistributionTime := ?nextRunTime;
                await* scheduleDistributionAt<system>(distributionPeriodNS);
              } else {
                nextScheduledDistributionTime := null;
              };
            }
          );
          logger.info("Postupgrade", "Timer restored for adjusted time: " # Int.toText(nextValidTime), "postupgrade");
        };
      };
      case null {
        logger.info("Postupgrade", "No scheduled distribution found, timer not restored", "postupgrade");
      };
    };
  };

  // Get all withdrawal history (admin only)
  public shared ({ caller }) func getAllWithdrawalHistory(limit: ?Nat) : async Result.Result<[WithdrawalRecord], RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    let maxLimit = switch (limit) {
      case (?l) { if (l > 100) { 100 } else { l } }; // Cap at 100 records
      case null { 50 }; // Default to 50 records
    };
    
    let historySize = Vector.size(withdrawalHistory);
    let recordsToTake = if (historySize < maxLimit) { historySize } else { maxLimit };
    
    let records = Buffer.Buffer<WithdrawalRecord>(recordsToTake);
    
    // Get the most recent records (iterate backwards)
    var i = historySize;
    var taken = 0;
    while (i > 0 and taken < recordsToTake) {
      i -= 1;
      switch (Vector.getOpt(withdrawalHistory, i)) {
        case (?record) {
          records.add(record);
          taken += 1;
        };
        case null { };
      };
    };
    
    #ok(Buffer.toArray(records));
  };

  // Get user's withdrawal history (authenticated user only)
  public shared ({ caller }) func getUserWithdrawalHistory(limit: ?Nat) : async Result.Result<[WithdrawalRecord], RewardsError> {
    let maxLimit = switch (limit) {
      case (?l) { if (l > 50) { 50 } else { l } }; // Cap at 50 records for users
      case null { 20 }; // Default to 20 records
    };
    
    let historySize = Vector.size(withdrawalHistory);
    let userRecords = Buffer.Buffer<WithdrawalRecord>(maxLimit);
    
    // Search through withdrawal history for records from this caller
    var i = historySize;
    var found = 0;
    while (i > 0 and found < maxLimit) {
      i -= 1;
      switch (Vector.getOpt(withdrawalHistory, i)) {
        case (?record) {
          if (record.caller == caller) {
            userRecords.add(record);
            found += 1;
          };
        };
        case null { };
      };
    };
    
    #ok(Buffer.toArray(userRecords));
  };

  // Get withdrawal statistics (admin only)
  public shared ({ caller }) func getWithdrawalStats() : async Result.Result<{
    totalWithdrawn: Nat;
    totalWithdrawals: Nat;
    totalRecordsInHistory: Nat;
  }, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    #ok({
      totalWithdrawn = totalWithdrawn;
      totalWithdrawals = totalWithdrawals;
      totalRecordsInHistory = Vector.size(withdrawalHistory);
    });
  };

  private func isAdmin(caller: Principal) : Bool {
    AdminAuth.isAdmin(caller, canister_ids.isKnownCanister)
  };

  //=========================================================================
  // Admin Functions
  //=========================================================================

  //=========================================================================
  // Withdrawal Functions
  //=========================================================================

  // Withdraw rewards from specified neurons to ICRC1 Account
  public shared ({ caller }) func withdraw(account: ICRC.Account, neuronIds: [Blob]) : async ICRC.Result {
    if (neuronIds.size() == 0) {
      return #Err(#GenericError({ error_code = 1001; message = "No neurons specified" }));
    };

    // Track original balances for rollback if needed
    let originalBalances = Buffer.Buffer<(Blob, Nat)>(neuronIds.size());

    try {
      // Create SNS governance actor to verify neuron ownership
      let snsGov = actor (Principal.toText(SNS_GOVERNANCE_CANISTER_ID)) : actor {
        list_neurons : shared query NeuronSnapshot.ListNeurons -> async NeuronSnapshot.ListNeuronsResponse;
      };

      // Fetch caller's neurons from SNS governance
      let neuronsResult = await snsGov.list_neurons({
        of_principal = ?caller;
        limit = 1000; // Should be enough for most users
        start_page_at = null;
      });

      // Create set of caller's owned neuron IDs for fast lookup
      let ownedNeuronIds = Buffer.Buffer<Blob>(neuronsResult.neurons.size());
      for (neuron in neuronsResult.neurons.vals()) {
        switch (neuron.id) {
          case (?neuronId) {
            ownedNeuronIds.add(neuronId.id);
          };
          case null { /* Skip neurons without IDs */ };
        };
      };
      let ownedNeuronSet = Buffer.toArray(ownedNeuronIds);

      // Verify all requested neurons belong to caller
      for (neuronId in neuronIds.vals()) {
        let isOwned = Array.find<Blob>(ownedNeuronSet, func(ownedId) { 
          neuronId == ownedId 
        });
        if (isOwned == null) {
          return #Err(#GenericError({ error_code = 1002; message = "Neuron not owned by caller" }));
        };
      };

      // Calculate total balance from all specified neurons and collect original balances
      var totalBalance : Nat = 0;
      
      for (neuronId in neuronIds.vals()) {
        let balance = switch (Map.get(neuronRewardBalances, bhash, neuronId)) {
          case (?bal) { bal };
          case null { 0 };
        };
        originalBalances.add((neuronId, balance));
        totalBalance += balance;
      };

      // Validate total amount
      if (totalBalance <= TACO_WITHDRAWAL_FEE) {
        return #Err(#InsufficientFunds({ balance = totalBalance }));
      };

      // Calculate amount to send (total - fee)
      let amountToSend = totalBalance - TACO_WITHDRAWAL_FEE;

      // Deduct exact recorded balances from all neurons BEFORE transfer to prevent double spending
      for ((neuronId, originalBalance) in originalBalances.vals()) {
        let currentBalance = switch (Map.get(neuronRewardBalances, bhash, neuronId)) {
          case (?balance) { balance };
          case null { 0 };
        };
        let newBalance = if (currentBalance >= originalBalance) {
          currentBalance - originalBalance
        } else {
          // This should never happen - indicates serious bug or data corruption
          _Debug.trap("Withdrawal error: current balance (" # Nat.toText(currentBalance) # ") < original balance (" # Nat.toText(originalBalance) # ") for neuron");
        };
        
        if (newBalance == 0) {
          ignore Map.remove(neuronRewardBalances, bhash, neuronId);
        } else {
          ignore Map.put(neuronRewardBalances, bhash, neuronId, newBalance);
        };
      };

      // Create withdrawal record
      withdrawalCounter += 1;
      let withdrawalRecord : WithdrawalRecord = {
        id = withdrawalCounter;
        caller = caller;
        neuronWithdrawals = Buffer.toArray(originalBalances);
        totalAmount = totalBalance;
        amountSent = amountToSend;
        fee = TACO_WITHDRAWAL_FEE;
        targetAccount = account;
        timestamp = Time.now();
        transactionId = null; // Will be updated if transfer succeeds
      };

      // Perform ICRC1 transfer
      let tacoLedger : ICRC.Self = actor(TACO_LEDGER_CANISTER_ID);
      let transferArgs : ICRC.TransferArg = {
        from_subaccount = null;
        to = account;
        amount = amountToSend;
        fee = null; // Let the ledger handle the fee
        memo = null;
        created_at_time = ?Nat64.fromNat(Int.abs(Time.now()));
      };

      let transferResult = await tacoLedger.icrc1_transfer(transferArgs);
      
      switch (transferResult) {
        case (#Ok(transactionId)) {
          // Transfer successful - update tracking
          totalWithdrawn += totalBalance;
          totalWithdrawals += 1;
          
          // Update withdrawal record with transaction ID
          let finalRecord = { withdrawalRecord with transactionId = ?transactionId };
          Vector.add(withdrawalHistory, finalRecord);
          
          // Log successful withdrawal
          logger.info("Withdrawal", "User " # Principal.toText(caller) # " withdrew " # Nat.toText(amountToSend) # " TACO satoshis (total: " # Nat.toText(totalBalance) # ") from " # Nat.toText(neuronIds.size()) # " neurons to account " # Principal.toText(account.owner), "withdraw");
          
          #Ok(transactionId);
        };
        case (#Err(error)) {
          // Transfer failed - restore balances to all neurons
          for ((neuronId, originalBalance) in originalBalances.vals()) {
            if (originalBalance > 0) {
              let currentBalance = switch (Map.get(neuronRewardBalances, bhash, neuronId)) {
                case (?balance) { balance };
                case null { 0 };
              };
              ignore Map.put(neuronRewardBalances, bhash, neuronId, currentBalance + originalBalance);
            };
          };
          
          // Log failed withdrawal
          logger.error("Withdrawal", "Failed withdrawal for user " # Principal.toText(caller) # ": " # debug_show(error), "withdraw");
          
          #Err(error);
        };
      };
    } catch (error) {
      // Exception occurred - restore balances if they were deducted
      for ((neuronId, originalBalance) in originalBalances.vals()) {
        if (originalBalance > 0) {
          let currentBalance = switch (Map.get(neuronRewardBalances, bhash, neuronId)) {
            case (?balance) { balance };
            case null { 0 };
          };
          ignore Map.put(neuronRewardBalances, bhash, neuronId, currentBalance + originalBalance);
        };
      };
      
      logger.error("Withdrawal", "Withdrawal exception for user " # Principal.toText(caller) # ": " # Error.message(error), "withdraw");
      
      #Err(#GenericError({ error_code = 1003; message = "Withdrawal failed: " # Error.message(error) }));
    };
  };

  // Get the sum of all current neuron reward balances
  public query func getCurrentTotalNeuronBalances() : async Nat {
    var total : Nat = 0;
    for ((_, balance) in Map.entries(neuronRewardBalances)) {
      total += balance;
    };
    total
  };

  // Get available balance for distribution (canister balance minus current neuron balances)
  public func getAvailableBalance() : async Nat {
    let canisterBalance = await getTacoBalance();
    let currentNeuronBalances = await getCurrentTotalNeuronBalances();
    
    if (canisterBalance >= currentNeuronBalances) {
      canisterBalance - currentNeuronBalances
    } else {
      // This should not happen in normal operation
      logger.error("Balance Check", "Canister balance (" # Nat.toText(canisterBalance) # ") is less than neuron balances (" # Nat.toText(currentNeuronBalances) # ")", "getAvailableBalance");
      0
    }
  };



  public shared ({ caller = _ }) func getCanisterStatus() : async {
    neuronAllocationArchiveId: Principal;
    priceArchiveId: Principal;
    daoId: Principal;
    environment: Text;
    distributionStatus: {
      inProgress: Bool;
      lastDistribution: Int;
      nextDistribution: Int;
      totalDistributions: Nat;
      totalRewardsDistributed: Nat; // Total TACO tokens distributed
    };
  } {
    // Calculate total rewards distributed
    var totalRewards : Nat = 0;
    for (record in Vector.vals(distributionHistory)) {
      switch (record.status) {
        case (#Completed) {
          totalRewards += record.totalRewardPot;
        };
        case (#PartiallyCompleted(_)) {
          // Include partially completed distributions in total
          totalRewards += record.totalRewardPot;
        };
        case _ { };
      };
    };

    {
      neuronAllocationArchiveId = DAO_NEURON_ALLOCATION_ARCHIVE_ID;
      priceArchiveId = PRICE_ARCHIVE_ID;
      daoId = DAO_ID;
      environment = switch (canister_ids.getEnvironment()) {
        case (#Staging) { "staging" };
        case (#Production) { "production" };
      };
      distributionStatus = {
        inProgress = switch (currentDistributionId) { case (?_) { true }; case null { false } };
        lastDistribution = lastDistributionTime;
        nextDistribution = lastDistributionTime + distributionPeriodNS;
        totalDistributions = Vector.size(distributionHistory);
        totalRewardsDistributed = totalRewards;
      };
    };
  };
}

