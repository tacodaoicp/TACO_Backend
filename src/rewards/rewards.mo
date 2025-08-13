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
import Text "mo:base/Text";

import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";

shared (deployer) persistent actor class Rewards() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Logger instance
  private transient var logger = Logger.Logger();

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
    #DistributionInProgress;
    #InsufficientRewardPot;
    #NotAuthorized;
  };

  // Weekly distribution types
  public type NeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Float;
  };

  public type DistributionRecord = {
    id: Nat;
    startTime: Int;
    endTime: Int;
    distributionTime: Int;
    totalRewardPot: Float;
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [NeuronReward];
    status: DistributionStatus;
  };

  public type DistributionStatus = {
    #InProgress: {currentNeuron: Nat; totalNeurons: Nat};
    #Completed;
    #Failed: Text;
  };

  public type DistributionError = {
    #SystemError: Text;
    #InProgress;
    #NotAuthorized;
  };

  // Configuration
  stable var distributionPeriodNS : Nat = 604_800_000_000_000; // 7 days in nanoseconds
  stable var weeklyRewardPot : Float = 1000.0; // Default reward pot
  stable var maxDistributionHistory : Nat = 52; // Keep 1 year of weekly distributions
  stable var distributionEnabled : Bool = true;

  // Distribution state
  stable var distributionCounter : Nat = 0;
  stable var currentDistributionId : ?Nat = null;
  stable var lastDistributionTime : Int = 0;
  private transient var distributionTimerId : ?Nat = null;
  
  // Reward tracking
  private transient let { phash; bhash } = Map;
  stable var neuronRewardBalances = Map.new<Blob, Float>(); // neuronId -> accumulated rewards
  
  // Distribution history (circular buffer using Vector)
  private transient let distributionHistory = Vector.new<DistributionRecord>();

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
    neuronId: Blob;
    allocations: [Allocation];
    votingPower: Nat;
    lastAllocationUpdate: Int;
    pastAllocations: [Allocation];
    maker: Principal;
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
    
    // Track asset values between checkpoints
    var assetValues = Buffer.Buffer<(Principal, Float)>(10); // (token, current_value)
    var previousPrices = Buffer.Buffer<(Principal, Float)>(10); // (token, price)
    
    for (i in timeline.keys()) {
      let (timestamp, allocations) = timeline[i];
      
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
  // Weekly Distribution System
  //=========================================================================

  // Start the recurring distribution timer
  public shared ({ caller }) func startDistributionTimer() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    // Cancel existing timer if any
    switch (distributionTimerId) {
      case (?id) { Timer.cancelTimer(id); };
      case null { };
    };

    // Start new timer
    await* scheduleNextDistribution<system>();
    
    logger.info("Distribution", "Distribution timer started with period: " # Nat.toText(distributionPeriodNS / 1_000_000_000) # "s", "startDistributionTimer");
    #ok("Distribution timer started");
  };

  // Stop the distribution timer
  public shared ({ caller }) func stopDistributionTimer() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    switch (distributionTimerId) {
      case (?id) {
        Timer.cancelTimer(id);
        distributionTimerId := null;
        logger.info("Distribution", "Distribution timer stopped", "stopDistributionTimer");
        #ok("Distribution timer stopped");
      };
      case null {
        #ok("No timer was running");
      };
    };
  };

  // Schedule the next distribution
  private func scheduleNextDistribution<system>() : async* () {
    if (not distributionEnabled) {
      return;
    };

    distributionTimerId := ?Timer.setTimer<system>(
      #nanoseconds(distributionPeriodNS),
      func() : async () {
        try {
          await* runWeeklyDistribution<system>();
          // Schedule next distribution
          await* scheduleNextDistribution<system>();
        } catch (error) {
          logger.error("Distribution", "Error in scheduled distribution: " # "Error occurred", "scheduleNextDistribution");
          // Still schedule next attempt
          await* scheduleNextDistribution<system>();
        };
      }
    );
  };

  // Main distribution function
  private func runWeeklyDistribution<system>() : async* () {
    // Check if distribution is already in progress
    switch (currentDistributionId) {
      case (?_) {
        logger.warn("Distribution", "Distribution already in progress, skipping", "runWeeklyDistribution");
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
      totalRewardPot = weeklyRewardPot;
      totalRewardScore = 0.0;
      neuronsProcessed = 0;
      neuronRewards = [];
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

    // Get all neurons from DAO
    try {
      let neuronsResult = await daoCanister.admin_getNeuronAllocations();
      let neurons = neuronsResult;
      
      logger.info("Distribution", "Found " # Nat.toText(neurons.size()) # " neurons to process", "runWeeklyDistribution");
      
      if (neurons.size() == 0) {
        await* completeDistribution(distributionCounter, [], 0.0, "No neurons found");
        return;
      };

      // Update status with total neuron count
      let updatedRecord = {
        initialRecord with
        status = #InProgress({currentNeuron = 0; totalNeurons = neurons.size()});
      };
      Vector.put(distributionHistory, Vector.size(distributionHistory) - 1, updatedRecord);

      // Start processing neurons one by one
      await* processNeuronsSequentially<system>(distributionCounter, neurons, 0, [], 0.0, startTime, endTime, priceType);
      
    } catch (error) {
      logger.error("Distribution", "Failed to get neurons: " # "Error occurred", "runWeeklyDistribution");
      await* completeDistribution(distributionCounter, [], 0.0, "Failed to get neurons: " # "Error occurred");
    };
  };

  // Process neurons sequentially using timer pattern to avoid timeouts
  private func processNeuronsSequentially<system>(
    distributionId: Nat,
    neurons: [(Blob, NeuronAllocation)],
    currentIndex: Nat,
    neuronRewards: [NeuronReward],
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
      await* calculateAndDistributeRewards(distributionId, neuronRewards, totalRewardScore);
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
          let rewardScore = performance.performanceScore * Float.fromInt(votingPower);
          
          let neuronReward : NeuronReward = {
            neuronId = neuronId;
            performanceScore = performance.performanceScore;
            votingPower = votingPower;
            rewardScore = rewardScore;
            rewardAmount = 0.0; // Will be calculated later
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
                updatedTotalScore,
                startTime,
                endTime,
                priceType
              );
            }
          );
        };
        case (#err(error)) {
          logger.warn("Distribution", "Failed to calculate performance for neuron: " # "Error occurred", "processNeuronsSequentially");
          
          // Continue with next neuron
          ignore Timer.setTimer<system>(
            #nanoseconds(0),
            func() : async () {
              await* processNeuronsSequentially<system>(
                distributionId,
                neurons,
                currentIndex + 1,
                neuronRewards,
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
      logger.error("Distribution", "Error processing neuron: " # "Error occurred", "processNeuronsSequentially");
      
      // Continue with next neuron
      ignore Timer.setTimer<system>(
        #nanoseconds(0),
        func() : async () {
          await* processNeuronsSequentially<system>(
            distributionId,
            neurons,
            currentIndex + 1,
            neuronRewards,
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
    totalRewardScore: Float
  ) : async* () {
    
    if (totalRewardScore == 0.0) {
      await* completeDistribution(distributionId, neuronRewards, totalRewardScore, "No valid reward scores");
      return;
    };

    // Calculate individual reward amounts
    let finalRewards = Array.map<NeuronReward, NeuronReward>(neuronRewards, func(reward) {
      let rewardAmount = (reward.rewardScore / totalRewardScore) * weeklyRewardPot;
      { reward with rewardAmount = rewardAmount }
    });

    // Credit rewards to neuron balances
    for (reward in finalRewards.vals()) {
      let currentBalance = switch (Map.get(neuronRewardBalances, bhash, reward.neuronId)) {
        case (?balance) { balance };
        case null { 0.0 };
      };
      Map.set(neuronRewardBalances, bhash, reward.neuronId, currentBalance + reward.rewardAmount);
    };

    await* completeDistribution(distributionId, finalRewards, totalRewardScore, "");
    
    logger.info("Distribution", "Distribution " # Nat.toText(distributionId) # " completed. Processed " # Nat.toText(finalRewards.size()) # " neurons, distributed " # Float.toText(weeklyRewardPot) # " rewards", "calculateAndDistributeRewards");
  };

  // Complete the distribution and update records
  private func completeDistribution(
    distributionId: Nat,
    neuronRewards: [NeuronReward],
    totalRewardScore: Float,
    errorMessage: Text
  ) : async* () {
    
    currentDistributionId := null;
    lastDistributionTime := Time.now();

    // Update the distribution record
    let historyIndex = Vector.size(distributionHistory) - 1;
    switch (Vector.get(distributionHistory, historyIndex)) {
      case (record) {
        let status = if (errorMessage == "") {
          #Completed
        } else {
          #Failed(errorMessage)
        };

        let finalRecord = {
          record with
          totalRewardScore = totalRewardScore;
          neuronsProcessed = neuronRewards.size();
          neuronRewards = neuronRewards;
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
          await* runWeeklyDistribution<system>();
          #ok("Distribution triggered successfully");
        } catch (error) {
          #err(#SystemError("Failed to trigger distribution: " # "Error occurred"));
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
          #err(#SystemError("Failed to trigger custom distribution: " # "Error occurred"));
        };
      };
    };
  };

  //=========================================================================
  // Reward Claim Functions
  //=========================================================================

  // Get neuron reward balance
  public query func getNeuronRewardBalance(neuronId: Blob) : async Float {
    switch (Map.get(neuronRewardBalances, bhash, neuronId)) {
      case (?balance) { balance };
      case null { 0.0 };
    };
  };

  // Get all neuron reward balances (admin only)
  public shared query ({ caller }) func getAllNeuronRewardBalances() : async [(Blob, Float)] {
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

  // Set weekly reward pot
  public shared ({ caller }) func setWeeklyRewardPot(amount: Float) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    weeklyRewardPot := amount;
    logger.info("Config", "Weekly reward pot set to " # Float.toText(amount), "setWeeklyRewardPot");
    #ok("Weekly reward pot updated");
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

  // Get configuration
  public query func getConfiguration() : async {
    distributionPeriodNS: Nat;
    weeklyRewardPot: Float;
    maxDistributionHistory: Nat;
    distributionEnabled: Bool;
  } {
    {
      distributionPeriodNS = distributionPeriodNS;
      weeklyRewardPot = weeklyRewardPot;
      maxDistributionHistory = maxDistributionHistory;
      distributionEnabled = distributionEnabled;
    };
  };

  private func isAdmin(caller: Principal) : Bool {
    AdminAuth.isAdmin(caller, canister_ids.isKnownCanister)
  };

  //=========================================================================
  // Admin Functions
  //=========================================================================

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
      totalRewardsDistributed: Float;
    };
  } {
    // Calculate total rewards distributed
    var totalRewards : Float = 0.0;
    for (record in Vector.vals(distributionHistory)) {
      switch (record.status) {
        case (#Completed) {
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
