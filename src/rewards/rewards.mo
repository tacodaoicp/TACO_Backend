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
import Bool "mo:base/Bool";
import Blob "mo:base/Blob";
import Option "mo:base/Option";
import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import ICRC "../helper/icrc.types";
import NeuronSnapshot "../neuron_snapshot/ns_types";
import Cycles "mo:base/ExperimentalCycles";
import Char "mo:base/Char";
import Service "mo:icrc3-mo/service";
//import Migration "migration";

//(with migration = Migration.migrate)
shared (deployer) persistent actor class Rewards() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Logger instance
  private transient var logger = Logger.Logger();

  // TACO token constants
  private  let TACO_DECIMALS : Nat = 8;
  private  let TACO_SATOSHIS_PER_TOKEN : Nat = 100_000_000; // 10^8
  private  let TACO_LEDGER_CANISTER_ID : Text = "kknbx-zyaaa-aaaaq-aae4a-cai";
  private  let TACO_WITHDRAWAL_FEE : Nat = 10_000; // 0.0001 TACO in satoshis

  private  let SNS_GOVERNANCE_CANISTER_ID : Principal = Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai");

  // ICP token principal for price derivation
  private let ICP_PRINCIPAL : Principal = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");

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
    reason: ?Text; // The note/reason for this allocation change
  };

  // Extended checkpoint data with ICP portfolio value for graph rendering
  public type GraphCheckpointData = {
    timestamp: Int;
    allocations: [Allocation];
    tokenValues: [(Principal, Float)];
    totalPortfolioValue: Float; // Cumulative USD-scaled value
    totalPortfolioValueICP: Float; // Cumulative ICP-interpolated value
    pricesUsed: [(Principal, PriceInfo)];
    maker: ?Principal;
    reason: ?Text;
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
    #InvalidDisplayName: Text;
  };

  // Periodic distribution types
  public type NeuronReward = {
    neuronId: Blob;
    performanceScore: Float;           // USD-based performance (backward compatible)
    performanceScoreICP: ?Float;       // ICP-based performance (new, optional for migration)
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

  // Backfill types for populating historical performance data
  public type BackfillConfig = {
    startTime: Int;           // Earliest time to backfill from
    periodDays: Nat;          // Days per distribution period (default 7)
    maxPeriods: Nat;          // Max periods to backfill (default 52)
    clearExisting: Bool;      // Whether to clear existing history first
    skipExistingPeriods: Bool; // Whether to skip periods that already exist in history
  };

  public type BackfillResult = {
    periodsCreated: Nat;
    neuronsProcessed: Nat;
    totalNeuronRewards: Nat;
    errors: [Text];
    startTime: Int;
    endTime: Int;
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

  // Leaderboard types
  public type LeaderboardTimeframe = {
    #OneWeek;      // Latest distribution period (7 days)
    #OneMonth;     // Average of last ~4 distributions (28 days)
    #OneYear;      // Average of last 52 distributions (364 days)
    #AllTime;      // Compound return across all distributions
  };

  public type LeaderboardPriceType = {
    #USD;
    #ICP;
  };

  public type LeaderboardEntry = {
    rank: Nat;                          // 1-100
    principal: Principal;               // User principal
    neuronId: Blob;                     // Best performing neuron for this timeframe
    performanceScore: Float;            // Performance score for this timeframe/priceType
    distributionsCount: Nat;            // How many distributions in this timeframe
    lastActivity: Int;                  // Timestamp of last allocation change
    displayName: ?Text;                 // Optional user-set display name
  };

  // Individual neuron/user performance lookup types
  public type NeuronPerformanceDetail = {
    neuronId: Blob;
    votingPower: Nat;
    performance: {
      oneWeekUSD: ?Float;
      oneWeekICP: ?Float;
      oneMonthUSD: ?Float;
      oneMonthICP: ?Float;
      oneYearUSD: ?Float;
      oneYearICP: ?Float;
      allTimeUSD: ?Float;
      allTimeICP: ?Float;
    };
    distributionsParticipated: Nat;
    lastAllocationChange: Int;
  };

  public type UserPerformanceResult = {
    principal: Principal;
    neurons: [NeuronPerformanceDetail];
    aggregatedPerformance: {
      oneWeekUSD: ?Float;
      oneWeekICP: ?Float;
      oneMonthUSD: ?Float;
      oneMonthICP: ?Float;
      oneYearUSD: ?Float;
      oneYearICP: ?Float;
      allTimeUSD: ?Float;
      allTimeICP: ?Float;
    };
    totalVotingPower: Nat;
    distributionsParticipated: Nat;
    lastActivity: Int;
  };

  // Graph data type for performance visualization (per-neuron)
  public type NeuronPerformanceGraphData = {
    neuronId: Blob;
    timeframe: { startTime: Int; endTime: Int };
    checkpoints: [CheckpointData];  // Contains timestamp, allocations, tokenValues, pricesUsed, maker
    performanceScoreUSD: Float;
    performanceScoreICP: ?Float;
  };

  // Graph data type for user performance visualization (aggregated across neurons)
  public type NeuronGraphData = {
    neuronId: Blob;
    checkpoints: [GraphCheckpointData];
    performanceScoreUSD: Float;
    performanceScoreICP: ?Float;
  };

  // Extended graph data type with per-timeframe scores for each neuron
  public type NeuronGraphDataExtended = {
    neuronId: Blob;
    checkpoints: [GraphCheckpointData];
    performanceScoreUSD: Float;
    performanceScoreICP: ?Float;
    oneWeekUSD: ?Float;
    oneWeekICP: ?Float;
    oneMonthUSD: ?Float;
    oneMonthICP: ?Float;
    oneYearUSD: ?Float;
    oneYearICP: ?Float;
    allocationChangeCount: Nat;
  };

  public type UserPerformanceGraphData = {
    timeframe: { startTime: Int; endTime: Int };  // Earliest & latest checkpoint timestamps across all neurons
    allocationNeuronId: ?Blob;         // Neuron with most allocation changes (for tooltip display)
    aggregatedPerformanceUSD: Float;   // Compound performance across all neurons
    aggregatedPerformanceICP: ?Float;
    neurons: [NeuronGraphDataExtended];  // All neurons with full data
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

  // Backfill state
  stable var backfillInProgress : Bool = false;
  stable var backfillPeriodsCompleted : Nat = 0;
  stable var backfillTotalPeriods : Nat = 0;
  stable var backfillStartedAt : Int = 0;
  stable var backfillCurrentPeriodStart : Int = 0;
  stable var backfillCurrentPeriodEnd : Int = 0;
  stable var backfillDataStartTime : Int = 0;  // The startTime from config
  stable var backfillDataEndTime : Int = 0;    // Will be set to now()
  stable var backfillLastErrors : [Text] = []; // Last 5 errors for debugging

  // DEPRECATED: Legacy skiplist - kept for migration compatibility, no longer used
  // Use rewardPenalties instead (with multiplier=0 for skip behavior)
  stable var rewardSkipList : [Blob] = [];

  // Reward penalties for distribution - neurons with reduced reward scores
  // multiplier = 0 means skip entirely (same as old skiplist)
  // multiplier = 23 means keep 23% of reward score
  // multiplier = 100 or not in map means no penalty
  stable var rewardPenalties = Map.new<Blob, Nat>(); // neuronId -> multiplier (0-100)
  
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

  // Leaderboard storage (8 pre-computed leaderboards)
  stable var leaderboards: {
    oneWeekUSD: [LeaderboardEntry];
    oneWeekICP: [LeaderboardEntry];
    oneMonthUSD: [LeaderboardEntry];
    oneMonthICP: [LeaderboardEntry];
    oneYearUSD: [LeaderboardEntry];
    oneYearICP: [LeaderboardEntry];
    allTimeUSD: [LeaderboardEntry];
    allTimeICP: [LeaderboardEntry];
  } = {
    oneWeekUSD = [];
    oneWeekICP = [];
    oneMonthUSD = [];
    oneMonthICP = [];
    oneYearUSD = [];
    oneYearICP = [];
    allTimeUSD = [];
    allTimeICP = [];
  };

  stable var leaderboardLastUpdate: Int = 0;
  stable var leaderboardSize: Nat = 50;                // Top N per leaderboard
  stable var leaderboardUpdateEnabled: Bool = true;    // On/off switch
  stable var leaderboardTimerId: ?Nat = null;          // Timer ID for recurring leaderboard updates
  stable var leaderboardCutoffDate: Int = 1767225600000000000;  // Cutoff date: Jan 1, 2026 00:00:00 UTC (in nanoseconds)
  let LEADERBOARD_REFRESH_INTERVAL_NS: Nat = 2 * 60 * 60 * 1_000_000_000; // 2 hours in nanoseconds

  // Display name system
  stable var displayNames = Map.new<Principal, Text>();       // principal -> display name
  stable var bannedWords : [Text] = [];                       // stored normalized (lowercase, alpha-only)

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
    penaltyMultiplier: ?Nat; // null or ?100 = no penalty, ?23 = 77% penalty
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
  // Note: Archive methods are query methods (same subnet, composite query compatible)
  type NeuronAllocationArchive = actor {
    getNeuronAllocationChangesByNeuronInTimeRange: query (Blob, Int, Int, Nat) -> async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError>;
    getNeuronAllocationChangesWithContext: query (Blob, Int, Int, Nat) -> async Result.Result<{
      preTimespanAllocation: ?NeuronAllocationChangeBlockData;
      inTimespanChanges: [NeuronAllocationChangeBlockData];
    }, ArchiveError>;
    // Bulk query for multiple neurons - optimized for backfill operations
    getAllNeuronsAllocationChangesInTimeRange: query (Int, Int, Nat, Nat) -> async Result.Result<{
      neurons: [(Blob, {
        preTimespanAllocation: ?NeuronAllocationChangeBlockData;
        inTimespanChanges: [NeuronAllocationChangeBlockData];
      })];
      totalNeurons: Nat;
      hasMore: Bool;
    }, ArchiveError>;
  };

  type PriceArchive = actor {
    getPriceAtTime: query (Principal, Int) -> async Result.Result<?PriceInfo, ArchiveError>;
    getPricesAtTime: query ([Principal], Int) -> async Result.Result<[(Principal, ?PriceInfo)], ArchiveError>;
    getPriceAtOrAfterTime: query (Principal, Int) -> async Result.Result<?PriceInfo, ArchiveError>;
  };

  type NeuronAllocation = {
    allocations: [Allocation];
    lastUpdate: Int;
    votingPower: Nat;
    lastAllocationMaker: Principal;
  };

  type DAOCanister = actor {
    admin_getNeuronAllocations: () -> async [(Blob, NeuronAllocation)];
    getAllNeuronOwners: query () -> async [(Blob, [Principal])];
    getActiveDecisionMakers: query () -> async [(Blob, [Principal])];  // Only active makers, excludes passive hotkeys
    admin_getAllActiveNeuronIds: query () -> async [Blob];
    getNeuronAllocation: query (Blob) -> async ?NeuronAllocation;
  };

  private transient let neuronAllocationArchive : NeuronAllocationArchive = actor (Principal.toText(DAO_NEURON_ALLOCATION_ARCHIVE_ID));
  private transient let priceArchive : PriceArchive = actor (Principal.toText(PRICE_ARCHIVE_ID));
  private transient let daoCanister : DAOCanister = actor (Principal.toText(DAO_ID));

  //=========================================================================
  // Main Performance Calculation Method
  //=========================================================================

  // Note: This is a shared (update) function because it's called internally
  // by the distribution system. For external callers who just want to query
  // performance, use calculateNeuronPerformanceQuery which is a composite query.
  public shared ({ caller }) func calculateNeuronPerformance(
    neuronId: Blob,
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async Result.Result<PerformanceResult, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };
    
    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };

    // Get allocation data from the neuron allocation archive
    let allocationResult = await (with timeout = 65)  neuronAllocationArchive.getNeuronAllocationChangesWithContext(
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
            // No archive allocation data found - fallback to current DAO allocation
            let currentAlloc = await (with timeout = 65)  daoCanister.getNeuronAllocation(neuronId);
            switch (currentAlloc) {
              case (?alloc) {
                if (alloc.allocations.size() > 0) {
                  alloc.allocations  // Use current allocation as baseline
                } else {
                  return #err(#NeuronNotFound);
                }
              };
              case (null) { return #err(#NeuronNotFound); };
            };
          };
        };
      };
    };

    // Build timeline of allocation changes with makers and reasons
    let timelineBuffer = Buffer.Buffer<(Int, [Allocation], ?Principal, ?Text)>(10);

    // For start time, get maker and reason from preTimespanAllocation if available
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
    let startReason : ?Text = switch (allocationData.preTimespanAllocation) {
      case (?preAlloc) { preAlloc.reason };
      case (null) {
        switch (Array.find(allocationData.inTimespanChanges, func(change: NeuronAllocationChangeBlockData) : Bool {
          change.timestamp == startTime
        })) {
          case (?exactChange) { exactChange.reason };
          case (null) { null };
        };
      };
    };
    timelineBuffer.add((startTime, startAllocation, startMaker, startReason));

    // Add all in-timespan changes with their makers and reasons
    for (change in allocationData.inTimespanChanges.vals()) {
      timelineBuffer.add((change.timestamp, change.newAllocations, ?change.maker, change.reason));
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
      // For end time, use the maker and reason from the last change
      let endMaker = if (timelineBuffer.size() == 0) {
        startMaker
      } else {
        timelineBuffer.get(timelineBuffer.size() - 1).2
      };
      let endReason : ?Text = if (timelineBuffer.size() == 0) {
        startReason
      } else {
        timelineBuffer.get(timelineBuffer.size() - 1).3
      };
      timelineBuffer.add((endTime, endAllocations, endMaker, endReason));
    };

    let timeline = Buffer.toArray(timelineBuffer);

    // Calculate checkpoints for each point in timeline
    let checkpointsBuffer = Buffer.Buffer<CheckpointData>(timeline.size());
    
    // Track asset values between checkpoints
    var assetValues = Buffer.Buffer<(Principal, Float)>(10); // (token, current_value)
    var previousPrices = Buffer.Buffer<(Principal, Float)>(10); // (token, price)
    
    for (i in timeline.keys()) {
      let (timestamp, allocations, maker, reason) = timeline[i];

      // Get prices for all tokens at this timestamp
      let tokenPricesBuffer = Buffer.Buffer<(Principal, PriceInfo)>(allocations.size());
      let tokenValuesBuffer = Buffer.Buffer<(Principal, Float)>(allocations.size());
      
      if (i == 0) {
        // First checkpoint: Initialize with 1.0 total value, distributed by allocation
        var totalValue : Float = 1.0;
        
        // Collect all tokens for batch price request
        let tokens = Array.map<Allocation, Principal>(allocations, func(allocation) { allocation.token });
        
        // Get prices for all tokens at once
        let batchPriceResult = await (with timeout = 65)  priceArchive.getPricesAtTime(tokens, timestamp);
        let tokenPrices = switch (batchPriceResult) {
          case (#ok(prices)) { prices };
          case (#err(_)) {
            return #err(#SystemError("Failed to get price data"));
          };
        };
        
        // Process each allocation with its corresponding price, with fallback behavior
        for (allocation in allocations.vals()) {
          // Find the price for this token in batch result
          let priceEntry = Array.find<(Principal, ?PriceInfo)>(tokenPrices, func((token, _)) {
            Principal.equal(token, allocation.token)
          });

          var priceOpt : ?PriceInfo = null;
          switch (priceEntry) {
            case (?( _, ?pi)) { priceOpt := ?pi; };
            case _ {
              // Fallback 1: try to find closest price after timestamp
              let futureRes = await (with timeout = 65)  priceArchive.getPriceAtOrAfterTime(allocation.token, timestamp);
              switch (futureRes) {
                case (#ok(?futurePrice)) { priceOpt := ?futurePrice; };
                case _ { priceOpt := null; };
              };
            };
          };

          switch (priceOpt) {
            case (?priceInfo) {
              let tokenPrice = getPriceValue(priceInfo, priceType);
              let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
              let tokenValue = totalValue * allocationPercent; // Just allocation percentage of 1.0

              tokenPricesBuffer.add((allocation.token, priceInfo));
              tokenValuesBuffer.add((allocation.token, tokenValue));

              // Store initial asset values and prices
              assetValues.add((allocation.token, tokenValue));
              previousPrices.add((allocation.token, tokenPrice));
            };
            case null {
              // Fallback 2: Skip this token entirely if no price in past or future
              // Do nothing for buffers; token is excluded from initial checkpoint
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
          reason = reason;
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
        let batchPriceResult = await (with timeout = 65)  priceArchive.getPricesAtTime(allTokens, timestamp);
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
        let updatedPrices = Buffer.Buffer<(Principal, Float)>(assetValues.size());

        // Build a lookup for old prices by token to avoid index mismatch
        let oldPriceByToken = Buffer.toArray(previousPrices);

        // Helper to find old price by token
        let findOldPrice = func(token: Principal) : ?Float {
          let entry = Array.find<(Principal, Float)>(oldPriceByToken, func((t, _)) { Principal.equal(t, token) });
          switch (entry) {
            case (?(t, p)) { ?p };
            case null { null };
          }
        };

        // Only iterate if we have assets to update
        if (assetValues.size() > 0) {
          for (j in Iter.range(0, assetValues.size() - 1)) {
            let (token, oldValue) = assetValues.get(j);

            // Look up the old price for this token; if missing, treat as missing price data
            switch (findOldPrice(token)) {
              case (?oldPrice) {
                // Get new price for this token from batch result, with fallback to future
                var priceInfoOpt : ?PriceInfo = findPrice(token);
                if (priceInfoOpt == null) {
                  let futureResStep1 = await (with timeout = 65)  priceArchive.getPriceAtOrAfterTime(token, timestamp);
                  switch (futureResStep1) {
                    case (#ok(?futurePrice)) { priceInfoOpt := ?futurePrice; };
                    case _ {};
                  };
                };

                switch (priceInfoOpt) {
                  case (?priceInfo) {
                    let newPrice = getPriceValue(priceInfo, priceType);
                    let priceRatio = newPrice / oldPrice;
                    let newValue = oldValue * priceRatio;

                    updatedAssetValues.add((token, newValue));
                    updatedPrices.add((token, newPrice));
                  };
                  case null {
                    // Fallback 2: Skip this token entirely if no price in past or future
                  };
                };
              };
              case null {
                // If we had no previous price for this token (newly added asset), carry over its current value unchanged
                // using the price at this checkpoint to populate updatedPrices.
                var priceInfoOpt2 : ?PriceInfo = findPrice(token);
                if (priceInfoOpt2 == null) {
                  let futureResNew = await (with timeout = 65)  priceArchive.getPriceAtOrAfterTime(token, timestamp);
                  switch (futureResNew) {
                    case (#ok(?futurePrice)) { priceInfoOpt2 := ?futurePrice; };
                    case _ {};
                  };
                };
                switch (priceInfoOpt2) {
                  case (?priceInfo) {
                    let newPrice = getPriceValue(priceInfo, priceType);
                    let newValue = oldValue; // no prior price to ratio against; keep value until rebalance step
                    updatedAssetValues.add((token, newValue));
                    updatedPrices.add((token, newPrice));
                  };
                  case null {
                    // Fallback 2: Skip this token entirely if no price in past or future
                  };
                };
              };
            };
          };
        };
        
        // Step 2: Calculate total portfolio value after price changes
        var totalValueAfterPriceChanges : Float = 0.0;
        if (updatedAssetValues.size() > 0) {
          for (j in Iter.range(0, updatedAssetValues.size() - 1)) {
            let (_, value) = updatedAssetValues.get(j);
            totalValueAfterPriceChanges += value;
          };
        } else {
          // If no previous assets, maintain portfolio value of 1.0 for proper rebalancing
          totalValueAfterPriceChanges := 1.0;
        };
        
        // Step 3: Rebalance to new allocations using the updated total value
        for (allocation in allocations.vals()) {
          let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
          let tokenValue = totalValueAfterPriceChanges * allocationPercent;

          // Get price info for this token from batch result, with same fallback rules
          var priceInfoOpt : ?PriceInfo = findPrice(allocation.token);
          if (priceInfoOpt == null) {
            let futureRes2 = await (with timeout = 65)  priceArchive.getPriceAtOrAfterTime(allocation.token, timestamp);
            switch (futureRes2) {
              case (#ok(?futurePrice)) { priceInfoOpt := ?futurePrice; };
              case _ {};
            };
          };

          switch (priceInfoOpt) {
            case (?priceInfo) {
              tokenPricesBuffer.add((allocation.token, priceInfo));
              tokenValuesBuffer.add((allocation.token, tokenValue));
            };
            case null {
              // Fallback 2: Skip this token if no price in past or future
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
          reason = reason;
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

  // Composite query version for external callers - significantly cheaper (~0 cycles)
  // Uses the same logic but as a composite query that can call archive query methods
  public composite query func calculateNeuronPerformanceQuery(
    neuronId: Blob,
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async Result.Result<PerformanceResult, RewardsError> {

    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };

    // Get allocation data from the neuron allocation archive
    let allocationResult = await (with timeout = 30)  neuronAllocationArchive.getNeuronAllocationChangesWithContext(
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
        switch (Array.find(allocationData.inTimespanChanges, func(change: NeuronAllocationChangeBlockData) : Bool {
          change.timestamp == startTime
        })) {
          case (?exactChange) { exactChange.oldAllocations };
          case (null) {
            return #err(#NeuronNotFound);
          };
        };
      };
    };

    // Build timeline of allocation changes with makers and reasons
    let timelineBuffer = Buffer.Buffer<(Int, [Allocation], ?Principal, ?Text)>(10);

    let startMaker = switch (allocationData.preTimespanAllocation) {
      case (?preAlloc) { ?preAlloc.maker };
      case (null) { null };
    };
    let startReason : ?Text = switch (allocationData.preTimespanAllocation) {
      case (?preAlloc) { preAlloc.reason };
      case (null) { null };
    };
    timelineBuffer.add((startTime, startAllocation, startMaker, startReason));

    for (change in allocationData.inTimespanChanges.vals()) {
      timelineBuffer.add((change.timestamp, change.newAllocations, ?change.maker, change.reason));
    };

    timelineBuffer.add((endTime, [], null, null));

    let timeline = Buffer.toArray(timelineBuffer);

    // Process each period between allocation changes
    let checkpointsBuffer = Buffer.Buffer<CheckpointData>(timeline.size());
    var assetValues = Buffer.Buffer<(Principal, Float)>(10);
    var previousPrices = Buffer.Buffer<(Principal, Float)>(10);

    for (i in Iter.range(0, timeline.size() - 2)) {
      let (timestamp, allocations, maker, reason) = timeline[i];
      let (nextTimestamp, _, _, _) = timeline[i + 1];

      // Collect all unique tokens
      let allTokensBuffer = Buffer.Buffer<Principal>(10);
      let existingTokens : [Principal] = Array.map<(Principal, Float), Principal>(Buffer.toArray(assetValues), func((t, _)) { t });
      for (token in existingTokens.vals()) { allTokensBuffer.add(token); };
      let newTokens : [Principal] = Array.map<Allocation, Principal>(allocations, func(a) { a.token });
      for (token in newTokens.vals()) {
        let isDuplicate = Array.find<Principal>(existingTokens, func(t) { Principal.equal(t, token) });
        switch (isDuplicate) {
          case null { allTokensBuffer.add(token); };
          case _ {};
        };
      };
      let allTokens = Buffer.toArray(allTokensBuffer);

      // Get prices for all tokens at once
      let batchPriceResult = await (with timeout = 30)  priceArchive.getPricesAtTime(allTokens, timestamp);
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

      let tokenPricesBuffer = Buffer.Buffer<(Principal, PriceInfo)>(allocations.size());
      let tokenValuesBuffer = Buffer.Buffer<(Principal, Float)>(allocations.size());

      // Update asset values based on price changes
      let updatedAssetValues = Buffer.Buffer<(Principal, Float)>(assetValues.size());
      let updatedPrices = Buffer.Buffer<(Principal, Float)>(assetValues.size());
      let oldPriceByToken = Buffer.toArray(previousPrices);

      let findOldPrice = func(token: Principal) : ?Float {
        let entry = Array.find<(Principal, Float)>(oldPriceByToken, func((t, _)) { Principal.equal(t, token) });
        switch (entry) {
          case (?(t, p)) { ?p };
          case null { null };
        }
      };

      if (assetValues.size() > 0) {
        for (j in Iter.range(0, assetValues.size() - 1)) {
          let (token, oldValue) = assetValues.get(j);

          switch (findOldPrice(token)) {
            case (?oldPrice) {
              var priceInfoOpt : ?PriceInfo = findPrice(token);
              if (priceInfoOpt == null) {
                let futureResStep1 = await (with timeout = 30)  priceArchive.getPriceAtOrAfterTime(token, timestamp);
                switch (futureResStep1) {
                  case (#ok(?futurePrice)) { priceInfoOpt := ?futurePrice; };
                  case _ {};
                };
              };

              switch (priceInfoOpt) {
                case (?priceInfo) {
                  let newPrice = getPriceValue(priceInfo, priceType);
                  let priceRatio = newPrice / oldPrice;
                  let newValue = oldValue * priceRatio;
                  updatedAssetValues.add((token, newValue));
                  updatedPrices.add((token, newPrice));
                };
                case null {};
              };
            };
            case null {
              var priceInfoOpt2 : ?PriceInfo = findPrice(token);
              if (priceInfoOpt2 == null) {
                let futureResNew = await (with timeout = 30)  priceArchive.getPriceAtOrAfterTime(token, timestamp);
                switch (futureResNew) {
                  case (#ok(?futurePrice)) { priceInfoOpt2 := ?futurePrice; };
                  case _ {};
                };
              };
              switch (priceInfoOpt2) {
                case (?priceInfo) {
                  let newPrice = getPriceValue(priceInfo, priceType);
                  let newValue = oldValue;
                  updatedAssetValues.add((token, newValue));
                  updatedPrices.add((token, newPrice));
                };
                case null {};
              };
            };
          };
        };
      };

      // Calculate total portfolio value after price changes
      var totalValueAfterPriceChanges : Float = 0.0;
      if (updatedAssetValues.size() > 0) {
        for (j in Iter.range(0, updatedAssetValues.size() - 1)) {
          let (_, value) = updatedAssetValues.get(j);
          totalValueAfterPriceChanges += value;
        };
      } else {
        totalValueAfterPriceChanges := 1.0;
      };

      // Rebalance to new allocations
      for (allocation in allocations.vals()) {
        let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
        let tokenValue = totalValueAfterPriceChanges * allocationPercent;

        var priceInfoOpt : ?PriceInfo = findPrice(allocation.token);
        if (priceInfoOpt == null) {
          let futureRes2 = await (with timeout = 30)  priceArchive.getPriceAtOrAfterTime(allocation.token, timestamp);
          switch (futureRes2) {
            case (#ok(?futurePrice)) { priceInfoOpt := ?futurePrice; };
            case _ {};
          };
        };

        switch (priceInfoOpt) {
          case (?priceInfo) {
            tokenPricesBuffer.add((allocation.token, priceInfo));
            tokenValuesBuffer.add((allocation.token, tokenValue));
          };
          case null {};
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
        reason = reason;
      };
      checkpointsBuffer.add(checkpoint);

      // Update asset values for next iteration
      assetValues := Buffer.Buffer<(Principal, Float)>(allocations.size());
      previousPrices := updatedPrices;

      for (allocation in allocations.vals()) {
        let allocationPercent = basisPointsToPercentage(allocation.basisPoints);
        let tokenValue = totalValueAfterPriceChanges * allocationPercent;
        assetValues.add((allocation.token, tokenValue));
      };
    };

    let checkpoints = Buffer.toArray(checkpointsBuffer);

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

  // Recompute performance score from existing checkpoints using a different price type
  // This is cycle-efficient because checkpoints already contain both USD and ICP prices
  private func recomputePerformanceFromCheckpoints(
    checkpoints: [CheckpointData],
    targetPriceType: PriceType
  ) : Float {
    if (checkpoints.size() < 2) return 1.0;

    let firstCheckpoint = checkpoints[0];
    let lastCheckpoint = checkpoints[checkpoints.size() - 1];

    // Calculate initial portfolio value with target price type
    var initialValue : Float = 0.0;
    for (allocation in firstCheckpoint.allocations.vals()) {
      let priceOpt = Array.find<(Principal, PriceInfo)>(
        firstCheckpoint.pricesUsed,
        func ((t, _)) { Principal.equal(t, allocation.token) }
      );
      switch (priceOpt) {
        case (?(_, priceInfo)) {
          let price = getPriceValue(priceInfo, targetPriceType);
          let allocationPercent = Float.fromInt(allocation.basisPoints) / 10000.0;
          initialValue += allocationPercent * price;
        };
        case null {};
      };
    };

    // Calculate final portfolio value with target price type
    var finalValue : Float = 0.0;
    for (allocation in lastCheckpoint.allocations.vals()) {
      let priceOpt = Array.find<(Principal, PriceInfo)>(
        lastCheckpoint.pricesUsed,
        func ((t, _)) { Principal.equal(t, allocation.token) }
      );
      switch (priceOpt) {
        case (?(_, priceInfo)) {
          let price = getPriceValue(priceInfo, targetPriceType);
          let allocationPercent = Float.fromInt(allocation.basisPoints) / 10000.0;
          finalValue += allocationPercent * price;
        };
        case null {};
      };
    };

    if (initialValue == 0.0) return 1.0;
    finalValue / initialValue
  };

  // Extract ICP's USD price from a checkpoint's pricesUsed array
  private func getIcpUsdPriceFromCheckpoint(pricesUsed: [(Principal, PriceInfo)]) : ?Float {
    for ((token, priceInfo) in pricesUsed.vals()) {
      if (Principal.equal(token, ICP_PRINCIPAL)) {
        return ?priceInfo.usdPrice;
      };
    };
    null
  };

  // Recompute ICP performance from USD performance and ICP/USD exchange rates
  // Formula: ICP_performance = USD_performance × (start_icp_usd / end_icp_usd)
  // This is simpler and more reliable than recalculating from individual token prices
  private func recomputePerformanceFromCheckpointsDerived(
    checkpoints: [CheckpointData],
    usdPerformance: Float  // The already-correct USD performance score
  ) : ?Float {
    if (checkpoints.size() < 2) return null;

    let firstCheckpoint = checkpoints[0];
    let lastCheckpoint = checkpoints[checkpoints.size() - 1];

    // Get ICP/USD rate for first checkpoint
    let firstIcpUsd = switch (getIcpUsdPriceFromCheckpoint(firstCheckpoint.pricesUsed)) {
      case (?rate) {
        if (rate <= 0.0) return null; // Avoid division by zero
        rate
      };
      case null { return null }; // Can't derive without ICP rate
    };

    // Get ICP/USD rate for last checkpoint
    let lastIcpUsd = switch (getIcpUsdPriceFromCheckpoint(lastCheckpoint.pricesUsed)) {
      case (?rate) {
        if (rate <= 0.0) return null;
        rate
      };
      case null { return null };
    };

    // ICP performance = USD performance × (start_icp_usd / end_icp_usd)
    // If ICP got cheaper, your ICP performance is better than USD performance
    // If ICP got more expensive, your ICP performance is worse
    ?(usdPerformance * (firstIcpUsd / lastIcpUsd))
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
    await (with timeout = 65)  startDistributionTimerAt(nextRunTime)
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
    let availableBalance = await (with timeout = 65)  getAvailableBalance();
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
      let neuronsResult = await (with timeout = 65)  daoCanister.admin_getNeuronAllocations();
      let allNeurons = neuronsResult;
      
      logger.info("Distribution", "Found " # Nat.toText(allNeurons.size()) # " total neurons", "runPeriodicDistribution");
      
      // Separate neurons into processable and skipped (multiplier=0 means skip)
      let neuronsBuffer = Buffer.Buffer<(Blob, NeuronAllocation)>(allNeurons.size());
      let skippedNeuronsBuffer = Buffer.Buffer<FailedNeuron>(allNeurons.size());

      for ((neuronId, neuronAllocation) in allNeurons.vals()) {
        let penalty = Map.get(rewardPenalties, bhash, neuronId);
        switch (penalty) {
          case (?0) {
            // Multiplier is 0 - skip entirely (same as old skiplist behavior)
            let skippedNeuron : FailedNeuron = {
              neuronId = neuronId;
              errorMessage = "Neuron skipped by admin (penalty multiplier = 0)";
            };
            skippedNeuronsBuffer.add(skippedNeuron);
          };
          case (_) {
            // No penalty or partial penalty - include for processing
            neuronsBuffer.add((neuronId, neuronAllocation));
          };
        };
      };

      let neurons = Buffer.toArray(neuronsBuffer);
      let initialSkippedNeurons = Buffer.toArray(skippedNeuronsBuffer);

      if (initialSkippedNeurons.size() > 0) {
        logger.info("Distribution", "Skipped " # Nat.toText(initialSkippedNeurons.size()) # " neurons with penalty=0", "runPeriodicDistribution");
      };

      logger.info("Distribution", "Processing " # Nat.toText(neurons.size()) # " neurons (after penalty filtering)", "runPeriodicDistribution");

      if (neurons.size() == 0) {
        let message = if (allNeurons.size() > 0) {
          "All " # Nat.toText(allNeurons.size()) # " neurons have penalty=0"
        } else {
          "No neurons found"
        };
        await* completeDistribution(distributionCounter, ([] : [NeuronReward]), initialSkippedNeurons, 0.0, message);
        return;
      };

      // Update status with total neuron count (after filtering)
      let updatedRecord = {
        initialRecord with
        status = #InProgress({currentNeuron = 0; totalNeurons = neurons.size()});
      };
      Vector.put(distributionHistory, Vector.size(distributionHistory) - 1, updatedRecord);

      // Start processing neurons one by one (with initial skipped neurons in failed list)
      await* processNeuronsSequentially<system>(distributionCounter, neurons, 0, ([] : [NeuronReward]), initialSkippedNeurons, 0.0, startTime, endTime, priceType, 0);
      
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
    priceType: PriceType,
    retryCount: Nat
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
      let performanceResult = await (with timeout = 65)  calculateNeuronPerformance(
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
          let baseRewardScore = adjustedPerformanceScore * adjustedVotingPower;

          // Apply reward penalty if neuron has one (multiplier < 100)
          let rewardScore = switch (Map.get(rewardPenalties, bhash, neuronId)) {
            case (?multiplier) {
              if (multiplier >= 100) { baseRewardScore }
              else { (baseRewardScore * Float.fromInt(multiplier)) / 100.0 }
            };
            case (null) { baseRewardScore }; // No penalty
          };

          // Derive ICP performance from USD score and ICP/USD exchange rate change
          let performanceICP : ?Float = if (performance.checkpoints.size() >= 2) {
            recomputePerformanceFromCheckpointsDerived(performance.checkpoints, performance.performanceScore)
          } else {
            null
          };

          let neuronReward : NeuronReward = {
            neuronId = neuronId;
            performanceScore = performance.performanceScore;  // USD-based (original)
            performanceScoreICP = performanceICP;             // ICP-based (derived from USD)
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
                priceType,
                0
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
                priceType,
                0
              );
            }
          );
        };
      };
    } catch (error) {
      let errorMsg = Error.message(error);
      let isTimeout = Text.contains(errorMsg, #text "deadline") or Text.contains(errorMsg, #text "timeout");
      let maxRetries = 3;

      // Determine next index and retry count
      let (nextIndex, nextRetryCount, shouldAddToFailed) = if (isTimeout and retryCount < maxRetries) {
        // Retry same neuron
        logger.warn("Distribution", "Timeout processing neuron, will retry (" # Nat.toText(retryCount + 1) # "/" # Nat.toText(maxRetries) # "): " # errorMsg, "processNeuronsSequentially");
        (currentIndex, retryCount + 1, false)
      } else {
        // Move to next neuron
        logger.error("Distribution", "Error processing neuron: " # errorMsg, "processNeuronsSequentially");
        (currentIndex + 1, 0, true)
      };

      let updatedFailedNeurons = if (shouldAddToFailed) {
        let failedNeuron : FailedNeuron = {
          neuronId = neuronId;
          errorMessage = "System error: " # errorMsg # (if isTimeout  " (after " # Nat.toText(maxRetries) # " retries)" else "");
        };
        Array.flatten([failedNeurons, [failedNeuron]])
      } else {
        failedNeurons
      };

      // Use existing timer pattern
      ignore Timer.setTimer<system>(
        #nanoseconds(0),
        func() : async () {
          await* processNeuronsSequentially<system>(
            distributionId,
            neurons,
            nextIndex,
            neuronRewards,
            updatedFailedNeurons,
            totalRewardScore,
            startTime,
            endTime,
            priceType,
            nextRetryCount
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

    // Trigger leaderboard update after distribution completes
    // Run asynchronously to avoid blocking distribution finalization
    if (leaderboardUpdateEnabled and errorMessage == "") {
      logger.info("Leaderboard", "Triggering leaderboard update after distribution completion", "completeDistribution");
      ignore Timer.setTimer<system>(
        #nanoseconds(0),
        func() : async () {
          try {
            await* computeAllLeaderboards<system>();
          } catch (e) {
            logger.error("Leaderboard", "Failed to update leaderboards: " # Error.message(e), "completeDistribution");
          };
        }
      );
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
  // Leaderboard Admin Functions
  //=========================================================================

  // Update leaderboard configuration
  public shared ({ caller }) func updateLeaderboardConfig(
    size: ?Nat,
    enabled: ?Bool
  ) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    switch (size) { case (?s) leaderboardSize := s; case null {} };
    switch (enabled) { case (?e) leaderboardUpdateEnabled := e; case null {} };

    #ok("Leaderboard configuration updated")
  };

  // Manual refresh of all leaderboards and start the 2-hour recurring timer
  public shared ({ caller }) func refreshLeaderboards() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    try {
      await* computeAllLeaderboards<system>();
      // Start the recurring timer after successful refresh
      startLeaderboardTimer<system>();
      #ok("All leaderboards refreshed successfully. Recurring 2-hour timer started.")
    } catch (error) {
      #err(#SystemError("Failed to refresh leaderboards: " # Error.message(error)))
    };
  };

  // Start the recurring leaderboard refresh timer (2 hours)
  private func startLeaderboardTimer<system>() {
    // Cancel existing timer if any
    switch (leaderboardTimerId) {
      case (?id) { Timer.cancelTimer(id); };
      case null {};
    };

    // Set up recurring timer
    leaderboardTimerId := ?Timer.setTimer<system>(
      #nanoseconds(LEADERBOARD_REFRESH_INTERVAL_NS),
      func() : async () {
        if (leaderboardUpdateEnabled) {
          try {
            await* computeAllLeaderboards<system>();
            logger.info("Leaderboard", "Scheduled leaderboard refresh completed", "leaderboardTimer");
          } catch (error) {
            logger.error("Leaderboard", "Scheduled leaderboard refresh failed: " # Error.message(error), "leaderboardTimer");
          };
        };
        // Reschedule the timer for the next interval
        startLeaderboardTimer<system>();
      }
    );
    logger.info("Leaderboard", "Leaderboard refresh timer started (2-hour interval)", "startLeaderboardTimer");
  };

  // Stop the recurring leaderboard refresh timer
  public shared ({ caller }) func stopLeaderboardTimer() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    switch (leaderboardTimerId) {
      case (?id) {
        Timer.cancelTimer(id);
        leaderboardTimerId := null;
        logger.info("Leaderboard", "Leaderboard refresh timer stopped", "stopLeaderboardTimer");
        #ok("Leaderboard refresh timer stopped")
      };
      case null {
        #ok("No active leaderboard timer to stop")
      };
    };
  };

  // Get leaderboard timer status
  public query func getLeaderboardTimerStatus() : async { active: Bool; intervalHours: Nat } {
    {
      active = Option.isSome(leaderboardTimerId);
      intervalHours = 2;
    }
  };

  //=========================================================================
  // Backfill Historical Performance Data
  //=========================================================================

  // Admin function to backfill distribution history from historical allocation data
  // This calculates performance for all neurons across historical time periods
  // and populates distributionHistory as if the system had been running from the start
  // Optimized to use bulk queries - reduces inter-canister calls by ~27x
  public shared ({ caller }) func admin_backfillDistributionHistory(
    config: BackfillConfig
  ) : async Result.Result<BackfillResult, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    let NANOSECONDS_PER_DAY : Int = 86_400_000_000_000;
    let periodNS = config.periodDays * NANOSECONDS_PER_DAY;
    let now = Time.now();
    let NEURONS_PER_BATCH : Nat = 50; // Process 50 neurons per bulk query
    let PARALLEL_NEURON_BATCH : Nat = 5; // Process 5 neurons in parallel for performance calculation

    // Validate config
    if (config.startTime >= now) {
      return #err(#SystemError("startTime must be in the past"));
    };
    if (config.periodDays == 0) {
      return #err(#SystemError("periodDays must be greater than 0"));
    };
    if (config.maxPeriods == 0) {
      return #err(#SystemError("maxPeriods must be greater than 0"));
    };

    logger.info("Backfill", "Starting backfill from " # Int.toText(config.startTime) # " with " # Nat.toText(config.periodDays) # " day periods, max " # Nat.toText(config.maxPeriods) # " periods", "admin_backfillDistributionHistory");

    // Set backfill status
    backfillInProgress := true;
    backfillPeriodsCompleted := 0;
    backfillTotalPeriods := config.maxPeriods;
    backfillStartedAt := Time.now();
    backfillDataStartTime := config.startTime;
    backfillDataEndTime := now;
    backfillCurrentPeriodStart := config.startTime;
    backfillCurrentPeriodEnd := config.startTime;

    // Clear existing history if requested
    if (config.clearExisting) {
      while (Vector.size(distributionHistory) > 0) {
        ignore Vector.removeLast(distributionHistory);
      };
      distributionCounter := 0;
      logger.info("Backfill", "Cleared existing distribution history", "admin_backfillDistributionHistory");
    };

    // Calculate periods
    var periodsCreated : Nat = 0;
    var totalNeuronRewards : Nat = 0;
    var totalNeuronsProcessed : Nat = 0;
    let errorsBuffer = Buffer.Buffer<Text>(10);
    var periodStart = config.startTime;
    var actualEndTime = config.startTime;

    // Process each period
    label periodLoop while (periodsCreated < config.maxPeriods and periodStart < now) {
      let periodEnd = Int.min(periodStart + periodNS, now);

      // Update current period tracking for status queries
      backfillCurrentPeriodStart := periodStart;
      backfillCurrentPeriodEnd := periodEnd;

      if (periodEnd <= periodStart) {
        break periodLoop;
      };

      // Skip periods that already exist in history if configured
      if (config.skipExistingPeriods) {
        var periodExists = false;
        for (dist in Vector.vals(distributionHistory)) {
          if (dist.startTime == periodStart and dist.endTime == periodEnd) {
            periodExists := true;
          };
        };
        if (periodExists) {
          logger.info("Backfill", "Skipping existing period " # Int.toText(periodStart) # " - " # Int.toText(periodEnd), "admin_backfillDistributionHistory");
          periodsCreated += 1;
          backfillPeriodsCompleted := periodsCreated;
          periodStart := periodEnd;
          continue periodLoop;
        };
      };

      let neuronRewardsBuffer = Buffer.Buffer<NeuronReward>(100);
      let failedNeuronsBuffer = Buffer.Buffer<FailedNeuron>(10);
      var totalRewardScore : Float = 0.0;

      // Price cache for this period - prefetch common timestamps to avoid redundant calls
      // Key: timestamp, Value: Map of token -> price
      let periodPriceCache = Map.new<Int, [(Principal, ?PriceInfo)]>();

      // Use parallel bulk queries - issue 10 queries at once, then await (with timeout = 65)  all
      let PARALLEL_QUERIES : Nat = 10;
      let MAX_RETRIES : Nat = 3;
      var offset : Nat = 0;
      var hasMore = true;

      label batchLoop while (hasMore) {
        // Issue up to 10 parallel queries
        let futures = Buffer.Buffer<async Result.Result<{
          neurons: [(Blob, {
            preTimespanAllocation: ?NeuronAllocationChangeBlockData;
            inTimespanChanges: [NeuronAllocationChangeBlockData];
          })];
          totalNeurons: Nat;
          hasMore: Bool;
        }, ArchiveError>>(PARALLEL_QUERIES);

        // Launch parallel queries
        for (i in Iter.range(0, PARALLEL_QUERIES - 1)) {
          let queryOffset = offset + (i * NEURONS_PER_BATCH);
          futures.add(
            (with timeout = 65) neuronAllocationArchive.getAllNeuronsAllocationChangesInTimeRange(
              periodStart, periodEnd, queryOffset, NEURONS_PER_BATCH
            )
          );
        };

        // await (with timeout = 65)  all parallel queries
        var anyHasMore = false;
        var queryIndex = 0;
        var failedQueries : Nat = 0;
        label futureLoop for (future in futures.vals()) {
          let bulkResult = try {
            await future
          } catch (e) {
            let errorMsg = "Period " # Int.toText(periodStart) # " batch " # Nat.toText(queryIndex) # ": Failed to get bulk allocation data: " # Error.message(e);
            errorsBuffer.add(errorMsg);
            // Update last errors for status tracking
            let currentErrors = Buffer.fromArray<Text>(backfillLastErrors);
            currentErrors.add(errorMsg);
            // Keep only last 5 errors
            while (currentErrors.size() > 5) {
              ignore currentErrors.remove(0);
            };
            backfillLastErrors := Buffer.toArray(currentErrors);
            queryIndex += 1;
            failedQueries += 1;
            // Continue to next future instead of breaking out
            continue futureLoop;
          };

          switch (bulkResult) {
            case (#ok(data)) {
              if (data.hasMore and queryIndex == PARALLEL_QUERIES - 1) {
                anyHasMore := true;
              };
              totalNeuronsProcessed := data.totalNeurons;

              // Collect neurons with valid allocation data for parallel processing
              type NeuronToProcess = {
                neuronId: Blob;
                allocationData: {
                  preTimespanAllocation: ?NeuronAllocationChangeBlockData;
                  inTimespanChanges: [NeuronAllocationChangeBlockData];
                };
                startAllocation: [Allocation];
              };
              let neuronsToProcess = Buffer.Buffer<NeuronToProcess>(data.neurons.size());

              // First pass: collect neurons that can be processed
              for ((neuronId, allocationData) in data.neurons.vals()) {
                let startAllocation = switch (allocationData.preTimespanAllocation) {
                  case (?preAlloc) { preAlloc.newAllocations };
                  case (null) {
                    switch (Array.find(allocationData.inTimespanChanges, func(change: NeuronAllocationChangeBlockData) : Bool {
                      change.timestamp == periodStart
                    })) {
                      case (?exactChange) { exactChange.oldAllocations };
                      case (null) { [] }; // No allocation data
                    };
                  };
                };

                if (startAllocation.size() > 0) {
                  neuronsToProcess.add({
                    neuronId = neuronId;
                    allocationData = allocationData;
                    startAllocation = startAllocation;
                  });
                };
              };

              // Second pass: process neurons in parallel batches
              let neuronsArray = Buffer.toArray(neuronsToProcess);
              var batchStart : Nat = 0;

              while (batchStart < neuronsArray.size()) {
                let batchEnd = Nat.min(batchStart + PARALLEL_NEURON_BATCH, neuronsArray.size());
                let batchSize = batchEnd - batchStart;

                // Launch parallel futures for this batch
                let perfFutures = Buffer.Buffer<async Result.Result<PerformanceResult, RewardsError>>(batchSize);
                let batchNeurons = Buffer.Buffer<NeuronToProcess>(batchSize);

                var idx = batchStart;
                while (idx < batchEnd) {
                  let neuron = neuronsArray[idx];
                  batchNeurons.add(neuron);
                  perfFutures.add(
                    (with timeout = 65) calculatePerformanceFromAllocationData(
                      neuron.neuronId, periodStart, periodEnd, #USD, neuron.allocationData, neuron.startAllocation, ?periodPriceCache
                    )
                  );
                  idx += 1;
                };

                // await (with timeout = 65)  all futures in this batch
                var futureIdx : Nat = 0;
                for (future in perfFutures.vals()) {
                  let neuron = Buffer.toArray(batchNeurons)[futureIdx];
                  try {
                    let performanceResult = await  future;

                    switch (performanceResult) {
                      case (#ok(performance)) {
                        let performanceICP : ?Float = if (performance.checkpoints.size() >= 2) {
                          recomputePerformanceFromCheckpointsDerived(performance.checkpoints, performance.performanceScore)
                        } else { null };

                        let rewardScore = performance.performanceScore;
                        totalRewardScore += rewardScore;

                        let neuronReward : NeuronReward = {
                          neuronId = neuron.neuronId;
                          performanceScore = performance.performanceScore;
                          performanceScoreICP = performanceICP;
                          votingPower = 0;
                          rewardScore = rewardScore;
                          rewardAmount = 0;
                          checkpoints = performance.checkpoints;
                        };

                        neuronRewardsBuffer.add(neuronReward);
                        totalNeuronRewards += 1;
                      };
                      case (#err(error)) {
                        switch (error) {
                          case (#NeuronNotFound) { };
                          case (#PriceDataMissing(_)) { };
                          case (_) {
                            failedNeuronsBuffer.add({
                              neuronId = neuron.neuronId;
                              errorMessage = debug_show(error);
                            });
                          };
                        };
                      };
                    };
                  } catch (e) {
                    failedNeuronsBuffer.add({
                      neuronId = neuron.neuronId;
                      errorMessage = Error.message(e);
                    });
                  };
                  futureIdx += 1;
                };

                batchStart := batchEnd;
              };
            };
            case (#err(error)) {
              errorsBuffer.add("Period " # Int.toText(periodStart) # " batch " # Nat.toText(queryIndex) # ": Bulk query error: " # debug_show(error));
            };
          };
          queryIndex += 1;
        };

        // Move offset forward by all parallel batches
        offset += PARALLEL_QUERIES * NEURONS_PER_BATCH;
        hasMore := anyHasMore;
      };

      // Only create distribution record if we have at least one neuron reward
      if (neuronRewardsBuffer.size() > 0) {
        distributionCounter += 1;

        let distributionRecord : DistributionRecord = {
          id = distributionCounter;
          startTime = periodStart;
          endTime = periodEnd;
          distributionTime = periodEnd;
          totalRewardPot = 0;
          actualDistributed = 0;
          totalRewardScore = totalRewardScore;
          neuronsProcessed = neuronRewardsBuffer.size();
          neuronRewards = Buffer.toArray(neuronRewardsBuffer);
          failedNeurons = Buffer.toArray(failedNeuronsBuffer);
          status = #Completed;
        };

        Vector.add(distributionHistory, distributionRecord);
        periodsCreated += 1;
        backfillPeriodsCompleted := periodsCreated;
        actualEndTime := periodEnd;

        logger.info("Backfill", "Created period " # Nat.toText(periodsCreated) # " with " # Nat.toText(neuronRewardsBuffer.size()) # " neurons", "admin_backfillDistributionHistory");
      } else {
        // Log when no neurons found for period (common when querying time ranges before data exists)
        logger.info("Backfill", "No neurons found for period " # Int.toText(periodStart) # " - " # Int.toText(periodEnd) # " (failed: " # Nat.toText(failedNeuronsBuffer.size()) # ")", "admin_backfillDistributionHistory");
        if (failedNeuronsBuffer.size() > 0) {
          errorsBuffer.add("Period " # Int.toText(periodStart) # "-" # Int.toText(periodEnd) # ": No successful neuron calculations, " # Nat.toText(failedNeuronsBuffer.size()) # " failed");
        };
      };

      periodStart := periodEnd;
    };

    // Trim to maxDistributionHistory if needed
    while (Vector.size(distributionHistory) > maxDistributionHistory) {
      Vector.reverse(distributionHistory);
      ignore Vector.removeLast(distributionHistory);
      Vector.reverse(distributionHistory);
    };

    // Recompute all leaderboards
    try {
      await* computeAllLeaderboards<system>();
      logger.info("Backfill", "Leaderboards recomputed successfully", "admin_backfillDistributionHistory");
    } catch (e) {
      errorsBuffer.add("Failed to recompute leaderboards: " # Error.message(e));
    };

    // Mark backfill as complete
    backfillInProgress := false;

    logger.info("Backfill", "Backfill completed: " # Nat.toText(periodsCreated) # " periods, " # Nat.toText(totalNeuronRewards) # " neuron rewards", "admin_backfillDistributionHistory");

    #ok({
      periodsCreated = periodsCreated;
      neuronsProcessed = totalNeuronsProcessed;
      totalNeuronRewards = totalNeuronRewards;
      errors = Buffer.toArray(errorsBuffer);
      startTime = config.startTime;
      endTime = actualEndTime;
    })
  };

  // Helper function to calculate performance from pre-fetched allocation data
  // This avoids redundant inter-canister calls when we already have the allocation data
  // Optional priceCache parameter enables caching during backfill to avoid redundant price fetches
  private func calculatePerformanceFromAllocationData(
    neuronId: Blob,
    startTime: Int,
    endTime: Int,
    priceType: PriceType,
    allocationData: {
      preTimespanAllocation: ?NeuronAllocationChangeBlockData;
      inTimespanChanges: [NeuronAllocationChangeBlockData];
    },
    startAllocation: [Allocation],
    priceCache: ?Map.Map<Int, [(Principal, ?PriceInfo)]>
  ) : async Result.Result<PerformanceResult, RewardsError> {

    // Build timeline of allocation changes with makers and reasons
    let timelineBuffer = Buffer.Buffer<(Int, [Allocation], ?Principal, ?Text)>(10);

    // Add start point
    let startMaker = switch (allocationData.preTimespanAllocation) {
      case (?preAlloc) { ?preAlloc.maker };
      case (null) { null };
    };
    let startReason : ?Text = switch (allocationData.preTimespanAllocation) {
      case (?preAlloc) { preAlloc.reason };
      case (null) { null };
    };
    timelineBuffer.add((startTime, startAllocation, startMaker, startReason));

    // Add all in-timespan changes
    for (change in allocationData.inTimespanChanges.vals()) {
      if (change.timestamp > startTime and change.timestamp <= endTime) {
        timelineBuffer.add((change.timestamp, change.newAllocations, ?change.maker, change.reason));
      };
    };

    // Add end point with final allocation
    let finalAllocation = if (allocationData.inTimespanChanges.size() > 0) {
      let lastChange = allocationData.inTimespanChanges[allocationData.inTimespanChanges.size() - 1];
      lastChange.newAllocations
    } else {
      startAllocation
    };

    let lastMaker = if (allocationData.inTimespanChanges.size() > 0) {
      ?allocationData.inTimespanChanges[allocationData.inTimespanChanges.size() - 1].maker
    } else {
      startMaker
    };
    let lastReason : ?Text = if (allocationData.inTimespanChanges.size() > 0) {
      allocationData.inTimespanChanges[allocationData.inTimespanChanges.size() - 1].reason
    } else {
      startReason
    };
    timelineBuffer.add((endTime, finalAllocation, lastMaker, lastReason));

    let timeline = Buffer.toArray(timelineBuffer);

    if (timeline.size() < 2) {
      return #err(#AllocationDataMissing);
    };

    // Calculate checkpoints and performance
    let checkpointsBuffer = Buffer.Buffer<CheckpointData>(timeline.size() + 1);
    var cumulativeReturn : Float = 1.0;

    // Add START checkpoint (needed for ICP performance recomputation which requires >= 2 checkpoints)
    let (initialTimestamp, initialAllocations, initialMaker, initialReason) = timeline[0];
    let initialPrices = try {
      switch (priceCache) {
        case (?cache) { await (with timeout = 65)  getPricesForAllocationsCached(initialAllocations, initialTimestamp, cache) };
        case null { await (with timeout = 65)  getPricesForAllocations(initialAllocations, initialTimestamp) };
      }
    } catch (e) {
      return #err(#SystemError("Failed to get initial prices for start checkpoint: " # Error.message(e)));
    };

    let startTokenValuesBuffer = Buffer.Buffer<(Principal, Float)>(initialAllocations.size());
    let startPricesUsedBuffer = Buffer.Buffer<(Principal, PriceInfo)>(initialAllocations.size());
    var startTotalWeight : Float = 0.0;

    // First pass: collect tokens with valid prices
    for (alloc in initialAllocations.vals()) {
      switch (Array.find(initialPrices, func(p: (Principal, ?PriceInfo)) : Bool { p.0 == alloc.token })) {
        case (?(_, ?price)) {
          let weight = Float.fromInt(alloc.basisPoints) / 10000.0;
          startTotalWeight += weight;
          let priceValue = switch (priceType) {
            case (#USD) { price.usdPrice };
            case (#ICP) { Float.fromInt(price.icpPrice) / 100000000.0 };
          };
          startTokenValuesBuffer.add((alloc.token, weight * priceValue));
          startPricesUsedBuffer.add((alloc.token, price));
        };
        case (_) {}; // Skip tokens without price data
      };
    };

    // If no tokens have valid prices for start checkpoint, fail
    if (startTokenValuesBuffer.size() == 0) {
      return #err(#PriceDataMissing({ token = initialAllocations[0].token; timestamp = initialTimestamp }));
    };

    // Add start checkpoint with initial portfolio value of 1.0
    checkpointsBuffer.add({
      timestamp = initialTimestamp;
      allocations = initialAllocations;
      tokenValues = Buffer.toArray(startTokenValuesBuffer);
      totalPortfolioValue = 1.0;
      pricesUsed = Buffer.toArray(startPricesUsedBuffer);
      maker = initialMaker;
      reason = initialReason;
    });

    var i = 0;
    while (i < timeline.size() - 1) {
      let (segmentStart, allocations, maker, reason) = timeline[i];
      let (segmentEnd, _, _, _) = timeline[i + 1];

      // Get prices at start and end of segment (use cache if provided)
      let startPrices = try {
        switch (priceCache) {
          case (?cache) { await (with timeout = 65)  getPricesForAllocationsCached(allocations, segmentStart, cache) };
          case null { await (with timeout = 65)  getPricesForAllocations(allocations, segmentStart) };
        }
      } catch (e) {
        return #err(#SystemError("Failed to get start prices: " # Error.message(e)));
      };

      let endPrices = try {
        switch (priceCache) {
          case (?cache) { await (with timeout = 65)  getPricesForAllocationsCached(allocations, segmentEnd, cache) };
          case null { await (with timeout = 65)  getPricesForAllocations(allocations, segmentEnd) };
        }
      } catch (e) {
        return #err(#SystemError("Failed to get end prices: " # Error.message(e)));
      };

      // Calculate segment return - skip tokens without prices and normalize weights
      var startValue : Float = 0.0;
      var endValue : Float = 0.0;
      let tokenValuesBuffer = Buffer.Buffer<(Principal, Float)>(allocations.size());
      let pricesUsedBuffer = Buffer.Buffer<(Principal, PriceInfo)>(allocations.size());

      // First pass: collect tokens with valid prices and calculate total weight
      type ValidAlloc = { token: Principal; weight: Float; startPrice: Float; endPrice: Float; endPriceInfo: PriceInfo };
      let validAllocsBuffer = Buffer.Buffer<ValidAlloc>(allocations.size());
      var totalValidWeight : Float = 0.0;

      for (alloc in allocations.vals()) {
        let weight = Float.fromInt(alloc.basisPoints) / 10000.0;

        let startPriceOpt = switch (Array.find(startPrices, func(p: (Principal, ?PriceInfo)) : Bool { p.0 == alloc.token })) {
          case (?(_, ?price)) {
            ?(switch (priceType) {
              case (#USD) { price.usdPrice };
              case (#ICP) { Float.fromInt(price.icpPrice) / 100000000.0 };
            })
          };
          case (_) { null }; // Skip - no start price
        };

        let endPriceOpt = switch (Array.find(endPrices, func(p: (Principal, ?PriceInfo)) : Bool { p.0 == alloc.token })) {
          case (?(_, ?price)) {
            ?(switch (priceType) {
              case (#USD) { price.usdPrice };
              case (#ICP) { Float.fromInt(price.icpPrice) / 100000000.0 };
            }, price)
          };
          case (_) { null }; // Skip - no end price
        };

        // Only include token if both start and end prices are available
        switch (startPriceOpt, endPriceOpt) {
          case (?sPrice, ?(ePrice, ePriceInfo)) {
            validAllocsBuffer.add({
              token = alloc.token;
              weight = weight;
              startPrice = sPrice;
              endPrice = ePrice;
              endPriceInfo = ePriceInfo;
            });
            totalValidWeight += weight;
          };
          case _ {}; // Skip tokens without complete price data
        };
      };

      // If no tokens have valid prices, fail this segment
      if (validAllocsBuffer.size() == 0) {
        return #err(#PriceDataMissing({ token = allocations[0].token; timestamp = segmentStart }));
      };

      // Second pass: calculate values with normalized weights
      for (validAlloc in validAllocsBuffer.vals()) {
        // Normalize weight so remaining tokens sum to 100%
        let normalizedWeight = validAlloc.weight / totalValidWeight;

        startValue += normalizedWeight * validAlloc.startPrice;
        endValue += normalizedWeight * validAlloc.endPrice;
        tokenValuesBuffer.add((validAlloc.token, normalizedWeight * validAlloc.endPrice));
        pricesUsedBuffer.add((validAlloc.token, validAlloc.endPriceInfo));
      };

      // Calculate segment return and compound it
      let segmentReturn = if (startValue > 0.0) { endValue / startValue } else { 1.0 };
      cumulativeReturn *= segmentReturn;

      // Create checkpoint
      let checkpoint : CheckpointData = {
        timestamp = segmentEnd;
        allocations = allocations;
        tokenValues = Buffer.toArray(tokenValuesBuffer);
        totalPortfolioValue = cumulativeReturn;
        pricesUsed = Buffer.toArray(pricesUsedBuffer);
        maker = maker;
        reason = reason;
      };
      checkpointsBuffer.add(checkpoint);

      i += 1;
    };

    #ok({
      neuronId = neuronId;
      startTime = startTime;
      endTime = endTime;
      initialValue = 1.0;
      finalValue = cumulativeReturn;
      performanceScore = cumulativeReturn;
      allocationChanges = allocationData.inTimespanChanges.size();
      checkpoints = Buffer.toArray(checkpointsBuffer);
      preTimespanAllocation = allocationData.preTimespanAllocation;
      inTimespanChanges = allocationData.inTimespanChanges;
    })
  };

  // Helper to get prices for a set of allocations at a given time
  private func getPricesForAllocations(allocations: [Allocation], timestamp: Int) : async [(Principal, ?PriceInfo)] {
    let tokens = Array.map<Allocation, Principal>(allocations, func(a) { a.token });
    let pricesResult = await (with timeout = 65)  priceArchive.getPricesAtTime(tokens, timestamp);
    switch (pricesResult) {
      case (#ok(prices)) { prices };
      case (#err(_)) { [] };
    };
  };

  // Cached version for backfill - checks cache first, fetches if not found
  private func getPricesForAllocationsCached(
    allocations: [Allocation],
    timestamp: Int,
    cache: Map.Map<Int, [(Principal, ?PriceInfo)]>
  ) : async [(Principal, ?PriceInfo)] {
    // Check cache first
    switch (Map.get(cache, Map.ihash, timestamp)) {
      case (?cached) {
        // Return cached prices, but only for tokens in allocations
        let tokens = Array.map<Allocation, Principal>(allocations, func(a) { a.token });
        let filteredPrices = Buffer.Buffer<(Principal, ?PriceInfo)>(tokens.size());
        for (token in tokens.vals()) {
          let found = Array.find<(Principal, ?PriceInfo)>(cached, func(p) { p.0 == token });
          switch (found) {
            case (?price) { filteredPrices.add(price) };
            case null {
              // Token not in cache, need to fetch it
              let newPrices = await (with timeout = 65)  getPricesForAllocations(allocations, timestamp);
              // Update cache with all fetched tokens
              Map.set(cache, Map.ihash, timestamp, newPrices);
              return newPrices;
            };
          };
        };
        Buffer.toArray(filteredPrices)
      };
      case null {
        // Not in cache, fetch and store
        let prices = await (with timeout = 65)  getPricesForAllocations(allocations, timestamp);
        Map.set(cache, Map.ihash, timestamp, prices);
        prices
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
      await (with timeout = 65)  tacoLedger.icrc1_balance_of(account)
    } catch (error) {
      logger.error("TacoBalance", "Failed to get TACO balance: " # Error.message(error), "getTacoBalance");
      0 // Return 0 on error
    }
  };

  // Get all neuron reward balances (admin only) - returns TACO satoshis
  // Public query - allows anyone to view neuron reward balances (read-only transparency)
  public shared query func getAllNeuronRewardBalances() : async [(Blob, Nat)] {
    Iter.toArray(Map.entries(neuronRewardBalances));
  };

  // TODO: Implement reward claiming mechanism
  // This would transfer rewards to user wallets and deduct from balances

  //=========================================================================
  // Distribution History and Status
  //=========================================================================

  // Get distribution history with pagination (returns most recent first)
  // offset: number of records to skip from the most recent
  // limit: maximum number of records to return
  public query func getDistributionHistory(offset: Nat, limit: Nat) : async {
    records: [DistributionRecord];
    total: Nat;
    hasMore: Bool;
  } {
    let totalSize = Vector.size(distributionHistory);
    
    // Cap limit to prevent overly large responses
    let actualLimit = Nat.min(limit, 10);
    
    // If offset exceeds total, return empty
    if (offset >= totalSize) {
      return {
        records = [];
        total = totalSize;
        hasMore = false;
      };
    };
    
    // Calculate the actual range (most recent first)
    // Records are stored oldest first, so we need to reverse the access
    let availableFromOffset = totalSize - offset;
    let recordsToTake = Nat.min(actualLimit, availableFromOffset);
    
    // Build result array (most recent first)
    let resultBuffer = Buffer.Buffer<DistributionRecord>(recordsToTake);
    var i = 0;
    while (i < recordsToTake) {
      let index = totalSize - 1 - offset - i;
      switch (Vector.getOpt(distributionHistory, index)) {
        case (?record) { resultBuffer.add(record); };
        case null { };
      };
      i += 1;
    };
    
    {
      records = Buffer.toArray(resultBuffer);
      total = totalSize;
      hasMore = offset + recordsToTake < totalSize;
    };
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

  // Get backfill status
  public query func getBackfillStatus() : async {
    inProgress: Bool;
    periodsCompleted: Nat;
    totalPeriods: Nat;
    startedAt: Int;
    elapsedNS: Int;
    currentPeriodStart: Int;
    currentPeriodEnd: Int;
    dataStartTime: Int;
    dataEndTime: Int;
    progressPercent: Nat;  // 0-100
    lastErrors: [Text];    // Last 5 errors for debugging
  } {
    let totalTimeRange = backfillDataEndTime - backfillDataStartTime;
    let processedTimeRange = backfillCurrentPeriodEnd - backfillDataStartTime;
    let progress : Nat = if (totalTimeRange > 0 and backfillInProgress) {
      Int.abs(processedTimeRange * 100 / totalTimeRange)
    } else if (backfillPeriodsCompleted > 0) {
      100
    } else {
      0
    };

    {
      inProgress = backfillInProgress;
      periodsCompleted = backfillPeriodsCompleted;
      totalPeriods = backfillTotalPeriods;
      startedAt = backfillStartedAt;
      elapsedNS = if (backfillInProgress) { Time.now() - backfillStartedAt } else { 0 };
      currentPeriodStart = backfillCurrentPeriodStart;
      currentPeriodEnd = backfillCurrentPeriodEnd;
      dataStartTime = backfillDataStartTime;
      dataEndTime = backfillDataEndTime;
      progressPercent = progress;
      lastErrors = backfillLastErrors;
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

  // Set max distribution history (number of distributions to keep)
  public shared ({ caller }) func setMaxDistributionHistory(max: Nat) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    if (max == 0) {
      return #err(#SystemError("Max distribution history must be > 0"));
    };

    maxDistributionHistory := max;
    logger.info("Config", "Max distribution history set to " # Nat.toText(max), "setMaxDistributionHistory");
    #ok("Max distribution history updated");
  };

  // Set leaderboard cutoff date (distributions before this date are excluded from leaderboards and graph data)
  public shared ({ caller }) func setLeaderboardCutoffDate(cutoffTime: Int) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    leaderboardCutoffDate := cutoffTime;
    logger.info("Config", "Leaderboard cutoff date set to " # Int.toText(cutoffTime), "setLeaderboardCutoffDate");
    #ok("Leaderboard cutoff date updated")
  };

  // Get current leaderboard cutoff date
  public query func getLeaderboardCutoffDate() : async Int {
    leaderboardCutoffDate
  };

  //=========================================================================
  // Reward Penalty Management Functions
  //=========================================================================

  // Get all reward penalties
  // Public query - allows anyone to view the reward penalties (read-only transparency)
  public shared query func getRewardPenalties() : async Result.Result<[(Blob, Nat)], RewardsError> {
    #ok(Iter.toArray(Map.entries(rewardPenalties)));
  };

  // Get penalty for a specific neuron (null = no penalty)
  public shared query func getRewardPenalty(neuronId: Blob) : async Result.Result<?Nat, RewardsError> {
    #ok(Map.get(rewardPenalties, bhash, neuronId));
  };

  // Set reward penalty for a neuron
  // multiplier: 0 = skip entirely, 1-99 = reduce reward score by that %, 100+ = no penalty
  public shared ({ caller }) func setRewardPenalty(neuronId: Blob, multiplier: Nat) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    if (multiplier > 100) {
      return #err(#SystemError("Multiplier must be 0-100 (percentage of reward score to keep)"));
    };

    Map.set(rewardPenalties, bhash, neuronId, multiplier);
    logger.info("Config", "Set reward penalty multiplier=" # Nat.toText(multiplier) # " for neuron", "setRewardPenalty");
    #ok("Reward penalty set");
  };

  // Set multiple reward penalties at once (replaces all penalties)
  public shared ({ caller }) func setRewardPenalties(penalties: [(Blob, Nat)]) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    // Clear existing penalties
    rewardPenalties := Map.new<Blob, Nat>();

    // Add new penalties (validate each)
    var count : Nat = 0;
    for ((neuronId, multiplier) in penalties.vals()) {
      if (multiplier <= 100) {
        Map.set(rewardPenalties, bhash, neuronId, multiplier);
        count += 1;
      };
    };

    logger.info("Config", "Set " # Nat.toText(count) # " reward penalties", "setRewardPenalties");
    #ok("Reward penalties updated");
  };

  // Remove reward penalty for a neuron (neuron will get full rewards)
  public shared ({ caller }) func removeRewardPenalty(neuronId: Blob) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    let existed = Map.has(rewardPenalties, bhash, neuronId);
    Map.delete(rewardPenalties, bhash, neuronId);
    logger.info("Config", "Removed reward penalty (existed: " # Bool.toText(existed) # ")", "removeRewardPenalty");
    #ok(if (existed) { "Reward penalty removed" } else { "Neuron had no penalty" });
  };

  // DEPRECATED: Legacy skiplist functions for backwards compatibility
  // These now map to the penalty system with multiplier=0

  // Get the current reward skip list (returns neurons with multiplier=0)
  public shared query func getRewardSkipList() : async Result.Result<[Blob], RewardsError> {
    let skipped = Buffer.Buffer<Blob>(Map.size(rewardPenalties));
    for ((neuronId, multiplier) in Map.entries(rewardPenalties)) {
      if (multiplier == 0) {
        skipped.add(neuronId);
      };
    };
    #ok(Buffer.toArray(skipped));
  };

  // Set the reward skip list (sets all to multiplier=0)
  public shared ({ caller }) func setRewardSkipList(neuronIds: [Blob]) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    // Clear existing penalties that were skips (multiplier=0)
    let toRemove = Buffer.Buffer<Blob>(Map.size(rewardPenalties));
    for ((neuronId, multiplier) in Map.entries(rewardPenalties)) {
      if (multiplier == 0) {
        toRemove.add(neuronId);
      };
    };
    for (neuronId in toRemove.vals()) {
      Map.delete(rewardPenalties, bhash, neuronId);
    };

    // Add new skips
    for (neuronId in neuronIds.vals()) {
      Map.set(rewardPenalties, bhash, neuronId, 0);
    };

    logger.info("Config", "Reward skip list updated with " # Nat.toText(neuronIds.size()) # " neurons (via penalty system)", "setRewardSkipList");
    #ok("Reward skip list updated");
  };

  // Add a neuron to the reward skip list (sets multiplier=0)
  public shared ({ caller }) func addToRewardSkipList(neuronId: Blob) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    // Check if already skipped
    switch (Map.get(rewardPenalties, bhash, neuronId)) {
      case (?0) { return #err(#SystemError("Neuron is already in the skip list")); };
      case (_) {};
    };

    Map.set(rewardPenalties, bhash, neuronId, 0);
    logger.info("Config", "Added neuron to reward skip list (via penalty system)", "addToRewardSkipList");
    #ok("Neuron added to skip list");
  };

  // Remove a neuron from the reward skip list
  public shared ({ caller }) func removeFromRewardSkipList(neuronId: Blob) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    // Check if in skip list (multiplier=0)
    switch (Map.get(rewardPenalties, bhash, neuronId)) {
      case (?0) {
        Map.delete(rewardPenalties, bhash, neuronId);
        logger.info("Config", "Removed neuron from reward skip list (via penalty system)", "removeFromRewardSkipList");
        #ok("Neuron removed from skip list");
      };
      case (?_) { #err(#SystemError("Neuron has a penalty but is not fully skipped")); };
      case (null) { #err(#SystemError("Neuron is not in the skip list")); };
    };
  };

  //=========================================================================
  // Leaderboard Query Functions
  //=========================================================================

  // Get leaderboard for a specific timeframe and price type
  public query func getLeaderboard(
    timeframe: LeaderboardTimeframe,
    priceType: LeaderboardPriceType,
    limit: ?Nat,
    offset: ?Nat
  ) : async [LeaderboardEntry] {
    // Select the correct leaderboard
    let selectedLeaderboard = switch (timeframe, priceType) {
      case (#OneWeek, #USD) { leaderboards.oneWeekUSD };
      case (#OneWeek, #ICP) { leaderboards.oneWeekICP };
      case (#OneMonth, #USD) { leaderboards.oneMonthUSD };
      case (#OneMonth, #ICP) { leaderboards.oneMonthICP };
      case (#OneYear, #USD) { leaderboards.oneYearUSD };
      case (#OneYear, #ICP) { leaderboards.oneYearICP };
      case (#AllTime, #USD) { leaderboards.allTimeUSD };
      case (#AllTime, #ICP) { leaderboards.allTimeICP };
    };

    let start = switch (offset) { case (?o) o; case null 0 };
    let count = switch (limit) { case (?l) l; case null leaderboardSize };
    let end = Nat.min(start + count, selectedLeaderboard.size());

    if (start >= selectedLeaderboard.size()) {
      return [];
    };

    Array.tabulate<LeaderboardEntry>(
      end - start,
      func (i) { selectedLeaderboard[start + i] }
    )
  };

  // Get leaderboard metadata/info
  public query func getLeaderboardInfo() : async {
    lastUpdate: Int;
    maxSize: Nat;
    updateEnabled: Bool;
    leaderboardCounts: {
      oneWeekUSD: Nat;
      oneWeekICP: Nat;
      oneMonthUSD: Nat;
      oneMonthICP: Nat;
      oneYearUSD: Nat;
      oneYearICP: Nat;
      allTimeUSD: Nat;
      allTimeICP: Nat;
    };
    totalDistributions: Nat;
  } {
    {
      lastUpdate = leaderboardLastUpdate;
      maxSize = leaderboardSize;
      updateEnabled = leaderboardUpdateEnabled;
      leaderboardCounts = {
        oneWeekUSD = leaderboards.oneWeekUSD.size();
        oneWeekICP = leaderboards.oneWeekICP.size();
        oneMonthUSD = leaderboards.oneMonthUSD.size();
        oneMonthICP = leaderboards.oneMonthICP.size();
        oneYearUSD = leaderboards.oneYearUSD.size();
        oneYearICP = leaderboards.oneYearICP.size();
        allTimeUSD = leaderboards.allTimeUSD.size();
        allTimeICP = leaderboards.allTimeICP.size();
      };
      totalDistributions = Vector.size(distributionHistory);
    }
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
    rewardPenaltiesCount: Nat;
    rewardSkipListSize: Nat; // DEPRECATED: now counts neurons with penalty=0
  } {
    // Count neurons with multiplier=0 (skipped) for backwards compatibility
    var skipCount : Nat = 0;
    for ((_, multiplier) in Map.entries(rewardPenalties)) {
      if (multiplier == 0) { skipCount += 1; };
    };
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
      rewardPenaltiesCount = Map.size(rewardPenalties);
      rewardSkipListSize = skipCount;
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
  // Public query - allows anyone to view withdrawal history (read-only transparency)
  public shared func getAllWithdrawalHistory(limit: ?Nat) : async Result.Result<[WithdrawalRecord], RewardsError> {
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

  // Get withdrawal statistics
  // Public query - allows anyone to view withdrawal stats (read-only transparency)
  public query func getWithdrawalStats() : async {
    totalWithdrawn: Nat;
    totalWithdrawals: Nat;
    totalRecordsInHistory: Nat;
  } {
    {
      totalWithdrawn = totalWithdrawn;
      totalWithdrawals = totalWithdrawals;
      totalRecordsInHistory = Vector.size(withdrawalHistory);
    }
  };

  //=========================================================================
  // Leaderboard Computation Functions
  //=========================================================================

  private func computeAllLeaderboards<system>() : async* () {
    if (not leaderboardUpdateEnabled) {
      return;
    };

    logger.info("Leaderboard", "Starting leaderboard computation for all 8 timeframe/price combinations", "computeAllLeaderboards");

    // 1. Fetch active decision makers from DAO (makers + followers, excludes passive hotkeys)
    // Falls back to getAllNeuronOwners if getActiveDecisionMakers is not available
    let activeDecisionMakers = try {
      await (with timeout = 65) daoCanister.getActiveDecisionMakers()
    } catch (e) {
      logger.warn("Leaderboard", "getActiveDecisionMakers failed, falling back to getAllNeuronOwners: " # Error.message(e), "computeAllLeaderboards");
      try {
        await (with timeout = 65) daoCanister.getAllNeuronOwners()
      } catch (e2) {
        logger.error("Leaderboard", "Failed to fetch neuron owners from DAO: " # Error.message(e2), "computeAllLeaderboards");
        return;
      }
    };

    // Build a map of neuronId -> principals (only active decision makers)
    let neuronToPrincipals = Map.new<Blob, [Principal]>();
    for ((neuronId, principals) in activeDecisionMakers.vals()) {
      Map.set(neuronToPrincipals, bhash, neuronId, principals);
    };

    // 2. Get all distribution history
    let allDistributions = distributionHistory;

    // 3. For each combination of timeframe and price type, compute leaderboard
    leaderboards := {
      oneWeekUSD = computeLeaderboardFor(neuronToPrincipals, #OneWeek, #USD, allDistributions);
      oneWeekICP = computeLeaderboardFor(neuronToPrincipals, #OneWeek, #ICP, allDistributions);
      oneMonthUSD = computeLeaderboardFor(neuronToPrincipals, #OneMonth, #USD, allDistributions);
      oneMonthICP = computeLeaderboardFor(neuronToPrincipals, #OneMonth, #ICP, allDistributions);
      oneYearUSD = computeLeaderboardFor(neuronToPrincipals, #OneYear, #USD, allDistributions);
      oneYearICP = computeLeaderboardFor(neuronToPrincipals, #OneYear, #ICP, allDistributions);
      allTimeUSD = computeLeaderboardFor(neuronToPrincipals, #AllTime, #USD, allDistributions);
      allTimeICP = computeLeaderboardFor(neuronToPrincipals, #AllTime, #ICP, allDistributions);
    };

    leaderboardLastUpdate := Time.now();

    logger.info("Leaderboard", "Leaderboard computation completed successfully", "computeAllLeaderboards");
  };

  private func computeLeaderboardFor(
    neuronToPrincipals: Map.Map<Blob, [Principal]>,
    timeframe: LeaderboardTimeframe,
    priceType: LeaderboardPriceType,
    allDistributions: Vector.Vector<DistributionRecord>
  ) : [LeaderboardEntry] {

    // 1. Determine which distributions to include based on timeframe
    let distributionsToAnalyze = selectDistributions(allDistributions, timeframe);

    // 2. Aggregate per-neuron performance scores
    type NeuronPerformance = {
      neuronId: Blob;
      performanceScores: [Float];  // One per distribution
      distributionCount: Nat;
      lastActivity: Int;
    };

    let neuronPerformances = Map.new<Blob, NeuronPerformance>();

    for (distribution in distributionsToAnalyze.vals()) {
      for (neuronReward in distribution.neuronRewards.vals()) {
        // Select the correct performance score based on price type
        let score : Float = switch (priceType) {
          case (#USD) { neuronReward.performanceScore };
          case (#ICP) {
            switch (neuronReward.performanceScoreICP) {
              case (?icpScore) { icpScore };
              case null { neuronReward.performanceScore }; // Fallback for old data without ICP score
            }
          };
        };

        let existingPerf = Map.get(neuronPerformances, bhash, neuronReward.neuronId);

        switch (existingPerf) {
          case (?existing) {
            // Append this period's performance score
            let updatedScores = Array.append(existing.performanceScores, [score]);
            Map.set(neuronPerformances, bhash, neuronReward.neuronId, {
              neuronId = existing.neuronId;
              performanceScores = updatedScores;
              distributionCount = existing.distributionCount + 1;
              lastActivity = Int.max(existing.lastActivity, distribution.endTime);
            });
          };
          case null {
            // First distribution for this neuron
            Map.set(neuronPerformances, bhash, neuronReward.neuronId, {
              neuronId = neuronReward.neuronId;
              performanceScores = [score];
              distributionCount = 1;
              lastActivity = distribution.endTime;
            });
          };
        };
      };
    };

    // 3. Calculate aggregate performance score for each neuron based on timeframe
    type ScoredNeuron = {
      neuronId: Blob;
      aggregateScore: Float;
      distributionCount: Nat;
      lastActivity: Int;
    };

    let scoredNeurons = Array.map<(Blob, NeuronPerformance), ScoredNeuron>(
      Iter.toArray(Map.entries(neuronPerformances)),
      func ((neuronId, perf)) : ScoredNeuron {
        let aggregateScore = switch (timeframe) {
          case (#OneWeek) {
            // Latest period only
            if (perf.performanceScores.size() > 0) {
              perf.performanceScores[perf.performanceScores.size() - 1]
            } else { 1.0 }
          };
          case (#OneMonth) {
            // Compound return of last ~4 periods
            calculateCompoundReturn(perf.performanceScores)
          };
          case (#OneYear) {
            // Compound return of all periods (up to 52)
            calculateCompoundReturn(perf.performanceScores)
          };
          case (#AllTime) {
            // Compound return: multiply all performance scores
            calculateCompoundReturn(perf.performanceScores)
          };
        };

        {
          neuronId;
          aggregateScore;
          distributionCount = perf.distributionCount;
          lastActivity = perf.lastActivity;
        }
      }
    );

    // 4. Group by Principal and select best neuron per user
    type UserPerformance = {
      principal: Principal;
      bestNeuron: ScoredNeuron;
    };

    let userPerformances = Map.new<Principal, UserPerformance>();

    for (scored in scoredNeurons.vals()) {
      switch (Map.get(neuronToPrincipals, bhash, scored.neuronId)) {
        case (?principals) {
          // For each principal owning this neuron
          for (principal in principals.vals()) {
            switch (Map.get(userPerformances, phash, principal)) {
              case (?existing) {
                // Keep neuron with most distributions (longest running)
                // Tie-breaker: higher performance score
                let shouldReplace =
                  scored.distributionCount > existing.bestNeuron.distributionCount or
                  (scored.distributionCount == existing.bestNeuron.distributionCount and
                   scored.aggregateScore > existing.bestNeuron.aggregateScore);

                if (shouldReplace) {
                  Map.set(userPerformances, phash, principal, {
                    principal;
                    bestNeuron = scored;
                  });
                };
              };
              case null {
                // First neuron for this user
                Map.set(userPerformances, phash, principal, {
                  principal;
                  bestNeuron = scored;
                });
              };
            };
          };
        };
        case null { /* Neuron has no owners, skip */ };
      };
    };

    // 5. Sort by aggregate performance score (descending)
    let sortedUsers = Array.sort<UserPerformance>(
      Iter.toArray(Map.vals(userPerformances)),
      func (a, b) {
        Float.compare(b.bestNeuron.aggregateScore, a.bestNeuron.aggregateScore)
      }
    );

    // 6. Take top N and create leaderboard entries
    let topN = Array.tabulate<LeaderboardEntry>(
      Nat.min(leaderboardSize, sortedUsers.size()),
      func (i) : LeaderboardEntry {
        let user = sortedUsers[i];
        {
          rank = i + 1;
          principal = user.principal;
          neuronId = user.bestNeuron.neuronId;
          performanceScore = user.bestNeuron.aggregateScore;
          distributionsCount = user.bestNeuron.distributionCount;
          lastActivity = user.bestNeuron.lastActivity;
          displayName = Map.get(displayNames, phash, user.principal);
        }
      }
    );

    topN
  };

  //=========================================================================
  // Leaderboard Helper Functions
  //=========================================================================

  private func selectDistributions(
    allDistributions: Vector.Vector<DistributionRecord>,
    timeframe: LeaderboardTimeframe
  ) : [DistributionRecord] {
    let now = Time.now();
    let count = Vector.size(allDistributions);

    // Calculate cutoff time based on timeframe (calendar-based)
    let timeframeCutoff : Int = switch (timeframe) {
      case (#OneWeek) { now - 7 * 24 * 60 * 60 * 1_000_000_000 };      // 7 days in ns
      case (#OneMonth) { now - 31 * 24 * 60 * 60 * 1_000_000_000 };    // 31 days in ns
      case (#OneYear) { now - 365 * 24 * 60 * 60 * 1_000_000_000 };    // 365 days in ns
      case (#AllTime) { 0 };  // No time-based cutoff for AllTime
    };

    // Use the later of timeframe cutoff and global cutoff date
    // This means cutoff affects ALL timeframes (week, month, year, allTime)
    let effectiveCutoff = Int.max(timeframeCutoff, leaderboardCutoffDate);

    // Filter distributions that end after the cutoff
    let filtered = Vector.new<DistributionRecord>();
    for (i in Iter.range(0, count - 1)) {
      let dist = Vector.get(allDistributions, i);
      if (dist.endTime >= effectiveCutoff) {
        Vector.add(filtered, dist);
      };
    };

    Vector.toArray(filtered)
  };

  private func calculateAverage(scores: [Float]) : Float {
    if (scores.size() == 0) return 1.0;

    var sum : Float = 0.0;
    for (score in scores.vals()) {
      sum += score;
    };
    sum / Float.fromInt(scores.size())
  };

  private func calculateCompoundReturn(scores: [Float]) : Float {
    if (scores.size() == 0) return 1.0;

    var compound : Float = 1.0;
    for (score in scores.vals()) {
      compound *= score;
    };
    compound
  };

  //=========================================================================
  // Individual Performance Lookup Functions
  //=========================================================================

  // Get performance for a single neuron across all timeframes
  public query func getNeuronPerformance(neuronId: Blob) : async Result.Result<NeuronPerformanceDetail, RewardsError> {
    // Aggregate performance scores from distribution history
    var oneWeekScoresUSD = Buffer.Buffer<Float>(1);
    var oneWeekScoresICP = Buffer.Buffer<Float>(1);
    var oneMonthScoresUSD = Buffer.Buffer<Float>(4);
    var oneMonthScoresICP = Buffer.Buffer<Float>(4);
    var oneYearScoresUSD = Buffer.Buffer<Float>(52);
    var oneYearScoresICP = Buffer.Buffer<Float>(52);
    var allTimeScoresUSD = Buffer.Buffer<Float>(100);
    var allTimeScoresICP = Buffer.Buffer<Float>(100);
    var lastActivity : Int = 0;
    var votingPower : Nat = 0;

    let distributionCount = Vector.size(distributionHistory);

    if (distributionCount == 0) {
      return #err(#NeuronNotFound);
    };

    var foundNeuron = false;

    // Iterate from oldest to newest for proper ordering
    for (i in Iter.range(0, distributionCount - 1)) {
      let dist = Vector.get(distributionHistory, i);

      for (reward in dist.neuronRewards.vals()) {
        if (reward.neuronId == neuronId) {
          foundNeuron := true;
          let distIndex = distributionCount - 1 - i; // 0 = most recent

          // Update voting power (use most recent)
          votingPower := reward.votingPower;

          // USD scores
          if (distIndex == 0) oneWeekScoresUSD.add(reward.performanceScore);
          if (distIndex < 4) oneMonthScoresUSD.add(reward.performanceScore);
          if (distIndex < 52) oneYearScoresUSD.add(reward.performanceScore);
          allTimeScoresUSD.add(reward.performanceScore);

          // ICP scores
          switch (reward.performanceScoreICP) {
            case (?icpScore) {
              if (distIndex == 0) oneWeekScoresICP.add(icpScore);
              if (distIndex < 4) oneMonthScoresICP.add(icpScore);
              if (distIndex < 52) oneYearScoresICP.add(icpScore);
              allTimeScoresICP.add(icpScore);
            };
            case null {};
          };

          lastActivity := Int.max(lastActivity, dist.endTime);
        };
      };
    };

    if (not foundNeuron) {
      return #err(#NeuronNotFound);
    };

    #ok({
      neuronId;
      votingPower;
      performance = {
        oneWeekUSD = if (oneWeekScoresUSD.size() > 0) ?oneWeekScoresUSD.get(0) else null;
        oneWeekICP = if (oneWeekScoresICP.size() > 0) ?oneWeekScoresICP.get(0) else null;
        oneMonthUSD = if (oneMonthScoresUSD.size() > 0) ?calculateAverage(Buffer.toArray(oneMonthScoresUSD)) else null;
        oneMonthICP = if (oneMonthScoresICP.size() > 0) ?calculateAverage(Buffer.toArray(oneMonthScoresICP)) else null;
        oneYearUSD = if (oneYearScoresUSD.size() > 0) ?calculateAverage(Buffer.toArray(oneYearScoresUSD)) else null;
        oneYearICP = if (oneYearScoresICP.size() > 0) ?calculateAverage(Buffer.toArray(oneYearScoresICP)) else null;
        allTimeUSD = if (allTimeScoresUSD.size() > 0) ?calculateCompoundReturn(Buffer.toArray(allTimeScoresUSD)) else null;
        allTimeICP = if (allTimeScoresICP.size() > 0) ?calculateCompoundReturn(Buffer.toArray(allTimeScoresICP)) else null;
      };
      distributionsParticipated = allTimeScoresUSD.size();
      lastAllocationChange = lastActivity;
    })
  };

  // Get user performance graph data from precomputed distribution history
  // This is much more efficient than calculateNeuronPerformanceQuery as it reads stored data
  // Use this for performance visualization graphs
  // Takes userPrincipal and looks up their neurons from DAO, then reads checkpoints locally
  // Returns all neurons owned by the principal with full checkpoint data and per-timeframe scores
  public composite query func getUserPerformanceGraphData(
    userPrincipal: Principal, // User to get graph data for
    startTime: Int,           // Start of time range (nanoseconds)
    endTime: Int              // End of time range (nanoseconds)
  ) : async Result.Result<UserPerformanceGraphData, RewardsError> {
    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };

    // Step 1: Get user's neurons from DAO (composite query call)
    let neuronOwners = try {
      await (with timeout = 65) daoCanister.getActiveDecisionMakers()
    } catch (e) {
      logger.warn("Leaderboard", "getActiveDecisionMakers failed, falling back to getAllNeuronOwners: " # Error.message(e), "computeAllLeaderboards");
      try {
        await (with timeout = 65) daoCanister.getAllNeuronOwners()
      } catch (e2) {
        logger.error("Leaderboard", "Failed to fetch neuron owners from DAO: " # Error.message(e2), "computeAllLeaderboards");
        return #err(#SystemError("Failed to fetch neuron owners from DAO"));
      }
    };

    // Step 2: Find neurons owned by this principal
    let neuronIds = Buffer.Buffer<Blob>(5);
    for ((neuronId, principals) in neuronOwners.vals()) {
      for (p in principals.vals()) {
        if (Principal.equal(p, userPrincipal)) {
          neuronIds.add(neuronId);
        };
      };
    };

    if (neuronIds.size() == 0) {
      return #err(#NeuronNotFound);
    };

    // Step 3: Read checkpoints from distribution history (local, fast)
    let totalDistributions = Vector.size(distributionHistory);

    if (totalDistributions == 0) {
      return #err(#NeuronNotFound);
    };

    // Create a map to track data per neuron
    let neuronDataMap = Buffer.Buffer<{
      neuronId: Blob;
      checkpoints: Buffer.Buffer<GraphCheckpointData>;
      performanceUSD: Float;
      performanceICP: Float;
      hasIcpScore: Bool;
      found: Bool;
    }>(neuronIds.size());

    // Initialize tracking for each neuron
    for (nid in neuronIds.vals()) {
      neuronDataMap.add({
        neuronId = nid;
        checkpoints = Buffer.Buffer<GraphCheckpointData>(20);
        performanceUSD = 1.0;
        performanceICP = 1.0;
        hasIcpScore = false;
        found = false;
      });
    };

    var actualStartTime : Int = endTime;
    var actualEndTime : Int = startTime;

    // Iterate through distributions in chronological order (oldest first for proper checkpoint ordering)
    for (distIndex in Iter.range(0, totalDistributions - 1)) {
      let dist = Vector.get(distributionHistory, distIndex);

      // Skip distributions outside our time range OR before cutoff date
      if (dist.endTime < startTime or dist.startTime > endTime or dist.endTime < leaderboardCutoffDate) {
        // Skip this distribution
      } else {
        // Track actual time bounds
        if (dist.startTime < actualStartTime) { actualStartTime := dist.startTime };
        if (dist.endTime > actualEndTime) { actualEndTime := dist.endTime };

        // Look for each neuron's data in this distribution
        for (reward in dist.neuronRewards.vals()) {
          // Check if this neuron is in our list
          for (i in Iter.range(0, neuronDataMap.size() - 1)) {
            let entry = neuronDataMap.get(i);
            if (entry.neuronId == reward.neuronId) {
              // Scale checkpoints so totalPortfolioValue is cumulative across distributions
              // entry.performanceUSD is the compounded score from all PRIOR distributions
              let priorPerformance = entry.performanceUSD;
              let priorIcpPerformance = entry.performanceICP;
              let distUsdScore = reward.performanceScore;
              let distIcpScore : Float = switch (reward.performanceScoreICP) {
                case (?icp) { icp };
                case null { reward.performanceScore }; // fallback to USD
              };

              for (checkpoint in reward.checkpoints.vals()) {
                let scaledUSD = priorPerformance * checkpoint.totalPortfolioValue;

                // Linear interpolation: how much of this distribution's USD return is at this checkpoint
                let rawCP = checkpoint.totalPortfolioValue; // raw value before cumulative scaling
                let scaledICP : Float = if (Float.abs(distUsdScore - 1.0) > 1e-10) {
                  // Interpolate ICP based on how far through the USD return this checkpoint is
                  let fraction = (rawCP - 1.0) / (distUsdScore - 1.0);
                  priorIcpPerformance * (1.0 + fraction * (distIcpScore - 1.0))
                } else {
                  // USD barely changed — use raw checkpoint progress to interpolate ICP directly
                  // This handles the case where ICP moved significantly even though USD didn't
                  let checkpointCount = reward.checkpoints.size();
                  if (checkpointCount > 1) {
                    // Use position within checkpoints as fraction of distribution progress
                    var cpIdx : Nat = 0;
                    label findIdx for (cp in reward.checkpoints.vals()) {
                      if (cp.timestamp == checkpoint.timestamp) { break findIdx };
                      cpIdx += 1;
                    };
                    let fraction = Float.fromInt(cpIdx) / Float.fromInt(checkpointCount - 1);
                    priorIcpPerformance * (1.0 + fraction * (distIcpScore - 1.0))
                  } else {
                    // Single checkpoint — apply full ICP score
                    priorIcpPerformance * distIcpScore
                  }
                };

                entry.checkpoints.add({
                  timestamp = checkpoint.timestamp;
                  allocations = checkpoint.allocations;
                  tokenValues = checkpoint.tokenValues;
                  totalPortfolioValue = scaledUSD;
                  totalPortfolioValueICP = scaledICP;
                  pricesUsed = checkpoint.pricesUsed;
                  maker = checkpoint.maker;
                  reason = checkpoint.reason;
                });
              };

              // Compound performance scores (AFTER using priorPerformance for scaling)
              let newPerfUSD = entry.performanceUSD * reward.performanceScore;
              // Use same fallback as interpolation: if no ICP score, compound with USD
              let newPerfICP = entry.performanceICP * distIcpScore;
              let newHasIcp = entry.hasIcpScore or Option.isSome(reward.performanceScoreICP);

              // Update the entry (recreate since records are immutable)
              neuronDataMap.put(i, {
                neuronId = entry.neuronId;
                checkpoints = entry.checkpoints;
                performanceUSD = newPerfUSD;
                performanceICP = newPerfICP;
                hasIcpScore = newHasIcp;
                found = true;
              });
            };
          };
        };
      };
    };

    // Build the result - collect all neurons with their scores
    var foundAny = false;
    let distCount = Vector.size(distributionHistory);

    // Track aggregated performance and allocation neuron
    var totalAggregatedUSD : Float = 0.0;
    var totalAggregatedICP : Float = 0.0;
    var hasAnyIcp = false;
    var mostAllocChanges : Nat = 0;
    var allocationNeuronId : ?Blob = null;

    // Track earliest and latest checkpoint timestamps across all neurons
    var earliestCheckpoint : Int = 9223372036854775807; // Max Int
    var latestCheckpoint : Int = 0;

    // Build neurons array with all data
    let neuronsVec = Vector.new<NeuronGraphDataExtended>();

    for (i in Iter.range(0, neuronDataMap.size() - 1)) {
      let entry = neuronDataMap.get(i);
      if (entry.found) {
        foundAny := true;

        // Track min/max checkpoint timestamps
        for (cp in entry.checkpoints.vals()) {
          if (cp.timestamp < earliestCheckpoint) { earliestCheckpoint := cp.timestamp };
          if (cp.timestamp > latestCheckpoint) { latestCheckpoint := cp.timestamp };
        };

        // Collect per-distribution scores for this neuron
        var weekScoresUSD = Vector.new<Float>();
        var weekScoresICP = Vector.new<Float>();
        var monthScoresUSD = Vector.new<Float>();
        var monthScoresICP = Vector.new<Float>();
        var yearScoresUSD = Vector.new<Float>();
        var yearScoresICP = Vector.new<Float>();
        var allTimeScoresUSD = Vector.new<Float>();
        var allTimeScoresICP = Vector.new<Float>();

        // Count allocation changes across distributions
        var allocChangeCount : Nat = 0;
        var prevAllocations : ?[{ token: Principal; basisPoints: Nat }] = null;

        // Calendar-based timeframe cutoffs (matching leaderboard calculation)
        let now = Time.now();
        let weekCutoff = Int.max(now - 7 * 24 * 60 * 60 * 1_000_000_000, leaderboardCutoffDate);
        let monthCutoff = Int.max(now - 31 * 24 * 60 * 60 * 1_000_000_000, leaderboardCutoffDate);
        let yearCutoff = Int.max(now - 365 * 24 * 60 * 60 * 1_000_000_000, leaderboardCutoffDate);
        let allTimeCutoff = leaderboardCutoffDate;

        for (di in Iter.range(0, distCount - 1)) {
          let dist = Vector.get(distributionHistory, di);

          // Skip distributions before cutoff date
          if (dist.endTime < leaderboardCutoffDate) {
            // Skip this distribution
          } else {
            for (reward in dist.neuronRewards.vals()) {
              if (reward.neuronId == entry.neuronId) {
                let usdScore = reward.performanceScore;
                let icpScoreVal = switch (reward.performanceScoreICP) {
                  case (?s) { s };
                  case null { reward.performanceScore };
                };

                // Calendar-based timeframe inclusion
                if (dist.endTime >= weekCutoff) { Vector.add(weekScoresUSD, usdScore); Vector.add(weekScoresICP, icpScoreVal); };
                if (dist.endTime >= monthCutoff) { Vector.add(monthScoresUSD, usdScore); Vector.add(monthScoresICP, icpScoreVal); };
                if (dist.endTime >= yearCutoff) { Vector.add(yearScoresUSD, usdScore); Vector.add(yearScoresICP, icpScoreVal); };
                if (dist.endTime >= allTimeCutoff) { Vector.add(allTimeScoresUSD, usdScore); Vector.add(allTimeScoresICP, icpScoreVal); };

                // Count allocation changes by comparing checkpoint allocations
                for (cp in reward.checkpoints.vals()) {
                  let currentAlloc = cp.allocations;
                  switch (prevAllocations) {
                    case null {
                      // First allocation seen — record as baseline, not a change
                      prevAllocations := ?currentAlloc;
                    };
                    case (?prev) {
                      // Compare with previous allocation
                      var changed = false;
                      if (prev.size() != currentAlloc.size()) {
                        changed := true;
                      } else {
                        label checkAlloc for (j in Iter.range(0, prev.size() - 1)) {
                          if (prev[j].basisPoints != currentAlloc[j].basisPoints or
                              Principal.notEqual(prev[j].token, currentAlloc[j].token)) {
                            changed := true;
                            break checkAlloc;
                          };
                        };
                      };
                      if (changed) {
                        allocChangeCount += 1;
                        prevAllocations := ?currentAlloc;
                      };
                    };
                  };
                };
              };
            };
          };
        };

        // Compute all timeframe scores
        let owUSD = if (Vector.size(weekScoresUSD) > 0) ?Vector.get(weekScoresUSD, 0) else null;
        let owICP = if (Vector.size(weekScoresICP) > 0) ?Vector.get(weekScoresICP, 0) else null;
        let omUSD = if (Vector.size(monthScoresUSD) > 0) ?calculateCompoundReturn(Vector.toArray(monthScoresUSD)) else null;
        let omICP = if (Vector.size(monthScoresICP) > 0) ?calculateCompoundReturn(Vector.toArray(monthScoresICP)) else null;
        let oyUSD = if (Vector.size(yearScoresUSD) > 0) ?calculateCompoundReturn(Vector.toArray(yearScoresUSD)) else null;
        let oyICP = if (Vector.size(yearScoresICP) > 0) ?calculateCompoundReturn(Vector.toArray(yearScoresICP)) else null;
        let oaUSD = if (Vector.size(allTimeScoresUSD) > 0) ?calculateCompoundReturn(Vector.toArray(allTimeScoresUSD)) else null;
        let oaICP = if (Vector.size(allTimeScoresICP) > 0) ?calculateCompoundReturn(Vector.toArray(allTimeScoresICP)) else null;

        // Add neuron to the result array
        Vector.add(neuronsVec, {
          neuronId = entry.neuronId;
          checkpoints = Buffer.toArray(entry.checkpoints);
          performanceScoreUSD = entry.performanceUSD;
          performanceScoreICP = if (entry.hasIcpScore) ?entry.performanceICP else null;
          oneWeekUSD = owUSD;
          oneWeekICP = owICP;
          oneMonthUSD = omUSD;
          oneMonthICP = omICP;
          oneYearUSD = oyUSD;
          oneYearICP = oyICP;
          allocationChangeCount = allocChangeCount;
        });

        // Track aggregated performance
        totalAggregatedUSD += entry.performanceUSD;
        if (entry.hasIcpScore) {
          totalAggregatedICP += entry.performanceICP;
          hasAnyIcp := true;
        };

        // Track neuron with most allocation changes
        if (allocChangeCount > mostAllocChanges) {
          mostAllocChanges := allocChangeCount;
          allocationNeuronId := ?entry.neuronId;
        };
      };
    };

    if (not foundAny) {
      return #err(#NeuronNotFound);
    };

    #ok({
      timeframe = { startTime = earliestCheckpoint; endTime = latestCheckpoint };
      allocationNeuronId = allocationNeuronId;
      aggregatedPerformanceUSD = totalAggregatedUSD;
      aggregatedPerformanceICP = if (hasAnyIcp) ?totalAggregatedICP else null;
      neurons = Vector.toArray(neuronsVec);
    })
  };

  // Get performance for all neurons owned by a user
  // Uses composite query to call DAO canister query methods
  public composite query func getUserPerformance(userPrincipal: Principal) : async Result.Result<UserPerformanceResult, RewardsError> {
    // Step 1: Get all neurons owned by this user from DAO (composite query call)
    let neuronOwners = try {
      await (with timeout = 65) daoCanister.getActiveDecisionMakers()
    } catch (e) {
      logger.warn("Leaderboard", "getActiveDecisionMakers failed, falling back to getAllNeuronOwners: " # Error.message(e), "computeAllLeaderboards");
      try {
        await (with timeout = 65) daoCanister.getAllNeuronOwners()
      } catch (e2) {
        logger.error("Leaderboard", "Failed to fetch neuron owners from DAO: " # Error.message(e2), "computeAllLeaderboards");
        return #err(#SystemError("Failed to fetch neuron owners from DAO"));
      }
    };

    // Find neurons owned by this principal
    let userNeuronIds = Buffer.Buffer<Blob>(5);
    for ((neuronId, principals) in neuronOwners.vals()) {
      for (p in principals.vals()) {
        if (Principal.equal(p, userPrincipal)) {
          userNeuronIds.add(neuronId);
        };
      };
    };

    if (userNeuronIds.size() == 0) {
      return #err(#NeuronNotFound);
    };

    // Step 2: Get performance for each neuron (using the query function logic inline)
    let neuronDetails = Buffer.Buffer<NeuronPerformanceDetail>(userNeuronIds.size());
    var totalVP : Nat = 0;
    var maxDistributions : Nat = 0;
    var maxLastActivity : Int = 0;

    // Aggregate scores across all neurons for user-level performance
    var userOneWeekUSD = Buffer.Buffer<Float>(userNeuronIds.size());
    var userOneWeekICP = Buffer.Buffer<Float>(userNeuronIds.size());
    var userOneMonthUSD = Buffer.Buffer<Float>(userNeuronIds.size());
    var userOneMonthICP = Buffer.Buffer<Float>(userNeuronIds.size());
    var userOneYearUSD = Buffer.Buffer<Float>(userNeuronIds.size());
    var userOneYearICP = Buffer.Buffer<Float>(userNeuronIds.size());
    var userAllTimeUSD = Buffer.Buffer<Float>(userNeuronIds.size());
    var userAllTimeICP = Buffer.Buffer<Float>(userNeuronIds.size());

    for (neuronId in userNeuronIds.vals()) {
      // Inline neuron performance calculation (can't call query from shared)
      var oneWeekScoresUSD = Buffer.Buffer<Float>(1);
      var oneWeekScoresICP = Buffer.Buffer<Float>(1);
      var oneMonthScoresUSD = Buffer.Buffer<Float>(4);
      var oneMonthScoresICP = Buffer.Buffer<Float>(4);
      var oneYearScoresUSD = Buffer.Buffer<Float>(52);
      var oneYearScoresICP = Buffer.Buffer<Float>(52);
      var allTimeScoresUSD = Buffer.Buffer<Float>(100);
      var allTimeScoresICP = Buffer.Buffer<Float>(100);
      var lastActivity : Int = 0;
      var votingPower : Nat = 0;

      let distributionCount = Vector.size(distributionHistory);
      var foundNeuron = false;

      for (i in Iter.range(0, distributionCount - 1)) {
        let dist = Vector.get(distributionHistory, i);

        for (reward in dist.neuronRewards.vals()) {
          if (reward.neuronId == neuronId) {
            foundNeuron := true;
            let distIndex = distributionCount - 1 - i;

            votingPower := reward.votingPower;

            if (distIndex == 0) oneWeekScoresUSD.add(reward.performanceScore);
            if (distIndex < 4) oneMonthScoresUSD.add(reward.performanceScore);
            if (distIndex < 52) oneYearScoresUSD.add(reward.performanceScore);
            allTimeScoresUSD.add(reward.performanceScore);

            switch (reward.performanceScoreICP) {
              case (?icpScore) {
                if (distIndex == 0) oneWeekScoresICP.add(icpScore);
                if (distIndex < 4) oneMonthScoresICP.add(icpScore);
                if (distIndex < 52) oneYearScoresICP.add(icpScore);
                allTimeScoresICP.add(icpScore);
              };
              case null {};
            };

            lastActivity := Int.max(lastActivity, dist.endTime);
          };
        };
      };

      if (foundNeuron) {
        let detail : NeuronPerformanceDetail = {
          neuronId;
          votingPower;
          performance = {
            oneWeekUSD = if (oneWeekScoresUSD.size() > 0) ?oneWeekScoresUSD.get(0) else null;
            oneWeekICP = if (oneWeekScoresICP.size() > 0) ?oneWeekScoresICP.get(0) else null;
            oneMonthUSD = if (oneMonthScoresUSD.size() > 0) ?calculateCompoundReturn(Buffer.toArray(oneMonthScoresUSD)) else null;
            oneMonthICP = if (oneMonthScoresICP.size() > 0) ?calculateCompoundReturn(Buffer.toArray(oneMonthScoresICP)) else null;
            oneYearUSD = if (oneYearScoresUSD.size() > 0) ?calculateCompoundReturn(Buffer.toArray(oneYearScoresUSD)) else null;
            oneYearICP = if (oneYearScoresICP.size() > 0) ?calculateCompoundReturn(Buffer.toArray(oneYearScoresICP)) else null;
            allTimeUSD = if (allTimeScoresUSD.size() > 0) ?calculateCompoundReturn(Buffer.toArray(allTimeScoresUSD)) else null;
            allTimeICP = if (allTimeScoresICP.size() > 0) ?calculateCompoundReturn(Buffer.toArray(allTimeScoresICP)) else null;
          };
          distributionsParticipated = allTimeScoresUSD.size();
          lastAllocationChange = lastActivity;
        };

        neuronDetails.add(detail);
        totalVP += detail.votingPower;
        maxDistributions := Nat.max(maxDistributions, detail.distributionsParticipated);
        maxLastActivity := Int.max(maxLastActivity, detail.lastAllocationChange);

        // Collect scores for aggregation
        switch (detail.performance.oneWeekUSD) { case (?v) userOneWeekUSD.add(v); case null {} };
        switch (detail.performance.oneWeekICP) { case (?v) userOneWeekICP.add(v); case null {} };
        switch (detail.performance.oneMonthUSD) { case (?v) userOneMonthUSD.add(v); case null {} };
        switch (detail.performance.oneMonthICP) { case (?v) userOneMonthICP.add(v); case null {} };
        switch (detail.performance.oneYearUSD) { case (?v) userOneYearUSD.add(v); case null {} };
        switch (detail.performance.oneYearICP) { case (?v) userOneYearICP.add(v); case null {} };
        switch (detail.performance.allTimeUSD) { case (?v) userAllTimeUSD.add(v); case null {} };
        switch (detail.performance.allTimeICP) { case (?v) userAllTimeICP.add(v); case null {} };
      };
    };

    if (neuronDetails.size() == 0) {
      return #err(#NeuronNotFound);
    };

    // Aggregate user performance: use the BEST performing neuron for each timeframe (same as leaderboard)
    let findBest = func (scores: Buffer.Buffer<Float>) : ?Float {
      if (scores.size() == 0) return null;
      var best : Float = scores.get(0);
      for (score in scores.vals()) {
        if (score > best) { best := score };
      };
      ?best
    };

    #ok({
      principal = userPrincipal;
      neurons = Buffer.toArray(neuronDetails);
      aggregatedPerformance = {
        oneWeekUSD = findBest(userOneWeekUSD);
        oneWeekICP = findBest(userOneWeekICP);
        oneMonthUSD = findBest(userOneMonthUSD);
        oneMonthICP = findBest(userOneMonthICP);
        oneYearUSD = findBest(userOneYearUSD);
        oneYearICP = findBest(userOneYearICP);
        allTimeUSD = findBest(userAllTimeUSD);
        allTimeICP = findBest(userAllTimeICP);
      };
      totalVotingPower = totalVP;
      distributionsParticipated = maxDistributions;
      lastActivity = maxLastActivity;
    })
  };

  private func isAdmin(caller: Principal) : Bool {
    AdminAuth.isAdmin(caller, canister_ids.isKnownCanister)
  };

  //=========================================================================
  // Display Name Helpers
  //=========================================================================

  // Normalize text for banned word checking: lowercase + strip non-alphanumeric
  // Defeats bypass attempts like "FaGgOt", "f-a-g-g-o-t", "f.a.g.g.o.t", "F A G"
  private func normalizeForBannedCheck(input: Text) : Text {
    let lower = Text.toLowercase(input);
    var result = "";
    for (c in lower.chars()) {
      if (Char.isAlphabetic(c) or Char.isDigit(c)) {
        result := result # Char.toText(c);
      };
    };
    result
  };

  // Check if normalized input contains any banned word as a substring
  // Returns the first matching banned word, or null if clean
  private func containsBannedWord(normalizedInput: Text) : ?Text {
    for (word in bannedWords.vals()) {
      if (Text.contains(normalizedInput, #text word)) {
        return ?word;
      };
    };
    null
  };

  // Full validation of a display name
  private func validateDisplayName(name: Text) : Result.Result<Text, RewardsError> {
    let trimmed = Text.trim(name, #predicate(func(c: Char) : Bool { Char.isWhitespace(c) }));

    let len = trimmed.size();
    if (len < 2) {
      return #err(#InvalidDisplayName("Display name must be at least 2 characters"));
    };
    if (len > 24) {
      return #err(#InvalidDisplayName("Display name must be at most 24 characters"));
    };

    // Allowed characters: letters, digits, spaces, underscores, hyphens
    for (c in trimmed.chars()) {
      if (not (Char.isAlphabetic(c) or Char.isDigit(c) or c == '_' or c == '-' or c == ' ')) {
        return #err(#InvalidDisplayName("Display name can only contain letters, numbers, spaces, underscores, and hyphens"));
      };
    };

    // Banned word check on normalized version
    let normalized = normalizeForBannedCheck(trimmed);
    switch (containsBannedWord(normalized)) {
      case (?_word) {
        return #err(#InvalidDisplayName("Display name contains a prohibited word"));
      };
      case null {};
    };

    #ok(trimmed)
  };

  //=========================================================================
  // Display Name Functions
  //=========================================================================

  // User sets their own display name
  public shared ({ caller }) func setDisplayName(name: Text) : async Result.Result<Text, RewardsError> {
    switch (validateDisplayName(name)) {
      case (#err(e)) { return #err(e) };
      case (#ok(validName)) {
        Map.set(displayNames, phash, caller, validName);
        #ok("Display name set to: " # validName)
      };
    };
  };

  // User deletes their own display name
  public shared ({ caller }) func deleteMyDisplayName() : async Result.Result<Text, RewardsError> {
    Map.delete(displayNames, phash, caller);
    #ok("Display name removed")
  };

  // Admin deletes any user's display name
  public shared ({ caller }) func adminDeleteDisplayName(target: Principal) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) { return #err(#NotAuthorized) };
    Map.delete(displayNames, phash, target);
    #ok("Display name removed for " # Principal.toText(target))
  };

  // Query a single user's display name
  public query func getDisplayName(user: Principal) : async ?Text {
    Map.get(displayNames, phash, user)
  };

  // Batch query display names for multiple principals
  public query func getDisplayNames(users: [Principal]) : async [(Principal, ?Text)] {
    Array.map<Principal, (Principal, ?Text)>(users, func (p) {
      (p, Map.get(displayNames, phash, p))
    })
  };

  // Admin adds banned words (normalized: lowercase, alpha-only, deduplicated)
  public shared ({ caller }) func addBannedWords(words: [Text]) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) { return #err(#NotAuthorized) };

    let newWords = Buffer.Buffer<Text>(bannedWords.size() + words.size());
    for (w in bannedWords.vals()) { newWords.add(w) };

    var added : Nat = 0;
    for (word in words.vals()) {
      let normalized = normalizeForBannedCheck(word);
      if (normalized.size() > 0) {
        var exists = false;
        for (existing in bannedWords.vals()) {
          if (existing == normalized) { exists := true };
        };
        if (not exists) {
          newWords.add(normalized);
          added += 1;
        };
      };
    };

    bannedWords := Buffer.toArray(newWords);
    #ok("Added " # Nat.toText(added) # " banned words. Total: " # Nat.toText(bannedWords.size()))
  };

  // Admin removes banned words
  public shared ({ caller }) func removeBannedWords(words: [Text]) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) { return #err(#NotAuthorized) };

    let normalizedToRemove = Array.map<Text, Text>(words, normalizeForBannedCheck);
    bannedWords := Array.filter<Text>(bannedWords, func (w) {
      not Array.foldLeft<Text, Bool>(normalizedToRemove, false, func (found, r) {
        found or (r == w)
      })
    });

    #ok("Banned words updated. Total: " # Nat.toText(bannedWords.size()))
  };

  // Admin views banned word list
  public shared ({ caller }) func getBannedWords() : async Result.Result<[Text], RewardsError> {
    if (not isAdmin(caller)) { return #err(#NotAuthorized) };
    #ok(bannedWords)
  };

  //=========================================================================
  // Admin Functions
  //=========================================================================

  // Recalculate ICP performance scores for all distributions using derived prices
  // This fixes incorrect stored icpPrice values by deriving them from correct USD prices
  // Uses yield points (await (with timeout = 65)  async {}) to avoid instruction limit
  public shared ({ caller }) func admin_recalculateAllIcpPerformance() : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    let totalDistributions = Vector.size(distributionHistory);
    var updatedCount : Nat = 0;
    var neuronsProcessed : Nat = 0;

    for (i in Iter.range(0, totalDistributions - 1)) {
      let existingRecord = Vector.get(distributionHistory, i);

      // Create buffer for updated neuron rewards
      let updatedRewardsBuffer = Buffer.Buffer<NeuronReward>(existingRecord.neuronRewards.size());

      for (neuronReward in existingRecord.neuronRewards.vals()) {
        // Recalculate ICP performance using derived prices
        let newIcpScore = recomputePerformanceFromCheckpointsDerived(neuronReward.checkpoints, neuronReward.performanceScore);

        // Create updated reward preserving all other fields
        let updatedReward : NeuronReward = {
          neuronId = neuronReward.neuronId;
          performanceScore = neuronReward.performanceScore;    // USD score unchanged
          performanceScoreICP = newIcpScore;                   // Recalculated
          votingPower = neuronReward.votingPower;
          rewardScore = neuronReward.rewardScore;
          rewardAmount = neuronReward.rewardAmount;
          checkpoints = neuronReward.checkpoints;
        };
        updatedRewardsBuffer.add(updatedReward);
        neuronsProcessed += 1;

        // Yield every 50 neurons to avoid instruction limit
        if (neuronsProcessed % 50 == 0) {
          await (with timeout = 65)  async {};
        };
      };

      // Create updated distribution record
      let updatedRecord : DistributionRecord = {
        id = existingRecord.id;
        startTime = existingRecord.startTime;
        endTime = existingRecord.endTime;
        distributionTime = existingRecord.distributionTime;
        totalRewardPot = existingRecord.totalRewardPot;
        actualDistributed = existingRecord.actualDistributed;
        totalRewardScore = existingRecord.totalRewardScore;
        neuronsProcessed = existingRecord.neuronsProcessed;
        neuronRewards = Buffer.toArray(updatedRewardsBuffer);
        failedNeurons = existingRecord.failedNeurons;
        status = existingRecord.status;
      };

      Vector.put(distributionHistory, i, updatedRecord);
      updatedCount += 1;

      // Also yield after each distribution
      await (with timeout = 65)  async {};
    };

    #ok("Recalculated ICP performance for " # Nat.toText(updatedCount) # " distributions (" # Nat.toText(neuronsProcessed) # " neurons)")
  };

  // Recalculate ICP performance for a single distribution (for testing/debugging)
  public shared ({ caller }) func admin_recalculateIcpPerformanceForDistribution(
    distributionId: Nat
  ) : async Result.Result<Text, RewardsError> {
    if (not isAdmin(caller)) {
      return #err(#NotAuthorized);
    };

    // Find distribution by ID
    var foundIndex : ?Nat = null;
    let totalDistributions = Vector.size(distributionHistory);
    for (i in Iter.range(0, totalDistributions - 1)) {
      let record = Vector.get(distributionHistory, i);
      if (record.id == distributionId) {
        foundIndex := ?i;
      };
    };

    switch (foundIndex) {
      case (?index) {
        let existingRecord = Vector.get(distributionHistory, index);

        // Create buffer for updated neuron rewards
        let updatedRewardsBuffer = Buffer.Buffer<NeuronReward>(existingRecord.neuronRewards.size());
        var neuronsProcessed : Nat = 0;

        for (neuronReward in existingRecord.neuronRewards.vals()) {
          let newIcpScore = recomputePerformanceFromCheckpointsDerived(neuronReward.checkpoints, neuronReward.performanceScore);

          let updatedReward : NeuronReward = {
            neuronId = neuronReward.neuronId;
            performanceScore = neuronReward.performanceScore;
            performanceScoreICP = newIcpScore;
            votingPower = neuronReward.votingPower;
            rewardScore = neuronReward.rewardScore;
            rewardAmount = neuronReward.rewardAmount;
            checkpoints = neuronReward.checkpoints;
          };
          updatedRewardsBuffer.add(updatedReward);
          neuronsProcessed += 1;

          // Yield every 50 neurons
          if (neuronsProcessed % 50 == 0) {
            await (with timeout = 65)  async {};
          };
        };

        let updatedRecord : DistributionRecord = {
          id = existingRecord.id;
          startTime = existingRecord.startTime;
          endTime = existingRecord.endTime;
          distributionTime = existingRecord.distributionTime;
          totalRewardPot = existingRecord.totalRewardPot;
          actualDistributed = existingRecord.actualDistributed;
          totalRewardScore = existingRecord.totalRewardScore;
          neuronsProcessed = existingRecord.neuronsProcessed;
          neuronRewards = Buffer.toArray(updatedRewardsBuffer);
          failedNeurons = existingRecord.failedNeurons;
          status = existingRecord.status;
        };

        Vector.put(distributionHistory, index, updatedRecord);
        #ok("Recalculated ICP performance for distribution #" # Nat.toText(distributionId) # " (" # Nat.toText(neuronsProcessed) # " neurons)")
      };
      case null {
        #err(#SystemError("Distribution not found with ID: " # Nat.toText(distributionId)))
      };
    };
  };

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
      let neuronsResult = await (with timeout = 65)  snsGov.list_neurons({
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

      let transferResult = await (with timeout = 65)  tacoLedger.icrc1_transfer(transferArgs);
      
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
    let canisterBalance = await (with timeout = 65)  getTacoBalance();
    let currentNeuronBalances = await (with timeout = 65)  getCurrentTotalNeuronBalances();
    
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

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };

  //=========================================================================
  // Logging Functions
  //=========================================================================

  /**
   * Get the last N log entries
   */
  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    if (not isAdmin(caller)) {
      return [];
    };
    logger.getLastLogs(count);
  };

  /**
   * Get the last N log entries for a specific context
   */
  public query ({ caller }) func getLogsByContext(context : Text, count : Nat) : async [Logger.LogEntry] {
    if (not isAdmin(caller)) {
      return [];
    };
    logger.getContextLogs(context, count);
  };

  /**
   * Get the last N log entries for a specific level
   */
  public query ({ caller }) func getLogsByLevel(level : Logger.LogLevel, count : Nat) : async [Logger.LogEntry] {
    if (not isAdmin(caller)) {
      return [];
    };
    logger.getLogsByLevel(level, count);
  };

  /**
   * Clear all logs
   */
  public shared ({ caller }) func clearLogs() : async () {
    if (not isAdmin(caller)) {
      return;
    };
    logger.clearLogs();
  };

}

