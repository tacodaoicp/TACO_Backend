import Vector "mo:vector";
import Array "mo:base/Array";
import Map "mo:map/Map";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Blob "mo:base/Blob";

module {
  //==========================================================================
  // OLD TYPES (from deployed canister) - these must EXACTLY match what's deployed
  //==========================================================================

  // Old Allocation: token/basisPoints (same as new)
  public type OldAllocation = {
    token: Principal;
    basisPoints: Nat;
  };

  // Old PriceInfo: icpPrice/usdPrice/timestamp (same as new)
  public type OldPriceInfo = {
    icpPrice: Nat;
    usdPrice: Float;
    timestamp: Int;
  };

  // Old CheckpointData (same as new)
  public type OldCheckpointData = {
    timestamp: Int;
    allocations: [OldAllocation];
    tokenValues: [(Principal, Float)];
    totalPortfolioValue: Float;
    pricesUsed: [(Principal, OldPriceInfo)];
    maker: ?Principal;
  };

  // Old NeuronReward (WITHOUT performanceScoreICP - this is the key difference)
  public type OldNeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    // performanceScoreICP was NOT in old version
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Nat;
    checkpoints: [OldCheckpointData];
  };

  public type OldFailedNeuron = {
    neuronId: Blob;
    errorMessage: Text;
  };

  public type OldDistributionStatus = {
    #InProgress: {currentNeuron: Nat; totalNeurons: Nat};
    #Completed;
    #Failed: Text;
    #PartiallyCompleted: {successfulNeurons: Nat; failedNeurons: Nat};
  };

  public type OldDistributionRecord = {
    id: Nat;
    startTime: Int;
    endTime: Int;
    distributionTime: Int;
    totalRewardPot: Nat;
    actualDistributed: Nat;
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [OldNeuronReward];
    failedNeurons: [OldFailedNeuron];
    status: OldDistributionStatus;
  };

  // Old WithdrawalRecord: uses [Nat8] for subaccount (same as ICRC standard)
  public type OldWithdrawalRecord = {
    id: Nat;
    caller: Principal;
    neuronWithdrawals: [(Blob, Nat)];
    totalAmount: Nat;
    amountSent: Nat;
    fee: Nat;
    targetAccount: {owner: Principal; subaccount: ?[Nat8]};
    timestamp: Int;
    transactionId: ?Nat;
  };

  //==========================================================================
  // NEW TYPES (current code)
  //==========================================================================

  // New Allocation (same as old)
  public type NewAllocation = {
    token: Principal;
    basisPoints: Nat;
  };

  // New PriceInfo (same as old)
  public type NewPriceInfo = {
    icpPrice: Nat;
    usdPrice: Float;
    timestamp: Int;
  };

  // New CheckpointData (same as old)
  public type NewCheckpointData = {
    timestamp: Int;
    allocations: [NewAllocation];
    tokenValues: [(Principal, Float)];
    totalPortfolioValue: Float;
    pricesUsed: [(Principal, NewPriceInfo)];
    maker: ?Principal;
  };

  // New NeuronReward (WITH performanceScoreICP - the key change)
  public type NewNeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    performanceScoreICP: ?Float;  // NEW FIELD
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Nat;
    checkpoints: [NewCheckpointData];
  };

  public type NewFailedNeuron = {
    neuronId: Blob;
    errorMessage: Text;
  };

  public type NewDistributionStatus = {
    #InProgress: {currentNeuron: Nat; totalNeurons: Nat};
    #Completed;
    #Failed: Text;
    #PartiallyCompleted: {successfulNeurons: Nat; failedNeurons: Nat};
  };

  public type NewDistributionRecord = {
    id: Nat;
    startTime: Int;
    endTime: Int;
    distributionTime: Int;
    totalRewardPot: Nat;
    actualDistributed: Nat;
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [NewNeuronReward];
    failedNeurons: [NewFailedNeuron];
    status: NewDistributionStatus;
  };

  // New WithdrawalRecord (same as old)
  public type NewWithdrawalRecord = {
    id: Nat;
    caller: Principal;
    neuronWithdrawals: [(Blob, Nat)];
    totalAmount: Nat;
    amountSent: Nat;
    fee: Nat;
    targetAccount: {owner: Principal; subaccount: ?[Nat8]};
    timestamp: Int;
    transactionId: ?Nat;
  };

  // New LeaderboardEntry
  public type NewLeaderboardEntry = {
    rank: Nat;
    principal: Principal;
    neuronId: Blob;
    performanceScore: Float;
    distributionsCount: Nat;
    lastActivity: Int;
  };

  public type NewLeaderboards = {
    oneWeekUSD: [NewLeaderboardEntry];
    oneWeekICP: [NewLeaderboardEntry];
    oneMonthUSD: [NewLeaderboardEntry];
    oneMonthICP: [NewLeaderboardEntry];
    oneYearUSD: [NewLeaderboardEntry];
    oneYearICP: [NewLeaderboardEntry];
    allTimeUSD: [NewLeaderboardEntry];
    allTimeICP: [NewLeaderboardEntry];
  };

  //==========================================================================
  // STATE TYPES - Old state doesn't have leaderboard fields
  //==========================================================================

  public type OldState = {
    distributionPeriodNS: Nat;
    periodicRewardPot: Nat;
    maxDistributionHistory: Nat;
    distributionEnabled: Bool;
    performanceScorePower: Float;
    votingPowerPower: Float;
    distributionCounter: Nat;
    currentDistributionId: ?Nat;
    lastDistributionTime: Int;
    nextScheduledDistributionTime: ?Int;
    rewardSkipList: [Blob];
    rewardPenalties: Map.Map<Blob, Nat>;
    neuronRewardBalances: Map.Map<Blob, Nat>;
    totalDistributed: Nat;
    totalWithdrawn: Nat;
    totalWithdrawals: Nat;
    withdrawalCounter: Nat;
    distributionHistory: Vector.Vector<OldDistributionRecord>;
    withdrawalHistory: Vector.Vector<OldWithdrawalRecord>;
    // NOTE: leaderboards, leaderboardLastUpdate, leaderboardSize, leaderboardUpdateEnabled
    // did NOT exist in the old version - they are new fields
  };

  public type NewState = {
    distributionPeriodNS: Nat;
    periodicRewardPot: Nat;
    maxDistributionHistory: Nat;
    distributionEnabled: Bool;
    performanceScorePower: Float;
    votingPowerPower: Float;
    distributionCounter: Nat;
    currentDistributionId: ?Nat;
    lastDistributionTime: Int;
    nextScheduledDistributionTime: ?Int;
    rewardSkipList: [Blob];
    rewardPenalties: Map.Map<Blob, Nat>;
    neuronRewardBalances: Map.Map<Blob, Nat>;
    totalDistributed: Nat;
    totalWithdrawn: Nat;
    totalWithdrawals: Nat;
    withdrawalCounter: Nat;
    distributionHistory: Vector.Vector<NewDistributionRecord>;
    withdrawalHistory: Vector.Vector<NewWithdrawalRecord>;
    leaderboards: NewLeaderboards;
    leaderboardLastUpdate: Int;
    leaderboardSize: Nat;
    leaderboardUpdateEnabled: Bool;
  };

  //==========================================================================
  // MIGRATION HELPERS
  //==========================================================================

  // Convert old neuron reward to new neuron reward (only change is adding performanceScoreICP)
  func migrateNeuronReward(old: OldNeuronReward): NewNeuronReward {
    {
      neuronId = old.neuronId;
      performanceScore = old.performanceScore;
      performanceScoreICP = null; // Old data didn't have this, set to null
      votingPower = old.votingPower;
      rewardScore = old.rewardScore;
      rewardAmount = old.rewardAmount;
      checkpoints = old.checkpoints; // CheckpointData is unchanged
    }
  };

  // Convert old distribution record to new distribution record
  func migrateDistributionRecord(old: OldDistributionRecord): NewDistributionRecord {
    {
      id = old.id;
      startTime = old.startTime;
      endTime = old.endTime;
      distributionTime = old.distributionTime;
      totalRewardPot = old.totalRewardPot;
      actualDistributed = old.actualDistributed;
      totalRewardScore = old.totalRewardScore;
      neuronsProcessed = old.neuronsProcessed;
      neuronRewards = Array.map<OldNeuronReward, NewNeuronReward>(old.neuronRewards, migrateNeuronReward);
      failedNeurons = old.failedNeurons;
      status = old.status;
    }
  };

  //==========================================================================
  // MAIN MIGRATION FUNCTION
  //==========================================================================

  public func migrate(oldState: OldState): NewState {
    // Migrate distribution history (only NeuronReward changes)
    let newDistributionHistory = Vector.new<NewDistributionRecord>();
    for (oldRecord in Vector.vals(oldState.distributionHistory)) {
      Vector.add(newDistributionHistory, migrateDistributionRecord(oldRecord));
    };

    // Withdrawal history doesn't need migration (types unchanged)
    // But we need to create a new Vector with the same type
    let newWithdrawalHistory = Vector.new<NewWithdrawalRecord>();
    for (oldRecord in Vector.vals(oldState.withdrawalHistory)) {
      Vector.add(newWithdrawalHistory, oldRecord);
    };

    // Create empty leaderboards (these are new fields)
    let emptyLeaderboards: NewLeaderboards = {
      oneWeekUSD = [];
      oneWeekICP = [];
      oneMonthUSD = [];
      oneMonthICP = [];
      oneYearUSD = [];
      oneYearICP = [];
      allTimeUSD = [];
      allTimeICP = [];
    };

    {
      distributionPeriodNS = oldState.distributionPeriodNS;
      periodicRewardPot = oldState.periodicRewardPot;
      maxDistributionHistory = oldState.maxDistributionHistory;
      distributionEnabled = oldState.distributionEnabled;
      performanceScorePower = oldState.performanceScorePower;
      votingPowerPower = oldState.votingPowerPower;
      distributionCounter = oldState.distributionCounter;
      currentDistributionId = oldState.currentDistributionId;
      lastDistributionTime = oldState.lastDistributionTime;
      nextScheduledDistributionTime = oldState.nextScheduledDistributionTime;
      rewardSkipList = oldState.rewardSkipList;
      rewardPenalties = oldState.rewardPenalties;
      neuronRewardBalances = oldState.neuronRewardBalances;
      totalDistributed = oldState.totalDistributed;
      totalWithdrawn = oldState.totalWithdrawn;
      totalWithdrawals = oldState.totalWithdrawals;
      withdrawalCounter = oldState.withdrawalCounter;
      distributionHistory = newDistributionHistory;
      withdrawalHistory = newWithdrawalHistory;
      // Initialize new leaderboard fields with defaults
      leaderboards = emptyLeaderboards;
      leaderboardLastUpdate = 0;
      leaderboardSize = 100;
      leaderboardUpdateEnabled = true;
    }
  };
}
