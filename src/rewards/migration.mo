import Array "mo:base/Array";

module {
  //==========================================================================
  // SHARED TYPES (unchanged between old and new)
  //==========================================================================

  type Allocation = {
    token: Principal;
    basisPoints: Nat;
  };

  type PriceInfo = {
    icpPrice: Nat;
    usdPrice: Float;
    timestamp: Int;
  };

  type FailedNeuron = {
    neuronId: Blob;
    errorMessage: Text;
  };

  type DistributionStatus = {
    #InProgress: {currentNeuron: Nat; totalNeurons: Nat};
    #Completed;
    #Failed: Text;
    #PartiallyCompleted: {successfulNeurons: Nat; failedNeurons: Nat};
  };

  //==========================================================================
  // OLD TYPES (CheckpointData WITHOUT reason)
  //==========================================================================

  type OldCheckpointData = {
    timestamp: Int;
    allocations: [Allocation];
    tokenValues: [(Principal, Float)];
    totalPortfolioValue: Float;
    pricesUsed: [(Principal, PriceInfo)];
    maker: ?Principal;
  };

  type OldNeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    performanceScoreICP: ?Float;
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Nat;
    checkpoints: [OldCheckpointData];
  };

  type OldDistributionRecord = {
    id: Nat;
    startTime: Int;
    endTime: Int;
    distributionTime: Int;
    totalRewardPot: Nat;
    actualDistributed: Nat;
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [OldNeuronReward];
    failedNeurons: [FailedNeuron];
    status: DistributionStatus;
  };

  //==========================================================================
  // NEW TYPES (CheckpointData WITH reason)
  //==========================================================================

  type NewCheckpointData = {
    timestamp: Int;
    allocations: [Allocation];
    tokenValues: [(Principal, Float)];
    totalPortfolioValue: Float;
    pricesUsed: [(Principal, PriceInfo)];
    maker: ?Principal;
    reason: ?Text;
  };

  type NewNeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    performanceScoreICP: ?Float;
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Nat;
    checkpoints: [NewCheckpointData];
  };

  type NewDistributionRecord = {
    id: Nat;
    startTime: Int;
    endTime: Int;
    distributionTime: Int;
    totalRewardPot: Nat;
    actualDistributed: Nat;
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [NewNeuronReward];
    failedNeurons: [FailedNeuron];
    status: DistributionStatus;
  };

  //==========================================================================
  // VECTOR INTERNAL TYPE
  //==========================================================================

  type OldVector = {
    var data_blocks: [var [var ?OldDistributionRecord]];
    var i_block: Nat;
    var i_element: Nat;
  };

  type NewVector = {
    var data_blocks: [var [var ?NewDistributionRecord]];
    var i_block: Nat;
    var i_element: Nat;
  };

  //==========================================================================
  // STATE TYPES
  //==========================================================================

  public type OldState = {
    distributionHistory: OldVector;
  };

  public type NewState = {
    distributionHistory: NewVector;
  };

  //==========================================================================
  // MIGRATION HELPERS
  //==========================================================================

  func migrateCheckpoint(old: OldCheckpointData): NewCheckpointData {
    {
      timestamp = old.timestamp;
      allocations = old.allocations;
      tokenValues = old.tokenValues;
      totalPortfolioValue = old.totalPortfolioValue;
      pricesUsed = old.pricesUsed;
      maker = old.maker;
      reason = null;
    }
  };

  func migrateNeuronReward(old: OldNeuronReward): NewNeuronReward {
    {
      neuronId = old.neuronId;
      performanceScore = old.performanceScore;
      performanceScoreICP = old.performanceScoreICP;
      votingPower = old.votingPower;
      rewardScore = old.rewardScore;
      rewardAmount = old.rewardAmount;
      checkpoints = Array.map<OldCheckpointData, NewCheckpointData>(old.checkpoints, migrateCheckpoint);
    }
  };

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

  public func migrate(old: OldState): NewState {
    let oldVec = old.distributionHistory;
    let oldBlocks = oldVec.data_blocks;
    let emptyBlock: [var ?NewDistributionRecord] = [var];
    let newBlocks: [var [var ?NewDistributionRecord]] = Array.init(oldBlocks.size(), emptyBlock);

    for (b in oldBlocks.keys()) {
      let oldBlock = oldBlocks[b];
      let newBlock: [var ?NewDistributionRecord] = Array.init(oldBlock.size(), null);
      for (e in oldBlock.keys()) {
        switch (oldBlock[e]) {
          case (?oldRec) { newBlock[e] := ?migrateDistributionRecord(oldRec) };
          case null { newBlock[e] := null };
        };
      };
      newBlocks[b] := newBlock;
    };

    {
      distributionHistory = {
        var data_blocks = newBlocks;
        var i_block = oldVec.i_block;
        var i_element = oldVec.i_element;
      };
    }
  };
}
