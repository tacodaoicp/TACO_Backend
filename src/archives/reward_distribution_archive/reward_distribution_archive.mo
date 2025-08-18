import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Map "mo:map/Map";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Text "mo:base/Text";
import Error "mo:base/Error";
import Debug "mo:base/Debug";

import ICRC3 "mo:icrc3-mo";
import ICRC3Service "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";

shared (deployer) actor class RewardDistributionArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type RewardDistributionBlockData = ArchiveTypes.RewardDistributionBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;

  // ICRC3 State Management for Scalable Storage (500GB)
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  // Initialize the generic base class with reward distribution-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 1000000;
    blockRetentionPeriodNS = 94608000000000000; // 3 years in nanoseconds (reward history is valuable)
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  private let base = ArchiveBase.ArchiveBase<RewardDistributionBlockData>(
    this_canister_id(),
    ["3reward_distribution"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Reward distribution-specific indexes
  private stable var distributionIdIndex = Map.new<Nat, [Nat]>(); // Distribution ID -> block indices
  private stable var timestampIndex = Map.new<Int, [Nat]>(); // Timestamp -> block indices
  private stable var statusIndex = Map.new<Text, [Nat]>(); // Status -> block indices

  // Reward distribution-specific statistics
  private stable var totalRewardDistributions : Nat = 0;

  // Tracking state for batch imports
  private stable var lastImportedDistributionTimestamp : Int = 0;

  // Rewards canister interface for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let REWARDS_ID = canister_ids.getCanisterId(#rewards);

  // Define the rewards canister interface
  type RewardsCanister = actor {
    getDistributionsSince: query (Int, Nat) -> async Result.Result<{
      distributions: [DistributionRecord];
    }, RewardsError>;
  };

  // Types from rewards canister (local definitions to avoid circular dependencies)
  type DistributionRecord = {
    id: Nat;
    startTime: Int;
    endTime: Int;
    distributionTime: Int;
    totalRewardPot: Nat;
    actualDistributed: Nat;
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [NeuronReward];
    failedNeurons: [FailedNeuron];
    status: DistributionStatus;
  };

  type NeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Nat;
    checkpoints: [CheckpointData];
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

  type CheckpointData = {
    timestamp: Int;
    allocations: [Allocation];
    tokenValues: [(Principal, Float)];
    totalPortfolioValue: Float;
    pricesUsed: [(Principal, PriceInfo)];
    maker: ?Principal;
  };

  type Allocation = {
    token: Principal;
    basisPoints: Nat;
  };

  type PriceInfo = {
    icpPrice: Nat;
    usdPrice: Float;
    timestamp: Int;
  };

  type RewardsError = {
    #SystemError: Text;
    #NotAuthorized;
  };

  private let rewardsCanister : RewardsCanister = actor (Principal.toText(REWARDS_ID));

  //=========================================================================
  // ICRC-3 Standard endpoints (delegated to base class)
  //=========================================================================

  public query func icrc3_get_archives(args : ICRC3Service.GetArchivesArgs) : async ICRC3Service.GetArchivesResult {
    base.icrc3_get_archives(args);
  };

  public query func icrc3_get_tip_certificate() : async ?ICRC3Service.DataCertificate {
    base.icrc3_get_tip_certificate();
  };

  public query func icrc3_get_blocks(args : ICRC3Service.GetBlocksArgs) : async ICRC3Service.GetBlocksResult {
    base.icrc3_get_blocks(args);
  };

  public query func icrc3_supported_block_types() : async [ICRC3Service.BlockType] {
    base.icrc3_supported_block_types();
  };

  //=========================================================================
  // Custom Reward Distribution Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveRewardDistribution<system>(distribution : RewardDistributionBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.rewardDistributionToValue(distribution, distribution.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3reward_distribution",
      [], // No token indexing for distributions
      distribution.timestamp
    );

    // Update custom indexes
    updateDistributionIdIndex(distribution.id, blockIndex);
    updateTimestampIndex(distribution.timestamp, blockIndex);
    updateStatusIndex(getStatusString(distribution.status), blockIndex);

    // Update statistics
    totalRewardDistributions += 1;

    #ok(blockIndex);
  };

  //=========================================================================
  // Query Methods
  //=========================================================================

  public shared query ({ caller }) func getRewardDistributionsByDistributionId(
    distributionId: Nat,
    startIndex: ?Nat,
    length: Nat
  ) : async Result.Result<[RewardDistributionBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    if (length > 500) {
      return #err(#InvalidData);
    };

    let blockIndices = switch (Map.get(distributionIdIndex, Map.nhash, distributionId)) {
      case (?indices) { indices };
      case null { return #ok([]) };
    };

    // For now, return empty results - we'll enhance this later if needed
    #ok([]);
  };

  public shared query ({ caller }) func getRewardDistributionsByTimeRange(
    startTime: Int,
    endTime: Int,
    limit: Nat
  ) : async Result.Result<[RewardDistributionBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    if (startTime > endTime) {
      return #err(#InvalidTimeRange);
    };

    // For now, return empty results - we'll enhance this later if needed
    #ok([]);
  };

  //=========================================================================
  // Custom Index Management
  //=========================================================================

  private func updateDistributionIdIndex(distributionId: Nat, blockIndex: Nat) {
    let currentIndices = switch (Map.get(distributionIdIndex, Map.nhash, distributionId)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(distributionIdIndex, Map.nhash, distributionId, Array.append(currentIndices, [blockIndex]));
  };

  private func updateTimestampIndex(timestamp: Int, blockIndex: Nat) {
    let currentIndices = switch (Map.get(timestampIndex, Map.ihash, timestamp)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(timestampIndex, Map.ihash, timestamp, Array.append(currentIndices, [blockIndex]));
  };

  private func updateStatusIndex(status: Text, blockIndex: Nat) {
    let currentIndices = switch (Map.get(statusIndex, Map.thash, status)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(statusIndex, Map.thash, status, Array.append(currentIndices, [blockIndex]));
  };

  private func getStatusString(status: DistributionStatus) : Text {
    switch (status) {
      case (#InProgress(_)) { "InProgress" };
      case (#Completed) { "Completed" };
      case (#Failed(_)) { "Failed" };
      case (#PartiallyCompleted(_)) { "PartiallyCompleted" };
    };
  };

  //=========================================================================
  // Import Logic
  //=========================================================================

  // Import function that fetches data from Rewards canister
  private func importBatchData<system>() : async {imported: Nat; failed: Nat} {
    var totalImported = 0;
    var totalFailed = 0;
    
    try {
      // Get Rewards canister actor
      let canisterIds = CanisterIds.CanisterIds(this_canister_id());
      let rewardsCanisterId = canisterIds.getCanisterId(#rewards);
      let rewardsActor : RewardsCanister = actor(Principal.toText(rewardsCanisterId));
      
      // Get the last timestamp we imported
      let sinceTimestamp = lastImportedDistributionTimestamp;
      let limit = 100; // Import in batches
      
      // Fetch distributions from Rewards canister
      let result = await rewardsActor.getDistributionsSince(sinceTimestamp, limit);
      
      switch (result) {
        case (#ok(response)) {
          // Process each distribution
          for (distribution in response.distributions.vals()) {
            try {
              // Convert to archive block data format
              let blockData : RewardDistributionBlockData = {
                id = distribution.id;
                timestamp = distribution.distributionTime;
                startTime = distribution.startTime;
                endTime = distribution.endTime;
                totalRewardPot = distribution.totalRewardPot;
                actualDistributed = distribution.actualDistributed;
                totalRewardScore = distribution.totalRewardScore;
                neuronsProcessed = distribution.neuronsProcessed;
                neuronRewards = distribution.neuronRewards;
                failedNeurons = distribution.failedNeurons;
                status = distribution.status;
              };
              
              // Archive the data
              switch (await archiveRewardDistribution<system>(blockData)) {
                case (#ok(_)) { 
                  totalImported += 1;
                  // Update the timestamp tracking
                  lastImportedDistributionTimestamp := distribution.distributionTime;
                };
                case (#err(_)) { totalFailed += 1; };
              };
            } catch (e) {
              totalFailed += 1;
            };
          };
        };
        case (#err(_)) { totalFailed += 1; };
      };
    } catch (e) {
      totalFailed += 1;
    };
    
    {imported = totalImported; failed = totalFailed};
  };

  //=========================================================================
  // Standard Archive Management Methods (required by admin interface)
  //=========================================================================

  // Public stats method for frontend (no authorization required)
  public query func getArchiveStats() : async ArchiveTypes.ArchiveStatus {
    let totalBlocks = base.getTotalBlocks();
    let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
    let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };
    
    {
      totalBlocks = totalBlocks;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = ["3reward_distribution"];
      storageUsed = 0;
      lastArchiveTime = lastImportedDistributionTimestamp;
    };
  };

  public query ({ caller }) func getArchiveStatus() : async Result.Result<ArchiveTypes.ArchiveStatus, ArchiveError> {
    base.getArchiveStatus(caller);
  };

  public query func getBatchImportStatus() : async {isRunning: Bool; intervalSeconds: Nat} {
    base.getBatchImportStatus();
  };

  public query func getTimerStatus() : async BatchImportTimer.TimerStatus {
    base.getTimerStatus();
  };

  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    base.getLogs(count, caller);
  };

  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startAdvancedBatchImportSystem<system>(caller, null, null, ?importBatchData);
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    base.stopBatchImportSystem(caller);
  };

  public shared ({ caller }) func stopAllTimers() : async Result.Result<Text, Text> {
    base.stopAllTimers(caller);
  };

  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, Text> {
    await base.runAdvancedManualBatchImport<system>(caller, null, null, ?importBatchData);
  };

  public shared ({ caller }) func setMaxInnerLoopIterations(iterations: Nat) : async Result.Result<Text, Text> {
    base.setMaxInnerLoopIterations(caller, iterations);
  };

  public shared ({ caller }) func resetImportTimestamps() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err("Not authorized");
    };
    // Reset import timestamp to re-import all historical data
    lastImportedDistributionTimestamp := 0;
    #ok("Import timestamps reset for reward distribution archive");
  };

  //=========================================================================
  // System Functions
  //=========================================================================

  system func preupgrade() {
    icrc3State := icrc3StateRef.value;
    base.preupgrade();
  };

  system func postupgrade() {
    icrc3StateRef.value := icrc3State;
    base.postupgrade<system>(func() : async () { /* no-op */ });
  };
}
