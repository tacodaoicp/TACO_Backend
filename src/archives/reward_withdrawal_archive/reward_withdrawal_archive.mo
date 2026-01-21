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
import Blob "mo:base/Blob";

import ICRC3 "mo:icrc3-mo";
import ICRC3Service "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";
import Cycles "mo:base/ExperimentalCycles";

shared (deployer) persistent actor class RewardWithdrawalArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type RewardWithdrawalBlockData = ArchiveTypes.RewardWithdrawalBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;

  // ICRC3 State Management for Scalable Storage (500GB)
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  // Initialize the generic base class with reward withdrawal-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 2000000; // 2M blocks per canister (withdrawals are frequent)
    blockRetentionPeriodNS = 94608000000000000; // 3 years in nanoseconds (withdrawal history is valuable)
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  private let base = ArchiveBase.ArchiveBase<RewardWithdrawalBlockData>(
    this_canister_id(),
    ["3reward_withdrawal"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Reward withdrawal-specific indexes
  private stable var callerIndex = Map.new<Principal, [Nat]>(); // Caller -> block indices
  private stable var neuronIndex = Map.new<Blob, [Nat]>(); // Neuron ID -> block indices
  private stable var timestampIndex = Map.new<Int, [Nat]>(); // Timestamp -> block indices

  // Reward withdrawal-specific statistics
  private stable var totalRewardWithdrawals : Nat = 0;

  // Tracking state for batch imports
  private stable var lastImportedWithdrawalTimestamp : Int = 0;

  // Rewards canister interface for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let REWARDS_ID = canister_ids.getCanisterId(#rewards);

  // Define the rewards canister interface
  type RewardsCanister = actor {
    getWithdrawalsSince: query (Int, Nat) -> async Result.Result<{
      withdrawals: [WithdrawalRecord];
    }, RewardsError>;
  };

  // Types from rewards canister (local definitions to avoid circular dependencies)
  type WithdrawalRecord = {
    id: Nat;
    caller: Principal;
    neuronWithdrawals: [(Blob, Nat)];
    totalAmount: Nat;
    amountSent: Nat;
    fee: Nat;
    targetAccount: ICRCAccount;
    timestamp: Int;
    transactionId: ?Nat;
  };

  type ICRCAccount = {
    owner: Principal;
    subaccount: ?Blob;
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
  // Custom Reward Withdrawal Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveRewardWithdrawal<system>(withdrawal : RewardWithdrawalBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.rewardWithdrawalToValue(withdrawal, withdrawal.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3reward_withdrawal",
      [withdrawal.targetAccountOwner], // Index by target account owner
      withdrawal.timestamp
    );

    // Update custom indexes
    updateCallerIndex(withdrawal.caller, blockIndex);
    updateTimestampIndex(withdrawal.timestamp, blockIndex);
    
    // Update neuron indexes for all neurons involved in the withdrawal
    for ((neuronId, _) in withdrawal.neuronWithdrawals.vals()) {
      updateNeuronIndex(neuronId, blockIndex);
    };

    // Update statistics
    totalRewardWithdrawals += 1;

    #ok(blockIndex);
  };

  //=========================================================================
  // Query Methods
  //=========================================================================

  public shared query ({ caller }) func getRewardWithdrawalsByCaller(
    callerPrincipal: Principal,
    startIndex: ?Nat,
    length: Nat
  ) : async Result.Result<[RewardWithdrawalBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    if (length > 500) {
      return #err(#InvalidData);
    };

    let blockIndices = switch (Map.get(callerIndex, Map.phash, callerPrincipal)) {
      case (?indices) { indices };
      case null { return #ok([]) };
    };

    // For now, return empty results - we'll enhance this later if needed
    #ok([]);
  };

  public shared query ({ caller }) func getRewardWithdrawalsByNeuron(
    neuronId: Blob,
    startIndex: ?Nat,
    length: Nat
  ) : async Result.Result<[RewardWithdrawalBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    if (length > 500) {
      return #err(#InvalidData);
    };

    let blockIndices = switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?indices) { indices };
      case null { return #ok([]) };
    };

    // For now, return empty results - we'll enhance this later if needed
    #ok([]);
  };

  public shared query ({ caller }) func getRewardWithdrawalsByTimeRange(
    startTime: Int,
    endTime: Int,
    limit: Nat
  ) : async Result.Result<[RewardWithdrawalBlockData], ArchiveError> {
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

  private func updateCallerIndex(caller: Principal, blockIndex: Nat) {
    let currentIndices = switch (Map.get(callerIndex, Map.phash, caller)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(callerIndex, Map.phash, caller, Array.append(currentIndices, [blockIndex]));
  };

  private func updateNeuronIndex(neuronId: Blob, blockIndex: Nat) {
    let currentIndices = switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(neuronIndex, Map.bhash, neuronId, Array.append(currentIndices, [blockIndex]));
  };

  private func updateTimestampIndex(timestamp: Int, blockIndex: Nat) {
    let currentIndices = switch (Map.get(timestampIndex, Map.ihash, timestamp)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(timestampIndex, Map.ihash, timestamp, Array.append(currentIndices, [blockIndex]));
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
      let sinceTimestamp = lastImportedWithdrawalTimestamp;
      let limit = 100; // Import in batches
      
      // Fetch withdrawals from Rewards canister
      let result = await rewardsActor.getWithdrawalsSince(sinceTimestamp, limit);
      
      switch (result) {
        case (#ok(response)) {
          // Process each withdrawal
          for (withdrawal in response.withdrawals.vals()) {
            try {
              // Convert to archive block data format
              let blockData : RewardWithdrawalBlockData = {
                id = withdrawal.id;
                timestamp = withdrawal.timestamp;
                caller = withdrawal.caller;
                neuronWithdrawals = withdrawal.neuronWithdrawals;
                totalAmount = withdrawal.totalAmount;
                amountSent = withdrawal.amountSent;
                fee = withdrawal.fee;
                targetAccountOwner = withdrawal.targetAccount.owner;
                targetAccountSubaccount = withdrawal.targetAccount.subaccount;
                transactionId = withdrawal.transactionId;
              };
              
              // Archive the data
              switch (await archiveRewardWithdrawal<system>(blockData)) {
                case (#ok(_)) { 
                  totalImported += 1;
                  // Update the timestamp tracking
                  lastImportedWithdrawalTimestamp := withdrawal.timestamp;
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
      supportedBlockTypes = ["3reward_withdrawal"];
      storageUsed = 0;
      lastArchiveTime = lastImportedWithdrawalTimestamp;
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
    lastImportedWithdrawalTimestamp := 0;
    #ok("Import timestamps reset for reward withdrawal archive");
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

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };
}
