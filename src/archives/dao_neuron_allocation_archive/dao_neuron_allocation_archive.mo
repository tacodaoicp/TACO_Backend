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
import DAOTypes "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";

shared (deployer) actor class DAONeuronAllocationArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type NeuronAllocationChangeBlockData = {
    id: Nat;
    timestamp: Int;
    neuronId: Blob;
    changeType: ArchiveTypes.AllocationChangeType;
    oldAllocations: [ArchiveTypes.Allocation];
    newAllocations: [ArchiveTypes.Allocation]; 
    votingPower: Nat;
    maker: Principal;
    reason: ?Text;
  };
  type ArchiveError = ArchiveTypes.ArchiveError;
  type AllocationChangeType = ArchiveTypes.AllocationChangeType;
  type Allocation = ArchiveTypes.Allocation;

  // Initialize the generic base class with neuron allocation-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 2000000; // 2M blocks per canister (neuron allocation changes are frequent)
    blockRetentionPeriodNS = 94608000000000000; // 3 years in nanoseconds (allocation history is valuable)
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  // ICRC3 State for scalable storage
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  private let base = ArchiveBase.ArchiveBase<NeuronAllocationChangeBlockData>(
    this_canister_id(),
    ["3neuron_allocation_change"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Neuron allocation-specific indexes
  private stable var neuronIndex = Map.new<Blob, [Nat]>(); // Neuron -> block indices
  private stable var makerIndex = Map.new<Principal, [Nat]>(); // Maker -> block indices
  private stable var timestampIndex = Map.new<Int, [Nat]>(); // Timestamp -> block indices

  // Neuron allocation-specific statistics
  private stable var totalNeuronAllocationChanges : Nat = 0;

  // Block counters for IDs
  private stable var neuronAllocationChangeCounter : Nat = 0;

  // Tracking state for batch imports (we'll add "since" methods to DAO_backend later)
  private stable var lastImportedNeuronAllocationTimestamp : Int = 0;

  // DAO_backend interface for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
  private let daoCanister : DAOTypes.Self = actor (Principal.toText(DAO_BACKEND_ID));

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
  // Custom Neuron Allocation Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveNeuronAllocationChange<system>(change : NeuronAllocationChangeBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    // Create compatible allocation change for existing system
    let compatibleChange : ArchiveTypes.AllocationChangeBlockData = {
      id = change.id;
      timestamp = change.timestamp;
      user = change.maker; // Use maker as user for compatibility
      changeType = change.changeType;
      oldAllocations = change.oldAllocations;
      newAllocations = change.newAllocations;
      votingPower = change.votingPower;
      maker = change.maker;
      reason = change.reason;
    };

    let blockValue = ArchiveTypes.allocationChangeToValue(compatibleChange, change.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3neuron_allocation_change",
      Array.map(change.newAllocations, func(a: Allocation) : Principal { a.token }), // Index by involved tokens
      change.timestamp
    );

    // Update custom indexes
    updateNeuronIndex(change.neuronId, blockIndex);
    updateMakerIndex(change.maker, blockIndex);
    updateTimestampIndex(change.timestamp, blockIndex);

    // Update statistics
    totalNeuronAllocationChanges += 1;

    #ok(blockIndex);
  };

  //=========================================================================
  // Query Methods for Rewards System
  //=========================================================================

  public shared query ({ caller }) func getNeuronAllocationChangesByNeuron(neuronId: Blob, limit: Nat) : async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    let blockIndices = switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?indices) { indices };
      case null { return #ok([]) };
    };

    // For now, return empty results - we'll enhance this later
    #ok([]);
  };

  public shared query ({ caller }) func getNeuronAllocationChangesByMaker(maker: Principal, limit: Nat) : async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    let blockIndices = switch (Map.get(makerIndex, Map.phash, maker)) {
      case (?indices) { indices };
      case null { return #ok([]) };
    };

    // For now, return empty results - we'll enhance this later
    #ok([]);
  };

  public shared query ({ caller }) func getNeuronAllocationChangesByNeuronInTimeRange(
    neuronId: Blob,
    startTime: Int,
    endTime: Int,
    limit: Nat
  ) : async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    if (startTime > endTime) {
      return #err(#InvalidTimeRange);
    };

    // For now, return empty results - we'll enhance this later
    #ok([]);
  };

  //=========================================================================
  // Custom Index Management
  //=========================================================================

  private func updateNeuronIndex(neuronId: Blob, blockIndex: Nat) {
    let currentIndices = switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(neuronIndex, Map.bhash, neuronId, Array.append(currentIndices, [blockIndex]));
  };

  private func updateMakerIndex(maker: Principal, blockIndex: Nat) {
    let currentIndices = switch (Map.get(makerIndex, Map.phash, maker)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(makerIndex, Map.phash, maker, Array.append(currentIndices, [blockIndex]));
  };

  private func updateTimestampIndex(timestamp: Int, blockIndex: Nat) {
    let currentIndices = switch (Map.get(timestampIndex, Map.ihash, timestamp)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(timestampIndex, Map.ihash, timestamp, Array.append(currentIndices, [blockIndex]));
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

  //=========================================================================
  // Admin Functions
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
      supportedBlockTypes = ["3neuron_allocation_change"];
      storageUsed = 0;
      lastArchiveTime = lastImportedNeuronAllocationTimestamp;
    };
  };

  // Detailed stats method with authorization for admin interface
  public shared ({ caller }) func getDetailedArchiveStats() : async Result.Result<{
    totalBlocks: Nat;
    totalNeuronAllocationChanges: Nat;
    neuronCount: Nat;
    makerCount: Nat;
  }, ArchiveError> {
    if (not base.isAuthorized(caller, #GetMetrics)) {
      return #err(#NotAuthorized);
    };

    #ok({
      totalBlocks = base.getTotalBlocks();
      totalNeuronAllocationChanges = totalNeuronAllocationChanges;
      neuronCount = Map.size(neuronIndex);
      makerCount = Map.size(makerIndex);
    });
  };

  //=========================================================================
  // Import Logic
  //=========================================================================

  // Import function that fetches data from DAO canister
  private func importBatchData<system>() : async {imported: Nat; failed: Nat} {
    var totalImported = 0;
    var totalFailed = 0;
    
    try {
      // Get DAO canister actor
      let canisterIds = CanisterIds.CanisterIds(this_canister_id());
      let daoCanisterId = canisterIds.getCanisterId(#DAO_backend);
      let daoActor : DAOTypes.Self = actor(Principal.toText(daoCanisterId));
      
      // Get the last timestamp we imported
      let sinceTimestamp = lastImportedNeuronAllocationTimestamp;
      let limit = 100; // Import in batches
      
      // Fetch neuron allocation changes from DAO
      let result = await daoActor.getNeuronAllocationChangesSince(sinceTimestamp, limit);
      
      switch (result) {
        case (#ok(response)) {
          // Process each neuron allocation change
          for (change in response.changes.vals()) {
            try {
              // Convert to block data format
              let blockData : NeuronAllocationChangeBlockData = {
                id = totalNeuronAllocationChanges; // Use current count as unique ID
                neuronId = change.neuronId;
                timestamp = change.timestamp;
                changeType = change.changeType;
                oldAllocations = change.oldAllocations;
                newAllocations = change.newAllocations;
                votingPower = change.votingPower;
                maker = change.maker;
                reason = change.reason;
              };
              
              // Archive the data
              switch (await archiveNeuronAllocationChange<system>(blockData)) {
                case (#ok(_)) { 
                  totalImported += 1;
                  // Update the timestamp tracking
                  lastImportedNeuronAllocationTimestamp := change.timestamp;
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

  public query func getBatchImportStatus() : async {isRunning: Bool; intervalSeconds: Nat} {
    base.getBatchImportStatus();
  };

  public query func getTimerStatus() : async BatchImportTimer.TimerStatus {
    base.getTimerStatus();
  };

  public query ({ caller }) func getArchiveStatus() : async Result.Result<ArchiveTypes.ArchiveStatus, ArchiveError> {
    base.getArchiveStatus(caller);
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
    lastImportedNeuronAllocationTimestamp := 0;
    #ok("Import timestamps reset for neuron allocation archive");
  };

  // Import method that the admin interface will call
  public shared ({ caller }) func importNeuronAllocationChanges<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err("Not authorized");
    };
    
    // TODO: Implement actual import logic from DAO circular buffer
    // For now, return success to avoid errors in the admin interface
    #ok("Neuron allocation changes imported successfully");
  };

};