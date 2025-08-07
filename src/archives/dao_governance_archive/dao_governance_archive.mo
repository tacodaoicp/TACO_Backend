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
import DAOTypes "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";

shared (deployer) actor class DAOGovernanceArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type VotingPowerBlockData = ArchiveTypes.VotingPowerBlockData;
  type NeuronUpdateBlockData = ArchiveTypes.NeuronUpdateBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type VotingPowerChangeType = ArchiveTypes.VotingPowerChangeType;
  type NeuronUpdateType = ArchiveTypes.NeuronUpdateType;

  // Initialize the generic base class with governance-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 1500000; // 1.5M blocks per canister (governance events are important but less frequent)
    blockRetentionPeriodNS = 126144000000000000; // 4 years in nanoseconds (governance history is critical)
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  // ICRC3 State for scalable storage
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  private let base = ArchiveBase.ArchiveBase<VotingPowerBlockData>(
    this_canister_id(),
    ["3voting_power", "3neuron_update"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Governance-specific indexes
  private stable var userIndex = Map.new<Principal, [Nat]>(); // User -> block indices
  private stable var neuronIndex = Map.new<Text, [Nat]>(); // Neuron ID (as Text) -> block indices
  private stable var changeTypeIndex = Map.new<Text, [Nat]>(); // Change type -> block indices
  private stable var updateTypeIndex = Map.new<Text, [Nat]>(); // Update type -> block indices

  // Governance-specific statistics
  private stable var totalVotingPowerChanges : Nat = 0;
  private stable var totalNeuronUpdates : Nat = 0;
  private stable var totalVotingPowerGained : Nat = 0;
  private stable var totalVotingPowerLost : Nat = 0;
  private stable var activeNeuronCount : Nat = 0;

  // Block counters for IDs
  private stable var votingPowerCounter : Nat = 0;
  private stable var neuronUpdateCounter : Nat = 0;

  // Tracking state for batch imports (we'll add "since" methods to DAO_backend later)
  private stable var lastImportedVotingPowerTimestamp : Int = 0;
  private stable var lastImportedNeuronTimestamp : Int = 0;

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
  // Custom Governance Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveVotingPowerChange<system>(change : VotingPowerBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.votingPowerToValue(change, change.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3voting_power",
      [], // No specific tokens for voting power changes
      change.timestamp
    );

    // Update custom indexes
    updateUserIndex(change.user, blockIndex);
    updateChangeTypeIndex(getVotingPowerChangeTypeString(change.changeType), blockIndex);
    
    // Index by neurons involved
    for (neuron in change.neurons.vals()) {
      updateNeuronIndex(neuronIdToText(neuron.neuronId), blockIndex);
    };

    // Update statistics
    totalVotingPowerChanges += 1;
    if (change.newVotingPower > change.oldVotingPower) {
      totalVotingPowerGained += (change.newVotingPower - change.oldVotingPower);
    } else {
      totalVotingPowerLost += (change.oldVotingPower - change.newVotingPower);
    };

    #ok(blockIndex);
  };

  public shared ({ caller }) func archiveNeuronUpdate<system>(update : NeuronUpdateBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.neuronUpdateToValue(update, update.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3neuron_update",
      [], // No specific tokens for neuron updates
      update.timestamp
    );

    // Update custom indexes
    updateNeuronIndex(neuronIdToText(update.neuronId), blockIndex);
    updateUpdateTypeIndex(getNeuronUpdateTypeString(update.updateType), blockIndex);
    
    // Index by affected users
    for (user in update.affectedUsers.vals()) {
      updateUserIndex(user, blockIndex);
    };

    // Update statistics
    totalNeuronUpdates += 1;
    switch (update.updateType) {
      case (#Added) { activeNeuronCount += 1 };
      case (#Removed) { if (activeNeuronCount > 0) activeNeuronCount -= 1 };
      case (_) { /* No change to neuron count */ };
    };

    #ok(blockIndex);
  };

  //=========================================================================
  // Batch Import System (Placeholder - needs DAO_backend "since" methods)
  //=========================================================================

  // Import voting power changes from DAO_backend
  public shared ({ caller }) func importVotingPowerChanges<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    try {
      // Call DAO_backend to get voting power changes since last import
      let response = await daoCanister.getVotingPowerChangesSince(lastImportedVotingPowerTimestamp, 100);
      
      switch (response) {
        case (#ok(data)) {
          var importedCount = 0;
          
          // Convert and store each voting power change
          for (userRecord in data.users.vals()) {
            // Only include users with recent voting power updates
            if (userRecord.lastVotingPowerUpdate >= lastImportedVotingPowerTimestamp) {
              let votingPowerChange: ArchiveTypes.VotingPowerBlockData = {
                id = votingPowerCounter;
                timestamp = userRecord.lastVotingPowerUpdate;
                user = userRecord.user;
                changeType = #NeuronSnapshot; // Default type for now
                oldVotingPower = 0; // Would need previous voting power
                newVotingPower = userRecord.votingPower;
                neurons = userRecord.neurons;
              };
              
              let result = await archiveVotingPowerChange<system>(votingPowerChange);
              switch (result) {
                case (#ok(_)) { 
                  importedCount += 1;
                  votingPowerCounter += 1;
                  lastImportedVotingPowerTimestamp := userRecord.lastVotingPowerUpdate;
                };
                case (#err(e)) {
                  // Log error but continue processing
                };
              };
            };
          };
          
          Debug.print("DAO Governance Archive: Imported " # Nat.toText(importedCount) # " voting power changes");
          if (importedCount > 0) {
            lastImportedVotingPowerTimestamp := Time.now();
          };
          #ok("Successfully imported " # Nat.toText(importedCount) # " voting power changes");
        };
        case (#err(e)) {
          #err("Failed to fetch voting power changes from DAO_backend: " # debug_show(e));
        };
      };
    } catch (error) {
      #err("Error importing voting power changes: " # Error.message(error));
    };
  };

  // Import neuron updates from DAO_backend  
  public shared ({ caller }) func importNeuronUpdates<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    try {
      // Call DAO_backend to get neuron updates since last import
      let response = await daoCanister.getNeuronUpdatesSince(lastImportedNeuronTimestamp, 100);
      
      switch (response) {
        case (#ok(data)) {
          var importedCount = 0;
          
          // Convert and store each neuron update
          for (neuronRecord in data.neurons.vals()) {
            let neuronUpdate: ArchiveTypes.NeuronUpdateBlockData = {
              id = neuronUpdateCounter;
              timestamp = Time.now(); // Current timestamp since we don't have neuron update timestamps
              updateType = #StateChanged; // Default type for now
              neuronId = neuronRecord.neuronId;
              oldVotingPower = null; // Would need previous voting power
              newVotingPower = ?neuronRecord.votingPower;
              affectedUsers = neuronRecord.users;
            };
            
            let result = await archiveNeuronUpdate<system>(neuronUpdate);
            switch (result) {
              case (#ok(_)) { 
                importedCount += 1;
                neuronUpdateCounter += 1;
                lastImportedNeuronTimestamp := Time.now();
              };
              case (#err(e)) {
                // Log error but continue processing
              };
            };
          };
          
          Debug.print("DAO Governance Archive: Imported " # Nat.toText(importedCount) # " neuron updates");
          if (importedCount > 0) {
            lastImportedNeuronTimestamp := Time.now();
          };
          #ok("Successfully imported " # Nat.toText(importedCount) # " neuron updates");
        };
        case (#err(e)) {
          #err("Failed to fetch neuron updates from DAO_backend: " # debug_show(e));
        };
      };
    } catch (error) {
      #err("Error importing neuron updates: " # Error.message(error));
    };
  };

  //=========================================================================
  // Query Functions
  //=========================================================================

  public query func getVotingPowerChangesByUser(user : Principal, limit : Nat) : async Result.Result<[VotingPowerBlockData], ArchiveError> {
    switch (Map.get(userIndex, Map.phash, user)) {
      case (?blockIndices) {
        let limitedIndices = if (Array.size(blockIndices) > limit) {
          Array.tabulate<Nat>(limit, func(i) = blockIndices[i]);
        } else {
          blockIndices;
        };
        
        // For now, return empty array (proper ICRC3 block querying to be implemented)
        #ok([]);
      };
      case null { #ok([]) };
    };
  };

  public query func getNeuronUpdatesByNeuron(neuronId : Blob, limit : Nat) : async Result.Result<[NeuronUpdateBlockData], ArchiveError> {
    let neuronIdText = neuronIdToText(neuronId);
    switch (Map.get(neuronIndex, Map.thash, neuronIdText)) {
      case (?blockIndices) {
        let limitedIndices = if (Array.size(blockIndices) > limit) {
          Array.tabulate<Nat>(limit, func(i) = blockIndices[i]);
        } else {
          blockIndices;
        };
        
        // For now, return empty array (proper ICRC3 block querying to be implemented)
        #ok([]);
      };
      case null { #ok([]) };
    };
  };

  public query func getGovernanceMetrics() : async {
    totalVotingPowerInSystem: Nat;
    totalActiveUsers: Nat;
    activeNeuronCount: Nat;
    averageVotingPowerPerUser: Nat;
  } {
    // These would need to be calculated from current state
    // For now, return placeholder values
    {
      totalVotingPowerInSystem = totalVotingPowerGained;
      totalActiveUsers = Map.size(userIndex);
      activeNeuronCount = activeNeuronCount;
      averageVotingPowerPerUser = if (Map.size(userIndex) > 0) { totalVotingPowerGained / Map.size(userIndex) } else { 0 };
    };
  };

  // Helper function to parse ICRC3 block and extract voting power data
  private func parseBlockForVotingPower(block : ICRC3Service.Block, targetUser : Principal, maxTimestamp : Int) : (?Nat, ?Int) {
    // The ICRC3 Block has structure {block : Value; id : Nat}
    // The block.block field contains our Value data
    switch (block.block) {
      case (#Map(entries)) {
        var blockUser : ?Principal = null;
        var newVotingPower : ?Nat = null;
        var blockTimestamp : ?Int = null;
        var isVotingPowerBlock : Bool = false;
        
        // Extract the relevant fields from the map
        for ((key, value) in entries.vals()) {
          switch (key, value) {
            case ("user", #Blob(userBlob)) {
              // Principal.fromBlob can trap, so we need to handle it carefully
              // For now, we'll assume the blob is valid since it comes from our own archive
              blockUser := ?Principal.fromBlob(userBlob);
            };
            case ("newVotingPower", #Nat(vp)) {
              newVotingPower := ?vp;
              isVotingPowerBlock := true; // This indicates it's a voting power block
            };
            case ("timestamp", #Int(ts)) {
              blockTimestamp := ?ts;
            };
            case _ { /* Ignore other fields */ };
          };
        };
        
        // Check if this block is for the target user and is a voting power block
        if (isVotingPowerBlock) {
          switch (blockUser, newVotingPower, blockTimestamp) {
            case (?user, ?vp, ?ts) {
              if (Principal.equal(user, targetUser)) {
                (?vp, ?ts);
              } else {
                (null, null);
              };
            };
            case _ { (null, null) };
          };
        } else {
          (null, null);
        };
      };
      case _ { (null, null) };
    };
  };

  // Query method to find user's voting power at a specific timestamp
  // Returns the most recent voting power change before the given timestamp
  public query func getUserVotingPowerAtTime(user : Principal, timestamp : Int) : async Result.Result<Nat, ArchiveError> {
    switch (Map.get(userIndex, Map.phash, user)) {
      case (?blockIndices) {
        var mostRecentVotingPower : Nat = 0;
        var mostRecentTimestamp : Int = -1;
        
        // Iterate through the user's block indices
        for (blockIndex in blockIndices.vals()) {
          // Get the specific block using ICRC3
          let getBlocksArgs = [{
            start = blockIndex;
            length = 1;
          }];
          
          let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);
          
          // Process the single block if available
          if (Array.size(icrc3Result.blocks) > 0) {
            let block = icrc3Result.blocks[0];
            
            // Parse the block to check if it's a voting power change for our user
            switch (parseBlockForVotingPower(block, user, timestamp)) {
              case (?votingPower, ?blockTimestamp) {
                // Only consider blocks before or at the target timestamp
                if (blockTimestamp <= timestamp and blockTimestamp > mostRecentTimestamp) {
                  mostRecentVotingPower := votingPower;
                  mostRecentTimestamp := blockTimestamp;
                };
              };
              case _ { /* Not a relevant voting power block */ };
            };
          };
        };
        
        #ok(mostRecentVotingPower);
      };
      case null { 
        // No blocks found for this user
        #ok(0);
      };
    };
  };

  public query func getArchiveStats() : async ArchiveTypes.ArchiveStatus {
    let totalBlocks = base.getTotalBlocks();
    let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
    let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };
    
    {
      totalBlocks = totalBlocks;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = ["3voting_power", "3neuron_update"];
      storageUsed = 0;
      lastArchiveTime = Int.max(lastImportedVotingPowerTimestamp, lastImportedNeuronTimestamp);
    };
  };

  //=========================================================================
  // Timer Management - Using BatchImportTimer system instead of system timer
  //=========================================================================

  //=========================================================================
  // Lifecycle Management
  //=========================================================================

  // Batch import function for both voting power changes and neuron updates
  private func importBatchData<system>() : async {imported: Nat; failed: Nat} {
    var totalImported = 0;
    var totalFailed = 0;
    
    // Import voting power changes
    switch (await importVotingPowerChanges<system>()) {
      case (#ok(message)) {
        // Extract count from message "Successfully imported X voting power changes"
        if (Text.contains(message, #text "imported 0")) {
          // No items imported
        } else {
          totalImported += 1; // For now, count operations, not individual items
        };
      };
      case (#err(_)) { totalFailed += 1; };
    };
    
    // Import neuron updates  
    switch (await importNeuronUpdates<system>()) {
      case (#ok(message)) {
        if (Text.contains(message, #text "imported 0")) {
          // No items imported
        } else {
          totalImported += 1;
        };
      };
      case (#err(_)) { totalFailed += 1; };
    };
    
    {imported = totalImported; failed = totalFailed};
  };

  // Frontend compatibility methods - matching treasury archive signatures
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
    // Reset all import timestamps to re-import from beginning
    lastImportedVotingPowerTimestamp := 0;
    lastImportedNeuronTimestamp := 0;
    #ok("Import timestamps reset successfully");
  };

  system func preupgrade() {
    // Save ICRC3 state before upgrade
    icrc3State := icrc3StateRef.value;
    base.preupgrade();
  };

  system func postupgrade() {
    // Restore ICRC3 state after upgrade
    icrc3StateRef.value := icrc3State;
    base.postupgrade<system>(func() : async () { /* no-op */ });
  };

  //=========================================================================
  // Private Helper Functions
  //=========================================================================

  private func updateUserIndex(user : Principal, blockIndex : Nat) {
    let existing = switch (Map.get(userIndex, Map.phash, user)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(userIndex, Map.phash, user, Array.append(existing, [blockIndex]));
  };

  private func updateNeuronIndex(neuronIdText : Text, blockIndex : Nat) {
    let existing = switch (Map.get(neuronIndex, Map.thash, neuronIdText)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(neuronIndex, Map.thash, neuronIdText, Array.append(existing, [blockIndex]));
  };

  private func updateChangeTypeIndex(changeType : Text, blockIndex : Nat) {
    let existing = switch (Map.get(changeTypeIndex, Map.thash, changeType)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(changeTypeIndex, Map.thash, changeType, Array.append(existing, [blockIndex]));
  };

  private func updateUpdateTypeIndex(updateType : Text, blockIndex : Nat) {
    let existing = switch (Map.get(updateTypeIndex, Map.thash, updateType)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(updateTypeIndex, Map.thash, updateType, Array.append(existing, [blockIndex]));
  };

  private func getVotingPowerChangeTypeString(changeType : VotingPowerChangeType) : Text {
    switch (changeType) {
      case (#NeuronSnapshot) { "NeuronSnapshot" };
      case (#ManualRefresh) { "ManualRefresh" };
      case (#SystemUpdate) { "SystemUpdate" };
    };
  };

  private func getNeuronUpdateTypeString(updateType : NeuronUpdateType) : Text {
    switch (updateType) {
      case (#Added) { "Added" };
      case (#Removed) { "Removed" };
      case (#VotingPowerChanged) { "VotingPowerChanged" };
      case (#StateChanged) { "StateChanged" };
    };
  };

  private func neuronIdToText(neuronId : Blob) : Text {
    // Convert Blob to Text for indexing (simple debug representation)
    debug_show(neuronId);
  };
};
