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
import Cycles "mo:base/ExperimentalCycles";

shared (deployer) persistent actor class DAOAllocationArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type AllocationChangeBlockData = ArchiveTypes.AllocationChangeBlockData;
  type FollowActionBlockData = ArchiveTypes.FollowActionBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type AllocationChangeType = ArchiveTypes.AllocationChangeType;
  type FollowActionType = ArchiveTypes.FollowActionType;
  type Allocation = ArchiveTypes.Allocation;

  // Initialize the generic base class with allocation-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 2000000; // 2M blocks per canister (allocation changes are frequent)
    blockRetentionPeriodNS = 94608000000000000; // 3 years in nanoseconds (allocation history is valuable)
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  // ICRC3 State for scalable storage
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  private let base = ArchiveBase.ArchiveBase<AllocationChangeBlockData>(
    this_canister_id(),
    ["3allocation_change", "3follow_action"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Allocation-specific indexes
  private stable var userIndex = Map.new<Principal, [Nat]>(); // User -> block indices
  private stable var tokenIndex = Map.new<Principal, [Nat]>(); // Token -> block indices
  private stable var changeTypeIndex = Map.new<Text, [Nat]>(); // Change type -> block indices
  private stable var followIndex = Map.new<Principal, [Nat]>(); // Followed user -> block indices

  // Allocation-specific statistics
  private stable var totalAllocationChanges : Nat = 0;
  private stable var totalFollowActions : Nat = 0;
  private stable var totalFollowCount : Nat = 0;
  private stable var totalUnfollowCount : Nat = 0;

  // Block counters for IDs
  private stable var allocationChangeCounter : Nat = 0;
  private stable var followActionCounter : Nat = 0;

  // Tracking state for batch imports (we'll add "since" methods to DAO_backend later)
  private stable var lastImportedAllocationTimestamp : Int = 0;
  private stable var lastImportedFollowTimestamp : Int = 0;

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
  // Custom Allocation Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveAllocationChange<system>(change : AllocationChangeBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.allocationChangeToValue(change, change.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3allocation_change",
      Array.map(change.newAllocations, func(a: DAOTypes.Allocation) : Principal { a.token }), // Index by involved tokens
      change.timestamp
    );

    // Update custom indexes
    updateUserIndex(change.user, blockIndex);
    updateMakerIndex(change.maker, blockIndex);
    updateChangeTypeIndex(getAllocationChangeTypeString(change.changeType), blockIndex);
    
    // Index by tokens involved
    for (allocation in change.newAllocations.vals()) {
      updateTokenIndex(allocation.token, blockIndex);
    };

    totalAllocationChanges += 1;

    #ok(blockIndex);
  };

  public shared ({ caller }) func archiveFollowAction<system>(action : FollowActionBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.followActionToValue(action, action.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3follow_action",
      [], // No specific tokens for follow actions
      action.timestamp
    );

    // Update custom indexes
    updateUserIndex(action.follower, blockIndex);
    updateFollowIndex(action.followed, blockIndex);
    updateChangeTypeIndex(getFollowActionTypeString(action.action), blockIndex);

    // Update statistics
    totalFollowActions += 1;
    switch (action.action) {
      case (#Follow) { totalFollowCount += 1 };
      case (#Unfollow) { totalUnfollowCount += 1 };
    };

    #ok(blockIndex);
  };

  //=========================================================================
  // Batch Import System (Placeholder - needs DAO_backend "since" methods)
  //=========================================================================

  // Import allocation changes from DAO_backend
  public shared ({ caller }) func importAllocationChanges<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    try {
      // Call DAO_backend to get allocation changes since last import
      let response = await daoCanister.getAllocationChangesSince(lastImportedAllocationTimestamp, 100);
      
      switch (response) {
        case (#ok(data)) {
          var importedCount = 0;
          
          // Convert and store each allocation change
          for (change in data.changes.vals()) {
            let allocationChange: ArchiveTypes.AllocationChangeBlockData = {
              id = allocationChangeCounter;
              timestamp = change.from;
              user = change.user;
              changeType = #UserUpdate({userInitiated = true}); // Simplified for now
              oldAllocations = []; // Not available in DAO PastAllocationRecord - needs migration
              newAllocations = change.allocation;
              votingPower = 0; // Not available in DAO PastAllocationRecord - needs migration
              maker = change.allocationMaker;
              reason = null;
            };
            
            let result = await archiveAllocationChange<system>(allocationChange);
            switch (result) {
              case (#ok(_)) { 
                importedCount += 1;
                allocationChangeCounter += 1;
                lastImportedAllocationTimestamp := change.from;
              };
              case (#err(e)) {
                // Log error but continue processing
              };
            };
          };
          
          Debug.print("DAO Allocation Archive: Imported " # Nat.toText(importedCount) # " allocation changes");
          #ok("Successfully imported " # Nat.toText(importedCount) # " allocation changes");
        };
        case (#err(e)) {
          #err("Failed to fetch allocation changes from DAO_backend: " # debug_show(e));
        };
      };
    } catch (error) {
      #err("Error importing allocation changes: " # Error.message(error));
    };
  };

  // Import follow actions from DAO_backend  
  public shared ({ caller }) func importFollowActions<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    try {
      // Call DAO_backend to get follow actions since last import
      let response = await daoCanister.getFollowActionsSince(lastImportedFollowTimestamp, 100);
      
      switch (response) {
        case (#ok(data)) {
          var importedCount = 0;
          
          // Convert and store each follow action
          for (follow in data.follows.vals()) {
            let followAction: ArchiveTypes.FollowActionBlockData = {
              id = followActionCounter;
              timestamp = follow.since;
              follower = follow.follower;
              followed = follow.followed;
              action = #Follow;
              previousFollowCount = 0; // Would need to track this separately
              newFollowCount = 1; // Would need to track this separately
            };
            
            let result = await archiveFollowAction<system>(followAction);
            switch (result) {
              case (#ok(_)) { 
                importedCount += 1;
                followActionCounter += 1;
                lastImportedFollowTimestamp := follow.since;
              };
              case (#err(e)) {
                // Log error but continue processing
              };
            };
          };
          
          // Handle unfollows separately (though current DAO_backend doesn't track 'until' timestamps)
          // for (unfollow in data.unfollows.vals()) {
          //   // Would implement unfollow handling here
          // };
          
          Debug.print("DAO Allocation Archive: Imported " # Nat.toText(importedCount) # " follow actions");
          #ok("Successfully imported " # Nat.toText(importedCount) # " follow actions");
        };
        case (#err(e)) {
          #err("Failed to fetch follow actions from DAO_backend: " # debug_show(e));
        };
      };
    } catch (error) {
      #err("Error importing follow actions: " # Error.message(error));
    };
  };

  //=========================================================================
  // Query Functions
  //=========================================================================

  public query func getAllocationChangesByUser(user : Principal, limit : Nat) : async Result.Result<[AllocationChangeBlockData], ArchiveError> {
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

  public query func getFollowActionsByUser(user : Principal, limit : Nat) : async Result.Result<[FollowActionBlockData], ArchiveError> {
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

  public query func getAllocationChangesByToken(token : Principal, limit : Nat) : async Result.Result<[AllocationChangeBlockData], ArchiveError> {
    switch (Map.get(tokenIndex, Map.phash, token)) {
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

  // New method for rewards calculation - get allocation changes in time range
  public query ({ caller }) func getAllocationChangesByUserInTimeRange(user : Principal, startTime : Int, endTime : Int) : async Result.Result<[AllocationChangeBlockData], ArchiveError> {
    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };

    // Use base class query functionality with filters
    let filter : ArchiveTypes.BlockFilter = {
      blockTypes = ?[#AllocationChange]; // Only allocation change blocks
      startTime = ?startTime;
      endTime = ?endTime;
      tokens = null;
      traders = ?[user]; // Filter by user (using traders field as user filter)
      minAmount = null;
      maxAmount = null;
    };

    switch (base.queryBlocks(filter, caller)) {
      case (#ok(queryResult)) {
        // Convert the generic blocks to AllocationChangeBlockData
        let allocationChanges = Array.mapFilter<ArchiveTypes.Block, AllocationChangeBlockData>(
          queryResult.blocks,
          func(block) {
            // Extract AllocationChangeBlockData from the block
            switch (block.block) {
              case (#Map(entries)) {
                // Check if this is an allocation change block by looking for operation type
                var isAllocationChange = false;
                for ((key, value) in entries.vals()) {
                  switch (key, value) {
                    case ("operation", #Text("3allocation_change")) {
                      isAllocationChange := true;
                    };
                    case _ {};
                  };
                };
                
                if (isAllocationChange) {
                  convertValueToAllocationChange(#Map(entries));
                } else {
                  null;
                };
              };
              case _ { null };
            };
          }
        );
        #ok(allocationChanges);
      };
      case (#err(error)) { #err(error) };
    };
  };

  // Helper function to convert Value back to AllocationChangeBlockData
  private func convertValueToAllocationChange(value : ArchiveTypes.Value) : ?AllocationChangeBlockData {
    switch (value) {
      case (#Map(fields)) {
        // Extract fields from the Value map
        var id : ?Nat = null;
        var timestamp : ?Int = null;
        var user : ?Principal = null;
        var changeType : ?AllocationChangeType = null;
        var oldAllocations : ?[Allocation] = null;
        var newAllocations : ?[Allocation] = null;
        var votingPower : ?Nat = null;
        var maker : ?Principal = null;
        var reason : ?Text = null;

        for ((key, val) in fields.vals()) {
          switch (key, val) {
            case ("id", #Nat(n)) { id := ?n };
            case ("timestamp", #Int(t)) { timestamp := ?t };
            case ("user", #Blob(b)) { 
              // Principal.fromBlob can trap, so we need to handle it carefully
              if (b.size() == 29) { // Valid principal blob size
                user := ?Principal.fromBlob(b);
              };
            };
            case ("maker", #Blob(b)) { 
              // Principal.fromBlob can trap, so we need to handle it carefully  
              if (b.size() == 29) { // Valid principal blob size
                maker := ?Principal.fromBlob(b);
              };
            };
            case ("votingPower", #Nat(vp)) { votingPower := ?vp };
            case ("reason", #Text(r)) { reason := ?r };
            // TODO: Parse changeType, oldAllocations, newAllocations from their Value representations
            case _ {};
          };
        };

        // Return the parsed data if we have the required fields
        switch (id, timestamp, user, maker, votingPower) {
          case (?i, ?t, ?u, ?m, ?vp) {
            ?{
              id = i;
              timestamp = t;
              user = u;
              changeType = switch (changeType) { case (?ct) ct; case null #SystemRebalance };
              oldAllocations = switch (oldAllocations) { case (?oa) oa; case null [] };
              newAllocations = switch (newAllocations) { case (?na) na; case null [] };
              votingPower = vp;
              maker = m;
              reason = reason;
            };
          };
          case _ { null };
        };
      };
      case _ { null };
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
      supportedBlockTypes = ["3allocation_change", "3follow_action"];
      storageUsed = 0;
      lastArchiveTime = Int.max(lastImportedAllocationTimestamp, lastImportedFollowTimestamp);
    };
  };

  //=========================================================================
  // Timer Management - Using BatchImportTimer system instead of system timer
  //=========================================================================

  //=========================================================================
  // Lifecycle Management
  //=========================================================================

  // Batch import function for both allocation changes and follow actions
  private func importBatchData<system>() : async {imported: Nat; failed: Nat} {
    var totalImported = 0;
    var totalFailed = 0;
    
    // Import allocation changes
    switch (await importAllocationChanges<system>()) {
      case (#ok(message)) {
        if (Text.contains(message, #text "imported 0")) {
          // No items imported
        } else {
          totalImported += 1;
        };
      };
      case (#err(_)) { totalFailed += 1; };
    };
    
    // Import follow actions
    switch (await importFollowActions<system>()) {
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
    lastImportedAllocationTimestamp := 0;
    lastImportedFollowTimestamp := 0;
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

  private func updateMakerIndex(maker : Principal, blockIndex : Nat) {
    let existing = switch (Map.get(userIndex, Map.phash, maker)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(userIndex, Map.phash, maker, Array.append(existing, [blockIndex]));
  };

  private func updateTokenIndex(token : Principal, blockIndex : Nat) {
    let existing = switch (Map.get(tokenIndex, Map.phash, token)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(tokenIndex, Map.phash, token, Array.append(existing, [blockIndex]));
  };

  private func updateChangeTypeIndex(changeType : Text, blockIndex : Nat) {
    let existing = switch (Map.get(changeTypeIndex, Map.thash, changeType)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(changeTypeIndex, Map.thash, changeType, Array.append(existing, [blockIndex]));
  };

  private func updateFollowIndex(followed : Principal, blockIndex : Nat) {
    let existing = switch (Map.get(followIndex, Map.phash, followed)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(followIndex, Map.phash, followed, Array.append(existing, [blockIndex]));
  };

  private func getAllocationChangeTypeString(changeType : AllocationChangeType) : Text {
    switch (changeType) {
      case (#UserUpdate(_)) { "UserUpdate" };
      case (#FollowAction(_)) { "FollowAction" };
      case (#SystemRebalance) { "SystemRebalance" };
      case (#VotingPowerChange) { "VotingPowerChange" };
    };
  };

  private func getFollowActionTypeString(actionType : FollowActionType) : Text {
    switch (actionType) {
      case (#Follow) { "Follow" };
      case (#Unfollow) { "Unfollow" };
    };
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };
};
