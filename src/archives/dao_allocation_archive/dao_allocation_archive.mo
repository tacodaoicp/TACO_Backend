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

shared (deployer) actor class DAOAllocationArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type AllocationChangeBlockData = ArchiveTypes.AllocationChangeBlockData;
  type FollowActionBlockData = ArchiveTypes.FollowActionBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type AllocationChangeType = ArchiveTypes.AllocationChangeType;
  type FollowActionType = ArchiveTypes.FollowActionType;

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

  // Tracking state for batch imports (we'll add "since" methods to DAO_backend later)
  private stable var lastImportedAllocationTimestamp : Int = 0;
  private stable var lastImportedFollowActionId : Nat = 0;

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

  // Future: Import allocation changes from DAO_backend
  public shared ({ caller }) func importAllocationChanges<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    // TODO: Implement once DAO_backend has getAllocationChangesSince method
    // For now, return placeholder
    #ok("Allocation changes import not yet implemented - needs DAO_backend integration");
  };

  // Future: Import follow actions from DAO_backend  
  public shared ({ caller }) func importFollowActions<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    // TODO: Implement once DAO_backend has getFollowActionsSince method
    // For now, return placeholder
    #ok("Follow actions import not yet implemented - needs DAO_backend integration");
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

  public query func getArchiveStats() : async {
    totalBlocks: Nat;
    totalAllocationChanges: Nat;
    totalFollowActions: Nat;
    totalFollowCount: Nat;
    totalUnfollowCount: Nat;
    lastImportedAllocationTimestamp: Int;
    lastImportedFollowActionId: Nat;
  } {
    {
      totalBlocks = base.getTotalBlocks();
      totalAllocationChanges = totalAllocationChanges;
      totalFollowActions = totalFollowActions;
      totalFollowCount = totalFollowCount;
      totalUnfollowCount = totalUnfollowCount;
      lastImportedAllocationTimestamp = lastImportedAllocationTimestamp;
      lastImportedFollowActionId = lastImportedFollowActionId;
    };
  };

  //=========================================================================
  // Timer Management (Future implementation)
  //=========================================================================

  system func timer(setGlobalTimer : Nat64 -> ()) : async () {
    // Future: Import from DAO_backend every 10 minutes (allocation changes are less frequent)
    // ignore await importAllocationChanges<system>();
    // ignore await importFollowActions<system>();
    setGlobalTimer(1_000_000_000 * 600); // 10 minutes
  };

  //=========================================================================
  // Lifecycle Management
  //=========================================================================

  system func preupgrade() {
    base.preupgrade();
  };

  system func postupgrade() {
    icrc3State := null;
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
};
