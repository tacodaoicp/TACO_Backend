import Array "mo:base/Array";
import Principal "mo:base/Principal";

module {

  //==========================================================================
  // SHARED TYPES (unchanged between old and new)
  //==========================================================================

  type SystemState = {
    #Active;
    #Paused;
    #Emergency;
  };

  type TokenType = {
    #ICP;
    #ICRC12;
    #ICRC3;
  };

  type SystemParameter = {
    #FollowDepth : Nat;
    #MaxFollowers : Nat;
    #MaxPastAllocations : Nat;
    #SnapshotInterval : Nat;
    #MaxTotalUpdates : Nat;
    #MaxAllocationsPerDay : Int;
    #AllocationWindow : Nat;
    #MaxFollowUnfollowActionsPerDay : Nat;
    #MaxFollowed : Nat;
    #LogAdmin : Principal;
  };

  //==========================================================================
  // OLD TYPES (AdminActionType WITHOUT #TokenMaxAllocationUpdate)
  //==========================================================================

  type OldAdminActionType = {
    #TokenAdd : { token : Principal; tokenType : TokenType; viaGovernance : Bool };
    #TokenRemove : { token : Principal };
    #TokenDelete : { token : Principal };
    #TokenPause : { token : Principal };
    #TokenUnpause : { token : Principal };
    #SystemStateChange : { oldState : SystemState; newState : SystemState };
    #ParameterUpdate : { parameter : SystemParameter; oldValue : Text; newValue : Text };
    #AdminPermissionGrant : { targetAdmin : Principal; function : Text; durationDays : Nat };
    #AdminAdd : { newAdmin : Principal };
    #AdminRemove : { removedAdmin : Principal };
    #CanisterStart;
    #CanisterStop;
  };

  type OldAdminActionRecord = {
    id : Nat;
    timestamp : Int;
    admin : Principal;
    actionType : OldAdminActionType;
    reason : Text;
    success : Bool;
    errorMessage : ?Text;
  };

  //==========================================================================
  // NEW TYPES (AdminActionType WITH #TokenMaxAllocationUpdate)
  //==========================================================================

  type NewAdminActionType = {
    #TokenAdd : { token : Principal; tokenType : TokenType; viaGovernance : Bool };
    #TokenRemove : { token : Principal };
    #TokenDelete : { token : Principal };
    #TokenPause : { token : Principal };
    #TokenUnpause : { token : Principal };
    #SystemStateChange : { oldState : SystemState; newState : SystemState };
    #ParameterUpdate : { parameter : SystemParameter; oldValue : Text; newValue : Text };
    #AdminPermissionGrant : { targetAdmin : Principal; function : Text; durationDays : Nat };
    #AdminAdd : { newAdmin : Principal };
    #AdminRemove : { removedAdmin : Principal };
    #CanisterStart;
    #CanisterStop;
    #TokenMaxAllocationUpdate : { token : Principal; oldMaxBP : ?Nat; newMaxBP : ?Nat };
  };

  type NewAdminActionRecord = {
    id : Nat;
    timestamp : Int;
    admin : Principal;
    actionType : NewAdminActionType;
    reason : Text;
    success : Bool;
    errorMessage : ?Text;
  };

  //==========================================================================
  // INTERNAL STRUCTURES
  //==========================================================================

  // Vector<AdminActionRecord> internal representation
  type OldActionVector = {
    var data_blocks : [var [var ?OldAdminActionRecord]];
    var i_block : Nat;
    var i_element : Nat;
  };

  type NewActionVector = {
    var data_blocks : [var [var ?NewAdminActionRecord]];
    var i_block : Nat;
    var i_element : Nat;
  };

  //==========================================================================
  // STATE TYPES
  //==========================================================================

  public type OldState = {
    adminActions : OldActionVector;
  };

  public type NewState = {
    adminActions : NewActionVector;
  };

  //==========================================================================
  // MIGRATION HELPERS
  //==========================================================================

  func migrateActionType(old : OldAdminActionType) : NewAdminActionType {
    switch (old) {
      case (#TokenAdd(d)) { #TokenAdd(d) };
      case (#TokenRemove(d)) { #TokenRemove(d) };
      case (#TokenDelete(d)) { #TokenDelete(d) };
      case (#TokenPause(d)) { #TokenPause(d) };
      case (#TokenUnpause(d)) { #TokenUnpause(d) };
      case (#SystemStateChange(d)) { #SystemStateChange(d) };
      case (#ParameterUpdate(d)) { #ParameterUpdate(d) };
      case (#AdminPermissionGrant(d)) { #AdminPermissionGrant(d) };
      case (#AdminAdd(d)) { #AdminAdd(d) };
      case (#AdminRemove(d)) { #AdminRemove(d) };
      case (#CanisterStart) { #CanisterStart };
      case (#CanisterStop) { #CanisterStop };
    };
  };

  func migrateRecord(old : OldAdminActionRecord) : NewAdminActionRecord {
    {
      id = old.id;
      timestamp = old.timestamp;
      admin = old.admin;
      actionType = migrateActionType(old.actionType);
      reason = old.reason;
      success = old.success;
      errorMessage = old.errorMessage;
    };
  };

  //==========================================================================
  // MAIN MIGRATION FUNCTION
  //==========================================================================

  public func migrate(old : OldState) : NewState {
    let oldBlocks = old.adminActions.data_blocks;
    let newBlocks : [var [var ?NewAdminActionRecord]] = Array.init(oldBlocks.size(), [var] : [var ?NewAdminActionRecord]);

    for (i in oldBlocks.keys()) {
      let oldBlock = oldBlocks[i];
      let newBlock : [var ?NewAdminActionRecord] = Array.init(oldBlock.size(), null);
      for (j in oldBlock.keys()) {
        newBlock[j] := switch (oldBlock[j]) {
          case (?record) { ?migrateRecord(record) };
          case null { null };
        };
      };
      newBlocks[i] := newBlock;
    };

    {
      adminActions = {
        var data_blocks = newBlocks;
        var i_block = old.adminActions.i_block;
        var i_element = old.adminActions.i_element;
      };
    };
  };
};
