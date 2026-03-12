import Nat8 "mo:base/Nat8";
import Nat64 "mo:base/Nat64";
import Array "mo:base/Array";

module {
  //==========================================================================
  // OLD TYPES (TransferOperationType WITHOUT #ForwardToPortfolio)
  //==========================================================================

  type OldTransferOperationType = {
    #MintReturn;
    #BurnPayout;
    #ExcessReturn;
    #CancelReturn;
    #Recovery;
  };

  type TransferStatus = {
    #Pending;
    #Sent;
    #Confirmed : Nat64;
    #Failed : Text;
  };

  type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Blob };
  };

  type OldVaultTransferTask = {
    id : Nat;
    caller : Principal;
    recipient : TransferRecipient;
    amount : Nat;
    tokenPrincipal : Principal;
    fromSubaccount : Nat8;
    operationType : OldTransferOperationType;
    operationId : Nat;
    status : TransferStatus;
    createdAt : Int;
    updatedAt : Int;
    retryCount : Nat;
    actualAmountSent : ?Nat;
    blockIndex : ?Nat64;
  };

  //==========================================================================
  // NEW TYPES (TransferOperationType WITH #ForwardToPortfolio)
  //==========================================================================

  type NewTransferOperationType = {
    #MintReturn;
    #BurnPayout;
    #ExcessReturn;
    #CancelReturn;
    #Recovery;
    #ForwardToPortfolio;
  };

  type NewVaultTransferTask = {
    id : Nat;
    caller : Principal;
    recipient : TransferRecipient;
    amount : Nat;
    tokenPrincipal : Principal;
    fromSubaccount : Nat8;
    operationType : NewTransferOperationType;
    operationId : Nat;
    status : TransferStatus;
    createdAt : Int;
    updatedAt : Int;
    retryCount : Nat;
    actualAmountSent : ?Nat;
    blockIndex : ?Nat64;
  };

  //==========================================================================
  // INTERNAL STRUCTURES (Vector and Map representations)
  //==========================================================================

  // Vector<VaultTransferTask> internal representation
  type OldVector = {
    var data_blocks : [var [var ?OldVaultTransferTask]];
    var i_block : Nat;
    var i_element : Nat;
  };

  type NewVector = {
    var data_blocks : [var [var ?NewVaultTransferTask]];
    var i_block : Nat;
    var i_element : Nat;
  };

  // Map<Nat, VaultTransferTask> internal representation
  type OldMapBucket = (
    [var ?Nat],              // keys
    [var ?OldVaultTransferTask], // values
    [var Nat],               // indexes
    [var Nat32],             // bounds
  );

  type NewMapBucket = (
    [var ?Nat],              // keys
    [var ?NewVaultTransferTask], // values
    [var Nat],               // indexes
    [var Nat32],             // bounds
  );

  //==========================================================================
  // STATE TYPES
  //==========================================================================

  public type OldState = {
    pendingTransfers : OldVector;
    completedTransfers : [var ?OldMapBucket];
  };

  public type NewState = {
    pendingTransfers : NewVector;
    completedTransfers : [var ?NewMapBucket];
  };

  //==========================================================================
  // MIGRATION HELPERS
  //==========================================================================

  func migrateOpType(old : OldTransferOperationType) : NewTransferOperationType {
    switch (old) {
      case (#MintReturn) { #MintReturn };
      case (#BurnPayout) { #BurnPayout };
      case (#ExcessReturn) { #ExcessReturn };
      case (#CancelReturn) { #CancelReturn };
      case (#Recovery) { #Recovery };
    };
  };

  func migrateTask(old : OldVaultTransferTask) : NewVaultTransferTask {
    {
      id = old.id;
      caller = old.caller;
      recipient = old.recipient;
      amount = old.amount;
      tokenPrincipal = old.tokenPrincipal;
      fromSubaccount = old.fromSubaccount;
      operationType = migrateOpType(old.operationType);
      operationId = old.operationId;
      status = old.status;
      createdAt = old.createdAt;
      updatedAt = old.updatedAt;
      retryCount = old.retryCount;
      actualAmountSent = old.actualAmountSent;
      blockIndex = old.blockIndex;
    };
  };

  //==========================================================================
  // MAIN MIGRATION FUNCTION
  //==========================================================================

  public func migrate(old : OldState) : NewState {
    // Migrate pendingTransfers (Vector internals)
    let oldBlocks = old.pendingTransfers.data_blocks;
    let newBlocks : [var [var ?NewVaultTransferTask]] = Array.init(oldBlocks.size(), [var] : [var ?NewVaultTransferTask]);

    for (i in oldBlocks.keys()) {
      let oldBlock = oldBlocks[i];
      let newBlock : [var ?NewVaultTransferTask] = Array.init(oldBlock.size(), null);
      for (j in oldBlock.keys()) {
        newBlock[j] := switch (oldBlock[j]) {
          case (?task) { ?migrateTask(task) };
          case null { null };
        };
      };
      newBlocks[i] := newBlock;
    };

    // Migrate completedTransfers (Map internals)
    let oldMap = old.completedTransfers;
    let newMap : [var ?NewMapBucket] = Array.init(oldMap.size(), null);

    for (i in oldMap.keys()) {
      switch (oldMap[i]) {
        case (?bucket) {
          let oldKeys = bucket.0;
          let oldValues = bucket.1;
          let indexes = bucket.2;
          let bounds = bucket.3;

          let newValues : [var ?NewVaultTransferTask] = Array.init(oldValues.size(), null);
          for (j in oldValues.keys()) {
            newValues[j] := switch (oldValues[j]) {
              case (?task) { ?migrateTask(task) };
              case null { null };
            };
          };

          newMap[i] := ?(oldKeys, newValues, indexes, bounds);
        };
        case null {
          newMap[i] := null;
        };
      };
    };

    {
      pendingTransfers = {
        var data_blocks = newBlocks;
        var i_block = old.pendingTransfers.i_block;
        var i_element = old.pendingTransfers.i_element;
      };
      completedTransfers = newMap;
    };
  };
};
