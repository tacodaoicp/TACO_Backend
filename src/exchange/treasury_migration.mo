import Array "mo:base/Array";

module {

  type Subaccount = Blob;

  type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Subaccount };
  };

  type OldTransfer = (TransferRecipient, Nat, Text);
  type NewTransfer = (TransferRecipient, Nat, Text, Text);

  type OldTransferVector = {
    var data_blocks : [var [var ?OldTransfer]];
    var i_block : Nat;
    var i_element : Nat;
  };

  type NewTransferVector = {
    var data_blocks : [var [var ?NewTransfer]];
    var i_block : Nat;
    var i_element : Nat;
  };

  public type OldState = {
    transferQueue : OldTransferVector;
  };

  public type NewState = {
    transferQueue : NewTransferVector;
  };

  func migrateBlock(block : [var ?OldTransfer]) : [var ?NewTransfer] {
    Array.tabulateVar<?NewTransfer>(
      block.size(),
      func(i : Nat) : ?NewTransfer {
        switch (block[i]) {
          case null null;
          case (?old) { ?(old.0, old.1, old.2, "") };
        };
      },
    );
  };

  public func migrate(old : OldState) : NewState {
    let oldBlocks = old.transferQueue.data_blocks;
    let newBlocks = Array.tabulateVar<[var ?NewTransfer]>(
      oldBlocks.size(),
      func(i : Nat) : [var ?NewTransfer] {
        migrateBlock(oldBlocks[i]);
      },
    );

    {
      transferQueue = {
        var data_blocks = newBlocks;
        var i_block = old.transferQueue.i_block;
        var i_element = old.transferQueue.i_element;
      };
    };
  };
};
