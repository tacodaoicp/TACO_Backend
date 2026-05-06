module {

  type Subaccount = Blob;

  type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Subaccount };
  };

  // Already 4-tuple from previous deploy — identity migration (no type change).
  // Original 3→4 migration kept in git history; replaced once the on-chain shape
  // matched the new type, otherwise the compatibility check rejects the upgrade.
  type Transfer = (TransferRecipient, Nat, Text, Text);

  type TransferVector = {
    var data_blocks : [var [var ?Transfer]];
    var i_block : Nat;
    var i_element : Nat;
  };

  public type OldState = {
    transferQueue : TransferVector;
  };

  public type NewState = OldState;

  public func migrate(old : OldState) : NewState { old };
};
