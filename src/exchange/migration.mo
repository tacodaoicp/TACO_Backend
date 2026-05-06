import Array "mo:base/Array";

module {

  type Subaccount = Blob;

  type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Subaccount };
  };

  // Already 4-tuple from previous deploy — identity migration (no type change)
  type Transfer = (TransferRecipient, Nat, Text, Text);

  type TransferVector = {
    var data_blocks : [var [var ?Transfer]];
    var i_block : Nat;
    var i_element : Nat;
  };

  public type OldState = {
    tempTransferQueue : TransferVector;
  };

  public type NewState = OldState;

  public func migrate(old : OldState) : NewState { old };
};
