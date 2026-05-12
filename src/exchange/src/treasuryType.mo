module {
  type Subaccount=Blob;
  type TransferRecipient = {
  #principal : Principal;
  #accountId : { owner : Principal; subaccount : ?Subaccount };
};
  public type Treasury = actor {
    getAcceptedtokens : shared ([Text]) -> async ();
    getTokenInfo : shared query () -> async [(Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text })];
    receiveTransferTasks : shared ([(TransferRecipient, Nat, Text, Text)], Bool) -> async Bool;
    drainTransferQueue : shared () -> async ();
    getPendingTransferCount : shared query () -> async Nat;
    getPendingTransfersByToken : shared query () -> async [(Text, Nat)];
    setTest : shared (Bool) -> async ();
    setOTCCanister : shared (Text) -> async ();
  };

  public type TreasuryFactory = () -> Treasury;
}