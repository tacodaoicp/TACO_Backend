import Deque "mo:base/Deque";
import List "mo:base/List";
import Time "mo:base/Time";
import Result "mo:base/Result";

module {
  public type Account = { owner : Principal; subaccount : ?[Nat8] };
  public type ArchivedTransactionRange = {
    callback : shared query GetTransactionsRequest -> async {
      transactions : [Transaction];
    };
    start : Nat;
    length : Nat;
  };
  public type Burn = {
    from : Account;
    memo : ?[Nat8];
    created_at_time : ?Nat64;
    amount : Nat;
  };
  public type GetTransactionsRequest = { start : Nat; length : Nat };
  public type GetTransactionsResponse = {
    first_index : Nat;
    log_length : Nat;
    transactions : [Transaction];
    archived_transactions : [ArchivedTransactionRange];
  };
  public type HttpRequest = {
    url : Text;
    method : Text;
    body : [Nat8];
    headers : [(Text, Text)];
  };
  public type HttpResponse = {
    body : [Nat8];
    headers : [(Text, Text)];
    status_code : Nat16;
  };
  public type Mint = {
    to : Account;
    memo : ?[Nat8];
    created_at_time : ?Nat64;
    amount : Nat;
  };
  public type Result = { #Ok : Nat; #Err : TransferError };
  public type StandardRecord = { url : Text; name : Text };
  public type Transaction = {
    burn : ?Burn;
    kind : Text;
    mint : ?Mint;
    timestamp : Nat64;
    index : ?Nat;
    transfer : ?Transfer;
  };
  public type Transfer = {
    to : Account;
    fee : ?Nat;
    from : Account;
    memo : ?[Nat8];
    created_at_time : ?Nat64;
    amount : Nat;
  };
  public type TransferArg = {
    to : Account;
    fee : ?Nat;
    memo : ?[Nat8];
    from_subaccount : ?[Nat8];
    created_at_time : ?Nat64;
    amount : Nat;
  };
  public type TransferError = {
    #GenericError : { message : Text; error_code : Nat };
    #TemporarilyUnavailable;
    #BadBurn : { min_burn_amount : Nat };
    #Duplicate : { duplicate_of : Nat };
    #BadFee : { expected_fee : Nat };
    #CreatedInFuture : { ledger_time : Nat64 };
    #TooOld;
    #InsufficientFunds : { balance : Nat };
  };
  public type Value = { #Int : Int; #Nat : Nat; #Blob : [Nat8]; #Text : Text };

  // ── ICRC-2 (approve/allowance/transfer_from) ─────────────────────────────
  // mo:icrc1@0.0.2's FullInterface has NO icrc2 methods, so the V2 test actors
  // need this hand-rolled typed interface. Wire-compatible with both the ICP
  // ledger (ledger-suite-icp-2025-08-29) and ic-icrc1-ledger (5849c6d):
  // [Nat8] encodes as vec nat8 == blob on the candid wire.
  public type ApproveArgs = {
    from_subaccount : ?[Nat8];
    spender : Account;
    amount : Nat;
    expected_allowance : ?Nat;
    expires_at : ?Nat64;
    fee : ?Nat;
    memo : ?[Nat8];
    created_at_time : ?Nat64;
  };
  // NOTE: #Expired is an ApproveError ONLY. It is NOT a TransferFromError —
  // a transfer_from against an expired approval fails #InsufficientAllowance.
  public type ApproveError = {
    #BadFee : { expected_fee : Nat };
    #InsufficientFunds : { balance : Nat };
    #AllowanceChanged : { current_allowance : Nat };
    #Expired : { ledger_time : Nat64 };
    #TooOld;
    #CreatedInFuture : { ledger_time : Nat64 };
    #Duplicate : { duplicate_of : Nat };
    #TemporarilyUnavailable;
    #GenericError : { error_code : Nat; message : Text };
  };
  public type AllowanceArgs = { account : Account; spender : Account };
  public type Allowance = { allowance : Nat; expires_at : ?Nat64 };
  public type TransferFromArgs = {
    spender_subaccount : ?[Nat8];
    from : Account;
    to : Account;
    amount : Nat;
    fee : ?Nat;
    memo : ?[Nat8];
    created_at_time : ?Nat64;
  };
  public type TransferFromError = {
    #BadFee : { expected_fee : Nat };
    #BadBurn : { min_burn_amount : Nat };
    #InsufficientFunds : { balance : Nat };
    #InsufficientAllowance : { allowance : Nat };
    #TooOld;
    #CreatedInFuture : { ledger_time : Nat64 };
    #Duplicate : { duplicate_of : Nat };
    #TemporarilyUnavailable;
    #GenericError : { error_code : Nat; message : Text };
  };
  // Typed actor ref for ICRC-2 capable ledgers (approve + allowance +
  // transfer_from + balance). Works against all five local test ledgers.
  public type Self2Full = actor {
    icrc1_balance_of : shared query Account -> async Nat;
    icrc1_fee : shared query () -> async Nat;
    icrc2_approve : shared ApproveArgs -> async { #Ok : Nat; #Err : ApproveError };
    icrc2_allowance : shared query AllowanceArgs -> async Allowance;
    icrc2_transfer_from : shared TransferFromArgs -> async { #Ok : Nat; #Err : TransferFromError };
  };

  public type Self = actor {
    get_transactions : shared query GetTransactionsRequest -> async GetTransactionsResponse;
    http_request : shared query HttpRequest -> async HttpResponse;
    icrc1_balance_of : shared query Account -> async Nat;
    icrc1_decimals : shared query () -> async Nat8;
    icrc1_fee : shared query () -> async Nat;
    icrc1_metadata : shared query () -> async [(Text, Value)];
    icrc1_minting_account : shared query () -> async ?Account;
    icrc1_name : shared query () -> async Text;
    icrc1_supported_standards : shared query () -> async [StandardRecord];
    icrc1_symbol : shared query () -> async Text;
    icrc1_total_supply : shared query () -> async Nat;
    icrc1_transfer : shared TransferArg -> async Result;
  };
};
