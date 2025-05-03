import LedgerType "mo:ledger-types";
import ICRC2 "../helper/icrc.types";
import ICRC3 "mo:icrc3-mo/service";
import DAO_types "../DAO_backend/dao_types";
import Result "mo:base/Result";

module {
  public type Token = Text;
  public type Decimals = Nat;
  public type TransferFee = Nat;
  public type Holdings = Nat;

  public type SwapError = {
    #InsufficientBalance;
    #InvalidPrice;
    #TokenNotTrusted;
    #InvalidAmount;
    #BlockAlreadyProcessed;
    #InvalidBlock;
    #TransferError;
    #SwapAlreadyRunning;
    #UnexpectedError : Text;
  };

  public type SwapResult = {
    success : Bool;
    error : ?SwapError;
    blockNumber : Nat;
    sentTokenAddress : Text;
    wantedTokenAddress : Text;
    swappedAmount : Nat;
    returnedWantedAmount : Nat;
    returnedSentAmount : Nat;
    usedSentAmount : Nat;
  };

  // Add to type definitions
  public type TokenAllocation = {
    basisPoints : Nat; // Out of 10000
    tacoAllocated : Nat; // Total TACO allocated for this token
    tacoRemaining : Nat; // TACO still available for swaps
  };

  public type PricePoint = DAO_types.PricePoint;

  // Transfer types
  public type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Blob };
  };
  public type SyncError = {
    #NotDAO;
    #UnexpectedError : Text;
  };

  public type TransferResultICP = {
    #Ok : Nat64;
    #Err : {
      #BadFee : { expected_fee : { e8s : Nat64 } };
      #InsufficientFunds : { balance : { e8s : Nat64 } };
      #TxTooOld : { allowed_window_nanos : Nat64 };
      #TxCreatedInFuture;
      #TxDuplicate : { duplicate_of : Nat64 };
    };
  };

  public type BlockData = {
    #ICP : LedgerType.QueryBlocksResponse;
    #ICRC12 : [ICRC2.Transaction];
    #ICRC3 : ICRC3.GetBlocksResult;
  };

  public type TransferResult = {
    #Ok : Nat;
    #Err : {
      #BadFee : { expected_fee : Nat };
      #BadBurn : { min_burn_amount : Nat };
      #InsufficientFunds : { balance : Nat };
      #TooOld;
      #CreatedInFuture : { ledger_time : Nat64 };
      #Duplicate : { duplicate_of : Nat };
      #TemporarilyUnavailable;
      #GenericError : { error_code : Nat; message : Text };
    };
  };

  public type TokenDetails = DAO_types.TokenDetails;

  // Error types
  public type AddTokenError = {
    #TokenAlreadyExists;
    #InvalidAddress;
    #InvalidDecimals;
    #InvalidTokenType;
    #UnexpectedError : Text;
  };

  public type UpdateConfig = {
    balanceUpdateInterval : ?Int;
    blockCleanupInterval : ?Int;
    maxPremium : ?Float;
    minPremium : ?Float;
    maxSlippageBasisPoints : ?Nat;
    PRICE_HISTORY_WINDOW : ?Int;
    swappingEnabled : ?Bool;
    minSwapValueUSD : ?Float;
  };

  // Define the actor interface
  public type Self = actor {
    getSupportedSwapPairs : query () -> async [(Principal, Principal)];

    // Administration and management
    syncTokenDetailsFromDAO : shared (tokenDetails : [(Principal, TokenDetails)]) -> async Result.Result<Text, SyncError>;
    updateConfiguration : shared {
      balanceUpdateInterval : ?Int;
      blockCleanupInterval : ?Int;
      maxPremium : ?Float;
      minPremium : ?Float;
      maxSlippageBasisPoints : ?Nat;
      PRICE_HISTORY_WINDOW : ?Int;
      swappingEnabled : ?Bool;
    } -> async Result.Result<(), Text>;

    // Treasury/DAO integration
    handleExternalSwap : shared (fromToken : Principal, toToken : Principal, amount : Nat) -> async Result.Result<Nat, Text>;
    withdrawTokens : shared (token : Principal, amount : Nat) -> async Result.Result<(), Text>;
    getSystemParameters : query () -> async {
      minPremium : Float;
      maxPremium : Float;
      balanceUpdateInterval : Int;
      blockCleanupInterval : Int;
      maxSlippageBasisPoints : Nat;
      swappingEnabled : Bool;
      priceHistoryWindow : Int;
    };
    setSwappingEnabled : shared (Bool) -> async Result.Result<Text, Text>;
    setTest : shared (Bool) -> async Result.Result<Text, Text>;
    swapTokenForTaco : shared (token : Principal, block : Nat, minimumReceive : Nat) -> async Result.Result<SwapResult, Text>;
    estimateSwapAmount : query (token : Principal, amount : Nat) -> async Result.Result<{ maxAcceptedAmount : Nat; estimatedTacoAmount : Nat; premium : Float; tokenPrice : Nat; tacoPrice : Nat }, Text>;
    getVaultStatus : query () -> async {
      tokenDetails : [(Principal, TokenDetails)];
      targetAllocations : [(Principal, Nat)];
      currentAllocations : [(Principal, Nat)];
      exchangeRates : [(Principal, Float)];
      premiumRange : { min : Float; max : Float };
      totalValueICP : Nat;
    };

  };
};
