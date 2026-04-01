import Principal "mo:base/Principal";

module {

  // ═══════════════════════════════════════════════════════
  // ACCOUNT TYPE (ICRC-1 compatible)
  // ═══════════════════════════════════════════════════════

  public type Account = { owner : Principal; subaccount : ?Blob };

  // ═══════════════════════════════════════════════════════
  // ACCEPTED TOKEN TYPES
  // ═══════════════════════════════════════════════════════

  public type AcceptedTokenConfig = {
    addedAt : Int;
    addedBy : Principal;
    enabled : Bool;
  };

  // ═══════════════════════════════════════════════════════
  // MINT TYPES
  // ═══════════════════════════════════════════════════════

  public type MintMode = {
    #ICP;
    #SingleToken;
    #PortfolioShare;
  };

  public type TokenDeposit = {
    token : Principal;
    amount : Nat;
    priceUsed : Nat;
    valueICP : Nat;
  };

  public type MintResult = {
    success : Bool;
    mintId : Nat;
    mintMode : MintMode;
    nachosReceived : Nat;
    navUsed : Nat;
    deposits : [TokenDeposit];
    totalDepositValueICP : Nat;
    excessReturned : [TokenDeposit];
    feeValueICP : Nat;
    netValueICP : Nat;
    nachosLedgerTxId : ?Nat;
    recipient : Account;
  };

  public type MintRecord = {
    id : Nat;
    timestamp : Int;
    caller : Principal;
    recipient : ?Account;
    mintMode : MintMode;
    deposits : [TokenDeposit];
    excessReturned : [TokenDeposit];
    nachosReceived : Nat;
    navUsed : Nat;
    totalDepositValueICP : Nat;
    feeValueICP : Nat;
    netValueICP : Nat;
    nachosLedgerTxId : ?Nat;
  };

  // ═══════════════════════════════════════════════════════
  // FEE EXEMPTION TYPES
  // ═══════════════════════════════════════════════════════

  public type FeeExemptConfig = {
    addedAt : Int;
    addedBy : Principal;
    reason : Text;
    enabled : Bool;
  };

  // ═══════════════════════════════════════════════════════
  // TRANSFER QUEUE TYPES
  // ═══════════════════════════════════════════════════════

  public type TransferOperationType = {
    #MintReturn;
    #BurnPayout;
    #ExcessReturn;
    #CancelReturn;
    #Recovery;
    #ForwardToPortfolio;
  };

  public type TransferStatus = {
    #Pending;
    #Sent;
    #Confirmed : Nat64;
    #Failed : Text;
  };

  public type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Blob };
  };

  public type VaultTransferTask = {
    id : Nat;
    caller : Principal;
    recipient : TransferRecipient;
    amount : Nat;
    tokenPrincipal : Principal;
    fromSubaccount : Nat8;
    operationType : TransferOperationType;
    operationId : Nat;
    status : TransferStatus;
    createdAt : Int;
    updatedAt : Int;
    retryCount : Nat;
    actualAmountSent : ?Nat;
    blockIndex : ?Nat64;
  };

  // ═══════════════════════════════════════════════════════
  // DEPOSIT TRACKING TYPES
  // ═══════════════════════════════════════════════════════

  public type DepositStatus = {
    #Verified;
    #Processing;
    #Consumed;
    #Cancelled;
    #Expired;
  };

  public type ActiveDeposit = {
    blockKey : Text;
    caller : Principal;
    fromSubaccount : ?Blob;
    tokenPrincipal : Principal;
    amount : Nat;
    blockNumber : Nat;
    timestamp : Int;
    status : DepositStatus;
    mintBurnId : ?Nat;
    cancellationTxId : ?Nat64;
  };

  // ═══════════════════════════════════════════════════════
  // DEPOSIT STATISTICS TYPES
  // ═══════════════════════════════════════════════════════

  public type DepositEntry = {
    timestamp : Int;
    amount : Nat;
    caller : Principal;
    blockNumber : Nat;
    valueICP : Nat;
  };

  // ═══════════════════════════════════════════════════════
  // BURN TYPES
  // ═══════════════════════════════════════════════════════

  public type TokenTransferResult = {
    token : Principal;
    amount : Nat;
    txId : ?Nat;
  };

  public type FailedTokenTransfer = {
    token : Principal;
    requestedAmount : Nat;
    error : Text;
  };

  public type BurnResult = {
    success : Bool;
    burnId : Nat;
    nachosBurned : Nat;
    navUsed : Nat;
    redemptionValueICP : Nat;
    feeValueICP : Nat;
    netValueICP : Nat;
    tokensReceived : [TokenTransferResult];
    skippedDustTokens : [Principal];
    nachosLedgerTxId : ?Nat;
    partialFailure : Bool;
    failedTokens : [FailedTokenTransfer];
  };

  public type BurnRecord = {
    id : Nat;
    timestamp : Int;
    caller : Principal;
    nachosBurned : Nat;
    navUsed : Nat;
    redemptionValueICP : Nat;
    feeValueICP : Nat;
    netValueICP : Nat;
    tokensReceived : [TokenTransferResult];
    skippedDustTokens : [Principal];
    nachosLedgerTxId : ?Nat;
    partialFailure : Bool;
  };

  // ═══════════════════════════════════════════════════════
  // NAV TYPES
  // ═══════════════════════════════════════════════════════

  public type CachedNAV = {
    navPerTokenE8s : Nat;
    portfolioValueICP : Nat;
    nachosSupply : Nat;
    timestamp : Int;
  };

  public type NavSnapshotReason = {
    #Mint;
    #Burn;
    #Scheduled;
    #Manual;
  };

  public type NavSnapshot = {
    timestamp : Int;
    navPerTokenE8s : Nat;
    portfolioValueICP : Nat;
    nachosSupply : Nat;
    reason : NavSnapshotReason;
  };

  // ═══════════════════════════════════════════════════════
  // FEE TYPES
  // ═══════════════════════════════════════════════════════

  public type FeeRecord = {
    timestamp : Int;
    feeType : { #Mint; #Burn };
    feeAmountICP : Nat;
    userPrincipal : Principal;
    operationId : Nat;
  };

  // ═══════════════════════════════════════════════════════
  // ERROR TYPES
  // ═══════════════════════════════════════════════════════

  public type NachosError = {
    #SystemPaused;
    #MintingDisabled;
    #BurningDisabled;
    #PriceStale;
    #InsufficientBalance;
    #BelowMinimumValue;
    #RateLimitExceeded;
    #OperationInProgress;
    #BlockAlreadyProcessed;
    #BlockVerificationFailed : Text;
    #TokenNotActive;
    #TokenPaused;
    #PortfolioTokenPaused : { pausedTokens : [{ token : Principal; symbol : Text }] };
    #TokenNotAccepted;
    #AllocationExceeded;
    #InvalidAllocation;
    #SlippageExceeded;
    #InvalidPrice;
    #InsufficientLiquidity : { token : Principal; available : Nat; requested : Nat };
    #CircuitBreakerActive;
    #BurnLimitExceeded : { maxPer4Hours : Nat; recentBurns : Nat; requested : Nat };
    #MintLimitExceeded : { maxPer4Hours : Nat; recentMints : Nat; requested : Nat };
    #UserMintLimitExceeded : { maxPer4Hours : Nat; recentMints : Nat; requested : Nat };
    #UserBurnLimitExceeded : { maxPer4Hours : Nat; recentBurns : Nat; requested : Nat };
    #AboveMaximumValue : { max : Nat; requested : Nat };
    #PortfolioShareMismatch : {
      expected : [{ token : Principal; basisPoints : Nat }];
      received : [{ token : Principal; basisPoints : Nat }];
    };
    #GenesisNotComplete;
    #GenesisAlreadyDone;
    #TransferError : Text;
    #DepositNotFound;
    #DepositAlreadyCancelled;
    #DepositAlreadyConsumed;
    #DepositExpired;
    #NotDepositor;
    #RollbackFailed : Text;
    #NotAuthorized;
    #UnexpectedError : Text;
  };

  // ═══════════════════════════════════════════════════════
  // FAILED DELIVERY TYPES
  // ═══════════════════════════════════════════════════════

  public type FailedDeliveryStatus = {
    #Undelivered;
    #RetryQueued;
    #Delivered;
  };

  public type FailedDeliveryEntry = {
    token : Principal;
    amount : Nat;
    originalTaskId : Nat;
    retryTaskId : ?Nat;
    status : FailedDeliveryStatus;
    exhaustedAt : Int;
    retriedAt : ?Int;
  };

  // ═══════════════════════════════════════════════════════
  // CIRCUIT BREAKER TYPES
  // ═══════════════════════════════════════════════════════

  public type CircuitBreakerAction = {
    #PauseMint;
    #PauseBurn;
    #PauseBoth;
    #RejectOperation;
  };

  public type CircuitBreakerConditionType = {
    #NavDrop;
    #PriceChange;
    #BalanceChange;
    #DecimalChange;
    #TokenPaused;
  };

  public type CircuitBreakerCondition = {
    id : Nat;
    conditionType : CircuitBreakerConditionType;
    thresholdPercent : Float;
    timeWindowNS : Nat;
    direction : { #Up; #Down; #Both };
    action : CircuitBreakerAction;
    applicableTokens : [Principal];
    enabled : Bool;
    createdAt : Int;
    createdBy : Principal;
  };

  public type CircuitBreakerAlert = {
    id : Nat;
    conditionId : Nat;
    conditionType : CircuitBreakerConditionType;
    token : ?Principal;
    tokenSymbol : Text;
    timestamp : Int;
    actionTaken : CircuitBreakerAction;
    details : Text;
  };

  public type CircuitBreakerConditionInput = {
    conditionType : CircuitBreakerConditionType;
    thresholdPercent : Float;
    timeWindowNS : Nat;
    direction : { #Up; #Down; #Both };
    action : CircuitBreakerAction;
    applicableTokens : [Principal];
    enabled : Bool;
  };

  // ═══════════════════════════════════════════════════════
  // CONFIGURATION UPDATE TYPE
  // ═══════════════════════════════════════════════════════

  public type NachosUpdateConfig = {
    mintFeeBasisPoints : ?Nat;
    burnFeeBasisPoints : ?Nat;
    minMintValueICP : ?Nat;
    minBurnValueICP : ?Nat;
    MAX_PRICE_STALENESS_NS : ?Int;
    PRICE_HISTORY_WINDOW : ?Int;
    maxSlippageBasisPoints : ?Nat;
    maxNachosBurnPer4Hours : ?Nat;
    maxMintICPWorthPer4Hours : ?Nat;
    maxMintOpsPerUser4Hours : ?Nat;
    maxBurnOpsPerUser4Hours : ?Nat;
    navDropThresholdPercent : ?Float;
    navDropTimeWindowNS : ?Nat;
    portfolioShareMaxDeviationBP : ?Nat;
    cancellationFeeMultiplier : ?Nat;
    mintingEnabled : ?Bool;
    burningEnabled : ?Bool;
    maxMintICPPerUser4Hours : ?Nat;
    maxBurnNachosPerUser4Hours : ?Nat;
    maxMintAmountICP : ?Nat;
    maxBurnAmountNachos : ?Nat;
  };

};
