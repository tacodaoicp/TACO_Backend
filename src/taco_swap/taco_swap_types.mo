import Principal "mo:base/Principal";

module {

  // ═══════════════════════════════════════════════════════════════════
  // SWAP STATUS & RECORDS
  // ═══════════════════════════════════════════════════════════════════

  // Tracks exactly which step the swap reached — enables stage-based recovery
  // Each stage carries the expected token amount at that location
  public type SwapStage = {
    #AwaitingDeposit : Nat;              // ICP on user subaccount. Nat = icpAmount
    #TransferredToPool : Nat;            // ICP on pool subaccount. Nat = poolTransferAmount (after 1 fee)
    #DepositRegistered : Nat;            // ICP registered with pool. Nat = swapAmountIn (after 2 fees)
    #SwapCompleted : Nat;                // TACO on pool unused balance. Nat = tacoSwapAmount
    #WithdrawnToSubaccount : Nat;        // TACO on canister user subaccount. Nat = tacoAmount (after withdraw fee)
  };

  public type ClaimPath = {
    #FrontendClaim;
    #WebhookClaim;
    #CoinbaseWebhook;
    #ManualClaim;
    #TimerSweep;
  };

  public type PendingSwap = {
    principal : Principal;
    icpAmount : Nat;           // Original ICP deposit amount (for records/refund)
    stage : SwapStage;         // Where the swap stopped — determines recovery path
    createdAt : Int;
    lastAttempt : Int;
    attempts : Nat;
    errorMessage : ?Text;
  };

  public type OrderRecord = {
    id : Nat;
    principal : Principal;
    icpDeposited : Nat;
    tacoReceived : Nat;
    slippage : Float;
    poolId : Principal;
    timestamp : Int;
    transakOrderId : ?Text;
    tacoTxId : ?Nat;
    claimPath : ClaimPath;
    fiatAmount : ?Text;
    fiatCurrency : ?Text;
  };

  // ═══════════════════════════════════════════════════════════════════
  // CLAIM RESULT
  // ═══════════════════════════════════════════════════════════════════

  public type ClaimResult = {
    #Success : { tacoAmount : Nat; txId : Nat; orderId : Nat };
    #NoDeposit;
    #BelowMinimum : { balance : Nat; minimum : Nat };
    #SwapFailed : Text;
    #AlreadyProcessing;
    #SystemPaused;
    #RateLimited;
    #NotAuthorized;
  };

  // ═══════════════════════════════════════════════════════════════════
  // QUOTE
  // ═══════════════════════════════════════════════════════════════════

  public type QuoteResult = {
    #Ok : {
      estimatedTaco : Nat;
      slippage : Float;
      icpFee : Nat;
      tacoFee : Nat;
    };
    #Err : Text;
  };

  // ═══════════════════════════════════════════════════════════════════
  // CONFIGURATION
  // ═══════════════════════════════════════════════════════════════════

  public type SwapConfig = {
    maxSlippageBasisPoints : ?Nat;
    minDepositICP : ?Nat;
    sweepIntervalNS : ?Int;
    maxRetries : ?Nat;
    systemPaused : ?Bool;
  };

  // ═══════════════════════════════════════════════════════════════════
  // ADMIN
  // ═══════════════════════════════════════════════════════════════════

  public type AdminAction = {
    #Pause;
    #Unpause;
    #UpdateConfig;
    #RecoverFunds;
    #SetPoolId;
    #RetryPending;
    #DiscoverPool;
  };

  public type AdminActionRecord = {
    timestamp : Int;
    caller : Principal;
    action : AdminAction;
    details : Text;
    success : Bool;
  };

  // ═══════════════════════════════════════════════════════════════════
  // HTTP TYPES (with upgrade field for IC gateway)
  // ═══════════════════════════════════════════════════════════════════

  public type HttpRequest = {
    url : Text;
    method : Text;
    body : Blob;
    headers : [(Text, Text)];
  };

  public type HttpResponse = {
    status_code : Nat16;
    headers : [(Text, Text)];
    body : Blob;
    upgrade : ?Bool;
  };

  // ═══════════════════════════════════════════════════════════════════
  // SWAP PROGRESS TRACKING (for frontend polling)
  // ═══════════════════════════════════════════════════════════════════

  // The step the user is currently on in the swap journey (for display)
  public type SwapStep = {
    #NotStarted;
    #WaitingForDeposit;      // Payment registered, waiting for ICP from Coinbase
    #DepositReceived;        // ICP detected on subaccount
    #GettingQuote;           // Fetching price quote from ICPSwap
    #TransferringToPool;     // ICP being sent to ICPSwap pool
    #SwappingTokens;         // Swap executing on ICPSwap (covers deposit+swap+withdraw)
    #TransferringToWallet;   // TACO being sent to user's wallet
    #Complete;               // Done
    #Failed;                 // Error (with retry info)
  };

  // Rich progress object returned by get_swap_status()
  public type SwapProgress = {
    step : SwapStep;
    stepNumber : Nat;        // 0-6 for progress bar
    totalSteps : Nat;        // Always 7
    description : Text;      // Human-readable message
    icpAmount : ?Nat;        // ICP deposited (null if unknown)
    estimatedTaco : ?Nat;    // From quote (null until quoted)
    actualTaco : ?Nat;       // Final amount (null until complete)
    startedAt : Int;         // When tracking began (ns)
    updatedAt : Int;         // Last update (ns)
    errorMessage : ?Text;    // If Failed
    retryCount : Nat;        // Retry attempts
    orderId : ?Nat;          // Set on completion
    txId : ?Nat;             // TACO transfer txId on completion
  };

  // ═══════════════════════════════════════════════════════════════════
  // STATS
  // ═══════════════════════════════════════════════════════════════════

  public type SwapStats = {
    totalSwapsCompleted : Nat;
    totalICPSwapped : Nat;
    totalTACODelivered : Nat;
    pendingSwapsCount : Nat;
    completedOrdersCount : Nat;
    systemPaused : Bool;
    poolConfigured : Bool;
  };

  // ═══════════════════════════════════════════════════════════════════
  // NACHOS MINTING TYPES
  // ═══════════════════════════════════════════════════════════════════

  public type NachosSwapStage = {
    #AwaitingDeposit : Nat;
    #TransferredToTreasury : Nat;
    #MintRequested : Nat;
  };

  public type NachosPendingSwap = {
    principal : Principal;
    icpAmount : Nat;
    stage : NachosSwapStage;
    blockNumber : ?Nat;
    createdAt : Int;
    lastAttempt : Int;
    attempts : Nat;
    errorMessage : ?Text;
  };

  public type NachosClaimResult = {
    #Success : { nachosAmount : Nat; mintId : Nat; orderId : Nat };
    #NoDeposit;
    #BelowMinimum : { balance : Nat; minimum : Nat };
    #MintFailed : Text;
    #AlreadyProcessing;
    #SystemPaused;
    #RateLimited;
    #NotAuthorized;
  };

  public type NachosOrderRecord = {
    id : Nat;
    principal : Principal;
    icpDeposited : Nat;
    nachosReceived : Nat;
    navUsed : Nat;
    feeICP : Nat;
    timestamp : Int;
    nachosMintId : ?Nat;
    claimPath : ClaimPath;
    fiatAmount : ?Text;
    fiatCurrency : ?Text;
  };

  public type NachosQuoteResult = {
    #Ok : { estimatedNachos : Nat; feeEstimate : Nat; navUsed : Nat };
    #Err : Text;
  };

  public type NachosSwapStep = {
    #NotStarted;
    #DepositReceived;
    #TransferringToTreasury;
    #MintingNachos;
    #Complete;
    #Failed;
  };

  public type NachosSwapProgress = {
    step : NachosSwapStep;
    stepNumber : Nat;
    totalSteps : Nat;
    description : Text;
    icpAmount : ?Nat;
    estimatedNachos : ?Nat;
    actualNachos : ?Nat;
    startedAt : Int;
    updatedAt : Int;
    errorMessage : ?Text;
    retryCount : Nat;
    orderId : ?Nat;
    mintId : ?Nat;
  };

  // ═══════════════════════════════════════════════════════════════════
  // SWAP DASHBOARD (bundled query response)
  // ═══════════════════════════════════════════════════════════════════

  public type SwapDashboard = {
    // from get_full_swap_state
    tacoStatus : SwapProgress;
    nachosStatus : NachosSwapProgress;
    hasActiveLock : Bool;
    hasPendingTaco : Bool;
    hasPendingNachos : Bool;
    tacoDepositSubaccount : [Nat8];
    nachosDepositSubaccount : [Nat8];
    // from get_deposit_address
    depositAddress : Text;
    // from get_nachos_deposit_address_for (inlined pure computation)
    nachosDepositAddress : Text;
    // from get_my_orders(20)
    recentTacoOrders : [OrderRecord];
    // from get_my_nachos_orders(20)
    recentNachosOrders : [NachosOrderRecord];
    // from get_config
    config : {
      systemPaused : Bool;
      maxSlippageBasisPoints : Nat;
      minDepositICP : Nat;
      sweepIntervalNS : Int;
      maxRetries : Nat;
      icpTacoPoolId : ?Principal;
      poolZeroForOne : Bool;
      tacoLedgerFee : Nat;
    };
    // from get_stats
    stats : SwapStats;
    // from get_nachos_stats
    nachosStats : {
      totalNachosMints : Nat;
      totalNachosICPDeposited : Nat;
      totalNachosDelivered : Nat;
      nachosPendingCount : Nat;
      nachosOrdersCount : Nat;
      nachosMintingEnabled : Bool;
    };
  };
};
