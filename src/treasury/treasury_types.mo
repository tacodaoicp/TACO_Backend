import Result "mo:base/Result";
import DAO_types "../DAO_backend/dao_types";
import Vector "mo:vector";
import Prim "mo:prim";
import Principal "mo:base/Principal";
module {
  public type TokenDetails = DAO_types.TokenDetails;

  public type PricePoint = DAO_types.PricePoint;

  public type TokenInfo = {
    address : Text;
    name : Text;
    symbol : Text;
    decimals : Nat;
    transfer_fee : Nat;
  };

  public type Subaccount = Blob;

  public type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Subaccount };
  };

  public type Allocation = DAO_types.Allocation;

  public type TransferResultICRC1 = {
    #Ok : Nat;
    #Err : {
      #BadFee : { expected_fee : Nat };
      #BadBurn : { min_burn_amount : Nat };
      #InsufficientFunds : { balance : Nat };
      #Duplicate : { duplicate_of : Nat };
      #TemporarilyUnavailable;
      #GenericError : { error_code : Nat; message : Text };
      #TooOld;
      #CreatedInFuture : { ledger_time : Nat64 };
    };
  };

  public type TransferResultICP = {
    #Ok : Nat64;
    #Err : {
      #BadFee : {
        expected_fee : {
          e8s : Nat64;
        };
      };
      #InsufficientFunds : {
        balance : {
          e8s : Nat64;
        };
      };
      #TxTooOld : { allowed_window_nanos : Nat64 };
      #TxCreatedInFuture;
      #TxDuplicate : { duplicate_of : Nat64 };
    };
  };
  public type SyncErrorTreasury = {
    #NotDAO;
    #UnexpectedError : Text;
  };

  // Rebalance configuration parameters
  public type RebalanceConfig = {
    rebalanceIntervalNS : Nat; // How often to check for rebalancing needs
    maxTradeAttemptsPerInterval : Nat; // Maximum number of trade attempts per interval
    minTradeValueICP : Nat; // Minimum trade size in ICP e8s
    maxTradeValueICP : Nat; // Maximum trade size in ICP e8s
    portfolioRebalancePeriodNS : Nat; // Target period for complete portfolio rebalance
    maxSlippageBasisPoints : Nat; // Maximum allowed slippage in basis points (e.g. 10 = 0.1%)
    maxTradesStored : Nat; // Maximum number of trades to store
    maxKongswapAttempts : Nat; // Maximum number of attempts to call kongswap
    shortSyncIntervalNS : Nat; // frequent sync for prices and balances
    longSyncIntervalNS : Nat; // less frequent sync for metadata updates
    tokenSyncTimeoutNS : Nat; // maximum time without sync before pausing
  };

  public type UpdateConfig = {
    priceUpdateIntervalNS : ?Nat;
    rebalanceIntervalNS : ?Nat;
    maxTradeAttemptsPerInterval : ?Nat;
    minTradeValueICP : ?Nat;
    maxTradeValueICP : ?Nat;
    portfolioRebalancePeriodNS : ?Nat;
    maxSlippageBasisPoints : ?Nat;
    maxTradesStored : ?Nat;
    maxKongswapAttempts : ?Nat;
    shortSyncIntervalNS : ?Nat;
    longSyncIntervalNS : ?Nat;
    maxPriceHistoryEntries : ?Nat;
    tokenSyncTimeoutNS : ?Nat;
  };

  type hash<K> = (
    getHash : (K) -> Nat32,
    areEqual : (K, K) -> Bool,
  );
  func hashPrincipalPrincipal(key : (Principal, Principal)) : Nat32 {
    Prim.hashBlob(Prim.encodeUtf8(Principal.toText(key.0) #Principal.toText(key.1))) & 0x3fffffff;
  };

  public let hashpp = (hashPrincipalPrincipal, func(a, b) = a == b) : hash<(Principal, Principal)>;

  public type ExchangeType = {
    #KongSwap;
    #ICPSwap;
  };

  // Price source for a token
  public type PriceSource = {
    #Direct : ExchangeType; // Direct ICP pair
    #Indirect : {
      exchange : ExchangeType;
      intermediaryToken : Principal;
    };
    #NTN; // Price from NTN service
  };

  // Extended price information for a token
  public type PriceInfo = {
    priceInICP : Nat; // Price in ICP (e8s)
    priceInUSD : Float; // USD price
    lastUpdate : Int; // Timestamp of last update
    source : PriceSource; // Source of the price
  };

  // Liquidity information for a trading pair
  public type LiquidityInfo = {
    exchange : ExchangeType;
    tokenA : Principal;
    tokenB : Principal;
    liquidityTokenA : Nat;
    liquidityTokenB : Nat;
    slippageBasisPoints : Nat; // Estimated price impact in basis points
  };

  // Status of a rebalancing operation
  public type RebalanceStatus = {
    #Idle;
    #Trading;
    #Failed : Text;
  };

  // Record of an attempted or completed trade
  public type TradeRecord = {
    tokenSold : Principal;
    tokenBought : Principal;
    amountSold : Nat;
    amountBought : Nat;
    exchange : ExchangeType;
    timestamp : Int;
    success : Bool;
    error : ?Text;
    slippage : Float; // Add this field
  };

  // Trade execution result
  public type TradeResult = {
    #Success : {
      tokenSold : Principal;
      tokenBought : Principal;
      amountSold : Nat;
      amountBought : Nat;
      txId : Nat64;
    };
    #Failure : {
      error : Text;
      retryable : Bool;
    };
  };

  // Granular skip tracking
  public type SkipBreakdown = {
    noPairsFound : Nat;         // When selectTradingPair returns null
    noExecutionPath : Nat;      // When findBestExecution fails
    tokensFiltered : Nat;       // When tokens excluded due to small allocation differences
    pausedTokens : Nat;         // When tokens excluded due to being paused/sync failures
    insufficientCandidates : Nat; // When less than 2 viable tokens for trading
  };

  // System metrics for monitoring
  public type RebalanceMetrics = {
    lastPriceUpdate : Int;
    lastRebalanceAttempt : Int;
    totalTradesExecuted : Nat;
    totalTradesFailed : Nat;
    totalTradesSkipped : Nat;   // NEW: Total count of all skipped trades
    skipBreakdown : SkipBreakdown; // NEW: Granular breakdown of skip reasons
    currentStatus : RebalanceStatus;
    portfolioValueICP : Nat;
    portfolioValueUSD : Float;
  };

  // Rebalance operation parameters
  public type RebalanceOperation = {
    #UpdateConfig : RebalanceConfig;
    #StartRebalance;
    #StopRebalance;
    #UpdatePrices;
  };

  // Errors that can occur during rebalancing
  public type RebalanceError = {
    #ConfigError : Text;
    #PriceError : Text;
    #TradeError : Text;
    #LiquidityError : Text;
    #SystemError : Text;
  };

  // State variables to track ongoing operations
  public type RebalanceState = {
    status : RebalanceStatus;
    config : RebalanceConfig;
    metrics : RebalanceMetrics;
    lastTrades : Vector.Vector<TradeRecord>;
    priceUpdateTimerId : ?Nat;
    rebalanceTimerId : ?Nat;
  };

  public type RebalanceStateArray = {
    status : RebalanceStatus;
    config : RebalanceConfig;
    metrics : RebalanceMetrics;
    lastTrades : [TradeRecord];
    priceUpdateTimerId : ?Nat;
    rebalanceTimerId : ?Nat;
  };

  // Response for status queries
  public type RebalanceStatusResponse = {
    state : RebalanceStateArray;
    currentAllocations : [(Principal, Nat)];
    targetAllocations : [(Principal, Nat)];
    priceInfo : [(Principal, PriceInfo)];
  };

  public type TokenAllocation = {
    token : Principal;
    currentBasisPoints : Nat; // Current allocation in basis points
    targetBasisPoints : Nat; // Target allocation from DAO
    diffBasisPoints : Int; // Difference (target - current)
    valueInICP : Nat; // Current value in ICP
  };

  //=========================================================================
  // PRICE FAILSAFE SYSTEM TYPES
  //=========================================================================

  // Price movement direction for trigger conditions
  public type PriceDirection = {
    #Up;    // Price increase
    #Down;  // Price decrease
  };

  // Type of price change that triggered the condition
  public type ChangeType = {
    #CurrentToMin;  // Change from current price to minimum in window
    #CurrentToMax;  // Change from current price to maximum in window
    #MinToMax;      // Change from minimum to maximum in window
  };

  // Price trigger condition configuration
  public type TriggerCondition = {
    id : Nat;                      // Unique identifier
    name : Text;                   // Human-readable name
    direction : PriceDirection;    // Price direction to monitor
    percentage : Float;            // Percentage change threshold (e.g., 20.0 for 20%)
    timeWindowNS : Nat;           // Time window in nanoseconds
    applicableTokens : [Principal]; // Empty array means applies to all tokens
    isActive : Bool;               // Can be disabled without deleting
    createdAt : Int;               // Creation timestamp
    createdBy : Principal;         // Creator principal
  };

  // Price data at the time of trigger
  public type TriggerPriceData = {
    currentPrice : Nat;         // Current price when triggered
    minPriceInWindow : Nat;     // Minimum price in the time window
    maxPriceInWindow : Nat;     // Maximum price in the time window
    windowStartTime : Int;      // Start of the analysis window
    actualChangePercent : Float; // Actual percentage change that triggered
    changeType : ChangeType;    // Type of change that caused the trigger
  };

  // Log entry for price alert events
  public type PriceAlertLog = {
    id : Nat;                           // Unique log entry ID
    timestamp : Int;                    // When the alert was triggered
    token : Principal;                  // Token that triggered the alert
    tokenSymbol : Text;                 // Token symbol for readability
    triggeredCondition : TriggerCondition; // The condition that was triggered
    priceData : TriggerPriceData;       // Price data at trigger time
  };

  // Errors for price failsafe operations
  public type PriceFailsafeError = {
    #NotAuthorized;
    #ConditionNotFound;
    #InvalidPercentage;
    #InvalidTimeWindow;
    #InvalidTokenList;
    #DuplicateName;
    #SystemError : Text;
  };

  // Update parameters for trigger conditions
  public type TriggerConditionUpdate = {
    name : ?Text;
    direction : ?PriceDirection;
    percentage : ?Float;
    timeWindowNS : ?Nat;
    applicableTokens : ?[Principal];
    isActive : ?Bool;
  };

  //=========================================================================
  // PORTFOLIO SNAPSHOT SYSTEM TYPES
  //=========================================================================

  // Individual token data in a portfolio snapshot
  public type TokenSnapshot = {
    token : Principal;
    symbol : Text;
    balance : Nat;           // Absolute token amount
    decimals : Nat;
    priceInICP : Nat;        // Price at snapshot time (e8s)
    priceInUSD : Float;      // Price at snapshot time
    valueInICP : Nat;        // balance * priceInICP
    valueInUSD : Float;      // balance * priceInUSD
  };

  // Complete portfolio state at a point in time
  public type PortfolioSnapshot = {
    timestamp : Int;
    tokens : [TokenSnapshot];
    totalValueICP : Nat;
    totalValueUSD : Float;
    snapshotReason : SnapshotReason;
  };

  // Reason why a portfolio snapshot was taken
  public type SnapshotReason = {
    #PreTrade;          // Before executing trades
    #PostTrade;         // After executing trades
    #Scheduled;         // Timer-based hourly snapshot
    #PriceUpdate;       // After price sync
    #Manual;            // Admin-triggered
  };

  // Response for portfolio history queries
  public type PortfolioHistoryResponse = {
    snapshots : [PortfolioSnapshot];
    totalCount : Nat;
  };

  // Errors for portfolio snapshot operations
  public type PortfolioSnapshotError = {
    #NotAuthorized;
    #InvalidLimit;
    #SystemError : Text;
  };

  //=========================================================================
  // PORTFOLIO CIRCUIT BREAKER SYSTEM TYPES
  //=========================================================================

  // Portfolio value change direction for circuit breaker conditions
  public type PortfolioDirection = {
    #Up;    // Portfolio value increase
    #Down;  // Portfolio value decrease
  };

  // Portfolio circuit breaker trigger condition configuration
  public type PortfolioCircuitBreakerCondition = {
    id : Nat;                      // Unique identifier
    name : Text;                   // Human-readable name
    direction : PortfolioDirection; // Portfolio value direction to monitor
    percentage : Float;            // Percentage change threshold (e.g., 20.0 for 20%)
    timeWindowNS : Nat;           // Time window in nanoseconds
    valueType : PortfolioValueType; // Whether to monitor ICP or USD value
    isActive : Bool;               // Can be disabled without deleting
    createdAt : Int;               // Creation timestamp
    createdBy : Principal;         // Creator principal
  };

  // Type of portfolio value to monitor
  public type PortfolioValueType = {
    #ICP;  // Monitor portfolio value in ICP
    #USD;  // Monitor portfolio value in USD
  };

  // Portfolio value data at the time of trigger
  public type PortfolioTriggerData = {
    currentValue : Float;        // Current portfolio value when triggered
    minValueInWindow : Float;    // Minimum portfolio value in the time window
    maxValueInWindow : Float;    // Maximum portfolio value in the time window
    windowStartTime : Int;       // Start of the analysis window
    actualChangePercent : Float; // Actual percentage change that triggered
    valueType : PortfolioValueType; // Whether this is ICP or USD value
  };

  // Log entry for portfolio circuit breaker events
  public type PortfolioCircuitBreakerLog = {
    id : Nat;                           // Unique log entry ID
    timestamp : Int;                    // When the circuit breaker was triggered
    triggeredCondition : PortfolioCircuitBreakerCondition; // The condition that was triggered
    portfolioData : PortfolioTriggerData; // Portfolio data at trigger time
    pausedTokens : [Principal];         // Tokens that were paused as a result
  };

  // Errors for portfolio circuit breaker operations
  public type PortfolioCircuitBreakerError = {
    #NotAuthorized;
    #ConditionNotFound;
    #InvalidPercentage;
    #InvalidTimeWindow;
    #InvalidParameters : Text;
    #DuplicateName;
    #SystemError : Text;
  };

  // Update parameters for portfolio circuit breaker conditions
  public type PortfolioCircuitBreakerUpdate = {
    name : ?Text;
    direction : ?PortfolioDirection;
    percentage : ?Float;
    timeWindowNS : ?Nat;
    valueType : ?PortfolioValueType;
    isActive : ?Bool;
  };

  //=========================================================================
  // TRADING PAUSE SYSTEM TYPES
  //=========================================================================

  // Reasons why a token can be paused from trading
  public type TradingPauseReason = {
    #PriceAlert : { 
      conditionName : Text; 
      triggeredAt : Int;
      alertId : Nat;
    };
    #CircuitBreaker : { 
      reason : Text; 
      triggeredAt : Int;
      severity : Text; // "High", "Critical", etc.
    };
  };

  // Record of a token paused from trading
  public type TradingPauseRecord = {
    token : Principal;
    tokenSymbol : Text;
    reason : TradingPauseReason;
    pausedAt : Int;
  };

  // Response for querying trading pauses
  public type TradingPausesResponse = {
    pausedTokens : [TradingPauseRecord];
    totalCount : Nat;
  };

  // Errors for trading pause operations
  public type TradingPauseError = {
    #NotAuthorized;
    #TokenNotFound;
    #TokenNotPaused;
    #TokenAlreadyPaused;
    #SystemError : Text;
  };

  public type Self = actor {
    receiveTransferTasks : shared ([(TransferRecipient, Nat, Principal, Nat8)], Bool) -> async (Bool, ?[(Principal, Nat64)]);
    getTokenDetails : shared () -> async [(Principal, TokenDetails)];
    getCurrentAllocations : shared () -> async [Allocation];
    setTest : shared (Bool) -> async ();
    syncTokenDetailsFromDAO : shared ([(Principal, TokenDetails)]) -> async Result.Result<Text, SyncErrorTreasury>;
    updateRebalanceConfig : shared (UpdateConfig, ?Bool) -> async Result.Result<Text, SyncErrorTreasury>;
  };
};
