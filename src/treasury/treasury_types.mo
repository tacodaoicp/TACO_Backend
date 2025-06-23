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

  public type Self = actor {
    receiveTransferTasks : shared ([(TransferRecipient, Nat, Principal, Nat8)], Bool) -> async (Bool, ?[(Principal, Nat64)]);
    getTokenDetails : shared () -> async [(Principal, TokenDetails)];
    getCurrentAllocations : shared () -> async [Allocation];
    setTest : shared (Bool) -> async ();
    syncTokenDetailsFromDAO : shared ([(Principal, TokenDetails)]) -> async Result.Result<Text, SyncErrorTreasury>;
    updateRebalanceConfig : shared (UpdateConfig, ?Bool) -> async Result.Result<Text, SyncErrorTreasury>;
  };
};
