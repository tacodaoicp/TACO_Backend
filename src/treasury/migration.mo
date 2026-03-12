import Vector "mo:vector";

module {
  // =========================================
  // SHARED TYPES (unchanged between versions)
  // =========================================

  public type RebalanceStatus = {
    #Idle;
    #Trading;
    #Failed : Text;
  };

  public type ExchangeType = {
    #ICPSwap;
    #KongSwap;
  };

  public type TradeRecord = {
    tokenSold : Principal;
    tokenBought : Principal;
    amountSold : Nat;
    amountBought : Nat;
    exchange : ExchangeType;
    timestamp : Int;
    success : Bool;
    error : ?Text;
    slippage : Float;
  };

  public type SkipBreakdown = {
    noPairsFound : Nat;
    noExecutionPath : Nat;
    tokensFiltered : Nat;
    pausedTokens : Nat;
    insufficientCandidates : Nat;
  };

  public type RebalanceMetrics = {
    lastPriceUpdate : Int;
    lastRebalanceAttempt : Int;
    totalTradesExecuted : Nat;
    totalTradesFailed : Nat;
    totalTradesSkipped : Nat;
    skipBreakdown : SkipBreakdown;
    currentStatus : RebalanceStatus;
    portfolioValueICP : Nat;
    portfolioValueUSD : Float;
  };

  // =========================================
  // OLD TYPES (staging had minAllocationDiffBasisPoints IN RebalanceConfig)
  // =========================================

  public type OldRebalanceConfig = {
    rebalanceIntervalNS : Nat;
    maxTradeAttemptsPerInterval : Nat;
    minTradeValueICP : Nat;
    maxTradeValueICP : Nat;
    portfolioRebalancePeriodNS : Nat;
    maxSlippageBasisPoints : Nat;
    maxTradesStored : Nat;
    maxKongswapAttempts : Nat;
    shortSyncIntervalNS : Nat;
    longSyncIntervalNS : Nat;
    tokenSyncTimeoutNS : Nat;
    minAllocationDiffBasisPoints : Nat;
  };

  public type OldRebalanceState = {
    status : RebalanceStatus;
    config : OldRebalanceConfig;
    metrics : RebalanceMetrics;
    lastTrades : Vector.Vector<TradeRecord>;
    priceUpdateTimerId : ?Nat;
    rebalanceTimerId : ?Nat;
  };

  // =========================================
  // NEW TYPES (minAllocationDiffBasisPoints removed from config, now standalone var)
  // =========================================

  public type NewRebalanceConfig = {
    rebalanceIntervalNS : Nat;
    maxTradeAttemptsPerInterval : Nat;
    minTradeValueICP : Nat;
    maxTradeValueICP : Nat;
    portfolioRebalancePeriodNS : Nat;
    maxSlippageBasisPoints : Nat;
    maxTradesStored : Nat;
    maxKongswapAttempts : Nat;
    shortSyncIntervalNS : Nat;
    longSyncIntervalNS : Nat;
    tokenSyncTimeoutNS : Nat;
  };

  public type NewRebalanceState = {
    status : RebalanceStatus;
    config : NewRebalanceConfig;
    metrics : RebalanceMetrics;
    lastTrades : Vector.Vector<TradeRecord>;
    priceUpdateTimerId : ?Nat;
    rebalanceTimerId : ?Nat;
  };

  // =========================================
  // STATE WRAPPERS
  // =========================================

  public type OldState = {
    rebalanceConfig : OldRebalanceConfig;
    rebalanceState : OldRebalanceState;
    minAllocationDiffBasisPoints : Nat;
  };

  public type NewState = {
    rebalanceConfig : NewRebalanceConfig;
    rebalanceState : NewRebalanceState;
    minAllocationDiffBasisPoints : Nat;
  };

  // =========================================
  // MIGRATION FUNCTION
  // Staging-only: extracts minAllocationDiffBasisPoints from RebalanceConfig
  // into a standalone stable var, removes it from the config record.
  // =========================================

  public func migrate(oldState : OldState) : NewState {
    let newConfig : NewRebalanceConfig = {
      rebalanceIntervalNS = oldState.rebalanceConfig.rebalanceIntervalNS;
      maxTradeAttemptsPerInterval = oldState.rebalanceConfig.maxTradeAttemptsPerInterval;
      minTradeValueICP = oldState.rebalanceConfig.minTradeValueICP;
      maxTradeValueICP = oldState.rebalanceConfig.maxTradeValueICP;
      portfolioRebalancePeriodNS = oldState.rebalanceConfig.portfolioRebalancePeriodNS;
      maxSlippageBasisPoints = oldState.rebalanceConfig.maxSlippageBasisPoints;
      maxTradesStored = oldState.rebalanceConfig.maxTradesStored;
      maxKongswapAttempts = oldState.rebalanceConfig.maxKongswapAttempts;
      shortSyncIntervalNS = oldState.rebalanceConfig.shortSyncIntervalNS;
      longSyncIntervalNS = oldState.rebalanceConfig.longSyncIntervalNS;
      tokenSyncTimeoutNS = oldState.rebalanceConfig.tokenSyncTimeoutNS;
    };

    let newState : NewRebalanceState = {
      status = oldState.rebalanceState.status;
      config = newConfig;
      metrics = oldState.rebalanceState.metrics;
      lastTrades = oldState.rebalanceState.lastTrades;
      priceUpdateTimerId = oldState.rebalanceState.priceUpdateTimerId;
      rebalanceTimerId = oldState.rebalanceState.rebalanceTimerId;
    };

    {
      rebalanceConfig = newConfig;
      rebalanceState = newState;
      minAllocationDiffBasisPoints = oldState.minAllocationDiffBasisPoints;
    };
  };
};
