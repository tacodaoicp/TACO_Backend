import Vector "mo:vector";
import Principal "mo:base/Principal";

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
  // OLD TYPES (without minAllocationDiffBasisPoints)
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
  // NEW TYPES (with minAllocationDiffBasisPoints)
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
    minAllocationDiffBasisPoints : Nat;
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
  };

  public type NewState = {
    rebalanceConfig : NewRebalanceConfig;
    rebalanceState : NewRebalanceState;
  };

  // =========================================
  // MIGRATION FUNCTION
  // =========================================

  public func migrate(oldState : OldState) : NewState {
    // Transform rebalanceConfig by adding new field with default value
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
      minAllocationDiffBasisPoints = 15; // Default: 0.15%
    };

    // Transform rebalanceState with updated config
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
    };
  };
};
