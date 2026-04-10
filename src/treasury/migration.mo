import Vector "mo:vector";
import TreasuryTypes "./treasury_types";

module {
  // =========================================
  // OLD TYPES (currently deployed on-chain)
  // ExchangeType without #TACO, TreasuryAdminActionType without LP variants
  // =========================================

  public type OldExchangeType = {
    #ICPSwap;
    #KongSwap;
    #TACO;
  };

  public type OldTradeRecord = {
    tokenSold : Principal;
    tokenBought : Principal;
    amountSold : Nat;
    amountBought : Nat;
    exchange : OldExchangeType;
    timestamp : Int;
    success : Bool;
    error : ?Text;
    slippage : Float;
  };

  public type RebalanceStatus = {
    #Idle;
    #Trading;
    #Failed : Text;
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

  public type RebalanceConfig = {
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
    config : RebalanceConfig;
    metrics : RebalanceMetrics;
    lastTrades : Vector.Vector<OldTradeRecord>;
    priceUpdateTimerId : ?Nat;
    rebalanceTimerId : ?Nat;
  };

  public type OldTreasuryAdminActionType = {
    #StartRebalancing;
    #StopRebalancing;
    #ResetRebalanceState;
    #UpdateRebalanceConfig: {oldConfig: Text; newConfig: Text};
    #CanisterStart;
    #CanisterStop;
    #PauseTokenManual: {token: Principal; pauseType: Text};
    #UnpauseToken: {token: Principal};
    #ClearAllTradingPauses;
    #AddTriggerCondition: {conditionId: Nat; conditionType: Text; details: Text};
    #RemoveTriggerCondition: {conditionId: Nat};
    #UpdateTriggerCondition: {conditionId: Nat; oldCondition: Text; newCondition: Text};
    #SetTriggerConditionActive: {conditionId: Nat; isActive: Bool};
    #ClearPriceAlerts;
    #AddPortfolioCircuitBreaker: {conditionId: Nat; conditionType: Text; details: Text};
    #RemovePortfolioCircuitBreaker: {conditionId: Nat};
    #UpdatePortfolioCircuitBreaker: {conditionId: Nat; oldCondition: Text; newCondition: Text};
    #SetPortfolioCircuitBreakerActive: {conditionId: Nat; isActive: Bool};
    #UpdatePausedTokenThreshold: {oldThreshold: Nat; newThreshold: Nat};
    #ClearPortfolioCircuitBreakerLogs;
    #UpdateMaxPortfolioSnapshots: {oldLimit: Nat; newLimit: Nat};
    #TakeManualSnapshot;
    #StartPortfolioSnapshots;
    #StopPortfolioSnapshots;
    #UpdatePortfolioSnapshotInterval: {oldIntervalNS: Nat; newIntervalNS: Nat};
    #ExecuteTradingCycle;
    #SetTestMode: {isTestMode: Bool};
    #ClearSystemLogs;
    // LP Management
    #LPAddLiquidity: {pool: Text; details: Text};
    #LPRemoveLiquidity: {pool: Text; details: Text};
    #LPClaimFees: {pool: Text; details: Text};
    #LPEmergencyExit: {positionsRemoved: Nat};
    #LPConfigUpdate: {details: Text};
    #LPPoolConfigUpdate: {pool: Text; details: Text};
  };

  public type OldTreasuryAdminActionRecord = {
    id: Nat;
    timestamp: Int;
    admin: Principal;
    actionType: OldTreasuryAdminActionType;
    reason: Text;
    success: Bool;
    errorMessage: ?Text;
  };

  // =========================================
  // STATE WRAPPERS
  // Old = on-chain deployed types
  // New = current code types (from TreasuryTypes)
  // =========================================

  public type OldState = {
    rebalanceState : OldRebalanceState;
    treasuryAdminActions : Vector.Vector<OldTreasuryAdminActionRecord>;
  };

  public type NewState = {
    rebalanceState : TreasuryTypes.RebalanceState;
    treasuryAdminActions : Vector.Vector<TreasuryTypes.TreasuryAdminActionRecord>;
  };

  // =========================================
  // MIGRATION HELPERS
  // =========================================

  func migrateExchangeType(old : OldExchangeType) : TreasuryTypes.ExchangeType {
    switch (old) {
      case (#ICPSwap) { #ICPSwap };
      case (#KongSwap) { #KongSwap };
    };
  };

  func migrateTradeRecord(old : OldTradeRecord) : TreasuryTypes.TradeRecord {
    {
      tokenSold = old.tokenSold;
      tokenBought = old.tokenBought;
      amountSold = old.amountSold;
      amountBought = old.amountBought;
      exchange = migrateExchangeType(old.exchange);
      timestamp = old.timestamp;
      success = old.success;
      error = old.error;
      slippage = old.slippage;
    };
  };

  func migrateAdminActionType(old : OldTreasuryAdminActionType) : TreasuryTypes.TreasuryAdminActionType {
    switch (old) {
      case (#StartRebalancing) { #StartRebalancing };
      case (#StopRebalancing) { #StopRebalancing };
      case (#ResetRebalanceState) { #ResetRebalanceState };
      case (#UpdateRebalanceConfig(v)) { #UpdateRebalanceConfig(v) };
      case (#CanisterStart) { #CanisterStart };
      case (#CanisterStop) { #CanisterStop };
      case (#PauseTokenManual(v)) { #PauseTokenManual(v) };
      case (#UnpauseToken(v)) { #UnpauseToken(v) };
      case (#ClearAllTradingPauses) { #ClearAllTradingPauses };
      case (#AddTriggerCondition(v)) { #AddTriggerCondition(v) };
      case (#RemoveTriggerCondition(v)) { #RemoveTriggerCondition(v) };
      case (#UpdateTriggerCondition(v)) { #UpdateTriggerCondition(v) };
      case (#SetTriggerConditionActive(v)) { #SetTriggerConditionActive(v) };
      case (#ClearPriceAlerts) { #ClearPriceAlerts };
      case (#AddPortfolioCircuitBreaker(v)) { #AddPortfolioCircuitBreaker(v) };
      case (#RemovePortfolioCircuitBreaker(v)) { #RemovePortfolioCircuitBreaker(v) };
      case (#UpdatePortfolioCircuitBreaker(v)) { #UpdatePortfolioCircuitBreaker(v) };
      case (#SetPortfolioCircuitBreakerActive(v)) { #SetPortfolioCircuitBreakerActive(v) };
      case (#UpdatePausedTokenThreshold(v)) { #UpdatePausedTokenThreshold(v) };
      case (#ClearPortfolioCircuitBreakerLogs) { #ClearPortfolioCircuitBreakerLogs };
      case (#UpdateMaxPortfolioSnapshots(v)) { #UpdateMaxPortfolioSnapshots(v) };
      case (#TakeManualSnapshot) { #TakeManualSnapshot };
      case (#StartPortfolioSnapshots) { #StartPortfolioSnapshots };
      case (#StopPortfolioSnapshots) { #StopPortfolioSnapshots };
      case (#UpdatePortfolioSnapshotInterval(v)) { #UpdatePortfolioSnapshotInterval(v) };
      case (#ExecuteTradingCycle) { #ExecuteTradingCycle };
      case (#SetTestMode(v)) { #SetTestMode(v) };
      case (#ClearSystemLogs) { #ClearSystemLogs };
    };
  };

  func migrateAdminActionRecord(old : OldTreasuryAdminActionRecord) : TreasuryTypes.TreasuryAdminActionRecord {
    {
      id = old.id;
      timestamp = old.timestamp;
      admin = old.admin;
      actionType = migrateAdminActionType(old.actionType);
      reason = old.reason;
      success = old.success;
      errorMessage = old.errorMessage;
    };
  };

  // =========================================
  // MIGRATION FUNCTION
  // =========================================

  public func migrate(oldState : OldState) : NewState {
    let newTrades = Vector.new<TreasuryTypes.TradeRecord>();
    for (trade in Vector.vals(oldState.rebalanceState.lastTrades)) {
      Vector.add(newTrades, migrateTradeRecord(trade));
    };

    let newRebalanceState : TreasuryTypes.RebalanceState = {
      status = oldState.rebalanceState.status;
      config = oldState.rebalanceState.config;
      metrics = oldState.rebalanceState.metrics;
      lastTrades = newTrades;
      priceUpdateTimerId = oldState.rebalanceState.priceUpdateTimerId;
      rebalanceTimerId = oldState.rebalanceState.rebalanceTimerId;
    };

    let newActions = Vector.new<TreasuryTypes.TreasuryAdminActionRecord>();
    for (action in Vector.vals(oldState.treasuryAdminActions)) {
      Vector.add(newActions, migrateAdminActionRecord(action));
    };

    {
      rebalanceState = newRebalanceState;
      treasuryAdminActions = newActions;
    };
  };
};
