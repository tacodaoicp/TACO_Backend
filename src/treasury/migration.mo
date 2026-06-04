import Vector "mo:vector";
import TreasuryTypes "./treasury_types";

module {
  // =========================================
  // OLD TYPES (currently deployed on-chain)
  // 3-case ExchangeType WITHOUT #Neutrinite.
  // The previous (#TACO, 2->3) migration has already been deployed, so the on-chain
  // ExchangeType is now {#ICPSwap; #KongSwap; #TACO}. This migration widens it 3->4 by
  // adding #Neutrinite.
  //
  // Only rebalanceState carries ExchangeType (inside lastTrades : Vector<TradeRecord>),
  // so this migration touches rebalanceState ONLY. All other stable vars (incl.
  // treasuryAdminActions) pass through automatically and are NOT in the migration record.
  // The upgrade dry-run (--wasm-memory-persistence keep) is the hard M0170 gate.
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

  // RebalanceState shape, but lastTrades holds OldTradeRecord.
  // config / metrics contain NO ExchangeType references, so they reuse the current
  // TreasuryTypes types unchanged.
  public type OldRebalanceState = {
    status : TreasuryTypes.RebalanceStatus;
    config : TreasuryTypes.RebalanceConfig;
    metrics : TreasuryTypes.RebalanceMetrics;
    lastTrades : Vector.Vector<OldTradeRecord>;
    priceUpdateTimerId : ?Nat;
    rebalanceTimerId : ?Nat;
  };

  // =========================================
  // STATE WRAPPERS
  // A partial migration record lists ONLY the stable vars that change.
  // =========================================

  public type OldState = {
    rebalanceState : OldRebalanceState;
  };

  public type NewState = {
    rebalanceState : TreasuryTypes.RebalanceState;
  };

  // =========================================
  // MIGRATION HELPERS
  // =========================================

  func migrateExchangeType(old : OldExchangeType) : TreasuryTypes.ExchangeType {
    switch (old) {
      case (#ICPSwap) { #ICPSwap };
      case (#KongSwap) { #KongSwap };
      case (#TACO) { #TACO };
      // Old data never contains #Neutrinite; the variant is only widened.
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

  // =========================================
  // MIGRATION FUNCTION
  // Output type is EXACTLY { rebalanceState : TreasuryTypes.RebalanceState }, matching the
  // actor's declared stable shape for the single changed var.
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

    {
      rebalanceState = newRebalanceState;
    };
  };
};
