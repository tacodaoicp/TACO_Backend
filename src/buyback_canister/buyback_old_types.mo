// Old (pre-flash-arb) shape of BuybackCycleRecord and friends, used solely by
// the explicit stable migration in buyback_canister.mo to discard the empty
// V1 `buybackHistory` Vector and initialize a fresh `buybackHistoryV2` Vector
// with the new BuybackArbIteration shape (which gained 5 optional fields for
// flash arb auditability).
import Principal "mo:base/Principal";
import BuybackTypes "./buyback_canister_types";

module {
  public type BuybackArbIteration = {
    iteration : Nat;
    startToken : Principal;
    amountIn : Nat;
    expectedProfit : Nat;
    minOutput : Nat;
    amountOut : Nat;
    profit : Nat;
    profitICP : Nat;
    routeHops : Nat;
    result : BuybackTypes.BuybackArbIterationResult;
  };

  public type BuybackArbRun = {
    phase : { #pre; #post };
    iterations : [BuybackArbIteration];
    totalProfitPerToken : [(Principal, Nat)];
    status : BuybackTypes.BuybackArbStatus;
  };

  public type BuybackCycleRecord = {
    id : Nat;
    startedAt : Int;
    finishedAt : Int;
    preArb : ?BuybackArbRun;
    claimedFromVault : [(Principal, Nat)];
    swapResults : [BuybackTypes.BuybackSwapResult];
    totalTacoBurned : Nat;
    burnBlockIndex : ?Nat;
    postArb : ?BuybackArbRun;
    status : BuybackTypes.BuybackCycleStatus;
  };
}
