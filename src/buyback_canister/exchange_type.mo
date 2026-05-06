// Buyback-canister-side actor type for the OTC_backend / exchange canister.
//
// Mirrors the exact types declared in src/exchange/exchangeTypes.mo and
// src/exchange/main.mo, narrowed to only the methods the buyback canister calls:
//   - adminAnalyzeRouteEfficiency  (query, free per-call 40B instruction budget)
//   - adminExecuteRouteStrategy    (update; OTC-orders-first matching via
//                                   orderPairing, principal-protected via
//                                   slippage refund at exchange/main.mo:15677)
//
// Field names and shapes must match the exchange's exact return types verbatim
// — Candid is structural, mismatches show up as decode errors at runtime.

module {

  public type SwapHop = {
    tokenIn : Text;
    tokenOut : Text;
  };

  public type HopDetail = {
    tokenIn : Text;
    tokenOut : Text;
    amountIn : Nat;
    amountOut : Nat;
    fee : Nat;
    priceImpact : Float;
  };

  public type RouteEfficiency = {
    route : [SwapHop];
    outputAmount : Nat;
    efficiency : Int;
    efficiencyBps : Int;
    hopDetails : [HopDetail];
  };

  // Mirrors exchange/exchangeTypes.mo:10-24
  public type ExchangeError = {
    #NotAuthorized;
    #Banned;
    #InvalidInput : Text;
    #TokenNotAccepted : Text;
    #TokenPaused : Text;
    #InsufficientFunds : Text;
    #PoolNotFound : Text;
    #SlippageExceeded : { expected : Nat; got : Nat };
    #RouteFailed : { hop : Nat; reason : Text };
    #OrderNotFound : Text;
    #ExchangeFrozen;
    #TransferFailed : Text;
    #SystemError : Text;
  };

  // Mirrors exchange/exchangeTypes.mo:30-41
  public type SwapOk = {
    amountIn : Nat;
    amountOut : Nat;
    tokenIn : Text;
    tokenOut : Text;
    route : [Text];
    fee : Nat;
    swapId : Nat;
    hops : Nat;
    firstHopOrderbookMatch : Bool;
    lastHopAMMOnly : Bool;
  };

  // Mirrors exchange/exchangeTypes.mo:43-46
  public type SwapResult = {
    #Ok : SwapOk;
    #Err : ExchangeError;
  };

  // Single-call result for adminFindOptimalArb — best (amount, route) found by
  // the exchange's internal ternary search.
  public type OptimalArb = {
    amount : Nat;
    route : [SwapHop];
    outputAmount : Nat;
    efficiency : Int;
    efficiencyBps : Int;
    hopDetails : [HopDetail];
    probesRun : Nat;
  };

  public type Exchange = actor {
    // Path-finding query — fresh 40B instruction budget per inter-canister call.
    adminAnalyzeRouteEfficiency : shared query (
      token : Text,
      sampleSize : Nat,
      depth : Nat,
    ) -> async [RouteEfficiency];

    // Single-call ternary-search optimal arb finder. Replaces N separate
    // adminAnalyzeRouteEfficiency calls + external ternary. Snapshot-consistent
    // across probes (no pool drift mid-search).
    adminFindOptimalArb : shared query (
      token : Text,
      minSample : Nat,
      maxSample : Nat,
      depth : Nat,
      probes : Nat,
      tolerancePct : Nat,
    ) -> async ?OptimalArb;

    // Route execution. Each call requires its own ICRC-1 deposit block
    // (BlocksDone dedup at exchange/main.mo:15552).
    // OTC-first matching via orderPairing. Slippage refund returns principal
    // if realized < minOutput.
    adminExecuteRouteStrategy : shared (
      amount : Nat,
      route : [SwapHop],
      minOutput : Nat,
      block : Nat,
    ) -> async SwapResult;

    // Flash arbitrage — zero-capital circular arb. Buyback-canister-only.
    // Lends notional from feescollectedDAO[startToken] (or runs in phantom mode
    // if insufficient), executes the route, traps if grossProfit < minProfit + costs,
    // sends netProfit to caller via single ICRC-1 transfer. Profit guarantee is
    // enforced by Debug.trap reverting all state on insufficient profit.
    // Mirrors adminExecuteRouteStrategy fee accounting exactly — same drift profile.
    adminFlashArb : shared (
      notional : Nat,
      route : [SwapHop],
      minProfit : Nat,
    ) -> async {
      #Ok : {
        notional : Nat;
        realized : Nat;
        grossProfit : Nat;
        netProfit : Nat;
        tradingFee : Nat;
        inputTfees : Nat;
        transferFee : Nat;
        hops : Nat;
        swapId : Nat;
        capitalSource : { #lent; #phantom };
      };
      #Err : Text;
    };
  };
}
