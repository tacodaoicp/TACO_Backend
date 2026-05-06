// Types for the buyback canister.
//
// Moved from treasury_types.mo when the buyback flow was extracted from treasury
// into its own dedicated canister.
//
// See plan: ~/.claude/plans/we-have-the-nachos-purring-puffin.md

import Principal "mo:base/Principal";

module {

  public type BuybackConfig = {
    enabled : Bool;                          // master gate; cannot be true outside production
    intervalNS : Nat;                        // default 86_400_000_000_000 (1 day)
    minTokenValueICP : Nat;                  // dust threshold; default 50_000_000 (~$5)
    sweepVault : Bool;                       // V1 default true
    sweepExchange : Bool;                    // V3 future; default false

    // Arb (V2). Inert when runArb=false OR OTC_BACKEND_ID == aaaaa-aa.
    runArb : Bool;                           // default false until exchange ships
    arbWorkingCapitalICP : Nat;              // floor in buyback canister default
    arbWorkingCapitalUSDC : Nat;
    arbDepth : Nat;                          // hops per route, 2-6, default 4
    arbMinSampleAmount : [(Principal, Nat)]; // per-token gas-floor
    arbTernaryProbes : Nat;                  // probes per (token, iteration); default 6
    arbTernaryTolerancePct : Nat;            // stop ternary when range < % of balance
    arbMinProfitICP : Nat;                   // stop iterating below this absolute ICP profit
    arbMaxIterations : Nat;                  // safety cap; default 20
    arbMaxRoutesPerAnalysis : Nat;           // top-N from each analyze considered
    arbSettlementTimeoutMs : Nat;            // poll timeout for output settlement
    // Slippage tolerance applied to ARB ITERATIONS, expressed as bps of expected profit
    // that may be lost to slippage. The principal is ALWAYS preserved (minOutput >=
    // amountIn unconditionally); this knob controls how much of the expected profit
    // can erode before the route is refunded.
    //   - 0 (0%):     require full expected profit; tightest, most refunds
    //   - 5000 (50%): require half the expected profit; moderate (default)
    //   - 10000 (100%): accept any positive profit including zero — equivalent to the
    //                   pure principal-protection semantics
    // minOutput = amountIn + (expectedProfit * (10000 - arbProfitSlippageBps) / 10000)
    // Always satisfies minOutput >= amountIn for non-negative slippageBps and non-negative
    // expectedProfit, so principal is preserved at the same time.
    arbProfitSlippageBps : Nat;

    // Token-details cache freshness threshold
    tokenDetailsStaleAfterNS : Nat;          // sync-from-treasury if cache older than this
  };

  public type BuybackConfigUpdate = {
    enabled : ?Bool;
    intervalNS : ?Nat;
    minTokenValueICP : ?Nat;
    sweepVault : ?Bool;
    sweepExchange : ?Bool;
    runArb : ?Bool;
    arbWorkingCapitalICP : ?Nat;
    arbWorkingCapitalUSDC : ?Nat;
    arbDepth : ?Nat;
    arbMinSampleAmount : ?[(Principal, Nat)];
    arbTernaryProbes : ?Nat;
    arbTernaryTolerancePct : ?Nat;
    arbMinProfitICP : ?Nat;
    arbMaxIterations : ?Nat;
    arbMaxRoutesPerAnalysis : ?Nat;
    arbSettlementTimeoutMs : ?Nat;
    arbProfitSlippageBps : ?Nat;
    tokenDetailsStaleAfterNS : ?Nat;
  };

  public type BuybackArbIterationResult = { #ok; #refunded; #failed : Text };

  public type BuybackArbIteration = {
    iteration : Nat;
    startToken : Principal;
    amountIn : Nat;
    expectedProfit : Nat;    // expected profit per the analyzer at iteration start
    minOutput : Nat;         // amountIn + (expectedProfit * (10000 - arbProfitSlippageBps) / 10000)
                              //   — i.e. the floor we required; below this, exchange refunds.
                              //   Always >= amountIn (principal is unconditionally preserved).
                              //   For flash arb, this field carries the minProfit floor (not minOutput)
                              //   since flash arb has no input deposit.
    amountOut : Nat;          // For flash arb: realized (notional + grossProfit before fees).
    profit : Nat;            // realized: amountOut - amountIn (always >= 0).
                              //   For flash arb: netProfit (after exchange fees + ledger fee).
    profitICP : Nat;         // ICP-equivalent for cross-token comparison
    routeHops : Nat;
    result : BuybackArbIterationResult;
    // ── Flash arb audit fields (?null for legacy deposit-flow records) ──
    grossProfit : ?Nat;      // realized − notional (before fees)
    tradingFee : ?Nat;       // calculateFee(notional, ...) + firstHopProtocolFee booked to feescollectedDAO
    inputTfees : ?Nat;       // returnTfees(startToken) absorbed (0 if first hop hit orderbook)
    transferFee : ?Nat;      // ledger fee on the outgoing netProfit transfer
    capitalSource : ?{ #lent; #phantom }; // exchange's working-capital decision
  };

  public type BuybackArbStatus = {
    #completed;
    #stoppedNoProfit;
    #stoppedMaxIterations;
    #failed : Text;
    #skipped : Text;
  };

  public type BuybackArbRun = {
    phase : { #pre; #post };
    iterations : [BuybackArbIteration];
    totalProfitPerToken : [(Principal, Nat)];
    status : BuybackArbStatus;
  };

  public type BuybackSwapResult = {
    sellToken : Principal;
    amountIn : Nat;
    dex : Text;  // "Kong" | "ICPSwap" | "TACO" | "skip-dust" | "skip-no-route"
                 //   | "skip-floor" | "TACO-direct" | "no-route"
    tacoOut : Nat;
    error : ?Text;
  };

  public type BuybackCycleStatus = {
    #ok;
    #partial;
    #skipped_environment;
    #skipped_disabled;
    #skipped_already_running;
    #failed : Text;
  };

  public type BuybackCycleRecord = {
    id : Nat;
    startedAt : Int;
    finishedAt : Int;
    preArb : ?BuybackArbRun;          // null before V2 active
    claimedFromVault : [(Principal, Nat)];
    swapResults : [BuybackSwapResult];
    totalTacoBurned : Nat;
    burnBlockIndex : ?Nat;
    postArb : ?BuybackArbRun;          // null before V2 active
    status : BuybackCycleStatus;
  };

  // Subset of treasury's TokenDetails — what we actually need locally for swap
  // dispatch and dust filtering. Refreshed periodically via syncTokenDetailsFromTreasury.
  public type CachedTokenDetails = {
    tokenSymbol : Text;
    tokenDecimals : Nat;
    tokenTransferFee : Nat;
    priceInICP : Nat;
  };

  // Aggregated stats over the entire buybackHistory.
  public type BuybackStats = {
    totalCycles : Nat;
    cyclesOk : Nat;
    cyclesPartial : Nat;
    cyclesFailed : Nat;
    cyclesSkipped : Nat;        // any #skipped_* status
    totalTacoBurned : Nat;       // mirrors stable totalBurned counter
    totalArbProfitPerToken : [(Principal, Nat)];
    totalArbIterations : Nat;
    arbIterationsOk : Nat;
    arbIterationsRefunded : Nat;
    arbIterationsFailed : Nat;
    refundRateBps : Nat;          // (refunded / total iterations) * 10000
    avgCycleWallClockNs : Int;    // 0 if no cycles
    avgRealizedToExpectedProfitBps : Nat; // (realized / expected) * 10000, ok-only
  };

  // Single-pane operator view. Most fields are stable-state reads (cheap); live
  // balance reads require inter-canister calls so getHealth is shared, not query.
  public type BuybackHealth = {
    environment : { #Local; #Staging; #Production };
    enabled : Bool;
    runArb : Bool;
    exchangeOnProd : Bool;        // OTC_BACKEND_ID != aaaaa-aa
    cycleInProgress : Bool;
    timerActive : Bool;            // buybackTimerId != 0
    liveBalances : { icp : Nat; usdc : Nat; taco : Nat };
    pendingTacoToBurn : Nat;       // = liveBalances.taco; surfaced explicitly for clarity
    lastCycleAt : ?Int;            // finishedAt of most recent cycle
    lastCycleStatus : ?BuybackCycleStatus;
    totalBurned : Nat;
    cachedTokenCount : Nat;
    lastTokenSyncAt : Int;         // 0 if never synced
  };

}
