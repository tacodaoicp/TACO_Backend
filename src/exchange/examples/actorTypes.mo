import ExTypes "../exchangeTypes";

module {
  public type SwapHop = {
    tokenIn : Text;
    tokenOut : Text;
  };
  public type SplitLeg = {
    amountIn : Nat;
    route : [SwapHop];
    minLegOut : Nat;
  };
  public type TradePrivate = {
    amount_sell : Nat;
    amount_init : Nat;
    token_sell_identifier : Text;
    token_init_identifier : Text;
    trade_done : Nat;
    seller_paid : Nat;
    init_paid : Nat;
    trade_number : Nat;
    SellerPrincipal : Text;
    initPrincipal : Text;
    Fee : Nat;
    seller_paid2 : Nat;
    init_paid2 : Nat;
    RevokeFee : Nat;
    time : Int;
    OCname : Text;
    accesscode : Text;
    filledInit : Nat;
    filledSell : Nat;
    allOrNothing : Bool;
    strictlyOTC : Bool;
  };
  public type OrderbookLevel = {
    price : Float;
    ammAmount : Nat;
    limitAmount : Nat;
    limitOrders : Nat;
  };

  public type OrderbookCombinedResult = {
    bids : [OrderbookLevel];
    asks : [OrderbookLevel];
    ammMidPrice : Float;
    spread : Float;
    ammReserve0 : Nat;
    ammReserve1 : Nat;
  };

  public type ReferralInfo = {
    hasReferrer : Bool;
    referrer : ?Text;
    isFirstTrade : Bool;
    referralEarnings : [(Text, Nat)];
  };

  public type AMMPoolSummary = {
    token0 : Text;
    token1 : Text;
    reserve0 : Nat;
    reserve1 : Nat;
    price0 : Float;
    price1 : Float;
    totalLiquidity : Nat;
  };

  public type DetailedLiquidityPosition = {
    token0 : Text;
    token1 : Text;
    liquidity : Nat;
    token0Amount : Nat;
    token1Amount : Nat;
    shareOfPool : Float;
    fee0 : Nat;
    fee1 : Nat;
  };

  public type ConcentratedPosition = {
    positionId : Nat;
    token0 : Text;
    token1 : Text;
    liquidity : Nat;
    ratioLower : Nat;
    ratioUpper : Nat;
    lastFeeGrowth0 : Nat;
    lastFeeGrowth1 : Nat;
    lastUpdateTime : Int;
  };

  public type TradeHistoryEntry = {
    amount_init : Nat;
    amount_sell : Nat;
    token_init_identifier : Text;
    token_sell_identifier : Text;
    timestamp : Int;
    accesscode : Text;
    counterparty : Text;
  };

  // ═══════════════════════════════════════════════════════════════════════
  // V2 (gross-input + ICRC-2 pull) — typed exchange interface for the test
  // actors and test.mo. Signatures mirror main.mo's V2 API exactly.
  // ═══════════════════════════════════════════════════════════════════════

  public type PullRecordV2 = {
    id : Nat;
    caller : Principal;
    token : Text;
    gross : Nat;
    feeBp : Nat;
    revokeBp : Nat;
    tf : Nat;
    time : Int;
    context : Text;
    note : Text;
  };

  public type QuoteResultV2 = {
    expectedBuyAmount : Nat;
    fee : Nat;
    priceImpact : Float;
    routeDescription : Text;
    canFulfillFully : Bool;
    potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
    hopDetails : [{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }];
  };

  public type MultiQuoteRouteV2 = {
    expectedBuyAmount : Nat;
    fee : Nat;
    priceImpact : Float;
    routeDescription : Text;
    canFulfillFully : Bool;
    potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
    hopDetails : [{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }];
    routeTokens : [Text];
    tradingFeeBps : Nat;
  };

  public type OptimalPlanV2 = {
    expectedBuyAmount : Nat;
    fee : Nat;
    priceImpact : Float;
    canFulfillFully : Bool;
    tradingFeeBps : Nat;
    routeDescription : Text;
    legs : [{
      bp : Nat;
      expectedBuyAmount : Nat;
      route : [SwapHop];
      routeDescription : Text;
    }];
  };

  public type MultiHopQuoteV2 = {
    bestRoute : [SwapHop];
    expectedAmountOut : Nat;
    totalFee : Nat;
    priceImpact : Float;
    hops : Nat;
    routeTokens : [Text];
    hopDetails : [{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }];
  };

  public type ExchangeV2 = actor {
    // fund-moving twins
    swapMultiHopV2 : shared (Text, Text, Nat, [SwapHop], Nat) -> async ExTypes.SwapResult;
    swapSplitRoutesV2 : shared (Text, Text, [SplitLeg], Nat) -> async ExTypes.SwapResult;
    addPositionV2 : shared (Nat, Nat, Text, Text, Bool, Bool, ?Text, Text, Bool, Bool) -> async ExTypes.OrderResult;
    FinishSellV2 : shared (Text, Nat) -> async ExTypes.ActionResult;
    FinishSellBatchV2 : shared ([Text], [Nat], Text, Text) -> async ExTypes.ActionResult;
    addLiquidityV2 : shared (Text, Text, Nat, Nat, ?Bool) -> async ExTypes.AddLiquidityResult;
    addConcentratedLiquidityV2 : shared (Text, Text, Nat, Nat, Nat, Nat) -> async ExTypes.AddConcentratedResult;
    treasurySwapV2 : shared (Text, Text, Nat, Nat) -> async ExTypes.SwapResult;
    // helpers
    grossToNetV2 : shared query (Text, Nat) -> async Nat;
    netToGrossV2 : shared query (Text, Nat) -> async Nat;
    requiredAllowanceV2 : shared query (Text, Nat) -> async Nat;
    quoteDepositV2 : shared query (Text, Nat) -> async { transferFee : Nat; tradingFee : Nat; netSwapped : Nat };
    // pending-pull ops
    getMyPendingPulls : shared query () -> async [PullRecordV2];
    adminListPendingPulls : shared query () -> async [PullRecordV2];
    getV2AllowedTokens : shared query () -> async [Text];
    adminSetV2TokenAllowed : shared (Text, Bool) -> async ExTypes.ActionResult;
    admin_setV2Enabled : shared (Bool) -> async ExTypes.ActionResult;
    getV2Enabled : shared query () -> async Bool;
    adminResolvePendingPull : shared (Nat, Nat, { #ICP; #ICRC12; #ICRC3 }) -> async ExTypes.ActionResult;
    adminDropPendingPull : shared Nat -> async ExTypes.ActionResult;
    adminSweepPendingPulls : shared Nat -> async ExTypes.ActionResult;
    getBlockDoneStatus : shared query (Text, Nat) -> async Bool;
    // V2 quotes
    getExpectedReceiveAmountV2 : shared query (Text, Text, Nat) -> async QuoteResultV2;
    getExpectedReceiveAmountBatchV2 : shared query ([{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }]) -> async [QuoteResultV2];
    getExpectedReceiveAmountBatchMultiV2 : shared query ([{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }], Nat) -> async [{ routes : [MultiQuoteRouteV2] }];
    getExpectedReceiveAmountBatchMultiOptimalV2 : shared query (Text, Text, Nat) -> async OptimalPlanV2;
    simulateSplitRoutesV2 : shared query ([{ amountIn : Nat; route : [SwapHop] }]) -> async { totalOut : Nat; perLegOut : [Nat]; error : Text };
    getExpectedMultiHopAmountV2 : shared query (Text, Text, Nat) -> async MultiHopQuoteV2;
    // V1 quote counterparts missing from examples/exchange.mo's Self —
    // needed for the V2quote(gross) == V1quote(net) equality tests
    simulateSplitRoutes : shared query ([{ amountIn : Nat; route : [SwapHop] }]) -> async { totalOut : Nat; perLegOut : [Nat]; error : Text };
    getExpectedReceiveAmountBatchMulti : shared query ([{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }], Nat) -> async [{ routes : [MultiQuoteRouteV2] }];
    getExpectedReceiveAmountBatchMultiOptimal : shared query (Text, Text, Nat) -> async OptimalPlanV2;
  };

  public type Vote = { tokenIndex : Nat; token : Text; basisPoints : Nat };
  public type TokenAmount = (Text, Nat);
  public type TransactionType = {
    #Burn;
    #Mint;
    #Vouch : Nat;
  };
  public type Transaction = {
    txType : TransactionType;
    sentToDAO : [(Text, Nat)];
    sentFromDAO : [(Text, Nat)];
    when : Nat64;
  };
  public type Self = actor {

    CreatePrivatePosition : shared (Nat, Nat, Nat, Text, Text) -> async Text;
    CreatePublicPosition : shared (Nat, Nat, Nat, Text, Text) -> async Text;
    TransferICPtoExchange : shared (Nat, Nat, Nat) -> async Nat;
    TransferICRCAtoExchange : shared (Nat, Nat, Nat) -> async Nat;
    TransferICRCBtoExchange : shared (Nat, Nat, Nat) -> async Nat;
    TransferICRCCtoExchange : shared (Nat, Nat, Nat) -> async Nat;
    TransferCKUSDCtoExchange : shared (Nat, Nat, Nat) -> async Nat;
    TransferTACOtoDAO : shared Nat -> async Nat64; // Added
    addTACOforMintBurn : shared Nat64 -> async Bool; // Added
    acceptBatchPositions : shared (Nat64, [Text], [Nat], Text, Text) -> async Text;
    acceptPosition : shared (Nat, Text, Nat) -> async Text;
    getICPbalance : shared () -> async Nat;
    getICRCAbalance : shared () -> async Nat;
    getICRCBbalance : shared () -> async Nat;
    getTACObalance : shared () -> async Nat;
    getCKUSDCbalance : shared () -> async Nat;
    CancelPosition : shared Text -> async Text;
    voteOnDAO : shared [Vote] -> async ();
    burnTACO : shared Nat -> async Nat;
    mintTACO : shared (Nat, Bool) -> async Nat;
    vouchInSNSstyleAuctionICRCA : shared Nat -> async Nat;
    vouchInSNSstyleAuctionICRCB : shared Nat -> async Nat;
    vouchInSNSstyleAuctionICRCC : shared Nat -> async Nat;
    vouchInSNSstyleAuctionICP : shared Nat -> async Nat;
    vouchInSNSstyleAuctionCKUSDC : shared Nat -> async Nat;
    recoverUnprocessedTokens : shared [(Text, Nat, Nat)] -> async [(Text, Nat, Bool)];
    claimFees : shared () -> async ();
    addLiquidity : shared (Text, Text, Nat, Nat, Nat, Nat) -> async Text;
    removeLiquidity : shared (Text, Text, Nat) -> async Text;
    getUserTrades : shared () -> async [TradePrivate];
    getUserPreviousTrades : shared (Text, Text) -> async [{
      amount_init : Nat;
      amount_sell : Nat;
      init_principal : Text;
      sell_principal : Text;
      accesscode : Text;
      token_init_identifier : Text;
      timestamp : Int;
      strictlyOTC : Bool;
      allOrNothing : Bool;

    }];
    CreatePublicPositionOTC : shared (Nat, Nat, Nat, Text, Text) -> async Text;
    createSNSstyleAuction : shared (Nat, Nat) -> async Nat;
    getDAOTransactions : shared () -> async ?{
      transactions : [{
        txType : { #Burn; #Mint; #Vouch : Nat64 };
        sentToDAO : [(Text, Nat)];
        sentFromDAO : [(Text, Nat)];
        when : Nat64;
      }];
      totalTransactions : Nat;
    };
    recoverUnprocessedTokensDAO : shared [(Text, Nat, Nat)] -> async [(Text, Nat, Bool)];
    swapMultiHop : shared (Text, Text, Nat, [SwapHop], Nat, Nat) -> async Text;
    swapSplitRoutes : shared (Text, Text, [SplitLeg], Nat, Nat) -> async Text;
    adminAnalyzeRouteEfficiency : shared (Text, Nat, Nat) -> async [{
      route : [{ tokenIn : Text; tokenOut : Text }];
      outputAmount : Nat;
      efficiency : Int;
      efficiencyBps : Int;
      hopDetails : [{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }];
    }];
    adminExecuteRouteStrategy : shared (Nat, [{ tokenIn : Text; tokenOut : Text }], Nat, Nat) -> async Text;
    claimLPFees : shared (Text, Text) -> async Text;
    getUserLiquidityDetailed : shared () -> async [DetailedLiquidityPosition];
    addConcentratedLiquidity : shared (Text, Text, Nat, Nat, Nat, Nat, Nat, Nat) -> async Text;
    removeConcentratedLiquidity : shared (Text, Text, Nat, Nat) -> async Text;
    getUserConcentratedPositions : shared () -> async [ConcentratedPosition];

    // ── V2 additions (icrc2 approve helpers + thin V2 wrappers) ──
    ApproveICPforExchange : shared (Nat, ?Nat64) -> async Nat;
    ApproveICRCAforExchange : shared (Nat, ?Nat64) -> async Nat;
    ApproveICRCBforExchange : shared (Nat, ?Nat64) -> async Nat;
    getAllowanceICP : shared () -> async Nat;
    getAllowanceICRCA : shared () -> async Nat;
    getAllowanceICRCB : shared () -> async Nat;
    RevokeApprovalICP : shared () -> async Nat;
    RevokeApprovalICRCA : shared () -> async Nat;
    RevokeApprovalICRCB : shared () -> async Nat;
    getMyPendingPullsCount : shared () -> async Nat;
    swapMultiHopV2 : shared (Text, Text, Nat, [SwapHop], Nat) -> async Text;
    swapSplitRoutesV2 : shared (Text, Text, [SplitLeg], Nat) -> async Text;
    CreatePrivatePositionV2 : shared (Nat, Nat, Text, Text) -> async Text;
    CreatePublicPositionV2 : shared (Nat, Nat, Text, Text) -> async Text;
    CreatePublicPositionOTCV2 : shared (Nat, Nat, Text, Text) -> async Text;
    acceptPositionV2 : shared (Text, Nat) -> async Text;
    acceptBatchPositionsV2 : shared ([Text], [Nat], Text, Text) -> async Text;
    addLiquidityV2 : shared (Text, Text, Nat, Nat) -> async Text;
    addConcentratedLiquidityV2 : shared (Text, Text, Nat, Nat, Nat, Nat) -> async Text;
    treasurySwapV2 : shared (Text, Text, Nat, Nat) -> async Text;
    recoverBlock : shared (Text, Nat) -> async Bool;
    // PULL-RACE concurrency probes (Test122)
    raceV2SwapVsRecover : shared (Text, Text, Nat, Nat, Nat) -> async { swap : Text; recovered : Bool; tries : Nat };
    raceV2SwapVsV1Swap : shared (Text, Text, Nat, Nat, Nat, Nat) -> async { swapV2 : Text; swapV1 : Text; tries : Nat };
  };
};
