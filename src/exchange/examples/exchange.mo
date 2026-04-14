import ExTypes "../exchangeTypes";

module {
  public type Current_Liquidity = [[{
    time : Int;
    amount_init : Nat;
    amount_sell : Nat;
    init_principal : Text;
    accesscode : Text;
    Fee : Nat;
    RevokeFee : Nat;
    token_init_identifier : Text;
    OCname : Text;
  }]];
  public type Pool_History = [[{
    time : Int;
    amount_init : Nat;
    amount_sell : Nat;
    init_principal : Text;
    sell_principal : Text;
    accesscode : Text;
    token_init_identifier : Text;
  }]];
  public type Poolcanisters = (Text, Text);
  public type Ratio = {
    #Max;
    #Zero;
    #Value : Nat;
  };

  type LastTradedPrice = (
    Float
  );
  type pool = {
    pool_canister : [(Text, Text)];
    asset_names : [(Text, Text)];
    asset_symbols : [(Text, Text)];
    asset_decimals : [(Nat8, Nat8)];
    asset_transferfees : [(Nat, Nat)];
    asset_minimum_amount : [(Nat, Nat)];
    last_traded_price : [Float];
    price_day_before : [Float];
    volume_24h : [Nat];
    amm_reserve0 : [Nat];
    amm_reserve1 : [Nat];
  };

  type TokenInfo = {
    address : Text;
    name : Text;
    symbol : Text;
    decimals : Nat;
    transfer_fee : Nat;
    minimum_amount : Nat;
    asset_type : { #ICP; #ICRC12; #ICRC3 };
  };

  type TradePosition = {
    amount_sell : Nat;
    amount_init : Nat;
    token_sell_identifier : Text;
    token_init_identifier : Text;
    trade_number : Nat;
    Fee : Nat;
    trade_done : Nat;
    strictlyOTC : Bool;
    allOrNothing : Bool;
    OCname : Text;
    time : Int;
    filledInit : Nat;
    filledSell : Nat;
    initPrincipal : Text;
  };

  type OrderbookLevel = {
    price : Float;
    ammAmount : Nat;
    limitAmount : Nat;
    limitOrders : Nat;
  };

  type OrderbookCombinedResult = {
    bids : [OrderbookLevel];
    asks : [OrderbookLevel];
    ammMidPrice : Float;
    spread : Float;
    ammReserve0 : Nat;
    ammReserve1 : Nat;
  };

  type ReferralInfo = {
    hasReferrer : Bool;
    referrer : ?Text;
    isFirstTrade : Bool;
    referralEarnings : [(Text, Nat)];
  };

  type AMMPoolSummary = {
    token0 : Text;
    token1 : Text;
    reserve0 : Nat;
    reserve1 : Nat;
    price0 : Float;
    price1 : Float;
    totalLiquidity : Nat;
  };

  type TradeHistoryEntry = {
    amount_init : Nat;
    amount_sell : Nat;
    token_init_identifier : Text;
    token_sell_identifier : Text;
    timestamp : Int;
    accesscode : Text;
    counterparty : Text;
  };

  type Time = Int;

  type TimeFrame = {
    #fivemin;
    #hour;
    #fourHours;
    #day;
    #week;
  };

  type KlineData = {
    timestamp : Int;
    open : Float;
    high : Float;
    low : Float;
    close : Float;
    volume : Nat;
  };

  type foreignPoolData = {
    pool : (Text, Text);
    forward : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
    backward : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
  };

  type TradeData = {
    identifier : Text;
    amountBuy : Nat;
    amountSell : Nat;
    ICPPrice : Nat;
    decimals : Nat;
    block : Nat64;
    transferFee : Nat;
  };

  type ProcessedTrade = {
    identifier : Text;
    amountBought : Nat;
    amountSold : Nat;
  };

  type BatchProcessResult = {
    execMessage : Text;
    processedTrades : [ProcessedTrade];
    accesscodes : [{
      poolId : Text;
      accesscode : Text;
      amountInit : Nat;
      amountSell : Nat;
      fee : Nat;
      revokeFee : Nat;
    }];
  };

  type PositionData = {
    accesscode : Text;
    ICPprice : (Nat, Nat);
    decimals : (Nat, Nat);
  };

  type RecalibratedPosition = {
    poolId : (Text, Text);
    accesscode : Text;
    amountInit : Nat;
    amountSell : Nat;
    fee : Nat;
    revokeFee : Nat;
  };

  type PoolQuery = {
    pool : (Text, Text);
    forwardCursor : ?Ratio;
    backwardCursor : ?Ratio;
  };

  type ForeignPoolLiquidity = {
    pool : (Text, Text);
    liquidity : {
      forward : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
      backward : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
    };
    forwardCursor : Ratio;
    backwardCursor : Ratio;
  };

  type ForeignPoolsResponse = {
    pools : [ForeignPoolLiquidity];
    nextPoolCursor : ?PoolQuery;
  };

  type DetailedLiquidityPosition = {
    token0 : Text;
    token1 : Text;
    liquidity : Nat;
    token0Amount : Nat;
    token1Amount : Nat;
    shareOfPool : Float;
    fee0 : Nat;
    fee1 : Nat;
  };

  public type HopDetail = {
    tokenIn : Text;
    tokenOut : Text;
    amountIn : Nat;
    amountOut : Nat;
    fee : Nat;
    priceImpact : Float;
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

  type TradePrivate = {
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
    filledInit : Nat;
    filledSell : Nat;
    allOrNothing : Bool;
    strictlyOTC : Bool;
  };

  type TradePrivate2 = {
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
    filledInit : Nat;
    filledSell : Nat;
    allOrNothing : Bool;
    accesscode : Text;
    strictlyOTC : Bool;
  };

  public type SwapHop = {
    tokenIn : Text;
    tokenOut : Text;
  };

  public type SplitLeg = {
    amountIn : Nat;
    route : [SwapHop];
    minLegOut : Nat;
  };

  public type Self = actor {
    ChangeRevokefees : shared Nat -> async ();
    ChangeTradingfees : shared Nat -> async ();
    ChangeReferralFees : shared Nat -> async ();
    FinishSell : shared (Nat64, Text, Nat) -> async ExTypes.ActionResult;
    FinishSellBatch : shared (Nat64, [Text], [Nat], Text, Text) -> async ExTypes.ActionResult;
    FinishSellBatchDAO : shared ([TradeData], Bool, [Nat]) -> async ?BatchProcessResult;
    FixStuckTX : shared Text -> async ExTypes.ActionResult;
    parameterManagement : shared {
      deleteFromDayBan : ?[Text];
      deleteFromAllTimeBan : ?[Text];
      addToAllTimeBan : ?[Text];
      changeAllowedCalls : ?Nat;
      changeallowedSilentWarnings : ?Nat;
      addAllowedCanisters : ?[Text];
      deleteAllowedCanisters : ?[Text];
      treasury_principal : ?Text;
    } -> async ();
    Freeze : shared () -> async ();
    revokeTrade : shared (Text, { #DAO : [Text]; #Seller; #Initiator }) -> async ExTypes.RevokeResult;
    addAcceptedToken : shared ({ #Add; #Remove; #Opposite }, Text, Nat, { #ICP; #ICRC12; #ICRC3 }) -> async ExTypes.ActionResult;
    addTimer : shared () -> async ();
    addPosition : shared (Nat, Nat, Nat, Text, Text, Bool, Bool, ?Text, Text, Bool, Bool) -> async ExTypes.OrderResult;
    changeOwner2 : shared Principal -> async ();
    changeOwner3 : shared Principal -> async ();
    collectFees : shared () -> async ExTypes.ActionResult;
    exchangeInfo : shared query () -> async ?pool;
    getAcceptedTokens : shared query () -> async ?[Text];
    getAcceptedTokensInfo : shared query () -> async ?[TokenInfo];
    getAllTradesPublic : shared query () -> async ?([Text], [TradePrivate]);
    getAllTradesPrivateCostly : shared query () -> async ?([Text], [TradePrivate]);
    getCurrentLiquidity : shared query (Text, Text, { #forward; #backward }, Nat, ?Ratio) -> async {
      liquidity : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
      nextCursor : Ratio;
    };
    getPrivateTrade : shared query Text -> async ?TradePosition;
    getUserTrades : shared query () -> async [TradePrivate2];
    getUserPreviousTrades : shared query (Text, Text) -> async [{
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
    getTokenUSDPrices : shared query (Float, Float) -> async ?{
      error : Bool;
      data : [(Text, { address : Text; priceUSD : Float; timeLastValidUpdate : Int })];
    };
    getPoolHistory : shared query (Text, Text, Nat) -> async [(Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
    getKlineData : shared query (Text, Text, TimeFrame, Bool) -> async [KlineData];
    recoverWronglysent : shared (Text, Nat, { #ICP; #ICRC12; #ICRC3 }) -> async Bool;
    get_cycles : shared query () -> async Nat;
    hmFee : shared query () -> async Nat;
    hmRevokeFee : shared query () -> async Nat;
    hmRefFee : shared query () -> async Nat;
    p2a : shared query () -> async Text;
    p2acannister : shared query () -> async Text;
    p2athird : shared Text -> async Text;
    pauseToken : shared Text -> async ();
    recalibrateDAOpositions : shared [PositionData] -> async [RecalibratedPosition];
    retrieveFundsDao : shared [(Text, Nat64)] -> async ();
    returncontractprincipal : shared query () -> async Text;
    sendDAOInfo : shared query () -> async [(Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text })];
    setTest : shared Bool -> async ();
    resetAllState : shared () -> async Text;
    checkDiffs : shared (Bool, Bool) -> async ?(Bool, [(Int, Text)], [[{ accessCode : Text; identifier : Text; poolCanister : (Text, Text) }]]);
    getCurrentLiquidityForeignPools : shared query (Nat, ?[PoolQuery], Bool) -> async ForeignPoolsResponse;
    checkFeesReferrer : shared query () -> async [(Text, Nat)];
    claimFeesReferrer : shared () -> async [(Text, Nat)];
    addLiquidity : shared (Text, Text, Nat, Nat, Nat, Nat) -> async ExTypes.AddLiquidityResult;
    removeLiquidity : shared (Text, Text, Nat) -> async ExTypes.RemoveLiquidityResult;
    getPausedTokens : shared query () -> async ?[Text];
    getAMMPoolInfo : shared query (Text, Text) -> async ?{
      token0 : Text;
      token1 : Text;
      reserve0 : Nat;
      reserve1 : Nat;
      price0 : Float;
      price1 : Float;
    };
    getUserLiquidityDetailed : shared query () -> async [DetailedLiquidityPosition];
    getExpectedReceiveAmount : shared query (Text, Text, Nat) -> async {
      expectedBuyAmount : Nat;
      fee : Nat;
      priceImpact : Float;
      routeDescription : Text;
      canFulfillFully : Bool;
      potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
      hopDetails : [HopDetail];
    };
    getExpectedReceiveAmountBatch : shared query ([{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }]) -> async [{
      expectedBuyAmount : Nat; fee : Nat; priceImpact : Float;
      routeDescription : Text; canFulfillFully : Bool;
      potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
      hopDetails : [HopDetail];
    }];
    getLogging : shared query ({ #FinishSellBatchDAO; #addAcceptedToken }, Nat) -> async [(Nat, Text)];
    getExpectedMultiHopAmount : shared query (Text, Text, Nat) -> async {
      bestRoute : [SwapHop];
      expectedAmountOut : Nat;
      totalFee : Nat;
      priceImpact : Float;
      hops : Nat;
      routeTokens : [Text];
      hopDetails : [HopDetail];
    };
    swapMultiHop : shared (Text, Text, Nat, [SwapHop], Nat, Nat) -> async ExTypes.SwapResult;
    swapSplitRoutes : shared (Text, Text, [SplitLeg], Nat, Nat) -> async ExTypes.SwapResult;
    adminAnalyzeRouteEfficiency : shared query (Text, Nat, Nat) -> async [{
      route : [SwapHop];
      outputAmount : Nat;
      efficiency : Int;
      efficiencyBps : Int;
      hopDetails : [HopDetail];
    }];
    adminExecuteRouteStrategy : shared (Nat, [SwapHop], Nat, Nat) -> async ExTypes.SwapResult;
    claimLPFees : shared (Text, Text) -> async ExTypes.ClaimFeesResult;
    addConcentratedLiquidity : shared (Text, Text, Nat, Nat, Nat, Nat, Nat, Nat) -> async ExTypes.AddConcentratedResult;
    removeConcentratedLiquidity : shared (Text, Text, Nat, Nat) -> async ExTypes.RemoveConcentratedResult;
    getUserConcentratedPositions : shared query () -> async [ConcentratedPosition];
    getOrderbookCombined : shared query (Text, Text, Nat, Nat) -> async OrderbookCombinedResult;
    getUserReferralInfo : shared query () -> async ReferralInfo;
    getAllAMMPools : shared query () -> async [AMMPoolSummary];
    getUserTradeHistory : shared query Nat -> async [TradeHistoryEntry];
    getDAOLiquiditySnapshot : shared query () -> async {
      positions : [{ token0 : Text; token1 : Text; liquidity : Nat; token0Amount : Nat; token1Amount : Nat; shareOfPool : Float; fee0 : Nat; fee1 : Nat }];
      pools : [{ token0 : Text; token1 : Text; reserve0 : Nat; reserve1 : Nat; totalLiquidity : Nat; price0 : Float; price1 : Float }];
    };
    batchClaimAllFees : shared () -> async [{ token0 : Text; token1 : Text; fees0 : Nat; fees1 : Nat; transferred0 : Nat; transferred1 : Nat }];
    batchAdjustLiquidity : shared ([{ token0 : Text; token1 : Text; action : { #Remove : { liquidityAmount : Nat } } }]) -> async [{ token0 : Text; token1 : Text; success : Bool; result : Text }];
    addLiquidityDAO : shared (Text, Text, Nat, Nat, Nat, Nat) -> async ExTypes.AddLiquidityResult;
    getDAOLPPerformance : shared query () -> async [{ token0 : Text; token1 : Text; currentValue0 : Nat; currentValue1 : Nat; totalFeesEarned0 : Nat; totalFeesEarned1 : Nat; shareOfPool : Float; poolVolume24h : Nat }];
  };
};
