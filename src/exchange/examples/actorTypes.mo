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
    claimLPFees : shared (Text, Text) -> async Text;
    getUserLiquidityDetailed : shared () -> async [DetailedLiquidityPosition];
    addConcentratedLiquidity : shared (Text, Text, Nat, Nat, Nat, Nat, Nat, Nat) -> async Text;
    removeConcentratedLiquidity : shared (Text, Text, Nat, Nat) -> async Text;
    getUserConcentratedPositions : shared () -> async [ConcentratedPosition];
  };
};
