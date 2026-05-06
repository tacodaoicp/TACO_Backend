// Exchange Result Types — structured return types for all exchange functions
// Convention: #Ok / #Err (capitalized), matching existing codebase patterns

module {

  // ═══════════════════════════════════════════════════════════════
  // ERROR TYPE — 13 categories covering all error return paths
  // ═══════════════════════════════════════════════════════════════

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

  // ═══════════════════════════════════════════════════════════════
  // SWAP RESULTS — swapMultiHop, swapSplitRoutes, treasurySwap, adminExecuteRouteStrategy
  // ═══════════════════════════════════════════════════════════════

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

  public type SwapResult = {
    #Ok : SwapOk;
    #Err : ExchangeError;
  };

  // ═══════════════════════════════════════════════════════════════
  // ORDER RESULTS — addPosition
  // ═══════════════════════════════════════════════════════════════

  public type OrderOk = {
    accessCode : Text;
    tokenIn : Text;
    tokenOut : Text;
    amountIn : Nat;
    filled : Nat;
    remaining : Nat;
    buyAmountReceived : Nat;
    swapId : ?Nat;
    isPublic : Bool;
  };

  public type OrderResult = {
    #Ok : OrderOk;
    #Err : ExchangeError;
  };

  // ═══════════════════════════════════════════════════════════════
  // LIQUIDITY RESULTS
  // ═══════════════════════════════════════════════════════════════

  public type AddLiquidityOk = {
    liquidityMinted : Nat;
    token0 : Text;
    token1 : Text;
    amount0Used : Nat;
    amount1Used : Nat;
    refund0 : Nat;
    refund1 : Nat;
  };

  public type AddLiquidityResult = {
    #Ok : AddLiquidityOk;
    #Err : ExchangeError;
  };

  public type AddConcentratedOk = {
    liquidity : Nat;
    positionId : Nat;
    token0 : Text;
    token1 : Text;
    amount0Used : Nat;
    amount1Used : Nat;
    refund0 : Nat;
    refund1 : Nat;
    priceLower : Nat;
    priceUpper : Nat;
  };

  public type AddConcentratedResult = {
    #Ok : AddConcentratedOk;
    #Err : ExchangeError;
  };

  public type RemoveConcentratedOk = {
    amount0 : Nat;
    amount1 : Nat;
    fees0 : Nat;
    fees1 : Nat;
    liquidityRemoved : Nat;
    liquidityRemaining : Nat;
  };

  public type RemoveConcentratedResult = {
    #Ok : RemoveConcentratedOk;
    #Err : ExchangeError;
  };

  public type RemoveLiquidityOk = {
    amount0 : Nat;
    amount1 : Nat;
    fees0 : Nat;
    fees1 : Nat;
    liquidityBurned : Nat;
  };

  public type RemoveLiquidityResult = {
    #Ok : RemoveLiquidityOk;
    #Err : ExchangeError;
  };

  public type ClaimFeesOk = {
    fees0 : Nat;
    fees1 : Nat;
    transferred0 : Nat;
    transferred1 : Nat;
    dust0ToDAO : Nat;
    dust1ToDAO : Nat;
  };

  public type ClaimFeesResult = {
    #Ok : ClaimFeesOk;
    #Err : ExchangeError;
  };

  // ═══════════════════════════════════════════════════════════════
  // TRADE SETTLEMENT RESULTS — revokeTrade, FinishSell, FinishSellBatch
  // ═══════════════════════════════════════════════════════════════

  public type RevokeOk = {
    accessCode : Text;
    revokeType : { #DAO; #Seller; #Initiator };
    refunds : [{ token : Text; amount : Nat }];
  };

  public type RevokeResult = {
    #Ok : RevokeOk;
    #Err : ExchangeError;
  };

  public type FinishSellOk = {
    amountSold : Nat;
    amountBought : Nat;
    partial : Bool;
    swapId : Nat;
  };

  public type FinishSellResult = {
    #Ok : FinishSellOk;
    #Err : ExchangeError;
  };

  public type FinishSellBatchOk = {
    tradesProcessed : Nat;
    totalSold : Nat;
    totalBought : Nat;
    errors : [Text];
  };

  public type FinishSellBatchResult = {
    #Ok : FinishSellBatchOk;
    #Err : ExchangeError;
  };

  // ═══════════════════════════════════════════════════════════════
  // ADMIN / UTILITY RESULTS — collectFees, addAcceptedToken, etc.
  // ═══════════════════════════════════════════════════════════════

  public type CollectFeesOk = {
    collected : [{ token : Text; amount : Nat }];
  };

  public type CollectFeesResult = {
    #Ok : CollectFeesOk;
    #Err : ExchangeError;
  };

  public type ActionResult = {
    #Ok : Text;
    #Err : ExchangeError;
  };
};
