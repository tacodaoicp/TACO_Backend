import Types "swap_types";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Error "mo:base/Error";
import Principal "mo:base/Principal";
import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Float "mo:base/Float";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Iter "mo:base/Iter";
import Array "mo:base/Array";

module {

  // TACO Exchange canister IDs
  let OTC_BACKEND = "qioex-5iaaa-aaaan-q52ba-cai";
  let EXCHANGE_TREASURY = "qbnpl-laaaa-aaaan-q52aq-cai";

  type SwapHop = { tokenIn : Text; tokenOut : Text };

  type ExchangeQuoteResult = {
    expectedBuyAmount : Nat;
    fee : Nat;
    priceImpact : Float;
    routeDescription : Text;
    canFulfillFully : Bool;
    potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
    hopDetails : [{
      tokenIn : Text; tokenOut : Text;
      amountIn : Nat; amountOut : Nat;
      fee : Nat; priceImpact : Float;
    }];
  };

  type MultiHopQuoteResult = {
    bestRoute : [SwapHop];
    expectedAmountOut : Nat;
    totalFee : Nat;
    priceImpact : Float;
    hops : Nat;
    routeTokens : [Text];
    hopDetails : [{
      tokenIn : Text; tokenOut : Text;
      amountIn : Nat; amountOut : Nat;
      fee : Nat; priceImpact : Float;
    }];
  };

  // ═══ QUOTE ═══
  // Returns SwapAmountsReply matching the KongSwap pattern
  public func getQuote(
    tokenA : Text,
    tokenB : Text,
    amountIn : Nat,
    sellDecimals : Nat,
    buyDecimals : Nat,
  ) : async Result.Result<Types.SwapAmountsReply, Text> {
    Debug.print("TACO getQuote: " # tokenA # " → " # tokenB # " amount=" # Nat.toText(amountIn));

    let exchange = actor (OTC_BACKEND) : actor {
      getExpectedReceiveAmount : shared query (Text, Text, Nat) -> async ExchangeQuoteResult;
    };

    try {
      let q = await exchange.getExpectedReceiveAmount(tokenA, tokenB, amountIn);

      if (q.expectedBuyAmount == 0) {
        return #err("TACO: no liquidity for " # tokenA # " → " # tokenB);
      };

      // Calculate execution price in human terms (post-slippage)
      let sellHuman = Float.fromInt(amountIn) / Float.fromInt(10 ** sellDecimals);
      let buyHuman = Float.fromInt(q.expectedBuyAmount) / Float.fromInt(10 ** buyDecimals);
      let executionPrice = if (sellHuman > 0.0) { buyHuman / sellHuman } else { 0.0 };

      // Derive spot price from execution + priceImpact
      // Exchange priceImpact is 0.0-1.0: impact = |1 - mathOutput / spotOutput|
      // So executionRate ≈ spotRate * (1 - impact) → spotRate = executionRate / (1 - impact)
      let spotPrice = if (q.priceImpact >= 0.0 and q.priceImpact < 0.99) {
        executionPrice / (1.0 - q.priceImpact)
      } else {
        executionPrice
      };

      // priceImpact is 0.0-1.0, convert to percentage for SwapAmountsReply
      let slippage = q.priceImpact * 100.0;

      Debug.print("TACO quote: out=" # Nat.toText(q.expectedBuyAmount) # " slip=" # Float.toText(slippage) # "% exec=" # Float.toText(executionPrice) # " spot=" # Float.toText(spotPrice) # " route=" # q.routeDescription);

      #ok({
        pay_chain = "IC";
        pay_symbol = tokenA;
        pay_address = tokenA;
        pay_amount = amountIn;
        receive_chain = "IC";
        receive_symbol = tokenB;
        receive_address = tokenB;
        receive_amount = q.expectedBuyAmount;
        price = executionPrice;
        mid_price = spotPrice;
        slippage = slippage;
        txs = [];
      });
    } catch (e) {
      Debug.print("TACO getQuote error: " # Error.message(e));
      #err("TACO quote failed: " # Error.message(e));
    };
  };

  // ═══ QUOTE WITH ROUTE (returns route info for multi-route detection) ═══
  public func getQuoteWithRoute(
    tokenA : Text,
    tokenB : Text,
    amountIn : Nat,
    sellDecimals : Nat,
    buyDecimals : Nat,
  ) : async Result.Result<Types.TACOQuoteReply, Text> {
    Debug.print("TACO getQuoteWithRoute: " # tokenA # " → " # tokenB # " amount=" # Nat.toText(amountIn));

    let exchange = actor (OTC_BACKEND) : actor {
      getExpectedReceiveAmount : shared query (Text, Text, Nat) -> async ExchangeQuoteResult;
    };

    try {
      let q = await exchange.getExpectedReceiveAmount(tokenA, tokenB, amountIn);

      if (q.expectedBuyAmount == 0) {
        return #err("TACO: no liquidity for " # tokenA # " → " # tokenB);
      };

      let sellHuman = Float.fromInt(amountIn) / Float.fromInt(10 ** sellDecimals);
      let buyHuman = Float.fromInt(q.expectedBuyAmount) / Float.fromInt(10 ** buyDecimals);
      let executionPrice = if (sellHuman > 0.0) { buyHuman / sellHuman } else { 0.0 };
      let spotPrice = if (q.priceImpact >= 0.0 and q.priceImpact < 0.99) {
        executionPrice / (1.0 - q.priceImpact)
      } else { executionPrice };
      let slippage = q.priceImpact * 100.0;

      // Extract route from hopDetails
      let route = Array.map<{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }, { tokenIn : Text; tokenOut : Text }>(
        q.hopDetails,
        func(h) { { tokenIn = h.tokenIn; tokenOut = h.tokenOut } }
      );

      // If hopDetails empty (direct AMM/orderbook), construct single-hop route
      let finalRoute = if (route.size() == 0) {
        [{ tokenIn = tokenA; tokenOut = tokenB }]
      } else { route };

      Debug.print("TACO quoteWithRoute: out=" # Nat.toText(q.expectedBuyAmount) # " slip=" # Float.toText(slippage) # "% route=" # q.routeDescription # " hops=" # Nat.toText(finalRoute.size()));

      // routeTokens: canonical [tokenA, …intermediates, tokenB] derived from finalRoute hops
      let routeTokens = Array.tabulate<Text>(
        finalRoute.size() + 1,
        func(i) { if (i == 0) { tokenA } else { finalRoute[i - 1].tokenOut } },
      );

      #ok({
        receive_amount = q.expectedBuyAmount;
        price = executionPrice;
        mid_price = spotPrice;
        slippage = slippage;
        route = finalRoute;
        routeDescription = q.routeDescription;
        canFulfillFully = q.canFulfillFully;
        routeTokens;
      });
    } catch (e) {
      Debug.print("TACO getQuoteWithRoute error: " # Error.message(e));
      #err("TACO quote failed: " # Error.message(e));
    };
  };

  // ═══ BATCH QUOTE (10 quotes in 1 inter-canister call instead of 10) ═══
  public func getQuoteWithRouteBatch(
    requests : [{ tokenA : Text; tokenB : Text; amountIn : Nat }],
    sellDecimals : Nat,
    buyDecimals : Nat,
  ) : async Result.Result<[Types.TACOQuoteReply], Text> {
    Debug.print("TACO getQuoteWithRouteBatch: " # Nat.toText(requests.size()) # " quotes");

    let exchange = actor (OTC_BACKEND) : actor {
      getExpectedReceiveAmountBatch : shared query ([{
        tokenSell : Text; tokenBuy : Text; amountSell : Nat;
      }]) -> async [{
        expectedBuyAmount : Nat; fee : Nat; priceImpact : Float;
        routeDescription : Text; canFulfillFully : Bool;
        potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
        hopDetails : [{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }];
      }];
    };

    try {
      // Convert to exchange format
      let batchReqs = Array.map<{ tokenA : Text; tokenB : Text; amountIn : Nat }, { tokenSell : Text; tokenBuy : Text; amountSell : Nat }>(
        requests,
        func(r) { { tokenSell = r.tokenA; tokenBuy = r.tokenB; amountSell = r.amountIn } },
      );

      let batchResults = await exchange.getExpectedReceiveAmountBatch(batchReqs);

      // Convert each result to TACOQuoteReply (same as getQuoteWithRoute)
      let replies = Array.tabulate<Types.TACOQuoteReply>(batchResults.size(), func(i) {
        let q = batchResults[i];
        let req = requests[i];

        if (q.expectedBuyAmount == 0) {
          { receive_amount = 0; price = 0.0; mid_price = 0.0; slippage = 0.0;
            route = []; routeDescription = "No liquidity"; canFulfillFully = false;
            routeTokens = [] };
        } else {
          let sellHuman = Float.fromInt(req.amountIn) / Float.fromInt(10 ** sellDecimals);
          let buyHuman = Float.fromInt(q.expectedBuyAmount) / Float.fromInt(10 ** buyDecimals);
          let executionPrice = if (sellHuman > 0.0) { buyHuman / sellHuman } else { 0.0 };
          let spotPrice = if (q.priceImpact >= 0.0 and q.priceImpact < 0.99) {
            executionPrice / (1.0 - q.priceImpact)
          } else { executionPrice };
          let slippage = q.priceImpact * 100.0;

          let route = Array.map<{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }, { tokenIn : Text; tokenOut : Text }>(
            q.hopDetails,
            func(h) { { tokenIn = h.tokenIn; tokenOut = h.tokenOut } },
          );
          let finalRoute = if (route.size() == 0) {
            [{ tokenIn = req.tokenA; tokenOut = req.tokenB }]
          } else { route };

          let routeTokens = Array.tabulate<Text>(
            finalRoute.size() + 1,
            func(j) { if (j == 0) { req.tokenA } else { finalRoute[j - 1].tokenOut } },
          );

          { receive_amount = q.expectedBuyAmount; price = executionPrice; mid_price = spotPrice;
            slippage = slippage; route = finalRoute; routeDescription = q.routeDescription;
            canFulfillFully = q.canFulfillFully; routeTokens };
        };
      });

      #ok(replies);
    } catch (e) {
      #err("TACO batch quote failed: " # Error.message(e));
    };
  };

  // ═══ MULTI-ROUTE BATCH QUOTE — top-N routes per fraction in 1 inter-canister call ═══
  // Same call budget as getQuoteWithRouteBatch (single inter-canister query) but the
  // canister returns up to `maxRoutesPerRequest` routes per fraction with state isolation
  // between every route simulation. Lets the treasury scenario builder enumerate
  // TACO×TACO same-DEX splits — provided the pool-disjointness filter is applied
  // before any sum is admitted to the comparator (otherwise overlapping pools cause
  // the second leg to execute against the first leg's mutated state and quoted output
  // is an upper bound). routeTokens on each reply is the canonical pool-set key.
  public func getQuoteWithRouteBatchMulti(
    requests : [{ tokenA : Text; tokenB : Text; amountIn : Nat }],
    sellDecimals : Nat,
    buyDecimals : Nat,
    maxRoutesPerRequest : Nat,
  ) : async Result.Result<[[Types.TACOQuoteReply]], Text> {
    Debug.print("TACO getQuoteWithRouteBatchMulti: " # Nat.toText(requests.size()) # " quotes × top-" # Nat.toText(maxRoutesPerRequest));

    let exchange = actor (OTC_BACKEND) : actor {
      getExpectedReceiveAmountBatchMulti : shared query (
        [{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }], Nat
      ) -> async [{
        routes : [{
          expectedBuyAmount : Nat; fee : Nat; priceImpact : Float;
          routeDescription : Text; canFulfillFully : Bool;
          potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
          hopDetails : [{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }];
          routeTokens : [Text];
          tradingFeeBps : Nat;
        }];
      }];
    };

    try {
      let batchReqs = Array.map<{ tokenA : Text; tokenB : Text; amountIn : Nat }, { tokenSell : Text; tokenBuy : Text; amountSell : Nat }>(
        requests,
        func(r) { { tokenSell = r.tokenA; tokenBuy = r.tokenB; amountSell = r.amountIn } },
      );

      let batchResults = await exchange.getExpectedReceiveAmountBatchMulti(batchReqs, maxRoutesPerRequest);

      let bundles = Array.tabulate<[Types.TACOQuoteReply]>(batchResults.size(), func(i) {
        let bundle = batchResults[i];
        let req = requests[i];

        Array.map<{
          expectedBuyAmount : Nat; fee : Nat; priceImpact : Float;
          routeDescription : Text; canFulfillFully : Bool;
          potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
          hopDetails : [{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }];
          routeTokens : [Text];
          tradingFeeBps : Nat;
        }, Types.TACOQuoteReply>(bundle.routes, func(q) {
          if (q.expectedBuyAmount == 0) {
            { receive_amount = 0; price = 0.0; mid_price = 0.0; slippage = 0.0;
              route = []; routeDescription = "No liquidity"; canFulfillFully = false;
              routeTokens = [] };
          } else {
            let sellHuman = Float.fromInt(req.amountIn) / Float.fromInt(10 ** sellDecimals);
            let buyHuman = Float.fromInt(q.expectedBuyAmount) / Float.fromInt(10 ** buyDecimals);
            let executionPrice = if (sellHuman > 0.0) { buyHuman / sellHuman } else { 0.0 };
            let spotPrice = if (q.priceImpact >= 0.0 and q.priceImpact < 0.99) {
              executionPrice / (1.0 - q.priceImpact)
            } else { executionPrice };
            let slippage = q.priceImpact * 100.0;

            let route = Array.map<{ tokenIn : Text; tokenOut : Text; amountIn : Nat; amountOut : Nat; fee : Nat; priceImpact : Float }, { tokenIn : Text; tokenOut : Text }>(
              q.hopDetails,
              func(h) { { tokenIn = h.tokenIn; tokenOut = h.tokenOut } },
            );
            // For direct routes the canister returns hopDetails = [] AND routeTokens = [tokenSell, tokenBuy]
            let finalRoute = if (route.size() == 0) {
              [{ tokenIn = req.tokenA; tokenOut = req.tokenB }]
            } else { route };

            { receive_amount = q.expectedBuyAmount; price = executionPrice; mid_price = spotPrice;
              slippage = slippage; route = finalRoute; routeDescription = q.routeDescription;
              canFulfillFully = q.canFulfillFully; routeTokens = q.routeTokens };
          };
        });
      });

      #ok(bundles);
    } catch (e) {
      Debug.print("TACO getQuoteWithRouteBatchMulti error: " # Error.message(e));
      #err("TACO multi-route batch quote failed: " # Error.message(e));
    };
  };

  // ═══ ALL POOLS (one-call snapshot for price sync + liquidity weights) ═══
  public func getAllPools() : async Result.Result<[{
    token0 : Text;
    token1 : Text;
    reserve0 : Nat;
    reserve1 : Nat;
    price0 : Float;
    price1 : Float;
    totalLiquidity : Nat;
  }], Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      getAllAMMPools : shared query () -> async [{
        token0 : Text;
        token1 : Text;
        reserve0 : Nat;
        reserve1 : Nat;
        price0 : Float;
        price1 : Float;
        totalLiquidity : Nat;
      }];
    };

    try {
      let pools = await exchange.getAllAMMPools();
      Debug.print("TACO getAllPools: " # Nat.toText(pools.size()) # " active pools");
      #ok(pools);
    } catch (e) {
      #err("TACO getAllPools failed: " # Error.message(e));
    };
  };

  // ═══ EXECUTE ═══
  // Transfer to exchange treasury + get route + swapMultiHop
  public func executeTransferAndSwap(
    params : Types.TACOSwapParams,
  ) : async Result.Result<Types.TACOSwapReply, Text> {
    Debug.print("TACO execute: " # Principal.toText(params.tokenIn) # " → " # Principal.toText(params.tokenOut) # " amt=" # Nat.toText(params.amountIn));

    let exchangeTreasury = Principal.fromText(EXCHANGE_TREASURY);
    let tokenInText = Principal.toText(params.tokenIn);
    let tokenOutText = Principal.toText(params.tokenOut);

    type SwapOk = { amountIn : Nat; amountOut : Nat; tokenIn : Text; tokenOut : Text; route : [Text]; fee : Nat; swapId : Nat; hops : Nat; firstHopOrderbookMatch : Bool; lastHopAMMOnly : Bool };
    type ExchangeError = { #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text; #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text; #SlippageExceeded : { expected : Nat; got : Nat }; #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text; #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text };
    let exchangeActor = actor (OTC_BACKEND) : actor {
      getExpectedMultiHopAmount : shared query (Text, Text, Nat) -> async MultiHopQuoteResult;
      swapMultiHop : shared (Text, Text, Nat, [SwapHop], Nat, Nat) -> async { #Ok : SwapOk; #Err : ExchangeError };
    };

    // Step 1: Get route
    let routeResult = try {
      await exchangeActor.getExpectedMultiHopAmount(tokenInText, tokenOutText, params.amountIn);
    } catch (e) {
      return #err("TACO route lookup failed: " # Error.message(e));
    };

    if (routeResult.expectedAmountOut == 0 or routeResult.bestRoute.size() < 1) {
      return #err("TACO: no route found for " # tokenInText # " → " # tokenOutText);
    };

    Debug.print("TACO route: " # Nat.toText(routeResult.hops) # " hops, expected=" # Nat.toText(routeResult.expectedAmountOut));

    // Step 2: Verify tokens are tradeable before transferring
    let canTrade = try {
      await (actor (OTC_BACKEND) : actor { canTradeTokens : shared query (Text, Text) -> async Bool }).canTradeTokens(tokenInText, tokenOutText);
    } catch (_) { false };
    if (not canTrade) {
      return #err("TACO: token not accepted or paused on exchange");
    };

    // Step 3: Calculate deposit (includes exchange fee 5bp)
    let exchangeFeeBps : Nat = 5;
    let depositAmount = params.amountIn * (exchangeFeeBps + 10000) / 10000 + params.transferFee;

    // Step 4: Transfer to exchange treasury
    let blockResult : Result.Result<Nat, Text> = if (tokenInText == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
      // ICP legacy transfer
      let icpLedger = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : actor {
        transfer : shared ({
          to : Blob;
          fee : { e8s : Nat64 };
          memo : Nat64;
          from_subaccount : ?Blob;
          created_at_time : ?{ timestamp_nanos : Nat64 };
          amount : { e8s : Nat64 };
        }) -> async { #Ok : Nat64; #Err : Types.ICP_TransferError };
      };
      try {
        let result = await icpLedger.transfer({
          to = params.exchangeTreasuryAccountId;
          fee = { e8s = 10_000 : Nat64 };
          memo = 0 : Nat64;
          from_subaccount = null;
          created_at_time = null;
          amount = { e8s = Nat64.fromNat(depositAmount) };
        });
        switch (result) {
          case (#Ok(block)) { #ok(Nat64.toNat(block)) };
          case (#Err(e)) { #err("ICP transfer failed: " # debug_show (e)) };
        };
      } catch (e) { #err("ICP transfer error: " # Error.message(e)) };
    } else {
      // ICRC-1 transfer
      let tokenActor = actor (tokenInText) : actor {
        icrc1_transfer : shared (Types.ICRC1TransferArgs) -> async { #Ok : Nat; #Err : Types.ICRC1TransferError };
      };
      try {
        let result = await tokenActor.icrc1_transfer({
          to = { owner = exchangeTreasury; subaccount = null };
          fee = null;
          memo = null;
          from_subaccount = null;
          created_at_time = null;
          amount = depositAmount;
        });
        switch (result) {
          case (#Ok(block)) { #ok(block) };
          case (#Err(e)) { #err("ICRC1 transfer failed: " # debug_show (e)) };
        };
      } catch (e) { #err("ICRC1 transfer error: " # Error.message(e)) };
    };

    let blockNumber = switch (blockResult) {
      case (#ok(b)) { b };
      case (#err(e)) { return #err(e) };
    };

    Debug.print("TACO transfer done, block=" # Nat.toText(blockNumber));

    // Step 4: Execute swap
    let swapResult = try {
      await exchangeActor.swapMultiHop(
        tokenInText,
        tokenOutText,
        params.amountIn,
        routeResult.bestRoute,
        params.minAmountOut,
        blockNumber,
      );
    } catch (e) {
      return #err("TACO swap call failed [block=" # Nat.toText(blockNumber) # " token=" # tokenInText # "]: " # Error.message(e));
    };

    Debug.print("TACO swap result: " # debug_show(swapResult));

    switch (swapResult) {
      case (#Ok(ok)) {
        let slippage = if (routeResult.expectedAmountOut > 0) {
          Float.abs(1.0 - Float.fromInt(ok.amountOut) / Float.fromInt(routeResult.expectedAmountOut)) * 100.0;
        } else { 0.0 };
        Debug.print("TACO success: received=" # Nat.toText(ok.amountOut) # " slip=" # Float.toText(slippage) # "%");
        #ok({
          amountIn = params.amountIn;
          amountOut = ok.amountOut;
          slippage = slippage;
          route = routeResult.routeTokens;
          blockNumber = blockNumber;
        });
      };
      case (#Err(e)) {
        #err("TACO swap failed [block=" # Nat.toText(blockNumber) # " token=" # tokenInText # "]: " # debug_show(e));
      };
    };
  };

  // ═══ RECOVERY ═══
  // Recover tokens stuck in exchange from failed swaps
  public func recoverStuckTokens(
    tokenId : Text,
    blockNumber : Nat,
    tokenType : { #ICP; #ICRC12; #ICRC3 },
  ) : async Result.Result<Bool, Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      recoverWronglysent : shared (Text, Nat, { #ICP; #ICRC12; #ICRC3 }) -> async Bool;
    };

    try {
      let result = await exchange.recoverWronglysent(tokenId, blockNumber, tokenType);
      if (result) {
        Debug.print("TACO recovery success: token=" # tokenId # " block=" # Nat.toText(blockNumber));
        #ok(true);
      } else {
        Debug.print("TACO recovery failed: block already used or not found");
        #ok(false);
      };
    } catch (e) {
      #err("TACO recovery error: " # Error.message(e));
    };
  };

  // ═══ EXECUTE (no tracking variant for parallel split execution) ═══
  // Same as executeTransferAndSwap but designed to be fired as a future
  public func executeTransferAndSwapNoTracking(
    params : Types.TACOSwapParams,
  ) : async Result.Result<Types.TACOSwapReply, Text> {
    // Identical to executeTransferAndSwap — the "NoTracking" name matches Kong's pattern
    // where tracking (trade recording) is done by the caller, not the swap module
    await executeTransferAndSwap(params);
  };

  // ═══ EXECUTE MULTI-ROUTE (split across up to 3 routes via swapSplitRoutes) ═══
  public func executeTransferAndSwapMultiRoute(
    params : Types.TACOSwapParams,
    legs : [Types.TACOSplitLeg],
  ) : async Result.Result<Types.TACOSwapReply, Text> {
    Debug.print("TACO multi-route execute: " # Principal.toText(params.tokenIn) # " → " # Principal.toText(params.tokenOut) # " legs=" # Nat.toText(legs.size()));

    let exchangeTreasury = Principal.fromText(EXCHANGE_TREASURY);
    let tokenInText = Principal.toText(params.tokenIn);
    let tokenOutText = Principal.toText(params.tokenOut);

    type SplitSwapOk = { amountIn : Nat; amountOut : Nat; tokenIn : Text; tokenOut : Text; route : [Text]; fee : Nat; swapId : Nat; hops : Nat; firstHopOrderbookMatch : Bool; lastHopAMMOnly : Bool };
    type SplitExchangeError = { #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text; #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text; #SlippageExceeded : { expected : Nat; got : Nat }; #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text; #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text };
    let exchangeActor = actor (OTC_BACKEND) : actor {
      swapSplitRoutes : shared (Text, Text, [{ amountIn : Nat; route : [SwapHop]; minLegOut : Nat }], Nat, Nat) -> async { #Ok : SplitSwapOk; #Err : SplitExchangeError };
    };

    // Step 1: Verify tokens are tradeable before transferring
    let canTrade = try {
      await (actor (OTC_BACKEND) : actor { canTradeTokens : shared query (Text, Text) -> async Bool }).canTradeTokens(tokenInText, tokenOutText);
    } catch (_) { false };
    if (not canTrade) {
      return #err("TACO: token not accepted or paused on exchange");
    };

    // Step 2: Calculate deposit (includes exchange fee 5bp)
    let exchangeFeeBps : Nat = 5;
    let depositAmount = params.amountIn * (exchangeFeeBps + 10000) / 10000 + params.transferFee;

    // Step 3: Transfer to exchange treasury
    let blockResult : Result.Result<Nat, Text> = if (tokenInText == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
      let icpLedger = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : actor {
        transfer : shared ({
          to : Blob;
          fee : { e8s : Nat64 };
          memo : Nat64;
          from_subaccount : ?Blob;
          created_at_time : ?{ timestamp_nanos : Nat64 };
          amount : { e8s : Nat64 };
        }) -> async { #Ok : Nat64; #Err : Types.ICP_TransferError };
      };
      try {
        let result = await icpLedger.transfer({
          to = params.exchangeTreasuryAccountId;
          fee = { e8s = 10_000 : Nat64 };
          memo = 0 : Nat64;
          from_subaccount = null;
          created_at_time = null;
          amount = { e8s = Nat64.fromNat(depositAmount) };
        });
        switch (result) {
          case (#Ok(block)) { #ok(Nat64.toNat(block)) };
          case (#Err(e)) { #err("ICP transfer failed: " # debug_show (e)) };
        };
      } catch (e) { #err("ICP transfer error: " # Error.message(e)) };
    } else {
      let tokenActor = actor (tokenInText) : actor {
        icrc1_transfer : shared (Types.ICRC1TransferArgs) -> async { #Ok : Nat; #Err : Types.ICRC1TransferError };
      };
      try {
        let result = await tokenActor.icrc1_transfer({
          to = { owner = exchangeTreasury; subaccount = null };
          fee = null;
          memo = null;
          from_subaccount = null;
          created_at_time = null;
          amount = depositAmount;
        });
        switch (result) {
          case (#Ok(block)) { #ok(block) };
          case (#Err(e)) { #err("ICRC1 transfer failed: " # debug_show (e)) };
        };
      } catch (e) { #err("ICRC1 transfer error: " # Error.message(e)) };
    };

    let blockNumber = switch (blockResult) {
      case (#ok(b)) { b };
      case (#err(e)) { return #err(e) };
    };

    Debug.print("TACO multi-route transfer done, block=" # Nat.toText(blockNumber));

    // Step 3: Convert legs to exchange format
    let splitLegs = Array.map<Types.TACOSplitLeg, { amountIn : Nat; route : [SwapHop]; minLegOut : Nat }>(
      legs,
      func(leg) {
        {
          amountIn = leg.amountIn;
          route = Array.map<{ tokenIn : Text; tokenOut : Text }, SwapHop>(
            leg.route,
            func(h) { { tokenIn = h.tokenIn; tokenOut = h.tokenOut } }
          );
          minLegOut = leg.minLegOut;
        }
      }
    );

    // Step 4: Execute via swapSplitRoutes
    let swapResult = try {
      await exchangeActor.swapSplitRoutes(
        tokenInText,
        tokenOutText,
        splitLegs,
        params.minAmountOut,
        blockNumber,
      );
    } catch (e) {
      return #err("TACO split routes failed [block=" # Nat.toText(blockNumber) # " token=" # tokenInText # "]: " # Error.message(e));
    };

    Debug.print("TACO multi-route result: " # debug_show(swapResult));

    switch (swapResult) {
      case (#Ok(ok)) {
        let slippage = if (params.minAmountOut > 0 and ok.amountOut < params.minAmountOut) {
          100.0
        } else { 0.0 };
        Debug.print("TACO multi-route success: received=" # Nat.toText(ok.amountOut));
        #ok({
          amountIn = params.amountIn;
          amountOut = ok.amountOut;
          slippage = slippage;
          route = ok.route;
          blockNumber = blockNumber;
        });
      };
      case (#Err(e)) {
        #err("TACO split routes failed [block=" # Nat.toText(blockNumber) # " token=" # tokenInText # "]: " # debug_show(e));
      };
    };
  };

  // ═══ EXECUTE MULTI-ROUTE (no tracking variant) ═══
  public func executeTransferAndSwapMultiRouteNoTracking(
    params : Types.TACOSwapParams,
    legs : [Types.TACOSplitLeg],
  ) : async Result.Result<Types.TACOSwapReply, Text> {
    await executeTransferAndSwapMultiRoute(params, legs);
  };

  // ═══ LP MANAGEMENT ═══
  // These functions wrap exchange LP calls for treasury use

  // Query all LP positions for the caller
  public func getUserLiquidityDetailed() : async Result.Result<[Types.DetailedLiquidityPosition], Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      getUserLiquidityDetailed : shared query () -> async [Types.DetailedLiquidityPosition];
    };
    try {
      let positions = await exchange.getUserLiquidityDetailed();
      Debug.print("TACO getUserLiquidityDetailed: " # Nat.toText(positions.size()) # " positions");
      #ok(positions);
    } catch (e) {
      #err("TACO getUserLiquidityDetailed failed: " # Error.message(e));
    };
  };

  // Exchange token info (from getAcceptedTokensInfo)
  public type ExchangeTokenInfo = {
    address : Text;
    name : Text;
    symbol : Text;
    transfer_fee : Nat;
    decimals : Nat;
    minimum_amount : Nat;
    asset_type : { #ICP; #ICRC12; #ICRC3 };
  };

  // Query accepted tokens with info (minimums, decimals, fees) from the exchange
  public func getAcceptedTokensInfo() : async Result.Result<[ExchangeTokenInfo], Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      getAcceptedTokensInfo : shared query () -> async ?[ExchangeTokenInfo];
    };
    try {
      let result = await exchange.getAcceptedTokensInfo();
      switch (result) {
        case (?tokens) { #ok(tokens) };
        case null { #ok([]) };
      };
    } catch (e) {
      #err("TACO getAcceptedTokensInfo failed: " # Error.message(e));
    };
  };

  // Query all AMM pool states (reserves, TVL, totalLiquidity)
  public func getAllAMMPoolsFull() : async Result.Result<[Types.AMMPoolInfo], Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      getAllAMMPools : shared query () -> async [Types.AMMPoolInfo];
    };
    try {
      let pools = await exchange.getAllAMMPools();
      Debug.print("TACO getAllAMMPoolsFull: " # Nat.toText(pools.size()) # " pools");
      #ok(pools);
    } catch (e) {
      #err("TACO getAllAMMPoolsFull failed: " # Error.message(e));
    };
  };

  // Add liquidity to a V2 pool (requires prior token transfers with block numbers)
  public func doAddLiquidity(
    token0 : Text, token1 : Text,
    amount0 : Nat, amount1 : Nat,
    block0 : Nat, block1 : Nat,
    isInitial : ?Bool,
  ) : async {
    #Ok : {
      liquidityMinted : Nat; token0 : Text; token1 : Text;
      amount0Used : Nat; amount1Used : Nat; refund0 : Nat; refund1 : Nat;
    };
    #Err : {
      #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
      #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
      #SlippageExceeded : { expected : Nat; got : Nat };
      #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
      #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
    };
  } {
    let exchange = actor (OTC_BACKEND) : actor {
      addLiquidity : shared (Text, Text, Nat, Nat, Nat, Nat, ?Bool) -> async {
        #Ok : {
          liquidityMinted : Nat; token0 : Text; token1 : Text;
          amount0Used : Nat; amount1Used : Nat; refund0 : Nat; refund1 : Nat;
        };
        #Err : {
          #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
          #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
          #SlippageExceeded : { expected : Nat; got : Nat };
          #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
          #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
        };
      };
    };
    Debug.print("TACO addLiquidity: " # token0 # "/" # token1 # " amounts=" # Nat.toText(amount0) # "/" # Nat.toText(amount1) # " blocks=" # Nat.toText(block0) # "/" # Nat.toText(block1));
    await exchange.addLiquidity(token0, token1, amount0, amount1, block0, block1, isInitial);
  };

  // Remove liquidity from a V2 pool (returns tokens via exchange treasury transfer)
  public func doRemoveLiquidity(
    token0 : Text, token1 : Text,
    liquidityAmount : Nat,
  ) : async {
    #Ok : { amount0 : Nat; amount1 : Nat; fees0 : Nat; fees1 : Nat; liquidityBurned : Nat };
    #Err : {
      #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
      #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
      #SlippageExceeded : { expected : Nat; got : Nat };
      #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
      #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
    };
  } {
    let exchange = actor (OTC_BACKEND) : actor {
      removeLiquidity : shared (Text, Text, Nat) -> async {
        #Ok : { amount0 : Nat; amount1 : Nat; fees0 : Nat; fees1 : Nat; liquidityBurned : Nat };
        #Err : {
          #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
          #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
          #SlippageExceeded : { expected : Nat; got : Nat };
          #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
          #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
        };
      };
    };
    Debug.print("TACO removeLiquidity: " # token0 # "/" # token1 # " liq=" # Nat.toText(liquidityAmount));
    await exchange.removeLiquidity(token0, token1, liquidityAmount);
  };

  // Remove a V3 concentrated liquidity position by positionId
  public func doRemoveConcentratedLiquidity(
    token0 : Text, token1 : Text,
    positionId : Nat,
    liquidityAmount : Nat,
  ) : async {
    #Ok : { amount0 : Nat; amount1 : Nat; fees0 : Nat; fees1 : Nat };
    #Err : {
      #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
      #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
      #SlippageExceeded : { expected : Nat; got : Nat };
      #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
      #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
    };
  } {
    let exchange = actor (OTC_BACKEND) : actor {
      removeConcentratedLiquidity : shared (Text, Text, Nat, Nat) -> async {
        #Ok : { amount0 : Nat; amount1 : Nat; fees0 : Nat; fees1 : Nat };
        #Err : {
          #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
          #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
          #SlippageExceeded : { expected : Nat; got : Nat };
          #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
          #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
        };
      };
    };
    Debug.print("TACO removeConcentratedLiquidity: " # token0 # "/" # token1 # " posId=" # Nat.toText(positionId) # " liq=" # Nat.toText(liquidityAmount));
    await exchange.removeConcentratedLiquidity(token0, token1, positionId, liquidityAmount);
  };

  // Claim accumulated LP fees from a pool
  public func doClaimLPFees(
    token0 : Text, token1 : Text,
  ) : async {
    #Ok : { fees0 : Nat; fees1 : Nat; transferred0 : Nat; transferred1 : Nat; dust0ToDAO : Nat; dust1ToDAO : Nat };
    #Err : {
      #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
      #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
      #SlippageExceeded : { expected : Nat; got : Nat };
      #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
      #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
    };
  } {
    let exchange = actor (OTC_BACKEND) : actor {
      claimLPFees : shared (Text, Text) -> async {
        #Ok : { fees0 : Nat; fees1 : Nat; transferred0 : Nat; transferred1 : Nat; dust0ToDAO : Nat; dust1ToDAO : Nat };
        #Err : {
          #NotAuthorized; #Banned; #InvalidInput : Text; #TokenNotAccepted : Text;
          #TokenPaused : Text; #InsufficientFunds : Text; #PoolNotFound : Text;
          #SlippageExceeded : { expected : Nat; got : Nat };
          #RouteFailed : { hop : Nat; reason : Text }; #OrderNotFound : Text;
          #ExchangeFrozen; #TransferFailed : Text; #SystemError : Text;
        };
      };
    };
    Debug.print("TACO claimLPFees: " # token0 # "/" # token1);
    await exchange.claimLPFees(token0, token1);
  };

  // Transfer a token to the exchange treasury (for LP deposits)
  // Returns the block number from the transfer
  public func transferToExchangeTreasury(
    tokenPrincipal : Principal,
    amount : Nat,
    exchangeTreasuryAccountId : Blob, // For ICP legacy transfers
  ) : async Result.Result<Nat, Text> {
    let tokenText = Principal.toText(tokenPrincipal);
    let exchangeTreasury = Principal.fromText(EXCHANGE_TREASURY);

    // Verify token is accepted and not paused before transferring
    let canTrade = try {
      await (actor (OTC_BACKEND) : actor { canTradeTokens : shared query (Text, Text) -> async Bool }).canTradeTokens(tokenText, tokenText);
    } catch (_) { false };
    if (not canTrade) {
      return #err("TACO: token " # tokenText # " not accepted or paused on exchange");
    };

    if (tokenText == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
      // ICP legacy transfer
      let icpLedger = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : actor {
        transfer : shared ({
          to : Blob;
          fee : { e8s : Nat64 };
          memo : Nat64;
          from_subaccount : ?Blob;
          created_at_time : ?{ timestamp_nanos : Nat64 };
          amount : { e8s : Nat64 };
        }) -> async { #Ok : Nat64; #Err : Types.ICP_TransferError };
      };
      try {
        let result = await icpLedger.transfer({
          to = exchangeTreasuryAccountId;
          fee = { e8s = 10_000 : Nat64 };
          memo = 0 : Nat64;
          from_subaccount = null;
          created_at_time = null;
          amount = { e8s = Nat64.fromNat(amount) };
        });
        switch (result) {
          case (#Ok(block)) { #ok(Nat64.toNat(block)) };
          case (#Err(e)) { #err("ICP transfer to exchange failed: " # debug_show(e)) };
        };
      } catch (e) { #err("ICP transfer error: " # Error.message(e)) };
    } else {
      // ICRC-1 transfer
      let tokenActor = actor (tokenText) : actor {
        icrc1_transfer : shared (Types.ICRC1TransferArgs) -> async { #Ok : Nat; #Err : Types.ICRC1TransferError };
      };
      try {
        let result = await tokenActor.icrc1_transfer({
          to = { owner = exchangeTreasury; subaccount = null };
          fee = null;
          memo = null;
          from_subaccount = null;
          created_at_time = null;
          amount = amount;
        });
        switch (result) {
          case (#Ok(block)) { #ok(block) };
          case (#Err(e)) { #err("ICRC1 transfer to exchange failed: " # debug_show(e)) };
        };
      } catch (e) { #err("ICRC1 transfer error: " # Error.message(e)) };
    };
  };

  // ═══ DAO LP HELPERS (Phase 3) ═══

  // Combined LP positions + pool data in one call
  public func getDAOLiquiditySnapshot() : async Result.Result<{
    positions : [{ token0 : Text; token1 : Text; liquidity : Nat; token0Amount : Nat; token1Amount : Nat; shareOfPool : Float; fee0 : Nat; fee1 : Nat }];
    pools : [Types.AMMPoolInfo];
  }, Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      getDAOLiquiditySnapshot : shared query () -> async {
        positions : [{ token0 : Text; token1 : Text; liquidity : Nat; token0Amount : Nat; token1Amount : Nat; shareOfPool : Float; fee0 : Nat; fee1 : Nat }];
        pools : [Types.AMMPoolInfo];
      };
    };
    try { #ok(await exchange.getDAOLiquiditySnapshot()) }
    catch (e) { #err("TACO getDAOLiquiditySnapshot failed: " # Error.message(e)) };
  };

  // Batch claim fees from all LP positions
  public func doBatchClaimAllFees() : async Result.Result<[{
    token0 : Text; token1 : Text; fees0 : Nat; fees1 : Nat; transferred0 : Nat; transferred1 : Nat;
  }], Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      batchClaimAllFees : shared () -> async [{
        token0 : Text; token1 : Text; fees0 : Nat; fees1 : Nat; transferred0 : Nat; transferred1 : Nat;
      }];
    };
    try { #ok(await exchange.batchClaimAllFees()) }
    catch (e) { #err("TACO batchClaimAllFees failed: " # Error.message(e)) };
  };

  // Batch remove liquidity across multiple pools
  public func doBatchRemoveLiquidity(adjustments : [{
    token0 : Text; token1 : Text;
    action : { #Remove : { liquidityAmount : Nat } };
  }]) : async Result.Result<[{
    token0 : Text; token1 : Text; success : Bool; result : Text;
  }], Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      batchAdjustLiquidity : shared ([{
        token0 : Text; token1 : Text;
        action : { #Remove : { liquidityAmount : Nat } };
      }]) -> async [{ token0 : Text; token1 : Text; success : Bool; result : Text }];
    };
    try { #ok(await exchange.batchAdjustLiquidity(adjustments)) }
    catch (e) { #err("TACO batchAdjustLiquidity failed: " # Error.message(e)) };
  };

  // LP performance data for monitoring
  public func getDAOLPPerformance() : async Result.Result<[{
    token0 : Text; token1 : Text;
    currentValue0 : Nat; currentValue1 : Nat;
    totalFeesEarned0 : Nat; totalFeesEarned1 : Nat;
    shareOfPool : Float; poolVolume24h : Nat;
  }], Text> {
    let exchange = actor (OTC_BACKEND) : actor {
      getDAOLPPerformance : shared query () -> async [{
        token0 : Text; token1 : Text;
        currentValue0 : Nat; currentValue1 : Nat;
        totalFeesEarned0 : Nat; totalFeesEarned1 : Nat;
        shareOfPool : Float; poolVolume24h : Nat;
      }];
    };
    try { #ok(await exchange.getDAOLPPerformance()) }
    catch (e) { #err("TACO getDAOLPPerformance failed: " # Error.message(e)) };
  };

  // Helper: parse Nat from text starting at offset
  func parseNatFromText(text : Text, offset : Nat) : Nat {
    var result : Nat = 0;
    var idx : Nat = 0;
    for (c in text.chars()) {
      if (idx >= offset) {
        let code = Char.toNat32(c);
        if (code >= 48 and code <= 57) {
          result := result * 10 + Nat.sub(Nat32.toNat(code), 48);
        } else if (result > 0) {
          return result; // stop at first non-digit after digits started
        };
      };
      idx += 1;
    };
    result;
  };
};
