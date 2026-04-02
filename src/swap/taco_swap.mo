import Types "swap_types";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Error "mo:base/Error";
import Principal "mo:base/Principal";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Float "mo:base/Float";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Iter "mo:base/Iter";

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

      // Calculate price in human terms
      let sellHuman = Float.fromInt(amountIn) / Float.fromInt(10 ** sellDecimals);
      let buyHuman = Float.fromInt(q.expectedBuyAmount) / Float.fromInt(10 ** buyDecimals);
      let price = if (sellHuman > 0.0) { buyHuman / sellHuman } else { 0.0 };

      // priceImpact is 0.0-1.0, convert to percentage for SwapAmountsReply
      let slippage = q.priceImpact * 100.0;

      Debug.print("TACO quote: out=" # Nat.toText(q.expectedBuyAmount) # " slip=" # Float.toText(slippage) # "% route=" # q.routeDescription);

      #ok({
        pay_chain = "IC";
        pay_symbol = tokenA;
        pay_address = tokenA;
        pay_amount = amountIn;
        receive_chain = "IC";
        receive_symbol = tokenB;
        receive_address = tokenB;
        receive_amount = q.expectedBuyAmount;
        price = price;
        mid_price = price;
        slippage = slippage;
        txs = [];
      });
    } catch (e) {
      Debug.print("TACO getQuote error: " # Error.message(e));
      #err("TACO quote failed: " # Error.message(e));
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

    let exchangeActor = actor (OTC_BACKEND) : actor {
      getExpectedMultiHopAmount : shared query (Text, Text, Nat) -> async MultiHopQuoteResult;
      swapMultiHop : shared (Text, Text, Nat, [SwapHop], Nat, Nat) -> async Text;
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

    // Step 2: Calculate deposit (includes exchange fee 5bp)
    let exchangeFeeBps : Nat = 5;
    let depositAmount = params.amountIn * (exchangeFeeBps + 10000) / 10000 + params.transferFee;

    // Step 3: Transfer to exchange treasury
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
      return #err("TACO swap call failed: " # Error.message(e));
    };

    Debug.print("TACO swap result: " # swapResult);

    // Step 5: Parse result
    if (Text.startsWith(swapResult, #text "done:")) {
      let received = parseNatFromText(swapResult, 5); // skip "done:"
      let slippage = if (routeResult.expectedAmountOut > 0) {
        Float.abs(1.0 - Float.fromInt(received) / Float.fromInt(routeResult.expectedAmountOut)) * 100.0;
      } else { 0.0 };

      Debug.print("TACO success: received=" # Nat.toText(received) # " slip=" # Float.toText(slippage) # "%");

      #ok({
        amountIn = params.amountIn;
        amountOut = received;
        slippage = slippage;
        route = routeResult.routeTokens;
        blockNumber = blockNumber;
      });
    } else if (Text.contains(swapResult, #text "Warning: slippage")) {
      // Swap executed but with high slippage
      let received = parseNatFromText(swapResult, Text.size("Warning: slippage exceeded but swap executed. Got "));
      #ok({
        amountIn = params.amountIn;
        amountOut = received;
        slippage = 100.0;
        route = routeResult.routeTokens;
        blockNumber = blockNumber;
      });
    } else {
      #err("TACO swap failed: " # swapResult);
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
