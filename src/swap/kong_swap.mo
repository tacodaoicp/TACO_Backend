import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Time "mo:base/Time";
import Error "mo:base/Error";
import Float "mo:base/Float";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Nat64 "mo:base/Nat64";
import Array "mo:base/Array";
import Text "mo:base/Text";
import Debug "mo:base/Debug";
import Types "./swap_types";
import Map "mo:map/Map";

module {
  // Kong Swap canister ID
  private let KONG_CANISTER_ID = "2ipq2-uqaaa-aaaar-qailq-cai";
  private let test = false;
  let { nhash } = Map;

  // Get price from Kong Swap
  public func getPrice(tokenA : Text, tokenB : Text) : async Result.Result<Types.KongSwapPriceInfo, Text> {
    Debug.print("KongSwap.getPrice: Getting price for pair " # tokenA # "/" # tokenB);
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);

      // Get pool information using query call
      // Use the non-ICP token for the query as ICP pairs are too numerous
      var searchToken = if (tokenA == "ICP") tokenB else tokenA;
      searchToken := "IC." # searchToken;

      let poolsResult = await (with timeout = 65) kong.pools(?searchToken);

      switch (poolsResult) {
        case (#Ok(poolsReply)) {
          // Find the pool for this token pair
          let pool = Array.find<Types.PoolReply>(
            poolsReply.pools,
            func(p) = (p.symbol_0 == tokenA and p.symbol_1 == tokenB) or
            (p.symbol_1 == tokenA and p.symbol_0 == tokenB),
          );

          switch (pool) {
            case (?p) {
              Debug.print("KongSwap.getPrice: Pool found");
              Debug.print("KongSwap.getPrice: Pool price: " # debug_show (p.price));
              #ok({
                tokenA = tokenA;
                tokenB = tokenB;
                price = p.price;
                timestamp = Time.now();
                fee = Nat8.toNat(p.lp_fee_bps);
                liquidity = p.tvl;
                midPrice = p.price; // Kong uses mid price
                slippage = 0.0; // Will be calculated during swap_amounts
              });
            };
            case (null) {
              Debug.print("KongSwap.getPrice: Pool not found for the pair: " # tokenA # "/" # tokenB);
              #err("Pool not found for token pair: " # tokenA # "/" # tokenB);
            };
          };
        };
        case (#Err(e)) {
          Debug.print("KongSwap.getPrice: Error getting pool information");
          #err("Error getting pool information: " # e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.getPrice: Exception");
      #err("Error calling Kong Swap: " # Error.message(e));
    };
  };

  // Get quote for a swap
  // sellDecimals and buyDecimals are needed to normalize slippage calculation
  // (mid_price is in human units, but amountIn/receive_amount are in raw token units)
  public func getQuote(tokenA : Text, tokenB : Text, amountIn : Nat, sellDecimals : Nat, buyDecimals : Nat) : async Result.Result<Types.SwapAmountsReply, Text> {
    Debug.print("KongSwap.getQuote: Getting quote for " # debug_show (amountIn) # " " # tokenA # " to " # tokenB);
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);
      let result = await (with timeout = 65) kong.swap_amounts("IC." # tokenA, amountIn, "IC." # tokenB);

      switch (result) {
        case (#Ok(quote)) {
          Debug.print("KongSwap.getQuote: Quote received successfully of tokenA: " # tokenA # " to tokenB: " # tokenB);
          Debug.print("KongSwap.getQuote: Original quote: " # debug_show (quote));

          // Calculate our own slippage using mid_price (spot price) vs actual quote
          // IMPORTANT: mid_price is in human units (buyToken per sellToken)
          // but amountIn and receive_amount are in raw token units (e8, e18, etc.)
          // We must normalize to human units before calculating slippage
          let calculatedSlippage = if (quote.mid_price > 0.0) {
            // Normalize amountIn to human units
            let sellDecimalsFactor = Float.pow(10.0, Float.fromInt(sellDecimals));
            let buyDecimalsFactor = Float.pow(10.0, Float.fromInt(buyDecimals));

            let amountInHuman = Float.fromInt(amountIn) / sellDecimalsFactor;
            let actualAmountOutHuman = Float.fromInt(quote.receive_amount) / buyDecimalsFactor;

            // Calculate what we should get at spot price (mid_price)
            let spotAmountOut = amountInHuman * quote.mid_price;

            Debug.print("KongSwap.getQuote: amountInHuman=" # Float.toText(amountInHuman) #
                       ", spotAmountOut=" # Float.toText(spotAmountOut) #
                       ", actualAmountOutHuman=" # Float.toText(actualAmountOutHuman));

            // Slippage = (spot_amount - actual_amount) / spot_amount * 100
            if (spotAmountOut > actualAmountOutHuman) {
              (spotAmountOut - actualAmountOutHuman) / spotAmountOut * 100.0;
            } else {
              0.0; // No slippage if we got more than expected
            };
          } else {
            quote.slippage; // Fallback to their calculation if mid_price is unavailable
          };

          Debug.print("KongSwap.getQuote: Calculated slippage: " # Float.toText(calculatedSlippage) # "% vs Kong's reported: " # Float.toText(quote.slippage) # "%");

          // Return quote with our calculated slippage
          let correctedQuote = {
            quote with
            slippage = calculatedSlippage;
          };

          #ok(correctedQuote);
        };
        case (#Err(e)) {
          Debug.print("KongSwap.getQuote: Error getting swap quote for tokenA: " # tokenA # " to tokenB: " # tokenB);
          #err("Error getting swap quote: " # e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.getQuote: Exception");
      #err("Error calling Kong Swap: " # Error.message(e));
    };
  };

  // Get all pending claims for a principal (for recovering tokens from failed swaps)
  public func getPendingClaims(treasuryPrincipal : Principal) : async Result.Result<[Types.ClaimsReply], Text> {
    Debug.print("KongSwap.getPendingClaims: Getting claims for " # Principal.toText(treasuryPrincipal));
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);
      let result = await (with timeout = 65) kong.claims(Principal.toText(treasuryPrincipal));

      switch (result) {
        case (#Ok(claims)) {
          Debug.print("KongSwap.getPendingClaims: Found " # Nat.toText(claims.size()) # " claims");
          #ok(claims);
        };
        case (#Err(e)) {
          Debug.print("KongSwap.getPendingClaims: Error getting claims: " # e);
          #err("Error getting claims: " # e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.getPendingClaims: Exception");
      #err("Error calling Kong Swap: " # Error.message(e));
    };
  };

  // Execute a claim to recover tokens from a failed swap
  public func executeClaim(claimId : Nat64) : async Result.Result<Types.ClaimReply, Text> {
    Debug.print("KongSwap.executeClaim: Executing claim " # Nat64.toText(claimId));
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);
      let result = await (with timeout = 65) kong.claim(claimId);

      switch (result) {
        case (#Ok(reply)) {
          Debug.print("KongSwap.executeClaim: Claim successful - " # reply.symbol # " amount=" # Nat.toText(reply.amount));
          #ok(reply);
        };
        case (#Err(e)) {
          Debug.print("KongSwap.executeClaim: Error executing claim: " # e);
          #err("Error executing claim: " # e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.executeClaim: Exception");
      #err("Error calling Kong Swap: " # Error.message(e));
    };
  };

  // Execute swap on Kong Swap
  public func executeSwap(params : Types.KongSwapParams) : async Result.Result<Types.SwapReply, Text> {
    Debug.print("KongSwap.executeSwap: Starting swap with params: " # debug_show (params));
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);

      let swapArgs : Types.KongSwapArgs = {
        pay_token = "IC." # params.token0_symbol; // Kong expects just the symbol
        pay_amount = params.amountIn;
        pay_tx_id = switch (params.txId) {
          case (?id) { ? #BlockIndex(id) };
          case (null) { null };
        };
        receive_token = "IC." # params.token1_symbol; // Kong expects just the symbol
        receive_amount = null; // Let Kong calculate the receive amount
        receive_address = params.recipient;
        max_slippage = ?params.slippageTolerance; // Hardcode 100% slippage
        referred_by = null; // No referral code
      };
      Debug.print("KongSwap.executeSwap: Prepared swap args: " # debug_show (swapArgs));

      // Execute swap
      Debug.print("KongSwap.executeSwap: Executing swap...");
      let result = await (with timeout = 65) kong.swap(swapArgs);
      Debug.print("KongSwap.executeSwap: Swap result: " # debug_show (result));

      switch (result) {
        case (#Ok(reply)) {
          Debug.print("KongSwap.executeSwap: Swap executed successfully");
          #ok(reply);
        };
        case (#Err(e)) {
          Debug.print("KongSwap.executeSwap: Error executing swap");
          #err(e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.executeSwap: Exception");
      #err("Error calling Kong Swap: " # Error.message(e));
    };
  };

  // Execute async swap on Kong Swap
  public func executeSwapAsync(params : Types.KongSwapParams) : async Result.Result<Nat64, Text> {
    Debug.print("KongSwap.executeSwapAsync: Starting async swap with params: " # debug_show (params));
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);

      // Prepare swap arguments
      let swapArgs : Types.KongSwapArgs = {
        pay_token = "IC." # params.token0_symbol;
        pay_amount = params.amountIn;
        pay_tx_id = switch (params.txId) {
          case (?id) { ? #BlockIndex(id) };
          case (null) { null };
        };
        receive_token = "IC." # params.token1_symbol;
        receive_amount = ?params.minAmountOut;
        receive_address = null; // Use default (our canister)
        max_slippage = ?1.0; // Allow 100% slippage if user specified minAmountOut
        referred_by = null; // No referral code
      };
      Debug.print("KongSwap.executeSwapAsync: Prepared swap args: " # debug_show (swapArgs));

      // Execute async swap
      Debug.print("KongSwap.executeSwapAsync: Executing async swap...");
      let result = await (with timeout = 65) kong.swap_async(swapArgs);
      Debug.print("KongSwap.executeSwapAsync: Async swap result: " # debug_show (result));

      switch (result) {
        case (#Ok(requestId)) {
          Debug.print("KongSwap.executeSwapAsync: Async swap executed successfully");
          #ok(requestId);
        };
        case (#Err(e)) {
          Debug.print("KongSwap.executeSwapAsync: Error executing async swap");
          #err("Error executing async swap: " # e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.executeSwapAsync: Exception");
      #err("Error calling Kong Swap: " # Error.message(e));
    };
  };

  // Get request status from Kong Swap
  public func getRequestStatus(requestId : Nat64) : async Result.Result<Types.RequestsReply, Text> {
    Debug.print("KongSwap.getRequestStatus: Getting request status for " # Nat64.toText(requestId));
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);
      let result = await (with timeout = 65) kong.requests(?requestId);

      switch (result) {
        case (#Ok(requests)) {
          Debug.print("KongSwap.getRequestStatus: Request found");
          switch (requests.size()) {
            case (0) {
              Debug.print("KongSwap.getRequestStatus: Request not found");
              #err("Request not found: " # Nat64.toText(requestId));
            };
            case (_) {
              Debug.print("KongSwap.getRequestStatus: Request found");
              #ok(requests[0]);
            };
          };
        };
        case (#Err(e)) {
          Debug.print("KongSwap.getRequestStatus: Error getting request status");
          #err("Error getting request status: " # e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.getRequestStatus: Exception");
      #err("Error calling Kong Swap: " # Error.message(e));
    };
  };

  // Execute transfer and swap in one operation
  // Kong automatically creates claims for failed swaps - no local tracking needed
  public func executeTransferAndSwap(
    params : Types.KongSwapParams,
  ) : async Result.Result<Types.SwapReply, Text> {
    Debug.print("KongSwap.executeTransferAndSwap: Starting with params: " # debug_show (params));
    try {
      // Step 1: Execute transfer
      Debug.print("KongSwap.executeTransferAndSwap: Step 1 - Executing ICRC1 transfer...");
      let transferResult = await (with timeout = 65) executeICRC1Transfer(params.token0_ledger, params.amountIn);
      Debug.print("KongSwap.executeTransferAndSwap: Transfer result: " # debug_show (transferResult));

      switch (transferResult) {
        case (#ok(blockIndex)) {
          Debug.print("KongSwap.executeTransferAndSwap: Transfer successful with blockIndex: " # Nat.toText(blockIndex));

          // Step 2: Try to execute the swap
          Debug.print("KongSwap.executeTransferAndSwap: Step 2 - Executing swap...");
          let swapParams = {
            params with
            txId = ?blockIndex
          };
          let swapResult = await (with timeout = 65) executeSwap(swapParams);
          Debug.print("KongSwap.executeTransferAndSwap: Swap result: " # debug_show (swapResult));

          switch (swapResult) {
            case (#ok(reply)) {
              Debug.print("KongSwap.executeTransferAndSwap: Swap successful");
              #ok(reply);
            };
            case (#err(e)) {
              // Swap failed after transfer - Kong automatically creates a claim
              // Tokens will be recovered via recoverKongswapClaims()
              Debug.print("KongSwap.executeTransferAndSwap: Swap failed - Kong will create claim for blockIndex=" # Nat.toText(blockIndex));
              #err("Swap failed (tokens held by Kong, will be claimed): " # e);
            };
          };
        };
        case (#err(e)) {
          Debug.print("KongSwap.executeTransferAndSwap: Transfer failed: " # e);
          #err("Transfer failed: " # e);
        };
      };
    } catch (e) {
      Debug.print("KongSwap.executeTransferAndSwap: Exception: " # Error.message(e));
      #err("Error in executeTransferAndSwap: " # Error.message(e));
    };
  };

  // Execute transfer and swap - Kong automatically creates claims for failed swaps
  // Used for parallel split trades
  public type SplitTradeResult = {
    #ok : Types.SwapReply;
    #err : Text;
  };

  public func executeTransferAndSwapNoTracking(
    params : Types.KongSwapParams,
  ) : async SplitTradeResult {
    Debug.print("KongSwap.executeTransferAndSwapNoTracking: Starting");
    try {
      // Step 1: Execute transfer
      let transferResult = await (with timeout = 65) executeICRC1Transfer(params.token0_ledger, params.amountIn);

      switch (transferResult) {
        case (#ok(blockIndex)) {
          // Step 2: Execute swap
          let swapParams = { params with txId = ?blockIndex };
          let swapResult = await (with timeout = 65) executeSwap(swapParams);

          switch (swapResult) {
            case (#ok(reply)) { #ok(reply) };
            case (#err(e)) {
              // Swap failed after transfer - Kong automatically creates a claim
              // Tokens will be recovered via recoverKongswapClaims()
              Debug.print("KongSwap.executeTransferAndSwapNoTracking: Swap failed after transfer - Kong will create claim for blockIndex=" # Nat.toText(blockIndex));
              #err("Swap failed (tokens held by Kong, will be claimed): " # e);
            };
          };
        };
        case (#err(e)) { #err("Transfer failed: " # e) };
      };
    } catch (e) {
      #err("Error: " # Error.message(e));
    };
  };

  // Helper function to execute ICRC1 transfer
  private func executeICRC1Transfer(tokenId : Principal, amount : Nat) : async Result.Result<Nat, Text> {
    Debug.print("KongSwap.executeICRC1Transfer: Starting transfer of " # debug_show (amount) # " tokens from " # Principal.toText(tokenId));
    try {
      let tokenCanister : Types.ICRC1 = actor (Principal.toText(tokenId));
      let transferArgs : Types.ICRC1TransferArgs = {
        to = {
          owner = Principal.fromText(KONG_CANISTER_ID);
          subaccount = null;
        };
        fee = null; // Let the token determine the fee
        memo = null;
        from_subaccount = null;
        created_at_time = null;
        amount = amount;
      };
      Debug.print("KongSwap.executeICRC1Transfer: Prepared transfer args: " # debug_show (transferArgs));

      Debug.print("KongSwap.executeICRC1Transfer: Executing transfer...");
      let result : { #Err : Types.ICRC1TransferError; #Ok : Nat } = if (test) {
        #Ok(10);
      } else { await (with timeout = 65) tokenCanister.icrc1_transfer(transferArgs) };
      Debug.print("KongSwap.executeICRC1Transfer: Transfer result: " # debug_show (result));

      switch (result) {
        case (#Ok(blockIndex)) {
          Debug.print("KongSwap.executeICRC1Transfer: Transfer successful with block index: " # debug_show (blockIndex));
          #ok(blockIndex);
        };
        case (#Err(e)) {
          Debug.print("KongSwap.executeICRC1Transfer: Transfer failed with error: " # debug_show (e));
          #err("Transfer failed: " # debug_show (e));
        };
      };
    } catch (e) {
      Debug.print("KongSwap.executeICRC1Transfer: Exception: " # Error.message(e));
      #err("Error executing transfer: " # Error.message(e));
    };
  };
};
