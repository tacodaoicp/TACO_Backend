import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Time "mo:base/Time";
import Error "mo:base/Error";
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
      let searchToken = if (tokenA == "ICP") tokenB else tokenA;
      let poolsResult = await kong.pools(?searchToken);

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
  public func getQuote(tokenA : Text, tokenB : Text, amountIn : Nat) : async Result.Result<Types.SwapAmountsReply, Text> {
    Debug.print("KongSwap.getQuote: Getting quote for " # debug_show (amountIn) # " " # tokenA # " to " # tokenB);
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);
      let result = await kong.swap_amounts(tokenA, amountIn, tokenB);

      switch (result) {
        case (#Ok(quote)) {
          Debug.print("KongSwap.getQuote: Quote received successfully of tokenA: " # tokenA # " to tokenB: " # tokenB);
          Debug.print("KongSwap.getQuote: Quote: " # debug_show (quote));
          #ok(quote);
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

  // Execute swap on Kong Swap
  public func executeSwap(params : Types.KongSwapParams) : async Result.Result<Types.SwapReply, Text> {
    Debug.print("KongSwap.executeSwap: Starting swap with params: " # debug_show (params));
    try {
      let kong : Types.KongSwap = actor (KONG_CANISTER_ID);

      let swapArgs : Types.KongSwapArgs = {
        pay_token = params.token0_symbol; // Kong expects just the symbol
        pay_amount = params.amountIn;
        pay_tx_id = switch (params.txId) {
          case (?id) { ? #BlockIndex(id) };
          case (null) { null };
        };
        receive_token = params.token1_symbol; // Kong expects just the symbol
        receive_amount = null; // Let Kong calculate the receive amount
        receive_address = params.recipient;
        max_slippage = ?params.slippageTolerance; // Hardcode 100% slippage
        referred_by = null; // No referral code
      };
      Debug.print("KongSwap.executeSwap: Prepared swap args: " # debug_show (swapArgs));

      // Execute swap
      Debug.print("KongSwap.executeSwap: Executing swap...");
      let result = await kong.swap(swapArgs);
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
        pay_token = params.token0_symbol;
        pay_amount = params.amountIn;
        pay_tx_id = switch (params.txId) {
          case (?id) { ? #BlockIndex(id) };
          case (null) { null };
        };
        receive_token = params.token1_symbol;
        receive_amount = ?params.minAmountOut;
        receive_address = null; // Use default (our canister)
        max_slippage = ?1.0; // Allow 100% slippage if user specified minAmountOut
        referred_by = null; // No referral code
      };
      Debug.print("KongSwap.executeSwapAsync: Prepared swap args: " # debug_show (swapArgs));

      // Execute async swap
      Debug.print("KongSwap.executeSwapAsync: Executing async swap...");
      let result = await kong.swap_async(swapArgs);
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
      let result = await kong.requests(?requestId);

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
  public func executeTransferAndSwap(
    params : Types.KongSwapParams,
    pendingTxs : Map.Map<Nat, Types.SwapTxRecord>,
  ) : async Result.Result<Types.SwapReply, Text> {
    Debug.print("KongSwap.executeTransferAndSwap: Starting with params: " # debug_show (params));
    try {
      // Step 1: Execute transfer
      Debug.print("KongSwap.executeTransferAndSwap: Step 1 - Executing ICRC1 transfer...");
      let transferResult = await executeICRC1Transfer(params.token0_ledger, params.amountIn);
      Debug.print("KongSwap.executeTransferAndSwap: Transfer result: " # debug_show (transferResult));

      switch (transferResult) {
        case (#ok(blockIndex)) {
          Debug.print("KongSwap.executeTransferAndSwap: Transfer successful, recording pending swap...");
          // Record the pending swap
          let record : Types.SwapTxRecord = {
            txId = blockIndex;
            token0_ledger = params.token0_ledger;
            token0_symbol = params.token0_symbol;
            token1_ledger = params.token1_ledger;
            token1_symbol = params.token1_symbol;
            amount = params.amountIn;
            minAmountOut = params.minAmountOut;
            lastAttempt = Time.now();
            attempts = 1;
            status = #SwapPending;
          };
          Map.set(pendingTxs, nhash, blockIndex, record);
          Debug.print("KongSwap.executeTransferAndSwap: Recorded pending swap with txId: " # debug_show (blockIndex));

          // Step 2: Try to execute the swap
          Debug.print("KongSwap.executeTransferAndSwap: Step 2 - Executing swap...");
          let swapParams = {
            params with
            txId = ?blockIndex
          };
          Debug.print("KongSwap.executeTransferAndSwap: Updated params with txId: " # debug_show (blockIndex));
          let swapResult = await executeSwap(swapParams);
          Debug.print("KongSwap.executeTransferAndSwap: Swap result: " # debug_show (swapResult));

          switch (swapResult) {
            case (#ok(reply)) {
              Debug.print("KongSwap.executeTransferAndSwap: Swap successful, removing from pending...");
              // Update record status and remove from pending
              Map.delete(pendingTxs, nhash, blockIndex);
              #ok(reply);
            };
            case (#err(e)) {
              Debug.print("KongSwap.executeTransferAndSwap: Swap failed: " # e);
              // Update record status but keep in pending for retry
              switch (Map.get(pendingTxs, nhash, blockIndex)) {
                case (?record) {
                  let updatedRecord = {
                    record with
                    status = #SwapFailed(e);
                    lastAttempt = Time.now();
                  };
                  Map.set(pendingTxs, nhash, blockIndex, updatedRecord);
                  Debug.print("KongSwap.executeTransferAndSwap: Updated pending record with failure status");
                };
                case null {
                  Debug.print("KongSwap.executeTransferAndSwap: Warning - Could not find pending record to update");
                };
              };
              #err(e);
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
      } else { await tokenCanister.icrc1_transfer(transferArgs) };
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

  // Retry a failed transaction
  public func retryTransaction(
    txId : Nat,
    pendingTxs : Map.Map<Nat, Types.SwapTxRecord>,
    failedTxs : Map.Map<Nat, Types.SwapTxRecord>,
    maxAttempts : Nat,
  ) : async Result.Result<(), Text> {
    try {
      let record = switch (Map.get(pendingTxs, nhash, txId)) {
        case (?r) { ?r };
        case null { Map.get(failedTxs, nhash, txId) };
      };

      switch (record) {
        case (?r) {
          // Move back to pending if it was in failed
          if (Map.get(failedTxs, nhash, txId) != null) {
            Map.delete(failedTxs, nhash, txId);
            Map.set(pendingTxs, nhash, txId, r);
          };

          // Reconstruct params from record
          let params : Types.KongSwapParams = {
            token0_ledger = r.token0_ledger;
            token0_symbol = r.token0_symbol;
            token1_ledger = r.token1_ledger;
            token1_symbol = r.token1_symbol;
            amountIn = r.amount;
            minAmountOut = r.minAmountOut;
            deadline = null;
            recipient = null;
            txId = ?txId;
            slippageTolerance = 0.5; // Default value
          };

          // Try swap again
          let swapResult = await executeSwap(params);

          switch (swapResult) {
            case (#ok(_)) {
              Map.delete(pendingTxs, nhash, txId);
              #ok();
            };
            case (#err(e)) {
              let updatedRecord = {
                r with
                status = #SwapFailed(e);
                lastAttempt = Time.now();
                attempts = r.attempts + 1;
              };
              if (updatedRecord.attempts < maxAttempts) {
                Map.set(pendingTxs, nhash, txId, updatedRecord);
              } else {
                Map.set(failedTxs, nhash, txId, updatedRecord);
                Map.delete(pendingTxs, nhash, txId);
              };
              #err(e);
            };
          };
        };
        case null {
          #err("Transaction not found");
        };
      };
    } catch (e) {
      #err("Error in retryTransaction: " # Error.message(e));
    };
  };

  // Abandon a pending transaction
  public func abandonTransaction(
    txId : Nat,
    pendingTxs : Map.Map<Nat, Types.SwapTxRecord>,
    failedTxs : Map.Map<Nat, Types.SwapTxRecord>,
  ) : Result.Result<(), Text> {
    switch (Map.get(pendingTxs, nhash, txId)) {
      case (?record) {
        Map.delete(pendingTxs, nhash, txId);
        Map.set(failedTxs, nhash, txId, record);
        #ok();
      };
      case null {
        #err("Transaction not found in pending list");
      };
    };
  };
};
