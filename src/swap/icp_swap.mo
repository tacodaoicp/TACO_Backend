import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Time "mo:base/Time";
import Error "mo:base/Error";
import Float "mo:base/Float";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Debug "mo:base/Debug";
import Types "./swap_types";
import Utils "./utils";
import Map "mo:map/Map";
import Vector "mo:vector";
import dao_types "../DAO_backend/dao_types";

module {
  // ICPSwap Factory canister ID
  private let FACTORY_CANISTER_ID = "4mmnk-kiaaa-aaaag-qbllq-cai";
  private let DEFAULT_FEE = 3000; // 0.3%

  private let test = false;
  private let phash = Map.phash;

  public func getPoolByTokens(token0Principal : Principal, token1Principal : Principal) : async Result.Result<Types.PoolData, Text> {
    Debug.print("ICPSwap.getPoolByTokens: Getting pool for tokens " # Principal.toText(token0Principal) # " and " # Principal.toText(token1Principal));
    try {
      let factory : Types.SwapFactory = actor (FACTORY_CANISTER_ID);

      // Create token objects
      let token0 : Types.Token = {
        address = Principal.toText(token0Principal);
        standard = "ICRC1"; // Default to ICRC1, could be made dynamic if needed
      };
      let token1 : Types.Token = {
        address = Principal.toText(token1Principal);
        standard = "ICRC1"; // Default to ICRC1, could be made dynamic if needed
      };

      // Prepare getPool args
      let args : Types.GetPoolArgs = {
        token0 = token0;
        token1 = token1;
        fee = DEFAULT_FEE;
      };

      Debug.print("ICPSwap.getPoolByTokens: Calling factory.getPool with args: " # debug_show (args));
      let result = await factory.getPool(args);

      switch (result) {
        case (#ok(poolData)) {
          Debug.print("ICPSwap.getPoolByTokens: Found pool: " # Principal.toText(poolData.canisterId));
          #ok(poolData);
        };
        case (#err(e)) {
          Debug.print("ICPSwap.getPoolByTokens: Error getting pool: " # e);
          #err("Pool not found for token pair");
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.getPoolByTokens: Exception: " # Error.message(e));
      #err("Error calling ICPSwap Factory: " # Error.message(e));
    };
  };

  public func getAllPools() : async Result.Result<[Types.PoolData], Text> {
    Debug.print("ICPSwap.getAllPools: Starting");
    try {
      let factory : actor {
        getPools : shared query () -> async {
          #ok : [Types.PoolData];
          #err : Text;
        };
      } = actor (FACTORY_CANISTER_ID);

      let result = await factory.getPools();

      switch (result) {
        case (#ok(pools)) {
          Debug.print("ICPSwap.getAllPools: Found " # Nat.toText(pools.size()) # " pools");
          #ok(pools);
        };
        case (#err(e)) {
          Debug.print("ICPSwap.getAllPools: Error getting pools: " # e);
          #err(e);
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.getAllPools: Exception: " # Error.message(e));
      #err("Error calling ICPSwap: " # Error.message(e));
    };
  };

  public func getPrice(poolId : Principal) : async Result.Result<Types.ICPSwapPriceInfo, Text> {
    Debug.print("ICPSwap.getPrice: Getting price for pool " # Principal.toText(poolId));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(poolId) });

      // Get pool metadata
      Debug.print("ICPSwap.getPrice: Fetching pool metadata...");

      let metadataResult = await pool.metadata();
      Debug.print("ICPSwap.getPrice: Metadata result: " # debug_show (metadataResult));

      switch (metadataResult) {
        case (#ok(metadata)) {
          // Calculate price from sqrtPriceX96 using proper arithmetic
          // price = (sqrtPriceX96)^2 / 2^192
          // This represents the price of token1 in terms of token0
          let sqrtPriceX96_squared = metadata.sqrtPriceX96 * metadata.sqrtPriceX96;
          let price = Float.fromInt(sqrtPriceX96_squared) / Float.fromInt(2 ** 192);
          Debug.print("ICPSwap.getPrice: Calculated price: " # debug_show (price));

          #ok({
            price = price;
            timestamp = Time.now();
            fee = metadata.fee;
            liquidity = metadata.liquidity;
            priceX96 = metadata.sqrtPriceX96;
            tick = metadata.tick;
            token0 = metadata.token0;
            token1 = metadata.token1;
          });
        };
        case (#err(e)) {
          Debug.print("ICPSwap.getPrice: Error getting pool metadata: " # debug_show (e));
          #err("Error getting pool metadata: " # debug_show (e));
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.getPrice: Exception: " # Error.message(e));
      #err("Error calling ICPSwap: " # Error.message(e));
    };
  };

  public func recoverBalancesFromPools(
    selfId : Principal,
    poolsWithData : [(Principal, Principal, ?Types.PoolData)],
    tokenDetailsMap : Map.Map<Principal, dao_types.TokenDetails>,
  ) : async* () {

    let poolPairs = Vector.new<(Principal, [Principal])>();

    // Process pools - use provided pool data if available, query if not
    for ((token0, token1, poolDataOpt) in poolsWithData.vals()) {
      switch (poolDataOpt) {
        case (?poolData) {
          // Use the provided pool data directly
          let token0Principal = Principal.fromText(poolData.token0.address);
          let token1Principal = Principal.fromText(poolData.token1.address);
          Vector.add(poolPairs, (poolData.canisterId, [token0Principal, token1Principal]));
        };
        case (null) {
          // Need to query - fall back to old behavior
          try {
            let factory : Types.SwapFactory = actor (FACTORY_CANISTER_ID);
            let poolResult = await factory.getPool({
              token0 = {
                address = Principal.toText(token0);
                standard = if (token0 != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) "ICRC1" else "ICP";
              };
              token1 = {
                address = Principal.toText(token1);
                standard = if (token1 != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) "ICRC1" else "ICP";
              };
              fee = DEFAULT_FEE;
            });

            switch (poolResult) {
              case (#ok(poolData)) {
                let token0Principal = Principal.fromText(poolData.token0.address);
                let token1Principal = Principal.fromText(poolData.token1.address);
                Vector.add(poolPairs, (poolData.canisterId, [token0Principal, token1Principal]));
              };
              case (#err(_)) {}; // Skip if pool doesn't exist
            };
          } catch (e) {
            Debug.print(
              "Error checking pool for tokens " # Principal.toText(token0) #
              " and " # Principal.toText(token1) # ": " # Error.message(e)
            );
          };
        };
      };
    };

    Debug.print("Starting balance recovery from ICPSwap pools...");

    // Track total amounts recovered
    let recoveredBalances = Map.new<Principal, Nat>();
    let { phash } = Map;

    // Launch all balance checks in parallel
    let balanceChecks = Vector.new<(Principal, [Principal], async Result.Result<Types.ICPSwapBalance, Text>)>();

    // Initiate parallel balance queries directly to pool canisters
    for ((poolId, tokens) in Vector.vals(poolPairs)) {
      Debug.print("Launching balance check for pool: " # Principal.toText(poolId));

      // Direct call to the pool canister
      let pool : Types.ICPSwapPool = actor (Principal.toText(poolId));
      let balanceFuture = pool.getUserUnusedBalance(selfId);

      Vector.add(balanceChecks, (poolId, tokens, balanceFuture));
    };

    // Process each pool sequentially, but with parallel metadata queries
    for ((poolId, tokens, balanceFuture) in Vector.vals(balanceChecks)) {
      try {
        let balanceResult = await balanceFuture;

        switch (balanceResult) {
          case (#ok(balance)) {
            // Process token0 if balance exists
            if (balance.balance0 > 0 and tokens.size() > 0) {
              let token0 = tokens[0];
              Debug.print("Processing token0 withdrawal...");

              let metadataResult = Map.get(tokenDetailsMap, phash, token0);

              switch (metadataResult) {
                case (?metadata) {
                  if (metadata.tokenTransferFee < balance.balance0) {
                    let withdrawParams : Types.ICPSwapWithdrawParams = {
                      poolId = poolId;
                      token = token0;
                      amount = balance.balance0;
                      fee = metadata.tokenTransferFee;
                    };

                    let result = await executeWithdraw(selfId, withdrawParams);
                    switch (result) {
                      case (#ok(_tx_id)) {
                        let amountOut = balance.balance0 - metadata.tokenTransferFee;
                        Debug.print("Successfully withdrew " # Nat.toText(amountOut) # " of token0");
                        let currentAmount = switch (Map.get(recoveredBalances, phash, token0)) {
                          case (?existing) { existing + amountOut };
                          case null { amountOut };
                        };
                        Map.set(recoveredBalances, phash, token0, currentAmount);
                      };
                      case (#err(e)) {
                        Debug.print("Failed to withdraw token0: " # e);
                      };
                    };

                  };
                };
                case (_) {
                  Debug.print("Failed to get token0 metadata from map");
                };
              };
            };

            // Process token1 if balance exists
            if (balance.balance1 > 0 and tokens.size() > 1) {
              let token1 = tokens[1];
              Debug.print("Processing token1 withdrawal...");

              let metadataResult = Map.get(tokenDetailsMap, phash, token1);
              switch (metadataResult) {
                case (?metadata) {
                  if (metadata.tokenTransferFee < balance.balance1) {
                    let withdrawParams : Types.ICPSwapWithdrawParams = {
                      poolId = poolId;
                      token = token1;
                      amount = balance.balance1;
                      fee = metadata.tokenTransferFee;
                    };

                    let result = await executeWithdraw(selfId, withdrawParams);
                    switch (result) {
                      case (#ok(_tx_id)) {
                        let amountOut = balance.balance1 - metadata.tokenTransferFee;
                        Debug.print("Successfully withdrew " # Nat.toText(amountOut) # " of token1");
                        let currentAmount = switch (Map.get(recoveredBalances, phash, token1)) {
                          case (?existing) { existing + amountOut };
                          case null { amountOut };
                        };
                        Map.set(recoveredBalances, phash, token1, currentAmount);
                      };
                      case (#err(e)) {
                        Debug.print("Failed to withdraw token1: " # e);
                      };
                    };
                  };
                };
                case (_) {
                  Debug.print("Failed to get token1 metadata from map");
                };
              };
            };
          };
          case (#err(e)) {
            Debug.print("Failed to get balances for pool " # Principal.toText(poolId) # ": " # e);
          };
        };
      } catch (e) {
        Debug.print("Error processing pool " # Principal.toText(poolId) # ": " # Error.message(e));
      };
    };

    // Print summary of recovered amounts
    Debug.print("Balance recovery complete. Summary:");
    for ((token, amount) in Map.entries(recoveredBalances)) {
      Debug.print("Recovered " # Nat.toText(amount) # " from token " # Principal.toText(token));
    };
  };

  public func getBalance(selfId : Principal, poolId : Principal) : async Result.Result<Types.ICPSwapBalance, Text> {
    Debug.print("ICPSwap.getBalance: Getting balance for principal " # Principal.toText(selfId) # " in pool " # Principal.toText(poolId));
    try {
      let pool : Types.ICPSwapPool = actor (Principal.toText(poolId));
      Debug.print("ICPSwap.getBalance: Fetching user unused balance...");
      let balanceResult = await pool.getUserUnusedBalance(selfId);
      Debug.print("ICPSwap.getBalance: Balance result: " # debug_show (balanceResult));

      switch (balanceResult) {
        case (#ok(balance)) {
          #ok({
            balance0 = balance.balance0;
            balance1 = balance.balance1;
          });
        };
        case (#err(e)) {
          Debug.print("ICPSwap.getBalance: Error getting balance: " # e);
          #err("Error getting balance: " # e);
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.getBalance: Exception: " # Error.message(e));
      #err("Error calling ICPSwap: " # Error.message(e));
    };
  };

  public func executeSwap(params : Types.ICPSwapParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeSwap: Starting swap with params: " # debug_show (params));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(params.poolId) });

      // Prepare swap args
      let swapArgs : Types.SwapArgs = {
        amountIn = Nat.toText(params.amountIn);
        amountOutMinimum = Nat.toText(params.minAmountOut);
        zeroForOne = params.zeroForOne;
      };
      Debug.print("ICPSwap.executeSwap: Prepared swap args: " # debug_show (swapArgs));

      // Execute swap
      Debug.print("ICPSwap.executeSwap: Executing swap...");
      let result = await pool.swap(swapArgs);
      Debug.print("ICPSwap.executeSwap: Swap result: " # debug_show (result));

      switch (result) {
        case (#ok(amount)) {
          Debug.print("ICPSwap.executeSwap: Swap successful, amount out: " # debug_show (amount));
          #ok(amount);
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeSwap: Error executing swap: " # debug_show (e));
          #err("Error executing swap: " # debug_show (e));
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeSwap: Exception: " # Error.message(e));
      #err("Error calling ICPSwap: " # Error.message(e));
    };
  };

  public func getQuote(params : Types.ICPSwapQuoteParams) : async Result.Result<Types.ICPSwapQuoteResult, Text> {
    Debug.print("ICPSwap.getQuote: Getting quote with params: " # debug_show (params));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(params.poolId) });

      // Get pool metadata first for fee and token info
      Debug.print("ICPSwap.getQuote: Fetching pool metadata...");
      let metadataResult = await pool.metadata();
      Debug.print("ICPSwap.getQuote: Metadata result: " # debug_show (metadataResult));

      switch (metadataResult) {
        case (#ok(metadata)) {
          // Prepare quote args
          let quoteArgs : Types.SwapArgs = {
            amountIn = Nat.toText(params.amountIn);
            amountOutMinimum = Nat.toText(params.amountOutMinimum);
            zeroForOne = params.zeroForOne;
          };
          Debug.print("ICPSwap.getQuote: Prepared quote args: " # debug_show (quoteArgs));

          // Get quote
          Debug.print("ICPSwap.getQuote: Getting quote...");
          let quoteResult = await pool.quote(quoteArgs);
          Debug.print("ICPSwap.getQuote: Quote result: " # debug_show (quoteResult));

          switch (quoteResult) {
            case (#ok(amountOut)) {
              // Calculate spot price from sqrtPriceX96 using proper Nat arithmetic for precision
              // price = (sqrtPriceX96)^2 / 2^192
              // This represents the price of token1 in terms of token0
              let sqrtPriceX96_squared = metadata.sqrtPriceX96 * metadata.sqrtPriceX96;
              let spotPrice = Float.fromInt(sqrtPriceX96_squared) / Float.fromInt(2 ** 192);

              // Calculate effective price from the quote
              // Effective price = amountIn / amountOut for both directions
              // This gives us how much we're paying per unit received
              let effectivePrice = Float.fromInt(params.amountIn) / Float.fromInt(amountOut);

              // For slippage calculation, we need to compare prices in the same direction
              // The spot price from sqrtPriceX96 represents token1/token0 ratio
              let normalizedSpotPrice = if (params.zeroForOne) {
                // Trading token0 for token1, so we want token0/token1 (inverse of spot price)
                if (spotPrice > 0.0) { 1.0 / spotPrice } else { 0.0 };
              } else {
                // Trading token1 for token0, so we want token1/token0 (same as spot price)
                spotPrice;
              };

              // Calculate slippage as percentage
              // Slippage = (effectivePrice - spotPrice) / spotPrice * 100
              // Positive slippage means we're paying more than spot price
              let slippage : Float = if (normalizedSpotPrice > 0.0) {
                (effectivePrice - normalizedSpotPrice) / normalizedSpotPrice * 100.0;
              } else {
                0.0; // Fallback if spot price calculation fails
              };

              #ok({
                amountOut = amountOut;
                slippage = Float.abs(slippage); // Return absolute slippage
                fee = metadata.fee;
                token0 = metadata.token0;
                token1 = metadata.token1;
              });
            };
            case (#err(e)) {
              Debug.print("ICPSwap.getQuote: Error getting quote: " # debug_show (e));
              #err("Error getting quote: " # debug_show (e));
            };
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap.getQuote: Error getting pool metadata: " # debug_show (e));
          #err("Error getting pool metadata: " # debug_show (e));
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.getQuote: Exception: " # Error.message(e));
      #err("Error calling ICPSwap: " # Error.message(e));
    };
  };

  public func transferToPoolSubaccount(selfId : Principal, params : Types.ICPSwapDepositParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.transferToPoolSubaccount: Starting transfer with params: " # debug_show (params));
    try {
      let subaccount = Utils.principalToSubaccount(selfId);
      Debug.print("ICPSwap.transferToPoolSubaccount: Derived subaccount: " # debug_show (subaccount));
      let token : Types.ICRC1 = actor (Principal.toText(params.token));

      let transferArgs : Types.ICRC1TransferArgs = {
        to = {
          owner = params.poolId;
          subaccount = ?subaccount;
        };
        fee = ?params.fee;
        memo = null;
        from_subaccount = null;
        created_at_time = null;
        amount = params.amount;
      };

      Debug.print("ICPSwap.transferToPoolSubaccount: Prepared transfer args: " # debug_show (transferArgs));
      Debug.print("ICPSwap.transferToPoolSubaccount: Executing transfer...");
      let result : { #Err : Types.ICRC1TransferError; #Ok : Nat } = if (test) {
        #Ok(10 ** 18);
      } else { await token.icrc1_transfer(transferArgs) };
      Debug.print("ICPSwap.transferToPoolSubaccount: Transfer result: " # debug_show (result));

      switch (result) {
        case (#Ok(blockIndex)) {
          Debug.print("ICPSwap.transferToPoolSubaccount: Transfer successful, block index: " # debug_show (blockIndex));
          #ok(blockIndex);

        };
        case (#Err(e)) {
          Debug.print("ICPSwap.transferToPoolSubaccount: Transfer error: " # debug_show (e));
          switch (e) {
            case (#InsufficientFunds { balance }) {
              #err("Insufficient funds. Available balance: " # Nat.toText(balance));
            };
            case (#BadFee { expected_fee }) {
              #err("Incorrect fee. Expected: " # Nat.toText(expected_fee));
            };
            case (#TooOld) {
              #err("Transaction too old");
            };
            case (#CreatedInFuture { ledger_time }) {
              #err("Transaction created in future. Ledger time: " # Nat64.toText(ledger_time));
            };
            case (#Duplicate { duplicate_of }) {
              #err("Duplicate transaction of block: " # Nat.toText(duplicate_of));
            };
            case (#TemporarilyUnavailable) {
              #err("Ledger temporarily unavailable");
            };
            case (#GenericError { error_code; message }) {
              #err("Error " # Nat.toText(error_code) # ": " # message);
            };
            case (#BadBurn { min_burn_amount }) {
              #err("Bad burn amount. Minimum: " # Nat.toText(min_burn_amount));
            };
          };
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.transferToPoolSubaccount: Exception: " # Error.message(e));
      #err("Error executing transfer: " # Error.message(e));
    };
  };

  public func registerPoolDeposit(params : Types.ICPSwapDepositParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.registerPoolDeposit: Starting deposit registration with params: " # debug_show (params));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(params.poolId) });
      let depositArgs : Types.DepositArgs = {
        amount = params.amount;
        fee = params.fee;
        token = Principal.toText(params.token);
      };
      Debug.print("ICPSwap.registerPoolDeposit: Prepared deposit args: " # debug_show (depositArgs));

      Debug.print("ICPSwap.registerPoolDeposit: Registering deposit...");
      let result = await pool.deposit(depositArgs);
      Debug.print("ICPSwap.registerPoolDeposit: Deposit result: " # debug_show (result));

      switch (result) {
        case (#ok(index)) {
          Debug.print("ICPSwap.registerPoolDeposit: Deposit registration successful, index: " # debug_show (index));
          #ok(index);
        };
        case (#err(e)) {
          Debug.print("ICPSwap.registerPoolDeposit: Error registering deposit: " # debug_show (e));
          #err("Error registering deposit with pool: " # debug_show (e));
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.registerPoolDeposit: Exception: " # Error.message(e));
      #err("Error registering deposit: " # Error.message(e));
    };
  };

  public func executeTransferAndDeposit(selfId : Principal, params : Types.ICPSwapDepositParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeTransferAndDeposit: Starting deposit execution with params: " # debug_show (params));
    try {
      // Step 1: Transfer tokens
      Debug.print("ICPSwap.executeTransferAndDeposit: Step 1 - Transferring tokens...");
      let transferResult = await transferToPoolSubaccount(selfId, params);
      Debug.print("ICPSwap.executeTransferAndDeposit: Transfer result: " # debug_show (transferResult));

      switch (transferResult) {
        case (#ok(_)) {
          // Step 2: Register deposit
          Debug.print("ICPSwap.executeTransferAndDeposit: Step 2 - Registering deposit...");
          let depositResult = await registerPoolDeposit(params);
          Debug.print("ICPSwap.executeTransferAndDeposit: Deposit result: " # debug_show (depositResult));

          switch (depositResult) {
            case (#ok(index)) {
              Debug.print("ICPSwap.executeTransferAndDeposit: Deposit execution successful, index: " # debug_show (index));
              #ok(index);
            };
            case (#err(e)) {
              Debug.print("ICPSwap.executeTransferAndDeposit: Error in deposit registration: " # e);
              #err(e);
            };
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeTransferAndDeposit: Error in token transfer: " # e);
          #err(e);
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeTransferAndDeposit: Exception: " # Error.message(e));
      #err("Error executing deposit: " # Error.message(e));
    };
  };

  public func executeDepositAndSwap(depositParams : Types.ICPSwapDepositParams, swapParams : Types.ICPSwapParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeDepositAndSwap: Starting deposit and swap with params: " # debug_show ({ deposit = depositParams; swap = swapParams }));
    try {
      // Step 1: Register the deposit
      Debug.print("ICPSwap.executeDepositAndSwap: Step 1 - Registering deposit...");
      let depositResult = await registerPoolDeposit(depositParams);
      Debug.print("ICPSwap.executeDepositAndSwap: Deposit result: " # debug_show (depositResult));

      switch (depositResult) {
        case (#ok(_)) {
          // Step 2: Execute the swap
          Debug.print("ICPSwap.executeDepositAndSwap: Step 2 - Executing swap...");
          let swapResult = await executeSwap(swapParams);
          Debug.print("ICPSwap.executeDepositAndSwap: Swap result: " # debug_show (swapResult));

          switch (swapResult) {
            case (#ok(amount)) {
              Debug.print("ICPSwap.executeDepositAndSwap: Operation successful, amount out: " # debug_show (amount));
              #ok(amount);
            };
            case (#err(e)) {
              Debug.print("ICPSwap.executeDepositAndSwap: Error in swap execution: " # e);
              #err(e);
            };
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeDepositAndSwap: Error in deposit registration: " # e);
          #err(e);
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeDepositAndSwap: Exception: " # Error.message(e));
      #err("Error executing deposit and swap: " # Error.message(e));
    };
  };

  public func executeTransferDepositAndSwap(selfId : Principal, depositParams : Types.ICPSwapDepositParams, swapParams : Types.ICPSwapParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeTransferDepositAndSwap: Starting transfer, deposit and swap with params: " # debug_show ({ deposit = depositParams; swap = swapParams }));
    try {
      // Step 1: Transfer tokens to pool subaccount
      Debug.print("ICPSwap.executeTransferDepositAndSwap: Step 1 - Transferring tokens...");
      let transferResult = await transferToPoolSubaccount(selfId, depositParams);
      Debug.print("ICPSwap.executeTransferDepositAndSwap: Transfer result: " # debug_show (transferResult));

      switch (transferResult) {
        case (#ok(_)) {
          // Step 2: Register deposit and execute swap
          Debug.print("ICPSwap.executeTransferDepositAndSwap: Step 2 - Executing deposit and swap...");
          let swapResult = await executeDepositAndSwap(depositParams, swapParams);
          Debug.print("ICPSwap.executeTransferDepositAndSwap: Deposit and swap result: " # debug_show (swapResult));

          switch (swapResult) {
            case (#ok(amount)) {
              Debug.print("ICPSwap.executeTransferDepositAndSwap: Operation successful, amount out: " # debug_show (amount));
              #ok(amount);
            };
            case (#err(e)) {
              Debug.print("ICPSwap.executeTransferDepositAndSwap: Error in deposit and swap: " # e);
              #err(e);
            };
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeTransferDepositAndSwap: Error in token transfer: " # e);
          #err(e);
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeTransferDepositAndSwap: Exception: " # Error.message(e));
      #err("Error executing transfer, deposit and swap: " # Error.message(e));
    };
  };

  public func executeWithdraw(selfId : Principal, params : Types.ICPSwapWithdrawParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeWithdraw: Starting withdrawal with params: " # debug_show (params));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(params.poolId) });

      let balance = await getBalance(selfId, params.poolId);
      switch (balance) {
        case (#ok(balance)) {
          if (balance.balance0 < params.amount and balance.balance1 < params.amount) {
            Debug.print("ICPSwap.executeWithdraw: Insufficient balance in pool! Balance: " # debug_show (balance) # " Amount: " # debug_show (params.amount));
            //#err("Insufficient balance in pool");
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeWithdraw: Error getting balance: " # e);
          //#err(e);
        };
      };

      // Prepare withdraw args
      let withdrawArgs : Types.WithdrawArgs = {
        amount = params.amount;
        fee = params.fee;
        token = Principal.toText(params.token);
      };
      Debug.print("ICPSwap.executeWithdraw: Prepared withdraw args: " # debug_show (withdrawArgs));

      // Execute withdraw
      Debug.print("ICPSwap.executeWithdraw: Executing withdrawal...");
      let result = await pool.withdraw(withdrawArgs);
      Debug.print("ICPSwap.executeWithdraw: Withdrawal result: " # debug_show (result));

      switch (result) {
        case (#ok(tx_id)) {
          Debug.print("ICPSwap.executeWithdraw: Withdrawal successful, amount: " # debug_show(params.amount) # " tx_id: " # debug_show (tx_id));
          #ok(tx_id);
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeWithdraw: Error executing withdrawal: " # debug_show (e));
          #err("Error executing withdraw: " # debug_show (e));
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeWithdraw: Exception: " # Error.message(e));
      #err("Error calling ICPSwap: " # Error.message(e));
    };
  };

  public func executeTransferDepositSwapAndWithdraw(
    selfId : Principal,
    depositParams : Types.ICPSwapDepositParams,
    swapParams : Types.ICPSwapParams,
    withdrawParams : Types.OptionalWithdrawParams,
    tokenDetails : Map.Map<Principal, dao_types.TokenDetails>,
  ) : async Result.Result<Types.TransferDepositSwapWithdrawResult, Text> {
    Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Starting combined operation with params: " # debug_show ({ deposit = depositParams; swap = swapParams; withdraw = withdrawParams }));
    try {
      // Step 1: Execute transfer, deposit and swap using existing function
      Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Step 1 - Executing transfer, deposit and swap...");
      let swapResult = await executeTransferDepositAndSwap(selfId, depositParams, swapParams);
      Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Transfer, deposit and swap result: " # debug_show (swapResult));

      switch (swapResult) {
        case (#ok(swapAmount)) {
          // Step 2: Withdraw
          Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Step 2 - Executing withdrawal...");

          let receivedToken = withdrawParams.token;

          // Get token metadata for fee
          let metadataResult = Map.get(tokenDetails, phash, receivedToken);

          switch (metadataResult) {
            case (?metadata) {
              // Prepare withdraw parameters
              let withdrawAmount = swapAmount;

              if (withdrawAmount > metadata.tokenTransferFee) {
                let withdrawParamsWithFee : Types.ICPSwapWithdrawParams = {
                  poolId = depositParams.poolId;
                  token = receivedToken;
                  amount = withdrawAmount;
                  fee = metadata.tokenTransferFee;
                };

                let withdrawResult = await executeWithdraw(selfId, withdrawParamsWithFee);
                Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Withdrawal result: " # debug_show (withdrawResult));

                switch (withdrawResult) {
                  case (#ok(_tx_id)) {
                    #ok({
                      swapAmount = withdrawAmount - metadata.tokenTransferFee;
                    });
                  };
                  case (#err(e)) {
                    Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Error in withdrawal: " # e);
                    #err(e);
                  };
                };
              } else {
                #err("Insufficient balance in pool for withdrawal.");
              };
            };
            case (null) {
              Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Error getting token metadata");
              #err("Error getting token metadata");
            };
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Error in transfer, deposit and swap: " # e);
          #err(e);
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Exception: " # Error.message(e));
      #err("Error executing combined operation: " # Error.message(e));
    };
  };
};
