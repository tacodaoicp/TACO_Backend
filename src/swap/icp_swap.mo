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
      let result = await (with timeout = 65) factory.getPool(args);

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

      let result = await (with timeout = 65) factory.getPools();

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

      let metadataResult = await (with timeout = 65) pool.metadata();
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
            let poolResult = await (with timeout = 65) factory.getPool({
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
      let balanceFuture = (with timeout = 65) pool.getUserUnusedBalance(selfId);

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

                    let result = await (with timeout = 65) executeWithdraw(selfId, withdrawParams, false);
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

                    let result = await (with timeout = 65) executeWithdraw(selfId, withdrawParams, false);
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
      let balanceResult = await (with timeout = 65) pool.getUserUnusedBalance(selfId);
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
      let result = await (with timeout = 65) pool.swap(swapArgs);
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

  // getQuote - simplified without callback to avoid race conditions
  // Parallel metadata + quote to reduce inter-canister call count
  public func getQuote(
    params : Types.ICPSwapQuoteParams
  ) : async Result.Result<Types.ICPSwapQuoteResult, Text> {
    Debug.print("ICPSWAP_QUOTE: Getting quote for pool " # Principal.toText(params.poolId) # " amountIn=" # Nat.toText(params.amountIn));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(params.poolId) });

      let quoteArgs : Types.SwapArgs = {
        amountIn = Nat.toText(params.amountIn);
        amountOutMinimum = Nat.toText(params.amountOutMinimum);
        zeroForOne = params.zeroForOne;
      };

      // Start both calls in parallel (no await yet) - reduces total inter-canister calls
      let metadataFuture = (with timeout = 65) pool.metadata();
      let quoteFuture = (with timeout = 65) pool.quote(quoteArgs);

      // Await both results
      let (metadataResult, quoteResult)= try{(await metadataFuture, await quoteFuture)
      } catch(e){
        Debug.print("ICPSWAP_QUOTEorMETADATA: Exception during parallel calls: " # Error.message(e));
        return #err("Error during parallel ICPSwap calls" # Error.message(e));
      };

      switch (metadataResult, quoteResult) {
        case (#ok(metadata), #ok(amountOut)) {
          // Calculate spot price from sqrtPriceX96 using proper Nat arithmetic for precision
          // price = (sqrtPriceX96)^2 / 2^192
          let sqrtPriceX96_squared = metadata.sqrtPriceX96 * metadata.sqrtPriceX96;
          let spotPrice = Float.fromInt(sqrtPriceX96_squared) / Float.fromInt(2 ** 192);

          // Calculate effective price from the quote
          let effectivePrice = Float.fromInt(params.amountIn) / Float.fromInt(amountOut);

          // Normalize spot price based on trade direction
          let normalizedSpotPrice = if (params.zeroForOne) {
            if (spotPrice > 0.0) { 1.0 / spotPrice } else { 0.0 };
          } else {
            spotPrice;
          };

          // Calculate slippage as percentage
          let slippage : Float = if (normalizedSpotPrice > 0.0) {
            (effectivePrice - normalizedSpotPrice) / normalizedSpotPrice * 100.0;
          } else {
            0.0;
          };

          Debug.print("ICPSWAP_QUOTE: Success amountOut=" # Nat.toText(amountOut));

          #ok({
            amountOut = amountOut;
            slippage = Float.abs(slippage);
            fee = metadata.fee;
            token0 = metadata.token0;
            token1 = metadata.token1;
          });
        };
        case (#err(e), _) {
          Debug.print("ICPSWAP_QUOTE: Metadata error: " # debug_show(e));
          #err("Error getting pool metadata: " # debug_show(e));
        };
        case (_, #err(e)) {
          Debug.print("ICPSWAP_QUOTE: Quote error: " # debug_show(e));
          #err("Error getting quote: " # debug_show(e));
        };
      };
    } catch (e) {
      Debug.print("ICPSWAP_QUOTE: Exception: " # Error.message(e));
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
      } else { await (with timeout = 65) token.icrc1_transfer(transferArgs) };
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
      let result = await (with timeout = 65) pool.deposit(depositArgs);
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
      let transferResult = await (with timeout = 65) transferToPoolSubaccount(selfId, params);
      Debug.print("ICPSwap.executeTransferAndDeposit: Transfer result: " # debug_show (transferResult));

      switch (transferResult) {
        case (#ok(_)) {
          // Step 2: Register deposit
          Debug.print("ICPSwap.executeTransferAndDeposit: Step 2 - Registering deposit...");
          let depositResult = await (with timeout = 65) registerPoolDeposit(params);
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

  // DEPRECATED: Use executeDepositAndSwapNative instead - this makes 2 calls where native makes 1
  public func executeDepositAndSwap(depositParams : Types.ICPSwapDepositParams, swapParams : Types.ICPSwapParams) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeDepositAndSwap: Starting deposit and swap with params: " # debug_show ({ deposit = depositParams; swap = swapParams }));
    try {
      // Step 1: Register the deposit
      Debug.print("ICPSwap.executeDepositAndSwap: Step 1 - Registering deposit...");
      let depositResult = await (with timeout = 65) registerPoolDeposit(depositParams);
      Debug.print("ICPSwap.executeDepositAndSwap: Deposit result: " # debug_show (depositResult));

      switch (depositResult) {
        case (#ok(_)) {
          // Step 2: Execute the swap
          Debug.print("ICPSwap.executeDepositAndSwap: Step 2 - Executing swap...");
          let swapResult = await (with timeout = 65) executeSwap(swapParams);
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

  // Native ICPSwap depositAndSwap - combines deposit+swap into 1 inter-canister call
  // Saves ~5-8s per trade compared to executeDepositAndSwap which makes 2 calls
  public func executeDepositAndSwapNative(
    poolId : Principal,
    zeroForOne : Bool,
    tokenInFee : Nat,
    tokenOutFee : Nat,
    amountIn : Nat,
    amountOutMinimum : Nat
  ) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeDepositAndSwapNative: Starting with poolId=" # Principal.toText(poolId) #
      " zeroForOne=" # debug_show(zeroForOne) #
      " amountIn=" # Nat.toText(amountIn) #
      " amountOutMinimum=" # Nat.toText(amountOutMinimum));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(poolId) });
      let args : Types.DepositAndSwapArgs = {
        zeroForOne = zeroForOne;
        tokenInFee = tokenInFee;
        tokenOutFee = tokenOutFee;
        amountIn = Nat.toText(amountIn);
        amountOutMinimum = Nat.toText(amountOutMinimum);
      };
      Debug.print("ICPSwap.executeDepositAndSwapNative: Calling pool.depositAndSwap with args: " # debug_show(args));

      let result = await (with timeout = 65) pool.depositAndSwap(args);
      Debug.print("ICPSwap.executeDepositAndSwapNative: Result: " # debug_show(result));

      switch (result) {
        case (#ok(amount)) {
          Debug.print("ICPSwap.executeDepositAndSwapNative: Success, amount out: " # Nat.toText(amount));
          #ok(amount);
        };
        case (#err(e)) {
          Debug.print("ICPSwap.executeDepositAndSwapNative: Error: " # debug_show(e));
          #err("depositAndSwap error: " # debug_show(e));
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeDepositAndSwapNative: Exception: " # Error.message(e));
      #err("Error executing depositAndSwap: " # Error.message(e));
    };
  };

  // tokenOutFee: the transfer fee of the output token (needed for native depositAndSwap)
  public func executeTransferDepositAndSwap(selfId : Principal, depositParams : Types.ICPSwapDepositParams, swapParams : Types.ICPSwapParams, tokenOutFee : Nat) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeTransferDepositAndSwap: Starting transfer, deposit and swap with params: " # debug_show ({ deposit = depositParams; swap = swapParams; tokenOutFee = tokenOutFee }));
    try {
      // Step 1: Transfer tokens to pool subaccount
      Debug.print("ICPSwap.executeTransferDepositAndSwap: Step 1 - Transferring tokens...");
      let transferResult = await (with timeout = 65) transferToPoolSubaccount(selfId, depositParams);
      Debug.print("ICPSwap.executeTransferDepositAndSwap: Transfer result: " # debug_show (transferResult));

      switch (transferResult) {
        case (#ok(_)) {
          // Step 2: Combined deposit and swap using native ICPSwap function (saves ~5-8s)
          // IMPORTANT: depositAndSwap adds tokenInFee to amountIn internally (amountInDeposit = amountIn + feeIn)
          // So we must pass the NET amount (depositParams.amount - depositParams.fee) as amountIn
          // Otherwise ICPSwap tries to transfer more than what's in the subaccount
          let netAmountIn = if (depositParams.amount > depositParams.fee) {
            depositParams.amount - depositParams.fee
          } else { 0 };
          Debug.print("ICPSwap.executeTransferDepositAndSwap: Step 2 - Executing native depositAndSwap with netAmountIn=" # Nat.toText(netAmountIn));
          let swapResult = await (with timeout = 65) executeDepositAndSwapNative(
            depositParams.poolId,
            swapParams.zeroForOne,
            depositParams.fee,      // tokenInFee
            tokenOutFee,            // tokenOutFee
            netAmountIn,            // amountIn (NET, depositAndSwap will add fee)
            swapParams.minAmountOut
          );
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

  // skipBalanceCheck: set to true when called right after a swap (we know the balance exists)
  // This saves ~2-3s per trade by avoiding an extra inter-canister call
  public func executeWithdraw(selfId : Principal, params : Types.ICPSwapWithdrawParams, skipBalanceCheck : Bool) : async Result.Result<Nat, Text> {
    Debug.print("ICPSwap.executeWithdraw: Starting withdrawal with params: " # debug_show (params) # " skipBalanceCheck: " # debug_show(skipBalanceCheck));
    try {
      let pool : Types.ICPSwapPool = actor (if test { FACTORY_CANISTER_ID } else { Principal.toText(params.poolId) });

      // Only check balance when needed (recovery operations)
      // Skip for post-swap withdrawals to save ~2-3s
      if (not skipBalanceCheck) {
        let balance = await (with timeout = 65) getBalance(selfId, params.poolId);
        switch (balance) {
          case (#ok(balance)) {
            if (balance.balance0 < params.amount and balance.balance1 < params.amount) {
              Debug.print("ICPSwap.executeWithdraw: Insufficient balance in pool! Balance: " # debug_show (balance) # " Amount: " # debug_show (params.amount));
            };
          };
          case (#err(e)) {
            Debug.print("ICPSwap.executeWithdraw: Error getting balance: " # e);
          };
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
      let result = await (with timeout = 65) pool.withdraw(withdrawArgs);
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

  public func recoverBalanceFromSpecificPool(
    selfId : Principal,
    poolId : Principal,
    tokens : [Principal],
    tokenDetailsMap : Map.Map<Principal, dao_types.TokenDetails>,
  ) : async* () {
    Debug.print("Starting targeted balance recovery from pool: " # Principal.toText(poolId));

    try {
      // Direct call to the specific pool canister
      let pool : Types.ICPSwapPool = actor (Principal.toText(poolId));
      let balanceResult = await (with timeout = 65) pool.getUserUnusedBalance(selfId);

      switch (balanceResult) {
        case (#ok(balance)) {
          Debug.print("Pool balance check - Balance0: " # Nat.toText(balance.balance0) # " Balance1: " # Nat.toText(balance.balance1));

          // Track recovered amounts
          let recoveredBalances = Map.new<Principal, Nat>();
          let { phash } = Map;

          // Recover token0 if there's a balance
          if (balance.balance0 > 0 and tokens.size() >= 1) {
            let token0 = tokens[0];
            switch (Map.get(tokenDetailsMap, phash, token0)) {
              case (?tokenDetails) {
                if (balance.balance0 > tokenDetails.tokenTransferFee) {
                  let withdrawAmount = balance.balance0;
                  let withdrawParams : Types.ICPSwapWithdrawParams = {
                    poolId = poolId;
                    token = token0;
                    amount = withdrawAmount;
                    fee = tokenDetails.tokenTransferFee;
                  };

                  Debug.print("Attempting to recover " # Nat.toText(withdrawAmount) # " of token0: " # Principal.toText(token0));
                  let withdrawResult = await (with timeout = 65) executeWithdraw(selfId, withdrawParams, false);

                  switch (withdrawResult) {
                    case (#ok(_)) {
                      let recovered = withdrawAmount - tokenDetails.tokenTransferFee;
                      Map.set(recoveredBalances, phash, token0, recovered);
                      Debug.print("Successfully recovered " # Nat.toText(recovered) # " of token0");
                    };
                    case (#err(e)) {
                      Debug.print("Failed to recover token0 balance: " # e);
                    };
                  };
                } else {
                  Debug.print("Token0 balance too small to withdraw (below fee): " # Nat.toText(balance.balance0));
                };
              };
              case (null) {
                Debug.print("Token0 details not found in tokenDetailsMap: " # Principal.toText(token0));
              };
            };
          };

          // Recover token1 if there's a balance
          if (balance.balance1 > 0 and tokens.size() >= 2) {
            let token1 = tokens[1];
            switch (Map.get(tokenDetailsMap, phash, token1)) {
              case (?tokenDetails) {
                if (balance.balance1 > tokenDetails.tokenTransferFee) {
                  let withdrawAmount = balance.balance1;
                  let withdrawParams : Types.ICPSwapWithdrawParams = {
                    poolId = poolId;
                    token = token1;
                    amount = withdrawAmount;
                    fee = tokenDetails.tokenTransferFee;
                  };

                  Debug.print("Attempting to recover " # Nat.toText(withdrawAmount) # " of token1: " # Principal.toText(token1));
                  let withdrawResult = await (with timeout = 65) executeWithdraw(selfId, withdrawParams, false);

                  switch (withdrawResult) {
                    case (#ok(_)) {
                      let recovered = withdrawAmount - tokenDetails.tokenTransferFee;
                      Map.set(recoveredBalances, phash, token1, recovered);
                      Debug.print("Successfully recovered " # Nat.toText(recovered) # " of token1");
                    };
                    case (#err(e)) {
                      Debug.print("Failed to recover token1 balance: " # e);
                    };
                  };
                } else {
                  Debug.print("Token1 balance too small to withdraw (below fee): " # Nat.toText(balance.balance1));
                };
              };
              case (null) {
                Debug.print("Token1 details not found in tokenDetailsMap: " # Principal.toText(token1));
              };
            };
          };

          // Log recovery summary
          let totalRecovered = Map.size(recoveredBalances);
          if (totalRecovered > 0) {
            Debug.print("Recovery complete - Successfully recovered balances for " # Nat.toText(totalRecovered) # " tokens from pool " # Principal.toText(poolId));
          } else {
            Debug.print("Recovery complete - No balances recovered from pool " # Principal.toText(poolId));
          };

        };
        case (#err(e)) {
          Debug.print("Error getting balance from pool " # Principal.toText(poolId) # ": " # e);
        };
      };
    } catch (e) {
      Debug.print("Exception during targeted recovery from pool " # Principal.toText(poolId) # ": " # Error.message(e));
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
      let receivedToken = withdrawParams.token;

      // Get token metadata for fee FIRST - we need tokenOutFee for the native depositAndSwap
      let metadataResult = Map.get(tokenDetails, phash, receivedToken);

      switch (metadataResult) {
        case (?metadata) {
          let tokenOutFee = metadata.tokenTransferFee;

          // Step 1: Execute transfer, deposit and swap using native function (saves ~5-8s)
          Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Step 1 - Executing transfer, deposit and swap with tokenOutFee=" # Nat.toText(tokenOutFee));
          let swapResult = await (with timeout = 65) executeTransferDepositAndSwap(selfId, depositParams, swapParams, tokenOutFee);
          Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Transfer, deposit and swap result: " # debug_show (swapResult));

          switch (swapResult) {
            case (#ok(swapAmount)) {
              // NOTE: depositAndSwap automatically queues withdrawal via _enqueueWithdraw
              // We do NOT need to call executeWithdraw - tokens are already being sent to us
              // The swapAmount returned is pre-fee, actual received = swapAmount - tokenOutFee
              Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: depositAndSwap succeeded with swapAmount=" # Nat.toText(swapAmount) # " (auto-withdrawal queued by ICPSwap)");

              let receivedAmount = if (swapAmount > tokenOutFee) {
                swapAmount - tokenOutFee
              } else { 0 };

              #ok({
                swapAmount = swapAmount;      // Pre-fee amount for price calculation
                receivedAmount = receivedAmount;  // Post-fee amount actually received
              });
            };
            case (#err(e)) {
              Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Error in transfer, deposit and swap: " # e);
              #err(e);
            };
          };
        };
        case (null) {
          Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Error getting token metadata for " # Principal.toText(receivedToken));
          #err("Error getting token metadata for output token");
        };
      };
    } catch (e) {
      Debug.print("ICPSwap.executeTransferDepositSwapAndWithdraw: Exception: " # Error.message(e));
      #err("Error executing combined operation: " # Error.message(e));
    };
  };
};
