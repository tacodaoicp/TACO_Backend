import Map "mo:map/Map";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Text "mo:base/Text";
import Principal "mo:base/Principal";
import Timer "mo:base/Timer";
import swaptypes "../src/swap/swap_types";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Vector "mo:vector";

actor MockICPSwap {
  let { thash; phash } = Map;

  // Store pool data
  type PoolData = {
    key : Text;
    canisterId : Principal;
    token0 : swaptypes.Token;
    token1 : swaptypes.Token;
    fee : Nat;
    liquidity : Nat;
    sqrtPriceX96 : Nat;
    tick : Int;
  };

  // Store balances per user and token
  type Balance = {
    balance0 : Nat;
    balance1 : Nat;
  };

  // Pool and balance storage

  let balanceStore = Map.new<Text, Balance>(); // key: principal + pool
  let poolsByCanister = Map.new<Principal, PoolData>();

  private let poolsByPair = Map.new<Text, Principal>();

  private let poolMetadataStore = Map.new<Principal, swaptypes.PoolMetadata>();
  private let poolDataStore = Map.new<Principal, swaptypes.PoolData>();

  // Helper to create balance key
  private func makeBalanceKey(user : Principal, poolId : Principal) : Text {
    Principal.toText(user) # "-" # Principal.toText(poolId);
  };

  // Initialize mock pools
  // Initialize with realistic pool data
  public shared func initializeMockPools() : async () {
    Debug.print("MockICPSwap.initializeMockPools: Initializing mock pools");

    let ICP = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
    let ICRCA = Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai");
    let ICRCB = Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai");
    let TACO = Principal.fromText("csyra-haaaa-aaaaq-aacva-cai");
    let CKUSDC = Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai");

    // Create pools
    await addPool(
      Principal.fromText("aaaaa-aa"), // ICP/USDC pool
      ICP,
      CKUSDC,
      20.0,
      1_000_000_000_000,
    );

    await addPool(
      Principal.fromText("2ipq2-uqaaa-aaaar-qailq-cai"), // ICP/ICRCA pool
      ICP,
      ICRCA,
      5.0,
      800_000_000_000,
    );

    await addPool(
      Principal.fromText("t6s4t-dzahw-w2cqi-csimw-vn4xf-fjte4-doj3g-gyjxt-6v2uk-cjeft-eae"), // ICP/ICRCB pool
      ICP,
      ICRCB,
      2.0,
      600_000_000_000,
    );

    await addPool(
      Principal.fromText("btn47-pp4lk-4qn3s-s753g-tqhrn-j6zen-hnxpr-4lvws-2c46n-ddmvn-nqe"), // USDC/ICRCA pool
      CKUSDC,
      ICRCA,
      0.25,
      500_000_000_000,
    );

    await addPool(
      Principal.fromText("npyks-khhf5-dcgjq-jkuj2-szk7v-hkjya-urhbc-ruzvl-pwfl4-363sw-2ae"), // USDC/TACO pool
      CKUSDC,
      TACO,
      5.0,
      400_000_000_000,
    );

    Debug.print("MockICPSwap.initializeMockPools: Successfully initialized pools");
  };

  // Helper to convert a desired price to sqrtPriceX96
  private func priceToSqrtPriceX96(price : Float) : Nat {
    let sqrtPrice = Float.sqrt(price);
    let two96 = Float.pow(2.0, 96.0);
    let sqrtPriceX96 = sqrtPrice * two96;
    let result = Int.abs(Float.toInt(sqrtPriceX96));

    Debug.print(
      "priceToSqrtPriceX96: " #
      "Input price: " # Float.toText(price) # ", " #
      "Square root: " # Float.toText(sqrtPrice) # ", " #
      "Result: " # Nat.toText(result)
    );

    result;
  };

  // Helper to get pool by token pair (for debugging)
  public shared query func getPoolByPair(token0 : Principal, token1 : Principal) : async ?Principal {
    let pairKey = getPairKey(token0, token1);
    Map.get(poolsByPair, thash, pairKey);
  };

  // Helper method to update pool metadata (e.g., after trades)
  public shared func updatePoolMetadata(
    poolId : Principal,
    newSqrtPriceX96 : Nat,
    newLiquidity : Nat,
  ) : async () {
    switch (Map.get(poolMetadataStore, phash, poolId)) {
      case (?metadata) {
        let updatedMetadata : swaptypes.PoolMetadata = {
          metadata with
          sqrtPriceX96 = newSqrtPriceX96;
          liquidity = newLiquidity;
        };
        Map.set(poolMetadataStore, phash, poolId, updatedMetadata);

        // Update this canister's pool data if it matches
        if (poolId == Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai")) {
          Map.set(poolMetadataStore, phash, Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai"), updatedMetadata);
        };
      };
      case null {
        Debug.print("Warning: Attempted to update non-existent pool: " # Principal.toText(poolId));
      };
    };
  };

  // Helper to get all pool metadata (for debugging)
  public shared query func getAllPoolMetadata() : async [(Principal, swaptypes.PoolMetadata)] {
    Iter.toArray(Map.entries(poolMetadataStore));
  };

  // Get pools by canister
  public shared query func getPoolsByCanister(canisterId : Principal) : async Result.Result<[swaptypes.PoolData], Text> {
    Debug.print("MockICPSwap.getPoolsByCanister: Called for canister " # Principal.toText(canisterId));
    let result = Vector.new<swaptypes.PoolData>();

    for ((id, data) in Map.entries(poolDataStore)) {
      Vector.add(result, data);
    };

    Debug.print("MockICPSwap.getPoolsByCanister: Returning " # Nat.toText(Vector.size(result)) # " pools");
    #ok(Vector.toArray(result));
  };

  public shared func addPool(
    poolId : Principal,
    token0 : Principal,
    token1 : Principal,
    price : Float,
    liquidity : Nat,
  ) : async () {
    let pairKey = getPairKey(token0, token1);
    Map.set(poolsByPair, thash, pairKey, poolId);

    let poolData : swaptypes.PoolData = {
      key = Principal.toText(poolId);
      token0 = {
        address = Principal.toText(token0);
        standard = "ICRC1";
      };
      token1 = {
        address = Principal.toText(token1);
        standard = "ICRC1";
      };
      fee = 3000;
      tickSpacing = 60;
      canisterId = poolId;
    };

    let sqrtPriceX96 = priceToSqrtPriceX96(price);
    Debug.print("Setting sqrtPriceX96 to " # debug_show (sqrtPriceX96) # " for price " # Float.toText(price));

    let poolMetadata : swaptypes.PoolMetadata = {
      fee = 3000;
      key = Principal.toText(poolId);
      liquidity = liquidity;
      maxLiquidityPerTick = liquidity / 10;
      nextPositionId = 1;
      sqrtPriceX96 = sqrtPriceX96;
      tick = 0;
      token0 = {
        address = Principal.toText(token0);
        standard = "ICRC1";
      };
      token1 = {
        address = Principal.toText(token1);
        standard = "ICRC1";
      };
    };

    Map.set(poolDataStore, phash, poolId, poolData);
    Map.set(poolMetadataStore, phash, poolId, poolMetadata);
    Map.set(poolsByCanister, phash, poolId, { liquidity = liquidity; sqrtPriceX96 = sqrtPriceX96; tick = 0; canisterId = poolId; fee = 3000; key = Principal.toText(poolId); token0 = { address = Principal.toText(token0); standard = "ICRC1" }; token1 = { address = Principal.toText(token1); standard = "ICRC1" }; tickSpacing : Int = 60 });

    if (Map.size(poolMetadataStore) == 1) {
      Map.set(poolMetadataStore, phash, Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai"), poolMetadata);
      Map.set(poolDataStore, phash, Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai"), poolData);
    };
  };

  // Get pool by tokens
  public shared query func getPool(args : swaptypes.GetPoolArgs) : async Result.Result<swaptypes.PoolData, Text> {
    Debug.print("MockICPSwap.getPool: Called with tokens " # args.token0.address # " and " # args.token1.address);

    let token0Principal = Principal.fromText(args.token0.address);
    let token1Principal = Principal.fromText(args.token1.address);
    let pairKey = getPairKey(token0Principal, token1Principal);

    Debug.print("MockICPSwap.getPool: Looking up pair key: " # pairKey);

    switch (Map.get(poolsByPair, thash, pairKey)) {
      case (?poolId) {
        Debug.print("MockICPSwap.getPool: Found pool ID: " # Principal.toText(poolId));
        switch (Map.get(poolDataStore, phash, poolId)) {
          case (?poolData) {
            Debug.print("MockICPSwap.getPool: Found pool data");
            #ok(poolData);
          };
          case null {
            Debug.print("MockICPSwap.getPool: No pool data found");
            #err("Pool data not found");
          };
        };
      };
      case null {
        Debug.print("MockICPSwap.getPool: No pool found for pair");
        #err("Pool not found for token pair");
      };
    };
  };

  // Regular metadata call
  public shared query func metadata() : async Result.Result<swaptypes.PoolMetadata, Text> {
    Debug.print("MockICPSwap.metadata: Called for self");
    switch (Map.get(poolMetadataStore, phash, Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai"))) {
      case (?metadata) {
        Debug.print("MockICPSwap.metadata: Found metadata for self");
        #ok(metadata);
      };
      case null {
        Debug.print("MockICPSwap.metadata: No metadata found for self");
        #err("Pool not found");
      };
    };
  };
  // Helper to generate consistent pair key
  private func getPairKey(token0 : Principal, token1 : Principal) : Text {
    let (first, second) = if (Principal.toText(token0) < Principal.toText(token1)) {
      (token0, token1);
    } else { (token1, token0) };
    Principal.toText(first) # ":" # Principal.toText(second);
  };

  // Extended metadata call for specific pool
  public shared query func metadata2(poolId : Principal) : async Result.Result<swaptypes.PoolMetadata, Text> {
    Debug.print("MockICPSwap.metadata2: Called for pool " # Principal.toText(poolId));
    switch (Map.get(poolMetadataStore, phash, poolId)) {
      case (?metadata) {
        Debug.print("MockICPSwap.metadata2: Found metadata for pool");
        #ok(metadata);
      };
      case null {
        Debug.print("MockICPSwap.metadata2: No metadata found for pool");
        #err("Pool not found for ID: " # Principal.toText(poolId));
      };
    };
  };

  // Get quote for swap
  public shared query func quote(args : swaptypes.SwapArgs) : async Result.Result<Nat, Text> {
    let caller = Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai");

    switch (Map.get(poolsByCanister, phash, caller)) {
      case (?pool) {
        let amountIn = switch (Nat.fromText(args.amountIn)) {
          case (?amount) { amount };
          case null { 0 };
        };
        let price = Float.fromInt(pool.sqrtPriceX96 * pool.sqrtPriceX96) / Float.fromInt(2 ** 192);

        // Calculate output amount including fee
        let amountOut = if (args.zeroForOne) {
          Float.toInt(Float.fromInt(amountIn) * price * 0.997) // 0.3% fee
        } else {
          Float.toInt(Float.fromInt(amountIn) / price * 0.997) // 0.3% fee
        };

        #ok(
          Int.abs(amountOut)
        );
      };
      case null { #err("Pool not found") };
    };
  };

  public shared query func quote2(args : swaptypes.SwapArgs, poolId : Principal) : async Result.Result<Nat, Text> {
    Debug.print("MockICPSwap.quote: Called for pool " # Principal.toText(poolId));

    switch (Map.get(poolMetadataStore, phash, poolId)) {
      case (?pool) {
        let amountIn = switch (Nat.fromText(args.amountIn)) {
          case (?amount) { amount };
          case null { 0 };
        };

        let price = Float.fromInt(pool.sqrtPriceX96 * pool.sqrtPriceX96) / Float.fromInt(2 ** 192);
        // Calculate output amount including fee
        let amountOut = if (args.zeroForOne) {
          Float.toInt(Float.fromInt(amountIn) * price * 0.997);
        } else {
          Float.toInt(Float.fromInt(amountIn) / price * 0.997);
        };

        #ok(Int.abs(amountOut));
      };
      case null {
        Debug.print("MockICPSwap.quote: Pool not found for id " # Principal.toText(poolId));
        #err("Pool not found");
      };
    };
  };
  // Execute swap
  public shared func swap(args : swaptypes.SwapArgs) : async Result.Result<Nat, Text> {
    let caller = Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai");

    switch (Map.get(poolsByCanister, phash, caller)) {
      case (?pool) {
        let amountIn = switch (Nat.fromText(args.amountIn)) {
          case (?amount) { amount };
          case null { 0 };
        };
        let price = Float.fromInt(pool.sqrtPriceX96 * pool.sqrtPriceX96) / Float.fromInt(2 ** 192);

        // Calculate output amount with fee
        let amountOut = if (args.zeroForOne) {
          Float.toInt(Float.fromInt(amountIn) * price * 0.997);
        } else {
          Float.toInt(Float.fromInt(amountIn) / price * 0.997);
        };

        // Update pool liquidity
        Map.set(
          poolsByCanister,
          phash,
          caller,
          {
            pool with
            liquidity = if (args.zeroForOne) {
              if (pool.liquidity + amountIn > Int.abs(amountOut)) {
                pool.liquidity + amountIn - Int.abs(amountOut);
              } else {
                pool.liquidity;
              };
            } else {
              if (pool.liquidity + Int.abs(amountOut) > amountIn) {
                pool.liquidity + Int.abs(amountOut) - amountIn;
              } else {
                pool.liquidity;
              };
            };
          },
        );

        // Update user balances
        let userBalanceKey = makeBalanceKey(Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai"), caller);
        let userBalance = switch (Map.get(balanceStore, thash, userBalanceKey)) {
          case (?b) { b };
          case null { { balance0 = 0; balance1 = 0 } };
        };

        // Update balances based on swap direction
        Map.set(
          balanceStore,
          thash,
          userBalanceKey,
          if (args.zeroForOne) {
            {
              balance0 = if (userBalance.balance0 > amountIn) {
                userBalance.balance0 - amountIn;
              } else { 0 };
              balance1 = userBalance.balance1 + Int.abs(amountOut);
            };
          } else {
            {
              balance0 = userBalance.balance0 + Int.abs(amountOut);
              balance1 = if (userBalance.balance1 > amountIn) {
                userBalance.balance1 - amountIn;
              } else { 0 };
            };
          },
        );

        #ok(Int.abs(amountOut));
      };
      case null { #err("Pool not found") };
    };
  };

  public shared func swap2(poolId : Principal, args : swaptypes.SwapArgs) : async Result.Result<Nat, Text> {
    let caller = Principal.fromText(Principal.toText(poolId));

    switch (Map.get(poolsByCanister, phash, caller)) {
      case (?pool) {
        let amountIn = switch (Nat.fromText(args.amountIn)) {
          case (?amount) { amount };
          case null { 0 };
        };
        let price = Float.fromInt(pool.sqrtPriceX96 * pool.sqrtPriceX96) / Float.fromInt(2 ** 192);

        // Calculate output amount with fee
        let amountOut = if (args.zeroForOne) {
          Float.toInt(Float.fromInt(amountIn) * price * 0.997);
        } else {
          Float.toInt(Float.fromInt(amountIn) / price * 0.997);
        };

        // Update pool liquidity
        Map.set(
          poolsByCanister,
          phash,
          caller,
          {
            pool with
            liquidity = if (args.zeroForOne) {
              if (pool.liquidity + amountIn > Int.abs(amountOut)) {
                pool.liquidity + amountIn - Int.abs(amountOut);
              } else {
                pool.liquidity;
              };
            } else {
              if (pool.liquidity + Int.abs(amountOut) > amountIn) {
                pool.liquidity + Int.abs(amountOut) - amountIn;
              } else {
                pool.liquidity;
              };
            };
          },
        );

        // Update user balances
        let userBalanceKey = makeBalanceKey(Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai"), caller);
        let userBalance = switch (Map.get(balanceStore, thash, userBalanceKey)) {
          case (?b) { b };
          case null { { balance0 = 0; balance1 = 0 } };
        };

        // Update balances based on swap direction
        Map.set(
          balanceStore,
          thash,
          userBalanceKey,
          if (args.zeroForOne) {
            {
              balance0 = if (userBalance.balance0 > amountIn) {
                userBalance.balance0 - amountIn;
              } else { 0 };
              balance1 = userBalance.balance1 + Int.abs(amountOut);
            };
          } else {
            {
              balance0 = userBalance.balance0 + Int.abs(amountOut);
              balance1 = if (userBalance.balance1 > amountIn) {
                userBalance.balance1 - amountIn;
              } else { 0 };
            };
          },
        );

        #ok(Int.abs(amountOut));
      };
      case null { #err("Pool not found") };
    };
  };

  // Get user unused balance
  public shared query func getUserUnusedBalance(user : Principal) : async Result.Result<swaptypes.ICPSwapBalance, Text> {
    let caller = Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai");
    let balanceKey = makeBalanceKey(user, caller);

    switch (Map.get(balanceStore, thash, balanceKey)) {
      case (?balance) {
        #ok({
          balance0 = balance.balance0;
          balance1 = balance.balance1;
        });
      };
      case null {
        #ok({
          balance0 = 0;
          balance1 = 0;
        });
      };
    };
  };

  // Register deposit
  public shared func deposit2(poolId : Principal, args : swaptypes.DepositArgs) : async Result.Result<Nat, Text> {
    Debug.print("MockICPSwap.deposit2: Starting deposit for pool " # Principal.toText(poolId) # " with args: " # debug_show (args));

    let user = Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai"); // Treasury principal
    let balanceKey = makeBalanceKey(user, poolId);

    Debug.print("MockICPSwap.deposit2: Checking pool metadata");

    switch (Map.get(poolMetadataStore, phash, poolId)) {
      case (?pool) {
        Debug.print("MockICPSwap.deposit2: Found pool metadata. Token0: " # pool.token0.address # ", Token1: " # pool.token1.address);

        let currentBalance = switch (Map.get(balanceStore, thash, balanceKey)) {
          case (?b) {
            Debug.print("MockICPSwap.deposit2: Current balance found - Balance0: " # Nat.toText(b.balance0) # ", Balance1: " # Nat.toText(b.balance1));
            b;
          };
          case null {
            Debug.print("MockICPSwap.deposit2: No existing balance found, initializing with zeros");
            { balance0 = 0; balance1 = 0 };
          };
        };

        // Determine which token is being deposited and update balance
        if (args.token == pool.token0.address) {
          Debug.print("MockICPSwap.deposit2: Updating token0 balance. Adding amount: " # Nat.toText(args.amount));

          let newBalance = {
            balance0 = currentBalance.balance0 + args.amount;
            balance1 = currentBalance.balance1;
          };

          Map.set(balanceStore, thash, balanceKey, newBalance);
          Debug.print("MockICPSwap.deposit2: Updated token0 balance to: " # Nat.toText(newBalance.balance0));
        } else if (args.token == pool.token1.address) {
          Debug.print("MockICPSwap.deposit2: Updating token1 balance. Adding amount: " # Nat.toText(args.amount));

          let newBalance = {
            balance0 = currentBalance.balance0;
            balance1 = currentBalance.balance1 + args.amount;
          };

          Map.set(balanceStore, thash, balanceKey, newBalance);
          Debug.print("MockICPSwap.deposit2: Updated token1 balance to: " # Nat.toText(newBalance.balance1));
        } else {
          Debug.print("MockICPSwap.deposit2: Error - Token address does not match either pool token");
          return #err("Invalid token address for this pool");
        };

        Debug.print("MockICPSwap.deposit2: Deposit completed successfully");
        #ok(1);
      };
      case null {
        Debug.print("MockICPSwap.deposit2: Error - Pool not found with ID: " # Principal.toText(poolId));
        #err("Pool not found");
      };
    };
  };

  // Process withdrawal
  public shared func withdraw(args : swaptypes.WithdrawArgs) : async Result.Result<Nat, Text> {
    let caller = Principal.fromText("4mmnk-kiaaa-aaaag-qbllq-cai");
    let user = Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai"); // Treasury principal
    let balanceKey = makeBalanceKey(user, caller);

    switch (Map.get(poolsByCanister, phash, caller)) {
      case (?pool) {
        switch (Map.get(balanceStore, thash, balanceKey)) {
          case (?balance) {
            if (args.token == pool.token0.address and balance.balance0 >= args.amount) {
              Map.set(
                balanceStore,
                thash,
                balanceKey,
                {
                  balance0 = if (balance.balance0 > args.amount) {
                    balance.balance0 - args.amount;
                  } else { 0 };
                  balance1 = balance.balance1;
                },
              );
              #ok(args.amount);
            } else if (args.token == pool.token1.address and balance.balance1 >= args.amount) {
              Map.set(
                balanceStore,
                thash,
                balanceKey,
                {
                  balance0 = balance.balance0;
                  balance1 = if (balance.balance1 > args.amount) {
                    balance.balance1 - args.amount;
                  } else { 0 };
                },
              );
              #ok(args.amount);
            } else {
              Map.set(
                balanceStore,
                thash,
                balanceKey,
                {
                  balance0 = balance.balance0;
                  balance1 = if (balance.balance1 > args.amount) {
                    balance.balance1 - args.amount;
                  } else { 0 };
                },
              );
              #ok(args.amount);
            };
          };
          case null {
            Map.set(
              balanceStore,
              thash,
              balanceKey,
              {
                balance0 = 10 ** 18;
                balance1 = 10 ** 18;
              },
            );
            #ok(10 ** 18);
          };
        };
      };
      case null { #err("Pool not found") };
    };
  };

  public shared func withdraw2(poolId : Principal, args : swaptypes.WithdrawArgs) : async Result.Result<Nat, Text> {
    let caller = Principal.fromText(Principal.toText(poolId));
    let user = Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai"); // Treasury principal
    let balanceKey = makeBalanceKey(user, caller);

    switch (Map.get(poolsByCanister, phash, caller)) {
      case (?pool) {
        switch (Map.get(balanceStore, thash, balanceKey)) {
          case (?balance) {
            if (args.token == pool.token0.address and balance.balance0 >= args.amount) {
              Map.set(
                balanceStore,
                thash,
                balanceKey,
                {
                  balance0 = if (balance.balance0 > args.amount) {
                    balance.balance0 - args.amount;
                  } else { 0 };
                  balance1 = balance.balance1;
                },
              );
              #ok(args.amount);
            } else if (args.token == pool.token1.address and balance.balance1 >= args.amount) {
              Map.set(
                balanceStore,
                thash,
                balanceKey,
                {
                  balance0 = balance.balance0;
                  balance1 = if (balance.balance1 > args.amount) {
                    balance.balance1 - args.amount;
                  } else { 0 };
                },
              );
              #ok(args.amount);
            } else {
              Map.set(
                balanceStore,
                thash,
                balanceKey,
                {
                  balance0 = balance.balance0;
                  balance1 = if (balance.balance1 > args.amount) {
                    balance.balance1 - args.amount;
                  } else { 0 };
                },
              );
              #ok(args.amount);
            };
          };
          case null { #err("No balance found") };
        };
      };
      case null { #err("Pool not found") };
    };
  };

  // In mock_icpswap.mo
  public shared query func getPools() : async {
    #ok : [swaptypes.PoolData];
    #err : Text;
  } {
    Debug.print("MockICPSwap.getPools: Called");

    let result = Vector.new<swaptypes.PoolData>();
    for ((_, data) in Map.entries(poolDataStore)) {
      Vector.add(result, data);
    };

    Debug.print("MockICPSwap.getPools: Returning " # Nat.toText(Vector.size(result)) # " pools");
    #ok(Vector.toArray(result));
  };

  // Initialize pools when canister is deployed
  Timer.setTimer<system>(
    #nanoseconds(1),
    func() : async () {
      await initializeMockPools();
    },
  );
};
