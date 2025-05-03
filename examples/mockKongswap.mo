import Map "mo:map/Map";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Vector "mo:vector";
import { now } = "mo:base/Time";
import swaptypes "../src/swap/swap_types";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";
import Timer "mo:base/Timer";
import Debug "mo:base/Debug";
actor MockKongSwap {
  type TokenReply = swaptypes.TokenReply;
  type PoolReply = swaptypes.PoolReply;
  type PoolsReply = swaptypes.PoolsReply;
  type SwapAmountsReply = swaptypes.SwapAmountsReply;
  type SwapReply = swaptypes.SwapReply;
  type RequestsReply = swaptypes.RequestsReply;
  type KongSwapArgs = swaptypes.KongSwapArgs;

  let { thash; nhash } = Map;

  // Store mock token data
  let tokenStore = Map.new<Text, TokenReply>();
  let poolStore = Map.new<Text, PoolReply>();
  let requestStore = Map.new<Nat, RequestsReply>();

  // Mock liquidity data structure
  let liquidityStore = Map.new<Text, (Nat, Nat)>(); // (token0Balance, token1Balance)
  private let priceStore = Map.new<Text, Float>(); // Direct token pair prices

  // Calculate price impact and fees along a route
  private func calculateRouteImpact(amountIn : Nat, route : [Text]) : ?(Float, Float) {
    var remainingAmount = Float.fromInt(amountIn);
    var totalPriceImpact : Float = 0;
    var totalFees : Float = 0;

    for (i in Iter.range(0, route.size() - 2)) {
      let tokenA = route[i];
      let tokenB = route[i + 1];
      let pairKey = getPairKey(tokenA, tokenB);

      switch (Map.get(liquidityStore, thash, pairKey)) {
        case (?(liq0, liq1)) {
          // Calculate price impact for this hop
          let impactRatio = remainingAmount / Float.fromInt(liq0);
          totalPriceImpact += impactRatio;

          // Apply 0.3% fee
          let fee = remainingAmount * 0.003;
          totalFees += fee;
          remainingAmount -= fee;

          // Convert amount to next token
          switch (Map.get(priceStore, thash, pairKey)) {
            case (?price) {
              let isReversed = tokenA > tokenB;
              remainingAmount *= if (isReversed) 1.0 / price else price;
            };
            case null { return null };
          };
        };
        case null { return null };
      };
    };

    ?(totalPriceImpact * 100.0, totalFees);
  };

  private func getTokenPrice(tokenA : Text, tokenB : Text) : ?(Float, [Text]) {
    Debug.print("KongSwap: Getting price for " # tokenA # " -> " # tokenB);
    let directKey = getPairKey(tokenA, tokenB);

    switch (Map.get(priceStore, thash, directKey)) {
      case (?price) {
        Debug.print("KongSwap: Found direct pair with price " # Float.toText(price));
        let isReversed = tokenA > tokenB;
        let finalPrice = if (isReversed) 1.0 / price else price;
        Debug.print("KongSwap: Final price (after reversal check): " # Float.toText(finalPrice));
        ?(finalPrice, [tokenA, tokenB]);
      };
      case null {
        Debug.print("KongSwap: No direct pair, trying route through ICP");
        if (tokenA != "ICP" and tokenB != "ICP") {
          let keyA = getPairKey(tokenA, "ICP");
          let keyB = getPairKey(tokenB, "ICP");

          switch (Map.get(priceStore, thash, keyA), Map.get(priceStore, thash, keyB)) {
            case (?priceA, ?priceB) {
              Debug.print("KongSwap: Found route through ICP:");
              Debug.print("KongSwap: " # tokenA # "/ICP price: " # Float.toText(priceA));
              Debug.print("KongSwap: ICP/" # tokenB # " price: " # Float.toText(priceB));

              let isReversedA = tokenA > "ICP";
              let isReversedB = "ICP" > tokenB;
              let finalPrice = (if (isReversedA) 1.0 / priceA else priceA) * (if (isReversedB) 1.0 / priceB else priceB);
              Debug.print("KongSwap: Final routed price: " # Float.toText(finalPrice));
              ?(finalPrice, [tokenA, "ICP", tokenB]);
            };
            case _ {
              Debug.print("KongSwap: Could not find complete route through ICP");
              null;
            };
          };
        } else {
          // One token is ICP, try to find direct pair with the other token
          let nonICPToken = if (tokenA == "ICP") tokenB else tokenA;
          let pairKey = getPairKey("ICP", nonICPToken);

          switch (Map.get(priceStore, thash, pairKey)) {
            case (?price) {
              let isReversed = tokenA == "ICP"; // If tokenA is ICP, price needs to be reversed
              ?(if (isReversed) 1.0 / price else price, [tokenA, tokenB]);
            };
            case null { null };
          };
        };
      };
    };
  };
  private func getPairKey(tokenA : Text, tokenB : Text) : Text {
    let (first, second) = if (tokenA < tokenB) { (tokenA, tokenB) } else {
      (tokenB, tokenA);
    };
    // Use consistent key format for both price and pool lookups
    let key = first # "/" # second;
    Debug.print("KongSwap: Generated key " # key # " for tokens " # tokenA # " and " # tokenB);
    key;
  };

  stable var nextRequestId : Nat = 1;

  private func addPair(token0 : Text, token1 : Text, price : Float, liquidity0 : Nat, liquidity1 : Nat) {
    // Ensure consistent pair key generation
    let pairKey = getPairKey(token0, token1);
    Debug.print("KongSwap: Adding pair with key " # pairKey);

    Map.set(priceStore, thash, pairKey, price);
    Map.set(liquidityStore, thash, pairKey, (liquidity0, liquidity1));

    // Create pool data
    let pool : swaptypes.PoolReply = {
      pool_id = 1;
      name = token0 # "/" # token1;
      symbol = token0 # "/" # token1;
      chain_0 = "ICP";
      symbol_0 = token0;
      address_0 = getTokenAddress(token0);
      balance_0 = liquidity0;
      lp_fee_0 = 10000;
      chain_1 = "ICP";
      symbol_1 = token1;
      address_1 = getTokenAddress(token1);
      balance_1 = liquidity1;
      lp_fee_1 = 10000;
      price = price;
      lp_fee_bps = 30; // 0.3%
      tvl = liquidity0;
      rolling_24h_volume = liquidity0 / 10;
      rolling_24h_lp_fee = liquidity0 / 10 * 3 / 1000;
      rolling_24h_num_swaps = 100;
      rolling_24h_apy = 10.5;
      lp_token_symbol = token0 # "-" # token1 # "-LP";
      is_removed = false;
    };

    Map.set(poolStore, thash, pairKey, pool);
  };

  // Helper to get token address
  private func getTokenAddress(symbol : Text) : Text {
    switch (symbol) {
      case "ICP" { "ryjl3-tyaaa-aaaaa-aaaba-cai" };
      case "ICRCA" { "mxzaz-hqaaa-aaaar-qaada-cai" };
      case "ICRCB" { "zxeu2-7aaaa-aaaaq-aaafa-cai" };
      case "TACO" { "csyra-haaaa-aaaaq-aacva-cai" };
      case "CKUSDC" { "xevnm-gaaaa-aaaar-qafnq-cai" };
      case _ { "" };
    };
  };

  // Initialize with some mock data
  public shared func initializeMockData() : async () {
    // Add mock tokens
    let mockICP : TokenReply = #IC({
      token_id = 1;
      chain = "ICP";
      canister_id = "ryjl3-tyaaa-aaaaa-aaaba-cai";
      name = "Internet Computer";
      symbol = "ICP";
      decimals = 8;
      fee = 10000;
      icrc1 = true;
      icrc2 = false;
      icrc3 = false;
      is_removed = false;
    });

    let mockICRCA : TokenReply = #IC({
      token_id = 2;
      chain = "ICP";
      canister_id = "mxzaz-hqaaa-aaaar-qaada-cai";
      name = "ICRC A";
      symbol = "ICRCA";
      decimals = 8;
      fee = 10000;
      icrc1 = true;
      icrc2 = false;
      icrc3 = false;
      is_removed = false;
    });

    let mockICRCB : TokenReply = #IC({
      token_id = 3;
      chain = "ICP";
      canister_id = "zxeu2-7aaaa-aaaaq-aaafa-cai";
      name = "ICRC B";
      symbol = "ICRCB";
      decimals = 8;
      fee = 10000;
      icrc1 = true;
      icrc2 = false;
      icrc3 = false;
      is_removed = false;
    });

    let mockTACO : TokenReply = #IC({
      token_id = 4;
      chain = "ICP";
      canister_id = "csyra-haaaa-aaaaq-aacva-cai";
      name = "TACO Token";
      symbol = "TACO";
      decimals = 8;
      fee = 10000;
      icrc1 = true;
      icrc2 = false;
      icrc3 = false;
      is_removed = false;
    });

    let mockCKUSDC : TokenReply = #IC({
      token_id = 5;
      chain = "ICP";
      canister_id = "xevnm-gaaaa-aaaar-qafnq-cai";
      name = "Wrapped USDC";
      symbol = "CKUSDC";
      decimals = 8;
      fee = 10000;
      icrc1 = true;
      icrc2 = false;
      icrc3 = false;
      is_removed = false;
    });

    // Add tokens to store
    Map.set(tokenStore, thash, "ICP", mockICP);
    Map.set(tokenStore, thash, "ICRCA", mockICRCA);
    Map.set(tokenStore, thash, "ICRCB", mockICRCB);
    Map.set(tokenStore, thash, "TACO", mockTACO);
    Map.set(tokenStore, thash, "CKUSDC", mockCKUSDC);

    addPair(
      "ICP",
      "CKUSDC",
      20.0, // 1 ICP = 20 USDC
      1_000_000_000_000,
      20_000_000_000_000,
    );

    addPair(
      "ICP",
      "ICRCA",
      5.0, // 1 ICP = 5 ICRCA
      1_000_000_000_000,
      5_000_000_000_000,
    );

    addPair(
      "ICP",
      "ICRCB",
      2.0, // 1 ICP = 2 ICRCB
      1_000_000_000_000,
      2_000_000_000_000,
    );

    addPair(
      "ICP",
      "TACO",
      100.0, // 1 ICP = 100 TACO
      1_000_000_000_000,
      100_000_000_000_000,
    );
    addPair(
      "ICP",
      "TEST1",
      15.0, // example price for TEST1
      1_000_000_000_000,
      15_000_000_000_000,
    );
    addPair(
      "ICP",
      "TEST2",
      15.0, // example price for TEST1
      1_000_000_000_000,
      15_000_000_000_000,
    );
  };

  // Calculate amounts for a swap
  private func calculateSwapAmount(amountIn : Nat, route : [Text]) : ?(Nat, Float) {
    if (route.size() < 2) { return null };

    var currentAmount = Float.fromInt(amountIn);
    var totalSlippage : Float = 0;

    for (i in Iter.range(0, route.size() - 2)) {
      let tokenA = route[i];
      let tokenB = route[i + 1];
      let pairKey = getPairKey(tokenA, tokenB);

      switch (Map.get(poolStore, thash, pairKey)) {
        case (?pool) {
          // Calculate price impact based on liquidity
          let (liq0, liq1) = switch (Map.get(liquidityStore, thash, pairKey)) {
            case (?liq) { liq };
            case null { return null };
          };

          let priceImpact = currentAmount / Float.fromInt(liq0);
          totalSlippage := totalSlippage + priceImpact;

          // Apply exchange rate
          let isReversed = tokenA > tokenB;
          let price = if (isReversed) 1.0 / pool.price else pool.price;

          // Apply fees (0.3%)
          currentAmount := currentAmount * price * 0.997;
        };
        case null { return null };
      };
    };

    ?(Int.abs(Float.toInt(currentAmount)), totalSlippage * 100.0);
  };

  // Query pools
  // Query functions
  public query func pools(symbol : ?Text) : async {
    #Ok : swaptypes.PoolsReply;
    #Err : Text;
  } {
    let filteredPools = Vector.new<swaptypes.PoolReply>();

    for ((key, pool) in Map.entries(poolStore)) {
      switch (symbol) {
        case null { Vector.add(filteredPools, pool) };
        case (?s) {
          if (pool.symbol_0 == s or pool.symbol_1 == s) {
            Vector.add(filteredPools, pool);
          };
        };
      };
    };

    #Ok({
      pools = Vector.toArray(filteredPools);
      total_tvl = 0;
      total_24h_volume = 0;
      total_24h_lp_fee = 0;
      total_24h_num_swaps = 0;
    });
  };

  // Query tokens
  public query func tokens(symbol : ?Text) : async {
    #Ok : [TokenReply];
    #Err : Text;
  } {
    let filteredTokens = Vector.new<TokenReply>();

    for ((key, token) in Map.entries(tokenStore)) {
      switch (symbol) {
        case null { Vector.add(filteredTokens, token) };
        case (?s) {
          if (key == s) {
            Vector.add(filteredTokens, token);
          };
        };
      };
    };

    #Ok(Vector.toArray(filteredTokens));
  };

  // Find best routing path
  private func findBestRoute(tokenA : Text, tokenB : Text) : ?(PoolReply, Bool, [Text]) {
    // Try direct route first
    let poolKey = tokenA # "/" # tokenB;
    let reversePoolKey = tokenB # "/" # tokenA;

    switch (Map.get(poolStore, thash, poolKey)) {
      case (?p) { ?(p, false, [tokenA, tokenB]) };
      case null {
        switch (Map.get(poolStore, thash, reversePoolKey)) {
          case (?p) { ?(p, true, [tokenA, tokenB]) };
          case null {
            // Try routing through ICP
            if (tokenA != "ICP" and tokenB != "ICP") {
              let routeThroughICP = findRouteThroughIntermediary(tokenA, tokenB, "ICP");
              switch (routeThroughICP) {
                case (?route) { ?route };
                case null {
                  // Try routing through CKUSDC
                  let routeThroughUSDC = findRouteThroughIntermediary(tokenA, tokenB, "CKUSDC");
                  switch (routeThroughUSDC) {
                    case (?route) { ?route };
                    case null { null };
                  };
                };
              };
            } else { null };
          };
        };
      };
    };
  };

  // Price impact and receive amount calculations
  private func calculateReceiveAmount(amountIn : Nat, pool : PoolReply, isReverse : Bool) : Nat {
    let inputFloat = Float.fromInt(amountIn);
    let outputAmount = if (isReverse) {
      inputFloat / pool.price; // If reverse (e.g. TACO->ICP), divide by price
    } else {
      inputFloat * pool.price; // If normal (e.g. ICP->TACO), multiply by price
    };

    // Apply the 0.3% fee
    let feeAmount = outputAmount * (Float.fromInt(Nat8.toNat(pool.lp_fee_bps)) / 10000.0);
    let finalAmount = outputAmount - feeAmount;

    // Convert back to Nat, using abs in case of rounding
    Int.abs(Float.toInt(finalAmount));
  };

  // Calculate pool slippage based on input size vs liquidity
  private func calculateSlippage(amountIn : Nat, pool : PoolReply) : Float {
    let amountFloat = Float.fromInt(amountIn);
    let liquidityFloat = Float.fromInt(pool.balance_0);

    // Slippage is approximated as input amount / pool liquidity
    let priceImpact = amountFloat / liquidityFloat;

    // Cap maximum slippage at 100%
    Float.min(priceImpact * 100.0, 100.0);
  };

  // Find direct pool between tokens
  private func findDirectRoute(tokenA : Text, tokenB : Text) : ?(PoolReply, Bool, [Text]) {
    let poolKey = tokenA # "/" # tokenB;
    let reversePoolKey = tokenB # "/" # tokenA;

    switch (Map.get(poolStore, thash, poolKey)) {
      case (?p) { ?(p, false, [tokenA, tokenB]) };
      case null {
        switch (Map.get(poolStore, thash, reversePoolKey)) {
          case (?p) { ?(p, true, [tokenA, tokenB]) };
          case null { null };
        };
      };
    };
  };

  // Find route through an intermediary token
  private func findRouteThroughIntermediary(tokenA : Text, tokenB : Text, intermediary : Text) : ?(PoolReply, Bool, [Text]) {
    switch (findDirectRoute(tokenA, intermediary), findDirectRoute(intermediary, tokenB)) {
      case (?routeA, _) {

        ?(routeA.0, routeA.1, [tokenA, intermediary, tokenB]);
      };
      case _ { null };
    };
  };

  // Calculate amounts for multi-hop route
  private func calculateRouteAmounts(amountIn : Nat, route : [Text]) : ?(Nat, Float, [(Text, Text, Nat, Nat)]) {
    if (route.size() < 2) { return null };

    var currentAmount = amountIn;
    var totalSlippage : Float = 0;
    let hops = Vector.new<(Text, Text, Nat, Nat)>(); // (fromToken, toToken, amountIn, amountOut)

    for (i in Iter.range(0, route.size() - 2)) {
      let tokenA = route[i];
      let tokenB = route[i + 1];

      let poolKey = tokenA # "/" # tokenB;
      let reversePoolKey = tokenB # "/" # tokenA;

      let (pool, isReverse) = switch (Map.get(poolStore, thash, poolKey)) {
        case (?p) { (p, false) };
        case null {
          switch (Map.get(poolStore, thash, reversePoolKey)) {
            case (?p) { (p, true) };
            case null { return null };
          };
        };
      };

      let slippage = calculateSlippage(currentAmount, pool);
      let receiveAmount = calculateReceiveAmount(currentAmount, pool, isReverse);

      Vector.add(hops, (tokenA, tokenB, currentAmount, receiveAmount));

      totalSlippage := totalSlippage + slippage;
      currentAmount := receiveAmount;
    };

    ?(currentAmount, totalSlippage, Vector.toArray(hops));
  };

  public query func swap_amounts(tokenA : Text, amountIn : Nat, tokenB : Text) : async {
    #Ok : swaptypes.SwapAmountsReply;
    #Err : Text;
  } {
    Debug.print("KongSwap.swap_amounts: Called for " # tokenA # " -> " # tokenB # " amount: " # Nat.toText(amountIn));

    switch (getTokenPrice(tokenA, tokenB)) {
      case (?(price, route)) {
        Debug.print("KongSwap: Found route: " # debug_show (route));

        switch (calculateRouteImpact(amountIn, route)) {
          case (?(priceImpact, fees)) {
            Debug.print("KongSwap: Calculated impact: " # Float.toText(priceImpact) # "% fees: " # Float.toText(fees));

            // Calculate final amount out considering fees and price
            let amountAfterFees = Float.fromInt(amountIn) - fees;
            let amountOut = Int.abs(Float.toInt(amountAfterFees * price));

            let hops = Vector.new<swaptypes.SwapAmountsTxReply>();

            // Create hop entries for each step in the route
            for (i in Iter.range(0, route.size() - 2)) {
              let fromToken = route[i];
              let toToken = route[i + 1];
              let hopKey = getPairKey(fromToken, toToken);

              let hopPrice = switch (Map.get(priceStore, thash, hopKey)) {
                case (?p) { p };
                case null { 1.0 }; // Shouldn't happen if route exists
              };

              let hop : swaptypes.SwapAmountsTxReply = {
                pool_symbol = fromToken # "/" # toToken;
                pay_chain = "ICP";
                pay_symbol = fromToken;
                pay_address = getTokenAddress(fromToken);
                pay_amount = if (i == 0) amountIn else Int.abs(Float.toInt(amountAfterFees));
                receive_chain = "ICP";
                receive_symbol = toToken;
                receive_address = getTokenAddress(toToken);
                receive_amount = if (i == route.size() - 2) amountOut else Int.abs(Float.toInt(amountAfterFees * hopPrice));
                price = hopPrice;
                lp_fee = Int.abs(Float.toInt(fees / Float.fromInt(route.size() - 1)));
                gas_fee = 10000;
              };
              Vector.add(hops, hop);
            };

            #Ok({
              pay_chain = "ICP";
              pay_symbol = tokenA;
              pay_address = getTokenAddress(tokenA);
              pay_amount = amountIn;
              receive_chain = "ICP";
              receive_symbol = tokenB;
              receive_address = getTokenAddress(tokenB);
              receive_amount = amountOut;
              price = price;
              mid_price = price;
              slippage = priceImpact;
              txs = Vector.toArray(hops);
            });
          };
          case null {
            Debug.print("KongSwap: Failed to calculate route impact");
            #Err("Failed to calculate route impact");
          };
        };
      };
      case null {
        Debug.print("KongSwap: No route found");
        #Err("No route found between tokens");
      };
    };
  };
  // Swap execution
  public shared func swap(args : swaptypes.KongSwapArgs) : async {
    #Ok : swaptypes.SwapReply;
    #Err : Text;
  } {
    Debug.print("MockKongSwap.swap: Called with args: " # debug_show (args));

    let amountResult = await swap_amounts(
      args.pay_token,
      args.pay_amount,
      args.receive_token,
    );

    switch (amountResult) {
      case (#Ok(amounts)) {
        let txId = Nat64.fromNat(Int.abs(now()) / 1_000_000);
        Debug.print("MockKongSwap.swap: Swap successful, txId: " # Nat64.toText(txId));

        #Ok({
          tx_id = txId;
          request_id = txId;
          status = "Completed";
          pay_chain = amounts.pay_chain;
          pay_address = amounts.pay_address;
          pay_symbol = amounts.pay_symbol;
          pay_amount = amounts.pay_amount;
          receive_chain = amounts.receive_chain;
          receive_address = amounts.receive_address;
          receive_symbol = amounts.receive_symbol;
          receive_amount = amounts.receive_amount;
          mid_price = amounts.mid_price;
          price = amounts.price;
          slippage = amounts.slippage;
          txs = [];
          transfer_ids = [];
          claim_ids = [];
          ts = Nat64.fromNat(Int.abs(now()));
        });
      };
      case (#Err(e)) {
        Debug.print("MockKongSwap.swap: Swap failed: " # e);
        #Err(e);
      };
    };
  };

  // Execute async swap
  public shared func swap_async(args : KongSwapArgs) : async {
    #Ok : Nat64;
    #Err : Text;
  } {
    let swapResult = await swap(args);

    switch (swapResult) {
      case (#Ok(reply)) { #Ok(reply.request_id) };
      case (#Err(e)) { #Err(e) };
    };
  };

  // Query requests
  public query func requests(requestId : ?Nat64) : async {
    #Ok : [RequestsReply];
    #Err : Text;
  } {
    switch (requestId) {
      case null {
        let allRequests = Vector.new<RequestsReply>();
        for ((_, request) in Map.entries(requestStore)) {
          Vector.add(allRequests, request);
        };
        #Ok(Vector.toArray(allRequests));
      };
      case (?id) {
        switch (Map.get(requestStore, nhash, Nat64.toNat(id))) {
          case (?request) { #Ok([request]) };
          case null { #Ok([]) };
        };
      };
    };
  };

  Timer.setTimer<system>(
    #nanoseconds(1),
    func() : async () {
      await initializeMockData();
    },
  );

};
