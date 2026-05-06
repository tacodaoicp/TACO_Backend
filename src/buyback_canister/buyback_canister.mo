// TACO Fee Buyback Canister
//
// Owns the entire buyback flow end-to-end:
//   1. Claim vault fees → buyback canister default account
//   2. (V2) Pre-arb on the exchange
//   3. Per-token swap to TACO via Kong / ICPSwap / TACO Exchange
//   4. Burn accumulated TACO by transfer to SNS minting account
//   5. (V2) Post-arb
//
// See plan: ~/.claude/plans/we-have-the-nachos-purring-puffin.md
//
// Deployment requirement: this canister's principal MUST be in
//   - src/helper/CanisterIds.mo (canisterMappings) BEFORE install (so
//     canister_ids.getEnvironment() doesn't trap at init)
//   - src/helper/admin_authorization.mo masterAdminTexts (so vault.claimAllFees
//     and exchange admin functions accept calls from this canister)

import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Vector "mo:vector";
import Map "mo:map/Map";
import Buffer "mo:base/Buffer";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Int "mo:base/Int";
import Float "mo:base/Float";
import Text "mo:base/Text";
import Iter "mo:base/Iter";
import Error "mo:base/Error";
import Array "mo:base/Array";
import { now } = "mo:base/Time";
import { setTimer; cancelTimer } = "mo:base/Timer";
import ICRC1 "mo:icrc1/ICRC1";
import KongSwap "../swap/kong_swap";
import ICPSwap "../swap/icp_swap";
import TACOSwap "../swap/taco_swap";
import swaptypes "../swap/swap_types";
import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import SpamProtection "../helper/spam_protection";
import BuybackTypes "./buyback_canister_types";
import ExchangeType "./exchange_type";
import TreasuryQueryType "./treasury_query_type";
// Migration to drop legacy `buybackHistory` already ran in a prior upgrade;
// no migration block needed for subsequent upgrades.
shared (deployer) persistent actor class buyback_canister() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  transient let logger = Logger.Logger();
  transient let { phash; thash } = Map;

  // ── Constants ──────────────────────────────────────────────────────────
  let TACO_LEDGER : Principal = Principal.fromText("kknbx-zyaaa-aaaaq-aae4a-cai");
  let CKUSDC_LEDGER : Principal = Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai");
  let ICP_LEDGER : Principal = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
  let SNS_BURN_OWNER : Principal = Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai");
  let OTC_PLACEHOLDER : Principal = Principal.fromText("aaaaa-aa");

  transient let TREASURY_ID = canister_ids.getCanisterId(#treasury);
  transient let NACHOS_VAULT_ID = canister_ids.getCanisterId(#nachos_vault);
  transient let OTC_BACKEND_ID = canister_ids.getCanisterId(#OTC_backend);
  transient let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
  transient let DAO_PRINCIPAL : Principal = DAO_BACKEND_ID;

  // ── Actor refs ─────────────────────────────────────────────────────────
  transient let tacoLedger = actor (Principal.toText(TACO_LEDGER)) : ICRC1.FullInterface;
  transient let ckUSDCLedger = actor (Principal.toText(CKUSDC_LEDGER)) : ICRC1.FullInterface;
  transient let icpLedger = actor (Principal.toText(ICP_LEDGER)) : ICRC1.FullInterface;
  transient let nachosVault = actor (Principal.toText(NACHOS_VAULT_ID)) : actor {
    claimAllFees : (Principal, ?Blob) -> async Result.Result<{
      mint : [(Principal, Nat)];
      burn : [(Principal, Nat)];
      cancellation : [(Principal, Nat)];
    }, Text>;
  };
  transient let exchange : ExchangeType.Exchange = actor (Principal.toText(OTC_BACKEND_ID));
  transient let treasuryQuery = actor (Principal.toText(TREASURY_ID)) : TreasuryQueryType.Treasury;
  transient let dao = actor (Principal.toText(DAO_BACKEND_ID)) : actor {
    hasAdminPermission : query (principal : Principal, function : SpamProtection.AdminFunction) -> async Bool;
  };

  // ── Type aliases ───────────────────────────────────────────────────────
  type BuybackConfig = BuybackTypes.BuybackConfig;
  type BuybackConfigUpdate = BuybackTypes.BuybackConfigUpdate;
  type BuybackCycleRecord = BuybackTypes.BuybackCycleRecord;
  type BuybackSwapResult = BuybackTypes.BuybackSwapResult;
  type BuybackArbIteration = BuybackTypes.BuybackArbIteration;
  type BuybackArbRun = BuybackTypes.BuybackArbRun;
  type BuybackArbStatus = BuybackTypes.BuybackArbStatus;
  type BuybackCycleStatus = BuybackTypes.BuybackCycleStatus;
  type CachedTokenDetails = BuybackTypes.CachedTokenDetails;

  // ── Stable state ───────────────────────────────────────────────────────
  stable var buybackConfig : BuybackConfig = {
    enabled = false;
    intervalNS = 86_400_000_000_000;       // 1 day
    minTokenValueICP = 50_000_000;          // ~$5
    sweepVault = true;
    sweepExchange = false;
    runArb = false;
    arbWorkingCapitalICP = 500_000_000;     // 5 ICP
    arbWorkingCapitalUSDC = 500_000_000;    // 500 ckUSDC (6 decimals)
    arbDepth = 4;
    arbMinSampleAmount = [
      (Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), 1_000_000),
      (Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai"), 10_000),
    ];
    arbTernaryProbes = 6;
    arbTernaryTolerancePct = 1;
    arbMinProfitICP = 1_000_000;
    arbMaxIterations = 20;
    arbMaxRoutesPerAnalysis = 5;
    arbSettlementTimeoutMs = 30_000;
    arbProfitSlippageBps = 5000; // accept routes capturing >= 50% of expected profit
    tokenDetailsStaleAfterNS = 1_800_000_000_000; // 30 minutes
  };
  stable var buybackTimerId : Nat = 0;
  stable var buybackHistoryV2 : Vector.Vector<BuybackCycleRecord> = Vector.new<BuybackCycleRecord>();
  stable var nextBuybackCycleId : Nat = 0;
  stable var totalBurned : Nat = 0;

  // Local token details cache. Refreshed from treasury via syncTokenDetailsFromTreasury().
  stable let tokenDetailsCache = Map.new<Principal, CachedTokenDetails>();
  stable var lastTokenDetailsSync : Int = 0;

  // Transient mutex against concurrent cycle execution
  transient var cycleInProgress : Bool = false;

  let MAX_BUYBACK_HISTORY : Nat = 365;

  // ────────────────────────────────────────────────────────────────────────
  // Auth helpers
  // ────────────────────────────────────────────────────────────────────────

  private func isMasterAdmin(caller : Principal) : Bool {
    AdminAuth.isMasterAdmin(caller, canister_ids.isKnownCanister);
  };

  // Mirrors treasury's pattern (treasury.mo:1057+): a granted DAO permission
  // OR DAO itself OR canister controller OR master admin.
  // Used by all admin-flow-protected endpoints.
  private func authForFunction(caller : Principal, fn : SpamProtection.AdminFunction) : async Bool {
    if (Principal.equal(caller, DAO_PRINCIPAL)) { return true };
    if (Principal.isController(caller)) { return true };
    if (isMasterAdmin(caller)) { return true };
    let granted = try { await dao.hasAdminPermission(caller, fn) } catch (_) { false };
    granted;
  };

  private func isProduction() : Bool {
    canister_ids.getEnvironment() == #Production;
  };

  // ────────────────────────────────────────────────────────────────────────
  // Token details cache helpers
  // ────────────────────────────────────────────────────────────────────────

  private func tokenSymbolOf(token : Principal) : Text {
    switch (Map.get(tokenDetailsCache, phash, token)) {
      case (?d) { d.tokenSymbol };
      case null { "UNKNOWN" };
    };
  };

  private func tokenDecimalsOf(token : Principal) : Nat {
    switch (Map.get(tokenDetailsCache, phash, token)) {
      case (?d) { d.tokenDecimals };
      case null { 8 };
    };
  };

  private func tokenFeeOf(token : Principal) : Nat {
    switch (Map.get(tokenDetailsCache, phash, token)) {
      case (?d) { d.tokenTransferFee };
      case null { 10000 };
    };
  };

  private func priceInICPOf(token : Principal) : Nat {
    switch (Map.get(tokenDetailsCache, phash, token)) {
      case (?d) { d.priceInICP };
      case null { 0 };
    };
  };

  private func valueInICP(token : Principal, amount : Nat) : Nat {
    let price = priceInICPOf(token);
    let dec = tokenDecimalsOf(token);
    let denom = 10 ** dec;
    if (price == 0 or denom == 0) { 0 } else { (amount * price) / denom };
  };

  // Sync token details from treasury into local cache.
  // Idempotent — skips if cache is fresh enough per `tokenDetailsStaleAfterNS`.
  private func syncTokenDetailsFromTreasuryInternal(force : Bool) : async Result.Result<{ tokensRefreshed : Nat }, Text> {
    let staleness = now() - lastTokenDetailsSync;
    if (not force and staleness < buybackConfig.tokenDetailsStaleAfterNS) {
      return #ok({ tokensRefreshed = 0 }); // Cache is fresh
    };

    let entries = try {
      await treasuryQuery.getTokenDetails();
    } catch (e) {
      logger.error("BUYBACK", "syncTokenDetailsFromTreasury: " # Error.message(e), "syncTokenDetailsFromTreasuryInternal");
      return #err("Treasury query failed: " # Error.message(e));
    };

    var count : Nat = 0;
    for ((p, d) in entries.vals()) {
      Map.set(tokenDetailsCache, phash, p, ({
        tokenSymbol = d.tokenSymbol;
        tokenDecimals = d.tokenDecimals;
        tokenTransferFee = d.tokenTransferFee;
        priceInICP = d.priceInICP;
      } : CachedTokenDetails));
      count += 1;
    };
    lastTokenDetailsSync := now();
    logger.info("BUYBACK", "synced " # Nat.toText(count) # " token details from treasury", "syncTokenDetailsFromTreasuryInternal");
    #ok({ tokensRefreshed = count });
  };

  // ────────────────────────────────────────────────────────────────────────
  // Balance / ledger helpers
  // ────────────────────────────────────────────────────────────────────────

  private func ledgerForToken(token : Principal) : ICRC1.FullInterface {
    if (Principal.equal(token, ICP_LEDGER)) { icpLedger }
    else if (Principal.equal(token, TACO_LEDGER)) { tacoLedger }
    else if (Principal.equal(token, CKUSDC_LEDGER)) { ckUSDCLedger }
    else { actor (Principal.toText(token)) : ICRC1.FullInterface };
  };

  private func balanceOf(token : Principal) : async Nat {
    await ledgerForToken(token).icrc1_balance_of({
      owner = this_canister_id();
      subaccount = null;
    });
  };

  // ────────────────────────────────────────────────────────────────────────
  // History append with cap
  // ────────────────────────────────────────────────────────────────────────

  private func appendBuybackHistory(rec : BuybackCycleRecord) {
    Vector.add(buybackHistoryV2, rec);
    while (Vector.size(buybackHistoryV2) > MAX_BUYBACK_HISTORY) {
      // Vector has no removeFirst; reverse + removeLast + reverse is O(n) but called rarely.
      Vector.reverse(buybackHistoryV2);
      ignore Vector.removeLast(buybackHistoryV2);
      Vector.reverse(buybackHistoryV2);
    };
  };

  // ────────────────────────────────────────────────────────────────────────
  // findBestExecutionFullAmount — single-quote-per-DEX picker
  // Mirrors treasury's findBestExecution (treasury.mo:10105) but issues ONE
  // quote per DEX (full amount). Returns ExecutionPlan #Single only.
  // ────────────────────────────────────────────────────────────────────────

  type ExchangeChoice = { #KongSwap; #ICPSwap; #TACO };

  type SinglePlan = {
    exchange : ExchangeChoice;
    expectedOut : Nat;
    slippageBP : Nat;
    icpswapPool : ?TreasuryQueryType.ICPSwapPoolInfo;  // populated only when exchange = #ICPSwap
  };

  type FindExecutionError = { reason : Text };

  // Per-pair skip: tracked locally in transient state. Buyback canister could
  // also persist these but the cycle is daily so a transient skip is fine —
  // we re-discover on each cycle's first quote.
  transient var skipKongPair : Map.Map<(Principal, Principal), Bool> = Map.new();
  transient var skipTacoPair : Map.Map<(Principal, Principal), Bool> = Map.new();

  transient let pairHash : Map.HashUtils<(Principal, Principal)> = (
    func((a, b) : (Principal, Principal)) : Nat32 {
      Principal.hash(a) +% Principal.hash(b)
    },
    func(x : (Principal, Principal), y : (Principal, Principal)) : Bool {
      Principal.equal(x.0, y.0) and Principal.equal(x.1, y.1)
    },
  );

  private func shouldSkipKong(sell : Principal, buy : Principal) : Bool {
    Map.has(skipKongPair, pairHash, (sell, buy));
  };

  private func shouldSkipTaco(sell : Principal, buy : Principal) : Bool {
    Map.has(skipTacoPair, pairHash, (sell, buy));
  };

  private func markKongSkip(sell : Principal, buy : Principal) {
    Map.set(skipKongPair, pairHash, (sell, buy), true);
  };

  private func markTacoSkip(sell : Principal, buy : Principal) {
    Map.set(skipTacoPair, pairHash, (sell, buy), true);
  };

  private func isFiniteFloat(x : Float) : Bool {
    not Float.isNaN(x) and x < 9.0e18 and x > -9.0e18
  };

  private func findBestExecutionFullAmount(
    sellToken : Principal,
    buyToken : Principal,
    amountIn : Nat,
    maxSlippageBp : Nat,
  ) : async* Result.Result<SinglePlan, FindExecutionError> {
    let sellSymbol = tokenSymbolOf(sellToken);
    let buySymbol = tokenSymbolOf(buyToken);
    let sellDecimals = tokenDecimalsOf(sellToken);
    let buyDecimals = tokenDecimalsOf(buyToken);
    let sellTokenFee = tokenFeeOf(sellToken);

    if (amountIn == 0) {
      return #err({ reason = "amount is zero" });
    };

    // 5% sanity check on transfer fee
    if (sellTokenFee > 0) {
      let feeRatioBP = (sellTokenFee * 10000) / amountIn;
      if (feeRatioBP > 500) {
        return #err({ reason = "Transfer fee (" # Nat.toText(feeRatioBP) # "bp) exceeds 5% of amount" });
      };
    };

    let skipKong = shouldSkipKong(sellToken, buyToken);
    let skipTaco = shouldSkipTaco(sellToken, buyToken);

    // Fire all three quote queries in parallel. Each is a separate inter-canister
    // query (fresh budget on the called side).
    let kongFutureOpt : ?(async Result.Result<swaptypes.SwapAmountsReply, Text>) =
      if (skipKong) { null }
      else { ?((with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, amountIn, sellDecimals, buyDecimals)) };

    // For ICPSwap, query treasury for the pool data and fire quote in parallel
    // (treasury maintains the pool map via its periodic discovery; buyback queries
    // it on-demand rather than maintaining a local cache).
    let icpAmountIn : Nat = if (amountIn > sellTokenFee) (amountIn - sellTokenFee : Nat) else 0;
    let icpPoolFut = treasuryQuery.getICPSwapPoolInfo(sellToken, buyToken);
    let icpPool : ?TreasuryQueryType.ICPSwapPoolInfo = try { await icpPoolFut } catch (_) { null };
    let icpFutureOpt : ?(async Result.Result<swaptypes.ICPSwapQuoteResult, Text>) =
      switch (icpPool) {
        case (?pool) {
          if (icpAmountIn == 0) { null }
          else {
            let zeroForOne = sellToken == Principal.fromText(pool.token0);
            ?((with timeout = 65) ICPSwap.getQuote({
              poolId = pool.canisterId;
              amountIn = icpAmountIn;
              amountOutMinimum = 0;
              zeroForOne = zeroForOne;
            }))
          };
        };
        case null { null };
      };

    let sellTokenText = Principal.toText(sellToken);
    let buyTokenText = Principal.toText(buyToken);
    let tacoFutureOpt : ?(async Result.Result<[[swaptypes.TACOQuoteReply]], Text>) =
      if (skipTaco) { null }
      else {
        ?((with timeout = 65) TACOSwap.getQuoteWithRouteBatchMulti(
          [{ tokenA = sellTokenText; tokenB = buyTokenText; amountIn = amountIn }],
          sellDecimals,
          buyDecimals,
          5, // maxRoutes — same default as treasury
        ))
      };

    let kongResult : Result.Result<swaptypes.SwapAmountsReply, Text> =
      switch (kongFutureOpt) {
        case (?f) { try { await f } catch (e) { #err("Kong quote exception: " # Error.message(e)) } };
        case null { #err("Kong skipped") };
      };
    let icpResult : Result.Result<swaptypes.ICPSwapQuoteResult, Text> =
      switch (icpFutureOpt) {
        case (?f) { try { await f } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) } };
        case null { #err("ICPSwap unavailable for pair (no local pool cache in V1)") };
      };
    let tacoBatchResult : Result.Result<[[swaptypes.TACOQuoteReply]], Text> =
      switch (tacoFutureOpt) {
        case (?f) { try { await f } catch (e) { #err("TACO quote exception: " # Error.message(e)) } };
        case null { #err("TACO skipped") };
      };

    let maxSlippagePct = Float.fromInt(maxSlippageBp) / 100.0;

    // Extract Kong
    type Q = { out : Nat; slipBP : Nat; valid : Bool };
    let kongQuote : Q = switch (kongResult) {
      case (#ok(r)) {
        let slipBP : Nat = if (isFiniteFloat(r.slippage * 100.0)) { Int.abs(Float.toInt(r.slippage * 100.0)) } else { 10000 };
        let valid = r.slippage <= maxSlippagePct and r.receive_amount > 0;
        { out = r.receive_amount; slipBP; valid };
      };
      case (#err(_)) { { out = 0; slipBP = 10000; valid = false } };
    };

    let icpQuote : Q = switch (icpResult) {
      case (#ok(r)) {
        let slipBP : Nat = if (isFiniteFloat(r.slippage * 100.0)) { Int.abs(Float.toInt(r.slippage * 100.0)) } else { 10000 };
        let valid = r.slippage <= maxSlippagePct and r.amountOut > 0;
        { out = r.amountOut; slipBP; valid };
      };
      case (#err(_)) { { out = 0; slipBP = 10000; valid = false } };
    };

    let tacoQuote : Q = switch (tacoBatchResult) {
      case (#ok(bundles)) {
        if (bundles.size() > 0 and bundles[0].size() > 0) {
          let r = bundles[0][0];
          let slipBP : Nat = if (isFiniteFloat(r.slippage * 100.0)) { Int.abs(Float.toInt(r.slippage * 100.0)) } else { 10000 };
          let valid = r.slippage <= maxSlippagePct and r.receive_amount > 0;
          { out = r.receive_amount; slipBP; valid };
        } else { { out = 0; slipBP = 10000; valid = false } };
      };
      case (#err(_)) { { out = 0; slipBP = 10000; valid = false } };
    };

    // Update per-pair skip flags
    if (not skipKong and not kongQuote.valid) {
      // Kong quoted but invalid (slippage too high or zero output) — mark for this cycle
      markKongSkip(sellToken, buyToken);
    };
    if (not skipTaco and not tacoQuote.valid) {
      markTacoSkip(sellToken, buyToken);
    };

    var bestExchange : ?ExchangeChoice = null;
    var bestOut : Nat = 0;
    var bestSlipBP : Nat = 0;
    if (kongQuote.valid and kongQuote.out > bestOut) {
      bestExchange := ?(#KongSwap); bestOut := kongQuote.out; bestSlipBP := kongQuote.slipBP;
    };
    if (icpQuote.valid and icpQuote.out > bestOut) {
      bestExchange := ?(#ICPSwap); bestOut := icpQuote.out; bestSlipBP := icpQuote.slipBP;
    };
    if (tacoQuote.valid and tacoQuote.out > bestOut) {
      bestExchange := ?(#TACO); bestOut := tacoQuote.out; bestSlipBP := tacoQuote.slipBP;
    };

    switch (bestExchange) {
      case (?ex) {
        let plan : SinglePlan = {
          exchange = ex;
          expectedOut = bestOut;
          slippageBP = bestSlipBP;
          icpswapPool = if (ex == #ICPSwap) { icpPool } else { null };
        };
        #ok(plan);
      };
      case null { #err({ reason = "no DEX produced a valid quote for full amount" }) };
    };
  };

  // ────────────────────────────────────────────────────────────────────────
  // Per-token swap to TACO
  // ────────────────────────────────────────────────────────────────────────

  // Generic "swap from token A to token B" using best DEX (Kong / ICPSwap / TACO Exchange).
  // Returns (BuybackSwapResult, output amount that arrived in default account).
  // BuybackSwapResult.tacoOut is reused as a generic "out amount" field for record compat.
  private func swapToken(from : Principal, to : Principal, amount : Nat, maxSlippageBp : Nat) : async (BuybackSwapResult, Nat) {
    // Same-token — no-op, already in our default account
    if (Principal.equal(from, to)) {
      return ({
        sellToken = from; amountIn = amount;
        dex = "direct"; tacoOut = amount;
        error = null;
      }, amount);
    };

    // Dust filter
    let valICP = valueInICP(from, amount);
    if (valICP < buybackConfig.minTokenValueICP) {
      return ({
        sellToken = from; amountIn = amount;
        dex = "skip-dust"; tacoOut = 0;
        error = null;
      }, 0);
    };

    let plan = await* findBestExecutionFullAmount(from, to, amount, maxSlippageBp);
    switch (plan) {
      case (#err(reason)) {
        ({
          sellToken = from; amountIn = amount;
          dex = "skip-no-route"; tacoOut = 0;
          error = ?reason.reason;
        }, 0);
      };
      case (#ok(execution)) {
        // Capture pre-swap target balance for safety floor
        let toBefore = await balanceOf(to);

        let sellSymbol = tokenSymbolOf(from);
        let buySymbol = tokenSymbolOf(to);

        // Slippage envelope: tolerate up to maxSlippageBp on top of expectedOut
        let toleranceMultiplier : Nat = if (maxSlippageBp >= 10000) { 0 } else { 10000 - maxSlippageBp };
        let minAmountOut : Nat = (execution.expectedOut * toleranceMultiplier) / 10000;

        let dexLabel = switch (execution.exchange) {
          case (#KongSwap) { "Kong" };
          case (#ICPSwap) { "ICPSwap" };
          case (#TACO) { "TACO" };
        };

        // Dispatch
        let result : Result.Result<Nat, Text> = switch (execution.exchange) {
          case (#KongSwap) {
            let swapArgs : swaptypes.KongSwapParams = {
              token0_ledger = from;
              token0_symbol = sellSymbol;
              token1_ledger = to;
              token1_symbol = buySymbol;
              amountIn = amount;
              minAmountOut = minAmountOut;
              deadline = ?(now() + 300_000_000_000); // 5 min
              recipient = null; // output to self (default account)
              txId = null;
              slippageTolerance = Float.fromInt(maxSlippageBp) / 100.0;
            };
            let r = try {
              await KongSwap.executeTransferAndSwap(swapArgs);
            } catch (e) { return ({
              sellToken = from; amountIn = amount;
              dex = dexLabel; tacoOut = 0;
              error = ?("Kong exception: " # Error.message(e));
            }, 0) };
            switch (r) {
              case (#ok(reply)) { #ok(reply.receive_amount) };
              case (#err(e)) { #err("Kong: " # e) };
            };
          };
          case (#ICPSwap) {
            switch (execution.icpswapPool) {
              case null { #err("ICPSwap chosen but no pool data available — internal error") };
              case (?pool) {
                let buyTokenFee = tokenFeeOf(to);
                let zeroForOne = Principal.equal(from, Principal.fromText(pool.token0));
                let depositParams : swaptypes.ICPSwapDepositParams = {
                  poolId = pool.canisterId;
                  token = from;
                  amount = amount;
                  fee = tokenFeeOf(from);
                };
                let swapParams : swaptypes.ICPSwapParams = {
                  poolId = pool.canisterId;
                  amountIn = if (amount > tokenFeeOf(from)) { amount - tokenFeeOf(from) } else { 0 };
                  minAmountOut = minAmountOut;
                  zeroForOne = zeroForOne;
                };
                let r = try {
                  await ICPSwap.executeTransferDepositAndSwap(this_canister_id(), depositParams, swapParams, buyTokenFee);
                } catch (e) { return ({
                  sellToken = from; amountIn = amount;
                  dex = dexLabel; tacoOut = 0;
                  error = ?("ICPSwap exception: " # Error.message(e));
                }, 0) };
                switch (r) {
                  case (#ok(swapAmount)) {
                    let delivered : Nat = if (swapAmount > buyTokenFee) { swapAmount - buyTokenFee } else { 0 };
                    #ok(delivered);
                  };
                  case (#err(e)) { #err("ICPSwap: " # e) };
                };
              };
            };
          };
          case (#TACO) {
            let exchangeTreasuryPrincipal = canister_ids.getCanisterId(#exchange_treasury);
            let exchangeTreasuryAccountId = Principal.toLedgerAccount(exchangeTreasuryPrincipal, null);
            let r = try {
              await TACOSwap.executeTransferAndSwap({
                tokenIn = from;
                tokenOut = to;
                amountIn = amount;
                minAmountOut = minAmountOut;
                transferFee = tokenFeeOf(from);
                exchangeTreasuryAccountId = exchangeTreasuryAccountId;
              });
            } catch (e) { return ({
              sellToken = from; amountIn = amount;
              dex = dexLabel; tacoOut = 0;
              error = ?("TACO exception: " # Error.message(e));
            }, 0) };
            switch (r) {
              case (#ok(reply)) { #ok(reply.amountOut) };
              case (#err(e)) { #err("TACO: " # e) };
            };
          };
        };

        switch (result) {
          case (#ok(reportedOut)) {
            let toAfter = await balanceOf(to);
            let diff : Nat = if (toAfter > toBefore) { toAfter - toBefore } else { 0 };
            let actual : Nat = if (reportedOut < diff) { reportedOut } else { diff };
            ({
              sellToken = from; amountIn = amount;
              dex = dexLabel; tacoOut = actual;
              error = null;
            }, actual);
          };
          case (#err(reason)) {
            ({
              sellToken = from; amountIn = amount;
              dex = dexLabel; tacoOut = 0;
              error = ?reason;
            }, 0);
          };
        };
      };
    };
  };

  // Backward-compatible wrapper — most callers want token→TACO.
  private func swapTokenToTaco(token : Principal, amount : Nat, maxSlippageBp : Nat) : async (BuybackSwapResult, Nat) {
    await swapToken(token, TACO_LEDGER, amount, maxSlippageBp);
  };

  // ────────────────────────────────────────────────────────────────────────
  // Burn step
  // ────────────────────────────────────────────────────────────────────────

  // Transfers entire TACO balance to SNS minting account.
  // Production-only. If burn fails, TACO stays in canister default and is
  // retried on the next cycle.
  private func burnAccumulatedTaco() : async { burned : Nat; blockIndex : ?Nat; error : ?Text } {
    if (not isProduction()) {
      return { burned = 0; blockIndex = null; error = ?"not on production" };
    };
    let balance = await balanceOf(TACO_LEDGER);
    if (balance == 0) {
      return { burned = 0; blockIndex = null; error = null };
    };
    let r = try {
      await tacoLedger.icrc1_transfer({
        from_subaccount = null;
        to = { owner = SNS_BURN_OWNER; subaccount = null };
        amount = balance;
        fee = ?0; // verified zero burn fee
        memo = ?Text.encodeUtf8("buyback-burn");
        created_at_time = null;
      });
    } catch (e) {
      logger.error("BUYBACK", "burn transfer threw: " # Error.message(e), "burnAccumulatedTaco");
      return { burned = 0; blockIndex = null; error = ?("transfer threw: " # Error.message(e)) };
    };
    switch (r) {
      case (#Ok(b)) {
        totalBurned += balance;
        logger.info("BUYBACK", "burned " # Nat.toText(balance) # " TACO, block " # Nat.toText(b), "burnAccumulatedTaco");
        { burned = balance; blockIndex = ?b; error = null };
      };
      case (#Err(e)) {
        logger.error("BUYBACK", "burn err: " # debug_show(e), "burnAccumulatedTaco");
        { burned = 0; blockIndex = null; error = ?("ledger err: " # debug_show(e)) };
      };
    };
  };

  // ────────────────────────────────────────────────────────────────────────
  // Arb leg (V2)
  // ────────────────────────────────────────────────────────────────────────
  // Pre-arb runs at the start of each cycle (captures inefficiencies that
  // exist now); post-arb runs after the swap step (captures inefficiencies
  // our own swaps just created). Both implementations share runArbLeg and
  // its loop infrastructure.
  //
  // Per iteration: single call to exchange.adminFindOptimalArb to find the
  // best (amount, route) via internal ternary search, then a single call to
  // exchange.adminFlashArb to execute. Flash arb provides zero-capital
  // execution (the exchange lends from feescollectedDAO[token] when available
  // or runs in phantom mode); the buyback canister never deposits ICP/ckUSDC.
  // Profit guarantee is enforced by the exchange's trap-on-insufficient-profit
  // — the buyback either receives netProfit > 0 or sees an error (no funds move).
  // ────────────────────────────────────────────────────────────────────────

  type ArbCandidate = {
    amount : Nat;
    route : [ExchangeType.SwapHop];
    profit : Nat;
    profitICP : Nat;
  };

  private func lookupMinSample(token : Principal) : Nat {
    for ((t, v) in buybackConfig.arbMinSampleAmount.vals()) {
      if (Principal.equal(t, token)) { return v };
    };
    1_000_000; // conservative fallback (~0.01 ICP)
  };

  // Find optimal arb amount + route for a given starting token.
  // SINGLE inter-canister call to exchange.adminFindOptimalArb — the exchange
  // does the entire ternary search internally and returns the best candidate.
  // Snapshot-consistent (all probes hit the same pool state, no drift).
  // Replaces 6+ separate adminAnalyzeRouteEfficiency calls.
  private func findOptimalArbAmount(
    token : Principal,
    depth : Nat,
    balance : Nat,
  ) : async ?ArbCandidate {
    let tokenText = Principal.toText(token);
    let minSample = lookupMinSample(token);

    if (balance <= minSample) { return null };

    let result = try {
      await exchange.adminFindOptimalArb(
        tokenText,
        minSample,
        balance,
        depth,
        buybackConfig.arbTernaryProbes,
        buybackConfig.arbTernaryTolerancePct,
      );
    } catch (e) {
      logger.warn("BUYBACK", "adminFindOptimalArb threw: " # Error.message(e), "findOptimalArbAmount");
      return null;
    };

    switch (result) {
      case null { null };
      case (?r) {
        // efficiency is signed Int; convert to Nat (we know it's positive
        // because the exchange only returns candidates with outputAmount > amount).
        let profit : Nat = if (r.efficiency > 0) { Int.abs(r.efficiency) } else { 0 };
        if (profit == 0) { return null };
        let profitICP = valueInICP(token, profit);
        ?{
          amount = r.amount;
          route = r.route;
          profit;
          profitICP;
        };
      };
    };
  };

  type ArbExecutionOutcome = {
    #ok : {
      amountOut : Nat;       // realized = notional + grossProfit (before fees)
      tokenOut : Principal;
      minProfit : Nat;       // profit-aware floor passed to adminFlashArb
      grossProfit : Nat;
      netProfit : Nat;       // grossProfit − tradingFee − inputTfees − transferFee
      tradingFee : Nat;
      inputTfees : Nat;
      transferFee : Nat;
      capitalSource : { #lent; #phantom };
    };
    #failed : { reason : Text; minProfit : Nat };
  };

  // Executes a single arb route via the exchange's adminFlashArb (zero-capital
  // flash arbitrage). The exchange lends `amount` from its feescollectedDAO[token]
  // when available, runs the route synchronously, and traps if profit is
  // insufficient — guaranteeing the buyback canister never loses principal.
  //
  // minProfit is profit-aware:
  //
  //   minProfit = (expectedProfit * (10000 - arbProfitSlippageBps)) / 10000
  //
  // Examples (expectedProfit = 100):
  //   slippageBps=0      => minProfit = 100 (full profit required)
  //   slippageBps=5000   => minProfit = 50  (capture >= 50% of expected profit)
  //   slippageBps=10000  => minProfit = 0   (accept any positive profit after fees)
  //
  // The exchange's profit guard checks: grossProfit >= minProfit + tradingFee +
  // inputTfees + transferFee. If the inequality fails, the entire message reverts —
  // no funds move, no record is created.
  private func executeArbIteration(
    _token : Principal,
    amount : Nat,
    expectedProfit : Nat,
    route : [ExchangeType.SwapHop],
  ) : async ArbExecutionOutcome {
    // Compute profit-aware minProfit. Clamp slippage to [0, 10000].
    let slipBps : Nat = if (buybackConfig.arbProfitSlippageBps > 10000) { 10000 } else { buybackConfig.arbProfitSlippageBps };
    let minProfit : Nat = (expectedProfit * (10000 - slipBps)) / 10000;

    // Single inter-canister call — no deposit, no block index, no settlement wait.
    let result = try {
      await exchange.adminFlashArb(amount, route, minProfit);
    } catch (e) {
      // Trap from the exchange (e.g., profit guard failed, partial fill on hop 0,
      // route diverged) is delivered as a catchable error. State on the exchange
      // is reverted by the trap; no funds moved.
      return #failed({ reason = "adminFlashArb trapped/threw: " # Error.message(e); minProfit });
    };

    switch (result) {
      case (#Ok(r)) {
        // Output token = startToken (route is circular). Last hop's tokenOut text
        // equals startToken, but we already have the principal from the caller.
        let outTokenP = _token;
        #ok({
          amountOut = r.realized;
          tokenOut = outTokenP;
          minProfit;
          grossProfit = r.grossProfit;
          netProfit = r.netProfit;
          tradingFee = r.tradingFee;
          inputTfees = r.inputTfees;
          transferFee = r.transferFee;
          capitalSource = r.capitalSource;
        });
      };
      case (#Err(msg)) {
        #failed({ reason = "adminFlashArb err: " # msg; minProfit });
      };
    };
  };

  private func runArbLeg(phase : { #pre; #post }) : async ?BuybackArbRun {
    if (Principal.equal(OTC_BACKEND_ID, OTC_PLACEHOLDER)) {
      return ?{
        phase;
        iterations = [];
        totalProfitPerToken = [];
        status = #skipped("exchange not on production");
      };
    };
    if (not buybackConfig.runArb) {
      return ?{
        phase;
        iterations = [];
        totalProfitPerToken = [];
        status = #skipped("runArb disabled");
      };
    };

    // Flash arb requires no buyback-side capital — the exchange lends from
    // feescollectedDAO or runs phantom mode. arbWorkingCapital* values are now
    // pure SIZE CAPS per token (upper bound for the exchange's ternary search).
    let icpMax = buybackConfig.arbWorkingCapitalICP;
    let usdcMax = buybackConfig.arbWorkingCapitalUSDC;
    let minIcp = lookupMinSample(ICP_LEDGER);
    let minUsdc = lookupMinSample(CKUSDC_LEDGER);

    if (icpMax < minIcp and usdcMax < minUsdc) {
      return ?{
        phase;
        iterations = [];
        totalProfitPerToken = [];
        status = #skipped("arb max-size config below min-sample for both tokens");
      };
    };

    let iterations = Vector.new<BuybackArbIteration>();
    var status : BuybackArbStatus = #stoppedNoProfit;
    var iterIdx : Nat = 0;
    var hitMaxIterations = false;

    label iterLoop while (iterIdx < buybackConfig.arbMaxIterations) {
      // Build candidates per starting token with sufficient capital
      type Cand = { token : Principal; cand : ArbCandidate };
      let candidates = Vector.new<Cand>();

      if (icpMax > minIcp) {
        switch (await findOptimalArbAmount(ICP_LEDGER, buybackConfig.arbDepth, icpMax)) {
          case (?c) { Vector.add(candidates, { token = ICP_LEDGER; cand = c }) };
          case null {};
        };
      };
      if (usdcMax > minUsdc) {
        switch (await findOptimalArbAmount(CKUSDC_LEDGER, buybackConfig.arbDepth, usdcMax)) {
          case (?c) { Vector.add(candidates, { token = CKUSDC_LEDGER; cand = c }) };
          case null {};
        };
      };

      if (Vector.size(candidates) == 0) {
        status := #stoppedNoProfit;
        break iterLoop;
      };

      // Pick winner by absolute ICP-equivalent profit (not BPS — wrong objective)
      var winner : ?Cand = null;
      for (c in Vector.vals(candidates)) {
        switch (winner) {
          case null { winner := ?c };
          case (?w) { if (c.cand.profitICP > w.cand.profitICP) { winner := ?c } };
        };
      };

      let w = switch (winner) {
        case (?x) { x };
        case null { status := #stoppedNoProfit; break iterLoop };
      };

      if (w.cand.profitICP < buybackConfig.arbMinProfitICP) {
        status := #stoppedNoProfit;
        break iterLoop;
      };

      // Execute. Pass expected profit so the iteration can compute a profit-aware
      // minOutput (principal still preserved unconditionally).
      let outcome = await executeArbIteration(w.token, w.cand.amount, w.cand.profit, w.cand.route);

      switch (outcome) {
        case (#ok({ amountOut; tokenOut; minProfit; grossProfit; netProfit; tradingFee; inputTfees; transferFee; capitalSource })) {
          // For flash arb: profit field carries netProfit (what actually landed in
          // the buyback's account). grossProfit/feeFields recorded for audit.
          let profitICP = valueInICP(w.token, netProfit);
          Vector.add(iterations, ({
            iteration = iterIdx;
            startToken = w.token;
            amountIn = w.cand.amount;
            expectedProfit = w.cand.profit;
            minOutput = minProfit;        // legacy field name; carries the floor we required
            amountOut;                     // realized
            profit = netProfit;            // realized − notional − fees − transferFee
            profitICP;
            routeHops = w.cand.route.size();
            result = #ok;
            grossProfit = ?grossProfit;
            tradingFee = ?tradingFee;
            inputTfees = ?inputTfees;
            transferFee = ?transferFee;
            capitalSource = ?capitalSource;
          } : BuybackArbIteration));
          // No balance tracking needed — flash arb consumed no input from buyback's
          // account; netProfit credits buyback's default account on the next ledger
          // settlement (visible in getCurrentBalances after the cycle completes).
          // The icpMax/usdcMax caps remain stable across iterations.
          let _ = tokenOut; // explicitly unused
        };
        case (#failed({ reason; minProfit })) {
          Vector.add(iterations, ({
            iteration = iterIdx;
            startToken = w.token;
            amountIn = w.cand.amount;
            expectedProfit = w.cand.profit;
            minOutput = minProfit;
            amountOut = 0;
            profit = 0;
            profitICP = 0;
            routeHops = w.cand.route.size();
            result = #failed(reason);
            grossProfit = null;
            tradingFee = null;
            inputTfees = null;
            transferFee = null;
            capitalSource = null;
          } : BuybackArbIteration));
          // Balance unchanged — flash arb either traps (no state change) or
          // returns Err early (no state change). Continue to next iteration
          // unless this was a hard error.
          status := #failed(reason);
          break iterLoop;
        };
      };

      iterIdx += 1;
    };

    if (iterIdx >= buybackConfig.arbMaxIterations) { hitMaxIterations := true };
    if (hitMaxIterations) { status := #stoppedMaxIterations };

    // Aggregate per-token profit
    let profitMap = Map.new<Principal, Nat>();
    for (it in Vector.vals(iterations)) {
      let cur = switch (Map.get(profitMap, phash, it.startToken)) { case (?v) v; case null 0 };
      Map.set(profitMap, phash, it.startToken, cur + it.profit);
    };
    let profitArr = Buffer.Buffer<(Principal, Nat)>(Map.size(profitMap));
    for ((t, p) in Map.entries(profitMap)) { profitArr.add((t, p)) };

    ?{
      phase;
      iterations = Vector.toArray(iterations);
      totalProfitPerToken = Buffer.toArray(profitArr);
      status;
    };
  };

  // ────────────────────────────────────────────────────────────────────────
  // Main cycle
  // ────────────────────────────────────────────────────────────────────────

  private func executeBuybackCycleInternal() : async BuybackCycleRecord {
    let cycleId = nextBuybackCycleId;
    nextBuybackCycleId += 1;
    let startedAt = now();

    let baseRecord : BuybackCycleRecord = {
      id = cycleId;
      startedAt;
      finishedAt = startedAt;
      preArb = null;
      claimedFromVault = [];
      swapResults = [];
      totalTacoBurned = 0;
      burnBlockIndex = null;
      postArb = null;
      status = #ok;
    };

    // ── Guards ──
    if (not isProduction()) {
      let r = { baseRecord with finishedAt = now(); status = #skipped_environment };
      appendBuybackHistory(r);
      return r;
    };
    if (not buybackConfig.enabled) {
      let r = { baseRecord with finishedAt = now(); status = #skipped_disabled };
      appendBuybackHistory(r);
      return r;
    };
    if (cycleInProgress) {
      let r = { baseRecord with finishedAt = now(); status = #skipped_already_running };
      appendBuybackHistory(r);
      return r;
    };
    cycleInProgress := true;

    // Refresh token details from treasury (no-op if cache is fresh)
    let _ = try { await syncTokenDetailsFromTreasuryInternal(false) } catch (e) {
      logger.warn("BUYBACK", "token details sync failed: " # Error.message(e), "executeBuybackCycle");
      #err("sync failed");
    };

    // Re-fetch slippage tolerance (synced via tokenDetailsCache or via a separate query;
    // for V1 we use a fixed conservative value matching treasury's prod default).
    let maxSlippageBp : Nat = 450; // 4.5% — matches treasury production default

    // ── Step 1: Pre-arb ──
    let preArb = await runArbLeg(#pre);

    // ── Step 2: Vault claim ──
    let claimedFromVault = Buffer.Buffer<(Principal, Nat)>(8);
    if (buybackConfig.sweepVault) {
      let claimResult = try {
        await nachosVault.claimAllFees(this_canister_id(), null);
      } catch (e) {
        logger.warn("BUYBACK", "vault claimAllFees failed: " # Error.message(e), "executeBuybackCycle");
        #err("exception: " # Error.message(e));
      };
      switch (claimResult) {
        case (#ok(r)) {
          let agg = Map.new<Principal, Nat>();
          for ((t, a) in r.mint.vals()) {
            let cur = switch (Map.get(agg, phash, t)) { case (?v) v; case null 0 };
            Map.set(agg, phash, t, cur + a);
          };
          for ((t, a) in r.burn.vals()) {
            let cur = switch (Map.get(agg, phash, t)) { case (?v) v; case null 0 };
            Map.set(agg, phash, t, cur + a);
          };
          for ((t, a) in r.cancellation.vals()) {
            let cur = switch (Map.get(agg, phash, t)) { case (?v) v; case null 0 };
            Map.set(agg, phash, t, cur + a);
          };
          for ((t, a) in Map.entries(agg)) { claimedFromVault.add((t, a)) };
        };
        case (#err(e)) {
          logger.warn("BUYBACK", "vault claim returned err: " # e, "executeBuybackCycle");
        };
      };
    };

    // ── Step 3: Snapshot non-ICP/non-TACO tokens to convert to ICP first ──
    // Strategy: consolidate everything into ICP, then do a SINGLE ICP→TACO swap
    // before burn. Keeps slippage small and price discovery clean.
    //
    // Candidates include:
    //   1. Tokens just claimed from the nachos vault
    //   2. ckUSDC (in case prior arb left a balance)
    //   3. Every token in the local tokenDetailsCache — catches leftover
    //      balances from failed swaps / wrong trades / accidental transfers.
    type ToSwap = { token : Principal; amount : Nat };
    let toIcp = Buffer.Buffer<ToSwap>(8);

    let mgmtCanister = Principal.fromText("aaaaa-aa");
    let candidateSet = Map.new<Principal, Bool>();
    for ((t, _) in claimedFromVault.vals()) { Map.set(candidateSet, phash, t, true) };
    Map.set(candidateSet, phash, CKUSDC_LEDGER, true);
    // Sweep every cached token — catches any orphaned balance from prior
    // failed/wrong trades that left tokens in the default account. Filter out
    // any placeholder/management-canister entries (tokens not yet deployed).
    for ((t, _) in Map.entries(tokenDetailsCache)) {
      if (not Principal.equal(t, mgmtCanister)) {
        Map.set(candidateSet, phash, t, true);
      };
    };

    for ((token, _) in Map.entries(candidateSet)) {
      // Skip ICP itself (already the consolidation target) and TACO (already the burn target)
      if (Principal.equal(token, ICP_LEDGER) or Principal.equal(token, TACO_LEDGER)) {
        // no-op
      } else {
        let bal = await balanceOf(token);
        let f = tokenFeeOf(token);
        // The outgoing ICRC-1 transfer to the swap pool requires (amount + fee)
        // available; pass at most (bal - fee) as the swap input.
        if (bal > f * 2) { toIcp.add({ token; amount = bal - f }) };
      };
    };

    let swapResults = Buffer.Buffer<BuybackSwapResult>(toIcp.size() + 1);

    // ── Step 4a: Convert each fee/profit token to ICP ──
    for (item in toIcp.vals()) {
      let (sr, _addedICP) = await swapToken(item.token, ICP_LEDGER, item.amount, maxSlippageBp);
      swapResults.add(sr);
    };

    // ── Step 4b: Single ICP → TACO swap of all accumulated ICP ──
    let icpBal = await balanceOf(ICP_LEDGER);
    let icpFee = tokenFeeOf(ICP_LEDGER);
    // Need (amount + ledger fee) available — pass at most (bal - fee).
    if (icpBal > icpFee * 2) {
      let (sr, _addedTaco) = await swapToken(ICP_LEDGER, TACO_LEDGER, icpBal - icpFee, maxSlippageBp);
      swapResults.add(sr);
    };

    // ── Step 5: Burn ──
    let burnRes = await burnAccumulatedTaco();

    // ── Step 6: Post-arb ──
    let postArb = await runArbLeg(#post);

    // ── Determine status ──
    var hasErrors = false;
    for (sr in swapResults.vals()) {
      switch (sr.error) { case (?_) hasErrors := true; case null {} };
    };
    let preArbHasError = switch (preArb) {
      case (?run) { switch (run.status) { case (#failed(_)) true; case _ false } };
      case null { false };
    };
    let postArbHasError = switch (postArb) {
      case (?run) { switch (run.status) { case (#failed(_)) true; case _ false } };
      case null { false };
    };
    let finalStatus : BuybackCycleStatus = switch (burnRes.error) {
      case (?reason) { #failed(reason) };
      case null { if (hasErrors or preArbHasError or postArbHasError) { #partial } else { #ok } };
    };

    let finalRecord : BuybackCycleRecord = {
      id = cycleId;
      startedAt;
      finishedAt = now();
      preArb;
      claimedFromVault = Buffer.toArray(claimedFromVault);
      swapResults = Buffer.toArray(swapResults);
      totalTacoBurned = burnRes.burned;
      burnBlockIndex = burnRes.blockIndex;
      postArb;
      status = finalStatus;
    };
    appendBuybackHistory(finalRecord);
    cycleInProgress := false;
    finalRecord;
  };

  // ────────────────────────────────────────────────────────────────────────
  // Timer
  // ────────────────────────────────────────────────────────────────────────

  private func startBuybackTimer<system>() {
    if (buybackTimerId != 0) {
      cancelTimer(buybackTimerId);
      buybackTimerId := 0;
    };
    if (not buybackConfig.enabled) { return };
    if (not isProduction()) { return };

    buybackTimerId := setTimer<system>(
      #nanoseconds(buybackConfig.intervalNS),
      func() : async () {
        try { ignore await executeBuybackCycleInternal() }
        catch (e) {
          logger.error("BUYBACK", "cycle threw: " # Error.message(e), "buybackTimer");
        };
        startBuybackTimer<system>();
      },
    );
  };

  // ────────────────────────────────────────────────────────────────────────
  // Admin endpoints
  // ────────────────────────────────────────────────────────────────────────

  public shared ({ caller }) func updateBuybackConfig(patch : BuybackConfigUpdate)
    : async Result.Result<BuybackConfig, Text>
  {
    if (not (await authForFunction(caller, #updateBuybackConfig))) { return #err("Not authorized") };

    // Validation
    switch (patch.enabled) {
      case (?true) {
        if (not isProduction()) { return #err("Buyback can only be enabled on production") };
      };
      case _ {};
    };
    switch (patch.intervalNS) {
      case (?v) {
        if (v < 3_600_000_000_000 or v > 30 * 86_400_000_000_000) {
          return #err("intervalNS out of range [1 hour, 30 days]");
        };
      };
      case null {};
    };
    switch (patch.minTokenValueICP) {
      case (?v) { if (v == 0) { return #err("minTokenValueICP must be > 0") } };
      case null {};
    };
    switch (patch.arbDepth) {
      case (?v) { if (v < 2 or v > 6) { return #err("arbDepth out of range [2, 6]") } };
      case null {};
    };
    switch (patch.arbTernaryProbes) {
      case (?v) { if (v < 3 or v > 20) { return #err("arbTernaryProbes out of range [3, 20]") } };
      case null {};
    };
    switch (patch.arbMaxIterations) {
      case (?v) { if (v < 1 or v > 50) { return #err("arbMaxIterations out of range [1, 50]") } };
      case null {};
    };
    switch (patch.arbMaxRoutesPerAnalysis) {
      case (?v) { if (v < 1 or v > 20) { return #err("arbMaxRoutesPerAnalysis out of range [1, 20]") } };
      case null {};
    };
    switch (patch.arbSettlementTimeoutMs) {
      case (?v) { if (v < 5_000 or v > 120_000) { return #err("arbSettlementTimeoutMs out of range [5000, 120000]") } };
      case null {};
    };
    switch (patch.arbProfitSlippageBps) {
      case (?v) { if (v > 10000) { return #err("arbProfitSlippageBps out of range [0, 10000]") } };
      case null {};
    };

    let was_enabled = buybackConfig.enabled;
    buybackConfig := {
      enabled = switch (patch.enabled) { case (?v) v; case null buybackConfig.enabled };
      intervalNS = switch (patch.intervalNS) { case (?v) v; case null buybackConfig.intervalNS };
      minTokenValueICP = switch (patch.minTokenValueICP) { case (?v) v; case null buybackConfig.minTokenValueICP };
      sweepVault = switch (patch.sweepVault) { case (?v) v; case null buybackConfig.sweepVault };
      sweepExchange = switch (patch.sweepExchange) { case (?v) v; case null buybackConfig.sweepExchange };
      runArb = switch (patch.runArb) { case (?v) v; case null buybackConfig.runArb };
      arbWorkingCapitalICP = switch (patch.arbWorkingCapitalICP) { case (?v) v; case null buybackConfig.arbWorkingCapitalICP };
      arbWorkingCapitalUSDC = switch (patch.arbWorkingCapitalUSDC) { case (?v) v; case null buybackConfig.arbWorkingCapitalUSDC };
      arbDepth = switch (patch.arbDepth) { case (?v) v; case null buybackConfig.arbDepth };
      arbMinSampleAmount = switch (patch.arbMinSampleAmount) { case (?v) v; case null buybackConfig.arbMinSampleAmount };
      arbTernaryProbes = switch (patch.arbTernaryProbes) { case (?v) v; case null buybackConfig.arbTernaryProbes };
      arbTernaryTolerancePct = switch (patch.arbTernaryTolerancePct) { case (?v) v; case null buybackConfig.arbTernaryTolerancePct };
      arbMinProfitICP = switch (patch.arbMinProfitICP) { case (?v) v; case null buybackConfig.arbMinProfitICP };
      arbMaxIterations = switch (patch.arbMaxIterations) { case (?v) v; case null buybackConfig.arbMaxIterations };
      arbMaxRoutesPerAnalysis = switch (patch.arbMaxRoutesPerAnalysis) { case (?v) v; case null buybackConfig.arbMaxRoutesPerAnalysis };
      arbSettlementTimeoutMs = switch (patch.arbSettlementTimeoutMs) { case (?v) v; case null buybackConfig.arbSettlementTimeoutMs };
      arbProfitSlippageBps = switch (patch.arbProfitSlippageBps) { case (?v) v; case null buybackConfig.arbProfitSlippageBps };
      tokenDetailsStaleAfterNS = switch (patch.tokenDetailsStaleAfterNS) { case (?v) v; case null buybackConfig.tokenDetailsStaleAfterNS };
    };

    if (buybackConfig.enabled and not was_enabled) {
      startBuybackTimer<system>();
    } else if (not buybackConfig.enabled and was_enabled) {
      if (buybackTimerId != 0) {
        cancelTimer(buybackTimerId);
        buybackTimerId := 0;
      };
    };

    logger.info("BUYBACK", "config updated by " # Principal.toText(caller), "updateBuybackConfig");
    #ok(buybackConfig);
  };

  public shared ({ caller }) func triggerBuybackNow() : async Result.Result<BuybackCycleRecord, Text> {
    if (not (await authForFunction(caller, #triggerBuyback))) { return #err("Not authorized") };
    let r = await executeBuybackCycleInternal();
    #ok(r);
  };

  public shared ({ caller }) func syncTokenDetailsFromTreasury() : async Result.Result<{ tokensRefreshed : Nat }, Text> {
    if (not (await authForFunction(caller, #syncBuybackTokenDetails))) { return #err("Not authorized") };
    await syncTokenDetailsFromTreasuryInternal(true);
  };

  // Emergency: drain a token from buyback canister to a specific recipient.
  // Controllers only — used if buyback canister gets stuck somehow.
  public shared ({ caller }) func adminWithdraw(
    token : Principal,
    to : Principal,
    amount : Nat,
  ) : async Result.Result<Nat, Text> {
    if (not Principal.isController(caller)) { return #err("Not authorized") };
    let ledger = ledgerForToken(token);
    let r = try {
      await ledger.icrc1_transfer({
        from_subaccount = null;
        to = { owner = to; subaccount = null };
        amount = amount;
        fee = null;
        memo = ?Text.encodeUtf8("buyback-admin-withdraw");
        created_at_time = null;
      });
    } catch (e) { return #err("threw: " # Error.message(e)) };
    switch (r) {
      case (#Ok(b)) {
        logger.warn("BUYBACK", "ADMIN WITHDREW " # Nat.toText(amount) # " of " # Principal.toText(token) # " to " # Principal.toText(to) # " by " # Principal.toText(caller), "adminWithdraw");
        #ok(b);
      };
      case (#Err(e)) #err(debug_show(e));
    };
  };

  // Emergency: trigger burn outside of the normal cycle.
  public shared ({ caller }) func adminBurnNow() : async Result.Result<{ burned : Nat; blockIndex : ?Nat }, Text> {
    if (not (await authForFunction(caller, #buybackAdminBurn))) { return #err("Not authorized") };
    let r = await burnAccumulatedTaco();
    switch (r.error) {
      case (?e) #err(e);
      case null #ok({ burned = r.burned; blockIndex = r.blockIndex });
    };
  };

  // Emergency stop. Stronger than `updateBuybackConfig({ enabled = ?false })`:
  //   - Cancels the timer (so even an in-flight scheduled fire is cancelled)
  //   - Clears cycleInProgress (so a future re-enable doesn't think a cycle is running)
  //   - Sets enabled=false in stable config
  // Use when something is going wrong and you want immediate hard-stop without
  // waiting for the current cycle to finish or the next firing to skip.
  public shared ({ caller }) func emergencyStop(reason : Text) : async Result.Result<Text, Text> {
    if (not (await authForFunction(caller, #updateBuybackConfig))) { return #err("Not authorized") };
    if (buybackTimerId != 0) {
      cancelTimer(buybackTimerId);
      buybackTimerId := 0;
    };
    cycleInProgress := false;
    buybackConfig := { buybackConfig with enabled = false };
    logger.warn("BUYBACK", "EMERGENCY STOP by " # Principal.toText(caller) # " - reason: " # reason, "emergencyStop");
    #ok("Emergency stop applied. Timer cancelled, cycleInProgress cleared, enabled=false. Use updateBuybackConfig to re-enable.");
  };

  // ────────────────────────────────────────────────────────────────────────
  // Query endpoints
  // ────────────────────────────────────────────────────────────────────────

  public query func getBuybackConfig() : async BuybackConfig { buybackConfig };

  public query func getBuybackHistory(limit : Nat) : async [BuybackCycleRecord] {
    let total = Vector.size(buybackHistoryV2);
    if (total == 0 or limit == 0) { return [] };
    let take = if (limit < total) { limit } else { total };
    let start : Nat = total - take;
    let buf = Buffer.Buffer<BuybackCycleRecord>(take);
    var i : Nat = start;
    while (i < total) { buf.add(Vector.get(buybackHistoryV2, i)); i += 1 };
    Buffer.toArray(buf);
  };

  public query func getTotalBurned() : async Nat { totalBurned };

  public query func getCachedTokenDetails() : async [(Principal, CachedTokenDetails)] {
    let buf = Buffer.Buffer<(Principal, CachedTokenDetails)>(Map.size(tokenDetailsCache));
    for ((p, d) in Map.entries(tokenDetailsCache)) { buf.add((p, d)) };
    Buffer.toArray(buf);
  };

  public func getCurrentBalances() : async [(Principal, Nat)] {
    let buf = Buffer.Buffer<(Principal, Nat)>(8);
    let icp = await balanceOf(ICP_LEDGER);
    let usdc = await balanceOf(CKUSDC_LEDGER);
    let taco = await balanceOf(TACO_LEDGER);
    if (icp > 0) buf.add((ICP_LEDGER, icp));
    if (usdc > 0) buf.add((CKUSDC_LEDGER, usdc));
    if (taco > 0) buf.add((TACO_LEDGER, taco));
    let mgmtCanister = Principal.fromText("aaaaa-aa");
    for ((p, _) in Map.entries(tokenDetailsCache)) {
      if (not Principal.equal(p, ICP_LEDGER) and not Principal.equal(p, CKUSDC_LEDGER) and not Principal.equal(p, TACO_LEDGER) and not Principal.equal(p, mgmtCanister)) {
        let bal = await balanceOf(p);
        if (bal > 0) buf.add((p, bal));
      };
    };
    Buffer.toArray(buf);
  };

  // Aggregated stats over the entire buybackHistoryV2.
  // Pure local computation — fast query, safe to call frequently.
  public query func getStats() : async BuybackTypes.BuybackStats {
    let total = Vector.size(buybackHistoryV2);
    var ok : Nat = 0;
    var partial : Nat = 0;
    var failed : Nat = 0;
    var skipped : Nat = 0;
    var arbOk : Nat = 0;
    var arbRefunded : Nat = 0;
    var arbFailed : Nat = 0;
    var sumWallClock : Int = 0;
    var wallClockSamples : Nat = 0;
    // Profit ratio: sum of (realized_profit * 10000 / expected_profit) over arb-#ok
    // iterations where expectedProfit > 0; divided by sample count at the end.
    var profitRatioSum : Nat = 0;
    var profitRatioSamples : Nat = 0;
    let profitMap = Map.new<Principal, Nat>();

    let accumulateArb = func(run : BuybackTypes.BuybackArbRun) {
      for (it in run.iterations.vals()) {
        switch (it.result) {
          case (#ok) {
            arbOk += 1;
            // Track per-token profit
            let cur = switch (Map.get(profitMap, phash, it.startToken)) { case (?v) v; case null 0 };
            Map.set(profitMap, phash, it.startToken, cur + it.profit);
            // Track realized-vs-expected ratio
            if (it.expectedProfit > 0) {
              profitRatioSum += (it.profit * 10000) / it.expectedProfit;
              profitRatioSamples += 1;
            };
          };
          case (#refunded) { arbRefunded += 1 };
          case (#failed(_)) { arbFailed += 1 };
        };
      };
    };

    var i : Nat = 0;
    while (i < total) {
      let cycle = Vector.get(buybackHistoryV2, i);
      switch (cycle.status) {
        case (#ok) ok += 1;
        case (#partial) partial += 1;
        case (#failed(_)) failed += 1;
        case (#skipped_environment) skipped += 1;
        case (#skipped_disabled) skipped += 1;
        case (#skipped_already_running) skipped += 1;
      };
      // Wall-clock — only count cycles that actually ran (not pure-skip cycles
      // where startedAt == finishedAt or skip statuses).
      let isSkipped = switch (cycle.status) {
        case (#skipped_environment or #skipped_disabled or #skipped_already_running) true;
        case _ false;
      };
      if (not isSkipped) {
        sumWallClock += cycle.finishedAt - cycle.startedAt;
        wallClockSamples += 1;
      };
      switch (cycle.preArb) { case (?run) accumulateArb(run); case null {} };
      switch (cycle.postArb) { case (?run) accumulateArb(run); case null {} };
      i += 1;
    };

    let totalArbIter : Nat = arbOk + arbRefunded + arbFailed;
    let refundRateBps : Nat = if (totalArbIter == 0) { 0 } else { (arbRefunded * 10000) / totalArbIter };
    let avgWallClock : Int = if (wallClockSamples == 0) { 0 } else { sumWallClock / wallClockSamples };
    let avgProfitRatioBps : Nat = if (profitRatioSamples == 0) { 0 } else { profitRatioSum / profitRatioSamples };

    let profitArr = Buffer.Buffer<(Principal, Nat)>(Map.size(profitMap));
    for ((t, p) in Map.entries(profitMap)) { profitArr.add((t, p)) };

    {
      totalCycles = total;
      cyclesOk = ok;
      cyclesPartial = partial;
      cyclesFailed = failed;
      cyclesSkipped = skipped;
      totalTacoBurned = totalBurned;
      totalArbProfitPerToken = Buffer.toArray(profitArr);
      totalArbIterations = totalArbIter;
      arbIterationsOk = arbOk;
      arbIterationsRefunded = arbRefunded;
      arbIterationsFailed = arbFailed;
      refundRateBps;
      avgCycleWallClockNs = avgWallClock;
      avgRealizedToExpectedProfitBps = avgProfitRatioBps;
    };
  };

  // Single-pane operator view. Shared (not query) because it makes ICRC-1
  // balance queries to live ledgers.
  public func getHealth() : async BuybackTypes.BuybackHealth {
    let icp = await balanceOf(ICP_LEDGER);
    let usdc = await balanceOf(CKUSDC_LEDGER);
    let taco = await balanceOf(TACO_LEDGER);
    let total = Vector.size(buybackHistoryV2);
    let lastCycle = if (total == 0) { null } else { ?Vector.get(buybackHistoryV2, total - 1) };
    let lastCycleAt = switch (lastCycle) { case (?c) ?c.finishedAt; case null null };
    let lastCycleStatus = switch (lastCycle) { case (?c) ?c.status; case null null };
    {
      environment = canister_ids.getEnvironment();
      enabled = buybackConfig.enabled;
      runArb = buybackConfig.runArb;
      exchangeOnProd = not Principal.equal(OTC_BACKEND_ID, OTC_PLACEHOLDER);
      cycleInProgress;
      timerActive = buybackTimerId != 0;
      liveBalances = { icp; usdc; taco };
      pendingTacoToBurn = taco;
      lastCycleAt;
      lastCycleStatus;
      totalBurned;
      cachedTokenCount = Map.size(tokenDetailsCache);
      lastTokenSyncAt = lastTokenDetailsSync;
    };
  };

  // ────────────────────────────────────────────────────────────────────────
  // Lifecycle
  // ────────────────────────────────────────────────────────────────────────

  system func preupgrade() {
    // All important state is stable; transient state (cycleInProgress, skipKong/Taco
    // pair maps, timer id) is recomputed/recreated on postupgrade.
  };

  system func postupgrade() {
    buybackTimerId := 0;
    cycleInProgress := false;
    skipKongPair := Map.new();
    skipTacoPair := Map.new();
    startBuybackTimer<system>();
  };

  // Initialize the timer at module init (gated on env=Production AND
  // buybackConfig.enabled inside startBuybackTimer — no-ops otherwise).
  startBuybackTimer<system>();

};
