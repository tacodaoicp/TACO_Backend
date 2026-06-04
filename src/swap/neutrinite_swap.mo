import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Error "mo:base/Error";
import Float "mo:base/Float";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Time "mo:base/Time";
import Types "./swap_types";

module {
  // Neutrinite ICRC-55 pylon DEX adapter — mirrors src/swap/kong_swap.mo.
  // Stateless module{}, hardcoded pylon ID, every inter-canister await uses (with timeout = 65),
  // every public fn returns Result / never traps.

  private let PYLON_CANISTER_ID = "togwv-zqaaa-aaaal-qr7aa-cai";

  // Credit-poll bound = WALL-CLOCK cap (POLL_CAP_NS) AND a hard iteration backstop (MAX_POLL_ITERS).
  // We wait by REAL time, not iteration count: a fresh deposit credits slowly relative to a tight
  // inter-canister await loop. If the credit doesn't land within ~90s the trade can't proceed, so the
  // caller fails the leg and 3-day-skips ("N") that pair — so this (up to ~45-60-query) burst happens at
  // most ONCE per slow-indexed pair per 3 days; fast tokens (ICP, 3s scan) exit in ~3-5 polls on credit.
  // NOTE: on the SPLIT path this whole dance runs inside a best-effort self-call whose deadline
  // (treasury executeSplitTrade, `with timeout`) MUST exceed POLL_CAP + the ~4 fixed non-poll calls
  // (resolve/transfer/swap/command) — that wrapper is set to 150s. (Real ~every-10s polling would need a
  // timer-driven flow off the trade cycle; not done here.)
  private let POLL_CAP_NS : Int = 90_000_000_000; // 90s wall-clock
  private let MAX_POLL_ITERS : Nat = 60;

  // Local copy of treasury's isFiniteFloat (modules can't see privates).
  private func isFiniteFloat(x : Float) : Bool {
    not Float.isNaN(x) and x < 9.0e18 and x > -9.0e18;
  };

  // ── getQuote ──
  // Slippage = PERCENT from RAW before_price (out-per-in, to-per-from, pre-impact). before_price and
  // the amounts are BOTH raw -> decimals CANCEL. Do NOT copy Kong's human normalization. sellDecimals/
  // buyDecimals stay in the signature for symmetry but are unused.
  public func getQuote(
    sellLedger : Principal,
    buyLedger : Principal,
    amountIn : Nat,
    sellDecimals : Nat,
    buyDecimals : Nat,
  ) : async Result.Result<Types.NeutriniteQuoteResult, Text> {
    ignore sellDecimals;
    ignore buyDecimals;
    try {
      let pylon : Types.NeutritePylon = actor (PYLON_CANISTER_ID);
      let req : Types.NeutriniteQuoteRequest = {
        amount = amountIn;
        ledger_from = #ic(sellLedger);
        ledger_to = #ic(buyLedger);
      };
      let result = await (with timeout = 65) pylon.dex_quote(req);
      switch (result) {
        case (#ok(q)) {
          if (q.amount_out == 0) { return #err("Neutrinite: zero amount_out (no liquidity)") };
          let spotExpectedOut = Float.fromInt(amountIn) * q.before_price; // raw out-units
          let amountOutF = Float.fromInt(q.amount_out);
          let slippage = if (isFiniteFloat(q.before_price) and spotExpectedOut > amountOutF and spotExpectedOut > 0.0) {
            (spotExpectedOut - amountOutF) / spotExpectedOut * 100.0;
          } else { 0.0 };
          #ok({ amount_out = q.amount_out; slippage = slippage; before_price = q.before_price });
        };
        case (#err(e)) { #err("Neutrinite quote error: " # e) }; // "No price for exchange" = illiquid
      };
    } catch (e) { #err("Error calling Neutrinite pylon: " # Error.message(e)) };
  };

  // ── register ── icrc55_account_register({owner=self; subaccount=null}); idempotent on the pylon.
  public func register(selfPrincipal : Principal) : async Result.Result<(), Text> {
    try {
      let pylon : Types.NeutritePylon = actor (PYLON_CANISTER_ID);
      await (with timeout = 65) pylon.icrc55_account_register({ owner = selfPrincipal; subaccount = null });
      #ok(());
    } catch (e) { #err("Neutrinite register failed: " # Error.message(e)) };
  };

  // ── getLedgerFollowSettings ── per-ledger indexer cadence (follow_interval_sec). Used by the
  // treasury to proactively skip Neutrinite for sell tokens that index too slowly to credit a deposit
  // inside the poll window. Read-only query; never traps.
  public func getLedgerFollowSettings() : async Result.Result<[(Principal, Nat)], Text> {
    try {
      let pylon : Types.NeutritePylon = actor (PYLON_CANISTER_ID);
      let settings = await (with timeout = 65) pylon.ledger_follow_settings();
      let buf = Buffer.Buffer<(Principal, Nat)>(settings.size());
      for (s in settings.vals()) { buf.add((s.ledger, s.follow_interval_sec)) };
      #ok(Buffer.toArray(buf));
    } catch (e) { #err("Neutrinite ledger_follow_settings error: " # Error.message(e)) };
  };

  // ── private: resolve the #ic deposit endpoint for a ledger ──
  private func resolveDepositAccount(
    selfPrincipal : Principal,
    ledger : Principal,
  ) : async Result.Result<(Types.NeutriniteAccount, Nat), Text> {
    try {
      let pylon : Types.NeutritePylon = actor (PYLON_CANISTER_ID);
      let accounts = await (with timeout = 65) pylon.icrc55_accounts({ owner = selfPrincipal; subaccount = null });
      for (entry in accounts.vals()) {
        switch (entry.endpoint) {
          case (#ic(icEp)) { if (Principal.equal(icEp.ledger, ledger)) { return #ok((icEp.account, entry.balance)) } };
          case (#other(_)) {};
        };
      };
      #err("Neutrinite: no #ic deposit endpoint for ledger " # Principal.toText(ledger) # " (needs register)");
    } catch (e) { #err("Neutrinite resolveDepositAccount error: " # Error.message(e)) };
  };

  // ── private: virtual balance for one #ic ledger (0 if absent) ──
  private func balanceForLedger(selfPrincipal : Principal, ledger : Principal) : async Result.Result<Nat, Text> {
    try {
      let pylon : Types.NeutritePylon = actor (PYLON_CANISTER_ID);
      let accounts = await (with timeout = 65) pylon.icrc55_accounts({ owner = selfPrincipal; subaccount = null });
      for (entry in accounts.vals()) {
        switch (entry.endpoint) {
          case (#ic(icEp)) { if (Principal.equal(icEp.ledger, ledger)) { return #ok(entry.balance) } };
          case (#other(_)) {};
        };
      };
      #ok(0);
    } catch (e) { #err("Neutrinite balanceForLedger error: " # Error.message(e)) };
  };

  // ── private: poll for ANY increase over balanceBefore -> measured creditedDelta ──
  // Bounded by REAL wall-clock (POLL_CAP_NS) AND an iteration backstop (MAX_POLL_ITERS, so a fast-returning
  // icrc55_accounts can't spin hundreds of queries). Breaks IMMEDIATELY on credit. On exhaustion -> #err
  // whose message KEEPS the literal substring "not credited" (the caller's skip + register-reset gate on
  // it). Funds are NOT lost on timeout (deposit sits in the pylon virtual balance, reclaimed by the sweep).
  // The #ok/#err text carries elapsed-ms so the treasury logger can surface real credit latency.
  private func pollForCredit(selfPrincipal : Principal, ledger : Principal, balanceBefore : Nat) : async Result.Result<Nat, Text> {
    let start = Time.now();
    var i : Nat = 0;
    while (i < MAX_POLL_ITERS and (Time.now() - start) < POLL_CAP_NS) {
      let balResult = await balanceForLedger(selfPrincipal, ledger); // each await self-paces ~1-2s
      switch (balResult) {
        case (#ok(bal)) {
          if (bal > balanceBefore) {
            let elapsedMs = (Time.now() - start) / 1_000_000;
            Debug.print("Neutrinite.pollForCredit: credited after " # Int.toText(elapsedMs) # "ms (" # Nat.toText(i + 1) # " polls)");
            return #ok(bal - balanceBefore : Nat);
          };
        };
        case (#err(_)) {}; // transient: keep polling
      };
      i += 1;
    };
    let elapsedMs = (Time.now() - start) / 1_000_000;
    #err("Neutrinite: deposit not credited within " # Int.toText(elapsedMs) # "ms / " # Nat.toText(i) # " polls (funds in pylon virtual balance, recoverable by sweep)");
  };

  // ── executeTransferAndSwap ── full 7-step dance; NEVER traps; returns GROSS amountOut.
  // Caller passes the FULL-leg minAmountOut; this fn re-scales by creditedDelta/amountIn (once).
  public func executeTransferAndSwap(params : Types.NeutriniteParams) : async Result.Result<Types.NeutriniteSwapResult, Text> {
    try {
      let pylon : Types.NeutritePylon = actor (PYLON_CANISTER_ID);
      let self = params.selfPrincipal;

      // 1: resolve deposit endpoint + balanceBefore
      let depositResult = await resolveDepositAccount(self, params.sellLedger);
      let (depositAccount, balanceBefore) = switch (depositResult) { case (#ok(v)) { v }; case (#err(e)) { return #err(e) } };

      // 2: plain icrc1_transfer(amountIn) from treasury sub-0 to the pylon deposit account
      let sellToken : Types.ICRC1 = actor (Principal.toText(params.sellLedger));
      let transferArgs : Types.ICRC1TransferArgs = {
        to = {
          owner = depositAccount.owner;
          subaccount = switch (depositAccount.subaccount) { case (?b) { ?Blob.toArray(b) }; case null { null } };
        };
        fee = null;
        memo = null;
        from_subaccount = null;
        created_at_time = null;
        amount = params.amountIn;
      };
      let xferResult = await (with timeout = 65) sellToken.icrc1_transfer(transferArgs);
      switch (xferResult) { case (#Ok(_)) {}; case (#Err(e)) { return #err("Neutrinite deposit transfer failed: " # debug_show (e)) } };

      // 3: poll for ANY increase -> measured creditedDelta
      let creditResult = await pollForCredit(self, params.sellLedger, balanceBefore);
      let creditedDelta = switch (creditResult) { case (#ok(d)) { d }; case (#err(e)) { return #err(e) } };
      if (creditedDelta == 0) { return #err("Neutrinite: creditedDelta is 0 (unexpected)") };

      // 4: scale the full-leg minAmountOut by the actual credited fraction
      let scaledMinOut : Nat = if (params.amountIn == 0) { 0 } else { params.minAmountOut * creditedDelta / params.amountIn };

      // 5: dex_swap the credited delta
      let swapResult = await (with timeout = 65) pylon.dex_swap({
        account = { owner = self; subaccount = null };
        amount = creditedDelta;
        ledger_from = #ic(params.sellLedger);
        ledger_to = #ic(params.buyLedger);
        min_amount_out = scaledMinOut;
      });
      let grossOut = switch (swapResult) {
        case (#ok(s)) { s.amount_out };
        case (#err(e)) { return #err("Neutrinite swap failed (sell tokens in pylon, recoverable): " # e) };
      };
      if (grossOut == 0) { return #err("Neutrinite swap returned 0 (buy tokens may be in pylon, recoverable)") };

      // 6: icrc55_command withdraw grossOut of buyLedger back to treasury sub-0 (enqueued; async)
      let batch : Types.NeutriniteBatchCommandRequest = {
        commands = [#transfer({
          amount = grossOut;
          from = #account({ owner = self; subaccount = null });
          ledger = #ic(params.buyLedger);
          memo = null;
          to = #external_account(#ic({ owner = self; subaccount = null }));
        })];
        controller = { owner = self; subaccount = null };
        expire_at = null;
        request_id = null;
        signature = null;
      };
      let cmdResult = await (with timeout = 65) pylon.icrc55_command(batch);
      switch (cmdResult) { case (#ok(_)) {}; case (#err(e)) { return #err("Neutrinite withdraw command failed (buy tokens in pylon, recoverable): " # debug_show (e)) } };

      // 7: report GROSS amountOut. Output arrives async -> treasury briefly under-counts (safe direction).
      #ok({ amountOut = grossOut; slippage = 0.0 });
    } catch (e) { #err("Error in Neutrinite executeTransferAndSwap: " # Error.message(e)) };
  };

  // Thin alias for the split-leg future (mirrors taco_swap.mo:526).
  public func executeTransferAndSwapNoTracking(params : Types.NeutriniteParams) : async Result.Result<Types.NeutriniteSwapResult, Text> {
    await executeTransferAndSwap(params);
  };

  // ── recoverNeutriniteBalances (sweep) ── tokens==[] sweeps all #ic endpoints. async*. Never traps.
  // Returns the number of ledgers swept (0 = nothing to recover) so the caller can log/audit recovery
  // (a silent #ok previously hid successful sweeps from getLogs).
  public func recoverNeutriniteBalances(selfPrincipal : Principal, tokens : [Principal]) : async* Nat {
    try {
      let pylon : Types.NeutritePylon = actor (PYLON_CANISTER_ID);
      let accounts = await (with timeout = 65) pylon.icrc55_accounts({ owner = selfPrincipal; subaccount = null });
      let sweepAll = tokens.size() == 0;
      let commands = Buffer.Buffer<Types.NeutriniteCommand>(accounts.size());
      for (entry in accounts.vals()) {
        switch (entry.endpoint) {
          case (#ic(icEp)) {
            let wanted = sweepAll or (Array.find<Principal>(tokens, func(p) = Principal.equal(p, icEp.ledger)) != null);
            if (wanted and entry.balance > 0) {
              commands.add(#transfer({
                amount = entry.balance;
                from = #account({ owner = selfPrincipal; subaccount = null });
                ledger = #ic(icEp.ledger);
                memo = null;
                to = #external_account(#ic({ owner = selfPrincipal; subaccount = null }));
              }));
            };
          };
          case (#other(_)) {};
        };
      };
      if (commands.size() == 0) { return 0 };
      let batch : Types.NeutriniteBatchCommandRequest = {
        commands = Buffer.toArray(commands);
        controller = { owner = selfPrincipal; subaccount = null };
        expire_at = null;
        request_id = null;
        signature = null;
      };
      let cmdResult = await (with timeout = 65) pylon.icrc55_command(batch);
      switch (cmdResult) {
        case (#ok(_)) { commands.size() };
        case (#err(e)) { Debug.print("Neutrinite sweep err: " # debug_show (e)); 0 };
      };
    } catch (e) { Debug.print("Neutrinite recover exception: " # Error.message(e)); 0 };
  };
};
