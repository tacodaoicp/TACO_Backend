import ICRC2 "../src/icrc.types";
import Map "mo:map/Map";
import Vector "mo:vector";
import Principal "mo:base/Principal";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Nat8 "mo:base/Nat8";
import Int "mo:base/Int";
import Blob "mo:base/Blob";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import { now } = "mo:base/Time";

// ═══════════════════════════════════════════════════════════════════════════
// MISBEHAVING MOCK ICRC-1/ICRC-2 LEDGER  (test-only)
//
// A genuine ICRC-1/2 ledger from the exchange's point of view (balance_of,
// fee, transfer, transfer_from, approve, allowance, get_transactions) whose
// icrc2_transfer_from can be DRIVEN into every response class on demand — the
// point being the one that has no test coverage anywhere else:
//
//   #debitThenTrap : actually move the funds AND commit the block, THEN trap on
//                    a post-commit await. This is the AMBIGUOUS case that the
//                    PanIndustrial icrc1-mo/icrc2-mo ledgers (sGLDT, EXE) can
//                    produce for real — the caller sees a reject, but the debit
//                    is already durable. The commit is real because the state
//                    mutation happens BEFORE `await async {}` (a real IC commit
//                    point); the subsequent Debug.trap only rolls back the
//                    empty continuation, leaving the debit committed.
//
// Deliberately NOT wired for enhanced-orthogonal-persistence — it is reinstalled
// on every test run. Control methods are open (no auth): this is a local test
// fixture, never deployed anywhere real.
// ═══════════════════════════════════════════════════════════════════════════

persistent actor class mock_ledger() = this {

  transient let { thash } = Map;

  // How icrc2_transfer_from behaves on the NEXT call. `remaining` lets a mode
  // fire a bounded number of times, then fall back to #normal (so a driver can
  // force exactly one misbehaving pull and have retries behave).
  type Mode = {
    #normal; // move funds, reply #Ok(block)
    #errAllowance; // reply #Err(#InsufficientAllowance ..) — funds NOT moved
    #errFunds; // reply #Err(#InsufficientFunds ..)     — funds NOT moved
    #debitThenTrap; // move funds + commit block, THEN trap on a post-commit await
    #trapBefore; // trap before touching any state — funds NOT moved
    #slow; // many yields, then #Ok — a parked-but-eventually-returning reply
  };

  stable var mode : Mode = #normal;
  stable var modeRemaining : Nat = 0; // 0 == unlimited; else decrements per fire
  stable var fee : Nat = 10_000;
  stable var symbol : Text = "MOCK";
  stable var name : Text = "Misbehaving Mock";
  stable var decimals : Nat = 8;

  stable let balances = Map.new<Text, Nat>();
  // key = fromText # "|" # spenderText
  stable let allowances = Map.new<Text, { allowance : Nat; expiresAt : ?Nat64 }>();
  stable let blocks = Vector.new<ICRC2.Transaction>();

  // Set before the post-commit await in #debitThenTrap so the block index is
  // observable even though the call rejects (committed at the await).
  stable var lastCommittedTransfer : ?Nat = null;

  // ── control surface ────────────────────────────────────────────────────────

  public func setMode(m : Mode, fireCount : Nat) : async () {
    mode := m;
    modeRemaining := fireCount;
    lastCommittedTransfer := null;
  };
  public query func getMode() : async (Text, Nat) {
    let t = switch (mode) {
      case (#normal) "normal";
      case (#errAllowance) "errAllowance";
      case (#errFunds) "errFunds";
      case (#debitThenTrap) "debitThenTrap";
      case (#trapBefore) "trapBefore";
      case (#slow) "slow";
    };
    (t, modeRemaining);
  };

  public func mint(to : Principal, amount : Nat) : async Nat {
    creditP(to, amount);
    appendBlock({
      burn = null;
      kind = "mint";
      mint = ?{ to = acct(to); memo = null; created_at_time = null; amount };
      timestamp = nowNat64();
      index = null;
      transfer = null;
    });
  };

  // Fabricate an arbitrary transfer block WITHOUT moving funds — used by the
  // negative tests of adminResolvePendingPull (wrong from/to, wrong amount,
  // >21-day age). ageNanos is subtracted from `now` for the block timestamp.
  public func adminAppendTransfer(from : Principal, to : Principal, amount : Nat, ageNanos : Nat) : async Nat {
    let ts : Nat64 = Nat64.fromNat(Int.abs(now() - ageNanos));
    appendBlock({
      burn = null;
      kind = "transfer";
      mint = null;
      timestamp = ts;
      index = null;
      transfer = ?{
        to = acct(to);
        fee = ?fee;
        from = acct(from);
        memo = null;
        created_at_time = null;
        amount;
      };
    });
  };

  public query func blockCount() : async Nat { Vector.size(blocks) };

  // Index of the most recent transfer block matching from→to (any amount), or
  // null. Robust to intervening mint/approve blocks — the driver uses it to
  // learn the committed block after an ambiguous (rejecting) pull.
  public query func lastTransferIndexFor(from : Principal, to : Principal) : async ?Nat {
    let n = Vector.size(blocks);
    if (n == 0) { return null };
    var i = n;
    while (i > 0) {
      i -= 1;
      let b = Vector.get(blocks, i);
      switch (b.transfer) {
        case (?t) { if (t.from.owner == from and t.to.owner == to) { return ?i } };
        case null {};
      };
    };
    null;
  };

  // ── ICRC-1 surface ─────────────────────────────────────────────────────────

  public query func icrc1_fee() : async Nat { fee };
  public query func icrc1_decimals() : async Nat8 { Nat8.fromNat(decimals) };
  public query func icrc1_name() : async Text { name };
  public query func icrc1_symbol() : async Text { symbol };
  public query func icrc1_total_supply() : async Nat {
    var s = 0;
    for (v in Map.vals(balances)) { s += v };
    s;
  };
  public query func icrc1_minting_account() : async ?ICRC2.Account { null };
  public query func icrc1_supported_standards() : async [{ url : Text; name : Text }] {
    [
      { name = "ICRC-1"; url = "https://github.com/dfinity/ICRC-1" },
      { name = "ICRC-2"; url = "https://github.com/dfinity/ICRC-1" },
    ];
  };
  public query func icrc1_metadata() : async [(Text, ICRC2.Value)] {
    [
      ("icrc1:fee", #Nat(fee)),
      ("icrc1:decimals", #Nat(decimals)),
      ("icrc1:name", #Text(name)),
      ("icrc1:symbol", #Text(symbol)),
    ];
  };
  public query func icrc1_balance_of(a : ICRC2.Account) : async Nat { balanceOf(a.owner) };

  // Lenient on the provided `fee` (always charges the ledger's own fee) so a
  // treasury payout with an unknown/zero cached fee still lands — the mock is a
  // fixture for the exchange's recovery logic, not a fee-negotiation test.
  public shared ({ caller }) func icrc1_transfer(args : ICRC2.TransferArg) : async ICRC2.Result {
    // sender is the caller (from_subaccount ignored — flat owner model)
    let sender = caller;
    if (balanceOf(sender) < args.amount + fee) {
      return #Err(#InsufficientFunds({ balance = balanceOf(sender) }));
    };
    debitP(sender, args.amount + fee);
    creditP(args.to.owner, args.amount);
    let idx = appendBlock({
      burn = null;
      kind = "transfer";
      mint = null;
      timestamp = nowNat64();
      index = null;
      transfer = ?{
        to = args.to;
        fee = ?fee;
        from = acct(sender);
        memo = args.memo;
        created_at_time = args.created_at_time;
        amount = args.amount;
      };
    });
    #Ok(idx);
  };

  // ── ICRC-2 surface ─────────────────────────────────────────────────────────

  public shared ({ caller }) func icrc2_approve(args : ICRC2.ApproveArgs) : async {
    #Ok : Nat;
    #Err : ICRC2.ApproveError;
  } {
    // caller is the token holder; spender is args.spender.owner
    let key = akey(caller, args.spender.owner);
    Map.set(allowances, thash, key, { allowance = args.amount; expiresAt = args.expires_at });
    // no block appended for approvals (keeps transfer indices clean)
    #Ok(Vector.size(blocks));
  };

  public query func icrc2_allowance(args : ICRC2.AllowanceArgs) : async ICRC2.Allowance {
    switch (Map.get(allowances, thash, akey(args.account.owner, args.spender.owner))) {
      case (?a) {
        if (expired(a.expiresAt)) { { allowance = 0; expires_at = a.expiresAt } } else {
          { allowance = a.allowance; expires_at = a.expiresAt };
        };
      };
      case null { { allowance = 0; expires_at = null } };
    };
  };

  // THE method under test. `caller` is the spender (the exchange).
  public shared ({ caller }) func icrc2_transfer_from(args : ICRC2.TransferFromArgs) : async {
    #Ok : Nat;
    #Err : ICRC2.TransferFromError;
  } {
    let active : Mode = mode;
    // decrement / expire the mode
    if (modeRemaining > 0) {
      modeRemaining -= 1;
      if (modeRemaining == 0) { mode := #normal };
    };

    switch (active) {
      case (#trapBefore) {
        Debug.trap("MOCK trapBefore: trapped before any state change (funds NOT moved)");
      };
      case (#errAllowance) {
        #Err(#InsufficientAllowance({ allowance = allowanceOf(args.from.owner, caller) }));
      };
      case (#errFunds) {
        #Err(#InsufficientFunds({ balance = balanceOf(args.from.owner) }));
      };
      case (#normal) {
        applyPull(caller, args);
      };
      case (#slow) {
        var i = 0;
        while (i < 25) { await async {}; i += 1 };
        applyPull(caller, args);
      };
      case (#debitThenTrap) {
        // 1. validate + mutate SYNCHRONOUSLY (no await yet)
        switch (checkAndApply(caller, args)) {
          case (#err(e)) {
            // validation failed → clean decline, nothing moved. (Not the
            // scenario under test; the driver funds/approves enough to pass.)
            #Err(e);
          };
          case (#ok(idx)) {
            lastCommittedTransfer := ?idx;
            // 2. COMMIT POINT: the debit/credit/block above become durable here.
            await async {};
            // 3. trap AFTER the commit — caller sees a reject, funds already moved.
            Debug.trap(
              "MOCK debitThenTrap: committed transfer block " # Nat.toText(idx)
              # " (debit " # Principal.toText(args.from.owner) # " -> treasury), THEN trapped on post-commit await"
            );
          };
        };
      };
    };
  };

  // ── internal helpers ─────────────────────────────────────────────────────

  // Validate allowance + balance, then move funds and append a transfer block.
  // Returns the block index. Shared by #normal / #slow / #debitThenTrap.
  func checkAndApply(spender : Principal, args : ICRC2.TransferFromArgs) : {
    #ok : Nat;
    #err : ICRC2.TransferFromError;
  } {
    let required = args.amount + fee;
    let allow = allowanceOf(args.from.owner, spender);
    if (allow < required) { return #err(#InsufficientAllowance({ allowance = allow })) };
    if (balanceOf(args.from.owner) < required) {
      return #err(#InsufficientFunds({ balance = balanceOf(args.from.owner) }));
    };
    debitP(args.from.owner, required);
    creditP(args.to.owner, args.amount);
    // reduce the allowance by amount+fee (standard ICRC-2 semantics)
    let key = akey(args.from.owner, spender);
    switch (Map.get(allowances, thash, key)) {
      case (?a) { Map.set(allowances, thash, key, { allowance = a.allowance - required; expiresAt = a.expiresAt }) };
      case null {};
    };
    let idx = appendBlock({
      burn = null;
      kind = "transfer";
      mint = null;
      timestamp = nowNat64();
      index = null;
      transfer = ?{
        to = args.to;
        fee = ?fee;
        from = args.from;
        memo = args.memo;
        created_at_time = args.created_at_time;
        amount = args.amount;
      };
    });
    #ok(idx);
  };

  func applyPull(spender : Principal, args : ICRC2.TransferFromArgs) : {
    #Ok : Nat;
    #Err : ICRC2.TransferFromError;
  } {
    switch (checkAndApply(spender, args)) {
      case (#ok(i)) { #Ok(i) };
      case (#err(e)) { #Err(e) };
    };
  };

  func acct(p : Principal) : ICRC2.Account { { owner = p; subaccount = null } };
  func balanceOf(p : Principal) : Nat { switch (Map.get(balances, thash, Principal.toText(p))) { case (?v) v; case null 0 } };
  func creditP(p : Principal, amt : Nat) { Map.set(balances, thash, Principal.toText(p), balanceOf(p) + amt) };
  func debitP(p : Principal, amt : Nat) {
    let b = balanceOf(p);
    Map.set(balances, thash, Principal.toText(p), if (b >= amt) { b - amt } else { 0 });
  };
  func akey(from : Principal, spender : Principal) : Text { Principal.toText(from) # "|" # Principal.toText(spender) };
  func allowanceOf(from : Principal, spender : Principal) : Nat {
    switch (Map.get(allowances, thash, akey(from, spender))) {
      case (?a) { if (expired(a.expiresAt)) 0 else a.allowance };
      case null 0;
    };
  };
  func expired(e : ?Nat64) : Bool {
    switch (e) { case (?t) { Nat64.toNat(t) < Int.abs(now()) }; case null false };
  };
  func nowNat64() : Nat64 { Nat64.fromNat(Int.abs(now())) };

  // Append a block, stamping its global index into the record so filterByIndex
  // in the exchange's getBlockData matches it exactly. Returns the index.
  func appendBlock(b : ICRC2.Transaction) : Nat {
    let idx = Vector.size(blocks);
    Vector.add(blocks, { b with index = ?idx });
    idx;
  };

  // ICRC-3-ish block query the exchange's getBlockData(#ICRC12) path calls.
  // Returns the exact window with real first_index / log_length so the
  // exchange's `block >= first_index and block < first_index+log_length`
  // gate and filterByIndex both pass.
  public query func get_transactions(req : ICRC2.GetTransactionsRequest) : async ICRC2.GetTransactionsResponse {
    let n = Vector.size(blocks);
    let out = Vector.new<ICRC2.Transaction>();
    var i = req.start;
    let stop = Nat.min(req.start + req.length, n);
    while (i < stop) { Vector.add(out, Vector.get(blocks, i)); i += 1 };
    {
      first_index = 0;
      log_length = n;
      transactions = Vector.toArray(out);
      archived_transactions = [];
    };
  };
};
