import Text "mo:base/Text";
import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import Map "mo:map/Map";
import Prim "mo:prim";
import Int "mo:base/Int";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import ICRC1 "mo:icrc1/ICRC1";
import Ledger "src/Ledger";
import Debug "mo:base/Debug"; //
import Vector "mo:vector";
import { now } = "mo:base/Time";
import { setTimer; cancelTimer } = "mo:base/Timer";
import Error "mo:base/Error";

import TreasuryMigration "./treasury_migration";

(with migration = TreasuryMigration.migrate)
shared (deployer) persistent actor class treasury() = this {
  transient let { thash } = Map;
  transient let {
    natToNat64;
  } = Prim;

  type TransferResultICP = {
    #Ok : Nat64;
    #Err : {
      #BadFee : {
        expected_fee : {
          e8s : Nat64;
        };
      };
      #InsufficientFunds : {
        balance : {
          e8s : Nat64;
        };
      };
      #TxTooOld : { allowed_window_nanos : Nat64 };
      #TxCreatedInFuture;
      #TxDuplicate : { duplicate_of : Nat64 };
    };
  };
  public type Subaccount = Blob;

  type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Subaccount };
  };

  type TransferResultICRC1 = {
    #Ok : Nat;
    #Err : {
      #BadFee : { expected_fee : Nat };
      #BadBurn : { min_burn_amount : Nat };
      #InsufficientFunds : { balance : Nat };
      #Duplicate : { duplicate_of : Nat };
      #TemporarilyUnavailable;
      #GenericError : { error_code : Nat; message : Text };
      #TooOld;
      #CreatedInFuture : { ledger_time : Nat64 };
    };
  };

  stable let transferQueue = Vector.new<(TransferRecipient, Nat, Text, Text)>();
  transient let ICPprincipalText = "ryjl3-tyaaa-aaaaa-aaaba-cai";
  stable var acceptedTokens : [Text] = ["ryjl3-tyaaa-aaaaa-aaaba-cai", "kknbx-zyaaa-aaaaq-aae4a-cai"];
  stable let tokenInfo = Map.new<Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text }>();
  stable var canisterOTC = "qioex-5iaaa-aaaan-q52ba-cai"; // Set via setOTCCanister after deploy
  stable var canisterOTCPrincipal = Principal.fromText(canisterOTC);

  public shared (msg) func setOTCCanister(id : Text) : async () {
    assert (msg.caller == deployer.caller or msg.caller == canisterOTCPrincipal or canisterOTC == "aaaaa-aa");
    canisterOTC := id;
    canisterOTCPrincipal := Principal.fromText(id);
  };

  stable let transferTimerIDs = Vector.new<Nat>();
  stable let tokenInfoTimerIDs = Vector.new<Nat>();

  stable var test = false;

  stable var nsAdd : Nat64 = 0;
  stable var transferNonce : Nat64 = 0;
  stable var lastNonceReset : Int = 0;
  transient var transferTimerRunning = false;

  // Per-transaction idempotency: stores txId → timestamp of successful transfer
  stable let processedTxIds = Map.new<Text, Int>();

  // 24h rolling inter-canister call telemetry. Key = "<method>:<token>".
  // Each entry is a 24-slot ring buffer indexed by absolute hour; a slot whose
  // `hour` no longer matches the current absolute hour is reset on next write.
  type StatSlot = { var ok : Nat; var err : Nat; var hour : Int };
  stable let callStats24h = Map.new<Text, [var StatSlot]>();

  private func recordCall(method : Text, token : Text, success : Bool) {
    let hour = now() / 3_600_000_000_000;
    let key = method # ":" # token;
    let entry = switch (Map.get(callStats24h, thash, key)) {
      case (?e) { e };
      case null {
        let e = Array.tabulateVar<StatSlot>(24, func(_) = { var ok = 0; var err = 0; var hour : Int = -1 });
        Map.set(callStats24h, thash, key, e);
        e;
      };
    };
    let slot = entry[Int.abs(hour) % 24];
    if (slot.hour != hour) { slot.ok := 0; slot.err := 0; slot.hour := hour };
    if (success) { slot.ok += 1 } else { slot.err += 1 };
  };

  private func cleanupTxIds<system>() {
    let cutoff = now() - 30 * 86_400_000_000_000;
    let toDelete = Vector.new<Text>();
    for ((k, v) in Map.entries(processedTxIds)) {
      if (v < cutoff) { Vector.add(toDelete, k) };
    };
    for (k in Vector.vals(toDelete)) { Map.delete(processedTxIds, thash, k) };
    ignore setTimer<system>(#seconds(86400), func() : async () { cleanupTxIds<system>() });
  };
  ignore setTimer<system>(#seconds(86400), func() : async () { cleanupTxIds<system>() });

  type TokenInfo = {
    address : Text;
    name : Text;
    symbol : Text;
    decimals : Nat;
    transfer_fee : Nat;
  };

  //This function is called by the exchange each time it has new transfers that have to be done.
  // `immediate=true` (passed by the exchange when the original caller is in
  // allowedCanisters) drains the transfer queue synchronously — `swapMultiHop`
  // returns AFTER the icrc1_transfer to user main commits, eliminating the
  // queue-vs-consensus race that was causing arb bots' next leg to see
  // InsufficientFunds. `immediate=false` (default for regular users / system
  // tasks) preserves the original setTimer-queued behavior.
  public shared ({ caller }) func receiveTransferTasks(tempTransferQueue : [(TransferRecipient, Nat, Text, Text)], immediate : Bool) : async Bool {
    assert (caller == canisterOTCPrincipal);

    if (tempTransferQueue.size() != 0) {
      try {
        Vector.addFromIter(transferQueue, tempTransferQueue.vals());
        if (test or immediate) {
          // CORRECTNESS: every immediate caller MUST await until the queue is
          // empty. The previous `if (not transferTimerRunning)` guard was a
          // BUG — when two swap_multi_hop calls overlapped, the second
          // returned `true` without waiting for its own items to drain
          // (relying on the running loop). The caller (swap_multi_hop) then
          // returned to the bot, which fired the next leg before its TACO
          // payout had committed → ICPSwap pool saw `balance=10_001` and
          // rejected. Instead, every immediate caller now runs its own drain
          // loop. transferTimer pops items atomically between awaits, so
          // concurrent loops cooperate (each grabs distinct items via
          // Vector.removeLast). transferTimerRunning is kept as a hint but
          // not enforced for correctness in immediate mode.
          var rounds = 0;
          while (not Vector.isEmpty(transferQueue) and rounds < 10) {
            try { await transferTimer(true) } catch (_) {};
            rounds += 1;
          };
        } else {
          // De-duplicate: if a transfer timer is already pending (or one is
          // mid-flight with its ID still in the vector), skip scheduling.
          // The new items are already in transferQueue and will be drained
          // when that timer fires (transferTimer's end-block reschedules at
          // 5 s if the queue is still non-empty after each batch).
          if (Vector.size(transferTimerIDs) == 0) {
            Vector.add(
              transferTimerIDs,
              setTimer<system>(
                #seconds(5),
                func() : async () {
                  try { await transferTimer(false) } catch (_) {
                    retryFunc<system>(20, 5, 1);
                  };
                },
              ),
            );
          };
        };
        true;
      } catch (_) { false };
    } else {
      true;
    };
  };

  // Function to handle all the transfers. removeLast is a perfect function for this as it removes the last item in the vector while also retuurning that item. If a transfer fails, it gets added to transferQueueTemp, so it can later be readded and retried.
  // Since weve seen token canisters in the past get overflooded with transactions, making each transfer take a long time, ive decided to not await each Transfer, instead adding the future to a Vector, so multiple transfers are sent at once.
  private func transferTimer(all : Bool) : async () {
    let transferBatch = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    let transferTasksICP = Vector.new<(async TransferResultICP, (TransferRecipient, Nat, Text, Text))>();
    let transferTasksICRC1 = Vector.new<(async TransferResultICRC1, (TransferRecipient, Nat, Text, Text))>();

    // Remove the first X entries from transferQueue and add them to transferBatch
    let batchSize = if all { Vector.size(transferQueue) } else { 100 };
    if (batchSize > 0) {
      label a for (i in Iter.range(0, batchSize - 1)) {
        switch (Vector.removeLast(transferQueue)) {
          case (?transfer) {
            Vector.add(transferBatch, transfer);
          };
          case (null) {
            break a;
          };
        };
      };

      // Process transfers in transferBatch
      // Reset nonce every 10 seconds to keep offset small
      if (Int.abs(now() - lastNonceReset) > 10_000_000_000) {
        transferNonce := 0;
        lastNonceReset := now();
      };
      for (data in Vector.vals(transferBatch)) {
        // Idempotency: skip transfers already successfully processed
        let txId = data.3;
        if (txId != "" and Map.has(processedTxIds, thash, txId)) {
          // Already processed — skip
        } else {
        transferNonce += 1;
        nsAdd += 1;
        if (data.2 != ICPprincipalText) {
          // Transfer ICRC1 token. Dispatch wrapped in try/catch so that a
          // synchronous throw (e.g. outbound message queue full) re-enqueues
          // the un-dispatched item rather than aborting the whole batch and
          // losing already-popped items.
          try {
            let token = actor (data.2) : ICRC1.FullInterface;
            let Tfees2 = Map.get(tokenInfo, thash, data.2);
            var Tfees = 0;
            switch (Tfees2) {
              case null {};
              case (?(foundTrades)) {
                Tfees := foundTrades.TransferFee;
              };
            };
            let recipient = switch (data.0) {
              case (#principal(p)) { p };
              case (#accountId({ owner })) { owner };
            };

            let subaccount = switch (data.0) {
              case (#principal(_)) { null };
              case (#accountId({ subaccount })) { subaccount };
            };

            let transferTask = token.icrc1_transfer({
              from_subaccount = null;
              to = { owner = recipient; subaccount = subaccount };
              amount = (data.1);
              fee = ?Tfees;
              memo = ?Blob.fromArray([1]);
              created_at_time = ?(natToNat64(Int.abs(now())) - transferNonce);
            });
            Vector.add(transferTasksICRC1, (transferTask, data));
          } catch (_) {
            recordCall("icrc1_transfer", data.2, false);
            Vector.add(transferQueue, data);
          };
        } else {
          // Transfer ICP. Same dispatch-throw protection as ICRC1 path.
          try {
            let ledger = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : Ledger.Interface;
            let Tfees2 = Map.get(tokenInfo, thash, "ryjl3-tyaaa-aaaaa-aaaba-cai");
            var Tfees = 0;
            switch (Tfees2) {
              case null {};
              case (?(foundTrades)) {
                Tfees := foundTrades.TransferFee;
              };
            };
            let transferTask = ledger.transfer({
              memo : Nat64 = 0;
              from_subaccount = null;
              to = switch (data.0) {
                case (#principal(p)) Principal.toLedgerAccount(p, null);
                case (#accountId({ owner; subaccount })) Principal.toLedgerAccount(owner, subaccount);
              };
              amount = { e8s = natToNat64(data.1) };
              fee = { e8s = natToNat64(Tfees) };
              created_at_time = ?{
                timestamp_nanos = natToNat64(Int.abs(now())) - transferNonce;
              };
            });
            Vector.add(transferTasksICP, (transferTask, data));
          } catch (_) {
            recordCall("icp_transfer", ICPprincipalText, false);
            Vector.add(transferQueue, data);
          };
        };
        }; // end idempotency else
      };

      // Process ICRC1 transfer results
      for (transferTask in Vector.vals(transferTasksICRC1)) {
        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(_)) {
              let tid = transferTask.1.3;
              if (tid != "") { Map.set(processedTxIds, thash, tid, now()) };
              recordCall("icrc1_transfer", transferTask.1.2, true);
            };
            case (#Err(transferError)) {
              recordCall("icrc1_transfer", transferTask.1.2, false);
              Vector.add(transferQueue, transferTask.1);
            };
          };
        } catch (err) {
          recordCall("icrc1_transfer", transferTask.1.2, false);
          Vector.add(transferQueue, transferTask.1);
        };
      };

      // Process ICP transfer results
      for (transferTask in Vector.vals(transferTasksICP)) {
        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(_)) {
              let tid = transferTask.1.3;
              if (tid != "") { Map.set(processedTxIds, thash, tid, now()) };
              recordCall("icp_transfer", ICPprincipalText, true);
            };
            case (#Err(transferError)) {
              recordCall("icp_transfer", ICPprincipalText, false);
              Vector.add(transferQueue, transferTask.1);
            };
          };
        } catch (err) {
          recordCall("icp_transfer", ICPprincipalText, false);
          Vector.add(transferQueue, transferTask.1);
        };
      };

      if (not Vector.isEmpty(transferQueue)) {
        try {
          let timersize = Vector.size(transferTimerIDs);
          if (timersize > 0) {
            for (i in Iter.range(0, timersize - 1)) {
              try {
                cancelTimer(Vector.get(transferTimerIDs, i));
              } catch (_) {};
            };
          };
          Vector.clear(transferTimerIDs);
          Vector.add(
            transferTimerIDs,
            setTimer<system>(
              #seconds(5),
              func() : async () {
                try {
                  await transferTimer(false);
                } catch (_) {
                  retryFunc<system>(20, 5, 1);
                };
              },
            ),
          );

        } catch (_) {
          Debug.print("Error at 110");
        };
      } else {
        Vector.clear(transferTimerIDs);
      };
      if (Vector.size(tokenInfoTimerIDs) == 0) {
        Vector.add(
          tokenInfoTimerIDs,
          setTimer<system>(
            #seconds(1),
            func() : async () {
              try { await updateTokenInfoTimer() } catch (_) {};
            },
          ),
        );
      };
    };
  };

  // Drain the transfer queue completely — keeps calling transferTimer until nothing remains.
  // Used by checkDiffs to ensure all pending transfers are settled before querying balances.
  public shared ({ caller }) func drainTransferQueue() : async () {
    assert (caller == canisterOTCPrincipal);
    // Wait for any running transferTimer to finish before draining
    var waitRounds = 0;
    while (transferTimerRunning and waitRounds < 50) {
      await async {};  // yield to let running transferTimer progress
      waitRounds += 1;
    };
    // Now drain
    if (not transferTimerRunning) {
      transferTimerRunning := true;
      var retries = 0;
      while (not Vector.isEmpty(transferQueue) and retries < 10) {
        try { await transferTimer(true) } catch (_) {};
        retries += 1;
      };
      transferTimerRunning := false;
    };
  };

  public query ({ caller }) func getPendingTransferCount() : async Nat {
    Vector.size(transferQueue);
  };

  // Aggregated inter-canister call counts over the rolling last 24 hours.
  // Returned as (key, ok, err) where key = "<method>:<token>".
  public query func getCallStats24h() : async [(Text, Nat, Nat)] {
    let now_hour = now() / 3_600_000_000_000;
    let out = Vector.new<(Text, Nat, Nat)>();
    for ((key, entry) in Map.entries(callStats24h)) {
      var ok = 0;
      var err = 0;
      for (slot in entry.vals()) {
        if (slot.hour > now_hour - 24) {
          ok += slot.ok;
          err += slot.err;
        };
      };
      if (ok > 0 or err > 0) { Vector.add(out, (key, ok, err)) };
    };
    Vector.toArray(out);
  };

  // Sum of pending outgoing-transfer amounts grouped by token. Used by checkDiffs
  // on the exchange to discount tokens that are queued to leave the treasury but
  // haven't been transmitted to the ledger yet, so positive-drift detection isn't
  // confused by transient queue depth (e.g. a removeConcentratedLiquidity payout
  // sitting between debit-from-internal-state and ledger-side execution).
  public query ({ caller }) func getPendingTransfersByToken() : async [(Text, Nat)] {
    let sums = Map.new<Text, Nat>();
    for (transfer in Vector.vals(transferQueue)) {
      let token = transfer.2;
      let amount = transfer.1;
      let cur = switch (Map.get(sums, thash, token)) { case (?n) { n }; case null { 0 } };
      Map.set(sums, thash, token, cur + amount);
    };
    let out = Vector.new<(Text, Nat)>();
    for ((token, amount) in Map.entries(sums)) {
      Vector.add(out, (token, amount));
    };
    Vector.toArray(out);
  };

  //This function is set to update each tokens decimals, transferfee and name. This is also important as the exchange calls the function named getTokenInfo to get the same data.
  private func updateTokenInfoTimer() : async () {
    let timersize = Vector.size(tokenInfoTimerIDs);
    if (timersize > 0) {
      for (i in Iter.range(0, timersize -1)) {
        try { cancelTimer(Vector.get(tokenInfoTimerIDs, i)) } catch (_) {};
      };
    };
    nsAdd := 0;
    try {
      await updateTokenInfo();
    } catch (_) {};

    let timersize2 = Vector.size(tokenInfoTimerIDs);
    if (timersize2 > 0) {
      for (i in Iter.range(0, timersize2 -1)) {
        try { cancelTimer(Vector.get(tokenInfoTimerIDs, i)) } catch (_) {};
      };
    };
    Vector.clear(tokenInfoTimerIDs);
    Vector.add(
      tokenInfoTimerIDs,
      setTimer<system>(
        #seconds(3000),
        func() : async () {
          try { await updateTokenInfoTimer() } catch (_) {};
        },
      ),
    );

  };

  public shared ({ caller }) func setTest(a : Bool) : async () {
    assert (caller == canisterOTCPrincipal);
    test := a;
  };

  public shared ({ caller }) func getAcceptedtokens(a : [Text]) : async () {
    assert (caller == canisterOTCPrincipal);
    acceptedTokens := a;
    try { await updateTokenInfo() } catch (_) {};
  };
  public query ({ caller }) func getTokenInfo() : async [(Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text })] {
    assert (caller == canisterOTCPrincipal);
    Map.toArray<Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text }>(tokenInfo);
  };

  // When 1 of the timer function fails, this function is called so it gets retried. Assuming that it fails due the task queue being full, its a <system> function, so it does not need to add another task to the queue.
  //1= transferTimer 2=tokenInfoTimer
  private func retryFunc<system>(
    maxRetries : Nat,
    initialDelay : Nat,
    timerType : Nat,
  ) : () {
    //if maxRetries== 0 all the attempts failed. The vectors are cleared so this can be seen periodically in other function to again retry.
    if (maxRetries == 0) {
      if (timerType == 1) {
        let timersize = Vector.size(transferTimerIDs);
        if (timersize > 0) {
          for (i in Iter.range(0, timersize -1)) {
            cancelTimer(Vector.get(transferTimerIDs, i));
          };
        };
        Vector.clear(transferTimerIDs);
      } else { Vector.clear(tokenInfoTimerIDs) };
      return;
    };
    if (timerType == 1) {
      let timersize = Vector.size(transferTimerIDs);
      if (timersize > 0) {
        for (i in Iter.range(0, timersize -1)) {
          cancelTimer(Vector.get(transferTimerIDs, i));
        };
      };
      Vector.clear(transferTimerIDs);
      Vector.add(
        transferTimerIDs,
        setTimer<system>(
          #seconds(initialDelay),
          func() : async () {
            try {
              if test { await transferTimer(true) } else {
                await transferTimer(false);
              };
            } catch (_) {
              retryFunc<system>(maxRetries -1, initialDelay, timerType);
            };
          },
        ),
      );
    } else {
      let timersize = Vector.size(tokenInfoTimerIDs);
      if (timersize > 0) {
        for (i in Iter.range(0, timersize -1)) {
          cancelTimer(Vector.get(tokenInfoTimerIDs, i));
        };
      };
      Vector.clear(tokenInfoTimerIDs);
      Vector.add(
        tokenInfoTimerIDs,
        setTimer<system>(
          #seconds(initialDelay),
          func() : async () {
            try { await updateTokenInfoTimer() } catch (_) {
              retryFunc<system>(maxRetries -1, initialDelay, timerType);
            };
          },
        ),
      );
    };

  };

  //Getting the metadata of each token. Refresh runs in serial batches of
  //BATCH_SIZE so we never have more than BATCH_SIZE outbound metadata calls
  //outstanding at once — that prevents the canister's outbound message queue
  //from filling, which is what previously caused synchronous dispatch throws
  //("Error initializing future") and a 5s retry storm.
  private func updateTokenInfo() : async () {
    let timersize = Vector.size(tokenInfoTimerIDs);
    if (timersize > 0) {
      for (i in Iter.range(0, timersize - 1)) {
        try { cancelTimer(Vector.get(tokenInfoTimerIDs, i)) } catch (_) {};
      };
    };
    Vector.clear(tokenInfoTimerIDs);

    let BATCH_SIZE = 3;
    let total = acceptedTokens.size();
    var idx = 0;
    while (idx < total) {
      let endIdx = if (idx + BATCH_SIZE > total) total else idx + BATCH_SIZE;

      // Synchronously dispatch up to BATCH_SIZE futures, each guarded so a
      // dispatch-time throw on one token doesn't abort the batch.
      let futures = Vector.new<(Text, async [(Text, { #Nat : Nat; #Int : Int; #Blob : Blob; #Text : Text })])>();
      var i = idx;
      while (i < endIdx) {
        let token = acceptedTokens[i];
        try {
          let ledger = actor (token) : ICRC1.FullInterface;
          Vector.add(futures, (token, ledger.icrc1_metadata()));
        } catch (_) {
          recordCall("icrc1_metadata", token, false);
          Debug.print("Error initializing future for token: " # token);
        };
        i += 1;
      };

      // Await this batch before starting the next one.
      for ((token, future) in Vector.vals(futures)) {
        try {
          let get = await future;
          var fee2 = 0;
          var decimals2 = 0;
          var name2 = "";
          var symbol2 = "";

          for (entry in Array.vals(get)) {
            switch (entry.0) {
              case "icrc1:fee" {
                switch (entry.1) { case (#Nat(ok)) { fee2 := ok }; case (#Int(ok)) { fee2 := Int.abs(ok) }; case _ {} };
              };
              case "icrc1:name" {
                switch (entry.1) { case (#Text(ok)) { name2 := ok }; case _ {} };
              };
              case "icrc1:symbol" {
                switch (entry.1) { case (#Text(ok)) { symbol2 := ok }; case _ {} };
              };
              case "icrc1:decimals" {
                switch (entry.1) { case (#Nat(ok)) { decimals2 := ok }; case (#Int(ok)) { decimals2 := Int.abs(ok) }; case _ {} };
              };
              case _ {};
            };
          };

          Map.set(tokenInfo, thash, token, {
            TransferFee = fee2;
            Decimals = decimals2;
            Name = name2;
            Symbol = symbol2;
          });
          recordCall("icrc1_metadata", token, true);
        } catch (_) {
          recordCall("icrc1_metadata", token, false);
          Debug.print("Error processing metadata for token: " # token);
        };
      };

      idx := endIdx;
    };
  };

  if (Vector.size(tokenInfoTimerIDs) == 0) {
    Vector.add(
      tokenInfoTimerIDs,
      setTimer<system>(
        #seconds(1),
        func() : async () {
          try { await updateTokenInfoTimer() } catch (_) {
            // Bootstrap path: if the very first invocation fails, schedule a
            // 3000 s recovery so the timer chain is guaranteed to keep going.
            ignore setTimer<system>(
              #seconds(3000),
              func() : async () {
                try { await updateTokenInfoTimer() } catch (_) {};
              },
            );
          };
        },
      ),
    );
  };

  system func inspect({
    arg : Blob;
    caller : Principal;
    msg : {
      #drainTransferQueue : () -> ();
      #getAcceptedtokens : () -> (a : [Text]);
      #getCallStats24h : () -> ();
      #getPendingTransferCount : () -> ();
      #getPendingTransfersByToken : () -> ();
      #getTokenInfo : () -> ();
      #receiveTransferTasks :
        () -> (tempTransferQueue : [(TransferRecipient, Nat, Text, Text)], immediate : Bool);
      #setOTCCanister : () -> (id : Text);
      #setTest : () -> (a : Bool);
    };
  }) : Bool {
    (caller == deployer.caller or caller == canisterOTCPrincipal or canisterOTC == "aaaaa-aa");
  };

};
