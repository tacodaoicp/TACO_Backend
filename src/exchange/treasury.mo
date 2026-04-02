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

  stable let transferQueue = Vector.new<(TransferRecipient, Nat, Text)>();
  transient let ICPprincipalText = "ryjl3-tyaaa-aaaaa-aaaba-cai";
  stable var acceptedTokens : [Text] = ["ryjl3-tyaaa-aaaaa-aaaba-cai", "kknbx-zyaaa-aaaaq-aae4a-cai"];
  stable let tokenInfo = Map.new<Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text }>();
  stable var canisterOTC = "5kuny-yiaaa-aaaal-acgta-cai"; // Set via setOTCCanister after deploy
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

  type TokenInfo = {
    address : Text;
    name : Text;
    symbol : Text;
    decimals : Nat;
    transfer_fee : Nat;
  };

  //This function is called by the exchange each time it has new transfers that have to be done. When not testing, it does not fulfill these transfers directly but instead creates a timer.
  public shared ({ caller }) func receiveTransferTasks(tempTransferQueue : [(TransferRecipient, Nat, Text)]) : async Bool {
    assert (caller == canisterOTCPrincipal);

    if (tempTransferQueue.size() != 0) {
      try {
        Vector.addFromIter(transferQueue, tempTransferQueue.vals());
        if test {
          if (not transferTimerRunning) {
            transferTimerRunning := true;
            // Loop until queue empty — picks up items added by concurrent callers
            var rounds = 0;
            while (not Vector.isEmpty(transferQueue) and rounds < 10) {
              try { await transferTimer(true) } catch (_) {};
              rounds += 1;
            };
            transferTimerRunning := false;
          };
          // If lock was held: items are in transferQueue, the running loop will pick them up
        } else {
          Vector.add(
            transferTimerIDs,
            setTimer<system>(
              #nanoseconds(100000000),
              func() : async () {
                try { await transferTimer(false) } catch (_) {
                  retryFunc<system>(20, 5, 1);
                };
              },
            ),
          );
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
    let transferBatch = Vector.new<(TransferRecipient, Nat, Text)>();
    let transferTasksICP = Vector.new<(async TransferResultICP, (TransferRecipient, Nat, Text))>();
    let transferTasksICRC1 = Vector.new<(async TransferResultICRC1, (TransferRecipient, Nat, Text))>();

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
        transferNonce += 1;
        nsAdd += 1;
        //Debug.print("Sending " #debug_show (data.1) # " " #debug_show (data.2) # " to " #debug_show (data.0));
        if (data.2 != ICPprincipalText) {
          // Transfer ICRC1 token
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
          Vector.add(transferTasksICRC1, (transferTask, (data.0, data.1, data.2)));
        } else {
          // Transfer ICP
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
          Vector.add(transferTasksICP, (transferTask, (data.0, data.1, data.2)));
        };
      };

      // Process ICRC1 transfer results
      for (transferTask in Vector.vals(transferTasksICRC1)) {
        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(_)) {
              // Transfer successful
            };
            case (#Err(transferError)) {
              // Transfer failed, re-queue
              Vector.add(transferQueue, transferTask.1);
            };

          };
        } catch (err) {
          // Transfer failed, re-queue
          Vector.add(transferQueue, transferTask.1);
        };
      };

      // Process ICP transfer results
      for (transferTask in Vector.vals(transferTasksICP)) {
        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(_)) {
              // Transfer successful
            };
            case (#Err(transferError)) {
              // Transfer failed, re-queue
              Vector.add(transferQueue, transferTask.1);
            };
          };
        } catch (err) {
          // Transfer failed, re-queue
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
              #nanoseconds(100000000),
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
              try {
                await updateTokenInfoTimer();
              } catch (_) {
                retryFunc<system>(20, 5, 2);
              };
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
        #seconds(500),
        func() : async () {
          try {
            await updateTokenInfoTimer();
          } catch (_) {
            retryFunc<system>(20, 5, 2);
          };
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
    try {
      await updateTokenInfo();
    } catch (_) {
      retryFunc<system>(20, 5, 2);
    };
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

  //Getting the metadata of each token
  private func updateTokenInfo() : async () {
    let timersize = Vector.size(tokenInfoTimerIDs);
    if (timersize > 0) {
      for (i in Iter.range(0, timersize - 1)) {
        try { cancelTimer(Vector.get(tokenInfoTimerIDs, i)) } catch (_) {};
      };
    };
    Vector.clear(tokenInfoTimerIDs);

    // Create a map to store the futures
    let metadataFutures = Map.new<Text, async [(Text, { #Nat : Nat; #Int : Int; #Blob : Blob; #Text : Text })]>();

    // Initialize all async calls and store futures
    for (token in acceptedTokens.vals()) {
      try {
        let ledger = actor (token) : ICRC1.FullInterface;
        let future = ledger.icrc1_metadata();
        Map.set(metadataFutures, thash, token, future);
      } catch (_) {
        Debug.print("Error initializing future for token: " # token);
      };
    };

    // Process all futures
    for (token in acceptedTokens.vals()) {
      try {
        let futureMaybe = Map.get(metadataFutures, thash, token);
        switch (futureMaybe) {
          case (?future) {
            let get = await future;
            var fee2 = 0;
            var decimals2 = 0;
            var name2 = "";
            var symbol2 = "";

            for (i in Array.vals(get)) {
              switch (i.0) {
                case "icrc1:fee" {
                  switch (i.1) { case (#Nat(ok)) { fee2 := ok }; case (#Int(ok)) { fee2 := Int.abs(ok) }; case _ {} };
                };
                case "icrc1:name" {
                  switch (i.1) { case (#Text(ok)) { name2 := ok }; case _ {} };
                };
                case "icrc1:symbol" {
                  switch (i.1) { case (#Text(ok)) { symbol2 := ok }; case _ {} };
                };
                case "icrc1:decimals" {
                  switch (i.1) { case (#Nat(ok)) { decimals2 := ok }; case (#Int(ok)) { decimals2 := Int.abs(ok) }; case _ {} };
                };
                case _ {};
              };
            };

            let data2 = {
              TransferFee = fee2;
              Decimals = decimals2;
              Name = name2;
              Symbol = symbol2;
            };
            Map.set(tokenInfo, thash, token, data2);
          };
          case (null) {
            Debug.print("No future found for token: " # token);
          };
        };
      } catch (_) {
        Debug.print("Error processing metadata for token: " # token);
      };
    };
  };

  if (Vector.size(tokenInfoTimerIDs) == 0) {
    Vector.add(
      tokenInfoTimerIDs,
      setTimer<system>(
        #seconds(1),
        func() : async () {
          try {
            await updateTokenInfoTimer();
          } catch (_) {
            retryFunc<system>(20, 5, 2);
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
      #getPendingTransferCount : () -> ();
      #getTokenInfo : () -> ();
      #receiveTransferTasks :
        () -> (tempTransferQueue : [(TransferRecipient, Nat, Text)]);
      #setOTCCanister : () -> (id : Text);
      #setTest : () -> (a : Bool);
    };
  }) : Bool {
    (caller == deployer.caller or caller == canisterOTCPrincipal or canisterOTC == "aaaaa-aa");
  };

};
