import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Error "mo:base/Error";
import Map "mo:map/Map";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import ICRC1 "mo:icrc1/ICRC1";
import Nat64 "mo:base/Nat64";
import Int "mo:base/Int";
import Time "mo:base/Time";
import Timer "mo:base/Timer";

shared (deployer) persistent actor class Faucet() = this {
  transient let { phash } = Map;

  transient let TOKEN1_CANISTER = "4xacp-pqaaa-aaaan-qm4oa-cai";
  transient let TOKEN2_CANISTER = "4zcph-uaaaa-aaaan-qm4pa-cai";
  transient let TOKEN3_CANISTER = "46djt-zyaaa-aaaan-qm4pq-cai";

  transient let FAUCET_AMOUNT : Nat = 100_000_000_000;

  transient let usedPrincipals1 = Map.new<Principal, Null>();
  transient let usedPrincipals2 = Map.new<Principal, Null>();
  transient let usedPrincipals3 = Map.new<Principal, Null>();
  transient let CLEAR_INTERVAL : Nat = 3 * 24 * 60 * 60 * 1_000_000_000; // 3 days in nanoseconds

  func isEligible1(p : Principal) : Bool {
    not Map.has(usedPrincipals1, phash, p);
  };

  func isEligible2(p : Principal) : Bool {
    not Map.has(usedPrincipals2, phash, p);
  };

  func isEligible3(p : Principal) : Bool {
    not Map.has(usedPrincipals3, phash, p);
  };

  func addUsedPrincipal1(p : Principal) {
    Map.set(usedPrincipals1, phash, p, null);
  };

  func addUsedPrincipal2(p : Principal) {
    Map.set(usedPrincipals2, phash, p, null);
  };

  func addUsedPrincipal3(p : Principal) {
    Map.set(usedPrincipals3, phash, p, null);
  };

  public shared (msg) func requestToken1(receiver : Text) : async {
    #Ok : Text;
    #Err : Text;
  } {
    let caller = msg.caller;
    if (Principal.toText(caller).size() < 30) {
      return #Err("Caller principal must be longer than 30 characters");
    };

    let receiverPrincipal = Principal.fromText(receiver);

    if (not isEligible1(caller) or not isEligible1(receiverPrincipal)) {
      return #Err("Caller or receiver has already used the faucet");
    };

    let token = actor (TOKEN1_CANISTER) : ICRC1.FullInterface;

    let result = await token.icrc1_transfer({
      from_subaccount = null;
      to = { owner = receiverPrincipal; subaccount = null };
      amount = (FAUCET_AMOUNT);
      fee = null;
      memo = ?Blob.fromArray([1]);
      created_at_time = ?Nat64.fromNat(Int.abs(Time.now()));
    });

    switch (result) {
      case (#Ok(e)) {
        addUsedPrincipal1(caller);
        addUsedPrincipal1(receiverPrincipal);
        return #Ok("Token1 transfer of " # debug_show (FAUCET_AMOUNT) # " completed to " # receiver # " result: " #debug_show (e));
      };
      case (#Err(e)) {
        return #Err("Transfer failed: " # debug_show (e));
      };
    };
  };

  public shared (msg) func requestToken2(receiver : Text) : async {
    #Ok : Text;
    #Err : Text;
  } {
    let caller = msg.caller;
    if (Principal.toText(caller).size() < 30) {
      return #Err("Caller principal must be longer than 30 characters");
    };

    let receiverPrincipal = Principal.fromText(receiver);

    if (not isEligible2(caller) or not isEligible2(receiverPrincipal)) {
      return #Err("Caller or receiver has already used the faucet");
    };

    let token = actor (TOKEN2_CANISTER) : ICRC1.FullInterface;

    let result = await token.icrc1_transfer({
      from_subaccount = null;
      to = { owner = receiverPrincipal; subaccount = null };
      amount = (FAUCET_AMOUNT);
      fee = null;
      memo = ?Blob.fromArray([1]);
      created_at_time = ?Nat64.fromNat(Int.abs(Time.now()));
    });

    switch (result) {
      case (#Ok(e)) {
        addUsedPrincipal2(caller);
        addUsedPrincipal2(receiverPrincipal);
        return #Ok("Token2 transfer of " # debug_show (FAUCET_AMOUNT) # " completed to " # receiver # " result: " #debug_show (e));
      };
      case (#Err(e)) {
        return #Err("Transfer failed: " # debug_show (e));
      };
    };
  };

  public shared (msg) func requestToken3(receiver : Text) : async {
    #Ok : Text;
    #Err : Text;
  } {
    let caller = msg.caller;
    if (Principal.toText(caller).size() < 30) {
      return #Err("Caller principal must be longer than 30 characters");
    };

    let receiverPrincipal = Principal.fromText(receiver);

    if (not isEligible3(caller) or not isEligible3(receiverPrincipal)) {
      return #Err("Caller or receiver has already used the faucet");
    };

    let token = actor (TOKEN3_CANISTER) : ICRC1.FullInterface;

    let result = await token.icrc1_transfer({
      from_subaccount = null;
      to = { owner = receiverPrincipal; subaccount = null };
      amount = (FAUCET_AMOUNT);
      fee = null;
      memo = ?Blob.fromArray([1]);
      created_at_time = ?Nat64.fromNat(Int.abs(Time.now()));
    });

    switch (result) {
      case (#Ok(e)) {
        addUsedPrincipal3(caller);
        addUsedPrincipal3(receiverPrincipal);
        return #Ok("Token3 transfer of " # debug_show (FAUCET_AMOUNT) # " completed to " # receiver # " result: " #debug_show (e));
      };
      case (#Err(e)) {
        return #Err("Transfer failed: " # debug_show (e));
      };
    };
  };

  ignore Timer.recurringTimer<system>(
    #nanoseconds(CLEAR_INTERVAL),
    func() : async () {
      Map.clear(usedPrincipals1);
      Map.clear(usedPrincipals2);
      Map.clear(usedPrincipals3);
    },
  );

  system func inspect({
    arg : Blob;
    caller : Principal;
    msg : {
      #requestToken1 : () -> Text;
      #requestToken2 : () -> Text;
      #requestToken3 : () -> Text;
    };
  }) : Bool {
    if (Principal.toText(caller).size() < 30) {
      return false;
    };

    switch (msg) {
      case (#requestToken1(getReceiver)) {
        let receiver = Principal.fromText(getReceiver());
        isEligible1(caller) and isEligible1(receiver);
      };
      case (#requestToken2(getReceiver)) {
        let receiver = Principal.fromText(getReceiver());
        isEligible2(caller) and isEligible2(receiver);
      };
      case (#requestToken3(getReceiver)) {
        let receiver = Principal.fromText(getReceiver());
        isEligible3(caller) and isEligible3(receiver);
      };
    };
  };
};
