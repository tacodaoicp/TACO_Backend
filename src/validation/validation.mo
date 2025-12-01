import DAO_types "../DAO_backend/dao_types";
import Blob "mo:base/Blob";
import Result "mo:base/Result";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";

actor validation {

    type Subaccount = Blob;
    type TokenType = DAO_types.TokenType;
    type AuthorizationError = DAO_types.AuthorizationError;

    type ValidationResult = {
        #Ok: Text;
        #Err :Text;
    };

    public shared func validate_addToken(token : Principal, tokenType : TokenType) : async ValidationResult {

      let msg : Text = 
        "token: " # Principal.toText(token) #  
        ", tokenType: " # debug_show(tokenType);

      #Ok(msg);

    };


    public query func validate_sendToken(token : Principal, amount_e8s : Nat, to_principal : Principal, to_subaccount : ?Subaccount) : async ValidationResult {

      let to_account = { owner = to_principal; subaccount = to_subaccount };

      let msg:Text = "amount_e8s: " # debug_show(amount_e8s) #  
      ", token: " # Principal.toText(token) # 
      ", to_account: " # debug_show(to_account);

      #Ok(msg);
    };

// treasury.mo GNSF Admin functions

    public query func validate_stopRebalancing(reason : ?Text) : async ValidationResult {
      #Ok("stopRebalancing called with reason: " # debug_show(reason));
    };

    public query func validate_startRebalancing(reason : ?Text) : async ValidationResult {
      #Ok("startRebalancing called with reason: " # debug_show(reason));
    };

    public query func validate_executeTradingCycle(reason : ?Text) : async ValidationResult {
      #Ok("executeTradingCycle called with reason: " # debug_show(reason));
    };

    public query func validate_takeManualPortfolioSnapshot(reason : ?Text) : async ValidationResult {
      #Ok("takeManualPortfolioSnapshot called with reason: " # debug_show(reason));
    };

    public query func validate_pauseTokenFromTradingManual(token : Principal, reason : Text) : async ValidationResult {
      #Ok("pauseTokenFromTradingManual called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

    public query func validate_unpauseTokenFromTrading(token : Principal, reason : ?Text) : async ValidationResult {
      #Ok("unpauseTokenFromTrading called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

//admin_executeTradingCycle, takeManualPortfolioSnapshot
    transient var gnsf1_cnt : Nat = 0;    
    transient var gnsf2_cnt : Nat = 0;    
    transient var gnsf2_principal : Principal = Principal.fromText("aaaaa-aa");    

    public query func get_gnsf1_cnt() : async Nat {
      gnsf1_cnt;
    };

    public shared ({ caller }) func test_gnsf1() : async () {
      assert (caller == Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai"));
      gnsf1_cnt += 1;
      return;
    };

    public shared ({ caller }) func test_gnsf2(principal : Principal) : async () {
      assert (caller == Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai"));
      gnsf2_cnt += 1;
      gnsf2_principal := principal;
      return;
    };

    public query func validate_test_gnsf1() : async ValidationResult {
      #Ok("validate_test_gnsf1: " # debug_show(gnsf1_cnt));
    };

    public query func validate_test_gnsf2() : async ValidationResult {
      #Ok("validate_test_gnsf2: " # debug_show(gnsf2_cnt) # 
        " " # debug_show(gnsf2_principal));
    };

    public query func get_canister_cycles() : async { cycles : Nat } {
      { cycles = Cycles.balance() };
    };

}