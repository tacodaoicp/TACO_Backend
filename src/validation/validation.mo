import Blob "mo:base/Blob";
import Result "mo:base/Result";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";
import Treasury_types "../treasury/treasury_types";
import DAO_types "../DAO_backend/dao_types";

actor validation {

    type UpdateConfig = Treasury_types.UpdateConfig;
    type SystemParameter = DAO_types.SystemParameter;

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

// DAO.mo GNSF Admin functions    

// /admin page functions
    // 3007
    public query func validate_pauseToken(token : Principal, reason : Text) : async ValidationResult {
      #Ok("pauseToken called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

    // 3008
    public query func validate_unpauseToken(token : Principal, reason : Text) : async ValidationResult {
      #Ok("unpauseToken called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

    // 3011
    public query func validate_updateSystemParameter(param : SystemParameter, reason : ?Text) : async ValidationResult {
      #Ok("updateSystemParameter called with param " # debug_show(param) # " with reason: " # debug_show(reason));
    };


// treasury.mo GNSF Admin functions

// /admin page functions
    // 3003
    public query func validate_stopRebalancing(reason : ?Text) : async ValidationResult {
      #Ok("stopRebalancing called with reason: " # debug_show(reason));
    };

    // 3004
    public query func validate_startRebalancing(reason : ?Text) : async ValidationResult {
      #Ok("startRebalancing called with reason: " # debug_show(reason));
    };

    // 3005
    public query func validate_executeTradingCycle(reason : ?Text) : async ValidationResult {
      #Ok("executeTradingCycle called with reason: " # debug_show(reason));
    };

    // 3006
    public query func validate_takeManualPortfolioSnapshot(reason : ?Text) : async ValidationResult {
      #Ok("takeManualPortfolioSnapshot called with reason: " # debug_show(reason));
    };

    // 3009
    public query func validate_updateRebalanceConfig(updates : UpdateConfig, rebalanceStateNew : ?Bool, reason : ?Text) : async ValidationResult {
      #Ok("updateRebalanceConfig called with updates " # debug_show(updates) # " with reason: " # debug_show(reason));
    };

    // 3010
    public query func validate_updateMaxPortfolioSnapshots(newLimit : Nat, reason : ?Text) : async ValidationResult {
      #Ok("updateMaxPortfolioSnapshots called with newLimit " # debug_show(newLimit) # " with reason: " # debug_show(reason));
    };

    // 3012
    public query func validate_startPortfolioSnapshots(reason : ?Text) : async ValidationResult {
      #Ok("startPortfolioSnapshots called with reason: " # debug_show(reason));
    };

    // 3013
    public query func validate_stopPortfolioSnapshots(reason : ?Text) : async ValidationResult {
      #Ok("stopPortfolioSnapshots called with reason: " # debug_show(reason));
    };

    // 3014
    public query func validate_updatePortfolioSnapshotInterval(intervalMinutes: Nat, reason: ?Text) : async ValidationResult {
      #Ok("updatePortfolioSnapshotInterval called newLimit with intervalMinutes " # debug_show(intervalMinutes) # " with reason: " # debug_show(reason));
    };


// /admin/price page functions

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