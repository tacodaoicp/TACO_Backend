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
    type PriceDirection = Treasury_types.PriceDirection;
    type PortfolioCircuitBreakerUpdate = Treasury_types.PortfolioCircuitBreakerUpdate;
    type TriggerConditionUpdate = Treasury_types.TriggerConditionUpdate;
    type PortfolioDirection = Treasury_types.PortfolioDirection;
    type PortfolioValueType = Treasury_types.PortfolioValueType;
    type PortfolioCircuitBreakerCondition = Treasury_types.PortfolioCircuitBreakerCondition;

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
    // 3009
    public query func validate_pauseToken(token : Principal, reason : Text) : async ValidationResult {
      #Ok("pauseToken called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

    // 3010
    public query func validate_unpauseToken(token : Principal, reason : Text) : async ValidationResult {
      #Ok("unpauseToken called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

    // 3013
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

    // 3011
    public query func validate_updateRebalanceConfig(updates : UpdateConfig, rebalanceStateNew : ?Bool, reason : ?Text) : async ValidationResult {
      #Ok("updateRebalanceConfig called with updates " # debug_show(updates) # " with reason: " # debug_show(reason));
    };

    // 3012
    public query func validate_updateMaxPortfolioSnapshots(newLimit : Nat, reason : ?Text) : async ValidationResult {
      #Ok("updateMaxPortfolioSnapshots called with newLimit " # debug_show(newLimit) # " with reason: " # debug_show(reason));
    };

    // 3014
    public query func validate_startPortfolioSnapshots(reason : ?Text) : async ValidationResult {
      #Ok("startPortfolioSnapshots called with reason: " # debug_show(reason));
    };

    // 3015
    public query func validate_stopPortfolioSnapshots(reason : ?Text) : async ValidationResult {
      #Ok("stopPortfolioSnapshots called with reason: " # debug_show(reason));
    };

    // 3016
    public query func validate_updatePortfolioSnapshotInterval(intervalMinutes: Nat, reason: ?Text) : async ValidationResult {
      #Ok("updatePortfolioSnapshotInterval called newLimit with intervalMinutes " # debug_show(intervalMinutes) # " with reason: " # debug_show(reason));
    };

    // 3017
    public query func validate_syncWithDao() : async ValidationResult {
      #Ok("syncWithDao called.");
    };

    // 3018
    public query func validate_recoverPoolBalances() : async ValidationResult {
      #Ok("recoverPoolBalances called.");
    };


// /admin/price page functions

    public query func validate_pauseTokenFromTradingManual(token : Principal, reason : Text) : async ValidationResult {
      #Ok("pauseTokenFromTradingManual called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

    public query func validate_unpauseTokenFromTrading(token : Principal, reason : ?Text) : async ValidationResult {
      #Ok("unpauseTokenFromTrading called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

// neuronSnapshot.mo GNSF Admin functions

// /admin page functions
    // 3019
    public query func validate_take_neuron_snapshot() : async ValidationResult {
      #Ok("take_neuron_snapshot called.");
    };

// rewards.mo GNSF Admin functions

// /admin/distributions page functions
    // 3020
    public query func validate_addTriggerCondition(
      name : Text,
      direction : PriceDirection,
      percentage : Float,
      timeWindowNS : Nat,
      applicableTokens : [Principal]
    ) : async ValidationResult {
      #Ok("addTriggerCondition called with name: " # 
          debug_show(name) # " with direction: " # 
          debug_show(direction) # " with percentage: " # 
          debug_show(percentage) # " with timeWindowNS: " # 
          debug_show(timeWindowNS) # " with applicableTokens: " # 
          debug_show(applicableTokens));
    };

    // 3021
    public query func validate_setTriggerConditionActive(
      conditionId : Nat,
      isActive : Bool
    ) : async ValidationResult {
      #Ok("setTriggerConditionActive called with conditionId: " # 
          debug_show(conditionId) # " with isActive: " # 
          debug_show(isActive));
    };

    // 3022
    public query func validate_removeTriggerCondition(
      conditionId : Nat
    ) : async ValidationResult {
      #Ok("removeTriggerCondition called with conditionId: " # 
          debug_show(conditionId));
    };

    // 3023
    public query func validate_updateTriggerCondition(
      conditionId : Nat,
      updates : TriggerConditionUpdate
    ) : async ValidationResult {
      #Ok("updateTriggerCondition called with conditionId: " # 
          debug_show(conditionId) # " with updates: " # 
          debug_show(updates)); 
    };

    // 3024
    public query func validate_clearPriceAlerts() : async ValidationResult {
      #Ok("clearPriceAlerts called.");
    };

    // 3025
    public query func validate_clearSystemLogs() : async ValidationResult {
      #Ok("clearSystemLogs called.");
    };

    // 3026
    public query func validate_addPortfolioCircuitBreakerCondition(
      name : Text,
      direction : PortfolioDirection,
      percentage : Float,
      timeWindowNS : Nat,
      valueType : PortfolioValueType
    ) : async ValidationResult {
      #Ok("addPortfolioCircuitBreakerCondition called with name: " # 
        debug_show(name) # " with direction: " # 
        debug_show(direction) # " with percentage: " # 
        debug_show(percentage) # " with timeWindowNS: " # 
        debug_show(timeWindowNS) # " with valueType: " # 
        debug_show(valueType));
    };

    // 3027
    public query func validate_setPortfolioCircuitBreakerConditionActive(
      conditionId : Nat,
      isActive : Bool
    ) : async ValidationResult {
      #Ok("setPortfolioCircuitBreakerConditionActive called with conditionId: " # 
          debug_show(conditionId) # " with isActive: " # 
          debug_show(isActive));
    };

    // 3028
    public query func validate_removePortfolioCircuitBreakerCondition(
      conditionId : Nat
    ) : async ValidationResult {
      #Ok("removePortfolioCircuitBreakerCondition called with conditionId: " # 
          debug_show(conditionId));
    };

    // 3029
    public query func validate_updatePortfolioCircuitBreakerCondition(
      conditionId : Nat,
      updates : PortfolioCircuitBreakerUpdate
    ) : async ValidationResult {
      #Ok("updatePortfolioCircuitBreakerCondition called with conditionId: " # 
          debug_show(conditionId) # " with updates: " # 
          debug_show(updates));
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