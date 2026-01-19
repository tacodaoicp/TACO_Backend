import Blob "mo:base/Blob";
import Result "mo:base/Result";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";
import Treasury_types "../treasury/treasury_types";
import DAO_types "../DAO_backend/dao_types";

persistent actor validation {

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

    public type PriceType = {
      #ICP;
      #USD;
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

// ==============================
// DAO.mo GNSF Admin functions    
// ==============================

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

// ==============================
// treasury.mo GNSF Admin functions
// ==============================

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
    public query func validate_unpauseTokenFromTrading(token : Principal, reason : ?Text) : async ValidationResult {
      #Ok("unpauseTokenFromTrading called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };

    // 3024
    public query func validate_pauseTokenFromTradingManual(token : Principal, reason : Text) : async ValidationResult {
      #Ok("pauseTokenFromTradingManual called for token " # Principal.toText(token) # " with reason: " # debug_show(reason));
    };
    

    // 3025
    public query func validate_clearAllTradingPauses(reason : ?Text) : async ValidationResult {
      #Ok("clearAllTradingPauses called with reason: " # debug_show(reason));
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

// ==============================
// neuronSnapshot.mo GNSF Admin functions
// ==============================

// /admin page functions
    // 3019
    public query func validate_take_neuron_snapshot() : async ValidationResult {
      #Ok("take_neuron_snapshot called.");
    };

// /admin/neuron page functions

    // 3029
    public query func validate_setMaxNeuronSnapshots(maxSnapshots : Nat)  : async ValidationResult {
      #Ok("setMaxNeuronSnapshots called with maxSnapshots: " # debug_show(maxSnapshots));
    };

// /admin/archive page (proxy) functions

    // 3040
    public query func validate_startBatchImportSystem(archivePrincipal : Principal) : async ValidationResult {
      #Ok("startBatchImportSystem called with archivePrincipal: . # Principal.toText(archivePrincipal)");
    };

    // 3041
    public query func validate_stopBatchImportSystem(archivePrincipal : Principal) : async ValidationResult {
      #Ok("stopBatchImportSystem called with archivePrincipal: " # Principal.toText(archivePrincipal));
    };

    // 3042
    public query func validate_stopAllTimers(archivePrincipal : Principal) : async ValidationResult {
      #Ok("stopAllTimers called with archivePrincipal: " # Principal.toText(archivePrincipal));
    };

    // 3043
    public query func validate_runManualBatchImport(archivePrincipal : Principal) : async ValidationResult {
      #Ok("runManualBatchImport called with archivePrincipal: " # Principal.toText(archivePrincipal));
    };

    // 3044
    public query func validate_setMaxInnerLoopIterations(archivePrincipal : Principal, iterations : Nat) : async ValidationResult {
      #Ok("setMaxInnerLoopIterations called with archivePrincipal: " # Principal.toText(archivePrincipal) # " with iterations: " # debug_show(iterations));
    };

    // 3045
    public query func validate_resetImportTimestamps(archivePrincipal : Principal) : async ValidationResult {
      #Ok("resetImportTimestamps called with archivePrincipal: " # Principal.toText(archivePrincipal));
    };

// ==============================
// rewards.mo GNSF Admin functions
// ==============================

// /admin/distributions page functions

    // 3030
    public query func validate_triggerDistribution() : async ValidationResult {
      #Ok("triggerDistribution called.");
    };

    // 3031
    public query func validate_startDistributionTimer() : async ValidationResult {
      #Ok("startDistributionTimer called.");
    };

    // 3032
    public query func validate_stopDistributionTimer() : async ValidationResult {
      #Ok("stopDistributionTimer called.");
    };

    // 3033
    public query func validate_triggerDistributionCustom(startTime : Int, endTime : Int, priceType : PriceType) : async ValidationResult {
      #Ok("triggerDistributionCustom called with startTime: " # debug_show(startTime) # " with endTime: " # debug_show(endTime) # " with priceType: " # debug_show(priceType));
    };

    // 3034
    public query func validate_setPeriodicRewardPot(amount : Nat) : async ValidationResult {
      #Ok("setPeriodicRewardPot called with amount: " # debug_show(amount));
    };

    // 3035
    public query func validate_setDistributionPeriod(periodNS : Nat) : async ValidationResult {
      #Ok("setDistributionPeriod called with periodNS: " # debug_show(periodNS));
    };

    // 3036
    public query func validate_setPerformanceScorePower(power : Float) : async ValidationResult {
      #Ok("setPerformanceScorePower called with power: " # debug_show(power));
    };

    // 3037
    public query func validate_setVotingPowerPower(power : Float) : async ValidationResult {
      #Ok("setVotingPowerPower called with power: " # debug_show(power));
    };

    // 3038
    public query func validate_addToRewardSkipList(neuronId : Blob) : async ValidationResult {
      #Ok("addToRewardSkipList called with neuronId: " # debug_show(neuronId));
    };

    // 3039
    public query func validate_removeFromRewardSkipList(neuronId : Blob) : async ValidationResult {
      #Ok("removeFromRewardSkipList called with neuronId: " # debug_show(neuronId));
    };

    // 3050 - New penalty functions
    public query func validate_setRewardPenalty(neuronId : Blob, multiplier : Nat) : async ValidationResult {
      #Ok("setRewardPenalty called with neuronId: " # debug_show(neuronId) # " with multiplier: " # debug_show(multiplier));
    };

    // 3051
    public query func validate_removeRewardPenalty(neuronId : Blob) : async ValidationResult {
      #Ok("removeRewardPenalty called with neuronId: " # debug_show(neuronId));
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