import Blob "mo:base/Blob";
import Result "mo:base/Result";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";
import Treasury_types "../treasury/treasury_types";
import DAO_types "../DAO_backend/dao_types";
import NachosTypes "../nachos_vault/nachos_vault_types";
import TacoSwapTypes "../taco_swap/taco_swap_types";
import BuybackTypes "../buyback_canister/buyback_canister_types";
import Nat "mo:base/Nat";

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

    public query func validate_setTokenMaxAllocation(token : Principal, maxBP : ?Nat, reason : Text) : async ValidationResult {
      #Ok("setTokenMaxAllocation called for token " # Principal.toText(token) # " with maxBP: " # debug_show(maxBP) # " reason: " # debug_show(reason));
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

// ==============================
// NACHO ledger GNSF — treasury transfer
// ==============================
// Equivalent of TransferSnsTreasuryFunds for the NACHO token. The built-in
// SNS treasury action only handles ICP (treasury=1) and TACO (treasury=2);
// NACHO sits in the SNS Governance canister's main account from genesis but
// has no native transfer path. This validator pairs with target
// (canister=o6ncl-lyaaa-aaaan-q6dua-cai, method=icrc1_transfer) so that an
// ExecuteGenericNervousSystemFunction proposal calls NACHO ledger directly —
// SNS Governance is the caller, NACHO is debited from its own account.

    public query func validate_transfer_nacho_treasury(arg : {
      to : { owner : Principal; subaccount : ?Subaccount };
      amount : Nat;
      fee : ?Nat;
      memo : ?Blob;
      from_subaccount : ?Subaccount;
      created_at_time : ?Nat64;
    }) : async ValidationResult {
      if (arg.amount == 0) return #Err("amount must be > 0");
      let toAcct = "owner=" # Principal.toText(arg.to.owner) #
                   " subaccount=" # debug_show(arg.to.subaccount);
      let fromSub = switch (arg.from_subaccount) {
        case (?b) { "subaccount=" # debug_show(b) };
        case null { "main account" };
      };
      #Ok(
        "Transfer NACHO from SNS treasury (" # fromSub # ")" #
        " → " # toAcct #
        "; amount=" # debug_show(arg.amount) # " e8s" #
        "; fee=" # debug_show(arg.fee) #
        "; memo=" # debug_show(arg.memo) #
        "; created_at_time=" # debug_show(arg.created_at_time)
      );
    };

// ==============================
// nachos_vault.mo GNSF Admin functions
// ==============================

    public query func validate_updateNachosFees(mintFeeBP : ?Nat, burnFeeBP : ?Nat) : async ValidationResult {
      #Ok("updateNachosFees called with mintFeeBP: " # debug_show(mintFeeBP) # " burnFeeBP: " # debug_show(burnFeeBP));
    };

    public query func validate_pauseNachosMinting(reason : Text) : async ValidationResult {
      #Ok("pauseNachosMinting called with reason: " # debug_show(reason));
    };

    public query func validate_unpauseNachosMinting(reason : Text) : async ValidationResult {
      #Ok("unpauseNachosMinting called with reason: " # debug_show(reason));
    };

    public query func validate_pauseNachosBurning(reason : Text) : async ValidationResult {
      #Ok("pauseNachosBurning called with reason: " # debug_show(reason));
    };

    public query func validate_unpauseNachosBurning(reason : Text) : async ValidationResult {
      #Ok("unpauseNachosBurning called with reason: " # debug_show(reason));
    };

    public query func validate_nachosEmergencyPause(reason : Text) : async ValidationResult {
      #Ok("nachosEmergencyPause called with reason: " # debug_show(reason));
    };

    public query func validate_nachosEmergencyUnpause(reason : Text) : async ValidationResult {
      #Ok("nachosEmergencyUnpause called with reason: " # debug_show(reason));
    };

    public query func validate_resetNachosCircuitBreaker(reason : Text) : async ValidationResult {
      #Ok("resetNachosCircuitBreaker called with reason: " # debug_show(reason));
    };

    public query func validate_addAcceptedMintToken(token : Principal) : async ValidationResult {
      #Ok("addAcceptedMintToken called with token: " # Principal.toText(token));
    };

    public query func validate_removeAcceptedMintToken(token : Principal) : async ValidationResult {
      #Ok("removeAcceptedMintToken called with token: " # Principal.toText(token));
    };

    public query func validate_setAcceptedMintTokenEnabled(token : Principal, enabled : Bool) : async ValidationResult {
      #Ok("setAcceptedMintTokenEnabled called with token: " # Principal.toText(token) # " enabled: " # debug_show(enabled));
    };

    public query func validate_updateCancellationFeeMultiplier(multiplier : Nat) : async ValidationResult {
      #Ok("updateCancellationFeeMultiplier called with multiplier: " # debug_show(multiplier));
    };

    public query func validate_addFeeExemptPrincipal(principal : Principal) : async ValidationResult {
      #Ok("addFeeExemptPrincipal called with principal: " # Principal.toText(principal));
    };

    public query func validate_removeFeeExemptPrincipal(principal : Principal) : async ValidationResult {
      #Ok("removeFeeExemptPrincipal called with principal: " # Principal.toText(principal));
    };

    public query func validate_addRateLimitExemptPrincipal(principal : Principal) : async ValidationResult {
      #Ok("addRateLimitExemptPrincipal called with principal: " # Principal.toText(principal));
    };

    public query func validate_removeRateLimitExemptPrincipal(principal : Principal) : async ValidationResult {
      #Ok("removeRateLimitExemptPrincipal called with principal: " # Principal.toText(principal));
    };

    public query func validate_recoverWronglySentTokens(tokenPrincipal : Principal, blockNumber : Nat, senderPrincipal : Principal) : async ValidationResult {
      #Ok("recoverWronglySentTokens called with token: " # Principal.toText(tokenPrincipal) # " block: " # debug_show(blockNumber) # " sender: " # Principal.toText(senderPrincipal));
    };

    public query func validate_retryFailedTransfers() : async ValidationResult {
      #Ok("retryFailedTransfers: Reset retry counts for all exhausted transfer tasks");
    };

    public query func validate_recoverStuckNachos(recipient : Principal, amount : Nat) : async ValidationResult {
      #Ok("recoverStuckNachos: Recover " # debug_show(amount) # " stuck NACHOS to " # Principal.toText(recipient));
    };

    public query func validate_retryFailedBurnDelivery(burnId : Nat) : async ValidationResult {
      #Ok("retryFailedBurnDelivery: Retry failed token deliveries for burn #" # debug_show(burnId));
    };

    public query func validate_retryFailedForwardDelivery(mintId : Nat) : async ValidationResult {
      #Ok("retryFailedForwardDelivery: Retry failed forward transfers for mint #" # debug_show(mintId));
    };

    public query func validate_retryFailedRefundDelivery(opId : Nat) : async ValidationResult {
      #Ok("retryFailedRefundDelivery: Retry failed refund transfers for operation #" # debug_show(opId));
    };

    public query func validate_claimNachosMintFees(recipient : Principal, tokenPrincipal : Principal, amount : Nat) : async ValidationResult {
      #Ok("claimMintFees: Claim " # debug_show(amount) # " mint fees for token " # Principal.toText(tokenPrincipal) # " to " # Principal.toText(recipient));
    };

    public query func validate_claimNachosBurnFees(recipient : Principal, tokenPrincipal : Principal, amount : Nat) : async ValidationResult {
      #Ok("claimBurnFees: Claim " # debug_show(amount) # " burn fees for token " # Principal.toText(tokenPrincipal) # " to " # Principal.toText(recipient));
    };

    public query func validate_claimNachosCancellationFees(recipient : Principal, tokenPrincipal : Principal, amount : Nat) : async ValidationResult {
      #Ok("claimCancellationFees: Claim " # debug_show(amount) # " cancellation fees for token " # Principal.toText(tokenPrincipal) # " to " # Principal.toText(recipient));
    };

    // --- Circuit Breaker Condition Validation ---

    public query func validate_addCircuitBreakerCondition(input : NachosTypes.CircuitBreakerConditionInput) : async ValidationResult {
      #Ok("addCircuitBreakerCondition: type=" # debug_show(input.conditionType) # " threshold=" # debug_show(input.thresholdPercent) # "% action=" # debug_show(input.action));
    };

    public query func validate_removeCircuitBreakerCondition(conditionId : Nat) : async ValidationResult {
      #Ok("removeCircuitBreakerCondition: id=" # Nat.toText(conditionId));
    };

    public query func validate_updateCircuitBreakerCondition(
      conditionId : Nat,
      thresholdPercent : ?Float,
      timeWindowNS : ?Nat,
      direction : ?{ #Up; #Down; #Both },
      action : ?NachosTypes.CircuitBreakerAction,
      applicableTokens : ?[Principal],
    ) : async ValidationResult {
      #Ok("updateCircuitBreakerCondition: id=" # Nat.toText(conditionId) # " threshold=" # debug_show(thresholdPercent) # " action=" # debug_show(action));
    };

    public query func validate_enableCircuitBreakerCondition(conditionId : Nat, enabled : Bool) : async ValidationResult {
      #Ok("enableCircuitBreakerCondition: id=" # Nat.toText(conditionId) # " enabled=" # debug_show(enabled));
    };

// ==============================
// taco_swap.mo GNSF Admin functions
// ==============================

    public query func validate_pauseTacoSwap(reason : Text) : async ValidationResult {
      #Ok("pauseTacoSwap called with reason: " # debug_show(reason));
    };

    public query func validate_unpauseTacoSwap(reason : Text) : async ValidationResult {
      #Ok("unpauseTacoSwap called with reason: " # debug_show(reason));
    };

    public query func validate_setTacoSwapPoolId(poolId : Principal, zeroForOne : Bool) : async ValidationResult {
      #Ok("setTacoSwapPoolId called with poolId: " # Principal.toText(poolId) # " zeroForOne: " # debug_show(zeroForOne));
    };

    public query func validate_updateTacoSwapConfig(config : TacoSwapTypes.SwapConfig) : async ValidationResult {
      #Ok("updateTacoSwapConfig called with config: " # debug_show(config));
    };

    public query func validate_recoverTacoSwapFunds(principal : Principal) : async ValidationResult {
      #Ok("recoverTacoSwapFunds called for principal: " # Principal.toText(principal));
    };

    public query func validate_retryTacoSwapPending() : async ValidationResult {
      #Ok("retryTacoSwapPending: Retry all pending swaps");
    };

    public query func validate_recoverTacoSwapPoolBalances() : async ValidationResult {
      #Ok("recoverTacoSwapPoolBalances: Recover stuck tokens from ICPSwap pools");
    };

    // ──────────────────────────────────────────────────────────────
    // Vault claim validator (vault function used by buyback canister)
    // ──────────────────────────────────────────────────────────────

    public query func validate_claimAllFees(recipient : Principal, recipientSubaccount : ?Subaccount) : async ValidationResult {
      #Ok("claimAllFees recipient=" # Principal.toText(recipient) # " subaccount=" # debug_show(recipientSubaccount));
    };

    // ──────────────────────────────────────────────────────────────
    // Buyback canister validators
    // ──────────────────────────────────────────────────────────────

    public query func validate_updateBuybackConfig(patch : BuybackTypes.BuybackConfigUpdate) : async ValidationResult {
      switch (patch.intervalNS) {
        case (?v) {
          if (v < 3_600_000_000_000 or v > 30 * 86_400_000_000_000) {
            return #Err("intervalNS out of range [1 hour, 30 days]");
          };
        };
        case null {};
      };
      switch (patch.minTokenValueICP) {
        case (?v) { if (v == 0) { return #Err("minTokenValueICP must be > 0") } };
        case null {};
      };
      switch (patch.arbDepth) {
        case (?v) { if (v < 2 or v > 6) { return #Err("arbDepth out of range [2, 6]") } };
        case null {};
      };
      switch (patch.arbTernaryProbes) {
        case (?v) { if (v < 3 or v > 20) { return #Err("arbTernaryProbes out of range [3, 20]") } };
        case null {};
      };
      switch (patch.arbMaxIterations) {
        case (?v) { if (v < 1 or v > 50) { return #Err("arbMaxIterations out of range [1, 50]") } };
        case null {};
      };
      switch (patch.arbMaxRoutesPerAnalysis) {
        case (?v) { if (v < 1 or v > 20) { return #Err("arbMaxRoutesPerAnalysis out of range [1, 20]") } };
        case null {};
      };
      switch (patch.arbSettlementTimeoutMs) {
        case (?v) { if (v < 5_000 or v > 120_000) { return #Err("arbSettlementTimeoutMs out of range [5000, 120000]") } };
        case null {};
      };
      switch (patch.arbProfitSlippageBps) {
        case (?v) { if (v > 10000) { return #Err("arbProfitSlippageBps out of range [0, 10000]") } };
        case null {};
      };
      // enabled=true outside production is enforced at runtime in the buyback canister
      #Ok("updateBuybackConfig patch: " # debug_show(patch));
    };

    public query func validate_triggerBuybackNow() : async ValidationResult {
      #Ok("triggerBuybackNow: run one buyback cycle synchronously on the buyback canister");
    };

    public query func validate_syncTokenDetailsFromTreasury() : async ValidationResult {
      #Ok("syncTokenDetailsFromTreasury: pull fresh price/decimals/fee from treasury into buyback cache");
    };

    public query func validate_adminWithdraw(token : Principal, to : Principal, amount : Nat) : async ValidationResult {
      #Ok("adminWithdraw token=" # Principal.toText(token) # " to=" # Principal.toText(to) # " amount=" # debug_show(amount) # " (controllers only — emergency recovery)");
    };

    public query func validate_adminBurnNow() : async ValidationResult {
      #Ok("adminBurnNow: trigger TACO burn outside the daily cycle");
    };

    public query func validate_emergencyStop(reason : Text) : async ValidationResult {
      #Ok("emergencyStop: cancel timer, clear cycleInProgress, set enabled=false. Reason: " # reason);
    };

    // ── Exchange (OTC_backend) batch LP fee claim ──
    // User-callable: claims every unclaimed LP fee across the caller's V3
    // (concentratedPositions) and V2 (userLiquidityPositions) entries in a
    // single message. Per-position math identical to the existing single-position
    // functions (claimLPFees / claimConcentratedFees / batchClaimAllFees).
    // Transfers consolidated by token; saved ledger fees recovered to feescollectedDAO.
    public query func validate_claimAllLPFees() : async ValidationResult {
      #Ok(
        "claimAllLPFees: harvest all LP fees across the caller's V3 unified positions" #
        " (full-range + concentrated) and V2 legacy positions in one call." #
        " Per-position math mirrors claimLPFees/claimConcentratedFees/batchClaimAllFees." #
        " Transfers consolidate by (recipient, token); saved ledger fees recovered to" #
        " feescollectedDAO. Single inter-canister flush. User-callable; identical drift" #
        " profile to N single-position calls."
      );
    };

    // ── Exchange (OTC_backend) flash arbitrage ──
    // Buyback-canister-only; the exchange's inspect block rejects all other callers
    // at ingress. Validator documents the call shape for SNS proposal previews.
    public query func validate_adminFlashArb(
      notional : Nat,
      route : [{ tokenIn : Text; tokenOut : Text }],
      minProfit : Nat,
    ) : async ValidationResult {
      let hopCount = route.size();
      if (notional == 0) return #Err("notional must be > 0");
      if (hopCount < 2 or hopCount > 6) return #Err("route must have 2-6 hops");
      if (route[0].tokenIn != route[hopCount - 1].tokenOut) {
        return #Err("route must be circular: route[0].tokenIn must equal route[last].tokenOut");
      };
      var i = 0;
      var routeStr = route[0].tokenIn;
      while (i < hopCount) {
        if (i > 0 and route[i].tokenIn != route[i - 1].tokenOut) {
          return #Err("route broken at hop " # debug_show(i));
        };
        routeStr := routeStr # " → " # route[i].tokenOut;
        i += 1;
      };
      #Ok(
        "adminFlashArb (BUYBACK-CANISTER-ONLY): notional=" # debug_show(notional) #
        " minProfit=" # debug_show(minProfit) #
        " hops=" # debug_show(hopCount) #
        " route=" # routeStr #
        ". Exchange lends notional from feescollectedDAO[" # route[0].tokenIn #
        "] (or runs phantom mode), executes circular route, traps if grossProfit" #
        " < minProfit + tradingFee + inputTfees + transferFee." #
        " Profit guard guarantees recipient gets netProfit > 0 or message reverts."
      );
    };

}