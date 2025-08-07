// Code Flow:
//=========================================================================
// 1. SYSTEM CONFIGURATION & STATE
//=========================================================================
/*
 * Core configuration values, state management, and initialization parameters
 * that control the Treasury's operation
 */

//=========================================================================
// 2. PORTFOLIO DATA STRUCTURES
//=========================================================================
/*
 * Data structures for tracking tokens, balances, allocations, and
 * transaction status
 */

//=========================================================================
// 3. PUBLIC INTERFACES
//=========================================================================
/*
 * Public functions exposed to the DAO and other services for controlling
 * the Treasury and retrieving status information
 */

//=========================================================================
// 4. TOKEN TRANSFER SYSTEM
//=========================================================================
/*
 * Functions for handling token transfers to and from the Treasury for
 * DAO operations, with queuing and retry mechanisms
 */

//=========================================================================
// 5. REBALANCING ENGINE CORE
//=========================================================================
/*
 * Core rebalancing logic including trade cycle management, token selection,
 * and execution flow control
 */

//=========================================================================
// 6. TRADING ALGORITHM HELPERS
//=========================================================================
/*
 * Helper functions for the trading engine that handle allocation calculations,
 * trading pair selection, and size determination
 */

//=========================================================================
// 7. EXCHANGE INTEGRATION
//=========================================================================
/*
 * Functions for interacting with decentralized exchanges (KongSwap and ICPSwap),
 * including price quotes, liquidity checking, and trade execution
 */

//=========================================================================
// 8. DATA SYNCHRONIZATION & PRICE UPDATES
//=========================================================================
/*
 * Functions for keeping token data, prices, and balances up to date
 * with external systems
 */

//=========================================================================
// 9. RECOVERY & SAFETY SYSTEMS
//=========================================================================
/*
 * Safety mechanisms, error recovery functions, and circuit breakers
 * to handle exceptional conditions
 */

//=========================================================================
// 10. SYSTEM INITIALIZATION
//=========================================================================
/*
 * Initialization code that runs when the canister starts and
 * security validation
 */

import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Bool "mo:base/Bool";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import Map "mo:map/Map";
import Prim "mo:prim";
import Int "mo:base/Int";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import ICRC1 "mo:icrc1/ICRC1";
import Ledger "../helper/Ledger";
import Debug "mo:base/Debug";
import Vector "mo:vector";
import Error "mo:base/Error";
import { now } = "mo:base/Time";
import { setTimer; cancelTimer } = "mo:base/Timer";
import Result "mo:base/Result";
import DAO_types "../DAO_backend/dao_types";
import TreasuryTypes "../treasury/treasury_types";
import Nat8 "mo:base/Nat8";
import NTN "../helper/ntn";
import Float "mo:base/Float";
import Array "mo:base/Array";
import Order "mo:base/Order";
import KongSwap "../swap/kong_swap";
import ICPSwap "../swap/icp_swap";
import swaptypes "../swap/swap_types";
import Fuzz "mo:fuzz";
import SpamProtection "../helper/spam_protection";
import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";

shared (deployer) actor class treasury() = this {

  private func this_canister_id() : Principal {
      Principal.fromActor(this);
  };

  //=========================================================================
  // 1. SYSTEM CONFIGURATION & STATE
  //=========================================================================

  var test = false;

  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
  let NEURON_SNAPSHOT_ID = canister_ids.getCanisterId(#neuronSnapshot);

  // Logger
  let logger = Logger.Logger();

  // Canister principals and references
  //let self = "z4is7-giaaa-aaaad-qg6uq-cai";
  let self = Principal.toText(this_canister_id());
  let ICPprincipalText = "ryjl3-tyaaa-aaaaa-aaaba-cai";
  let ICPprincipal = Principal.fromText(ICPprincipalText);
  //stable var DAOText = "ywhqf-eyaaa-aaaad-qg6tq-cai";
  //stable var DAOText = "vxqw7-iqaaa-aaaan-qzziq-cai";
  let DAOText = Principal.toText(DAO_BACKEND_ID);
  //stable var DAOPrincipal = Principal.fromText(DAOText);
  let DAOPrincipal = DAO_BACKEND_ID;
  //stable var MintVaultPrincipal = Principal.fromText("z3jul-lqaaa-aaaad-qg6ua-cai");
  stable var MintVaultPrincipal = DAO_BACKEND_ID;

  var masterAdmins = [
    Principal.fromText("d7zib-qo5mr-qzmpb-dtyof-l7yiu-pu52k-wk7ng-cbm3n-ffmys-crbkz-nae"), 
    Principal.fromText("uuyso-zydjd-tsb4o-lgpgj-dfsvq-awald-j2zfp-e6h72-d2je3-whmjr-xae"), // lx7ws-diaaa-aaaag-aubda-cai.icp0.io identities
    Principal.fromText("5uvsz-em754-ulbgb-vxihq-wqyzd-brdgs-snzlu-mhlqw-k74uu-4l5h3-2qe"),
    Principal.fromText("6mxg4-njnu6-qzizq-2ekit-rnagc-4d42s-qyayx-jghoe-nd72w-elbsy-xqe"),
    Principal.fromText("6q3ra-pds56-nqzzc-itigw-tsw4r-vs235-yqx5u-dg34n-nnsus-kkpqf-aqe"),
    Principal.fromText("chxs6-z6h3t-hjrgk-i5x57-rm7fm-3tvlz-b352m-heq2g-hu23b-sxasf-kqe"), // tacodao.com identities
    Principal.fromText("k2xol-5avzc-lf3wt-vwoft-pjx6k-77fjh-7pera-6b7qt-fwt5e-a3ekl-vqe"),
    Principal.fromText("as6jn-gaoo7-k4kji-tdkxg-jlsrk-avxkc-zu76j-vz7hj-di3su-2f74z-qqe"),
    Principal.fromText("r27hb-ckxon-xohqv-afcvx-yhemm-xoggl-37dg6-sfyt3-n6jer-ditge-6qe"), // staging identities
    Principal.fromText("yjdlk-jqx52-ha6xa-w6iqe-b4jrr-s5ova-mirv4-crlfi-xgsaa-ib3cg-3ae"),
    Principal.fromText("as6jn-gaoo7-k4kji-tdkxg-jlsrk-avxkc-zu76j-vz7hj-di3su-2f74z-qqe")];


  // Core type aliases
  type Subaccount = Blob;
  type TransferRecipient = TreasuryTypes.TransferRecipient;
  type TransferResultICRC1 = TreasuryTypes.TransferResultICRC1;
  type TransferResultICP = TreasuryTypes.TransferResultICP;
  type TokenDetails = TreasuryTypes.TokenDetails;
  type Allocation = DAO_types.Allocation;
  type SyncError = DAO_types.SyncError;
  type SyncErrorTreasury = TreasuryTypes.SyncErrorTreasury;
  type RebalanceConfig = TreasuryTypes.RebalanceConfig;
  type RebalanceState = TreasuryTypes.RebalanceState;
  type RebalanceError = TreasuryTypes.RebalanceError;
  type RebalanceStatus = TreasuryTypes.RebalanceStatus;
  type RebalanceStatusResponse = TreasuryTypes.RebalanceStatusResponse;
  type PriceInfo = TreasuryTypes.PriceInfo;
  type PriceSource = TreasuryTypes.PriceSource;
  type TokenAllocation = TreasuryTypes.TokenAllocation;
  type ExchangeType = TreasuryTypes.ExchangeType;
  type TradeRecord = TreasuryTypes.TradeRecord;
  type UpdateConfig = TreasuryTypes.UpdateConfig;
  type PricePoint = TreasuryTypes.PricePoint;

  // Price failsafe system type aliases
  type PriceDirection = TreasuryTypes.PriceDirection;
  type ChangeType = TreasuryTypes.ChangeType;
  type TriggerCondition = TreasuryTypes.TriggerCondition;
  type TriggerPriceData = TreasuryTypes.TriggerPriceData;
  type PriceAlertLog = TreasuryTypes.PriceAlertLog;
  type PriceFailsafeError = TreasuryTypes.PriceFailsafeError;
  type TriggerConditionUpdate = TreasuryTypes.TriggerConditionUpdate;

  // Trading pause system type aliases
  type TradingPauseReason = TreasuryTypes.TradingPauseReason;
  type TradingPauseRecord = TreasuryTypes.TradingPauseRecord;
  type TradingPausesResponse = TreasuryTypes.TradingPausesResponse;
  type TradingPauseError = TreasuryTypes.TradingPauseError;

  // Portfolio snapshot system type aliases
  type TokenSnapshot = TreasuryTypes.TokenSnapshot;
  type PortfolioSnapshot = TreasuryTypes.PortfolioSnapshot;
  type SnapshotReason = TreasuryTypes.SnapshotReason;
  type PortfolioHistoryResponse = TreasuryTypes.PortfolioHistoryResponse;
  type PortfolioSnapshotError = TreasuryTypes.PortfolioSnapshotError;

  // Portfolio circuit breaker system type aliases
  type PortfolioDirection = TreasuryTypes.PortfolioDirection;
  type PortfolioValueType = TreasuryTypes.PortfolioValueType;
  type PortfolioCircuitBreakerCondition = TreasuryTypes.PortfolioCircuitBreakerCondition;
  type PortfolioTriggerData = TreasuryTypes.PortfolioTriggerData;
  type PortfolioCircuitBreakerLog = TreasuryTypes.PortfolioCircuitBreakerLog;
  type PortfolioCircuitBreakerError = TreasuryTypes.PortfolioCircuitBreakerError;
  type PortfolioCircuitBreakerUpdate = TreasuryTypes.PortfolioCircuitBreakerUpdate;

  // Admin action logging type aliases
  type TreasuryAdminActionType = TreasuryTypes.TreasuryAdminActionType;
  type TreasuryAdminActionRecord = TreasuryTypes.TreasuryAdminActionRecord;
  type TreasuryAdminActionsSinceResponse = TreasuryTypes.TreasuryAdminActionsSinceResponse;

  // Actor references
  let dao = actor (DAOText) : actor {
    getTokenDetails : shared () -> async [(Principal, TokenDetails)];
    getAggregateAllocation : shared () -> async [(Principal, Nat)];
    syncTokenDetailsFromTreasury : shared ([(Principal, TokenDetails)]) -> async Result.Result<Text, SyncError>;
    hasAdminPermission : query (principal : Principal, function : SpamProtection.AdminFunction) -> async Bool;
  };

  let priceCheckNTN = actor ("moe7a-tiaaa-aaaag-qclfq-cai") : NTN.Self;

  // Map utilities
  let { phash; thash } = Map;
  let hashpp = TreasuryTypes.hashpp;
  let { natToNat64 } = Prim;

  // Randomization
  let fuzz = Fuzz.fromSeed(Fuzz.fromSeed(Int.abs(now()) * Fuzz.Fuzz().nat.randomRange(0, 2 ** 70)).nat.randomRange(45978345345987, 2 ** 256) -45978345345987);

  // Rebalancing configuration
  stable var rebalanceConfig : RebalanceConfig = {
    rebalanceIntervalNS = if test { 2_000_000_000 } else { 60_000_000_000 }; // 1 minute
    maxTradeAttemptsPerInterval = 2;
    //minTradeValueICP = 200_000_000; // 2 ICP
    //maxTradeValueICP = 1_000_000_000; // 10 ICP
    minTradeValueICP = 2_000_000; // 0.02 ICP
    maxTradeValueICP = 10_000_000; // 0.1 ICP
    portfolioRebalancePeriodNS = 604_800_000_000_000; // 1 week
    maxSlippageBasisPoints = if test { 10000 } else { 450 }; // 0.5% incl fee
    maxTradesStored = 2000;
    maxKongswapAttempts = 1;
    shortSyncIntervalNS = 900_000_000_000; // 15 minutes (for prices, balances)
    longSyncIntervalNS = 5 * 3600_000_000_000; // 5 hours (for metadata, pools)
    tokenSyncTimeoutNS = 21_600_000_000_000; // 6 hours
  };

  // Separate stable variable for circuit breaker configuration
  stable var pausedTokenThresholdForCircuitBreaker : Nat = 3; // Circuit breaker when 3+ tokens are paused

  // Rebalancing state
  stable var rebalanceState : RebalanceState = {
    status = #Idle;
    config = rebalanceConfig;
    metrics = {
      lastPriceUpdate = 0;
      lastRebalanceAttempt = 0;
      totalTradesExecuted = 0;
      totalTradesFailed = 0;
      totalTradesSkipped = 0;
      skipBreakdown = {
        noPairsFound = 0;
        noExecutionPath = 0;
        tokensFiltered = 0;
        pausedTokens = 0;
        insufficientCandidates = 0;
      };
      currentStatus = #Idle;
      portfolioValueICP = 0;
      portfolioValueUSD = 0;
    };
    lastTrades = Vector.new<TradeRecord>();
    priceUpdateTimerId = null;
    rebalanceTimerId = null;
  };

  stable var MAX_PRICE_HISTORY_ENTRIES = 2000;

  //=========================================================================
  // 2. PORTFOLIO DATA STRUCTURES
  //=========================================================================

  // Core portfolio data
  stable let tokenDetailsMap = Map.new<Principal, TokenDetails>();
  stable let currentAllocations = Map.new<Principal, Nat>();

  // Exchange pool data
  stable var ICPswapPools = Map.new<(Principal, Principal), swaptypes.PoolData>();

  // Transaction tracking
  stable let pendingTxs = Map.new<Nat, swaptypes.SwapTxRecord>();
  stable let failedTxs = Map.new<Nat, swaptypes.SwapTxRecord>();

  // Timer IDs for scheduling
  var shortSyncTimerId : Nat = 0;
  var longSyncTimerId : Nat = 0;

  // Transfer queue and management
  stable let transferQueue = Vector.new<(TransferRecipient, Nat, Principal, Nat8)>();
  stable var transferTimerIDs = Vector.new<Nat>();
  stable var nsAdd : Nat64 = 0; // ns to add tx time to avoid errorsstable var nsAdd : Nat64 = 0;

  //=========================================================================
  // PRICE FAILSAFE SYSTEM STORAGE
  //=========================================================================
  
  // Storage for trigger conditions and price alerts
  stable let triggerConditions = Map.new<Nat, TriggerCondition>();
  stable let priceAlerts = Vector.new<PriceAlertLog>();
  stable var nextConditionId : Nat = 1;
  stable var nextAlertId : Nat = 1;
  stable var maxPriceAlerts = 1000; // Maximum number of price alerts to store

  // Trading pause storage
  stable let tradingPauses = Map.new<Principal, TradingPauseRecord>();

  //=========================================================================
  // PORTFOLIO SNAPSHOT SYSTEM STORAGE
  //=========================================================================
  
  // Portfolio snapshot storage
  stable let portfolioSnapshots = Vector.new<PortfolioSnapshot>();
  stable var maxPortfolioSnapshots : Nat = 1000; // Keep ~42 days of hourly data
  stable var lastPortfolioSnapshotTime : Int = 0;
  stable var portfolioSnapshotTimerId : Nat = 0;

  //=========================================================================
  // PORTFOLIO CIRCUIT BREAKER SYSTEM STORAGE
  //=========================================================================
  
  // Storage for portfolio circuit breaker conditions and logs
  stable let portfolioCircuitBreakerConditions = Map.new<Nat, PortfolioCircuitBreakerCondition>();
  stable let portfolioCircuitBreakerLogs = Vector.new<PortfolioCircuitBreakerLog>();
  stable var nextPortfolioConditionId : Nat = 1;
  stable var nextPortfolioCircuitBreakerId : Nat = 1;
  stable var maxPortfolioCircuitBreakerLogs = 500; // Maximum number of circuit breaker logs to store

  // Admin action logging storage
  stable var treasuryAdminActionCounter: Nat = 0;
  stable var treasuryAdminActions = Vector.new<TreasuryAdminActionRecord>();
  stable var maxTreasuryAdminActionsStored: Nat = 10000; // Keep last 10k actions before archiving

  private func isMasterAdmin(caller : Principal) : Bool {
    // Check if caller is a human master admin
    for (admin in masterAdmins.vals()) {
      if (admin == caller) {
        return true;
      };
    };
    
    // Check if caller is one of our own canisters
    canister_ids.isKnownCanister(caller);
  };

  // Treasury admin action logging function
  private func logTreasuryAdminAction(
    admin: Principal,
    actionType: TreasuryAdminActionType,
    reason: Text,
    success: Bool,
    errorMessage: ?Text
  ) {
    treasuryAdminActionCounter += 1;
    let record: TreasuryAdminActionRecord = {
      id = treasuryAdminActionCounter;
      timestamp = now();
      admin = admin;
      actionType = actionType;
      reason = reason;
      success = success;
      errorMessage = errorMessage;
    };
    
    Vector.add(treasuryAdminActions, record);
    
    // Keep only the most recent actions (before archiving takes over)
    // Use standard treasury pattern for consistency
    if (Vector.size(treasuryAdminActions) > maxTreasuryAdminActionsStored) {
      Vector.reverse(treasuryAdminActions);
      while (Vector.size(treasuryAdminActions) > maxTreasuryAdminActionsStored) {
        ignore Vector.removeLast(treasuryAdminActions);
      };
      Vector.reverse(treasuryAdminActions);
    };
    
    // Still log to text logger for immediate debugging
    let actionDesc = getTreasuryActionDescription(actionType);
    let statusText = if (success) "SUCCESS" else "FAILED";
    logger.info("TreasuryAdminAction", 
      statusText # " - " # actionDesc # " by " # Principal.toText(admin) # 
      (if (reason != "") " | Reason: " # reason else "") #
      (switch (errorMessage) { case (?err) " | Error: " # err; case null "" }),
      "logTreasuryAdminAction"
    );
  };

  // Helper function to get human-readable action descriptions
  private func getTreasuryActionDescription(actionType: TreasuryAdminActionType) : Text {
    switch (actionType) {
      case (#StartRebalancing) "Start Trading Bot";
      case (#StopRebalancing) "Stop Trading Bot";
      case (#ResetRebalanceState) "Reset Trading State";
      case (#UpdateRebalanceConfig(_)) "Update Trading Config";
      case (#PauseTokenManual(details)) "Manual Token Pause: " # Principal.toText(details.token);
      case (#UnpauseToken(details)) "Unpause Token: " # Principal.toText(details.token);
      case (#ClearAllTradingPauses) "Clear All Trading Pauses";
      case (#AddTriggerCondition(details)) "Add Price Alert (ID: " # Nat.toText(details.conditionId) # ")";
      case (#RemoveTriggerCondition(details)) "Remove Price Alert (ID: " # Nat.toText(details.conditionId) # ")";
      case (#UpdateTriggerCondition(details)) "Update Price Alert (ID: " # Nat.toText(details.conditionId) # ")";
      case (#SetTriggerConditionActive(details)) "Set Price Alert Active (ID: " # Nat.toText(details.conditionId) # ", Active: " # Bool.toText(details.isActive) # ")";
      case (#ClearPriceAlerts) "Clear All Price Alerts";
      case (#AddPortfolioCircuitBreaker(details)) "Add Portfolio Circuit Breaker (ID: " # Nat.toText(details.conditionId) # ")";
      case (#RemovePortfolioCircuitBreaker(details)) "Remove Portfolio Circuit Breaker (ID: " # Nat.toText(details.conditionId) # ")";
      case (#UpdatePortfolioCircuitBreaker(details)) "Update Portfolio Circuit Breaker (ID: " # Nat.toText(details.conditionId) # ")";
      case (#SetPortfolioCircuitBreakerActive(details)) "Set Portfolio Circuit Breaker Active (ID: " # Nat.toText(details.conditionId) # ", Active: " # Bool.toText(details.isActive) # ")";
      case (#UpdatePausedTokenThreshold(details)) "Update Paused Token Threshold: " # Nat.toText(details.oldThreshold) # " → " # Nat.toText(details.newThreshold);
      case (#ClearPortfolioCircuitBreakerLogs) "Clear Portfolio Circuit Breaker Logs";
      case (#UpdateMaxPortfolioSnapshots(details)) "Update Max Portfolio Snapshots: " # Nat.toText(details.oldLimit) # " → " # Nat.toText(details.newLimit);
      case (#SetTestMode(details)) "Set Test Mode: " # Bool.toText(details.isTestMode);
      case (#ClearSystemLogs) "Clear System Logs";
      case (#TakeManualSnapshot) "Take Manual Portfolio Snapshot";
      case (#ExecuteTradingCycle) "Execute Manual Trading Cycle";
    }
  };

  // Helper function to increment skip counters
  private func incrementSkipCounter(skipType : { #noPairsFound; #noExecutionPath; #tokensFiltered; #pausedTokens; #insufficientCandidates }) {
    rebalanceState := {
      rebalanceState with
      metrics = {
        rebalanceState.metrics with
        totalTradesSkipped = rebalanceState.metrics.totalTradesSkipped + 1;
        skipBreakdown = switch (skipType) {
          case (#noPairsFound) {
            {
              rebalanceState.metrics.skipBreakdown with
              noPairsFound = rebalanceState.metrics.skipBreakdown.noPairsFound + 1;
            };
          };
          case (#noExecutionPath) {
            {
              rebalanceState.metrics.skipBreakdown with
              noExecutionPath = rebalanceState.metrics.skipBreakdown.noExecutionPath + 1;
            };
          };
          case (#tokensFiltered) {
            {
              rebalanceState.metrics.skipBreakdown with
              tokensFiltered = rebalanceState.metrics.skipBreakdown.tokensFiltered + 1;
            };
          };
          case (#pausedTokens) {
            {
              rebalanceState.metrics.skipBreakdown with
              pausedTokens = rebalanceState.metrics.skipBreakdown.pausedTokens + 1;
            };
          };
          case (#insufficientCandidates) {
            {
              rebalanceState.metrics.skipBreakdown with
              insufficientCandidates = rebalanceState.metrics.skipBreakdown.insufficientCandidates + 1;
            };
          };
        };
      };
    };
  };

  //=========================================================================
  // TRADING PAUSE SYSTEM HELPERS
  //=========================================================================

  /**
   * Check if a token is paused from trading
   * This checks BOTH the old pause system AND the new trading pause system
   */
  private func isTokenPausedFromTrading(token : Principal) : Bool {
    // Check old pause system
    let pausedByOldSystem = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) { details.isPaused or details.pausedDueToSyncFailure };
      case null { false };
    };
    
    // Check new trading pause system
    let pausedByTradingRules = switch (Map.get(tradingPauses, phash, token)) {
      case (?_) { true };
      case null { false };
    };
    
    // Token is paused if either system says it's paused
    pausedByOldSystem or pausedByTradingRules;
  };

  /**
   * Pause a token from trading due to price alert
   */
  private func pauseTokenFromTrading(token : Principal, reason : TradingPauseReason) : Bool {
    switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) {
        // Check if already paused
        switch (Map.get(tradingPauses, phash, token)) {
          case (?_) { 
            Debug.print("Token " # details.tokenSymbol # " already paused from trading");
            return false; 
          };
          case null {};
        };

        // Create pause record
        let pauseRecord : TradingPauseRecord = {
          token = token;
          tokenSymbol = details.tokenSymbol;
          reason = reason;
          pausedAt = now();
        };

        Map.set(tradingPauses, phash, token, pauseRecord);
        
        // Log the pause
        switch (reason) {
          case (#PriceAlert(data)) {
            logger.warn("TRADING_PAUSE", 
              "Token paused from trading due to PRICE ALERT - Token=" # details.tokenSymbol # 
              " (" # Principal.toText(token) # ")" #
              " Condition=" # data.conditionName #
              " Alert_ID=" # Nat.toText(data.alertId),
              "pauseTokenFromTrading"
            );
          };
          case (#CircuitBreaker(data)) {
            logger.warn("TRADING_PAUSE", 
              "Token paused from trading due to CIRCUIT BREAKER - Token=" # details.tokenSymbol # 
              " (" # Principal.toText(token) # ")" #
              " Reason=" # data.reason #
              " Severity=" # data.severity,
              "pauseTokenFromTrading"
            );
          };
        };
        
        true;
      };
      case null { 
        Debug.print("Error: Token details not found when trying to pause from trading");
        false;
      };
    };
  };

  //=========================================================================
  // 3. PUBLIC INTERFACES
  //=========================================================================

  /**
   * Start the automatic rebalancing process
   *
   * Initializes the rebalancing engine, which will periodically:
   * 1. Check current vs target allocations
   * 2. Select tokens to trade
   * 3. Execute trades on the best exchange
   *
   * Only callable by DAO or controller.
   */
  public shared ({ caller }) func startRebalancing(reason : ?Text) : async Result.Result<Text, RebalanceError> {
    if (((await hasAdminPermission(caller, #startRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#ConfigError("Not authorized"));
    };

    Debug.print("Starting rebalancing");

    if (rebalanceState.status != #Idle) {
      Debug.print("Rebalancing already in progress");
      return #err(#SystemError("Rebalancing already in progress"));
    };

    rebalanceState := {
      rebalanceState with
      status = #Trading;
      metrics = {
        rebalanceState.metrics with
        lastRebalanceAttempt = now();
      };
    };

    try {
      startTradingTimer<system>();
      
      // Start portfolio snapshot timer if not already running
      if (portfolioSnapshotTimerId == 0) {
        ignore await* startPortfolioSnapshotTimer<system>();
      };
      
      Debug.print("Rebalancing started");
      
      // Log the successful admin action
      let reasonText = switch (reason) {
        case (?r) r;
        case null "Rebalancing started programmatically";
      };
      logTreasuryAdminAction(
        caller,
        #StartRebalancing,
        reasonText,
        true,
        null
      );
      
      #ok("Rebalancing started");
    } catch (e) {
      rebalanceState := {
        rebalanceState with
        status = #Failed("Failed to start trading: " # Error.message(e));
        metrics = {
          rebalanceState.metrics with
          currentStatus = #Failed("Startup error");
        };
      };
      Debug.print("Failed to start trading: " # Error.message(e));
      
      // Log the failed admin action
      logTreasuryAdminAction(
        caller,
        #StartRebalancing,
        "Failed to start rebalancing",
        false,
        ?Error.message(e)
      );
      
      #err(#SystemError("Failed to start trading: " # Error.message(e)));
    };
  };

  /**
   * Stop the automatic rebalancing process
   *
   * Cancels all timers and sets the system to idle state
   * Only callable by DAO or controller.
   */
  public shared ({ caller }) func stopRebalancing(reason : ?Text) : async Result.Result<Text, RebalanceError> {
    if (((await hasAdminPermission(caller, #stopRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      Debug.print("Not authorized to stop rebalancing: " # debug_show(caller));
      logTreasuryAdminAction(caller, #StopRebalancing, "Unauthorized attempt", false, ?"Not authorized");
      return #err(#ConfigError("Not authorized"));
    };

    Debug.print("Stopping rebalancing");
    // Cancel existing timers
    switch (rebalanceState.priceUpdateTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };
    switch (rebalanceState.rebalanceTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };
    
    // Cancel portfolio snapshot timer
    if (portfolioSnapshotTimerId != 0) {
      cancelTimer(portfolioSnapshotTimerId);
      portfolioSnapshotTimerId := 0;
    };

    rebalanceState := {
      rebalanceState with
      status = #Idle;
      priceUpdateTimerId = null;
      rebalanceTimerId = null;
      metrics = {
        rebalanceState.metrics with
        currentStatus = #Idle;
      };
    };

    Debug.print("Rebalancing stopped" # debug_show(rebalanceState));
    let reasonText = switch (reason) {
      case (?r) r;
      case null "Rebalancing stopped programmatically";
    };
    logTreasuryAdminAction(caller, #StopRebalancing, reasonText, true, null);
    #ok("Rebalancing stopped");
  };

  /**
   * Reset the rebalancing state to initial values
   *
   * Completely resets all metrics, trade history, and timers
   * Only callable by DAO or controller.
   */
  public shared ({ caller }) func resetRebalanceState(reason : ?Text) : async Result.Result<Text, RebalanceError> {
    if (((await hasAdminPermission(caller, #stopRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      Debug.print("Not authorized to reset rebalance state: " # debug_show(caller));
      logTreasuryAdminAction(caller, #ResetRebalanceState, "Unauthorized attempt", false, ?"Not authorized");
      return #err(#ConfigError("Not authorized"));
    };

    Debug.print("Resetting rebalance state");
    
    // Cancel existing timers
    switch (rebalanceState.priceUpdateTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };
    switch (rebalanceState.rebalanceTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };

    // Reset to initial state
    rebalanceState := {
      status = #Idle;
      config = rebalanceConfig;
      metrics = {
        lastPriceUpdate = 0;
        lastRebalanceAttempt = 0;
        totalTradesExecuted = 0;
        totalTradesFailed = 0;
        totalTradesSkipped = 0;
        skipBreakdown = {
          noPairsFound = 0;
          noExecutionPath = 0;
          tokensFiltered = 0;
          pausedTokens = 0;
          insufficientCandidates = 0;
        };
        currentStatus = #Idle;
        portfolioValueICP = 0;
        portfolioValueUSD = 0;
      };
      lastTrades = Vector.new<TradeRecord>();
      priceUpdateTimerId = null;
      rebalanceTimerId = null;
    };

    Debug.print("Rebalance state reset complete");
    let reasonText = switch (reason) {
      case (?r) r;
      case null "Rebalance state reset programmatically";
    };
    logTreasuryAdminAction(caller, #ResetRebalanceState, reasonText, true, null);
    #ok("Rebalance state reset to initial values");
  };

  /**
   * Serialize RebalanceConfig to a structured, parseable text format
   * Format: field_name=value|field_name=value|...
   * This format is easy to parse and display in GUI with diffing capabilities
   */
  private func serializeRebalanceConfig(config : RebalanceConfig) : Text {
    "rebalanceIntervalNS=" # debug_show(config.rebalanceIntervalNS) #
    "|maxTradeAttemptsPerInterval=" # debug_show(config.maxTradeAttemptsPerInterval) #
    "|minTradeValueICP=" # debug_show(config.minTradeValueICP) #
    "|maxTradeValueICP=" # debug_show(config.maxTradeValueICP) #
    "|portfolioRebalancePeriodNS=" # debug_show(config.portfolioRebalancePeriodNS) #
    "|maxSlippageBasisPoints=" # debug_show(config.maxSlippageBasisPoints) #
    "|maxTradesStored=" # debug_show(config.maxTradesStored) #
    "|maxKongswapAttempts=" # debug_show(config.maxKongswapAttempts) #
    "|shortSyncIntervalNS=" # debug_show(config.shortSyncIntervalNS) #
    "|longSyncIntervalNS=" # debug_show(config.longSyncIntervalNS) #
    "|tokenSyncTimeoutNS=" # debug_show(config.tokenSyncTimeoutNS)
  };

  /**
   * Update the rebalancing configuration parameters
   *
   * Allows adjustment of trading intervals, sizes, and safety limits
   * Only callable by DAO or controller.
   */
  public shared ({ caller }) func updateRebalanceConfig(updates : UpdateConfig, rebalanceStateNew : ?Bool, reason : ?Text) : async Result.Result<Text, RebalanceError> {
    // Capture old configuration before any changes for audit trail
    let oldConfig = rebalanceConfig;
    let oldConfigText = serializeRebalanceConfig(oldConfig);

    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      logTreasuryAdminAction(caller, #UpdateRebalanceConfig({oldConfig = oldConfigText; newConfig = oldConfigText}), "Unauthorized configuration update attempt", false, ?"Not authorized");
      return #err(#ConfigError("Not authorized"));
    };

    // Get current configuration as the base
    var updatedConfig = rebalanceConfig;
    var hasChanges = false;
    var validationErrors = "";

    // Update and validate rebalance interval
    switch (updates.rebalanceIntervalNS) {
      case (?value) {
        if (value < 30_000_000_000) {
          // Min 30 seconds
          validationErrors #= "Rebalance interval must be at least 30 seconds; ";
        } else if (value > 86400_000_000_000) {
          // Max 24 hours
          validationErrors #= "Rebalance interval must be at most 24 hours; ";
        } else {
          updatedConfig := {
            updatedConfig with
            rebalanceIntervalNS = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate token sync timeout
    switch (updates.tokenSyncTimeoutNS) {
      case (?value) {
        if (value < 1800_000_000_000) {
          validationErrors #= "Token sync timeout must be at least 30 minutes; ";
        } else if (value > 86400_000_000_000) {
          validationErrors #= "Token sync timeout must be at most 24 hours; ";
        } else {
          updatedConfig := {
            updatedConfig with
            tokenSyncTimeoutNS = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate max trade attempts
    switch (updates.maxTradeAttemptsPerInterval) {
      case (?value) {
        if (value < 1) {
          validationErrors #= "Max trade attempts must be at least 1; ";
        } else if (value > 20) {
          validationErrors #= "Max trade attempts must be at most 20; ";
        } else {
          updatedConfig := {
            updatedConfig with
            maxTradeAttemptsPerInterval = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate min trade value
    switch (updates.minTradeValueICP) {
      case (?value) {
        //let minAllowed = 10_000_000; // 0.1 ICP
        //let maxAllowed = 1_000_000_000; // 10 ICP
        let minAllowed = 100_000; // 0.001 ICP
        let maxAllowed = 1_000_000_000; // 10 ICP

        if (value < minAllowed) {
          validationErrors #= "Minimum trade value cannot be less than 0.1 ICP; ";
        } else if (value > maxAllowed) {
          validationErrors #= "Minimum trade value cannot be more than 10 ICP; ";
        } else if (value >= updatedConfig.maxTradeValueICP) {
          validationErrors #= "Minimum trade value must be less than maximum trade value; ";
        } else {
          updatedConfig := {
            updatedConfig with
            minTradeValueICP = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate max trade value
    switch (updates.maxTradeValueICP) {
      case (?value) {
        //let minAllowed = 500_000_000; // 5 ICP
        //let maxAllowed = 10_000_000_000; // 100 ICP
        let minAllowed = 500_000; // 0.005 ICP
        let maxAllowed = 10_000_000_000; // 100 ICP

        if (value < minAllowed) {
          validationErrors #= "Maximum trade value cannot be less than 1 ICP; ";
        } else if (value > maxAllowed) {
          validationErrors #= "Maximum trade value cannot be more than 100 ICP; ";
        } else if (value <= updatedConfig.minTradeValueICP) {
          validationErrors #= "Maximum trade value must be greater than minimum trade value; ";
        } else {
          updatedConfig := {
            updatedConfig with
            maxTradeValueICP = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate portfolio rebalance period
    switch (updates.portfolioRebalancePeriodNS) {
      case (?value) {
        let minAllowed = 86400_000_000_000; // 1 day
        let maxAllowed = 2_592_000_000_000_000; // 30 days

        if (value < minAllowed) {
          validationErrors #= "Portfolio rebalance period cannot be less than 1 day; ";
        } else if (value > maxAllowed) {
          validationErrors #= "Portfolio rebalance period cannot be more than 30 days; ";
        } else {
          updatedConfig := {
            updatedConfig with
            portfolioRebalancePeriodNS = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate max price history entries
    switch (updates.maxPriceHistoryEntries) {
      case (?value) {
        if (value < 10) {
          validationErrors #= "Maximum price history entries cannot be less than 10; ";
        } else if (value > 1000000) {
          validationErrors #= "Maximum price history entries cannot be more than 1,000,000; ";
        } else {
          MAX_PRICE_HISTORY_ENTRIES := value;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate max slippage
    switch (updates.maxSlippageBasisPoints) {
      case (?value) {
        if (value < 35) {
          // Min 0.35%
          validationErrors #= "Maximum slippage cannot be less than 0.35%; ";
        } else if (value > 500) {
          // Max 5%
          validationErrors #= "Maximum slippage cannot be more than 10%; ";
        } else {
          updatedConfig := {
            updatedConfig with
            maxSlippageBasisPoints = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate max trades stored
    switch (updates.maxTradesStored) {
      case (?value) {
        if (value < 10) {
          validationErrors #= "Maximum trades stored cannot be less than 10; ";
        } else if (value > 4000) {
          validationErrors #= "Maximum trades stored cannot be more than 4000; ";
        } else {
          updatedConfig := {
            updatedConfig with
            maxTradesStored = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Update and validate max Kongswap attempts
    switch (updates.maxKongswapAttempts) {
      case (?value) {
        if (value < 1) {
          validationErrors #= "Maximum Kongswap attempts cannot be less than 1; ";
        } else if (value > 10) {
          validationErrors #= "Maximum Kongswap attempts cannot be more than 10; ";
        } else {
          updatedConfig := {
            updatedConfig with
            maxKongswapAttempts = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Validate short sync interval
    switch (updates.shortSyncIntervalNS) {
      case (?value) {
        let minAllowed = 300_000_000_000; // 5 minutes
        let maxAllowed = 3600_000_000_000; // 1 hour

        if (value < minAllowed) {
          validationErrors #= "Short sync interval cannot be less than 5 minutes; ";
        } else if (value > maxAllowed) {
          validationErrors #= "Short sync interval cannot be more than 1 hour; ";
        } else {
          updatedConfig := {
            updatedConfig with
            shortSyncIntervalNS = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Validate long sync interval
    switch (updates.longSyncIntervalNS) {
      case (?value) {
        let minAllowed = 1800_000_000_000; // 30 minutes
        let maxAllowed = 86400_000_000_000; // 24 hours

        if (value < minAllowed) {
          validationErrors #= "Long sync interval cannot be less than 30 minutes; ";
        } else if (value > maxAllowed) {
          validationErrors #= "Long sync interval cannot be more than 24 hours; ";
        } else {
          updatedConfig := {
            updatedConfig with
            longSyncIntervalNS = value
          };
          hasChanges := true;
        };
      };
      case null {}; // Keep existing value
    };

    // Return any validation errors
    if (Text.size(validationErrors) > 0) {
      logTreasuryAdminAction(caller, #UpdateRebalanceConfig({oldConfig = oldConfigText; newConfig = oldConfigText}), "Configuration validation failed", false, ?validationErrors);
      return #err(#ConfigError(validationErrors));
    };

    // If no changes were requested, return early
    if (not hasChanges) {
      logTreasuryAdminAction(caller, #UpdateRebalanceConfig({oldConfig = oldConfigText; newConfig = oldConfigText}), "No configuration changes requested", true, null);
      return #ok("No changes requested to rebalance configuration");
    };

    // Update configuration
    rebalanceConfig := updatedConfig;
    rebalanceState := {
      rebalanceState with
      config = updatedConfig
    };

    // Reset timers if they exist
    switch (rebalanceState.priceUpdateTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };
    switch (rebalanceState.rebalanceTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };

    // Handle rebalance state changes, if it fails it can be retried
    try {
      switch (rebalanceStateNew) {
        case (?value) {
          if (value and rebalanceState.status == #Idle) {
            ignore await startRebalancing(?"Auto-start via config update");
          } else if (not value and rebalanceState.status != #Idle) {
            ignore await stopRebalancing(?"Auto-stop via config update");
          };
        };
        case null {};
      };
    } catch (_) {

    };

    // Serialize new configuration for audit trail (old config was captured at the beginning)
    let newConfigText = serializeRebalanceConfig(updatedConfig);
    
    let reasonText = switch (reason) {
      case (?r) r;
      case null "Configuration updated";
    };
    logTreasuryAdminAction(caller, #UpdateRebalanceConfig({oldConfig = oldConfigText; newConfig = newConfigText}), reasonText, true, null);
    #ok("Rebalance configuration updated successfully");
  };

  /**
 * Get system rebalance parameters
 *
 * Returns all rebalancing configuration parameters that control the behavior
 * of the Treasury including trading intervals, size limits, slippage tolerance, etc.
 *
 * Accessible by any user with query access.
 */
  public query func getSystemParameters() : async RebalanceConfig {
    rebalanceConfig;
  };

  /**
   * Get detailed rebalancing status information
   *
   * Returns:
   * - Current system status
   * - Recent trade history
   * - Portfolio valuation
   * - Current vs target allocations
   * - Performance metrics
   */
  public shared query func getTradingStatus() : async Result.Result<{ rebalanceStatus : RebalanceStatus; executedTrades : [TradeRecord]; portfolioState : { totalValueICP : Nat; totalValueUSD : Float; currentAllocations : [(Principal, Nat)]; targetAllocations : [(Principal, Nat)] }; metrics : { lastUpdate : Int; lastRebalanceAttempt : Int; totalTradesExecuted : Nat; totalTradesFailed : Nat; totalTradesSkipped : Nat; skipBreakdown : { noPairsFound : Nat; noExecutionPath : Nat; tokensFiltered : Nat; pausedTokens : Nat; insufficientCandidates : Nat }; avgSlippage : Float; successRate : Float; skipRate : Float } }, Text> {

    // Calculate total portfolio value
    var totalValueICP = 0;
    var totalValueUSD : Float = 0;
    let currentAllocs = Vector.new<(Principal, Nat)>();

    // First pass - calculate totals
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
        let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        totalValueICP += valueInICP;
        totalValueUSD += details.priceInUSD * Float.fromInt(details.balance) / (10.0 ** Float.fromInt(details.tokenDecimals));
    };

    // Second pass - calculate allocations
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
        let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        if (valueInICP > 0) {
            let basisPoints = (valueInICP * 10000) / totalValueICP;
            Vector.add(currentAllocs, (principal, basisPoints));
        };
    };


    // Calculate metrics from trade history
    var totalSlippage : Float = 0;
    var impactCount = 0;
    for (trade in Vector.vals(rebalanceState.lastTrades)) {
      if (trade.success) {
        totalSlippage += trade.slippage;
        impactCount += 1;
      };
    };

    let avgSlippage = if (impactCount > 0) {
      totalSlippage / Float.fromInt(impactCount);
    } else { 0.0 };

    let totalAttempts = rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed + rebalanceState.metrics.totalTradesSkipped;
    
    let successRate = if (rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed > 0) {
      Float.fromInt(rebalanceState.metrics.totalTradesExecuted) / Float.fromInt(rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed);
    } else { 0.0 };

    let skipRate = if (totalAttempts > 0) {
      Float.fromInt(rebalanceState.metrics.totalTradesSkipped) / Float.fromInt(totalAttempts);
    } else { 0.0 };

    #ok({
      rebalanceStatus = rebalanceState.status;
      executedTrades = Vector.toArray(rebalanceState.lastTrades);
      portfolioState = {
        totalValueICP = totalValueICP;
        totalValueUSD = totalValueUSD;
        currentAllocations = Vector.toArray(currentAllocs);
        targetAllocations = Iter.toArray(Map.entries(currentAllocations));
      };
      metrics = {
        lastUpdate = rebalanceState.metrics.lastPriceUpdate;
        lastRebalanceAttempt = rebalanceState.metrics.lastRebalanceAttempt;
        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted;
        totalTradesFailed = rebalanceState.metrics.totalTradesFailed;
        totalTradesSkipped = rebalanceState.metrics.totalTradesSkipped;
        skipBreakdown = {
          noPairsFound = rebalanceState.metrics.skipBreakdown.noPairsFound;
          noExecutionPath = rebalanceState.metrics.skipBreakdown.noExecutionPath;
          tokensFiltered = rebalanceState.metrics.skipBreakdown.tokensFiltered;
          pausedTokens = rebalanceState.metrics.skipBreakdown.pausedTokens;
          insufficientCandidates = rebalanceState.metrics.skipBreakdown.insufficientCandidates;
        };
        avgSlippage = avgSlippage;
        successRate = successRate;
        skipRate = skipRate;
      };
    });
  };

  /**
   * Get current token allocations in basis points
   */
  public query func getCurrentAllocations() : async [(Principal, Nat)] {
    Iter.toArray(Map.entries(currentAllocations));
  };

  /**
   * Get skip metrics and breakdown
   * 
   * Returns detailed information about skipped trades including:
   * - Total skipped trades
   * - Breakdown by skip reason
   * - Skip rate as percentage of all attempts
   */
  public query func getSkipMetrics() : async { totalTradesSkipped : Nat; skipBreakdown : { noPairsFound : Nat; noExecutionPath : Nat; tokensFiltered : Nat; pausedTokens : Nat; insufficientCandidates : Nat }; skipRate : Float } {
    let totalAttempts = rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed + rebalanceState.metrics.totalTradesSkipped;
    
    let skipRate = if (totalAttempts > 0) {
      Float.fromInt(rebalanceState.metrics.totalTradesSkipped) / Float.fromInt(totalAttempts);
    } else { 0.0 };

    {
      totalTradesSkipped = rebalanceState.metrics.totalTradesSkipped;
      skipBreakdown = {
        noPairsFound = rebalanceState.metrics.skipBreakdown.noPairsFound;
        noExecutionPath = rebalanceState.metrics.skipBreakdown.noExecutionPath;
        tokensFiltered = rebalanceState.metrics.skipBreakdown.tokensFiltered;
        pausedTokens = rebalanceState.metrics.skipBreakdown.pausedTokens;
        insufficientCandidates = rebalanceState.metrics.skipBreakdown.insufficientCandidates;
      };
      skipRate = skipRate;
    };
  };

  /**
   * Get all token details including balances and prices
   */
  public query func getTokenDetails() : async [(Principal, TokenDetails)] {
    Iter.toArray(Map.entries(tokenDetailsMap));
  };

  //=========================================================================
  // EFFICIENT TIMESTAMP-FILTERED METHODS FOR ARCHIVES
  //=========================================================================

  /**
   * Get trading status with trades filtered by timestamp (for archive efficiency)
   * Returns only trades newer than the specified timestamp
   */
  public shared query func getTradingStatusSince(sinceTimestamp: Int) : async Result.Result<{ rebalanceStatus : RebalanceStatus; executedTrades : [TradeRecord]; portfolioState : { totalValueICP : Nat; totalValueUSD : Float; currentAllocations : [(Principal, Nat)]; targetAllocations : [(Principal, Nat)] }; metrics : { lastUpdate : Int; totalTradesExecuted : Nat; totalTradesFailed : Nat; totalTradesSkipped : Nat; skipBreakdown : { noPairsFound : Nat; noExecutionPath : Nat; tokensFiltered : Nat; pausedTokens : Nat; insufficientCandidates : Nat }; avgSlippage : Float; successRate : Float; skipRate : Float } }, Text> {
    
    // Filter trades by timestamp - only return trades newer than sinceTimestamp
    let filteredTrades = Vector.new<TradeRecord>();
    for (trade in Vector.vals(rebalanceState.lastTrades)) {
      if (trade.timestamp > sinceTimestamp) {
        Vector.add(filteredTrades, trade);
      };
    };

    // Calculate total portfolio value (same as original method)
    var totalValueICP = 0;
    var totalValueUSD : Float = 0;
    let currentAllocs = Vector.new<(Principal, Nat)>();

    // First pass - calculate totals
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
        let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        totalValueICP += valueInICP;
        totalValueUSD += details.priceInUSD * Float.fromInt(details.balance) / (10.0 ** Float.fromInt(details.tokenDecimals));
    };

    // Second pass - calculate allocations
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
        let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        if (valueInICP > 0) {
            let basisPoints = (valueInICP * 10000) / totalValueICP;
            Vector.add(currentAllocs, (principal, basisPoints));
        };
    };

    // Calculate metrics from filtered trade history only
    var totalSlippage : Float = 0;
    var impactCount = 0;  
    for (trade in Vector.vals(filteredTrades)) {
      if (trade.success) {
        totalSlippage += trade.slippage;
        impactCount += 1;
      };
    };

    let avgSlippage = if (impactCount > 0) {
      totalSlippage / Float.fromInt(impactCount);
    } else { 0.0 };

    let totalAttempts = rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed + rebalanceState.metrics.totalTradesSkipped;
    
    let successRate = if (rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed > 0) {
      Float.fromInt(rebalanceState.metrics.totalTradesExecuted) / Float.fromInt(rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed);
    } else { 0.0 };

    let skipRate = if (totalAttempts > 0) {
      Float.fromInt(rebalanceState.metrics.totalTradesSkipped) / Float.fromInt(totalAttempts);
    } else { 0.0 };

    #ok({
      rebalanceStatus = rebalanceState.status;
      executedTrades = Vector.toArray(filteredTrades); // Return only filtered trades
      portfolioState = {
        totalValueICP = totalValueICP;
        totalValueUSD = totalValueUSD;
        currentAllocations = Vector.toArray(currentAllocs);
        targetAllocations = Iter.toArray(Map.entries(currentAllocations));
      };
      metrics = {
        lastUpdate = rebalanceState.metrics.lastPriceUpdate;
        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted;
        totalTradesFailed = rebalanceState.metrics.totalTradesFailed;
        totalTradesSkipped = rebalanceState.metrics.totalTradesSkipped;
        skipBreakdown = {
          noPairsFound = rebalanceState.metrics.skipBreakdown.noPairsFound;
          noExecutionPath = rebalanceState.metrics.skipBreakdown.noExecutionPath;
          tokensFiltered = rebalanceState.metrics.skipBreakdown.tokensFiltered;
          pausedTokens = rebalanceState.metrics.skipBreakdown.pausedTokens;
          insufficientCandidates = rebalanceState.metrics.skipBreakdown.insufficientCandidates;
        };
        avgSlippage = avgSlippage;
        successRate = successRate;
        skipRate = skipRate;
      };
    });
  };

  /**
   * Get portfolio history filtered by timestamp (for archive efficiency)
   * Returns only snapshots newer than the specified timestamp
   */
  public shared query ({ caller }) func getPortfolioHistorySince(sinceTimestamp: Int, limit: Nat) : async Result.Result<PortfolioHistoryResponse, PortfolioSnapshotError> {
    // Allow any authenticated caller (not anonymous)
    if (Principal.isAnonymous(caller)) {
      return #err(#NotAuthorized);
    };

    if (limit == 0 or limit > 2000) {
      return #err(#InvalidLimit);
    };

    // Filter snapshots by timestamp first
    let allSnapshots = Vector.toArray(portfolioSnapshots);
    let filteredSnapshots = Array.filter<PortfolioSnapshot>(allSnapshots, func(snapshot) {
      snapshot.timestamp > sinceTimestamp
    });
    
    let totalFilteredCount = filteredSnapshots.size();
    
    // Apply limit to filtered results (get most recent ones)
    let limitedSnapshots = if (totalFilteredCount > limit) {
      let startIndex = totalFilteredCount - limit;
      Array.subArray(filteredSnapshots, startIndex, limit)
    } else {
      filteredSnapshots
    };

    let response : PortfolioHistoryResponse = {
      snapshots = limitedSnapshots;
      totalCount = totalFilteredCount;
    };

    #ok(response);
  };

  /**
   * Get token details with price history filtered by timestamp (for archive efficiency)
   * Returns only price points newer than the specified timestamp per token
   */
  public query func getTokenDetailsSince(sinceTimestamp: Int) : async [(Principal, TokenDetails)] {
    let result = Vector.new<(Principal, TokenDetails)>();
    
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      // Filter pastPrices by timestamp
      let filteredPrices = Array.filter<PricePoint>(details.pastPrices, func(pricePoint) {
        pricePoint.time > sinceTimestamp
      });
      
      // Create new TokenDetails with filtered price history
      let filteredDetails : TokenDetails = {
        Active = details.Active;
        isPaused = details.isPaused;
        epochAdded = details.epochAdded;
        tokenName = details.tokenName;
        tokenSymbol = details.tokenSymbol;
        tokenDecimals = details.tokenDecimals;
        tokenTransferFee = details.tokenTransferFee;
        balance = details.balance;
        priceInICP = details.priceInICP;
        priceInUSD = details.priceInUSD;
        tokenType = details.tokenType;
        pastPrices = filteredPrices; // Only include prices newer than sinceTimestamp
        lastTimeSynced = details.lastTimeSynced;
        pausedDueToSyncFailure = details.pausedDueToSyncFailure;
      };
      
      Vector.add(result, (principal, filteredDetails));
    };

    Vector.toArray(result);
  };

  public query func getTokenPriceHistory(tokens : [Principal]) : async Result.Result<[(Principal, [PricePoint])], Text> {

    let result = Vector.new<(Principal, [PricePoint])>();

    for (token in tokens.vals()) {
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case (?details) {
          Vector.add(result, (token, details.pastPrices));
        };
        case null {};
      };
    };

    #ok(Vector.toArray(result));
  };

  /**
   * Set test mode (modifies safety parameters)
   * Only callable by DAO.
   */
  public shared ({ caller }) func setTest(a : Bool) : async () {
    if (caller != DAOPrincipal) {
      logTreasuryAdminAction(caller, #SetTestMode({isTestMode = a}), "Unauthorized attempt", false, ?"Only DAO can set test mode");
      assert (false); // Keep original assert behavior
    };
    test := a;
    Debug.print("Test is set");
    logTreasuryAdminAction(caller, #SetTestMode({isTestMode = a}), "Test mode updated", true, null);
  };

  //=========================================================================
  // PRICE FAILSAFE SYSTEM - PUBLIC INTERFACE
  //=========================================================================

  /**
   * Add a new price trigger condition
   *
   * Creates a failsafe rule that will pause tokens when price movements
   * exceed the specified threshold within the time window.
   *
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func addTriggerCondition(
    name : Text,
    direction : PriceDirection,
    percentage : Float,
    timeWindowNS : Nat,
    applicableTokens : [Principal]
  ) : async Result.Result<Nat, PriceFailsafeError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    // Validate inputs
    if (percentage <= 0.0 or percentage > 1000.0) {
      return #err(#InvalidPercentage);
    };

    if (timeWindowNS < 60_000_000_000 or timeWindowNS > (86400_000_000_000 * 7)) { // 1 minute to 7 * 24 hours (1 week)
      return #err(#InvalidTimeWindow);
    };

    // Check for duplicate names
    for ((_, condition) in Map.entries(triggerConditions)) {
      if (condition.name == name) {
        return #err(#DuplicateName);
      };
    };

    // Validate applicable tokens exist in our system
    for (token in applicableTokens.vals()) {
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case null {
          return #err(#InvalidTokenList);
        };
        case (?_) {};
      };
    };

    let conditionId = nextConditionId;
    nextConditionId += 1;

    let newCondition : TriggerCondition = {
      id = conditionId;
      name = name;
      direction = direction;
      percentage = percentage;
      timeWindowNS = timeWindowNS;
      applicableTokens = applicableTokens;
      isActive = true;
      createdAt = now();
      createdBy = caller;
    };

    Map.set(triggerConditions, Map.nhash, conditionId, newCondition);

    logger.info(
      "PRICE_FAILSAFE", 
      "Trigger condition added - ID=" # Nat.toText(conditionId) #
      " Name=" # name #
      " Direction=" # debug_show(direction) #
      " Percentage=" # Float.toText(percentage) # "%" #
      " TimeWindow=" # Nat.toText(timeWindowNS / 1_000_000_000) # "s" #
      " ApplicableTokens=" # Nat.toText(applicableTokens.size()),
      " CreatedBy=" # Principal.toText(caller)
    );

    #ok(conditionId);
  };

  /**
   * Update an existing trigger condition
   *
   * Modifies parameters of an existing failsafe rule.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func updateTriggerCondition(
    conditionId : Nat,
    updates : TriggerConditionUpdate
  ) : async Result.Result<Text, PriceFailsafeError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    switch (Map.get(triggerConditions, Map.nhash, conditionId)) {
      case null {
        return #err(#ConditionNotFound);
      };
      case (?currentCondition) {
        var updatedCondition = currentCondition;

        // Apply updates
        switch (updates.name) {
          case (?newName) {
            // Check for duplicate names (excluding current condition)
            for ((id, condition) in Map.entries(triggerConditions)) {
              if (id != conditionId and condition.name == newName) {
                return #err(#DuplicateName);
              };
            };
            updatedCondition := { updatedCondition with name = newName };
          };
          case null {};
        };

        switch (updates.direction) {
          case (?newDirection) {
            updatedCondition := { updatedCondition with direction = newDirection };
          };
          case null {};
        };

        switch (updates.percentage) {
          case (?newPercentage) {
            if (newPercentage <= 0.0 or newPercentage > 1000.0) {
              return #err(#InvalidPercentage);
            };
            updatedCondition := { updatedCondition with percentage = newPercentage };
          };
          case null {};
        };

        switch (updates.timeWindowNS) {
          case (?newTimeWindow) {
            if (newTimeWindow < 60_000_000_000 or newTimeWindow > 86400_000_000_000) {
              return #err(#InvalidTimeWindow);
            };
            updatedCondition := { updatedCondition with timeWindowNS = newTimeWindow };
          };
          case null {};
        };

        switch (updates.applicableTokens) {
          case (?newTokens) {
            // Validate applicable tokens exist in our system
            for (token in newTokens.vals()) {
              switch (Map.get(tokenDetailsMap, phash, token)) {
                case null {
                  return #err(#InvalidTokenList);
                };
                case (?_) {};
              };
            };
            updatedCondition := { updatedCondition with applicableTokens = newTokens };
          };
          case null {};
        };

        switch (updates.isActive) {
          case (?newActive) {
            updatedCondition := { updatedCondition with isActive = newActive };
          };
          case null {};
        };

        Map.set(triggerConditions, Map.nhash, conditionId, updatedCondition);

        Debug.print("PRICE_FAILSAFE: " # 
          "Trigger condition updated - ID=" # Nat.toText(conditionId) #
          " Name=" # updatedCondition.name #
          " UpdatedBy=" # Principal.toText(caller)
        );

        #ok("Trigger condition updated successfully");
      };
    };
  };

  /**
   * Remove a trigger condition
   *
   * Deletes a failsafe rule permanently.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func removeTriggerCondition(conditionId : Nat) : async Result.Result<Text, PriceFailsafeError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    switch (Map.remove(triggerConditions, Map.nhash, conditionId)) {
      case null {
        return #err(#ConditionNotFound);
      };
      case (?removedCondition) {
        Debug.print("PRICE_FAILSAFE: " # 
          "Trigger condition removed - ID=" # Nat.toText(conditionId) #
          " Name=" # removedCondition.name #
          " RemovedBy=" # Principal.toText(caller)
        );

        #ok("Trigger condition removed successfully");
      };
    };
  };

  /**
   * List all trigger conditions
   *
   * Returns all configured failsafe rules.
   * Accessible by any user with query access.
   */
  public query func listTriggerConditions() : async [TriggerCondition] {
    Iter.toArray(Map.vals(triggerConditions));
  };

  /**
   * Get a specific trigger condition by ID
   *
   * Returns details of a single failsafe rule.
   * Accessible by any user with query access.
   */
  public query func getTriggerCondition(conditionId : Nat) : async ?TriggerCondition {
    Map.get(triggerConditions, Map.nhash, conditionId);
  };

  /**
   * Activate or deactivate a trigger condition
   *
   * Enables or disables a failsafe rule without deleting it.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func setTriggerConditionActive(
    conditionId : Nat,
    isActive : Bool
  ) : async Result.Result<Text, PriceFailsafeError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    switch (Map.get(triggerConditions, Map.nhash, conditionId)) {
      case null {
        return #err(#ConditionNotFound);
      };
      case (?condition) {
        let updatedCondition = { condition with isActive = isActive };
        Map.set(triggerConditions, Map.nhash, conditionId, updatedCondition);

        Debug.print("PRICE_FAILSAFE: " # 
          "Trigger condition " # (if isActive "activated" else "deactivated") # 
          " - ID=" # Nat.toText(conditionId) #
          " Name=" # condition.name #
          " UpdatedBy=" # Principal.toText(caller)
        );

        #ok("Trigger condition " # (if isActive "activated" else "deactivated") # " successfully");
      };
    };
  };

  /**
   * Get price alerts (paginated)
   *
   * Returns recent price alert events that triggered token pausing.
   * Accessible by any user with query access.
   */
  public query func getPriceAlerts(offset : Nat, limit : Nat) : async {
    alerts : [PriceAlertLog];
    totalCount : Nat;
  } {
    let totalCount = Vector.size(priceAlerts);
    let alertsArray = Vector.toArray(priceAlerts);
    
    // Reverse the array to show most recent first
    let reversedAlerts = Array.reverse(alertsArray);
    
    let actualLimit = if (limit > 100) { 100 } else { limit }; // Cap at 100
    let endIndex = if (offset + actualLimit > reversedAlerts.size()) {
      reversedAlerts.size();
    } else {
      offset + actualLimit;
    };
    
    if (offset >= reversedAlerts.size()) {
      return { alerts = []; totalCount = totalCount };
    };
    
    let slicedAlerts = Iter.toArray(Array.slice(reversedAlerts, offset, endIndex));
    
    {
      alerts = slicedAlerts;
      totalCount = totalCount;
    };
  };

  /**
   * Get price alerts for a specific token
   *
   * Returns price alert events for a particular token.
   * Accessible by any user with query access.
   */
  public query func getPriceAlertsForToken(token : Principal, limit : Nat) : async [PriceAlertLog] {
    let alertsArray = Vector.toArray(priceAlerts);
    let reversedAlerts = Array.reverse(alertsArray);
    
    let filteredAlerts = Array.filter<PriceAlertLog>(reversedAlerts, func(alert) {
      alert.token == token
    });
    
    let actualLimit = if (limit > 100) { 100 } else { limit };
    Iter.toArray(Array.slice(filteredAlerts, 0, if (actualLimit > filteredAlerts.size()) { filteredAlerts.size() } else { actualLimit }));
  };

  /**
   * Clear price alerts log
   *
   * Removes all price alert history.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func clearPriceAlerts() : async Result.Result<Text, PriceFailsafeError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    let clearedCount = Vector.size(priceAlerts);
    Vector.clear(priceAlerts);
    nextAlertId := 1;

    Debug.print("PRICE_FAILSAFE: " # 
      "Price alerts cleared - Count=" # Nat.toText(clearedCount) #
      " ClearedBy=" # Principal.toText(caller)
    );

    #ok("Cleared " # Nat.toText(clearedCount) # " price alerts");
  };

  //=========================================================================
  // PRICE FAILSAFE SYSTEM - CORE MONITORING
  //=========================================================================

  /**
   * Check all active trigger conditions for a token
   *
   * Analyzes price history to detect significant price movements
   * and triggers token pausing if conditions are met.
   */
  private func checkPriceFailsafeConditions(token : Principal, currentPrice : Nat, priceHistory : [PricePoint]) {
    // Optimization: Skip price alert checks if token is already paused
    if (isTokenPausedFromTrading(token)) {
      // Get token symbol for logging
      let tokenSymbol = switch (Map.get(tokenDetailsMap, phash, token)) {
        case (?details) { details.tokenSymbol };
        case null { Principal.toText(token) };
      };
      
      //logger.info("PRICE_ALERT", 
      //  "Skipping price alert checks - Token already paused: " # tokenSymbol,
      //  "checkPriceFailsafeConditions"
      //);
      return;
    };
    
    // Get token symbol for logging
    let tokenSymbol = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) { details.tokenSymbol };
      case null { Principal.toText(token) };
    };

    // Check all active trigger conditions
    for ((conditionId, condition) in Map.entries(triggerConditions)) {
      if (condition.isActive and isTokenApplicable(token, condition)) {
                 switch (analyzePriceChangeInWindow(currentPrice, priceHistory, condition)) {
          case (?triggerData) {
            // Condition triggered - pause the token and log the alert
            pauseTokenDueToPriceAlert(token, condition, triggerData);
          };
          case null {
            // No trigger - continue monitoring
          };
        };
      };
    };
  };

  /**
   * Check if a token is applicable for a given trigger condition
   */
  private func isTokenApplicable(token : Principal, condition : TriggerCondition) : Bool {
    if (condition.applicableTokens.size() == 0) {
      return true; // Empty list means applies to all tokens
    };
    
    for (applicableToken in condition.applicableTokens.vals()) {
      if (applicableToken == token) {
        return true;
      };
    };
    
    false;
  };

  /**
   * Analyze price changes within the specified time window
   *
   * Returns trigger data if the condition is met, null otherwise
   */
  private func analyzePriceChangeInWindow(
    currentPrice : Nat,
    priceHistory : [PricePoint],
    condition : TriggerCondition
  ) : ?TriggerPriceData {
    let currentTime = now();
    let windowStartTime = currentTime - condition.timeWindowNS;
    
    // Filter price points within the time window
    let relevantPrices = Array.filter<PricePoint>(priceHistory, func(point) {
      point.time >= windowStartTime
    });
    
    // Need at least 2 price points to detect changes
    if (relevantPrices.size() < 2) {
      return null;
    };
    
    // Create array of all price points with timestamps (including current)
    let allPrices = Array.append(relevantPrices, [{
      icpPrice = currentPrice;
      usdPrice = 0.0; // Not used in this analysis
      time = currentTime;
    }]);
    
    // Sort by timestamp (oldest first)
    let sortedPrices = Array.sort(allPrices, func(a: PricePoint, b: PricePoint) : { #less; #equal; #greater } {
      if (a.time < b.time) { #less }
      else if (a.time > b.time) { #greater }
      else { #equal }
    });
    
    switch (condition.direction) {
      case (#Down) {
        // For DROP detection: Find min first, then find max in range [window_start, min_time]
        
        // Find minimum price and its time
        var minPrice = currentPrice;
        var minPriceTime = currentTime;
        
        for (point in sortedPrices.vals()) {
          if (point.icpPrice < minPrice) {
            minPrice := point.icpPrice;
            minPriceTime := point.time;
          };
        };
        
        // Find maximum price in the range [window_start, min_time]
        var maxPriceBeforeMin = minPrice; // Default to min if no earlier values
        
        for (point in sortedPrices.vals()) {
          if (point.time <= minPriceTime) {
            if (point.icpPrice > maxPriceBeforeMin) {
              maxPriceBeforeMin := point.icpPrice;
            };
          };
        };
        
        // Calculate drop percentage: (max_before_min - min) / max_before_min
        if (maxPriceBeforeMin > minPrice) {
          let dropPercentage = calculatePercentageChange(maxPriceBeforeMin, minPrice);
          
          if (Float.abs(dropPercentage) >= condition.percentage) {
            return ?{
              currentPrice = currentPrice;
              minPriceInWindow = minPrice;
              maxPriceInWindow = maxPriceBeforeMin;
              windowStartTime = windowStartTime;
              actualChangePercent = Float.abs(dropPercentage);
              changeType = #MinToMax;
            };
          };
        };
      };
      
      case (#Up) {
        // For CLIMB detection: Find max first, then find min in range [window_start, max_time]
        
        // Find maximum price and its time
        var maxPrice = currentPrice;
        var maxPriceTime = currentTime;
        
        for (point in sortedPrices.vals()) {
          if (point.icpPrice > maxPrice) {
            maxPrice := point.icpPrice;
            maxPriceTime := point.time;
          };
        };
        
        // Find minimum price in the range [window_start, max_time]
        var minPriceBeforeMax = maxPrice; // Default to max if no earlier values
        
        for (point in sortedPrices.vals()) {
          if (point.time <= maxPriceTime) {
            if (point.icpPrice < minPriceBeforeMax) {
              minPriceBeforeMax := point.icpPrice;
            };
          };
        };
        
        // Calculate climb percentage: (max - min_before_max) / min_before_max
        if (minPriceBeforeMax > 0 and maxPrice > minPriceBeforeMax) {
          let climbPercentage = calculatePercentageChange(minPriceBeforeMax, maxPrice);
          
          if (Float.abs(climbPercentage) >= condition.percentage) {
            return ?{
              currentPrice = currentPrice;
              minPriceInWindow = minPriceBeforeMax;
              maxPriceInWindow = maxPrice;
              windowStartTime = windowStartTime;
              actualChangePercent = Float.abs(climbPercentage);
              changeType = #MinToMax;
            };
          };
        };
      };
    };
    
    null;
  };

  /**
   * Calculate percentage change between two prices
   */
  private func calculatePercentageChange(fromPrice : Nat, toPrice : Nat) : Float {
    if (fromPrice == 0) {
      return 0.0;
    };
    
    let change = Float.fromInt(toPrice) - Float.fromInt(fromPrice);
    let percentage = (change / Float.fromInt(fromPrice)) * 100.0;
    percentage;
  };

  /**
   * Pause a token due to price alert and create log entry
   */
  private func pauseTokenDueToPriceAlert(
    token : Principal,
    condition : TriggerCondition,
    triggerData : TriggerPriceData
  ) {
    // Create price alert log entry
    let alertId = nextAlertId;
    nextAlertId += 1;
    
    switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) {
        let alert : PriceAlertLog = {
          id = alertId;
          timestamp = now();
          token = token;
          tokenSymbol = details.tokenSymbol;
          triggeredCondition = condition;
          priceData = triggerData;
        };
        
        // Add to alerts and maintain size limit
        Vector.add(priceAlerts, alert);
        if (Vector.size(priceAlerts) > maxPriceAlerts) {
          Vector.reverse(priceAlerts);
          while (Vector.size(priceAlerts) > maxPriceAlerts) {
            ignore Vector.removeLast(priceAlerts);
          };
          Vector.reverse(priceAlerts);
        };
        
        // Pause the token using the new trading pause system
        let pauseReason : TradingPauseReason = #PriceAlert({
          conditionName = condition.name;
          triggeredAt = now();
          alertId = alertId;
        });
        
        let paused = pauseTokenFromTrading(token, pauseReason);
        
        // Log the event
        logger.warn("PRICE_FAILSAFE_TRIGGER", 
          "PRICE ALERT TRIGGERED - Token=" # details.tokenSymbol # 
          " (" # Principal.toText(token) # ")" #
          " Condition=" # condition.name #
          " Direction=" # debug_show(condition.direction) #
          " Threshold=" # Float.toText(condition.percentage) # "%" #
          " Actual_Change=" # Float.toText(triggerData.actualChangePercent) # "%" #
          " Change_Type=" # debug_show(triggerData.changeType) #
          " Current_Price=" # Nat.toText(triggerData.currentPrice) #
          " Min_Price=" # Nat.toText(triggerData.minPriceInWindow) #
          " Max_Price=" # Nat.toText(triggerData.maxPriceInWindow) #
          " Window_Start=" # Int.toText((now() - triggerData.windowStartTime) / 1_000_000_000) # "s_ago" #
          " TOKEN_PAUSED=" # Bool.toText(paused) #
          " Alert_ID=" # Nat.toText(alertId),
          "pauseTokenDueToPriceAlert"
        );
        
        Debug.print("PRICE ALERT: Token " # details.tokenSymbol # " paused due to " # 
          Float.toText(triggerData.actualChangePercent) # "% " # debug_show(condition.direction) # 
          " price change (condition: " # condition.name # ")");
      };
      case null {
        Debug.print("Error: Token details not found when trying to pause due to price alert");
      };
    };
  };

  //=========================================================================
  // TRADING PAUSE SYSTEM - PUBLIC API
  //=========================================================================

  /**
   * List all tokens currently paused from trading
   *
   * Returns all tokens in the trading pause registry with their pause reasons.
   * Accessible by any user with query access.
   */
  public query func listTradingPauses() : async TradingPausesResponse {
    let pausedArray = Iter.toArray(Map.vals(tradingPauses));
    
    {
      pausedTokens = pausedArray;
      totalCount = pausedArray.size();
    };
  };

  /**
   * Get trading pause record for a specific token
   *
   * Returns the pause record if the token is paused from trading, null otherwise.
   * Accessible by any user with query access.
   */
  public query func getTradingPauseInfo(token : Principal) : async ?TradingPauseRecord {
    Map.get(tradingPauses, phash, token);
  };

  /**
   * Manually unpause a token from trading
   *
   * Removes a token from the trading pause registry, allowing it to trade again.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func unpauseTokenFromTrading(token : Principal, reason : ?Text) : async Result.Result<Text, TradingPauseError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      logTreasuryAdminAction(caller, #UnpauseToken({token}), "Unauthorized unpause attempt", false, ?"Not authorized");
      return #err(#NotAuthorized);
    };

    // Check if token is currently paused
    let pauseRecord = switch (Map.remove(tradingPauses, phash, token)) {
      case (?record) { record };
      case null { 
        logTreasuryAdminAction(caller, #UnpauseToken({token}), "Token was not paused", false, ?"Token not paused");
        return #err(#TokenNotPaused) 
      };
    };

    logger.info(
      "TRADING_PAUSE",
      "Token UNPAUSED from trading - Token=" # pauseRecord.tokenSymbol #
      " (" # Principal.toText(token) # ")" #
      " Original_reason=" # debug_show(pauseRecord.reason) #
      " UnpausedBy=" # Principal.toText(caller) #
      " Paused_duration=" # Int.toText((now() - pauseRecord.pausedAt) / 1_000_000_000) # "s",
      "unpauseTokenFromTrading"
    );

    let reasonText = switch (reason) {
      case (?r) r;
      case null "Token unpaused from trading";
    };
    logTreasuryAdminAction(caller, #UnpauseToken({token}), reasonText, true, null);
    #ok("Token " # pauseRecord.tokenSymbol # " unpaused from trading successfully");
  };

  /**
   * Manually pause a token from trading (for admin use)
   *
   * Allows admins to pause tokens from trading with a circuit breaker reason.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func pauseTokenFromTradingManual(
    token : Principal,
    reason : Text
  ) : async Result.Result<Text, TradingPauseError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      logTreasuryAdminAction(caller, #PauseTokenManual({token; pauseType = "manual"}), reason, false, ?"Not authorized");
      return #err(#NotAuthorized);
    };

    // Check if token exists in our system
    let tokenDetails = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) { details };
      case null { 
        logTreasuryAdminAction(caller, #PauseTokenManual({token; pauseType = "manual"}), reason, false, ?"Token not found");
        return #err(#TokenNotFound) 
      };
    };

    // Check if already paused
    switch (Map.get(tradingPauses, phash, token)) {
      case (?_) { 
        logTreasuryAdminAction(caller, #PauseTokenManual({token; pauseType = "manual"}), reason, false, ?"Token already paused");
        return #err(#TokenAlreadyPaused) 
      };
      case null {};
    };

    // Create pause record with circuit breaker reason
    let pauseReason : TradingPauseReason = #CircuitBreaker({
      reason = reason;
      triggeredAt = now();
      severity = "Manual";
    });

    let success = pauseTokenFromTrading(token, pauseReason);
    
    if (success) {
      logTreasuryAdminAction(caller, #PauseTokenManual({token; pauseType = "manual"}), reason, true, null);
      #ok("Token " # tokenDetails.tokenSymbol # " paused from trading successfully");
    } else {
      logTreasuryAdminAction(caller, #PauseTokenManual({token; pauseType = "manual"}), reason, false, ?"Failed to pause token");
      #err(#SystemError("Failed to pause token"));
    };
  };

  /**
   * Clear all trading pauses (emergency function)
   *
   * Removes all tokens from the trading pause registry.
   * Only callable by master admins.
   */
  public shared ({ caller }) func clearAllTradingPauses(reason : ?Text) : async Result.Result<Text, TradingPauseError> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller)) {
      logTreasuryAdminAction(caller, #ClearAllTradingPauses, "Unauthorized attempt", false, ?"Not authorized");
      return #err(#NotAuthorized);
    };

    let clearedCount = Map.size(tradingPauses);
    Map.clear(tradingPauses);

    logger.warn(
      "TRADING_PAUSE",
      "ALL TRADING PAUSES CLEARED - Count=" # Nat.toText(clearedCount) #
      " ClearedBy=" # Principal.toText(caller) #
      " Reason=Emergency_admin_action",
      "clearAllTradingPauses"
    );

    let reasonText = switch (reason) {
      case (?r) r;
      case null "Emergency clear of all trading pauses";
    };
    logTreasuryAdminAction(caller, #ClearAllTradingPauses, reasonText, true, null);
    #ok("Cleared " # Nat.toText(clearedCount) # " trading pauses");
  };

  //=========================================================================
  // PORTFOLIO SNAPSHOT SYSTEM
  //=========================================================================

  /**
   * Take a portfolio snapshot for the given reason
   * 
   * Captures current portfolio state including all token balances,
   * prices, and calculated values at the current moment.
   */
  private func takePortfolioSnapshot(reason : SnapshotReason) : async () {
    try {
      let timestamp = now();
      let tokenSnapshots = Vector.new<TokenSnapshot>();
      var totalValueICP : Nat = 0;
      var totalValueUSD : Float = 0.0;

      // Collect data for each active token
      for ((token, details) in Map.entries(tokenDetailsMap)) {
        if (details.Active and details.balance > 0) {
          let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
          let valueInUSD = (Float.fromInt(details.balance) * details.priceInUSD) / Float.fromInt(10 ** details.tokenDecimals);
          
          totalValueICP += valueInICP;
          totalValueUSD += valueInUSD;

          let tokenSnapshot : TokenSnapshot = {
            token = token;
            symbol = details.tokenSymbol;
            balance = details.balance;
            decimals = details.tokenDecimals;
            priceInICP = details.priceInICP;
            priceInUSD = details.priceInUSD;
            valueInICP = valueInICP;
            valueInUSD = valueInUSD;
          };
          
          Vector.add(tokenSnapshots, tokenSnapshot);
        };
      };

      // Create the portfolio snapshot
      let snapshot : PortfolioSnapshot = {
        timestamp = timestamp;
        tokens = Vector.toArray(tokenSnapshots);
        totalValueICP = totalValueICP;
        totalValueUSD = totalValueUSD;
        snapshotReason = reason;
      };

      // Add to storage and manage size limit
      Vector.add(portfolioSnapshots, snapshot);
      
      // Remove oldest snapshots if we exceed the limit
      if (Vector.size(portfolioSnapshots) > maxPortfolioSnapshots) {
        Vector.reverse(portfolioSnapshots);
        while (Vector.size(portfolioSnapshots) > maxPortfolioSnapshots) {
          ignore Vector.removeLast(portfolioSnapshots);
        };
        Vector.reverse(portfolioSnapshots);
      };

      lastPortfolioSnapshotTime := timestamp;

      // Log the snapshot (only for non-scheduled to avoid spam)
      switch (reason) {
        case (#Scheduled) { /* Skip logging for scheduled snapshots */ };
        case (_) {
          logger.info(
            "PORTFOLIO_SNAPSHOT",
            "Portfolio snapshot taken - Reason=" # debug_show(reason) #
            " Total_ICP=" # Nat.toText(totalValueICP / 100_000_000) # "." # 
            Nat.toText((totalValueICP % 100_000_000) / 1_000_000) #
            " Total_USD=" # Float.toText(totalValueUSD) #
            " Tokens=" # Nat.toText(Vector.size(tokenSnapshots)),
            "takePortfolioSnapshot"
          );
        };
      };

    } catch (e) {
      logger.error(
        "PORTFOLIO_SNAPSHOT",
        "Failed to take portfolio snapshot: " # Error.message(e),
        "takePortfolioSnapshot"
      );
    };
  };

  /**
   * Start the hourly portfolio snapshot timer
   */
  private func startPortfolioSnapshotTimer<system>() : async* () {
    if (portfolioSnapshotTimerId != 0) {
      cancelTimer(portfolioSnapshotTimerId);
    };

    portfolioSnapshotTimerId := setTimer<system>(
      #nanoseconds(3_600_000_000_000), // 1 hour
      func() : async () {
        await takePortfolioSnapshot(#Scheduled);
        await* startPortfolioSnapshotTimer();
      }
    );

    logger.info(
      "PORTFOLIO_SNAPSHOT",
      "Portfolio snapshot timer started - Interval=1h Timer_ID=" # Nat.toText(portfolioSnapshotTimerId),
      "startPortfolioSnapshotTimer"
    );
  };

  /**
   * Get portfolio history
   * 
   * Returns recent portfolio snapshots for analysis and charting.
   * Accessible by authenticated users.
   */
  public shared query ({ caller }) func getPortfolioHistory(limit : Nat) : async Result.Result<PortfolioHistoryResponse, PortfolioSnapshotError> {
    // Allow any authenticated caller (not anonymous)
    if (Principal.isAnonymous(caller)) {
      return #err(#NotAuthorized);
    };

    if (limit == 0 or limit > 2000) {
      return #err(#InvalidLimit);
    };

    let snapshots = Vector.toArray(portfolioSnapshots);
    let totalCount = snapshots.size();
    
    // Get the most recent 'limit' snapshots in chronological order (oldest first)
    let startIndex = if (totalCount > limit) { totalCount - limit } else { 0 };
    let limitedSnapshots = Array.subArray(snapshots, startIndex, totalCount - startIndex);

    let response : PortfolioHistoryResponse = {
      snapshots = limitedSnapshots;
      totalCount = totalCount;
    };

    #ok(response);
  };

  /**
   * Manually trigger a portfolio snapshot (admin function)
   */
  public shared ({ caller }) func takeManualPortfolioSnapshot(reason : ?Text) : async Result.Result<Text, PortfolioSnapshotError> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    await takePortfolioSnapshot(#Manual);
    
    let reasonText = switch (reason) {
      case (?r) r;
      case null "Manual portfolio snapshot taken";
    };
    logTreasuryAdminAction(caller, #TakeManualSnapshot, reasonText, true, null);
    #ok("Manual portfolio snapshot taken successfully");
  };

  /**
   * Get the current maximum portfolio snapshots limit
   * 
   * Returns the current limit for portfolio snapshots storage.
   * Accessible by any user with query access.
   */
  public query func getMaxPortfolioSnapshots() : async Nat {
    maxPortfolioSnapshots;
  };

  /**
   * Update the maximum portfolio snapshots limit
   * 
   * Sets the maximum number of portfolio snapshots to store.
   * Older snapshots will be automatically removed when the limit is exceeded.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func updateMaxPortfolioSnapshots(newLimit : Nat, reason : ?Text) : async Result.Result<Text, PortfolioSnapshotError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    // Validate the new limit
    if (newLimit < 10) {
      return #err(#SystemError("Maximum portfolio snapshots cannot be less than 10"));
    };

    if (newLimit > 10000) {
      return #err(#SystemError("Maximum portfolio snapshots cannot be more than 10,000"));
    };

    let oldLimit = maxPortfolioSnapshots;
    maxPortfolioSnapshots := newLimit;

    // If the new limit is smaller than current storage, trim excess snapshots
    if (Vector.size(portfolioSnapshots) > newLimit) {
      Vector.reverse(portfolioSnapshots);
      while (Vector.size(portfolioSnapshots) > newLimit) {
        ignore Vector.removeLast(portfolioSnapshots);
      };
      Vector.reverse(portfolioSnapshots);
    };

    logger.info(
      "PORTFOLIO_SNAPSHOT_CONFIG",
      "Maximum portfolio snapshots limit updated - Old=" # Nat.toText(oldLimit) #
      " New=" # Nat.toText(newLimit) #
      " Current_count=" # Nat.toText(Vector.size(portfolioSnapshots)) #
      " Updated_by=" # Principal.toText(caller),
      "updateMaxPortfolioSnapshots"
    );

    let reasonText = switch (reason) {
      case (?r) r;
      case null "Portfolio snapshots limit updated";
    };
    logTreasuryAdminAction(caller, #UpdateMaxPortfolioSnapshots({oldLimit; newLimit}), reasonText, true, null);
    #ok("Maximum portfolio snapshots limit updated to " # Nat.toText(newLimit));
  };

  //=========================================================================
  // PORTFOLIO CIRCUIT BREAKER SYSTEM
  //=========================================================================

  /**
   * Add a portfolio circuit breaker condition
   *
   * Creates a circuit breaker rule that will pause all trading when portfolio value
   * changes exceed the specified threshold within the time window.
   *
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func addPortfolioCircuitBreakerCondition(
    name : Text,
    direction : PortfolioDirection,
    percentage : Float,
    timeWindowNS : Nat,
    valueType : PortfolioValueType
  ) : async Result.Result<Nat, PortfolioCircuitBreakerError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    // Validate inputs
    if (percentage <= 0.0 or percentage > 1000.0) {
      return #err(#InvalidPercentage);
    };

    if (timeWindowNS < 60_000_000_000 or timeWindowNS > (86400_000_000_000 * 7)) { // 1 minute to 7 days
      return #err(#InvalidTimeWindow);
    };

    // Check for duplicate names
    for ((_, condition) in Map.entries(portfolioCircuitBreakerConditions)) {
      if (condition.name == name) {
        return #err(#DuplicateName);
      };
    };

    let conditionId = nextPortfolioConditionId;
    nextPortfolioConditionId += 1;

    let newCondition : PortfolioCircuitBreakerCondition = {
      id = conditionId;
      name = name;
      direction = direction;
      percentage = percentage;
      timeWindowNS = timeWindowNS;
      valueType = valueType;
      isActive = true;
      createdAt = now();
      createdBy = caller;
    };

    Map.set(portfolioCircuitBreakerConditions, Map.nhash, conditionId, newCondition);

    logger.info(
      "PORTFOLIO_CIRCUIT_BREAKER", 
      "Portfolio circuit breaker condition added - ID=" # Nat.toText(conditionId) #
      " Name=" # name #
      " Direction=" # debug_show(direction) #
      " Percentage=" # Float.toText(percentage) # "%" #
      " TimeWindow=" # Nat.toText(timeWindowNS / 1_000_000_000) # "s" #
      " ValueType=" # debug_show(valueType) #
      " CreatedBy=" # Principal.toText(caller),
      "addPortfolioCircuitBreakerCondition"
    );

    #ok(conditionId);
  };

  /**
   * Update an existing portfolio circuit breaker condition
   */
  public shared ({ caller }) func updatePortfolioCircuitBreakerCondition(
    conditionId : Nat,
    updates : PortfolioCircuitBreakerUpdate
  ) : async Result.Result<Text, PortfolioCircuitBreakerError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    switch (Map.get(portfolioCircuitBreakerConditions, Map.nhash, conditionId)) {
      case null {
        return #err(#ConditionNotFound);
      };
      case (?currentCondition) {
        var updatedCondition = currentCondition;

        // Apply updates
        switch (updates.name) {
          case (?newName) {
            // Check for duplicate names (excluding current condition)
            for ((id, condition) in Map.entries(portfolioCircuitBreakerConditions)) {
              if (id != conditionId and condition.name == newName) {
                return #err(#DuplicateName);
              };
            };
            updatedCondition := { updatedCondition with name = newName };
          };
          case null {};
        };

        switch (updates.direction) {
          case (?newDirection) {
            updatedCondition := { updatedCondition with direction = newDirection };
          };
          case null {};
        };

        switch (updates.percentage) {
          case (?newPercentage) {
            if (newPercentage <= 0.0 or newPercentage > 1000.0) {
              return #err(#InvalidPercentage);
            };
            updatedCondition := { updatedCondition with percentage = newPercentage };
          };
          case null {};
        };

        switch (updates.timeWindowNS) {
          case (?newTimeWindow) {
            if (newTimeWindow < 60_000_000_000 or newTimeWindow > (86400_000_000_000 * 7)) {
              return #err(#InvalidTimeWindow);
            };
            updatedCondition := { updatedCondition with timeWindowNS = newTimeWindow };
          };
          case null {};
        };

        switch (updates.valueType) {
          case (?newValueType) {
            updatedCondition := { updatedCondition with valueType = newValueType };
          };
          case null {};
        };

        switch (updates.isActive) {
          case (?newActive) {
            updatedCondition := { updatedCondition with isActive = newActive };
          };
          case null {};
        };

        Map.set(portfolioCircuitBreakerConditions, Map.nhash, conditionId, updatedCondition);

        logger.info(
          "PORTFOLIO_CIRCUIT_BREAKER",
          "Portfolio circuit breaker condition updated - ID=" # Nat.toText(conditionId) #
          " Name=" # updatedCondition.name #
          " UpdatedBy=" # Principal.toText(caller),
          "updatePortfolioCircuitBreakerCondition"
        );

        #ok("Portfolio circuit breaker condition updated successfully");
      };
    };
  };

  /**
   * Remove a portfolio circuit breaker condition
   */
  public shared ({ caller }) func removePortfolioCircuitBreakerCondition(conditionId : Nat) : async Result.Result<Text, PortfolioCircuitBreakerError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    switch (Map.remove(portfolioCircuitBreakerConditions, Map.nhash, conditionId)) {
      case null {
        return #err(#ConditionNotFound);
      };
      case (?removedCondition) {
        logger.info(
          "PORTFOLIO_CIRCUIT_BREAKER",
          "Portfolio circuit breaker condition removed - ID=" # Nat.toText(conditionId) #
          " Name=" # removedCondition.name #
          " RemovedBy=" # Principal.toText(caller),
          "removePortfolioCircuitBreakerCondition"
        );

        #ok("Portfolio circuit breaker condition removed successfully");
      };
    };
  };

  /**
   * Set portfolio circuit breaker condition active/inactive
   */
  public shared ({ caller }) func setPortfolioCircuitBreakerConditionActive(conditionId : Nat, isActive : Bool) : async Result.Result<Text, PortfolioCircuitBreakerError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    switch (Map.get(portfolioCircuitBreakerConditions, Map.nhash, conditionId)) {
      case null {
        return #err(#ConditionNotFound);
      };
      case (?condition) {
        let updatedCondition = { condition with isActive = isActive };
        Map.set(portfolioCircuitBreakerConditions, Map.nhash, conditionId, updatedCondition);

        logger.info(
          "PORTFOLIO_CIRCUIT_BREAKER",
          "Portfolio circuit breaker condition " # (if isActive "activated" else "deactivated") # 
          " - ID=" # Nat.toText(conditionId) #
          " Name=" # condition.name #
          " UpdatedBy=" # Principal.toText(caller),
          "setPortfolioCircuitBreakerConditionActive"
        );

        #ok("Portfolio circuit breaker condition " # (if isActive "activated" else "deactivated") # " successfully");
      };
    };
  };

  /**
   * List all portfolio circuit breaker conditions
   */
  public query func listPortfolioCircuitBreakerConditions() : async [PortfolioCircuitBreakerCondition] {
    Iter.toArray(Map.vals(portfolioCircuitBreakerConditions));
  };

  /**
   * Get a specific portfolio circuit breaker condition
   */
  public query func getPortfolioCircuitBreakerCondition(conditionId : Nat) : async ?PortfolioCircuitBreakerCondition {
    Map.get(portfolioCircuitBreakerConditions, Map.nhash, conditionId);
  };

  /**
   * Get portfolio circuit breaker logs
   */
  public query func getPortfolioCircuitBreakerLogs(offset : Nat, limit : Nat) : async { logs : [PortfolioCircuitBreakerLog]; totalCount : Nat } {
    let logsArray = Vector.toArray(portfolioCircuitBreakerLogs);
    let reversedLogs = Array.reverse(logsArray);
    let totalCount = reversedLogs.size();
    
    let actualLimit = if (limit > 100) { 100 } else { limit };
    let endIndex = if (offset + actualLimit > reversedLogs.size()) {
      reversedLogs.size();
    } else {
      offset + actualLimit;
    };
    
    if (offset >= reversedLogs.size()) {
      return { logs = []; totalCount = totalCount };
    };
    
    let slicedLogs = Iter.toArray(Array.slice(reversedLogs, offset, endIndex));
    
    {
      logs = slicedLogs;
      totalCount = totalCount;
    };
  };

  /**
   * Clear portfolio circuit breaker logs
   */
  public shared ({ caller }) func clearPortfolioCircuitBreakerLogs() : async Result.Result<Text, PortfolioCircuitBreakerError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    let clearedCount = Vector.size(portfolioCircuitBreakerLogs);
    Vector.clear(portfolioCircuitBreakerLogs);
    nextPortfolioCircuitBreakerId := 1;

    logger.info(
      "PORTFOLIO_CIRCUIT_BREAKER",
      "Portfolio circuit breaker logs cleared - Count=" # Nat.toText(clearedCount) #
      " ClearedBy=" # Principal.toText(caller),
      "clearPortfolioCircuitBreakerLogs"
    );

    #ok("Cleared " # Nat.toText(clearedCount) # " portfolio circuit breaker logs");
  };

  /**
   * Get the current paused token threshold for circuit breaker
   */
  public query func getPausedTokenThresholdForCircuitBreaker() : async Nat {
    pausedTokenThresholdForCircuitBreaker;
  };

  /**
   * Update the paused token threshold for circuit breaker
   *
   * Sets the number of paused tokens that will trigger the circuit breaker.
   * Only callable by admins with appropriate permissions.
   */
  public shared ({ caller }) func updatePausedTokenThresholdForCircuitBreaker(newThreshold : Nat) : async Result.Result<Text, PortfolioCircuitBreakerError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      return #err(#NotAuthorized);
    };

    // Validate the threshold
    if (newThreshold < 1) {
      return #err(#InvalidParameters("Threshold cannot be less than 1"));
    };

    if (newThreshold > 100) {
      return #err(#InvalidParameters("Threshold cannot be more than 100"));
    };

    let oldThreshold = pausedTokenThresholdForCircuitBreaker;
    pausedTokenThresholdForCircuitBreaker := newThreshold;

    logger.info(
      "PAUSED_TOKEN_THRESHOLD_CONFIG",
      "Paused token threshold for circuit breaker updated - Old=" # Nat.toText(oldThreshold) #
      " New=" # Nat.toText(newThreshold) #
      " Updated_by=" # Principal.toText(caller),
      "updatePausedTokenThresholdForCircuitBreaker"
    );

    #ok("Paused token threshold for circuit breaker updated to " # Nat.toText(newThreshold));
  };

  //=========================================================================
  // PORTFOLIO CIRCUIT BREAKER SYSTEM - CORE MONITORING
  //=========================================================================

  /**
   * Check all active portfolio circuit breaker conditions
   *
   * Analyzes portfolio value history to detect significant changes
   * and triggers circuit breaker (pauses all trading) if conditions are met.
   */
  private func checkPortfolioCircuitBreakerConditions() {
    // Optimization: Skip circuit breaker checks if all tokens are already paused
    var allTokensPaused = true;
    var pausedTokenCount = 0;
    
    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active) {
        if (isTokenPausedFromTrading(token)) {
          pausedTokenCount += 1;
        } else {
          allTokensPaused := false;
        };
      };
    };
    
    if (allTokensPaused) {
      // All tokens are already paused - no need to check circuit breakers
      //logger.info("CIRCUIT_BREAKER", 
      //  "Skipping circuit breaker checks - All active tokens are already paused",
      //  "checkPortfolioCircuitBreakerConditions"
      //);
      return;
    };
    
    // Check if we've hit the paused token threshold circuit breaker
    if (pausedTokenCount >= pausedTokenThresholdForCircuitBreaker) {
      triggerPausedTokenThresholdCircuitBreaker(pausedTokenCount);
      return; // Exit early since circuit breaker was triggered
    };
    
    // Check all active portfolio circuit breaker conditions
    for ((conditionId, condition) in Map.entries(portfolioCircuitBreakerConditions)) {
      if (condition.isActive) {
        switch (analyzePortfolioValueChangeInWindow(condition)) {
          case (?triggerData) {
            // Condition triggered - activate circuit breaker
            triggerPortfolioCircuitBreaker(condition, triggerData);
          };
          case null {
            // No trigger - continue monitoring
          };
        };
      };
    };
  };

  /**
   * Analyze portfolio value changes within the specified time window
   *
   * Returns trigger data if the condition is met, null otherwise
   */
  private func analyzePortfolioValueChangeInWindow(
    condition : PortfolioCircuitBreakerCondition
  ) : ?PortfolioTriggerData {
    let currentTime = now();
    let windowStartTime = currentTime - condition.timeWindowNS;
    
    // Filter portfolio snapshots within the time window
    let snapshotsArray = Vector.toArray(portfolioSnapshots);
    let relevantSnapshots = Array.filter<PortfolioSnapshot>(snapshotsArray, func(snapshot) {
      snapshot.timestamp >= windowStartTime
    });
    
    // Need at least 2 snapshots to detect changes
    if (relevantSnapshots.size() < 2) {
      return null;
    };
    
    // Get current portfolio value
    let currentValue = getCurrentPortfolioValue(condition.valueType);
    
    // Create array of all values with timestamps (including current)
    let allSnapshots = Array.append(relevantSnapshots, [{
      timestamp = currentTime;
      totalValueICP = switch (condition.valueType) {
        case (#ICP) { Int.abs(Float.toInt(currentValue * 100_000_000.0)) };
        case (#USD) { 0 }; // Not used for USD
      };
      totalValueUSD = switch (condition.valueType) {
        case (#USD) { currentValue };
        case (#ICP) { 0.0 }; // Not used for ICP
      };
      tokens = []; // Not needed for this analysis
      snapshotReason = #Manual; // Not relevant
    }]);
    
    // Sort by timestamp (oldest first)
    let sortedSnapshots = Array.sort(allSnapshots, func(a: PortfolioSnapshot, b: PortfolioSnapshot) : { #less; #equal; #greater } {
      if (a.timestamp < b.timestamp) { #less }
      else if (a.timestamp > b.timestamp) { #greater }
      else { #equal }
    });
    
    switch (condition.direction) {
      case (#Down) {
        // For DROP detection: Find min first, then find max in range [window_start, min_time]
        
        // Find minimum value and its time
        var minValue = currentValue;
        var minValueTime = currentTime;
        
        for (snapshot in sortedSnapshots.vals()) {
          let snapshotValue = switch (condition.valueType) {
            case (#ICP) { Float.fromInt(snapshot.totalValueICP) / 100_000_000.0 };
            case (#USD) { snapshot.totalValueUSD };
          };
          
          if (snapshotValue < minValue) {
            minValue := snapshotValue;
            minValueTime := snapshot.timestamp;
          };
        };
        
        // Find maximum value in the range [window_start, min_time]
        var maxValueBeforeMin = minValue; // Default to min if no earlier values
        
        for (snapshot in sortedSnapshots.vals()) {
          if (snapshot.timestamp <= minValueTime) {
            let snapshotValue = switch (condition.valueType) {
              case (#ICP) { Float.fromInt(snapshot.totalValueICP) / 100_000_000.0 };
              case (#USD) { snapshot.totalValueUSD };
            };
            
            if (snapshotValue > maxValueBeforeMin) {
              maxValueBeforeMin := snapshotValue;
            };
          };
        };
        
        // Calculate drop percentage: (max_before_min - min) / max_before_min
        if (maxValueBeforeMin > minValue) {
          let dropPercentage = ((maxValueBeforeMin - minValue) / maxValueBeforeMin) * 100.0;
          
          if (dropPercentage >= condition.percentage) {
            return ?{
              currentValue = currentValue;
              minValueInWindow = minValue;
              maxValueInWindow = maxValueBeforeMin;
              windowStartTime = windowStartTime;
              actualChangePercent = dropPercentage;
              valueType = condition.valueType;
            };
          };
        };
      };
      
      case (#Up) {
        // For CLIMB detection: Find max first, then find min in range [window_start, max_time]
        
        // Find maximum value and its time
        var maxValue = currentValue;
        var maxValueTime = currentTime;
        
        for (snapshot in sortedSnapshots.vals()) {
          let snapshotValue = switch (condition.valueType) {
            case (#ICP) { Float.fromInt(snapshot.totalValueICP) / 100_000_000.0 };
            case (#USD) { snapshot.totalValueUSD };
          };
          
          if (snapshotValue > maxValue) {
            maxValue := snapshotValue;
            maxValueTime := snapshot.timestamp;
          };
        };
        
        // Find minimum value in the range [window_start, max_time]
        var minValueBeforeMax = maxValue; // Default to max if no earlier values
        
        for (snapshot in sortedSnapshots.vals()) {
          if (snapshot.timestamp <= maxValueTime) {
            let snapshotValue = switch (condition.valueType) {
              case (#ICP) { Float.fromInt(snapshot.totalValueICP) / 100_000_000.0 };
              case (#USD) { snapshot.totalValueUSD };
            };
            
            if (snapshotValue < minValueBeforeMax) {
              minValueBeforeMax := snapshotValue;
            };
          };
        };
        
        // Calculate climb percentage: (max - min_before_max) / min_before_max
        if (minValueBeforeMax > 0.0 and maxValue > minValueBeforeMax) {
          let climbPercentage = ((maxValue - minValueBeforeMax) / minValueBeforeMax) * 100.0;
          
          if (climbPercentage >= condition.percentage) {
            return ?{
              currentValue = currentValue;
              minValueInWindow = minValueBeforeMax;
              maxValueInWindow = maxValue;
              windowStartTime = windowStartTime;
              actualChangePercent = climbPercentage;
              valueType = condition.valueType;
            };
          };
        };
      };
    };
    
    null;
  };

  /**
   * Get current portfolio value
   */
  private func getCurrentPortfolioValue(valueType : PortfolioValueType) : Float {
    var totalValueICP : Nat = 0;
    var totalValueUSD : Float = 0.0;

    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and details.balance > 0) {
        let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        let valueInUSD = (Float.fromInt(details.balance) * details.priceInUSD) / Float.fromInt(10 ** details.tokenDecimals);
        
        totalValueICP += valueInICP;
        totalValueUSD += valueInUSD;
      };
    };

    switch (valueType) {
      case (#ICP) { Float.fromInt(totalValueICP) / 100_000_000.0 }; // Convert e8s to ICP
      case (#USD) { totalValueUSD };
    };
  };

  /**
   * Calculate percentage change between two portfolio values
   */
  private func calculatePortfolioPercentageChange(fromValue : Float, toValue : Float) : Float {
    if (fromValue == 0.0) {
      return 0.0;
    };
    
    let change = toValue - fromValue;
    let percentage = (change / fromValue) * 100.0;
    percentage;
  };

  /**
   * Trigger circuit breaker due to paused token threshold
   *
   * Called when the number of paused tokens reaches the configured threshold.
   * Pauses all remaining active tokens.
   */
  private func triggerPausedTokenThresholdCircuitBreaker(pausedTokenCount : Nat) {
    // Get all active trading tokens to pause
    let tokensToProcess = Iter.toArray(Map.entries(tokenDetailsMap));
    let pausedTokens = Vector.new<Principal>();

    for ((token, details) in tokensToProcess.vals()) {
      if (details.Active and not isTokenPausedFromTrading(token)) {
        let pauseReason : TradingPauseReason = #CircuitBreaker({
          reason = "Paused token threshold circuit breaker triggered: " # Nat.toText(pausedTokenCount) # " tokens already paused (threshold: " # Nat.toText(pausedTokenThresholdForCircuitBreaker) # ")";
          triggeredAt = now();
          severity = "Critical";
        });

        let success = pauseTokenFromTrading(token, pauseReason);
        if (success) {
          Vector.add(pausedTokens, token);
        };
      };
    };

    logger.warn(
      "PAUSED_TOKEN_THRESHOLD_CIRCUIT_BREAKER",
      "PAUSED TOKEN THRESHOLD CIRCUIT BREAKER TRIGGERED - " #
      " Paused_Tokens=" # Nat.toText(pausedTokenCount) #
      " Threshold=" # Nat.toText(pausedTokenThresholdForCircuitBreaker) #
      " Additional_Tokens_Paused=" # Nat.toText(Vector.size(pausedTokens)),
      "triggerPausedTokenThresholdCircuitBreaker"
    );
  };

  /**
   * Trigger portfolio circuit breaker
   *
   * Pauses all trading tokens and logs the event
   */
  private func triggerPortfolioCircuitBreaker(
    condition : PortfolioCircuitBreakerCondition,
    triggerData : PortfolioTriggerData
  ) {
    // Get all active trading tokens to pause
    let tokensToProcess = Iter.toArray(Map.entries(tokenDetailsMap));
    let pausedTokens = Vector.new<Principal>();

    for ((token, details) in tokensToProcess.vals()) {
      if (details.Active and not isTokenPausedFromTrading(token)) {
        let pauseReason : TradingPauseReason = #CircuitBreaker({
          reason = "Portfolio circuit breaker triggered: " # condition.name;
          triggeredAt = now();
          severity = "Critical";
        });

        let success = pauseTokenFromTrading(token, pauseReason);
        if (success) {
          Vector.add(pausedTokens, token);
        };
      };
    };

    // Create and store the circuit breaker log
    let logId = nextPortfolioCircuitBreakerId;
    nextPortfolioCircuitBreakerId += 1;

    let circuitBreakerLog : PortfolioCircuitBreakerLog = {
      id = logId;
      timestamp = now();
      triggeredCondition = condition;
      portfolioData = triggerData;
      pausedTokens = Vector.toArray(pausedTokens);
    };

    Vector.add(portfolioCircuitBreakerLogs, circuitBreakerLog);

    // Remove oldest logs if we exceed the limit
    if (Vector.size(portfolioCircuitBreakerLogs) > maxPortfolioCircuitBreakerLogs) {
      Vector.reverse(portfolioCircuitBreakerLogs);
      while (Vector.size(portfolioCircuitBreakerLogs) > maxPortfolioCircuitBreakerLogs) {
        ignore Vector.removeLast(portfolioCircuitBreakerLogs);
      };
      Vector.reverse(portfolioCircuitBreakerLogs);
    };

    let valueTypeText = switch (triggerData.valueType) {
      case (#ICP) { "ICP" };
      case (#USD) { "USD" };
    };

    logger.warn(
      "PORTFOLIO_CIRCUIT_BREAKER",
      "PORTFOLIO CIRCUIT BREAKER TRIGGERED - Condition=" # condition.name #
      " Current_Value=" # Float.toText(triggerData.currentValue) # valueTypeText #
      " Change=" # Float.toText(triggerData.actualChangePercent) # "%" #
      " Direction=" # debug_show(condition.direction) #
      " Tokens_Paused=" # Nat.toText(Vector.size(pausedTokens)),
      "triggerPortfolioCircuitBreaker"
    );
  };

  //=========================================================================
  // 4. TOKEN TRANSFER SYSTEM
  //=========================================================================

  /**
   * Process batch transfers from the DAO
   *
   * Handles both immediate and queued transfers of various token types.
   * Only callable by DAO.
   *
   * tempTransferQueue - Array of transfer instructions
   * Immediate - If true, process immediately and return block IDs
   *
   */
  public shared ({ caller }) func receiveTransferTasks(tempTransferQueue : [(TransferRecipient, Nat, Principal, Nat8)], Immediate : Bool) : async (Bool, ?[(Principal, Nat64)]) {
    assert (caller == DAOPrincipal or caller == MintVaultPrincipal);

    if (Immediate) {
      let blocks = Vector.new<(Principal, Nat64)>();
      // Pre-fill with placeholder values
      for (i in tempTransferQueue.vals()) {
        Vector.add(blocks, (i.2, 0 : Nat64)); // Initialize with 0 as blockIndex
      };

      let transferTasksICP = Vector.new<(async TransferResultICP, (TransferRecipient, Nat, Principal, Nat8, Nat))>();
      let transferTasksICRC1 = Vector.new<(async TransferResultICRC1, (TransferRecipient, Nat, Principal, Nat8, Nat))>();

      // Process immediate transfers
      var index = 0;
      for (data in tempTransferQueue.vals()) {
        nsAdd += 1;
        Debug.print("Processing immediate transfer: " #debug_show (data.1) # " " #debug_show (data.2) # " to " #debug_show (data.0));

        if (data.2 != ICPprincipal) {
          // ICRC1 Transfer
          let token = actor (Principal.toText(data.2)) : ICRC1.FullInterface;

          let recipient = switch (data.0) {
            case (#principal(p)) { p };
            case (#accountId({ owner })) { owner };
          };

          let subaccount = switch (data.0) {
            case (#principal(_)) { null };
            case (#accountId({ subaccount })) { subaccount };
          };

          let transferTask = token.icrc1_transfer({
            from_subaccount = ?Blob.fromArray([data.3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            to = { owner = recipient; subaccount = subaccount };
            amount = data.1;
            fee = null;
            memo = ?Blob.fromArray([1]);
            created_at_time = ?(natToNat64(Int.abs(now())) + nsAdd);
          });
          Vector.add(transferTasksICRC1, (transferTask, (data.0, data.1, data.2, data.3, index)));

        } else {
          // ICP Transfer
          let ledger = actor (ICPprincipalText) : Ledger.Interface;
          var Tfees = switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
            case null { 10000 };
            case (?(foundTrades)) { foundTrades.tokenTransferFee };
          };

          let transferTask = ledger.transfer({
            memo : Nat64 = 0;
            from_subaccount = ?Blob.fromArray([data.3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            to = switch (data.0) {
              case (#principal(p)) Principal.toLedgerAccount(p, null);
              case (#accountId({ owner; subaccount })) Principal.toLedgerAccount(owner, subaccount);
            };
            amount = { e8s = natToNat64(data.1) };
            fee = { e8s = natToNat64(Tfees) };
            created_at_time = ?{
              timestamp_nanos = natToNat64(Int.abs(now())) + nsAdd;
            };
          });
          Vector.add(transferTasksICP, (transferTask, (data.0, data.1, data.2, data.3, index)));
        };
        index += 1;
      };

      // Wait for all ICRC1 transfers to complete
      for (transferTask in Vector.vals(transferTasksICRC1)) {
        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(txIndex)) {
              Vector.put(blocks, transferTask.1.4, (transferTask.1.2, natToNat64(txIndex)));
            };
            case (#Err(_)) {
              Vector.put(blocks, transferTask.1.4, (transferTask.1.2, natToNat64(0)));
            };
          };
        } catch (_) {
          Vector.put(blocks, transferTask.1.4, (transferTask.1.2, natToNat64(0)));
        };
      };

      // Wait for all ICP transfers to complete
      for (transferTask in Vector.vals(transferTasksICP)) {
        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(blockIndex)) {
              Vector.put(blocks, transferTask.1.4, (transferTask.1.2, blockIndex));
            };
            case (#Err(_)) {
              Vector.put(blocks, transferTask.1.4, (transferTask.1.2, natToNat64(0)));
            };
          };
        } catch (_) {
          Vector.put(blocks, transferTask.1.4, (transferTask.1.2, natToNat64(0)));
        };
      };

      return (true, ?Vector.toArray(blocks));

    } else {
      // Handle non-immediate transfers as before
      if (tempTransferQueue.size() != 0) {
        try {
          Vector.addFromIter(transferQueue, tempTransferQueue.vals());
          if (test) {
            try {
              await transferTimer(true);
            } catch (_) {};
          } else {
            Vector.add(
              transferTimerIDs,
              setTimer<system>(
                #nanoseconds(100000000),
                func() : async () {
                  try {
                    await transferTimer(false);
                  } catch (_) {
                    retryFunc<system>(50, 15, #transfer);
                  };
                },
              ),
            );
          };
          return (true, ?[]);
        } catch (e) {
          if test Debug.print("Error transfering: " # Error.message(e));
          return (false, ?[]);
        };
      } else {
        return (true, ?[]);
      };
    };
  };

  /**
   * Process scheduled transfers from the queue
   *
   * Takes transfers from the queue and executes them, handling
   * failures by re-queueing failed transfers.
   */
  private func transferTimer(all : Bool) : async () {
    let transferBatch = Vector.new<(TransferRecipient, Nat, Principal, Nat8)>();
    let transferTasksICP = Vector.new<(async TransferResultICP, (TransferRecipient, Nat, Principal, Nat8))>();
    let transferTasksICRC1 = Vector.new<(async TransferResultICRC1, (TransferRecipient, Nat, Principal, Nat8))>();

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
      for (data in Vector.vals(transferBatch)) {
        //Variable to make sure no transfers have the same execuution time (gets reset in tokeninfotimer)
        nsAdd += 1;
        Debug.print("Sending " #debug_show (data.1) # " " #debug_show (data.2) # " to " #debug_show (data.0));
        if (data.0 != #principal(Principal.fromText(self))) {
          if (data.2 != ICPprincipal) {
            // Transfer ICRC1 token
            let token = actor (Principal.toText(data.2)) : ICRC1.FullInterface;
            var Tfees = switch (Map.get(tokenDetailsMap, phash, data.2)) {
              case null { 10000 };
              case (?(foundTrades)) { foundTrades.tokenTransferFee };
            };

            let transferTask = token.icrc1_transfer({
              from_subaccount = ?Blob.fromArray([data.3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
              to = { owner = Principal.fromText(self); subaccount = null };
              amount = (data.1);
              fee = ?Tfees;
              memo = ?Blob.fromArray([1]);
              created_at_time = ?(natToNat64(Int.abs(now())) + nsAdd);
            });
            Vector.add(transferTasksICRC1, (transferTask, (data.0, data.1, data.2, data.3)));

          } else {
            // Transfer ICP
            let ledger = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : Ledger.Interface;
            var Tfees = switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
              case null { 10000 };
              case (?(foundTrades)) { foundTrades.tokenTransferFee };
            };
            let transferTask = ledger.transfer({
              memo : Nat64 = 0;
              from_subaccount = ?Blob.fromArray([data.3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
              to = Principal.toLedgerAccount(Principal.fromText(self), null);
              amount = { e8s = natToNat64(data.1) };
              fee = { e8s = natToNat64(Tfees) };
              created_at_time = ?{
                timestamp_nanos = natToNat64(Int.abs(now())) + nsAdd;
              };
            });
            Vector.add(transferTasksICP, (transferTask, (data.0, data.1, data.2, data.3)));
          };
        };
      };

      // Process ICRC1 transfers
      for (transferTask in Vector.vals(transferTasksICRC1)) {
        Debug.print("Processing ICRC1 transfer: " # debug_show ({ amount = transferTask.1.1; token = transferTask.1.2; recipient = debug_show (transferTask.1.0); fromAccount = debug_show (transferTask.1.3) }));

        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(txIndex)) {
              Debug.print("ICRC1 transfer successful. Transaction index: " # debug_show (txIndex));
            };
            case (#Err(transferError)) {
              Debug.print("ICRC1 transfer failed with error: " # debug_show (transferError));
              Debug.print("Adding failed ICRC1 transfer back to queue: " # debug_show ({ amount = transferTask.1.1; token = transferTask.1.2; recipient = debug_show (transferTask.1.0) }));
              Vector.add(transferQueue, transferTask.1);
            };
          };
        } catch (err) {
          Debug.print("ICRC1 transfer threw error: " # Error.message(err));
          Debug.print("Transfer details: " # debug_show ({ amount = transferTask.1.1; token = transferTask.1.2; recipient = debug_show (transferTask.1.0) }));
          Debug.print("Adding failed ICRC1 transfer back to queue");
          Vector.add(transferQueue, transferTask.1);
        };
      };

      // Process ICP transfers
      for (transferTask in Vector.vals(transferTasksICP)) {
        Debug.print("Processing ICP transfer: " # debug_show ({ amount = transferTask.1.1; token = transferTask.1.2; recipient = debug_show (transferTask.1.0) }));

        try {
          let result = await transferTask.0;
          switch (result) {
            case (#Ok(blockIndex)) {
              Debug.print("ICP transfer successful. Block index: " # debug_show (blockIndex));
            };
            case (#Err(transferError)) {
              Debug.print("ICP transfer failed with error: " # debug_show (transferError));
              Debug.print("Adding failed ICP transfer back to queue: " # debug_show ({ amount = transferTask.1.1; token = transferTask.1.2; recipient = debug_show (transferTask.1.0) }));
              Vector.add(transferQueue, transferTask.1);
            };
          };
        } catch (err) {
          Debug.print("ICP transfer threw error: " # Error.message(err));
          Debug.print("Transfer details: " # debug_show ({ amount = transferTask.1.1; token = transferTask.1.2; recipient = debug_show (transferTask.1.0) }));
          Debug.print("Adding failed ICP transfer back to queue");
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
                  retryFunc<system>(50, 15, #transfer);
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
    };
  };

  //=========================================================================
  // 5. REBALANCING ENGINE CORE
  //=========================================================================

  public shared ({ caller }) func admin_executeTradingCycle(reason : ?Text) : async Result.Result<Text, RebalanceError> {
    if (((await hasAdminPermission(caller, #startRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      Debug.print("Not authorized to execute trading cycle: " # debug_show(caller));
      return #err(#ConfigError("Not authorized"));
    };
    Debug.print("Starting trading cycle");

    await* do_executeTradingCycle();

    if (rebalanceState.status == #Idle) {
      await* do_executeTradingStep();
    };

    Debug.print("Completing trading cycle");
    
    let reasonText = switch (reason) {
      case (?r) r;
      case null "Manual trading cycle execution";
    };
    logTreasuryAdminAction(caller, #ExecuteTradingCycle, reasonText, true, null);
    return #ok("Trading cycle executed");
  };

  /**
   * Timer for rebalancing trading execution
   *
   * Schedules periodic rebalancing based on configured interval
   */
  private func startTradingTimer<system>() {
    switch (rebalanceState.rebalanceTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };

    let newTimerId = setTimer<system>(
      #nanoseconds(rebalanceConfig.rebalanceIntervalNS),
      func() : async () {
        await* executeTradingCycle();
        // Recursively set next timer if still active
        if (rebalanceState.status != #Idle) {
          startTradingTimer<system>();
        };
      },
    );

    rebalanceState := {
      rebalanceState with
      rebalanceTimerId = ?newTimerId;
    };
  };

  /**
   * Execute a complete trading cycle
   *
   * Main trading loop that:
   * 1. Checks circuit breaker conditions
   * 2. Retries any failed transactions
   * 3. Executes the trading step
   * 4. Recovers from failures if needed
   */
  private func executeTradingCycle() : async* () {
    if (rebalanceState.status == #Idle) {
      return;
    };

    // Check circuit breaker conditions before each trading cycle
    checkPortfolioCircuitBreakerConditions();

    await* do_executeTradingCycle();
  };

  private func do_executeTradingCycle() : async* () {

    // Update lastRebalanceAttempt at the start of every trading cycle
    rebalanceState := {
      rebalanceState with
      metrics = {
        rebalanceState.metrics with
        lastRebalanceAttempt = now();
      };
    };

    // VERBOSE LOGGING: Trading cycle start
    logger.info("REBALANCE_CYCLE", 
      "Trading cycle started - Status=" # debug_show(rebalanceState.status) # 
      " Pending_Txs=" # Nat.toText(Map.size(pendingTxs)) #
      " Failed_Txs=" # Nat.toText(Map.size(failedTxs)) #
      " Executed_Trades=" # Nat.toText(rebalanceState.metrics.totalTradesExecuted) #
      " Failed_Trades=" # Nat.toText(rebalanceState.metrics.totalTradesFailed) #
      " Last_Attempt=" # Int.toText((now() - rebalanceState.metrics.lastRebalanceAttempt) / 1_000_000_000) # "s_ago",
      "do_executeTradingCycle"
    );

    // Update balances before any trading decisions
    await updateBalances();

    // Retry failed kongswap transactions
    await* retryFailedKongswapTransactions();

    // Execute price updates and trading step
    try {
      Debug.print("Starting trading cycle");
      await* executeTradingStep();
    } catch (e) {
      Debug.print("Trading cycle failed: " # Error.message(e));
      rebalanceState := {
        rebalanceState with
        status = #Failed("Trading cycle error: " # Error.message(e));
        metrics = {
          rebalanceState.metrics with
          currentStatus = #Failed("Execution error");
        };
      };
    };
  };

  /**
   * Retry failed KongSwap transactions
   *
   * Checks for and attempts to retry any failed KongSwap
   * transactions until they succeed or reach max attempts
   */
  private func retryFailedKongswapTransactions() : async* () {
    Debug.print("Checking for failed transactions to retry...");

    let retryableTransactions = Vector.new<(Nat, swaptypes.SwapTxRecord)>();

    // Collect transactions that are ready for retry
    for ((txId, record) in Map.entries(pendingTxs)) {
      switch (record.status) {
        case (#SwapFailed(_)) {
          Vector.add(retryableTransactions, (txId, record));
        };
        case (_) {}; // Skip non-failed transactions
      };
    };

    // Attempt retries
    for ((txId, record) in Vector.vals(retryableTransactions)) {
      Debug.print(
        "Retrying transaction " # Nat.toText(txId) # " (attempt " #
        Nat.toText(record.attempts + 1) # " of "
      );

      let retryResult = await KongSwap.retryTransaction(txId, pendingTxs, failedTxs, rebalanceConfig.maxKongswapAttempts);

      switch (retryResult) {
        case (#ok(_)) {
          Debug.print("Retry successful for transaction " # Nat.toText(txId));
        };
        case (#err(e)) {
          Debug.print("Retry failed for transaction " # Nat.toText(txId) # ": " # e);
        };
      };
    };
  };

  /**
   * Main trading step for rebalancing
   *
   * Core algorithm for portfolio rebalancing:
   * 1. Calculates current and target allocations
   * 2. Selects tokens for trading
   * 3. Checks pricing and liquidity
   * 4. Executes trades with optimal routing
   * 5. Updates prices and balances
   */
  private func executeTradingStep() : async* () {
    if (rebalanceState.status != #Trading) {
      return;
    };

    await* do_executeTradingStep();
  };

  private func do_executeTradingStep() : async* () {

    // Take portfolio snapshot before trading
    await takePortfolioSnapshot(#PreTrade);

    // Check circuit breaker conditions after taking pre-trade snapshot
    checkPortfolioCircuitBreakerConditions();

    // VERBOSE LOGGING: Portfolio state snapshot before trade analysis
    await* logPortfolioState("Pre-trade analysis");

    try {
      var attempts = 0;
      var success = false;
      label a while (attempts < rebalanceConfig.maxTradeAttemptsPerInterval and not success) {
        attempts += 1;
        Debug.print("Trading attempt " # Nat.toText(attempts) # " of " # Nat.toText(rebalanceConfig.maxTradeAttemptsPerInterval));

        // VERBOSE LOGGING: Trading attempt start
        logger.info("REBALANCE_CYCLE", 
          "Trading attempt " # Nat.toText(attempts) # "/" # Nat.toText(rebalanceConfig.maxTradeAttemptsPerInterval) #
          " started - Interval=" # Nat.toText(rebalanceConfig.rebalanceIntervalNS / 1_000_000_000) # "s" #
          " Min_Trade_Value=" # Nat.toText(rebalanceConfig.minTradeValueICP / 100000000) # "ICP" #
          " Max_Trade_Value=" # Nat.toText(rebalanceConfig.maxTradeValueICP / 100000000) # "ICP" #
          " Max_Slippage=" # Nat.toText(rebalanceConfig.maxSlippageBasisPoints) # "bp",
          "do_executeTradingStep"
        );

        Debug.print("Calculating trade requirements...");
        let tradeDiffs = calculateTradeRequirements();

        Debug.print("Trade diffs: " # debug_show (tradeDiffs));

        // Check if we have any viable trading candidates
        if (tradeDiffs.size() == 0) {
          Debug.print("No viable trading candidates after filtering - all tokens too close to target");
          incrementSkipCounter(#tokensFiltered);
          rebalanceState := {
            rebalanceState with
            status = #Idle;
            metrics = {
              rebalanceState.metrics with
              currentStatus = #Idle;
            };
          };
          
          logger.info("REBALANCE_CYCLE", 
            "No viable trading candidates - All tokens too close to target" #
            " Total_skipped=" # Nat.toText(rebalanceState.metrics.totalTradesSkipped),
            "do_executeTradingStep"
          );
          continue a;
        };

        Debug.print("Selecting trading pair...");
        switch (selectTradingPair(tradeDiffs)) {
          case (?(sellToken, buyToken)) {
            // Calculate total portfolio value for exact targeting decisions
            var totalValueICP = 0;
            for ((principal, details) in Map.entries(tokenDetailsMap)) {
              if (details.Active and not isTokenPausedFromTrading(principal)) {
                let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
                totalValueICP += valueInICP;
              };
            };
            
            let tokenDetailsSell = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
              case (?details) { details };
              case (null) {
                Debug.print("Error: Sell token not found in details");
                continue a;
              };
            };

            // Extract allocation differences for the selected tokens from tradeDiffs
            var sellTokenDiff : Nat = 0;
            var buyTokenDiff : Nat = 0;
            for ((token, diff, _) in tradeDiffs.vals()) {
              if (token == sellToken) {
                sellTokenDiff := Int.abs(diff);
              } else if (token == buyToken) {
                buyTokenDiff := Int.abs(diff);
              };
            };

            // Determine trade size using hybrid approach
            let useExactTargeting = shouldUseExactTargeting(sellTokenDiff, buyTokenDiff, totalValueICP);
            let maxTradeValueBP = if (totalValueICP > 0) { (rebalanceConfig.maxTradeValueICP * 10000) / totalValueICP } else { 0 };
            
            let tradeSize = if (useExactTargeting) {
              // VERBOSE LOGGING: Using exact targeting
              logger.info("TRADE_SIZING", 
                "Using EXACT targeting - Sell_diff=" # Int.toText(sellTokenDiff) # "bp" #
                " Buy_diff=" # Int.toText(buyTokenDiff) # "bp" #
                " Portfolio_value=" # Nat.toText(totalValueICP / 100000000) # "." # 
                Nat.toText((totalValueICP % 100000000) / 1000000) # "ICP" #
                " Max_trade_threshold=" # Nat.toText(maxTradeValueBP) # "bp" #
                " Reason=Close_to_target",
                "do_executeTradingStep"
              );
              calculateExactTargetTradeSize(sellToken, buyToken, totalValueICP, sellTokenDiff, buyTokenDiff);
            } else {
              // VERBOSE LOGGING: Using random sizing  
              logger.info("TRADE_SIZING", 
                "Using RANDOM sizing - Sell_diff=" # Int.toText(sellTokenDiff) # "bp" #
                " Buy_diff=" # Int.toText(buyTokenDiff) # "bp" #
                " Portfolio_value=" # Nat.toText(totalValueICP / 100000000) # "." # 
                Nat.toText((totalValueICP % 100000000) / 1000000) # "ICP" #
                " Max_trade_threshold=" # Nat.toText(maxTradeValueBP) # "bp" #
                " Reason=Large_imbalance",
                "do_executeTradingStep"
              );
              ((calculateTradeSizeMinMax() * (10 ** tokenDetailsSell.tokenDecimals)) / tokenDetailsSell.priceInICP);
            };
            
            // VERBOSE LOGGING: Final trade size decision
            let tradeSizeICP = (tradeSize * tokenDetailsSell.priceInICP) / (10 ** tokenDetailsSell.tokenDecimals);
            logger.info("TRADE_SIZING", 
              "Trade size calculated - Amount=" # Nat.toText(tradeSize) # " (raw)" #
              " ICP_value=" # Nat.toText(tradeSizeICP / 100000000) # "." # 
              Nat.toText((tradeSizeICP % 100000000) / 1000000) # "ICP" #
              " Min_allowed=" # Nat.toText(rebalanceConfig.minTradeValueICP / 100000000) # "." # 
              Nat.toText((rebalanceConfig.minTradeValueICP % 100000000) / 1000000) # "ICP" #
              " Max_allowed=" # Nat.toText(rebalanceConfig.maxTradeValueICP / 100000000) # "." # 
              Nat.toText((rebalanceConfig.maxTradeValueICP % 100000000) / 1000000) # "ICP",
              "do_executeTradingStep"
            );
            
            Debug.print("Selected pair: " # Principal.toText(sellToken) # " -> " # Principal.toText(buyToken) # " with size: " # Nat.toText(tradeSize));

            let bestExecution = await* findBestExecution(sellToken, buyToken, tradeSize);

            switch (bestExecution) {
              case (#ok(execution)) {
                // Calculate minimum amount out considering the actual slippage already in expectedOut
                // execution.expectedOut already includes the exchange's price impact (execution.slippage)
                // We need to calculate what the ideal output would be, then apply our slippage tolerance
                
                let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };
                let ourSlippageToleranceFloat = Float.fromInt(ourSlippageToleranceBasisPoints) / 100.0; // Convert basis points to percentage
                
                // Calculate ideal output (what we'd get with zero slippage)
                let idealOut : Nat = if (execution.slippage < 99.0) { // Avoid division by values too close to 1
                  let actualSlippageDecimal = execution.slippage / 100.0; // Convert percentage to decimal
                  Int.abs(Float.toInt(Float.fromInt(execution.expectedOut) / (1.0 - actualSlippageDecimal)))
                } else {
                  execution.expectedOut // Fallback if slippage is too high
                };
                
                // Apply our slippage tolerance to the ideal output
                let minAmountOutFloat = Float.fromInt(idealOut) * (1.0 - ourSlippageToleranceFloat / 100.0);
                let minAmountOut = Int.abs(Float.toInt(minAmountOutFloat));
                
                let tradeResult = await* executeTrade(
                  sellToken,
                  buyToken,
                  tradeSize,
                  execution.exchange,
                  minAmountOut,
                  idealOut, // Pass the spot price amount for correct slippage calculation
                );

                switch (tradeResult) {
                  case (#ok(record)) {
                    // Update trade history
                    let lastTrades = Vector.clone(rebalanceState.lastTrades);
                    Vector.add(lastTrades, record);
                    if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
                      Vector.reverse(lastTrades);
                      while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                        ignore Vector.removeLast(lastTrades);
                      };
                      Vector.reverse(lastTrades);
                    };

                    // Update token prices based on trade
                    let token0Details = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
                      case (?details) { details };
                      case (null) {
                        Debug.print("Error: Sell token not found in details");
                        return;
                      };
                    };
                    let token1Details = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                      case (?details) { details };
                      case (null) {
                        Debug.print("Error: Buy token not found in details");
                        return;
                      };
                    };

                    // Calculate prices based on trade
                    if (sellToken == ICPprincipal or buyToken == ICPprincipal) {
                      if (sellToken == ICPprincipal) {
                        // Selling ICP for token
                        let actualTokens = Float.fromInt(record.amountBought) / Float.fromInt(10 ** token1Details.tokenDecimals);
                        let actualICP = Float.fromInt(record.amountSold) / Float.fromInt(100000000);
                        let newPriceInICP = Int.abs(Float.toInt((actualICP / actualTokens) * 100000000));
                        let newPriceInUSD = token0Details.priceInUSD * actualICP / actualTokens;

                        updateTokenPriceWithHistory(buyToken, newPriceInICP, newPriceInUSD);
                      } else {
                        // Buying ICP for token
                        let actualTokens = Float.fromInt(record.amountSold) / Float.fromInt(10 ** token0Details.tokenDecimals);
                        let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                        let newPriceInICP = Int.abs(Float.toInt((actualICP / actualTokens) * 100000000));
                        let newPriceInUSD = token1Details.priceInUSD * actualICP / actualTokens;

                        updateTokenPriceWithHistory(sellToken, newPriceInICP, newPriceInUSD);
                      };
                    } else {
                      // Non-ICP pair
                      let maintainFirst = fuzz.nat.randomRange(0, 1) == 0;

                      if (maintainFirst) {
                        // Keep sell token price stable, calculate real amounts
                        let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** token0Details.tokenDecimals);
                        let actualTokensBought = Float.fromInt(record.amountBought) / Float.fromInt(10 ** token1Details.tokenDecimals);
                        let priceRatio = actualTokensSold / actualTokensBought;

                        let newPriceInICP = Int.abs(Float.toInt(Float.fromInt(token0Details.priceInICP) * priceRatio));
                        let newPriceInUSD = token0Details.priceInUSD * priceRatio;

                        updateTokenPriceWithHistory(buyToken, newPriceInICP, newPriceInUSD);
                      } else {
                        // Keep buy token price stable, calculate real amounts
                        let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** token0Details.tokenDecimals);
                        let actualTokensBought = Float.fromInt(record.amountBought) / Float.fromInt(10 ** token1Details.tokenDecimals);
                        let priceRatio = actualTokensBought / actualTokensSold;

                        let newPriceInICP = Int.abs(Float.toInt(Float.fromInt(token1Details.priceInICP) * priceRatio));
                        let newPriceInUSD = token1Details.priceInUSD * priceRatio;

                        updateTokenPriceWithHistory(sellToken, newPriceInICP, newPriceInUSD);
                      };
                    };

                    // Update rebalance state
                    rebalanceState := {
                      rebalanceState with
                      metrics = {
                        rebalanceState.metrics with
                        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                      };
                      lastTrades = lastTrades;
                    };
                    success := true;
                    Debug.print("Trade executed successfully and prices updated");
                    
                    // Refresh balances to get accurate post-trade data
                    await updateBalances();
                    
                    // Take portfolio snapshot after successful trade
                    await takePortfolioSnapshot(#PostTrade);

                    // Check circuit breaker conditions after post-trade snapshot
                    checkPortfolioCircuitBreakerConditions();
                              
                    // VERBOSE LOGGING: Portfolio state after successful trade
                    await* logPortfolioState("Post-trade completed");
                  };
                  case (#err(errorMsg)) {
                    // Create a failed trade record using the correct structure
                    let failedRecord : TradeRecord = {
                      tokenSold = sellToken;
                      tokenBought = buyToken;
                      amountSold = tradeSize;
                      amountBought = 0; // No tokens received
                      exchange = execution.exchange;
                      timestamp = now();
                      success = false;
                      error = ?errorMsg;
                      slippage = 0.0; // No slippage on failed trade
                    };

                    // Update trade history with the failed trade
                    let lastTrades = Vector.clone(rebalanceState.lastTrades);
                    Vector.add(lastTrades, failedRecord);

                    // Maintain maximum size for trade history
                    if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                      Vector.reverse(lastTrades);
                      while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                        ignore Vector.removeLast(lastTrades);
                      };
                      Vector.reverse(lastTrades);
                    };
                    rebalanceState := {
                      rebalanceState with
                      metrics = {
                        rebalanceState.metrics with
                        totalTradesFailed = rebalanceState.metrics.totalTradesFailed + 1;
                      };
                      lastTrades = lastTrades;
                    };
                  };
                };
              };
              case (#err(e)) {
                Debug.print("Could not find execution path: " # e);
                
                // ICP FALLBACK STRATEGY: If we can't find a direct route, try selling for ICP instead
                // This creates an ICP overweight that will be corrected in the next cycle
                if (buyToken != ICPprincipal and sellToken != ICPprincipal) {
                  Debug.print("Attempting ICP fallback route: " # Principal.toText(sellToken) # " -> ICP");
                  
                  // VERBOSE LOGGING: ICP fallback attempt
                  let sellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
                    case (?details) { details.tokenSymbol };
                    case null { "UNKNOWN" };
                  };
                  let buySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                    case (?details) { details.tokenSymbol };
                    case null { "UNKNOWN" };
                  };
                  
                  logger.info("ICP_FALLBACK", 
                    "Direct route failed, attempting ICP fallback - Original_pair=" # sellSymbol # "/" # buySymbol #
                    " Fallback_pair=" # sellSymbol # "/ICP" #
                    " Original_error=" # e #
                    " Strategy=Two_step_rebalancing",
                    "do_executeTradingStep"
                  );
                  
                  let icpFallbackExecution = await* findBestExecution(sellToken, ICPprincipal, tradeSize);
                  
                  switch (icpFallbackExecution) {
                    case (#ok(icpExecution)) {
                      Debug.print("ICP fallback route found, executing trade");
                      
                      // Calculate minimum amount out for ICP trade
                      let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };
                      let ourSlippageToleranceFloat = Float.fromInt(ourSlippageToleranceBasisPoints) / 100.0;
                      
                      let idealOut : Nat = if (icpExecution.slippage < 99.0) {
                        let actualSlippageDecimal = icpExecution.slippage / 100.0;
                        Int.abs(Float.toInt(Float.fromInt(icpExecution.expectedOut) / (1.0 - actualSlippageDecimal)))
                      } else {
                        icpExecution.expectedOut
                      };
                      
                      let minAmountOutFloat = Float.fromInt(idealOut) * (1.0 - ourSlippageToleranceFloat / 100.0);
                      let minAmountOut = Int.abs(Float.toInt(minAmountOutFloat));
                      
                      let icpTradeResult = await* executeTrade(
                        sellToken,
                        ICPprincipal,
                        tradeSize,
                        icpExecution.exchange,
                        minAmountOut,
                        idealOut,
                      );
                      
                      switch (icpTradeResult) {
                        case (#ok(record)) {
                          // Update trade history with fallback trade
                          let lastTrades = Vector.clone(rebalanceState.lastTrades);
                          
                          // Create a special trade record that indicates this was a fallback
                          let fallbackRecord : TradeRecord = {
                            record with
                            // Add a note in the error field to indicate this was a fallback trade
                            error = ?("ICP_FALLBACK: Original target was " # buySymbol # ", traded for ICP to enable future " # buySymbol # " purchase");
                          };
                          
                          Vector.add(lastTrades, fallbackRecord);
                          if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
                            Vector.reverse(lastTrades);
                            while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                              ignore Vector.removeLast(lastTrades);
                            };
                            Vector.reverse(lastTrades);
                          };

                          // Update ICP price based on trade
                          let sellTokenDetails = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
                            case (?details) { details };
                            case (null) {
                              Debug.print("Error: Sell token not found in details");
                              return;
                            };
                          };
                          
                          // Calculate new ICP price from the trade
                          let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** sellTokenDetails.tokenDecimals);
                          let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                          let newICPPrice = Int.abs(Float.toInt((actualTokensSold / actualICP) * Float.fromInt(sellTokenDetails.priceInICP)));
                          let newICPPriceUSD = sellTokenDetails.priceInUSD * actualTokensSold / actualICP;
                          
                          updateTokenPriceWithHistory(ICPprincipal, newICPPrice, newICPPriceUSD);

                          // Update rebalance state
                          rebalanceState := {
                            rebalanceState with
                            metrics = {
                              rebalanceState.metrics with
                              totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                            };
                            lastTrades = lastTrades;
                          };
                          
                          success := true;
                          Debug.print("ICP fallback trade executed successfully");
                          
                          // VERBOSE LOGGING: ICP fallback success
                          logger.info("ICP_FALLBACK", 
                            "ICP fallback trade SUCCESS - Sold=" # sellSymbol # 
                            " Amount_sold=" # Nat.toText(record.amountSold) #
                            " ICP_received=" # Nat.toText(record.amountBought) #
                            " Exchange=" # debug_show(record.exchange) #
                            " Next_cycle_will_trade=ICP_to_" # buySymbol,
                            "do_executeTradingStep"
                          );
                          
                          // Refresh balances to get accurate post-trade data
                          await updateBalances();
                          
                          // Take portfolio snapshot after successful ICP fallback trade
                          await takePortfolioSnapshot(#PostTrade);

                          // Check circuit breaker conditions after ICP fallback trade snapshot
                          checkPortfolioCircuitBreakerConditions();
                          
                          // VERBOSE LOGGING: Portfolio state after fallback trade
                          await* logPortfolioState("Post-ICP-fallback completed");
                        };
                        case (#err(icpErrorMsg)) {
                          Debug.print("ICP fallback trade also failed: " # icpErrorMsg);
                          
                          // VERBOSE LOGGING: ICP fallback failed
                          logger.error("ICP_FALLBACK", 
                            "ICP fallback trade FAILED - Original_error=" # e #
                            " ICP_fallback_error=" # icpErrorMsg #
                            " Status=Both_routes_failed",
                            "do_executeTradingStep"
                          );
                          
                          // Both direct and ICP fallback failed - count as skip
                          incrementSkipCounter(#noExecutionPath);
                          rebalanceState := {
                            rebalanceState with
                            metrics = {
                              rebalanceState.metrics with
                              currentStatus = #Failed("Both direct and ICP fallback routes failed: " # e # " | " # icpErrorMsg);
                            };
                          };
                          
                          // Attempt system recovery
                          await* recoverFromFailure();
                        };
                      };
                    };
                    case (#err(icpRouteError)) {
                      Debug.print("ICP fallback route also not available: " # icpRouteError);
                      
                      // VERBOSE LOGGING: ICP fallback route not found
                      logger.error("ICP_FALLBACK", 
                        "ICP fallback route NOT FOUND - Original_error=" # e #
                        " ICP_route_error=" # icpRouteError #
                        " Status=No_routes_available",
                        "do_executeTradingStep"
                      );
                      
                      // No routes available at all - count as skip
                      incrementSkipCounter(#noExecutionPath);
                      rebalanceState := {
                        rebalanceState with
                        metrics = {
                          rebalanceState.metrics with
                          currentStatus = #Failed("No routes available: " # e # " | ICP fallback: " # icpRouteError);
                        };
                      };
                      
                      // Attempt system recovery
                      await* recoverFromFailure();
                    };
                  };
                } else {
                  // We were already trying to buy ICP and that failed - no fallback possible
                  Debug.print("No ICP fallback possible - was already trying to buy ICP");
                  
                  logger.warn("ICP_FALLBACK", 
                    "No ICP fallback possible - Original trade was already targeting ICP" #
                    " Error=" # e #
                    " Status=ICP_route_failed",
                    "do_executeTradingStep"
                  );
                  
                  incrementSkipCounter(#noExecutionPath);
                  rebalanceState := {
                    rebalanceState with
                    metrics = {
                      rebalanceState.metrics with
                      currentStatus = #Failed(e);
                    };
                  };
                  
                  // Attempt system recovery
                  await* recoverFromFailure();
                };
              };
            };
          };
          case null {
            Debug.print("No valid trading pairs found");
            incrementSkipCounter(#noPairsFound);
            rebalanceState := {
              rebalanceState with
              status = #Idle;
              metrics = {
                rebalanceState.metrics with
                currentStatus = #Idle;
              };
            };
            
            // VERBOSE LOGGING: No trading pairs found
            logger.info("REBALANCE_CYCLE", 
              "No valid trading pairs found - Setting status to Idle" #
              " Trade_diffs_count=" # Nat.toText(tradeDiffs.size()) #
              " Active_tokens=" # Nat.toText(Map.size(tokenDetailsMap)) #
              " Total_skipped=" # Nat.toText(rebalanceState.metrics.totalTradesSkipped),
              "do_executeTradingStep"
            );
          };
        };
      };
      if (not success) {
        Debug.print("All trade attempts exhausted without success");
        rebalanceState := {
          rebalanceState with
          status = #Trading;
          metrics = {
            rebalanceState.metrics with
            currentStatus = #Trading;
          };
        };
        
        // VERBOSE LOGGING: All attempts failed
        logger.warn("REBALANCE_CYCLE", 
          "All " # Nat.toText(rebalanceConfig.maxTradeAttemptsPerInterval) # " trade attempts failed" #
          " - Will retry in next cycle" #
          " Total_Executed=" # Nat.toText(rebalanceState.metrics.totalTradesExecuted) #
          " Total_Failed=" # Nat.toText(rebalanceState.metrics.totalTradesFailed + attempts),
          "do_executeTradingStep"
        );
      };
    } catch (e) {
      Debug.print("Trading step failed: " # Error.message(e));
      
      // VERBOSE LOGGING: Trading step exception
      logger.error("REBALANCE_CYCLE", 
        "Trading step failed with exception: " # Error.message(e) # 
        " - Attempting recovery",
        "do_executeTradingStep"
      );
      
      await* recoverFromFailure();
    };
  };

  //=========================================================================
  // 6. TRADING ALGORITHM HELPERS
  //=========================================================================

  /**
   * Calculate current allocations and differences from targets
   *
   * Determines what trades need to be made by comparing:
   * - Current token allocations (in basis points)
   * - Target allocations from the DAO
   * 
   * Paused tokens are excluded from total value calculations and their
   * target allocations are redistributed proportionally among active tokens.
   *
   * Returns an array of (token, basisPointDiff, valueInICP) for trading
   */
  private func calculateTradeRequirements() : [(Principal, Int, Nat)] {
    var totalValueICP = 0;
    var totalTargetBasisPoints = 0;
    let allocations = Vector.new<TokenAllocation>();
    let activeTokens = Vector.new<Principal>();

    // VERBOSE LOGGING: Start allocation analysis
    logger.info("ALLOCATION_ANALYSIS", 
      "Starting allocation analysis - Total_tokens=" # Nat.toText(Map.size(tokenDetailsMap)) #
      " Current_allocations_count=" # Nat.toText(Map.size(currentAllocations)),
      "calculateTradeRequirements"
    );

    // First pass - identify active tokens and calculate total target basis points
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
        if ((details.Active or details.balance > 0) and not isTokenPausedFromTrading(principal)) {
            Vector.add(activeTokens, principal);
            switch (Map.get(currentAllocations, phash, principal)) {
                case (?target) { totalTargetBasisPoints += target };
                case null {};
            };
        };
    };

    // VERBOSE LOGGING: Active tokens and target allocations
    logger.info("ALLOCATION_ANALYSIS", 
      "Active token analysis - Active_tokens=" # Nat.toText(Vector.size(activeTokens)) #
      " Total_target_basis_points=" # Nat.toText(totalTargetBasisPoints) #
      " Expected_total=" # Nat.toText(10000),
      "calculateTradeRequirements"
    );

    // Log individual target allocations
    for (token in Vector.vals(activeTokens)) {
      let targetAllocation = switch (Map.get(currentAllocations, phash, token)) {
        case (?target) { target };
        case null { 0 };
      };
      let tokenSymbol = switch (Map.get(tokenDetailsMap, phash, token)) {
        case (?details) { details.tokenSymbol };
        case null { "UNKNOWN" };
      };
      logger.info("ALLOCATION_ANALYSIS", 
        "Target allocation - " # tokenSymbol # " (" # Principal.toText(token) # "): " #
        Nat.toText(targetAllocation) # "bp (" # 
        Nat.toText(targetAllocation / 100) # "." # Nat.toText((targetAllocation % 100) / 10) # "%)",
        "calculateTradeRequirements"
      );
    };

    // Second pass - calculate total value and store allocations
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
        if (details.Active or details.balance > 0) {
            let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
            
            // Only include value of unpaused tokens in total
            if (not isTokenPausedFromTrading(principal)) {
                totalValueICP += valueInICP;
            };

            // Calculate adjusted target basis points
            let targetBasisPoints = if (not details.Active and details.balance > 0) {
                0 // Force sell inactive tokens with balance
            } else if (isTokenPausedFromTrading(principal)) {
                0 // No target allocation for paused tokens
            } else {
                switch (Map.get(currentAllocations, phash, principal)) {
                    case (?target) {
                        // Adjust target proportionally based on total active target points
                        (target * 10000) / totalTargetBasisPoints
                    };
                    case null { 0 };
                };
            };

            Vector.add(
                allocations,
                {
                    token = principal;
                    currentBasisPoints = 0; // Will calculate in third pass
                    targetBasisPoints = targetBasisPoints;
                    diffBasisPoints = 0; // Will calculate in third pass
                    valueInICP = valueInICP;
                },
            );
        };
    };

    // Third pass - calculate current basis points and differences
    if (totalValueICP > 0) {
        for (i in Iter.range(0, Vector.size(allocations) - 1)) {
            let alloc = Vector.get(allocations, i);
            let details = Map.get(tokenDetailsMap, phash, alloc.token);
            
            let currentBasisPoints = switch details {
                case (?d) {
                    if (isTokenPausedFromTrading(alloc.token)) {
                        0 // Paused tokens don't contribute to current allocation
                    } else {
                        (alloc.valueInICP * 10000) / totalValueICP
                    };
                };
                case null { 0 };
            };
            
            let diffBasisPoints : Int = alloc.targetBasisPoints - currentBasisPoints;

            Vector.put(
                allocations,
                i,
                {
                    alloc with
                    currentBasisPoints = currentBasisPoints;
                    diffBasisPoints = diffBasisPoints;
                },
            );
        };
    };

    // VERBOSE LOGGING: Portfolio value and allocation calculations
    logger.info("ALLOCATION_ANALYSIS", 
      "Portfolio valuation complete - Total_ICP=" # Nat.toText(totalValueICP / 100000000) # "." # 
      Nat.toText((totalValueICP % 100000000) / 1000000) # " Active_positions=" # 
      Nat.toText(Vector.size(allocations)),
      "calculateTradeRequirements"
    );

    // Create weighted list of tokens to trade based on differences
    // Exclude tokens where the difference is smaller than min trade size
    let minTradeValueBasisPoints = if (totalValueICP > 0) {
        (rebalanceConfig.minTradeValueICP * 10000) / totalValueICP
    } else { 0 };
    
    let tradePairs = Vector.new<(Principal, Int, Nat)>();
    var excludedDueToMinTradeSize = 0;
    
    for (alloc in Vector.vals(allocations)) {
        let details = Map.get(tokenDetailsMap, phash, alloc.token);
        switch details {
            case (?d) {
                if (not isTokenPausedFromTrading(alloc.token) and alloc.diffBasisPoints != 0) {
                    // Check if the allocation difference is significant enough to warrant a trade
                    if (Int.abs(alloc.diffBasisPoints) > minTradeValueBasisPoints) {
                        Vector.add(tradePairs, (alloc.token, alloc.diffBasisPoints, alloc.valueInICP));
                    } else {
                        excludedDueToMinTradeSize += 1;
                        // VERBOSE LOGGING: Token excluded due to small difference
                        logger.info("ALLOCATION_ANALYSIS", 
                          "Token EXCLUDED (too close to target) - " # d.tokenSymbol # 
                          ": Diff=" # Int.toText(Int.abs(alloc.diffBasisPoints)) # "bp" #
                          " Min_trade_threshold=" # Nat.toText(minTradeValueBasisPoints) # "bp" #
                          " Reason=Below_min_trade_size",
                          "calculateTradeRequirements"
                        );
                    };
                };
            };
            case null {};
        };
    };

    // VERBOSE LOGGING: Final allocation analysis with differences
    var overweightCount = 0;
    var underweightCount = 0;
    var balancedCount = 0;
    var maxOverweight : Int = 0;
    var maxUnderweight : Int = 0;
    
    for (alloc in Vector.vals(allocations)) {
      let details = Map.get(tokenDetailsMap, phash, alloc.token);
      switch details {
        case (?d) {
          if (not isTokenPausedFromTrading(alloc.token)) {
            if (alloc.diffBasisPoints > 0) {
              underweightCount += 1;
              if (alloc.diffBasisPoints > maxUnderweight) {
                maxUnderweight := alloc.diffBasisPoints;
              };
            } else if (alloc.diffBasisPoints < 0) {
              overweightCount += 1;
              let absOverweight = Int.abs(alloc.diffBasisPoints);
              if (absOverweight > Int.abs(maxOverweight)) {
                maxOverweight := alloc.diffBasisPoints;
              };
            } else {
              balancedCount += 1;
            };
            
            // Log individual token analysis
            let tokenSymbol = d.tokenSymbol;
            let diffText = if (alloc.diffBasisPoints > 0) {
              "UNDERWEIGHT +" # Int.toText(alloc.diffBasisPoints) # "bp";
            } else if (alloc.diffBasisPoints < 0) {
              "OVERWEIGHT " # Int.toText(alloc.diffBasisPoints) # "bp";
            } else {
              "BALANCED";
            };
            
            logger.info("ALLOCATION_ANALYSIS", 
              "Token analysis - " # tokenSymbol # ": Current=" # Nat.toText(alloc.currentBasisPoints) # "bp" #
              " Target=" # Nat.toText(alloc.targetBasisPoints) # "bp" #
              " Diff=" # diffText #
              " Value=" # Nat.toText(alloc.valueInICP / 100000000) # "." # 
              Nat.toText((alloc.valueInICP % 100000000) / 1000000) # "ICP",
              "calculateTradeRequirements"
            );
          };
        };
        case null {};
      };
    };
    
    // Summary of allocation analysis
    logger.info("ALLOCATION_ANALYSIS", 
      "Allocation summary - Overweight=" # Nat.toText(overweightCount) # 
      " Underweight=" # Nat.toText(underweightCount) #
      " Balanced=" # Nat.toText(balancedCount) #
      " Max_overweight=" # Int.toText(Int.abs(maxOverweight)) # "bp" #
      " Max_underweight=" # Int.toText(maxUnderweight) # "bp" #
      " Excluded_too_close=" # Nat.toText(excludedDueToMinTradeSize) #
      " Min_trade_threshold=" # Nat.toText(minTradeValueBasisPoints) # "bp" #
      " Tradeable_pairs=" # Nat.toText(Vector.size(tradePairs)),
      "calculateTradeRequirements"
    );

    // Note: We don't increment skip counters here for filtered tokens
    // Skip counters are only incremented when an actual trade attempt is skipped
    // Individual token filtering is just part of the normal allocation analysis

    Vector.toArray(tradePairs);
  };

  /**
   * Select trading pair using weighted random selection
   *
   * Uses the allocation differences to select which tokens to trade:
   * - Tokens with negative diffs (overweight) become sell candidates
   * - Tokens with positive diffs (underweight) become buy candidates
   * - Weighted random selection gives preference to larger imbalances
   */
  private func selectTradingPair(
    tradeDiffs : [(Principal, Int, Nat)]
  ) : ?(Principal, Principal) {
    Debug.print("Selecting trading pair with " # Nat.toText(tradeDiffs.size()) # " diffs");
    Debug.print("Trade diffs: " # debug_show (tradeDiffs));
    
    // VERBOSE LOGGING: Trading pair selection start
    logger.info("PAIR_SELECTION", 
      "Starting pair selection - Total_candidates=" # Nat.toText(tradeDiffs.size()) #
      " Min_required=2",
      "selectTradingPair"
    );
    
    if (tradeDiffs.size() < 2) {
      incrementSkipCounter(#insufficientCandidates);
      logger.warn("PAIR_SELECTION", 
        "Insufficient candidates for pair selection - Need_at_least=2 Have=" # Nat.toText(tradeDiffs.size()),
        "selectTradingPair"
      );
      return null;
    };

    // Split into tokens to sell (negative diff) and buy (positive diff)
    let toSell = Vector.new<(Principal, Nat)>();
    let toBuy = Vector.new<(Principal, Nat)>();

    for ((token, diff, value) in tradeDiffs.vals()) {
      if (not isTokenPausedFromTrading(token)) {
        if (diff < 0) {
          Vector.add(toSell, (token, Int.abs(diff)));
        } else if (diff > 0) {
          Vector.add(toBuy, (token, Int.abs(diff)));
        };
      };
    };

    if (Vector.size(toSell) == 0 or Vector.size(toBuy) == 0) {
      // VERBOSE LOGGING: Insufficient candidates after filtering
      logger.warn("PAIR_SELECTION", 
        "Insufficient candidates after filtering - Sell_candidates=" # Nat.toText(Vector.size(toSell)) #
        " Buy_candidates=" # Nat.toText(Vector.size(toBuy)) #
        " Need_both_non_zero=true",
        "selectTradingPair"
      );
      return null;
    };

    // VERBOSE LOGGING: Candidates ready for selection
    logger.info("PAIR_SELECTION", 
      "Candidates ready for weighted selection - Sell_candidates=" # Nat.toText(Vector.size(toSell)) #
      " Buy_candidates=" # Nat.toText(Vector.size(toBuy)),
      "selectTradingPair"
    );

    // Calculate total weight for sell and buy sides
    var totalSellWeight = 0;
    var totalBuyWeight = 0;

    for ((_, weight) in Vector.vals(toSell)) {
      totalSellWeight += weight;
    };
    for ((_, weight) in Vector.vals(toBuy)) {
      totalBuyWeight += weight;
    };
    Debug.print("selectTradingPair- totalSellWeight: " # Nat.toText(totalSellWeight));
    Debug.print("selectTradingPair- totalBuyWeight: " # Nat.toText(totalBuyWeight));

    // Generate random numbers within the weight ranges
    let sellRandom = fuzz.nat.randomRange(0, totalSellWeight);
    let buyRandom = fuzz.nat.randomRange(0, totalBuyWeight);

    // Select tokens based on weighted random numbers
    var sellSum = 0;
    var buySum = 0;
    var selectedSell : ?Principal = null;
    var selectedBuy : ?Principal = null;

    for ((token, weight) in Vector.vals(toSell)) {
      sellSum += weight;
      if (sellSum >= sellRandom and selectedSell == null) {
        selectedSell := ?token;
      };
    };
    Debug.print("selectTradingPair- selectedSell: " # debug_show (selectedSell));

    for ((token, weight) in Vector.vals(toBuy)) {
      buySum += weight;
      if (buySum >= buyRandom and selectedBuy == null) {
        selectedBuy := ?token;
      };
    };
    Debug.print("selectTradingPair- selectedBuy: " # debug_show (selectedBuy));

    // VERBOSE LOGGING: Final pair selection result
    switch (selectedSell, selectedBuy) {
      case (?sell, ?buy) {
        let sellSymbol = switch (Map.get(tokenDetailsMap, phash, sell)) {
          case (?details) { details.tokenSymbol };
          case null { "UNKNOWN" };
        };
        let buySymbol = switch (Map.get(tokenDetailsMap, phash, buy)) {
          case (?details) { details.tokenSymbol };
          case null { "UNKNOWN" };
        };
        logger.info("PAIR_SELECTION", 
          "Pair selected successfully - Sell=" # sellSymbol # " (" # Principal.toText(sell) # ")" #
          " Buy=" # buySymbol # " (" # Principal.toText(buy) # ")" #
          " Sell_random=" # Nat.toText(sellRandom) # "/" # Nat.toText(totalSellWeight) #
          " Buy_random=" # Nat.toText(buyRandom) # "/" # Nat.toText(totalBuyWeight),
          "selectTradingPair"
        );
      };
      case _ {
        logger.warn("PAIR_SELECTION", 
          "Failed to select valid pair - Sell_selected=" # debug_show(selectedSell) #
          " Buy_selected=" # debug_show(selectedBuy),
          "selectTradingPair"
        );
      };
    };

    switch (selectedSell, selectedBuy) {
      case (?sell, ?buy) { ?(sell, buy) };
      case _ { null };
    };
  };

  /**
   * Calculate trade size based on min and max trade value
   */
  private func calculateTradeSizeMinMax() : Nat {
    let range = rebalanceConfig.maxTradeValueICP - rebalanceConfig.minTradeValueICP;
    let randomOffset = fuzz.nat.randomRange(0, range);
    rebalanceConfig.minTradeValueICP + randomOffset
  };

  /**
   * Determine whether to use exact targeting or random trade sizing
   * 
   * Uses exact targeting when either token is close enough to target
   * that a max trade size would overshoot significantly.
   */
  private func shouldUseExactTargeting(
    sellTokenDiffBasisPoints: Int,
    buyTokenDiffBasisPoints: Int, 
    totalPortfolioValueICP: Nat
  ) : Bool {
    if (totalPortfolioValueICP == 0) { return false };
    
    // Calculate what 50% of max trade size represents in basis points
    // This ensures exact targeting activates before a max trade would overshoot
    let halfMaxTradeValueBasisPoints = (rebalanceConfig.maxTradeValueICP * 10000 / 2) / totalPortfolioValueICP;
    
    // Use exact targeting if either token is within 50% of max trade size of target
    let sellTokenCloseToTarget = Int.abs(sellTokenDiffBasisPoints) <= halfMaxTradeValueBasisPoints;
    let buyTokenCloseToTarget = Int.abs(buyTokenDiffBasisPoints) <= halfMaxTradeValueBasisPoints;
    
    sellTokenCloseToTarget or buyTokenCloseToTarget
  };

  /**
   * Calculate exact trade size to bring one token to its target allocation
   * 
   * Chooses which token to target exactly (prefers the one closer to target)
   * and calculates the precise trade size needed.
   */
  private func calculateExactTargetTradeSize(
    sellToken: Principal,
    buyToken: Principal, 
    totalPortfolioValueICP: Nat,
    sellTokenDiffBasisPoints: Nat,
    buyTokenDiffBasisPoints: Nat
  ) : Nat {
    
    // Choose which token to target exactly (prefer the one closer to target)
    let targetSellToken = Int.abs(sellTokenDiffBasisPoints) <= Int.abs(buyTokenDiffBasisPoints);
    
    if (targetSellToken) {
      // Calculate exact amount to sell to reach sell token's target
      // sellTokenDiffBasisPoints is negative (overweight), so we need Int.abs
      let excessValueICP = (Int.abs(sellTokenDiffBasisPoints) * totalPortfolioValueICP) / 10000;
      
      let sellTokenDetails = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details };
        case null { 
          Debug.print("Warning: Sell token details not found for exact targeting, using random size");
          return calculateTradeSizeMinMax();
        };
      };
      
      let exactTradeSize = (excessValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP;
      
      // Ensure trade size is within reasonable bounds
      let tradeSizeICP = (exactTradeSize * sellTokenDetails.priceInICP) / (10 ** sellTokenDetails.tokenDecimals);
      if (tradeSizeICP < rebalanceConfig.minTradeValueICP) {
        Debug.print("Exact trade size too small, using minimum trade size");
        ((rebalanceConfig.minTradeValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP)
      } else if (tradeSizeICP > rebalanceConfig.maxTradeValueICP) {
        Debug.print("Exact trade size too large, using maximum trade size");  
        ((rebalanceConfig.maxTradeValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP)
      } else {
        exactTradeSize
      }
      
    } else {
      // Calculate exact amount to buy to reach buy token's target
      // buyTokenDiffBasisPoints is positive (underweight)
      let deficitValueICP : Nat = (buyTokenDiffBasisPoints * totalPortfolioValueICP) / 10000;
      
      let sellTokenDetails = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details };
        case null { 
          Debug.print("Warning: Sell token details not found for exact targeting, using random size");
          return calculateTradeSizeMinMax();
        };
      };
      
      let exactTradeSize : Nat = (deficitValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP;
      
      // Ensure trade size is within reasonable bounds
      let tradeSizeICP = (exactTradeSize * sellTokenDetails.priceInICP) / (10 ** sellTokenDetails.tokenDecimals);
      if (tradeSizeICP < rebalanceConfig.minTradeValueICP) {
        Debug.print("Exact trade size too small, using minimum trade size");
        ((rebalanceConfig.minTradeValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP)
      } else if (tradeSizeICP > rebalanceConfig.maxTradeValueICP) {
        Debug.print("Exact trade size too large, using maximum trade size");
        ((rebalanceConfig.maxTradeValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP)
      } else {
        exactTradeSize
      }
    }
  };

  /**
   * Calculate trade size based on portfolio value and rebalance period
   */
  private func calculateTradeSizeRebalancePeriod(totalPortfolioValue : Nat) : Nat {
    // Target complete rebalance over portfolioRebalancePeriodNS
    let intervalsInPeriod = rebalanceConfig.portfolioRebalancePeriodNS / rebalanceConfig.rebalanceIntervalNS;
    let baseTradeSize = totalPortfolioValue / intervalsInPeriod;

    // Ensure trade size is within configured bounds
    if (baseTradeSize < rebalanceConfig.minTradeValueICP) {
      rebalanceConfig.minTradeValueICP;
    } else if (baseTradeSize > rebalanceConfig.maxTradeValueICP) {
      rebalanceConfig.maxTradeValueICP;
    } else {
      baseTradeSize;
    };
  };

  /**
   * VERBOSE LOGGING: Log comprehensive portfolio state
   * 
   * Captures complete portfolio snapshot including:
   * - Total portfolio value in ICP and USD
   * - Individual token balances, prices, and values
   * - Token status (active/paused/sync failures)
   */
  private func logPortfolioState(context : Text) : async* () {
    var totalValueICP = 0;
    var totalValueUSD : Float = 0;
    var activeTokenCount = 0;
    var pausedTokenCount = 0;
    var syncFailureCount = 0;
    
    let tokenStates = Vector.new<Text>();
    
    // Calculate totals and collect per-token data
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      let rawBalance = details.balance;
      let decimals = details.tokenDecimals;
      let priceICP = details.priceInICP;
      let priceUSD = details.priceInUSD;
      
      // Calculate values
      let valueInICP = if (decimals > 0) {
        (rawBalance * priceICP) / (10 ** decimals);
      } else { 0 };
      let valueInUSD = if (decimals > 0) {
        priceUSD * Float.fromInt(rawBalance) / (10.0 ** Float.fromInt(decimals));
      } else { 0.0 };
      
      // Only include active, unpaused tokens in total
      if (details.Active and not isTokenPausedFromTrading(principal)) {
        totalValueICP += valueInICP;
        totalValueUSD += valueInUSD;
        activeTokenCount += 1;
      } else {
        if (details.isPaused) pausedTokenCount += 1;
        if (details.pausedDueToSyncFailure) syncFailureCount += 1;
        if (Map.get(tradingPauses, phash, principal) != null) pausedTokenCount += 1;
      };
      
      // Format balance for display - simple decimal conversion
      let formattedBalance = if (decimals > 0) {
        let actualBalance = Float.fromInt(rawBalance) / Float.fromInt(10 ** decimals);
        Float.toText(actualBalance);
      } else {
        Nat.toText(rawBalance);
      };
      
      // Create token state string (simplified without Float.format)
      let tokenState = details.tokenSymbol # " (" # Principal.toText(principal) # "): " #
        "Balance=" # formattedBalance # " " #
        "ICP_Value=" # Nat.toText(valueInICP / 100000000) # "." # Nat.toText((valueInICP % 100000000) / 1000000) # " " #
        "USD_Value=" # Float.toText(valueInUSD) # " " #
        "Price_ICP=" # Nat.toText(priceICP / 100000000) # "." # Nat.toText((priceICP % 100000000) / 1000000) # " " #
        "Price_USD=" # Float.toText(priceUSD) # " " #
        "Status=" # (if (details.Active) "Active" else "Inactive") # 
        (if (details.isPaused) "+Paused" else "") #
        (if (details.pausedDueToSyncFailure) "+SyncFail" else "") #
        " LastSync=" # Int.toText((now() - details.lastTimeSynced) / 1_000_000_000) # "s_ago";
      
      Vector.add(tokenStates, tokenState);
    };
    
    // Log portfolio summary (simplified without Float.format)
    logger.info("PORTFOLIO_STATE", 
      context # " - Portfolio Summary: " #
      "Total_ICP=" # Nat.toText(totalValueICP / 100000000) # "." # Nat.toText((totalValueICP % 100000000) / 1000000) # " " #
      "Total_USD=" # Float.toText(totalValueUSD) # " " #
      "Active_Tokens=" # Nat.toText(activeTokenCount) # " " #
      "Paused_Tokens=" # Nat.toText(pausedTokenCount) # " " #
      "Sync_Failed_Tokens=" # Nat.toText(syncFailureCount) # " " #
      "Total_Tokens=" # Nat.toText(Map.size(tokenDetailsMap)), 
      "logPortfolioState"
    );
    
    // Log individual token details
    for (tokenState in Vector.vals(tokenStates)) {
      logger.info("PORTFOLIO_STATE", context # " - Token: " # tokenState, "logPortfolioState");
    };
  };

  //=========================================================================
  // 7. EXCHANGE INTEGRATION
  //=========================================================================

  /**
   * Find the best execution route across exchanges
   *
   * Checks multiple DEXs to find the optimal execution:
   * - Best price (expected output)
   * - Minimum slippage
   * - Sufficient liquidity
   */
  private func findBestExecution(
    sellToken : Principal,
    buyToken : Principal,
    amountIn : Nat,
  ) : async* Result.Result<{ exchange : ExchangeType; expectedOut : Nat; slippage : Float }, Text> {
    try {
      var bestExchange : ?ExchangeType = null;
      var bestAmountOut : Nat = 0;
      var bestPriceSlippage : Float = 100.0; // Start with 100% as worse case

      // Check KongSwap
      // VERBOSE LOGGING: Exchange comparison start
      let sellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenSymbol };
        case null { return #err("Token details not found for sell token") };
      };
      let buySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
        case (?details) { details.tokenSymbol };
        case null { return #err("Token details not found for buy token") };
      };
      let sellDecimals = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenDecimals };
        case null { 8 };
      };
      
      logger.info("EXCHANGE_COMPARISON", 
        "Starting exchange comparison - Pair=" # sellSymbol # "/" # buySymbol #
        " Amount_in=" # Nat.toText(amountIn) # " (raw)" #
        " Amount_formatted=" # Float.toText(Float.fromInt(amountIn) / Float.fromInt(10 ** sellDecimals)) #
        " Max_slippage=" # Nat.toText(rebalanceConfig.maxSlippageBasisPoints) # "bp",
        "findBestExecution"
      );

      // Check KongSwap

      Debug.print("Checking KongSwap for " # sellSymbol # " -> " # buySymbol);
      
      // VERBOSE LOGGING: KongSwap quote attempt
      logger.info("EXCHANGE_COMPARISON", 
        "Requesting KongSwap quote - Pair=" # sellSymbol # "/" # buySymbol #
        " Amount=" # Nat.toText(amountIn),
        "findBestExecution"
      );
      
      try {
        let kongQuote = await KongSwap.getQuote(sellSymbol, buySymbol, amountIn);
        switch (kongQuote) {
          case (#ok(quote)) {
            if (quote.slippage <= Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0) {
              bestExchange := ? #KongSwap;
              bestAmountOut := quote.receive_amount;
              bestPriceSlippage := quote.slippage;
              Debug.print("KongSwap quote seems good: " # debug_show (quote));
              
              // VERBOSE LOGGING: KongSwap quote success
              logger.info("EXCHANGE_COMPARISON", 
                "KongSwap quote received - Amount_out=" # Nat.toText(quote.receive_amount) #
                " Slippage=" # Float.toText(quote.slippage) # "%" #
                " Price=" # Float.toText(quote.price) #
                " Status=ACCEPTED",
                "findBestExecution"
              );
            } else {
              logger.info("EXCHANGE_COMPARISON", 
                "KongSwap quote seems bad: " # debug_show (quote) # " as slippage: " # debug_show (quote.slippage),
                "findBestExecution"
              );
              Debug.print("KongSwap quote seems bad: " # debug_show (quote) # " as slippage: " # debug_show (quote.slippage));
            };
          };
          case (#err(e)) {
            Debug.print("KongSwap quote error: " # e);
            
            // VERBOSE LOGGING: KongSwap quote error
            logger.warn("EXCHANGE_COMPARISON", 
              "KongSwap quote failed - Error=" # e #
              " Status=REJECTED",
              "findBestExecution"
            );
          };
        };
      } catch (e) {
        Debug.print("KongSwap error: " # Error.message(e));
        
        // VERBOSE LOGGING: KongSwap exception
        logger.error("EXCHANGE_COMPARISON", 
          "KongSwap threw exception - Error=" # Error.message(e) #
          " Status=EXCEPTION",
          "findBestExecution"
        );
      };

      // Check ICPSwap
      Debug.print("Checking ICPSwap for pair");
      
      // VERBOSE LOGGING: ICPSwap pool check
      logger.info("EXCHANGE_COMPARISON", 
        "Checking ICPSwap pool - Pair=" # sellSymbol # "/" # buySymbol #
        " Pool_count=" # Nat.toText(Map.size(ICPswapPools)),
        "findBestExecution"
      );
      
      try {
        let poolResult = Map.get(ICPswapPools, hashpp, (sellToken, buyToken));

        switch (poolResult) {
          case (?poolData) {
            
            // VERBOSE LOGGING: ICPSwap pool found
            logger.info("EXCHANGE_COMPARISON", 
              "ICPSwap pool found - Pool_ID=" # Principal.toText(poolData.canisterId) #
              " Token0=" # poolData.token0.address # " Token1=" # poolData.token1.address #
              " Requesting_quote=true",
              "findBestExecution"
            );

            let quoteArgs = {
              poolId = poolData.canisterId;
              amountIn = amountIn;
              amountOutMinimum = 0;
              zeroForOne = if (sellToken == Principal.fromText(poolData.token0.address)) {
                true;
              } else { false };
            };

            let quoteResult = await ICPSwap.getQuote(quoteArgs);
            switch (quoteResult) {
              case (#ok(quote)) {
                if (quote.slippage < Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0) {
                  // If no quote yet or better than KongSwap
                  if (bestExchange == null or (quote.amountOut > bestAmountOut or test)) {
                    bestExchange := ? #ICPSwap;
                    bestAmountOut := quote.amountOut;
                    bestPriceSlippage := quote.slippage;
                    
                    // VERBOSE LOGGING: ICPSwap selected as best
                    logger.info("EXCHANGE_COMPARISON", 
                      "ICPSwap quote accepted as BEST - Amount_out=" # Nat.toText(quote.amountOut) #
                      " Slippage=" # Float.toText(quote.slippage) # "%" #
                      " Previous_best=" # (switch bestExchange { case null "NONE"; case _ "KONGSWAP" }) #
                      " Status=BEST_EXECUTION",
                      "findBestExecution"
                    );
                  } else {
                    // VERBOSE LOGGING: ICPSwap quote but not selected
                    logger.info("EXCHANGE_COMPARISON", 
                      "ICPSwap quote received but NOT selected - Amount_out=" # Nat.toText(quote.amountOut) #
                      " Slippage=" # Float.toText(quote.slippage) # "%" #
                      " Best_amount=" # Nat.toText(bestAmountOut) #
                      " Status=NOT_SELECTED",
                      "findBestExecution"
                    );
                  };
                } else {
                  logger.info("EXCHANGE_COMPARISON", 
                    "ICPSwap quote seems bad: " # debug_show (quote) # " as slippage: " # debug_show (quote.slippage),
                    "findBestExecution"
                  );
                  Debug.print("ICPSwap quote seems bad: " # debug_show (quote) # " as slippage: " # debug_show (quote.slippage));
                };
                Debug.print("ICPSwap quote: " # debug_show (quote));
              };
              case (#err(e)) {
                Debug.print("ICPSwap quote error: " # e);
                
                // VERBOSE LOGGING: ICPSwap quote error
                logger.warn("EXCHANGE_COMPARISON", 
                  "ICPSwap quote failed - Error=" # e #
                  " Status=QUOTE_ERROR",
                  "findBestExecution"
                );
              };
            };
          };
          case (_) {
            // VERBOSE LOGGING: ICPSwap pool not found
            logger.warn("EXCHANGE_COMPARISON", 
              "ICPSwap pool NOT found - Pair=" # sellSymbol # "/" # buySymbol #
              " Available_pools=" # Nat.toText(Map.size(ICPswapPools)) #
              " Status=NO_POOL",
              "findBestExecution"
            );
          };
        };
      } catch (e) {
        Debug.print("ICPSwap error: " # Error.message(e));
        
        // VERBOSE LOGGING: ICPSwap exception
        logger.error("EXCHANGE_COMPARISON", 
          "ICPSwap threw exception - Error=" # Error.message(e) #
          " Status=EXCEPTION",
          "findBestExecution"
        );
      };

      switch (bestExchange) {
        case (?exchange) {
          
          // VERBOSE LOGGING: Final exchange selection
          logger.info("EXCHANGE_COMPARISON", 
            "Exchange selection FINAL - Selected=" # debug_show(exchange) #
            " Amount_out=" # Nat.toText(bestAmountOut) #
            " Best_slippage=" # Float.toText(bestPriceSlippage) # "%" #
            " Status=SELECTED",
            "findBestExecution"
          );
          
          #ok({
            exchange = exchange;
            expectedOut = bestAmountOut;
            slippage = bestPriceSlippage;
          });
        };
        case null {
          
          // VERBOSE LOGGING: No exchange found
          logger.warn("EXCHANGE_COMPARISON", 
            "No viable exchange found - KongSwap_available=" # debug_show(Map.size(tokenDetailsMap) > 0) #
            " ICPSwap_pools=" # Nat.toText(Map.size(ICPswapPools)) #
            " Status=NO_EXECUTION_PATH",
            "findBestExecution"
          );
          
          #err("No viable execution path found");
        };
      };
    } catch (e) {
      #err("Error finding best execution: " # Error.message(e));
    };
  };

  /**
   * Execute a trade on the selected exchange
   *
   * Handles the complete trade flow with:
   * - Transfer of tokens to exchange (if needed)
   * - Execution of swap
   * - Withdrawal of tokens back to treasury
   * - Slippage protection
   */
  private func executeTrade(
    sellToken : Principal,
    buyToken : Principal,
    amountIn : Nat,
    exchange : ExchangeType,
    minAmountOut : Nat,
    idealAmountOut : Nat, // Original spot price for proper slippage calculation
  ) : async* Result.Result<TradeRecord, Text> {
    try {
      let startTime = now();
      Debug.print("Executing trade on " # debug_show (exchange));

      // VERBOSE LOGGING: Trade execution start
      let sellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenSymbol };
        case null { "UNKNOWN" };
      };
      let buySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
        case (?details) { details.tokenSymbol };
        case null { "UNKNOWN" };
      };
      let sellDecimals = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenDecimals };
        case null { 8 };
      };
      let buyDecimals = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
        case (?details) { details.tokenDecimals };
        case null { 8 };
      };
      
      logger.info("TRADE_EXECUTION", 
        "Trade execution STARTED - Exchange=" # debug_show(exchange) #
        " Pair=" # sellSymbol # "/" # buySymbol #
        " Amount_in=" # Nat.toText(amountIn) # " (raw)" #
        " Amount_formatted=" # Float.toText(Float.fromInt(amountIn) / Float.fromInt(10 ** sellDecimals)) #
        " Min_amount_out=" # Nat.toText(minAmountOut) # " (raw)" #
        " Min_amount_out_formatted=" # Float.toText(Float.fromInt(minAmountOut) / Float.fromInt(10 ** buyDecimals)) #
        " Timestamp=" # Int.toText(startTime),
        "executeTrade"
      );

      Debug.print("Min amount out: " # Nat.toText(minAmountOut));
      switch (exchange) {
        case (#KongSwap) {
          let sellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
            case (?details) { details.tokenSymbol };
            case null {
              return #err("Token details not found for sell token");
            };
          };
          let buySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
            case (?details) { details.tokenSymbol };
            case null {
              return #err("Token details not found for buy token");
            };
          };

          // VERBOSE LOGGING: KongSwap trade preparation
          let slippageTolerancePercent = Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0;
          let deadlineSeconds = (startTime + 300_000_000_000) / 1_000_000_000;
          
          logger.info("TRADE_EXECUTION", 
            "KongSwap trade preparation - Symbols=" # sellSymbol # "/" # buySymbol #
            " Slippage_tolerance=" # Float.toText(slippageTolerancePercent) # "%" #
            " Deadline=" # Int.toText(deadlineSeconds) # "s" #
            " Min_amount_out=" # Nat.toText(minAmountOut),
            "executeTrade"
          );

          let swapArgs : swaptypes.KongSwapParams = {
            token0_ledger = sellToken;
            token0_symbol = sellSymbol;
            token1_ledger = buyToken;
            token1_symbol = buySymbol;
            amountIn = amountIn;
            minAmountOut = minAmountOut;
            deadline = ?(startTime + 300_000_000_000); // 5 minutes
            recipient = null;
            txId = null; // Will be set by the swap
            slippageTolerance = Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0;
          };

          // VERBOSE LOGGING: KongSwap execution start
          logger.info("TRADE_EXECUTION", 
            "KongSwap execution STARTING - Parameters_set=true" #
            " Calling_executeTransferAndSwap=true",
            "executeTrade"
          );

          let swapResult = await KongSwap.executeTransferAndSwap(swapArgs, pendingTxs);
          switch (swapResult) {
            case (#ok(reply)) {
              
              // VERBOSE LOGGING: KongSwap trade success
              let actualSlippage = reply.slippage;
              let amountReceived = reply.receive_amount;
              let executionTime = now() - startTime;
              
              logger.info("TRADE_EXECUTION", 
                "KongSwap trade SUCCESS - Amount_received=" # Nat.toText(amountReceived) #
                " Expected_min=" # Nat.toText(minAmountOut) #
                " Actual_slippage=" # Float.toText(actualSlippage) # "%" #
                " Execution_time=" # Int.toText(executionTime / 1_000_000) # "ms" #
                " Status=COMPLETED",
                "executeTrade"
              );

              #ok({
                tokenSold = sellToken;
                tokenBought = buyToken;
                amountSold = amountIn;
                amountBought = reply.receive_amount;
                exchange = #KongSwap;
                timestamp = startTime;
                success = true;
                error = null;
                slippage = reply.slippage;
              });
            };
            case (#err(e)) {
              
              // VERBOSE LOGGING: KongSwap trade failure
              let executionTime = now() - startTime;
              
              logger.error("TRADE_EXECUTION", 
                "KongSwap trade FAILED - Error=" # e #
                " Execution_time=" # Int.toText(executionTime / 1_000_000) # "ms" #
                " Status=FAILED",
                "executeTrade"
              );
              
              #err("KongSwap trade failed: " # e);
            };
          };
        };
        case (#ICPSwap) {
          let poolResult = Map.get(ICPswapPools, hashpp, (sellToken, buyToken));

          switch (poolResult) {
            case (?poolData) {
              
              // VERBOSE LOGGING: ICPSwap pool validation
              logger.info("TRADE_EXECUTION", 
                "ICPSwap pool validation - Pool_ID=" # Principal.toText(poolData.canisterId) #
                " Token0=" # poolData.token0.address # " Token1=" # poolData.token1.address #
                " Checking_transfer_fee=true",
                "executeTrade"
              );

              let tokenDetails = Map.get(tokenDetailsMap, phash, sellToken);
              let tx_fee = switch (tokenDetails) {
                case (?details) { details.tokenTransferFee };
                case null { 
                  logger.error("TRADE_EXECUTION", 
                    "ICPSwap FAILED - Token details not found for sell token",
                    "executeTrade"
                  );
                  return #err("Token details not found") 
                };
              };

              if (tx_fee > amountIn) {
                logger.error("TRADE_EXECUTION", 
                  "ICPSwap FAILED - Transfer fee " # Nat.toText(tx_fee) # 
                  " exceeds amount " # Nat.toText(amountIn),
                  "executeTrade"
                );
                return #err("Token transfer fee " # Nat.toText(tx_fee) # " is greater than amount in " # Nat.toText(amountIn) # " for " # Principal.toText(sellToken));
              };

              // Prepare deposit params
              let depositParams : swaptypes.ICPSwapDepositParams = {
                poolId = poolData.canisterId;
                token = sellToken;
                amount = amountIn;
                fee = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
                  case (?details) { details.tokenTransferFee };
                  case null { return #err("Token details not found") };
                };
              };

              // Prepare swap params - minAmountOut already has correct slippage protection applied
              let swapParams : swaptypes.ICPSwapParams = {
                poolId = poolData.canisterId;
                amountIn = amountIn - tx_fee;
                minAmountOut = minAmountOut;
                zeroForOne = if (sellToken == Principal.fromText(poolData.token0.address)) {
                  true;
                } else { false };
              };

              // Optional withdraw params to withdraw all
              let withdrawParams : swaptypes.OptionalWithdrawParams = {
                token = buyToken;
                amount = null; // Withdraw all received tokens
              };

              // VERBOSE LOGGING: ICPSwap execution parameters
              logger.info("TRADE_EXECUTION", 
                "ICPSwap execution parameters - Amount_after_fee=" # Nat.toText(amountIn - tx_fee) #
                " Transfer_fee=" # Nat.toText(tx_fee) #
                " Min_amount_out=" # Nat.toText(minAmountOut) #
                " Zero_for_one=" # debug_show(swapParams.zeroForOne) #
                " Starting_transfer_deposit_swap_withdraw=true",
                "executeTrade"
              );

              let swapResult = await ICPSwap.executeTransferDepositSwapAndWithdraw(
                Principal.fromText(self),
                depositParams,
                swapParams,
                withdrawParams,
                tokenDetailsMap,
              );

              switch (swapResult) {
                case (#ok(result)) {
                  
                  // VERBOSE LOGGING: ICPSwap trade success
                  let executionTime = now() - startTime;
                  let actualAmountOut = result.swapAmount;
                  let effectiveSlippage = if (idealAmountOut > 0) {
                    // Calculate how much worse we did compared to the original quote
                    if (actualAmountOut < idealAmountOut) {
                      Float.fromInt(idealAmountOut - actualAmountOut) / Float.fromInt(idealAmountOut) * 100.0;
                    } else { 0.0 }; // We got more than expected, so no negative slippage
                  } else { 0.0 };
                  
                  logger.info("TRADE_EXECUTION", 
                    "ICPSwap trade SUCCESS - Amount_received=" # Nat.toText(actualAmountOut) #
                    " Expected_min=" # Nat.toText(minAmountOut) #
                    " Effective_slippage=" # Float.toText(effectiveSlippage) # "%" #
                    " Execution_time=" # Int.toText(executionTime / 1_000_000) # "ms" #
                    " Status=COMPLETED",
                    "executeTrade"
                  );

                  #ok({
                    tokenSold = sellToken;
                    tokenBought = buyToken;
                    amountSold = amountIn;
                    amountBought = result.swapAmount;
                    exchange = #ICPSwap;
                    timestamp = startTime;
                    success = true;
                    error = null;
                    slippage = 0;
                  });

                };
                case (#err(e)) {
                  
                  // VERBOSE LOGGING: ICPSwap trade failure
                  let executionTime = now() - startTime;
                  
                  logger.error("TRADE_EXECUTION", 
                    "ICPSwap trade FAILED - Error=" # e #
                    " Execution_time=" # Int.toText(executionTime / 1_000_000) # "ms" #
                    " Status=FAILED" #
                    " Attempting_immediate_recovery=true",
                    "executeTrade"
                  );
                  
                  // Attempt immediate recovery from the specific pool
                  try {
                    logger.info("TRADE_RECOVERY", 
                      "Starting immediate recovery from pool " # Principal.toText(poolData.canisterId) #
                      " for tokens " # Principal.toText(sellToken) # "/" # Principal.toText(buyToken),
                      "executeTrade"
                    );
                    
                    await* ICPSwap.recoverBalanceFromSpecificPool(
                      Principal.fromText(self),
                      poolData.canisterId,
                      [sellToken, buyToken],
                      tokenDetailsMap
                    );
                    
                    logger.info("TRADE_RECOVERY", 
                      "Immediate recovery completed for pool " # Principal.toText(poolData.canisterId),
                      "executeTrade"
                    );
                  } catch (recoveryError) {
                    logger.error("TRADE_RECOVERY", 
                      "Immediate recovery failed for pool " # Principal.toText(poolData.canisterId) #
                      " Error=" # Error.message(recoveryError),
                      "executeTrade"
                    );
                    // Don't change the original error - recovery failure is secondary
                  };
                  
                  #err("Failed to execute ICPSwap trade: " # e);
                };
              };
            };
            case (_) { 
              
              // VERBOSE LOGGING: ICPSwap pool not found
              logger.error("TRADE_EXECUTION", 
                "ICPSwap FAILED - Pool not found for pair " # sellSymbol # "/" # buySymbol #
                " Available_pools=" # Nat.toText(Map.size(ICPswapPools)),
                "executeTrade"
              );
              
              #err("ICPSwap pool not found") 
            };
          };
        };
      };
    } catch (e) {
      
      // VERBOSE LOGGING: Trade execution exception
      logger.error("TRADE_EXECUTION", 
        "Trade execution EXCEPTION - Error=" # Error.message(e) #
        " Exchange=" # debug_show(exchange) #
        " Status=EXCEPTION",
        "executeTrade"
      );
      
      #err("Trade execution error: " # Error.message(e));
    };
  };

  /**
   * Update and cache known ICPSwap pools
   *
   * Fetches all available pools and stores them for quick access.
   */
  private func updateICPSwapPools() : async () {
    Debug.print("Starting ICPSwap pool discovery");

    try {
      let poolsResult = await ICPSwap.getAllPools();

      switch (poolsResult) {
        case (#ok(pools)) {
          // Clear existing pool mappings
          Map.clear(ICPswapPools);

          for (pool in pools.vals()) {
            let token0Principal = Principal.fromText(pool.token0.address);
            let token1Principal = Principal.fromText(pool.token1.address);

            // Store pool data in both directions
            Map.set(
              ICPswapPools,
              hashpp,
              (token0Principal, token1Principal),
              pool,
            );
            Map.set(
              ICPswapPools,
              hashpp,
              (token1Principal, token0Principal),
              pool,
            );

            Debug.print(
              "Added pool " # Principal.toText(pool.canisterId)
              # " for " # pool.token0.address
              # " and " # pool.token1.address
            );
          };
        };
        case (#err(e)) {
          Debug.print("Error fetching pools: " # e);
        };
      };
    } catch (e) {
      Debug.print("Exception in pool discovery: " # Error.message(e));
    };
  };

  //=========================================================================
  // 8. DATA SYNCHRONIZATION & PRICE UPDATES
  //=========================================================================

  /**
   * Timer for data synchronization
   *
   * Schedules periodic updates of token info from:
   * - DAO (allocation targets and token status)
   * - NTN service (token prices)
   * - Ledgers (token balances)
   */
  // Split the sync functions into short and long loops
  private func startShortSyncTimer<system>(instant : Bool) {
    if (shortSyncTimerId != 0) {
      cancelTimer(shortSyncTimerId);
    };

    shortSyncTimerId := setTimer<system>(
      #nanoseconds(if (instant) { 0 } else { rebalanceConfig.shortSyncIntervalNS }),
      func() : async () {
        try {
          await syncFromDAO();
          await updateBalances();
          try {
            await* syncPriceWithNTN();
          } catch (_) {};
          try {
            ignore await dao.syncTokenDetailsFromTreasury(Iter.toArray(Map.entries(tokenDetailsMap)));
          } catch (_) {};
          for ((token, details) in Map.entries(tokenDetailsMap)) {
            if ((details.lastTimeSynced + rebalanceConfig.tokenSyncTimeoutNS) < now()) {
              Map.set(tokenDetailsMap, phash, token, { details with pausedDueToSyncFailure = true });
            };
          };
          // Schedule next sync
          startShortSyncTimer<system>(false);
        } catch (_) {
          retryFunc<system>(50, 15, #shortSync);
        };
      },
    );
  };

  public shared ({ caller }) func admin_syncWithDao() : async Result.Result<Text, Text> {

    if (((await hasAdminPermission(caller, #startRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      Debug.print("Not authorized to execute trading cycle: " # debug_show(caller));
      return #err("Not authorized");
    };

    try {
      Debug.print("Debug sync DAO");
      await syncFromDAO();
      Debug.print("Update balances");
      await updateBalances();
      try {
        Debug.print("Sync price with NTN");
        await* syncPriceWithNTN();
      } catch (_) {};
      try {
        Debug.print("Sync token details to DAO");
        ignore await dao.syncTokenDetailsFromTreasury(Iter.toArray(Map.entries(tokenDetailsMap)));
      } catch (_) {};
      for ((token, details) in Map.entries(tokenDetailsMap)) {
        Debug.print("Check token details sync failure for " # Principal.toText(token));
        if ((details.lastTimeSynced + rebalanceConfig.tokenSyncTimeoutNS) < now()) {
          Map.set(tokenDetailsMap, phash, token, { details with pausedDueToSyncFailure = true });
        };
      };
      return #ok("Synced with DAO");
    } catch (e) {
      return #err("Error syncing with DAO: " # Error.message(e));
    };
  };


  private func startLongSyncTimer<system>(instant : Bool) {
    if (longSyncTimerId != 0) {
      cancelTimer(longSyncTimerId);
    };

    longSyncTimerId := setTimer<system>(
      #nanoseconds(if (instant) { 1_000_000 } else { rebalanceConfig.longSyncIntervalNS }),
      func() : async () {
        try {
          // Run less frequent maintenance tasks
          var err = false;
          try {
            await updateTokenMetadata();
          } catch (_) {
            err := true;
          };
          try {
            await updateICPSwapPools();
          } catch (_) {
            err := true;
          };
          try {
            ignore await recoverPoolBalances();
          } catch (_) {
            err := true;
          };
          if (err) {
            retryFunc<system>(50, 15, #longSync);
          } else {
            startLongSyncTimer<system>(false);
          };
        } catch (_) {
          retryFunc<system>(50, 15, #longSync);
        };
      },
    );
  };

  private func startAllSyncTimers<system>(instant : Bool) {
    startShortSyncTimer<system>(instant);
    startLongSyncTimer<system>(instant);
  };

  /**
   * Fetch allocation targets and token details from DAO
   *
   * Gets the latest target allocations and token metadata
   * from the DAO canister.
   */
  private func syncFromDAO() : async () {
    // Run token details and allocation fetches in parallel

    let tokenDetailsFuture = dao.getTokenDetails();
    let allocationFuture = dao.getAggregateAllocation();
    let tokenDetailsResult = await tokenDetailsFuture;
    let allocationResult = await allocationFuture;
    Debug.print("Token details result: " # debug_show (tokenDetailsResult));
    // Update token info map
    for ((principal, details) in tokenDetailsResult.vals()) {
      Debug.print("Updating token details for " # Principal.toText(principal));
      let oldDetails = Map.get(tokenDetailsMap, phash, principal);
      switch (oldDetails) {
        case null {
          Map.set(
            tokenDetailsMap,
            phash,
            principal,
            details,
          );
        };
        case (?(oldDetails)) {
          Map.set(
            tokenDetailsMap,
            phash,
            principal,
            {
              oldDetails with Active = details.Active;
              isPaused = details.isPaused;
              pausedDueToSyncFailure = false;
              lastTimeSynced = now();
            },
          );
        };
      };
    };

    // Update allocations map
    Map.clear(currentAllocations);
    for ((principal, allocation) in allocationResult.vals()) {
      Map.set(currentAllocations, phash, principal, allocation);
    };
  };

  /**
   * Synchronize token details with DAO
   *
   * Updates token status from the DAO including:
   * - Active/Inactive status
   * - Paused/Unpaused state
   */ 
  public shared ({ caller }) func syncTokenDetailsFromDAO(tokenDetails : [(Principal, TokenDetails)]) : async Result.Result<Text, SyncErrorTreasury> {
    Debug.print("Sync token details from DAO");
    if (caller != DAOPrincipal) {
      Debug.print("Not authorized to sync token details from DAO");
      return #err(#NotDAO);
    };
    let timenow = now();
    for (tokenDetails in tokenDetails.vals()) {
      let token = tokenDetails.0;
      let details = tokenDetails.1;
      Debug.print("Sync token details from DAO for " # Principal.toText(token));
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case null {
          Map.set(tokenDetailsMap, phash, token, details);
        };
        case (?currentDetails) {
          Map.set(
            tokenDetailsMap,
            phash,
            token,
            {
              currentDetails with
              Active = details.Active;
              isPaused = details.isPaused;
              epochAdded = details.epochAdded;
              tokenType = details.tokenType;
              pausedDueToSyncFailure = false;
              lastTimeSynced = timenow;
            },
          );
        };
      };
    };
    Debug.print("Start all sync timers");
    startAllSyncTimers<system>(true);
    Debug.print("Token details synced successfully");
    #ok("Token details synced successfully");
  };

  // Helper function to update token price and maintain history
  private func updateTokenPriceWithHistory(token : Principal, priceInICP : Nat, priceInUSD : Float) {
    switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) {
        // Only add to history if price has changed        
        if (details.priceInICP != priceInICP or details.priceInUSD != priceInUSD) {
          let timestamp = now();

          let newPriceHistory = Vector.fromArray<PricePoint>(details.pastPrices);

          // Add new price point
          Vector.add(
            newPriceHistory,
            {
              icpPrice = priceInICP;
              usdPrice = priceInUSD;
              time = timestamp;
            },
          );

          // Check if we need to remove oldest entries
          if (Vector.size(newPriceHistory) > MAX_PRICE_HISTORY_ENTRIES) {
            Vector.reverse(newPriceHistory);
            while (Vector.size(newPriceHistory) > MAX_PRICE_HISTORY_ENTRIES) {
              ignore Vector.removeLast(newPriceHistory);
            };
            Vector.reverse(newPriceHistory);
          };

          // Update token details with new price and history
          Map.set(
            tokenDetailsMap,
            phash,
            token,
            {
              details with
              priceInICP = priceInICP;
              priceInUSD = priceInUSD;
              pastPrices = Vector.toArray(newPriceHistory);
              lastTimeSynced = now();
            },
          );

          // Check price failsafe conditions after updating the history
          checkPriceFailsafeConditions(token, priceInICP, Vector.toArray(newPriceHistory));
        };
      };
      case null {};
    };
  };

  /**
   * Update token prices from NTN service
   *
   * Fetches latest price data from NTN, including
   * USD prices and converts to ICP-denominated prices.
   */
  private func syncPriceWithNTN() : async* () {
    let allTokensNTN = try { await priceCheckNTN.getAllTokens() } catch (_) {
      if test {
        for (t in [(ICPprincipal, 100000000, 20.0), (Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"), 20000000, 4.0), (Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai"), 50000000, 10.0), (Principal.fromText("kknbx-zyaaa-aaaaq-aae4a-cai"), 1000000, 0.2), (Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai"), 5000000, 1.0)].vals()) {
          switch (Map.get(tokenDetailsMap, phash, t.0)) {
            case (?_) {
              // Update price with history
              updateTokenPriceWithHistory(t.0, t.1, t.2);
            };
            case null {};
          };
        };
      };
      return ();
    };

    let allTokensNTNmap = Map.fromIter<Text, NTN.PublicTokenOverview>(Array.map<NTN.PublicTokenOverview, (Text, NTN.PublicTokenOverview)>(allTokensNTN, func(token) = (token.address, token)).vals(), thash);
    let ICPprice = switch (Map.get(allTokensNTNmap, thash, "ryjl3-tyaaa-aaaaa-aaaba-cai")) {
      case null {
        return;
      };
      case (?tokenDetails) { tokenDetails.priceUSD };
    };

    label a for ((principal, details) in Iter.toArray(Map.entries(tokenDetailsMap)).vals()) {
      let usdPrice = switch (Map.get(allTokensNTNmap, thash, Principal.toText(principal))) {
        case null { continue a; 0.0 };
        case (?tokenDetails) { tokenDetails.priceUSD };
      };

      let icpPrice = Float.toInt((usdPrice * (10.0 ** Float.fromInt(details.tokenDecimals))) / ICPprice);
      // testing, remove this
      //if (principal == Principal.fromText("k45jy-aiaaa-aaaaq-aadcq-cai")) {
      //  updateTokenPriceWithHistory(principal, Int.abs(icpPrice) * 2, usdPrice * 2.0);
      //} else {
        updateTokenPriceWithHistory(principal, Int.abs(icpPrice), usdPrice);
      //};
    };

    rebalanceState := {
      rebalanceState with
      metrics = {
        rebalanceState.metrics with
        lastPriceUpdate = now();
      };
    };

    // Take portfolio snapshot after price updates
    await takePortfolioSnapshot(#PriceUpdate);

    // Check portfolio circuit breaker conditions after price updates and snapshot
    checkPortfolioCircuitBreakerConditions();
  };

  /**
   * Update token metadata (name, symbol, decimals, fees)
   *
   * Gets the latest token metadata from their ledger canisters
   */
  private func updateTokenMetadata() : async () {
    let metadataFutures = Map.new<Principal, async [ICRC1.MetaDatum]>();

    // Collect all metadata fetch futures
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (principal != ICPprincipal) {
        let token = actor (Principal.toText(principal)) : ICRC1.FullInterface;
        let metadataFuture = token.icrc1_metadata();
        Map.set(metadataFutures, phash, principal, metadataFuture);
      };
    };

    // Process futures and update tokenDetailsMap
    label a for ((principal, futureMetadata) in Map.entries(metadataFutures)) {
      try {
        let metadata = await futureMetadata;

        // Initialize with defaults
        var name = "";
        var symbol = "";
        var decimals = 0;
        var fee = 0;

        // Process metadata entries
        for ((key, value) in metadata.vals()) {
          switch (key, value) {
            case ("icrc1:name", #Text(val)) { name := val };
            case ("icrc1:symbol", #Text(val)) { symbol := val };
            case ("icrc1:decimals", #Nat(val)) { decimals := val };
            case ("icrc1:decimals", #Int(val)) { decimals := Int.abs(val) };
            case ("icrc1:fee", #Nat(val)) { fee := val };
            case ("icrc1:fee", #Int(val)) { fee := Int.abs(val) };
            case _ { /* ignore other fields */ };
          };
        };
        let currentDetails = switch (Map.get(tokenDetailsMap, phash, principal)) {
          case null { continue a };
          case (?details) { details };
        };

        // Update token details with new metadata while preserving other fields
        Map.set(
          tokenDetailsMap,
          phash,
          principal,
          {
            currentDetails with
            // Update metadata fields
            tokenName = name;
            tokenSymbol = symbol;
            tokenDecimals = decimals;
            tokenTransferFee = fee;
          },
        );
      } catch (e) {
        Debug.print("Error updating metadata for token " # Principal.toText(principal) # ": " # Error.message(e));
      };
    };
  };

  /**
   * Update current token balances
   *
   * Queries all token ledgers for current balances
   */
  private func updateBalances() : async () {
    let balanceFutures = Map.new<Principal, async Nat>();

    let ledger = actor (ICPprincipalText) : Ledger.Interface;
    let ICPbalanceFuture = ledger.account_balance({
      account = Principal.toLedgerAccount(Principal.fromText(self), null);
    });

    // Collect all balance fetch futures
    for ((principal, _) in Map.entries(tokenDetailsMap)) {
      if (principal != ICPprincipal) {
        // For ICRC1 tokens
        let token = actor (Principal.toText(principal)) : ICRC1.FullInterface;
        let balanceFuture = token.icrc1_balance_of({
          owner = Principal.fromText(self);
          subaccount = null;
        });
        Debug.print("Updating balance for token " # Principal.toText(principal));
        Map.set(balanceFutures, phash, principal, balanceFuture);
      };
    };

    // Process futures and update tokenDetailsMap
    label a for ((principal, futureBalance) in Map.entries(balanceFutures)) {
      try {
        let balance = await futureBalance;
        Debug.print("Balance for token " # Principal.toText(principal) # ": " # Nat.toText(balance));
        let currentDetails = switch (Map.get(tokenDetailsMap, phash, principal)) {
          case null { continue a };
          case (?details) { details };
        };

        // Update token details with new balance while preserving other fields
        Map.set(
          tokenDetailsMap,
          phash,
          principal,
          {
            currentDetails with
            balance = balance;
          },
        );
      } catch (e) {
        Debug.print("Error updating balance for token " # Principal.toText(principal) # ": " # Error.message(e));
      };
    };
    try {
      let ICPbalance = await ICPbalanceFuture;
      switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
        case null {};
        case (?details) {
          Map.set(tokenDetailsMap, phash, ICPprincipal, { details with balance = Nat64.toNat(ICPbalance.e8s) });
        };
      };
    } catch (e) {
      Debug.print("Error updating ICP balance: " # Error.message(e));
    };
  };

  private func hasAdminPermission(caller : Principal, permission : SpamProtection.AdminFunction) : async Bool {
    if (caller == DAOPrincipal or Principal.isController(caller)) { return true; };
    // call DAO canister to ask if caller is admin
    await dao.hasAdminPermission(caller, permission);
  };

  //=========================================================================
  // 9. RECOVERY & SAFETY SYSTEMS
  //=========================================================================

  public shared ({ caller }) func admin_recoverPoolBalances() : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    await recoverPoolBalances();
  };

  /**
   * Recovery of forgotten balances from ICPSwap pools
   *
   * Checks for and retrieves any unused balances in pools.
   */
  private func recoverPoolBalances() : async Result.Result<Text, Text> {
    Debug.print("Starting recovery of unused pool balances");
    try {
      // Get all active tokens from tokenDetailsMap
      let allTokens = Vector.new<Principal>();
      for ((principal, details) in Map.entries(tokenDetailsMap)) {
        Vector.add(allTokens, principal);
      };

      // Create a list of pools with their data
      let poolsWithData = Vector.new<(Principal, Principal, ?swaptypes.PoolData)>();
      let tokenArray = Vector.toArray(allTokens);

      // Check if each possible pair has a pool in ICPswapPools
      for (i in Iter.range(0, tokenArray.size() - 1)) {
        for (j in Iter.range(i + 1, tokenArray.size() - 1)) {
          let token0 = tokenArray[i];
          let token1 = tokenArray[j];

          // Check if the pair exists in the map and include its data
          let poolData = Map.get(ICPswapPools, hashpp, (token0, token1));
          if (poolData != null) {
            Vector.add(poolsWithData, (token0, token1, poolData));
          };
        };
      };

      // Execute recovery on all identified pools, passing the pool data we have
      await* ICPSwap.recoverBalancesFromPools(Principal.fromText(self), Vector.toArray(poolsWithData), tokenDetailsMap);
      #ok("Successfully processed " # Nat.toText(Vector.size(poolsWithData)) # " pools");
    } catch (e) {
      Debug.print("Error in recoverPoolBalances: " # Error.message(e));
      #err("Failed to recover balances: " # Error.message(e));
    };
  };

  /**
   * System recovery after failure
   *
   * Attempts to recover from trading failures by:
   * 1. Syncing prices from NTN
   * 2. Resetting system state
   * 3. Restarting trading timer
   */
  private func recoverFromFailure() : async* () {
    Debug.print("Attempting system recovery");
    try {
      await* syncPriceWithNTN();
    } catch (e) {
      Debug.print("Recovery failed: " # Error.message(e));
    };

    rebalanceState := {
      rebalanceState with
      status = #Trading;
    };

    // Resume normal operation
    startTradingTimer<system>();
  };

  /**
   * Retry mechanism for failed operations
   *
   * Handles errors in timer operations with exponential backoff
   */
  private func retryFunc<system>(maxRetries : Nat, initialDelay : Nat, timerType : { #transfer; #shortSync; #longSync }) : () {
    if (maxRetries == 0) {
      if (timerType == #transfer) {
        let timersize = Vector.size(transferTimerIDs);
        if (timersize > 0) {
          for (i in Iter.range(0, timersize - 1)) {
            cancelTimer(Vector.get(transferTimerIDs, i));
          };
        };
        Vector.clear(transferTimerIDs);
      } else if (timerType == #shortSync) {
        if (rebalanceState.status != #Idle) {
          ignore setTimer<system>(
            #seconds(0),
            func() : async () {
              ignore await stopRebalancing(?"Auto-stop via timer");
            },
          );
        };
        startShortSyncTimer<system>(false);
      } else if (timerType == #longSync) {
        startLongSyncTimer<system>(false);
      };
      return;
    };

    if (timerType == #transfer) {
      // Transfer timer retry logic (unchanged)
      let timersize = Vector.size(transferTimerIDs);
      if (timersize > 0) {
        for (i in Iter.range(0, timersize - 1)) {
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
              if test {
                await transferTimer(true);
              } else {
                await transferTimer(false);
              };
            } catch (_) {
              retryFunc<system>(maxRetries - 1, initialDelay, timerType);
            };
          },
        ),
      );
    } else if (timerType == #shortSync) {
      // Short sync timer retry logic
      if (shortSyncTimerId != 0) {
        cancelTimer(shortSyncTimerId);
      };
      shortSyncTimerId := setTimer<system>(
        #seconds(initialDelay),
        func() : async () {
          try {
            await syncFromDAO();
            await updateBalances();
            try {
              await* syncPriceWithNTN();
            } catch (_) {};
            for ((token, details) in Map.entries(tokenDetailsMap)) {
              if ((details.lastTimeSynced + rebalanceConfig.tokenSyncTimeoutNS) < now()) {
                Map.set(tokenDetailsMap, phash, token, { details with pausedDueToSyncFailure = true });
              };
            };
            // Schedule next sync
            startShortSyncTimer<system>(false);
          } catch (_) {
            retryFunc<system>(maxRetries - 1, initialDelay, timerType);
          };
        },
      );
    } else if (timerType == #longSync) {
      // Long sync timer retry logic
      if (longSyncTimerId != 0) {
        cancelTimer(longSyncTimerId);
      };
      longSyncTimerId := setTimer<system>(
        #seconds(initialDelay),
        func() : async () {
          try {
            // Run less frequent maintenance tasks
            var err = false;
            try {
              await updateTokenMetadata();
            } catch (_) {
              err := true;
            };
            try {
              await updateICPSwapPools();
            } catch (_) {
              err := true;
            };
            try {
              ignore await recoverPoolBalances();
            } catch (_) {
              err := true;
            };
            if (err) {
              throw Error.reject("Failed to recover from failure");
            } else {
              startLongSyncTimer<system>(false);
            };
          } catch (_) {
            retryFunc<system>(maxRetries - 1, initialDelay, timerType);
          };
        },
      );
    };
  };

  //=========================================================================
  // 10. SYSTEM INITIALIZATION
  //=========================================================================

  // Initialize sync timer at system startup
  startAllSyncTimers<system>(true);

  //=========================================================================
  // LOG ACCESS METHODS
  //=========================================================================

  /**
   * Get the last N log entries
   * Only accessible by master admin, controller, or DAO
   */
  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal) {
      logger.getLastLogs(count);
    } else {
      [];
    };
  };

  /**
   * Get the last N log entries for a specific context
   * Only accessible by master admin, controller, or DAO
   */
  public query ({ caller }) func getLogsByContext(context : Text, count : Nat) : async [Logger.LogEntry] {
    if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal) {
      logger.getContextLogs(context, count);
    } else {
      [];
    };
  };

  /**
   * Get the last N log entries for a specific level
   * Only accessible by master admin, controller, or DAO
   */
  public query ({ caller }) func getLogsByLevel(level : Logger.LogLevel, count : Nat) : async [Logger.LogEntry] {
    if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal) {
      logger.getLogsByLevel(level, count);
    } else {
      [];
    };
  };

  /**
   * Clear all logs
   * Only accessible by master admin or controller
   */
  public shared ({ caller }) func clearLogs() : async () {
    if (isMasterAdmin(caller) or Principal.isController(caller)) {
      logger.info("System", "Logs cleared by: " # Principal.toText(caller), "clearLogs");
      logger.clearLogs();
      logger.clearContextLogs("all");
    };
  };

  /**
   * Get current maximum price history entries configuration
   * 
   * Returns the current limit for price history entries per token.
   * Accessible by any user with query access.
   */
  public query func getMaxPriceHistoryEntries() : async Nat {
    MAX_PRICE_HISTORY_ENTRIES;
  };

/* NB: Turn on again after initial setup
  // Security check for message inspection
  system func inspect({
    arg : Blob;
    caller : Principal;
    msg : {
      #admin_executeTradingCycle : () -> ();
      #admin_recoverPoolBalances : () -> ();
      #admin_syncWithDao : () -> ();
      #clearLogs : () -> ();
      #getCurrentAllocations : () -> ();
      #getLogs : () -> (count : Nat);
      #getLogsByContext : () -> (context : Text, count : Nat);
      #getLogsByLevel : () -> (level : Logger.LogLevel, count : Nat);
      #getTokenDetails : () -> ();
      #getTradingStatus : () -> ();
      #getMaxPortfolioSnapshots : () -> ();
      #receiveTransferTasks : () -> ([(TransferRecipient, Nat, Principal, Nat8)], Bool);
      #setTest : () -> Bool;
      #startRebalancing : () -> ();
      #stopRebalancing : () -> ();
      #resetRebalanceState : () -> ();
      #syncTokenDetailsFromDAO : () -> [(Principal, TokenDetails)];
      #updateRebalanceConfig : () -> (UpdateConfig, ?Bool, ?Text);
      #updateMaxPortfolioSnapshots : () -> ();
      #getMaxPriceHistoryEntries : () -> Nat;
      #getTokenPriceHistory : () -> [Principal];
      #getSystemParameters : () -> ();
    };
  }) : Bool {
     (isMasterAdmin(caller) or Principal.isController(caller)) and arg.size() < 50000;
  };
  */

  // Query method for treasury admin actions since timestamp (for archiving)
  public shared query ({ caller }) func getTreasuryAdminActionsSince(
    sinceTimestamp: Int, 
    limit: Nat
  ) : async Result.Result<TreasuryAdminActionsSinceResponse, TradingPauseError> {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal)) {
      return #err(#NotAuthorized);
    };

    if (limit == 0 or limit > 1000) {
      return #err(#SystemError("Invalid limit: must be between 1 and 1000"));
    };

    let allActions = Vector.toArray(treasuryAdminActions);
    var filteredActions = Array.filter<TreasuryAdminActionRecord>(allActions, func(action) {
      action.timestamp > sinceTimestamp
    });
    
    // Sort by timestamp (oldest first for proper archive ordering)
    filteredActions := Array.sort(filteredActions, func(a: TreasuryAdminActionRecord, b: TreasuryAdminActionRecord) : Order.Order {
      Int.compare(a.timestamp, b.timestamp)
    });
    
    let totalFilteredCount = filteredActions.size();
    let limitedActions = if (totalFilteredCount > limit) {
      Array.subArray(filteredActions, 0, limit)
    } else {
      filteredActions
    };

    #ok({
      actions = limitedActions;
      totalCount = totalFilteredCount;
    })
  };

};

