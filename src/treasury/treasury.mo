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
    Principal.fromText("qgjut-u3ase-3lxef-vcxtr-4g6jb-mazlw-jpins-wrvpv-jt5wn-2nrx6-sae"),
    Principal.fromText("as6jn-gaoo7-k4kji-tdkxg-jlsrk-avxkc-zu76j-vz7hj-di3su-2f74z-qqe"),
    Principal.fromText("r27hb-ckxon-xohqv-afcvx-yhemm-xoggl-37dg6-sfyt3-n6jer-ditge-6qe"), // staging identities
    Principal.fromText("k2xol-5avzc-lf3wt-vwoft-pjx6k-77fjh-7pera-6b7qt-fwt5e-a3ekl-vqe"),
    Principal.fromText("qgjut-u3ase-3lxef-vcxtr-4g6jb-mazlw-jpins-wrvpv-jt5wn-2nrx6-sae"),
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

  // Rebalancing state
  stable var rebalanceState : RebalanceState = {
    status = #Idle;
    config = rebalanceConfig;
    metrics = {
      lastPriceUpdate = 0;
      lastRebalanceAttempt = 0;
      totalTradesExecuted = 0;
      totalTradesFailed = 0;
      currentStatus = #Idle;
      portfolioValueICP = 0;
      portfolioValueUSD = 0;
    };
    lastTrades = Vector.new<TradeRecord>();
    priceUpdateTimerId = null;
    rebalanceTimerId = null;
  };

  stable var MAX_PRICE_HISTORY_ENTRIES = 100;

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

  private func isMasterAdmin(caller : Principal) : Bool {
    for (admin in masterAdmins.vals()) {
      if (admin == caller) {
        return true;
      };
    };
    false;
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
  public shared ({ caller }) func startRebalancing() : async Result.Result<Text, RebalanceError> {
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
      Debug.print("Rebalancing started");
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
      #err(#SystemError("Failed to start trading: " # Error.message(e)));
    };
  };

  /**
   * Stop the automatic rebalancing process
   *
   * Cancels all timers and sets the system to idle state
   * Only callable by DAO or controller.
   */
  public shared ({ caller }) func stopRebalancing() : async Result.Result<Text, RebalanceError> {
    if (((await hasAdminPermission(caller, #stopRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      Debug.print("Not authorized to stop rebalancing: " # debug_show(caller));
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
    #ok("Rebalancing stopped");
  };

  /**
   * Update the rebalancing configuration parameters
   *
   * Allows adjustment of trading intervals, sizes, and safety limits
   * Only callable by DAO or controller.
   */
  public shared ({ caller }) func updateRebalanceConfig(updates : UpdateConfig, rebalanceStateNew : ?Bool) : async Result.Result<Text, RebalanceError> {
    if (((await hasAdminPermission(caller, #updateTreasuryConfig)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
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
        } else if (value > 86400_000_000) {
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
        } else if (value > 500) {
          validationErrors #= "Maximum price history entries cannot be more than 500; ";
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
      return #err(#ConfigError(validationErrors));
    };

    // If no changes were requested, return early
    if (not hasChanges) {
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
            ignore await startRebalancing();
          } else if (not value and rebalanceState.status != #Idle) {
            ignore await stopRebalancing();
          };
        };
        case null {};
      };
    } catch (_) {

    };

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
  public shared query func getTradingStatus() : async Result.Result<{ rebalanceStatus : RebalanceStatus; executedTrades : [TradeRecord]; portfolioState : { totalValueICP : Nat; totalValueUSD : Float; currentAllocations : [(Principal, Nat)]; targetAllocations : [(Principal, Nat)] }; metrics : { lastUpdate : Int; totalTradesExecuted : Nat; totalTradesFailed : Nat; avgSlippage : Float; successRate : Float } }, Text> {

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

    let successRate = if (rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed > 0) {
      Float.fromInt(rebalanceState.metrics.totalTradesExecuted) / Float.fromInt(rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed);
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
        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted;
        totalTradesFailed = rebalanceState.metrics.totalTradesFailed;
        avgSlippage = avgSlippage;
        successRate = successRate;
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
   * Get all token details including balances and prices
   */
  public query func getTokenDetails() : async [(Principal, TokenDetails)] {
    Iter.toArray(Map.entries(tokenDetailsMap));
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
    assert (caller == DAOPrincipal);
    test := a;
    Debug.print("Test is set");
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

  public shared ({ caller }) func admin_executeTradingCycle() : async Result.Result<Text, RebalanceError> {
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
   * 1. Retries any failed transactions
   * 2. Executes the trading step
   * 3. Recovers from failures if needed
   */
  private func executeTradingCycle() : async* () {
    if (rebalanceState.status == #Idle) {
      return;
    };

    await* do_executeTradingCycle();
  };

  private func do_executeTradingCycle() : async* () {

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
          " Max_Trade_Value=" # Nat.toText(rebalanceConfig.maxTradeValueICP / 100000000) # "ICP" #
          " Max_Slippage=" # Nat.toText(rebalanceConfig.maxSlippageBasisPoints) # "bp",
          "do_executeTradingStep"
        );

        Debug.print("Calculating trade requirements...");
        let tradeDiffs = calculateTradeRequirements();

        Debug.print("Trade diffs: " # debug_show (tradeDiffs));

        Debug.print("Selecting trading pair...");
        switch (selectTradingPair(tradeDiffs)) {
          case (?(sellToken, buyToken)) {
            // only needed if we want to use calculateTradeSizeRebalancePeriod.
            //var totalValueICP = 0;
            //for ((principal, details) in Map.entries(tokenDetailsMap)) {
            //  if (details.Active and not details.isPaused and not details.pausedDueToSyncFailure) {
            //    let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
            //    totalValueICP += valueInICP;
            //  };
            //};
            let tokenDetailsSell = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
              case (?details) { details };
              case (null) {
                Debug.print("Error: Sell token not found in details");
                continue a;
              };
            };

            let tradeSize = ((calculateTradeSizeMinMax() * (10 ** tokenDetailsSell.tokenDecimals)) / tokenDetailsSell.priceInICP);
            Debug.print("Selected pair: " # Principal.toText(sellToken) # " -> " # Principal.toText(buyToken) # " with size: " # Nat.toText(tradeSize));

            let bestExecution = await* findBestExecution(sellToken, buyToken, tradeSize);

            switch (bestExecution) {
              case (#ok(execution)) {
                let slippage = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };
                //let minAmountOut = (execution.expectedOut * (10000 - slippage + 30)) / 10000;
                let minAmountOut = execution.expectedOut / 2;
                let tradeResult = await* executeTrade(
                  sellToken,
                  buyToken,
                  tradeSize,
                  execution.exchange,
                  minAmountOut,
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
          case null {
            Debug.print("No valid trading pairs found");
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
              " Active_tokens=" # Nat.toText(Map.size(tokenDetailsMap)),
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
        if ((details.Active or details.balance > 0) and not details.isPaused and not details.pausedDueToSyncFailure) {
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
            if (not details.isPaused and not details.pausedDueToSyncFailure) {
                totalValueICP += valueInICP;
            };

            // Calculate adjusted target basis points
            let targetBasisPoints = if (not details.Active and details.balance > 0) {
                0 // Force sell inactive tokens with balance
            } else if (details.isPaused or details.pausedDueToSyncFailure) {
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
                    if (d.isPaused or d.pausedDueToSyncFailure) {
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
    let tradePairs = Vector.new<(Principal, Int, Nat)>();
    for (alloc in Vector.vals(allocations)) {
        let details = Map.get(tokenDetailsMap, phash, alloc.token);
        switch details {
            case (?d) {
                if (not d.isPaused and not d.pausedDueToSyncFailure and alloc.diffBasisPoints != 0) {
                    Vector.add(tradePairs, (alloc.token, alloc.diffBasisPoints, alloc.valueInICP));
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
          if (not d.isPaused and not d.pausedDueToSyncFailure) {
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
      " Tradeable_pairs=" # Nat.toText(Vector.size(tradePairs)),
      "calculateTradeRequirements"
    );

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
      // Check if token is paused
      let isPaused = switch (Map.get(tokenDetailsMap, phash, token)) {
        case (?details) { details.isPaused or details.pausedDueToSyncFailure };
        case (null) { true }; // Treat unknown tokens as paused
      };

      if (not isPaused) {
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
      if (details.Active and not details.isPaused and not details.pausedDueToSyncFailure) {
        totalValueICP += valueInICP;
        totalValueUSD += valueInUSD;
        activeTokenCount += 1;
      } else {
        if (details.isPaused) pausedTokenCount += 1;
        if (details.pausedDueToSyncFailure) syncFailureCount += 1;
      };
      
      // Format balance for display
      let formattedBalance = if (decimals >= 6) {
        Nat.toText(rawBalance / (10 ** (decimals - 6))) # "." # 
        Nat.toText((rawBalance % (10 ** (decimals - 6))) / (10 ** (decimals - 8))) # "M";
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
        " Amount_formatted=" # Nat.toText(amountIn / (10 ** (if (sellDecimals >= 6) { sellDecimals - 6 } else { 1 }))) # 
        (if (sellDecimals >= 6) { "M" } else { "" }) #
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
            //if (quote.slippage <= Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0) {
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
            //};
            //Debug.print("KongSwap quote seems bad: " # debug_show (quote) # " as slippage: " # debug_show (quote.slippage));
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
                //if (quote.slippage < Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0) {
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
                //};
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
      
      logger.info("TRADE_EXECUTION", 
        "Trade execution STARTED - Exchange=" # debug_show(exchange) #
        " Pair=" # sellSymbol # "/" # buySymbol #
        " Amount_in=" # Nat.toText(amountIn) # " (raw)" #
        " Amount_formatted=" # Nat.toText(amountIn / (10 ** (if (sellDecimals >= 6) { sellDecimals - 6 } else { 1 }))) # 
        (if (sellDecimals >= 6) { "M" } else { "" }) #
        " Min_amount_out=" # Nat.toText(minAmountOut) # " (raw)" #
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

              // Prepare swap params with slippage protection
              let safeMaxSlippage = if (rebalanceConfig.maxSlippageBasisPoints <= 10000) { 10000 - rebalanceConfig.maxSlippageBasisPoints } else { 0 };
              let adjustedMinOut = minAmountOut * safeMaxSlippage / 10000;
              let swapParams : swaptypes.ICPSwapParams = {
                poolId = poolData.canisterId;
                amountIn = amountIn - tx_fee;
                minAmountOut = adjustedMinOut;
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
                " Adjusted_min_out=" # Nat.toText(adjustedMinOut) #
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
                  let effectiveSlippage = if (minAmountOut > 0) {
                    Float.fromInt(Int.abs(actualAmountOut - minAmountOut)) / Float.fromInt(minAmountOut) * 100.0;
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
                    " Status=FAILED",
                    "executeTrade"
                  );
                  
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
      updateTokenPriceWithHistory(principal, Int.abs(icpPrice), usdPrice);
    };

    rebalanceState := {
      rebalanceState with
      metrics = {
        rebalanceState.metrics with
        lastPriceUpdate = now();
      };
    };
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
              ignore await stopRebalancing();
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
      #receiveTransferTasks : () -> ([(TransferRecipient, Nat, Principal, Nat8)], Bool);
      #setTest : () -> Bool;
      #startRebalancing : () -> ();
      #stopRebalancing : () -> ();
      #syncTokenDetailsFromDAO : () -> [(Principal, TokenDetails)];
      #updateRebalanceConfig : () -> (UpdateConfig, ?Bool);
      #getTokenPriceHistory : () -> [Principal];
      #getSystemParameters : () -> ();
    };
  }) : Bool {
     (isMasterAdmin(caller) or Principal.isController(caller)) and arg.size() < 50000;
  };
  */
};

