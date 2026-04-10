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
import Char "mo:base/Char";
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
import TACOSwap "../swap/taco_swap";
import SwapUtils "../swap/utils";
import swaptypes "../swap/swap_types";
import Fuzz "mo:fuzz";
import SpamProtection "../helper/spam_protection";
import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import Cycles "mo:base/ExperimentalCycles";
import Buffer "mo:base/Buffer";

//import Migration "./migration";

//(with migration = Migration.migrate)
shared (deployer) persistent actor class treasury() = this {

  private func this_canister_id() : Principal {
      Principal.fromActor(this);
  };

  //=========================================================================
  // 1. SYSTEM CONFIGURATION & STATE
  //=========================================================================

  transient var test = false;

  transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  transient let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
  transient let NEURON_SNAPSHOT_ID = canister_ids.getCanisterId(#neuronSnapshot);

  // Logger
  transient let logger = Logger.Logger();

  // Canister principals and references
  //let self = "z4is7-giaaa-aaaad-qg6uq-cai";
  transient let self = Principal.toText(this_canister_id());
  transient let ICPprincipalText = "ryjl3-tyaaa-aaaaa-aaaba-cai";
  transient let ICPprincipal = Principal.fromText(ICPprincipalText);
  //stable var DAOText = "ywhqf-eyaaa-aaaad-qg6tq-cai";
  //stable var DAOText = "vxqw7-iqaaa-aaaan-qzziq-cai";
  transient let DAOText = Principal.toText(DAO_BACKEND_ID);
  //stable var DAOPrincipal = Principal.fromText(DAOText);
  transient let DAOPrincipal = DAO_BACKEND_ID;
  //stable var MintVaultPrincipal = Principal.fromText("z3jul-lqaaa-aaaad-qg6ua-cai");
  stable var MintVaultPrincipal = DAO_BACKEND_ID;
  transient let NachosVaultPrincipal = canister_ids.getCanisterId(#nachos_vault);
  transient let priceArchiveId = canister_ids.getCanisterId(#price_archive);
  transient let neuronAllocArchiveId = canister_ids.getCanisterId(#dao_neuron_allocation_archive);

  transient let taco_dao_sns_governance_canister_id : Principal = Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai");

  // Price refresh rate limiting for nachos_vault
  stable var lastPriceRefreshTime : Int = 0;
  let MIN_PRICE_REFRESH_INTERVAL_NS : Int = 30_000_000_000; // 30 seconds



  // Core type aliases
  type Subaccount = Blob;
  type TransferRecipient = TreasuryTypes.TransferRecipient;
  type TransferResultICRC1 = TreasuryTypes.TransferResultICRC1;
  type TransferResultICP = TreasuryTypes.TransferResultICP;
  type TokenDetails = TreasuryTypes.TokenDetails;
  type TokenDetailsWithoutPastPrices = DAO_types.TokenDetailsWithoutPastPrices;
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

  // Execution plan type for split trades (Phase 2 anti-arb)
  // slippageBP is slippage in basis points (100bp = 1%)
  // percentBP is the percentage of trade going to this exchange in basis points (10000bp = 100%)
  type ExecutionPlan = {
    #Single : { exchange : ExchangeType; expectedOut : Nat; slippageBP : Nat };
    #Split : {
      kongswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
      icpswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
      taco : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
    };
    #Partial : {
      kongswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
      icpswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
      taco : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
      totalPercentBP : Nat;
    };
  };

  // Quote data from exchange (used for reduced amount estimation)
  type QuoteData = { out : Nat; slipBP : Nat; valid : Bool };

  // Extended error with quote data for reduced amount fallback
  type FindExecutionError = {
    reason : Text;
    kongQuotes : [QuoteData];
    icpQuotes : [QuoteData];
  };

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
  transient let dao = actor (DAOText) : actor {
    getTokenDetails : shared () -> async [(Principal, TokenDetails)];
    getTokenDetailsWithoutPastPrices : shared () -> async [TokenDetailsWithoutPastPrices];
    getAggregateAllocation : shared () -> async [(Principal, Nat)];
    syncTokenDetailsFromTreasury : shared ([(Principal, TokenDetails)]) -> async Result.Result<Text, SyncError>;
    hasAdminPermission : query (principal : Principal, function : SpamProtection.AdminFunction) -> async Bool;
  };

  transient let priceCheckNTN = actor ("moe7a-tiaaa-aaaag-qclfq-cai") : NTN.Self;

  // Map utilities
  transient let { phash; thash } = Map;
  transient let hashpp = TreasuryTypes.hashpp;
  transient let { natToNat64 } = Prim;

  // Helper: Check if a Float is finite (not NaN and not Inf)
  // Float.toInt traps on NaN and Inf, so we must guard before calling it
  // Max safe Int value is ~9.2e18 (2^63 - 1), we use a conservative bound
  private func isFiniteFloat(x : Float) : Bool {
    not Float.isNaN(x) and x < 9.0e18 and x > -9.0e18
  };

  // Pair skip helpers: normalize key so (A,B) and (B,A) produce the same entry
  private func normalizePair(tokenA : Principal, tokenB : Principal) : (Principal, Principal) {
    if (Principal.toText(tokenA) <= Principal.toText(tokenB)) {
      (tokenA, tokenB)
    } else {
      (tokenB, tokenA)
    };
  };

  private func addSkipPair(tokenA : Principal, tokenB : Principal) {
    // Never skip ICP pairs - ICP is the fallback route itself
    if (tokenA == ICPprincipal or tokenB == ICPprincipal) { return };
    let key = normalizePair(tokenA, tokenB);
    Map.set(pairSkipMap, hashpp, key, now());
    logger.info("PAIR_SKIP",
      "Added pair to skip map - TokenA=" # Principal.toText(tokenA) #
      " TokenB=" # Principal.toText(tokenB) #
      " Expires_in=5_days",
      "addSkipPair"
    );
  };

  private func removeSkipPair(tokenA : Principal, tokenB : Principal) {
    let key = normalizePair(tokenA, tokenB);
    ignore Map.remove(pairSkipMap, hashpp, key);
    logger.info("PAIR_SKIP",
      "Removed expired pair from skip map - TokenA=" # Principal.toText(tokenA) #
      " TokenB=" # Principal.toText(tokenB),
      "removeSkipPair"
    );
  };

  private func shouldSkipPair(tokenA : Principal, tokenB : Principal) : Bool {
    let key = normalizePair(tokenA, tokenB);
    switch (Map.get(pairSkipMap, hashpp, key)) {
      case (?timestamp) {
        if (now() - timestamp < PAIR_SKIP_EXPIRY_NS) {
          logger.info("PAIR_SKIP",
            "Skipping pair - going straight to ICP fallback - TokenA=" # Principal.toText(tokenA) #
            " TokenB=" # Principal.toText(tokenB) #
            " Skip_age_seconds=" # Int.toText((now() - timestamp) / 1_000_000_000),
            "shouldSkipPair"
          );
          true
        } else {
          removeSkipPair(tokenA, tokenB);
          false
        }
      };
      case null { false };
    };
  };

  // Per-exchange per-pair quote skip helpers
  private func exchangePairKey(tag : Text, tokenA : Principal, tokenB : Principal) : Text {
    let a = Principal.toText(tokenA);
    let b = Principal.toText(tokenB);
    if (a < b) { tag # ":" # a # ":" # b } else { tag # ":" # b # ":" # a }
  };

  private func addExchangePairSkip(tag : Text, tokenA : Principal, tokenB : Principal) {
    let key = exchangePairKey(tag, tokenA, tokenB);
    Map.set(exchangePairSkipMap, thash, key, now());
    logger.info("EXCHANGE_PAIR_SKIP",
      "Skipping " # tag # " quotes for " # Principal.toText(tokenA) # "/" # Principal.toText(tokenB) # " for 3 days (all quotes invalid)",
      "addExchangePairSkip"
    );
  };

  private func shouldSkipExchangePair(tag : Text, tokenA : Principal, tokenB : Principal) : Bool {
    let key = exchangePairKey(tag, tokenA, tokenB);
    switch (Map.get(exchangePairSkipMap, thash, key)) {
      case (?timestamp) {
        if (now() - timestamp < EXCHANGE_PAIR_SKIP_NS) { true }
        else { ignore Map.remove(exchangePairSkipMap, thash, key); false }
      };
      case null { false };
    };
  };

  private func clearExchangePairSkip(tag : Text, tokenA : Principal, tokenB : Principal) {
    let key = exchangePairKey(tag, tokenA, tokenB);
    ignore Map.remove(exchangePairSkipMap, thash, key);
  };

  // Pending burn tracking helpers
  // These coordinate with NACHOS vault to prevent trading reserved tokens
  private func addPendingBurn(token : Principal, amountE8s : Nat) {
    let current = switch (Map.get(pendingBurnsByToken, phash, token)) {
      case (?amount) { amount };
      case null { 0 };
    };
    Map.set(pendingBurnsByToken, phash, token, current + amountE8s);
  };

  private func releasePendingBurn(token : Principal, amountE8s : Nat) {
    let current = switch (Map.get(pendingBurnsByToken, phash, token)) {
      case (?amount) { amount };
      case null { 0 };
    };
    let newValue = if (current > amountE8s) { current - amountE8s } else { 0 };
    if (newValue > 0) {
      Map.set(pendingBurnsByToken, phash, token, newValue);
    } else {
      ignore Map.remove(pendingBurnsByToken, phash, token);
    };
  };

  private func getPendingBurn(token : Principal) : Nat {
    switch (Map.get(pendingBurnsByToken, phash, token)) {
      case (?amount) { amount };
      case null { 0 };
    };
  };

  // Randomization
  // NOTE: Fuzz.fromSeed internally uses Nat64.fromNat, so seed must be < 2^64
  transient let fuzz = Fuzz.fromSeed((Fuzz.fromSeed(Int.abs(now()) % (2 ** 63)).nat.randomRange(45978345345987, 2 ** 63)) % (2 ** 63));

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
    shortSyncIntervalNS = 1_800_000_000_000; // 30 minutes (for prices, balances)
    longSyncIntervalNS = 5 * 3600_000_000_000; // 5 hours (for metadata, pools)
    tokenSyncTimeoutNS = 21_600_000_000_000; // 6 hours
    //minAllocationDiffBasisPoints = 15;
    // Note: minAllocationDiffBasisPoints is now a standalone stable var (see below)
  };

  // Separate stable variable for circuit breaker configuration
  stable var pausedTokenThresholdForCircuitBreaker : Nat = 3; // Circuit breaker when 3+ tokens are paused

  // Minimum allocation difference to trigger trade (basis points, e.g., 15 = 0.15%)
  // Note: This is a standalone stable var to avoid EOP migration issues
  stable var minAllocationDiffBasisPoints : Nat = 15;

  // Pair skip: when a pair fails and falls to ICP fallback,
  // skip direct trading for this pair for this duration (nanoseconds)
  // 5 days = 5 * 24 * 3600 * 1_000_000_000
  let PAIR_SKIP_EXPIRY_NS : Int = 432_000_000_000_000;

  // Trading cycle backoff: exponential backoff when all trade attempts fail
  let MAX_TRADING_BACKOFF : Nat = 3; // Cap at 2^3 = 8x base interval

  // Watchdog timer: checks every 2 hours if the trading timer has stalled
  let WATCHDOG_INTERVAL_NS : Nat = 7_200_000_000_000; // 2 hours
  let WATCHDOG_STALE_THRESHOLD_NS : Int = 300_000_000_000; // 5 minutes (base buffer, added to backoff interval dynamically)

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

  // Track if rebalancing was active before upgrade (for auto-restart)
  stable var wasRebalancingActiveBeforeUpgrade : Bool = false; // NOT USED

  stable var MAX_PRICE_HISTORY_ENTRIES = 2000;

  //=========================================================================
  // 2. PORTFOLIO DATA STRUCTURES
  //=========================================================================

  // Core portfolio data
  stable let tokenDetailsMap = Map.new<Principal, TokenDetails>();
  stable let currentAllocations = Map.new<Principal, Nat>();

  // Exchange pool data
  stable var ICPswapPools = Map.new<(Principal, Principal), swaptypes.PoolData>();

  // Track TACO swap block numbers for automated recovery of failed swaps
  // Key: "tokenPrincipal:blockNum", Value: (blockNumber, tokenPrincipal, timestamp)
  stable var tacoFailedSwapBlocks = Map.new<Text, (Nat, Principal, Int)>();

  // DEPRECATED: pendingTxs and failedTxs - kept for stable variable backwards compatibility only
  // Kong now tracks failed swaps as claims - we use recoverKongswapClaims() instead
  // These are no longer used but cannot be deleted to maintain upgrade compatibility
  stable let pendingTxs = Map.new<Nat, swaptypes.SwapTxRecord>();
  stable let failedTxs = Map.new<Nat, swaptypes.SwapTxRecord>();

  // Pair skip map: (tokenA, tokenB) -> timestamp when pair was flagged
  // Pairs that failed and fell to ICP fallback are stored here.
  // Key is normalized (sorted) so order doesn't matter.
  // Skipped pairs go directly to ICP fallback in do_executeTradingStep.
  stable let pairSkipMap = Map.new<(Principal, Principal), Int>();

  // Per-exchange per-pair quote skip: "K:tok0:tok1" for Kong, "T:tok0:tok1" for TACO
  // Value: timestamp when all quotes were invalid. Expires after 3 days.
  stable let exchangePairSkipMap = Map.new<Text, Int>();
  let EXCHANGE_PAIR_SKIP_NS : Int = 259_200_000_000_000; // 3 days

  // Track pending burns to coordinate with NACHOS vault
  // Prevents trading cycle from selling tokens that are reserved for burn payouts
  // TACO multi-route detection: stores distinct routes found during last findBestExecution
  // Used by executeSplitTrade/executeTrade to decide single vs multi-route execution
  transient var lastTacoMultiRoute : Bool = false;
  transient var lastTacoRouteLegs : [{ route : [{ tokenIn : Text; tokenOut : Text }]; weight : Nat }] = [];

  stable let pendingBurnsByToken = Map.new<Principal, Nat>();

  // Note: Kong claims are now managed by Kong directly - no local tracking needed
  // Use KongSwap.getPendingClaims() to query and KongSwap.executeClaim() to recover

  //=========================================================================
  // LP MANAGEMENT STATE
  //=========================================================================

  // LP position metadata (exchange is source of truth for liquidity/backing)
  // Key: normalized "token0Text:token1Text" (alphabetical by principal text)
  stable var treasuryLPPositions = Map.new<Text, {
    token0Principal : Principal;
    token1Principal : Principal;
    totalDeposited0 : Nat;
    totalDeposited1 : Nat;
    totalWithdrawn0 : Nat;
    totalWithdrawn1 : Nat;
    totalFeesEarned0 : Nat;
    totalFeesEarned1 : Nat;
    firstDeployTimestamp : Int;
    lastFeeClaimTimestamp : Int;
  }>();

  // LP master configuration
  stable var lpConfig = {
    enabled = false : Bool;             // master switch — disabled by default
    lpRatioBP = 6000 : Nat;            // 60% of eligible deployed to LP
    maxPoolShareBP = 10_000 : Nat;     // disabled (100%) — allocation formula is the only limit
    minLPValueICP = 100_000_000 : Nat;  // min 1 ICP per pool (dust filter)
    rebalanceThresholdBP = 200 : Nat;   // 2% min |target-current| before adjusting
    maxAdjustmentsPerCycle = 3 : Nat;   // cap LP operations per cycle
    priceDeviationMaxBP = 1000 : Nat;   // 10% max pool vs treasury price divergence
    nachosRedemptionBufferBP = 2000 : Nat; // 20% kept liquid for NACHOS burns
    nachosHighVolumeThresholdBP = 500 : Nat; // pause LP when pending burns > 5%
  };

  // Per-pool LP config overrides
  stable var lpPoolConfig = Map.new<Text, {
    enabled : Bool;
    customLpRatioBP : ?Nat;
    customMaxPoolShareBP : ?Nat;
  }>();

  // Pending LP deposits for crash recovery (survives canister upgrade)
  // Key includes timestamp for uniqueness: "token0:token1:timestampNat"
  stable var lpPendingDeposits = Map.new<Text, {
    block0 : Nat;
    block1 : Nat;
    token0 : Text;
    token1 : Text;
    amount0 : Nat;
    amount1 : Nat;
    timestamp : Int;
  }>();

  // LP backing per token from exchange (raw token units, refreshed each cycle)
  transient var lpBackingPerToken = Map.new<Principal, Nat>();
  // Tokens removed from LP but not yet arrived at treasury wallet
  // Value: (amount, timestamp of removal) — timestamp prevents false-positive arrival detection
  transient var lpTokensInTransit = Map.new<Principal, (Nat, Int)>();
  // Tokens transferred to exchange but not yet confirmed as LP position
  transient var lpDepositsInFlight = Map.new<Principal, Nat>();
  // ICP value of each token's LP across all pools (budget tracking)
  transient var lpBudgetUsedPerToken = Map.new<Principal, Nat>();
  // Liquid (wallet) balance per token — NOT including LP
  transient var liquidBalancePerToken = Map.new<Principal, Nat>();
  // Cached LP positions from last successful exchange query
  transient var cachedLPPositions : [swaptypes.DetailedLiquidityPosition] = [];
  // Cached pool data from last successful exchange query
  transient var cachedPoolData : [swaptypes.AMMPoolInfo] = [];
  // Cached accepted tokens from the exchange
  transient var cachedExchangeAcceptedTokens : [Text] = [];
  // Cached exchange minimum amounts per token (for LP pre-checks)
  transient var cachedExchangeMinimums = Map.new<Text, Nat>();
  // Timestamp of last successful LP exchange query
  transient var lastLPQueryTimestamp : Int = 0;
  // Previous liquid balance (for detecting in-transit token arrival)
  transient var previousLiquidBalance = Map.new<Principal, Nat>();
  // Post-circuit-breaker: uncap LP adjustments for one cycle after pauses cleared
  transient var lpUncapNextCycle : Bool = false;

  // Timer IDs for scheduling
  var shortSyncTimerId : Nat = 0;
  var longSyncTimerId : Nat = 0;
  var watchdogTimerId : Nat = 0;
  var tacoRecoveryTimerId : Nat = 0;
  var lpFeeClaimTimerId : Nat = 0;

  // Trading cycle backoff level (0 = base interval, capped at MAX_TRADING_BACKOFF)
  // Transient: resets to 0 on upgrade
  transient var tradingBackoffLevel : Nat = 0;

  // Long Sync Timer tracking
  stable var lastLongSyncTime : Int = 0;  // Last time the long sync timer executed
  var nextLongSyncTime : Int = 0;  // Next scheduled execution time

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
  stable var portfolioSnapshotIntervalNS : Nat = 3_600_000_000_000; // 1 hour default
  stable var portfolioSnapshotStatus : {#Running; #Stopped} = #Stopped;

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

  //=========================================================================
  // LP HELPER FUNCTIONS
  //=========================================================================

  // Normalize pool key: alphabetical by principal text (deterministic ordering)
  private func normalizePoolKey(t0 : Principal, t1 : Principal) : Text {
    let s0 = Principal.toText(t0);
    let s1 = Principal.toText(t1);
    if (s0 < s1) { s0 # ":" # s1 } else { s1 # ":" # s0 };
  };

  private func normalizePoolKeyText(t0 : Text, t1 : Text) : Text {
    if (t0 < t1) { t0 # ":" # t1 } else { t1 # ":" # t0 };
  };

  // Get effective LP ratio for a pool (per-pool override or global default)
  private func getPoolLpRatio(poolKey : Text) : Nat {
    switch (Map.get(lpPoolConfig, thash, poolKey)) {
      case (?config) {
        switch (config.customLpRatioBP) { case (?r) { r }; case null { lpConfig.lpRatioBP } };
      };
      case null { lpConfig.lpRatioBP };
    };
  };

  // Get effective max pool share for a pool
  private func getPoolMaxShare(poolKey : Text) : Nat {
    switch (Map.get(lpPoolConfig, thash, poolKey)) {
      case (?config) {
        switch (config.customMaxPoolShareBP) { case (?r) { r }; case null { lpConfig.maxPoolShareBP } };
      };
      case null { lpConfig.maxPoolShareBP };
    };
  };

  // Is LP enabled for this pool (global AND per-pool check)
  private func isPoolLPEnabled(poolKey : Text) : Bool {
    if (not lpConfig.enabled) return false;
    switch (Map.get(lpPoolConfig, thash, poolKey)) {
      case (?config) { config.enabled };
      case null { false }; // Only explicitly configured pools are eligible
    };
  };

  // Check if NACHOS is in high-volume burn mode (should pause LP deployment)
  private func isNachosHighVolume() : Bool {
    var totalPendingICP : Nat = 0;
    for ((token, amount) in Map.entries(pendingBurnsByToken)) {
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case (?d) {
          if (d.priceInICP > 0 and d.tokenDecimals > 0) {
            let decimals : Nat = d.tokenDecimals;
            totalPendingICP += (amount * d.priceInICP) / (10 ** decimals);
          };
        };
        case null {};
      };
    };
    var totalPortfolioICP : Nat = 0;
    for ((_, d) in Map.entries(tokenDetailsMap)) {
      if (d.Active) {
        let decimals : Nat = d.tokenDecimals;
        totalPortfolioICP += (d.balance * d.priceInICP) / (10 ** decimals);
      };
    };
    if (totalPortfolioICP == 0) return true;
    (totalPendingICP * 10_000 / totalPortfolioICP) > lpConfig.nachosHighVolumeThresholdBP;
  };

  // Get current LP value in ICP for a specific pool from cached positions
  private func getCurrentLPValueICP(poolKey : Text) : Nat {
    for (pos in cachedLPPositions.vals()) {
      if (normalizePoolKeyText(pos.token0, pos.token1) == poolKey) {
        let t0 = Principal.fromText(pos.token0);
        let t1 = Principal.fromText(pos.token1);
        let p0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.priceInICP }; case null { 0 } };
        let p1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.priceInICP }; case null { 0 } };
        let d0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
        let d1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
        return (pos.token0Amount * p0) / (10 ** d0) + (pos.token1Amount * p1) / (10 ** d1);
      };
    };
    0;
  };

  // Get current liquidity units for a pool from cached positions
  private func getCurrentLiquidity(poolKey : Text) : Nat {
    for (pos in cachedLPPositions.vals()) {
      if (normalizePoolKeyText(pos.token0, pos.token1) == poolKey) {
        return pos.liquidity;
      };
    };
    0;
  };

  // Sum of allocations of all tokens paired with `token` in LP-eligible pools
  // Considers both existing pools on the exchange AND configured-but-not-yet-created pools
  private func getPartnerAllocSum(token : Principal) : Nat {
    var sum : Nat = 0;
    let seen = Map.new<Text, Bool>();

    // From existing pools on the exchange
    for (pool in cachedPoolData.vals()) {
      let poolKey = normalizePoolKeyText(pool.token0, pool.token1);
      if (not isPoolLPEnabled(poolKey)) {} else {
        Map.set(seen, thash, poolKey, true);
        let t0 = Principal.fromText(pool.token0);
        let t1 = Principal.fromText(pool.token1);
        if (t0 == token) {
          sum += switch (Map.get(currentAllocations, phash, t1)) { case (?v) { v }; case null { 0 } };
        } else if (t1 == token) {
          sum += switch (Map.get(currentAllocations, phash, t0)) { case (?v) { v }; case null { 0 } };
        };
      };
    };

    // From accepted token pairs not yet on the exchange
    for (t0Text in cachedExchangeAcceptedTokens.vals()) {
      for (t1Text in cachedExchangeAcceptedTokens.vals()) {
        if (t0Text >= t1Text) {} else {
          let poolKey = normalizePoolKeyText(t0Text, t1Text);
          if (isPoolLPEnabled(poolKey) and not Map.has(seen, thash, poolKey)) {
            let t0 = Principal.fromText(t0Text);
            let t1 = Principal.fromText(t1Text);
            if (Map.has(tokenDetailsMap, phash, t0) and Map.has(tokenDetailsMap, phash, t1)) {
              if (t0 == token) {
                sum += switch (Map.get(currentAllocations, phash, t1)) { case (?v) { v }; case null { 0 } };
              } else if (t1 == token) {
                sum += switch (Map.get(currentAllocations, phash, t0)) { case (?v) { v }; case null { 0 } };
              };
            };
          };
        };
      };
    };
    sum;
  };

  private func isMasterAdmin(caller : Principal) : Bool {
    AdminAuth.isMasterAdmin(caller, canister_ids.isKnownCanister)
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
      case (#CanisterStart) "Canister Start";
      case (#CanisterStop) "Canister Stop";
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
      case (#StartPortfolioSnapshots) "Start Portfolio Snapshots";
      case (#StopPortfolioSnapshots) "Stop Portfolio Snapshots";
      case (#UpdatePortfolioSnapshotInterval(details)) "Update Portfolio Snapshot Interval: " # Nat.toText(details.oldIntervalNS / 60_000_000_000) # "min → " # Nat.toText(details.newIntervalNS / 60_000_000_000) # "min";
      case (#LPAddLiquidity(details)) "LP Add Liquidity: " # details.pool;
      case (#LPRemoveLiquidity(details)) "LP Remove Liquidity: " # details.pool;
      case (#LPClaimFees(details)) "LP Claim Fees: " # details.pool;
      case (#LPEmergencyExit(details)) "LP Emergency Exit: " # Nat.toText(details.positionsRemoved) # " positions";
      case (#LPConfigUpdate(details)) "LP Config Update: " # details.details;
      case (#LPPoolConfigUpdate(details)) "LP Pool Config Update: " # details.pool;
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
        
        // Check circuit breaker conditions after pausing a token
        // This ensures immediate response if ICP or threshold conditions are met
        checkPortfolioCircuitBreakerConditions();
        
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
    // TODO: Move caller != DAOPrincipal and not Principal.isController(caller) into hasAdminPermission,
    //       check them first and make hasAdminPermission async* 
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
      tradingBackoffLevel := 0;
      startTradingTimer<system>();


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
      //logTreasuryAdminAction(caller, #StopRebalancing, "Unauthorized attempt", false, ?"Not authorized");
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
      //logTreasuryAdminAction(caller, #ResetRebalanceState, "Unauthorized attempt", false, ?"Not authorized");
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
    "|tokenSyncTimeoutNS=" # debug_show(config.tokenSyncTimeoutNS) #
    "|minAllocationDiffBasisPoints=" # debug_show(minAllocationDiffBasisPoints)
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
      //logTreasuryAdminAction(caller, #UpdateRebalanceConfig({oldConfig = oldConfigText; newConfig = oldConfigText}), "Unauthorized configuration update attempt", false, ?"Not authorized");
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

    // Validate min allocation diff basis points (stored as standalone stable var)
    switch (updates.minAllocationDiffBasisPoints) {
      case (?value) {
        let minAllowed = 1; // 0.01% minimum
        let maxAllowed = 500; // 5% maximum

        if (value < minAllowed) {
          validationErrors #= "Minimum allocation diff cannot be less than 1 basis point (0.01%); ";
        } else if (value > maxAllowed) {
          validationErrors #= "Minimum allocation diff cannot be more than 500 basis points (5%); ";
        } else {
          // Update standalone stable var (not in RebalanceConfig to avoid EOP migration issues)
          minAllocationDiffBasisPoints := value;
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
  public query func getSystemParameters() : async TreasuryTypes.RebalanceConfigResponse {
    {
      rebalanceIntervalNS = rebalanceConfig.rebalanceIntervalNS;
      maxTradeAttemptsPerInterval = rebalanceConfig.maxTradeAttemptsPerInterval;
      minTradeValueICP = rebalanceConfig.minTradeValueICP;
      maxTradeValueICP = rebalanceConfig.maxTradeValueICP;
      portfolioRebalancePeriodNS = rebalanceConfig.portfolioRebalancePeriodNS;
      maxSlippageBasisPoints = rebalanceConfig.maxSlippageBasisPoints;
      maxTradesStored = rebalanceConfig.maxTradesStored;
      maxKongswapAttempts = rebalanceConfig.maxKongswapAttempts;
      shortSyncIntervalNS = rebalanceConfig.shortSyncIntervalNS;
      longSyncIntervalNS = rebalanceConfig.longSyncIntervalNS;
      tokenSyncTimeoutNS = rebalanceConfig.tokenSyncTimeoutNS;
      minAllocationDiffBasisPoints = minAllocationDiffBasisPoints;
    };
  };

  /**
   * Get Long Sync Timer status
   *
   * Returns information about the Long Sync Timer including:
   * - lastRunTime: The last time the timer executed (0 if never run)
   * - nextScheduledTime: The next scheduled execution time (0 if not scheduled)
   * - isRunning: Whether the timer is currently active
   * - timerId: The timer ID (0 if not running)
   * - intervalNS: The configured interval in nanoseconds
   *
   * Accessible by any user with query access.
   */
  public query func getLongSyncTimerStatus() : async {
    lastRunTime : Int;
    nextScheduledTime : Int;
    isRunning : Bool;
    timerId : Nat;
    intervalNS : Nat;
  } {
    {
      lastRunTime = lastLongSyncTime;
      nextScheduledTime = nextLongSyncTime;
      isRunning = longSyncTimerId != 0;
      timerId = longSyncTimerId;
      intervalNS = rebalanceConfig.longSyncIntervalNS;
    }
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

    // Second pass - calculate allocations (only if totalValueICP > 0 to avoid division by zero)
    if (totalValueICP > 0) {
      for ((principal, details) in Map.entries(tokenDetailsMap)) {
          let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
          if (valueInICP > 0) {
              let basisPoints = (valueInICP * 10000) / totalValueICP;
              Vector.add(currentAllocs, (principal, basisPoints));
          };
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

  // Enhanced treasury dashboard — bundles 6 query results (tradingStatus, snapshotStatus,
  // recentSnapshots, tradingPauses, longSyncTimerStatus, systemParameters) into one call
  public shared query func getEnhancedTreasuryDashboard() : async Result.Result<TreasuryTypes.EnhancedTreasuryDashboard, Text> {
    // --- Trading status (same as getTradingStatus) ---
    var totalValueICP = 0;
    var totalValueUSD : Float = 0;
    let currentAllocs = Vector.new<(Principal, Nat)>();

    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
      totalValueICP += valueInICP;
      totalValueUSD += details.priceInUSD * Float.fromInt(details.balance) / (10.0 ** Float.fromInt(details.tokenDecimals));
    };

    if (totalValueICP > 0) {
      for ((principal, details) in Map.entries(tokenDetailsMap)) {
        let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        if (valueInICP > 0) {
          Vector.add(currentAllocs, (principal, (valueInICP * 10000) / totalValueICP));
        };
      };
    };

    var totalSlippage : Float = 0;
    var impactCount = 0;
    for (trade in Vector.vals(rebalanceState.lastTrades)) {
      if (trade.success) {
        totalSlippage += trade.slippage;
        impactCount += 1;
      };
    };

    let avgSlippage = if (impactCount > 0) { totalSlippage / Float.fromInt(impactCount) } else { 0.0 };
    let totalAttempts = rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed + rebalanceState.metrics.totalTradesSkipped;
    let successRate = if (rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed > 0) {
      Float.fromInt(rebalanceState.metrics.totalTradesExecuted) / Float.fromInt(rebalanceState.metrics.totalTradesExecuted + rebalanceState.metrics.totalTradesFailed);
    } else { 0.0 };
    let skipRate = if (totalAttempts > 0) { Float.fromInt(rebalanceState.metrics.totalTradesSkipped) / Float.fromInt(totalAttempts) } else { 0.0 };

    // --- Recent snapshots (last 5, same as getPortfolioHistory(5)) ---
    let allSnapshots = Vector.toArray(portfolioSnapshots);
    let snapshotTotal = allSnapshots.size();
    let snapshotStart = if (snapshotTotal > 5) { snapshotTotal - 5 } else { 0 };
    let recentSnaps = Array.subArray(allSnapshots, snapshotStart, snapshotTotal - snapshotStart);

    // --- Trading pauses (same as listTradingPauses) ---
    let pausedArray = Iter.toArray(Map.vals(tradingPauses));

    #ok({
      tradingStatus = {
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
      };
      portfolioSnapshotStatus = {
        status = portfolioSnapshotStatus;
        intervalMinutes = portfolioSnapshotIntervalNS / (60 * 1_000_000_000);
        lastSnapshotTime = lastPortfolioSnapshotTime;
      };
      recentSnapshots = {
        snapshots = recentSnaps;
        totalCount = snapshotTotal;
      };
      tradingPauses = {
        pausedTokens = pausedArray;
        totalCount = pausedArray.size();
      };
      longSyncTimerStatus = {
        lastRunTime = lastLongSyncTime;
        nextScheduledTime = nextLongSyncTime;
        isRunning = longSyncTimerId != 0;
        timerId = longSyncTimerId;
        intervalNS = rebalanceConfig.longSyncIntervalNS;
      };
      systemParameters = {
        rebalanceIntervalNS = rebalanceConfig.rebalanceIntervalNS;
        maxTradeAttemptsPerInterval = rebalanceConfig.maxTradeAttemptsPerInterval;
        minTradeValueICP = rebalanceConfig.minTradeValueICP;
        maxTradeValueICP = rebalanceConfig.maxTradeValueICP;
        portfolioRebalancePeriodNS = rebalanceConfig.portfolioRebalancePeriodNS;
        maxSlippageBasisPoints = rebalanceConfig.maxSlippageBasisPoints;
        maxTradesStored = rebalanceConfig.maxTradesStored;
        maxKongswapAttempts = rebalanceConfig.maxKongswapAttempts;
        shortSyncIntervalNS = rebalanceConfig.shortSyncIntervalNS;
        longSyncIntervalNS = rebalanceConfig.longSyncIntervalNS;
        tokenSyncTimeoutNS = rebalanceConfig.tokenSyncTimeoutNS;
        minAllocationDiffBasisPoints = minAllocationDiffBasisPoints;
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

    // Second pass - calculate allocations (only if totalValueICP > 0 to avoid division by zero)
    if (totalValueICP > 0) {
      for ((principal, details) in Map.entries(tokenDetailsMap)) {
          let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
          if (valueInICP > 0) {
              let basisPoints = (valueInICP * 10000) / totalValueICP;
              Vector.add(currentAllocs, (principal, basisPoints));
          };
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
   * Public query - allows anyone to view portfolio history (read-only transparency).
   */
  public shared query func getPortfolioHistorySince(sinceTimestamp: Int, limit: Nat) : async Result.Result<PortfolioHistoryResponse, PortfolioSnapshotError> {
    if (limit == 0 or limit > 2000) {
      return #err(#InvalidLimit);
    };

    // Filter snapshots by timestamp first
    let allSnapshots = Vector.toArray(portfolioSnapshots);
    let filteredSnapshots = Array.filter<PortfolioSnapshot>(allSnapshots, func(snapshot) {
      snapshot.timestamp > sinceTimestamp
    });
    
    let totalFilteredCount = filteredSnapshots.size();
    
    // Apply limit to filtered results (get oldest ones first for proper archive processing)
    let limitedSnapshots = if (totalFilteredCount > limit) {
      Array.subArray(filteredSnapshots, 0, limit)
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
      //logTreasuryAdminAction(caller, #UnpauseToken({token}), "Unauthorized unpause attempt", false, ?"Not authorized");
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
      //logTreasuryAdminAction(caller, #PauseTokenManual({token; pauseType = "manual"}), reason, false, ?"Not authorized");
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
      //logTreasuryAdminAction(caller, #ClearAllTradingPauses, "Unauthorized attempt", false, ?"Not authorized");
      return #err(#NotAuthorized);
    };

    let clearedCount = Map.size(tradingPauses);
    Map.clear(tradingPauses);

    // Post-circuit-breaker: LP positions may have drifted during freeze.
    // Uncap LP adjustments for the next cycle to catch up in one pass.
    if (lpConfig.enabled and clearedCount > 0) { lpUncapNextCycle := true };

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
      #nanoseconds(portfolioSnapshotIntervalNS),
      func() : async () {
        await takePortfolioSnapshot(#Scheduled);
        // Only restart if still in running status
        if (portfolioSnapshotStatus == #Running) {
          await* startPortfolioSnapshotTimer();
        };
      }
    );

    portfolioSnapshotStatus := #Running;
    logger.info(
      "PORTFOLIO_SNAPSHOT",
      "Portfolio snapshot timer started - Interval=" # Nat.toText(portfolioSnapshotIntervalNS / 1_000_000_000) # "s Timer_ID=" # Nat.toText(portfolioSnapshotTimerId),
      "startPortfolioSnapshotTimer"
    );
  };

  /**
   * Stop the portfolio snapshot timer
   */
  private func stopPortfolioSnapshotTimer() {
    if (portfolioSnapshotTimerId != 0) {
      cancelTimer(portfolioSnapshotTimerId);
      portfolioSnapshotTimerId := 0;
    };
    portfolioSnapshotStatus := #Stopped;
    
    logger.info(
      "PORTFOLIO_SNAPSHOT",
      "Portfolio snapshot timer stopped",
      "stopPortfolioSnapshotTimer"
    );
  };

  /**
   * Start portfolio snapshots (Admin method)
   */
  public shared ({ caller }) func startPortfolioSnapshots(reason: ?Text) : async Result.Result<Text, Text> {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal)) {
      let reasonText = switch (reason) { case (?r) r; case null "Start portfolio snapshots" };
      //logTreasuryAdminAction(caller, #StartPortfolioSnapshots, reasonText, false, ?"Not authorized");
      return #err("Not authorized");
    };

    if (portfolioSnapshotStatus == #Running) {
      let reasonText = switch (reason) { case (?r) r; case null "Start portfolio snapshots" };
      logTreasuryAdminAction(caller, #StartPortfolioSnapshots, reasonText, false, ?"Already running");
      return #err("Portfolio snapshots already running");
    };

    try {
      await* startPortfolioSnapshotTimer<system>();
      let reasonText = switch (reason) { case (?r) r; case null "Portfolio snapshots started" };
      logTreasuryAdminAction(caller, #StartPortfolioSnapshots, reasonText, true, null);
      #ok("Portfolio snapshots started successfully")
    } catch (e) {
      let reasonText = switch (reason) { case (?r) r; case null "Start portfolio snapshots" };
      logTreasuryAdminAction(caller, #StartPortfolioSnapshots, reasonText, false, ?Error.message(e));
      #err("Failed to start portfolio snapshots: " # Error.message(e))
    };
  };

  /**
   * Stop portfolio snapshots (Admin method)
   */
  public shared ({ caller }) func stopPortfolioSnapshots(reason: ?Text) : async Result.Result<Text, Text> {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal)) {
      let reasonText = switch (reason) { case (?r) r; case null "Stop portfolio snapshots" };
      //logTreasuryAdminAction(caller, #StopPortfolioSnapshots, reasonText, false, ?"Not authorized");
      return #err("Not authorized");
    };

    if (portfolioSnapshotStatus == #Stopped) {
      let reasonText = switch (reason) { case (?r) r; case null "Stop portfolio snapshots" };
      logTreasuryAdminAction(caller, #StopPortfolioSnapshots, reasonText, false, ?"Already stopped");
      return #err("Portfolio snapshots already stopped");
    };

    stopPortfolioSnapshotTimer();
    let reasonText = switch (reason) { case (?r) r; case null "Portfolio snapshots stopped" };
    logTreasuryAdminAction(caller, #StopPortfolioSnapshots, reasonText, true, null);
    #ok("Portfolio snapshots stopped successfully");
  };

  /**
   * Update portfolio snapshot interval (Admin method)
   */
  public shared ({ caller }) func updatePortfolioSnapshotInterval(intervalMinutes: Nat, reason: ?Text) : async Result.Result<Text, Text> {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal)) {
      let reasonText = switch (reason) { case (?r) r; case null "Update portfolio snapshot interval" };
      //logTreasuryAdminAction(caller, #UpdatePortfolioSnapshotInterval({oldIntervalNS = portfolioSnapshotIntervalNS; newIntervalNS = portfolioSnapshotIntervalNS}), reasonText, false, ?"Not authorized");
      return #err("Not authorized");
    };

    // Validate interval (between 1 minute and 24 hours)
    if (intervalMinutes < 1 or intervalMinutes > 1440) {
      let reasonText = switch (reason) { case (?r) r; case null "Update portfolio snapshot interval" };
      logTreasuryAdminAction(caller, #UpdatePortfolioSnapshotInterval({oldIntervalNS = portfolioSnapshotIntervalNS; newIntervalNS = portfolioSnapshotIntervalNS}), reasonText, false, ?"Invalid interval: must be between 1 and 1440 minutes");
      return #err("Invalid interval: must be between 1 and 1440 minutes");
    };

    let oldIntervalNS = portfolioSnapshotIntervalNS;
    let newIntervalNS = intervalMinutes * 60 * 1_000_000_000; // Convert minutes to nanoseconds
    
    if (oldIntervalNS == newIntervalNS) {
      let reasonText = switch (reason) { case (?r) r; case null "Update portfolio snapshot interval" };
      logTreasuryAdminAction(caller, #UpdatePortfolioSnapshotInterval({oldIntervalNS; newIntervalNS}), reasonText, true, null);
      return #ok("No change needed - interval already set to " # Nat.toText(intervalMinutes) # " minutes");
    };

    portfolioSnapshotIntervalNS := newIntervalNS;
    
    // If currently running, restart with new interval
    let wasRunning = portfolioSnapshotStatus == #Running;
    if (wasRunning) {
      stopPortfolioSnapshotTimer();
      await* startPortfolioSnapshotTimer<system>();
    };

    let reasonText = switch (reason) { case (?r) r; case null "Portfolio snapshot interval updated to " # Nat.toText(intervalMinutes) # " minutes" };
    logTreasuryAdminAction(caller, #UpdatePortfolioSnapshotInterval({oldIntervalNS; newIntervalNS}), reasonText, true, null);
    
    let statusMsg = if (wasRunning) " (timer restarted with new interval)" else "";
    #ok("Portfolio snapshot interval updated to " # Nat.toText(intervalMinutes) # " minutes" # statusMsg);
  };

  /**
   * Get portfolio snapshot status
   */
  public query func getPortfolioSnapshotStatus() : async {
    status: {#Running; #Stopped};
    intervalMinutes: Nat;
    lastSnapshotTime: Int;
  } {
    {
      status = portfolioSnapshotStatus;
      intervalMinutes = portfolioSnapshotIntervalNS / (60 * 1_000_000_000);
      lastSnapshotTime = lastPortfolioSnapshotTime;
    }
  };

  /**
   * Get portfolio history
   * 
   * Returns recent portfolio snapshots for analysis and charting.
   * Public query - allows anyone to view portfolio history (read-only transparency).
   */
  public shared query func getPortfolioHistory(limit : Nat) : async Result.Result<PortfolioHistoryResponse, PortfolioSnapshotError> {
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
    
    // Check if ICP is paused - if so, trigger circuit breaker immediately
    if (isTokenPausedFromTrading(ICPprincipal)) {
      triggerICPPausedCircuitBreaker();
      return; // Exit early since circuit breaker was triggered
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
   * Trigger circuit breaker due to ICP being paused
   *
   * Called when ICP token is paused from trading.
   * Pauses all remaining active tokens since ICP is critical for fallback trading.
   */
  private func triggerICPPausedCircuitBreaker() {
    // Get all active trading tokens to pause (excluding ICP since it's already paused)
    let tokensToProcess = Iter.toArray(Map.entries(tokenDetailsMap));
    let pausedTokens = Vector.new<Principal>();

    for ((token, details) in tokensToProcess.vals()) {
      if (details.Active and token != ICPprincipal and not isTokenPausedFromTrading(token)) {
        let pauseReason : TradingPauseReason = #CircuitBreaker({
          reason = "ICP circuit breaker triggered: ICP token has been paused from trading, disabling all trading to prevent system instability";
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
      "ICP_PAUSED_CIRCUIT_BREAKER",
      "ICP PAUSED CIRCUIT BREAKER TRIGGERED - " #
      " ICP_Status=Paused" #
      " Additional_Tokens_Paused=" # Nat.toText(Vector.size(pausedTokens)) #
      " Reason=ICP_critical_for_fallback_trading",
      "triggerICPPausedCircuitBreaker"
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

  // simple token send for dao tasks. 
  public shared ({ caller }) func sendToken(token : Principal, amount_e8s : Nat, to_principal : Principal, to_subaccount : ?Subaccount) : async () {

    // ONLY TACO DAO governance canister may call this method (via adopted proposal)!
    assert (caller == taco_dao_sns_governance_canister_id);

    let ledger = actor (Principal.toText(token)) : ICRC1.FullInterface;
    let to_account = { owner = to_principal; subaccount = to_subaccount };
    
    logger.info(
      "SEND_TOKEN",
      "SEND TOKEN - " #
      " Token=" # Principal.toText(token) #
      " Amount=" # Nat.toText(amount_e8s) #
      " To=" # Principal.toText(to_principal) #
      " To Subaccount=" # debug_show(to_subaccount),
      "sendToken"
    );

    let result = await (with timeout = 65) ledger.icrc1_transfer({
      from_subaccount = null;
      to = to_account;
      amount = amount_e8s;
      fee = null;
      memo = null;
      created_at_time = null;
    });

    logger.info(
      "SEND_TOKEN",
      "SEND TOKEN - " #
      " Token=" # Principal.toText(token) #
      " Amount=" # Nat.toText(amount_e8s) #
      " To=" # Principal.toText(to_principal) #
      " To Subaccount=" # debug_show(to_subaccount) #
      " Result=" # debug_show(result),
      "sendToken"
    );
  };

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
    assert (caller == DAOPrincipal or caller == MintVaultPrincipal or caller == NachosVaultPrincipal);

    if (Immediate) {
      // Track pending burns: reserve tokens being sent out (prevents trading cycle from selling them)
      // Heuristic: transfers from subaccount 2 (NachosTreasurySubaccount) are burn payouts
      let burnPayouts = Vector.new<(Principal, Nat)>(); // Store for cleanup after transfers
      for (task in tempTransferQueue.vals()) {
        let (recipient, amount, token, fromSubaccount) = task;
        if (fromSubaccount == 2) {  // NachosTreasurySubaccount = 2 (NACHOS burn payouts)
          addPendingBurn(token, amount);
          Vector.add(burnPayouts, (token, amount));
        };
      };

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

          let transferTask = (with timeout = 65) ledger.transfer({
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
          let result = await  transferTask.0;
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

      // Release pending burns now that transfers are complete
      for ((token, amount) in Vector.vals(burnPayouts)) {
        releasePendingBurn(token, amount);
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

            let transferTask = (with timeout = 65) token.icrc1_transfer({
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
            let transferTask = (with timeout = 65)ledger.transfer({
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

    let backoffMultiplier = 2 ** tradingBackoffLevel;
    let effectiveInterval = rebalanceConfig.rebalanceIntervalNS * backoffMultiplier;

    if (tradingBackoffLevel > 0) {
      logger.info("TRADING_TIMER",
        "Scheduling next trading cycle with backoff - Level=" # Nat.toText(tradingBackoffLevel) #
        " Multiplier=" # Nat.toText(backoffMultiplier) #
        " Interval=" # Nat.toText(effectiveInterval / 1_000_000_000) # "s",
        "startTradingTimer"
      );
    };

    let newTimerId = setTimer<system>(
      #nanoseconds(effectiveInterval),
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
   * Watchdog timer for the trading cycle
   *
   * Runs every 2 hours. If the trading timer appears to have stopped
   * (lastRebalanceAttempt is more than 5 minutes stale) and the system
   * is not idle, restart the trading timer automatically.
   */
  private func startWatchdogTimer<system>() {
    if (watchdogTimerId != 0) {
      cancelTimer(watchdogTimerId);
    };

    watchdogTimerId := setTimer<system>(
      #nanoseconds(WATCHDOG_INTERVAL_NS),
      func() : async () {
        let staleness = now() - rebalanceState.metrics.lastRebalanceAttempt;

        // Dynamic threshold: base buffer + current backoff interval
        let backoffMultiplier : Int = 2 ** tradingBackoffLevel;
        let effectiveInterval : Int = rebalanceConfig.rebalanceIntervalNS * backoffMultiplier;
        let effectiveThreshold = effectiveInterval + WATCHDOG_STALE_THRESHOLD_NS;

        if (rebalanceState.status != #Idle and staleness > effectiveThreshold) {
          logger.warn(
            "WATCHDOG",
            "Trading timer appears stalled - last attempt " #
              Int.toText(staleness / 1_000_000_000) # "s ago. " #
              "Status=" # debug_show(rebalanceState.status) #
              " Backoff_level=" # Nat.toText(tradingBackoffLevel) #
              " Threshold=" # Int.toText(effectiveThreshold / 1_000_000_000) # "s" #
              ". Restarting trading timer.",
            "startWatchdogTimer"
          );

          tradingBackoffLevel := 0;
          rebalanceState := {
            rebalanceState with
            status = #Trading;
            metrics = {
              rebalanceState.metrics with
              lastRebalanceAttempt = now();
            };
          };
          startTradingTimer<system>();
          logTreasuryAdminAction(
            this_canister_id(),
            #StartRebalancing,
            "Watchdog auto-restart: trading timer was stale for " #
              Int.toText(staleness / 1_000_000_000) # "s",
            true,
            null
          );
        } else {
          logger.info(
            "WATCHDOG",
            "Trading timer healthy - last attempt " #
              Int.toText(staleness / 1_000_000_000) # "s ago. " #
              "Status=" # debug_show(rebalanceState.status),
            "startWatchdogTimer"
          );
        };

        // Ensure critical archive import timers are running
        try {
          let priceArchive = actor (Principal.toText(priceArchiveId)) : actor {
            startBatchImportSystem : shared () -> async Result.Result<Text, Text>;
          };
          let neuronAllocArchive = actor (Principal.toText(neuronAllocArchiveId)) : actor {
            startBatchImportSystem : shared () -> async Result.Result<Text, Text>;
          };
          ignore await priceArchive.startBatchImportSystem();
          ignore await neuronAllocArchive.startBatchImportSystem();
        } catch (e) {
          logger.warn("WATCHDOG", "Failed to ensure archive timers: " # Error.message(e), "startWatchdogTimer");
        };

        startWatchdogTimer<system>();
      },
    );
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
      " Executed_Trades=" # Nat.toText(rebalanceState.metrics.totalTradesExecuted) #
      " Failed_Trades=" # Nat.toText(rebalanceState.metrics.totalTradesFailed) #
      " Last_Attempt=" # Int.toText((now() - rebalanceState.metrics.lastRebalanceAttempt) / 1_000_000_000) # "s_ago",
      "do_executeTradingCycle"
    );

    // Sync allocations from DAO if empty (e.g., after upgrade or first cycle)
    if (Map.size(currentAllocations) == 0) {
      await syncFromDAO();
    };

    // Update balances before any trading decisions
    await updateBalances();

    // Retry failed kongswap transactions
    await* recoverKongswapClaims();

    // Recover failed TACO exchange swaps
    await* recoverTacoSwapFunds();

    // Recover any pending LP deposits from crashed operations
    if (lpConfig.enabled) { await* recoverLPPendingDeposits() };

    // If LP is enabled but exchange data is stale (>1h), skip this cycle
    if (lpConfig.enabled and lastLPQueryTimestamp > 0 and now() - lastLPQueryTimestamp > 3_600_000_000_000 and Map.size(lpBackingPerToken) > 0) {
      logger.error("LP_CYCLE", "Skipping trading cycle — LP data expired and exchange unreachable", "do_executeTradingCycle");
      return;
    };

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
   * Checks for and recovers any pending Kong claims (tokens from failed swaps)
   * Kong automatically creates claims when swaps fail - we just query and execute them
   */
  private func recoverKongswapClaims() : async* () {
    Debug.print("Checking for Kong claims to recover...");

    // Get all pending claims from Kong for this treasury
    let claimsResult = await (with timeout = 65) KongSwap.getPendingClaims(Principal.fromActor(this));

    switch (claimsResult) {
      case (#ok(claims)) {
        if (claims.size() == 0) {
          Debug.print("No pending claims to recover");
          return;
        };

        Debug.print("Found " # Nat.toText(claims.size()) # " pending claims to recover");

        for (claim in claims.vals()) {
          Debug.print("Recovering claim " # Nat64.toText(claim.claim_id) #
                     ": " # claim.symbol # " amount=" # Nat.toText(claim.amount));

          let result = await (with timeout = 65) KongSwap.executeClaim(claim.claim_id);

          switch (result) {
            case (#ok(reply)) {
              logger.info("CLAIM_RECOVERED",
                "Successfully recovered Kong claim - ID=" # Nat64.toText(claim.claim_id) #
                " Symbol=" # reply.symbol #
                " Amount=" # Nat.toText(reply.amount),
                "recoverKongswapClaims"
              );
            };
            case (#err(e)) {
              logger.warn("CLAIM_FAILED",
                "Failed to recover Kong claim - ID=" # Nat64.toText(claim.claim_id) #
                " Error=" # e,
                "recoverKongswapClaims"
              );
            };
          };
        };
      };
      case (#err(e)) {
        Debug.print("Error getting claims: " # e);
      };
    };
  };

  /**
   * Parse block number and token from TACO swap error messages
   * Format: "... [block=12345 token=abc-cai]: ..."
   */
  private func parseTacoErrorInfo(errorMsg : Text) : ?(Nat, Text) {
    let chars = Iter.toArray(errorMsg.chars());
    let size = chars.size();

    // Find "[block="
    var blockStart : ?Nat = null;
    label search1 for (i in Iter.range(0, size - 7)) {
      if (chars[i] == '[' and chars[i + 1] == 'b' and chars[i + 2] == 'l' and chars[i + 3] == 'o' and chars[i + 4] == 'c' and chars[i + 5] == 'k' and chars[i + 6] == '=') {
        blockStart := ?(i + 7);
        break search1;
      };
    };

    switch (blockStart) {
      case null { return null };
      case (?bs) {
        // Parse block number
        var blockNum : Nat = 0;
        var idx = bs;
        while (idx < size and Char.toNat32(chars[idx]) >= 48 and Char.toNat32(chars[idx]) <= 57) {
          blockNum := blockNum * 10 + Prim.nat32ToNat(Char.toNat32(chars[idx]) - 48);
          idx += 1;
        };

        if (blockNum == 0) { return null };

        // Find "token=" after block number
        var tokenStart : ?Nat = null;
        label search2 for (i in Iter.range(idx, size - 7)) {
          if (chars[i] == 't' and chars[i + 1] == 'o' and chars[i + 2] == 'k' and chars[i + 3] == 'e' and chars[i + 4] == 'n' and chars[i + 5] == '=') {
            tokenStart := ?(i + 6);
            break search2;
          };
        };

        switch (tokenStart) {
          case null { return null };
          case (?ts) {
            // Extract token until ']'
            var tokenEnd = ts;
            while (tokenEnd < size and chars[tokenEnd] != ']') {
              tokenEnd += 1;
            };
            if (tokenEnd == ts) { return null };

            var tokenText = "";
            for (j in Iter.range(ts, tokenEnd - 1)) {
              tokenText := tokenText # Char.toText(chars[j]);
            };
            ?(blockNum, tokenText)
          };
        };
      };
    };
  };

  /**
   * Recover tokens from failed TACO exchange swaps
   *
   * Iterates tracked failed swaps and calls recoverWronglysent on the exchange.
   * Only attempts recovery for entries 2+ minutes old and less than 21 days old.
   */
  private func recoverTacoSwapFunds() : async* () {
    if (Map.size(tacoFailedSwapBlocks) == 0) { return };
    Debug.print("TACO recovery: checking " # Nat.toText(Map.size(tacoFailedSwapBlocks)) # " entries...");

    let currentTime = now();
    let TWO_MIN : Int = 120_000_000_000;
    let TWENTY_ONE_DAYS : Int = 21 * 86_400_000_000_000;
    let toRemove = Vector.new<Text>();

    for ((key, (blockNum, tokenPrincipal, timestamp)) in Map.entries(tacoFailedSwapBlocks)) {
      let age = currentTime - timestamp;
      if (age < TWO_MIN) {
        Debug.print("TACO recovery: skipping " # key # " - too recent");
      } else if (age > TWENTY_ONE_DAYS) {
        logger.warn("TACO_RECOVERY", "Removing expired entry: " # key, "recoverTacoSwapFunds");
        Vector.add(toRemove, key);
      } else {
        let tokenType : { #ICP; #ICRC12; #ICRC3 } =
          if (Principal.toText(tokenPrincipal) == "ryjl3-tyaaa-aaaaa-aaaba-cai") { #ICP } else { #ICRC12 };
        try {
          let result = await (with timeout = 65) TACOSwap.recoverStuckTokens(
            Principal.toText(tokenPrincipal), blockNum, tokenType
          );
          switch (result) {
            case (#ok(recovered)) {
              if (recovered) {
                logger.info("TACO_RECOVERY",
                  "Recovered: block=" # Nat.toText(blockNum) # " token=" # Principal.toText(tokenPrincipal),
                  "recoverTacoSwapFunds"
                );
              };
              Vector.add(toRemove, key);
            };
            case (#err(e)) {
              Debug.print("TACO recovery error for " # key # ": " # e);
            };
          };
        } catch (e) {
          Debug.print("TACO recovery exception for " # key # ": " # Error.message(e));
        };
      };
    };

    for (key in Vector.vals(toRemove)) {
      Map.delete(tacoFailedSwapBlocks, thash, key);
    };
  };

  // Standalone TACO recovery timer — runs every 6 hours regardless of trading state
  private func startTacoRecoveryTimer<system>() {
    if (tacoRecoveryTimerId != 0) { cancelTimer(tacoRecoveryTimerId) };
    tacoRecoveryTimerId := setTimer<system>(
      #nanoseconds(21_600_000_000_000), // 6 hours
      func() : async () {
        if (Map.size(tacoFailedSwapBlocks) > 0) {
          logger.info("TACO_RECOVERY", "Standalone recovery timer: " # Nat.toText(Map.size(tacoFailedSwapBlocks)) # " pending entries", "tacoRecoveryTimer");
          try {
            await* recoverTacoSwapFunds();
          } catch (e) {
            logger.error("TACO_RECOVERY", "Standalone recovery error: " # Error.message(e), "tacoRecoveryTimer");
          };
        };
        startTacoRecoveryTimer<system>();
      }
    );
  };

  // LP fee claim timer — runs every 6 hours to collect accumulated fees
  private func startLPFeeClaimTimer<system>() {
    if (not lpConfig.enabled) return;
    if (lpFeeClaimTimerId != 0) { cancelTimer(lpFeeClaimTimerId) };
    lpFeeClaimTimerId := setTimer<system>(
      #nanoseconds(21_600_000_000_000), // 6 hours
      func() : async () {
        if (lpConfig.enabled and Map.size(treasuryLPPositions) > 0) {
          logger.info("LP_FEES", "Fee claim timer: " # Nat.toText(Map.size(treasuryLPPositions)) # " positions", "lpFeeClaimTimer");
          try {
            await claimAllLPFees();
          } catch (e) {
            logger.error("LP_FEES", "Fee claim timer error: " # Error.message(e), "lpFeeClaimTimer");
          };
        };
        startLPFeeClaimTimer<system>();
      },
    );
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

    // ===== STEP A: LP REMOVALS (before trading) =====
    // Remove LP from overweight pools to free tokens + thin pool for better DEX routing
    // Also runs when LP is DISABLED but positions still exist (graceful wind-down)
    if (lastLPQueryTimestamp > 0) {
      if (lpConfig.enabled) {
        // Normal mode: remove overweight LP based on targets
        let lpTargets = computeLPTargets();
        let removals = Vector.new<(Text, Principal, Principal, Nat, Nat, Nat)>();
        for ((poolKey, t0, t1, target, current, liq) in lpTargets.vals()) {
          if (current > target) {
            let excess = current - target;
            // Only adjust if above rebalance threshold
            if (current > 0 and (excess * 10_000 / current) > lpConfig.rebalanceThresholdBP) {
              Vector.add(removals, (poolKey, t0, t1, excess, current, liq));
            };
          };
        };
        // Sort by excess descending (largest first) and cap at maxAdjustmentsPerCycle
        let sortedRemovals = Array.sort<(Text, Principal, Principal, Nat, Nat, Nat)>(
          Vector.toArray(removals),
          func(a, b) { Nat.compare(b.3, a.3) },
        );
        var lpRemoveOps : Nat = 0;
        label lpRemoveLoop for (removal in sortedRemovals.vals()) {
          if (not lpUncapNextCycle and lpRemoveOps >= lpConfig.maxAdjustmentsPerCycle) break lpRemoveLoop;
          await removeLiquidityFromPool(removal.0, removal.1, removal.2, removal.3, removal.4, removal.5);
          lpRemoveOps += 1;
        };
      } else {
        // LP disabled — wind down: remove all remaining positions (max per cycle)
        var lpRemoveOps : Nat = 0;
        label lpWindDown for (pos in cachedLPPositions.vals()) {
          if (not lpUncapNextCycle and lpRemoveOps >= 3) break lpWindDown; // Cap at 3 per cycle even during wind-down
          if (pos.positionType == #fullRange and pos.liquidity > 0) {
            let t0 = Principal.fromText(pos.token0);
            let t1 = Principal.fromText(pos.token1);
            let poolKey = normalizePoolKeyText(pos.token0, pos.token1);
            let currentVal = getCurrentLPValueICP(poolKey);
            await removeLiquidityFromPool(poolKey, t0, t1, currentVal, currentVal, pos.liquidity);
            lpRemoveOps += 1;
          };
        };
      };
    };

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

            // Get trade size and exact targeting flag
            // isExactTargeting = true means slippage adjustment will be applied after quote
            let (tradeSize, isExactTargeting) : (Nat, Bool) = if (useExactTargeting) {
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
              (((calculateTradeSizeMinMax() * (10 ** tokenDetailsSell.tokenDecimals)) / tokenDetailsSell.priceInICP), false);
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

            // === PAIR SKIP CHECK: If this pair previously failed and fell to ICP fallback,
            // skip the direct trade attempt and go straight to ICP fallback ===
            if (shouldSkipPair(sellToken, buyToken) and
                buyToken != ICPprincipal and
                sellToken != ICPprincipal and
                not isTokenPausedFromTrading(ICPprincipal)) {

              let skipSellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
                case (?details) { details.tokenSymbol };
                case null { "UNKNOWN" };
              };
              let skipBuySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                case (?details) { details.tokenSymbol };
                case null { "UNKNOWN" };
              };

              logger.info("PAIR_SKIP",
                "Pair in skip map, going straight to ICP fallback - Original_pair=" # skipSellSymbol # "/" # skipBuySymbol #
                " Fallback_pair=" # skipSellSymbol # "/ICP",
                "do_executeTradingStep"
              );

              // Cap at liquid balance for pair-skip fallback (LP-locked tokens can't be traded)
              // Reserve fee headroom: worst case TACO 5bp + transferFee
              var skipTradeSize = tradeSize;
              switch (Map.get(liquidBalancePerToken, phash, sellToken)) {
                case (?liquid) {
                  let pending = getPendingBurn(sellToken);
                  let avail = if (liquid > pending) { liquid - pending } else { 0 };
                  let sellFee = tokenDetailsSell.tokenTransferFee;
                  let feeRoom = (avail * 5) / 10_000 + sellFee;
                  let maxTrade = if (avail > feeRoom) { avail - feeRoom } else { 0 };
                  if (maxTrade < skipTradeSize) { skipTradeSize := maxTrade };
                };
                case null {};
              };
              if (skipTradeSize == 0) {
                logger.info("REBALANCE", "Pair-skip trade skipped — no liquid balance for sell token", "do_executeTradingStep");
                incrementSkipCounter(#noExecutionPath);
                continue a;
              };

              let skipFallbackExecution = await* findBestExecution(sellToken, ICPprincipal, skipTradeSize);

              switch (skipFallbackExecution) {
                case (#ok(#Single(icpExecution))) {
                  let ourSlippageToleranceBP = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                  let idealOut : Nat = if (icpExecution.slippageBP < 9900) {
                    (icpExecution.expectedOut * 10000) / (10000 - icpExecution.slippageBP)
                  } else {
                    icpExecution.expectedOut
                  };

                  let toleranceMultiplier : Nat = if (ourSlippageToleranceBP >= 10000) { 0 } else { 10000 - ourSlippageToleranceBP };
                  let minAmountOut : Nat = (idealOut * toleranceMultiplier) / 10000;

                  let skipTradeResult = await* executeTrade(
                    sellToken,
                    ICPprincipal,
                    tradeSize,
                    icpExecution.exchange,
                    minAmountOut,
                    idealOut,
                  );

                  switch (skipTradeResult) {
                    case (#ok(record)) {
                      let lastTrades = Vector.clone(rebalanceState.lastTrades);
                      let fallbackRecord : TradeRecord = {
                        record with
                        error = ?("PAIR_SKIP_FALLBACK: Pair " # skipSellSymbol # "/" # skipBuySymbol # " in skip map, traded for ICP instead");
                      };
                      Vector.add(lastTrades, fallbackRecord);
                      if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
                        Vector.reverse(lastTrades);
                        while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                          ignore Vector.removeLast(lastTrades);
                        };
                        Vector.reverse(lastTrades);
                      };

                      // Update ICP price from trade
                      let deviationFromQuote : Float = if (icpExecution.expectedOut > 0) {
                        Float.abs(Float.fromInt(record.amountBought) - Float.fromInt(icpExecution.expectedOut)) / Float.fromInt(icpExecution.expectedOut)
                      } else { 1.0 };
                      let shouldUpdatePrice = isFiniteFloat(deviationFromQuote) and deviationFromQuote <= 0.05;

                      if (shouldUpdatePrice) {
                        let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** tokenDetailsSell.tokenDecimals);
                        let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                        if (actualICP > 0.0) {
                          let priceRatio = (actualTokensSold / actualICP) * Float.fromInt(tokenDetailsSell.priceInICP);
                          if (isFiniteFloat(priceRatio)) {
                            let newICPPrice = Int.abs(Float.toInt(priceRatio));
                            let newICPPriceUSD = tokenDetailsSell.priceInUSD * actualTokensSold / actualICP;
                            updateTokenPriceWithHistory(ICPprincipal, newICPPrice, newICPPriceUSD);
                          };
                        };
                      };

                      rebalanceState := {
                        rebalanceState with
                        metrics = { rebalanceState.metrics with totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1 };
                        lastTrades = lastTrades;
                      };

                      success := true;
                      tradingBackoffLevel := 0;
                      logger.info("PAIR_SKIP",
                        "Pair skip ICP fallback SUCCESS - Sold=" # skipSellSymbol #
                        " Amount_sold=" # Nat.toText(record.amountSold) #
                        " ICP_received=" # Nat.toText(record.amountBought) #
                        " Exchange=" # debug_show(record.exchange),
                        "do_executeTradingStep"
                      );

                      await updateBalances();
                      await takePortfolioSnapshot(#PostTrade);
                      checkPortfolioCircuitBreakerConditions();
                      await* logPortfolioState("Post-pair-skip-ICP-fallback completed");
                    };
                    case (#err(skipError)) {
                      logger.warn("PAIR_SKIP",
                        "Pair skip ICP fallback trade failed - Error=" # skipError,
                        "do_executeTradingStep"
                      );
                      incrementSkipCounter(#noExecutionPath);
                    };
                  };
                };
                case (#ok(#Split(split))) {
                  // SPLIT ICP FALLBACK for PAIR SKIP
                  Debug.print("Pair skip: ICP fallback split route found, executing both legs");
                  logger.info("PAIR_SKIP_SPLIT",
                    "Executing split ICP fallback for skipped pair - Kong=" # Nat.toText(split.kongswap.percentBP / 100) # "%" #
                    " ICP=" # Nat.toText(split.icpswap.percentBP / 100) # "%" #
                    " TACO=" # Nat.toText(split.taco.percentBP / 100) # "%",
                    "do_executeTradingStep"
                  );

                  let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                  // KongSwap leg
                  let kongFinalAmount : Nat = if (isExactTargeting and split.kongswap.slippageBP > 0) {
                    let denominator = 10000 + split.kongswap.slippageBP;
                    (split.kongswap.amount * 10000) / denominator
                  } else { split.kongswap.amount };

                  let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < split.kongswap.amount and split.kongswap.amount > 0) {
                    (split.kongswap.expectedOut * kongFinalAmount) / split.kongswap.amount
                  } else { split.kongswap.expectedOut };

                  let kongIdealOut : Nat = if (split.kongswap.slippageBP < 9900) {
                    (kongAdjustedExpectedOut * 10000) / (10000 - split.kongswap.slippageBP)
                  } else { kongAdjustedExpectedOut };

                  let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                  let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                  // ICPSwap leg
                  let icpFinalAmount : Nat = if (isExactTargeting and split.icpswap.slippageBP > 0) {
                    let denominator = 10000 + split.icpswap.slippageBP;
                    (split.icpswap.amount * 10000) / denominator
                  } else { split.icpswap.amount };

                  let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < split.icpswap.amount and split.icpswap.amount > 0) {
                    (split.icpswap.expectedOut * icpFinalAmount) / split.icpswap.amount
                  } else { split.icpswap.expectedOut };

                  let icpIdealOut : Nat = if (split.icpswap.slippageBP < 9900) {
                    (icpAdjustedExpectedOut * 10000) / (10000 - split.icpswap.slippageBP)
                  } else { icpAdjustedExpectedOut };

                  let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                  let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                  // TACO leg
                  let tacoFinalAmount : Nat = if (isExactTargeting and split.taco.slippageBP > 0) {
                    let denominator = 10000 + split.taco.slippageBP;
                    (split.taco.amount * 10000) / denominator
                  } else { split.taco.amount };
                  
                  let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < split.taco.amount and split.taco.amount > 0) {
                    (split.taco.expectedOut * tacoFinalAmount) / split.taco.amount
                  } else { split.taco.expectedOut };
                  
                  let tacoIdealOut : Nat = if (split.taco.slippageBP < 9900) {
                    (tacoAdjustedExpectedOut * 10000) / (10000 - split.taco.slippageBP)
                  } else { tacoAdjustedExpectedOut };
                  
                  let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                  let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                  // Execute both trades IN PARALLEL to ICP
                  let splitResult = await* executeSplitTrade(
                    sellToken, ICPprincipal,
                    kongFinalAmount, kongMinAmountOut, kongIdealOut,
                    icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                  );

                  var kongSuccess = false;
                  var icpSuccess = false;
                  let lastTrades = Vector.clone(rebalanceState.lastTrades);

                  // Handle KongSwap result
                  switch (splitResult.kongResult) {
                    case (#ok(record)) {
                      let fallbackRecord : TradeRecord = {
                        record with
                        error = ?("PAIR_SKIP_SPLIT: Pair in skip map, original target was " # skipBuySymbol # ", traded for ICP (KongSwap leg)");
                      };
                      Vector.add(lastTrades, fallbackRecord);
                      kongSuccess := true;
                      logger.info("PAIR_SKIP_SPLIT", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                    };
                    case (#err(errKong)) {
                      let failedRecord : TradeRecord = {
                        tokenSold = sellToken; tokenBought = ICPprincipal;
                        amountSold = kongFinalAmount; amountBought = 0;
                        exchange = #KongSwap; timestamp = now();
                        success = false; error = ?("PAIR_SKIP_SPLIT failed: " # errKong); slippage = 0.0;
                      };
                      Vector.add(lastTrades, failedRecord);
                      logger.error("PAIR_SKIP_SPLIT", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                    };
                  };

                  // Handle ICPSwap result
                  switch (splitResult.icpResult) {
                    case (#ok(record)) {
                      let fallbackRecord : TradeRecord = {
                        record with
                        error = ?("PAIR_SKIP_SPLIT: Pair in skip map, original target was " # skipBuySymbol # ", traded for ICP (ICPSwap leg)");
                      };
                      Vector.add(lastTrades, fallbackRecord);
                      icpSuccess := true;
                      logger.info("PAIR_SKIP_SPLIT", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                    };
                    case (#err(errIcp)) {
                      let failedRecord : TradeRecord = {
                        tokenSold = sellToken; tokenBought = ICPprincipal;
                        amountSold = icpFinalAmount; amountBought = 0;
                        exchange = #ICPSwap; timestamp = now();
                        success = false; error = ?("PAIR_SKIP_SPLIT failed: " # errIcp); slippage = 0.0;
                      };
                      Vector.add(lastTrades, failedRecord);
                      logger.error("PAIR_SKIP_SPLIT", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                    };
                  };

                  // Handle TACO result
                  var tacoSuccess = false;
                  switch (splitResult.tacoResult) {
                    case (#ok(record)) {
                      Vector.add(lastTrades, record);
                      tacoSuccess := true;
                      logger.info("PAIR_SKIP_SPLIT", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                    };
                    case (#err(e)) {
                      if (tacoFinalAmount > 0) {
                        let failedRecord : TradeRecord = {
                          tokenSold = sellToken; tokenBought = ICPprincipal;
                          amountSold = tacoFinalAmount; amountBought = 0;
                          exchange = #TACO; timestamp = now();
                          success = false; error = ?e; slippage = 0.0;
                        };
                        Vector.add(lastTrades, failedRecord);
                        logger.error("PAIR_SKIP_SPLIT", "TACO leg failed: " # e, "do_executeTradingStep");
                      };
                    };
                  };

                  // Trim trade history
                  if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                    Vector.reverse(lastTrades);
                    while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                      ignore Vector.removeLast(lastTrades);
                    };
                    Vector.reverse(lastTrades);
                  };

                  let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                  let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                  let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };

                  rebalanceState := {
                    rebalanceState with
                    metrics = {
                      rebalanceState.metrics with
                      totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                      totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                    };
                    lastTrades = lastTrades;
                  };

                  if (kongSuccess or icpSuccess or tacoSuccess) {
                    success := true;
                    tradingBackoffLevel := 0;
                    Debug.print("Pair skip ICP fallback split completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                    await updateBalances();
                    await takePortfolioSnapshot(#PostTrade);
                    checkPortfolioCircuitBreakerConditions();
                    await* logPortfolioState("Post-pair-skip-ICP-fallback-split completed");
                  } else {
                    incrementSkipCounter(#noExecutionPath);
                    rebalanceState := {
                      rebalanceState with
                      metrics = {
                        rebalanceState.metrics with
                        currentStatus = #Failed("Pair skip ICP fallback split: both legs failed");
                      };
                    };
                    await* recoverFromFailure();
                  };
                };

                case (#ok(#Partial(partial))) {
                  // PARTIAL ICP FALLBACK for PAIR SKIP
                  Debug.print("Pair skip: ICP fallback partial route found, executing both legs");
                  logger.info("PAIR_SKIP_PARTIAL",
                    "Executing partial ICP fallback for skipped pair - Kong=" # Nat.toText(partial.kongswap.percentBP / 100) # "%" #
                    " ICP=" # Nat.toText(partial.icpswap.percentBP / 100) # "%" #
                    " TACO=" # Nat.toText(partial.taco.percentBP / 100) # "%" #
                    " Total=" # Nat.toText(partial.totalPercentBP / 100) # "%",
                    "do_executeTradingStep"
                  );

                  let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                  // KongSwap leg
                  let kongFinalAmount : Nat = if (isExactTargeting and partial.kongswap.slippageBP > 0) {
                    let denominator = 10000 + partial.kongswap.slippageBP;
                    (partial.kongswap.amount * 10000) / denominator
                  } else { partial.kongswap.amount };

                  let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < partial.kongswap.amount and partial.kongswap.amount > 0) {
                    (partial.kongswap.expectedOut * kongFinalAmount) / partial.kongswap.amount
                  } else { partial.kongswap.expectedOut };

                  let kongIdealOut : Nat = if (partial.kongswap.slippageBP < 9900) {
                    (kongAdjustedExpectedOut * 10000) / (10000 - partial.kongswap.slippageBP)
                  } else { kongAdjustedExpectedOut };

                  let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                  let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                  // ICPSwap leg
                  let icpFinalAmount : Nat = if (isExactTargeting and partial.icpswap.slippageBP > 0) {
                    let denominator = 10000 + partial.icpswap.slippageBP;
                    (partial.icpswap.amount * 10000) / denominator
                  } else { partial.icpswap.amount };

                  let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < partial.icpswap.amount and partial.icpswap.amount > 0) {
                    (partial.icpswap.expectedOut * icpFinalAmount) / partial.icpswap.amount
                  } else { partial.icpswap.expectedOut };

                  let icpIdealOut : Nat = if (partial.icpswap.slippageBP < 9900) {
                    (icpAdjustedExpectedOut * 10000) / (10000 - partial.icpswap.slippageBP)
                  } else { icpAdjustedExpectedOut };

                  let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                  let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                  // TACO leg
                  let tacoFinalAmount : Nat = if (isExactTargeting and partial.taco.slippageBP > 0) {
                    let denominator = 10000 + partial.taco.slippageBP;
                    (partial.taco.amount * 10000) / denominator
                  } else { partial.taco.amount };
                  
                  let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < partial.taco.amount and partial.taco.amount > 0) {
                    (partial.taco.expectedOut * tacoFinalAmount) / partial.taco.amount
                  } else { partial.taco.expectedOut };
                  
                  let tacoIdealOut : Nat = if (partial.taco.slippageBP < 9900) {
                    (tacoAdjustedExpectedOut * 10000) / (10000 - partial.taco.slippageBP)
                  } else { tacoAdjustedExpectedOut };
                  
                  let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                  let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                  let splitResult = await* executeSplitTrade(
                    sellToken, ICPprincipal,
                    kongFinalAmount, kongMinAmountOut, kongIdealOut,
                    icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                  );

                  var kongSuccess = false;
                  var icpSuccess = false;
                  let lastTrades = Vector.clone(rebalanceState.lastTrades);

                  switch (splitResult.kongResult) {
                    case (#ok(record)) {
                      let fallbackRecord : TradeRecord = {
                        record with
                        error = ?("PAIR_SKIP_PARTIAL: Pair in skip map, original target was " # skipBuySymbol # ", traded for ICP (KongSwap leg)");
                      };
                      Vector.add(lastTrades, fallbackRecord);
                      kongSuccess := true;
                      logger.info("PAIR_SKIP_PARTIAL", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                    };
                    case (#err(errKong)) {
                      let failedRecord : TradeRecord = {
                        tokenSold = sellToken; tokenBought = ICPprincipal;
                        amountSold = kongFinalAmount; amountBought = 0;
                        exchange = #KongSwap; timestamp = now();
                        success = false; error = ?("PAIR_SKIP_PARTIAL failed: " # errKong); slippage = 0.0;
                      };
                      Vector.add(lastTrades, failedRecord);
                      logger.error("PAIR_SKIP_PARTIAL", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                    };
                  };

                  switch (splitResult.icpResult) {
                    case (#ok(record)) {
                      let fallbackRecord : TradeRecord = {
                        record with
                        error = ?("PAIR_SKIP_PARTIAL: Pair in skip map, original target was " # skipBuySymbol # ", traded for ICP (ICPSwap leg)");
                      };
                      Vector.add(lastTrades, fallbackRecord);
                      icpSuccess := true;
                      logger.info("PAIR_SKIP_PARTIAL", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                    };
                    case (#err(errIcp)) {
                      let failedRecord : TradeRecord = {
                        tokenSold = sellToken; tokenBought = ICPprincipal;
                        amountSold = icpFinalAmount; amountBought = 0;
                        exchange = #ICPSwap; timestamp = now();
                        success = false; error = ?("PAIR_SKIP_PARTIAL failed: " # errIcp); slippage = 0.0;
                      };
                      Vector.add(lastTrades, failedRecord);
                      logger.error("PAIR_SKIP_PARTIAL", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                    };
                  };

                  // Handle TACO result
                  var tacoSuccess = false;
                  switch (splitResult.tacoResult) {
                    case (#ok(record)) {
                      Vector.add(lastTrades, record);
                      tacoSuccess := true;
                      logger.info("PAIR_SKIP_PARTIAL", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                    };
                    case (#err(e)) {
                      if (tacoFinalAmount > 0) {
                        let failedRecord : TradeRecord = {
                          tokenSold = sellToken; tokenBought = ICPprincipal;
                          amountSold = tacoFinalAmount; amountBought = 0;
                          exchange = #TACO; timestamp = now();
                          success = false; error = ?e; slippage = 0.0;
                        };
                        Vector.add(lastTrades, failedRecord);
                        logger.error("PAIR_SKIP_PARTIAL", "TACO leg failed: " # e, "do_executeTradingStep");
                      };
                    };
                  };

                  let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                  let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                  let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };
                  rebalanceState := {
                    rebalanceState with
                    metrics = {
                      rebalanceState.metrics with
                      totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                      totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                    };
                    lastTrades = lastTrades;
                  };

                  if (kongSuccess or icpSuccess or tacoSuccess) {
                    success := true;
                    tradingBackoffLevel := 0;
                    Debug.print("Pair skip ICP fallback partial completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                    await updateBalances();
                    await takePortfolioSnapshot(#PostTrade);
                    checkPortfolioCircuitBreakerConditions();
                    await* logPortfolioState("Post-pair-skip-ICP-fallback-partial completed");
                  } else {
                    incrementSkipCounter(#noExecutionPath);
                    rebalanceState := {
                      rebalanceState with
                      metrics = {
                        rebalanceState.metrics with
                        currentStatus = #Failed("Pair skip ICP fallback partial: both legs failed");
                      };
                    };
                    await* recoverFromFailure();
                  };
                };
                case (#err(skipRouteError)) {
                  logger.warn("PAIR_SKIP",
                    "Pair skip ICP fallback failed at full size - Error=" # skipRouteError.reason,
                    "do_executeTradingStep"
                  );

                  // NEW: Try REDUCED amount for ICP fallback (reuses quotes from skipRouteError)
                  label reducedPairSkipFallback switch (estimateMaxTradeableAmount(
                    skipRouteError.kongQuotes, skipRouteError.icpQuotes, tradeSize,
                    rebalanceConfig.maxSlippageBasisPoints, sellToken, ICPprincipal
                  )) {
                    case (?reduced) {
                      if (reduced.icpWorth < rebalanceConfig.minTradeValueICP / 3) {
                        logger.info("SKIP_REDUCED",
                          "Reduced pair skip ICP fallback too small - IcpWorth=" # Nat.toText(reduced.icpWorth) #
                          " < " # Nat.toText(rebalanceConfig.minTradeValueICP / 3),
                          "do_executeTradingStep"
                        );
                        break reducedPairSkipFallback;
                      };

                      logger.info("PAIR_SKIP_REDUCED_ICP",
                        "Executing reduced ICP fallback for skipped pair - Original=" # Nat.toText(tradeSize) #
                        " Reduced=" # Nat.toText(reduced.amount) #
                        " Exchange=" # debug_show(reduced.exchange) #
                        " Pair=" # skipSellSymbol # "/" # skipBuySymbol #
                        " Fallback=" # skipSellSymbol # "/ICP" #
                        " IcpWorth=" # Nat.toText(reduced.icpWorth),
                        "do_executeTradingStep"
                      );

                      let reducedIcpTradeResult = await* executeTrade(
                        sellToken, ICPprincipal,
                        reduced.amount, reduced.exchange,
                        reduced.minAmountOut, reduced.idealOut
                      );

                      switch (reducedIcpTradeResult) {
                        case (#ok(record)) {
                          let lastTrades = Vector.clone(rebalanceState.lastTrades);
                          let reducedIcpRecord : TradeRecord = {
                            record with
                            error = ?("PAIR_SKIP_REDUCED_ICP: Pair in skip map, original target was " # skipBuySymbol # ", traded reduced amount for ICP");
                          };
                          Vector.add(lastTrades, reducedIcpRecord);

                          if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
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
                              totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                            };
                            lastTrades = lastTrades;
                          };

                          success := true;
                          tradingBackoffLevel := 0;

                          logger.info("PAIR_SKIP_REDUCED_ICP",
                            "REDUCED ICP fallback SUCCESS - Sold=" # Nat.toText(record.amountSold) #
                            " ICP_received=" # Nat.toText(record.amountBought) #
                            " Exchange=" # debug_show(record.exchange),
                            "do_executeTradingStep"
                          );

                          await updateBalances();
                          await takePortfolioSnapshot(#PostTrade);
                          checkPortfolioCircuitBreakerConditions();
                          await* logPortfolioState("Post-PAIR-SKIP-REDUCED-ICP-fallback");
                        };
                        case (#err(reducedIcpError)) {
                          logger.warn("PAIR_SKIP_REDUCED_ICP",
                            "Reduced ICP fallback also failed - Error=" # reducedIcpError,
                            "do_executeTradingStep"
                          );
                        };
                      };
                    };
                    case null {
                      Debug.print("No viable reduced ICP fallback amount for pair skip");
                    };
                  };

                  // If reduced succeeded, return early (success will be true)
                  // Otherwise continue to skip counter
                  if (not success) {
                    incrementSkipCounter(#noExecutionPath);
                  };
                };
              };
              continue a;
            };

            // Cap trade size at liquid balance (can't spend LP-locked tokens)
            // Reserve headroom for exchange fees: TACO adds 5bp + transferFee on top of amountIn
            // Worst case = 100% routed through TACO = amountIn * 10005/10000 + transferFee
            var cappedTradeSize = tradeSize;
            switch (Map.get(liquidBalancePerToken, phash, sellToken)) {
              case (?liquid) {
                let pending = getPendingBurn(sellToken);
                let availLiquid = if (liquid > pending) { liquid - pending } else { 0 };
                // Subtract fee headroom: worst case all goes via TACO (5bp) + one transfer fee
                let sellFee = tokenDetailsSell.tokenTransferFee;
                let feeHeadroom = (availLiquid * 5) / 10_000 + sellFee;
                let maxTradeable = if (availLiquid > feeHeadroom) { availLiquid - feeHeadroom } else { 0 };
                if (maxTradeable < cappedTradeSize) {
                  cappedTradeSize := maxTradeable;
                  if (cappedTradeSize == 0) {
                    logger.info("REBALANCE", "Trade skipped — no liquid balance (after fee headroom) for sell token", "do_executeTradingStep");
                    continue a;
                  };
                };
              };
              case null {}; // LP not initialized, no cap
            };

            let bestExecution = await* findBestExecution(sellToken, buyToken, cappedTradeSize);

            switch (bestExecution) {
              case (#ok(plan)) {
                // Handle both #Single and #Split execution plans
                switch (plan) {
                  case (#Single(execution)) {
                    // SINGLE EXCHANGE EXECUTION (existing Phase 1 logic with slippageBP)
                    let slippageBasisPoints : Nat = execution.slippageBP;

                    // SLIPPAGE ADJUSTMENT: Reduce trade size to compensate for price impact
                    // This prevents overshoot when using exact targeting
                    // Formula: adjusted = size * 10000 / (10000 + slippageBP)
                    let finalTradeSize : Nat = if (isExactTargeting and slippageBasisPoints > 0) {
                      let denominator = 10000 + slippageBasisPoints;
                      let adjusted = (cappedTradeSize * 10000) / denominator;

                      // Safety check: if adjusted size exceeds max, fall back to random
                      let adjustedICP = (adjusted * tokenDetailsSell.priceInICP) / (10 ** tokenDetailsSell.tokenDecimals);
                      if (adjustedICP > rebalanceConfig.maxTradeValueICP) {
                        Debug.print("Adjusted trade exceeds max, falling back to random size");
                        ((calculateTradeSizeMinMax() * (10 ** tokenDetailsSell.tokenDecimals)) / tokenDetailsSell.priceInICP)
                      } else {
                        logger.info("SLIPPAGE_ADJUSTMENT",
                          "Adjusting single trade for slippage - Original=" # Nat.toText(cappedTradeSize) #
                          " Adjusted=" # Nat.toText(adjusted) #
                          " SlippageBP=" # Nat.toText(slippageBasisPoints),
                          "do_executeTradingStep"
                        );
                        adjusted
                      }
                    } else {
                      cappedTradeSize
                    };

                    // Adjust expected output proportionally if we reduced trade size
                    let adjustedExpectedOut : Nat = if (finalTradeSize < cappedTradeSize and cappedTradeSize > 0) {
                      (execution.expectedOut * finalTradeSize) / tradeSize
                    } else {
                      execution.expectedOut
                    };

                    let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                    // Calculate ideal output
                    let idealOut : Nat = if (slippageBasisPoints < 9900) {
                      (adjustedExpectedOut * 10000) / (10000 - slippageBasisPoints)
                    } else {
                      adjustedExpectedOut
                    };

                    // Apply our slippage tolerance
                    let toleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                    let minAmountOut : Nat = (idealOut * toleranceMultiplier) / 10000;

                    let tradeResult = await* executeTrade(
                      sellToken,
                      buyToken,
                      finalTradeSize,
                      execution.exchange,
                      minAmountOut,
                      idealOut,
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
                          case (null) { Debug.print("Error: Sell token not found in details"); return; };
                        };
                        let token1Details = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                          case (?details) { details };
                          case (null) { Debug.print("Error: Buy token not found in details"); return; };
                        };

                        // Check if execution matches quote (5% tolerance) before updating price
                        let deviationFromQuote : Float = if (adjustedExpectedOut > 0) {
                          Float.abs(Float.fromInt(record.amountBought) - Float.fromInt(adjustedExpectedOut)) / Float.fromInt(adjustedExpectedOut)
                        } else { 1.0 };
                        let shouldUpdatePrice = isFiniteFloat(deviationFromQuote) and deviationFromQuote <= 0.05;

                        if (not shouldUpdatePrice) {
                          logger.warn("PRICE_SKIP",
                            "Execution deviated " # Float.toText(deviationFromQuote * 100.0) # "% from quote - skipping price update",
                            "do_executeTradingStep");
                        };

                        // Calculate prices based on trade - only if deviation is acceptable
                        // Guard: Only update price if we have valid amounts to avoid division by zero / Inf
                        if (shouldUpdatePrice and (sellToken == ICPprincipal or buyToken == ICPprincipal)) {
                          if (sellToken == ICPprincipal) {
                            let actualTokens = Float.fromInt(record.amountBought) / Float.fromInt(10 ** token1Details.tokenDecimals);
                            let actualICP = Float.fromInt(record.amountSold) / Float.fromInt(100000000);
                            // Guard against division by zero (actualTokens = 0 would produce Inf)
                            if (actualTokens > 0.0) {
                              let ratio = actualICP / actualTokens;
                              let scaledPrice = ratio * 100000000;
                              if (isFiniteFloat(scaledPrice)) {
                                let newPriceInICP = Int.abs(Float.toInt(scaledPrice));
                                let newPriceInUSD = token0Details.priceInUSD * ratio;
                                updateTokenPriceWithHistory(buyToken, newPriceInICP, newPriceInUSD);
                              };
                            };
                          } else {
                            let actualTokens = Float.fromInt(record.amountSold) / Float.fromInt(10 ** token0Details.tokenDecimals);
                            let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                            // Guard against division by zero (actualTokens = 0 would produce Inf)
                            if (actualTokens > 0.0) {
                              let ratio = actualICP / actualTokens;
                              let scaledPrice = ratio * 100000000;
                              if (isFiniteFloat(scaledPrice)) {
                                let newPriceInICP = Int.abs(Float.toInt(scaledPrice));
                                let newPriceInUSD = token1Details.priceInUSD * ratio;
                                updateTokenPriceWithHistory(sellToken, newPriceInICP, newPriceInUSD);
                              };
                            };
                          };
                        } else if (shouldUpdatePrice) {
                          let maintainFirst = fuzz.nat.randomRange(0, 1) == 0;
                          if (maintainFirst) {
                            let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** token0Details.tokenDecimals);
                            let actualTokensBought = Float.fromInt(record.amountBought) / Float.fromInt(10 ** token1Details.tokenDecimals);
                            // Guard against division by zero
                            if (actualTokensBought > 0.0) {
                              let priceRatio = actualTokensSold / actualTokensBought;
                              let scaledPrice = Float.fromInt(token0Details.priceInICP) * priceRatio;
                              if (isFiniteFloat(scaledPrice)) {
                                let newPriceInICP = Int.abs(Float.toInt(scaledPrice));
                                let newPriceInUSD = token0Details.priceInUSD * priceRatio;
                                updateTokenPriceWithHistory(buyToken, newPriceInICP, newPriceInUSD);
                              };
                            };
                          } else {
                            let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** token0Details.tokenDecimals);
                            let actualTokensBought = Float.fromInt(record.amountBought) / Float.fromInt(10 ** token1Details.tokenDecimals);
                            // Guard against division by zero
                            if (actualTokensSold > 0.0) {
                              let priceRatio = actualTokensBought / actualTokensSold;
                              let scaledPrice = Float.fromInt(token1Details.priceInICP) * priceRatio;
                              if (isFiniteFloat(scaledPrice)) {
                                let newPriceInICP = Int.abs(Float.toInt(scaledPrice));
                                let newPriceInUSD = token1Details.priceInUSD * priceRatio;
                                updateTokenPriceWithHistory(sellToken, newPriceInICP, newPriceInUSD);
                              };
                            };
                          };
                        };

                        rebalanceState := {
                          rebalanceState with
                          metrics = { rebalanceState.metrics with totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1 };
                          lastTrades = lastTrades;
                        };
                        success := true;
                        tradingBackoffLevel := 0;
                        Debug.print("Single trade executed successfully");
                        await updateBalances();
                        await takePortfolioSnapshot(#PostTrade);
                        checkPortfolioCircuitBreakerConditions();
                        await* logPortfolioState("Post-trade completed (single)");
                      };
                      case (#err(errorMsg)) {
                        // Log the initial failure
                        let sellSymbol = tokenDetailsSell.tokenSymbol;
                        let buySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                          case (?details) { details.tokenSymbol };
                          case null { "UNKNOWN" };
                        };

                        logger.warn("TRADE_FAILED",
                          "Single trade execution failed - Pair=" # sellSymbol # "/" # buySymbol #
                          " Exchange=" # debug_show(execution.exchange) #
                          " Error=" # errorMsg,
                          "do_executeTradingStep"
                        );

                        // Record the failed attempt
                        let failedRecord : TradeRecord = {
                          tokenSold = sellToken;
                          tokenBought = buyToken;
                          amountSold = finalTradeSize;
                          amountBought = 0;
                          exchange = execution.exchange;
                          timestamp = now();
                          success = false;
                          error = ?errorMsg;
                          slippage = 0.0;
                        };
                        let lastTrades = Vector.clone(rebalanceState.lastTrades);
                        Vector.add(lastTrades, failedRecord);
                        if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                          Vector.reverse(lastTrades);
                          while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                            ignore Vector.removeLast(lastTrades);
                          };
                          Vector.reverse(lastTrades);
                        };

                        // === ICP FALLBACK: Try selling for ICP instead if eligible ===
                        // Works for both ICPSwap and KongSwap failures - tokens are available or will be claimed
                        var fallbackSucceeded = false;

                        if ((execution.exchange == #ICPSwap or execution.exchange == #KongSwap) and
                            buyToken != ICPprincipal and
                            sellToken != ICPprincipal and
                            not isTokenPausedFromTrading(ICPprincipal)) {
                          // Record this pair in the skip map so next time we go straight to ICP fallback
                          addSkipPair(sellToken, buyToken);

                          logger.info("ICP_FALLBACK",
                            "Execution failed, attempting ICP fallback - Original_pair=" # sellSymbol # "/" # buySymbol #
                            " Fallback_pair=" # sellSymbol # "/ICP" #
                            " Original_error=" # errorMsg,
                            "do_executeTradingStep"
                          );

                          let icpFallbackExecution = await* findBestExecution(sellToken, ICPprincipal, finalTradeSize);

                          switch (icpFallbackExecution) {
                            case (#ok(#Single(icpExecution))) {
                              let ourSlippageToleranceBP = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                              let idealOut : Nat = if (icpExecution.slippageBP < 9900) {
                                (icpExecution.expectedOut * 10000) / (10000 - icpExecution.slippageBP)
                              } else {
                                icpExecution.expectedOut
                              };

                              let toleranceMultiplier : Nat = if (ourSlippageToleranceBP >= 10000) { 0 } else { 10000 - ourSlippageToleranceBP };
                              let minAmountOut : Nat = (idealOut * toleranceMultiplier) / 10000;

                              let icpTradeResult = await* executeTrade(
                                sellToken,
                                ICPprincipal,
                                finalTradeSize,
                                icpExecution.exchange,
                                minAmountOut,
                                idealOut,
                              );

                              switch (icpTradeResult) {
                                case (#ok(record)) {
                                  // ICP fallback succeeded!
                                  let fallbackRecord : TradeRecord = {
                                    record with
                                    error = ?("ICP_FALLBACK: Original " # buySymbol # " trade failed (" # errorMsg # "), sold for ICP instead");
                                  };

                                  Vector.add(lastTrades, fallbackRecord);
                                  if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                    Vector.reverse(lastTrades);
                                    while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                      ignore Vector.removeLast(lastTrades);
                                    };
                                    Vector.reverse(lastTrades);
                                  };

                                  // Update price if deviation acceptable
                                  let deviationFromQuote : Float = if (icpExecution.expectedOut > 0) {
                                    Float.abs(Float.fromInt(record.amountBought) - Float.fromInt(icpExecution.expectedOut)) / Float.fromInt(icpExecution.expectedOut)
                                  } else { 1.0 };
                                  let shouldUpdatePrice = isFiniteFloat(deviationFromQuote) and deviationFromQuote <= 0.05;

                                  if (shouldUpdatePrice) {
                                    let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** tokenDetailsSell.tokenDecimals);
                                    let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                                    if (actualICP > 0.0) {
                                      let priceRatio = (actualTokensSold / actualICP) * Float.fromInt(tokenDetailsSell.priceInICP);
                                      if (isFiniteFloat(priceRatio)) {
                                        let newICPPrice = Int.abs(Float.toInt(priceRatio));
                                        let newICPPriceUSD = tokenDetailsSell.priceInUSD * actualTokensSold / actualICP;
                                        updateTokenPriceWithHistory(ICPprincipal, newICPPrice, newICPPriceUSD);
                                      };
                                    };
                                  };

                                  rebalanceState := {
                                    rebalanceState with
                                    metrics = { rebalanceState.metrics with totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1 };
                                    lastTrades = lastTrades;
                                  };

                                  success := true;
                                  tradingBackoffLevel := 0;
                                  fallbackSucceeded := true;

                                  logger.info("ICP_FALLBACK",
                                    "ICP fallback SUCCESS after execution failure - Sold=" # sellSymbol #
                                    " Amount_sold=" # Nat.toText(record.amountSold) #
                                    " ICP_received=" # Nat.toText(record.amountBought) #
                                    " Exchange=" # debug_show(record.exchange),
                                    "do_executeTradingStep"
                                  );

                                  await updateBalances();
                                  await takePortfolioSnapshot(#PostTrade);
                                  checkPortfolioCircuitBreakerConditions();
                                  await* logPortfolioState("Post-ICP-fallback (after exec failure)");
                                };
                                case (#err(icpError)) {
                                  logger.warn("ICP_FALLBACK",
                                    "ICP fallback also failed - Error=" # icpError,
                                    "do_executeTradingStep"
                                  );
                                };
                              };
                            };
                            case (#ok(#Split(split))) {
                              // SPLIT ICP FALLBACK after Single execution failure
                              Debug.print("ICP fallback split route found after single exec failure, executing both legs");
                              logger.info("ICP_FALLBACK_SPLIT",
                                "Executing split ICP fallback after single failure - Kong=" # Nat.toText(split.kongswap.percentBP / 100) # "%" #
                                " ICP=" # Nat.toText(split.icpswap.percentBP / 100) # "%" #
                                " TACO=" # Nat.toText(split.taco.percentBP / 100) # "%",
                                "do_executeTradingStep"
                              );

                              let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                              // KongSwap leg
                              let kongFinalAmount : Nat = if (isExactTargeting and split.kongswap.slippageBP > 0) {
                                let denominator = 10000 + split.kongswap.slippageBP;
                                (split.kongswap.amount * 10000) / denominator
                              } else { split.kongswap.amount };

                              let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < split.kongswap.amount and split.kongswap.amount > 0) {
                                (split.kongswap.expectedOut * kongFinalAmount) / split.kongswap.amount
                              } else { split.kongswap.expectedOut };

                              let kongIdealOut : Nat = if (split.kongswap.slippageBP < 9900) {
                                (kongAdjustedExpectedOut * 10000) / (10000 - split.kongswap.slippageBP)
                              } else { kongAdjustedExpectedOut };

                              let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                              let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                              // ICPSwap leg
                              let icpFinalAmount : Nat = if (isExactTargeting and split.icpswap.slippageBP > 0) {
                                let denominator = 10000 + split.icpswap.slippageBP;
                                (split.icpswap.amount * 10000) / denominator
                              } else { split.icpswap.amount };

                              let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < split.icpswap.amount and split.icpswap.amount > 0) {
                                (split.icpswap.expectedOut * icpFinalAmount) / split.icpswap.amount
                              } else { split.icpswap.expectedOut };

                              let icpIdealOut : Nat = if (split.icpswap.slippageBP < 9900) {
                                (icpAdjustedExpectedOut * 10000) / (10000 - split.icpswap.slippageBP)
                              } else { icpAdjustedExpectedOut };

                              let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                              let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                              // TACO leg
                              let tacoFinalAmount : Nat = if (isExactTargeting and split.taco.slippageBP > 0) {
                                let denominator = 10000 + split.taco.slippageBP;
                                (split.taco.amount * 10000) / denominator
                              } else { split.taco.amount };
                              
                              let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < split.taco.amount and split.taco.amount > 0) {
                                (split.taco.expectedOut * tacoFinalAmount) / split.taco.amount
                              } else { split.taco.expectedOut };
                              
                              let tacoIdealOut : Nat = if (split.taco.slippageBP < 9900) {
                                (tacoAdjustedExpectedOut * 10000) / (10000 - split.taco.slippageBP)
                              } else { tacoAdjustedExpectedOut };
                              
                              let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                              let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                              let splitResult = await* executeSplitTrade(
                                sellToken, ICPprincipal,
                                kongFinalAmount, kongMinAmountOut, kongIdealOut,
                                icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                              );

                              var kongSuccess = false;
                              var icpSuccess = false;

                              // Handle KongSwap result
                              switch (splitResult.kongResult) {
                                case (#ok(record)) {
                                  let fallbackRecord : TradeRecord = {
                                    record with
                                    error = ?("ICP_FALLBACK_SPLIT: Original " # buySymbol # " trade failed, sold for ICP (KongSwap leg)");
                                  };
                                  Vector.add(lastTrades, fallbackRecord);
                                  kongSuccess := true;
                                  logger.info("ICP_FALLBACK_SPLIT", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                                };
                                case (#err(errKong)) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = kongFinalAmount; amountBought = 0;
                                    exchange = #KongSwap; timestamp = now();
                                    success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errKong); slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_SPLIT", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                                };
                              };

                              // Handle ICPSwap result
                              switch (splitResult.icpResult) {
                                case (#ok(record)) {
                                  let fallbackRecord : TradeRecord = {
                                    record with
                                    error = ?("ICP_FALLBACK_SPLIT: Original " # buySymbol # " trade failed, sold for ICP (ICPSwap leg)");
                                  };
                                  Vector.add(lastTrades, fallbackRecord);
                                  icpSuccess := true;
                                  logger.info("ICP_FALLBACK_SPLIT", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                                };
                                case (#err(errIcp)) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = icpFinalAmount; amountBought = 0;
                                    exchange = #ICPSwap; timestamp = now();
                                    success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errIcp); slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_SPLIT", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                                };
                              };

                              // Handle TACO result
                              var tacoSuccess = false;
                              switch (splitResult.tacoResult) {
                                case (#ok(record)) {
                                  Vector.add(lastTrades, record);
                                  tacoSuccess := true;
                                  logger.info("ICP_FALLBACK_SPLIT", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                                };
                                case (#err(e)) {
                                  if (tacoFinalAmount > 0) {
                                    let failedRecord : TradeRecord = {
                                      tokenSold = sellToken; tokenBought = ICPprincipal;
                                      amountSold = tacoFinalAmount; amountBought = 0;
                                      exchange = #TACO; timestamp = now();
                                      success = false; error = ?e; slippage = 0.0;
                                    };
                                    Vector.add(lastTrades, failedRecord);
                                    logger.error("ICP_FALLBACK_SPLIT", "TACO leg failed: " # e, "do_executeTradingStep");
                                  };
                                };
                              };

                              // Trim trade history
                              if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                Vector.reverse(lastTrades);
                                while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                  ignore Vector.removeLast(lastTrades);
                                };
                                Vector.reverse(lastTrades);
                              };

                              let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                              let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                              let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };

                              rebalanceState := {
                                rebalanceState with
                                metrics = {
                                  rebalanceState.metrics with
                                  totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                                  totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                                };
                                lastTrades = lastTrades;
                              };

                              if (kongSuccess or icpSuccess or tacoSuccess) {
                                success := true;
                                fallbackSucceeded := true;  // CRITICAL: Set this for outer code check
                                tradingBackoffLevel := 0;
                                Debug.print("ICP fallback split after single failure completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                                await updateBalances();
                                await takePortfolioSnapshot(#PostTrade);
                                checkPortfolioCircuitBreakerConditions();
                                await* logPortfolioState("Post-ICP-fallback-split (after single exec failure)");
                              } else {
                                // Both legs failed - fallbackSucceeded remains false, outer code will handle metrics
                                rebalanceState := {
                                  rebalanceState with
                                  metrics = {
                                    rebalanceState.metrics with
                                    currentStatus = #Failed("ICP fallback split after single failure: both legs failed");
                                  };
                                };
                              };
                            };

                            case (#ok(#Partial(partial))) {
                              // PARTIAL ICP FALLBACK after Single execution failure
                              Debug.print("ICP fallback partial route found after single exec failure, executing both legs");
                              logger.info("ICP_FALLBACK_PARTIAL",
                                "Executing partial ICP fallback after single failure - Kong=" # Nat.toText(partial.kongswap.percentBP / 100) # "%" #
                                " ICP=" # Nat.toText(partial.icpswap.percentBP / 100) # "%" #
                                " TACO=" # Nat.toText(partial.taco.percentBP / 100) # "%" #
                                " Total=" # Nat.toText(partial.totalPercentBP / 100) # "%",
                                "do_executeTradingStep"
                              );

                              let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                              // KongSwap leg
                              let kongFinalAmount : Nat = if (isExactTargeting and partial.kongswap.slippageBP > 0) {
                                let denominator = 10000 + partial.kongswap.slippageBP;
                                (partial.kongswap.amount * 10000) / denominator
                              } else { partial.kongswap.amount };

                              let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < partial.kongswap.amount and partial.kongswap.amount > 0) {
                                (partial.kongswap.expectedOut * kongFinalAmount) / partial.kongswap.amount
                              } else { partial.kongswap.expectedOut };

                              let kongIdealOut : Nat = if (partial.kongswap.slippageBP < 9900) {
                                (kongAdjustedExpectedOut * 10000) / (10000 - partial.kongswap.slippageBP)
                              } else { kongAdjustedExpectedOut };

                              let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                              let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                              // ICPSwap leg
                              let icpFinalAmount : Nat = if (isExactTargeting and partial.icpswap.slippageBP > 0) {
                                let denominator = 10000 + partial.icpswap.slippageBP;
                                (partial.icpswap.amount * 10000) / denominator
                              } else { partial.icpswap.amount };

                              let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < partial.icpswap.amount and partial.icpswap.amount > 0) {
                                (partial.icpswap.expectedOut * icpFinalAmount) / partial.icpswap.amount
                              } else { partial.icpswap.expectedOut };

                              let icpIdealOut : Nat = if (partial.icpswap.slippageBP < 9900) {
                                (icpAdjustedExpectedOut * 10000) / (10000 - partial.icpswap.slippageBP)
                              } else { icpAdjustedExpectedOut };

                              let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                              let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                              // TACO leg
                              let tacoFinalAmount : Nat = if (isExactTargeting and partial.taco.slippageBP > 0) {
                                let denominator = 10000 + partial.taco.slippageBP;
                                (partial.taco.amount * 10000) / denominator
                              } else { partial.taco.amount };
                              
                              let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < partial.taco.amount and partial.taco.amount > 0) {
                                (partial.taco.expectedOut * tacoFinalAmount) / partial.taco.amount
                              } else { partial.taco.expectedOut };
                              
                              let tacoIdealOut : Nat = if (partial.taco.slippageBP < 9900) {
                                (tacoAdjustedExpectedOut * 10000) / (10000 - partial.taco.slippageBP)
                              } else { tacoAdjustedExpectedOut };
                              
                              let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                              let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                              let splitResult = await* executeSplitTrade(
                                sellToken, ICPprincipal,
                                kongFinalAmount, kongMinAmountOut, kongIdealOut,
                                icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                              );

                              var kongSuccess = false;
                              var icpSuccess = false;

                              switch (splitResult.kongResult) {
                                case (#ok(record)) {
                                  let fallbackRecord : TradeRecord = {
                                    record with
                                    error = ?("ICP_FALLBACK_PARTIAL: Original " # buySymbol # " trade failed, sold for ICP (KongSwap leg)");
                                  };
                                  Vector.add(lastTrades, fallbackRecord);
                                  kongSuccess := true;
                                  logger.info("ICP_FALLBACK_PARTIAL", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                                };
                                case (#err(errKong)) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = kongFinalAmount; amountBought = 0;
                                    exchange = #KongSwap; timestamp = now();
                                    success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errKong); slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_PARTIAL", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                                };
                              };

                              switch (splitResult.icpResult) {
                                case (#ok(record)) {
                                  let fallbackRecord : TradeRecord = {
                                    record with
                                    error = ?("ICP_FALLBACK_PARTIAL: Original " # buySymbol # " trade failed, sold for ICP (ICPSwap leg)");
                                  };
                                  Vector.add(lastTrades, fallbackRecord);
                                  icpSuccess := true;
                                  logger.info("ICP_FALLBACK_PARTIAL", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                                };
                                case (#err(errIcp)) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = icpFinalAmount; amountBought = 0;
                                    exchange = #ICPSwap; timestamp = now();
                                    success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errIcp); slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_PARTIAL", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                                };
                              };

                              // Handle TACO result
                              var tacoSuccess = false;
                              switch (splitResult.tacoResult) {
                                case (#ok(record)) {
                                  Vector.add(lastTrades, record);
                                  tacoSuccess := true;
                                  logger.info("ICP_FALLBACK_PARTIAL", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                                };
                                case (#err(e)) {
                                  if (tacoFinalAmount > 0) {
                                    let failedRecord : TradeRecord = {
                                      tokenSold = sellToken; tokenBought = ICPprincipal;
                                      amountSold = tacoFinalAmount; amountBought = 0;
                                      exchange = #TACO; timestamp = now();
                                      success = false; error = ?e; slippage = 0.0;
                                    };
                                    Vector.add(lastTrades, failedRecord);
                                    logger.error("ICP_FALLBACK_PARTIAL", "TACO leg failed: " # e, "do_executeTradingStep");
                                  };
                                };
                              };

                              let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                              let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                              let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };
                              rebalanceState := {
                                rebalanceState with
                                metrics = {
                                  rebalanceState.metrics with
                                  totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                                  totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                                };
                                lastTrades = lastTrades;
                              };

                              if (kongSuccess or icpSuccess or tacoSuccess) {
                                success := true;
                                fallbackSucceeded := true;  // CRITICAL: Set this for outer code check
                                tradingBackoffLevel := 0;
                                Debug.print("ICP fallback partial after single failure completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                                await updateBalances();
                                await takePortfolioSnapshot(#PostTrade);
                                checkPortfolioCircuitBreakerConditions();
                                await* logPortfolioState("Post-ICP-fallback-partial (after single exec failure)");
                              } else {
                                // Both legs failed - fallbackSucceeded remains false, outer code will handle metrics
                                rebalanceState := {
                                  rebalanceState with
                                  metrics = {
                                    rebalanceState.metrics with
                                    currentStatus = #Failed("ICP fallback partial after single failure: both legs failed");
                                  };
                                };
                              };
                            };
                            case (#err(icpRouteError)) {
                              logger.warn("ICP_FALLBACK",
                                "ICP fallback failed at full size after single exec failure - Error=" # icpRouteError.reason,
                                "do_executeTradingStep"
                              );

                              // NEW: Try REDUCED amount for ICP fallback (reuses quotes from icpRouteError)
                              label reducedIcpFallbackAfterSingle switch (estimateMaxTradeableAmount(
                                icpRouteError.kongQuotes, icpRouteError.icpQuotes, finalTradeSize,
                                rebalanceConfig.maxSlippageBasisPoints, sellToken, ICPprincipal
                              )) {
                                case (?reduced) {
                                  if (reduced.icpWorth < rebalanceConfig.minTradeValueICP / 3) {
                                    logger.info("SKIP_REDUCED",
                                      "Reduced ICP fallback after single exec failure too small - IcpWorth=" # Nat.toText(reduced.icpWorth) #
                                      " < " # Nat.toText(rebalanceConfig.minTradeValueICP / 3),
                                      "do_executeTradingStep"
                                    );
                                    break reducedIcpFallbackAfterSingle;
                                  };

                                  logger.info("REDUCED_ICP_FALLBACK",
                                    "Executing reduced ICP fallback after single exec failure - Original=" # Nat.toText(finalTradeSize) #
                                    " Reduced=" # Nat.toText(reduced.amount) #
                                    " Exchange=" # debug_show(reduced.exchange) #
                                    " Pair=" # sellSymbol # "/" # buySymbol #
                                    " Fallback=" # sellSymbol # "/ICP" #
                                    " IcpWorth=" # Nat.toText(reduced.icpWorth),
                                    "do_executeTradingStep"
                                  );

                                  let reducedIcpTradeResult = await* executeTrade(
                                    sellToken, ICPprincipal,
                                    reduced.amount, reduced.exchange,
                                    reduced.minAmountOut, reduced.idealOut
                                  );

                                  switch (reducedIcpTradeResult) {
                                    case (#ok(record)) {
                                      let reducedIcpRecord : TradeRecord = {
                                        record with
                                        error = ?("REDUCED_ICP_FALLBACK: Original " # buySymbol # " trade failed, traded reduced amount for ICP");
                                      };
                                      Vector.add(lastTrades, reducedIcpRecord);

                                      if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
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
                                          totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                                        };
                                        lastTrades = lastTrades;
                                      };

                                      success := true;
                                      fallbackSucceeded := true;  // CRITICAL: Prevents outer code from double-counting failure
                                      tradingBackoffLevel := 0;

                                      logger.info("REDUCED_ICP_FALLBACK",
                                        "REDUCED ICP fallback SUCCESS after single exec failure - Sold=" # Nat.toText(record.amountSold) #
                                        " ICP_received=" # Nat.toText(record.amountBought) #
                                        " Exchange=" # debug_show(record.exchange),
                                        "do_executeTradingStep"
                                      );

                                      await updateBalances();
                                      await takePortfolioSnapshot(#PostTrade);
                                      checkPortfolioCircuitBreakerConditions();
                                      await* logPortfolioState("Post-REDUCED-ICP-fallback (after single exec failure)");
                                    };
                                    case (#err(reducedIcpError)) {
                                      logger.warn("REDUCED_ICP_FALLBACK",
                                        "Reduced ICP fallback also failed after single exec failure - Error=" # reducedIcpError,
                                        "do_executeTradingStep"
                                      );
                                      // fallbackSucceeded remains false, outer code at line 5402 will handle metrics
                                    };
                                  };
                                };
                                case null {
                                  Debug.print("No viable reduced ICP fallback amount after single exec failure");
                                };
                              };
                            };
                          };
                        };

                        // If fallback didn't succeed, record the failure
                        if (not fallbackSucceeded) {
                          rebalanceState := {
                            rebalanceState with
                            metrics = { rebalanceState.metrics with totalTradesFailed = rebalanceState.metrics.totalTradesFailed + 1 };
                            lastTrades = lastTrades;
                          };
                        };
                      };
                    };
                  };

                  case (#Split(split)) {
                    // SPLIT TRADE EXECUTION (Phase 2: Anti-arb)
                    logger.info("TRADE_SPLIT",
                      "Executing split trade - Kong=" # Nat.toText(split.kongswap.percentBP / 100) # "%" #
                      " (" # Nat.toText(split.kongswap.amount) # " tokens)" #
                      " ICP=" # Nat.toText(split.icpswap.percentBP / 100) # "%" #
                      " TACO=" # Nat.toText(split.taco.percentBP / 100) # "%" #
                      " (" # Nat.toText(split.icpswap.amount) # " tokens)",
                      "do_executeTradingStep"
                    );

                    let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                    // Apply slippage adjustment to BOTH legs (Phase 1 logic for each)
                    // KongSwap leg
                    let kongFinalAmount : Nat = if (isExactTargeting and split.kongswap.slippageBP > 0) {
                      let denominator = 10000 + split.kongswap.slippageBP;
                      (split.kongswap.amount * 10000) / denominator
                    } else { split.kongswap.amount };

                    let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < split.kongswap.amount and split.kongswap.amount > 0) {
                      (split.kongswap.expectedOut * kongFinalAmount) / split.kongswap.amount
                    } else { split.kongswap.expectedOut };

                    let kongIdealOut : Nat = if (split.kongswap.slippageBP < 9900) {
                      (kongAdjustedExpectedOut * 10000) / (10000 - split.kongswap.slippageBP)
                    } else { kongAdjustedExpectedOut };

                    let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                    let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                    // ICPSwap leg
                    let icpFinalAmount : Nat = if (isExactTargeting and split.icpswap.slippageBP > 0) {
                      let denominator = 10000 + split.icpswap.slippageBP;
                      (split.icpswap.amount * 10000) / denominator
                    } else { split.icpswap.amount };

                    let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < split.icpswap.amount and split.icpswap.amount > 0) {
                      (split.icpswap.expectedOut * icpFinalAmount) / split.icpswap.amount
                    } else { split.icpswap.expectedOut };

                    let icpIdealOut : Nat = if (split.icpswap.slippageBP < 9900) {
                      (icpAdjustedExpectedOut * 10000) / (10000 - split.icpswap.slippageBP)
                    } else { icpAdjustedExpectedOut };

                    let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                    let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                    // TACO leg
                    let tacoFinalAmount : Nat = if (isExactTargeting and split.taco.slippageBP > 0) {
                      let denominator = 10000 + split.taco.slippageBP;
                      (split.taco.amount * 10000) / denominator
                    } else { split.taco.amount };
                    
                    let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < split.taco.amount and split.taco.amount > 0) {
                      (split.taco.expectedOut * tacoFinalAmount) / split.taco.amount
                    } else { split.taco.expectedOut };
                    
                    let tacoIdealOut : Nat = if (split.taco.slippageBP < 9900) {
                      (tacoAdjustedExpectedOut * 10000) / (10000 - split.taco.slippageBP)
                    } else { tacoAdjustedExpectedOut };
                    
                    let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                    let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                    // Execute both trades IN PARALLEL (no race conditions - uses executeTransferAndSwapNoTracking)
                    let splitResult = await* executeSplitTrade(
                      sellToken, buyToken,
                      kongFinalAmount, kongMinAmountOut, kongIdealOut,
                      icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                    );

                    // Track results
                    var kongSuccess = false;
                    var icpSuccess = false;
                    let lastTrades = Vector.clone(rebalanceState.lastTrades);

                    // Handle KongSwap result
                    switch (splitResult.kongResult) {
                      case (#ok(record)) {
                        Vector.add(lastTrades, record);
                        kongSuccess := true;
                        logger.info("TRADE_SPLIT", "KongSwap leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                      };
                      case (#err(e)) {
                        let failedRecord : TradeRecord = {
                          tokenSold = sellToken; tokenBought = buyToken;
                          amountSold = kongFinalAmount; amountBought = 0;
                          exchange = #KongSwap; timestamp = now();
                          success = false; error = ?e; slippage = 0.0;
                        };
                        Vector.add(lastTrades, failedRecord);
                        logger.error("TRADE_SPLIT", "KongSwap leg failed: " # e, "do_executeTradingStep");
                      };
                    };

                    // Handle ICPSwap result
                    switch (splitResult.icpResult) {
                      case (#ok(record)) {
                        Vector.add(lastTrades, record);
                        icpSuccess := true;
                        logger.info("TRADE_SPLIT", "ICPSwap leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                      };
                      case (#err(e)) {
                        let failedRecord : TradeRecord = {
                          tokenSold = sellToken; tokenBought = buyToken;
                          amountSold = icpFinalAmount; amountBought = 0;
                          exchange = #ICPSwap; timestamp = now();
                          success = false; error = ?e; slippage = 0.0;
                        };
                        Vector.add(lastTrades, failedRecord);
                        logger.error("TRADE_SPLIT", "ICPSwap leg failed: " # e, "do_executeTradingStep");
                      };
                    };

                    // Handle TACO result
                    var tacoSuccess = false;
                    switch (splitResult.tacoResult) {
                      case (#ok(record)) {
                        Vector.add(lastTrades, record);
                        tacoSuccess := true;
                        logger.info("TRADE_SPLIT", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                      };
                      case (#err(e)) {
                        if (tacoFinalAmount > 0) {
                          let failedRecord : TradeRecord = {
                            tokenSold = sellToken; tokenBought = buyToken;
                            amountSold = tacoFinalAmount; amountBought = 0;
                            exchange = #TACO; timestamp = now();
                            success = false; error = ?e; slippage = 0.0;
                          };
                          Vector.add(lastTrades, failedRecord);
                          logger.error("TRADE_SPLIT", "TACO leg failed: " # e, "do_executeTradingStep");
                        };
                      };
                    };

                    // Trim trade history if needed
                    if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                      Vector.reverse(lastTrades);
                      while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                        ignore Vector.removeLast(lastTrades);
                      };
                      Vector.reverse(lastTrades);
                    };

                    // Update metrics
                    let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                    let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                    let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };

                    rebalanceState := {
                      rebalanceState with
                      metrics = {
                        rebalanceState.metrics with
                        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                        totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                      };
                      lastTrades = lastTrades;
                    };

                    // At least one leg succeeded
                    if (kongSuccess or icpSuccess or tacoSuccess) {
                      success := true;
                      tradingBackoffLevel := 0;
                      Debug.print("Split trade completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                      await updateBalances();
                      await takePortfolioSnapshot(#PostTrade);
                      checkPortfolioCircuitBreakerConditions();
                      await* logPortfolioState("Post-trade completed (split)");
                    } else {
                      // Both split legs failed - attempt ICP fallback
                      if (buyToken != ICPprincipal and sellToken != ICPprincipal and not isTokenPausedFromTrading(ICPprincipal)) {
                        addSkipPair(sellToken, buyToken);

                        let splitSellSymbol = tokenDetailsSell.tokenSymbol;
                        let splitBuySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                          case (?details) { details.tokenSymbol };
                          case null { "UNKNOWN" };
                        };

                        logger.info("ICP_FALLBACK",
                          "Both split legs failed, attempting ICP fallback - Original_pair=" # splitSellSymbol # "/" # splitBuySymbol #
                          " Fallback_pair=" # splitSellSymbol # "/ICP",
                          "do_executeTradingStep"
                        );

                        // Use original tradeSize - both legs failed so no tokens consumed by DEXes
                        let splitFallbackExecution = await* findBestExecution(sellToken, ICPprincipal, cappedTradeSize);

                        switch (splitFallbackExecution) {
                          case (#ok(#Single(icpExecution))) {
                            let ourSlippageToleranceBP = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                            let idealOut : Nat = if (icpExecution.slippageBP < 9900) {
                              (icpExecution.expectedOut * 10000) / (10000 - icpExecution.slippageBP)
                            } else {
                              icpExecution.expectedOut
                            };

                            let toleranceMultiplier : Nat = if (ourSlippageToleranceBP >= 10000) { 0 } else { 10000 - ourSlippageToleranceBP };
                            let minAmountOut : Nat = (idealOut * toleranceMultiplier) / 10000;

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
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK: Both split legs failed for " # splitBuySymbol # ", sold for ICP instead");
                                };

                                Vector.add(lastTrades, fallbackRecord);
                                if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                  Vector.reverse(lastTrades);
                                  while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                    ignore Vector.removeLast(lastTrades);
                                  };
                                  Vector.reverse(lastTrades);
                                };

                                // Update price if deviation acceptable
                                let deviationFromQuote : Float = if (icpExecution.expectedOut > 0) {
                                  Float.abs(Float.fromInt(record.amountBought) - Float.fromInt(icpExecution.expectedOut)) / Float.fromInt(icpExecution.expectedOut)
                                } else { 1.0 };
                                let shouldUpdatePrice = isFiniteFloat(deviationFromQuote) and deviationFromQuote <= 0.05;

                                if (shouldUpdatePrice) {
                                  let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** tokenDetailsSell.tokenDecimals);
                                  let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                                  if (actualICP > 0.0) {
                                    let priceRatio = (actualTokensSold / actualICP) * Float.fromInt(tokenDetailsSell.priceInICP);
                                    if (isFiniteFloat(priceRatio)) {
                                      let newICPPrice = Int.abs(Float.toInt(priceRatio));
                                      let newICPPriceUSD = tokenDetailsSell.priceInUSD * actualTokensSold / actualICP;
                                      updateTokenPriceWithHistory(ICPprincipal, newICPPrice, newICPPriceUSD);
                                    };
                                  };
                                };

                                rebalanceState := {
                                  rebalanceState with
                                  metrics = { rebalanceState.metrics with totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1 };
                                  lastTrades = lastTrades;
                                };

                                success := true;
                                tradingBackoffLevel := 0;

                                logger.info("ICP_FALLBACK",
                                  "ICP fallback SUCCESS after split failure - Sold=" # splitSellSymbol #
                                  " Amount_sold=" # Nat.toText(record.amountSold) #
                                  " ICP_received=" # Nat.toText(record.amountBought) #
                                  " Exchange=" # debug_show(record.exchange),
                                  "do_executeTradingStep"
                                );

                                await updateBalances();
                                await takePortfolioSnapshot(#PostTrade);
                                checkPortfolioCircuitBreakerConditions();
                                await* logPortfolioState("Post-ICP-fallback (after split failure)");
                              };
                              case (#err(icpError)) {
                                logger.warn("ICP_FALLBACK",
                                  "ICP fallback also failed after split failure - Error=" # icpError,
                                  "do_executeTradingStep"
                                );
                                incrementSkipCounter(#noExecutionPath);
                              };
                            };
                          };
                          case (#ok(#Split(split))) {
                            // SPLIT ICP FALLBACK after Split execution failure
                            Debug.print("ICP fallback split route found after split exec failure, executing both legs");
                            logger.info("ICP_FALLBACK_SPLIT",
                              "Executing split ICP fallback after split failure - Kong=" # Nat.toText(split.kongswap.percentBP / 100) # "%" #
                              " ICP=" # Nat.toText(split.icpswap.percentBP / 100) # "%" #
                              " TACO=" # Nat.toText(split.taco.percentBP / 100) # "%",
                              "do_executeTradingStep"
                            );

                            let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                            // KongSwap leg
                            let kongFinalAmount : Nat = if (isExactTargeting and split.kongswap.slippageBP > 0) {
                              let denominator = 10000 + split.kongswap.slippageBP;
                              (split.kongswap.amount * 10000) / denominator
                            } else { split.kongswap.amount };

                            let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < split.kongswap.amount and split.kongswap.amount > 0) {
                              (split.kongswap.expectedOut * kongFinalAmount) / split.kongswap.amount
                            } else { split.kongswap.expectedOut };

                            let kongIdealOut : Nat = if (split.kongswap.slippageBP < 9900) {
                              (kongAdjustedExpectedOut * 10000) / (10000 - split.kongswap.slippageBP)
                            } else { kongAdjustedExpectedOut };

                            let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                            // ICPSwap leg
                            let icpFinalAmount : Nat = if (isExactTargeting and split.icpswap.slippageBP > 0) {
                              let denominator = 10000 + split.icpswap.slippageBP;
                              (split.icpswap.amount * 10000) / denominator
                            } else { split.icpswap.amount };

                            let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < split.icpswap.amount and split.icpswap.amount > 0) {
                              (split.icpswap.expectedOut * icpFinalAmount) / split.icpswap.amount
                            } else { split.icpswap.expectedOut };

                            let icpIdealOut : Nat = if (split.icpswap.slippageBP < 9900) {
                              (icpAdjustedExpectedOut * 10000) / (10000 - split.icpswap.slippageBP)
                            } else { icpAdjustedExpectedOut };

                            let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                            // TACO leg
                            let tacoFinalAmount : Nat = if (isExactTargeting and split.taco.slippageBP > 0) {
                              let denominator = 10000 + split.taco.slippageBP;
                              (split.taco.amount * 10000) / denominator
                            } else { split.taco.amount };
                            
                            let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < split.taco.amount and split.taco.amount > 0) {
                              (split.taco.expectedOut * tacoFinalAmount) / split.taco.amount
                            } else { split.taco.expectedOut };
                            
                            let tacoIdealOut : Nat = if (split.taco.slippageBP < 9900) {
                              (tacoAdjustedExpectedOut * 10000) / (10000 - split.taco.slippageBP)
                            } else { tacoAdjustedExpectedOut };
                            
                            let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                            let splitResult = await* executeSplitTrade(
                              sellToken, ICPprincipal,
                              kongFinalAmount, kongMinAmountOut, kongIdealOut,
                              icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                            );

                            var kongSuccess = false;
                            var icpSuccess = false;
                            let lastTrades = Vector.clone(rebalanceState.lastTrades);

                            // Handle KongSwap result
                            switch (splitResult.kongResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_SPLIT: Both split legs failed for " # splitBuySymbol # ", sold for ICP (KongSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                kongSuccess := true;
                                logger.info("ICP_FALLBACK_SPLIT", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errKong)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = kongFinalAmount; amountBought = 0;
                                  exchange = #KongSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errKong); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_SPLIT", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                              };
                            };

                            // Handle ICPSwap result
                            switch (splitResult.icpResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_SPLIT: Both split legs failed for " # splitBuySymbol # ", sold for ICP (ICPSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                icpSuccess := true;
                                logger.info("ICP_FALLBACK_SPLIT", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errIcp)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = icpFinalAmount; amountBought = 0;
                                  exchange = #ICPSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errIcp); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_SPLIT", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                              };
                            };

                            // Handle TACO result
                            var tacoSuccess = false;
                            switch (splitResult.tacoResult) {
                              case (#ok(record)) {
                                Vector.add(lastTrades, record);
                                tacoSuccess := true;
                                logger.info("ICP_FALLBACK_SPLIT", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(e)) {
                                if (tacoFinalAmount > 0) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = tacoFinalAmount; amountBought = 0;
                                    exchange = #TACO; timestamp = now();
                                    success = false; error = ?e; slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_SPLIT", "TACO leg failed: " # e, "do_executeTradingStep");
                                };
                              };
                            };

                            // Trim trade history
                            if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                              Vector.reverse(lastTrades);
                              while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                ignore Vector.removeLast(lastTrades);
                              };
                              Vector.reverse(lastTrades);
                            };

                            let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                            let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                            let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };

                            rebalanceState := {
                              rebalanceState with
                              metrics = {
                                rebalanceState.metrics with
                                totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                                totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                              };
                              lastTrades = lastTrades;
                            };

                            if (kongSuccess or icpSuccess or tacoSuccess) {
                              success := true;
                              tradingBackoffLevel := 0;
                              Debug.print("ICP fallback split after split failure completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                              await updateBalances();
                              await takePortfolioSnapshot(#PostTrade);
                              checkPortfolioCircuitBreakerConditions();
                              await* logPortfolioState("Post-ICP-fallback-split (after split failure)");
                            } else {
                              incrementSkipCounter(#noExecutionPath);
                              rebalanceState := {
                                rebalanceState with
                                metrics = {
                                  rebalanceState.metrics with
                                  currentStatus = #Failed("ICP fallback (after split failure) split: both legs failed");
                                };
                              };
                              await* recoverFromFailure();
                            };
                          };

                          case (#ok(#Partial(partial))) {
                            // PARTIAL ICP FALLBACK after Split execution failure
                            Debug.print("ICP fallback partial route found after split exec failure, executing both legs");
                            logger.info("ICP_FALLBACK_PARTIAL",
                              "Executing partial ICP fallback after split failure - Kong=" # Nat.toText(partial.kongswap.percentBP / 100) # "%" #
                              " ICP=" # Nat.toText(partial.icpswap.percentBP / 100) # "%" #
                              " TACO=" # Nat.toText(partial.taco.percentBP / 100) # "%" #
                              " Total=" # Nat.toText(partial.totalPercentBP / 100) # "%",
                              "do_executeTradingStep"
                            );

                            let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                            // KongSwap leg
                            let kongFinalAmount : Nat = if (isExactTargeting and partial.kongswap.slippageBP > 0) {
                              let denominator = 10000 + partial.kongswap.slippageBP;
                              (partial.kongswap.amount * 10000) / denominator
                            } else { partial.kongswap.amount };

                            let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < partial.kongswap.amount and partial.kongswap.amount > 0) {
                              (partial.kongswap.expectedOut * kongFinalAmount) / partial.kongswap.amount
                            } else { partial.kongswap.expectedOut };

                            let kongIdealOut : Nat = if (partial.kongswap.slippageBP < 9900) {
                              (kongAdjustedExpectedOut * 10000) / (10000 - partial.kongswap.slippageBP)
                            } else { kongAdjustedExpectedOut };

                            let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                            // ICPSwap leg
                            let icpFinalAmount : Nat = if (isExactTargeting and partial.icpswap.slippageBP > 0) {
                              let denominator = 10000 + partial.icpswap.slippageBP;
                              (partial.icpswap.amount * 10000) / denominator
                            } else { partial.icpswap.amount };

                            let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < partial.icpswap.amount and partial.icpswap.amount > 0) {
                              (partial.icpswap.expectedOut * icpFinalAmount) / partial.icpswap.amount
                            } else { partial.icpswap.expectedOut };

                            let icpIdealOut : Nat = if (partial.icpswap.slippageBP < 9900) {
                              (icpAdjustedExpectedOut * 10000) / (10000 - partial.icpswap.slippageBP)
                            } else { icpAdjustedExpectedOut };

                            let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                            // TACO leg
                            let tacoFinalAmount : Nat = if (isExactTargeting and partial.taco.slippageBP > 0) {
                              let denominator = 10000 + partial.taco.slippageBP;
                              (partial.taco.amount * 10000) / denominator
                            } else { partial.taco.amount };
                            
                            let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < partial.taco.amount and partial.taco.amount > 0) {
                              (partial.taco.expectedOut * tacoFinalAmount) / partial.taco.amount
                            } else { partial.taco.expectedOut };
                            
                            let tacoIdealOut : Nat = if (partial.taco.slippageBP < 9900) {
                              (tacoAdjustedExpectedOut * 10000) / (10000 - partial.taco.slippageBP)
                            } else { tacoAdjustedExpectedOut };
                            
                            let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                            let splitResult = await* executeSplitTrade(
                              sellToken, ICPprincipal,
                              kongFinalAmount, kongMinAmountOut, kongIdealOut,
                              icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                            );

                            var kongSuccess = false;
                            var icpSuccess = false;
                            let lastTrades = Vector.clone(rebalanceState.lastTrades);

                            switch (splitResult.kongResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_PARTIAL: Both split legs failed for " # splitBuySymbol # ", sold for ICP (KongSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                kongSuccess := true;
                                logger.info("ICP_FALLBACK_PARTIAL", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errKong)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = kongFinalAmount; amountBought = 0;
                                  exchange = #KongSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errKong); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_PARTIAL", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                              };
                            };

                            switch (splitResult.icpResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_PARTIAL: Both split legs failed for " # splitBuySymbol # ", sold for ICP (ICPSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                icpSuccess := true;
                                logger.info("ICP_FALLBACK_PARTIAL", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errIcp)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = icpFinalAmount; amountBought = 0;
                                  exchange = #ICPSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errIcp); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_PARTIAL", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                              };
                            };

                            // Handle TACO result
                            var tacoSuccess = false;
                            switch (splitResult.tacoResult) {
                              case (#ok(record)) {
                                Vector.add(lastTrades, record);
                                tacoSuccess := true;
                                logger.info("ICP_FALLBACK_PARTIAL", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(e)) {
                                if (tacoFinalAmount > 0) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = tacoFinalAmount; amountBought = 0;
                                    exchange = #TACO; timestamp = now();
                                    success = false; error = ?e; slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_PARTIAL", "TACO leg failed: " # e, "do_executeTradingStep");
                                };
                              };
                            };

                            let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                            let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                            let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };
                            rebalanceState := {
                              rebalanceState with
                              metrics = {
                                rebalanceState.metrics with
                                totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                                totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                              };
                              lastTrades = lastTrades;
                            };

                            if (kongSuccess or icpSuccess or tacoSuccess) {
                              success := true;
                              tradingBackoffLevel := 0;
                              Debug.print("ICP fallback partial after split failure completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                              await updateBalances();
                              await takePortfolioSnapshot(#PostTrade);
                              checkPortfolioCircuitBreakerConditions();
                              await* logPortfolioState("Post-ICP-fallback-partial (after split failure)");
                            } else {
                              incrementSkipCounter(#noExecutionPath);
                              rebalanceState := {
                                rebalanceState with
                                metrics = {
                                  rebalanceState.metrics with
                                  currentStatus = #Failed("ICP fallback (after split failure) partial: both legs failed");
                                };
                              };
                              await* recoverFromFailure();
                            };
                          };
                          case (#err(icpRouteError)) {
                            logger.warn("ICP_FALLBACK",
                              "ICP fallback failed at full size after split failure - Error=" # icpRouteError.reason,
                              "do_executeTradingStep"
                            );

                            // NEW: Try REDUCED amount for ICP fallback (reuses quotes from icpRouteError)
                            label reducedIcpFallbackAfterSplit switch (estimateMaxTradeableAmount(
                              icpRouteError.kongQuotes, icpRouteError.icpQuotes, tradeSize,
                              rebalanceConfig.maxSlippageBasisPoints, sellToken, ICPprincipal
                            )) {
                              case (?reduced) {
                                if (reduced.icpWorth < rebalanceConfig.minTradeValueICP / 3) {
                                  logger.info("SKIP_REDUCED",
                                    "Reduced ICP fallback after split failure too small - IcpWorth=" # Nat.toText(reduced.icpWorth) #
                                    " < " # Nat.toText(rebalanceConfig.minTradeValueICP / 3),
                                    "do_executeTradingStep"
                                  );
                                  break reducedIcpFallbackAfterSplit;
                                };

                                logger.info("REDUCED_ICP_FALLBACK",
                                  "Executing reduced ICP fallback after split failure - Original=" # Nat.toText(tradeSize) #
                                  " Reduced=" # Nat.toText(reduced.amount) #
                                  " Exchange=" # debug_show(reduced.exchange) #
                                  " Pair=" # splitSellSymbol # "/" # splitBuySymbol #
                                  " Fallback=" # splitSellSymbol # "/ICP" #
                                  " IcpWorth=" # Nat.toText(reduced.icpWorth),
                                  "do_executeTradingStep"
                                );

                                let reducedIcpTradeResult = await* executeTrade(
                                  sellToken, ICPprincipal,
                                  reduced.amount, reduced.exchange,
                                  reduced.minAmountOut, reduced.idealOut
                                );

                                switch (reducedIcpTradeResult) {
                                  case (#ok(record)) {
                                    let lastTrades = Vector.clone(rebalanceState.lastTrades);
                                    let reducedIcpRecord : TradeRecord = {
                                      record with
                                      error = ?("REDUCED_ICP_FALLBACK: Both split legs failed for " # splitBuySymbol # ", traded reduced amount for ICP");
                                    };
                                    Vector.add(lastTrades, reducedIcpRecord);

                                    if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
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
                                        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                                      };
                                      lastTrades = lastTrades;
                                    };

                                    success := true;
                                    tradingBackoffLevel := 0;

                                    logger.info("REDUCED_ICP_FALLBACK",
                                      "REDUCED ICP fallback SUCCESS after split failure - Sold=" # Nat.toText(record.amountSold) #
                                      " ICP_received=" # Nat.toText(record.amountBought) #
                                      " Exchange=" # debug_show(record.exchange),
                                      "do_executeTradingStep"
                                    );

                                    await updateBalances();
                                    await takePortfolioSnapshot(#PostTrade);
                                    checkPortfolioCircuitBreakerConditions();
                                    await* logPortfolioState("Post-REDUCED-ICP-fallback (after split failure)");
                                  };
                                  case (#err(reducedIcpError)) {
                                    logger.warn("REDUCED_ICP_FALLBACK",
                                      "Reduced ICP fallback also failed after split failure - Error=" # reducedIcpError,
                                      "do_executeTradingStep"
                                    );
                                  };
                                };
                              };
                              case null {
                                Debug.print("No viable reduced ICP fallback amount after split failure");
                              };
                            };

                            // If reduced succeeded, return early
                            if (not success) {
                              incrementSkipCounter(#noExecutionPath);
                            };
                          };
                        };
                      };
                    };
                  };

                  case (#Partial(partial)) {
                    // PARTIAL SPLIT EXECUTION - same as Split but trades less than 100%
                    logger.info("TRADE_PARTIAL",
                      "Executing partial split - Kong=" # Nat.toText(partial.kongswap.percentBP / 100) # "%" #
                      " (" # Nat.toText(partial.kongswap.amount) # " tokens)" #
                      " ICP=" # Nat.toText(partial.icpswap.percentBP / 100) # "%" #
                      " TACO=" # Nat.toText(partial.taco.percentBP / 100) # "%" #
                      " (" # Nat.toText(partial.icpswap.amount) # " tokens)" #
                      " Total=" # Nat.toText(partial.totalPercentBP / 100) # "%",
                      "do_executeTradingStep"
                    );

                    let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                    // Apply slippage adjustment to BOTH legs (same as Split)
                    // KongSwap leg
                    let kongFinalAmount : Nat = if (isExactTargeting and partial.kongswap.slippageBP > 0) {
                      let denominator = 10000 + partial.kongswap.slippageBP;
                      (partial.kongswap.amount * 10000) / denominator
                    } else { partial.kongswap.amount };

                    let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < partial.kongswap.amount and partial.kongswap.amount > 0) {
                      (partial.kongswap.expectedOut * kongFinalAmount) / partial.kongswap.amount
                    } else { partial.kongswap.expectedOut };

                    let kongIdealOut : Nat = if (partial.kongswap.slippageBP < 9900) {
                      (kongAdjustedExpectedOut * 10000) / (10000 - partial.kongswap.slippageBP)
                    } else { kongAdjustedExpectedOut };

                    let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                    let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                    // ICPSwap leg
                    let icpFinalAmount : Nat = if (isExactTargeting and partial.icpswap.slippageBP > 0) {
                      let denominator = 10000 + partial.icpswap.slippageBP;
                      (partial.icpswap.amount * 10000) / denominator
                    } else { partial.icpswap.amount };

                    let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < partial.icpswap.amount and partial.icpswap.amount > 0) {
                      (partial.icpswap.expectedOut * icpFinalAmount) / partial.icpswap.amount
                    } else { partial.icpswap.expectedOut };

                    let icpIdealOut : Nat = if (partial.icpswap.slippageBP < 9900) {
                      (icpAdjustedExpectedOut * 10000) / (10000 - partial.icpswap.slippageBP)
                    } else { icpAdjustedExpectedOut };

                    let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                    let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                    // TACO leg
                    let tacoFinalAmount : Nat = if (isExactTargeting and partial.taco.slippageBP > 0) {
                      let denominator = 10000 + partial.taco.slippageBP;
                      (partial.taco.amount * 10000) / denominator
                    } else { partial.taco.amount };
                    
                    let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < partial.taco.amount and partial.taco.amount > 0) {
                      (partial.taco.expectedOut * tacoFinalAmount) / partial.taco.amount
                    } else { partial.taco.expectedOut };
                    
                    let tacoIdealOut : Nat = if (partial.taco.slippageBP < 9900) {
                      (tacoAdjustedExpectedOut * 10000) / (10000 - partial.taco.slippageBP)
                    } else { tacoAdjustedExpectedOut };
                    
                    let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                    let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                    // Execute both trades IN PARALLEL
                    let splitResult = await* executeSplitTrade(
                      sellToken, buyToken,
                      kongFinalAmount, kongMinAmountOut, kongIdealOut,
                      icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                    );

                    // Track results
                    var kongSuccess = false;
                    var icpSuccess = false;
                    let lastTrades = Vector.clone(rebalanceState.lastTrades);

                    // Handle KongSwap result
                    switch (splitResult.kongResult) {
                      case (#ok(record)) {
                        Vector.add(lastTrades, record);
                        kongSuccess := true;
                        logger.info("TRADE_PARTIAL", "KongSwap leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                      };
                      case (#err(e)) {
                        let failedRecord : TradeRecord = {
                          tokenSold = sellToken; tokenBought = buyToken;
                          amountSold = kongFinalAmount; amountBought = 0;
                          exchange = #KongSwap; timestamp = now();
                          success = false; error = ?e; slippage = 0.0;
                        };
                        Vector.add(lastTrades, failedRecord);
                        logger.error("TRADE_PARTIAL", "KongSwap leg failed: " # e, "do_executeTradingStep");
                      };
                    };

                    // Handle ICPSwap result
                    switch (splitResult.icpResult) {
                      case (#ok(record)) {
                        Vector.add(lastTrades, record);
                        icpSuccess := true;
                        logger.info("TRADE_PARTIAL", "ICPSwap leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                      };
                      case (#err(e)) {
                        let failedRecord : TradeRecord = {
                          tokenSold = sellToken; tokenBought = buyToken;
                          amountSold = icpFinalAmount; amountBought = 0;
                          exchange = #ICPSwap; timestamp = now();
                          success = false; error = ?e; slippage = 0.0;
                        };
                        Vector.add(lastTrades, failedRecord);
                        logger.error("TRADE_PARTIAL", "ICPSwap leg failed: " # e, "do_executeTradingStep");
                      };
                    };

                    // Handle TACO result
                    var tacoSuccess = false;
                    switch (splitResult.tacoResult) {
                      case (#ok(record)) {
                        Vector.add(lastTrades, record);
                        tacoSuccess := true;
                        logger.info("TRADE_PARTIAL", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                      };
                      case (#err(e)) {
                        if (tacoFinalAmount > 0) {
                          let failedRecord : TradeRecord = {
                            tokenSold = sellToken; tokenBought = buyToken;
                            amountSold = tacoFinalAmount; amountBought = 0;
                            exchange = #TACO; timestamp = now();
                            success = false; error = ?e; slippage = 0.0;
                          };
                          Vector.add(lastTrades, failedRecord);
                          logger.error("TRADE_PARTIAL", "TACO leg failed: " # e, "do_executeTradingStep");
                        };
                      };
                    };

                    // Trim trade history if needed
                    if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                      Vector.reverse(lastTrades);
                      while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                        ignore Vector.removeLast(lastTrades);
                      };
                      Vector.reverse(lastTrades);
                    };

                    // Update metrics - count as partial trades
                    let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                    let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                    let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };

                    rebalanceState := {
                      rebalanceState with
                      metrics = {
                        rebalanceState.metrics with
                        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                        totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                      };
                      lastTrades = lastTrades;
                    };

                    // At least one leg succeeded = overall success
                    if (kongSuccess or icpSuccess or tacoSuccess) {
                      success := true;
                      tradingBackoffLevel := 0;
                      Debug.print("Partial trade completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                      await updateBalances();
                      await takePortfolioSnapshot(#PostTrade);
                      checkPortfolioCircuitBreakerConditions();
                      await* logPortfolioState("Post-partial-split completed");
                    } else {
                      // Both partial legs failed - attempt ICP fallback before recovery
                      if (buyToken != ICPprincipal and sellToken != ICPprincipal and not isTokenPausedFromTrading(ICPprincipal)) {
                        addSkipPair(sellToken, buyToken);

                        let partialSellSymbol = tokenDetailsSell.tokenSymbol;
                        let partialBuySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                          case (?details) { details.tokenSymbol };
                          case null { "UNKNOWN" };
                        };

                        logger.info("ICP_FALLBACK",
                          "Both partial legs failed, attempting ICP fallback - Original_pair=" # partialSellSymbol # "/" # partialBuySymbol #
                          " Fallback_pair=" # partialSellSymbol # "/ICP",
                          "do_executeTradingStep"
                        );

                        let partialFallbackExecution = await* findBestExecution(sellToken, ICPprincipal, cappedTradeSize);

                        switch (partialFallbackExecution) {
                          case (#ok(#Single(icpExecution))) {
                            let ourSlippageToleranceBP = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                            let idealOut : Nat = if (icpExecution.slippageBP < 9900) {
                              (icpExecution.expectedOut * 10000) / (10000 - icpExecution.slippageBP)
                            } else {
                              icpExecution.expectedOut
                            };

                            let toleranceMultiplier : Nat = if (ourSlippageToleranceBP >= 10000) { 0 } else { 10000 - ourSlippageToleranceBP };
                            let minAmountOut : Nat = (idealOut * toleranceMultiplier) / 10000;

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
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK: Both partial legs failed for " # partialBuySymbol # ", sold for ICP instead");
                                };

                                Vector.add(lastTrades, fallbackRecord);
                                if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                  Vector.reverse(lastTrades);
                                  while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                    ignore Vector.removeLast(lastTrades);
                                  };
                                  Vector.reverse(lastTrades);
                                };

                                // Update price if deviation acceptable
                                let deviationFromQuote : Float = if (icpExecution.expectedOut > 0) {
                                  Float.abs(Float.fromInt(record.amountBought) - Float.fromInt(icpExecution.expectedOut)) / Float.fromInt(icpExecution.expectedOut)
                                } else { 1.0 };
                                let shouldUpdatePrice = isFiniteFloat(deviationFromQuote) and deviationFromQuote <= 0.05;

                                if (shouldUpdatePrice) {
                                  let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** tokenDetailsSell.tokenDecimals);
                                  let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                                  if (actualICP > 0.0) {
                                    let priceRatio = (actualTokensSold / actualICP) * Float.fromInt(tokenDetailsSell.priceInICP);
                                    if (isFiniteFloat(priceRatio)) {
                                      let newICPPrice = Int.abs(Float.toInt(priceRatio));
                                      let newICPPriceUSD = tokenDetailsSell.priceInUSD * actualTokensSold / actualICP;
                                      updateTokenPriceWithHistory(ICPprincipal, newICPPrice, newICPPriceUSD);
                                    };
                                  };
                                };

                                rebalanceState := {
                                  rebalanceState with
                                  metrics = { rebalanceState.metrics with totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1 };
                                  lastTrades = lastTrades;
                                };

                                success := true;
                                tradingBackoffLevel := 0;

                                logger.info("ICP_FALLBACK",
                                  "ICP fallback SUCCESS after partial failure - Sold=" # partialSellSymbol #
                                  " Amount_sold=" # Nat.toText(record.amountSold) #
                                  " ICP_received=" # Nat.toText(record.amountBought) #
                                  " Exchange=" # debug_show(record.exchange),
                                  "do_executeTradingStep"
                                );

                                await updateBalances();
                                await takePortfolioSnapshot(#PostTrade);
                                checkPortfolioCircuitBreakerConditions();
                                await* logPortfolioState("Post-ICP-fallback (after partial failure)");
                              };
                              case (#err(icpError)) {
                                logger.warn("ICP_FALLBACK",
                                  "ICP fallback also failed after partial failure - Error=" # icpError,
                                  "do_executeTradingStep"
                                );
                                incrementSkipCounter(#noExecutionPath);
                              };
                            };
                          };
                          case (#ok(#Split(split))) {
                            // SPLIT ICP FALLBACK after Partial execution failure
                            Debug.print("ICP fallback split route found after partial exec failure, executing both legs");
                            logger.info("ICP_FALLBACK_SPLIT",
                              "Executing split ICP fallback after partial failure - Kong=" # Nat.toText(split.kongswap.percentBP / 100) # "%" #
                              " ICP=" # Nat.toText(split.icpswap.percentBP / 100) # "%" #
                              " TACO=" # Nat.toText(split.taco.percentBP / 100) # "%",
                              "do_executeTradingStep"
                            );

                            let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                            // KongSwap leg
                            let kongFinalAmount : Nat = if (isExactTargeting and split.kongswap.slippageBP > 0) {
                              let denominator = 10000 + split.kongswap.slippageBP;
                              (split.kongswap.amount * 10000) / denominator
                            } else { split.kongswap.amount };

                            let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < split.kongswap.amount and split.kongswap.amount > 0) {
                              (split.kongswap.expectedOut * kongFinalAmount) / split.kongswap.amount
                            } else { split.kongswap.expectedOut };

                            let kongIdealOut : Nat = if (split.kongswap.slippageBP < 9900) {
                              (kongAdjustedExpectedOut * 10000) / (10000 - split.kongswap.slippageBP)
                            } else { kongAdjustedExpectedOut };

                            let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                            // ICPSwap leg
                            let icpFinalAmount : Nat = if (isExactTargeting and split.icpswap.slippageBP > 0) {
                              let denominator = 10000 + split.icpswap.slippageBP;
                              (split.icpswap.amount * 10000) / denominator
                            } else { split.icpswap.amount };

                            let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < split.icpswap.amount and split.icpswap.amount > 0) {
                              (split.icpswap.expectedOut * icpFinalAmount) / split.icpswap.amount
                            } else { split.icpswap.expectedOut };

                            let icpIdealOut : Nat = if (split.icpswap.slippageBP < 9900) {
                              (icpAdjustedExpectedOut * 10000) / (10000 - split.icpswap.slippageBP)
                            } else { icpAdjustedExpectedOut };

                            let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                            // TACO leg
                            let tacoFinalAmount : Nat = if (isExactTargeting and split.taco.slippageBP > 0) {
                              let denominator = 10000 + split.taco.slippageBP;
                              (split.taco.amount * 10000) / denominator
                            } else { split.taco.amount };
                            
                            let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < split.taco.amount and split.taco.amount > 0) {
                              (split.taco.expectedOut * tacoFinalAmount) / split.taco.amount
                            } else { split.taco.expectedOut };
                            
                            let tacoIdealOut : Nat = if (split.taco.slippageBP < 9900) {
                              (tacoAdjustedExpectedOut * 10000) / (10000 - split.taco.slippageBP)
                            } else { tacoAdjustedExpectedOut };
                            
                            let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                            // Execute both trades IN PARALLEL to ICP
                            let splitResult = await* executeSplitTrade(
                              sellToken, ICPprincipal,
                              kongFinalAmount, kongMinAmountOut, kongIdealOut,
                              icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                            );

                            var kongSuccess = false;
                            var icpSuccess = false;
                            let lastTrades = Vector.clone(rebalanceState.lastTrades);

                            // Handle KongSwap result
                            switch (splitResult.kongResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_SPLIT: Both partial legs failed for " # partialBuySymbol # ", sold for ICP (KongSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                kongSuccess := true;
                                logger.info("ICP_FALLBACK_SPLIT", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errKong)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = kongFinalAmount; amountBought = 0;
                                  exchange = #KongSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errKong); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_SPLIT", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                              };
                            };

                            // Handle ICPSwap result
                            switch (splitResult.icpResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_SPLIT: Both partial legs failed for " # partialBuySymbol # ", sold for ICP (ICPSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                icpSuccess := true;
                                logger.info("ICP_FALLBACK_SPLIT", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errIcp)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = icpFinalAmount; amountBought = 0;
                                  exchange = #ICPSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errIcp); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_SPLIT", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                              };
                            };

                            // Handle TACO result
                            var tacoSuccess = false;
                            switch (splitResult.tacoResult) {
                              case (#ok(record)) {
                                Vector.add(lastTrades, record);
                                tacoSuccess := true;
                                logger.info("ICP_FALLBACK_SPLIT", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(e)) {
                                if (tacoFinalAmount > 0) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = tacoFinalAmount; amountBought = 0;
                                    exchange = #TACO; timestamp = now();
                                    success = false; error = ?e; slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_SPLIT", "TACO leg failed: " # e, "do_executeTradingStep");
                                };
                              };
                            };

                            // Trim trade history
                            if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                              Vector.reverse(lastTrades);
                              while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                                ignore Vector.removeLast(lastTrades);
                              };
                              Vector.reverse(lastTrades);
                            };

                            let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                            let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                            let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };

                            rebalanceState := {
                              rebalanceState with
                              metrics = {
                                rebalanceState.metrics with
                                totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                                totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                              };
                              lastTrades = lastTrades;
                            };

                            if (kongSuccess or icpSuccess or tacoSuccess) {
                              success := true;
                              tradingBackoffLevel := 0;
                              Debug.print("ICP fallback split after partial failure completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                              await updateBalances();
                              await takePortfolioSnapshot(#PostTrade);
                              checkPortfolioCircuitBreakerConditions();
                              await* logPortfolioState("Post-ICP-fallback-split (after partial exec failure)");
                            } else {
                              incrementSkipCounter(#noExecutionPath);
                              rebalanceState := {
                                rebalanceState with
                                metrics = {
                                  rebalanceState.metrics with
                                  currentStatus = #Failed("ICP fallback split after partial failure: both legs failed");
                                };
                              };
                              await* recoverFromFailure();
                            };
                          };

                          case (#ok(#Partial(partial))) {
                            // PARTIAL ICP FALLBACK after Partial execution failure
                            Debug.print("ICP fallback partial route found after partial exec failure, executing both legs");
                            logger.info("ICP_FALLBACK_PARTIAL",
                              "Executing partial ICP fallback after partial failure - Kong=" # Nat.toText(partial.kongswap.percentBP / 100) # "%" #
                              " ICP=" # Nat.toText(partial.icpswap.percentBP / 100) # "%" #
                              " TACO=" # Nat.toText(partial.taco.percentBP / 100) # "%" #
                              " Total=" # Nat.toText(partial.totalPercentBP / 100) # "%",
                              "do_executeTradingStep"
                            );

                            let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                            // KongSwap leg
                            let kongFinalAmount : Nat = if (isExactTargeting and partial.kongswap.slippageBP > 0) {
                              let denominator = 10000 + partial.kongswap.slippageBP;
                              (partial.kongswap.amount * 10000) / denominator
                            } else { partial.kongswap.amount };

                            let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < partial.kongswap.amount and partial.kongswap.amount > 0) {
                              (partial.kongswap.expectedOut * kongFinalAmount) / partial.kongswap.amount
                            } else { partial.kongswap.expectedOut };

                            let kongIdealOut : Nat = if (partial.kongswap.slippageBP < 9900) {
                              (kongAdjustedExpectedOut * 10000) / (10000 - partial.kongswap.slippageBP)
                            } else { kongAdjustedExpectedOut };

                            let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                            // ICPSwap leg
                            let icpFinalAmount : Nat = if (isExactTargeting and partial.icpswap.slippageBP > 0) {
                              let denominator = 10000 + partial.icpswap.slippageBP;
                              (partial.icpswap.amount * 10000) / denominator
                            } else { partial.icpswap.amount };

                            let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < partial.icpswap.amount and partial.icpswap.amount > 0) {
                              (partial.icpswap.expectedOut * icpFinalAmount) / partial.icpswap.amount
                            } else { partial.icpswap.expectedOut };

                            let icpIdealOut : Nat = if (partial.icpswap.slippageBP < 9900) {
                              (icpAdjustedExpectedOut * 10000) / (10000 - partial.icpswap.slippageBP)
                            } else { icpAdjustedExpectedOut };

                            let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                            // TACO leg
                            let tacoFinalAmount : Nat = if (isExactTargeting and partial.taco.slippageBP > 0) {
                              let denominator = 10000 + partial.taco.slippageBP;
                              (partial.taco.amount * 10000) / denominator
                            } else { partial.taco.amount };
                            
                            let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < partial.taco.amount and partial.taco.amount > 0) {
                              (partial.taco.expectedOut * tacoFinalAmount) / partial.taco.amount
                            } else { partial.taco.expectedOut };
                            
                            let tacoIdealOut : Nat = if (partial.taco.slippageBP < 9900) {
                              (tacoAdjustedExpectedOut * 10000) / (10000 - partial.taco.slippageBP)
                            } else { tacoAdjustedExpectedOut };
                            
                            let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                            let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                            let splitResult = await* executeSplitTrade(
                              sellToken, ICPprincipal,
                              kongFinalAmount, kongMinAmountOut, kongIdealOut,
                              icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                            );

                            var kongSuccess = false;
                            var icpSuccess = false;

                            switch (splitResult.kongResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_PARTIAL: Both partial legs failed for " # partialBuySymbol # ", sold for ICP (KongSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                kongSuccess := true;
                                logger.info("ICP_FALLBACK_PARTIAL", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errKong)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = kongFinalAmount; amountBought = 0;
                                  exchange = #KongSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errKong); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_PARTIAL", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                              };
                            };

                            switch (splitResult.icpResult) {
                              case (#ok(record)) {
                                let fallbackRecord : TradeRecord = {
                                  record with
                                  error = ?("ICP_FALLBACK_PARTIAL: Both partial legs failed for " # partialBuySymbol # ", sold for ICP (ICPSwap leg)");
                                };
                                Vector.add(lastTrades, fallbackRecord);
                                icpSuccess := true;
                                logger.info("ICP_FALLBACK_PARTIAL", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(errIcp)) {
                                let failedRecord : TradeRecord = {
                                  tokenSold = sellToken; tokenBought = ICPprincipal;
                                  amountSold = icpFinalAmount; amountBought = 0;
                                  exchange = #ICPSwap; timestamp = now();
                                  success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errIcp); slippage = 0.0;
                                };
                                Vector.add(lastTrades, failedRecord);
                                logger.error("ICP_FALLBACK_PARTIAL", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                              };
                            };

                            // Handle TACO result
                            var tacoSuccess = false;
                            switch (splitResult.tacoResult) {
                              case (#ok(record)) {
                                Vector.add(lastTrades, record);
                                tacoSuccess := true;
                                logger.info("ICP_FALLBACK_PARTIAL", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                              };
                              case (#err(e)) {
                                if (tacoFinalAmount > 0) {
                                  let failedRecord : TradeRecord = {
                                    tokenSold = sellToken; tokenBought = ICPprincipal;
                                    amountSold = tacoFinalAmount; amountBought = 0;
                                    exchange = #TACO; timestamp = now();
                                    success = false; error = ?e; slippage = 0.0;
                                  };
                                  Vector.add(lastTrades, failedRecord);
                                  logger.error("ICP_FALLBACK_PARTIAL", "TACO leg failed: " # e, "do_executeTradingStep");
                                };
                              };
                            };

                            let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                            let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                            let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };
                            rebalanceState := {
                              rebalanceState with
                              metrics = {
                                rebalanceState.metrics with
                                totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                                totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                              };
                              lastTrades = lastTrades;
                            };

                            if (kongSuccess or icpSuccess or tacoSuccess) {
                              success := true;
                              tradingBackoffLevel := 0;
                              Debug.print("ICP fallback partial after partial failure completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                              await updateBalances();
                              await takePortfolioSnapshot(#PostTrade);
                              checkPortfolioCircuitBreakerConditions();
                              await* logPortfolioState("Post-ICP-fallback-partial (after partial exec failure)");
                            } else {
                              incrementSkipCounter(#noExecutionPath);
                              rebalanceState := {
                                rebalanceState with
                                metrics = {
                                  rebalanceState.metrics with
                                  currentStatus = #Failed("ICP fallback partial after partial failure: both legs failed");
                                };
                              };
                              await* recoverFromFailure();
                            };
                          };
                          case (#err(icpRouteError)) {
                            logger.warn("ICP_FALLBACK",
                              "ICP fallback failed at full size after partial failure - Error=" # icpRouteError.reason,
                              "do_executeTradingStep"
                            );

                            // NEW: Try REDUCED amount for ICP fallback (reuses quotes from icpRouteError)
                            label reducedIcpFallbackAfterPartial switch (estimateMaxTradeableAmount(
                              icpRouteError.kongQuotes, icpRouteError.icpQuotes, tradeSize,
                              rebalanceConfig.maxSlippageBasisPoints, sellToken, ICPprincipal
                            )) {
                              case (?reduced) {
                                if (reduced.icpWorth < rebalanceConfig.minTradeValueICP / 3) {
                                  logger.info("SKIP_REDUCED",
                                    "Reduced ICP fallback after partial failure too small - IcpWorth=" # Nat.toText(reduced.icpWorth) #
                                    " < " # Nat.toText(rebalanceConfig.minTradeValueICP / 3),
                                    "do_executeTradingStep"
                                  );
                                  break reducedIcpFallbackAfterPartial;
                                };

                                logger.info("REDUCED_ICP_FALLBACK",
                                  "Executing reduced ICP fallback after partial failure - Original=" # Nat.toText(tradeSize) #
                                  " Reduced=" # Nat.toText(reduced.amount) #
                                  " Exchange=" # debug_show(reduced.exchange) #
                                  " Pair=" # partialSellSymbol # "/" # partialBuySymbol #
                                  " Fallback=" # partialSellSymbol # "/ICP" #
                                  " IcpWorth=" # Nat.toText(reduced.icpWorth),
                                  "do_executeTradingStep"
                                );

                                let reducedIcpTradeResult = await* executeTrade(
                                  sellToken, ICPprincipal,
                                  reduced.amount, reduced.exchange,
                                  reduced.minAmountOut, reduced.idealOut
                                );

                                switch (reducedIcpTradeResult) {
                                  case (#ok(record)) {
                                    let lastTrades = Vector.clone(rebalanceState.lastTrades);
                                    let reducedIcpRecord : TradeRecord = {
                                      record with
                                      error = ?("REDUCED_ICP_FALLBACK: Both partial legs failed for " # partialBuySymbol # ", traded reduced amount for ICP");
                                    };
                                    Vector.add(lastTrades, reducedIcpRecord);

                                    if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
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
                                        totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                                      };
                                      lastTrades = lastTrades;
                                    };

                                    success := true;
                                    tradingBackoffLevel := 0;

                                    logger.info("REDUCED_ICP_FALLBACK",
                                      "REDUCED ICP fallback SUCCESS after partial failure - Sold=" # Nat.toText(record.amountSold) #
                                      " ICP_received=" # Nat.toText(record.amountBought) #
                                      " Exchange=" # debug_show(record.exchange),
                                      "do_executeTradingStep"
                                    );

                                    await updateBalances();
                                    await takePortfolioSnapshot(#PostTrade);
                                    checkPortfolioCircuitBreakerConditions();
                                    await* logPortfolioState("Post-REDUCED-ICP-fallback (after partial failure)");
                                  };
                                  case (#err(reducedIcpError)) {
                                    logger.warn("REDUCED_ICP_FALLBACK",
                                      "Reduced ICP fallback also failed after partial failure - Error=" # reducedIcpError,
                                      "do_executeTradingStep"
                                    );
                                  };
                                };
                              };
                              case null {
                                Debug.print("No viable reduced ICP fallback amount after partial failure");
                              };
                            };

                            // If reduced succeeded, return early
                            if (not success) {
                              incrementSkipCounter(#noExecutionPath);
                            };
                          };
                        };
                      };

                      // If ICP fallback didn't succeed, run recovery
                      if (not success) {
                        await* recoverFromFailure();
                      };
                    };
                  };
                };
              };
              case (#err(e)) {
                Debug.print("Could not find execution path: " # e.reason);

                // Bug 3 fix: Check if ALL quotes are invalid (both Kong and ICPSwap returned 0)
                // If so, skip reduced trade entirely and go straight to ICP fallback
                let allQuotesInvalid = do {
                  var kongAllZero = true;
                  var icpAllZero = true;
                  for (q in e.kongQuotes.vals()) {
                    if (q.out > 0) { kongAllZero := false };
                  };
                  for (q in e.icpQuotes.vals()) {
                    if (q.out > 0) { icpAllZero := false };
                  };
                  kongAllZero and icpAllZero
                };

                // Only try reduced trade if at least one quote was valid
                // If all quotes are 0, skip directly to ICP fallback
                if (allQuotesInvalid) {
                  logger.info("SKIP_REDUCED",
                    "All quotes returned 0 for direct pair - skipping reduced trade, going directly to ICP fallback",
                    "do_executeTradingStep"
                  );
                } else {
                  // NEW: Try REDUCED amount before ICP fallback
                  // estimateMaxTradeableAmount already checked both exchanges and returns idealOut/minAmountOut
                  label reducedDirect switch (estimateMaxTradeableAmount(e.kongQuotes, e.icpQuotes, cappedTradeSize, rebalanceConfig.maxSlippageBasisPoints, sellToken, buyToken)) {
                  case (?reduced) {
                    // Skip if reduced trade is too small to be worthwhile
                    if (reduced.icpWorth < rebalanceConfig.minTradeValueICP / 3) {
                      logger.info("SKIP_REDUCED",
                        "Reduced trade ICP worth (" # Nat.toText(reduced.icpWorth) #
                        ") < minTradeValueICP/3 (" # Nat.toText(rebalanceConfig.minTradeValueICP / 3) #
                        ") - skipping to ICP fallback",
                        "do_executeTradingStep"
                      );
                      break reducedDirect;
                    };

                    // Get symbols for logging
                    let reducedSellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
                      case (?details) { details.tokenSymbol };
                      case null { break reducedDirect };
                    };
                    let reducedBuySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
                      case (?details) { details.tokenSymbol };
                      case null { break reducedDirect };
                    };

                    logger.info("REDUCED_TRADE",
                      "Executing reduced trade directly - Original=" # Nat.toText(tradeSize) #
                      " Reduced=" # Nat.toText(reduced.amount) #
                      " Exchange=" # debug_show(reduced.exchange) #
                      " Pair=" # reducedSellSymbol # "/" # reducedBuySymbol #
                      " IdealOut=" # Nat.toText(reduced.idealOut) #
                      " MinAmountOut=" # Nat.toText(reduced.minAmountOut) #
                      " IcpWorth=" # Nat.toText(reduced.icpWorth),
                      "do_executeTradingStep"
                    );

                    // Execute directly - estimateMaxTradeableAmount already calculated idealOut and minAmountOut
                    let reducedTradeResult = await* executeTrade(
                      sellToken,
                      buyToken,
                      reduced.amount,
                      reduced.exchange,
                      reduced.minAmountOut,
                      reduced.idealOut,
                    );

                    switch (reducedTradeResult) {
                      case (#ok(record)) {
                        // Update trade history
                        let lastTrades = Vector.clone(rebalanceState.lastTrades);
                        let reducedRecord : TradeRecord = {
                          record with
                          error = ?("REDUCED_TRADE: Original amount was " # Nat.toText(tradeSize) # ", traded " # Nat.toText(reduced.amount) # " due to slippage");
                        };
                        Vector.add(lastTrades, reducedRecord);
                        if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
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
                            totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                          };
                          lastTrades = lastTrades;
                        };

                        success := true;
                        tradingBackoffLevel := 0;
                        logger.info("REDUCED_TRADE",
                          "REDUCED trade SUCCESS - Sold=" # Nat.toText(record.amountSold) #
                          " Bought=" # Nat.toText(record.amountBought) #
                          " Exchange=" # debug_show(record.exchange),
                          "do_executeTradingStep"
                        );

                        await updateBalances();
                        await takePortfolioSnapshot(#PostTrade);
                        checkPortfolioCircuitBreakerConditions();
                        await* logPortfolioState("Post-REDUCED-trade completed");
                      };
                      case (#err(reducedError)) {
                        logger.warn("REDUCED_TRADE",
                          "REDUCED trade execution failed - Error=" # reducedError,
                          "do_executeTradingStep"
                        );
                        // Continue to ICP fallback
                      };
                    };
                  };
                  case null {
                    Debug.print("No viable reduced amount estimated");
                  };
                };
                }; // end of else block (allQuotesInvalid check)

                // Guard: if reduced trade succeeded, skip ICP fallback
                if (success) { return };

                // ICP FALLBACK STRATEGY: If we can't find a direct route, try selling for ICP instead
                // This creates an ICP overweight that will be corrected in the next cycle
                // Only attempt fallback if ICP itself is not paused
                if (buyToken != ICPprincipal and sellToken != ICPprincipal and not isTokenPausedFromTrading(ICPprincipal)) {
                  // Record this pair in the skip map so next time we go straight to ICP fallback
                  addSkipPair(sellToken, buyToken);

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
                    " Original_error=" # e.reason #
                    " Strategy=Two_step_rebalancing",
                    "do_executeTradingStep"
                  );
                  
                  let icpFallbackExecution = await* findBestExecution(sellToken, ICPprincipal, cappedTradeSize);

                  switch (icpFallbackExecution) {
                    case (#ok(#Single(icpExecution))) {
                      // ICP fallback typically uses single exchange
                      Debug.print("ICP fallback route found, executing trade");

                      let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                      // Calculate ideal output using integer arithmetic
                      let idealOut : Nat = if (icpExecution.slippageBP < 9900) {
                        (icpExecution.expectedOut * 10000) / (10000 - icpExecution.slippageBP)
                      } else {
                        icpExecution.expectedOut
                      };

                      let toleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                      let minAmountOut : Nat = (idealOut * toleranceMultiplier) / 10000;

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

                          // Check if execution matches quote (5% tolerance) before updating price
                          let deviationFromQuote : Float = if (icpExecution.expectedOut > 0) {
                            Float.abs(Float.fromInt(record.amountBought) - Float.fromInt(icpExecution.expectedOut)) / Float.fromInt(icpExecution.expectedOut)
                          } else { 1.0 };
                          let shouldUpdatePrice = isFiniteFloat(deviationFromQuote) and deviationFromQuote <= 0.05;

                          if (not shouldUpdatePrice) {
                            logger.warn("PRICE_SKIP",
                              "ICP fallback execution deviated " # Float.toText(deviationFromQuote * 100.0) # "% from quote - skipping price update",
                              "do_executeTradingStep");
                          };

                          // Calculate new ICP price from the trade - only if deviation is acceptable
                          if (shouldUpdatePrice) {
                            let actualTokensSold = Float.fromInt(record.amountSold) / Float.fromInt(10 ** sellTokenDetails.tokenDecimals);
                            let actualICP = Float.fromInt(record.amountBought) / Float.fromInt(100000000);
                            // Guard against division by zero (actualICP = 0 would produce Inf)
                            if (actualICP > 0.0) {
                              let priceRatio = (actualTokensSold / actualICP) * Float.fromInt(sellTokenDetails.priceInICP);
                              if (isFiniteFloat(priceRatio)) {
                                let newICPPrice = Int.abs(Float.toInt(priceRatio));
                                let newICPPriceUSD = sellTokenDetails.priceInUSD * actualTokensSold / actualICP;
                                updateTokenPriceWithHistory(ICPprincipal, newICPPrice, newICPPriceUSD);
                              };
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
                          tradingBackoffLevel := 0;
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
                            "ICP fallback trade FAILED - Original_error=" # e.reason #
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
                              currentStatus = #Failed("Both direct and ICP fallback routes failed: " # e.reason # " | " # icpErrorMsg);
                            };
                          };
                          
                          // Attempt system recovery
                          await* recoverFromFailure();
                        };
                      };
                    };
                    case (#ok(#Split(split))) {
                      // SPLIT ICP FALLBACK EXECUTION (Phase 2: Anti-arb)
                      Debug.print("ICP fallback split route found, executing both legs");
                      logger.info("ICP_FALLBACK_SPLIT",
                        "Executing split ICP fallback - Kong=" # Nat.toText(split.kongswap.percentBP / 100) # "%" #
                        " ICP=" # Nat.toText(split.icpswap.percentBP / 100) # "%" #
                        " TACO=" # Nat.toText(split.taco.percentBP / 100) # "%",
                        "do_executeTradingStep"
                      );

                      let ourSlippageToleranceBasisPoints : Nat = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                      // Apply slippage adjustment to BOTH legs (Phase 1 logic for each)
                      // KongSwap leg
                      let kongFinalAmount : Nat = if (isExactTargeting and split.kongswap.slippageBP > 0) {
                        let denominator = 10000 + split.kongswap.slippageBP;
                        (split.kongswap.amount * 10000) / denominator
                      } else { split.kongswap.amount };

                      let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < split.kongswap.amount and split.kongswap.amount > 0) {
                        (split.kongswap.expectedOut * kongFinalAmount) / split.kongswap.amount
                      } else { split.kongswap.expectedOut };

                      let kongIdealOut : Nat = if (split.kongswap.slippageBP < 9900) {
                        (kongAdjustedExpectedOut * 10000) / (10000 - split.kongswap.slippageBP)
                      } else { kongAdjustedExpectedOut };

                      let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                      let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                      // ICPSwap leg
                      let icpFinalAmount : Nat = if (isExactTargeting and split.icpswap.slippageBP > 0) {
                        let denominator = 10000 + split.icpswap.slippageBP;
                        (split.icpswap.amount * 10000) / denominator
                      } else { split.icpswap.amount };

                      let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < split.icpswap.amount and split.icpswap.amount > 0) {
                        (split.icpswap.expectedOut * icpFinalAmount) / split.icpswap.amount
                      } else { split.icpswap.expectedOut };

                      let icpIdealOut : Nat = if (split.icpswap.slippageBP < 9900) {
                        (icpAdjustedExpectedOut * 10000) / (10000 - split.icpswap.slippageBP)
                      } else { icpAdjustedExpectedOut };

                      let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                      let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                      // TACO leg
                      let tacoFinalAmount : Nat = if (isExactTargeting and split.taco.slippageBP > 0) {
                        let denominator = 10000 + split.taco.slippageBP;
                        (split.taco.amount * 10000) / denominator
                      } else { split.taco.amount };
                      
                      let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < split.taco.amount and split.taco.amount > 0) {
                        (split.taco.expectedOut * tacoFinalAmount) / split.taco.amount
                      } else { split.taco.expectedOut };
                      
                      let tacoIdealOut : Nat = if (split.taco.slippageBP < 9900) {
                        (tacoAdjustedExpectedOut * 10000) / (10000 - split.taco.slippageBP)
                      } else { tacoAdjustedExpectedOut };
                      
                      let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                      let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                      // Execute both trades IN PARALLEL (to ICP) - no race conditions
                      let splitResult = await* executeSplitTrade(
                        sellToken, ICPprincipal,
                        kongFinalAmount, kongMinAmountOut, kongIdealOut,
                        icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                      );

                      // Track results
                      var kongSuccess = false;
                      var icpSuccess = false;
                      let lastTrades = Vector.clone(rebalanceState.lastTrades);

                      // Handle KongSwap result
                      switch (splitResult.kongResult) {
                        case (#ok(record)) {
                          let fallbackRecord : TradeRecord = {
                            record with
                            error = ?("ICP_FALLBACK_SPLIT: Original target was " # buySymbol # ", traded for ICP (KongSwap leg)");
                          };
                          Vector.add(lastTrades, fallbackRecord);
                          kongSuccess := true;
                          logger.info("ICP_FALLBACK_SPLIT", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                        };
                        case (#err(errKong)) {
                          let failedRecord : TradeRecord = {
                            tokenSold = sellToken; tokenBought = ICPprincipal;
                            amountSold = kongFinalAmount; amountBought = 0;
                            exchange = #KongSwap; timestamp = now();
                            success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errKong); slippage = 0.0;
                          };
                          Vector.add(lastTrades, failedRecord);
                          logger.error("ICP_FALLBACK_SPLIT", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                        };
                      };

                      // Handle ICPSwap result
                      switch (splitResult.icpResult) {
                        case (#ok(record)) {
                          let fallbackRecord : TradeRecord = {
                            record with
                            error = ?("ICP_FALLBACK_SPLIT: Original target was " # buySymbol # ", traded for ICP (ICPSwap leg)");
                          };
                          Vector.add(lastTrades, fallbackRecord);
                          icpSuccess := true;
                          logger.info("ICP_FALLBACK_SPLIT", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                        };
                        case (#err(errIcp)) {
                          let failedRecord : TradeRecord = {
                            tokenSold = sellToken; tokenBought = ICPprincipal;
                            amountSold = icpFinalAmount; amountBought = 0;
                            exchange = #ICPSwap; timestamp = now();
                            success = false; error = ?("ICP_FALLBACK_SPLIT failed: " # errIcp); slippage = 0.0;
                          };
                          Vector.add(lastTrades, failedRecord);
                          logger.error("ICP_FALLBACK_SPLIT", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                        };
                      };

                      // Handle TACO result
                      var tacoSuccess = false;
                      switch (splitResult.tacoResult) {
                        case (#ok(record)) {
                          Vector.add(lastTrades, record);
                          tacoSuccess := true;
                          logger.info("ICP_FALLBACK_SPLIT", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                        };
                        case (#err(e)) {
                          if (tacoFinalAmount > 0) {
                            let failedRecord : TradeRecord = {
                              tokenSold = sellToken; tokenBought = ICPprincipal;
                              amountSold = tacoFinalAmount; amountBought = 0;
                              exchange = #TACO; timestamp = now();
                              success = false; error = ?e; slippage = 0.0;
                            };
                            Vector.add(lastTrades, failedRecord);
                            logger.error("ICP_FALLBACK_SPLIT", "TACO leg failed: " # e, "do_executeTradingStep");
                          };
                        };
                      };

                      // Trim trade history if needed
                      if (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                        Vector.reverse(lastTrades);
                        while (Vector.size(lastTrades) > rebalanceConfig.maxTradesStored) {
                          ignore Vector.removeLast(lastTrades);
                        };
                        Vector.reverse(lastTrades);
                      };

                      // Update metrics
                      let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                      let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                      let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };

                      rebalanceState := {
                        rebalanceState with
                        metrics = {
                          rebalanceState.metrics with
                          totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                          totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                        };
                        lastTrades = lastTrades;
                      };

                      // At least one leg succeeded
                      if (kongSuccess or icpSuccess or tacoSuccess) {
                        success := true;
                        tradingBackoffLevel := 0;
                        Debug.print("ICP fallback split trade completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                        await updateBalances();
                        await takePortfolioSnapshot(#PostTrade);
                        checkPortfolioCircuitBreakerConditions();
                        await* logPortfolioState("Post-ICP-fallback-split completed");
                      } else {
                        // Both legs failed
                        incrementSkipCounter(#noExecutionPath);
                        rebalanceState := {
                          rebalanceState with
                          metrics = {
                            rebalanceState.metrics with
                            currentStatus = #Failed("ICP fallback split: both legs failed");
                          };
                        };
                        await* recoverFromFailure();
                      };
                    };

                    case (#ok(#Partial(partial))) {
                      // PARTIAL ICP FALLBACK EXECUTION
                      Debug.print("ICP fallback partial route found, executing both legs");
                      logger.info("ICP_FALLBACK_PARTIAL",
                        "Executing partial ICP fallback - Kong=" # Nat.toText(partial.kongswap.percentBP / 100) # "%" #
                        " ICP=" # Nat.toText(partial.icpswap.percentBP / 100) # "%" #
                        " TACO=" # Nat.toText(partial.taco.percentBP / 100) # "%" #
                        " Total=" # Nat.toText(partial.totalPercentBP / 100) # "%",
                        "do_executeTradingStep"
                      );

                      let ourSlippageToleranceBasisPoints = if (rebalanceConfig.maxSlippageBasisPoints > 10000) { 10000 } else { rebalanceConfig.maxSlippageBasisPoints };

                      // Slippage adjustment for both legs (same pattern as direct partial)
                      let kongFinalAmount : Nat = if (isExactTargeting and partial.kongswap.slippageBP > 0) {
                        let denominator = 10000 + partial.kongswap.slippageBP;
                        (partial.kongswap.amount * 10000) / denominator
                      } else { partial.kongswap.amount };

                      let kongAdjustedExpectedOut : Nat = if (kongFinalAmount < partial.kongswap.amount and partial.kongswap.amount > 0) {
                        (partial.kongswap.expectedOut * kongFinalAmount) / partial.kongswap.amount
                      } else { partial.kongswap.expectedOut };

                      let kongIdealOut : Nat = if (partial.kongswap.slippageBP < 9900) {
                        (kongAdjustedExpectedOut * 10000) / (10000 - partial.kongswap.slippageBP)
                      } else { kongAdjustedExpectedOut };

                      let kongToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                      let kongMinAmountOut : Nat = (kongIdealOut * kongToleranceMultiplier) / 10000;

                      let icpFinalAmount : Nat = if (isExactTargeting and partial.icpswap.slippageBP > 0) {
                        let denominator = 10000 + partial.icpswap.slippageBP;
                        (partial.icpswap.amount * 10000) / denominator
                      } else { partial.icpswap.amount };

                      let icpAdjustedExpectedOut : Nat = if (icpFinalAmount < partial.icpswap.amount and partial.icpswap.amount > 0) {
                        (partial.icpswap.expectedOut * icpFinalAmount) / partial.icpswap.amount
                      } else { partial.icpswap.expectedOut };

                      let icpIdealOut : Nat = if (partial.icpswap.slippageBP < 9900) {
                        (icpAdjustedExpectedOut * 10000) / (10000 - partial.icpswap.slippageBP)
                      } else { icpAdjustedExpectedOut };

                      let icpToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                      let icpMinAmountOut : Nat = (icpIdealOut * icpToleranceMultiplier) / 10000;

                      // TACO leg
                      let tacoFinalAmount : Nat = if (isExactTargeting and partial.taco.slippageBP > 0) {
                        let denominator = 10000 + partial.taco.slippageBP;
                        (partial.taco.amount * 10000) / denominator
                      } else { partial.taco.amount };
                      
                      let tacoAdjustedExpectedOut : Nat = if (tacoFinalAmount < partial.taco.amount and partial.taco.amount > 0) {
                        (partial.taco.expectedOut * tacoFinalAmount) / partial.taco.amount
                      } else { partial.taco.expectedOut };
                      
                      let tacoIdealOut : Nat = if (partial.taco.slippageBP < 9900) {
                        (tacoAdjustedExpectedOut * 10000) / (10000 - partial.taco.slippageBP)
                      } else { tacoAdjustedExpectedOut };
                      
                      let tacoToleranceMultiplier : Nat = if (ourSlippageToleranceBasisPoints >= 10000) { 0 } else { 10000 - ourSlippageToleranceBasisPoints };
                      let tacoMinAmountOut : Nat = (tacoIdealOut * tacoToleranceMultiplier) / 10000;

                      // Execute both trades IN PARALLEL (to ICP)
                      let splitResult = await* executeSplitTrade(
                        sellToken, ICPprincipal,
                        kongFinalAmount, kongMinAmountOut, kongIdealOut,
                        icpFinalAmount, icpMinAmountOut, icpIdealOut,
                    tacoFinalAmount, tacoMinAmountOut, tacoIdealOut
                      );

                      // Track results
                      var kongSuccess = false;
                      var icpSuccess = false;
                      let lastTrades = Vector.clone(rebalanceState.lastTrades);

                      switch (splitResult.kongResult) {
                        case (#ok(record)) {
                          let fallbackRecord : TradeRecord = {
                            record with
                            error = ?("ICP_FALLBACK_PARTIAL: Original target was " # buySymbol # ", traded for ICP (KongSwap leg)");
                          };
                          Vector.add(lastTrades, fallbackRecord);
                          kongSuccess := true;
                          logger.info("ICP_FALLBACK_PARTIAL", "KongSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                        };
                        case (#err(errKong)) {
                          let failedRecord : TradeRecord = {
                            tokenSold = sellToken; tokenBought = ICPprincipal;
                            amountSold = kongFinalAmount; amountBought = 0;
                            exchange = #KongSwap; timestamp = now();
                            success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errKong); slippage = 0.0;
                          };
                          Vector.add(lastTrades, failedRecord);
                          logger.error("ICP_FALLBACK_PARTIAL", "KongSwap leg failed: " # errKong, "do_executeTradingStep");
                        };
                      };

                      switch (splitResult.icpResult) {
                        case (#ok(record)) {
                          let fallbackRecord : TradeRecord = {
                            record with
                            error = ?("ICP_FALLBACK_PARTIAL: Original target was " # buySymbol # ", traded for ICP (ICPSwap leg)");
                          };
                          Vector.add(lastTrades, fallbackRecord);
                          icpSuccess := true;
                          logger.info("ICP_FALLBACK_PARTIAL", "ICPSwap leg succeeded - ICP_received=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                        };
                        case (#err(errIcp)) {
                          let failedRecord : TradeRecord = {
                            tokenSold = sellToken; tokenBought = ICPprincipal;
                            amountSold = icpFinalAmount; amountBought = 0;
                            exchange = #ICPSwap; timestamp = now();
                            success = false; error = ?("ICP_FALLBACK_PARTIAL failed: " # errIcp); slippage = 0.0;
                          };
                          Vector.add(lastTrades, failedRecord);
                          logger.error("ICP_FALLBACK_PARTIAL", "ICPSwap leg failed: " # errIcp, "do_executeTradingStep");
                        };
                      };

                      // Handle TACO result
                      var tacoSuccess = false;
                      switch (splitResult.tacoResult) {
                        case (#ok(record)) {
                          Vector.add(lastTrades, record);
                          tacoSuccess := true;
                          logger.info("ICP_FALLBACK_PARTIAL", "TACO leg succeeded - Amount_out=" # Nat.toText(record.amountBought), "do_executeTradingStep");
                        };
                        case (#err(e)) {
                          if (tacoFinalAmount > 0) {
                            let failedRecord : TradeRecord = {
                              tokenSold = sellToken; tokenBought = ICPprincipal;
                              amountSold = tacoFinalAmount; amountBought = 0;
                              exchange = #TACO; timestamp = now();
                              success = false; error = ?e; slippage = 0.0;
                            };
                            Vector.add(lastTrades, failedRecord);
                            logger.error("ICP_FALLBACK_PARTIAL", "TACO leg failed: " # e, "do_executeTradingStep");
                          };
                        };
                      };

                      // Update metrics
                      let successCount : Nat = (if kongSuccess { 1 } else { 0 }) + (if icpSuccess { 1 } else { 0 }) + (if tacoSuccess { 1 } else { 0 });
                      let attemptedLegs : Nat = (if (kongFinalAmount > 0) { 1 } else { 0 }) + (if (icpFinalAmount > 0) { 1 } else { 0 }) + (if (tacoFinalAmount > 0) { 1 } else { 0 });
                      let failCount : Nat = if (attemptedLegs > successCount) { attemptedLegs - successCount } else { 0 };
                      rebalanceState := {
                        rebalanceState with
                        metrics = {
                          rebalanceState.metrics with
                          totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + successCount;
                          totalTradesFailed = rebalanceState.metrics.totalTradesFailed + failCount;
                        };
                        lastTrades = lastTrades;
                      };

                      // At least one leg succeeded
                      if (kongSuccess or icpSuccess or tacoSuccess) {
                        success := true;
                        tradingBackoffLevel := 0;
                        Debug.print("ICP fallback partial trade completed - Kong=" # debug_show(kongSuccess) # " ICP=" # debug_show(icpSuccess));
                        await updateBalances();
                        await takePortfolioSnapshot(#PostTrade);
                        checkPortfolioCircuitBreakerConditions();
                        await* logPortfolioState("Post-ICP-fallback-partial completed");
                      } else {
                        incrementSkipCounter(#noExecutionPath);
                        rebalanceState := {
                          rebalanceState with
                          metrics = {
                            rebalanceState.metrics with
                            currentStatus = #Failed("ICP fallback partial: both legs failed");
                          };
                        };
                        await* recoverFromFailure();
                      };
                    };

                    case (#err(icpRouteError)) {
                      Debug.print("ICP fallback route also not available: " # icpRouteError.reason);

                      // NEW: Try REDUCED amount for ICP fallback
                      // estimateMaxTradeableAmount already checked both exchanges and returns idealOut/minAmountOut
                      label reducedIcpFallback switch (estimateMaxTradeableAmount(icpRouteError.kongQuotes, icpRouteError.icpQuotes, cappedTradeSize, rebalanceConfig.maxSlippageBasisPoints, sellToken, ICPprincipal)) {
                        case (?reduced) {
                          // Skip if reduced trade is too small to be worthwhile
                          if (reduced.icpWorth < rebalanceConfig.minTradeValueICP / 3) {
                            logger.info("SKIP_REDUCED",
                              "Reduced ICP fallback worth (" # Nat.toText(reduced.icpWorth) #
                              ") < minTradeValueICP/3 (" # Nat.toText(rebalanceConfig.minTradeValueICP / 3) #
                              ") - skipping",
                              "do_executeTradingStep"
                            );
                            break reducedIcpFallback;
                          };

                          let reducedSellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
                            case (?details) { details.tokenSymbol };
                            case null { break reducedIcpFallback };
                          };

                          logger.info("REDUCED_ICP_FALLBACK",
                            "Executing reduced ICP fallback directly - Original=" # Nat.toText(tradeSize) #
                            " Reduced=" # Nat.toText(reduced.amount) #
                            " Exchange=" # debug_show(reduced.exchange) #
                            " IdealOut=" # Nat.toText(reduced.idealOut) #
                            " MinAmountOut=" # Nat.toText(reduced.minAmountOut) #
                            " IcpWorth=" # Nat.toText(reduced.icpWorth),
                            "do_executeTradingStep"
                          );

                          // Execute directly - estimateMaxTradeableAmount already calculated idealOut and minAmountOut
                          let reducedIcpTradeResult = await* executeTrade(
                            sellToken,
                            ICPprincipal,
                            reduced.amount,
                            reduced.exchange,
                            reduced.minAmountOut,
                            reduced.idealOut,
                          );

                          switch (reducedIcpTradeResult) {
                            case (#ok(record)) {
                              let lastTrades = Vector.clone(rebalanceState.lastTrades);
                              let reducedIcpRecord : TradeRecord = {
                                record with
                                error = ?("REDUCED_ICP_FALLBACK: Original target was " # buySymbol # ", traded reduced amount " # Nat.toText(reduced.amount) # " for ICP");
                              };
                              Vector.add(lastTrades, reducedIcpRecord);
                              if (Vector.size(lastTrades) >= rebalanceConfig.maxTradesStored) {
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
                                  totalTradesExecuted = rebalanceState.metrics.totalTradesExecuted + 1;
                                };
                                lastTrades = lastTrades;
                              };

                              success := true;
                              tradingBackoffLevel := 0;
                              logger.info("REDUCED_ICP_FALLBACK",
                                "REDUCED ICP fallback SUCCESS - Sold=" # Nat.toText(record.amountSold) #
                                " ICP_received=" # Nat.toText(record.amountBought),
                                "do_executeTradingStep"
                              );

                              await updateBalances();
                              await takePortfolioSnapshot(#PostTrade);
                              checkPortfolioCircuitBreakerConditions();
                              await* logPortfolioState("Post-REDUCED-ICP-fallback completed");
                            };
                            case (#err(reducedIcpError)) {
                              logger.warn("REDUCED_ICP_FALLBACK",
                                "REDUCED ICP fallback execution failed - Error=" # reducedIcpError,
                                "do_executeTradingStep"
                              );
                              // Continue to failure path
                            };
                          };
                        };
                        case null {
                          Debug.print("No viable reduced ICP fallback amount estimated");
                        };
                      };

                      // If reduced ICP fallback succeeded, return early
                      if (success) { return };

                      // VERBOSE LOGGING: ICP fallback route not found
                      logger.error("ICP_FALLBACK",
                        "ICP fallback route NOT FOUND - Original_error=" # e.reason #
                        " ICP_route_error=" # icpRouteError.reason #
                        " Status=No_routes_available",
                        "do_executeTradingStep"
                      );

                      // No routes available at all - count as skip
                      incrementSkipCounter(#noExecutionPath);
                      rebalanceState := {
                        rebalanceState with
                        metrics = {
                          rebalanceState.metrics with
                          currentStatus = #Failed("No routes available: " # e.reason # " | ICP fallback: " # icpRouteError.reason);
                        };
                      };

                      // Attempt system recovery
                      await* recoverFromFailure();
                    };
                  };
                } else if (buyToken == ICPprincipal or sellToken == ICPprincipal) {
                  // We were already trying to buy ICP and that failed - no fallback possible
                  Debug.print("No ICP fallback possible - was already trying to buy ICP");

                  logger.warn("ICP_FALLBACK",
                    "No ICP fallback possible - Original trade was already targeting ICP" #
                    " Error=" # e.reason #
                    " Status=ICP_route_failed",
                    "do_executeTradingStep"
                  );

                  incrementSkipCounter(#noExecutionPath);
                  rebalanceState := {
                    rebalanceState with
                    metrics = {
                      rebalanceState.metrics with
                      currentStatus = #Failed(e.reason);
                    };
                  };

                  // Attempt system recovery
                  await* recoverFromFailure();
                } else {
                  // ICP fallback would be possible but ICP is paused
                  Debug.print("ICP fallback not attempted - ICP is paused from trading");
                  
                  logger.warn("ICP_FALLBACK", 
                    "ICP fallback skipped - ICP is paused from trading" #
                    " Original_error=" # e.reason #
                    " Status=ICP_paused_fallback_skipped",
                    "do_executeTradingStep"
                  );
                  
                  incrementSkipCounter(#pausedTokens);
                  rebalanceState := {
                    rebalanceState with
                    metrics = {
                      rebalanceState.metrics with
                      currentStatus = #Failed("No execution path and ICP fallback unavailable (ICP paused): " # e.reason);
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

        // Exponential backoff: double interval each failure, cap at MAX_TRADING_BACKOFF
        if (tradingBackoffLevel < MAX_TRADING_BACKOFF) {
          tradingBackoffLevel += 1;
        };

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
          " - Backoff_level=" # Nat.toText(tradingBackoffLevel) #
          " Next_interval=" # Nat.toText(rebalanceConfig.rebalanceIntervalNS * (2 ** tradingBackoffLevel) / 1_000_000_000) # "s" #
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

    // ===== STEP C: LP DEPLOYMENTS (after trading) =====
    // Deploy LP to underweight pools using newly bought tokens
    if (lpConfig.enabled and not isNachosHighVolume() and lastLPQueryTimestamp > 0) {
      let lpTargets = computeLPTargets(); // Recompute with post-trade state
      let additions = Vector.new<(Text, Principal, Principal, Nat)>();
      for ((poolKey, t0, t1, target, current, _) in lpTargets.vals()) {
        if (target > current) {
          let deficit = target - current;
          if (target > 0 and (deficit * 10_000 / target) > lpConfig.rebalanceThresholdBP) {
            // deficit/2 = one side's ICP value (pool is 50/50)
            Vector.add(additions, (poolKey, t0, t1, deficit / 2));
          };
        };
      };
      let sortedAdditions = Array.sort<(Text, Principal, Principal, Nat)>(
        Vector.toArray(additions),
        func(a, b) { Nat.compare(b.3, a.3) },
      );
      var lpAddOps : Nat = 0;
      label lpAddLoop for (addition in sortedAdditions.vals()) {
        if (not lpUncapNextCycle and lpAddOps >= lpConfig.maxAdjustmentsPerCycle) break lpAddLoop;
        await addLiquidityToPool(addition.0, addition.1, addition.2, addition.3);
        lpAddOps += 1;
      };
    };

    // ===== STEP D: REFRESH (after all LP moves) =====
    // Refresh balances to reflect LP changes for accurate state going into next cycle
    if (lpConfig.enabled) {
      await updateBalances();
    };

    // Reset post-circuit-breaker uncap flag after one full cycle
    if (lpUncapNextCycle) {
      lpUncapNextCycle := false;
      logger.info("LP_CYCLE", "Post-circuit-breaker uncapped LP cycle complete", "do_executeTradingStep");
    };
  };

  //=========================================================================
  // 5B. LP MANAGEMENT — TARGET COMPUTATION & OPERATIONS
  //=========================================================================

  // Compute target LP for each eligible pool
  // Returns: [(poolKey, token0, token1, targetLPValueICP, currentLPValueICP, currentLiquidity)]
  private func computeLPTargets() : [(Text, Principal, Principal, Nat, Nat, Nat)] {
    if (not lpConfig.enabled) return [];

    let targets = Vector.new<(Text, Principal, Principal, Nat, Nat, Nat)>();

    // Total portfolio value for percentage calculations
    var totalPortfolioICP : Nat = 0;
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and not isTokenPausedFromTrading(principal)) {
        totalPortfolioICP += (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
      };
    };
    if (totalPortfolioICP == 0) return [];

    let processedPools = Map.new<Text, Bool>();

    // For each pool that has BOTH tokens in allocations:
    for (pool in cachedPoolData.vals()) {
      let t0 = Principal.fromText(pool.token0);
      let t1 = Principal.fromText(pool.token1);
      let poolKey = normalizePoolKeyText(pool.token0, pool.token1);
      Map.set(processedPools, thash, poolKey, true);
      let currentLPVal = getCurrentLPValueICP(poolKey);
      let currentLiq = getCurrentLiquidity(poolKey);

      if (not isPoolLPEnabled(poolKey)) {
        // LP disabled for this pool — target = 0 (forces removal if position exists)
        if (currentLPVal > 0) { Vector.add(targets, (poolKey, t0, t1, 0, currentLPVal, currentLiq)) };
      } else {
        let alloc0 = switch (Map.get(currentAllocations, phash, t0)) { case (?v) v; case null 0 };
        let alloc1 = switch (Map.get(currentAllocations, phash, t1)) { case (?v) v; case null 0 };
        if (alloc0 == 0 or alloc1 == 0) {
          // One or both tokens have no allocation — remove LP if present
          if (currentLPVal > 0) { Vector.add(targets, (poolKey, t0, t1, 0, currentLPVal, currentLiq)) };
        } else {
          // Step 1: How much of each token's allocation is available for THIS pool?
          let partners0 = getPartnerAllocSum(t0);
          let partners1 = getPartnerAllocSum(t1);
          if (partners0 == 0 or partners1 == 0) {
            if (currentLPVal > 0) { Vector.add(targets, (poolKey, t0, t1, 0, currentLPVal, currentLiq)) };
          } else {
            let available0BP = alloc0 * alloc1 / partners0;
            let available1BP = alloc1 * alloc0 / partners1;

            // Step 2: LP bounded by smaller side (50/50 by value)
            let maxLPBP = Nat.min(available0BP, available1BP);
            let maxLPValueICP = (maxLPBP * totalPortfolioICP) / 10_000;

            // Step 3: Apply LP ratio with NACHOS buffer
            let lpRatio = getPoolLpRatio(poolKey);
            let effectiveRatioBP = (lpRatio * (10_000 - lpConfig.nachosRedemptionBufferBP)) / 10_000;

            // Step 4: Final target — allocation formula × effective ratio is the only limit
            // No pool depth cap: treasury can be sole LP, allocation formula naturally bounds deployment
            let targetLPValueICP = maxLPValueICP * effectiveRatioBP / 10_000;

            // Step 6: Subtract transfer fees and check minimum
            let tfee0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
            let tfee1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
            let d0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
            let d1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
            let p0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.priceInICP }; case null { 0 } };
            let p1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.priceInICP }; case null { 0 } };
            let tfee0ICP = if (tfee0 > 0 and p0 > 0 and d0 > 0) { (tfee0 * p0) / (10 ** d0) } else { 0 };
            let tfee1ICP = if (tfee1 > 0 and p1 > 0 and d1 > 0) { (tfee1 * p1) / (10 ** d1) } else { 0 };
            let netTarget = if (targetLPValueICP > tfee0ICP + tfee1ICP) { targetLPValueICP - tfee0ICP - tfee1ICP } else { 0 };

            if (netTarget < lpConfig.minLPValueICP) {
              // Below minimum — set target to 0 (remove if exists)
              if (currentLPVal > 0) { Vector.add(targets, (poolKey, t0, t1, 0, currentLPVal, currentLiq)) };
            } else {
              // Per-token LP budget constraint: total LP across all pools ≤ token allocation × lpRatio
              // This prevents over-deploying a single token across many pools
              let budget0 = (alloc0 * totalPortfolioICP * lpConfig.lpRatioBP) / (10_000 * 10_000);
              let budget1 = (alloc1 * totalPortfolioICP * lpConfig.lpRatioBP) / (10_000 * 10_000);
              let used0 = switch (Map.get(lpBudgetUsedPerToken, phash, t0)) { case (?v) v; case null 0 };
              let used1 = switch (Map.get(lpBudgetUsedPerToken, phash, t1)) { case (?v) v; case null 0 };
              let remaining0 = if (budget0 > used0) { budget0 - used0 } else { 0 };
              let remaining1 = if (budget1 > used1) { budget1 - used1 } else { 0 };
              let budgetCapped = Nat.min(netTarget, Nat.min(remaining0, remaining1));

              if (budgetCapped < lpConfig.minLPValueICP) {
                // After budget cap, below minimum — skip addition but keep existing
                Vector.add(targets, (poolKey, t0, t1, Nat.min(budgetCapped, currentLPVal), currentLPVal, currentLiq));
              } else {
                Vector.add(targets, (poolKey, t0, t1, budgetCapped, currentLPVal, currentLiq));
              };
            };
          };
        };
      };
    };

    // Second pass: pools derivable from accepted tokens that don't exist yet on the exchange
    // Generate all pairs of (treasuryToken, ICP) where both are accepted by the exchange
    for (t0Text in cachedExchangeAcceptedTokens.vals()) {
      for (t1Text in cachedExchangeAcceptedTokens.vals()) {
        if (t0Text >= t1Text) {} else { // Skip self-pairs and duplicates (only process t0 < t1)
          let poolKey = normalizePoolKeyText(t0Text, t1Text);
          if (not isPoolLPEnabled(poolKey) or Map.has(processedPools, thash, poolKey)) {} else {
          let t0 = Principal.fromText(t0Text);
          let t1 = Principal.fromText(t1Text);
          // Only consider pairs where both tokens are in the treasury portfolio
          let inPortfolio0 = Map.has(tokenDetailsMap, phash, t0);
          let inPortfolio1 = Map.has(tokenDetailsMap, phash, t1);
          if (inPortfolio0 and inPortfolio1) {
          let alloc0 = switch (Map.get(currentAllocations, phash, t0)) { case (?v) v; case null 0 };
          let alloc1 = switch (Map.get(currentAllocations, phash, t1)) { case (?v) v; case null 0 };
          if (alloc0 > 0 and alloc1 > 0) {
            let partners0 = getPartnerAllocSum(t0);
            let partners1 = getPartnerAllocSum(t1);
            if (partners0 > 0 and partners1 > 0) {
              let available0BP = alloc0 * alloc1 / partners0;
              let available1BP = alloc1 * alloc0 / partners1;
              let maxLPBP = Nat.min(available0BP, available1BP);
              let maxLPValueICP = (maxLPBP * totalPortfolioICP) / 10_000;
              let lpRatio = getPoolLpRatio(poolKey);
              let effectiveRatioBP = (lpRatio * (10_000 - lpConfig.nachosRedemptionBufferBP)) / 10_000;
              // New pool, no depth cap
              let targetLPValueICP = maxLPValueICP * effectiveRatioBP / 10_000;

              // Transfer fees check
              let tfee0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
              let tfee1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
              let d0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
              let d1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
              let p0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.priceInICP }; case null { 0 } };
              let p1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.priceInICP }; case null { 0 } };
              let tfee0ICP = if (tfee0 > 0 and p0 > 0 and d0 > 0) { (tfee0 * p0) / (10 ** d0) } else { 0 };
              let tfee1ICP = if (tfee1 > 0 and p1 > 0 and d1 > 0) { (tfee1 * p1) / (10 ** d1) } else { 0 };
              let netTarget = if (targetLPValueICP > tfee0ICP + tfee1ICP) { targetLPValueICP - tfee0ICP - tfee1ICP } else { 0 };

              if (netTarget >= lpConfig.minLPValueICP) {
                // Budget constraint
                let budget0 = (alloc0 * totalPortfolioICP * lpConfig.lpRatioBP) / (10_000 * 10_000);
                let budget1 = (alloc1 * totalPortfolioICP * lpConfig.lpRatioBP) / (10_000 * 10_000);
                let used0 = switch (Map.get(lpBudgetUsedPerToken, phash, t0)) { case (?v) v; case null 0 };
                let used1 = switch (Map.get(lpBudgetUsedPerToken, phash, t1)) { case (?v) v; case null 0 };
                let remaining0 = if (budget0 > used0) { budget0 - used0 } else { 0 };
                let remaining1 = if (budget1 > used1) { budget1 - used1 } else { 0 };
                let budgetCapped = Nat.min(netTarget, Nat.min(remaining0, remaining1));

                if (budgetCapped >= lpConfig.minLPValueICP) {
                  Vector.add(targets, (poolKey, t0, t1, budgetCapped, 0, 0));
                };
              };
            };
          };
          }; // isPoolLPEnabled and not processed
          }; // t0 < t1
        };
      };
    };

    Vector.toArray(targets);
  };

  // Remove LP from a pool (Step A in trading cycle)
  // LP removal is NOT gated by token pause (protective action)
  private func removeLiquidityFromPool(
    poolKey : Text, t0 : Principal, t1 : Principal,
    excessValueICP : Nat, currentLPValueICP : Nat, currentLiquidity : Nat,
  ) : async () {
    if (currentLPValueICP == 0 or currentLiquidity == 0) return;

    // Compute liquidity units to remove proportionally
    let liquidityToRemove = (excessValueICP * currentLiquidity) / currentLPValueICP;
    if (liquidityToRemove == 0) return;

    let t0Text = Principal.toText(t0);
    let t1Text = Principal.toText(t1);

    logger.info("LP_REMOVE", "Removing LP from " # poolKey # " liq=" # Nat.toText(liquidityToRemove) # " excess=" # Nat.toText(excessValueICP) # "e8s", "removeLiquidityFromPool");

    try {
      let result = await (with timeout = 65) TACOSwap.doRemoveLiquidity(t0Text, t1Text, liquidityToRemove);
      switch (result) {
        case (#Ok(ok)) {
          // Tokens being sent back via exchange treasury → arrive async
          // Track NET amounts (after exchange transfer fees) so arrival detection works.
          // Exchange sends (amount + fees - Tfees) to treasury wallet.
          let tfee0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
          let tfee1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
          let gross0 = ok.amount0 + ok.fees0;
          let gross1 = ok.amount1 + ok.fees1;
          let net0 = if (gross0 > tfee0) { gross0 - tfee0 } else { 0 };
          let net1 = if (gross1 > tfee1) { gross1 - tfee1 } else { 0 };
          let curT0 = switch (Map.get(lpTokensInTransit, phash, t0)) { case (?(amt, _)) amt; case null 0 };
          let curT1 = switch (Map.get(lpTokensInTransit, phash, t1)) { case (?(amt, _)) amt; case null 0 };
          Map.set(lpTokensInTransit, phash, t0, (curT0 + net0, now()));
          Map.set(lpTokensInTransit, phash, t1, (curT1 + net1, now()));

          // Reduce lpBacking (position removed on exchange side)
          let curB0 = switch (Map.get(lpBackingPerToken, phash, t0)) { case (?v) v; case null 0 };
          let curB1 = switch (Map.get(lpBackingPerToken, phash, t1)) { case (?v) v; case null 0 };
          let reduc0 = ok.amount0 + ok.fees0;
          let reduc1 = ok.amount1 + ok.fees1;
          Map.set(lpBackingPerToken, phash, t0, if (curB0 > reduc0) { curB0 - reduc0 } else { 0 });
          Map.set(lpBackingPerToken, phash, t1, if (curB1 > reduc1) { curB1 - reduc1 } else { 0 });

          // Update cumulative tracking
          switch (Map.get(treasuryLPPositions, thash, poolKey)) {
            case (?pos) {
              Map.set(treasuryLPPositions, thash, poolKey, {
                pos with
                totalWithdrawn0 = pos.totalWithdrawn0 + ok.amount0;
                totalWithdrawn1 = pos.totalWithdrawn1 + ok.amount1;
                totalFeesEarned0 = pos.totalFeesEarned0 + ok.fees0;
                totalFeesEarned1 = pos.totalFeesEarned1 + ok.fees1;
              });
            };
            case null {};
          };

          logTreasuryAdminAction(Principal.fromActor(this), #LPRemoveLiquidity({ pool = poolKey; details = "burned=" # Nat.toText(ok.liquidityBurned) # " returned=" # Nat.toText(ok.amount0) # "/" # Nat.toText(ok.amount1) }), "LP removal from " # poolKey, true, null);
          logger.info("LP_REMOVE", "Success: a0=" # Nat.toText(ok.amount0) # " a1=" # Nat.toText(ok.amount1) # " f0=" # Nat.toText(ok.fees0) # " f1=" # Nat.toText(ok.fees1), "removeLiquidityFromPool");
        };
        case (#Err(e)) {
          logger.error("LP_REMOVE", "Failed: " # debug_show(e), "removeLiquidityFromPool");
        };
      };
    } catch (e) {
      logger.error("LP_REMOVE", "Exception: " # Error.message(e), "removeLiquidityFromPool");
    };
  };

  // Add LP to a pool (Step C in trading cycle)
  private func addLiquidityToPool(
    poolKey : Text, t0 : Principal, t1 : Principal,
    targetOneSideICP : Nat, // One side's ICP value target
  ) : async () {
    let t0Text = Principal.toText(t0);
    let t1Text = Principal.toText(t1);

    // Get token details
    let d0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) d; case null return };
    let d1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) d; case null return };
    let dec0 : Nat = d0.tokenDecimals;
    let dec1 : Nat = d1.tokenDecimals;

    // Price sanity check: pool price vs treasury consensus
    for (pool in cachedPoolData.vals()) {
      if (normalizePoolKeyText(pool.token0, pool.token1) == poolKey) {
        if (pool.reserve0 > 0 and d0.priceInICP > 0 and d1.priceInICP > 0 and dec0 > 0 and dec1 > 0) {
          // Pool price ratio: reserve1/reserve0 adjusted for decimals
          // Treasury price ratio: priceInICP0/priceInICP1
          // Simple deviation: compare ICP values of 1 unit of each in pool vs treasury
          let poolVal0 = (pool.reserve1 * (10 ** dec0)) / pool.reserve0; // value of 1 token0 in token1 units
          let treasuryVal0 = if (d1.priceInICP > 0) { (d0.priceInICP * (10 ** dec1)) / d1.priceInICP } else { 0 };
          if (treasuryVal0 > 0) {
            let deviation = if (poolVal0 > treasuryVal0) {
              ((poolVal0 - treasuryVal0) * 10_000) / treasuryVal0
            } else {
              ((treasuryVal0 - poolVal0) * 10_000) / treasuryVal0
            };
            if (deviation > lpConfig.priceDeviationMaxBP) {
              logger.warn("LP_ADD", "Price deviation " # Nat.toText(deviation) # "bp for " # poolKey # " — skipping", "addLiquidityToPool");
              return;
            };
          };
        };
      };
    };

    // Convert ICP value to token amounts
    if (d0.priceInICP == 0 or d1.priceInICP == 0 or dec0 == 0 or dec1 == 0) return;
    let amount0 = (targetOneSideICP * (10 ** dec0)) / d0.priceInICP;
    let amount1 = (targetOneSideICP * (10 ** dec1)) / d1.priceInICP;
    if (amount0 == 0 or amount1 == 0) return;

    // Check liquid balance (subtract pending burns)
    let liquid0 = switch (Map.get(liquidBalancePerToken, phash, t0)) { case (?v) v; case null 0 };
    let liquid1 = switch (Map.get(liquidBalancePerToken, phash, t1)) { case (?v) v; case null 0 };
    let pending0 = getPendingBurn(t0);
    let pending1 = getPendingBurn(t1);
    let available0 = if (liquid0 > pending0 + d0.tokenTransferFee) { liquid0 - pending0 - d0.tokenTransferFee } else { 0 };
    let available1 = if (liquid1 > pending1 + d1.tokenTransferFee) { liquid1 - pending1 - d1.tokenTransferFee } else { 0 };

    if (available0 < amount0 or available1 < amount1) {
      logger.info("LP_ADD", "Insufficient liquid for " # poolKey # " need=" # Nat.toText(amount0) # "/" # Nat.toText(amount1) # " avail=" # Nat.toText(available0) # "/" # Nat.toText(available1), "addLiquidityToPool");
      return;
    };

    // Pre-check exchange minimums: exchange requires amount > minimumAmount * 10 for LP
    let exchangeMin0 = switch (Map.get(cachedExchangeMinimums, thash, t0Text)) { case (?m) { m * 10 }; case null { d0.tokenTransferFee * 20 } };
    let exchangeMin1 = switch (Map.get(cachedExchangeMinimums, thash, t1Text)) { case (?m) { m * 10 }; case null { d1.tokenTransferFee * 20 } };
    if (amount0 < exchangeMin0 or amount1 < exchangeMin1) {
      logger.info("LP_ADD", "Below exchange minimum for " # poolKey # " amounts=" # Nat.toText(amount0) # "/" # Nat.toText(amount1) # " mins=" # Nat.toText(exchangeMin0) # "/" # Nat.toText(exchangeMin1), "addLiquidityToPool");
      return;
    };

    logger.info("LP_ADD", "Adding LP to " # poolKey # " amounts=" # Nat.toText(amount0) # "/" # Nat.toText(amount1), "addLiquidityToPool");

    // Store pending deposit for crash recovery
    let depositKey = poolKey # ":" # Int.toText(now());
    Map.set(lpPendingDeposits, thash, depositKey, {
      block0 = 0; block1 = 0;
      token0 = t0Text; token1 = t1Text;
      amount0 = amount0; amount1 = amount1;
      timestamp = now();
    });

    // Compute exchange treasury account ID for ICP legacy transfers
    let exchangeTreasuryAccountId = Principal.toLedgerAccount(Principal.fromText("qbnpl-laaaa-aaaan-q52aq-cai"), null);

    // Transfer token0 to exchange treasury
    let block0Result = try {
      await (with timeout = 65) TACOSwap.transferToExchangeTreasury(t0, amount0, exchangeTreasuryAccountId);
    } catch (e) { #err("Transfer0 exception: " # Error.message(e)) };

    let block0 = switch (block0Result) {
      case (#ok(b)) { b };
      case (#err(e)) {
        logger.error("LP_ADD", "Token0 transfer failed: " # e, "addLiquidityToPool");
        Map.delete(lpPendingDeposits, thash, depositKey);
        return;
      };
    };

    // Update pending with block0 and track in-flight
    Map.set(lpPendingDeposits, thash, depositKey, {
      block0 = block0; block1 = 0;
      token0 = t0Text; token1 = t1Text;
      amount0 = amount0; amount1 = amount1;
      timestamp = now();
    });
    let curFlight0 = switch (Map.get(lpDepositsInFlight, phash, t0)) { case (?v) v; case null 0 };
    Map.set(lpDepositsInFlight, phash, t0, curFlight0 + amount0);
    // Immediately decrement liquid tracking: tokens have left the wallet
    // This maintains net-zero: -liquid + inFlight = 0
    let liq0now = switch (Map.get(liquidBalancePerToken, phash, t0)) { case (?v) v; case null 0 };
    Map.set(liquidBalancePerToken, phash, t0, if (liq0now > amount0) { liq0now - amount0 } else { 0 });

    // Transfer token1 to exchange treasury
    let block1Result = try {
      await (with timeout = 65) TACOSwap.transferToExchangeTreasury(t1, amount1, exchangeTreasuryAccountId);
    } catch (e) { #err("Transfer1 exception: " # Error.message(e)) };

    let block1 = switch (block1Result) {
      case (#ok(b)) { b };
      case (#err(e)) {
        logger.error("LP_ADD", "Token1 transfer failed: " # e # " — block0=" # Nat.toText(block0) # " saved for recovery", "addLiquidityToPool");
        return; // lpPendingDeposits has block0 for recovery
      };
    };

    // Update pending with both blocks and track in-flight
    Map.set(lpPendingDeposits, thash, depositKey, {
      block0 = block0; block1 = block1;
      token0 = t0Text; token1 = t1Text;
      amount0 = amount0; amount1 = amount1;
      timestamp = now();
    });
    let curFlight1 = switch (Map.get(lpDepositsInFlight, phash, t1)) { case (?v) v; case null 0 };
    Map.set(lpDepositsInFlight, phash, t1, curFlight1 + amount1);
    // Immediately decrement liquid tracking: tokens have left the wallet
    let liq1now = switch (Map.get(liquidBalancePerToken, phash, t1)) { case (?v) v; case null 0 };
    Map.set(liquidBalancePerToken, phash, t1, if (liq1now > amount1) { liq1now - amount1 } else { 0 });

    // Call addLiquidity on exchange
    try {
      let result = await (with timeout = 65) TACOSwap.doAddLiquidity(t0Text, t1Text, amount0, amount1, block0, block1);
      switch (result) {
        case (#Ok(ok)) {
          // Clear pending deposit and in-flight
          Map.delete(lpPendingDeposits, thash, depositKey);
          Map.delete(lpDepositsInFlight, phash, t0);
          Map.delete(lpDepositsInFlight, phash, t1);

          // Increase LP backing: -inFlight + lpBacking = net 0 (for used amounts)
          let curB0 = switch (Map.get(lpBackingPerToken, phash, t0)) { case (?v) v; case null 0 };
          let curB1 = switch (Map.get(lpBackingPerToken, phash, t1)) { case (?v) v; case null 0 };
          Map.set(lpBackingPerToken, phash, t0, curB0 + ok.amount0Used);
          Map.set(lpBackingPerToken, phash, t1, curB1 + ok.amount1Used);

          // Track refunds as inTransit — use NET amounts (after exchange transfer fees)
          // so arrival detection in updateBalances() works correctly.
          // Exchange sends refund - Tfees back to treasury wallet.
          let rfee0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
          let rfee1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenTransferFee }; case null { 0 } };
          if (ok.refund0 > rfee0) {
            let netRefund0 = ok.refund0 - rfee0;
            let curT0 = switch (Map.get(lpTokensInTransit, phash, t0)) { case (?(amt, _)) amt; case null 0 };
            Map.set(lpTokensInTransit, phash, t0, (curT0 + netRefund0, now()));
          };
          if (ok.refund1 > rfee1) {
            let netRefund1 = ok.refund1 - rfee1;
            let curT1 = switch (Map.get(lpTokensInTransit, phash, t1)) { case (?(amt, _)) amt; case null 0 };
            Map.set(lpTokensInTransit, phash, t1, (curT1 + netRefund1, now()));
          };

          // Update cumulative tracking
          let existingPos = switch (Map.get(treasuryLPPositions, thash, poolKey)) {
            case (?pos) { pos };
            case null {
              { token0Principal = t0; token1Principal = t1;
                totalDeposited0 = 0; totalDeposited1 = 0;
                totalWithdrawn0 = 0; totalWithdrawn1 = 0;
                totalFeesEarned0 = 0; totalFeesEarned1 = 0;
                firstDeployTimestamp = now(); lastFeeClaimTimestamp = 0 };
            };
          };
          Map.set(treasuryLPPositions, thash, poolKey, {
            existingPos with
            totalDeposited0 = existingPos.totalDeposited0 + ok.amount0Used;
            totalDeposited1 = existingPos.totalDeposited1 + ok.amount1Used;
          });

          // Note: liquidBalancePerToken was already decremented at transfer time (before addLiquidity call)
          // No further decrement needed here — avoids double-counting

          logTreasuryAdminAction(Principal.fromActor(this), #LPAddLiquidity({ pool = poolKey; details = "minted=" # Nat.toText(ok.liquidityMinted) # " used=" # Nat.toText(ok.amount0Used) # "/" # Nat.toText(ok.amount1Used) }), "LP addition to " # poolKey, true, null);
          logger.info("LP_ADD", "Success: minted=" # Nat.toText(ok.liquidityMinted) # " used=" # Nat.toText(ok.amount0Used) # "/" # Nat.toText(ok.amount1Used), "addLiquidityToPool");
        };
        case (#Err(e)) {
          // Exchange error — tokens refunded via checkReceive (async)
          Map.delete(lpPendingDeposits, thash, depositKey);
          // Move from inFlight to inTransit (refund arriving async)
          // This preserves net-zero: -inFlight + inTransit = 0
          let flight0 = switch (Map.get(lpDepositsInFlight, phash, t0)) { case (?v) v; case null 0 };
          let flight1 = switch (Map.get(lpDepositsInFlight, phash, t1)) { case (?v) v; case null 0 };
          Map.delete(lpDepositsInFlight, phash, t0);
          Map.delete(lpDepositsInFlight, phash, t1);
          if (flight0 > 0) {
            let curT = switch (Map.get(lpTokensInTransit, phash, t0)) { case (?(amt, _)) amt; case null 0 };
            Map.set(lpTokensInTransit, phash, t0, (curT + flight0, now()));
          };
          if (flight1 > 0) {
            let curT = switch (Map.get(lpTokensInTransit, phash, t1)) { case (?(amt, _)) amt; case null 0 };
            Map.set(lpTokensInTransit, phash, t1, (curT + flight1, now()));
          };
          logger.error("LP_ADD", "addLiquidity error: " # debug_show(e) # " — refund tracked as inTransit", "addLiquidityToPool");
        };
      };
    } catch (e) {
      // Canister call failed — lpPendingDeposits survives for recovery
      logger.error("LP_ADD", "addLiquidity exception: " # Error.message(e) # " — pending " # depositKey # " saved", "addLiquidityToPool");
    };
  };

  // Recover pending LP deposits from crashed operations
  private func recoverLPPendingDeposits() : async* () {
    for ((key, deposit) in Map.entries(lpPendingDeposits)) {
      if (now() - deposit.timestamp > 300_000_000_000) { // > 5 min old
        logger.info("LP_RECOVER", "Recovering pending LP deposit: " # key, "recoverLPPendingDeposits");
        try {
          if (deposit.block0 > 0) {
            let tokenType : { #ICP; #ICRC12; #ICRC3 } = if (deposit.token0 == "ryjl3-tyaaa-aaaaa-aaaba-cai") { #ICP } else { #ICRC12 };
            ignore await (with timeout = 65) TACOSwap.recoverStuckTokens(deposit.token0, deposit.block0, tokenType);
          };
          if (deposit.block1 > 0) {
            let tokenType : { #ICP; #ICRC12; #ICRC3 } = if (deposit.token1 == "ryjl3-tyaaa-aaaaa-aaaba-cai") { #ICP } else { #ICRC12 };
            ignore await (with timeout = 65) TACOSwap.recoverStuckTokens(deposit.token1, deposit.block1, tokenType);
          };
          Map.delete(lpPendingDeposits, thash, key);
          logger.info("LP_RECOVER", "Recovery complete for " # key, "recoverLPPendingDeposits");
        } catch (e) {
          logger.error("LP_RECOVER", "Recovery failed for " # key # ": " # Error.message(e), "recoverLPPendingDeposits");
        };
      };
    };
  };

  // Claim fees from all LP positions (called by 6h timer)
  private func claimAllLPFees() : async () {
    if (not lpConfig.enabled) return;
    for ((poolKey, pos) in Map.entries(treasuryLPPositions)) {
      let t0Text = Principal.toText(pos.token0Principal);
      let t1Text = Principal.toText(pos.token1Principal);
      try {
        let result = await (with timeout = 65) TACOSwap.doClaimLPFees(t0Text, t1Text);
        switch (result) {
          case (#Ok(ok)) {
            Map.set(treasuryLPPositions, thash, poolKey, {
              pos with
              totalFeesEarned0 = pos.totalFeesEarned0 + ok.fees0;
              totalFeesEarned1 = pos.totalFeesEarned1 + ok.fees1;
              lastFeeClaimTimestamp = now();
            });
            if (ok.fees0 > 0 or ok.fees1 > 0) {
              logger.info("LP_FEES", "Claimed from " # poolKey # ": f0=" # Nat.toText(ok.fees0) # " f1=" # Nat.toText(ok.fees1) # " t0=" # Nat.toText(ok.transferred0) # " t1=" # Nat.toText(ok.transferred1), "claimAllLPFees");
            };
          };
          case (#Err(e)) {
            logger.warn("LP_FEES", "Claim failed for " # poolKey # ": " # debug_show(e), "claimAllLPFees");
          };
        };
      } catch (e) {
        logger.error("LP_FEES", "Claim exception for " # poolKey # ": " # Error.message(e), "claimAllLPFees");
      };
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
                        // Guard against division by zero
                        if (totalTargetBasisPoints > 0) {
                            (target * 10000) / totalTargetBasisPoints
                        } else { 0 }
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
    // Exclude tokens where the allocation difference is smaller than minAllocationDiffBasisPoints (e.g., 15bp = 0.15%)
    let minDiffBasisPoints = minAllocationDiffBasisPoints; // Use standalone stable var

    let tradePairs = Vector.new<(Principal, Int, Nat)>();
    var excludedDueToMinDiff = 0;

    for (alloc in Vector.vals(allocations)) {
        let details = Map.get(tokenDetailsMap, phash, alloc.token);
        switch details {
            case (?d) {
                if (not isTokenPausedFromTrading(alloc.token) and alloc.diffBasisPoints != 0) {
                    // Check if the allocation difference is significant enough to warrant a trade
                    if (Int.abs(alloc.diffBasisPoints) > minDiffBasisPoints) {
                        Vector.add(tradePairs, (alloc.token, alloc.diffBasisPoints, alloc.valueInICP));
                    } else {
                        excludedDueToMinDiff += 1;
                        // VERBOSE LOGGING: Token excluded due to small allocation difference
                        logger.info("ALLOCATION_ANALYSIS",
                          "Token EXCLUDED (too close to target) - " # d.tokenSymbol #
                          ": Diff=" # Int.toText(Int.abs(alloc.diffBasisPoints)) # "bp" #
                          " Min_diff_threshold=" # Nat.toText(minDiffBasisPoints) # "bp" #
                          " Reason=Below_min_allocation_diff",
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
      " Excluded_too_close=" # Nat.toText(excludedDueToMinDiff) #
      " Min_diff_threshold=" # Nat.toText(minDiffBasisPoints) # "bp" #
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
      // Skip tokens that are paused or have active pending burns (reserved for NACHOS vault payouts)
      let hasPendingBurn = getPendingBurn(token) > 0;
      if (not isTokenPausedFromTrading(token) and not hasPendingBurn) {
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
  // Returns (tradeSize, isExactTargeting)
  // isExactTargeting = true means we should apply slippage adjustment after getting quote
  // isExactTargeting = false means we're using random sizing (don't adjust)
  private func calculateExactTargetTradeSize(
    sellToken: Principal,
    buyToken: Principal,
    totalPortfolioValueICP: Nat,
    sellTokenDiffBasisPoints: Nat,
    buyTokenDiffBasisPoints: Nat
  ) : (Nat, Bool) {

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
          return (calculateTradeSizeMinMax(), false);
        };
      };

      // Guard against division by zero for priceInICP
      if (sellTokenDetails.priceInICP == 0) {
        Debug.print("Warning: Sell token priceInICP is 0, using random size");
        return (calculateTradeSizeMinMax(), false);
      };

      let exactTradeSize = (excessValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP;

      // Only check max bound - 15bp filter handles trivial trades
      // If trade exceeds max, use random sizing to confuse arb bots
      let tradeSizeICP = (exactTradeSize * sellTokenDetails.priceInICP) / (10 ** sellTokenDetails.tokenDecimals);
      if (tradeSizeICP > rebalanceConfig.maxTradeValueICP) {
        Debug.print("Exact trade size too large, using random trade size");
        (calculateTradeSizeMinMax(), false)  // Random, not exact targeting
      } else {
        Debug.print("Using exact targeting with slippage adjustment");
        (exactTradeSize, true)  // Exact targeting, will apply slippage adjustment
      }

    } else {
      // Calculate exact amount to buy to reach buy token's target
      // buyTokenDiffBasisPoints is positive (underweight)
      let deficitValueICP : Nat = (buyTokenDiffBasisPoints * totalPortfolioValueICP) / 10000;

      let sellTokenDetails = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details };
        case null {
          Debug.print("Warning: Sell token details not found for exact targeting, using random size");
          return (calculateTradeSizeMinMax(), false);
        };
      };

      // Guard against division by zero for priceInICP
      if (sellTokenDetails.priceInICP == 0) {
        Debug.print("Warning: Sell token priceInICP is 0, using random size");
        return (calculateTradeSizeMinMax(), false);
      };

      let exactTradeSize : Nat = (deficitValueICP * (10 ** sellTokenDetails.tokenDecimals)) / sellTokenDetails.priceInICP;

      // Only check max bound - 15bp filter handles trivial trades
      // If trade exceeds max, use random sizing to confuse arb bots
      let tradeSizeICP = (exactTradeSize * sellTokenDetails.priceInICP) / (10 ** sellTokenDetails.tokenDecimals);
      if (tradeSizeICP > rebalanceConfig.maxTradeValueICP) {
        Debug.print("Exact trade size too large, using random trade size");
        (calculateTradeSizeMinMax(), false)  // Random, not exact targeting
      } else {
        Debug.print("Using exact targeting with slippage adjustment");
        (exactTradeSize, true)  // Exact targeting, will apply slippage adjustment
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
  ) : async* Result.Result<ExecutionPlan, FindExecutionError> {
    try {
      // Get token symbols for exchange APIs
      let sellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenSymbol };
        case null { return #err({ reason = "Token details not found for sell token"; kongQuotes = []; icpQuotes = [] }) };
      };
      let buySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
        case (?details) { details.tokenSymbol };
        case null { return #err({ reason = "Token details not found for buy token"; kongQuotes = []; icpQuotes = [] }) };
      };
      let sellDecimals : Nat = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenDecimals };
        case null { 8 };
      };
      let buyDecimals : Nat = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
        case (?details) { details.tokenDecimals };
        case null { 8 };
      };

      // Check if Kong/TACO should be skipped for this pair (all quotes invalid last time)
      let skipKong = shouldSkipExchangePair("K", sellToken, buyToken);
      let skipTaco = shouldSkipExchangePair("T", sellToken, buyToken);

      // Get transfer fee for sell token (needed for ICPSwap quote adjustment)
      // ICPSwap executes swaps with (amountIn - fee), so quotes must reflect this
      let sellTokenFee = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenTransferFee };
        case null { 0 };
      };

      // Skip trade if transfer fee > 5% of amount (would waste cycles on doomed trades)
      // This triggers ICP fallback which may succeed via KongSwap (handles fees internally)
      if (amountIn > 0 and sellTokenFee > 0) {
        let feeRatioBP = (sellTokenFee * 10000) / amountIn;
        if (feeRatioBP > 500) {  // 5%
          logger.warn("QUOTE_SKIP",
            "Transfer fee too high relative to trade - Pair=" # sellSymbol # "/" # buySymbol #
            " Fee=" # Nat.toText(sellTokenFee) #
            " Amount=" # Nat.toText(amountIn) #
            " Ratio=" # Nat.toText(feeRatioBP) # "bp (>5%)",
            "findBestExecution"
          );
          return #err({
            reason = "Transfer fee (" # Nat.toText(feeRatioBP) # "bp) exceeds 5% of trade amount";
            kongQuotes = [];
            icpQuotes = [];
          });
        };
      };

      logger.info("EXCHANGE_COMPARISON",
        "Starting exchange comparison with 10 quotes (20% increments) - Pair=" # sellSymbol # "/" # buySymbol #
        " Amount_in=" # Nat.toText(amountIn) # " (raw)" #
        " Amount_formatted=" # Float.toText(Float.fromInt(amountIn) / Float.fromInt(10 ** sellDecimals)) #
        " Max_slippage=" # Nat.toText(rebalanceConfig.maxSlippageBasisPoints) # "bp",
        "findBestExecution"
      );

      // Get ICPSwap pool data first (needed for quote args)
      let icpPoolData = Map.get(ICPswapPools, hashpp, (sellToken, buyToken));
      let zeroForOne = switch (icpPoolData) {
        case (?poolData) { sellToken == Principal.fromText(poolData.token0.address) };
        case null { true };
      };

      // ========================================
      // PARALLEL QUOTE CALLS (20 quotes total)
      // Get 10%, 20%, 30%, ..., 100% quotes from both exchanges
      // Query calls are FREE and run in parallel
      // ========================================

      // Calculate amounts for 10% increments (indices 0-9 = 10%, 20%, ..., 100%)
      // Kong uses full amounts (handles fee internally via pay_tx_id)
      let kongAmounts : [Nat] = [
        amountIn * 1 / 10,    // 10%  - idx 0
        amountIn * 2 / 10,    // 20%  - idx 1
        amountIn * 3 / 10,    // 30%  - idx 2
        amountIn * 4 / 10,    // 40%  - idx 3
        amountIn * 5 / 10,    // 50%  - idx 4
        amountIn * 6 / 10,    // 60%  - idx 5
        amountIn * 7 / 10,    // 70%  - idx 6
        amountIn * 8 / 10,    // 80%  - idx 7
        amountIn * 9 / 10,    // 90%  - idx 8
        amountIn,             // 100% - idx 9
      ];

      // ICPSwap uses fee-adjusted amounts (fee deducted before swap in pool)
      // This ensures quotes match what will actually be swapped
      let icpAmounts : [Nat] = [
        if (amountIn * 1 / 10 > sellTokenFee) { amountIn * 1 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 2 / 10 > sellTokenFee) { amountIn * 2 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 3 / 10 > sellTokenFee) { amountIn * 3 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 4 / 10 > sellTokenFee) { amountIn * 4 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 5 / 10 > sellTokenFee) { amountIn * 5 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 6 / 10 > sellTokenFee) { amountIn * 6 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 7 / 10 > sellTokenFee) { amountIn * 7 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 8 / 10 > sellTokenFee) { amountIn * 8 / 10 - sellTokenFee } else { 0 },
        if (amountIn * 9 / 10 > sellTokenFee) { amountIn * 9 / 10 - sellTokenFee } else { 0 },
        if (amountIn > sellTokenFee) { amountIn - sellTokenFee } else { 0 },
      ];

      // Start all 20 quote requests in parallel (no await yet)
      // Kong quotes for 10 percentages (uses full amounts - Kong handles fees internally)
      let kongFuture0 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[0], sellDecimals, buyDecimals) };
      let kongFuture1 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[1], sellDecimals, buyDecimals) };
      let kongFuture2 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[2], sellDecimals, buyDecimals) };
      let kongFuture3 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[3], sellDecimals, buyDecimals) };
      let kongFuture4 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[4], sellDecimals, buyDecimals) };
      let kongFuture5 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[5], sellDecimals, buyDecimals) };
      let kongFuture6 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[6], sellDecimals, buyDecimals) };
      let kongFuture7 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[7], sellDecimals, buyDecimals) };
      let kongFuture8 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[8], sellDecimals, buyDecimals) };
      let kongFuture9 = if (skipKong) { async { #err("KongSwap skipped: pair quotes invalid") : Result.Result<swaptypes.SwapAmountsReply, Text> } } else { (with timeout = 65) KongSwap.getQuote(sellSymbol, buySymbol, kongAmounts[9], sellDecimals, buyDecimals) };
      // ICP quotes for 10 percentages (uses fee-adjusted amounts - ICPSwap swaps amountIn-fee)
      var skipICPswap=false;
      let (icpFuture0, icpFuture1, icpFuture2, icpFuture3, icpFuture4, icpFuture5, icpFuture6, icpFuture7, icpFuture8, icpFuture9) = switch (icpPoolData) {
        case (?poolData) {
          (
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[0]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[1]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[2]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[3]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[4]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[5]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[6]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[7]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[8]; amountOutMinimum = 0; zeroForOne = zeroForOne }),
            (with timeout = 65) ICPSwap.getQuote({ poolId = poolData.canisterId; amountIn = icpAmounts[9]; amountOutMinimum = 0; zeroForOne = zeroForOne })
          )
        };
        case null {
          var skipICPswap=true;
          // No ICPSwap pool - create a dummy async that returns error immediately (no actual network call)
          let errFuture = async { #err("No ICPSwap pool available for this pair") : Result.Result<swaptypes.ICPSwapQuoteResult, Text> };
          (errFuture, errFuture, errFuture, errFuture, errFuture, errFuture, errFuture, errFuture, errFuture, errFuture)
        };
      };

      // TACO Exchange quotes - 10 quotes at 10%, 20%, 30%, ..., 100% (matching Kong/ICPSwap granularity)
      let sellTokenText = Principal.toText(sellToken);
      let buyTokenText = Principal.toText(buyToken);
      // TACO: batch quote — 10 quotes in 1 inter-canister call (instead of 10 separate calls)
      let tacoBatchFuture = if (skipTaco) {
        async { #err("TACO skipped: pair quotes invalid") : Result.Result<[swaptypes.TACOQuoteReply], Text> }
      } else {
        (with timeout = 65) TACOSwap.getQuoteWithRouteBatch(
          Array.tabulate<{ tokenA : Text; tokenB : Text; amountIn : Nat }>(10, func(i) {
            { tokenA = sellTokenText; tokenB = buyTokenText; amountIn = amountIn * (i + 1) / 10 }
          }),
          sellDecimals,
          buyDecimals,
        )
      };

      // Await all 25 quotes (must await each individually in Motoko)
      let kongResult0 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture0 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult1 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture1 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult2 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture2 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult3 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture3 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult4 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture4 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult5 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture5 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult6 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture6 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult7 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture7 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult8 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture8 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let kongResult9 = if skipKong { #err("KongSwap skipped") } else { try { await kongFuture9 } catch (e) { #err("KongSwap quote exception: " # Error.message(e)) } };
      let icpResult0 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture0 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult1 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture1 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult2 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture2 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult3 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture3 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult4 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture4 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult5 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture5 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult6 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture6 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult7 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture7 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult8 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture8 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      let icpResult9 = if skipICPswap{#err("ICPSwap quote exception: " # "No ICPswap pool")} else{try { await icpFuture9 } catch (e) { #err("ICPSwap quote exception: " # Error.message(e)) }};
      // Await TACO batch (1 call instead of 10)
      let tacoBatchResult = if skipTaco { #err("TACO skipped") } else { try { await tacoBatchFuture } catch (e) { #err("TACO batch quote exception: " # Error.message(e)) } };
      // Unpack batch result into individual results for downstream compatibility
      let (tacoResult0, tacoResult1, tacoResult2, tacoResult3, tacoResult4, tacoResult5, tacoResult6, tacoResult7, tacoResult8, tacoResult9) : (
        Result.Result<swaptypes.TACOQuoteReply, Text>, Result.Result<swaptypes.TACOQuoteReply, Text>,
        Result.Result<swaptypes.TACOQuoteReply, Text>, Result.Result<swaptypes.TACOQuoteReply, Text>,
        Result.Result<swaptypes.TACOQuoteReply, Text>, Result.Result<swaptypes.TACOQuoteReply, Text>,
        Result.Result<swaptypes.TACOQuoteReply, Text>, Result.Result<swaptypes.TACOQuoteReply, Text>,
        Result.Result<swaptypes.TACOQuoteReply, Text>, Result.Result<swaptypes.TACOQuoteReply, Text>,
      ) = switch (tacoBatchResult) {
        case (#ok(quotes)) {
          if (quotes.size() >= 10) {
            (#ok(quotes[0]), #ok(quotes[1]), #ok(quotes[2]), #ok(quotes[3]), #ok(quotes[4]),
             #ok(quotes[5]), #ok(quotes[6]), #ok(quotes[7]), #ok(quotes[8]), #ok(quotes[9]));
          } else {
            let e = #err("TACO batch returned " # Nat.toText(quotes.size()) # " quotes, expected 10") : Result.Result<swaptypes.TACOQuoteReply, Text>;
            (e, e, e, e, e, e, e, e, e, e);
          };
        };
        case (#err(msg)) {
          let e = #err(msg) : Result.Result<swaptypes.TACOQuoteReply, Text>;
          (e, e, e, e, e, e, e, e, e, e);
        };
      };

      let kongResults = [kongResult0, kongResult1, kongResult2, kongResult3, kongResult4, kongResult5, kongResult6, kongResult7, kongResult8, kongResult9];
      let icpResults = [icpResult0, icpResult1, icpResult2, icpResult3, icpResult4, icpResult5, icpResult6, icpResult7, icpResult8, icpResult9];
      let tacoResults = [tacoResult0, tacoResult1, tacoResult2, tacoResult3, tacoResult4, tacoResult5, tacoResult6, tacoResult7, tacoResult8, tacoResult9];

      // Log raw quote results for debugging
      logger.info("QUOTE_DEBUG", "Raw Kong 100% result: " # debug_show(kongResult9), "findBestExecution");
      logger.info("QUOTE_DEBUG", "Raw ICPSwap 100% result: " # debug_show(icpResult9), "findBestExecution");

      // ========================================
      // PROCESS QUOTE RESULTS
      // Extract output and slippage, validate against threshold
      // ========================================

      let maxSlippagePct = Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0;

      // Get token prices and decimals for dust output validation
      let sellTokenDetails = Map.get(tokenDetailsMap, phash, sellToken);
      let buyTokenDetails = Map.get(tokenDetailsMap, phash, buyToken);
      let sellPriceICP : Nat = switch (sellTokenDetails) { case (?d) { d.priceInICP }; case null { 0 } };
      let buyPriceICP : Nat = switch (buyTokenDetails) { case (?d) { d.priceInICP }; case null { 0 } };
      // Helper to check for dust output: output < 1% of expected at spot price
      func isDustOutput(amountIn : Nat, amountOut : Nat) : Bool {
        if (sellPriceICP == 0 or buyPriceICP == 0 or amountOut == 0) { return false };
        // Calculate expected output at spot price
        let sellValueE8s = (amountIn * sellPriceICP) / (10 ** sellDecimals);
        let expectedOut = (sellValueE8s * (10 ** buyDecimals)) / buyPriceICP;
        let minExpected = if (expectedOut / 100 > 100) { expectedOut / 100 } else { 100 };
        amountOut < minExpected
      };

      // Helper to extract KongSwap quote (returns lowercase #ok/#err from Result.Result)
      func extractKong(result : Result.Result<swaptypes.SwapAmountsReply, Text>, amountIn : Nat) : QuoteData {
        switch (result) {
          case (#ok(r)) {
            let slipFloat = r.slippage * 100.0;
            let slipBP = if (isFiniteFloat(slipFloat)) { Int.abs(Float.toInt(slipFloat)) } else { 10000 };
            // Dust output validation: reject if output < 1% of expected
            let isDust = isDustOutput(amountIn, r.receive_amount);
            let valid = r.slippage <= maxSlippagePct and r.receive_amount > 0 and not isDust;
            { out = r.receive_amount; slipBP = slipBP; valid = valid }
          };
          case (#err(_)) { { out = 0; slipBP = 10000; valid = false } };
        }
      };

      // Helper to extract ICPSwap quote (returns lowercase #ok/#err from Result.Result)
      func extractIcp(result : Result.Result<swaptypes.ICPSwapQuoteResult, Text>, amountIn : Nat) : QuoteData {
        switch (result) {
          case (#ok(r)) {
            let slipFloat = r.slippage * 100.0;
            let slipBP = if (isFiniteFloat(slipFloat)) { Int.abs(Float.toInt(slipFloat)) } else { 10000 };
            // Dust output validation: reject if output < 1% of expected
            let isDust = isDustOutput(amountIn, r.amountOut);
            let valid = r.slippage <= maxSlippagePct and r.amountOut > 0 and not isDust;
            { out = r.amountOut; slipBP = slipBP; valid = valid }
          };
          case (#err(_)) { { out = 0; slipBP = 10000; valid = false } };
        }
      };

      // Extract KongSwap quotes (indices 0-9 = 10%, 20%, ..., 100%)
      let kong = Array.tabulate<QuoteData>(10, func(i) : QuoteData {
        extractKong(kongResults[i], kongAmounts[i])
      });

      // Extract ICPSwap quotes (indices 0-9 = 10%, 20%, ..., 100%)
      let icp = Array.tabulate<QuoteData>(10, func(i) : QuoteData {
        extractIcp(icpResults[i], icpAmounts[i])
      });

      // Helper to extract TACO quote — same as extractKong but rejects slippage == 0 (invalid/no real liquidity)
      func extractTaco(result : Result.Result<swaptypes.TACOQuoteReply, Text>, amountIn : Nat) : QuoteData {
        switch (result) {
          case (#ok(r)) {
            let slipFloat = r.slippage * 100.0;
            let slipBP = if (isFiniteFloat(slipFloat)) { Int.abs(Float.toInt(slipFloat)) } else { 10000 };
            let isDust = isDustOutput(amountIn, r.receive_amount);
            // TACO: slippage == 0 is valid (orderbook-only fills have 0 priceImpact)
            // Invalid quotes return #err from taco_swap.mo (expectedBuyAmount == 0 → error)
            let valid = r.slippage <= maxSlippagePct and r.receive_amount > 0 and not isDust;
            { out = r.receive_amount; slipBP = slipBP; valid = valid }
          };
          case (#err(_)) { { out = 0; slipBP = 10000; valid = false } };
        }
      };

      // TACO quote data with route info for multi-route detection
      type TACOQuoteData = { out : Nat; slipBP : Nat; valid : Bool; route : [{ tokenIn : Text; tokenOut : Text }] };

      func extractTacoWithRoute(result : Result.Result<swaptypes.TACOQuoteReply, Text>, amountIn : Nat) : TACOQuoteData {
        switch (result) {
          case (#ok(r)) {
            let slipFloat = r.slippage * 100.0;
            let slipBP = if (isFiniteFloat(slipFloat)) { Int.abs(Float.toInt(slipFloat)) } else { 10000 };
            let isDust = isDustOutput(amountIn, r.receive_amount);
            let valid = r.slippage <= maxSlippagePct and r.receive_amount > 0 and not isDust;
            { out = r.receive_amount; slipBP = slipBP; valid = valid; route = r.route }
          };
          case (#err(_)) { { out = 0; slipBP = 10000; valid = false; route = [] } };
        }
      };

      // Extract TACO quotes with route info
      let tacoWithRoutes = Array.tabulate<TACOQuoteData>(10, func(i) {
        extractTacoWithRoute(tacoResults[i], amountIn * (i + 1) / 10)
      });

      // Build regular QuoteData array for scenario evaluation (compatible with existing code)
      let taco = Array.tabulate<QuoteData>(10, func(i) : QuoteData {
        { out = tacoWithRoutes[i].out; slipBP = tacoWithRoutes[i].slipBP; valid = tacoWithRoutes[i].valid }
      });

      // Detect if TACO quotes use different routes at different amounts
      // If so, we should use swapSplitRoutes instead of swapMultiHop
      // Collect distinct routes from TACO quotes
      var tacoDistinctRoutes = Map.new<Text, [{ tokenIn : Text; tokenOut : Text }]>();
      for (i in Iter.range(0, 9)) {
        if (tacoWithRoutes[i].valid and tacoWithRoutes[i].route.size() > 0) {
          let routeKey = Array.foldLeft<{ tokenIn : Text; tokenOut : Text }, Text>(
            tacoWithRoutes[i].route, "",
            func(acc, h) { acc # h.tokenIn # ">" # h.tokenOut # ";" }
          );
          if (not Map.has(tacoDistinctRoutes, thash, routeKey)) {
            Map.set(tacoDistinctRoutes, thash, routeKey, tacoWithRoutes[i].route);
          };
        };
      };

      // Filter routes for pool independence — routes sharing a pool must NOT be split
      // Two routes share a pool if any hop pair has same tokens (in either direction)
      func hopsSharePool(
        routeA : [{ tokenIn : Text; tokenOut : Text }],
        routeB : [{ tokenIn : Text; tokenOut : Text }],
      ) : Bool {
        for (hopA in routeA.vals()) {
          for (hopB in routeB.vals()) {
            if ((hopA.tokenIn == hopB.tokenIn and hopA.tokenOut == hopB.tokenOut) or
                (hopA.tokenIn == hopB.tokenOut and hopA.tokenOut == hopB.tokenIn)) {
              return true;
            };
          };
        };
        false
      };

      // Greedy: keep routes in discovery order, skip any that overlap with already-kept routes
      let independentRoutes = Vector.new<{ route : [{ tokenIn : Text; tokenOut : Text }]; weight : Nat }>();
      for ((_, route) in Map.entries(tacoDistinctRoutes)) {
        var hasOverlap = false;
        for (kept in Vector.vals(independentRoutes)) {
          if (hopsSharePool(route, kept.route)) {
            hasOverlap := true;
          };
        };
        if (not hasOverlap and Vector.size(independentRoutes) < 3) {
          Vector.add(independentRoutes, { route = route; weight = 1 });
        };
      };

      let tacoMultiRoute = Vector.size(independentRoutes) > 1;
      if (tacoMultiRoute) {
        Debug.print("TACO multi-route: " # Nat.toText(Vector.size(independentRoutes)) # " independent routes (from " # Nat.toText(Map.size(tacoDistinctRoutes)) # " distinct)");
      };

      // Record exchange-pair skip if ALL quotes are invalid (saves cycles next time)
      if (not skipKong) {
        var allKongInvalid = true;
        for (q in kong.vals()) { if (q.valid) { allKongInvalid := false } };
        if (allKongInvalid) { addExchangePairSkip("K", sellToken, buyToken) };
      };

      if (not skipTaco) {
        var allTacoInvalid = true;
        for (q in taco.vals()) { if (q.valid) { allTacoInvalid := false } };
        if (allTacoInvalid) { addExchangePairSkip("T", sellToken, buyToken) };
      };

      // Log quote results summary
      logger.info("EXCHANGE_COMPARISON",
        "Quotes received - Kong_100%=" # Nat.toText(kong[9].out) # " (valid=" # debug_show(kong[9].valid) # ")" #
        " ICP_100%=" # Nat.toText(icp[9].out) # " (valid=" # debug_show(icp[9].valid) # ")" #
        " TACO_100%=" # Nat.toText(taco[9].out) # " (valid=" # debug_show(taco[9].valid) # ")",
        "findBestExecution"
      );

      // Store multi-route info for execution (only pool-independent routes)
      lastTacoMultiRoute := tacoMultiRoute;
      lastTacoRouteLegs := if (tacoMultiRoute) {
        Vector.toArray(independentRoutes)
      } else { [] };

      // ========================================
      // CALCULATE ALL SCENARIOS
      // Singles (100%), Full splits (sum to 100%), and Partials (sum < 100%)
      // Full splits: pick scenario with maximum total output
      // Partials: used only when no full scenarios - pick by minimum combined slippage
      // ========================================

      // Constants for 10 quotes
      let NUM_QUOTES : Nat = 10;
      let STEP_BP : Nat = 1000;  // 10% per step
      let MIN_PARTIAL_TOTAL_BP : Nat = 4000;  // 40% minimum for partials

      type Scenario = { name : Text; kongPct : Nat; icpPct : Nat; tacoPct : Nat; totalOut : Nat; kongSlipBP : Nat; icpSlipBP : Nat; tacoSlipBP : Nat; kongIdx : Nat; icpIdx : Nat; tacoIdx : Nat };

      var bestScenario : ?Scenario = null;
      var secondBestScenario : ?Scenario = null;
      var partialScenarios = Vector.new<Scenario>();

      // Helper to update best/second-best
      func updateBest(scenario : Scenario) {
        switch (bestScenario) {
          case (null) { bestScenario := ?scenario };
          case (?best) {
            if (scenario.totalOut > best.totalOut) {
              secondBestScenario := ?best;
              bestScenario := ?scenario;
            } else {
              switch (secondBestScenario) {
                case (null) { secondBestScenario := ?scenario };
                case (?second) { if (scenario.totalOut > second.totalOut) { secondBestScenario := ?scenario } };
              };
            };
          };
        };
      };

      // Scenario 1: Single KongSwap (100%) - index 9
      if (kong[9].valid) {
        updateBest({
          name = "SINGLE_KONG";
          kongPct = 10000; icpPct = 0; tacoPct = 0;
          totalOut = kong[9].out;
          kongSlipBP = kong[9].slipBP; icpSlipBP = 0; tacoSlipBP = 0;
          kongIdx = 9; icpIdx = 9; tacoIdx = 9;
        });
      };

      // Scenario 2: Single ICPSwap (100%) - index 9
      if (icp[9].valid) {
        updateBest({
          name = "SINGLE_ICP";
          kongPct = 0; icpPct = 10000; tacoPct = 0;
          totalOut = icp[9].out;
          kongSlipBP = 0; icpSlipBP = icp[9].slipBP; tacoSlipBP = 0;
          kongIdx = 9; icpIdx = 9; tacoIdx = 9;
        });
      };

      // Scenario 3: Single TACO (100%) - index 9
      if (taco[9].valid) {
        updateBest({
          name = "SINGLE_TACO";
          kongPct = 0; icpPct = 0; tacoPct = 10000;
          totalOut = taco[9].out;
          kongSlipBP = 0; icpSlipBP = 0; tacoSlipBP = taco[9].slipBP;
          kongIdx = 9; icpIdx = 9; tacoIdx = 9;
        });
      };

      // All combinations: Kong(10% steps) x ICP(10% steps) x TACO(10% steps)
      // All three: 10 quotes at indices 0-9 = 10%-100% (STEP_BP = 1000)
      let TACO_STEP_BP : Nat = 1000; // 10% per step (10 quotes, matching Kong/ICPSwap)

      // 2-way Kong+ICP combinations (original 10x10, no TACO)
      label outerLoop for (kongIdxIter in Iter.range(0, 9)) {
        label innerLoop for (icpIdxIter in Iter.range(0, 9)) {
          let kongPctCalc = (kongIdxIter + 1) * STEP_BP;
          let icpPctCalc = (icpIdxIter + 1) * STEP_BP;
          let totalPctCalc = kongPctCalc + icpPctCalc;

          if (totalPctCalc > 10000) { continue innerLoop };
          if (kongPctCalc == 10000 or icpPctCalc == 10000) { continue innerLoop };

          if (kong[kongIdxIter].valid and icp[icpIdxIter].valid) {
            let totalOutCalc = kong[kongIdxIter].out + icp[icpIdxIter].out;
            let scenario : Scenario = {
              name = (if (totalPctCalc == 10000) { "SPLIT_" } else { "PARTIAL_" }) # Nat.toText(kongPctCalc / 100) # "K_" # Nat.toText(icpPctCalc / 100) # "I";
              kongPct = kongPctCalc; icpPct = icpPctCalc; tacoPct = 0;
              totalOut = totalOutCalc;
              kongSlipBP = kong[kongIdxIter].slipBP; icpSlipBP = icp[icpIdxIter].slipBP; tacoSlipBP = 0;
              kongIdx = kongIdxIter; icpIdx = icpIdxIter; tacoIdx = 9;
            };
            if (totalPctCalc == 10000) { updateBest(scenario) }
            else if (totalPctCalc >= MIN_PARTIAL_TOTAL_BP) { Vector.add(partialScenarios, scenario) };
          };
        };
      };

      // 2-way Kong+TACO combinations
      label kongTacoLoop for (kongIdxIter in Iter.range(0, 9)) {
        label tacoLoop1 for (tacoIdxIter in Iter.range(0, 9)) {
          let kongPctCalc = (kongIdxIter + 1) * STEP_BP;
          let tacoPctCalc = (tacoIdxIter + 1) * TACO_STEP_BP;
          let totalPctCalc = kongPctCalc + tacoPctCalc;

          if (totalPctCalc > 10000) { continue tacoLoop1 };
          if (kongPctCalc == 10000 or tacoPctCalc == 10000) { continue tacoLoop1 };

          if (kong[kongIdxIter].valid and taco[tacoIdxIter].valid) {
            let totalOutCalc = kong[kongIdxIter].out + taco[tacoIdxIter].out;
            let scenario : Scenario = {
              name = (if (totalPctCalc == 10000) { "SPLIT_" } else { "PARTIAL_" }) # Nat.toText(kongPctCalc / 100) # "K_" # Nat.toText(tacoPctCalc / 100) # "T";
              kongPct = kongPctCalc; icpPct = 0; tacoPct = tacoPctCalc;
              totalOut = totalOutCalc;
              kongSlipBP = kong[kongIdxIter].slipBP; icpSlipBP = 0; tacoSlipBP = taco[tacoIdxIter].slipBP;
              kongIdx = kongIdxIter; icpIdx = 9; tacoIdx = tacoIdxIter;
            };
            if (totalPctCalc == 10000) { updateBest(scenario) }
            else if (totalPctCalc >= MIN_PARTIAL_TOTAL_BP) { Vector.add(partialScenarios, scenario) };
          };
        };
      };

      // 2-way ICP+TACO combinations
      label icpTacoLoop for (icpIdxIter in Iter.range(0, 9)) {
        label tacoLoop2 for (tacoIdxIter in Iter.range(0, 9)) {
          let icpPctCalc = (icpIdxIter + 1) * STEP_BP;
          let tacoPctCalc = (tacoIdxIter + 1) * TACO_STEP_BP;
          let totalPctCalc = icpPctCalc + tacoPctCalc;

          if (totalPctCalc > 10000) { continue tacoLoop2 };
          if (icpPctCalc == 10000 or tacoPctCalc == 10000) { continue tacoLoop2 };

          if (icp[icpIdxIter].valid and taco[tacoIdxIter].valid) {
            let totalOutCalc = icp[icpIdxIter].out + taco[tacoIdxIter].out;
            let scenario : Scenario = {
              name = (if (totalPctCalc == 10000) { "SPLIT_" } else { "PARTIAL_" }) # Nat.toText(icpPctCalc / 100) # "I_" # Nat.toText(tacoPctCalc / 100) # "T";
              kongPct = 0; icpPct = icpPctCalc; tacoPct = tacoPctCalc;
              totalOut = totalOutCalc;
              kongSlipBP = 0; icpSlipBP = icp[icpIdxIter].slipBP; tacoSlipBP = taco[tacoIdxIter].slipBP;
              kongIdx = 9; icpIdx = icpIdxIter; tacoIdx = tacoIdxIter;
            };
            if (totalPctCalc == 10000) { updateBest(scenario) }
            else if (totalPctCalc >= MIN_PARTIAL_TOTAL_BP) { Vector.add(partialScenarios, scenario) };
          };
        };
      };

      // 3-way Kong+ICP+TACO combinations
      label kongLoop3 for (kongIdxIter in Iter.range(0, 7)) { // max 80% Kong in 3-way
        label icpLoop3 for (icpIdxIter in Iter.range(0, 7)) { // max 80% ICP in 3-way
          label tacoLoop3 for (tacoIdxIter in Iter.range(0, 7)) { // max 80% TACO in 3-way (matching Kong/ICP)
            let kongPctCalc = (kongIdxIter + 1) * STEP_BP;
            let icpPctCalc = (icpIdxIter + 1) * STEP_BP;
            let tacoPctCalc = (tacoIdxIter + 1) * TACO_STEP_BP;
            let totalPctCalc = kongPctCalc + icpPctCalc + tacoPctCalc;

            if (totalPctCalc > 10000) { continue tacoLoop3 };

            if (kong[kongIdxIter].valid and icp[icpIdxIter].valid and taco[tacoIdxIter].valid) {
              let totalOutCalc = kong[kongIdxIter].out + icp[icpIdxIter].out + taco[tacoIdxIter].out;
              let scenario : Scenario = {
                name = (if (totalPctCalc == 10000) { "SPLIT_" } else { "PARTIAL_" }) # Nat.toText(kongPctCalc / 100) # "K_" # Nat.toText(icpPctCalc / 100) # "I_" # Nat.toText(tacoPctCalc / 100) # "T";
                kongPct = kongPctCalc; icpPct = icpPctCalc; tacoPct = tacoPctCalc;
                totalOut = totalOutCalc;
                kongSlipBP = kong[kongIdxIter].slipBP; icpSlipBP = icp[icpIdxIter].slipBP; tacoSlipBP = taco[tacoIdxIter].slipBP;
                kongIdx = kongIdxIter; icpIdx = icpIdxIter; tacoIdx = tacoIdxIter;
              };
              if (totalPctCalc == 10000) { updateBest(scenario) }
              else if (totalPctCalc >= MIN_PARTIAL_TOTAL_BP) { Vector.add(partialScenarios, scenario) };
            };
          };
        };
      };

      // ========================================
      // BUILD EXECUTION PLAN FROM BEST SCENARIO
      // With slippage interpolation between adjacent scenarios
      // ========================================

      switch (bestScenario) {
        case (null) {
          // No full scenario (single or 100% split) found - TRY PARTIALS
          logger.info("EXCHANGE_COMPARISON",
            "No full execution path - checking " # Nat.toText(Vector.size(partialScenarios)) # " partial scenarios",
            "findBestExecution"
          );

          // GUARD: No partials at all
          if (Vector.size(partialScenarios) == 0) {
            logger.warn("EXCHANGE_COMPARISON",
              "No viable execution path - all quotes invalid or exceed slippage threshold",
              "findBestExecution"
            );
            #err({ reason = "No viable execution path found"; kongQuotes = kong; icpQuotes = icp })
          } else {
            // Calculate trade value in ICP for filtering
            let tradeValueICP : Nat = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
              case (?d) {
                if (d.tokenDecimals > 0) {
                  (amountIn * d.priceInICP) / (10 ** d.tokenDecimals)
                } else { 0 }
              };
              case null { 0 };
            };
            let icpInvolved = sellToken == ICPprincipal or buyToken == ICPprincipal;

            // Helper: combined slippage (NO FLOAT)
            func combinedSlip(p : Scenario) : Nat {
              p.kongSlipBP + p.icpSlipBP + p.tacoSlipBP
            };

            // Helper: partial total percentage
            func totalPctFunc(p : Scenario) : Nat {
              p.kongPct + p.icpPct + p.tacoPct
            };

            // Helper: partial value in ICP (safe division)
            func partialValueICP(p : Scenario) : Nat {
              if (tradeValueICP == 0) { return 0 };
              (tradeValueICP * totalPctFunc(p)) / 10000
            };

            // ============================================
            // STEP A: Filter by BOTH MIN_TRADE_VALUE_ICP AND MIN_PARTIAL_TOTAL_BP
            // ============================================
            let validPartials = Vector.new<Scenario>();
            for (p in Vector.vals(partialScenarios)) {
              let meetsTotalPct = totalPctFunc(p) >= MIN_PARTIAL_TOTAL_BP;
              let meetsValueICP = partialValueICP(p) >= rebalanceConfig.minTradeValueICP;
              if (meetsTotalPct and meetsValueICP) {
                Vector.add(validPartials, p);
              };
            };

            // ============================================
            // STEP B/C: Select best - EXACT PYTHON if/elif/else ORDER
            // ============================================
            var bestPartial : ?Scenario = null;
            var selectedPool = Vector.new<Scenario>();  // Track which pool best came from

            // BRANCH 1: valid_partials has items
            if (Vector.size(validPartials) > 0) {
              // Sort by combined slippage (ascending - lowest first)
              let arr = Array.sort<Scenario>(Vector.toArray(validPartials), func(a : Scenario, b : Scenario) : Order.Order {
                Nat.compare(combinedSlip(a), combinedSlip(b))
              });
              bestPartial := ?arr[0];
              // Copy to selectedPool
              for (p in arr.vals()) { Vector.add(selectedPool, p) };
            }
            // BRANCH 2: ICP involved AND partial_candidates exists
            else if (icpInvolved and Vector.size(partialScenarios) > 0) {
              // ICP EXCEPTION: Filter by MIN_PARTIAL_TOTAL_BP only (skip MIN_TRADE_VALUE_ICP)
              let icpValid = Vector.new<Scenario>();
              for (p in Vector.vals(partialScenarios)) {
                if (totalPctFunc(p) >= MIN_PARTIAL_TOTAL_BP) {
                  Vector.add(icpValid, p);
                };
              };

              // NESTED: if icp_valid has items
              if (Vector.size(icpValid) > 0) {
                let arr = Array.sort<Scenario>(Vector.toArray(icpValid), func(a : Scenario, b : Scenario) : Order.Order {
                  Nat.compare(combinedSlip(a), combinedSlip(b))
                });
                bestPartial := ?arr[0];
                for (p in arr.vals()) { Vector.add(selectedPool, p) };
              }
              // NESTED else: best = None (falls through)
            };
            // BRANCH 3: else - best stays None

            // ============================================
            // STEP D: Process best (or return error)
            // ============================================
            switch (bestPartial) {
              case (null) {
                // No valid partials - return error with quotes for REDUCED fallback
                logger.warn("EXCHANGE_COMPARISON",
                  "No viable partial execution path found",
                  "findBestExecution"
                );
                #err({ reason = "No viable partial execution path"; kongQuotes = kong; icpQuotes = icp })
              };
              case (?partialBest) {
                // ============================================
                // INTERPOLATION CHECK FOR PARTIALS
                // ============================================
                var finalKongPctP = partialBest.kongPct;
                var finalIcpPctP = partialBest.icpPct;
                var finalKongSlipBPP = partialBest.kongSlipBP;
                var finalIcpSlipBPP = partialBest.icpSlipBP;
                var interpolatedP = false;

                // Get "others" from selectedPool (excluding best)
                let others = Vector.new<Scenario>();
                for (p in Vector.vals(selectedPool)) {
                  // Compare by identity (name is unique)
                  if (p.name != partialBest.name) {
                    Vector.add(others, p);
                  };
                };

                // INTERPOLATION: if others is not empty
                if (Vector.size(others) > 0) {
                  // Sort others by combined slippage
                  let othersArr = Array.sort<Scenario>(Vector.toArray(others), func(a : Scenario, b : Scenario) : Order.Order {
                    Nat.compare(combinedSlip(a), combinedSlip(b))
                  });
                  let second = othersArr[0];

                  // ADJACENCY CHECK: BOTH kong and icp must differ by STEP_BP
                  let kongDiff = if (partialBest.kongPct > second.kongPct) {
                    partialBest.kongPct - second.kongPct
                  } else {
                    second.kongPct - partialBest.kongPct
                  };
                  let icpDiff = if (partialBest.icpPct > second.icpPct) {
                    partialBest.icpPct - second.icpPct
                  } else {
                    second.icpPct - partialBest.icpPct
                  };

                  // BOTH must match step size for partials
                  if (kongDiff == STEP_BP and icpDiff == STEP_BP) {
                    // Calculate average slippages (integer, no float)
                    let avgKongSlipBP = (partialBest.kongSlipBP + second.kongSlipBP) / 2;
                    let avgIcpSlipBP = (partialBest.icpSlipBP + second.icpSlipBP) / 2;
                    let totalSlipBPP = avgKongSlipBP + avgIcpSlipBP;

                    // GUARD: totalSlip > 0
                    if (totalSlipBPP > 0) {
                      // kongRatio = avgIcpSlip / totalSlip
                      // Scale by 10000 for precision: kongRatioBP = (avgIcpSlipBP * 10000) / totalSlipBP
                      let kongRatioBP = (avgIcpSlipBP * 10000) / totalSlipBPP;

                      // Kong percentage interpolation
                      let lowKong = if (partialBest.kongPct < second.kongPct) { partialBest.kongPct } else { second.kongPct };
                      let highKong = if (partialBest.kongPct > second.kongPct) { partialBest.kongPct } else { second.kongPct };
                      let kongRange = highKong - lowKong;  // Safe: highKong >= lowKong by definition
                      finalKongPctP := lowKong + (kongRatioBP * kongRange) / 10000;

                      // ICP percentage: INVERSE direction (highIcp - ratio * range)
                      let lowIcp = if (partialBest.icpPct < second.icpPct) { partialBest.icpPct } else { second.icpPct };
                      let highIcp = if (partialBest.icpPct > second.icpPct) { partialBest.icpPct } else { second.icpPct };
                      let icpRange = highIcp - lowIcp;
                      // SAFE subtraction: highIcp >= (kongRatioBP * icpRange) / 10000 always
                      // because kongRatioBP <= 10000 and icpRange <= STEP_BP
                      finalIcpPctP := highIcp - (kongRatioBP * icpRange) / 10000;

                      // Slippage interpolation (integer)
                      let lowKongSlip = if (partialBest.kongPct < second.kongPct) { partialBest.kongSlipBP } else { second.kongSlipBP };
                      let highKongSlip = if (partialBest.kongPct > second.kongPct) { partialBest.kongSlipBP } else { second.kongSlipBP };
                      let lowIcpSlip = if (partialBest.icpPct < second.icpPct) { partialBest.icpSlipBP } else { second.icpSlipBP };
                      let highIcpSlip = if (partialBest.icpPct > second.icpPct) { partialBest.icpSlipBP } else { second.icpSlipBP };

                      // Safe: highSlip >= lowSlip by selection
                      let kongSlipRange = if (highKongSlip > lowKongSlip) { highKongSlip - lowKongSlip } else { 0 };
                      let icpSlipRange = if (highIcpSlip > lowIcpSlip) { highIcpSlip - lowIcpSlip } else { 0 };

                      finalKongSlipBPP := lowKongSlip + (kongRatioBP * kongSlipRange) / 10000;
                      finalIcpSlipBPP := lowIcpSlip + (kongRatioBP * icpSlipRange) / 10000;

                      interpolatedP := true;
                    };
                  };
                };

                // ============================================
                // BUILD PARTIAL EXECUTION PLAN
                // ============================================
                let finalTotalPct = finalKongPctP + finalIcpPctP;
                let kongAmountP = (amountIn * finalKongPctP) / 10000;
                let icpAmountP = (amountIn * finalIcpPctP) / 10000;

                // Find closest quote index for interpolated percentages
                // closestIdx: find i where (i+1)*STEP_BP is closest to pct
                func closestIdx(pct : Nat) : Nat {
                  var bestIdx : Nat = 0;
                  var bestDiff : Nat = if (pct > STEP_BP) { pct - STEP_BP } else { STEP_BP - pct };
                  for (i in Iter.range(1, NUM_QUOTES - 1)) {
                    let stepPct = (i + 1) * STEP_BP;
                    let diff = if (pct > stepPct) { pct - stepPct } else { stepPct - pct };
                    if (diff < bestDiff) {
                      bestIdx := i;
                      bestDiff := diff;
                    };
                  };
                  bestIdx
                };

                let kongIdxP = closestIdx(finalKongPctP);
                let icpIdxP = closestIdx(finalIcpPctP);

                // SAFE array access (kongIdxP and icpIdxP are in 0..NUM_QUOTES-1)
                let kongExpectedOutP = kong[kongIdxP].out;
                let icpExpectedOutP = icp[icpIdxP].out;

                logger.info("PARTIAL_EXECUTION",
                  "Partial split - Kong=" # Nat.toText(finalKongPctP / 100) # "%" #
                  " ICP=" # Nat.toText(finalIcpPctP / 100) # "%" #
                  " Total=" # Nat.toText(finalTotalPct / 100) # "%" #
                  (if (interpolatedP) { " INTERP" } else { "" }),
                  "findBestExecution"
                );

                #ok(#Partial({
                  kongswap = {
                    amount = kongAmountP;
                    expectedOut = kongExpectedOutP;
                    slippageBP = finalKongSlipBPP;
                    percentBP = finalKongPctP;
                  };
                  icpswap = {
                    amount = icpAmountP;
                    expectedOut = icpExpectedOutP;
                    slippageBP = finalIcpSlipBPP;
                    percentBP = finalIcpPctP;
                  };
                  taco = {
                    amount = 0; expectedOut = 0; slippageBP = 0; percentBP = 0;
                  };
                  totalPercentBP = finalTotalPct;
                }))
              };
            }
          }
        };
        case (?best) {
          // Check if we can interpolate between best and second-best
          // Only interpolate if both are splits and adjacent (differ by 10% = STEP_BP)
          let (finalKongPct, finalIcpPct, finalKongSlipBP, finalIcpSlipBP, interpolated) : (Nat, Nat, Nat, Nat, Bool) = switch (secondBestScenario) {
            case (?second) {
              // Check if both are splits (not single exchange)
              let bothAreSplits = best.kongPct > 0 and best.kongPct < 10000 and second.kongPct > 0 and second.kongPct < 10000;

              // Check if they're adjacent (differ by exactly 10% = STEP_BP)
              let diff = if (best.kongPct > second.kongPct) { best.kongPct - second.kongPct } else { second.kongPct - best.kongPct };
              let areAdjacent = diff == STEP_BP;

              if (bothAreSplits and areAdjacent) {
                // Use inverse slippage weighting to find optimal split within the 10% range
                // Using INTEGER MATH to avoid Float traps

                // Average slippages (integer)
                let avgKongSlipBP = (best.kongSlipBP + second.kongSlipBP) / 2;
                let avgIcpSlipBP = (best.icpSlipBP + second.icpSlipBP) / 2;
                let totalSlipBP = avgKongSlipBP + avgIcpSlipBP;

                if (totalSlipBP > 0) {
                  // kongRatio = avgIcpSlip / totalSlip
                  // Scale by 10000 for precision: kongRatioBP = (avgIcpSlipBP * 10000) / totalSlipBP
                  let kongRatioBP = (avgIcpSlipBP * 10000) / totalSlipBP;

                  // Scale to the range between scenarios
                  let lowKongPct = if (best.kongPct < second.kongPct) { best.kongPct } else { second.kongPct };
                  let highKongPct = if (best.kongPct > second.kongPct) { best.kongPct } else { second.kongPct };

                  // Calculate interpolated percentage within the 10% range (integer math)
                  let kongRange = highKongPct - lowKongPct;
                  let interpolatedKongPct = lowKongPct + (kongRatioBP * kongRange) / 10000;
                  let interpolatedIcpPct = 10000 - interpolatedKongPct;

                  // Interpolate slippage values (integer)
                  let lowKongSlip = if (best.kongPct < second.kongPct) { best.kongSlipBP } else { second.kongSlipBP };
                  let highKongSlip = if (best.kongPct > second.kongPct) { best.kongSlipBP } else { second.kongSlipBP };
                  let lowIcpSlip = if (best.icpPct < second.icpPct) { best.icpSlipBP } else { second.icpSlipBP };
                  let highIcpSlip = if (best.icpPct > second.icpPct) { best.icpSlipBP } else { second.icpSlipBP };

                  let kongSlipRange = if (highKongSlip > lowKongSlip) { highKongSlip - lowKongSlip } else { 0 };
                  let icpSlipRange = if (highIcpSlip > lowIcpSlip) { highIcpSlip - lowIcpSlip } else { 0 };

                  let interpKongSlip = lowKongSlip + (kongRatioBP * kongSlipRange) / 10000;
                  let interpIcpSlip = lowIcpSlip + (kongRatioBP * icpSlipRange) / 10000;

                  logger.info("EXCHANGE_COMPARISON",
                    "Interpolating between " # best.name # " and " # second.name #
                    " - avgKongSlip=" # Nat.toText(avgKongSlipBP) # "bp avgIcpSlip=" # Nat.toText(avgIcpSlipBP) # "bp" #
                    " kongRatioBP=" # Nat.toText(kongRatioBP) #
                    " Result: Kong=" # Nat.toText(interpolatedKongPct / 100) # "% ICP=" # Nat.toText(interpolatedIcpPct / 100) # "%",
                    "findBestExecution"
                  );

                  (interpolatedKongPct, interpolatedIcpPct, interpKongSlip, interpIcpSlip, true)
                } else {
                  // Zero slippage - use best scenario as-is
                  (best.kongPct, best.icpPct, best.kongSlipBP, best.icpSlipBP, false)
                }
              } else {
                // Not both splits or not adjacent - use best scenario as-is
                (best.kongPct, best.icpPct, best.kongSlipBP, best.icpSlipBP, false)
              }
            };
            case (null) {
              // No second-best scenario - use best as-is
              (best.kongPct, best.icpPct, best.kongSlipBP, best.icpSlipBP, false)
            };
          };

          logger.info("EXCHANGE_COMPARISON",
            "Final execution plan - " # (if (interpolated) { "INTERPOLATED" } else { best.name }) #
            " Kong=" # Nat.toText(finalKongPct / 100) # "% (slip=" # Nat.toText(finalKongSlipBP) # "bp)" #
            " ICP=" # Nat.toText(finalIcpPct / 100) # "% (slip=" # Nat.toText(finalIcpSlipBP) # "bp)",
            "findBestExecution"
          );

          // Also extract TACO percentage from best scenario
          let finalTacoPct = best.tacoPct;
          let finalTacoSlipBP = best.tacoSlipBP;

          if (finalKongPct == 10000) {
            #ok(#Single({ exchange = #KongSwap; expectedOut = best.totalOut; slippageBP = finalKongSlipBP }))
          } else if (finalIcpPct == 10000) {
            #ok(#Single({ exchange = #ICPSwap; expectedOut = best.totalOut; slippageBP = finalIcpSlipBP }))
          } else if (finalTacoPct == 10000) {
            #ok(#Single({ exchange = #TACO; expectedOut = best.totalOut; slippageBP = finalTacoSlipBP }))
          } else {
            // Split trade (2-way or 3-way)
            let kongAmount = (amountIn * finalKongPct) / 10000;
            let icpAmount = (amountIn * finalIcpPct) / 10000;
            let tacoAmount = amountIn - kongAmount - icpAmount;

            let kongExpectedOut = if (best.kongPct > 0) { (kong[best.kongIdx].out * finalKongPct) / best.kongPct } else { 0 };
            let icpExpectedOut = if (best.icpPct > 0) { (icp[best.icpIdx].out * finalIcpPct) / best.icpPct } else { 0 };
            let tacoExpectedOut = if (best.tacoPct > 0) { (taco[best.tacoIdx].out * finalTacoPct) / best.tacoPct } else { 0 };

            #ok(#Split({
              kongswap = {
                amount = kongAmount;
                expectedOut = kongExpectedOut;
                slippageBP = finalKongSlipBP;
                percentBP = finalKongPct;
              };
              icpswap = {
                amount = icpAmount;
                expectedOut = icpExpectedOut;
                slippageBP = finalIcpSlipBP;
                percentBP = finalIcpPct;
              };
              taco = {
                amount = tacoAmount;
                expectedOut = tacoExpectedOut;
                slippageBP = finalTacoSlipBP;
                percentBP = finalTacoPct;
              };
            }))
          }
        };
      };
    } catch (e) {
      #err({ reason = "Error finding best execution: " # Error.message(e); kongQuotes = []; icpQuotes = [] });
    };
  };

  /**
   * Estimate max tradeable amount when slippage is too high
   *
   * Uses the 10% quote's slippage to estimate the maximum amount that can be traded
   * at 70% of max allowed slippage (for safety margin).
   *
   * Formula: maxAmount = (amountIn * targetSlip) / (bestSlip * 10)
   *
   * Key changes matching Python:
   * 1. If no valid quotes (min_slip >= 99999): return 1 ICP worth, prefer Kong
   *    - The verify step makes a fresh API call - bulk quotes might have failed
   * 2. If calculated == 0 and amountIn > 0: return 1 ICP worth
   *    - Don't check bestSlip <= maxSlippageBP - let verify step filter bad quotes
   */
  private func estimateMaxTradeableAmount(
    kongQuotes : [QuoteData],
    icpQuotes : [QuoteData],
    amountIn : Nat,
    maxSlippageBP : Nat,
    sellToken : Principal,
    buyToken : Principal
  ) : ?{ amount : Nat; exchange : { #KongSwap; #ICPSwap }; idealOut : Nat; minAmountOut : Nat; icpWorth : Nat } {
    // Need at least one quote at 10% (index 0)
    if (kongQuotes.size() == 0 or icpQuotes.size() == 0) { return null };

    // Get 10% quote data (index 0) - use 99999 as sentinel for invalid slippage
    let kong10Slip : Nat = if (kongQuotes[0].out > 0) { kongQuotes[0].slipBP } else { 99999 };
    let icp10Slip : Nat = if (icpQuotes[0].out > 0) { icpQuotes[0].slipBP } else { 99999 };
    let kong10Out : Nat = kongQuotes[0].out;
    let icp10Out : Nat = icpQuotes[0].out;

    // Find best (lowest slippage) exchange
    let (bestSlip, bestExchange, best10Out) : (Nat, { #KongSwap; #ICPSwap }, Nat) =
      if (kong10Slip <= icp10Slip) { (kong10Slip, #KongSwap, kong10Out) }
      else { (icp10Slip, #ICPSwap, icp10Out) };

    // Helper: calculate 1 ICP worth of sell token
    func oneIcpWorth() : Nat {
      switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?d) {
          if (d.priceInICP > 0 and d.tokenDecimals <= 18) {
            let decimals = 10 ** d.tokenDecimals;
            (100_000_000 * decimals) / d.priceInICP
          } else { 0 }
        };
        case null { 0 };
      }
    };

    // Helper: calculate idealOut and minAmountOut from expectedOut and slippage
    // idealOut = expectedOut * 10000 / (10000 - slipBP)
    // minAmountOut = idealOut * (10000 - maxSlippageBP) / 10000
    func calcOutputs(expectedOut : Nat, slipBP : Nat) : (Nat, Nat) {
      let idealOut : Nat = if (slipBP < 9900 and expectedOut > 0) {
        (expectedOut * 10000) / (10000 - slipBP)
      } else {
        expectedOut
      };
      let toleranceMultiplier : Nat = if (maxSlippageBP >= 10000) { 0 } else { 10000 - maxSlippageBP };
      let minAmountOut = (idealOut * toleranceMultiplier) / 10000;
      (idealOut, minAmountOut)
    };

    // Helper: calculate ICP worth of a sell-token amount
    func icpWorthOf(tokenAmount : Nat) : Nat {
      switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?d) {
          if (d.tokenDecimals <= 18 and d.priceInICP > 0) {
            (tokenAmount * d.priceInICP) / (10 ** d.tokenDecimals)
          } else { 0 }
        };
        case null { 0 };
      }
    };

    // If no valid quotes at all, return null to trigger ICP fallback
    // DO NOT attempt reduced trade with idealOut=0/minAmountOut=0 - Kong will reject with "Receive amount is zero"
    if (bestSlip >= 99999) {
      return null;
    };

    // Guard: zero slippage (would divide by zero) - shouldn't happen
    if (bestSlip == 0) { return null };

    // Pool fee is ~0.3% = 30bp, included in both quoted slippage and our max slippage
    // We need to subtract it from both to compare pure price impact vs price impact allowance
    let poolFeeBP : Nat = 30;

    // Subtract pool fee from both slippages (use 1 as minimum to avoid division by zero)
    let bestSlipMinusFee = if (bestSlip > poolFeeBP) { bestSlip - poolFeeBP } else { 1 };
    let maxSlipMinusFee = if (maxSlippageBP > poolFeeBP) { maxSlippageBP - poolFeeBP } else { 1 };

    // Formula: (amountIn/10) * (maxSlipMinusFee / bestSlipMinusFee) * 0.7
    // Reordered for integer math to avoid precision loss:
    // maxAmount = (amountIn * maxSlipMinusFee * 7) / (bestSlipMinusFee * 10 * 10)
    let rawMaxAmount = (amountIn * maxSlipMinusFee * 7) / (bestSlipMinusFee * 100);
    // CAP: reduced amount must NEVER exceed original (can happen when bestSlip is close to pool fee)
    let maxAmount = if (rawMaxAmount > amountIn) { amountIn } else { rawMaxAmount };

    // If calculation gives 0 but we have input, try at least 1 ICP worth
    if (maxAmount == 0 and amountIn > 0) {
      let minAmount = oneIcpWorth();
      if (minAmount > 0) {
        // Scale the 10% quote output: expectedOut = best10Out * minAmount / (amountIn/10)
        let amountIn10 = amountIn / 10;
        let expectedOut = if (amountIn10 > 0) { (best10Out * minAmount) / amountIn10 } else { 0 };
        let (idealOut, minAmountOut) = calcOutputs(expectedOut, bestSlip);
        return ?{ amount = minAmount; exchange = bestExchange; idealOut = idealOut; minAmountOut = minAmountOut; icpWorth = icpWorthOf(minAmount) };
      };
      return null;
    };

    // Scale the 10% quote output to the reduced amount
    // 10% quote was for amountIn/10, so expectedOut = best10Out * maxAmount / (amountIn/10)
    let amountIn10 = amountIn / 10;
    let expectedOut = if (amountIn10 > 0) { (best10Out * maxAmount) / amountIn10 } else { 0 };
    let (idealOut, minAmountOut) = calcOutputs(expectedOut, bestSlip);

    ?{ amount = maxAmount; exchange = bestExchange; idealOut = idealOut; minAmountOut = minAmountOut; icpWorth = icpWorthOf(maxAmount) }
  };

  /**
   * Execute a split trade on up to 3 exchanges IN PARALLEL
   *
   * Kong tracks failed swaps as claims - recovery via recoverKongswapClaims()
   * TACO tracks failed swaps via recoverWronglysent
   */
  private func executeSplitTrade(
    sellToken : Principal,
    buyToken : Principal,
    kongAmount : Nat,
    kongMinOut : Nat,
    kongIdealOut : Nat,
    icpAmount : Nat,
    icpMinOut : Nat,
    icpIdealOut : Nat,
    tacoAmount : Nat,
    tacoMinOut : Nat,
    tacoIdealOut : Nat,
  ) : async* { kongResult : Result.Result<TradeRecord, Text>; icpResult : Result.Result<TradeRecord, Text>; tacoResult : Result.Result<TradeRecord, Text> } {
    let startTime = now();

    // Get symbols for KongSwap
    let sellSymbol = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
      case (?details) { details.tokenSymbol };
      case null { "UNKNOWN" };
    };
    let buySymbol = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
      case (?details) { details.tokenSymbol };
      case null { "UNKNOWN" };
    };

    // Prepare KongSwap params
    let kongParams : swaptypes.KongSwapParams = {
      token0_ledger = sellToken;
      token0_symbol = sellSymbol;
      token1_ledger = buyToken;
      token1_symbol = buySymbol;
      amountIn = kongAmount;
      minAmountOut = kongMinOut;
      deadline = ?(startTime + 300_000_000_000);
      recipient = null;
      txId = null;
      slippageTolerance = Float.fromInt(rebalanceConfig.maxSlippageBasisPoints) / 100.0;
    };

    // Prepare ICPSwap params
    let icpPoolResult = Map.get(ICPswapPools, hashpp, (sellToken, buyToken));

    // START BOTH TRADES IN PARALLEL (no await yet!)
    let kongFuture = (with timeout = 65) KongSwap.executeTransferAndSwapNoTracking(kongParams);

    let icpFuture : async Result.Result<swaptypes.TransferDepositSwapWithdrawResult, Text> = switch (icpPoolResult) {
      case (?poolData) {
        let tx_fee = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
          case (?details) { details.tokenTransferFee };
          case null { 0 };
        };

        let depositParams : swaptypes.ICPSwapDepositParams = {
          poolId = poolData.canisterId;
          token = sellToken;
          amount = icpAmount;
          fee = tx_fee;
        };

        let swapParams : swaptypes.ICPSwapParams = {
          poolId = poolData.canisterId;
          amountIn = if (icpAmount > tx_fee) { icpAmount - tx_fee } else { 0 };
          minAmountOut = icpMinOut;
          zeroForOne = sellToken == Principal.fromText(poolData.token0.address);
        };

        let withdrawParams : swaptypes.OptionalWithdrawParams = {
          token = buyToken;
          amount = null;
        };

        (with timeout = 65) ICPSwap.executeTransferDepositSwapAndWithdraw(
          Principal.fromText(self),
          depositParams,
          swapParams,
          withdrawParams,
          tokenDetailsMap,
        );
      };
      case null {
        async { #err("ICPSwap pool not found") };
      };
    };

    // Launch TACO future in parallel (if amount > 0)
    let tacoFutureExec : async Result.Result<swaptypes.TACOSwapReply, Text> = if (tacoAmount > 0) {
      let sellTokenFee = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?d) { d.tokenTransferFee }; case null { 10000 };
      };
      let exchangeTreasuryPrincipal = Principal.fromText("qbnpl-laaaa-aaaan-q52aq-cai");
      let exchangeTreasuryAccountId = Blob.fromArray(SwapUtils.principalToSubaccount(exchangeTreasuryPrincipal));

      if (lastTacoMultiRoute and lastTacoRouteLegs.size() > 1) {
        // Multi-route: split amount evenly across detected routes (max 3)
        let numLegs = Nat.min(lastTacoRouteLegs.size(), 3);
        let perLeg = tacoAmount / numLegs;
        let legs = Array.tabulate<swaptypes.TACOSplitLeg>(numLegs, func(i) {
          let legAmount = if (i == numLegs - 1) { tacoAmount - perLeg * (numLegs - 1) } else { perLeg };
          { amountIn = legAmount; route = lastTacoRouteLegs[i].route; minLegOut = 0 }
        });
        (with timeout = 65) TACOSwap.executeTransferAndSwapMultiRouteNoTracking({
          tokenIn = sellToken;
          tokenOut = buyToken;
          amountIn = tacoAmount;
          minAmountOut = tacoMinOut;
          transferFee = sellTokenFee;
          exchangeTreasuryAccountId = exchangeTreasuryAccountId;
        }, legs)
      } else {
        // Single route: use existing swapMultiHop path
        (with timeout = 65) TACOSwap.executeTransferAndSwapNoTracking({
          tokenIn = sellToken;
          tokenOut = buyToken;
          amountIn = tacoAmount;
          minAmountOut = tacoMinOut;
          transferFee = sellTokenFee;
          exchangeTreasuryAccountId = exchangeTreasuryAccountId;
        })
      };
    } else {
      async { #err("TACO: zero amount") };
    };

    // NOW AWAIT ALL 3 RESULTS (they were running in parallel!)
    let kongRawResult = await kongFuture;
    let icpRawResult = await icpFuture;
    let tacoRawResult = await tacoFutureExec;

    // Process KongSwap result
    let kongResult : Result.Result<TradeRecord, Text> = switch (kongRawResult) {
      case (#ok(reply)) {
        #ok({
          tokenSold = sellToken;
          tokenBought = buyToken;
          amountSold = kongAmount;
          amountBought = reply.receive_amount;
          exchange = #KongSwap;
          timestamp = startTime;
          success = true;
          error = null;
          slippage = reply.slippage;
        });
      };
      case (#err(e)) {
        // Kong tracks failed swaps as claims - we recover via recoverKongswapClaims()
        #err(e);
      };
    };

    // Clear exchange-pair skip on success
    switch (kongResult) { case (#ok(_)) { clearExchangePairSkip("K", sellToken, buyToken) }; case _ {} };

    // Process ICPSwap result
    let icpResult : Result.Result<TradeRecord, Text> = switch (icpRawResult) {
      case (#ok(result)) {
        #ok({
          tokenSold = sellToken;
          tokenBought = buyToken;
          amountSold = icpAmount;
          amountBought = result.swapAmount;
          exchange = #ICPSwap;
          timestamp = startTime;
          success = true;
          error = null;
          slippage = 0.0;
        });
      };
      case (#err(e)) {
        #err(e);
      };
    };

    // Process TACO result
    let tacoResult : Result.Result<TradeRecord, Text> = switch (tacoRawResult) {
      case (#ok(reply)) {
        #ok({
          tokenSold = sellToken;
          tokenBought = buyToken;
          amountSold = tacoAmount;
          amountBought = reply.amountOut;
          exchange = #TACO;
          timestamp = startTime;
          success = true;
          error = null;
          slippage = reply.slippage;
        });
      };
      case (#err(e)) {
        // Track for automated recovery
        switch (parseTacoErrorInfo(e)) {
          case (?(blockNum, tokenText)) {
            let key = tokenText # ":" # Nat.toText(blockNum);
            Map.set(tacoFailedSwapBlocks, thash, key, (blockNum, Principal.fromText(tokenText), now()));
            logger.warn("TACO_TRACK", "Tracked failed swap for recovery: " # key, "executeSplitTrade");
          };
          case null {};
        };
        #err(e);
      };
    };

    // Clear exchange-pair skip on success
    switch (tacoResult) { case (#ok(_)) { clearExchangePairSkip("T", sellToken, buyToken) }; case _ {} };

    { kongResult = kongResult; icpResult = icpResult; tacoResult = tacoResult };
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
      let sellDecimals : Nat = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
        case (?details) { details.tokenDecimals };
        case null { 8 };
      };
      let buyDecimals : Nat = switch (Map.get(tokenDetailsMap, phash, buyToken)) {
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

          let swapResult = await KongSwap.executeTransferAndSwap(swapArgs);
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

              clearExchangePairSkip("K", sellToken, buyToken);
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
                  // Use swapAmount (pre-withdraw-fee) for price calculation to avoid fee affecting exchange rate
                  let amountForPrice = result.swapAmount;
                  // Use receivedAmount (post-withdraw-fee) for actual balance tracking
                  let actualAmountReceived = result.receivedAmount;
                  let effectiveSlippage = if (idealAmountOut > 0) {
                    // Calculate slippage based on swap output (not affected by withdraw fee)
                    if (amountForPrice < idealAmountOut) {
                      Float.fromInt(idealAmountOut - amountForPrice) / Float.fromInt(idealAmountOut) * 100.0;
                    } else { 0.0 }; // We got more than expected, so no negative slippage
                  } else { 0.0 };

                  logger.info("TRADE_EXECUTION",
                    "ICPSwap trade SUCCESS - Swap_output=" # Nat.toText(amountForPrice) #
                    " After_withdraw_fee=" # Nat.toText(actualAmountReceived) #
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
                    amountBought = amountForPrice; // Use pre-fee amount for price calculation
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
        case (#TACO) {
          logger.info("TRADE_EXECUTION", "Executing TACO exchange swap", "executeTrade");

          let sellTokenFee = switch (Map.get(tokenDetailsMap, phash, sellToken)) {
            case (?d) { d.tokenTransferFee }; case null { 10000 };
          };

          // Compute exchange treasury account ID for ICP legacy transfers
          let exchangeTreasuryPrincipal = Principal.fromText("qbnpl-laaaa-aaaan-q52aq-cai");
          let exchangeTreasuryAccountId = Blob.fromArray(SwapUtils.principalToSubaccount(exchangeTreasuryPrincipal));

          let result = if (lastTacoMultiRoute and lastTacoRouteLegs.size() > 1) {
            let numLegs = Nat.min(lastTacoRouteLegs.size(), 3);
            let perLeg = amountIn / numLegs;
            let legs = Array.tabulate<swaptypes.TACOSplitLeg>(numLegs, func(i) {
              let legAmount = if (i == numLegs - 1) { amountIn - perLeg * (numLegs - 1) } else { perLeg };
              { amountIn = legAmount; route = lastTacoRouteLegs[i].route; minLegOut = 0 }
            });
            await TACOSwap.executeTransferAndSwapMultiRoute({
              tokenIn = sellToken;
              tokenOut = buyToken;
              amountIn = amountIn;
              minAmountOut = minAmountOut;
              transferFee = sellTokenFee;
              exchangeTreasuryAccountId = exchangeTreasuryAccountId;
            }, legs)
          } else {
            await TACOSwap.executeTransferAndSwap({
              tokenIn = sellToken;
              tokenOut = buyToken;
              amountIn = amountIn;
              minAmountOut = minAmountOut;
              transferFee = sellTokenFee;
              exchangeTreasuryAccountId = exchangeTreasuryAccountId;
            })
          };

          switch (result) {
            case (#ok(reply)) {
              let effectiveSlippage = reply.slippage;
              logger.info("TRADE_EXECUTION",
                "TACO swap SUCCESS - In=" # Nat.toText(amountIn) # " Out=" # Nat.toText(reply.amountOut) #
                " Slip=" # Float.toText(effectiveSlippage) # "%" #
                " Route=" # debug_show(reply.route),
                "executeTrade"
              );
              clearExchangePairSkip("T", sellToken, buyToken);
              #ok({
                tokenSold = sellToken;
                tokenBought = buyToken;
                amountSold = amountIn;
                amountBought = reply.amountOut;
                exchange = #TACO;
                timestamp = now();
                success = true;
                error = null;
                slippage = effectiveSlippage;
              });
            };
            case (#err(e)) {
              logger.error("TRADE_EXECUTION", "TACO swap FAILED: " # e, "executeTrade");
              // Track for automated recovery
              switch (parseTacoErrorInfo(e)) {
                case (?(blockNum, tokenText)) {
                  let key = tokenText # ":" # Nat.toText(blockNum);
                  Map.set(tacoFailedSwapBlocks, thash, key, (blockNum, Principal.fromText(tokenText), now()));
                  logger.warn("TACO_TRACK", "Tracked failed swap for recovery: " # key, "executeTrade");
                };
                case null {};
              };
              #err("TACO trade failed: " # e);
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
  public shared ({ caller }) func admin_startShortSyncTimer() : async Bool {
    assert (isMasterAdmin(caller) or Principal.isController(caller));
    startShortSyncTimer<system>(true);
    return true;
  };

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
            await* syncPriceWithDEX();
          } catch (e_dex) {
            logger.error(
              "SHORT_SYNC",
              "Failed to perform syncPriceWithDEX: " # Error.message(e_dex),
              "startShortSyncTimer"
            );
          };
          try {
            ignore await (with timeout = 65) dao.syncTokenDetailsFromTreasury(Iter.toArray(Map.entries(tokenDetailsMap)));
          } catch (e_sync) {
            logger.error(
              "SHORT_SYNC",
              "Failed to perform syncTokenDetailsFromTreasury: " # Error.message(e_sync),
              "startShortSyncTimer"
            );
          };
          for ((token, details) in Map.entries(tokenDetailsMap)) {
            if ((details.lastTimeSynced + rebalanceConfig.tokenSyncTimeoutNS) < now()) {
              Map.set(tokenDetailsMap, phash, token, { details with pausedDueToSyncFailure = true });
            };
          };
          // Schedule next sync
          startShortSyncTimer<system>(false);
        } catch (e) {
          logger.error(
            "SHORT_SYNC",
            "Failed to perform short sync: " # Error.message(e),
            "startShortSyncTimer"
          );
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
        Debug.print("Sync price with DEX");
        await* syncPriceWithDEX();
      } catch (_) {};
      try {
        Debug.print("Sync token details to DAO");
        ignore await (with timeout = 65) dao.syncTokenDetailsFromTreasury(Iter.toArray(Map.entries(tokenDetailsMap)));
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

  public shared ({ caller }) func admin_syncWithDaoNoPull() : async Result.Result<Text, Text> {

    if (((await hasAdminPermission(caller, #startRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      Debug.print("Not authorized to execute trading cycle: " # debug_show(caller));
      return #err("Not authorized");
    };

    try {
      Debug.print("Debug sync DAO");
      //await syncFromDAO();
      Debug.print("Update balances");
      await updateBalances();
      try {
        Debug.print("Sync price with DEX");
        await* syncPriceWithDEX();
      } catch (_) {};
      try {
        Debug.print("Sync token details to DAO");
        ignore await (with timeout = 65) dao.syncTokenDetailsFromTreasury(Iter.toArray(Map.entries(tokenDetailsMap)));
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

  public shared ({ caller }) func admin_syncToDao() : async Result.Result<Text, Text> {

    if (((await hasAdminPermission(caller, #startRebalancing)) == false) and caller != DAOPrincipal and not Principal.isController(caller)) {
      Debug.print("Not authorized to execute trading cycle: " # debug_show(caller));
      return #err("Not authorized");
    };

    try {
      ignore await (with timeout = 65) dao.syncTokenDetailsFromTreasury(Iter.toArray(Map.entries(tokenDetailsMap)));
      return #ok("Synced with DAO");
    } catch (e) {
      return #err("Error syncing with DAO: " # Error.message(e));
    };
  };


  private func startLongSyncTimer<system>(instant : Bool) {
    if (longSyncTimerId != 0) {
      cancelTimer(longSyncTimerId);
    };

    // Calculate next scheduled time
    let intervalNS = if (instant) { 1_000_000 } else { rebalanceConfig.longSyncIntervalNS };
    nextLongSyncTime := now() + intervalNS;

    longSyncTimerId := setTimer<system>(
      #nanoseconds(intervalNS),
      func() : async () {
        // Record execution time
        lastLongSyncTime := now();
        
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

    let tokenDetailsFuture = (with timeout = 65) dao.getTokenDetailsWithoutPastPrices();
    let allocationFuture = (with timeout = 65) dao.getAggregateAllocation();
    let tokenDetailsResult = await tokenDetailsFuture;
    let allocationResult = await allocationFuture;
    Debug.print("Token details result: " # debug_show (tokenDetailsResult));
    // Update token info map
    for ((principal, details) in tokenDetailsResult.vals()) {
      Debug.print("Updating token details for " # Principal.toText(principal));
      let oldDetails = Map.get(tokenDetailsMap, phash, principal);
      switch (oldDetails) {
        case null {
          // New token - create full TokenDetails with empty pastPrices
          Map.set(
            tokenDetailsMap,
            phash,
            principal,
            {
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
              pastPrices = []; // Initialize with empty price history
              lastTimeSynced = details.lastTimeSynced;
              pausedDueToSyncFailure = details.pausedDueToSyncFailure;
            },
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
    let allTokensNTN = try { await (with timeout = 65) priceCheckNTN.getAllTokens() } catch (_) {
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

      // priceInICP is always stored in e8s (10^8), regardless of token decimals
      // Guard against division by zero (ICPprice = 0) and ensure result is finite
      if (ICPprice > 0.0) {
        let priceRatio = (usdPrice * 100000000.0) / ICPprice;
        if (isFiniteFloat(priceRatio)) {
          let icpPrice = Float.toInt(priceRatio);
          updateTokenPriceWithHistory(principal, Int.abs(icpPrice), usdPrice);
        };
      };
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
   * Sync token prices with DEX data (Kong and ICPSwap)
   *
   * Fetches latest price data from DEXes instead of NTN oracle.
   * Strategy:
   * 1. Get ICP/USD price via ICP/ckUSDC pair
   * 2. Get token/ICP prices for all tokens
   * 3. Calculate USD prices using ICP/USD rate
   * 4. Liquidity-weighted merge when available from both DEXes
   */

  // Kong confidence weight: derived from slippage on a 1-unit quote.
  // Lower slippage = deeper liquidity = more reliable price.
  private func kongSlippageToWeight(slippage : ?Float) : Float {
    switch (slippage) {
      case (?s) {
        if (s == 0.001) { 1.0 }       // Effectively zero slippage
        else if (s < 0.1) { 0.9 }     // <0.1%: excellent liquidity
        else if (s < 0.35) { 0.7 }     // <0.5%: good
        else if (s < 0.39) { 0.4 }     // <1%: moderate
        else if (s < 0.45) { 0.15 }    // <3%: poor
        else if (s < 0.55) { 0.05 } 
        else if (s < 0.70) { 0.02 }
        else { 0.0 };                  // ≥3%: too thin, ignore
      };
      case null { 0.5 };              // No data: neutral
    };
  };

  // ICPSwap confidence weight: derived from concentrated liquidity (L value).
  // Higher L = more capital at current tick = more reliable price.
  private func icpSwapLiquidityToWeight(liquidity : ?Nat) : Float {
    switch (liquidity) {
      case (?liq) {
        let l = Float.fromInt(liq);
        if (l <= 0.0) { 0.0 }                   // No liquidity, ignore
        else if (l < 1_000_000) { 0.0 }          // Near-zero: ignore
        else if (l < 100_000_000) { 0.4 }        // Moderate
        else if (l < 10_000_000_000) { 0.7 }     // Good
        else { 1.0 };                             // Deep
      };
      case null { 0.5 };                          // No data: neutral
    };
  };

  // TACO confidence weight: from AMM totalLiquidity.
  // Conservatively halved vs ICPSwap (newer, less proven depth).
  private func tacoLiquidityToWeight(liquidity : ?Nat) : Float {
    switch (liquidity) {
      case (?liq) {
        let l = Float.fromInt(liq);
        if (l <= 0.0) { 0.0 }
        else if (l < 1_000_000) { 0.0 }
        else if (l < 100_000_000) { 0.2 }
        else if (l < 10_000_000_000) { 0.35 }
        else { 0.5 };
      };
      case null { 0.25 };
    };
  };

  private func syncPriceWithDEX() : async* () {
    Debug.print("Starting DEX price sync...");
    
    // ckUSDC principal and ICP/ckUSDC pool
    let ckUSDCPrincipal = Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai");
    let icpUsdcPoolPrincipal = Principal.fromText("mohjv-bqaaa-aaaag-qjyia-cai");
    
    // Step 1: Get ICP/USD price via ICP/ckUSDC pair (PARALLEL: fire both, await both)
    var icpPriceUSD : ?Float = null;

    Debug.print("Getting ICP/USD price via ckUSDC (Kong + ICPSwap + TACO in parallel)...");

    // Fire all DEX requests simultaneously
    let kongICPFuture = (with timeout = 65) KongSwap.getQuote("ICP", "ckUSDC", 100000000, 8, 6);
    let icpSwapICPFuture = (with timeout = 65) ICPSwap.getPrice(icpUsdcPoolPrincipal);
    // TACO: one getAllPools call for liquidity weights + ICP/USD quote for multi-hop aggregated price
    let tacoPoolsFuture = (with timeout = 65) TACOSwap.getAllPools();
    let tacoICPFuture = (with timeout = 65) TACOSwap.getQuote(ICPprincipalText, Principal.toText(ckUSDCPrincipal), 100000000, 8, 6);

    // Await Kong ICP/ckUSDC result
    var kongICPPrice : ?Float = null;
    var kongICPSlippage : ?Float = null;
    try {
      let kongResult = await kongICPFuture;
      switch (kongResult) {
        case (#ok(quote)) {
          if (quote.mid_price > 0.0) {
            kongICPPrice := ?quote.mid_price;
            kongICPSlippage := ?quote.slippage;
            Debug.print("Kong ICP/ckUSDC mid_price: " # Float.toText(quote.mid_price) # " USD per ICP, slippage: " # Float.toText(quote.slippage) # "%");
          } else {
            Debug.print("Kong ICP/ckUSDC returned zero/invalid price; ignoring");
          };
        };
        case (#err(e)) {
          Debug.print("Kong ICP/ckUSDC quote failed: " # e);
        };
      };
    } catch (e) {
      Debug.print("Kong ICP/ckUSDC exception: " # Error.message(e));
    };

    // Await ICPSwap ICP/ckUSDC result
    var icpSwapICPPrice : ?Float = null;
    var icpSwapICPLiquidity : ?Nat = null;
    try {
      let icpSwapResult = await icpSwapICPFuture;
      switch (icpSwapResult) {
        case (#ok(priceInfo)) {
          let icpAddress = Principal.toText(ICPprincipal);
          let usdcAddress = Principal.toText(ckUSDCPrincipal);

          let icpDecimals = 8;
          let usdcDecimals = 6;
          let decimalAdjustment = Float.fromInt(10 ** (icpDecimals - usdcDecimals)); // 10^(8-6) = 100

          let icpPrice = if (icpAddress == priceInfo.token0.address and usdcAddress == priceInfo.token1.address) {
            let adjustedPrice = priceInfo.price * decimalAdjustment;
            Debug.print("ICPSwap raw price: " # Float.toText(priceInfo.price) # ", decimal adjusted: " # Float.toText(adjustedPrice));
            adjustedPrice
          } else if (icpAddress == priceInfo.token1.address and usdcAddress == priceInfo.token0.address) {
            let adjustedPrice = priceInfo.price * decimalAdjustment;
            if (adjustedPrice > 0.0) { 1.0 / adjustedPrice } else { 0.0 }
          } else {
            Debug.print("Unexpected token configuration in ICP/ckUSDC pool - token0: " # priceInfo.token0.address # ", token1: " # priceInfo.token1.address);
            0.0
          };

          if (icpPrice > 0.0) {
            icpSwapICPPrice := ?icpPrice;
            icpSwapICPLiquidity := ?priceInfo.liquidity;
            Debug.print("ICPSwap ICP/ckUSDC price: " # Float.toText(icpPrice) # ", liquidity: " # Nat.toText(priceInfo.liquidity) # " (token0=" # priceInfo.token0.address # ", token1=" # priceInfo.token1.address # ")");
          } else {
            Debug.print("ICPSwap ICP/ckUSDC price calculation resulted in 0 or invalid");
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap ICP/ckUSDC price failed: " # e);
        };
      };
    } catch (e) {
      Debug.print("ICPSwap ICP/ckUSDC exception: " # Error.message(e));
    };

    // Await TACO pools for liquidity weights
    let tacoPoolLiquidity = Map.new<Text, Nat>();
    try {
      let poolsResult = await tacoPoolsFuture;
      switch (poolsResult) {
        case (#ok(pools)) {
          for (pool in pools.vals()) {
            if (pool.token1 == ICPprincipalText or pool.token0 == ICPprincipalText) {
              let otherToken = if (pool.token0 == ICPprincipalText) { pool.token1 } else { pool.token0 };
              Map.set(tacoPoolLiquidity, thash, otherToken, pool.totalLiquidity);
            };
          };
          Debug.print("TACO pools indexed: " # Nat.toText(Map.size(tacoPoolLiquidity)) # " token/ICP pairs");
        };
        case (#err(e)) { Debug.print("TACO getAllPools failed: " # e) };
      };
    } catch (e) { Debug.print("TACO getAllPools exception: " # Error.message(e)) };

    // Await TACO ICP/ckUSDC result
    var tacoICPPrice : ?Float = null;
    var tacoICPLiquidity : ?Nat = null;
    try {
      let tacoResult = await tacoICPFuture;
      switch (tacoResult) {
        case (#ok(quote)) {
          if (quote.mid_price > 0.0) {
            tacoICPPrice := ?quote.mid_price;
            tacoICPLiquidity := Map.get(tacoPoolLiquidity, thash, Principal.toText(ckUSDCPrincipal));
            Debug.print("TACO ICP/ckUSDC mid_price: " # Float.toText(quote.mid_price) # " slippage: " # Float.toText(quote.slippage) # "%");
          } else {
            Debug.print("TACO ICP/ckUSDC zero price; ignoring");
          };
        };
        case (#err(e)) { Debug.print("TACO ICP/ckUSDC failed: " # e) };
      };
    } catch (e) { Debug.print("TACO ICP/ckUSDC exception: " # Error.message(e)) };

    // Calculate final ICP/USD price (N-source liquidity-weighted)
    var icpWeightedSum : Float = 0.0;
    var icpTotalWeight : Float = 0.0;
    var icpSourceCount : Nat = 0;

    switch (kongICPPrice) {
      case (?kong) {
        let w = kongSlippageToWeight(kongICPSlippage);
        icpWeightedSum += kong * w; icpTotalWeight += w; icpSourceCount += 1;
        Debug.print("ICP/USD Kong: " # Float.toText(kong) # " w=" # Float.toText(w));
      }; case null {};
    };
    switch (icpSwapICPPrice) {
      case (?icpSwap) {
        let w = icpSwapLiquidityToWeight(icpSwapICPLiquidity);
        icpWeightedSum += icpSwap * w; icpTotalWeight += w; icpSourceCount += 1;
        Debug.print("ICP/USD ICPSwap: " # Float.toText(icpSwap) # " w=" # Float.toText(w));
      }; case null {};
    };
    switch (tacoICPPrice) {
      case (?taco) {
        let w = tacoLiquidityToWeight(tacoICPLiquidity);
        icpWeightedSum += taco * w; icpTotalWeight += w; icpSourceCount += 1;
        Debug.print("ICP/USD TACO: " # Float.toText(taco) # " w=" # Float.toText(w));
      }; case null {};
    };

    if (icpSourceCount == 0) {
      Debug.print("Failed to get ICP/USD price from all DEXes - aborting price sync to trigger sync failure detection");
      return;
    };

    icpPriceUSD := if (icpTotalWeight > 0.0) {
      ?(icpWeightedSum / icpTotalWeight)
    } else {
      var sum = 0.0;
      switch (kongICPPrice) { case (?p) { sum += p }; case null {} };
      switch (icpSwapICPPrice) { case (?p) { sum += p }; case null {} };
      switch (tacoICPPrice) { case (?p) { sum += p }; case null {} };
      ?(sum / Float.fromInt(icpSourceCount))
    };

    Debug.print("ICP/USD final: " # Float.toText(switch (icpPriceUSD) { case (?p) p; case null 0.0 }) # " from " # Nat.toText(icpSourceCount) # " sources");

    // (old 2-source switch block removed — replaced by N-source accumulator above)
    
    // Step 2: Get prices for all tokens against ICP
    Debug.print("Getting token/ICP prices for all tokens...");
    
    // Extract the ICP/USD price (we know it exists at this point)
    let finalICPPriceUSD = switch (icpPriceUSD) {
      case (?price) { price };
      case null { 
        Debug.print("Internal error: icpPriceUSD is null after successful price discovery");
        return;
      };
    };
    
    // ── Step 2a: Fire ALL Kong + ICPSwap + TACO price requests in parallel ──
    // Same pattern as updateBalances(): collect futures first, await later.
    // This reduces total time from O(N × 3 × 65s) to O(65s).
    Debug.print("Firing all token price requests in parallel...");

    let kongFutures = Map.new<Principal, async Result.Result<swaptypes.SwapAmountsReply, Text>>();
    let icpSwapFutures = Map.new<Principal, async Result.Result<swaptypes.ICPSwapPriceInfo, Text>>();
    let tacoFutures = Map.new<Principal, async Result.Result<swaptypes.SwapAmountsReply, Text>>();

    // Update ICP price directly (no DEX call needed)
    updateTokenPriceWithHistory(ICPprincipal, 100000000, finalICPPriceUSD);

    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (principal == ICPprincipal) { /* already handled above */ } else {
        // Fire Kong future
        let oneTokenAmount = 10 ** details.tokenDecimals;
        let kongFut = (with timeout = 65) KongSwap.getQuote(details.tokenSymbol, "ICP", oneTokenAmount, details.tokenDecimals, 8);
        Map.set(kongFutures, phash, principal, kongFut);

        // Fire ICPSwap future (only if pool exists)
        let poolKey = (principal, ICPprincipal);
        switch (Map.get(ICPswapPools, hashpp, poolKey)) {
          case (?poolData) {
            let icpFut = (with timeout = 65) ICPSwap.getPrice(poolData.canisterId);
            Map.set(icpSwapFutures, phash, principal, icpFut);
          };
          case null {
            Debug.print("No ICPSwap pool found for " # details.tokenSymbol # "/ICP");
          };
        };

        // Fire TACO quote (multi-hop aggregated price via getExpectedReceiveAmount)
        let tacoFut = (with timeout = 65) TACOSwap.getQuote(Principal.toText(principal), ICPprincipalText, oneTokenAmount, details.tokenDecimals, 8);
        Map.set(tacoFutures, phash, principal, tacoFut);
      };
    };

    // ── Step 2b: Await all futures and process results per token ──
    Debug.print("Awaiting all price futures...");

    label tokenLoop for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (principal == ICPprincipal) { continue tokenLoop };

      let tokenSymbol = details.tokenSymbol;

      // Await Kong result
      var kongTokenPrice : ?Float = null;
      var kongTokenSlippage : ?Float = null;
      switch (Map.get(kongFutures, phash, principal)) {
        case (?kongFut) {
          try {
            let kongResult = await kongFut;
            switch (kongResult) {
              case (#ok(quote)) {
                if (quote.mid_price > 0.0 and quote.mid_price <= 100000.0) {
                  kongTokenPrice := ?quote.mid_price;
                  kongTokenSlippage := ?quote.slippage;
                  Debug.print("Kong " # tokenSymbol # "/ICP mid_price: " # Float.toText(quote.mid_price) # " ICP per " # tokenSymbol # ", slippage: " # Float.toText(quote.slippage) # "% (ACCEPTED)");
                } else {
                  Debug.print("Kong " # tokenSymbol # "/ICP returned zero/unreasonable price (" # Float.toText(quote.mid_price) # "); ignoring");
                };
              };
              case (#err(e)) {
                Debug.print("Kong " # tokenSymbol # "/ICP quote failed: " # e);
              };
            };
          } catch (e) {
            Debug.print("Kong " # tokenSymbol # "/ICP exception: " # Error.message(e));
          };
        };
        case null {};
      };

      // Await ICPSwap result
      var icpSwapTokenPrice : ?Float = null;
      var icpSwapTokenLiquidity : ?Nat = null;
      switch (Map.get(icpSwapFutures, phash, principal)) {
        case (?icpFut) {
          try {
            let icpSwapResult = await icpFut;
            switch (icpSwapResult) {
              case (#ok(priceInfo)) {
                let icpAddress = Principal.toText(ICPprincipal);
                let tokenAddress = Principal.toText(principal);

                Debug.print("ICPSwap price analysis - Raw price: " # Float.toText(priceInfo.price) # ", token0: " # priceInfo.token0.address # ", token1: " # priceInfo.token1.address);

                let icpDecimals : Nat = 8;
                let tokenDecimals : Nat = details.tokenDecimals;

                let tokenICPPrice = if (tokenAddress == priceInfo.token0.address and icpAddress == priceInfo.token1.address) {
                  let adjustment = Float.fromInt(10 ** (Int.abs(tokenDecimals - icpDecimals)));
                  let adjustedPrice = if (tokenDecimals > icpDecimals) {
                    priceInfo.price * adjustment
                  } else if (tokenDecimals < icpDecimals) {
                    priceInfo.price / adjustment
                  } else {
                    priceInfo.price
                  };
                  Debug.print("Token is token0, ICP is token1. Raw=" # Float.toText(priceInfo.price) # " Adjusted=" # Float.toText(adjustedPrice) # " ICP per " # tokenSymbol);
                  adjustedPrice
                } else if (tokenAddress == priceInfo.token1.address and icpAddress == priceInfo.token0.address) {
                  let adjustment = Float.fromInt(10 ** (Int.abs(icpDecimals - tokenDecimals)));
                  let adjustedPrice = if (tokenDecimals > icpDecimals) {
                    priceInfo.price / adjustment
                  } else if (tokenDecimals < icpDecimals) {
                    priceInfo.price * adjustment
                  } else {
                    priceInfo.price
                  };
                  let icpPerToken = if (adjustedPrice > 0.0) { 1.0 / adjustedPrice } else { 0.0 };
                  Debug.print("Token is token1, ICP is token0. Raw=" # Float.toText(priceInfo.price) # " Adjusted=" # Float.toText(adjustedPrice) # " ICP/TOKEN=" # Float.toText(icpPerToken));
                  icpPerToken
                } else {
                  Debug.print("Unexpected token configuration in pool - token0: " # priceInfo.token0.address # ", token1: " # priceInfo.token1.address);
                  0.0
                };

                if (tokenICPPrice > 0.0) {
                  if (tokenICPPrice >= 0.000001 and tokenICPPrice <= 100000.0) {
                    icpSwapTokenPrice := ?tokenICPPrice;
                    icpSwapTokenLiquidity := ?priceInfo.liquidity;
                    Debug.print("ICPSwap " # tokenSymbol # "/ICP price: " # Float.toText(tokenICPPrice) # ", liquidity: " # Nat.toText(priceInfo.liquidity) # " (raw: " # Float.toText(priceInfo.price) # ") - ACCEPTED");
                  } else {
                    Debug.print("ICPSwap " # tokenSymbol # "/ICP price: " # Float.toText(tokenICPPrice) # " seems unreasonable - REJECTED");
                  };
                } else {
                  Debug.print("ICPSwap " # tokenSymbol # "/ICP price calculation resulted in 0 or invalid");
                };
              };
              case (#err(e)) {
                Debug.print("ICPSwap " # tokenSymbol # "/ICP price failed: " # e);
              };
            };
          } catch (e) {
            Debug.print("ICPSwap " # tokenSymbol # "/ICP exception: " # Error.message(e));
          };
        };
        case null {};
      };

      // Await TACO result + look up liquidity from cached pools
      var tacoTokenPrice : ?Float = null;
      var tacoTokenLiquidity : ?Nat = null;
      switch (Map.get(tacoFutures, phash, principal)) {
        case (?tacoFut) {
          try {
            let tacoResult = await tacoFut;
            switch (tacoResult) {
              case (#ok(quote)) {
                if (quote.mid_price > 0.0 and quote.mid_price <= 100000.0) {
                  tacoTokenPrice := ?quote.mid_price;
                  tacoTokenLiquidity := Map.get(tacoPoolLiquidity, thash, Principal.toText(principal));
                  Debug.print("TACO " # tokenSymbol # "/ICP mid_price: " # Float.toText(quote.mid_price) #
                    " slippage: " # Float.toText(quote.slippage) # "%" #
                    " liq: " # (switch (tacoTokenLiquidity) { case (?l) Nat.toText(l); case null "none" }) #
                    " (ACCEPTED)");
                } else {
                  Debug.print("TACO " # tokenSymbol # "/ICP unreasonable price (" # Float.toText(quote.mid_price) # "); ignoring");
                };
              };
              case (#err(e)) {
                Debug.print("TACO " # tokenSymbol # "/ICP quote failed: " # e);
              };
            };
          } catch (e) {
            Debug.print("TACO " # tokenSymbol # "/ICP exception: " # Error.message(e));
          };
        };
        case null {};
      };

      // Calculate final token price (N-source liquidity-weighted)
      var tokenWeightedSum : Float = 0.0;
      var tokenTotalWeight : Float = 0.0;
      var tokenSourceCount : Nat = 0;

      switch (kongTokenPrice) {
        case (?kong) {
          let w = kongSlippageToWeight(kongTokenSlippage);
          tokenWeightedSum += kong * w; tokenTotalWeight += w; tokenSourceCount += 1;
        }; case null {};
      };
      switch (icpSwapTokenPrice) {
        case (?icpSwap) {
          let w = icpSwapLiquidityToWeight(icpSwapTokenLiquidity);
          tokenWeightedSum += icpSwap * w; tokenTotalWeight += w; tokenSourceCount += 1;
        }; case null {};
      };
      switch (tacoTokenPrice) {
        case (?taco) {
          let w = tacoLiquidityToWeight(tacoTokenLiquidity);
          tokenWeightedSum += taco * w; tokenTotalWeight += w; tokenSourceCount += 1;
        }; case null {};
      };

      if (tokenSourceCount > 0) {
        let finalPrice : Float = if (tokenTotalWeight > 0.0) {
          let weighted = tokenWeightedSum / tokenTotalWeight;
          Debug.print(tokenSymbol # " DEX weighted: " # Float.toText(weighted) #
            " from " # Nat.toText(tokenSourceCount) # " sources (totalW=" # Float.toText(tokenTotalWeight) # ")");
          weighted
        } else {
          var sum = 0.0;
          switch (kongTokenPrice) { case (?p) { sum += p }; case null {} };
          switch (icpSwapTokenPrice) { case (?p) { sum += p }; case null {} };
          switch (tacoTokenPrice) { case (?p) { sum += p }; case null {} };
          let avg = sum / Float.fromInt(tokenSourceCount);
          Debug.print(tokenSymbol # " DEX all weights zero - average: " # Float.toText(avg));
          avg
        };

        let scaledPrice = finalPrice * 100000000.0;
        if (finalPrice > 0.0 and isFiniteFloat(scaledPrice)) {
          let icpPrice = Float.toInt(scaledPrice);
          let usdPrice = finalPrice * finalICPPriceUSD;
          updateTokenPriceWithHistory(principal, Int.abs(icpPrice), usdPrice);
          Debug.print("Updated " # tokenSymbol # " - ICP: " # Int.toText(Int.abs(icpPrice)) # ", USD: " # Float.toText(usdPrice));
        } else {
          Debug.print("Final price zero/unreasonable; skipping " # tokenSymbol);
        };
      } else {
        Debug.print("No price for " # tokenSymbol # " from any DEX, keeping existing");
      };
    };
    
    // Update rebalance state metrics
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
   * Public price refresh function for nachos_vault and authorized callers.
   * Rate-limited to MIN_PRICE_REFRESH_INTERVAL_NS between calls.
   * Wraps the private syncPriceWithDEX() function.
   */
  public shared ({ caller }) func refreshAllPrices() : async Result.Result<{
    tokensRefreshed : Nat;
    timestamp : Int;
    icpPriceUSD : Float;
  }, Text> {
    if (
      caller != DAOPrincipal and
      caller != NachosVaultPrincipal and
      not isMasterAdmin(caller) and
      not Principal.isController(caller)
    ) {
      return #err("Not authorized to refresh prices");
    };

    // Rate limiting: minimum 30 seconds between refreshes
    if (now() - lastPriceRefreshTime < MIN_PRICE_REFRESH_INTERVAL_NS) {
      // Prices are recent enough, return cached info
      let icpPrice : Float = switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
        case (?details) { details.priceInUSD };
        case null { 0.0 };
      };
      return #ok({
        tokensRefreshed = Map.size(tokenDetailsMap);
        timestamp = lastPriceRefreshTime;
        icpPriceUSD = icpPrice;
      });
    };

    try {
      await* syncPriceWithDEX();
      lastPriceRefreshTime := now();

      let icpPrice : Float = switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
        case (?details) { details.priceInUSD };
        case null { 0.0 };
      };

      #ok({
        tokensRefreshed = Map.size(tokenDetailsMap);
        timestamp = now();
        icpPriceUSD = icpPrice;
      });
    } catch (e) {
      #err("Price refresh failed: " # Error.message(e));
    };
  };

  // Combined price refresh + token details fetch (saves 1 inter-canister call for nachos vault)
  public shared ({ caller }) func refreshPricesAndGetDetails() : async Result.Result<{
    tokensRefreshed : Nat;
    timestamp : Int;
    icpPriceUSD : Float;
    tokenDetails : [(Principal, TokenDetails)];
  }, Text> {
    if (
      caller != DAOPrincipal and
      caller != NachosVaultPrincipal and
      not isMasterAdmin(caller) and
      not Principal.isController(caller)
    ) {
      return #err("Not authorized");
    };

    // Rate limiting: minimum 30 seconds between refreshes
    if (now() - lastPriceRefreshTime < MIN_PRICE_REFRESH_INTERVAL_NS) {
      let icpPrice : Float = switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
        case (?details) { details.priceInUSD };
        case null { 0.0 };
      };
      return #ok({
        tokensRefreshed = Map.size(tokenDetailsMap);
        timestamp = lastPriceRefreshTime;
        icpPriceUSD = icpPrice;
        tokenDetails = Array.map<(Principal, TokenDetails), (Principal, TokenDetails)>(
          Iter.toArray(Map.entries(tokenDetailsMap)),
          func((p, d)) { (p, { d with pastPrices = [] }) },
        );
      });
    };

    try {
      await* syncPriceWithDEX();
      lastPriceRefreshTime := now();

      let icpPrice : Float = switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
        case (?details) { details.priceInUSD };
        case null { 0.0 };
      };

      #ok({
        tokensRefreshed = Map.size(tokenDetailsMap);
        timestamp = now();
        icpPriceUSD = icpPrice;
        tokenDetails = Array.map<(Principal, TokenDetails), (Principal, TokenDetails)>(
          Iter.toArray(Map.entries(tokenDetailsMap)),
          func((p, d)) { (p, { d with pastPrices = [] }) },
        );
      });
    } catch (e) {
      #err("Price refresh failed: " # Error.message(e));
    };
  };

  // Return cached token details without triggering DEX sync (non-query for inter-canister calls)
  // Used by nachos vault as fallback when refreshPricesAndGetDetails fails
  public shared ({ caller }) func getTokenDetailsCache() : async {
    timestamp : Int;
    icpPriceUSD : Float;
    tokenDetails : [(Principal, TokenDetails)];
    tradingPauses : [TradingPauseRecord];
    lpBackingPerToken : [(Principal, Nat)];
  } {
    if (
      caller != DAOPrincipal and
      caller != NachosVaultPrincipal and
      not isMasterAdmin(caller) and
      not Principal.isController(caller)
    ) {
      return {
        timestamp = 0;
        icpPriceUSD = 0.0;
        tokenDetails = [];
        tradingPauses = [];
        lpBackingPerToken = [];
      };
    };
    let icpPrice : Float = switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
      case (?details) { details.priceInUSD };
      case null { 0.0 };
    };

    // Compute LP backing: lpBacking + inTransit + depositsInFlight per token
    let lpBuf = Buffer.Buffer<(Principal, Nat)>(Map.size(tokenDetailsMap));
    for ((token, _) in Map.entries(tokenDetailsMap)) {
      let backing = switch (Map.get(lpBackingPerToken, phash, token)) { case (?v) v; case null 0 };
      let transit = switch (Map.get(lpTokensInTransit, phash, token)) { case (?(amt, _)) amt; case null 0 };
      let flight = switch (Map.get(lpDepositsInFlight, phash, token)) { case (?v) v; case null 0 };
      let total = backing + transit + flight;
      if (total > 0) { lpBuf.add((token, total)) };
    };

    {
      timestamp = lastPriceRefreshTime;
      icpPriceUSD = icpPrice;
      tokenDetails = Array.map<(Principal, TokenDetails), (Principal, TokenDetails)>(
        Iter.toArray(Map.entries(tokenDetailsMap)),
        func((p, d)) { (p, { d with pastPrices = [] }) },
      );
      tradingPauses = Iter.toArray(Map.vals(tradingPauses));
      lpBackingPerToken = Buffer.toArray(lpBuf);
    };
  };

  // Query fresh available balances for specified tokens, accounting for pending burns
  // Used by NACHOS vault to get real-time balances during burn operations
  public shared query ({ caller }) func getAvailableBalancesForBurn(
    tokens : [Principal]
  ) : async Result.Result<[(Principal, Nat)], Text> {
    // Authorization: Only NachosVaultPrincipal can query available balances
    if (caller != NachosVaultPrincipal) {
      return #err("Unauthorized: Only NACHOS vault can query available balances for burn");
    };

    let results = Buffer.Buffer<(Principal, Nat)>(tokens.size());

    for (token in tokens.vals()) {
      // Use liquid balance only (not effective balance which includes LP backing)
      // LP-locked tokens cannot be distributed to NACHOS redeemers
      let balance = switch (Map.get(liquidBalancePerToken, phash, token)) {
        case (?liquid) { liquid };
        case null {
          // LP not initialized — fallback to tokenDetailsMap (backwards compatible)
          switch (Map.get(tokenDetailsMap, phash, token)) {
            case (?details) { details.balance };
            case null { 0 };
          };
        };
      };

      // Subtract pending burns (tokens reserved for in-flight burn payouts)
      let pending = getPendingBurn(token);
      let available = if (balance > pending) { balance - pending } else { 0 };

      results.add((token, available));
    };

    #ok(Buffer.toArray(results))
  };

  // Returns ALL non-liquid portfolio value per token:
  //   lpBacking (locked in exchange pools) + inTransit (removed, arriving) + depositsInFlight (sent, not yet LP)
  // Used by NACHOS vault for NAV calculation
  public shared query ({ caller }) func getLPBackingPerToken() : async [(Principal, Nat)] {
    if (
      caller != NachosVaultPrincipal and
      caller != DAOPrincipal and
      not isMasterAdmin(caller) and
      not Principal.isController(caller)
    ) {
      return [];
    };
    let result = Buffer.Buffer<(Principal, Nat)>(Map.size(tokenDetailsMap));
    for ((token, _) in Map.entries(tokenDetailsMap)) {
      let backing = switch (Map.get(lpBackingPerToken, phash, token)) { case (?v) v; case null 0 };
      let transit = switch (Map.get(lpTokensInTransit, phash, token)) { case (?(amt, _)) amt; case null 0 };
      let flight = switch (Map.get(lpDepositsInFlight, phash, token)) { case (?v) v; case null 0 };
      let total = backing + transit + flight;
      if (total > 0) { result.add((token, total)) };
    };
    Buffer.toArray(result);
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
        let metadataFuture = (with timeout = 65) token.icrc1_metadata();
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
    let ICPbalanceFuture = (with timeout = 65) ledger.account_balance({
      account = Principal.toLedgerAccount(Principal.fromText(self), null);
    });

    // Collect all balance fetch futures
    for ((principal, _) in Map.entries(tokenDetailsMap)) {
      if (principal != ICPprincipal) {
        // For ICRC1 tokens
        let token = actor (Principal.toText(principal)) : ICRC1.FullInterface;
        let balanceFuture = (with timeout = 65) token.icrc1_balance_of({
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

    // ===== LP Balance Integration (four-bucket effective balance) =====
    // Runs when LP is enabled OR when positions/in-transit/in-flight still exist (wind-down)
    let hasLPState = lpConfig.enabled or Map.size(lpBackingPerToken) > 0 or Map.size(lpTokensInTransit) > 0 or Map.size(lpDepositsInFlight) > 0 or Map.size(treasuryLPPositions) > 0;
    if (hasLPState) {
      // Save previous liquid balances (for in-transit arrival detection)
      for ((token, _) in Map.entries(tokenDetailsMap)) {
        let prevLiquid = switch (Map.get(liquidBalancePerToken, phash, token)) {
          case (?v) { v }; case null { 0 };
        };
        Map.set(previousLiquidBalance, phash, token, prevLiquid);
      };

      // Store liquid balances BEFORE adding LP backing
      for ((token, details) in Map.entries(tokenDetailsMap)) {
        Map.set(liquidBalancePerToken, phash, token, details.balance);
      };

      // Query exchange for LP positions + pool data
      try {
        let positionsFuture = (with timeout = 65) TACOSwap.getUserLiquidityDetailed();
        let poolsFuture = (with timeout = 65) TACOSwap.getAllAMMPoolsFull();
        let acceptedTokensFuture = (with timeout = 65) TACOSwap.getAcceptedTokensInfo();

        let positionsResult = await positionsFuture;
        let poolsResult = await poolsFuture;
        let acceptedResult = await acceptedTokensFuture;

        // Store accepted tokens and their minimums
        switch (acceptedResult) {
          case (#ok(tokenInfos)) {
            let addresses = Array.map<TACOSwap.ExchangeTokenInfo, Text>(tokenInfos, func(t) { t.address });
            cachedExchangeAcceptedTokens := addresses;
            for (info in tokenInfos.vals()) {
              Map.set(cachedExchangeMinimums, thash, info.address, info.minimum_amount);
            };
          };
          case (#err(_)) {};
        };

        switch (positionsResult, poolsResult) {
          case (#ok(positions), #ok(pools)) {
            cachedLPPositions := positions;
            cachedPoolData := pools;
            lastLPQueryTimestamp := now();

            // Compute LP backing per token from V2 full-range positions only
            let newBacking = Map.new<Principal, Nat>();
            for (pos in positions.vals()) {
              switch (pos.positionType) {
                case (#fullRange) {
                  let t0 = Principal.fromText(pos.token0);
                  let t1 = Principal.fromText(pos.token1);
                  let cur0 = switch (Map.get(newBacking, phash, t0)) { case (?v) v; case null 0 };
                  let cur1 = switch (Map.get(newBacking, phash, t1)) { case (?v) v; case null 0 };
                  Map.set(newBacking, phash, t0, cur0 + pos.token0Amount + pos.fee0);
                  Map.set(newBacking, phash, t1, cur1 + pos.token1Amount + pos.fee1);
                };
                case (#concentrated) {}; // Skip V3 for now
              };
            };
            lpBackingPerToken := newBacking;

            // Detect in-transit token arrival (with timestamp-based safety)
            // Only clear if: balance increased by >= inTransit AND at least 30s since removal
            // This prevents false positives from other deposits arriving in the same cycle
            for ((token, (inTransitAmount, removalTime)) in Map.entries(lpTokensInTransit)) {
              let newLiquid = switch (Map.get(liquidBalancePerToken, phash, token)) { case (?v) v; case null 0 };
              let oldLiquid = switch (Map.get(previousLiquidBalance, phash, token)) { case (?v) v; case null 0 };
              let elapsed = now() - removalTime;
              if (newLiquid >= oldLiquid + inTransitAmount and elapsed > 30_000_000_000) {
                // Balance increased sufficiently and enough time passed — tokens likely arrived
                Map.delete(lpTokensInTransit, phash, token);
              } else if (elapsed > 600_000_000_000) {
                // 10 min timeout — clear regardless (exchange transfer should have completed)
                // Tokens either arrived (liquid will reflect) or are stuck (recovery handles)
                Map.delete(lpTokensInTransit, phash, token);
              };
            };

            // Compute LP budget used per token (ICP value in LP)
            let newBudget = Map.new<Principal, Nat>();
            for (pos in positions.vals()) {
              switch (pos.positionType) {
                case (#fullRange) {
                  let t0 = Principal.fromText(pos.token0);
                  let t1 = Principal.fromText(pos.token1);
                  let d0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
                  let d1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.tokenDecimals }; case null { 8 : Nat } };
                  let p0 = switch (Map.get(tokenDetailsMap, phash, t0)) { case (?d) { d.priceInICP }; case null { 0 } };
                  let p1 = switch (Map.get(tokenDetailsMap, phash, t1)) { case (?d) { d.priceInICP }; case null { 0 } };
                  let val0 = if (p0 > 0 and d0 > 0) { (pos.token0Amount * p0) / (10 ** d0) } else { 0 };
                  let val1 = if (p1 > 0 and d1 > 0) { (pos.token1Amount * p1) / (10 ** d1) } else { 0 };
                  let c0 = switch (Map.get(newBudget, phash, t0)) { case (?v) v; case null 0 };
                  let c1 = switch (Map.get(newBudget, phash, t1)) { case (?v) v; case null 0 };
                  Map.set(newBudget, phash, t0, c0 + val0);
                  Map.set(newBudget, phash, t1, c1 + val1);
                };
                case _ {};
              };
            };
            lpBudgetUsedPerToken := newBudget;

            // Set effectiveBalance = liquid + lpBacking + inTransit + inFlight
            for ((token, details) in Map.entries(tokenDetailsMap)) {
              let liquid = details.balance;
              let backing = switch (Map.get(lpBackingPerToken, phash, token)) { case (?v) v; case null 0 };
              let transit = switch (Map.get(lpTokensInTransit, phash, token)) { case (?(amt, _)) amt; case null 0 };
              let flight = switch (Map.get(lpDepositsInFlight, phash, token)) { case (?v) v; case null 0 };
              Map.set(tokenDetailsMap, phash, token, { details with balance = liquid + backing + transit + flight });
            };

            logger.info("LP_BALANCE", "LP state updated: " # Nat.toText(positions.size()) # " positions, " # Nat.toText(pools.size()) # " pools", "updateBalances");
          };
          case _ {
            // Exchange query failed — use cache or degrade gracefully
            if (now() - lastLPQueryTimestamp < 3_600_000_000_000) {
              // Cache < 1hr — add cached backing to balances
              logger.warn("LP_QUERY", "Using cached LP backing (age: " # Int.toText((now() - lastLPQueryTimestamp) / 1_000_000_000) # "s)", "updateBalances");
              for ((token, details) in Map.entries(tokenDetailsMap)) {
                let backing = switch (Map.get(lpBackingPerToken, phash, token)) { case (?v) v; case null 0 };
                let transit = switch (Map.get(lpTokensInTransit, phash, token)) { case (?(amt, _)) amt; case null 0 };
                let flight = switch (Map.get(lpDepositsInFlight, phash, token)) { case (?v) v; case null 0 };
                Map.set(tokenDetailsMap, phash, token, { details with balance = details.balance + backing + transit + flight });
              };
            } else {
              logger.error("LP_QUERY", "Exchange unreachable, cache expired — LP data stale", "updateBalances");
            };
          };
        };
      } catch (e) {
        logger.error("LP_QUERY", "LP query exception: " # Error.message(e), "updateBalances");
        // Same cache fallback
        if (now() - lastLPQueryTimestamp < 3_600_000_000_000) {
          for ((token, details) in Map.entries(tokenDetailsMap)) {
            let backing = switch (Map.get(lpBackingPerToken, phash, token)) { case (?v) v; case null 0 };
            let transit = switch (Map.get(lpTokensInTransit, phash, token)) { case (?(amt, _)) amt; case null 0 };
            let flight = switch (Map.get(lpDepositsInFlight, phash, token)) { case (?v) v; case null 0 };
            Map.set(tokenDetailsMap, phash, token, { details with balance = details.balance + backing + transit + flight });
          };
        };
      };
    } else {
      // No LP state — still populate liquidBalancePerToken for consistency
      for ((token, details) in Map.entries(tokenDetailsMap)) {
        Map.set(liquidBalancePerToken, phash, token, details.balance);
      };
    };
  };

  private func hasAdminPermission(caller : Principal, permission : SpamProtection.AdminFunction) : async Bool {
    if (caller == DAOPrincipal or Principal.isController(caller)) { return true; };
    // call DAO canister to ask if caller is admin
    await (with timeout = 65) dao.hasAdminPermission(caller, permission);
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
   * Manually refresh ICPSwap pools from factory
   * Use this to pick up newly created pools
   */
  public shared ({ caller }) func admin_refreshICPSwapPools() : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    await updateICPSwapPools();
    #ok("ICPSwap pools refreshed. Found " # Nat.toText(Map.size(ICPswapPools)) # " pool mappings.")
  };



  /**
   * Query pending KongSwap claims for this treasury
   * Returns list of claims that can be recovered
   */
  public shared ({ caller }) func admin_getKongClaims() : async Result.Result<[swaptypes.ClaimsReply], Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    await (with timeout = 65) KongSwap.getPendingClaims(Principal.fromActor(this));
  };

  /**
   * Execute all pending KongSwap claims to recover tokens
   */
  public shared ({ caller }) func admin_executeKongClaims() : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    await* recoverKongswapClaims();
    #ok("Claim recovery completed");
  };

  /**
   * Query pending TACO failed swaps tracked for recovery
   */
  public shared ({ caller }) func admin_getTacoFailedSwaps() : async Result.Result<[(Text, Nat, Text, Int)], Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    let result = Vector.new<(Text, Nat, Text, Int)>();
    for ((key, (blockNum, tokenPrincipal, timestamp)) in Map.entries(tacoFailedSwapBlocks)) {
      Vector.add(result, (key, blockNum, Principal.toText(tokenPrincipal), timestamp));
    };
    #ok(Vector.toArray(result));
  };

  /**
   * Execute recovery of all pending TACO failed swaps
   */
  public shared ({ caller }) func admin_recoverTacoSwaps() : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    await* recoverTacoSwapFunds();
    #ok("TACO recovery completed. Remaining: " # Nat.toText(Map.size(tacoFailedSwapBlocks)));
  };

  //=========================================================================
  // LP ADMIN ENDPOINTS
  //=========================================================================

  // Emergency: remove ALL LP positions immediately and disable LP
  public shared ({ caller }) func admin_exitAllLP() : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    var removed = 0;
    for (pos in cachedLPPositions.vals()) {
      if (pos.liquidity > 0) {
        try {
          switch (pos.positionId) {
            case (?posId) {
              // V3 concentrated position — remove by positionId
              let result = await (with timeout = 65) TACOSwap.doRemoveConcentratedLiquidity(pos.token0, pos.token1, posId, pos.liquidity);
              switch (result) { case (#Ok(_)) { removed += 1 }; case (#Err(_)) {} };
            };
            case null {
              // V2 full-range position — remove by liquidity amount
              let result = await (with timeout = 65) TACOSwap.doRemoveLiquidity(pos.token0, pos.token1, pos.liquidity);
              switch (result) { case (#Ok(_)) { removed += 1 }; case (#Err(_)) {} };
            };
          };
        } catch (_) {};
      };
    };
    lpConfig := { lpConfig with enabled = false };
    logTreasuryAdminAction(caller, #LPEmergencyExit({ positionsRemoved = removed }), "Emergency LP exit: " # Nat.toText(removed) # " positions removed, LP disabled", true, null);
    #ok("Removed " # Nat.toText(removed) # " LP positions. LP disabled.");
  };

  // Remove a specific pool's LP position
  public shared ({ caller }) func admin_removeLPPosition(token0 : Text, token1 : Text, liquidityAmount : Nat) : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    try {
      let result = await (with timeout = 65) TACOSwap.doRemoveLiquidity(token0, token1, liquidityAmount);
      switch (result) {
        case (#Ok(ok)) {
          logTreasuryAdminAction(caller, #LPRemoveLiquidity({ pool = token0 # "/" # token1; details = "admin burned=" # Nat.toText(ok.liquidityBurned) }), "Admin LP removal: " # token0 # "/" # token1, true, null);
          #ok("Removed " # Nat.toText(ok.liquidityBurned) # " liquidity. Returned " # Nat.toText(ok.amount0) # "/" # Nat.toText(ok.amount1));
        };
        case (#Err(e)) { #err("Exchange error: " # debug_show(e)) };
      };
    } catch (e) { #err("Exception: " # Error.message(e)) };
  };

  // Update LP master configuration
  public shared ({ caller }) func admin_setLPConfig(update : {
    enabled : ?Bool; lpRatioBP : ?Nat; maxPoolShareBP : ?Nat; minLPValueICP : ?Nat;
    rebalanceThresholdBP : ?Nat; maxAdjustmentsPerCycle : ?Nat; priceDeviationMaxBP : ?Nat;
    nachosRedemptionBufferBP : ?Nat; nachosHighVolumeThresholdBP : ?Nat;
  }) : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    switch (update.lpRatioBP) { case (?v) { if (v > 10_000) return #err("lpRatioBP cannot exceed 10000") }; case null {} };
    switch (update.maxPoolShareBP) { case (?v) { if (v > 10_000) return #err("maxPoolShareBP cannot exceed 10000") }; case null {} };
    switch (update.nachosRedemptionBufferBP) { case (?v) { if (v > 10_000) return #err("nachosRedemptionBufferBP cannot exceed 10000") }; case null {} };
    switch (update.nachosHighVolumeThresholdBP) { case (?v) { if (v > 10_000) return #err("nachosHighVolumeThresholdBP cannot exceed 10000") }; case null {} };
    switch (update.rebalanceThresholdBP) { case (?v) { if (v > 10_000) return #err("rebalanceThresholdBP cannot exceed 10000") }; case null {} };
    switch (update.priceDeviationMaxBP) { case (?v) { if (v > 10_000) return #err("priceDeviationMaxBP cannot exceed 10000") }; case null {} };
    lpConfig := {
      enabled = switch (update.enabled) { case (?v) v; case null lpConfig.enabled };
      lpRatioBP = switch (update.lpRatioBP) { case (?v) v; case null lpConfig.lpRatioBP };
      maxPoolShareBP = switch (update.maxPoolShareBP) { case (?v) v; case null lpConfig.maxPoolShareBP };
      minLPValueICP = switch (update.minLPValueICP) { case (?v) v; case null lpConfig.minLPValueICP };
      rebalanceThresholdBP = switch (update.rebalanceThresholdBP) { case (?v) v; case null lpConfig.rebalanceThresholdBP };
      maxAdjustmentsPerCycle = switch (update.maxAdjustmentsPerCycle) { case (?v) v; case null lpConfig.maxAdjustmentsPerCycle };
      priceDeviationMaxBP = switch (update.priceDeviationMaxBP) { case (?v) v; case null lpConfig.priceDeviationMaxBP };
      nachosRedemptionBufferBP = switch (update.nachosRedemptionBufferBP) { case (?v) v; case null lpConfig.nachosRedemptionBufferBP };
      nachosHighVolumeThresholdBP = switch (update.nachosHighVolumeThresholdBP) { case (?v) v; case null lpConfig.nachosHighVolumeThresholdBP };
    };
    logTreasuryAdminAction(caller, #LPConfigUpdate({ details = "enabled=" # debug_show(lpConfig.enabled) # " ratio=" # Nat.toText(lpConfig.lpRatioBP) # "bp" }), "LP config updated", true, null);
    if (lpConfig.enabled) { startLPFeeClaimTimer<system>() };
    #ok("LP config updated. Enabled=" # debug_show(lpConfig.enabled));
  };

  // Set per-pool LP config (enable/disable specific pool, override ratios)
  public shared ({ caller }) func admin_setPoolLPConfig(poolKey : Text, config : { enabled : Bool; customLpRatioBP : ?Nat; customMaxPoolShareBP : ?Nat }) : async Result.Result<Text, Text> {
    if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    Map.set(lpPoolConfig, thash, poolKey, config);
    logTreasuryAdminAction(caller, #LPPoolConfigUpdate({ pool = poolKey; details = "enabled=" # debug_show(config.enabled) }), "Pool LP config: " # poolKey, true, null);
    #ok("Pool LP config updated for " # poolKey);
  };

  // Query LP status dashboard
  public shared query ({ caller }) func admin_getLPStatus() : async {
    config : { enabled : Bool; lpRatioBP : Nat; maxPoolShareBP : Nat; minLPValueICP : Nat; rebalanceThresholdBP : Nat; maxAdjustmentsPerCycle : Nat; nachosRedemptionBufferBP : Nat; nachosHighVolumeThresholdBP : Nat; priceDeviationMaxBP : Nat };
    positions : [{
      poolKey : Text; token0 : Text; token1 : Text;
      liquidity : Nat; backing0 : Nat; backing1 : Nat;
      unclaimedFees0 : Nat; unclaimedFees1 : Nat; shareOfPool : Float;
      // Cumulative IL tracking from treasuryLPPositions
      totalDeposited0 : Nat; totalDeposited1 : Nat;
      totalWithdrawn0 : Nat; totalWithdrawn1 : Nat;
      totalFeesEarned0 : Nat; totalFeesEarned1 : Nat;
      firstDeployTimestamp : Int; lastFeeClaimTimestamp : Int;
    }];
    poolConfigs : [(Text, { enabled : Bool; customLpRatioBP : ?Nat; customMaxPoolShareBP : ?Nat })];
    budgetUsage : [{ tokenSymbol : Text; usedICP : Nat; budgetICP : Nat }];
    pendingDepositsCount : Nat;
    inTransit : [(Text, Nat)];
    depositsInFlight : [(Text, Nat)];
    lastQueryAgeSeconds : Int;
  } {
    let posVec = Vector.new<{
      poolKey : Text; token0 : Text; token1 : Text;
      liquidity : Nat; backing0 : Nat; backing1 : Nat;
      unclaimedFees0 : Nat; unclaimedFees1 : Nat; shareOfPool : Float;
      totalDeposited0 : Nat; totalDeposited1 : Nat;
      totalWithdrawn0 : Nat; totalWithdrawn1 : Nat;
      totalFeesEarned0 : Nat; totalFeesEarned1 : Nat;
      firstDeployTimestamp : Int; lastFeeClaimTimestamp : Int;
    }>();
    for (pos in cachedLPPositions.vals()) {
      if (pos.positionType == #fullRange and pos.liquidity > 0) {
        let poolKey = normalizePoolKeyText(pos.token0, pos.token1);
        // Get cumulative tracking data from treasuryLPPositions
        let cumulative = switch (Map.get(treasuryLPPositions, thash, poolKey)) {
          case (?p) { p };
          case null {
            { token0Principal = Principal.fromText(pos.token0); token1Principal = Principal.fromText(pos.token1);
              totalDeposited0 = 0; totalDeposited1 = 0; totalWithdrawn0 = 0; totalWithdrawn1 = 0;
              totalFeesEarned0 = 0; totalFeesEarned1 = 0; firstDeployTimestamp = 0; lastFeeClaimTimestamp = 0 };
          };
        };
        Vector.add(posVec, {
          poolKey = poolKey;
          token0 = pos.token0; token1 = pos.token1;
          liquidity = pos.liquidity;
          backing0 = pos.token0Amount; backing1 = pos.token1Amount;
          unclaimedFees0 = pos.fee0; unclaimedFees1 = pos.fee1;
          shareOfPool = pos.shareOfPool;
          totalDeposited0 = cumulative.totalDeposited0; totalDeposited1 = cumulative.totalDeposited1;
          totalWithdrawn0 = cumulative.totalWithdrawn0; totalWithdrawn1 = cumulative.totalWithdrawn1;
          totalFeesEarned0 = cumulative.totalFeesEarned0; totalFeesEarned1 = cumulative.totalFeesEarned1;
          firstDeployTimestamp = cumulative.firstDeployTimestamp;
          lastFeeClaimTimestamp = cumulative.lastFeeClaimTimestamp;
        });
      };
    };

    // Budget usage per token
    let budgetVec = Vector.new<{ tokenSymbol : Text; usedICP : Nat; budgetICP : Nat }>();
    for ((token, used) in Map.entries(lpBudgetUsedPerToken)) {
      let sym = switch (Map.get(tokenDetailsMap, phash, token)) { case (?d) { d.tokenSymbol }; case null { Principal.toText(token) } };
      let alloc = switch (Map.get(currentAllocations, phash, token)) { case (?v) v; case null 0 };
      var totalPortfolio : Nat = 0;
      for ((_, d) in Map.entries(tokenDetailsMap)) {
        if (d.Active) { totalPortfolio += (d.balance * d.priceInICP) / (10 ** d.tokenDecimals) };
      };
      let budget = (alloc * totalPortfolio * lpConfig.lpRatioBP) / (10_000 * 10_000);
      Vector.add(budgetVec, { tokenSymbol = sym; usedICP = used; budgetICP = budget });
    };

    // In-transit tokens
    let transitVec = Vector.new<(Text, Nat)>();
    for ((token, (amount, _)) in Map.entries(lpTokensInTransit)) {
      let sym = switch (Map.get(tokenDetailsMap, phash, token)) { case (?d) { d.tokenSymbol }; case null { Principal.toText(token) } };
      Vector.add(transitVec, (sym, amount));
    };

    // In-flight deposits
    let flightVec = Vector.new<(Text, Nat)>();
    for ((token, amount) in Map.entries(lpDepositsInFlight)) {
      let sym = switch (Map.get(tokenDetailsMap, phash, token)) { case (?d) { d.tokenSymbol }; case null { Principal.toText(token) } };
      Vector.add(flightVec, (sym, amount));
    };

    {
      config = {
        enabled = lpConfig.enabled;
        lpRatioBP = lpConfig.lpRatioBP;
        maxPoolShareBP = lpConfig.maxPoolShareBP;
        minLPValueICP = lpConfig.minLPValueICP;
        rebalanceThresholdBP = lpConfig.rebalanceThresholdBP;
        maxAdjustmentsPerCycle = lpConfig.maxAdjustmentsPerCycle;
        nachosRedemptionBufferBP = lpConfig.nachosRedemptionBufferBP;
        nachosHighVolumeThresholdBP = lpConfig.nachosHighVolumeThresholdBP;
        priceDeviationMaxBP = lpConfig.priceDeviationMaxBP;
      };
      positions = Vector.toArray(posVec);
      poolConfigs = Iter.toArray(Map.entries(lpPoolConfig));
      budgetUsage = Vector.toArray(budgetVec);
      pendingDepositsCount = Map.size(lpPendingDeposits);
      inTransit = Vector.toArray(transitVec);
      depositsInFlight = Vector.toArray(flightVec);
      lastQueryAgeSeconds = if (lastLPQueryTimestamp > 0) { (now() - lastLPQueryTimestamp) / 1_000_000_000 } else { -1 };
    };
  };

public shared ({ caller }) func withdrawAllCyclesToSelf() : async Result.Result<Text, Text> {
      if ((await hasAdminPermission(caller, #recoverPoolBalances)) == false) {
      return #err("Unauthorized");
    };
    let cyclesLedger : actor {
        icrc1_balance_of : shared query { owner : Principal; subaccount : ?Blob } -> async Nat;
        withdraw : shared { to : Principal; amount : Nat } -> async { #Ok : Nat; #Err : Any };
    } = actor "um5iw-rqaaa-aaaaq-qaaba-cai";

    let self = Principal.fromActor(this);
    
    let balance = await (with timeout = 65) cyclesLedger.icrc1_balance_of({
        owner = self;
        subaccount = null;
    });
    
    if (balance == 0) return #ok("Withdrew " # Nat.toText(balance) # " cycles to self");
    
    ignore switch (await (with timeout = 65) cyclesLedger.withdraw({ to = self; amount = balance-100_000_000 })) {
        case (#Ok(_)) { balance };
        case (#Err(_)) { 0 };
    };
    #ok("Withdrew " # Nat.toText(balance) # " cycles to self");
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
      await* syncPriceWithDEX();
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
        //if (rebalanceState.status != #Idle) {
        //  ignore setTimer<system>(
        //    #seconds(0),
        //    func() : async () {
        //      ignore await stopRebalancing(?"Auto-stop via timer");
        //    },
        //  );
        //};
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
              await* syncPriceWithDEX();
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
      
      // Set next scheduled time for retry
      nextLongSyncTime := now() + (initialDelay * 1_000_000_000);
      
      longSyncTimerId := setTimer<system>(
        #seconds(initialDelay),
        func() : async () {
          // Record execution time
          lastLongSyncTime := now();
          
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

  // Start watchdog timer to monitor trading cycle health
  startWatchdogTimer<system>();

  // Auto-restart trading bot after upgrade if it was running before
  private func autoRestartTradingAfterUpgrade<system>() {
    // Small delay to allow system to stabilize after upgrade
    ignore setTimer<system>(
      #nanoseconds(5_000_000_000), // 5 second delay
      func() : async () {
        logger.info("UPGRADE", "Auto-restarting trading bot after canister upgrade", "autoRestartTradingAfterUpgrade");
        // Ensure LP backing and balances are fresh before trading resumes.
        // startAllSyncTimers(instant=true) already schedules updateBalances(),
        // but it may not have completed by the time this timer fires.
        try { await updateBalances() } catch (_) {
          logger.warn("UPGRADE", "updateBalances failed during auto-restart — will retry in trading cycle", "autoRestartTradingAfterUpgrade");
        };
        rebalanceState := {
          rebalanceState with
          status = #Trading;
          metrics = {
            rebalanceState.metrics with
            lastRebalanceAttempt = now();
          };
        };
        startTradingTimer<system>();
        logTreasuryAdminAction(this_canister_id(), #StartRebalancing, "Auto-restart after canister upgrade", true, null);
      },
    );
  };

  autoRestartTradingAfterUpgrade<system>();
  startTacoRecoveryTimer<system>();
  startLPFeeClaimTimer<system>();

  system func preupgrade() {
    // Log lifecycle: canister stop
    logTreasuryAdminAction(this_canister_id(), #CanisterStop, "Canister stopping (preupgrade)", true, null);
  };

  //=========================================================================
  // LOG ACCESS METHODS
  //=========================================================================

  /**
   * Get the last N log entries
   * Only accessible by master admin, controller, or DAO
   */
  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    //if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal) {
      logger.getLastLogs(count);
    //} else {
    //  [];
    //};
  };

  /**
   * Get the last N log entries for a specific context
   * Only accessible by master admin, controller, or DAO
   */
  public query ({ caller }) func getLogsByContext(context : Text, count : Nat) : async [Logger.LogEntry] {
    //if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal) {
      logger.getContextLogs(context, count);
    //} else {
    //  [];
    //};
  };

  /**
   * Get the last N log entries for a specific level
   * Only accessible by master admin, controller, or DAO
   */
  public query ({ caller }) func getLogsByLevel(level : Logger.LogLevel, count : Nat) : async [Logger.LogEntry] {
    //if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOPrincipal) {
      logger.getLogsByLevel(level, count);
    //} else {
    //  [];
    //};
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

  /**
   * System upgrade functions
   */
  system func postupgrade() {
    // After canister upgrade, all timers are invalidated
    // Reset trading status to Idle to prevent inconsistent state
    // where status shows Trading but no timer is actually running
    // The trading bot will be auto-restarted by autoRestartTradingAfterUpgrade
    // Log lifecycle: canister start
    logTreasuryAdminAction(this_canister_id(), #CanisterStart, "Canister started (postupgrade)", true, null);

      // Reset portfolio snapshot timer ID but preserve status
      // If it was running before upgrade, admin will need to restart it manually
      if (portfolioSnapshotStatus == #Running) {
        logger.info("UPGRADE", "Portfolio snapshots were running before upgrade - set to Stopped (restart manually)", "postupgrade");
        portfolioSnapshotStatus := #Stopped;
      };
      portfolioSnapshotTimerId := 0;
  };

  // ICPSwap Pool Query Functions (Bug 2 fix)

  /**
   * Get ICPSwap pool info for a specific token pair
   *
   * Returns the pool data if a pool exists for the given token pair,
   * or null if no pool is mapped.
   */
  public query func getICPSwapPoolInfo(tokenA : Principal, tokenB : Principal) : async ?{
    canisterId : Principal;
    token0 : Text;
    token1 : Text;
    fee : Nat;
  } {
    switch (Map.get(ICPswapPools, hashpp, (tokenA, tokenB))) {
      case (?pool) {
        ?{
          canisterId = pool.canisterId;
          token0 = pool.token0.address;
          token1 = pool.token1.address;
          fee = pool.fee;
        }
      };
      case null { null };
    }
  };

  /**
   * List all discovered ICPSwap pools
   *
   * Returns a unique list of all ICPSwap pools that have been discovered.
   * Since pools are stored bidirectionally, this filters out duplicates.
   */
  public query func listICPSwapPools() : async [{
    canisterId : Principal;
    token0 : Text;
    token1 : Text;
    fee : Nat;
  }] {
    // Use a Set to track seen canister IDs to avoid duplicates
    let seen = Map.new<Principal, Bool>();
    let result = Vector.new<{
      canisterId : Principal;
      token0 : Text;
      token1 : Text;
      fee : Nat;
    }>();

    for ((_, pool) in Map.entries(ICPswapPools)) {
      switch (Map.get(seen, phash, pool.canisterId)) {
        case null {
          Map.set(seen, phash, pool.canisterId, true);
          Vector.add(result, {
            canisterId = pool.canisterId;
            token0 = pool.token0.address;
            token1 = pool.token1.address;
            fee = pool.fee;
          });
        };
        case (?_) {}; // Already seen, skip
      };
    };

    Vector.toArray(result)
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };

  // Debug function for monitoring pending burns during development/testing
  public query func debugPendingBurnsInTreasury() : async [(Principal, Nat)] {
    Iter.toArray(Map.entries(pendingBurnsByToken))
  };

};

