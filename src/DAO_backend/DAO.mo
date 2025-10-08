import Principal "mo:base/Principal";
import Map "mo:map/Map";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Text "mo:base/Text";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Error "mo:base/Error";
import Debug "mo:base/Debug";
import Time "mo:base/Time";
import Timer "mo:base/Timer";
import Result "mo:base/Result";
import DAO_types "./dao_types";
import SpamProtection "../helper/spam_protection";
import Vector "mo:vector";
import Array "mo:base/Array";
import NeuronSnapshot "../neuron_snapshot/ns_types";
import ICRC1 "mo:icrc1/ICRC1";
import Treasury "../treasury/treasury_types";
import BTree "mo:stableheapbtreemap/BTree";
import Float "mo:base/Float";
import MintingVault "../minting_vault/minting_vault_types";
import TreasuryTypes "../treasury/treasury_types";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import CanisterIds "../helper/CanisterIds";
import calcHelp "../neuron_snapshot/VPcalculation";
import Cycles "mo:base/ExperimentalCycles";

shared (deployer) actor class ContinuousDAO() = this {

  private func this_canister_id() : Principal {
      Principal.fromActor(this);
  };

  // Core types
  type TokenDetails = DAO_types.TokenDetails;

  type Holdings = DAO_types.Holdings;

  type Allocation = DAO_types.Allocation;

  type SystemParameter = DAO_types.SystemParameter;

  type LogLevel = DAO_types.LogLevel;

  type LogEntry = DAO_types.LogEntry;

  // Voting power and allocation storage
  type UserState = DAO_types.UserState;

  type UserWalletData = DAO_types.UserWalletData;

  type SystemState = DAO_types.SystemState;

  type NeuronVP = NeuronSnapshot.NeuronVP;

  // Error types
  type UpdateError = DAO_types.UpdateError;
  type FollowError = DAO_types.FollowError;
  type UnfollowError = DAO_types.UnfollowError;
  type AuthorizationError = DAO_types.AuthorizationError;
  type SyncError = DAO_types.SyncError;
  type RefreshError = DAO_types.RefreshError;
  type TokenRegistrationError = DAO_types.TokenRegistrationError;
  type TokenType = DAO_types.TokenType;
  type NeuronAllocation = DAO_types.NeuronAllocation;
  type NeuronAllocationMap = DAO_types.NeuronAllocationMap;
  type HistoricBalanceAllocation = DAO_types.HistoricBalanceAllocation;
  
  // Admin action logging types
  type AdminActionType = DAO_types.AdminActionType;
  type AdminActionRecord = DAO_types.AdminActionRecord;
  type AdminActionsSinceResponse = DAO_types.AdminActionsSinceResponse;
  type AllocationChangesSinceResponse = DAO_types.AllocationChangesSinceResponse;
  type PastAllocationRecord = DAO_types.PastAllocationRecord;
  type FollowActionsSinceResponse = DAO_types.FollowActionsSinceResponse;
  type FollowRecord = DAO_types.FollowRecord;
  type UnfollowRecord = DAO_types.UnfollowRecord;
  type VotingPowerChangesSinceResponse = DAO_types.VotingPowerChangesSinceResponse;
  type UserVotingPowerRecord = DAO_types.UserVotingPowerRecord;
  type NeuronUpdatesSinceResponse = DAO_types.NeuronUpdatesSinceResponse;
  type NeuronRecord = DAO_types.NeuronRecord;
  type NeuronAllocationChangeRecord = DAO_types.NeuronAllocationChangeRecord;
  type NeuronAllocationChangesSinceResponse = DAO_types.NeuronAllocationChangesSinceResponse;
  
  // Constants
  let BASIS_POINTS_TOTAL = 10000;
  stable var SNAPSHOT_INTERVAL = 900_000_000_000; // 15 minutes in nanoseconds
  stable var MAX_PAST_ALLOCATIONS = 100;
  stable var MAX_FOLLOWERS = 500;
  stable var MAX_FOLLOWED = 3;
  stable var MAX_FOLLOW_DEPTH = 1;
  stable var MAX_TOTAL_UPDATES = 2000;
  stable var MAX_ALLOCATIONS_PER_DAY : Int = 5;
  stable var ALLOCATION_WINDOW = 86_400_000_000_000; // 24 hours in nanoseconds
  stable var MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY = 10;
  stable var MAX_NEURON_ALLOCATION_CHANGES = 10000; // Configurable circular buffer size

  stable var sns_governance_canister_id : ?Principal = ?Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai");

  // Spam protection
  let spamGuard = SpamProtection.SpamGuard(this_canister_id());

  //spamGuard.setTest(true); // delete this in production
  //MAX_ALLOCATIONS_PER_DAY := 500; // delete this in production
  //MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY := 500; // delete this in production
  //SNAPSHOT_INTERVAL := 86400000000000; // delete this in production
  spamGuard.setTest(false); 
  //MAX_ALLOCATIONS_PER_DAY := 5; // delete this in production
  //MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY := 10; // delete this in production
  //SNAPSHOT_INTERVAL := 900_000_000_000; // delete this in production

  // Logger
  let logger = Logger.Logger();

  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  let TREASURY_ID = canister_ids.getCanisterId(#treasury);
  //let NACHOS_ID = canister_ids.getCanisterId(#nachos);

  let NEURON_SNAPSHOT_ID = canister_ids.getCanisterId(#neuronSnapshot);

  //spamGuard.setAllowedCanisters([Principal.fromText("ywhqf-eyaaa-aaaad-qg6tq-cai")]);
  //spamGuard.setSelf(Principal.fromText("ywhqf-eyaaa-aaaad-qg6tq-cai"));

  spamGuard.setAllowedCanisters([this_canister_id(), // Really? TODO: See if we can remove!
                                 TREASURY_ID/*, NACHOS_ID*/]);

  spamGuard.setSelf(this_canister_id());

  // Admin other than controller that has access to logs
  stable var logAdmin = Principal.fromText("d7zib-qo5mr-qzmpb-dtyof-l7yiu-pu52k-wk7ng-cbm3n-ffmys-crbkz-nae");



//
  // State variables
  let { phash; bhash } = Map;

  var tacoAddress = Principal.fromText("kknbx-zyaaa-aaaaq-aae4a-cai");

  // Token info storage
  stable let tokenDetailsMap = Map.new<Principal, TokenDetails>();



  stable var activeTokenCount : Nat = 0;

  stable var balanceHistory = BTree.init<Int, HistoricBalanceAllocation>(?64);
  stable var lastBalanceHistoryUpdate : Int = 0;

  activeTokenCount := 0;
  for ((_, details) in Map.entries(tokenDetailsMap)) {
    if (details.Active) {
      activeTokenCount += 1;
    };
  };

  // User state storage
  stable var userStates = Map.new<Principal, UserState>();

  // Wallet data storage (separate from UserState to avoid migration issues)
  stable let userWalletData = Map.new<Principal, UserWalletData>();

  // Neuron allocation storage

  stable var neuronAllocationMap : NeuronAllocationMap = Map.new<Blob, NeuronAllocation>();

  // Circular buffer for neuron allocation changes (FIFO)
  stable var neuronAllocationChanges : [var ?NeuronAllocationChangeRecord] = Array.init<?NeuronAllocationChangeRecord>(MAX_NEURON_ALLOCATION_CHANGES, null);
  stable var neuronAllocationChangesHead : Nat = 0;
  stable var neuronAllocationChangesSize : Nat = 0;

  let emptyUserState : UserState = {
    allocations = [];
    votingPower = 0;
    lastVotingPowerUpdate = 0;
    lastAllocationUpdate = 0;
    pastAllocations = [];
    allocationFollows = [];
    allocationFollowedBy = [];
    followUnfollowActions = [];
    lastAllocationMaker = Principal.fromText("aaaaa-aa");
    neurons = [];
  };

  // Current aggregate allocation - Principal -> basis points
  stable let aggregateAllocation = Map.new<Principal, Nat>();

  // Snapshot related state
  stable var lastSnapshotId : Nat = 0;
  stable var lastSnapshotTime : Int = 0;
  stable var totalVotingPower : Nat = 0;
  // Track total voting power that has been allocated
  stable var allocatedVotingPower : Nat = 0;
  stable var totalVotingPowerByHotkeySetters : Nat = 0;

  // System state

  stable var systemState : SystemState = #Paused;

  // Admin action logging storage
  stable var adminActionCounter: Nat = 0;
  stable var adminActions = Vector.new<AdminActionRecord>();
  stable var maxAdminActionsStored: Nat = 10000; // Keep last 10k actions before archiving

  // Neuron snapshot interface
  let neuronSnapshot = actor (Principal.toText(NEURON_SNAPSHOT_ID)) : NeuronSnapshot.Self;

  //let treasury = actor ("z4is7-giaaa-aaaad-qg6uq-cai") : Treasury.Self;
  let treasury = actor (Principal.toText(TREASURY_ID)) : Treasury.Self;
  //let nachos = actor (Principal.toText(NACHOS_ID)) : Treasury.Self;
  //let mintingVault = actor ("ywhqf-eyaaa-aaaad-qg6tq-cai") : MintingVault.Self;
  let mintingVault = actor (Principal.toText(this_canister_id())) : MintingVault.Self;
  //let treasuryPrincipal = Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai");
  let treasuryPrincipal = TREASURY_ID;
  // Timer IDs
  private stable var snapshotTimerId : Nat = 0;

  private func isMasterAdmin(caller : Principal) : Bool {
    AdminAuth.isMasterAdmin(caller, canister_ids.isKnownCanister)
  };

  // Admin action logging functions
  private func logAdminAction(
    admin: Principal,
    actionType: AdminActionType,
    reason: Text,
    success: Bool,
    errorMessage: ?Text
  ) {
    adminActionCounter += 1;
    let record: AdminActionRecord = {
      id = adminActionCounter;
      timestamp = Time.now();
      admin = admin;
      actionType = actionType;
      reason = reason;
      success = success;
      errorMessage = errorMessage;
    };
    
    Vector.add(adminActions, record);

    // Keep only the most recent actions (before archiving takes over)
    // Use standard treasury pattern for consistency
    if (Vector.size(adminActions) > maxAdminActionsStored) {
      Vector.reverse(adminActions);
      while (Vector.size(adminActions) > maxAdminActionsStored) {
        ignore Vector.removeLast(adminActions);
      };
      Vector.reverse(adminActions);
    };
    
    // Still log to text logger for immediate debugging
    let actionDesc = getActionDescription(actionType);
    let statusText = if (success) "SUCCESS" else "FAILED";
    logger.info("AdminAction", 
      "[" # statusText # "] " # actionDesc # " by " # Principal.toText(admin) # 
      " - Reason: " # reason, "logAdminAction");
  };

  private func getActionDescription(actionType: AdminActionType): Text {
    switch (actionType) {
      case (#TokenAdd(details)) {
        let viaText = if (details.viaGovernance) " (via DAO Governance)" else " (via Admin Interface)";
        "Add token " # Principal.toText(details.token) # viaText;
      };
      case (#TokenRemove(details)) "Remove token " # Principal.toText(details.token);
      case (#TokenDelete(details)) "Delete token " # Principal.toText(details.token);
      case (#TokenPause(details)) "Pause token " # Principal.toText(details.token);
      case (#TokenUnpause(details)) "Unpause token " # Principal.toText(details.token);
      case (#SystemStateChange(details)) "Change system state from " # debug_show(details.oldState) # " to " # debug_show(details.newState);
      case (#ParameterUpdate(details)) "Update " # debug_show(details.parameter) # " from " # details.oldValue # " to " # details.newValue;
      case (#AdminPermissionGrant(details)) "Grant " # details.function # " permission to " # Principal.toText(details.targetAdmin);
      case (#AdminAdd(details)) "Add admin " # Principal.toText(details.newAdmin);
      case (#AdminRemove(details)) "Remove admin " # Principal.toText(details.removedAdmin);
      case (#CanisterStart) "Canister Start";
      case (#CanisterStop) "Canister Stop";
    };
  };

  // Log lifecycle events in admin log on upgrade/stop/start
  system func preupgrade() {
    // Record a canister stop (pre-upgrade) event in admin logs
    logAdminAction(this_canister_id(), #CanisterStop, "Canister stopping (preupgrade)", true, null);
  };

  system func postupgrade() {
    // Record a canister start (post-upgrade) event in admin logs
    logAdminAction(this_canister_id(), #CanisterStart, "Canister started (postupgrade)", true, null);
  };

  private func getDefaultTokenDetails(): TokenDetails {
    {
      Active = false;
      isPaused = false;
      epochAdded = Time.now();
      tokenName = "";
      tokenSymbol = "";
      tokenDecimals = 0;
      tokenTransferFee = 0;
      balance = 0;
      priceInICP = 0;
      priceInUSD = 0.0;
      tokenType = #ICRC12;
      pastPrices = [];
      lastTimeSynced = 0;
      pausedDueToSyncFailure = false;
    };
  };

  // Adds new token to DAO. Fetches metadata from token's ledger canister.
  // BACKWARD COMPATIBLE - delegates to addTokenWithReason with default reason
  public shared ({ caller }) func addToken(token : Principal, tokenType : TokenType) : async Result.Result<Text, AuthorizationError> {
    // Call the new implementation with default reason for DAO governance compatibility
    await addTokenWithReason(token, tokenType, "DAO Governance Vote");
  };

  // Adds new token to DAO with required reason. Fetches metadata from token's ledger canister.
  public shared ({ caller }) func addTokenWithReason(token : Principal, tokenType : TokenType, reason : Text) : async Result.Result<Text, AuthorizationError> {
    if (not isAdmin(caller, #addToken)) {
      //logAdminAction(caller, #TokenAdd({token; tokenType; viaGovernance = false}), reason, false, ?"Not authorized");
      return #err(#NotAdmin);
    };

    // Validate reason is not empty for explicit admin calls
    if (Text.size(reason) == 0) {
      return #err(#UnexpectedError("Reason is required for admin actions"));
    };

    // Determine if this is via governance (default reason) or direct admin interface
    let viaGovernance = reason == "DAO Governance Vote";

    try {
      let metadata = if (token == Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
        {
          tokenName = "ICP";
          tokenSymbol = "ICP";
          tokenDecimals = 8;
          tokenTransferFee = 10000;
          tokenType = #ICP;
        };
      } else {
        let ledger = actor (Principal.toText(token)) : ICRC1.FullInterface;
        let metadata = await ledger.icrc1_metadata();

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

        if (name == "" or symbol == "") {
          let errorMsg = "Invalid metadata: missing name or symbol";
          logAdminAction(caller, #TokenAdd({token; tokenType; viaGovernance}), reason, false, ?errorMsg);
          return #err(#UnexpectedError(errorMsg));
        };

        {
          tokenName = name;
          tokenSymbol = symbol;
          tokenDecimals = decimals;
          tokenTransferFee = fee;
          tokenType = tokenType;
        };
      };

      // Check if token already exists
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case (?details) {
          if (details.Active) {
            Map.set(tokenDetailsMap, phash, token, { metadata with Active = details.Active; isPaused = details.isPaused; epochAdded = details.epochAdded; balance = details.balance; priceInICP = details.priceInICP; priceInUSD = details.priceInUSD; pastPrices = details.pastPrices; lastTimeSynced = details.lastTimeSynced; pausedDueToSyncFailure = details.pausedDueToSyncFailure });
            let errorMsg = "Token already exists";
            logAdminAction(caller, #TokenAdd({token; tokenType; viaGovernance}), reason, false, ?errorMsg);
            return #err(#UnexpectedError(errorMsg));
          } else {
            Map.set(tokenDetailsMap, phash, token, { metadata with Active = true; isPaused = false; epochAdded = Time.now(); balance = 0; priceInICP = 0; priceInUSD = 0.0; pastPrices = []; lastTimeSynced = 0; pausedDueToSyncFailure = false });
          };
        };
        case null {
          Map.set(
            tokenDetailsMap,
            phash,
            token,
            {
              metadata with
              Active = true;
              isPaused = false;
              epochAdded = Time.now();
              balance = 0;
              priceInICP = 0;
              priceInUSD = 0.0;
              pastPrices = [];
              lastTimeSynced = 0;
              pausedDueToSyncFailure = false;
            },
          );
        };
      };
      
      activeTokenCount := 0;
      for ((_, details) in Map.entries(tokenDetailsMap)) {
        if (details.Active) {
          activeTokenCount += 1;
        };
      };

      try {
        ignore await treasury.syncTokenDetailsFromDAO(Iter.toArray(Map.entries(tokenDetailsMap)));
      } catch (e) {
        // Don't fail the whole operation for sync issues, just log
        logger.warn("Admin", "Failed to sync token details with treasury: " # Error.message(e), "addTokenWithReason");
      };

      // Log successful action
      let finalTokenDetails = switch (Map.get(tokenDetailsMap, phash, token)) {
        case (?details) { details };
        case null { getDefaultTokenDetails() }; // Should not happen but handle gracefully
      };
      
      logAdminAction(caller, #TokenAdd({token; tokenType; viaGovernance}), reason, true, null);
      #ok("Token added successfully");
      
    } catch (e) {
      let errorMsg = Error.message(e);
      logAdminAction(caller, #TokenAdd({token; tokenType; viaGovernance}), reason, false, ?errorMsg);
      #err(#UnexpectedError(errorMsg));
    };
  };

  // Chosen to not remove the token from the existing allocations of people, instead only remove it from aggregateAllocation.
  // This allows the user to get notified about the need for a new allocation, as one of the tokens is not active anymore.
  public shared ({ caller }) func removeToken(token : Principal, reason : Text) : async Result.Result<Text, AuthorizationError> {
    if (not isAdmin(caller, #removeToken)) {
      //logAdminAction(caller, #TokenRemove({token}), reason, false, ?"Not authorized");
      return #err(#NotAdmin);
    };

    // Validate reason is not empty
    if (Text.size(reason) == 0) {
      return #err(#UnexpectedError("Reason is required for admin actions"));
    };

    try {
      // Check if token exists
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case (null) {
          // Token doesn't exist - this is actually OK, not an error
          logAdminAction(caller, #TokenRemove({token}), reason, true, null);
          return #ok("Token doesn't exist");
        };
        case (?details) {
          // Remove from tokenDetailsMap
          Map.set(tokenDetailsMap, phash, token, { details with Active = false; isPaused = false; epochAdded = 0 });

          // Remove from aggregateAllocation if present
          if (Map.has(aggregateAllocation, phash, token)) {
            Map.delete(aggregateAllocation, phash, token);
          };
        };
      };
      
      activeTokenCount := 0;
      for ((_, details) in Map.entries(tokenDetailsMap)) {
        if (details.Active) {
          activeTokenCount += 1;
        };
      };
      
      try {
        ignore await treasury.syncTokenDetailsFromDAO(Iter.toArray(Map.entries(tokenDetailsMap)));
      } catch (e) {
        // Don't fail the whole operation for sync issues, just log
        logger.warn("Admin", "Failed to sync token details with treasury: " # Error.message(e), "removeToken");
      };

      logAdminAction(caller, #TokenRemove({token}), reason, true, null);
      #ok("Token removed successfully");
      
    } catch (e) {
      let errorMsg = Error.message(e);
      logAdminAction(caller, #TokenRemove({token}), reason, false, ?errorMsg);
      #err(#UnexpectedError(errorMsg));
    };
  };

  // Permanently delete a token from the trusted token list
  public shared ({ caller }) func deleteToken(token : Principal, reason : Text) : async Result.Result<Text, AuthorizationError> {
    if (not isAdmin(caller, #deleteToken)) {
      return #err(#NotAdmin);
    };

    // Validate reason is not empty
    if (Text.size(reason) == 0) {
      return #err(#UnexpectedError("Reason is required for admin actions"));
    };

    try {
      // Check if token exists
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case (null) {
          // Token doesn't exist - this is actually OK, not an error
          logAdminAction(caller, #TokenDelete({token}), reason, true, null);
          return #ok("Token doesn't exist");
        };
        case (?details) {
          // Actually delete from tokenDetailsMap
          Map.delete(tokenDetailsMap, phash, token);

          // Remove from aggregateAllocation if present
          if (Map.has(aggregateAllocation, phash, token)) {
            Map.delete(aggregateAllocation, phash, token);
          };
        };
      };
      
      activeTokenCount := 0;
      for ((_, details) in Map.entries(tokenDetailsMap)) {
        if (details.Active) {
          activeTokenCount += 1;
        };
      };
      
      try {
        ignore await treasury.syncTokenDetailsFromDAO(Iter.toArray(Map.entries(tokenDetailsMap)));
      } catch (e) {
        // Don't fail the whole operation for sync issues, just log
        logger.warn("Admin", "Failed to sync token details with treasury: " # Error.message(e), "deleteToken");
      };

      logAdminAction(caller, #TokenDelete({token}), reason, true, null);
      #ok("Token deleted successfully");
      
    } catch (e) {
      let errorMsg = Error.message(e);
      logAdminAction(caller, #TokenDelete({token}), reason, false, ?errorMsg);
      #err(#UnexpectedError(errorMsg));
    };
  };

  // Pausing means the trasury wont trade using that token. Seemed more natural to add the pausing logic here as it also removes and adds tokens.
  public shared ({ caller }) func pauseToken(token : Principal, reason: Text) : async Result.Result<Text, AuthorizationError> {
    if (not isAdmin(caller, #pauseToken)) {
      logger.warn("Admin", "Unauthorized pauseToken attempt by: " # Principal.toText(caller), "pauseToken");
      //logAdminAction(caller, #TokenPause({token}), reason, false, ?"Not authorized");
      return #err(#NotAdmin);
    };

    logger.info("Admin", "Pausing token " # Principal.toText(token) # " by " # Principal.toText(caller), "pauseToken");

    // Check if token exists
    switch (Map.get(tokenDetailsMap, phash, token)) {
      case (null) {
        logger.info("Admin", "Token " # Principal.toText(token) # " doesn't exist", "pauseToken");
        logAdminAction(caller, #TokenPause({token}), reason, true, null);
        return #ok("Token doesn't exist");
      };
      case (?details) {
        if (details.isPaused) {
          logger.info("Admin", "Token " # Principal.toText(token) # " is already paused", "pauseToken");
          logAdminAction(caller, #TokenPause({token}), reason, true, null);
          return #ok("Token is already paused");
        };
        if (not details.Active) {
          logger.warn("Admin", "Token " # Principal.toText(token) # " is not active", "pauseToken");
          logAdminAction(caller, #TokenPause({token}), reason, false, ?"Token is not active");
          return #err(#UnexpectedError("Token is not active"));
        };
        Map.set(tokenDetailsMap, phash, token, { details with isPaused = true });
        logger.info("Admin", "Token " # Principal.toText(token) # " paused successfully", "pauseToken");
      };
    };

    try {
      ignore await treasury.syncTokenDetailsFromDAO(Iter.toArray(Map.entries(tokenDetailsMap)));
      logger.info("Admin", "Synced token details with treasury", "pauseToken");
      logAdminAction(caller, #TokenPause({token}), reason, true, null);
    } catch (e) {
      logger.warn("Admin", "Failed to sync token details with treasury: " # Error.message(e), "pauseToken");
      logAdminAction(caller, #TokenPause({token}), reason, false, ?Error.message(e));
      return #err(#UnexpectedError("Failed to sync with treasury: " # Error.message(e)));
    };

    /*try {
      ignore await mintingVault.syncTokenDetailsFromDAO(Iter.toArray(Map.entries(tokenDetailsMap)));
      logger.info("Admin", "Synced token details with minting vault", "pauseToken");
    } catch (e) {
      logger.warn("Admin", "Failed to sync token details with minting vault: " # Error.message(e), "pauseToken");
    };*/

    #ok("Token paused successfully");
  };

  public shared ({ caller }) func unpauseToken(token : Principal, reason: Text) : async Result.Result<Text, AuthorizationError> {
    if (not isAdmin(caller, #unpauseToken)) {
      logger.warn("Admin", "Unauthorized unpauseToken attempt by: " # Principal.toText(caller), "unpauseToken");
      //logAdminAction(caller, #TokenUnpause({token}), reason, false, ?"Not authorized");
      return #err(#NotAdmin);
    };

    logger.info("Admin", "Unpausing token " # Principal.toText(token) # " by " # Principal.toText(caller), "unpauseToken");

    // Check if token exists
    switch (Map.get(tokenDetailsMap, phash, token)) {
      case (null) {
        logger.info("Admin", "Token " # Principal.toText(token) # " doesn't exist", "unpauseToken");
        logAdminAction(caller, #TokenUnpause({token}), reason, true, null);
        return #ok("Token doesn't exist");
      };
      case (?details) {
        if (not details.isPaused) {
          logger.info("Admin", "Token " # Principal.toText(token) # " is not paused", "unpauseToken");
          logAdminAction(caller, #TokenUnpause({token}), reason, true, null);
          return #ok("Token is not paused");
        };
        Map.set(tokenDetailsMap, phash, token, { details with isPaused = false });
        logger.info("Admin", "Token " # Principal.toText(token) # " unpaused", "unpauseToken");

        try {
          ignore await treasury.syncTokenDetailsFromDAO(Iter.toArray(Map.entries(tokenDetailsMap)));
          logger.info("Admin", "Synced token details with treasury", "unpauseToken");
          logAdminAction(caller, #TokenUnpause({token}), reason, true, null);
        } catch (e) {
          logger.warn("Admin", "Failed to sync token details with treasury: " # Error.message(e), "unpauseToken");
          logAdminAction(caller, #TokenUnpause({token}), reason, false, ?Error.message(e));
          return #err(#UnexpectedError("Failed to sync with treasury: " # Error.message(e)));
        };

        /*try {
          ignore await mintingVault.syncTokenDetailsFromDAO(Iter.toArray(Map.entries(tokenDetailsMap)));
          logger.info("Admin", "Synced token details with minting vault", "unpauseToken");
        } catch (e) {
          logger.warn("Admin", "Failed to sync token details with minting vault: " # Error.message(e), "unpauseToken");
        };*/
      };
    };

    #ok("Token unpaused successfully");
  };

  // Calculates voting power changes when allocations update. Handles empty allocations.
  // Returns array of (token, votingPowerDelta) pairs for aggregate updates.
  private func calculateAllocationDelta(
    oldAllocations : [Allocation],
    newAllocations : [Allocation],
    votingPower : Nat,
  ) : [(Principal, Int)] {
    let deltaMap = Map.new<Principal, Int>();

    // Subtract old allocations
    for (alloc in oldAllocations.vals()) {
      let currentDelta = switch (Map.get(deltaMap, phash, alloc.token)) {
        case (?d) { d };
        case null { 0 };
      };
      Map.set(deltaMap, phash, alloc.token, currentDelta - (alloc.basisPoints * votingPower));
    };

    // Add new allocations
    for (alloc in newAllocations.vals()) {
      let currentDelta = switch (Map.get(deltaMap, phash, alloc.token)) {
        case (?d) { d };
        case null { 0 };
      };
      Map.set(deltaMap, phash, alloc.token, currentDelta + (alloc.basisPoints * votingPower));
    };

    // Convert map to array
    Iter.toArray(Map.entries(deltaMap));
  };

  // Helper function to add neuron allocation change to circular buffer (FIFO)
  private func addNeuronAllocationChange(change: NeuronAllocationChangeRecord) {
    // Calculate the position to insert (circular buffer)
    let insertPos = (neuronAllocationChangesHead + neuronAllocationChangesSize) % MAX_NEURON_ALLOCATION_CHANGES;
    
    // Insert the new record
    neuronAllocationChanges[insertPos] := ?change;
    
    // Update size and head pointer
    if (neuronAllocationChangesSize < MAX_NEURON_ALLOCATION_CHANGES) {
      neuronAllocationChangesSize += 1;
    } else {
      // Buffer is full, move head pointer (FIFO - remove oldest)
      neuronAllocationChangesHead := (neuronAllocationChangesHead + 1) % MAX_NEURON_ALLOCATION_CHANGES;
    };
  };

  // Updates user's token allocation strategy. Validates total basis points = 10000.
  // Updates aggregate allocation and triggers updates for all followers.
  // Worst-case cost when 1000 max updates: 110 million cycles
  public shared ({ caller }) func updateAllocation(newAllocations : [Allocation]) : async Result.Result<Text, UpdateError> {
    if (not isAllowed(caller)) {
      return #err(#NotAllowed);
    };

    if (systemState != #Active) {
      return #err(#SystemInactive);
    };

    let initialUserState = switch (Map.get(userStates, phash, caller)) {
      case (?state) { state };
      case null {
        if (spamGuard.state.test and Map.size(neuronAllocationMap) > 0) {
          let randomNeuron = Iter.toArray(Map.entries(neuronAllocationMap))[Int.abs(Float.toInt(Float.fromInt(Time.now()) % Float.fromInt(Map.size(neuronAllocationMap))))];
          let neuronVotingPower = randomNeuron.1.votingPower;
          let testUser : UserState = {
            allocations = [];
            votingPower = neuronVotingPower;
            lastVotingPowerUpdate = 0;
            lastAllocationUpdate = 0;
            pastAllocations = [];
            allocationFollows = [];
            allocationFollowedBy = [];
            lastAllocationMaker = caller;
            followUnfollowActions = [];
            neurons = [{
              neuronId = randomNeuron.0;
              votingPower = neuronVotingPower;
            }];
          };
          Map.set(userStates, phash, caller, testUser);
          testUser;
        } else {
          return #err(#NoVotingPower);
        };
      };
    };

    // Allow empty allocation [], otherwise validate
    if (newAllocations.size() > 0) {
      if (not validateAllocations(newAllocations)) {
        return #err(#InvalidAllocation);
      };
    };

    let timenow = Time.now();

    // Check rate limit using pastAllocations
    let pastAllocSize = initialUserState.pastAllocations.size();
    let max_past_allocations = if (MAX_ALLOCATIONS_PER_DAY > 0) {
      Int.abs(MAX_ALLOCATIONS_PER_DAY) -1;
    } else { 0 };

    if (pastAllocSize >= max_past_allocations) {
      let recentAllocations = Array.subArray(
        initialUserState.pastAllocations,
        pastAllocSize - max_past_allocations,
        max_past_allocations,
      );

      if (recentAllocations.size() == max_past_allocations and recentAllocations[0].from > timenow - ALLOCATION_WINDOW) {
        let timeUntilNext = recentAllocations[0].from + ALLOCATION_WINDOW - timenow;
        return #err(#UnexpectedError("Rate limit exceeded. Try again in " # Int.toText(timeUntilNext / 1_000_000_000) # " seconds"));
      };
    };

    if (timenow > lastBalanceHistoryUpdate + 3_600_000_000_000) {
      // 1 hour in nanoseconds
      // Get current balance distribution
      let (currentBalances, totalWorthInICP, totalWorthInUSD) = calculateBalanceDistribution();

      // Get current allocation (reuse existing getAggregateAllocation logic)
      var totalAllocatedVP = 0;
      for (vp in Map.vals(aggregateAllocation)) {
        totalAllocatedVP += vp;
      };

      let currentAllocations = if (totalAllocatedVP == 0) { [] } else {
        let results = Vector.new<(Principal, Nat)>();
        for ((token, vp) in Map.entries(aggregateAllocation)) {
          if (vp > 0) {
            let basisPoints = (vp * 10000) / totalAllocatedVP;
            Vector.add(results, (token, basisPoints));
          };
        };
        Vector.toArray(results);
      };

      // Store in BTree
      ignore BTree.insert<Int, HistoricBalanceAllocation>(
        balanceHistory,
        Int.compare,
        timenow,
        {
          balances = currentBalances;
          allocations = currentAllocations;
          totalWorthInICP = totalWorthInICP;
          totalWorthInUSD = totalWorthInUSD;
        },
      );
      lastBalanceHistoryUpdate := timenow;
    };

    try {
      var totalUpdates = 0;

      // Function to handle state updates for a principal
      func updateStateForPrincipal(principal : Principal) : Result.Result<UserState, Text> {
        let userState = switch (Map.get(userStates, phash, principal)) {
          case (?state) { state };
          case null { return #err("User state not found") };
        };

        // Count total neuron-token updates that will be needed
        let numberOfNeurons = userState.neurons.size();

        if (totalUpdates + numberOfNeurons > MAX_TOTAL_UPDATES) {
          return #err(
            "Too many neurons to update at once (" # Nat.toText(numberOfNeurons) #
            "). This is usually caused by having many neurons or complex follow relationships. " #
            "Consider: 1) Reducing your neuron count by merging neurons, 2) Limiting your follow network, " #
            "or 3) Requesting a higher MAX_TOTAL_UPDATES limit from governance."
          );
        };
        totalUpdates += numberOfNeurons;

        // Update allocations for each neuron
        for (neuron in userState.neurons.vals()) {
          let existingAlloc = switch (Map.get(neuronAllocationMap, bhash, neuron.neuronId)) {
            case (?alloc) { alloc.allocations };
            case null { [] };
          };

          let deltas = calculateAllocationDelta(
            existingAlloc,
            newAllocations,
            neuron.votingPower,
          );

          // Update aggregate allocation with deltas
          for ((token, delta) in deltas.vals()) {
            let currentVP = switch (Map.get(aggregateAllocation, phash, token)) {
              case (?vp) { vp };
              case null { 0 };
            };

            let newVP = if (delta >= 0) {
              currentVP + Int.abs(delta) / 10000;
            } else {
              let d = Int.abs(delta) / 10000;
              if (d > currentVP) { 0 } else { currentVP - d };
            };

            Map.set(aggregateAllocation, phash, token, newVP);
          };

          // Update neuron allocation
          Map.set(
            neuronAllocationMap,
            bhash,
            neuron.neuronId,
            {
              neuron with
              allocations = newAllocations;
              lastUpdate = timenow;
              lastAllocationMaker = caller;
            },
          );

          // Log neuron allocation change to circular buffer
          let neuronAllocationChange: NeuronAllocationChangeRecord = {
            timestamp = timenow;
            neuronId = neuron.neuronId;
            changeType = #UserUpdate({userInitiated = true}); // User-initiated change
            oldAllocations = existingAlloc;
            newAllocations = newAllocations;
            votingPower = neuron.votingPower;
            maker = caller;
            reason = null; // No reason for regular user updates
          };
          addNeuronAllocationChange(neuronAllocationChange);
        };

        // Update user state
        let pastAllocationsSize = userState.pastAllocations.size();
        let newUserState = {
          userState with
          allocations = newAllocations;
          lastAllocationUpdate = timenow;
          lastAllocationMaker = caller;
          pastAllocations = if (userState.lastAllocationUpdate == 0) {
            userState.pastAllocations;
          } else if (pastAllocationsSize < MAX_PAST_ALLOCATIONS) {
            let a = Vector.fromArray<{ from : Int; to : Int; allocation : [Allocation]; allocationMaker : Principal }>(userState.pastAllocations);
            Vector.add(a, { from = userState.lastAllocationUpdate; to = timenow; allocation = userState.allocations; allocationMaker = userState.lastAllocationMaker });
            Vector.toArray(a);
          } else {
            let a = Vector.fromArray<{ from : Int; to : Int; allocation : [Allocation]; allocationMaker : Principal }>(userState.pastAllocations);
            Vector.add(a, { from = userState.lastAllocationUpdate; to = timenow; allocation = userState.allocations; allocationMaker = userState.lastAllocationMaker });
            Vector.reverse(a);
            ignore Vector.removeLast(a);
            Vector.reverse(a);
            Vector.toArray(a);
          };
        };

        Map.set(userStates, phash, principal, newUserState);
        #ok(newUserState);
      };

      // Update state for the caller first
      switch (updateStateForPrincipal(caller)) {
        case (#err(e)) { return #err(#UnexpectedError(e)) };
        case (#ok(_)) {
          var neuronCountTemp = 0;
          // Get followers recursively up to max depth
          let followersToUpdate = Vector.new<Principal>();
          let seen = Map.new<Principal, Null>();

          func addFollowers(principal : Principal) {

            // Initialize with current depth = 1 followers
            var currentDepthQueue = Vector.new<Principal>();
            Vector.add(currentDepthQueue, principal);

            // Process one depth at a time
            label a for (depth in Iter.range(1, MAX_FOLLOW_DEPTH)) {
              // Skip if we've reached the depth limit
              if (depth > MAX_FOLLOW_DEPTH) {
                break a;
              };

              // Process all followers at current depth
              let nextDepthQueue = Vector.new<Principal>();

              for (current in Vector.vals(currentDepthQueue)) {
                switch (Map.get(userStates, phash, current)) {
                  case (?state) {
                    // Process all followers of the current principal
                    for (follower in state.allocationFollowedBy.vals()) {
                      // Check if we've hit the update limit
                      if (totalUpdates + neuronCountTemp >= MAX_TOTAL_UPDATES) {
                        break a;
                      };

                      // Check if we've already seen this follower
                      switch (Map.get(seen, phash, follower.follow)) {
                        case (?_) { /* already processed */ };
                        case null {
                          // Mark as seen and add to update list
                          Map.set(seen, phash, follower.follow, null);
                          Vector.add(followersToUpdate, follower.follow);

                          // Get follower's user state for neuron count
                          switch (Map.get(userStates, phash, follower.follow)) {
                            case (?followerState) {
                              neuronCountTemp += followerState.neurons.size();

                              // Add to next depth queue if we haven't hit depth limit
                              if (depth < MAX_FOLLOW_DEPTH) {
                                Vector.add(nextDepthQueue, follower.follow);
                              };
                            };
                            case null { /* Skip if user state not found */ };
                          };
                        };
                      };
                    };
                  };
                  case null { /* Skip if user state not found */ };
                };
              };

              // If no users at next depth or we've hit update limit, stop processing
              if (Vector.size(nextDepthQueue) == 0 or totalUpdates + neuronCountTemp >= MAX_TOTAL_UPDATES) {
                break a;
              };

              // Prepare for next iteration
              currentDepthQueue := nextDepthQueue;
            };
          };

          addFollowers(caller);

          // Update followers
          for (follower in Vector.toArray(followersToUpdate).vals()) {
            ignore updateStateForPrincipal(follower);
          };

          #ok("Allocation updated successfully");
        };
      };
    } catch (e) {
      #err(#UnexpectedError(Error.message(e)));
    };
  };

  // Enables user to follow another user's allocation strategy. Limited by MAX_FOLLOW_DEPTH.
  // Updates both follower and followee states. Requires both users to have made allocations.
  public shared ({ caller }) func followAllocation(followee : Principal) : async Result.Result<Text, FollowError> {
    if (not isAllowed(caller)) {
      return #err(#NotAllowed);
    };

    if (systemState != #Active) {
      return #err(#SystemInactive);
    };

    if (caller == followee) {
      return #err(#FolloweeIsSelf);
    };

    let userStateFollowee = switch (Map.get(userStates, phash, followee)) {
      case (?state) { state };
      case null {
        if (spamGuard.state.test) {
          let testUser = {
            allocations = [];
            votingPower = 1000;
            lastVotingPowerUpdate = 0;
            lastAllocationUpdate = 0;
            pastAllocations = [];
            allocationFollows = [];
            allocationFollowedBy = [];
            lastAllocationMaker = caller;
            followUnfollowActions = [];
            neurons = [];
          };
          Map.set(userStates, phash, followee, testUser);
          testUser;
        } else {
          return #err(#FolloweeNotFound);
        };
      };
    };

    let userStateCaller = switch (Map.get(userStates, phash, caller)) {
      case (?state) { state };
      case null {
        if (spamGuard.state.test) {
          let testUser = {
            allocations = [];
            votingPower = 1000;
            lastVotingPowerUpdate = 0;
            lastAllocationUpdate = 0;
            pastAllocations = [];
            allocationFollows = [];
            allocationFollowedBy = [];
            lastAllocationMaker = caller;
            followUnfollowActions = [];
            neurons = [];
          };
          Map.set(userStates, phash, caller, testUser);
          testUser;
        } else {
          // Allowing users only to follow if they have set an allocation before. To stimulate some initial commitment
          return #err(#FollowerNotFound);
        };
      };
    };

    if (userStateFollowee.allocations.size() == 0 and userStateFollowee.pastAllocations.size() == 0) {
      return #err(#FolloweeNoAllocationYetMade);
    };

    if (userStateCaller.allocations.size() == 0 and userStateCaller.pastAllocations.size() == 0) {
      return #err(#FollowerNoAllocationYetMade);
    };

    if (userStateCaller.allocationFollows.size() >= MAX_FOLLOWED) {
      return #err(#FollowLimitReached);
    };

    if (userStateFollowee.allocationFollowedBy.size() >= MAX_FOLLOWERS) {
      return #err(#FolloweeLimitReached);
    };

    if (Array.find<{ follow : Principal; since : Int }>(userStateCaller.allocationFollows, func(f) { f.follow == followee }) != null) {
      return #err(#AlreadyFollowing);
    };
    let timenow = Time.now();

    let newFollowUnfollowActions = Vector.fromArray<Int>(Array.filter<Int>(userStateCaller.followUnfollowActions, func(f) { f > timenow - 86400000000000 }));

    if (userStateCaller.followUnfollowActions.size() >= MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY) {
      return #err(#FollowUnfollowLimitReached);
    };
    Vector.add(newFollowUnfollowActions, timenow);

    let follow = { follow = followee; since = timenow };
    let followBy = { follow = caller; since = timenow };

    let newFollowedBys = Vector.fromArray<{ follow : Principal; since : Int }>(userStateFollowee.allocationFollowedBy);
    Vector.add(newFollowedBys, followBy);
    let newFollowedBysArray = Vector.toArray(newFollowedBys);

    let newFollowed = Vector.fromArray<{ follow : Principal; since : Int }>(userStateCaller.allocationFollows);
    Vector.add(newFollowed, follow);
    let newFollowsArray = Vector.toArray(newFollowed);

    Map.set(userStates, phash, caller, { userStateCaller with allocationFollows = newFollowsArray; followUnfollowActions = Vector.toArray(newFollowUnfollowActions) });
    Map.set(userStates, phash, followee, { userStateFollowee with allocationFollowedBy = newFollowedBysArray });

    #ok("Allocation followed successfully");
  };

  public shared ({ caller }) func unfollowAllocation(followee : Principal) : async Result.Result<Text, UnfollowError> {
    if (not isAllowed(caller)) {
      return #err(#NotAllowed);
    };

    if (systemState != #Active) {
      return #err(#SystemInactive);
    };

    if (caller == followee) {
      return #err(#FolloweeIsSelf);
    };

    let userStateFollowee = switch (Map.get(userStates, phash, followee)) {
      case (?state) { state };
      case null {
        if (spamGuard.state.test) {
          let testUser = {
            allocations = [];
            votingPower = 1000;
            lastVotingPowerUpdate = 0;
            lastAllocationUpdate = 0;
            pastAllocations = [];
            allocationFollows = [];
            allocationFollowedBy = [];
            lastAllocationMaker = caller;
            followUnfollowActions = [];
            neurons = [];
          };
          Map.set(userStates, phash, followee, testUser);
          testUser;
        } else {
          return #err(#FolloweeNotFound);
        };
      };
    };

    let userStateCaller = switch (Map.get(userStates, phash, caller)) {
      case (?state) { state };
      case null {
        if (spamGuard.state.test) {
          let testUser = {
            allocations = [];
            votingPower = 1000;
            lastVotingPowerUpdate = 0;
            lastAllocationUpdate = 0;
            pastAllocations = [];
            allocationFollows = [];
            allocationFollowedBy = [];
            lastAllocationMaker = caller;
            followUnfollowActions = [];
            neurons = [];
          };
          Map.set(userStates, phash, caller, testUser);
          testUser;
        } else {
          return #err(#FollowerNotFound);
        };
      };
    };

    if (Array.find<{ follow : Principal; since : Int }>(userStateCaller.allocationFollows, func(f) { f.follow == followee }) == null) {
      return #err(#AlreadyUnfollowing);
    };
    let timenow = Time.now();
    let newFollowUnfollowActions = Vector.fromArray<Int>(Array.filter<Int>(userStateCaller.followUnfollowActions, func(f) { f > timenow - 86400000000000 }));

    if (userStateCaller.followUnfollowActions.size() >= MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY) {
      return #err(#FollowUnfollowLimitReached);
    };
    Vector.add(newFollowUnfollowActions, timenow);

    let newFollows = Array.filter<{ follow : Principal; since : Int }>(userStateCaller.allocationFollows, func(f) { f.follow != followee });
    let newFollowedBys = Array.filter<{ follow : Principal; since : Int }>(userStateFollowee.allocationFollowedBy, func(f) { f.follow != caller });

    Map.set(userStates, phash, caller, { userStateCaller with allocationFollows = newFollows; followUnfollowActions = Vector.toArray(newFollowUnfollowActions) });
    Map.set(userStates, phash, followee, { userStateFollowee with allocationFollowedBy = newFollowedBys });

    #ok("Allocation unfollowed successfully");
  };

  // Function to remove a follower
  public shared ({ caller }) func removeFollower(follower : Principal) : async Result.Result<Text, FollowError> {
    if (not isAllowed(caller)) {
      return #err(#NotAllowed);
    };

    if (systemState != #Active) {
      return #err(#SystemInactive);
    };

    // Get caller's state
    let userStateCaller = switch (Map.get(userStates, phash, caller)) {
      case (?state) { state };
      case null { return #err(#FolloweeNotFound) };
    };

    // Check if the specified principal is actually following the caller
    if (
      Array.find<{ follow : Principal; since : Int }>(
        userStateCaller.allocationFollowedBy,
        func(f) { f.follow == follower },
      ) == null
    ) {
      return #err(#UnexpectedError("This principal is not following you"));
    };

    // Get follower's state
    let userStateFollower = switch (Map.get(userStates, phash, follower)) {
      case (?state) { state };
      case null { return #err(#FollowerNotFound) };
    };

    // Check follow/unfollow daily limit
    let timenow = Time.now();
    let newFollowUnfollowActions = Vector.fromArray<Int>(
      Array.filter<Int>(
        userStateCaller.followUnfollowActions,
        func(f) { f > timenow - 86400000000000 },
      )
    );

    if (Vector.size(newFollowUnfollowActions) >= MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY) {
      return #err(#FollowUnfollowLimitReached);
    };
    Vector.add(newFollowUnfollowActions, timenow);

    // Remove follower from caller's follower list
    let newFollowedBys = Array.filter<{ follow : Principal; since : Int }>(
      userStateCaller.allocationFollowedBy,
      func(f) { f.follow != follower },
    );

    // Remove caller from follower's following list
    let newFollows = Array.filter<{ follow : Principal; since : Int }>(
      userStateFollower.allocationFollows,
      func(f) { f.follow != caller },
    );

    // Update both states
    Map.set(
      userStates,
      phash,
      caller,
      {
        userStateCaller with
        allocationFollowedBy = newFollowedBys;
        followUnfollowActions = Vector.toArray(newFollowUnfollowActions);
      },
    );

    Map.set(
      userStates,
      phash,
      follower,
      { userStateFollower with allocationFollows = newFollows },
    );

    #ok("Follower removed successfully");
  };

  // calculate balance distribution in basis points
  private func calculateBalanceDistribution() : ([(Principal, Nat)], Nat, Float) {
    var totalValue = 0;
    let tokenValues = Vector.new<(Principal, Nat)>();

    var icpUSDPrice : Float = 0.0;

    // Calculate total value in terms of ICP
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (Principal.toText(principal) == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
        icpUSDPrice := details.priceInUSD;
      };
      if (details.balance > 0 and details.priceInICP > 0) {
        let valueInICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        totalValue += valueInICP;
        Vector.add(tokenValues, (principal, valueInICP));
      };
    };

    // If no value, return empty array
    if (totalValue == 0) {
      return ([], 0, 0.0);
    };

    // Convert to basis points
    let results = Vector.new<(Principal, Nat)>();
    for ((principal, value) in Vector.vals(tokenValues)) {
      let basisPoints = (value * 10000) / totalValue;
      Vector.add(results, (principal, basisPoints));
    };

    let totalWorthInUSD = Float.fromInt(totalValue) * icpUSDPrice / (10.0 ** 8.0);

    (Vector.toArray(results), totalValue, totalWorthInUSD);
  };

  public query ({ caller }) func getHistoricBalanceAndAllocation(limit : Nat) : async [(Int, HistoricBalanceAllocation)] {
    if (not isAllowedQuery(caller)) {
      return [];
    };

    let result = BTree.scanLimit(
      balanceHistory,
      Int.compare,
      0, // Start bound
      Time.now(), // End bound
      #bwd, // Newest to oldest
      limit,
    );

    result.results;
  };

  // Starts/restarts snapshot timer with current interval.
  // Cancels existing timer if present. Called after interval updates.
  private func startSnapshotTimer<system>() : async* () {
    logger.info("Snapshot", "Starting snapshot timer with interval: " # Int.toText(SNAPSHOT_INTERVAL), "startSnapshotTimer");

    // Cancel existing timer if any
    if (snapshotTimerId != 0) {
      Timer.cancelTimer(snapshotTimerId);
      logger.info("Snapshot", "Cancelled existing timer: " # Nat.toText(snapshotTimerId), "startSnapshotTimer");
    };

    // Start new timer
    snapshotTimerId := Timer.setTimer<system>(
      #nanoseconds(SNAPSHOT_INTERVAL),
      updateSnapshot,
    );

    logger.info("Snapshot", "New timer started with ID: " # Nat.toText(snapshotTimerId), "startSnapshotTimer");
  };

  // Takes new neuron snapshot and updates voting power data.
  // Triggered by timer. Restarts timer after completion.
  private func updateSnapshot() : async () {
    logger.info("Snapshot", "Starting scheduled snapshot update", "updateSnapshot");

    try {
      let status = await neuronSnapshot.get_neuron_snapshot_status();
      logger.info("Snapshot", "Current snapshot status: " # debug_show (status), "updateSnapshot");

      switch (status) {
        case (#Ready) {
          logger.info("Snapshot", "Neuron snapshot canister is ready, taking new snapshot", "updateSnapshot");
          let result = await neuronSnapshot.take_neuron_snapshot();

          switch (result) {
            case (#Ok(id)) {
              logger.info("Snapshot", "Successfully started snapshot with ID: " # Nat.toText(id), "updateSnapshot");
              await pollSnapshotStatus(id);
            };
            case (#Err(err)) {
              logger.error("Snapshot", "Failed to start snapshot: " # debug_show (err), "updateSnapshot");
              await* startSnapshotTimer();
            };
          };
        };
        case (_) {
          logger.warn("Snapshot", "Snapshot already in progress, skipping this update cycle", "updateSnapshot");
          await* startSnapshotTimer();
          return;
        };
      };
    } catch (e) {
      logger.error("Snapshot", "Error in updateSnapshot: " # Error.message(e), "updateSnapshot");
      await* startSnapshotTimer();
    };
  };

  // Polls snapshot status until complete. Triggers voting power recalculation on success.
  // Retries on failure.
  private func pollSnapshotStatus(id : Nat) : async () {
    logger.info("Snapshot", "Polling status for snapshot ID: " # Nat.toText(id), "pollSnapshotStatus");

    try {
      let status = await neuronSnapshot.get_neuron_snapshot_status();
      logger.info("Snapshot", "Current status: " # debug_show (status), "pollSnapshotStatus");

      switch (status) {
        case (#Ready) {
          // Snapshot complete, verify it was successful
          switch (await neuronSnapshot.get_neuron_snapshot_info(id)) {
            case (?info) {
              switch (info.result) {
                case (#Ok) {
                  logger.info("Snapshot", "Snapshot " # Nat.toText(id) # " completed successfully", "pollSnapshotStatus");
                  lastSnapshotId := id;
                  lastSnapshotTime := Time.now();

                  logger.info("Snapshot", "Starting voting power recalculation", "pollSnapshotStatus");
                  await recalculateAllVotingPower();
                };
                case (#Err(err)) {
                  logger.error("Snapshot", "Snapshot " # Nat.toText(id) # " failed: " # debug_show (err), "pollSnapshotStatus");
                };
              };
            };
            case (null) {
              logger.error("Snapshot", "Could not find snapshot info for ID: " # Nat.toText(id), "pollSnapshotStatus");
            };
          };
          await* startSnapshotTimer();
        };
        case (_) {
          logger.info("Snapshot", "Snapshot still in progress, will check again in 30 seconds", "pollSnapshotStatus");
          // Check again in 30 seconds
          ignore Timer.setTimer<system>(
            #seconds(30),
            func() : async () {
              await pollSnapshotStatus(id);
            },
          );
        };
      };
    } catch (e) {
      logger.error("Snapshot", "Error polling snapshot status: " # Error.message(e), "pollSnapshotStatus");
      await* startSnapshotTimer();
    };
  };

  public shared ({ caller }) func admin_recalculateAllVotingPower(id : Nat) : async () {
    if (isAdmin(caller, #getLogs)) {
      lastSnapshotId := id;
      await recalculateAllVotingPower();
    } else {
      return;
    };
  };

  // Fetches latest neuron voting power data and updates all user states.
  // Gets entries from snapshot canister in chunks to manage 2MB query limit. Updates totalVotingPower and cachedVotingPowers.
  private func recalculateAllVotingPower() : async () {
    logger.info("Snapshot", "Starting voting power recalculation for snapshot: " # Nat.toText(lastSnapshotId), "recalculateAllVotingPower");

    try {
      // Get cumulative voting power from snapshot
      let cumulativeVP = switch (await neuronSnapshot.getCumulativeValuesAtSnapshot(?lastSnapshotId)) {
        case (?vp) {
          logger.info("Snapshot", "Received cumulative voting power: " # Nat.toText(vp.total_staked_vp), "recalculateAllVotingPower");
          vp;
        };
        case (null) {
          logger.error("Snapshot", "Failed to get cumulative voting power for snapshot: " # Nat.toText(lastSnapshotId), "recalculateAllVotingPower");
          return;
        };
      };

      // Get all voting power entries and combine into a single Vector
      let principalNeuronsVec = Vector.new<(Principal, [NeuronVP])>();

      // Get first page to determine total entries
      logger.info("Snapshot", "Fetching first page of neuron data", "recalculateAllVotingPower");
      switch (await neuronSnapshot.getNeuronDataForDAO(lastSnapshotId, 0, 39000)) {
        case (?{ entries; total_entries; stopped_at }) {
          logger.info("Snapshot", "Received " # Nat.toText(entries.size()) # " entries out of " # Nat.toText(total_entries), "recalculateAllVotingPower");
          Vector.addFromIter(principalNeuronsVec, entries.vals());

          switch (stopped_at) {
            case (null) {
              logger.info("Snapshot", "All data retrieved in first request", "recalculateAllVotingPower");
            };
            case (?stopped_at_number) {
              logger.info("Snapshot", "Need to fetch more data starting at index: " # Nat.toText(stopped_at_number), "recalculateAllVotingPower");

              // If more pages exist, fetch them
              let pageSize = 39000;
              var currentIndex = stopped_at_number;
              var isMoreData = true;
              var pageCount = 1;

              while isMoreData {
                logger.info("Snapshot", "Fetching additional page " # Nat.toText(pageCount) # " starting at index: " # Nat.toText(currentIndex), "recalculateAllVotingPower");

                switch (await neuronSnapshot.getNeuronDataForDAO(lastSnapshotId, currentIndex, pageSize)) {
                  case (?{ entries; stopped_at }) {
                    logger.info("Snapshot", "Received " # Nat.toText(entries.size()) # " entries in page " # Nat.toText(pageCount), "recalculateAllVotingPower");
                    Vector.addFromIter(principalNeuronsVec, entries.vals());

                    switch (stopped_at) {
                      case (null) {
                        logger.info("Snapshot", "All data retrieved", "recalculateAllVotingPower");
                        isMoreData := false;
                      };
                      case (?stopped_at_number) {
                        currentIndex := stopped_at_number;
                        pageCount += 1;
                      };
                    };
                  };
                  case (null) {
                    logger.error("Snapshot", "Failed to get page starting at " # Nat.toText(currentIndex), "recalculateAllVotingPower");
                    return;
                  };
                };
              };
            };
          };
        };
        case (null) {
          logger.error("Snapshot", "Failed to get first page of neuron data", "recalculateAllVotingPower");
          return;
        };
      };

      // Convert to array all at once
      let principalNeuronsData = Vector.toArray(principalNeuronsVec);
      logger.info("Snapshot", "Total entries assembled: " # Nat.toText(principalNeuronsData.size()), "recalculateAllVotingPower");

      totalVotingPower := cumulativeVP.total_staked_vp;
      totalVotingPowerByHotkeySetters := cumulativeVP.total_staked_vp_by_hotkey_setters;
      logger.info(
        "Snapshot",
        "Updated voting power totals - Total: " # Nat.toText(totalVotingPower) #
        ", HotKey Setters: " # Nat.toText(totalVotingPowerByHotkeySetters),
        "recalculateAllVotingPower",
      );

      let timenow = Time.now();
      // First loop - update user states and neurons
      let newUserStates = Map.new<Principal, UserState>();
      allocatedVotingPower := 0;

      // Create new neuron allocation map
      let newNeuronAllocationMap = Map.new<Blob, NeuronAllocation>();

      logger.info("Snapshot", "Clearing aggregate allocation", "recalculateAllVotingPower");
      Map.clear(aggregateAllocation);

      let neuronsSeen = Map.new<Blob, Null>();
      logger.info("Snapshot", "Processing principals and neurons", "recalculateAllVotingPower");

      // Combined single loop to handle both user states and neuron allocations
      for ((principal, neurons) in principalNeuronsData.vals()) {
        var principalVP = 0;

        // Create user state first
        label a for (neuron in neurons.vals()) {

          principalVP += neuron.votingPower;

          let hasAllocations = switch (Map.get(neuronAllocationMap, bhash, neuron.neuronId)) {
            case (?existingAlloc) { existingAlloc.allocations.size() > 0 and existingAlloc.lastAllocationMaker == principal };
            case null { false };
          };

          if (not hasAllocations or Map.has(neuronsSeen, bhash, neuron.neuronId)) {
            continue a;
          };

          Map.set(neuronsSeen, bhash, neuron.neuronId, null);

          // Handle neuron allocation in the same loop
          switch (Map.get(neuronAllocationMap, bhash, neuron.neuronId)) {
            case (?existingAlloc) {
              if (existingAlloc.allocations.size() > 0) {

                Debug.print("Adding Neuron ID: " # debug_show(neuron.neuronId) # ", Principal: " # debug_show(principal) # ", Existing allocations: " # debug_show(existingAlloc.allocations.size()) # ", Voting power: " # debug_show(neuron.votingPower));

                // Update neuron allocation with new voting power
                Map.set(
                  newNeuronAllocationMap,
                  bhash,
                  neuron.neuronId,
                  {
                    existingAlloc with
                    votingPower = neuron.votingPower;
                  },
                );

                // Update aggregate allocation
                for (alloc in existingAlloc.allocations.vals()) {
                  switch (Map.get(tokenDetailsMap, phash, alloc.token)) {
                    case (?details) {
                      if (details.Active) {
                        let currentVP = switch (Map.get(aggregateAllocation, phash, alloc.token)) {
                          case (?existing) { existing };
                          case null { 0 };
                        };
                        let newVP = currentVP + ((neuron.votingPower * alloc.basisPoints) / 10000);
                        Map.set(aggregateAllocation, phash, alloc.token, newVP);
                      };
                    };
                    case null {};
                  };
                };

                allocatedVotingPower += neuron.votingPower;
                
              } else {
                Map.set(
                  newNeuronAllocationMap,
                  bhash,
                  neuron.neuronId,
                  {
                    existingAlloc with
                    votingPower = neuron.votingPower;
                  },
                );
              };
            };
            case null {
              // New neuron, create empty allocation
              Map.set(
                newNeuronAllocationMap,
                bhash,
                neuron.neuronId,
                {
                  allocations = [];
                  lastUpdate = 0;
                  votingPower = neuron.votingPower;
                  lastAllocationMaker = spamGuard.getSelf();
                },
              );
            };
          };
        };

        // Update user state after processing all neurons
        let state = switch (Map.get(userStates, phash, principal)) {
          case (?existingState) {
            {
              existingState with
              neurons = neurons;
              votingPower = principalVP;
              lastVotingPowerUpdate = timenow;
            };
          };
          case null {
            {
              allocations = [];
              votingPower = principalVP;
              lastVotingPowerUpdate = timenow;
              lastAllocationUpdate = 0;
              pastAllocations = [];
              allocationFollows = [];
              allocationFollowedBy = [];
              lastAllocationMaker = spamGuard.getSelf();
              followUnfollowActions = [];
              neurons = neurons;
            };
          };
        };
        Map.set(newUserStates, phash, principal, state);
      };

      logger.info("Snapshot", "Checking for principals with no neurons", "recalculateAllVotingPower");
      // Process principals that have no neurons in this snapshot
      label a for ((user, _) in Map.entries(userStates)) {
        if (Map.has(newUserStates, phash, user)) {
          continue a;
        } else {
          let existingUserState = switch (Map.get(userStates, phash, user)) {
            case (?a) { a };
            case _ { continue a };
          };
          Map.set(
            newUserStates,
            phash,
            user,
            {
              existingUserState with neurons = [];
              votingPower = 0;
              lastVotingPowerUpdate = timenow;
            },
          );
        };
      };

      // Replace maps
      logger.info("Snapshot", "Updating user states and neuron allocations", "recalculateAllVotingPower");
      userStates := newUserStates;
      neuronAllocationMap := newNeuronAllocationMap;

      logger.info(
        "Snapshot",
        "Voting power recalculation complete - Neurons processed: " # Nat.toText(Map.size(neuronsSeen)) #
        ", Users updated: " # Nat.toText(Map.size(newUserStates)) #
        ", Allocated VP: " # Nat.toText(allocatedVotingPower),
        "recalculateAllVotingPower",
      );
    } catch (e) {
      logger.error("Snapshot", "Error in recalculateAllVotingPower: " # Error.message(e), "recalculateAllVotingPower");
    };
  };

  // Validates allocation array. Checks for duplicate tokens, active status, and total = 10000 basis points.
  // Returns false if any validation fails.
  private func validateAllocations(allocations : [Allocation]) : Bool {
    var total : Nat = 0;

    // Check if allocation size exceeds available active tokens
    if (allocations.size() > activeTokenCount) {
      return false;
    };

    // Track seen tokens to check for duplicates
    let seenTokens = Map.new<Principal, Null>();

    for (alloc in allocations.vals()) {
      // Check for duplicate tokens
      switch (Map.get(seenTokens, phash, alloc.token)) {
        case (?_) { return false }; // Duplicate token found
        case null { Map.set(seenTokens, phash, alloc.token, null) };
      };

      // Check token is active
      switch (Map.get(tokenDetailsMap, phash, alloc.token)) {
        case (?details) {
          if (not details.Active) {
            return false;
          };
        };
        case null { return false };
      };

      total += alloc.basisPoints;
    };

    total == BASIS_POINTS_TOTAL;
  };

  // Getting basis points for each token based on total voting power. This is easier than precalculating the basis points for each token during the updateAllocation call.
  // This makes it also possible to easily handle tokens that are paused in future.
  public query ({ caller }) func getAggregateAllocation() : async [(Principal, Nat)] {
    if (isAllowedQuery(caller)) {
      // First calculate total VP allocated across all tokens
      var totalAllocatedVP : Nat = 0;
      for (vp in Map.vals(aggregateAllocation)) {
        totalAllocatedVP += vp;
      };

      // If no VP is allocated, return empty array to avoid division by zero
      if (totalAllocatedVP == 0) {
        return [];
      };

      // Convert each token's VP to basis points
      let results = Vector.new<(Principal, Nat)>();
      for ((token, vp) in Map.entries(aggregateAllocation)) {
        let basisPoints = (vp * 10000) / totalAllocatedVP;
        Vector.add(results, (token, basisPoints));
      };

      Vector.toArray(results);
    } else {
      [];
    };
  };

  public query ({ caller }) func admin_getUserAllocation(principal : Principal) : async ?UserState {
    if (isAdmin(caller, #getLogs)) {
      Map.get(userStates, phash, principal);
    } else {
      null;
    };
  };

  public query ({ caller }) func admin_getUserAllocations() : async [(Principal, UserState)] {
    if (isAdmin(caller, #getLogs)) {
      Iter.toArray(Map.entries(userStates));
    } else {
      [];
    };
  };

  public query ({ caller }) func admin_getNeuronAllocations() : async [(Blob, NeuronAllocation)] {
    if (isAdmin(caller, #getLogs)) {
      Iter.toArray(Map.entries(neuronAllocationMap));
    } else {
      [];
    };
  };

  public query ({ caller }) func getUserAllocation() : async ?UserState {
    if (isAllowedQuery(caller)) {
      Map.get(userStates, phash, caller);
    } else {
      null;
    };
  };

  // Refreshes voting power for the calling user by fetching their current neurons from SNS governance
  // This allows users to see their voting power immediately after adding hotkeys without waiting for the next snapshot
  public shared ({ caller }) func refreshUserVotingPower() : async Result.Result<{
    oldVotingPower: Nat;
    newVotingPower: Nat;
    neuronsUpdated: Nat;
    aggregateUpdated: Bool;
  }, RefreshError> {
    if (not isAllowed(caller)) {
      return #err(#NotAllowed);
    };

    if (systemState != #Active) {
      return #err(#SystemInactive);
    };

    // Check if SNS governance canister is set
    let snsGovId = switch (sns_governance_canister_id) {
      case (?id) { id };
      case (null) {
        return #err(#SnsGovernanceError("SNS governance canister ID not set"));
      };
    };

    try {
      // Get current user state for comparison
      let currentUserState = switch (Map.get(userStates, phash, caller)) {
        case (?state) { state };
        case (null) {
          // User doesn't exist yet, create empty state for comparison
          emptyUserState;
        };
      };

      let oldVotingPower = currentUserState.votingPower;

      // Create SNS governance actor
      let snsGov = actor (Principal.toText(snsGovId)) : actor {
        list_neurons : shared query NeuronSnapshot.ListNeurons -> async NeuronSnapshot.ListNeuronsResponse;
        get_nervous_system_parameters : shared () -> async NeuronSnapshot.NervousSystemParameters;
      };

      // Fetch user's neurons from SNS governance
      logger.info("VotingPower", "Refreshing voting power for user: " # Principal.toText(caller), "refreshUserVotingPower");
      
      let neuronsResult = await snsGov.list_neurons({
        of_principal = ?caller;
        limit = 1000; // Should be enough for most users
        start_page_at = null;
      });

      if (neuronsResult.neurons.size() == 0) {
        logger.info("VotingPower", "No neurons found for user: " # Principal.toText(caller), "refreshUserVotingPower");
        return #err(#NoNeuronsFound);
      };

      // Get nervous system parameters for voting power calculation
      let params = await snsGov.get_nervous_system_parameters();
      
      // Create voting power calculator (reuse from neuron snapshot)
      let vpCalc = calcHelp.vp_calc();
      vpCalc.setParams(params);

      // Process neurons and calculate voting power
      let newNeurons = Vector.new<NeuronVP>();
      var newTotalVP : Nat = 0;
      var validNeuronsCount = 0;

      for (neuron in neuronsResult.neurons.vals()) {
        switch (neuron.id) {
          case (null) { /* Skip neurons without ID */ };
          case (?neuronId) {
            // Check if this neuron has the caller as hotkey (permission type [4, 3] or [3, 4])
            var isHotkey = false;
            for (permission in neuron.permissions.vals()) {
              switch (permission.principal) {
                case (?p) {
                  if (p == caller) {
                    isHotkey := true;
                  };
                };
                case (null) { /* Skip */ };
              };
            };

            if (isHotkey) {
              // Calculate voting power for this neuron
              let neuronDetails : NeuronSnapshot.NeuronDetails = {
                id = neuron.id;
                staked_maturity_e8s_equivalent = neuron.staked_maturity_e8s_equivalent;
                cached_neuron_stake_e8s = neuron.cached_neuron_stake_e8s;
                aging_since_timestamp_seconds = neuron.aging_since_timestamp_seconds;
                dissolve_state = neuron.dissolve_state;
                voting_power_percentage_multiplier = neuron.voting_power_percentage_multiplier;
              };

              let votingPower = vpCalc.getVotingPower(neuronDetails);
              if (votingPower > 0) {
                Vector.add(newNeurons, {
                  neuronId = neuronId.id;
                  votingPower = votingPower;
                });
                newTotalVP += votingPower;
                validNeuronsCount += 1;
              };
            };
          };
        };
      };

      let timenow = Time.now();

      // Calculate aggregate allocation changes
      var aggregateUpdated = false;
      
      // If user has existing allocations, we need to update the aggregate
      if (currentUserState.allocations.size() > 0) {
        // Remove old voting power from aggregate
        for (oldNeuron in currentUserState.neurons.vals()) {
          switch (Map.get(neuronAllocationMap, bhash, oldNeuron.neuronId)) {
            case (?neuronAlloc) {
              if (neuronAlloc.allocations.size() > 0) {
                for (alloc in neuronAlloc.allocations.vals()) {
                  let currentVP = switch (Map.get(aggregateAllocation, phash, alloc.token)) {
                    case (?vp) { vp };
                    case (null) { 0 };
                  };
                  let vpToRemove = (oldNeuron.votingPower * alloc.basisPoints) / 10000;
                  let newVP = if (vpToRemove > currentVP) { 0 } else { currentVP - vpToRemove };
                  Map.set(aggregateAllocation, phash, alloc.token, newVP);
                };
                aggregateUpdated := true;
              };
            };
            case (null) { /* Skip */ };
          };
        };

        // Add new voting power to aggregate
        for (newNeuron in Vector.vals(newNeurons)) {
          switch (Map.get(neuronAllocationMap, bhash, newNeuron.neuronId)) {
            case (?neuronAlloc) {
              // Update neuron allocation with new voting power
              Map.set(neuronAllocationMap, bhash, newNeuron.neuronId, {
                neuronAlloc with
                votingPower = newNeuron.votingPower;
              });

              if (neuronAlloc.allocations.size() > 0) {
                for (alloc in neuronAlloc.allocations.vals()) {
                  let currentVP = switch (Map.get(aggregateAllocation, phash, alloc.token)) {
                    case (?vp) { vp };
                    case (null) { 0 };
                  };
                  let vpToAdd = (newNeuron.votingPower * alloc.basisPoints) / 10000;
                  Map.set(aggregateAllocation, phash, alloc.token, currentVP + vpToAdd);
                };
                aggregateUpdated := true;
              };
            };
            case (null) {
              // Create new neuron allocation entry
              Map.set(neuronAllocationMap, bhash, newNeuron.neuronId, {
                allocations = currentUserState.allocations;
                lastUpdate = if (currentUserState.lastAllocationUpdate > 0) { currentUserState.lastAllocationUpdate } else { timenow };
                votingPower = newNeuron.votingPower;
                lastAllocationMaker = caller;
              });

              // Add to aggregate if user has allocations
              if (currentUserState.allocations.size() > 0) {
                for (alloc in currentUserState.allocations.vals()) {
                  let currentVP = switch (Map.get(aggregateAllocation, phash, alloc.token)) {
                    case (?vp) { vp };
                    case (null) { 0 };
                  };
                  let vpToAdd = (newNeuron.votingPower * alloc.basisPoints) / 10000;
                  Map.set(aggregateAllocation, phash, alloc.token, currentVP + vpToAdd);
                };
                aggregateUpdated := true;
              };
            };
          };
        };
      } else {
        // User has no allocations yet, just update neuron allocations with voting power
        for (newNeuron in Vector.vals(newNeurons)) {
          switch (Map.get(neuronAllocationMap, bhash, newNeuron.neuronId)) {
            case (?neuronAlloc) {
              Map.set(neuronAllocationMap, bhash, newNeuron.neuronId, {
                neuronAlloc with
                votingPower = newNeuron.votingPower;
              });
            };
            case (null) {
              Map.set(neuronAllocationMap, bhash, newNeuron.neuronId, {
                allocations = [];
                lastUpdate = 0;
                votingPower = newNeuron.votingPower;
                lastAllocationMaker = caller;
              });
            };
          };
        };
      };

      // Update user state
      let newUserState : UserState = {
        currentUserState with
        neurons = Vector.toArray(newNeurons);
        votingPower = newTotalVP;
        lastVotingPowerUpdate = timenow;
      };

      Map.set(userStates, phash, caller, newUserState);

      logger.info(
        "VotingPower", 
        "Updated voting power for " # Principal.toText(caller) # 
        " from " # Nat.toText(oldVotingPower) # 
        " to " # Nat.toText(newTotalVP) # 
        " (" # Nat.toText(validNeuronsCount) # " neurons)",
        "refreshUserVotingPower"
      );

      #ok({
        oldVotingPower = oldVotingPower;
        newVotingPower = newTotalVP;
        neuronsUpdated = validNeuronsCount;
        aggregateUpdated = aggregateUpdated;
      });

    } catch (e) {
      let errorMsg = "Failed to refresh voting power: " # Error.message(e);
      logger.error("VotingPower", errorMsg, "refreshUserVotingPower");
      #err(#UnexpectedError(errorMsg));
    };
  };

  public query ({ caller }) func getSnapshotInfo() : async ?{
    lastSnapshotId : Nat;
    lastSnapshotTime : Int;
    totalVotingPower : Nat;
  } {
    if (isAllowedQuery(caller)) {
      ?{
        lastSnapshotId;
        lastSnapshotTime;
        totalVotingPower;
      };
    } else {
      null;
    };
  };

  // Admin methods, kind of a placeholder
  public query func hasAdminPermission  (principal : Principal, function : SpamProtection.AdminFunction) : async Bool {
    isAdmin(principal, function);
  };

  public shared ({ caller }) func addAdmin(principal : Principal, reason : ?Text) : async Result.Result<Text, AuthorizationError> {
    let reasonText = switch (reason) {
      case (?r) r;
      case null "Admin added";
    };
    
    if (isAdmin(caller, #addAdmin)) {
      let admins = Vector.fromArray<Principal>(spamGuard.getAdmins());
      if (Vector.indexOf(principal, admins, Principal.equal) == null) {
        Vector.add(admins, principal);
        spamGuard.setAdmins(Vector.toArray(admins));
        logAdminAction(caller, #AdminAdd({newAdmin = principal}), reasonText, true, null);
        #ok("Admin added successfully");
      } else {
        logAdminAction(caller, #AdminAdd({newAdmin = principal}), reasonText, false, ?"Admin already exists");
        #err(#UnexpectedError("Admin already exists"));
      };
    } else {
      //logAdminAction(caller, #AdminAdd({newAdmin = principal}), reasonText, false, ?"Not authorized");
      #err(#NotAdmin);
    };
  };

  public shared ({ caller }) func removeAdmin(principal : Principal, reason : ?Text) : async Result.Result<Text, AuthorizationError> {
    let reasonText = switch (reason) {
      case (?r) r;
      case null "Admin removed";
    };
    
    if (isAdmin(caller, #removeAdmin)) {
      let oldAdmins = spamGuard.getAdmins();
      let admins = Vector.new<Principal>();
      for (admin in oldAdmins.vals()) {
        if (admin != principal) {
          Vector.add(admins, admin);
        };
      };
      spamGuard.setAdmins(Vector.toArray(admins));
      logAdminAction(caller, #AdminRemove({removedAdmin = principal}), reasonText, true, null);
      #ok("Admin removed successfully");
    } else {
      //logAdminAction(caller, #AdminRemove({removedAdmin = principal}), reasonText, false, ?"Not authorized");
      #err(#NotAdmin);
    };
  };

  public shared ({ caller }) func updateSystemState(newState : SystemState, reason: Text) : async Result.Result<Text, AuthorizationError> {
    if (not Principal.isController(caller) and not isAdmin(caller, #updateSystemState) and sns_governance_canister_id != ?caller) {
      //logAdminAction(caller, #SystemStateChange({oldState = systemState; newState}), reason, false, ?"Not authorized");
      return #err(#NotAdmin);
    };

    let oldState = systemState;
    let stateText = switch (newState) {
      case (#Active) { "Active" };
      case (#Paused) { "Paused" };
      case (#Emergency) { "Emergency" };
    };

    logger.info("Admin", "Updating system state to " # stateText # " by " # Principal.toText(caller), "updateSystemState");

    systemState := newState;
    logger.info("Admin", "System state updated to " # stateText # " successfully", "updateSystemState");
    logAdminAction(caller, #SystemStateChange({oldState; newState}), reason, true, null);
    #ok("System state updated successfully");
  };

  public query ({ caller }) func votingPowerMetrics() : async Result.Result<{ 
    totalVotingPower : Nat; 
    totalVotingPowerByHotkeySetters : Nat; 
    allocatedVotingPower : Nat;
    principalCount : Nat;
    neuronCount : Nat;
  }, AuthorizationError> {
    if (isAllowedQuery(caller)) {
      #ok({
        totalVotingPower;
        totalVotingPowerByHotkeySetters;
        allocatedVotingPower;
        principalCount = Map.size(userStates);
        neuronCount = Map.size(neuronAllocationMap);
      });
    } else {
      #err(#NotAdmin);
    };
  };

  // Query function to get followers with neuron counts
  public query ({ caller }) func getFollowersWithNeuronCounts() : async [(Principal, Nat)] {
    if (isAllowedQuery(caller)) {
      // Get caller's state
      switch (Map.get(userStates, phash, caller)) {
        case (?state) {
          let followerCounts = Vector.new<(Principal, Nat)>();

          // Process each follower
          for (follower in state.allocationFollowedBy.vals()) {
            switch (Map.get(userStates, phash, follower.follow)) {
              case (?followerState) {
                Vector.add(followerCounts, (follower.follow, followerState.neurons.size()));
              };
              case null {
                Vector.add(followerCounts, (follower.follow, 0));
              };
            };
          };

          // Sort the result by neuron count (high to low)
          Array.sort<(Principal, Nat)>(
            Vector.toArray(followerCounts),
            func((_, countA) : (Principal, Nat), (_, countB) : (Principal, Nat)) : {
              #less;
              #equal;
              #greater;
            } {
              if (countA > countB) #less else if (countA < countB) #greater else #equal;
            },
          );
        };
        case null { [] };
      };
    } else {
      [];
    };
  };

  private func isAllowed(principal : Principal) : Bool {
    if (isMasterAdmin(principal)) { return true; };
    switch (spamGuard.isAllowed(principal, sns_governance_canister_id)) {
      case (1) { return true };
      case (_) { return false };
    };
  };

  private func isAllowedQuery(principal : Principal) : Bool {
    if (isMasterAdmin(principal)) { return true; };
    switch (spamGuard.isAllowedQuery(principal)) {
      case (1) { return true };
      case (_) { return false };
    };
  };

  private func isAdmin(principal : Principal, function : SpamProtection.AdminFunction) : Bool {
    if (isMasterAdmin(principal) or Principal.isController(principal) or (sns_governance_canister_id == ?principal)) {
      return true;
    } else {
      spamGuard.isAdmin(principal, function, sns_governance_canister_id);
    };
  };

  // Grants time-limited admin permissions for specific functions.
  // Only callable by canister controllers. Duration in days.
  public shared ({ caller }) func grantAdminPermission(
    admin : Principal,
    function : SpamProtection.AdminFunction,
    durationDays : Nat,
  ) : async Result.Result<Text, AuthorizationError> {
    logger.info("Admin", "Admin permission request from " # Principal.toText(caller) # " for " # Principal.toText(admin), "grantAdminPermission");

    if (not Principal.isController(caller) and sns_governance_canister_id != ?caller) {
      logger.warn("Admin", "Unauthorized attempt to grant admin permission by: " # Principal.toText(caller), "grantAdminPermission");
      return #err(#NotAdmin);
    };

    let functionStr = debug_show (function);
    logger.info("Admin", "Granting permission for function: " # functionStr # " to " # Principal.toText(admin) # " for " # Nat.toText(durationDays) # " days", "grantAdminPermission");

    if (spamGuard.grantAdminPermission(caller, admin, function, durationDays, sns_governance_canister_id)) {
      logger.info("Admin", "Permission granted successfully", "grantAdminPermission");
      #ok("Permission granted successfully");
    } else {
      logger.error("Admin", "Failed to grant permission", "grantAdminPermission");
      #err(#UnexpectedError("Failed to grant permission"));
    };
  };

  // Query admin permissions
  public query ({ caller }) func getAdminPermissions() : async [(Principal, [SpamProtection.AdminPermission])] {
    if (isAllowedQuery(caller)) {
      spamGuard.getAdminPermissions();
    } else {
      [];
    };
  };

  // Updates system configuration parameters with bounds checking.
  // Affects follow depth (1-3), max followers (50-5000), past allocations (20-500), snapshot interval (10m-48h).
  public shared ({ caller }) func updateSystemParameter(param : SystemParameter, reason : ?Text) : async Result.Result<Text, AuthorizationError> {
    let reasonText = switch (reason) {
      case (?r) r;
      case null "System parameter updated";
    };
    
    if (not isAdmin(caller, #updateSystemParameter)) {
      // Log the failed attempt first
      let paramText = switch (param) {
        case (#FollowDepth(v)) "FollowDepth: " # Nat.toText(v);
        case (#LogAdmin(v)) "LogAdmin: " # Principal.toText(v);
        case (#MaxFollowed(v)) "MaxFollowed: " # Nat.toText(v);
        case (#MaxFollowUnfollowActionsPerDay(v)) "MaxFollowUnfollowActionsPerDay: " # Nat.toText(v);
        case (#MaxAllocationsPerDay(v)) "MaxAllocationsPerDay: " # Int.toText(v);
        case (#AllocationWindow(v)) "AllocationWindow: " # Int.toText(v);
        case (#MaxFollowers(v)) "MaxFollowers: " # Nat.toText(v);
        case (#MaxTotalUpdates(v)) "MaxTotalUpdates: " # Nat.toText(v);
        case (#MaxPastAllocations(v)) "MaxPastAllocations: " # Nat.toText(v);
        case (#SnapshotInterval(v)) "SnapshotInterval: " # Int.toText(v);
      };
      //logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = "N/A"; newValue = paramText}), reasonText, false, ?"Not authorized");
      return #err(#NotAdmin);
    };

    switch (param) {
      case (#FollowDepth(newDepth)) {
        if (newDepth < 1 or newDepth > 3) {
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(MAX_FOLLOW_DEPTH); newValue = Nat.toText(newDepth)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Follow depth must be between " #
              Nat.toText(1) # " and " #
              Nat.toText(3)
            )
          );
        };
        let oldValue = MAX_FOLLOW_DEPTH;
        MAX_FOLLOW_DEPTH := newDepth;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(oldValue); newValue = Nat.toText(newDepth)}), reasonText, true, null);
        logger.info("Admin", "Follow depth updated to " # Nat.toText(newDepth), "updateSystemParameter");
        #ok("Follow depth updated to " # Nat.toText(newDepth));
      };

      case (#LogAdmin(newLogAdmin)) {
        let oldValue = logAdmin;
        logAdmin := newLogAdmin;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Principal.toText(oldValue); newValue = Principal.toText(newLogAdmin)}), reasonText, true, null);
        logger.info("Admin", "Log admin updated to " # Principal.toText(newLogAdmin), "updateSystemParameter");
        try {
          await neuronSnapshot.setLogAdmin(newLogAdmin);
        } catch (e) {
          logger.error("Admin", "Error setting log admin: " # Error.message(e), "updateSystemParameter");
        };
        #ok("Log admin updated to " # Principal.toText(newLogAdmin));
      };

      case (#MaxFollowed(newMax)) {
        if (newMax < 1 or newMax > 10) {
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(MAX_FOLLOWED); newValue = Nat.toText(newMax)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError("Max followed must be between " # Nat.toText(1) # " and " # Nat.toText(10))
          );
        };
        let oldValue = MAX_FOLLOWED;
        MAX_FOLLOWED := newMax;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(oldValue); newValue = Nat.toText(newMax)}), reasonText, true, null);
        logger.info("Admin", "Max followed updated to " # Nat.toText(newMax), "updateSystemParameter");
        #ok("Max followed updated to " # Nat.toText(newMax));
      };

      case (#MaxFollowUnfollowActionsPerDay(newMax)) {
        if (newMax < 9 or newMax > 100) {
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY); newValue = Nat.toText(newMax)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Max follow/unfollow actions per day must be between " #
              Nat.toText(9) # " and " #
              Nat.toText(100)
            )
          );
        };
        let oldValue = MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY;
        MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY := newMax;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(oldValue); newValue = Nat.toText(newMax)}), reasonText, true, null);
        logger.info("Admin", "Max follow/unfollow actions per day updated to " # Nat.toText(newMax), "updateSystemParameter");
        #ok("Max follow/unfollow actions per day updated to " # Nat.toText(newMax));
      };

      case (#MaxAllocationsPerDay(newMax)) {
        if (newMax < 1 or newMax > 10) {
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Int.toText(MAX_ALLOCATIONS_PER_DAY); newValue = Int.toText(newMax)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Max allocations per day must be between " #
              Nat.toText(1) # " and " #
              Nat.toText(10)
            )
          );
        };
        let oldValue = MAX_ALLOCATIONS_PER_DAY;
        MAX_ALLOCATIONS_PER_DAY := newMax;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Int.toText(oldValue); newValue = Int.toText(newMax)}), reasonText, true, null);
        logger.info("Admin", "Max allocations per day updated to " # Int.toText(newMax), "updateSystemParameter");
        #ok("Max allocations per day updated to " # Int.toText(newMax));
      };

      case (#AllocationWindow(newWindow)) {
        if (newWindow < 3600000000000 or newWindow > 7 * 86400000000000) {
          // 1 hour to 7 days
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Int.toText(ALLOCATION_WINDOW); newValue = Int.toText(newWindow)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Allocation window must be between " #
              Nat.toText(3600000000000) # " and " #
              Nat.toText(7 * 86400000000000)
            )
          );
        };
        let oldValue = ALLOCATION_WINDOW;
        ALLOCATION_WINDOW := newWindow;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Int.toText(oldValue); newValue = Int.toText(newWindow)}), reasonText, true, null);
        logger.info("Admin", "Allocation window updated to " # Int.toText(newWindow), "updateSystemParameter");
        #ok("Allocation window updated to " # Int.toText(newWindow));
      };

      case (#MaxFollowers(newMax)) {
        if (newMax < 50 or newMax > 5000) {
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(MAX_FOLLOWERS); newValue = Nat.toText(newMax)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Max followers must be between " #
              Nat.toText(50) # " and " #
              Nat.toText(5000)
            )
          );
        };
        let oldValue = MAX_FOLLOWERS;
        MAX_FOLLOWERS := newMax;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(oldValue); newValue = Nat.toText(newMax)}), reasonText, true, null);
        logger.info("Admin", "Max followers updated to " # Nat.toText(newMax), "updateSystemParameter");
        #ok("Max followers updated to " # Nat.toText(newMax));
      };

      case (#MaxTotalUpdates(newMax)) {
        if (newMax < 200 or newMax > 10000) {
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(MAX_TOTAL_UPDATES); newValue = Nat.toText(newMax)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Max total updates must be between " #
              Nat.toText(10) # " and " #
              Nat.toText(10000)
            )
          );
        };
        let oldValue = MAX_TOTAL_UPDATES;
        MAX_TOTAL_UPDATES := newMax;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(oldValue); newValue = Nat.toText(newMax)}), reasonText, true, null);
        logger.info("Admin", "Max total updates updated to " # Nat.toText(newMax), "updateSystemParameter");
        #ok("Max total updates updated to " # Nat.toText(newMax));
      };

      case (#MaxPastAllocations(newMax)) {
        if (newMax < 20 or newMax > 500) {
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(MAX_PAST_ALLOCATIONS); newValue = Nat.toText(newMax)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Max past allocations must be between " #
              Nat.toText(20) # " and " #
              Nat.toText(500)
            )
          );
        };
        let oldValue = MAX_PAST_ALLOCATIONS;
        MAX_PAST_ALLOCATIONS := newMax;
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Nat.toText(oldValue); newValue = Nat.toText(newMax)}), reasonText, true, null);
        logger.info("Admin", "Max past allocations updated to " # Nat.toText(newMax), "updateSystemParameter");
        #ok("Max past allocations updated to " # Nat.toText(newMax));
      };

      case (#SnapshotInterval(newInterval)) {
        if (newInterval < 600_000_000_000 or newInterval > 172_800_000_000_000) {
          // 10 minutes to 48 hours
          logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Int.toText(SNAPSHOT_INTERVAL); newValue = Int.toText(newInterval)}), reasonText, false, ?"Value out of range");
          return #err(
            #UnexpectedError(
              "Snapshot interval must be between " #
              Int.toText(600_000_000_000) # " and " #
              Int.toText(172_800_000_000_000)
            )
          );
        };
        let oldValue = SNAPSHOT_INTERVAL;
        SNAPSHOT_INTERVAL := newInterval;
        ignore startSnapshotTimer();
        logAdminAction(caller, #ParameterUpdate({parameter = param; oldValue = Int.toText(oldValue); newValue = Int.toText(newInterval)}), reasonText, true, null);
        logger.info("Admin", "Snapshot interval updated to " # Int.toText(newInterval), "updateSystemParameter");
        #ok("Snapshot interval updated to " # Int.toText(newInterval));
      };
    };
  };

  /**
 * Update treasury rebalance configuration
 *
 * Allows configuration of trading intervals, sizes, and safety limits
 * Only callable by admins with the updateTreasuryConfig permission.
 */
  public shared ({ caller }) func updateTreasuryConfig(updates : TreasuryTypes.UpdateConfig, rebalanceState : ?Bool, reason : ?Text) : async Result.Result<Text, AuthorizationError> {
    if (not isAdmin(caller, #updateTreasuryConfig)) {
      return #err(#NotAdmin);
    };

    let treasury = actor (Principal.toText(treasuryPrincipal)) : TreasuryTypes.Self;

    try {
      let result = await treasury.updateRebalanceConfig(updates, rebalanceState, reason);

      switch (result) {
        case (#ok(message)) {
          #ok(message);
        };
        case (#err(error)) {
          // Convert the treasury error to DAO error format
          switch (error) {
            case (#NotDAO) {
              #err(#UnexpectedError("Not DAO"));
            };
            case (#UnexpectedError(message)) {
              #err(#UnexpectedError(message));
            };
          };
        };
      };
    } catch (e) {
      #err(#UnexpectedError("Error calling treasury: " # Error.message(e)));
    };

  };

  public query ({ caller }) func getSystemParameters() : async [SystemParameter] {
    if (isAllowedQuery(caller)) {
      [
        #FollowDepth(MAX_FOLLOW_DEPTH),
        #MaxFollowed(MAX_FOLLOWED),
        #MaxFollowUnfollowActionsPerDay(MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY),
        #MaxAllocationsPerDay(MAX_ALLOCATIONS_PER_DAY),
        #AllocationWindow(ALLOCATION_WINDOW),
        #MaxFollowers(MAX_FOLLOWERS),
        #MaxTotalUpdates(MAX_TOTAL_UPDATES),
        #MaxPastAllocations(MAX_PAST_ALLOCATIONS),
        #SnapshotInterval(SNAPSHOT_INTERVAL),
      ];
    } else {
      [];
    };
  };
  public query ({ caller }) func getTokenDetails() : async [(Principal, TokenDetails)] {
    if (isAllowedQuery(caller)) {
      Iter.toArray(Map.entries(tokenDetailsMap));
    } else {
      [];
    };
  };

  // Admin method to update spam parameters
  public shared ({ caller }) func updateSpamParameters(
    params : {
      allowedCalls : ?Nat;
      allowedSilentWarnings : ?Nat;
      timeWindowSpamCheck : ?Int;
    }
  ) : async Result.Result<Text, AuthorizationError> {
    if (isAdmin(caller, #updateSpamParameters)) {
      spamGuard.updateSpamParameters(params);
      #ok("Spam parameters updated successfully");
    } else {
      #err(#NotAdmin);
    };
  };

  private func addSnapshotUpdateTimer<system>(interval : Int) {
    snapshotTimerId := Timer.setTimer<system>(
      #nanoseconds(1),
      func() : async () {
        try {
          if (systemState == #Active) {
            await updateSnapshot();
          } else {
            addSnapshotUpdateTimer<system>(interval);
          };
        } catch (e) {
          Debug.print("Error updating snapshot: " # Error.message(e));
          addSnapshotUpdateTimer<system>(interval);
        };
      },
    );

  };

  public shared ({ caller }) func syncTokenDetailsFromTreasury(tokenDetails : [(Principal, TokenDetails)]) : async Result.Result<Text, SyncError> {
    logger.info("Treasury", "Syncing token details from treasury. Caller: " # Principal.toText(caller) # ", Tokens: " # Nat.toText(tokenDetails.size()), "syncTokenDetailsFromTreasury");

    if (not isAllowed(caller) or caller != treasuryPrincipal) {
      logger.warn("Treasury", "Unauthorized sync attempt from: " # Principal.toText(caller), "syncTokenDetailsFromTreasury");
      return #err(#NotTreasury);
    };

    try {
      var tokensUpdated = 0;
      var tokensSkipped = 0;

      label a for ((principal, details) in tokenDetails.vals()) {
        let currentDetails = Map.get(tokenDetailsMap, phash, principal);

        switch (currentDetails) {
          case null {
            // Skip if token doesn't exist in DAO's tokenDetailsMAP
            tokensSkipped += 1;
            continue a;
          };
          case (?existingDetails) {
            // Update balance, price, and lastTimeSynced while preserving other fields
            Map.set(
              tokenDetailsMap,
              phash,
              principal,
              {
                existingDetails with
                balance = details.balance;
                priceInICP = details.priceInICP;
                priceInUSD = details.priceInUSD;
                pastPrices = details.pastPrices;
                lastTimeSynced = details.lastTimeSynced;
              },
            );
            tokensUpdated += 1;
          };
        };
      };

      logger.info("Treasury", "Token sync complete. Updated: " # Nat.toText(tokensUpdated) # ", Skipped: " # Nat.toText(tokensSkipped), "syncTokenDetailsFromTreasury");
      #ok("Token details synced successfully");
    } catch (e) {
      let errorMsg = "Failed to sync token details: " # Error.message(e);
      logger.error("Treasury", errorMsg, "syncTokenDetailsFromTreasury");
      #err(#UnexpectedError(errorMsg));
    };
  };

  public query ({ caller }) func getNeuronAllocation(neuronId : Blob) : async ?NeuronAllocation {
    if (isAllowedQuery(caller)) {
      Map.get(neuronAllocationMap, bhash, neuronId);
    } else {
      null;
    };
  };

  /**
 * Update Minting Vault configuration
 *
 * Allows configuration of premium rates, update intervals, and enabling/disabling swapping
 * Only callable by admins with the updateMintingVaultConfig permission.
 */
  public shared ({ caller }) func updateMintingVaultConfig(
    config : MintingVault.UpdateConfig
  ) : async Result.Result<Text, AuthorizationError> {
    if (not isAdmin(caller, #updateMintingVaultConfig)) {
      return #err(#NotAdmin);
    };

    try {
      // Update configuration if any parameter is specified
      if (
        config.minPremium != null or config.maxPremium != null or
        config.balanceUpdateInterval != null or config.blockCleanupInterval != null or
        config.maxSlippageBasisPoints != null or config.PRICE_HISTORY_WINDOW != null
      ) {

        let configResult = await mintingVault.updateConfiguration({
          minPremium = config.minPremium;
          maxPremium = config.maxPremium;
          balanceUpdateInterval = config.balanceUpdateInterval;
          blockCleanupInterval = config.blockCleanupInterval;
          maxSlippageBasisPoints = config.maxSlippageBasisPoints;
          PRICE_HISTORY_WINDOW = config.PRICE_HISTORY_WINDOW;
          swappingEnabled = config.swappingEnabled;
        });

        switch (configResult) {
          case (#ok()) {};
          case (#err(e)) { return #err(#UnexpectedError(e)) };
        };
      };

      // Update swapping state if provided
      switch (config.swappingEnabled) {
        case (?enabled) {
          let swapResult = await mintingVault.setSwappingEnabled(enabled);
          switch (swapResult) {
            case (#ok(_)) {};
            case (#err(e)) { return #err(#UnexpectedError(e)) };
          };
        };
        case null {};
      };

      #ok("Minting vault configuration updated successfully");
    } catch (e) {
      #err(#UnexpectedError("Error updating minting vault configuration: " # Error.message(e)));
    };
  };

  // Function to get logs - restricted to controllers only
  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    //if (caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
    if (isAdmin(caller, #getLogs) or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.getLastLogs(count);
    } else { [] };
  };

  // Function to get logs by context - restricted to controllers only
  public query ({ caller }) func getLogsByContext(context : Text, count : Nat) : async [Logger.LogEntry] {
    if (isAdmin(caller, #getLogs) or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.getContextLogs(context, count);
    } else { [] };
  };

  // Function to get logs by level - restricted to controllers only
  public query ({ caller }) func getLogsByLevel(level : Logger.LogLevel, count : Nat) : async [Logger.LogEntry] {
    if (isAdmin(caller, #getLogs) or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.getLogsByLevel(level, count);
    } else { [] };
  };

  // Function to clear logs - restricted to controllers
  public shared ({ caller }) func clearLogs() : async () {
    if (isAdmin(caller, #clearLogs) or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.info("System", "Logs cleared by: " # Principal.toText(caller), "clearLogs");
      logger.clearLogs();
      logger.clearContextLogs("all");
    };
  };

  public shared ({ caller }) func setTacoAddress(address : Principal) : async () {
    if (Principal.isController(caller) or isAdmin(caller, #setTacoAddress) or sns_governance_canister_id == ?caller) {
      tacoAddress := address;
    };
  };

  // Start snapshot timer on canister upgrades:
  addSnapshotUpdateTimer<system>(SNAPSHOT_INTERVAL);

  // Set the SNS governance canister id
  public shared ({ caller }) func set_sns_governance_canister_id(p : Principal) : async () {
    assert (Principal.isController(caller) or isAdmin(caller, #updateSystemParameter) or sns_governance_canister_id == ?caller);
    sns_governance_canister_id := ?p;
  };


/* NB: Turn on again after initial setup
  // Inspect message validation
  system func inspect({
    caller : Principal;
    msg : {
      #addAdmin : () -> Principal;
      #addToken : () -> (Principal, TokenType);
      #admin_getNeuronAllocations : () -> ();
      #admin_getUserAllocation : () -> Principal;
      #admin_getUserAllocations : () -> ();
      #admin_recalculateAllVotingPower : () -> Nat;      
      #clearLogs : () -> ();
      #followAllocation : () -> Principal;
      #getAdminPermissions : () -> ();
      #getAggregateAllocation : () -> ();
      #getFollowersWithNeuronCounts : () -> ();
      #getHistoricBalanceAndAllocation : () -> Nat;
      #getLogs : () -> Nat;
      #getLogsByContext : () -> (Text, Nat);
      #getLogsByLevel : () -> (LogLevel, Nat);
      #getNeuronAllocation : () -> Blob;
      #getSnapshotInfo : () -> ();
      #getSystemParameters : () -> ();
      #getTokenDetails : () -> ();
      #getUserAllocation : () -> ();
      #refreshUserVotingPower : () -> ();
      #grantAdminPermission : () -> (Principal, SpamProtection.AdminFunction, Nat);
      #hasAdminPermission : () -> (Principal, SpamProtection.AdminFunction);
      #pauseToken : () -> Principal;
      #removeAdmin : () -> Principal;
      #removeFollower : () -> (follower : Principal);
      #removeToken : () -> Principal;
      #deleteToken : () -> Principal;
      #setTacoAddress : () -> Principal;
      #set_sns_governance_canister_id : () -> Principal;
      #syncTokenDetailsFromTreasury : () -> [(Principal, TokenDetails)];
      #unfollowAllocation : () -> Principal;
      #unpauseToken : () -> Principal;
      #updateAllocation : () -> [Allocation];
      #updateMintingVaultConfig : () -> MintingVault.UpdateConfig;
      #updateSpamParameters : () -> {
        allowedCalls : ?Nat;
        allowedSilentWarnings : ?Nat;
        timeWindowSpamCheck : ?Int;
      };
      #updateSystemParameter : () -> SystemParameter;
      #updateSystemState : () -> SystemState;      
      #updateTreasuryConfig : () -> (TreasuryTypes.UpdateConfig, ?Bool, ?Text);
      #votingPowerMetrics : () -> ();
    };
    arg : Blob;
  }) : Bool {
    if (arg.size() > 50000) { return false }; //Not sure how much this should be
    switch (msg) {
      case (#set_sns_governance_canister_id p) {
        isMasterAdmin(caller) or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#updateAllocation d) {
        let newAllocations = d();

        // Get user state for additional checks
        let userState = switch (Map.get(userStates, phash, caller)) {
          case (?state) { state };
          case null {
            // If no state and not in test mode, reject
            if (not spamGuard.state.test) {
              return false;
            };
            emptyUserState;
          };
        };

        // All validation checks from updateAllocation:
        // 1. Basic checks
        let basicChecks = isAllowed(caller) and (systemState == #Active);
        if (not basicChecks) { return false };

        // 2. Allocation validation
        if (newAllocations.size() > 0) {
          if (not validateAllocations(newAllocations)) {
            return false;
          };
        };

        // 3. Check if trying to submit empty allocation when no previous allocation exists

        if (userState.allocations == [] and newAllocations.size() == 0) {
          return false;
        };

        // 4. Rate limit check
        let timenow = Time.now();
        let pastAllocSize = userState.pastAllocations.size();
        let max_past_allocations = if (MAX_ALLOCATIONS_PER_DAY > 0) {
          if (Int.abs(MAX_ALLOCATIONS_PER_DAY) >= 1) { Int.abs(MAX_ALLOCATIONS_PER_DAY) - 1 } else { 0 };
        } else { 0 };

        if (pastAllocSize >= max_past_allocations) {
          let recentAllocations = Array.subArray(
            userState.pastAllocations,
            if (pastAllocSize < max_past_allocations) { 0 } else { pastAllocSize - max_past_allocations },
            max_past_allocations,
          );

          if (
            recentAllocations.size() == max_past_allocations and
            recentAllocations[0].from > timenow - ALLOCATION_WINDOW
          ) {
            return false;
          };
        };

        true;
      };
      case (#followAllocation d) {
        let followee = d();

        // 1. Basic checks
        let basicChecks = isAllowed(caller) and (systemState == #Active);
        if (not basicChecks) { return false };

        // 2. Can't follow self
        if (caller == followee) { return false };

        // 3. Check followee state exists or test mode
        let followeeState = switch (Map.get(userStates, phash, followee)) {
          case (?state) { state };
          case null {
            if (not spamGuard.state.test) {
              return false;
            };
            emptyUserState;
          };
        };

        // 4. Check caller state exists or test mode
        let callerState = switch (Map.get(userStates, phash, caller)) {
          case (?state) { state };
          case null {
            if (not spamGuard.state.test) {
              return false;
            };
            emptyUserState;
          };
        };

        // 5. Check if followee has made any allocations

        if (followeeState.allocations.size() == 0 and followeeState.pastAllocations.size() == 0) {
          return false;
        };

        // 6. Check if caller has made any allocations
        if (callerState.allocations.size() == 0 and callerState.pastAllocations.size() == 0) {
          return false;
        };

        // 7. Check follow limits
        if (callerState.allocationFollows.size() >= MAX_FOLLOWED) {
          return false;
        };

        // 8. Check if already following
        if (
          Array.find<{ follow : Principal; since : Int }>(
            callerState.allocationFollows,
            func(f) { f.follow == followee },
          ) != null
        ) {
          return false;
        };

        // 9. Check follow/unfollow rate limit
        let timenow = Time.now();
        let followUnfollowActions = Array.filter<Int>(
          callerState.followUnfollowActions,
          func(f) { f > timenow - 86400000000000 },
        );
        if (followUnfollowActions.size() >= MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY) {
          return false;
        };

        // 10. Check followee's follower limit

        if (followeeState.allocationFollowedBy.size() >= MAX_FOLLOWERS) {
          return false;
        };

        true;
      };
      case (#unfollowAllocation d) {
        let followee = d();

        // 1. Basic checks
        let basicChecks = isAllowed(caller) and (systemState == #Active);
        if (not basicChecks) { return false };

        // 2. Can't unfollow self
        if (caller == followee) { return false };

        // 3. Check caller state exists or test mode
        let callerState = switch (Map.get(userStates, phash, caller)) {
          case (?state) { state };
          case null {
            if (not spamGuard.state.test) {
              return false;
            };
            emptyUserState;
          };
        };

        // 4. Check if actually following

        // Must be currently following to unfollow
        if (
          Array.find<{ follow : Principal; since : Int }>(
            callerState.allocationFollows,
            func(f) { f.follow == followee },
          ) == null
        ) {
          return false;
        };

        // Check follow/unfollow rate limit
        let timenow = Time.now();
        let followUnfollowActions = Array.filter<Int>(
          callerState.followUnfollowActions,
          func(f) { f > timenow - 86400000000000 },
        );
        if (followUnfollowActions.size() >= MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY) {
          return false;
        };

        true;
      };

      case (#getFollowersWithNeuronCounts _) {
        isAllowedQuery(caller);
      };
      case (#removeFollower d) {
        let follower = d();

        // 1. Basic checks
        let basicChecks = isAllowed(caller) and (systemState == #Active);
        if (not basicChecks) { return false };

        // 2. Get caller's state
        let callerState = switch (Map.get(userStates, phash, caller)) {
          case (?state) { state };
          case null {
            if (not spamGuard.state.test) {
              return false;
            };
            emptyUserState;
          };
        };

        // 3. Check if the principal is actually following the caller
        if (
          Array.find<{ follow : Principal; since : Int }>(
            callerState.allocationFollowedBy,
            func(f) { f.follow == follower },
          ) == null
        ) {
          return false;
        };

        // 4. Check follow/unfollow rate limit
        let timenow = Time.now();
        let followUnfollowActions = Array.filter<Int>(
          callerState.followUnfollowActions,
          func(f) { f > timenow - 86400000000000 },
        );
        if (followUnfollowActions.size() >= MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY) {
          return false;
        };

        true;
      };

      case (#updateMintingVaultConfig d) {
        isAdmin(caller, #updateMintingVaultConfig);
      };
      case (#syncTokenDetailsFromTreasury _) {
        (caller == treasuryPrincipal and isAllowed(caller));
      };
      case (#updateSystemState _) {
        isAdmin(caller, #updateSystemState);
      };
      case (#addAdmin _) {
        isAdmin(caller, #addAdmin);
      };
      case (#removeAdmin _) {
        isAdmin(caller, #removeAdmin);
      };
      case (#getAggregateAllocation _) {
        isAllowedQuery(caller);
      };
      case (#getUserAllocation _) {
        isAllowedQuery(caller);
      };
      case (#refreshUserVotingPower _) {
        isAllowed(caller) and (systemState == #Active);
      };
      case (#getSnapshotInfo _) {
        isAllowedQuery(caller);
      };
      case (#updateSpamParameters _) {
        isAdmin(caller, #updateSpamParameters);
      };
      case (#addToken _) {
        isAdmin(caller, #addToken);
      };
      case (#removeToken _) {
        isAdmin(caller, #removeToken);
      };
      case (#deleteToken _) {
        isAdmin(caller, #deleteToken);
      };
      case (#pauseToken _) {
        isAdmin(caller, #pauseToken);
      };
      case (#unpauseToken _) {
        isAdmin(caller, #unpauseToken);
      };
      case (#grantAdminPermission _) {
        Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getAdminPermissions _) {
        isAllowedQuery(caller);
      };
      case (#getTokenDetails _) {
        isAllowedQuery(caller);
      };
      case (#votingPowerMetrics _) {
        isAllowedQuery(caller);
      };
      case (#updateSystemParameter _) {
        isAdmin(caller, #updateSystemParameter);
      };
      case (#getNeuronAllocation d) {
        isAllowedQuery(caller);
      };
      case (#getSystemParameters _) {
        isAllowedQuery(caller);
      };
      case (#getHistoricBalanceAndAllocation _) {
        isAllowedQuery(caller);
      };
      case (#updateTreasuryConfig d) {
        isAdmin(caller, #updateTreasuryConfig);
      };
      case (#admin_getNeuronAllocations d) {
        isAdmin(caller, #getLogs);
      };
      case (#admin_getUserAllocation d) {
        isAdmin(caller, #getLogs);
      };
      case (#admin_getUserAllocations d) {
        isAdmin(caller, #getLogs);
      };
      case (#admin_recalculateAllVotingPower d) {
        isMasterAdmin(caller);
      };
      case (#hasAdminPermission d) {
        isMasterAdmin(caller) or caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller or isAdmin(d());
      };
      case (#clearLogs _) {
        isMasterAdmin(caller) or caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getLogs _) {
        isMasterAdmin(caller) or caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getLogsByContext _) {
        isMasterAdmin(caller) or caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getLogsByLevel _) {
        isMasterAdmin(caller) or caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#setTacoAddress _) {
        isMasterAdmin(caller) or isAdmin(caller, #setTacoAddress) or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };

    };
  };
  */

  // Query method for admin actions since timestamp (for archiving)
  public shared query ({ caller }) func getAdminActionsSince(
    sinceTimestamp: Int, 
    limit: Nat
  ) : async Result.Result<AdminActionsSinceResponse, AuthorizationError> {
    if (not isAdmin(caller, #getAdminActions)) {
      return #err(#NotAdmin);
    };

    if (limit == 0 or limit > 1000) {
      return #err(#UnexpectedError("Invalid limit: must be between 1 and 1000"));
    };

    let allActions = Vector.toArray(adminActions);
    var filteredActions = Array.filter<AdminActionRecord>(allActions, func(action) {
      action.timestamp > sinceTimestamp
    });
    
    // Sort by timestamp (oldest first for proper archive ordering)
    filteredActions := Array.sort(filteredActions, func(a: AdminActionRecord, b: AdminActionRecord) : Order.Order {
      Int.compare(a.timestamp, b.timestamp)
    });
    
    let totalFilteredCount = filteredActions.size();
    let limitedActions = if (totalFilteredCount > limit) {
      Array.subArray(filteredActions, 0, limit)
    } else {
      filteredActions
    };

    let response: AdminActionsSinceResponse = {
      actions = limitedActions;
      totalCount = totalFilteredCount;
    };

    #ok(response);
  };

  // Query method for allocation changes since timestamp (for dao_allocation_archive)
  public shared query ({ caller }) func getAllocationChangesSince(
    sinceTimestamp: Int, 
    limit: Nat
  ) : async Result.Result<AllocationChangesSinceResponse, AuthorizationError> {
    // Allow master admins and our own canisters (secure approach)
    if (not isMasterAdmin(caller)) {
      return #err(#NotAdmin);
    };

    if (limit == 0 or limit > 500) {
      return #err(#UnexpectedError("Invalid limit: must be between 1 and 500"));
    };

    // Collect past allocations from all users
    var allChanges : [PastAllocationRecord] = [];
    for ((user, userState) in Map.entries(userStates)) {
      for (pastAlloc in userState.pastAllocations.vals()) {
        if (pastAlloc.from > sinceTimestamp) {
          let change: PastAllocationRecord = {
            user = user;
            from = pastAlloc.from;
            to = pastAlloc.to;
            allocation = pastAlloc.allocation;
            allocationMaker = pastAlloc.allocationMaker;
          };
          allChanges := Array.append(allChanges, [change]);
        };
      };
    };

    // Sort by timestamp (oldest first for proper archive ordering)
    allChanges := Array.sort(allChanges, func(a: PastAllocationRecord, b: PastAllocationRecord) : Order.Order {
      Int.compare(a.from, b.from)
    });
    
    let totalCount = allChanges.size();
    let limitedChanges = if (totalCount > limit) {
      Array.subArray(allChanges, 0, limit)
    } else {
      allChanges
    };

    let response: AllocationChangesSinceResponse = {
      changes = limitedChanges;
      totalCount = totalCount;
    };

    #ok(response);
  };

  // Query method for follow actions since timestamp (for dao_allocation_archive)
  public shared query ({ caller }) func getFollowActionsSince(
    sinceTimestamp: Int, 
    limit: Nat
  ) : async Result.Result<FollowActionsSinceResponse, AuthorizationError> {
    // Allow master admins and our own canisters (secure approach)
    if (not isMasterAdmin(caller)) {
      return #err(#NotAdmin);
    };

    if (limit == 0 or limit > 200) {
      return #err(#UnexpectedError("Invalid limit: must be between 1 and 200"));
    };

    var allFollows : [FollowRecord] = [];
    var allUnfollows : [UnfollowRecord] = [];

    // Collect current follows from all users
    for ((user, userState) in Map.entries(userStates)) {
      for (follow in userState.allocationFollows.vals()) {
        if (follow.since > sinceTimestamp) {
          let followRecord: FollowRecord = {
            follower = user;
            followed = follow.follow;
            since = follow.since;
          };
          allFollows := Array.append(allFollows, [followRecord]);
        };
      };
    };

    // Note: We don't currently track unfollow timestamps in the existing data structure
    // This would require enhancing the data model if we need unfollows

    let totalCount = allFollows.size() + allUnfollows.size();
    let limitedFollows = if (allFollows.size() > limit) {
      Array.subArray(allFollows, 0, limit)
    } else {
      allFollows
    };

    let response: FollowActionsSinceResponse = {
      follows = limitedFollows;
      unfollows = allUnfollows; // Empty for now
      totalCount = totalCount;
    };

    #ok(response);
  };

  // Query method for voting power changes since timestamp (for dao_governance_archive)
  public shared query ({ caller }) func getVotingPowerChangesSince(
    sinceTimestamp: Int, 
    limit: Nat
  ) : async Result.Result<VotingPowerChangesSinceResponse, AuthorizationError> {
    // Allow master admins and our own canisters (secure approach)
    if (not isMasterAdmin(caller)) {
      return #err(#NotAdmin);
    };

    if (limit == 0 or limit > 300) {
      return #err(#UnexpectedError("Invalid limit: must be between 1 and 300"));
    };

    var userRecords : [UserVotingPowerRecord] = [];

    // Collect users who had voting power updates since timestamp
    for ((user, userState) in Map.entries(userStates)) {
      if (userState.lastVotingPowerUpdate > sinceTimestamp) {
        let userRecord: UserVotingPowerRecord = {
          user = user;
          votingPower = userState.votingPower;
          lastVotingPowerUpdate = userState.lastVotingPowerUpdate;
          neurons = userState.neurons;
        };
        userRecords := Array.append(userRecords, [userRecord]);
      };
    };

    // Sort by timestamp (most recent first)
    userRecords := Array.sort(userRecords, func(a: UserVotingPowerRecord, b: UserVotingPowerRecord) : Order.Order {
      Int.compare(b.lastVotingPowerUpdate, a.lastVotingPowerUpdate)
    });
    
    let totalCount = userRecords.size();
    let limitedRecords = if (totalCount > limit) {
      Array.subArray(userRecords, 0, limit)
    } else {
      userRecords
    };

    let response: VotingPowerChangesSinceResponse = {
      users = limitedRecords;
      totalCount = totalCount;
    };

    #ok(response);
  };

  // Query method for neuron updates since timestamp (for dao_governance_archive)
  public shared query ({ caller }) func getNeuronUpdatesSince(
    sinceTimestamp: Int, 
    limit: Nat
  ) : async Result.Result<NeuronUpdatesSinceResponse, AuthorizationError> {
    // Allow master admins and our own canisters (secure approach)
    if (not isMasterAdmin(caller)) {
      return #err(#NotAdmin);
    };

    if (limit == 0 or limit > 100) {
      return #err(#UnexpectedError("Invalid limit: must be between 1 and 100"));
    };

    var neuronRecords : [NeuronRecord] = [];
    var neuronMap = Map.new<Blob, {votingPower: Nat; users: [Principal]}>();

    // Collect all unique neurons and their users (only if user had updates since timestamp)
    for ((user, userState) in Map.entries(userStates)) {
      if (userState.lastVotingPowerUpdate > sinceTimestamp) {
        for (neuron in userState.neurons.vals()) {
          switch (Map.get(neuronMap, Map.bhash, neuron.neuronId)) {
            case (?existing) {
              // Add user to existing neuron record
              let updatedUsers = Array.append(existing.users, [user]);
              Map.set(neuronMap, Map.bhash, neuron.neuronId, {
                votingPower = neuron.votingPower; // Use latest voting power
                users = updatedUsers;
              });
            };
            case null {
              // Create new neuron record
              Map.set(neuronMap, Map.bhash, neuron.neuronId, {
                votingPower = neuron.votingPower;
                users = [user];
              });
            };
          };
        };
      };
    };

    // Convert map to array
    for ((neuronId, neuronData) in Map.entries(neuronMap)) {
      let neuronRecord: NeuronRecord = {
        neuronId = neuronId;
        votingPower = neuronData.votingPower;
        users = neuronData.users;
      };
      neuronRecords := Array.append(neuronRecords, [neuronRecord]);
    };
    
    let totalCount = neuronRecords.size();
    let limitedRecords = if (totalCount > limit) {
      Array.subArray(neuronRecords, 0, limit)
    } else {
      neuronRecords
    };

    let response: NeuronUpdatesSinceResponse = {
      neurons = limitedRecords;
      totalCount = totalCount;
    };

    #ok(response);
  };

  // Query method for neuron allocation changes since timestamp (for dao_neuron_allocation_archive)
  public shared query ({ caller }) func getNeuronAllocationChangesSince(
    sinceTimestamp: Int, 
    limit: Nat
  ) : async Result.Result<NeuronAllocationChangesSinceResponse, AuthorizationError> {
    // Allow master admins and our own canisters (secure approach)
    if (not isMasterAdmin(caller)) {
      return #err(#NotAdmin);
    };

    if (limit == 0 or limit > 500) {
      return #err(#UnexpectedError("Invalid limit: must be between 1 and 500"));
    };

    // Collect neuron allocation changes from circular buffer
    var allChanges : [NeuronAllocationChangeRecord] = [];
    
    // Iterate through the circular buffer
    for (i in Iter.range(0, neuronAllocationChangesSize - 1)) {
      let pos = (neuronAllocationChangesHead + i) % MAX_NEURON_ALLOCATION_CHANGES;
      switch (neuronAllocationChanges[pos]) {
        case (?change) {
          if (change.timestamp > sinceTimestamp) {
            allChanges := Array.append(allChanges, [change]);
          };
        };
        case null {
          // Should not happen in a properly maintained circular buffer
        };
      };
    };

    // Sort by timestamp (oldest first for proper archive ordering)
    allChanges := Array.sort(allChanges, func(a: NeuronAllocationChangeRecord, b: NeuronAllocationChangeRecord) : Order.Order {
      Int.compare(a.timestamp, b.timestamp)
    });
    
    let totalCount = allChanges.size();
    let limitedChanges = if (totalCount > limit) {
      Array.subArray(allChanges, 0, limit)
    } else {
      allChanges
    };

    let response: NeuronAllocationChangesSinceResponse = {
      changes = limitedChanges;
      totalCount = totalCount;
    };

    #ok(response);
  };

  // User token registration methods
  public shared(msg) func registerUserToken(tokenPrincipal: Principal): async Result.Result<Text, TokenRegistrationError> {
    let caller = msg.caller;
    
    // Check if system is active
    if (systemState != #Active) {
      return #err(#SystemInactive);
    };
    
    // Allow registration of any ICRC1 token (no validation against trusted tokens)
    
    // Get or create user wallet data
    let currentWalletData = switch (Map.get(userWalletData, phash, caller)) {
      case null {
        let newWalletData: UserWalletData = {
          registeredTokens = [];
          lastUpdated = Time.now();
        };
        Map.set(userWalletData, phash, caller, newWalletData);
        newWalletData;
      };
      case (?existing) { existing };
    };
    
    // Check if token is already registered
    if (Array.find(currentWalletData.registeredTokens, func(p: Principal): Bool { Principal.equal(p, tokenPrincipal) }) != null) {
      return #err(#TokenAlreadyRegistered);
    };
    
    // Check max tokens limit (e.g., 50 tokens per user)
    if (currentWalletData.registeredTokens.size() >= 50) {
      return #err(#MaxTokensReached);
    };
    
    // Add token to user's registered tokens
    let updatedTokens = Array.append(currentWalletData.registeredTokens, [tokenPrincipal]);
    let updatedWalletData: UserWalletData = {
      registeredTokens = updatedTokens;
      lastUpdated = Time.now();
    };
    
    Map.set(userWalletData, phash, caller, updatedWalletData);
    
    #ok("Token registered successfully");
  };

  public shared(msg) func unregisterUserToken(tokenPrincipal: Principal): async Result.Result<Text, TokenRegistrationError> {
    let caller = msg.caller;
    
    // Check if system is active
    if (systemState != #Active) {
      return #err(#SystemInactive);
    };
    
    // Get user wallet data
    switch (Map.get(userWalletData, phash, caller)) {
      case null {
        return #err(#TokenNotRegistered);
      };
      case (?currentWalletData) {
        // Check if token is registered
        let tokenExists = Array.find(currentWalletData.registeredTokens, func(p: Principal): Bool { Principal.equal(p, tokenPrincipal) }) != null;
        if (not tokenExists) {
          return #err(#TokenNotRegistered);
        };
        
        // Remove token from registered tokens
        let updatedTokens = Array.filter(currentWalletData.registeredTokens, func(p: Principal): Bool { not Principal.equal(p, tokenPrincipal) });
        let updatedWalletData: UserWalletData = {
          registeredTokens = updatedTokens;
          lastUpdated = Time.now();
        };
        
        Map.set(userWalletData, phash, caller, updatedWalletData);
        
        #ok("Token unregistered successfully");
      };
    };
  };

  public shared query(msg) func getUserRegisteredTokens(): async [Principal] {
    let caller = msg.caller;
    
    switch (Map.get(userWalletData, phash, caller)) {
      case null { [] };
      case (?walletData) { walletData.registeredTokens };
    };
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };  
};
