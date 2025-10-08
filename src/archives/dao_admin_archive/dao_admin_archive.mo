import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Order "mo:base/Order";
import Map "mo:map/Map";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Text "mo:base/Text";
import Error "mo:base/Error";
import Debug "mo:base/Debug";

import ICRC3 "mo:icrc3-mo";
import ICRC3Service "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import DAOTypes "../../DAO_backend/dao_types";
import TreasuryTypes "../../treasury/treasury_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";
import Cycles "mo:base/ExperimentalCycles";

shared (deployer) actor class DAOAdminArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type AdminActionBlockData = ArchiveTypes.AdminActionBlockData;
  type AdminCanisterSource = ArchiveTypes.AdminCanisterSource;
  type AdminActionVariant = ArchiveTypes.AdminActionVariant;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type AdminActionRecord = DAOTypes.AdminActionRecord;
  type TreasuryAdminActionRecord = TreasuryTypes.TreasuryAdminActionRecord;

  // Initialize the generic base class with admin-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 1000000; // 1M blocks per canister
    blockRetentionPeriodNS = 63072000000000000; // 2 years in nanoseconds (admin actions are important)
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  // ICRC3 State for scalable storage
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  private let base = ArchiveBase.ArchiveBase<AdminActionBlockData>(
    this_canister_id(),
    ["3admin"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Admin-specific indexes (not covered by base class)
  private stable var adminIndex = Map.new<Principal, [Nat]>(); // Admin -> block indices
  private stable var canisterIndex = Map.new<Text, [Nat]>(); // Canister -> block indices (Text representation)
  private stable var actionTypeIndex = Map.new<Text, [Nat]>(); // Action type -> block indices

  // Admin-specific statistics
  private stable var totalAdminActions : Nat = 0;
  private stable var totalSuccessfulActions : Nat = 0;
  private stable var totalDAOActions : Nat = 0;
  private stable var totalTreasuryActions : Nat = 0;

  // Tracking state for batch imports
  private stable var lastImportedDAOActionId : Nat = 0;
  private stable var lastImportedTreasuryActionId : Nat = 0;

  // Canister interfaces for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
  private let TREASURY_ID = canister_ids.getCanisterId(#treasury);
  private let daoCanister : DAOTypes.Self = actor (Principal.toText(DAO_BACKEND_ID));
  private let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(TREASURY_ID));

  //=========================================================================
  // ICRC-3 Standard endpoints (delegated to base class)
  //=========================================================================

  public query func icrc3_get_archives(args : ICRC3Service.GetArchivesArgs) : async ICRC3Service.GetArchivesResult {
    base.icrc3_get_archives(args);
  };

  public query func icrc3_get_tip_certificate() : async ?ICRC3Service.DataCertificate {
    base.icrc3_get_tip_certificate();
  };

  public query func icrc3_get_blocks(args : ICRC3Service.GetBlocksArgs) : async ICRC3Service.GetBlocksResult {
    base.icrc3_get_blocks(args);
  };

  public query func icrc3_supported_block_types() : async [ICRC3Service.BlockType] {
    base.icrc3_supported_block_types();
  };

  //=========================================================================
  // Custom Admin Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveAdminAction<system>(adminAction : AdminActionBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    let blockData = #Admin(adminAction);
    let blockValue = ArchiveTypes.adminActionToValue(adminAction, adminAction.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3admin",
      [], // No specific tokens for admin actions
      adminAction.timestamp
    );

    // Update custom indexes
    updateAdminIndex(adminAction.admin, blockIndex);
    updateCanisterIndex(adminAction.canister, blockIndex);
    updateActionTypeIndex(getActionTypeString(adminAction.actionType), blockIndex);

    // Update statistics
    totalAdminActions += 1;
    if (adminAction.success) {
      totalSuccessfulActions += 1;
    };
    switch (adminAction.canister) {
      case (#DAO_backend) { totalDAOActions += 1 };
      case (#Treasury) { totalTreasuryActions += 1 };
    };

    #ok(blockIndex);
  };

  //=========================================================================
  // Batch Import System
  //=========================================================================

  // Import admin actions from DAO_backend
  public shared ({ caller }) func importDAOAdminActions<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    try {
      let response = await daoCanister.getAdminActionsSince(lastImportedDAOActionId, 100);
      switch (response) {
        case (#ok(data)) {
          var importedCount = 0;
          
          // Sort actions by timestamp to ensure oldest-first ordering (defensive measure)
          let sortedActions = Array.sort(data.actions, func(a: DAOTypes.AdminActionRecord, b: DAOTypes.AdminActionRecord) : Order.Order {
            Int.compare(a.timestamp, b.timestamp)
          });
          
          for (action in sortedActions.vals()) {
            if (action.id > lastImportedDAOActionId) {
              let blockData : AdminActionBlockData = {
                id = action.id;
                timestamp = action.timestamp;
                admin = action.admin;
                canister = #DAO_backend;
                actionType = convertDAOActionType(action.actionType);
                reason = action.reason;
                success = action.success;
                errorMessage = action.errorMessage;
              };
              
              switch (await archiveAdminAction<system>(blockData)) {
                case (#ok(_)) {
                  importedCount += 1;
                  lastImportedDAOActionId := action.id;
                };
                case (#err(error)) {
                  Debug.print("Failed to archive DAO admin action " # Nat.toText(action.id) # ": " # debug_show(error));
                };
              };
            };
          };
          Debug.print("DAO Admin Archive: Imported " # Nat.toText(importedCount) # " DAO admin actions from DAO_backend");
          #ok("Imported " # Nat.toText(importedCount) # " DAO admin actions");
        };
        case (#err(error)) {
          #err("Failed to fetch DAO admin actions: " # debug_show(error));
        };
      };
    } catch (e) {
      #err("Error importing DAO admin actions: " # Error.message(e));
    };
  };

  // Import admin actions from Treasury
  public shared ({ caller }) func importTreasuryAdminActions<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err("Not authorized");
    };

    try {
      let response = await treasuryCanister.getTreasuryAdminActionsSince(lastImportedTreasuryActionId, 100);
      switch (response) {
        case (#ok(data)) {
          var importedCount = 0;
          
          // Sort actions by timestamp to ensure oldest-first ordering (defensive measure)
          let sortedActions = Array.sort(data.actions, func(a: TreasuryTypes.TreasuryAdminActionRecord, b: TreasuryTypes.TreasuryAdminActionRecord) : Order.Order {
            Int.compare(a.timestamp, b.timestamp)
          });
          
          for (action in sortedActions.vals()) {
            if (action.id > lastImportedTreasuryActionId) {
              let blockData : AdminActionBlockData = {
                id = action.id;
                timestamp = action.timestamp;
                admin = action.admin;
                canister = #Treasury;
                actionType = convertTreasuryActionType(action.actionType);
                reason = action.reason;
                success = action.success;
                errorMessage = action.errorMessage;
              };
              
              switch (await archiveAdminAction<system>(blockData)) {
                case (#ok(_)) {
                  importedCount += 1;
                  lastImportedTreasuryActionId := action.id;
                };
                case (#err(error)) {
                  Debug.print("Failed to archive Treasury admin action " # Nat.toText(action.id) # ": " # debug_show(error));
                };
              };
            };
          };
          Debug.print("DAO Admin Archive: Imported " # Nat.toText(importedCount) # " Treasury admin actions");
          #ok("Imported " # Nat.toText(importedCount) # " Treasury admin actions");
        };
        case (#err(error)) {
          #err("Failed to fetch Treasury admin actions: " # debug_show(error));
        };
      };
    } catch (e) {
      #err("Error importing Treasury admin actions: " # Error.message(e));
    };
  };

  //=========================================================================
  // Query Functions
  //=========================================================================

  public query func getAdminActionsByAdmin(admin : Principal, limit : Nat) : async Result.Result<[AdminActionBlockData], ArchiveError> {
    switch (Map.get(adminIndex, Map.phash, admin)) {
      case (?blockIndices) {
        let limitedIndices = if (Array.size(blockIndices) > limit) {
          Array.tabulate<Nat>(limit, func(i) = blockIndices[i]);
        } else {
          blockIndices;
        };
        
        // For now, return empty array since we need to implement proper ICRC3 block querying
        #ok([]);
      };
      case null { #ok([]) };
    };
  };

  public query func getAdminActionsByCanister(canister : AdminCanisterSource, limit : Nat) : async Result.Result<[AdminActionBlockData], ArchiveError> {
    let canisterKey = switch (canister) {
      case (#DAO_backend) { "DAO_backend" };
      case (#Treasury) { "Treasury" };
    };
    
    switch (Map.get(canisterIndex, Map.thash, canisterKey)) {
      case (?blockIndices) {
        let limitedIndices = if (Array.size(blockIndices) > limit) {
          Array.tabulate<Nat>(limit, func(i) = blockIndices[i]);
        } else {
          blockIndices;
        };
        
        // For now, return empty array since we need to implement proper ICRC3 block querying
        #ok([]);
      };
      case null { #ok([]) };
    };
  };

  public query func getArchiveStats() : async ArchiveTypes.ArchiveStatus {
    let totalBlocks = base.getTotalBlocks();
    let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
    let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };
    
    {
      totalBlocks = totalBlocks;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = ["3admin"];
      storageUsed = 0;
      lastArchiveTime = 0; // Will be updated when we have actual timestamps
    };
  };

  //=========================================================================
  // Timer Management - Using BatchImportTimer system instead of system timer
  //=========================================================================

  //=========================================================================
  // Lifecycle Management
  //=========================================================================

  // Batch import function for admin actions  
  private func importBatchAdminActions<system>() : async {imported: Nat; failed: Nat} {
    var totalImported = 0;
    var totalFailed = 0;
    
    // Import DAO admin actions
    switch (await importDAOAdminActions<system>()) {
      case (#ok(message)) {
        if (Text.contains(message, #text "Imported 0")) {
          // No items imported
        } else {
          totalImported += 1;
        };
      };
      case (#err(_)) { totalFailed += 1; };
    };
    
    // Import Treasury admin actions
    switch (await importTreasuryAdminActions<system>()) {
      case (#ok(message)) {
        if (Text.contains(message, #text "Imported 0")) {
          // No items imported
        } else {
          totalImported += 1;
        };
      };
      case (#err(_)) { totalFailed += 1; };
    };
    
    {imported = totalImported; failed = totalFailed};
  };

  // Frontend compatibility methods - matching treasury archive signatures
  public query func getBatchImportStatus() : async {isRunning: Bool; intervalSeconds: Nat} {
    base.getBatchImportStatus();
  };

  public query func getTimerStatus() : async BatchImportTimer.TimerStatus {
    base.getTimerStatus();
  };

  public query ({ caller }) func getArchiveStatus() : async Result.Result<ArchiveTypes.ArchiveStatus, ArchiveError> {
    base.getArchiveStatus(caller);
  };

  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    base.getLogs(count, caller);
  };

  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startAdvancedBatchImportSystem<system>(caller, null, null, ?importBatchAdminActions);
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    base.stopBatchImportSystem(caller);
  };

  public shared ({ caller }) func stopAllTimers() : async Result.Result<Text, Text> {
    base.stopAllTimers(caller);
  };

  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, Text> {
    await base.runAdvancedManualBatchImport<system>(caller, null, null, ?importBatchAdminActions);
  };

  public shared ({ caller }) func setMaxInnerLoopIterations(iterations: Nat) : async Result.Result<Text, Text> {
    base.setMaxInnerLoopIterations(caller, iterations);
  };

  public shared ({ caller }) func resetImportTimestamps() : async Result.Result<Text, Text> {
    // Reset all import timestamps to re-import from beginning
    lastImportedDAOActionId := 0;
    lastImportedTreasuryActionId := 0;
    Debug.print("Import timestamps reset successfully");
    #ok("Import timestamps reset successfully");
  };

  system func preupgrade() {
    // Save ICRC3 state before upgrade
    icrc3State := icrc3StateRef.value;
    base.preupgrade();
  };

  system func postupgrade() {
        // Restore ICRC3 state after upgrade
    icrc3StateRef.value := icrc3State;
    base.postupgrade<system>(func() : async () { /* no-op */ });
  };

  //=========================================================================
  // Private Helper Functions
  //=========================================================================

  private func updateAdminIndex(admin : Principal, blockIndex : Nat) {
    let existing = switch (Map.get(adminIndex, Map.phash, admin)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(adminIndex, Map.phash, admin, Array.append(existing, [blockIndex]));
  };

  private func updateCanisterIndex(canister : AdminCanisterSource, blockIndex : Nat) {
    let canisterKey = switch (canister) {
      case (#DAO_backend) { "DAO_backend" };
      case (#Treasury) { "Treasury" };
    };
    let existing = switch (Map.get(canisterIndex, Map.thash, canisterKey)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(canisterIndex, Map.thash, canisterKey, Array.append(existing, [blockIndex]));
  };

  private func updateActionTypeIndex(actionType : Text, blockIndex : Nat) {
    let existing = switch (Map.get(actionTypeIndex, Map.thash, actionType)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(actionTypeIndex, Map.thash, actionType, Array.append(existing, [blockIndex]));
  };

  // Convert DAO admin action types to archive format
  private func convertDAOActionType(actionType : DAOTypes.AdminActionType) : AdminActionVariant {
    switch (actionType) {
      case (#TokenAdd(details)) { #TokenAdd(details) };
      case (#TokenRemove(details)) { #TokenRemove(details) };
      case (#TokenDelete(details)) { #TokenDelete(details) };
      case (#TokenPause(details)) { #TokenPause(details) };
      case (#TokenUnpause(details)) { #TokenUnpause(details) };
      case (#SystemStateChange(details)) { #SystemStateChange(details) };
      case (#ParameterUpdate(details)) { #ParameterUpdate(details) };
      case (#AdminPermissionGrant(details)) { #AdminPermissionGrant(details) };
      case (#AdminAdd(details)) { #AdminAdd(details) };
      case (#AdminRemove(details)) { #AdminRemove(details) };
      case (#CanisterStart) { #CanisterStart };
      case (#CanisterStop) { #CanisterStop };
    };
  };

  // Convert Treasury admin action types to archive format
  private func convertTreasuryActionType(actionType : TreasuryTypes.TreasuryAdminActionType) : AdminActionVariant {
    switch (actionType) {
      case (#StartRebalancing) { #StartRebalancing };
      case (#StopRebalancing) { #StopRebalancing };
      case (#ResetRebalanceState) { #ResetRebalanceState };
      case (#UpdateRebalanceConfig(details)) { #UpdateRebalanceConfig(details) };
      case (#StartPortfolioSnapshots) { #StartPortfolioSnapshots };
      case (#StopPortfolioSnapshots) { #StopPortfolioSnapshots };
      case (#UpdatePortfolioSnapshotInterval(details)) { #UpdatePortfolioSnapshotInterval(details) };
      case (#CanisterStart) { #CanisterStart };
      case (#CanisterStop) { #CanisterStop };
      case (#PauseTokenManual(details)) { #PauseTokenManual(details) };
      case (#UnpauseToken(details)) { #UnpauseToken(details) };
      case (#ClearAllTradingPauses) { #ClearAllTradingPauses };
      case (#AddTriggerCondition(details)) { #AddTriggerCondition(details) };
      case (#RemoveTriggerCondition(details)) { #RemoveTriggerCondition(details) };
      case (#UpdateTriggerCondition(details)) { #UpdateTriggerCondition(details) };
      case (#SetTriggerConditionActive(details)) { #SetTriggerConditionActive(details) };
      case (#ClearPriceAlerts) { #ClearPriceAlerts };
      case (#AddPortfolioCircuitBreaker(details)) { #AddPortfolioCircuitBreaker(details) };
      case (#RemovePortfolioCircuitBreaker(details)) { #RemovePortfolioCircuitBreaker(details) };
      case (#UpdatePortfolioCircuitBreaker(details)) { #UpdatePortfolioCircuitBreaker(details) };
      case (#SetPortfolioCircuitBreakerActive(details)) { #SetPortfolioCircuitBreakerActive(details) };
      case (#UpdatePausedTokenThreshold(details)) { #UpdatePausedTokenThreshold(details) };
      case (#ClearPortfolioCircuitBreakerLogs) { #ClearPortfolioCircuitBreakerLogs };
      case (#UpdateMaxPortfolioSnapshots(details)) { #UpdateMaxPortfolioSnapshots(details) };
      case (#TakeManualSnapshot) { #TakeManualSnapshot };
      case (#ExecuteTradingCycle) { #ExecuteTradingCycle };
      case (#SetTestMode(details)) { #SetTestMode(details) };
      case (#ClearSystemLogs) { #ClearSystemLogs };
    };
  };

  private func getActionTypeString(actionType : AdminActionVariant) : Text {
    switch (actionType) {
      case (#TokenAdd(_)) { "TokenAdd" };
      case (#TokenRemove(_)) { "TokenRemove" };
      case (#TokenDelete(_)) { "TokenDelete" };
      case (#TokenPause(_)) { "TokenPause" };
      case (#TokenUnpause(_)) { "TokenUnpause" };
      case (#SystemStateChange(_)) { "SystemStateChange" };
      case (#ParameterUpdate(_)) { "ParameterUpdate" };
      case (#AdminPermissionGrant(_)) { "AdminPermissionGrant" };
      case (#AdminAdd(_)) { "AdminAdd" };
      case (#AdminRemove(_)) { "AdminRemove" };
      case (#StartRebalancing) { "StartRebalancing" };
      case (#StopRebalancing) { "StopRebalancing" };
      case (#ResetRebalanceState) { "ResetRebalanceState" };
      case (#UpdateRebalanceConfig(_)) { "UpdateRebalanceConfig" };
      case (#PauseTokenManual(_)) { "PauseTokenManual" };
      case (#UnpauseToken(_)) { "UnpauseToken" };
      case (#ClearAllTradingPauses) { "ClearAllTradingPauses" };
      case (#AddTriggerCondition(_)) { "AddTriggerCondition" };
      case (#RemoveTriggerCondition(_)) { "RemoveTriggerCondition" };
      case (#UpdateTriggerCondition(_)) { "UpdateTriggerCondition" };
      case (#SetTriggerConditionActive(_)) { "SetTriggerConditionActive" };
      case (#ClearPriceAlerts) { "ClearPriceAlerts" };
      case (#AddPortfolioCircuitBreaker(_)) { "AddPortfolioCircuitBreaker" };
      case (#RemovePortfolioCircuitBreaker(_)) { "RemovePortfolioCircuitBreaker" };
      case (#UpdatePortfolioCircuitBreaker(_)) { "UpdatePortfolioCircuitBreaker" };
      case (#SetPortfolioCircuitBreakerActive(_)) { "SetPortfolioCircuitBreakerActive" };
      case (#UpdatePausedTokenThreshold(_)) { "UpdatePausedTokenThreshold" };
      case (#ClearPortfolioCircuitBreakerLogs) { "ClearPortfolioCircuitBreakerLogs" };
      case (#UpdateMaxPortfolioSnapshots(_)) { "UpdateMaxPortfolioSnapshots" };
      case (#StartPortfolioSnapshots) { "StartPortfolioSnapshots" };
      case (#StopPortfolioSnapshots) { "StopPortfolioSnapshots" };
      case (#UpdatePortfolioSnapshotInterval(_)) { "UpdatePortfolioSnapshotInterval" };
      case (#TakeManualSnapshot) { "TakeManualSnapshot" };
      case (#ExecuteTradingCycle) { "ExecuteTradingCycle" };
      case (#SetTestMode(_)) { "SetTestMode" };
      case (#ClearSystemLogs) { "ClearSystemLogs" };
      case (#CanisterStart) { "CanisterStart" };
      case (#CanisterStop) { "CanisterStop" };
    };
  };

  // Helper function to convert AdminCanisterSource to Text for indexing
  private func canisterSourceToText(source : AdminCanisterSource) : Text {
    switch (source) {
      case (#DAO_backend) { "DAO_backend" };
      case (#Treasury) { "Treasury" };
    };
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };

};
