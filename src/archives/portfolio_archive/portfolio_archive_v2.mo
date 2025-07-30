import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Map "mo:map/Map";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Error "mo:base/Error";
import Array "mo:base/Array";

import ICRC3 "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import BatchImportTimer "../../helper/batch_import_timer";
import Logger "../../helper/logger";

shared (deployer) actor class PortfolioArchiveV2() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type PortfolioBlockData = ArchiveTypes.PortfolioBlockData;
  type AllocationBlockData = ArchiveTypes.AllocationBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type TokenSnapshot = TreasuryTypes.TokenSnapshot;
  type PortfolioSnapshot = TreasuryTypes.PortfolioSnapshot;

  // Initialize the generic base class with portfolio-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 1000000;
    blockRetentionPeriodNS = 31536000000000000;
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  private let base = ArchiveBase.ArchiveBase<PortfolioBlockData>(
    this_canister_id(),
    ["3portfolio", "3allocation"],
    initialConfig
  );

  // Portfolio-specific indexes
  private stable var userIndex = Map.new<Principal, [Nat]>();
  private stable var valueIndex = Map.new<Nat, [Nat]>();

  // Tracking state for batch imports
  private stable var lastPortfolioImportTime : Int = 0;

  // Treasury interface for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let TREASURY_ID = canister_ids.getCanisterId(#treasury);
  private let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(TREASURY_ID));

  //=========================================================================
  // ICRC-3 Standard endpoints (delegated to base class)
  //=========================================================================

  public query func icrc3_get_archives(args : ICRC3.GetArchivesArgs) : async ICRC3.GetArchivesResult {
    base.icrc3_get_archives(args);
  };

  public query func icrc3_get_tip_certificate() : async ?ICRC3.DataCertificate {
    base.icrc3_get_tip_certificate();
  };

  public query func icrc3_get_blocks(args : ICRC3.GetBlocksArgs) : async ICRC3.GetBlocksResult {
    base.icrc3_get_blocks(args);
  };

  public query func icrc3_supported_block_types() : async [ICRC3.BlockType] {
    base.icrc3_supported_block_types();
  };

  //=========================================================================
  // Custom Portfolio Archive Functions
  //=========================================================================

  public shared ({ caller }) func archivePortfolioBlock(portfolio : PortfolioBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized portfolio archive attempt by: " # Principal.toText(caller), "archivePortfolioBlock");
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.portfolioToValue(portfolio, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock(
      blockValue,
      "3portfolio",
      portfolio.activeTokens,
      portfolio.timestamp
    );
    
    // Update portfolio-specific indexes
    base.addToIndex(valueIndex, portfolio.totalValueICP, blockIndex, Map.nhash);

    base.logger.info("Archive", "Archived portfolio block at index: " # Nat.toText(blockIndex) # 
      " Value: " # Nat.toText(portfolio.totalValueICP) # " ICP", "archivePortfolioBlock");

    #ok(blockIndex);
  };

  //=========================================================================
  // Batch Import System
  //=========================================================================

  // Import batch of portfolio snapshots from treasury
  private func importPortfolioSnapshotsBatch() : async { imported: Nat; failed: Nat } {
    try {
      let result = await treasuryCanister.getPortfolioHistory(50); // Get 50 snapshots
      
      switch (result) {
        case (#ok(response)) {
          var imported = 0;
          var failed = 0;
          
          // Filter snapshots newer than last imported
          let newSnapshots = Array.filter<TreasuryTypes.PortfolioSnapshot>(response.snapshots, func(snapshot) {
            snapshot.timestamp > lastPortfolioImportTime
          });
          
          for (snapshot in newSnapshots.vals()) {
            let portfolioBlock : PortfolioBlockData = {
              timestamp = snapshot.timestamp;
              totalValueICP = snapshot.totalValueICP;
              totalValueUSD = snapshot.totalValueUSD;
              tokenCount = snapshot.tokens.size();
              activeTokens = Array.map<TreasuryTypes.TokenSnapshot, Principal>(snapshot.tokens, func(token) = token.token);
              pausedTokens = [];
              reason = #Scheduled; // Default for imported snapshots
            };
            
            let archiveResult = await archivePortfolioBlock(portfolioBlock);
            switch (archiveResult) {
              case (#ok(_)) {
                imported += 1;
                lastPortfolioImportTime := Int.max(lastPortfolioImportTime, snapshot.timestamp);
              };
              case (#err(error)) {
                failed += 1;
                base.logger.error(
                  "BATCH_IMPORT",
                  "Failed to import portfolio snapshot: " # debug_show(error),
                  "importPortfolioSnapshotsBatch"
                );
              };
            };
          };
          
          if (imported > 0) {
            base.logger.info(
              "BATCH_IMPORT",
              "Imported " # Nat.toText(imported) # " portfolio snapshots, failed " # Nat.toText(failed),
              "importPortfolioSnapshotsBatch"
            );
          };
          
          { imported = imported; failed = failed };
        };
        case (#err(error)) {
          base.logger.error(
            "BATCH_IMPORT", 
            "Failed to get portfolio history: " # debug_show(error),
            "importPortfolioSnapshotsBatch"
          );
          { imported = 0; failed = 1 };
        };
      };
    } catch (e) {
      base.logger.error(
        "BATCH_IMPORT",
        "Exception in importPortfolioSnapshotsBatch: " # Error.message(e),
        "importPortfolioSnapshotsBatch"
      );
      { imported = 0; failed = 1 };
    };
  };

  // Three-tier timer system - portfolio import function
  private func importPortfolioOnly() : async {imported: Nat; failed: Nat} {
    base.logger.info("INNER_LOOP", "Starting portfolio import batch", "importPortfolioOnly");
    await importPortfolioSnapshotsBatch();
  };

  // Legacy compatibility - combined import
  private func runPortfolioBatchImport() : async () {
    base.logger.info(
      "BATCH_IMPORT",
      "Starting portfolio batch import cycle",
      "runPortfolioBatchImport"
    );
    
    let result = await importPortfolioSnapshotsBatch();
    
    base.logger.info(
      "BATCH_IMPORT",
      "Portfolio batch import completed - Imported: " # Nat.toText(result.imported) # 
      " Failed: " # Nat.toText(result.failed),
      "runPortfolioBatchImport"
    );
  };

  // Advanced batch import using three-tier timer system
  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startAdvancedBatchImportSystem<system>(caller, null, null, ?importPortfolioOnly);
  };
  
  // Legacy compatibility - combined import
  public shared ({ caller }) func startLegacyBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startBatchImportSystem<system>(caller, runPortfolioBatchImport);
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    base.stopBatchImportSystem(caller);
  };
  
  // Emergency stop all timers
  public shared ({ caller }) func stopAllTimers() : async Result.Result<Text, Text> {
    base.stopAllTimers(caller);
  };

  // Advanced manual import using three-tier timer system
  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, Text> {
    await base.runAdvancedManualBatchImport<system>(caller, null, null, ?importPortfolioOnly);
  };
  
  // Legacy compatibility - combined import
  public shared ({ caller }) func runLegacyManualBatchImport() : async Result.Result<Text, Text> {
    await base.runManualBatchImport(caller, runPortfolioBatchImport);
  };
  
  // Timer configuration
  public shared ({ caller }) func setMaxInnerLoopIterations(iterations: Nat) : async Result.Result<Text, Text> {
    base.setMaxInnerLoopIterations(caller, iterations);
  };

  // Legacy status for compatibility
  public query func getBatchImportStatus() : async {
    isRunning: Bool; 
    intervalSeconds: Nat;
    lastPortfolioImportTime: Int;
  } {
    let baseStatus = base.getBatchImportStatus();
    {
      isRunning = baseStatus.isRunning;
      intervalSeconds = baseStatus.intervalSeconds;
      lastPortfolioImportTime = lastPortfolioImportTime;
    };
  };
  
  // Comprehensive three-tier timer status
  public query func getTimerStatus() : async BatchImportTimer.TimerStatus {
    base.getTimerStatus();
  };

  // Admin method to reset import timestamps and re-import all historical data
  public shared ({ caller }) func resetImportTimestamps() : async Result.Result<Text, Text> {
    // Only authorized users can call this method
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err("Unauthorized: Only admin can reset import timestamps");
    };

    // Reset tracking state
    lastPortfolioImportTime := 0;
    
    base.logger.info("Admin", "Import timestamps reset by " # Principal.toText(caller) # " - will re-import all historical data on next batch", "resetImportTimestamps");
    
    #ok("Import timestamps reset successfully. Next batch import will re-import all historical portfolio data.")
  };

  //=========================================================================
  // Admin Functions (delegated to base class)
  //=========================================================================

  public shared ({ caller }) func updateConfig(newConfig : ArchiveTypes.ArchiveConfig) : async Result.Result<Text, ArchiveError> {
    base.updateConfig(newConfig, caller);
  };

  public query ({ caller }) func getArchiveStatus() : async Result.Result<ArchiveTypes.ArchiveStatus, ArchiveError> {
    base.getArchiveStatus(caller);
  };

  // Public archive statistics (no authorization required)
  public query func getArchiveStats() : async ArchiveTypes.ArchiveStatus {
    // Try to get block count by testing if blocks exist
    let testBlocks = base.icrc3_get_blocks([{start = 0; length = 10}]);
    let actualBlockCount = testBlocks.blocks.size();
    
    // Use the larger of nextBlockIndex or actual block count found
    let totalBlocks = if (actualBlockCount > 0 and actualBlockCount > base.nextBlockIndex) {
      actualBlockCount; // Use actual count if we found blocks and it's higher
    } else {
      base.nextBlockIndex; // Use nextBlockIndex as fallback
    };
    
    let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
    let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };
    
    {
      totalBlocks = totalBlocks;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = ["portfolio"];
      storageUsed = 0;
      lastArchiveTime = lastPortfolioImportTime;
    }
  };

  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    base.getLogs(count, caller);
  };

  //=========================================================================
  // Lifecycle Functions (delegated to base class)
  //=========================================================================

  system func preupgrade() {
    base.preupgrade();
  };

  system func postupgrade() {
    base.postupgrade<system>(runPortfolioBatchImport);
  };
} 