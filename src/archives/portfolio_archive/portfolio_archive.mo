import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Map "mo:map/Map";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Error "mo:base/Error";
import Array "mo:base/Array";

import ICRC3 "mo:icrc3-mo";                    // ← THE FIX: Use the actual library, not just types
import ICRC3Service "mo:icrc3-mo/service";        // ← Keep service types for API
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

  // ICRC3 State Management for Scalable Storage (500GB)
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

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
    initialConfig,
    deployer.caller,
    icrc3StateRef
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
  // Custom Portfolio Archive Functions
  //=========================================================================

  public shared ({ caller }) func archivePortfolioBlock<system>(portfolio : PortfolioBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized portfolio archive attempt by: " # Principal.toText(caller), "archivePortfolioBlock");
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.portfolioToValue(portfolio, null);
    
    // Extract Principal IDs from detailed tokens for indexing
    let tokenPrincipals = Array.map<ArchiveTypes.DetailedTokenSnapshot, Principal>(
      portfolio.tokens, 
      func(token) = token.token
    );
    
    // Use base class to store the block
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3portfolio",
      tokenPrincipals,
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
      // Use new efficient method that filters on server-side (100 per batch × 100 batches = 10,000 per cycle)
      let result = await treasuryCanister.getPortfolioHistorySince(lastPortfolioImportTime, 100);
      
      switch (result) {
        case (#ok(response)) {
          var imported = 0;
          var failed = 0;
          
          // No need to filter client-side anymore - server already filtered
          let newSnapshots = response.snapshots;
          
          // Sort snapshots chronologically (oldest first) for proper block ordering
          let sortedSnapshots = Array.sort<TreasuryTypes.PortfolioSnapshot>(newSnapshots, func(a, b) {
            Int.compare(a.timestamp, b.timestamp)
          });
          
          for (snapshot in sortedSnapshots.vals()) {
            // Convert treasury TokenSnapshots to archive DetailedTokenSnapshots (excluding symbol)
            let detailedTokens = Array.map<TreasuryTypes.TokenSnapshot, ArchiveTypes.DetailedTokenSnapshot>(
              snapshot.tokens, 
              func(token) : ArchiveTypes.DetailedTokenSnapshot = {
                token = token.token;
                balance = token.balance;
                decimals = token.decimals;
                priceInICP = token.priceInICP;
                priceInUSD = token.priceInUSD;
                valueInICP = token.valueInICP;
                valueInUSD = token.valueInUSD;
              }
            );

            // Map Treasury SnapshotReason to Archive SnapshotReason
            let archiveReason = switch (snapshot.snapshotReason) {
              case (#Manual) { #ManualTrigger };
              case (#PostTrade) { #PostTrade };
              case (#PreTrade) { #PostTrade };  // Map PreTrade to PostTrade as closest match
              case (#PriceUpdate) { #SystemEvent };  // Generic system event
              case (#Scheduled) { #Scheduled };
            };

            let portfolioBlock : PortfolioBlockData = {
              timestamp = snapshot.timestamp;
              totalValueICP = snapshot.totalValueICP;
              totalValueUSD = snapshot.totalValueUSD;
              tokenCount = snapshot.tokens.size();
              tokens = detailedTokens;  // Store full token details
              pausedTokens = [];        // No paused token data from treasury
              reason = archiveReason;   // Use mapped reason
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
  // Portfolio-specific Query Functions
  //=========================================================================

  // Binary search to find the smallest block index i where block[i].timestamp >= ts
  public query func lower_bound_ts(ts : Int) : async Result.Result<Nat, ArchiveError> {
    let totalBlocks = base.getTotalBlocks();
    
    // Handle empty archive
    if (totalBlocks == 0) {
      return #ok(0);
    };
    
    // Binary search implementation
    var left : Nat = 0;
    var right : Nat = totalBlocks;
    
    while (left < right) {
      let mid = left + (right - left) / 2;
      
      // Get the block at mid index
      let blockResult = base.icrc3_get_blocks([{start = mid; length = 1}]);
      
      if (blockResult.blocks.size() == 0) {
        // Block not found, search in left half
        right := mid;
      } else {
        let block = blockResult.blocks[0];
        
        // Extract timestamp from the block data
        switch (extractTimestampFromBlock(block.block)) {
          case (?blockTimestamp) {
            if (blockTimestamp >= ts) {
              // Found a candidate, continue searching left for smaller index
              right := mid;
            } else {
              // Block timestamp too small, search right half
              left := mid + 1;
            };
          };
          case null {
            // Unable to extract timestamp, skip this block by searching right
            left := mid + 1;
          };
        };
      };
    };
    
    // left is now the smallest index where block[index].timestamp >= ts
    // or totalBlocks if no such block exists
    #ok(left);
  };
  
  public query func getTimestampFromBlock(blockIndex : Nat) : async Result.Result<Int, ArchiveError> {
    let blockResult = base.icrc3_get_blocks([{start = blockIndex; length = 1}]);
    if (blockResult.blocks.size() > 0) {
      let block = blockResult.blocks[0];
      switch (extractTimestampFromBlock(block.block)) {
        case (?timestamp) { #ok(timestamp) };
        case null { #err(#InvalidData) };
      };
    } else {
      #err(#BlockNotFound);
    };
  };

  // Debug function to see the actual block structure
  public query func debugBlockStructure(blockIndex : Nat) : async Result.Result<Text, ArchiveError> {
    let blockResult = base.icrc3_get_blocks([{start = blockIndex; length = 1}]);
    if (blockResult.blocks.size() > 0) {
      let block = blockResult.blocks[0];
      #ok(debug_show(block.block));
    } else {
      #err(#BlockNotFound);
    };
  };
  // Helper function to extract timestamp from block data
  private func extractTimestampFromBlock(blockValue : ArchiveTypes.Value) : ?Int {
    switch (blockValue) {
      case (#Map(entries)) {
        // Look for the tx field which contains the transaction data
        for ((key, value) in entries.vals()) {
          if (key == "tx") {
            switch (value) {
              case (#Map(txEntries)) {
                // Look for data field in the tx
                for ((txKey, txValue) in txEntries.vals()) {
                  if (txKey == "data") {
                    switch (txValue) {
                      case (#Map(dataEntries)) {
                        // Look for ts field in the data (portfolio timestamp)
                        for ((dataKey, dataValue) in dataEntries.vals()) {
                          if (dataKey == "ts") {
                            switch (dataValue) {
                              case (#Int(timestamp)) {
                                return ?timestamp;
                              };
                              case _ {};
                            };
                          };
                        };
                      };
                      case _ {};
                    };
                  };
                };
              };
              case _ {};
            };
          };
        };
      };
      case _ {};
    };
    null;
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
    
    // Use the larger of getTotalBlocks() or actual block count found
    let totalBlocks = if (actualBlockCount > 0 and actualBlockCount > base.getTotalBlocks()) {
      actualBlockCount; // Use actual count if we found blocks and it's higher
    } else {
      base.getTotalBlocks(); // Use getTotalBlocks() as fallback
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
    icrc3State := icrc3StateRef.value;
    base.preupgrade();
  };

  system func postupgrade() {
    icrc3StateRef.value := icrc3State;
    base.postupgrade<system>(func() : async () { /* no-op */ });
  };
} 