import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Map "mo:map/Map";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Bool "mo:base/Bool";
import Text "mo:base/Text";
import Error "mo:base/Error";

import ICRC3 "mo:icrc3-mo";                    // ← THE FIX: Use the actual library, not just types
import ICRC3Service "mo:icrc3-mo/service";        // ← Keep service types for API
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";

shared (deployer) actor class PriceArchiveV2() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type PriceBlockData = ArchiveTypes.PriceBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type TokenDetails = DAO_types.TokenDetails;

  // ICRC3 State Management for Scalable Storage (500GB)
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  // Initialize the generic base class with price-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 1000000; // 1M blocks per canister
    blockRetentionPeriodNS = 31536000000000000; // 1 year in nanoseconds
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  private let base = ArchiveBase.ArchiveBase<PriceBlockData>(
    this_canister_id(),
    ["3price"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Price-specific indexes (not covered by base class)
  private stable var priceRangeIndex = Map.new<Nat, [Nat]>(); // Price range -> block indices
  
  // Price-specific statistics
  private stable var totalPriceUpdates : Nat = 0;
  private stable var lastKnownPrices = Map.new<Principal, {icpPrice: Nat; usdPrice: Float; timestamp: Int}>();

  // Tracking state for batch imports
  private stable var lastImportedPriceTime : Int = 0;

  // Treasury interface for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let TREASURY_ID = canister_ids.getCanisterId(#treasury);
  private let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(TREASURY_ID));

  // Helper function to get price range bucket for indexing
  private func getPriceRangeBucket(priceICP : Nat) : Nat {
    // Group prices into buckets (e.g., every 1000 ICP units)
    priceICP / 1000;
  };

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
  // Custom Price Archive Functions
  //=========================================================================

  public shared ({ caller }) func archivePriceBlock<system>(price : PriceBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized price archive attempt by: " # Principal.toText(caller), "archivePriceBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    let blockValue = ArchiveTypes.priceToValue(price, timestamp, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3price",
      [price.token],
      timestamp
    );
    
    // Update price-specific indexes
    base.addToIndex(priceRangeIndex, getPriceRangeBucket(price.priceICP), blockIndex, Map.nhash);

    // Update price-specific statistics and tracking
    totalPriceUpdates += 1;
    let priceInfo = {
      icpPrice = price.priceICP;
      usdPrice = price.priceUSD;
      timestamp = timestamp;
    };
    Map.set(lastKnownPrices, Map.phash, price.token, priceInfo);

    base.logger.info("Archive", "Archived price block at index: " # Nat.toText(blockIndex) # 
      " Token: " # Principal.toText(price.token) # 
      " Price: " # Nat.toText(price.priceICP) # " ICP", "archivePriceBlock");

    #ok(blockIndex);
  };

  //=========================================================================
  // Query Functions (leveraging base class)
  //=========================================================================

  public query ({ caller }) func queryBlocks(filter : ArchiveTypes.BlockFilter) : async Result.Result<ArchiveTypes.ArchiveQueryResult, ArchiveError> {
    base.queryBlocks(filter, caller);
  };

  public query ({ caller }) func getPriceHistory(token : Principal, startTime : Int, endTime : Int) : async Result.Result<[PriceBlockData], ArchiveError> {
    if (not base.isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };
    
    // This would implement price history query logic
    // For now, return empty array as placeholder
    #ok([]);
  };

  public query ({ caller }) func getLatestPrice(token : Principal) : async Result.Result<?{icpPrice: Nat; usdPrice: Float; timestamp: Int}, ArchiveError> {
    if (not base.isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    #ok(Map.get(lastKnownPrices, Map.phash, token));
  };

  // New method for rewards calculation - get price at specific time
  public query ({ caller }) func getPriceAtTime(token : Principal, timestamp : Int) : async Result.Result<?{icpPrice: Nat; usdPrice: Float; timestamp: Int}, ArchiveError> {
    if (not base.isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    // For now, return the latest known price as placeholder
    // This method will need to:
    // 1. Query ICRC3 blocks to find price at or before the timestamp
    // 2. Return the most recent price before the given timestamp
    // 3. Return null if no price exists before that timestamp
    
    // Temporary implementation: return latest price if it exists and is before timestamp
    switch (Map.get(lastKnownPrices, Map.phash, token)) {
      case (?priceInfo) {
        if (priceInfo.timestamp <= timestamp) {
          #ok(?priceInfo);
        } else {
          #ok(null); // No price data before this timestamp
        };
      };
      case (null) { #ok(null) };
    };
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
    let totalBlocks = base.getTotalBlocks();
    let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
    let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };
    
    {
      totalBlocks = totalBlocks;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = ["price"];
      storageUsed = 0;
      lastArchiveTime = lastImportedPriceTime;
    }
  };

  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    base.getLogs(count, caller);
  };

  //=========================================================================
  // Batch Import System (delegated to base class)
  //=========================================================================

  // Specific batch import logic for price data
  // Import historical price data from treasury
  private func importPriceHistoryBatch() : async { imported: Nat; failed: Nat } {
    try {
      // Get the earliest timestamp we need to import from across all tokens
      var earliestTimestamp = Time.now(); // Start with current time
      for ((token, lastPrice) in Map.entries(lastKnownPrices)) {
        if (lastPrice.timestamp < earliestTimestamp) {
          earliestTimestamp := lastPrice.timestamp;
        };
      };
      
      // If no previous prices, start from 0 (get all history)
      if (Map.size(lastKnownPrices) == 0) {
        earliestTimestamp := 0;
      };
      
      // Use new efficient method that filters on server-side
      let tokenDetails = await treasuryCanister.getTokenDetailsSince(earliestTimestamp);
      var imported = 0;
      var failed = 0;
      
      base.logger.info("Batch Import", "Retrieved " # Nat.toText(tokenDetails.size()) # " tokens from treasury with price history since " # Int.toText(earliestTimestamp), "importPriceHistoryBatch");
      
      for ((token, details) in tokenDetails.vals()) {
        base.logger.info("Batch Import", "Processing token " # Principal.toText(token) # " (" # details.tokenSymbol # ") with " # Nat.toText(details.pastPrices.size()) # " filtered price points", "importPriceHistoryBatch");
        // Get last known timestamp for this token
        let lastKnownTime = switch (Map.get(lastKnownPrices, Map.phash, token)) {
          case (?lastPrice) { lastPrice.timestamp };
          case null { 0 };
        };
        
        // Further filter by specific token's timestamp (server filtered globally, now filter per token)
        let newPricePoints = Array.filter<TreasuryTypes.PricePoint>(details.pastPrices, func(point) {
          point.time > lastKnownTime
        });
        
        base.logger.info("Batch Import", "Token " # details.tokenSymbol # ": lastKnownTime=" # Int.toText(lastKnownTime) # ", final filtered to " # Nat.toText(newPricePoints.size()) # " new price points", "importPriceHistoryBatch");
        
        // Import each new price point from historical data
        for (pricePoint in newPricePoints.vals()) {
          let priceData : PriceBlockData = {
            token = token;
            priceICP = pricePoint.icpPrice;
            priceUSD = pricePoint.usdPrice;
            source = #NTN;
            volume24h = null;
            change24h = null;
          };
          
          let result = await archivePriceBlock(priceData);
          switch (result) {
            case (#ok(_)) { 
              imported += 1;
              // Update last known price for this token
              Map.set(lastKnownPrices, Map.phash, token, {
                icpPrice = pricePoint.icpPrice;
                usdPrice = pricePoint.usdPrice;
                timestamp = pricePoint.time;
              });
              lastImportedPriceTime := Int.max(lastImportedPriceTime, pricePoint.time);
            };
            case (#err(e)) { 
              failed += 1;
              base.logger.error("Batch Import", "Failed to import price for " # Principal.toText(token) # " at time " # Int.toText(pricePoint.time) # ": " # debug_show(e), "importPriceHistoryBatch");
            };
          };
        };
      };
      
      if (imported > 0) {
        base.logger.info(
          "BATCH_IMPORT",
          "Imported " # Nat.toText(imported) # " price history points, failed " # Nat.toText(failed),
          "importPriceHistoryBatch"
        );
      };
      
      { imported = imported; failed = failed };
    } catch (e) {
      base.logger.error(
        "BATCH_IMPORT",
        "Exception in importPriceHistoryBatch: " # Error.message(e),
        "importPriceHistoryBatch"
      );
      { imported = 0; failed = 1 };
    };
  };

  // Three-tier timer system - price import function
  private func importPriceOnly() : async {imported: Nat; failed: Nat} {
    base.logger.info("INNER_LOOP", "Starting price import batch", "importPriceOnly");
    await importPriceHistoryBatch();
  };

  // Legacy compatibility - combined import
  private func runPriceBatchImport() : async () {
    base.logger.info(
      "BATCH_IMPORT",
      "Starting price batch import cycle",
      "runPriceBatchImport"
    );
    
    let result = await importPriceHistoryBatch();
    
    base.logger.info(
      "BATCH_IMPORT",
      "Price batch import completed - Imported: " # Nat.toText(result.imported) # 
      " Failed: " # Nat.toText(result.failed),
      "runPriceBatchImport"
    );
  };

  // Advanced batch import using three-tier timer system
  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startAdvancedBatchImportSystem<system>(caller, null, null, ?importPriceOnly);
  };
  
  // Legacy compatibility - combined import
  public shared ({ caller }) func startLegacyBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startBatchImportSystem<system>(caller, runPriceBatchImport);
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
    await base.runAdvancedManualBatchImport<system>(caller, null, null, ?importPriceOnly);
  };
  
  // Legacy compatibility - combined import
  public shared ({ caller }) func runLegacyManualBatchImport() : async Result.Result<Text, Text> {
    await base.runManualBatchImport(caller, runPriceBatchImport);
  };
  
  // Timer configuration
  public shared ({ caller }) func setMaxInnerLoopIterations(iterations: Nat) : async Result.Result<Text, Text> {
    base.setMaxInnerLoopIterations(caller, iterations);
  };

  // Legacy status for compatibility
  public query func getBatchImportStatus() : async {
    isRunning: Bool; 
    intervalSeconds: Nat;
    lastImportedPriceTime: Int;
  } {
    let baseStatus = base.getBatchImportStatus();
    {
      isRunning = baseStatus.isRunning;
      intervalSeconds = baseStatus.intervalSeconds;
      lastImportedPriceTime = lastImportedPriceTime;
    };
  };
  
  // Comprehensive three-tier timer status
  public query func getTimerStatus() : async BatchImportTimer.TimerStatus {
    base.getTimerStatus();
  };

  // Admin method to reset import timestamps and re-import all historical data
  public shared ({ caller }) func resetImportTimestamps() : async Result.Result<Text, Text> {
    // Only admin can call this method
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err("Unauthorized: Only admin can reset import timestamps");
    };

    // Reset tracking state
    lastImportedPriceTime := 0;
    lastKnownPrices := Map.new<Principal, {icpPrice: Nat; usdPrice: Float; timestamp: Int}>();
    
    base.logger.info("Admin", "Import timestamps reset by " # Principal.toText(caller) # " - will re-import all historical data on next batch", "resetImportTimestamps");
    
    #ok("Import timestamps reset successfully. Next batch import will re-import all historical price data.")
  };

  public shared ({ caller }) func catchUpImport() : async Result.Result<Text, Text> {
    let catchUpFunction = func() : async {imported: Nat; failed: Nat} {
      // Implement specific catch-up logic for price data
      {imported = 0; failed = 0}; // Placeholder
    };
    await base.catchUpImport(caller, catchUpFunction);
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
    base.postupgrade<system>(runPriceBatchImport);
  };
} 