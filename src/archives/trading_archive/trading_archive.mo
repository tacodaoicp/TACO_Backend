import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Map "mo:map/Map";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Text "mo:base/Text";
import Error "mo:base/Error";

import ICRC3 "mo:icrc3-mo";                    // ← THE FIX: Use actual library
import ICRC3Service "mo:icrc3-mo/service";    // ← Keep service types 
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";

shared (deployer) actor class TradingArchiveV2() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type TradeBlockData = ArchiveTypes.TradeBlockData;
  type CircuitBreakerBlockData = ArchiveTypes.CircuitBreakerBlockData;
  type TradingPauseBlockData = ArchiveTypes.TradingPauseBlockData;
  type PriceAlertLog = TreasuryTypes.PriceAlertLog;
  type TokenDetails = TreasuryTypes.TokenDetails;
  type TradeRecord = TreasuryTypes.TradeRecord;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type TradingMetrics = ArchiveTypes.TradingMetrics;

  // Initialize the generic base class with trading-specific configuration
  private let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 1000000; // 1M blocks per canister
    blockRetentionPeriodNS = 31536000000000000; // 1 year in nanoseconds
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  // ICRC3 State for scalable storage
  private stable var icrc3State : ?ICRC3.State = null;
  private var icrc3StateRef = { var value = icrc3State };

  private let base = ArchiveBase.ArchiveBase<TradeBlockData>(
    this_canister_id(),
    ["3trade", "3circuit", "3pause"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Trading-specific indexes (not covered by base class)
  private stable var traderIndex = Map.new<Principal, [Nat]>(); // Trader -> block indices

  // Trading-specific statistics
  private stable var totalTrades : Nat = 0;
  private stable var totalSuccessfulTrades : Nat = 0;
  private stable var totalVolume : Nat = 0;

  // Tracking state for batch imports
  private stable var lastImportedTradeTimestamp : Int = 0;
  private stable var lastImportedPriceAlertId : Nat = 0;

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
  // Custom Trading Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveTradeBlock<system>(trade : TradeBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized trade archive attempt by: " # Principal.toText(caller), "archiveTradeBlock");
      return #err(#NotAuthorized);
    };

    // Use original event timestamp from TradeBlockData, not import time!
    let timestamp = trade.timestamp;
    let blockValue = ArchiveTypes.tradeToValue(trade, timestamp, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3trade",
      [trade.tokenSold, trade.tokenBought],
      timestamp
    );
    
    // Update trading-specific indexes
    base.addToIndex(traderIndex, trade.trader, blockIndex, Map.phash);

    // Update trading-specific statistics
    totalTrades += 1;
    if (trade.success) {
      totalSuccessfulTrades += 1;
    };
    totalVolume += trade.amountSold;

    base.logger.info("Archive", "Archived trade block at index: " # Nat.toText(blockIndex) # 
      " for trader: " # Principal.toText(trade.trader), "archiveTradeBlock");

    #ok(blockIndex);
  };

  public shared ({ caller }) func archiveCircuitBreakerBlock<system>(circuitBreaker : CircuitBreakerBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized circuit breaker archive attempt by: " # Principal.toText(caller), "archiveCircuitBreakerBlock");
      return #err(#NotAuthorized);
    };

    // Use original event timestamp from CircuitBreakerBlockData, not import time!
    let timestamp = circuitBreaker.timestamp;
    let blockValue = ArchiveTypes.circuitBreakerToValue(circuitBreaker, timestamp, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3circuit",
      circuitBreaker.tokensAffected,
      timestamp
    );

    base.logger.error("Archive", "Archived circuit breaker block at index: " # Nat.toText(blockIndex) # 
      " Event: " # debug_show(circuitBreaker.eventType), "archiveCircuitBreakerBlock");

    #ok(blockIndex);
  };

  public shared ({ caller }) func archiveTradingPauseBlock<system>(pause : TradingPauseBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized trading pause archive attempt by: " # Principal.toText(caller), "archiveTradingPauseBlock");
      return #err(#NotAuthorized);
    };

    // Use original event timestamp from TradingPauseBlockData, not import time!
    let timestamp = pause.timestamp;
    let blockValue = ArchiveTypes.tradingPauseToValue(pause, timestamp, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3pause",
      [pause.token],
      timestamp
    );

    base.logger.warn("Archive", "Archived trading pause block at index: " # Nat.toText(blockIndex) # 
      " for token: " # pause.tokenSymbol, "archiveTradingPauseBlock");

    #ok(blockIndex);
  };

  //=========================================================================
  // Query Functions (leveraging base class)
  //=========================================================================

  public query ({ caller }) func queryBlocks(filter : ArchiveTypes.BlockFilter) : async Result.Result<ArchiveTypes.ArchiveQueryResult, ArchiveError> {
    base.queryBlocks(filter, caller);
  };

  public query ({ caller }) func getTradingMetrics(startTime : Int, endTime : Int) : async Result.Result<TradingMetrics, ArchiveError> {
    if (not base.isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    // Calculate trading metrics from archived data
    #ok({
      totalTrades = totalTrades;
      successfulTrades = totalSuccessfulTrades;
      totalVolume = totalVolume;
      uniqueTraders = Map.size(traderIndex);
      avgTradeSize = if (totalTrades > 0) { totalVolume / totalTrades } else { 0 };
      avgSlippage = 0.0; // Would calculate from trade data
      topTokensByVolume = []; // Would calculate from trade data  
      exchangeBreakdown = []; // Would calculate from trade data
    });
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
    // Get total blocks directly from ICRC3 library stats
    let totalBlocks = base.getTotalBlocks();
    let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
    let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };
    
    {
      totalBlocks = totalBlocks;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = ["trade", "circuit_breaker"];
      storageUsed = 0;
      lastArchiveTime = lastImportedTradeTimestamp;
    }
  };

  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    base.getLogs(count, caller);
  };

  //=========================================================================
  // Batch Import System (delegated to base class)
  //=========================================================================

  // Import batch of trades from treasury
  private func importTradesBatch() : async { imported: Nat; failed: Nat } {
    try {
      // Use new efficient method that filters on server-side
      let tradingStatusResult = await treasuryCanister.getTradingStatusSince(lastImportedTradeTimestamp);
      
      switch (tradingStatusResult) {
        case (#ok(status)) {
          let trades = status.executedTrades; // Already filtered by timestamp
          var imported = 0;
          var failed = 0;
          
          // No need to filter client-side anymore - server already filtered
          let newTrades = trades;
          
          // Process trades in batches (100 per batch × 100 batches = 10,000 per cycle)
          let batchSize = 100;
          let batchedTrades = if (newTrades.size() > batchSize) {
            Array.subArray(newTrades, 0, batchSize)
          } else {
            newTrades
          };
          
          // Sort trades chronologically (oldest first) for proper block ordering
          let sortedTrades = Array.sort<TreasuryTypes.TradeRecord>(batchedTrades, func(a, b) {
            Int.compare(a.timestamp, b.timestamp)
          });
          
          for (trade in sortedTrades.vals()) {
            let tradeBlockData : TradeBlockData = {
              trader = TREASURY_ID; // Treasury is the trader
              tokenSold = trade.tokenSold;
              tokenBought = trade.tokenBought;
              amountSold = trade.amountSold;
              amountBought = trade.amountBought;
              exchange = trade.exchange;
              success = trade.success;
              slippage = trade.slippage;
              fee = 0; // Treasury trades don't have explicit fees
              error = trade.error;
              timestamp = trade.timestamp; // Use original event timestamp!
            };
            
            let blockResult = await archiveTradeBlock(tradeBlockData);
            
            switch (blockResult) {
              case (#ok(index)) {
                imported += 1;
                lastImportedTradeTimestamp := trade.timestamp;
              };
              case (#err(error)) {
                failed += 1;
                base.logger.error(
                  "BATCH_IMPORT",
                  "Failed to import trade: " # debug_show(error),
                  "importTradesBatch"
                );
              };
            };
          };
          
          if (imported > 0) {
            base.logger.info(
              "BATCH_IMPORT",
              "Imported " # Nat.toText(imported) # " trades, failed " # Nat.toText(failed),
              "importTradesBatch"
            );
          };
          
          { imported = imported; failed = failed };
        };
        case (#err(error)) {
          base.logger.error(
            "BATCH_IMPORT", 
            "Failed to get trading status: " # error,
            "importTradesBatch"
          );
          { imported = 0; failed = 1 };
        };
      };
    } catch (e) {
      base.logger.error(
        "BATCH_IMPORT",
        "Exception in importTradesBatch: " # Error.message(e),
        "importTradesBatch"
      );
      { imported = 0; failed = 1 };
    };
  };

  // Import batch of price alerts from treasury
  private func importPriceAlertsBatch() : async { imported: Nat; failed: Nat } {
    try {
      let alertsResult = await treasuryCanister.getPriceAlerts(lastImportedPriceAlertId, 50);
      let alerts = alertsResult.alerts;
      var imported = 0;
      var failed = 0;
      
      for (alert in alerts.vals()) {
        if (alert.id > lastImportedPriceAlertId) {
          let circuitBreakerData : CircuitBreakerBlockData = {
            eventType = #PriceAlert;
            triggerToken = ?alert.token;
            thresholdValue = alert.triggeredCondition.percentage;
            actualValue = alert.priceData.actualChangePercent;
            tokensAffected = [alert.token];
            systemResponse = "Price alert triggered: " # alert.triggeredCondition.name;
            severity = "Medium";
            timestamp = alert.timestamp; // Use original alert timestamp
          };
          
          let blockResult = await archiveCircuitBreakerBlock(circuitBreakerData);
          
          switch (blockResult) {
            case (#ok(index)) {
              imported += 1;
              lastImportedPriceAlertId := alert.id;
            };
            case (#err(error)) {
              failed += 1;
              base.logger.error(
                "BATCH_IMPORT",
                "Failed to import price alert: " # debug_show(error),
                "importPriceAlertsBatch"
              );
            };
          };
        };
      };
      
      if (imported > 0) {
        base.logger.info(
          "BATCH_IMPORT",
          "Imported " # Nat.toText(imported) # " price alerts, failed " # Nat.toText(failed),
          "importPriceAlertsBatch"
        );
      };
      
      { imported = imported; failed = failed };
    } catch (e) {
      base.logger.error(
        "BATCH_IMPORT",
        "Exception in importPriceAlertsBatch: " # Error.message(e),
        "importPriceAlertsBatch"
      );
      { imported = 0; failed = 1 };
    };
  };

  // Three-tier timer system - trade import function
  private func importTradesOnly() : async {imported: Nat; failed: Nat} {
    base.logger.info("INNER_LOOP", "Starting trade import batch", "importTradesOnly");
    await importTradesBatch();
  };
  
  // Three-tier timer system - circuit breaker import function  
  private func importCircuitBreakersOnly() : async {imported: Nat; failed: Nat} {
    base.logger.info("INNER_LOOP", "Starting circuit breaker import batch", "importCircuitBreakersOnly");
    await importPriceAlertsBatch();
  };

  // Legacy compatibility - combined import
  private func runTradingBatchImport() : async () {
    base.logger.info(
      "BATCH_IMPORT",
      "Starting batch import cycle",
      "runTradingBatchImport"
    );
    
    // Import trades
    let tradeResults = await importTradesBatch();
    
    // Import price alerts
    let alertResults = await importPriceAlertsBatch();
    
    let totalImported = tradeResults.imported + alertResults.imported;
    let totalFailed = tradeResults.failed + alertResults.failed;
    
    base.logger.info(
      "BATCH_IMPORT",
      "Batch import cycle completed - Imported: " # Nat.toText(totalImported) # 
      " Failed: " # Nat.toText(totalFailed),
      "runTradingBatchImport"
    );
  };

  // Advanced batch import using three-tier timer system
  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startAdvancedBatchImportSystem<system>(caller, ?importTradesOnly, ?importCircuitBreakersOnly, null);
  };
  
  // Legacy compatibility - combined import
  public shared ({ caller }) func startLegacyBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startBatchImportSystem<system>(caller, runTradingBatchImport);
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
    await base.runAdvancedManualBatchImport<system>(caller, ?importTradesOnly, ?importCircuitBreakersOnly, null);
  };
  
  // Legacy compatibility - combined import
  public shared ({ caller }) func runLegacyManualBatchImport() : async Result.Result<Text, Text> {
    await base.runManualBatchImport(caller, runTradingBatchImport);
  };
  
  // Timer configuration
  public shared ({ caller }) func setMaxInnerLoopIterations(iterations: Nat) : async Result.Result<Text, Text> {
    base.setMaxInnerLoopIterations(caller, iterations);
  };

  // Legacy status for compatibility
  public query func getBatchImportStatus() : async {
    isRunning: Bool; 
    intervalSeconds: Nat;
    lastImportedTradeTimestamp: Int;
    lastImportedPriceAlertId: Nat;
  } {
    let baseStatus = base.getBatchImportStatus();
    {
      isRunning = baseStatus.isRunning;
      intervalSeconds = baseStatus.intervalSeconds;
      lastImportedTradeTimestamp = lastImportedTradeTimestamp;
      lastImportedPriceAlertId = lastImportedPriceAlertId;
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
    lastImportedTradeTimestamp := 0;
    lastImportedPriceAlertId := 0;
    
    base.logger.info("Admin", "Import timestamps reset by " # Principal.toText(caller) # " - will re-import all historical data on next batch", "resetImportTimestamps");
    
    #ok("Import timestamps reset successfully. Next batch import will re-import all historical trading data.")
  };

  public shared ({ caller }) func catchUpImport() : async Result.Result<Text, Text> {
    let catchUpFunction = func() : async {imported: Nat; failed: Nat} {
      // Implement specific catch-up logic for trading data
      {imported = 0; failed = 0}; // Placeholder
    };
    await base.catchUpImport(caller, catchUpFunction);
  };

  //=========================================================================
  // Lifecycle Functions (delegated to base class)
  //=========================================================================

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
} 