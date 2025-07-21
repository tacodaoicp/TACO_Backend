import Time "mo:base/Time";
import Timer "mo:base/Timer";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Map "mo:map/Map";
import Vector "mo:vector";
import BTree "mo:stableheapbtreemap/BTree";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Text "mo:base/Text";
import Debug "mo:base/Debug";
import Error "mo:base/Error";
import Blob "mo:base/Blob";
import Option "mo:base/Option";
import Hash "mo:base/Hash";

import ICRC3 "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import SpamProtection "../../helper/spam_protection";
import Logger "../../helper/logger";
import CanisterIds "../../helper/CanisterIds";
import BatchImportTimer "../../helper/batch_import_timer";
import ArchiveAuthorization "../../helper/archive_authorization";
import ArchiveICRC3 "../../helper/archive_icrc3";

shared (deployer) actor class TradingArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for batch import
  type PriceAlertLog = TreasuryTypes.PriceAlertLog;
  type TokenDetails = TreasuryTypes.TokenDetails;
  type TradeRecord = TreasuryTypes.TradeRecord;

  // Type aliases for convenience
  type Value = ArchiveTypes.Value;
  type Block = ArchiveTypes.Block;
  type TradeBlockData = ArchiveTypes.TradeBlockData;
  type CircuitBreakerBlockData = ArchiveTypes.CircuitBreakerBlockData;
  type TradingPauseBlockData = ArchiveTypes.TradingPauseBlockData;
  type BlockFilter = ArchiveTypes.BlockFilter;
  type TradingMetrics = ArchiveTypes.TradingMetrics;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type ArchiveConfig = ArchiveTypes.ArchiveConfig;
  type ArchiveStatus = ArchiveTypes.ArchiveStatus;
  type TacoBlockType = ArchiveTypes.TacoBlockType;
  type ArchiveQueryResult = ArchiveTypes.ArchiveQueryResult;

  // Logger
  let logger = Logger.Logger();

  // Canister IDs
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
  let TREASURY_ID = canister_ids.getCanisterId(#treasury);

  // Spam protection
  let spamGuard = SpamProtection.SpamGuard(this_canister_id());

  // Map utilities
  let { phash; thash; nhash } = Map;
  let ihash = Map.ihash;
  
  // Helper function to convert timestamp to day
  private func timestampToDay(timestamp : Int) : Int {
    timestamp / 86400000000000; // Convert nanoseconds to days
  };

  // Master admins
  var masterAdmins = [
    Principal.fromText("d7zib-qo5mr-qzmpb-dtyof-l7yiu-pu52k-wk7ng-cbm3n-ffmys-crbkz-nae"),
    Principal.fromText("uuyso-zydjd-tsb4o-lgpgj-dfsvq-awald-j2zfp-e6h72-d2je3-whmjr-xae"),
    Principal.fromText("5uvsz-em754-ulbgb-vxihq-wqyzd-brdgs-snzlu-mhlqw-k74uu-4l5h3-2qe"),
    Principal.fromText("6mxg4-njnu6-qzizq-2ekit-rnagc-4d42s-qyayx-jghoe-nd72w-elbsy-xqe"),
    Principal.fromText("6q3ra-pds56-nqzzc-itigw-tsw4r-vs235-yqx5u-dg34n-nnsus-kkpqf-aqe"),
    Principal.fromText("chxs6-z6h3t-hjrgk-i5x57-rm7fm-3tvlz-b352m-heq2g-hu23b-sxasf-kqe"),
    Principal.fromText("k2xol-5avzc-lf3wt-vwoft-pjx6k-77fjh-7pera-6b7qt-fwt5e-a3ekl-vqe"),
    Principal.fromText("qgjut-u3ase-3lxef-vcxtr-4g6jb-mazlw-jpins-wrvpv-jt5wn-2nrx6-sae"),
    Principal.fromText("as6jn-gaoo7-k4kji-tdkxg-jlsrk-avxkc-zu76j-vz7hj-di3su-2f74z-qqe"),
    Principal.fromText("r27hb-ckxon-xohqv-afcvx-yhemm-xoggl-37dg6-sfyt3-n6jer-ditge-6qe"),
    Principal.fromText("yjdlk-jqx52-ha6xa-w6iqe-b4jrr-s5ova-mirv4-crlfi-xgsaa-ib3cg-3ae"),
  ];

  // Configuration
  stable var config : ArchiveConfig = {
    maxBlocksPerCanister = 1000000; // 1M blocks per canister
    blockRetentionPeriodNS = 31536000000000000; // 1 year in nanoseconds
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  // ICRC-3 Block storage - using BTree for efficient range queries
  stable var blocks = BTree.init<Nat, Block>(?64);
  stable var nextBlockIndex : Nat = 0;
  stable var tipHash : ?Blob = null;

  // Block type tracking - focused on trading-related types
  stable var supportedBlockTypes = ["3trade", "3circuit", "3pause"];

  // Indexes for efficient querying
  stable var blockTypeIndex = Map.new<Text, [Nat]>(); // Block type -> block indices
  stable var traderIndex = Map.new<Principal, [Nat]>(); // Trader -> block indices
  stable var tokenIndex = Map.new<Principal, [Nat]>(); // Token -> block indices
  stable var timeIndex = Map.new<Int, [Nat]>(); // Day timestamp -> block indices

  // Statistics
  stable var totalTrades : Nat = 0;
  stable var totalSuccessfulTrades : Nat = 0;
  stable var totalVolume : Nat = 0;
  stable var lastArchiveTime : Int = 0;

  // Initialize authorization helper
  private let auth = ArchiveAuthorization.ArchiveAuthorization(
    masterAdmins,
    TREASURY_ID,
    DAO_BACKEND_ID,
    this_canister_id
  );

  // Authorization helper functions using abstraction
  private func isMasterAdmin(caller : Principal) : Bool {
    auth.isMasterAdmin(caller);
  };

  private func isAuthorized(caller : Principal, function : ArchiveTypes.AdminFunction) : Bool {
    auth.isAuthorized(caller, function);
  };

  private func isQueryAuthorized(caller : Principal) : Bool {
    auth.isQueryAuthorized(caller);
  };

  // Batch import timer using abstraction
  private let batchTimer = BatchImportTimer.BatchImportTimer(
    logger,
    BatchImportTimer.DEFAULT_CONFIG,
    isMasterAdmin
  );

  // ICRC-3 functionality using abstraction
  private let icrc3 = ArchiveICRC3.ArchiveICRC3(
    func() : BTree.BTree<Nat, ArchiveTypes.Block> { blocks },
    func() : Nat { nextBlockIndex },
    this_canister_id,
    supportedBlockTypes
  );

  private func addToIndex<K>(index : Map.Map<K, [Nat]>, key : K, blockIndex : Nat, hash : Map.HashUtils<K>) {
    let existing = switch (Map.get(index, hash, key)) {
      case (?ids) { ids };
      case null { [] };
    };
    Map.set(index, hash, key, Array.append(existing, [blockIndex]));
  };

  // Hash calculation for blocks using ICRC-3 representation-independent hashing
  private func calculateBlockHash(block : Value) : Blob {
    // This is a simplified hash calculation
    // In a production environment, you'd want to implement proper ICRC-3 RI hashing
    let blockText = debug_show(block);
    let textHash = Text.hash(blockText);
    let hash32 = textHash; // Text.hash returns Nat32
    let hashArray = [
      Nat8.fromNat(Nat32.toNat((hash32 >> 24) & 0xFF)),
      Nat8.fromNat(Nat32.toNat((hash32 >> 16) & 0xFF)),
      Nat8.fromNat(Nat32.toNat((hash32 >> 8) & 0xFF)),
      Nat8.fromNat(Nat32.toNat(hash32 & 0xFF))
    ];
    Blob.fromArray(hashArray);
  };

  // Create a new block with proper parent hash
  private func createBlock(blockValue : Value, blockIndex : Nat) : Block {
    // Get parent hash from previous block
    let phash = if (blockIndex == 0) {
      null;
    } else {
      switch (BTree.get(blocks, Nat.compare, blockIndex - 1)) {
        case (?prevBlock) { 
          ?calculateBlockHash(prevBlock.block);
        };
        case null { null };
      };
    };

    // Add parent hash to block if it exists
    let blockWithPhash = switch (phash) {
      case (?hash) {
        switch (blockValue) {
          case (#Map(entries)) {
            #Map(Array.append([("phash", #Blob(hash))], entries));
          };
          case _ { blockValue }; // Should not happen for ICRC-3 blocks
        };
      };
      case null { blockValue };
    };

    let block : Block = {
      id = blockIndex;
      block = blockWithPhash;
    };

    // Update tip hash
    tipHash := ?calculateBlockHash(blockWithPhash);
    
    block;
  };

  // ICRC-3 Standard Endpoints

  // ICRC-3 Standard endpoints (using abstraction)
  public query func icrc3_get_archives(args : ICRC3.GetArchivesArgs) : async ICRC3.GetArchivesResult {
    icrc3.icrc3_get_archives(args);
  };

  public query func icrc3_get_tip_certificate() : async ?ICRC3.DataCertificate {
    icrc3.icrc3_get_tip_certificate();
  };

  public query func icrc3_get_blocks(args : ICRC3.GetBlocksArgs) : async ICRC3.GetBlocksResult {
    icrc3.icrc3_get_blocks(args);
  };

  public query func icrc3_supported_block_types() : async [ICRC3.BlockType] {
    icrc3.icrc3_supported_block_types();
  };

  // Custom Archiving Functions

  public shared ({ caller }) func archiveTradeBlock(trade : TradeBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not isAuthorized(caller, #ArchiveData)) {
      logger.warn("Archive", "Unauthorized trade archive attempt by: " # Principal.toText(caller), "archiveTradeBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    let blockValue = ArchiveTypes.tradeToValue(trade, timestamp, null);
    let blockIndex = nextBlockIndex;
    let block = createBlock(blockValue, blockIndex);
    
    // Store block
    ignore BTree.insert(blocks, Nat.compare, blockIndex, block);
    
    // Update indexes
    addToIndex(blockTypeIndex, "3trade", blockIndex, thash);
    addToIndex(traderIndex, trade.trader, blockIndex, phash);
    addToIndex(tokenIndex, trade.tokenSold, blockIndex, phash);
    addToIndex(tokenIndex, trade.tokenBought, blockIndex, phash);
    addToIndex(timeIndex, timestampToDay(timestamp), blockIndex, ihash);

    // Update statistics
    totalTrades += 1;
    if (trade.success) {
      totalSuccessfulTrades += 1;
    };
    totalVolume += trade.amountSold;

    nextBlockIndex += 1;

    logger.info("Archive", "Archived trade block at index: " # Nat.toText(blockIndex) # 
      " for trader: " # Principal.toText(trade.trader), "archiveTradeBlock");

    #ok(blockIndex);
  };



  public shared ({ caller }) func archiveCircuitBreakerBlock(circuitBreaker : CircuitBreakerBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not isAuthorized(caller, #ArchiveData)) {
      logger.warn("Archive", "Unauthorized circuit breaker archive attempt by: " # Principal.toText(caller), "archiveCircuitBreakerBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    
    // Create circuit breaker block value
    let entries = [
      ("btype", #Text("3circuit")),
      ("ts", #Int(timestamp)),
      ("event_type", #Text(debug_show(circuitBreaker.eventType))),
      ("threshold_value", #Text(Float.toText(circuitBreaker.thresholdValue))),
      ("actual_value", #Text(Float.toText(circuitBreaker.actualValue))),
      ("system_response", #Text(circuitBreaker.systemResponse)),
      ("severity", #Text(circuitBreaker.severity)),
      ("tokens_affected", #Array(Array.map(circuitBreaker.tokensAffected, ArchiveTypes.principalToValue))),
    ];

    let entriesWithTrigger = switch (circuitBreaker.triggerToken) {
      case (?token) { Array.append(entries, [("trigger_token", ArchiveTypes.principalToValue(token))]) };
      case null { entries };
    };

    let blockValue = #Map(entriesWithTrigger);
    let blockIndex = nextBlockIndex;
    let block = createBlock(blockValue, blockIndex);
    
    // Store block
    ignore BTree.insert(blocks, Nat.compare, blockIndex, block);
    
    // Update indexes
    addToIndex(blockTypeIndex, "3circuit", blockIndex, thash);
    addToIndex(timeIndex, timestampToDay(timestamp), blockIndex, ihash);

    for (token in circuitBreaker.tokensAffected.vals()) {
      addToIndex(tokenIndex, token, blockIndex, phash);
    };

    nextBlockIndex += 1;

    logger.warn("Archive", "Archived circuit breaker block at index: " # Nat.toText(blockIndex), "archiveCircuitBreakerBlock");

    #ok(blockIndex);
  };


  public shared ({ caller }) func archiveTradingPauseBlock(pause : TradingPauseBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not isAuthorized(caller, #ArchiveData)) {
      logger.warn("Archive", "Unauthorized trading pause archive attempt by: " # Principal.toText(caller), "archiveTradingPauseBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    
    let reasonText = switch (pause.reason) {
      case (#PriceVolatility) { "price_volatility" };
      case (#LiquidityIssue) { "liquidity_issue" };
      case (#SystemMaintenance) { "system_maintenance" };
      case (#CircuitBreaker) { "circuit_breaker" };
      case (#AdminAction) { "admin_action" };
    };

    let entries = [
      ("btype", #Text("3pause")),
      ("ts", #Int(timestamp)),
      ("token", ArchiveTypes.principalToValue(pause.token)),
      ("token_symbol", #Text(pause.tokenSymbol)),
      ("reason", #Text(reasonText)),
    ];

    let entriesWithDuration = switch (pause.duration) {
      case (?dur) { Array.append(entries, [("duration", #Int(dur))]) };
      case null { entries };
    };

    let blockValue = #Map(entriesWithDuration);
    let blockIndex = nextBlockIndex;
    let block = createBlock(blockValue, blockIndex);
    
    // Store block
    ignore BTree.insert(blocks, Nat.compare, blockIndex, block);
    
    // Update indexes
    addToIndex(blockTypeIndex, "3pause", blockIndex, thash);
    addToIndex(tokenIndex, pause.token, blockIndex, phash);
    addToIndex(timeIndex, timestampToDay(timestamp), blockIndex, ihash);

    nextBlockIndex += 1;

    logger.warn("Archive", "Archived trading pause block at index: " # Nat.toText(blockIndex) # 
      " for token: " # pause.tokenSymbol, "archiveTradingPauseBlock");

    #ok(blockIndex);
  };



  // Query Functions

  public query ({ caller }) func queryBlocks(filter : BlockFilter) : async Result.Result<ArchiveQueryResult, ArchiveError> {
    if (not isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    // Get blocks based on filter criteria
    let candidateBlocks = Vector.new<Block>();
    
    // If no specific filters, get all blocks in time range
    if (filter.blockTypes == null and filter.tokens == null and filter.traders == null) {
      let startTime = Option.get(filter.startTime, 0);
      let endTime = Option.get(filter.endTime, Time.now());
      
      let results = BTree.scanLimit(blocks, Nat.compare, 0, nextBlockIndex, #fwd, 1000);
      for ((_, block) in results.results.vals()) {
        Vector.add(candidateBlocks, block);
      };
    } else {
      // Use indexes to find relevant blocks
      let blockIndices = Vector.new<Nat>();
      
      // Filter by block type
      switch (filter.blockTypes) {
        case (?types) {
          for (btype in types.vals()) {
            let btypeStr = ArchiveTypes.blockTypeToString(btype);
            switch (Map.get(blockTypeIndex, thash, btypeStr)) {
              case (?indices) {
                for (idx in indices.vals()) {
                  Vector.add(blockIndices, idx);
                };
              };
              case null {};
            };
          };
        };
        case null {};
      };
      
      // Get blocks from indices
      for (idx in Vector.vals(blockIndices)) {
        switch (BTree.get(blocks, Nat.compare, idx)) {
          case (?block) { Vector.add(candidateBlocks, block) };
          case null {};
        };
      };
    };

    #ok({
      blocks = Vector.toArray(candidateBlocks);
      totalCount = Vector.size(candidateBlocks);
      hasMore = false;
      nextIndex = null;
    });
  };

  public query ({ caller }) func getTradingMetrics(startTime : Int, endTime : Int) : async Result.Result<TradingMetrics, ArchiveError> {
    if (not isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    // Calculate trading metrics from archived data
    // This is a simplified implementation
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



  // Admin Functions

  public shared ({ caller }) func updateConfig(newConfig : ArchiveConfig) : async Result.Result<Text, ArchiveError> {
    if (not isAuthorized(caller, #UpdateConfig)) {
      return #err(#NotAuthorized);
    };

    config := newConfig;
    logger.info("Archive", "Configuration updated by: " # Principal.toText(caller), "updateConfig");
    
    #ok("Configuration updated successfully");
  };

  public query ({ caller }) func getArchiveStatus() : async Result.Result<ArchiveStatus, ArchiveError> {
    if (not isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    let oldestBlock = if (nextBlockIndex > 0) { ?0 } else { null };
    let newestBlock = if (nextBlockIndex > 0) { ?(nextBlockIndex - 1) } else { null };

    #ok({
      totalBlocks = nextBlockIndex;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = supportedBlockTypes;
      storageUsed = 0; // Would calculate actual storage usage
      lastArchiveTime = lastArchiveTime;
    });
  };

  // Logging functions
  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    if (not isAuthorized(caller, #GetLogs)) {
      return [];
    };
    
    logger.getLastLogs(count);
  };

  //=========================================================================
  // BATCH IMPORT SYSTEM FOR TREASURY DATA
  //=========================================================================
  
  // Tracking state for batch imports
  private stable var lastImportedTradeTimestamp : Int = 0;
  private stable var lastImportedPriceAlertId : Nat = 0;

  // Treasury interface for batch imports
  private let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(canister_ids.getCanisterId(#treasury)));

  /**
   * Import batch of trades from treasury
   */
  private func importTradesBatch() : async { imported: Nat; failed: Nat } {
    try {
      let tradingStatusResult = await treasuryCanister.getTradingStatus();
      
      switch (tradingStatusResult) {
        case (#ok(status)) {
          let trades = status.executedTrades;
          var imported = 0;
          var failed = 0;
          
          // Filter trades newer than last imported
          let newTrades = Array.filter<TradeRecord>(trades, func(trade) {
            trade.timestamp > lastImportedTradeTimestamp
          });
          
          // Process trades in batches
          let batchedTrades = if (newTrades.size() > BatchImportTimer.DEFAULT_CONFIG.batchSize) {
            Array.subArray(newTrades, 0, BatchImportTimer.DEFAULT_CONFIG.batchSize)
          } else {
            newTrades
          };
          
          for (trade in batchedTrades.vals()) {
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
            };
            
            let blockResult = await archiveTradeBlock(tradeBlockData);
            
            switch (blockResult) {
              case (#ok(index)) {
                imported += 1;
                lastImportedTradeTimestamp := trade.timestamp;
              };
              case (#err(error)) {
                failed += 1;
                logger.error(
                  "BATCH_IMPORT",
                  "Failed to import trade: " # debug_show(error),
                  "importTradesBatch"
                );
              };
            };
          };
          
          if (imported > 0) {
            logger.info(
              "BATCH_IMPORT",
              "Imported " # Nat.toText(imported) # " trades, failed " # Nat.toText(failed),
              "importTradesBatch"
            );
          };
          
          { imported = imported; failed = failed };
        };
        case (#err(error)) {
          logger.error(
            "BATCH_IMPORT", 
            "Failed to get trading status: " # error,
            "importTradesBatch"
          );
          { imported = 0; failed = 1 };
        };
      };
    } catch (e) {
      logger.error(
        "BATCH_IMPORT",
        "Exception in importTradesBatch: " # Error.message(e),
        "importTradesBatch"
      );
      { imported = 0; failed = 1 };
    };
  };

  /**
   * Import batch of price alerts from treasury
   */
  private func importPriceAlertsBatch() : async { imported: Nat; failed: Nat } {
    try {
      let alertsResult = await treasuryCanister.getPriceAlerts(lastImportedPriceAlertId, BatchImportTimer.DEFAULT_CONFIG.batchSize);
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
          };
          
          let blockResult = await archiveCircuitBreakerBlock(circuitBreakerData);
          
          switch (blockResult) {
            case (#ok(index)) {
              imported += 1;
              lastImportedPriceAlertId := alert.id;
            };
            case (#err(error)) {
              failed += 1;
              logger.error(
                "BATCH_IMPORT",
                "Failed to import price alert: " # debug_show(error),
                "importPriceAlertsBatch"
              );
            };
          };
        };
      };
      
      if (imported > 0) {
        logger.info(
          "BATCH_IMPORT",
          "Imported " # Nat.toText(imported) # " price alerts, failed " # Nat.toText(failed),
          "importPriceAlertsBatch"
        );
      };
      
      { imported = imported; failed = failed };
    } catch (e) {
      logger.error(
        "BATCH_IMPORT",
        "Exception in importPriceAlertsBatch: " # Error.message(e),
        "importPriceAlertsBatch"
      );
      { imported = 0; failed = 1 };
    };
  };

  /**
   * Run complete batch import cycle
   */
  private func runBatchImport() : async () {
    logger.info(
      "BATCH_IMPORT",
      "Starting batch import cycle",
      "runBatchImport"
    );
    
    // Import trades
    let tradeResults = await importTradesBatch();
    
    // Import price alerts
    let alertResults = await importPriceAlertsBatch();
    
    let totalImported = tradeResults.imported + alertResults.imported;
    let totalFailed = tradeResults.failed + alertResults.failed;
    
    logger.info(
      "BATCH_IMPORT",
      "Batch import cycle completed - Imported: " # Nat.toText(totalImported) # 
      " Failed: " # Nat.toText(totalFailed),
      "runBatchImport"
    );
  };



  /**
   * Manual batch import (admin function)
   */
  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, ArchiveError> {
    let result = await batchTimer.adminManualImport(caller, runBatchImport);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  /**
   * Start batch import system (admin function)
   */
  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, ArchiveError> {
    let result = await batchTimer.adminStart<system>(caller, runBatchImport);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  /**
   * Stop batch import system (admin function)
   */
  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, ArchiveError> {
    let result = batchTimer.adminStop(caller);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  /**
   * Get batch import status
   */
  public query func getBatchImportStatus() : async {
    isRunning: Bool;
    lastImportedTradeTimestamp: Int;
    lastImportedPriceAlertId: Nat;
    intervalSeconds: Nat;
  } {
    {
      isRunning = batchTimer.isRunning();
      lastImportedTradeTimestamp = lastImportedTradeTimestamp;
      lastImportedPriceAlertId = lastImportedPriceAlertId;
      intervalSeconds = batchTimer.getIntervalSeconds();
    };
  };

  /**
   * Force catch-up import (admin function)
   */
  public shared ({ caller }) func catchUpImport() : async Result.Result<Text, ArchiveError> {
    // Custom catch-up function that combines trade and alert imports
    let catchUpFunction = func() : async {imported: Nat; failed: Nat} {
      let tradeResults = await importTradesBatch();
      let alertResults = await importPriceAlertsBatch();
      {
        imported = tradeResults.imported + alertResults.imported;
        failed = tradeResults.failed + alertResults.failed;
      }
    };
    
    let result = await batchTimer.runCatchUpImport(caller, catchUpFunction);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  //=========================================================================
  // SYSTEM INITIALIZATION - START BATCH IMPORT TIMER
  //=========================================================================

  system func preupgrade() {
    batchTimer.preupgrade();
  };

  system func postupgrade() {
    batchTimer.postupgrade<system>(runBatchImport);
  };

  // Setup authorization
  spamGuard.setAllowedCanisters([this_canister_id(), DAO_BACKEND_ID, TREASURY_ID]);
  spamGuard.setSelf(this_canister_id());

  logger.info("Archive", "ICRC-3 Trading Archive canister initialized with " # Nat.toText(supportedBlockTypes.size()) # " supported block types", "init");
}
