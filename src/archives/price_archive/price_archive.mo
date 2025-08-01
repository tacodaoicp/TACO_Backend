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

shared (deployer) actor class PriceArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for convenience
  type Value = ArchiveTypes.Value;
  type Block = ArchiveTypes.Block;
  type PriceBlockData = ArchiveTypes.PriceBlockData;
  type BlockFilter = ArchiveTypes.BlockFilter;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type ArchiveConfig = ArchiveTypes.ArchiveConfig;
  type ArchiveStatus = ArchiveTypes.ArchiveStatus;
  type TacoBlockType = ArchiveTypes.TacoBlockType;
  type ArchiveQueryResult = ArchiveTypes.ArchiveQueryResult;
  type TokenDetails = DAO_types.TokenDetails;
  type PricePoint = DAO_types.PricePoint;

  // Price source for tracking where price came from
  type PriceSource = {
    #Treasury;
    #Exchange: TreasuryTypes.ExchangeType;
    #NTN;
    #Oracle;
  };

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

  // Block type tracking - focused on price-related types
  stable var supportedBlockTypes = ["3price"];

  // Indexes for efficient querying (price-specific)
  stable var blockTypeIndex = Map.new<Text, [Nat]>(); // Block type -> block indices
  stable var tokenIndex = Map.new<Principal, [Nat]>(); // Token -> block indices
  stable var timeIndex = Map.new<Int, [Nat]>(); // Day timestamp -> block indices
  stable var priceRangeIndex = Map.new<Nat, [Nat]>(); // Price range -> block indices (for price-based queries)

  // Statistics
  stable var totalPriceUpdates : Nat = 0;
  stable var lastArchiveTime : Int = 0;
  stable var lastPriceImportTime : Int = 0;

  // Timer for periodic tasks
  private stable var periodicTimerId : Nat = 0;

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

  // Calculate price change percentage
  private func calculatePriceChange(oldPrice : Nat, newPrice : Nat) : Float {
    if (oldPrice == 0) { 0.0 }
    else {
      let change = if (newPrice > oldPrice) {
        Float.fromInt(Int.abs(newPrice - oldPrice)) / Float.fromInt(oldPrice)
      } else {
        -(Float.fromInt(Int.abs(oldPrice - newPrice)) / Float.fromInt(oldPrice))
      };
      change * 100.0; // Convert to percentage
    };
  };

  // Get price range bucket for indexing (e.g., 0-1 ICP, 1-10 ICP, etc.)
  private func getPriceRangeBucket(priceICP : Nat) : Nat {
    if (priceICP < 100_000_000) { 0 } // < 1 ICP
    else if (priceICP < 1_000_000_000) { 1 } // 1-10 ICP
    else if (priceICP < 10_000_000_000) { 2 } // 10-100 ICP
    else { 3 } // 100+ ICP
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

  // Archive price change event
  public shared ({ caller }) func archivePriceBlock(price : PriceBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not isAuthorized(caller, #ArchiveData)) {
      logger.warn("Archive", "Unauthorized price archive attempt by: " # Principal.toText(caller), "archivePriceBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    let blockValue = ArchiveTypes.priceToValue(price, timestamp, null);
    let blockIndex = nextBlockIndex;
    let block = createBlock(blockValue, blockIndex);
    
    // Store block
    ignore BTree.insert(blocks, Nat.compare, blockIndex, block);
    
    // Update indexes
    addToIndex(blockTypeIndex, "3price", blockIndex, thash);
    addToIndex(tokenIndex, price.token, blockIndex, phash);
    addToIndex(timeIndex, timestampToDay(timestamp), blockIndex, ihash);
    addToIndex(priceRangeIndex, getPriceRangeBucket(price.priceICP), blockIndex, nhash);

    // Update statistics
    totalPriceUpdates += 1;
    nextBlockIndex += 1;

    logger.info("Archive", "Archived price block at index: " # Nat.toText(blockIndex) # 
      " Token: " # Principal.toText(price.token) # 
      " New Price: " # Nat.toText(price.priceICP) # " ICP", "archivePriceBlock");

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
    if (filter.blockTypes == null and filter.tokens == null) {
      let startTime = Option.get(filter.startTime, 0);
      let endTime = Option.get(filter.endTime, Time.now());
      
      // Get blocks in range similar to icrc3_get_blocks pattern
      let endIndex = Nat.min(1000, nextBlockIndex);
      if (endIndex > 0) {
        for (i in Iter.range(0, endIndex - 1)) {
          switch (BTree.get(blocks, Nat.compare, i)) {
            case (?block) {
              Vector.add(candidateBlocks, block);
            };
            case null { /* Block not found */ };
          };
        };
      };
    } else {
      // Use indexes to find relevant blocks
      let blockIndices = Vector.new<Nat>();
      
      // Filter by block type (should be "3price" only)
      switch (filter.blockTypes) {
        case (?types) {
          for (blockType in types.vals()) {
            let typeStr = ArchiveTypes.blockTypeToString(blockType);
            switch (Map.get(blockTypeIndex, thash, typeStr)) {
              case (?indices) {
                for (index in indices.vals()) {
                  Vector.add(blockIndices, index);
                };
              };
              case null { /* No blocks of this type */ };
            };
          };
        };
        case null { /* No block type filter */ };
      };
      
      // Filter by tokens
      switch (filter.tokens) {
        case (?tokens) {
          for (token in tokens.vals()) {
            switch (Map.get(tokenIndex, phash, token)) {
              case (?indices) {
                for (index in indices.vals()) {
                  Vector.add(blockIndices, index);
                };
              };
              case null { /* No blocks for this token */ };
            };
          };
        };
        case null { /* No token filter */ };
      };
      
      // Get blocks by indices
      let uniqueIndices = Vector.toArray(blockIndices);
      for (index in uniqueIndices.vals()) {
        switch (BTree.get(blocks, Nat.compare, index)) {
          case (?block) {
            Vector.add(candidateBlocks, block);
          };
          case null { /* Block not found */ };
        };
      };
    };

    let resultBlocks = Vector.toArray(candidateBlocks);
    
    #ok({
      blocks = resultBlocks;
      totalCount = resultBlocks.size();
      hasMore = false; // Simple implementation
      nextIndex = null;
    });
  };

  // Get price history for specific tokens
  public query ({ caller }) func getPriceHistory(tokens : [Principal], startTime : ?Int, endTime : ?Int, limit : ?Nat) : async Result.Result<[(Principal, [Block])], ArchiveError> {
    if (not isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    let results = Vector.new<(Principal, [Block])>();
    let queryLimit = Option.get(limit, 100);

    for (token in tokens.vals()) {
      let tokenBlocks = Vector.new<Block>();
      
      switch (Map.get(tokenIndex, phash, token)) {
        case (?indices) {
          var count = 0;
          label inner_loop for (index in indices.vals()) {
            if (count >= queryLimit) {
              break inner_loop;
            };
            
            switch (BTree.get(blocks, Nat.compare, index)) {
              case (?block) {
                Vector.add(tokenBlocks, block);
                count += 1;
              };
              case null { /* Block not found */ };
            };
          };
        };
        case null { /* No blocks for this token */ };
      };
      
      Vector.add(results, (token, Vector.toArray(tokenBlocks)));
    };

    #ok(Vector.toArray(results));
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
      lastArchiveTime = lastPriceImportTime;
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
  // BATCH IMPORT SYSTEM FOR PRICE DATA
  //=========================================================================
  
  // Tracking state for batch imports
  private stable var lastImportedTokenSync : Int = 0;

  // Initialize batch import timer with default configuration
  private let batchTimer = BatchImportTimer.BatchImportTimer(logger, BatchImportTimer.DEFAULT_CONFIG, isMasterAdmin);

  // ICRC-3 functionality using abstraction
  private let icrc3 = ArchiveICRC3.ArchiveICRC3(
    func() : BTree.BTree<Nat, ArchiveTypes.Block> { blocks },
    func() : Nat { nextBlockIndex },
    this_canister_id,
    ["3price"]
  );

  // Treasury interface for batch imports
  private let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(canister_ids.getCanisterId(#treasury)));

  // Track last known prices to detect changes
  private stable var lastKnownPrices = Map.new<Principal, {icpPrice: Nat; usdPrice: Float; timestamp: Int}>();

  /**
   * Import price changes from treasury token details
   */
  private func importPriceChangesBatch() : async { imported: Nat; failed: Nat } {
    try {
      // Get current token details from treasury
      let tokenDetails = await treasuryCanister.getTokenDetails();
      var imported = 0;
      var failed = 0;
      
      for ((tokenPrincipal, details) in tokenDetails.vals()) {
            if (details.Active and details.lastTimeSynced > lastImportedTokenSync) {
              // Check if this is a price change
              let currentPrice = {
                icpPrice = details.priceInICP;
                usdPrice = details.priceInUSD;
                timestamp = details.lastTimeSynced;
              };
              
              switch (Map.get(lastKnownPrices, phash, tokenPrincipal)) {
                case (?lastPrice) {
                  // Check if price changed significantly (more than 0.1%)
                  if (lastPrice.icpPrice != currentPrice.icpPrice or 
                      Float.abs(lastPrice.usdPrice - currentPrice.usdPrice) > 0.001) {
                    
                    let changePercent = calculatePriceChange(lastPrice.icpPrice, currentPrice.icpPrice);
                    
                    let priceBlock : PriceBlockData = {
                      token = tokenPrincipal;
                      priceICP = currentPrice.icpPrice;
                      priceUSD = currentPrice.usdPrice;
                      source = #NTN;
                      volume24h = null;
                      change24h = ?changePercent;
                    };
                    
                    let archiveResult = await archivePriceBlock(priceBlock);
                    
                    switch (archiveResult) {
                      case (#ok(index)) {
                        imported += 1;
                        Map.set(lastKnownPrices, phash, tokenPrincipal, currentPrice);
                        lastImportedTokenSync := Int.max(lastImportedTokenSync, details.lastTimeSynced);
                      };
                      case (#err(error)) {
                        failed += 1;
                        logger.error(
                          "BATCH_IMPORT",
                          "Failed to import price change for " # Principal.toText(tokenPrincipal) # ": " # debug_show(error),
                          "importPriceChangesBatch"
                        );
                      };
                    };
                  } else {
                    // Update timestamp even if price didn't change
                    Map.set(lastKnownPrices, phash, tokenPrincipal, currentPrice);
                  };
                };
                case null {
                  // First time seeing this token - record initial price
                  Map.set(lastKnownPrices, phash, tokenPrincipal, currentPrice);
                  
                  let priceBlock : PriceBlockData = {
                    token = tokenPrincipal;
                    priceICP = currentPrice.icpPrice;
                    priceUSD = currentPrice.usdPrice;
                    source = #NTN;
                    volume24h = null;
                    change24h = null;
                  };
                  
                  let archiveResult = await archivePriceBlock(priceBlock);
                  
                  switch (archiveResult) {
                    case (#ok(index)) {
                      imported += 1;
                      lastImportedTokenSync := Int.max(lastImportedTokenSync, details.lastTimeSynced);
                    };
                    case (#err(error)) {
                      failed += 1;
                      logger.error(
                        "BATCH_IMPORT",
                        "Failed to import initial price for " # Principal.toText(tokenPrincipal) # ": " # debug_show(error),
                        "importPriceChangesBatch"
                      );
                    };
                  };
                };
              };
            };
          };
          
          if (imported > 0) {
            logger.info(
              "BATCH_IMPORT",
              "Imported " # Nat.toText(imported) # " price changes, failed " # Nat.toText(failed),
              "importPriceChangesBatch"
            );
          };
          
          { imported = imported; failed = failed };
    } catch (e) {
      logger.error(
        "BATCH_IMPORT",
        "Exception during price import: " # Error.message(e),
        "importPriceChangesBatch"
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
      "Starting price batch import cycle",
      "runBatchImport"
    );
    
    let result = await importPriceChangesBatch();
    
    logger.info(
      "BATCH_IMPORT",
      "Price batch import completed - Imported: " # Nat.toText(result.imported) # 
      " Failed: " # Nat.toText(result.failed),
      "runBatchImport"
    );
    
    lastArchiveTime := lastImportedTokenSync;
  };

  // Timer functionality now handled by BatchImportTimer class

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
   * Run catch-up import (admin function)
   */
  public shared ({ caller }) func catchUpImport() : async Result.Result<Text, ArchiveError> {
    let result = await batchTimer.runCatchUpImport(caller, importPriceChangesBatch);
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
    lastImportedTokenSync: Int;
    intervalSeconds: Nat;
    totalPriceUpdates: Nat;
    trackedTokens: Nat;
  } {
    {
      isRunning = batchTimer.isRunning();
      lastImportedTokenSync = lastImportedTokenSync;
      intervalSeconds = batchTimer.getIntervalSeconds();
      totalPriceUpdates = totalPriceUpdates;
      trackedTokens = Map.size(lastKnownPrices);
    };
  };

  //=========================================================================
  // SYSTEM INITIALIZATION
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

  logger.info("Archive", "ICRC-3 Price Archive canister initialized with " # Nat.toText(supportedBlockTypes.size()) # " supported block types", "init");
}
