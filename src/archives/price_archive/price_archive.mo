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
import Timer "mo:base/Timer";

import ICRC3 "mo:icrc3-mo";                    // ← THE FIX: Use the actual library, not just types
import ICRC3Service "mo:icrc3-mo/service";        // ← Keep service types for API
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";
import Cycles "mo:base/ExperimentalCycles";

shared (deployer) persistent actor class PriceArchiveV2() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type PriceBlockData = ArchiveTypes.PriceBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type TokenDetails = DAO_types.TokenDetails;

  // ICRC3 State Management for Scalable Storage (500GB)
  private stable var icrc3State : ?ICRC3.State = null;
  private transient var icrc3StateRef = { var value = icrc3State };

  // Initialize the generic base class with price-specific configuration
  private transient let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 1000000; // 1M blocks per canister
    blockRetentionPeriodNS = 31536000000000000; // 1 year in nanoseconds
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  private transient let base = ArchiveBase.ArchiveBase<PriceBlockData>(
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
  private stable var batchImportShouldRun : Bool = false;

  // Treasury interface for batch imports
  transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private transient let TREASURY_ID = canister_ids.getCanisterId(#treasury);
  private transient let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(TREASURY_ID));

  // Helper function to get price range bucket for indexing
  private func getPriceRangeBucket(priceICP : Nat) : Nat {
    // Group prices into buckets (e.g., every 1000 ICP units)
    priceICP / 1000;
  };

  // Internal synchronous price archival — no auth check, no logging, no await.
  // Used by public methods and batch import to avoid inter-canister self-call overhead.
  private func archivePriceBlockInternal<system>(price : PriceBlockData) : Nat {
    let timestamp = price.timestamp;
    let blockValue = ArchiveTypes.priceToValue(price, timestamp, null);

    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3price",
      [price.token],
      timestamp
    );

    base.addToIndex(priceRangeIndex, getPriceRangeBucket(price.priceICP), blockIndex, Map.nhash);

    totalPriceUpdates += 1;
    Map.set(lastKnownPrices, Map.phash, price.token, {
      icpPrice = price.priceICP;
      usdPrice = price.priceUSD;
      timestamp = timestamp;
    });

    blockIndex;
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

    let blockIndex = archivePriceBlockInternal<system>(price);

    base.logger.info("Archive", "Archived price block at index: " # Nat.toText(blockIndex) #
      " Token: " # Principal.toText(price.token) #
      " Price: " # Nat.toText(price.priceICP) # " ICP", "archivePriceBlock");

    #ok(blockIndex);
  };

  // Bulk archive function - archives multiple price records at once (max 1000)
  public shared ({ caller }) func archivePriceBlockBatch<system>(
    prices: [PriceBlockData]
  ) : async Result.Result<{ archived: Nat; failed: Nat }, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    if (prices.size() > 1000) {
      return #err(#InvalidData); // Limit batch size
    };

    for (price in prices.vals()) {
      ignore archivePriceBlockInternal<system>(price);
    };

    base.logger.info("Archive", "Batch archived " # Nat.toText(prices.size()) # " price blocks", "archivePriceBlockBatch");

    #ok({ archived = prices.size(); failed = 0 });
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

  //=========================================================================
  // Binary Search Helpers for Efficient Historical Queries
  //=========================================================================

  // Extract timestamp from a block result
  // Looks for tx.timestamp first, then tx.data.ts as fallback
  private func extractBlockTimestamp(blocks: [{id: Nat; block: ICRC3Service.Value}]) : Int {
    if (blocks.size() == 0) return 0;

    switch (blocks[0].block) {
      case (#Map(entries)) {
        for ((key, value) in entries.vals()) {
          if (key == "tx") {
            switch (value) {
              case (#Map(txEntries)) {
                for ((txKey, txValue) in txEntries.vals()) {
                  switch (txKey, txValue) {
                    case ("timestamp", #Int(t)) { return t };
                    case ("data", #Map(dataEntries)) {
                      for ((dataKey, dataValue) in dataEntries.vals()) {
                        if (dataKey == "ts") {
                          switch (dataValue) {
                            case (#Int(t)) { return t };
                            case _ {};
                          };
                        };
                      };
                    };
                    case _ {};
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
    0
  };

  // Binary search to find block index near target timestamp
  // Returns index of block with timestamp <= targetTimestamp
  // Uses O(log n) iterations - ~15 for 37K blocks, ~20 for 1M blocks
  private func findBlockIndexNearTimestamp(targetTimestamp: Int, totalBlocks: Nat) : Nat {
    if (totalBlocks == 0) return 0;
    if (totalBlocks == 1) return 0;

    var low : Nat = 0;
    var high : Nat = totalBlocks - 1;

    // Binary search: each iteration halves the search space
    while (low + 1 < high) {
      let mid = (low + high) / 2;
      let midResult = base.icrc3_get_blocks([{ start = mid; length = 1 }]);
      let midTimestamp = extractBlockTimestamp(midResult.blocks);

      if (midTimestamp <= targetTimestamp) {
        low := mid;
      } else {
        high := mid;
      };
    };

    low
  };

  //=========================================================================
  // Price Query Functions (using binary search)
  //=========================================================================

  // New method for rewards calculation - get price at specific time
  public query ({ caller }) func getPriceAtTime(token : Principal, timestamp : Int) : async Result.Result<?{icpPrice: Nat; usdPrice: Float; timestamp: Int}, ArchiveError> {
    // Query method is open for everyone. (Data not sensitive and query methods are free)

    // Get total blocks count
    let sampleResult = base.icrc3_get_blocks([{ start = 0; length = 1 }]);
    let totalBlocks = sampleResult.log_length;

    if (totalBlocks == 0) return #ok(null);

    // Binary search to find approximate block position (~15 iterations for 37K blocks)
    let targetBlock = findBlockIndexNearTimestamp(timestamp, totalBlocks);

    // Search a window around the target (larger window for "at or before" to find best match)
    // Increased from 500 to 2000 to handle sparse price data (e.g., ICP prices)
    let WINDOW_SIZE : Nat = 2000;
    let searchStart = if (targetBlock > WINDOW_SIZE) { targetBlock - WINDOW_SIZE } else { 0 };
    let searchLength = Nat.min(WINDOW_SIZE * 2, totalBlocks - searchStart);

    let getBlocksArgs = [{ start = searchStart; length = searchLength }];
    let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);
    
    // Find the most recent price block for this token at or before the timestamp
    var mostRecentPrice : ?{icpPrice: Nat; usdPrice: Float; timestamp: Int} = null;
    var mostRecentTimestamp : Int = -1;

    for (block in icrc3Result.blocks.vals()) {
      switch (block.block) {
        case (#Map(entries)) {
          // Look for the tx.data structure
          for ((key, value) in entries.vals()) {
            switch (key, value) {
              case ("tx", #Map(txEntries)) {
                // Check if this is a price operation
                var isPrice = false;
                var txTimestamp : Int = 0;
                
                for ((txKey, txValue) in txEntries.vals()) {
                  switch (txKey, txValue) {
                    case ("operation", #Text("3price")) {
                      isPrice := true;
                    };
                    case ("timestamp", #Int(t)) {
                      txTimestamp := t;
                    };
                    case ("data", #Map(dataEntries)) {
                      if (isPrice) {
                        // Parse the price data
                        var blockToken : ?Principal = null;
                        var blockTimestamp : Int = txTimestamp;
                        var blockPriceICP : ?Nat = null;
                        var blockPriceUSD : ?Text = null;
                        
                        for ((dataKey, dataValue) in dataEntries.vals()) {
                          switch (dataKey, dataValue) {
                            case ("token", #Blob(b)) { 
                              if (b.size() <= 29 and b.size() > 0) {
                                blockToken := ?Principal.fromBlob(b);
                              };
                            };
                            case ("ts", #Int(t)) { 
                              blockTimestamp := t;
                            };
                            case ("price_icp", #Nat(p)) { 
                              blockPriceICP := ?p;
                            };
                            case ("price_usd", #Text(p)) { 
                              blockPriceUSD := ?p;
                            };
                            case _ {};
                          };
                        };
                        
                        // Check if this matches our criteria
                        switch (blockToken, blockPriceICP, blockPriceUSD) {
                          case (?bToken, ?icp, ?usdText) {
                            if (Principal.equal(bToken, token) and 
                                blockTimestamp <= timestamp and 
                                blockTimestamp > mostRecentTimestamp) {
                              // Convert USD price from Text to Float
                              // Note: Motoko doesn't have Float.fromText, so we'll use a simple approach
                              // assuming the text is a valid float representation
                              switch (textToFloat(usdText)) {
                                case (?usd) {
                                  mostRecentTimestamp := blockTimestamp;
                                  mostRecentPrice := ?{
                                    icpPrice = icp;
                                    usdPrice = usd;
                                    timestamp = blockTimestamp;
                                  };
                                };
                                case null {};
                              };
                            };
                          };
                          case _ {};
                        };
                      };
                    };
                    case _ {};
                  };
                };
              };
              case _ {};
            };
          };
        };
        case _ {};
      };
    };

    #ok(mostRecentPrice);
  };

  // Get the closest (oldest) price in the future of the given timestamp
  public query ({ caller }) func getPriceAtOrAfterTime(token : Principal, timestamp : Int) : async Result.Result<?{icpPrice: Nat; usdPrice: Float; timestamp: Int}, ArchiveError> {
    // Query method is open for everyone.

    // Get total blocks count
    let sampleResult = base.icrc3_get_blocks([{ start = 0; length = 1 }]);
    let totalBlocks = sampleResult.log_length;

    if (totalBlocks == 0) return #ok(null);

    // Binary search to find approximate block position
    let targetBlock = findBlockIndexNearTimestamp(timestamp, totalBlocks);

    // For "at or after", search more forward from target (bias search window forward)
    // Increased from 500 to 2000 to handle sparse price data (e.g., ICP prices)
    let WINDOW_SIZE : Nat = 2000;
    let searchStart = if (targetBlock > WINDOW_SIZE / 4) { targetBlock - WINDOW_SIZE / 4 } else { 0 };
    let searchLength = Nat.min(WINDOW_SIZE, totalBlocks - searchStart);

    let getBlocksArgs = [{ start = searchStart; length = searchLength }];
    let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);

    // Find the nearest price block for this token at or after the timestamp
    var bestFuturePrice : ?{icpPrice: Nat; usdPrice: Float; timestamp: Int} = null;
    var bestFutureTimestamp : Int = 9_223_372_036_854_775_807; // effectively +infinity for Int64 range

    for (block in icrc3Result.blocks.vals()) {
      switch (block.block) {
        case (#Map(entries)) {
          for ((key, value) in entries.vals()) {
            switch (key, value) {
              case ("tx", #Map(txEntries)) {
                var isPrice = false;
                var txTimestamp : Int = 0;

                for ((txKey, txValue) in txEntries.vals()) {
                  switch (txKey, txValue) {
                    case ("operation", #Text("3price")) { isPrice := true; };
                    case ("timestamp", #Int(t)) { txTimestamp := t; };
                    case ("data", #Map(dataEntries)) {
                      if (isPrice) {
                        var blockToken : ?Principal = null;
                        var blockTimestamp : Int = txTimestamp;
                        var blockPriceICP : ?Nat = null;
                        var blockPriceUSD : ?Text = null;

                        for ((dataKey, dataValue) in dataEntries.vals()) {
                          switch (dataKey, dataValue) {
                            case ("token", #Blob(b)) { if (b.size() <= 29 and b.size() > 0) { blockToken := ?Principal.fromBlob(b); }; };
                            case ("ts", #Int(t)) { blockTimestamp := t; };
                            case ("price_icp", #Nat(p)) { blockPriceICP := ?p; };
                            case ("price_usd", #Text(p)) { blockPriceUSD := ?p; };
                            case _ {};
                          };
                        };

                        switch (blockToken, blockPriceICP, blockPriceUSD) {
                          case (?bToken, ?icp, ?usdText) {
                            if (Principal.equal(bToken, token) and blockTimestamp >= timestamp and blockTimestamp < bestFutureTimestamp) {
                              switch (textToFloat(usdText)) {
                                case (?usd) {
                                  bestFutureTimestamp := blockTimestamp;
                                  bestFuturePrice := ?{ icpPrice = icp; usdPrice = usd; timestamp = blockTimestamp };
                                };
                                case null {};
                              };
                            };
                          };
                          case _ {};
                        };
                      };
                    };
                    case _ {};
                  };
                };
              };
              case _ {};
            };
          };
        };
        case _ {};
      };
    };

    #ok(bestFuturePrice);
  };

  // Helper function to convert Value back to PriceBlockData
  private func convertValueToPriceData(value : ArchiveTypes.Value) : ?ArchiveTypes.PriceBlockData {
    switch (value) {
      case (#Map(fields)) {
        // Extract fields from the Value map
        var token : ?Principal = null;
        var priceICP : ?Nat = null;
        var priceUSD : ?Float = null;
        var timestamp : ?Int = null;
        var source : ?ArchiveTypes.PriceSource = null;
        var volume24h : ?Nat = null;
        var change24h : ?Float = null;

        for ((key, val) in fields.vals()) {
          switch (key, val) {
            case ("token", #Blob(b)) { 
              // Principal.fromBlob can trap, so we need to handle it carefully
              if (b.size() == 29) { // Valid principal blob size
                token := ?Principal.fromBlob(b);
              };
            };
            case ("priceICP", #Nat(p)) { priceICP := ?p };
            case ("priceUSD", #Float(p)) { priceUSD := ?p };
            case ("timestamp", #Int(t)) { timestamp := ?t };
            // TODO: Parse source, volume24h, change24h if needed
            case _ {};
          };
        };

        // Return the parsed data if we have the required fields
        switch (token, priceICP, priceUSD, timestamp) {
          case (?t, ?icp, ?usd, ?ts) {
            ?{
              token = t;
              priceICP = icp;
              priceUSD = usd;
              timestamp = ts;
              source = switch (source) { case (?s) s; case null #Aggregated };
              volume24h = volume24h; // Already optional
              change24h = change24h; // Already optional
            };
          };
          case _ { null };
        };
      };
      case _ { null };
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
        
        // Sort price points chronologically (oldest first) for proper block ordering
        let sortedPricePoints = Array.sort<TreasuryTypes.PricePoint>(newPricePoints, func(a, b) {
          Int.compare(a.time, b.time)
        });
        
        // Build batch of PriceBlockData for this token
        let batchData = Array.map<TreasuryTypes.PricePoint, PriceBlockData>(sortedPricePoints, func(pricePoint) : PriceBlockData {
          {
            token = token;
            priceICP = pricePoint.icpPrice;
            priceUSD = pricePoint.usdPrice;
            source = #NTN;
            volume24h = null;
            change24h = null;
            timestamp = pricePoint.time;
          }
        });

        // Archive in chunks of 1000 (archivePriceBlockBatch limit)
        let BATCH_SIZE = 1000;
        var offset = 0;
        while (offset < batchData.size()) {
          let chunkSize = Nat.min(BATCH_SIZE, batchData.size() - offset);
          let chunk = Array.subArray<PriceBlockData>(batchData, offset, chunkSize);

          let result = await archivePriceBlockBatch(chunk);
          switch (result) {
            case (#ok({ archived })) {
              imported += archived;
              // Update lastImportedPriceTime to the latest timestamp in this chunk
              if (chunk.size() > 0) {
                let lastInChunk = chunk[chunk.size() - 1];
                lastImportedPriceTime := Int.max(lastImportedPriceTime, lastInChunk.timestamp);
              };
            };
            case (#err(e)) {
              failed += chunkSize;
              base.logger.error("Batch Import", "Failed to batch import " # Nat.toText(chunkSize) # " prices for " # Principal.toText(token) # ": " # debug_show(e), "importPriceHistoryBatch");
            };
          };
          offset += BATCH_SIZE;
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
    let result = await base.startAdvancedBatchImportSystem<system>(caller, null, null, ?importPriceOnly);
    switch (result) { case (#ok(_)) { batchImportShouldRun := true }; case _ {} };
    result;
  };

  // Legacy compatibility - combined import
  public shared ({ caller }) func startLegacyBatchImportSystem() : async Result.Result<Text, Text> {
    let result = await base.startBatchImportSystem<system>(caller, runPriceBatchImport);
    switch (result) { case (#ok(_)) { batchImportShouldRun := true }; case _ {} };
    result;
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    let result = base.stopBatchImportSystem(caller);
    switch (result) { case (#ok(_)) { batchImportShouldRun := false }; case _ {} };
    result;
  };
  
  // Emergency stop all timers
  public shared ({ caller }) func stopAllTimers() : async Result.Result<Text, Text> {
    let result = base.stopAllTimers(caller);
    switch (result) { case (#ok(_)) { batchImportShouldRun := false }; case _ {} };
    result;
  };

  // Emergency force reset for stuck middle loop
  public shared ({ caller }) func forceResetMiddleLoop() : async Result.Result<Text, Text> {
    base.forceResetMiddleLoop(caller);
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
  // Batch Price Query Method
  //=========================================================================

  public query ({ caller }) func getPricesAtTime(tokens : [Principal], timestamp : Int) : async Result.Result<[(Principal, ?{icpPrice: Nat; usdPrice: Float; timestamp: Int})], ArchiveError> {
    // Get total blocks count
    let sampleResult = base.icrc3_get_blocks([{ start = 0; length = 1 }]);
    let totalBlocks = sampleResult.log_length;

    if (totalBlocks == 0) {
      // Return null prices for all tokens if no blocks exist
      let emptyResults = Array.map<Principal, (Principal, ?{icpPrice: Nat; usdPrice: Float; timestamp: Int})>(tokens, func(token) {
        (token, null)
      });
      return #ok(emptyResults);
    };

    // Binary search to find approximate block position (~15 iterations for 37K blocks)
    let targetBlock = findBlockIndexNearTimestamp(timestamp, totalBlocks);

    // Search a window around the target (larger window for batch queries to find prices for multiple tokens)
    // Increased from 500 to 2000 to handle sparse price data (e.g., ICP prices)
    let WINDOW_SIZE : Nat = 2000;
    let searchStart = if (targetBlock > WINDOW_SIZE) { targetBlock - WINDOW_SIZE } else { 0 };
    let searchLength = Nat.min(WINDOW_SIZE * 2, totalBlocks - searchStart);

    let getBlocksArgs = [{ start = searchStart; length = searchLength }];
    let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);
    
    // Map to store most recent price for each token
    let tokenPrices = Map.new<Principal, {icpPrice: Nat; usdPrice: Float; timestamp: Int}>();
    let tokenTimestamps = Map.new<Principal, Int>();

    for (block in icrc3Result.blocks.vals()) {
      switch (block.block) {
        case (#Map(entries)) {
          for ((key, value) in entries.vals()) {
            switch (key, value) {
              case ("tx", #Map(txEntries)) {
                var isPrice = false;
                var txTimestamp : Int = 0;
                
                for ((txKey, txValue) in txEntries.vals()) {
                  switch (txKey, txValue) {
                    case ("operation", #Text("3price")) { isPrice := true; };
                    case ("timestamp", #Int(t)) { txTimestamp := t; };
                    case ("data", #Map(dataEntries)) {
                      if (isPrice) {
                        var blockToken : ?Principal = null;
                        var blockTimestamp : Int = txTimestamp;
                        var blockPriceICP : ?Nat = null;
                        var blockPriceUSD : ?Text = null;
                        
                        for ((dataKey, dataValue) in dataEntries.vals()) {
                          switch (dataKey, dataValue) {
                            case ("token", #Blob(b)) { if (b.size() <= 29 and b.size() > 0) { blockToken := ?Principal.fromBlob(b); }; };
                            case ("ts", #Int(t)) { blockTimestamp := t; };
                            case ("price_icp", #Nat(p)) { blockPriceICP := ?p; };
                            case ("price_usd", #Text(p)) { blockPriceUSD := ?p; };
                            case _ {};
                          };
                        };
                        
                        switch (blockToken, blockPriceICP, blockPriceUSD) {
                          case (?bToken, ?icp, ?usdText) {
                            // Check if this token is in our requested list
                            let isRequestedToken = Array.find<Principal>(tokens, func(t) { Principal.equal(t, bToken) });
                            
                            switch (isRequestedToken) {
                              case (?_) {
                                if (blockTimestamp <= timestamp) {
                                  let currentBestTimestamp = switch (Map.get(tokenTimestamps, Map.phash, bToken)) {
                                    case (?ts) ts;
                                    case null -1;
                                  };
                                  
                                  if (blockTimestamp > currentBestTimestamp) {
                                    switch (textToFloat(usdText)) {
                                      case (?usd) {
                                        Map.set(tokenTimestamps, Map.phash, bToken, blockTimestamp);
                                        Map.set(tokenPrices, Map.phash, bToken, { icpPrice = icp; usdPrice = usd; timestamp = blockTimestamp; });
                                      };
                                      case null {};
                                    };
                                  };
                                };
                              };
                              case null {};
                            };
                          };
                          case _ {};
                        };
                      };
                    };
                    case _ {};
                  };
                };
              };
              case _ {};
            };
          };
        };
        case _ {};
      };
    };
    
    // Build result array in the same order as requested tokens
    let results = Array.map<Principal, (Principal, ?{icpPrice: Nat; usdPrice: Float; timestamp: Int})>(tokens, func(token) {
      let price = Map.get(tokenPrices, Map.phash, token);
      (token, price)
    });
    
    #ok(results);
  };

  //=========================================================================
  // Lifecycle Functions (delegated to base class)
  //=========================================================================

  // Helper function to convert text to float
  // Simple implementation that handles basic decimal numbers
  private func textToFloat(text: Text) : ?Float {
    // For now, we'll use a simple approach
    // In production, you might want a more robust parser
    switch (text) {
      case ("0") { ?0.0 };
      case ("1") { ?1.0 };
      case _ {
        // Try to parse as a basic decimal number
        // This is a simplified implementation
        let chars = text.chars();
        var result : Float = 0.0;
        var decimal : Float = 0.0;
        var afterDecimal = false;
        var decimalPlace : Float = 0.1;
        
        for (char in chars) {
          switch (char) {
            case ('.') {
              afterDecimal := true;
            };
            case ('0') {
              if (afterDecimal) {
                decimal := decimal + (0.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 0.0;
              };
            };
            case ('1') {
              if (afterDecimal) {
                decimal := decimal + (1.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 1.0;
              };
            };
            case ('2') {
              if (afterDecimal) {
                decimal := decimal + (2.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 2.0;
              };
            };
            case ('3') {
              if (afterDecimal) {
                decimal := decimal + (3.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 3.0;
              };
            };
            case ('4') {
              if (afterDecimal) {
                decimal := decimal + (4.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 4.0;
              };
            };
            case ('5') {
              if (afterDecimal) {
                decimal := decimal + (5.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 5.0;
              };
            };
            case ('6') {
              if (afterDecimal) {
                decimal := decimal + (6.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 6.0;
              };
            };
            case ('7') {
              if (afterDecimal) {
                decimal := decimal + (7.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 7.0;
              };
            };
            case ('8') {
              if (afterDecimal) {
                decimal := decimal + (8.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 8.0;
              };
            };
            case ('9') {
              if (afterDecimal) {
                decimal := decimal + (9.0 * decimalPlace);
                decimalPlace := decimalPlace * 0.1;
              } else {
                result := result * 10.0 + 9.0;
              };
            };
            case _ {
              // Invalid character, return null
              return null;
            };
          };
        };
        
        ?(result + decimal);
      };
    };
  };

  system func preupgrade() {
    icrc3State := icrc3StateRef.value;
    base.preupgrade();
  };

  system func postupgrade() {
    icrc3StateRef.value := icrc3State;
    base.postupgrade<system>(func() : async () { /* no-op for base compat */ });

    // Auto-restart batch import timer if it was running before the upgrade
    if (batchImportShouldRun) {
      ignore Timer.setTimer<system>(
        #nanoseconds(15_000_000_000), // 15 second delay after upgrade
        func() : async () {
          ignore await startBatchImportSystem();
        }
      );
    };
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };
} 