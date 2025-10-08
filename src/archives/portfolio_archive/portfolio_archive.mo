import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Map "mo:map/Map";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Nat32 "mo:base/Nat32";
import Int "mo:base/Int";
import Error "mo:base/Error";
import Array "mo:base/Array";
import Float "mo:base/Float";
import Text "mo:base/Text";
import Vector "mo:vector";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Nat8 "mo:base/Nat8";

import ICRC3 "mo:icrc3-mo";                    // ← THE FIX: Use the actual library, not just types
import ICRC3Service "mo:icrc3-mo/service";        // ← Keep service types for API
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import BatchImportTimer "../../helper/batch_import_timer";
import Logger "../../helper/logger";
import Cycles "mo:base/ExperimentalCycles";

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

  //=========================================================================
  // OHLC Aggregation System Types
  //=========================================================================

  // OHLC candle with fixed-precision values
  public type Candle = {
    t_start : Nat64;   // UTC timestamp in nanoseconds
    o : Nat64;         // Open price (ICP e8s)
    h : Nat64;         // High price (ICP e8s)  
    l : Nat64;         // Low price (ICP e8s)
    c : Nat64;         // Close price (ICP e8s)
    n : Nat32;         // Number of snapshots in this candle
  };

  // Accumulator for building candles across chunk boundaries
  public type Acc = {
    var seen: Bool;     // Whether this accumulator has any data
    var t_start: Nat64; // Start time of current bucket
    var o: Nat64;       // Open price
    var h: Nat64;       // High price
    var l: Nat64;       // Low price
    var c: Nat64;       // Close price
    var n: Nat32;       // Count of data points
  };

  // Time resolution for OHLC buckets
  public type Resolution = {
    #hour;
    #day;
    #week;
    #month;
    #year;
  };

  // Time constants in nanoseconds (UTC-aligned)
  private let HOUR_NS : Nat64 = 3_600_000_000_000;         // 1 hour
  private let DAY_NS : Nat64 = 86_400_000_000_000;         // 24 hours
  private let WEEK_NS : Nat64 = 604_800_000_000_000;       // 7 days (Mon 00:00 UTC)
  private let MONTH_NS : Nat64 = 2_592_000_000_000_000;    // 30 days (approx)
  private let YEAR_NS : Nat64 = 31_536_000_000_000_000;    // 365 days

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

  //=========================================================================
  // OHLC Aggregation System Storage
  //=========================================================================

  // OHLC processing state
  private stable var indexed_until_idx : Nat64 = 0;  // Highest snapshot index processed

  // Carry state for partial buckets across chunk boundaries
  private stable var carry_hour : ?Acc = null;
  private stable var carry_day : ?Acc = null;
  private stable var carry_week : ?Acc = null;
  private stable var carry_month : ?Acc = null;
  private stable var carry_year : ?Acc = null;

  // Last flushed timestamps per resolution (for idempotence)
  private stable var last_flushed_hour : ?Nat64 = null;
  private stable var last_flushed_day : ?Nat64 = null;
  private stable var last_flushed_week : ?Nat64 = null;
  private stable var last_flushed_month : ?Nat64 = null;
  private stable var last_flushed_year : ?Nat64 = null;

  // Per-resolution shards by calendar month: Store[resolution][yyyy_mm] : Vector<Candle>
  private stable var hour_shards = Map.new<Text, Vector.Vector<Candle>>();
  private stable var day_shards = Map.new<Text, Vector.Vector<Candle>>();
  private stable var week_shards = Map.new<Text, Vector.Vector<Candle>>();
  private stable var month_shards = Map.new<Text, Vector.Vector<Candle>>();
  private stable var year_shards = Map.new<Text, Vector.Vector<Candle>>();

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

    // Live OHLC ingestion - update aggregates with new snapshot
    updateOHLCLive(portfolio);

    base.logger.info("Archive", "Archived portfolio block at index: " # Nat.toText(blockIndex) # 
      " Value: " # Nat.toText(portfolio.totalValueICP) # " ICP", "archivePortfolioBlock");

    #ok(blockIndex);
  };

  // Live OHLC update for each new snapshot
  private func updateOHLCLive(portfolio: PortfolioBlockData) {
    let ts = Nat64.fromNat(Int.abs(portfolio.timestamp));
    let price = extractPortfolioValue(portfolio);
    
    // Hour level processing
    let hs = bucketStart(ts, HOUR_NS);
    let hourAcc = getCarryAcc(#hour);
    
    if (hourAcc.seen and hs != hourAcc.t_start) {
      // Flush completed hour bucket and cascade
      switch (flush(hourAcc)) {
        case (?hourCandle) {
          appendCandle(#hour, shardKey(#hour, hourCandle.t_start), hourCandle);
          feedDay(hourCandle);
        };
        case null {};
      };
    };
    
    // Update hour accumulator
    step(hourAcc, hs, price);
    
    // Update indexed_until_idx to reflect live processing
    indexed_until_idx := Nat64.max(indexed_until_idx, Nat64.fromNat(base.getTotalBlocks()));
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
    lower_bound_ts_impl(ts);
  };

  private func lower_bound_ts_impl(ts : Int) : Result.Result<Nat, ArchiveError> {
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

  //=========================================================================
  // OHLC Helper Functions
  //=========================================================================

  // UTC-aligned bucket start time
  private func bucketStart(ts: Nat64, delta: Nat64) : Nat64 {
    (ts / delta) * delta
  };

  // Get time delta for resolution
  private func getResolutionDelta(res: Resolution) : Nat64 {
    switch (res) {
      case (#hour) { HOUR_NS };
      case (#day) { DAY_NS };
      case (#week) { WEEK_NS };
      case (#month) { MONTH_NS };
      case (#year) { YEAR_NS };
    }
  };

  // Create new accumulator
  private func newAcc() : Acc {
    {
      var seen = false;
      var t_start = 0;
      var o = 0;
      var h = 0;
      var l = 0;
      var c = 0;
      var n = 0;
    }
  };

  // Step accumulator with new data point
  private func step(acc: Acc, t_start: Nat64, price: Nat64) {
    if (not acc.seen) {
      // First data point in this bucket
      acc.seen := true;
      acc.t_start := t_start;
      acc.o := price;
      acc.h := price;
      acc.l := price;
      acc.c := price;
      acc.n := 1;
    } else {
      // Update existing bucket
      acc.h := Nat64.max(acc.h, price);
      acc.l := Nat64.min(acc.l, price);
      acc.c := price;
      acc.n += 1;
    };
  };

  // Flush accumulator to candle and reset
  private func flush(acc: Acc) : ?Candle {
    if (not acc.seen) {
      return null;
    };
    
    let candle : Candle = {
      t_start = acc.t_start;
      o = acc.o;
      h = acc.h;
      l = acc.l;
      c = acc.c;
      n = acc.n;
    };
    
    // Reset accumulator
    acc.seen := false;
    acc.t_start := 0;
    acc.o := 0;
    acc.h := 0;
    acc.l := 0;
    acc.c := 0;
    acc.n := 0;
    
    ?candle
  };

  // Generate shard key (YYYY-MM format) from timestamp
  private func shardKey(res: Resolution, t_start: Nat64) : Text {
    // Convert nanoseconds to seconds for easier date calculation
    let seconds = t_start / 1_000_000_000;
    
    // Simple approximation: assume Unix epoch and calculate year/month
    // This is a simplified version - in production you'd want proper date/time library
    let year = 1970 + (seconds / (365 * 24 * 60 * 60));
    let dayOfYear = (seconds / (24 * 60 * 60)) % 365;
    let month = 1 + (dayOfYear / 30); // Rough approximation
    
    let yearText = Nat64.toText(year);
    let monthText = if (month < 10) { "0" # Nat64.toText(month) } else { Nat64.toText(month) };
    
    yearText # "-" # monthText
  };

  // Get shard map for resolution
  private func getShardMap(res: Resolution) : Map.Map<Text, Vector.Vector<Candle>> {
    switch (res) {
      case (#hour) { hour_shards };
      case (#day) { day_shards };
      case (#week) { week_shards };
      case (#month) { month_shards };
      case (#year) { year_shards };
    }
  };

  // Append candle to shard (ensuring monotone t_start, overwrite if duplicate)
  private func appendCandle(res: Resolution, shardKey: Text, candle: Candle) {
    let shardMap = getShardMap(res);
    
    let shard = switch (Map.get(shardMap, Map.thash, shardKey)) {
      case (?existing) { existing };
      case null {
        let newShard = Vector.new<Candle>();
        Map.set(shardMap, Map.thash, shardKey, newShard);
        newShard
      };
    };
    
    // Check if we need to overwrite the last candle (same t_start)
    let size = Vector.size(shard);
    if (size > 0) {
      let lastCandle = Vector.get(shard, size - 1);
      if (lastCandle.t_start == candle.t_start) {
        // Overwrite duplicate
        Vector.put(shard, size - 1, candle);
        return;
      };
    };
    
    // Append new candle (assumes monotone t_start)
    Vector.add(shard, candle);
  };

  // Get or create carry accumulator for resolution
  private func getCarryAcc(res: Resolution) : Acc {
    switch (res) {
      case (#hour) {
        switch (carry_hour) {
          case (?acc) { acc };
          case null {
            let acc = newAcc();
            carry_hour := ?acc;
            acc
          };
        }
      };
      case (#day) {
        switch (carry_day) {
          case (?acc) { acc };
          case null {
            let acc = newAcc();
            carry_day := ?acc;
            acc
          };
        }
      };
      case (#week) {
        switch (carry_week) {
          case (?acc) { acc };
          case null {
            let acc = newAcc();
            carry_week := ?acc;
            acc
          };
        }
      };
      case (#month) {
        switch (carry_month) {
          case (?acc) { acc };
          case null {
            let acc = newAcc();
            carry_month := ?acc;
            acc
          };
        }
      };
      case (#year) {
        switch (carry_year) {
          case (?acc) { acc };
          case null {
            let acc = newAcc();
            carry_year := ?acc;
            acc
          };
        }
      };
    }
  };

  // Update accumulator with OHLC rules from completed candle
  private func stepWithCandle(acc: Acc, t_start: Nat64, candle: Candle) {
    if (not acc.seen) {
      // First candle in this bucket
      acc.seen := true;
      acc.t_start := t_start;
      acc.o := candle.o;
      acc.h := candle.h;
      acc.l := candle.l;
      acc.c := candle.c;
      acc.n := candle.n;
    } else {
      // Update existing bucket with OHLC rules
      acc.h := Nat64.max(acc.h, candle.h);
      acc.l := Nat64.min(acc.l, candle.l);
      acc.c := candle.c;  // Close is always the latest
      acc.n += candle.n;
    };
  };

  // Cascade completed candle to coarser resolutions
  private func feedDay(candle: Candle) {
    let ds = bucketStart(candle.t_start, DAY_NS);
    let dayAcc = getCarryAcc(#day);
    
    if (dayAcc.seen and ds != dayAcc.t_start) {
      // Flush completed day bucket
      switch (flush(dayAcc)) {
        case (?dayCandle) {
          appendCandle(#day, shardKey(#day, dayCandle.t_start), dayCandle);
          feedWeek(dayCandle);
        };
        case null {};
      };
    };
    
    stepWithCandle(dayAcc, ds, candle);
  };

  private func feedWeek(candle: Candle) {
    let ws = bucketStart(candle.t_start, WEEK_NS);
    let weekAcc = getCarryAcc(#week);
    
    if (weekAcc.seen and ws != weekAcc.t_start) {
      // Flush completed week bucket
      switch (flush(weekAcc)) {
        case (?weekCandle) {
          appendCandle(#week, shardKey(#week, weekCandle.t_start), weekCandle);
          feedMonth(weekCandle);
        };
        case null {};
      };
    };
    
    stepWithCandle(weekAcc, ws, candle);
  };

  private func feedMonth(candle: Candle) {
    let ms = bucketStart(candle.t_start, MONTH_NS);
    let monthAcc = getCarryAcc(#month);
    
    if (monthAcc.seen and ms != monthAcc.t_start) {
      // Flush completed month bucket
      switch (flush(monthAcc)) {
        case (?monthCandle) {
          appendCandle(#month, shardKey(#month, monthCandle.t_start), monthCandle);
          feedYear(monthCandle);
        };
        case null {};
      };
    };
    
    stepWithCandle(monthAcc, ms, candle);
  };

  private func feedYear(candle: Candle) {
    let ys = bucketStart(candle.t_start, YEAR_NS);
    let yearAcc = getCarryAcc(#year);
    
    if (yearAcc.seen and ys != yearAcc.t_start) {
      // Flush completed year bucket
      switch (flush(yearAcc)) {
        case (?yearCandle) {
          appendCandle(#year, shardKey(#year, yearCandle.t_start), yearCandle);
        };
        case null {};
      };
    };
    
    stepWithCandle(yearAcc, ys, candle);
  };

  //=========================================================================
  // OHLC Backfill System
  //=========================================================================

  // Helper function to get last snapshot index from archive
  private func getLastSnapshotIndex() : Nat64 {
    let totalBlocks = base.getTotalBlocks();
    if (totalBlocks == 0) { 0 } else { Nat64.fromNat(totalBlocks - 1) }
  };

  // Extract portfolio value from snapshot
  private func extractPortfolioValue(snapshot: PortfolioBlockData) : Nat64 {
    // Convert ICP e8s to Nat64 for OHLC processing
    Nat64.fromNat(snapshot.totalValueICP)
  };

  // Backfill OHLC data from archived snapshots (chunked processing)
  public shared ({ caller }) func backfillOHLC(maxSnapshots: Nat32) : async Result.Result<{processed: Nat32; newCandlesCreated: Nat32}, ArchiveError> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err(#NotAuthorized);
    };

    let hi = getLastSnapshotIndex();
    let startIdx = indexed_until_idx;
    let endIdx = Nat64.min(startIdx + Nat64.fromNat32(maxSnapshots) - 1, hi);
    
    if (startIdx > hi) {
      // Already fully indexed
      return #ok({processed = 0; newCandlesCreated = 0});
    };

    var processed: Nat32 = 0;
    var newCandlesCreated: Nat32 = 0;
    let hourAcc = getCarryAcc(#hour);
    
    // Process snapshots in chronological order
    var i = startIdx;
    while (i <= endIdx) {
      // Get block at index i
      let blockResult = base.icrc3_get_blocks([{start = Nat64.toNat(i); length = 1}]);
      
      if (blockResult.blocks.size() > 0) {
        let block = blockResult.blocks[0];
        
        // Extract portfolio data from block
        switch (extractPortfolioDataFromBlock(block.block)) {
          case (?portfolioData) {
            let ts = Nat64.fromNat(Int.abs(portfolioData.timestamp));
            let price = extractPortfolioValue(portfolioData);
            
            // Hour level processing
            let hs = bucketStart(ts, HOUR_NS);
            
            if (hourAcc.seen and hs != hourAcc.t_start) {
              // Flush completed hour bucket and cascade
              switch (flush(hourAcc)) {
                case (?hourCandle) {
                  appendCandle(#hour, shardKey(#hour, hourCandle.t_start), hourCandle);
                  feedDay(hourCandle);
                  newCandlesCreated += 1;
                };
                case null {};
              };
            };
            
            // Update hour accumulator
            step(hourAcc, hs, price);
            processed += 1;
          };
          case null {
            // Skip blocks we can't parse
            processed += 1;
          };
        };
      };
      
      i += 1;
    };
    
    // Update indexed state (don't flush open carries - they continue next run)
    indexed_until_idx := startIdx + Nat64.fromNat32(processed);
    
    base.logger.info(
      "OHLC_BACKFILL", 
      "Processed " # Nat32.toText(processed) # " snapshots, created " # Nat32.toText(newCandlesCreated) # " candles. " #
      "Indexed up to: " # Nat64.toText(indexed_until_idx),
      "backfillOHLC"
    );
    
    #ok({processed = processed; newCandlesCreated = newCandlesCreated})
  };

  // Get backfill status
  public query func getOHLCBackfillStatus() : async {
    indexedUntilIdx: Nat64;
    lastSnapshotIdx: Nat64;
    totalSnapshots: Nat64;
    progressPercent: Float;
    hasCarryState: Bool;
  } {
    let lastIdx = getLastSnapshotIndex();
    let totalSnapshots = lastIdx + 1;
    let progress = if (totalSnapshots > 0) {
      Float.fromInt(Nat64.toNat(indexed_until_idx)) / Float.fromInt(Nat64.toNat(totalSnapshots)) * 100.0
    } else { 100.0 };
    
    let hasCarry = switch (carry_hour, carry_day, carry_week, carry_month, carry_year) {
      case (null, null, null, null, null) { false };
      case _ { true };
    };
    
    {
      indexedUntilIdx = indexed_until_idx;
      lastSnapshotIdx = lastIdx;
      totalSnapshots = totalSnapshots;
      progressPercent = progress;
      hasCarryState = hasCarry;
    }
  };

  //=========================================================================
  // OHLC Query API
  //=========================================================================

  // Coarsen interval if needed to stay under max_points limit
  private func chooseOptimalInterval(fromNS: Nat64, toNS: Nat64, requestedInterval: Resolution, maxPoints: Nat32) : Resolution {
    let timeSpan = toNS - fromNS;
    let requestedDelta = getResolutionDelta(requestedInterval);
    let estimatedPoints = timeSpan / requestedDelta;
    
    if (estimatedPoints <= Nat64.fromNat32(maxPoints)) {
      return requestedInterval;
    };
    
    // Try coarser intervals
    let intervals = [#hour, #day, #week, #month, #year];
    for (interval in intervals.vals()) {
      let delta = getResolutionDelta(interval);
      let points = timeSpan / delta;
      if (points <= Nat64.fromNat32(maxPoints)) {
        return interval;
      };
    };
    
    // Fallback to yearly if still too many points
    #year
  };

  // Get all shard keys that might contain data in the time range
  private func getRelevantShardKeys(fromNS: Nat64, toNS: Nat64) : [Text] {
    // Simple implementation: generate all possible YYYY-MM keys in range
    // In production, you'd optimize this with better date/time handling
    
    let startSeconds = fromNS / 1_000_000_000;
    let endSeconds = toNS / 1_000_000_000;
    
    let startYear = 1970 + (startSeconds / (365 * 24 * 60 * 60));
    let endYear = 1970 + (endSeconds / (365 * 24 * 60 * 60));
    
    var keys: [Text] = [];
    
    var year = startYear;
    while (year <= endYear + 1) { // +1 to be safe with approximations
      var month: Nat64 = 1;
      while (month <= 12) {
        let yearText = Nat64.toText(year);
        let monthText = if (month < 10) { "0" # Nat64.toText(month) } else { Nat64.toText(month) };
        keys := Array.append(keys, [yearText # "-" # monthText]);
        month += 1;
      };
      year += 1;
    };
    
    keys
  };

  // Fetch candles from shards within time range
  private func fetchCandlesFromShards(res: Resolution, fromNS: Nat64, toNS: Nat64) : [Candle] {
    let shardMap = getShardMap(res);
    let relevantKeys = getRelevantShardKeys(fromNS, toNS);
    
    var allCandles: [Candle] = [];
    
    for (key in relevantKeys.vals()) {
      switch (Map.get(shardMap, Map.thash, key)) {
        case (?shard) {
          let shardArray = Vector.toArray(shard);
          // Filter candles within time range
          let filteredCandles = Array.filter<Candle>(shardArray, func(candle) {
            candle.t_start >= fromNS and candle.t_start < toNS
          });
          allCandles := Array.append(allCandles, filteredCandles);
        };
        case null {};
      };
    };
    
    // Sort by timestamp (should already be sorted within shards)
    Array.sort<Candle>(allCandles, func(a, b) {
      Nat64.compare(a.t_start, b.t_start)
    })
  };

  // Main OHLC query API
  public query func get_ohlc(
    from_ns: Nat64, 
    to_ns: Nat64,
    interval: Resolution, 
    max_points: Nat32
  ) : async Result.Result<[Candle], ArchiveError> {
    
    if (from_ns >= to_ns) {
      return #err(#InvalidTimeRange);
    };
    
    if (max_points == 0) {
      return #err(#InvalidData);
    };
    
    // Choose optimal interval to stay under max_points
    let optimalInterval = chooseOptimalInterval(from_ns, to_ns, interval, max_points);
    
    // Fetch candles from relevant shards
    let candles = fetchCandlesFromShards(optimalInterval, from_ns, to_ns);
    
    // Limit to max_points if still too many
    let limitedCandles = if (candles.size() > Nat32.toNat(max_points)) {
      Array.subArray(candles, 0, Nat32.toNat(max_points))
    } else {
      candles
    };
    
    #ok(limitedCandles)
  };

  // Get OHLC statistics
  public query func getOHLCStats() : async {
    totalShards: Nat;
    totalCandles: Nat;
    shardBreakdown: [(Resolution, Nat)];
  } {
    let hourShards = Map.size(hour_shards);
    let dayShards = Map.size(day_shards);
    let weekShards = Map.size(week_shards);
    let monthShards = Map.size(month_shards);
    let yearShards = Map.size(year_shards);
    
    var totalCandles = 0;
    for ((_, shard) in Map.entries(hour_shards)) {
      totalCandles += Vector.size(shard);
    };
    for ((_, shard) in Map.entries(day_shards)) {
      totalCandles += Vector.size(shard);
    };
    for ((_, shard) in Map.entries(week_shards)) {
      totalCandles += Vector.size(shard);
    };
    for ((_, shard) in Map.entries(month_shards)) {
      totalCandles += Vector.size(shard);
    };
    for ((_, shard) in Map.entries(year_shards)) {
      totalCandles += Vector.size(shard);
    };
    
    {
      totalShards = hourShards + dayShards + weekShards + monthShards + yearShards;
      totalCandles = totalCandles;
      shardBreakdown = [
        (#hour, hourShards),
        (#day, dayShards), 
        (#week, weekShards),
        (#month, monthShards),
        (#year, yearShards)
      ];
    }
  };

  //=========================================================================
  // OHLC Admin Functions
  //=========================================================================

  // Start automated OHLC backfill process (chunked)
  public shared ({ caller }) func startOHLCBackfill(chunkSize: Nat32) : async Result.Result<Text, ArchiveError> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err(#NotAuthorized);
    };

    // Run initial backfill chunk
    let result = await backfillOHLC(chunkSize);
    switch (result) {
      case (#ok(stats)) {
        base.logger.info(
          "OHLC_ADMIN", 
          "OHLC backfill started by " # Principal.toText(caller) # 
          ". Processed: " # Nat32.toText(stats.processed) # 
          " Created: " # Nat32.toText(stats.newCandlesCreated),
          "startOHLCBackfill"
        );
        #ok("OHLC backfill started. Processed " # Nat32.toText(stats.processed) # " snapshots, created " # Nat32.toText(stats.newCandlesCreated) # " candles.")
      };
      case (#err(error)) { #err(error) };
    }
  };

  // Reset OHLC system (clear all data and restart from beginning)
  public shared ({ caller }) func resetOHLCSystem() : async Result.Result<Text, ArchiveError> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err(#NotAuthorized);
    };

    // Clear all OHLC data
    indexed_until_idx := 0;
    
    // Reset carry state
    carry_hour := null;
    carry_day := null;
    carry_week := null;
    carry_month := null;
    carry_year := null;
    
    // Reset last flushed timestamps
    last_flushed_hour := null;
    last_flushed_day := null;
    last_flushed_week := null;
    last_flushed_month := null;
    last_flushed_year := null;
    
    // Clear all shards
    hour_shards := Map.new<Text, Vector.Vector<Candle>>();
    day_shards := Map.new<Text, Vector.Vector<Candle>>();
    week_shards := Map.new<Text, Vector.Vector<Candle>>();
    month_shards := Map.new<Text, Vector.Vector<Candle>>();
    year_shards := Map.new<Text, Vector.Vector<Candle>>();
    
    base.logger.info(
      "OHLC_ADMIN", 
      "OHLC system reset by " # Principal.toText(caller),
      "resetOHLCSystem"
    );
    
    #ok("OHLC system has been reset. All aggregated data cleared.")
  };

  // Manual flush of current carry state (for debugging/maintenance)
  public shared ({ caller }) func flushOHLCCarryState() : async Result.Result<{flushedCandles: Nat}, ArchiveError> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err(#NotAuthorized);
    };

    var flushedCount = 0;
    
    // Flush all carry accumulators
    let resolutions = [#hour, #day, #week, #month, #year];
    for (res in resolutions.vals()) {
      let acc = getCarryAcc(res);
      if (acc.seen) {
        switch (flush(acc)) {
          case (?candle) {
            appendCandle(res, shardKey(res, candle.t_start), candle);
            flushedCount += 1;
          };
          case null {};
        };
      };
    };
    
    base.logger.info(
      "OHLC_ADMIN", 
      "Manual carry state flush by " # Principal.toText(caller) # ". Flushed " # Nat.toText(flushedCount) # " candles.",
      "flushOHLCCarryState"
    );
    
    #ok({flushedCandles = flushedCount})
  };

  // Legacy OHLC candle data structure (for backward compatibility)
  public type OHLCCandle = {
    timestamp: Int;        // interval start time
    usdOHLC: {open: Float; high: Float; low: Float; close: Float};
    icpOHLC: {open: Nat; high: Nat; low: Nat; close: Nat};
  };

  // Generate OHLC candle data for portfolio values over time intervals
  public query func getOHLCCandles(startTime: Int, endTime: Int, intervalNS: Int) : async Result.Result<[OHLCCandle], ArchiveError> {
    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };
    
    if (intervalNS <= 0) {
      return #err(#InvalidData);
    };

    let totalBlocks = base.getTotalBlocks();
    if (totalBlocks == 0) {
      return #ok([]);
    };

    // Find first block at or after startTime
    let startBlockResult = lower_bound_ts_impl(startTime);
    let startBlockIndex = switch (startBlockResult) {
      case (#ok(index)) { index };
      case (#err(error)) { return #err(error) };
    };

    if (startBlockIndex >= totalBlocks) {
      return #ok([]);
    };

    // Efficient accumulation using Buffer and single-pass batched scan
    let out = Buffer.Buffer<OHLCCandle>(64);

    // Accumulator state for current interval
    var haveAcc = false;
    var accTs : Int = 0;
    var usdOpen : Float = 0.0;
    var usdHigh : Float = 0.0;
    var usdLow : Float = 0.0;
    var usdClose : Float = 0.0;
    var icpOpen : Nat = 0;
    var icpHigh : Nat = 0;
    var icpLow : Nat = 0;
    var icpClose : Nat = 0;

    var lastKnownUSD : ?Float = null;
    var lastKnownICP : ?Nat = null;

    // Helper to push current accumulator as a candle and reset acc state
    let flushAcc = func() : () {
      if (haveAcc) {
        let candle : OHLCCandle = {
          timestamp = accTs;
          usdOHLC = { open = usdOpen; high = usdHigh; low = usdLow; close = usdClose };
          icpOHLC = { open = icpOpen; high = icpHigh; low = icpLow; close = icpClose };
        };
        out.add(candle);
        lastKnownUSD := ?usdClose;
        lastKnownICP := ?icpClose;
        haveAcc := false;
      };
    };

    // Helper to carry-forward candles for empty intervals
    let fillGaps = func(fromTs: Int, toTs: Int) : () {
      if (fromTs >= toTs) { return };
      switch (lastKnownUSD, lastKnownICP) {
        case (?usd, ?icp) {
          var t = fromTs;
          while (t < toTs) {
            let c : OHLCCandle = {
              timestamp = t;
              usdOHLC = { open = usd; high = usd; low = usd; close = usd };
              icpOHLC = { open = icp; high = icp; low = icp; close = icp };
            };
            out.add(c);
            t += intervalNS;
          };
        };
        case _ {};
      };
    };

    // Convert a timestamp to its interval bucket start aligned to startTime
    let alignBucketStart = func(ts: Int) : Int {
      startTime + ((ts - startTime) / intervalNS) * intervalNS
    };

    // Single-pass over blocks in batches
    let batchSize : Nat = 256;
    var currentIndex = startBlockIndex;
    var done = false;

    while (currentIndex < totalBlocks and not done) {
      let remaining = totalBlocks - currentIndex;
      let len = if (remaining > batchSize) { batchSize } else { remaining };
      let blockResult = base.icrc3_get_blocks([{ start = currentIndex; length = len }]);

      // If no blocks are returned, break to avoid infinite loop
      if (blockResult.blocks.size() == 0) { done := true; };

      for (archivedBlock in blockResult.blocks.vals()) {
        let blkVal = archivedBlock.block;

        // Extract portfolio timestamp
        switch (extractTimestampFromBlock(blkVal)) {
          case (?ts) {
            if (ts >= endTime) {
              done := true;
            };
            if (ts < startTime) {
              // Before range, skip
            } else {
              // Extract portfolio values
              switch (extractPortfolioDataFromBlock(blkVal)) {
                case (?data) {
                  let bStart = alignBucketStart(ts);
                  if (not haveAcc) {
                    accTs := bStart;
                    usdOpen := data.totalValueUSD; usdHigh := data.totalValueUSD; usdLow := data.totalValueUSD; usdClose := data.totalValueUSD;
                    icpOpen := data.totalValueICP; icpHigh := data.totalValueICP; icpLow := data.totalValueICP; icpClose := data.totalValueICP;
                    haveAcc := true;
                  } else if (bStart != accTs) {
                    // New interval: flush previous and fill gaps if any
                    flushAcc();
                    fillGaps(accTs + intervalNS, bStart);
                    accTs := bStart;
                    usdOpen := data.totalValueUSD; usdHigh := data.totalValueUSD; usdLow := data.totalValueUSD; usdClose := data.totalValueUSD;
                    icpOpen := data.totalValueICP; icpHigh := data.totalValueICP; icpLow := data.totalValueICP; icpClose := data.totalValueICP;
                    haveAcc := true;
                  } else {
                    // Same interval: update OHLC
                    if (data.totalValueUSD > usdHigh) { usdHigh := data.totalValueUSD };
                    if (data.totalValueUSD < usdLow) { usdLow := data.totalValueUSD };
                    usdClose := data.totalValueUSD;

                    if (data.totalValueICP > icpHigh) { icpHigh := data.totalValueICP };
                    if (data.totalValueICP < icpLow) { icpLow := data.totalValueICP };
                    icpClose := data.totalValueICP;
                  };
                };
                case null { /* Skip unparsable blocks */ };
              };
            };
          };
          case null { /* Skip blocks without timestamps */ };
        };
      };

      currentIndex += len;
    };

    // Flush the last accumulator and fill any trailing gap up to endTime
    if (haveAcc) {
      flushAcc();
      fillGaps(accTs + intervalNS, endTime);
    } else {
      // No data points in range; nothing to fill unless we have prior lastKnown
      // which we don't at this point, so return empty
    };

    #ok(Buffer.toArray(out));
  };

  // Helper function to get blocks within a time range
  private func getBlocksInTimeRange(startTime: Int, endTime: Int) : Result.Result<[PortfolioBlockData], ArchiveError> {
    let totalBlocks = base.getTotalBlocks();
    if (totalBlocks == 0) {
      return #ok([]);
    };

    // Find start index
    let startIndexResult = lower_bound_ts_impl(startTime);
    let startIndex = switch (startIndexResult) {
      case (#ok(index)) { index };
      case (#err(error)) { return #err(error) };
    };

    if (startIndex >= totalBlocks) {
      return #ok([]);
    };

    // Collect blocks until we exceed endTime
    var blocks: [PortfolioBlockData] = [];
    var currentIndex = startIndex;
    
    while (currentIndex < totalBlocks) {
      let blockResult = base.icrc3_get_blocks([{start = currentIndex; length = 1}]);
      
      if (blockResult.blocks.size() > 0) {
        let block = blockResult.blocks[0];
        
        switch (extractTimestampFromBlock(block.block)) {
          case (?timestamp) {
            if (timestamp >= endTime) {
              // We've passed the end time, stop collecting
              return #ok(blocks);
            };
            
            if (timestamp >= startTime) {
              // Extract portfolio data from block
              switch (extractPortfolioDataFromBlock(block.block)) {
                case (?portfolioData) {
                  blocks := Array.append(blocks, [portfolioData]);
                };
                case null {
                  // Skip blocks we can't parse
                };
              };
            };
          };
          case null {
            // Skip blocks without timestamps
          };
        };
      };
      
      currentIndex += 1;
    };
    
    #ok(blocks);
  };

  // Helper function to extract portfolio data from block
  private func extractPortfolioDataFromBlock(blockValue: ArchiveTypes.Value) : ?PortfolioBlockData {
    switch (blockValue) {
      case (#Map(entries)) {
        for ((key, value) in entries.vals()) {
          if (key == "tx") {
            switch (value) {
              case (#Map(txEntries)) {
                for ((txKey, txValue) in txEntries.vals()) {
                  if (txKey == "data") {
                    return parsePortfolioDataFromValue(txValue);
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

  // Helper to parse portfolio data from the data value
  private func parsePortfolioDataFromValue(dataValue: ArchiveTypes.Value) : ?PortfolioBlockData {
    switch (dataValue) {
      case (#Map(dataEntries)) {
        var timestamp: ?Int = null;
        var totalValueICP: ?Nat = null;
        var totalValueUSD: ?Float = null;
        var tokenCount: ?Nat = null;
        var tokens: ?[ArchiveTypes.DetailedTokenSnapshot] = null;
        var pausedTokens: ?[Principal] = null;
        var reason: ?ArchiveTypes.SnapshotReason = null;

        for ((dataKey, dataVal) in dataEntries.vals()) {
          switch (dataKey) {
            case ("ts") {
              switch (dataVal) {
                case (#Int(ts)) { timestamp := ?ts };
                case _ {};
              };
            };
            case ("total_value_icp") {
              switch (dataVal) {
                case (#Nat(val)) { totalValueICP := ?val };
                case _ {};
              };
            };
            case ("total_value_usd") {
              switch (dataVal) {
                case (#Text(val)) { 
                  totalValueUSD := parseFloatFromText(val);
                };
                case (#Float(f)) {
                  totalValueUSD := ?f;
                };
                case _ {};
              };
            };
            case ("token_count") {
              switch (dataVal) {
                case (#Nat(val)) { tokenCount := ?val };
                case _ {};
              };
            };
            // Add parsing for other fields as needed
            case _ {};
          };
        };

        // Return parsed data if we have the essential fields
        switch (timestamp, totalValueICP, totalValueUSD, tokenCount) {
          case (?ts, ?valueICP, ?valueUSD, ?count) {
            let defaultTokens: [ArchiveTypes.DetailedTokenSnapshot] = switch (tokens) {
              case (?t) { t };
              case null { [] };
            };
            let defaultPausedTokens: [Principal] = switch (pausedTokens) {
              case (?p) { p };
              case null { [] };
            };
            let defaultReason: ArchiveTypes.SnapshotReason = switch (reason) {
              case (?r) { r };
              case null { #SystemEvent };
            };
            
            ?{
              timestamp = ts;
              totalValueICP = valueICP;
              totalValueUSD = valueUSD;
              tokenCount = count;
              tokens = defaultTokens;
              pausedTokens = defaultPausedTokens;
              reason = defaultReason;
            };
          };
          case _ { null };
        };
      };
      case _ { null };
    };
  };

  // Text to float parser using library parseFloat if available; fallback minimal parser
  private func parseFloatFromText(text: Text) : ?Float {
    // ASCII-only parser using UTF-8 bytes
    var seenDot = false;
    var intPart : Float = 0.0;
    var fracPart : Float = 0.0;
    var fracDiv : Float = 1.0;
    var sign : Float = 1.0;
    let bytes = Blob.toArray(Text.encodeUtf8(text));
    var i = 0;
    let n = bytes.size();
    while (i < n) {
      let b : Nat8 = bytes[i];
      if (i == 0 and b == 45) { // '-'
        sign := -1.0;
        i += 1;
      } else if (b == 46) { // '.'
        if (seenDot) { return null };
        seenDot := true;
        i += 1;
      } else if (b >= 48 and b <= 57) { // '0'..'9'
        let digit : Float = Float.fromInt(Nat8.toNat(b - 48));
        if (not seenDot) {
          intPart := intPart * 10.0 + digit;
        } else {
          fracDiv := fracDiv * 10.0;
          fracPart := fracPart + digit / fracDiv;
        };
        i += 1;
      } else {
        return null;
      };
    };
    ?(sign * (intPart + fracPart))
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

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };
} 