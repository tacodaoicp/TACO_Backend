import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Map "mo:map/Map";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Text "mo:base/Text";

import ICRC3 "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";

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

  private let base = ArchiveBase.ArchiveBase<TradeBlockData>(
    this_canister_id(),
    ["3trade", "3circuit", "3pause"],
    initialConfig
  );

  // Trading-specific indexes (not covered by base class)
  private stable var traderIndex = Map.new<Principal, [Nat]>(); // Trader -> block indices

  // Trading-specific statistics
  private stable var totalTrades : Nat = 0;
  private stable var totalSuccessfulTrades : Nat = 0;
  private stable var totalVolume : Nat = 0;

  // Treasury interface for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(canister_ids.getCanisterId(#treasury)));

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
  // Custom Trading Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveTradeBlock(trade : TradeBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized trade archive attempt by: " # Principal.toText(caller), "archiveTradeBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    let blockValue = ArchiveTypes.tradeToValue(trade, timestamp, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock(
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

  public shared ({ caller }) func archiveCircuitBreakerBlock(circuitBreaker : CircuitBreakerBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized circuit breaker archive attempt by: " # Principal.toText(caller), "archiveCircuitBreakerBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    let blockValue = ArchiveTypes.circuitBreakerToValue(circuitBreaker, timestamp, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock(
      blockValue,
      "3circuit",
      circuitBreaker.tokensAffected,
      timestamp
    );

    base.logger.error("Archive", "Archived circuit breaker block at index: " # Nat.toText(blockIndex) # 
      " Event: " # debug_show(circuitBreaker.eventType), "archiveCircuitBreakerBlock");

    #ok(blockIndex);
  };

  public shared ({ caller }) func archiveTradingPauseBlock(pause : TradingPauseBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized trading pause archive attempt by: " # Principal.toText(caller), "archiveTradingPauseBlock");
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
    
    // Use base class to store the block
    let blockIndex = base.storeBlock(
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

  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    base.getLogs(count, caller);
  };

  //=========================================================================
  // Batch Import System (delegated to base class)
  //=========================================================================

  // Specific batch import logic for trading data
  private func runTradingBatchImport() : async () {
    try {
      // Import price alerts from treasury
      let alerts = await treasuryCanister.getPriceAlerts(50);
      var imported = 0;
      
      for (alert in alerts.vals()) {
        let tradeData : TradeBlockData = {
          trader = alert.user;
          tokenSold = alert.token;
          tokenBought = alert.token; // Price alert doesn't involve actual trading
          amountSold = 0;
          amountBought = 0;
          exchange = #ICPSwap; // Default
          success = true;
          slippage = 0.0;
          fee = 0;
          error = null;
        };
        
        let result = await archiveTradeBlock(tradeData);
        switch (result) {
          case (#ok(_)) { imported += 1 };
          case (#err(e)) { 
            base.logger.error("Batch Import", "Failed to import price alert: " # debug_show(e), "runTradingBatchImport");
          };
        };
      };
      
      base.logger.info("Batch Import", "Imported " # Nat.toText(imported) # " price alerts", "runTradingBatchImport");
    } catch (e) {
      base.logger.error("Batch Import", "Batch import failed: " # debug_show(e), "runTradingBatchImport");
    };
  };

  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startBatchImportSystem<system>(caller, runTradingBatchImport);
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    base.stopBatchImportSystem(caller);
  };

  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, Text> {
    await base.runManualBatchImport(caller, runTradingBatchImport);
  };

  public query func getBatchImportStatus() : async {isRunning: Bool; intervalSeconds: Nat} {
    base.getBatchImportStatus();
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

  public func preupgrade() {
    base.preupgrade();
  };

  public func postupgrade() {
    base.postupgrade<system>(runTradingBatchImport);
  };
} 