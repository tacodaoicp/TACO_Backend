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

import ICRC3 "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";

shared (deployer) actor class PriceArchiveV2() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type PriceBlockData = ArchiveTypes.PriceBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type TokenDetails = DAO_types.TokenDetails;

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
    initialConfig
  );

  // Price-specific indexes (not covered by base class)
  private stable var priceRangeIndex = Map.new<Nat, [Nat]>(); // Price range -> block indices
  
  // Price-specific statistics
  private stable var totalPriceUpdates : Nat = 0;
  private stable var lastKnownPrices = Map.new<Principal, {icpPrice: Nat; usdPrice: Float; timestamp: Int}>();

  // Treasury interface for batch imports
  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private let treasuryCanister : TreasuryTypes.Self = actor (Principal.toText(canister_ids.getCanisterId(#treasury)));

  // Helper function to get price range bucket for indexing
  private func getPriceRangeBucket(priceICP : Nat) : Nat {
    // Group prices into buckets (e.g., every 1000 ICP units)
    priceICP / 1000;
  };

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
  // Custom Price Archive Functions
  //=========================================================================

  public shared ({ caller }) func archivePriceBlock(price : PriceBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      base.logger.warn("Archive", "Unauthorized price archive attempt by: " # Principal.toText(caller), "archivePriceBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    let blockValue = ArchiveTypes.priceToValue(price, timestamp, null);
    
    // Use base class to store the block
    let blockIndex = base.storeBlock(
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

  // Specific batch import logic for price data
  private func runPriceBatchImport() : async () {
    try {
      // Import price data from treasury
      let tokenDetails = await treasuryCanister.getTokenDetails();
      var imported = 0;
      
      for ((token, details) in tokenDetails.vals()) {
        // Check if price has changed since last import
        let shouldImport = switch (Map.get(lastKnownPrices, Map.phash, token)) {
          case (?lastPrice) {
            lastPrice.icpPrice != details.priceInICP or lastPrice.usdPrice != details.priceInUSD
          };
          case null { true }; // First time seeing this token
        };

        if (shouldImport) {
          let priceData : PriceBlockData = {
            token = token;
            priceICP = details.priceInICP;
            priceUSD = details.priceInUSD;
            source = #Aggregated;
            volume24h = null;
            change24h = null;
          };
          
          let result = await archivePriceBlock(priceData);
          switch (result) {
            case (#ok(_)) { imported += 1 };
            case (#err(e)) { 
              base.logger.error("Batch Import", "Failed to import price for " # Principal.toText(token) # ": " # debug_show(e), "runPriceBatchImport");
            };
          };
        };
      };
      
      base.logger.info("Batch Import", "Imported " # Nat.toText(imported) # " price updates", "runPriceBatchImport");
    } catch (e) {
      base.logger.error("Batch Import", "Price batch import failed: " # Error.message(e), "runPriceBatchImport");
    };
  };

  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startBatchImportSystem<system>(caller, runPriceBatchImport);
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    base.stopBatchImportSystem(caller);
  };

  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, Text> {
    await base.runManualBatchImport(caller, runPriceBatchImport);
  };

  public query func getBatchImportStatus() : async {isRunning: Bool; intervalSeconds: Nat} {
    base.getBatchImportStatus();
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
    base.preupgrade();
  };

  system func postupgrade() {
    base.postupgrade<system>(runPriceBatchImport);
  };
} 