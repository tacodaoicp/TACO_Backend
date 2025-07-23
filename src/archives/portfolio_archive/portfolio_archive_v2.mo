import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Map "mo:map/Map";

import ICRC3 "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import DAO_types "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";

shared (deployer) actor class PortfolioArchiveV2() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type PortfolioBlockData = ArchiveTypes.PortfolioBlockData;
  type AllocationBlockData = ArchiveTypes.AllocationBlockData;
  type ArchiveError = ArchiveTypes.ArchiveError;
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

  private func runPortfolioBatchImport() : async () {
    try {
      let snapshots = await treasuryCanister.getPortfolioHistory(50);
      var imported = 0;
      
      for (snapshot in snapshots.vals()) {
        // Convert treasury snapshot to archive format
        let portfolioData : PortfolioBlockData = {
          timestamp = snapshot.timestamp;
          totalValueICP = snapshot.totalValueICP;
          totalValueUSD = snapshot.totalValueUSD;
          tokenCount = snapshot.tokenCount;
          activeTokens = snapshot.activeTokens;
          pausedTokens = snapshot.pausedTokens;
          reason = switch (snapshot.reason) {
            case (#Scheduled) { #Scheduled };
            case (#PostTrade) { #PostTrade };
            case (#CircuitBreaker) { #CircuitBreaker };
            case (#ManualTrigger) { #ManualTrigger };
            case (#SystemEvent) { #SystemEvent };
          };
        };
        
        let result = await archivePortfolioBlock(portfolioData);
        switch (result) {
          case (#ok(_)) { imported += 1 };
          case (#err(e)) { 
            base.logger.error("Batch Import", "Failed to import portfolio snapshot: " # debug_show(e), "runPortfolioBatchImport");
          };
        };
      };
      
      base.logger.info("Batch Import", "Imported " # Nat.toText(imported) # " portfolio snapshots", "runPortfolioBatchImport");
    } catch (e) {
      base.logger.error("Batch Import", "Portfolio batch import failed: " # debug_show(e), "runPortfolioBatchImport");
    };
  };

  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startBatchImportSystem<system>(caller, runPortfolioBatchImport);
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    base.stopBatchImportSystem(caller);
  };

  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, Text> {
    await base.runManualBatchImport(caller, runPortfolioBatchImport);
  };

  public query func getBatchImportStatus() : async {isRunning: Bool; intervalSeconds: Nat} {
    base.getBatchImportStatus();
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

  //=========================================================================
  // Lifecycle Functions (delegated to base class)
  //=========================================================================

  public func preupgrade() {
    base.preupgrade();
  };

  public func postupgrade() {
    base.postupgrade<system>(runPortfolioBatchImport);
  };
} 