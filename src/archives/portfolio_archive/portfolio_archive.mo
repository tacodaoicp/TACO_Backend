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

shared (deployer) actor class PortfolioArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for batch import
  type PortfolioSnapshot = TreasuryTypes.PortfolioSnapshot;
  type TokenDetails = TreasuryTypes.TokenDetails;

  // Type aliases for convenience
  type Value = ArchiveTypes.Value;
  type Block = ArchiveTypes.Block;
  type PortfolioBlockData = ArchiveTypes.PortfolioBlockData;
  type AllocationBlockData = ArchiveTypes.AllocationBlockData;
  type BlockFilter = ArchiveTypes.BlockFilter;
  type PortfolioMetrics = ArchiveTypes.PortfolioMetrics;
  type ArchiveError = ArchiveTypes.ArchiveError;
  type ArchiveConfig = ArchiveTypes.ArchiveConfig;
  type ArchiveStatus = ArchiveTypes.ArchiveStatus;
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
  let { phash; thash; nhash; ihash } = Map;
  
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

  // Block type tracking - focused on portfolio-related types
  stable var supportedBlockTypes = ["3portfolio", "3allocation"];

  // Indexes for efficient querying (portfolio-specific)
  stable var blockTypeIndex = Map.new<Text, [Nat]>(); // Block type -> block indices
  stable var userIndex = Map.new<Principal, [Nat]>(); // User -> block indices (for allocations)
  stable var tokenIndex = Map.new<Principal, [Nat]>(); // Token -> block indices
  stable var timeIndex = Map.new<Int, [Nat]>(); // Day timestamp -> block indices
  stable var valueIndex = Map.new<Nat, [Nat]>(); // Value range -> block indices (for portfolio values)

  // Batch import state
  stable var lastPortfolioImportTime : Int = 0;
  stable var lastAllocationImportTime : Int = 0;

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

  // Initialize batch import timer with default configuration (after isMasterAdmin is defined)
  private let batchTimer = BatchImportTimer.BatchImportTimer(logger, BatchImportTimer.DEFAULT_CONFIG, isMasterAdmin);

  // Wrapper function for batch import that matches BatchImportTimer interface
  private func runBatchImport() : async () {
    ignore await importPortfolioSnapshotsBatch();
  };

  // ICRC-3 functionality using abstraction
  private let icrc3 = ArchiveICRC3.ArchiveICRC3(
    func() : BTree.BTree<Nat, ArchiveTypes.Block> { blocks },
    func() : Nat { nextBlockIndex },
    this_canister_id,
    ["3portfolio", "3allocation"]
  );

  // Helper function to add entry to index
  private func addToIndex<K>(index : Map.Map<K, [Nat]>, key : K, blockIndex : Nat, hashUtils : Map.HashUtils<K>) {
    let currentIndices = switch (Map.get(index, hashUtils, key)) {
      case (?indices) { indices };
      case null { [] };
    };
    let newIndices = Array.append(currentIndices, [blockIndex]);
    Map.set(index, hashUtils, key, newIndices);
  };

  // Create ICRC-3 block
  private func createBlock(value : Value, blockIndex : Nat) : Block {

    let blockText = debug_show(value);
    let blockHash = Text.hash(blockText);
    let hashArray = [
      Nat32.toNat(blockHash), 
      Nat32.toNat(blockHash >> 8), 
      Nat32.toNat(blockHash >> 16), 
      Nat32.toNat(blockHash >> 24)
    ];
    let newHash = Blob.fromArray(Array.map(hashArray, func(n : Nat) : Nat8 { 
      Nat8.fromNat(n % 256) 
    }));
    
    tipHash := ?newHash;

    {
      id = blockIndex;
      block = value;
    };
  };

  // Archive portfolio snapshot
  public shared ({ caller }) func archivePortfolioBlock(portfolio : PortfolioBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not isAuthorized(caller, #ArchiveData)) {
      logger.warn("Archive", "Unauthorized portfolio archive attempt by: " # Principal.toText(caller), "archivePortfolioBlock");
      return #err(#NotAuthorized);
    };

    let blockValue = ArchiveTypes.portfolioToValue(portfolio, null);
    let blockIndex = nextBlockIndex;
    let block = createBlock(blockValue, blockIndex);
    
    // Store block
    ignore BTree.insert(blocks, Nat.compare, blockIndex, block);
    
    // Update indexes
    addToIndex(blockTypeIndex, "3portfolio", blockIndex, thash);
    addToIndex(timeIndex, timestampToDay(portfolio.timestamp), blockIndex, ihash);
    addToIndex(valueIndex, portfolio.totalValueICP, blockIndex, nhash);

    for (token in portfolio.activeTokens.vals()) {
      addToIndex(tokenIndex, token, blockIndex, phash);
    };

    nextBlockIndex += 1;

    logger.info("Archive", "Archived portfolio block at index: " # Nat.toText(blockIndex) # 
      " Value: " # Nat.toText(portfolio.totalValueICP) # " ICP", "archivePortfolioBlock");

    #ok(blockIndex);
  };

  // Archive allocation change
  public shared ({ caller }) func archiveAllocationBlock(allocation : AllocationBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not isAuthorized(caller, #ArchiveData)) {
      logger.warn("Archive", "Unauthorized allocation archive attempt by: " # Principal.toText(caller), "archiveAllocationBlock");
      return #err(#NotAuthorized);
    };

    let timestamp = Time.now();
    
    let reasonText = switch (allocation.reason) {
      case (#UserUpdate) { "user_update" };
      case (#FollowAction) { "follow_action" };
      case (#SystemRebalance) { "system_rebalance" };
      case (#Emergency) { "emergency" };
    };

    let oldAllocationArray = #Array(Array.map(allocation.oldAllocation, func(alloc : DAO_types.Allocation) : Value = 
      #Map([
        ("token", ArchiveTypes.principalToValue(alloc.token)),
        ("basis_points", #Nat(alloc.basisPoints))
      ])
    ));

    let newAllocationArray = #Array(Array.map(allocation.newAllocation, func(alloc : DAO_types.Allocation) : Value = 
      #Map([
        ("token", ArchiveTypes.principalToValue(alloc.token)),
        ("basis_points", #Nat(alloc.basisPoints))
      ])
    ));

    let entries = [
      ("btype", #Text("3allocation")),
      ("ts", #Int(timestamp)),
      ("user", ArchiveTypes.principalToValue(allocation.user)),
      ("old_allocation", oldAllocationArray),
      ("new_allocation", newAllocationArray),
      ("voting_power", #Nat(allocation.votingPower)),
      ("reason", #Text(reasonText)),
    ];

    let blockValue = #Map(entries);
    let blockIndex = nextBlockIndex;
    let block = createBlock(blockValue, blockIndex);
    
    // Store block
    ignore BTree.insert(blocks, Nat.compare, blockIndex, block);
    
    // Update indexes
    addToIndex(blockTypeIndex, "3allocation", blockIndex, thash);
    addToIndex(userIndex, allocation.user, blockIndex, phash);
    addToIndex(timeIndex, timestampToDay(timestamp), blockIndex, ihash);

    nextBlockIndex += 1;

    logger.info("Archive", "Archived allocation block at index: " # Nat.toText(blockIndex) # 
      " User: " # Principal.toText(allocation.user), "archiveAllocationBlock");

    #ok(blockIndex);
  };

  // ICRC-3 Standard endpoints (using abstraction)
  public query func icrc3_get_blocks(args : ICRC3.GetBlocksArgs) : async ICRC3.GetBlocksResult {
    icrc3.icrc3_get_blocks(args);
  };

  public query func icrc3_supported_block_types() : async [ICRC3.BlockType] {
    icrc3.icrc3_supported_block_types();
  };

  public query func icrc3_get_archives(args : ICRC3.GetArchivesArgs) : async ICRC3.GetArchivesResult {
    icrc3.icrc3_get_archives(args);
  };

  public query func icrc3_get_tip_certificate() : async ?ICRC3.DataCertificate {
    icrc3.icrc3_get_tip_certificate();
  };

  // Batch import from treasury
  private func importPortfolioSnapshotsBatch() : async { imported : Nat; failed : Nat } {
    try {
      let treasury = actor(Principal.toText(TREASURY_ID)) : actor {
        getPortfolioHistory : shared (Nat) -> async Result.Result<TreasuryTypes.PortfolioHistoryResponse, TreasuryTypes.PortfolioSnapshotError>;
      };
      
      let result = await treasury.getPortfolioHistory(50); // Use default batch size
      
              switch (result) {
          case (#ok(response)) {
            var imported = 0;
            var failed = 0;
            
            for (snapshot in response.snapshots.vals()) {
            let portfolioBlock : PortfolioBlockData = {
              timestamp = snapshot.timestamp;
              totalValueICP = snapshot.totalValueICP;
              totalValueUSD = snapshot.totalValueUSD;
              tokenCount = snapshot.tokens.size();
              activeTokens = Array.map<TreasuryTypes.TokenSnapshot, Principal>(snapshot.tokens, func(token) = token.token);
              pausedTokens = [];
              reason = #Scheduled; // Default for imported snapshots
            };
            
            let archiveResult = await archivePortfolioBlock(portfolioBlock);
            switch (archiveResult) {
              case (#ok(_)) {
                imported += 1;
                lastPortfolioImportTime := Int.max(lastPortfolioImportTime, snapshot.timestamp);
              };
              case (#err(error)) {
                failed += 1;
                logger.error(
                  "BATCH_IMPORT",
                  "Failed to import portfolio snapshot: " # debug_show(error),
                  "importPortfolioSnapshotsBatch"
                );
              };
            };
          };
          
          if (imported > 0) {
            logger.info(
              "BATCH_IMPORT",
              "Imported " # Nat.toText(imported) # " portfolio snapshots, failed " # Nat.toText(failed),
              "importPortfolioSnapshotsBatch"
            );
          };
          
          { imported = imported; failed = failed };
        };
        case (#err(error)) {
          let errorMsg = switch (error) {
            case (#NotAuthorized) { "Not authorized to access portfolio history" };
            case (#InvalidLimit) { "Invalid limit for portfolio history request" };
            case (#SystemError(msg)) { "System error: " # msg };
          };
          logger.error(
            "BATCH_IMPORT",
            "Failed to get portfolio history: " # errorMsg,
            "importPortfolioSnapshotsBatch"
          );
          { imported = 0; failed = 1 };
        };
      };
    } catch (e) {
      logger.error(
        "BATCH_IMPORT",
        "Exception during portfolio import: " # Error.message(e),
        "importPortfolioSnapshotsBatch"
      );
      { imported = 0; failed = 1 };
    };
  };

  // Timer functionality now handled by BatchImportTimer class

  // Admin functions
  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, ArchiveError> {
    let result = await batchTimer.adminStart<system>(caller, runBatchImport);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, ArchiveError> {
    let result = batchTimer.adminStop(caller);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, ArchiveError> {
    let result = await batchTimer.adminManualImport(caller, runBatchImport);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  /**
   * Run catch-up import (admin function)
   */
  public shared ({ caller }) func catchUpImport() : async Result.Result<Text, ArchiveError> {
    let result = await batchTimer.runCatchUpImport(caller, importPortfolioSnapshotsBatch);
    switch (result) {
      case (#ok(message)) { #ok(message) };
      case (#err(message)) { #err(#NotAuthorized) };
    };
  };

  /**
   * Get batch import status
   */
  public shared ({ caller }) func getBatchImportStatus() : async Result.Result<{
    isRunning: Bool;
    intervalSeconds: Nat;
    lastPortfolioImportTime: Int;
    lastAllocationImportTime: Int;
    totalBlocks: Nat;
  }, ArchiveError> {
    if (not isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };
    
    #ok({
      isRunning = batchTimer.isRunning();
      intervalSeconds = batchTimer.getIntervalSeconds();
      lastPortfolioImportTime = lastPortfolioImportTime;
      lastAllocationImportTime = lastAllocationImportTime;
      totalBlocks = nextBlockIndex;
    });
  };

  public query ({ caller }) func getArchiveStatus() : async Result.Result<ArchiveStatus, ArchiveError> {
    if (not isQueryAuthorized(caller)) {
      return #err(#NotAuthorized);
    };

    #ok({
      totalBlocks = nextBlockIndex;
      oldestBlock = if (nextBlockIndex > 0) { ?0 } else { null };
      newestBlock = if (nextBlockIndex > 0) { ?(nextBlockIndex - 1) } else { null };
      supportedBlockTypes = supportedBlockTypes;
      storageUsed = BTree.size(blocks);
      lastArchiveTime = lastPortfolioImportTime;
    });
  };

  // System upgrade handling
  system func preupgrade() {
    batchTimer.preupgrade();
  };

  system func postupgrade() {
    batchTimer.postupgrade<system>(runBatchImport);
  };
}

