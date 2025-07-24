import Time "mo:base/Time";
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
import Blob "mo:base/Blob";

import ICRC3 "mo:icrc3-mo/service";
import ArchiveTypes "../archives/archive_types";
import TreasuryTypes "../treasury/treasury_types";
import DAO_types "../DAO_backend/dao_types";
import SpamProtection "./spam_protection";
import Logger "./logger";
import CanisterIds "./CanisterIds";
import BatchImportTimer "./batch_import_timer";
import ArchiveAuthorization "./archive_authorization";
import ArchiveICRC3 "./archive_icrc3";

module {

  /// Generic Archive Base Class
  /// T: The specific block data type this archive handles (e.g., TradeBlockData, PortfolioBlockData)
  public class ArchiveBase<T>(
    canisterPrincipal: Principal,
    supportedBlockTypes: [Text],
    initialConfig: ArchiveTypes.ArchiveConfig
  ) {

    // Type aliases
    public type Value = ArchiveTypes.Value;
    public type Block = ArchiveTypes.Block;
    public type ArchiveError = ArchiveTypes.ArchiveError;
    public type ArchiveConfig = ArchiveTypes.ArchiveConfig;
    public type ArchiveStatus = ArchiveTypes.ArchiveStatus;
    public type ArchiveQueryResult = ArchiveTypes.ArchiveQueryResult;
    public type BlockFilter = ArchiveTypes.BlockFilter;

    // Core infrastructure
    private let canister_ids = CanisterIds.CanisterIds(canisterPrincipal);
    private let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
    private let TREASURY_ID = canister_ids.getCanisterId(#treasury);
    
    // Logger
    public let logger = Logger.Logger();
    
    // Spam protection
    private let spamGuard = SpamProtection.SpamGuard(canisterPrincipal);
    
    // Map utilities
    private let { phash; thash; nhash; ihash } = Map;
    
    // Master admins (this should be passed in or configured per deployment)
    private let masterAdmins = [
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
    public var config : ArchiveConfig = initialConfig;

    // ICRC-3 Block storage - using BTree for efficient range queries
    public var blocks = BTree.init<Nat, Block>(?64);
    public var nextBlockIndex : Nat = 0;
    public var tipHash : ?Blob = null;

    // Common indexes for efficient querying
    public var blockTypeIndex = Map.new<Text, [Nat]>(); // Block type -> block indices
    public var tokenIndex = Map.new<Principal, [Nat]>(); // Token -> block indices
    public var timeIndex = Map.new<Int, [Nat]>(); // Day timestamp -> block indices

    // Statistics
    public var lastArchiveTime : Int = 0;
    
    // Authorization helper
    private let auth = ArchiveAuthorization.ArchiveAuthorization(
      masterAdmins,
      TREASURY_ID,
      DAO_BACKEND_ID,
      func() { canisterPrincipal }
    );

    // Authorization helper functions
    public func isMasterAdmin(caller : Principal) : Bool {
      auth.isMasterAdmin(caller);
    };

    public func isAuthorized(caller : Principal, function : ArchiveTypes.AdminFunction) : Bool {
      auth.isAuthorized(caller, function);
    };

    public func isQueryAuthorized(caller : Principal) : Bool {
      auth.isQueryAuthorized(caller);
    };

    // Batch import timer
    public let batchTimer = BatchImportTimer.BatchImportTimer(
      logger,
      BatchImportTimer.DEFAULT_CONFIG,
      isMasterAdmin
    );

    // ICRC-3 functionality
    private let icrc3 = ArchiveICRC3.ArchiveICRC3(
      func() : BTree.BTree<Nat, ArchiveTypes.Block> { blocks },
      func() : Nat { nextBlockIndex },
      func() : Principal { canisterPrincipal },
      supportedBlockTypes
    );

    // Helper function to convert timestamp to day
    public func timestampToDay(timestamp : Int) : Int {
      timestamp / 86400000000000; // Convert nanoseconds to days
    };

    // Helper function to add entry to index
    public func addToIndex<K>(index : Map.Map<K, [Nat]>, key : K, blockIndex : Nat, hashUtils : Map.HashUtils<K>) {
      let existing = switch (Map.get(index, hashUtils, key)) {
        case (?ids) { ids };
        case null { [] };
      };
      Map.set(index, hashUtils, key, Array.append(existing, [blockIndex]));
    };

    // Hash calculation for blocks using ICRC-3 representation-independent hashing
    public func calculateBlockHash(block : Value) : Blob {
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

    // Create ICRC-3 block with proper parent hash
    public func createBlock(value : Value, blockIndex : Nat) : Block {
      let newHash = calculateBlockHash(value);
      tipHash := ?newHash;

      {
        id = blockIndex;
        block = value;
      };
    };

    // Store a block and update basic indexes
    public func storeBlock(
      value : Value, 
      blockType : Text,
      tokens : [Principal],
      timestamp : Int
    ) : Nat {
      let blockIndex = nextBlockIndex;
      let block = createBlock(value, blockIndex);
      
      // Store block
      ignore BTree.insert(blocks, Nat.compare, blockIndex, block);
      
      // Update basic indexes
      addToIndex(blockTypeIndex, blockType, blockIndex, thash);
      addToIndex(timeIndex, timestampToDay(timestamp), blockIndex, ihash);
      
      for (token in tokens.vals()) {
        addToIndex(tokenIndex, token, blockIndex, phash);
      };

      nextBlockIndex += 1;
      blockIndex;
    };

    //=========================================================================
    // ICRC-3 Standard endpoints (delegated to abstraction)
    //=========================================================================

    public func icrc3_get_archives(args : ICRC3.GetArchivesArgs) : ICRC3.GetArchivesResult {
      icrc3.icrc3_get_archives(args);
    };

    public func icrc3_get_tip_certificate() : ?ICRC3.DataCertificate {
      icrc3.icrc3_get_tip_certificate();
    };

    public func icrc3_get_blocks(args : ICRC3.GetBlocksArgs) : ICRC3.GetBlocksResult {
      icrc3.icrc3_get_blocks(args);
    };

    public func icrc3_supported_block_types() : [ICRC3.BlockType] {
      icrc3.icrc3_supported_block_types();
    };

    //=========================================================================
    // Query Functions
    //=========================================================================

    public func queryBlocks(filter : BlockFilter, caller : Principal) : Result.Result<ArchiveQueryResult, ArchiveError> {
      if (not isQueryAuthorized(caller)) {
        return #err(#NotAuthorized);
      };

      let candidateBlocks = Vector.new<Block>();
      
      // If no specific filters, get all blocks
      if (filter.blockTypes == null and filter.tokens == null and filter.traders == null) {
        // Get all blocks by iterating through stored indices
        if (nextBlockIndex > 0) {
          for (i in Iter.range(0, nextBlockIndex - 1)) {
            switch (BTree.get(blocks, Nat.compare, i)) {
              case (?block) { Vector.add(candidateBlocks, block) };
              case null {}; // Skip missing blocks
            };
          };
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

    //=========================================================================
    // Admin Functions
    //=========================================================================

    public func updateConfig(newConfig : ArchiveConfig, caller : Principal) : Result.Result<Text, ArchiveError> {
      if (not isAuthorized(caller, #UpdateConfig)) {
        return #err(#NotAuthorized);
      };

      config := newConfig;
      logger.info("Archive", "Configuration updated by: " # Principal.toText(caller), "updateConfig");
      
      #ok("Configuration updated successfully");
    };

    public func getArchiveStatus(caller : Principal) : Result.Result<ArchiveStatus, ArchiveError> {
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
    public func getLogs(count : Nat, caller : Principal) : [Logger.LogEntry] {
      if (not isAuthorized(caller, #GetLogs)) {
        return [];
      };
      
      logger.getLastLogs(count);
    };

    //=========================================================================
    // Batch Import System (delegated to timer abstraction)
    //=========================================================================

    public func startBatchImportSystem<system>(caller : Principal, importFunction : () -> async ()) : async Result.Result<Text, Text> {
      await batchTimer.adminStart<system>(caller, importFunction);
    };

    public func stopBatchImportSystem(caller : Principal) : Result.Result<Text, Text> {
      batchTimer.adminStop(caller);
    };

    public func runManualBatchImport(caller : Principal, importFunction : () -> async ()) : async Result.Result<Text, Text> {
      await batchTimer.adminManualImport(caller, importFunction);
    };

    public func getBatchImportStatus() : {isRunning: Bool; intervalSeconds: Nat} {
      {
        isRunning = batchTimer.isRunning();
        intervalSeconds = batchTimer.getIntervalSeconds();
      };
    };

    public func catchUpImport(
      caller : Principal, 
      catchUpFunction : () -> async {imported: Nat; failed: Nat}
    ) : async Result.Result<Text, Text> {
      await batchTimer.runCatchUpImport(caller, catchUpFunction);
    };

    //=========================================================================
    // Lifecycle Functions
    //=========================================================================

    public func preupgrade() {
      batchTimer.preupgrade();
    };

    public func postupgrade<system>(importFunction : () -> async ()) {
      batchTimer.postupgrade<system>(importFunction);
    };
  };
} 