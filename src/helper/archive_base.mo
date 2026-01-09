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
import Nat64 "mo:base/Nat64";
import Text "mo:base/Text";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";

import ICRC3 "mo:icrc3-mo";                    // ← THE FIX: Use actual library
import ICRC3Service "mo:icrc3-mo/service";    // ← Keep service types
import ArchiveTypes "../archives/archive_types";
import TreasuryTypes "../treasury/treasury_types";
import DAO_types "../DAO_backend/dao_types";
import SpamProtection "./spam_protection";
import Logger "./logger";
import CanisterIds "./CanisterIds";
import BatchImportTimer "./batch_import_timer";
import ArchiveAuthorization "./archive_authorization";

module {

  /// Generic Archive Base Class
  /// T: The specific block data type this archive handles (e.g., TradeBlockData, PortfolioBlockData)
  public class ArchiveBase<T>(
    canisterPrincipal: Principal,
    supportedBlockTypes: [Text],
    initialConfig: ArchiveTypes.ArchiveConfig,
    deployerCaller: Principal,
    icrc3StateRef: {var value: ?ICRC3.State}
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
      Principal.fromText("as6jn-gaoo7-k4kji-tdkxg-jlsrk-avxkc-zu76j-vz7hj-di3su-2f74z-qqe"),
      Principal.fromText("r27hb-ckxon-xohqv-afcvx-yhemm-xoggl-37dg6-sfyt3-n6jer-ditge-6qe"),
      Principal.fromText("yjdlk-jqx52-ha6xa-w6iqe-b4jrr-s5ova-mirv4-crlfi-xgsaa-ib3cg-3ae"),
      Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe"),
      Principal.fromText("nfzo4-i26mj-e2tuj-bt3ba-cuco4-vcqxx-ybjw7-gzyzh-kvyp7-wjeyp-hqe")
    ];

    // Configuration
    public var config : ArchiveConfig = initialConfig;

    // Create ICRC3 Environment with correct Store type from the library  
    private let icrc3Environment = {
      get_certificate_store = null : ?(() -> ICRC3.CertTree.Store);
      updated_certification = null : ?((Blob, Nat) -> Bool);
    };

    // ICRC3 Library - Scalable Storage (up to 500GB)
    private let icrc3 = ICRC3.ICRC3(
      icrc3StateRef.value,              // stored state
      deployerCaller,                   // caller
      canisterPrincipal,                // canister
      ?{
        maxRecordsInArchiveInstance = 10000000;  // 10M records per archive
        maxArchivePages = 62500;                 // Up to 500GB total
        archiveIndexType = #Stable;
        maxActiveRecords = 2000;
        maxRecordsToArchive = 1000;
        archiveCycles = 2000000000000;           // 2T cycles for archive creation
        settleToRecords = 100;
        archiveControllers = ?null;
        supportedBlocks = Array.map<Text, ICRC3.BlockType>(supportedBlockTypes, func(blockType) : ICRC3.BlockType {
          { block_type = blockType; url = "https://github.com/ICRC-3/icrc3-mo" }
        });
      },
      ?icrc3Environment,                         // provide proper environment
      func(newState : ICRC3.State) {             // state change callback
        icrc3StateRef.value := ?newState;
      }
    );

    // Legacy indexes for backward compatibility (TODO: migrate to ICRC3)
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
      func() { canisterPrincipal },
      canister_ids.isKnownCanister
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

    // Old ICRC3 wrapper removed - now using real ICRC3 library above

    // Helper function to convert timestamp to day
    public func timestampToDay(timestamp : Int) : Int {
      timestamp / 86400000000000; // Convert nanoseconds to days
    };

    // Helper function to convert ArchiveTypes.Value to ICRC3 transaction details
    private func valueToDetails(value : Value) : [(Text, ICRC3.Value)] {
      switch (value) {
        case (#Map(entries)) {
          // Convert each entry's value from ArchiveTypes.Value to ICRC3.Value
          Array.map<(Text, Value), (Text, ICRC3.Value)>(entries, func((key : Text, val : Value)) : (Text, ICRC3.Value) {
            (key, convertToLibraryValue(val))
          });
        };
        case (#Text(t)) { [("data", #Text(t))] };
        case (#Nat(n)) { [("data", #Nat(n))] };
        case (#Int(i)) { [("data", #Int(i))] };
        case (#Blob(b)) { [("data", #Blob(b))] };
        case (#Array(arr)) { [("data", #Array(Array.map<Value, ICRC3.Value>(arr, convertToLibraryValue)))] };
      }
    };

    // Since ArchiveTypes.Value is already ICRC3Service.Value, we don't need conversion
    // for the top-level parameter - the types are compatible
    private func convertToLibraryValue(value : Value) : ICRC3.Value {
      switch (value) {
        case (#Map(entries)) { 
          #Map(Array.map<(Text, Value), (Text, ICRC3.Value)>(entries, func((key : Text, val : Value)) : (Text, ICRC3.Value) {
            (key, convertToLibraryValue(val))
          }))
        };
        case (#Text(t)) { #Text(t) };
        case (#Nat(n)) { #Nat(n) };
        case (#Int(i)) { #Int(i) };
        case (#Blob(b)) { #Blob(b) };
        case (#Array(arr)) { #Array(Array.map<Value, ICRC3.Value>(arr, convertToLibraryValue)) };
      }
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
      // ICRC3 library handles hashing internally, no need for manual tipHash management
      {
        id = blockIndex;
        block = value;
      };
    };

    // Store a block using ICRC3 library (scalable storage up to 500GB!)
    public func storeBlock<system>(
      value : Value, 
      blockType : Text,
      tokens : [Principal],
      timestamp : Int
    ) : Nat {
      // Create transaction as a Value map since add_record expects Value types
      let transactionValue = #Map([
        ("operation", #Text(blockType)),
        ("timestamp", #Int(timestamp)),
        ("data", convertToLibraryValue(value))
      ]);
      
      let blockIndex = icrc3.add_record<system>(transactionValue, null);
      
      // Update legacy indexes for backward compatibility
      addToIndex(blockTypeIndex, blockType, blockIndex, thash);
      addToIndex(timeIndex, timestampToDay(timestamp), blockIndex, ihash);
      
      for (token in tokens.vals()) {
        addToIndex(tokenIndex, token, blockIndex, phash);
      };

      blockIndex;
    };

    //=========================================================================
    // ICRC-3 Standard endpoints (delegated to abstraction)
    //=========================================================================

    public func icrc3_get_archives(args : ICRC3Service.GetArchivesArgs) : ICRC3Service.GetArchivesResult {
      icrc3.get_archives(args);
    };

    public func icrc3_get_tip_certificate() : ?ICRC3Service.DataCertificate {
      icrc3.get_tip_certificate();
    };

    public func icrc3_get_blocks(args : ICRC3Service.GetBlocksArgs) : ICRC3Service.GetBlocksResult {
      icrc3.get_blocks(args);
    };

    public func icrc3_supported_block_types() : [ICRC3Service.BlockType] {
      icrc3.supported_block_types();
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
        // Use ICRC3 library to get blocks - this is a placeholder for now
        // The actual implementation should use icrc3.get_blocks() properly
        // For now, we'll use legacy indexes to find blocks
        // TODO: Implement proper ICRC3 block querying
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
        
        // Get blocks from indices using ICRC3 library
        // TODO: Implement proper ICRC3 block retrieval
        // For now, this is a placeholder - the actual implementation would use:
        // let icrc3Blocks = icrc3.get_blocks({start = firstIndex; length = count});
        // and then convert the results to the expected format
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

    // Simple public method to get total blocks without authorization (for public stats)
    public func getTotalBlocks() : Nat {
      let stats = icrc3.stats();
      stats.localLedgerSize;
    };

    public func getArchiveStatus(caller : Principal) : Result.Result<ArchiveStatus, ArchiveError> {
      if (not isQueryAuthorized(caller)) {
        return #err(#NotAuthorized);
      };

      // Get stats from ICRC3 library
      let stats = icrc3.stats();
      let totalBlocks = stats.localLedgerSize;
      
      let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
      let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };

      #ok({
        totalBlocks = totalBlocks;
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
    // Batch Import System (three-tier timer architecture)
    //=========================================================================

    // Legacy compatibility - single import function
    public func startBatchImportSystem<system>(caller : Principal, importFunction : () -> async ()) : async Result.Result<Text, Text> {
      await batchTimer.adminStart<system>(caller, importFunction);
    };

    // Advanced - multiple import functions for trading archive
    public func startAdvancedBatchImportSystem<system>(
      caller : Principal,
      tradeImport: ?(() -> async {imported: Nat; failed: Nat}),
      circuitBreakerImport: ?(() -> async {imported: Nat; failed: Nat}),
      generalImport: ?(() -> async {imported: Nat; failed: Nat})
    ) : async Result.Result<Text, Text> {
      await batchTimer.adminStartAdvanced<system>(caller, tradeImport, circuitBreakerImport, generalImport);
    };

    public func stopBatchImportSystem(caller : Principal) : Result.Result<Text, Text> {
      batchTimer.adminStop(caller);
    };

    // Emergency stop - cancels all running timers
    public func stopAllTimers(caller : Principal) : Result.Result<Text, Text> {
      batchTimer.adminStopAll(caller);
    };

    // Emergency force reset for stuck middle loop
    public func forceResetMiddleLoop(caller : Principal) : Result.Result<Text, Text> {
      batchTimer.adminForceResetMiddleLoop(caller);
    };

    // Legacy compatibility - single import function
    public func runManualBatchImport(caller : Principal, importFunction : () -> async ()) : async Result.Result<Text, Text> {
      await batchTimer.adminManualImport(caller, importFunction);
    };

    // Advanced manual import - multiple import functions
    public func runAdvancedManualBatchImport<system>(
      caller : Principal,
      tradeImport: ?(() -> async {imported: Nat; failed: Nat}),
      circuitBreakerImport: ?(() -> async {imported: Nat; failed: Nat}),
      generalImport: ?(() -> async {imported: Nat; failed: Nat})
    ) : async Result.Result<Text, Text> {
      await batchTimer.adminManualImportAdvanced<system>(caller, tradeImport, circuitBreakerImport, generalImport);
    };

    // Configuration
    public func setMaxInnerLoopIterations(caller : Principal, iterations : Nat) : Result.Result<Text, Text> {
      batchTimer.setMaxInnerLoopIterations(caller, iterations);
    };

    // Status - legacy compatibility
    public func getBatchImportStatus() : {isRunning: Bool; intervalSeconds: Nat} {
      {
        isRunning = batchTimer.isRunning();
        intervalSeconds = batchTimer.getIntervalSeconds();
      };
    };

    // Status - comprehensive three-tier timer status
    public func getTimerStatus() : BatchImportTimer.TimerStatus {
      batchTimer.getTimerStatus();
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