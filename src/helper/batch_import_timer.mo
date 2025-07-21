import Timer "mo:base/Timer";
import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Nat "mo:base/Nat";
import Logger "./logger";

module {
  
  // Generic timer configuration
  public type BatchImportConfig = {
    batchSize: Nat;
    intervalNS: Nat;
    maxCatchUpBatches: Nat;
  };

  // Default configuration (30 minutes, batch size 50)
  public let DEFAULT_CONFIG : BatchImportConfig = {
    batchSize = 50;
    intervalNS = 1_800_000_000_000; // 30 minutes in nanoseconds
    maxCatchUpBatches = 10;
  };

  // Timer state management
  public class BatchImportTimer(
    logger: Logger.Logger,
    config: BatchImportConfig,
    isMasterAdmin: (Principal) -> Bool
  ) {
    
    private var batchImportTimerId : Nat = 0;
    private let IMPORT_INTERVAL_NS = config.intervalNS;
    private let MAX_CATCH_UP_BATCHES = config.maxCatchUpBatches;

    // Check if timer is currently running
    public func isRunning() : Bool {
      batchImportTimerId != 0
    };

    // Get interval in seconds for status queries
    public func getIntervalSeconds() : Nat {
      IMPORT_INTERVAL_NS / 1_000_000_000
    };

    // Start the batch import timer with custom import function
    public func startTimer<system>(importFunction: () -> async ()) : async* () {
      if (batchImportTimerId != 0) {
        Timer.cancelTimer(batchImportTimerId);
      };

      batchImportTimerId := Timer.setTimer<system>(
        #nanoseconds(IMPORT_INTERVAL_NS),
        func() : async () {
          await importFunction();
          await* startTimer<system>(importFunction);
        }
      );

      logger.info(
        "BATCH_IMPORT",
        "Batch import timer started - Interval: " # Nat.toText(getIntervalSeconds()) # "s",
        "BatchImportTimer.startTimer"
      );
    };

    // Stop the batch import timer
    public func stopTimer() : Bool {
      if (batchImportTimerId != 0) {
        Timer.cancelTimer(batchImportTimerId);
        batchImportTimerId := 0;
        logger.info(
          "BATCH_IMPORT",
          "Batch import timer stopped",
          "BatchImportTimer.stopTimer"
        );
        true
      } else {
        false
      };
    };

    // Admin function: Start batch import system
    public func adminStart<system>(caller: Principal, importFunction: () -> async ()) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      await* startTimer<system>(importFunction);
      #ok("Batch import system started")
    };

    // Admin function: Stop batch import system
    public func adminStop(caller: Principal) : Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      if (stopTimer()) {
        #ok("Batch import system stopped")
      } else {
        #ok("Batch import system was not running")
      };
    };

    // Admin function: Manual batch import
    public func adminManualImport(caller: Principal, importFunction: () -> async ()) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      await importFunction();
      #ok("Manual batch import completed")
    };

    // System upgrade handling
    public func preupgrade() {
      // Timer IDs are not stable, will be restarted in postupgrade
      if (batchImportTimerId != 0) {
        Timer.cancelTimer(batchImportTimerId);
        batchImportTimerId := 0;
      };
    };

    public func postupgrade<system>(importFunction: () -> async ()) {
      // Restart the batch import timer after upgrade with delay
      ignore Timer.setTimer<system>(
        #nanoseconds(10_000_000_000), // 10 seconds delay
        func() : async () {
          await* startTimer<system>(importFunction);
        }
      );
    };

    // Get current timer ID (for debugging/status)
    public func getTimerId() : Nat {
      batchImportTimerId
    };

    // Catch-up import functionality
    public func runCatchUpImport(
      caller: Principal,
      catchUpFunction: () -> async {imported: Nat; failed: Nat}
    ) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      logger.info(
        "BATCH_IMPORT",
        "Starting catch-up import",
        "BatchImportTimer.runCatchUpImport"
      );
      
      var totalImported = 0;
      var totalFailed = 0;
      var batchCount = 0;
      
      // Run multiple import batches up to limit
      label exit_loop while (batchCount < MAX_CATCH_UP_BATCHES) {
        let result = await catchUpFunction();
        
        totalImported += result.imported;
        totalFailed += result.failed;
        batchCount += 1;
        
        // If no new data imported, we're caught up
        if (result.imported == 0) {
          break exit_loop;
        };
      };
      
      logger.info(
        "BATCH_IMPORT",
        "Catch-up import completed - Batches: " # Nat.toText(batchCount) # 
        " Imported: " # Nat.toText(totalImported) # 
        " Failed: " # Nat.toText(totalFailed),
        "BatchImportTimer.runCatchUpImport"
      );
      
      #ok("Catch-up import completed: " # Nat.toText(totalImported) # " records imported, " # 
          Nat.toText(totalFailed) # " failed")
    };
  };
} 