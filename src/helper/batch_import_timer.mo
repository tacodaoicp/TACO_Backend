import Timer "mo:base/Timer";
import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import Text "mo:base/Text";
import Error "mo:base/Error";
import Logger "./logger";

module {
  
  // Timer configuration
  public type BatchImportConfig = {
    batchSize: Nat;
    outerLoopIntervalNS: Nat; // How often outer loop runs (e.g., 30 minutes)
    maxInnerLoopIterations: Nat; // Max batches per inner loop execution
    middleLoopDelayNS: Nat; // Delay between middle loop iterations (10 seconds)
    innerLoopDelayNS: Nat; // Delay between inner loop iterations (10 seconds)
  };

  // Default configuration
  public let DEFAULT_CONFIG : BatchImportConfig = {
    batchSize = 100;
    outerLoopIntervalNS = 1_800_000_000_000; // 30 minutes
    maxInnerLoopIterations = 100;
    middleLoopDelayNS = 10_000_000_000; // 10 seconds
    innerLoopDelayNS = 10_000_000_000; // 10 seconds
  };

  // Middle loop states for trading archive
  public type MiddleLoopState = {
    #Done;
    #StartTradeImport;
    #MonitoringTradeImport;
    #StartCircuitBreakerImport;
    #MonitoringCircuitBreakerImport;
    // For non-trading archives:
    #StartImport;
    #MonitoringImport;
  };

  // Inner loop types
  public type InnerLoopType = {
    #None;
    #Trades;
    #CircuitBreakers;
    #Portfolio;
    #Price;
    #General; // For single-type archives
  };

  // Status information
  public type TimerStatus = {
    // Outer loop
    outerLoopRunning: Bool;
    outerLoopLastRun: Int;
    outerLoopTotalRuns: Nat;
    outerLoopIntervalSeconds: Nat;
    
    // Middle loop  
    middleLoopRunning: Bool;
    middleLoopLastRun: Int;
    middleLoopTotalRuns: Nat;
    middleLoopCurrentState: Text;
    middleLoopStartTime: Int;
    middleLoopNextScheduled: Int;
    
    // Inner loop
    innerLoopRunning: Bool;
    innerLoopLastRun: Int;
    innerLoopTotalBatches: Nat;
    innerLoopCurrentType: Text;
    innerLoopCurrentBatch: Nat;
    innerLoopStartTime: Int;
    innerLoopNextScheduled: Int;
  };

  // Three-tier timer management
  public class BatchImportTimer(
    logger: Logger.Logger,
    config: BatchImportConfig,
    isMasterAdmin: (Principal) -> Bool
  ) {
    
    //=========================================================================
    // RUNTIME STATE (automatically reset on upgrade)
    //=========================================================================
    
    // Timer IDs
    private var outerLoopTimerId : ?Nat = null;
    private var middleLoopTimerId : ?Nat = null; 
    private var innerLoopTimerId : ?Nat = null;
    
    // Running states
    private var outerLoopRunning : Bool = false;
    private var middleLoopRunning : Bool = false;
    private var innerLoopRunning : Bool = false;
    
    // Current state
    private var middleLoopCurrentState : Text = "Done";
    private var innerLoopCurrentType : Text = "None";
    private var innerLoopCurrentBatch : Nat = 0;
    
    // Cancellation flags
    private var middleLoopCancelled : Bool = false;
    private var innerLoopCancelled : Bool = false;
    
    // Configuration
    private var maxInnerLoopIterations : Nat = config.maxInnerLoopIterations;
    
    // Import functions (set by archive)
    private var importFunctions : {
      tradeImport: ?(() -> async {imported: Nat; failed: Nat});
      circuitBreakerImport: ?(() -> async {imported: Nat; failed: Nat});
      generalImport: ?(() -> async {imported: Nat; failed: Nat});
    } = {
      tradeImport = null;
      circuitBreakerImport = null;
      generalImport = null;
    };
    
    // Statistics/history - will be lost on upgrade but that's okay for statistics
    private var outerLoopLastRun : Int = 0;
    private var outerLoopTotalRuns : Nat = 0;
    private var middleLoopLastRun : Int = 0;
    private var middleLoopTotalRuns : Nat = 0;
    private var middleLoopStartTime : Int = 0;
    private var middleLoopNextScheduled : Int = 0;
    private var innerLoopLastRun : Int = 0;
    private var innerLoopTotalBatches : Nat = 0;
    private var innerLoopStartTime : Int = 0;
    private var innerLoopNextScheduled : Int = 0;
    
    //=========================================================================
    // CONFIGURATION METHODS
    //=========================================================================
    
    public func setImportFunctions(
      tradeImport: ?(() -> async {imported: Nat; failed: Nat}),
      circuitBreakerImport: ?(() -> async {imported: Nat; failed: Nat}),
      generalImport: ?(() -> async {imported: Nat; failed: Nat})
    ) {
      importFunctions := {
        tradeImport = tradeImport;
        circuitBreakerImport = circuitBreakerImport;
        generalImport = generalImport;
      };
    };
    
    public func setMaxInnerLoopIterations(caller: Principal, iterations: Nat) : Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      maxInnerLoopIterations := iterations;
      logger.info("TIMER_CONFIG", "Max inner loop iterations set to: " # Nat.toText(iterations), "setMaxInnerLoopIterations");
      #ok("Max inner loop iterations updated to: " # Nat.toText(iterations));
    };
    
    //=========================================================================
    // OUTER LOOP TIMER (Periodic trigger)
    //=========================================================================
    
    private func startOuterLoopTimer<system>() : async* () {
      if (outerLoopRunning) {
        return; // Already running
      };
      
      switch (outerLoopTimerId) {
        case (?id) { Timer.cancelTimer(id) };
        case null {};
      };

      outerLoopTimerId := ?Timer.setTimer<system>(
        #nanoseconds(config.outerLoopIntervalNS),
        func() : async () {
          await* runOuterLoop<system>();
          await* startOuterLoopTimer<system>(); // Reschedule
        }
      );
      
      outerLoopRunning := true;
      logger.info("OUTER_LOOP", "Outer loop timer started - Interval: " # Nat.toText(config.outerLoopIntervalNS / 1_000_000_000) # "s", "startOuterLoopTimer");
    };
    
    private func runOuterLoop<system>() : async* () {
      outerLoopLastRun := Time.now();
      outerLoopTotalRuns += 1;
      
      logger.info("OUTER_LOOP", "Outer loop triggered (run #" # Nat.toText(outerLoopTotalRuns) # ")", "runOuterLoop");
      
      // Only start middle loop if not already running
      if (not middleLoopRunning) {
        await* startMiddleLoop<system>();
      } else {
        logger.info("OUTER_LOOP", "Middle loop already running, skipping", "runOuterLoop");
      };
      
      // Reset running flag to allow rescheduling
      outerLoopRunning := false;
    };
    
    private func stopOuterLoopTimer() {
      switch (outerLoopTimerId) {
        case (?id) { 
          Timer.cancelTimer(id);
          outerLoopTimerId := null;
        };
        case null {};
      };
      
      outerLoopRunning := false;
      logger.info("OUTER_LOOP", "Outer loop timer stopped", "stopOuterLoopTimer");
    };
    
    //=========================================================================
    // MIDDLE LOOP TIMER (State machine coordinator)
    //=========================================================================
    
    private func startMiddleLoop<system>() : async* () {
      if (middleLoopRunning or middleLoopCancelled) {
        return;
      };
      
      middleLoopRunning := true;
      middleLoopCancelled := false;
      middleLoopStartTime := Time.now();
      middleLoopTotalRuns += 1;
      
      // Determine initial state based on available import functions
      middleLoopCurrentState := switch (importFunctions.tradeImport, importFunctions.generalImport) {
        case (?_, _) { "StartTradeImport" }; // Trading archive
        case (null, ?_) { "StartImport" }; // Single-type archive
        case _ { "Done" }; // No import functions available
      };
      
      logger.info("MIDDLE_LOOP", "Middle loop started (run #" # Nat.toText(middleLoopTotalRuns) # ") - Initial state: " # middleLoopCurrentState, "startMiddleLoop");
      
      await* scheduleMiddleLoop<system>(0); // Start immediately
    };
    
    private func scheduleMiddleLoop<system>(delayNS: Nat) : async* () {
      if (middleLoopCancelled) {
        middleLoopRunning := false;
        middleLoopCurrentState := "Done";
        logger.info("MIDDLE_LOOP", "Middle loop cancelled", "scheduleMiddleLoop");
        return;
      };
      
      middleLoopNextScheduled := Time.now() + Int.abs(delayNS);
      
      middleLoopTimerId := ?Timer.setTimer<system>(
        #nanoseconds(delayNS),
        func() : async () {
          await* runMiddleLoop<system>();
        }
      );
    };
    
    private func runMiddleLoop<system>() : async* () {
      middleLoopLastRun := Time.now();
      
      logger.info("MIDDLE_LOOP", "Middle loop execution - State: " # middleLoopCurrentState, "runMiddleLoop");
      
      switch (middleLoopCurrentState) {
        // Trading archive states
        case ("StartTradeImport") {
          await* startInnerLoop<system>("Trades");
          middleLoopCurrentState := "MonitoringTradeImport";
          await* scheduleMiddleLoop<system>(config.middleLoopDelayNS);
        };
        case ("MonitoringTradeImport") {
          if (not innerLoopRunning) {
            middleLoopCurrentState := switch (importFunctions.circuitBreakerImport) {
              case (?_) { "StartCircuitBreakerImport" };
              case null { "Done" };
            };
            // If we're done completely, set middleLoopRunning to false
            if (middleLoopCurrentState == "Done") {
              middleLoopRunning := false;
            };
            await* scheduleMiddleLoop<system>(0); // Continue immediately
          } else {
            await* scheduleMiddleLoop<system>(config.middleLoopDelayNS);
          };
        };
        case ("StartCircuitBreakerImport") {
          await* startInnerLoop<system>("CircuitBreakers");
          middleLoopCurrentState := "MonitoringCircuitBreakerImport";
          await* scheduleMiddleLoop<system>(config.middleLoopDelayNS);
        };
        case ("MonitoringCircuitBreakerImport") {
          if (not innerLoopRunning) {
            middleLoopCurrentState := "Done";
            middleLoopRunning := false;
          } else {
            await* scheduleMiddleLoop<system>(config.middleLoopDelayNS);
          };
        };
        
        // Single-type archive states
        case ("StartImport") {
          await* startInnerLoop<system>("General");
          middleLoopCurrentState := "MonitoringImport";
          await* scheduleMiddleLoop<system>(config.middleLoopDelayNS);
        };
        case ("MonitoringImport") {
          if (not innerLoopRunning) {
            middleLoopCurrentState := "Done";
            middleLoopRunning := false;
          } else {
            await* scheduleMiddleLoop<system>(config.middleLoopDelayNS);
          };
        };
        
        case ("Done") {
          middleLoopRunning := false;
          logger.info("MIDDLE_LOOP", "Middle loop completed", "runMiddleLoop");
        };
        
        case (_) {
          // Unknown state, reset to done
          middleLoopCurrentState := "Done";
          middleLoopRunning := false;
          logger.error("MIDDLE_LOOP", "Unknown middle loop state, resetting to Done", "runMiddleLoop");
        };
      };
    };
    
    //=========================================================================
    // INNER LOOP TIMER (Data import worker)
    //=========================================================================
    
    private func startInnerLoop<system>(importType: Text) : async* () {
      if (innerLoopRunning or innerLoopCancelled) {
        return;
      };
      
      innerLoopRunning := true;
      innerLoopCancelled := false;
      innerLoopCurrentType := importType;
      innerLoopStartTime := Time.now();
      innerLoopCurrentBatch := 0;
      
      logger.info("INNER_LOOP", "Inner loop started - Type: " # importType, "startInnerLoop");
      
      await* scheduleInnerLoop<system>(0); // Start immediately
    };
    
    private func scheduleInnerLoop<system>(delayNS: Nat) : async* () {
      if (innerLoopCancelled) {
        innerLoopRunning := false;
        innerLoopCurrentType := "None";
        logger.info("INNER_LOOP", "Inner loop cancelled", "scheduleInnerLoop");
        return;
      };
      
      innerLoopNextScheduled := Time.now() + Int.abs(delayNS);
      
      innerLoopTimerId := ?Timer.setTimer<system>(
        #nanoseconds(delayNS),
        func() : async () {
          await* runInnerLoop<system>();
        }
      );
    };
    
    private func runInnerLoop<system>() : async* () {
      innerLoopLastRun := Time.now();
      innerLoopCurrentBatch += 1;
      
      logger.info("INNER_LOOP", "Inner loop execution - Type: " # innerLoopCurrentType # " Batch: " # Nat.toText(innerLoopCurrentBatch), "runInnerLoop");
      
      // Select appropriate import function
      let importFunction = switch (innerLoopCurrentType) {
        case ("Trades") { importFunctions.tradeImport };
        case ("CircuitBreakers") { importFunctions.circuitBreakerImport };
        case ("General") { importFunctions.generalImport };
        case (_) { null };
      };
      
      switch (importFunction) {
        case (?fn) {
          try {
            let result = await fn();
            innerLoopTotalBatches += 1;
            
            logger.info("INNER_LOOP", "Batch completed - Imported: " # Nat.toText(result.imported) # " Failed: " # Nat.toText(result.failed), "runInnerLoop");
            
            // Continue if we haven't reached max iterations and there's still data
            if (innerLoopCurrentBatch < maxInnerLoopIterations and result.imported > 0) {
              await* scheduleInnerLoop<system>(config.innerLoopDelayNS);
            } else {
              // Finished - either reached max iterations or no more data
              innerLoopRunning := false;
              innerLoopCurrentType := "None";
              
              let reason = if (innerLoopCurrentBatch >= maxInnerLoopIterations) {
                "reached max iterations (" # Nat.toText(maxInnerLoopIterations) # ")"
              } else {
                "no more data to import"
              };
              
              logger.info("INNER_LOOP", "Inner loop completed - " # reason, "runInnerLoop");
            };
          } catch (e) {
            logger.error("INNER_LOOP", "Import function failed: " # Error.message(e), "runInnerLoop");
            // Continue on error, but with delay
            if (innerLoopCurrentBatch < maxInnerLoopIterations) {
              await* scheduleInnerLoop<system>(config.innerLoopDelayNS);
      } else {
              innerLoopRunning := false;
              innerLoopCurrentType := "None";
            };
          };
        };
        case null {
          logger.error("INNER_LOOP", "No import function available for type: " # innerLoopCurrentType, "runInnerLoop");
          innerLoopRunning := false;
          innerLoopCurrentType := "None";
        };
      };
    };
    
    //=========================================================================
    // PUBLIC ADMIN INTERFACE
    //=========================================================================
    
    public func adminStart<system>(caller: Principal, importFunction: () -> async ()) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      // For backwards compatibility, convert single import function to general import
      let compatFunction = func() : async {imported: Nat; failed: Nat} {
        await importFunction();
        {imported = 1; failed = 0}; // Assume success for compatibility
      };
      
      setImportFunctions(null, null, ?compatFunction);
      await* startOuterLoopTimer<system>();
      
      #ok("Batch import system started")
    };

    public func adminStartAdvanced<system>(
      caller: Principal,
      tradeImport: ?(() -> async {imported: Nat; failed: Nat}),
      circuitBreakerImport: ?(() -> async {imported: Nat; failed: Nat}),
      generalImport: ?(() -> async {imported: Nat; failed: Nat})
    ) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      setImportFunctions(tradeImport, circuitBreakerImport, generalImport);
      await* startOuterLoopTimer<system>();
      
      #ok("Advanced batch import system started")
    };
    
    public func adminStop(caller: Principal) : Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      stopOuterLoopTimer();
        #ok("Batch import system stopped")
    };
    
    public func adminStopAll(caller: Principal) : Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      stopOuterLoopTimer();
      middleLoopCancelled := true;
      innerLoopCancelled := true;
      
      #ok("All timers stopped and cancelled")
    };
    
    public func adminManualImport(caller: Principal, importFunction: () -> async ()) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      if (middleLoopRunning) {
        return #err("Middle loop already running. Use adminStopAll() first if needed.");
      };
      
      // For backwards compatibility, convert single import function
      let compatFunction = func() : async {imported: Nat; failed: Nat} {
      await importFunction();
        {imported = 1; failed = 0};
      };
      
      setImportFunctions(null, null, ?compatFunction);
      await* startMiddleLoop<system>();
      
      #ok("Manual batch import started")
    };
    
    public func adminManualImportAdvanced<system>(
      caller: Principal,
      tradeImport: ?(() -> async {imported: Nat; failed: Nat}),
      circuitBreakerImport: ?(() -> async {imported: Nat; failed: Nat}),
      generalImport: ?(() -> async {imported: Nat; failed: Nat})
    ) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      if (middleLoopRunning) {
        return #err("Middle loop already running. Use adminStopAll() first if needed.");
      };
      
      setImportFunctions(tradeImport, circuitBreakerImport, generalImport);
      await* startMiddleLoop<system>();
      
      #ok("Advanced manual batch import started")
    };
    
    //=========================================================================
    // STATUS AND MONITORING
    //=========================================================================
    
    public func getTimerStatus() : TimerStatus {
      {
        outerLoopRunning = outerLoopRunning;
        outerLoopLastRun = outerLoopLastRun;
        outerLoopTotalRuns = outerLoopTotalRuns;
        outerLoopIntervalSeconds = config.outerLoopIntervalNS / 1_000_000_000;
        
        middleLoopRunning = middleLoopRunning;
        middleLoopLastRun = middleLoopLastRun;
        middleLoopTotalRuns = middleLoopTotalRuns;
        middleLoopCurrentState = middleLoopCurrentState;
        middleLoopStartTime = middleLoopStartTime;
        middleLoopNextScheduled = middleLoopNextScheduled;
        
        innerLoopRunning = innerLoopRunning;
        innerLoopLastRun = innerLoopLastRun;
        innerLoopTotalBatches = innerLoopTotalBatches;
        innerLoopCurrentType = innerLoopCurrentType;
        innerLoopCurrentBatch = innerLoopCurrentBatch;
        innerLoopStartTime = innerLoopStartTime;
        innerLoopNextScheduled = innerLoopNextScheduled;
      }
    };
    
    // Legacy compatibility methods
    public func isRunning() : Bool {
      outerLoopRunning
    };
    
    public func getIntervalSeconds() : Nat {
      config.outerLoopIntervalNS / 1_000_000_000
    };
    
    //=========================================================================
    // LIFECYCLE METHODS
    //=========================================================================
    
    public func preupgrade() {
      // Cancel all timers
      switch (outerLoopTimerId) {
        case (?id) { Timer.cancelTimer(id) };
        case null {};
      };
      switch (middleLoopTimerId) {
        case (?id) { Timer.cancelTimer(id) };
        case null {};
      };
      switch (innerLoopTimerId) {
        case (?id) { Timer.cancelTimer(id) };
        case null {};
      };
      
      logger.info("TIMER_LIFECYCLE", "All timers cancelled for upgrade", "preupgrade");
    };

    public func postupgrade<system>(importFunction: () -> async ()) {
      // Reset state variables to "not running" states
      middleLoopCurrentState := "Done";
      innerLoopCurrentType := "None";
      
      logger.info("TIMER_LIFECYCLE", "Timer system reset after upgrade", "postupgrade");
      
      // For backwards compatibility, restart outer loop with delay if import function provided
      if (outerLoopTotalRuns > 0) { // Was running before upgrade
        let compatFunction = func() : async {imported: Nat; failed: Nat} {
          await importFunction();
          {imported = 1; failed = 0};
        };
        
        setImportFunctions(null, null, ?compatFunction);
        
      ignore Timer.setTimer<system>(
        #nanoseconds(10_000_000_000), // 10 seconds delay
        func() : async () {
            await* startOuterLoopTimer<system>();
        }
      );
    };
    };

    //=========================================================================
    // LEGACY COMPATIBILITY (maintained for existing code)
    //=========================================================================
    
    public func runCatchUpImport(
      caller: Principal,
      catchUpFunction: () -> async {imported: Nat; failed: Nat}
    ) : async Result.Result<Text, Text> {
      if (not isMasterAdmin(caller)) {
        return #err("Not authorized");
      };
      
      logger.info("CATCH_UP", "Starting catch-up import", "runCatchUpImport");
      
      var totalImported = 0;
      var totalFailed = 0;
      var batchCount = 0;
      
      // Run multiple import batches up to limit
      label exit_loop while (batchCount < maxInnerLoopIterations) {
        let result = await catchUpFunction();
        
        totalImported += result.imported;
        totalFailed += result.failed;
        batchCount += 1;
        
        // If no new data imported, we're caught up
        if (result.imported == 0) {
          break exit_loop;
        };
      };
      
      logger.info("CATCH_UP", "Catch-up import completed - Batches: " # Nat.toText(batchCount) # " Imported: " # Nat.toText(totalImported) # " Failed: " # Nat.toText(totalFailed), "runCatchUpImport");
      
      #ok("Catch-up import completed: " # Nat.toText(totalImported) # " records imported, " # Nat.toText(totalFailed) # " failed")
    };
  };
} 