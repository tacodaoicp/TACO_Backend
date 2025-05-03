import Time "mo:base/Time";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Text "mo:base/Text";
import Vector "mo:vector";
import Iter "mo:base/Iter";
import Debug "mo:base/Debug";
import Map "mo:map/Map";
import Array "mo:base/Array";

module {
  // Log levels for filtering
  public type LogLevel = {
    #INFO;
    #WARN;
    #ERROR;
  };

  // Log entry structure
  public type LogEntry = {
    timestamp : Int;
    level : LogLevel;
    component : Text;
    message : Text;
    context : Text;
  };

  // Log storage using Vector for efficient push/pop operations
  public class Logger() {
    // Store logs in a circular vector with fixed size
    private let MAX_LOGS = 50000;
    private let RETENTION_BUFFER = 10000; // How many logs to trim when max is reached
    private let CONTEXT_MAX_LOGS = 2000;
    private let logs = Vector.new<LogEntry>();

    // Context-specific log storage
    private let contextLogs = Map.new<Text, Vector.Vector<LogEntry>>();

    // Helper to format log level as text
    private func levelToText(level : LogLevel) : Text {
      switch (level) {
        case (#INFO) { "INFO" };
        case (#WARN) { "WARN" };
        case (#ERROR) { "ERROR" };
      };
    };

    // Create and store a log entry
    private func createLogEntry(level : LogLevel, component : Text, message : Text, context : Text) : LogEntry {
      let entry = {
        timestamp = Time.now();
        level;
        component;
        message;
        context;
      };

      // Add to main log vector
      Vector.add(logs, entry);

      // More efficient trimming - only when needed and in batch
      if (Vector.size(logs) > MAX_LOGS) {
        // Remove oldest entries in one operation
        let logsSize = Vector.size(logs);
        let startIndex = RETENTION_BUFFER;
        let keepCount = logsSize - RETENTION_BUFFER;

        // Convert to array, get subarray of newest logs, and create new vector
        let allLogs = Vector.toArray(logs);
        let newLogs = Vector.fromArray<LogEntry>(
          Array.subArray(allLogs, startIndex, keepCount)
        );

        // Replace the logs vector
        Vector.clear(logs);
        for (entry in Vector.vals(newLogs)) {
          Vector.add(logs, entry);
        };
      };

      // Add to context-specific log vector
      switch (Map.get(contextLogs, Map.thash, context)) {
        case (null) {
          let contextVector = Vector.new<LogEntry>();
          Vector.add(contextVector, entry);
          Map.set(contextLogs, Map.thash, context, contextVector);
        };
        case (?vector) {
          Vector.add(vector, entry);

          // More efficient context logs trimming
          if (Vector.size(vector) > CONTEXT_MAX_LOGS) {
            let vectorSize = Vector.size(vector);
            let retainCount = CONTEXT_MAX_LOGS / 2; // Remove half when threshold reached

            // Convert vector to array once
            let allLogs = Vector.toArray(vector);

            // Create a new vector directly from the subarray of recent logs
            let startIndex = vectorSize - retainCount;
            let newContextLogs = Vector.fromArray<LogEntry>(
              Array.subArray(allLogs, startIndex, retainCount)
            );

            // Replace the old vector directly
            Map.set(contextLogs, Map.thash, context, newContextLogs);
          };
        };
      };

      // Print log to console
      Debug.print("[" # levelToText(level) # "] [" # Int.toText(entry.timestamp) # "] [" # component # "] " # message # " | Context: " # context);
      entry;
    };

    // Public logging methods
    public func info(component : Text, message : Text, context : Text) {
      ignore createLogEntry(#INFO, component, message, context);
    };

    public func warn(component : Text, message : Text, context : Text) {
      ignore createLogEntry(#WARN, component, message, context);
    };

    public func error(component : Text, message : Text, context : Text) {
      ignore createLogEntry(#ERROR, component, message, context);
    };

    // Add multiple log entries at once - more efficiently
    public func addEntries(entries : [(LogLevel, Text, Text, Text)]) {
      for ((level, component, message, context) in entries.vals()) {
        ignore createLogEntry(level, component, message, context);
      };
    };

    // Get the last N logs - optimized with subArray
    public func getLastLogs(last : Nat) : [LogEntry] {
      let size = Vector.size(logs);
      if (size == 0 or last == 0) {
        return [];
      };

      let count = Nat.min(last, size);
      let startIdx = size - count;

      // Convert to array and use subArray for efficient slicing
      let allLogs = Vector.toArray(logs);
      return Array.subArray(allLogs, startIdx, count);
    };

    // Get the last N logs for a specific context - optimized with subArray
    public func getContextLogs(context : Text, last : Nat) : [LogEntry] {
      switch (Map.get(contextLogs, Map.thash, context)) {
        case (null) { [] };
        case (?vector) {
          let size = Vector.size(vector);
          if (size == 0 or last == 0) {
            return [];
          };

          let count = Nat.min(last, size);
          let startIdx = size - count;

          // Convert to array and use subArray for efficient slicing
          let allContextLogs = Vector.toArray(vector);
          return Array.subArray(allContextLogs, startIdx, count);
        };
      };
    };

    // Get all available contexts
    public func getContexts() : [Text] {
      Iter.toArray(Map.keys(contextLogs));
    };

    // Filter logs by level - more efficient with Array.filter and subArray
    public func getLogsByLevel(level : LogLevel, last : Nat) : [LogEntry] {
      if (last == 0) {
        return [];
      };

      // Convert to array once
      let allLogs = Vector.toArray(logs);

      // Use Array.filter for more efficient filtering
      let filtered = Array.filter<LogEntry>(
        allLogs,
        func(entry : LogEntry) : Bool {
          entry.level == level;
        },
      );

      let filteredSize = filtered.size();
      if (filteredSize == 0) {
        return [];
      };

      // Get the last N entries from filtered results using subArray
      let count = Nat.min(last, filteredSize);
      let startIdx = filteredSize - count;

      return Array.subArray(filtered, startIdx, count);
    };

    // Clear logs
    public func clearLogs() {
      Vector.clear(logs);
      Map.clear(contextLogs);
    };

    // Clear context logs
    public func clearContextLogs(context : Text) {
      if (context == "all") {
        Map.clear(contextLogs);
      } else {
        switch (Map.get(contextLogs, Map.thash, context)) {
          case (null) {};
          case (?vector) {
            Vector.clear(vector);
          };
        };
      };
    };
  };
};
