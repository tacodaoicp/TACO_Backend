import Array "mo:base/Array";

module {
  //==========================================================================
  // SHARED TYPES (unchanged between old and new)
  //==========================================================================

  type CircuitBreakerAction = {
    #PauseMint;
    #PauseBurn;
    #PauseBoth;
    #RejectOperation;
  };

  //==========================================================================
  // OLD TYPES (CircuitBreakerConditionType WITHOUT #TokenPaused)
  //==========================================================================

  type OldCircuitBreakerConditionType = {
    #NavDrop;
    #PriceChange;
    #BalanceChange;
    #DecimalChange;
  };

  type OldCircuitBreakerAlert = {
    id : Nat;
    conditionId : Nat;
    conditionType : OldCircuitBreakerConditionType;
    token : ?Principal;
    tokenSymbol : Text;
    timestamp : Int;
    actionTaken : CircuitBreakerAction;
    details : Text;
  };

  type OldCircuitBreakerCondition = {
    id : Nat;
    conditionType : OldCircuitBreakerConditionType;
    thresholdPercent : Float;
    timeWindowNS : Nat;
    direction : { #Up; #Down; #Both };
    action : CircuitBreakerAction;
    applicableTokens : [Principal];
    enabled : Bool;
    createdAt : Int;
    createdBy : Principal;
  };

  //==========================================================================
  // NEW TYPES (CircuitBreakerConditionType WITH #TokenPaused)
  //==========================================================================

  type NewCircuitBreakerConditionType = {
    #NavDrop;
    #PriceChange;
    #BalanceChange;
    #DecimalChange;
    #TokenPaused;
  };

  type NewCircuitBreakerAlert = {
    id : Nat;
    conditionId : Nat;
    conditionType : NewCircuitBreakerConditionType;
    token : ?Principal;
    tokenSymbol : Text;
    timestamp : Int;
    actionTaken : CircuitBreakerAction;
    details : Text;
  };

  type NewCircuitBreakerCondition = {
    id : Nat;
    conditionType : NewCircuitBreakerConditionType;
    thresholdPercent : Float;
    timeWindowNS : Nat;
    direction : { #Up; #Down; #Both };
    action : CircuitBreakerAction;
    applicableTokens : [Principal];
    enabled : Bool;
    createdAt : Int;
    createdBy : Principal;
  };

  //==========================================================================
  // INTERNAL STRUCTURES
  //==========================================================================

  // Vector<CircuitBreakerAlert> internal representation
  type OldAlertVector = {
    var data_blocks : [var [var ?OldCircuitBreakerAlert]];
    var i_block : Nat;
    var i_element : Nat;
  };

  type NewAlertVector = {
    var data_blocks : [var [var ?NewCircuitBreakerAlert]];
    var i_block : Nat;
    var i_element : Nat;
  };

  // Map<Nat, CircuitBreakerCondition> internal representation
  type OldConditionMapBucket = (
    [var ?Nat],
    [var ?OldCircuitBreakerCondition],
    [var Nat],
    [var Nat32],
  );

  type NewConditionMapBucket = (
    [var ?Nat],
    [var ?NewCircuitBreakerCondition],
    [var Nat],
    [var Nat32],
  );

  //==========================================================================
  // STATE TYPES
  //==========================================================================

  public type OldState = {
    circuitBreakerAlerts : OldAlertVector;
    circuitBreakerConditions : [var ?OldConditionMapBucket];
  };

  public type NewState = {
    circuitBreakerAlerts : NewAlertVector;
    circuitBreakerConditions : [var ?NewConditionMapBucket];
  };

  //==========================================================================
  // MIGRATION HELPERS
  //==========================================================================

  func migrateConditionType(old : OldCircuitBreakerConditionType) : NewCircuitBreakerConditionType {
    switch (old) {
      case (#NavDrop) { #NavDrop };
      case (#PriceChange) { #PriceChange };
      case (#BalanceChange) { #BalanceChange };
      case (#DecimalChange) { #DecimalChange };
    };
  };

  func migrateAlert(old : OldCircuitBreakerAlert) : NewCircuitBreakerAlert {
    {
      id = old.id;
      conditionId = old.conditionId;
      conditionType = migrateConditionType(old.conditionType);
      token = old.token;
      tokenSymbol = old.tokenSymbol;
      timestamp = old.timestamp;
      actionTaken = old.actionTaken;
      details = old.details;
    };
  };

  func migrateCondition(old : OldCircuitBreakerCondition) : NewCircuitBreakerCondition {
    {
      id = old.id;
      conditionType = migrateConditionType(old.conditionType);
      thresholdPercent = old.thresholdPercent;
      timeWindowNS = old.timeWindowNS;
      direction = old.direction;
      action = old.action;
      applicableTokens = old.applicableTokens;
      enabled = old.enabled;
      createdAt = old.createdAt;
      createdBy = old.createdBy;
    };
  };

  //==========================================================================
  // MAIN MIGRATION FUNCTION
  //==========================================================================

  public func migrate(old : OldState) : NewState {
    // Migrate circuitBreakerAlerts (Vector internals)
    let oldBlocks = old.circuitBreakerAlerts.data_blocks;
    let newBlocks : [var [var ?NewCircuitBreakerAlert]] = Array.init(oldBlocks.size(), [var] : [var ?NewCircuitBreakerAlert]);

    for (i in oldBlocks.keys()) {
      let oldBlock = oldBlocks[i];
      let newBlock : [var ?NewCircuitBreakerAlert] = Array.init(oldBlock.size(), null);
      for (j in oldBlock.keys()) {
        newBlock[j] := switch (oldBlock[j]) {
          case (?alert) { ?migrateAlert(alert) };
          case null { null };
        };
      };
      newBlocks[i] := newBlock;
    };

    // Migrate circuitBreakerConditions (Map internals)
    let oldMap = old.circuitBreakerConditions;
    let newMap : [var ?NewConditionMapBucket] = Array.init(oldMap.size(), null);

    for (i in oldMap.keys()) {
      switch (oldMap[i]) {
        case (?bucket) {
          let oldKeys = bucket.0;
          let oldValues = bucket.1;
          let indexes = bucket.2;
          let bounds = bucket.3;

          let newValues : [var ?NewCircuitBreakerCondition] = Array.init(oldValues.size(), null);
          for (j in oldValues.keys()) {
            newValues[j] := switch (oldValues[j]) {
              case (?cond) { ?migrateCondition(cond) };
              case null { null };
            };
          };

          newMap[i] := ?(oldKeys, newValues, indexes, bounds);
        };
        case null {
          newMap[i] := null;
        };
      };
    };

    {
      circuitBreakerAlerts = {
        var data_blocks = newBlocks;
        var i_block = old.circuitBreakerAlerts.i_block;
        var i_element = old.circuitBreakerAlerts.i_element;
      };
      circuitBreakerConditions = newMap;
    };
  };
};
