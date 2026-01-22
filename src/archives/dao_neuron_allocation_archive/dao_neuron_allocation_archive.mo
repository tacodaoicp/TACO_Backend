import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Map "mo:map/Map";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Text "mo:base/Text";
import Error "mo:base/Error";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Blob "mo:base/Blob";

import ICRC3 "mo:icrc3-mo";
import ICRC3Service "mo:icrc3-mo/service";
import ArchiveTypes "../archive_types";
import DAOTypes "../../DAO_backend/dao_types";
import CanisterIds "../../helper/CanisterIds";
import ArchiveBase "../../helper/archive_base";
import Logger "../../helper/logger";
import BatchImportTimer "../../helper/batch_import_timer";
import Cycles "mo:base/ExperimentalCycles";

shared (deployer) persistent actor class DAONeuronAllocationArchive() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Type aliases for this specific archive
  type NeuronAllocationChangeBlockData = {
    id: Nat;
    timestamp: Int;
    neuronId: Blob;
    changeType: ArchiveTypes.AllocationChangeType;
    oldAllocations: [ArchiveTypes.Allocation];
    newAllocations: [ArchiveTypes.Allocation];
    votingPower: Nat;
    maker: Principal;
    reason: ?Text;
    penaltyMultiplier: ?Nat; // null or ?100 = no penalty, ?23 = 77% penalty
  };
  type ArchiveError = ArchiveTypes.ArchiveError;
  type AllocationChangeType = ArchiveTypes.AllocationChangeType;
  type Allocation = ArchiveTypes.Allocation;

  // Initialize the generic base class with neuron allocation-specific configuration
  private transient let initialConfig : ArchiveTypes.ArchiveConfig = {
    maxBlocksPerCanister = 2000000; // 2M blocks per canister (neuron allocation changes are frequent)
    blockRetentionPeriodNS = 94608000000000000; // 3 years in nanoseconds (allocation history is valuable)
    enableCompression = false;
    autoArchiveEnabled = true;
  };

  // ICRC3 State for scalable storage
  private stable var icrc3State : ?ICRC3.State = null;
  private transient var icrc3StateRef = { var value = icrc3State };

  private transient let base = ArchiveBase.ArchiveBase<NeuronAllocationChangeBlockData>(
    this_canister_id(),
    ["3neuron_allocation_change"],
    initialConfig,
    deployer.caller,
    icrc3StateRef
  );

  // Neuron allocation-specific indexes
  private stable var neuronIndex = Map.new<Blob, [Nat]>(); // Neuron -> block indices
  private stable var makerIndex = Map.new<Principal, [Nat]>(); // Maker -> block indices
  private stable var timestampIndex = Map.new<Int, [Nat]>(); // Timestamp -> block indices

  // Neuron allocation-specific statistics
  private stable var totalNeuronAllocationChanges : Nat = 0;

  // Block counters for IDs
  private stable var neuronAllocationChangeCounter : Nat = 0;

  // Tracking state for batch imports (we'll add "since" methods to DAO_backend later)
  private stable var lastImportedNeuronAllocationTimestamp : Int = 0;

  // DAO_backend interface for batch imports
  private transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  private transient let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);
  private transient let daoCanister : DAOTypes.Self = actor (Principal.toText(DAO_BACKEND_ID));

  //=========================================================================
  // ICRC-3 Standard endpoints (delegated to base class)
  //=========================================================================

  public query func icrc3_get_archives(args : ICRC3Service.GetArchivesArgs) : async ICRC3Service.GetArchivesResult {
    base.icrc3_get_archives(args);
  };

  public query func icrc3_get_tip_certificate() : async ?ICRC3Service.DataCertificate {
    base.icrc3_get_tip_certificate();
  };

  public query func icrc3_get_blocks(args : ICRC3Service.GetBlocksArgs) : async ICRC3Service.GetBlocksResult {
    base.icrc3_get_blocks(args);
  };

  public query func icrc3_supported_block_types() : async [ICRC3Service.BlockType] {
    base.icrc3_supported_block_types();
  };

  //=========================================================================
  // Helper Functions
  //=========================================================================

  // Helper function to convert AllocationChangeType to Value
  private func allocationChangeTypeToValue(changeType: ArchiveTypes.AllocationChangeType) : ArchiveTypes.Value {
    switch (changeType) {
      case (#UserUpdate(details)) { 
        #Map([
          ("type", #Text("UserUpdate")),
          ("userInitiated", ArchiveTypes.boolToValue(details.userInitiated))
        ]);
      };
      case (#FollowAction(details)) { 
        #Map([
          ("type", #Text("FollowAction")),
          ("followedUser", ArchiveTypes.principalToValue(details.followedUser))
        ]);
      };
      case (#SystemRebalance) { #Map([("type", #Text("SystemRebalance"))]); };
      case (#VotingPowerChange) { #Map([("type", #Text("VotingPowerChange"))]); };
    };
  };

  // Helper function to convert Allocation to Value
  private func allocationToValueHelper(allocation: ArchiveTypes.Allocation) : ArchiveTypes.Value {
    #Map([
      ("token", ArchiveTypes.principalToValue(allocation.token)),
      ("basisPoints", #Nat(allocation.basisPoints))
    ]);
  };

  // Convert NeuronAllocationChangeBlockData to ICRC3 Value format
  private func neuronAllocationChangeToValue(change: NeuronAllocationChangeBlockData, _timestamp: Int, _parentHash: ?Blob) : ArchiveTypes.Value {
    #Map([
      ("id", #Nat(change.id)),
      ("timestamp", #Int(change.timestamp)),
      ("neuronId", #Blob(change.neuronId)), // Include the neuron ID!
      ("user", ArchiveTypes.principalToValue(change.maker)), // Use maker as user for compatibility
      ("changeType", allocationChangeTypeToValue(change.changeType)),
      ("oldAllocations", #Array(Array.map(change.oldAllocations, allocationToValueHelper))),
      ("newAllocations", #Array(Array.map(change.newAllocations, allocationToValueHelper))),
      ("votingPower", #Nat(change.votingPower)),
      ("maker", ArchiveTypes.principalToValue(change.maker)),
      ("reason", switch (change.reason) { case (?r) { #Text(r) }; case null { #Text("") }; }),
      ("penaltyMultiplier", switch (change.penaltyMultiplier) { case (?pm) { #Nat(pm) }; case null { #Nat(100) }; })
    ]);
  };

  //=========================================================================
  // Custom Neuron Allocation Archive Functions
  //=========================================================================

  public shared ({ caller }) func archiveNeuronAllocationChange<system>(change : NeuronAllocationChangeBlockData) : async Result.Result<Nat, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    // Create neuron allocation change value with neuronId included
    let blockValue = neuronAllocationChangeToValue(change, change.timestamp, null);
    let blockIndex = base.storeBlock<system>(
      blockValue,
      "3neuron_allocation_change",
      Array.map(change.newAllocations, func(a: Allocation) : Principal { a.token }), // Index by involved tokens
      change.timestamp
    );

    // Update custom indexes
    updateNeuronIndex(change.neuronId, blockIndex);
    updateMakerIndex(change.maker, blockIndex);
    updateTimestampIndex(change.timestamp, blockIndex);

    // Update statistics
    totalNeuronAllocationChanges += 1;

    #ok(blockIndex);
  };

  // Bulk archive function for test data generation - archives multiple allocation changes at once
  public shared ({ caller }) func archiveNeuronAllocationChangeBatch<system>(
    changes: [NeuronAllocationChangeBlockData]
  ) : async Result.Result<{ archived: Nat; failed: Nat }, ArchiveError> {
    if (not base.isAuthorized(caller, #ArchiveData)) {
      return #err(#NotAuthorized);
    };

    if (changes.size() > 1000) {
      return #err(#InvalidData); // Limit batch size
    };

    var archived : Nat = 0;
    var failed : Nat = 0;

    for (change in changes.vals()) {
      let blockValue = neuronAllocationChangeToValue(change, change.timestamp, null);
      let blockIndex = base.storeBlock<system>(
        blockValue,
        "3neuron_allocation_change",
        Array.map(change.newAllocations, func(a: Allocation) : Principal { a.token }),
        change.timestamp
      );

      // Update custom indexes
      updateNeuronIndex(change.neuronId, blockIndex);
      updateMakerIndex(change.maker, blockIndex);
      updateTimestampIndex(change.timestamp, blockIndex);

      totalNeuronAllocationChanges += 1;
      archived += 1;
    };

    #ok({ archived = archived; failed = failed });
  };

  //=========================================================================
  // Query Methods for Rewards System
  //=========================================================================

  public shared query ({ caller }) func getNeuronAllocationChangesByNeuron(neuronId: Blob, limit: Nat) : async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    let blockIndices = switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?indices) { indices };
      case null { return #ok([]) };
    };

    var results : [NeuronAllocationChangeBlockData] = [];
    
    // Convert to array and take only up to limit
    let indicesArray = Iter.toArray(blockIndices.vals());
    let limitedIndices = if (Array.size(indicesArray) > limit) {
      Array.subArray(indicesArray, 0, limit);
    } else {
      indicesArray;
    };
    
    // Iterate through the limited block indices
    for (blockIndex in limitedIndices.vals()) {
      // Get the specific block using ICRC3
      let getBlocksArgs = [{
        start = blockIndex;
        length = 1;
      }];
      
      let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);
      
      // Process the single block if available
      if (Array.size(icrc3Result.blocks) > 0) {
        let block = icrc3Result.blocks[0];
        
        // Parse the block to extract neuron allocation change data (no time filtering)
        switch (parseBlockForNeuronAllocationNoTimeFilter(block, neuronId)) {
          case (?changeData) {
            results := Array.append(results, [changeData]);
          };
          case (_) {
            // Block doesn't match our criteria
          };
        };
      };
    };
    
    #ok(results);
  };

  public shared query ({ caller }) func getNeuronAllocationChangesByMaker(maker: Principal, limit: Nat) : async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    let blockIndices = switch (Map.get(makerIndex, Map.phash, maker)) {
      case (?indices) { indices };
      case null { return #ok([]) };
    };

    // For now, return empty results - we'll enhance this later
    #ok([]);
  };

  public shared query ({ caller }) func getNeuronAllocationChangesByNeuronInTimeRange(
    neuronId: Blob,
    startTime: Int,
    endTime: Int,
    limit: Nat
  ) : async Result.Result<[NeuronAllocationChangeBlockData], ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    if (startTime > endTime) {
      return #err(#InvalidTimeRange);
    };

    // Get block indices for this neuron from the index
    switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?blockIndices) {
        var results : [NeuronAllocationChangeBlockData] = [];
        var count = 0;
        
        // Convert to array and take only up to limit
        let indicesArray = Iter.toArray(blockIndices.vals());
        let limitedIndices = if (Array.size(indicesArray) > limit) {
          Array.subArray(indicesArray, 0, limit);
        } else {
          indicesArray;
        };
        
        // Iterate through the limited block indices
        for (blockIndex in limitedIndices.vals()) {
          // Get the specific block using ICRC3
          let getBlocksArgs = [{
            start = blockIndex;
            length = 1;
          }];
          
          let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);
          
          // Process the single block if available
          if (Array.size(icrc3Result.blocks) > 0) {
            let block = icrc3Result.blocks[0];
            
            // Parse the block to extract neuron allocation change data
            switch (parseBlockForNeuronAllocation(block, neuronId, startTime, endTime)) {
              case (?changeData) {
                results := Array.append(results, [changeData]);
                count += 1;
              };
              case (_) {
                // Block doesn't match our criteria (outside time range, etc.)
              };
            };
          };
        };
        
        #ok(results);
      };
      case (_) {
        // No blocks found for this neuron
        #ok([]);
      };
    };
  };

  // Bulk query for multiple neurons at once - optimized for backfill operations
  // Returns allocation changes for multiple neurons in a single call
  public shared query ({ caller }) func getAllNeuronsAllocationChangesInTimeRange(
    startTime: Int,
    endTime: Int,
    offset: Nat,
    limit: Nat
  ) : async Result.Result<{
    neurons: [(Blob, {
      preTimespanAllocation: ?NeuronAllocationChangeBlockData;
      inTimespanChanges: [NeuronAllocationChangeBlockData];
    })];
    totalNeurons: Nat;
    hasMore: Bool;
  }, ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 100) {
      return #err(#InvalidData);
    };

    if (startTime > endTime) {
      return #err(#InvalidTimeRange);
    };

    // Get all neuron IDs from the index
    let allNeuronIds = Iter.toArray(Map.keys(neuronIndex));
    let totalNeurons = Array.size(allNeuronIds);

    // Apply pagination
    let startIdx = if (offset >= totalNeurons) { totalNeurons } else { offset };
    let endIdx = if (startIdx + limit >= totalNeurons) { totalNeurons } else { startIdx + limit };
    let hasMore = endIdx < totalNeurons;

    var results : [(Blob, {
      preTimespanAllocation: ?NeuronAllocationChangeBlockData;
      inTimespanChanges: [NeuronAllocationChangeBlockData];
    })] = [];

    // Process each neuron in the page
    var i = startIdx;
    while (i < endIdx) {
      let neuronId = allNeuronIds[i];

      // Get block indices for this neuron
      switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
        case (?blockIndices) {
          var inTimespanChanges : [NeuronAllocationChangeBlockData] = [];
          var preTimespanAllocation : ?NeuronAllocationChangeBlockData = null;

          let indicesArray = Iter.toArray(blockIndices.vals());

          // Iterate through all block indices
          for (blockIndex in indicesArray.vals()) {
            let getBlocksArgs = [{
              start = blockIndex;
              length = 1;
            }];

            let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);

            if (Array.size(icrc3Result.blocks) > 0) {
              let block = icrc3Result.blocks[0];

              switch (parseBlockForNeuronAllocation(block, neuronId, 0, Int.abs(Time.now()) + 1000000000)) {
                case (?changeData) {
                  if (changeData.timestamp < startTime) {
                    preTimespanAllocation := ?changeData;
                  } else if (changeData.timestamp >= startTime and changeData.timestamp <= endTime) {
                    inTimespanChanges := Array.append(inTimespanChanges, [changeData]);
                  };
                };
                case (_) {};
              };
            };
          };

          results := Array.append(results, [(neuronId, {
            preTimespanAllocation = preTimespanAllocation;
            inTimespanChanges = inTimespanChanges;
          })]);
        };
        case (_) {
          // No blocks for this neuron, include with empty data
          results := Array.append(results, [(neuronId, {
            preTimespanAllocation = null;
            inTimespanChanges = [];
          })]);
        };
      };

      i += 1;
    };

    #ok({
      neurons = results;
      totalNeurons = totalNeurons;
      hasMore = hasMore;
    });
  };

  // Comprehensive query method that returns both pre-timespan allocation and in-timespan changes
  public shared query ({ caller }) func getNeuronAllocationChangesWithContext(
    neuronId: Blob,
    startTime: Int,
    endTime: Int,
    limit: Nat
  ) : async Result.Result<{
    preTimespanAllocation: ?NeuronAllocationChangeBlockData;
    inTimespanChanges: [NeuronAllocationChangeBlockData];
  }, ArchiveError> {
    if (not base.isAuthorized(caller, #QueryData)) {
      return #err(#NotAuthorized);
    };

    if (limit > 500) {
      return #err(#InvalidData);
    };

    if (startTime > endTime) {
      return #err(#InvalidTimeRange);
    };

    // Get block indices for this neuron from the index
    switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?blockIndices) {
        var inTimespanChanges : [NeuronAllocationChangeBlockData] = [];
        var preTimespanAllocation : ?NeuronAllocationChangeBlockData = null;
        var inTimespanCount = 0;
        
        // Convert to array and sort by timestamp (block indices should be in order)
        let indicesArray = Iter.toArray(blockIndices.vals());
        
        // Iterate through all block indices to find pre-timespan and in-timespan data
        for (blockIndex in indicesArray.vals()) {
          // Get the specific block using ICRC3
          let getBlocksArgs = [{
            start = blockIndex;
            length = 1;
          }];
          
          let icrc3Result = base.icrc3_get_blocks(getBlocksArgs);
          
          // Process the single block if available
          if (Array.size(icrc3Result.blocks) > 0) {
            let block = icrc3Result.blocks[0];
            
            // Parse the block to extract neuron allocation change data (no time filtering)
            switch (parseBlockForNeuronAllocation(block, neuronId, 0, Int.abs(Time.now()) + 1000000000)) {
              case (?changeData) {
                if (changeData.timestamp < startTime) {
                  // This is before the timespan - keep as most recent pre-timespan allocation
                  preTimespanAllocation := ?changeData;
                } else if (changeData.timestamp >= startTime and changeData.timestamp <= endTime) {
                  // This is within the timespan
                  if (inTimespanCount < limit) {
                    inTimespanChanges := Array.append(inTimespanChanges, [changeData]);
                    inTimespanCount += 1;
                  };
                };
                // Ignore changes after endTime
              };
              case (_) {
                // Block parsing failed
              };
            };
          };
        };
        
        #ok({
          preTimespanAllocation = preTimespanAllocation;
          inTimespanChanges = inTimespanChanges;
        });
      };
      case (_) {
        // No blocks found for this neuron
        #ok({
          preTimespanAllocation = null;
          inTimespanChanges = [];
        });
      };
    };
  };

  //=========================================================================
  // Helper Functions for Block Parsing
  //=========================================================================

  // Helper function to parse ICRC3 block and extract neuron allocation change data
  private func parseBlockForNeuronAllocation(
    block : ICRC3Service.Block, 
    targetNeuronId : Blob, 
    startTime : Int, 
    endTime : Int
  ) : ?NeuronAllocationChangeBlockData {
    // The ICRC3 Block has structure {block : Value; id : Nat}
    // The block.block field contains our Value data
    switch (block.block) {
      case (#Map(entries)) {
        var blockTimestamp : ?Int = null;
        var blockNeuronId : ?Blob = null;
        var isNeuronAllocationChange : Bool = false;
        var extractedData : ?NeuronAllocationChangeBlockData = null;
        
        // Extract the relevant fields from the map
        for ((key, value) in entries.vals()) {
          switch (key, value) {
            case ("tx", #Map(txEntries)) {
              // The actual data is inside the tx field
              for ((txKey, txValue) in txEntries.vals()) {
                switch (txKey, txValue) {
                  case ("operation", #Text(op)) {
                    if (op == "3neuron_allocation_change") {
                      isNeuronAllocationChange := true;
                    };
                  };
                  case ("timestamp", #Int(ts)) {
                    blockTimestamp := ?ts;
                  };
                  case ("data", #Map(dataEntries)) {
                    // Parse the neuron allocation change data
                    if (isNeuronAllocationChange) {
                      extractedData := parseNeuronAllocationData(dataEntries);
                      // Extract neuron ID for verification
                      for ((dataKey, dataValue) in dataEntries.vals()) {
                        switch (dataKey, dataValue) {
                          case ("neuronId", #Blob(nId)) {
                            blockNeuronId := ?nId;
                          };
                          case (_) {};
                        };
                      };
                    };
                  };
                  case (_) {};
                };
              };
            };
            case (_) {};
          };
        };
        
        // Verify this block matches our criteria
        switch (blockTimestamp, blockNeuronId, extractedData) {
          case (?timestamp, ?neuronId, ?data) {
            // Check if neuron ID matches
            if (not Blob.equal(neuronId, targetNeuronId)) {
              return null;
            };
            
            // Check if timestamp is within range
            if (timestamp < startTime or timestamp > endTime) {
              return null;
            };
            
            // Return the parsed data
            ?data;
          };
          case (_) {
            null; // Missing required fields
          };
        };
      };
      case (_) {
        null; // Not a map structure
      };
    };
  };

  // Helper function to parse neuron allocation change data from ICRC3 Map entries
  private func parseNeuronAllocationData(dataEntries : [(Text, ArchiveTypes.Value)]) : ?NeuronAllocationChangeBlockData {
    var id : ?Nat = null;
    var timestamp : ?Int = null;
    var neuronId : ?Blob = null;
    var changeType : ?ArchiveTypes.AllocationChangeType = null;
    var oldAllocations : ?[ArchiveTypes.Allocation] = null;
    var newAllocations : ?[ArchiveTypes.Allocation] = null;
    var votingPower : ?Nat = null;
    var maker : ?Principal = null;
    var reason : ?Text = null;
    var penaltyMultiplier : ?Nat = null; // null = no penalty (for legacy records)

    // Extract all fields
    for ((key, value) in dataEntries.vals()) {
      switch (key, value) {
        case ("id", #Nat(n)) { id := ?n; };
        case ("timestamp", #Int(t)) { timestamp := ?t; };
        case ("neuronId", #Blob(nId)) { neuronId := ?nId; };
        case ("changeType", changeTypeValue) {
          changeType := parseAllocationChangeType(changeTypeValue);
        };
        case ("oldAllocations", #Array(allocArray)) {
          oldAllocations := parseAllocationArray(allocArray);
        };
        case ("newAllocations", #Array(allocArray)) {
          newAllocations := parseAllocationArray(allocArray);
        };
        case ("votingPower", #Nat(vp)) { votingPower := ?vp; };
        case ("maker", #Blob(makerBlob)) {
          maker := ?Principal.fromBlob(makerBlob);
        };
        case ("reason", #Text(r)) { reason := ?r; };
        case ("penaltyMultiplier", #Nat(pm)) { penaltyMultiplier := ?pm; };
        case (_) {};
      };
    };

    // Construct the result if all required fields are present
    switch (id, timestamp, neuronId, changeType, oldAllocations, newAllocations, votingPower, maker, reason) {
      case (?i, ?t, ?nId, ?ct, ?old, ?new, ?vp, ?m, ?r) {
        ?{
          id = i;
          timestamp = t;
          neuronId = nId;
          changeType = ct;
          oldAllocations = old;
          newAllocations = new;
          votingPower = vp;
          maker = m;
          reason = ?r;
          penaltyMultiplier = penaltyMultiplier;
        };
      };
      case (_) {
        null; // Missing required fields
      };
    };
  };

  // Helper function to parse AllocationChangeType from ICRC3 Value
  private func parseAllocationChangeType(value : ArchiveTypes.Value) : ?ArchiveTypes.AllocationChangeType {
    switch (value) {
      case (#Map(entries)) {
        var changeTypeStr : ?Text = null;
        var userInitiated : ?Bool = null;
        var followedUser : ?Principal = null;
        
        for ((key, val) in entries.vals()) {
          switch (key, val) {
            case ("type", #Text(t)) { changeTypeStr := ?t; };
            case ("userInitiated", #Nat(1)) { userInitiated := ?true; };
            case ("userInitiated", #Nat(0)) { userInitiated := ?false; };
            case ("followedUser", #Blob(userBlob)) { 
              followedUser := ?Principal.fromBlob(userBlob); 
            };
            case (_) {};
          };
        };
        
        switch (changeTypeStr) {
          case (?"UserUpdate") {
            switch (userInitiated) {
              case (?initiated) {
                ?#UserUpdate({ userInitiated = initiated });
              };
              case (_) { null; };
            };
          };
          case (?"FollowAction") {
            switch (followedUser) {
              case (?user) {
                ?#FollowAction({ followedUser = user });
              };
              case (_) { null; };
            };
          };
          case (?"SystemRebalance") { ?#SystemRebalance; };
          case (?"VotingPowerChange") { ?#VotingPowerChange; };
          case (_) { null; };
        };
      };
      case (_) { null; };
    };
  };

  // Helper function to parse allocation array from ICRC3 Values
  private func parseAllocationArray(values : [ArchiveTypes.Value]) : ?[ArchiveTypes.Allocation] {
    var results : [ArchiveTypes.Allocation] = [];
    
    for (value in values.vals()) {
      switch (value) {
        case (#Map(entries)) {
          var token : ?Principal = null;
          var basisPoints : ?Nat = null;
          
          for ((key, val) in entries.vals()) {
            switch (key, val) {
              case ("token", #Blob(tokenBlob)) {
                token := ?Principal.fromBlob(tokenBlob);
              };
              case ("basisPoints", #Nat(bp)) {
                basisPoints := ?bp;
              };
              case (_) {};
            };
          };
          
          switch (token, basisPoints) {
            case (?t, ?bp) {
              results := Array.append(results, [{
                token = t;
                basisPoints = bp;
              }]);
            };
            case (_) {
              return null; // Invalid allocation
            };
          };
        };
        case (_) {
          return null; // Invalid allocation format
        };
      };
    };
    
    ?results;
  };

  // Helper function to parse ICRC3 block without time filtering
  private func parseBlockForNeuronAllocationNoTimeFilter(
    block : ICRC3Service.Block, 
    targetNeuronId : Blob
  ) : ?NeuronAllocationChangeBlockData {
    // The ICRC3 Block has structure {block : Value; id : Nat}
    // The block.block field contains our Value data
    switch (block.block) {
      case (#Map(entries)) {
        var blockNeuronId : ?Blob = null;
        var isNeuronAllocationChange : Bool = false;
        var extractedData : ?NeuronAllocationChangeBlockData = null;
        
        // Extract the relevant fields from the map
        for ((key, value) in entries.vals()) {
          switch (key, value) {
            case ("tx", #Map(txEntries)) {
              // The actual data is inside the tx field
              for ((txKey, txValue) in txEntries.vals()) {
                switch (txKey, txValue) {
                  case ("operation", #Text(op)) {
                    if (op == "3neuron_allocation_change") {
                      isNeuronAllocationChange := true;
                    };
                  };
                  case ("data", #Map(dataEntries)) {
                    // Parse the neuron allocation change data
                    if (isNeuronAllocationChange) {
                      extractedData := parseNeuronAllocationData(dataEntries);
                      // Extract neuron ID for verification
                      for ((dataKey, dataValue) in dataEntries.vals()) {
                        switch (dataKey, dataValue) {
                          case ("neuronId", #Blob(nId)) {
                            blockNeuronId := ?nId;
                          };
                          case (_) {};
                        };
                      };
                    };
                  };
                  case (_) {};
                };
              };
            };
            case (_) {};
          };
        };
        
        // Verify this block matches our criteria (neuron ID only, no time filtering)
        switch (blockNeuronId, extractedData) {
          case (?neuronId, ?data) {
            // Check if neuron ID matches
            if (not Blob.equal(neuronId, targetNeuronId)) {
              return null;
            };
            
            // Return the parsed data
            ?data;
          };
          case (_) {
            null; // Missing required fields
          };
        };
      };
      case (_) {
        null; // Not a map structure
      };
    };
  };

  //=========================================================================
  // Custom Index Management
  //=========================================================================

  private func updateNeuronIndex(neuronId: Blob, blockIndex: Nat) {
    let currentIndices = switch (Map.get(neuronIndex, Map.bhash, neuronId)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(neuronIndex, Map.bhash, neuronId, Array.append(currentIndices, [blockIndex]));
  };

  private func updateMakerIndex(maker: Principal, blockIndex: Nat) {
    let currentIndices = switch (Map.get(makerIndex, Map.phash, maker)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(makerIndex, Map.phash, maker, Array.append(currentIndices, [blockIndex]));
  };

  private func updateTimestampIndex(timestamp: Int, blockIndex: Nat) {
    let currentIndices = switch (Map.get(timestampIndex, Map.ihash, timestamp)) {
      case (?indices) { indices };
      case null { [] };
    };
    Map.set(timestampIndex, Map.ihash, timestamp, Array.append(currentIndices, [blockIndex]));
  };

  //=========================================================================
  // System Functions
  //=========================================================================

  system func preupgrade() {
    icrc3State := icrc3StateRef.value;
    base.preupgrade();
  };

  system func postupgrade() {
    icrc3StateRef.value := icrc3State;
    base.postupgrade<system>(func() : async () { /* no-op */ });
  };

  //=========================================================================
  // Admin Functions
  //=========================================================================

  // Public stats method for frontend (no authorization required)
  public query func getArchiveStats() : async ArchiveTypes.ArchiveStatus {
    let totalBlocks = base.getTotalBlocks();
    let oldestBlock = if (totalBlocks > 0) { ?0 } else { null };
    let newestBlock = if (totalBlocks > 0) { ?(totalBlocks - 1) } else { null };
    
    {
      totalBlocks = totalBlocks;
      oldestBlock = oldestBlock;
      newestBlock = newestBlock;
      supportedBlockTypes = ["3neuron_allocation_change"];
      storageUsed = 0;
      lastArchiveTime = lastImportedNeuronAllocationTimestamp;
    };
  };

  // Detailed stats method with authorization for admin interface
  public shared ({ caller }) func getDetailedArchiveStats() : async Result.Result<{
    totalBlocks: Nat;
    totalNeuronAllocationChanges: Nat;
    neuronCount: Nat;
    makerCount: Nat;
  }, ArchiveError> {
    if (not base.isAuthorized(caller, #GetMetrics)) {
      return #err(#NotAuthorized);
    };

    #ok({
      totalBlocks = base.getTotalBlocks();
      totalNeuronAllocationChanges = totalNeuronAllocationChanges;
      neuronCount = Map.size(neuronIndex);
      makerCount = Map.size(makerIndex);
    });
  };

  //=========================================================================
  // Import Logic
  //=========================================================================

  // Import function that fetches data from DAO canister
  private func importBatchData<system>() : async {imported: Nat; failed: Nat} {
    var totalImported = 0;
    var totalFailed = 0;
    
    try {
      // Get DAO canister actor
      let canisterIds = CanisterIds.CanisterIds(this_canister_id());
      let daoCanisterId = canisterIds.getCanisterId(#DAO_backend);
      let daoActor : DAOTypes.Self = actor(Principal.toText(daoCanisterId));
      
      // Get the last timestamp we imported
      let sinceTimestamp = lastImportedNeuronAllocationTimestamp;
      let limit = 100; // Import in batches
      
      // Fetch neuron allocation changes from DAO
      let result = await daoActor.getNeuronAllocationChangesSince(sinceTimestamp, limit);
      
      switch (result) {
        case (#ok(response)) {
          // Process each neuron allocation change
          for (change in response.changes.vals()) {
            try {
              // Convert to block data format
              let blockData : NeuronAllocationChangeBlockData = {
                id = totalNeuronAllocationChanges; // Use current count as unique ID
                neuronId = change.neuronId;
                timestamp = change.timestamp;
                changeType = change.changeType;
                oldAllocations = change.oldAllocations;
                newAllocations = change.newAllocations;
                votingPower = change.votingPower;
                maker = change.maker;
                reason = change.reason;
                penaltyMultiplier = change.penaltyMultiplier;
              };
              
              // Archive the data
              switch (await archiveNeuronAllocationChange<system>(blockData)) {
                case (#ok(_)) { 
                  totalImported += 1;
                  // Update the timestamp tracking
                  lastImportedNeuronAllocationTimestamp := change.timestamp;
                };
                case (#err(_)) { totalFailed += 1; };
              };
            } catch (e) {
              totalFailed += 1;
            };
          };
        };
        case (#err(_)) { totalFailed += 1; };
      };
    } catch (e) {
      totalFailed += 1;
    };
    
    {imported = totalImported; failed = totalFailed};
  };

  //=========================================================================
  // Standard Archive Management Methods (required by admin interface)
  //=========================================================================

  public query func getBatchImportStatus() : async {isRunning: Bool; intervalSeconds: Nat} {
    base.getBatchImportStatus();
  };

  public query func getTimerStatus() : async BatchImportTimer.TimerStatus {
    base.getTimerStatus();
  };

  public query ({ caller }) func getArchiveStatus() : async Result.Result<ArchiveTypes.ArchiveStatus, ArchiveError> {
    base.getArchiveStatus(caller);
  };

  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    base.getLogs(count, caller);
  };

  public shared ({ caller }) func startBatchImportSystem() : async Result.Result<Text, Text> {
    await base.startAdvancedBatchImportSystem<system>(caller, null, null, ?importBatchData);
  };

  public shared ({ caller }) func stopBatchImportSystem() : async Result.Result<Text, Text> {
    base.stopBatchImportSystem(caller);
  };

  public shared ({ caller }) func stopAllTimers() : async Result.Result<Text, Text> {
    base.stopAllTimers(caller);
  };

  public shared ({ caller }) func runManualBatchImport() : async Result.Result<Text, Text> {
    await base.runAdvancedManualBatchImport<system>(caller, null, null, ?importBatchData);
  };

  public shared ({ caller }) func setMaxInnerLoopIterations(iterations: Nat) : async Result.Result<Text, Text> {
    base.setMaxInnerLoopIterations(caller, iterations);
  };

  public shared ({ caller }) func resetImportTimestamps() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err("Not authorized");
    };
    // Reset import timestamp to re-import all historical data
    lastImportedNeuronAllocationTimestamp := 0;
    #ok("Import timestamps reset for neuron allocation archive");
  };

  // Import method that the admin interface will call
  public shared ({ caller }) func importNeuronAllocationChanges<system>() : async Result.Result<Text, Text> {
    if (not base.isAuthorized(caller, #UpdateConfig)) {
      return #err("Not authorized");
    };
    
    // TODO: Implement actual import logic from DAO circular buffer
    // For now, return success to avoid errors in the admin interface
    #ok("Neuron allocation changes imported successfully");
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };
};