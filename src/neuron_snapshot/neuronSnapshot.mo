import Int "mo:base/Int";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import List "mo:base/List";
import Time "mo:base/Time";
import Timer "mo:base/Timer";
import Principal "mo:base/Principal";
import T "./ns_types";
import Map "mo:map/Map";
import calcHelp "./VPcalculation";
import Vector "mo:vector";
import Iter "mo:base/Iter";
import Blob "mo:base/Blob";
import Nat8 "mo:base/Nat8";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import Error "mo:base/Error";
import Array "mo:base/Array";
import Result "mo:base/Result";
import CanisterIds "../helper/CanisterIds";
import NNSPropCopy "./NNSPropCopy";

shared deployer actor class neuronSnapshot() = this {

  private func this_canister_id() : Principal {
      Principal.fromActor(this);
  };

  let logger = Logger.Logger();

  var vp_calc = calcHelp.vp_calc();

  // Map of principal-snapshot combinations to their associated neuron IDs
  stable let neuronStore : T.NeuronStore = Map.new<T.SnapshotId, [(Principal, Vector.Vector<T.NeuronVP>)]>();

  // Cumulative values per snapshot (total staked maturity and cached stake)
  stable let snapshotCumulativeValues : Map.Map<T.SnapshotId, T.CumulativeVP> = Map.new<T.SnapshotId, T.CumulativeVP>();

  let { nhash; n64hash; phash } = Map;

  let second_ns : Nat64 = 1_000_000_000; // 1 second in nanoseconds
  let minute_ns : Nat64 = 60 * second_ns; // 1 minute in nanoseconds

  let NEURON_SNAPSHOT_TIMEOUT_NS : Nat64 = 10 * minute_ns;
  let NEURON_SNAPSHOT_NEURONS_PER_CALL : Nat32 = 50;
  let NEURON_SNAPSHOT_NEURONS_PER_TICK : Nat32 = NEURON_SNAPSHOT_NEURONS_PER_CALL * 5;

  stable var sns_governance_canister_id = Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai"); // TACO DAO SNS Governance Canister ID
  //var sns_governance_canister_id = Principal.fromText("aaaaa-aa"); // NB: SNEED GOV! change in production when known!

  // NNS Governance Canister ID (mainnet)
  let nns_governance_canister_id = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");

  let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);

  //let DAOprincipal = Principal.fromText("ywhqf-eyaaa-aaaad-qg6tq-cai");
  let DAOprincipal = DAO_BACKEND_ID;

  stable var masterAdmin : Principal = Principal.fromText("d7zib-qo5mr-qzmpb-dtyof-l7yiu-pu52k-wk7ng-cbm3n-ffmys-crbkz-nae");




  var snapshotTimerId : Nat = 0;

  stable var neuron_snapshots : List.List<T.NeuronSnapshot> = List.nil();

  // The latest snaphsot id
  stable var neuron_snapshot_head_id : T.SnapshotId = 0;

  // Current snapshot taking status (#Ready, #TakingSnapshot or #StoringSnapshot)
  stable var neuron_snapshot_status : T.NeuronSnapshotStatus = #Ready;

  // If currently taking a snapshot, the work will timeout at this timestamp
  stable var neuron_snapshot_timeout : ?T.Timestamp = null;

  // used for keeping track which neuron id to import from next across ticks
  stable var neuron_snapshot_curr_neuron_id : ?T.NeuronId = null;

  // used for storing imported neurons across ticks
  stable var neuron_snapshot_importing : List.List<T.Neuron> = List.nil();

  // Flag for cancelling snapshot tacking loop
  stable var CANCEL_NEURON_SNAPSHOT = false;

  // Maximum number of snapshots to keep (configurable by admin/DAO)
  stable var maxNeuronSnapshots : Nat = 100; // Keep last 100 snapshots by default

  // Track which NNS proposals we have already copied (to avoid duplicates)
  stable var copiedNNSProposals : Map.Map<Nat64, Nat64> = Map.new<Nat64, Nat64>(); // NNS Proposal ID -> SNS Proposal ID
  
  // Track highest NNS proposal ID we have processed (not necessarily copied)
  stable var highestProcessedNNSProposalId : Nat64 = 138609;

  // Test mode variables
  stable var test = false;
  stable let mockNeurons = Map.new<Principal, T.Neuron>();

  var sns_gov_canister = actor (Principal.toText(sns_governance_canister_id)) : actor {
    list_neurons : shared query T.ListNeurons -> async T.ListNeuronsResponse;
    get_nervous_system_parameters : shared () -> async T.NervousSystemParameters;
    manage_neuron : shared NNSPropCopy.ManageNeuron -> async NNSPropCopy.ManageNeuronResponse;
    get_proposal : shared query NNSPropCopy.GetSNSProposal -> async NNSPropCopy.GetSNSProposalResponse;
  };

  let nns_gov_canister = actor (Principal.toText(nns_governance_canister_id)) : NNSPropCopy.NNSGovernanceActor;

  // Mock principals for testing
  let testActorA = Principal.fromText("hhaaz-2aaaa-aaaaq-aacla-cai");
  let testActorB = Principal.fromText("qtooy-2yaaa-aaaaq-aabvq-cai");
  let testActorC = Principal.fromText("aanaa-xaaaa-aaaah-aaeiq-cai");

  public query func get_neuron_snapshot_curr_neuron_id() : async ?T.NeuronId {
    neuron_snapshot_curr_neuron_id;
  };

  public query func get_neuron_snapshot_importing_count() : async Nat {
    List.size(neuron_snapshot_importing);
  };

  private func isMasterAdmin(caller : Principal) : Bool {
    // Check single master admin (legacy)
    if (caller == masterAdmin) { return true; };
    
    // Use shared admin authorization
    AdminAuth.isMasterAdmin(caller, canister_ids.isKnownCanister)
  };

  public shared ({ caller }) func setTest(enabled : Bool) : async () {
    assert (Principal.isController(caller) or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")));
    test := enabled;
    logger.info("System", "Test mode " # (if enabled "enabled" else "disabled"), "setTest");

    if (enabled) {
      // Setup mock neurons for test actors
      Map.clear(mockNeurons);

      // Actor A: 4000 VP
      Map.set(
        mockNeurons,
        phash,
        testActorA,
        createMockNeuron(1, 4000, testActorA),
      );

      // Actor B: 3000 VP
      Map.set(
        mockNeurons,
        phash,
        testActorB,
        createMockNeuron(2, 3000, testActorB),
      );

      // Actor C: 2000 VP
      Map.set(
        mockNeurons,
        phash,
        testActorC,
        createMockNeuron(3, 2000, testActorC),
      );
      logger.info("System", "Mock neurons created for testing", "setTest");
    };
  };

  // Helper to create mock neuron
  func createMockNeuron(id : Nat, vp : Nat64, principal : Principal) : T.Neuron {
    {
      id = ?{ id = Blob.fromArray([Nat8.fromNat(id)]) };
      staked_maturity_e8s_equivalent = ?vp;
      permissions = [{
        principal = ?principal;
        permission_type = [4, 3];
      }];
      maturity_e8s_equivalent = vp;
      cached_neuron_stake_e8s = vp;
      created_timestamp_seconds = 1000000;
      source_nns_neuron_id = null;
      auto_stake_maturity = null;
      aging_since_timestamp_seconds = 1000000;
      dissolve_state = ? #DissolveDelaySeconds(1204800); // 2 weeks
      voting_power_percentage_multiplier = 100;
      vesting_period_seconds = null;
      disburse_maturity_in_progress = [];
      followees = [];
      neuron_fees_e8s = 0;
    };
  };

  // Returns the highest (latest) snapshot id.
  public query func get_neuron_snapshot_head_id() : async T.SnapshotId {
    neuron_snapshot_head_id;
  };

  // Returns the current snapshot taking status (#Ready, #TakingSnapshot or #StoringSnapshot)
  public query func get_neuron_snapshot_status() : async T.NeuronSnapshotStatus {
    neuron_snapshot_status;
  };

  // Returns information about the snapshot with the given id (it's id, timestamp and result status #Ok, #Err(#Cancelled/#Timedout))
  public query func get_neuron_snapshot_info(id : T.SnapshotId) : async ?T.NeuronSnapshotInfo {
    let neuron_snapshot = get_neuron_snapshot_by_id(id);
    switch (neuron_snapshot) {
      case (null) {
        logger.warn("Snapshot", "Snapshot not found with ID: " # Nat.toText(id), "get_neuron_snapshot_info");
        null;
      };
      case (?snapshot) {
        let info : T.NeuronSnapshotInfo = {
          id = snapshot.id;
          timestamp = snapshot.timestamp;
          result = snapshot.result;
        };
        logger.info("Snapshot", "Retrieved snapshot info for ID: " # Nat.toText(id), "get_neuron_snapshot_info");
        ?info;
      };
    };
  };

  // Returns the information (id, timestamp, result) for the given range of snapshots (newest to oldest)
  public query func get_neuron_snapshots_info(start : Nat, length : Nat) : async [T.NeuronSnapshotInfo] {
    logger.info("Snapshot", "Retrieved snapshots info from " # Nat.toText(start) # " with length " # Nat.toText(length), "get_neuron_snapshots_info");
    List.toArray<T.NeuronSnapshotInfo>(
      List.map<T.NeuronSnapshot, T.NeuronSnapshotInfo>(
        List.take(
          List.drop(
            neuron_snapshots,
            start,
          ),
          length,
        ),
        func neuron_snapshot {
          {
            id = neuron_snapshot.id;
            timestamp = neuron_snapshot.timestamp;
            result = neuron_snapshot.result;
          };
        },
      )
    );
  };

  // Returns the given range of stored neurons in the snapshot with the specified id.
  public query func get_neuron_snapshot_neurons(snapshot_id : T.SnapshotId, start : Nat, length : Nat) : async [T.Neuron] {
    let neuron_snapshot = get_neuron_snapshot_by_id(snapshot_id);
    switch (neuron_snapshot) {
      case (null) {
        logger.warn("Snapshot", "No snapshot found for ID: " # Nat.toText(snapshot_id), "get_neuron_snapshot_neurons");
        [];
      };
      case (?snapshot) {
        var result : List.List<T.Neuron> = List.nil();
        let neurons = snapshot.neurons;
        var curr = start;
        let stop = curr + length;
        while (curr < stop and curr < neurons.size()) {
          result := List.push(neurons.get(curr), result);
          curr += 1;
        };
        logger.info("Snapshot", "Retrieved " # Nat.toText(List.size(result)) # " neurons from snapshot " # Nat.toText(snapshot_id), "get_neuron_snapshot_neurons");
        List.toArray<T.Neuron>(result);
      };
    };
  };

  // Take a neuron snapshot. Returns the id of the snapshot it creates. To use:
  // 1) Start by ensuring there is not already a snapshot currently being taken by calling
  //    the get_neuron_snapshot_status() function and ensuring it returns #Ready (if not, wait until it does)
  // 2) Call take_neuron_snapshot() and save away the resulting snapshot id it returns.
  // 3) Periodically call the get_neuron_snapshot_status() function until it returns #Ready again
  //    (not #TakingSnapshot or #StoringSnapshot).
  // 3.1) You may also want to check that the result of calling get_neuron_snapshot_head_id()
  //      matches the snapshot id you saved away. If it has advanced ahead of the id you saved
  //      that also means you can proceed to step 4.
  // 4) When the status is #Ready, call the get_neuron_snapshot_info function, passing it the
  //    snapshot id that was returned from take_neuron_snapshot() and ensure the status is #Ok
  // 5) Page through the snapshotted neurons using the get_neuron_snapshot_neurons() function.
  public shared ({ caller }) func take_neuron_snapshot() : async T.TakeNeuronSnapshotResult {
    logger.info("Snapshot", "Snapshot requested by " # Principal.toText(caller), "take_neuron_snapshot");

    //assert (caller == DAOprincipal);
    if (sns_governance_canister_id == Principal.fromText("aaaaa-aa") and not test) {
      logger.warn("Snapshot", "SNS governance canister ID not set", "take_neuron_snapshot");
      return #Err(#SnsGovernanceCanisterIdNotSet);
    };

    ignore check_neuron_snapshot_timeout();

    switch (neuron_snapshot_status) {
      case (#Ready) {
        neuron_snapshot_head_id += 1;
        logger.info("Snapshot", "Starting new snapshot with ID: " # Nat.toText(neuron_snapshot_head_id), "take_neuron_snapshot");
        await* take_neuron_snapshot_tick();
        #Ok(neuron_snapshot_head_id);
      };
      case (_) {
        logger.warn("Snapshot", "Cannot take snapshot - already in progress", "take_neuron_snapshot");
        #Err(#AlreadyTakingSnapshot);
      };
    };
  };

  // Cancel an ongoing snapshot (only for rare scenarios where the snapshot canister hangs)
  public shared ({ caller }) func cancel_neuron_snapshot() : async T.CancelNeuronSnapshotResult {
    logger.info("Snapshot", "Snapshot cancellation requested by " # Principal.toText(caller), "cancel_neuron_snapshot");

    //assert (caller == DAOprincipal);

    switch (neuron_snapshot_status) {
      case (#Ready) {
        logger.warn("Snapshot", "No snapshot in progress to cancel", "cancel_neuron_snapshot");
        #Err(#NotTakingSnapshot);
      };
      case (_) {
        logger.info("Snapshot", "Cancelling snapshot with ID: " # Nat.toText(neuron_snapshot_head_id), "cancel_neuron_snapshot");
        CANCEL_NEURON_SNAPSHOT := true;
        #Ok(neuron_snapshot_head_id);
      };
    };
  };

  // One iteration of the main snapshot taking loop
  private func take_neuron_snapshot_tick<system>() : async* () {
    if (CANCEL_NEURON_SNAPSHOT) {
      logger.info("Snapshot", "Processing snapshot cancellation", "take_neuron_snapshot_tick");
      CANCEL_NEURON_SNAPSHOT := false;
      add_neuron_snapshot(#Err(#Cancelled));
      reset_neuron_snapshot();
      return;
    };

    // if a snapshot is still running its set as #Timeout
    if (check_neuron_snapshot_timeout()) {
      logger.warn("Snapshot", "Snapshot timeout detected", "take_neuron_snapshot_tick");
      return;
    };

    switch (neuron_snapshot_status) {
      case (#Ready) {
        logger.info("Snapshot", "Starting snapshot process", "take_neuron_snapshot_tick");
        neuron_snapshot_status := #TakingSnapshot;
        neuron_snapshot_timeout := ?(get_current_timestamp() + NEURON_SNAPSHOT_TIMEOUT_NS);
        neuron_snapshot_curr_neuron_id := null;
        neuron_snapshot_importing := List.nil();

        if (test) {
          logger.info("Snapshot", "Using test mode for neuron import", "take_neuron_snapshot_tick");
          if (await* import_neuron_batch(NEURON_SNAPSHOT_NEURONS_PER_TICK, NEURON_SNAPSHOT_NEURONS_PER_CALL)) {
            // Continue importing on next tick (do nothing here now)
          } else {
            // Done importing
            neuron_snapshot_status := #StoringSnapshot;
          };
          add_neuron_snapshot(#Ok);
          reset_neuron_snapshot();
          return;
        };
      };
      case (#TakingSnapshot) {
        logger.info("Snapshot", "Continuing neuron import", "take_neuron_snapshot_tick");
        if (await* import_neuron_batch(NEURON_SNAPSHOT_NEURONS_PER_TICK, NEURON_SNAPSHOT_NEURONS_PER_CALL)) {
          // Continue importing on next tick (do nothing here now)
        } else {
          // Done importing
          logger.info("Snapshot", "Neuron import complete, moving to storage phase", "take_neuron_snapshot_tick");
          neuron_snapshot_status := #StoringSnapshot;
        };
      };
      case (#StoringSnapshot) {
        logger.info("Snapshot", "Storing snapshot", "take_neuron_snapshot_tick");
        add_neuron_snapshot(#Ok);
        reset_neuron_snapshot();
      };
    };

    // Run the next iteration of the snapshot taking loop
    if (neuron_snapshot_status != #Ready and not test) {
      // Cancel any existing timer first
      if (snapshotTimerId != 0) {
        Timer.cancelTimer(snapshotTimerId);
      };

      // Set the new timer and store its ID
      snapshotTimerId := Timer.setTimer<system>(
        #seconds 2,
        func() : async () {
          // Clear the timer ID as we're now executing it
          snapshotTimerId := 0;
          await* take_neuron_snapshot_tick();
        },
      );
    };
  };

  private func import_neuron_batch(max : Nat32, batch_size : Nat32) : async* Bool {
    logger.info("Snapshot", "Importing neuron batch (max: " # Nat32.toText(max) # ", batch size: " # Nat32.toText(batch_size) # ")", "import_neuron_batch");

    if (test) {
      // In test mode, just add mock neurons to the importing list
      for ((_, neuron) in Map.entries(mockNeurons)) {
        neuron_snapshot_importing := List.push(neuron, neuron_snapshot_importing);
      };
      logger.info("Snapshot", "Test mode: Imported " # Nat.toText(Map.size(mockNeurons)) # " mock neurons", "import_neuron_batch");
      return false; // Return false to indicate we're done importing
    } else {
      if (CANCEL_NEURON_SNAPSHOT) {
        logger.info("Snapshot", "Import cancelled", "import_neuron_batch");
        add_neuron_snapshot(#Err(#Cancelled));
        reset_neuron_snapshot();
        return false;
      };

      // Variable to track how many neurons have been imported in this call to the function.
      var cnt : Nat32 = 0;

      // Fetch neurons in batches until we reach the max number of neurons to import or until
      // the SNS governance canister returns a batch smaller than the requested batch size.
      while (cnt < max) {
        // Fetch the batch of neurons from the governance canister.
        logger.info("Snapshot", "Fetching neurons from governance canister", "import_neuron_batch");
        let result = await sns_gov_canister.list_neurons({
          of_principal = null;
          limit = batch_size;
          start_page_at = neuron_snapshot_curr_neuron_id;
        });

        if (CANCEL_NEURON_SNAPSHOT) {
          logger.info("Snapshot", "Import cancelled while fetching neurons", "import_neuron_batch");
          add_neuron_snapshot(#Err(#Cancelled));
          reset_neuron_snapshot();
          return false;
        };

        // Iterate over the batch of neurons returned by the governance canister.
        for (neuron in result.neurons.vals()) {
          // Ensure the neuron has an id.
          switch (neuron.id) {
            case (null) {
              logger.warn("Snapshot", "Neuron without ID encountered", "import_neuron_batch");
              return false;
            };
            case (?_id) {
              neuron_snapshot_importing := List.push(neuron, neuron_snapshot_importing);
              neuron_snapshot_curr_neuron_id := neuron.id;
            };
          };
        };

        // Log progress
        logger.info("Snapshot", "Imported " # Nat.toText(result.neurons.size()) # " neurons this batch", "import_neuron_batch");

        // If the last batch returned from the SNS governance canister was smaller than the requested
        // batch size, we've reached the end of the list and should stop.
        if (Nat32.fromNat(result.neurons.size()) < batch_size) {
          logger.info("Snapshot", "Reached end of neuron list", "import_neuron_batch");
          return false;
        };

        // Increase the count of how many neurons we have imported by the batch size.
        cnt := cnt + batch_size;
      };

      try {
        logger.info("Snapshot", "Fetching nervous system parameters", "import_neuron_batch");
        let params = await sns_gov_canister.get_nervous_system_parameters();
        vp_calc.setParams(params);
        logger.info("Snapshot", "Updated voting power calculation parameters", "import_neuron_batch");
      } catch (e) {
        logger.error("Snapshot", "Failed to get nervous system parameters: " # debug_show (Error.message(e)), "import_neuron_batch");
      };

      // Return the neuron id of the last imported neuron.
      logger.info("Snapshot", "Neuron batch import complete, more batches needed", "import_neuron_batch");
      return true;
    };
  };

  private func add_neuron_snapshot(result : T.NeuronSnapshotResult) : () {
    logger.info("Snapshot", "Creating new snapshot with result: " # debug_show (result), "add_neuron_snapshot");

    let new_neuron_snapshot : T.NeuronSnapshot = {
      id = neuron_snapshot_head_id;
      timestamp = get_current_timestamp();
      neurons = List.toArray<T.Neuron>(neuron_snapshot_importing);
      result = result;
    };

    let principalNeuronMap = Map.new<Principal, Vector.Vector<T.NeuronVP>>();

    var total_vp : Nat = 0;
    var total_vp_by_hotkey_setters : Nat = 0;
    var neuronsProcessed = 0;

    // Process neurons and calculate VPs during snapshot creation
    label a for (neuron in List.toArray<T.Neuron>(neuron_snapshot_importing).vals()) {
      neuronsProcessed += 1;
      switch (neuron.id) {
        case (null) { continue a };
        case (?neuronId) {
          var tacoPrincipal : Vector.Vector<Principal> = Vector.new<Principal>();

          // Handle hotkey validation
          label b for (permission in neuron.permissions.vals()) {
            switch (permission.principal) {
              case (null) { continue b };
              case (?p) {
                  Vector.add(tacoPrincipal, p);
              };
            };
          };

          // Calculate voting power during snapshot creation
          let neuronDetails : T.NeuronDetails = {
            id = neuron.id;
            staked_maturity_e8s_equivalent = neuron.staked_maturity_e8s_equivalent;
            cached_neuron_stake_e8s = neuron.cached_neuron_stake_e8s;
            aging_since_timestamp_seconds = neuron.aging_since_timestamp_seconds;
            dissolve_state = neuron.dissolve_state;
            voting_power_percentage_multiplier = neuron.voting_power_percentage_multiplier;
          };

          let votingPower = vp_calc.getVotingPower(neuronDetails);
          // NB: Snassy: this may not be a good idea - do we ever clear the neuron from the snapshot if it has 0 VP?
          if (votingPower == 0) {
            continue a;
          };

          // Store neuron with its voting power
          let neuronVP : T.NeuronVP = {
            neuronId = neuronId.id;
            votingPower = votingPower;
          };

          // Update principal's neuron array
          for (principal in Vector.vals(tacoPrincipal)) {
            let existingNeurons = switch (Map.get(principalNeuronMap, phash, principal)) {
              case null { Vector.new<T.NeuronVP>() };
              case (?neurons) { neurons };
            };
            Vector.add(existingNeurons, neuronVP);
            Map.set(principalNeuronMap, phash, principal, existingNeurons);
          };

          total_vp += votingPower;
          if (Vector.size(tacoPrincipal) > 0) {
            total_vp_by_hotkey_setters += votingPower;
          };
        };
      };
    };

    logger.info(
      "Snapshot",
      "Processed " # Nat.toText(neuronsProcessed) # " neurons with total VP: " #
      Nat.toText(total_vp) # " (hotkey setters: " # Nat.toText(total_vp_by_hotkey_setters) # ")",
      "add_neuron_snapshot",
    );

    // Store array representation
    Map.set(
      neuronStore,
      nhash,
      neuron_snapshot_head_id,
      Iter.toArray(Map.entries(principalNeuronMap)),
    );

    let cumulativeVP : T.CumulativeVP = {
      total_staked_vp = total_vp;
      total_staked_vp_by_hotkey_setters = total_vp_by_hotkey_setters;
    };
    Map.set(snapshotCumulativeValues, nhash, neuron_snapshot_head_id, cumulativeVP);

    neuron_snapshots := List.push(new_neuron_snapshot, neuron_snapshots);
    logger.info("Snapshot", "Snapshot " # Nat.toText(neuron_snapshot_head_id) # " added successfully", "add_neuron_snapshot");
    
    // Cleanup old snapshots if we exceed the limit
    cleanup_old_snapshots();
  };

  /*public query func debug_get_neuron_data_for_principal(principal : Principal) : async [(Principal, [T.NeuronVP])] {
    switch (Map.get(neuronStore, nhash, neuron_snapshot_head_id)) {
      case (?entries) {
        Array.filter<(Principal, [T.NeuronVP])>(
          Array.map<(Principal, Vector.Vector<T.NeuronVP>), (Principal, [T.NeuronVP])>(
            entries,
            func((p, v)) { (p, Vector.toArray(v)) }
          ),
          func((p, _)) { p == principal }
        );
      };
      case null { [] };
    };
  };*/


  public query func getNeuronDataForDAO(
    snapshotId : T.SnapshotId,
    start : Nat,
    limit : Nat,
  ) : async ?{
    entries : [(Principal, [T.NeuronVP])];
    total_entries : Nat;
    stopped_at : ?Nat;
  } {
    logger.info("Snapshot", "DAO requesting neuron data for snapshot: " # Nat.toText(snapshotId), "getNeuronDataForDAO");

    switch (Map.get(neuronStore, nhash, snapshotId)) {
      case (?entries) {
        let totalEntries = entries.size();

        if (start >= totalEntries) {
          logger.warn("Snapshot", "Invalid start index: " # Nat.toText(start) # " >= " # Nat.toText(totalEntries), "getNeuronDataForDAO");
          return null;
        };

        // Pre-calculate how many entries we need to fulfill the limit
        var entriesNeeded = 0;
        var itemsCount = 0; // This counts both principals and neurons
        var i = start;

        // Only scan forward enough to figure out how many entries to include
        label a while (i < totalEntries and itemsCount < limit) {
          let (_, neurons) = entries[i];
          let neuronSize = Vector.size(neurons);

          // Count the principal itself (1) plus all its neurons
          let entrySize = 1 + neuronSize;

          // If adding this entry would exceed the limit
          if (itemsCount + entrySize > limit) {
            break a;
          };

          itemsCount += entrySize;
          entriesNeeded += 1;
          i += 1;
        };

        // Now we know exactly how many entries to process, so get that slice
        let slicedEntries = Array.subArray(entries, start, entriesNeeded);

        // Map the slice directly to the output format
        let resultEntries = Array.map<(Principal, Vector.Vector<T.NeuronVP>), (Principal, [T.NeuronVP])>(
          slicedEntries,
          func((principal, neurons) : (Principal, Vector.Vector<T.NeuronVP>)) : (Principal, [T.NeuronVP]) {
            (principal, Vector.toArray(neurons));
          },
        );

        logger.info(
          "Snapshot",
          "Returned " # Nat.toText(resultEntries.size()) # " entries with " #
          Nat.toText(itemsCount - entriesNeeded) # " neurons (total items: " # Nat.toText(itemsCount) #
          ") out of " # Nat.toText(totalEntries) # " total entries",
          "getNeuronDataForDAO",
        );

        ?{
          entries = resultEntries;
          total_entries = totalEntries;
          stopped_at = if (start + entriesNeeded < totalEntries) ?(start + entriesNeeded) else null;
        };
      };
      case null {
        logger.warn("Snapshot", "No data found for snapshot ID: " # Nat.toText(snapshotId), "getNeuronDataForDAO");
        null;
      };
    };
  };

  // Get the total maturity and voting power of snapshot
  public query func getCumulativeValuesAtSnapshot(snapshotId : ?T.SnapshotId) : async ?T.CumulativeVP {
    let effectiveSnapshotId = switch (snapshotId) {
      case (?id) { id };
      case (null) {
        neuron_snapshot_head_id;
      };
    };

    logger.info("Snapshot", "Getting cumulative values for snapshot: " # Nat.toText(effectiveSnapshotId), "getCumulativeValuesAtSnapshot");
    let result = Map.get(snapshotCumulativeValues, nhash, effectiveSnapshotId);

    if (result == null) {
      logger.warn("Snapshot", "No cumulative values found for snapshot: " # Nat.toText(effectiveSnapshotId), "getCumulativeValuesAtSnapshot");
    };

    result;
  };

  private func get_neuron_snapshot_by_id(id : T.SnapshotId) : ?T.NeuronSnapshot {
    List.find<T.NeuronSnapshot>(neuron_snapshots, func test_neuron_snapshot { test_neuron_snapshot.id == id });
  };

  private func check_neuron_snapshot_timeout() : Bool {
    if (neuron_snapshot_is_timedout()) {
      reset_neuron_snapshot();
      add_neuron_snapshot(#Err(#Timeout));
      return true;
    };
    return false;
  };

  private func reset_neuron_snapshot() : () {
    neuron_snapshot_status := #Ready;
    neuron_snapshot_timeout := null;
    logger.info("Snapshot", "Snapshot state reset to Ready", "reset_neuron_snapshot");
  };

  private func neuron_snapshot_is_timedout() : Bool {
    switch (neuron_snapshot_timeout) {
      case (null) { false };
      case (?timeout) {
        let result = get_current_timestamp() > timeout;
        if (result) {
          logger.warn("Snapshot", "Snapshot timed out", "neuron_snapshot_is_timedout");
        };
        result;
      };
    };
  };

  private func get_current_timestamp() : T.Timestamp {
    Nat64.fromNat(Int.abs(Time.now()));
  };

  // Cleanup old snapshots when limit is exceeded
  private func cleanup_old_snapshots() : () {
    let currentCount = List.size(neuron_snapshots);
    if (currentCount <= maxNeuronSnapshots) {
      return; // No cleanup needed
    };

    logger.info("Snapshot", "Starting cleanup: " # Nat.toText(currentCount) # " snapshots, limit: " # Nat.toText(maxNeuronSnapshots), "cleanup_old_snapshots");

    // Keep only the most recent maxNeuronSnapshots
    let snapshotsToKeep = List.take(neuron_snapshots, maxNeuronSnapshots);
    let snapshotsToRemove = List.drop(neuron_snapshots, maxNeuronSnapshots);

    // Clean up associated data structures for removed snapshots
    List.iterate<T.NeuronSnapshot>(snapshotsToRemove, func(snapshot) {
      // Remove from neuronStore
      Map.delete(neuronStore, nhash, snapshot.id);
      // Remove from snapshotCumulativeValues
      Map.delete(snapshotCumulativeValues, nhash, snapshot.id);
      logger.info("Snapshot", "Cleaned up snapshot ID: " # Nat.toText(snapshot.id), "cleanup_old_snapshots");
    });

    // Update the snapshots list
    neuron_snapshots := snapshotsToKeep;
    
    let removedCount = currentCount - maxNeuronSnapshots;
    logger.info("Snapshot", "Cleanup completed: removed " # Nat.toText(removedCount) # " old snapshots", "cleanup_old_snapshots");
  };

  system func preupgrade() {
    logger.info("System", "Pre-upgrade started", "preupgrade");
    // Reset neuron_snapshot_importing to ensure clean state after upgrade
    neuron_snapshot_importing := List.nil();
    neuron_snapshot_status := #Ready;
    neuron_snapshot_timeout := null;
    if (snapshotTimerId != 0) {
      Timer.cancelTimer(snapshotTimerId);
    };
    logger.info("System", "Pre-upgrade completed", "preupgrade");
  };

  system func postupgrade() {
    logger.info("System", "Post-upgrade started", "postupgrade");
    // Reset timer ID since it's no longer valid after upgrade
    snapshotTimerId := 0;
    logger.info("System", "Post-upgrade completed", "postupgrade");
  };

  // Function to get logs - restricted to controllers only
  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    if (isMasterAdmin(caller) or Principal.isController(caller) or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"))) {
      logger.getLastLogs(count);
    } else { [] };
  };

  // Function to get logs by context - restricted to controllers only
  public query ({ caller }) func getLogsByContext(context : Text, count : Nat) : async [Logger.LogEntry] {
    if (isMasterAdmin(caller)or Principal.isController(caller) or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"))) {
      logger.getContextLogs(context, count);
    } else { [] };
  };

  // Function to get logs by level - restricted to controllers only
  public query ({ caller }) func getLogsByLevel(level : Logger.LogLevel, count : Nat) : async [Logger.LogEntry] {
    if (isMasterAdmin(caller) or Principal.isController(caller) or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"))) {
      logger.getLogsByLevel(level, count);
    } else { [] };
  };

  // Function to clear logs - restricted to controllers
  public shared ({ caller }) func clearLogs() : async () {
    if (isMasterAdmin(caller) or Principal.isController(caller) or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"))) {
      logger.info("System", "Logs cleared by: " # Principal.toText(caller), "clearLogs");
      logger.clearLogs();
      logger.clearContextLogs("all");
    };
  };

  public shared ({ caller }) func setLogAdmin(admin : Principal) : async () {
    if (isMasterAdmin(caller) or Principal.isController(caller) or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"))) {
      masterAdmin := admin;
    };
  };

  public shared ({ caller }) func setSnsGovernanceCanisterId(canisterId : Principal) : async () {
    if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"))) {
      sns_governance_canister_id := canisterId;
    };
  };

  // Set the maximum number of neuron snapshots to keep
  public shared ({ caller }) func setMaxNeuronSnapshots(maxSnapshots : Nat) : async () {
    if (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"))) {
      if (maxSnapshots == 0) {
        logger.error("Config", "Invalid max snapshots: cannot be zero", "setMaxNeuronSnapshots");
        return;
      };
      let oldMax = maxNeuronSnapshots;
      maxNeuronSnapshots := maxSnapshots;
      logger.info("Config", "Max snapshots updated from " # Nat.toText(oldMax) # " to " # Nat.toText(maxSnapshots), "setMaxNeuronSnapshots");
      
      // If new limit is smaller, trigger cleanup
      if (maxSnapshots < oldMax) {
        cleanup_old_snapshots();
      };
    };
  };

  // Get the current maximum number of neuron snapshots setting
  public query func getMaxNeuronSnapshots() : async Nat {
    maxNeuronSnapshots;
  };

  // Copy an NNS proposal to create an SNS motion proposal
  public shared ({ caller }) func copyNNSProposal(
    nnsProposalId : Nat64,
    proposerSubaccount : Blob
  ) : async NNSPropCopy.CopyNNSProposalResult {
    logger.info("NNSPropCopy", "Copy NNS proposal request by " # Principal.toText(caller), "copyNNSProposal");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller: " # Principal.toText(caller), "copyNNSProposal");
      return #err(#UnauthorizedCaller);
    };

    // Call the NNSPropCopy module function
    await NNSPropCopy.copyNNSProposal(
      nnsProposalId,
      nns_gov_canister,
      sns_gov_canister,
      proposerSubaccount,
      logger
    );
  };

  // Test function for proposal text formatting (for development/testing purposes)
  public query func testProposalTextFormatting() : async Text {
    NNSPropCopy.testFormatProposalText();
  };

  // Test function to demonstrate voting status calculation
  public query func testVotingStatus() : async [(Text, NNSPropCopy.VotingStatus)] {
    [
      ("No votes yet", NNSPropCopy.determineVotingStatus(null, false)),
      ("Yes leading", NNSPropCopy.determineVotingStatus(?{ yes = 100; no = 50; total = 150; timestamp_seconds = 1000 }, false)),
      ("No leading", NNSPropCopy.determineVotingStatus(?{ yes = 30; no = 80; total = 110; timestamp_seconds = 1000 }, false)),
      ("Tied votes", NNSPropCopy.determineVotingStatus(?{ yes = 50; no = 50; total = 100; timestamp_seconds = 1000 }, false)),
      ("Decided", NNSPropCopy.determineVotingStatus(?{ yes = 100; no = 50; total = 150; timestamp_seconds = 1000 }, true))
    ];
  };


  // Get full SNS proposal details
  public shared ({ caller }) func getSNSProposal(
    proposalId : Nat64
  ) : async NNSPropCopy.GetSNSProposalFullResult {
    logger.info("SNSProposal", "Full proposal request by " # Principal.toText(caller) # " for ID: " # Nat64.toText(proposalId), "getSNSProposal");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("SNSProposal", "Unauthorized caller: " # Principal.toText(caller), "getSNSProposal");
      return #err(#SNSGovernanceError({ error_message = "Unauthorized caller"; error_type = 403 }));
    };

    // Call the NNSPropCopy module function
    await NNSPropCopy.getSNSProposalFull(
      proposalId,
      sns_gov_canister,
      logger
    );
  };

  // Get SNS proposal summary with voting status and time remaining
  public shared ({ caller }) func getSNSProposalSummary(
    proposalId : Nat64
  ) : async NNSPropCopy.GetSNSProposalSummaryResult {
    logger.info("SNSProposal", "Proposal summary request by " # Principal.toText(caller) # " for ID: " # Nat64.toText(proposalId), "getSNSProposalSummary");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("SNSProposal", "Unauthorized caller: " # Principal.toText(caller), "getSNSProposalSummary");
      return #err(#SNSGovernanceError({ error_message = "Unauthorized caller"; error_type = 403 }));
    };

    // Call the NNSPropCopy module function
    await NNSPropCopy.getSNSProposalSummary(
      proposalId,
      sns_gov_canister,
      logger
    );
  };

  // Check if an NNS proposal should be copied based on its topic
  public shared ({ caller }) func shouldCopyNNSProposal(
    nnsProposalId : Nat64
  ) : async NNSPropCopy.ShouldCopyProposalResult {
    logger.info("NNSPropCopy", "Should copy check request by " # Principal.toText(caller) # " for NNS proposal: " # Nat64.toText(nnsProposalId), "shouldCopyNNSProposal");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller: " # Principal.toText(caller), "shouldCopyNNSProposal");
      return #err(#UnauthorizedCaller);
    };

    // Call the NNSPropCopy module function
    await NNSPropCopy.shouldCopyNNSProposal(
      nnsProposalId,
      nns_gov_canister,
      logger
    );
  };

  // Get detailed information about whether an NNS proposal should be copied
  public shared ({ caller }) func getNNSProposalCopyInfo(
    nnsProposalId : Nat64
  ) : async Result.Result<{
    proposal_id : Nat64;
    topic_id : Int32;
    topic_name : Text;
    should_copy : Bool;
    reason : Text;
  }, NNSPropCopy.CopyNNSProposalError> {
    logger.info("NNSPropCopy", "Copy info request by " # Principal.toText(caller) # " for NNS proposal: " # Nat64.toText(nnsProposalId), "getNNSProposalCopyInfo");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller: " # Principal.toText(caller), "getNNSProposalCopyInfo");
      return #err(#UnauthorizedCaller);
    };

    // Call the NNSPropCopy module function
    await NNSPropCopy.getNNSProposalCopyInfo(
      nnsProposalId,
      nns_gov_canister,
      logger
    );
  };

  // Process newest NNS proposals and automatically copy relevant ones we haven't copied yet
  public shared ({ caller }) func processNewestNNSProposals(
    limit : ?Nat32,
    proposerSubaccount : Blob
  ) : async NNSPropCopy.ProcessSequentialProposalsResult {
    let effectiveLimit = switch (limit) { case (?l) { l }; case (null) { 20 : Nat32 }; };
    logger.info("NNSPropCopy", "Process newest proposals request by " # Principal.toText(caller) # " (limit: " # Nat32.toText(effectiveLimit) # ")", "processNewestNNSProposals");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller: " # Principal.toText(caller), "processNewestNNSProposals");
      return #err(#UnauthorizedCaller);
    };

    // Call the NNSPropCopy module function
    let result = await NNSPropCopy.processSequentialNNSProposals(
      highestProcessedNNSProposalId,
      Nat32.toNat(effectiveLimit),
      copiedNNSProposals,
      nns_gov_canister,
      sns_gov_canister,
      proposerSubaccount,
      logger
    );

    // Update the stable variable with the highest processed proposal ID
    switch (result) {
      case (#ok(data)) {
        highestProcessedNNSProposalId := data.highest_processed_id;
        logger.info("NNSPropCopy", "Updated highest processed NNS proposal ID to: " # Nat64.toText(data.highest_processed_id), "processNewestNNSProposals");
      };
      case (#err(_)) {
        // Don't update on error
      };
    };

    result;
  };

  // Get the count of copied NNS proposals
  public query func getCopiedNNSProposalsCount() : async Nat {
    Map.size(copiedNNSProposals);
  };

  // Get all copied NNS proposals (returns array of (NNS ID, SNS ID) pairs)
  public query func getCopiedNNSProposals() : async [(Nat64, Nat64)] {
    Iter.toArray(Map.entries(copiedNNSProposals));
  };

  // Check if a specific NNS proposal has been copied
  public query func isNNSProposalCopied(nnsProposalId : Nat64) : async ?Nat64 {
    Map.get(copiedNNSProposals, n64hash, nnsProposalId);
  };

  // Add a copied proposal to the tracking (admin only)
  public shared ({ caller }) func addCopiedNNSProposal(nnsProposalId : Nat64, snsProposalId : Nat64) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller trying to add copied proposal: " # Principal.toText(caller), "addCopiedNNSProposal");
      return;
    };

    Map.set(copiedNNSProposals, n64hash, nnsProposalId, snsProposalId);
    logger.info("NNSPropCopy", "Added copied proposal mapping: NNS " # Nat64.toText(nnsProposalId) # " -> SNS " # Nat64.toText(snsProposalId) # " by " # Principal.toText(caller), "addCopiedNNSProposal");
  };

  // Remove a copied proposal from tracking (admin only)
  public shared ({ caller }) func removeCopiedNNSProposal(nnsProposalId : Nat64) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller trying to remove copied proposal: " # Principal.toText(caller), "removeCopiedNNSProposal");
      return;
    };

    switch (Map.get(copiedNNSProposals, n64hash, nnsProposalId)) {
      case (?snsProposalId) {
        Map.delete(copiedNNSProposals, n64hash, nnsProposalId);
        logger.info("NNSPropCopy", "Removed copied proposal mapping: NNS " # Nat64.toText(nnsProposalId) # " -> SNS " # Nat64.toText(snsProposalId) # " by " # Principal.toText(caller), "removeCopiedNNSProposal");
      };
      case (null) {
        logger.warn("NNSPropCopy", "No copied proposal found for NNS ID: " # Nat64.toText(nnsProposalId), "removeCopiedNNSProposal");
      };
    };
  };

  // Get the current highest processed NNS proposal ID
  public query func getHighestProcessedNNSProposalId() : async Nat64 {
    highestProcessedNNSProposalId;
  };

  // Set the highest processed NNS proposal ID (admin only)
  public shared ({ caller }) func setHighestProcessedNNSProposalId(proposalId : Nat64) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller trying to set highest processed ID: " # Principal.toText(caller), "setHighestProcessedNNSProposalId");
      return;
    };

    let oldId = highestProcessedNNSProposalId;
    highestProcessedNNSProposalId := proposalId;
    logger.info("NNSPropCopy", "Highest processed NNS proposal ID updated from " # Nat64.toText(oldId) # " to " # Nat64.toText(proposalId) # " by " # Principal.toText(caller), "setHighestProcessedNNSProposalId");
  };

/* NB: Turn on again after initial setup
  system func inspect({
    arg : Blob;
    caller : Principal;
    msg : {
      #cancel_neuron_snapshot : () -> ();
      #clearLogs : () -> ();
      #getLogs : () -> Nat;
      #getLogsByContext : () -> (Text, Nat);
      #getLogsByLevel : () -> (T.LogLevel, Nat);
      #getNeuronDataForDAO : () -> (T.SnapshotId, Nat, Nat);
      #get_neuron_snapshot_head_id : () -> ();
      #get_neuron_snapshot_info : () -> T.SnapshotId;
      #get_neuron_snapshot_neurons : () -> (T.SnapshotId, Nat, Nat);
      #get_neuron_snapshot_status : () -> ();
      #get_neuron_snapshots_info : () -> (Nat, Nat);
      #getCumulativeValuesAtSnapshot : () -> ?T.SnapshotId;
      #setLogAdmin : () -> Principal;
      #setSnsGovernanceCanisterId : () -> Principal;
      #setMaxNeuronSnapshots : () -> Nat;
      #getMaxNeuronSnapshots : () -> ();
      #setTest : () -> Bool;
      #take_neuron_snapshot : () -> ();
      #get_neuron_snapshot_curr_neuron_id : () -> ();
      #get_neuron_snapshot_importing_count : () -> ();
    };
  }) : Bool {
    switch (msg) {
      case (#setLogAdmin(_)) {
        ((isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) and arg.size() < 50000;
      };
      case (#clearLogs(_)) {
        ((isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) and arg.size() < 50000;
      };
      case (#getLogs(_)) {
        ((isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) and arg.size() < 50000;
      };
      case (#getLogsByContext(_)) {
        ((isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) and arg.size() < 50000;
      };
      case (#getLogsByLevel(_)) {
        ((isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) and arg.size() < 50000;
      };
      case (#setMaxNeuronSnapshots(_)) {
        ((isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) and arg.size() < 50000;
      };
      case (#getMaxNeuronSnapshots(_)) {
        true and arg.size() < 50000; // Query function, no special access control needed
      };
      case (_) {
        ((isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) and arg.size() < 500000;
      };
    };
  };
  */
};
