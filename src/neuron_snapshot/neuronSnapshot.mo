import Int "mo:base/Int";
import Int64 "mo:base/Int64";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import Bool "mo:base/Bool";
import List "mo:base/List";
import Time "mo:base/Time";
import Timer "mo:base/Timer";
import Principal "mo:base/Principal";
import T "./ns_types";
import NNSTypes "./nns_types";
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
import Cycles "mo:base/ExperimentalCycles";
import ArchiveTypes "../archives/archive_types";
import BatchImportTimer "../helper/batch_import_timer";

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

  let { nhash; n64hash; phash; bhash } = Map;

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

  // Non-stable periodic timer ID (reset after upgrades)
  var periodicTimerId : ?Nat = null;

  
  // Proposer subaccount for NNS proposal copying (hex: b294fae22d75d32a793a1cf131acb649c3943f585c52d413b412680fe2db26c1)
  stable var proposerSubaccount : Blob = Blob.fromArray([0xb2, 0x94, 0xfa, 0xe2, 0x2d, 0x75, 0xd3, 0x2a, 0x79, 0x3a, 0x1c, 0xf1, 0x31, 0xac, 0xb6, 0x49, 0xc3, 0x94, 0x3f, 0x58, 0x5c, 0x52, 0xd4, 0x13, 0xb4, 0x12, 0x68, 0x0f, 0xe2, 0xdb, 0x26, 0xc1]);

  // TACO DAO Neuron ID for NNS voting
  stable var taco_dao_neuron_id : NNSTypes.NeuronId = { id = 1833423628191905776 }; // TACO DAO Named Neuron ID


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
  
  // Track auto-processing state to prevent infinite loops and allow emergency stopping
  stable var isAutoProcessingNNSProposals : Bool = false;

  // Track auto-voting state to prevent infinite loops and allow emergency stopping
  stable var isAutoVotingOnUrgentProposals : Bool = false;

  // Auto-voting round counter to prevent re-attempting same proposals in one session
  stable var autoVotingRoundCounter : Nat64 = 0;

  // Track which NNS proposals we've attempted in the current round (non-stable, resets on upgrade)
  var attemptedProposalsThisRound : Map.Map<Nat64, Nat64> = Map.new<Nat64, Nat64>(); // NNS Proposal ID -> Round ID

  // Auto-voting threshold in seconds (default: 2 hours = 7200 seconds)
  stable var autoVotingThresholdSeconds : Nat64 = 7200;

  // Default voting behavior when no DAO votes exist or there's a tie
  public type DefaultVoteBehavior = {
    #VoteAdopt;   // Vote Adopt by default
    #VoteReject;  // Vote Reject by default  
    #Skip;        // Skip voting (current behavior)
  };

  // Default vote behavior (default: Vote Adopt)
  stable var defaultVoteBehavior : DefaultVoteBehavior = #VoteAdopt;

  // Periodic timer interval in seconds (default: 1 hour = 3600 seconds)
  stable var periodicTimerIntervalSeconds : Nat64 = 3600;

  // Periodic timer state tracking
  stable var periodicTimerLastRunTime : ?Nat64 = null; // Last execution timestamp
  stable var periodicTimerNextRunTime : ?Nat64 = null; // Next scheduled execution timestamp

  // TODO: retire with a migrathion path expression. This is here for backwards compatibility.
  stable var daoVotedNNSProposals : Map.Map<Nat64, Bool> = Map.new<Nat64, Bool>(); // NNS Proposal ID -> true (voted)

  // DAO Voting System - Track which NNS proposals the DAO has already voted on
  stable var daoVotedNNSProposals2 : Map.Map<Nat64, T.DAONNSVoteRecord> = Map.new<Nat64, T.DAONNSVoteRecord>(); // NNS Proposal ID -> Vote Record
  
  // DAO Voting System - Track votes per SNS proposal per neuron  
  // Structure: SNS Proposal ID -> Neuron ID -> Vote Details
  stable var daoVotes : Map.Map<Nat64, Map.Map<Blob, T.DAOVote>> = Map.new<Nat64, Map.Map<Blob, T.DAOVote>>();

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
    if (caller == DAOprincipal) { return true; };
    if (Principal.isController(caller)) { return true; };
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

    assert (isMasterAdmin(caller));

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

    assert (isMasterAdmin(caller));

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
    nnsProposalId : Nat64
  ) : async NNSPropCopy.CopyNNSProposalResult {
    logger.info("NNSPropCopy", "Copy NNS proposal request by " # Principal.toText(caller), "copyNNSProposal");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller: " # Principal.toText(caller), "copyNNSProposal");
      return #err(#UnauthorizedCaller);
    };

    // Call the NNSPropCopy module function using the stable proposer subaccount
    let result = await NNSPropCopy.copyNNSProposal(
      nnsProposalId,
      nns_gov_canister,
      sns_gov_canister,
      proposerSubaccount,
      logger
    );

    // If successful, store the NNS-SNS proposal mapping
    switch (result) {
      case (#ok(snsProposalId)) {
        Map.set(copiedNNSProposals, n64hash, nnsProposalId, snsProposalId);
        logger.info("NNSPropCopy", "Stored mapping: NNS " # Nat64.toText(nnsProposalId) # " -> SNS " # Nat64.toText(snsProposalId), "copyNNSProposal");
        return #ok(snsProposalId);
      };
      case (#err(error)) {
        return #err(error);
      };
    };
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
    limit : ?Nat32
  ) : async NNSPropCopy.ProcessSequentialProposalsResult {
    let effectiveLimit = switch (limit) { case (?l) { l }; case (null) { 20 : Nat32 }; };
    logger.info("NNSPropCopy", "Process newest proposals request by " # Principal.toText(caller) # " (limit: " # Nat32.toText(effectiveLimit) # ")", "processNewestNNSProposals");
    
    // Authorization check - only master admin, controllers, DAO backend, or SNS governance can call this
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller: " # Principal.toText(caller), "processNewestNNSProposals");
      return #err(#UnauthorizedCaller);
    };

    // Call the NNSPropCopy module function using the stable proposer subaccount
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

  // Clear all copied NNS proposals (admin only)
  public shared ({ caller }) func clearCopiedNNSProposals() : async Nat {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller trying to clear copied proposals: " # Principal.toText(caller), "clearCopiedNNSProposals");
      return 0;
    };

    let countBeforeClear = Map.size(copiedNNSProposals);
    copiedNNSProposals := Map.new<Nat64, Nat64>();
    
    logger.info("NNSPropCopy", "Cleared " # Nat.toText(countBeforeClear) # " copied NNS proposals by " # Principal.toText(caller), "clearCopiedNNSProposals");
    
    countBeforeClear;
  };

  // Start auto-processing all new NNS proposals in 10-proposal chunks (admin only)
  public shared ({ caller }) func startAutoProcessNNSProposals() : async Bool {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller trying to start auto-processing: " # Principal.toText(caller), "startAutoProcessNNSProposals");
      return false;
    };

    if (isAutoProcessingNNSProposals) {
      logger.warn("NNSPropCopy", "Auto-processing is already running, ignoring start request from " # Principal.toText(caller), "startAutoProcessNNSProposals");
      return false;
    };

    isAutoProcessingNNSProposals := true;
    logger.info("NNSPropCopy", "Starting auto-processing of NNS proposals by " # Principal.toText(caller), "startAutoProcessNNSProposals");
    
    // Start the recursive processing using the stable proposer subaccount
    await autoProcessNNSProposalsChunk();
    
    true;
  };

  // Internal recursive method to process proposals in chunks
  private func autoProcessNNSProposalsChunk() : async () {
    if (not isAutoProcessingNNSProposals) {
      logger.info("NNSPropCopy", "Auto-processing stopped via emergency stop", "autoProcessNNSProposalsChunk");
      return;
    };

    logger.info("NNSPropCopy", "Processing chunk of up to 10 NNS proposals (starting from ID: " # Nat64.toText(highestProcessedNNSProposalId) # ")", "autoProcessNNSProposalsChunk");

    try {
      // Process up to 10 proposals using the stable proposer subaccount
      let result = await NNSPropCopy.processSequentialNNSProposals(
        highestProcessedNNSProposalId,
        10, // Fixed chunk size of 10
        copiedNNSProposals,
        nns_gov_canister,
        sns_gov_canister,
        proposerSubaccount,
        logger
      );

      switch (result) {
        case (#ok(data)) {
          highestProcessedNNSProposalId := data.highest_processed_id;
          
          logger.info(
            "NNSPropCopy", 
            "Auto-processing chunk completed: " # Nat.toText(data.processed_count) # " processed, " #
            Nat.toText(data.new_copied_count) # " copied, " # Nat.toText(data.already_copied_count) # 
            " already copied, " # Nat.toText(data.skipped_count) # " skipped, " # Nat.toText(data.error_count) # 
            " errors, highest ID: " # Nat64.toText(data.highest_processed_id),
            "autoProcessNNSProposalsChunk"
          );

          // If we processed exactly 10 proposals, there might be more
          if (data.processed_count == 10) {
            logger.info("NNSPropCopy", "Found 10 proposals, scheduling next chunk via timer", "autoProcessNNSProposalsChunk");
            
            // Schedule next chunk via 0-second timer to avoid instruction limit
            let timerId = Timer.setTimer<system>(#seconds(0), func() : async () {
              let _ = autoProcessNNSProposalsChunk(); // Don't await to avoid self-call
            });
            
            ignore timerId; // We don't need to track the timer ID
          } else {
            // Less than 10 proposals found, we're done
            isAutoProcessingNNSProposals := false;
            logger.info("NNSPropCopy", "🛑 Auto-processing STOPPED - found only " # Nat.toText(data.processed_count) # " proposals", "autoProcessNNSProposalsChunk");
          };
        };
        case (#err(error)) {
          isAutoProcessingNNSProposals := false;
          logger.error("NNSPropCopy", "Auto-processing failed with error: " # debug_show(error) # " - stopping", "autoProcessNNSProposalsChunk");
        };
      };
    } catch (error) {
      isAutoProcessingNNSProposals := false;
      logger.error("NNSPropCopy", "Auto-processing crashed with exception: " # Error.message(error) # " - stopping", "autoProcessNNSProposalsChunk");
    };
  };

  // Emergency stop for auto-processing (admin only)
  public shared ({ caller }) func stopAutoProcessNNSProposals() : async Bool {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("NNSPropCopy", "Unauthorized caller trying to stop auto-processing: " # Principal.toText(caller), "stopAutoProcessNNSProposals");
      return false;
    };

    let wasRunning = isAutoProcessingNNSProposals;
    isAutoProcessingNNSProposals := false;
    
    if (wasRunning) {
      logger.info("NNSPropCopy", "Emergency stop activated by " # Principal.toText(caller) # " - auto-processing will halt", "stopAutoProcessNNSProposals");
    } else {
      logger.info("NNSPropCopy", "Stop requested by " # Principal.toText(caller) # " but auto-processing was not running", "stopAutoProcessNNSProposals");
    };
    
    wasRunning;
  };

  // Check if auto-processing is currently running
  public query func isAutoProcessingRunning() : async Bool {
    isAutoProcessingNNSProposals;
  };

  // Start auto-voting on urgent proposals in batches (admin only)
  public shared ({ caller }) func startAutoVoteOnUrgentProposals() : async Bool {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("DAOVoting", "Unauthorized caller trying to start auto-voting: " # Principal.toText(caller), "startAutoVoteOnUrgentProposals");
      return false;
    };

    if (isAutoVotingOnUrgentProposals) {
      logger.warn("DAOVoting", "Auto-voting is already running, ignoring start request from " # Principal.toText(caller), "startAutoVoteOnUrgentProposals");
      return false;
    };

    isAutoVotingOnUrgentProposals := true;
    
    // Increment the round counter for this auto-voting session
    autoVotingRoundCounter += 1;
    logger.info("DAOVoting", "Starting auto-voting on urgent proposals (round " # Nat64.toText(autoVotingRoundCounter) # ") by " # Principal.toText(caller), "startAutoVoteOnUrgentProposals");
    
    // Start the recursive processing
    await autoVoteOnUrgentProposalsChunk();
    
    true;
  };

  // Internal recursive method to process urgent proposals in batches
  private func autoVoteOnUrgentProposalsChunk() : async () {
    if (not isAutoVotingOnUrgentProposals) {
      logger.info("DAOVoting", "Auto-voting stopped via emergency stop", "autoVoteOnUrgentProposalsChunk");
      return;
    };

    logger.info("DAOVoting", "Processing batch of up to 3 urgent proposals (" # Nat64.toText(autoVotingThresholdSeconds) # "s threshold)", "autoVoteOnUrgentProposalsChunk");

    try {
      // Process up to 3 urgent proposals with configurable threshold
      let result = await autoVoteOnUrgentProposals(autoVotingThresholdSeconds, 1);
      
      switch (result) {
        case (#ok(data)) {
          logger.info(
            "DAOVoting", 
            "Auto-voting batch completed: " # Nat.toText(data.urgent_proposals_found) # " urgent found, " #
            Nat.toText(data.votes_attempted) # " attempted, " # Nat.toText(data.votes_successful) # " successful, " #
            Nat.toText(data.votes_failed) # " failed, " # Nat.toText(data.votes_already_voted) # " already voted, " #
            Nat.toText(data.votes_no_dao_votes) # " no DAO votes",
            "autoVoteOnUrgentProposalsChunk"
          );

          // Check if there are more urgent proposals to process and if we actually attempted any votes
          // If we found urgent proposals and attempted to vote on some, there might be more
          let moreUrgentProposalsRemain = data.urgent_proposals_found > data.votes_attempted;
          let attemptedAnyVotes = data.votes_attempted > 0;
          
          if (moreUrgentProposalsRemain and attemptedAnyVotes) {
            logger.info("DAOVoting", "More urgent proposals remain (" # Nat.toText(data.urgent_proposals_found - data.votes_attempted) # "), scheduling next batch via timer", "autoVoteOnUrgentProposalsChunk");
            
            // Schedule next batch via 0-second timer to avoid instruction limit
            let timerId = Timer.setTimer<system>(#seconds(0), func() : async () {
              let _ = autoVoteOnUrgentProposalsChunk(); // Don't await to avoid self-call
            });
            
            ignore timerId; // We don't need to track the timer ID
          } else {
            // No more urgent proposals or no votes attempted in this round, stop auto-voting
            isAutoVotingOnUrgentProposals := false;
            let reason = if (not attemptedAnyVotes) {
              "no new proposals to attempt in this round (round " # Nat64.toText(autoVotingRoundCounter) # ")";
            } else {
              "no more urgent proposals found";
            };
            logger.info("DAOVoting", "🛑 Auto-voting STOPPED - " # reason, "autoVoteOnUrgentProposalsChunk");
          };
        };
        case (#err(error)) {
          isAutoVotingOnUrgentProposals := false;
          logger.error("DAOVoting", "Auto-voting failed with error: " # error # " - stopping", "autoVoteOnUrgentProposalsChunk");
        };
      };
    } catch (error) {
      isAutoVotingOnUrgentProposals := false;
      logger.error("DAOVoting", "Auto-voting crashed with exception: " # Error.message(error) # " - stopping", "autoVoteOnUrgentProposalsChunk");
    };
  };

  // Emergency stop for auto-voting (admin only)
  public shared ({ caller }) func stopAutoVoteOnUrgentProposals() : async Bool {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("DAOVoting", "Unauthorized caller trying to stop auto-voting: " # Principal.toText(caller), "stopAutoVoteOnUrgentProposals");
      return false;
    };

    let wasRunning = isAutoVotingOnUrgentProposals;
    isAutoVotingOnUrgentProposals := false;
    
    if (wasRunning) {
      logger.info("DAOVoting", "Emergency stop activated by " # Principal.toText(caller) # " - auto-voting will halt", "stopAutoVoteOnUrgentProposals");
    } else {
      logger.info("DAOVoting", "Stop requested by " # Principal.toText(caller) # " but auto-voting was not running", "stopAutoVoteOnUrgentProposals");
    };
    
    wasRunning;
  };

  // Check if auto-voting is currently running
  public query func isAutoVotingRunning() : async Bool {
    isAutoVotingOnUrgentProposals;
  };

  // Get the current auto-voting round counter
  public query func getAutoVotingRoundCounter() : async Nat64 {
    autoVotingRoundCounter;
  };

  // Get the current auto-voting threshold in seconds
  public query func getAutoVotingThresholdSeconds() : async Nat64 {
    autoVotingThresholdSeconds;
  };

  // Get the current default vote behavior
  public query func getDefaultVoteBehavior() : async DefaultVoteBehavior {
    defaultVoteBehavior;
  };

  // Set the default vote behavior (admin only)
  public shared ({ caller }) func setDefaultVoteBehavior(behavior : DefaultVoteBehavior) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("DAOVoting", "Unauthorized caller trying to set default vote behavior: " # Principal.toText(caller), "setDefaultVoteBehavior");
      return;
    };

    let oldBehavior = defaultVoteBehavior;
    defaultVoteBehavior := behavior;
    
    let behaviorText = switch (behavior) {
      case (#VoteAdopt) { "Vote Adopt" };
      case (#VoteReject) { "Vote Reject" };
      case (#Skip) { "Skip" };
    };
    
    let oldBehaviorText = switch (oldBehavior) {
      case (#VoteAdopt) { "Vote Adopt" };
      case (#VoteReject) { "Vote Reject" };
      case (#Skip) { "Skip" };
    };
    
    logger.info("DAOVoting", "Default vote behavior updated from '" # oldBehaviorText # "' to '" # behaviorText # "' by " # Principal.toText(caller), "setDefaultVoteBehavior");
  };

  // Set the auto-voting threshold in seconds (admin only)
  public shared ({ caller }) func setAutoVotingThresholdSeconds(thresholdSeconds : Nat64) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("DAOVoting", "Unauthorized caller trying to set auto-voting threshold: " # Principal.toText(caller), "setAutoVotingThresholdSeconds");
      return;
    };

    if (thresholdSeconds == 0) {
      logger.error("DAOVoting", "Invalid threshold: cannot be zero", "setAutoVotingThresholdSeconds");
      return;
    };

    // Validate that threshold is larger than periodic timer interval
    if (thresholdSeconds <= periodicTimerIntervalSeconds) {
      logger.error("DAOVoting", "Invalid threshold: auto-voting threshold (" # Nat64.toText(thresholdSeconds) # "s) must be larger than periodic timer interval (" # Nat64.toText(periodicTimerIntervalSeconds) # "s)", "setAutoVotingThresholdSeconds");
      return;
    };

    let oldThreshold = autoVotingThresholdSeconds;
    autoVotingThresholdSeconds := thresholdSeconds;
    
    logger.info("DAOVoting", "Auto-voting threshold updated from " # Nat64.toText(oldThreshold) # "s to " # Nat64.toText(thresholdSeconds) # "s by " # Principal.toText(caller), "setAutoVotingThresholdSeconds");
  };

  // Periodic Timer System - Master timer that orchestrates all automated processes

  // Get the current periodic timer interval in seconds
  public query func getPeriodicTimerIntervalSeconds() : async Nat64 {
    periodicTimerIntervalSeconds;
  };

  // Set the periodic timer interval in seconds (admin only)
  public shared ({ caller }) func setPeriodicTimerIntervalSeconds(intervalSeconds : Nat64) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("PeriodicTimer", "Unauthorized caller trying to set periodic timer interval: " # Principal.toText(caller), "setPeriodicTimerIntervalSeconds");
      return;
    };

    if (intervalSeconds == 0) {
      logger.error("PeriodicTimer", "Invalid interval: cannot be zero", "setPeriodicTimerIntervalSeconds");
      return;
    };

    // Validate that auto-voting threshold is larger than timer interval
    if (autoVotingThresholdSeconds <= intervalSeconds) {
      logger.error("PeriodicTimer", "Invalid interval: auto-voting threshold (" # Nat64.toText(autoVotingThresholdSeconds) # "s) must be larger than timer interval (" # Nat64.toText(intervalSeconds) # "s)", "setPeriodicTimerIntervalSeconds");
      return;
    };

    let oldInterval = periodicTimerIntervalSeconds;
    periodicTimerIntervalSeconds := intervalSeconds;
    
    logger.info("PeriodicTimer", "Periodic timer interval updated from " # Nat64.toText(oldInterval) # "s to " # Nat64.toText(intervalSeconds) # "s by " # Principal.toText(caller), "setPeriodicTimerIntervalSeconds");
  };

  // Get the current proposer subaccount
  public query func getProposerSubaccount() : async Blob {
    proposerSubaccount;
  };

  // Set the proposer subaccount (admin only)
  public shared ({ caller }) func setProposerSubaccount(subaccount : Blob) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("Config", "Unauthorized caller trying to set proposer subaccount: " # Principal.toText(caller), "setProposerSubaccount");
      return;
    };

    proposerSubaccount := subaccount;
    logger.info("Config", "Proposer subaccount updated by " # Principal.toText(caller), "setProposerSubaccount");
  };

  // Get the current TACO DAO neuron ID
  public query func getTacoDAONeuronId() : async NNSTypes.NeuronId {
    taco_dao_neuron_id;
  };

  // Get SNS proposal ID for a given NNS proposal ID
  public query func getSNSProposalIdForNNS(nnsProposalId : Nat64) : async ?Nat64 {
    Map.get(copiedNNSProposals, Map.n64hash, nnsProposalId);
  };

  // Set the TACO DAO neuron ID (admin only)
  public shared ({ caller }) func setTacoDAONeuronId(neuronId : Nat64) : async () {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("Config", "Unauthorized caller trying to set TACO DAO neuron ID: " # Principal.toText(caller), "setTacoDAONeuronId");
      return;
    };

    let oldNeuronId = taco_dao_neuron_id.id;
    taco_dao_neuron_id := { id = neuronId };
    logger.info("Config", "TACO DAO neuron ID updated from " # Nat64.toText(oldNeuronId) # " to " # Nat64.toText(neuronId) # " by " # Principal.toText(caller), "setTacoDAONeuronId");
  };

  // Get periodic timer status information
  public query func getPeriodicTimerStatus() : async {
    is_running : Bool;
    timer_id : ?Nat;
    last_run_time : ?Nat64;
    next_run_time : ?Nat64;
    interval_seconds : Nat64;
  } {
    {
      is_running = switch (periodicTimerId) { case (?_) { true }; case (null) { false }; };
      timer_id = periodicTimerId;
      last_run_time = periodicTimerLastRunTime;
      next_run_time = periodicTimerNextRunTime;
      interval_seconds = periodicTimerIntervalSeconds;
    };
  };

  // Start the periodic timer (admin only)
  public shared ({ caller }) func startPeriodicTimer() : async Bool {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("PeriodicTimer", "Unauthorized caller trying to start periodic timer: " # Principal.toText(caller), "startPeriodicTimer");
      return false;
    };

    switch (periodicTimerId) {
      case (?_) {
        logger.warn("PeriodicTimer", "Periodic timer is already running, ignoring start request from " # Principal.toText(caller), "startPeriodicTimer");
        return false;
      };
      case (null) {
        logger.info("PeriodicTimer", "Starting periodic timer (interval: " # Nat64.toText(periodicTimerIntervalSeconds) # "s) by " # Principal.toText(caller), "startPeriodicTimer");
        
        // Schedule first execution immediately
        let timerId = Timer.setTimer<system>(#seconds(0), func() : async () {
          await executePeriodicTimerTick();
        });
        
        periodicTimerId := ?timerId;
        let currentTime = get_current_timestamp() / 1_000_000_000; // Convert to seconds
        periodicTimerNextRunTime := ?currentTime;
        
        true;
      };
    };
  };

  // Stop the periodic timer (admin only)
  public shared ({ caller }) func stopPeriodicTimer() : async Bool {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("PeriodicTimer", "Unauthorized caller trying to stop periodic timer: " # Principal.toText(caller), "stopPeriodicTimer");
      return false;
    };

    switch (periodicTimerId) {
      case (null) {
        logger.info("PeriodicTimer", "Stop requested by " # Principal.toText(caller) # " but periodic timer was not running", "stopPeriodicTimer");
        false;
      };
      case (?timerId) {
        Timer.cancelTimer(timerId);
        periodicTimerId := null;
        periodicTimerNextRunTime := null;
        
        logger.info("PeriodicTimer", "Periodic timer stopped by " # Principal.toText(caller), "stopPeriodicTimer");
        true;
      };
    };
  };

  // Internal function that executes on each periodic timer tick
  private func executePeriodicTimerTick() : async () {
    let currentTime = get_current_timestamp() / 1_000_000_000; // Convert to seconds
    periodicTimerLastRunTime := ?currentTime;
    
    logger.info("PeriodicTimer", "Executing periodic timer tick", "executePeriodicTimerTick");

    try {
      // 1. Start auto-processing NNS proposals (via 0-second timer) if not already running
      if (not isAutoProcessingNNSProposals) {
        let processTimerId = Timer.setTimer<system>(#seconds(0), func() : async () {
          let _ = await startAutoProcessNNSProposals(); // Uses stable proposer subaccount
        });
        ignore processTimerId;
        logger.info("PeriodicTimer", "Started auto-processing NNS proposals", "executePeriodicTimerTick");
      } else {
        logger.info("PeriodicTimer", "Auto-processing NNS proposals already running, skipping", "executePeriodicTimerTick");
      };
      
      // 2. Start auto-voting on urgent proposals (via 0-second timer) if not already running
      if (not isAutoVotingOnUrgentProposals) {
        let voteTimerId = Timer.setTimer<system>(#seconds(0), func() : async () {
          let _ = await startAutoVoteOnUrgentProposals();
        });
        ignore voteTimerId;
        logger.info("PeriodicTimer", "Started auto-voting on urgent proposals", "executePeriodicTimerTick");
      } else {
        logger.info("PeriodicTimer", "Auto-voting on urgent proposals already running, skipping", "executePeriodicTimerTick");
      };
      
      logger.info("PeriodicTimer", "Scheduled NNS processing and urgent voting tasks", "executePeriodicTimerTick");
      
    } catch (error) {
      logger.error("PeriodicTimer", "Error during periodic timer execution: " # Error.message(error), "executePeriodicTimerTick");
    };

    // 3. Schedule next execution
    let nextRunTime = currentTime + periodicTimerIntervalSeconds;
    periodicTimerNextRunTime := ?nextRunTime;
    
    let timerId = Timer.setTimer<system>(#seconds(Nat64.toNat(periodicTimerIntervalSeconds)), func() : async () {
      await executePeriodicTimerTick();
    });
    
    periodicTimerId := ?timerId;
    
    logger.info("PeriodicTimer", "Scheduled next periodic timer execution in " # Nat64.toText(periodicTimerIntervalSeconds) # "s (at timestamp " # Nat64.toText(nextRunTime) # ")", "executePeriodicTimerTick");
  };

  // DAO Voting System Functions

  // Submit votes for a copied SNS proposal using TACO neurons
  public shared ({ caller }) func submitDAOVotes(
    snsProposalId : Nat64,
    neuronIds : [Blob],
    decision : T.DAOVoteDecision
  ) : async Result.Result<{
    successful_votes : Nat;
    skipped_already_voted : Nat;
    skipped_no_access : Nat;
    total_voting_power : Nat;
  }, Text> {
    logger.info("DAOVoting", "Vote submission by " # Principal.toText(caller) # " for SNS proposal " # Nat64.toText(snsProposalId) # " with " # Nat.toText(neuronIds.size()) # " neurons", "submitDAOVotes");

    // Check if this SNS proposal is in our copied proposals list
    let correspondingNNSProposal = switch (findNNSProposalForSNS(snsProposalId)) {
      case (null) {
        logger.warn("DAOVoting", "SNS proposal " # Nat64.toText(snsProposalId) # " not found in copied proposals", "submitDAOVotes");
        return #err("SNS proposal not found in copied proposals list");
      };
      case (?nnsId) { nnsId };
    };

    // Check if DAO has already voted on the corresponding NNS proposal
    switch (Map.get(daoVotedNNSProposals2, n64hash, correspondingNNSProposal)) {
      case (?voteRecord) {
        logger.warn("DAOVoting", "DAO has already voted on NNS proposal " # Nat64.toText(correspondingNNSProposal) # " (" # voteRecord.dao_decision # ")", "submitDAOVotes");
        return #err("DAO has already voted on this NNS proposal");
      };
      case (_) { /* Continue */ };
    };

    // Get the caller's neurons from SNS governance to verify access
    let callerNeurons = try {
      let response = await sns_gov_canister.list_neurons({
        of_principal = ?caller;
        limit = 1000; // High limit to get all caller's neurons
        start_page_at = null;
      });
      response.neurons;
    } catch (error) {
      logger.error("DAOVoting", "Failed to fetch caller's neurons: " # Error.message(error), "submitDAOVotes");
      return #err("Failed to verify neuron access");
    };

    // Create a set of caller's neuron IDs for quick lookup
    var callerNeuronIds = Map.new<Blob, Bool>();
    for (neuron in callerNeurons.vals()) {
      switch (neuron.id) {
        case (?neuronId) {
          Map.set(callerNeuronIds, bhash, neuronId.id, true);
        };
        case (null) { /* Skip neurons without ID */ };
      };
    };

    // Get or create the votes map for this SNS proposal
    let proposalVotes = switch (Map.get(daoVotes, n64hash, snsProposalId)) {
      case (?existing) { existing };
      case (null) { 
        let newVotesMap = Map.new<Blob, T.DAOVote>();
        Map.set(daoVotes, n64hash, snsProposalId, newVotesMap);
        newVotesMap;
      };
    };

    var successfulVotes : Nat = 0;
    var skippedAlreadyVoted : Nat = 0;
    var skippedNoAccess : Nat = 0;
    var totalVotingPower : Nat = 0;
    let currentTime = get_current_timestamp();

    // Process each neuron ID
    for (neuronId in neuronIds.vals()) {
      // Check if this neuron has already voted
      switch (Map.get(proposalVotes, bhash, neuronId)) {
        case (?existingVote) {
          skippedAlreadyVoted += 1;
          logger.info("DAOVoting", "Neuron " # debug_show(neuronId) # " has already voted", "submitDAOVotes");
        };
        case (null) {
          // Check if caller has access to this neuron
          switch (Map.get(callerNeuronIds, bhash, neuronId)) {
            case (?true) {
              // Find the neuron to calculate voting power
              let neuronOpt = Array.find<T.Neuron>(callerNeurons, func(n) {
                switch (n.id) {
                  case (?nId) { nId.id == neuronId };
                  case (null) { false };
                };
              });

              switch (neuronOpt) {
                case (?neuron) {
                  // Calculate voting power
                  let neuronDetails : T.NeuronDetails = {
                    id = neuron.id;
                    staked_maturity_e8s_equivalent = neuron.staked_maturity_e8s_equivalent;
                    cached_neuron_stake_e8s = neuron.cached_neuron_stake_e8s;
                    aging_since_timestamp_seconds = neuron.aging_since_timestamp_seconds;
                    dissolve_state = neuron.dissolve_state;
                    voting_power_percentage_multiplier = neuron.voting_power_percentage_multiplier;
                  };

                  let votingPower = vp_calc.getVotingPower(neuronDetails);

                  // Create and store the vote
                  let vote : T.DAOVote = {
                    decision = decision;
                    voting_power = votingPower;
                    timestamp = currentTime;
                    voter_principal = caller;
                  };

                  Map.set(proposalVotes, bhash, neuronId, vote);
                  successfulVotes += 1;
                  totalVotingPower += votingPower;

                  logger.info("DAOVoting", "Recorded vote for neuron with " # Nat.toText(votingPower) # " VP", "submitDAOVotes");
                };
                case (null) {
                  skippedNoAccess += 1;
                  logger.warn("DAOVoting", "Neuron details not found for ID: " # debug_show(neuronId), "submitDAOVotes");
                };
              };
            };
            case (_) {
              skippedNoAccess += 1;
              logger.warn("DAOVoting", "Caller does not have access to neuron: " # debug_show(neuronId), "submitDAOVotes");
            };
          };
        };
      };
    };

    let result = {
      successful_votes = successfulVotes;
      skipped_already_voted = skippedAlreadyVoted;
      skipped_no_access = skippedNoAccess;
      total_voting_power = totalVotingPower;
    };

    logger.info("DAOVoting", "Vote submission completed: " # Nat.toText(successfulVotes) # " successful, " # Nat.toText(skippedAlreadyVoted) # " already voted, " # Nat.toText(skippedNoAccess) # " no access, " # Nat.toText(totalVotingPower) # " total VP", "submitDAOVotes");

    #ok(result);
  };

  // Helper function to find NNS proposal ID for a given SNS proposal ID
  private func findNNSProposalForSNS(snsProposalId : Nat64) : ?Nat64 {
    for ((nnsId, snsId) in Map.entries(copiedNNSProposals)) {
      if (snsId == snsProposalId) {
        return ?nnsId;
      };
    };
    null;
  };

  // Public function to find NNS proposal ID for a given SNS proposal ID
  public query func getNNSProposalIdForSNS(snsProposalId : Nat64) : async ?Nat64 {
    for ((nnsId, snsId) in Map.entries(copiedNNSProposals)) {
      if (snsId == snsProposalId) {
        return ?nnsId;
      };
    };
    null;
  };

  // Get vote tally for a specific SNS proposal
  public query func getDAOVoteTally(snsProposalId : Nat64) : async ?{
    adopt_votes : Nat;
    reject_votes : Nat;
    adopt_voting_power : Nat;
    reject_voting_power : Nat;
    total_votes : Nat;
    total_voting_power : Nat;
  } {
    switch (Map.get(daoVotes, n64hash, snsProposalId)) {
      case (null) { null };
      case (?proposalVotes) {
        var adoptVotes : Nat = 0;
        var rejectVotes : Nat = 0;
        var adoptVotingPower : Nat = 0;
        var rejectVotingPower : Nat = 0;

        for ((_, vote) in Map.entries(proposalVotes)) {
          switch (vote.decision) {
            case (#Adopt) {
              adoptVotes += 1;
              adoptVotingPower += vote.voting_power;
            };
            case (#Reject) {
              rejectVotes += 1;
              rejectVotingPower += vote.voting_power;
            };
          };
        };

        ?{
          adopt_votes = adoptVotes;
          reject_votes = rejectVotes;
          adopt_voting_power = adoptVotingPower;
          reject_voting_power = rejectVotingPower;
          total_votes = adoptVotes + rejectVotes;
          total_voting_power = adoptVotingPower + rejectVotingPower;
        };
      };
    };
  };

  // Get all votes for a specific SNS proposal (admin only)
  public query ({ caller }) func getDAOVotesForProposal(snsProposalId : Nat64) : async [(Blob, T.DAOVote)] {
    switch (Map.get(daoVotes, n64hash, snsProposalId)) {
      case (null) { [] };
      case (?proposalVotes) { Iter.toArray(Map.entries(proposalVotes)) };
    };
  };

  // Check if a specific neuron has voted on a proposal
  public query func hasNeuronVoted(snsProposalId : Nat64, neuronId : Blob) : async ?T.DAOVote {
    switch (Map.get(daoVotes, n64hash, snsProposalId)) {
      case (null) { null };
      case (?proposalVotes) { Map.get(proposalVotes, bhash, neuronId) };
    };
  };

  // Get list of SNS proposals available for DAO voting
  public query func getVotableProposals() : async [(Nat64, Nat64)] {
    // Return SNS proposals that haven't been voted on by DAO yet
    Array.filter<(Nat64, Nat64)>(
      Iter.toArray(Map.entries(copiedNNSProposals)),
      func((nnsId, snsId)) {
        switch (Map.get(daoVotedNNSProposals2, n64hash, nnsId)) {
          case (?voteRecord) { false }; // Already voted
          case (_) { true }; // Available for voting
        };
      }
    );
  };

  // Helper function to format time remaining in a human-readable way
  private func formatTimeRemaining(seconds: Int64) : Text {
    let absSeconds = Int64.abs(seconds);
    let days = absSeconds / 86400;
    let hours = (absSeconds % 86400) / 3600;
    let minutes = (absSeconds % 3600) / 60;
    let remainingSeconds = absSeconds % 60;
    
    if (seconds < 0) {
      "Expired " # Int64.toText(days) # "d " # Int64.toText(hours) # "h " # Int64.toText(minutes) # "m ago"
    } else if (days > 0) {
      Int64.toText(days) # "d " # Int64.toText(hours) # "h " # Int64.toText(minutes) # "m"
    } else if (hours > 0) {
      Int64.toText(hours) # "h " # Int64.toText(minutes) # "m"
    } else if (minutes > 0) {
      Int64.toText(minutes) # "m " # Int64.toText(remainingSeconds) # "s"
    } else {
      Int64.toText(remainingSeconds) # "s"
    }
  };

  // Get list of SNS proposals available for DAO voting with time remaining information
  public shared ({ caller }) func getVotableProposalsWithTimeLeft() : async [{
    nns_proposal_id : Nat64;
    sns_proposal_id : Nat64;
    time_remaining_seconds : ?Int64; // null if no deadline, negative if expired
    deadline_timestamp_seconds : ?Nat64;
    proposal_timestamp_seconds : ?Nat64;
    is_expired : Bool;
  }] {
    logger.info("DAOVoting", "Fetching votable proposals with time left by " # Principal.toText(caller), "getVotableProposalsWithTimeLeft");
    
    // Get current timestamp in seconds
    let currentTimestamp = get_current_timestamp() / 1_000_000_000; // Convert nanoseconds to seconds
    
    // Get all votable proposals
    let votableProposals = Array.filter<(Nat64, Nat64)>(
      Iter.toArray(Map.entries(copiedNNSProposals)),
      func((nnsId, snsId)) {
        switch (Map.get(daoVotedNNSProposals2, n64hash, nnsId)) {
          case (?voteRecord) { false }; // Already voted
          case (_) { true }; // Available for voting
        };
      }
    );
    
    let results = Vector.new<{
      nns_proposal_id : Nat64;
      sns_proposal_id : Nat64;
      time_remaining_seconds : ?Int64;
      deadline_timestamp_seconds : ?Nat64;
      proposal_timestamp_seconds : ?Nat64;
      is_expired : Bool;
    }>();
    
    // Fetch proposal info for each votable proposal
    for ((nnsId, snsId) in votableProposals.vals()) {
      try {
        let proposalInfoOpt = await nns_gov_canister.get_proposal_info(nnsId);
        
        switch (proposalInfoOpt) {
          case (?proposalInfo) {
            let timeRemaining = switch (proposalInfo.deadline_timestamp_seconds) {
              case (?deadline) {
                let remaining = Int64.fromNat64(deadline) - Int64.fromNat64(currentTimestamp);
                ?remaining;
              };
              case (null) { null }; // No deadline set
            };
            
            let isExpired = switch (proposalInfo.deadline_timestamp_seconds) {
              case (?deadline) { currentTimestamp >= deadline };
              case (null) { false }; // No deadline, so not expired
            };
            
            let proposalResult = {
              nns_proposal_id = nnsId;
              sns_proposal_id = snsId;
              time_remaining_seconds = timeRemaining;
              deadline_timestamp_seconds = proposalInfo.deadline_timestamp_seconds;
              proposal_timestamp_seconds = ?proposalInfo.proposal_timestamp_seconds;
              is_expired = isExpired;
            };
            
            Vector.add(results, proposalResult);
            
            logger.info("DAOVoting", "NNS proposal " # Nat64.toText(nnsId) # " - Time remaining: " # 
              (switch (timeRemaining) { 
                case (?t) { formatTimeRemaining(t) }; 
                case (null) { "no deadline" }; 
              }) # ", Expired: " # Bool.toText(isExpired), "getVotableProposalsWithTimeLeft");
          };
          case (null) {
            // Proposal not found, include it but mark as potentially expired/invalid
            let proposalResult = {
              nns_proposal_id = nnsId;
              sns_proposal_id = snsId;
              time_remaining_seconds = null;
              deadline_timestamp_seconds = null;
              proposal_timestamp_seconds = null;
              is_expired = true; // Assume expired if we can't fetch it
            };
            
            Vector.add(results, proposalResult);
            logger.warn("DAOVoting", "NNS proposal " # Nat64.toText(nnsId) # " not found in NNS governance", "getVotableProposalsWithTimeLeft");
          };
        };
      } catch (error) {
        // Handle network errors or other issues
        let proposalResult = {
          nns_proposal_id = nnsId;
          sns_proposal_id = snsId;
          time_remaining_seconds = null;
          deadline_timestamp_seconds = null;
          proposal_timestamp_seconds = null;
          is_expired = true; // Assume expired on error
        };
        
        Vector.add(results, proposalResult);
        logger.error("DAOVoting", "Error fetching NNS proposal " # Nat64.toText(nnsId) # ": " # Error.message(error), "getVotableProposalsWithTimeLeft");
      };
    };
    
    let resultsArray = Vector.toArray(results);
    logger.info("DAOVoting", "Retrieved " # Nat.toText(resultsArray.size()) # " votable proposals with timing info", "getVotableProposalsWithTimeLeft");
    resultsArray;
  };

  // Get votable proposals that are expiring within the specified number of hours (urgent voting needed)
  public shared ({ caller }) func getUrgentVotableProposals(hoursThreshold : Nat64) : async [{
    nns_proposal_id : Nat64;
    sns_proposal_id : Nat64;
    time_remaining_seconds : ?Int64;
    deadline_timestamp_seconds : ?Nat64;
    proposal_timestamp_seconds : ?Nat64;
    is_expired : Bool;
  }] {
    logger.info("DAOVoting", "Fetching urgent votable proposals (threshold: " # Nat64.toText(hoursThreshold) # "h) by " # Principal.toText(caller), "getUrgentVotableProposals");
    
    let allProposalsWithTime = await getVotableProposalsWithTimeLeft();
    let thresholdSeconds = Int64.fromNat64(hoursThreshold * 3600); // Convert hours to seconds
    
    let urgentProposals = Array.filter<{
      nns_proposal_id : Nat64;
      sns_proposal_id : Nat64;
      time_remaining_seconds : ?Int64;
      deadline_timestamp_seconds : ?Nat64;
      proposal_timestamp_seconds : ?Nat64;
      is_expired : Bool;
    }>(allProposalsWithTime, func(proposal) {
      switch (proposal.time_remaining_seconds) {
        case (?timeRemaining) {
          // Include if expired or expiring within threshold
          timeRemaining <= thresholdSeconds
        };
        case (null) { false }; // No deadline, not urgent
      }
    });
    
    logger.info("DAOVoting", "Found " # Nat.toText(urgentProposals.size()) # " urgent proposals out of " # Nat.toText(allProposalsWithTime.size()) # " total", "getUrgentVotableProposals");
    urgentProposals;
  };

  // Automatically vote on NNS proposals that are expiring within the specified time threshold
  public shared ({ caller }) func autoVoteOnUrgentProposals(timeThresholdSeconds : Nat64, maxProposalsToVote : Nat) : async Result.Result<{
    total_proposals_checked : Nat;
    urgent_proposals_found : Nat;
    max_proposals_limit : Nat;
    votes_attempted : Nat;
    votes_successful : Nat;
    votes_failed : Nat;
    votes_already_voted : Nat;
    votes_no_dao_votes : Nat;
    results : [{
      nns_proposal_id : Nat64;
      sns_proposal_id : Nat64;
      time_remaining_seconds : ?Int64;
      vote_result : {
        #success : {
          dao_decision : Text;
          adopt_vp : Nat;
          reject_vp : Nat;
          total_vp : Nat;
        };
        #already_voted : Text;
        #no_dao_votes : Text;
        #error : Text;
      };
    }];
  }, Text> {
    // Authorization check - only master admin, controllers, or DAO backend can trigger auto-voting
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal)) {
      logger.warn("DAOVoting", "Unauthorized caller trying to trigger auto-voting: " # Principal.toText(caller), "autoVoteOnUrgentProposals");
      return #err("Unauthorized: Only admins can trigger automated NNS voting");
    };

    logger.info("DAOVoting", "Starting automated voting for proposals expiring within " # Nat64.toText(timeThresholdSeconds) # " seconds (max " # Nat.toText(maxProposalsToVote) # " proposals), triggered by " # Principal.toText(caller), "autoVoteOnUrgentProposals");

    try {
      // Get all votable proposals with timing information
      let allProposalsWithTime = await getVotableProposalsWithTimeLeft();
      let thresholdSecondsInt64 = Int64.fromNat64(timeThresholdSeconds);
      
      // Filter for urgent proposals
      let urgentProposals = Array.filter<{
        nns_proposal_id : Nat64;
        sns_proposal_id : Nat64;
        time_remaining_seconds : ?Int64;
        deadline_timestamp_seconds : ?Nat64;
        proposal_timestamp_seconds : ?Nat64;
        is_expired : Bool;
      }>(allProposalsWithTime, func(proposal) {
        switch (proposal.time_remaining_seconds) {
          case (?timeRemaining) {
            // Include if time remaining is less than or equal to threshold
            // This includes expired proposals (negative time)
            timeRemaining <= thresholdSecondsInt64
          };
          case (null) { false }; // No deadline, not urgent
        }
      });

      // Apply the limit to the number of proposals to process
      let limitedUrgentProposals = if (urgentProposals.size() <= maxProposalsToVote) {
        urgentProposals;
      } else {
        // Take only the first maxProposalsToVote proposals
        // Note: In the future, we could add sorting by time remaining to prioritize most urgent
        Array.subArray(urgentProposals, 0, maxProposalsToVote);
      };

      logger.info("DAOVoting", "📊 Analysis: Total=" # Nat.toText(allProposalsWithTime.size()) # ", Urgent=" # Nat.toText(urgentProposals.size()) # ", WillProcess=" # Nat.toText(limitedUrgentProposals.size()) # " (max=" # Nat.toText(maxProposalsToVote) # ", threshold=" # Nat64.toText(timeThresholdSeconds) # "s)", "autoVoteOnUrgentProposals");

      // Early exit if no urgent proposals to process
      if (limitedUrgentProposals.size() == 0) {
        let finalResults = {
          total_proposals_checked = allProposalsWithTime.size();
          urgent_proposals_found = urgentProposals.size();
          max_proposals_limit = maxProposalsToVote;
          votes_attempted = 0;
          votes_successful = 0;
          votes_failed = 0;
          votes_already_voted = 0;
          votes_no_dao_votes = 0;
          results = [];
        };

        logger.info("DAOVoting", "⏭️  No urgent proposals to process - early exit", "autoVoteOnUrgentProposals");
        return #ok(finalResults);
      };

      let results = Vector.new<{
        nns_proposal_id : Nat64;
        sns_proposal_id : Nat64;
        time_remaining_seconds : ?Int64;
        vote_result : {
          #success : {
            dao_decision : Text;
            adopt_vp : Nat;
            reject_vp : Nat;
            total_vp : Nat;
          };
          #already_voted : Text;
          #no_dao_votes : Text;
          #error : Text;
        };
      }>();

      var votesAttempted : Nat = 0;
      var votesSuccessful : Nat = 0;
      var votesFailed : Nat = 0;
      var votesAlreadyVoted : Nat = 0;
      var votesNoDAOVotes : Nat = 0;

      // Process each urgent proposal (limited by maxProposalsToVote)
      for (proposal in limitedUrgentProposals.vals()) {
        let timeRemainingText = switch (proposal.time_remaining_seconds) {
          case (?t) { formatTimeRemaining(t) };
          case (null) { "no deadline" };
        };

        // Check if we've already attempted this NNS proposal in the current round
        let shouldSkip = switch (Map.get(attemptedProposalsThisRound, n64hash, proposal.nns_proposal_id)) {
          case (?roundId) {
            if (roundId == autoVotingRoundCounter) {
              logger.info("DAOVoting", "Skipping NNS proposal " # Nat64.toText(proposal.nns_proposal_id) # " - already attempted in round " # Nat64.toText(roundId), "autoVoteOnUrgentProposals");
              true; // Skip this proposal
            } else {
              false; // Different round, can process
            };
          };
          case (null) { false }; // Not attempted yet, can process
        };

        if (not shouldSkip) {
          logger.info("DAOVoting", "Processing urgent proposal - NNS: " # Nat64.toText(proposal.nns_proposal_id) # ", SNS: " # Nat64.toText(proposal.sns_proposal_id) # ", Time remaining: " # timeRemainingText, "autoVoteOnUrgentProposals");

          // Mark this proposal as attempted in the current round
          Map.set(attemptedProposalsThisRound, n64hash, proposal.nns_proposal_id, autoVotingRoundCounter);
          
          votesAttempted += 1;

          // Attempt to vote on this NNS proposal
          let voteResult = await voteOnNNSProposal(proposal.sns_proposal_id);
          
          let resultEntry = switch (voteResult) {
            case (#ok(voteData)) {
              votesSuccessful += 1;
              logger.info("DAOVoting", "Successfully voted " # voteData.dao_decision # " on NNS proposal " # Nat64.toText(proposal.nns_proposal_id) # " with " # Nat.toText(voteData.total_vp) # " VP", "autoVoteOnUrgentProposals");
              {
                nns_proposal_id = proposal.nns_proposal_id;
                sns_proposal_id = proposal.sns_proposal_id;
                time_remaining_seconds = proposal.time_remaining_seconds;
                vote_result = #success({
                  dao_decision = voteData.dao_decision;
                  adopt_vp = voteData.adopt_vp;
                  reject_vp = voteData.reject_vp;
                  total_vp = voteData.total_vp;
                });
              };
            };
            case (#err(errorMsg)) {
              if (errorMsg == "DAO has already voted on this NNS proposal") {
                votesAlreadyVoted += 1;
                logger.info("DAOVoting", "NNS proposal " # Nat64.toText(proposal.nns_proposal_id) # " already voted on", "autoVoteOnUrgentProposals");
                {
                  nns_proposal_id = proposal.nns_proposal_id;
                  sns_proposal_id = proposal.sns_proposal_id;
                  time_remaining_seconds = proposal.time_remaining_seconds;
                  vote_result = #already_voted(errorMsg);
                };
              } else if (errorMsg == "No DAO votes found for this proposal") {
                votesNoDAOVotes += 1;
                logger.warn("DAOVoting", "No DAO member votes found for SNS proposal " # Nat64.toText(proposal.sns_proposal_id), "autoVoteOnUrgentProposals");
                {
                  nns_proposal_id = proposal.nns_proposal_id;
                  sns_proposal_id = proposal.sns_proposal_id;
                  time_remaining_seconds = proposal.time_remaining_seconds;
                  vote_result = #no_dao_votes(errorMsg);
                };
              } else {
                votesFailed += 1;
                logger.error("DAOVoting", "Failed to vote on NNS proposal " # Nat64.toText(proposal.nns_proposal_id) # ": " # errorMsg, "autoVoteOnUrgentProposals");
                {
                  nns_proposal_id = proposal.nns_proposal_id;
                  sns_proposal_id = proposal.sns_proposal_id;
                  time_remaining_seconds = proposal.time_remaining_seconds;
                  vote_result = #error(errorMsg);
                };
              };
            };
          };

          Vector.add(results, resultEntry);
        };
      };

      let finalResults = {
        total_proposals_checked = allProposalsWithTime.size();
        urgent_proposals_found = urgentProposals.size();
        max_proposals_limit = maxProposalsToVote;
        votes_attempted = votesAttempted;
        votes_successful = votesSuccessful;
        votes_failed = votesFailed;
        votes_already_voted = votesAlreadyVoted;
        votes_no_dao_votes = votesNoDAOVotes;
        results = Vector.toArray(results);
      };

      logger.info("DAOVoting", "Automated voting completed - Checked: " # Nat.toText(finalResults.total_proposals_checked) # 
        ", Urgent: " # Nat.toText(finalResults.urgent_proposals_found) # 
        ", Limit: " # Nat.toText(finalResults.max_proposals_limit) # 
        ", Attempted: " # Nat.toText(finalResults.votes_attempted) # 
        ", Successful: " # Nat.toText(finalResults.votes_successful) # 
        ", Failed: " # Nat.toText(finalResults.votes_failed) # 
        ", Already voted: " # Nat.toText(finalResults.votes_already_voted) # 
        ", No DAO votes: " # Nat.toText(finalResults.votes_no_dao_votes), "autoVoteOnUrgentProposals");

      #ok(finalResults);

    } catch (error) {
      let errorMsg = "Exception during automated voting: " # Error.message(error);
      logger.error("DAOVoting", errorMsg, "autoVoteOnUrgentProposals");
      #err(errorMsg);
    };
  };

  // Convenience function to automatically vote on proposals expiring within 1 hour (3600 seconds)
  // This aligns with the specification requirement to vote when "1 hour remains before NNS voting closes"
  // Default limit of 10 proposals to prevent instruction limit issues
  public shared ({ caller }) func autoVoteOnProposalsExpiringWithinOneHour() : async Result.Result<{
    total_proposals_checked : Nat;
    urgent_proposals_found : Nat;
    max_proposals_limit : Nat;
    votes_attempted : Nat;
    votes_successful : Nat;
    votes_failed : Nat;
    votes_already_voted : Nat;
    votes_no_dao_votes : Nat;
    results : [{
      nns_proposal_id : Nat64;
      sns_proposal_id : Nat64;
      time_remaining_seconds : ?Int64;
      vote_result : {
        #success : {
          dao_decision : Text;
          adopt_vp : Nat;
          reject_vp : Nat;
          total_vp : Nat;
        };
        #already_voted : Text;
        #no_dao_votes : Text;
        #error : Text;
      };
    }];
  }, Text> {
    logger.info("DAOVoting", "Auto-voting on proposals expiring within 1 hour (limit: 10), triggered by " # Principal.toText(caller), "autoVoteOnProposalsExpiringWithinOneHour");
    await autoVoteOnUrgentProposals(3600, 10); // 1 hour = 3600 seconds, max 10 proposals
  };

  // Check if DAO has already voted on an NNS proposal
  public query func hasDAOVoted(nnsProposalId : Nat64) : async Bool {
    switch (Map.get(daoVotedNNSProposals2, n64hash, nnsProposalId)) {
      case (?voteRecord) { true };
      case (_) { false };
    };
  };

  // Get detailed DAO vote record for an NNS proposal
  public query func getDAOVoteRecord(nnsProposalId : Nat64) : async ?T.DAONNSVoteRecord {
    Map.get(daoVotedNNSProposals2, n64hash, nnsProposalId);
  };

  // Mark an NNS proposal as voted by DAO (admin only)
  public shared ({ caller }) func markNNSProposalAsVoted(nnsProposalId : Nat64) : async Bool {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("DAOVoting", "Unauthorized caller trying to mark NNS proposal as voted: " # Principal.toText(caller), "markNNSProposalAsVoted");
      return false;
    };

    // Create a manual vote record (admin override)
    let manualVoteRecord : T.DAONNSVoteRecord = {
      nns_proposal_id = nnsProposalId;
      dao_decision = "Manual";
      adopt_vp = 0;
      reject_vp = 0;
      total_vp = 0;
      vote_timestamp = Nat64.fromNat(Int.abs(Time.now()));
      voted_by_principal = caller;
    };
    
    Map.set(daoVotedNNSProposals2, n64hash, nnsProposalId, manualVoteRecord);
    logger.info("DAOVoting", "NNS proposal " # Nat64.toText(nnsProposalId) # " manually marked as voted by " # Principal.toText(caller), "markNNSProposalAsVoted");
    true;
  };

  // Vote on NNS proposal based on DAO collective decision
  public shared ({ caller }) func voteOnNNSProposal(snsProposalId : Nat64) : async Result.Result<{
    nns_proposal_id : Nat64;
    dao_decision : Text; // "Adopt" or "Reject"
    adopt_vp : Nat;
    reject_vp : Nat;
    total_vp : Nat;
  }, Text> {
    // Authorization check
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal)) {
      logger.warn("DAOVoting", "Unauthorized caller trying to vote on NNS proposal: " # Principal.toText(caller), "voteOnNNSProposal");
      return #err("Unauthorized: Only admins can trigger NNS voting");
    };

    logger.info("DAOVoting", "Starting NNS vote for SNS proposal " # Nat64.toText(snsProposalId) # " by " # Principal.toText(caller), "voteOnNNSProposal");

    // Find the corresponding NNS proposal ID
    let nnsProposalId = switch (findNNSProposalForSNS(snsProposalId)) {
      case (null) {
        logger.warn("DAOVoting", "SNS proposal " # Nat64.toText(snsProposalId) # " not found in copied proposals", "voteOnNNSProposal");
        return #err("SNS proposal not found in copied proposals list");
      };
      case (?id) { id };
    };

    // Check if DAO has already voted on this NNS proposal
    switch (Map.get(daoVotedNNSProposals2, n64hash, nnsProposalId)) {
      case (?voteRecord) {
        logger.warn("DAOVoting", "DAO has already voted on NNS proposal " # Nat64.toText(nnsProposalId) # " (" # voteRecord.dao_decision # ")", "voteOnNNSProposal");
        return #err("DAO has already voted on this NNS proposal");
      };
      case (_) { /* Continue */ };
    };

    // Get DAO vote tally for this SNS proposal
    let voteTally = switch (Map.get(daoVotes, n64hash, snsProposalId)) {
      case (null) {
        // No DAO votes found - check default behavior
        switch (defaultVoteBehavior) {
          case (#Skip) {
            logger.warn("DAOVoting", "No DAO votes found for SNS proposal " # Nat64.toText(snsProposalId) # " - skipping per default behavior", "voteOnNNSProposal");
            return #err("No DAO votes found for this proposal");
          };
          case (#VoteAdopt) {
            logger.info("DAOVoting", "No DAO votes found for SNS proposal " # Nat64.toText(snsProposalId) # " - using default behavior: Vote Adopt", "voteOnNNSProposal");
            { adopt_vp = 1; reject_vp = 0; total_vp = 1 }; // Use symbolic voting power
          };
          case (#VoteReject) {
            logger.info("DAOVoting", "No DAO votes found for SNS proposal " # Nat64.toText(snsProposalId) # " - using default behavior: Vote Reject", "voteOnNNSProposal");
            { adopt_vp = 0; reject_vp = 1; total_vp = 1 }; // Use symbolic voting power
          };
        };
      };
      case (?proposalVotes) {
        var adoptVP : Nat = 0;
        var rejectVP : Nat = 0;

        for ((neuronId, vote) in Map.entries(proposalVotes)) {
          switch (vote.decision) {
            case (#Adopt) { adoptVP += vote.voting_power };
            case (#Reject) { rejectVP += vote.voting_power };
          };
        };

        { adopt_vp = adoptVP; reject_vp = rejectVP; total_vp = adoptVP + rejectVP };
      };
    };

    // Determine DAO decision based on voting power
    if (voteTally.total_vp == 0) {
      logger.warn("DAOVoting", "No voting power found in DAO votes for SNS proposal " # Nat64.toText(snsProposalId), "voteOnNNSProposal");
      return #err("No voting power in DAO votes");
    };

    // Handle ties and clear winners
    let (daoDecision, nnsVote) = if (voteTally.adopt_vp > voteTally.reject_vp) {
      // Clear Adopt winner
      ("Adopt", 1 : Int32)
    } else if (voteTally.reject_vp > voteTally.adopt_vp) {
      // Clear Reject winner
      ("Reject", 2 : Int32)
    } else {
      // Tie case - use default behavior
      switch (defaultVoteBehavior) {
        case (#Skip) {
          logger.warn("DAOVoting", "Tie in DAO votes for SNS proposal " # Nat64.toText(snsProposalId) # " (Adopt: " # Nat.toText(voteTally.adopt_vp) # " VP, Reject: " # Nat.toText(voteTally.reject_vp) # " VP) - skipping per default behavior", "voteOnNNSProposal");
          return #err("Tie in DAO votes - skipping per default behavior");
        };
        case (#VoteAdopt) {
          logger.info("DAOVoting", "Tie in DAO votes for SNS proposal " # Nat64.toText(snsProposalId) # " (Adopt: " # Nat.toText(voteTally.adopt_vp) # " VP, Reject: " # Nat.toText(voteTally.reject_vp) # " VP) - using default behavior: Vote Adopt", "voteOnNNSProposal");
          ("Adopt", 1 : Int32)
        };
        case (#VoteReject) {
          logger.info("DAOVoting", "Tie in DAO votes for SNS proposal " # Nat64.toText(snsProposalId) # " (Adopt: " # Nat.toText(voteTally.adopt_vp) # " VP, Reject: " # Nat.toText(voteTally.reject_vp) # " VP) - using default behavior: Vote Reject", "voteOnNNSProposal");
          ("Reject", 2 : Int32)
        };
      };
    };

    logger.info("DAOVoting", "DAO decision for NNS proposal " # Nat64.toText(nnsProposalId) # ": " # daoDecision # " (Adopt: " # Nat.toText(voteTally.adopt_vp) # " VP, Reject: " # Nat.toText(voteTally.reject_vp) # " VP)", "voteOnNNSProposal");

    // Vote on the NNS proposal
    try {
      let voteResult = await NNSPropCopy.voteOnNNSProposal(nnsProposalId, nnsVote, nns_governance_canister_id, taco_dao_neuron_id, logger);
      switch (voteResult) {
        case (#ok(_)) {
          // Create detailed vote record
          let voteRecord : T.DAONNSVoteRecord = {
            nns_proposal_id = nnsProposalId;
            dao_decision = daoDecision;
            adopt_vp = voteTally.adopt_vp;
            reject_vp = voteTally.reject_vp;
            total_vp = voteTally.total_vp;
            vote_timestamp = Nat64.fromNat(Int.abs(Time.now()));
            voted_by_principal = caller;
          };
          
          // Mark this NNS proposal as voted with detailed record
          Map.set(daoVotedNNSProposals2, n64hash, nnsProposalId, voteRecord);
          
          logger.info("DAOVoting", "Successfully voted " # daoDecision # " on NNS proposal " # Nat64.toText(nnsProposalId), "voteOnNNSProposal");
          
          #ok({
            nns_proposal_id = nnsProposalId;
            dao_decision = daoDecision;
            adopt_vp = voteTally.adopt_vp;
            reject_vp = voteTally.reject_vp;
            total_vp = voteTally.total_vp;
          });
        };
        case (#err(error)) {
          logger.error("DAOVoting", "Failed to vote on NNS proposal " # Nat64.toText(nnsProposalId) # ": " # debug_show(error), "voteOnNNSProposal");
          #err("Failed to vote on NNS proposal: " # debug_show(error));
        };
      };
    } catch (error) {
      logger.error("DAOVoting", "Exception while voting on NNS proposal " # Nat64.toText(nnsProposalId) # ": " # Error.message(error), "voteOnNNSProposal");
      #err("Exception while voting on NNS proposal: " # Error.message(error));
    };
  };

  // Clear all DAO votes for a specific SNS proposal (admin only)
  public shared ({ caller }) func clearDAOVotesForProposal(snsProposalId : Nat64) : async Nat {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("DAOVoting", "Unauthorized caller trying to clear votes: " # Principal.toText(caller), "clearDAOVotesForProposal");
      return 0;
    };

    switch (Map.get(daoVotes, n64hash, snsProposalId)) {
      case (null) { 0 };
      case (?proposalVotes) {
        let count = Map.size(proposalVotes);
        Map.delete(daoVotes, n64hash, snsProposalId);
        logger.info("DAOVoting", "Cleared " # Nat.toText(count) # " votes for SNS proposal " # Nat64.toText(snsProposalId) # " by " # Principal.toText(caller), "clearDAOVotesForProposal");
        count;
      };
    };
  };

  // Get count of proposals with DAO votes
  public query func getDAOVotingProposalsCount() : async Nat {
    Map.size(daoVotes);
  };

  // Get count of NNS proposals DAO has voted on
  public query func getDAOVotedNNSProposalsCount() : async Nat {
    Map.size(daoVotedNNSProposals2);
  };

  // Clear all DAO voted NNS proposals (admin only)
  public shared ({ caller }) func clearDAOVotedNNSProposals() : async Nat {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("DAOVoting", "Unauthorized caller trying to clear DAO voted NNS proposals: " # Principal.toText(caller), "clearDAOVotedNNSProposals");
      return 0;
    };

    let countBeforeClear = Map.size(daoVotedNNSProposals2);
    daoVotedNNSProposals2 := Map.new<Nat64, T.DAONNSVoteRecord>();
    
    logger.info("DAOVoting", "Cleared " # Nat.toText(countBeforeClear) # " DAO voted NNS proposals by " # Principal.toText(caller), "clearDAOVotedNNSProposals");
    
    countBeforeClear;
  };

  // Clear all stored neuron snapshots and associated data (admin only)
  public shared ({ caller }) func clearNeuronSnapshots() : async {
    snapshots_cleared : Nat;
    neuron_store_entries_cleared : Nat;
    cumulative_values_cleared : Nat;
  } {
    if (not (isMasterAdmin(caller) or Principal.isController(caller) or caller == DAOprincipal or (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa")))) {
      logger.warn("Snapshot", "Unauthorized caller trying to clear neuron snapshots: " # Principal.toText(caller), "clearNeuronSnapshots");
      return {
        snapshots_cleared = 0;
        neuron_store_entries_cleared = 0;
        cumulative_values_cleared = 0;
      };
    };

    // Count items before clearing
    let snapshotsCount = List.size(neuron_snapshots);
    let neuronStoreCount = Map.size(neuronStore);
    let cumulativeValuesCount = Map.size(snapshotCumulativeValues);

    // Clear all snapshot-related data structures
    neuron_snapshots := List.nil();
    Map.clear(neuronStore);
    Map.clear(snapshotCumulativeValues);
    
    // Reset the head ID to 0
    neuron_snapshot_head_id := 0;
    
    logger.info("Snapshot", "Cleared all neuron snapshots: " # Nat.toText(snapshotsCount) # " snapshots, " # 
      Nat.toText(neuronStoreCount) # " neuron store entries, " # Nat.toText(cumulativeValuesCount) # 
      " cumulative values entries by " # Principal.toText(caller), "clearNeuronSnapshots");
    
    {
      snapshots_cleared = snapshotsCount;
      neuron_store_entries_cleared = neuronStoreCount;
      cumulative_values_cleared = cumulativeValuesCount;
    };
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };

  //=========================================================================
  // ARCHIVE CANISTER PROXY API
  // Allows SNS DAO to call archive canister methods through a single proxy
  //=========================================================================

  // Archive canister actor interface (common API)
  type ArchiveActor = actor {
    // Query methods
    getArchiveStatus : shared query () -> async Result.Result<ArchiveTypes.ArchiveStatus, ArchiveTypes.ArchiveError>;
    getArchiveStats : shared query () -> async ArchiveTypes.ArchiveStatus;
    getLogs : shared query (Nat) -> async [Logger.LogEntry];
    getBatchImportStatus : shared query () -> async { isRunning : Bool; intervalSeconds : Nat };
    getTimerStatus : shared query () -> async BatchImportTimer.TimerStatus;
    get_canister_cycles : shared query () -> async { cycles : Nat };
    
    // Update methods
    startBatchImportSystem : shared () -> async Result.Result<Text, Text>;
    stopBatchImportSystem : shared () -> async Result.Result<Text, Text>;
    stopAllTimers : shared () -> async Result.Result<Text, Text>;
    runManualBatchImport : shared () -> async Result.Result<Text, Text>;
    setMaxInnerLoopIterations : shared (Nat) -> async Result.Result<Text, Text>;
    resetImportTimestamps : shared () -> async Result.Result<Text, Text>;
    updateConfig : shared (ArchiveTypes.ArchiveConfig) -> async Result.Result<Text, ArchiveTypes.ArchiveError>;
  };

  // Helper function to get archive actor from principal
  private func getArchiveActor(archivePrincipal : Principal) : ArchiveActor {
    actor (Principal.toText(archivePrincipal)) : ArchiveActor;
  };

  // Validate that the caller is authorized to use the proxy
  private func isProxyAuthorized(caller : Principal) : Bool {
    isMasterAdmin(caller) or 
    Principal.isController(caller) or 
    caller == DAOprincipal or 
    (sns_governance_canister_id == caller and sns_governance_canister_id != Principal.fromText("aaaaa-aa"));
  };

  // Validate that the target archive is a known archive canister
  private func isValidArchiveCanister(archivePrincipal : Principal) : Bool {
    // Check against all known archive canister types
    let archiveTypes : [CanisterIds.CanisterType] = [
      #trading_archive,
      #portfolio_archive,
      #price_archive,
      #dao_admin_archive,
      #dao_governance_archive,
      #dao_allocation_archive,
      #dao_neuron_allocation_archive,
      #reward_distribution_archive,
      #reward_withdrawal_archive
    ];
    
    for (archiveType in archiveTypes.vals()) {
      if (canister_ids.getCanisterId(archiveType) == archivePrincipal) {
        return true;
      };
    };
    false;
  };

  //=========================================================================
  // Archive Proxy - Update Methods
  //=========================================================================

  // Proxy for startBatchImportSystem
  public shared ({ caller }) func archiveProxy_startBatchImportSystem(archivePrincipal : Principal) : async Result.Result<Text, Text> {
    if (not isProxyAuthorized(caller)) {
      logger.warn("ArchiveProxy", "Unauthorized caller: " # Principal.toText(caller), "archiveProxy_startBatchImportSystem");
      return #err("Unauthorized caller");
    };

    if (not isValidArchiveCanister(archivePrincipal)) {
      logger.warn("ArchiveProxy", "Invalid archive canister: " # Principal.toText(archivePrincipal), "archiveProxy_startBatchImportSystem");
      return #err("Invalid archive canister principal");
    };

    logger.info("ArchiveProxy", "Starting batch import on archive " # Principal.toText(archivePrincipal) # " by " # Principal.toText(caller), "archiveProxy_startBatchImportSystem");

    try {
      let archive = getArchiveActor(archivePrincipal);
      await archive.startBatchImportSystem();
    } catch (error) {
      let errorMsg = "Failed to call archive: " # Error.message(error);
      logger.error("ArchiveProxy", errorMsg, "archiveProxy_startBatchImportSystem");
      #err(errorMsg);
    };
  };

  // Proxy for stopBatchImportSystem
  public shared ({ caller }) func archiveProxy_stopBatchImportSystem(archivePrincipal : Principal) : async Result.Result<Text, Text> {
    if (not isProxyAuthorized(caller)) {
      logger.warn("ArchiveProxy", "Unauthorized caller: " # Principal.toText(caller), "archiveProxy_stopBatchImportSystem");
      return #err("Unauthorized caller");
    };

    if (not isValidArchiveCanister(archivePrincipal)) {
      logger.warn("ArchiveProxy", "Invalid archive canister: " # Principal.toText(archivePrincipal), "archiveProxy_stopBatchImportSystem");
      return #err("Invalid archive canister principal");
    };

    logger.info("ArchiveProxy", "Stopping batch import on archive " # Principal.toText(archivePrincipal) # " by " # Principal.toText(caller), "archiveProxy_stopBatchImportSystem");

    try {
      let archive = getArchiveActor(archivePrincipal);
      await archive.stopBatchImportSystem();
    } catch (error) {
      let errorMsg = "Failed to call archive: " # Error.message(error);
      logger.error("ArchiveProxy", errorMsg, "archiveProxy_stopBatchImportSystem");
      #err(errorMsg);
    };
  };

  // Proxy for stopAllTimers
  public shared ({ caller }) func archiveProxy_stopAllTimers(archivePrincipal : Principal) : async Result.Result<Text, Text> {
    if (not isProxyAuthorized(caller)) {
      logger.warn("ArchiveProxy", "Unauthorized caller: " # Principal.toText(caller), "archiveProxy_stopAllTimers");
      return #err("Unauthorized caller");
    };

    if (not isValidArchiveCanister(archivePrincipal)) {
      logger.warn("ArchiveProxy", "Invalid archive canister: " # Principal.toText(archivePrincipal), "archiveProxy_stopAllTimers");
      return #err("Invalid archive canister principal");
    };

    logger.info("ArchiveProxy", "Stopping all timers on archive " # Principal.toText(archivePrincipal) # " by " # Principal.toText(caller), "archiveProxy_stopAllTimers");

    try {
      let archive = getArchiveActor(archivePrincipal);
      await archive.stopAllTimers();
    } catch (error) {
      let errorMsg = "Failed to call archive: " # Error.message(error);
      logger.error("ArchiveProxy", errorMsg, "archiveProxy_stopAllTimers");
      #err(errorMsg);
    };
  };

  // Proxy for runManualBatchImport
  public shared ({ caller }) func archiveProxy_runManualBatchImport(archivePrincipal : Principal) : async Result.Result<Text, Text> {
    if (not isProxyAuthorized(caller)) {
      logger.warn("ArchiveProxy", "Unauthorized caller: " # Principal.toText(caller), "archiveProxy_runManualBatchImport");
      return #err("Unauthorized caller");
    };

    if (not isValidArchiveCanister(archivePrincipal)) {
      logger.warn("ArchiveProxy", "Invalid archive canister: " # Principal.toText(archivePrincipal), "archiveProxy_runManualBatchImport");
      return #err("Invalid archive canister principal");
    };

    logger.info("ArchiveProxy", "Running manual batch import on archive " # Principal.toText(archivePrincipal) # " by " # Principal.toText(caller), "archiveProxy_runManualBatchImport");

    try {
      let archive = getArchiveActor(archivePrincipal);
      await archive.runManualBatchImport();
    } catch (error) {
      let errorMsg = "Failed to call archive: " # Error.message(error);
      logger.error("ArchiveProxy", errorMsg, "archiveProxy_runManualBatchImport");
      #err(errorMsg);
    };
  };

  // Proxy for setMaxInnerLoopIterations
  public shared ({ caller }) func archiveProxy_setMaxInnerLoopIterations(archivePrincipal : Principal, iterations : Nat) : async Result.Result<Text, Text> {
    if (not isProxyAuthorized(caller)) {
      logger.warn("ArchiveProxy", "Unauthorized caller: " # Principal.toText(caller), "archiveProxy_setMaxInnerLoopIterations");
      return #err("Unauthorized caller");
    };

    if (not isValidArchiveCanister(archivePrincipal)) {
      logger.warn("ArchiveProxy", "Invalid archive canister: " # Principal.toText(archivePrincipal), "archiveProxy_setMaxInnerLoopIterations");
      return #err("Invalid archive canister principal");
    };

    logger.info("ArchiveProxy", "Setting max inner loop iterations to " # Nat.toText(iterations) # " on archive " # Principal.toText(archivePrincipal) # " by " # Principal.toText(caller), "archiveProxy_setMaxInnerLoopIterations");

    try {
      let archive = getArchiveActor(archivePrincipal);
      await archive.setMaxInnerLoopIterations(iterations);
    } catch (error) {
      let errorMsg = "Failed to call archive: " # Error.message(error);
      logger.error("ArchiveProxy", errorMsg, "archiveProxy_setMaxInnerLoopIterations");
      #err(errorMsg);
    };
  };

  // Proxy for resetImportTimestamps
  public shared ({ caller }) func archiveProxy_resetImportTimestamps(archivePrincipal : Principal) : async Result.Result<Text, Text> {
    if (not isProxyAuthorized(caller)) {
      logger.warn("ArchiveProxy", "Unauthorized caller: " # Principal.toText(caller), "archiveProxy_resetImportTimestamps");
      return #err("Unauthorized caller");
    };

    if (not isValidArchiveCanister(archivePrincipal)) {
      logger.warn("ArchiveProxy", "Invalid archive canister: " # Principal.toText(archivePrincipal), "archiveProxy_resetImportTimestamps");
      return #err("Invalid archive canister principal");
    };

    logger.info("ArchiveProxy", "Resetting import timestamps on archive " # Principal.toText(archivePrincipal) # " by " # Principal.toText(caller), "archiveProxy_resetImportTimestamps");

    try {
      let archive = getArchiveActor(archivePrincipal);
      await archive.resetImportTimestamps();
    } catch (error) {
      let errorMsg = "Failed to call archive: " # Error.message(error);
      logger.error("ArchiveProxy", errorMsg, "archiveProxy_resetImportTimestamps");
      #err(errorMsg);
    };
  };

  // Proxy for updateConfig
  public shared ({ caller }) func archiveProxy_updateConfig(archivePrincipal : Principal, newConfig : ArchiveTypes.ArchiveConfig) : async Result.Result<Text, Text> {
    if (not isProxyAuthorized(caller)) {
      logger.warn("ArchiveProxy", "Unauthorized caller: " # Principal.toText(caller), "archiveProxy_updateConfig");
      return #err("Unauthorized caller");
    };

    if (not isValidArchiveCanister(archivePrincipal)) {
      logger.warn("ArchiveProxy", "Invalid archive canister: " # Principal.toText(archivePrincipal), "archiveProxy_updateConfig");
      return #err("Invalid archive canister principal");
    };

    logger.info("ArchiveProxy", "Updating config on archive " # Principal.toText(archivePrincipal) # " by " # Principal.toText(caller), "archiveProxy_updateConfig");

    try {
      let archive = getArchiveActor(archivePrincipal);
      let result = await archive.updateConfig(newConfig);
      switch (result) {
        case (#ok(msg)) { #ok(msg) };
        case (#err(error)) { #err(debug_show(error)) };
      };
    } catch (error) {
      let errorMsg = "Failed to call archive: " # Error.message(error);
      logger.error("ArchiveProxy", errorMsg, "archiveProxy_updateConfig");
      #err(errorMsg);
    };
  };

  //=========================================================================
  // Archive Proxy - Utility Methods
  //=========================================================================

  // Get list of all known archive canister principals
  public query func archiveProxy_getKnownArchives() : async [(Text, Principal)] {
    [
      ("trading_archive", canister_ids.getCanisterId(#trading_archive)),
      ("portfolio_archive", canister_ids.getCanisterId(#portfolio_archive)),
      ("price_archive", canister_ids.getCanisterId(#price_archive)),
      ("dao_admin_archive", canister_ids.getCanisterId(#dao_admin_archive)),
      ("dao_governance_archive", canister_ids.getCanisterId(#dao_governance_archive)),
      ("dao_allocation_archive", canister_ids.getCanisterId(#dao_allocation_archive)),
      ("dao_neuron_allocation_archive", canister_ids.getCanisterId(#dao_neuron_allocation_archive)),
      ("reward_distribution_archive", canister_ids.getCanisterId(#reward_distribution_archive)),
      ("reward_withdrawal_archive", canister_ids.getCanisterId(#reward_withdrawal_archive))
    ];
  };

  // Check if a principal is a valid archive canister
  public query func archiveProxy_isValidArchive(archivePrincipal : Principal) : async Bool {
    isValidArchiveCanister(archivePrincipal);
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
