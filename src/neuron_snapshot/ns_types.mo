import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Bool "mo:base/Bool";
import Principal "mo:base/Principal";
import Int32 "mo:base/Int32";
import Map "mo:map/Map";
import Vector "mo:vector";
module {

  public type SnapshotId = Nat;
  public type NeuronSnapshotStatus = {
    #Ready;
    #TakingSnapshot;
    #StoringSnapshot;
  };

  public type TakeNeuronSnapshotResult = {
    #Ok : SnapshotId;
    #Err : TakeNeuronSnapshotError;
  };

  public type TakeNeuronSnapshotError = {
    #AlreadyTakingSnapshot;
    #SnsGovernanceCanisterIdNotSet;
  };

  public type ResumeNeuronSnapshotResult = {
    #Ok : SnapshotId;
    #Err : ResumeNeuronSnapshotError;
  };

  public type ResumeNeuronSnapshotError = {
    #NotTakingSnapshot;
  };

  public type CancelNeuronSnapshotResult = {
    #Ok : SnapshotId;
    #Err : CancelNeuronSnapshotError;
  };

  public type CancelNeuronSnapshotError = {
    #NotTakingSnapshot;
  };

  public type NeuronId = { id : Blob };
  public type Timestamp = Nat64;
  public type Followees = { followees : [NeuronId] };
  type Subaccount = Blob;

  type Account = {
    owner : Principal;
    subaccount : ?Subaccount;
  };

  public type NeuronSnapshotInfo = {
    id : SnapshotId;
    timestamp : Timestamp;
    result : NeuronSnapshotResult;
  };

  public type NeuronSnapshotResult = {
    #Ok;
    #Err : NeuronSnapshotError;
  };

  public type NeuronSnapshotError = {
    #Timeout;
    #Cancelled;
  };

  public type NeuronSnapshot = NeuronSnapshotInfo and {
    neurons : [Neuron];
  };

  public type NervousSystemParameters = {
    default_followees : ?{
      followees : [(Nat64, { followees : [{ id : Blob }] })];
    };
    max_dissolve_delay_seconds : ?Nat64;
    max_dissolve_delay_bonus_percentage : ?Nat64;
    max_followees_per_function : ?Nat64;
    automatically_advance_target_version : ?Bool;
    neuron_claimer_permissions : ?{ permissions : [Int32] };
    neuron_minimum_stake_e8s : ?Nat64;
    max_neuron_age_for_age_bonus : ?Nat64;
    initial_voting_period_seconds : ?Nat64;
    neuron_minimum_dissolve_delay_to_vote_seconds : ?Nat64;
    reject_cost_e8s : ?Nat64;
    max_proposals_to_keep_per_action : ?Nat32;
    wait_for_quiet_deadline_increase_seconds : ?Nat64;
    max_number_of_neurons : ?Nat64;
    transaction_fee_e8s : ?Nat64;
    max_number_of_proposals_with_ballots : ?Nat64;
    max_age_bonus_percentage : ?Nat64;
    neuron_grantable_permissions : ?{ permissions : [Int32] };
    voting_rewards_parameters : ?{
      final_reward_rate_basis_points : ?Nat64;
      initial_reward_rate_basis_points : ?Nat64;
      reward_rate_transition_duration_seconds : ?Nat64;
      round_duration_seconds : ?Nat64;
    };
    maturity_modulation_disabled : ?Bool;
    max_number_of_principals_per_neuron : ?Nat64;
  };

  public type Neuron = {
    id : ?NeuronId;
    staked_maturity_e8s_equivalent : ?Nat64;
    permissions : [NeuronPermission]; // this makes identifying hotkeys possible
    maturity_e8s_equivalent : Nat64;
    cached_neuron_stake_e8s : Nat64;
    created_timestamp_seconds : Nat64;
    source_nns_neuron_id : ?Nat64;
    auto_stake_maturity : ?Bool;
    aging_since_timestamp_seconds : Nat64;
    dissolve_state : ?DissolveState;
    voting_power_percentage_multiplier : Nat64;
    vesting_period_seconds : ?Nat64;
    disburse_maturity_in_progress : [DisburseMaturityInProgress];
    followees : [(Nat64, Followees)];
    neuron_fees_e8s : Nat64;
  };

  public type NeuronPermission = {
    principal : ?Principal;
    permission_type : [Int32];
  };

  // Type for cumulative values
  public type CumulativeValues = {
    total_staked_maturity : Nat64;
    total_cached_stake : Nat64;
  };

  public type NeuronVP = {
    neuronId : Blob;
    votingPower : Nat;
  };

  public type LogEntry = {
    timestamp : Int;
    level : LogLevel;
    component : Text;
    message : Text;
    context : Text;
  };

  public type LogLevel = {
    #INFO;
    #WARN;
    #ERROR;
  };

  // Types for neuron storage
  public type NeuronStoreKey = Text; // Format: "SnapshotId-Principal"
  public type NeuronStore = Map.Map<SnapshotId, [(Principal, Vector.Vector<NeuronVP>)]>;

  public type NeuronDetailsStore = Map.Map<NeuronDetailsKey, Nat>; // Nat = voting power
  public type SnapshotNeuronDetailsStore = Map.Map<SnapshotId, NeuronDetailsStore>;

  public type NeuronDetailsKey = Blob; // NeuronId

  public type NeuronDetails = {
    id : ?NeuronId;
    staked_maturity_e8s_equivalent : ?Nat64;
    cached_neuron_stake_e8s : Nat64;
    aging_since_timestamp_seconds : Nat64;
    dissolve_state : ?DissolveState;
    voting_power_percentage_multiplier : Nat64;
  };

  public type NeuronDetailsFinal = {
    calculated_VP : VotingPower;
  };

  public type VotingPower = Nat;

  public type DissolveState = {
    #DissolveDelaySeconds : Nat64;
    #WhenDissolvedTimestampSeconds : Nat64;
  };

  public type DisburseMaturityInProgress = {
    timestamp_of_disbursement_seconds : Nat64;
    amount_e8s : Nat64;
    account_to_disburse_to : ?Account;
    finalize_disbursement_timestamp_seconds : ?Nat64;
  };

  public type ListNeurons = {
    of_principal : ?Principal;
    limit : Nat32;
    start_page_at : ?NeuronId;
  };

  public type ListNeuronsResponse = { neurons : [Neuron] };
  public type CumulativeVP = {
    total_staked_vp : Nat;
    total_staked_vp_by_hotkey_setters : Nat;
    // Raw VP values without penalty applied (for transparency)
    total_staked_vp_raw : ?Nat;
    total_staked_vp_by_hotkey_setters_raw : ?Nat;
  };

  // DAO Voting System Types  
  public type DAOVoteDecision = {
    #Adopt;
    #Reject;
  };

  public type DAOVote = {
    decision : DAOVoteDecision;
    voting_power : Nat;
    timestamp : Timestamp;
    voter_principal : Principal;
  };

  public type Self = actor {
    // Snapshot management
    take_neuron_snapshot : shared () -> async TakeNeuronSnapshotResult;
    cancel_neuron_snapshot : shared () -> async CancelNeuronSnapshotResult;

    // Snapshot status queries
    get_neuron_snapshot_head_id : shared query () -> async SnapshotId;
    get_neuron_snapshot_status : shared query () -> async NeuronSnapshotStatus;
    get_neuron_snapshot_info : shared query (id : SnapshotId) -> async ?NeuronSnapshotInfo;
    get_neuron_snapshots_info : shared query (start : Nat, length : Nat) -> async [NeuronSnapshotInfo];
    get_neuron_snapshot_neurons : shared query (snapshot_id : SnapshotId, start : Nat, length : Nat) -> async [Neuron];

    // Log admin
    setLogAdmin : shared Principal -> async ();

    // Voting power queries
    getCumulativeValuesAtSnapshot : shared query (snapshotId : ?SnapshotId) -> async ?CumulativeVP;

    // Paginated voting power entries
    getVotingPowerEntries : shared query (
      snapshotId : ?SnapshotId,
      start : Nat,
      length : Nat,
    ) -> async ?{
      entries : [(Principal, VotingPower)];
      total_entries : Nat;
    };

    getNeuronDataForDAO : shared query (
      snapshotId : SnapshotId,
      start : Nat,
      limit : Nat,
    ) -> async ?{
      entries : [(Principal, [NeuronVP])];
      total_entries : Nat;
      stopped_at : ?Nat;
    };

    // Test mode
    setTest : shared (enabled : Bool) -> async ();
  };

  // DAO NNS Vote Record - stores details of when DAO voted on an NNS proposal
  public type DAONNSVoteRecord = {
    nns_proposal_id : Nat64;
    dao_decision : Text; // "Adopt" or "Reject"
    adopt_vp : Nat;
    reject_vp : Nat;
    total_vp : Nat;
    vote_timestamp : Timestamp;
    voted_by_principal : Principal; // Who triggered the NNS vote
  };

};
