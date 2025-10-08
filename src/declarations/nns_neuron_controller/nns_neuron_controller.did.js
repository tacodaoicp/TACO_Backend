export const idlFactory = ({ IDL }) => {
  const NeuronId = IDL.Record({ 'id' : IDL.Nat64 });
  const GovernanceError = IDL.Record({
    'error_message' : IDL.Text,
    'error_type' : IDL.Int32,
  });
  const ProposalId = IDL.Record({ 'id' : IDL.Nat64 });
  const BallotInfo = IDL.Record({
    'vote' : IDL.Int32,
    'proposal_id' : IDL.Opt(ProposalId),
  });
  const AccountIdentifier = IDL.Record({ 'hash' : IDL.Vec(IDL.Nat8) });
  const Account = IDL.Record({
    'owner' : IDL.Opt(IDL.Principal),
    'subaccount' : IDL.Opt(IDL.Vec(IDL.Nat8)),
  });
  const MaturityDisbursement = IDL.Record({
    'account_identifier_to_disburse_to' : IDL.Opt(AccountIdentifier),
    'timestamp_of_disbursement_seconds' : IDL.Opt(IDL.Nat64),
    'amount_e8s' : IDL.Opt(IDL.Nat64),
    'account_to_disburse_to' : IDL.Opt(Account),
    'finalize_disbursement_timestamp_seconds' : IDL.Opt(IDL.Nat64),
  });
  const DissolveState = IDL.Variant({
    'DissolveDelaySeconds' : IDL.Nat64,
    'WhenDissolvedTimestampSeconds' : IDL.Nat64,
  });
  const Followees = IDL.Record({ 'followees' : IDL.Vec(NeuronId) });
  const NeuronStakeTransfer = IDL.Record({
    'to_subaccount' : IDL.Vec(IDL.Nat8),
    'neuron_stake_e8s' : IDL.Nat64,
    'from' : IDL.Opt(IDL.Principal),
    'memo' : IDL.Nat64,
    'from_subaccount' : IDL.Vec(IDL.Nat8),
    'transfer_timestamp' : IDL.Nat64,
    'block_height' : IDL.Nat64,
  });
  const KnownNeuronData = IDL.Record({
    'name' : IDL.Text,
    'description' : IDL.Opt(IDL.Text),
    'links' : IDL.Opt(IDL.Vec(IDL.Text)),
  });
  const Neuron = IDL.Record({
    'id' : IDL.Opt(NeuronId),
    'staked_maturity_e8s_equivalent' : IDL.Opt(IDL.Nat64),
    'controller' : IDL.Opt(IDL.Principal),
    'recent_ballots' : IDL.Vec(BallotInfo),
    'voting_power_refreshed_timestamp_seconds' : IDL.Opt(IDL.Nat64),
    'kyc_verified' : IDL.Bool,
    'potential_voting_power' : IDL.Opt(IDL.Nat64),
    'neuron_type' : IDL.Opt(IDL.Int32),
    'not_for_profit' : IDL.Bool,
    'maturity_e8s_equivalent' : IDL.Nat64,
    'deciding_voting_power' : IDL.Opt(IDL.Nat64),
    'cached_neuron_stake_e8s' : IDL.Nat64,
    'created_timestamp_seconds' : IDL.Nat64,
    'auto_stake_maturity' : IDL.Opt(IDL.Bool),
    'aging_since_timestamp_seconds' : IDL.Nat64,
    'hot_keys' : IDL.Vec(IDL.Principal),
    'account' : IDL.Vec(IDL.Nat8),
    'joined_community_fund_timestamp_seconds' : IDL.Opt(IDL.Nat64),
    'maturity_disbursements_in_progress' : IDL.Opt(
      IDL.Vec(MaturityDisbursement)
    ),
    'dissolve_state' : IDL.Opt(DissolveState),
    'followees' : IDL.Vec(IDL.Tuple(IDL.Int32, Followees)),
    'neuron_fees_e8s' : IDL.Nat64,
    'visibility' : IDL.Opt(IDL.Int32),
    'transfer' : IDL.Opt(NeuronStakeTransfer),
    'known_neuron_data' : IDL.Opt(KnownNeuronData),
    'spawn_at_timestamp_seconds' : IDL.Opt(IDL.Nat64),
  });
  const Result_2 = IDL.Variant({ 'Ok' : Neuron, 'Err' : GovernanceError });
  const NnsNeuronController = IDL.Service({
    'add_hotkey' : IDL.Func(
        [NeuronId, IDL.Principal],
        [IDL.Record({ 'ok' : IDL.Bool, 'err' : IDL.Opt(GovernanceError) })],
        [],
      ),
    'get_canister_cycles' : IDL.Func(
        [],
        [IDL.Record({ 'cycles' : IDL.Nat })],
        ['query'],
      ),
    'get_full_neuron' : IDL.Func([IDL.Nat64], [Result_2], []),
  });
  return NnsNeuronController;
};
export const init = ({ IDL }) => { return []; };
