import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export interface Account {
  'owner' : [] | [Principal],
  'subaccount' : [] | [Uint8Array | number[]],
}
export interface AccountIdentifier { 'hash' : Uint8Array | number[] }
export interface BallotInfo {
  'vote' : number,
  'proposal_id' : [] | [ProposalId],
}
export type DissolveState = { 'DissolveDelaySeconds' : bigint } |
  { 'WhenDissolvedTimestampSeconds' : bigint };
export interface Followees { 'followees' : Array<NeuronId> }
export interface GovernanceError {
  'error_message' : string,
  'error_type' : number,
}
export interface KnownNeuronData {
  'name' : string,
  'description' : [] | [string],
  'links' : [] | [Array<string>],
}
export interface MaturityDisbursement {
  'account_identifier_to_disburse_to' : [] | [AccountIdentifier],
  'timestamp_of_disbursement_seconds' : [] | [bigint],
  'amount_e8s' : [] | [bigint],
  'account_to_disburse_to' : [] | [Account],
  'finalize_disbursement_timestamp_seconds' : [] | [bigint],
}
export interface Neuron {
  'id' : [] | [NeuronId],
  'staked_maturity_e8s_equivalent' : [] | [bigint],
  'controller' : [] | [Principal],
  'recent_ballots' : Array<BallotInfo>,
  'voting_power_refreshed_timestamp_seconds' : [] | [bigint],
  'kyc_verified' : boolean,
  'potential_voting_power' : [] | [bigint],
  'neuron_type' : [] | [number],
  'not_for_profit' : boolean,
  'maturity_e8s_equivalent' : bigint,
  'deciding_voting_power' : [] | [bigint],
  'cached_neuron_stake_e8s' : bigint,
  'created_timestamp_seconds' : bigint,
  'auto_stake_maturity' : [] | [boolean],
  'aging_since_timestamp_seconds' : bigint,
  'hot_keys' : Array<Principal>,
  'account' : Uint8Array | number[],
  'joined_community_fund_timestamp_seconds' : [] | [bigint],
  'maturity_disbursements_in_progress' : [] | [Array<MaturityDisbursement>],
  'dissolve_state' : [] | [DissolveState],
  'followees' : Array<[number, Followees]>,
  'neuron_fees_e8s' : bigint,
  'visibility' : [] | [number],
  'transfer' : [] | [NeuronStakeTransfer],
  'known_neuron_data' : [] | [KnownNeuronData],
  'spawn_at_timestamp_seconds' : [] | [bigint],
}
export interface NeuronId { 'id' : bigint }
export interface NeuronStakeTransfer {
  'to_subaccount' : Uint8Array | number[],
  'neuron_stake_e8s' : bigint,
  'from' : [] | [Principal],
  'memo' : bigint,
  'from_subaccount' : Uint8Array | number[],
  'transfer_timestamp' : bigint,
  'block_height' : bigint,
}
export interface NnsNeuronController {
  'add_hotkey' : ActorMethod<
    [NeuronId, Principal],
    { 'ok' : boolean, 'err' : [] | [GovernanceError] }
  >,
  'get_canister_cycles' : ActorMethod<[], { 'cycles' : bigint }>,
  'get_full_neuron' : ActorMethod<[bigint], Result_2>,
}
export interface ProposalId { 'id' : bigint }
export type Result_2 = { 'Ok' : Neuron } |
  { 'Err' : GovernanceError };
export interface _SERVICE extends NnsNeuronController {}
export declare const idlFactory: IDL.InterfaceFactory;
export declare const init: (args: { IDL: typeof IDL }) => IDL.Type[];
