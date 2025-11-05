import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export type Subaccount = Uint8Array | number[];
export type TokenType = { 'ICP' : null } |
  { 'ICRC3' : null } |
  { 'ICRC12' : null };
export type ValidationResult = { 'Ok' : string } |
  { 'Err' : string };
export interface _SERVICE {
  'get_canister_cycles' : ActorMethod<[], { 'cycles' : bigint }>,
  'get_gnsf1_cnt' : ActorMethod<[], bigint>,
  'test_gnsf1' : ActorMethod<[], undefined>,
  'test_gnsf2' : ActorMethod<[Principal], undefined>,
  'validate_addToken' : ActorMethod<[Principal, TokenType], ValidationResult>,
  'validate_sendToken' : ActorMethod<
    [Principal, bigint, Principal, [] | [Subaccount]],
    ValidationResult
  >,
  'validate_test_gnsf1' : ActorMethod<[], ValidationResult>,
  'validate_test_gnsf2' : ActorMethod<[], ValidationResult>,
}
export declare const idlFactory: IDL.InterfaceFactory;
export declare const init: (args: { IDL: typeof IDL }) => IDL.Type[];
