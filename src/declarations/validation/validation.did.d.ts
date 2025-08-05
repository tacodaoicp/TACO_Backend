import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export type TokenType = { 'ICP' : null } |
  { 'ICRC3' : null } |
  { 'ICRC12' : null };
export type ValidationResult = { 'Ok' : string } |
  { 'Err' : string };
export interface _SERVICE {
  'validate_addToken' : ActorMethod<[Principal, TokenType], ValidationResult>,
}
export declare const idlFactory: IDL.InterfaceFactory;
export declare const init: (args: { IDL: typeof IDL }) => IDL.Type[];
