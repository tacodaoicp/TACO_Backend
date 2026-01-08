import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export type PortfolioDirection = { 'Up' : null } |
  { 'Down' : null };
export type PortfolioValueType = { 'ICP' : null } |
  { 'USD' : null };
export type PriceDirection = { 'Up' : null } |
  { 'Down' : null };
export type PriceType = { 'ICP' : null } |
  { 'USD' : null };
export type Subaccount = Uint8Array | number[];
export type SystemParameter = { 'MaxFollowers' : bigint } |
  { 'MaxAllocationsPerDay' : bigint } |
  { 'MaxTotalUpdates' : bigint } |
  { 'MaxPastAllocations' : bigint } |
  { 'SnapshotInterval' : bigint } |
  { 'FollowDepth' : bigint } |
  { 'MaxFollowed' : bigint } |
  { 'LogAdmin' : Principal } |
  { 'AllocationWindow' : bigint } |
  { 'MaxFollowUnfollowActionsPerDay' : bigint };
export type TokenType = { 'ICP' : null } |
  { 'ICRC3' : null } |
  { 'ICRC12' : null };
export interface UpdateConfig {
  'maxPriceHistoryEntries' : [] | [bigint],
  'priceUpdateIntervalNS' : [] | [bigint],
  'tokenSyncTimeoutNS' : [] | [bigint],
  'maxSlippageBasisPoints' : [] | [bigint],
  'shortSyncIntervalNS' : [] | [bigint],
  'rebalanceIntervalNS' : [] | [bigint],
  'maxTradesStored' : [] | [bigint],
  'maxTradeValueICP' : [] | [bigint],
  'minTradeValueICP' : [] | [bigint],
  'portfolioRebalancePeriodNS' : [] | [bigint],
  'longSyncIntervalNS' : [] | [bigint],
  'maxTradeAttemptsPerInterval' : [] | [bigint],
  'maxKongswapAttempts' : [] | [bigint],
}
export type ValidationResult = { 'Ok' : string } |
  { 'Err' : string };
export interface _SERVICE {
  'get_canister_cycles' : ActorMethod<[], { 'cycles' : bigint }>,
  'get_gnsf1_cnt' : ActorMethod<[], bigint>,
  'test_gnsf1' : ActorMethod<[], undefined>,
  'test_gnsf2' : ActorMethod<[Principal], undefined>,
  'validate_addPortfolioCircuitBreakerCondition' : ActorMethod<
    [string, PortfolioDirection, number, bigint, PortfolioValueType],
    ValidationResult
  >,
  'validate_addToRewardSkipList' : ActorMethod<
    [Uint8Array | number[]],
    ValidationResult
  >,
  'validate_addToken' : ActorMethod<[Principal, TokenType], ValidationResult>,
  'validate_addTriggerCondition' : ActorMethod<
    [string, PriceDirection, number, bigint, Array<Principal>],
    ValidationResult
  >,
  'validate_clearAllTradingPauses' : ActorMethod<
    [[] | [string]],
    ValidationResult
  >,
  'validate_executeTradingCycle' : ActorMethod<
    [[] | [string]],
    ValidationResult
  >,
  'validate_pauseToken' : ActorMethod<[Principal, string], ValidationResult>,
  'validate_pauseTokenFromTradingManual' : ActorMethod<
    [Principal, string],
    ValidationResult
  >,
  'validate_recoverPoolBalances' : ActorMethod<[], ValidationResult>,
  'validate_removeFromRewardSkipList' : ActorMethod<
    [Uint8Array | number[]],
    ValidationResult
  >,
  'validate_removePortfolioCircuitBreakerCondition' : ActorMethod<
    [bigint],
    ValidationResult
  >,
  'validate_removeTriggerCondition' : ActorMethod<[bigint], ValidationResult>,
  'validate_resetImportTimestamps' : ActorMethod<[Principal], ValidationResult>,
  'validate_runManualBatchImport' : ActorMethod<[Principal], ValidationResult>,
  'validate_sendToken' : ActorMethod<
    [Principal, bigint, Principal, [] | [Subaccount]],
    ValidationResult
  >,
  'validate_setDistributionPeriod' : ActorMethod<[bigint], ValidationResult>,
  'validate_setMaxInnerLoopIterations' : ActorMethod<
    [Principal, bigint],
    ValidationResult
  >,
  'validate_setMaxNeuronSnapshots' : ActorMethod<[bigint], ValidationResult>,
  'validate_setPerformanceScorePower' : ActorMethod<[number], ValidationResult>,
  'validate_setPeriodicRewardPot' : ActorMethod<[bigint], ValidationResult>,
  'validate_setPortfolioCircuitBreakerConditionActive' : ActorMethod<
    [bigint, boolean],
    ValidationResult
  >,
  'validate_setTriggerConditionActive' : ActorMethod<
    [bigint, boolean],
    ValidationResult
  >,
  'validate_setVotingPowerPower' : ActorMethod<[number], ValidationResult>,
  'validate_startBatchImportSystem' : ActorMethod<
    [Principal],
    ValidationResult
  >,
  'validate_startDistributionTimer' : ActorMethod<[], ValidationResult>,
  'validate_startPortfolioSnapshots' : ActorMethod<
    [[] | [string]],
    ValidationResult
  >,
  'validate_startRebalancing' : ActorMethod<[[] | [string]], ValidationResult>,
  'validate_stopAllTimers' : ActorMethod<[Principal], ValidationResult>,
  'validate_stopBatchImportSystem' : ActorMethod<[Principal], ValidationResult>,
  'validate_stopDistributionTimer' : ActorMethod<[], ValidationResult>,
  'validate_stopPortfolioSnapshots' : ActorMethod<
    [[] | [string]],
    ValidationResult
  >,
  'validate_stopRebalancing' : ActorMethod<[[] | [string]], ValidationResult>,
  'validate_syncWithDao' : ActorMethod<[], ValidationResult>,
  'validate_takeManualPortfolioSnapshot' : ActorMethod<
    [[] | [string]],
    ValidationResult
  >,
  'validate_take_neuron_snapshot' : ActorMethod<[], ValidationResult>,
  'validate_test_gnsf1' : ActorMethod<[], ValidationResult>,
  'validate_test_gnsf2' : ActorMethod<[], ValidationResult>,
  'validate_triggerDistribution' : ActorMethod<[], ValidationResult>,
  'validate_triggerDistributionCustom' : ActorMethod<
    [bigint, bigint, PriceType],
    ValidationResult
  >,
  'validate_unpauseToken' : ActorMethod<[Principal, string], ValidationResult>,
  'validate_unpauseTokenFromTrading' : ActorMethod<
    [Principal, [] | [string]],
    ValidationResult
  >,
  'validate_updateMaxPortfolioSnapshots' : ActorMethod<
    [bigint, [] | [string]],
    ValidationResult
  >,
  'validate_updatePortfolioSnapshotInterval' : ActorMethod<
    [bigint, [] | [string]],
    ValidationResult
  >,
  'validate_updateRebalanceConfig' : ActorMethod<
    [UpdateConfig, [] | [boolean], [] | [string]],
    ValidationResult
  >,
  'validate_updateSystemParameter' : ActorMethod<
    [SystemParameter, [] | [string]],
    ValidationResult
  >,
}
export declare const idlFactory: IDL.InterfaceFactory;
export declare const init: (args: { IDL: typeof IDL }) => IDL.Type[];
