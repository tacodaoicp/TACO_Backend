import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export type CircuitBreakerAction = { 'RejectOperation' : null } |
  { 'PauseBoth' : null } |
  { 'PauseBurn' : null } |
  { 'PauseMint' : null };
export interface CircuitBreakerConditionInput {
  'direction' : { 'Up' : null } |
    { 'Both' : null } |
    { 'Down' : null },
  'action' : CircuitBreakerAction,
  'timeWindowNS' : bigint,
  'conditionType' : CircuitBreakerConditionType,
  'enabled' : boolean,
  'thresholdPercent' : number,
  'applicableTokens' : Array<Principal>,
}
export type CircuitBreakerConditionType = { 'DecimalChange' : null } |
  { 'BalanceChange' : null } |
  { 'TokenPaused' : null } |
  { 'NavDrop' : null } |
  { 'PriceChange' : null };
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
  'minAllocationDiffBasisPoints' : [] | [bigint],
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
  'validate_addAcceptedMintToken' : ActorMethod<[Principal], ValidationResult>,
  'validate_addCircuitBreakerCondition' : ActorMethod<
    [CircuitBreakerConditionInput],
    ValidationResult
  >,
  'validate_addFeeExemptPrincipal' : ActorMethod<[Principal], ValidationResult>,
  'validate_addPortfolioCircuitBreakerCondition' : ActorMethod<
    [string, PortfolioDirection, number, bigint, PortfolioValueType],
    ValidationResult
  >,
  'validate_addRateLimitExemptPrincipal' : ActorMethod<
    [Principal],
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
  'validate_claimNachosBurnFees' : ActorMethod<
    [Principal, bigint],
    ValidationResult
  >,
  'validate_claimNachosCancellationFees' : ActorMethod<
    [Principal, Principal, bigint],
    ValidationResult
  >,
  'validate_claimNachosMintFees' : ActorMethod<
    [Principal, bigint],
    ValidationResult
  >,
  'validate_clearAllTradingPauses' : ActorMethod<
    [[] | [string]],
    ValidationResult
  >,
  'validate_enableCircuitBreakerCondition' : ActorMethod<
    [bigint, boolean],
    ValidationResult
  >,
  'validate_executeTradingCycle' : ActorMethod<
    [[] | [string]],
    ValidationResult
  >,
  'validate_nachosEmergencyPause' : ActorMethod<[string], ValidationResult>,
  'validate_nachosEmergencyUnpause' : ActorMethod<[string], ValidationResult>,
  'validate_pauseNachosBurning' : ActorMethod<[string], ValidationResult>,
  'validate_pauseNachosMinting' : ActorMethod<[string], ValidationResult>,
  'validate_pauseToken' : ActorMethod<[Principal, string], ValidationResult>,
  'validate_pauseTokenFromTradingManual' : ActorMethod<
    [Principal, string],
    ValidationResult
  >,
  'validate_recoverPoolBalances' : ActorMethod<[], ValidationResult>,
  'validate_recoverStuckNachos' : ActorMethod<
    [Principal, bigint],
    ValidationResult
  >,
  'validate_recoverWronglySentTokens' : ActorMethod<
    [Principal, bigint, Principal],
    ValidationResult
  >,
  'validate_removeAcceptedMintToken' : ActorMethod<
    [Principal],
    ValidationResult
  >,
  'validate_removeCircuitBreakerCondition' : ActorMethod<
    [bigint],
    ValidationResult
  >,
  'validate_removeFeeExemptPrincipal' : ActorMethod<
    [Principal],
    ValidationResult
  >,
  'validate_removeFromRewardSkipList' : ActorMethod<
    [Uint8Array | number[]],
    ValidationResult
  >,
  'validate_removePortfolioCircuitBreakerCondition' : ActorMethod<
    [bigint],
    ValidationResult
  >,
  'validate_removeRateLimitExemptPrincipal' : ActorMethod<
    [Principal],
    ValidationResult
  >,
  'validate_removeRewardPenalty' : ActorMethod<
    [Uint8Array | number[]],
    ValidationResult
  >,
  'validate_removeTriggerCondition' : ActorMethod<[bigint], ValidationResult>,
  'validate_resetImportTimestamps' : ActorMethod<[Principal], ValidationResult>,
  'validate_resetNachosCircuitBreaker' : ActorMethod<
    [string],
    ValidationResult
  >,
  'validate_retryFailedTransfers' : ActorMethod<[], ValidationResult>,
  'validate_runManualBatchImport' : ActorMethod<[Principal], ValidationResult>,
  'validate_sendToken' : ActorMethod<
    [Principal, bigint, Principal, [] | [Subaccount]],
    ValidationResult
  >,
  'validate_setAcceptedMintTokenEnabled' : ActorMethod<
    [Principal, boolean],
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
  'validate_setRewardPenalty' : ActorMethod<
    [Uint8Array | number[], bigint],
    ValidationResult
  >,
  'validate_setTokenMaxAllocation' : ActorMethod<
    [Principal, [] | [bigint], string],
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
  'validate_unpauseNachosBurning' : ActorMethod<[string], ValidationResult>,
  'validate_unpauseNachosMinting' : ActorMethod<[string], ValidationResult>,
  'validate_unpauseToken' : ActorMethod<[Principal, string], ValidationResult>,
  'validate_unpauseTokenFromTrading' : ActorMethod<
    [Principal, [] | [string]],
    ValidationResult
  >,
  'validate_updateCancellationFeeMultiplier' : ActorMethod<
    [bigint],
    ValidationResult
  >,
  'validate_updateCircuitBreakerCondition' : ActorMethod<
    [
      bigint,
      [] | [number],
      [] | [bigint],
      [] | [{ 'Up' : null } | { 'Both' : null } | { 'Down' : null }],
      [] | [CircuitBreakerAction],
      [] | [Array<Principal>],
    ],
    ValidationResult
  >,
  'validate_updateMaxPortfolioSnapshots' : ActorMethod<
    [bigint, [] | [string]],
    ValidationResult
  >,
  'validate_updateNachosFees' : ActorMethod<
    [[] | [bigint], [] | [bigint]],
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
