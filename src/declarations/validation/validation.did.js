export const idlFactory = ({ IDL }) => {
  const PortfolioDirection = IDL.Variant({
    'Up' : IDL.Null,
    'Down' : IDL.Null,
  });
  const PortfolioValueType = IDL.Variant({
    'ICP' : IDL.Null,
    'USD' : IDL.Null,
  });
  const ValidationResult = IDL.Variant({ 'Ok' : IDL.Text, 'Err' : IDL.Text });
  const TokenType = IDL.Variant({
    'ICP' : IDL.Null,
    'ICRC3' : IDL.Null,
    'ICRC12' : IDL.Null,
  });
  const PriceDirection = IDL.Variant({ 'Up' : IDL.Null, 'Down' : IDL.Null });
  const Subaccount = IDL.Vec(IDL.Nat8);
  const PriceType = IDL.Variant({ 'ICP' : IDL.Null, 'USD' : IDL.Null });
  const UpdateConfig = IDL.Record({
    'maxPriceHistoryEntries' : IDL.Opt(IDL.Nat),
    'priceUpdateIntervalNS' : IDL.Opt(IDL.Nat),
    'tokenSyncTimeoutNS' : IDL.Opt(IDL.Nat),
    'maxSlippageBasisPoints' : IDL.Opt(IDL.Nat),
    'shortSyncIntervalNS' : IDL.Opt(IDL.Nat),
    'rebalanceIntervalNS' : IDL.Opt(IDL.Nat),
    'maxTradesStored' : IDL.Opt(IDL.Nat),
    'maxTradeValueICP' : IDL.Opt(IDL.Nat),
    'minTradeValueICP' : IDL.Opt(IDL.Nat),
    'portfolioRebalancePeriodNS' : IDL.Opt(IDL.Nat),
    'longSyncIntervalNS' : IDL.Opt(IDL.Nat),
    'maxTradeAttemptsPerInterval' : IDL.Opt(IDL.Nat),
    'maxKongswapAttempts' : IDL.Opt(IDL.Nat),
  });
  const SystemParameter = IDL.Variant({
    'MaxFollowers' : IDL.Nat,
    'MaxAllocationsPerDay' : IDL.Int,
    'MaxTotalUpdates' : IDL.Nat,
    'MaxPastAllocations' : IDL.Nat,
    'SnapshotInterval' : IDL.Nat,
    'FollowDepth' : IDL.Nat,
    'MaxFollowed' : IDL.Nat,
    'LogAdmin' : IDL.Principal,
    'AllocationWindow' : IDL.Nat,
    'MaxFollowUnfollowActionsPerDay' : IDL.Nat,
  });
  return IDL.Service({
    'get_canister_cycles' : IDL.Func(
        [],
        [IDL.Record({ 'cycles' : IDL.Nat })],
        ['query'],
      ),
    'get_gnsf1_cnt' : IDL.Func([], [IDL.Nat], ['query']),
    'test_gnsf1' : IDL.Func([], [], []),
    'test_gnsf2' : IDL.Func([IDL.Principal], [], []),
    'validate_addPortfolioCircuitBreakerCondition' : IDL.Func(
        [
          IDL.Text,
          PortfolioDirection,
          IDL.Float64,
          IDL.Nat,
          PortfolioValueType,
        ],
        [ValidationResult],
        ['query'],
      ),
    'validate_addToRewardSkipList' : IDL.Func(
        [IDL.Vec(IDL.Nat8)],
        [ValidationResult],
        ['query'],
      ),
    'validate_addToken' : IDL.Func(
        [IDL.Principal, TokenType],
        [ValidationResult],
        [],
      ),
    'validate_addTriggerCondition' : IDL.Func(
        [
          IDL.Text,
          PriceDirection,
          IDL.Float64,
          IDL.Nat,
          IDL.Vec(IDL.Principal),
        ],
        [ValidationResult],
        ['query'],
      ),
    'validate_clearAllTradingPauses' : IDL.Func(
        [IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_executeTradingCycle' : IDL.Func(
        [IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_pauseToken' : IDL.Func(
        [IDL.Principal, IDL.Text],
        [ValidationResult],
        ['query'],
      ),
    'validate_pauseTokenFromTradingManual' : IDL.Func(
        [IDL.Principal, IDL.Text],
        [ValidationResult],
        ['query'],
      ),
    'validate_recoverPoolBalances' : IDL.Func(
        [],
        [ValidationResult],
        ['query'],
      ),
    'validate_removeFromRewardSkipList' : IDL.Func(
        [IDL.Vec(IDL.Nat8)],
        [ValidationResult],
        ['query'],
      ),
    'validate_removePortfolioCircuitBreakerCondition' : IDL.Func(
        [IDL.Nat],
        [ValidationResult],
        ['query'],
      ),
    'validate_removeTriggerCondition' : IDL.Func(
        [IDL.Nat],
        [ValidationResult],
        ['query'],
      ),
    'validate_resetImportTimestamps' : IDL.Func(
        [IDL.Principal],
        [ValidationResult],
        ['query'],
      ),
    'validate_runManualBatchImport' : IDL.Func(
        [IDL.Principal],
        [ValidationResult],
        ['query'],
      ),
    'validate_sendToken' : IDL.Func(
        [IDL.Principal, IDL.Nat, IDL.Principal, IDL.Opt(Subaccount)],
        [ValidationResult],
        ['query'],
      ),
    'validate_setDistributionPeriod' : IDL.Func(
        [IDL.Nat],
        [ValidationResult],
        ['query'],
      ),
    'validate_setMaxInnerLoopIterations' : IDL.Func(
        [IDL.Principal, IDL.Nat],
        [ValidationResult],
        ['query'],
      ),
    'validate_setMaxNeuronSnapshots' : IDL.Func(
        [IDL.Nat],
        [ValidationResult],
        ['query'],
      ),
    'validate_setPerformanceScorePower' : IDL.Func(
        [IDL.Float64],
        [ValidationResult],
        ['query'],
      ),
    'validate_setPeriodicRewardPot' : IDL.Func(
        [IDL.Nat],
        [ValidationResult],
        ['query'],
      ),
    'validate_setPortfolioCircuitBreakerConditionActive' : IDL.Func(
        [IDL.Nat, IDL.Bool],
        [ValidationResult],
        ['query'],
      ),
    'validate_setTriggerConditionActive' : IDL.Func(
        [IDL.Nat, IDL.Bool],
        [ValidationResult],
        ['query'],
      ),
    'validate_setVotingPowerPower' : IDL.Func(
        [IDL.Float64],
        [ValidationResult],
        ['query'],
      ),
    'validate_startBatchImportSystem' : IDL.Func(
        [IDL.Principal],
        [ValidationResult],
        ['query'],
      ),
    'validate_startDistributionTimer' : IDL.Func(
        [],
        [ValidationResult],
        ['query'],
      ),
    'validate_startPortfolioSnapshots' : IDL.Func(
        [IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_startRebalancing' : IDL.Func(
        [IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_stopAllTimers' : IDL.Func(
        [IDL.Principal],
        [ValidationResult],
        ['query'],
      ),
    'validate_stopBatchImportSystem' : IDL.Func(
        [IDL.Principal],
        [ValidationResult],
        ['query'],
      ),
    'validate_stopDistributionTimer' : IDL.Func(
        [],
        [ValidationResult],
        ['query'],
      ),
    'validate_stopPortfolioSnapshots' : IDL.Func(
        [IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_stopRebalancing' : IDL.Func(
        [IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_syncWithDao' : IDL.Func([], [ValidationResult], ['query']),
    'validate_takeManualPortfolioSnapshot' : IDL.Func(
        [IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_take_neuron_snapshot' : IDL.Func(
        [],
        [ValidationResult],
        ['query'],
      ),
    'validate_test_gnsf1' : IDL.Func([], [ValidationResult], ['query']),
    'validate_test_gnsf2' : IDL.Func([], [ValidationResult], ['query']),
    'validate_triggerDistribution' : IDL.Func(
        [],
        [ValidationResult],
        ['query'],
      ),
    'validate_triggerDistributionCustom' : IDL.Func(
        [IDL.Int, IDL.Int, PriceType],
        [ValidationResult],
        ['query'],
      ),
    'validate_unpauseToken' : IDL.Func(
        [IDL.Principal, IDL.Text],
        [ValidationResult],
        ['query'],
      ),
    'validate_unpauseTokenFromTrading' : IDL.Func(
        [IDL.Principal, IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_updateMaxPortfolioSnapshots' : IDL.Func(
        [IDL.Nat, IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_updatePortfolioSnapshotInterval' : IDL.Func(
        [IDL.Nat, IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_updateRebalanceConfig' : IDL.Func(
        [UpdateConfig, IDL.Opt(IDL.Bool), IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
    'validate_updateSystemParameter' : IDL.Func(
        [SystemParameter, IDL.Opt(IDL.Text)],
        [ValidationResult],
        ['query'],
      ),
  });
};
export const init = ({ IDL }) => { return []; };
