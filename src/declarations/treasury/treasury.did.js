export const idlFactory = ({ IDL }) => {
  const RebalanceError = IDL.Variant({
    'LiquidityError' : IDL.Text,
    'TradeError' : IDL.Text,
    'SystemError' : IDL.Text,
    'ConfigError' : IDL.Text,
    'PriceError' : IDL.Text,
  });
  const Result = IDL.Variant({ 'ok' : IDL.Text, 'err' : RebalanceError });
  const Result_4 = IDL.Variant({ 'ok' : IDL.Text, 'err' : IDL.Text });
  const RebalanceConfig = IDL.Record({
    'tokenSyncTimeoutNS' : IDL.Nat,
    'maxSlippageBasisPoints' : IDL.Nat,
    'shortSyncIntervalNS' : IDL.Nat,
    'rebalanceIntervalNS' : IDL.Nat,
    'maxTradesStored' : IDL.Nat,
    'maxTradeValueICP' : IDL.Nat,
    'minTradeValueICP' : IDL.Nat,
    'portfolioRebalancePeriodNS' : IDL.Nat,
    'longSyncIntervalNS' : IDL.Nat,
    'maxTradeAttemptsPerInterval' : IDL.Nat,
    'maxKongswapAttempts' : IDL.Nat,
  });
  const PricePoint = IDL.Record({
    'usdPrice' : IDL.Float64,
    'time' : IDL.Int,
    'icpPrice' : IDL.Nat,
  });
  const TokenType = IDL.Variant({
    'ICP' : IDL.Null,
    'ICRC3' : IDL.Null,
    'ICRC12' : IDL.Null,
  });
  const TokenDetails = IDL.Record({
    'lastTimeSynced' : IDL.Int,
    'balance' : IDL.Nat,
    'isPaused' : IDL.Bool,
    'Active' : IDL.Bool,
    'epochAdded' : IDL.Int,
    'priceInICP' : IDL.Nat,
    'priceInUSD' : IDL.Float64,
    'tokenTransferFee' : IDL.Nat,
    'tokenDecimals' : IDL.Nat,
    'pastPrices' : IDL.Vec(PricePoint),
    'tokenSymbol' : IDL.Text,
    'tokenName' : IDL.Text,
    'pausedDueToSyncFailure' : IDL.Bool,
    'tokenType' : TokenType,
  });
  const Result_3 = IDL.Variant({
    'ok' : IDL.Vec(IDL.Tuple(IDL.Principal, IDL.Vec(PricePoint))),
    'err' : IDL.Text,
  });
  const ExchangeType = IDL.Variant({
    'KongSwap' : IDL.Null,
    'ICPSwap' : IDL.Null,
  });
  const TradeRecord = IDL.Record({
    'error' : IDL.Opt(IDL.Text),
    'amountSold' : IDL.Nat,
    'amountBought' : IDL.Nat,
    'timestamp' : IDL.Int,
    'tokenSold' : IDL.Principal,
    'success' : IDL.Bool,
    'exchange' : ExchangeType,
    'tokenBought' : IDL.Principal,
    'slippage' : IDL.Float64,
  });
  const RebalanceStatus = IDL.Variant({
    'Failed' : IDL.Text,
    'Idle' : IDL.Null,
    'Trading' : IDL.Null,
  });
  const Result_2 = IDL.Variant({
    'ok' : IDL.Record({
      'executedTrades' : IDL.Vec(TradeRecord),
      'metrics' : IDL.Record({
        'avgSlippage' : IDL.Float64,
        'successRate' : IDL.Float64,
        'lastUpdate' : IDL.Int,
        'totalTradesExecuted' : IDL.Nat,
        'totalTradesFailed' : IDL.Nat,
      }),
      'rebalanceStatus' : RebalanceStatus,
      'portfolioState' : IDL.Record({
        'currentAllocations' : IDL.Vec(IDL.Tuple(IDL.Principal, IDL.Nat)),
        'totalValueICP' : IDL.Nat,
        'totalValueUSD' : IDL.Float64,
        'targetAllocations' : IDL.Vec(IDL.Tuple(IDL.Principal, IDL.Nat)),
      }),
    }),
    'err' : IDL.Text,
  });
  const Subaccount = IDL.Vec(IDL.Nat8);
  const TransferRecipient = IDL.Variant({
    'principal' : IDL.Principal,
    'accountId' : IDL.Record({
      'owner' : IDL.Principal,
      'subaccount' : IDL.Opt(Subaccount),
    }),
  });
  const SyncErrorTreasury = IDL.Variant({
    'NotDAO' : IDL.Null,
    'UnexpectedError' : IDL.Text,
  });
  const Result_1 = IDL.Variant({ 'ok' : IDL.Text, 'err' : SyncErrorTreasury });
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
  const treasury = IDL.Service({
    'admin_executeTradingCycle' : IDL.Func([], [Result], []),
    'admin_recoverPoolBalances' : IDL.Func([], [Result_4], []),
    'admin_syncWithDao' : IDL.Func([], [Result_4], []),
    'getCurrentAllocations' : IDL.Func(
        [],
        [IDL.Vec(IDL.Tuple(IDL.Principal, IDL.Nat))],
        ['query'],
      ),
    'getSystemParameters' : IDL.Func([], [RebalanceConfig], ['query']),
    'getTokenDetails' : IDL.Func(
        [],
        [IDL.Vec(IDL.Tuple(IDL.Principal, TokenDetails))],
        ['query'],
      ),
    'getTokenPriceHistory' : IDL.Func(
        [IDL.Vec(IDL.Principal)],
        [Result_3],
        ['query'],
      ),
    'getTradingStatus' : IDL.Func([], [Result_2], ['query']),
    'receiveTransferTasks' : IDL.Func(
        [
          IDL.Vec(
            IDL.Tuple(TransferRecipient, IDL.Nat, IDL.Principal, IDL.Nat8)
          ),
          IDL.Bool,
        ],
        [IDL.Bool, IDL.Opt(IDL.Vec(IDL.Tuple(IDL.Principal, IDL.Nat64)))],
        [],
      ),
    'setTest' : IDL.Func([IDL.Bool], [], []),
    'startRebalancing' : IDL.Func([], [Result], []),
    'stopRebalancing' : IDL.Func([], [Result], []),
    'syncTokenDetailsFromDAO' : IDL.Func(
        [IDL.Vec(IDL.Tuple(IDL.Principal, TokenDetails))],
        [Result_1],
        [],
      ),
    'updateRebalanceConfig' : IDL.Func(
        [UpdateConfig, IDL.Opt(IDL.Bool)],
        [Result],
        [],
      ),
  });
  return treasury;
};
export const init = ({ IDL }) => { return []; };
