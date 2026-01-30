
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Float "mo:base/Float";
import Nat "mo:base/Nat";
import Int "mo:base/Int";
import ICRC3 "mo:icrc3-mo/service";
import TreasuryTypes "../treasury/treasury_types";
import DAOTypes "../DAO_backend/dao_types";

module {
  // Re-export ICRC3 types for convenience
  public type Value = ICRC3.Value;
  public type Block = ICRC3.Block;
  public type GetBlocksArgs = ICRC3.GetBlocksArgs;
  public type GetBlocksResult = ICRC3.GetBlocksResult;
  public type DataCertificate = ICRC3.DataCertificate;
  public type ArchivedBlock = ICRC3.ArchivedBlock;

  // Re-export treasury types
  public type TradeRecord = TreasuryTypes.TradeRecord;
  public type ExchangeType = TreasuryTypes.ExchangeType;
  public type TokenDetails = TreasuryTypes.TokenDetails;
  public type PricePoint = TreasuryTypes.PricePoint;
  public type TradingPauseRecord = TreasuryTypes.TradingPauseRecord;
  public type PortfolioSnapshot = TreasuryTypes.PortfolioSnapshot;
  public type PriceAlertLog = TreasuryTypes.PriceAlertLog;
  public type PortfolioCircuitBreakerLog = TreasuryTypes.PortfolioCircuitBreakerLog;

  // Custom TACO DAO Block Types
  public type TacoBlockType = {
    #Trade;         // "3trade" - Individual trade records
    #Portfolio;     // "3portfolio" - Portfolio snapshots
    #Circuit;       // "3circuit" - Circuit breaker events
    #Price;         // "3price" - Price history entries
    #Pause;         // "3pause" - Trading pause events
    #Allocation;    // "3allocation" - Portfolio allocation changes
    #Admin;         // "3admin" - Administrative actions
    #AllocationChange; // "3allocation_change" - User allocation transitions
    #FollowAction;  // "3follow_action" - Follow/unfollow relationships
    #VotingPower;   // "3voting_power" - Voting power changes
    #NeuronUpdate;  // "3neuron_update" - Neuron state updates
    #RewardDistribution; // "3reward_distribution" - Reward distribution records
    #RewardWithdrawal; // "3reward_withdrawal" - Reward withdrawal records
  };

  // Convert block type to string identifier
  public func blockTypeToString(btype : TacoBlockType) : Text {
    switch (btype) {
      case (#Trade) { "3trade" };
      case (#Portfolio) { "3portfolio" };
      case (#Circuit) { "3circuit" };
      case (#Price) { "3price" };
      case (#Pause) { "3pause" };
      case (#Allocation) { "3allocation" };
      case (#Admin) { "3admin" };
      case (#AllocationChange) { "3allocation_change" };
      case (#FollowAction) { "3follow_action" };
      case (#VotingPower) { "3voting_power" };
      case (#NeuronUpdate) { "3neuron_update" };
      case (#RewardDistribution) { "3reward_distribution" };
      case (#RewardWithdrawal) { "3reward_withdrawal" };
    };
  };

  // Archive-specific types
  public type ArchiveConfig = {
    maxBlocksPerCanister: Nat;
    blockRetentionPeriodNS: Int;
    enableCompression: Bool;
    autoArchiveEnabled: Bool;
  };

  public type ArchiveStatus = {
    totalBlocks: Nat;
    oldestBlock: ?Nat;
    newestBlock: ?Nat;
    supportedBlockTypes: [Text];
    storageUsed: Nat;
    lastArchiveTime: Int;
  };

  // Trading-specific block data structures
  public type TradeBlockData = {
    trader: Principal;
    tokenSold: Principal;
    tokenBought: Principal;
    amountSold: Nat;
    amountBought: Nat;
    exchange: ExchangeType;
    success: Bool;
    slippage: Float;
    fee: Nat;
    error: ?Text;
    timestamp: Int; // Original event timestamp (not import time!)
  };

  // Detailed token information stored in portfolio archives (excluding symbol since it can be looked up)
  public type DetailedTokenSnapshot = {
    token: Principal;
    balance: Nat;           // Absolute token amount
    decimals: Nat;
    priceInICP: Nat;        // Price at snapshot time (e8s)
    priceInUSD: Float;      // Price at snapshot time
    valueInICP: Nat;        // balance * priceInICP
    valueInUSD: Float;      // balance * priceInUSD
  };

  public type PortfolioBlockData = {
    timestamp: Int;
    totalValueICP: Nat;
    totalValueUSD: Float;
    tokenCount: Nat;
    tokens: [DetailedTokenSnapshot];  // Full token details instead of just Principal IDs
    pausedTokens: [Principal];        // Keep as-is since we don't have detailed data for paused tokens
    reason: SnapshotReason;
  };

  public type CircuitBreakerBlockData = {
    eventType: CircuitBreakerEventType;
    triggerToken: ?Principal;
    thresholdValue: Float;
    actualValue: Float;
    tokensAffected: [Principal];
    systemResponse: Text;
    severity: Text;
    timestamp: Int; // Original event timestamp (not import time!)
  };

  public type PriceBlockData = {
    token: Principal;
    priceICP: Nat;
    priceUSD: Float;
    source: PriceSource;
    volume24h: ?Nat;
    change24h: ?Float;
    timestamp: Int; // Original event timestamp (not import time!)
  };

  public type TradingPauseBlockData = {
    token: Principal;
    tokenSymbol: Text;
    reason: TradingPauseReason;
    duration: ?Int;
    timestamp: Int; // Original event timestamp (not import time!)
  };

  public type AllocationBlockData = {
    user: Principal;
    oldAllocation: [Allocation];
    newAllocation: [Allocation];
    votingPower: Nat;
    reason: AllocationChangeReason;
  };

  // Administrative actions from both DAO_backend and Treasury
  public type AdminActionBlockData = {
    id: Nat;
    timestamp: Int;
    admin: Principal;
    canister: AdminCanisterSource;
    actionType: AdminActionVariant;
    reason: Text;
    success: Bool;
    errorMessage: ?Text;
  };

  public type AdminCanisterSource = {
    #DAO_backend;
    #Treasury;
  };

  // Unified admin action types covering both DAO and Treasury operations
  public type AdminActionVariant = {
    // DAO_backend actions
    #TokenAdd: {token: Principal; tokenType: DAOTypes.TokenType; viaGovernance: Bool};
    #TokenRemove: {token: Principal};
    #TokenDelete: {token: Principal};
    #TokenPause: {token: Principal};
    #TokenUnpause: {token: Principal};
    #SystemStateChange: {oldState: DAOTypes.SystemState; newState: DAOTypes.SystemState};
    #ParameterUpdate: {parameter: DAOTypes.SystemParameter; oldValue: Text; newValue: Text};
    #AdminPermissionGrant: {targetAdmin: Principal; function: Text; durationDays: Nat};
    #AdminAdd: {newAdmin: Principal};
    #AdminRemove: {removedAdmin: Principal};
    // Treasury actions
    #StartRebalancing;
    #StopRebalancing;
    #ResetRebalanceState;
    #UpdateRebalanceConfig: {oldConfig: Text; newConfig: Text};
    #PauseTokenManual: {token: Principal; pauseType: Text};
    #UnpauseToken: {token: Principal};
    #ClearAllTradingPauses;
    #AddTriggerCondition: {conditionId: Nat; conditionType: Text; details: Text};
    #RemoveTriggerCondition: {conditionId: Nat};
    #UpdateTriggerCondition: {conditionId: Nat; oldCondition: Text; newCondition: Text};
    #SetTriggerConditionActive: {conditionId: Nat; isActive: Bool};
    #ClearPriceAlerts;
    #AddPortfolioCircuitBreaker: {conditionId: Nat; conditionType: Text; details: Text};
    #RemovePortfolioCircuitBreaker: {conditionId: Nat};
    #UpdatePortfolioCircuitBreaker: {conditionId: Nat; oldCondition: Text; newCondition: Text};
    #SetPortfolioCircuitBreakerActive: {conditionId: Nat; isActive: Bool};
    #UpdatePausedTokenThreshold: {oldThreshold: Nat; newThreshold: Nat};
    #ClearPortfolioCircuitBreakerLogs;
    #UpdateMaxPortfolioSnapshots: {oldLimit: Nat; newLimit: Nat};
    #StartPortfolioSnapshots;
    #StopPortfolioSnapshots;
    #UpdatePortfolioSnapshotInterval: {oldIntervalNS: Nat; newIntervalNS: Nat};
    #TakeManualSnapshot;
    #ExecuteTradingCycle;
    #SetTestMode: {isTestMode: Bool};
    #ClearSystemLogs;

    // Canister lifecycle (both DAO and Treasury)
    #CanisterStart;
    #CanisterStop;
  };

  // Allocation change tracking for dao_allocation_archive
  public type AllocationChangeBlockData = {
    id: Nat;
    timestamp: Int;
    user: Principal;
    changeType: AllocationChangeType;
    oldAllocations: [DAOTypes.Allocation];
    newAllocations: [DAOTypes.Allocation]; 
    votingPower: Nat;
    maker: Principal;
    reason: ?Text; // For manual changes
  };

  public type AllocationChangeType = {
    #UserUpdate: {userInitiated: Bool};
    #FollowAction: {followedUser: Principal};
    #SystemRebalance;
    #VotingPowerChange;
  };

  // Follow/unfollow action tracking for dao_allocation_archive
  public type FollowActionBlockData = {
    id: Nat;
    timestamp: Int;
    follower: Principal;
    followed: Principal;
    action: FollowActionType;
    previousFollowCount: Nat;
    newFollowCount: Nat;
  };

  public type FollowActionType = {
    #Follow;
    #Unfollow;
  };

  // Voting power changes for dao_governance_archive
  public type VotingPowerBlockData = {
    id: Nat;
    timestamp: Int;
    user: Principal;
    changeType: VotingPowerChangeType;
    oldVotingPower: Nat;
    newVotingPower: Nat;
    neurons: [DAOTypes.NeuronVP];
  };

  public type VotingPowerChangeType = {
    #NeuronSnapshot; // Regular neuron snapshot update
    #ManualRefresh; // User-triggered refresh
    #SystemUpdate; // System-triggered update
  };

  // Neuron updates for dao_governance_archive
  public type NeuronUpdateBlockData = {
    id: Nat;
    timestamp: Int;
    updateType: NeuronUpdateType;
    neuronId: Blob;
    oldVotingPower: ?Nat;
    newVotingPower: ?Nat;
    affectedUsers: [Principal]; // Users whose voting power changed
  };

  public type NeuronUpdateType = {
    #Added;
    #Removed;
    #VotingPowerChanged;
    #StateChanged;
  };

  // Reward distribution tracking for reward_distribution_archive
  public type RewardDistributionBlockData = {
    id: Nat;
    timestamp: Int; // distributionTime from DistributionRecord
    startTime: Int;
    endTime: Int;
    totalRewardPot: Nat; // Reward pot in whole TACO tokens
    actualDistributed: Nat; // Actual amount distributed in TACO satoshis
    totalRewardScore: Float;
    neuronsProcessed: Nat;
    neuronRewards: [NeuronReward]; // Individual neuron rewards
    failedNeurons: [FailedNeuron]; // Failed neuron processing
    status: DistributionStatus;
  };

  // Reward withdrawal tracking for reward_withdrawal_archive
  public type RewardWithdrawalBlockData = {
    id: Nat;
    timestamp: Int;
    caller: Principal;
    neuronWithdrawals: [(Blob, Nat)]; // Array of (neuronId, amount withdrawn from it)
    totalAmount: Nat; // Total from all neurons (including fee)
    amountSent: Nat; // Amount actually sent (total - fee)
    fee: Nat; // Fee deducted
    targetAccountOwner: Principal; // ICRC.Account.owner
    targetAccountSubaccount: ?Blob; // ICRC.Account.subaccount
    transactionId: ?Nat; // ICRC1 transaction ID if successful
  };

  // Supporting types for reward archives
  public type NeuronReward = {
    neuronId: Blob;
    performanceScore: Float;
    performanceScoreICP: ?Float;
    votingPower: Nat;
    rewardScore: Float;
    rewardAmount: Nat; // Reward amount in TACO satoshis (integer)
    checkpoints: [CheckpointData]; // Include checkpoints to access maker information
  };

  public type FailedNeuron = {
    neuronId: Blob;
    errorMessage: Text;
  };

  public type DistributionStatus = {
    #InProgress: {currentNeuron: Nat; totalNeurons: Nat};
    #Completed;
    #Failed: Text;
    #PartiallyCompleted: {successfulNeurons: Nat; failedNeurons: Nat};
  };

  public type CheckpointData = {
    timestamp: Int;
    allocations: [Allocation]; // Active allocations at this point
    tokenValues: [(Principal, Float)]; // Per-token values (allocation % × token price)
    totalPortfolioValue: Float; // Sum of all token values
    pricesUsed: [(Principal, PriceInfo)]; // Prices used for this calculation
    maker: ?Principal; // The principal responsible for the allocation at this checkpoint
    reason: ?Text; // The note/reason for this allocation change
  };

  public type PriceInfo = {
    icpPrice: Nat;
    usdPrice: Float;
    timestamp: Int;
  };

  // Supporting types
  public type SnapshotReason = {
    #Scheduled;
    #PostTrade;
    #CircuitBreaker;
    #ManualTrigger;
    #SystemEvent;
  };

  public type CircuitBreakerEventType = {
    #PriceAlert;
    #PortfolioBreaker;
    #TradingPause;
    #SystemEmergency;
  };

  public type PriceSource = {
    #Exchange: ExchangeType;
    #NTN;
    #Aggregated;
    #Oracle;
  };

  public type TradingPauseReason = {
    #PriceVolatility;
    #LiquidityIssue;
    #SystemMaintenance;
    #CircuitBreaker;
    #AdminAction;
  };

  public type AllocationChangeReason = {
    #UserUpdate;
    #FollowAction;
    #SystemRebalance;
    #Emergency;
  };

  public type Allocation = {
    token: Principal;
    basisPoints: Nat;
  };

  // Query filters for retrieving blocks
  public type BlockFilter = {
    blockTypes: ?[TacoBlockType];
    startTime: ?Int;
    endTime: ?Int;
    tokens: ?[Principal];
    traders: ?[Principal];
    minAmount: ?Nat;
    maxAmount: ?Nat;
  };

  // Analytics and reporting types
  public type TradingMetrics = {
    totalTrades: Nat;
    successfulTrades: Nat;
    totalVolume: Nat;
    uniqueTraders: Nat;
    avgTradeSize: Nat;
    avgSlippage: Float;
    topTokensByVolume: [(Principal, Nat)];
    exchangeBreakdown: [(ExchangeType, Nat)];
  };

  public type PortfolioMetrics = {
    snapshots: Nat;
    avgValueICP: Nat;
    valueGrowth: Float;
    volatility: Float;
    allocationChanges: Nat;
    circuitBreakerEvents: Nat;
  };

  // Response types for queries
  public type ArchiveQueryResult = {
    blocks: [Block];
    totalCount: Nat;
    hasMore: Bool;
    nextIndex: ?Nat;
  };

  // Error types
  public type ArchiveError = {
    #NotAuthorized;
    #InvalidBlockType;
    #BlockNotFound;
    #InvalidTimeRange;
    #StorageFull;
    #InvalidData;
    #SystemError: Text;
  };

  // Admin functions
  public type AdminFunction = {
    #ArchiveData;
    #QueryData;
    #DeleteData;
    #UpdateConfig;
    #GetLogs;
    #GetMetrics;
  };

  // Helper functions for converting data to ICRC3 Value format
  public func principalToValue(p: Principal) : Value {
    #Blob(Principal.toBlob(p));
  };

  public func textToValue(t: Text) : Value {
    #Text(t);
  };

  public func natToValue(n: Nat) : Value {
    #Nat(n);
  };

  public func intToValue(i: Int) : Value {
    #Int(i);
  };

  public func floatToValue(f: Float) : Value {
    // Convert float to text for storage
    #Text(Float.toText(f));
  };

  public func boolToValue(b: Bool) : Value {
    if (b) { #Nat(1) } else { #Nat(0) };
  };

  public func exchangeTypeToValue(et: ExchangeType) : Value {
    switch (et) {
      case (#KongSwap) { #Text("KongSwap") };
      case (#ICPSwap) { #Text("ICPSwap") };
    };
  };

  // Create a map entry for ICRC3 blocks
  public func makeMapEntry(key: Text, value: Value) : (Text, Value) {
    (key, value);
  };

  // Convert trade data to ICRC3 Value format
  public func tradeToValue(trade: TradeBlockData, timestamp: Int, phash: ?Blob) : Value {
    let entries = [
      makeMapEntry("btype", #Text("3trade")),
      makeMapEntry("ts", intToValue(timestamp)),
      makeMapEntry("trader", principalToValue(trade.trader)),
      makeMapEntry("token_sold", principalToValue(trade.tokenSold)),
      makeMapEntry("token_bought", principalToValue(trade.tokenBought)),
      makeMapEntry("amount_sold", natToValue(trade.amountSold)),
      makeMapEntry("amount_bought", natToValue(trade.amountBought)),
      makeMapEntry("exchange", exchangeTypeToValue(trade.exchange)),
      makeMapEntry("success", boolToValue(trade.success)),
      makeMapEntry("slippage", floatToValue(trade.slippage)),
      makeMapEntry("fee", natToValue(trade.fee)),
    ];
    
    let entriesWithPhash = switch (phash) {
      case (?hash) { Array.append([makeMapEntry("phash", #Blob(hash))], entries) };
      case (null) { entries };
    };
    
    let entriesWithError = switch (trade.error) {
      case (?err) { Array.append(entriesWithPhash, [makeMapEntry("error", textToValue(err))]) };
      case (null) { entriesWithPhash };
    };

    #Map(entriesWithError);
  };

  // Convert detailed token snapshot to ICRC3 Value format
  private func detailedTokenToValue(token: DetailedTokenSnapshot) : Value {
    let tokenEntries = [
      makeMapEntry("token", principalToValue(token.token)),
      makeMapEntry("balance", natToValue(token.balance)),
      makeMapEntry("decimals", natToValue(token.decimals)),
      makeMapEntry("price_in_icp", natToValue(token.priceInICP)),
      makeMapEntry("price_in_usd", floatToValue(token.priceInUSD)),
      makeMapEntry("value_in_icp", natToValue(token.valueInICP)),
      makeMapEntry("value_in_usd", floatToValue(token.valueInUSD)),
    ];
    #Map(tokenEntries);
  };

  // Convert portfolio data to ICRC3 Value format
  public func portfolioToValue(portfolio: PortfolioBlockData, phash: ?Blob) : Value {
    let detailedTokensArray = #Array(Array.map(portfolio.tokens, detailedTokenToValue));
    let pausedTokensArray = #Array(Array.map(portfolio.pausedTokens, principalToValue));
    
    let reasonText = switch (portfolio.reason) {
      case (#Scheduled) { "scheduled" };
      case (#PostTrade) { "post_trade" };
      case (#CircuitBreaker) { "circuit_breaker" };
      case (#ManualTrigger) { "manual_trigger" };
      case (#SystemEvent) { "system_event" };
    };

    let entries = [
      makeMapEntry("btype", #Text("3portfolio")),
      makeMapEntry("ts", intToValue(portfolio.timestamp)),
      makeMapEntry("total_value_icp", natToValue(portfolio.totalValueICP)),
      makeMapEntry("total_value_usd", floatToValue(portfolio.totalValueUSD)),
      makeMapEntry("token_count", natToValue(portfolio.tokenCount)),
      makeMapEntry("tokens", detailedTokensArray),  // Changed from active_tokens to tokens with detailed data
      makeMapEntry("paused_tokens", pausedTokensArray),
      makeMapEntry("reason", textToValue(reasonText)),
    ];
    
    let entriesWithPhash = switch (phash) {
      case (?hash) { Array.append([makeMapEntry("phash", #Blob(hash))], entries) };
      case (null) { entries };
    };

    #Map(entriesWithPhash);
  };

  // Convert circuit breaker data to ICRC3 Value format
  public func circuitBreakerToValue(circuitBreaker: CircuitBreakerBlockData, timestamp: Int, phash: ?Blob) : Value {
    let eventTypeText = switch (circuitBreaker.eventType) {
      case (#PriceAlert) { "price_alert" };
      case (#PortfolioBreaker) { "portfolio_breaker" };
      case (#TradingPause) { "trading_pause" };
      case (#SystemEmergency) { "system_emergency" };
    };

    let entries = [
      makeMapEntry("btype", #Text("3circuit")),
      makeMapEntry("ts", intToValue(timestamp)),
      makeMapEntry("event_type", textToValue(eventTypeText)),
      makeMapEntry("threshold_value", floatToValue(circuitBreaker.thresholdValue)),
      makeMapEntry("actual_value", floatToValue(circuitBreaker.actualValue)),
      makeMapEntry("system_response", textToValue(circuitBreaker.systemResponse)),
      makeMapEntry("severity", textToValue(circuitBreaker.severity)),
      makeMapEntry("tokens_affected", #Array(Array.map(circuitBreaker.tokensAffected, principalToValue))),
    ];

    let entriesWithTrigger = switch (circuitBreaker.triggerToken) {
      case (?token) { Array.append(entries, [makeMapEntry("trigger_token", principalToValue(token))]) };
      case null { entries };
    };
    
    let entriesWithPhash = switch (phash) {
      case (?hash) { Array.append([makeMapEntry("phash", #Blob(hash))], entriesWithTrigger) };
      case null { entriesWithTrigger };
    };

    #Map(entriesWithPhash);
  };

  // Convert price data to ICRC3 Value format
  public func priceToValue(price: PriceBlockData, timestamp: Int, phash: ?Blob) : Value {
    let sourceText = switch (price.source) {
      case (#Exchange(exch)) { 
        switch (exch) {
          case (#KongSwap) { "KongSwap" };
          case (#ICPSwap) { "ICPSwap" };
        };
      };
      case (#NTN) { "NTN" };
      case (#Aggregated) { "Aggregated" };
      case (#Oracle) { "Oracle" };
    };

    let entries = [
      makeMapEntry("btype", #Text("3price")),
      makeMapEntry("ts", intToValue(timestamp)),
      makeMapEntry("token", principalToValue(price.token)),
      makeMapEntry("price_icp", natToValue(price.priceICP)),
      makeMapEntry("price_usd", floatToValue(price.priceUSD)),
      makeMapEntry("source", textToValue(sourceText)),
    ];
    
    let entriesWithVolume = switch (price.volume24h) {
      case (?vol) { Array.append(entries, [makeMapEntry("volume_24h", natToValue(vol))]) };
      case (null) { entries };
    };
    
    let entriesWithChange = switch (price.change24h) {
      case (?change) { Array.append(entriesWithVolume, [makeMapEntry("change_24h", floatToValue(change))]) };
      case (null) { entriesWithVolume };
    };
    
    let entriesWithPhash = switch (phash) {
      case (?hash) { Array.append([makeMapEntry("phash", #Blob(hash))], entriesWithChange) };
      case (null) { entriesWithChange };
    };

    #Map(entriesWithPhash);
  };

  // Convert trading pause data to ICRC3 Value format
  public func tradingPauseToValue(pause: TradingPauseBlockData, timestamp: Int, phash: ?Blob) : Value {
    let reasonText = switch (pause.reason) {
      case (#PriceVolatility) { "price_volatility" };
      case (#LiquidityIssue) { "liquidity_issue" };
      case (#SystemMaintenance) { "system_maintenance" };
      case (#CircuitBreaker) { "circuit_breaker" };
      case (#AdminAction) { "admin_action" };
    };

    let entries = [
      makeMapEntry("btype", #Text("3pause")),
      makeMapEntry("ts", intToValue(timestamp)),
      makeMapEntry("token", principalToValue(pause.token)),
      makeMapEntry("token_symbol", textToValue(pause.tokenSymbol)),
      makeMapEntry("reason", textToValue(reasonText)),
    ];

    let entriesWithDuration = switch (pause.duration) {
      case (?dur) { Array.append(entries, [makeMapEntry("duration", intToValue(dur))]) };
      case null { entries };
    };
    
    let entriesWithPhash = switch (phash) {
      case (?hash) { Array.append([makeMapEntry("phash", #Blob(hash))], entriesWithDuration) };
      case null { entriesWithDuration };
    };

    #Map(entriesWithPhash);
  };

  // Convert allocation change data to ICRC3 Value format  
  public func allocationToValue(allocation: AllocationBlockData, timestamp: Int, phash: ?Blob) : Value {
    let reasonText = switch (allocation.reason) {
      case (#UserUpdate) { "user_update" };
      case (#FollowAction) { "follow_action" };
      case (#SystemRebalance) { "system_rebalance" };
      case (#Emergency) { "emergency" };
    };

    let oldAllocationArray = #Array(Array.map(allocation.oldAllocation, func(alloc : Allocation) : Value = 
      #Map([
        ("token", principalToValue(alloc.token)),
        ("basis_points", natToValue(alloc.basisPoints))
      ])
    ));

    let newAllocationArray = #Array(Array.map(allocation.newAllocation, func(alloc : Allocation) : Value = 
      #Map([
        ("token", principalToValue(alloc.token)),
        ("basis_points", natToValue(alloc.basisPoints))
      ])
    ));

    let entries = [
      makeMapEntry("btype", #Text("3allocation")),
      makeMapEntry("ts", intToValue(timestamp)),
      makeMapEntry("user", principalToValue(allocation.user)),
      makeMapEntry("old_allocation", oldAllocationArray),
      makeMapEntry("new_allocation", newAllocationArray),
      makeMapEntry("voting_power", natToValue(allocation.votingPower)),
      makeMapEntry("reason", textToValue(reasonText)),
    ];
    
    let entriesWithPhash = switch (phash) {
      case (?hash) { Array.append([makeMapEntry("phash", #Blob(hash))], entries) };
      case null { entries };
    };

    #Map(entriesWithPhash);
  };

  // Interface for ICRC3-compliant archive
  public type ICRC3ArchiveInterface = actor {
    // Standard ICRC3 endpoints
    icrc3_get_archives: query (ICRC3.GetArchivesArgs) -> async ICRC3.GetArchivesResult;
    icrc3_get_tip_certificate: query () -> async ?DataCertificate;
    icrc3_get_blocks: query (GetBlocksArgs) -> async GetBlocksResult;
    icrc3_supported_block_types: query () -> async [ICRC3.BlockType];
    
    // Custom archiving endpoints
    archiveTradeBlock: shared (TradeBlockData) -> async Result.Result<Nat, ArchiveError>;
    archivePortfolioBlock: shared (PortfolioBlockData) -> async Result.Result<Nat, ArchiveError>;
    archiveCircuitBreakerBlock: shared (CircuitBreakerBlockData) -> async Result.Result<Nat, ArchiveError>;
    archivePriceBlock: shared (PriceBlockData) -> async Result.Result<Nat, ArchiveError>;
    archiveTradingPauseBlock: shared (TradingPauseBlockData) -> async Result.Result<Nat, ArchiveError>;
    archiveAllocationBlock: shared (AllocationBlockData) -> async Result.Result<Nat, ArchiveError>;
    
    // Query endpoints
    queryBlocks: query (BlockFilter) -> async Result.Result<ArchiveQueryResult, ArchiveError>;
    getTradingMetrics: query (Int, Int) -> async Result.Result<TradingMetrics, ArchiveError>;
    getPortfolioMetrics: query (Int, Int) -> async Result.Result<PortfolioMetrics, ArchiveError>;
    
    // Admin endpoints
    updateConfig: shared (ArchiveConfig) -> async Result.Result<Text, ArchiveError>;
    getArchiveStatus: query () -> async Result.Result<ArchiveStatus, ArchiveError>;
  };

  // Helper function to convert AdminActionBlockData to Value format
  public func adminActionToValue(action: AdminActionBlockData, _timestamp: Int, _parentHash: ?Blob) : Value {
    #Map([
      ("id", #Nat(action.id)),
      ("timestamp", #Int(action.timestamp)),
      ("admin", principalToValue(action.admin)),
      ("canister", canisterSourceToValue(action.canister)),
      ("actionType", actionVariantToValue(action.actionType)),
      ("reason", #Text(action.reason)),
      ("success", boolToValue(action.success)),
      ("errorMessage", switch (action.errorMessage) {
        case (?msg) { #Text(msg) };
        case null { #Text("") };
      })
    ]);
  };

  // Helper function to convert AdminCanisterSource to Value
  public func canisterSourceToValue(source: AdminCanisterSource) : Value {
    switch (source) {
      case (#DAO_backend) { #Text("DAO_backend") };
      case (#Treasury) { #Text("Treasury") };
    };
  };

  // Helper function to convert AdminActionVariant to Value
  public func actionVariantToValue(actionType: AdminActionVariant) : Value {
    switch (actionType) {
      case (#TokenAdd(details)) { 
        #Map([
          ("type", #Text("TokenAdd")),
          ("token", principalToValue(details.token)),
          ("tokenType", tokenTypeToValue(details.tokenType)),
          ("viaGovernance", boolToValue(details.viaGovernance))
        ]);
      };
      case (#TokenRemove(details)) { 
        #Map([
          ("type", #Text("TokenRemove")),
          ("token", principalToValue(details.token))
        ]);
      };
      case (#TokenDelete(details)) { 
        #Map([
          ("type", #Text("TokenDelete")),
          ("token", principalToValue(details.token))
        ]);
      };
      case (#TokenPause(details)) { 
        #Map([
          ("type", #Text("TokenPause")),
          ("token", principalToValue(details.token))
        ]);
      };
      case (#TokenUnpause(details)) { 
        #Map([
          ("type", #Text("TokenUnpause")),
          ("token", principalToValue(details.token))
        ]);
      };
      case (#SystemStateChange(details)) { 
        #Map([
          ("type", #Text("SystemStateChange")),
          ("oldState", systemStateToValue(details.oldState)),
          ("newState", systemStateToValue(details.newState))
        ]);
      };
      case (#ParameterUpdate(details)) { 
        #Map([
          ("type", #Text("ParameterUpdate")),
          ("parameter", systemParameterToValue(details.parameter)),
          ("oldValue", #Text(details.oldValue)),
          ("newValue", #Text(details.newValue))
        ]);
      };
      case (#AdminPermissionGrant(details)) { 
        #Map([
          ("type", #Text("AdminPermissionGrant")),
          ("targetAdmin", principalToValue(details.targetAdmin)),
          ("function", #Text(details.function)),
          ("durationDays", #Nat(details.durationDays))
        ]);
      };
      case (#AdminAdd(details)) { 
        #Map([
          ("type", #Text("AdminAdd")),
          ("newAdmin", principalToValue(details.newAdmin))
        ]);
      };
      case (#AdminRemove(details)) { 
        #Map([
          ("type", #Text("AdminRemove")),
          ("removedAdmin", principalToValue(details.removedAdmin))
        ]);
      };
      // Treasury actions
      case (#StartRebalancing) { #Map([("type", #Text("StartRebalancing"))]); };
      case (#StopRebalancing) { #Map([("type", #Text("StopRebalancing"))]); };
      case (#ResetRebalanceState) { #Map([("type", #Text("ResetRebalanceState"))]); };
      case (#UpdateRebalanceConfig(details)) { 
        #Map([
          ("type", #Text("UpdateRebalanceConfig")),
          ("oldConfig", #Text(details.oldConfig)),
          ("newConfig", #Text(details.newConfig))
        ]);
      };
      case (#PauseTokenManual(details)) { 
        #Map([
          ("type", #Text("PauseTokenManual")),
          ("token", principalToValue(details.token)),
          ("pauseType", #Text(details.pauseType))
        ]);
      };
      case (#UnpauseToken(details)) { 
        #Map([
          ("type", #Text("UnpauseToken")),
          ("token", principalToValue(details.token))
        ]);
      };
      case (#ClearAllTradingPauses) { #Map([("type", #Text("ClearAllTradingPauses"))]); };
      case (#AddTriggerCondition(details)) { 
        #Map([
          ("type", #Text("AddTriggerCondition")),
          ("conditionId", #Nat(details.conditionId)),
          ("conditionType", #Text(details.conditionType)),
          ("details", #Text(details.details))
        ]);
      };
      case (#RemoveTriggerCondition(details)) { 
        #Map([
          ("type", #Text("RemoveTriggerCondition")),
          ("conditionId", #Nat(details.conditionId))
        ]);
      };
      case (#UpdateTriggerCondition(details)) { 
        #Map([
          ("type", #Text("UpdateTriggerCondition")),
          ("conditionId", #Nat(details.conditionId)),
          ("oldCondition", #Text(details.oldCondition)),
          ("newCondition", #Text(details.newCondition))
        ]);
      };
      case (#SetTriggerConditionActive(details)) { 
        #Map([
          ("type", #Text("SetTriggerConditionActive")),
          ("conditionId", #Nat(details.conditionId)),
          ("isActive", boolToValue(details.isActive))
        ]);
      };
      case (#ClearPriceAlerts) { #Map([("type", #Text("ClearPriceAlerts"))]); };
      case (#AddPortfolioCircuitBreaker(details)) { 
        #Map([
          ("type", #Text("AddPortfolioCircuitBreaker")),
          ("conditionId", #Nat(details.conditionId)),
          ("conditionType", #Text(details.conditionType)),
          ("details", #Text(details.details))
        ]);
      };
      case (#RemovePortfolioCircuitBreaker(details)) { 
        #Map([
          ("type", #Text("RemovePortfolioCircuitBreaker")),
          ("conditionId", #Nat(details.conditionId))
        ]);
      };
      case (#UpdatePortfolioCircuitBreaker(details)) { 
        #Map([
          ("type", #Text("UpdatePortfolioCircuitBreaker")),
          ("conditionId", #Nat(details.conditionId)),
          ("oldCondition", #Text(details.oldCondition)),
          ("newCondition", #Text(details.newCondition))
        ]);
      };
      case (#SetPortfolioCircuitBreakerActive(details)) { 
        #Map([
          ("type", #Text("SetPortfolioCircuitBreakerActive")),
          ("conditionId", #Nat(details.conditionId)),
          ("isActive", boolToValue(details.isActive))
        ]);
      };
      case (#UpdatePausedTokenThreshold(details)) { 
        #Map([
          ("type", #Text("UpdatePausedTokenThreshold")),
          ("oldThreshold", #Nat(details.oldThreshold)),
          ("newThreshold", #Nat(details.newThreshold))
        ]);
      };
      case (#ClearPortfolioCircuitBreakerLogs) { #Map([("type", #Text("ClearPortfolioCircuitBreakerLogs"))]); };
      case (#UpdateMaxPortfolioSnapshots(details)) { 
        #Map([
          ("type", #Text("UpdateMaxPortfolioSnapshots")),
          ("oldLimit", #Nat(details.oldLimit)),
          ("newLimit", #Nat(details.newLimit))
        ]);
      };
      case (#SetTestMode(details)) { 
        #Map([
          ("type", #Text("SetTestMode")),
          ("isTestMode", boolToValue(details.isTestMode))
        ]);
      };
      case (#ClearSystemLogs) { #Map([("type", #Text("ClearSystemLogs"))]); };
      case (#StartPortfolioSnapshots) { #Map([("type", #Text("StartPortfolioSnapshots"))]); };
      case (#StopPortfolioSnapshots) { #Map([("type", #Text("StopPortfolioSnapshots"))]); };
      case (#UpdatePortfolioSnapshotInterval(details)) { 
        #Map([
          ("type", #Text("UpdatePortfolioSnapshotInterval")),
          ("oldIntervalNS", #Nat(details.oldIntervalNS)),
          ("newIntervalNS", #Nat(details.newIntervalNS))
        ]);
      };
      case (#TakeManualSnapshot) { #Map([("type", #Text("TakeManualSnapshot"))]); };
      case (#ExecuteTradingCycle) { #Map([("type", #Text("ExecuteTradingCycle"))]); };
      case (#CanisterStart) { #Map([("type", #Text("CanisterStart"))]); };
      case (#CanisterStop) { #Map([("type", #Text("CanisterStop"))]); };
    };
  };

  // Helper functions for converting DAO types to Value
  public func tokenTypeToValue(tokenType: DAOTypes.TokenType) : Value {
    switch (tokenType) {
      case (#ICP) { #Text("ICP") };
      case (#ICRC12) { #Text("ICRC12") };
      case (#ICRC3) { #Text("ICRC3") };
    };
  };

  public func systemStateToValue(state: DAOTypes.SystemState) : Value {
    switch (state) {
      case (#Active) { #Text("Active") };
      case (#Paused) { #Text("Paused") };
      case (#Emergency) { #Text("Emergency") };
    };
  };

  public func systemParameterToValue(param: DAOTypes.SystemParameter) : Value {
    switch (param) {
      case (#FollowDepth(n)) { #Text("FollowDepth(" # debug_show(n) # ")") };
      case (#MaxFollowers(n)) { #Text("MaxFollowers(" # debug_show(n) # ")") };
      case (#MaxPastAllocations(n)) { #Text("MaxPastAllocations(" # debug_show(n) # ")") };
      case (#SnapshotInterval(n)) { #Text("SnapshotInterval(" # debug_show(n) # ")") };
      case (#MaxTotalUpdates(n)) { #Text("MaxTotalUpdates(" # debug_show(n) # ")") };
      case (#MaxAllocationsPerDay(n)) { #Text("MaxAllocationsPerDay(" # debug_show(n) # ")") };
      case (#AllocationWindow(n)) { #Text("AllocationWindow(" # debug_show(n) # ")") };
      case (#MaxFollowUnfollowActionsPerDay(n)) { #Text("MaxFollowUnfollowActionsPerDay(" # debug_show(n) # ")") };
      case (#MaxFollowed(n)) { #Text("MaxFollowed(" # debug_show(n) # ")") };
      case (#LogAdmin(p)) { #Text("LogAdmin(" # Principal.toText(p) # ")") };
    };
  };

  // Helper functions for converting new block data types to Value format

  // Convert AllocationChangeBlockData to Value
  public func allocationChangeToValue(change: AllocationChangeBlockData, _timestamp: Int, _parentHash: ?Blob) : Value {
    #Map([
      ("id", #Nat(change.id)),
      ("timestamp", #Int(change.timestamp)),
      ("user", principalToValue(change.user)),
      ("changeType", allocationChangeTypeToValue(change.changeType)),
      ("oldAllocations", #Array(Array.map(change.oldAllocations, allocationToValueHelper))),
      ("newAllocations", #Array(Array.map(change.newAllocations, allocationToValueHelper))),
      ("votingPower", #Nat(change.votingPower)),
      ("maker", principalToValue(change.maker)),
      ("reason", switch (change.reason) { case (?r) { #Text(r) }; case null { #Text("") }; })
    ]);
  };

  // Convert FollowActionBlockData to Value
  public func followActionToValue(action: FollowActionBlockData, _timestamp: Int, _parentHash: ?Blob) : Value {
    #Map([
      ("id", #Nat(action.id)),
      ("timestamp", #Int(action.timestamp)),
      ("follower", principalToValue(action.follower)),
      ("followed", principalToValue(action.followed)),
      ("action", followActionTypeToValue(action.action)),
      ("previousFollowCount", #Nat(action.previousFollowCount)),
      ("newFollowCount", #Nat(action.newFollowCount))
    ]);
  };

  // Convert VotingPowerBlockData to Value
  public func votingPowerToValue(power: VotingPowerBlockData, _timestamp: Int, _parentHash: ?Blob) : Value {
    #Map([
      ("id", #Nat(power.id)),
      ("timestamp", #Int(power.timestamp)),
      ("user", principalToValue(power.user)),
      ("changeType", votingPowerChangeTypeToValue(power.changeType)),
      ("oldVotingPower", #Nat(power.oldVotingPower)),
      ("newVotingPower", #Nat(power.newVotingPower)),
      ("neurons", #Array(Array.map(power.neurons, neuronVPToValue)))
    ]);
  };

  // Convert NeuronUpdateBlockData to Value
  public func neuronUpdateToValue(update: NeuronUpdateBlockData, _timestamp: Int, _parentHash: ?Blob) : Value {
    #Map([
      ("id", #Nat(update.id)),
      ("timestamp", #Int(update.timestamp)),
      ("updateType", neuronUpdateTypeToValue(update.updateType)),
      ("neuronId", #Blob(update.neuronId)),
      ("oldVotingPower", switch (update.oldVotingPower) { case (?n) { #Nat(n) }; case null { #Nat(0) }; }),
      ("newVotingPower", switch (update.newVotingPower) { case (?n) { #Nat(n) }; case null { #Nat(0) }; }),
      ("affectedUsers", #Array(Array.map(update.affectedUsers, principalToValue)))
    ]);
  };

  // Convert RewardDistributionBlockData to Value
  public func rewardDistributionToValue(distribution: RewardDistributionBlockData, _timestamp: Int, _parentHash: ?Blob) : Value {
    #Map([
      ("id", #Nat(distribution.id)),
      ("timestamp", #Int(distribution.timestamp)),
      ("startTime", #Int(distribution.startTime)),
      ("endTime", #Int(distribution.endTime)),
      ("totalRewardPot", #Nat(distribution.totalRewardPot)),
      ("actualDistributed", #Nat(distribution.actualDistributed)),
      ("totalRewardScore", floatToValue(distribution.totalRewardScore)),
      ("neuronsProcessed", #Nat(distribution.neuronsProcessed)),
      ("neuronRewards", #Array(Array.map(distribution.neuronRewards, neuronRewardToValue))),
      ("failedNeurons", #Array(Array.map(distribution.failedNeurons, failedNeuronToValue))),
      ("status", distributionStatusToValue(distribution.status))
    ]);
  };

  // Convert RewardWithdrawalBlockData to Value
  public func rewardWithdrawalToValue(withdrawal: RewardWithdrawalBlockData, _timestamp: Int, _parentHash: ?Blob) : Value {
    #Map([
      ("id", #Nat(withdrawal.id)),
      ("timestamp", #Int(withdrawal.timestamp)),
      ("caller", principalToValue(withdrawal.caller)),
      ("neuronWithdrawals", #Array(Array.map(withdrawal.neuronWithdrawals, neuronWithdrawalToValue))),
      ("totalAmount", #Nat(withdrawal.totalAmount)),
      ("amountSent", #Nat(withdrawal.amountSent)),
      ("fee", #Nat(withdrawal.fee)),
      ("targetAccountOwner", principalToValue(withdrawal.targetAccountOwner)),
      ("targetAccountSubaccount", switch (withdrawal.targetAccountSubaccount) { case (?sub) { #Blob(sub) }; case null { #Text("") }; }),
      ("transactionId", switch (withdrawal.transactionId) { case (?id) { #Nat(id) }; case null { #Nat(0) }; })
    ]);
  };

  // Helper conversion functions for reward archive types
  private func neuronRewardToValue(reward: NeuronReward) : Value {
    #Map([
      ("neuronId", #Blob(reward.neuronId)),
      ("performanceScore", floatToValue(reward.performanceScore)),
      ("performanceScoreICP", switch (reward.performanceScoreICP) { case (?v) { floatToValue(v) }; case null { #Text("") }; }),
      ("votingPower", #Nat(reward.votingPower)),
      ("rewardScore", floatToValue(reward.rewardScore)),
      ("rewardAmount", #Nat(reward.rewardAmount)),
      ("checkpoints", #Array(Array.map(reward.checkpoints, checkpointDataToValue)))
    ]);
  };

  private func failedNeuronToValue(failed: FailedNeuron) : Value {
    #Map([
      ("neuronId", #Blob(failed.neuronId)),
      ("errorMessage", #Text(failed.errorMessage))
    ]);
  };

  private func distributionStatusToValue(status: DistributionStatus) : Value {
    switch (status) {
      case (#InProgress(details)) {
        #Map([
          ("type", #Text("InProgress")),
          ("currentNeuron", #Nat(details.currentNeuron)),
          ("totalNeurons", #Nat(details.totalNeurons))
        ]);
      };
      case (#Completed) {
        #Map([("type", #Text("Completed"))]);
      };
      case (#Failed(msg)) {
        #Map([
          ("type", #Text("Failed")),
          ("message", #Text(msg))
        ]);
      };
      case (#PartiallyCompleted(details)) {
        #Map([
          ("type", #Text("PartiallyCompleted")),
          ("successfulNeurons", #Nat(details.successfulNeurons)),
          ("failedNeurons", #Nat(details.failedNeurons))
        ]);
      };
    };
  };

  private func neuronWithdrawalToValue(withdrawal: (Blob, Nat)) : Value {
    #Map([
      ("neuronId", #Blob(withdrawal.0)),
      ("amount", #Nat(withdrawal.1))
    ]);
  };

  private func checkpointDataToValue(checkpoint: CheckpointData) : Value {
    #Map([
      ("timestamp", #Int(checkpoint.timestamp)),
      ("allocations", #Array(Array.map(checkpoint.allocations, allocationToValueHelper))),
      ("tokenValues", #Array(Array.map(checkpoint.tokenValues, tokenValueToValue))),
      ("totalPortfolioValue", floatToValue(checkpoint.totalPortfolioValue)),
      ("pricesUsed", #Array(Array.map(checkpoint.pricesUsed, priceInfoEntryToValue))),
      ("maker", switch (checkpoint.maker) { case (?p) { principalToValue(p) }; case null { #Text("") }; }),
      ("reason", switch (checkpoint.reason) { case (?r) { #Text(r) }; case null { #Text("") }; })
    ]);
  };

  private func tokenValueToValue(tokenValue: (Principal, Float)) : Value {
    #Map([
      ("token", principalToValue(tokenValue.0)),
      ("value", floatToValue(tokenValue.1))
    ]);
  };

  private func priceInfoEntryToValue(priceEntry: (Principal, PriceInfo)) : Value {
    #Map([
      ("token", principalToValue(priceEntry.0)),
      ("priceInfo", priceInfoToValue(priceEntry.1))
    ]);
  };

  private func priceInfoToValue(priceInfo: PriceInfo) : Value {
    #Map([
      ("icpPrice", #Nat(priceInfo.icpPrice)),
      ("usdPrice", floatToValue(priceInfo.usdPrice)),
      ("timestamp", #Int(priceInfo.timestamp))
    ]);
  };

  // Helper conversion functions for new types
  private func allocationChangeTypeToValue(changeType: AllocationChangeType) : Value {
    switch (changeType) {
      case (#UserUpdate(details)) { 
        #Map([
          ("type", #Text("UserUpdate")),
          ("userInitiated", boolToValue(details.userInitiated))
        ]);
      };
      case (#FollowAction(details)) { 
        #Map([
          ("type", #Text("FollowAction")),
          ("followedUser", principalToValue(details.followedUser))
        ]);
      };
      case (#SystemRebalance) { #Map([("type", #Text("SystemRebalance"))]); };
      case (#VotingPowerChange) { #Map([("type", #Text("VotingPowerChange"))]); };
    };
  };

  private func followActionTypeToValue(actionType: FollowActionType) : Value {
    switch (actionType) {
      case (#Follow) { #Text("Follow") };
      case (#Unfollow) { #Text("Unfollow") };
    };
  };

  private func votingPowerChangeTypeToValue(changeType: VotingPowerChangeType) : Value {
    switch (changeType) {
      case (#NeuronSnapshot) { #Text("NeuronSnapshot") };
      case (#ManualRefresh) { #Text("ManualRefresh") };
      case (#SystemUpdate) { #Text("SystemUpdate") };
    };
  };

  private func neuronUpdateTypeToValue(updateType: NeuronUpdateType) : Value {
    switch (updateType) {
      case (#Added) { #Text("Added") };
      case (#Removed) { #Text("Removed") };
      case (#VotingPowerChanged) { #Text("VotingPowerChanged") };
      case (#StateChanged) { #Text("StateChanged") };
    };
  };

  private func allocationToValueHelper(allocation: DAOTypes.Allocation) : Value {
    #Map([
      ("token", principalToValue(allocation.token)),
      ("basisPoints", #Nat(allocation.basisPoints))
    ]);
  };

  private func neuronVPToValue(neuron: DAOTypes.NeuronVP) : Value {
    #Map([
      ("neuronId", #Blob(neuron.neuronId)),
      ("votingPower", #Nat(neuron.votingPower))
    ]);
  };
} 