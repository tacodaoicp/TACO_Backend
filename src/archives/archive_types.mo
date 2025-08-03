import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Float "mo:base/Float";
import ICRC3 "mo:icrc3-mo/service";
import TreasuryTypes "../treasury/treasury_types";

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
} 