# Treasury Verbose Logging Specification

## Overview
This document specifies comprehensive logging for the Treasury's automatic rebalancing system. The goal is to provide complete visibility into every trade decision and execution step, while maintaining the ability to disable/remove this logging later when the system is stable.

## Design Principles
1. **Additive Only**: Only add new logging lines, never modify existing code logic
2. **Comprehensive**: Log every decision point in the trading process
3. **Contextual**: Use consistent context strings for easy filtering
4. **Removable**: All verbose logging should be easily identifiable and removable
5. **Performance Aware**: Minimize computational overhead of logging operations

## Logging Contexts
We'll use these context strings to organize logs:

- `"REBALANCE_CYCLE"` - Overall trading cycle management
- `"PORTFOLIO_STATE"` - Portfolio composition and valuation
- `"ALLOCATION_ANALYSIS"` - Current vs target allocation calculations
- `"PAIR_SELECTION"` - Trading pair selection logic
- `"EXCHANGE_COMPARISON"` - DEX quote comparison and selection
- `"TRADE_EXECUTION"` - Actual trade execution process
- `"PRICE_UPDATES"` - Price discovery and updates
- `"BALANCE_SYNC"` - Balance and metadata synchronization

## Detailed Logging Requirements

### 1. Trading Cycle Initialization (`REBALANCE_CYCLE`)

**Location**: `executeTradingCycle()` and `executeTradingStep()`

**Log Events**:
- Cycle start with timestamp and attempt number
- Current rebalance state and configuration
- Any failed transaction retries being processed
- Cycle completion with success/failure status

**Data to Log**:
```
- Cycle ID/timestamp
- Current status (#Trading, #Idle, etc.)
- Max attempts configured vs current attempt
- Number of pending/failed transactions
- Time since last successful trade
```

### 2. Portfolio State Analysis (`PORTFOLIO_STATE`)

**Location**: Start of `calculateTradeRequirements()` and `executeTradingStep()`

**Log Events**:
- Complete portfolio snapshot before trade analysis
- Total portfolio value in ICP and USD
- Individual token balances and values
- Active vs paused token status

**Data to Log**:
```
- Total portfolio value (ICP/USD)
- Per-token breakdown:
  - Principal ID
  - Symbol
  - Balance (raw and formatted)
  - Price (ICP/USD)
  - Value (ICP/USD)
  - Status (Active/Paused/PausedDueToSync)
  - Last sync time
```

### 3. Allocation Analysis (`ALLOCATION_ANALYSIS`)

**Location**: Throughout `calculateTradeRequirements()`

**Log Events**:
- Target allocations from DAO
- Current allocations (basis points)
- Allocation differences (over/under-weight)
- Tokens excluded due to pausing/sync failures

**Data to Log**:
```
- Per-token analysis:
  - Target allocation (basis points)
  - Current allocation (basis points)  
  - Difference (+ for underweight, - for overweight)
  - Absolute difference value
  - Inclusion status (active/paused/excluded)
- Summary statistics:
  - Total active tokens
  - Total target basis points
  - Largest overweight position
  - Largest underweight position
```

### 4. Trading Pair Selection (`PAIR_SELECTION`)

**Location**: `selectTradingPair()` function

**Log Events**:
- Candidates for selling (overweight tokens)
- Candidates for buying (underweight tokens)
- Weighted random selection process
- Final pair selection with reasoning

**Data to Log**:
```
- Sell candidates:
  - Token symbols and principals
  - Overweight amount (basis points)
  - Selection weights
- Buy candidates:
  - Token symbols and principals  
  - Underweight amount (basis points)
  - Selection weights
- Selection process:
  - Total sell weight
  - Total buy weight
  - Random numbers generated
  - Selected sell token with reason
  - Selected buy token with reason
  - Final pair decision
```

### 5. Exchange Comparison (`EXCHANGE_COMPARISON`)

**Location**: `findBestExecution()` function

**Log Events**:
- Trade size calculation
- KongSwap quote request and response
- ICPSwap quote request and response  
- Quote comparison and best execution selection

**Data to Log**:
```
- Trade parameters:
  - Sell token symbol
  - Buy token symbol
  - Amount to trade (raw and formatted)
  - Max slippage tolerance
- KongSwap analysis:
  - Quote request details
  - Response (amount out, slippage, fees)
  - Success/failure status
  - Error messages if any
- ICPSwap analysis:
  - Pool existence check
  - Quote request details
  - Response (amount out, slippage, fees)  
  - Success/failure status
  - Error messages if any
- Best execution decision:
  - Winning exchange
  - Expected output amount
  - Expected slippage
  - Reasoning for selection
```

### 6. Trade Execution (`TRADE_EXECUTION`)

**Location**: `executeTrade()` function

**Log Events**:
- Trade execution start
- Exchange-specific execution steps
- Slippage protection calculations
- Trade result processing
- Balance and price updates

**Data to Log**:
```
- Pre-execution state:
  - Selected exchange
  - Trade parameters
  - Minimum amount out calculation
  - Deadline settings
- Execution progress:
  - Transfer steps (if needed)
  - Swap execution
  - Withdrawal steps (if needed)
- Post-execution analysis:
  - Actual amounts traded
  - Actual slippage vs expected
  - Execution time
  - Success/failure status
  - Error details if failed
```

### 7. Price Updates (`PRICE_UPDATES`)

**Location**: Price update logic after successful trades

**Log Events**:
- Price calculation methodology
- New price derivation from trade data
- Price history updates
- Cross-token price impacts

**Data to Log**:
```
- Price calculation:
  - Which token's price is being updated
  - Calculation method (ICP-based, ratio-based, etc.)
  - Input values used
  - Calculated new price
  - Previous price for comparison
- Price history:
  - New entry added to history
  - History size management
  - Oldest entry removed (if any)
```

### 8. Balance Synchronization (`BALANCE_SYNC`)

**Location**: Balance and metadata update functions

**Log Events**:
- Sync operation triggers
- Individual token balance updates
- Metadata refresh operations
- Sync failures and recovery

**Data to Log**:
```
- Sync trigger:
  - Sync type (short/long interval)
  - Tokens being synchronized
  - Last sync timestamps
- Balance updates:
  - Previous balance vs new balance
  - Balance change amount
  - Update success/failure
- Metadata updates:
  - Fields being updated
  - Old vs new values
  - Update source (ledger/DAO/NTN)
```

## Implementation Guidelines

### Log Level Usage
- **INFO**: Normal operation events, successful operations
- **WARN**: Unexpected conditions that don't stop execution
- **ERROR**: Failures that affect trading operations

### Message Format
All verbose log messages should follow this pattern:
```
logger.info("CONTEXT", "Brief description: detailed_data", "function_name");
```

### Data Formatting
- Use `debug_show()` for complex data structures
- Format large numbers with separators where helpful
- Include units (ICP, USD, basis points) in all numeric data
- Use consistent token identification (symbol + principal)

### Performance Considerations
- Minimize string concatenation in hot paths
- Use conditional logging for expensive data preparation
- Consider batching related log entries
- Avoid logging in tight loops without throttling

## Future Considerations

### Logging Controls
- Add a `verboseLogging` flag to enable/disable all verbose logging
- Consider log level filtering for verbose logs
- Implement context-based filtering

### Removal Strategy
- All verbose logging calls will be tagged with comments for easy identification
- Use consistent prefixes in log messages for automated removal
- Maintain this specification for future reference

## Implementation Plan

### Phase 1: Foundation ✅ COMPLETE
1. **PORTFOLIO_STATE** - Implement portfolio snapshots at key decision points
   - ✅ Pre-trade portfolio analysis snapshots
   - ✅ Post-trade portfolio state logging
   - ✅ Per-token balance, price, and status details
   - ✅ Portfolio value summaries in ICP and USD

2. **REBALANCE_CYCLE** - Add cycle-level tracking
   - ✅ Trading cycle initialization logging
   - ✅ Attempt tracking with configuration details
   - ✅ Success/failure/no-pairs outcome logging
   - ✅ Exception handling and recovery logging

### Phase 2: Core Trading Logic ✅ COMPLETE
3. **ALLOCATION_ANALYSIS** - Detail the allocation calculations
   - ✅ Target allocation analysis from DAO
   - ✅ Current vs target allocation comparisons
   - ✅ Per-token over/underweight calculations
   - ✅ Summary statistics (max imbalances, counts)

4. **PAIR_SELECTION** - Log the weighted random selection process
   - ✅ Candidate filtering and validation
   - ✅ Sell/buy candidate separation
   - ✅ Weighted random selection process
   - ✅ Final pair selection with reasoning

### Phase 3: Execution Details (NEXT)
5. **EXCHANGE_COMPARISON** - Compare DEX quotes and selection
   - Important for optimizing execution
   - Shows market conditions and liquidity
   - Helps validate exchange selection logic

6. **TRADE_EXECUTION** - Detailed execution steps
   - Critical for debugging failed trades
   - Shows actual vs expected results
   - Tracks slippage and timing

### Phase 4: Supporting Systems
7. **PRICE_UPDATES** - Price calculation and updates
   - Important for understanding price discovery
   - Shows impact of trades on internal prices
   - Helps validate price update logic

8. **BALANCE_SYNC** - Synchronization operations
   - Important for system reliability
   - Shows data freshness and sync failures
   - Helps optimize sync intervals

### Testing and Refinement Strategy
- Implement one context at a time
- Test each context thoroughly before moving to the next
- Monitor log volume and performance impact
- Refine message formats and data selection
- Get feedback on log usefulness before proceeding

## Review Process
- Each logging implementation should be reviewed against this spec
- Logging should not change any business logic
- Performance impact should be measured and documented
- Log volume and storage impact should be monitored
