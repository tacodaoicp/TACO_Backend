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

### Phase 1: Foundation (COMPLETED ✅)
**Status**: Implemented and tested  
**Contexts**: PORTFOLIO_STATE, REBALANCE_CYCLE  

**Accomplishments**:
- **PORTFOLIO_STATE**: Implemented `logPortfolioState()` function with comprehensive portfolio snapshots
  - Per-token balance, price, and value tracking in both ICP and USD
  - Token status tracking (active/paused/sync failures)
  - Portfolio summary statistics and totals
  - Formatted display for easy readability
- **REBALANCE_CYCLE**: Added cycle-level tracking throughout trading system
  - Trading cycle initialization and configuration display
  - Attempt tracking with detailed reasoning
  - Success/failure/no-pairs outcome logging
  - Exception handling with context
  - Integrated into `do_executeTradingCycle()` and `do_executeTradingStep()`

### Phase 2: Core Logic (COMPLETED ✅)
**Status**: Implemented and tested  
**Contexts**: ALLOCATION_ANALYSIS, PAIR_SELECTION  

**Accomplishments**:
- **ALLOCATION_ANALYSIS**: Detailed logging in `calculateTradeRequirements()`
  - Target allocation analysis from DAO with basis points and percentages
  - Current vs target allocation comparisons
  - Per-token over/underweight calculations with imbalance statistics
  - Portfolio value calculations and active token counts
  - Summary statistics: overweight/underweight/balanced token counts
- **PAIR_SELECTION**: Comprehensive pair selection logging in `selectTradingPair()`
  - Candidate filtering with sell/buy token separation
  - Weighted random selection process with actual random numbers
  - Final pair selection with detailed reasoning
  - Edge case handling (insufficient candidates, no valid pairs)
  - All using purely additive approach without modifying existing logic

### Phase 3: Execution Details (COMPLETED ✅)
**Status**: Implemented and tested  
**Contexts**: EXCHANGE_COMPARISON, TRADE_EXECUTION  

**Accomplishments**:
- **EXCHANGE_COMPARISON**: Complete exchange comparison logging in `findBestExecution()`
  - Trade size calculation and formatting for display
  - KongSwap quote request/response with success/failure tracking
  - ICPSwap pool validation and quote processing
  - Best execution selection logic with detailed comparison
  - Final exchange selection with reasoning
  - Exception handling for both exchanges
- **TRADE_EXECUTION**: Detailed execution logging in `executeTrade()`
  - Trade execution start with comprehensive parameter display
  - KongSwap-specific execution steps:
    - Swap parameter preparation and validation
    - Transfer and swap execution tracking
    - Success/failure logging with slippage and timing
  - ICPSwap-specific execution steps:
    - Pool validation and fee checking
    - Deposit, swap, and withdrawal parameter preparation
    - Multi-step execution tracking
    - Success/failure logging with effective slippage calculation
  - General exception handling with execution time tracking

### Phase 4: Support Features (PENDING)
**Status**: Not yet implemented  
**Contexts**: PRICE_UPDATES, BALANCE_SYNC  

**Next Steps**:
5. **PRICE_UPDATES** - Price discovery and update logging
   - Real-time price fetching from exchanges
   - NTN service price synchronization
   - Price history management and validation
   - Cross-exchange price comparison

6. **BALANCE_SYNC** - Balance synchronization and validation
   - Token balance fetching and validation
   - Sync failure detection and handling
   - Balance discrepancy identification
   - Recovery mechanism logging

## Review Process
- Each logging implementation should be reviewed against this spec
- Logging should not change any business logic
- Performance impact should be measured and documented
- Log volume and storage impact should be monitored
