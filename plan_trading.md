# Treasury Trading Cycle - Complete Decision Tree & Calculations

## Overview

The trading cycle is triggered every 60 seconds via `executeTradingCycle()` at [treasury.mo:3673](src/treasury/treasury.mo#L3673). It attempts to rebalance the portfolio by trading overweight tokens for underweight tokens across two DEXs (KongSwap and ICPSwap).

---

## Decision Tree

```
executeTradingCycle() [Line 3673]
│
├─► CHECK: rebalanceState.status == #Idle?
│   └─► YES → RETURN (no trading)
│
├─► checkPortfolioCircuitBreakerConditions()
│   └─► May pause tokens if price/portfolio thresholds exceeded
│
└─► do_executeTradingCycle() [Line 3684]
    │
    ├─► Update lastRebalanceAttempt timestamp
    ├─► updateBalances() - sync token balances from ledgers
    ├─► retryFailedKongswapTransactions() - retry pending Kong txs
    │
    └─► executeTradingStep() [Line 3780]
        │
        └─► CHECK: status == #Trading?
            └─► NO → RETURN
            └─► YES → do_executeTradingStep() [Line 3788]
                │
                ├─► takePortfolioSnapshot(#PreTrade)
                ├─► checkPortfolioCircuitBreakerConditions()
                │
                └─► LOOP: attempts < maxTradeAttemptsPerInterval (default: 2)
                    │
                    ├─► calculateTradeRequirements() [Line 4972]
                    │   │
                    │   ├─► PASS 1: Identify active tokens
                    │   │   └─► Token included if: (Active OR balance > 0) AND NOT paused
                    │   │
                    │   ├─► PASS 2: Calculate total portfolio value
                    │   │   └─► valueInICP = (balance * priceInICP) / 10^decimals
                    │   │   └─► totalValueICP += valueInICP (if not paused)
                    │   │
                    │   ├─► PASS 3: Calculate allocation differences
                    │   │   └─► currentBP = (valueInICP * 10000) / totalValueICP
                    │   │   └─► targetBP = (targetAlloc * 10000) / totalTargetBP
                    │   │   └─► diffBP = targetBP - currentBP
                    │   │
                    │   └─► FILTER: |diffBP| > minAllocationDiffBasisPoints (15bp)?
                    │       ├─► NO → Exclude from trading
                    │       └─► YES → Add to tradePairs[(token, diff, value)]
                    │
                    ├─► CHECK: tradeDiffs.size() == 0?
                    │   └─► YES → incrementSkipCounter(#tokensFiltered)
                    │            status := #Idle, CONTINUE loop
                    │
                    ├─► selectTradingPair(tradeDiffs) [Line 5210]
                    │   │
                    │   ├─► CHECK: tradeDiffs.size() < 2?
                    │   │   └─► YES → incrementSkipCounter(#insufficientCandidates), RETURN null
                    │   │
                    │   ├─► Split into toSell (diff < 0) and toBuy (diff > 0)
                    │   │
                    │   ├─► CHECK: toSell.size() == 0 OR toBuy.size() == 0?
                    │   │   └─► YES → RETURN null
                    │   │
                    │   └─► WEIGHTED RANDOM SELECTION:
                    │       ├─► totalSellWeight = sum of |diff| for all sell candidates
                    │       ├─► totalBuyWeight = sum of |diff| for all buy candidates
                    │       ├─► sellRandom = random(0, totalSellWeight)
                    │       ├─► buyRandom = random(0, totalBuyWeight)
                    │       └─► Select tokens where cumulative weight >= random value
                    │
                    ├─► CHECK: pair == null?
                    │   └─► YES → CONTINUE to next attempt
                    │
                    ├─► TRADE SIZE DETERMINATION:
                    │   │
                    │   ├─► shouldUseExactTargeting() [Line 5352]
                    │   │   │
                    │   │   ├─► halfMaxTradeBP = (maxTradeValueICP * 10000 / 2) / totalPortfolioValue
                    │   │   ├─► sellCloseToTarget = |sellDiff| <= halfMaxTradeBP
                    │   │   ├─► buyCloseToTarget = |buyDiff| <= halfMaxTradeBP
                    │   │   └─► RETURN: sellCloseToTarget OR buyCloseToTarget
                    │   │
                    │   ├─► IF useExactTargeting:
                    │   │   └─► calculateExactTargetTradeSize() [Line 5379]
                    │   │       │
                    │   │       ├─► Choose token closer to target
                    │   │       │
                    │   │       ├─► IF targeting sell token:
                    │   │       │   └─► excessValueICP = (|sellDiff| * totalValue) / 10000
                    │   │       │   └─► exactSize = (excessValue * 10^decimals) / priceInICP
                    │   │       │
                    │   │       ├─► IF targeting buy token:
                    │   │       │   └─► deficitValueICP = (buyDiff * totalValue) / 10000
                    │   │       │   └─► exactSize = (deficitValue * 10^decimals) / priceInICP
                    │   │       │
                    │   │       └─► CHECK: tradeSizeICP > maxTradeValueICP?
                    │   │           ├─► YES → RETURN (calculateTradeSizeMinMax(), false)
                    │   │           └─► NO → RETURN (exactSize, true)
                    │   │
                    │   └─► ELSE (random sizing):
                    │       └─► calculateTradeSizeMinMax() [Line 5340]
                    │           └─► range = maxTradeValueICP - minTradeValueICP
                    │           └─► RETURN: minTradeValueICP + random(0, range)
                    │
                    ├─► findBestExecution() [Line 5558]
                    │   │
                    │   ├─► GET 10 QUOTES IN PARALLEL (20%, 40%, 60%, 80%, 100%)
                    │   │   ├─► KongSwap: 5 quotes
                    │   │   └─► ICPSwap: 5 quotes
                    │   │
                    │   ├─► EXTRACT quote data:
                    │   │   └─► QuoteData = { out: Nat, slipBP: Nat, valid: Bool }
                    │   │   └─► valid = (slippage <= maxSlippageBP AND out > 0)
                    │   │
                    │   ├─► EVALUATE 6 SCENARIOS:
                    │   │   ├─► Scenario 1: Single KongSwap (100%)
                    │   │   ├─► Scenario 2: Single ICPSwap (100%)
                    │   │   ├─► Scenario 3: Split 80% Kong / 20% ICP
                    │   │   ├─► Scenario 4: Split 60% Kong / 40% ICP
                    │   │   ├─► Scenario 5: Split 40% Kong / 60% ICP
                    │   │   └─► Scenario 6: Split 20% Kong / 80% ICP
                    │   │
                    │   ├─► SELECT: Scenario with maximum totalOut
                    │   │
                    │   ├─► SLIPPAGE INTERPOLATION (if best & 2nd-best are adjacent splits):
                    │   │   ├─► avgKongSlip = (best.kongSlip + second.kongSlip) / 2
                    │   │   ├─► avgIcpSlip = (best.icpSlip + second.icpSlip) / 2
                    │   │   ├─► kongRatio = avgIcpSlip / (avgKongSlip + avgIcpSlip)
                    │   │   └─► interpolatedKongPct = lowKongPct + (kongRatio * range)
                    │   │
                    │   └─► RETURN: #Single OR #Split execution plan
                    │
                    ├─► CHECK: bestExecution == #err?
                    │   └─► YES → CONTINUE to next attempt
                    │
                    └─► EXECUTE TRADE:
                        │
                        ├─► IF #Single execution:
                        │   │
                        │   ├─► SLIPPAGE ADJUSTMENT (if isExactTargeting):
                        │   │   └─► finalSize = (tradeSize * 10000) / (10000 + slippageBP)
                        │   │
                        │   ├─► CALCULATE minAmountOut:
                        │   │   ├─► adjustedExpectedOut = (expectedOut * finalSize) / tradeSize
                        │   │   ├─► idealOut = (adjustedExpectedOut * 10000) / (10000 - slippageBP)
                        │   │   └─► minAmountOut = (idealOut * (10000 - maxSlippageBP)) / 10000
                        │   │
                        │   └─► executeTrade() → TradeRecord or error
                        │
                        └─► IF #Split execution:
                            │
                            ├─► APPLY slippage adjustment to BOTH legs
                            │   ├─► kongFinalAmount = (kongAmount * 10000) / (10000 + kongSlipBP)
                            │   └─► icpFinalAmount = (icpAmount * 10000) / (10000 + icpSlipBP)
                            │
                            └─► executeSplitTrade() [Line 5974]
                                ├─► Launch KongSwap trade (async)
                                ├─► Launch ICPSwap trade (async)
                                ├─► AWAIT both results in parallel
                                └─► Process results, update state
```

---

## Key Calculations

### 1. Portfolio Value Calculation
```
For each token:
  valueInICP = (balance * priceInICP) / (10 ^ tokenDecimals)

totalValueICP = sum of valueInICP for all active, unpaused tokens
```

### 2. Allocation Basis Points
```
currentBasisPoints = (valueInICP * 10000) / totalValueICP
targetBasisPoints = (targetAllocation * 10000) / totalTargetBasisPoints
diffBasisPoints = targetBasisPoints - currentBasisPoints

// Positive diff = underweight (need to buy)
// Negative diff = overweight (need to sell)
```

### 3. Trade Size - Random Sizing
```
range = maxTradeValueICP - minTradeValueICP    // e.g., 10M - 2M = 8M e8s
randomOffset = random(0, range)
tradeSize = minTradeValueICP + randomOffset     // Random between 0.02-0.1 ICP
```

### 4. Trade Size - Exact Targeting
```
// When targeting sell token (overweight):
excessValueICP = (|sellTokenDiff| * totalPortfolioValue) / 10000
exactTradeSize = (excessValueICP * 10^decimals) / priceInICP

// When targeting buy token (underweight):
deficitValueICP = (buyTokenDiff * totalPortfolioValue) / 10000
exactTradeSize = (deficitValueICP * 10^decimals) / priceInICP

// Check max constraint:
tradeSizeICP = (exactTradeSize * priceInICP) / 10^decimals
if tradeSizeICP > maxTradeValueICP → fall back to random sizing
```

### 5. Should Use Exact Targeting Decision
```
halfMaxTradeBP = (maxTradeValueICP * 10000 / 2) / totalPortfolioValueICP

sellCloseToTarget = |sellTokenDiff| <= halfMaxTradeBP
buyCloseToTarget = |buyTokenDiff| <= halfMaxTradeBP

useExactTargeting = sellCloseToTarget OR buyCloseToTarget
```

### 6. Slippage Adjustment (for exact targeting)
```
// Reduce trade size to compensate for price impact
finalTradeSize = (tradeSize * 10000) / (10000 + slippageBP)

// Adjust expected output proportionally
adjustedExpectedOut = (expectedOut * finalTradeSize) / tradeSize

// Calculate ideal output (reverse slippage)
idealOut = (adjustedExpectedOut * 10000) / (10000 - slippageBP)

// Apply our slippage tolerance
minAmountOut = (idealOut * (10000 - maxSlippageBP)) / 10000
```

### 7. Exchange Selection - Scenario Comparison
```
Scenarios evaluated:
1. Single Kong: totalOut = kong[100%].out
2. Single ICP: totalOut = icp[100%].out
3-6. Splits: totalOut = kong[X%].out + icp[Y%].out (where X+Y=100%)

Best scenario = max(totalOut) where all quotes are valid
```

### 8. Slippage Interpolation Between Adjacent Splits
```
// Only when best and 2nd-best are both splits differing by 20%

avgKongSlip = (best.kongSlipBP + second.kongSlipBP) / 2
avgIcpSlip = (best.icpSlipBP + second.icpSlipBP) / 2
totalSlip = avgKongSlip + avgIcpSlip

// Inverse weighting: higher ICP slippage → more Kong
kongRatio = avgIcpSlip / totalSlip

// Interpolate within the 20% range
interpolatedKongPct = lowKongPct + (kongRatio * (highKongPct - lowKongPct))
interpolatedIcpPct = 10000 - interpolatedKongPct
```

### 9. Weighted Random Selection for Token Pairs
```
// Tokens with larger imbalances have higher probability of selection

totalSellWeight = sum(|diff|) for all overweight tokens
totalBuyWeight = sum(|diff|) for all underweight tokens

sellRandom = random(0, totalSellWeight)
buyRandom = random(0, totalBuyWeight)

// Select token where cumulative weight >= random value
for each token:
  cumulative += |diff|
  if cumulative >= random AND not selected:
    selected = token
```

### 10. Price Update After Trade
```
// For ICP pairs:
if sellToken == ICP:
  actualTokens = amountBought / 10^buyDecimals
  actualICP = amountSold / 10^8
  buyToken.priceInICP = (actualICP / actualTokens) * 10^8

if buyToken == ICP:
  actualTokens = amountSold / 10^sellDecimals
  actualICP = amountBought / 10^8
  sellToken.priceInICP = (actualICP / actualTokens) * 10^8

// For non-ICP pairs (random which to update):
priceRatio = actualTokensSold / actualTokensBought
newPrice = otherToken.priceInICP * priceRatio
```

---

## Configuration Parameters (Default Values)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `rebalanceIntervalNS` | 60B ns (1 min) | Time between trading cycles |
| `maxTradeAttemptsPerInterval` | 2 | Max trade attempts per cycle |
| `minTradeValueICP` | 2,000,000 (0.02 ICP) | Minimum trade size in e8s |
| `maxTradeValueICP` | 10,000,000 (0.1 ICP) | Maximum trade size in e8s |
| `maxSlippageBasisPoints` | 450 (0.45%) | Max allowed slippage |
| `minAllocationDiffBasisPoints` | 15 (0.15%) | Min imbalance to trigger trade |
| `portfolioRebalancePeriodNS` | 604.8T ns (1 week) | Target full rebalance period |

---

## Skip Reasons Tracked

| Skip Reason | When Triggered |
|-------------|----------------|
| `#tokensFiltered` | All tokens too close to target allocation |
| `#insufficientCandidates` | Less than 2 tradeable tokens |
| `#noPairsFound` | selectTradingPair returned null |
| `#noExecutionPath` | findBestExecution failed (no valid quotes) |
| `#pausedTokens` | Tokens paused due to circuit breaker |

---

## Key Files

- **Main Logic**: [treasury.mo](src/treasury/treasury.mo)
- **Types**: [treasury_types.mo](src/treasury/treasury_types.mo)
- **KongSwap Integration**: [kong_swap.mo](src/swap/kong_swap.mo)
- **ICPSwap Integration**: [icp_swap.mo](src/swap/icp_swap.mo)
- **Swap Types**: [swap_types.mo](src/swap/swap_types.mo)

---

# Comparison: Treasury vs test_exchange_selection.py

## Executive Summary

The Python test script (`test_exchange_selection.py`) **partially matches** the treasury logic. It correctly implements the **exchange selection algorithm** but is **missing several critical components** of the full trading cycle.

---

## What MATCHES (Exchange Selection Algorithm)

### 1. Quote Fetching Strategy - MATCHES
**Treasury ([treasury.mo:5599-5645](src/treasury/treasury.mo#L5599)):**
```motoko
// 5 amounts at 20%, 40%, 60%, 80%, 100%
let amounts = [amountIn * 2/10, amountIn * 4/10, amountIn * 6/10, amountIn * 8/10, amountIn]
// Fetch 10 quotes in parallel (5 Kong + 5 ICP)
```

**Python ([test_exchange_selection.py:381-406](test_exchange_selection.py#L381)):**
```python
# Quote amounts: 20%, 40%, 60%, 80%, 100%
amounts = [base_amount * p // 10 for p in [2, 4, 6, 8, 10]]
# Fetch ALL quotes in parallel (5 Kong + up to 5 ICPSwap = 10 requests)
```
**IDENTICAL**

### 2. Slippage Extraction - MATCHES
**Treasury ([treasury.mo:5658-5679](src/treasury/treasury.mo#L5658)):**
```motoko
func extractKong(result): QuoteData {
  let slipBP = Int.abs(Float.toInt(r.slippage * 100.0));
  let valid = r.slippage <= maxSlippagePct and r.receive_amount > 0;
}
```

**Python ([test_exchange_selection.py:119-124](test_exchange_selection.py#L119)):**
```python
slippage_bp = int(slippage_pct * 100)
valid = slippage_bp <= MAX_SLIPPAGE_BP and receive_amount > 0
```
**IDENTICAL**

### 3. Scenario Evaluation - MATCHES
**Treasury ([treasury.mo:5728-5778](src/treasury/treasury.mo#L5728)):**
- Scenario 1: Single Kong (100%)
- Scenario 2: Single ICP (100%)
- Scenarios 3-6: Splits (80/20, 60/40, 40/60, 20/80)
- Select scenario with max `totalOut`

**Python ([test_exchange_selection.py:262-292](test_exchange_selection.py#L262)):**
```python
# Single Kong (100%)
if kong_quotes[4].valid: scenarios.append(...)
# Single ICPSwap (100%)
if icp_quotes[4].valid: scenarios.append(...)
# Splits: 80/20, 60/40, 40/60, 20/80
for kong_idx in [3, 2, 1, 0]:
    icp_idx = 3 - kong_idx
    ...
scenarios.sort(key=lambda s: s.total_out, reverse=True)
```
**IDENTICAL**

### 4. Slippage Interpolation Logic - MATCHES
**Treasury ([treasury.mo:5796-5864](src/treasury/treasury.mo#L5796)):**
```motoko
let bothAreSplits = best.kongPct > 0 and best.kongPct < 10000 and second.kongPct > 0 and second.kongPct < 10000;
let diff = |best.kongPct - second.kongPct|;
let areAdjacent = diff == 2000;

if (bothAreSplits and areAdjacent) {
  let avgKongSlip = (best.kongSlipBP + second.kongSlipBP) / 2;
  let avgIcpSlip = (best.icpSlipBP + second.icpSlipBP) / 2;
  let kongRatio = avgIcpSlip / (avgKongSlip + avgIcpSlip);
  let interpolatedKongPct = lowKongPct + (kongRatio * range);
}
```

**Python ([test_exchange_selection.py:299-323](test_exchange_selection.py#L299)):**
```python
both_splits = 0 < best.kong_pct < 10000 and 0 < second.kong_pct < 10000
diff = abs(best.kong_pct - second.kong_pct)

if both_splits and diff == 2000:
    avg_kong_slip = (best.kong_slip_bp + second.kong_slip_bp) / 2
    avg_icp_slip = (best.icp_slip_bp + second.icp_slip_bp) / 2
    kong_ratio = avg_icp_slip / total_slip
    interp_kong = low_kong + int(kong_ratio * (high_kong - low_kong))
```
**IDENTICAL**

### 5. Reduced Amount Estimation (estimateMaxTradeableAmount) - MATCHES
**Treasury ([treasury.mo:5924-5963](src/treasury/treasury.mo#L5924)):**
```motoko
let amountAt20Pct = amountIn / 5;
let targetSlip = maxSlippageBP / 2;
let maxAmount = (amountAt20Pct * targetSlip) / bestSlip;
```

**Python ([test_exchange_selection.py:338-372](test_exchange_selection.py#L338)):**
```python
icp_at_20pct = amount_icp * 0.2
target_slip = MAX_SLIPPAGE_BP / 2.0
max_icp = icp_at_20pct * (target_slip / min_slip)
```
**IDENTICAL**

### 6. ICPSwap Slippage Calculation - MATCHES
**Treasury (via ICPSwap.getQuote):**
```motoko
// spotPrice = (sqrtPriceX96)^2 / 2^192
// effectivePrice = amountIn / amountOut
// slippage = (effectivePrice - spotPrice) / spotPrice * 100
```

**Python ([test_exchange_selection.py:183-202](test_exchange_selection.py#L183)):**
```python
sqrt_squared = sqrt_price_x96 * sqrt_price_x96
spot_price = sqrt_squared / (2 ** 192)
effective_price = amount / amount_out
if zero_for_one:
    normalized_spot = 1.0 / spot_price
else:
    normalized_spot = spot_price
slippage_pct = (effective_price - normalized_spot) / normalized_spot * 100
slippage_bp = int(abs(slippage_pct) * 100)
```
**IDENTICAL**

---

## What is MISSING from Python Script

### 1. Trade Requirements Calculation - COMPLETELY MISSING
**Treasury has ([treasury.mo:4972-5200](src/treasury/treasury.mo#L4972)):**
- Calculate total portfolio value in ICP
- Compute current vs target allocations in basis points
- Filter tokens where `|diff| < minAllocationDiffBasisPoints` (15bp)
- Create weighted list of (token, diff, valueInICP)

**Python:** Does NOT calculate allocations. It tests with fixed ICP amounts.

### 2. Weighted Random Pair Selection - COMPLETELY MISSING
**Treasury has ([treasury.mo:5210-5335](src/treasury/treasury.mo#L5210)):**
```motoko
// Split into toSell (diff < 0) and toBuy (diff > 0)
// Calculate totalSellWeight = sum of |diff| for overweight tokens
// Generate sellRandom = random(0, totalSellWeight)
// Select token where cumulative weight >= random
```

**Python:** Tests ALL pairs sequentially. Does not use weighted random selection.

### 3. Exact Targeting vs Random Sizing - COMPLETELY MISSING

#### What is Exact Targeting?

**Purpose:** When a token is close to its target allocation, the treasury uses a precise trade size to bring it exactly to target. When tokens are far from target, it uses random sizing to obfuscate trading patterns from arbitrage bots.

**Treasury Logic ([treasury.mo:3873-3903](src/treasury/treasury.mo#L3873)):**

```motoko
// Step 1: Decide whether to use exact targeting
let useExactTargeting = shouldUseExactTargeting(sellTokenDiff, buyTokenDiff, totalValueICP);

// Step 2: Calculate trade size based on decision
let (tradeSize, isExactTargeting) = if (useExactTargeting) {
    calculateExactTargetTradeSize(sellToken, buyToken, totalValueICP, sellTokenDiff, buyTokenDiff)
} else {
    // Random sizing: between minTradeValueICP and maxTradeValueICP
    (calculateTradeSizeMinMax() * 10^decimals / priceInICP, false)
};
```

**shouldUseExactTargeting() ([treasury.mo:5352-5368](src/treasury/treasury.mo#L5352)):**
```motoko
// Calculate what 50% of max trade size represents in basis points of portfolio
halfMaxTradeValueBP = (maxTradeValueICP * 10000 / 2) / totalPortfolioValueICP

// Example: If maxTradeValueICP = 0.1 ICP and portfolio = 100 ICP
// halfMaxTradeValueBP = (10_000_000 * 10000 / 2) / 10_000_000_000 = 5 bp

// Use exact targeting if either token is within this threshold
sellTokenCloseToTarget = |sellTokenDiff| <= halfMaxTradeValueBP
buyTokenCloseToTarget = |buyTokenDiff| <= halfMaxTradeValueBP

return sellTokenCloseToTarget OR buyTokenCloseToTarget
```

**calculateExactTargetTradeSize() ([treasury.mo:5379-5442](src/treasury/treasury.mo#L5379)):**
```motoko
// Choose token closer to target (smaller diff)
targetSellToken = |sellTokenDiff| <= |buyTokenDiff|

if targetSellToken:
    // Calculate exact ICP value to sell to reach target
    excessValueICP = (|sellTokenDiff| * totalPortfolioValue) / 10000
    // Convert to token amount
    exactTradeSize = (excessValueICP * 10^decimals) / priceInICP
else:
    // Calculate exact ICP value deficit to buy
    deficitValueICP = (buyTokenDiff * totalPortfolioValue) / 10000
    exactTradeSize = (deficitValueICP * 10^decimals) / priceInICP

// Safety check: if trade exceeds max, fall back to random
if tradeSizeICP > maxTradeValueICP:
    return (calculateTradeSizeMinMax(), false)  // Random sizing
else:
    return (exactTradeSize, true)  // Exact targeting
```

**Python Test:** Uses FIXED trade sizes (1, 5, 10, 20 ICP). Has NO concept of:
- Portfolio value calculation
- Token allocation differences
- Dynamic trade sizing based on proximity to target
- The isExactTargeting flag

---

### 4. Slippage Adjustment for Exact Targeting - COMPLETELY MISSING

#### What is Slippage Adjustment?

**Purpose:** When using exact targeting, slippage causes the trade to "overshoot" the target. To compensate, the treasury REDUCES the trade size proportionally to the expected slippage BEFORE execution.

**Problem it solves:**
- You want to sell exactly 100 tokens to reach target
- Quote shows 0.45% slippage (45 bp)
- If you sell 100 tokens, you get less ICP than expected
- Result: You end up slightly UNDER target allocation

**Solution:** Reduce trade size to compensate:
- Sell 99.55 tokens instead of 100
- After slippage, you end up at exactly the target

**Treasury Logic ([treasury.mo:3930-3973](src/treasury/treasury.mo#L3930)):**

```motoko
// ONLY applies when isExactTargeting = true AND slippageBP > 0
let finalTradeSize = if (isExactTargeting and slippageBasisPoints > 0) {
    // Formula: adjusted = size * 10000 / (10000 + slippageBP)
    // Example: 100 * 10000 / (10000 + 45) = 99.55
    let denominator = 10000 + slippageBasisPoints;
    let adjusted = (tradeSize * 10000) / denominator;

    // Safety: if adjusted exceeds max, fall back to random
    if adjustedICP > maxTradeValueICP:
        calculateTradeSizeMinMax()  // Fall back to random
    else:
        adjusted  // Use adjusted size
} else {
    tradeSize  // No adjustment (random sizing mode)
};

// Adjust expected output proportionally
let adjustedExpectedOut = if (finalTradeSize < tradeSize) {
    (execution.expectedOut * finalTradeSize) / tradeSize
} else {
    execution.expectedOut
};

// Calculate ideal output (what we'd get with zero slippage)
let idealOut = (adjustedExpectedOut * 10000) / (10000 - slippageBP);

// Calculate minAmountOut with our slippage tolerance
let minAmountOut = (idealOut * (10000 - maxSlippageBP)) / 10000;
```

**Numerical Example:**
```
Given:
  tradeSize = 1000 tokens
  slippageBP = 45 (0.45%)
  expectedOut = 950 tokens
  maxSlippageBP = 450 (0.45%)

Calculations:
  finalTradeSize = (1000 * 10000) / (10000 + 45) = 995.5 tokens
  adjustedExpectedOut = (950 * 995.5) / 1000 = 945.7 tokens
  idealOut = (945.7 * 10000) / (10000 - 45) = 950.0 tokens
  minAmountOut = (950 * (10000 - 450)) / 10000 = 907.3 tokens
```

**Python Test:** Does NOT implement:
- Trade size reduction based on slippage
- The isExactTargeting flag
- idealOut calculation
- minAmountOut calculation with tolerance

---

### 5. Split Trade Execution - COMPLETELY MISSING
**Treasury has ([treasury.mo:5974-6107](src/treasury/treasury.mo#L5974)):**
- Launch KongSwap and ICPSwap trades in parallel
- Await both results
- Handle pending transactions for retry
- Update trade records

**Python:** Only calculates expected output. Does NOT execute trades.

### 6. Price Updates After Trade - COMPLETELY MISSING

#### What are Price Updates?

**Purpose:** After every successful trade, the treasury updates its internal token prices based on the ACTUAL trade execution. This creates a feedback loop:
1. Trade executes at market price
2. Treasury updates its price model
3. Future allocation calculations use the new price
4. Next trading decisions are based on real market data

This is critical because:
- External price feeds can be stale or manipulated
- Actual trade execution reflects true market liquidity
- Prevents repeated bad trades based on incorrect price assumptions

**Treasury Logic ([treasury.mo:4007-4039](src/treasury/treasury.mo#L4007)):**

```motoko
// CASE 1: Trade involves ICP (most common)
if (sellToken == ICPprincipal or buyToken == ICPprincipal) {

    if (sellToken == ICPprincipal) {
        // Sold ICP, bought another token
        // Calculate how much ICP we paid per token received
        actualTokens = amountBought / 10^buyDecimals  // e.g., 1000 TACO
        actualICP = amountSold / 10^8                  // e.g., 5.5 ICP

        // New price = ICP paid / tokens received
        newPriceInICP = (actualICP / actualTokens) * 10^8  // e.g., 0.0055 ICP per TACO
        newPriceInUSD = icpPriceInUSD * actualICP / actualTokens

        // Update the BOUGHT token's price
        updateTokenPriceWithHistory(buyToken, newPriceInICP, newPriceInUSD)

    } else {
        // Sold another token, received ICP
        // Calculate how much ICP we got per token sold
        actualTokens = amountSold / 10^sellDecimals   // e.g., 1000 TACO
        actualICP = amountBought / 10^8               // e.g., 5.3 ICP

        // New price = ICP received / tokens sold
        newPriceInICP = (actualICP / actualTokens) * 10^8  // e.g., 0.0053 ICP per TACO
        newPriceInUSD = icpPriceInUSD * actualICP / actualTokens

        // Update the SOLD token's price
        updateTokenPriceWithHistory(sellToken, newPriceInICP, newPriceInUSD)
    }
}

// CASE 2: Non-ICP pair (e.g., TACO/CHAT)
else {
    // Randomly choose which token's price to maintain as reference
    maintainFirst = random(0, 1) == 0

    if maintainFirst:
        // Keep sell token price, update buy token price
        priceRatio = actualTokensSold / actualTokensBought
        newPriceInICP = sellToken.priceInICP * priceRatio
        newPriceInUSD = sellToken.priceInUSD * priceRatio
        updateTokenPriceWithHistory(buyToken, newPriceInICP, newPriceInUSD)
    else:
        // Keep buy token price, update sell token price
        priceRatio = actualTokensBought / actualTokensSold
        newPriceInICP = buyToken.priceInICP * priceRatio
        newPriceInUSD = buyToken.priceInUSD * priceRatio
        updateTokenPriceWithHistory(sellToken, newPriceInICP, newPriceInUSD)
}
```

**Numerical Example (ICP pair):**
```
Trade: Sell 1000 TACO -> Receive 5.3 ICP

Before trade:
  TACO.priceInICP = 0.0050 ICP (stale/incorrect)

After trade:
  actualTokens = 1000 TACO
  actualICP = 5.3 ICP
  newPriceInICP = (5.3 / 1000) * 10^8 = 530_000 e8s = 0.0053 ICP

  TACO.priceInICP = 0.0053 ICP (updated from real trade)
```

**Numerical Example (non-ICP pair):**
```
Trade: Sell 500 TACO -> Receive 200 CHAT

Before trade:
  TACO.priceInICP = 0.0053 ICP
  CHAT.priceInICP = 0.0100 ICP

After trade (if maintaining TACO price):
  priceRatio = 500 / 200 = 2.5
  newChatPriceInICP = 0.0053 * 2.5 = 0.01325 ICP

  CHAT.priceInICP = 0.01325 ICP (updated)
```

**Why random selection for non-ICP pairs?**
- Prevents systematic bias in price updates
- Both tokens' prices should eventually converge to market reality
- Avoids always trusting one token's price over another

**Python Test:** Does NOT implement:
- Price tracking or storage
- Post-trade price calculation
- Price history updates
- The `updateTokenPriceWithHistory()` function
- Any price-dependent calculations for subsequent tests

### 7. Circuit Breaker & Token Pause Logic - COMPLETELY MISSING
**Treasury has:**
- `checkPortfolioCircuitBreakerConditions()`
- `isTokenPausedFromTrading()`
- Price failsafe system
- Portfolio snapshot system

**Python:** Does NOT implement any safety mechanisms.

### 8. Retry Failed Transactions - COMPLETELY MISSING
**Treasury has ([treasury.mo:3735-3768](src/treasury/treasury.mo#L3735)):**
```motoko
private func retryFailedKongswapTransactions() : async* () {
    for ((txId, record) in Map.entries(pendingTxs)) {
        await KongSwap.retryTransaction(txId, pendingTxs, failedTxs, maxAttempts);
    }
}
```

**Python:** Does NOT handle transaction retries.

---

## Key Configuration Differences

| Parameter | Treasury | Python Test |
|-----------|----------|-------------|
| `maxSlippageBP` | 450 (0.45%) | 100 (1%) |
| `minTradeValueICP` | 2,000,000 e8s (0.02 ICP) | 1 ICP |
| `maxTradeValueICP` | 10,000,000 e8s (0.1 ICP) | 20 ICP |
| `minAllocationDiffBP` | 15 (0.15%) | N/A |

**NOTE:** Python uses `MAX_SLIPPAGE_BP = 100` but comments say "4.5% - matches treasury". This is WRONG:
- 100 basis points = 1%
- 450 basis points = 4.5%

---

## Summary of Differences

| Component | Treasury | Python Test |
|-----------|----------|-------------|
| Quote fetching (5 per exchange) | YES | YES |
| Scenario evaluation (6 scenarios) | YES | YES |
| Best scenario selection | YES | YES |
| Slippage interpolation | YES | YES |
| Reduced amount estimation | YES | YES |
| ICP fallback route | YES | YES |
| ICPSwap slippage calculation | YES | YES |
| **Allocation calculation** | YES | **NO** |
| **Weighted random pair selection** | YES | **NO** |
| **Exact targeting logic** | YES | **NO** |
| **Slippage adjustment for exact targeting** | YES | **NO** |
| **Trade execution** | YES | **NO** |
| **Price updates** | YES | **NO** |
| **Circuit breakers** | YES | **NO** |
| **Transaction retry** | YES | **NO** |

---

## Conclusion

The Python test script accurately replicates the **exchange selection algorithm** (`findBestExecution`), which is ~30% of the trading cycle logic. It is useful for testing:
- Quote fetching and parsing
- Scenario comparison
- Slippage interpolation
- ICP fallback routing

However, it is **NOT a complete replica** of the treasury trading cycle. It is missing:
- Portfolio allocation logic (what/when to trade)
- Trade sizing decisions (exact vs random)
- Slippage adjustments
- Trade execution
- State management (prices, balances, pending txs)
