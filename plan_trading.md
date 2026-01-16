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
                    ├─► findBestExecution() [Line 5900]
                    │   │
                    │   ├─► GET 20 QUOTES IN PARALLEL (10%, 20%, 30%, ..., 100%)
                    │   │   ├─► KongSwap: 10 quotes (indices 0-9)
                    │   │   └─► ICPSwap: 10 quotes (indices 0-9)
                    │   │
                    │   ├─► CONSTANTS:
                    │   │   ├─► NUM_QUOTES = 10
                    │   │   ├─► STEP_BP = 1000 (10% per step)
                    │   │   └─► MIN_PARTIAL_TOTAL_BP = 4000 (40% minimum)
                    │   │
                    │   ├─► EXTRACT quote data:
                    │   │   ├─► QuoteData = { out: Nat, slipBP: Nat, valid: Bool }
                    │   │   ├─► valid = (slippage <= maxSlippageBP AND out > 0 AND NOT dustOutput)
                    │   │   └─► dustOutput = (output < 1% of expected at spot price)
                    │   │
                    │   ├─► EVALUATE ALL SCENARIOS (10x10 = 100 combinations):
                    │   │   │
                    │   │   ├─► STEP 1: Single exchanges
                    │   │   │   ├─► SINGLE_KONG (100%) - kong[9]
                    │   │   │   └─► SINGLE_ICP (100%) - icp[9]
                    │   │   │
                    │   │   ├─► STEP 2: Nested loop for all combos
                    │   │   │   for kongIdx in 0..9:
                    │   │   │     for icpIdx in 0..9:
                    │   │   │       kongPct = (kongIdx + 1) * STEP_BP  // 1000, 2000, ..., 10000
                    │   │   │       icpPct = (icpIdx + 1) * STEP_BP
                    │   │   │       totalPct = kongPct + icpPct
                    │   │   │
                    │   │   │       SKIP if totalPct > 10000  // Over 100%
                    │   │   │       SKIP if kongPct == 10000 or icpPct == 10000  // Singles handled
                    │   │   │
                    │   │   │       if kong[kongIdx].valid AND icp[icpIdx].valid:
                    │   │   │         if totalPct == 10000:
                    │   │   │           updateBest(scenario)  // FULL SPLIT -> MAX output
                    │   │   │         else:
                    │   │   │           partialScenarios.add(scenario)  // PARTIAL -> MIN slippage
                    │   │   │
                    │   │   └─► RESULT: bestScenario, secondBestScenario, partialScenarios[]
                    │   │
                    │   ├─► SCENARIO SELECTION:
                    │   │   │
                    │   │   ├─► IF bestScenario found (single or full split):
                    │   │   │   ├─► INTERPOLATION (if best & second are adjacent splits):
                    │   │   │   │   ├─► diff = |best.kongPct - second.kongPct|
                    │   │   │   │   ├─► areAdjacent = (diff == STEP_BP)  // 1000bp = 10%
                    │   │   │   │   ├─► avgKongSlip = (best.kongSlipBP + second.kongSlipBP) / 2
                    │   │   │   │   ├─► avgIcpSlip = (best.icpSlipBP + second.icpSlipBP) / 2
                    │   │   │   │   ├─► kongRatioBP = (avgIcpSlipBP * 10000) / totalSlipBP  // INTEGER
                    │   │   │   │   └─► interpolatedKongPct = lowKong + (kongRatioBP * range) / 10000
                    │   │   │   └─► RETURN: #Single OR #Split
                    │   │   │
                    │   │   └─► ELSE (no full scenario - TRY PARTIALS):
                    │   │       │
                    │   │       ├─► STEP A: Filter partials by BOTH conditions:
                    │   │       │   ├─► meetsTotalPct = totalPct >= MIN_PARTIAL_TOTAL_BP (40%)
                    │   │       │   └─► meetsValueICP = partialValueICP >= minTradeValueICP
                    │   │       │   └─► validPartials = partials meeting BOTH
                    │   │       │
                    │   │       ├─► STEP B: If validPartials non-empty:
                    │   │       │   ├─► Sort by combinedSlip (MIN slippage wins)
                    │   │       │   └─► bestPartial = validPartials[0]
                    │   │       │
                    │   │       ├─► STEP C (ICP EXCEPTION): Elif ICP involved AND partials exist:
                    │   │       │   ├─► icpValid = partials where totalPct >= 40% (skip minTradeValueICP)
                    │   │       │   ├─► Sort by combinedSlip
                    │   │       │   └─► bestPartial = icpValid[0] or null
                    │   │       │
                    │   │       ├─► STEP D: If bestPartial found:
                    │   │       │   ├─► INTERPOLATION (if adjacent partial exists):
                    │   │       │   │   ├─► BOTH kongDiff AND icpDiff must == STEP_BP
                    │   │       │   │   ├─► ICP uses INVERSE: highIcp - (ratio * range)
                    │   │       │   │   └─► Integer math scaled by 10000
                    │   │       │   └─► RETURN: #Partial({ totalPercentBP, kongswap, icpswap })
                    │   │       │
                    │   │       └─► ELSE: RETURN #err with quotes for REDUCED fallback
                    │   │
                    │   └─► RETURN: #Single OR #Split OR #Partial OR #err
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
                        ├─► IF #Split execution:
                        │   │
                        │   ├─► APPLY slippage adjustment to BOTH legs
                        │   │   ├─► kongFinalAmount = (kongAmount * 10000) / (10000 + kongSlipBP)
                        │   │   └─► icpFinalAmount = (icpAmount * 10000) / (10000 + icpSlipBP)
                        │   │
                        │   └─► executeSplitTrade() [Line 6658]
                        │       ├─► Launch KongSwap trade (async)
                        │       ├─► Launch ICPSwap trade (async)
                        │       ├─► AWAIT both results in parallel
                        │       └─► Process results, update state
                        │
                        └─► IF #Partial execution:
                            │
                            ├─► LOG: "Partial split - Kong=X% ICP=Y% Total=Z%"
                            │
                            ├─► APPLY slippage adjustment to BOTH legs (same as #Split)
                            │   ├─► kongFinalAmount = (partial.kongswap.amount * 10000) / (10000 + kongSlipBP)
                            │   └─► icpFinalAmount = (partial.icpswap.amount * 10000) / (10000 + icpSlipBP)
                            │
                            ├─► executeSplitTrade() - SAME as #Split
                            │   ├─► Launch KongSwap trade (async)
                            │   ├─► Launch ICPSwap trade (async)
                            │   ├─► AWAIT both results in parallel
                            │   └─► Track: kongSuccess, icpSuccess
                            │
                            └─► RESULT: At least one leg succeeded = overall success
                                (partial.totalPercentBP < 10000 means less than 100% traded)
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

### 7. Exchange Selection - Scenario Comparison (10 Quotes)
```
CONSTANTS:
  NUM_QUOTES = 10
  STEP_BP = 1000  (10% per step)
  MIN_PARTIAL_TOTAL_BP = 4000  (40% minimum for partials)

Scenarios evaluated (10x10 = 100 combinations):
  1. Single Kong: totalOut = kong[9].out (index 9 = 100%)
  2. Single ICP: totalOut = icp[9].out
  3+. All combos: kong[i].out + icp[j].out where (i+1)*10% + (j+1)*10% <= 100%

Selection criteria:
  - FULL SPLITS (totalPct == 10000): Best = max(totalOut)
  - PARTIALS (totalPct < 10000): Best = min(combinedSlippage)

Partial filtering:
  - Must meet totalPct >= 40% (MIN_PARTIAL_TOTAL_BP)
  - Must meet partialValueICP >= minTradeValueICP
  - ICP EXCEPTION: Skip minTradeValueICP for ICP pairs (but keep 40% min)
```

### 8. Slippage Interpolation (Integer Math)
```
// For FULL SPLITS: Only when best and 2nd-best are adjacent splits differing by 10%
// For PARTIALS: BOTH kongDiff AND icpDiff must == STEP_BP

// Check adjacency
diff = |best.kongPct - second.kongPct|
areAdjacent = (diff == STEP_BP)  // 1000bp = 10%

// INTEGER MATH (scaled by 10000 for precision)
avgKongSlipBP = (best.kongSlipBP + second.kongSlipBP) / 2
avgIcpSlipBP = (best.icpSlipBP + second.icpSlipBP) / 2
totalSlipBP = avgKongSlipBP + avgIcpSlipBP

// Inverse weighting: higher ICP slippage → more Kong
kongRatioBP = (avgIcpSlipBP * 10000) / totalSlipBP  // Scaled integer

// Kong interpolation
kongRange = highKongPct - lowKongPct
interpolatedKongPct = lowKongPct + (kongRatioBP * kongRange) / 10000

// ICP interpolation (INVERSE for partials)
// Full splits: interpolatedIcpPct = 10000 - interpolatedKongPct
// Partials: interpolatedIcpPct = highIcpPct - (kongRatioBP * icpRange) / 10000
```

### 8b. Partial Execution Plan Type
```motoko
type ExecutionPlan = {
  #Single : { exchange : ExchangeType; expectedOut : Nat; slippageBP : Nat };
  #Split : {
    kongswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
    icpswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
  };
  #Partial : {
    kongswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
    icpswap : { amount : Nat; expectedOut : Nat; slippageBP : Nat; percentBP : Nat };
    totalPercentBP : Nat;  // Sum < 10000 (e.g., 6000 = 60%)
  };
};
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

**UPDATED**: Treasury now uses **10 quotes** (10%, 20%, ..., 100%) instead of 5 quotes, and supports **partial splits** (e.g., 60% total = 40% Kong + 20% ICP). The Python test script needs updating to match.

---

## CURRENT TREASURY IMPLEMENTATION (Updated)

### 1. Quote Fetching - NOW 10 QUOTES
**Treasury ([treasury.mo:5944-6024](src/treasury/treasury.mo#L5944)):**
```motoko
// Calculate amounts for 10% increments (indices 0-9 = 10%, 20%, ..., 100%)
let amounts : [Nat] = [
  amountIn * 1 / 10,    // 10%  - idx 0
  amountIn * 2 / 10,    // 20%  - idx 1
  amountIn * 3 / 10,    // 30%  - idx 2
  amountIn * 4 / 10,    // 40%  - idx 3
  amountIn * 5 / 10,    // 50%  - idx 4
  amountIn * 6 / 10,    // 60%  - idx 5
  amountIn * 7 / 10,    // 70%  - idx 6
  amountIn * 8 / 10,    // 80%  - idx 7
  amountIn * 9 / 10,    // 90%  - idx 8
  amountIn,             // 100% - idx 9
];
// Fetch 20 quotes in parallel (10 Kong + 10 ICP)
```

### 2. Constants
```motoko
let NUM_QUOTES : Nat = 10;
let STEP_BP : Nat = 1000;  // 10% per step (was 2000 for 5 quotes)
let MIN_PARTIAL_TOTAL_BP : Nat = 4000;  // 40% minimum for partials
```

### 3. Slippage Extraction - WITH DUST VALIDATION
```motoko
func extractKong(result, amountIn): QuoteData {
  let slipBP = Int.abs(Float.toInt(r.slippage * 100.0));
  let isDust = isDustOutput(amountIn, r.receive_amount);  // < 1% of expected
  let valid = r.slippage <= maxSlippagePct and r.receive_amount > 0 and not isDust;
}
```

### 4. Scenario Evaluation - NOW 10x10 = 100 COMBINATIONS
**Treasury ([treasury.mo:6162-6203](src/treasury/treasury.mo#L6162)):**
```motoko
// Single exchanges
if (kong[9].valid) { updateBest(SINGLE_KONG) }  // 100% at index 9
if (icp[9].valid) { updateBest(SINGLE_ICP) }

// All 10x10 combinations
for (kongIdx in 0..9):
  for (icpIdx in 0..9):
    kongPct = (kongIdx + 1) * STEP_BP  // 1000, 2000, ..., 10000
    icpPct = (icpIdx + 1) * STEP_BP
    totalPct = kongPct + icpPct

    if totalPct > 10000: continue  // Over 100%
    if kongPct == 10000 or icpPct == 10000: continue  // Singles handled

    if kong[kongIdx].valid and icp[icpIdx].valid:
      if totalPct == 10000:
        updateBest(scenario)  // FULL SPLIT -> MAX output selection
      else:
        partialScenarios.add(scenario)  // PARTIAL -> MIN slippage selection
```

### 5. Partial Selection Logic - NEW
**Treasury ([treasury.mo:6253-6453](src/treasury/treasury.mo#L6253)):**
```motoko
// STEP A: Filter partials by BOTH conditions
validPartials = partials.filter(p =>
  totalPct(p) >= MIN_PARTIAL_TOTAL_BP AND  // >= 40%
  partialValueICP(p) >= minTradeValueICP
)

// STEP B: If validPartials non-empty
if validPartials.size > 0:
  sort by combinedSlip (ascending - MIN wins)
  bestPartial = validPartials[0]

// STEP C: ICP EXCEPTION - elif ICP involved AND partials exist
elif icpInvolved and partialScenarios.size > 0:
  icpValid = partials.filter(p => totalPct(p) >= 40%)  // Skip minTradeValueICP
  if icpValid.size > 0:
    sort by combinedSlip
    bestPartial = icpValid[0]

// STEP D: If bestPartial found, check for interpolation
// For partials: BOTH kongDiff AND icpDiff must == STEP_BP
```

### 6. Slippage Interpolation - NOW INTEGER MATH
**Treasury ([treasury.mo:6468-6511](src/treasury/treasury.mo#L6468)):**
```motoko
// Full splits: diff = |best.kongPct - second.kongPct| == STEP_BP (1000)
// Partials: kongDiff == STEP_BP AND icpDiff == STEP_BP

// INTEGER MATH (scaled by 10000)
kongRatioBP = (avgIcpSlipBP * 10000) / totalSlipBP
interpolatedKongPct = lowKong + (kongRatioBP * kongRange) / 10000

// For partials, ICP uses INVERSE direction:
interpolatedIcpPct = highIcp - (kongRatioBP * icpRange) / 10000
```

### 7. estimateMaxTradeableAmount - UPDATED
**Treasury ([treasury.mo:6586-6656](src/treasury/treasury.mo#L6586)):**
```motoko
// Index 0 is now 10% (not 20%), divisor is 10 (not 5)
let kong10Slip = if kongQuotes[0].out > 0 { kongQuotes[0].slipBP } else { 99999 }
let icp10Slip = if icpQuotes[0].out > 0 { icpQuotes[0].slipBP } else { 99999 }

// CHANGE 1: No valid quotes → return 1 ICP worth, prefer Kong
if bestSlip >= 99999:
  return ?{ amount = oneIcpWorth(); exchange = #KongSwap }

// targetSlip = maxSlippageBP * 7 / 10 (70% of max)
let maxAmount = (amountIn * targetSlip) / (bestSlip * 10)  // Divisor is 10 now

// CHANGE 2: calculated == 0 and amountIn > 0 → return 1 ICP worth
// NOTE: Don't check bestSlip <= maxSlippageBP - let verify step filter
if maxAmount == 0 and amountIn > 0:
  return ?{ amount = oneIcpWorth(); exchange = bestExchange }
```

---

## What Python Script STILL MATCHES (Core Logic)

### ✅ Slippage Extraction
Both extract slippage in basis points: `slipBP = slippage_pct * 100`

### ✅ Selection Criteria
- Full splits: MAX total output
- Partials: MIN combined slippage

### ✅ Partial If/Elif/Else Order
Same branch structure for partial selection

### ✅ ICP Exception
Skip minTradeValueICP for ICP pairs, keep 40% minimum

### ✅ Interpolation Logic
Same inverse weighting: `kongRatio = avgIcpSlip / totalSlip`

---

## What Python Script NEEDS UPDATING

| Aspect | Current Python | Should Be |
|--------|---------------|-----------|
| Quote count | 5 (20% steps) | 10 (10% steps) |
| STEP_BP | 2000 | 1000 |
| Index for 100% | [4] | [9] |
| Divisor in estimateMax | 5 | 10 |
| Dust output check | Missing | Add |
| oneIcpWorth() | Returns 1 | Calculate dynamically |
| Integer math | Float | Scaled by 10000 |

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

## Summary of Differences (Updated for 10 Quotes + Partials)

### Exchange Selection Algorithm

| Component | Treasury | Python Test | Notes |
|-----------|----------|-------------|-------|
| Quote count | **10 per exchange** | 5 per exchange | **UPDATE NEEDED** |
| STEP_BP | **1000 (10%)** | 2000 (20%) | **UPDATE NEEDED** |
| Scenario count | **100 combinations** | 6 scenarios | **UPDATE NEEDED** |
| Partial splits | **YES (#Partial type)** | YES | Logic matches |
| MIN_PARTIAL_TOTAL_BP | **4000 (40%)** | 4000 (40%) | ✓ Matches |
| ICP exception | YES | YES | ✓ Matches |
| Interpolation math | **Integer (×10000)** | Float | **UPDATE NEEDED** |
| Dust output check | **YES** | NO | **ADD** |
| estimateMaxTradeableAmount divisor | **10** | 5 | **UPDATE NEEDED** |
| oneIcpWorth() | **Dynamic calculation** | Returns 1 | **UPDATE NEEDED** |

### Full Trading Cycle (NOT in Python Script)

| Component | Treasury | Python Test |
|-----------|----------|-------------|
| **Allocation calculation** | YES | **NO** |
| **Weighted random pair selection** | YES | **NO** |
| **Exact targeting logic** | YES | **NO** |
| **Slippage adjustment for exact targeting** | YES | **NO** |
| **Trade execution (#Partial handler)** | YES | **NO** |
| **Price updates** | YES | **NO** |
| **Circuit breakers** | YES | **NO** |
| **Transaction retry** | YES | **NO** |

---

## Conclusion

The Python test script implements the **exchange selection algorithm** (`findBestExecution`) core logic, but needs updating to match treasury.mo:

**What needs updating in Python:**
1. Change from 5 to 10 quotes per exchange
2. Update STEP_BP from 2000 to 1000
3. Change 100% index from [4] to [9]
4. Update divisor in estimateMaxTradeableAmount from 5 to 10
5. Add dust output validation
6. Convert Float interpolation to Integer (scaled by 10000)
7. Update oneIcpWorth() to calculate dynamically

**What already matches:**
- Partial selection if/elif/else order
- ICP exception (skip minTradeValueICP, keep 40% min)
- Combined slippage selection (MIN for partials)
- Interpolation inverse weighting logic
- ICPSwap slippage calculation

The Python script is **NOT a complete replica** of the trading cycle - it's only for testing exchange selection. Missing:
- Portfolio allocation logic
- Trade sizing decisions
- Slippage adjustments
- Trade execution
- State management
