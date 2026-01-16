# Treasury Trading Logic - Overview

## What Does It Do?

Every 60 seconds, the treasury checks if any tokens are out of balance and trades to rebalance them.

---

## The Trading Decision Tree

```
executeTradingCycle() - EVERY 60 SECONDS
│
├─► CHECK: Is trading enabled? (status == #Trading)
│   └─► NO → STOP (system is idle or paused)
│
├─► checkPortfolioCircuitBreakerConditions()
│   └─► May pause tokens if price dropped >30% or portfolio value dropped significantly
│
├─► updateBalances()
│   └─► Sync actual token balances from all ledgers
│
├─► retryFailedKongswapTransactions()
│   └─► Retry any pending Kong swaps that failed previously
│
└─► LOOP: Up to 2 trade attempts per cycle
    │
    ├─► calculateTradeRequirements()
    │   │
    │   ├─► PASS 1: Get all active tokens (not paused, has balance or is active)
    │   │
    │   ├─► PASS 2: Calculate portfolio value
    │   │   └─► For each token: valueInICP = (balance × priceInICP) / 10^decimals
    │   │   └─► Sum up totalPortfolioValueICP
    │   │
    │   ├─► PASS 3: Calculate allocation differences
    │   │   └─► currentBP = (tokenValueICP × 10000) / totalValueICP
    │   │   └─► targetBP = (targetAllocation × 10000) / totalTargetBP
    │   │   └─► diffBP = targetBP - currentBP
    │   │       • Positive diff → UNDERWEIGHT (need to BUY)
    │   │       • Negative diff → OVERWEIGHT (need to SELL)
    │   │
    │   └─► FILTER: Only include tokens where |diffBP| > 15 basis points (0.15%)
    │
    ├─► CHECK: Any tradeable tokens?
    │   └─► NO → Skip cycle (#tokensFiltered)
    │
    ├─► selectTradingPair()
    │   │
    │   ├─► Split tokens into: toSell (diff < 0) and toBuy (diff > 0)
    │   │
    │   ├─► CHECK: Have at least 1 seller AND 1 buyer?
    │   │   └─► NO → Skip (#insufficientCandidates)
    │   │
    │   └─► WEIGHTED RANDOM SELECTION:
    │       │
    │       │   Tokens further from target = higher probability
    │       │
    │       │   Example weights:
    │       │     CHAT: -200bp (overweight) → weight 200
    │       │     ckBTC: -50bp (overweight) → weight 50
    │       │     Total sell weight: 250
    │       │
    │       │     Random(0, 250) = 180
    │       │     180 > 50 (ckBTC) → skip
    │       │     180 < 250 (CHAT cumulative) → SELECT CHAT
    │       │
    │       └─► Returns: (sellToken, buyToken)
    │
    ├─► DETERMINE TRADE SIZE:
    │   │
    │   ├─► shouldUseExactTargeting()?
    │   │   │
    │   │   │   halfMaxTradeBP = (maxTradeValueICP × 5000) / totalPortfolioValueICP
    │   │   │
    │   │   │   Example: max=0.1 ICP, portfolio=50 ICP
    │   │   │   halfMaxTradeBP = (10M × 5000) / 5000M = 10bp
    │   │   │
    │   │   ├─► IF |sellDiff| ≤ halfMaxTradeBP OR |buyDiff| ≤ halfMaxTradeBP:
    │   │   │   └─► YES → Use EXACT targeting (token is close to target)
    │   │   │
    │   │   └─► ELSE:
    │   │       └─► NO → Use RANDOM sizing (hide patterns from bots)
    │   │
    │   ├─► IF EXACT TARGETING:
    │   │   │
    │   │   │   Pick whichever token is CLOSER to target
    │   │   │
    │   │   ├─► If targeting SELL token:
    │   │   │   └─► excessICP = (|sellDiff| × totalValue) / 10000
    │   │   │   └─► tradeSize = (excessICP × 10^decimals) / priceInICP
    │   │   │
    │   │   └─► If targeting BUY token:
    │   │       └─► deficitICP = (buyDiff × totalValue) / 10000
    │   │       └─► tradeSize = (deficitICP × 10^decimals) / priceInICP
    │   │
    │   └─► IF RANDOM SIZING:
    │       └─► tradeSize = minTradeValueICP + random(0, maxTradeValueICP - minTradeValueICP)
    │           (Random between 0.02 and 0.1 ICP)
    │
    └─► findBestExecution(sellToken, buyToken, tradeSize)
        │
        ├─► FETCH 20 QUOTES IN PARALLEL:
        │   │
        │   │   KongSwap: 10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%, 100%
        │   │   ICPSwap:  10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%, 100%
        │   │
        │   │   NOTE: ICPSwap quotes use (amount - transfer_fee) because
        │   │         the fee is deducted BEFORE the swap in the pool
        │   │
        │   └─► Each quote returns: { amountOut, slippageBP, isValid }
        │       └─► Valid if: slippage ≤ 4.5% AND amountOut > 0 AND not dust
        │
        ├─► EVALUATE 100+ SCENARIOS:
        │   │
        │   │   ┌────────────────────────────────────────────────────┐
        │   │   │ For each KongSwap % (10-100) × ICPSwap % (10-100): │
        │   │   │                                                    │
        │   │   │   totalPct = kongPct + icpPct                      │
        │   │   │                                                    │
        │   │   │   IF totalPct > 100% → SKIP (impossible)           │
        │   │   │   IF either quote invalid → SKIP                   │
        │   │   │                                                    │
        │   │   │   IF totalPct == 100%:                             │
        │   │   │     → Add to FULL scenarios (Single or Split)      │
        │   │   │     → Selection: HIGHEST TOTAL OUTPUT wins         │
        │   │   │                                                    │
        │   │   │   IF totalPct < 100% AND ≥ 40%:                    │
        │   │   │     → Add to PARTIAL scenarios                     │
        │   │   │     → Selection: LOWEST COMBINED SLIPPAGE wins     │
        │   │   └────────────────────────────────────────────────────┘
        │   │
        │   ├─► SINGLE scenarios: Kong 100% alone, ICPSwap 100% alone
        │   ├─► SPLIT scenarios: Kong 60% + ICP 40%, Kong 70% + ICP 30%, etc.
        │   └─► PARTIAL scenarios: Kong 30% + ICP 40% = 70%, etc.
        │
        ├─► SCENARIO SELECTION (Priority Order):
        │   │
        │   │   ┌─────────────────────────────────────────────────────────────┐
        │   │   │  1. SINGLE or SPLIT found (100% coverage)?                  │
        │   │   │     │                                                       │
        │   │   │     ├─► YES → Check for INTERPOLATION                       │
        │   │   │     │         │                                             │
        │   │   │     │         │  If best & 2nd-best are adjacent splits     │
        │   │   │     │         │  (e.g., 60/40 vs 70/30):                    │
        │   │   │     │         │                                             │
        │   │   │     │         │  Blend based on inverse slippage:           │
        │   │   │     │         │  kongRatio = avgIcpSlip / totalSlip         │
        │   │   │     │         │  → Result: Kong 63% / ICP 37%               │
        │   │   │     │         │                                             │
        │   │   │     └─► RETURN: #Single or #Split                           │
        │   │   │                                                             │
        │   │   │  2. No full coverage → Try PARTIAL                          │
        │   │   │     │                                                       │
        │   │   │     ├─► Filter: totalPct ≥ 40% AND valueICP ≥ minTradeValue │
        │   │   │     │   (ICP pairs skip the minTradeValue check)            │
        │   │   │     │                                                       │
        │   │   │     ├─► Sort by combined slippage (lowest first)            │
        │   │   │     │                                                       │
        │   │   │     ├─► Apply INTERPOLATION if adjacent partial exists      │
        │   │   │     │                                                       │
        │   │   │     └─► RETURN: #Partial { totalPct, kongPct, icpPct }      │
        │   │   │                                                             │
        │   │   │  3. No partial ≥ 40% → Try REDUCED                          │
        │   │   │     │                                                       │
        │   │   │     ├─► estimateMaxTradeableAmount()                        │
        │   │   │     │   └─► Find largest amount where slippage is OK        │
        │   │   │     │                                                       │
        │   │   │     └─► RETURN: reduced amount + best exchange              │
        │   │   │                                                             │
        │   │   │  4. Nothing works → FAIL                                    │
        │   │   │     │                                                       │
        │   │   │     └─► RETURN: #err (triggers ICP FALLBACK)                │
        │   │   └─────────────────────────────────────────────────────────────┘
        │
        └─► EXECUTE TRADE:
            │
            ├─► IF #Single:
            │   │
            │   ├─► Apply slippage adjustment (if exact targeting):
            │   │   └─► finalSize = (tradeSize × 10000) / (10000 + slippageBP)
            │   │
            │   ├─► Calculate minAmountOut with tolerance
            │   │
            │   └─► executeTrade() on chosen exchange
            │       │
            │       ├─► SUCCESS → Record trade, update prices
            │       │
            │       └─► FAIL → ICP FALLBACK (if ICPSwap failure)
            │           └─► Try selling to ICP instead
            │
            ├─► IF #Split or #Partial:
            │   │
            │   ├─► Apply slippage adjustment to BOTH legs
            │   │
            │   └─► executeSplitTrade()
            │       ├─► Launch KongSwap trade (async)
            │       ├─► Launch ICPSwap trade (async)
            │       ├─► AWAIT both in parallel
            │       └─► At least one succeeds = overall success
            │
            └─► IF #err (no route):
                │
                └─► ICP FALLBACK:
                    ├─► Try: sellToken → ICP instead
                    ├─► If succeeds: Next cycle handles ICP → buyToken
                    └─► If fails: Record failure, try next cycle
```

---

## Exchange Selection: The 5 Possible Outcomes

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        findBestExecution() Results                          │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐
     │  SINGLE  │      │  SPLIT   │      │ PARTIAL  │      │ REDUCED  │      │   FAIL   │
     │          │      │          │      │          │      │          │      │          │
     │ 100% on  │      │ 100%     │      │ 40-90%   │      │ Smaller  │      │ No valid │
     │ ONE      │      │ across   │      │ across   │      │ trade    │      │ route    │
     │ exchange │      │ BOTH     │      │ BOTH     │      │ possible │      │ found    │
     └──────────┘      └──────────┘      └──────────┘      └──────────┘      └──────────┘
          │                 │                 │                 │                 │
          ▼                 ▼                 ▼                 ▼                 ▼

    ┌─────────┐       ┌─────────┐       ┌─────────┐       ┌─────────┐       ┌─────────┐
    │ Kong OR │       │ Kong 60%│       │ Kong 30%│       │ Trade   │       │ Try ICP │
    │ ICPSwap │       │ ICP  40%│       │ ICP  40%│       │ less    │       │ fallback│
    │  100%   │       │ ════════│       │ ════════│       │ than    │       │ instead │
    │         │       │ = 100%  │       │ = 70%   │       │ planned │       │         │
    └─────────┘       └─────────┘       └─────────┘       └─────────┘       └─────────┘
```

### 1. SINGLE (Best Case - One Exchange Does It All)
```
One exchange can handle 100% of the trade with acceptable slippage (<4.5%)

Example: Selling 0.05 ICP worth of CHAT
  KongSwap quote: 100% → 2.1% slippage ✓
  ICPSwap quote:  100% → 5.2% slippage ✗ (too high)

  Result: Use KongSwap for entire trade
```

### 2. SPLIT (Both Exchanges Together = 100%)
```
Neither exchange alone is good, but COMBINED they can do 100%

Example: Selling 0.08 ICP worth of CHAT
  KongSwap 100%: 6% slippage ✗
  ICPSwap 100%:  5% slippage ✗

  But...
  KongSwap 60%: 2.5% slippage ✓
  ICPSwap 40%:  1.8% slippage ✓

  Result: Split 60/40 across both = 100% traded

Selection: HIGHEST TOTAL OUTPUT wins
```

### 3. PARTIAL (Best Effort - Less Than 100%)
```
Can't do 100% even combined, but can do at least 40%

Example: Selling 0.1 ICP worth of low-liquidity token
  KongSwap max valid: 30% (beyond that, slippage too high)
  ICPSwap max valid:  40% (beyond that, slippage too high)

  Combined: 70% is the best we can do

  Result: Trade 70%, remaining 30% stays for next cycle

Requirements:
  - Must be at least 40% total
  - Must meet minimum trade value (0.02 ICP)
  - ICP pairs get an exception (skip min value check)

Selection: LOWEST SLIPPAGE wins (not highest output)
```

### 4. REDUCED (Fallback - Smaller Trade)
```
Can't do 40% of requested amount, but CAN do a smaller trade

Example: Requested 0.1 ICP trade, but liquidity is thin
  At 0.1 ICP: All quotes exceed 4.5% slippage ✗
  At 0.03 ICP: KongSwap 100% → 2% slippage ✓

  Result: Trade 0.03 ICP instead of 0.1 ICP

How it works:
  - estimateMaxTradeableAmount() calculates what IS possible
  - Returns smaller amount that can succeed
  - Better to trade something than nothing
```

### 5. FAIL → ICP FALLBACK (Safety Net)
```
No valid route found for the original pair

Example: Trying CHAT → SNEED
  KongSwap: No pool exists
  ICPSwap:  Pool exists but 0 liquidity

  Result: #err - no route found

  THEN → ICP FALLBACK activates:
    Try: CHAT → ICP instead
    If succeeds: Next cycle handles ICP → SNEED
```

---

## Interpolation: Fine-Tuning the Split

When two similar options exist (e.g., 60/40 vs 70/30), we **blend** them.

```
Without Interpolation (picking exact boundaries):
  Option A: Kong 60% / ICP 40%  → Kong slip: 2.0%, ICP slip: 1.0%
  Option B: Kong 70% / ICP 30%  → Kong slip: 3.0%, ICP slip: 0.5%

  System picks A (best output at boundary)

With Interpolation (blending based on slippage):
  ICP has lower average slippage → lean more toward Kong

  Calculation:
    avgKongSlip = (2.0 + 3.0) / 2 = 2.5%
    avgIcpSlip  = (1.0 + 0.5) / 2 = 0.75%
    kongRatio = icpSlip / totalSlip = 0.75 / 3.25 = 23%

  Result: Kong 63% / ICP 37% (interpolated between 60% and 70%)
```

**Why?** The 10% quote steps are coarse. Interpolation finds the sweet spot between them.

---

## ICP Fallback: Two Safety Nets

### Safety Net 1: Quote-Level Fallback
**When:** `findBestExecution()` returns no valid route

```
CHAT → SNEED fails (no liquidity)
       │
       └─► Try CHAT → ICP instead
           │
           ├─► Success → Trade executes, next cycle handles ICP → SNEED
           └─► Fail → Record failure, try again next cycle
```

### Safety Net 2: Execution-Level Fallback (ICPSwap Only)
**When:** Trade was planned but execution fails (e.g., "slippage over range")

```
CHAT → SNEED execution fails on ICPSwap
       │
       ├─► Tokens recovered from ICPSwap pool
       │
       └─► Try CHAT → ICP instead
           │
           ├─► Success → Trade executes
           └─► Fail → Record original failure

NOTE: Only works for ICPSwap failures!
      KongSwap failures use retry mechanism (tokens held by Kong)
```

---

## Slippage: Why Prices Get Worse

```
SMALL TRADE (0.01 ICP):          LARGE TRADE (1 ICP):
┌──────────────────────┐         ┌──────────────────────┐
│ ████                 │         │ ████████████████████ │
│ You get good price   │         │ You move the market  │
│ ~0% slippage         │         │ ~3% slippage         │
└──────────────────────┘         └──────────────────────┘

The more you trade, the more you "eat into" the order book,
getting progressively worse prices.

Treasury rejects trades with >4.5% slippage.
```

---

## Exact Targeting vs Random Sizing

```
FAR FROM TARGET (e.g., 5% off):     CLOSE TO TARGET (e.g., 0.5% off):
┌─────────────────────────┐         ┌─────────────────────────┐
│ Use RANDOM sizing       │         │ Use EXACT sizing        │
│                         │         │                         │
│ Why? Hides our pattern  │         │ Why? Precision matters  │
│ from arbitrage bots     │         │ when almost balanced    │
│                         │         │                         │
│ Trade: 0.02-0.1 ICP     │         │ Trade: exactly 0.037 ICP│
│ (random in range)       │         │ (calculated precisely)  │
└─────────────────────────┘         └─────────────────────────┘
```

---

## Quick Reference

| Term | Meaning |
|------|---------|
| **Single** | 100% on one exchange (KongSwap OR ICPSwap) |
| **Split** | 100% divided between both exchanges |
| **Partial** | 40-90% traded (liquidity limits prevent 100%) |
| **Reduced** | Smaller trade than planned (fallback amount) |
| **Interpolation** | Blend two split options for optimal ratio |
| **Slippage** | Price worsens as trade size increases |
| **ICP Fallback** | If original trade fails, try selling to ICP instead |
| **Exact Targeting** | Precise trade size when close to target allocation |
| **Random Sizing** | Random trade size to hide patterns from bots |

---

## The Numbers

| Parameter | Value | Meaning |
|-----------|-------|---------|
| Trade interval | 60 sec | Check for trades every minute |
| Max slippage | 4.5% | Reject trades above this |
| Min trade | 0.02 ICP | Smallest allowed trade |
| Max trade | 0.1 ICP | Largest single trade |
| Min imbalance | 0.15% | Ignore smaller differences |
| Min partial | 40% | Partial must cover at least this much |
