# Top 100 Performers Leaderboard Implementation Plan

## Executive Summary

This plan implements **8 sortable performance leaderboards** (top 100 each) based on pure allocation skill:

### What the User Wants:
- **Multiple timeframes:** 1 week, 1 month, 1 year, all-time
- **Multiple price types:** USD value, ICP value
- **Pure performance:** No voting power multipliers - just allocation skill
- **One user, best neuron:** Deduplicated by Principal
- **Frontend sortable:** Users can switch between 8 pre-computed views

### Key Features:
- ✅ 8 leaderboards: (1W, 1M, 1Y, All) × (USD, ICP)
- ✅ Auto-updates every reward period (7 days)
- ✅ Cycle-efficient: Runs once, stores results, queries are O(1)
- ✅ Follow integration: Shows follower count, follow capability
- ✅ Frontend-ready: Simple query API with all metadata

### Critical Implementation Requirement:
**Must store BOTH USD and ICP performance scores** for each distribution.
Current system only stores one price type. Two approaches:
1. **Recommended:** Modify `NeuronReward` to include both `performanceScoreUSD` and `performanceScoreICP`
2. **Alternative:** Parallel storage map for ICP scores alongside existing USD scores

### Cycle Optimizations Implemented:
1. **No follower metadata in leaderboard entries** - Frontend fetches separately (saves ~10B cycles per computation)
2. **Batch follower query API** - New `getUsersFollowerInfo()` in DAO for efficient batch fetching
3. **Reuse distribution data** - No recalculation, just aggregate existing performance scores
4. **2-call dashboard pattern** - Parallel queries to rewards + DAO, combine client-side
5. **Historical backfill only for top 100** - Retroactive calculation only for current leaders, not all neurons

**Result:** Leaderboard computation ~1B cycles (down from ~10B), dashboard load ~1-2M cycles total

## Current System Analysis

### Performance Calculation (Rewards Canister)

**How Performance Works:**
- **Performance Score** = Final Portfolio Value / Initial Value (always 1.0)
  - Score of 1.5 = 50% gain
  - Score of 0.8 = 20% loss
- Calculated via `calculateNeuronPerformance()` using checkpoint-based portfolio tracking
- Timeline: Tracks allocation changes and applies price movements to calculate value evolution
- **Reward Score** = (performanceScore^power) × (votingPower^power) × penaltyMultiplier
  - Default: both powers = 1.0 (linear)
  - penaltyMultiplier: 0-100 (0 = excluded, 100 = full rewards)

**Reward Distribution Flow:**
1. Runs every 7 days (configurable via `distributionPeriodNS`)
2. Fetches all neurons from DAO
3. Calculates performance for each neuron over the period
4. Computes reward scores and distributes TACO tokens
5. Stores detailed results in `DistributionRecord` (kept for 52 periods = 1 year)

**Key Data Structures:**
```motoko
// Per-neuron accumulated rewards
neuronRewardBalances: Map<Blob, Nat>  // neuronId -> total TACO satoshis

// Distribution history (last 52 distributions)
distributionHistory: Vector<DistributionRecord>

// Each DistributionRecord contains:
- neuronRewards: [NeuronReward] with:
  - performanceScore, votingPower, rewardScore, rewardAmount
  - checkpoints with maker (Principal who made allocation changes)
- timestamp range, total pot, status
```

### DAO Follow System

**Constraints:**
- `MAX_FOLLOWERS = 500` - Max followers per user
- `MAX_FOLLOWED = 3` - Max users a person can follow
- `MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY = 10` - Rate limit
- `MAX_FOLLOW_DEPTH = 1` - No transitive following

**Data Structures:**
```motoko
// Per-user state
UserState {
  neurons: [NeuronVP],              // All neurons owned by this user
  votingPower: Nat,                 // Total VP across neurons
  allocationFollows: [{              // Max 3 - who THIS user follows
    since: Int,
    follow: Principal
  }],
  allocationFollowedBy: [{           // Max 500 - who follows THIS user
    since: Int,
    follow: Principal
  }],
  followUnfollowActions: [Int]      // Rate limit timestamps
}
```

**Neuron-to-User Mapping:**
- Stored as `userStates: Map<Principal, UserState>`
- Multiple users can co-own neurons (SNS hotkey holders)
- No direct neuronId->Principal reverse index currently exists

## Design: Multi-Timeframe Performance Leaderboards

### User Requirements

**Key Insight:** Users want to sort traders by **pure allocation skill** across different timeframes:
- Timeframes: 1 week, 1 month, 1 year, all-time
- Price types: USD value, ICP value
- NO voting power multipliers - just raw performance scores
- One entry per user (best neuron if multiple)
- Frontend should be able to sort/filter dynamically

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    REWARDS CANISTER (New)                        │
├─────────────────────────────────────────────────────────────────┤
│  Post-Distribution Hook (triggered after distribution completes) │
│    ↓                                                             │
│  computeAllLeaderboards()                                        │
│    1. Fetch neuron->principal mapping from DAO                   │
│    2. For each timeframe (1w, 1m, 1y, all):                      │
│       - For each price type (USD, ICP):                          │
│         a. Aggregate performance scores for that period          │
│         b. Group by Principal (one neuron per user)              │
│         c. Sort by performance score                             │
│         d. Take top 100                                          │
│    3. Store all 8 leaderboards in stable storage                 │
│    4. Add follow metadata (follower count, can be followed)      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    FRONTEND QUERIES                              │
├─────────────────────────────────────────────────────────────────┤
│  getLeaderboard(timeframe, priceType, ?limit)                    │
│    → Returns: [LeaderboardEntry] for that view                   │
│                                                                  │
│  getLeaderboardWithFollowStatus(timeframe, priceType, caller)    │
│    → Returns: Leaderboard + follow statuses for viewer           │
│                                                                  │
│  canFollowUser(followerPrincipal, leaderPrincipal)               │
│    → Returns: FollowCapability with detailed error reasons       │
└─────────────────────────────────────────────────────────────────┘

  8 Pre-Computed Leaderboards (updated every distribution period):
  ┌─────────────┬─────────────┐
  │  Timeframe  │ Price Type  │
  ├─────────────┼─────────────┤
  │  1 Week     │    USD      │  Top 100 by latest period performance (USD)
  │  1 Week     │    ICP      │  Top 100 by latest period performance (ICP)
  │  1 Month    │    USD      │  Top 100 by avg last ~4 periods (USD)
  │  1 Month    │    ICP      │  Top 100 by avg last ~4 periods (ICP)
  │  1 Year     │    USD      │  Top 100 by avg last 52 periods (USD)
  │  1 Year     │    ICP      │  Top 100 by avg last 52 periods (ICP)
  │  All-Time   │    USD      │  Top 100 by total accumulated return (USD)
  │  All-Time   │    ICP      │  Top 100 by total accumulated return (ICP)
  └─────────────┴─────────────┘
```

### Data Structures

#### Leaderboard Types
```motoko
public type LeaderboardTimeframe = {
  #OneWeek;      // Latest distribution period (7 days)
  #OneMonth;     // Average of last ~4 distributions (28 days)
  #OneYear;      // Average of last 52 distributions (364 days)
  #AllTime;      // Compound return across all distributions
};

public type LeaderboardPriceType = {
  #USD;
  #ICP;
};

public type LeaderboardEntry = {
  rank: Nat;                          // 1-100
  principal: Principal;                // User principal
  neuronId: Blob;                      // Best performing neuron for this timeframe

  // Performance metrics (pure allocation skill)
  performanceScore: Float;             // Performance score for this timeframe/priceType
  // Examples:
  //   1.0 = break even (0% return)
  //   1.25 = 25% gain
  //   0.80 = 20% loss
  //   2.5 = 150% gain

  // Additional context
  distributionsCount: Nat;             // How many distributions in this timeframe
  lastActivity: Int;                   // Timestamp of last allocation change

  // NOTE: Follow metadata (follower count, can be followed, etc.) is NOT included
  // Frontend should fetch this separately from DAO canister to save cycles
};
```

#### Follow Capability Check
```motoko
public type FollowCapability = {
  canFollow: Bool;
  reason: ?FollowBlockReason;
};

public type FollowBlockReason = {
  #LeaderAtMaxFollowers: { current: Nat; max: Nat };
  #FollowerAtMaxFollowed: { current: Nat; max: Nat };
  #FollowerRateLimited: { actionsToday: Nat; max: Nat };
  #AlreadyFollowing;
  #CannotFollowSelf;
};
```

#### Storage Structure
```motoko
// Stable storage for all 8 leaderboards
stable var leaderboards: {
  oneWeekUSD: [LeaderboardEntry];
  oneWeekICP: [LeaderboardEntry];
  oneMonthUSD: [LeaderboardEntry];
  oneMonthICP: [LeaderboardEntry];
  oneYearUSD: [LeaderboardEntry];
  oneYearICP: [LeaderboardEntry];
  allTimeUSD: [LeaderboardEntry];
  allTimeICP: [LeaderboardEntry];
} = {
  oneWeekUSD = [];
  oneWeekICP = [];
  oneMonthUSD = [];
  oneMonthICP = [];
  oneYearUSD = [];
  oneYearICP = [];
  allTimeUSD = [];
  allTimeICP = [];
};

stable var leaderboardLastUpdate: Int = 0;
stable var leaderboardSize: Nat = 100;               // Top N per leaderboard
stable var leaderboardUpdateEnabled: Bool = true;    // On/off switch
```

### Implementation Strategy

#### Phase 1: Neuron-to-Principal Mapping (DAO Canister)

**Add new query function to DAO:**
```motoko
public query func getAllNeuronOwners() : async [(Blob, [Principal])] {
  // Returns all neurons with their owning principals
  // Iterate userStates map and collect neuronId -> [principals]
}
```

**Why needed:**
- Rewards canister has neuronId but not the owning Principal
- Need to deduplicate by Principal (one neuron per user)
- DAO already has this mapping in `userStates`

#### Phase 2: Multi-Leaderboard Computation (Rewards Canister)

**Trigger Point:**
- Hook into `processNextNeuron()` at completion (line ~1105 when all neurons processed)
- Only run if `leaderboardUpdateEnabled = true`
- Async call to avoid blocking distribution finalization

**Key Insight on Performance Data:**
The rewards canister already computes performance scores for BOTH USD and ICP during each distribution:
- Each distribution can be run with `priceType: #USD` or `#ICP`
- Currently, periodic distributions use `#USD` (see line 809)
- Performance calculation (`calculateNeuronPerformance()`) accepts priceType parameter
- We need BOTH price types to be calculated and stored

**Important:** We need to modify the distribution system to calculate BOTH USD and ICP performance scores per distribution, not just one.

**Algorithm:**
```motoko
private func computeAllLeaderboards<system>() : async* () {
  // 1. Fetch neuron ownership data from DAO
  let neuronOwners = await dao.getAllNeuronOwners();
  let neuronToPrincipals = Map.fromIter<Blob, [Principal]>(neuronOwners.vals(), Blob.hash, Blob.equal);

  // 2. Get all distribution history (up to 52 periods = 1 year)
  let allDistributions = getDistributionHistory();
  let distributionCount = Vector.size(allDistributions);

  // 3. For each combination of timeframe and price type, compute leaderboard
  leaderboards.oneWeekUSD := computeLeaderboardFor(
    neuronToPrincipals,
    #OneWeek,
    #USD,
    allDistributions
  );

  leaderboards.oneWeekICP := computeLeaderboardFor(
    neuronToPrincipals,
    #OneWeek,
    #ICP,
    allDistributions
  );

  leaderboards.oneMonthUSD := computeLeaderboardFor(
    neuronToPrincipals,
    #OneMonth,
    #USD,
    allDistributions
  );

  leaderboards.oneMonthICP := computeLeaderboardFor(
    neuronToPrincipals,
    #OneMonth,
    #ICP,
    allDistributions
  );

  leaderboards.oneYearUSD := computeLeaderboardFor(
    neuronToPrincipals,
    #OneYear,
    #USD,
    allDistributions
  );

  leaderboards.oneYearICP := computeLeaderboardFor(
    neuronToPrincipals,
    #OneYear,
    #ICP,
    allDistributions
  );

  leaderboards.allTimeUSD := computeLeaderboardFor(
    neuronToPrincipals,
    #AllTime,
    #USD,
    allDistributions
  );

  leaderboards.allTimeICP := computeLeaderboardFor(
    neuronToPrincipals,
    #AllTime,
    #ICP,
    allDistributions
  );

  leaderboardLastUpdate := Time.now();
};

private func computeLeaderboardFor(
  neuronToPrincipals: Map<Blob, [Principal]>,
  timeframe: LeaderboardTimeframe,
  priceType: LeaderboardPriceType,
  allDistributions: Vector<DistributionRecord>
) : [LeaderboardEntry] {

  // 1. Determine which distributions to include based on timeframe
  let distributionsToAnalyze = selectDistributions(allDistributions, timeframe);

  // 2. Filter distributions by price type
  // NOTE: This requires storing priceType in DistributionRecord
  let relevantDistributions = Array.filter(
    distributionsToAnalyze,
    func (d: DistributionRecord) : Bool {
      d.priceType == priceType
    }
  );

  // 3. Aggregate per-neuron performance scores
  type NeuronPerformance = {
    neuronId: Blob;
    performanceScores: [Float];  // One per distribution
    distributionCount: Nat;
    lastActivity: Int;
  };

  let neuronPerformances = Map.new<Blob, NeuronPerformance>();

  for (distribution in relevantDistributions.vals()) {
    for (neuronReward in distribution.neuronRewards.vals()) {
      let existingPerf = Map.get(neuronPerformances, Blob.hash, Blob.equal, neuronReward.neuronId);

      switch (existingPerf) {
        case (?existing) {
          // Append this period's performance score
          let updatedScores = Array.append(existing.performanceScores, [neuronReward.performanceScore]);
          Map.set(neuronPerformances, Blob.hash, Blob.equal, neuronReward.neuronId, {
            neuronId = existing.neuronId;
            performanceScores = updatedScores;
            distributionCount = existing.distributionCount + 1;
            lastActivity = Int.max(existing.lastActivity, distribution.endTime);
          });
        };
        case null {
          // First distribution for this neuron
          Map.set(neuronPerformances, Blob.hash, Blob.equal, neuronReward.neuronId, {
            neuronId = neuronReward.neuronId;
            performanceScores = [neuronReward.performanceScore];
            distributionCount = 1;
            lastActivity = distribution.endTime;
          });
        };
      };
    };
  };

  // 4. Calculate aggregate performance score for each neuron based on timeframe
  type ScoredNeuron = {
    neuronId: Blob;
    aggregateScore: Float;
    distributionCount: Nat;
    lastActivity: Int;
  };

  let scoredNeurons = Array.map<(Blob, NeuronPerformance), ScoredNeuron>(
    Iter.toArray(Map.entries(neuronPerformances)),
    func ((neuronId, perf)) : ScoredNeuron {
      let aggregateScore = switch (timeframe) {
        case (#OneWeek) {
          // Latest period only
          if (perf.performanceScores.size() > 0) {
            perf.performanceScores[perf.performanceScores.size() - 1]
          } else { 1.0 }
        };
        case (#OneMonth) {
          // Average of last ~4 periods
          calculateAverage(perf.performanceScores)
        };
        case (#OneYear) {
          // Average of all periods (up to 52)
          calculateAverage(perf.performanceScores)
        };
        case (#AllTime) {
          // Compound return: multiply all performance scores
          // e.g., [1.1, 1.2, 0.9] = 1.1 * 1.2 * 0.9 = 1.188 (18.8% total return)
          calculateCompoundReturn(perf.performanceScores)
        };
      };

      {
        neuronId;
        aggregateScore;
        distributionCount = perf.distributionCount;
        lastActivity = perf.lastActivity;
      }
    }
  );

  // 5. Group by Principal and select best neuron per user
  type UserPerformance = {
    principal: Principal;
    bestNeuron: ScoredNeuron;
  };

  let userPerformances = Map.new<Principal, UserPerformance>();

  for (scored in scoredNeurons.vals()) {
    switch (Map.get(neuronToPrincipals, Blob.hash, Blob.equal, scored.neuronId)) {
      case (?principals) {
        // For each principal owning this neuron
        for (principal in principals.vals()) {
          switch (Map.get(userPerformances, phash, Principal.equal, principal)) {
            case (?existing) {
              // Keep neuron with highest aggregate score for this user
              if (scored.aggregateScore > existing.bestNeuron.aggregateScore) {
                Map.set(userPerformances, phash, Principal.equal, principal, {
                  principal;
                  bestNeuron = scored;
                });
              };
            };
            case null {
              // First neuron for this user
              Map.set(userPerformances, phash, Principal.equal, principal, {
                principal;
                bestNeuron = scored;
              });
            };
          };
        };
      };
      case null { /* Neuron has no owners, skip */ };
    };
  };

  // 6. Sort by aggregate performance score (descending)
  let sortedUsers = Array.sort<UserPerformance>(
    Iter.toArray(Map.vals(userPerformances)),
    func (a, b) {
      Float.compare(b.bestNeuron.aggregateScore, a.bestNeuron.aggregateScore)
    }
  );

  // 7. Take top N and create leaderboard entries
  // NOTE: No follower metadata here - frontend will fetch separately
  let topN = Array.tabulate<LeaderboardEntry>(
    Nat.min(leaderboardSize, sortedUsers.size()),
    func (i) : LeaderboardEntry {
      let user = sortedUsers[i];
      {
        rank = i + 1;
        principal = user.principal;
        neuronId = user.bestNeuron.neuronId;
        performanceScore = user.bestNeuron.aggregateScore;
        distributionsCount = user.bestNeuron.distributionCount;
        lastActivity = user.bestNeuron.lastActivity;
      }
    }
  );

  topN
};

// Helper functions
private func selectDistributions(
  allDistributions: Vector<DistributionRecord>,
  timeframe: LeaderboardTimeframe
) : [DistributionRecord] {
  let count = Vector.size(allDistributions);

  let periodsToInclude = switch (timeframe) {
    case (#OneWeek) { 1 };     // Latest period only
    case (#OneMonth) { 4 };    // Last ~4 weeks
    case (#OneYear) { 52 };    // Last 52 weeks
    case (#AllTime) { count }; // All available
  };

  let startIdx = if (count > periodsToInclude) {
    count - periodsToInclude
  } else { 0 };

  Array.tabulate<DistributionRecord>(
    Nat.min(periodsToInclude, count),
    func (i) { Vector.get(allDistributions, startIdx + i) }
  )
};

private func calculateAverage(scores: [Float]) : Float {
  if (scores.size() == 0) return 1.0;

  var sum : Float = 0.0;
  for (score in scores.vals()) {
    sum += score;
  };
  sum / Float.fromInt(scores.size())
};

private func calculateCompoundReturn(scores: [Float]) : Float {
  if (scores.size() == 0) return 1.0;

  var compound : Float = 1.0;
  for (score in scores.vals()) {
    compound *= score;
  };
  compound
};

```

**Cycle Efficiency Analysis:**

Let's analyze cycle costs to optimize:

**Current Distribution Flow (per period):**
1. Fetch all neurons from DAO: ~100-500M cycles (inter-canister call)
2. For each neuron (~100-500 neurons):
   - `calculateNeuronPerformance()`: ~100-500M cycles
   - Fetches allocation history from archive
   - Fetches price data from treasury
   - Calculates checkpoints
3. Total distribution cost: ~10-250B cycles

**Leaderboard Computation Cost (8 leaderboards, once per period):**
1. Fetch neuron owners from DAO: ~500M cycles (one-time per period)
2. Iterate through distribution history (already in memory): ~free
3. Aggregate performance scores (pure computation): ~100M cycles
4. Sort and rank (100-500 users): ~50M cycles
5. Query follower counts from DAO for top 100: ~100M cycles × 100 = 10B cycles ⚠️

**OPTIMIZATION 1: Cache Follower Counts in Leaderboard Entry**
Instead of querying DAO for each user during every leaderboard computation:
- Store follower count in leaderboard entry (from last query)
- Only refresh follower counts when leaderboard is QUERIED, not computed
- Reduces leaderboard computation from ~10B to ~1B cycles

```motoko
// During leaderboard computation: DON'T query DAO for follower counts
let topN = Array.tabulate<LeaderboardEntry>(
  Nat.min(leaderboardSize, sortedUsers.size()),
  func (i) : LeaderboardEntry {
    let user = sortedUsers[i];
    {
      rank = i + 1;
      principal = user.principal;
      neuronId = user.bestNeuron.neuronId;
      performanceScore = user.bestNeuron.aggregateScore;
      distributionsCount = user.bestNeuron.distributionCount;
      lastActivity = user.bestNeuron.lastActivity;
      followerCount = 0;  // ⚠️ Set to 0 during computation, update on query if needed
      canBeFollowed = true;  // Assume true, check on query
      followersRemaining = ?500;  // Assume max, check on query
    }
  }
);

// During query: Optionally enrich with fresh follower data if needed
// OR: Let frontend query DAO directly for follow metadata
```

**OPTIMIZATION 2: Don't Duplicate Performance Calculation**
During distribution, we already calculate performance for each neuron. Instead of recalculating:
- Store BOTH USD and ICP scores during the distribution itself
- Leaderboard computation just reads stored data (no re-calculation)

**Modified NeuronReward (Lighter Approach):**
```motoko
public type NeuronReward = {
  neuronId: Blob;
  performanceScore: Float;        // Keep for backward compatibility (USD)
  performanceScoreICP: ?Float;    // NEW: Add ICP score (optional for migration)
  votingPower: Nat;
  rewardScore: Float;
  rewardAmount: Nat;
  checkpoints: [CheckpointData];
};
```

**During distribution, calculate both:**
```motoko
let perfUSD = await* calculateNeuronPerformance(neuronId, startTime, endTime, #USD);
let perfICP = await* calculateNeuronPerformance(neuronId, startTime, endTime, #ICP);

// Store both in NeuronReward
{
  neuronId;
  performanceScore = perfUSD.performanceScore;  // USD
  performanceScoreICP = ?perfICP.performanceScore;  // ICP
  // ... rest
}
```

**OPTIMIZATION 3: Use Existing Distribution Data**
The `neuronRewards` array in each `DistributionRecord` already contains:
- Performance scores
- Neuron IDs
- Timestamps
- Checkpoints with makers

We don't need to re-query archives or recalculate anything. Just:
1. Read distribution history from memory (already loaded)
2. Filter by timeframe
3. Aggregate scores
4. Sort and rank

**Final Optimized Cost per Period:**
- Distribution with dual prices: ~20-500B cycles (2x current)
- Leaderboard computation: ~1B cycles (read-only, no DAO queries)
- **Total: ~21-501B cycles per period (acceptable)**

**Query Cost:**
- `getLeaderboard()`: ~100K cycles (pure array lookup)
- Client-side follow data fetch from DAO: ~1M cycles
- **Total: ~1.1M cycles per dashboard load**

**Critical Modification Needed:**
Currently, distributions only store one price type's performance. We need to either:
1. **Option A (Recommended):** Run dual-price calculation during each distribution
   - Store both USD and ICP performance scores in NeuronReward
   - Modify NeuronReward type to include both
   - **Cost:** ~2x cycles per distribution (run calculation twice)
2. **Option B:** Retroactively calculate ICP performance from historical data
   - Re-run `calculateNeuronPerformance()` with `#ICP` for past distributions
   - More expensive but possible as a one-time migration

**Cycle Optimization for Historical Backfill:**
For top 100 users at launch, we can cheaply backfill historical performance:
- **Key insight:** `calculateNeuronPerformance()` is already callable as a `public shared` function
- **Allocation data** is stored in archives (already fetched by the function)
- **Price data** is stored in treasury (already fetched by the function)
- **Cost:** ~100-500M cycles per neuron per calculation (expensive if done for all neurons, but manageable for 100)

**Backfill Strategy:**
1. Run initial leaderboard computation with only 1-week data (latest distribution)
2. Identify top 100 users from 1-week leaderboard
3. For those 100 users only:
   - Retroactively calculate 1-month performance (last 4 distributions, both USD and ICP)
   - Retroactively calculate 1-year performance (last 52 distributions, both USD and ICP)
   - Retroactively calculate all-time performance (all distributions, both USD and ICP)
4. Store results in leaderboards
5. Going forward, all new distributions include both USD and ICP calculations

**Cost estimate for backfill:**
- 100 users × 4 calculations (1M, 1Y, All) × 2 price types = 800 calculations
- ~100-500M cycles each = 80-400B cycles total (one-time cost)
- Can be spread over multiple transactions if needed (process 10-20 at a time)

#### Phase 3: Frontend Query APIs (Rewards Canister)

**Dashboard Query Optimization:**

The user asks: "Is it possible to get all needed for the dashboard in 1 query call?"

**Answer:** Not easily, because data comes from two canisters:
- **Rewards canister:** Leaderboard entries with performance scores
- **DAO canister:** Follow status for the viewer (who they follow, their limits)

**Recommended approach: 2 parallel calls**
```javascript
// Call both in parallel for best performance
const [leaderboardData, viewerFollowState] = await Promise.all([
  rewardsCanister.getLeaderboard(timeframe, priceType, 100, 0),
  daoCanister.getUserState(viewerPrincipal)
]);

// Client-side combination
const leaderboardWithFollowStatus = leaderboardData.map(entry => ({
  ...entry,
  isFollowedByViewer: viewerFollowState.allocationFollows.some(f => f.follow === entry.principal),
  canFollow: checkCanFollow(entry, viewerFollowState)
}));
```

**Why this is better than 1 call:**
1. **Avoids cross-canister call during query:** Rewards canister would need to call DAO canister (expensive, not allowed in queries)
2. **Parallel execution:** Browser/client can fetch both simultaneously
3. **Caching flexibility:** Frontend can cache viewerFollowState independently from leaderboard

**Alternative: Add `getLeaderboardWithFollowStatus()` as `shared query`**
This would work but has limitations:
- Cannot make inter-canister calls from query methods
- Would need to be `shared` (update call), which is slower and costs cycles
- Better for complex logic, but not needed here

**Recommended for MVP: 2 parallel query calls, combine client-side**

**Basic Leaderboard Query:**
```motoko
public query func getLeaderboard(
  timeframe: LeaderboardTimeframe,
  priceType: LeaderboardPriceType,
  limit: ?Nat,
  offset: ?Nat
) : async [LeaderboardEntry] {
  // Select the correct leaderboard
  let selectedLeaderboard = switch (timeframe, priceType) {
    case (#OneWeek, #USD) { leaderboards.oneWeekUSD };
    case (#OneWeek, #ICP) { leaderboards.oneWeekICP };
    case (#OneMonth, #USD) { leaderboards.oneMonthUSD };
    case (#OneMonth, #ICP) { leaderboards.oneMonthICP };
    case (#OneYear, #USD) { leaderboards.oneYearUSD };
    case (#OneYear, #ICP) { leaderboards.oneYearICP };
    case (#AllTime, #USD) { leaderboards.allTimeUSD };
    case (#AllTime, #ICP) { leaderboards.allTimeICP };
  };

  let start = Option.get(offset, 0);
  let count = Option.get(limit, leaderboardSize);
  let end = Nat.min(start + count, selectedLeaderboard.size());

  if (start >= selectedLeaderboard.size()) {
    return [];
  };

  Array.tabulate<LeaderboardEntry>(
    end - start,
    func (i) { selectedLeaderboard[start + i] }
  )
};
```

**Simplified API (No Cross-Canister Calls):**

Since we're doing 2-call approach, remove the complex `getLeaderboardWithFollowStatus()` that requires cross-canister calls. Keep it simple:

```motoko
// That's it - just the basic leaderboard query
// Frontend will fetch follow status separately from DAO canister
```

The follow capability checking should be done client-side using data from both canisters, or if needed server-side, it should be in the DAO canister (not rewards canister).

#### Phase 4: Leaderboard Metadata (Rewards Canister)

**Stats and Configuration:**
```motoko
public query func getLeaderboardInfo() : async {
  lastUpdate: Int;
  maxSize: Nat;
  updateEnabled: Bool;
  leaderboardCounts: {
    oneWeekUSD: Nat;
    oneWeekICP: Nat;
    oneMonthUSD: Nat;
    oneMonthICP: Nat;
    oneYearUSD: Nat;
    oneYearICP: Nat;
    allTimeUSD: Nat;
    allTimeICP: Nat;
  };
  totalDistributions: Nat;
} {
  {
    lastUpdate = leaderboardLastUpdate;
    maxSize = leaderboardSize;
    updateEnabled = leaderboardUpdateEnabled;
    leaderboardCounts = {
      oneWeekUSD = leaderboards.oneWeekUSD.size();
      oneWeekICP = leaderboards.oneWeekICP.size();
      oneMonthUSD = leaderboards.oneMonthUSD.size();
      oneMonthICP = leaderboards.oneMonthICP.size();
      oneYearUSD = leaderboards.oneYearUSD.size();
      oneYearICP = leaderboards.oneYearICP.size();
      allTimeUSD = leaderboards.allTimeUSD.size();
      allTimeICP = leaderboards.allTimeICP.size();
    };
    totalDistributions = Vector.size(distributionHistory);
  }
};
```

**Admin Configuration:**
```motoko
public shared ({ caller }) func updateLeaderboardConfig(
  size: ?Nat,
  enabled: ?Bool
) : async Result<Text, RewardsError> {
  if (not isAdmin(caller)) {
    return #err(#NotAuthorized);
  };

  switch (size) { case (?s) leaderboardSize := s; case null {} };
  switch (enabled) { case (?e) leaderboardUpdateEnabled := e; case null {} };

  #ok("Leaderboard configuration updated")
};
```

#### Phase 5: Manual Refresh (Rewards Canister)

**For testing and emergency updates:**
```motoko
public shared ({ caller }) func refreshLeaderboards() : async Result<Text, RewardsError> {
  if (not isAdmin(caller)) {
    return #err(#NotAuthorized);
  };

  await* computeAllLeaderboards<system>();
  #ok("All leaderboards refreshed successfully")
};
```

#### Phase 6: Dual-Price Performance Tracking (Critical Modification)

**Current Issue:**
- Each distribution only calculates performance for ONE price type (currently USD)
- We need BOTH USD and ICP performance scores for each distribution
- Options:
  1. Calculate both during distribution (recommended)
  2. Store raw checkpoint data and calculate on-demand (expensive)
  3. Retroactively recalculate for historical distributions (migration)

**Recommended Solution: Store Both Price Types**

Modify the `NeuronReward` type to include both USD and ICP performance:
```motoko
public type NeuronReward = {
  neuronId: Blob;
  performanceScoreUSD: Float;   // NEW: USD-based performance
  performanceScoreICP: Float;   // NEW: ICP-based performance
  votingPower: Nat;
  rewardScore: Float;           // Keep this for backward compatibility (based on USD)
  rewardAmount: Nat;
  checkpoints: [CheckpointData];
};
```

Modify the distribution flow to calculate both:
```motoko
// In processNextNeuron() around line 997-1031
let performanceUSD = await* calculateNeuronPerformance(neuronId, startTime, endTime, #USD);
let performanceICP = await* calculateNeuronPerformance(neuronId, startTime, endTime, #ICP);

// Store both in NeuronReward
let neuronReward : NeuronReward = {
  neuronId;
  performanceScoreUSD = performanceUSD.performanceScore;
  performanceScoreICP = performanceICP.performanceScore;
  votingPower = extractVotingPower(performanceUSD);  // Doesn't matter which, VP is same
  rewardScore = baseRewardScore;  // Still calculated from USD for rewards
  rewardAmount = rewardAmt;
  checkpoints = performanceUSD.checkpoints;  // Can keep USD checkpoints for audit
};
```

**Alternative: Lighter Approach**
Instead of modifying NeuronReward (breaking change), add a separate storage:
```motoko
// Parallel storage for ICP performance scores
stable var neuronICPPerformance: Map<(Nat, Blob), Float> = Map.new();
// Key: (distributionId, neuronId) -> ICP performance score

// During distribution, also calculate and store ICP performance
let performanceICP = await* calculateNeuronPerformance(neuronId, startTime, endTime, #ICP);
Map.set(
  neuronICPPerformance,
  hashDistributionNeuron,
  equalDistributionNeuron,
  (currentDistributionId, neuronId),
  performanceICP.performanceScore
);
```

This keeps the existing DistributionRecord structure intact and adds ICP data separately.

### Edge Cases & Considerations

1. **No neuron owners data:**
   - If DAO returns empty mapping, skip that neuron
   - Log warning but don't fail entire computation

2. **User has multiple high-performing neurons:**
   - For each timeframe/priceType combo, select neuron with highest aggregate score
   - A user may appear with different neurons in different leaderboards
   - Example: User's neuron A best for 1-week USD, but neuron B best for all-time ICP

3. **Tie-breaking:**
   - If performance scores equal, use distribution count as tiebreaker (more data = higher)
   - If still equal, use lastActivity (more recent = higher)
   - If still equal, use Principal ordering (deterministic)

4. **Follower count synchronization:**
   - Query DAO for follower count during leaderboard computation
   - May be slightly stale (up to 7 days old) but acceptable
   - Frontend can query real-time follow status separately

5. **Performance with many distributions:**
   - One Week: Only latest distribution
   - One Month: Last 4 distributions (~28 days)
   - One Year: Last 52 distributions (~364 days)
   - All-Time: All available distributions
   - Computation bounded by 52-distribution history limit

6. **Neuron penalties:**
   - Neurons with `penaltyMultiplier = 0` are excluded from distributions
   - These won't appear in neuron rewards, so naturally excluded from leaderboard
   - Neurons with partial penalties will have reduced reward amounts but unaffected performance scores
   - **Performance scores are pure allocation skill, NOT affected by penalties**

7. **Missing price type data:**
   - If historical distributions only have USD performance, ICP leaderboards will be incomplete
   - Need migration strategy to backfill ICP performance or start fresh
   - Frontend should show "Insufficient data" for timeframes without enough distributions

8. **Empty leaderboards:**
   - If no distributions exist yet, all leaderboards will be empty arrays
   - Frontend should handle gracefully with "No data yet" message

9. **Compound return edge cases (All-Time):**
   - If a neuron had 0.0 performance (total loss) in any period, compound = 0.0
   - This is correct: one period of total loss means total account loss
   - Protects against showing unrealistic recovery scenarios

## Implementation Summary

### Files to Modify

#### 1. [src/rewards/rewards.mo](src/rewards/rewards.mo)
**Major additions:**
- **New types:** `LeaderboardTimeframe`, `LeaderboardPriceType`, `LeaderboardEntry`, `FollowCapability`, `FollowBlockReason`
- **Storage:** 8 leaderboard arrays + metadata (oneWeekUSD, oneWeekICP, oneMonthUSD, etc.)
- **Core function:** `computeAllLeaderboards()` - generates all 8 leaderboards
- **Helper function:** `computeLeaderboardFor()` - generates one leaderboard for timeframe/priceType
- **Query functions:**
  - `getLeaderboard(timeframe, priceType, ?limit, ?offset)` - fetch one leaderboard
  - `getLeaderboardWithFollowStatus(timeframe, priceType, ?limit)` - with follow metadata
  - `canFollowUser(targetPrincipal)` - check follow capability
  - `getLeaderboardInfo()` - metadata about all leaderboards
- **Admin functions:**
  - `updateLeaderboardConfig(size, enabled)` - configure settings
  - `refreshLeaderboards()` - manual trigger for testing
- **Hook:** Call `computeAllLeaderboards()` at end of `processNextNeuron()` when distribution completes

**Critical modification:**
- **Dual-price tracking:** Must store BOTH USD and ICP performance scores per distribution
- **Option A:** Modify `NeuronReward` type to include `performanceScoreUSD` and `performanceScoreICP`
- **Option B:** Add parallel storage `neuronICPPerformance: Map<(Nat, Blob), Float>`

#### 2. [src/DAO_backend/DAO.mo](src/DAO_backend/DAO.mo)
**New query functions:**

1. `getAllNeuronOwners() : async [(Blob, [Principal])]`
   - Iterates over `userStates` map
   - For each user, returns their `neurons` array with their Principal
   - Builds reverse mapping: neuronId → [principals who own it]
   - Used by rewards canister to deduplicate by Principal

2. `getUsersFollowerInfo(principals: [Principal]) : async [FollowerInfo]`
   - Batch query for follower metadata
   - Returns array of `{ followerCount: Nat; canBeFollowed: Bool }` for each principal
   - Used by frontend to enrich leaderboard entries
   - More efficient than querying each user individually
   ```motoko
   public type FollowerInfo = {
     followerCount: Nat;
     canBeFollowed: Bool;  // true if followerCount < MAX_FOLLOWERS
   };

   public query func getUsersFollowerInfo(principals: [Principal]) : async [FollowerInfo] {
     Array.map<Principal, FollowerInfo>(
       principals,
       func (p) {
         switch (Map.get(userStates, phash, Principal.equal, p)) {
           case (?state) {
             let count = state.allocationFollowedBy.size();
             { followerCount = count; canBeFollowed = count < MAX_FOLLOWERS }
           };
           case null {
             { followerCount = 0; canBeFollowed = true }
           };
         }
       }
     )
   };
   ```

#### 3. Frontend Integration (Documentation)
**Query pattern:**
```javascript
// User selects view
const timeframe = 'OneMonth';  // OneWeek | OneMonth | OneYear | AllTime
const priceType = 'USD';       // USD | ICP

// Fetch leaderboard
const { entries, followStatuses, viewerFollowing } =
  await rewardsCanister.getLeaderboardWithFollowStatus(
    { [timeframe]: null },
    { [priceType]: null },
    100
  );

// Display with sorting/filtering
```

**UI components:**
- Timeframe selector (tabs/dropdown)
- Price type toggle (USD/ICP)
- Leaderboard table with rank, principal, return %, follower count
- Follow/Unfollow buttons with proper state management
- Error tooltips for disabled follow buttons

## Testing Strategy

### Unit Tests
1. Leaderboard computation with sample data
2. Deduplication logic (one neuron per user)
3. Ranking score calculation with different weights
4. Follow capability checks for all edge cases

### Integration Tests
1. Full reward distribution → leaderboard update flow
2. Query leaderboard after distribution
3. Follow capability checks against real DAO state
4. Multiple distributions with changing performance

### Edge Case Tests
1. User with no neurons
2. Neuron with no owner (orphaned)
3. User with 10+ neurons (select best)
4. Tie-breaking scenarios
5. Empty distribution history
6. Leaderboard with < 100 qualifying users

## Configuration Recommendations

**Initial Settings:**
```motoko
leaderboardSize = 100              // Top N per leaderboard
leaderboardUpdateEnabled = true    // Auto-update after distributions
```

**Rationale:**
- 100 leaders per leaderboard × 8 leaderboards = 800 total entries (manageable storage)
- No complex weighting - pure performance scores make rankings transparent
- Frontend can sort/filter as needed across 8 pre-computed views
- Updates automatically every 7 days with minimal cycle cost

## Frontend Integration Guide

### Leaderboard Dashboard with Filters

```javascript
// User selects timeframe and price type via dropdown/tabs
const [timeframe, setTimeframe] = useState('OneMonth');  // OneWeek, OneMonth, OneYear, AllTime
const [priceType, setPriceType] = useState('USD');      // USD, ICP

// Fetch data from BOTH canisters in parallel
const [leaderboardEntries, viewerFollowState] = await Promise.all([
  rewardsCanister.getLeaderboard(
    { [timeframe]: null },  // Variant type
    { [priceType]: null },  // Variant type
    100,  // Limit
    0     // Offset
  ),
  daoCanister.getUserState(viewerPrincipal)
]);

// Fetch follower info for leaderboard entries (batch query to DAO)
const leaderPrincipals = leaderboardEntries.map(e => e.principal);
const leaderFollowInfo = await daoCanister.getUsersFollowerInfo(leaderPrincipals);

// Combine data client-side
const entries = leaderboardEntries.map((entry, idx) => ({
  ...entry,
  followerCount: leaderFollowInfo[idx].followerCount,
  canBeFollowed: leaderFollowInfo[idx].followerCount < 500,
  isFollowedByViewer: viewerFollowState.allocationFollows.some(
    f => f.follow === entry.principal
  )
}));

// Render table
entries.forEach((entry, index) => {
  const followStatus = followStatuses[index];
  const isFollowing = viewerFollowing.includes(entry.principal);

  // Calculate return percentage (performance score - 1.0)
  const returnPct = ((entry.performanceScore - 1.0) * 100).toFixed(2);
  const returnClass = returnPct >= 0 ? 'positive' : 'negative';

  // Display rank, principal, performance
  console.log(`#${entry.rank} - ${entry.principal.substring(0, 8)}...`);
  console.log(`Return: ${returnPct}% (${priceType})`);
  console.log(`Periods: ${entry.distributionsCount}`);
  console.log(`Followers: ${entry.followerCount}/${entry.canBeFollowed ? entry.followersRemaining : 'FULL'}`);

  // Follow button state
  if (isFollowing) {
    console.log('Button: Unfollow (enabled)');
  } else if (followStatus.canFollow) {
    console.log('Button: Follow (enabled)');
  } else {
    const reason = followStatus.reason;
    console.log(`Button: Follow (disabled) - ${formatReason(reason)}`);
  }
});
```

### Example UI Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  Top Performers                                                 │
├─────────────────────────────────────────────────────────────────┤
│  Timeframe: [1 Week] [1 Month*] [1 Year] [All-Time]            │
│  Currency:  [USD*] [ICP]                                        │
├─────────────────────────────────────────────────────────────────┤
│  Rank  User             Return (USD)  Periods  Followers Action │
│  ───────────────────────────────────────────────────────────────│
│   1    abc123...xyz     +127.3%        4       245/500   Follow │
│   2    def456...abc     +89.5%         4       500/500   - FULL │
│   3    ghi789...def     +67.2%         4       12/500    Follow │
│   4    jkl012...ghi     +54.8%         3       87/500    Follow │
│  ...                                                             │
└─────────────────────────────────────────────────────────────────┘

Notes:
- "Return" shows (performanceScore - 1.0) × 100%
  - 1.273 → +127.3%
  - 0.895 → -10.5%
- "Periods" = distributionsCount (how many distributions in this timeframe)
- "Followers" shows current count and capacity
- Action button respects follow limits and rate limits
```

### Error Messages
```javascript
function formatReason(reason) {
  if (!reason) return '';

  switch (reason.type) {
    case 'LeaderAtMaxFollowers':
      return `This user has reached max followers (${reason.current}/${reason.max})`;
    case 'FollowerAtMaxFollowed':
      return `You're following max users (${reason.current}/${reason.max}). Unfollow someone first.`;
    case 'FollowerRateLimited':
      return `Rate limited: ${reason.actionsToday}/${reason.max} follow actions today. Try again tomorrow.`;
    case 'AlreadyFollowing':
      return `You're already following this user`;
    case 'CannotFollowSelf':
      return `You can't follow yourself`;
  }
}
```

### Follow/Unfollow Actions
```javascript
// Use existing DAO functions
async function followLeader(principal) {
  // Check capability first
  const capability = await rewardsCanister.canFollowUser(principal);
  if (!capability.canFollow) {
    alert(formatReason(capability.reason));
    return;
  }

  // Execute follow via DAO
  const result = await daoCanister.followAllocation(principal);
  if (result.ok) {
    alert('Successfully following!');
    refreshLeaderboard();
  } else {
    alert(`Error: ${result.err}`);
  }
}

async function unfollowLeader(principal) {
  const result = await daoCanister.unfollowAllocation(principal);
  if (result.ok) {
    alert('Unfollowed');
    refreshLeaderboard();
  }
}
```

## Success Metrics

1. **Cycle Efficiency:**
   - Leaderboard update < 1B cycles per distribution
   - Query cost < 1M cycles per request

2. **Data Accuracy:**
   - One neuron per user (no duplicates)
   - Correct ranking order per configured weights
   - Follow capability checks match DAO state

3. **User Experience:**
   - Leaderboard loads < 1s
   - Clear error messages when can't follow
   - Real-time follow button state

## Future Enhancements

1. **Historical Leaderboard Snapshots:**
   - Store leaderboard state at each distribution (archive)
   - Allow viewing "top 100 in January 2026" vs "top 100 in February 2026"
   - Track rank changes over time ("+5 from last week")

2. **Additional Filters:**
   - Min distributions participated (e.g., "only show users with 10+ distributions")
   - Min follower count (e.g., "only show users with 50+ followers")
   - Search by principal

3. **Performance Badges/Tags:**
   - "Top 10" badge
   - "Consistent Performer" (in top 100 for X consecutive periods)
   - "Rising Star" (biggest rank improvement)
   - "High Stakes" (high voting power)

4. **Follow Network Insights:**
   - "X of your follows also follow this user" (social proof)
   - "Your follows have averaged +Y% return this month"
   - Show allocation similarity to users you follow

5. **Performance Charts:**
   - Graph showing performance score evolution over time
   - Compare multiple users' performance
   - Show allocation history alongside performance

6. **Notifications:**
   - Alert when a followed user enters/exits top 100
   - Alert when a followed user changes allocations
   - Alert when ranking spots open up (someone drops below 500 followers)

## Verification Plan

After implementation, verify:

1. **All 8 leaderboards update automatically after each distribution**
   - Check `leaderboardLastUpdate` timestamp matches distribution completion
   - Verify entries in each leaderboard (oneWeekUSD, oneWeekICP, etc.)
   - Confirm rankings differ appropriately between USD and ICP views

2. **No duplicate users within each leaderboard**
   - Query each leaderboard, extract principals, verify all unique
   - Check users with multiple neurons only appear once per leaderboard
   - Verify same user may appear with different neurons in different leaderboards

3. **Performance scores are correct for each timeframe**
   - **One Week:** Verify performance score matches latest distribution
   - **One Month:** Verify average of last 4 distributions
   - **One Year:** Verify average of last 52 distributions (or all available)
   - **All-Time:** Verify compound return (multiply all scores together)

4. **USD vs ICP leaderboards show different rankings**
   - Compare oneWeekUSD vs oneWeekICP - rankings should differ
   - Verify this reflects actual price movements (ICP vs USD performance)

5. **Follow capabilities work correctly**
   - Test following a user at max followers (should fail with LeaderAtMaxFollowers)
   - Test following when at personal limit (should fail with FollowerAtMaxFollowed)
   - Test rate limit (10 actions in 24h - should fail with FollowerRateLimited)
   - Test following user successfully
   - Test unfollowing user successfully

6. **Ranking is stable and deterministic**
   - Call `refreshLeaderboards()` multiple times
   - Verify order doesn't change without new distributions
   - Verify tie-breaking works (distribution count, then lastActivity, then Principal)

7. **Performance is acceptable**
   - Time the leaderboard update after distribution (target: < 5B cycles for all 8)
   - Measure query response time for `getLeaderboard()` (target: < 1M cycles)
   - Check memory consumption (800 entries × ~200 bytes ≈ 160KB acceptable)

8. **Edge cases handled gracefully**
   - Empty distribution history → empty leaderboards
   - User with single distribution → appears in OneWeek but not necessarily OneMonth
   - Neuron with penalty=0 → excluded from all leaderboards
   - No ICP performance data (if not yet implemented) → ICP leaderboards empty or incomplete

9. **Manual testing scenarios:**
   ```bash
   # Query each leaderboard
   dfx canister call rewards getLeaderboard '(variant { OneWeek }, variant { USD }, opt 10, opt 0)'
   dfx canister call rewards getLeaderboard '(variant { OneWeek }, variant { ICP }, opt 10, opt 0)'
   dfx canister call rewards getLeaderboard '(variant { OneMonth }, variant { USD }, opt 10, opt 0)'
   # ... test all 8 combinations

   # Get leaderboard info
   dfx canister call rewards getLeaderboardInfo '()'

   # Test follow capability
   dfx canister call rewards canFollowUser '(principal "abc123...")'

   # Manual refresh (admin only)
   dfx canister call rewards refreshLeaderboards '()'
   ```
