import Array "mo:base/Array";

module {
  //==========================================================================
  // OLD TYPES (LeaderboardEntry WITHOUT totalRewardsEarned)
  //==========================================================================

  type OldLeaderboardEntry = {
    rank: Nat;
    principal: Principal;
    neuronId: Blob;
    performanceScore: Float;
    distributionsCount: Nat;
    lastActivity: Int;
    displayName: ?Text;
  };

  //==========================================================================
  // NEW TYPES (LeaderboardEntry WITH totalRewardsEarned)
  //==========================================================================

  type NewLeaderboardEntry = {
    rank: Nat;
    principal: Principal;
    neuronId: Blob;
    performanceScore: Float;
    distributionsCount: Nat;
    lastActivity: Int;
    displayName: ?Text;
    totalRewardsEarned: Nat;
  };

  //==========================================================================
  // STATE TYPES
  //==========================================================================

  type LeaderboardsRecord<T> = {
    oneWeekUSD: [T];
    oneWeekICP: [T];
    oneMonthUSD: [T];
    oneMonthICP: [T];
    oneYearUSD: [T];
    oneYearICP: [T];
    allTimeUSD: [T];
    allTimeICP: [T];
  };

  public type OldState = {
    leaderboards: LeaderboardsRecord<OldLeaderboardEntry>;
  };

  public type NewState = {
    leaderboards: LeaderboardsRecord<NewLeaderboardEntry>;
  };

  //==========================================================================
  // MIGRATION HELPERS
  //==========================================================================

  func migrateEntry(old: OldLeaderboardEntry): NewLeaderboardEntry {
    {
      rank = old.rank;
      principal = old.principal;
      neuronId = old.neuronId;
      performanceScore = old.performanceScore;
      distributionsCount = old.distributionsCount;
      lastActivity = old.lastActivity;
      displayName = old.displayName;
      totalRewardsEarned = 0;
    }
  };

  func migrateBoard(board: [OldLeaderboardEntry]): [NewLeaderboardEntry] {
    Array.map<OldLeaderboardEntry, NewLeaderboardEntry>(board, migrateEntry)
  };

  //==========================================================================
  // MAIN MIGRATION FUNCTION
  //==========================================================================

  public func migrate(old: OldState): NewState {
    {
      leaderboards = {
        oneWeekUSD = migrateBoard(old.leaderboards.oneWeekUSD);
        oneWeekICP = migrateBoard(old.leaderboards.oneWeekICP);
        oneMonthUSD = migrateBoard(old.leaderboards.oneMonthUSD);
        oneMonthICP = migrateBoard(old.leaderboards.oneMonthICP);
        oneYearUSD = migrateBoard(old.leaderboards.oneYearUSD);
        oneYearICP = migrateBoard(old.leaderboards.oneYearICP);
        allTimeUSD = migrateBoard(old.leaderboards.allTimeUSD);
        allTimeICP = migrateBoard(old.leaderboards.allTimeICP);
      };
    }
  };
}
