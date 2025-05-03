import Result "mo:base/Result";
import SpamProtection "../helper/spam_protection";
import Map "mo:map/Map";

module {

  public type PricePoint = {
    icpPrice : Nat;
    usdPrice : Float;
    time : Int;
  };
  public type TokenDetails = {
    Active : Bool;
    isPaused : Bool;
    epochAdded : Int;
    tokenName : Text;
    tokenSymbol : Text;
    tokenDecimals : Nat;
    tokenTransferFee : Nat;
    balance : Nat;
    priceInICP : Nat;
    priceInUSD : Float;
    tokenType : TokenType;
    pastPrices : [PricePoint];
    lastTimeSynced : Int; // if read from tokendetails in DAO, this will be the last time it was synced from treasury (and the opposite for treasury)
    pausedDueToSyncFailure : Bool;
  };

  public type LogLevel = {
    #INFO;
    #WARN;
    #ERROR;
  };

  // Log entry structure
  public type LogEntry = {
    timestamp : Int;
    level : LogLevel;
    component : Text;
    message : Text;
    context : Text;
  };

  public type Holdings = {
    amount : Nat;
    valueUSD : Nat;
  };

  public type Allocation = {
    token : Principal;
    basisPoints : Nat;
  };

  public type NeuronVP = {
    neuronId : Blob;
    votingPower : Nat;
  };

  //tracking allocations per neuron
  public type NeuronAllocation = {
    allocations : [Allocation];
    lastUpdate : Int;
    votingPower : Nat;
    lastAllocationMaker : Principal;
  };

  // Map type for neuron allocations
  public type NeuronAllocationMap = Map.Map<Blob, NeuronAllocation>;

  // Voting power and allocation storage
  public type UserState = {
    // User's allocations
    allocations : [Allocation];
    // Last cached voting power from snapshot
    votingPower : Nat;
    // Timestamp when voting power was last updated
    lastVotingPowerUpdate : Int;
    // Last allocation update timestamp
    lastAllocationUpdate : Int;
    // Allocation maker
    lastAllocationMaker : Principal;
    // Past allocations max 50 saved
    pastAllocations : [{
      from : Int;
      to : Int;
      allocation : [Allocation];
      allocationMaker : Principal;
    }];
    // Allocation follows, max 3
    allocationFollows : [{ since : Int; follow : Principal }];
    // Allocation followed by, max 50
    allocationFollowedBy : [{ since : Int; follow : Principal }];
    // Follow/unfollow actions last (Max saved depending on MAX_FOLLOW_UNFOLLOW_ACTIONS_PER_DAY)
    followUnfollowActions : [Int];
    neurons : [NeuronVP];
  };

  public type SystemState = {
    #Active;
    #Paused;
    #Emergency;
  };

  public type TokenType = {
    #ICP;
    #ICRC12;
    #ICRC3;
  };

  public type SyncError = {
    #NotTreasury;
    #UnexpectedError : Text;
  };

  public type SystemParameter = {
    #FollowDepth : Nat;
    #MaxFollowers : Nat;
    #MaxPastAllocations : Nat;
    #SnapshotInterval : Nat;
    #MaxTotalUpdates : Nat;
    #MaxAllocationsPerDay : Int;
    #AllocationWindow : Nat;
    #MaxFollowUnfollowActionsPerDay : Nat;
    #MaxFollowed : Nat;
    #LogAdmin : Principal;
  };

  public type UpdateError = {
    #SystemInactive;
    #InvalidAllocation;
    #UnexpectedError : Text;
    #NotAllowed;
    #NoVotingPower;
  };

  public type FollowError = {
    #NotAllowed;
    #NotAdmin;
    #AlreadyFollowing;
    #FollowLimitReached;
    #FolloweeNotFound;
    #FollowerNotFound;
    #FolloweeNoAllocationYetMade;
    #FollowerNoAllocationYetMade;
    #FolloweeIsSelf;
    #FolloweeLimitReached;
    #UnexpectedError : Text;
    #SystemInactive;
    #FollowUnfollowLimitReached;
  };

  public type UnfollowError = {
    #NotAllowed;
    #NotAdmin;
    #AlreadyUnfollowing;
    #FolloweeIsSelf;
    #FolloweeNotFound;
    #FollowerNotFound;
    #UnexpectedError : Text;
    #SystemInactive;
    #FollowUnfollowLimitReached;
  };

  public type AuthorizationError = {
    #NotAllowed;
    #NotAdmin;
    #UnexpectedError : Text;
  };

  public type HistoricBalanceAllocation = {
    balances : [(Principal, Nat)]; // Token -> basis points of total balance
    allocations : [(Principal, Nat)]; // Token -> basis points of voting power allocation
    totalWorthInICP : Nat;
    totalWorthInUSD : Float;
  };

  public type Self = actor {
    updateAllocation : shared ([Allocation]) -> async Result.Result<Text, UpdateError>;
    getAggregateAllocation : shared query () -> async [(Principal, Nat)];
    getUserAllocation : shared query () -> async ?UserState;
    getSnapshotInfo : shared query () -> async {
      lastSnapshotId : Nat;
      lastSnapshotTime : Int;
      totalVotingPower : Nat;
    };
    addAdmin : shared (Principal) -> async Result.Result<Text, AuthorizationError>;
    removeAdmin : shared (Principal) -> async Result.Result<Text, AuthorizationError>;
    updateSystemState : shared (SystemState) -> async Result.Result<Text, AuthorizationError>;
    updateSpamParameters : shared ({
      allowedCalls : ?Nat;
      allowedSilentWarnings : ?Nat;
      timeWindowSpamCheck : ?Int;
    }) -> async Result.Result<Text, AuthorizationError>;
    addToken : shared (Principal, TokenType) -> async Result.Result<Text, AuthorizationError>;
    removeToken : shared (Principal) -> async Result.Result<Text, AuthorizationError>;
    pauseToken : shared (Principal) -> async Result.Result<Text, AuthorizationError>;
    unpauseToken : shared (Principal) -> async Result.Result<Text, AuthorizationError>;
    grantAdminPermission : shared (Principal, SpamProtection.AdminFunction, Nat) -> async Result.Result<Text, AuthorizationError>;
    getAdminPermissions : shared () -> async [(Principal, [SpamProtection.AdminPermission])];
    votingPowerMetrics : shared () -> async Result.Result<{ totalVotingPower : Nat; totalVotingPowerByHotkeySetters : Nat; allocatedVotingPower : Nat }, AuthorizationError>;
    followAllocation : shared (Principal) -> async Result.Result<Text, FollowError>;
    unfollowAllocation : shared (Principal) -> async Result.Result<Text, UnfollowError>;
    syncTokenDetailsFromTreasury : shared () -> async Result.Result<Text, SyncError>;
    getSystemParameters : shared () -> async [SystemParameter];
    getTokenDetails : shared () -> async [(Principal, TokenDetails)];
  };
};
