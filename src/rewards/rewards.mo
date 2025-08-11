import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";

import CanisterIds "../helper/CanisterIds";

shared (deployer) actor class Rewards() = this {

  private func this_canister_id() : Principal {
    Principal.fromActor(this);
  };

  // Types for our rewards calculations
  public type PriceType = {
    #ICP;
    #USD;
  };

  public type PerformanceResult = {
    user: Principal;
    startTime: Int;
    endTime: Int;
    initialValue: Float; // Always 1.0
    finalValue: Float;   // Calculated performance score
    allocationChanges: Nat; // Number of rebalances in the period
  };

  public type RewardsError = {
    #UserNotFound;
    #InvalidTimeRange;
    #PriceDataMissing: {token: Principal; timestamp: Int};
    #AllocationDataMissing;
    #SystemError: Text;
  };

  // External canister interfaces
  private transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  
  private transient let DAO_ALLOCATION_ARCHIVE_ID = canister_ids.getCanisterId(#dao_allocation_archive);
  private transient let PRICE_ARCHIVE_ID = canister_ids.getCanisterId(#price_archive);

  // Import the external canister types (we'll need to define these)
  type AllocationChangeBlockData = {
    user: Principal;
    newAllocations: [Allocation];
    oldAllocations: [Allocation];
    timestamp: Int;
    changeType: AllocationChangeType;
    id: Nat;
    maker: Principal;
    reason: ?Text;
    votingPower: Nat;
  };

  type Allocation = {
    token: Principal;
    basisPoints: Nat; // Out of 10,000 (100%)
  };

  type AllocationChangeType = {
    #FollowAction: {followedUser: Principal};
    #UserUpdate: {userInitiated: Bool};
    #SystemRebalance;
    #VotingPowerChange;
  };

  type ArchiveError = {
    #BlockNotFound;
    #InvalidBlockType;
    #InvalidData;
    #InvalidTimeRange;
    #NotAuthorized;
    #StorageFull;
    #SystemError: Text;
  };

  type PriceInfo = {
    icpPrice: Nat;
    usdPrice: Float;
    timestamp: Int;
  };

  // External canister interfaces
  type AllocationArchive = actor {
    getAllocationChangesByUserInTimeRange: (Principal, Int, Int) -> async Result.Result<[AllocationChangeBlockData], ArchiveError>;
  };

  type PriceArchive = actor {
    getPriceAtTime: (Principal, Int) -> async Result.Result<?PriceInfo, ArchiveError>;
  };

  private transient let allocationArchive : AllocationArchive = actor (Principal.toText(DAO_ALLOCATION_ARCHIVE_ID));
  private transient let priceArchive : PriceArchive = actor (Principal.toText(PRICE_ARCHIVE_ID));

  //=========================================================================
  // Main Query Method
  //=========================================================================

  public query func calculateUserPerformance(
    user: Principal,
    startTime: Int,
    endTime: Int,
    priceType: PriceType
  ) : async Result.Result<PerformanceResult, RewardsError> {
    
    if (startTime >= endTime) {
      return #err(#InvalidTimeRange);
    };

    // This will be implemented as a composite query once we have the required methods
    // For now, return a placeholder
    #ok({
      user = user;
      startTime = startTime;
      endTime = endTime;
      initialValue = 1.0;
      finalValue = 1.0; // Placeholder
      allocationChanges = 0;
    });
  };

  //=========================================================================
  // Helper Functions (for when we implement the full logic)
  //=========================================================================

  // Convert allocation basis points to percentage (0.0 to 1.0)
  private func basisPointsToPercentage(basisPoints: Nat) : Float {
    Float.fromInt(basisPoints) / 10000.0;
  };

  // Get price value based on price type
  private func getPriceValue(priceInfo: PriceInfo, priceType: PriceType) : Float {
    switch (priceType) {
      case (#ICP) { Float.fromInt(priceInfo.icpPrice) / 100000000.0 }; // Convert from e8s
      case (#USD) { priceInfo.usdPrice };
    };
  };

  // Calculate portfolio value given allocations and prices
  private func calculatePortfolioValue(
    allocations: [Allocation], 
    prices: [(Principal, PriceInfo)],
    priceType: PriceType,
    baseValue: Float
  ) : Float {
    var totalValue : Float = 0.0;
    
    for (allocation in allocations.vals()) {
      let percentage = basisPointsToPercentage(allocation.basisPoints);
      let tokenValue = baseValue * percentage;
      
      // Find the price for this token
      var priceMultiplier : Float = 1.0;
      for ((token, priceInfo) in prices.vals()) {
        if (Principal.equal(token, allocation.token)) {
          // This would need to be adjusted based on how we handle price changes
          // For now, we assume prices are relative to initial prices
          priceMultiplier := getPriceValue(priceInfo, priceType);
        };
      };
      
      totalValue += tokenValue * priceMultiplier;
    };
    
    totalValue;
  };

  //=========================================================================
  // Admin Functions
  //=========================================================================

  public shared ({ caller }) func getCanisterStatus() : async {
    allocationArchiveId: Principal;
    priceArchiveId: Principal;
    environment: Text;
  } {
    {
      allocationArchiveId = DAO_ALLOCATION_ARCHIVE_ID;
      priceArchiveId = PRICE_ARCHIVE_ID;
      environment = switch (canister_ids.getEnvironment()) {
        case (#Staging) { "staging" };
        case (#Production) { "production" };
      };
    };
  };
}
