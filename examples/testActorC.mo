import Result "mo:base/Result";
import DAO_types "../src/DAO_backend/dao_types";

actor testActorC {
  // Get the interface type from DAO_types
  type DAOActor = DAO_types.Self;

  // The actor we're proxying to (this would be set to the actual DAO canister ID in practice)
  let dao_canister : DAOActor = actor ("ywhqf-eyaaa-aaaad-qg6tq-cai");

  public shared func updateAllocation(newAllocations : [DAO_types.Allocation]) : async Result.Result<Text, DAO_types.UpdateError> {
    await dao_canister.updateAllocation(newAllocations);
  };

  public shared func getUserAllocation() : async ?DAO_types.UserState {
    await dao_canister.getUserAllocation();
  };
  public shared func followAllocation(followee : Principal) : async Result.Result<Text, DAO_types.FollowError> {
    await dao_canister.followAllocation(followee);
  };
  public shared func unfollowAllocation(followee : Principal) : async Result.Result<Text, DAO_types.UnfollowError> {
    await dao_canister.unfollowAllocation(followee);
  };
};
