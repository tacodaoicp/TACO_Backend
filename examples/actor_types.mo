import Result "mo:base/Result";
import DAO "../src/DAO_backend/dao_types";
import MintingVaultTypes "../src/minting_vault/minting_vault_types";
module {

  public type Allocation = DAO.Allocation;

  public type UserState = DAO.UserState;

  public type UpdateError = DAO.UpdateError;

  public type FollowError = DAO.FollowError;

  public type UnfollowError = DAO.UnfollowError;

  public type Self = actor {
    updateAllocation : shared ([Allocation]) -> async Result.Result<Text, UpdateError>;
    getUserAllocation : shared query () -> async ?UserState;
    followAllocation : shared (Principal) -> async Result.Result<Text, FollowError>;
    unfollowAllocation : shared (Principal) -> async Result.Result<Text, UnfollowError>;
    TransferICPtoMintVault : shared (Nat) -> async Nat;
    TransferICRCBtoMintVault : shared (Nat) -> async Nat;
    TransferICRCAtoMintVault : shared (Nat) -> async Nat;
    swapTokenForTaco : shared (Principal, Nat, Nat) -> async Result.Result<MintingVaultTypes.SwapResult, Text>;

  };

};
