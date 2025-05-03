import Result "mo:base/Result";
import DAO_types "../src/DAO_backend/dao_types";
import MintingVaultTypes "../src/minting_vault/minting_vault_types";
import ICPLedger "../src/helper/Ledger";
import ICRC1 "mo:icrc1/ICRC1";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import Prim "mo:prim";
import TreasuryTypes "../src/treasury/treasury_types";
import Blob "mo:base/Blob";
import Time "mo:base/Time";
import Int "mo:base/Int";
import Debug "mo:base/Debug";

actor testActorA {
  // Get the interface type from DAO_types
  type DAOActor = DAO_types.Self;
  type MintingVaultActor = actor {
    swapTokenForTaco : shared (token : Principal, block : Nat, minimumReceive : Nat) -> async Result.Result<MintingVaultTypes.SwapResult, Text>;
    estimateSwapAmount : query (token : Principal, amount : Nat) -> async Result.Result<{ maxAcceptedAmount : Nat; estimatedTacoAmount : Nat; premium : Float; tokenPrice : Nat; tacoPrice : Nat }, Text>;
    getVaultStatus : query () -> async {
      tokenDetails : [(Principal, MintingVaultTypes.TokenDetails)];
      targetAllocations : [(Principal, Nat)];
      currentAllocations : [(Principal, Nat)];
      exchangeRates : [(Principal, Float)];
      premiumRange : { min : Float; max : Float };
      totalValueICP : Nat;
    };
  };
  type TreasuryActor = TreasuryTypes.Self;

  let icp = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : ICPLedger.Interface;
  let icrcA = actor ("mxzaz-hqaaa-aaaar-qaada-cai") : ICRC1.FullInterface;
  let icrcB = actor ("zxeu2-7aaaa-aaaaq-aaafa-cai") : ICRC1.FullInterface;

  var add : Nat64 = 1;

  // The actor we're proxying to (this would be set to the actual DAO canister ID in practice)
  let dao_canister : DAOActor = actor ("ywhqf-eyaaa-aaaad-qg6tq-cai");
  let mintingVault : MintingVaultActor = actor ("z3jul-lqaaa-aaaad-qg6ua-cai");
  let treasury : TreasuryActor = actor ("z4is7-giaaa-aaaad-qg6uq-cai");

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

  public func TransferICPtoMintVault(amount : Nat) : async Nat {
    var Block : Nat64 = 0;
    add += 1;
    let transferResult = await icp.transfer({
      memo : Nat64 = 0;
      from_subaccount = null;
      to = Principal.toLedgerAccount(Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai"), ?Blob.fromArray([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]));
      amount = {
        e8s = Prim.natToNat64(amount);
      };
      fee = { e8s = 10000 };
      created_at_time = ?{
        timestamp_nanos = (Prim.natToNat64(Int.abs(Time.now())) + add);
      };
    });
    add += 1;

    switch (transferResult) {
      case (#Ok(value)) { Block := value };
      case (#Err(value)) {
        Debug.print("Transfer failed: " # debug_show (value));
      };
    };
    Prim.nat64ToNat(Block);
  };

  public func TransferICRCBtoMintVault(amount : Nat) : async Nat {
    var Block = 0;
    add += 1;
    let transferResult = await icrcB.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai");
        subaccount = ?Blob.fromArray([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = amount;
      fee = ?10000;
      memo = null;
      created_at_time = ?(Prim.natToNat64(Int.abs(Time.now())) + add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) { Block := value };
      case (#Err(value)) {
        Debug.print("Transfer failed: " # debug_show (value));
      };
    };
    Block;
  };

  public func TransferICRCAtoMintVault(amount : Nat) : async Nat {
    var Block = 0;
    add += 1;
    let transferResult = await icrcA.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai");
        subaccount = ?Blob.fromArray([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = amount;
      fee = ?10000;
      memo = null;
      created_at_time = ?(Prim.natToNat64(Int.abs(Time.now())) + add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) { Block := value };
      case (#Err(value)) {
        Debug.print("Transfer failed: " # debug_show (value));
      };
    };
    Block;
  };

  // New function to swap token for TACO
  public func swapTokenForTaco(token : Principal, block : Nat, minimumReceive : Nat) : async Result.Result<MintingVaultTypes.SwapResult, Text> {
    await mintingVault.swapTokenForTaco(token, block, minimumReceive);
  };

};
