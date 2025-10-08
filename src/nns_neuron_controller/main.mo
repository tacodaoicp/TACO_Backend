import Principal "mo:base/Principal";
import Blob "mo:base/Blob";
import Nat64 "mo:base/Nat64";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Time "mo:base/Time";
import Array "mo:base/Array";

import Ledger "../helper/Ledger";
import Utils "../helper/Utils";
import SHA224 "../helper/SHA224";
import CRC32 "../helper/CRC32";
import NnsTypes "../neuron_snapshot/nns_types";
import Iter "mo:base/Iter";
import Int "mo:base/Int";
import Cycles "mo:base/ExperimentalCycles";

shared (deployer) persistent actor class NnsNeuronController() = this {
  // NNS canister principals
  let NNS_GOVERNANCE_CANISTER_ID : Text = "rrkah-fqaaa-aaaaa-aaaaq-cai"; // governance
  let NNS_LEDGER_CANISTER_ID : Text = Ledger.CANISTER_ID; // ryjl3-tyaaa-aaaaa-aaaba-cai

  // Transaction fee on ICP Ledger (10_000 e8s)
  let ICP_FEE_E8S : Nat64 = 10_000;

  // Helper: build the NNS governance staking account text for a given memo (neuron creation)
  // Follows the dfx/ledger convention: 64-hex prefixed with CRC32 of the hash; uses subaccount = 8-byte big-endian memo padded to 32 bytes
  func governanceStakeAccountText(memo : Nat64) : Text {
    // Governance account is derived from the governance canister principal and a 32-byte subaccount (memo big-endian padded)
    let govPrincipal = Principal.fromText(NNS_GOVERNANCE_CANISTER_ID);
    let memoBytes = Array.init<Nat8>(32, 0);
    // write memo as big-endian u64 into the last 8 bytes of the 32-byte subaccount
    for (i in Iter.range(0, 7)) {
      let shift : Nat64 = Nat64.fromNat(7 - i) * 8;
      memoBytes[24 + i] := Nat8.fromIntWrap(Nat64.toNat((memo >> shift) & 0xff));
    };
    // Build account identifier: sha224(\x0Aaccount-id | gov_principal_blob | subaccount) then prefix crc32
    let digest = SHA224.Digest();
    digest.write([10, 97, 99, 99, 111, 117, 110, 116, 45, 105, 100]);
    let gpBlob = Principal.toBlob(govPrincipal);
    digest.write(Blob.toArray(gpBlob));
    digest.write(Array.freeze(memoBytes));
    let hash = digest.sum();
    let crc = CRC32.crc32(hash);
    let aid = Array.append<Nat8>(crc, hash);
    Utils.encode(aid)
  };

  // External actors
  let ledger : Ledger.Interface = actor (NNS_LEDGER_CANISTER_ID);
  type GovernanceActor = actor {
    claim_or_refresh_neuron_from_account : (NnsTypes.ClaimOrRefreshNeuronFromAccount) -> async NnsTypes.ClaimOrRefreshNeuronFromAccountResponse;
    manage_neuron : (NnsTypes.ManageNeuronRequest) -> async NnsTypes.ManageNeuronResponse;
    get_full_neuron : (Nat64) -> async NnsTypes.Result_2;
  };
  let governance : GovernanceActor = actor (NNS_GOVERNANCE_CANISTER_ID);
  // Create a new neuron owned by this canister by transferring ICP to the NNS governance staking account and claiming it.
/*  public shared ({ caller = _ }) func create_neuron(amount_e8s : Nat64, memo : Nat64, dissolve_delay_seconds : ?Nat64) : async { neuron_id : ?NnsTypes.NeuronId; transfer_block : ?Nat64 } {
    // Destination account for staking using memo-derived subaccount under governance canister
    let toText = governanceStakeAccountText(memo);

    // Perform ICP transfer from this canister's default subaccount
    let nowNanos = Nat64.fromNat(Int.abs(Time.now()));
    let transferArgs : Ledger.SendArgs2 = {
      memo = memo;
      amount = { e8s = amount_e8s };
      fee = { e8s = ICP_FEE_E8S };
      from_subaccount = null;
      to = toText;
      created_at_time = ?{ timestamp_nanos = nowNanos };
    };
    let transferResult = await ledger.send_dfx(transferArgs);
    // send_dfx returns a block index on success; if it traps on error, this call fails
    let blockIndex : Nat64 = transferResult;

    // Claim or refresh neuron using memo and controller = this canister's principal
    let claimReq : NnsTypes.ClaimOrRefreshNeuronFromAccount = {
      controller = ?Principal.fromActor(this);
      memo = memo;
    };
    let claimResp = await governance.claim_or_refresh_neuron_from_account(claimReq);
    let neuronId : ?NnsTypes.NeuronId = switch (claimResp.result) {
      case (?#NeuronId nid) ?nid;
      case (?#Error _) null;
      case (_) null;
    };

    // Optionally set initial dissolve delay if provided
    if (neuronId != null) {
      switch (dissolve_delay_seconds) {
        case (?dd) {
          let op : NnsTypes.Operation = #IncreaseDissolveDelay({ additional_dissolve_delay_seconds = Nat32.fromNat(Nat64.toNat(dd)) });
          let cfg : NnsTypes.Configure = { operation = ?op };
          let req : NnsTypes.ManageNeuronRequest = {
            id = neuronId;
            command = ?#Configure(cfg);
            neuron_id_or_subaccount = null;
          };
          ignore await governance.manage_neuron(req);
        };
        case (null) {};
      }
    };

    { neuron_id = neuronId; transfer_block = ?blockIndex };
  };*/

  // Add a hotkey to a neuron controlled by this canister
  public shared ({ caller }) func add_hotkey(neuron_id : NnsTypes.NeuronId, hotkey : Principal) : async { ok : Bool; err : ?NnsTypes.GovernanceError } {
    
    assert (Principal.isController(caller));

    let op : NnsTypes.Operation = #AddHotKey({ new_hot_key = ?hotkey });
    let cfg : NnsTypes.Configure = { operation = ?op };
    let req : NnsTypes.ManageNeuronRequest = {
      id = ?neuron_id;
      command = ?#Configure(cfg);
      neuron_id_or_subaccount = null;
    };
    let resp = await governance.manage_neuron(req);
    switch (resp.command) {
      case (?#Error e) { return { ok = false; err = ?e } };
      case (_) { return { ok = true; err = null } };
    };
  };

  // Read full neuron from NNS governance and forward the result (update call)
  public shared ({ caller }) func get_full_neuron(neuron_id : Nat64) : async NnsTypes.Result_2 {

    assert (Principal.isController(caller));

    await governance.get_full_neuron(neuron_id)
  };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };
}

