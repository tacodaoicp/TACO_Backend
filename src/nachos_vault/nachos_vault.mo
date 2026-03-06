import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import Map "mo:map/Map";
import Prim "mo:prim";
import Int "mo:base/Int";
import Nat64 "mo:base/Nat64";
import Nat "mo:base/Nat";
import ICRC1 "mo:icrc1/ICRC1";
import Ledger "../helper/Ledger";
import Debug "mo:base/Debug";
import Vector "mo:vector";
import Error "mo:base/Error";
import { now } = "mo:base/Time";
import { setTimer; cancelTimer } = "mo:base/Timer";
import Result "mo:base/Result";
import DAO_types "../DAO_backend/dao_types";
import TreasuryTypes "../treasury/treasury_types";
import NachosTypes "./nachos_vault_types";
import Nat8 "mo:base/Nat8";
import Float "mo:base/Float";
import Array "mo:base/Array";
import KongSwap "../swap/kong_swap";
import ICPSwap "../swap/icp_swap";
import swaptypes "../swap/swap_types";
import SpamProtection "../helper/spam_protection";
import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import Cycles "mo:base/ExperimentalCycles";
import SHA224 "../helper/SHA224";
import CRC32 "../helper/CRC32";

shared (deployer) persistent actor class NachosVaultDAO() = this {

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 1: CORE SETUP
  // ═══════════════════════════════════════════════════════════════════

  private func this_canister_id() : Principal { Principal.fromActor(this) };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 2: SYSTEM CONFIG & CANISTER IDS
  // ═══════════════════════════════════════════════════════════════════

  transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());
  transient let TREASURY_ID = canister_ids.getCanisterId(#treasury);
  transient let DAO_BACKEND_ID = canister_ids.getCanisterId(#DAO_backend);

  transient let logger = Logger.Logger();
  transient let spamGuard = SpamProtection.SpamGuard(this_canister_id());

  transient let taco_dao_sns_governance_canister_id : Principal = Principal.fromText("lhdfz-wqaaa-aaaaq-aae3q-cai");
  transient let ICPprincipalText = "ryjl3-tyaaa-aaaaa-aaaba-cai";
  transient let ICPprincipal = Principal.fromText(ICPprincipalText);

  transient let { phash; thash; nhash } = Map;
  transient let { natToNat64 } = Prim;

  // Type aliases
  type TokenDetails = DAO_types.TokenDetails;
  type TokenType = DAO_types.TokenType;
  type Allocation = DAO_types.Allocation;
  type TransferRecipient = TreasuryTypes.TransferRecipient;
  type AcceptedTokenConfig = NachosTypes.AcceptedTokenConfig;
  type MintMode = NachosTypes.MintMode;
  type MintResult = NachosTypes.MintResult;
  type TokenDeposit = NachosTypes.TokenDeposit;
  type MintRecord = NachosTypes.MintRecord;
  type FeeExemptConfig = NachosTypes.FeeExemptConfig;
  type TransferOperationType = NachosTypes.TransferOperationType;
  type TransferStatus = NachosTypes.TransferStatus;
  type VaultTransferTask = NachosTypes.VaultTransferTask;
  type DepositStatus = NachosTypes.DepositStatus;
  type ActiveDeposit = NachosTypes.ActiveDeposit;
  type DepositEntry = NachosTypes.DepositEntry;
  type BurnResult = NachosTypes.BurnResult;
  type BurnRecord = NachosTypes.BurnRecord;
  type TokenTransferResult = NachosTypes.TokenTransferResult;
  type FailedTokenTransfer = NachosTypes.FailedTokenTransfer;
  type CachedNAV = NachosTypes.CachedNAV;
  type NavSnapshot = NachosTypes.NavSnapshot;
  type NavSnapshotReason = NachosTypes.NavSnapshotReason;
  type FeeRecord = NachosTypes.FeeRecord;
  type NachosError = NachosTypes.NachosError;
  type NachosUpdateConfig = NachosTypes.NachosUpdateConfig;

  // Actor references
  transient let treasury = actor (Principal.toText(TREASURY_ID)) : actor {
    getTokenDetails : shared () -> async [(Principal, TokenDetails)];
    receiveTransferTasks : shared ([(TransferRecipient, Nat, Principal, Nat8)], Bool) -> async (Bool, ?[(Principal, Nat64)]);
    refreshAllPrices : shared () -> async Result.Result<{ tokensRefreshed : Nat; timestamp : Int; icpPriceUSD : Float }, Text>;
    refreshPricesAndGetDetails : shared () -> async Result.Result<{ tokensRefreshed : Nat; timestamp : Int; icpPriceUSD : Float; tokenDetails : [(Principal, TokenDetails)] }, Text>;
  };

  transient let dao = actor (Principal.toText(DAO_BACKEND_ID)) : actor {
    getAggregateAllocation : shared () -> async [(Principal, Nat)];
  };

  // Nachos ledger actor ref (reconstructed from stable principal)
  stable var nachosLedgerPrincipal : ?Principal = null;

  transient var nachosLedger : ?ICRC1.FullInterface = switch (nachosLedgerPrincipal) {
    case (?p) { ?( actor (Principal.toText(p)) : ICRC1.FullInterface ) };
    case null { null };
  };

  private func isMasterAdmin(caller : Principal) : Bool {
    AdminAuth.isMasterAdmin(caller, canister_ids.isKnownCanister);
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 3: STABLE STATE VARIABLES
  // ═══════════════════════════════════════════════════════════════════

  // --- Core Config ---
  stable var INITIAL_NAV_PER_TOKEN_E8S : Nat = 100_000_000; // 1.0 ICP per NACHOS
  stable var genesisComplete : Bool = false;
  stable var systemPaused : Bool = false;
  stable var mintingEnabled : Bool = true;
  stable var burningEnabled : Bool = true;

  // --- Accepted Mint Tokens ---
  stable let acceptedMintTokens = Map.new<Principal, AcceptedTokenConfig>();

  // --- Fee Config ---
  stable var mintFeeBasisPoints : Nat = 50; // 0.50%
  stable var burnFeeBasisPoints : Nat = 50; // 0.50%
  stable var minMintFeeICP : Nat = 10_000; // 0.0001 ICP
  stable var minBurnFeeICP : Nat = 10_000; // 0.0001 ICP

  // --- Min Values ---
  stable var minMintValueICP : Nat = 1_000_000; // 0.01 ICP
  stable var minBurnValueICP : Nat = 1_000_000; // 0.01 ICP

  // --- Price Config ---
  stable var MAX_PRICE_STALENESS_NS : Int = 5 * 60 * 1_000_000_000; // 5 minutes
  stable var PRICE_HISTORY_WINDOW : Int = 2 * 3600 * 1_000_000_000; // 2 hours
  stable var maxSlippageBasisPoints : Nat = 50; // 0.50%

  // --- Rate Limits ---
  stable var maxNachosBurnPer4Hours : Nat = 100_000_000_000; // 1000 NACHOS
  stable var maxMintICPWorthPer4Hours : Nat = 100_000_000_000; // 1000 ICP worth
  stable var maxMintOpsPerUser4Hours : Nat = 20;
  stable var maxBurnOpsPerUser4Hours : Nat = 20;

  // --- Circuit Breaker ---
  stable var circuitBreakerActive : Bool = false;
  stable var navDropThresholdPercent : Float = 10.0;
  stable var navDropTimeWindowNS : Nat = 3600 * 1_000_000_000; // 1 hour

  // --- Portfolio Share Config ---
  stable var portfolioShareMaxDeviationBP : Nat = 500; // 5%

  // --- Cancellation Fee ---
  stable var cancellationFeeMultiplier : Nat = 3;

  // --- Subaccounts ---
  let NachosTreasurySubaccount : Nat8 = 2; // Where users send tokens for minting
  let NachosDepositSubaccount : Nat8 = 1; // Where users send NACHOS for burning

  // --- Fee/Rate-limit Exemptions ---
  stable let feeExemptPrincipals = Map.new<Principal, FeeExemptConfig>();
  stable let rateLimitExemptPrincipals = Map.new<Principal, FeeExemptConfig>();

  // --- Block Dedup ---
  stable let blocksDone = Map.new<Text, Int>(); // blockKey -> timestamp

  // --- Active Deposits ---
  stable let activeDeposits = Map.new<Text, ActiveDeposit>(); // blockKey -> deposit

  // --- Deposit Statistics ---
  stable let depositStats = Map.new<Principal, Vector.Vector<DepositEntry>>(); // token -> entries

  // --- Transfer Queue ---
  stable let pendingTransfers = Vector.new<VaultTransferTask>();
  stable let completedTransfers = Map.new<Nat, VaultTransferTask>();
  stable let transferTaskKeys = Map.new<Text, Nat>(); // dedup key -> task id
  stable var nextTransferTaskId : Nat = 0;
  // maxCompletedTransfers removed — time-based cleanup (30 days) replaces count-based limit

  // --- Local Token Data (synced from treasury/DAO) ---
  stable let tokenDetailsMap = Map.new<Principal, TokenDetails>();
  stable let aggregateAllocation = Map.new<Principal, Nat>();

  // --- ICPSwap Pool Cache ---
  stable let ICPswapPools = Map.new<Principal, swaptypes.PoolData>();

  // --- Operation History ---
  stable let mintHistory = Vector.new<MintRecord>();
  stable let burnHistory = Vector.new<BurnRecord>();
  stable let navHistory = Vector.new<NavSnapshot>();
  stable let feeHistory = Vector.new<FeeRecord>();

  // --- Operation Counters ---
  stable var nextMintId : Nat = 0;
  stable var nextBurnId : Nat = 0;

  // --- NAV Cache ---
  stable var cachedNAV : ?CachedNAV = null;

  // --- Concurrent Operation Locks ---
  stable let operationLocks = Map.new<Principal, Int>(); // caller -> lock timestamp
  let LOCK_TIMEOUT_NS : Int = 5 * 60 * 1_000_000_000; // 5 minutes (mint/burn flows can have 5+ async calls)

  // --- Rate Limit Trackers ---
  stable let mintRateTracker = Vector.new<(Int, Nat)>(); // (timestamp, icpValue)
  stable let burnRateTracker = Vector.new<(Int, Nat)>(); // (timestamp, nachosAmount)
  stable let userMintOps = Map.new<Principal, Vector.Vector<Int>>(); // caller -> timestamps
  stable let userBurnOps = Map.new<Principal, Vector.Vector<Int>>(); // caller -> timestamps

  // --- Timer IDs ---
  transient var balanceUpdateTimerId : ?Nat = null;
  transient var poolDiscoveryTimerId : ?Nat = null;
  transient var blockCleanupTimerId : ?Nat = null;
  transient var navSnapshotTimerId : ?Nat = null;
  transient var circuitBreakerTimerId : ?Nat = null;
  transient var transferQueueTimerId : ?Nat = null;

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 4: CONSTANTS
  // ═══════════════════════════════════════════════════════════════════

  let ONE_MONTH_NS : Int = 30 * 24 * 3600 * 1_000_000_000;
  let FOUR_HOURS_NS : Int = 4 * 3600 * 1_000_000_000;
  let MAX_FEE_BP : Nat = 1000; // 10% absolute max
  let NACHOS_FEE : Nat = 10_000; // NACHOS ledger transfer fee
  let ONE_E8S : Nat = 100_000_000;

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 5: BLOCK VERIFICATION SYSTEM
  // ═══════════════════════════════════════════════════════════════════

  // --- ICP Ledger Types ---
  type ICPTimestamp = { timestamp_nanos : Nat64 };
  type ICPTokens = { e8s : Nat64 };
  type ICPOperation = {
    #Transfer : { from : Blob; to : Blob; amount : ICPTokens; fee : ICPTokens };
    #Mint : { to : Blob; amount : ICPTokens };
    #Burn : { from : Blob; amount : ICPTokens; spender : ?Blob };
    #Approve : { from : Blob; spender : Blob; allowance : ICPTokens; allowance_e8s : ?Int; fee : ICPTokens; expires_at : ?ICPTimestamp };
  };
  type ICPTransaction = {
    memo : Nat64;
    icrc1_memo : ?Blob;
    operation : ?ICPOperation;
    created_at_time : ICPTimestamp;
  };
  type ICPBlock = {
    parent_hash : ?Blob;
    transaction : ICPTransaction;
    timestamp : ICPTimestamp;
  };
  type ArchivedBlocksRange = {
    start : Nat64;
    length : Nat64;
    callback : shared query { start : Nat64; length : Nat64 } -> async { blocks : [ICPBlock] };
  };
  type QueryBlocksResponse = {
    chain_length : Nat64;
    certificate : ?Blob;
    blocks : [ICPBlock];
    first_block_index : Nat64;
    archived_blocks : [ArchivedBlocksRange];
  };

  // --- ICRC-3 Value Type ---
  type ICRC3Value = {
    #Nat : Nat;
    #Int : Int;
    #Text : Text;
    #Blob : Blob;
    #Array : [ICRC3Value];
    #Map : [(Text, ICRC3Value)];
  };

  // --- Account ID Computation ---
  private func computeAccountIdentifier(owner : Principal, subaccount : ?Blob) : Blob {
    let digest = SHA224.Digest();
    digest.write([10, 97, 99, 99, 111, 117, 110, 116, 45, 105, 100] : [Nat8]); // b"\x0Aaccount-id"
    digest.write(Blob.toArray(Principal.toBlob(owner)));
    switch (subaccount) {
      case (?sub) { digest.write(Blob.toArray(sub)) };
      case null { digest.write(Array.freeze<Nat8>(Array.init<Nat8>(32, 0 : Nat8))) };
    };
    let hash = digest.sum();
    let crc = CRC32.crc32(hash);
    Blob.fromArray(Array.append<Nat8>(crc, hash));
  };

  private func subaccountByteToBlob(b : Nat8) : Blob {
    Blob.fromArray([b, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
  };

  // --- ICRC-3 Value Parsing Helpers ---
  private func lookupInMap(map : [(Text, ICRC3Value)], key : Text) : ?ICRC3Value {
    for ((k, v) in map.vals()) { if (k == key) return ?v };
    null;
  };

  private func extractNat(v : ICRC3Value) : ?Nat {
    switch (v) { case (#Nat(n)) { ?n }; case _ { null } };
  };

  private func extractText(v : ICRC3Value) : ?Text {
    switch (v) { case (#Text(t)) { ?t }; case _ { null } };
  };

  private func extractBlob(v : ICRC3Value) : ?Blob {
    switch (v) { case (#Blob(b)) { ?b }; case _ { null } };
  };

  private func extractMap(v : ICRC3Value) : ?[(Text, ICRC3Value)] {
    switch (v) { case (#Map(m)) { ?m }; case _ { null } };
  };

  private func isAllZeros(blob : Blob) : Bool {
    for (b in blob.vals()) { if (b != 0) return false };
    true;
  };

  private func matchSubaccount(actual : ?Blob, expected : ?[Nat8]) : Bool {
    switch (actual, expected) {
      case (null, null) { true };
      case (null, ?exp) { isAllZeros(Blob.fromArray(exp)) };
      case (?act, null) { isAllZeros(act) };
      case (?act, ?exp) { Blob.toArray(act) == exp };
    };
  };

  // --- ICP Block Verifier ---
  private func verifyICPBlock(blockNumber : Nat, expectedFrom : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
    let icpLedger : actor {
      query_blocks : shared query { start : Nat64; length : Nat64 } -> async QueryBlocksResponse;
    } = actor (ICPprincipalText);

    let blockIdx = natToNat64(blockNumber);
    try {
      let response = await icpLedger.query_blocks({ start = blockIdx; length = 1 });

      var foundBlock : ?ICPBlock = null;

      if (response.blocks.size() > 0) {
        foundBlock := ?response.blocks[0];
      } else {
        // Check archived blocks
        for (archive in response.archived_blocks.vals()) {
          if (blockIdx >= archive.start and blockIdx < archive.start + archive.length) {
            try {
              let archiveResult = await archive.callback({ start = blockIdx; length = 1 });
              if (archiveResult.blocks.size() > 0) {
                foundBlock := ?archiveResult.blocks[0];
              };
            } catch (_) {};
          };
        };
      };

      switch (foundBlock) {
        case null { #err("Block not found") };
        case (?block) {
          switch (block.transaction.operation) {
            case (?#Transfer({ from; to; amount })) {
              let expectedFromAid = computeAccountIdentifier(expectedFrom, null);
              let expectedToAid = computeAccountIdentifier(this_canister_id(), ?subaccountByteToBlob(expectedToSubaccount));

              if (from != expectedFromAid) return #err("Sender mismatch");
              if (to != expectedToAid) return #err("Recipient mismatch");

              #ok({ amount = Nat64.toNat(amount.e8s); from = expectedFrom });
            };
            case _ { #err("Not a transfer operation") };
          };
        };
      };
    } catch (e) {
      #err("ICP block query failed: " # Error.message(e));
    };
  };

  // --- ICRC-1/2 Block Verifier ---
  private func verifyICRC1Block(tokenPrincipal : Principal, blockNumber : Nat, expectedFrom : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
    let token : actor {
      get_transactions : shared query { start : Nat; length : Nat } -> async {
        first_index : Nat;
        log_length : Nat;
        transactions : [{
          burn : ?{ from : { owner : Principal; subaccount : ?[Nat8] }; amount : Nat };
          kind : Text;
          mint : ?{ to : { owner : Principal; subaccount : ?[Nat8] }; amount : Nat };
          timestamp : Nat64;
          transfer : ?{
            to : { owner : Principal; subaccount : ?[Nat8] };
            fee : ?Nat;
            from : { owner : Principal; subaccount : ?[Nat8] };
            amount : Nat;
          };
        }];
        archived_transactions : [{
          start : Nat;
          length : Nat;
          callback : shared query { start : Nat; length : Nat } -> async {
            transactions : [{
              burn : ?{ from : { owner : Principal; subaccount : ?[Nat8] }; amount : Nat };
              kind : Text;
              mint : ?{ to : { owner : Principal; subaccount : ?[Nat8] }; amount : Nat };
              timestamp : Nat64;
              transfer : ?{
                to : { owner : Principal; subaccount : ?[Nat8] };
                fee : ?Nat;
                from : { owner : Principal; subaccount : ?[Nat8] };
                amount : Nat;
              };
            }];
          };
        }];
      };
    } = actor (Principal.toText(tokenPrincipal));

    try {
      let response = await token.get_transactions({ start = blockNumber; length = 1 });
      let expectedToSub : [Nat8] = [expectedToSubaccount, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

      // Try direct transactions first
      if (response.transactions.size() > 0) {
        let tx = response.transactions[0];
        switch (tx.transfer) {
          case (?xfer) {
            if (xfer.from.owner != expectedFrom) return #err("Sender mismatch");
            // Validate sender subaccount is default (null or all-zeros)
            switch (xfer.from.subaccount) {
              case (?sub) { if (not isAllZeros(Blob.fromArray(sub))) return #err("Sender used non-default subaccount") };
              case null {};
            };
            if (xfer.to.owner != this_canister_id()) return #err("Recipient owner mismatch");
            if (not matchSubaccount(?Blob.fromArray(expectedToSub), xfer.to.subaccount)) return #err("Recipient subaccount mismatch");
            return #ok({ amount = xfer.amount; from = expectedFrom });
          };
          case null { return #err("Not a transfer transaction") };
        };
      };

      // Check archived transactions
      for (archive in response.archived_transactions.vals()) {
        if (blockNumber >= archive.start and blockNumber < archive.start + archive.length) {
          try {
            let archiveResult = await archive.callback({ start = blockNumber; length = 1 });
            if (archiveResult.transactions.size() > 0) {
              let tx = archiveResult.transactions[0];
              switch (tx.transfer) {
                case (?xfer) {
                  if (xfer.from.owner != expectedFrom) return #err("Sender mismatch (archive)");
                  switch (xfer.from.subaccount) {
                    case (?sub) { if (not isAllZeros(Blob.fromArray(sub))) return #err("Sender used non-default subaccount (archive)") };
                    case null {};
                  };
                  if (xfer.to.owner != this_canister_id()) return #err("Recipient owner mismatch (archive)");
                  if (not matchSubaccount(?Blob.fromArray(expectedToSub), xfer.to.subaccount)) return #err("Recipient subaccount mismatch (archive)");
                  return #ok({ amount = xfer.amount; from = expectedFrom });
                };
                case null {};
              };
            };
          } catch (_) {};
        };
      };

      #err("Transaction not found");
    } catch (e) {
      #err("ICRC-1 block query failed: " # Error.message(e));
    };
  };

  // --- ICRC-3 Block Verifier ---
  private func verifyICRC3Block(tokenPrincipal : Principal, blockNumber : Nat, expectedFrom : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
    let token : actor {
      icrc3_get_blocks : shared query [{ start : Nat; length : Nat }] -> async {
        log_length : Nat;
        blocks : [{ id : Nat; block : ICRC3Value }];
        archived_blocks : [{
          args : [{ start : Nat; length : Nat }];
          callback : shared query [{ start : Nat; length : Nat }] -> async {
            log_length : Nat;
            blocks : [{ id : Nat; block : ICRC3Value }];
            archived_blocks : [None];
          };
        }];
      };
    } = actor (Principal.toText(tokenPrincipal));

    try {
      let response = await token.icrc3_get_blocks([{ start = blockNumber; length = 1 }]);

      var blockValue : ?ICRC3Value = null;

      if (response.blocks.size() > 0) {
        blockValue := ?response.blocks[0].block;
      } else {
        for (archive in response.archived_blocks.vals()) {
          try {
            let archResult = await archive.callback([{ start = blockNumber; length = 1 }]);
            if (archResult.blocks.size() > 0) {
              blockValue := ?archResult.blocks[0].block;
            };
          } catch (_) {};
        };
      };

      switch (blockValue) {
        case null { #err("ICRC-3 block not found") };
        case (?val) {
          switch (extractMap(val)) {
            case null { #err("Block is not a Map") };
            case (?map) {
              // Check btype
              switch (lookupInMap(map, "btype")) {
                case (?btypeVal) {
                  switch (extractText(btypeVal)) {
                    case (?btype) {
                      if (btype != "1xfer" and btype != "2xfer") return #err("Not a transfer block: " # btype);
                    };
                    case null { return #err("btype is not Text") };
                  };
                };
                case null { return #err("No btype field") };
              };

              // Extract tx map
              let txMap = switch (lookupInMap(map, "tx")) {
                case (?txVal) {
                  switch (extractMap(txVal)) {
                    case (?m) { m };
                    case null { return #err("tx is not a Map") };
                  };
                };
                case null { return #err("No tx field") };
              };

              // Extract amount
              let amount = switch (lookupInMap(txMap, "amt")) {
                case (?amtVal) {
                  switch (extractNat(amtVal)) {
                    case (?n) { n };
                    case null { return #err("amt is not Nat") };
                  };
                };
                case null { return #err("No amt field") };
              };

              // Extract and verify sender
              let fromOwner = switch (lookupInMap(txMap, "from")) {
                case (?fromVal) {
                  switch (extractMap(fromVal)) {
                    case (?fromMap) {
                      switch (lookupInMap(fromMap, "owner")) {
                        case (?ownerVal) {
                          switch (extractBlob(ownerVal)) {
                            case (?b) { Principal.fromBlob(b) };
                            case null { return #err("from.owner not Blob") };
                          };
                        };
                        case null { return #err("No from.owner") };
                      };
                    };
                    case null { return #err("from is not Map") };
                  };
                };
                case null { return #err("No from field") };
              };

              if (fromOwner != expectedFrom) return #err("Sender mismatch");

              // Validate sender subaccount is default (null or all-zeros)
              switch (lookupInMap(txMap, "from")) {
                case (?fromVal) {
                  switch (extractMap(fromVal)) {
                    case (?fromMap) {
                      switch (lookupInMap(fromMap, "subaccount")) {
                        case (?subVal) {
                          switch (extractBlob(subVal)) {
                            case (?sub) {
                              if (not isAllZeros(sub)) return #err("Sender used non-default subaccount");
                            };
                            case null {};
                          };
                        };
                        case null {};
                      };
                    };
                    case null {};
                  };
                };
                case null {};
              };

              // Extract and verify recipient
              let toOwner = switch (lookupInMap(txMap, "to")) {
                case (?toVal) {
                  switch (extractMap(toVal)) {
                    case (?toMap) {
                      switch (lookupInMap(toMap, "owner")) {
                        case (?ownerVal) {
                          switch (extractBlob(ownerVal)) {
                            case (?b) { Principal.fromBlob(b) };
                            case null { return #err("to.owner not Blob") };
                          };
                        };
                        case null { return #err("No to.owner") };
                      };
                    };
                    case null { return #err("to is not Map") };
                  };
                };
                case null { return #err("No to field") };
              };

              if (toOwner != this_canister_id()) return #err("Recipient mismatch");

              // Verify subaccount
              let expectedToSub : Blob = subaccountByteToBlob(expectedToSubaccount);
              switch (lookupInMap(txMap, "to")) {
                case (?toVal) {
                  switch (extractMap(toVal)) {
                    case (?toMap) {
                      switch (lookupInMap(toMap, "subaccount")) {
                        case (?subVal) {
                          switch (extractBlob(subVal)) {
                            case (?sub) {
                              if (sub != expectedToSub) return #err("Recipient subaccount mismatch");
                            };
                            case null { return #err("subaccount not Blob") };
                          };
                        };
                        case null {
                          if (expectedToSubaccount != 0) return #err("Expected non-zero subaccount but none found");
                        };
                      };
                    };
                    case null {};
                  };
                };
                case null {};
              };

              #ok({ amount; from = expectedFrom });
            };
          };
        };
      };
    } catch (e) {
      #err("ICRC-3 block query failed: " # Error.message(e));
    };
  };

  // --- Unified Block Verification Dispatcher ---
  private func verifyBlock(tokenPrincipal : Principal, blockNumber : Nat, expectedFrom : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
    let tokenType : ?TokenType = switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
      case (?details) { ?details.tokenType };
      case null {
        // ICP special case
        if (tokenPrincipal == ICPprincipal) { ?#ICP } else { null };
      };
    };

    switch (tokenType) {
      case null { #err("Unknown token") };
      case (?#ICP) { await verifyICPBlock(blockNumber, expectedFrom, expectedToSubaccount) };
      case (?#ICRC12) { await verifyICRC1Block(tokenPrincipal, blockNumber, expectedFrom, expectedToSubaccount) };
      case (?#ICRC3) { await verifyICRC3Block(tokenPrincipal, blockNumber, expectedFrom, expectedToSubaccount) };
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 6: LOCAL TRANSFER QUEUE
  // ═══════════════════════════════════════════════════════════════════

  private func operationTypeToText(opType : TransferOperationType) : Text {
    switch (opType) {
      case (#MintReturn) { "MintReturn" };
      case (#BurnPayout) { "BurnPayout" };
      case (#ExcessReturn) { "ExcessReturn" };
      case (#CancelReturn) { "CancelReturn" };
      case (#Recovery) { "Recovery" };
    };
  };

  private func createTransferTask(
    caller : Principal,
    recipient : TransferRecipient,
    amount : Nat,
    tokenPrincipal : Principal,
    fromSubaccount : Nat8,
    opType : TransferOperationType,
    opId : Nat,
  ) : Nat {
    let dedupKey = Principal.toText(tokenPrincipal) # ":" # operationTypeToText(opType) # ":" # Nat.toText(opId) # ":" # Nat.toText(amount);

    // Check for duplicate
    switch (Map.get(transferTaskKeys, thash, dedupKey)) {
      case (?existingId) { return existingId };
      case null {};
    };

    let taskId = nextTransferTaskId;
    nextTransferTaskId += 1;

    let task : VaultTransferTask = {
      id = taskId;
      caller;
      recipient;
      amount;
      tokenPrincipal;
      fromSubaccount;
      operationType = opType;
      operationId = opId;
      status = #Pending;
      createdAt = now();
      updatedAt = now();
      retryCount = 0;
      actualAmountSent = null;
      blockIndex = null;
    };

    Vector.add(pendingTransfers, task);
    Map.set(transferTaskKeys, thash, dedupKey, taskId);

    logger.info("TRANSFER_QUEUE", "Created task #" # Nat.toText(taskId) # " type=" # operationTypeToText(opType) # " amount=" # Nat.toText(amount), "createTransferTask");
    taskId;
  };

  private func processTransferQueue() : async () {
    let pending = Vector.new<(TransferRecipient, Nat, Principal, Nat8)>();
    let taskIndices = Vector.new<Nat>();

    // Collect pending and failed-retryable tasks
    var i = 0;
    label collect while (i < Vector.size(pendingTransfers)) {
      let task = Vector.get(pendingTransfers, i);
      switch (task.status) {
        case (#Pending or #Failed(_)) {
          if (task.retryCount < 5) {
            Vector.add(pending, (task.recipient, task.amount, task.tokenPrincipal, task.fromSubaccount));
            Vector.add(taskIndices, i);
            // Mark as Sent BEFORE await (safe async)
            Vector.put(pendingTransfers, i, { task with status = #Sent; updatedAt = now(); retryCount = task.retryCount + 1 });
          } else if (task.retryCount == 5) {
            // Log once on exhaustion, bump to 6 to prevent repeated logging
            logger.error("TRANSFER_QUEUE", "Task #" # Nat.toText(task.id) # " EXHAUSTED all retries. type=" # operationTypeToText(task.operationType) # " amount=" # Nat.toText(task.amount) # " token=" # Principal.toText(task.tokenPrincipal), "processTransferQueue");
            Vector.put(pendingTransfers, i, { task with retryCount = 6 });
          };
        };
        case _ {};
      };
      i += 1;
    };

    if (Vector.size(pending) == 0) return;

    let batch = Vector.toArray(pending);

    try {
      let (success, results) = await treasury.receiveTransferTasks(batch, true);

      if (success) {
        switch (results) {
          case (?blockResults) {
            var idx = 0;
            for (result in blockResults.vals()) {
              if (idx < Vector.size(taskIndices)) {
                let taskIdx = Vector.get(taskIndices, idx);
                let task = Vector.get(pendingTransfers, taskIdx);
                let blockIndex = result.1;
                if (blockIndex > 0) {
                  Vector.put(pendingTransfers, taskIdx, { task with status = #Confirmed(blockIndex); updatedAt = now(); blockIndex = ?blockIndex; actualAmountSent = ?task.amount });
                } else {
                  Vector.put(pendingTransfers, taskIdx, { task with status = #Failed("Zero block index returned"); updatedAt = now() });
                };
              };
              idx += 1;
            };
          };
          case null {
            // Treasury returned success but no block indices — mark as failed for retry
            logger.warn("TRANSFER_QUEUE", "Treasury returned (true, null) — no block indices, marking tasks as failed for retry", "processTransferQueue");
            for (taskIdx in Vector.vals(taskIndices)) {
              let task = Vector.get(pendingTransfers, taskIdx);
              Vector.put(pendingTransfers, taskIdx, { task with status = #Failed("Treasury returned no block indices"); updatedAt = now() });
            };
          };
        };
      } else {
        // Mark all as failed
        for (taskIdx in Vector.vals(taskIndices)) {
          let task = Vector.get(pendingTransfers, taskIdx);
          Vector.put(pendingTransfers, taskIdx, { task with status = #Failed("Treasury rejected batch"); updatedAt = now() });
        };
      };
    } catch (e) {
      for (taskIdx in Vector.vals(taskIndices)) {
        let task = Vector.get(pendingTransfers, taskIdx);
        Vector.put(pendingTransfers, taskIdx, { task with status = #Failed(Error.message(e)); updatedAt = now() });
      };
    };

    // Move confirmed to completed
    cleanupConfirmedTransfers();
  };

  private func cleanupConfirmedTransfers() {
    var i = 0;
    while (i < Vector.size(pendingTransfers)) {
      let task = Vector.get(pendingTransfers, i);
      switch (task.status) {
        case (#Confirmed(_)) {
          Map.set(completedTransfers, nhash, task.id, task);
          // Remove from pending by swapping with last
          let lastIdx = Vector.size(pendingTransfers) - 1;
          if (i < lastIdx) {
            Vector.put(pendingTransfers, i, Vector.get(pendingTransfers, lastIdx));
          };
          ignore Vector.removeLast(pendingTransfers);
          // Don't increment i since we swapped
        };
        case _ { i += 1 };
      };
    };

    // Time-based cleanup of old completed transfers (30 days)
    let cutoffTime = now() - (30 * 24 * 3600 * 1_000_000_000);
    for ((id, task) in Map.entries(completedTransfers)) {
      if (task.updatedAt < cutoffTime) {
        ignore Map.remove(completedTransfers, nhash, id);
      };
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 7: DEPOSIT TRACKING & STATISTICS
  // ═══════════════════════════════════════════════════════════════════

  private func makeBlockKey(tokenPrincipal : Principal, blockNumber : Nat) : Text {
    Principal.toText(tokenPrincipal) # ":" # Nat.toText(blockNumber);
  };

  private func recordDeposit(blockKey : Text, caller : Principal, tokenPrincipal : Principal, amount : Nat, blockNumber : Nat) {
    let deposit : ActiveDeposit = {
      blockKey;
      caller;
      tokenPrincipal;
      amount;
      blockNumber;
      timestamp = now();
      status = #Verified;
      mintBurnId = null;
      cancellationTxId = null;
    };
    Map.set(activeDeposits, thash, blockKey, deposit);
  };

  private func consumeDeposit(blockKey : Text, mintBurnId : Nat) {
    switch (Map.get(activeDeposits, thash, blockKey)) {
      case (?deposit) {
        Map.set(activeDeposits, thash, blockKey, { deposit with status = #Consumed; mintBurnId = ?mintBurnId });
      };
      case null {};
    };
  };

  // Cancel deposit AND create refund task atomically — prevents double-refund via cancelDeposit()
  private func cancelDepositAndRefund(blockKey : Text, caller : Principal, refundAmount : Nat, tokenPrincipal : Principal, fromSubaccount : Nat8, opType : TransferOperationType, opId : Nat) : Nat {
    switch (Map.get(activeDeposits, thash, blockKey)) {
      case (?deposit) { Map.set(activeDeposits, thash, blockKey, { deposit with status = #Cancelled }) };
      case null {};
    };
    createTransferTask(caller, #principal(caller), refundAmount, tokenPrincipal, fromSubaccount, opType, opId);
  };

  public shared ({ caller }) func cancelDeposit(tokenPrincipal : Principal, blockNumber : Nat) : async Result.Result<{ refundTaskId : Nat }, NachosError> {
    let blockKey = makeBlockKey(tokenPrincipal, blockNumber);

    let deposit = switch (Map.get(activeDeposits, thash, blockKey)) {
      case (?d) { d };
      case null { return #err(#DepositNotFound) };
    };

    if (deposit.caller != caller) return #err(#NotDepositor);

    switch (deposit.status) {
      case (#Cancelled) { return #err(#DepositAlreadyCancelled) };
      case (#Consumed) { return #err(#DepositAlreadyConsumed) };
      case (#Expired) { return #err(#DepositExpired) };
      case _ {};
    };

    // Belt-and-suspenders: check if a refund task already exists for this block
    let refundDedupKey = Principal.toText(tokenPrincipal) # ":MintReturn:" # Nat.toText(blockNumber);
    let cancelDedupKey = Principal.toText(tokenPrincipal) # ":CancelReturn:" # Nat.toText(blockNumber);
    if (Map.has(transferTaskKeys, thash, refundDedupKey) or Map.has(transferTaskKeys, thash, cancelDedupKey)) {
      return #err(#DepositAlreadyCancelled);
    };

    // Get token fee
    let tokenFee = switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
      case (?details) { details.tokenTransferFee };
      case null { 10_000 };
    };

    let cancellationFee = tokenFee * cancellationFeeMultiplier;
    if (deposit.amount <= cancellationFee) return #err(#InsufficientBalance);

    let refundAmount = deposit.amount - cancellationFee;

    // Mark as cancelled BEFORE await (safe async)
    Map.set(activeDeposits, thash, blockKey, { deposit with status = #Cancelled });

    let taskId = createTransferTask(
      caller,
      #principal(caller),
      refundAmount,
      tokenPrincipal,
      NachosTreasurySubaccount,
      #CancelReturn,
      blockNumber,
    );

    logger.info("DEPOSIT", "Cancelled deposit blockKey=" # blockKey # " refund=" # Nat.toText(refundAmount), "cancelDeposit");
    #ok({ refundTaskId = taskId });
  };

  private func recordDepositStat(tokenPrincipal : Principal, amount : Nat, caller : Principal, blockNumber : Nat, valueICP : Nat) {
    let entry : DepositEntry = {
      timestamp = now();
      amount;
      caller;
      blockNumber;
      valueICP;
    };
    switch (Map.get(depositStats, phash, tokenPrincipal)) {
      case (?vec) { Vector.add(vec, entry) };
      case null {
        let vec = Vector.new<DepositEntry>();
        Vector.add(vec, entry);
        Map.set(depositStats, phash, tokenPrincipal, vec);
      };
    };
  };

  private func cleanupExpiredDeposits() {
    let expiryThreshold = now() - ONE_MONTH_NS;
    let keysToRemove = Vector.new<Text>();
    for ((key, deposit) in Map.entries(activeDeposits)) {
      if (deposit.timestamp < expiryThreshold) {
        switch (deposit.status) {
          case (#Verified or #Processing) {
            Map.set(activeDeposits, thash, key, { deposit with status = #Expired });
          };
          case (#Consumed or #Cancelled or #Expired) {
            Vector.add(keysToRemove, key);
          };
        };
      };
    };
    for (key in Vector.vals(keysToRemove)) {
      ignore Map.remove(activeDeposits, thash, key);
    };
  };

  private func cleanupDepositStats() {
    let cutoff = now() - ONE_MONTH_NS;
    for ((token, vec) in Map.entries(depositStats)) {
      let newVec = Vector.new<DepositEntry>();
      for (entry in Vector.vals(vec)) {
        if (entry.timestamp >= cutoff) Vector.add(newVec, entry);
      };
      Map.set(depositStats, phash, token, newVec);
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 8: NAV CALCULATION & PRICE DISCOVERY
  // ═══════════════════════════════════════════════════════════════════

  private func calculatePortfolioValueICP() : Nat {
    var totalValueE8s : Nat = 0;
    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and details.balance > 0 and details.priceInICP > 0) {
        let tokenValue = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        totalValueE8s += tokenValue;
      };
    };
    totalValueE8s;
  };

  private func calculateNAV() : async Result.Result<CachedNAV, Text> {
    let ledger = switch (nachosLedger) {
      case (?l) { l };
      case null { return #err("Nachos ledger not set") };
    };

    try {
      let supply = await ledger.icrc1_total_supply();
      let portfolioValue = calculatePortfolioValueICP();

      let navPerToken : Nat = if (supply == 0) {
        INITIAL_NAV_PER_TOKEN_E8S;
      } else {
        (portfolioValue * ONE_E8S) / supply;
      };

      let nav : CachedNAV = {
        navPerTokenE8s = navPerToken;
        portfolioValueICP = portfolioValue;
        nachosSupply = supply;
        timestamp = now();
      };

      cachedNAV := ?nav;
      #ok(nav);
    } catch (e) {
      #err("NAV calculation failed: " # Error.message(e));
    };
  };

  private func arePricesFresh() : Bool {
    let threshold = now() - MAX_PRICE_STALENESS_NS;
    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and not details.isPaused and token != ICPprincipal) {
        if (details.lastTimeSynced < threshold) return false;
      };
    };
    true;
  };

  private func getHistoricalLowPrice(token : Principal) : Nat {
    let cutoff = now() - PRICE_HISTORY_WINDOW;
    var lowest : Nat = 0;
    switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) {
        lowest := details.priceInICP;
        for (pp in details.pastPrices.vals()) {
          if (pp.time >= cutoff and pp.icpPrice > 0) {
            if (lowest == 0 or pp.icpPrice < lowest) {
              lowest := pp.icpPrice;
            };
          };
        };
      };
      case null {};
    };
    lowest;
  };

  // Find ICPSwap pool for a token paired with ICP
  private func findICPSwapPoolForToken(token : Principal) : ?Principal {
    let tokenText = Principal.toText(token);
    for ((_, pool) in Map.entries(ICPswapPools)) {
      if ((pool.token0.address == tokenText and pool.token1.address == ICPprincipalText) or (pool.token1.address == tokenText and pool.token0.address == ICPprincipalText)) {
        return ?pool.canisterId;
      };
    };
    null;
  };

  // Conservative price: uses treasury-cached prices (already refreshed by performSharedPreChecks
  // via treasury.refreshAllPrices which queries both KongSwap + ICPSwap with variance-aware selection)
  // plus the vault's own 2-hour historical low. Takes the minimum for deposit safety.
  private func getConservativePrice(token : Principal) : Nat {
    let treasuryPrice = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) { details.priceInICP };
      case null { return 0 };
    };

    let historicalLow = getHistoricalLowPrice(token);

    // Return the LOWEST valid price (conservative for deposits)
    if (historicalLow > 0 and historicalLow < treasuryPrice) historicalLow
    else treasuryPrice;
  };

  private func recordNavSnapshot(reason : NavSnapshotReason) {
    switch (cachedNAV) {
      case (?nav) {
        Vector.add(navHistory, {
          timestamp = now();
          navPerTokenE8s = nav.navPerTokenE8s;
          portfolioValueICP = nav.portfolioValueICP;
          nachosSupply = nav.nachosSupply;
          reason;
        });
        // Trim to last 10000
        while (Vector.size(navHistory) > 10000) {
          ignore Vector.removeLast(navHistory);
        };
      };
      case null {};
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 9: SHARED PRE-CHECKS & RATE LIMITING
  // ═══════════════════════════════════════════════════════════════════

  private func acquireLock(caller : Principal) : Result.Result<(), NachosError> {
    switch (Map.get(operationLocks, phash, caller)) {
      case (?lockTime) {
        if (now() - lockTime < LOCK_TIMEOUT_NS) {
          return #err(#OperationInProgress);
        };
      };
      case null {};
    };
    Map.set(operationLocks, phash, caller, now());
    #ok(());
  };

  private func releaseLock(caller : Principal) {
    ignore Map.remove(operationLocks, phash, caller);
  };

  private func isRateLimitExempt(caller : Principal) : Bool {
    switch (Map.get(rateLimitExemptPrincipals, phash, caller)) {
      case (?config) { config.enabled };
      case null { false };
    };
  };

  private func isFeeExempt(caller : Principal) : Bool {
    switch (Map.get(feeExemptPrincipals, phash, caller)) {
      case (?config) { config.enabled };
      case null { false };
    };
  };

  private func calculateFee(caller : Principal, valueICP : Nat, feeBP : Nat, minFee : Nat) : Nat {
    if (isFeeExempt(caller)) return 0;
    let fee = (valueICP * feeBP) / 10_000;
    if (fee < minFee) minFee else fee;
  };

  private func cleanRateLimitWindows() {
    let cutoff = now() - FOUR_HOURS_NS;

    // Clean global mint tracker
    let newMintTracker = Vector.new<(Int, Nat)>();
    for (entry in Vector.vals(mintRateTracker)) {
      if (entry.0 >= cutoff) Vector.add(newMintTracker, entry);
    };
    // Replace contents
    while (Vector.size(mintRateTracker) > 0) { ignore Vector.removeLast(mintRateTracker) };
    for (entry in Vector.vals(newMintTracker)) { Vector.add(mintRateTracker, entry) };

    // Clean global burn tracker
    let newBurnTracker = Vector.new<(Int, Nat)>();
    for (entry in Vector.vals(burnRateTracker)) {
      if (entry.0 >= cutoff) Vector.add(newBurnTracker, entry);
    };
    while (Vector.size(burnRateTracker) > 0) { ignore Vector.removeLast(burnRateTracker) };
    for (entry in Vector.vals(newBurnTracker)) { Vector.add(burnRateTracker, entry) };
  };

  // Combined check + record: records optimistically BEFORE awaits to prevent
  // global rate limit bypass by concurrent users interleaving at await points.
  // If the mint later fails, the rate slot stays consumed (conservative but safe).
  private func checkAndRecordMintRateLimit(caller : Principal, valueICP : Nat) : Result.Result<(), NachosError> {
    if (isRateLimitExempt(caller)) return #ok(());

    cleanRateLimitWindows();

    // Global ICP value limit
    var totalMintValue : Nat = 0;
    for (entry in Vector.vals(mintRateTracker)) {
      totalMintValue += entry.1;
    };
    if (totalMintValue + valueICP > maxMintICPWorthPer4Hours) {
      return #err(#MintLimitExceeded({ maxPer4Hours = maxMintICPWorthPer4Hours; recentMints = totalMintValue; requested = valueICP }));
    };

    // Per-user ops limit
    let cutoff = now() - FOUR_HOURS_NS;
    switch (Map.get(userMintOps, phash, caller)) {
      case (?ops) {
        var recentCount : Nat = 0;
        for (ts in Vector.vals(ops)) {
          if (ts >= cutoff) recentCount += 1;
        };
        if (recentCount >= maxMintOpsPerUser4Hours) {
          return #err(#RateLimitExceeded);
        };
      };
      case null {};
    };

    // Record immediately (before any await points)
    Vector.add(mintRateTracker, (now(), valueICP));
    switch (Map.get(userMintOps, phash, caller)) {
      case (?ops) { Vector.add(ops, now()) };
      case null {
        let ops = Vector.new<Int>();
        Vector.add(ops, now());
        Map.set(userMintOps, phash, caller, ops);
      };
    };

    #ok(());
  };

  // Combined check + record for burn rate limits (same optimistic pattern).
  private func checkAndRecordBurnRateLimit(caller : Principal, nachosAmount : Nat) : Result.Result<(), NachosError> {
    if (isRateLimitExempt(caller)) return #ok(());

    cleanRateLimitWindows();

    // Global nachos burn limit
    var totalBurned : Nat = 0;
    for (entry in Vector.vals(burnRateTracker)) {
      totalBurned += entry.1;
    };
    if (totalBurned + nachosAmount > maxNachosBurnPer4Hours) {
      return #err(#BurnLimitExceeded({ maxPer4Hours = maxNachosBurnPer4Hours; recentBurns = totalBurned; requested = nachosAmount }));
    };

    // Per-user ops limit
    let cutoff = now() - FOUR_HOURS_NS;
    switch (Map.get(userBurnOps, phash, caller)) {
      case (?ops) {
        var recentCount : Nat = 0;
        for (ts in Vector.vals(ops)) {
          if (ts >= cutoff) recentCount += 1;
        };
        if (recentCount >= maxBurnOpsPerUser4Hours) {
          return #err(#RateLimitExceeded);
        };
      };
      case null {};
    };

    // Record immediately (before any await points)
    Vector.add(burnRateTracker, (now(), nachosAmount));
    switch (Map.get(userBurnOps, phash, caller)) {
      case (?ops) { Vector.add(ops, now()) };
      case null {
        let ops = Vector.new<Int>();
        Vector.add(ops, now());
        Map.set(userBurnOps, phash, caller, ops);
      };
    };

    #ok(());
  };

  private func performSharedPreChecks(caller : Principal, isMint : Bool) : async Result.Result<(), NachosError> {
    // Spam protection
    let spamLevel = spamGuard.isAllowed(caller, ?taco_dao_sns_governance_canister_id);
    if (spamLevel >= 3) return #err(#NotAuthorized);

    // System state checks
    if (systemPaused) return #err(#SystemPaused);
    if (isMint and not mintingEnabled) return #err(#MintingDisabled);
    if (not isMint and not burningEnabled) return #err(#BurningDisabled);
    if (not genesisComplete) return #err(#GenesisNotComplete);
    if (circuitBreakerActive) return #err(#CircuitBreakerActive);

    // Acquire lock
    switch (acquireLock(caller)) {
      case (#err(e)) { return #err(e) };
      case (#ok(())) {};
    };

    // Force price refresh + fetch token details in one call
    try {
      switch (await treasury.refreshPricesAndGetDetails()) {
        case (#ok(result)) {
          for ((token, detail) in result.tokenDetails.vals()) {
            Map.set(tokenDetailsMap, phash, token, detail);
          };
        };
        case (#err(msg)) {
          logger.warn("PRE_CHECK", "Price refresh failed: " # msg, "performSharedPreChecks");
        };
      };
    } catch (e) {
      logger.warn("PRE_CHECK", "Price refresh call failed: " # Error.message(e), "performSharedPreChecks");
    };

    // Check price staleness
    if (not arePricesFresh()) {
      releaseLock(caller);
      return #err(#PriceStale);
    };

    #ok(());
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 10: MINTING FLOWS
  // ═══════════════════════════════════════════════════════════════════

  // Helper: Mint NACHOS tokens to a user (vault IS the minting account)
  private func mintNachosTokens(to : Principal, amount : Nat) : async Result.Result<Nat, Text> {
    let ledger = switch (nachosLedger) {
      case (?l) { l };
      case null { return #err("Nachos ledger not set") };
    };

    try {
      let result = await ledger.icrc1_transfer({
        from_subaccount = null; // null = minting account = vault
        to = { owner = to; subaccount = null };
        amount;
        fee = null;
        memo = null;
        created_at_time = null;
      });
      switch (result) {
        case (#Ok(txId)) { #ok(txId) };
        case (#Err(e)) { #err("NACHOS mint failed: " # debug_show (e)) };
      };
    } catch (e) {
      #err("NACHOS mint call failed: " # Error.message(e));
    };
  };

  // --- 10A: Genesis Mint ---
  public shared ({ caller }) func genesisMint(blockNumber : Nat) : async Result.Result<MintResult, NachosError> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller)) return #err(#NotAuthorized);
    if (genesisComplete) return #err(#GenesisAlreadyDone);
    if (nachosLedger == null) return #err(#UnexpectedError("Nachos ledger not set"));

    let blockKey = makeBlockKey(ICPprincipal, blockNumber);
    if (Map.has(blocksDone, thash, blockKey)) return #err(#BlockAlreadyProcessed);

    // Mark block as done BEFORE await (safe async)
    Map.set(blocksDone, thash, blockKey, now());

    // Verify block
    let verifyResult = await verifyICPBlock(blockNumber, caller, NachosTreasurySubaccount);
    let depositAmount = switch (verifyResult) {
      case (#ok({ amount })) { amount };
      case (#err(e)) {
        ignore Map.remove(blocksDone, thash, blockKey);
        return #err(#BlockVerificationFailed(e));
      };
    };

    if (depositAmount < minMintValueICP) {
      ignore Map.remove(blocksDone, thash, blockKey);
      return #err(#BelowMinimumValue);
    };

    // Calculate NACHOS at initial NAV (no fee for genesis)
    let nachosAmount = (depositAmount * ONE_E8S) / INITIAL_NAV_PER_TOKEN_E8S;

    // Mint NACHOS tokens
    let mintTxResult = await mintNachosTokens(caller, nachosAmount);
    let nachosLedgerTxId = switch (mintTxResult) {
      case (#ok(txId)) { ?txId };
      case (#err(e)) {
        // Return ICP on failure
        ignore createTransferTask(caller, #principal(caller), depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, 0);
        ignore Map.remove(blocksDone, thash, blockKey);
        return #err(#TransferError(e));
      };
    };

    genesisComplete := true;

    // Record deposit and operation
    recordDeposit(blockKey, caller, ICPprincipal, depositAmount, blockNumber);
    consumeDeposit(blockKey, 0);
    recordDepositStat(ICPprincipal, depositAmount, caller, blockNumber, depositAmount);

    let deposits : [TokenDeposit] = [{ token = ICPprincipal; amount = depositAmount; priceUsed = ONE_E8S; valueICP = depositAmount }];

    let mintRecord : MintRecord = {
      id = 0;
      timestamp = now();
      caller;
      mintMode = #ICP;
      deposits;
      excessReturned = [];
      nachosReceived = nachosAmount;
      navUsed = INITIAL_NAV_PER_TOKEN_E8S;
      totalDepositValueICP = depositAmount;
      feeValueICP = 0;
      netValueICP = depositAmount;
      nachosLedgerTxId;
    };
    Vector.add(mintHistory, mintRecord);

    // Initialize NAV
    cachedNAV := ?{
      navPerTokenE8s = INITIAL_NAV_PER_TOKEN_E8S;
      portfolioValueICP = depositAmount;
      nachosSupply = nachosAmount;
      timestamp = now();
    };
    recordNavSnapshot(#Mint);

    nextMintId := 1;

    logger.info("GENESIS", "Genesis mint complete: " # Nat.toText(nachosAmount) # " NACHOS for " # Nat.toText(depositAmount) # " ICP", "genesisMint");

    #ok({
      success = true;
      mintId = 0;
      mintMode = #ICP;
      nachosReceived = nachosAmount;
      navUsed = INITIAL_NAV_PER_TOKEN_E8S;
      deposits;
      totalDepositValueICP = depositAmount;
      excessReturned = [];
      feeValueICP = 0;
      netValueICP = depositAmount;
      nachosLedgerTxId;
    });
  };

  // --- 10B: Mode A — ICP Deposit ---
  public shared ({ caller }) func mintNachos(blockNumber : Nat, minimumNachosReceive : Nat) : async Result.Result<MintResult, NachosError> {
    // Pre-checks
    switch (await performSharedPreChecks(caller, true)) {
      case (#err(e)) { return #err(e) };
      case (#ok(())) {};
    };

    let blockKey = makeBlockKey(ICPprincipal, blockNumber);
    if (Map.has(blocksDone, thash, blockKey)) {
      releaseLock(caller);
      return #err(#BlockAlreadyProcessed);
    };

    // Mark block done BEFORE await
    Map.set(blocksDone, thash, blockKey, now());

    // Verify block
    let verifyResult = await verifyICPBlock(blockNumber, caller, NachosTreasurySubaccount);
    let depositAmount = switch (verifyResult) {
      case (#ok({ amount })) { amount };
      case (#err(e)) {
        ignore Map.remove(blocksDone, thash, blockKey);
        releaseLock(caller);
        return #err(#BlockVerificationFailed(e));
      };
    };

    // Record deposit
    recordDeposit(blockKey, caller, ICPprincipal, depositAmount, blockNumber);

    // Check minimum value
    if (depositAmount < minMintValueICP) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };

    // Rate limit check
    switch (checkAndRecordMintRateLimit(caller, depositAmount)) {
      case (#err(e)) {
        ignore cancelDepositAndRefund(blockKey, caller, depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
        releaseLock(caller);
        return #err(e);
      };
      case (#ok(())) {};
    };

    // Calculate NAV
    let nav = switch (await calculateNAV()) {
      case (#ok(n)) { n };
      case (#err(e)) {
        ignore cancelDepositAndRefund(blockKey, caller, depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
        releaseLock(caller);
        return #err(#UnexpectedError(e));
      };
    };

    // Calculate fee (guard against underflow if minFee > deposit)
    let feeValue = calculateFee(caller, depositAmount, mintFeeBasisPoints, minMintFeeICP);
    if (feeValue >= depositAmount) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };
    let netValue = depositAmount - feeValue;

    // Calculate NACHOS amount
    let nachosAmount = (netValue * ONE_E8S) / nav.navPerTokenE8s;

    // Slippage check
    if (nachosAmount < minimumNachosReceive) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#SlippageExceeded);
    };

    // Mint NACHOS
    let mintTxResult = await mintNachosTokens(caller, nachosAmount);
    let nachosLedgerTxId = switch (mintTxResult) {
      case (#ok(txId)) { ?txId };
      case (#err(e)) {
        ignore cancelDepositAndRefund(blockKey, caller, depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
        releaseLock(caller);
        return #err(#TransferError(e));
      };
    };

    // Record everything
    consumeDeposit(blockKey, nextMintId);
    recordDepositStat(ICPprincipal, depositAmount, caller, blockNumber, depositAmount);

    let deposits : [TokenDeposit] = [{ token = ICPprincipal; amount = depositAmount; priceUsed = ONE_E8S; valueICP = depositAmount }];

    let mintRecord : MintRecord = {
      id = nextMintId;
      timestamp = now();
      caller;
      mintMode = #ICP;
      deposits;
      excessReturned = [];
      nachosReceived = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      totalDepositValueICP = depositAmount;
      feeValueICP = feeValue;
      netValueICP = netValue;
      nachosLedgerTxId;
    };
    Vector.add(mintHistory, mintRecord);

    if (feeValue > 0) {
      Vector.add(feeHistory, { timestamp = now(); feeType = #Mint; feeAmountICP = feeValue; userPrincipal = caller; operationId = nextMintId });
    };

    let mintId = nextMintId;
    nextMintId += 1;

    recordNavSnapshot(#Mint);
    releaseLock(caller);

    logger.info("MINT", "Mint #" # Nat.toText(mintId) # " " # Nat.toText(nachosAmount) # " NACHOS for " # Nat.toText(depositAmount) # " ICP", "mintNachos");

    #ok({
      success = true;
      mintId;
      mintMode = #ICP;
      nachosReceived = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      deposits;
      totalDepositValueICP = depositAmount;
      excessReturned = [];
      feeValueICP = feeValue;
      netValueICP = netValue;
      nachosLedgerTxId;
    });
  };

  // --- 10C: Mode B — Single Token Deposit ---
  public shared ({ caller }) func mintNachosWithToken(tokenPrincipal : Principal, blockNumber : Nat, minimumNachosReceive : Nat) : async Result.Result<MintResult, NachosError> {
    // Pre-checks
    switch (await performSharedPreChecks(caller, true)) {
      case (#err(e)) { return #err(e) };
      case (#ok(())) {};
    };

    // Check token is accepted
    switch (Map.get(acceptedMintTokens, phash, tokenPrincipal)) {
      case (?config) { if (not config.enabled) { releaseLock(caller); return #err(#TokenNotAccepted) } };
      case null { releaseLock(caller); return #err(#TokenNotAccepted) };
    };

    // Check token is active
    switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
      case (?details) {
        if (not details.Active) { releaseLock(caller); return #err(#TokenNotActive) };
        if (details.isPaused) { releaseLock(caller); return #err(#TokenPaused) };
      };
      case null { releaseLock(caller); return #err(#TokenNotActive) };
    };

    let blockKey = makeBlockKey(tokenPrincipal, blockNumber);
    if (Map.has(blocksDone, thash, blockKey)) { releaseLock(caller); return #err(#BlockAlreadyProcessed) };

    Map.set(blocksDone, thash, blockKey, now());

    // Verify block
    let verifyResult = await verifyBlock(tokenPrincipal, blockNumber, caller, NachosTreasurySubaccount);
    let depositAmount = switch (verifyResult) {
      case (#ok({ amount })) { amount };
      case (#err(e)) {
        ignore Map.remove(blocksDone, thash, blockKey);
        releaseLock(caller);
        return #err(#BlockVerificationFailed(e));
      };
    };

    recordDeposit(blockKey, caller, tokenPrincipal, depositAmount, blockNumber);

    // Conservative price discovery
    let tokenPriceICP = getConservativePrice(tokenPrincipal);
    if (tokenPriceICP == 0) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#InvalidPrice);
    };

    let tokenDecimals = switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
      case (?d) { d.tokenDecimals };
      case null { 8 };
    };

    let depositValueICP = (depositAmount * tokenPriceICP) / (10 ** tokenDecimals);

    if (depositValueICP < minMintValueICP) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };

    switch (checkAndRecordMintRateLimit(caller, depositValueICP)) {
      case (#err(e)) {
        ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
        releaseLock(caller);
        return #err(e);
      };
      case (#ok(())) {};
    };

    // Allocation enforcement: check if this token is over its target
    let portfolioValue = calculatePortfolioValueICP();
    let targetBP = switch (Map.get(aggregateAllocation, phash, tokenPrincipal)) {
      case (?vp) {
        var totalVP : Nat = 0;
        for ((_, v) in Map.entries(aggregateAllocation)) { totalVP += v };
        if (totalVP > 0) { (vp * 10_000) / totalVP } else { 0 };
      };
      case null { 0 };
    };

    // Calculate how much we can accept before exceeding allocation
    var usedAmount = depositAmount;
    var excessAmount : Nat = 0;

    if (targetBP > 0 and portfolioValue > 0) {
      let currentTokenValue = switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
        case (?d) { (d.balance * d.priceInICP) / (10 ** d.tokenDecimals) };
        case null { 0 };
      };
      let maxAllowedValue = (portfolioValue * targetBP) / 10_000;

      if (currentTokenValue + depositValueICP > maxAllowedValue) {
        if (currentTokenValue >= maxAllowedValue) {
          // Already at or over allocation
          ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
          releaseLock(caller);
          return #err(#AllocationExceeded);
        };
        let allowedValueICP = maxAllowedValue - currentTokenValue;
        usedAmount := (allowedValueICP * (10 ** tokenDecimals)) / tokenPriceICP;
        if (usedAmount > depositAmount) usedAmount := depositAmount;
        excessAmount := depositAmount - usedAmount;
      };
    };

    let usedValueICP = (usedAmount * tokenPriceICP) / (10 ** tokenDecimals);

    // Return excess
    let excessReturned = if (excessAmount > 0) {
      ignore createTransferTask(caller, #principal(caller), excessAmount, tokenPrincipal, NachosTreasurySubaccount, #ExcessReturn, blockNumber);
      [{ token = tokenPrincipal; amount = excessAmount; priceUsed = tokenPriceICP; valueICP = (excessAmount * tokenPriceICP) / (10 ** tokenDecimals) }];
    } else { [] : [TokenDeposit] };

    // Calculate NAV
    let nav = switch (await calculateNAV()) {
      case (#ok(n)) { n };
      case (#err(e)) {
        ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
        releaseLock(caller);
        return #err(#UnexpectedError(e));
      };
    };

    let feeValue = calculateFee(caller, usedValueICP, mintFeeBasisPoints, minMintFeeICP);
    if (feeValue >= usedValueICP) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };
    let netValue = usedValueICP - feeValue;
    let nachosAmount = (netValue * ONE_E8S) / nav.navPerTokenE8s;

    if (nachosAmount < minimumNachosReceive) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#SlippageExceeded);
    };

    // Mint NACHOS
    let mintTxResult = await mintNachosTokens(caller, nachosAmount);
    let nachosLedgerTxId = switch (mintTxResult) {
      case (#ok(txId)) { ?txId };
      case (#err(e)) {
        ignore cancelDepositAndRefund(blockKey, caller, usedAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
        releaseLock(caller);
        return #err(#TransferError(e));
      };
    };

    consumeDeposit(blockKey, nextMintId);
    recordDepositStat(tokenPrincipal, usedAmount, caller, blockNumber, usedValueICP);

    let deposits : [TokenDeposit] = [{ token = tokenPrincipal; amount = usedAmount; priceUsed = tokenPriceICP; valueICP = usedValueICP }];

    let mintRecord : MintRecord = {
      id = nextMintId;
      timestamp = now();
      caller;
      mintMode = #SingleToken;
      deposits;
      excessReturned;
      nachosReceived = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      totalDepositValueICP = usedValueICP;
      feeValueICP = feeValue;
      netValueICP = netValue;
      nachosLedgerTxId;
    };
    Vector.add(mintHistory, mintRecord);

    if (feeValue > 0) {
      Vector.add(feeHistory, { timestamp = now(); feeType = #Mint; feeAmountICP = feeValue; userPrincipal = caller; operationId = nextMintId });
    };

    let mintId = nextMintId;
    nextMintId += 1;
    recordNavSnapshot(#Mint);
    releaseLock(caller);

    logger.info("MINT", "Token mint #" # Nat.toText(mintId) # " " # Nat.toText(nachosAmount) # " NACHOS for " # Nat.toText(usedAmount) # " tokens", "mintNachosWithToken");

    #ok({
      success = true;
      mintId;
      mintMode = #SingleToken;
      nachosReceived = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      deposits;
      totalDepositValueICP = usedValueICP;
      excessReturned;
      feeValueICP = feeValue;
      netValueICP = netValue;
      nachosLedgerTxId;
    });
  };

  // --- 10D: Mode C — Portfolio Share Deposit ---
  public shared ({ caller }) func mintNachosWithPortfolioShare(
    depositInfos : [{ token : Principal; blockNumber : Nat }],
    minimumNachosReceive : Nat,
  ) : async Result.Result<MintResult, NachosError> {
    switch (await performSharedPreChecks(caller, true)) {
      case (#err(e)) { return #err(e) };
      case (#ok(())) {};
    };

    if (depositInfos.size() == 0) { releaseLock(caller); return #err(#InvalidAllocation) };

    // Verify all blocks and collect deposit data
    let verifiedDeposits = Vector.new<{ token : Principal; amount : Nat; blockNumber : Nat; blockKey : Text }>();
    let blockKeys = Vector.new<Text>();

    // Mark all blocks BEFORE any await (safe async)
    for (info in depositInfos.vals()) {
      let blockKey = makeBlockKey(info.token, info.blockNumber);
      if (Map.has(blocksDone, thash, blockKey)) {
        releaseLock(caller);
        return #err(#BlockAlreadyProcessed);
      };
      Map.set(blocksDone, thash, blockKey, now());
      Vector.add(blockKeys, blockKey);
    };

    // Verify each block
    var verifyIdx : Nat = 0;
    for (info in depositInfos.vals()) {
      let verifyResult = await verifyBlock(info.token, info.blockNumber, caller, NachosTreasurySubaccount);
      switch (verifyResult) {
        case (#ok({ amount })) {
          let blockKey = makeBlockKey(info.token, info.blockNumber);
          Vector.add(verifiedDeposits, { token = info.token; amount; blockNumber = info.blockNumber; blockKey });
          recordDeposit(blockKey, caller, info.token, amount, info.blockNumber);
        };
        case (#err(e)) {
          // Cancel and refund already verified deposits on failure
          for (vd in Vector.vals(verifiedDeposits)) {
            ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
          };
          // Clean up block keys for unverified
          var cleanIdx = verifyIdx;
          while (cleanIdx < depositInfos.size()) {
            let bk = makeBlockKey(depositInfos[cleanIdx].token, depositInfos[cleanIdx].blockNumber);
            ignore Map.remove(blocksDone, thash, bk);
            cleanIdx += 1;
          };
          releaseLock(caller);
          return #err(#BlockVerificationFailed(e));
        };
      };
      verifyIdx += 1;
    };

    // Calculate value per token with conservative prices
    let tokenDeposits = Vector.new<TokenDeposit>();
    var totalValueICP : Nat = 0;

    for (vd in Vector.vals(verifiedDeposits)) {
      let price = getConservativePrice(vd.token);
      if (price == 0) {
        // Cancel all deposits and return everything
        for (vd2 in Vector.vals(verifiedDeposits)) {
          ignore cancelDepositAndRefund(vd2.blockKey, caller, vd2.amount, vd2.token, NachosTreasurySubaccount, #MintReturn, vd2.blockNumber);
        };
        releaseLock(caller);
        return #err(#InvalidPrice);
      };

      let decimals = switch (Map.get(tokenDetailsMap, phash, vd.token)) {
        case (?d) { d.tokenDecimals };
        case null { 8 };
      };

      let valueICP = (vd.amount * price) / (10 ** decimals);
      totalValueICP += valueICP;
      Vector.add(tokenDeposits, { token = vd.token; amount = vd.amount; priceUsed = price; valueICP });
    };

    if (totalValueICP < minMintValueICP) {
      for (vd in Vector.vals(verifiedDeposits)) {
        ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
      };
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };

    // Validate portfolio share proportions
    let portfolioValue = calculatePortfolioValueICP();
    if (portfolioValue > 0) {
      for (td in Vector.vals(tokenDeposits)) {
        let expectedBP = switch (Map.get(tokenDetailsMap, phash, td.token)) {
          case (?details) {
            let tokenValueInPortfolio = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
            if (portfolioValue > 0) { (tokenValueInPortfolio * 10_000) / portfolioValue } else { 0 };
          };
          case null { 0 };
        };

        let actualBP = if (totalValueICP > 0) { (td.valueICP * 10_000) / totalValueICP } else { 0 };

        let deviation = if (actualBP > expectedBP) { actualBP - expectedBP } else { expectedBP - actualBP };
        if (deviation > portfolioShareMaxDeviationBP) {
          for (vd in Vector.vals(verifiedDeposits)) {
            ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
          };
          releaseLock(caller);
          return #err(#PortfolioShareMismatch({
            expected = [];
            received = [];
          }));
        };
      };
    };

    switch (checkAndRecordMintRateLimit(caller, totalValueICP)) {
      case (#err(e)) {
        for (vd in Vector.vals(verifiedDeposits)) {
          ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
        };
        releaseLock(caller);
        return #err(e);
      };
      case (#ok(())) {};
    };

    let nav = switch (await calculateNAV()) {
      case (#ok(n)) { n };
      case (#err(e)) {
        for (vd in Vector.vals(verifiedDeposits)) {
          ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
        };
        releaseLock(caller);
        return #err(#UnexpectedError(e));
      };
    };

    let feeValue = calculateFee(caller, totalValueICP, mintFeeBasisPoints, minMintFeeICP);
    if (feeValue >= totalValueICP) {
      for (vd in Vector.vals(verifiedDeposits)) {
        ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
      };
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };
    let netValue = totalValueICP - feeValue;
    let nachosAmount = (netValue * ONE_E8S) / nav.navPerTokenE8s;

    if (nachosAmount < minimumNachosReceive) {
      for (vd in Vector.vals(verifiedDeposits)) {
        ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
      };
      releaseLock(caller);
      return #err(#SlippageExceeded);
    };

    let mintTxResult = await mintNachosTokens(caller, nachosAmount);
    let nachosLedgerTxId = switch (mintTxResult) {
      case (#ok(txId)) { ?txId };
      case (#err(e)) {
        for (vd in Vector.vals(verifiedDeposits)) {
          ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
        };
        releaseLock(caller);
        return #err(#TransferError(e));
      };
    };

    // Consume all deposits
    for (vd in Vector.vals(verifiedDeposits)) {
      consumeDeposit(vd.blockKey, nextMintId);
      let vICP = switch (Array.find<TokenDeposit>(Vector.toArray(tokenDeposits), func(td) { td.token == vd.token })) {
        case (?td) { td.valueICP };
        case null { 0 };
      };
      recordDepositStat(vd.token, vd.amount, caller, vd.blockNumber, vICP);
    };
    let depositsArr = Vector.toArray(tokenDeposits);

    let mintRecord : MintRecord = {
      id = nextMintId;
      timestamp = now();
      caller;
      mintMode = #PortfolioShare;
      deposits = depositsArr;
      excessReturned = [];
      nachosReceived = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      totalDepositValueICP = totalValueICP;
      feeValueICP = feeValue;
      netValueICP = netValue;
      nachosLedgerTxId;
    };
    Vector.add(mintHistory, mintRecord);

    if (feeValue > 0) {
      Vector.add(feeHistory, { timestamp = now(); feeType = #Mint; feeAmountICP = feeValue; userPrincipal = caller; operationId = nextMintId });
    };

    let mintId = nextMintId;
    nextMintId += 1;
    recordNavSnapshot(#Mint);
    releaseLock(caller);

    logger.info("MINT", "Portfolio mint #" # Nat.toText(mintId) # " " # Nat.toText(nachosAmount) # " NACHOS for " # Nat.toText(totalValueICP) # " ICP worth", "mintNachosWithPortfolioShare");

    #ok({
      success = true;
      mintId;
      mintMode = #PortfolioShare;
      nachosReceived = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      deposits = depositsArr;
      totalDepositValueICP = totalValueICP;
      excessReturned = [];
      feeValueICP = feeValue;
      netValueICP = netValue;
      nachosLedgerTxId;
    });
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 11: BURNING / REDEMPTION FLOW
  // ═══════════════════════════════════════════════════════════════════

  // Helper: Burn NACHOS (transfer from deposit subaccount to minting account = burn)
  private func burnNachosTokens(amount : Nat) : async Result.Result<Nat, Text> {
    let ledger = switch (nachosLedger) {
      case (?l) { l };
      case null { return #err("Nachos ledger not set") };
    };

    try {
      let result = await ledger.icrc1_transfer({
        from_subaccount = ?Blob.fromArray([NachosDepositSubaccount, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        to = { owner = this_canister_id(); subaccount = null }; // To minting account = burn
        amount;
        fee = null;
        memo = null;
        created_at_time = null;
      });
      switch (result) {
        case (#Ok(txId)) { #ok(txId) };
        case (#Err(e)) { #err("NACHOS burn failed: " # debug_show (e)) };
      };
    } catch (e) {
      #err("NACHOS burn call failed: " # Error.message(e));
    };
  };

  // Helper: Return NACHOS from deposit subaccount back to caller on failure
  private func returnNachosToUser(caller : Principal, amount : Nat) : async Result.Result<Nat, Text> {
    let ledger = switch (nachosLedger) {
      case (?l) { l };
      case null { return #err("Nachos ledger not set") };
    };

    try {
      let result = await ledger.icrc1_transfer({
        from_subaccount = ?Blob.fromArray([NachosDepositSubaccount, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        to = { owner = caller; subaccount = null };
        amount;
        fee = null;
        memo = null;
        created_at_time = null;
      });
      switch (result) {
        case (#Ok(txId)) { #ok(txId) };
        case (#Err(e)) { #err("Return NACHOS failed: " # debug_show (e)) };
      };
    } catch (e) {
      #err("Return NACHOS call failed: " # Error.message(e));
    };
  };

  public shared ({ caller }) func redeemNachos(nachosBlockNumber : Nat, minimumValues : ?[{ token : Principal; minAmount : Nat }]) : async Result.Result<BurnResult, NachosError> {
    // Pre-checks
    switch (await performSharedPreChecks(caller, false)) {
      case (#err(e)) { return #err(e) };
      case (#ok(())) {};
    };

    let nachosLedgerP = switch (nachosLedgerPrincipal) {
      case (?p) { p };
      case null { releaseLock(caller); return #err(#UnexpectedError("Nachos ledger not set")) };
    };

    let blockKey = "NACHOS:" # Nat.toText(nachosBlockNumber);
    if (Map.has(blocksDone, thash, blockKey)) { releaseLock(caller); return #err(#BlockAlreadyProcessed) };

    Map.set(blocksDone, thash, blockKey, now());

    // Verify NACHOS transfer to deposit subaccount
    let verifyResult = await verifyBlock(nachosLedgerP, nachosBlockNumber, caller, NachosDepositSubaccount);
    let nachosAmount = switch (verifyResult) {
      case (#ok({ amount })) { amount };
      case (#err(e)) {
        ignore Map.remove(blocksDone, thash, blockKey);
        releaseLock(caller);
        return #err(#BlockVerificationFailed(e));
      };
    };

    // Rate limit check
    switch (checkAndRecordBurnRateLimit(caller, nachosAmount)) {
      case (#err(e)) {
        if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
        releaseLock(caller);
        return #err(e);
      };
      case (#ok(())) {};
    };

    // Calculate NAV
    let nav = switch (await calculateNAV()) {
      case (#ok(n)) { n };
      case (#err(e)) {
        if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
        releaseLock(caller);
        return #err(#UnexpectedError(e));
      };
    };

    // Calculate redemption value
    let redemptionValueICP = (nachosAmount * nav.navPerTokenE8s) / ONE_E8S;

    if (redemptionValueICP < minBurnValueICP) {
      if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };

    let feeValue = calculateFee(caller, redemptionValueICP, burnFeeBasisPoints, minBurnFeeICP);
    if (feeValue >= redemptionValueICP) {
      if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
      releaseLock(caller);
      return #err(#BelowMinimumValue);
    };
    let netValueICP = redemptionValueICP - feeValue;

    // Calculate proportional entitlements per token
    let portfolioValue = calculatePortfolioValueICP();
    if (portfolioValue == 0) {
      if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
      releaseLock(caller);
      return #err(#UnexpectedError("Portfolio value is zero"));
    };

    let tokensToSend = Vector.new<{ token : Principal; amount : Nat; fromSubaccount : Nat8 }>();
    let skippedDust = Vector.new<Principal>();

    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and details.balance > 0 and details.priceInICP > 0) {
        let tokenValueICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        let tokenShareBP = (tokenValueICP * 10_000) / portfolioValue;

        // Proportional amount of this token to send (fee distributed proportionally)
        let tokenEntitlementICP = (netValueICP * tokenShareBP) / 10_000;
        let tokenAmount = (tokenEntitlementICP * (10 ** details.tokenDecimals)) / details.priceInICP;

        // Skip dust (less than 3x transfer fee)
        if (tokenAmount <= details.tokenTransferFee * 3) {
          Vector.add(skippedDust, token);
        } else if (tokenAmount > details.balance) {
          // Insufficient balance
          if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
          releaseLock(caller);
          return #err(#InsufficientLiquidity({ token; available = details.balance; requested = tokenAmount }));
        } else {
          Vector.add(tokensToSend, { token; amount = tokenAmount; fromSubaccount = 0 : Nat8 });
        };
      };
    };

    // Per-token slippage check
    switch (minimumValues) {
      case (?mins) {
        for (minVal in mins.vals()) {
          var found = false;
          for (ts in Vector.vals(tokensToSend)) {
            if (ts.token == minVal.token) {
              if (ts.amount < minVal.minAmount) {
                if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
                releaseLock(caller);
                return #err(#SlippageExceeded);
              };
              found := true;
            };
          };
        };
      };
      case null {};
    };

    // Burn NACHOS tokens
    let burnTxResult = await burnNachosTokens(nachosAmount);
    let nachosLedgerTxId = switch (burnTxResult) {
      case (#ok(txId)) { ?txId };
      case (#err(e)) {
        if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
        releaseLock(caller);
        return #err(#TransferError(e));
      };
    };

    // Send all tokens via transfer queue (failures handled async by queue timer retries)
    let tokensReceived = Vector.new<TokenTransferResult>();

    for (ts in Vector.vals(tokensToSend)) {
      let taskId = createTransferTask(
        caller,
        #principal(caller),
        ts.amount,
        ts.token,
        ts.fromSubaccount,
        #BurnPayout,
        nextBurnId,
      );
      Vector.add(tokensReceived, { token = ts.token; amount = ts.amount; txId = ?taskId });
    };

    let burnRecord : BurnRecord = {
      id = nextBurnId;
      timestamp = now();
      caller;
      nachosBurned = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      redemptionValueICP;
      feeValueICP = feeValue;
      netValueICP;
      tokensReceived = Vector.toArray(tokensReceived);
      skippedDustTokens = Vector.toArray(skippedDust);
      nachosLedgerTxId;
      partialFailure = false;
    };
    Vector.add(burnHistory, burnRecord);

    if (feeValue > 0) {
      Vector.add(feeHistory, { timestamp = now(); feeType = #Burn; feeAmountICP = feeValue; userPrincipal = caller; operationId = nextBurnId });
    };

    let burnId = nextBurnId;
    nextBurnId += 1;
    recordNavSnapshot(#Burn);
    releaseLock(caller);

    logger.info("BURN", "Burn #" # Nat.toText(burnId) # " " # Nat.toText(nachosAmount) # " NACHOS redeemed for " # Nat.toText(netValueICP) # " ICP worth", "redeemNachos");

    #ok({
      success = true;
      burnId;
      nachosBurned = nachosAmount;
      navUsed = nav.navPerTokenE8s;
      redemptionValueICP;
      feeValueICP = feeValue;
      netValueICP;
      tokensReceived = Vector.toArray(tokensReceived);
      skippedDustTokens = Vector.toArray(skippedDust);
      nachosLedgerTxId;
      partialFailure = false;
      failedTokens = [];
    });
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 12A: ADMIN FUNCTIONS
  // ═══════════════════════════════════════════════════════════════════

  public shared ({ caller }) func setNachosLedgerPrincipal(ledgerPrincipal : Principal) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller)) return #err("Not authorized");
    nachosLedgerPrincipal := ?ledgerPrincipal;
    nachosLedger := ?(actor (Principal.toText(ledgerPrincipal)) : ICRC1.FullInterface);
    logger.info("ADMIN", "Set NACHOS ledger principal to " # Principal.toText(ledgerPrincipal), "setNachosLedgerPrincipal");
    #ok("Ledger principal set");
  };

  public shared ({ caller }) func updateNachosConfig(config : NachosUpdateConfig) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");

    switch (config.mintFeeBasisPoints) { case (?v) { if (v <= MAX_FEE_BP) mintFeeBasisPoints := v }; case null {} };
    switch (config.burnFeeBasisPoints) { case (?v) { if (v <= MAX_FEE_BP) burnFeeBasisPoints := v }; case null {} };
    switch (config.minMintValueICP) { case (?v) { if (v > 0) minMintValueICP := v }; case null {} };
    switch (config.minBurnValueICP) { case (?v) { if (v > 0) minBurnValueICP := v }; case null {} };
    switch (config.MAX_PRICE_STALENESS_NS) { case (?v) { if (v > 0) MAX_PRICE_STALENESS_NS := v }; case null {} };
    switch (config.PRICE_HISTORY_WINDOW) { case (?v) { if (v > 0) PRICE_HISTORY_WINDOW := v }; case null {} };
    switch (config.maxSlippageBasisPoints) { case (?v) { if (v > 0 and v <= 5000) maxSlippageBasisPoints := v }; case null {} };
    switch (config.maxNachosBurnPer4Hours) { case (?v) { if (v > 0) maxNachosBurnPer4Hours := v }; case null {} };
    switch (config.maxMintICPWorthPer4Hours) { case (?v) { if (v > 0) maxMintICPWorthPer4Hours := v }; case null {} };
    switch (config.maxMintOpsPerUser4Hours) { case (?v) { if (v > 0) maxMintOpsPerUser4Hours := v }; case null {} };
    switch (config.maxBurnOpsPerUser4Hours) { case (?v) { if (v > 0) maxBurnOpsPerUser4Hours := v }; case null {} };
    switch (config.navDropThresholdPercent) { case (?v) { if (v > 0.0 and v <= 100.0) navDropThresholdPercent := v }; case null {} };
    switch (config.navDropTimeWindowNS) { case (?v) { if (v > 0) navDropTimeWindowNS := v }; case null {} };
    switch (config.portfolioShareMaxDeviationBP) { case (?v) { if (v > 0 and v <= 10_000) portfolioShareMaxDeviationBP := v }; case null {} };
    switch (config.cancellationFeeMultiplier) { case (?v) { if (v >= 1 and v <= 100) cancellationFeeMultiplier := v }; case null {} };
    switch (config.mintingEnabled) { case (?v) { mintingEnabled := v }; case null {} };
    switch (config.burningEnabled) { case (?v) { burningEnabled := v }; case null {} };

    logger.info("ADMIN", "Config updated by " # Principal.toText(caller), "updateNachosConfig");
    #ok("Config updated");
  };

  public shared ({ caller }) func updateFees(mintBP : Nat, burnBP : Nat) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    if (mintBP > MAX_FEE_BP or burnBP > MAX_FEE_BP) return #err("Fee exceeds maximum");
    mintFeeBasisPoints := mintBP;
    burnFeeBasisPoints := burnBP;
    logger.info("ADMIN", "Fees updated: mint=" # Nat.toText(mintBP) # "bp burn=" # Nat.toText(burnBP) # "bp", "updateFees");
    #ok("Fees updated");
  };

  public shared ({ caller }) func pauseMinting() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    mintingEnabled := false;
    logger.info("ADMIN", "Minting paused by " # Principal.toText(caller), "pauseMinting");
    #ok("Minting paused");
  };

  public shared ({ caller }) func unpauseMinting() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    mintingEnabled := true;
    logger.info("ADMIN", "Minting unpaused by " # Principal.toText(caller), "unpauseMinting");
    #ok("Minting unpaused");
  };

  public shared ({ caller }) func pauseBurning() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    burningEnabled := false;
    logger.info("ADMIN", "Burning paused by " # Principal.toText(caller), "pauseBurning");
    #ok("Burning paused");
  };

  public shared ({ caller }) func unpauseBurning() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    burningEnabled := true;
    logger.info("ADMIN", "Burning unpaused by " # Principal.toText(caller), "unpauseBurning");
    #ok("Burning unpaused");
  };

  public shared ({ caller }) func emergencyPause() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    systemPaused := true;
    logger.warn("ADMIN", "EMERGENCY PAUSE activated by " # Principal.toText(caller), "emergencyPause");
    #ok("System paused");
  };

  public shared ({ caller }) func emergencyUnpause() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    systemPaused := false;
    logger.info("ADMIN", "Emergency unpause by " # Principal.toText(caller), "emergencyUnpause");
    #ok("System unpaused");
  };

  public shared ({ caller }) func resetCircuitBreaker() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    circuitBreakerActive := false;
    logger.info("ADMIN", "Circuit breaker reset by " # Principal.toText(caller), "resetCircuitBreaker");
    #ok("Circuit breaker reset");
  };

  public shared ({ caller }) func addAcceptedMintToken(token : Principal) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    Map.set(acceptedMintTokens, phash, token, { addedAt = now(); addedBy = caller; enabled = true });
    logger.info("ADMIN", "Added accepted mint token: " # Principal.toText(token), "addAcceptedMintToken");
    #ok("Token added");
  };

  public shared ({ caller }) func removeAcceptedMintToken(token : Principal) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    ignore Map.remove(acceptedMintTokens, phash, token);
    logger.info("ADMIN", "Removed accepted mint token: " # Principal.toText(token), "removeAcceptedMintToken");
    #ok("Token removed");
  };

  public shared ({ caller }) func setAcceptedMintTokenEnabled(token : Principal, enabled : Bool) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    switch (Map.get(acceptedMintTokens, phash, token)) {
      case (?config) { Map.set(acceptedMintTokens, phash, token, { config with enabled }) };
      case null { return #err("Token not found") };
    };
    #ok("Token " # (if enabled "enabled" else "disabled"));
  };

  public shared ({ caller }) func updateCancellationFeeMultiplier(multiplier : Nat) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    cancellationFeeMultiplier := multiplier;
    #ok("Updated to " # Nat.toText(multiplier) # "x");
  };

  public shared ({ caller }) func addFeeExemptPrincipal(principal : Principal, reason : Text) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    Map.set(feeExemptPrincipals, phash, principal, { addedAt = now(); addedBy = caller; reason; enabled = true });
    #ok("Added fee exemption");
  };

  public shared ({ caller }) func removeFeeExemptPrincipal(principal : Principal) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    ignore Map.remove(feeExemptPrincipals, phash, principal);
    #ok("Removed fee exemption");
  };

  public shared ({ caller }) func addRateLimitExemptPrincipal(principal : Principal, reason : Text) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    Map.set(rateLimitExemptPrincipals, phash, principal, { addedAt = now(); addedBy = caller; reason; enabled = true });
    #ok("Added rate limit exemption");
  };

  public shared ({ caller }) func removeRateLimitExemptPrincipal(principal : Principal) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    ignore Map.remove(rateLimitExemptPrincipals, phash, principal);
    #ok("Removed rate limit exemption");
  };

  // Recover tokens accidentally sent to vault (wrong subaccount, wrong token, etc.)
  public shared ({ caller }) func recoverWronglySentTokens(
    tokenPrincipal : Principal,
    blockNumber : Nat,
    senderPrincipal : Principal,
  ) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");

    // Block dedup check
    let blockKey = makeBlockKey(tokenPrincipal, blockNumber);
    if (Map.has(blocksDone, thash, blockKey)) return #err("Block already processed");

    // Mark block done BEFORE await (safe async)
    Map.set(blocksDone, thash, blockKey, now());

    // Try verifying against all subaccounts: default (0), deposit (1), treasury (2)
    let subaccountsToTry : [Nat8] = [0, NachosDepositSubaccount, NachosTreasurySubaccount];
    var verifiedAmount : Nat = 0;
    var verifiedSubaccount : Nat8 = 0;
    var verified = false;

    for (sub in subaccountsToTry.vals()) {
      if (not verified) {
        try {
          let result = await verifyBlock(tokenPrincipal, blockNumber, senderPrincipal, sub);
          switch (result) {
            case (#ok({ amount })) {
              verifiedAmount := amount;
              verifiedSubaccount := sub;
              verified := true;
            };
            case (#err(_)) {};
          };
        } catch (_) {};
      };
    };

    if (not verified) {
      ignore Map.remove(blocksDone, thash, blockKey);
      return #err("Block verification failed for all subaccounts");
    };

    // Recovery fee: 3x the token fee (same as cancellation pattern)
    let tokenFee = switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
      case (?details) { details.tokenTransferFee };
      case null { 10_000 };
    };
    let recoveryFee = tokenFee * 3;

    if (verifiedAmount <= recoveryFee) return #err("Amount too small to recover after fee");

    let refundAmount = verifiedAmount - recoveryFee;

    ignore createTransferTask(
      senderPrincipal,
      #principal(senderPrincipal),
      refundAmount,
      tokenPrincipal,
      verifiedSubaccount,
      #Recovery,
      blockNumber,
    );

    logger.info("ADMIN", "Recovered " # Nat.toText(refundAmount) # " tokens for " # Principal.toText(senderPrincipal) # " block=" # Nat.toText(blockNumber) # " subaccount=" # Nat8.toText(verifiedSubaccount), "recoverWronglySentTokens");
    #ok("Recovery task created: " # Nat.toText(refundAmount) # " tokens to " # Principal.toText(senderPrincipal));
  };

  // Admin: reset exhausted transfer tasks (retryCount >= 5) for retry
  public shared ({ caller }) func retryFailedTransfers() : async Result.Result<Nat, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) {
      return #err("Not authorized");
    };
    var reset : Nat = 0;
    var i = 0;
    while (i < Vector.size(pendingTransfers)) {
      let task = Vector.get(pendingTransfers, i);
      switch (task.status) {
        case (#Failed(_)) {
          if (task.retryCount >= 5) {
            Vector.put(pendingTransfers, i, { task with retryCount = 0; status = #Pending; updatedAt = now() });
            reset += 1;
          };
        };
        case _ {};
      };
      i += 1;
    };
    logger.info("TRANSFER_QUEUE", "Admin reset " # Nat.toText(reset) # " exhausted transfer tasks", "retryFailedTransfers");
    #ok(reset);
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 12B: QUERY ENDPOINTS
  // ═══════════════════════════════════════════════════════════════════

  public query func getNAV() : async ?CachedNAV { cachedNAV };

  public query func getNAVHistory(limit : Nat) : async [NavSnapshot] {
    let size = Vector.size(navHistory);
    let start = if (size > limit) { size - limit } else { 0 };
    let result = Vector.new<NavSnapshot>();
    var i = start;
    while (i < size) {
      Vector.add(result, Vector.get(navHistory, i));
      i += 1;
    };
    Vector.toArray(result);
  };

  public query func getAcceptedMintTokens() : async [(Principal, AcceptedTokenConfig)] {
    let result = Vector.new<(Principal, AcceptedTokenConfig)>();
    for ((token, config) in Map.entries(acceptedMintTokens)) {
      Vector.add(result, (token, config));
    };
    Vector.toArray(result);
  };

  public query func getPortfolioBreakdown() : async [{ token : Principal; symbol : Text; balance : Nat; priceICP : Nat; valueICP : Nat; basisPoints : Nat }] {
    let portfolioValue = calculatePortfolioValueICP();
    let result = Vector.new<{ token : Principal; symbol : Text; balance : Nat; priceICP : Nat; valueICP : Nat; basisPoints : Nat }>();

    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and details.balance > 0) {
        let valueICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        let bp = if (portfolioValue > 0) { (valueICP * 10_000) / portfolioValue } else { 0 };
        Vector.add(result, {
          token;
          symbol = details.tokenSymbol;
          balance = details.balance;
          priceICP = details.priceInICP;
          valueICP;
          basisPoints = bp;
        });
      };
    };
    Vector.toArray(result);
  };

  public query func estimateMintICP(icpAmount : Nat) : async { nachosEstimate : Nat; feeEstimate : Nat; navUsed : Nat } {
    let nav = switch (cachedNAV) {
      case (?n) { n.navPerTokenE8s };
      case null { INITIAL_NAV_PER_TOKEN_E8S };
    };
    let fee = (icpAmount * mintFeeBasisPoints) / 10_000;
    let netValue = icpAmount - fee;
    let nachos = (netValue * ONE_E8S) / nav;
    { nachosEstimate = nachos; feeEstimate = fee; navUsed = nav };
  };

  public query func estimateRedeem(nachosAmount : Nat) : async { redemptionValueICP : Nat; feeEstimate : Nat; netValueICP : Nat } {
    let nav = switch (cachedNAV) {
      case (?n) { n.navPerTokenE8s };
      case null { INITIAL_NAV_PER_TOKEN_E8S };
    };
    let redemptionValue = (nachosAmount * nav) / ONE_E8S;
    let fee = (redemptionValue * burnFeeBasisPoints) / 10_000;
    { redemptionValueICP = redemptionValue; feeEstimate = fee; netValueICP = redemptionValue - fee };
  };

  public query func getSystemStatus() : async {
    genesisComplete : Bool;
    systemPaused : Bool;
    mintingEnabled : Bool;
    burningEnabled : Bool;
    circuitBreakerActive : Bool;
    nachosLedger : ?Principal;
    cachedNAV : ?CachedNAV;
    totalMints : Nat;
    totalBurns : Nat;
    pendingTransferCount : Nat;
    activeDepositCount : Nat;
  } {
    {
      genesisComplete;
      systemPaused;
      mintingEnabled;
      burningEnabled;
      circuitBreakerActive;
      nachosLedger = nachosLedgerPrincipal;
      cachedNAV;
      totalMints = Vector.size(mintHistory);
      totalBurns = Vector.size(burnHistory);
      pendingTransferCount = Vector.size(pendingTransfers);
      activeDepositCount = Map.size(activeDeposits);
    };
  };

  public query func getMintHistory(limit : Nat, offset : Nat) : async [MintRecord] {
    let size = Vector.size(mintHistory);
    if (offset >= size) return [];
    let end = Nat.min(offset + limit, size);
    let result = Vector.new<MintRecord>();
    var i = offset;
    while (i < end) {
      Vector.add(result, Vector.get(mintHistory, i));
      i += 1;
    };
    Vector.toArray(result);
  };

  public query func getBurnHistory(limit : Nat, offset : Nat) : async [BurnRecord] {
    let size = Vector.size(burnHistory);
    if (offset >= size) return [];
    let end = Nat.min(offset + limit, size);
    let result = Vector.new<BurnRecord>();
    var i = offset;
    while (i < end) {
      Vector.add(result, Vector.get(burnHistory, i));
      i += 1;
    };
    Vector.toArray(result);
  };

  public query func getFeeHistory(limit : Nat) : async [FeeRecord] {
    let size = Vector.size(feeHistory);
    let start = if (size > limit) { size - limit } else { 0 };
    let result = Vector.new<FeeRecord>();
    var i = start;
    while (i < size) {
      Vector.add(result, Vector.get(feeHistory, i));
      i += 1;
    };
    Vector.toArray(result);
  };

  public shared query ({ caller }) func getUserMintHistory(user : Principal) : async [MintRecord] {
    if (caller != user and not isMasterAdmin(caller)) return [];
    let result = Vector.new<MintRecord>();
    for (record in Vector.vals(mintHistory)) {
      if (record.caller == user) Vector.add(result, record);
    };
    Vector.toArray(result);
  };

  public shared query ({ caller }) func getUserBurnHistory(user : Principal) : async [BurnRecord] {
    if (caller != user and not isMasterAdmin(caller)) return [];
    let result = Vector.new<BurnRecord>();
    for (record in Vector.vals(burnHistory)) {
      if (record.caller == user) Vector.add(result, record);
    };
    Vector.toArray(result);
  };

  public shared query ({ caller }) func getTransferQueueStatus() : async { pending : Nat; completed : Nat; tasks : [VaultTransferTask] } {
    if (not isMasterAdmin(caller)) return { pending = 0; completed = 0; tasks = [] };
    { pending = Vector.size(pendingTransfers); completed = Map.size(completedTransfers); tasks = Vector.toArray(pendingTransfers) };
  };

  public shared query ({ caller }) func getUserDeposits(user : Principal) : async [ActiveDeposit] {
    if (caller != user and not isMasterAdmin(caller)) return [];
    let result = Vector.new<ActiveDeposit>();
    for ((_, deposit) in Map.entries(activeDeposits)) {
      if (deposit.caller == user) Vector.add(result, deposit);
    };
    Vector.toArray(result);
  };

  public query func getDeposit(tokenPrincipal : Principal, blockNumber : Nat) : async ?ActiveDeposit {
    Map.get(activeDeposits, thash, makeBlockKey(tokenPrincipal, blockNumber));
  };

  public query func getActiveDepositsCount() : async Nat { Map.size(activeDeposits) };

  public query func getConfig() : async {
    mintFeeBasisPoints : Nat;
    burnFeeBasisPoints : Nat;
    minMintValueICP : Nat;
    minBurnValueICP : Nat;
    maxSlippageBasisPoints : Nat;
    maxNachosBurnPer4Hours : Nat;
    maxMintICPWorthPer4Hours : Nat;
    maxMintOpsPerUser4Hours : Nat;
    maxBurnOpsPerUser4Hours : Nat;
    navDropThresholdPercent : Float;
    portfolioShareMaxDeviationBP : Nat;
    cancellationFeeMultiplier : Nat;
  } {
    {
      mintFeeBasisPoints;
      burnFeeBasisPoints;
      minMintValueICP;
      minBurnValueICP;
      maxSlippageBasisPoints;
      maxNachosBurnPer4Hours;
      maxMintICPWorthPer4Hours;
      maxMintOpsPerUser4Hours;
      maxBurnOpsPerUser4Hours;
      navDropThresholdPercent;
      portfolioShareMaxDeviationBP;
      cancellationFeeMultiplier;
    };
  };

  public shared query ({ caller }) func getUserRateLimitStatus(user : Principal) : async { mintOpsIn4h : Nat; burnOpsIn4h : Nat } {
    if (caller != user and not isMasterAdmin(caller)) return { mintOpsIn4h = 0; burnOpsIn4h = 0 };
    let cutoff = now() - FOUR_HOURS_NS;
    var mintOps : Nat = 0;
    var burnOps : Nat = 0;

    switch (Map.get(userMintOps, phash, user)) {
      case (?ops) { for (ts in Vector.vals(ops)) { if (ts >= cutoff) mintOps += 1 } };
      case null {};
    };
    switch (Map.get(userBurnOps, phash, user)) {
      case (?ops) { for (ts in Vector.vals(ops)) { if (ts >= cutoff) burnOps += 1 } };
      case null {};
    };

    { mintOpsIn4h = mintOps; burnOpsIn4h = burnOps };
  };

  public query func getGlobalRateLimitStatus() : async { totalMintValueIn4h : Nat; totalBurnAmountIn4h : Nat } {
    let cutoff = now() - FOUR_HOURS_NS;
    var totalMint : Nat = 0;
    var totalBurn : Nat = 0;

    for (entry in Vector.vals(mintRateTracker)) { if (entry.0 >= cutoff) totalMint += entry.1 };
    for (entry in Vector.vals(burnRateTracker)) { if (entry.0 >= cutoff) totalBurn += entry.1 };

    { totalMintValueIn4h = totalMint; totalBurnAmountIn4h = totalBurn };
  };

  public query func getFeeExemptPrincipals() : async [(Principal, FeeExemptConfig)] {
    let result = Vector.new<(Principal, FeeExemptConfig)>();
    for ((p, c) in Map.entries(feeExemptPrincipals)) { Vector.add(result, (p, c)) };
    Vector.toArray(result);
  };

  public query func getRateLimitExemptPrincipals() : async [(Principal, FeeExemptConfig)] {
    let result = Vector.new<(Principal, FeeExemptConfig)>();
    for ((p, c) in Map.entries(rateLimitExemptPrincipals)) { Vector.add(result, (p, c)) };
    Vector.toArray(result);
  };

  public query func getLogs(count : Nat) : async [DAO_types.LogEntry] {
    logger.getLastLogs(count);
  };

  public query func get_canister_cycles() : async Nat {
    Cycles.balance();
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 13: TIMER SYSTEM & LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  // --- Timer: Balance & Allocation Update (15 min) ---
  private func startBalanceUpdateTimer<system>() {
    switch (balanceUpdateTimerId) { case (?id) { cancelTimer(id) }; case null {} };
    balanceUpdateTimerId := ?setTimer<system>(#nanoseconds(15 * 60 * 1_000_000_000), func() : async () {
      try {
        let details = await treasury.getTokenDetails();
        for ((token, detail) in details.vals()) {
          Map.set(tokenDetailsMap, phash, token, detail);
        };
      } catch (e) {
        logger.warn("TIMER", "Balance update failed: " # Error.message(e), "balanceUpdate");
      };

      try {
        let allocs = await dao.getAggregateAllocation();
        // Clear and repopulate
        for ((token, _) in Map.entries(aggregateAllocation)) {
          ignore Map.remove(aggregateAllocation, phash, token);
        };
        for ((token, vp) in allocs.vals()) {
          Map.set(aggregateAllocation, phash, token, vp);
        };
      } catch (e) {
        logger.warn("TIMER", "Allocation sync failed: " # Error.message(e), "balanceUpdate");
      };

      startBalanceUpdateTimer();
    });
  };

  // --- Timer: ICPSwap Pool Discovery (5 hours) ---
  private func startPoolDiscoveryTimer<system>() {
    switch (poolDiscoveryTimerId) { case (?id) { cancelTimer(id) }; case null {} };
    poolDiscoveryTimerId := ?setTimer<system>(#nanoseconds(5 * 3600 * 1_000_000_000), func() : async () {
      try {
        let result = await ICPSwap.getAllPools();
        switch (result) {
          case (#ok(pools)) {
            for (pool in pools.vals()) {
              Map.set(ICPswapPools, phash, pool.canisterId, pool);
            };
            logger.info("TIMER", "Pool discovery: " # Nat.toText(pools.size()) # " pools", "poolDiscovery");
          };
          case (#err(e)) {
            logger.warn("TIMER", "Pool discovery failed: " # e, "poolDiscovery");
          };
        };
      } catch (e) {
        logger.warn("TIMER", "Pool discovery error: " # Error.message(e), "poolDiscovery");
      };
      startPoolDiscoveryTimer();
    });
  };

  // --- Timer: Block & Deposit Cleanup (24 hours) ---
  private func startBlockCleanupTimer<system>() {
    switch (blockCleanupTimerId) { case (?id) { cancelTimer(id) }; case null {} };
    blockCleanupTimerId := ?setTimer<system>(#nanoseconds(24 * 3600 * 1_000_000_000), func() : async () {
      let cutoff = now() - ONE_MONTH_NS;

      // Clean old block dedup entries
      let keysToRemove = Vector.new<Text>();
      for ((key, ts) in Map.entries(blocksDone)) {
        if (ts < cutoff) Vector.add(keysToRemove, key);
      };
      for (key in Vector.vals(keysToRemove)) {
        ignore Map.remove(blocksDone, thash, key);
      };

      // Clean expired deposits
      cleanupExpiredDeposits();

      // Clean deposit stats
      cleanupDepositStats();

      logger.info("TIMER", "Cleanup complete, block dedup entries cleaned", "blockCleanup");
      startBlockCleanupTimer();
    });
  };

  // --- Timer: NAV Snapshot (15 min) ---
  private func startNavSnapshotTimer<system>() {
    switch (navSnapshotTimerId) { case (?id) { cancelTimer(id) }; case null {} };
    navSnapshotTimerId := ?setTimer<system>(#nanoseconds(15 * 60 * 1_000_000_000), func() : async () {
      ignore await calculateNAV();
      recordNavSnapshot(#Scheduled);
      startNavSnapshotTimer();
    });
  };

  // --- Timer: Circuit Breaker Check (5 min) ---
  private func startCircuitBreakerTimer<system>() {
    switch (circuitBreakerTimerId) { case (?id) { cancelTimer(id) }; case null {} };
    circuitBreakerTimerId := ?setTimer<system>(#nanoseconds(5 * 60 * 1_000_000_000), func() : async () {
      if (not circuitBreakerActive and Vector.size(navHistory) >= 2) {
        let currentNav = switch (cachedNAV) {
          case (?n) { n.navPerTokenE8s };
          case null { 0 };
        };

        if (currentNav > 0) {
          let windowCutoff = now() - navDropTimeWindowNS;
          var maxNavInWindow : Nat = currentNav;

          var i = Vector.size(navHistory);
          while (i > 0) {
            i -= 1;
            let snap = Vector.get(navHistory, i);
            if (snap.timestamp < windowCutoff) { i := 0 } // break
            else {
              if (snap.navPerTokenE8s > maxNavInWindow) {
                maxNavInWindow := snap.navPerTokenE8s;
              };
            };
          };

          if (maxNavInWindow > currentNav) {
            let dropPercent = Float.fromInt(maxNavInWindow - currentNav) / Float.fromInt(maxNavInWindow) * 100.0;
            if (dropPercent >= navDropThresholdPercent) {
              circuitBreakerActive := true;
              logger.warn("CIRCUIT_BREAKER", "NAV drop detected: " # Float.toText(dropPercent) # "% (threshold: " # Float.toText(navDropThresholdPercent) # "%)", "circuitBreakerCheck");
            };
          };
        };
      };
      startCircuitBreakerTimer();
    });
  };

  // --- Timer: Transfer Queue Processing (30 sec) ---
  private func startTransferQueueTimer<system>() {
    switch (transferQueueTimerId) { case (?id) { cancelTimer(id) }; case null {} };
    transferQueueTimerId := ?setTimer<system>(#nanoseconds(30 * 1_000_000_000), func() : async () {
      if (Vector.size(pendingTransfers) > 0) {
        await processTransferQueue();
      };
      startTransferQueueTimer();
    });
  };

  // --- Start All Timers ---
  private func startAllTimers<system>() {
    startBalanceUpdateTimer();
    startPoolDiscoveryTimer();
    startBlockCleanupTimer();
    startNavSnapshotTimer();
    startCircuitBreakerTimer();
    startTransferQueueTimer();
  };

  // --- Lifecycle: Post-upgrade ---
  system func postupgrade() {
    // Reconstruct transient actor reference
    nachosLedger := switch (nachosLedgerPrincipal) {
      case (?p) { ?(actor (Principal.toText(p)) : ICRC1.FullInterface) };
      case null { null };
    };

    // Restart all timers
    startAllTimers();

    logger.info("LIFECYCLE", "Post-upgrade complete, timers restarted", "postupgrade");
  };

  // --- Initial timer start (first deploy) ---
  ignore setTimer<system>(#nanoseconds(0), func() : async () {
    startAllTimers();
    logger.info("LIFECYCLE", "Initial timer startup complete", "init");
  });

};
