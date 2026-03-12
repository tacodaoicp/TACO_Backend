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
import SwapTypes "../swap/swap_types";
import SpamProtection "../helper/spam_protection";
import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import Cycles "mo:base/ExperimentalCycles";
import SHA224 "../helper/SHA224";
import CRC32 "../helper/CRC32";
// import Migration "migration"; // Migration already applied (ForwardToPortfolio variant)

// (with migration = Migration.migrate)
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

  // Query-typed actor refs for composite query calls (same canister, query interface)
  transient let treasuryQuery = actor (Principal.toText(TREASURY_ID)) : actor {
    getTokenDetails : shared query () -> async [(Principal, TokenDetails)];
    getTokenDetailsCache : shared query () -> async { timestamp : Int; icpPriceUSD : Float; tokenDetails : [(Principal, TokenDetails)] };
  };

  transient let dao = actor (Principal.toText(DAO_BACKEND_ID)) : actor {
    getAggregateAllocation : shared () -> async [(Principal, Nat)];
  };

  transient let daoQuery = actor (Principal.toText(DAO_BACKEND_ID)) : actor {
    getAggregateAllocation : shared query () -> async [(Principal, Nat)];
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
  stable var MAX_PRICE_STALENESS_NS : Int = 30 * 1_000_000_000; // 30 seconds
  stable var PRICE_HISTORY_WINDOW : Int = 2 * 3600 * 1_000_000_000; // 2 hours
  stable var maxSlippageBasisPoints : Nat = 50; // 0.50%

  // --- Rate Limits ---
  stable var maxNachosBurnPer4Hours : Nat = 100_000_000_000; // 1000 NACHOS
  stable var maxMintICPWorthPer4Hours : Nat = 100_000_000_000; // 1000 ICP worth
  stable var maxMintOpsPerUser4Hours : Nat = 20;
  stable var maxBurnOpsPerUser4Hours : Nat = 20;
  stable var maxMintICPPerUser4Hours : Nat = 100_000_000_000; // 1000 ICP per user per 4h
  stable var maxBurnNachosPerUser4Hours : Nat = 100_000_000_000; // 1000 NACHOS per user per 4h
  stable var maxMintAmountICP : Nat = 0; // 0 = no max per single mint (disabled)
  stable var maxBurnAmountNachos : Nat = 0; // 0 = no max per single burn (disabled)

  // --- Circuit Breaker ---
  stable var circuitBreakerActive : Bool = false;
  stable var navDropThresholdPercent : Float = 10.0;
  stable var navDropTimeWindowNS : Nat = 3600 * 1_000_000_000; // 1 hour

  // --- Portfolio Share Config ---
  stable var portfolioShareMaxDeviationBP : Nat = 500; // 5%

  // --- Cancellation Fee ---
  stable var cancellationFeeMultiplier : Nat = 10;

  // --- Subaccounts ---
  let NachosTreasurySubaccount : Nat8 = 2; // Subaccount on TREASURY where users send tokens for minting
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

  // --- Operation History ---
  stable let mintHistory = Vector.new<MintRecord>();
  stable let burnHistory = Vector.new<BurnRecord>();
  stable let navHistory = Vector.new<NavSnapshot>();
  stable let feeHistory = Vector.new<FeeRecord>();

  // --- Claimable Fee Tracking ---
  stable var accumulatedMintFeesICP : Nat = 0;
  stable var claimedMintFeesICP : Nat = 0;
  stable let accumulatedCancellationFees = Map.new<Principal, Nat>(); // token -> amount
  stable let claimedCancellationFees = Map.new<Principal, Nat>(); // token -> amount

  // --- Operation Counters ---
  stable var nextMintId : Nat = 0;
  stable var nextBurnId : Nat = 0;

  // --- NAV Cache ---
  stable var cachedNAV : ?CachedNAV = null;
  stable var cachedSupply : Nat = 0;
  stable var cachedSupplyTime : Int = 0;
  let SUPPLY_CACHE_TTL_NS : Int = 10_000_000_000; // 10 seconds

  // --- Price Refresh Cache (skip redundant treasury calls within 30s) ---
  stable var lastLocalPriceRefreshTime : Int = 0;

  // --- ICPSwap Pool Cache (for vault-local price refresh fallback) ---
  stable let icpSwapPoolCache = Map.new<Principal, Principal>(); // token principal -> pool canister ID

  // --- Concurrent Operation Locks ---
  stable let operationLocks = Map.new<Principal, Int>(); // caller -> lock timestamp
  let LOCK_TIMEOUT_NS : Int = 5 * 60 * 1_000_000_000; // 5 minutes (mint/burn flows can have 5+ async calls)

  // --- Rate Limit Trackers ---
  stable let mintRateTracker = Vector.new<(Int, Nat)>(); // (timestamp, icpValue)
  stable let burnRateTracker = Vector.new<(Int, Nat)>(); // (timestamp, nachosAmount)
  stable let userMintOps = Map.new<Principal, Vector.Vector<Int>>(); // caller -> timestamps
  stable let userBurnOps = Map.new<Principal, Vector.Vector<Int>>(); // caller -> timestamps
  stable let userMintValues = Map.new<Principal, Vector.Vector<(Int, Nat)>>(); // caller -> (timestamp, icpValue)
  stable let userBurnValues = Map.new<Principal, Vector.Vector<(Int, Nat)>>(); // caller -> (timestamp, nachosAmount)

  // --- Timer IDs ---
  transient var periodicSyncTimerId : ?Nat = null;
  transient var blockCleanupTimerId : ?Nat = null;
  transient var transferQueueTimerId : ?Nat = null;
  transient var lastMintBurnTime : Int = 0; // skip periodic treasury sync when mint/burn just refreshed data

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
  private func verifyICPBlock(blockNumber : Nat, expectedFrom : Principal, expectedRecipient : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
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
              let expectedToAid = computeAccountIdentifier(expectedRecipient, ?subaccountByteToBlob(expectedToSubaccount));

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
  private func verifyICRC1Block(tokenPrincipal : Principal, blockNumber : Nat, expectedFrom : Principal, expectedRecipient : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
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
            if (xfer.to.owner != expectedRecipient) return #err("Recipient owner mismatch");
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
                  if (xfer.to.owner != TREASURY_ID) return #err("Recipient owner mismatch (archive)");
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
  private func verifyICRC3Block(tokenPrincipal : Principal, blockNumber : Nat, expectedFrom : Principal, expectedRecipient : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
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

              if (toOwner != expectedRecipient) return #err("Recipient mismatch");

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
  private func verifyBlock(tokenPrincipal : Principal, blockNumber : Nat, expectedFrom : Principal, expectedRecipient : Principal, expectedToSubaccount : Nat8) : async Result.Result<{ amount : Nat; from : Principal }, Text> {
    let tokenType : ?TokenType = switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
      case (?details) { ?details.tokenType };
      case null {
        // ICP special case
        if (tokenPrincipal == ICPprincipal) { ?#ICP }
        else {
          // NACHOS ledger special case (uses get_transactions / ICRC-1 format)
          switch (nachosLedgerPrincipal) {
            case (?nlp) { if (tokenPrincipal == nlp) { ?#ICRC12 } else { null } };
            case null { null };
          };
        };
      };
    };

    switch (tokenType) {
      case null { #err("Unknown token") };
      case (?#ICP) { await verifyICPBlock(blockNumber, expectedFrom, expectedRecipient, expectedToSubaccount) };
      case (?#ICRC12) { await verifyICRC1Block(tokenPrincipal, blockNumber, expectedFrom, expectedRecipient, expectedToSubaccount) };
      case (?#ICRC3) { await verifyICRC3Block(tokenPrincipal, blockNumber, expectedFrom, expectedRecipient, expectedToSubaccount) };
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
      case (#ForwardToPortfolio) { "ForwardToPortfolio" };
    };
  };

  // Forward net deposit amounts from treasury subaccount 2 to default subaccount (0)
  // so that treasury balance queries include them in portfolio value.
  // Fee portion stays in subaccount 2 for admin claiming.
  private func forwardDepositsToPortfolio<system>(
    caller : Principal,
    deposits : [TokenDeposit],
    netValueICP : Nat,
    totalDepositValueICP : Nat,
    mintId : Nat,
  ) {
    if (totalDepositValueICP == 0) return;
    let netRatioE8s : Nat = (netValueICP * ONE_E8S) / totalDepositValueICP;

    for (deposit in deposits.vals()) {
      let netTokenAmount = (deposit.amount * netRatioE8s) / ONE_E8S;
      // Only forward if amount exceeds the token's transfer fee
      let tokenFee = switch (Map.get(tokenDetailsMap, phash, deposit.token)) {
        case (?d) { d.tokenTransferFee };
        case null { 10000 }; // conservative fallback
      };
      if (netTokenAmount > tokenFee) {
        ignore createTransferTask(
          caller,
          #accountId({ owner = TREASURY_ID; subaccount = null }),
          netTokenAmount,
          deposit.token,
          NachosTreasurySubaccount,
          #ForwardToPortfolio,
          mintId,
        );
        // Optimistic balance: reflect forwarded deposit in tokenDetailsMap
        // so subsequent calculateNAV() sees the updated portfolio value
        switch (Map.get(tokenDetailsMap, phash, deposit.token)) {
          case (?details) {
            Map.set(tokenDetailsMap, phash, deposit.token, { details with balance = details.balance + netTokenAmount });
          };
          case null {};
        };
      };
    };
  };

  private func createTransferTask<system>(
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
    ensureTransferQueueRunning();

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
  private func cancelDepositAndRefund<system>(blockKey : Text, caller : Principal, refundAmount : Nat, tokenPrincipal : Principal, fromSubaccount : Nat8, opType : TransferOperationType, opId : Nat) : Nat {
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

    // Track cancellation fee for admin claiming
    let prevCancelFees = switch (Map.get(accumulatedCancellationFees, phash, tokenPrincipal)) { case (?v) v; case null 0 };
    Map.set(accumulatedCancellationFees, phash, tokenPrincipal, prevCancelFees + cancellationFee);

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
      let supply = if (now() - cachedSupplyTime < SUPPLY_CACHE_TTL_NS and cachedSupply > 0) {
        cachedSupply;
      } else {
        let s = await ledger.icrc1_total_supply();
        cachedSupply := s;
        cachedSupplyTime := now();
        s;
      };
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

  // Direct DEX actor references — WITHOUT query keyword so calls go through update path
  // (query calls from update contexts fail with "could not perform remote call" on IC)
  transient let kong : actor {
    swap_amounts : shared (Text, Nat, Text) -> async SwapTypes.SwapAmountsResult;
  } = actor ("2ipq2-uqaaa-aaaar-qailq-cai");

  transient let icpSwapFactory : actor {
    getPools : shared () -> async { #ok : [SwapTypes.PoolData]; #err : Text };
  } = actor ("4mmnk-kiaaa-aaaag-qbllq-cai");

  // Discover ICPSwap pools that pair with ICP — populates icpSwapPoolCache
  private func discoverICPSwapPools() : async () {
    try {
      let result = await icpSwapFactory.getPools();
      switch (result) {
        case (#ok(pools)) {
          for (pool in pools.vals()) {
            let t0 = Principal.fromText(pool.token0.address);
            let t1 = Principal.fromText(pool.token1.address);
            if (t0 == ICPprincipal) {
              Map.set(icpSwapPoolCache, phash, t1, pool.canisterId);
            } else if (t1 == ICPprincipal) {
              Map.set(icpSwapPoolCache, phash, t0, pool.canisterId);
            };
          };
          logger.info("POOLS", "Discovered " # Nat.toText(Map.size(icpSwapPoolCache)) # " ICPSwap ICP pools", "discoverICPSwapPools");
        };
        case (#err(e)) {
          logger.warn("POOLS", "ICPSwap pool discovery failed: " # e, "discoverICPSwapPools");
        };
      };
    } catch (e) {
      logger.warn("POOLS", "ICPSwap pool discovery exception: " # Error.message(e), "discoverICPSwapPools");
    };
  };

  // Vault-local price refresh: queries BOTH ICPSwap and KongSwap directly.
  // Fires ALL futures upfront with (with timeout), then awaits — same pattern as treasury's syncPriceWithDEX.
  private func refreshPricesLocally() : async Bool {
    // Auto-discover pools on first use
    if (Map.size(icpSwapPoolCache) == 0) { await discoverICPSwapPools() };

    var anyUpdated = false;

    // Snapshot tokens to refresh
    let tokensToRefresh = Vector.new<(Principal, TokenDetails)>();
    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (token != ICPprincipal and details.Active and not details.isPaused) {
        Vector.add(tokensToRefresh, (token, details));
      };
    };

    logger.info("PRICE", "Starting parallel refresh for " # Nat.toText(Vector.size(tokensToRefresh)) # " tokens, icpSwapPoolCache=" # Nat.toText(Map.size(icpSwapPoolCache)), "refreshPricesLocally");

    // ── Fire ALL Kong + ICPSwap futures in parallel (same as treasury) ──
    let kongFutures = Map.new<Principal, async SwapTypes.SwapAmountsResult>();
    let icpSwapFutures = Map.new<Principal, async Result.Result<SwapTypes.PoolMetadata, SwapTypes.ICPSwapError>>();
    var kongCount : Nat = 0;
    var icpSwapCount : Nat = 0;

    for ((token, details) in Vector.vals(tokensToRefresh)) {
      let oneUnit = 10 ** details.tokenDecimals;
      let kongFut = kong.swap_amounts("IC." # details.tokenSymbol, oneUnit, "IC.ICP");
      Map.set(kongFutures, phash, token, kongFut);
      kongCount += 1;

      switch (Map.get(icpSwapPoolCache, phash, token)) {
        case (?poolId) {
          let pool : actor { metadata : shared () -> async Result.Result<SwapTypes.PoolMetadata, SwapTypes.ICPSwapError> } = actor (Principal.toText(poolId));
          let icpFut = pool.metadata();
          Map.set(icpSwapFutures, phash, token, icpFut);
          icpSwapCount += 1;
        };
        case null {};
      };
    };

    logger.info("PRICE", "Fired " # Nat.toText(kongCount) # " Kong + " # Nat.toText(icpSwapCount) # " ICPSwap futures", "refreshPricesLocally");

    // ── Await all futures and process results per token ──
    label tokenLoop for ((token, details) in Vector.vals(tokensToRefresh)) {
      var kongPrice : ?Float = null;
      var icpSwapPrice : ?Float = null;

      // Await KongSwap
      switch (Map.get(kongFutures, phash, token)) {
        case (?kongFut) {
          try {
            let result = await kongFut;
            switch (result) {
              case (#Ok(quote)) {
                if (quote.mid_price > 0.0 and quote.mid_price <= 100000.0) {
                  kongPrice := ?quote.mid_price;
                };
              };
              case (#Err(_)) {};
            };
          } catch (e) {
            logger.warn("PRICE", "Kong " # details.tokenSymbol # " error: " # Error.message(e), "refreshPricesLocally");
          };
        };
        case null {};
      };

      // Await ICPSwap
      switch (Map.get(icpSwapFutures, phash, token)) {
        case (?icpFut) {
          try {
            let metaResult = await icpFut;
            switch (metaResult) {
              case (#ok(metadata)) {
                let sqrtPriceX96_squared = metadata.sqrtPriceX96 * metadata.sqrtPriceX96;
                let rawPrice = Float.fromInt(sqrtPriceX96_squared) / Float.fromInt(2 ** 192);

                let tokenAddress = Principal.toText(token);
                let icpDecimals : Int = 8;
                let tokDecimals : Int = details.tokenDecimals;

                // Determine direction and apply decimal adjustment (same as treasury)
                let tokenICPPrice : Float = if (tokenAddress == metadata.token0.address and ICPprincipalText == metadata.token1.address) {
                  let adjustment = Float.fromInt((10 : Nat) ** Int.abs(tokDecimals - icpDecimals));
                  if (tokDecimals > icpDecimals) { rawPrice * adjustment }
                  else if (tokDecimals < icpDecimals) { rawPrice / adjustment }
                  else { rawPrice };
                } else if (tokenAddress == metadata.token1.address and ICPprincipalText == metadata.token0.address) {
                  let adjustment = Float.fromInt((10 : Nat) ** Int.abs(icpDecimals - tokDecimals));
                  let adjusted = if (tokDecimals > icpDecimals) { rawPrice / adjustment }
                    else if (tokDecimals < icpDecimals) { rawPrice * adjustment }
                    else { rawPrice };
                  if (adjusted > 0.0) { 1.0 / adjusted } else { 0.0 };
                } else { 0.0 };

                if (tokenICPPrice > 0.0 and tokenICPPrice >= 0.000001 and tokenICPPrice <= 100000.0) {
                  icpSwapPrice := ?tokenICPPrice;
                };
              };
              case (#err(_)) {};
            };
          } catch (e) {
            logger.warn("PRICE", "ICPSwap " # details.tokenSymbol # " error: " # Error.message(e), "refreshPricesLocally");
          };
        };
        case null {};
      };

      // Merge prices — same logic as treasury:
      // Variance <1%: average; >=1%: closest to last known
      let finalPrice : ?Float = switch (kongPrice, icpSwapPrice) {
        case (?kp, ?ip) {
          let maxP = Float.max(kp, ip);
          let minP = Float.min(kp, ip);
          let variance = if (maxP > 0.0) { (maxP - minP) / maxP } else { 1.0 };
          if (variance <= 0.01) {
            ?((kp + ip) / 2.0);
          } else {
            let lastPriceFloat = Float.fromInt(details.priceInICP) / 100_000_000.0;
            let kongDiff = Float.abs(kp - lastPriceFloat);
            let icpSwapDiff = Float.abs(ip - lastPriceFloat);
            if (kongDiff <= icpSwapDiff) { ?kp } else { ?ip };
          };
        };
        case (?kp, null) { if (kp > 0.0) { ?kp } else { null } };
        case (null, ?ip) { if (ip > 0.0) { ?ip } else { null } };
        case (null, null) { null };
      };

      switch (finalPrice) {
        case (?price) {
          let scaledPrice = price * 100_000_000.0;
          if (price > 0.0 and not Float.isNaN(scaledPrice) and scaledPrice < 9.0e18) {
            let priceE8s = Int.abs(Float.toInt(scaledPrice));
            Map.set(tokenDetailsMap, phash, token, { details with priceInICP = priceE8s; lastTimeSynced = now() });
            anyUpdated := true;
          };
        };
        case null {};
      };
    };

    // Update ICP's lastTimeSynced and cache timestamp
    if (anyUpdated) {
      let finalTime = now();
      switch (Map.get(tokenDetailsMap, phash, ICPprincipal)) {
        case (?icpDetails) {
          Map.set(tokenDetailsMap, phash, ICPprincipal, { icpDetails with lastTimeSynced = finalTime });
        };
        case null {};
      };
      lastLocalPriceRefreshTime := finalTime;
    };
    anyUpdated;
  };

  private func getPausedPortfolioTokens() : [{ token : Principal; symbol : Text }] {
    let paused = Vector.new<{ token : Principal; symbol : Text }>();
    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and details.isPaused) {
        Vector.add(paused, { token; symbol = details.tokenSymbol });
      };
    };
    Vector.toArray(paused);
  };

  private func getHistoricalLowPrice(token : Principal, window : Int) : Nat {
    let cutoff = now() - window;
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

  // Conservative price: min(treasury, historical low within window). Protects vault from overvalued deposits.
  // Default window: PRICE_HISTORY_WINDOW (2h for single-token minting)
  private func getConservativePrice(token : Principal) : Nat {
    getConservativePriceWithWindow(token, PRICE_HISTORY_WINDOW);
  };

  private func getConservativePriceWithWindow(token : Principal, window : Int) : Nat {
    let treasuryPrice = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) { details.priceInICP };
      case null { return 0 };
    };

    let historicalLow = getHistoricalLowPrice(token, window);

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

    // Clean per-user value trackers
    let mintKeysToRemove = Vector.new<Principal>();
    for ((principal, vals) in Map.entries(userMintValues)) {
      let fresh = Vector.new<(Int, Nat)>();
      for ((ts, v) in Vector.vals(vals)) { if (ts >= cutoff) Vector.add(fresh, (ts, v)) };
      if (Vector.size(fresh) == 0) { Vector.add(mintKeysToRemove, principal) }
      else {
        while (Vector.size(vals) > 0) { ignore Vector.removeLast(vals) };
        for (e in Vector.vals(fresh)) { Vector.add(vals, e) };
      };
    };
    for (k in Vector.vals(mintKeysToRemove)) { ignore Map.remove(userMintValues, phash, k) };

    let burnKeysToRemove = Vector.new<Principal>();
    for ((principal, vals) in Map.entries(userBurnValues)) {
      let fresh = Vector.new<(Int, Nat)>();
      for ((ts, v) in Vector.vals(vals)) { if (ts >= cutoff) Vector.add(fresh, (ts, v)) };
      if (Vector.size(fresh) == 0) { Vector.add(burnKeysToRemove, principal) }
      else {
        while (Vector.size(vals) > 0) { ignore Vector.removeLast(vals) };
        for (e in Vector.vals(fresh)) { Vector.add(vals, e) };
      };
    };
    for (k in Vector.vals(burnKeysToRemove)) { ignore Map.remove(userBurnValues, phash, k) };
  };

  // Combined check + record: records optimistically BEFORE awaits to prevent
  // global rate limit bypass by concurrent users interleaving at await points.
  // If the mint later fails, the rate slot stays consumed (conservative but safe).
  private func checkAndRecordMintRateLimit(caller : Principal, valueICP : Nat) : Result.Result<(), NachosError> {
    if (isRateLimitExempt(caller)) return #ok(());

    let cutoff = now() - FOUR_HOURS_NS;

    // Global ICP value limit (only count entries within 4h window)
    var totalMintValue : Nat = 0;
    for (entry in Vector.vals(mintRateTracker)) {
      if (entry.0 >= cutoff) totalMintValue += entry.1;
    };
    if (totalMintValue + valueICP > maxMintICPWorthPer4Hours) {
      return #err(#MintLimitExceeded({ maxPer4Hours = maxMintICPWorthPer4Hours; recentMints = totalMintValue; requested = valueICP }));
    };

    // Per-user ops limit
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

    // Per-user ICP value limit
    switch (Map.get(userMintValues, phash, caller)) {
      case (?vals) {
        var recentValue : Nat = 0;
        for ((ts, v) in Vector.vals(vals)) {
          if (ts >= cutoff) recentValue += v;
        };
        if (recentValue + valueICP > maxMintICPPerUser4Hours) {
          return #err(#UserMintLimitExceeded({ maxPer4Hours = maxMintICPPerUser4Hours; recentMints = recentValue; requested = valueICP }));
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
    switch (Map.get(userMintValues, phash, caller)) {
      case (?vals) { Vector.add(vals, (now(), valueICP)) };
      case null {
        let vals = Vector.new<(Int, Nat)>();
        Vector.add(vals, (now(), valueICP));
        Map.set(userMintValues, phash, caller, vals);
      };
    };

    #ok(());
  };

  // Combined check + record for burn rate limits (same optimistic pattern).
  private func checkAndRecordBurnRateLimit(caller : Principal, nachosAmount : Nat) : Result.Result<(), NachosError> {
    if (isRateLimitExempt(caller)) return #ok(());

    let cutoff = now() - FOUR_HOURS_NS;

    // Global nachos burn limit (only count entries within 4h window)
    var totalBurned : Nat = 0;
    for (entry in Vector.vals(burnRateTracker)) {
      if (entry.0 >= cutoff) totalBurned += entry.1;
    };
    if (totalBurned + nachosAmount > maxNachosBurnPer4Hours) {
      return #err(#BurnLimitExceeded({ maxPer4Hours = maxNachosBurnPer4Hours; recentBurns = totalBurned; requested = nachosAmount }));
    };

    // Per-user ops limit
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

    // Per-user NACHOS burn value limit
    switch (Map.get(userBurnValues, phash, caller)) {
      case (?vals) {
        var recentValue : Nat = 0;
        for ((ts, v) in Vector.vals(vals)) {
          if (ts >= cutoff) recentValue += v;
        };
        if (recentValue + nachosAmount > maxBurnNachosPerUser4Hours) {
          return #err(#UserBurnLimitExceeded({ maxPer4Hours = maxBurnNachosPerUser4Hours; recentBurns = recentValue; requested = nachosAmount }));
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
    switch (Map.get(userBurnValues, phash, caller)) {
      case (?vals) { Vector.add(vals, (now(), nachosAmount)) };
      case null {
        let vals = Vector.new<(Int, Nat)>();
        Vector.add(vals, (now(), nachosAmount));
        Map.set(userBurnValues, phash, caller, vals);
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

    // Price refresh: skip if refreshed within 30s and still fresh
    var priceRefreshOk = false;
    if (now() - lastLocalPriceRefreshTime < 30_000_000_000 and arePricesFresh()) {
      priceRefreshOk := true;
    } else {
      // Primary: refresh prices via treasury (parallel DEX calls with sufficient cycles)
      try {
        switch (await treasury.refreshPricesAndGetDetails()) {
          case (#ok(result)) {
            let refreshTime = now();
            for ((token, detail) in result.tokenDetails.vals()) {
              // Preserve optimistic balance — treasury doesn't know about in-flight deposits/withdrawals
              let preservedBalance = switch (Map.get(tokenDetailsMap, phash, token)) {
                case (?existing) { existing.balance };
                case null { detail.balance };
              };
              Map.set(tokenDetailsMap, phash, token, { detail with balance = preservedBalance; lastTimeSynced = refreshTime });
            };
            lastLocalPriceRefreshTime := refreshTime;
            priceRefreshOk := true;
            logger.info("PRE_CHECK", "Refreshed prices via treasury", "performSharedPreChecks");
          };
          case (#err(msg)) {
            logger.warn("PRE_CHECK", "Treasury price refresh failed: " # msg, "performSharedPreChecks");
          };
        };
      } catch (e) {
        logger.warn("PRE_CHECK", "Treasury price refresh exception: " # Error.message(e), "performSharedPreChecks");
      };

      // Fallback: refresh prices directly via ICPSwap + KongSwap if treasury failed
      if (not priceRefreshOk) {
        try {
          let success = await refreshPricesLocally();
          if (success) {
            priceRefreshOk := true;
            logger.info("PRE_CHECK", "Refreshed prices via direct DEX fallback (ICPSwap + KongSwap)", "performSharedPreChecks");
          } else {
            logger.warn("PRE_CHECK", "Direct DEX fallback returned no price updates", "performSharedPreChecks");
          };
        } catch (e) {
          logger.warn("PRE_CHECK", "Direct DEX fallback failed: " # Error.message(e), "performSharedPreChecks");
        };
      };
    };

    // Check price staleness
    if (not arePricesFresh()) {
      releaseLock(caller);
      return #err(#PriceStale);
    };

    // Check for paused portfolio tokens — block MINTING (not burning) if any token paused
    // Paused tokens have stale prices which would corrupt NAV for minting
    // Burning is allowed: proportional share distribution works regardless of exact prices
    if (isMint) {
      let pausedTokens = getPausedPortfolioTokens();
      if (pausedTokens.size() > 0) {
        releaseLock(caller);
        return #err(#PortfolioTokenPaused({ pausedTokens }));
      };
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
    let verifyResult = await verifyICPBlock(blockNumber, caller, TREASURY_ID, NachosTreasurySubaccount);
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
    lastMintBurnTime := now();

    // Forward full deposit to treasury default subaccount (no fee on genesis)
    forwardDepositsToPortfolio(caller, deposits, depositAmount, depositAmount, 0);

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
    let verifyResult = await verifyICPBlock(blockNumber, caller, TREASURY_ID, NachosTreasurySubaccount);
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

    // Check maximum value per operation
    if (maxMintAmountICP > 0 and depositAmount > maxMintAmountICP) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, ICPprincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#AboveMaximumValue({ max = maxMintAmountICP; requested = depositAmount }));
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
      accumulatedMintFeesICP += feeValue;
    };

    let mintId = nextMintId;
    nextMintId += 1;

    // Optimistic NAV: reflect net deposit before treasury sync (fee stays in sub2)
    let optPortfolio = nav.portfolioValueICP + netValue;
    let optSupply = nav.nachosSupply + nachosAmount;
    cachedNAV := ?{
      navPerTokenE8s = if (optSupply > 0) { (optPortfolio * ONE_E8S) / optSupply } else { INITIAL_NAV_PER_TOKEN_E8S };
      portfolioValueICP = optPortfolio;
      nachosSupply = optSupply;
      timestamp = now();
    };
    cachedSupply := optSupply;
    cachedSupplyTime := now();
    recordNavSnapshot(#Mint);
    lastMintBurnTime := now();

    // Forward net deposit to treasury default subaccount (fee stays in sub2)
    forwardDepositsToPortfolio(caller, deposits, netValue, depositAmount, mintId);

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
    let verifyResult = await verifyBlock(tokenPrincipal, blockNumber, caller, TREASURY_ID, NachosTreasurySubaccount);
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

    if (maxMintAmountICP > 0 and depositValueICP > maxMintAmountICP) {
      ignore cancelDepositAndRefund(blockKey, caller, depositAmount, tokenPrincipal, NachosTreasurySubaccount, #MintReturn, blockNumber);
      releaseLock(caller);
      return #err(#AboveMaximumValue({ max = maxMintAmountICP; requested = depositValueICP }));
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
      accumulatedMintFeesICP += feeValue;
    };

    let mintId = nextMintId;
    nextMintId += 1;

    // Optimistic NAV: reflect net deposit before treasury sync (fee stays in sub2)
    let optPortfolio = nav.portfolioValueICP + netValue;
    let optSupply = nav.nachosSupply + nachosAmount;
    cachedNAV := ?{
      navPerTokenE8s = if (optSupply > 0) { (optPortfolio * ONE_E8S) / optSupply } else { INITIAL_NAV_PER_TOKEN_E8S };
      portfolioValueICP = optPortfolio;
      nachosSupply = optSupply;
      timestamp = now();
    };
    cachedSupply := optSupply;
    cachedSupplyTime := now();
    recordNavSnapshot(#Mint);
    lastMintBurnTime := now();

    // Forward net deposit to treasury default subaccount (fee stays in sub2)
    forwardDepositsToPortfolio(caller, deposits, netValue, usedValueICP, mintId);

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
      let verifyResult = await verifyBlock(info.token, info.blockNumber, caller, TREASURY_ID, NachosTreasurySubaccount);
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

    // Calculate value per token using 30-minute conservative prices (min of treasury + 30m low)
    let THIRTY_MINUTES_NS : Int = 30 * 60 * 1_000_000_000;
    let tokenDeposits = Vector.new<TokenDeposit>();
    var totalValueICP : Nat = 0;

    for (vd in Vector.vals(verifiedDeposits)) {
      let price = getConservativePriceWithWindow(vd.token, THIRTY_MINUTES_NS);
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

    if (maxMintAmountICP > 0 and totalValueICP > maxMintAmountICP) {
      for (vd in Vector.vals(verifiedDeposits)) {
        ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
      };
      releaseLock(caller);
      return #err(#AboveMaximumValue({ max = maxMintAmountICP; requested = totalValueICP }));
    };

    // Validate portfolio share proportions
    // Uses treasury prices (not conservative) for both sides so proportions are consistent
    let portfolioValue = calculatePortfolioValueICP();
    if (portfolioValue > 0) {
      // Aggregate deposits by token (handles multiple blocks for same token)
      let aggregatedByToken = Map.new<Principal, { valueICP : Nat }>();
      var totalDepositValueTreasury : Nat = 0;
      for (vd in Vector.vals(verifiedDeposits)) {
        let treasuryPrice = switch (Map.get(tokenDetailsMap, phash, vd.token)) {
          case (?d) { d.priceInICP };
          case null { 0 };
        };
        let decimals = switch (Map.get(tokenDetailsMap, phash, vd.token)) {
          case (?d) { d.tokenDecimals };
          case null { 8 };
        };
        let valICP = (vd.amount * treasuryPrice) / (10 ** decimals);
        totalDepositValueTreasury += valICP;
        switch (Map.get(aggregatedByToken, phash, vd.token)) {
          case (?existing) { Map.set(aggregatedByToken, phash, vd.token, { valueICP = existing.valueICP + valICP }) };
          case null { Map.set(aggregatedByToken, phash, vd.token, { valueICP = valICP }) };
        };
      };

      // Build expected proportions for error reporting
      let expectedProportions = Vector.new<{ token : Principal; basisPoints : Nat }>();
      for ((tok, det) in Map.entries(tokenDetailsMap)) {
        if (det.Active and det.balance > 0 and det.priceInICP > 0) {
          let tokVal = (det.balance * det.priceInICP) / (10 ** det.tokenDecimals);
          let bp = (tokVal * 10_000) / portfolioValue;
          if (bp > 0) Vector.add(expectedProportions, { token = tok; basisPoints = bp });
        };
      };

      for ((tok, agg) in Map.entries(aggregatedByToken)) {
        let expectedBP = switch (Map.get(tokenDetailsMap, phash, tok)) {
          case (?details) {
            let tokenValueInPortfolio = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
            (tokenValueInPortfolio * 10_000) / portfolioValue;
          };
          case null { 0 };
        };

        let actualBP = if (totalDepositValueTreasury > 0) { (agg.valueICP * 10_000) / totalDepositValueTreasury } else { 0 };

        let deviation = if (actualBP > expectedBP) { actualBP - expectedBP } else { expectedBP - actualBP };
        if (deviation > portfolioShareMaxDeviationBP) {
          for (vd in Vector.vals(verifiedDeposits)) {
            ignore cancelDepositAndRefund(vd.blockKey, caller, vd.amount, vd.token, NachosTreasurySubaccount, #MintReturn, vd.blockNumber);
          };
          let receivedProportions = Vector.new<{ token : Principal; basisPoints : Nat }>();
          for ((rTok, rAgg) in Map.entries(aggregatedByToken)) {
            let bp = if (totalDepositValueTreasury > 0) { (rAgg.valueICP * 10_000) / totalDepositValueTreasury } else { 0 };
            Vector.add(receivedProportions, { token = rTok; basisPoints = bp });
          };
          releaseLock(caller);
          return #err(#PortfolioShareMismatch({
            expected = Vector.toArray(expectedProportions);
            received = Vector.toArray(receivedProportions);
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
      accumulatedMintFeesICP += feeValue;
    };

    let mintId = nextMintId;
    nextMintId += 1;

    // Optimistic NAV: reflect net deposit before treasury sync (fee stays in sub2)
    let optPortfolio = nav.portfolioValueICP + netValue;
    let optSupply = nav.nachosSupply + nachosAmount;
    cachedNAV := ?{
      navPerTokenE8s = if (optSupply > 0) { (optPortfolio * ONE_E8S) / optSupply } else { INITIAL_NAV_PER_TOKEN_E8S };
      portfolioValueICP = optPortfolio;
      nachosSupply = optSupply;
      timestamp = now();
    };
    cachedSupply := optSupply;
    cachedSupplyTime := now();
    recordNavSnapshot(#Mint);
    lastMintBurnTime := now();

    // Forward net deposits to treasury default subaccount (fee stays in sub2)
    forwardDepositsToPortfolio(caller, depositsArr, netValue, totalValueICP, mintId);

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
    let verifyResult = await verifyBlock(nachosLedgerP, nachosBlockNumber, caller, Principal.fromActor(this), NachosDepositSubaccount);
    let nachosAmount = switch (verifyResult) {
      case (#ok({ amount })) { amount };
      case (#err(e)) {
        ignore Map.remove(blocksDone, thash, blockKey);
        releaseLock(caller);
        return #err(#BlockVerificationFailed(e));
      };
    };

    // Check maximum burn amount per operation
    if (maxBurnAmountNachos > 0 and nachosAmount > maxBurnAmountNachos) {
      if (nachosAmount > NACHOS_FEE) { ignore await returnNachosToUser(caller, nachosAmount - NACHOS_FEE) };
      releaseLock(caller);
      return #err(#AboveMaximumValue({ max = maxBurnAmountNachos; requested = nachosAmount }));
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

    // Optimistic balance: reflect tokens leaving portfolio in tokenDetailsMap
    // so subsequent calculateNAV() sees the reduced portfolio value
    for (ts in Vector.vals(tokensToSend)) {
      switch (Map.get(tokenDetailsMap, phash, ts.token)) {
        case (?details) {
          let totalDebit = ts.amount + details.tokenTransferFee;
          let newBalance = if (details.balance > totalDebit) { details.balance - totalDebit } else { 0 };
          Map.set(tokenDetailsMap, phash, ts.token, { details with balance = newBalance });
        };
        case null {};
      };
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

    // Optimistic NAV: reflect redemption leaving portfolio (fee stays in treasury)
    let optPortfolio = if (nav.portfolioValueICP > netValueICP) { nav.portfolioValueICP - netValueICP } else { 0 };
    let optSupply = if (nav.nachosSupply > nachosAmount) { nav.nachosSupply - nachosAmount } else { 0 };
    cachedNAV := ?{
      navPerTokenE8s = if (optSupply > 0) { (optPortfolio * ONE_E8S) / optSupply } else { INITIAL_NAV_PER_TOKEN_E8S };
      portfolioValueICP = optPortfolio;
      nachosSupply = optSupply;
      timestamp = now();
    };
    cachedSupply := optSupply;
    cachedSupplyTime := now();
    recordNavSnapshot(#Burn);
    lastMintBurnTime := now();
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
    switch (config.maxMintICPPerUser4Hours) { case (?v) { maxMintICPPerUser4Hours := v }; case null {} };
    switch (config.maxBurnNachosPerUser4Hours) { case (?v) { maxBurnNachosPerUser4Hours := v }; case null {} };
    switch (config.maxMintAmountICP) { case (?v) { maxMintAmountICP := v }; case null {} };
    switch (config.maxBurnAmountNachos) { case (?v) { maxBurnAmountNachos := v }; case null {} };

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

  // Claim accumulated mint fees (ICP) — admin sends to any recipient
  public shared ({ caller }) func claimMintFees(recipient : Principal, amount : Nat) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    let claimable = accumulatedMintFeesICP - claimedMintFeesICP;
    if (amount == 0) return #err("Amount must be > 0");
    if (amount > claimable) return #err("Insufficient claimable fees: " # Nat.toText(claimable) # " available, " # Nat.toText(amount) # " requested");

    // Deduct before await (prevent double-claim across await points)
    claimedMintFeesICP += amount;

    let taskId = createTransferTask(caller, #principal(recipient), amount, ICPprincipal, NachosTreasurySubaccount, #Recovery, claimedMintFeesICP);

    logger.info("FEES", "Admin claimed " # Nat.toText(amount) # " ICP mint fees -> " # Principal.toText(recipient) # " (task #" # Nat.toText(taskId) # ")", "claimMintFees");
    #ok("Claimed " # Nat.toText(amount) # " e8s ICP, transfer task #" # Nat.toText(taskId));
  };

  // Claim accumulated cancellation/recovery fees (per-token) — admin sends to any recipient
  public shared ({ caller }) func claimCancellationFees(recipient : Principal, tokenPrincipal : Principal, amount : Nat) : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    let accumulated = switch (Map.get(accumulatedCancellationFees, phash, tokenPrincipal)) { case (?v) v; case null 0 };
    let claimed = switch (Map.get(claimedCancellationFees, phash, tokenPrincipal)) { case (?v) v; case null 0 };
    let claimable = accumulated - claimed;
    if (amount == 0) return #err("Amount must be > 0");
    if (amount > claimable) return #err("Insufficient claimable fees: " # Nat.toText(claimable) # " available, " # Nat.toText(amount) # " requested");

    Map.set(claimedCancellationFees, phash, tokenPrincipal, claimed + amount);

    let taskId = createTransferTask(caller, #principal(recipient), amount, tokenPrincipal, NachosTreasurySubaccount, #Recovery, claimed + amount);

    logger.info("FEES", "Admin claimed " # Nat.toText(amount) # " cancellation fees for " # Principal.toText(tokenPrincipal) # " -> " # Principal.toText(recipient) # " (task #" # Nat.toText(taskId) # ")", "claimCancellationFees");
    #ok("Claimed " # Nat.toText(amount) # " token fees, transfer task #" # Nat.toText(taskId));
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

    // Try verifying against treasury subaccounts: default (0), deposit (1), minting subaccount (2)
    // Note: tokens sent to the vault canister itself cannot be recovered through this function
    let subaccountsToTry : [Nat8] = [0, NachosDepositSubaccount, NachosTreasurySubaccount];
    var verifiedAmount : Nat = 0;
    var verifiedSubaccount : Nat8 = 0;
    var verified = false;

    for (sub in subaccountsToTry.vals()) {
      if (not verified) {
        try {
          let result = await verifyBlock(tokenPrincipal, blockNumber, senderPrincipal, TREASURY_ID, sub);
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
    let recoveryFee = tokenFee * cancellationFeeMultiplier;

    if (verifiedAmount <= recoveryFee) return #err("Amount too small to recover after fee");

    let refundAmount = verifiedAmount - recoveryFee;

    // Track recovery fee for admin claiming (same bucket as cancellation fees)
    let prevRecoveryFees = switch (Map.get(accumulatedCancellationFees, phash, tokenPrincipal)) { case (?v) v; case null 0 };
    Map.set(accumulatedCancellationFees, phash, tokenPrincipal, prevRecoveryFees + recoveryFee);

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
    if (reset > 0) { ensureTransferQueueRunning() };
    #ok(reset);
  };

  public shared ({ caller }) func refreshICPSwapPools() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller) and caller != taco_dao_sns_governance_canister_id) return #err("Not authorized");
    await discoverICPSwapPools();
    #ok("ICPSwap pool cache refreshed. " # Nat.toText(Map.size(icpSwapPoolCache)) # " ICP pools found.");
  };

  // Debug: test direct DEX calls
  public shared ({ caller }) func testRefreshPrices() : async Result.Result<Text, Text> {
    if (not isMasterAdmin(caller) and not Principal.isController(caller)) return #err("Not authorized");
    // If tokenDetailsMap empty, populate from treasury first (query — no self-call issue)
    if (Map.size(tokenDetailsMap) == 0) {
      try {
        let details = await treasury.getTokenDetails();
        for ((token, detail) in details.vals()) {
          Map.set(tokenDetailsMap, phash, token, detail);
        };
      } catch (e) { return #err("Treasury sync error: " # Error.message(e)) };
    };
    try {
      let success = await refreshPricesLocally();
      if (success) { #ok("Price refresh succeeded — " # Nat.toText(Map.size(tokenDetailsMap)) # " tokens") }
      else { #err("No prices updated — " # Nat.toText(Map.size(tokenDetailsMap)) # " tokens in map") };
    } catch (e) {
      #err("Exception: " # Error.message(e));
    };
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

  // Adaptive NAV history for frontend charts — returns up to ~554 points
  // with 6 resolution tiers: dense for recent data, sparse for old data.
  public query func getNAVHistoryAdaptive() : async [{
    timestamp : Int;
    navPerTokenE8s : Nat;
    reason : NavSnapshotReason;
  }] {
    let size = Vector.size(navHistory);
    if (size == 0) return [];

    let currentTime = now();
    let NS : Int = 1_000_000_000;

    // Tier boundaries and max points: [0-15m: 6, 15m-1h: 6, 1h-24h: 92, 1d-7d: 50, 7d-28d: 100, 28d-1y: 300]
    let tierEdges : [Int] = [
      15 * 60 * NS,
      60 * 60 * NS,
      24 * 60 * 60 * NS,
      7 * 24 * 60 * 60 * NS,
      28 * 24 * 60 * 60 * NS,
      365 * 24 * 60 * 60 * NS,
    ];
    let tierMaxPoints : [Nat] = [6, 6, 92, 50, 100, 300];
    let NUM_TIERS = 6;

    func getTier(age : Int) : Nat {
      var t = 0;
      while (t < NUM_TIERS) {
        if (age <= tierEdges[t]) return t;
        t += 1;
      };
      NUM_TIERS - 1; // clamp to last tier
    };

    // Pass 1: count entries per tier
    let tierCounts = Array.init<Nat>(NUM_TIERS, 0);
    var i = size;
    label pass1 while (i > 0) {
      i -= 1;
      let snap = Vector.get(navHistory, i);
      let age = currentTime - snap.timestamp;
      if (age > tierEdges[NUM_TIERS - 1]) break pass1;
      tierCounts[getTier(age)] += 1;
    };

    // Compute step sizes: step = ceil(count / maxPoints)
    let tierSteps = Array.init<Nat>(NUM_TIERS, 1);
    var t = 0;
    while (t < NUM_TIERS) {
      if (tierCounts[t] > tierMaxPoints[t]) {
        tierSteps[t] := (tierCounts[t] + tierMaxPoints[t] - 1) / tierMaxPoints[t];
      };
      t += 1;
    };

    // Pass 2: sample (walking backwards, collecting in reverse order)
    let tierCounters = Array.init<Nat>(NUM_TIERS, 0);
    let sampled = Vector.new<{ timestamp : Int; navPerTokenE8s : Nat; reason : NavSnapshotReason }>();
    i := size;
    label pass2 while (i > 0) {
      i -= 1;
      let snap = Vector.get(navHistory, i);
      let age = currentTime - snap.timestamp;
      if (age > tierEdges[NUM_TIERS - 1]) break pass2;
      let tier = getTier(age);
      tierCounters[tier] += 1;
      if (tierCounters[tier] % tierSteps[tier] == 0) {
        Vector.add(sampled, {
          timestamp = snap.timestamp;
          navPerTokenE8s = snap.navPerTokenE8s;
          reason = snap.reason;
        });
      };
    };

    // Reverse to chronological order
    let arr = Vector.toArray(sampled);
    Array.tabulate(arr.size(), func(j : Nat) : { timestamp : Int; navPerTokenE8s : Nat; reason : NavSnapshotReason } {
      arr[arr.size() - 1 - j];
    });
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

  // Per-token breakdown of what the user would receive when burning NACHOS.
  // Replicates the proportional distribution logic from redeemNachos().
  public query func estimateBurnTokens(estNachosAmount : Nat) : async {
    nachosAmount : Nat;
    navUsed : Nat;
    redemptionValueICP : Nat;
    feeEstimate : Nat;
    netValueICP : Nat;
    tokens : [{
      token : Principal;
      symbol : Text;
      decimals : Nat;
      amount : Nat;
      priceICP : Nat;
      valueICP : Nat;
      isDust : Bool;
      tokenFee : Nat;
    }];
    portfolioValueICP : Nat;
  } {
    let nav = switch (cachedNAV) {
      case (?n) { n.navPerTokenE8s };
      case null { INITIAL_NAV_PER_TOKEN_E8S };
    };
    let redemptionValue = (estNachosAmount * nav) / ONE_E8S;
    let fee = Nat.max((redemptionValue * burnFeeBasisPoints) / 10_000, minBurnFeeICP);
    let netValue = if (redemptionValue > fee) { redemptionValue - fee } else { 0 };

    let portfolioValue = calculatePortfolioValueICP();
    let tokensResult = Vector.new<{
      token : Principal;
      symbol : Text;
      decimals : Nat;
      amount : Nat;
      priceICP : Nat;
      valueICP : Nat;
      isDust : Bool;
      tokenFee : Nat;
    }>();

    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and details.balance > 0 and details.priceInICP > 0) {
        let tokenValueICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        let tokenShareBP = if (portfolioValue > 0) { (tokenValueICP * 10_000) / portfolioValue } else { 0 };
        let tokenEntitlementICP = (netValue * tokenShareBP) / 10_000;
        let tokenAmount = if (details.priceInICP > 0) {
          (tokenEntitlementICP * (10 ** details.tokenDecimals)) / details.priceInICP;
        } else { 0 };
        let isDust = tokenAmount <= details.tokenTransferFee * 3;
        Vector.add(tokensResult, {
          token;
          symbol = details.tokenSymbol;
          decimals = details.tokenDecimals;
          amount = tokenAmount;
          priceICP = details.priceInICP;
          valueICP = tokenEntitlementICP;
          isDust;
          tokenFee = details.tokenTransferFee;
        });
      };
    };

    {
      nachosAmount = estNachosAmount;
      navUsed = nav;
      redemptionValueICP = redemptionValue;
      feeEstimate = fee;
      netValueICP = netValue;
      tokens = Vector.toArray(tokensResult);
      portfolioValueICP = portfolioValue;
    };
  };

  // Estimate minting with a specific token, including allocation enforcement preview.
  // Shows how much would be accepted vs returned as excess.
  public query func estimateMintWithToken(tokenPrincipal : Principal, tokenAmount : Nat) : async Result.Result<{
    nachosEstimate : Nat;
    feeEstimate : Nat;
    navUsed : Nat;
    tokenPriceICP : Nat;
    depositValueICP : Nat;
    usedAmount : Nat;
    usedValueICP : Nat;
    excessAmount : Nat;
    excessValueICP : Nat;
    allocation : {
      currentBasisPoints : Nat;
      targetBasisPoints : Nat;
      afterDepositBasisPoints : Nat;
      wouldExceed : Bool;
      maxAcceptableAmount : Nat;
    };
  }, NachosError> {
    // Validate token is accepted
    switch (Map.get(acceptedMintTokens, phash, tokenPrincipal)) {
      case (?config) { if (not config.enabled) return #err(#TokenNotAccepted) };
      case null { return #err(#TokenNotAccepted) };
    };

    let details = switch (Map.get(tokenDetailsMap, phash, tokenPrincipal)) {
      case (?d) { d };
      case null { return #err(#TokenNotActive) };
    };
    if (not details.Active) return #err(#TokenNotActive);
    if (details.isPaused) return #err(#TokenPaused);

    let tokenPriceICP = getConservativePrice(tokenPrincipal);
    if (tokenPriceICP == 0) return #err(#InvalidPrice);

    let depositValueICP = (tokenAmount * tokenPriceICP) / (10 ** details.tokenDecimals);

    // Allocation enforcement
    let portfolioValue = calculatePortfolioValueICP();
    var totalVP : Nat = 0;
    for ((_, v) in Map.entries(aggregateAllocation)) { totalVP += v };
    let targetBP = switch (Map.get(aggregateAllocation, phash, tokenPrincipal)) {
      case (?vp) { if (totalVP > 0) { (vp * 10_000) / totalVP } else { 0 } };
      case null { 0 };
    };

    let currentTokenValue = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
    let currentBP = if (portfolioValue > 0) { (currentTokenValue * 10_000) / portfolioValue } else { 0 };

    var usedAmount = tokenAmount;
    var excessAmount : Nat = 0;
    var wouldExceed = false;
    var maxAcceptableAmount = tokenAmount;

    if (targetBP > 0 and portfolioValue > 0) {
      let maxAllowedValue = (portfolioValue * targetBP) / 10_000;
      if (currentTokenValue + depositValueICP > maxAllowedValue) {
        wouldExceed := true;
        if (currentTokenValue >= maxAllowedValue) {
          return #err(#AllocationExceeded);
        };
        let allowedValueICP = maxAllowedValue - currentTokenValue;
        usedAmount := (allowedValueICP * (10 ** details.tokenDecimals)) / tokenPriceICP;
        if (usedAmount > tokenAmount) usedAmount := tokenAmount;
        excessAmount := tokenAmount - usedAmount;
        maxAcceptableAmount := usedAmount;
      };
    };

    let usedValueICP = (usedAmount * tokenPriceICP) / (10 ** details.tokenDecimals);
    let excessValueICP = (excessAmount * tokenPriceICP) / (10 ** details.tokenDecimals);

    let afterDepositBP = if (portfolioValue + usedValueICP > 0) {
      ((currentTokenValue + usedValueICP) * 10_000) / (portfolioValue + usedValueICP);
    } else { 0 };

    let nav = switch (cachedNAV) {
      case (?n) { n.navPerTokenE8s };
      case null { INITIAL_NAV_PER_TOKEN_E8S };
    };

    let feeValue = Nat.max((usedValueICP * mintFeeBasisPoints) / 10_000, minMintFeeICP);
    let netValue = if (usedValueICP > feeValue) { usedValueICP - feeValue } else { 0 };
    let nachosEstimate = if (nav > 0) { (netValue * ONE_E8S) / nav } else { 0 };

    #ok({
      nachosEstimate;
      feeEstimate = feeValue;
      navUsed = nav;
      tokenPriceICP;
      depositValueICP;
      usedAmount;
      usedValueICP;
      excessAmount;
      excessValueICP;
      allocation = {
        currentBasisPoints = currentBP;
        targetBasisPoints = targetBP;
        afterDepositBasisPoints = afterDepositBP;
        wouldExceed;
        maxAcceptableAmount;
      };
    });
  };

  // Calculate the required deposit amounts per token for portfolio-proportional minting.
  // Tells the user exactly how much of each token to deposit for a given ICP-equivalent value.
  public query func getRequiredPortfolioShares(totalValueICP : Nat) : async {
    tokens : [{
      token : Principal;
      symbol : Text;
      decimals : Nat;
      requiredAmount : Nat;
      priceICP : Nat;
      valueICP : Nat;
      basisPoints : Nat;
      tokenFee : Nat;
    }];
    nachosEstimate : Nat;
    feeEstimate : Nat;
    navUsed : Nat;
    portfolioValueICP : Nat;
  } {
    let portfolioValue = calculatePortfolioValueICP();
    let tokensResult = Vector.new<{
      token : Principal;
      symbol : Text;
      decimals : Nat;
      requiredAmount : Nat;
      priceICP : Nat;
      valueICP : Nat;
      basisPoints : Nat;
      tokenFee : Nat;
    }>();

    let THIRTY_MINUTES_NS : Int = 30 * 60 * 1_000_000_000;
    for ((token, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and details.balance > 0 and details.priceInICP > 0) {
        // Portfolio proportions use treasury prices (matches proportion validation)
        let tokenValueICP = (details.balance * details.priceInICP) / (10 ** details.tokenDecimals);
        let tokenShareBP = if (portfolioValue > 0) { (tokenValueICP * 10_000) / portfolioValue } else { 0 };
        let requiredValueICP = (totalValueICP * tokenShareBP) / 10_000;
        // Required amounts use 30m conservative price (matches deposit valuation during minting)
        let depositPrice = getConservativePriceWithWindow(token, THIRTY_MINUTES_NS);
        let price = if (depositPrice > 0) { depositPrice } else { details.priceInICP };
        let requiredAmount = if (price > 0) {
          (requiredValueICP * (10 ** details.tokenDecimals)) / price;
        } else { 0 };
        Vector.add(tokensResult, {
          token;
          symbol = details.tokenSymbol;
          decimals = details.tokenDecimals;
          requiredAmount;
          priceICP = price;
          valueICP = requiredValueICP;
          basisPoints = tokenShareBP;
          tokenFee = details.tokenTransferFee;
        });
      };
    };

    let nav = switch (cachedNAV) {
      case (?n) { n.navPerTokenE8s };
      case null { INITIAL_NAV_PER_TOKEN_E8S };
    };
    let fee = Nat.max((totalValueICP * mintFeeBasisPoints) / 10_000, minMintFeeICP);
    let netValue = if (totalValueICP > fee) { totalValueICP - fee } else { 0 };
    let nachosEst = if (nav > 0) { (netValue * ONE_E8S) / nav } else { 0 };

    {
      tokens = Vector.toArray(tokensResult);
      nachosEstimate = nachosEst;
      feeEstimate = fee;
      navUsed = nav;
      portfolioValueICP = portfolioValue;
    };
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
    hasPausedTokens : Bool;
    pausedTokens : [{ token : Principal; symbol : Text }];
  } {
    let pt = getPausedPortfolioTokens();
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
      hasPausedTokens = pt.size() > 0;
      pausedTokens = pt;
    };
  };

  // All-in-one dashboard for the mint/burn UI.
  // Composite query: fetches live data from treasury + DAO (same subnet on production).
  // Falls back to local cached data on cross-subnet failures (staging).
  public composite query func getVaultDashboard(
    icpMintEstimateAmount : ?Nat,
    burnEstimateAmount : ?Nat,
  ) : async {
    // System state
    genesisComplete : Bool;
    systemPaused : Bool;
    mintingEnabled : Bool;
    burningEnabled : Bool;
    circuitBreakerActive : Bool;

    // NAV
    nav : ?CachedNAV;

    // Portfolio with target allocations
    portfolio : [{
      token : Principal;
      symbol : Text;
      decimals : Nat;
      balance : Nat;
      priceICP : Nat;
      priceUSD : Float;
      valueICP : Nat;
      currentBasisPoints : Nat;
      targetBasisPoints : Nat;
    }];
    portfolioValueICP : Nat;

    // Config
    mintFeeBasisPoints : Nat;
    burnFeeBasisPoints : Nat;
    minMintValueICP : Nat;
    minBurnValueICP : Nat;

    // Accepted mint tokens
    acceptedTokens : [(Principal, AcceptedTokenConfig)];

    // Optional estimates
    mintEstimate : ?{ nachosEstimate : Nat; feeEstimate : Nat; navUsed : Nat };
    burnEstimate : ?{ redemptionValueICP : Nat; feeEstimate : Nat; netValueICP : Nat };

    // Paused token status
    hasPausedTokens : Bool;
    pausedTokens : [{ token : Principal; symbol : Text }];

    // Data freshness
    dataTimestamp : Int;
    dataSource : Text;

    // Analytics (merged from getVaultAnalytics — one call instead of two)
    totalMintCount : Nat;
    totalMintVolumeICP : Nat;
    mintsByMode : { icp : Nat; singleToken : Nat; portfolioShare : Nat };
    totalBurnCount : Nat;
    totalBurnVolumeICP : Nat;
    totalBurnVolumeNACHOS : Nat;
    totalFeesCollectedICP : Nat;
    mintFeesICP : Nat;
    burnFeesICP : Nat;
    feeCount : Nat;
    navChangePercent : ?Float;
    nachosSupply : Nat;
    globalMintIn4h : Nat;
    globalBurnIn4h : Nat;
    maxMintPer4h : Nat;
    maxBurnPer4h : Nat;
  } {
    // 1. Try to fetch fresh data from treasury + DAO via query calls
    var tokenDetails : [(Principal, TokenDetails)] = [];
    var allocations : [(Principal, Nat)] = [];
    var source : Text = "cached";

    try {
      tokenDetails := await (with timeout = 5) treasuryQuery.getTokenDetails();
      source := "live";
    } catch (_) {
      tokenDetails := Iter.toArray(Map.entries(tokenDetailsMap));
    };

    try {
      allocations := await (with timeout = 5) daoQuery.getAggregateAllocation();
    } catch (_) {
      allocations := Iter.toArray(Map.entries(aggregateAllocation));
    };

    // 2. Build lookup maps
    let detailsMap = Map.new<Principal, TokenDetails>();
    for ((token, detail) in tokenDetails.vals()) {
      Map.set(detailsMap, phash, token, detail);
    };

    let allocMap = Map.new<Principal, Nat>();
    var totalAllocVP : Nat = 0;
    for ((token, vp) in allocations.vals()) {
      Map.set(allocMap, phash, token, vp);
      totalAllocVP += vp;
    };

    // 3. Calculate portfolio breakdown with target allocations
    var portfolioValue : Nat = 0;
    let portfolioEntries = Vector.new<{
      token : Principal;
      symbol : Text;
      decimals : Nat;
      balance : Nat;
      priceICP : Nat;
      priceUSD : Float;
      valueICP : Nat;
      currentBasisPoints : Nat;
      targetBasisPoints : Nat;
    }>();

    // First pass: compute total portfolio value
    for ((token, detail) in tokenDetails.vals()) {
      if (detail.Active and detail.balance > 0 and detail.priceInICP > 0) {
        let valueICP = (detail.balance * detail.priceInICP) / (10 ** detail.tokenDecimals);
        portfolioValue += valueICP;
      };
    };

    // Second pass: build portfolio entries with BP
    for ((token, detail) in tokenDetails.vals()) {
      if (detail.Active and detail.balance > 0) {
        let valueICP = (detail.balance * detail.priceInICP) / (10 ** detail.tokenDecimals);
        let currentBP = if (portfolioValue > 0) { (valueICP * 10_000) / portfolioValue } else { 0 };
        let targetBP = switch (Map.get(allocMap, phash, token)) {
          case (?vp) { if (totalAllocVP > 0) { (vp * 10_000) / totalAllocVP } else { 0 } };
          case null { 0 };
        };
        Vector.add(portfolioEntries, {
          token;
          symbol = detail.tokenSymbol;
          decimals = detail.tokenDecimals;
          balance = detail.balance;
          priceICP = detail.priceInICP;
          priceUSD = detail.priceInUSD;
          valueICP;
          currentBasisPoints = currentBP;
          targetBasisPoints = targetBP;
        });
      };
    };

    // 4. Compute optional estimates
    let nav = switch (cachedNAV) {
      case (?n) { n.navPerTokenE8s };
      case null { INITIAL_NAV_PER_TOKEN_E8S };
    };

    let mintEst : ?{ nachosEstimate : Nat; feeEstimate : Nat; navUsed : Nat } = switch (icpMintEstimateAmount) {
      case (?icpAmount) {
        let fee = (icpAmount * mintFeeBasisPoints) / 10_000;
        let netValue = if (icpAmount > fee) { icpAmount - fee } else { 0 };
        let nachos = (netValue * ONE_E8S) / nav;
        ?{ nachosEstimate = nachos; feeEstimate = fee; navUsed = nav };
      };
      case null { null };
    };

    let burnEst : ?{ redemptionValueICP : Nat; feeEstimate : Nat; netValueICP : Nat } = switch (burnEstimateAmount) {
      case (?nachosAmount) {
        let redemptionValue = (nachosAmount * nav) / ONE_E8S;
        let fee = (redemptionValue * burnFeeBasisPoints) / 10_000;
        let net = if (redemptionValue > fee) { redemptionValue - fee } else { 0 };
        ?{ redemptionValueICP = redemptionValue; feeEstimate = fee; netValueICP = net };
      };
      case null { null };
    };

    // 5. Accepted tokens
    let accepted = Vector.new<(Principal, AcceptedTokenConfig)>();
    for ((token, config) in Map.entries(acceptedMintTokens)) {
      Vector.add(accepted, (token, config));
    };

    // 6. Paused tokens
    let pt = getPausedPortfolioTokens();

    // 7. Analytics computation (avoids needing a separate getVaultAnalytics call)
    var aMintVol : Nat = 0;
    var aIcpMints : Nat = 0;
    var aSingleMints : Nat = 0;
    var aPortfolioMints : Nat = 0;
    for (record in Vector.vals(mintHistory)) {
      aMintVol += record.totalDepositValueICP;
      switch (record.mintMode) {
        case (#ICP) { aIcpMints += 1 };
        case (#SingleToken) { aSingleMints += 1 };
        case (#PortfolioShare) { aPortfolioMints += 1 };
      };
    };

    var aBurnVol : Nat = 0;
    var aBurnNachos : Nat = 0;
    for (record in Vector.vals(burnHistory)) {
      aBurnVol += record.redemptionValueICP;
      aBurnNachos += record.nachosBurned;
    };

    var aTotalFees : Nat = 0;
    var aMintFees : Nat = 0;
    var aBurnFees : Nat = 0;
    for (record in Vector.vals(feeHistory)) {
      aTotalFees += record.feeAmountICP;
      switch (record.feeType) {
        case (#Mint) { aMintFees += record.feeAmountICP };
        case (#Burn) { aBurnFees += record.feeAmountICP };
      };
    };

    let aNavChange : ?Float = switch (cachedNAV) {
      case (?n) {
        if (INITIAL_NAV_PER_TOKEN_E8S > 0) {
          ?(Float.fromInt(n.navPerTokenE8s - INITIAL_NAV_PER_TOKEN_E8S) / Float.fromInt(INITIAL_NAV_PER_TOKEN_E8S) * 100.0);
        } else { null };
      };
      case null { null };
    };

    let aSupply = switch (cachedNAV) { case (?n) { n.nachosSupply }; case null { 0 } };

    let aCutoff = now() - FOUR_HOURS_NS;
    var aGMint : Nat = 0;
    var aGBurn : Nat = 0;
    for (entry in Vector.vals(mintRateTracker)) { if (entry.0 >= aCutoff) aGMint += entry.1 };
    for (entry in Vector.vals(burnRateTracker)) { if (entry.0 >= aCutoff) aGBurn += entry.1 };

    // 8. Return combined result
    {
      genesisComplete;
      systemPaused;
      mintingEnabled;
      burningEnabled;
      circuitBreakerActive;
      nav = cachedNAV;
      portfolio = Vector.toArray(portfolioEntries);
      portfolioValueICP = portfolioValue;
      mintFeeBasisPoints;
      burnFeeBasisPoints;
      minMintValueICP;
      minBurnValueICP;
      acceptedTokens = Vector.toArray(accepted);
      mintEstimate = mintEst;
      burnEstimate = burnEst;
      hasPausedTokens = pt.size() > 0;
      pausedTokens = pt;
      dataTimestamp = now();
      dataSource = source;
      // Analytics
      totalMintCount = Vector.size(mintHistory);
      totalMintVolumeICP = aMintVol;
      mintsByMode = { icp = aIcpMints; singleToken = aSingleMints; portfolioShare = aPortfolioMints };
      totalBurnCount = Vector.size(burnHistory);
      totalBurnVolumeICP = aBurnVol;
      totalBurnVolumeNACHOS = aBurnNachos;
      totalFeesCollectedICP = aTotalFees;
      mintFeesICP = aMintFees;
      burnFeesICP = aBurnFees;
      feeCount = Vector.size(feeHistory);
      navChangePercent = aNavChange;
      nachosSupply = aSupply;
      globalMintIn4h = aGMint;
      globalBurnIn4h = aGBurn;
      maxMintPer4h = maxMintICPWorthPer4Hours;
      maxBurnPer4h = maxNachosBurnPer4Hours;
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

  public shared query ({ caller }) func getClaimableMintFees() : async { accumulated : Nat; claimed : Nat; claimable : Nat } {
    { accumulated = accumulatedMintFeesICP; claimed = claimedMintFeesICP; claimable = accumulatedMintFeesICP - claimedMintFeesICP };
  };

  public shared query ({ caller }) func getClaimableCancellationFees() : async [{ token : Principal; accumulated : Nat; claimed : Nat; claimable : Nat }] {
    let result = Vector.new<{ token : Principal; accumulated : Nat; claimed : Nat; claimable : Nat }>();
    for ((token, acc) in Map.entries(accumulatedCancellationFees)) {
      let cl = switch (Map.get(claimedCancellationFees, phash, token)) { case (?v) v; case null 0 };
      if (acc > cl) Vector.add(result, { token; accumulated = acc; claimed = cl; claimable = acc - cl });
    };
    Vector.toArray(result);
  };

  public shared query ({ caller }) func getTransferQueueStatus() : async { pending : Nat; completed : Nat; exhausted : Nat; tasks : [VaultTransferTask] } {
    if (not isMasterAdmin(caller)) return { pending = 0; completed = 0; exhausted = 0; tasks = [] };
    var exhausted : Nat = 0;
    for (task in Vector.vals(pendingTransfers)) {
      if (task.retryCount >= 5) exhausted += 1;
    };
    { pending = Vector.size(pendingTransfers); completed = Map.size(completedTransfers); exhausted; tasks = Vector.toArray(pendingTransfers) };
  };

  // User-visible transfer task status for polling burn payouts, refunds, etc.
  public shared query ({ caller }) func getUserTransferTasks(user : Principal) : async [{
    id : Nat;
    tokenPrincipal : Principal;
    amount : Nat;
    operationType : TransferOperationType;
    operationId : Nat;
    status : TransferStatus;
    createdAt : Int;
    updatedAt : Int;
    blockIndex : ?Nat64;
  }] {
    if (caller != user and not isMasterAdmin(caller)) return [];

    type TaskView = {
      id : Nat;
      tokenPrincipal : Principal;
      amount : Nat;
      operationType : TransferOperationType;
      operationId : Nat;
      status : TransferStatus;
      createdAt : Int;
      updatedAt : Int;
      blockIndex : ?Nat64;
    };
    let result = Vector.new<TaskView>();

    // Pending/in-flight tasks
    for (task in Vector.vals(pendingTransfers)) {
      if (task.caller == user) {
        Vector.add(result, {
          id = task.id;
          tokenPrincipal = task.tokenPrincipal;
          amount = task.amount;
          operationType = task.operationType;
          operationId = task.operationId;
          status = task.status;
          createdAt = task.createdAt;
          updatedAt = task.updatedAt;
          blockIndex = task.blockIndex;
        });
      };
    };

    // Recently completed tasks (last 24h)
    let cutoff = now() - 24 * 3600 * 1_000_000_000;
    for ((_, task) in Map.entries(completedTransfers)) {
      if (task.caller == user and task.updatedAt >= cutoff) {
        Vector.add(result, {
          id = task.id;
          tokenPrincipal = task.tokenPrincipal;
          amount = task.amount;
          operationType = task.operationType;
          operationId = task.operationId;
          status = task.status;
          createdAt = task.createdAt;
          updatedAt = task.updatedAt;
          blockIndex = task.blockIndex;
        });
      };
    };

    Vector.toArray(result);
  };

  // Combined user activity — replaces 5 separate calls with 1 authenticated query.
  // Returns: paginated mint/burn history (newest first), combined timeline, deposits, transfers, rate limits, totals.
  public shared query ({ caller }) func getUserActivity(
    user : Principal,
    mintLimit : Nat,
    mintOffset : Nat,
    burnLimit : Nat,
    burnOffset : Nat,
  ) : async {
    mints : [MintRecord];
    totalMints : Nat;
    burns : [BurnRecord];
    totalBurns : Nat;
    recentTransactions : [{
      txType : { #Mint; #Burn };
      id : Nat;
      timestamp : Int;
      nachosAmount : Nat;
      valueICP : Nat;
      feeICP : Nat;
      mintMode : ?MintMode;
    }];
    activeDeposits : [ActiveDeposit];
    transfers : [{
      id : Nat;
      tokenPrincipal : Principal;
      amount : Nat;
      operationType : TransferOperationType;
      operationId : Nat;
      status : TransferStatus;
      createdAt : Int;
      updatedAt : Int;
      blockIndex : ?Nat64;
    }];
    rateLimits : {
      mintOpsIn4h : Nat;
      burnOpsIn4h : Nat;
      mintValueIn4h : Nat;
      burnValueIn4h : Nat;
      maxMintOps : Nat;
      maxBurnOps : Nat;
      maxMintICPPerUser4Hours : Nat;
      maxBurnNachosPerUser4Hours : Nat;
    };
    userTotalMintVolumeICP : Nat;
    userTotalBurnVolumeICP : Nat;
    userTotalFeesICP : Nat;
  } {
    // Auth check
    if (caller != user and not isMasterAdmin(caller)) {
      return {
        mints = [];
        totalMints = 0;
        burns = [];
        totalBurns = 0;
        recentTransactions = [];
        activeDeposits = [];
        transfers = [];
        rateLimits = { mintOpsIn4h = 0; burnOpsIn4h = 0; mintValueIn4h = 0; burnValueIn4h = 0; maxMintOps = maxMintOpsPerUser4Hours; maxBurnOps = maxBurnOpsPerUser4Hours; maxMintICPPerUser4Hours; maxBurnNachosPerUser4Hours };
        userTotalMintVolumeICP = 0;
        userTotalBurnVolumeICP = 0;
        userTotalFeesICP = 0;
      };
    };

    // --- Mint history (paginated, newest first) ---
    let allUserMints = Vector.new<MintRecord>();
    for (record in Vector.vals(mintHistory)) {
      if (record.caller == user) Vector.add(allUserMints, record);
    };
    let uMintTotal = Vector.size(allUserMints);
    let mintPage = Vector.new<MintRecord>();
    if (uMintTotal > 0 and mintOffset < uMintTotal) {
      let cappedLimit = Nat.min(mintLimit, 100);
      var collected : Nat = 0;
      var skipped : Nat = 0;
      // Iterate backward for newest first
      var idx = uMintTotal;
      while (idx > 0 and collected < cappedLimit) {
        idx -= 1;
        if (skipped >= mintOffset) {
          Vector.add(mintPage, Vector.get(allUserMints, idx));
          collected += 1;
        } else {
          skipped += 1;
        };
      };
    };

    // --- Burn history (paginated, newest first) ---
    let allUserBurns = Vector.new<BurnRecord>();
    for (record in Vector.vals(burnHistory)) {
      if (record.caller == user) Vector.add(allUserBurns, record);
    };
    let uBurnTotal = Vector.size(allUserBurns);
    let burnPage = Vector.new<BurnRecord>();
    if (uBurnTotal > 0 and burnOffset < uBurnTotal) {
      let cappedLimit = Nat.min(burnLimit, 100);
      var collected : Nat = 0;
      var skipped : Nat = 0;
      var idx = uBurnTotal;
      while (idx > 0 and collected < cappedLimit) {
        idx -= 1;
        if (skipped >= burnOffset) {
          Vector.add(burnPage, Vector.get(allUserBurns, idx));
          collected += 1;
        } else {
          skipped += 1;
        };
      };
    };

    // --- Combined timeline (last 50, newest first via insertion sort by timestamp) ---
    type TxEntry = {
      txType : { #Mint; #Burn };
      id : Nat;
      timestamp : Int;
      nachosAmount : Nat;
      valueICP : Nat;
      feeICP : Nat;
      mintMode : ?MintMode;
    };
    let timeline = Vector.new<TxEntry>();
    // Collect all user mints
    for (record in Vector.vals(mintHistory)) {
      if (record.caller == user) {
        Vector.add(timeline, {
          txType = #Mint;
          id = record.id;
          timestamp = record.timestamp;
          nachosAmount = record.nachosReceived;
          valueICP = record.totalDepositValueICP;
          feeICP = record.feeValueICP;
          mintMode = ?record.mintMode;
        });
      };
    };
    // Collect all user burns
    for (record in Vector.vals(burnHistory)) {
      if (record.caller == user) {
        Vector.add(timeline, {
          txType = #Burn;
          id = record.id;
          timestamp = record.timestamp;
          nachosAmount = record.nachosBurned;
          valueICP = record.redemptionValueICP;
          feeICP = record.feeValueICP;
          mintMode = null;
        });
      };
    };
    // Sort by timestamp descending (simple insertion sort — user tx count is small)
    let tlSize = Vector.size(timeline);
    let sorted = Array.tabulate<TxEntry>(tlSize, func(i) { Vector.get(timeline, i) });
    let sortedBuf = Array.thaw<TxEntry>(sorted);
    var si : Nat = 1;
    while (si < tlSize) {
      let key = sortedBuf[si];
      var sj : Int = si - 1;
      while (sj >= 0 and sortedBuf[Int.abs(sj)].timestamp < key.timestamp) {
        sortedBuf[Int.abs(sj) + 1] := sortedBuf[Int.abs(sj)];
        sj -= 1;
      };
      sortedBuf[Int.abs(sj) + 1] := key;
      si += 1;
    };
    let recentTx = Vector.new<TxEntry>();
    var ti : Nat = 0;
    while (ti < Nat.min(tlSize, 50)) {
      Vector.add(recentTx, sortedBuf[ti]);
      ti += 1;
    };

    // --- Active deposits ---
    let deps = Vector.new<ActiveDeposit>();
    for ((_, deposit) in Map.entries(activeDeposits)) {
      if (deposit.caller == user) Vector.add(deps, deposit);
    };

    // --- Transfers (pending + completed 24h) ---
    type TaskView = {
      id : Nat;
      tokenPrincipal : Principal;
      amount : Nat;
      operationType : TransferOperationType;
      operationId : Nat;
      status : TransferStatus;
      createdAt : Int;
      updatedAt : Int;
      blockIndex : ?Nat64;
    };
    let xfers = Vector.new<TaskView>();
    for (task in Vector.vals(pendingTransfers)) {
      if (task.caller == user) {
        Vector.add(xfers, {
          id = task.id;
          tokenPrincipal = task.tokenPrincipal;
          amount = task.amount;
          operationType = task.operationType;
          operationId = task.operationId;
          status = task.status;
          createdAt = task.createdAt;
          updatedAt = task.updatedAt;
          blockIndex = task.blockIndex;
        });
      };
    };
    let xferCutoff = now() - 24 * 3600 * 1_000_000_000;
    for ((_, task) in Map.entries(completedTransfers)) {
      if (task.caller == user and task.updatedAt >= xferCutoff) {
        Vector.add(xfers, {
          id = task.id;
          tokenPrincipal = task.tokenPrincipal;
          amount = task.amount;
          operationType = task.operationType;
          operationId = task.operationId;
          status = task.status;
          createdAt = task.createdAt;
          updatedAt = task.updatedAt;
          blockIndex = task.blockIndex;
        });
      };
    };

    // --- Rate limits ---
    let rlCutoff = now() - FOUR_HOURS_NS;
    var rlMintOps : Nat = 0;
    var rlBurnOps : Nat = 0;
    var rlMintValue : Nat = 0;
    var rlBurnValue : Nat = 0;
    switch (Map.get(userMintOps, phash, user)) {
      case (?ops) { for (ts in Vector.vals(ops)) { if (ts >= rlCutoff) rlMintOps += 1 } };
      case null {};
    };
    switch (Map.get(userBurnOps, phash, user)) {
      case (?ops) { for (ts in Vector.vals(ops)) { if (ts >= rlCutoff) rlBurnOps += 1 } };
      case null {};
    };
    switch (Map.get(userMintValues, phash, user)) {
      case (?vals) { for ((ts, v) in Vector.vals(vals)) { if (ts >= rlCutoff) rlMintValue += v } };
      case null {};
    };
    switch (Map.get(userBurnValues, phash, user)) {
      case (?vals) { for ((ts, v) in Vector.vals(vals)) { if (ts >= rlCutoff) rlBurnValue += v } };
      case null {};
    };

    // --- User totals ---
    var uMintVol : Nat = 0;
    for (i in Iter.range(0, if (uMintTotal > 0) { uMintTotal - 1 } else { 0 })) {
      if (uMintTotal > 0) uMintVol += Vector.get(allUserMints, i).totalDepositValueICP;
    };
    var uBurnVol : Nat = 0;
    for (i in Iter.range(0, if (uBurnTotal > 0) { uBurnTotal - 1 } else { 0 })) {
      if (uBurnTotal > 0) uBurnVol += Vector.get(allUserBurns, i).redemptionValueICP;
    };
    var uFees : Nat = 0;
    for (record in Vector.vals(feeHistory)) {
      if (record.userPrincipal == user) uFees += record.feeAmountICP;
    };

    {
      mints = Vector.toArray(mintPage);
      totalMints = uMintTotal;
      burns = Vector.toArray(burnPage);
      totalBurns = uBurnTotal;
      recentTransactions = Vector.toArray(recentTx);
      activeDeposits = Vector.toArray(deps);
      transfers = Vector.toArray(xfers);
      rateLimits = {
        mintOpsIn4h = rlMintOps;
        burnOpsIn4h = rlBurnOps;
        mintValueIn4h = rlMintValue;
        burnValueIn4h = rlBurnValue;
        maxMintOps = maxMintOpsPerUser4Hours;
        maxBurnOps = maxBurnOpsPerUser4Hours;
        maxMintICPPerUser4Hours;
        maxBurnNachosPerUser4Hours;
      };
      userTotalMintVolumeICP = uMintVol;
      userTotalBurnVolumeICP = uBurnVol;
      userTotalFeesICP = uFees;
    };
  };

  // Aggregated stats for analytics/infographics: volume, fees, counts, NAV performance.
  public query func getVaultAnalytics() : async {
    totalMintCount : Nat;
    totalMintVolumeICP : Nat;
    mintsByMode : { icp : Nat; singleToken : Nat; portfolioShare : Nat };
    totalBurnCount : Nat;
    totalBurnVolumeICP : Nat;
    totalBurnVolumeNACHOS : Nat;
    totalFeesCollectedICP : Nat;
    mintFeesICP : Nat;
    burnFeesICP : Nat;
    feeCount : Nat;
    currentNAV : ?CachedNAV;
    initialNAVPerToken : Nat;
    navChangePercent : ?Float;
    nachosSupply : Nat;
    portfolioValueICP : Nat;
    globalMintIn4h : Nat;
    globalBurnIn4h : Nat;
    maxMintPer4h : Nat;
    maxBurnPer4h : Nat;
  } {
    var totalMintVol : Nat = 0;
    var icpMints : Nat = 0;
    var singleTokenMints : Nat = 0;
    var portfolioShareMints : Nat = 0;

    for (record in Vector.vals(mintHistory)) {
      totalMintVol += record.totalDepositValueICP;
      switch (record.mintMode) {
        case (#ICP) { icpMints += 1 };
        case (#SingleToken) { singleTokenMints += 1 };
        case (#PortfolioShare) { portfolioShareMints += 1 };
      };
    };

    var totalBurnVol : Nat = 0;
    var totalBurnNachos : Nat = 0;
    for (record in Vector.vals(burnHistory)) {
      totalBurnVol += record.redemptionValueICP;
      totalBurnNachos += record.nachosBurned;
    };

    var totalFees : Nat = 0;
    var mFees : Nat = 0;
    var bFees : Nat = 0;
    for (record in Vector.vals(feeHistory)) {
      totalFees += record.feeAmountICP;
      switch (record.feeType) {
        case (#Mint) { mFees += record.feeAmountICP };
        case (#Burn) { bFees += record.feeAmountICP };
      };
    };

    let navChange : ?Float = switch (cachedNAV) {
      case (?nav) {
        if (INITIAL_NAV_PER_TOKEN_E8S > 0) {
          ?(Float.fromInt(nav.navPerTokenE8s - INITIAL_NAV_PER_TOKEN_E8S) / Float.fromInt(INITIAL_NAV_PER_TOKEN_E8S) * 100.0);
        } else { null };
      };
      case null { null };
    };

    let supply = switch (cachedNAV) { case (?n) { n.nachosSupply }; case null { 0 } };
    let portfolioVal = switch (cachedNAV) { case (?n) { n.portfolioValueICP }; case null { 0 } };

    let cutoff = now() - FOUR_HOURS_NS;
    var gMint : Nat = 0;
    var gBurn : Nat = 0;
    for (entry in Vector.vals(mintRateTracker)) { if (entry.0 >= cutoff) gMint += entry.1 };
    for (entry in Vector.vals(burnRateTracker)) { if (entry.0 >= cutoff) gBurn += entry.1 };

    {
      totalMintCount = Vector.size(mintHistory);
      totalMintVolumeICP = totalMintVol;
      mintsByMode = { icp = icpMints; singleToken = singleTokenMints; portfolioShare = portfolioShareMints };
      totalBurnCount = Vector.size(burnHistory);
      totalBurnVolumeICP = totalBurnVol;
      totalBurnVolumeNACHOS = totalBurnNachos;
      totalFeesCollectedICP = totalFees;
      mintFeesICP = mFees;
      burnFeesICP = bFees;
      feeCount = Vector.size(feeHistory);
      currentNAV = cachedNAV;
      initialNAVPerToken = INITIAL_NAV_PER_TOKEN_E8S;
      navChangePercent = navChange;
      nachosSupply = supply;
      portfolioValueICP = portfolioVal;
      globalMintIn4h = gMint;
      globalBurnIn4h = gBurn;
      maxMintPer4h = maxMintICPWorthPer4Hours;
      maxBurnPer4h = maxNachosBurnPer4Hours;
    };
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
    maxMintICPPerUser4Hours : Nat;
    maxBurnNachosPerUser4Hours : Nat;
    maxMintAmountICP : Nat;
    maxBurnAmountNachos : Nat;
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
      maxMintICPPerUser4Hours;
      maxBurnNachosPerUser4Hours;
      maxMintAmountICP;
      maxBurnAmountNachos;
    };
  };

  public shared query ({ caller }) func getUserRateLimitStatus(user : Principal) : async {
    mintOpsIn4h : Nat; burnOpsIn4h : Nat; mintValueIn4h : Nat; burnValueIn4h : Nat;
  } {
    if (caller != user and not isMasterAdmin(caller)) return { mintOpsIn4h = 0; burnOpsIn4h = 0; mintValueIn4h = 0; burnValueIn4h = 0 };
    let cutoff = now() - FOUR_HOURS_NS;
    var mintOps : Nat = 0;
    var burnOps : Nat = 0;
    var mintValue : Nat = 0;
    var burnValue : Nat = 0;

    switch (Map.get(userMintOps, phash, user)) {
      case (?ops) { for (ts in Vector.vals(ops)) { if (ts >= cutoff) mintOps += 1 } };
      case null {};
    };
    switch (Map.get(userBurnOps, phash, user)) {
      case (?ops) { for (ts in Vector.vals(ops)) { if (ts >= cutoff) burnOps += 1 } };
      case null {};
    };
    switch (Map.get(userMintValues, phash, user)) {
      case (?vals) { for ((ts, v) in Vector.vals(vals)) { if (ts >= cutoff) mintValue += v } };
      case null {};
    };
    switch (Map.get(userBurnValues, phash, user)) {
      case (?vals) { for ((ts, v) in Vector.vals(vals)) { if (ts >= cutoff) burnValue += v } };
      case null {};
    };

    { mintOpsIn4h = mintOps; burnOpsIn4h = burnOps; mintValueIn4h = mintValue; burnValueIn4h = burnValue };
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

  // --- Timer: Periodic Sync (15 min) — token details, allocations, NAV snapshot, circuit breaker ---
  private func startPeriodicSyncTimer<system>() {
    switch (periodicSyncTimerId) { case (?id) { cancelTimer(id) }; case null {} };
    periodicSyncTimerId := ?setTimer<system>(#nanoseconds(15 * 60 * 1_000_000_000), func() : async () {
      // 1. Sync token details from treasury (skip if mint/burn just refreshed data)
      if (now() - lastMintBurnTime >= 15 * 60 * 1_000_000_000) {
        try {
          let details = await treasury.getTokenDetails();
          for ((token, detail) in details.vals()) {
            Map.set(tokenDetailsMap, phash, token, detail);
          };
        } catch (e) {
          logger.warn("TIMER", "Token details sync failed: " # Error.message(e), "periodicSync");
        };
      } else {
        logger.info("TIMER", "Skipping treasury balance sync — mint/burn refreshed data recently", "periodicSync");
      };

      // 2. Clean rate limit windows (moved here from per-operation for efficiency)
      cleanRateLimitWindows();

      // 3. Sync allocations from DAO
      try {
        let allocs = await dao.getAggregateAllocation();
        for ((token, _) in Map.entries(aggregateAllocation)) {
          ignore Map.remove(aggregateAllocation, phash, token);
        };
        for ((token, vp) in allocs.vals()) {
          Map.set(aggregateAllocation, phash, token, vp);
        };
      } catch (e) {
        logger.warn("TIMER", "Allocation sync failed: " # Error.message(e), "periodicSync");
      };

      // 4. Calculate NAV + record snapshot
      ignore await calculateNAV();
      recordNavSnapshot(#Scheduled);

      // 5. Circuit breaker check
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
            if (snap.timestamp < windowCutoff) { i := 0 }
            else if (snap.navPerTokenE8s > maxNavInWindow) {
              maxNavInWindow := snap.navPerTokenE8s;
            };
          };
          if (maxNavInWindow > currentNav) {
            let dropPercent = Float.fromInt(maxNavInWindow - currentNav) / Float.fromInt(maxNavInWindow) * 100.0;
            if (dropPercent >= navDropThresholdPercent) {
              circuitBreakerActive := true;
              logger.warn("CIRCUIT_BREAKER", "NAV drop detected: " # Float.toText(dropPercent) # "% (threshold: " # Float.toText(navDropThresholdPercent) # "%)", "periodicSync");
            };
          };
        };
      };

      startPeriodicSyncTimer();
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

  // --- Transfer Queue Processing (on-demand, 30 sec delay) ---
  private func ensureTransferQueueRunning<system>() {
    switch (transferQueueTimerId) {
      case (?_) { return }; // already scheduled
      case null {};
    };
    transferQueueTimerId := ?setTimer<system>(#nanoseconds(5 * 1_000_000_000), func() : async () {
      transferQueueTimerId := null;
      if (Vector.size(pendingTransfers) > 0) {
        await processTransferQueue();
        // Re-check: if still items remaining (retries), schedule again
        if (Vector.size(pendingTransfers) > 0) {
          ensureTransferQueueRunning();
        };
      };
    });
  };

  // --- Start All Timers ---
  private func startAllTimers<system>() {
    startPeriodicSyncTimer();
    startBlockCleanupTimer();
    // Transfer queue is on-demand — check if pending transfers from before upgrade
    if (Vector.size(pendingTransfers) > 0) {
      ensureTransferQueueRunning();
    };
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
