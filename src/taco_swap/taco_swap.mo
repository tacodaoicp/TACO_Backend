import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import Map "mo:map/Map";
import Prim "mo:prim";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Debug "mo:base/Debug";
import Vector "mo:vector";
import Error "mo:base/Error";
import { now } = "mo:base/Time";
import { setTimer; cancelTimer } = "mo:base/Timer";
import Result "mo:base/Result";
import Float "mo:base/Float";
import Array "mo:base/Array";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import SwapTypes "../swap/swap_types";
import ICPSwap "../swap/icp_swap";
import SwapUtils "../swap/utils";
import SpamProtection "../helper/spam_protection";
import CanisterIds "../helper/CanisterIds";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import SHA224 "../helper/SHA224";
import CRC32 "../helper/CRC32";
import DAO_types "../DAO_backend/dao_types";
import Types "./taco_swap_types";
import Migration "./migration";

(with migration = Migration.migrate)
shared (deployer) persistent actor class TacoSwapDAO() = this {

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 1: CORE SETUP
  // ═══════════════════════════════════════════════════════════════════

  private func this_canister_id() : Principal { Principal.fromActor(this) };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 2: SYSTEM CONFIG & CANISTER IDS
  // ═══════════════════════════════════════════════════════════════════

  transient let canister_ids = CanisterIds.CanisterIds(this_canister_id());

  transient let logger = Logger.Logger();
  transient let spamGuard = SpamProtection.SpamGuard(this_canister_id());

  transient let { phash; thash } = Map;

  // Type aliases
  type TokenDetails = DAO_types.TokenDetails;
  type ClaimResult = Types.ClaimResult;
  type ClaimPath = Types.ClaimPath;
  type PendingSwap = Types.PendingSwap;
  type OrderRecord = Types.OrderRecord;
  type SwapConfig = Types.SwapConfig;
  type AdminAction = Types.AdminAction;
  type AdminActionRecord = Types.AdminActionRecord;
  type HttpRequest = Types.HttpRequest;
  type HttpResponse = Types.HttpResponse;
  type NachosClaimResult = Types.NachosClaimResult;
  type NachosPendingSwap = Types.NachosPendingSwap;
  type NachosOrderRecord = Types.NachosOrderRecord;
  type NachosSwapProgress = Types.NachosSwapProgress;

  private func isAdmin(caller : Principal) : Bool {
    AdminAuth.isAdmin(caller, canister_ids.isKnownCanister);
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 3: STABLE STATE VARIABLES
  // ═══════════════════════════════════════════════════════════════════

  // --- Core Config ---
  var systemPaused : Bool = false;
  var maxSlippageBasisPoints : Nat = 9950; // 95% (wide open for staging/testing)
  var minDepositICP : Nat = 50_000; // 0.0005 ICP (low for staging testing)
  var maxRetries : Nat = 3;
  var sweepIntervalNS : Int = 5 * 60 * 1_000_000_000; // 5 minutes

  // --- ICPSwap Pool ---
  var icpTacoPoolId : ?Principal = null;
  var poolZeroForOne : Bool = true; // true if ICP is token0 in the pool

  // --- Token Fees (fetched on init) ---
  var tacoLedgerFee : Nat = 100_000; // default 0.001 TACO, updated on init
  var tacoDecimals : Nat8 = 8; // default, updated on init

  // --- Operation Tracking ---
  let pendingSwaps = Map.new<Principal, PendingSwap>();
  let completedOrders = Vector.new<OrderRecord>();
  let processedTransakOrders = Map.new<Text, Nat>(); // transakOrderId -> order index
  let processedCoinbaseOrders = Map.new<Text, Nat>(); // coinbase transactionId -> order index
  let adminActions = Vector.new<AdminActionRecord>();
  var nextOrderId : Nat = 0;

  // --- Concurrent Operation Locks ---
  let operationLocks = Map.new<Principal, Int>();
  let LOCK_TIMEOUT_NS : Int = 3 * 60 * 1_000_000_000; // 3 min

  // --- Swap Progress Tracking (for frontend polling) ---
  let swapProgress = Map.new<Principal, Types.SwapProgress>();

  // --- Statistics ---
  var totalSwapsCompleted : Nat = 0;
  var totalICPSwapped : Nat = 0;
  var totalTACODelivered : Nat = 0;

  // --- NACHOS Minting State ---
  let nachosPendingSwaps = Map.new<Principal, NachosPendingSwap>();
  let nachosCompletedOrders = Vector.new<NachosOrderRecord>();
  let nachosSwapProgress = Map.new<Principal, NachosSwapProgress>();
  var nextNachosOrderId : Nat = 0;
  var totalNachosMints : Nat = 0;
  var totalNachosICPDeposited : Nat = 0;
  var totalNachosDelivered : Nat = 0;
  var nachosMinDepositICP : Nat = 1_000_000; // 0.01 ICP
  var nachosMintingEnabled : Bool = true;

  // --- Timer IDs ---
  transient var sweepTimerId : ?Nat = null;

  // (localTokenDetailsMap reserved for future use if needed by ICPSwap operations)

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 4: CONSTANTS
  // ═══════════════════════════════════════════════════════════════════

  let ICP_FEE : Nat = 10_000; // 0.0001 ICP
  let ICP_LEDGER : Principal = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
  let TACO_LEDGER : Principal = Principal.fromText("kknbx-zyaaa-aaaaq-aae4a-cai");
  // ICPSwap Factory: 4mmnk-kiaaa-aaaag-qbllq-cai (used via icp_swap.mo module)

  // Hex encoding characters
  let hexSymbols = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 5: DEPOSIT ADDRESS COMPUTATION
  // ═══════════════════════════════════════════════════════════════════

  // Compute ICP AccountIdentifier from owner + subaccount (same as nachos_vault.mo:333)
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

  // Encode bytes to hex string (same as Utils.mo:encode)
  private func bytesToHex(bytes : [Nat8]) : Text {
    Array.foldLeft<Nat8, Text>(
      bytes,
      "",
      func(accum, u8) {
        let c1 = hexSymbols[Nat8.toNat(u8 / 16)];
        let c2 = hexSymbols[Nat8.toNat(u8 % 16)];
        accum # Char.toText(c1) # Char.toText(c2);
      },
    );
  };

  // Get user's subaccount as Blob (for AccountIdentifier computation)
  private func userSubaccountBlob(user : Principal) : Blob {
    Blob.fromArray(SwapUtils.principalToSubaccount(user));
  };

  // NACHOS deposit subaccount: same as TACO but last byte = 1 to avoid collision
  private func nachosSubaccount(user : Principal) : [Nat8] {
    let sub = SwapUtils.principalToSubaccount(user);
    let result = Array.thaw<Nat8>(sub);
    result[31] := 1;
    Array.freeze(result);
  };

  private func nachosSubaccountBlob(user : Principal) : Blob {
    Blob.fromArray(nachosSubaccount(user));
  };

  // Treasury subaccount 2 (where nachos_vault expects ICP deposits)
  private func nachosTreasurySubaccount() : [Nat8] {
    [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] : [Nat8];
  };

  // Get the canister's main ICP deposit address (64-char hex AccountIdentifier)
  public query func get_deposit_address() : async Text {
    let aid = computeAccountIdentifier(this_canister_id(), null);
    bytesToHex(Blob.toArray(aid));
  };

  // Get ICP deposit address for a specific principal
  public shared ({ caller }) func get_deposit_address_for(user : ?Principal) : async Text {
    let targetPrincipal = switch (user) {
      case (?p) {
        if (p != caller and not isAdmin(caller)) {
          Debug.trap("Not authorized to get deposit address for another principal");
        };
        p;
      };
      case null { caller };
    };

    if (targetPrincipal == Principal.fromText("2vxsx-fae")) {
      Debug.trap("Cannot generate deposit address for anonymous principal");
    };

    let subaccount = userSubaccountBlob(targetPrincipal);
    let aid = computeAccountIdentifier(this_canister_id(), ?subaccount);
    bytesToHex(Blob.toArray(aid));
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 6: BALANCE CHECKING
  // ═══════════════════════════════════════════════════════════════════

  // ICP ledger actor ref (ICRC-1 interface)
  transient let icpLedger : SwapTypes.ICRC1 = actor (Principal.toText(ICP_LEDGER));
  transient let tacoLedgerActor : SwapTypes.ICRC1 = actor (Principal.toText(TACO_LEDGER));

  // NACHOS vault actor interface
  type NachosAccount = { owner : Principal; subaccount : ?Blob };
  type NachosTokenDeposit = { token : Principal; amount : Nat; priceUsed : Nat; valueICP : Nat };
  type NachosMintMode = { #ICP; #SingleToken; #PortfolioShare };
  type NachosMintResult = {
    success : Bool;
    mintId : Nat;
    mintMode : NachosMintMode;
    nachosReceived : Nat;
    navUsed : Nat;
    deposits : [NachosTokenDeposit];
    totalDepositValueICP : Nat;
    excessReturned : [NachosTokenDeposit];
    feeValueICP : Nat;
    netValueICP : Nat;
    nachosLedgerTxId : ?Nat;
    recipient : NachosAccount;
  };
  type NachosError = {
    #SystemPaused;
    #MintingDisabled;
    #BurningDisabled;
    #PriceStale;
    #InsufficientBalance;
    #BelowMinimumValue;
    #RateLimitExceeded;
    #OperationInProgress;
    #BlockAlreadyProcessed;
    #BlockVerificationFailed : Text;
    #TokenNotActive;
    #TokenPaused;
    #PortfolioTokenPaused : { pausedTokens : [{ token : Principal; symbol : Text }] };
    #TokenNotAccepted;
    #AllocationExceeded;
    #InvalidAllocation;
    #SlippageExceeded;
    #InvalidPrice;
    #InsufficientLiquidity : { token : Principal; available : Nat; requested : Nat };
    #CircuitBreakerActive;
    #BurnLimitExceeded : { maxPer4Hours : Nat; recentBurns : Nat; requested : Nat };
    #MintLimitExceeded : { maxPer4Hours : Nat; recentMints : Nat; requested : Nat };
    #UserMintLimitExceeded : { maxPer4Hours : Nat; recentMints : Nat; requested : Nat };
    #UserBurnLimitExceeded : { maxPer4Hours : Nat; recentBurns : Nat; requested : Nat };
    #AboveMaximumValue : { max : Nat; requested : Nat };
    #PortfolioShareMismatch : {
      expected : [{ token : Principal; basisPoints : Nat }];
      received : [{ token : Principal; basisPoints : Nat }];
    };
    #GenesisNotComplete;
    #GenesisAlreadyDone;
    #TransferError : Text;
    #DepositNotFound;
    #DepositAlreadyCancelled;
    #DepositAlreadyConsumed;
    #DepositExpired;
    #NotDepositor;
    #RollbackFailed : Text;
    #NotAuthorized;
    #UnexpectedError : Text;
  };

  transient let NACHOS_VAULT_ID = canister_ids.getCanisterId(#nachos_vault);
  transient let TREASURY_ID = canister_ids.getCanisterId(#treasury);

  type NachosVaultActor = actor {
    mintNachos : shared (Nat, Nat, ?Blob, ?NachosAccount) -> async Result.Result<NachosMintResult, NachosError>;
    estimateMintICP : shared query (Nat) -> async { nachosEstimate : Nat; feeEstimate : Nat; navUsed : Nat };
  };
  transient let nachosVault : NachosVaultActor = actor (Principal.toText(NACHOS_VAULT_ID));

  // Check ICP balance on user's deposit subaccount
  public shared ({ caller }) func get_pending_balance() : async Nat {
    if (caller == Principal.fromText("2vxsx-fae")) return 0;
    let subaccount = SwapUtils.principalToSubaccount(caller);
    await icpLedger.icrc1_balance_of({
      owner = this_canister_id();
      subaccount = ?subaccount;
    });
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 7: CORE SWAP LOGIC
  // ═══════════════════════════════════════════════════════════════════

  // Acquire per-principal operation lock
  private func acquireLock(principal : Principal) : Bool {
    switch (Map.get(operationLocks, phash, principal)) {
      case (?lockTime) {
        if (now() - lockTime < LOCK_TIMEOUT_NS) return false; // Still locked
        // Lock expired, allow re-acquisition
      };
      case null {};
    };
    Map.set(operationLocks, phash, principal, now());
    true;
  };

  private func releaseLock(principal : Principal) {
    Map.delete(operationLocks, phash, principal);
  };

  // --- Swap Progress Helpers ---

  private func stepToDisplay(step : Types.SwapStep) : (Nat, Text) {
    switch (step) {
      case (#NotStarted) (0, "No swap in progress.");
      case (#WaitingForDeposit) (0, "Waiting for ICP deposit from Coinbase...");
      case (#DepositReceived) (1, "ICP deposit received. Preparing swap...");
      case (#GettingQuote) (2, "Getting price quote from ICPSwap...");
      case (#TransferringToPool) (3, "Transferring ICP to swap pool...");
      case (#SwappingTokens) (4, "Swapping ICP for TACO...");
      case (#TransferringToWallet) (5, "Transferring TACO to your wallet...");
      case (#Complete) (6, "Swap complete! TACO delivered.");
      case (#Failed) (0, "Swap encountered an error. Retrying...");
    };
  };

  private func updateProgress(
    principal : Principal,
    step : Types.SwapStep,
    updates : {
      icpAmount : ?Nat;
      estimatedTaco : ?Nat;
      actualTaco : ?Nat;
      errorMessage : ?Text;
      orderId : ?Nat;
      txId : ?Nat;
    },
  ) {
    let existing = Map.get(swapProgress, phash, principal);
    let (stepNumber, description) = stepToDisplay(step);

    // Helper: use new value if provided, otherwise keep existing
    func mergeOpt(newVal : ?Nat, field : ?Nat) : ?Nat {
      switch (newVal) { case (?v) ?v; case null field };
    };

    let prev : Types.SwapProgress = switch (existing) {
      case (?e) e;
      case null {
        {
          step = #NotStarted;
          stepNumber = 0;
          totalSteps = 7;
          description = "";
          icpAmount = null;
          estimatedTaco = null;
          actualTaco = null;
          startedAt = now();
          updatedAt = now();
          errorMessage = null;
          retryCount = 0;
          orderId = null;
          txId = null;
        };
      };
    };

    Map.set(swapProgress, phash, principal, {
      step;
      stepNumber;
      totalSteps = 7 : Nat;
      description;
      icpAmount = mergeOpt(updates.icpAmount, prev.icpAmount);
      estimatedTaco = mergeOpt(updates.estimatedTaco, prev.estimatedTaco);
      actualTaco = mergeOpt(updates.actualTaco, prev.actualTaco);
      startedAt = prev.startedAt;
      updatedAt = now();
      errorMessage = updates.errorMessage;
      retryCount = if (step == #Failed) { prev.retryCount + 1 } else { prev.retryCount };
      orderId = mergeOpt(updates.orderId, prev.orderId);
      txId = mergeOpt(updates.txId, prev.txId);
    });
  };

  // Main swap processing function
  private func processDeposit(userPrincipal : Principal, claimPath : ClaimPath, fiatAmount : ?Text, fiatCurrency : ?Text) : async ClaimResult {
    let ctx = "processDeposit";

    // --- Pre-checks ---
    if (systemPaused) return #SystemPaused;
    if (userPrincipal == Principal.fromText("2vxsx-fae")) return #NotAuthorized;

    // Check for existing pending swap first (retry path)
    switch (Map.get(pendingSwaps, phash, userPrincipal)) {
      case (?pending) {
        if (pending.attempts >= maxRetries) {
          logger.warn("SWAP", "Max retries exceeded for " # Principal.toText(userPrincipal), ctx);
          return #SwapFailed("Max retries exceeded. Admin intervention required.");
        };
        return await retryPendingSwap(userPrincipal, pending, claimPath, fiatAmount, fiatCurrency);
      };
      case null {};
    };

    // Acquire lock
    if (not acquireLock(userPrincipal)) return #AlreadyProcessing;

    // --- Step 1: Check ICP balance on user's subaccount ---
    let userSubaccount = SwapUtils.principalToSubaccount(userPrincipal);
    let balance = try {
      await icpLedger.icrc1_balance_of({
        owner = this_canister_id();
        subaccount = ?userSubaccount;
      });
    } catch (e) {
      releaseLock(userPrincipal);
      logger.error("SWAP", "Balance check failed: " # Error.message(e), ctx);
      return #SwapFailed("Balance check failed: " # Error.message(e));
    };

    if (balance == 0) {
      // Safety net: check if TACO is sitting on the user's subaccount
      // (e.g., from a previous swap where auto-withdraw went to subaccount, or manual send)
      let tacoBalance = try {
        await tacoLedgerActor.icrc1_balance_of({
          owner = this_canister_id();
          subaccount = ?userSubaccount;
        });
      } catch (_e) { 0 };

      if (tacoBalance > tacoLedgerFee * 2) {
        let tacoToSend = tacoBalance - tacoLedgerFee;
        logger.info("SWAP", "Found " # Nat.toText(tacoBalance) # " TACO on user subaccount, forwarding " # Nat.toText(tacoToSend) # " to " # Principal.toText(userPrincipal), ctx);
        updateProgress(userPrincipal, #TransferringToWallet, { icpAmount = null; estimatedTaco = null; actualTaco = ?tacoToSend; errorMessage = null; orderId = null; txId = null });

        let tacoTransferResult = try {
          await tacoLedgerActor.icrc1_transfer({
            from_subaccount = ?userSubaccount;
            to = { owner = userPrincipal; subaccount = null };
            amount = tacoToSend;
            fee = ?tacoLedgerFee;
            memo = null;
            created_at_time = null;
          });
        } catch (e) {
          releaseLock(userPrincipal);
          return #SwapFailed("TACO subaccount forward failed: " # Error.message(e));
        };

        switch (tacoTransferResult) {
          case (#Ok(txId)) {
            let orderId = nextOrderId;
            nextOrderId += 1;
            Vector.add(completedOrders, {
              id = orderId;
              principal = userPrincipal;
              icpDeposited = 0;
              tacoReceived = tacoToSend;
              slippage = 0.0;
              poolId = switch (icpTacoPoolId) { case (?p) p; case null Principal.fromText("aaaaa-aa") };
              timestamp = now();
              transakOrderId = null;
              tacoTxId = ?txId;
              claimPath;
              fiatAmount;
              fiatCurrency;
            });
            totalSwapsCompleted += 1;
            totalTACODelivered += tacoToSend;
            updateProgress(userPrincipal, #Complete, { icpAmount = null; estimatedTaco = null; actualTaco = ?tacoToSend; errorMessage = null; orderId = ?orderId; txId = ?txId });
            releaseLock(userPrincipal);
            return #Success({ tacoAmount = tacoToSend; txId; orderId });
          };
          case (#Err(e)) {
            updateProgress(userPrincipal, #Failed, { icpAmount = null; estimatedTaco = null; actualTaco = null; errorMessage = ?("TACO subaccount transfer error: " # debug_show (e)); orderId = null; txId = null });
            releaseLock(userPrincipal);
            return #SwapFailed("TACO subaccount transfer error: " # debug_show (e));
          };
        };
      };

      // Last resort: check ICPSwap pool for unclaimed TACO or ICP
      // (from a previous swap where withdrawToSubaccount failed)
      // SECURITY: Pool unused balance is per-canister (SHARED across all users).
      // We ONLY recover to canister main account — NEVER to the calling user.
      // This prevents users from stealing tokens from other users' failed swaps.
      // Admin must distribute recovered tokens manually.
      switch (icpTacoPoolId) {
        case (?poolId) {
          let poolBalance = try {
            let pool : SwapTypes.ICPSwapPool = actor (Principal.toText(poolId));
            await pool.getUserUnusedBalance(this_canister_id());
          } catch (_e) { #err("pool query failed") };

          switch (poolBalance) {
            case (#ok(bal)) {
              // Check for TACO on pool (token0 or token1 depending on pool direction)
              let tacoPoolBalance = if (poolZeroForOne) { bal.balance1 } else { bal.balance0 };

              if (tacoPoolBalance > tacoLedgerFee * 2) {
                // TACO found on pool — withdraw to CANISTER MAIN (not to user!)
                logger.info("SWAP", "Found " # Nat.toText(tacoPoolBalance) # " TACO on ICPSwap pool, recovering to canister main", ctx);
                let _withdrawResult = try {
                  await ICPSwap.executeWithdraw(this_canister_id(), {
                    poolId;
                    token = TACO_LEDGER;
                    amount = tacoPoolBalance;
                    fee = tacoLedgerFee;
                  }, false);
                } catch (_e) { #err("TACO pool recovery failed") };
                logger.info("SWAP", "TACO recovered to canister main. Admin can use admin_transfer_taco to distribute.", ctx);
              };

              // Check for ICP on pool (from a failed swap)
              let icpPoolBalance = if (poolZeroForOne) { bal.balance0 } else { bal.balance1 };
              if (icpPoolBalance > ICP_FEE * 2) {
                logger.info("SWAP", "Found " # Nat.toText(icpPoolBalance) # " ICP on ICPSwap pool, recovering to canister main", ctx);
                let _withdrawResult = try {
                  await ICPSwap.executeWithdraw(this_canister_id(), {
                    poolId;
                    token = ICP_LEDGER;
                    amount = icpPoolBalance;
                    fee = ICP_FEE;
                  }, false);
                } catch (_e) { #err("ICP pool recovery failed") };
                logger.info("SWAP", "ICP recovered to canister main. Admin can use admin_transfer_icp to distribute.", ctx);
              };
            };
            case (#err(_)) {};
          };
        };
        case null {};
      };

      releaseLock(userPrincipal);
      return #NoDeposit;
    };

    if (balance <= ICP_FEE * 2 + minDepositICP) {
      releaseLock(userPrincipal);
      return #BelowMinimum({ balance; minimum = ICP_FEE * 2 + minDepositICP });
    };

    logger.info("SWAP", "Processing deposit of " # Nat.toText(balance) # " e8s for " # Principal.toText(userPrincipal), ctx);
    updateProgress(userPrincipal, #DepositReceived, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });

    // --- Step 2: Swap ICP → TACO directly from subaccount ---
    // Optimized: skip consolidation to main account, transfer directly from subaccount to pool
    // Then deposit + swap + withdrawToSubaccount + icrc1_transfer sends TACO to user
    // Total fees: 2 ICP_FEE (transfer + pool internal) + 2 TACO_FEE (withdrawToSubaccount + transfer)

    let poolId = switch (icpTacoPoolId) {
      case (?p) p;
      case null {
        storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Pool not configured");
        updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = ?"Pool not configured"; orderId = null; txId = null });
        releaseLock(userPrincipal);
        return #SwapFailed("ICPSwap pool not configured. Admin must call discover_pool or set_pool_id.");
      };
    };

    // poolTransferAmount: what arrives at the pool subaccount after 1 transfer fee
    let poolTransferAmount = balance - ICP_FEE;
    // swapAmountIn: net amount to swap (pool deducts internal fee from deposit)
    let swapAmountIn = if (poolTransferAmount > ICP_FEE) { poolTransferAmount - ICP_FEE } else { 0 };

    if (swapAmountIn == 0) {
      releaseLock(userPrincipal);
      return #BelowMinimum({ balance; minimum = ICP_FEE * 2 + minDepositICP });
    };

    // Get quote based on net swap amount
    updateProgress(userPrincipal, #GettingQuote, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });
    let quoteResult = try {
      await ICPSwap.getQuote({
        poolId;
        amountIn = swapAmountIn;
        amountOutMinimum = 0;
        zeroForOne = poolZeroForOne;
      });
    } catch (e) {
      storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Quote failed: " # Error.message(e));
      updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = ?("Quote failed: " # Error.message(e)); orderId = null; txId = null });
      releaseLock(userPrincipal);
      return #SwapFailed("ICPSwap quote failed: " # Error.message(e));
    };

    let (estimatedTaco, slippage) = switch (quoteResult) {
      case (#ok(q)) {
        if (q.slippage > Float.fromInt(maxSlippageBasisPoints) / 100.0) {
          storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Slippage too high: " # debug_show (q.slippage) # "%");
          updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = ?"Slippage too high"; orderId = null; txId = null });
          releaseLock(userPrincipal);
          return #SwapFailed("Slippage too high: " # debug_show (q.slippage) # "%. Max: " # debug_show (Float.fromInt(maxSlippageBasisPoints) / 100.0) # "%");
        };
        (q.amountOut, q.slippage);
      };
      case (#err(e)) {
        storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Quote error: " # e);
        updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = ?("Quote error: " # e); orderId = null; txId = null });
        releaseLock(userPrincipal);
        return #SwapFailed("ICPSwap quote error: " # e);
      };
    };

    let minTacoOut = estimatedTaco * (10000 - maxSlippageBasisPoints) / 10000;

    logger.info("SWAP", "Quote: ~" # Nat.toText(estimatedTaco) # " TACO, slippage=" # debug_show (slippage) # "%, minOut=" # Nat.toText(minTacoOut), ctx);

    // Execute: subaccount → pool transfer → deposit → swap → withdrawToSubaccount → transfer to user
    updateProgress(userPrincipal, #TransferringToPool, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = null; errorMessage = null; orderId = null; txId = null });
    let depositParams : SwapTypes.ICPSwapDepositParams = {
      poolId;
      token = ICP_LEDGER;
      amount = poolTransferAmount;
      fee = ICP_FEE;
    };
    let swapParams : SwapTypes.ICPSwapParams = {
      poolId;
      amountIn = swapAmountIn;
      minAmountOut = minTacoOut;
      zeroForOne = poolZeroForOne;
    };

    let swapResult = try {
      await ICPSwap.executeSwapFromSubaccountToRecipient(
        this_canister_id(),
        userSubaccount,
        depositParams,
        swapParams,
        userPrincipal,
        TACO_LEDGER,
        tacoLedgerFee,
      );
    } catch (e) {
      storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Swap exception: " # Error.message(e));
      updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = null; errorMessage = ?("Swap exception: " # Error.message(e)); orderId = null; txId = null });
      releaseLock(userPrincipal);
      return #SwapFailed("ICPSwap swap failed: " # Error.message(e));
    };

    let (tacoSwapped, tacoReceived) = switch (swapResult) {
      case (#ok(result)) (result.swapAmount, result.receivedAmount);
      case (#err(e)) {
        // Determine stage from error message to enable correct recovery
        let stage : Types.SwapStage = if (Text.contains(e, #text "TACO transfer failed")) {
          // Step 5 failed: TACO on canister user subaccount
          #WithdrawnToSubaccount(0) // actual amount discovered on retry
        } else if (Text.contains(e, #text "withdrawToSubaccount failed")) {
          // Step 4 failed: TACO on pool unused balance
          #SwapCompleted(0)
        } else if (Text.contains(e, #text "Swap failed")) {
          // Step 3 failed: ICP on pool unused balance
          #DepositRegistered(swapAmountIn)
        } else if (Text.contains(e, #text "Deposit registration failed")) {
          // Step 2 failed: ICP on pool subaccount
          #TransferredToPool(poolTransferAmount)
        } else if (Text.contains(e, #text "Transfer to pool failed")) {
          // Step 1 failed: ICP still on user subaccount
          #AwaitingDeposit(balance)
        } else {
          // Unknown error — assume ICP may be anywhere, start from beginning
          #AwaitingDeposit(balance)
        };
        storePendingSwap(userPrincipal, balance, stage, "Swap error: " # e);
        updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = null; errorMessage = ?("Swap error: " # e); orderId = null; txId = null });
        releaseLock(userPrincipal);
        return #SwapFailed("ICPSwap swap error: " # e);
      };
    };

    logger.info("SWAP", "Swap success: " # Nat.toText(tacoReceived) # " TACO delivered to user (raw: " # Nat.toText(tacoSwapped) # ")", ctx);

    if (tacoReceived == 0) {
      releaseLock(userPrincipal);
      return #SwapFailed("Received TACO amount too small to cover withdraw fee");
    };

    // --- Step 3: Record success ---
    // TACO sent to user via withdrawToSubaccount + icrc1_transfer
    let orderId = nextOrderId;
    nextOrderId += 1;
    let order : OrderRecord = {
      id = orderId;
      principal = userPrincipal;
      icpDeposited = balance;
      tacoReceived;
      slippage;
      poolId;
      timestamp = now();
      transakOrderId = null;
      tacoTxId = null; // txId tracked internally by icp_swap
      claimPath;
      fiatAmount;
      fiatCurrency;
    };
    Vector.add(completedOrders, order);

    totalSwapsCompleted += 1;
    totalICPSwapped += balance;
    totalTACODelivered += tacoReceived;

    // Clean up any pending swap entry
    Map.delete(pendingSwaps, phash, userPrincipal);

    updateProgress(userPrincipal, #Complete, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = ?tacoReceived; errorMessage = null; orderId = ?orderId; txId = ?orderId });

    releaseLock(userPrincipal);

    logger.info("SWAP", "Order #" # Nat.toText(orderId) # " complete: " # Nat.toText(tacoReceived) # " TACO sent to " # Principal.toText(userPrincipal), ctx);

    #Success({ tacoAmount = tacoReceived; txId = orderId; orderId });
  };

  // Store a failed swap for retry — tracks the exact stage reached
  private func storePendingSwap(principal : Principal, icpAmount : Nat, stage : Types.SwapStage, errorMsg : Text) {
    let existing = Map.get(pendingSwaps, phash, principal);
    let attempts = switch (existing) { case (?e) e.attempts + 1; case null 1 };

    Map.set(pendingSwaps, phash, principal, {
      principal;
      icpAmount;
      stage;
      createdAt = switch (existing) { case (?e) e.createdAt; case null now() };
      lastAttempt = now();
      attempts;
      errorMessage = ?errorMsg;
    });

    logger.warn("SWAP", "Stored pending swap for " # Principal.toText(principal) # " stage=" # debug_show (stage) # " (attempt " # Nat.toText(attempts) # "): " # errorMsg, "storePendingSwap");
  };

  // Stage-based recovery: check actual token locations starting from the recorded stage,
  // falling through to later stages if tokens already moved forward.
  private func retryPendingSwap(userPrincipal : Principal, pending : PendingSwap, claimPath : ClaimPath, fiatAmount : ?Text, fiatCurrency : ?Text) : async ClaimResult {
    let ctx = "retryPendingSwap";
    logger.info("SWAP", "Retrying for " # Principal.toText(userPrincipal) # " stage=" # debug_show (pending.stage) # " attempt=" # Nat.toText(pending.attempts + 1), ctx);
    updateProgress(userPrincipal, #DepositReceived, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });

    let userSubaccount = SwapUtils.principalToSubaccount(userPrincipal);
    var currentStage = pending.stage;

    let poolId = switch (icpTacoPoolId) {
      case (?p) p;
      case null {
        storePendingSwap(userPrincipal, pending.icpAmount, currentStage, "Retry: pool not configured");
        updateProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = ?"Pool not configured"; orderId = null; txId = null });
        return #SwapFailed("Pool not configured — admin must call discover_pool");
      };
    };

    // ── Stage: AwaitingDeposit ──
    switch (currentStage) {
      case (#AwaitingDeposit(_)) {
        let icpBalance = try {
          await icpLedger.icrc1_balance_of({
            owner = this_canister_id();
            subaccount = ?userSubaccount;
          });
        } catch (_e) { 0 };

        if (icpBalance > ICP_FEE * 2 + minDepositICP) {
          Map.delete(pendingSwaps, phash, userPrincipal);
          return await executeFullSwapFromSubaccount(userPrincipal, userSubaccount, icpBalance, poolId, claimPath, fiatAmount, fiatCurrency);
        };
        logger.info("SWAP", "No ICP on subaccount (" # Nat.toText(icpBalance) # "), checking pool...", ctx);
        currentStage := #TransferredToPool(0);
      };
      case _ {};
    };

    // ── Stages: TransferredToPool / DepositRegistered ──
    switch (currentStage) {
      case (#TransferredToPool(_) or #DepositRegistered(_)) {
        updateProgress(userPrincipal, #SwappingTokens, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });
        let poolBalance = try {
          let pool : SwapTypes.ICPSwapPool = actor (Principal.toText(poolId));
          await pool.getUserUnusedBalance(this_canister_id());
        } catch (_e) { #err("pool query failed") };

        switch (poolBalance) {
          case (#ok(bal)) {
            let icpOnPool = if (poolZeroForOne) { bal.balance0 } else { bal.balance1 };
            if (icpOnPool > ICP_FEE * 2) {
              logger.info("SWAP", "Found " # Nat.toText(icpOnPool) # " ICP on pool, attempting swap", ctx);
              let minOut = 0;
              let swapResult = try {
                await ICPSwap.executeSwap({
                  poolId;
                  amountIn = icpOnPool;
                  minAmountOut = minOut;
                  zeroForOne = poolZeroForOne;
                });
              } catch (e) {
                storePendingSwap(userPrincipal, pending.icpAmount, #DepositRegistered(icpOnPool), "Retry swap failed: " # Error.message(e));
                updateProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = ?("Retry swap failed: " # Error.message(e)); orderId = null; txId = null });
                return #SwapFailed("Retry swap from pool failed: " # Error.message(e));
              };

              switch (swapResult) {
                case (#ok(swapAmount)) {
                  currentStage := #SwapCompleted(swapAmount);
                };
                case (#err(e)) {
                  logger.warn("SWAP", "Swap from pool failed, recovering ICP to main: " # e, ctx);
                  let _w = try {
                    await ICPSwap.executeWithdraw(this_canister_id(), { poolId; token = ICP_LEDGER; amount = icpOnPool; fee = ICP_FEE }, false);
                  } catch (_e) { #err("withdraw failed") };
                  storePendingSwap(userPrincipal, pending.icpAmount, #DepositRegistered(icpOnPool), "Retry swap error, ICP recovered: " # e);
                  updateProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = ?"Swap failed, ICP recovered to canister main"; orderId = null; txId = null });
                  return #SwapFailed("Swap failed, ICP recovered to canister main");
                };
              };
            } else {
              logger.info("SWAP", "No ICP on pool, checking for TACO...", ctx);
              currentStage := #SwapCompleted(0);
            };
          };
          case (#err(_)) {
            currentStage := #SwapCompleted(0);
          };
        };
      };
      case _ {};
    };

    // ── Stage: SwapCompleted ──
    switch (currentStage) {
      case (#SwapCompleted(_)) {
        updateProgress(userPrincipal, #SwappingTokens, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });
        let poolBalance = try {
          let pool : SwapTypes.ICPSwapPool = actor (Principal.toText(poolId));
          await pool.getUserUnusedBalance(this_canister_id());
        } catch (_e) { #err("pool query failed") };

        switch (poolBalance) {
          case (#ok(bal)) {
            let tacoOnPool = if (poolZeroForOne) { bal.balance1 } else { bal.balance0 };
            if (tacoOnPool > tacoLedgerFee * 2) {
              logger.info("SWAP", "Found " # Nat.toText(tacoOnPool) # " TACO on pool, withdrawing to subaccount", ctx);
              let withdrawResult = try {
                await ICPSwap.executeWithdrawToSubaccount(userSubaccount, {
                  poolId;
                  token = TACO_LEDGER;
                  amount = tacoOnPool;
                  fee = tacoLedgerFee;
                });
              } catch (e) {
                storePendingSwap(userPrincipal, pending.icpAmount, #SwapCompleted(tacoOnPool), "Retry withdrawToSubaccount failed: " # Error.message(e));
                updateProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = ?("Withdraw from pool failed: " # Error.message(e)); orderId = null; txId = null });
                return #SwapFailed("Retry withdrawToSubaccount failed: " # Error.message(e));
              };

              switch (withdrawResult) {
                case (#ok(_)) {
                  let tacoAfterFee = if (tacoOnPool > tacoLedgerFee) { tacoOnPool - tacoLedgerFee } else { 0 };
                  currentStage := #WithdrawnToSubaccount(tacoAfterFee);
                };
                case (#err(e)) {
                  storePendingSwap(userPrincipal, pending.icpAmount, #SwapCompleted(tacoOnPool), "Retry withdrawToSubaccount error: " # e);
                  updateProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = ?("Withdraw from pool error: " # e); orderId = null; txId = null });
                  return #SwapFailed("Retry withdrawToSubaccount error: " # e);
                };
              };
            } else {
              logger.info("SWAP", "No TACO on pool, checking subaccount...", ctx);
              currentStage := #WithdrawnToSubaccount(0);
            };
          };
          case (#err(_)) {
            currentStage := #WithdrawnToSubaccount(0);
          };
        };
      };
      case _ {};
    };

    // ── Stage: WithdrawnToSubaccount ──
    switch (currentStage) {
      case (#WithdrawnToSubaccount(_)) {
        updateProgress(userPrincipal, #TransferringToWallet, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });
        let tacoBalance = try {
          await tacoLedgerActor.icrc1_balance_of({
            owner = this_canister_id();
            subaccount = ?userSubaccount;
          });
        } catch (_e) { 0 };

        if (tacoBalance > tacoLedgerFee * 2) {
          let tacoToSend = tacoBalance - tacoLedgerFee;
          logger.info("SWAP", "Found " # Nat.toText(tacoBalance) # " TACO on subaccount, sending " # Nat.toText(tacoToSend) # " to user", ctx);

          let transferResult = try {
            await tacoLedgerActor.icrc1_transfer({
              from_subaccount = ?userSubaccount;
              to = { owner = userPrincipal; subaccount = null };
              amount = tacoToSend;
              fee = ?tacoLedgerFee;
              memo = null;
              created_at_time = null;
            });
          } catch (e) {
            storePendingSwap(userPrincipal, pending.icpAmount, #WithdrawnToSubaccount(tacoBalance), "Retry TACO transfer failed: " # Error.message(e));
            updateProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = ?tacoToSend; errorMessage = ?("TACO transfer failed: " # Error.message(e)); orderId = null; txId = null });
            return #SwapFailed("Retry TACO transfer failed: " # Error.message(e));
          };

          switch (transferResult) {
            case (#Ok(txId)) {
              Map.delete(pendingSwaps, phash, userPrincipal);
              let orderId = nextOrderId;
              nextOrderId += 1;
              Vector.add(completedOrders, {
                id = orderId;
                principal = userPrincipal;
                icpDeposited = pending.icpAmount;
                tacoReceived = tacoToSend;
                slippage = 0.0;
                poolId;
                timestamp = now();
                transakOrderId = null;
                tacoTxId = ?txId;
                claimPath;
                fiatAmount;
                fiatCurrency;
              });
              totalSwapsCompleted += 1;
              totalTACODelivered += tacoToSend;
              updateProgress(userPrincipal, #Complete, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = ?tacoToSend; errorMessage = null; orderId = ?orderId; txId = ?txId });
              logger.info("SWAP", "Retry success: " # Nat.toText(tacoToSend) # " TACO delivered (txId=" # Nat.toText(txId) # ")", ctx);
              return #Success({ tacoAmount = tacoToSend; txId; orderId });
            };
            case (#Err(e)) {
              storePendingSwap(userPrincipal, pending.icpAmount, #WithdrawnToSubaccount(tacoBalance), "Retry TACO transfer error: " # debug_show (e));
              updateProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedTaco = null; actualTaco = ?tacoToSend; errorMessage = ?("TACO transfer error: " # debug_show (e)); orderId = null; txId = null });
              return #SwapFailed("Retry TACO transfer error: " # debug_show (e));
            };
          };
        };
        logger.info("SWAP", "No TACO on subaccount either. Nothing found at any stage.", ctx);
      };
      case _ {};
    };

    // Nothing found at any stage — clean up stale pending
    logger.warn("SWAP", "No tokens found at any stage for " # Principal.toText(userPrincipal) # ". Cleaning up stale pending.", ctx);
    Map.delete(pendingSwaps, phash, userPrincipal);
    updateProgress(userPrincipal, #NotStarted, { icpAmount = null; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });
    #NoDeposit;
  };

  // Execute the full 5-step swap from user subaccount (used by retryPendingSwap when ICP is on subaccount)
  private func executeFullSwapFromSubaccount(
    userPrincipal : Principal,
    userSubaccount : [Nat8],
    balance : Nat,
    poolId : Principal,
    claimPath : ClaimPath,
    fiatAmount : ?Text,
    fiatCurrency : ?Text,
  ) : async ClaimResult {
    let ctx = "retryFullSwap";
    updateProgress(userPrincipal, #DepositReceived, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });

    let poolTransferAmount = balance - ICP_FEE;
    let swapAmountIn = if (poolTransferAmount > ICP_FEE) { poolTransferAmount - ICP_FEE } else { 0 };

    if (swapAmountIn == 0) {
      return #BelowMinimum({ balance; minimum = ICP_FEE * 2 + minDepositICP });
    };

    updateProgress(userPrincipal, #GettingQuote, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = null; orderId = null; txId = null });
    let quoteResult = try {
      await ICPSwap.getQuote({ poolId; amountIn = swapAmountIn; amountOutMinimum = 0; zeroForOne = poolZeroForOne });
    } catch (e) {
      storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Retry quote failed: " # Error.message(e));
      updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = ?("Quote failed: " # Error.message(e)); orderId = null; txId = null });
      return #SwapFailed("Retry quote failed: " # Error.message(e));
    };

    let estimatedTaco = switch (quoteResult) {
      case (#ok(q)) { q.amountOut };
      case (#err(e)) {
        storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Retry quote error: " # e);
        updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = null; actualTaco = null; errorMessage = ?("Quote error: " # e); orderId = null; txId = null });
        return #SwapFailed("Retry quote error: " # e);
      };
    };

    let minTacoOut = estimatedTaco * (10000 - maxSlippageBasisPoints) / 10000;

    updateProgress(userPrincipal, #TransferringToPool, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = null; errorMessage = null; orderId = null; txId = null });
    let depositParams : SwapTypes.ICPSwapDepositParams = { poolId; token = ICP_LEDGER; amount = poolTransferAmount; fee = ICP_FEE };
    let swapParams : SwapTypes.ICPSwapParams = { poolId; amountIn = swapAmountIn; minAmountOut = minTacoOut; zeroForOne = poolZeroForOne };

    let swapResult = try {
      await ICPSwap.executeSwapFromSubaccountToRecipient(
        this_canister_id(), userSubaccount, depositParams, swapParams, userPrincipal, TACO_LEDGER, tacoLedgerFee,
      );
    } catch (e) {
      storePendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), "Retry swap exception: " # Error.message(e));
      updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = null; errorMessage = ?("Swap exception: " # Error.message(e)); orderId = null; txId = null });
      return #SwapFailed("Retry swap failed: " # Error.message(e));
    };

    switch (swapResult) {
      case (#ok(result)) {
        if (result.receivedAmount == 0) {
          return #SwapFailed("Received TACO too small");
        };
        Map.delete(pendingSwaps, phash, userPrincipal);
        let orderId = nextOrderId;
        nextOrderId += 1;
        Vector.add(completedOrders, {
          id = orderId;
          principal = userPrincipal;
          icpDeposited = balance;
          tacoReceived = result.receivedAmount;
          slippage = 0.0;
          poolId;
          timestamp = now();
          transakOrderId = null;
          tacoTxId = null;
          claimPath;
          fiatAmount;
          fiatCurrency;
        });
        totalSwapsCompleted += 1;
        totalICPSwapped += balance;
        totalTACODelivered += result.receivedAmount;
        updateProgress(userPrincipal, #Complete, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = ?result.receivedAmount; errorMessage = null; orderId = ?orderId; txId = ?orderId });
        logger.info("SWAP", "Retry order #" # Nat.toText(orderId) # " complete: " # Nat.toText(result.receivedAmount) # " TACO", ctx);
        return #Success({ tacoAmount = result.receivedAmount; txId = orderId; orderId });
      };
      case (#err(e)) {
        let stage : Types.SwapStage = if (Text.contains(e, #text "TACO transfer failed")) {
          #WithdrawnToSubaccount(0)
        } else if (Text.contains(e, #text "withdrawToSubaccount failed")) {
          #SwapCompleted(0)
        } else if (Text.contains(e, #text "Swap failed")) {
          #DepositRegistered(swapAmountIn)
        } else if (Text.contains(e, #text "Deposit registration failed")) {
          #TransferredToPool(poolTransferAmount)
        } else if (Text.contains(e, #text "Transfer to pool failed")) {
          #AwaitingDeposit(balance)
        } else {
          #AwaitingDeposit(balance)
        };
        storePendingSwap(userPrincipal, balance, stage, "Retry swap error: " # e);
        updateProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedTaco = ?estimatedTaco; actualTaco = null; errorMessage = ?("Swap error: " # e); orderId = null; txId = null });
        return #SwapFailed("Retry swap error: " # e);
      };
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 8: CLAIM ENDPOINTS
  // ═══════════════════════════════════════════════════════════════════

  // Register an expected payment (called by frontend before Coinbase sends ICP)
  // Creates an initial tracking record so get_swap_status() shows "Waiting for deposit"
  public shared ({ caller }) func register_payment(expectedAmount : ?Nat) : async {
    #Ok;
    #AlreadyProcessing;
    #NotAuthorized;
  } {
    if (caller == Principal.fromText("2vxsx-fae")) return #NotAuthorized;

    // Don't overwrite an in-progress swap
    switch (Map.get(swapProgress, phash, caller)) {
      case (?existing) {
        switch (existing.step) {
          case (#Complete or #NotStarted or #Failed or #WaitingForDeposit) {};
          case _ { return #AlreadyProcessing };
        };
      };
      case null {};
    };

    updateProgress(caller, #WaitingForDeposit, {
      icpAmount = expectedAmount;
      estimatedTaco = null;
      actualTaco = null;
      errorMessage = null;
      orderId = null;
      txId = null;
    });

    #Ok;
  };

  // Pad 1 (frontend auto-claim) + Pad 4 (manual claim)
  public shared ({ caller }) func claim_taco(fiatAmount : ?Text, fiatCurrency : ?Text) : async ClaimResult {
    // Spam protection
    let spamResult = spamGuard.isAllowed(caller, ?caller);
    if (spamResult >= 3) return #RateLimited;

    await processDeposit(caller, #FrontendClaim, fiatAmount, fiatCurrency);
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 9: HTTP WEBHOOK HANDLER (Transak)
  // ═══════════════════════════════════════════════════════════════════

  // Base64 standard alphabet decoder (A-Z, a-z, 0-9, +, /)
  private func base64CharVal(c : Char) : ?Nat8 {
    let code = Char.toNat32(c);
    if (code >= 65 and code <= 90) { ?(Nat8.fromNat(Nat32.toNat(code - 65))) }            // A-Z → 0-25
    else if (code >= 97 and code <= 122) { ?(Nat8.fromNat(Nat32.toNat(code - 97 + 26))) } // a-z → 26-51
    else if (code >= 48 and code <= 57) { ?(Nat8.fromNat(Nat32.toNat(code - 48 + 52))) }  // 0-9 → 52-61
    else if (c == '+') { ?62 }
    else if (c == '/') { ?63 }
    else { null };
  };

  private func base64Decode(input : Text) : ?[Nat8] {
    // Strip padding and whitespace
    let chars = Array.filter<Char>(Text.toArray(input), func(c) { c != '=' and c != ' ' and c != '\n' and c != '\r' });
    let len = chars.size();
    if (len == 0) return ?[];

    let outputLen = (len * 3) / 4;
    let result = Array.init<Nat8>(outputLen, 0 : Nat8);
    var i = 0;
    var j = 0;

    while (i + 3 < len) {
      let a = switch (base64CharVal(chars[i])) { case (?v) v; case null return null };
      let b = switch (base64CharVal(chars[i + 1])) { case (?v) v; case null return null };
      let c = switch (base64CharVal(chars[i + 2])) { case (?v) v; case null return null };
      let d = switch (base64CharVal(chars[i + 3])) { case (?v) v; case null return null };

      let triple : Nat32 =
        (Nat32.fromNat(Nat8.toNat(a)) << 18) |
        (Nat32.fromNat(Nat8.toNat(b)) << 12) |
        (Nat32.fromNat(Nat8.toNat(c)) << 6) |
        Nat32.fromNat(Nat8.toNat(d));

      if (j < outputLen) { result[j] := Nat8.fromNat(Nat32.toNat((triple >> 16) & 0xFF)); j += 1 };
      if (j < outputLen) { result[j] := Nat8.fromNat(Nat32.toNat((triple >> 8) & 0xFF)); j += 1 };
      if (j < outputLen) { result[j] := Nat8.fromNat(Nat32.toNat(triple & 0xFF)); j += 1 };
      i += 4;
    };

    // Handle remaining 2 or 3 chars
    let rem = len - i;
    if (rem == 2) {
      let a = switch (base64CharVal(chars[i])) { case (?v) v; case null return null };
      let b = switch (base64CharVal(chars[i + 1])) { case (?v) v; case null return null };
      let triple : Nat32 = (Nat32.fromNat(Nat8.toNat(a)) << 18) | (Nat32.fromNat(Nat8.toNat(b)) << 12);
      if (j < outputLen) { result[j] := Nat8.fromNat(Nat32.toNat((triple >> 16) & 0xFF)) };
    } else if (rem == 3) {
      let a = switch (base64CharVal(chars[i])) { case (?v) v; case null return null };
      let b = switch (base64CharVal(chars[i + 1])) { case (?v) v; case null return null };
      let c = switch (base64CharVal(chars[i + 2])) { case (?v) v; case null return null };
      let triple : Nat32 = (Nat32.fromNat(Nat8.toNat(a)) << 18) | (Nat32.fromNat(Nat8.toNat(b)) << 12) | (Nat32.fromNat(Nat8.toNat(c)) << 6);
      if (j < outputLen) { result[j] := Nat8.fromNat(Nat32.toNat((triple >> 16) & 0xFF)); j += 1 };
      if (j < outputLen) { result[j] := Nat8.fromNat(Nat32.toNat((triple >> 8) & 0xFF)) };
    };

    ?Array.freeze(result);
  };

  // Minimal JSON field extraction using Text operations (no external library)
  // Searches for "key":"value" or "key": "value" patterns
  private func extractJsonField(body : Text, key : Text) : ?Text {
    // Search for "key" pattern
    let searchPattern = "\"" # key # "\"";

    // Find the key in the body
    let parts = Text.split(body, #text searchPattern);
    let partsArr = Iter.toArray(parts);

    if (partsArr.size() < 2) return null;

    // Everything after the key — should start with : then value
    let afterKey = partsArr[1];
    let afterKeyChars = Text.toArray(afterKey);
    let len = afterKeyChars.size();

    // Skip whitespace and colon
    var pos : Nat = 0;
    label skipWs while (pos < len) {
      let c = afterKeyChars[pos];
      if (c == ' ' or c == '\n' or c == '\t' or c == '\r') {
        pos += 1;
      } else {
        break skipWs;
      };
    };

    if (pos >= len) return null;
    if (afterKeyChars[pos] != ':') return null;
    pos += 1;

    // Skip whitespace after colon
    label skipWs2 while (pos < len) {
      let c = afterKeyChars[pos];
      if (c == ' ' or c == '\n' or c == '\t' or c == '\r') {
        pos += 1;
      } else {
        break skipWs2;
      };
    };

    if (pos >= len) return null;

    // Check if value is a quoted string
    if (afterKeyChars[pos] == '\"') {
      pos += 1;
      // Find closing quote
      var valueEnd = pos;
      label findClose while (valueEnd < len) {
        if (afterKeyChars[valueEnd] == '\"') {
          break findClose;
        };
        valueEnd += 1;
      };
      // Extract value between quotes
      let valueChars = Array.tabulate<Char>(
        if (valueEnd > pos) { valueEnd - pos } else { 0 },
        func(i) { afterKeyChars[pos + i] },
      );
      ?Text.fromIter(valueChars.vals());
    } else {
      // Non-string value — read until delimiter
      var valueEnd = pos;
      while (valueEnd < len and afterKeyChars[valueEnd] != ',' and afterKeyChars[valueEnd] != '}' and afterKeyChars[valueEnd] != ' ' and afterKeyChars[valueEnd] != '\n') {
        valueEnd += 1;
      };
      let valueChars = Array.tabulate<Char>(
        if (valueEnd > pos) { valueEnd - pos } else { 0 },
        func(i) { afterKeyChars[pos + i] },
      );
      ?Text.fromIter(valueChars.vals());
    };
  };

  // Query: route POST requests to update call
  public query func http_request(req : HttpRequest) : async HttpResponse {
    if (req.method == "POST") {
      return {
        status_code = 200;
        headers = [("Content-Type", "application/json")];
        body = Text.encodeUtf8("{\"status\":\"upgrading\"}");
        upgrade = ?true;
      };
    };

    // GET: return basic status
    let status = if (systemPaused) "paused" else "active";
    let body = "{\"canister\":\"taco_swap\",\"status\":\"" # status # "\",\"totalSwaps\":" # Nat.toText(totalSwapsCompleted) # "}";
    {
      status_code = 200;
      headers = [("Content-Type", "application/json"), ("Access-Control-Allow-Origin", "*")];
      body = Text.encodeUtf8(body);
      upgrade = null;
    };
  };

  // Helper: detect provider from URL
  private func detectProvider(url : Text) : Text {
    if (Text.contains(url, #text "/coinbase")) "coinbase"
    else if (Text.contains(url, #text "/webhook")) "transak"
    else "unknown";
  };

  // Update: route webhook POSTs to provider-specific handlers
  public shared func http_request_update(req : HttpRequest) : async HttpResponse {
    let provider = detectProvider(req.url);
    logger.info("HTTP", "http_request_update received: method=" # req.method # " url=" # req.url # " provider=" # provider # " bodySize=" # Nat.toText(req.body.size()), "http_update");

    if (req.method != "POST") {
      logger.warn("HTTP", "Rejected non-POST: method=" # req.method # " url=" # req.url, "http_update");
      return { status_code = 405; headers = []; body = Text.encodeUtf8("Method not allowed"); upgrade = null };
    };

    // Route by URL path (check specific paths first)
    if (provider == "coinbase") {
      let response = await handleCoinbaseWebhook(req);
      logger.info("HTTP", "Coinbase webhook completed: status=" # Nat.toText(Prim.nat16ToNat(response.status_code)), "http_update");
      return response;
    };
    if (provider == "transak") {
      let response = await handleTransakWebhook(req);
      logger.info("HTTP", "Transak webhook completed: status=" # Nat.toText(Prim.nat16ToNat(response.status_code)), "http_update");
      return response;
    };

    logger.warn("HTTP", "No route matched for url=" # req.url, "http_update");
    { status_code = 404; headers = []; body = Text.encodeUtf8("Not found"); upgrade = null };
  };

  // --- Transak Webhook Handler ---
  private func handleTransakWebhook(req : HttpRequest) : async HttpResponse {
    let ctx = "transak_webhook";

    let bodyText = switch (Text.decodeUtf8(req.body)) {
      case (?t) t;
      case null {
        return { status_code = 400; headers = []; body = Text.encodeUtf8("Invalid body encoding"); upgrade = null };
      };
    };

    logger.info("TRANSAK", "Received webhook: " # Text.fromIter(Iter.fromArray(Array.subArray(Text.toArray(bodyText), 0, Nat.min(200, Text.size(bodyText))))), ctx);

    // Extract partnerCustomerId (= user principal as text)
    let principalText = switch (extractJsonField(bodyText, "partnerCustomerId")) {
      case (?p) p;
      case null {
        logger.warn("TRANSAK", "No partnerCustomerId in webhook payload", ctx);
        return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"ok\",\"note\":\"no_principal\"}"); upgrade = null };
      };
    };

    // Extract transakOrderId for dedup
    let transakOrderId = extractJsonField(bodyText, "id");

    // Check dedup
    switch (transakOrderId) {
      case (?ordId) {
        if (Map.has(processedTransakOrders, thash, ordId)) {
          logger.info("TRANSAK", "Duplicate order " # ordId # ", skipping", ctx);
          return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"duplicate\"}"); upgrade = null };
        };
      };
      case null {};
    };

    // Parse principal
    let userPrincipal = try {
      Principal.fromText(principalText);
    } catch (_e) {
      logger.warn("TRANSAK", "Invalid principal: " # principalText, ctx);
      return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"invalid_principal\"}"); upgrade = null };
    };

    // Extract fiat info from Transak payload
    let transakFiatAmount = extractJsonField(bodyText, "fiatAmount");
    let transakFiatCurrency = extractJsonField(bodyText, "fiatCurrency");

    // Process deposit (balance check is the real verification)
    let result = await processDeposit(userPrincipal, #WebhookClaim, transakFiatAmount, transakFiatCurrency);

    // Record Transak order if successful
    switch (result, transakOrderId) {
      case (#Success(s), ?ordId) {
        Map.set(processedTransakOrders, thash, ordId, s.orderId);
      };
      case _ {};
    };

    buildWebhookResponse(result);
  };

  // --- Coinbase Onramp Webhook Handler ---
  private func handleCoinbaseWebhook(req : HttpRequest) : async HttpResponse {
    let ctx = "coinbase_webhook";

    let bodyText = switch (Text.decodeUtf8(req.body)) {
      case (?t) t;
      case null {
        return { status_code = 400; headers = []; body = Text.encodeUtf8("Invalid body encoding"); upgrade = null };
      };
    };

    logger.info("COINBASE", "Received webhook: " # Text.fromIter(Iter.fromArray(Array.subArray(Text.toArray(bodyText), 0, Nat.min(200, Text.size(bodyText))))), ctx);

    // Step 1: Check eventType — only process successful onramp transactions
    let eventType = extractJsonField(bodyText, "eventType");
    switch (eventType) {
      case (?et) {
        if (et != "onramp.transaction.success" and et != "onramp.transaction.updated") {
          logger.info("COINBASE", "Ignoring event type: " # et, ctx);
          return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"ok\",\"note\":\"event_ignored\"}"); upgrade = null };
        };
      };
      case null {
        logger.warn("COINBASE", "No eventType in Coinbase webhook payload", ctx);
        return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"ok\",\"note\":\"no_event_type\"}"); upgrade = null };
      };
    };

    // Step 2: Extract partnerUserRef (= user principal)
    let principalText = switch (extractJsonField(bodyText, "partnerUserRef")) {
      case (?p) p;
      case null {
        logger.warn("COINBASE", "No partnerUserRef in Coinbase webhook payload", ctx);
        return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"ok\",\"note\":\"no_principal\"}"); upgrade = null };
      };
    };

    // Step 3: Extract transactionId for dedup
    let coinbaseTransactionId = extractJsonField(bodyText, "transactionId");

    // Step 4: Check dedup
    switch (coinbaseTransactionId) {
      case (?txId) {
        if (Map.has(processedCoinbaseOrders, thash, txId)) {
          logger.info("COINBASE", "Duplicate transaction " # txId # ", skipping", ctx);
          return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"duplicate\"}"); upgrade = null };
        };
      };
      case null {};
    };

    // Step 5: Parse principal (base64-encoded raw bytes from frontend, or plain text fallback)
    let userPrincipal = switch (base64Decode(principalText)) {
      case (?bytes) {
        if (bytes.size() > 0 and bytes.size() <= 29) {
          Principal.fromBlob(Blob.fromArray(bytes));
        } else {
          // Invalid size for a principal — try text fallback
          try { Principal.fromText(principalText) } catch (_e) {
            logger.warn("COINBASE", "Invalid principal (bad base64 length " # Nat.toText(bytes.size()) # "): " # principalText, ctx);
            return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"invalid_principal\"}"); upgrade = null };
          };
        };
      };
      case null {
        // Not valid base64 — try plain text principal
        try { Principal.fromText(principalText) } catch (_e) {
          logger.warn("COINBASE", "Invalid principal: " # principalText, ctx);
          return { status_code = 200; headers = []; body = Text.encodeUtf8("{\"status\":\"invalid_principal\"}"); upgrade = null };
        };
      };
    };

    logger.info("COINBASE", "Resolved principal: " # Principal.toText(userPrincipal), ctx);

    // Step 6: Extract fiat info from Coinbase payload
    let coinbaseFiatAmount = extractJsonField(bodyText, "fiatAmount");
    let coinbaseFiatCurrency = extractJsonField(bodyText, "fiatCurrency");

    // Step 7: Process deposit (balance check is the real verification)
    let result = await processDeposit(userPrincipal, #CoinbaseWebhook, coinbaseFiatAmount, coinbaseFiatCurrency);

    // Step 7: Record Coinbase order if successful
    switch (result, coinbaseTransactionId) {
      case (#Success(s), ?txId) {
        Map.set(processedCoinbaseOrders, thash, txId, s.orderId);
      };
      case _ {};
    };

    buildWebhookResponse(result);
  };

  // --- Shared webhook response builder ---
  private func buildWebhookResponse(result : ClaimResult) : HttpResponse {
    let responseBody = switch (result) {
      case (#Success(s)) { "{\"status\":\"success\",\"tacoAmount\":" # Nat.toText(s.tacoAmount) # ",\"orderId\":" # Nat.toText(s.orderId) # "}" };
      case (#NoDeposit) { "{\"status\":\"no_deposit\"}" };
      case (#BelowMinimum(b)) { "{\"status\":\"below_minimum\",\"balance\":" # Nat.toText(b.balance) # ",\"minimum\":" # Nat.toText(b.minimum) # "}" };
      case (#SwapFailed(msg)) { "{\"status\":\"swap_failed\",\"error\":\"" # msg # "\"}" };
      case (#AlreadyProcessing) { "{\"status\":\"processing\"}" };
      case (#SystemPaused) { "{\"status\":\"paused\"}" };
      case _ { "{\"status\":\"error\"}" };
    };
    logger.info("HTTP", "Webhook result: " # responseBody, "http_update");

    {
      status_code = 200;
      headers = [("Content-Type", "application/json"), ("Access-Control-Allow-Origin", "*")];
      body = Text.encodeUtf8(responseBody);
      upgrade = null;
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 10: ADMIN FUNCTIONS
  // ═══════════════════════════════════════════════════════════════════

  private func logAdminAction(caller : Principal, action : AdminAction, details : Text, success : Bool) {
    Vector.add(adminActions, {
      timestamp = now();
      caller;
      action;
      details;
      success;
    });
    // Trim if too many
    // (production: use circular buffer)
  };

  public shared ({ caller }) func pause(reason : Text) : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    systemPaused := true;
    logAdminAction(caller, #Pause, reason, true);
    logger.info("ADMIN", "System paused by " # Principal.toText(caller) # ": " # reason, "pause");
    #ok();
  };

  public shared ({ caller }) func unpause(reason : Text) : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    systemPaused := false;
    logAdminAction(caller, #Unpause, reason, true);
    logger.info("ADMIN", "System unpaused by " # Principal.toText(caller) # ": " # reason, "unpause");
    #ok();
  };

  public shared ({ caller }) func set_pool_id(poolId : Principal, zeroForOne : Bool) : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    icpTacoPoolId := ?poolId;
    poolZeroForOne := zeroForOne;
    logAdminAction(caller, #SetPoolId, "poolId=" # Principal.toText(poolId) # " zeroForOne=" # debug_show (zeroForOne), true);
    logger.info("ADMIN", "Pool set to " # Principal.toText(poolId) # " (zeroForOne=" # debug_show (zeroForOne) # ")", "set_pool_id");
    #ok();
  };

  public shared ({ caller }) func discover_pool() : async Result.Result<Principal, Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    await discoverPool();
  };

  private func discoverPool() : async Result.Result<Principal, Text> {
    let ctx = "discoverPool";
    logger.info("POOL", "Discovering ICP/TACO pool on ICPSwap...", ctx);

    // Try both orderings
    let result1 = try {
      await ICPSwap.getPoolByTokens(ICP_LEDGER, TACO_LEDGER);
    } catch (e) {
      #err("Exception: " # Error.message(e));
    };

    switch (result1) {
      case (#ok(poolData)) {
        let isZeroForOne = Principal.toText(Principal.fromText(poolData.token0.address)) == Principal.toText(ICP_LEDGER);
        icpTacoPoolId := ?poolData.canisterId;
        poolZeroForOne := isZeroForOne;
        logAdminAction(Principal.fromText("aaaaa-aa"), #DiscoverPool, "Found pool: " # Principal.toText(poolData.canisterId), true);
        logger.info("POOL", "Discovered pool: " # Principal.toText(poolData.canisterId) # " (zeroForOne=" # debug_show (isZeroForOne) # ")", ctx);
        return #ok(poolData.canisterId);
      };
      case (#err(_)) {};
    };

    // Try reversed order
    let result2 = try {
      await ICPSwap.getPoolByTokens(TACO_LEDGER, ICP_LEDGER);
    } catch (e) {
      #err("Exception: " # Error.message(e));
    };

    switch (result2) {
      case (#ok(poolData)) {
        let isZeroForOne = Principal.toText(Principal.fromText(poolData.token0.address)) == Principal.toText(ICP_LEDGER);
        icpTacoPoolId := ?poolData.canisterId;
        poolZeroForOne := isZeroForOne;
        logAdminAction(Principal.fromText("aaaaa-aa"), #DiscoverPool, "Found pool (reversed): " # Principal.toText(poolData.canisterId), true);
        logger.info("POOL", "Discovered pool (reversed): " # Principal.toText(poolData.canisterId) # " (zeroForOne=" # debug_show (isZeroForOne) # ")", ctx);
        return #ok(poolData.canisterId);
      };
      case (#err(e)) {
        logAdminAction(Principal.fromText("aaaaa-aa"), #DiscoverPool, "Pool not found: " # e, false);
        logger.warn("POOL", "ICP/TACO pool not found: " # e, ctx);
        return #err("ICP/TACO pool not found on ICPSwap");
      };
    };
  };

  public shared ({ caller }) func update_config(config : SwapConfig) : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");

    switch (config.maxSlippageBasisPoints) { case (?v) { maxSlippageBasisPoints := v }; case null {} };
    switch (config.minDepositICP) { case (?v) { minDepositICP := v }; case null {} };
    switch (config.sweepIntervalNS) { case (?v) { sweepIntervalNS := v }; case null {} };
    switch (config.maxRetries) { case (?v) { maxRetries := v }; case null {} };
    switch (config.systemPaused) { case (?v) { systemPaused := v }; case null {} };

    logAdminAction(caller, #UpdateConfig, debug_show (config), true);
    logger.info("ADMIN", "Config updated by " # Principal.toText(caller), "update_config");
    #ok();
  };

  public shared ({ caller }) func recover_stuck_funds(principal : Principal) : async Result.Result<Nat, Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    let ctx = "recoverFunds";

    // Check subaccount balance
    let subaccount = SwapUtils.principalToSubaccount(principal);
    let balance = try {
      await icpLedger.icrc1_balance_of({
        owner = this_canister_id();
        subaccount = ?subaccount;
      });
    } catch (e) {
      return #err("Balance check failed: " # Error.message(e));
    };

    if (balance == 0) return #err("No funds on subaccount");

    if (balance <= ICP_FEE) return #err("Balance too small to recover (only " # Nat.toText(balance) # " e8s)");

    // Transfer to canister main account
    let transferResult = try {
      await icpLedger.icrc1_transfer({
        from_subaccount = ?subaccount;
        to = { owner = this_canister_id(); subaccount = null };
        amount = balance - ICP_FEE;
        fee = ?ICP_FEE;
        memo = null;
        created_at_time = null;
      });
    } catch (e) {
      return #err("Transfer failed: " # Error.message(e));
    };

    switch (transferResult) {
      case (#Ok(_)) {
        let recovered = balance - ICP_FEE;
        logAdminAction(caller, #RecoverFunds, "Recovered " # Nat.toText(recovered) # " e8s from " # Principal.toText(principal), true);
        logger.info("ADMIN", "Recovered " # Nat.toText(recovered) # " e8s from subaccount of " # Principal.toText(principal), ctx);
        #ok(recovered);
      };
      case (#Err(e)) {
        #err("Transfer error: " # debug_show (e));
      };
    };
  };

  public shared ({ caller }) func retry_pending_swaps() : async Result.Result<Nat, Text> {
    if (not isAdmin(caller)) return #err("Not authorized");

    var retried : Nat = 0;
    let entries = Iter.toArray(Map.entries(pendingSwaps));
    for ((principal, _pending) in entries.vals()) {
      let _result = await processDeposit(principal, #TimerSweep, null, null);
      retried += 1;
    };

    logAdminAction(caller, #RetryPending, "Retried " # Nat.toText(retried) # " pending swaps", true);
    #ok(retried);
  };

  // Refund ICP from a failed pending swap back to the user
  public shared ({ caller }) func refund_pending(principal : Principal) : async Result.Result<Nat, Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    let ctx = "refundPending";

    let pending = switch (Map.get(pendingSwaps, phash, principal)) {
      case (?p) p;
      case null { return #err("No pending swap for " # Principal.toText(principal)) };
    };

    // Only refund if ICP is still on user subaccount (AwaitingDeposit stage)
    switch (pending.stage) {
      case (#AwaitingDeposit(_)) {};
      case (other) {
        return #err("Cannot refund — swap already progressed past deposit stage: " # debug_show (other));
      };
    };

    if (pending.icpAmount == 0) {
      return #err("No ICP to refund");
    };

    if (pending.icpAmount <= ICP_FEE) {
      Map.delete(pendingSwaps, phash, principal);
      return #err("ICP amount too small to refund (" # Nat.toText(pending.icpAmount) # " e8s)");
    };

    let refundAmount = pending.icpAmount - ICP_FEE;

    // Refund from user's subaccount (where ICP sits in AwaitingDeposit stage)
    let userSubaccount = SwapUtils.principalToSubaccount(principal);
    let transferResult = try {
      await icpLedger.icrc1_transfer({
        from_subaccount = ?userSubaccount;
        to = { owner = principal; subaccount = null };
        amount = refundAmount;
        fee = ?ICP_FEE;
        memo = null;
        created_at_time = null;
      });
    } catch (e) {
      return #err("Refund transfer failed: " # Error.message(e));
    };

    switch (transferResult) {
      case (#Ok(_txId)) {
        Map.delete(pendingSwaps, phash, principal);
        logAdminAction(caller, #RecoverFunds, "Refunded " # Nat.toText(refundAmount) # " e8s ICP to " # Principal.toText(principal), true);
        logger.info("ADMIN", "Refunded " # Nat.toText(refundAmount) # " e8s to " # Principal.toText(principal), ctx);
        #ok(refundAmount);
      };
      case (#Err(e)) {
        #err("Refund transfer error: " # debug_show (e));
      };
    };
  };

  // Clear a stale pending swap entry (e.g., ICP already consumed but pending record remains)
  public shared ({ caller }) func admin_clear_pending(principal : Principal) : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    switch (Map.get(pendingSwaps, phash, principal)) {
      case (?pending) {
        Map.delete(pendingSwaps, phash, principal);
        logAdminAction(caller, #RecoverFunds, "Cleared pending swap for " # Principal.toText(principal) # " (icpAmount=" # Nat.toText(pending.icpAmount) # ", stage=" # debug_show (pending.stage) # ")", true);
        #ok();
      };
      case null { #err("No pending swap for " # Principal.toText(principal)) };
    };
  };

  public shared ({ caller }) func recover_icpswap_balances() : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");

    switch (icpTacoPoolId) {
      case (?poolId) {
        // Build minimal token details for recovery
        let recoveryMap = Map.new<Principal, TokenDetails>();
        // We need ICP and TACO token details
        try {
          let icpFee = await icpLedger.icrc1_fee();
          let tacoFee = await tacoLedgerActor.icrc1_fee();
          let icpDetails : TokenDetails = {
            tokenSymbol = "ICP";
            tokenName = "Internet Computer";
            tokenDecimals = 8;
            tokenTransferFee = icpFee;
            tokenType = #ICP;
            balance = 0;
            priceInICP = 100_000_000;
            priceInUSD = 0.0;
            pastPrices = [];
            Active = true;
            isPaused = false;
            pausedDueToSyncFailure = false;
            lastTimeSynced = now();
            epochAdded = now();
          };
          Map.set(recoveryMap, phash, ICP_LEDGER, icpDetails);
          let tacoDetails : TokenDetails = {
            tokenSymbol = "TACO";
            tokenName = "TACO";
            tokenDecimals = Nat8.toNat(tacoDecimals);
            tokenTransferFee = tacoFee;
            tokenType = #ICRC12;
            balance = 0;
            priceInICP = 0;
            priceInUSD = 0.0;
            pastPrices = [];
            Active = true;
            isPaused = false;
            pausedDueToSyncFailure = false;
            lastTimeSynced = now();
            epochAdded = now();
          };
          Map.set(recoveryMap, phash, TACO_LEDGER, tacoDetails);

          await* ICPSwap.recoverBalanceFromSpecificPool(
            this_canister_id(),
            poolId,
            [ICP_LEDGER, TACO_LEDGER],
            recoveryMap,
          );
          #ok();
        } catch (e) {
          #err("Recovery failed: " # Error.message(e));
        };
      };
      case null { #err("Pool not configured") };
    };
  };

  // Admin: transfer ICP from canister main account to a specified recipient
  // For recovering orphaned ICP (e.g., from failed swaps where pending record was lost)
  public shared ({ caller }) func admin_transfer_icp(
    recipient : Principal,
    recipientSubaccount : ?[Nat8],
    amount : Nat,
  ) : async Result.Result<Nat, Text> {
    if (not isAdmin(caller)) return #err("Not authorized");

    if (amount <= ICP_FEE) {
      return #err("Amount too small to transfer (must be > " # Nat.toText(ICP_FEE) # " e8s)");
    };

    let transferResult = try {
      await icpLedger.icrc1_transfer({
        from_subaccount = null;
        to = { owner = recipient; subaccount = recipientSubaccount };
        amount = amount - ICP_FEE;
        fee = ?ICP_FEE;
        memo = null;
        created_at_time = null;
      });
    } catch (e) {
      return #err("Transfer failed: " # Error.message(e));
    };

    switch (transferResult) {
      case (#Ok(txId)) {
        let sent = amount - ICP_FEE;
        logAdminAction(caller, #RecoverFunds, "Transferred " # Nat.toText(sent) # " e8s ICP to " # Principal.toText(recipient), true);
        logger.info("ADMIN", "admin_transfer_icp: " # Nat.toText(sent) # " e8s sent to " # Principal.toText(recipient) # " txId=" # Nat.toText(txId), "admin_transfer_icp");
        #ok(txId);
      };
      case (#Err(e)) {
        #err("Transfer error: " # debug_show (e));
      };
    };
  };

  // Admin: transfer TACO from canister main account to a specified recipient
  // For distributing recovered TACO from pool recovery
  public shared ({ caller }) func admin_transfer_taco(
    recipient : Principal,
    amount : Nat,
  ) : async Result.Result<Nat, Text> {
    if (not isAdmin(caller)) return #err("Not authorized");

    if (amount <= tacoLedgerFee) {
      return #err("Amount too small to transfer (must be > " # Nat.toText(tacoLedgerFee) # ")");
    };

    let tacoToSend = amount - tacoLedgerFee;

    let transferResult = try {
      await tacoLedgerActor.icrc1_transfer({
        from_subaccount = null;
        to = { owner = recipient; subaccount = null };
        amount = tacoToSend;
        fee = ?tacoLedgerFee;
        memo = null;
        created_at_time = null;
      });
    } catch (e) {
      return #err("Transfer failed: " # Error.message(e));
    };

    switch (transferResult) {
      case (#Ok(txId)) {
        logAdminAction(caller, #RecoverFunds, "Transferred " # Nat.toText(tacoToSend) # " TACO to " # Principal.toText(recipient), true);
        logger.info("ADMIN", "admin_transfer_taco: " # Nat.toText(tacoToSend) # " TACO sent to " # Principal.toText(recipient) # " txId=" # Nat.toText(txId), "admin_transfer_taco");
        #ok(txId);
      };
      case (#Err(e)) {
        #err("Transfer error: " # debug_show (e));
      };
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 11: QUERY FUNCTIONS
  // ═══════════════════════════════════════════════════════════════════

  // Real-time swap status for frontend polling (every ~5 seconds)
  public query ({ caller }) func get_swap_status() : async Types.SwapProgress {
    switch (Map.get(swapProgress, phash, caller)) {
      case (?progress) progress;
      case null {
        // Fallback: synthesize from pendingSwaps if it exists
        switch (Map.get(pendingSwaps, phash, caller)) {
          case (?pending) {
            let desc = switch (pending.stage) {
              case (#AwaitingDeposit(_)) "Swap failed at deposit stage. Will retry automatically.";
              case (#TransferredToPool(_)) "Swap failed during pool transfer. Will retry automatically.";
              case (#DepositRegistered(_)) "Swap failed during deposit registration. Will retry automatically.";
              case (#SwapCompleted(_)) "Swap partially complete. TACO on pool. Will retry automatically.";
              case (#WithdrawnToSubaccount(_)) "TACO ready for delivery. Will retry automatically.";
            };
            {
              step = #Failed;
              stepNumber = 0 : Nat;
              totalSteps = 7 : Nat;
              description = desc;
              icpAmount = ?pending.icpAmount;
              estimatedTaco = null;
              actualTaco = null;
              startedAt = pending.createdAt;
              updatedAt = pending.lastAttempt;
              errorMessage = pending.errorMessage;
              retryCount = pending.attempts;
              orderId = null;
              txId = null;
            };
          };
          case null {
            {
              step = #NotStarted;
              stepNumber = 0 : Nat;
              totalSteps = 7 : Nat;
              description = "No swap in progress.";
              icpAmount = null;
              estimatedTaco = null;
              actualTaco = null;
              startedAt = 0;
              updatedAt = 0;
              errorMessage = null;
              retryCount = 0 : Nat;
              orderId = null;
              txId = null;
            };
          };
        };
      };
    };
  };

  // Combined TACO + NACHOS status in one query call.
  // Pure query — no async ledger calls. Frontend checks ICP balances
  // directly via ICP ledger query using the returned subaccounts.
  public query ({ caller }) func get_full_swap_state() : async {
    tacoStatus : Types.SwapProgress;
    nachosStatus : Types.NachosSwapProgress;
    hasActiveLock : Bool;
    hasPendingTaco : Bool;
    hasPendingNachos : Bool;
    tacoDepositSubaccount : [Nat8];
    nachosDepositSubaccount : [Nat8];
  } {
    // TACO status
    let tacoStatus = switch (Map.get(swapProgress, phash, caller)) {
      case (?progress) progress;
      case null {
        switch (Map.get(pendingSwaps, phash, caller)) {
          case (?pending) {
            let desc = switch (pending.stage) {
              case (#AwaitingDeposit(_)) "Swap failed at deposit stage. Will retry automatically.";
              case (#TransferredToPool(_)) "Swap failed during pool transfer. Will retry automatically.";
              case (#DepositRegistered(_)) "Swap failed during deposit registration. Will retry automatically.";
              case (#SwapCompleted(_)) "Swap partially complete. TACO on pool. Will retry automatically.";
              case (#WithdrawnToSubaccount(_)) "TACO ready for delivery. Will retry automatically.";
            };
            {
              step = #Failed;
              stepNumber = 0 : Nat;
              totalSteps = 7 : Nat;
              description = desc;
              icpAmount = ?pending.icpAmount;
              estimatedTaco = null;
              actualTaco = null;
              startedAt = pending.createdAt;
              updatedAt = pending.lastAttempt;
              errorMessage = pending.errorMessage;
              retryCount = pending.attempts;
              orderId = null;
              txId = null;
            };
          };
          case null {
            {
              step = #NotStarted;
              stepNumber = 0 : Nat;
              totalSteps = 7 : Nat;
              description = "No swap in progress.";
              icpAmount = null;
              estimatedTaco = null;
              actualTaco = null;
              startedAt = 0;
              updatedAt = 0;
              errorMessage = null;
              retryCount = 0 : Nat;
              orderId = null;
              txId = null;
            };
          };
        };
      };
    };

    // NACHOS status
    let nachosStatus = switch (Map.get(nachosSwapProgress, phash, caller)) {
      case (?progress) progress;
      case null {
        switch (Map.get(nachosPendingSwaps, phash, caller)) {
          case (?pending) {
            let desc = switch (pending.stage) {
              case (#AwaitingDeposit(_)) "NACHOS mint failed at deposit stage. Will retry automatically.";
              case (#TransferredToTreasury(_)) "ICP transferred to treasury. Waiting to mint NACHOS...";
              case (#MintRequested(_)) "NACHOS mint in progress...";
            };
            {
              step = #Failed : Types.NachosSwapStep;
              stepNumber = 0 : Nat;
              totalSteps = 4 : Nat;
              description = desc;
              icpAmount = ?pending.icpAmount;
              estimatedNachos = null;
              actualNachos = null;
              startedAt = pending.createdAt;
              updatedAt = pending.lastAttempt;
              errorMessage = pending.errorMessage;
              retryCount = pending.attempts;
              orderId = null;
              mintId = null;
            };
          };
          case null {
            {
              step = #NotStarted : Types.NachosSwapStep;
              stepNumber = 0 : Nat;
              totalSteps = 4 : Nat;
              description = "No NACHOS mint in progress.";
              icpAmount = null;
              estimatedNachos = null;
              actualNachos = null;
              startedAt = 0;
              updatedAt = 0;
              errorMessage = null;
              retryCount = 0 : Nat;
              orderId = null;
              mintId = null;
            };
          };
        };
      };
    };

    // Lock status
    let locked = switch (Map.get(operationLocks, phash, caller)) {
      case (?lockTime) { now() - lockTime < LOCK_TIMEOUT_NS };
      case null { false };
    };

    {
      tacoStatus;
      nachosStatus;
      hasActiveLock = locked;
      hasPendingTaco = Map.has(pendingSwaps, phash, caller);
      hasPendingNachos = Map.has(nachosPendingSwaps, phash, caller);
      tacoDepositSubaccount = SwapUtils.principalToSubaccount(caller);
      nachosDepositSubaccount = nachosSubaccount(caller);
    };
  };

  // Bundled dashboard query — replaces 5-8 separate calls with one
  public query ({ caller }) func getSwapDashboard() : async Types.SwapDashboard {
    // --- TACO status (from get_full_swap_state) ---
    let tacoStatus = switch (Map.get(swapProgress, phash, caller)) {
      case (?progress) progress;
      case null {
        switch (Map.get(pendingSwaps, phash, caller)) {
          case (?pending) {
            let desc = switch (pending.stage) {
              case (#AwaitingDeposit(_)) "Swap failed at deposit stage. Will retry automatically.";
              case (#TransferredToPool(_)) "Swap failed during pool transfer. Will retry automatically.";
              case (#DepositRegistered(_)) "Swap failed during deposit registration. Will retry automatically.";
              case (#SwapCompleted(_)) "Swap partially complete. TACO on pool. Will retry automatically.";
              case (#WithdrawnToSubaccount(_)) "TACO ready for delivery. Will retry automatically.";
            };
            {
              step = #Failed;
              stepNumber = 0 : Nat;
              totalSteps = 7 : Nat;
              description = desc;
              icpAmount = ?pending.icpAmount;
              estimatedTaco = null;
              actualTaco = null;
              startedAt = pending.createdAt;
              updatedAt = pending.lastAttempt;
              errorMessage = pending.errorMessage;
              retryCount = pending.attempts;
              orderId = null;
              txId = null;
            };
          };
          case null {
            {
              step = #NotStarted;
              stepNumber = 0 : Nat;
              totalSteps = 7 : Nat;
              description = "No swap in progress.";
              icpAmount = null;
              estimatedTaco = null;
              actualTaco = null;
              startedAt = 0;
              updatedAt = 0;
              errorMessage = null;
              retryCount = 0 : Nat;
              orderId = null;
              txId = null;
            };
          };
        };
      };
    };

    // --- NACHOS status (from get_full_swap_state) ---
    let nachosStatus = switch (Map.get(nachosSwapProgress, phash, caller)) {
      case (?progress) progress;
      case null {
        switch (Map.get(nachosPendingSwaps, phash, caller)) {
          case (?pending) {
            let desc = switch (pending.stage) {
              case (#AwaitingDeposit(_)) "NACHOS mint failed at deposit stage. Will retry automatically.";
              case (#TransferredToTreasury(_)) "ICP transferred to treasury. Waiting to mint NACHOS...";
              case (#MintRequested(_)) "NACHOS mint in progress...";
            };
            {
              step = #Failed : Types.NachosSwapStep;
              stepNumber = 0 : Nat;
              totalSteps = 4 : Nat;
              description = desc;
              icpAmount = ?pending.icpAmount;
              estimatedNachos = null;
              actualNachos = null;
              startedAt = pending.createdAt;
              updatedAt = pending.lastAttempt;
              errorMessage = pending.errorMessage;
              retryCount = pending.attempts;
              orderId = null;
              mintId = null;
            };
          };
          case null {
            {
              step = #NotStarted : Types.NachosSwapStep;
              stepNumber = 0 : Nat;
              totalSteps = 4 : Nat;
              description = "No NACHOS mint in progress.";
              icpAmount = null;
              estimatedNachos = null;
              actualNachos = null;
              startedAt = 0;
              updatedAt = 0;
              errorMessage = null;
              retryCount = 0 : Nat;
              orderId = null;
              mintId = null;
            };
          };
        };
      };
    };

    // --- Lock status ---
    let locked = switch (Map.get(operationLocks, phash, caller)) {
      case (?lockTime) { now() - lockTime < LOCK_TIMEOUT_NS };
      case null { false };
    };

    // --- Deposit addresses ---
    let depositAddress = bytesToHex(Blob.toArray(computeAccountIdentifier(this_canister_id(), null)));
    let nachosDepositAddress = bytesToHex(Blob.toArray(computeAccountIdentifier(this_canister_id(), ?nachosSubaccountBlob(caller))));

    // --- Recent TACO orders (newest first, limit 20) ---
    let tacoOrders = Vector.new<OrderRecord>();
    var tacoCount : Nat = 0;
    var ti = Vector.size(completedOrders);
    label tacoIter while (ti > 0 and tacoCount < 20) {
      ti -= 1;
      let order = Vector.get(completedOrders, ti);
      if (order.principal == caller) {
        Vector.add(tacoOrders, order);
        tacoCount += 1;
      };
    };

    // --- Recent NACHOS orders (newest first, limit 20) ---
    let nachosOrders = Vector.new<NachosOrderRecord>();
    var nachosCount : Nat = 0;
    var ni = Vector.size(nachosCompletedOrders);
    label nachosIter while (ni > 0 and nachosCount < 20) {
      ni -= 1;
      let order = Vector.get(nachosCompletedOrders, ni);
      if (order.principal == caller) {
        Vector.add(nachosOrders, order);
        nachosCount += 1;
      };
    };

    {
      tacoStatus;
      nachosStatus;
      hasActiveLock = locked;
      hasPendingTaco = Map.has(pendingSwaps, phash, caller);
      hasPendingNachos = Map.has(nachosPendingSwaps, phash, caller);
      tacoDepositSubaccount = SwapUtils.principalToSubaccount(caller);
      nachosDepositSubaccount = nachosSubaccount(caller);
      depositAddress;
      nachosDepositAddress;
      recentTacoOrders = Vector.toArray(tacoOrders);
      recentNachosOrders = Vector.toArray(nachosOrders);
      config = {
        systemPaused;
        maxSlippageBasisPoints;
        minDepositICP;
        sweepIntervalNS;
        maxRetries;
        icpTacoPoolId;
        poolZeroForOne;
        tacoLedgerFee;
      };
      stats = {
        totalSwapsCompleted;
        totalICPSwapped;
        totalTACODelivered;
        pendingSwapsCount = Map.size(pendingSwaps);
        completedOrdersCount = Vector.size(completedOrders);
        systemPaused;
        poolConfigured = icpTacoPoolId != null;
      };
      nachosStats = {
        totalNachosMints;
        totalNachosICPDeposited;
        totalNachosDelivered;
        nachosPendingCount = Map.size(nachosPendingSwaps);
        nachosOrdersCount = Vector.size(nachosCompletedOrders);
        nachosMintingEnabled;
      };
    };
  };

  // Admin: view all swap progress entries
  public query ({ caller }) func get_all_swap_progress() : async [(Principal, Types.SwapProgress)] {
    if (not isAdmin(caller)) return [];
    Iter.toArray(Map.entries(swapProgress));
  };

  public query func get_config() : async {
    systemPaused : Bool;
    maxSlippageBasisPoints : Nat;
    minDepositICP : Nat;
    sweepIntervalNS : Int;
    maxRetries : Nat;
    icpTacoPoolId : ?Principal;
    poolZeroForOne : Bool;
    tacoLedgerFee : Nat;
  } {
    {
      systemPaused;
      maxSlippageBasisPoints;
      minDepositICP;
      sweepIntervalNS;
      maxRetries;
      icpTacoPoolId;
      poolZeroForOne;
      tacoLedgerFee;
    };
  };

  public query func get_stats() : async Types.SwapStats {
    {
      totalSwapsCompleted;
      totalICPSwapped;
      totalTACODelivered;
      pendingSwapsCount = Map.size(pendingSwaps);
      completedOrdersCount = Vector.size(completedOrders);
      systemPaused;
      poolConfigured = icpTacoPoolId != null;
    };
  };

  public query func get_order_history(limit : Nat, offset : Nat) : async [OrderRecord] {
    let size = Vector.size(completedOrders);
    if (offset >= size) return [];
    let result = Vector.new<OrderRecord>();
    // Return newest first
    var i = size;
    var count : Nat = 0;
    var skipped : Nat = 0;
    label iter while (i > 0) {
      i -= 1;
      if (skipped < offset) {
        skipped += 1;
      } else if (count < limit) {
        Vector.add(result, Vector.get(completedOrders, i));
        count += 1;
      } else {
        break iter;
      };
    };
    Vector.toArray(result);
  };

  public query func get_pending_swaps() : async [(Principal, PendingSwap)] {
    Iter.toArray(Map.entries(pendingSwaps));
  };

  public shared func get_taco_quote(icpAmount : Nat) : async Types.QuoteResult {
    let poolId = switch (icpTacoPoolId) {
      case (?p) p;
      case null { return #Err("Pool not configured") };
    };

    if (icpAmount <= ICP_FEE * 2) return #Err("Amount too small");

    let netIcp = icpAmount - ICP_FEE; // Account for consolidation fee

    let quoteResult = try {
      await ICPSwap.getQuote({
        poolId;
        amountIn = netIcp;
        amountOutMinimum = 0;
        zeroForOne = poolZeroForOne;
      });
    } catch (e) {
      return #Err("Quote failed: " # Error.message(e));
    };

    switch (quoteResult) {
      case (#ok(q)) {
        // Account for fees: TACO transfer fee + ICPSwap withdraw fee
        let estimatedTaco = if (q.amountOut > tacoLedgerFee * 2) {
          q.amountOut - tacoLedgerFee * 2; // withdraw fee + transfer fee
        } else { 0 };
        #Ok({
          estimatedTaco;
          slippage = q.slippage;
          icpFee = ICP_FEE;
          tacoFee = tacoLedgerFee;
        });
      };
      case (#err(e)) { #Err(e) };
    };
  };

  // Get user's order history
  public query ({ caller }) func get_my_orders(limit : Nat) : async [OrderRecord] {
    let result = Vector.new<OrderRecord>();
    var count : Nat = 0;
    let size = Vector.size(completedOrders);
    var i = size;
    label iter while (i > 0 and count < limit) {
      i -= 1;
      let order = Vector.get(completedOrders, i);
      if (order.principal == caller) {
        Vector.add(result, order);
        count += 1;
      };
    };
    Vector.toArray(result);
  };

  // Admin: get logs
  public shared ({ caller }) func get_logs(count : Nat, context : ?Text) : async [DAO_types.LogEntry] {
    if (not isAdmin(caller)) return [];
    switch (context) {
      case (?ctx) { logger.getContextLogs(ctx, count) };
      case null { logger.getLastLogs(count) };
    };
  };

  public query func get_processed_transak_orders() : async [(Text, Nat)] {
    Iter.toArray(Map.entries(processedTransakOrders));
  };

  public query func get_processed_coinbase_orders() : async [(Text, Nat)] {
    Iter.toArray(Map.entries(processedCoinbaseOrders));
  };

  public query func get_admin_actions(limit : Nat) : async [AdminActionRecord] {
    let size = Vector.size(adminActions);
    let result = Vector.new<AdminActionRecord>();
    var count : Nat = 0;
    var i = size;
    label iter while (i > 0 and count < limit) {
      i -= 1;
      Vector.add(result, Vector.get(adminActions, i));
      count += 1;
    };
    Vector.toArray(result);
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 12: NACHOS MINTING
  // ═══════════════════════════════════════════════════════════════════

  // --- NACHOS Deposit Address ---

  public shared ({ caller }) func get_nachos_deposit_address_for(user : ?Principal) : async Text {
    let targetPrincipal = switch (user) {
      case (?p) {
        if (p != caller and not isAdmin(caller)) {
          Debug.trap("Not authorized to get NACHOS deposit address for another principal");
        };
        p;
      };
      case null { caller };
    };
    if (targetPrincipal == Principal.fromText("2vxsx-fae")) {
      Debug.trap("Cannot generate deposit address for anonymous principal");
    };
    let subaccount = nachosSubaccountBlob(targetPrincipal);
    let aid = computeAccountIdentifier(this_canister_id(), ?subaccount);
    bytesToHex(Blob.toArray(aid));
  };

  // --- NACHOS Balance Check ---

  public shared ({ caller }) func get_pending_nachos_balance() : async Nat {
    if (caller == Principal.fromText("2vxsx-fae")) return 0;
    let subaccount = nachosSubaccount(caller);
    await icpLedger.icrc1_balance_of({
      owner = this_canister_id();
      subaccount = ?subaccount;
    });
  };

  // --- NACHOS Quote ---

  public shared func get_nachos_quote(icpAmount : Nat) : async Types.NachosQuoteResult {
    if (icpAmount <= ICP_FEE) return #Err("Amount too small");
    let netIcp = icpAmount - ICP_FEE;
    try {
      let estimate = await nachosVault.estimateMintICP(netIcp);
      #Ok({ estimatedNachos = estimate.nachosEstimate; feeEstimate = estimate.feeEstimate; navUsed = estimate.navUsed });
    } catch (e) {
      #Err("Quote failed: " # Error.message(e));
    };
  };

  // --- NACHOS Claim ---

  public shared ({ caller }) func claim_nachos(minimumNachosReceive : Nat, fiatAmount : ?Text, fiatCurrency : ?Text) : async NachosClaimResult {
    let spamResult = spamGuard.isAllowed(caller, ?caller);
    if (spamResult >= 3) return #RateLimited;
    await processNachosDeposit(caller, minimumNachosReceive, #FrontendClaim, fiatAmount, fiatCurrency);
  };

  // --- NACHOS Progress Helpers ---

  private func nachosStepToDisplay(step : Types.NachosSwapStep) : (Nat, Text) {
    switch (step) {
      case (#NotStarted) (0, "No NACHOS mint in progress.");
      case (#DepositReceived) (1, "ICP deposit received. Preparing mint...");
      case (#TransferringToTreasury) (2, "Transferring ICP to NACHOS treasury...");
      case (#MintingNachos) (3, "Minting NACHOS tokens...");
      case (#Complete) (4, "Mint complete! NACHOS delivered.");
      case (#Failed) (0, "Mint encountered an error. Retrying...");
    };
  };

  private func updateNachosProgress(
    principal : Principal,
    step : Types.NachosSwapStep,
    updates : {
      icpAmount : ?Nat;
      estimatedNachos : ?Nat;
      actualNachos : ?Nat;
      errorMessage : ?Text;
      orderId : ?Nat;
      mintId : ?Nat;
    },
  ) {
    let existing = Map.get(nachosSwapProgress, phash, principal);
    let (stepNumber, description) = nachosStepToDisplay(step);

    func mergeOpt(newVal : ?Nat, field : ?Nat) : ?Nat {
      switch (newVal) { case (?v) ?v; case null field };
    };

    let prev : NachosSwapProgress = switch (existing) {
      case (?e) e;
      case null {
        {
          step = #NotStarted;
          stepNumber = 0;
          totalSteps = 4;
          description = "";
          icpAmount = null;
          estimatedNachos = null;
          actualNachos = null;
          startedAt = now();
          updatedAt = now();
          errorMessage = null;
          retryCount = 0;
          orderId = null;
          mintId = null;
        };
      };
    };

    Map.set(nachosSwapProgress, phash, principal, {
      step;
      stepNumber;
      totalSteps = 4 : Nat;
      description;
      icpAmount = mergeOpt(updates.icpAmount, prev.icpAmount);
      estimatedNachos = mergeOpt(updates.estimatedNachos, prev.estimatedNachos);
      actualNachos = mergeOpt(updates.actualNachos, prev.actualNachos);
      startedAt = prev.startedAt;
      updatedAt = now();
      errorMessage = updates.errorMessage;
      retryCount = if (step == #Failed) { prev.retryCount + 1 } else { prev.retryCount };
      orderId = mergeOpt(updates.orderId, prev.orderId);
      mintId = mergeOpt(updates.mintId, prev.mintId);
    });
  };

  // --- NACHOS Pending Swap Storage ---

  private func storeNachosPendingSwap(principal : Principal, icpAmount : Nat, stage : Types.NachosSwapStage, blockNumber : ?Nat, errorMsg : Text) {
    let existing = Map.get(nachosPendingSwaps, phash, principal);
    let attempts = switch (existing) { case (?e) e.attempts + 1; case null 1 };

    Map.set(nachosPendingSwaps, phash, principal, {
      principal;
      icpAmount;
      stage;
      blockNumber;
      createdAt = switch (existing) { case (?e) e.createdAt; case null now() };
      lastAttempt = now();
      attempts;
      errorMessage = ?errorMsg;
    });

    logger.warn("NACHOS", "Stored pending swap for " # Principal.toText(principal) # " stage=" # debug_show (stage) # " (attempt " # Nat.toText(attempts) # "): " # errorMsg, "storeNachosPendingSwap");
  };

  // --- NACHOS Retry Logic ---

  private func retryNachosPendingSwap(userPrincipal : Principal, pending : NachosPendingSwap, minimumNachosReceive : Nat, claimPath : ClaimPath, fiatAmount : ?Text, fiatCurrency : ?Text) : async NachosClaimResult {
    let ctx = "retryNachosPendingSwap";
    logger.info("NACHOS", "Retrying for " # Principal.toText(userPrincipal) # " stage=" # debug_show (pending.stage) # " attempt=" # Nat.toText(pending.attempts + 1), ctx);

    switch (pending.stage) {
      case (#AwaitingDeposit(_)) {
        // ICP should still be on nachos subaccount — restart from beginning
        let userNachosSub = nachosSubaccount(userPrincipal);
        let balance = try {
          await icpLedger.icrc1_balance_of({
            owner = this_canister_id();
            subaccount = ?userNachosSub;
          });
        } catch (_e) { 0 };

        if (balance > ICP_FEE + nachosMinDepositICP) {
          Map.delete(nachosPendingSwaps, phash, userPrincipal);
          return await processNachosDeposit(userPrincipal, minimumNachosReceive, claimPath, fiatAmount, fiatCurrency);
        };
        // No balance — maybe already transferred. Check treasury.
        logger.info("NACHOS", "No ICP on nachos subaccount (" # Nat.toText(balance) # "), cleaning up stale pending.", ctx);
        Map.delete(nachosPendingSwaps, phash, userPrincipal);
        updateNachosProgress(userPrincipal, #NotStarted, { icpAmount = null; estimatedNachos = null; actualNachos = null; errorMessage = null; orderId = null; mintId = null });
        return #NoDeposit;
      };
      case (#TransferredToTreasury(transferAmount)) {
        // ICP was sent to treasury. nachos_vault may have refunded back.
        // Check if ICP is back on our nachos subaccount
        let userNachosSub = nachosSubaccount(userPrincipal);
        let balance = try {
          await icpLedger.icrc1_balance_of({
            owner = this_canister_id();
            subaccount = ?userNachosSub;
          });
        } catch (_e) { 0 };

        if (balance > ICP_FEE + nachosMinDepositICP) {
          // ICP was refunded back — restart fresh
          Map.delete(nachosPendingSwaps, phash, userPrincipal);
          return await processNachosDeposit(userPrincipal, minimumNachosReceive, claimPath, fiatAmount, fiatCurrency);
        };

        // ICP not back yet. Try re-calling mintNachos if we have a block number.
        switch (pending.blockNumber) {
          case (?blockNum) {
            updateNachosProgress(userPrincipal, #MintingNachos, { icpAmount = ?pending.icpAmount; estimatedNachos = null; actualNachos = null; errorMessage = null; orderId = null; mintId = null });
            let nachosSubBlob = nachosSubaccountBlob(userPrincipal);
            let mintResult = try {
              await nachosVault.mintNachos(
                blockNum,
                minimumNachosReceive,
                ?nachosSubBlob,
                ?{ owner = userPrincipal; subaccount = null },
              );
            } catch (e) {
              storeNachosPendingSwap(userPrincipal, pending.icpAmount, #TransferredToTreasury(transferAmount), ?blockNum, "Retry mint call failed: " # Error.message(e));
              updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedNachos = null; actualNachos = null; errorMessage = ?("Retry mint failed: " # Error.message(e)); orderId = null; mintId = null });
              return #MintFailed("Retry mint call failed: " # Error.message(e));
            };

            switch (mintResult) {
              case (#ok(result)) {
                let orderId = nextNachosOrderId;
                nextNachosOrderId += 1;
                Vector.add(nachosCompletedOrders, {
                  id = orderId;
                  principal = userPrincipal;
                  icpDeposited = pending.icpAmount;
                  nachosReceived = result.nachosReceived;
                  navUsed = result.navUsed;
                  feeICP = result.feeValueICP;
                  timestamp = now();
                  nachosMintId = ?result.mintId;
                  claimPath;
                  fiatAmount;
                  fiatCurrency;
                });
                totalNachosMints += 1;
                totalNachosICPDeposited += pending.icpAmount;
                totalNachosDelivered += result.nachosReceived;
                Map.delete(nachosPendingSwaps, phash, userPrincipal);
                updateNachosProgress(userPrincipal, #Complete, { icpAmount = ?pending.icpAmount; estimatedNachos = null; actualNachos = ?result.nachosReceived; errorMessage = null; orderId = ?orderId; mintId = ?result.mintId });
                return #Success({ nachosAmount = result.nachosReceived; mintId = result.mintId; orderId });
              };
              case (#err(e)) {
                let errText = debug_show (e);
                // BlockAlreadyProcessed means the block was consumed — check if mint succeeded or refund is incoming
                storeNachosPendingSwap(userPrincipal, pending.icpAmount, #TransferredToTreasury(transferAmount), ?blockNum, "Retry mint error: " # errText);
                updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedNachos = null; actualNachos = null; errorMessage = ?("Retry mint error: " # errText); orderId = null; mintId = null });
                return #MintFailed("Retry mint error: " # errText);
              };
            };
          };
          case null {
            // No block number stored — can't retry. Clean up.
            logger.warn("NACHOS", "No blockNumber for retry. Cleaning up.", ctx);
            Map.delete(nachosPendingSwaps, phash, userPrincipal);
            updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedNachos = null; actualNachos = null; errorMessage = ?"Lost block number. Admin intervention required."; orderId = null; mintId = null });
            return #MintFailed("Lost block number. Admin intervention required.");
          };
        };
      };
      case (#MintRequested(_)) {
        // Mint was in progress — check if ICP was refunded back
        let userNachosSub = nachosSubaccount(userPrincipal);
        let balance = try {
          await icpLedger.icrc1_balance_of({
            owner = this_canister_id();
            subaccount = ?userNachosSub;
          });
        } catch (_e) { 0 };

        if (balance > ICP_FEE + nachosMinDepositICP) {
          Map.delete(nachosPendingSwaps, phash, userPrincipal);
          return await processNachosDeposit(userPrincipal, minimumNachosReceive, claimPath, fiatAmount, fiatCurrency);
        };

        // Nothing to do — wait for refund
        storeNachosPendingSwap(userPrincipal, pending.icpAmount, pending.stage, pending.blockNumber, "Waiting for refund from nachos_vault");
        updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?pending.icpAmount; estimatedNachos = null; actualNachos = null; errorMessage = ?"Waiting for refund from nachos_vault"; orderId = null; mintId = null });
        return #MintFailed("Mint in progress or awaiting refund. Will retry automatically.");
      };
    };
  };

  // --- Core NACHOS Processing ---

  private func processNachosDeposit(userPrincipal : Principal, minimumNachosReceive : Nat, claimPath : ClaimPath, fiatAmount : ?Text, fiatCurrency : ?Text) : async NachosClaimResult {
    let ctx = "processNachosDeposit";

    if (systemPaused or not nachosMintingEnabled) return #SystemPaused;
    if (userPrincipal == Principal.fromText("2vxsx-fae")) return #NotAuthorized;

    // Check for existing pending swap (retry path)
    switch (Map.get(nachosPendingSwaps, phash, userPrincipal)) {
      case (?pending) {
        if (pending.attempts >= maxRetries) {
          return #MintFailed("Max retries exceeded. Admin intervention required.");
        };
        return await retryNachosPendingSwap(userPrincipal, pending, minimumNachosReceive, claimPath, fiatAmount, fiatCurrency);
      };
      case null {};
    };

    // Acquire lock
    if (not acquireLock(userPrincipal)) return #AlreadyProcessing;

    // Step 1: Check ICP balance on NACHOS subaccount
    let userNachosSub = nachosSubaccount(userPrincipal);
    let balance = try {
      await icpLedger.icrc1_balance_of({
        owner = this_canister_id();
        subaccount = ?userNachosSub;
      });
    } catch (e) {
      releaseLock(userPrincipal);
      return #MintFailed("Balance check failed: " # Error.message(e));
    };

    if (balance == 0) { releaseLock(userPrincipal); return #NoDeposit };
    if (balance <= ICP_FEE + nachosMinDepositICP) {
      releaseLock(userPrincipal);
      return #BelowMinimum({ balance; minimum = ICP_FEE + nachosMinDepositICP });
    };

    logger.info("NACHOS", "Processing deposit of " # Nat.toText(balance) # " e8s for " # Principal.toText(userPrincipal), ctx);
    updateNachosProgress(userPrincipal, #DepositReceived, { icpAmount = ?balance; estimatedNachos = null; actualNachos = null; errorMessage = null; orderId = null; mintId = null });

    // Step 2: Transfer ICP to treasury[subaccount 2]
    updateNachosProgress(userPrincipal, #TransferringToTreasury, { icpAmount = ?balance; estimatedNachos = null; actualNachos = null; errorMessage = null; orderId = null; mintId = null });

    let transferAmount = balance - ICP_FEE;
    let transferResult = try {
      await icpLedger.icrc1_transfer({
        from_subaccount = ?userNachosSub;
        to = { owner = TREASURY_ID; subaccount = ?nachosTreasurySubaccount() };
        amount = transferAmount;
        fee = ?ICP_FEE;
        memo = null;
        created_at_time = null;
      });
    } catch (e) {
      storeNachosPendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), null, "Transfer failed: " # Error.message(e));
      updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedNachos = null; actualNachos = null; errorMessage = ?("Transfer failed: " # Error.message(e)); orderId = null; mintId = null });
      releaseLock(userPrincipal);
      return #MintFailed("ICP transfer to treasury failed: " # Error.message(e));
    };

    let blockNumber = switch (transferResult) {
      case (#Ok(blockIdx)) { blockIdx };
      case (#Err(e)) {
        storeNachosPendingSwap(userPrincipal, balance, #AwaitingDeposit(balance), null, "Transfer error: " # debug_show (e));
        updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedNachos = null; actualNachos = null; errorMessage = ?("Transfer error: " # debug_show (e)); orderId = null; mintId = null });
        releaseLock(userPrincipal);
        return #MintFailed("ICP transfer error: " # debug_show (e));
      };
    };

    // Step 3: Call nachos_vault.mintNachos
    updateNachosProgress(userPrincipal, #MintingNachos, { icpAmount = ?balance; estimatedNachos = null; actualNachos = null; errorMessage = null; orderId = null; mintId = null });

    let nachosSubBlob = nachosSubaccountBlob(userPrincipal);
    let mintResult = try {
      await nachosVault.mintNachos(
        blockNumber,
        minimumNachosReceive,
        ?nachosSubBlob,
        ?{ owner = userPrincipal; subaccount = null },
      );
    } catch (e) {
      storeNachosPendingSwap(userPrincipal, balance, #TransferredToTreasury(transferAmount), ?blockNumber, "Mint call failed: " # Error.message(e));
      updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedNachos = null; actualNachos = null; errorMessage = ?("Mint call failed: " # Error.message(e)); orderId = null; mintId = null });
      releaseLock(userPrincipal);
      return #MintFailed("NACHOS mint call failed: " # Error.message(e));
    };

    switch (mintResult) {
      case (#ok(result)) {
        let orderId = nextNachosOrderId;
        nextNachosOrderId += 1;
        Vector.add(nachosCompletedOrders, {
          id = orderId;
          principal = userPrincipal;
          icpDeposited = balance;
          nachosReceived = result.nachosReceived;
          navUsed = result.navUsed;
          feeICP = result.feeValueICP;
          timestamp = now();
          nachosMintId = ?result.mintId;
          claimPath;
          fiatAmount;
          fiatCurrency;
        });
        totalNachosMints += 1;
        totalNachosICPDeposited += balance;
        totalNachosDelivered += result.nachosReceived;
        Map.delete(nachosPendingSwaps, phash, userPrincipal);

        updateNachosProgress(userPrincipal, #Complete, { icpAmount = ?balance; estimatedNachos = null; actualNachos = ?result.nachosReceived; errorMessage = null; orderId = ?orderId; mintId = ?result.mintId });
        releaseLock(userPrincipal);
        logger.info("NACHOS", "Order #" # Nat.toText(orderId) # " complete: " # Nat.toText(result.nachosReceived) # " NACHOS sent to " # Principal.toText(userPrincipal), ctx);
        #Success({ nachosAmount = result.nachosReceived; mintId = result.mintId; orderId });
      };
      case (#err(e)) {
        let errText = debug_show (e);
        storeNachosPendingSwap(userPrincipal, balance, #TransferredToTreasury(transferAmount), ?blockNumber, "Mint error: " # errText);
        updateNachosProgress(userPrincipal, #Failed, { icpAmount = ?balance; estimatedNachos = null; actualNachos = null; errorMessage = ?("Mint error: " # errText); orderId = null; mintId = null });
        releaseLock(userPrincipal);
        #MintFailed("NACHOS mint error: " # errText);
      };
    };
  };

  // --- NACHOS Query / Status ---

  public query ({ caller }) func get_nachos_swap_status() : async NachosSwapProgress {
    switch (Map.get(nachosSwapProgress, phash, caller)) {
      case (?progress) progress;
      case null {
        switch (Map.get(nachosPendingSwaps, phash, caller)) {
          case (?pending) {
            let desc = switch (pending.stage) {
              case (#AwaitingDeposit(_)) "NACHOS mint failed at deposit stage. Will retry automatically.";
              case (#TransferredToTreasury(_)) "ICP transferred to treasury. Waiting to mint NACHOS.";
              case (#MintRequested(_)) "NACHOS mint in progress.";
            };
            {
              step = #Failed;
              stepNumber = 0 : Nat;
              totalSteps = 4 : Nat;
              description = desc;
              icpAmount = ?pending.icpAmount;
              estimatedNachos = null;
              actualNachos = null;
              startedAt = pending.createdAt;
              updatedAt = pending.lastAttempt;
              errorMessage = pending.errorMessage;
              retryCount = pending.attempts;
              orderId = null;
              mintId = null;
            };
          };
          case null {
            {
              step = #NotStarted;
              stepNumber = 0 : Nat;
              totalSteps = 4 : Nat;
              description = "No NACHOS mint in progress.";
              icpAmount = null;
              estimatedNachos = null;
              actualNachos = null;
              startedAt = 0;
              updatedAt = 0;
              errorMessage = null;
              retryCount = 0 : Nat;
              orderId = null;
              mintId = null;
            };
          };
        };
      };
    };
  };

  public query ({ caller }) func get_my_nachos_orders(limit : Nat) : async [NachosOrderRecord] {
    let result = Vector.new<NachosOrderRecord>();
    var count : Nat = 0;
    let size = Vector.size(nachosCompletedOrders);
    var i = size;
    label iter while (i > 0 and count < limit) {
      i -= 1;
      let order = Vector.get(nachosCompletedOrders, i);
      if (order.principal == caller) {
        Vector.add(result, order);
        count += 1;
      };
    };
    Vector.toArray(result);
  };

  public query func get_nachos_order_history(limit : Nat, offset : Nat) : async [NachosOrderRecord] {
    let size = Vector.size(nachosCompletedOrders);
    if (offset >= size) return [];
    let result = Vector.new<NachosOrderRecord>();
    var i = size;
    var count : Nat = 0;
    var skipped : Nat = 0;
    label iter while (i > 0) {
      i -= 1;
      if (skipped < offset) {
        skipped += 1;
      } else if (count < limit) {
        Vector.add(result, Vector.get(nachosCompletedOrders, i));
        count += 1;
      } else {
        break iter;
      };
    };
    Vector.toArray(result);
  };

  public query func get_nachos_stats() : async {
    totalNachosMints : Nat;
    totalNachosICPDeposited : Nat;
    totalNachosDelivered : Nat;
    nachosPendingCount : Nat;
    nachosOrdersCount : Nat;
    nachosMintingEnabled : Bool;
  } {
    {
      totalNachosMints;
      totalNachosICPDeposited;
      totalNachosDelivered;
      nachosPendingCount = Map.size(nachosPendingSwaps);
      nachosOrdersCount = Vector.size(nachosCompletedOrders);
      nachosMintingEnabled;
    };
  };

  // --- NACHOS Admin Functions ---

  public shared ({ caller }) func set_nachos_minting_enabled(enabled : Bool) : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    nachosMintingEnabled := enabled;
    logAdminAction(caller, #Pause, "NACHOS minting " # (if enabled "enabled" else "disabled"), true);
    logger.info("ADMIN", "NACHOS minting " # (if enabled "enabled" else "disabled") # " by " # Principal.toText(caller), "set_nachos_minting_enabled");
    #ok();
  };

  public shared ({ caller }) func refund_nachos_pending(principal : Principal) : async Result.Result<Nat, Text> {
    if (not isAdmin(caller)) return #err("Not authorized");

    // Check NACHOS subaccount balance
    let userNachosSub = nachosSubaccount(principal);
    let balance = try {
      await icpLedger.icrc1_balance_of({
        owner = this_canister_id();
        subaccount = ?userNachosSub;
      });
    } catch (e) {
      return #err("Balance check failed: " # Error.message(e));
    };

    if (balance == 0) return #err("No ICP on NACHOS subaccount");
    if (balance <= ICP_FEE) return #err("Balance too small to refund (" # Nat.toText(balance) # " e8s)");

    let refundAmount = balance - ICP_FEE;
    let transferResult = try {
      await icpLedger.icrc1_transfer({
        from_subaccount = ?userNachosSub;
        to = { owner = principal; subaccount = null };
        amount = refundAmount;
        fee = ?ICP_FEE;
        memo = null;
        created_at_time = null;
      });
    } catch (e) {
      return #err("Refund transfer failed: " # Error.message(e));
    };

    switch (transferResult) {
      case (#Ok(_txId)) {
        Map.delete(nachosPendingSwaps, phash, principal);
        logAdminAction(caller, #RecoverFunds, "Refunded " # Nat.toText(refundAmount) # " e8s ICP from NACHOS subaccount to " # Principal.toText(principal), true);
        logger.info("ADMIN", "NACHOS refund: " # Nat.toText(refundAmount) # " e8s to " # Principal.toText(principal), "refund_nachos_pending");
        #ok(refundAmount);
      };
      case (#Err(e)) {
        #err("Refund transfer error: " # debug_show (e));
      };
    };
  };

  public query ({ caller }) func get_nachos_pending_swaps() : async [(Principal, NachosPendingSwap)] {
    if (not isAdmin(caller)) return [];
    Iter.toArray(Map.entries(nachosPendingSwaps));
  };

  public query ({ caller }) func get_all_nachos_swap_progress() : async [(Principal, NachosSwapProgress)] {
    if (not isAdmin(caller)) return [];
    Iter.toArray(Map.entries(nachosSwapProgress));
  };

  public shared ({ caller }) func admin_clear_nachos_pending(principal : Principal) : async Result.Result<(), Text> {
    if (not isAdmin(caller)) return #err("Not authorized");
    switch (Map.get(nachosPendingSwaps, phash, principal)) {
      case (?pending) {
        Map.delete(nachosPendingSwaps, phash, principal);
        logAdminAction(caller, #RecoverFunds, "Cleared NACHOS pending for " # Principal.toText(principal) # " (icpAmount=" # Nat.toText(pending.icpAmount) # ", stage=" # debug_show (pending.stage) # ")", true);
        #ok();
      };
      case null { #err("No NACHOS pending swap for " # Principal.toText(principal)) };
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 13: TIMER SYSTEM
  // ═══════════════════════════════════════════════════════════════════

  // Sweep timer: retry pending swaps periodically
  private func startSweepTimer<system>() {
    switch (sweepTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };

    sweepTimerId := ?setTimer<system>(
      #nanoseconds(Int.abs(sweepIntervalNS)),
      func() : async () {
        await sweepPendingSwaps();
        startSweepTimer<system>();
      },
    );
  };

  private func sweepPendingSwaps() : async () {
    let ctx = "sweep";

    // Clean up stale progress entries
    cleanupStaleProgress();
    cleanupStaleNachosProgress();

    // Sweep TACO pending swaps
    let entries = Iter.toArray(Map.entries(pendingSwaps));
    if (entries.size() > 0) {
      logger.info("TIMER", "Sweeping " # Nat.toText(entries.size()) # " pending TACO swaps", ctx);
      for ((principal, pending) in entries.vals()) {
        if (pending.attempts < maxRetries) {
          let _result = await processDeposit(principal, #TimerSweep, null, null);
        };
      };
    };

    // Sweep NACHOS pending swaps
    let nachosEntries = Iter.toArray(Map.entries(nachosPendingSwaps));
    if (nachosEntries.size() > 0) {
      logger.info("TIMER", "Sweeping " # Nat.toText(nachosEntries.size()) # " pending NACHOS swaps", ctx);
      for ((principal, pending) in nachosEntries.vals()) {
        if (pending.attempts < maxRetries) {
          let _result = await processNachosDeposit(principal, 0, #TimerSweep, null, null);
        };
      };
    };
  };

  // Clean up old completed/failed progress entries to prevent unbounded growth
  private func cleanupStaleProgress() {
    let COMPLETED_TTL : Int = 10 * 60 * 1_000_000_000; // 10 min
    let FAILED_TTL : Int = 30 * 60 * 1_000_000_000; // 30 min
    let STALE_TTL : Int = 60 * 60 * 1_000_000_000; // 1 hour
    let cutoffCompleted = now() - COMPLETED_TTL;
    let cutoffFailed = now() - FAILED_TTL;
    let cutoffStale = now() - STALE_TTL;

    let entries = Iter.toArray(Map.entries(swapProgress));
    for ((principal, progress) in entries.vals()) {
      switch (progress.step) {
        case (#Complete or #NotStarted) {
          if (progress.updatedAt < cutoffCompleted) {
            Map.delete(swapProgress, phash, principal);
          };
        };
        case (#Failed) {
          if (progress.updatedAt < cutoffFailed) {
            Map.delete(swapProgress, phash, principal);
          };
        };
        case _ {
          if (progress.updatedAt < cutoffStale) {
            Map.delete(swapProgress, phash, principal);
          };
        };
      };
    };
  };

  private func cleanupStaleNachosProgress() {
    let COMPLETED_TTL : Int = 10 * 60 * 1_000_000_000;
    let FAILED_TTL : Int = 30 * 60 * 1_000_000_000;
    let STALE_TTL : Int = 60 * 60 * 1_000_000_000;
    let cutoffCompleted = now() - COMPLETED_TTL;
    let cutoffFailed = now() - FAILED_TTL;
    let cutoffStale = now() - STALE_TTL;

    let entries = Iter.toArray(Map.entries(nachosSwapProgress));
    for ((principal, progress) in entries.vals()) {
      switch (progress.step) {
        case (#Complete or #NotStarted) {
          if (progress.updatedAt < cutoffCompleted) {
            Map.delete(nachosSwapProgress, phash, principal);
          };
        };
        case (#Failed) {
          if (progress.updatedAt < cutoffFailed) {
            Map.delete(nachosSwapProgress, phash, principal);
          };
        };
        case _ {
          if (progress.updatedAt < cutoffStale) {
            Map.delete(nachosSwapProgress, phash, principal);
          };
        };
      };
    };
  };

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 14: LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  system func postupgrade() {
    startSweepTimer<system>();
    logger.info("LIFECYCLE", "Post-upgrade complete. Sweep timer started.", "postupgrade");
  };

  // Init: discover pool and fetch token metadata
  private func initAsync() : async () {
    let ctx = "init";

    // Fetch TACO ledger fee and decimals
    try {
      tacoLedgerFee := await tacoLedgerActor.icrc1_fee();
      tacoDecimals := await tacoLedgerActor.icrc1_decimals();
      logger.info("INIT", "TACO fee=" # Nat.toText(tacoLedgerFee) # " decimals=" # Nat8.toText(tacoDecimals), ctx);
    } catch (e) {
      logger.warn("INIT", "Failed to fetch TACO metadata: " # Error.message(e), ctx);
    };

    // Auto-discover pool if not configured
    if (icpTacoPoolId == null) {
      let _result = await discoverPool();
    };
  };

  // Start init and sweep timer on first deploy
  ignore setTimer<system>(
    #nanoseconds(0),
    func() : async () {
      await initAsync();
      startSweepTimer<system>();
    },
  );

  // ═══════════════════════════════════════════════════════════════════
  // SECTION 15: UTILITY / CYCLES
  // ═══════════════════════════════════════════════════════════════════

  public query func get_canister_cycles() : async Nat {
    Prim.cyclesBalance();
  };
};
