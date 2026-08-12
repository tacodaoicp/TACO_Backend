#!/bin/bash
# Exchange (OTC) Local Test Script
# Deploys ledger canisters from sibling directories, then deploys exchange canisters and runs tests.
#
# Usage:
#   ./test_exchange_local.sh              # Run all tests including stress tests
#   ./test_exchange_local.sh skip_stress  # Skip stress tests
#   ./test_exchange_local.sh --v2         # V1 (0-77) + V2 (100-121) + MIXED (150-157), no stress
#   ./test_exchange_local.sh --v2-only    # V2 (100-121) only, standalone seed
#   ./test_exchange_local.sh --mixed      # MIXED (150-157) only, standalone seed
#   ./test_exchange_local.sh --residual   # RESIDUAL (210-214) only, standalone seed (no Test46)
#   ./test_exchange_local.sh --caps       # CAPS/KILL-SWITCH (300-308) only, standalone seed
#   ./test_exchange_local.sh --fixab      # FIXAB (220-256) only, standalone seed (FIX A/B + movers + parity + recovery)
#
# V2 modes poll getTestResults for the verdict — the runTestsV2 ingress call
# times out client-side long before the canister finishes; the exit code of
# `dfx canister call` is meaningless there.
#
# Prerequisites:
#   - dfx 0.30.1+
#   - Sibling directories ../ledger_canister/ and ../icrc1_ledger_canister/ must exist
#   - Identities: defaultTACO, minterTACO, archive_controllerTACO

set -e

# === Canister IDs (must match hardcoded values in test files) ===
OTC_BACKEND_ID="qioex-5iaaa-aaaan-q52ba-cai"
EXCHANGE_TREASURY_ID="qbnpl-laaaa-aaaan-q52aq-cai"
TEST_ACTOR_A_ID="hhaaz-2aaaa-aaaaq-aacla-cai"
TEST_ACTOR_B_ID="qtooy-2yaaa-aaaaq-aabvq-cai"
TEST_ACTOR_C_ID="aanaa-xaaaa-aaaah-aaeiq-cai"
EXCHANGE_TEST_ID="pcj6u-uaaaa-aaaak-aewnq-cai"

# Token canister IDs
ICP_LEDGER_ID="ryjl3-tyaaa-aaaaa-aaaba-cai"
TOKEN1_ID="mxzaz-hqaaa-aaaar-qaada-cai"
TOKEN2_ID="zxeu2-7aaaa-aaaaq-aaafa-cai"
CKUSDC_ID="xevnm-gaaaa-aaaar-qafnq-cai"
TACO_TOKEN_ID="csyra-haaaa-aaaaq-aacva-cai"

# DAO references (test values)
DAO_ID="hjcnr-bqaaa-aaaaq-aacka-cai"
DAO_TREASURY_ID="ar2zl-5qaaa-aaaan-qavoa-cai"

# Account IDs for test actors (precomputed from their principal IDs)
TEST_ACTOR_A_ACC="711a345146b5013508e630b4fca3645293f20de369b46d20219622cb84628c7e"
TEST_ACTOR_B_ACC="087bd635fda622aa0d7f2d66172a95a42b93eafe7dfd00de80429e9886c6189e"
TEST_ACTOR_C_ACC="758bdb7e54b73605d1d743da9f3aad70637d4cddcba03db13137eaf35f12d375"
DAO_TREASURY_ACC="366bf011a8ff33b5e2acc655803e3391b6057df6c9cc5a6952cda58effd4b40b"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Exchange Local Test Script ==="
echo "Working directory: $SCRIPT_DIR"
echo ""

# === Step 1: Ensure dfx is running ===
echo "--- Step 1: Starting dfx ---"
dfx stop 2>/dev/null || true
sleep 1
dfx start --background --clean --artificial-delay 10
sleep 2

# === Step 2: Set up identities ===
echo "--- Step 2: Setting up identities ---"
dfx identity new defaultTACO --storage-mode=plaintext 2>/dev/null || true
dfx identity new minterTACO --storage-mode=plaintext 2>/dev/null || true
dfx identity new archive_controllerTACO --storage-mode=plaintext 2>/dev/null || true
dfx identity use defaultTACO

# Get identity info
dfx identity use minterTACO
export MINTER=$(dfx identity get-principal)
export MINTER_ACCOUNT_ID=$(dfx ledger account-id 2>/dev/null || echo "")
dfx identity use archive_controllerTACO
export ARCHIVE_CONTROLLER=$(dfx identity get-principal)
dfx identity use defaultTACO
export DEFAULT=$(dfx identity get-principal)
export DEFAULT_ACCOUNT_ID=$(dfx ledger account-id 2>/dev/null || echo "")

# === Step 3: Deploy ICP Ledger ===
echo "--- Step 3: Deploying ICP Ledger ---"
cd "$PARENT_DIR/ledger_canister"

# Ensure wasm is in dfx cache
# Ledger release: ledger-suite-icp-2025-08-29 (full ICRC-2 incl. icrc2_transfer_from).
# NOTE: dfx re-downloads the wasm from the URL pinned in ledger_canister/dfx.json on
# every deploy; the local copy below is only an offline fallback / provenance record.
mkdir -p ./.dfx/local/canisters/ledger_canister/
if [ -f ledger-canister_notify-method.wasm.gz ]; then
  cp ledger-canister_notify-method.wasm.gz ./.dfx/local/canisters/ledger_canister/download-ledger-canister_notify-method.wasm.gz
else
  echo "ERROR: ledger-canister_notify-method.wasm.gz not found in $PARENT_DIR/ledger_canister/"
  echo "Download it from https://github.com/dfinity/ic/releases/tag/ledger-suite-icp-2025-08-29 first."
  exit 1
fi

dfx identity use minterTACO
export MINTER_ACCOUNT_ID=$(dfx ledger account-id)
dfx identity use defaultTACO
export DEFAULT_ACCOUNT_ID=$(dfx ledger account-id)

# No feature_flags in Init: verified empirically (2026-08-07) that this build
# (ledger-suite-icp-2025-08-29) enables ICRC-2 by default — icrc2_approve,
# icrc2_allowance and icrc2_transfer_from all work with exactly this init.
yes | dfx deploy --specified-id "$ICP_LEDGER_ID" ledger_canister --argument "
  (variant {
    Init = record {
      minting_account = \"$MINTER_ACCOUNT_ID\";
      initial_values = vec {
        record {
          \"$DEFAULT_ACCOUNT_ID\";
          record {
            e8s = 10_000_000_000_000_000 : nat64;
          };
        };
      };
      send_whitelist = vec {};
      transfer_fee = opt record {
        e8s = 10_000 : nat64;
      };
      token_symbol = opt \"LICP\";
      token_name = opt \"Local ICP\";
    }
  })
" --mode=reinstall --with-cycles 1_000_000_000_000_000_000

# Fund test actors with ICP
echo "Funding test actors with ICP..."
dfx ledger transfer --memo "433" --e8s 50000000000000 "$TEST_ACTOR_A_ACC" &
dfx ledger transfer --memo "433" --e8s 50000000000000 "$TEST_ACTOR_B_ACC" &
dfx ledger transfer --memo "433" --e8s 50000000000000 "$TEST_ACTOR_C_ACC" &
dfx ledger transfer --memo "433" --e8s 3000000000 "$DAO_TREASURY_ACC" &
wait

# === Step 4: Deploy ICRC1 Tokens ===
echo "--- Step 4: Deploying ICRC1 tokens ---"
cd "$PARENT_DIR/icrc1_ledger_canister"

# Ensure wasm is in dfx cache (same caveat as the ICP ledger: dfx re-downloads
# from the URL pinned in icrc1_ledger_canister/dfx.json on every deploy).
# No feature_flags in the Inits below: verified empirically (2026-08-07) that this
# build (ic commit 5849c6d, 2024-07-04) enables ICRC-2 by default — icrc2_approve,
# icrc2_allowance and icrc2_transfer_from all work with exactly these inits.
mkdir -p ./.dfx/local/canisters/icrc1_ledger_canister/
if [ -f ic-icrc1-ledger.wasm.gz ]; then
  cp ic-icrc1-ledger.wasm.gz ./.dfx/local/canisters/icrc1_ledger_canister/download-ic-icrc1-ledger.wasm.gz
else
  echo "ERROR: ic-icrc1-ledger.wasm.gz not found in $PARENT_DIR/icrc1_ledger_canister/"
  echo "Please download it first."
  exit 1
fi

export PRE_MINTED_TOKENS=10_000_000_000_000_000_000
export TACO_PMT=777_777_700_000_000
export TRANSFER_FEE=10_000
export TRIGGER_THRESHOLD=2000
export NUM_OF_BLOCK_TO_ARCHIVE=1000
export CYCLE_FOR_ARCHIVE_CREATION=10000000000000

# Token 1 (XMTK)
echo "Deploying Token 1..."
yes | dfx deploy icrc1_ledger_canister --specified-id "$TOKEN1_ID" --argument "(variant {Init =
record {
     token_symbol = \"XMTK\";
     token_name = \"My Token\";
     minting_account = record { owner = principal \"${MINTER}\" };
     transfer_fee = ${TRANSFER_FEE};
     metadata = vec {};
     initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${PRE_MINTED_TOKENS}; }; };
     archive_options = record {
         num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE};
         trigger_threshold = ${TRIGGER_THRESHOLD};
         controller_id = principal \"${ARCHIVE_CONTROLLER}\";
         cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION};
     };
 }
})" --mode=reinstall --with-cycles 1_000_000_000_000_000_000

# Token 2 (XMTK2)
echo "Deploying Token 2..."
yes | dfx deploy icrc1_ledger_canister2 --specified-id "$TOKEN2_ID" --argument "(variant {Init = record { token_symbol = \"XMTK2\"; token_name = \"My Token2\"; minting_account = record { owner = principal \"${MINTER}\" }; transfer_fee = ${TRANSFER_FEE}; metadata = vec {}; initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${PRE_MINTED_TOKENS}; }; }; archive_options = record { num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE}; trigger_threshold = ${TRIGGER_THRESHOLD}; controller_id = principal \"${ARCHIVE_CONTROLLER}\"; cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION}; }; }})" --mode=reinstall --with-cycles 1_000_000_000_000_000_000

# ckUSDC
echo "Deploying ckUSDC..."
yes | dfx deploy ckusdc --specified-id "$CKUSDC_ID" --argument "(variant {Init = record { token_symbol = \"USDC\"; token_name = \"USDC\"; minting_account = record { owner = principal \"${MINTER}\" }; transfer_fee = ${TRANSFER_FEE}; metadata = vec {}; initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${PRE_MINTED_TOKENS}; }; }; archive_options = record { num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE}; trigger_threshold = ${TRIGGER_THRESHOLD}; controller_id = principal \"${ARCHIVE_CONTROLLER}\"; cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION}; }; }})" --mode=reinstall --with-cycles 1_000_000_000_000_000_000

# TACO Token
echo "Deploying TACO token..."
yes | dfx deploy tacoToken --specified-id "$TACO_TOKEN_ID" --argument "(variant {Init =
record {
     token_symbol = \"TACO\";
     token_name = \"TACO\";
     minting_account = record { owner = principal \"${MINTER}\" };
     transfer_fee = 70000;
     metadata = vec {};
     initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${TACO_PMT}; }; };
     archive_options = record {
         num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE};
         trigger_threshold = ${TRIGGER_THRESHOLD};
         controller_id = principal \"${ARCHIVE_CONTROLLER}\";
         cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION};
     };
 }
})" --mode=reinstall --with-cycles 1_000_000_000_000_000_000

# Fund test actors with all ICRC1 tokens
echo "Funding test actors with ICRC1 tokens..."
dfx identity use defaultTACO
for TOKEN_CANISTER in icrc1_ledger_canister icrc1_ledger_canister2 ckusdc tacoToken; do
  dfx canister call "$TOKEN_CANISTER" icrc1_transfer "(record { to = record { owner = principal \"${TEST_ACTOR_A_ID}\";};  amount = 5_000_000_000_000;})" &
  dfx canister call "$TOKEN_CANISTER" icrc1_transfer "(record { to = record { owner = principal \"${TEST_ACTOR_B_ID}\";};  amount = 5_000_000_000_000;})" &
  dfx canister call "$TOKEN_CANISTER" icrc1_transfer "(record { to = record { owner = principal \"${TEST_ACTOR_C_ID}\";};  amount = 5_000_000_000_000;})" &
  dfx canister call "$TOKEN_CANISTER" icrc1_transfer "(record { to = record { owner = principal \"${DAO_TREASURY_ID}\";};  amount = 3_000_000_000;})" &
done
wait

# === Step 5: Deploy Exchange Canisters ===
echo "--- Step 5: Deploying exchange canisters ---"
cd "$SCRIPT_DIR"

# Source code already uses production IDs — no patching needed

# Deploy exchange treasury first
echo "Deploying exchange_treasury..."
dfx canister create --specified-id "$EXCHANGE_TREASURY_ID" exchange_treasury
yes | dfx deploy --specified-id "$EXCHANGE_TREASURY_ID" exchange_treasury --with-cycles 10000000000000000 --mode=reinstall

# Deploy OTC backend
echo "Deploying OTC_backend..."
dfx canister create --specified-id "$OTC_BACKEND_ID" OTC_backend
yes | dfx deploy --specified-id "$OTC_BACKEND_ID" OTC_backend --with-cycles 10000000000000000 --mode=reinstall

# === Step 6: Configure cross-references ===
echo "--- Step 6: Configuring cross-references ---"

# Set treasury on OTC backend (this also calls setOTCCanister on treasury via inter-canister call)
# Direct ingress to exchange_treasury is blocked by inspect returning false.
dfx canister call OTC_backend parameterManagement "(record {
  deleteFromDayBan = null;
  deleteFromAllTimeBan = null;
  addToAllTimeBan = null;
  changeAllowedCalls = null;
  changeallowedSilentWarnings = null;
  addAllowedCanisters = opt vec { \"$EXCHANGE_TEST_ID\"; \"$TEST_ACTOR_A_ID\"; \"$TEST_ACTOR_B_ID\"; \"$TEST_ACTOR_C_ID\"; \"$DAO_ID\" };
  deleteAllowedCanisters = null;
  treasury_principal = opt \"$EXCHANGE_TREASURY_ID\";
})"

# Enable test mode
dfx canister call OTC_backend setTest '(true)'

# === Step 7: Deploy test actors ===
echo "--- Step 7: Deploying test actors ---"

dfx canister create --specified-id "$TEST_ACTOR_A_ID" exchange_testActorA
yes | dfx deploy --specified-id "$TEST_ACTOR_A_ID" exchange_testActorA --with-cycles 10000000000000000 --mode=reinstall

dfx canister create --specified-id "$TEST_ACTOR_B_ID" exchange_testActorB
yes | dfx deploy --specified-id "$TEST_ACTOR_B_ID" exchange_testActorB --with-cycles 10000000000000000 --mode=reinstall

dfx canister create --specified-id "$TEST_ACTOR_C_ID" exchange_testActorC
yes | dfx deploy --specified-id "$TEST_ACTOR_C_ID" exchange_testActorC --with-cycles 10000000000000000 --mode=reinstall

# Misbehaving mock ICRC-1/2 ledgers for the V2 ambiguous-path / recovery tests
# (126-133). mock_ledger's icrc2_transfer_from can be driven into a
# debit-then-trap (the ambiguous case). Registered + allowlisted + funded from
# test.mo's seedMocks() at run time; here we only need the canisters to exist.
MOCK_A_ID="rh2pm-ryaaa-aaaan-qeniq-cai"
MOCK_B_ID="i2s4q-syaaa-aaaan-qz4sq-cai"
dfx canister create --specified-id "$MOCK_A_ID" exchange_mock_ledger
yes | dfx deploy --specified-id "$MOCK_A_ID" exchange_mock_ledger --with-cycles 10000000000000000 --mode=reinstall
dfx canister create --specified-id "$MOCK_B_ID" exchange_mock_ledger_b
yes | dfx deploy --specified-id "$MOCK_B_ID" exchange_mock_ledger_b --with-cycles 10000000000000000 --mode=reinstall

dfx canister create --specified-id "$EXCHANGE_TEST_ID" exchange_test
yes | dfx deploy --specified-id "$EXCHANGE_TEST_ID" exchange_test --with-cycles 10000000000000000 --mode=reinstall

# Add test canister as fee collector so collectFees() works from tests
echo "Adding test canister as fee collector..."
dfx canister call OTC_backend addFeeCollector "(principal \"$EXCHANGE_TEST_ID\")"

# === Step 8: Run tests ===
echo "--- Step 8: Running tests ---"
echo ""

# ── V2 modes ──
V2_MODE=""
case "$1" in
  --v2|v2)           V2_MODE=1; EXPECTED=133 ;;  # 78 V1 + 26 V2 + 8 MIXED + 12 BATCH/LOAD (170-181) + 9 AMBIG (126-134)
  --v2-only|v2_only) V2_MODE=2; EXPECTED=47  ;;  # tests 100-125 (26) + 170-181 (12) + 126-134 (9)
  --mixed|mixed)     V2_MODE=3; EXPECTED=8   ;;  # tests 150-157
  --ambig|ambig)     V2_MODE=6; EXPECTED=9   ;;  # tests 126-134 (V2 ambiguous-path / recovery)
  --overfill|overfill)     V2_MODE=7; EXPECTED=5   ;;  # tests 160-164 (single-fill over-fill clamp + V2 kill switch)
  --batchdedup|batchdedup) V2_MODE=8; EXPECTED=3   ;;  # tests 190-192 (batch duplicate-accesscode double-fill)
  --rcl|rcl)               V2_MODE=9; EXPECTED=2   ;;  # tests 200-201 (RCL-GUARD: removeConcentratedLiquidity full-range rejection + over-claim closure)
  --residual|residual)     V2_MODE=10; EXPECTED=5  ;;  # tests 210-214 (RESIDUAL: P15 sweep ordering, setMinimumAmount floor, minLegOut V1/V2, orderbook mid)
  --caps|caps)             V2_MODE=11; EXPECTED=9  ;;  # tests 300-308 (CAPS/KILL-SWITCH: global 200 + per-token 25 pending-pull caps, v2Enabled AND allowlist composition, refundPullV2 confiscation band + unreachability at live params, adminResolvePendingPull double-pay, mid-flight v2Enabled disable, trap-rolls-back-the-lock)
  --fixab|fixab)           V2_MODE=12; EXPECTED=37 ;;  # tests 220-256 (FIXAB: FinishSellBatchV2 guard-order regression + V1 trap lock, V2 quote allowlist gating x6, 8 fund-movers e2e, 6 quote parities, 4 helpers + disabled shapes, 9+ recovery/admin ops incl. adminResolvePendingPull via #debitThenTrap)
esac

if [ -n "$V2_MODE" ]; then
    echo "Running V2 tests (mode $V2_MODE, expecting $EXPECTED results)..."
    # Client ingress timeout mid-run is normal — the canister keeps executing.
    dfx canister call exchange_test runTestsV2 "($V2_MODE : nat, true)" || true

    echo "Polling getTestResults until the run completes..."
    LOG="$(mktemp /tmp/v2_results.XXXXXX)"
    PREV_COUNT=-1
    STALL=0
    for i in $(seq 1 360); do
        sleep 15
        if ! dfx canister call --query --output idl exchange_test getTestResults '()' > "$LOG" 2>/dev/null; then
            echo "  poll $i: query failed, retrying"
            continue
        fi
        COUNT=$(grep -o 'Test[0-9]*: ' "$LOG" | sort -u | wc -l)
        echo "  poll $i: $COUNT distinct test results"
        if [ "$COUNT" -ge "$EXPECTED" ]; then break; fi
        if [ "$COUNT" -eq "$PREV_COUNT" ]; then STALL=$((STALL+1)); else STALL=0; fi
        PREV_COUNT=$COUNT
        if [ "$STALL" -ge 40 ]; then
            echo "WARNING: results stopped growing at $COUNT/$EXPECTED (10 min stall)"
            break
        fi
    done

    echo ""
    echo "=== V2 Test Results (getTestResults) ==="
    cat "$LOG"
    echo ""
    # Failures print 'TestNN: Failed : <reason>' — match the colon form.
    if grep -q ': Failed' "$LOG"; then
        echo "!!! FAILURES DETECTED !!!"
        grep -o 'Test[0-9]*: Failed[^"]*' "$LOG"
        exit 1
    fi
    PASSCOUNT=$(grep -o 'Test[0-9]*: Success' "$LOG" | wc -l)
    echo "All reported tests passed ($PASSCOUNT Success)."
    echo "=== Exchange V2 test run complete ==="
    exit 0
fi

if [ "$1" = "skip_stress" ]; then
    echo "Running tests (skipping stress tests)..."
    dfx canister call exchange_test runTests '(false, true)'
elif [ "$1" = "stress_only" ]; then
    echo "Running stress tests only (minimal setup)..."
    dfx canister call exchange_test runOnlyStressTests
elif [ "$1" = "rebuild_stress" ]; then
    echo "Rebuilding exchange + test canisters and running stress only..."
    dfx build OTC_backend 2>&1 | tail -3
    dfx build exchange_test 2>&1 | tail -3
    dfx canister install OTC_backend --mode upgrade --wasm-memory-persistence keep 2>&1
    dfx canister install exchange_test --mode upgrade --wasm-memory-persistence keep 2>&1
    echo "Running stress tests..."
    dfx canister call exchange_test runOnlyStressTests
else
    echo "Running all tests (including stress tests)..."
    dfx canister call exchange_test runTests '(false, false)'
fi

echo ""
echo "=== Exchange test run complete ==="
