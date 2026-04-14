#!/bin/bash
# Exchange (OTC) Local Test Script
# Deploys ledger canisters from sibling directories, then deploys exchange canisters and runs tests.
#
# Usage:
#   ./test_exchange_local.sh              # Run all tests including stress tests
#   ./test_exchange_local.sh skip_stress  # Skip stress tests
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
dfx start --background --clean --artificial-delay 1
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
mkdir -p ./.dfx/local/canisters/ledger_canister/
if [ -f ledger-canister.wasm.gz ]; then
  cp ledger-canister.wasm.gz ./.dfx/local/canisters/ledger_canister/
else
  echo "ERROR: ledger-canister.wasm.gz not found in $PARENT_DIR/ledger_canister/"
  echo "Please download it first."
  exit 1
fi

dfx identity use minterTACO
export MINTER_ACCOUNT_ID=$(dfx ledger account-id)
dfx identity use defaultTACO
export DEFAULT_ACCOUNT_ID=$(dfx ledger account-id)

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

# Ensure wasm is in dfx cache
mkdir -p ./.dfx/local/canisters/icrc1_ledger_canister/
if [ -f ic-icrc1-ledger.wasm.gz ]; then
  cp ic-icrc1-ledger.wasm.gz ./.dfx/local/canisters/icrc1_ledger_canister/
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

dfx canister create --specified-id "$EXCHANGE_TEST_ID" exchange_test
yes | dfx deploy --specified-id "$EXCHANGE_TEST_ID" exchange_test --with-cycles 10000000000000000 --mode=reinstall

# Add test canister as fee collector so collectFees() works from tests
echo "Adding test canister as fee collector..."
dfx canister call OTC_backend addFeeCollector "(principal \"$EXCHANGE_TEST_ID\")"

# === Step 8: Run tests ===
echo "--- Step 8: Running tests ---"
echo ""

if [ "$1" = "skip_stress" ]; then
    echo "Running tests (skipping stress tests)..."
    dfx canister call exchange_test runTests '(false, true)'
else
    echo "Running all tests (including stress tests)..."
    dfx canister call exchange_test runTests '(false, false)'
fi

echo ""
echo "=== Exchange test run complete ==="
