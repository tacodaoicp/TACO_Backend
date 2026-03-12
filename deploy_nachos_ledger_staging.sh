#!/bin/bash
set -euo pipefail

# ════════════════════════════════════════════════════════════════
# Deploy NACHOS Token Ledger to Staging
#
# Uses icdevsorg/ICRC_fungible (cloned in nachos_ledger/)
# The nachos_vault canister is the minting account.
#
# Prerequisites:
#   - nachos_vault deployed to staging (p4nog-baaaa-aaaad-qkwpa-cai)
#   - nachos_ledger/ directory exists (git clone icdevsorg/ICRC_fungible)
#   - mops install completed in nachos_ledger/
#   - dfx identity with cycles and controller access
# ════════════════════════════════════════════════════════════════

NETWORK="${1:---network staging}"
NACHOS_VAULT_ID="p4nog-baaaa-aaaad-qkwpa-cai"
ADMIN_PRINCIPAL=$(dfx identity get-principal)

# Use mops-managed moc 1.1.0 (dfx 0.30.2 bundles moc 0.16.3 which is too old)
export DFX_MOC_PATH="$(cd nachos_ledger && mops toolchain bin moc)"

echo "═══ NACHOS Ledger Deployment ═══"
echo "Network:    $NETWORK"
echo "Vault ID:   $NACHOS_VAULT_ID"
echo "Admin:      $ADMIN_PRINCIPAL"
echo ""

# ── Step 1: Deploy the token ──
echo "► Step 1: Deploying NACHOS token ledger..."

cd nachos_ledger

dfx deploy token $NETWORK --argument "(opt record {
  icrc1 = opt record {
    name = opt \"NACHOS\";
    symbol = opt \"NACHOS\";
    logo = null;
    decimals = 8;
    fee = opt variant { Fixed = 10000 };
    minting_account = opt record {
      owner = principal \"$NACHOS_VAULT_ID\";
      subaccount = null;
    };
    max_supply = null;
    min_burn_amount = opt 10000;
    max_memo = opt 64;
    advanced_settings = null;
    metadata = null;
    fee_collector = null;
    transaction_window = null;
    permitted_drift = null;
    max_accounts = opt 100000000;
    settle_to_accounts = opt 99999000;
  };
  icrc2 = opt record {
    max_approvals_per_account = opt 10000;
    max_allowance = opt variant { TotalSupply = null };
    fee = opt variant { ICRC1 = null };
    advanced_settings = null;
    max_approvals = opt 10000000;
    settle_to_approvals = opt 9990000;
  };
  icrc3 = record {
    maxActiveRecords = 3000;
    settleToRecords = 2000;
    maxRecordsInArchiveInstance = 100000000;
    maxArchivePages = 62500;
    archiveIndexType = variant { Stable = null };
    maxRecordsToArchive = 8000;
    archiveCycles = 20_000_000_000_000;
    supportedBlocks = vec {};
    archiveControllers = opt opt vec { principal \"$ADMIN_PRINCIPAL\" };
  };
  icrc4 = opt record {
    max_balances = opt 200;
    max_transfers = opt 200;
    fee = opt variant { ICRC1 = null };
  };
})"

NACHOS_LEDGER_ID=$(dfx canister id token $NETWORK)
echo ""
echo "✓ NACHOS Ledger deployed: $NACHOS_LEDGER_ID"

# ── Step 2: Initialize ──
echo ""
echo "► Step 2: Calling admin_init..."
dfx canister call token admin_init $NETWORK
echo "✓ admin_init complete"

cd ..

# ── Step 3: Link ledger to vault ──
echo ""
echo "► Step 3: Linking ledger to vault..."
dfx canister call nachos_vault setNachosLedgerPrincipal "(principal \"$NACHOS_LEDGER_ID\")" $NETWORK
echo "✓ Vault linked to NACHOS ledger"

# ── Step 4: Verify ──
echo ""
echo "═══ Verification ═══"
echo ""

echo "Name:            $(dfx canister call "$NACHOS_LEDGER_ID" icrc1_name $NETWORK)"
echo "Symbol:          $(dfx canister call "$NACHOS_LEDGER_ID" icrc1_symbol $NETWORK)"
echo "Decimals:        $(dfx canister call "$NACHOS_LEDGER_ID" icrc1_decimals $NETWORK)"
echo "Fee:             $(dfx canister call "$NACHOS_LEDGER_ID" icrc1_fee $NETWORK)"
echo "Minting Account: $(dfx canister call "$NACHOS_LEDGER_ID" icrc1_minting_account $NETWORK)"
echo "Total Supply:    $(dfx canister call "$NACHOS_LEDGER_ID" icrc1_total_supply $NETWORK)"

echo ""
echo "Vault Status:    $(dfx canister call nachos_vault getSystemStatus $NETWORK 2>&1 | head -c 300)"

echo ""
echo "═══════════════════════════════════════════════════"
echo "NACHOS Ledger ID: $NACHOS_LEDGER_ID"
echo ""
echo "Next steps:"
echo "  1. Update test_nachos_staging.sh line 24: NACHOS_LEDGER=\"$NACHOS_LEDGER_ID\""
echo "  2. Add ICP as accepted mint token:"
echo "     dfx canister call nachos_vault addAcceptedMintToken '(principal \"ryjl3-tyaaa-aaaaa-aaaba-cai\")' $NETWORK"
echo "  3. Run genesis mint (send ICP to treasury subaccount 2, then call genesisMint)"
echo "═══════════════════════════════════════════════════"
