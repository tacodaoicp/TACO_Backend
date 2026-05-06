#!/bin/bash
# =============================================================================
# IC Deployment Script - Exchange (OTC_backend) + Exchange Treasury
# Deploys to IC mainnet or staging network
# =============================================================================
#
# Usage:
#   ./deploy_exchange_ic.sh              # Deploy to IC mainnet
#   ./deploy_exchange_ic.sh --staging    # Deploy to staging network
#

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

MAIN_MO="src/exchange/main.mo"
TREASURY_MO="src/exchange/treasury.mo"

NETWORK="ic"
NETWORK_FLAG="--network ic"
if [ "$1" = "--staging" ]; then
    NETWORK="staging"
    NETWORK_FLAG="--network staging"
fi

echo -e "${CYAN}======================================================${NC}"
echo -e "${CYAN}  Exchange Deployment - Network: ${NETWORK}${NC}"
echo -e "${CYAN}======================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# PHASE 1: Identity & Cycles Check
# ---------------------------------------------------------------------------

echo -e "${YELLOW}[Phase 1] Identity & Cycles Check${NC}"
echo ""

DEPLOYER=$(dfx identity get-principal)
IDENTITY_NAME=$(dfx identity whoami)
echo -e "  Identity:  ${GREEN}${IDENTITY_NAME}${NC}"
echo -e "  Principal: ${GREEN}${DEPLOYER}${NC}"
echo ""

read -p "Is this the correct identity for deployment? (y/n): " confirm
if [ "$confirm" != "y" ]; then
    echo -e "${RED}Aborted. Switch identity with: dfx identity use <name>${NC}"
    exit 1
fi

echo ""
echo "Checking cycles balance..."
dfx cycles balance $NETWORK_FLAG || {
    echo -e "${RED}Could not check cycles balance. Make sure you have cycles available.${NC}"
    exit 1
}
echo ""

# ---------------------------------------------------------------------------
# PHASE 2: Auto-patch owner2/owner3/DAO to deployer principal
# ---------------------------------------------------------------------------

echo -e "${YELLOW}[Phase 2] Patching owner & DAO references to deployer${NC}"
echo ""

cp "$MAIN_MO" "${MAIN_MO}.bak"
echo -e "  Backup created: ${MAIN_MO}.bak"

sed -i 's|stable var owner2 = Principal.fromText("[^"]*")|stable var owner2 = Principal.fromText("'"${DEPLOYER}"'")|' "$MAIN_MO"
echo -e "  ${GREEN}Patched${NC} owner2 -> ${DEPLOYER}"

sed -i 's|stable var owner3 = Principal.fromText("[^"]*")|stable var owner3 = Principal.fromText("'"${DEPLOYER}"'")|' "$MAIN_MO"
echo -e "  ${GREEN}Patched${NC} owner3 -> ${DEPLOYER}"

sed -i 's|stable var DAOentry = Principal.fromText("[^"]*")|stable var DAOentry = Principal.fromText("'"${DEPLOYER}"'")|' "$MAIN_MO"
echo -e "  ${GREEN}Patched${NC} DAOentry -> ${DEPLOYER}"

sed -i 's|stable var DAOTreasury = Principal.fromText("[^"]*")|stable var DAOTreasury = Principal.fromText("'"${DEPLOYER}"'")|' "$MAIN_MO"
echo -e "  ${GREEN}Patched${NC} DAOTreasury -> ${DEPLOYER}"

echo ""

# ---------------------------------------------------------------------------
# PHASE 3: Create canisters on IC
# ---------------------------------------------------------------------------

echo -e "${YELLOW}[Phase 3] Creating canisters${NC}"
echo ""

echo "Creating exchange_treasury canister..."
dfx canister create exchange_treasury $NETWORK_FLAG 2>/dev/null || echo "  (already exists)"
TREASURY_ID=$(dfx canister id exchange_treasury $NETWORK_FLAG)
echo -e "  ${GREEN}Exchange Treasury ID: ${TREASURY_ID}${NC}"

echo ""
echo "Creating OTC_backend canister..."
dfx canister create OTC_backend $NETWORK_FLAG 2>/dev/null || echo "  (already exists)"
EXCHANGE_ID=$(dfx canister id OTC_backend $NETWORK_FLAG)
echo -e "  ${GREEN}Exchange ID: ${EXCHANGE_ID}${NC}"

echo ""

# ---------------------------------------------------------------------------
# PHASE 4: Update treasury_text default to assigned treasury ID
# ---------------------------------------------------------------------------

echo -e "${YELLOW}[Phase 4] Patching treasury_text default${NC}"
echo ""

# Update the treasury_text default so the transient actor reference works on first init
sed -i 's|stable var treasury_text = "[^"]*"|stable var treasury_text = "'"${TREASURY_ID}"'"|' "$MAIN_MO"
echo -e "  ${GREEN}Patched${NC} treasury_text -> ${TREASURY_ID}"
echo ""

# ---------------------------------------------------------------------------
# PHASE 5: Build & Deploy
# ---------------------------------------------------------------------------

echo -e "${YELLOW}[Phase 5] Building & Deploying${NC}"
echo ""

echo "Installing mops dependencies..."
mops install
echo ""

echo -e "Deploying ${GREEN}exchange_treasury${NC}..."
dfx deploy exchange_treasury $NETWORK_FLAG
echo -e "  ${GREEN}Exchange Treasury deployed${NC}"
echo ""

echo -e "Deploying ${GREEN}OTC_backend${NC}..."
dfx deploy OTC_backend $NETWORK_FLAG
echo -e "  ${GREEN}OTC_backend deployed${NC}"
echo ""

# ---------------------------------------------------------------------------
# PHASE 6: Configure cross-references
# ---------------------------------------------------------------------------

echo -e "${YELLOW}[Phase 6] Configuring cross-references${NC}"
echo ""

# Set treasury on OTC_backend (this also calls setOTCCanister on treasury via inter-canister)
echo "Setting treasury principal on OTC_backend..."
dfx canister call OTC_backend parameterManagement "(record {
  deleteFromDayBan = null;
  deleteFromAllTimeBan = null;
  addToAllTimeBan = null;
  changeAllowedCalls = null;
  changeallowedSilentWarnings = null;
  addAllowedCanisters = null;
  deleteAllowedCanisters = null;
  treasury_principal = opt \"${TREASURY_ID}\";
})" $NETWORK_FLAG
echo -e "  ${GREEN}Treasury principal set and cross-reference configured${NC}"
echo ""

# Authorize DAO treasury and DAO backend so they aren't subject to spam protection.
# Without this, treasury rebalance cycles trigger rate limiting → bans → broken rebalancing.
# Production IDs (IC mainnet):
DAO_TREASURY_ID="v6t5d-6yaaa-aaaan-qzzja-cai"
DAO_BACKEND_ID="vxqw7-iqaaa-aaaan-qzziq-cai"
echo "Allowlisting DAO treasury and DAO backend on OTC_backend..."
dfx canister call OTC_backend parameterManagement "(record {
  deleteFromDayBan = null;
  deleteFromAllTimeBan = null;
  addToAllTimeBan = null;
  changeAllowedCalls = null;
  changeallowedSilentWarnings = null;
  addAllowedCanisters = opt vec { \"${DAO_TREASURY_ID}\"; \"${DAO_BACKEND_ID}\" };
  deleteAllowedCanisters = null;
  treasury_principal = null;
})" $NETWORK_FLAG
echo -e "  ${GREEN}DAO treasury + DAO backend allowlisted${NC}"
echo ""

# ---------------------------------------------------------------------------
# PHASE 7: Verification
# ---------------------------------------------------------------------------

echo -e "${YELLOW}[Phase 7] Verification${NC}"
echo ""

echo "Exchange Treasury status:"
dfx canister status exchange_treasury $NETWORK_FLAG
echo ""

echo "OTC_backend status:"
dfx canister status OTC_backend $NETWORK_FLAG
echo ""

echo -e "${GREEN}Querying exchange info...${NC}"
dfx canister call OTC_backend exchangeInfo '()' $NETWORK_FLAG || echo -e "${YELLOW}exchangeInfo call failed - may need a moment${NC}"
echo ""

# ---------------------------------------------------------------------------
# Post-deployment
# ---------------------------------------------------------------------------

echo -e "${CYAN}======================================================${NC}"
echo -e "${CYAN}  DEPLOYMENT COMPLETE${NC}"
echo -e "${CYAN}======================================================${NC}"
echo ""
echo "  Exchange Treasury: ${TREASURY_ID}"
echo "  OTC_backend:       ${EXCHANGE_ID}"
echo ""
echo -e "${YELLOW}Post-deployment steps:${NC}"
echo ""
echo "  1. Remove test tokens from accepted tokens if present:"
echo "     dfx canister call OTC_backend addAcceptedToken '(variant { Remove }, \"zxeu2-7aaaa-aaaaq-aaafa-cai\", 0, variant { ICRC12 })' $NETWORK_FLAG"
echo ""
echo "  2. Add production tokens as needed:"
echo "     dfx canister call OTC_backend addAcceptedToken '(variant { Add }, \"<token-id>\", <minimum>, variant { ICRC12 })' $NETWORK_FLAG"
echo ""
echo "  3. Update CanisterIds.mo with the deployed IDs:"
echo "     OTC_backend:       ${EXCHANGE_ID}"
echo "     exchange_treasury: ${TREASURY_ID}"
echo ""
echo "  4. When DAO is ready, update DAOentry and DAOTreasury via code and upgrade."
echo ""
echo -e "  5. Restore source backup: ${GREEN}mv ${MAIN_MO}.bak ${MAIN_MO}${NC}"
echo ""
echo -e "${GREEN}Done!${NC}"
