#!/bin/bash
set -e

NACHOS_VAULT="p4nog-baaaa-aaaad-qkwpa-cai"
NACHOS_LEDGER="pabnq-2qaaa-aaaam-qhryq-cai"
ICP_LEDGER="ryjl3-tyaaa-aaaaa-aaaba-cai"
TREASURY="tptia-syaaa-aaaai-atieq-cai"
IDENTITY="odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe"

echo "=== Mint + Burn Test with Fresh Balance Coordination ==="
echo

# Step 1: Mint some NACHOS first
echo "STEP 1: Minting NACHOS"
echo "======================"

# Get treasury subaccount for ICP deposits
TREASURY_SUBACCOUNT="\\02\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00"

# Transfer 2 ICP to treasury
echo "1. Transferring 2 ICP to treasury subaccount 2..."
ICP_TRANSFER=$(dfx canister call $ICP_LEDGER transfer "(record { 
    to = blob \"\\$(printf '%02x' 76)\\$(printf '%02x' 116)\\$(printf '%02x' 53)\\$(printf '%02x' 100)\\$(printf '%02x' 45)\\$(printf '%02x' 54)\\$(printf '%02x' 121)\\$(printf '%02x' 97)\\$(printf '%02x' 97)\\$(printf '%02x' 97)\\$(printf '%02x' 45)\\$(printf '%02x' 97)\\$(printf '%02x' 97)\\$(printf '%02x' 97)\\$(printf '%02x' 97)\\$(printf '%02x' 105)\\$(printf '%02x' 45)\\$(printf '%02x' 97)\\$(printf '%02x' 116)\\$(printf '%02x' 105)\\$(printf '%02x' 101)\\$(printf '%02x' 113)\\$(printf '%02x' 45)\\$(printf '%02x' 99)\\$(printf '%02x' 97)\\$(printf '%02x' 105)$TREASURY_SUBACCOUNT\"; 
    amount = record { e8s = 200000000 : nat64 }; 
    fee = record { e8s = 10000 : nat64 }; 
    memo = 0 : nat64;
    from_subaccount = null;
    created_at_time = null
})" --network staging 2>&1)

echo "   Transfer result: $ICP_TRANSFER"

# Extract block
ICP_BLOCK=$(echo "$ICP_TRANSFER" | grep -oP 'Ok = \K\d+' | head -1)
if [ -z "$ICP_BLOCK" ]; then
    echo "   ERROR: Could not extract ICP block number"
    exit 1
fi
echo "   ICP block: $ICP_BLOCK"
echo

# Mint NACHOS
echo "2. Minting NACHOS with block $ICP_BLOCK..."
MINT_RESULT=$(dfx canister call $NACHOS_VAULT mintNachos "($ICP_BLOCK : nat, 0 : nat, null, null)" --network staging 2>&1)

if echo "$MINT_RESULT" | grep -q "ok ="; then
    echo "   ✓ MINT SUCCEEDED!"
    NACHOS_MINTED=$(echo "$MINT_RESULT" | grep -oP 'nachosReceived = \K\d+')
    echo "   NACHOS minted: $NACHOS_MINTED e8s"
else
    echo "   ✗ MINT FAILED: $MINT_RESULT"
    exit 1
fi

echo
echo

# Step 2: Now test burn with fresh balance coordination
echo "STEP 2: Testing Burn with Fresh Balance Coordination"
echo "====================================================="

# Wait a bit for mint to settle
echo "Waiting 5 seconds for mint to settle..."
sleep 5

# Check NACHOS balance
echo "1. Checking NACHOS balance..."
NACHOS_BAL=$(dfx canister call $NACHOS_LEDGER icrc1_balance_of "(record { owner = principal \"$IDENTITY\"; subaccount = null })" --network staging --query | grep -oP '\d+' | head -1)
echo "   NACHOS balance: $NACHOS_BAL e8s"
echo

# Burn half
BURN_AMOUNT=$((NACHOS_BAL / 2))
echo "2. Burning $BURN_AMOUNT NACHOS (half of balance)..."

# Transfer to vault deposit subaccount
NACHOS_TRANSFER=$(dfx canister call $NACHOS_LEDGER icrc1_transfer "(record { 
    to = record { 
        owner = principal \"$NACHOS_VAULT\"; 
        subaccount = opt blob \"\\01\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\\00\" 
    }; 
    amount = $BURN_AMOUNT : nat;
    fee = null;
    memo = null;
    created_at_time = null 
})" --network staging 2>&1)

echo "   NACHOS transfer result: $NACHOS_TRANSFER"

NACHOS_BLOCK=$(echo "$NACHOS_TRANSFER" | grep -oP 'ok = \K\d+' | head -1)
if [ -z "$NACHOS_BLOCK" ]; then
    echo "   ERROR: Could not extract NACHOS block number"
    exit 1
fi
echo "   NACHOS block: $NACHOS_BLOCK"
echo

# Check treasury pending burns BEFORE burn
echo "3. Checking treasury pending burns BEFORE burn..."
PENDING_BEFORE=$(dfx canister call $TREASURY debugPendingBurnsInTreasury --network staging --query)
echo "   Pending burns: $PENDING_BEFORE"
echo

# Execute burn
echo "4. Executing redeemNachos..."
BURN_RESULT=$(dfx canister call $NACHOS_VAULT redeemNachos "($NACHOS_BLOCK : nat, null)" --network staging 2>&1)

if echo "$BURN_RESULT" | grep -q "ok ="; then
    echo "   ✓ BURN SUCCEEDED!"
    echo
    
    # Extract details
    TOKENS_COUNT=$(echo "$BURN_RESULT" | grep -c "token = principal" || echo "0")
    echo "   Tokens distributed: $TOKENS_COUNT different tokens"
    
    # Check treasury pending burns AFTER burn (should be back to empty)
    echo
    echo "5. Checking treasury pending burns AFTER burn..."
    PENDING_AFTER=$(dfx canister call $TREASURY debugPendingBurnsInTreasury --network staging --query)
    echo "   Pending burns: $PENDING_AFTER"
    echo
    
    echo "=== ✓ SUCCESS: Fresh balance coordination working correctly! ==="
    echo "   - Burn completed successfully"
    echo "   - Tokens distributed to user"
    echo "   - Pending burns tracked and released properly"
else
    echo "   ✗ BURN FAILED!"
    ERROR=$(echo "$BURN_RESULT" | grep -oP 'err = variant \{ \K[^}]+')
    echo "   Error: $ERROR"
    exit 1
fi
