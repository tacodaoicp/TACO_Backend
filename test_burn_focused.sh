#!/bin/bash
set -e

NACHOS_VAULT="p4nog-baaaa-aaaad-qkwpa-cai"
NACHOS_LEDGER="pabnq-2qaaa-aaaam-qhryq-cai"
TREASURY="tptia-syaaa-aaaai-atieq-cai"
IDENTITY="odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe"

echo "=== Focused Burn Test with Fresh Balance Coordination ==="
echo

# Check current NACHOS balance
echo "1. Checking NACHOS balance..."
NACHOS_BAL=$(dfx canister call $NACHOS_LEDGER icrc1_balance_of "(record { owner = principal \"$IDENTITY\"; subaccount = null })" --network staging --query | grep -oP '\d+' | head -1)
echo "   NACHOS balance: $NACHOS_BAL e8s"

if [ "$NACHOS_BAL" -lt 5000 ]; then
    echo "   ERROR: Insufficient NACHOS balance for burn test (need at least 5000 e8s)"
    exit 1
fi

# Use half of balance for burn test
BURN_AMOUNT=$((NACHOS_BAL / 2))
echo "   Will burn: $BURN_AMOUNT e8s"
echo

# Transfer NACHOS to vault deposit subaccount
echo "2. Transferring NACHOS to vault deposit subaccount..."
TRANSFER_RESULT=$(dfx canister call $NACHOS_LEDGER icrc1_transfer "(record { 
    to = record { 
        owner = principal \"$NACHOS_VAULT\"; 
        subaccount = opt blob \"\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\" 
    }; 
    amount = $BURN_AMOUNT : nat;
    fee = null;
    memo = null;
    created_at_time = null 
})" --network staging 2>&1)

echo "   Transfer result: $TRANSFER_RESULT"

# Extract block number
BLOCK=$(echo "$TRANSFER_RESULT" | grep -oP 'ok = \K\d+' | head -1)
if [ -z "$BLOCK" ]; then
    echo "   ERROR: Could not extract block number from transfer result"
    exit 1
fi
echo "   Block number: $BLOCK"
echo

# Check pending burns in treasury BEFORE burn
echo "3. Checking pending burns in treasury BEFORE burn..."
dfx canister call $TREASURY debugPendingBurnsInTreasury --network staging --query
echo

# Execute burn
echo "4. Executing redeemNachos..."
BURN_RESULT=$(dfx canister call $NACHOS_VAULT redeemNachos "($BLOCK : nat, null)" --network staging 2>&1)
echo "   Burn result: $BURN_RESULT"
echo

# Check if burn succeeded
if echo "$BURN_RESULT" | grep -q "ok ="; then
    echo "   ✓ BURN SUCCEEDED!"
    
    # Extract burn details
    echo "5. Analyzing burn result..."
    TOKENS_SENT=$(echo "$BURN_RESULT" | grep -oP 'tokensToSend = vec \{\K[^}]+' | wc -l)
    echo "   Tokens sent to user: $TOKENS_SENT different tokens"
    
    # Check pending burns AFTER burn (should be released now)
    echo
    echo "6. Checking pending burns in treasury AFTER burn..."
    dfx canister call $TREASURY debugPendingBurnsInTreasury --network staging --query
    echo
    
    echo "=== SUCCESS: Fresh balance coordination is working! ==="
else
    echo "   ✗ BURN FAILED!"
    echo "   Error: $BURN_RESULT"
    exit 1
fi
