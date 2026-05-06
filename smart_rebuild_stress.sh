#!/bin/bash
# Smart rebuild: only rebuild changed canisters, then run stress tests
# Usage: bash smart_rebuild_stress.sh

set -e

EXCHANGE_SRC="src/exchange/main.mo"
TEST_SRC="src/exchange/examples/test.mo"
EXCHANGE_HASH_FILE=".build_hash_exchange"
TEST_HASH_FILE=".build_hash_test"

# Compute hash of source files
exchange_hash=$(md5sum $EXCHANGE_SRC src/exchange/miscHelperFunctions.mo src/exchange/treasury.mo 2>/dev/null | md5sum | cut -d' ' -f1)
test_hash=$(md5sum $TEST_SRC src/exchange/examples/testActorA.mo src/exchange/examples/testActorB.mo src/exchange/examples/testActorC.mo 2>/dev/null | md5sum | cut -d' ' -f1)

old_exchange_hash=$(cat $EXCHANGE_HASH_FILE 2>/dev/null || echo "none")
old_test_hash=$(cat $TEST_HASH_FILE 2>/dev/null || echo "none")

rebuilt=0

if [ "$exchange_hash" != "$old_exchange_hash" ]; then
    echo "Exchange source changed, rebuilding OTC_backend..."
    dfx build OTC_backend 2>&1 | tail -3
    dfx canister install OTC_backend --mode upgrade --wasm-memory-persistence keep 2>&1
    echo "$exchange_hash" > $EXCHANGE_HASH_FILE
    rebuilt=1
else
    echo "OTC_backend unchanged, skipping build."
fi

if [ "$test_hash" != "$old_test_hash" ]; then
    echo "Test source changed, rebuilding exchange_test..."
    dfx build exchange_test 2>&1 | tail -3
    dfx canister install exchange_test --mode upgrade --wasm-memory-persistence keep 2>&1
    echo "$test_hash" > $TEST_HASH_FILE
    rebuilt=1
else
    echo "exchange_test unchanged, skipping build."
fi

if [ $rebuilt -eq 0 ]; then
    echo "No changes detected. Running stress tests with existing deployment..."
fi

echo "Starting stress tests..."
dfx canister call exchange_test runOnlyStressTests

echo ""
echo "Stress tests launched (async). Monitor with:"
echo "  dfx canister call exchange_test getDiffLogs --query"
echo ""
echo "Or wait ~10 min and check logs."
