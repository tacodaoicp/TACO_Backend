#!/bin/bash
set -euo pipefail

# ════════════════════════════════════════════════════════════════
# Sell All Portfolio Tokens for ICP
# Usage: ./sell_tokens_for_icp.sh [staging|ic]
#        Default: staging
#
# Discovers portfolio tokens from the NACHOS vault dashboard,
# finds ICPSwap pools via the treasury, and sells each non-ICP
# token for ICP. Falls back to KongSwap if no ICPSwap pool.
#
# Override slippage: MAX_SLIPPAGE=10.0 ./sell_tokens_for_icp.sh
# ════════════════════════════════════════════════════════════════

NETWORK="--network ${1:-staging}"

# ── Canister principals ──
NACHOS_VAULT="p4nog-baaaa-aaaad-qkwpa-cai"
TREASURY="tptia-syaaa-aaaai-atieq-cai"
ICP_LEDGER="ryjl3-tyaaa-aaaaa-aaaba-cai"
ICP_PRINCIPAL="ryjl3-tyaaa-aaaaa-aaaba-cai"
KONG="2ipq2-uqaaa-aaaar-qailq-cai"
ICPSWAP_FACTORY="4mmnk-kiaaa-aaaag-qbllq-cai"

# ── Config ──
MAX_SLIPPAGE="${MAX_SLIPPAGE:-5.0}"
ICP_FEE=10000

# ── Tracking ──
TOTAL_ICP_RECEIVED=0
SWAPS_OK=0
SWAPS_FAIL=0
SWAPS_SKIP=0

# ── Token arrays (populated by discover_tokens) ──
T_SYMS=(); T_PRINS=(); T_DECS=(); T_COUNT=0

# ── Pool arrays (populated by discover_pools) ──
P_CIDS=(); P_T0S=(); P_T1S=(); P_COUNT=0

# ════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════

info()  { echo "  → $1"; }
ok()    { echo -e "\033[32m  OK\033[0m: $1"; }
err()   { echo -e "\033[31m  ERR\033[0m: $1"; }
warn()  { echo -e "\033[33m  WARN\033[0m: $1"; }
section() { echo ""; echo "━━━ $1 ━━━"; }

call() {
  local canister="$1" method="$2"
  local result
  if [ -n "${3:-}" ]; then
    result=$(dfx canister call "$canister" "$method" "$3" $NETWORK 2>&1) || true
  else
    result=$(dfx canister call "$canister" "$method" $NETWORK 2>&1) || true
  fi
  echo "$result"
}

# Derive ICPSwap deposit subaccount from principal
# Matches src/swap/utils.mo:principalToSubaccount
# sub[0] = len(principal_bytes), sub[1..len] = bytes, rest = 0
compute_deposit_subaccount() {
  local principal="$1"
  python3 -c "
import base64, sys
p = '${principal}'.replace('-','').upper()
pad = (8 - len(p) % 8) % 8
d = base64.b32decode(p + '=' * pad)[4:]  # skip CRC32
s = bytearray(32)
s[0] = len(d)
for i, b in enumerate(d): s[i+1] = b
print(''.join(f'\\\\{b:02x}' for b in s))
"
}

# ════════════════════════════════════════════════════════════════
# PHASE 1: DISCOVERY
# ════════════════════════════════════════════════════════════════

discover_tokens() {
  info "Querying vault dashboard for portfolio tokens..."
  local dashboard
  dashboard=$(call "$NACHOS_VAULT" getVaultDashboard "(opt (100_000_000 : nat), opt (100_000_000 : nat))")

  # Flatten and extract portfolio records
  local flat
  flat=$(echo "$dashboard" | tr '\n' ' ')

  local portfolio_content
  portfolio_content=$(echo "$flat" | awk '{
    idx = index($0, "portfolio = vec {")
    if (idx == 0) exit
    rest = substr($0, idx + 17)
    depth = 1; result = ""
    for (i = 1; i <= length(rest); i++) {
      c = substr(rest, i, 1)
      if (c == "{") depth++
      if (c == "}") { depth--; if (depth == 0) break }
      result = result c
    }
    print result
  }')

  while IFS= read -r record; do
    [ -z "$record" ] && continue
    local sym prin dec
    sym=$(echo "$record" | grep -oP 'symbol = "\K[^"]+' | head -1)
    prin=$(echo "$record" | grep -oP 'token = principal "\K[^"]+' | head -1)
    dec=$(echo "$record" | grep -oP 'decimals = \K[0-9_]+' | head -1 | tr -d '_')

    if [ -n "$sym" ] && [ -n "$prin" ]; then
      T_SYMS+=("$sym")
      T_PRINS+=("$prin")
      T_DECS+=("${dec:-8}")
      T_COUNT=$((T_COUNT + 1))
    fi
  done <<< "$(echo "$portfolio_content" | grep -oP 'record \{[^{}]*\}' || true)"

  info "Found $T_COUNT portfolio tokens"
}

discover_pools() {
  info "Querying treasury for ICPSwap pools..."
  local pools_raw
  pools_raw=$(call "$TREASURY" listICPSwapPools)

  # Parse each pool record: canisterId, token0, token1
  local flat
  flat=$(echo "$pools_raw" | tr '\n' ' ')

  # Extract individual pool records
  while IFS= read -r record; do
    [ -z "$record" ] && continue
    local cid t0 t1
    cid=$(echo "$record" | grep -oP 'canisterId = principal "\K[^"]+' | head -1)
    t0=$(echo "$record" | grep -oP 'token0 = "\K[^"]+' | head -1)
    t1=$(echo "$record" | grep -oP 'token1 = "\K[^"]+' | head -1)

    if [ -n "$cid" ] && [ -n "$t0" ] && [ -n "$t1" ]; then
      P_CIDS+=("$cid")
      P_T0S+=("$t0")
      P_T1S+=("$t1")
      P_COUNT=$((P_COUNT + 1))
    fi
  done <<< "$(echo "$flat" | grep -oP 'record \{[^{}]*\}' || true)"

  info "Found $P_COUNT ICPSwap pools"
}

# Find ICPSwap pool for a token → ICP pair
# Sets: FOUND_POOL_CID, FOUND_ZERO_FOR_ONE
find_pool_for_token() {
  local token_prin="$1"
  FOUND_POOL_CID=""
  FOUND_ZERO_FOR_ONE=""

  local i
  for ((i=0; i<P_COUNT; i++)); do
    if [ "${P_T0S[$i]}" = "$token_prin" ] && [ "${P_T1S[$i]}" = "$ICP_PRINCIPAL" ]; then
      FOUND_POOL_CID="${P_CIDS[$i]}"
      FOUND_ZERO_FOR_ONE="true"  # selling token0, buying token1 (ICP)
      return 0
    elif [ "${P_T1S[$i]}" = "$token_prin" ] && [ "${P_T0S[$i]}" = "$ICP_PRINCIPAL" ]; then
      FOUND_POOL_CID="${P_CIDS[$i]}"
      FOUND_ZERO_FOR_ONE="false"  # selling token1, buying token0 (ICP)
      return 0
    fi
  done
  return 1
}

# ════════════════════════════════════════════════════════════════
# PHASE 2: BALANCE QUERIES
# ════════════════════════════════════════════════════════════════

# Arrays for balances and fees
B_BALS=(); B_FEES=()

query_balances() {
  info "Querying balances and fees for each token..."
  local i
  for ((i=0; i<T_COUNT; i++)); do
    local prin="${T_PRINS[$i]}"
    local raw bal fee_raw fee

    raw=$(dfx canister call "$prin" icrc1_balance_of "(record { owner = principal \"$USER_PRINCIPAL\"; subaccount = null })" $NETWORK 2>&1) || true
    bal=$(echo "$raw" | grep -oP '[0-9_]+' | head -1 | tr -d '_')
    bal=${bal:-0}

    fee_raw=$(dfx canister call "$prin" icrc1_fee $NETWORK 2>&1) || true
    fee=$(echo "$fee_raw" | grep -oP '[0-9_]+' | head -1 | tr -d '_')
    fee=${fee:-0}

    B_BALS+=("$bal")
    B_FEES+=("$fee")
  done
}

display_plan() {
  section "SELL PLAN"
  printf "  %-8s %20s %12s  %-27s  %s\n" "Symbol" "Balance" "Fee" "Pool" "DEX"
  printf "  %-8s %20s %12s  %-27s  %s\n" "------" "-------" "---" "----" "---"

  local i
  for ((i=0; i<T_COUNT; i++)); do
    local sym="${T_SYMS[$i]}"
    local prin="${T_PRINS[$i]}"
    local bal="${B_BALS[$i]}"
    local fee="${B_FEES[$i]}"
    local pool_info="—"
    local dex="skip"

    if [ "$prin" = "$ICP_PRINCIPAL" ]; then
      dex="(ICP — keep)"
    elif [ "$bal" -le $((fee * 2)) ]; then
      dex="skip (dust)"
    elif find_pool_for_token "$prin"; then
      pool_info="$FOUND_POOL_CID"
      dex="ICPSwap"
    else
      dex="KongSwap"
    fi

    printf "  %-8s %20s %12s  %-27s  %s\n" "$sym" "$bal" "$fee" "$pool_info" "$dex"
  done
  echo ""
}

# ════════════════════════════════════════════════════════════════
# PHASE 3: EXECUTE SWAPS
# ════════════════════════════════════════════════════════════════

sell_via_icpswap() {
  local sym="$1" prin="$2" dec="$3" bal="$4" fee="$5" pool_cid="$6" zero_for_one="$7"

  # Amount to transfer = balance - fee (fee deducted on top by ICRC-1)
  local transfer_amount=$((bal - fee))
  if [ "$transfer_amount" -le 0 ]; then
    warn "$sym: balance ($bal) too low after fee ($fee)"
    return 1
  fi

  # Step 1: Get quote
  info "$sym: Getting quote from ICPSwap pool $pool_cid..."
  local quote_result
  quote_result=$(call "$pool_cid" quote "(record { amountIn = \"$transfer_amount\"; amountOutMinimum = \"0\"; zeroForOne = $zero_for_one })")

  if echo "$quote_result" | grep -q "{ ok"; then
    local expected_out
    expected_out=$(echo "$quote_result" | grep -oP 'ok = \K[0-9_]+' | head -1 | tr -d '_')
    expected_out=${expected_out:-0}
    info "$sym: Quote: $transfer_amount → ~$expected_out ICP (e8s)"

    # Calculate min output with slippage
    local min_out
    min_out=$(python3 -c "print(int($expected_out * (1.0 - $MAX_SLIPPAGE / 100.0)))")
  else
    warn "$sym: Quote failed: $quote_result"
    return 1
  fi

  # Step 2: Transfer to pool's deposit subaccount
  info "$sym: Transferring $transfer_amount to pool $pool_cid..."
  local transfer_result
  transfer_result=$(dfx canister call "$prin" icrc1_transfer "(record {
    to = record {
      owner = principal \"$pool_cid\";
      subaccount = opt blob \"$DEPOSIT_SUBACCOUNT\";
    };
    amount = $transfer_amount : nat;
    fee = opt ($fee : nat);
    memo = null;
    from_subaccount = null;
    created_at_time = null;
  })" $NETWORK 2>&1) || true

  if ! echo "$transfer_result" | grep -q "Ok"; then
    err "$sym: Transfer failed: $transfer_result"
    return 1
  fi
  info "$sym: Transfer OK"

  # Step 3: depositAndSwap
  # amountIn = transfer_amount - fee (depositAndSwap adds fee internally)
  local net_amount=$((transfer_amount - fee))
  if [ "$net_amount" -le 0 ]; then
    warn "$sym: Net amount after double fee is 0"
    return 1
  fi

  info "$sym: Executing depositAndSwap (net=$net_amount, min=$min_out)..."
  local swap_result
  swap_result=$(dfx canister call "$pool_cid" depositAndSwap "(record {
    zeroForOne = $zero_for_one;
    tokenInFee = $fee : nat;
    tokenOutFee = $ICP_FEE : nat;
    amountIn = \"$net_amount\";
    amountOutMinimum = \"$min_out\";
  })" $NETWORK 2>&1) || true

  if echo "$swap_result" | grep -q "{ ok"; then
    local received
    received=$(echo "$swap_result" | grep -oP 'ok = \K[0-9_]+' | head -1 | tr -d '_')
    received=${received:-0}
    # depositAndSwap returns pre-fee amount, actual received = received - ICP_FEE
    local net_received=$((received > ICP_FEE ? received - ICP_FEE : 0))
    ok "$sym: Sold $transfer_amount → received $net_received ICP (e8s) via ICPSwap"
    TOTAL_ICP_RECEIVED=$((TOTAL_ICP_RECEIVED + net_received))
    SWAPS_OK=$((SWAPS_OK + 1))
    return 0
  else
    err "$sym: depositAndSwap failed: $swap_result"
    # Try to recover: check getUserUnusedBalance
    warn "$sym: Checking for recoverable balance in pool..."
    local unused
    unused=$(call "$pool_cid" getUserUnusedBalance "(principal \"$USER_PRINCIPAL\")")
    info "$sym: Pool unused balance: $unused"
    return 1
  fi
}

sell_via_kongswap() {
  local sym="$1" prin="$2" dec="$3" bal="$4" fee="$5"

  local transfer_amount=$((bal - fee))
  if [ "$transfer_amount" -le 0 ]; then
    warn "$sym: balance ($bal) too low after fee ($fee)"
    return 1
  fi

  # Step 1: Get quote
  info "$sym: Getting KongSwap quote..."
  local quote_result
  quote_result=$(call "$KONG" swap_amounts "(\"IC.$sym\", $transfer_amount : nat, \"IC.ICP\")")

  if echo "$quote_result" | grep -q "Err"; then
    local kong_err
    kong_err=$(echo "$quote_result" | grep -oP 'Err = "\K[^"]+' | head -1)
    warn "$sym: KongSwap quote failed (no pool?): $kong_err"
    return 1
  fi

  local expected_out
  expected_out=$(echo "$quote_result" | grep -oP 'receive_amount = \K[0-9_]+' | head -1 | tr -d '_')
  expected_out=${expected_out:-0}
  if [ "$expected_out" -eq 0 ]; then
    warn "$sym: KongSwap quote returned zero output"
    return 1
  fi
  info "$sym: KongSwap quote: $transfer_amount → ~$expected_out ICP (e8s)"

  # Step 2: Transfer to Kong
  info "$sym: Transferring $transfer_amount to KongSwap..."
  local transfer_result
  transfer_result=$(dfx canister call "$prin" icrc1_transfer "(record {
    to = record {
      owner = principal \"$KONG\";
      subaccount = null;
    };
    amount = $transfer_amount : nat;
    fee = opt ($fee : nat);
    memo = null;
    from_subaccount = null;
    created_at_time = null;
  })" $NETWORK 2>&1) || true

  if ! echo "$transfer_result" | grep -q "Ok"; then
    err "$sym: Transfer to Kong failed: $transfer_result"
    return 1
  fi

  # Extract block index
  local block_index
  block_index=$(echo "$transfer_result" | grep -oP 'Ok = \K[0-9_]+' | head -1 | tr -d '_')
  info "$sym: Transfer OK, block=$block_index"

  # Step 3: Swap
  local min_out
  min_out=$(python3 -c "print(int($expected_out * (1.0 - $MAX_SLIPPAGE / 100.0)))")

  info "$sym: Executing KongSwap swap..."
  local swap_result
  swap_result=$(dfx canister call "$KONG" swap "(record {
    pay_token = \"IC.$sym\";
    pay_amount = $transfer_amount : nat;
    pay_tx_id = opt variant { BlockIndex = $block_index : nat };
    receive_token = \"IC.ICP\";
    receive_amount = null;
    receive_address = null;
    max_slippage = opt ($MAX_SLIPPAGE : float64);
    referred_by = null;
  })" $NETWORK 2>&1) || true

  if echo "$swap_result" | grep -q 'status = "Success"'; then
    # KongSwap returns multiple receive_amount fields (per hop + total)
    # The top-level one after transfer_ids is the total
    local received
    received=$(echo "$swap_result" | tr '\n' ' ' | grep -oP 'claim_ids = vec \{[^}]*\};\s*pay_symbol.*?receive_amount = \K[0-9_]+' | head -1 | tr -d '_')
    # Fallback: just get the last receive_amount (top-level)
    if [ -z "$received" ]; then
      received=$(echo "$swap_result" | grep -oP 'receive_amount = \K[0-9_]+' | tail -1 | tr -d '_')
    fi
    received=${received:-0}
    ok "$sym: Sold $transfer_amount → received $received ICP (e8s) via KongSwap"
    TOTAL_ICP_RECEIVED=$((TOTAL_ICP_RECEIVED + received))
    SWAPS_OK=$((SWAPS_OK + 1))
    return 0
  elif echo "$swap_result" | grep -q "Err"; then
    err "$sym: KongSwap swap failed: $(echo "$swap_result" | grep -oP 'Err = "\K[^"]+' | head -1)"
    SWAPS_FAIL=$((SWAPS_FAIL + 1))
    return 1
  else
    err "$sym: KongSwap swap unknown result: $swap_result"
    SWAPS_FAIL=$((SWAPS_FAIL + 1))
    return 1
  fi
}

execute_swaps() {
  section "EXECUTING SWAPS"

  local i
  for ((i=0; i<T_COUNT; i++)); do
    local sym="${T_SYMS[$i]}"
    local prin="${T_PRINS[$i]}"
    local dec="${T_DECS[$i]}"
    local bal="${B_BALS[$i]}"
    local fee="${B_FEES[$i]}"

    echo ""
    info "── $sym ──"

    # Skip ICP (that's what we're buying)
    if [ "$prin" = "$ICP_PRINCIPAL" ]; then
      info "$sym: Skipping (this is ICP)"
      SWAPS_SKIP=$((SWAPS_SKIP + 1))
      continue
    fi

    # Skip dust
    if [ "$bal" -le $((fee * 2)) ]; then
      info "$sym: Skipping (dust: balance=$bal, fee=$fee)"
      SWAPS_SKIP=$((SWAPS_SKIP + 1))
      continue
    fi

    # Try ICPSwap first
    local sold=0
    if find_pool_for_token "$prin"; then
      if sell_via_icpswap "$sym" "$prin" "$dec" "$bal" "$fee" "$FOUND_POOL_CID" "$FOUND_ZERO_FOR_ONE"; then
        sold=1
      else
        warn "$sym: ICPSwap failed, trying KongSwap fallback..."
      fi
    else
      info "$sym: No ICPSwap pool found, trying KongSwap..."
    fi

    # KongSwap fallback
    if [ "$sold" -eq 0 ]; then
      if ! sell_via_kongswap "$sym" "$prin" "$dec" "$bal" "$fee"; then
        err "$sym: All DEXes failed — token not sold"
      fi
    fi
  done
}

# ════════════════════════════════════════════════════════════════
# PHASE 4: SUMMARY
# ════════════════════════════════════════════════════════════════

show_summary() {
  section "SUMMARY"

  # Get final ICP balance
  local icp_bal_raw icp_bal
  icp_bal_raw=$(dfx canister call "$ICP_LEDGER" icrc1_balance_of "(record { owner = principal \"$USER_PRINCIPAL\"; subaccount = null })" $NETWORK 2>&1) || true
  icp_bal=$(echo "$icp_bal_raw" | grep -oP '[0-9_]+' | head -1 | tr -d '_')
  icp_bal=${icp_bal:-0}

  local icp_human
  icp_human=$(python3 -c "print(f'{$icp_bal / 1e8:.4f}')")
  local received_human
  received_human=$(python3 -c "print(f'{$TOTAL_ICP_RECEIVED / 1e8:.4f}')")

  echo ""
  info "Swaps succeeded:  $SWAPS_OK"
  info "Swaps failed:     $SWAPS_FAIL"
  info "Swaps skipped:    $SWAPS_SKIP"
  info "Total ICP gained: $TOTAL_ICP_RECEIVED e8s ($received_human ICP)"
  info "Final ICP balance: $icp_bal e8s ($icp_human ICP)"
  echo ""
}

# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

main() {
  echo "╔══════════════════════════════════════════════╗"
  echo "║   Sell All Portfolio Tokens for ICP           ║"
  echo "║   Network: ${1:-staging}                              ║"
  echo "║   Slippage: ${MAX_SLIPPAGE}%                            ║"
  echo "╚══════════════════════════════════════════════╝"

  # Get identity
  USER_PRINCIPAL=$(dfx identity get-principal 2>&1)
  info "Identity: $USER_PRINCIPAL"

  # Compute deposit subaccount for ICPSwap
  DEPOSIT_SUBACCOUNT=$(compute_deposit_subaccount "$USER_PRINCIPAL")
  info "Deposit subaccount: $DEPOSIT_SUBACCOUNT"

  # Discover tokens and pools
  section "DISCOVERY"
  discover_tokens
  discover_pools

  # Query balances
  query_balances

  # Show plan
  display_plan

  # Execute
  execute_swaps

  # Summary
  show_summary
}

main "$@"
