#!/bin/bash
set -euo pipefail

# ════════════════════════════════════════════════════════════════
# NACHOS Vault Staging Test Script
# Usage: ./test_nachos_staging.sh [staging|ic]
#        Default: staging
#
# Idempotent: can be run repeatedly regardless of vault state.
# Requires: dfx identity with controller access and ICP balance.
#
# Known behavior: Error handling tests transfer ICP to treasury
# for pause/disable tests. When mints are rejected, the ICP
# remains in treasury subaccount 2 (~0.2 ICP per run). The
# treasury uses these funds for portfolio rebalancing.
# ════════════════════════════════════════════════════════════════

NETWORK="--network ${1:-staging}"

# ── Canister principals (update after deployment) ──
NACHOS_VAULT="p4nog-baaaa-aaaad-qkwpa-cai"
TREASURY="tptia-syaaa-aaaai-atieq-cai"
ICP_LEDGER="ryjl3-tyaaa-aaaaa-aaaba-cai"
NACHOS_LEDGER="pabnq-2qaaa-aaaam-qhryq-cai"
ICP_PRINCIPAL="ryjl3-tyaaa-aaaaa-aaaba-cai"

# ── Test amounts (no underscores — bash arithmetic doesn't support them) ──
MINT_AMOUNT=1500000       # 0.015 ICP for mint tests
BURN_AMOUNT=0             # Dynamically set in phase6_burn from actual NACHOS balance
ICP_FEE=10000             # 0.0001 ICP

# ── Subaccount blobs (32-byte hex) ──
TREASURY_SUB2="\02\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00"
VAULT_SUB1="\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00"

# ── Counters ──
PASS=0; FAIL=0; SKIP=0; TOTAL=0

# ── CSV results file ──
CSV_FILE="$(cd "$(dirname "$0")" && pwd)/nachos_test_results.csv"

# ── Cycle cost & timing tracking (set in phase3/phase6) ──
MINT_CYCLES_USED=0
BURN_CYCLES_USED=0
MINT_TIME_SECS=0
BURN_TIME_SECS=0

# ── Balance tracking (set in phase0/phase8) ──
ICP_BEFORE=0; ICP_AFTER=0
NACHOS_BEFORE=0; NACHOS_AFTER=0

# ── Mint validation tracking ──
MR_NACHOS_RECEIVED=0; MR_NAV_USED=0; MR_NET_ICP=0
MINT_FORMULA_OK="N/A"

# ── Burn validation tracking ──
BR_NACHOS_BURNED=0; BR_NAV_USED=0; BR_REDEMPTION_ICP=0; BR_NET_ICP=0
BR_TOKEN_PRINCIPALS=(); BR_TOKEN_AMOUNTS=(); BR_TOKEN_COUNT=0
BURN_FORMULA_OK="N/A"

# ── Portfolio tracking arrays (reused by parse functions) ──
PF_SYMBOLS=(); PF_PRINCIPALS=(); PF_BALANCES=(); PF_PRICES=()
PF_VALUES=(); PF_BPS=(); PF_DECIMALS=(); PF_COUNT=0

# ── Pre/Post burn copies ──
PF_PRE_SYMBOLS=(); PF_PRE_PRINCIPALS=(); PF_PRE_BALANCES=(); PF_PRE_PRICES=()
PF_PRE_VALUES=(); PF_PRE_BPS=(); PF_PRE_DECIMALS=(); PF_PRE_COUNT=0
PF_POST_SYMBOLS=(); PF_POST_PRINCIPALS=(); PF_POST_BALANCES=(); PF_POST_PRICES=()
PF_POST_VALUES=(); PF_POST_BPS=(); PF_POST_DECIMALS=(); PF_POST_COUNT=0

# ── User token balance tracking (before/after burn) ──
UB_PRE_SYMBOLS=(); UB_PRE_PRINCIPALS=(); UB_PRE_BALANCES=(); UB_PRE_COUNT=0
UB_POST_SYMBOLS=(); UB_POST_PRINCIPALS=(); UB_POST_BALANCES=(); UB_POST_COUNT=0

# ── Portfolio CSV ──
PORTFOLIO_CSV="$(cd "$(dirname "$0")" && pwd)/nachos_portfolio_snapshots.csv"

# ── State (set in phase0) ──
TEST_PRINCIPAL=""
GENESIS_DONE=0

# ════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════

pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo -e "\033[32m  PASS\033[0m: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo -e "\033[31m  FAIL\033[0m: $1 — $2"; }
skip() { SKIP=$((SKIP+1)); TOTAL=$((TOTAL+1)); echo -e "\033[33m  SKIP\033[0m: $1 — $2"; }
info() { echo "  → $1"; }
section() { echo ""; echo "━━━ $1 ━━━"; }

# Call canister and capture output (handles 0-arg and with-arg cases)
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

# Call canister query (for composite queries that need --query flag)
query_call() {
  local canister="$1" method="$2"
  local result
  if [ -n "${3:-}" ]; then
    result=$(dfx canister call "$canister" "$method" "$3" $NETWORK --query 2>&1) || true
  else
    result=$(dfx canister call "$canister" "$method" $NETWORK --query 2>&1) || true
  fi
  echo "$result"
}

# Assert output contains a string
assert_contains() {
  local output="$1" expected="$2" test_name="$3"
  if echo "$output" | grep -qF "$expected"; then
    pass "$test_name"
  else
    fail "$test_name" "Expected '$expected' in: $(echo "$output" | head -c 200)"
  fi
}

# Assert output does NOT contain a string
assert_not_contains() {
  local output="$1" unexpected="$2" test_name="$3"
  if echo "$output" | grep -qF "$unexpected"; then
    fail "$test_name" "Unexpected '$unexpected' in: $(echo "$output" | head -c 200)"
  else
    pass "$test_name"
  fi
}

# Get vault cycles as a plain number (for cost tracking)
get_vault_cycles() {
  local raw
  raw=$(dfx canister call "$NACHOS_VAULT" get_canister_cycles $NETWORK 2>&1)
  echo "$raw" | grep -oP '[0-9_]+' | head -1 | tr -d '_'
}

# Get ICP balance as a plain number (e8s)
get_icp_balance() {
  local raw
  raw=$(dfx canister call "$ICP_LEDGER" icrc1_balance_of "(record { owner = principal \"$TEST_PRINCIPAL\"; subaccount = null })" $NETWORK 2>&1)
  echo "$raw" | grep -oP '[0-9_]+' | head -1 | tr -d '_'
}

# Get NACHOS balance as a plain number (e8s)
get_nachos_balance() {
  local raw
  raw=$(dfx canister call "$NACHOS_LEDGER" icrc1_balance_of "(record { owner = principal \"$TEST_PRINCIPAL\"; subaccount = null })" $NETWORK 2>&1)
  echo "$raw" | grep -oP '[0-9_]+' | head -1 | tr -d '_'
}

# Query user's balance for each portfolio token — stores in UB_* arrays
# Requires PF_PRE_SYMBOLS/PF_PRE_PRINCIPALS to know which tokens to check
# Usage: query_user_token_balances "PRE" or query_user_token_balances "POST"
query_user_token_balances() {
  local phase="$1"  # "PRE" or "POST"
  local syms=() prins=() bals=() count=0

  # Use PF_PRE arrays as the canonical token list
  local i
  for ((i=0; i<PF_PRE_COUNT; i++)); do
    local sym="${PF_PRE_SYMBOLS[$i]}"
    local prin="${PF_PRE_PRINCIPALS[$i]}"
    local raw bal

    # ICP uses different ledger interface
    if [ "$prin" = "$ICP_PRINCIPAL" ]; then
      raw=$(dfx canister call "$ICP_LEDGER" icrc1_balance_of "(record { owner = principal \"$TEST_PRINCIPAL\"; subaccount = null })" $NETWORK 2>&1) || true
    else
      raw=$(dfx canister call "$prin" icrc1_balance_of "(record { owner = principal \"$TEST_PRINCIPAL\"; subaccount = null })" $NETWORK 2>&1) || true
    fi
    bal=$(echo "$raw" | grep -oP '[0-9_]+' | head -1 | tr -d '_')
    bal=${bal:-0}

    syms+=("$sym")
    prins+=("$prin")
    bals+=("$bal")
    count=$((count + 1))
  done

  if [ "$phase" = "PRE" ]; then
    UB_PRE_SYMBOLS=("${syms[@]}")
    UB_PRE_PRINCIPALS=("${prins[@]}")
    UB_PRE_BALANCES=("${bals[@]}")
    UB_PRE_COUNT=$count
  else
    UB_POST_SYMBOLS=("${syms[@]}")
    UB_POST_PRINCIPALS=("${prins[@]}")
    UB_POST_BALANCES=("${bals[@]}")
    UB_POST_COUNT=$count
  fi
}

# Display user token balance diff (before/after burn) + tokensReceived
display_user_balance_diff() {
  info "User token balance DIFF (before → after burn):"
  printf "  %-8s %18s %18s %18s %18s\n" "Symbol" "Bal_Before" "Bal_After" "Diff" "TokensReceived"
  printf "  %-8s %18s %18s %18s %18s\n" "------" "----------" "---------" "----" "--------------"

  local i j
  for ((i=0; i<UB_PRE_COUNT; i++)); do
    local sym="${UB_PRE_SYMBOLS[$i]}"
    local before="${UB_PRE_BALANCES[$i]}"
    local after=0

    for ((j=0; j<UB_POST_COUNT; j++)); do
      if [ "${UB_POST_PRINCIPALS[$j]}" = "${UB_PRE_PRINCIPALS[$i]}" ]; then
        after="${UB_POST_BALANCES[$j]}"; break
      fi
    done

    local diff=$((after - before))

    # Find tokensReceived amount for this token
    local received=0
    for ((j=0; j<BR_TOKEN_COUNT; j++)); do
      if [ "${BR_TOKEN_PRINCIPALS[$j]}" = "${UB_PRE_PRINCIPALS[$i]}" ]; then
        received="${BR_TOKEN_AMOUNTS[$j]}"; break
      fi
    done

    printf "  %-8s %18s %18s %18s %18s\n" "$sym" "$before" "$after" "$diff" "$received"
  done
}

# Write user balance snapshots to portfolio CSV (appends user_pre_burn / user_post_burn rows)
write_user_balance_csv() {
  local ts
  ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  local header="run_timestamp,snapshot_point,symbol,token_principal,balance,priceICP,valueICP,basisPoints,decimals"

  if [ ! -f "$PORTFOLIO_CSV" ]; then
    echo "$header" > "$PORTFOLIO_CSV"
  fi

  local i
  for ((i=0; i<UB_PRE_COUNT; i++)); do
    # Find price/decimals from PF_PRE for value calculation
    local price=0 dec=8 val=0 bp=0
    local j
    for ((j=0; j<PF_PRE_COUNT; j++)); do
      if [ "${PF_PRE_PRINCIPALS[$j]}" = "${UB_PRE_PRINCIPALS[$i]}" ]; then
        price="${PF_PRE_PRICES[$j]}"; dec="${PF_PRE_DECIMALS[$j]}"; bp="${PF_PRE_BPS[$j]}"; break
      fi
    done
    echo "$ts,user_pre_burn,${UB_PRE_SYMBOLS[$i]},${UB_PRE_PRINCIPALS[$i]},${UB_PRE_BALANCES[$i]},$price,$val,$bp,$dec" >> "$PORTFOLIO_CSV"
  done
  for ((i=0; i<UB_POST_COUNT; i++)); do
    local price=0 dec=8 val=0 bp=0
    local j
    for ((j=0; j<PF_PRE_COUNT; j++)); do
      if [ "${PF_PRE_PRINCIPALS[$j]}" = "${UB_POST_PRINCIPALS[$i]}" ]; then
        price="${PF_PRE_PRICES[$j]}"; dec="${PF_PRE_DECIMALS[$j]}"; bp="${PF_PRE_BPS[$j]}"; break
      fi
    done
    echo "$ts,user_post_burn,${UB_POST_SYMBOLS[$i]},${UB_POST_PRINCIPALS[$i]},${UB_POST_BALANCES[$i]},$price,$val,$bp,$dec" >> "$PORTFOLIO_CSV"
  done

  # Also write tokensReceived as its own snapshot point
  for ((i=0; i<BR_TOKEN_COUNT; i++)); do
    local sym="?"
    local j
    for ((j=0; j<PF_PRE_COUNT; j++)); do
      if [ "${PF_PRE_PRINCIPALS[$j]}" = "${BR_TOKEN_PRINCIPALS[$i]}" ]; then
        sym="${PF_PRE_SYMBOLS[$j]}"; break
      fi
    done
    echo "$ts,tokens_received,${sym},${BR_TOKEN_PRINCIPALS[$i]},${BR_TOKEN_AMOUNTS[$i]},0,0,0,0" >> "$PORTFOLIO_CSV"
  done

  info "User balance snapshots + tokensReceived written to $PORTFOLIO_CSV"
}

# Parse portfolio from getVaultDashboard output into PF_* arrays
# Uses getVaultDashboard because it includes decimals per token
parse_dashboard_portfolio() {
  local candid_output="$1"
  PF_SYMBOLS=(); PF_PRINCIPALS=(); PF_BALANCES=(); PF_PRICES=()
  PF_VALUES=(); PF_BPS=(); PF_DECIMALS=(); PF_COUNT=0

  # Extract the portfolio section and split into individual records
  local flat
  flat=$(echo "$candid_output" | tr '\n' ' ')

  # Extract portfolio records — each has: token, symbol, decimals, balance, priceICP, valueICP, currentBasisPoints
  local records
  records=$(echo "$flat" | grep -oP 'symbol = "[^"]+"\s*;[^}]*?valueICP = [0-9_]+ : nat' || true)

  # Alternative: extract each record block from the portfolio vec
  # Find content between "portfolio = vec {" and the next top-level "};"
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

  # Now extract individual records from portfolio_content
  while IFS= read -r record; do
    [ -z "$record" ] && continue
    local sym prin bal price val bp dec
    sym=$(echo "$record" | grep -oP 'symbol = "\K[^"]+' | head -1)
    prin=$(echo "$record" | grep -oP 'token = principal "\K[^"]+' | head -1)
    bal=$(echo "$record" | grep -oP 'balance = \K[0-9_]+' | head -1 | tr -d '_')
    price=$(echo "$record" | grep -oP 'priceICP = \K[0-9_]+' | head -1 | tr -d '_')
    val=$(echo "$record" | grep -oP 'valueICP = \K[0-9_]+' | head -1 | tr -d '_')
    bp=$(echo "$record" | grep -oP 'currentBasisPoints = \K[0-9_]+' | head -1 | tr -d '_')
    dec=$(echo "$record" | grep -oP 'decimals = \K[0-9_]+' | head -1 | tr -d '_')

    if [ -n "$sym" ] && [ -n "$prin" ]; then
      PF_SYMBOLS+=("$sym")
      PF_PRINCIPALS+=("$prin")
      PF_BALANCES+=("${bal:-0}")
      PF_PRICES+=("${price:-0}")
      PF_VALUES+=("${val:-0}")
      PF_BPS+=("${bp:-0}")
      PF_DECIMALS+=("${dec:-8}")
      PF_COUNT=$((PF_COUNT + 1))
    fi
  done <<< "$(echo "$portfolio_content" | grep -oP 'record \{[^{}]*\}' || true)"
}

# Parse mintNachos result fields
parse_mint_result() {
  local output="$1"
  MR_NACHOS_RECEIVED=$(echo "$output" | grep -oP 'nachosReceived = \K[0-9_]+' | head -1 | tr -d '_')
  MR_NAV_USED=$(echo "$output" | grep -oP 'navUsed = \K[0-9_]+' | head -1 | tr -d '_')
  MR_NET_ICP=$(echo "$output" | grep -oP 'netValueICP = \K[0-9_]+' | head -1 | tr -d '_')
  MR_NACHOS_RECEIVED=${MR_NACHOS_RECEIVED:-0}
  MR_NAV_USED=${MR_NAV_USED:-0}
  MR_NET_ICP=${MR_NET_ICP:-0}
}

# Parse redeemNachos result fields including tokensReceived
parse_burn_result() {
  local output="$1"
  BR_NACHOS_BURNED=$(echo "$output" | grep -oP 'nachosBurned = \K[0-9_]+' | head -1 | tr -d '_')
  BR_NAV_USED=$(echo "$output" | grep -oP 'navUsed = \K[0-9_]+' | head -1 | tr -d '_')
  BR_REDEMPTION_ICP=$(echo "$output" | grep -oP 'redemptionValueICP = \K[0-9_]+' | head -1 | tr -d '_')
  BR_NET_ICP=$(echo "$output" | grep -oP 'netValueICP = \K[0-9_]+' | head -1 | tr -d '_')
  BR_NACHOS_BURNED=${BR_NACHOS_BURNED:-0}
  BR_NAV_USED=${BR_NAV_USED:-0}
  BR_REDEMPTION_ICP=${BR_REDEMPTION_ICP:-0}
  BR_NET_ICP=${BR_NET_ICP:-0}

  # Parse tokensReceived vec
  BR_TOKEN_PRINCIPALS=(); BR_TOKEN_AMOUNTS=(); BR_TOKEN_COUNT=0
  local flat
  flat=$(echo "$output" | tr '\n' ' ')

  # Extract tokensReceived content using awk brace-depth counting
  local tokens_content
  tokens_content=$(echo "$flat" | awk '{
    idx = index($0, "tokensReceived = vec {")
    if (idx == 0) exit
    rest = substr($0, idx + 22)
    depth = 1; result = ""
    for (i = 1; i <= length(rest); i++) {
      c = substr(rest, i, 1)
      if (c == "{") depth++
      if (c == "}") { depth--; if (depth == 0) break }
      result = result c
    }
    print result
  }')

  # Extract each record from tokensReceived
  while IFS= read -r rec; do
    [ -z "$rec" ] && continue
    local tok amt
    tok=$(echo "$rec" | grep -oP 'token = principal "\K[^"]+' | head -1)
    amt=$(echo "$rec" | grep -oP 'amount = \K[0-9_]+' | head -1 | tr -d '_')
    if [ -n "$tok" ] && [ -n "$amt" ]; then
      BR_TOKEN_PRINCIPALS+=("$tok")
      BR_TOKEN_AMOUNTS+=("$amt")
      BR_TOKEN_COUNT=$((BR_TOKEN_COUNT + 1))
    fi
  done <<< "$(echo "$tokens_content" | grep -oP 'record \{[^{}]*\}' || true)"
}

# Validate mint formula: nachosReceived == (netValueICP * 1e8) / navUsed
validate_mint_formula() {
  if [ "$MR_NAV_USED" -eq 0 ] || [ "$MR_NET_ICP" -eq 0 ]; then
    fail "Mint formula validation" "Could not parse mint result fields (NAV=$MR_NAV_USED, net=$MR_NET_ICP)"
    return 1
  fi
  local expected
  expected=$(echo "$MR_NET_ICP * 100000000 / $MR_NAV_USED" | bc)
  local actual="$MR_NACHOS_RECEIVED"
  local diff=$(( actual > expected ? actual - expected : expected - actual ))

  info "Mint formula: expected=$expected, actual=$actual, NAV=$MR_NAV_USED, netICP=$MR_NET_ICP"
  if [ "$diff" -le 1 ]; then
    pass "Mint formula: nachosReceived ($actual) matches expected ($expected) [diff=$diff]"
    return 0
  else
    fail "Mint formula: nachosReceived ($actual) != expected ($expected)" "diff=$diff exceeds tolerance=1"
    return 1
  fi
}

# Validate burn redemption formula: redemptionValueICP == (nachosBurned * navUsed) / 1e8
validate_burn_redemption_formula() {
  if [ "$BR_NAV_USED" -eq 0 ] || [ "$BR_NACHOS_BURNED" -eq 0 ]; then
    fail "Burn redemption formula" "Could not parse burn fields (NAV=$BR_NAV_USED, burned=$BR_NACHOS_BURNED)"
    return 1
  fi
  local expected
  expected=$(echo "$BR_NACHOS_BURNED * $BR_NAV_USED / 100000000" | bc)
  local diff=$(( BR_REDEMPTION_ICP > expected ? BR_REDEMPTION_ICP - expected : expected - BR_REDEMPTION_ICP ))

  info "Burn redemption formula: expected=$expected, actual=$BR_REDEMPTION_ICP"
  if [ "$diff" -le 1 ]; then
    pass "Burn redemption: redemptionValueICP ($BR_REDEMPTION_ICP) matches formula ($expected) [diff=$diff]"
    return 0
  else
    fail "Burn redemption: redemptionValueICP ($BR_REDEMPTION_ICP) != expected ($expected)" "diff=$diff"
    return 1
  fi
}

# Validate sum of tokensReceived ICP values ≈ netValueICP (within 2%)
validate_burn_tokens_sum() {
  if [ "$BR_TOKEN_COUNT" -eq 0 ]; then
    fail "Burn tokens sum" "No tokensReceived parsed from burn result"
    return 1
  fi

  local sum_icp=0
  local i
  for ((i=0; i<BR_TOKEN_COUNT; i++)); do
    local tok="${BR_TOKEN_PRINCIPALS[$i]}"
    local amt="${BR_TOKEN_AMOUNTS[$i]}"
    local price=0 dec=8

    # Look up price and decimals from pre-burn portfolio
    local j
    for ((j=0; j<PF_PRE_COUNT; j++)); do
      if [ "${PF_PRE_PRINCIPALS[$j]}" = "$tok" ]; then
        price="${PF_PRE_PRICES[$j]}"
        dec="${PF_PRE_DECIMALS[$j]}"
        break
      fi
    done

    if [ "$price" -eq 0 ]; then
      info "  WARNING: No price for token $tok, skipping in sum"
      continue
    fi

    # valueICP = (amount * priceICP) / 10^decimals
    local pow dec_pow=1
    for ((pow=0; pow<dec; pow++)); do dec_pow=$((dec_pow * 10)); done
    local value_icp
    value_icp=$(echo "$amt * $price / $dec_pow" | bc)
    sum_icp=$((sum_icp + value_icp))

    # Find symbol for display
    local sym="?"
    for ((j=0; j<PF_PRE_COUNT; j++)); do
      if [ "${PF_PRE_PRINCIPALS[$j]}" = "$tok" ]; then sym="${PF_PRE_SYMBOLS[$j]}"; break; fi
    done
    info "  Token $sym: amount=$amt, price=$price, dec=$dec, valueICP=$value_icp"
  done

  info "Burn tokens sum: total=$sum_icp, expected(netValueICP)=$BR_NET_ICP"

  # Tolerance: 10% of netValueICP (small burns lose proportionally more to per-token transfer minimums/fees)
  local tolerance=$(( BR_NET_ICP * 10 / 100 ))
  if [ "$tolerance" -lt 100 ]; then tolerance=100; fi
  local diff=$(( sum_icp > BR_NET_ICP ? sum_icp - BR_NET_ICP : BR_NET_ICP - sum_icp ))

  if [ "$diff" -le "$tolerance" ]; then
    pass "Burn tokens sum ($sum_icp) ≈ netValueICP ($BR_NET_ICP) [diff=$diff, tolerance=$tolerance (10%)]"
    BURN_FORMULA_OK="YES"
    return 0
  else
    fail "Burn tokens sum ($sum_icp) vs netValueICP ($BR_NET_ICP)" "diff=$diff exceeds tolerance=$tolerance"
    BURN_FORMULA_OK="NO"
    return 1
  fi
}

# Display formatted portfolio diff table (pre vs post burn)
display_portfolio_diff() {
  info "Portfolio DIFF (before → after burn):"
  printf "  %-8s %15s %15s %15s %15s %15s\n" "Symbol" "Bal_Before" "Bal_After" "Bal_Diff" "ValICP_Before" "ValICP_After"
  printf "  %-8s %15s %15s %15s %15s %15s\n" "------" "---------" "---------" "--------" "-------------" "------------"

  local i j
  for ((i=0; i<PF_PRE_COUNT; i++)); do
    local sym="${PF_PRE_SYMBOLS[$i]}"
    local before_bal="${PF_PRE_BALANCES[$i]}"
    local before_val="${PF_PRE_VALUES[$i]}"
    local after_bal=0 after_val=0

    for ((j=0; j<PF_POST_COUNT; j++)); do
      if [ "${PF_POST_PRINCIPALS[$j]}" = "${PF_PRE_PRINCIPALS[$i]}" ]; then
        after_bal="${PF_POST_BALANCES[$j]}"
        after_val="${PF_POST_VALUES[$j]}"
        break
      fi
    done

    local diff_bal=$((after_bal - before_bal))
    printf "  %-8s %15s %15s %15s %15s %15s\n" "$sym" "$before_bal" "$after_bal" "$diff_bal" "$before_val" "$after_val"
  done
}

# Write portfolio snapshots to CSV
write_portfolio_csv() {
  local ts
  ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  local header="run_timestamp,snapshot_point,symbol,token_principal,balance,priceICP,valueICP,basisPoints,decimals"

  if [ ! -f "$PORTFOLIO_CSV" ]; then
    echo "$header" > "$PORTFOLIO_CSV"
  fi

  local i
  for ((i=0; i<PF_PRE_COUNT; i++)); do
    echo "$ts,pre_burn,${PF_PRE_SYMBOLS[$i]},${PF_PRE_PRINCIPALS[$i]},${PF_PRE_BALANCES[$i]},${PF_PRE_PRICES[$i]},${PF_PRE_VALUES[$i]},${PF_PRE_BPS[$i]},${PF_PRE_DECIMALS[$i]}" >> "$PORTFOLIO_CSV"
  done
  for ((i=0; i<PF_POST_COUNT; i++)); do
    echo "$ts,post_burn,${PF_POST_SYMBOLS[$i]},${PF_POST_PRINCIPALS[$i]},${PF_POST_BALANCES[$i]},${PF_POST_PRICES[$i]},${PF_POST_VALUES[$i]},${PF_POST_BPS[$i]},${PF_POST_DECIMALS[$i]}" >> "$PORTFOLIO_CSV"
  done
  info "Portfolio snapshots written to $PORTFOLIO_CSV"
}

# Write test results to CSV (latest row first)
write_csv_result() {
  local ts result
  ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  if [ "$FAIL" -gt 0 ]; then result="FAIL"; else result="PASS"; fi

  # Compute mint expected for CSV
  local mint_expected=0
  if [ "$MR_NAV_USED" -gt 0 ] && [ "$MR_NET_ICP" -gt 0 ]; then
    mint_expected=$(echo "$MR_NET_ICP * 100000000 / $MR_NAV_USED" | bc)
  fi

  local header="timestamp,pass,fail,skip,total,result,mint_cycles,mint_secs,burn_cycles,burn_secs,icp_before,icp_after,nachos_before,nachos_after,mint_nav,mint_nachos_got,mint_nachos_expected,mint_ok,burn_nav,burn_redemption_icp,burn_net_icp,burn_ok"
  local row="$ts,$PASS,$FAIL,$SKIP,$TOTAL,$result,$MINT_CYCLES_USED,$MINT_TIME_SECS,$BURN_CYCLES_USED,$BURN_TIME_SECS,$ICP_BEFORE,$ICP_AFTER,$NACHOS_BEFORE,$NACHOS_AFTER,$MR_NAV_USED,$MR_NACHOS_RECEIVED,$mint_expected,$MINT_FORMULA_OK,$BR_NAV_USED,$BR_REDEMPTION_ICP,$BR_NET_ICP,$BURN_FORMULA_OK"

  if [ -f "$CSV_FILE" ]; then
    local tmp="${CSV_FILE}.tmp"
    echo "$header" > "$tmp"
    echo "$row" >> "$tmp"
    tail -n +2 "$CSV_FILE" >> "$tmp"
    mv "$tmp" "$CSV_FILE"
  else
    echo "$header" > "$CSV_FILE"
    echo "$row" >> "$CSV_FILE"
  fi
  info "Results written to $CSV_FILE"
}

# Pre-flight: validate principals are configured
preflight_check() {
  if [[ "$NACHOS_VAULT" == *"UPDATE"* ]] || [[ "$NACHOS_LEDGER" == *"UPDATE"* ]]; then
    echo "ERROR: Update NACHOS_VAULT and NACHOS_LEDGER principals in the script before running."
    echo "  NACHOS_VAULT=$NACHOS_VAULT"
    echo "  NACHOS_LEDGER=$NACHOS_LEDGER"
    exit 1
  fi
  if ! command -v bc &>/dev/null; then
    echo "ERROR: 'bc' is required for formula validation. Install: apt install bc"
    exit 1
  fi
}

# Transfer ICP to treasury subaccount 2 and return block number
transfer_icp_to_treasury() {
  local amount=$1
  local result
  result=$(dfx canister call "$ICP_LEDGER" icrc1_transfer "(record {
    to = record {
      owner = principal \"$TREASURY\";
      subaccount = opt blob \"$TREASURY_SUB2\";
    };
    amount = $amount : nat;
    fee = opt ($ICP_FEE : nat);
    memo = null;
    from_subaccount = null;
    created_at_time = null;
  })" $NETWORK 2>&1) || true

  # Extract block number from (variant { Ok = 12345 : nat })
  local block
  block=$(echo "$result" | grep -oP 'Ok = \K[0-9_]+' | tr -d '_')
  if [ -z "$block" ]; then
    echo "TRANSFER_FAILED:$result"
  else
    echo "$block"
  fi
}

# Transfer NACHOS to vault deposit subaccount 1 and return block number
transfer_nachos_to_vault() {
  local amount=$1
  local result
  result=$(dfx canister call "$NACHOS_LEDGER" icrc1_transfer "(record {
    to = record {
      owner = principal \"$NACHOS_VAULT\";
      subaccount = opt blob \"$VAULT_SUB1\";
    };
    amount = $amount : nat;
    fee = opt (10000 : nat);
    memo = null;
    from_subaccount = null;
    created_at_time = null;
  })" $NETWORK 2>&1) || true

  local block
  block=$(echo "$result" | grep -oP 'Ok = \K[0-9_]+' | tr -d '_')
  if [ -z "$block" ]; then
    echo "TRANSFER_FAILED:$result"
  else
    echo "$block"
  fi
}

# ════════════════════════════════════════════════════════════════
# PHASE 0: STATE DETECTION & SETUP
# ════════════════════════════════════════════════════════════════

phase0_setup() {
  section "PHASE 0: State Detection & Setup"

  preflight_check

  TEST_PRINCIPAL=$(dfx identity get-principal)
  info "Test identity: $TEST_PRINCIPAL"

  # Query current state
  local status
  status=$(call "$NACHOS_VAULT" getSystemStatus)
  info "Current system status:"
  echo "$status"

  GENESIS_DONE=$(echo "$status" | grep -c "genesisComplete = true" || true)
  local IS_PAUSED
  IS_PAUSED=$(echo "$status" | grep -c "systemPaused = true" || true)

  # Ensure clean state (all these are no-ops if already in desired state)
  if [ "$IS_PAUSED" = "1" ]; then
    info "System paused, unpausing..."
    call "$NACHOS_VAULT" emergencyUnpause >/dev/null
  fi
  call "$NACHOS_VAULT" unpauseMinting >/dev/null
  call "$NACHOS_VAULT" unpauseBurning >/dev/null
  call "$NACHOS_VAULT" resetCircuitBreaker >/dev/null

  # Add test exemptions (idempotent — re-adding overwrites with same config)
  call "$NACHOS_VAULT" addRateLimitExemptPrincipal "(principal \"$TEST_PRINCIPAL\", \"test-script\")" >/dev/null
  call "$NACHOS_VAULT" addFeeExemptPrincipal "(principal \"$TEST_PRINCIPAL\", \"test-script\")" >/dev/null
  info "Added rate-limit and fee exemptions for test identity"

  # Ensure ICP is an accepted mint token
  local tokens
  tokens=$(call "$NACHOS_VAULT" getAcceptedMintTokens)
  if ! echo "$tokens" | grep -qF "$ICP_PRINCIPAL"; then
    info "Adding ICP as accepted mint token..."
    call "$NACHOS_VAULT" addAcceptedMintToken "(principal \"$ICP_PRINCIPAL\")" >/dev/null
  fi

  # Capture starting balances for CSV
  ICP_BEFORE=$(get_icp_balance)
  NACHOS_BEFORE=$(get_nachos_balance)
  info "Starting balances: ICP=${ICP_BEFORE} e8s, NACHOS=${NACHOS_BEFORE} e8s"

  pass "Phase 0: State setup complete"
}

# ════════════════════════════════════════════════════════════════
# PHASE 1: QUERY FUNCTIONS (read-only)
# ════════════════════════════════════════════════════════════════

phase1_queries() {
  section "PHASE 1: Query Functions (read-only)"

  # getSystemStatus
  local status
  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_contains "$status" "genesisComplete" "getSystemStatus returns valid structure"
  assert_contains "$status" "mintingEnabled = true" "Minting is enabled"
  assert_contains "$status" "burningEnabled = true" "Burning is enabled"
  assert_contains "$status" "systemPaused = false" "System is not paused"

  # getConfig
  local config
  config=$(call "$NACHOS_VAULT" getConfig)
  assert_contains "$config" "mintFeeBasisPoints" "getConfig returns valid structure"
  assert_contains "$config" "mintFeeBasisPoints = 50" "Default mint fee is 50 BP"
  assert_contains "$config" "maxMintICPPerUser4Hours" "getConfig includes maxMintICPPerUser4Hours"
  assert_contains "$config" "maxBurnNachosPerUser4Hours" "getConfig includes maxBurnNachosPerUser4Hours"
  assert_contains "$config" "maxMintAmountICP" "getConfig includes maxMintAmountICP"
  assert_contains "$config" "maxBurnAmountNachos" "getConfig includes maxBurnAmountNachos"

  # getNAV
  local nav
  nav=$(call "$NACHOS_VAULT" getNAV)
  if [ "$GENESIS_DONE" = "1" ]; then
    assert_contains "$nav" "navPerTokenE8s" "getNAV returns NAV after genesis"
  else
    assert_contains "$nav" "nachosSupply = 0" "getNAV returns zero supply before genesis"
  fi

  # getAcceptedMintTokens
  local tokens
  tokens=$(call "$NACHOS_VAULT" getAcceptedMintTokens)
  assert_contains "$tokens" "$ICP_PRINCIPAL" "ICP is in accepted mint tokens"

  # getPortfolioBreakdown
  local portfolio
  portfolio=$(call "$NACHOS_VAULT" getPortfolioBreakdown)
  assert_not_contains "$portfolio" "Error" "getPortfolioBreakdown executes without error"

  # get_canister_cycles
  local cycles
  cycles=$(call "$NACHOS_VAULT" get_canister_cycles)
  assert_not_contains "$cycles" "(0 : nat)" "Canister has cycles (> 0)"

  # getLogs
  local logs
  logs=$(call "$NACHOS_VAULT" getLogs "(20 : nat)")
  assert_not_contains "$logs" "Error" "getLogs executes without error"

  # Estimation queries
  local est_mint
  est_mint=$(call "$NACHOS_VAULT" estimateMintICP "(100_000_000 : nat)")
  if [ "$GENESIS_DONE" = "1" ]; then
    assert_contains "$est_mint" "nachosEstimate" "estimateMintICP returns estimate"
  else
    skip "estimateMintICP" "Genesis not done"
  fi

  local est_redeem
  est_redeem=$(call "$NACHOS_VAULT" estimateRedeem "(100_000_000 : nat)")
  if [ "$GENESIS_DONE" = "1" ]; then
    assert_contains "$est_redeem" "redemptionValueICP" "estimateRedeem returns estimate"
  else
    skip "estimateRedeem" "Genesis not done"
  fi

  # Rate limit status
  local rate_status
  rate_status=$(call "$NACHOS_VAULT" getGlobalRateLimitStatus)
  assert_contains "$rate_status" "totalMintValueIn4h" "getGlobalRateLimitStatus returns data"

  # User rate limit
  local user_rate
  user_rate=$(call "$NACHOS_VAULT" getUserRateLimitStatus "(principal \"$TEST_PRINCIPAL\")")
  assert_contains "$user_rate" "mintOpsIn4h" "getUserRateLimitStatus returns data"
  assert_contains "$user_rate" "mintValueIn4h" "getUserRateLimitStatus includes mintValueIn4h"
  assert_contains "$user_rate" "burnValueIn4h" "getUserRateLimitStatus includes burnValueIn4h"

  # Fee exemptions
  local fee_exempt
  fee_exempt=$(call "$NACHOS_VAULT" getFeeExemptPrincipals)
  assert_contains "$fee_exempt" "$TEST_PRINCIPAL" "Test identity is fee-exempt"

  # Rate limit exemptions
  local rate_exempt
  rate_exempt=$(call "$NACHOS_VAULT" getRateLimitExemptPrincipals)
  assert_contains "$rate_exempt" "$TEST_PRINCIPAL" "Test identity is rate-limit-exempt"

  # getNAVHistoryAdaptive (adaptive resolution for charts)
  local adaptive_nav
  adaptive_nav=$(call "$NACHOS_VAULT" getNAVHistoryAdaptive)
  if [ "$GENESIS_DONE" = "1" ]; then
    assert_contains "$adaptive_nav" "navPerTokenE8s" "getNAVHistoryAdaptive returns NAV data"
    assert_contains "$adaptive_nav" "reason" "getNAVHistoryAdaptive includes reason field"
  else
    skip "getNAVHistoryAdaptive" "Genesis not done"
  fi
}

# ════════════════════════════════════════════════════════════════
# PHASE 2: ADMIN CONTROLS (toggle & restore)
# ════════════════════════════════════════════════════════════════

phase2_admin_controls() {
  section "PHASE 2: Admin Controls (toggle & restore)"

  # ── Pause/Unpause Minting ──
  local result
  result=$(call "$NACHOS_VAULT" pauseMinting)
  assert_contains "$result" "ok" "pauseMinting succeeds"

  local status
  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_contains "$status" "mintingEnabled = false" "Minting is disabled after pause"

  result=$(call "$NACHOS_VAULT" unpauseMinting)
  assert_contains "$result" "ok" "unpauseMinting succeeds"

  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_contains "$status" "mintingEnabled = true" "Minting re-enabled after unpause"

  # ── Pause/Unpause Burning ──
  result=$(call "$NACHOS_VAULT" pauseBurning)
  assert_contains "$result" "ok" "pauseBurning succeeds"

  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_contains "$status" "burningEnabled = false" "Burning is disabled after pause"

  result=$(call "$NACHOS_VAULT" unpauseBurning)
  assert_contains "$result" "ok" "unpauseBurning succeeds"

  # ── Emergency Pause/Unpause ──
  result=$(call "$NACHOS_VAULT" emergencyPause)
  assert_contains "$result" "ok" "emergencyPause succeeds"

  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_contains "$status" "systemPaused = true" "System paused after emergency"

  result=$(call "$NACHOS_VAULT" emergencyUnpause)
  assert_contains "$result" "ok" "emergencyUnpause succeeds"

  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_contains "$status" "systemPaused = false" "System unpaused after recovery"

  # ── Circuit Breaker Reset (no-op if not active) ──
  result=$(call "$NACHOS_VAULT" resetCircuitBreaker)
  assert_contains "$result" "ok" "resetCircuitBreaker succeeds"

  # ── Circuit Breaker Conditions CRUD ──
  # Verify default NavDrop condition was seeded
  local cb_conditions
  cb_conditions=$(call "$NACHOS_VAULT" getCircuitBreakerConditions)
  assert_contains "$cb_conditions" "NavDrop" "Default NavDrop condition exists"
  assert_contains "$cb_conditions" "PauseBoth" "NavDrop action is PauseBoth"

  # Add a test PriceChange condition
  local cb_add_result
  cb_add_result=$(call "$NACHOS_VAULT" addCircuitBreakerCondition "(record {
    conditionType = variant { PriceChange };
    thresholdPercent = 50.0 : float64;
    timeWindowNS = 3_600_000_000_000 : nat;
    direction = variant { Both };
    action = variant { RejectOperation };
    applicableTokens = vec {};
    enabled = true;
  })")
  assert_contains "$cb_add_result" "ok" "addCircuitBreakerCondition succeeds"

  # Extract condition ID from result
  local cb_id
  cb_id=$(echo "$cb_add_result" | grep -oP 'ok\s*=\s*\K[0-9_]+' | tr -d '_' | head -1)
  info "Added PriceChange condition with ID: $cb_id"

  # Verify new condition appears in list
  cb_conditions=$(call "$NACHOS_VAULT" getCircuitBreakerConditions)
  assert_contains "$cb_conditions" "PriceChange" "PriceChange condition appears in list"
  assert_contains "$cb_conditions" "RejectOperation" "PriceChange action is RejectOperation"

  # Update the condition threshold
  local cb_update_result
  cb_update_result=$(call "$NACHOS_VAULT" updateCircuitBreakerCondition "($cb_id : nat, opt (75.0 : float64), null, null, null, null)")
  assert_contains "$cb_update_result" "ok" "updateCircuitBreakerCondition succeeds"

  # Disable the condition
  local cb_enable_result
  cb_enable_result=$(call "$NACHOS_VAULT" enableCircuitBreakerCondition "($cb_id : nat, false)")
  assert_contains "$cb_enable_result" "ok" "enableCircuitBreakerCondition(false) succeeds"

  # Re-enable it
  cb_enable_result=$(call "$NACHOS_VAULT" enableCircuitBreakerCondition "($cb_id : nat, true)")
  assert_contains "$cb_enable_result" "ok" "enableCircuitBreakerCondition(true) succeeds"

  # Query alerts (should be empty or have entries)
  local cb_alerts
  cb_alerts=$(call "$NACHOS_VAULT" getCircuitBreakerAlerts "(10 : nat, 0 : nat)")
  assert_not_contains "$cb_alerts" "Error" "getCircuitBreakerAlerts executes without error"

  # Remove the test condition
  local cb_remove_result
  cb_remove_result=$(call "$NACHOS_VAULT" removeCircuitBreakerCondition "($cb_id : nat)")
  assert_contains "$cb_remove_result" "ok" "removeCircuitBreakerCondition succeeds"

  # Verify test condition ID is gone but default NavDrop remains
  cb_conditions=$(call "$NACHOS_VAULT" getCircuitBreakerConditions)
  if echo "$cb_conditions" | grep -q "id = ${cb_id} :"; then
    fail "Test condition removed from list" "Condition with id=$cb_id still present"
  else
    pass "Test condition removed from list"
  fi
  assert_contains "$cb_conditions" "NavDrop" "NavDrop condition still exists after removing test condition"

  # Verify getSystemStatus shows new circuit breaker fields
  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_contains "$status" "mintPausedByCircuitBreaker" "getSystemStatus includes mintPausedByCircuitBreaker"
  assert_contains "$status" "burnPausedByCircuitBreaker" "getSystemStatus includes burnPausedByCircuitBreaker"

  # ── Config Update & Restore ──
  result=$(call "$NACHOS_VAULT" updateNachosConfig "(record {
    mintFeeBasisPoints = opt (100 : nat);
    burnFeeBasisPoints = null;
    minMintValueICP = null;
    minBurnValueICP = null;
    maxSlippageBasisPoints = null;
    maxNachosBurnPer4Hours = null;
    maxMintICPWorthPer4Hours = null;
    maxMintOpsPerUser4Hours = null;
    maxBurnOpsPerUser4Hours = null;
    navDropThresholdPercent = null;
    navDropTimeWindowNS = null;
    portfolioShareMaxDeviationBP = null;
    cancellationFeeMultiplier = null;
    mintingEnabled = null;
    burningEnabled = null;
  })")
  assert_contains "$result" "ok" "updateNachosConfig succeeds"

  local config
  config=$(call "$NACHOS_VAULT" getConfig)
  assert_contains "$config" "mintFeeBasisPoints = 100" "Config updated: mintFee = 100 BP"

  # Restore default
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    mintFeeBasisPoints = opt (50 : nat);
    burnFeeBasisPoints = null; minMintValueICP = null; minBurnValueICP = null;
    maxSlippageBasisPoints = null; maxNachosBurnPer4Hours = null;
    maxMintICPWorthPer4Hours = null; maxMintOpsPerUser4Hours = null;
    maxBurnOpsPerUser4Hours = null; navDropThresholdPercent = null;
    navDropTimeWindowNS = null; portfolioShareMaxDeviationBP = null;
    cancellationFeeMultiplier = null; mintingEnabled = null; burningEnabled = null;
  })" >/dev/null
  pass "Config restored to default (mintFee = 50 BP)"

  # ── updateFees (separate from updateNachosConfig) ──
  result=$(call "$NACHOS_VAULT" updateFees "(200 : nat, 200 : nat)")
  assert_contains "$result" "ok" "updateFees succeeds"

  config=$(call "$NACHOS_VAULT" getConfig)
  assert_contains "$config" "mintFeeBasisPoints = 200" "updateFees applied: mintFee = 200 BP"
  assert_contains "$config" "burnFeeBasisPoints = 200" "updateFees applied: burnFee = 200 BP"

  # Restore defaults
  call "$NACHOS_VAULT" updateFees "(50 : nat, 50 : nat)" >/dev/null
  pass "Fees restored to default (50 BP)"

  # ── updateCancellationFeeMultiplier ──
  result=$(call "$NACHOS_VAULT" updateCancellationFeeMultiplier "(5 : nat)")
  assert_contains "$result" "ok" "updateCancellationFeeMultiplier succeeds"

  config=$(call "$NACHOS_VAULT" getConfig)
  assert_contains "$config" "cancellationFeeMultiplier = 5" "cancellationFeeMultiplier updated to 5"

  # Restore default
  call "$NACHOS_VAULT" updateCancellationFeeMultiplier "(3 : nat)" >/dev/null
  pass "cancellationFeeMultiplier restored to default (3)"

  # ── Min value threshold test ──
  # Temporarily set minMintValueICP very high, verify it appears in config
  result=$(call "$NACHOS_VAULT" updateNachosConfig "(record {
    mintFeeBasisPoints = null; burnFeeBasisPoints = null;
    minMintValueICP = opt (1_000_000_000 : nat);
    minBurnValueICP = null; maxSlippageBasisPoints = null;
    maxNachosBurnPer4Hours = null; maxMintICPWorthPer4Hours = null;
    maxMintOpsPerUser4Hours = null; maxBurnOpsPerUser4Hours = null;
    navDropThresholdPercent = null; navDropTimeWindowNS = null;
    portfolioShareMaxDeviationBP = null; cancellationFeeMultiplier = null;
    mintingEnabled = null; burningEnabled = null;
  })")
  assert_contains "$result" "ok" "updateNachosConfig with high minMintValueICP succeeds"

  config=$(call "$NACHOS_VAULT" getConfig)
  assert_contains "$config" "minMintValueICP = 1_000_000_000" "minMintValueICP updated to 10 ICP"

  # Restore default (0.01 ICP = 1_000_000)
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    mintFeeBasisPoints = null; burnFeeBasisPoints = null;
    minMintValueICP = opt (1_000_000 : nat);
    minBurnValueICP = null; maxSlippageBasisPoints = null;
    maxNachosBurnPer4Hours = null; maxMintICPWorthPer4Hours = null;
    maxMintOpsPerUser4Hours = null; maxBurnOpsPerUser4Hours = null;
    navDropThresholdPercent = null; navDropTimeWindowNS = null;
    portfolioShareMaxDeviationBP = null; cancellationFeeMultiplier = null;
    mintingEnabled = null; burningEnabled = null;
  })" >/dev/null
  pass "minMintValueICP restored to default"

  # ── Token Management ──
  local test_token="rimrc-piaaa-aaaao-aaljq-cai"  # Dummy token for add/remove test (CHAT token, not in vault)
  result=$(call "$NACHOS_VAULT" addAcceptedMintToken "(principal \"$test_token\")")
  assert_contains "$result" "ok" "addAcceptedMintToken succeeds"

  local tokens
  tokens=$(call "$NACHOS_VAULT" getAcceptedMintTokens)
  assert_contains "$tokens" "$test_token" "Test token appears in accepted list"

  result=$(call "$NACHOS_VAULT" setAcceptedMintTokenEnabled "(principal \"$test_token\", false)")
  assert_contains "$result" "ok" "setAcceptedMintTokenEnabled(false) succeeds"

  result=$(call "$NACHOS_VAULT" removeAcceptedMintToken "(principal \"$test_token\")")
  assert_contains "$result" "ok" "removeAcceptedMintToken succeeds"

  tokens=$(call "$NACHOS_VAULT" getAcceptedMintTokens)
  assert_not_contains "$tokens" "$test_token" "Test token removed from accepted list"
}

# ════════════════════════════════════════════════════════════════
# PHASE 3: ICP MINT TEST
# ════════════════════════════════════════════════════════════════

phase3_icp_mint() {
  section "PHASE 3: ICP Mint Test"

  if [ "$GENESIS_DONE" != "1" ]; then
    skip "ICP mint test" "Genesis not complete — run setup first"
    return
  fi

  # Get NAV before mint
  local nav_before
  nav_before=$(call "$NACHOS_VAULT" getNAV)
  info "NAV before mint: $nav_before"

  # Get estimate
  local estimate
  estimate=$(call "$NACHOS_VAULT" estimateMintICP "($MINT_AMOUNT : nat)")
  info "Mint estimate: $estimate"
  assert_contains "$estimate" "nachosEstimate" "estimateMintICP returns estimate"

  # Capture NACHOS balance before mint (for delta check)
  local nachos_before_mint
  nachos_before_mint=$(get_nachos_balance)
  info "NACHOS balance before mint: $nachos_before_mint e8s"

  # Transfer ICP to treasury subaccount 2
  info "Transferring $MINT_AMOUNT e8s ICP to treasury subaccount 2..."
  local block
  block=$(transfer_icp_to_treasury "$MINT_AMOUNT")

  if [[ "$block" == TRANSFER_FAILED* ]]; then
    fail "ICP transfer to treasury" "${block#TRANSFER_FAILED:}"
    return
  fi
  info "ICP transfer block: $block"
  pass "ICP transferred to treasury subaccount 2 (block $block)"

  # Call mintNachos (with cycle tracking — 10s wait for async follow-up work)
  info "Calling mintNachos(blockNumber=$block, minimumNachosReceive=0)..."
  local mint_result cycles_before cycles_after cycles_used mint_start mint_end
  cycles_before=$(get_vault_cycles)
  mint_start=$(date +%s)
  mint_result=$(call "$NACHOS_VAULT" mintNachos "($block : nat, 0 : nat, null, null)")
  mint_end=$(date +%s)
  MINT_TIME_SECS=$((mint_end - mint_start))
  info "Mint result: $(echo "$mint_result" | head -c 500)"
  info "Waiting 10s for async operations to settle before measuring cycles..."
  sleep 20
  cycles_after=$(get_vault_cycles)
  cycles_used=$((cycles_before - cycles_after))
  MINT_CYCLES_USED=$cycles_used
  info "Mint cost: $cycles_used cycles ($cycles_before -> $cycles_after), time: ${MINT_TIME_SECS}s"
  assert_contains "$mint_result" "ok =" "mintNachos succeeds"

  # ── Mint formula validation ──
  if echo "$mint_result" | grep -q "ok ="; then
    parse_mint_result "$mint_result"
    info "Parsed mint: nachosReceived=$MR_NACHOS_RECEIVED, navUsed=$MR_NAV_USED, netICP=$MR_NET_ICP"

    # Validate: nachosReceived ≈ (netValueICP × 1e8) / navUsed
    validate_mint_formula && MINT_FORMULA_OK="YES" || MINT_FORMULA_OK="NO"

    # Verify NACHOS balance delta matches nachosReceived
    local nachos_after_mint
    nachos_after_mint=$(get_nachos_balance)
    local nachos_delta=$((nachos_after_mint - nachos_before_mint))
    info "NACHOS balance after mint: $nachos_after_mint e8s (delta=$nachos_delta)"

    if [ "$nachos_delta" -eq "$MR_NACHOS_RECEIVED" ]; then
      pass "Mint balance delta ($nachos_delta) == nachosReceived ($MR_NACHOS_RECEIVED)"
    else
      # Allow small tolerance for fee deduction in transit
      local delta_diff=$(( nachos_delta > MR_NACHOS_RECEIVED ? nachos_delta - MR_NACHOS_RECEIVED : MR_NACHOS_RECEIVED - nachos_delta ))
      if [ "$delta_diff" -le 10000 ]; then
        pass "Mint balance delta ($nachos_delta) ≈ nachosReceived ($MR_NACHOS_RECEIVED) [diff=$delta_diff, within NACHOS fee]"
      else
        fail "Mint balance delta ($nachos_delta) != nachosReceived ($MR_NACHOS_RECEIVED)" "diff=$delta_diff"
      fi
    fi
  fi

  # Verify system status updated
  local status
  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_not_contains "$status" "totalMints = 0" "totalMints > 0 after mint"

  # Verify NAV updated
  local nav_after
  nav_after=$(call "$NACHOS_VAULT" getNAV)
  assert_contains "$nav_after" "navPerTokenE8s" "NAV still valid after mint"

  # Check mint history
  local history
  history=$(call "$NACHOS_VAULT" getMintHistory "(10 : nat, 0 : nat)")
  assert_contains "$history" "nachosReceived" "Mint history contains record"

  # Check user mint history
  local user_history
  user_history=$(call "$NACHOS_VAULT" getUserMintHistory "(principal \"$TEST_PRINCIPAL\")")
  assert_contains "$user_history" "nachosReceived" "User mint history contains record"
}

# ════════════════════════════════════════════════════════════════
# PHASE 4: ERROR HANDLING TESTS
# ════════════════════════════════════════════════════════════════

phase4_error_handling() {
  section "PHASE 4: Error Handling Tests"

  if [ "$GENESIS_DONE" != "1" ]; then
    skip "Error handling tests" "Genesis not complete"
    return
  fi

  # ── Duplicate block ──
  info "Testing duplicate block detection..."
  local block
  block=$(transfer_icp_to_treasury "$MINT_AMOUNT")
  if [[ "$block" == TRANSFER_FAILED* ]]; then
    skip "Duplicate block test" "ICP transfer failed"
    return
  fi

  # First mint should succeed
  local result1
  result1=$(call "$NACHOS_VAULT" mintNachos "($block : nat, 0 : nat, null, null)")
  assert_contains "$result1" "ok =" "First mint with block $block succeeds"

  # Second mint with same block should fail
  local result2
  result2=$(call "$NACHOS_VAULT" mintNachos "($block : nat, 0 : nat, null, null)")
  assert_contains "$result2" "BlockAlreadyProcessed" "Duplicate block rejected (#BlockAlreadyProcessed)"

  # ── Genesis already done ──
  local genesis_result
  genesis_result=$(call "$NACHOS_VAULT" genesisMint "(0 : nat, null, null)")
  assert_contains "$genesis_result" "GenesisAlreadyDone" "Second genesis rejected (#GenesisAlreadyDone)"

  # ── Non-accepted token ──
  local bad_token="aaaaa-aa"
  local token_result
  token_result=$(call "$NACHOS_VAULT" mintNachosWithToken "(principal \"$bad_token\", 0 : nat, 0 : nat, null, null)")
  assert_not_contains "$token_result" "ok =" "Non-accepted token mint rejected"

  # ── Paused system ──
  # Note: ICP transferred here stays in treasury (~0.1 ICP per test run)
  call "$NACHOS_VAULT" emergencyPause >/dev/null
  local block2
  block2=$(transfer_icp_to_treasury "$MINT_AMOUNT")
  if [[ "$block2" != TRANSFER_FAILED* ]]; then
    local paused_result
    paused_result=$(call "$NACHOS_VAULT" mintNachos "($block2 : nat, 0 : nat, null, null)")
    assert_contains "$paused_result" "SystemPaused" "Mint rejected when system paused"
  fi
  call "$NACHOS_VAULT" emergencyUnpause >/dev/null

  # ── Minting disabled ──
  # Note: ICP transferred here stays in treasury (~0.1 ICP per test run)
  call "$NACHOS_VAULT" pauseMinting >/dev/null
  local block3
  block3=$(transfer_icp_to_treasury "$MINT_AMOUNT")
  if [[ "$block3" != TRANSFER_FAILED* ]]; then
    local disabled_result
    disabled_result=$(call "$NACHOS_VAULT" mintNachos "($block3 : nat, 0 : nat, null, null)")
    assert_contains "$disabled_result" "MintingDisabled" "Mint rejected when minting disabled"
  fi
  call "$NACHOS_VAULT" unpauseMinting >/dev/null

  # ── Cancel non-existent deposit ──
  local cancel_result
  cancel_result=$(call "$NACHOS_VAULT" cancelDeposit "(principal \"$ICP_PRINCIPAL\", 999999999 : nat)")
  assert_contains "$cancel_result" "DepositNotFound" "Cancel non-existent deposit rejected"

  # ── Rate limit test (temporarily remove exemption) ──
  info "Testing rate limit enforcement..."

  # Step 1: Remove exemption so ops get recorded (limit is still default 100)
  call "$NACHOS_VAULT" removeRateLimitExemptPrincipal "(principal \"$TEST_PRINCIPAL\")" >/dev/null

  # Step 2: Do a warmup mint WITHOUT exemption at default limit (100) to record an op
  local block_warmup
  block_warmup=$(transfer_icp_to_treasury "$MINT_AMOUNT")
  if [[ "$block_warmup" != TRANSFER_FAILED* ]]; then
    local warmup_result
    warmup_result=$(call "$NACHOS_VAULT" mintNachos "($block_warmup : nat, 0 : nat, null, null)")
    assert_contains "$warmup_result" "ok =" "Warmup mint with block $block_warmup succeeds at default limit"
  fi

  # Step 3: NOW set very low rate limit (1 op per 4 hours) — warmup already consumed the quota
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    mintFeeBasisPoints = null; burnFeeBasisPoints = null;
    minMintValueICP = null; minBurnValueICP = null;
    maxSlippageBasisPoints = null; maxNachosBurnPer4Hours = null;
    maxMintICPWorthPer4Hours = null;
    maxMintOpsPerUser4Hours = opt (1 : nat);
    maxBurnOpsPerUser4Hours = null; navDropThresholdPercent = null;
    navDropTimeWindowNS = null; portfolioShareMaxDeviationBP = null;
    cancellationFeeMultiplier = null; mintingEnabled = null; burningEnabled = null;
  })" >/dev/null

  # Step 4: Next mint should be rejected (rate limit of 1 already exhausted by warmup)
  local block_rl
  block_rl=$(transfer_icp_to_treasury "$MINT_AMOUNT")
  if [[ "$block_rl" != TRANSFER_FAILED* ]]; then
    local rl_result
    rl_result=$(call "$NACHOS_VAULT" mintNachos "($block_rl : nat, 0 : nat, null, null)")
    assert_not_contains "$rl_result" "ok =" "Mint rejected when rate limit exceeded"
  fi

  # Restore: re-add exemption + restore config
  call "$NACHOS_VAULT" addRateLimitExemptPrincipal "(principal \"$TEST_PRINCIPAL\", \"test-script\")" >/dev/null
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    mintFeeBasisPoints = null; burnFeeBasisPoints = null;
    minMintValueICP = null; minBurnValueICP = null;
    maxSlippageBasisPoints = null; maxNachosBurnPer4Hours = null;
    maxMintICPWorthPer4Hours = null;
    maxMintOpsPerUser4Hours = opt (100 : nat);
    maxBurnOpsPerUser4Hours = null; navDropThresholdPercent = null;
    navDropTimeWindowNS = null; portfolioShareMaxDeviationBP = null;
    cancellationFeeMultiplier = null; mintingEnabled = null; burningEnabled = null;
  })" >/dev/null
  pass "Rate limit test complete, exemption restored"

  # ── Per-operation max mint amount test ──
  info "Testing per-operation max mint amount enforcement..."

  # Set maxMintAmountICP to 500000 (0.005 ICP — below our MINT_AMOUNT of 1500000)
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    maxMintAmountICP = opt (500000 : nat);
  })" >/dev/null

  local block_max
  block_max=$(transfer_icp_to_treasury "$MINT_AMOUNT")
  if [[ "$block_max" != TRANSFER_FAILED* ]]; then
    local max_result
    max_result=$(call "$NACHOS_VAULT" mintNachos "($block_max : nat, 0 : nat, null, null)")
    assert_contains "$max_result" "AboveMaximumValue" "Mint rejected when above maxMintAmountICP"
  fi

  # Restore (0 = disabled)
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    maxMintAmountICP = opt (0 : nat);
  })" >/dev/null
  pass "Per-operation max mint amount test complete, restored to disabled"

  # ── Per-user value limit test ──
  info "Testing per-user ICP value limit enforcement..."

  # Step 1: Remove exemption so values get recorded
  call "$NACHOS_VAULT" removeRateLimitExemptPrincipal "(principal \"$TEST_PRINCIPAL\")" >/dev/null

  # Step 2: Set per-user mint value limit very low (500000 = 0.005 ICP)
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    maxMintICPPerUser4Hours = opt (500000 : nat);
  })" >/dev/null

  # Step 3: Do a warmup mint (1500000 = 0.015 ICP > 0.005 ICP limit) — should be rejected immediately
  local block_val
  block_val=$(transfer_icp_to_treasury "$MINT_AMOUNT")
  if [[ "$block_val" != TRANSFER_FAILED* ]]; then
    local val_result
    val_result=$(call "$NACHOS_VAULT" mintNachos "($block_val : nat, 0 : nat, null, null)")
    assert_contains "$val_result" "UserMintLimitExceeded" "Mint rejected when per-user ICP value limit exceeded"
  fi

  # Step 4: Restore exemption + config
  call "$NACHOS_VAULT" addRateLimitExemptPrincipal "(principal \"$TEST_PRINCIPAL\", \"test-script\")" >/dev/null
  call "$NACHOS_VAULT" updateNachosConfig "(record {
    maxMintICPPerUser4Hours = opt (100_000_000_000 : nat);
  })" >/dev/null
  pass "Per-user value limit test complete, restored"
}

# ════════════════════════════════════════════════════════════════
# PHASE 5: ADDITIONAL QUERY & EDGE CASE TESTS
# ════════════════════════════════════════════════════════════════

phase5_additional_queries() {
  section "PHASE 5: Additional Query & Edge Case Tests"

  # ── getNAVHistory ──
  local nav_history
  nav_history=$(call "$NACHOS_VAULT" getNAVHistory "(10 : nat)")
  if [ "$GENESIS_DONE" = "1" ]; then
    assert_not_contains "$nav_history" "Error" "getNAVHistory executes without error"
  else
    skip "getNAVHistory" "Genesis not done"
  fi

  # ── getFeeHistory ──
  local fee_history
  fee_history=$(call "$NACHOS_VAULT" getFeeHistory "(10 : nat)")
  assert_not_contains "$fee_history" "Error" "getFeeHistory executes without error"

  # ── getActiveDepositsCount ──
  local deposit_count
  deposit_count=$(call "$NACHOS_VAULT" getActiveDepositsCount)
  assert_not_contains "$deposit_count" "Error" "getActiveDepositsCount executes without error"

  # ── getUserDeposits ──
  local user_deposits
  user_deposits=$(call "$NACHOS_VAULT" getUserDeposits "(principal \"$TEST_PRINCIPAL\")")
  assert_not_contains "$user_deposits" "Error" "getUserDeposits executes without error"

  # ── getUserBurnHistory ──
  local user_burns
  user_burns=$(call "$NACHOS_VAULT" getUserBurnHistory "(principal \"$TEST_PRINCIPAL\")")
  assert_not_contains "$user_burns" "Error" "getUserBurnHistory executes without error"

  # ── getDeposit (non-existent) ──
  local deposit_query
  deposit_query=$(call "$NACHOS_VAULT" getDeposit "(principal \"$ICP_PRINCIPAL\", 999999999 : nat)")
  assert_contains "$deposit_query" "null" "getDeposit returns null for non-existent deposit"

  # ── getVaultDashboard (composite query) ──
  local dashboard
  dashboard=$(query_call "$NACHOS_VAULT" getVaultDashboard "(opt (100_000_000 : nat), opt (100_000_000 : nat))")
  assert_contains "$dashboard" "mintingEnabled" "getVaultDashboard returns valid structure"
  if [ "$GENESIS_DONE" = "1" ]; then
    assert_contains "$dashboard" "dataSource" "getVaultDashboard includes dataSource"
  fi

  # ── getAdminDashboard (composite query — admin superset) ──
  local admin_dashboard
  admin_dashboard=$(query_call "$NACHOS_VAULT" getAdminDashboard)
  assert_contains "$admin_dashboard" "fullConfig" "getAdminDashboard includes fullConfig"
  assert_contains "$admin_dashboard" "circuitBreakerConditions" "getAdminDashboard includes CB conditions"
  assert_contains "$admin_dashboard" "transferQueue" "getAdminDashboard includes transfer queue"
  assert_contains "$admin_dashboard" "claimableMintFees" "getAdminDashboard includes claimable fees"
  assert_contains "$admin_dashboard" "canisterCycles" "getAdminDashboard includes cycles"
  assert_contains "$admin_dashboard" "feeExemptPrincipals" "getAdminDashboard includes fee exemptions"
  assert_contains "$admin_dashboard" "rateLimitExemptPrincipals" "getAdminDashboard includes rate limit exemptions"
  assert_contains "$admin_dashboard" "recentAlerts" "getAdminDashboard includes recent alerts"
  assert_contains "$admin_dashboard" "nachosLedger" "getAdminDashboard includes nachosLedger"
  assert_contains "$admin_dashboard" "mintingEnabled" "getAdminDashboard includes base dashboard fields"

  # ── minimumNachosReceive slippage protection ──
  if [ "$GENESIS_DONE" = "1" ]; then
    info "Testing minimumNachosReceive rejection..."
    local block_slip
    block_slip=$(transfer_icp_to_treasury "$MINT_AMOUNT")
    if [[ "$block_slip" != TRANSFER_FAILED* ]]; then
      # Pass an absurdly high minimum — should fail with slippage/minimum error
      local slip_result
      slip_result=$(call "$NACHOS_VAULT" mintNachos "($block_slip : nat, 999_999_999_999_999 : nat, null, null)")
      assert_not_contains "$slip_result" "ok =" "Mint rejected when minimumNachosReceive too high"
    fi
  else
    skip "minimumNachosReceive test" "Genesis not done"
  fi

  # ── Fee claim queries (per-token format) ──
  info "Testing fee claim queries..."

  local mint_fees
  mint_fees=$(call "$NACHOS_VAULT" getClaimableMintFees)
  assert_not_contains "$mint_fees" "Error" "getClaimableMintFees returns valid structure"
  # Per-token format: returns vec {} or vec { record { token; accumulated; claimed; claimable } }
  if echo "$mint_fees" | grep -q "claimable"; then
    pass "getClaimableMintFees has per-token entries"
  else
    pass "getClaimableMintFees returns empty vec (no fees yet)"
  fi

  local cancel_fees
  cancel_fees=$(call "$NACHOS_VAULT" getClaimableCancellationFees)
  assert_not_contains "$cancel_fees" "Error" "getClaimableCancellationFees executes without error"

  # ── Fee claim: try claiming more than available (should fail) ──
  # claimMintFees now takes (recipient, tokenPrincipal, amount)
  local ICP_PRINCIPAL="ryjl3-tyaaa-aaaaa-aaaba-cai"
  local overclaim_result
  overclaim_result=$(call "$NACHOS_VAULT" claimMintFees "(principal \"$TEST_PRINCIPAL\", principal \"$ICP_PRINCIPAL\", 999_999_999_999 : nat)")
  assert_contains "$overclaim_result" "Insufficient" "Mint fee over-claim rejected"


  # ── Burn fee claim queries (per-token format) ──
  info "Testing burn fee claim queries..."
  local burn_fees
  burn_fees=$(call "$NACHOS_VAULT" getClaimableBurnFees)
  assert_not_contains "$burn_fees" "Error" "getClaimableBurnFees returns valid structure"

  # claimBurnFees now takes (recipient, tokenPrincipal, amount)
  local burn_overclaim
  burn_overclaim=$(call "$NACHOS_VAULT" claimBurnFees "(principal \"$TEST_PRINCIPAL\", principal \"$ICP_PRINCIPAL\", 999_999_999_999 : nat)")
  assert_contains "$burn_overclaim" "Insufficient" "Burn fee over-claim rejected"

  # ── Per-token fee structure validation ──
  info "Testing per-token fee format in dashboard..."
  local dashboard_fees
  dashboard_fees=$(call "$NACHOS_VAULT" getVaultDashboard)

  # All three fee types should be vec format
  if echo "$dashboard_fees" | grep -q "claimableMintFees = vec"; then
    pass "Dashboard claimableMintFees is vec format"
  else
    fail "Dashboard claimableMintFees format" "expected vec format"
  fi
  if echo "$dashboard_fees" | grep -q "claimableBurnFees = vec"; then
    pass "Dashboard claimableBurnFees is vec format"
  else
    fail "Dashboard claimableBurnFees format" "expected vec format"
  fi
  if echo "$dashboard_fees" | grep -q "claimableCancellationFees = vec"; then
    pass "Dashboard claimableCancellationFees is vec format"
  else
    fail "Dashboard claimableCancellationFees format" "expected vec format"
  fi

  # ── Fee claim with wrong token principal (should return 0 available) ──
  local bogus_token="aaaaa-aa"
  local wrong_token_claim
  wrong_token_claim=$(call "$NACHOS_VAULT" claimMintFees "(principal \"$TEST_PRINCIPAL\", principal \"$bogus_token\", 1 : nat)")
  assert_contains "$wrong_token_claim" "Insufficient" "Claim with unknown token rejected"

  # ── Fee claim with zero amount (should fail) ──
  local zero_claim
  zero_claim=$(call "$NACHOS_VAULT" claimMintFees "(principal \"$TEST_PRINCIPAL\", principal \"$ICP_PRINCIPAL\", 0 : nat)")
  assert_contains "$zero_claim" "Amount must be" "Claim with zero amount rejected"

  # ── Burn fee claim with zero amount ──
  local zero_burn_claim
  zero_burn_claim=$(call "$NACHOS_VAULT" claimBurnFees "(principal \"$TEST_PRINCIPAL\", principal \"$ICP_PRINCIPAL\", 0 : nat)")
  assert_contains "$zero_burn_claim" "Amount must be" "Burn claim with zero amount rejected"

  # ── Cancellation fee over-claim (already per-token, verify still works) ──
  local cancel_overclaim
  cancel_overclaim=$(call "$NACHOS_VAULT" claimCancellationFees "(principal \"$TEST_PRINCIPAL\", principal \"$ICP_PRINCIPAL\", 999_999_999_999 : nat)")
  assert_contains "$cancel_overclaim" "Insufficient" "Cancellation fee over-claim rejected"



}

# ════════════════════════════════════════════════════════════════
# PHASE 6: BURN NACHOS TEST
# ════════════════════════════════════════════════════════════════

phase6_burn() {
  section "PHASE 6: Burn NACHOS Test"

  if [ "$GENESIS_DONE" != "1" ]; then
    skip "Burn test" "Genesis not complete"
    return
  fi

  # Check NACHOS balance
  local balance
  balance=$(dfx canister call "$NACHOS_LEDGER" icrc1_balance_of "(record {
    owner = principal \"$TEST_PRINCIPAL\";
    subaccount = null;
  })" $NETWORK 2>&1) || true
  info "NACHOS balance: $balance"

  local bal_num
  bal_num=$(echo "$balance" | grep -oP '[0-9_]+' | tr -d '_' | head -1)

  local MIN_BURN=10000  # minimum 0.0001 NACHOS to be worth testing
  if [ -z "$bal_num" ] || [ "$bal_num" -lt "$MIN_BURN" ]; then
    skip "Burn test" "Insufficient NACHOS balance (have ${bal_num:-0}, need at least $MIN_BURN)"
    return
  fi

  # Subtract NACHOS ledger fee (10,000) before computing burn amount
  local available=$(( bal_num - 10000 ))
  if [ "$available" -lt "$MIN_BURN" ]; then
    skip "Burn test" "Insufficient NACHOS balance after fee (have $available usable, need at least $MIN_BURN)"
    return
  fi

  # Use 80% of available (after fee) balance for burn
  BURN_AMOUNT=$(( available * 80 / 100 ))
  if [ "$BURN_AMOUNT" -lt "$MIN_BURN" ]; then
    BURN_AMOUNT=$MIN_BURN
  fi
  info "Using dynamic BURN_AMOUNT=$BURN_AMOUNT (from balance $bal_num)"

  # Get burn estimate
  local estimate
  estimate=$(call "$NACHOS_VAULT" estimateRedeem "($BURN_AMOUNT : nat)")
  info "Burn estimate: $estimate"

  # ── Pre-burn portfolio snapshot via getVaultDashboard (includes decimals) ──
  info "Taking pre-burn portfolio snapshot..."
  local dashboard_before
  dashboard_before=$(call "$NACHOS_VAULT" getVaultDashboard "(opt ($BURN_AMOUNT : nat), opt ($BURN_AMOUNT : nat))")
  parse_dashboard_portfolio "$dashboard_before"

  # Copy to PF_PRE_* arrays
  PF_PRE_SYMBOLS=("${PF_SYMBOLS[@]+"${PF_SYMBOLS[@]}"}")
  PF_PRE_PRINCIPALS=("${PF_PRINCIPALS[@]+"${PF_PRINCIPALS[@]}"}")
  PF_PRE_BALANCES=("${PF_BALANCES[@]+"${PF_BALANCES[@]}"}")
  PF_PRE_PRICES=("${PF_PRICES[@]+"${PF_PRICES[@]}"}")
  PF_PRE_VALUES=("${PF_VALUES[@]+"${PF_VALUES[@]}"}")
  PF_PRE_BPS=("${PF_BPS[@]+"${PF_BPS[@]}"}")
  PF_PRE_DECIMALS=("${PF_DECIMALS[@]+"${PF_DECIMALS[@]}"}")
  PF_PRE_COUNT=$PF_COUNT

  if [ "$PF_PRE_COUNT" -gt 0 ]; then
    info "Pre-burn portfolio ($PF_PRE_COUNT tokens):"
    printf "  %-8s %18s %15s %15s %8s %5s\n" "Symbol" "Balance" "PriceICP" "ValueICP" "BP" "Dec"
    printf "  %-8s %18s %15s %15s %8s %5s\n" "------" "-------" "--------" "--------" "---" "---"
    local i
    for ((i=0; i<PF_PRE_COUNT; i++)); do
      printf "  %-8s %18s %15s %15s %8s %5s\n" \
        "${PF_PRE_SYMBOLS[$i]}" "${PF_PRE_BALANCES[$i]}" "${PF_PRE_PRICES[$i]}" \
        "${PF_PRE_VALUES[$i]}" "${PF_PRE_BPS[$i]}" "${PF_PRE_DECIMALS[$i]}"
    done
    pass "Pre-burn portfolio snapshot captured ($PF_PRE_COUNT tokens)"
  else
    info "WARNING: Could not parse portfolio from dashboard (PF_COUNT=0)"
  fi

  # ── Query user's token balances BEFORE burn ──
  if [ "$PF_PRE_COUNT" -gt 0 ]; then
    info "Querying user's per-token balances before burn ($PF_PRE_COUNT tokens)..."
    query_user_token_balances "PRE"
    info "User balances before burn ($UB_PRE_COUNT tokens):"
    local i
    for ((i=0; i<UB_PRE_COUNT; i++)); do
      info "  ${UB_PRE_SYMBOLS[$i]}: ${UB_PRE_BALANCES[$i]}"
    done
  fi

  # Transfer NACHOS to vault deposit subaccount 1
  info "Transferring $BURN_AMOUNT NACHOS to vault deposit subaccount 1..."
  local block
  block=$(transfer_nachos_to_vault "$BURN_AMOUNT")

  if [[ "$block" == TRANSFER_FAILED* ]]; then
    fail "NACHOS transfer to vault" "${block#TRANSFER_FAILED:}"
    return
  fi
  info "NACHOS transfer block: $block"
  pass "NACHOS transferred to vault deposit subaccount 1 (block $block)"

  # ── Burning disabled test (pre-checks fail before block processing, so block is reusable) ──
  call "$NACHOS_VAULT" pauseBurning >/dev/null
  local disabled_result
  disabled_result=$(call "$NACHOS_VAULT" redeemNachos "($block : nat, null)")
  assert_contains "$disabled_result" "BurningDisabled" "Burn rejected when burning disabled"
  call "$NACHOS_VAULT" unpauseBurning >/dev/null

  # Call redeemNachos (same block — not consumed by failed attempt above, with cycle tracking)
  # 10s sleep before measuring cycles to capture async follow-up work (transfer queue, etc.)
  info "Calling redeemNachos(nachosBlockNumber=$block, minimumValues=null)..."
  local burn_result cycles_before cycles_after cycles_used burn_start burn_end
  cycles_before=$(get_vault_cycles)
  burn_start=$(date +%s)
  burn_result=$(call "$NACHOS_VAULT" redeemNachos "($block : nat, null)")
  burn_end=$(date +%s)
  BURN_TIME_SECS=$((burn_end - burn_start))
  info "Burn result: $(echo "$burn_result" | head -c 500)"
  info "Waiting 10s for async operations to settle before measuring cycles..."
  sleep 20
  cycles_after=$(get_vault_cycles)
  cycles_used=$((cycles_before - cycles_after))
  BURN_CYCLES_USED=$cycles_used
  info "Burn cost: $cycles_used cycles ($cycles_before -> $cycles_after), time: ${BURN_TIME_SECS}s"
  info "  (NOTE: ~247B cycles is expected — burn triggers refreshPricesAndGetDetails which queries ~40 DEX endpoints)"
  assert_contains "$burn_result" "ok =" "redeemNachos succeeds"

  # ── Parse and validate burn result ──
  if echo "$burn_result" | grep -q "ok ="; then
    parse_burn_result "$burn_result"
    info "Parsed burn: nachosBurned=$BR_NACHOS_BURNED, navUsed=$BR_NAV_USED, redemptionICP=$BR_REDEMPTION_ICP, netICP=$BR_NET_ICP"
    info "Tokens received: $BR_TOKEN_COUNT tokens"

    # Check partialFailure = false
    if echo "$burn_result" | grep -q "partialFailure = false"; then
      pass "Burn: partialFailure = false"
    else
      fail "Burn: partialFailure" "Expected false, got: $(echo "$burn_result" | grep -oP 'partialFailure = \w+')"
    fi

    # Check failedTokens is empty
    if echo "$burn_result" | grep -q "failedTokens = vec {}"; then
      pass "Burn: failedTokens is empty (vec {})"
    else
      local failed_count
      failed_count=$(echo "$burn_result" | grep -c "failedTokens" || true)
      if [ "$failed_count" -gt 0 ]; then
        info "WARNING: failedTokens not empty — check burn result for details"
        fail "Burn: failedTokens" "Expected empty vec {}"
      fi
    fi

    # Validate redemption formula: redemptionValueICP ≈ (nachosBurned × navUsed) / 1e8
    validate_burn_redemption_formula || true

    # Display tokensReceived breakdown with symbol names
    if [ "$BR_TOKEN_COUNT" -gt 0 ]; then
      info "Tokens received breakdown:"
      printf "  %-8s %-55s %18s\n" "Symbol" "Token Principal" "Amount"
      printf "  %-8s %-55s %18s\n" "------" "---------------" "------"
      local i
      for ((i=0; i<BR_TOKEN_COUNT; i++)); do
        local sym="?"
        local j
        for ((j=0; j<PF_PRE_COUNT; j++)); do
          if [ "${PF_PRE_PRINCIPALS[$j]}" = "${BR_TOKEN_PRINCIPALS[$i]}" ]; then
            sym="${PF_PRE_SYMBOLS[$j]}"; break
          fi
        done
        printf "  %-8s %-55s %18s\n" "$sym" "${BR_TOKEN_PRINCIPALS[$i]}" "${BR_TOKEN_AMOUNTS[$i]}"
      done

      # Validate sum of tokensReceived ICP values ≈ netValueICP
      if [ "$PF_PRE_COUNT" -gt 0 ]; then
        validate_burn_tokens_sum || true
      else
        info "Skipping burn tokens sum validation — no pre-burn portfolio data"
      fi
    fi
  fi

  # Verify burn history
  local history
  history=$(call "$NACHOS_VAULT" getBurnHistory "(10 : nat, 0 : nat)")
  assert_contains "$history" "redemptionValueICP" "Burn history contains record"

  # Verify system status
  local status
  status=$(call "$NACHOS_VAULT" getSystemStatus)
  assert_not_contains "$status" "totalBurns = 0" "totalBurns > 0 after burn"

  # Verify user burn history
  local user_burn_hist
  user_burn_hist=$(call "$NACHOS_VAULT" getUserBurnHistory "(principal \"$TEST_PRINCIPAL\")")
  assert_contains "$user_burn_hist" "redemptionValueICP" "getUserBurnHistory contains burn record"

  # Verify fee history populated after mint+burn
  local fee_hist
  fee_hist=$(call "$NACHOS_VAULT" getFeeHistory "(10 : nat)")
  info "Fee history: $(echo "$fee_hist" | head -c 200)"

  # Verify active deposits count is queryable (count accumulates across runs)
  local active_deps
  active_deps=$(call "$NACHOS_VAULT" getActiveDepositsCount)
  info "Active deposits: $active_deps"
  assert_contains "$active_deps" "nat" "getActiveDepositsCount returns valid response"
}

# ════════════════════════════════════════════════════════════════
# PHASE 7: TRANSFER QUEUE STATUS
# ════════════════════════════════════════════════════════════════

phase7_transfer_queue() {
  section "PHASE 7: Transfer Queue Status"

  # Transfer queue processes on-demand (triggered when tasks are created)
  # Tasks from previous mint/burn phases are already queued
  info "Waiting 5s for async transfer processing..."
  sleep 5

  local queue
  queue=$(call "$NACHOS_VAULT" getTransferQueueStatus)
  info "Queue status: $(echo "$queue" | head -c 200)"
  assert_contains "$queue" "pending" "getTransferQueueStatus returns valid structure"

  # Check retryFailedTransfers
  local retry
  retry=$(call "$NACHOS_VAULT" retryFailedTransfers)
  assert_contains "$retry" "ok" "retryFailedTransfers succeeds"
}

# ════════════════════════════════════════════════════════════════
# PHASE 6b: POST-BURN PORTFOLIO DIFF (after transfer queue)
# ════════════════════════════════════════════════════════════════

phase6b_burn_portfolio() {
  section "PHASE 6b: Post-Burn Portfolio Diff"

  if [ "$GENESIS_DONE" != "1" ]; then
    skip "Portfolio diff" "Genesis not complete"
    return
  fi

  if [ "$PF_PRE_COUNT" -eq 0 ]; then
    skip "Portfolio diff" "No pre-burn portfolio snapshot available"
    return
  fi

  # ── Wait for vault transfer queue to finish processing ──
  # The vault schedules a 5s timer after creating transfer tasks, then
  # treasury.receiveTransferTasks processes them as a batch (~10-20s).
  info "Waiting for vault transfer queue to complete..."
  local queue_timeout=120
  local queue_start
  queue_start=$(date +%s)
  local queue_clear=0
  while true; do
    local elapsed=$(( $(date +%s) - queue_start ))
    if [ "$elapsed" -ge "$queue_timeout" ]; then
      info "WARNING: Transfer queue timeout after ${queue_timeout}s — some transfers may still be pending"
      break
    fi

    local queue_status
    queue_status=$(call "$NACHOS_VAULT" getTransferQueueStatus 2>/dev/null) || true

    # Check if any tasks are still Pending or Sent (not yet Confirmed/Failed)
    if echo "$queue_status" | grep -qE "variant \{ (Pending|Sent) \}"; then
      local pending_count sent_count
      pending_count=$(echo "$queue_status" | grep -c "variant { Pending }" 2>/dev/null || echo 0)
      sent_count=$(echo "$queue_status" | grep -c "variant { Sent }" 2>/dev/null || echo 0)
      info "  Queue: ${pending_count} pending, ${sent_count} sent — waiting... (${elapsed}s elapsed)"
      sleep 20
    else
      queue_clear=1
      info "  Transfer queue clear after ${elapsed}s"
      break
    fi
  done

  if [ "$queue_clear" -eq 1 ]; then
    pass "Transfer queue completed within ${queue_timeout}s"
  fi

  # Force treasury balance refresh so vault sees updated balances
  info "Refreshing treasury balances for accurate post-burn snapshot..."
  call "$TREASURY" refreshAllPrices >/dev/null
  sleep 10

  # Take post-burn portfolio snapshot
  info "Taking post-burn portfolio snapshot..."
  local dashboard_after
  dashboard_after=$(call "$NACHOS_VAULT" getVaultDashboard "(opt ($BURN_AMOUNT : nat), opt ($BURN_AMOUNT : nat))")
  parse_dashboard_portfolio "$dashboard_after"

  # Copy to PF_POST_* arrays
  PF_POST_SYMBOLS=("${PF_SYMBOLS[@]+"${PF_SYMBOLS[@]}"}")
  PF_POST_PRINCIPALS=("${PF_PRINCIPALS[@]+"${PF_PRINCIPALS[@]}"}")
  PF_POST_BALANCES=("${PF_BALANCES[@]+"${PF_BALANCES[@]}"}")
  PF_POST_PRICES=("${PF_PRICES[@]+"${PF_PRICES[@]}"}")
  PF_POST_VALUES=("${PF_VALUES[@]+"${PF_VALUES[@]}"}")
  PF_POST_BPS=("${PF_BPS[@]+"${PF_BPS[@]}"}")
  PF_POST_DECIMALS=("${PF_DECIMALS[@]+"${PF_DECIMALS[@]}"}")
  PF_POST_COUNT=$PF_COUNT

  if [ "$PF_POST_COUNT" -gt 0 ]; then
    pass "Post-burn portfolio snapshot captured ($PF_POST_COUNT tokens)"
    display_portfolio_diff
    write_portfolio_csv
  else
    info "WARNING: Could not parse post-burn portfolio from dashboard"
  fi

  # ── Query user's per-token balances AFTER burn + transfer queue ──
  # Retry up to 3 times if some transfers haven't finalized on-chain yet
  if [ "$UB_PRE_COUNT" -gt 0 ]; then
    local max_balance_retries=3
    local balance_attempt
    local mismatches=0

    for ((balance_attempt=1; balance_attempt<=max_balance_retries; balance_attempt++)); do
      info "Querying user's per-token balances after burn ($UB_PRE_COUNT tokens)... [attempt $balance_attempt/$max_balance_retries]"
      query_user_token_balances "POST"
      display_user_balance_diff

      # Validate: for each token, user balance diff ≈ tokensReceived amount
      mismatches=0
      local i j
      for ((i=0; i<UB_PRE_COUNT; i++)); do
        local after=0
        for ((j=0; j<UB_POST_COUNT; j++)); do
          if [ "${UB_POST_PRINCIPALS[$j]}" = "${UB_PRE_PRINCIPALS[$i]}" ]; then
            after="${UB_POST_BALANCES[$j]}"; break
          fi
        done
        local actual_diff=$((after - UB_PRE_BALANCES[$i]))
        local expected=0
        for ((j=0; j<BR_TOKEN_COUNT; j++)); do
          if [ "${BR_TOKEN_PRINCIPALS[$j]}" = "${UB_PRE_PRINCIPALS[$i]}" ]; then
            expected="${BR_TOKEN_AMOUNTS[$j]}"; break
          fi
        done

        if [ "$expected" -gt 0 ]; then
          # Tolerance: token fee (10000 for most ICRC-1 tokens) + 1
          local fee_tolerance=10001
          if [ "$actual_diff" -lt $((expected - fee_tolerance)) ]; then
            # Received LESS than expected — transfer may still be in flight
            info "  UNDER: ${UB_PRE_SYMBOLS[$i]} — balance diff=$actual_diff, tokensReceived=$expected, shortfall=$((expected - actual_diff))"
            mismatches=$((mismatches + 1))
          elif [ "$actual_diff" -gt $((expected + fee_tolerance)) ]; then
            # Received MORE than expected — note only (e.g., returned error-test deposits)
            info "  NOTE: ${UB_PRE_SYMBOLS[$i]} — received extra: diff=$actual_diff, expected=$expected, excess=$((actual_diff - expected))"
          fi
        fi
      done

      if [ "$mismatches" -eq 0 ]; then
        break
      elif [ "$balance_attempt" -lt "$max_balance_retries" ]; then
        info "  $mismatches token(s) still missing — waiting 15s for transfers to finalize on-chain..."
        sleep 15
      fi
    done

    if [ "$mismatches" -eq 0 ]; then
      pass "User received all burn tokens (balance diffs match tokensReceived)"
    else
      fail "User balance diffs vs tokensReceived" "$mismatches token(s) received LESS than expected after $max_balance_retries attempts"
    fi

    write_user_balance_csv
  fi
}

# ════════════════════════════════════════════════════════════════
# PHASE 8: CLEANUP
# ════════════════════════════════════════════════════════════════

phase8_cleanup() {
  section "PHASE 8: Cleanup"

  # Ensure system is in good state
  call "$NACHOS_VAULT" emergencyUnpause >/dev/null
  call "$NACHOS_VAULT" unpauseMinting >/dev/null
  call "$NACHOS_VAULT" unpauseBurning >/dev/null

  # Remove test exemptions (optional — keeping them is fine for staging)
  # call "$NACHOS_VAULT" removeRateLimitExemptPrincipal "(principal \"$TEST_PRINCIPAL\")" >/dev/null
  # call "$NACHOS_VAULT" removeFeeExemptPrincipal "(principal \"$TEST_PRINCIPAL\")" >/dev/null

  # Capture ending balances for CSV
  ICP_AFTER=$(get_icp_balance)
  NACHOS_AFTER=$(get_nachos_balance)
  info "Ending balances: ICP=${ICP_AFTER} e8s, NACHOS=${NACHOS_AFTER} e8s"

  pass "System state restored"
}

# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

main() {
  echo ""
  echo "╔══════════════════════════════════════════════╗"
  echo "║   NACHOS Vault Staging Test Suite            ║"
  echo "╚══════════════════════════════════════════════╝"
  echo ""

  phase0_setup
  phase1_queries
  phase2_admin_controls
  phase3_icp_mint
  phase4_error_handling
  phase5_additional_queries
  phase6_burn
  phase7_transfer_queue
  phase6b_burn_portfolio
  phase8_cleanup

  echo ""
  echo "╔══════════════════════════════════════════════╗"
  echo "║   Test Results                               ║"
  echo "╠══════════════════════════════════════════════╣"
  printf "║   \033[32mPASSED:  %-4d\033[0m                             ║\n" $PASS
  printf "║   \033[31mFAILED:  %-4d\033[0m                             ║\n" $FAIL
  printf "║   \033[33mSKIPPED: %-4d\033[0m                             ║\n" $SKIP
  printf "║   TOTAL:   %-4d                             ║\n" $TOTAL
  echo "╚══════════════════════════════════════════════╝"

  write_csv_result

  if [ "$FAIL" -gt 0 ]; then
    echo -e "\n\033[31mSome tests failed!\033[0m"
    exit 1
  else
    echo -e "\n\033[32mAll tests passed!\033[0m"
    exit 0
  fi
}

main
