#!/usr/bin/env python3
"""
Test script to verify the exchange selection algorithm against real quotes.
Tests ALL token pair combinations with ICPSwap fallback logic.
Matches treasury's findBestExecution exactly.

ICP Fallback Logic (matching treasury.mo):
1. Quote-level fallback (line 4584): When findBestExecution returns #err, try sell->ICP route
2. Execution-level fallback (line 4110): When executeTrade returns #err (e.g. "slippage over range"),
   try ICP fallback - but ONLY for ICPSwap failures (tokens recovered immediately via
   recoverBalanceFromSpecificPool). KongSwap failures use pendingTxs retry mechanism instead.

Result types:
- ICP_FALLBACK: Direct quote failed, routed via ICP (single exchange)
- ICP_FB_SPLIT: Direct quote failed, routed via ICP (split between Kong/ICPSwap)
- ICP_FB_EXEC: Execution failed after quote succeeded, ICP fallback used (ICPSwap only)
"""

import subprocess
import re
import random
import time
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Shared executor for parallel quote fetching within tests
quote_executor = ThreadPoolExecutor(max_workers=30)

# ============================================
# Configuration
# ============================================

NETWORK = "ic"
KONGSWAP_CANISTER = "2ipq2-uqaaa-aaaar-qailq-cai"
ICPSWAP_FACTORY = "4mmnk-kiaaa-aaaag-qbllq-cai"
MAX_SLIPPAGE_BP = 100  # 4.5% (450bp = 0.45%) - matches treasury production config
CALL_TIMEOUT = 25
MAX_PARALLEL = 12  # More parallel tests since quotes are now fetched in parallel too

# Token data: symbol -> (principal, decimals)
TOKENS = {
    "ICP": ("ryjl3-tyaaa-aaaaa-aaaba-cai", 8),
    "TACO": ("kknbx-zyaaa-aaaaq-aae4a-cai", 8),
    "ckBTC": ("mxzaz-hqaaa-aaaar-qaada-cai", 8),
    "DKP": ("zfcdd-tqaaa-aaaaq-aaaga-cai", 8),
    "SNEED": ("hvgxa-wqaaa-aaaaq-aacia-cai", 8),
    "MOTOKO": ("k45jy-aiaaa-aaaaq-aadcq-cai", 8),
    "sGLDT": ("i2s4q-syaaa-aaaan-qz4sq-cai", 8),
    "CHAT": ("2ouva-viaaa-aaaaq-aaamq-cai", 8),
    "GOLDAO": ("tyyy3-4aaaa-aaaaq-aab7a-cai", 8),
    "NTN": ("f54if-eqaaa-aaaaq-aacea-cai", 8),
    "cICP": ("n6tkf-tqaaa-aaaal-qsneq-cai", 8),
    "CLOWN": ("iwv6l-6iaaa-aaaal-ajjjq-cai", 8),
    "ckETH": ("ss2fx-dyaaa-aaaar-qacoq-cai", 18),
}

PRINCIPAL_TO_SYMBOL = {v[0]: k for k, v in TOKENS.items()}

# ICPSwap pools: (sell_symbol, buy_symbol) -> (pool_id, zero_for_one)
ICPSWAP_POOLS: Dict[Tuple[str, str], Tuple[str, bool]] = {}

# Test amounts in ICP equivalent
TRADE_SIZES = [1, 5, 10, 20]
MIN_TRADE_ICP = 1  # Minimum ICP equivalent worth trading
MIN_PARTIAL_TOTAL_BP = 4000  # Minimum 40% total for partial splits (prevent tiny partials like 10/10)

# ============================================
# Treasury Configuration (matches treasury.mo defaults)
# ============================================

TREASURY_CONFIG = {
    'min_trade_value_icp': 2_000_000,      # 0.02 ICP in e8s
    'max_trade_value_icp': 10_000_000,     # 0.1 ICP in e8s
    'max_slippage_bp': 450,                # 0.45%
    'min_allocation_diff_bp': 15,          # 0.15% - minimum imbalance to trigger trade
    'max_trade_attempts': 2,               # max trades per cycle
}

# Approximate ICP prices for tokens (used for random portfolio generation)
# These are rough estimates - actual quotes will come from exchanges
TOKEN_APPROX_PRICES_ICP = {
    "ICP": 100_000_000,      # 1 ICP = 1 ICP
    "TACO": 50_000,          # ~0.0005 ICP
    "ckBTC": 2_280_000_000_000,  # ~22800 ICP
    "DKP": 10_000,           # ~0.0001 ICP
    "SNEED": 100_000,        # ~0.001 ICP
    "MOTOKO": 50_000,        # ~0.0005 ICP
    "sGLDT": 1_000_000,      # ~0.01 ICP
    "CHAT": 50_000,          # ~0.0005 ICP
    "GOLDAO": 100_000,       # ~0.001 ICP
    "NTN": 50_000,           # ~0.0005 ICP
    "cICP": 100_000_000,     # ~1 ICP
    "CLOWN": 10_000,         # ~0.0001 ICP
    "ckETH": 77_000_000_000, # ~770 ICP (with 18 decimals adjustment handled separately)
}

# ============================================
# Data Types
# ============================================

@dataclass
class TokenDetails:
    """Token details for portfolio tracking - matches treasury.mo TokenDetails"""
    principal: str
    symbol: str
    decimals: int
    balance: int           # Raw balance in smallest units
    price_in_icp: int      # Price in e8s (1 ICP = 10^8)
    target_allocation_bp: int  # Target allocation in basis points (0-10000)


@dataclass
class PortfolioState:
    """Portfolio state tracking - matches treasury.mo rebalanceState"""
    tokens: Dict[str, TokenDetails]  # symbol -> details
    total_value_icp: int             # Total portfolio value in e8s


@dataclass
class Quote:
    amount_in: int
    amount_out: int
    slippage_bp: int
    valid: bool
    error: Optional[str] = None

@dataclass
class Scenario:
    name: str
    kong_pct: int
    icp_pct: int
    total_out: int
    kong_slip_bp: int
    icp_slip_bp: int

@dataclass
class TestResult:
    pair: str
    amount: int
    result_type: str  # 'SINGLE_KONG', 'SINGLE_ICP', 'SPLIT', 'SPLIT_INTERP', 'ICP_FALLBACK', 'ICP_FB_SPLIT', 'ICP_FB_EXEC', 'REDUCED', 'FAILURE'
    # Note: ICP_FB_EXEC = ICP fallback after execution failure (quote succeeded but swap failed with "slippage over range")
    # This is handled in treasury.mo at line 4110 - only for ICPSwap failures where tokens are recovered immediately
    algorithm_output: int = 0
    actual_output: int = 0
    error_pct: float = 0.0
    split_pct: Tuple[int, int] = (0, 0)  # (kong_pct, icp_pct)
    interpolated: bool = False
    details: str = ""
    max_tradeable_icp: float = 0.0  # For REDUCED: estimated max ICP at half max slippage

# ============================================
# Shared State
# ============================================

results_lock = threading.Lock()
all_results: List[TestResult] = []
completed_count = 0
total_tests = 0
stop_requested = False

# ============================================
# DFX Helpers
# ============================================

def get_kong_quote(sell_symbol: str, buy_symbol: str, amount: int, max_retries: int = 2) -> Quote:
    """Get a KongSwap quote. Uses IC. prefix for all tokens.

    Retries on transient failures (timeout, dfx_error) with exponential backoff.
    """
    kong_sell = f"IC.{sell_symbol}"
    kong_buy = f"IC.{buy_symbol}"
    args = f'("{kong_sell}", {amount}, "{kong_buy}")'
    cmd = f'dfx canister call {KONGSWAP_CANISTER} swap_amounts \'{args}\' --network {NETWORK} --identity anonymous'

    last_error = "unknown"
    for attempt in range(max_retries + 1):
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=CALL_TIMEOUT)
            if result.returncode != 0:
                last_error = "dfx_error"
                if attempt < max_retries:
                    time.sleep(0.5 * (2 ** attempt))  # Exponential backoff: 0.5s, 1s, 2s
                    continue
                return Quote(amount, 0, 10000, False, "dfx_error")

            output = result.stdout
            receive_matches = re.findall(r'receive_amount\s*=\s*(\d[_\d]*)', output)
            slippage_match = re.search(r'slippage\s*=\s*([\d.]+)', output)

            if receive_matches and slippage_match:
                receive_amount = int(receive_matches[-1].replace('_', ''))  # Use LAST match (top-level)
                slippage_pct = float(slippage_match.group(1))
                slippage_bp = int(slippage_pct * 100)
                valid = slippage_bp <= MAX_SLIPPAGE_BP and receive_amount > 0
                return Quote(amount, receive_amount, slippage_bp, valid)
            else:
                if "Err" in output:
                    return Quote(amount, 0, 10000, False, "no_pool")  # Don't retry - no pool is permanent
                last_error = "parse_error"
                if attempt < max_retries:
                    time.sleep(0.5 * (2 ** attempt))
                    continue
                return Quote(amount, 0, 10000, False, "parse_error")
        except subprocess.TimeoutExpired:
            last_error = "timeout"
            if attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return Quote(amount, 0, 10000, False, "timeout")
        except Exception as e:
            last_error = str(e)[:20]
            if attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return Quote(amount, 0, 10000, False, str(e)[:20])

    return Quote(amount, 0, 10000, False, last_error)


# Cache for pool metadata (sqrtPriceX96)
pool_metadata_cache: Dict[str, int] = {}


def get_pool_metadata(pool_id: str, max_retries: int = 2) -> Optional[int]:
    """Get sqrtPriceX96 from pool metadata. Returns None on error.

    Retries on transient failures with exponential backoff.
    """
    if pool_id in pool_metadata_cache:
        return pool_metadata_cache[pool_id]

    cmd = f'dfx canister call {pool_id} metadata \'()\' --network {NETWORK} --identity anonymous'
    for attempt in range(max_retries + 1):
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=CALL_TIMEOUT)
            if result.returncode != 0:
                if attempt < max_retries:
                    time.sleep(0.5 * (2 ** attempt))
                    continue
                return None

            output = result.stdout
            # Parse sqrtPriceX96 from metadata
            sqrt_match = re.search(r'sqrtPriceX96\s*=\s*(\d[_\d]*)', output)
            if sqrt_match:
                sqrt_price = int(sqrt_match.group(1).replace('_', ''))
                pool_metadata_cache[pool_id] = sqrt_price
                return sqrt_price
            return None  # Parse succeeded but no sqrtPriceX96 - don't retry
        except subprocess.TimeoutExpired:
            if attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return None
        except:
            if attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return None
    return None


def get_icpswap_quote(pool_id: str, amount: int, zero_for_one: bool, sqrt_price_x96: Optional[int] = None, max_retries: int = 2) -> Quote:
    """Get an ICPSwap quote with slippage calculated exactly like treasury.mo.

    Retries on transient failures (timeout, dfx_error) with exponential backoff.
    """
    zfo = "true" if zero_for_one else "false"
    args = f'(record {{ amountIn = "{amount}"; zeroForOne = {zfo}; amountOutMinimum = "0" }})'
    cmd = f'dfx canister call {pool_id} quote \'{args}\' --network {NETWORK} --identity anonymous'

    last_error = "unknown"
    for attempt in range(max_retries + 1):
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=CALL_TIMEOUT)
            if result.returncode != 0:
                last_error = "dfx_error"
                if attempt < max_retries:
                    time.sleep(0.5 * (2 ** attempt))  # Exponential backoff: 0.5s, 1s, 2s
                    continue
                return Quote(amount, 0, 10000, False, "dfx_error")

            output = result.stdout
            amount_match = re.search(r'ok\s*=\s*(\d[_\d]*)', output)

            if amount_match:
                amount_out = int(amount_match.group(1).replace('_', ''))
                if amount_out <= 0:
                    return Quote(amount, 0, 10000, False, "zero_output")  # Don't retry - valid response

                # Calculate slippage exactly like treasury.mo
                slippage_bp = 0
                if sqrt_price_x96 and sqrt_price_x96 > 0:
                    # spotPrice = (sqrtPriceX96)^2 / 2^192
                    sqrt_squared = sqrt_price_x96 * sqrt_price_x96
                    spot_price = sqrt_squared / (2 ** 192)

                    # effectivePrice = amountIn / amountOut
                    effective_price = amount / amount_out

                    # Normalize based on direction
                    if zero_for_one:
                        # Trading token0 for token1, want token0/token1 (inverse)
                        normalized_spot = 1.0 / spot_price if spot_price > 0 else 0
                    else:
                        # Trading token1 for token0, same as spot price
                        normalized_spot = spot_price

                    # slippage = (effectivePrice - spotPrice) / spotPrice * 100
                    if normalized_spot > 0:
                        slippage_pct = (effective_price - normalized_spot) / normalized_spot * 100
                        slippage_bp = int(abs(slippage_pct) * 100)  # Convert % to basis points

                valid = slippage_bp <= MAX_SLIPPAGE_BP and amount_out > 0
                return Quote(amount, amount_out, slippage_bp, valid)

            if "err" in output.lower():
                return Quote(amount, 0, 10000, False, "icp_error")  # Don't retry - valid error response
            last_error = "parse_error"
            if attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return Quote(amount, 0, 10000, False, "parse_error")
        except subprocess.TimeoutExpired:
            last_error = "timeout"
            if attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return Quote(amount, 0, 10000, False, "timeout")
        except Exception as e:
            last_error = str(e)[:20]
            if attempt < max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return Quote(amount, 0, 10000, False, str(e)[:20])

    return Quote(amount, 0, 10000, False, last_error)


def fetch_icpswap_pools(max_retries: int = 2):
    """Fetch ALL ICPSwap pools and store by token pair.

    Retries on transient failures with exponential backoff.
    """
    global ICPSWAP_POOLS
    print("Fetching ICPSwap pools from factory...")
    cmd = f'dfx canister call {ICPSWAP_FACTORY} getPools \'()\' --network {NETWORK} --identity anonymous'

    for attempt in range(max_retries + 1):
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                print(f"  Error (attempt {attempt + 1}): {result.stderr[:100]}")
                if attempt < max_retries:
                    time.sleep(1.0 * (2 ** attempt))  # Longer backoff for pool fetch: 1s, 2s, 4s
                    continue
                return

            output = result.stdout
            pool_pattern = re.compile(
                r'token0\s*=\s*record\s*\{\s*address\s*=\s*"([^"]+)"[^}]*\}\s*;\s*'
                r'token1\s*=\s*record\s*\{\s*address\s*=\s*"([^"]+)"[^}]*\}\s*;\s*'
                r'canisterId\s*=\s*principal\s*"([^"]+)"'
            )
            pool_matches = pool_pattern.findall(output)

            found = 0
            for token0_principal, token1_principal, pool_id in pool_matches:
                sym0 = PRINCIPAL_TO_SYMBOL.get(token0_principal)
                sym1 = PRINCIPAL_TO_SYMBOL.get(token1_principal)

                if sym0 and sym1:
                    # zeroForOne=true means selling token0 for token1
                    ICPSWAP_POOLS[(sym0, sym1)] = (pool_id, True)
                    ICPSWAP_POOLS[(sym1, sym0)] = (pool_id, False)
                    found += 1

            print(f"  Found {found} pools ({len(ICPSWAP_POOLS)} directions)")
            return  # Success - exit retry loop
        except subprocess.TimeoutExpired:
            print(f"  Timeout (attempt {attempt + 1})")
            if attempt < max_retries:
                time.sleep(1.0 * (2 ** attempt))
                continue
        except Exception as e:
            print(f"  Exception (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                time.sleep(1.0 * (2 ** attempt))
                continue


# ============================================
# Algorithm (Matches Treasury Exactly)
# ============================================

def run_algorithm(kong_quotes: List[Quote], icp_quotes: List[Quote], num_quotes: int = 5):
    """
    Run the exchange selection algorithm exactly as treasury does.
    Returns for full scenarios: (result_type, kong_pct, icp_pct, expected_output, expected_output_no_interp, was_interpolated)
    Returns for partials: ('PARTIAL_CANDIDATES', partial_scenarios_list, step_bp, 0, 0, False)
    result_type: 'SINGLE_KONG', 'SINGLE_ICP', 'SPLIT', 'PARTIAL_CANDIDATES', 'NO_PATH'

    num_quotes: number of quote points (default 5 = 20/40/60/80/100%)
    """
    scenarios: List[Scenario] = []
    partial_scenarios: List[Scenario] = []  # Splits that don't sum to 100%
    n = num_quotes
    step_bp = 10000 // n  # e.g., 5 quotes = 2000bp step (20%)

    # Single Kong (100%) - last index
    if kong_quotes[n-1].valid and kong_quotes[n-1].slippage_bp <= MAX_SLIPPAGE_BP:
        scenarios.append(Scenario("KONG_100", 10000, 0, kong_quotes[n-1].amount_out,
                                  kong_quotes[n-1].slippage_bp, 0))

    # Single ICPSwap (100%) - last index
    if icp_quotes[n-1].valid and icp_quotes[n-1].slippage_bp <= MAX_SLIPPAGE_BP:
        scenarios.append(Scenario("ICP_100", 0, 10000, icp_quotes[n-1].amount_out,
                                  0, icp_quotes[n-1].slippage_bp))

    # Check ALL split combinations (both full 100% splits and partial splits)
    # This finds more valid routes when 100% single exchange fails but smaller amounts work
    for kong_idx in range(n):
        for icp_idx in range(n):
            kong_pct = (kong_idx + 1) * step_bp
            icp_pct = (icp_idx + 1) * step_bp
            total_pct = kong_pct + icp_pct

            # Skip if over 100% (can't trade more than we have)
            if total_pct > 10000:
                continue

            # Skip 100% singles (already checked above)
            if kong_pct == 10000 or icp_pct == 10000:
                continue

            # Check if both quotes are valid
            if (kong_quotes[kong_idx].valid and kong_quotes[kong_idx].slippage_bp <= MAX_SLIPPAGE_BP and
                icp_quotes[icp_idx].valid and icp_quotes[icp_idx].slippage_bp <= MAX_SLIPPAGE_BP):
                total_out = kong_quotes[kong_idx].amount_out + icp_quotes[icp_idx].amount_out

                if total_pct == 10000:
                    # Full split (sums to 100%)
                    scenarios.append(Scenario(
                        f"SPLIT_{kong_pct//100}_{icp_pct//100}",
                        kong_pct, icp_pct, total_out,
                        kong_quotes[kong_idx].slippage_bp, icp_quotes[icp_idx].slippage_bp
                    ))
                else:
                    # Partial split (sums to less than 100%)
                    partial_scenarios.append(Scenario(
                        f"PARTIAL_{kong_pct//100}_{icp_pct//100}",
                        kong_pct, icp_pct, total_out,
                        kong_quotes[kong_idx].slippage_bp, icp_quotes[icp_idx].slippage_bp
                    ))

    # If no full scenarios, return partial candidates for caller to filter/select
    # Caller has access to trade context (sell_symbol, buy_symbol, MIN_TRADE_ICP) needed for filtering
    if not scenarios and partial_scenarios:
        # Sort by combined slippage (best slippage first) as default ordering
        partial_scenarios.sort(key=lambda s: s.kong_slip_bp + s.icp_slip_bp)
        return ('PARTIAL_CANDIDATES', partial_scenarios, step_bp, 0, 0, False)

    if not scenarios:
        return ('NO_PATH', 0, 0, 0, 0, False)

    # Sort by total output (descending)
    scenarios.sort(key=lambda s: s.total_out, reverse=True)
    best = scenarios[0]
    second = scenarios[1] if len(scenarios) > 1 else None

    # No interpolation result (for comparison)
    no_interp_output = best.total_out

    # Check for interpolation between adjacent splits
    if second is not None:
        both_splits = 0 < best.kong_pct < 10000 and 0 < second.kong_pct < 10000
        diff = abs(best.kong_pct - second.kong_pct)

        if both_splits and diff == step_bp:  # Adjacent splits
            # Interpolate using average slippages
            avg_kong_slip = (best.kong_slip_bp + second.kong_slip_bp) / 2
            avg_icp_slip = (best.icp_slip_bp + second.icp_slip_bp) / 2
            total_slip = avg_kong_slip + avg_icp_slip

            if total_slip > 0:
                kong_ratio = avg_icp_slip / total_slip
                low_kong = min(best.kong_pct, second.kong_pct)
                high_kong = max(best.kong_pct, second.kong_pct)
                interp_kong = low_kong + int(kong_ratio * (high_kong - low_kong))
                interp_icp = 10000 - interp_kong

                # Estimate interpolated output
                t = kong_ratio
                if best.kong_pct < second.kong_pct:
                    interp_out = int(best.total_out + t * (second.total_out - best.total_out))
                else:
                    interp_out = int(second.total_out + t * (best.total_out - second.total_out))

                return ('SPLIT', interp_kong, interp_icp, interp_out, no_interp_output, True)

    # No interpolation - use best as-is
    if best.kong_pct == 10000:
        return ('SINGLE_KONG', 10000, 0, best.total_out, no_interp_output, False)
    elif best.icp_pct == 10000:
        return ('SINGLE_ICP', 0, 10000, best.total_out, no_interp_output, False)
    else:
        return ('SPLIT', best.kong_pct, best.icp_pct, best.total_out, no_interp_output, False)


# ============================================
# Treasury Portfolio Logic (matches treasury.mo exactly)
# ============================================

def generate_random_allocations(tokens: List[str]) -> Dict[str, int]:
    """
    Generate random target allocations that sum to 10000 bp.
    Matches treasury.mo allocation setup logic.
    """
    weights = [random.randint(100, 2000) for _ in tokens]
    total = sum(weights)
    allocations = {t: int(w * 10000 / total) for t, w in zip(tokens, weights)}

    # Ensure allocations sum to exactly 10000
    diff = 10000 - sum(allocations.values())
    if diff != 0:
        # Add remainder to first token
        first_token = tokens[0]
        allocations[first_token] += diff

    return allocations


# ============================================
# Production Data Fetching (from mainnet canisters)
# ============================================

DAO_CANISTER_ID = "vxqw7-iqaaa-aaaan-qzziq-cai"
TREASURY_CANISTER_ID = "v6t5d-6yaaa-aaaan-qzzja-cai"


def parse_production_token_details(output: str) -> Dict[str, Dict]:
    """
    Parse getTokenDetailsWithoutPastPrices output from DAO canister.
    Returns: {principal -> {decimals, balance, priceInICP}}
    """
    tokens = {}
    # Pattern: principal "xxx"; record { ... priceInICP = N : nat; ... tokenDecimals = N : nat; ... balance = N : nat; ...}
    # Split by record pairs
    records = re.findall(r'record\s*\{\s*principal\s*"([^"]+)";\s*record\s*\{([^}]+(?:\{[^}]*\}[^}]*)*)\}', output)

    for principal, record_data in records:
        # Extract priceInICP
        price_match = re.search(r'priceInICP\s*=\s*(\d[_\d]*)\s*:\s*nat', record_data)
        # Extract tokenDecimals
        decimals_match = re.search(r'tokenDecimals\s*=\s*(\d+)\s*:\s*nat', record_data)
        # Extract balance
        balance_match = re.search(r'balance\s*=\s*(\d[_\d]*)\s*:\s*nat', record_data)

        if price_match and decimals_match and balance_match:
            tokens[principal] = {
                'priceInICP': int(price_match.group(1).replace('_', '')),
                'decimals': int(decimals_match.group(1)),
                'balance': int(balance_match.group(1).replace('_', '')),
            }

    return tokens


def parse_production_allocations(output: str) -> Dict[str, int]:
    """
    Parse getCurrentAllocations output from Treasury canister.
    Returns: {principal -> allocation_bp}
    """
    allocations = {}
    # Pattern: record { principal "xxx"; N : nat;}
    records = re.findall(r'record\s*\{\s*principal\s*"([^"]+)";\s*(\d[_\d]*)\s*:\s*nat;\s*\}', output)

    for principal, alloc_bp in records:
        allocations[principal] = int(alloc_bp.replace('_', ''))

    return allocations


def parse_production_config(output: str) -> Dict[str, int]:
    """
    Parse getSystemParameters output from Treasury canister.
    Returns config dict with trade limits.
    """
    config = {}

    # Extract maxSlippageBasisPoints
    match = re.search(r'maxSlippageBasisPoints\s*=\s*(\d[_\d]*)\s*:\s*nat', output)
    if match:
        config['max_slippage_bp'] = int(match.group(1).replace('_', ''))

    # Extract maxTradeValueICP
    match = re.search(r'maxTradeValueICP\s*=\s*(\d[_\d]*)\s*:\s*nat', output)
    if match:
        config['max_trade_value_icp'] = int(match.group(1).replace('_', ''))

    # Extract minTradeValueICP
    match = re.search(r'minTradeValueICP\s*=\s*(\d[_\d]*)\s*:\s*nat', output)
    if match:
        config['min_trade_value_icp'] = int(match.group(1).replace('_', ''))

    # Extract maxTradeAttemptsPerInterval
    match = re.search(r'maxTradeAttemptsPerInterval\s*=\s*(\d+)\s*:\s*nat', output)
    if match:
        config['max_trade_attempts'] = int(match.group(1))

    return config


def fetch_production_data() -> Tuple[Dict[str, Dict], Dict[str, int], Dict[str, int]]:
    """
    Fetch real data from production canisters.
    Returns: (token_details, allocations, config)
    """
    import concurrent.futures

    def fetch_token_details():
        cmd = f'dfx canister call {DAO_CANISTER_ID} getTokenDetailsWithoutPastPrices "()" --network ic --identity anonymous'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to fetch token details: {result.stderr}")
        return parse_production_token_details(result.stdout)

    def fetch_allocations():
        cmd = f'dfx canister call {TREASURY_CANISTER_ID} getCurrentAllocations "()" --network ic --identity anonymous'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to fetch allocations: {result.stderr}")
        return parse_production_allocations(result.stdout)

    def fetch_config():
        cmd = f'dfx canister call {TREASURY_CANISTER_ID} getSystemParameters "()" --network ic --identity anonymous'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to fetch config: {result.stderr}")
        return parse_production_config(result.stdout)

    # Fetch all in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        token_future = executor.submit(fetch_token_details)
        alloc_future = executor.submit(fetch_allocations)
        config_future = executor.submit(fetch_config)

        token_details = token_future.result()
        allocations = alloc_future.result()
        config = config_future.result()

    return token_details, allocations, config


def initialize_portfolio_from_production(
    use_real_balances: bool = True
) -> Tuple[PortfolioState, Dict[str, int]]:
    """
    Initialize portfolio with REAL data from production canisters.

    Uses:
    - REAL prices from DAO getTokenDetailsWithoutPastPrices
    - REAL decimals from DAO getTokenDetailsWithoutPastPrices
    - REAL balances from DAO getTokenDetailsWithoutPastPrices (if use_real_balances=True)
    - RANDOM target allocations (for test diversity)

    Returns: (portfolio, config)
    """
    print("  Fetching production data from mainnet canisters...")
    token_details, prod_allocations, prod_config = fetch_production_data()

    # Build symbol lookup from principals
    principal_to_symbol = {v[0]: k for k, v in TOKENS.items()}

    # Generate RANDOM target allocations (not using production allocations)
    symbols = list(TOKENS.keys())
    random_target_allocations = generate_random_allocations(symbols)

    tokens: Dict[str, TokenDetails] = {}
    total_value_icp = 0

    for principal, details in token_details.items():
        symbol = principal_to_symbol.get(principal)
        if not symbol:
            continue  # Skip unknown tokens

        price_in_icp = details['priceInICP']
        decimals = details['decimals']
        balance = details['balance'] if use_real_balances else 0

        # Calculate value contribution
        if use_real_balances:
            value_in_icp = (balance * price_in_icp) // (10 ** decimals)
            total_value_icp += value_in_icp

        tokens[symbol] = TokenDetails(
            principal=principal,
            symbol=symbol,
            decimals=decimals,
            balance=balance,
            price_in_icp=price_in_icp,
            target_allocation_bp=random_target_allocations.get(symbol, 0)
        )

    # If not using real balances, create synthetic balances based on target allocations
    if not use_real_balances:
        # Use a synthetic total value of ~100 ICP
        total_value_icp = 100_00_000_000  # 100 ICP in e8s

        # Generate random current allocations (to create imbalance vs targets)
        current_allocations = generate_random_allocations(symbols)

        for symbol, token in tokens.items():
            current_bp = current_allocations.get(symbol, 0)
            value_in_icp = (current_bp * total_value_icp) // 10000

            # Convert value to token amount using real price
            if token.price_in_icp > 0:
                token.balance = (value_in_icp * (10 ** token.decimals)) // token.price_in_icp

    # Build config from production (with defaults for missing values)
    config = {
        'min_trade_value_icp': prod_config.get('min_trade_value_icp', 500_000_000),  # 5 ICP default
        'max_trade_value_icp': prod_config.get('max_trade_value_icp', 2_000_000_000),  # 20 ICP default
        'max_slippage_bp': prod_config.get('max_slippage_bp', 100),  # 1% default
        'min_allocation_diff_bp': 15,  # Not in production config, use default
        'max_trade_attempts': prod_config.get('max_trade_attempts', 1),  # 1 per interval default
    }

    print(f"  Loaded {len(tokens)} tokens with real prices from production")
    print(f"  Config: maxSlippage={config['max_slippage_bp']}bp, "
          f"minTrade={config['min_trade_value_icp']/1e8:.1f} ICP, "
          f"maxTrade={config['max_trade_value_icp']/1e8:.1f} ICP")

    # Interactive override prompt
    config = prompt_for_config_overrides(config)

    return PortfolioState(tokens=tokens, total_value_icp=total_value_icp), config


def prompt_for_config_overrides(config: Dict[str, int]) -> Dict[str, int]:
    """
    Prompt user to optionally override config values.
    Press Enter to keep production values.
    """
    print("\n  Override config values? (Enter to skip)")

    # Max slippage override
    try:
        slippage_input = input(f"    Reduce maxSlippage ({config['max_slippage_bp']}bp) by X%: ").strip()
        if slippage_input:
            reduction_pct = float(slippage_input)
            if 0 < reduction_pct < 100:
                new_val = int(config['max_slippage_bp'] * (100 - reduction_pct) / 100)
                print(f"      -> {config['max_slippage_bp']}bp reduced by {reduction_pct}% = {new_val}bp")
                config['max_slippage_bp'] = new_val
    except (ValueError, EOFError):
        pass

    # Max trade value override
    try:
        max_trade_input = input(f"    Reduce maxTrade ({config['max_trade_value_icp']/1e8:.1f} ICP) by X%: ").strip()
        if max_trade_input:
            reduction_pct = float(max_trade_input)
            if 0 < reduction_pct < 100:
                new_val = int(config['max_trade_value_icp'] * (100 - reduction_pct) / 100)
                print(f"      -> {config['max_trade_value_icp']/1e8:.1f} ICP reduced by {reduction_pct}% = {new_val/1e8:.1f} ICP")
                config['max_trade_value_icp'] = new_val
    except (ValueError, EOFError):
        pass

    # Number of quote points per exchange (default 5: 20%, 40%, 60%, 80%, 100%)
    # More = finer split granularity, fewer = faster but less precision
    config['num_quotes'] = 5  # Default
    try:
        quotes_input = input(f"    Quote points per exchange (default 5): ").strip()
        if quotes_input:
            num_quotes = int(quotes_input)
            if 2 <= num_quotes <= 10:
                config['num_quotes'] = num_quotes
                step = 100 // num_quotes
                pcts = [f"{step*(i+1)}%" for i in range(num_quotes)]
                print(f"      -> Using {num_quotes} quote points: {', '.join(pcts)}")
            else:
                print(f"      -> Invalid (must be 2-10), using default 5")
    except (ValueError, EOFError):
        pass

    print(f"  Using: maxSlippage={config['max_slippage_bp']}bp, maxTrade={config['max_trade_value_icp']/1e8:.1f} ICP, quotePoints={config['num_quotes']}")
    return config


# Global for num_quotes (set from config during initialization)
NUM_QUOTES = 5


def initialize_portfolio_with_random_allocations(
    total_portfolio_icp: int = 100_00_000_000  # 100 ICP in e8s default
) -> PortfolioState:
    """
    Initialize a portfolio with random held and target allocations.
    Matches treasury.mo portfolio initialization.
    """
    symbols = list(TOKENS.keys())

    # Generate random target allocations (what we want)
    target_allocations = generate_random_allocations(symbols)

    # Generate random current allocations (what we have - slightly different to create imbalance)
    current_allocations = generate_random_allocations(symbols)

    tokens: Dict[str, TokenDetails] = {}

    for symbol in symbols:
        principal, decimals = TOKENS[symbol]
        price_in_icp = TOKEN_APPROX_PRICES_ICP.get(symbol, 100_000)

        # Calculate balance based on current allocation
        current_bp = current_allocations[symbol]
        value_in_icp = (current_bp * total_portfolio_icp) // 10000

        # Convert value to token amount
        if symbol == "ckETH":
            # ckETH has 18 decimals - special handling
            balance = (value_in_icp * (10 ** 18)) // price_in_icp
        else:
            balance = (value_in_icp * (10 ** decimals)) // price_in_icp

        tokens[symbol] = TokenDetails(
            principal=principal,
            symbol=symbol,
            decimals=decimals,
            balance=balance,
            price_in_icp=price_in_icp,
            target_allocation_bp=target_allocations[symbol]
        )

    return PortfolioState(tokens=tokens, total_value_icp=total_portfolio_icp)


def calculate_total_portfolio_value(portfolio: PortfolioState) -> int:
    """
    Calculate total portfolio value in ICP (e8s).
    Matches treasury.mo calculateTradeRequirements() Phase 2.
    """
    total = 0
    for symbol, details in portfolio.tokens.items():
        value_in_icp = (details.balance * details.price_in_icp) // (10 ** details.decimals)
        total += value_in_icp
    return total


def calculate_trade_requirements(
    portfolio: PortfolioState,
    min_allocation_diff_bp: int = 15
) -> List[Tuple[str, int, int]]:
    """
    Calculate which tokens need trading.
    Matches treasury.mo calculateTradeRequirements() exactly.

    Returns: [(token_symbol, diff_bp, value_in_icp)]
    - diff_bp > 0: underweight (need to buy)
    - diff_bp < 0: overweight (need to sell)
    """
    # Recalculate total value
    total_value_icp = calculate_total_portfolio_value(portfolio)
    portfolio.total_value_icp = total_value_icp

    if total_value_icp == 0:
        return []

    # Sanity check: ensure we have valid target allocations
    if sum(d.target_allocation_bp for d in portfolio.tokens.values()) == 0:
        return []

    trade_pairs = []

    for symbol, details in portfolio.tokens.items():
        # Calculate current value in ICP
        value_in_icp = (details.balance * details.price_in_icp) // (10 ** details.decimals)

        # Calculate current basis points
        current_bp = (value_in_icp * 10000) // total_value_icp

        # Use target allocation directly (already in basis points)
        # NOTE: Removed buggy normalization that caused 200-1000bp jumps when
        # total_target_bp != 10000 due to integer truncation in generate_random_allocations()
        target_bp = details.target_allocation_bp

        # Calculate diff
        diff_bp = target_bp - current_bp

        # Filter by minimum threshold (matches treasury.mo:5190)
        # Also skip tokens with 0 balance that are overweight (can't sell what we don't have)
        if abs(diff_bp) > min_allocation_diff_bp:
            if diff_bp < 0 and details.balance == 0:
                continue  # Can't sell a token with no balance
            trade_pairs.append((symbol, diff_bp, value_in_icp))

    return trade_pairs


def select_trading_pair(
    trade_diffs: List[Tuple[str, int, int]]
) -> Optional[Tuple[str, str, int, int]]:
    """
    Select sell/buy tokens using weighted random selection.
    Matches treasury.mo selectTradingPair() exactly.

    Returns: (sell_symbol, buy_symbol, sell_diff_bp, buy_diff_bp) or None
    """
    # Split into overweight (sell) and underweight (buy)
    to_sell = [(t, abs(d), v) for t, d, v in trade_diffs if d < 0]  # negative diff = overweight
    to_buy = [(t, abs(d), v) for t, d, v in trade_diffs if d > 0]   # positive diff = underweight

    if not to_sell or not to_buy:
        return None

    # Weighted random selection (matches treasury.mo:5270-5330)
    total_sell_weight = sum(w for _, w, _ in to_sell)
    total_buy_weight = sum(w for _, w, _ in to_buy)

    if total_sell_weight == 0 or total_buy_weight == 0:
        return None

    sell_random = random.randint(0, total_sell_weight - 1)
    buy_random = random.randint(0, total_buy_weight - 1)

    # Select sell token based on cumulative weight
    selected_sell = None
    sell_diff = 0
    cumulative = 0
    for token, weight, _ in to_sell:
        cumulative += weight
        if cumulative > sell_random and selected_sell is None:
            selected_sell = token
            sell_diff = -weight  # Negative because overweight

    # Select buy token based on cumulative weight
    selected_buy = None
    buy_diff = 0
    cumulative = 0
    for token, weight, _ in to_buy:
        cumulative += weight
        if cumulative > buy_random and selected_buy is None:
            selected_buy = token
            buy_diff = weight  # Positive because underweight

    if selected_sell and selected_buy:
        return (selected_sell, selected_buy, sell_diff, buy_diff)
    return None


def should_use_exact_targeting(
    sell_token_diff_bp: int,
    buy_token_diff_bp: int,
    total_portfolio_value_icp: int,
    max_trade_value_icp: int
) -> bool:
    """
    Determine if exact targeting should be used.
    Matches treasury.mo shouldUseExactTargeting() exactly.

    Use exact targeting when either token is close to target
    (within 50% of max trade size expressed in basis points).
    """
    if total_portfolio_value_icp == 0:
        return False

    # 50% of max trade size in basis points of portfolio
    half_max_trade_bp = (max_trade_value_icp * 10000 // 2) // total_portfolio_value_icp

    sell_close = abs(sell_token_diff_bp) <= half_max_trade_bp
    buy_close = abs(buy_token_diff_bp) <= half_max_trade_bp

    return sell_close or buy_close


def calculate_exact_target_trade_size(
    sell_token: TokenDetails,
    buy_token: TokenDetails,
    total_portfolio_value_icp: int,
    sell_token_diff_bp: int,
    buy_token_diff_bp: int,
    max_trade_value_icp: int,
    min_trade_value_icp: int
) -> Tuple[int, bool]:
    """
    Calculate exact trade size to reach target allocation.
    Matches treasury.mo calculateExactTargetTradeSize() exactly.

    Returns: (trade_size_in_sell_tokens, is_exact_targeting)
    """
    # Choose token closer to target (smaller diff)
    target_sell = abs(sell_token_diff_bp) <= abs(buy_token_diff_bp)

    if target_sell:
        # Calculate excess value to sell to reach target
        excess_value_icp = (abs(sell_token_diff_bp) * total_portfolio_value_icp) // 10000
        exact_size = (excess_value_icp * (10 ** sell_token.decimals)) // sell_token.price_in_icp
    else:
        # Calculate deficit value to buy (expressed in sell token)
        deficit_value_icp = (abs(buy_token_diff_bp) * total_portfolio_value_icp) // 10000
        exact_size = (deficit_value_icp * (10 ** sell_token.decimals)) // sell_token.price_in_icp

    # Check max bound
    trade_size_icp = (exact_size * sell_token.price_in_icp) // (10 ** sell_token.decimals)

    if trade_size_icp > max_trade_value_icp:
        # Fall back to random sizing
        random_icp = random.randint(min_trade_value_icp, max_trade_value_icp)
        random_size = (random_icp * (10 ** sell_token.decimals)) // sell_token.price_in_icp
        return (random_size, False)

    return (exact_size, True)


def calculate_trade_size_min_max(
    min_trade_value_icp: int,
    max_trade_value_icp: int,
    sell_token: TokenDetails
) -> int:
    """
    Calculate random trade size between min and max.
    Matches treasury.mo calculateTradeSizeMinMax() exactly.
    """
    random_icp = random.randint(min_trade_value_icp, max_trade_value_icp)
    return (random_icp * (10 ** sell_token.decimals)) // sell_token.price_in_icp


def adjust_trade_for_slippage(
    trade_size: int,
    slippage_bp: int,
    is_exact_targeting: bool,
    expected_out: int,
    max_slippage_bp: int
) -> Tuple[int, int, int]:
    """
    Adjust trade size to compensate for slippage.
    Matches treasury.mo slippage adjustment logic (lines 3930-3973) exactly.

    Returns: (final_trade_size, adjusted_expected_out, min_amount_out)
    """
    # Only adjust if exact targeting AND slippage > 0
    if is_exact_targeting and slippage_bp > 0:
        # Reduce trade size: adjusted = size * 10000 / (10000 + slippageBP)
        denominator = 10000 + slippage_bp
        final_size = (trade_size * 10000) // denominator
    else:
        final_size = trade_size

    # Adjust expected output proportionally
    if final_size < trade_size and trade_size > 0:
        adjusted_expected_out = (expected_out * final_size) // trade_size
    else:
        adjusted_expected_out = expected_out

    # Calculate ideal output (reverse slippage)
    if slippage_bp < 9900:
        ideal_out = (adjusted_expected_out * 10000) // (10000 - slippage_bp)
    else:
        ideal_out = adjusted_expected_out

    # Apply slippage tolerance to get min amount out
    tolerance_multiplier = 10000 - min(max_slippage_bp, 10000)
    min_amount_out = (ideal_out * tolerance_multiplier) // 10000

    return (final_size, adjusted_expected_out, min_amount_out)


def update_prices_after_trade(
    portfolio: PortfolioState,
    sell_symbol: str,
    buy_symbol: str,
    amount_sold: int,
    amount_bought: int
) -> None:
    """
    Update token prices based on trade execution.
    Matches treasury.mo price update logic (lines 4007-4039) exactly.
    """
    sell_token = portfolio.tokens[sell_symbol]
    buy_token = portfolio.tokens[buy_symbol]

    if sell_symbol == "ICP" or buy_symbol == "ICP":
        if sell_symbol == "ICP":
            # Sold ICP, bought token - update buy token price
            if amount_bought > 0:
                actual_tokens = amount_bought / (10 ** buy_token.decimals)
                actual_icp = amount_sold / 1e8
                if actual_tokens > 0:
                    new_price = int((actual_icp / actual_tokens) * 1e8)
                    if new_price > 0:  # Ensure price never goes to 0
                        buy_token.price_in_icp = new_price
        else:
            # Sold token, received ICP - update sell token price
            if amount_sold > 0:
                actual_tokens = amount_sold / (10 ** sell_token.decimals)
                actual_icp = amount_bought / 1e8
                if actual_tokens > 0:
                    new_price = int((actual_icp / actual_tokens) * 1e8)
                    if new_price > 0:  # Ensure price never goes to 0
                        sell_token.price_in_icp = new_price
    else:
        # Non-ICP pair - randomly choose which token's price to maintain
        maintain_first = random.choice([True, False])

        if amount_sold > 0 and amount_bought > 0:
            actual_sold = amount_sold / (10 ** sell_token.decimals)
            actual_bought = amount_bought / (10 ** buy_token.decimals)

            if maintain_first and actual_bought > 0:
                # Keep sell token price, update buy token price
                price_ratio = actual_sold / actual_bought
                new_price = int(sell_token.price_in_icp * price_ratio)
                if new_price > 0:  # Ensure price never goes to 0
                    buy_token.price_in_icp = new_price
            elif actual_sold > 0:
                # Keep buy token price, update sell token price
                price_ratio = actual_bought / actual_sold
                new_price = int(buy_token.price_in_icp * price_ratio)
                if new_price > 0:  # Ensure price never goes to 0
                    sell_token.price_in_icp = new_price

    # Update balances after trade
    sell_token.balance -= amount_sold
    buy_token.balance += amount_bought

    # Recalculate total portfolio value
    portfolio.total_value_icp = calculate_total_portfolio_value(portfolio)


def run_full_trading_cycle_test(num_cycles: int = 5) -> List[Dict]:
    """
    Run a complete trading cycle test matching treasury.mo logic.
    This simulates the full trading decision process without executing real trades.

    Returns list of trade decisions made.
    """
    # Initialize portfolio with random allocations
    portfolio = initialize_portfolio_with_random_allocations()
    config = TREASURY_CONFIG

    trades = []

    print("\n" + "=" * 80)
    print("FULL TRADING CYCLE TEST (matches treasury.mo) - SIMULATED")
    print("=" * 80)
    print(f"\nInitial Portfolio ({len(portfolio.tokens)} tokens):")
    print(f"  Total Value: {portfolio.total_value_icp / 1e8:.2f} ICP")
    print("\nToken Allocations:")
    for symbol, details in sorted(portfolio.tokens.items()):
        value = (details.balance * details.price_in_icp) // (10 ** details.decimals)
        current_bp = (value * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
        print(f"  {symbol:8} target={details.target_allocation_bp:4}bp current={current_bp:4}bp diff={details.target_allocation_bp - current_bp:+4}bp")

    for cycle in range(num_cycles):
        print(f"\n--- Cycle {cycle + 1}/{num_cycles} ---")

        for attempt in range(config['max_trade_attempts']):
            # Step 1: Calculate trade requirements
            trade_diffs = calculate_trade_requirements(
                portfolio,
                config['min_allocation_diff_bp']
            )

            if not trade_diffs:
                print(f"  Attempt {attempt + 1}: No viable trading candidates (all within {config['min_allocation_diff_bp']}bp)")
                break

            print(f"  Attempt {attempt + 1}: {len(trade_diffs)} tokens need rebalancing")

            # Step 2: Select trading pair
            pair = select_trading_pair(trade_diffs)
            if not pair:
                print(f"    Could not select trading pair")
                continue

            sell_symbol, buy_symbol, sell_diff, buy_diff = pair
            print(f"    Selected: {sell_symbol} ({sell_diff:+}bp) -> {buy_symbol} ({buy_diff:+}bp)")

            # Step 3: Determine trade size strategy
            use_exact = should_use_exact_targeting(
                sell_diff, buy_diff,
                portfolio.total_value_icp,
                config['max_trade_value_icp']
            )

            sell_token = portfolio.tokens[sell_symbol]
            buy_token = portfolio.tokens[buy_symbol]

            if use_exact:
                trade_size, is_exact = calculate_exact_target_trade_size(
                    sell_token, buy_token,
                    portfolio.total_value_icp,
                    sell_diff, buy_diff,
                    config['max_trade_value_icp'],
                    config['min_trade_value_icp']
                )
                sizing_method = "EXACT" if is_exact else "RANDOM (fallback)"
            else:
                trade_size = calculate_trade_size_min_max(
                    config['min_trade_value_icp'],
                    config['max_trade_value_icp'],
                    sell_token
                )
                is_exact = False
                sizing_method = "RANDOM"

            # Cap trade size at available balance (can't sell more than we have)
            if trade_size > sell_token.balance:
                trade_size = sell_token.balance

            # Skip if trade size is 0 (no balance left)
            if trade_size == 0:
                print(f"    SKIPPED: No {sell_symbol} balance available")
                continue

            trade_value_icp = (trade_size * sell_token.price_in_icp) // (10 ** sell_token.decimals)
            print(f"    Trade size: {trade_size} {sell_symbol} (~{trade_value_icp / 1e8:.4f} ICP) [{sizing_method}]")

            # Step 4: Get quotes (this is where we'd call test_pair_internal in a real test)
            # For simulation, we'll estimate the output based on price
            if buy_token.price_in_icp == 0:
                print(f"    SKIPPED: {buy_symbol} has zero price")
                continue
            # Convert trade_size to ICP value, then to buy_token amount
            # This properly handles different token decimals
            trade_value_in_icp_e8s = (trade_size * sell_token.price_in_icp) // (10 ** sell_token.decimals)
            estimated_out = (trade_value_in_icp_e8s * (10 ** buy_token.decimals)) // buy_token.price_in_icp
            slippage_bp = random.randint(10, 100)  # Simulate some slippage

            # Step 5: Apply slippage adjustment
            final_size, adj_expected, min_out = adjust_trade_for_slippage(
                trade_size, slippage_bp, is_exact,
                estimated_out,
                config['max_slippage_bp']
            )

            if is_exact and final_size != trade_size:
                print(f"    Slippage adjustment: {trade_size} -> {final_size} ({slippage_bp}bp)")

            # Step 6: Simulate trade result and update prices
            # Use adjusted expected output as simulated result
            simulated_out = adj_expected

            trade_record = {
                'cycle': cycle + 1,
                'attempt': attempt + 1,
                'sell': sell_symbol,
                'buy': buy_symbol,
                'amount_sold': final_size,
                'amount_bought': simulated_out,
                'is_exact': is_exact,
                'slippage_bp': slippage_bp,
                'sell_diff_bp': sell_diff,
                'buy_diff_bp': buy_diff,
            }
            trades.append(trade_record)

            # Update portfolio state
            update_prices_after_trade(
                portfolio,
                sell_symbol, buy_symbol,
                final_size, simulated_out
            )

            print(f"    Simulated: {final_size} {sell_symbol} -> {simulated_out} {buy_symbol}")

            # Show portfolio state after trade
            print(f"    Portfolio after trade:")
            for sym, det in sorted(portfolio.tokens.items()):
                val = (det.balance * det.price_in_icp) // (10 ** det.decimals)
                cur_bp = (val * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
                diff = det.target_allocation_bp - cur_bp
                if abs(diff) > 50:  # Only show significant imbalances
                    print(f"      {sym:8} target={det.target_allocation_bp:4}bp current={cur_bp:4}bp diff={diff:+4}bp")

    print(f"\n--- Summary ---")
    print(f"Total trades: {len(trades)}")
    exact_trades = sum(1 for t in trades if t['is_exact'])
    print(f"  Exact targeting: {exact_trades}")
    print(f"  Random sizing: {len(trades) - exact_trades}")

    return trades


def get_real_quote_for_trade(
    sell_symbol: str,
    buy_symbol: str,
    trade_size: int,
    sell_token: TokenDetails,
    buy_token: Optional[TokenDetails] = None,  # For dust output validation
    _is_fallback_leg: bool = False,  # Guard against infinite recursion in ICP fallback
    num_quotes: int = 5  # Number of quote points (default 5 = 20/40/60/80/100%)
) -> Tuple[int, int, str, Tuple[int, int], str]:
    """
    Get real DEX quote for a trade using findBestExecution logic.
    Returns: (amount_out, slippage_bp, route_type, split_pct, actual_buy_symbol)

    When direct pair fails and ICP fallback is used, routes to ICP only (one-leg).
    Matches treasury.mo: creates ICP overweight that corrects in next cycle.
    """
    # Calculate ICP equivalent for quote fetching
    trade_value_icp = (trade_size * sell_token.price_in_icp) // (10 ** sell_token.decimals)
    amount_icp = max(1, trade_value_icp // 100_000_000)  # Convert e8s to ICP units

    # Quote amounts based on num_quotes (e.g., 5 = 20%, 40%, 60%, 80%, 100%)
    # Each quote is at (i+1)/num_quotes of the trade size
    amounts = [trade_size * (i + 1) // num_quotes for i in range(num_quotes)]

    # Check for ICPSwap pool
    pool_key = (sell_symbol, buy_symbol)
    has_icpswap_pool = pool_key in ICPSWAP_POOLS

    # Fetch ALL quotes in parallel
    kong_futures = [quote_executor.submit(get_kong_quote, sell_symbol, buy_symbol, amt) for amt in amounts]

    if has_icpswap_pool:
        pool_id, zero_for_one = ICPSWAP_POOLS[pool_key]
        sqrt_price = get_pool_metadata(pool_id)
        icp_futures = [quote_executor.submit(get_icpswap_quote, pool_id, amt, zero_for_one, sqrt_price) for amt in amounts]
    else:
        icp_futures = None

    # Collect results
    kong_quotes = [f.result() for f in kong_futures]

    if icp_futures:
        icp_quotes = [f.result() for f in icp_futures]
    else:
        icp_quotes = [Quote(amt, 0, 10000, False, "no_pool") for amt in amounts]

    # Dust output validation: mark quotes as invalid if output < 1% of expected
    # Matches treasury.mo fix for Kong returning amount=1 with slippage=0%
    if buy_token is not None and buy_token.price_in_icp > 0:
        def validate_dust(quote: Quote, amount_in: int) -> Quote:
            """Mark quote invalid if output is suspiciously low (dust)."""
            if quote.amount_out == 0:
                return quote
            # Calculate expected output at spot price
            sell_value_e8s = (amount_in * sell_token.price_in_icp) // (10 ** sell_token.decimals)
            expected_out = (sell_value_e8s * (10 ** buy_token.decimals)) // buy_token.price_in_icp
            min_expected = max(100, expected_out // 100)  # At least 100 units, or 1% of expected
            if quote.amount_out < min_expected:
                # Mark as invalid - dust output
                return Quote(quote.amount_in, quote.amount_out, 10000, False, "dust_output")
            return quote

        kong_quotes = [validate_dust(q, amounts[i]) for i, q in enumerate(kong_quotes)]
        icp_quotes = [validate_dust(q, amounts[i]) for i, q in enumerate(icp_quotes)]

    # Check if any exchange works
    kong_works = any(q.valid for q in kong_quotes)
    icp_works = any(q.valid for q in icp_quotes)

    if not kong_works and not icp_works:
        # Step 1: Try REDUCED amount estimation (no extra API call)
        trade_value_icp_amount = (trade_size * sell_token.price_in_icp) // (10 ** sell_token.decimals) // 100_000_000
        icp_involved = sell_symbol == "ICP" or buy_symbol == "ICP"
        max_icp, best_exch = estimate_max_tradeable_icp(kong_quotes, icp_quotes, max(1, trade_value_icp_amount), icp_involved, num_quotes)
        if max_icp > 0 and (icp_involved or max_icp >= MIN_TRADE_ICP):
            # Calculate reduced trade size and verify
            reduced_trade_size = int(trade_size * max_icp / max(1, trade_value_icp_amount))

            # Try both exchanges for verify - if first fails, try second
            # This handles cases where bulk quotes failed for both but one exchange might work
            verify = None
            actual_exch = best_exch

            for exch in ([best_exch, "ICP" if best_exch == "Kong" else "Kong"]):
                if exch == "Kong":
                    test_verify = get_kong_quote(sell_symbol, buy_symbol, reduced_trade_size)
                else:
                    if pool_key in ICPSWAP_POOLS:
                        pool_id, zero_for_one = ICPSWAP_POOLS[pool_key]
                        sqrt_price = get_pool_metadata(pool_id)
                        test_verify = get_icpswap_quote(pool_id, reduced_trade_size, zero_for_one, sqrt_price)
                    else:
                        test_verify = Quote(reduced_trade_size, 0, 10000, False, "no_pool")

                # Check if this verify is valid
                if test_verify.amount_out > 0 and test_verify.slippage_bp <= MAX_SLIPPAGE_BP:
                    verify = test_verify
                    actual_exch = exch
                    break

            # Return if we found a valid verify quote
            if verify is not None:
                reduced_route = f"REDUCED_{actual_exch[0]}"  # REDUCED_K or REDUCED_I
                # Calculate actual percentage traded: (max_icp / trade_value_icp_amount) * 10000
                reduced_pct_bp = (max_icp * 10000) // max(1, trade_value_icp_amount)
                split = (reduced_pct_bp, 0) if actual_exch == "Kong" else (0, reduced_pct_bp)
                return (verify.amount_out, verify.slippage_bp, reduced_route, split, buy_symbol)

        # Step 2: Try ONE-LEG ICP fallback route (sell -> ICP only)
        # Matches treasury.mo: creates ICP overweight that corrects in next cycle
        # Only attempt if not already in a fallback leg (prevents infinite recursion)
        if not _is_fallback_leg and sell_symbol != "ICP" and buy_symbol != "ICP":
            # Create ICP token for dust validation (ICP price is 1e8 e8s, 8 decimals)
            icp_token_for_validation = TokenDetails(
                principal="ryjl3-tyaaa-aaaaa-aaaba-cai",
                symbol="ICP", decimals=8, balance=0,
                price_in_icp=100_000_000, target_allocation_bp=0
            )
            # Only leg: sell_symbol -> ICP
            leg1 = get_real_quote_for_trade(sell_symbol, "ICP", trade_size, sell_token, icp_token_for_validation, _is_fallback_leg=True, num_quotes=num_quotes)
            if leg1[0] > 0:
                # Return ICP as the actual buy (not original buy_symbol)
                # Route format depends on whether the sell->ICP leg was a split/partial
                if leg1[2].startswith("SPLIT"):
                    # Preserve split percentages: ICP_FB_SPLIT_60_40
                    kong_pct_, icp_pct_ = leg1[3]
                    route_type = f"ICP_FB_SPLIT_{kong_pct_//100}_{icp_pct_//100}"
                elif leg1[2].startswith("PARTIAL"):
                    # Preserve partial percentages: ICP_FB_PARTIAL_60_20
                    kong_pct_, icp_pct_ = leg1[3]
                    route_type = f"ICP_FB_PARTIAL_{kong_pct_//100}_{icp_pct_//100}"
                    if leg1[2].endswith("_INTERP"):
                        route_type += "_INTERP"
                else:
                    # Single exchange: ICP_FB:K or ICP_FB:I
                    def leg_code(r):
                        if r.startswith("KONG"): return "K"
                        if r.startswith("ICP_"): return "I"
                        if r.startswith("REDUCED"): return "R"
                        return "?"
                    route_type = f"ICP_FB:{leg_code(leg1[2])}"
                return (leg1[0], leg1[1], route_type, leg1[3], "ICP")  # actual_buy = "ICP"

        return (0, 10000, "FAILURE", (0, 0), buy_symbol)

    # Run the algorithm (same as treasury findBestExecution)
    algo_result = run_algorithm(kong_quotes, icp_quotes, num_quotes)
    result_type = algo_result[0]

    # Handle PARTIAL_CANDIDATES - select best partial based on MIN_TRADE_ICP filtering
    if result_type == 'PARTIAL_CANDIDATES':
        partial_candidates = algo_result[1]  # List of Scenario objects
        step_bp = algo_result[2]

        # Calculate trade value in ICP for filtering
        trade_value_icp = (trade_size * sell_token.price_in_icp) // (10 ** sell_token.decimals * 100_000_000)
        icp_involved = sell_symbol == "ICP" or buy_symbol == "ICP"

        def partial_value_icp(p):
            """Calculate ICP value of a partial split"""
            total_pct = p.kong_pct + p.icp_pct
            return (trade_value_icp * total_pct) // 10000

        def partial_total_pct(p):
            """Calculate total percentage of a partial split"""
            return p.kong_pct + p.icp_pct

        def combined_slippage(p):
            """Calculate combined slippage for a partial"""
            return p.kong_slip_bp + p.icp_slip_bp

        # Step A: Filter to partials meeting MIN_TRADE_ICP AND MIN_PARTIAL_TOTAL_BP
        # This prevents tiny partials like PARTIAL_10_10 (20% total)
        valid_partials = [p for p in partial_candidates
                         if partial_value_icp(p) >= MIN_TRADE_ICP
                         and partial_total_pct(p) >= MIN_PARTIAL_TOTAL_BP]

        # Step B: If we have valid partials, pick best by slippage
        if valid_partials:
            valid_partials.sort(key=combined_slippage)
            best = valid_partials[0]
        # Step C: If no valid partials AND ICP is in pair, allow partials meeting min total only
        elif icp_involved and partial_candidates:
            # Still enforce MIN_PARTIAL_TOTAL_BP even for ICP pairs (prevent tiny partials like 10/10)
            icp_valid = [p for p in partial_candidates if partial_total_pct(p) >= MIN_PARTIAL_TOTAL_BP]
            if icp_valid:
                icp_valid.sort(key=combined_slippage)
                best = icp_valid[0]
            else:
                best = None  # Fall through to REDUCED/NO_PATH
        else:
            # No partials meet criteria for non-ICP pair - fall through to NO_PATH
            best = None

        if best is not None:
            # Check for interpolation: if the 2 best partials by slippage are adjacent, interpolate
            pool = valid_partials if valid_partials else partial_candidates
            others = [p for p in pool if p != best]

            was_interpolated = False
            if others:
                # Get second-best by slippage
                others.sort(key=combined_slippage)
                second = others[0]

                # Check if best and second-best are adjacent (differ by one step in both directions)
                kong_diff = abs(best.kong_pct - second.kong_pct)
                icp_diff = abs(best.icp_pct - second.icp_pct)

                if kong_diff == step_bp and icp_diff == step_bp:
                    # Adjacent! Interpolate between them
                    avg_kong_slip = (best.kong_slip_bp + second.kong_slip_bp) / 2
                    avg_icp_slip = (best.icp_slip_bp + second.icp_slip_bp) / 2
                    total_slip = avg_kong_slip + avg_icp_slip

                    if total_slip > 0:
                        kong_ratio = avg_icp_slip / total_slip
                        low_kong = min(best.kong_pct, second.kong_pct)
                        high_kong = max(best.kong_pct, second.kong_pct)
                        interp_kong = low_kong + int(kong_ratio * (high_kong - low_kong))

                        # Calculate interpolated ICP pct (may not sum to same total as best/second)
                        low_icp = min(best.icp_pct, second.icp_pct)
                        high_icp = max(best.icp_pct, second.icp_pct)
                        # Use inverse ratio for ICP (if kong goes up, icp goes down)
                        interp_icp = high_icp - int(kong_ratio * (high_icp - low_icp))

                        # Update best with interpolated values
                        best = Scenario(
                            f"PARTIAL_{interp_kong//100}_{interp_icp//100}",
                            interp_kong, interp_icp, best.total_out,
                            best.kong_slip_bp, best.icp_slip_bp
                        )
                        was_interpolated = True

            # Get actual output from quotes
            # Build pct_to_idx mapping dynamically based on num_quotes
            # For 5 quotes: {2000: 0, 4000: 1, 6000: 2, 8000: 3, 10000: 4}
            # For 10 quotes: {1000: 0, 2000: 1, 3000: 2, ..., 10000: 9}
            pct_to_idx = {(i + 1) * step_bp: i for i in range(num_quotes)}
            kong_idx = pct_to_idx.get(best.kong_pct, -1)
            icp_idx = pct_to_idx.get(best.icp_pct, -1)

            # For interpolated values, use closest quote indices
            if kong_idx == -1:
                kong_idx = min(range(num_quotes), key=lambda i: abs((i+1)*step_bp - best.kong_pct))
            if icp_idx == -1:
                icp_idx = min(range(num_quotes), key=lambda i: abs((i+1)*step_bp - best.icp_pct))

            actual_kong = kong_quotes[kong_idx]
            actual_icp = icp_quotes[icp_idx]

            actual_out = 0
            if actual_kong.valid:
                actual_out += actual_kong.amount_out
            if actual_icp.valid:
                actual_out += actual_icp.amount_out

            if actual_out > 0:
                kong_weight = actual_kong.amount_out / actual_out if actual_kong.valid else 0
                icp_weight = actual_icp.amount_out / actual_out if actual_icp.valid else 0
                actual_slippage = int(
                    actual_kong.slippage_bp * kong_weight + actual_icp.slippage_bp * icp_weight
                )
            else:
                actual_slippage = 10000

            route = f"PARTIAL_{best.kong_pct//100}_{best.icp_pct//100}"
            if was_interpolated:
                route += "_INTERP"

            return (actual_out, actual_slippage, route, (best.kong_pct, best.icp_pct), buy_symbol)
        else:
            # No valid partials - set result_type to NO_PATH to fall through
            result_type = 'NO_PATH'
    else:
        # Unpack normal result for non-partial cases
        kong_pct, icp_pct, expected_out, _, was_interpolated = algo_result[1], algo_result[2], algo_result[3], algo_result[4], algo_result[5]

    if result_type == 'NO_PATH':
        # Algorithm found no valid scenarios (all exceed slippage)
        # Try REDUCED amount estimation before giving up
        trade_value_icp_amount = (trade_size * sell_token.price_in_icp) // (10 ** sell_token.decimals) // 100_000_000
        icp_involved = sell_symbol == "ICP" or buy_symbol == "ICP"
        max_icp, best_exch = estimate_max_tradeable_icp(kong_quotes, icp_quotes, max(1, trade_value_icp_amount), icp_involved, num_quotes)
        if max_icp > 0 and (icp_involved or max_icp >= MIN_TRADE_ICP):
            # Calculate reduced trade size and verify
            reduced_trade_size = int(trade_size * max_icp / max(1, trade_value_icp_amount))

            # Try both exchanges for verify - if first fails, try second
            # This handles cases where bulk quotes failed for both but one exchange might work
            verify = None
            actual_exch = best_exch

            for exch in ([best_exch, "ICP" if best_exch == "Kong" else "Kong"]):
                if exch == "Kong":
                    test_verify = get_kong_quote(sell_symbol, buy_symbol, reduced_trade_size)
                else:
                    if pool_key in ICPSWAP_POOLS:
                        pool_id, zero_for_one = ICPSWAP_POOLS[pool_key]
                        sqrt_price = get_pool_metadata(pool_id)
                        test_verify = get_icpswap_quote(pool_id, reduced_trade_size, zero_for_one, sqrt_price)
                    else:
                        test_verify = Quote(reduced_trade_size, 0, 10000, False, "no_pool")

                # Check if this verify is valid
                if test_verify.amount_out > 0 and test_verify.slippage_bp <= MAX_SLIPPAGE_BP:
                    verify = test_verify
                    actual_exch = exch
                    break

            # Return if we found a valid verify quote
            if verify is not None:
                reduced_route = f"REDUCED_{actual_exch[0]}"  # REDUCED_K or REDUCED_I
                # Calculate actual percentage traded: (max_icp / trade_value_icp_amount) * 10000
                reduced_pct_bp = (max_icp * 10000) // max(1, trade_value_icp_amount)
                split = (reduced_pct_bp, 0) if actual_exch == "Kong" else (0, reduced_pct_bp)
                return (verify.amount_out, verify.slippage_bp, reduced_route, split, buy_symbol)

        # Try ONE-LEG ICP fallback (sell -> ICP only)
        # Matches treasury.mo: creates ICP overweight that corrects in next cycle
        if not _is_fallback_leg and sell_symbol != "ICP" and buy_symbol != "ICP":
            # Create ICP token for dust validation
            icp_token_for_validation = TokenDetails(
                principal="ryjl3-tyaaa-aaaaa-aaaba-cai",
                symbol="ICP", decimals=8, balance=0,
                price_in_icp=100_000_000, target_allocation_bp=0
            )
            # Only leg: sell_symbol -> ICP
            leg1 = get_real_quote_for_trade(sell_symbol, "ICP", trade_size, sell_token, icp_token_for_validation, _is_fallback_leg=True, num_quotes=num_quotes)
            if leg1[0] > 0:
                # Return ICP as the actual buy (one-leg like treasury.mo)
                # Route format depends on whether the sell->ICP leg was a split/partial
                if leg1[2].startswith("SPLIT"):
                    # Preserve split percentages: ICP_FB_SPLIT_60_40
                    kong_pct__, icp_pct__ = leg1[3]
                    route_type = f"ICP_FB_SPLIT_{kong_pct__//100}_{icp_pct__//100}"
                elif leg1[2].startswith("PARTIAL"):
                    # Preserve partial percentages: ICP_FB_PARTIAL_60_20
                    kong_pct__, icp_pct__ = leg1[3]
                    route_type = f"ICP_FB_PARTIAL_{kong_pct__//100}_{icp_pct__//100}"
                    if leg1[2].endswith("_INTERP"):
                        route_type += "_INTERP"
                else:
                    # Single exchange: ICP_FB:K or ICP_FB:I
                    def leg_code(r):
                        if r.startswith("KONG"): return "K"
                        if r.startswith("ICP_"): return "I"
                        if r.startswith("REDUCED"): return "R"
                        return "?"
                    route_type = f"ICP_FB:{leg_code(leg1[2])}"
                return (leg1[0], leg1[1], route_type, leg1[3], "ICP")

        return (0, 10000, "NO_PATH", (0, 0), buy_symbol)

    # Get actual output and slippage at the selected split
    if result_type == 'SINGLE_KONG':
        # 100% Kong quote
        actual_out = kong_quotes[4].amount_out
        actual_slippage = kong_quotes[4].slippage_bp
        route = "KONG_100"
    elif result_type == 'SINGLE_ICP':
        # 100% ICPSwap quote
        actual_out = icp_quotes[4].amount_out
        actual_slippage = icp_quotes[4].slippage_bp
        route = "ICP_100"
    else:
        # Full split (sums to 100%) - USE EXISTING QUOTES instead of refetching
        # Algorithm returns percentages: 2000, 4000, 6000, 8000
        # These map to our quote indices: 0 (20%), 1 (40%), 2 (60%), 3 (80%)
        # Note: PARTIAL_CANDIDATES are handled separately above
        pct_to_idx = {2000: 0, 4000: 1, 6000: 2, 8000: 3, 10000: 4}
        kong_idx = pct_to_idx.get(kong_pct, 4)
        icp_idx = pct_to_idx.get(icp_pct, 4)

        actual_kong = kong_quotes[kong_idx]
        actual_icp = icp_quotes[icp_idx]

        actual_out = 0
        if actual_kong.valid:
            actual_out += actual_kong.amount_out
        if actual_icp.valid:
            actual_out += actual_icp.amount_out

        # Combined slippage - weighted average
        if actual_out > 0:
            kong_weight = actual_kong.amount_out / actual_out if actual_kong.valid else 0
            icp_weight = actual_icp.amount_out / actual_out if actual_icp.valid else 0
            actual_slippage = int(
                actual_kong.slippage_bp * kong_weight + actual_icp.slippage_bp * icp_weight
            )
        else:
            actual_slippage = 10000

        # Full split (sums to 100%)
        route = f"SPLIT_{kong_pct//100}_{icp_pct//100}"
        if was_interpolated:
            route += "_INTERP"

    return (actual_out, actual_slippage, route, (kong_pct, icp_pct), buy_symbol)


def run_full_trading_cycle_with_real_quotes(num_cycles: int = 5, use_production_data: bool = False) -> List[Dict]:
    """
    Run a complete trading cycle test matching treasury.mo logic WITH REAL DEX QUOTES.
    This combines:
    - Full treasury portfolio allocation logic
    - Real-time DEX quotes from KongSwap and ICPSwap
    - Actual slippage-based adjustments
    - Price updates based on real quote outputs

    Args:
        num_cycles: Number of trading cycles to run
        use_production_data: If True, fetch REAL prices/decimals/config from production canisters
                            (but keep random target allocations for test diversity)

    Returns list of trade decisions made.
    """
    global MAX_SLIPPAGE_BP  # May be updated from production config

    # Fetch ICPSwap pools first
    if not ICPSWAP_POOLS:
        fetch_icpswap_pools()

    # Pre-warm metadata cache for all known pools
    print("  Pre-warming pool metadata cache...")
    meta_futures = [quote_executor.submit(get_pool_metadata, pool_id)
                    for pool_id, _ in ICPSWAP_POOLS.values()]
    for f in meta_futures:
        try:
            f.result()
        except:
            pass

    # Initialize portfolio
    global MAX_SLIPPAGE_BP  # Need to update the global for quote validation
    if use_production_data:
        # Use REAL prices/decimals/config from production, but RANDOM target allocations
        portfolio, config = initialize_portfolio_from_production(use_real_balances=True)
        # Update global MAX_SLIPPAGE_BP to match production (including any user overrides)
        MAX_SLIPPAGE_BP = config['max_slippage_bp']
    else:
        # Use simulated data (old behavior)
        portfolio = initialize_portfolio_with_random_allocations()
        config = TREASURY_CONFIG.copy()
        config['num_quotes'] = 5  # Default for simulated mode

    num_quotes = config.get('num_quotes', 5)

    trades = []

    # Stats tracking for live status line
    stats = {
        'kong': 0, 'icp': 0, 'split': 0, 'split_interp': 0, 'partial': 0, 'partial_interp': 0, 'reduced': 0,  # Direct routes
        'icp_fb': 0, 'icp_fb_split': 0, 'icp_fb_partial': 0, 'icp_fb_reduced': 0,  # Fallback routes
        'fail': 0,
        'fb_detail': {},      # Track fallback exchange details e.g. {'ICP_FB:K': 3, 'ICP_FB:I': 1}
        'reduced_detail': {}, # Track reduced exchange details e.g. {'REDUCED_K': 2, 'REDUCED_I': 1}
        'split_detail': {},   # Track split details e.g. {'SPLIT_60_40': 2, 'SPLIT_40_60_INTERP': 1}
        'partial_detail': {}, # Track partial split details e.g. {'PARTIAL_60_20': 1, 'PARTIAL_50_30_INTERP': 1}
        'slippages': [],      # Track all slippages for running average
        'last_slip': 0,       # Last slippage for display
        'avg_slip': 0,        # Running average slippage
        'last_fail_reason': '',  # Track last fail reason for display
    }
    trade_count = 0
    total_expected = num_cycles * config['max_trade_attempts']

    print("\n" + "=" * 80)
    print("FULL TRADING CYCLE TEST WITH REAL DEX QUOTES")
    if use_production_data:
        print("  (Using REAL prices/config from production canisters)")
    print("=" * 80)
    print("\nNOTE: Trades use REAL DEX quotes. Target allocations are RANDOM for testing.")
    print(f"\nInitial Portfolio ({len(portfolio.tokens)} tokens):")
    print(f"  Total Value: {portfolio.total_value_icp / 1e8:.2f} ICP (simulated)")
    print("\nToken Allocations:")
    for symbol, details in sorted(portfolio.tokens.items()):
        value = (details.balance * details.price_in_icp) // (10 ** details.decimals)
        current_bp = (value * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
        print(f"  {symbol:8} target={details.target_allocation_bp:4}bp current={current_bp:4}bp diff={details.target_allocation_bp - current_bp:+4}bp")

    # Calculate initial imbalance for convergence analysis
    initial_trade_diffs = calculate_trade_requirements(portfolio, config['min_allocation_diff_bp'])
    initial_imbalance = sum(abs(d) for _, d, _ in initial_trade_diffs)

    print(f"\nStarting trades... (initial imbalance: {initial_imbalance}bp)\n")

    for cycle in range(num_cycles):
        for attempt in range(config['max_trade_attempts']):
            # Step 1: Calculate trade requirements (matches treasury.mo calculateTradeRequirements)
            trade_diffs = calculate_trade_requirements(
                portfolio,
                config['min_allocation_diff_bp']
            )

            if not trade_diffs:
                break

            # Step 2: Select trading pair (matches treasury.mo selectTradingPair - weighted random)
            pair = select_trading_pair(trade_diffs)
            if not pair:
                continue

            sell_symbol, buy_symbol, sell_diff, buy_diff = pair

            # Step 3: Determine trade size strategy (matches treasury.mo shouldUseExactTargeting)
            use_exact = should_use_exact_targeting(
                sell_diff, buy_diff,
                portfolio.total_value_icp,
                config['max_trade_value_icp']
            )

            sell_token = portfolio.tokens[sell_symbol]
            buy_token = portfolio.tokens[buy_symbol]

            if use_exact:
                trade_size, is_exact = calculate_exact_target_trade_size(
                    sell_token, buy_token,
                    portfolio.total_value_icp,
                    sell_diff, buy_diff,
                    config['max_trade_value_icp'],
                    config['min_trade_value_icp']
                )
                sizing_method = "EXACT" if is_exact else "RANDOM (fallback)"
            else:
                trade_size = calculate_trade_size_min_max(
                    config['min_trade_value_icp'],
                    config['max_trade_value_icp'],
                    sell_token
                )
                is_exact = False
                sizing_method = "RANDOM"

            # Cap trade size at available balance (can't sell more than we have)
            if trade_size > sell_token.balance:
                trade_size = sell_token.balance

            # Skip if trade size is 0 (no balance left)
            if trade_size == 0:
                continue

            trade_value_icp = (trade_size * sell_token.price_in_icp) // (10 ** sell_token.decimals)

            # Get buy token for dust validation
            buy_token = portfolio.tokens.get(buy_symbol)

            # Step 4: GET REAL DEX QUOTES (this is the key difference from --cycle mode)
            amount_out, slippage_bp, route_type, split_pct, actual_buy_symbol = get_real_quote_for_trade(
                sell_symbol, buy_symbol, trade_size, sell_token, buy_token, num_quotes=num_quotes
            )

            # actual_buy_symbol may differ from buy_symbol when ICP fallback is used
            # In ICP fallback, we route sell_symbol->ICP instead of sell_symbol->buy_symbol
            actual_buy_token = portfolio.tokens[actual_buy_symbol]

            # Helper to print 5-line status display for failures
            def print_fail_status(fail_reason: str):
                import sys
                is_tty = sys.stdout.isatty()
                if is_tty and trade_count > 1:
                    print("\033[5A", end="")
                print(f"[{trade_count}/{total_expected}] {sell_symbol} -> {buy_symbol} FAILED: {fail_reason}" + ("\033[K" if is_tty else ""))
                print(f"  Direct: Kong:{stats['kong']} ICPSwap:{stats['icp']} Split:{stats['split']}(+{stats['split_interp']}i) Partial:{stats['partial']}(+{stats['partial_interp']}i) Reduced:{stats['reduced']}" + ("\033[K" if is_tty else ""))
                print(f"  Fallback: {stats['icp_fb']}(+{stats['icp_fb_split']}spl +{stats['icp_fb_partial']}par +{stats['icp_fb_reduced']}red) | Failed:{stats['fail']} (last: {fail_reason})" + ("\033[K" if is_tty else ""))
                print(f"  Slippage: last={stats['last_slip']}bp avg={stats['avg_slip']}bp" + ("\033[K" if is_tty else ""))
                print(f"  Imbalance: (calculating...)" + ("\033[K" if is_tty else ""), flush=True)

            # Calculate current imbalance for fail tracking (no balance change on failure)
            current_imb_for_fail = 0
            for sym, det in portfolio.tokens.items():
                val = (det.balance * det.price_in_icp) // (10 ** det.decimals)
                cur_bp = (val * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
                current_imb_for_fail += abs(det.target_allocation_bp - cur_bp)

            if amount_out == 0:
                fail_reason = f"NO_QUOTES ({route_type})"
                stats['fail'] += 1
                stats['last_fail_reason'] = fail_reason
                trade_count += 1
                # Record failed trade (imbalance unchanged since no trade executed)
                trades.append({
                    'cycle': cycle + 1, 'attempt': attempt + 1,
                    'sell': sell_symbol, 'buy': buy_symbol, 'intended_buy': buy_symbol,
                    'amount_sold': trade_size, 'amount_bought': 0,
                    'intended_icp': trade_value_icp // 100_000_000,  # Intended ICP value
                    'actual_icp': 0,  # Failed trade - no actual ICP traded
                    'is_exact': use_exact, 'slippage_bp': 10000,
                    'sell_diff_bp': sell_diff, 'buy_diff_bp': buy_diff,
                    'route': f"FAIL_{route_type}", 'split_pct': (0, 0),
                    'fail_reason': fail_reason,
                    'imbalance_before': current_imb_for_fail,
                    'imbalance_after': current_imb_for_fail,  # No change on failure
                    'imbalance_change': 0,
                })
                print_fail_status(fail_reason)
                continue

            # Check max slippage (circuit breaker)
            if slippage_bp > config['max_slippage_bp']:
                fail_reason = f"HIGH_SLIPPAGE ({slippage_bp}bp > {config['max_slippage_bp']}bp)"
                stats['fail'] += 1
                stats['last_fail_reason'] = fail_reason
                trade_count += 1
                # Record failed trade (imbalance unchanged since no trade executed)
                trades.append({
                    'cycle': cycle + 1, 'attempt': attempt + 1,
                    'sell': sell_symbol, 'buy': buy_symbol, 'intended_buy': buy_symbol,
                    'amount_sold': trade_size, 'amount_bought': 0,
                    'intended_icp': trade_value_icp // 100_000_000,  # Intended ICP value
                    'actual_icp': 0,  # Failed trade - no actual ICP traded
                    'is_exact': use_exact, 'slippage_bp': slippage_bp,
                    'sell_diff_bp': sell_diff, 'buy_diff_bp': buy_diff,
                    'route': "FAIL_HIGH_SLIPPAGE", 'split_pct': (0, 0),
                    'fail_reason': fail_reason,
                    'imbalance_before': current_imb_for_fail,
                    'imbalance_after': current_imb_for_fail,  # No change on failure
                    'imbalance_change': 0,
                })
                print_fail_status(fail_reason)
                continue

            # Step 5: Apply slippage adjustment for exact targeting
            # (matches treasury.mo lines 3930-3973)
            final_size, adj_expected, min_out = adjust_trade_for_slippage(
                trade_size, slippage_bp, is_exact,
                amount_out,
                config['max_slippage_bp']
            )

            if is_exact and final_size != trade_size:
                # Re-fetch quote at adjusted size
                adj_amount_out, adj_slippage_bp, adj_route, _, adj_actual_buy = get_real_quote_for_trade(
                    sell_symbol, buy_symbol, final_size, sell_token, buy_token, num_quotes=num_quotes
                )
                if adj_amount_out > 0:
                    amount_out = adj_amount_out
                    slippage_bp = adj_slippage_bp
                    route_type = adj_route
                    actual_buy_symbol = adj_actual_buy
                    actual_buy_token = portfolio.tokens[actual_buy_symbol]

            # Step 6: Record trade and update portfolio state
            # Calculate imbalance BEFORE the trade for tracking
            imbalance_before = 0
            for sym, det in portfolio.tokens.items():
                val = (det.balance * det.price_in_icp) // (10 ** det.decimals)
                cur_bp = (val * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
                imbalance_before += abs(det.target_allocation_bp - cur_bp)

            # Calculate actual ICP traded based on route type
            intended_icp = trade_value_icp // 100_000_000
            if "PARTIAL" in route_type:
                # For PARTIAL routes (including ICP_FB_PARTIAL), actual = intended * total_pct / 10000
                kong_pct, icp_pct = split_pct
                total_pct = kong_pct + icp_pct
                actual_icp = (intended_icp * total_pct) // 10000
            elif "REDUCED" in route_type or route_type == "ICP_FB:R":
                # For REDUCED routes (REDUCED_K, REDUCED_I, ICP_FB:R), split_pct contains the reduced percentage
                kong_pct, icp_pct = split_pct
                reduced_pct = kong_pct if kong_pct > 0 else icp_pct
                actual_icp = (intended_icp * reduced_pct) // 10000
            else:
                # For normal trades (KONG_100, ICP_100, SPLIT, ICP_FB:K, ICP_FB:I, ICP_FB_SPLIT), actual = intended
                actual_icp = intended_icp

            trade_record = {
                'cycle': cycle + 1,
                'attempt': attempt + 1,
                'sell': sell_symbol,
                'buy': actual_buy_symbol,  # Use actual buy (may be ICP if fallback was used)
                'intended_buy': buy_symbol,  # Original intended buy for reference
                'amount_sold': final_size,
                'amount_bought': amount_out,
                'intended_icp': intended_icp,  # Intended ICP value
                'actual_icp': actual_icp,  # Actual ICP traded (less for PARTIAL/REDUCED)
                'is_exact': is_exact,
                'slippage_bp': slippage_bp,
                'sell_diff_bp': sell_diff,
                'buy_diff_bp': buy_diff,
                'route': route_type,
                'split_pct': split_pct,
                'fail_reason': '',  # Empty for successful trades
                'imbalance_before': imbalance_before,
                'imbalance_after': 0,  # Will be updated after balance changes
                'imbalance_change': 0,  # Will be updated after balance changes
            }
            trades.append(trade_record)

            # Update stats for status line - handle new detailed route formats
            if route_type == "KONG_100":
                stats['kong'] += 1
            elif route_type == "ICP_100":
                stats['icp'] += 1
            elif route_type.startswith("PARTIAL"):
                if route_type.endswith("_INTERP"):
                    stats['partial_interp'] += 1
                else:
                    stats['partial'] += 1
                # Track partial split details
                if route_type not in stats['partial_detail']:
                    stats['partial_detail'][route_type] = 0
                stats['partial_detail'][route_type] += 1
            elif route_type.startswith("SPLIT"):
                if route_type.endswith("_INTERP"):
                    stats['split_interp'] += 1
                else:
                    stats['split'] += 1
                # Track split details
                if route_type not in stats['split_detail']:
                    stats['split_detail'][route_type] = 0
                stats['split_detail'][route_type] += 1
            elif route_type.startswith("REDUCED"):
                stats['reduced'] += 1
                # Track which exchange was used for REDUCED
                if route_type not in stats['reduced_detail']:
                    stats['reduced_detail'][route_type] = 0
                stats['reduced_detail'][route_type] += 1
            elif route_type.startswith("ICP_FB_SPLIT"):
                stats['icp_fb_split'] += 1
                # Track which exchanges were used
                if route_type not in stats['fb_detail']:
                    stats['fb_detail'][route_type] = 0
                stats['fb_detail'][route_type] += 1
            elif route_type.startswith("ICP_FB_PARTIAL"):
                stats['icp_fb_partial'] += 1
                # Track which exchanges were used
                if route_type not in stats['fb_detail']:
                    stats['fb_detail'][route_type] = 0
                stats['fb_detail'][route_type] += 1
            elif route_type == "ICP_FB:R":
                # ICP fallback via REDUCED amount
                stats['icp_fb_reduced'] += 1
                if route_type not in stats['fb_detail']:
                    stats['fb_detail'][route_type] = 0
                stats['fb_detail'][route_type] += 1
            elif route_type.startswith("ICP_FB"):
                stats['icp_fb'] += 1
                # Track which exchanges were used
                if route_type not in stats['fb_detail']:
                    stats['fb_detail'][route_type] = 0
                stats['fb_detail'][route_type] += 1
            trade_count += 1

            # Track slippage for running avg/last display
            stats['slippages'].append(slippage_bp)
            stats['last_slip'] = slippage_bp
            stats['avg_slip'] = sum(stats['slippages']) // len(stats['slippages'])

            # Update balances based on trade execution
            # NOTE: We do NOT update prices here. In production, prices come from external
            # sources (oracle, price discovery), not from trade execution ratios.
            # Updating prices from trades caused cascade failures due to price drift.
            sell_token.balance -= final_size
            actual_buy_token.balance += amount_out

            # Recalculate total portfolio value with fixed prices
            portfolio.total_value_icp = calculate_total_portfolio_value(portfolio)

            # Print live status - refreshing 4-line display with detailed breakdown
            # Translate route_type to human-readable format
            def route_readable(rt):
                if rt == "KONG_100": return "Kong 100%"
                if rt == "ICP_100": return "ICPSwap 100%"
                if rt.startswith("PARTIAL"):
                    # Format: PARTIAL_60_20 -> Partial 60%+20%=80%
                    parts = rt.split("_")
                    if len(parts) >= 3:
                        kong_pct, icp_pct = parts[1], parts[2]
                        total = int(kong_pct) + int(icp_pct)
                        return f"Partial {kong_pct}%+{icp_pct}%={total}%"
                    return f"Partial ({rt})"
                if rt.startswith("SPLIT"): return f"Split ({rt})"
                if rt.startswith("REDUCED_K"): return "Reduced via Kong"
                if rt.startswith("REDUCED_I"): return "Reduced via ICPSwap"
                if rt.startswith("ICP_FB_SPLIT_"):
                    # New format: ICP_FB_SPLIT_60_40 (sell -> ICP via split)
                    parts = rt.split("_")
                    if len(parts) >= 5:
                        kong_pct, icp_pct = parts[3], parts[4]
                        return f"ICP Fallback Split (Kong:{kong_pct}% ICPSwap:{icp_pct}%)"
                    return "ICP Fallback Split"
                if rt.startswith("ICP_FB_PARTIAL_"):
                    # New format: ICP_FB_PARTIAL_60_20 (sell -> ICP via partial)
                    parts = rt.replace("_INTERP", "").split("_")
                    if len(parts) >= 5:
                        kong_pct, icp_pct = parts[3], parts[4]
                        total = int(kong_pct) + int(icp_pct)
                        interp = " (interp)" if "_INTERP" in rt else ""
                        return f"ICP Fallback Partial ({kong_pct}%+{icp_pct}%={total}%){interp}"
                    return "ICP Fallback Partial"
                if rt.startswith("ICP_FB:"):
                    # One-leg format: ICP_FB:K (sell -> ICP)
                    leg = rt.split(":")[1] if ":" in rt else "?"
                    leg_names = {"K": "Kong", "I": "ICPSwap", "R": "Reduced"}
                    return f"ICP Fallback via {leg_names.get(leg, leg)}"
                return rt

            # Build detailed breakdown string for fallback routes
            def fb_breakdown():
                if not stats['fb_detail']:
                    return ""
                parts = []
                for rt, cnt in sorted(stats['fb_detail'].items()):
                    if rt.startswith("ICP_FB_SPLIT_"):
                        # New format: ICP_FB_SPLIT_60_40
                        parts.append(f"Split:{cnt}")
                    elif ":" in rt:
                        # One-leg format: ICP_FB:K (sell -> ICP via exchange)
                        leg = rt.split(":")[1]
                        leg_names = {"K": "Kong", "I": "ICPSwap", "R": "Reduced"}
                        parts.append(f"{leg_names.get(leg, leg)}:{cnt}")
                return " [" + ", ".join(parts) + "]" if parts else ""

            def reduced_breakdown():
                if not stats['reduced_detail']:
                    return ""
                parts = []
                for rt, cnt in sorted(stats['reduced_detail'].items()):
                    if rt == "REDUCED_K":
                        parts.append(f"Kong:{cnt}")
                    elif rt == "REDUCED_I":
                        parts.append(f"ICPSwap:{cnt}")
                return " [" + ", ".join(parts) + "]" if parts else ""

            # Calculate current imbalance after this trade
            current_imbalance = 0
            for sym, det in portfolio.tokens.items():
                val = (det.balance * det.price_in_icp) // (10 ** det.decimals)
                cur_bp = (val * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
                current_imbalance += abs(det.target_allocation_bp - cur_bp)

            # Update trade record with imbalance info
            trade_record['imbalance_after'] = current_imbalance
            trade_record['imbalance_change'] = imbalance_before - current_imbalance  # Positive = improvement

            # Use ANSI escape codes for refreshing display (only if stdout is a terminal)
            import sys
            is_tty = sys.stdout.isatty()

            if is_tty and trade_count > 1:
                # Move up 5 lines and clear each
                print("\033[5A", end="")
            print(f"[{trade_count}/{total_expected}] {sell_symbol} -> {actual_buy_symbol} via {route_readable(route_type)}" + ("\033[K" if is_tty else ""))
            print(f"  Direct: Kong:{stats['kong']} ICPSwap:{stats['icp']} Split:{stats['split']}(+{stats['split_interp']}i) Partial:{stats['partial']}(+{stats['partial_interp']}i) Reduced:{stats['reduced']}{reduced_breakdown()}" + ("\033[K" if is_tty else ""))
            print(f"  Fallback: {stats['icp_fb']}(+{stats['icp_fb_split']}spl +{stats['icp_fb_partial']}par +{stats['icp_fb_reduced']}red){fb_breakdown()} | Failed:{stats['fail']}" + ("\033[K" if is_tty else ""))
            print(f"  Slippage: last={stats['last_slip']}bp avg={stats['avg_slip']}bp" + ("\033[K" if is_tty else ""))
            # Show imbalance progress
            imbalance_change = initial_imbalance - current_imbalance
            imbalance_pct = (imbalance_change * 100) // initial_imbalance if initial_imbalance > 0 else 0
            print(f"  Imbalance: {current_imbalance}bp (started:{initial_imbalance}bp, improved:{imbalance_pct}%)" + ("\033[K" if is_tty else ""), flush=True)

    # Final summary
    print(f"\n\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)

    # Route distribution with percentages
    total = trade_count
    split_total = stats['split'] + stats['split_interp']
    partial_total = stats['partial'] + stats['partial_interp']
    direct_total = stats['kong'] + stats['icp'] + split_total + partial_total + stats['reduced']
    fb_total = stats['icp_fb'] + stats['icp_fb_split'] + stats['icp_fb_partial'] + stats['icp_fb_reduced']
    print(f"\nRoute Distribution ({total} trade attempts):")
    print(f"  DIRECT ROUTES ({direct_total} total, {direct_total*100//total if total else 0}%):")
    print(f"    Kong 100%:       {stats['kong']:3} ({stats['kong']*100//total if total else 0}%)")
    print(f"    ICPSwap 100%:    {stats['icp']:3} ({stats['icp']*100//total if total else 0}%)")
    print(f"    Split (fixed):   {stats['split']:3} ({stats['split']*100//total if total else 0}%)")
    print(f"    Split (interp):  {stats['split_interp']:3} ({stats['split_interp']*100//total if total else 0}%)")
    print(f"    Partial (fixed): {stats['partial']:3} ({stats['partial']*100//total if total else 0}%)")
    print(f"    Partial (interp):{stats['partial_interp']:3} ({stats['partial_interp']*100//total if total else 0}%)")
    print(f"    Reduced:         {stats['reduced']:3} ({stats['reduced']*100//total if total else 0}%)")
    print(f"  ICP FALLBACK ({fb_total} total, {fb_total*100//total if total else 0}%):")
    print(f"    Single:        {stats['icp_fb']:3} ({stats['icp_fb']*100//total if total else 0}%)")
    print(f"    Split:         {stats['icp_fb_split']:3} ({stats['icp_fb_split']*100//total if total else 0}%)")
    print(f"    Partial:       {stats['icp_fb_partial']:3} ({stats['icp_fb_partial']*100//total if total else 0}%)")
    print(f"    Reduced:       {stats['icp_fb_reduced']:3} ({stats['icp_fb_reduced']*100//total if total else 0}%)")
    print(f"  FAILED:          {stats['fail']:3} ({stats['fail']*100//total if total else 0}%)")

    # Show detailed breakdown for splits, fallback, and reduced routes
    if stats['split_detail']:
        print(f"\n  Split Breakdown (INTERP = interpolated between adjacent):")
        for route, count in sorted(stats['split_detail'].items()):
            print(f"    {route}: {count}")
    if stats['partial_detail']:
        print(f"\n  Partial Split Breakdown (sum < 100%, INTERP = interpolated):")
        for route, count in sorted(stats['partial_detail'].items()):
            # PARTIAL_60_20 or PARTIAL_55_25_INTERP -> 60%Kong + 20%ICP = 80%
            parts = route.replace("_INTERP", "").split("_")
            is_interp = "_INTERP" in route
            if len(parts) >= 3:
                kong_pct, icp_pct = parts[1], parts[2]
                total_pct = int(kong_pct) + int(icp_pct)
                interp_mark = " (INTERP)" if is_interp else ""
                print(f"    {kong_pct}%Kong + {icp_pct}%ICP = {total_pct}%{interp_mark}: {count}")
            else:
                print(f"    {route}: {count}")
    if stats['fb_detail']:
        print(f"\n  ICP Fallback Breakdown (K=Kong, I=ICP, R=Reduced):")
        for route, count in sorted(stats['fb_detail'].items()):
            print(f"    {route}: {count}")
    if stats['reduced_detail']:
        print(f"\n  Reduced Amount Breakdown (K=Kong, I=ICP):")
        for route, count in sorted(stats['reduced_detail'].items()):
            print(f"    {route}: {count}")

    if trades:
        # Slippage histogram
        slippages = [t['slippage_bp'] for t in trades]
        print(f"\nSlippage Distribution:")
        for low, high in [(0, 25), (25, 50), (50, 100), (100, 200), (200, 500)]:
            count = sum(1 for s in slippages if low <= s < high)
            bar = "█" * min(count * 2, 40)
            print(f"  {low:3}-{high:3}bp: {bar} ({count})")

        # Slippage stats
        print(f"\nSlippage Stats:")
        print(f"  Min: {min(slippages)}bp | Max: {max(slippages)}bp | Avg: {sum(slippages) // len(slippages)}bp")

        # Trade sizing stats
        exact_trades = sum(1 for t in trades if t['is_exact'])
        random_trades = len(trades) - exact_trades
        print(f"\nTrade Sizing:")
        print(f"  Exact targeting: {exact_trades}")
        print(f"  Random sizing: {random_trades}")

    # Portfolio convergence analysis
    final_imbalance = 0
    for symbol, details in portfolio.tokens.items():
        value = (details.balance * details.price_in_icp) // (10 ** details.decimals)
        current_bp = (value * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
        final_imbalance += abs(details.target_allocation_bp - current_bp)

    print(f"\nPortfolio Convergence:")
    print(f"  Initial imbalance: {initial_imbalance}bp")
    print(f"  Final imbalance:   {final_imbalance}bp")
    if initial_imbalance > 0:
        improvement = (initial_imbalance - final_imbalance) * 100 // initial_imbalance
        print(f"  Improvement:       {improvement}%")

    # Final portfolio state
    print(f"\nFinal Portfolio State:")
    for symbol, details in sorted(portfolio.tokens.items()):
        value = (details.balance * details.price_in_icp) // (10 ** details.decimals)
        current_bp = (value * 10000) // portfolio.total_value_icp if portfolio.total_value_icp > 0 else 0
        print(f"  {symbol:8} target={details.target_allocation_bp:4}bp current={current_bp:4}bp diff={details.target_allocation_bp - current_bp:+4}bp")

    # Export trades to CSV
    if trades:
        import csv
        from datetime import datetime
        csv_filename = f"trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Convert split_pct tuple to readable string (e.g., (6000, 4000) -> "60/40")
        for t in trades:
            if 'split_pct' in t and isinstance(t['split_pct'], tuple):
                kong_bp, icp_bp = t['split_pct']
                t['split_pct'] = f"{kong_bp // 100}/{icp_bp // 100}"

        with open(csv_filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'cycle', 'attempt', 'sell', 'buy', 'intended_buy',
                'amount_sold', 'amount_bought', 'intended_icp', 'actual_icp', 'is_exact',
                'slippage_bp', 'sell_diff_bp', 'buy_diff_bp',
                'route', 'split_pct', 'fail_reason',
                'imbalance_before', 'imbalance_after', 'imbalance_change'
            ])
            writer.writeheader()
            writer.writerows(trades)
        print(f"\nTrades exported to: {csv_filename}")

    return trades


# ============================================
# Test Function
# ============================================

def estimate_max_tradeable_icp(kong_quotes: List[Quote], icp_quotes: List[Quote], amount_icp: int, icp_involved: bool = False, num_quotes: int = 5) -> Tuple[int, str]:
    """
    Estimate max tradeable ICP at 70% of max slippage using existing smallest quote.
    Returns (estimated_icp, best_exchange) or (0, "") if can't estimate.
    No extra API calls - uses data we already have.

    If icp_involved=True (sell or buy is ICP), skip minimum trade check.
    Matches treasury.mo: ICP trades are always valuable regardless of size.

    num_quotes: number of quote points (default 5). The smallest quote is at (100/num_quotes)%.
    For 5 quotes: smallest is 20% (divide by 5). For 10 quotes: smallest is 10% (divide by 10).
    """
    # Get the smallest quote (index 0) slippage from whichever exchange has lower slippage
    kong_20_slip = kong_quotes[0].slippage_bp if kong_quotes[0].amount_out > 0 else 99999
    icp_20_slip = icp_quotes[0].slippage_bp if icp_quotes[0].amount_out > 0 else 99999

    # Use the better (lower slippage) exchange
    if kong_20_slip <= icp_20_slip:
        min_slip = kong_20_slip
        best_exchange = "Kong"
    else:
        min_slip = icp_20_slip
        best_exchange = "ICP"

    if min_slip >= 99999:
        # No valid quotes at all from either exchange
        # Still try 1 ICP - the verify step will make a fresh API call
        # and might succeed even if the bulk quotes failed
        if amount_icp > 0:
            # Prefer Kong as it has better routing, fallback to ICPSwap
            return (1, "Kong")
        return (0, "")

    if min_slip == 0:
        return (0, "")  # Zero slippage shouldn't happen

    # Slippage scales roughly linearly with amount
    # target_slip = maxSlippageBP * 7 / 10 (70% of max for safety margin)
    # Using Nat-safe integer math
    target_slip_bp = (MAX_SLIPPAGE_BP * 7) // 10

    # The smallest quote is for amount_icp / num_quotes (e.g., 10% for 10 quotes, 20% for 5 quotes)
    # max_amount = (amount_icp / num_quotes) * target_slip_bp / min_slip
    #
    # IMPORTANT: Multiply before dividing to avoid integer precision loss!
    # Old buggy code: max_icp = (amount_icp // num_quotes * target_slip_bp) // min_slip
    # For 15 ICP with 500bp slippage: (15 // 5 * 70) // 500 = (3 * 70) // 500 = 0
    # Fixed: Do all multiplication first, then single division
    # max_icp = (amount_icp * target_slip_bp) // (min_slip * num_quotes)
    #
    # For very small trades with high slippage, ensure at least 1 ICP if any trade is possible
    if min_slip > 0:
        max_icp = (amount_icp * target_slip_bp) // (min_slip * num_quotes)
        # If calculation gives 0 but we have some input, try at least 1 ICP
        # This allows tiny trades through fallback instead of failing outright
        # NOTE: Don't check min_slip <= MAX_SLIPPAGE_BP here - let the verify step filter bad quotes
        # Even high-slippage pairs might work with 1 ICP trade
        if max_icp == 0 and amount_icp > 0:
            max_icp = 1
    else:
        max_icp = 0

    # Skip minimum check if ICP is involved (ICP trades always valuable)
    # Matches treasury.mo behavior
    if icp_involved:
        if max_icp > 0:
            return (max_icp, best_exchange)
        return (0, "")

    # For non-ICP pairs, check minimum trade size
    if max_icp >= MIN_TRADE_ICP:
        return (max_icp, best_exchange)
    return (0, "")


def simulate_execution_failure_with_icp_fallback(
    sell_symbol: str,
    buy_symbol: str,
    amount_icp: int,
    base_amount: int,
    original_exchange: str,  # 'ICPSwap' or 'KongSwap'
    error_msg: str = "Slippage is over range"
) -> Optional[TestResult]:
    """
    Simulate what happens when executeTrade fails after findBestExecution succeeded.

    This matches treasury.mo line 4110-4276:
    - Only ICPSwap failures trigger ICP fallback (tokens recovered immediately)
    - KongSwap failures use pendingTxs retry mechanism instead
    - Fallback only if: buyToken != ICP and sellToken != ICP

    Returns: TestResult with type 'ICP_FB_EXEC' if fallback works, None otherwise
    """
    # Only ICPSwap failures trigger ICP fallback
    if original_exchange != 'ICPSwap':
        return None  # KongSwap uses pendingTxs retry

    # Can't fallback if already trading ICP
    if sell_symbol == "ICP" or buy_symbol == "ICP":
        return None

    # Try the ICP fallback route (sell -> ICP)
    fallback_result, fb_kong, fb_icp = test_pair_internal(sell_symbol, "ICP", amount_icp, base_amount)

    if fallback_result and fallback_result.result_type not in ['FAILURE', 'SKIP']:
        return TestResult(
            f"{sell_symbol}/{buy_symbol}", amount_icp, 'ICP_FB_EXEC',
            algorithm_output=fallback_result.algorithm_output,
            actual_output=fallback_result.actual_output,
            error_pct=fallback_result.error_pct,
            split_pct=fallback_result.split_pct,
            interpolated=fallback_result.interpolated,
            details=f"Exec failed ({error_msg}), ICP fallback: {sell_symbol}->ICP via {fallback_result.result_type}"
        )

    return None  # Fallback also failed


def test_pair_internal(sell_symbol: str, buy_symbol: str, amount_icp: int, base_amount: int) -> Tuple[Optional[TestResult], List[Quote], List[Quote]]:
    """
    Internal test function that runs the algorithm on a pair.
    Does NOT attempt ICP fallback - caller handles that.
    Returns: (result, kong_quotes, icp_quotes) - quotes needed for reduced amount estimation
    """
    # Quote amounts: 20%, 40%, 60%, 80%, 100%
    amounts = [base_amount * p // 10 for p in [2, 4, 6, 8, 10]]

    # Check for ICPSwap pool
    pool_key = (sell_symbol, buy_symbol)
    has_icpswap_pool = pool_key in ICPSWAP_POOLS

    # Fetch ALL quotes in parallel (5 Kong + up to 5 ICPSwap = 10 requests)
    kong_futures = [quote_executor.submit(get_kong_quote, sell_symbol, buy_symbol, amt) for amt in amounts]

    if has_icpswap_pool:
        pool_id, zero_for_one = ICPSWAP_POOLS[pool_key]
        # Fetch pool metadata for slippage calculation (like treasury.mo)
        sqrt_price = get_pool_metadata(pool_id)
        # Fetch ICPSwap quotes in parallel with sqrt_price for slippage calc
        icp_futures = [quote_executor.submit(get_icpswap_quote, pool_id, amt, zero_for_one, sqrt_price) for amt in amounts]
    else:
        icp_futures = None

    # Collect results
    kong_quotes = [f.result() for f in kong_futures]

    if icp_futures:
        icp_quotes = [f.result() for f in icp_futures]
    else:
        icp_quotes = [Quote(amt, 0, 10000, False, "no_pool") for amt in amounts]

    # Check if any exchange works
    kong_works = any(q.valid for q in kong_quotes)
    icp_works = any(q.valid for q in icp_quotes)

    if not kong_works and not icp_works:
        # Collect error info
        kong_errs = [q.error for q in kong_quotes if q.error]
        icp_errs = [q.error for q in icp_quotes if q.error]
        err_info = f"Kong:{kong_errs[0] if kong_errs else 'high_slip'} ICP:{icp_errs[0] if icp_errs else 'high_slip'}"
        return (TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'FAILURE',
                          details=f"Both failed: {err_info}"), kong_quotes, icp_quotes)

    # Run algorithm
    result_type, kong_pct, icp_pct, expected_out, no_interp_out, was_interpolated = run_algorithm(kong_quotes, icp_quotes)

    if result_type == 'NO_PATH':
        return (TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'FAILURE',
                          details="No viable path (slippage too high)"), kong_quotes, icp_quotes)

    # Categorize result - match treasury exactly
    if result_type == 'SINGLE_KONG':
        return (TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'SINGLE_KONG',
                          algorithm_output=expected_out, split_pct=(kong_pct, icp_pct),
                          details="Kong 100%"), kong_quotes, icp_quotes)

    if result_type == 'SINGLE_ICP':
        return (TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'SINGLE_ICP',
                          algorithm_output=expected_out, split_pct=(kong_pct, icp_pct),
                          details="ICPSwap 100%"), kong_quotes, icp_quotes)

    # SPLIT - verify with actual quotes at split percentage (fetch both in parallel)
    kong_amount = base_amount * kong_pct // 10000
    icp_amount = base_amount - kong_amount

    verify_kong_future = quote_executor.submit(get_kong_quote, sell_symbol, buy_symbol, kong_amount)
    if has_icpswap_pool:
        verify_icp_future = quote_executor.submit(get_icpswap_quote, pool_id, icp_amount, zero_for_one, sqrt_price)
    else:
        verify_icp_future = None

    actual_kong = verify_kong_future.result()
    actual_icp = verify_icp_future.result() if verify_icp_future else Quote(icp_amount, 0, 10000, False, "no_pool")

    actual_total = 0
    if actual_kong.valid:
        actual_total += actual_kong.amount_out
    if actual_icp.valid:
        actual_total += actual_icp.amount_out

    # Calculate error
    if actual_total > 0:
        error_pct = abs(expected_out - actual_total) / actual_total * 100
    else:
        error_pct = 0

    interp_tag = "INTERP" if was_interpolated else "FIXED"
    details = f"{interp_tag} Kong:{kong_pct/100:.1f}% ICP:{icp_pct/100:.1f}%"
    rtype = 'SPLIT_INTERP' if was_interpolated else 'SPLIT'

    return (TestResult(
        f"{sell_symbol}/{buy_symbol}", amount_icp, rtype,
        algorithm_output=expected_out, actual_output=actual_total,
        error_pct=error_pct, split_pct=(kong_pct, icp_pct),
        interpolated=was_interpolated, details=details
    ), kong_quotes, icp_quotes)


def test_pair(sell_symbol: str, buy_symbol: str, amount_icp: int) -> TestResult:
    """Test a single pair at a given amount, with reduced amount and ICP fallbacks."""
    if sell_symbol == buy_symbol:
        return TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'SKIP')

    # Calculate base amount
    decimals = TOKENS[sell_symbol][1]
    if sell_symbol == "ICP":
        base_amount = amount_icp * (10 ** 8)
    elif sell_symbol == "ckETH":
        base_amount = amount_icp * (10 ** 18) // 770
    elif sell_symbol == "ckBTC":
        base_amount = amount_icp * (10 ** 8) // 22800
    else:
        base_amount = amount_icp * (10 ** decimals)

    # Try direct pair first
    result, kong_quotes, icp_quotes = test_pair_internal(sell_symbol, buy_symbol, amount_icp, base_amount)

    # If direct pair failed, try fallbacks
    if result and result.result_type == 'FAILURE':
        # First: try REDUCED amount - estimate and VERIFY with actual quote
        icp_involved = sell_symbol == "ICP" or buy_symbol == "ICP"
        max_icp, best_exch = estimate_max_tradeable_icp(kong_quotes, icp_quotes, amount_icp, icp_involved)
        if max_icp > 0 and (icp_involved or max_icp >= MIN_TRADE_ICP):
            # Verify with actual quote at reduced amount
            reduced_base = int(base_amount * max_icp / amount_icp)
            pool_key = (sell_symbol, buy_symbol)

            # Try both exchanges for verify - if first fails, try second
            verify_quote = None
            actual_exch = best_exch

            for exch in ([best_exch, "ICP" if best_exch == "Kong" else "Kong"]):
                if exch == "Kong":
                    test_verify = get_kong_quote(sell_symbol, buy_symbol, reduced_base)
                else:
                    if pool_key in ICPSWAP_POOLS:
                        pool_id, zfo = ICPSWAP_POOLS[pool_key]
                        sqrt_price = get_pool_metadata(pool_id)
                        test_verify = get_icpswap_quote(pool_id, reduced_base, zfo, sqrt_price)
                    else:
                        test_verify = Quote(reduced_base, 0, 10000, False, "no_pool")

                if test_verify.valid:
                    verify_quote = test_verify
                    actual_exch = exch
                    break

            if verify_quote is not None:
                return TestResult(
                    f"{sell_symbol}/{buy_symbol}", amount_icp, 'REDUCED',
                    details=f"Direct {actual_exch}: verified ~{max_icp:.1f} ICP @ {verify_quote.slippage_bp}bp",
                    max_tradeable_icp=max_icp,
                    algorithm_output=verify_quote.amount_out
                )
            # Verification failed - continue to ICP fallback

        # Second: try ICP fallback (sell -> ICP)
        if sell_symbol == "ICP":
            return TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'FAILURE',
                              details=f"Direct: {result.details} | No fallback (sell=ICP)")
        elif buy_symbol == "ICP":
            return TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'FAILURE',
                              details=f"Direct: {result.details} | No fallback (buy=ICP)")
        else:
            # Try sell_symbol -> ICP fallback
            fallback_result, fb_kong, fb_icp = test_pair_internal(sell_symbol, "ICP", amount_icp, base_amount)
            if fallback_result and fallback_result.result_type not in ['FAILURE', 'SKIP']:
                # Distinguish ICP fallback single vs split
                is_split = fallback_result.result_type in ['SPLIT', 'SPLIT_INTERP']
                fb_type = 'ICP_FB_SPLIT' if is_split else 'ICP_FALLBACK'
                return TestResult(
                    f"{sell_symbol}/{buy_symbol}", amount_icp, fb_type,
                    algorithm_output=fallback_result.algorithm_output,
                    actual_output=fallback_result.actual_output,
                    error_pct=fallback_result.error_pct,
                    split_pct=fallback_result.split_pct,
                    interpolated=fallback_result.interpolated,
                    details=f"{sell_symbol}->ICP: {fallback_result.details}"
                )
            # ICP fallback also failed - check if reduced amount works for ICP route
            # ICP is involved here (buy=ICP), so skip minimum check
            fb_max_icp, fb_best_exch = estimate_max_tradeable_icp(fb_kong, fb_icp, amount_icp, icp_involved=True)
            if fb_max_icp > 0:  # No minimum check when ICP involved
                # Verify with actual quote at reduced amount for ICP route
                reduced_base = int(base_amount * fb_max_icp / amount_icp)
                if fb_best_exch == "Kong":
                    verify_quote = get_kong_quote(sell_symbol, "ICP", reduced_base)
                else:
                    pool_key = (sell_symbol, "ICP")
                    if pool_key in ICPSWAP_POOLS:
                        pool_id, zfo = ICPSWAP_POOLS[pool_key]
                        sqrt_price = get_pool_metadata(pool_id)
                        verify_quote = get_icpswap_quote(pool_id, reduced_base, zfo, sqrt_price)
                    else:
                        verify_quote = Quote(reduced_base, 0, 10000, False, "no_pool")

                if verify_quote.valid:
                    return TestResult(
                        f"{sell_symbol}/{buy_symbol}", amount_icp, 'REDUCED',
                        details=f"{sell_symbol}->ICP {fb_best_exch}: verified ~{fb_max_icp:.1f} ICP @ {verify_quote.slippage_bp}bp",
                        max_tradeable_icp=fb_max_icp,
                        algorithm_output=verify_quote.amount_out
                    )
            # Everything failed
            fb_detail = fallback_result.details if fallback_result else "unknown"
            return TestResult(f"{sell_symbol}/{buy_symbol}", amount_icp, 'FAILURE',
                              details=f"Direct+Fallback failed: {fb_detail}")

    return result


def print_status():
    """Print current status."""
    with results_lock:
        singles_kong = sum(1 for r in all_results if r.result_type == 'SINGLE_KONG')
        singles_icp = sum(1 for r in all_results if r.result_type == 'SINGLE_ICP')
        splits = sum(1 for r in all_results if r.result_type in ['SPLIT', 'SPLIT_INTERP'])
        icp_fb_single = sum(1 for r in all_results if r.result_type == 'ICP_FALLBACK')
        icp_fb_split = sum(1 for r in all_results if r.result_type == 'ICP_FB_SPLIT')
        icp_fb_exec = sum(1 for r in all_results if r.result_type == 'ICP_FB_EXEC')
        reduced = sum(1 for r in all_results if r.result_type == 'REDUCED')
        failures = sum(1 for r in all_results if r.result_type == 'FAILURE')

        status = f"\r[{completed_count}/{total_tests}] Kong:{singles_kong} ICP:{singles_icp} Split:{splits} ICP_FB:{icp_fb_single}({icp_fb_split}) ExecFB:{icp_fb_exec} Red:{reduced} Fail:{failures}"
        print(status, end="", flush=True)


def worker(sell: str, buy: str, amount: int) -> TestResult:
    """Worker function."""
    global completed_count, stop_requested

    if stop_requested:
        return TestResult(f"{sell}/{buy}", amount, 'SKIP', details="Stopped")

    result = test_pair(sell, buy, amount)

    with results_lock:
        all_results.append(result)
        completed_count += 1

    print_status()
    return result


# ============================================
# Main
# ============================================

def print_final_summary():
    """Print final summary of results."""
    print("\n\n" + "=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)

    singles_kong = [r for r in all_results if r.result_type == 'SINGLE_KONG']
    singles_icp = [r for r in all_results if r.result_type == 'SINGLE_ICP']
    splits_fixed = [r for r in all_results if r.result_type == 'SPLIT']
    splits_interp = [r for r in all_results if r.result_type == 'SPLIT_INTERP']
    icp_fb_single = [r for r in all_results if r.result_type == 'ICP_FALLBACK']
    icp_fb_split = [r for r in all_results if r.result_type == 'ICP_FB_SPLIT']
    icp_fb_exec = [r for r in all_results if r.result_type == 'ICP_FB_EXEC']
    reduced = [r for r in all_results if r.result_type == 'REDUCED']
    failures = [r for r in all_results if r.result_type == 'FAILURE']
    skipped = [r for r in all_results if r.result_type == 'SKIP']

    print(f"\nTotal tests: {completed_count}/{total_tests}")
    print(f"  SINGLE_KONG: {len(singles_kong)} (100% via KongSwap)")
    print(f"  SINGLE_ICP:  {len(singles_icp)} (100% via ICPSwap)")
    print(f"  SPLITS (fixed %): {len(splits_fixed)} (exact 20/40/60/80%)")

    if splits_interp:
        errors = [r.error_pct for r in splits_interp]
        print(f"  SPLITS (interpolated): {len(splits_interp)}")
        print(f"    - Interpolation error: avg={sum(errors)/len(errors):.3f}% max={max(errors):.3f}%")
    else:
        print(f"  SPLITS (interpolated): 0")

    print(f"  ICP_FALLBACK (single): {len(icp_fb_single)} (direct failed, routed via ICP)")
    print(f"  ICP_FALLBACK (split):  {len(icp_fb_split)} (direct failed, split via ICP route)")
    print(f"  ICP_FB_EXEC: {len(icp_fb_exec)} (execution failed, ICP fallback succeeded)")
    print(f"  REDUCED: {len(reduced)} (slippage too high, estimated max tradeable)")
    print(f"  FAILURES: {len(failures)} (no viable route)")
    if skipped:
        print(f"  SKIPPED: {len(skipped)}")

    # Show interpolated splits (the ones we actually care about)
    if splits_interp:
        print("\n" + "-" * 80)
        print(f"Interpolated splits ({len(splits_interp)}):")
        for r in sorted(splits_interp, key=lambda x: x.error_pct, reverse=True)[:15]:
            print(f"  {r.pair:15} @{r.amount:2}ICP: exp={r.algorithm_output:,} act={r.actual_output:,} err={r.error_pct:.3f}% ({r.details})")

    # Show ICP fallbacks (single)
    if icp_fb_single:
        print("\n" + "-" * 80)
        print(f"ICP Fallbacks - Single ({len(icp_fb_single)}):")
        for r in icp_fb_single[:10]:
            print(f"  {r.pair:15} @{r.amount:2}ICP: {r.details}")

    # Show ICP fallbacks (split)
    if icp_fb_split:
        print("\n" + "-" * 80)
        print(f"ICP Fallbacks - Split ({len(icp_fb_split)}):")
        for r in icp_fb_split[:10]:
            print(f"  {r.pair:15} @{r.amount:2}ICP: {r.details}")

    # Show ICP fallbacks after execution failure (ICPSwap "slippage over range" etc)
    if icp_fb_exec:
        print("\n" + "-" * 80)
        print(f"ICP Fallbacks - After Execution Failure ({len(icp_fb_exec)}):")
        print("  (These are trades where findBestExecution succeeded but executeTrade failed)")
        print("  (Only ICPSwap failures trigger this - tokens recovered immediately)")
        for r in icp_fb_exec[:10]:
            print(f"  {r.pair:15} @{r.amount:2}ICP: {r.details}")

    # Show reduced amount suggestions
    if reduced:
        print("\n" + "-" * 80)
        print(f"Reduced Amount ({len(reduced)}) - trades possible at smaller size:")
        for r in sorted(reduced, key=lambda x: x.max_tradeable_icp, reverse=True)[:15]:
            print(f"  {r.pair:15} @{r.amount:2}ICP: max ~{r.max_tradeable_icp:.1f} ICP ({r.details})")

    # Show some failures
    if failures:
        print("\n" + "-" * 80)
        print(f"Failures ({len(failures)}):")
        for r in failures[:10]:
            print(f"  {r.pair:15} @{r.amount:2}ICP: {r.details}")


def main():
    global total_tests, completed_count, all_results, stop_requested

    import sys

    # Parse command line arguments
    args = sys.argv[1:]
    use_production = "--prod" in args or "-p" in args
    if use_production:
        args = [a for a in args if a not in ("--prod", "-p")]

    # Check for command line arguments
    if args:
        if args[0] == "--cycle" or args[0] == "-c":
            # Run full trading cycle simulation (no real quotes)
            num_cycles = int(args[1]) if len(args) > 1 else 5
            run_full_trading_cycle_test(num_cycles)
            return
        elif args[0] == "--full" or args[0] == "-f":
            # Run full trading cycle with REAL DEX quotes
            num_cycles = int(args[1]) if len(args) > 1 else 5
            run_full_trading_cycle_with_real_quotes(num_cycles, use_production_data=use_production)
            return
        elif args[0] == "--exec-fallback" or args[0] == "-e":
            # Test execution failure with ICP fallback
            print("=" * 80)
            print("Execution Failure ICP Fallback Simulation")
            print("=" * 80)
            print("Simulating: ICPSwap trade succeeds in findBestExecution but fails in executeTrade")
            print("Treasury.mo line 4110-4276 handles this by attempting ICP fallback")
            print()

            # Discover ICPSwap pools first
            print("Discovering ICPSwap pools...")
            discover_icpswap_pools()
            print(f"Found {len(ICPSWAP_POOLS)} ICPSwap pools")
            print()

            # Test all non-ICP pairs that could fail on ICPSwap
            exec_fallback_results = []
            for sell in TOKENS:
                if sell == "ICP":
                    continue
                for buy in TOKENS:
                    if buy == "ICP" or buy == sell:
                        continue

                    # Calculate base amount for 5 ICP equivalent
                    decimals = TOKENS[sell][1]
                    if sell == "ckETH":
                        base_amount = 5 * (10 ** 18) // 770
                    elif sell == "ckBTC":
                        base_amount = 5 * (10 ** 8) // 22800
                    else:
                        base_amount = 5 * (10 ** decimals)

                    result = simulate_execution_failure_with_icp_fallback(
                        sell, buy, 5, base_amount, "ICPSwap", "Slippage is over range"
                    )
                    if result:
                        exec_fallback_results.append(result)
                        print(f"  {result.pair:15} -> ICP fallback available: {result.details}")
                    else:
                        print(f"  {sell}/{buy}:15 -> No ICP fallback route")

            print()
            print(f"Total pairs with ICP execution fallback: {len(exec_fallback_results)}")
            return
        elif args[0] == "--help" or args[0] == "-h":
            print("Usage:")
            print("  python test_exchange_selection.py              # Run exchange selection tests with real quotes")
            print("  python test_exchange_selection.py --cycle 5    # Run 5 trading cycle simulations (no real quotes)")
            print("  python test_exchange_selection.py -c 10        # Run 10 trading cycle simulations")
            print("  python test_exchange_selection.py --full 5     # Run 5 trading cycles with REAL DEX quotes")
            print("  python test_exchange_selection.py -f 10        # Run 10 trading cycles with REAL DEX quotes")
            print("  python test_exchange_selection.py --full --prod # Use REAL prices/config from production canisters")
            print("  python test_exchange_selection.py -f -p 10      # Production data with 10 cycles")
            print("  python test_exchange_selection.py --exec-fallback  # Test execution failure ICP fallback")
            print("\nFlags:")
            print("  --prod, -p   Use REAL prices/decimals/config from production DAO/Treasury canisters")
            print("               (Target allocations remain random for test diversity)")
            print("\nTreasury Configuration (matches treasury.mo):")
            for key, value in TREASURY_CONFIG.items():
                print(f"  {key}: {value}")
            return

    print("=" * 80)
    print("Exchange Selection Algorithm Test")
    print("=" * 80)
    print(f"Max slippage: {MAX_SLIPPAGE_BP}bp | Parallel: {MAX_PARALLEL} | Timeout: {CALL_TIMEOUT}s")
    print()
    print("Modes:")
    print("  - Run with no args: Test all pairs with real DEX quotes")
    print("  - Run with --cycle N: Simulate N trading cycles (no real quotes)")
    print("  - Run with --full N: Full treasury logic with REAL DEX quotes")
    print()
    print("Press Ctrl+C at any time to stop and show results")
    print()

    # Fetch ICPSwap pools
    fetch_icpswap_pools()
    print()

    # Build ALL token pair combinations
    symbols = list(TOKENS.keys())
    tasks = []
    for sell in symbols:
        for buy in symbols:
            if sell != buy:
                for amount in TRADE_SIZES:
                    tasks.append((sell, buy, amount))

    total_tests = len(tasks)
    completed_count = 0
    all_results = []
    stop_requested = False

    print(f"Testing {len(symbols)} tokens × {len(symbols)-1} pairs × {len(TRADE_SIZES)} amounts = {total_tests} tests")
    print()

    # Run tests with keyboard interrupt handling
    try:
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
            futures = {executor.submit(worker, s, b, a): (s, b, a) for s, b, a in tasks}
            for future in as_completed(futures):
                if stop_requested:
                    break
                try:
                    future.result()
                except Exception as e:
                    print(f"\nError: {e}")
    except KeyboardInterrupt:
        print("\n\n*** Ctrl+C pressed - stopping tests and showing results ***")
        stop_requested = True

    # Final summary
    print_final_summary()


if __name__ == "__main__":
    main()
