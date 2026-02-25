#!/usr/bin/env python3
"""Count transactions in the last 30 days for an ICRC-3 ledger canister."""

import subprocess
import re
import sys
from datetime import datetime, timedelta

CANISTER_ID = "um5iw-rqaaa-aaaaq-qaaba-cai"
NETWORK = "ic"


def dfx_call(canister_id, method, args, timeout=30):
    """Call a canister method via dfx and return stdout."""
    cmd = (
        f"dfx canister call {canister_id} {method} "
        f"'{args}' --network {NETWORK}"
    )
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, timeout=timeout
    )
    if result.returncode != 0:
        print(f"  dfx error: {result.stderr.strip()}", file=sys.stderr)
    return result.stdout


def get_log_length_and_latest_ts():
    """Get total blocks and latest block timestamp."""
    # Get log_length
    out = dfx_call(CANISTER_ID, "icrc3_get_blocks",
                   '(vec { record { start = 0 : nat; length = 0 : nat } })')
    m = re.search(r'log_length\s*=\s*([\d_]+)', out)
    if not m:
        raise ValueError(f"Could not parse log_length: {out}")
    log_length = int(m.group(1).replace('_', ''))
    return log_length


def get_block_timestamp(block_id):
    """Get timestamp (nanoseconds) of a block. Returns None if archived."""
    out = dfx_call(CANISTER_ID, "icrc3_get_blocks",
                   f'(vec {{ record {{ start = {block_id} : nat; length = 1 : nat }} }})')
    # Check if block was returned directly
    ts_match = re.search(r'"ts";\s*variant\s*\{\s*Nat\s*=\s*([\d_]+)', out)
    if ts_match:
        return int(ts_match.group(1).replace('_', ''))

    # Check if it's in archived_blocks (has a callback reference)
    if 'callback' in out and 'archived_blocks' in out:
        return "ARCHIVED"

    return None


def get_archives():
    """Get archive canister ranges."""
    out = dfx_call(CANISTER_ID, "icrc3_get_archives", '(record {})')
    # Parse archive entries: record { end = N; canister_id = principal "xxx"; start = M }
    archives = []
    for m in re.finditer(
        r'record\s*\{\s*end\s*=\s*([\d_]+)\s*:\s*nat;\s*canister_id\s*=\s*principal\s*"([^"]+)";\s*start\s*=\s*([\d_]+)',
        out
    ):
        archives.append({
            'end': int(m.group(1).replace('_', '')),
            'canister_id': m.group(2),
            'start': int(m.group(3).replace('_', '')),
        })
    return archives


def get_block_timestamp_from_archive(archive_canister_id, block_id):
    """Get timestamp from an archive canister."""
    out = dfx_call(archive_canister_id, "icrc3_get_blocks",
                   f'(vec {{ record {{ start = {block_id} : nat; length = 1 : nat }} }})')
    ts_match = re.search(r'"ts";\s*variant\s*\{\s*Nat\s*=\s*([\d_]+)', out)
    if ts_match:
        return int(ts_match.group(1).replace('_', ''))
    return None


def get_timestamp(block_id, archives):
    """Get timestamp for a block, checking archives if needed."""
    ts = get_block_timestamp(block_id)
    if ts == "ARCHIVED":
        # Find the right archive
        for arc in archives:
            if arc['start'] <= block_id <= arc['end']:
                ts = get_block_timestamp_from_archive(arc['canister_id'], block_id)
                if ts:
                    return ts
        return None
    return ts


def binary_search(target_ts_ns, log_length, archives):
    """Find first block with timestamp >= target_ts_ns."""
    lo, hi = 0, log_length - 1
    result = log_length

    iterations = 0
    while lo <= hi:
        iterations += 1
        mid = (lo + hi) // 2
        ts = get_timestamp(mid, archives)

        if ts is None:
            # Try a few nearby blocks
            found = False
            for offset in [1, -1, 2, -2, 5, -5, 10, -10]:
                ts = get_timestamp(mid + offset, archives)
                if ts is not None:
                    mid = mid + offset
                    found = True
                    break
            if not found:
                lo = mid + 100
                continue

        mid_dt = datetime.utcfromtimestamp(ts / 1e9)
        print(f"  iteration {iterations}: block {mid:,} -> {mid_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")

        if ts >= target_ts_ns:
            result = mid
            hi = mid - 1
        else:
            lo = mid + 1

    return result


def main():
    print(f"Canister: {CANISTER_ID}")
    print(f"Network:  {NETWORK}")
    print()

    # Step 1: Get total blocks
    print("Step 1: Getting total block count...")
    log_length = get_log_length_and_latest_ts()
    print(f"  Total blocks: {log_length:,}")

    # Step 2: Get latest block timestamp
    print("\nStep 2: Getting latest block timestamp...")
    latest_ts = get_block_timestamp(log_length - 1)
    if latest_ts and latest_ts != "ARCHIVED":
        latest_dt = datetime.utcfromtimestamp(latest_ts / 1e9)
        print(f"  Latest block: {latest_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    else:
        print("  Could not get latest block timestamp")

    # Step 3: Get archives
    print("\nStep 3: Getting archive info...")
    archives = get_archives()
    if archives:
        for arc in archives:
            print(f"  Archive {arc['canister_id']}: blocks {arc['start']:,} - {arc['end']:,}")
    else:
        print("  No archives found (all blocks in main canister)")

    # Step 4: Calculate target timestamp (30 days ago)
    now = datetime.utcnow()
    one_month_ago = now - timedelta(days=30)
    target_ts_ns = int(one_month_ago.timestamp() * 1e9)
    print(f"\nStep 4: Target date: {one_month_ago.strftime('%Y-%m-%d %H:%M:%S')} UTC (30 days ago)")

    # Step 5: Estimate starting point for binary search
    if latest_ts and latest_ts != "ARCHIVED":
        # Try to estimate block rate using a block from ~1 day ago
        recent_block = max(0, log_length - 10000)
        recent_ts = get_timestamp(recent_block, archives)
        if recent_ts:
            time_diff = latest_ts - recent_ts  # nanoseconds
            block_diff = (log_length - 1) - recent_block
            if time_diff > 0:
                blocks_per_ns = block_diff / time_diff
                time_to_target = latest_ts - target_ts_ns
                estimated_blocks_back = int(time_to_target * blocks_per_ns)
                estimated_start = max(0, log_length - 1 - estimated_blocks_back)
                print(f"  Estimated block rate: ~{block_diff / (time_diff / 1e9 / 86400):.0f} blocks/day")
                print(f"  Estimated boundary: ~block {estimated_start:,}")

    # Step 6: Binary search
    print(f"\nStep 5: Binary searching for 30-day boundary...")
    boundary = binary_search(target_ts_ns, log_length, archives)

    # Step 7: Results
    transactions_last_month = log_length - boundary
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"  Total blocks in ledger:          {log_length:,}")
    print(f"  Boundary block (30 days ago):     {boundary:,}")
    print(f"  Transactions in last 30 days:     {transactions_last_month:,}")

    # Verify boundary timestamp
    boundary_ts = get_timestamp(boundary, archives)
    if boundary_ts:
        boundary_dt = datetime.utcfromtimestamp(boundary_ts / 1e9)
        print(f"  Boundary block timestamp:         {boundary_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # Also check block just before boundary
    if boundary > 0:
        prev_ts = get_timestamp(boundary - 1, archives)
        if prev_ts:
            prev_dt = datetime.utcfromtimestamp(prev_ts / 1e9)
            print(f"  Block before boundary timestamp:  {prev_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    print(f"{'='*60}")


if __name__ == "__main__":
    main()
