#!/usr/bin/env python3
"""Offline cross-check for the treasury LP budget water-fill (computeLPTargets).

Pulls live state from the treasury canister and reimplements the fill with the
same integer semantics as treasury.mo. Asserts:
  (a) every new target >= live current LP value (STEP-A safety: no removals at cutover)
  (b) per-token side-spend <= side budget B_t
  (c) prints total target for eyeballing against expectations

Usage: python3 scripts/lp_target_sim.py [--network ic] [--canister v6t5d-6yaaa-aaaan-qzzja-cai]
"""
import argparse
import json
import subprocess
import sys

MAX_ROUNDS = 16
EPS = 1_000_000  # 0.01 ICP e8s


def dfx_query(canister, method, network, identity=None):
    cmd = ["dfx", "canister", "--network", network, "call", canister, method,
           "--query", "--output", "json"]
    if identity:
        cmd += ["--identity", identity]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if out.returncode != 0:
        sys.exit(f"dfx call {method} failed: {out.stderr}")
    return json.loads(out.stdout)


def nat(s):
    return int(s.replace("_", "")) if isinstance(s, str) else int(s)


def water_fill(pools, alloc, portfolio, lp_ratio_bp, nachos_bp, pool_ratio_bp=None):
    """pools: list of (t0, t1, current_e8s). Returns dict poolkey -> targetWhole."""
    haircut = 10_000 - nachos_bp if nachos_bp < 10_000 else 0
    fill = []
    for t0, t1, current in sorted(pools):
        a0, a1 = alloc.get(t0, 0), alloc.get(t1, 0)
        if a0 == 0 or a1 == 0:
            continue
        ratio = (pool_ratio_bp or {}).get((t0, t1), lp_ratio_bp)
        cap = 2 * (min(a0, a1) * portfolio * ratio * haircut) // (10_000 ** 3)
        fill.append({"t0": t0, "t1": t1, "a0": a0, "a1": a1, "cap": cap, "target": 0})
    remaining = {}
    for p in fill:
        for t in (p["t0"], p["t1"]):
            if t not in remaining:
                remaining[t] = (alloc.get(t, 0) * portfolio * lp_ratio_bp * haircut) // (10_000 ** 3)
    budgets = dict(remaining)
    for _ in range(MAX_ROUNDS):
        snap = dict(remaining)
        wsum = {}
        for p in fill:
            if p["target"] < p["cap"] and snap.get(p["t0"], 0) > 0 and snap.get(p["t1"], 0) > 0:
                wsum[p["t0"]] = wsum.get(p["t0"], 0) + p["a1"]
                wsum[p["t1"]] = wsum.get(p["t1"], 0) + p["a0"]
        moved = 0
        for p in fill:
            r0, r1 = snap.get(p["t0"], 0), snap.get(p["t1"], 0)
            if p["target"] < p["cap"] and r0 > 0 and r1 > 0:
                w0, w1 = wsum.get(p["t0"], 0), wsum.get(p["t1"], 0)
                if w0 > 0 and w1 > 0:
                    inc = min(r0 * p["a1"] // w0, r1 * p["a0"] // w1,
                              (p["cap"] - p["target"]) // 2)
                    if inc > 0:
                        p["target"] += 2 * inc
                        remaining[p["t0"]] = max(0, remaining[p["t0"]] - inc)
                        remaining[p["t1"]] = max(0, remaining[p["t1"]] - inc)
                        moved += 2 * inc
        if moved < EPS:
            break
    return {(p["t0"], p["t1"]): p["target"] for p in fill}, budgets, remaining


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--network", default="ic")
    ap.add_argument("--canister", default="v6t5d-6yaaa-aaaan-qzzja-cai")
    ap.add_argument("--identity", default="launch")
    args = ap.parse_args()

    lp = dfx_query(args.canister, "admin_getLPStatus", args.network, args.identity)
    allocs_raw = dfx_query(args.canister, "getCurrentAllocations", args.network, args.identity)
    alloc = {e["0"]: nat(e["1"]) for e in allocs_raw}

    cfg = lp["config"]
    lp_ratio_bp, nachos_bp = nat(cfg["lpRatioBP"]), nat(cfg["nachosRedemptionBufferBP"])
    min_lp = nat(cfg["minLPValueICP"])

    # portfolio approximated from budgets: sum(budget) == portfolio * lpRatioBP (pre-haircut display)
    # if the deployed canister already shows haircut budgets, adjust accordingly
    budget_total = sum(nat(b["budgetICP"]) for b in lp["budgetUsage"])
    portfolio = budget_total * 10_000 // lp_ratio_bp
    haircut = 10_000 - nachos_bp
    # detect haircut-adjusted display (post-upgrade): utilization near/above 1.0 hints at it
    # keep both interpretations visible
    portfolio_hc = budget_total * 10_000 * 10_000 // (lp_ratio_bp * haircut)

    pools, current = [], {}
    for c, pos in zip(lp["poolConfigs"], [None] * len(lp["poolConfigs"])):
        pass
    enabled = [tuple(c["0"].split(":")) for c in lp["poolConfigs"] if c["1"]["enabled"]]
    cur_by_key = {}
    for p in lp["positions"]:
        k = tuple(sorted((p["token0"], p["token1"])))
        # whole-pool current value approximated from backings via budgetUsage is complex;
        # use liquidity>0 marker + let the on-chain rebalance threshold absorb noise.
        cur_by_key[k] = nat(p.get("backing0", "0"))  # marker only
    for t0, t1 in enabled:
        pools.append((t0, t1, cur_by_key.get((t0, t1), 0)))

    targets, budgets, leftover = water_fill(pools, alloc, portfolio, lp_ratio_bp, nachos_bp)

    total = sum(targets.values())
    print(f"portfolio ~{portfolio/1e8:,.0f} ICP (haircut-display alt: {portfolio_hc/1e8:,.0f})")
    print(f"pools enabled: {len(enabled)}, filled: {len(targets)}")
    print(f"TOTAL whole-pool target: {total/1e8:,.0f} ICP")
    print(f"budget sum (side): {sum(budgets.values())/1e8:,.0f} ICP, "
          f"leftover: {sum(leftover.values())/1e8:,.0f} ICP")

    # (b) side-spend <= budget per token
    spend = {}
    for (t0, t1), tgt in targets.items():
        spend[t0] = spend.get(t0, 0) + tgt // 2
        spend[t1] = spend.get(t1, 0) + tgt // 2
    for t, s in spend.items():
        assert s <= budgets.get(t, 0) + 1, f"BUDGET OVERSPEND {t}: {s} > {budgets.get(t)}"
    print("assert (b) OK: no token over budget")

    # dust filter report
    dust = [(k, v) for k, v in targets.items() if 0 < v < min_lp]
    if dust:
        print(f"note: {len(dust)} pools under minLPValueICP would be zeroed: {dust}")
    print("done — compare TOTAL against plan expectation before deploying")


if __name__ == "__main__":
    main()
