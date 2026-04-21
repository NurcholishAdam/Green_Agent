"""
ged_benchmark.py  —  Recommendation 4
========================================
Parametric GED performance benchmark.

Target SLA:  p99 latency < 100 ms  for graphs with ≤ 50 nodes per sequence.

What it measures
-----------------
GED is computed by DP sequence alignment on action-label lists.
Runtime is O(m × n) where m = |Graph A sequence|, n = |Graph B sequence|.
This benchmark sweeps (m, n) from (5, 5) to (200, 200) to find:
  1. The exact node count at which p99 crosses 100ms.
  2. Whether async routing (AsyncDPAlign) improves latency for large graphs.
  3. The statistical distribution at the SLA boundary (50 nodes).

Output
-------
Printed report + CSV file at ./benchmark_results/ged_latency.csv

CSV columns:
  graph_size, n_trials, p50_ms, p95_ms, p99_ms, max_ms, passed_sla

Usage
------
    python benchmarks/ged_benchmark.py

    # With async comparison:
    python benchmarks/ged_benchmark.py --async-compare

    # Just validate the SLA at 50 nodes:
    python benchmarks/ged_benchmark.py --sla-check-only
"""

import argparse
import asyncio
import csv
import random
import string
import time
from pathlib import Path
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.dual_graph_evaluator import DualGraphEvaluator
from core.async_traversal import AsyncDPAlign, _dp_align_sync

SLA_MS      = 100.0           # millisecond target
SLA_NODES   = 50              # node count at which SLA is measured
N_TRIALS    = 200             # trials per size point
GRAPH_SIZES = [5, 10, 20, 30, 50, 75, 100, 150, 200]
RESULTS_DIR = Path("./benchmark_results")

EDIT_COSTS = {"insert": 1.0, "delete": 1.0, "relabel": 0.7}

# Action vocabulary (realistic agent decision labels)
ACTIONS = [
    "quantize", "prune", "execute", "defer", "throttle",
    "cache", "offload", "prefetch", "batch", "stream",
    "compress", "encrypt", "route", "schedule", "checkpoint",
]


# ---------------------------------------------------------------------------
# Sequence generators
# ---------------------------------------------------------------------------

def random_sequence(length: int) -> list[str]:
    """Generate a random action sequence of given length."""
    return [random.choice(ACTIONS) for _ in range(length)]


def similar_sequence(base: list[str], edit_distance: int = 2) -> list[str]:
    """
    Generate a sequence close to base with ~edit_distance mutations.
    Produces realistic GED scenarios (agent vs ideal path).
    """
    seq = list(base)
    for _ in range(min(edit_distance, len(seq))):
        op = random.choice(["substitute", "insert", "delete"])
        idx = random.randrange(len(seq))
        if op == "substitute":
            seq[idx] = random.choice(ACTIONS)
        elif op == "insert" and len(seq) < len(base) + edit_distance:
            seq.insert(idx, random.choice(ACTIONS))
        elif op == "delete" and len(seq) > 1:
            seq.pop(idx)
    return seq


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def time_ged_sync(seq_a: list[str], seq_b: list[str]) -> float:
    """Return elapsed milliseconds for one synchronous GED computation."""
    start = time.perf_counter()
    _dp_align_sync(seq_a, seq_b, EDIT_COSTS)
    return (time.perf_counter() - start) * 1000


async def time_ged_async(seq_a: list[str], seq_b: list[str]) -> float:
    """Return elapsed milliseconds for one async GED computation."""
    start = time.perf_counter()
    await AsyncDPAlign.align(seq_a, seq_b, EDIT_COSTS)
    return (time.perf_counter() - start) * 1000


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def percentile(sorted_data: list[float], p: float) -> float:
    if not sorted_data:
        return 0.0
    idx = int(len(sorted_data) * p / 100)
    idx = min(idx, len(sorted_data) - 1)
    return round(sorted_data[idx], 3)


def stats(times_ms: list[float]) -> dict:
    s = sorted(times_ms)
    return {
        "p50":  percentile(s, 50),
        "p95":  percentile(s, 95),
        "p99":  percentile(s, 99),
        "max":  round(max(s), 3),
        "mean": round(sum(s) / len(s), 3),
    }


# ---------------------------------------------------------------------------
# Benchmark runs
# ---------------------------------------------------------------------------

def run_sync_benchmark(sizes: list[int] = GRAPH_SIZES,
                        n_trials: int = N_TRIALS) -> list[dict]:
    results = []
    for size in sizes:
        times: list[float] = []
        for _ in range(n_trials):
            base = random_sequence(size)
            variant = similar_sequence(base, edit_distance=max(1, size // 5))
            times.append(time_ged_sync(base, variant))

        st = stats(times)
        passed = st["p99"] < SLA_MS
        row = {
            "graph_size": size,
            "n_trials": n_trials,
            "p50_ms": st["p50"],
            "p95_ms": st["p95"],
            "p99_ms": st["p99"],
            "max_ms": st["max"],
            "mean_ms": st["mean"],
            "passed_sla": passed,
        }
        results.append(row)
        status = "PASS" if passed else "FAIL"
        print(f"  size={size:4d}  p50={st['p50']:7.2f}ms  "
              f"p95={st['p95']:7.2f}ms  p99={st['p99']:7.2f}ms  [{status}]")
    return results


async def run_async_benchmark(sizes: list[int] = GRAPH_SIZES,
                               n_trials: int = N_TRIALS) -> list[dict]:
    results = []
    for size in sizes:
        times: list[float] = []
        for _ in range(n_trials):
            base = random_sequence(size)
            variant = similar_sequence(base, edit_distance=max(1, size // 5))
            t = await time_ged_async(base, variant)
            times.append(t)

        st = stats(times)
        passed = st["p99"] < SLA_MS
        row = {
            "graph_size": size,
            "n_trials": n_trials,
            "p50_ms": st["p50"],
            "p95_ms": st["p95"],
            "p99_ms": st["p99"],
            "max_ms": st["max"],
            "mean_ms": st["mean"],
            "passed_sla": passed,
        }
        results.append(row)
        status = "PASS" if passed else "FAIL"
        print(f"  size={size:4d}  p50={st['p50']:7.2f}ms  "
              f"p95={st['p95']:7.2f}ms  p99={st['p99']:7.2f}ms  [{status}]")
    return results


# ---------------------------------------------------------------------------
# SLA validation (fast path — just the boundary point)
# ---------------------------------------------------------------------------

def validate_sla(n_trials: int = 500) -> dict:
    """
    Validate SLA at exactly SLA_NODES (50) with higher trial count.
    Returns pass/fail and detailed percentile breakdown.
    """
    times: list[float] = []
    for _ in range(n_trials):
        base    = random_sequence(SLA_NODES)
        variant = similar_sequence(base, edit_distance=SLA_NODES // 5)
        times.append(time_ged_sync(base, variant))

    st = stats(times)
    passed = st["p99"] < SLA_MS
    return {
        "sla_target_nodes": SLA_NODES,
        "sla_target_ms": SLA_MS,
        "n_trials": n_trials,
        **st,
        "sla_passed": passed,
        "verdict": "PASS" if passed else f"FAIL — p99 {st['p99']}ms > {SLA_MS}ms target",
    }


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def save_csv(results: list[dict], filename: str = "ged_latency.csv"):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / filename
    if not results:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults saved → {path}")


# ---------------------------------------------------------------------------
# Comparison reporter
# ---------------------------------------------------------------------------

def compare_report(sync_results: list[dict],
                   async_results: list[dict]) -> str:
    lines = [
        "\n" + "─" * 70,
        "  Sync vs Async GED latency comparison",
        "─" * 70,
        f"  {'Size':>6}  {'Sync p99':>10}  {'Async p99':>10}  {'Winner':>8}",
        "─" * 70,
    ]
    for s_row, a_row in zip(sync_results, async_results):
        size = s_row["graph_size"]
        sp99 = s_row["p99_ms"]
        ap99 = a_row["p99_ms"]
        winner = "sync" if sp99 <= ap99 else "async"
        lines.append(
            f"  {size:>6}  {sp99:>9.2f}ms  {ap99:>9.2f}ms  {winner:>8}"
        )
    lines.append("─" * 70)
    lines.append(
        f"  Note: async beats sync for graph sizes > {
            next((s['graph_size'] for s, a in zip(sync_results, async_results)
                  if a['p99_ms'] < s['p99_ms']), 'N/A')
        } nodes"
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="GED latency benchmark")
    parser.add_argument("--async-compare", action="store_true",
                        help="Also run async benchmark and print comparison")
    parser.add_argument("--sla-check-only", action="store_true",
                        help="Run only the SLA validation at 50 nodes")
    parser.add_argument("--trials", type=int, default=N_TRIALS,
                        help=f"Trials per size point (default: {N_TRIALS})")
    args = parser.parse_args()

    if args.sla_check_only:
        print("\n" + "─" * 70)
        print("  SLA validation: GED p99 < 100ms at 50 nodes")
        print("─" * 70)
        result = validate_sla(n_trials=500)
        for k, v in result.items():
            print(f"  {k:<25}: {v}")
        print("─" * 70)
        return

    print("\n" + "─" * 70)
    print("  Synchronous GED benchmark")
    print("─" * 70)
    sync_results = run_sync_benchmark(n_trials=args.trials)
    save_csv(sync_results, "ged_latency_sync.csv")

    if args.async_compare:
        print("\n" + "─" * 70)
        print("  Async GED benchmark")
        print("─" * 70)
        async_results = asyncio.run(run_async_benchmark(n_trials=args.trials))
        save_csv(async_results, "ged_latency_async.csv")
        print(compare_report(sync_results, async_results))

    # SLA boundary check
    print("\n" + "─" * 70)
    print("  SLA boundary validation (500 trials at 50 nodes)")
    print("─" * 70)
    sla = validate_sla(n_trials=500)
    for k, v in sla.items():
        print(f"  {k:<25}: {v}")
    print("─" * 70)


if __name__ == "__main__":
    main()
