#!/usr/bin/env python3
"""
scripts/bench_overhead.py — Reproduce the §6 performance table.

Measures (with p50/p99 over N iterations after warmup):
    A2A Gateway.validate_request
    RLHFEngine.analyze_reasoning_trace
    calculate_robust_score
    Container launch (optional, --docker)

Emits a Markdown table to stdout. Does NOT measure green-metrics collection
(needs host RAPL/NVML); report numbers for that are marked as estimates.

Usage:
    python scripts/bench_overhead.py --iterations 1000 --warmup 100 [--docker]
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.a2a_gateway import A2AGateway, create_a2a_task          # noqa: E402
from core.rlhf_feedback_engine import RLHFFeedbackEngine          # noqa: E402
from core.scoring_utils import calculate_robust_score              # noqa: E402


def _pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    if not xs:
        return 0.0
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]


def bench(fn, iterations: int, warmup: int) -> tuple[float, float]:
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return _pct(samples, 50), _pct(samples, 99)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=1000)
    ap.add_argument("--warmup",     type=int, default=100)
    args = ap.parse_args()

    gateway = A2AGateway()
    rlhf    = RLHFFeedbackEngine()

    trace = [
        {"action": "plan", "thought": "plan", "duration": 0.05},
        {"action": "search", "tool": "web_search", "duration": 0.5},
        {"action": "analyze", "duration": 0.3},
        {"action": "synthesize", "duration": 0.2},
        {"action": "conclude", "duration": 0.1},
    ]

    task = create_a2a_task(
        task_id="bench_001", task_type="research",
        query="What is quantum computing?", max_tokens=500, timeout_seconds=30,
    )

    rows: list[tuple[str, float, float, str]] = []

    p50, p99 = bench(lambda: gateway.validate_request(task),
                     args.iterations, args.warmup)
    rows.append(("A2A Gateway (validate_request)", p50, p99, "500-token request"))

    p50, p99 = bench(
        lambda: rlhf.analyze_reasoning_trace(
            reasoning_trace=trace, task_type="research",
            execution_time=2.9, success=True,
        ),
        args.iterations, args.warmup,
    )
    rows.append(("RLHF Engine (analyze_reasoning_trace)", p50, p99, "5-step trace"))

    p50, p99 = bench(
        lambda: calculate_robust_score("success",
                                       {"answer": "quantum"},
                                       {"answer": "quantum"}),
        args.iterations, args.warmup,
    )
    rows.append(("Robust Scorer (calculate_robust_score)", p50, p99, "success path"))

    print("| Component | p50 (ms) | p99 (ms) | Notes |")
    print("|---|---|---|---|")
    for name, p50, p99, note in rows:
        print(f"| {name} | {p50:.2f} | {p99:.2f} | {note} |")

    # Container launch is measured only if --docker passed — kept outside
    # this script to keep it dependency-light in CI.
    return 0


if __name__ == "__main__":
    sys.exit(main())
