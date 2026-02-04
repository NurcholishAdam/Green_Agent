"""
Main AgentBeats-compatible entrypoint.

This file is intentionally defensive:
- Never raises uncaught exceptions
- Always emits metrics
"""

import json
import os
import time
import traceback

from analysis.pareto_analyzer import ParetoAnalyzer
from policy.policy_engine import PolicyEngine
from policy.policy_reporter import PolicyReporter
from chaos import inject_chaos


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def run_single_query(query_mode: str) -> dict:
    start = time.time()

    # --- Simulated metrics (replace with real hooks if needed)
    metrics = {
        "energy_wh": 0.04 if query_mode == "low_energy" else 0.07,
        "carbon_kg": 0.0008,
        "latency_s": time.time() - start,
        "memory_mb": 120.0,
        "framework_overhead_latency": 0.01,
        "framework_overhead_energy": 0.005,
        "tool_calls": 4,
        "conversation_depth": 2,
        "accuracy": 0.82 if query_mode != "low_energy" else 0.75,
    }

    return metrics


def main():
    results = []
    policy = PolicyEngine("config/green_policy.yml")
    reporter = PolicyReporter()
    analyzer = ParetoAnalyzer()

    query_mode = os.getenv("QUERY_MODE", "balanced")

    try:
        metrics = run_single_query(query_mode)
        metrics = inject_chaos(metrics, enabled=True)
        metrics = policy.enforce(metrics)
        results.append(metrics)

    except Exception:
        results.append({
            "error": "runtime_failure",
            "trace": traceback.format_exc(),
        })

    pareto = analyzer.frontier(results)

    reporter.write_json("results.json", results)
    reporter.write_json("pareto.json", pareto)

    print(json.dumps({
        "status": "ok",
        "runs": len(results),
        "pareto_points": len(pareto)
    }, indent=2))


if __name__ == "__main__":
    main()
