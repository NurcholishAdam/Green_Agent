#!/usr/bin/env python3
"""
run_agent.py

AgentBeats-compatible runner:
- multi-query execution
- container-internal energy & carbon estimation
- framework adapters (LangChain / AutoGen)
- Pareto aggregation
- budget-aware early stopping
"""

import argparse
import json
from typing import Dict, Any, List, Callable

from docker_metrics_collector import DockerMetricsCollector

# -----------------------------
# Framework runtimes (pure adapters)
# -----------------------------

from src.analysis.runtime_adapter import AgentRuntime
from src.analysis.langchain_runtime import LangChainRuntime
from src.analysis.autogen_runtime import AutoGenRuntime

# -----------------------------
# Analysis utilities
# -----------------------------

from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.analysis.overhead_analyzer import OverheadAnalyzer
from src.analysis.streaming import MetricsStreamer


# =====================================================
# Runtime factory
# =====================================================

def create_runtime(framework: str, config: Dict[str, Any]) -> AgentRuntime:
    """
    Instantiate a framework runtime.
    Must be deterministic and offline.
    """
    if framework == "langchain":
        runtime = LangChainRuntime()
    elif framework == "autogen":
        runtime = AutoGenRuntime()
    else:
        raise ValueError(f"Unsupported framework: {framework}")

    runtime.init(config or {})
    return runtime


# =====================================================
# Single query execution
# =====================================================

def run_single_query(
    runtime: AgentRuntime,
    query: Dict[str, Any],
    overhead: OverheadAnalyzer,
    collector: DockerMetricsCollector,
) -> Dict[str, Any]:
    """
    Execute exactly ONE query and measure metrics.
    """

    def _execute() -> float:
        """
        Wrapper required by DockerMetricsCollector.
        Must return accuracy or score (float).
        """
        result = runtime.run(query)
        return float(result.get("accuracy", 0.0))

    metrics = collector.run_and_measure(_execute, runs=1)

    # Stable, explicit metric schema
    record = {
        "query_id": query.get("id"),
        "latency": metrics["latency"],
        "latency_variance": metrics["latency_variance"],
        "cpu_time": metrics["cpu_time"],
        "energy": metrics["energy"],
        "carbon": metrics["carbon"],
        "memory": metrics["memory"],
        "accuracy": metrics["accuracy"],
        "framework": runtime.name,
        "tool_calls": runtime.tool_calls,
        "conversation_depth": runtime.depth,
    }

    # Framework overhead attribution
    record = overhead.compute(record)

    return record


# =====================================================
# Budget logic
# =====================================================

def within_budget(
    records: List[Dict[str, Any]],
    budget: Dict[str, float],
) -> bool:
    """
    Check cumulative energy / carbon budgets.
    """
    if not budget:
        return True

    total_energy = sum(r.get("energy", 0.0) for r in records)
    total_carbon = sum(r.get("carbon", 0.0) for r in records)

    if "max_energy" in budget and total_energy > budget["max_energy"]:
        return False
    if "max_carbon" in budget and total_carbon > budget["max_carbon"]:
        return False

    return True


# =====================================================
# Main entry
# =====================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", default="agentbeats_results.json")
    args = parser.parse_args()

    # Streaming is side-channel only (safe)
    streamer = MetricsStreamer(enabled=True)

    # -----------------------------
    # Load config
    # -----------------------------

    with open(args.config, "r") as f:
        config = json.load(f)

    # AgentBeats REQUIRES queries to be an array
    queries: List[Dict[str, Any]] = config["queries"]

    framework = config["framework"]
    runtime_config = config.get("runtime", {})
    budget = config.get("budget", {})

    # -----------------------------
    # Initialize components
    # -----------------------------

    runtime = create_runtime(framework, runtime_config)

    overhead = OverheadAnalyzer(
        baseline_latency=config.get("baseline_latency", 0.0),
        baseline_energy=config.get("baseline_energy", 0.0),
    )

    collector = DockerMetricsCollector()

    all_results: List[Dict[str, Any]] = []

    # -----------------------------
    # Execute queries
    # -----------------------------

    for query in queries:
        streamer.emit("query_start", {"query_id": query.get("id")})

        record = run_single_query(
            runtime=runtime,
            query=query,
            overhead=overhead,
            collector=collector,
        )

        all_results.append(record)

        streamer.emit("query_end", record)

        if not within_budget(all_results, budget):
            print("⚠️ Budget exceeded — stopping early.")
            break

    runtime.finalize()

    # -----------------------------
    # Pareto aggregation (offline)
    # -----------------------------

    pareto = ParetoAnalyzer()
    frontier = pareto.pareto_frontier(all_results)

    # -----------------------------
    # AgentBeats output bundle
    # -----------------------------

    output = {
        "framework": framework,
        "queries": queries,            # REQUIRED ARRAY
        "results": all_results,
        "pareto_frontier": frontier,
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✅ AgentBeats results written to {args.output}")
    print(f"🏁 Pareto frontier size: {len(frontier)}")


# =====================================================
# Entrypoint
# =====================================================

if __name__ == "__main__":
    main()
