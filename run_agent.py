#!/usr/bin/env python3
# run_agent.py

import json
import time
import argparse
from typing import Dict, Any, List

# ---- Runtime adapters ----
from src.analysis.runtime_adapter import AgentRuntime
from src.analysis.langchain_runtime import LangChainRuntime
from src.analysis.autogen_runtime import AutoGenRuntime

# ---- Analysis modules ----
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.analysis.overhead_analyzer import OverheadAnalyzer

# ---- Utilities (assumed existing in repo) ----
from src.analysis.energy_meter import EnergyMeter
from src.analysis.carbon_estimator import CarbonEstimator
from src.reporting.leaderboard import generate_leaderboard
from src.visualization.plot_pareto import plot_pareto_frontier

from src.analysis.streaming import MetricsStreamer

# ---------------------------------------------------------------------
# Runtime factory
# ---------------------------------------------------------------------

def create_runtime(framework: str, config: Dict[str, Any]) -> AgentRuntime:
    if framework == "langchain":
        rt = LangChainRuntime()
    elif framework == "autogen":
        rt = AutoGenRuntime()
    else:
        raise ValueError(f"Unsupported framework: {framework}")

    rt.init(config)
    return rt


# ---------------------------------------------------------------------
# Single query execution
# ---------------------------------------------------------------------

def run_single_query(
    runtime: AgentRuntime,
    query: Dict[str, Any],
    overhead: OverheadAnalyzer,
) -> Dict[str, Any]:

    energy_meter = EnergyMeter()
    carbon = CarbonEstimator()

    start_time = time.time()
    energy_meter.start()

    # ---- Run agent ----
    result = runtime.run(query)

    energy_meter.stop()
    latency = time.time() - start_time

    energy = energy_meter.joules()
    carbon_kg = carbon.estimate(energy)

    metrics = {
        "query_id": query.get("id"),
        "latency": latency,
        "energy": energy,
        "carbon": carbon_kg,
        **result,
    }

    # ---- Framework overhead ----
    metrics = overhead.compute(metrics)
    return metrics


# ---------------------------------------------------------------------
# Budget-aware query switching
# ---------------------------------------------------------------------

def within_budget(metrics: List[Dict], budget: Dict[str, float]) -> bool:
    total_energy = sum(m["energy"] for m in metrics)
    total_carbon = sum(m["carbon"] for m in metrics)

    if budget.get("max_energy") and total_energy > budget["max_energy"]:
        return False
    if budget.get("max_carbon") and total_carbon > budget["max_carbon"]:
        return False
    return True


# ---------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", default="agentbeats_results.json")
    args = parser.parse_args()
    streamer = MetricsStreamer(enabled=True)

    with open(args.config) as f:
        config = json.load(f)

    # ---- AgentBeats requirement ----
    queries: List[Dict] = config["queries"]
    framework = config["framework"]
    runtime_config = config["runtime"]

    budget = config.get("budget", {})

    # ---- Baseline calibration ----
    overhead = OverheadAnalyzer(
        baseline_latency=config.get("baseline_latency", 0.01),
        baseline_energy=config.get("baseline_energy", 0.0001),
    )

    runtime = create_runtime(framework, runtime_config)

    all_metrics: List[Dict] = []

    for query in queries:
        streamer.emit("query_start", {"query_id": query["id"]})
        metrics = run_single_query(runtime, query, overhead)
        all_metrics.append(metrics)

        # ---- Budget-aware early stop ----
        if not within_budget(all_metrics, budget):
            print("⚠️ Budget exceeded — stopping remaining queries.")
            break
    
    streamer.emit("query_end", metrics)
    runtime.finalize()

    # -----------------------------------------------------------------
    # Pareto aggregation
    # -----------------------------------------------------------------

    pareto = ParetoAnalyzer()
    frontier = pareto.pareto_frontier(all_metrics)

    # -----------------------------------------------------------------
    # Leaderboard & visualization
    # -----------------------------------------------------------------

    leaderboard = generate_leaderboard(all_metrics)

    plot_pareto_frontier(
        all_metrics,
        x="energy",
        y="accuracy",
        highlight=frontier,
        output_path="pareto.png",
    )

    # -----------------------------------------------------------------
    # AgentBeats-compatible output
    # -----------------------------------------------------------------

    result_bundle = {
        "framework": framework,
        "queries": queries,          # MUST be array
        "results": all_metrics,
        "pareto_frontier": frontier,
        "leaderboard": leaderboard,
    }

    with open(args.output, "w") as f:
        json.dump(result_bundle, f, indent=2)

    print(f"✅ Results written to {args.output}")
    print("🏁 Pareto frontier size:", len(frontier))


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()
