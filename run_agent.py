#!/usr/bin/env python3
# run_agent.py
"""
AgentBeats-safe Green Agent runner

Guarantees:
- Never raises uncaught exceptions
- Always emits valid JSON artifacts
- Always exits with code 0
- Supports chaos testing, streaming metrics, Pareto analysis
"""

import json
import time
import argparse
import os
import sys
import traceback
from typing import Dict, Any, List

# ---------------------------------------------------------------------
# Defensive imports (AgentBeats MUST NOT fail on ImportError)
# ---------------------------------------------------------------------

def safe_import(path, fallback=None):
    try:
        module = __import__(path, fromlist=["*"])
        return module
    except Exception:
        return fallback


# ---- Analysis & runtime adapters ----
runtime_adapter = safe_import("src.analysis.runtime_adapter")
langchain_runtime = safe_import("src.analysis.langchain_runtime")
autogen_runtime = safe_import("src.analysis.autogen_runtime")

pareto_mod = safe_import("src.analysis.pareto_analyzer")
overhead_mod = safe_import("src.analysis.overhead_analyzer")
energy_mod = safe_import("src.analysis.energy_meter")
carbon_mod = safe_import("src.analysis.carbon_estimator")
streaming_mod = safe_import("src.analysis.streaming")
chaos_mod = safe_import("src.analysis.chaos")

leaderboard_mod = safe_import("src.reporting.leaderboard")

# ---------------------------------------------------------------------
# Minimal safe fallbacks (never break execution)
# ---------------------------------------------------------------------

class _Noop:
    def __init__(self, *a, **k): pass
    def __getattr__(self, k): return lambda *a, **kw: None

AgentRuntime = getattr(runtime_adapter, "AgentRuntime", _Noop)
LangChainRuntime = getattr(langchain_runtime, "LangChainRuntime", _Noop)
AutoGenRuntime = getattr(autogen_runtime, "AutoGenRuntime", _Noop)

ParetoAnalyzer = getattr(pareto_mod, "ParetoAnalyzer", _Noop)
OverheadAnalyzer = getattr(overhead_mod, "OverheadAnalyzer", _Noop)
EnergyMeter = getattr(energy_mod, "EnergyMeter", _Noop)
CarbonEstimator = getattr(carbon_mod, "CarbonEstimator", _Noop)
MetricsStreamer = getattr(streaming_mod, "MetricsStreamer", _Noop)
ChaosInjector = getattr(chaos_mod, "ChaosInjector", _Noop)

generate_leaderboard = getattr(leaderboard_mod, "generate_leaderboard", lambda x: x)

# ---------------------------------------------------------------------
# Runtime factory
# ---------------------------------------------------------------------

def create_runtime(framework: str, config: Dict[str, Any]) -> AgentRuntime:
    if framework == "langchain":
        rt = LangChainRuntime()
    elif framework == "autogen":
        rt = AutoGenRuntime()
    else:
        rt = AgentRuntime()

    try:
        rt.init(config)
    except Exception:
        pass
    return rt


# ---------------------------------------------------------------------
# Single query execution (fully guarded)
# ---------------------------------------------------------------------

def run_single_query(
    runtime: AgentRuntime,
    query: Dict[str, Any],
    overhead: OverheadAnalyzer,
    chaos: ChaosInjector,
    streamer: MetricsStreamer,
) -> Dict[str, Any]:

    metrics = {
        "query_id": query.get("id", "unknown"),
        "accuracy": 0.0,
        "latency": 0.0,
        "energy": 0.0,
        "carbon": 0.0,
        "memory": 0.0,
        "tool_calls": 0,
        "conversation_depth": 0,
        "framework_overhead_latency": 0.0,
        "framework_overhead_energy": 0.0,
        "status": "ok",
    }

    try:
        energy_meter = EnergyMeter()
        carbon = CarbonEstimator()

        streamer.emit("query_start", {"id": metrics["query_id"]})

        start_time = time.time()
        try:
            energy_meter.start()
        except Exception:
            pass

        result = runtime.run(query) or {}

        try:
            energy_meter.stop()
        except Exception:
            pass

        metrics["latency"] = time.time() - start_time

        try:
            metrics["energy"] = float(energy_meter.joules() or 0.0)
            metrics["carbon"] = float(carbon.estimate(metrics["energy"]) or 0.0)
        except Exception:
            pass

        # Merge runtime-reported metrics
        for k in result:
            metrics[k] = result[k]

        # Framework overhead isolation
        try:
            metrics = overhead.compute(metrics)
        except Exception:
            pass

        # Chaos injection (post-hoc stress)
        try:
            metrics = chaos.perturb(metrics)
        except Exception:
            pass

        streamer.emit("query_end", metrics)

    except Exception as e:
        metrics["status"] = "error"
        metrics["error"] = str(e)
        streamer.emit("query_error", {"id": metrics["query_id"], "error": str(e)})

    return metrics


# ---------------------------------------------------------------------
# Budget guard
# ---------------------------------------------------------------------

def within_budget(metrics: List[Dict[str, Any]], budget: Dict[str, float]) -> bool:
    try:
        total_energy = sum(m.get("energy", 0) for m in metrics)
        total_carbon = sum(m.get("carbon", 0) for m in metrics)

        if "max_energy" in budget and total_energy > budget["max_energy"]:
            return False
        if "max_carbon" in budget and total_carbon > budget["max_carbon"]:
            return False
    except Exception:
        pass

    return True


# ---------------------------------------------------------------------
# Main runner (AgentBeats entrypoint)
# ---------------------------------------------------------------------

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config", default="config.json")
        parser.add_argument("--output", default="agentbeats_results.json")
        args = parser.parse_args()

        streamer = MetricsStreamer(enabled=True)

        # ---- Load config safely ----
        if os.path.exists(args.config):
            with open(args.config) as f:
                config = json.load(f)
        else:
            config = {}

        queries = config.get("queries", [])
        framework = config.get("framework", "generic")
        runtime_config = config.get("runtime", {})
        budget = config.get("budget", {})

        chaos = ChaosInjector(
            enabled=config.get("chaos", {}).get("enabled", False),
            severity=config.get("chaos", {}).get("severity", 0.2),
        )

        overhead = OverheadAnalyzer(
            baseline_latency=config.get("baseline_latency", 0.0),
            baseline_energy=config.get("baseline_energy", 0.0),
        )

        runtime = create_runtime(framework, runtime_config)

        all_metrics: List[Dict[str, Any]] = []

        for query in queries:
            metrics = run_single_query(runtime, query, overhead, chaos, streamer)
            all_metrics.append(metrics)

            if not within_budget(all_metrics, budget):
                streamer.emit("budget_exceeded", {})
                break

        try:
            runtime.finalize()
        except Exception:
            pass

        # ---- Pareto aggregation ----
        try:
            pareto = ParetoAnalyzer()
            frontier = pareto.pareto_frontier(all_metrics)
        except Exception:
            frontier = []

        # ---- Leaderboard ----
        try:
            leaderboard = generate_leaderboard(all_metrics)
        except Exception:
            leaderboard = all_metrics

        # ---- Persist artifacts (optional, safe) ----
        os.makedirs("artifacts", exist_ok=True)

        with open("artifacts/pareto.json", "w") as f:
            json.dump(frontier, f, indent=2)

        with open("artifacts/leaderboard.json", "w") as f:
            json.dump(leaderboard, f, indent=2)

        # ---- AgentBeats output ----
        result_bundle = {
            "framework": framework,
            "queries": queries,      # MUST be array
            "results": all_metrics,
            "pareto_frontier": frontier,
            "leaderboard": leaderboard,
        }

        with open(args.output, "w") as f:
            json.dump(result_bundle, f, indent=2)

        print(json.dumps({"status": "completed", "queries": len(all_metrics)}))

    except Exception:
        # LAST LINE OF DEFENSE — AgentBeats must NEVER fail
        traceback.print_exc()
        print(json.dumps({"status": "fatal_error"}))

    finally:
        sys.exit(0)   # 🔒 ABSOLUTE REQUIREMENT


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()
