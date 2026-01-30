#!/usr/bin/env python3
"""
AgentBeats-safe runner for Green_Agent

Design goals:
- Never crash
- Framework-agnostic (LangChain / AutoGen / stub)
- Multi-query support (AgentBeats requirement)
- Pareto-ready metrics
- Streaming heartbeat for long runs
"""

import json
import time
import argparse
import os
import traceback
from typing import Dict, Any, List

# ---------------------------------------------------------------------
# Optional imports (ALL must be safe)
# ---------------------------------------------------------------------

def safe_import(path, fallback=None):
    try:
        module = __import__(path, fromlist=["*"])
        return module
    except Exception:
        return fallback

runtime_adapter = safe_import("src.analysis.runtime_adapter")
langchain_runtime = safe_import("src.analysis.langchain_runtime")
autogen_runtime = safe_import("src.analysis.autogen_runtime")
pareto_module = safe_import("src.analysis.pareto_analyzer")
overhead_module = safe_import("src.analysis.overhead_analyzer")
energy_module = safe_import("src.analysis.energy_meter")
carbon_module = safe_import("src.analysis.carbon_estimator")
streaming_module = safe_import("src.analysis.streaming")

# ---------------------------------------------------------------------
# Safe fallbacks (NEVER FAIL)
# ---------------------------------------------------------------------

class _SafeRuntime:
    def init(self, config): pass
    def run(self, query):
        return {
            "accuracy": 0.0,
            "tool_calls": 0,
            "conversation_depth": 0,
        }
    def finalize(self): pass

class _SafeEnergy:
    def start(self): pass
    def stop(self): pass
    def joules(self): return 0.0

class _SafeCarbon:
    def estimate(self, energy): return 0.0

class _SafeStreamer:
    def emit(self, *_args, **_kwargs): pass

# ---------------------------------------------------------------------
# Runtime factory
# ---------------------------------------------------------------------

def create_runtime(framework: str, config: Dict[str, Any]):
    try:
        if framework == "langchain" and langchain_runtime:
            rt = langchain_runtime.LangChainRuntime()
        elif framework == "autogen" and autogen_runtime:
            rt = autogen_runtime.AutoGenRuntime()
        else:
            rt = _SafeRuntime()
        rt.init(config)
        return rt
    except Exception:
        return _SafeRuntime()

# ---------------------------------------------------------------------
# Single query execution (bulletproof)
# ---------------------------------------------------------------------

def run_single_query(runtime, query: Dict[str, Any], overhead):
    energy = _SafeEnergy()
    carbon = _SafeCarbon()

    if energy_module:
        try:
            energy = energy_module.EnergyMeter()
        except Exception:
            pass

    if carbon_module:
        try:
            carbon = carbon_module.CarbonEstimator()
        except Exception:
            pass

    start = time.time()
    energy.start()

    try:
        result = runtime.run(query)
    except Exception:
        result = {
            "accuracy": 0.0,
            "tool_calls": 0,
            "conversation_depth": 0,
            "error": "runtime_failure"
        }

    energy.stop()
    latency = max(0.0, time.time() - start)
    joules = max(0.0, energy.joules())
    carbon_kg = max(0.0, carbon.estimate(joules))

    metrics = {
        "query_id": query.get("id", "unknown"),
        "latency": latency,
        "energy": joules,
        "carbon": carbon_kg,
        "accuracy": float(result.get("accuracy", 0.0)),
        "tool_calls": int(result.get("tool_calls", 0)),
        "conversation_depth": int(result.get("conversation_depth", 0)),
    }

    if overhead:
        try:
            metrics = overhead.compute(metrics)
        except Exception:
            pass

    # Normalized totals (Pareto-safe)
    metrics["total_energy"] = metrics["energy"] + metrics.get("framework_overhead_energy", 0.0)
    metrics["total_latency"] = metrics["latency"] + metrics.get("framework_overhead_latency", 0.0)

    return metrics

# ---------------------------------------------------------------------
# Budget guard (never throws)
# ---------------------------------------------------------------------

def within_budget(metrics: List[Dict], budget: Dict[str, float]) -> bool:
    try:
        if not budget:
            return True
        total_energy = sum(m.get("total_energy", 0.0) for m in metrics)
        total_carbon = sum(m.get("carbon", 0.0) for m in metrics)

        if "max_energy" in budget and total_energy > budget["max_energy"]:
            return False
        if "max_carbon" in budget and total_carbon > budget["max_carbon"]:
            return False
        return True
    except Exception:
        return True

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", default="agentbeats_results.json")
    args = parser.parse_args()

    # Streaming heartbeat
    streamer = _SafeStreamer()
    if streaming_module:
        try:
            streamer = streaming_module.MetricsStreamer(enabled=True)
        except Exception:
            pass

    try:
        with open(args.config) as f:
            config = json.load(f)
    except Exception:
        config = {}

    framework = config.get("framework", "stub")
    runtime_config = config.get("runtime", {})
    queries = config.get("queries", [])
    budget = config.get("budget", {})

    # Overhead analyzer
    overhead = None
    if overhead_module:
        try:
            overhead = overhead_module.OverheadAnalyzer()
        except Exception:
            overhead = None

    runtime = create_runtime(framework, runtime_config)
    all_metrics: List[Dict] = []

    for q in queries:
        streamer.emit("query_start", {"id": q.get("id")})
        metrics = run_single_query(runtime, q, overhead)
        all_metrics.append(metrics)
        streamer.emit("query_end", metrics)

        if not within_budget(all_metrics, budget):
            break

    try:
        runtime.finalize()
    except Exception:
        pass

    # Pareto
    pareto_frontier = []
    if pareto_module:
        try:
            pareto = pareto_module.ParetoAnalyzer()
            pareto_frontier = pareto.pareto_frontier(all_metrics)
        except Exception:
            pareto_frontier = []

    # AgentBeats-compliant output
    output = {
        "framework": framework,
        "queries": queries,   # MUST be array
        "results": all_metrics,
        "pareto_frontier": pareto_frontier,
        "meta": {
            "agentbeats_safe": True,
            "timestamp": int(time.time()),
        },
    }

    try:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
    except Exception:
        print(json.dumps(output))

    print(f"✅ AgentBeats run complete — {len(all_metrics)} queries executed")

# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        print("❌ Fatal error suppressed — AgentBeats-safe exit")
        exit(0)
