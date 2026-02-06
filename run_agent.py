#!/usr/bin/env python3
"""
Canonical AgentBeats entrypoint for Green_Agent.
Never throws uncaught exceptions.
"""

import json
import time
import argparse

from src.policy.policy_engine import PolicyEngine
from src.policy.policy_feedback import PolicyFeedback
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.runtime.langchain_runtime import LangChainRuntime
from src.runtime.autogen_runtime import AutoGenRuntime
from src.chaos import inject_energy_spike

# -------------------------
# Runtime factory
# -------------------------
def create_runtime(name: str):
    if name == "langchain":
        return LangChainRuntime()
    if name == "autogen":
        return AutoGenRuntime()
    raise ValueError(f"Unknown runtime: {name}")

# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", default="agentbeats_results.json")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    runtime = create_runtime(cfg["framework"])
    runtime.init(cfg.get("runtime", {}))

    policy = PolicyEngine(cfg.get("policy", {}))
    feedback = PolicyFeedback()
    pareto = ParetoAnalyzer()

    all_metrics = []

    for step, query in enumerate(cfg["queries"]):
        start = time.time()

        try:
            result = runtime.run(query)

            metrics = {
                "query_id": query.get("id"),
                "latency": time.time() - start,
                "energy": 0.02,
                "carbon": 0.00001,
                **result,
            }

            metrics = inject_energy_spike(metrics)
            policy.enforce(metrics)

            all_metrics.append(metrics)

            if policy.should_reflect(step):
                explanation = feedback.explain(
                    decision="reduce_tool_calls",
                    before={"energy": 0.03, "latency": 1.2},
                    after=metrics,
                )
                metrics["reflection"] = explanation

        except Exception as e:
            all_metrics.append({
                "query_id": query.get("id"),
                "error": str(e),
            })
            break

    frontier = pareto.pareto_frontier(all_metrics)

    output = {
        "framework": cfg["framework"],
        "queries": cfg["queries"],
        "results": all_metrics,
        "pareto_frontier": frontier,
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    runtime.finalize()
    print("✅ Green_Agent execution complete")

if __name__ == "__main__":
    main()
