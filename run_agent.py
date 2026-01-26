"""
run_agent.py

Multi-query, Pareto-aware green benchmarking entrypoint.
Compatible with AgentBeats leaderboards.
"""

import json
import os
import random
from typing import Dict, List

from docker_metrics_collector import DockerMetricsCollector
from src.constraints.energy_budget import check_energy_budget
from src.analysis.green_score import compute_green_score
from src.analysis.pareto import pareto_front
from src.feedback.energy_feedback import generate_energy_feedback


# -------------------------------------------------
# Agent inference (replace with real agent)
# -------------------------------------------------

def run_agent_inference(mode: str) -> float:
    """
    Simulated agent inference.

    mode affects accuracy/energy tradeoff.
    """
    base = {
        "low_energy": 0.65,
        "balanced": 0.72,
        "high_accuracy": 0.78,
    }[mode]

    return base + random.uniform(-0.01, 0.01)


# -------------------------------------------------
# Query definitions (AgentBeats-aligned)
# -------------------------------------------------

def get_queries() -> List[Dict]:
    return [
        {
            "id": "low-energy",
            "mode": "low_energy",
            "max_energy_wh": 0.03,
        },
        {
            "id": "balanced",
            "mode": "balanced",
            "max_energy_wh": 0.06,
        },
        {
            "id": "high-accuracy",
            "mode": "high_accuracy",
            "max_energy_wh": None,
        },
    ]


# -------------------------------------------------
# Main
# -------------------------------------------------

def main():
    carbon_intensity = float(os.getenv("CARBON_INTENSITY", "0.0004"))
    collector = DockerMetricsCollector(carbon_intensity=carbon_intensity)

    all_results: List[Dict] = []

    for query in get_queries():
        mode = query["mode"]

        metrics = collector.run_and_measure(
            fn=lambda: run_agent_inference(mode),
            runs=5,
        )

        passed, reason = check_energy_budget(
            metrics,
            max_energy_wh=query["max_energy_wh"],
        )

        result = {
            "query_id": query["id"],
            "mode": mode,
            "passed": passed,
            "reason": reason,
            **metrics,
            "green_score": compute_green_score(
                metrics["accuracy"],
                metrics["energy"],
                metrics["latency"],
            ),
            "feedback": generate_energy_feedback(metrics),
        }

        all_results.append(result)

    # -------------------------------------------------
    # Pareto aggregation
    # -------------------------------------------------
    pareto = pareto_front(
        all_results,
        objectives=("accuracy", "energy", "latency"),
    )

    output = {
        "results": all_results,
        "pareto_front": pareto,
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
