"""
Enterprise Green Policy Engine.

Applies hard constraints and weighted priorities.
"""

from typing import Dict


class PolicyEngine:
    def __init__(self, policy: Dict):
        self.policy = policy
        self.weights = policy.get("optimization", {}).get(
            "priority_weight",
            {"accuracy": 0.5, "sustainability": 0.5},
        )
        self.constraints = policy.get("constraints", {})

    def check_constraints(self, metrics: Dict) -> bool:
        if metrics.get("energy", 0) > self.constraints.get("max_energy_per_task_wh", float("inf")):
            return False
        if metrics.get("carbon", 0) > self.constraints.get("max_carbon_per_task_kg", float("inf")):
            return False
        if metrics.get("latency", 0) > self.constraints.get("max_latency_seconds", float("inf")):
            return False
        return True

    def weighted_score(self, metrics: Dict) -> float:
        """
        Lower score is better.
        """
        acc = 1.0 - metrics.get("accuracy", 0)
        sustain = (
            metrics.get("energy", 0)
            + metrics.get("carbon", 0)
            + metrics.get("framework_overhead_energy", 0)
        )

        return (
            self.weights["accuracy"] * acc
            + self.weights["sustainability"] * sustain
        )
