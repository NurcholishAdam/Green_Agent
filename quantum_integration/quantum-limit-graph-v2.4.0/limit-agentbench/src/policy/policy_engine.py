"""
Green policy enforcement engine.

Hard constraints = circuit breakers.
Soft constraints = warnings.
"""

import yaml
from typing import Dict


class PolicyEngine:
    def __init__(self, policy_path: str):
        with open(policy_path, "r") as f:
            self.policy = yaml.safe_load(f)

    def enforce(self, metrics: Dict) -> Dict:
        constraints = self.policy.get("constraints", {})
        violations = []

        if metrics.get("energy_wh", 0) > constraints.get("max_energy_per_task_wh", float("inf")):
            violations.append("energy_budget_exceeded")

        if metrics.get("carbon_kg", 0) > constraints.get("max_carbon_per_task_kg", float("inf")):
            violations.append("carbon_budget_exceeded")

        if metrics.get("latency_s", 0) > constraints.get("max_latency_seconds", float("inf")):
            violations.append("latency_budget_exceeded")

        if metrics.get("memory_mb", 0) / 1024 > constraints.get("memory_limit_gb", float("inf")):
            violations.append("memory_budget_exceeded")

        metrics["policy_violations"] = violations
        metrics["policy_pass"] = len(violations) == 0
        return metrics
