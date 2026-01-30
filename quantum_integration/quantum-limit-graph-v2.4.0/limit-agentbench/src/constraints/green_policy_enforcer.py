from typing import Dict, List


class PolicyViolation(Exception):
    pass


class GreenPolicyEnforcer:
    def __init__(self, policy):
        self.policy = policy
        self.mode = policy.identity.get("mode", "passive_log")

    def preflight_check(self):
        # Reserved for future model/memory gating
        return True

    def check_runtime(self, metrics: Dict) -> Dict:
        violations: List[str] = []
        c = self.policy.constraints

        if metrics["energy"] > c.get("max_energy_per_task_wh", float("inf")):
            violations.append("energy_limit_exceeded")

        if metrics["carbon"] > c.get("max_carbon_per_task_kg", float("inf")):
            violations.append("carbon_limit_exceeded")

        if metrics["latency"] > c.get("max_latency_seconds", float("inf")):
            violations.append("latency_limit_exceeded")

        if metrics["memory"] / 1024 > c.get("memory_limit_gb", float("inf")):
            violations.append("memory_limit_exceeded")

        compliant = len(violations) == 0

        if not compliant and self.mode == "active_enforcement":
            raise PolicyViolation(", ".join(violations))

        return {
            "compliant": compliant,
            "violations": violations
        }
