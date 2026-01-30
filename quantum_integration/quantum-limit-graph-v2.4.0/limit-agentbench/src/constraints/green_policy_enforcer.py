class PolicyViolation(Exception):
    pass


class GreenPolicyEnforcer:
    def __init__(self, policy):
        self.constraints = policy["constraints"]
        self.mode = policy["agent_identity"]["mode"]

    def check(self, metrics):
        violations = []

        if metrics["energy"] > self.constraints["max_energy_per_task_wh"]:
            violations.append("energy")

        if metrics["carbon"] > self.constraints["max_carbon_per_task_kg"]:
            violations.append("carbon")

        if metrics["latency"] > self.constraints["max_latency_seconds"]:
            violations.append("latency")

        compliant = not violations

        if violations and self.mode == "active_enforcement":
            raise PolicyViolation(violations)

        return {"compliant": compliant, "violations": violations}
