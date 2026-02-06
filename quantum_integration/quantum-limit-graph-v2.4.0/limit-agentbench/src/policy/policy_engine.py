"""
Policy Engine

Hard + soft constraints, including meta-cognitive reflection rules.
"""

class PolicyEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.constraints = cfg.get("constraints", {})
        self.meta = cfg.get("meta_policy", {})

        self.reflection_interval = self.meta.get("reflection_interval", 3)

    # -------------------------
    # Hard enforcement
    # -------------------------
    def enforce(self, metrics: dict):
        if metrics.get("energy", 0) > self.constraints.get("max_energy_per_task_wh", float("inf")):
            raise RuntimeError("Policy violation: energy budget exceeded")

        if metrics.get("carbon", 0) > self.constraints.get("max_carbon_per_task_kg", float("inf")):
            raise RuntimeError("Policy violation: carbon budget exceeded")

        if metrics.get("latency", 0) > self.constraints.get("max_latency_seconds", float("inf")):
            raise RuntimeError("Policy violation: latency exceeded")

    # -------------------------
    # Meta-cognitive rules
    # -------------------------
    def should_reflect(self, step: int) -> bool:
        return step % self.reflection_interval == 0
