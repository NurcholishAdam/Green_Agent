class PolicyEngine:
    """
    Enforces sustainability constraints and meta-cognitive rules.
    """

    def __init__(self, config):
        self.constraints = config.get("constraints", {})
        self.meta = config.get("meta_policy", {})
        self.reflection_interval = self.meta.get("reflection_interval", 3)

    def enforce(self, metrics):
        if metrics.get("energy", 0) > self.constraints.get("max_energy_per_task_wh", float("inf")):
            raise RuntimeError("Energy budget exceeded")

        if metrics.get("carbon", 0) > self.constraints.get("max_carbon_per_task_kg", float("inf")):
            raise RuntimeError("Carbon budget exceeded")

        if metrics.get("latency", 0) > self.constraints.get("max_latency_seconds", float("inf")):
            raise RuntimeError("Latency exceeded")

    def should_reflect(self, step):
        return step % self.reflection_interval == 0
