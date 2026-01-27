# src/constraints/budget_enforcer.py

class BudgetExceeded(Exception):
    pass


class BudgetEnforcer:
    """
    Enforces execution budgets during benchmarking.
    """

    def __init__(
        self,
        max_energy=None,
        max_carbon=None,
        max_latency=None,
    ):
        self.max_energy = max_energy
        self.max_carbon = max_carbon
        self.max_latency = max_latency

    def check(self, metrics: dict) -> None:
        if self.max_energy is not None and metrics["energy"] > self.max_energy:
            raise BudgetExceeded("Energy budget exceeded")

        if self.max_carbon is not None and metrics["carbon"] > self.max_carbon:
            raise BudgetExceeded("Carbon budget exceeded")

        if self.max_latency is not None and metrics["latency"] > self.max_latency:
            raise BudgetExceeded("Latency budget exceeded")
