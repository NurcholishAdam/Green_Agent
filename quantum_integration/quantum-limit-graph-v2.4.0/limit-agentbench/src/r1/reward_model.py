"""
Defines reward function for energy-aware tuning.
"""


class GreenRewardModel:
    """
    Multi-objective reward balancing latency, energy, and carbon.
    """

    def compute(self, metrics: dict) -> float:
        energy = metrics.get("energy_kwh", 0)
        latency = metrics.get("latency", 0)
        carbon = metrics.get("carbon_kg", 0)

        # Lower is better
        reward = -(0.5 * energy + 0.3 * latency + 0.2 * carbon)
        return reward
