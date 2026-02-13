"""
reward_model.py

Defines reward function for energy-aware reinforcement learning.
"""

from typing import Dict


class GreenRewardModel:
    """
    Computes multi-objective reward for RL policy tuning.

    Lower energy, latency, and carbon → higher reward.
    """

    def __init__(
        self,
        energy_weight: float = 0.5,
        latency_weight: float = 0.3,
        carbon_weight: float = 0.2,
    ):
        self.energy_weight = energy_weight
        self.latency_weight = latency_weight
        self.carbon_weight = carbon_weight

    def compute(self, metrics: Dict) -> float:
        energy = metrics.get("energy_kwh", 0.0)
        latency = metrics.get("latency", 0.0)
        carbon = metrics.get("carbon_kg", 0.0)

        cost = (
            self.energy_weight * energy
            + self.latency_weight * latency
            + self.carbon_weight * carbon
        )

        # Reward is negative cost (minimize objectives)
        return -cost
