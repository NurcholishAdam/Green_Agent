"""
policy_engine.py

Core Green Policy Engine with:
- Budget enforcement
- Pareto evaluation
- Reinforcement learning adaptation
"""

from typing import Dict
from rl.energy_rl_agent import EnergyRLAgent
from rl.reward_model import GreenRewardModel


class PolicyEngine:
    """
    Enforces sustainability policies and adapts weights via RL.
    """

    def __init__(self, config: Dict):
        self.config = config

        # Initial weights
        self.weights = {
            "energy": config.get("energy_weight", 0.5),
            "latency": config.get("latency_weight", 0.3),
            "carbon": config.get("carbon_weight", 0.2),
        }

        self.rl_agent = EnergyRLAgent()
        self.reward_model = GreenRewardModel(
            self.weights["energy"],
            self.weights["latency"],
            self.weights["carbon"],
        )

    # -------------------------
    # Budget Enforcement
    # -------------------------
    def enforce_budgets(self, metrics: Dict) -> bool:
        energy_budget = self.config.get("max_energy_kwh")
        latency_budget = self.config.get("max_latency")
        carbon_budget = self.config.get("max_carbon_kg")

        if energy_budget and metrics["energy_kwh"] > energy_budget:
            return False
        if latency_budget and metrics["latency"] > latency_budget:
            return False
        if carbon_budget and metrics["carbon_kg"] > carbon_budget:
            return False

        return True

    # -------------------------
    # RL Adaptation
    # -------------------------
    def adapt(self, metrics: Dict):
        state = self._discretize(metrics)

        action = self.rl_agent.choose_action(state)
        reward = self.reward_model.compute(metrics)

        next_state = state  # stateless episodic update

        self.rl_agent.update(state, action, reward, next_state)

        self._apply_action(action)

    def _apply_action(self, action: str):
        delta = 0.05

        if action == "increase_energy_weight":
            self.weights["energy"] += delta
        elif action == "increase_latency_weight":
            self.weights["latency"] += delta
        elif action == "increase_carbon_weight":
            self.weights["carbon"] += delta
        elif action == "balanced":
            pass

        # Normalize
        total = sum(self.weights.values())
        for k in self.weights:
            self.weights[k] /= total

        # Update reward model weights
        self.reward_model.energy_weight = self.weights["energy"]
        self.reward_model.latency_weight = self.weights["latency"]
        self.reward_model.carbon_weight = self.weights["carbon"]

    def _discretize(self, metrics: Dict) -> str:
        energy_bin = round(metrics.get("energy_kwh", 0), 2)
        latency_bin = round(metrics.get("latency", 0), 2)
        carbon_bin = round(metrics.get("carbon_kg", 0), 2)

        return f"E{energy_bin}_L{latency_bin}_C{carbon_bin}"
