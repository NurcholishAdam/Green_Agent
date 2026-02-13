"""
energy_rl_agent.py

Q-learning agent for adaptive energy-aware policy tuning.
"""

import random
from typing import Dict


class EnergyRLAgent:
    """
    Lightweight Q-learning agent.

    State: discretized metric profile
    Actions: adjust policy weights
    """

    def __init__(
        self,
        alpha: float = 0.1,
        gamma: float = 0.9,
        epsilon: float = 0.1,
    ):
        self.q_table: Dict[str, Dict[str, float]] = {}
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon

        self.actions = [
            "increase_energy_weight",
            "increase_latency_weight",
            "increase_carbon_weight",
            "balanced",
        ]

    def _ensure_state(self, state: str):
        if state not in self.q_table:
            self.q_table[state] = {a: 0.0 for a in self.actions}

    def choose_action(self, state: str) -> str:
        self._ensure_state(state)

        if random.random() < self.epsilon:
            return random.choice(self.actions)

        return max(self.q_table[state], key=self.q_table[state].get)

    def update(self, state: str, action: str, reward: float, next_state: str):
        self._ensure_state(state)
        self._ensure_state(next_state)

        old_value = self.q_table[state][action]
        next_max = max(self.q_table[next_state].values())

        new_value = old_value + self.alpha * (
            reward + self.gamma * next_max - old_value
        )

        self.q_table[state][action] = new_value
