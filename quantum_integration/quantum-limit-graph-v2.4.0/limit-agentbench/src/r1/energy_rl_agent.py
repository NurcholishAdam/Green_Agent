"""
Simple Q-learning agent for policy tuning.
"""

import random
import numpy as np


class EnergyRLAgent:
    """
    Learns optimal policy weight configuration.
    """

    def __init__(self):
        self.q_table = {}
        self.actions = [
            "increase_energy_weight",
            "increase_latency_weight",
            "balanced"
        ]
        self.alpha = 0.1
        self.gamma = 0.9
        self.epsilon = 0.1

    def choose_action(self, state):
        if random.random() < self.epsilon:
            return random.choice(self.actions)

        return max(self.q_table.get(state, {}), key=self.q_table.get(state, {}).get, default="balanced")

    def update(self, state, action, reward, next_state):
        old_value = self.q_table.get(state, {}).get(action, 0)
        next_max = max(self.q_table.get(next_state, {}).values(), default=0)

        new_value = old_value + self.alpha * (
            reward + self.gamma * next_max - old_value
        )

        self.q_table.setdefault(state, {})[action] = new_value
