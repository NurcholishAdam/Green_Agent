# src/enhancements/causal_reward_shaper.py
"""
Causal Reward Shaper.

Modifies the scalar reward from the environment using the CausalGraph.
The shaped reward is used to update the distillation student, encouraging
the agent to learn actions that positively influence downstream variables.

Usage:
    shaper = CausalRewardShaper(causal_graph, influence_weight=0.3)
    shaped_reward = shaper.shape_reward(action, reward, state_before, state_after)
"""

import numpy as np
from typing import Any, Dict, Optional

class CausalRewardShaper:
    def __init__(self, causal_graph, influence_weight: float = 0.3):
        """
        Args:
            causal_graph: Instance of CausalGraph (from core.causal_graph).
            influence_weight: Weight given to causal contribution in shaped reward.
        """
        self.graph = causal_graph
        self.influence_weight = influence_weight

    def shape_reward(self, action: int, reward: float,
                     state_before: Dict[str, float],
                     state_after: Dict[str, float]) -> float:
        """
        Compute shaped reward: reward = original_reward + w * causal_bonus.

        causal_bonus is derived from how much the action improved or worsened
        key causal variables (e.g., carbon_intensity, grid_strain).
        """
        # Map state dict to graph observations
        self.graph.observe_batch(state_after)

        # Compute anomaly score before/after
        anomalies_before = set(self.graph.get_anomalies())
        # Temporarily set state to before
        self.graph.observe_batch(state_before)
        anomalies_after = set(self.graph.get_anomalies())

        # Causal bonus: positive if anomalies decreased
        bonus = 0.0
        if len(anomalies_after) < len(anomalies_before):
            bonus = 1.0
        elif len(anomalies_after) == len(anomalies_before) and len(anomalies_after) == 0:
            bonus = 0.5  # maintain good state
        else:
            bonus = -0.5

        shaped_reward = reward + self.influence_weight * bonus
        return float(np.clip(shaped_reward, -1.0, 1.0))
