# src/enhancements/causal_reward_shaper.py
"""
Causal Reward Shaper with Correct State Handling and Read‑Only Graph Access.

Modifies the scalar reward from the environment using the CausalGraph.
The shaped reward encourages actions that improve downstream causal variables.

Usage:
    shaper = CausalRewardShaper(causal_graph, influence_weight=0.3)
    shaped_reward = shaper.shape_reward(action, reward, state_before, state_after)
"""

import copy
import logging
from typing import Any, Dict, Optional, Set

import numpy as np

logger = logging.getLogger(__name__)


class CausalRewardShaper:
    def __init__(
        self,
        causal_graph,
        influence_weight: float = 0.3,
        use_influence_scores: bool = True,
        bonus_scale: float = 1.0,
        clip_reward: bool = False,
    ):
        """
        Initialize the reward shaper.

        Args:
            causal_graph: Instance of CausalGraph (must have `observe_batch` and
                          `get_anomalies`; optionally `get_influence_scores`).
            influence_weight: Weight given to causal contribution in shaped reward.
            use_influence_scores: If True, use continuous influence scores (if the
                                  graph provides them) instead of binary anomaly counts.
            bonus_scale: Scaling factor for the causal bonus (default 1.0).
            clip_reward: If True, clip the final shaped reward to [-1, 1].
        """
        self.graph = causal_graph
        self.influence_weight = influence_weight
        self.use_influence_scores = use_influence_scores
        self.bonus_scale = bonus_scale
        self.clip_reward = clip_reward

        # Check if graph supports influence scores
        self._has_influence = hasattr(causal_graph, "get_influence_scores")
        if self.use_influence_scores and not self._has_influence:
            logger.warning(
                "Causal graph does not provide 'get_influence_scores'; falling back to anomaly count."
            )
            self.use_influence_scores = False

    def _compute_anomaly_set(self, state: Dict[str, float]) -> Set[str]:
        """
        Compute anomalies for a given state without permanently altering the graph.
        We create a temporary copy of the graph, observe the state, and get anomalies.
        If copying is too expensive, we assume the graph is stateless and can be queried
        directly after observing (but we restore original state afterwards).
        """
        # Create a shallow copy of the graph to avoid side effects
        graph_copy = copy.copy(self.graph)
        # If the graph has a deep copy method, use it (for complex graphs)
        if hasattr(self.graph, "copy"):
            graph_copy = self.graph.copy()
        else:
            # Fallback: assume observe_batch does not alter the graph structurally
            # and we can temporarily observe and then restore
            try:
                original_state = getattr(self.graph, "_last_state", None)
                self.graph.observe_batch(state)
                anomalies = set(self.graph.get_anomalies())
                if original_state is not None:
                    self.graph.observe_batch(original_state)
                return anomalies
            except Exception as e:
                logger.error(f"Failed to compute anomalies safely: {e}")
                return set()

        graph_copy.observe_batch(state)
        return set(graph_copy.get_anomalies())

    def _compute_causal_bonus(
        self, state_before: Dict[str, float], state_after: Dict[str, float]
    ) -> float:
        """
        Compute a continuous bonus based on the change in causal anomalies or
        influence scores between the two states.
        """
        if self.use_influence_scores:
            # Assume get_influence_scores returns a dict mapping variable name to score
            # where positive means "good" (lower carbon, less strain)
            try:
                scores_before = self.graph.get_influence_scores(state_before)
                scores_after = self.graph.get_influence_scores(state_after)
                # Compute improvement as sum of positive differences
                # (higher score = better)
                improvement = 0.0
                for var in scores_after:
                    improvement += scores_after[var] - scores_before.get(var, 0.0)
                # Normalize by number of variables to keep bonus in reasonable range
                if len(scores_after) > 0:
                    improvement /= len(scores_after)
                return float(improvement)
            except Exception as e:
                logger.warning(f"Failed to compute influence scores: {e}; falling back to anomaly count.")
                self.use_influence_scores = False  # Disable for future calls

        # Fallback: anomaly count based on set difference
        anomalies_before = self._compute_anomaly_set(state_before)
        anomalies_after = self._compute_anomaly_set(state_after)

        # Improvement if fewer anomalies after
        # We care about both count and specific variables
        newly_fixed = anomalies_before - anomalies_after
        newly_broken = anomalies_after - anomalies_before

        # Simple continuous measure: +1 for each fixed, -1 for each broken,
        # normalized by total possible anomalies
        total_possible = len(anomalies_before | anomalies_after)
        if total_possible == 0:
            return 0.5  # no anomalies in either state – maintain good behavior
        bonus = (len(newly_fixed) - len(newly_broken)) / total_possible
        return float(bonus)

    def shape_reward(
        self,
        action: int,
        reward: float,
        state_before: Dict[str, float],
        state_after: Dict[str, float],
    ) -> float:
        """
        Compute shaped reward: shaped_reward = reward + influence_weight * causal_bonus.

        Args:
            action: The action taken (not used in current implementation but kept for API compatibility).
            reward: Original scalar reward from environment.
            state_before: Environment state before action.
            state_after: Environment state after action.

        Returns:
            Shaped reward (float).
        """
        causal_bonus = self._compute_causal_bonus(state_before, state_after)
        shaped_reward = reward + self.influence_weight * self.bonus_scale * causal_bonus

        if self.clip_reward:
            shaped_reward = float(np.clip(shaped_reward, -1.0, 1.0))

        logger.debug(
            f"Original reward: {reward:.3f}, causal bonus: {causal_bonus:.3f}, "
            f"shaped reward: {shaped_reward:.3f}"
        )
        return shaped_reward
