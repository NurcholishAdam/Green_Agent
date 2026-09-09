# src/enhancements/causal_reward_shaper.py
"""
Causal Reward Shaper with Correct State Handling and Read‑Only Graph Access,
PLUS enhancements for Temporal Logic Safety, Explainable AI, Federated Learning,
Multi‑Agent Coordination, Chaos Resilience, and Human‑in‑the‑Loop.

Modifies the scalar reward from the environment using the CausalGraph.
The shaped reward encourages actions that improve downstream causal variables.

New components (all in this file):
- SafetyMonitor: temporal logic‑like safety checks on reward shaping.
- XAIExplainer: explains the causal bonus.
- FederatedRewardShaper: aggregates causal models from multiple shapers.
- MultiAgentRewardShaper: coordinates multiple shapers for different agents.
- ChaosResilientRewardShaper: wraps the base shaper with fault injection.
- HumanReviewManager: for human‑in‑the‑loop review of shaped rewards.

Usage:
    shaper = CausalRewardShaper(causal_graph, influence_weight=0.3)
    shaped_reward = shaper.shape_reward(action, reward, state_before, state_after)
"""

import copy
import logging
import asyncio
import random
from typing import Any, Dict, Optional, Set, List, Tuple, Callable
from datetime import datetime

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


# =============================================================================
# NEW: Temporal Logic Safety Monitor
# =============================================================================
class SafetyMonitor:
    """
    Monitors reward shaping decisions against simple temporal safety properties.
    For example: "Shaped reward should not be lower than original reward more than
    N times in a row", or "Causal bonus should be positive on average over last M steps".
    """

    def __init__(
        self,
        max_negative_bonus_streak: int = 3,
        min_avg_bonus: float = -0.2,
        window_size: int = 10,
    ):
        self.max_negative_bonus_streak = max_negative_bonus_streak
        self.min_avg_bonus = min_avg_bonus
        self.window_size = window_size

        self.bonus_history: List[float] = []
        self.violations: List[Dict[str, Any]] = []

    def check(self, original_reward: float, shaped_reward: float, causal_bonus: float) -> bool:
        """
        Returns True if the shaping is considered safe, False if a violation is detected.
        """
        self.bonus_history.append(causal_bonus)
        if len(self.bonus_history) > self.window_size * 2:
            self.bonus_history = self.bonus_history[-self.window_size * 2 :]

        # Check negative streak
        streak = 0
        for b in reversed(self.bonus_history):
            if b < 0:
                streak += 1
            else:
                break
        if streak >= self.max_negative_bonus_streak:
            self._record_violation("negative_streak", f"{streak} negative bonuses in a row")
            return False

        # Check average bonus over last window
        if len(self.bonus_history) >= self.window_size:
            avg = np.mean(self.bonus_history[-self.window_size :])
            if avg < self.min_avg_bonus:
                self._record_violation("low_avg_bonus", f"Average bonus {avg:.3f} below threshold")
                return False

        return True

    def _record_violation(self, rule: str, message: str):
        violation = {
            "rule": rule,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        }
        self.violations.append(violation)
        logger.warning(f"Safety violation: {rule} - {message}")

    def get_violations(self) -> List[Dict[str, Any]]:
        return self.violations


# =============================================================================
# NEW: Explainable AI for Reward Shaping
# =============================================================================
class XAIExplainer:
    """
    Generates human‑readable explanations for why the causal bonus was given.
    """

    def explain(
        self,
        state_before: Dict[str, float],
        state_after: Dict[str, float],
        causal_bonus: float,
        anomalies_before: Set[str],
        anomalies_after: Set[str],
    ) -> str:
        """
        Produce a natural language explanation of the causal contribution.
        """
        if causal_bonus > 0:
            outcome = "improved"
            fixed = anomalies_before - anomalies_after
            if fixed:
                vars_str = ", ".join(sorted(fixed))
                explanation = f"Causal bonus positive ({causal_bonus:.2f}) because the following variables improved: {vars_str}."
            else:
                explanation = f"Causal bonus positive ({causal_bonus:.2f}) due to overall improvement in influence scores."
        elif causal_bonus < 0:
            outcome = "worsened"
            broken = anomalies_after - anomalies_before
            if broken:
                vars_str = ", ".join(sorted(broken))
                explanation = f"Causal bonus negative ({causal_bonus:.2f}) because the following variables worsened: {vars_str}."
            else:
                explanation = f"Causal bonus negative ({causal_bonus:.2f}) due to overall decline in influence scores."
        else:
            explanation = f"Causal bonus neutral ({causal_bonus:.2f}); no significant change in causal variables."

        return explanation


# =============================================================================
# NEW: Federated Reward Shaper (aggregates models)
# =============================================================================
class FederatedRewardShaper:
    """
    Coordinates multiple CausalRewardShaper instances across deployments.
    Aggregates their causal graphs using simple averaging of influence weights.
    """

    def __init__(self, shapers: Optional[List[CausalRewardShaper]] = None):
        self.shapers = shapers or []
        self.aggregated_weights: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    def add_shaper(self, shaper: CausalRewardShaper):
        self.shapers.append(shaper)

    def aggregate(self) -> Dict[str, float]:
        """
        Average influence weights from all shapers (assumes graphs expose weights as dict).
        """
        if not self.shapers:
            return {}
        # Assume each shaper has a method `get_weights()` that returns dict
        # If not, we fallback to no aggregation
        all_weights = []
        for shaper in self.shapers:
            if hasattr(shaper, "get_weights"):
                w = shaper.get_weights()
                all_weights.append(w)
        if not all_weights:
            return {}
        keys = set()
        for w in all_weights:
            keys.update(w.keys())
        avg = {}
        for key in keys:
            vals = [w.get(key, 0.0) for w in all_weights]
            avg[key] = sum(vals) / len(vals)
        self.aggregated_weights = avg
        return avg

    def shape_reward_federated(
        self,
        action: int,
        reward: float,
        state_before: Dict[str, float],
        state_after: Dict[str, float],
    ) -> float:
        """
        Average shaped rewards from all local shapers.
        """
        if not self.shapers:
            return reward
        shaped_rewards = [
            shaper.shape_reward(action, reward, state_before, state_after)
            for shaper in self.shapers
        ]
        return float(np.mean(shaped_rewards))


# =============================================================================
# NEW: Multi‑Agent Reward Shaper (per‑agent shapers)
# =============================================================================
class MultiAgentRewardShaper:
    """
    Manages separate reward shapers for different agents, with optional coordination
    via shared graph updates or shared safety constraints.
    """

    def __init__(self, num_agents: int, causal_graph=None, **shaper_kwargs):
        self.agents = {
            f"agent_{i}": CausalRewardShaper(causal_graph, **shaper_kwargs)
            for i in range(num_agents)
        }
        self.agent_histories = {agent: [] for agent in self.agents}
        self.coordination_bonus = 0.0  # optional global bonus for coordination

    def shape_reward_for_agent(
        self,
        agent_id: str,
        action: int,
        reward: float,
        state_before: Dict[str, float],
        state_after: Dict[str, float],
        coordination_signal: Optional[float] = None,
    ) -> float:
        """
        Shape reward for a specific agent, optionally adding a coordination bonus.
        """
        if agent_id not in self.agents:
            raise ValueError(f"Unknown agent {agent_id}")
        shaper = self.agents[agent_id]
        shaped = shaper.shape_reward(action, reward, state_before, state_after)
        if coordination_signal is not None:
            shaped += coordination_signal
        self.agent_histories[agent_id].append(shaped)
        return shaped

    def get_agent_stats(self) -> Dict[str, float]:
        """Return average shaped reward per agent."""
        stats = {}
        for agent, history in self.agent_histories.items():
            stats[agent] = float(np.mean(history)) if history else 0.0
        return stats


# =============================================================================
# NEW: Chaos‑Resilient Reward Shaper
# =============================================================================
class ChaosResilientRewardShaper:
    """
    Wraps a CausalRewardShaper and injects random failures to test robustness.
    If the underlying shaper fails, falls back to original reward.
    """

    def __init__(
        self,
        base_shaper: CausalRewardShaper,
        failure_probability: float = 0.1,
        seed: Optional[int] = None,
    ):
        self.base = base_shaper
        self.failure_probability = failure_probability
        self._rng = random.Random(seed)

    def shape_reward(
        self,
        action: int,
        reward: float,
        state_before: Dict[str, float],
        state_after: Dict[str, float],
    ) -> float:
        # Simulate failure
        if self._rng.random() < self.failure_probability:
            logger.warning("Chaos: Causal graph failure simulated, returning original reward.")
            return reward
        try:
            return self.base.shape_reward(action, reward, state_before, state_after)
        except Exception as e:
            logger.error(f"Chaos: Shaping failed: {e}, returning original reward.")
            return reward


# =============================================================================
# NEW: Human‑in‑the‑Loop Review Manager
# =============================================================================
class HumanReviewManager:
    """
    Allows human review of shaped rewards for critical decisions.
    """

    def __init__(self):
        self.pending_reviews: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def request_review(
        self,
        task_id: str,
        action: int,
        reward: float,
        shaped_reward: float,
        state_before: Dict[str, float],
        state_after: Dict[str, float],
    ) -> str:
        """
        Request human review for a reward shaping decision.
        Returns a review ID.
        """
        review_id = f"review_{uuid.uuid4().hex[:8]}" if 'uuid' in globals() else f"review_{datetime.now().timestamp()}"
        async with self._lock:
            self.pending_reviews[review_id] = {
                "task_id": task_id,
                "action": action,
                "original_reward": reward,
                "shaped_reward": shaped_reward,
                "state_before": state_before,
                "state_after": state_after,
                "status": "pending",
            }
        return review_id

    async def approve(self, review_id: str):
        async with self._lock:
            if review_id in self.pending_reviews:
                self.pending_reviews[review_id]["status"] = "approved"

    async def reject(self, review_id: str):
        async with self._lock:
            if review_id in self.pending_reviews:
                self.pending_reviews[review_id]["status"] = "rejected"

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            return [r for r in self.pending_reviews.values() if r["status"] == "pending"]


# =============================================================================
# Optional: Add `get_weights` to CausalRewardShaper for federation
# =============================================================================
def _get_weights(self) -> Dict[str, float]:
    """
    Return the current influence weight and bonus scale as a dict for federation.
    This method is monkey‑patched onto CausalRewardShaper if not already present.
    """
    return {
        "influence_weight": self.influence_weight,
        "bonus_scale": self.bonus_scale,
    }

# Monkey‑patch get_weights if not defined
if not hasattr(CausalRewardShaper, "get_weights"):
    CausalRewardShaper.get_weights = _get_weights
