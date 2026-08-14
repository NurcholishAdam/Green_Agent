"""
contextual_bandit.py

Implements a Contextual Bandit with Thompson Sampling.
It learns which policy configuration works best for each workload context.
The confidence gate ensures we fall back to the LP solver when unsure.
"""
import numpy as np
from typing import Dict, Any, List, Tuple, Optional


class ContextualBandit:
    """
    Bandit that selects among a discrete set of policy actions.
    Uses a linear reward model with uncertainty (simple normal priors).
    """
    def __init__(self, action_space: List[Dict[str, Any]], fallback_solver):
        """
        Args:
            action_space: List of policy dicts (candidate configurations).
            fallback_solver: Callable that takes a WorkloadFingerprint and
                             returns a policy dict (the Phase 2 LP solver).
        """
        self.actions = action_space
        self.fallback_solver = fallback_solver
        self.num_actions = len(action_space)

        # Store: context_key -> (weights array of size num_actions, trials count)
        self.weights = {}    # context key -> np.ndarray of mean rewards
        self.trials = {}     # context key -> int
        self.reward_history = {}  # for debugging

    def _encode_context(self, fp) -> tuple:
        # Use the fingerprint vector as a key for simplicity
        return tuple(fp.to_vector().tolist())

    def select_action(self, fp, min_trials_before_bandit: int = 5,
                      confidence_threshold: float = 0.6) -> Tuple[Optional[Dict], float]:
        """
        Returns (policy, confidence) where confidence is 1.0 if fallback used.
        If bandit is unsure, returns (None, 0.0) to trigger fallback.
        """
        ctx_key = self._encode_context(fp)
        n_trials = self.trials.get(ctx_key, 0)

        # SAFETY GATE: Not enough trials -> force fallback
        if n_trials < min_trials_before_bandit:
            return None, 0.0

        weights = self.weights[ctx_key]
        # Thompson Sampling: sample from Normal(mean, std) for each action
        # std = 1 / sqrt(n_trials + 1) (simple approximation)
        std = 1.0 / np.sqrt(n_trials + 1)
        sampled = np.random.normal(weights, std)
        best_idx = np.argmax(sampled)

        confidence = 1.0 - (1.0 / (n_trials + 1))  # confidence increases with trials
        if confidence < confidence_threshold:
            return None, 0.0

        return self.actions[best_idx], confidence

    def update(self, fp, action: Dict[str, Any], reward: float):
        ctx_key = self._encode_context(fp)
        if ctx_key not in self.weights:
            self.weights[ctx_key] = np.zeros(self.num_actions)
            self.trials[ctx_key] = 0

        # Find index of the chosen action
        try:
            action_idx = self.actions.index(action)
        except ValueError:
            # If action not in action space (e.g., LP solver returned custom),
            # we cannot update. We'll ignore update.
            return

        # Online gradient update with decreasing learning rate
        n = self.trials[ctx_key]
        lr = 0.1 / (n + 1)
        old_weight = self.weights[ctx_key][action_idx]
        self.weights[ctx_key][action_idx] = old_weight + lr * (reward - old_weight)
        self.trials[ctx_key] += 1

    def seed_safe_policy(self, fp, policy: Dict[str, Any]):
        """
        Used to seed the bandit with a safe LP‑solver result.
        We record a dummy reward so the bandit will start with a good prior.
        """
        ctx_key = self._encode_context(fp)
        if ctx_key not in self.weights:
            self.weights[ctx_key] = np.zeros(self.num_actions)
            self.trials[ctx_key] = 0

        # Find the closest action in action_space (by policy content).
        # For simplicity, we just assign a high reward to the first action
        # that matches the policy's key parameters (you may improve this).
        # Here we'll assume the policy dict has a 'id' field or we compare
        # relevant fields. We'll use a simple heuristic: if policy equals
        # any of the actions exactly, we set its weight to 1.0.
        for i, act in enumerate(self.actions):
            if act == policy:
                self.weights[ctx_key][i] = 1.0
                break
        # else no match, do nothing
