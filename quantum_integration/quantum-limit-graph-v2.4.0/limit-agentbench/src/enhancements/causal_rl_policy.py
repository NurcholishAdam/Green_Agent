#!/usr/bin/env python3
"""
Causal Reinforcement Learning for Policy Adaptation.

Provides a lightweight causal policy updater that uses simple structural
causal models (SCM) to estimate treatment effects of choosing a particular
strategy on sustainability metrics. Integrates with existing bandit/RLHF.

Usage:
    causal_policy = CausalRLPolicy(action_space=['fedavg','carbon_aware'])
    probs = await causal_policy.get_probs(context)
    await causal_policy.update(context, action, reward)
"""

import asyncio
import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Any
import numpy as np

logger = logging.getLogger(__name__)


class CausalRLPolicy:
    """
    Maintains a linear structural equation for each action's effect on
    outcome (reward). Uses simple counterfactual reasoning (potential outcomes)
    to compute action probabilities.
    """

    def __init__(self, action_space: List[str], alpha: float = 0.01):
        self.actions = action_space
        self.alpha = alpha
        self.weights: Dict[str, np.ndarray] = {a: np.zeros(3) for a in action_space}  # [intercept, carbon_effect, trust_effect]
        self.usage: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    def _features(self, context: Dict) -> np.ndarray:
        return np.array([
            1.0,
            context.get('avg_carbon', 400) / 800.0,
            context.get('avg_trust', 0.5),
        ])

    async def get_probs(self, context: Dict) -> List[float]:
        async with self._lock:
            scores = []
            for a in self.actions:
                w = self.weights[a]
                score = np.dot(w, self._features(context))
                scores.append(score)
            # Softmax with temperature, offset by usage penalty (exploration)
            scores = np.array(scores)
            usage_penalty = np.array([self.usage[a] for a in self.actions]) * 0.01
            scores -= usage_penalty
            exp_scores = np.exp(scores - np.max(scores))
            probs = exp_scores / np.sum(exp_scores)
            return probs.tolist()

    async def update(self, context: Dict, action: str, reward: float):
        async with self._lock:
            if action not in self.weights:
                self.weights[action] = np.zeros(3)
            feats = self._features(context)
            self.weights[action] += self.alpha * (reward - np.dot(self.weights[action], feats)) * feats
            self.usage[action] += 1

    async def reset(self):
        async with self._lock:
            self.weights = {a: np.zeros(3) for a in self.actions}
            self.usage.clear()
