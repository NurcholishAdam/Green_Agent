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
from typing import Dict, List, Optional, Any, Callable
import numpy as np

logger = logging.getLogger(__name__)


class CausalRLPolicy:
    """
    A causal-aware policy based on structural equation modeling (SEM).
    For each action, a linear structural equation is maintained:
        reward = θ_action^T * features + noise
    This allows counterfactual reasoning: the effect of choosing action a
    on reward is captured by θ_a, under the assumption of no unobserved
    confounders (valid because the policy decides based on observed features).

    Features are arbitrary numeric values extracted from context.

    Uncertainty is quantified via Bayesian linear regression with a
    Gaussian prior and known noise precision. Action selection uses
    Upper Confidence Bound (UCB) to balance exploration and exploitation.

    The policy supports:
    - Explainability: feature contributions for a chosen action.
    - Temporal safety: add constraints that mask disallowed actions.
    - Human-in-the-loop: optional approval callback.
    - Persistence: save/load state.
    - Chaos testing: fault injection and simple resilience check.
    """

    def __init__(
        self,
        action_space: List[str],
        feature_names: Optional[List[str]] = None,
        alpha: float = 0.01,          # learning rate (used in recursive least squares)
        prior_precision: float = 1.0, # precision of the prior (lambda)
        noise_precision: float = 10.0,# assumed precision of observation noise
        ucb_beta: float = 1.0,        # exploration coefficient for UCB
    ):
        self.actions = action_space
        self.feature_names = feature_names  # if None, use sorted numeric keys from context
        self.alpha = alpha
        self.prior_precision = prior_precision
        self.noise_precision = noise_precision
        self.ucb_beta = ucb_beta

        # State for Bayesian linear regression
        self.weights: Dict[str, np.ndarray] = {a: np.zeros(self._get_feature_dim()) for a in action_space}
        self.covariances: Dict[str, np.ndarray] = {
            a: np.eye(self._get_feature_dim()) / prior_precision for a in action_space
        }
        self.usage: Dict[str, int] = defaultdict(int)

        # Safety constraints
        self.constraints: List[Callable[[Dict, str], bool]] = []  # each returns True if action disallowed

        # Human approval
        self.approval_callback: Optional[Callable[[str, Dict], bool]] = None

        self._lock = asyncio.Lock()
        self._fault_injected = False

    def _get_feature_dim(self) -> int:
        """Dimensionality of feature vector; if feature_names is given, use its length,
        otherwise we assume fixed at 3 (intercept, carbon, trust). Can be extended."""
        if self.feature_names is not None:
            return len(self.feature_names) + 1  # +1 for intercept
        return 3  # default fixed

    def _features(self, context: Dict) -> np.ndarray:
        """Extract feature vector from context.
        If feature_names is provided, use those keys; otherwise fallback to
        [intercept, avg_carbon/800, avg_trust]."""
        if self.feature_names is not None:
            vals = [1.0]
            for name in self.feature_names:
                val = context.get(name, 0.0)
                # Normalize if numeric
                if isinstance(val, (int, float)):
                    # Simple scaling for known keys (optional)
                    if name == 'avg_carbon':
                        val = val / 800.0
                    elif name == 'avg_trust':
                        val = min(1.0, max(0.0, val))
                    vals.append(float(val))
                else:
                    vals.append(0.0)
            return np.array(vals)
        # Default fallback
        return np.array([
            1.0,
            context.get('avg_carbon', 400) / 800.0,
            context.get('avg_trust', 0.5),
        ])

    async def get_probs(self, context: Dict) -> List[float]:
        """
        Return a probability distribution over actions using UCB scoring,
        with safety constraints applied and optional human approval.
        """
        async with self._lock:
            # Feature vector
            x = self._features(context)

            # Build mask of allowed actions
            allowed = np.ones(len(self.actions), dtype=bool)
            for i, action in enumerate(self.actions):
                for constraint in self.constraints:
                    if constraint(context, action):
                        allowed[i] = False
                        break

            # If no action allowed, return uniform over all actions with warning
            if not np.any(allowed):
                logger.warning("All actions disallowed by constraints; returning uniform distribution.")
                return [1.0 / len(self.actions)] * len(self.actions)

            # Compute UCB scores for allowed actions
            ucb_scores = np.full(len(self.actions), -np.inf)
            for i, action in enumerate(self.actions):
                if not allowed[i]:
                    continue
                w = self.weights[action]
                P = self.covariances[action]
                # Variance of prediction: x^T P x
                var = x @ P @ x  # scalar
                # UCB = mean + beta * sqrt(var)
                mean = w @ x
                ucb_scores[i] = mean + self.ucb_beta * np.sqrt(var + 1e-6)
                # Optional usage penalty to encourage exploration
                ucb_scores[i] -= self.usage[action] * 0.01

            # Softmax over allowed actions
            # For stability, subtract max
            max_score = np.max(ucb_scores[allowed])
            exp_scores = np.exp(ucb_scores - max_score)
            exp_scores[~allowed] = 0.0
            sum_exp = np.sum(exp_scores)
            if sum_exp <= 0:
                # Fallback to uniform over allowed
                probs = np.zeros(len(self.actions))
                probs[allowed] = 1.0 / np.sum(allowed)
                return probs.tolist()

            probs = exp_scores / sum_exp

            # Human approval for high-risk actions?
            # If callback is set, we may need to adjust; for now we just return.
            # A more sophisticated version could call approval for the top action if
            # it is considered critical, but that requires asynchronous handling.
            # We'll leave a hook for future improvement.

            return probs.tolist()

    async def update(self, context: Dict, action: str, reward: float):
        """
        Update the causal model for the chosen action using recursive least squares
        with forgetting factor alpha (treated as learning rate).
        """
        async with self._lock:
            if action not in self.weights:
                # Initialize if new action
                self.weights[action] = np.zeros(self._get_feature_dim())
                self.covariances[action] = np.eye(self._get_feature_dim()) / self.prior_precision
                self.usage[action] = 0

            x = self._features(context)
            w = self.weights[action]
            P = self.covariances[action]

            # Recursive least squares update with forgetting factor
            # Standard RLS: P = (1/λ) * (P - (P x x^T P)/(λ + x^T P x))
            # Here λ = alpha (learning rate). To avoid division issues, we clamp.
            lam = max(self.alpha, 0.01)
            denom = lam + x @ P @ x
            P_new = (1 / lam) * (P - (P @ np.outer(x, x) @ P) / denom)
            # Weight update: w = w + P_new x (reward - x^T w)
            error = reward - x @ w
            w_new = w + P_new @ x * error

            self.weights[action] = w_new
            self.covariances[action] = P_new
            self.usage[action] += 1

    async def explain(self, context: Dict, action: str) -> Dict[str, Any]:
        """
        Provide an explanation for the choice of a particular action.
        Returns feature contributions and a human-readable summary.
        """
        async with self._lock:
            if action not in self.weights:
                return {'error': 'Action not found'}
            x = self._features(context)
            w = self.weights[action]
            # Feature names for explanation
            if self.feature_names is not None:
                names = ['intercept'] + self.feature_names
            else:
                names = ['intercept', 'avg_carbon_normalized', 'avg_trust']
            contributions = {names[i]: float(w[i] * x[i]) for i in range(len(x))}
            total_score = float(w @ x)
            explanation = (
                f"Action '{action}' has expected score {total_score:.3f}. "
                f"Contributions: " + ", ".join(
                    f"{name}: {contrib:.3f}" for name, contrib in contributions.items()
                )
            )
            return {
                'action': action,
                'score': total_score,
                'contributions': contributions,
                'explanation': explanation
            }

    def add_constraint(self, constraint_fn: Callable[[Dict, str], bool], description: str = ""):
        """
        Add a temporal safety constraint. The function receives (context, action)
        and must return True if the action is disallowed.
        """
        self.constraints.append(constraint_fn)
        logger.info(f"Added constraint: {description}")

    def set_approval_callback(self, callback: Callable[[str, Dict], bool]):
        """
        Set a callback for human approval. The callback receives (action, context)
        and returns True if approved, False otherwise.
        """
        self.approval_callback = callback

    async def request_approval(self, action: str, context: Dict) -> bool:
        """Request approval for an action. Returns True if approved or no callback set."""
        if self.approval_callback is None:
            return True
        # In a real system, this would be asynchronous and await human response.
        # Here we call the callback directly (could be a coroutine).
        result = self.approval_callback(action, context)
        if asyncio.iscoroutine(result):
            return await result
        return bool(result)

    async def save_state(self) -> Dict[str, Any]:
        """Serialize the policy state."""
        async with self._lock:
            return {
                'weights': {a: w.tolist() for a, w in self.weights.items()},
                'covariances': {a: P.tolist() for a, P in self.covariances.items()},
                'usage': dict(self.usage),
                'feature_names': self.feature_names,
                'actions': self.actions,
            }

    async def load_state(self, state: Dict[str, Any]):
        """Restore policy state from a dictionary."""
        async with self._lock:
            self.actions = state.get('actions', self.actions)
            self.feature_names = state.get('feature_names', self.feature_names)
            self.weights = {a: np.array(w) for a, w in state.get('weights', {}).items()}
            self.covariances = {a: np.array(P) for a, P in state.get('covariances', {}).items()}
            self.usage = defaultdict(int, state.get('usage', {}))

    async def reset(self):
        """Reset all learned parameters."""
        async with self._lock:
            dim = self._get_feature_dim()
            self.weights = {a: np.zeros(dim) for a in self.actions}
            self.covariances = {a: np.eye(dim) / self.prior_precision for a in self.actions}
            self.usage.clear()
            self.constraints.clear()

    # Chaos testing
    def inject_fault(self, fault_type: str):
        """
        Inject a fault to test resilience.
        Supported faults:
          - 'reset_weights': set all weights to zero.
          - 'corrupt_covariance': set covariance matrices to identity (high uncertainty).
          - 'clear_usage': reset usage counts.
        """
        if fault_type == 'reset_weights':
            for a in self.weights:
                self.weights[a] = np.zeros_like(self.weights[a])
            logger.warning("Fault injected: reset weights")
        elif fault_type == 'corrupt_covariance':
            for a in self.covariances:
                self.covariances[a] = np.eye(self._get_feature_dim())
            logger.warning("Fault injected: corrupt covariance")
        elif fault_type == 'clear_usage':
            self.usage.clear()
            logger.warning("Fault injected: clear usage")
        else:
            logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        """
        Simple chaos test: inject a fault, then check if get_probs still returns a valid distribution.
        """
        report = {'faults': [], 'results': {}}
        # Test reset weights
        self.inject_fault('reset_weights')
        report['faults'].append('reset_weights')
        probs = await self.get_probs({'avg_carbon': 400, 'avg_trust': 0.5})
        report['results']['reset_weights'] = {
            'valid_distribution': abs(sum(probs) - 1.0) < 1e-6,
            'probs': probs
        }
        # Test corrupt covariance
        self.inject_fault('corrupt_covariance')
        report['faults'].append('corrupt_covariance')
        probs = await self.get_probs({'avg_carbon': 400, 'avg_trust': 0.5})
        report['results']['corrupt_covariance'] = {
            'valid_distribution': abs(sum(probs) - 1.0) < 1e-6,
            'probs': probs
        }
        # Restore to a sane state (reinitialize)
        await self.reset()
        return report
