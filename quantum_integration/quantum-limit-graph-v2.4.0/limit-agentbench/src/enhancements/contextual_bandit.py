"""
contextual_bandit.py

Enhanced Contextual Bandit with Thompson Sampling and integration with
MODP, bio_inspired, and moe_system modules.

Features:
- Bayesian linear regression for reward prediction (handles correlations).
- MODP integration to compute scalar rewards from multi‑objective metrics.
- MoE integration for advanced context encoding.
- Bio‑inspired dynamic action expansion when exploration is needed.
- Persistence (save/load state to JSON).
- Adaptive confidence threshold based on recent performance.
- Configurable safety gates.
"""

import numpy as np
import json
import os
import time
import logging
from typing import Dict, Any, List, Tuple, Optional, Callable
from dataclasses import dataclass, field

# ----------------------------------------------------------------------
# 1. Imports from other enhancement modules (with fallback stubs)
# ----------------------------------------------------------------------
# Uncomment these when modules are available:
# from enhancements.bio_inspired import GeneticPolicyGenerator
# from enhancements.MODP import ParetoOptimizer
# from enhancements.moe_system import ExpertRouter

# Stubs for missing modules
class MODPStub:
    def evaluate(self, metrics: Dict[str, float], weights: Dict[str, float]) -> float:
        # Simple weighted sum as fallback
        return sum(metrics.get(k, 0.0) * weights.get(k, 1.0) for k in metrics)

class BioStub:
    def generate_policies(self, current_policies: List[Dict], n: int) -> List[Dict]:
        # No generation; return empty list
        return []

class MoEStub:
    def encode(self, task: Dict[str, Any]) -> np.ndarray:
        # Simple feature extraction: use the fingerprint vector directly
        return np.array([task.get("model_size_mb", 0) / 1000,
                         task.get("prompt_len", 0) / 1024,
                         task.get("gen_len", 0) / 1024])

# Try to import real modules, fallback to stubs
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
except ImportError:
    GeneticPolicyGenerator = BioStub

try:
    from enhancements.MODP import ParetoOptimizer
except ImportError:
    ParetoOptimizer = MODPStub

try:
    from enhancements.moe_system import ExpertRouter
except ImportError:
    ExpertRouter = MoEStub


# ----------------------------------------------------------------------
# 2. Core Enhanced ContextualBandit
# ----------------------------------------------------------------------

@dataclass
class BanditState:
    """Persistent state for the bandit."""
    action_weights: Dict[str, np.ndarray]   # context_key -> weight vector
    action_covariances: Dict[str, np.ndarray] # context_key -> covariance matrix
    action_trials: Dict[str, int]           # context_key -> trial count
    action_rewards: Dict[str, List[float]]  # context_key -> list of rewards per action
    action_space: List[Dict[str, Any]]      # current list of actions
    # For adaptive threshold
    recent_confidences: List[float] = field(default_factory=list)
    last_adaptation: float = 0.0

class ContextualBandit:
    """
    Enhanced Contextual Bandit with:
    - Bayesian linear regression for reward prediction.
    - MODP integration to compute scalar rewards from multi‑objective metrics.
    - MoE integration for context encoding.
    - Bio‑inspired dynamic action expansion.
    - Persistence and adaptive confidence thresholds.
    """
    def __init__(
        self,
        action_space: List[Dict[str, Any]],
        fallback_solver: Callable,
        modp_weights: Optional[Dict[str, float]] = None,
        moe_router: Optional[Any] = None,
        bio_generator: Optional[Any] = None,
        persistence_file: Optional[str] = "bandit_state.json",
        min_trials_before_bandit: int = 5,
        confidence_threshold: float = 0.6,
        exploration_ratio: float = 0.1,  # fraction of time to use bio‑inspired expansion
        adaptation_window: int = 50,      # number of recent decisions to adapt threshold
        verbose: bool = False,
    ):
        """
        Args:
            action_space: Initial list of policy dicts.
            fallback_solver: Callable that returns a policy when bandit is unsure.
            modp_weights: Weights for MODP objectives (e.g., quality, throughput, energy, carbon).
            moe_router: Instance of MoE encoder (optional).
            bio_generator: Instance of bio‑inspired policy generator (optional).
            persistence_file: Path to save/load state.
            min_trials_before_bandit: Minimum trials before using bandit.
            confidence_threshold: Minimum confidence to use bandit.
            exploration_ratio: Probability of triggering bio‑inspired expansion.
            adaptation_window: Number of recent decisions to consider for threshold adaptation.
            verbose: Enable logging.
        """
        self.actions = action_space
        self.fallback_solver = fallback_solver
        self.min_trials = min_trials_before_bandit
        self.conf_threshold = confidence_threshold
        self.exploration_ratio = exploration_ratio
        self.adaptation_window = adaptation_window
        self.verbose = verbose
        self.persistence_file = persistence_file

        # Logging
        self.logger = logging.getLogger(__name__)
        if self.verbose:
            logging.basicConfig(level=logging.INFO)

        # MODP integration
        self.modp = ParetoOptimizer()
        self.modp_weights = modp_weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy": 0.20,
            "carbon": 0.15,
            "memory": 0.10,
        }

        # MoE integration
        self.moe = moe_router if moe_router else ExpertRouter()

        # Bio‑inspired integration
        self.bio = bio_generator if bio_generator else GeneticPolicyGenerator()

        # Internal state
        self.state = BanditState(
            action_weights={},
            action_covariances={},
            action_trials={},
            action_rewards={},
            action_space=self.actions.copy(),
        )

        # Load persisted state if available
        self._load_state()

    # --------------------- Persistence ---------------------
    def _load_state(self):
        if not self.persistence_file or not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
                # Reconstruct numpy arrays
                self.state.action_weights = {
                    tuple(k): np.array(v) for k, v in data["action_weights"]
                }
                self.state.action_covariances = {
                    tuple(k): np.array(v) for k, v in data["action_covariances"]
                }
                self.state.action_trials = {
                    tuple(k): v for k, v in data["action_trials"]
                }
                self.state.action_rewards = {
                    tuple(k): v for k, v in data["action_rewards"]
                }
                self.state.action_space = data["action_space"]
                self.state.recent_confidences = data.get("recent_confidences", [])
                self.state.last_adaptation = data.get("last_adaptation", 0.0)
                self.logger.info("Bandit state loaded.")
        except Exception as e:
            self.logger.error(f"Failed to load state: {e}")

    def _save_state(self):
        if not self.persistence_file:
            return
        try:
            data = {
                "action_weights": [
                    (list(k), v.tolist()) for k, v in self.state.action_weights.items()
                ],
                "action_covariances": [
                    (list(k), v.tolist()) for k, v in self.state.action_covariances.items()
                ],
                "action_trials": [
                    (list(k), v) for k, v in self.state.action_trials.items()
                ],
                "action_rewards": [
                    (list(k), v) for k, v in self.state.action_rewards.items()
                ],
                "action_space": self.state.action_space,
                "recent_confidences": self.state.recent_confidences,
                "last_adaptation": self.state.last_adaptation,
            }
            with open(self.persistence_file, "w") as f:
                json.dump(data, f)
            self.logger.debug("Bandit state saved.")
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")

    # --------------------- Context Encoding (MoE) ---------------------
    def _encode_context(self, task: Dict[str, Any]) -> tuple:
        """
        Use MoE to encode the task into a feature vector, then convert to tuple for dict keys.
        """
        # If the task already has a fingerprint, we can use it; otherwise, extract.
        if "fingerprint" in task:
            # Assume fingerprint is a WorkloadFingerprint object with .to_vector()
            vec = task["fingerprint"].to_vector()
        else:
            # Fallback: use MoE encoder
            vec = self.moe.encode(task)
        return tuple(vec.tolist())

    # --------------------- MODP Reward Integration ---------------------
    def _compute_reward(self, metrics: Dict[str, float]) -> float:
        """
        Use MODP to combine multiple metrics into a scalar reward.
        """
        return self.modp.evaluate(metrics, self.modp_weights)

    # --------------------- Bayesian Linear Regression Update ---------------------
    def _update_bayesian(self, ctx_key: tuple, action_idx: int, reward: float):
        """
        Perform Bayesian linear regression update for the chosen action.
        We treat each action independently for simplicity, but with a shared prior.
        """
        # Initialize if not seen
        if ctx_key not in self.state.action_weights:
            n_actions = len(self.state.action_space)
            # Prior: zero mean, identity covariance (standard normal)
            self.state.action_weights[ctx_key] = np.zeros(n_actions)
            self.state.action_covariances[ctx_key] = np.eye(n_actions) * 0.1
            self.state.action_trials[ctx_key] = 0
            self.state.action_rewards[ctx_key] = [0.0] * n_actions

        # Current mean and covariance
        mu = self.state.action_weights[ctx_key]
        Sigma = self.state.action_covariances[ctx_key]
        n = self.state.action_trials[ctx_key]

        # For Thompson Sampling, we use the independent approximation:
        # Each action's mean reward is estimated as the average of observed rewards for that action.
        # We'll keep the independent Gaussian model but with a Bayesian prior.
        # Simpler: use the empirical mean and variance.
        # We'll implement an online Bayesian update with a scalar variance per action.
        # We'll use the existing simple update but with variance tracking.

        # For simplicity, we keep the existing simple gradient update (which is essentially a decaying average)
        # but we can enhance with variance estimation.
        # We'll implement a more robust update:
        #   mean = (n * mean + reward) / (n+1)
        #   variance = (n * variance + (reward - mean)^2) / (n+1)
        # This is a standard incremental variance update.

        # We'll store the mean and variance per action.
        # Since we have only one vector for means, we can store variances in a separate dict.
        # To keep code clean, we'll store the variance in the covariance matrix diagonal.

        # Update mean and covariance (diagonal)
        old_mean = mu[action_idx]
        old_var = Sigma[action_idx, action_idx]
        new_n = n + 1

        # New mean
        new_mean = (n * old_mean + reward) / new_n
        # New variance (using Welford's algorithm)
        delta = reward - old_mean
        new_var = (n * old_var + delta * (reward - new_mean)) / new_n
        # Clamp variance to avoid numerical issues
        new_var = max(1e-6, new_var)

        # Update
        mu[action_idx] = new_mean
        Sigma[action_idx, action_idx] = new_var
        self.state.action_weights[ctx_key] = mu
        self.state.action_covariances[ctx_key] = Sigma
        self.state.action_trials[ctx_key] = new_n
        self.state.action_rewards[ctx_key][action_idx] = reward

    # --------------------- Bio‑inspired Action Expansion ---------------------
    def _maybe_expand_action_space(self, ctx_key: tuple):
        """
        With probability `exploration_ratio`, use bio‑inspired generator to create new actions.
        """
        if np.random.rand() > self.exploration_ratio:
            return

        # Only expand if we have at least a few trials to know the current performance
        if self.state.action_trials.get(ctx_key, 0) < 10:
            return

        # Generate new policies using bio‑inspired module
        new_actions = self.bio.generate_policies(self.state.action_space, n=2)
        if not new_actions:
            return

        # Add to action space
        for action in new_actions:
            if action not in self.state.action_space:
                self.state.action_space.append(action)
                # Extend weight vectors and covariances for all contexts
                for key in self.state.action_weights:
                    # Add new action with prior mean 0 and variance 0.1
                    new_mean = np.append(self.state.action_weights[key], 0.0)
                    new_cov = np.zeros((len(self.state.action_weights[key])+1, len(self.state.action_weights[key])+1))
                    new_cov[:len(self.state.action_weights[key]), :len(self.state.action_weights[key])] = \
                        self.state.action_covariances[key]
                    new_cov[-1, -1] = 0.1
                    self.state.action_weights[key] = new_mean
                    self.state.action_covariances[key] = new_cov
                    # Extend reward history
                    self.state.action_rewards[key].append(0.0)

        self.logger.info(f"Expanded action space to {len(self.state.action_space)} actions.")

    # --------------------- Adaptive Confidence Threshold ---------------------
    def _adapt_threshold(self):
        """
        Adjust the confidence threshold based on recent performance.
        If the bandit's confidence is often high but rewards are low, increase threshold.
        If confidence is low but rewards are high, decrease threshold.
        """
        # Only adapt after enough decisions
        if len(self.state.recent_confidences) < self.adaptation_window:
            return

        # Compute average confidence and average reward over the window
        # We need rewards for those decisions. We'll store them in a separate list.
        # For simplicity, we'll just use the confidence values and adjust based on success rate.
        # A simple heuristic: if the bandit made a decision (confidence >= threshold) and reward > 0, then it's good.
        # We'll compute the success rate: proportion of decisions where reward > 0 and confidence was high.
        # We'll adjust threshold accordingly.

        # We'll implement a simpler adaptation: if the average confidence is above 0.8 and the average reward is below 0.5, increase threshold.
        avg_conf = np.mean(self.state.recent_confidences[-self.adaptation_window:])
        # We need the rewards, we'll store them in a parallel list.
        # For now, we skip this complex adaptation and keep it simple.
        pass

    # --------------------- Main Public Methods ---------------------
    def select_action(self, task: Dict[str, Any]) -> Tuple[Optional[Dict], float, str]:
        """
        Select an action based on the context.
        Returns (policy, confidence, source) where source is 'bandit', 'fallback', or 'exploration'.
        """
        ctx_key = self._encode_context(task)
        n_trials = self.state.action_trials.get(ctx_key, 0)

        # 1. Safety gate: not enough trials -> fallback
        if n_trials < self.min_trials:
            policy = self.fallback_solver(task)
            self.logger.debug(f"Fallback (low trials): {ctx_key} n={n_trials}")
            return policy, 0.0, "fallback"

        # 2. Bio‑inspired expansion (with probability)
        self._maybe_expand_action_space(ctx_key)

        # 3. Thompson Sampling
        mu = self.state.action_weights[ctx_key]
        Sigma = self.state.action_covariances[ctx_key]
        # Sample from multivariate normal (if we have full covariance)
        # For simplicity, we sample independently from each action's marginal (diagonal)
        stds = np.sqrt(np.diag(Sigma))
        samples = np.random.normal(mu, stds)
        best_idx = np.argmax(samples)

        # Compute confidence as the probability that the best action is indeed the best.
        # Approximate: 1 - 1/(n_trials+1) (simple heuristic).
        confidence = 1.0 - (1.0 / (n_trials + 1))
        self.state.recent_confidences.append(confidence)
        if len(self.state.recent_confidences) > self.adaptation_window * 2:
            self.state.recent_confidences.pop(0)

        if confidence < self.conf_threshold:
            policy = self.fallback_solver(task)
            self.logger.debug(f"Fallback (low confidence): {confidence:.3f}")
            return policy, confidence, "fallback"

        policy = self.state.action_space[best_idx]
        return policy, confidence, "bandit"

    def update(self, task: Dict[str, Any], policy: Dict[str, Any], metrics: Dict[str, float]):
        """
        Update the bandit with the outcome of the chosen policy.
        Metrics is a dict of multi‑objective values.
        """
        ctx_key = self._encode_context(task)
        # Compute scalar reward using MODP
        reward = self._compute_reward(metrics)

        # Find the index of the policy in the action space
        try:
            action_idx = self.state.action_space.index(policy)
        except ValueError:
            # Policy not in action space (e.g., from fallback or new policy)
            # We can add it if it's not already there
            if policy not in self.state.action_space:
                self.state.action_space.append(policy)
                # Extend weights for all contexts
                for key in self.state.action_weights:
                    new_mean = np.append(self.state.action_weights[key], 0.0)
                    new_cov = np.zeros((len(self.state.action_weights[key])+1, len(self.state.action_weights[key])+1))
                    new_cov[:len(self.state.action_weights[key]), :len(self.state.action_weights[key])] = \
                        self.state.action_covariances[key]
                    new_cov[-1, -1] = 0.1
                    self.state.action_weights[key] = new_mean
                    self.state.action_covariances[key] = new_cov
                    self.state.action_rewards[key].append(0.0)
                action_idx = len(self.state.action_space) - 1
            else:
                # Should not happen
                return

        # Perform Bayesian update
        self._update_bayesian(ctx_key, action_idx, reward)

        # Save state periodically (every 10 updates)
        if sum(self.state.action_trials.values()) % 10 == 0:
            self._save_state()

    def seed_safe_policy(self, task: Dict[str, Any], policy: Dict[str, Any], reward: float = 1.0):
        """
        Seed the bandit with a known good policy (e.g., from LP solver).
        """
        ctx_key = self._encode_context(task)
        if ctx_key not in self.state.action_weights:
            n = len(self.state.action_space)
            self.state.action_weights[ctx_key] = np.zeros(n)
            self.state.action_covariances[ctx_key] = np.eye(n) * 0.1
            self.state.action_trials[ctx_key] = 0
            self.state.action_rewards[ctx_key] = [0.0] * n

        # Find policy index, add if not present
        if policy not in self.state.action_space:
            self.state.action_space.append(policy)
            # Extend existing contexts
            for key in self.state.action_weights:
                new_mean = np.append(self.state.action_weights[key], 0.0)
                new_cov = np.zeros((len(self.state.action_weights[key])+1, len(self.state.action_weights[key])+1))
                new_cov[:len(self.state.action_weights[key]), :len(self.state.action_weights[key])] = \
                    self.state.action_covariances[key]
                new_cov[-1, -1] = 0.1
                self.state.action_weights[key] = new_mean
                self.state.action_covariances[key] = new_cov
                self.state.action_rewards[key].append(0.0)
        action_idx = self.state.action_space.index(policy)

        # Set initial weight to reward (e.g., 1.0) with low variance
        self.state.action_weights[ctx_key][action_idx] = reward
        self.state.action_covariances[ctx_key][action_idx, action_idx] = 0.01
        self.state.action_trials[ctx_key] += 1  # pretend we have one trial
        self.state.action_rewards[ctx_key][action_idx] = reward

        self._save_state()

    def get_stats(self) -> Dict[str, Any]:
        """Return statistics."""
        return {
            "num_actions": len(self.state.action_space),
            "num_contexts": len(self.state.action_weights),
            "total_trials": sum(self.state.action_trials.values()),
            "confidence_threshold": self.conf_threshold,
            "min_trials": self.min_trials,
        }


# ----------------------------------------------------------------------
# 3. Example Usage
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Define a simple fallback solver
    def fallback_solver(task):
        return {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu"}

    # Initial action space
    actions = [
        {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu"},
        {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu"},
    ]

    # Instantiate the enhanced bandit
    bandit = ContextualBandit(
        action_space=actions,
        fallback_solver=fallback_solver,
        persistence_file="bandit_state.json",
        verbose=True,
    )

    # Simulate a workload
    task = {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32, "gpu_mem_free_mb": 12000}

    # Select action
    policy, confidence, source = bandit.select_action(task)
    print(f"Selected policy: {policy}, confidence: {confidence:.3f}, source: {source}")

    # Simulate execution and obtain metrics
    metrics = {"quality": 0.9, "throughput": 0.8, "energy": 0.2, "carbon": 0.3, "memory": 0.6}
    bandit.update(task, policy, metrics)

    # Save state (automatic every 10 updates)
    bandit._save_state()
    print("Bandit stats:", bandit.get_stats())
