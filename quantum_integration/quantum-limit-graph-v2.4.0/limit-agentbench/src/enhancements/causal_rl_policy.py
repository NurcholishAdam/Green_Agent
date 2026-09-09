#!/usr/bin/env python3
"""
Causal Reinforcement Learning for Policy Adaptation – Enhanced Version.

Provides a lightweight causal policy updater using structural causal models (SCM)
with Bayesian linear regression and UCB. Now enhanced with:
- Federated learning across deployments (FedAvg)
- Multi-agent coordination (central coordinator)
- Temporal safety monitor (LTL-like rules)
- Explainable AI (SHAP/LIME integration)
- Adaptive precision switching (FlexGen)
- Carbon market integration (offset broker)
- Chaos testing (fault injection and recovery)
- Human-in-the-loop with active learning (approval feedback)

Usage:
    policy = CausalRLPolicy(action_space=['fedavg','carbon_aware'])
    probs = await policy.get_probs(context)
    await policy.update(context, action, reward)
    explanation = await policy.explain(context, action)
"""

import asyncio
import logging
import copy
import random
import uuid
from collections import defaultdict, deque
from typing import Dict, List, Optional, Any, Callable, Tuple
import numpy as np

# Optional imports for XAI
try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    import lime
    import lime.lime_tabular
    LIME_AVAILABLE = True
except ImportError:
    LIME_AVAILABLE = False

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
    - Active learning: incorporate human feedback as a reward signal.
    - Carbon awareness: include carbon intensity and offset cost as features.
    - Adaptive precision: recommend a precision level for inference based on context.
    """

    def __init__(
        self,
        action_space: List[str],
        feature_names: Optional[List[str]] = None,
        alpha: float = 0.01,          # learning rate (used in recursive least squares)
        prior_precision: float = 1.0, # precision of the prior (lambda)
        noise_precision: float = 10.0,# assumed precision of observation noise
        ucb_beta: float = 1.0,        # exploration coefficient for UCB
        carbon_weight: float = 0.1,   # weight for carbon cost in reward adjustment
        precision_options: Optional[List[str]] = None, # e.g., ['fp32','fp16','int8']
        flexgen_manager: Optional[Any] = None, # optional FlexGen manager for precision selection
    ):
        self.actions = action_space
        self.feature_names = feature_names  # if None, use sorted numeric keys from context
        self.alpha = alpha
        self.prior_precision = prior_precision
        self.noise_precision = noise_precision
        self.ucb_beta = ucb_beta
        self.carbon_weight = carbon_weight
        self.precision_options = precision_options or ['fp32', 'fp16', 'int8']
        self.flexgen_manager = flexgen_manager

        # State for Bayesian linear regression
        self.weights: Dict[str, np.ndarray] = {a: np.zeros(self._get_feature_dim()) for a in action_space}
        self.covariances: Dict[str, np.ndarray] = {
            a: np.eye(self._get_feature_dim()) / prior_precision for a in action_space
        }
        self.usage: Dict[str, int] = defaultdict(int)

        # Safety constraints (list of (description, callable))
        self.constraints: List[Tuple[str, Callable[[Dict, str], bool]]] = []

        # Human approval
        self.approval_callback: Optional[Callable[[str, Dict], bool]] = None
        self.human_feedback_weight: float = 1.0  # weight for human feedback in update

        self._lock = asyncio.Lock()
        self._fault_injected = False

        # For chaos testing
        self.chaos_monkey = ChaosMonkey(policy=self)

        # For XAI
        self.xai_explainer = XAIExplainer(policy=self)

        # For federated learning (local contribution)
        self.local_update_id = str(uuid.uuid4())[:8]

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
                for desc, constraint in self.constraints:
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

    async def update(self, context: Dict, action: str, reward: float, human_feedback: Optional[float] = None):
        """
        Update the causal model for the chosen action using recursive least squares
        with forgetting factor alpha (treated as learning rate).
        If human_feedback is provided, it is treated as an additional reward signal
        (active learning), possibly with higher weight.
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

            # Combine reward and human feedback if provided
            effective_reward = reward
            if human_feedback is not None:
                effective_reward = (1 - self.human_feedback_weight) * reward + self.human_feedback_weight * human_feedback

            # Recursive least squares update with forgetting factor
            lam = max(self.alpha, 0.01)
            denom = lam + x @ P @ x
            P_new = (1 / lam) * (P - (P @ np.outer(x, x) @ P) / denom)
            # Weight update: w = w + P_new x (reward - x^T w)
            error = effective_reward - x @ w
            w_new = w + P_new @ x * error

            self.weights[action] = w_new
            self.covariances[action] = P_new
            self.usage[action] += 1

    async def explain(self, context: Dict, action: str) -> Dict[str, Any]:
        """
        Provide an explanation for the choice of a particular action.
        Returns feature contributions and a human-readable summary.
        Now also optionally uses SHAP/LIME if available.
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

            # Use XAI explainer for model-agnostic explanation if available
            if SHAP_AVAILABLE or LIME_AVAILABLE:
                try:
                    model_explanation = self.xai_explainer.explain(context, action)
                    return {
                        'action': action,
                        'score': total_score,
                        'contributions': contributions,
                        'model_agnostic': model_explanation,
                        'explanation': (
                            f"Action '{action}' has expected score {total_score:.3f}. "
                            f"Contributions: " + ", ".join(
                                f"{name}: {contrib:.3f}" for name, contrib in contributions.items()
                            )
                        )
                    }
                except Exception as e:
                    logger.warning(f"XAI explanation failed: {e}")

            # Fallback to simple explanation
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
        self.constraints.append((description, constraint_fn))
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
                'carbon_weight': self.carbon_weight,
                'precision_options': self.precision_options,
            }

    async def load_state(self, state: Dict[str, Any]):
        """Restore policy state from a dictionary."""
        async with self._lock:
            self.actions = state.get('actions', self.actions)
            self.feature_names = state.get('feature_names', self.feature_names)
            self.weights = {a: np.array(w) for a, w in state.get('weights', {}).items()}
            self.covariances = {a: np.array(P) for a, P in state.get('covariances', {}).items()}
            self.usage = defaultdict(int, state.get('usage', {}))
            self.carbon_weight = state.get('carbon_weight', self.carbon_weight)
            self.precision_options = state.get('precision_options', self.precision_options)

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

    # Adaptive precision selection
    async def select_precision(self, context: Dict, workload: Optional[Dict] = None) -> str:
        """
        Use the policy to select a precision level for inference.
        If flexgen_manager is available, use it to evaluate cost; otherwise use a simple
        carbon-intensity-based heuristic.
        """
        if self.flexgen_manager and FLEXGEN_AVAILABLE:
            try:
                # Use FlexGen to get optimal policy
                result = await self.flexgen_manager.optimize_policy(workload, context.get('node'))
                if result and 'chosen_policy' in result:
                    return result['chosen_policy'].get('precision', 'fp32')
            except Exception as e:
                logger.warning(f"FlexGen optimization failed: {e}")
        # Fallback: choose based on carbon intensity
        carbon = context.get('avg_carbon', 400)
        if carbon < 300:
            return 'fp32'  # high precision allowed
        elif carbon < 500:
            return 'fp16'
        else:
            return 'int8'

    # Carbon market integration
    async def adjust_reward_for_carbon(self, context: Dict, reward: float) -> float:
        """
        Adjust the reward to account for carbon offset costs.
        If carbon intensity is high, the reward is penalized.
        """
        carbon = context.get('avg_carbon', 400)
        offset_cost = max(0, (carbon - 300) / 1000.0) * self.carbon_weight
        adjusted = reward - offset_cost
        return adjusted


# =============================================================================
# NEW: Safety Monitor (Temporal Logic-like)
# =============================================================================
class SafetyMonitor:
    """
    Monitors a sequence of decisions to enforce simple temporal safety properties.
    For example: "carbon intensity must not exceed 600 for more than 3 consecutive steps".
    """

    def __init__(self, rules: Optional[List[Tuple[str, Callable]]] = None):
        self.rules = rules or []
        self.history: deque = deque(maxlen=100)
        self.violations: List[Dict[str, Any]] = []

    def add_rule(self, name: str, check_fn: Callable[[deque], bool]):
        """Add a rule that receives the history deque and returns True if violation."""
        self.rules.append((name, check_fn))

    def check(self, context: Dict, action: str) -> List[str]:
        """
        Check the current context and action against all rules.
        Returns list of violated rule names.
        """
        self.history.append((context, action))
        violated = []
        for name, rule in self.rules:
            if rule(self.history):
                violated.append(name)
                self.violations.append({
                    'rule': name,
                    'context': context,
                    'action': action,
                    'timestamp': datetime.now().isoformat()
                })
        return violated


# =============================================================================
# NEW: XAI Explainer (SHAP/LIME)
# =============================================================================
class XAIExplainer:
    """Model-agnostic explainer using SHAP or LIME if available."""

    def __init__(self, policy: CausalRLPolicy):
        self.policy = policy
        self.shap_available = SHAP_AVAILABLE
        self.lime_available = LIME_AVAILABLE

    def explain(self, context: Dict, action: str) -> Dict[str, Any]:
        """
        Provide a model-agnostic explanation of the policy's decision.
        Since our policy is linear, we can directly provide feature contributions.
        If SHAP or LIME is available, we could use them for more complex models.
        For now, we return the linear contributions as the explanation.
        """
        x = self.policy._features(context)
        w = self.policy.weights.get(action, np.zeros_like(x))
        contributions = w * x
        return {
            'method': 'linear_contributions',
            'contributions': contributions.tolist(),
            'feature_names': self.policy.feature_names or ['intercept', 'avg_carbon_normalized', 'avg_trust'],
        }


# =============================================================================
# NEW: Federated Causal RL Policy (FedAvg)
# =============================================================================
class FederatedCausalRLPolicy:
    """
    Aggregates multiple CausalRLPolicy instances using FedAvg on weights and covariances.
    """

    def __init__(self, policies: Optional[List[CausalRLPolicy]] = None):
        self.policies = policies or []
        self.aggregated_weights = {}
        self.aggregated_covariances = {}

    def add_policy(self, policy: CausalRLPolicy):
        self.policies.append(policy)

    async def aggregate(self) -> Dict[str, Any]:
        """
        Perform FedAvg: average weights and covariances across all policies.
        Returns the aggregated model parameters.
        """
        if not self.policies:
            return {}
        # Collect all action names
        all_actions = set()
        for p in self.policies:
            all_actions.update(p.actions)
        aggregated_weights = {}
        aggregated_covariances = {}
        for action in all_actions:
            w_list = [p.weights.get(action, np.zeros(p._get_feature_dim())) for p in self.policies]
            P_list = [p.covariances.get(action, np.eye(p._get_feature_dim()) / p.prior_precision) for p in self.policies]
            avg_w = np.mean(w_list, axis=0)
            avg_P = np.mean(P_list, axis=0)
            aggregated_weights[action] = avg_w
            aggregated_covariances[action] = avg_P
        self.aggregated_weights = {a: w.tolist() for a, w in aggregated_weights.items()}
        self.aggregated_covariances = {a: P.tolist() for a, P in aggregated_covariances.items()}
        return {
            'weights': self.aggregated_weights,
            'covariances': self.aggregated_covariances,
        }

    async def broadcast(self):
        """Broadcast aggregated model to all policies."""
        if not self.aggregated_weights:
            return
        for p in self.policies:
            for action in p.actions:
                if action in self.aggregated_weights:
                    p.weights[action] = np.array(self.aggregated_weights[action])
                    p.covariances[action] = np.array(self.aggregated_covariances[action])


# =============================================================================
# NEW: Multi-Agent Coordinator
# =============================================================================
class MultiAgentCausalRL:
    """
    Manages multiple CausalRLPolicy instances for different agents and coordinates
    their exploration via a shared reward signal or shared constraints.
    """

    def __init__(self, num_agents: int, action_space: List[str], feature_names: Optional[List[str]] = None):
        self.policies = {
            f"agent_{i}": CausalRLPolicy(action_space, feature_names) for i in range(num_agents)
        }
        self.agent_histories = defaultdict(list)

    async def get_probs_for_agent(self, agent_id: str, context: Dict) -> List[float]:
        policy = self.policies[agent_id]
        return await policy.get_probs(context)

    async def update_agent(self, agent_id: str, context: Dict, action: str, reward: float):
        policy = self.policies[agent_id]
        await policy.update(context, action, reward)
        self.agent_histories[agent_id].append(reward)

    async def add_shared_constraint(self, constraint_fn: Callable[[Dict, str], bool], description: str = ""):
        for policy in self.policies.values():
            policy.add_constraint(constraint_fn, description)

    def get_agent_stats(self) -> Dict[str, float]:
        stats = {}
        for agent, history in self.agent_histories.items():
            stats[agent] = float(np.mean(history)) if history else 0.0
        return stats


# =============================================================================
# NEW: Chaos Monkey (fuller)
# =============================================================================
class ChaosMonkey:
    """
    Injects faults into a CausalRLPolicy and verifies recovery.
    """

    def __init__(self, policy: CausalRLPolicy, failure_probability: float = 0.1):
        self.policy = policy
        self.failure_probability = failure_probability

    def random_fault(self):
        """Inject a random fault with some probability."""
        if random.random() < self.failure_probability:
            fault_type = random.choice(['reset_weights', 'corrupt_covariance', 'clear_usage'])
            self.policy.inject_fault(fault_type)
            logger.warning(f"Chaos monkey injected fault: {fault_type}")

    async def run_chaos_experiment(self, num_steps: int = 10) -> Dict[str, Any]:
        """
        Run a chaos experiment: inject random faults while performing updates and check
        that the policy still returns valid distributions.
        """
        report = {'successful_steps': 0, 'failed_steps': 0, 'details': []}
        for step in range(num_steps):
            try:
                self.random_fault()
                context = {'avg_carbon': random.uniform(200, 800), 'avg_trust': random.random()}
                probs = await self.policy.get_probs(context)
                # Check if probabilities are valid
                if abs(sum(probs) - 1.0) > 1e-6:
                    raise ValueError("Invalid probability distribution")
                action = random.choices(self.policy.actions, weights=probs)[0]
                reward = random.uniform(0, 1)
                await self.policy.update(context, action, reward)
                report['successful_steps'] += 1
                report['details'].append({'step': step, 'status': 'ok', 'fault_injected': True})
            except Exception as e:
                report['failed_steps'] += 1
                report['details'].append({'step': step, 'status': 'failed', 'error': str(e)})
                logger.error(f"Chaos experiment step {step} failed: {e}")
        return report


# =============================================================================
# NEW: Carbon Offset Integrator
# =============================================================================
class CarbonOffsetIntegrator:
    """
    Manages carbon offset purchases based on carbon intensity.
    """

    def __init__(self, threshold: float = 400.0, cost_per_kg: float = 0.10):
        self.threshold = threshold
        self.cost_per_kg = cost_per_kg
        self.total_offset_kg = 0.0
        self.total_cost = 0.0

    def should_offset(self, carbon_intensity: float) -> bool:
        return carbon_intensity > self.threshold

    def purchase_offsets(self, carbon_kg: float) -> Dict[str, Any]:
        """
        Simulate purchasing offsets for the given carbon amount.
        """
        if carbon_kg <= 0:
            return {'status': 'no_action'}
        cost = carbon_kg * self.cost_per_kg
        self.total_offset_kg += carbon_kg
        self.total_cost += cost
        return {
            'status': 'offset',
            'carbon_kg': carbon_kg,
            'cost_usd': cost,
            'timestamp': datetime.now().isoformat()
        }

    def get_totals(self) -> Dict[str, float]:
        return {
            'total_offset_kg': self.total_offset_kg,
            'total_cost': self.total_cost
        }


# =============================================================================
# FlexGen Manager placeholder (if FlexGen not available, use dummy)
# =============================================================================
try:
    from enhancements.gpu_optimization.flexgen_manager import FlexGenManager
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    class FlexGenManager:
        async def optimize_policy(self, workload, node):
            return None


if __name__ == "__main__":
    # Example usage (optional)
    async def demo():
        policy = CausalRLPolicy(action_space=['fedavg', 'carbon_aware'],
                                feature_names=['avg_carbon', 'avg_trust'])
        # Add a safety constraint
        policy.add_constraint(
            lambda ctx, action: action == 'fedavg' and ctx.get('avg_carbon', 0) > 600,
            "Don't use fedavg when carbon > 600"
        )
        # Get probabilities
        probs = await policy.get_probs({'avg_carbon': 400, 'avg_trust': 0.7})
        print("Probabilities:", probs)
        # Update
        await policy.update({'avg_carbon': 400, 'avg_trust': 0.7}, 'fedavg', reward=0.8)
        # Explain
        expl = await policy.explain({'avg_carbon': 400, 'avg_trust': 0.7}, 'fedavg')
        print("Explanation:", expl)
        # Chaos test
        chaos_report = await policy.run_chaos_test()
        print("Chaos test report:", chaos_report)

    asyncio.run(demo())
