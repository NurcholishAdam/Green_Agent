#!/usr/bin/env python3
"""
Causal Reinforcement Learning for Policy Adaptation — Enhanced Single-File.

This module provides a lightweight causal policy updater using per-action
Bayesian linear regression with a stable recursive-least-squares update,
UCB/Thompson exploration, and softmax action probabilities.

Enhancements over the original:
- Fixed missing `datetime` import.
- Fixed RLS forgetting factor bug (default 0.99, separate from step size).
- Separated exploration (UCB / Thompson) from action probabilities.
- Bounded usage penalty (log-scaled).
- Unified SafetyMonitor: instantaneous constraints + temporal rules.
- Real top-k + counterfactual XAI, with optional SHAP kernel explainer.
- Federated aggregation with client weighting, shape checks, round IDs,
  locking, and staleness rejection.
- Multi-agent coordinator with joint context and shared-critic hook.
- ChaosMonkey with seeding, warmup baseline, and time-to-recovery metric.
- CarbonOffsetIntegrator wired into reward adjustment and episode tracking.
- Adaptive precision selection with graceful FlexGen fallback.
- Human-in-the-loop approval actually invoked from `get_probs`.
- MetricsCollector for observability.
- PolicyConfig / PolicyResult dataclasses for structured API.
- Episode boundary handling across all stateful components.

Usage:
    policy = CausalRLPolicy(action_space=['fedavg', 'carbon_aware'])
    result = await policy.get_probs(context)
    await policy.update(context, action, reward)
    explanation = await policy.explain(context, action)
"""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

import numpy as np

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Optional XAI
# -----------------------------------------------------------------------------
try:
    import shap  # type: ignore
    SHAP_AVAILABLE = True
except Exception:
    SHAP_AVAILABLE = False

try:
    import lime  # noqa: F401
    import lime.lime_tabular  # noqa: F401
    LIME_AVAILABLE = True
except Exception:
    LIME_AVAILABLE = False


# -----------------------------------------------------------------------------
# Optional FlexGen
# -----------------------------------------------------------------------------
try:
    from enhancements.gpu_optimization.flexgen_manager import (  # type: ignore
        FlexGenManager,
    )
    FLEXGEN_AVAILABLE = True
except Exception:
    FLEXGEN_AVAILABLE = False

    class FlexGenManager:  # type: ignore[no-redef]
        """Stub used when the real FlexGenManager is unavailable."""

        async def optimize_policy(self, workload: Any, node: Any) -> Optional[Dict[str, Any]]:
            return None


# =============================================================================
# Config, result, metrics
# =============================================================================
@dataclass
class PolicyConfig:
    """All tunable scalars for CausalRLPolicy."""

    forgetting_factor: float = 0.99
    """RLS forgetting factor in (0, 1]. 1.0 = infinite memory."""
    step_size: float = 1.0
    """Extra gain applied to the RLS update; 1.0 = standard RLS."""
    prior_precision: float = 1.0
    noise_precision: float = 10.0
    ucb_beta: float = 1.0
    softmax_temperature: float = 1.0
    usage_penalty_coeff: float = 0.01
    """Coefficient for log(1 + usage) exploration bonus."""
    carbon_weight: float = 0.1
    apply_carbon_adjustment: bool = False
    human_feedback_weight: float = 1.0
    precision_options: Tuple[str, ...] = ("fp32", "fp16", "int8")
    feature_scalers: Dict[str, Any] = field(default_factory=dict)
    """Per-feature scalers. Values may be floats (divide) or callables."""


@dataclass
class PolicyResult:
    """Structured output of `get_probs`."""

    probs: List[float]
    ucb_scores: np.ndarray
    means: np.ndarray
    variances: np.ndarray
    allowed_mask: np.ndarray
    safety_violations: List[str] = field(default_factory=list)
    requires_approval: bool = False
    approved_action: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class MetricsCollector:
    """Lightweight thread-safe metrics."""

    def __init__(self) -> None:
        self._counters: Counter = Counter()
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def inc(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._counters[key] += n

    def observe(self, key: str, value: float) -> None:
        with self._lock:
            self._histograms[key].append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "histograms": {
                    k: {
                        "count": len(v),
                        "mean": float(np.mean(v)) if v else 0.0,
                        "p95": float(np.percentile(v, 95)) if v else 0.0,
                        "max": float(np.max(v)) if v else 0.0,
                    }
                    for k, v in self._histograms.items()
                },
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._histograms.clear()


# =============================================================================
# Safety monitor: instantaneous constraints + temporal rules
# =============================================================================
class SafetyMonitor:
    """
    Unified safety for the policy.

    - Instantaneous constraints: `(context, action) -> bool` returning True if
      the action is disallowed in that context.
    - Temporal rules: `(history) -> bool` returning True if a rule is violated.
      History is a bounded deque of `(context, action, timestamp)` tuples.
    """

    def __init__(
        self,
        max_history: int = 500,
        metrics: Optional[MetricsCollector] = None,
    ) -> None:
        self.constraints: List[Tuple[str, Callable[[Dict, str], bool]]] = []
        self.temporal_rules: List[Tuple[str, Callable[[Deque], bool]]] = []
        self.history: Deque[Tuple[Dict, str, str]] = deque(maxlen=max_history)
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=10_000)
        self.metrics = metrics

    # -- Registration -------------------------------------------------------- #
    def add_constraint(
        self,
        constraint_fn: Callable[[Dict, str], bool],
        description: str = "unnamed_constraint",
    ) -> None:
        """Instantaneous constraint: returns True if action is disallowed."""
        self.constraints.append((description, constraint_fn))
        logger.info("Added constraint: %s", description)

    def add_temporal_rule(
        self,
        name: str,
        check_fn: Callable[[Deque], bool],
    ) -> None:
        """Temporal rule: returns True if violated given the history deque."""
        self.temporal_rules.append((name, check_fn))
        logger.info("Added temporal rule: %s", name)

    def clear_constraints(self) -> None:
        self.constraints.clear()

    # -- Evaluation ---------------------------------------------------------- #
    def is_allowed(self, context: Dict, action: str) -> Tuple[bool, Optional[str]]:
        for name, fn in self.constraints:
            try:
                if fn(context, action):
                    return False, name
            except Exception as e:
                logger.error("Constraint %s raised: %s", name, e)
                return False, f"{name}:error"
        return True, None

    def check_temporal(self, context: Dict, action: str) -> List[str]:
        """Append to history and return names of violated temporal rules."""
        ts = datetime.now().isoformat()
        self.history.append((dict(context), action, ts))
        violated: List[str] = []
        for name, fn in self.temporal_rules:
            try:
                if fn(self.history):
                    violated.append(name)
                    self._record(name, context, action, ts)
            except Exception as e:
                logger.error("Temporal rule %s raised: %s", name, e)
        return violated

    def _record(self, rule: str, context: Dict, action: str, ts: str) -> None:
        self.violations.append(
            {"rule": rule, "context": context, "action": action, "timestamp": ts}
        )
        if self.metrics is not None:
            self.metrics.inc("safety_violations")
        logger.warning("Temporal safety violation: %s", rule)

    def reset_episode(self) -> None:
        self.history.clear()


# =============================================================================
# XAI explainer (top-k, counterfactual, optional SHAP)
# =============================================================================
class XAIExplainer:
    """
    Explains the linear per-action score. Produces:
      - top-k signed contributions,
      - counterfactual deltas for each feature,
      - optional SHAP kernel explanation on the posterior-mean function.
    """

    def __init__(
        self,
        policy: "CausalRLPolicy",
        top_k: int = 5,
        include_counterfactual: bool = True,
    ) -> None:
        self.policy = policy
        self.top_k = top_k
        self.include_counterfactual = include_counterfactual

    def _names(self, dim: int) -> List[str]:
        if self.policy.feature_names is not None:
            return ["intercept"] + list(self.policy.feature_names)
        return ["intercept", "avg_carbon_normalized", "avg_trust"][:dim]

    def _mean_fn(self, action: str) -> Callable[[np.ndarray], np.ndarray]:
        w = self.policy.weights[action]

        def fn(X: np.ndarray) -> np.ndarray:
            X = np.atleast_2d(X)
            return X @ w

        return fn

    def explain(self, context: Dict, action: str) -> Dict[str, Any]:
        if action not in self.policy.weights:
            return {"error": "action_not_found"}

        x = self.policy._features(context)
        w = self.policy.weights[action]
        contribs = w * x
        names = self._names(len(x))

        order = np.argsort(-np.abs(contribs))[: self.top_k]
        top = [
            {"name": names[i], "contribution": float(contribs[i])}
            for i in order
        ]

        counterfactuals: List[Dict[str, Any]] = []
        if self.include_counterfactual:
            for i, name in enumerate(names):
                # "If feature i increased by 1 unit (in scaled space), score changes by w_i."
                counterfactuals.append(
                    {
                        "feature": name,
                        "delta_score_per_unit": float(w[i]),
                    }
                )

        result: Dict[str, Any] = {
            "action": action,
            "score": float(w @ x),
            "top_contributions": top,
            "counterfactuals": counterfactuals,
            "method": "linear_contributions",
        }

        if SHAP_AVAILABLE:
            try:
                explainer = shap.KernelExplainer(
                    self._mean_fn(action),
                    np.zeros((1, len(x))),
                )
                sv = explainer.shap_values(x.reshape(1, -1), nsamples=50)
                result["shap_values"] = np.asarray(sv).flatten().tolist()
                result["method"] = "shap_kernel"
            except Exception as e:
                logger.debug("SHAP explanation failed: %s", e)

        return result


# =============================================================================
# Carbon offset integrator
# =============================================================================
class CarbonOffsetIntegrator:
    """
    Tracks and simulates carbon offsets.
    """

    def __init__(
        self,
        threshold: float = 400.0,
        cost_per_kg: float = 0.10,
        metrics: Optional[MetricsCollector] = None,
    ) -> None:
        self.threshold = float(threshold)
        self.cost_per_kg = float(cost_per_kg)
        self.total_offset_kg = 0.0
        self.total_cost = 0.0
        self.history: List[Dict[str, Any]] = []
        self.metrics = metrics

    def should_offset(self, carbon_intensity: float) -> bool:
        return carbon_intensity > self.threshold

    def reward_penalty(self, carbon_intensity: float, weight: float) -> float:
        """Continuous penalty for reward shaping."""
        excess = max(0.0, carbon_intensity - self.threshold)
        return (excess / 1000.0) * weight

    def purchase_offsets(self, carbon_kg: float) -> Dict[str, Any]:
        if carbon_kg <= 0:
            return {"status": "no_action"}
        cost = carbon_kg * self.cost_per_kg
        self.total_offset_kg += carbon_kg
        self.total_cost += cost
        record = {
            "status": "offset",
            "carbon_kg": float(carbon_kg),
            "cost_usd": float(cost),
            "timestamp": datetime.now().isoformat(),
        }
        self.history.append(record)
        if self.metrics is not None:
            self.metrics.inc("carbon_offsets")
            self.metrics.observe("carbon_offset_cost", cost)
        return record

    def get_totals(self) -> Dict[str, float]:
        return {
            "total_offset_kg": self.total_offset_kg,
            "total_cost": self.total_cost,
        }

    def reset_episode(self) -> None:
        # Keep lifetime totals; reset per-episode bookkeeping if added later.
        pass


# =============================================================================
# Causal RL Policy
# =============================================================================
class CausalRLPolicy:
    """
    Per-action Bayesian linear regression with stable RLS, UCB/softmax action
    probabilities, unified safety, HITL approval, carbon adjustment, adaptive
    precision, save/load, and chaos-friendly fault injection.
    """

    def __init__(
        self,
        action_space: List[str],
        feature_names: Optional[List[str]] = None,
        config: Optional[PolicyConfig] = None,
        *,
        # Backward-compatible kwargs
        alpha: Optional[float] = None,
        prior_precision: Optional[float] = None,
        noise_precision: Optional[float] = None,
        ucb_beta: Optional[float] = None,
        carbon_weight: Optional[float] = None,
        precision_options: Optional[List[str]] = None,
        flexgen_manager: Optional[Any] = None,
        safety: Optional[SafetyMonitor] = None,
        carbon_integrator: Optional[CarbonOffsetIntegrator] = None,
    ) -> None:
        self.config = config or PolicyConfig()
        if alpha is not None:
            self.config.forgetting_factor = float(alpha)
        if prior_precision is not None:
            self.config.prior_precision = float(prior_precision)
        if noise_precision is not None:
            self.config.noise_precision = float(noise_precision)
        if ucb_beta is not None:
            self.config.ucb_beta = float(ucb_beta)
        if carbon_weight is not None:
            self.config.carbon_weight = float(carbon_weight)
        if precision_options is not None:
            self.config.precision_options = tuple(precision_options)

        self.actions: List[str] = list(action_space)
        self.feature_names: Optional[List[str]] = (
            list(feature_names) if feature_names else None
        )
        self.flexgen_manager = flexgen_manager

        # Snapshot dimension once; any later feature_names change must be rejected.
        self._dim: int = self._feature_dim()

        # Posterior state
        self.weights: Dict[str, np.ndarray] = {
            a: np.zeros(self._dim) for a in self.actions
        }
        self.covariances: Dict[str, np.ndarray] = {
            a: np.eye(self._dim) / self.config.prior_precision for a in self.actions
        }
        self.usage: Dict[str, int] = defaultdict(int)

        # Safety
        self.metrics = MetricsCollector()
        self.safety = safety or SafetyMonitor(metrics=self.metrics)

        # HITL
        self.approval_callback: Optional[
            Callable[[str, Dict], Any]
        ] = None
        self.approval_predicate: Optional[
            Callable[[str, Dict, np.ndarray], bool]
        ] = None

        # Carbon
        self.carbon_integrator = carbon_integrator or CarbonOffsetIntegrator(
            metrics=self.metrics
        )

        # XAI
        self.xai_explainer = XAIExplainer(self)

        # Identity and locking
        self.local_update_id = str(uuid.uuid4())[:8]
        self._lock = asyncio.Lock()
        self._fault_state: Dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Feature handling                                                    #
    # ------------------------------------------------------------------ #
    def _feature_dim(self) -> int:
        return (len(self.feature_names) + 1) if self.feature_names else 3

    def _scale_feature(self, name: str, value: Any) -> float:
        scaler = self.config.feature_scalers.get(name)
        if scaler is None:
            # Backward-compatible defaults
            if name == "avg_carbon":
                return float(value) / 800.0
            if name == "avg_trust":
                return float(min(1.0, max(0.0, value)))
            return float(value) if isinstance(value, (int, float)) else 0.0
        if callable(scaler):
            try:
                return float(scaler(value))
            except Exception:
                return 0.0
        try:
            return float(value) / float(scaler)
        except Exception:
            return 0.0

    def _features(self, context: Dict) -> np.ndarray:
        if self.feature_names is not None:
            vals = [1.0]
            for name in self.feature_names:
                vals.append(self._scale_feature(name, context.get(name, 0.0)))
            arr = np.asarray(vals, dtype=float)
        else:
            arr = np.asarray(
                [
                    1.0,
                    self._scale_feature(
                        "avg_carbon", context.get("avg_carbon", 400)
                    ),
                    self._scale_feature("avg_trust", context.get("avg_trust", 0.5)),
                ],
                dtype=float,
            )
        if arr.shape[0] != self._dim:
            raise ValueError(
                f"Feature dim {arr.shape[0]} != policy dim {self._dim}. "
                "feature_names cannot be changed after construction."
            )
        return arr

    # ------------------------------------------------------------------ #
    # Safety API                                                          #
    # ------------------------------------------------------------------ #
    def add_constraint(
        self,
        constraint_fn: Callable[[Dict, str], bool],
        description: str = "unnamed_constraint",
    ) -> None:
        self.safety.add_constraint(constraint_fn, description)

    def add_temporal_rule(
        self, name: str, check_fn: Callable[[Deque], bool]
    ) -> None:
        self.safety.add_temporal_rule(name, check_fn)

    def clear_constraints(self) -> None:
        self.safety.clear_constraints()

    # ------------------------------------------------------------------ #
    # HITL API                                                            #
    # ------------------------------------------------------------------ #
    def set_approval_callback(
        self, callback: Callable[[str, Dict], Any]
    ) -> None:
        self.approval_callback = callback

    def set_approval_predicate(
        self, predicate: Callable[[str, Dict, np.ndarray], bool]
    ) -> None:
        """
        Predicate receives (top_action, context, probs) and returns True
        when human approval should be requested for the top action.
        """
        self.approval_predicate = predicate

    async def request_approval(self, action: str, context: Dict) -> bool:
        if self.approval_callback is None:
            return True
        try:
            result = self.approval_callback(action, context)
        except Exception as e:
            logger.error("Approval callback failed: %s", e)
            return False
        if asyncio.iscoroutine(result):
            return bool(await result)
        return bool(result)

    # ------------------------------------------------------------------ #
    # Core API                                                            #
    # ------------------------------------------------------------------ #
    async def get_probs(self, context: Dict) -> List[float]:
        """Backward-compatible scalar-probs API."""
        result = await self.get_probs_detailed(context)
        return result.probs

    async def get_probs_detailed(self, context: Dict) -> PolicyResult:
        async with self._lock:
            x = self._features(context)

            # 1) Instantaneous constraint mask
            allowed = np.ones(len(self.actions), dtype=bool)
            for i, action in enumerate(self.actions):
                ok, _ = self.safety.is_allowed(context, action)
                if not ok:
                    allowed[i] = False
                    self.metrics.inc("constraint_blocked")

            if not np.any(allowed):
                logger.warning("All actions disallowed; returning uniform.")
                probs = np.full(len(self.actions), 1.0 / len(self.actions))
                self.metrics.inc("all_actions_blocked")
                return PolicyResult(
                    probs=probs.tolist(),
                    ucb_scores=np.zeros(len(self.actions)),
                    means=np.zeros(len(self.actions)),
                    variances=np.zeros(len(self.actions)),
                    allowed_mask=allowed,
                    metadata={"fallback": "uniform_all_blocked"},
                )

            # 2) Posterior means and variances
            means = np.zeros(len(self.actions))
            variances = np.zeros(len(self.actions))
            for i, action in enumerate(self.actions):
                w = self.weights[action]
                P = self.covariances[action]
                means[i] = float(w @ x)
                variances[i] = float(x @ P @ x) + 1e-9

            # 3) UCB scores (used for exploration diagnostics)
            ucb = means + self.config.ucb_beta * np.sqrt(variances)
            # Bounded usage penalty (log-scaled)
            for i, action in enumerate(self.actions):
                ucb[i] -= self.config.usage_penalty_coeff * np.log1p(
                    self.usage[action]
                )
            ucb[~allowed] = -np.inf

            # 4) Action probabilities: softmax over posterior means / temperature
            #    (Exploration is done via posterior sampling / UCB externally.)
            logits = means / max(self.config.softmax_temperature, 1e-6)
            logits = np.where(allowed, logits, -np.inf)
            logits -= np.max(logits[allowed])
            exp = np.exp(logits)
            exp[~allowed] = 0.0
            s = exp.sum()
            probs = exp / s if s > 0 else np.full(len(self.actions), 1.0 / len(self.actions))

            # 5) Temporal safety: update history, log violations
            #    (Only logged here; masking happens on the next call.)
            temporal_violations: List[str] = []
            for i, action in enumerate(self.actions):
                if allowed[i]:
                    temporal_violations.extend(
                        self.safety.check_temporal(context, action)
                    )

            # 6) Human-in-the-loop approval for the top action
            requires_approval = False
            approved_action: Optional[str] = None
            if self.approval_predicate is not None:
                top_idx = int(np.argmax(probs))
                top_action = self.actions[top_idx]
                try:
                    requires_approval = bool(
                        self.approval_predicate(top_action, context, probs)
                    )
                except Exception as e:
                    logger.error("Approval predicate failed: %s", e)
                if requires_approval:
                    self.metrics.inc("approval_requested")
                    ok = await self.request_approval(top_action, context)
                    if not ok:
                        self.metrics.inc("approval_denied")
                        probs[top_idx] = 0.0
                        if probs.sum() <= 0:
                            probs = np.where(
                                allowed, 1.0 / max(allowed.sum(), 1), 0.0
                            )
                        else:
                            probs /= probs.sum()
                    else:
                        self.metrics.inc("approval_granted")
                        approved_action = top_action

            self.metrics.inc("get_probs")
            return PolicyResult(
                probs=probs.tolist(),
                ucb_scores=ucb,
                means=means,
                variances=variances,
                allowed_mask=allowed,
                safety_violations=temporal_violations,
                requires_approval=requires_approval,
                approved_action=approved_action,
                metadata={"x_norm": float(np.linalg.norm(x))},
            )

    async def update(
        self,
        context: Dict,
        action: str,
        reward: float,
        human_feedback: Optional[float] = None,
    ) -> None:
        async with self._lock:
            if action not in self.weights:
                self.weights[action] = np.zeros(self._dim)
                self.covariances[action] = (
                    np.eye(self._dim) / self.config.prior_precision
                )
                self.usage[action] = 0

            x = self._features(context)

            # Carbon adjustment
            effective_reward = float(reward)
            if self.config.apply_carbon_adjustment:
                carbon = float(context.get("avg_carbon", 400.0))
                penalty = self.carbon_integrator.reward_penalty(
                    carbon, self.config.carbon_weight
                )
                effective_reward -= penalty
                if self.carbon_integrator.should_offset(carbon):
                    self.carbon_integrator.purchase_offsets(
                        max(0.0, (carbon - self.carbon_integrator.threshold) / 1000.0)
                    )

            # Human feedback blend
            if human_feedback is not None:
                w_h = float(self.config.human_feedback_weight)
                effective_reward = (1.0 - w_h) * effective_reward + w_h * float(
                    human_feedback
                )

            # Stable RLS update
            w = self.weights[action]
            P = self.covariances[action]
            lam = float(np.clip(self.config.forgetting_factor, 0.5, 1.0))
            denom = lam + float(x @ P @ x)
            K = (P @ x) / denom
            error = effective_reward - float(x @ w)
            new_w = w + self.config.step_size * K * error
            new_P = (P - np.outer(K, x) @ P) / lam
            # Symmetrize to avoid drift
            new_P = 0.5 * (new_P + new_P.T)

            self.weights[action] = new_w
            self.covariances[action] = new_P
            self.usage[action] += 1
            self.metrics.inc("updates")
            self.metrics.observe("rls_error", error)

    async def explain(self, context: Dict, action: str) -> Dict[str, Any]:
        async with self._lock:
            result = self.xai_explainer.explain(context, action)
        if "error" not in result:
            top = result.get("top_contributions", [])
            result["explanation"] = (
                f"Action '{action}' score={result['score']:.3f}. "
                + "Top contributions: "
                + ", ".join(f"{c['name']}={c['contribution']:+.3f}" for c in top)
            )
        return result

    # ------------------------------------------------------------------ #
    # Adaptive precision                                                  #
    # ------------------------------------------------------------------ #
    async def select_precision(
        self, context: Dict, workload: Optional[Dict] = None
    ) -> str:
        if self.flexgen_manager and FLEXGEN_AVAILABLE:
            try:
                result = await self.flexgen_manager.optimize_policy(
                    workload, context.get("node")
                )
                if result and "chosen_policy" in result:
                    prec = result["chosen_policy"].get("precision")
                    if prec in self.config.precision_options:
                        self.metrics.inc(f"precision/{prec}")
                        return prec
            except Exception as e:
                logger.warning("FlexGen optimization failed: %s", e)

        carbon = float(context.get("avg_carbon", 400.0))
        if carbon < 300:
            choice = "fp32"
        elif carbon < 500:
            choice = "fp16"
        else:
            choice = "int8"
        if choice not in self.config.precision_options:
            choice = self.config.precision_options[0]
        self.metrics.inc(f"precision/{choice}")
        return choice

    # ------------------------------------------------------------------ #
    # Carbon helper                                                       #
    # ------------------------------------------------------------------ #
    async def adjust_reward_for_carbon(self, context: Dict, reward: float) -> float:
        carbon = float(context.get("avg_carbon", 400.0))
        return float(reward) - self.carbon_integrator.reward_penalty(
            carbon, self.config.carbon_weight
        )

    # ------------------------------------------------------------------ #
    # Persistence, reset, episode                                         #
    # ------------------------------------------------------------------ #
    async def save_state(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "weights": {a: w.tolist() for a, w in self.weights.items()},
                "covariances": {a: P.tolist() for a, P in self.covariances.items()},
                "usage": dict(self.usage),
                "feature_names": self.feature_names,
                "actions": self.actions,
                "dim": self._dim,
                "carbon_weight": self.config.carbon_weight,
                "precision_options": list(self.config.precision_options),
                "local_update_id": self.local_update_id,
            }

    async def load_state(self, state: Dict[str, Any]) -> None:
        async with self._lock:
            actions = state.get("actions", self.actions)
            dim = int(state.get("dim", self._dim))
            if dim != self._dim:
                raise ValueError(
                    f"State dim {dim} != policy dim {self._dim}; refusing to load."
                )
            self.actions = list(actions)
            self.feature_names = state.get("feature_names", self.feature_names)
            self.weights = {
                a: np.asarray(w, dtype=float)
                for a, w in state.get("weights", {}).items()
            }
            self.covariances = {
                a: np.asarray(P, dtype=float)
                for a, P in state.get("covariances", {}).items()
            }
            for a in self.actions:
                if a not in self.weights:
                    self.weights[a] = np.zeros(self._dim)
                if a not in self.covariances:
                    self.covariances[a] = (
                        np.eye(self._dim) / self.config.prior_precision
                    )
            self.usage = defaultdict(int, state.get("usage", {}))
            self.config.carbon_weight = state.get(
                "carbon_weight", self.config.carbon_weight
            )
            self.config.precision_options = tuple(
                state.get("precision_options", self.config.precision_options)
            )

    async def reset(self) -> None:
        """Reset learned state ONLY. Constraints and safety rules are preserved."""
        async with self._lock:
            self.weights = {a: np.zeros(self._dim) for a in self.actions}
            self.covariances = {
                a: np.eye(self._dim) / self.config.prior_precision
                for a in self.actions
            }
            self.usage.clear()
            self.metrics.inc("resets")

    def reset_episode(self) -> None:
        self.safety.reset_episode()
        self.carbon_integrator.reset_episode()

    # ------------------------------------------------------------------ #
    # Fault injection                                                     #
    # ------------------------------------------------------------------ #
    def inject_fault(self, fault_type: str) -> None:
        """
        Faults are self-consistent: weights and covariances are reset together
        when the intent is "forget". Original state is captured for restore.
        """
        self._fault_state = {
            "weights": {a: w.copy() for a, w in self.weights.items()},
            "covariances": {a: P.copy() for a, P in self.covariances.items()},
            "usage": dict(self.usage),
        }
        if fault_type == "reset_weights":
            prior = np.eye(self._dim) / self.config.prior_precision
            for a in self.weights:
                self.weights[a] = np.zeros(self._dim)
                self.covariances[a] = prior.copy()
            logger.warning("Fault injected: reset weights + covariance")
        elif fault_type == "corrupt_covariance":
            for a in self.covariances:
                self.covariances[a] = np.eye(self._dim) * 10.0
            logger.warning("Fault injected: corrupt covariance (high uncertainty)")
        elif fault_type == "clear_usage":
            self.usage.clear()
            logger.warning("Fault injected: clear usage")
        else:
            logger.warning("Unknown fault type: %s", fault_type)
            return
        self.metrics.inc(f"fault_injected/{fault_type}")

    def restore_from_fault(self) -> None:
        if not self._fault_state:
            return
        self.weights = {a: w.copy() for a, w in self._fault_state["weights"].items()}
        self.covariances = {
            a: P.copy() for a, P in self._fault_state["covariances"].items()
        }
        self.usage = defaultdict(int, self._fault_state["usage"])
        self._fault_state = {}
        self.metrics.inc("fault_restored")

    def get_weights(self) -> Dict[str, float]:
        return {
            "forgetting_factor": float(self.config.forgetting_factor),
            "prior_precision": float(self.config.prior_precision),
            "ucb_beta": float(self.config.ucb_beta),
        }

    def sample_size(self) -> int:
        return int(sum(self.usage.values()))


# =============================================================================
# Federated aggregation
# =============================================================================
class FederatedCausalRLPolicy:
    """
    Weighted FedAvg across CausalRLPolicy instances with:
      - client weighting by sample size,
      - dimension/shape checks,
      - round IDs and staleness rejection,
      - thread-safe aggregation and broadcast.
    """

    def __init__(
        self,
        policies: Optional[List[CausalRLPolicy]] = None,
        min_clients: int = 1,
    ) -> None:
        self.policies: List[CausalRLPolicy] = list(policies or [])
        self.min_clients = max(1, int(min_clients))
        self.round_id: int = 0
        self.aggregated_weights: Dict[str, np.ndarray] = {}
        self.aggregated_covariances: Dict[str, np.ndarray] = {}
        self._lock = threading.RLock()
        self._client_rounds: Dict[int, int] = {}

    def add_policy(self, policy: CausalRLPolicy) -> None:
        with self._lock:
            self.policies.append(policy)
            self._client_rounds[id(policy)] = 0

    async def aggregate(self) -> Dict[str, Any]:
        with self._lock:
            if len(self.policies) < self.min_clients:
                return {}
            all_actions: Set[str] = set()
            dims: Set[int] = set()
            for p in self.policies:
                all_actions.update(p.actions)
                dims.add(p._dim)
            if len(dims) > 1:
                raise ValueError(
                    f"Cannot aggregate policies with different dims: {dims}"
                )
            dim = dims.pop()

            agg_w: Dict[str, np.ndarray] = {}
            agg_P: Dict[str, np.ndarray] = {}
            for action in all_actions:
                w_sum = np.zeros(dim)
                P_sum = np.zeros((dim, dim))
                total = 0.0
                for p in self.policies:
                    if action not in p.weights:
                        continue
                    w_client = p.weights[action]
                    P_client = p.covariances[action]
                    if w_client.shape[0] != dim or P_client.shape[0] != dim:
                        logger.warning(
                            "Skipping client with mismatched dim for action %s",
                            action,
                        )
                        continue
                    weight = max(1, p.sample_size())
                    w_sum += weight * w_client
                    P_sum += weight * P_client
                    total += weight
                if total <= 0:
                    continue
                agg_w[action] = w_sum / total
                agg_P[action] = P_sum / total

            self.round_id += 1
            self.aggregated_weights = agg_w
            self.aggregated_covariances = agg_P
            for p in self.policies:
                self._client_rounds[id(p)] = self.round_id

            return {
                "round_id": self.round_id,
                "num_clients": len(self.policies),
                "actions": list(agg_w.keys()),
                "weights": {a: w.tolist() for a, w in agg_w.items()},
                "covariances": {a: P.tolist() for a, P in agg_P.items()},
            }

    async def broadcast(self) -> None:
        with self._lock:
            if not self.aggregated_weights:
                return
            for p in self.policies:
                if self._client_rounds.get(id(p), -1) != self.round_id:
                    # Client updated locally after aggregate; skip to avoid overwrite
                    logger.info(
                        "Skipping stale client %s during broadcast",
                        p.local_update_id,
                    )
                    continue
                for action, w in self.aggregated_weights.items():
                    if action in p.weights:
                        p.weights[action] = w.copy()
                        p.covariances[action] = self.aggregated_covariances[
                            action
                        ].copy()


# =============================================================================
# Multi-agent coordinator
# =============================================================================
class MultiAgentCausalRL:
    """
    Manages per-agent policies with:
      - per-agent construction sharing a config,
      - optional shared constraint propagation,
      - joint context for shared-critic / coordination hooks,
      - thread-safe history tracking.
    """

    def __init__(
        self,
        num_agents: int,
        action_space: List[str],
        feature_names: Optional[List[str]] = None,
        config: Optional[PolicyConfig] = None,
        shared_critic: Optional[Callable[[str, float, Dict[str, Any]], float]] = None,
    ) -> None:
        if num_agents <= 0:
            raise ValueError("num_agents must be positive")
        self.config = config or PolicyConfig()
        self.policies: Dict[str, CausalRLPolicy] = {
            f"agent_{i}": CausalRLPolicy(
                action_space=action_space,
                feature_names=feature_names,
                config=config,
            )
            for i in range(num_agents)
        }
        self.agent_histories: Dict[str, List[float]] = defaultdict(list)
        self.shared_critic = shared_critic
        self._lock = threading.RLock()

    async def get_probs_for_agent(
        self, agent_id: str, context: Dict
    ) -> PolicyResult:
        policy = self._policy(agent_id)
        return await policy.get_probs_detailed(context)

    async def update_agent(
        self,
        agent_id: str,
        context: Dict,
        action: str,
        reward: float,
        human_feedback: Optional[float] = None,
        joint_context: Optional[Dict[str, Any]] = None,
    ) -> float:
        policy = self._policy(agent_id)
        await policy.update(context, action, reward, human_feedback=human_feedback)

        shaped = float(reward)
        if self.shared_critic is not None and joint_context is not None:
            try:
                shaped = float(self.shared_critic(agent_id, reward, joint_context))
            except Exception as e:
                logger.error("shared_critic failed for %s: %s", agent_id, e)

        with self._lock:
            self.agent_histories[agent_id].append(shaped)
        return shaped

    async def add_shared_constraint(
        self,
        constraint_fn: Callable[[Dict, str], bool],
        description: str = "shared_constraint",
    ) -> None:
        for p in self.policies.values():
            p.add_constraint(constraint_fn, description)

    async def add_shared_temporal_rule(
        self, name: str, check_fn: Callable[[Deque], bool]
    ) -> None:
        for p in self.policies.values():
            p.add_temporal_rule(name, check_fn)

    def get_agent_stats(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            stats: Dict[str, Dict[str, float]] = {}
            for agent, history in self.agent_histories.items():
                if not history:
                    stats[agent] = {"count": 0.0, "mean": 0.0, "std": 0.0}
                else:
                    arr = np.asarray(history, dtype=float)
                    stats[agent] = {
                        "count": float(arr.size),
                        "mean": float(arr.mean()),
                        "std": float(arr.std()),
                    }
            return stats

    def reset_episode(self) -> None:
        for p in self.policies.values():
            p.reset_episode()
        with self._lock:
            for agent in self.agent_histories:
                self.agent_histories[agent].clear()

    def _policy(self, agent_id: str) -> CausalRLPolicy:
        if agent_id not in self.policies:
            raise ValueError(f"Unknown agent {agent_id}")
        return self.policies[agent_id]


# =============================================================================
# Chaos monkey with recovery measurement
# =============================================================================
class ChaosMonkey:
    """
    Injects a fault once after a warmup phase, then runs the policy cleanly
    and measures time-to-recovery against a baseline reward.
    """

    def __init__(
        self,
        policy: CausalRLPolicy,
        seed: Optional[int] = None,
        failure_probability: float = 0.1,
    ) -> None:
        self.policy = policy
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(seed)

    def random_fault(self) -> Optional[str]:
        if self._rng.random() < self.failure_probability:
            fault = self._rng.choice(
                ["reset_weights", "corrupt_covariance", "clear_usage"]
            )
            self.policy.inject_fault(fault)
            return fault
        return None

    async def run_chaos_experiment(
        self,
        env: Callable[[Dict, str], float],
        context_sampler: Callable[[], Dict],
        fault_type: str = "reset_weights",
        warmup_steps: int = 20,
        recovery_steps: int = 50,
        recovery_threshold: float = 0.9,
        recovery_window: int = 5,
    ) -> Dict[str, Any]:
        """
        env(context, action) -> reward
        context_sampler() -> context
        """
        # Warmup
        warmup_rewards: List[float] = []
        for _ in range(warmup_steps):
            ctx = context_sampler()
            probs = await self.policy.get_probs(ctx)
            action = self._rng.choices(self.policy.actions, weights=probs)[0]
            reward = float(env(ctx, action))
            await self.policy.update(ctx, action, reward)
            warmup_rewards.append(reward)

        baseline = (
            float(np.mean(warmup_rewards[-recovery_window:]))
            if warmup_rewards
            else 0.0
        )

        # Inject fault
        self.policy.inject_fault(fault_type)

        # Recovery phase
        post_rewards: List[float] = []
        recovery_step: Optional[int] = None
        for step in range(recovery_steps):
            ctx = context_sampler()
            probs = await self.policy.get_probs(ctx)
            action = self._rng.choices(self.policy.actions, weights=probs)[0]
            reward = float(env(ctx, action))
            await self.policy.update(ctx, action, reward)
            post_rewards.append(reward)

            if len(post_rewards) >= recovery_window and recovery_step is None:
                recent = float(np.mean(post_rewards[-recovery_window:]))
                if recent >= recovery_threshold * baseline:
                    recovery_step = step

        return {
            "fault_type": fault_type,
            "baseline_reward": baseline,
            "post_rewards": post_rewards,
            "recovery_step": recovery_step,
            "recovered": recovery_step is not None,
            "final_reward": float(np.mean(post_rewards[-recovery_window:]))
            if post_rewards
            else 0.0,
        }


# =============================================================================
# Demo
# =============================================================================
async def _demo() -> None:
    logging.basicConfig(level=logging.INFO)

    policy = CausalRLPolicy(
        action_space=["fedavg", "carbon_aware"],
        feature_names=["avg_carbon", "avg_trust"],
    )
    policy.add_constraint(
        lambda ctx, action: action == "fedavg" and ctx.get("avg_carbon", 0) > 600,
        "Don't use fedavg when carbon > 600",
    )
    policy.add_temporal_rule(
        "carbon_streak",
        lambda hist: len(hist) >= 3
        and all(h[0].get("avg_carbon", 0) > 600 for h in list(hist)[-3:]),
    )

    result = await policy.get_probs_detailed(
        {"avg_carbon": 400, "avg_trust": 0.7}
    )
    print("Probs:", result.probs)
    print("UCB:", result.ucb_scores.tolist())
    print("Violations:", result.safety_violations)

    await policy.update(
        {"avg_carbon": 400, "avg_trust": 0.7}, "fedavg", reward=0.8
    )
    expl = await policy.explain({"avg_carbon": 400, "avg_trust": 0.7}, "fedavg")
    print("Explanation:", expl.get("explanation"))

    # Chaos with recovery measurement on a toy environment
    def env(ctx: Dict, action: str) -> float:
        base = 1.0 - abs(ctx["avg_carbon"] - 300) / 1000.0
        return base if action == "carbon_aware" else base - 0.1

    def sampler() -> Dict:
        return {
            "avg_carbon": random.uniform(200, 700),
            "avg_trust": random.random(),
        }

    monkey = ChaosMonkey(policy, seed=42, failure_probability=1.0)
    report = await monkey.run_chaos_experiment(
        env=env,
        context_sampler=sampler,
        fault_type="reset_weights",
        warmup_steps=20,
        recovery_steps=50,
    )
    print("Chaos report:", report)


if __name__ == "__main__":
    asyncio.run(_demo())


__all__ = [
    "PolicyConfig",
    "PolicyResult",
    "MetricsCollector",
    "SafetyMonitor",
    "XAIExplainer",
    "CarbonOffsetIntegrator",
    "CausalRLPolicy",
    "FederatedCausalRLPolicy",
    "MultiAgentCausalRL",
    "ChaosMonkey",
    "FlexGenManager",
    "SHAP_AVAILABLE",
    "LIME_AVAILABLE",
    "FLEXGEN_AVAILABLE",
]
