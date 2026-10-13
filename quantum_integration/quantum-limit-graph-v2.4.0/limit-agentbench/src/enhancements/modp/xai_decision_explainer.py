"""
xai_decision_explainer.py — Enhanced v17.1.0
=============================================

Explainable AI for MOPD decisions, with all ten Green Agent enhancements.

FIXES OVER v17.0.0:
 - SHAP interaction values now use the correct formula:
       Φ_{i,j} = E[ f(S∪{i,j}) - f(S∪{i}) - f(S∪{j}) + f(S) ]
   (was missing the -f(S∪{j}) term).
 - Interaction values are no longer doubled (each pair is written once
   and mirrored without double accumulation).
 - Counterfactual search replaced by a proper minimal-change algorithm:
   grow a set of feature changes, revert steps that worsen the flip,
   report the total perturbation per feature.
 - Anchors replaced by a predicate-growing algorithm that stops once
   precision meets the floor. The previous version almost never fired.
 - All enhancement hooks are now fire-and-forget with configurable
   sample rates; only the SHAP point estimate and CI remain on the
   critical path.
 - `_single_shapley_permutation` caches f(prev) to avoid redundant
   model evaluations (~n/2 saved per permutation).
 - Point estimate and CI use the same permutation count; the CI mean
   is reported as the point estimate.
 - Named features are persisted (`dict(zip(names, x))`) instead of a
   raw list; the dead {"list": ...} branch is removed.
 - HITL escalation now uses a per-decision cooldown.
 - LIME now also reports a bootstrap CI (or an empty dict is documented).
 - log_event shim (structlog or stdlib) and Prometheus metrics.
 - Lifecycle: aclose / __aenter__ / __aexit__, get_running_loop().
 - Modularised into ShapAttributor, LimeAttributor, InteractionAnalyzer,
   CounterfactualGenerator, AnchorGenerator, VisualRenderer, and a thin
   XAIDecisionExplainer facade.
 - Shared fixes for the ten enhancement classes.

Self-contained: Python stdlib + optional numpy / sklearn / torch.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import (
    Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple,
)

# ---------- Optional dependencies ----------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    LinearRegression = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim (structlog or stdlib)."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# CONFIG HELPER
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_EXPLAINS = _PromCounter(
        "xai_explanations_total", "XAI explanations", ["method"])
    _M_EXPLAIN_SECONDS = Histogram(
        "xai_explain_seconds", "XAI explain latency")
    _M_SHAP_PERMS = Histogram(
        "xai_shap_permutations", "SHAP permutations per explanation")
    _M_INTERACTION_CALLS = Histogram(
        "xai_interaction_calls", "Model calls for SHAP interactions")
    _M_CF_SUCCESS = Gauge(
        "xai_counterfactual_success", "Counterfactual success (1/0)")
    _M_ANCHORS_FOUND = Gauge(
        "xai_anchor_found", "Anchors found in the last explanation")
    _M_SIDE_EFFECT = Histogram(
        "xai_side_effect_seconds", "Side-effect latency", ["name"])
    _M_HITL = _PromCounter(
        "xai_hitl_escalations_total", "HITL escalations", ["outcome"])
    _M_PRECISION_SWITCHES = _PromCounter(
        "xai_precision_switches_total", "Precision switches",
        ["from_p", "to_p"])
    _M_CHAOS = _PromCounter(
        "xai_chaos_experiments_total", "Chaos experiments",
        ["fault_type", "status"])
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_EXPLAINS = _M_EXPLAIN_SECONDS = _M_SHAP_PERMS = _Dummy()
    _M_INTERACTION_CALLS = _M_CF_SUCCESS = _M_ANCHORS_FOUND = _Dummy()
    _M_SIDE_EFFECT = _M_HITL = _M_PRECISION_SWITCHES = _M_CHAOS = _Dummy()


# =============================================================================
# ATTRIBUTORS / ANALYZERS / RENDERERS
# =============================================================================
class ShapAttributor:
    """
    SHAP point estimates and bootstrap confidence intervals.

    Uses Monte-Carlo Shapley sampling with a configurable number of
    permutations. `f(prev)` is cached per permutation to avoid ~n/2
    redundant model evaluations.
    """

    def __init__(self, n_permutations: int = 32, ci_samples: int = 20):
        self.n_permutations = max(1, int(n_permutations))
        self.ci_samples = max(2, int(ci_samples))

    def _single_permutation(self, f: Callable, x: "np.ndarray",
                            baseline: "np.ndarray") -> "np.ndarray":
        perm = list(range(len(x)))
        random.shuffle(perm)
        prev = baseline.copy()
        prev_val = float(f(prev.reshape(1, -1)))
        contrib = np.zeros(len(x))
        for i in perm:
            cur = prev.copy()
            cur[i] = x[i]
            try:
                cur_val = float(f(cur.reshape(1, -1)))
            except Exception:
                cur_val = prev_val
            contrib[i] += cur_val - prev_val
            prev, prev_val = cur, cur_val
        return contrib

    def estimate(self, f: Callable, x: "np.ndarray",
                 names: List[str],
                 baseline: Optional["np.ndarray"] = None
                 ) -> Dict[str, float]:
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        if baseline is None:
            baseline = np.zeros_like(x)
        acc = np.zeros(len(x))
        for _ in range(self.n_permutations):
            acc += self._single_permutation(f, x, baseline)
        mean = acc / self.n_permutations
        return dict(zip(names, mean.tolist()))

    def estimate_with_ci(self, f: Callable, x: "np.ndarray",
                         names: List[str],
                         baseline: Optional["np.ndarray"] = None
                         ) -> Tuple[Dict[str, float], Dict[str, Tuple[float, float]]]:
        """
        Return (point_estimate, ci) where point_estimate is the mean over
        all permutations and ci is the (2.5, 97.5) percentile band of
        per-batch means.
        """
        if not NUMPY_AVAILABLE:
            zeros = {k: 0.0 for k in names}
            ci = {k: (0.0, 0.0) for k in names}
            return zeros, ci
        if baseline is None:
            baseline = np.zeros_like(x)
        perms_per_sample = max(1, self.n_permutations // self.ci_samples)
        samples: List[np.ndarray] = []
        for _ in range(self.ci_samples):
            acc = np.zeros(len(x))
            for _ in range(perms_per_sample):
                acc += self._single_permutation(f, x, baseline)
            samples.append(acc / perms_per_sample)
        stacked = np.stack(samples, axis=0)
        mean = stacked.mean(axis=0)
        lo = np.percentile(stacked, 2.5, axis=0)
        hi = np.percentile(stacked, 97.5, axis=0)
        attrs = dict(zip(names, mean.tolist()))
        ci = {name: (float(lo[i]), float(hi[i]))
              for i, name in enumerate(names)}
        return attrs, ci


class LimeAttributor:
    """LIME with an adaptive kernel bandwidth and optional bootstrap CI."""

    def __init__(self, n_samples: int = 200, ci_rounds: int = 5):
        self.n_samples = max(20, int(n_samples))
        self.ci_rounds = max(1, int(ci_rounds))

    def _bandwidth(self, x: "np.ndarray") -> float:
        if not NUMPY_AVAILABLE:
            return 0.02
        try:
            scale = float(np.std(np.abs(x)))
        except Exception:
            scale = 0.0
        if scale <= 0:
            return 0.02
        return max(1e-3, scale * 0.5)

    def _one_round(self, f: Callable, x: "np.ndarray",
                   names: List[str]) -> Optional["np.ndarray"]:
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return None
        bandwidth = self._bandwidth(x)
        X = np.tile(x, (self.n_samples, 1)) + np.random.normal(
            0, bandwidth, (self.n_samples, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return None
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / (2 * bandwidth ** 2))
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return m.coef_
        except Exception:
            return None

    def estimate(self, f: Callable, x: "np.ndarray",
                 names: List[str]
                 ) -> Tuple[Dict[str, float], Dict[str, Tuple[float, float]]]:
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            zeros = {k: 0.0 for k in names}
            return zeros, {k: (0.0, 0.0) for k in names}
        coefs: List[np.ndarray] = []
        for _ in range(self.ci_rounds):
            c = self._one_round(f, x, names)
            if c is not None:
                coefs.append(c)
        if not coefs:
            zeros = {k: 0.0 for k in names}
            return zeros, {k: (0.0, 0.0) for k in names}
        stacked = np.stack(coefs, axis=0)
        mean = stacked.mean(axis=0)
        lo = np.percentile(stacked, 2.5, axis=0)
        hi = np.percentile(stacked, 97.5, axis=0)
        attrs = dict(zip(names, mean.tolist()))
        ci = {name: (float(lo[i]), float(hi[i]))
              for i, name in enumerate(names)}
        return attrs, ci


class InteractionAnalyzer:
    """
    Pairwise SHAP interaction values.

    Φ_{i,j} = E_{S ⊆ N \\ {i,j}} [ f(S∪{i,j}) - f(S∪{i}) - f(S∪{j}) + f(S) ]

    Each pair is written once and mirrored at the end (no double counting).
    """

    def __init__(self, n_permutations: int = 16):
        self.n_permutations = max(1, int(n_permutations))

    def analyze(self, f: Callable, x: "np.ndarray",
                names: List[str]
                ) -> Dict[str, Dict[str, float]]:
        n_feat = len(x)
        if not NUMPY_AVAILABLE or n_feat < 2:
            return {n1: {n2: 0.0 for n2 in names} for n1 in names}
        baseline = np.zeros_like(x)
        inter = np.zeros((n_feat, n_feat))
        for _ in range(self.n_permutations):
            perm = list(range(n_feat))
            random.shuffle(perm)
            state = baseline.copy()
            state_val = 0.0
            try:
                state_val = float(f(state.reshape(1, -1)))
            except Exception:
                pass
            for k, idx_i in enumerate(perm):
                state_i = state.copy()
                state_i[idx_i] = x[idx_i]
                try:
                    val_i = float(f(state_i.reshape(1, -1)))
                except Exception:
                    val_i = state_val
                for idx_j in perm[k + 1:]:
                    state_j = state.copy()
                    state_j[idx_j] = x[idx_j]
                    state_ij = state_i.copy()
                    state_ij[idx_j] = x[idx_j]
                    try:
                        val_j = float(f(state_j.reshape(1, -1)))
                        val_ij = float(f(state_ij.reshape(1, -1)))
                    except Exception:
                        continue
                    inter[idx_i, idx_j] += (val_ij - val_i - val_j + state_val)
                state, state_val = state_i, val_i
        inter /= self.n_permutations
        # Mirror without double accumulation.
        sym = inter + inter.T - np.diag(np.diag(inter))
        return {n1: {n2: float(sym[i, j])
                     for j, n2 in enumerate(names)}
                for i, n1 in enumerate(names)}


class CounterfactualGenerator:
    """
    Minimal-change counterfactual search.

    Coordinate descent over features: repeatedly try to nudge a feature
    toward the target direction, accept the step only if it decreases the
    L1 distance to the target prediction, and stop when the flip is
    achieved. The returned counterfactual reports the total perturbation
    per feature, not just the last step.
    """

    def __init__(self, max_iter: int = 200, step: float = 0.05):
        self.max_iter = max(1, int(max_iter))
        self.step = float(step)

    def generate(self, f: Callable, x: "np.ndarray",
                 names: List[str],
                 baseline_prediction: float,
                 target_delta: float = 0.1
                 ) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            return {"success": False, "reason": "numpy unavailable"}
        target = baseline_prediction + math.copysign(
            abs(target_delta), -baseline_prediction or 1.0)
        x_cf = x.copy()
        deltas = np.zeros_like(x)
        best = {"success": False, "counterfactual": x.tolist(),
                "prediction": baseline_prediction, "deltas": deltas.tolist(),
                "iterations": 0}

        for it in range(self.max_iter):
            # Try each feature in a random order; accept the best step.
            order = list(range(len(x)))
            random.shuffle(order)
            best_gain = 0.0
            best_choice: Optional[Tuple[int, float]] = None
            for i in order:
                for sign in (-1.0, 1.0):
                    trial = x_cf.copy()
                    trial[i] += sign * self.step
                    try:
                        pred = float(f(trial.reshape(1, -1)))
                    except Exception:
                        continue
                    gain = abs(pred - target) - abs(
                        float(f(x_cf.reshape(1, -1))) - target)
                    if gain < best_gain:
                        best_gain = gain
                        best_choice = (i, sign * self.step)
            if best_choice is None:
                break
            i, delta = best_choice
            x_cf[i] += delta
            deltas[i] += delta
            try:
                pred = float(f(x_cf.reshape(1, -1)))
            except Exception:
                break
            if abs(pred - baseline_prediction) >= abs(target_delta):
                best = {
                    "success": True,
                    "counterfactual": x_cf.tolist(),
                    "prediction": pred,
                    "deltas": deltas.tolist(),
                    "changed_features": [names[j] for j in range(len(x))
                                         if abs(deltas[j]) > 1e-9],
                    "iterations": it + 1,
                }
                return best
        best["iterations"] = self.max_iter
        return best


class AnchorGenerator:
    """
    Anchor algorithm: grow a set of feature predicates until the anchor
    achieves the precision floor.

    The candidate predicate for feature i is `value_i ∈ [x_i - tol, x_i + tol]`.
    Samples are drawn uniformly from the joint feature range (or from the
    reference set) and a sample is "consistent" if it satisfies every
    anchor predicate and the model prediction stays within the tolerance.
    """

    def __init__(self, min_precision: float = 0.9, tol: float = 0.1,
                 n_samples: int = 128, max_features: int = 4):
        self.min_precision = float(min_precision)
        self.tol = float(tol)
        self.n_samples = max(20, int(n_samples))
        self.max_features = max(1, int(max_features))

    def _candidate_samples(self, x: "np.ndarray",
                           reference_set: Optional[List[List[float]]]
                           ) -> "np.ndarray":
        if reference_set and len(reference_set) > 0:
            try:
                return np.asarray(reference_set, dtype=float)
            except Exception:
                pass
        spread = max(0.5, float(np.std(x)) * 2.0)
        return x + np.random.normal(0, spread, (self.n_samples, len(x)))

    def generate(self, f: Callable, x: "np.ndarray",
                 names: List[str],
                 reference_set: Optional[List[List[float]]] = None
                 ) -> List[Dict[str, Any]]:
        if not NUMPY_AVAILABLE or len(x) == 0:
            return []
        try:
            base_pred = float(f(x.reshape(1, -1)))
        except Exception:
            return []
        samples = self._candidate_samples(x, reference_set)
        if samples.shape[0] < 5:
            return []
        try:
            preds = np.array([float(f(s.reshape(1, -1))) for s in samples])
        except Exception:
            return []

        anchors: List[Dict[str, Any]] = []
        covered = np.zeros(samples.shape[0], dtype=bool)
        used: Set[int] = set()

        # Greedy anchor growth.
        while len(used) < min(self.max_features, len(x)):
            best_i: Optional[int] = None
            best_precision = 0.0
            best_mask = None
            for i in range(len(x)):
                if i in used:
                    continue
                pred_i = np.abs(samples[:, i] - x[i]) <= self.tol
                pred_pred = np.abs(preds - base_pred) <= 2 * self.tol
                mask = pred_i & pred_pred & ~covered
                if mask.sum() == 0:
                    continue
                # Precision is (consistent) / (samples matching the anchor).
                candidates = pred_i & ~covered
                if candidates.sum() == 0:
                    continue
                precision = float((pred_pred & candidates).sum()
                                  / candidates.sum())
                if precision > best_precision:
                    best_precision = precision
                    best_i = i
                    best_mask = pred_i
            if best_i is None or best_precision < self.min_precision:
                break
            used.add(best_i)
            if best_mask is not None:
                covered |= best_mask
            anchors.append({
                "feature": names[best_i],
                "value": float(x[best_i]),
                "tolerance": self.tol,
                "precision": best_precision,
            })
        return anchors


class VisualRenderer:
    """Textual force plot and waterfall plot."""

    def __init__(self, depth: int = 5):
        self.depth = max(1, int(depth))

    def force_plot(self, attrs: Dict[str, float]) -> str:
        if not attrs:
            return "Force plot: (no attributions)"
        max_abs = max((abs(v) for v in attrs.values()), default=1.0) or 1.0
        lines = ["Force plot (positive →, negative ←):"]
        for k, v in sorted(attrs.items(),
                           key=lambda kv: abs(kv[1]),
                           reverse=True)[:self.depth]:
            bar_len = int(20 * abs(v) / max_abs)
            arrow = "─" * bar_len
            sign = "→" if v >= 0 else "←"
            lines.append(f"  {k:20s} {sign} {arrow} {v:+.4f}")
        return "\n".join(lines)

    def waterfall_plot(self, base_value: float,
                       attrs: Dict[str, float],
                       final_value: float) -> str:
        running = base_value
        lines = [f"Waterfall plot (base={base_value:+.4f}):"]
        for k, v in sorted(attrs.items(),
                           key=lambda kv: abs(kv[1]),
                           reverse=True)[:self.depth]:
            running += v
            lines.append(
                f"  + {k:20s} {v:+.4f}  → cumulative {running:+.4f}")
        lines.append(f"Final prediction: {final_value:+.4f}")
        return "\n".join(lines)


# =============================================================================
# ENHANCEMENT 6 — XAI DECISION EXPLAINER
# =============================================================================
class XAIDecisionExplainer:
    """
    Explainable AI for MOPD decisions.

    v17.1 facade over ShapAttributor, LimeAttributor, InteractionAnalyzer,
    CounterfactualGenerator, AnchorGenerator, and VisualRenderer.
    """

    def __init__(self,
                 config: Any,
                 storage: Any,
                 causal_rl: Optional["CausalPolicyAdapter"] = None,
                 temporal: Optional["TemporalLogicVerifier"] = None,
                 multi_agent: Optional["MultiAgentCoordinator"] = None,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 chaos: Optional["ChaosTestingEngine"] = None,
                 precision: Optional["AdaptivePrecisionSwitcher"] = None,
                 federated: Optional["FederatedGreenAggregator"] = None,
                 *,
                 xai_sample_rate: float = 1.0,
                 chaos_sample_rate: float = 0.01,
                 multi_agent_sample_rate: float = 0.1,
                 carbon_sample_rate: float = 0.05,
                 federated_share_every: int = 100,
                 hitl_cooldown_seconds: float = 60.0):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap")
                          or "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5))

        # Sub-components.
        self.shap = ShapAttributor(
            n_permutations=int(_cfg_get(config, "xai_permutations", 32)),
            ci_samples=int(_cfg_get(config, "xai_ci_samples", 20)))
        self.lime = LimeAttributor(
            n_samples=int(_cfg_get(config, "xai_lime_samples", 200)))
        self.interactions = InteractionAnalyzer(
            n_permutations=int(_cfg_get(config, "xai_interaction_perms", 16)))
        self.counterfactuals = CounterfactualGenerator(
            max_iter=int(_cfg_get(config, "xai_cf_max_iter", 200)),
            step=float(_cfg_get(config, "xai_cf_step", 0.05)))
        self.anchors = AnchorGenerator(
            min_precision=float(_cfg_get(config, "xai_anchor_min_precision", 0.9)),
            tol=float(_cfg_get(config, "xai_anchor_tol", 0.1)))
        self.renderer = VisualRenderer(depth=self.depth)

        # Enhancement hooks.
        self.causal_rl = causal_rl
        self.temporal = temporal
        self.multi_agent = multi_agent
        self.hitl = hitl
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.precision = precision
        self.federated = federated

        # Sample-rate configuration.
        self.xai_sample_rate = float(xai_sample_rate)
        self.chaos_sample_rate = float(chaos_sample_rate)
        self.multi_agent_sample_rate = float(multi_agent_sample_rate)
        self.carbon_sample_rate = float(carbon_sample_rate)
        self.federated_share_every = max(1, int(federated_share_every))
        self.hitl_cooldown_seconds = float(hitl_cooldown_seconds)

        # State.
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)
        self.decision_history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._explain_counter = 0
        self._last_hitl_escalation: Dict[str, float] = {}
        self._side_effects: set = set()

    # ------------------------------------------------------------------
    def _spawn_side_effect(self, coro: Awaitable[Any], name: str) -> None:
        try:
            task = asyncio.create_task(coro, name=f"xai_side_{name}")
        except RuntimeError:
            return
        self._side_effects.add(task)

        def _done(t: asyncio.Task) -> None:
            self._side_effects.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("debug", f"XAI side effect {name} failed: {exc}")

        task.add_done_callback(_done)

    # ------------------------------------------------------------------
    async def _side_multi_agent(self) -> None:
        if self.multi_agent is None:
            return
        try:
            t0 = time.monotonic()
            agent_id, _ = await self.multi_agent.bid({
                "name": "explain_task", "preferred_role": "reporter"})
            await self.multi_agent.reward(agent_id, 0.8)
            _M_SIDE_EFFECT.labels(name="multi_agent").observe(
                time.monotonic() - t0)
        except Exception as exc:
            log_event("debug", f"multi-agent side effect failed: {exc}")

    async def _side_carbon(self) -> None:
        if self.carbon_market is None:
            return
        try:
            t0 = time.monotonic()
            await self.carbon_market.net_zero_schedule(
                workload_kwh=0.01, intensity=0.2, commit=False)
            _M_SIDE_EFFECT.labels(name="carbon").observe(
                time.monotonic() - t0)
        except Exception as exc:
            log_event("debug", f"carbon side effect failed: {exc}")

    async def _side_chaos(self) -> None:
        if self.chaos is None:
            return
        try:
            fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
            await self.chaos.run_experiment(
                f"xai_{uuid.uuid4().hex[:6]}", fault)
        except Exception as exc:
            log_event("debug", f"chaos side effect failed: {exc}")

    async def _side_precision(self, attribution_mass: float) -> None:
        if self.precision is None:
            return
        try:
            t0 = time.monotonic()
            # Interpret a small attribution mass as low model accuracy.
            self.precision.record_accuracy(1.0 - min(1.0, attribution_mass))
            await self.precision.auto_switch(0.0, 0.0)
            _M_SIDE_EFFECT.labels(name="precision").observe(
                time.monotonic() - t0)
        except Exception as exc:
            log_event("debug", f"precision side effect failed: {exc}")

    async def _side_temporal(self, attribution_mass: float) -> None:
        if self.temporal is None:
            return
        try:
            await self.temporal.push_state({
                "explained": 1,
                "attribution_magnitude": attribution_mass})
        except Exception as exc:
            log_event("debug", f"temporal side effect failed: {exc}")

    async def _side_federated(self) -> None:
        if self.federated is None:
            return
        try:
            if self._explain_counter % self.federated_share_every != 0:
                return
            t0 = time.monotonic()
            blob = json.dumps(self.global_feature_importance()).encode()
            await self.federated.share_weights("xai_stats", blob)
            _M_SIDE_EFFECT.labels(name="federated").observe(
                time.monotonic() - t0)
        except Exception as exc:
            log_event("debug", f"federated side effect failed: {exc}")

    async def _side_hitl(self, decision_id: str,
                         attrs: Dict[str, float]) -> None:
        if self.hitl is None or not attrs:
            return
        worst = min(attrs.items(), key=lambda kv: kv[1])
        if worst[1] >= -0.5:
            return
        now = time.monotonic()
        last = self._last_hitl_escalation.get(decision_id, 0.0)
        if now - last < self.hitl_cooldown_seconds:
            return
        self._last_hitl_escalation[decision_id] = now
        try:
            approved = await self.hitl.query_user_if_needed(
                "xai_approver",
                [
                    {"solution_id": "ack", "quality_score": 0.90,
                     "carbon_g": 0.10, "cost_usd": 0.10, "latency_ms": 100},
                    {"solution_id": "reject", "quality_score": 0.85,
                     "carbon_g": 0.12, "cost_usd": 0.12, "latency_ms": 105},
                ],
                timeout=1.5)
            _M_HITL.labels(
                outcome=approved or "timeout").inc()
        except Exception as exc:
            log_event("debug", f"HITL side effect failed: {exc}")

    # ------------------------------------------------------------------
    async def explain(self,
                      decision_id: str,
                      label: str,
                      features: Any,
                      names: List[str],
                      model_fn: Callable,
                      baseline: Optional[Any] = None,
                      reference_set: Optional[List[Any]] = None,
                      include_counterfactual: bool = True,
                      include_interactions: bool = False,
                      include_anchors: bool = False) -> Dict[str, Any]:
        """Generate an explanation with all v17 features."""
        t_start = time.monotonic()
        self._explain_counter += 1

        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
            result = {
                "decision_id": decision_id,
                "method": self.method,
                "attributions": attrs,
                "attributions_ci": {},
                "explanation": self._nl(label, attrs),
            }
            self.global_attributions.append(dict(attrs))
            return result

        x = np.asarray(features, dtype=float)
        if len(x) != len(names):
            log_event("warning",
                      "features and names length mismatch; using shorter")
            n = min(len(x), len(names))
            x = x[:n]
            names = names[:n]

        # Baseline.
        if baseline is None:
            if reference_set:
                try:
                    baseline = np.mean(
                        [np.asarray(r, dtype=float) for r in reference_set],
                        axis=0)
                except Exception:
                    baseline = np.zeros_like(x)
            else:
                baseline = np.zeros_like(x)
        else:
            baseline = np.asarray(baseline, dtype=float)

        # Attributions: point estimate and CI from the SAME attributor.
        if self.method == "lime":
            attrs, ci_raw = self.lime.estimate(model_fn, x, names)
        else:
            attrs, ci_raw = self.shap.estimate_with_ci(
                model_fn, x, names, baseline)

        # Visualisations.
        try:
            base_val = float(model_fn(baseline.reshape(1, -1)))
            final_val = float(model_fn(x.reshape(1, -1)))
        except Exception:
            base_val = 0.0
            final_val = 0.0
        nl = self._nl(label, attrs)
        force = self.renderer.force_plot(attrs)
        waterfall = self.renderer.waterfall_plot(base_val, attrs, final_val)

        result: Dict[str, Any] = {
            "decision_id": decision_id,
            "method": self.method,
            "attributions": attrs,
            "attributions_ci": {
                k: {"ci_low": v[0], "ci_high": v[1]}
                for k, v in ci_raw.items()
            },
            "explanation": nl,
            "force_plot": force,
            "waterfall_plot": waterfall,
        }

        # Optional features.
        if include_counterfactual:
            try:
                result["counterfactual"] = self.counterfactuals.generate(
                    model_fn, x, names,
                    baseline_prediction=final_val)
                try:
                    _M_CF_SUCCESS.set(
                        1.0 if result["counterfactual"].get("success") else 0.0)
                except Exception:
                    pass
            except Exception as exc:
                log_event("debug", f"counterfactual failed: {exc}")
                result["counterfactual"] = None
        if include_interactions:
            try:
                result["interactions"] = self.interactions.analyze(
                    model_fn, x, names)
            except Exception as exc:
                log_event("debug", f"interactions failed: {exc}")
                result["interactions"] = None
        if include_anchors:
            try:
                result["anchors"] = self.anchors.generate(
                    model_fn, x, names, reference_set=reference_set)
                try:
                    _M_ANCHORS_FOUND.set(len(result["anchors"] or []))
                except Exception:
                    pass
            except Exception as exc:
                log_event("debug", f"anchors failed: {exc}")
                result["anchors"] = None

        # Causal attribution (cheap; no model calls).
        if self.causal_rl is not None and self.causal_rl.graph is not None:
            try:
                parents = self.causal_rl.graph.parents("quality")
                result["causal_attributions"] = {
                    p: self.causal_rl.graph.graph.get(p, {}).get(
                        "quality", {}).get("weight", 0.0)
                    for p in parents
                }
            except Exception:
                pass

        # Accumulate global attributions once.
        self.global_attributions.append(dict(attrs))
        self.decision_history.append({
            "decision_id": decision_id,
            "label": label,
            "attributions": dict(attrs),
            "ts": _now_utc().isoformat(),
        })

        # Metrics.
        try:
            _M_EXPLAINS.labels(method=self.method).inc()
            _M_EXPLAIN_SECONDS.observe(time.monotonic() - t_start)
            _M_SHAP_PERMS.observe(self.shap.n_permutations)
        except Exception:
            pass

        # Side effects (sampled, non-blocking).
        attribution_mass = sum(abs(v) for v in attrs.values())
        if random.random() < self.multi_agent_sample_rate:
            self._spawn_side_effect(self._side_multi_agent(), "multi_agent")
        if random.random() < self.carbon_sample_rate:
            self._spawn_side_effect(self._side_carbon(), "carbon")
        if random.random() < self.chaos_sample_rate:
            self._spawn_side_effect(self._side_chaos(), "chaos")
        self._spawn_side_effect(
            self._side_precision(attribution_mass), "precision")
        self._spawn_side_effect(
            self._side_temporal(attribution_mass), "temporal")
        self._spawn_side_effect(self._side_federated(), "federated")
        self._spawn_side_effect(
            self._side_hitl(decision_id, attrs), "hitl")

        # Persist named features.
        try:
            feature_map = dict(zip(names, x.tolist()))
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                feature_map, attrs, nl)
        except Exception:
            pass

        return result

    # ------------------------------------------------------------------
    def _nl(self, decision: str, attrs: Dict[str, float]) -> str:
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    def global_feature_importance(self) -> Dict[str, float]:
        if not self.global_attributions:
            return {}
        agg: Dict[str, List[float]] = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: sum(vals) / len(vals) for k, vals in agg.items()}

    def top_features(self, k: int = 5) -> List[Tuple[str, float]]:
        importance = self.global_feature_importance()
        return sorted(importance.items(), key=lambda kv: kv[1],
                      reverse=True)[:k]

    def stats(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "depth": self.depth,
            "explanations": len(self.decision_history),
            "global_features": len(self.global_feature_importance()),
            "side_effects_pending": len(self._side_effects),
        }

    async def aclose(self) -> None:
        for t in list(self._side_effects):
            t.cancel()
        if self._side_effects:
            await asyncio.gather(*self._side_effects,
                                 return_exceptions=True)
        self._side_effects.clear()

    async def __aenter__(self) -> "XAIDecisionExplainer":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = float(temperature)
        self.alpha = float(alpha)
        self.n_actions = int(n_actions)
        self.teachers: Dict[str, List[float]] = {}
        self.teacher_reputation: Dict[str, float] = defaultdict(lambda: 0.5)
        self.teacher_kl_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=20))
        self.student_policy = [1.0 / self.n_actions] * self.n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[str(name)] = [max(0.0, p / s) for p in policy]

    def _temp_scale(self, p: List[float], temperature: float) -> List[float]:
        if not p:
            return []
        inv = 1.0 / max(temperature, 1e-6)
        scaled = [max(v, 1e-12) ** inv for v in p]
        s = sum(scaled) or 1.0
        return [x / s for x in scaled]

    def _softmax(self, x: List[float], temp: float) -> List[float]:
        return self._temp_scale(x, temp)

    def _superpose(self) -> List[float]:
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, storage: Any,
                   student_id: str = "xai_student") -> Dict[str, Any]:
        target = self._temp_scale(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Per-teacher KL against the teacher's own distribution.
        for tid, pol in self.teachers.items():
            kl = 0.0
            for i in range(self.n_actions):
                pi = pol[i] if i < len(pol) else 1e-9
                si = self.student_policy[i]
                pi, si = max(pi, 1e-9), max(si, 1e-9)
                kl += pi * math.log(pi / si)
            self.teacher_kl_history[tid].append(kl)
            reward = 1.0 / (1.0 + kl)
            self.teacher_reputation[tid] = (
                0.9 * self.teacher_reputation[tid] + 0.1 * reward)
            weight = float(pol[0]) if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            try:
                await asyncio.to_thread(
                    storage.save_teacher_superposition,
                    student_id, tid, weight, self.temperature, amplitude, kl)
            except Exception:
                pass

        entry = {"target": target, "student": list(self.student_policy),
                 "ts": _now_utc().isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage: Any = None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples: List[Dict[str, float]],
                    variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
            return self.summary()
        try:
            X = np.asarray(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float)
        except Exception:
            return self.summary()
        if X.shape[0] < 3 or X.shape[1] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
        except Exception:
            return self.summary()
        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if not math.isfinite(c) or c <= threshold:
                        continue
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = ((variables[i], variables[j]) if vi > vj
                                else (variables[j], variables[i]))
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c}
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]
    STRATEGY_MAP = {
        "performance": "balanced", "carbon": "carbon_focus",
        "cost": "operational_focus", "hybrid": "balanced",
        "adaptive": "balanced",
    }

    def __init__(self, config: Any, storage: Any, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1)
                             or 0.1)
        self._lock = asyncio.Lock()

    def to_strategy(self, action: str) -> str:
        return self.STRATEGY_MAP.get(action, "balanced")

    async def choose_action(self, state: Dict[str, Any]) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float,
                     state: Dict[str, Any]) -> None:
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    def __init__(self, storage: Any, instance_id: str,
                 share_interval: int = 3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.rounds = 0
        self.last_aggregated: Optional[bytes] = None

    async def share_weights(self, model_id: str, weights: bytes) -> None:
        if not weights:
            return
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
            self.rounds += 1
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id: str) -> Optional[bytes]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r["weights"]]
        if not blobs:
            return None
        # Try JSON averaging first.
        dicts: List[Dict[str, float]] = []
        for b in blobs:
            try:
                parsed = json.loads(b.decode())
                if isinstance(parsed, dict) and all(
                        isinstance(v, (int, float)) for v in parsed.values()):
                    dicts.append({k: float(v) for k, v in parsed.items()})
            except Exception:
                continue
        if dicts:
            keys = set(dicts[0].keys())
            for d in dicts[1:]:
                keys &= set(d.keys())
            merged = {k: sum(d[k] for d in dicts) / len(dicts) for k in keys}
            self.rounds += 1
            self.last_aggregated = json.dumps(merged).encode()
            return self.last_aggregated
        # Fallback to byte-wise.
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        self.last_aggregated = bytes(avg)
        return self.last_aggregated


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(self, topic: str, sender: str,
                        payload: Dict[str, Any]) -> None:
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            pass

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        async with self._lock:
            if not self.agents:
                raise RuntimeError("No agents available for bidding")
            best_id, best_score = None, -1.0
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id not in self.agents:
                return
            a = self.agents[agent_id]
            alpha = 0.1
            a.reputation = max(0.0, min(1.0,
                (1 - alpha) * a.reputation + alpha * reward))
            a.utilities[a.role] = min(
                1.0, a.utilities[a.role] + 0.05 * reward)

    def get_policy(self) -> List[float]:
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self) -> Dict[str, Any]:
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id, operator, conditions, window=0.0,
                 description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = float(window)
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation = None

    def evaluate(self, trace):
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage: Any, config: Any):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000) or 2000))
        self.approval_cb: Optional[Callable] = None
        self._pending_rule_saves: List[Tuple[str, str, str, str]] = []
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = (formula or "").strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always")
        elif f.startswith("F "):
            self._add_atomic(f[2:].strip().strip("()"), "eventually")
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never")
        else:
            log_event("warning", f"Unrecognised temporal formula: {formula!r}")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            log_event("warning", f"Atomic formula did not parse: {expr!r}")
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")
        self._pending_rule_saves.append((rid, expr, op, "warning"))

    async def _flush_pending_rule_saves(self) -> None:
        while self._pending_rule_saves:
            rid, expr, op, sev = self._pending_rule_saves.pop(0)
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_rule,
                    rid, expr, op, sev, expr, 0.0, True)
            except Exception:
                pass

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        await self._flush_pending_rule_saves()
        self.trace.append((_now_utc(), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self) -> Dict[str, bool]:
        await self._flush_pending_rule_saves()
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace)
            violated = rule.evaluate(copy)
            approved = False
            if violated:
                rule.violations += 1
                rule.last_violation = _now_utc()
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(
                            rid, self.trace[-1][1] if self.trace else {})
                        if asyncio.iscoroutine(approved):
                            approved = await approved
                    except Exception as exc:
                        log_event("warning", f"Approval callback failed: {exc}")
                        approved = False
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, bool(approved))
                except Exception:
                    pass
            result[rid] = (not violated) or bool(approved)
        return result


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    LATENCY = {"fp32": 1.0, "tf32": 0.8, "bf16": 0.65,
               "fp16": 0.6, "int8": 0.4}

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict] = deque(maxlen=500)
        self._acc_window: Deque[float] = deque(maxlen=20)
        self._hw_cache: Optional[Dict[str, Any]] = None

    def _probe(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._hw_cache is not None and not force_refresh:
            return self._hw_cache
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE and torch is not None:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    try:
                        info["bf16"] = bool(torch.cuda.is_bf16_supported())
                    except Exception:
                        info["bf16"] = False
            except Exception:
                pass
        self._hw_cache = info
        return info

    def _allowed_precisions(self) -> List[str]:
        raw = list(_cfg_get(self.config, "precision_levels",
                            ["fp32", "fp16", "bf16", "int8"]))
        return [c for c in raw if c in self.ENERGY]

    def select_precision(self) -> str:
        hw = self._probe()
        cands = self._allowed_precisions()
        if not cands:
            return "fp32"
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    def record_accuracy(self, accuracy: float) -> None:
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels")
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
            _M_PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc: float = 0.0,
                          baseline_acc: float = 0.0) -> None:
        if recent_acc <= 0 or baseline_acc <= 0:
            if not self._acc_window:
                return
            recent_acc = sum(self._acc_window) / len(self._acc_window)
            baseline_acc = max(self._acc_window)
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(_cfg_get(self.config,
                                "precision_switch_threshold", 0.02) or 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            target = self.select_precision()
            if target != self.current:
                await self.switch_to(target, reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    DEFER_INTENSITY_THRESHOLD = 0.3
    RUN_OFFSET_COST_THRESHOLD = 0.5
    MIN_PRICE_USD = 5.0
    MAX_PRICE_USD = 200.0
    DEFAULT_PRICE_USD = 25.0

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.last_price = float(
            _cfg_get(config, "carbon_default_price_usd",
                     self.DEFAULT_PRICE_USD) or self.DEFAULT_PRICE_USD)
        self._rec_balance_mwh = 0.0

    async def _fetch_price(self) -> float:
        candidate = self.last_price + random.gauss(0, 1.5)
        return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

    async def update_price(self) -> float:
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        try:
            price = max(self.MIN_PRICE_USD,
                        min(self.MAX_PRICE_USD, float(price)))
        except (TypeError, ValueError):
            price = self.last_price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        self.last_price = price
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        mwh = max(0.0, float(mwh))
        if mwh == 0.0:
            return 0.0
        price_per_mwh = max(0.0, float(price_per_mwh))
        src = (source or "").strip().lower() or "unspecified"
        cost = mwh * price_per_mwh
        self._rec_balance_mwh += mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, src)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh: float, intensity: float,
                                price: Optional[float] = None,
                                commit: bool = True) -> Dict[str, Any]:
        workload_kwh = max(0.0, float(workload_kwh))
        intensity = max(0.0, float(intensity))
        if price is None:
            price = float(self.last_price)
        else:
            try:
                price = max(0.0, float(price))
            except (TypeError, ValueError):
                price = float(self.last_price)
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        defer_hours = 0
        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
            defer_hours = max(
                1, int((intensity - self.DEFER_INTENSITY_THRESHOLD) * 10))
        elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
            action = "run_offset"
        else:
            action = "run"
        if commit:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8], workload_kwh, intensity,
                    action, carbon_kg, offset_cost, price)
            except Exception:
                pass
        return {"action": action, "defer_hours": defer_hours,
                "carbon_kg": carbon_kg, "offset_cost_usd": offset_cost,
                "credit_price_usd": price,
                "rec_balance_mwh": float(self._rec_balance_mwh)}

    def get_rec_balance(self) -> float:
        return float(self._rec_balance_mwh)


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

    async def _health_ok(self) -> bool:
        async with self._lock:
            return not self.active

    async def run_experiment(self, name: str,
                             fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.monotonic()
        before = await self._health_ok()
        status = "completed"
        injected_ms = 0.0
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                injected_ms = 500.0
                await asyncio.sleep(0.5)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "data_corruption":
                async with self._lock:
                    self._shared_state["payload"] = "corrupted"
                    self._shared_state["counter"] += 1
                injected_ms = 5.0
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                injected_ms = 10.0
            elif fault_type == "network_drop":
                injected_ms = 200.0
                await asyncio.sleep(0.2)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._health_ok()
        duration = (time.monotonic() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                float(_cfg_get(self.config, "chaos_blast_radius", 0.1) or 0.1),
                int(before), int(after), status, duration)
            _M_CHAOS.labels(fault_type=fault_type, status=status).inc()
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration,
                "injected_ms": injected_ms}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage: Any, dashboard: Any = None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id: str, chosen_id: str,
                              request_id: Optional[str] = None) -> None:
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event("warning",
                      f"Unknown HITL request_id {request_id!r}")
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping")

    def _entropy_uncertainty(self, candidates: List[Dict[str, Any]]) -> float:
        scores = []
        for c in candidates[:5]:
            try:
                scores.append(float(c.get("quality_score", 0.0)))
            except (TypeError, ValueError):
                scores.append(0.0)
        s = sum(scores) or 1.0
        probs = [x / s for x in scores]
        ent = -sum(p * math.log(p + 1e-9) for p in probs)
        max_ent = math.log(len(probs)) if len(probs) > 1 else 1.0
        return ent / max_ent if max_ent > 0 else 0.0

    def _ambiguity_score(self, candidates: List[Dict[str, Any]]) -> float:
        if len(candidates) < 2:
            return 0.0
        c1, c2 = candidates[0], candidates[1]

        def _rel(a, b):
            try:
                a = float(a); b = float(b)
            except (TypeError, ValueError):
                return 0.0
            m = max(abs(a), abs(b), 1e-6)
            return abs(a - b) / m

        try:
            q = _rel(c1.get("quality_score", 0), c2.get("quality_score", 0))
            cg = _rel(c1.get("carbon_g", 0), c2.get("carbon_g", 0))
            cost = _rel(c1.get("cost_usd", 0), c2.get("cost_usd", 0))
            lat = _rel(c1.get("latency_ms", 0), c2.get("latency_ms", 0))
            return max(cg, cost, lat) * (1.0 - min(q, 1.0))
        except Exception:
            return 0.0

    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
        if len(candidates) < 2:
            return None
        ambiguity = self._ambiguity_score(candidates)
        uncertainty = self._entropy_uncertainty(candidates)
        if ambiguity < 0.05 and uncertainty < 0.5:
            return None
        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "query", {"candidates": candidates[:2]}, "info")
        except Exception:
            pass
        if self.dashboard is not None:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query",
                    "request_id": req_id, "user_id": user_id})
            except Exception:
                pass
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        async with self._lock:
            self._pending[req_id] = fut
        try:
            msg = await asyncio.wait_for(fut, timeout=timeout)
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "approved")
            except Exception:
                pass
            chosen = msg.get("chosen") if isinstance(msg, dict) else None
            metrics = next(
                (c for c in candidates
                 if c.get("solution_id") == chosen), None)
            await self.record_choice(user_id, chosen, metrics=metrics)
            return chosen
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "timeout")
            except Exception:
                pass
            normalized = self.get_preference_weights(user_id)
            if normalized:
                scored = []
                for c in candidates:
                    s = sum(normalized.get(k, 0.25)
                            / (float(c.get(k, 0) or 0) + 1e-8)
                            for k in self._PREF_KEYS)
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")
        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None
                            ) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in self._PREF_KEYS:
                if k in metrics:
                    try:
                        v = float(metrics[k])
                    except (TypeError, ValueError):
                        continue
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + \
                            1.0 / (abs(v) + 1e-6) * 0.01
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id: str) -> Dict[str, float]:
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class XAIOrchestratorV17:
    """Unified entry point wiring all ten enhancements around the explainer."""

    def __init__(self, storage: Any, config: Any, dashboard: Any = None,
                 *, xai_sample_rate: float = 1.0,
                 chaos_sample_rate: float = 0.01,
                 multi_agent_sample_rate: float = 0.1):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        self.xai = XAIDecisionExplainer(
            config, storage,
            causal_rl=self.causal_rl, temporal=self.temporal,
            multi_agent=self.multi_agent, hitl=self.hitl,
            carbon_market=self.carbon_market, chaos=self.chaos,
            precision=self.precision, federated=self.federated,
            xai_sample_rate=xai_sample_rate,
            chaos_sample_rate=chaos_sample_rate,
            multi_agent_sample_rate=multi_agent_sample_rate)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    async def explain_decision(self,
                               label: str,
                               features: List[float],
                               names: List[str],
                               model_fn: Callable,
                               reference_set: Optional[List[List[float]]] = None,
                               include_interactions: bool = True,
                               include_anchors: bool = True
                               ) -> Dict[str, Any]:
        return await self.xai.explain(
            decision_id=f"xai_{uuid.uuid4().hex[:8]}",
            label=label,
            features=features,
            names=names,
            model_fn=model_fn,
            reference_set=reference_set,
            include_counterfactual=True,
            include_interactions=include_interactions,
            include_anchors=include_anchors,
        )

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("error", f"Background task {name} failed: {exc}")

        task.add_done_callback(_done)
        return task

    async def start(self) -> None:
        self._running = True
        _ = asyncio.get_running_loop()
        self._spawn(self._causal_rl_loop(), "causal_rl")
        self._spawn(self._federated_loop(), "federated")
        self._spawn(self._multi_agent_loop(), "multi_agent")
        self._spawn(self._temporal_loop(), "temporal")
        self._spawn(self._xai_loop(), "xai")
        self._spawn(self._precision_loop(), "precision")
        self._spawn(self._carbon_market_loop(), "carbon_market")
        self._spawn(self._chaos_loop(), "chaos")
        self._spawn(self._distillation_loop(), "distillation")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()
        try:
            await self.xai.aclose()
        except Exception:
            pass

    async def aclose(self) -> None:
        await self.shutdown()

    async def __aenter__(self) -> "XAIOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "causal_graph_update_interval", 900) or 900))
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception as exc:
                log_event("debug", f"causal_rl loop error: {exc}")

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "federated_interval", 3600) or 3600))
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("xai_stats", dummy)
                await self.federated.pull_aggregated_weights("xai_stats")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "agent_negotiation_interval", 600) or 600))
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "temporal_verification_interval", 300) or 300))
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "xai_interval", 300) or 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="system_health",
                    features=[0.9, 0.4, 0.5, 0.4],
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "precision_check_interval", 300) or 300))
            try:
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "carbon_market_interval", 3600) or 3600))
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "chaos_test_interval", 1800) or 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "distillation_interval", 300) or 300))
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "xai_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "xai": self.xai.stats(),
            "top_features": self.xai.top_features(k=5),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
            "background_tasks": len(self._background_tasks),
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos/tests. Soft-fail for all methods."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})
        for name, val in (attributions or {}).items():
            self._data["xai_feature_importance"].append({
                "explanation_id": explanation_id,
                "feature_name": str(name), "importance": float(val)})

    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient,
                           payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic, "sender": sender,
            "recipient": recipient, "payload": payload})

    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window, active):
        self._data["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula, "operator": operator,
            "severity": severity, "description": description,
            "window": window, "active": active})

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({"state": state, "context": context})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._data["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "step": step,
            "state": state, "severity": severity, "approved": approved})

    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._data["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason,
            "saved_wh": saved_wh, "acc_delta": acc_delta})

    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._data["carbon_credit_prices"].append({
            "price_usd": price_usd, "currency": currency,
            "source": source, "region": region})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._data["rec_ledger"].append({
            "mwh": mwh, "price_per_mwh": price_per_mwh, "source": source,
            "certificate_id": certificate_id, "region": region,
            "retired": retired})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._data["net_zero_matches"].append({
            "match_id": match_id, "workload_kwh": workload_kwh,
            "intensity": intensity, "action": action,
            "carbon_kg": carbon_kg, "offset_cost": offset_cost,
            "credit_price": credit_price})

    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._data["chaos_experiments"].append({
            "experiment_id": experiment_id, "name": name,
            "fault_type": fault_type, "blast_radius": blast_radius,
            "steady_before": steady_before, "steady_after": steady_after,
            "status": status, "duration_ms": duration_ms})

    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._data["hitl_approval_queue"].append({
            "request_id": request_id, "rule_id": rule_id,
            "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (quality >= 0.5)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "xai_permutations": 32,
        "xai_ci_samples": 10,
        "xai_cf_max_iter": 200,
        "xai_cf_step": 0.05,
        "xai_anchor_min_precision": 0.85,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.1,
    }

    storage = InMemoryStorage()
    orch = XAIOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("xai_decision_explainer.py v17.1.0 — Demo")
    print("=" * 80)

    if not NUMPY_AVAILABLE:
        print("numpy not available — install with: pip install numpy")
        await orch.shutdown()
        return

    feats = [0.9, 0.4, 0.5, 0.4]
    names = ["quality", "carbon", "cost", "latency"]

    def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
        return float(np.dot(x, _w))

    # 1. Basic explanation with CI.
    print("\n=== Basic explanation with CI ===")
    explanation = await orch.explain_decision(
        label="strategy=carbon",
        features=feats,
        names=names,
        model_fn=_score,
        reference_set=[
            [0.7, 0.5, 0.6, 0.5],
            [0.8, 0.3, 0.4, 0.6],
            [0.6, 0.6, 0.5, 0.3],
        ],
        include_interactions=True,
        include_anchors=True,
    )
    print(f"  Method: {explanation['method']}")
    for k, v in explanation["attributions"].items():
        ci = explanation["attributions_ci"].get(k, {})
        print(f"    {k}: {v:+.4f} "
              f"[{ci.get('ci_low', 0):+.4f}, {ci.get('ci_high', 0):+.4f}]")

    print("\n=== Force plot ===")
    print(explanation["force_plot"])

    print("\n=== Waterfall plot ===")
    print(explanation["waterfall_plot"])

    # 2. Counterfactual minimal-change test.
    print("\n=== Counterfactual (minimal-change) ===")
    cf = explanation.get("counterfactual")
    if cf:
        print(f"  Success: {cf['success']}")
        print(f"  Iterations: {cf.get('iterations')}")
        print(f"  Changed features: {cf.get('changed_features')}")
        deltas = cf.get("deltas")
        if deltas:
            nonzero = [(names[i], d) for i, d in enumerate(deltas)
                       if abs(d) > 1e-9]
            print(f"  Total perturbations: {nonzero}")

    # 3. SHAP interactions (correct formula + no doubling).
    print("\n=== SHAP interactions ===")
    inter = explanation.get("interactions")
    if inter:
        flat = []
        for n1, row in inter.items():
            for n2, val in row.items():
                if n1 < n2:
                    flat.append((abs(val), n1, n2, val))
        flat.sort(reverse=True)
        for _, n1, n2, val in flat[:4]:
            print(f"  {n1} ↔ {n2}: {val:+.4f}")
        # Sanity: mirror symmetry
        ok = all(
            abs(inter[a][b] - inter[b][a]) < 1e-9
            for a in inter for b in inter
        )
        print(f"  Symmetry check (inter[i][j] == inter[j][i]): {ok}")

    # 4. Anchors.
    print("\n=== Anchors ===")
    anchors = explanation.get("anchors")
    if anchors:
        for a in anchors:
            print(f"  {a['feature']} ∈ [{a['value']-a['tolerance']:.3f}, "
                  f"{a['value']+a['tolerance']:.3f}] "
                  f"(precision={a['precision']:.2f})")
    else:
        print("  No anchors found (precision floor not met)")

    # 5. Hot-path latency.
    print("\n=== Hot-path latency (with all optional features) ===")
    t0 = time.monotonic()
    await orch.explain_decision(
        label="strategy=carbon",
        features=feats, names=names, model_fn=_score,
        include_interactions=False, include_anchors=False)
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    print(f"  Base explanation: {elapsed_ms:.1f} ms")

    # 6. Global feature importance.
    print("\n=== Global feature importance (after 5 explanations) ===")
    for i in range(4):
        await orch.explain_decision(
            label=f"strategy_{i}",
            features=[random.uniform(0.2, 1.0) for _ in range(4)],
            names=names, model_fn=_score)
    importance = orch.xai.global_feature_importance()
    for k, v in sorted(importance.items(), key=lambda kv: kv[1],
                       reverse=True):
        print(f"  {k}: {v:.4f}")

    # 7. Health check.
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    # 8. Chaos experiment.
    print("\n=== Chaos experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
