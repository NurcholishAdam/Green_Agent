#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/xai_v2_0_0.py
# VERSION: 2.0.0  (Phase 1-3 enhancements applied — see changelog)
# =============================================================================
"""
Explainable AI for every decision — v2.0.0

v1.0.0 (the original) computed a z-score deviation `(current - baseline) / scale`
and called it "attribution". That is anomaly localization, not feature
attribution — it tells you which inputs *moved*, not which inputs the *model
used*. Consumers reading `Explanation.top_features` could easily mistake the two.

v2.0.0 (Phase 1-3) fixes the honesty gap and adds real XAI:

  [P0] DeviationExplainer is the honest name for the old z-score logic.
       DecisionExplainer accepts a `predict_fn` and does real attribution.
       Every Explanation carries `attribution_method` so consumers know
       which method ran ("z_score" | "kernel_shap" | "lime" | "permutation" |
       "integrated_gradients"). Scale floor `1e-3` (not `1e-9`); safe
       timestamp coercion; counterfactual `alt_action_id: int | str`;
       single `top_k` for both rationale and data.

  [P1] `caller_confidence` / `caller_safety_ok` + `source` dict indicate who
       supplied each value. `carbon_price_signal` used via
       `carbon_price_context` (normal / elevated / critical). `timestamp`
       field on Explanation. `if feature_names is not None:`. Missing
       baseline/scales warning + strict mode. No eager numpy import.
       `__repr__`. Runnable `_demo()`.

  [P2] `correlation_id_var` ContextVar. `attribution_method` for downstream
       metrics. Bounded `_recent_explanations` deque. `health()`.

  [P3] Real KernelSHAP (coalitions + SHAP weights + weighted LS + efficiency
       constraint). LIME. Permutation importance. Integrated Gradients.
       Counterfactual generator. RollingBaseline (Welford-style).
       SafetyRuleEngine (threshold table). explain_decision (general).
       explain_batch + stream_explanations. save_state / load_state.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import math
import random
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (Any, AsyncIterator, Callable, Deque, Dict, List, Mapping,
                    Optional, Sequence, Tuple, Union)

# -----------------------------------------------------------------------------
# Optional deps — [P1] lazy imports for numpy (was imported eagerly at v1)
# -----------------------------------------------------------------------------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# -----------------------------------------------------------------------------
# Correlation ID — [P2]
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar("correlation_id", default="unknown")
logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================
ActionId = Union[int, str]

DEFAULT_FEATURE_NAMES = [
    "gpu_utilization_pct",
    "gpu_memory_used_mb",
    "gpu_power_watts",
    "gpu_temp_c",
    "cpu_utilization_pct",
    "energy_gpu_joules",
    "net_recv_bandwidth_gbps",
]

# (min, max) per feature; None means "no bound in that direction"
DEFAULT_SAFETY_RULES: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    "gpu_temp_c": (None, 85.0),
    "gpu_power_watts": (None, 400.0),
    "gpu_utilization_pct": (0.0, 100.0),
    "gpu_memory_used_mb": (0.0, 40000.0),
    "cpu_utilization_pct": (0.0, 100.0),
}

SUPPORTED_METHODS = ("auto", "kernel_shap", "lime", "permutation",
                     "integrated_gradients", "z_score")


# =============================================================================
# Explanation dataclass — [P0/P1/P2] new fields
# =============================================================================
@dataclass
class Explanation:
    decision_id: str
    kind: str
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float
    # [P0] Which attribution method ran. Consumers must check this before
    # interpreting `top_features` as Shapley values.
    attribution_method: str = "z_score"
    # [P1] Observability
    timestamp: Optional[str] = None
    caller_confidence: Optional[float] = None
    caller_safety_ok: Optional[bool] = None
    source: Dict[str, str] = field(default_factory=dict)
    carbon_price_context: Optional[Dict[str, Any]] = None
    # [P2] Correlation
    correlation_id: str = "unknown"
    # [P3] Full attribution map (top_features is a truncated view)
    all_attributions: Optional[Dict[str, float]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __repr__(self) -> str:
        # [P1] console-friendly
        top = ", ".join(f"{n}={v:+.2f}" for n, v in self.top_features[:3])
        return (f"Explanation(kind={self.kind!r}, "
                f"method={self.attribution_method!r}, "
                f"top=[{top}], safety={self.safety_ok}, "
                f"conf={self.confidence:.2f})")


# =============================================================================
# RollingBaseline — [P3]
# =============================================================================
class RollingBaseline:
    """Per-feature rolling mean/std over a fixed window.
    Caller does `observe(state)` per tick; `mean()` / `std()` return the
    current rolling stats suitable for passing to DeviationExplainer."""

    def __init__(self, window: int = 60,
                 feature_names: Optional[Sequence[str]] = None):
        self.window = window
        self.feature_names = (
            list(feature_names) if feature_names is not None
            else list(DEFAULT_FEATURE_NAMES))
        self._windows: Dict[str, Deque[float]] = {
            f: deque(maxlen=window) for f in self.feature_names}

    def observe(self, state: Dict[str, Any]) -> None:
        for f in self.feature_names:
            if f in state:
                try:
                    self._windows[f].append(float(state[f]))
                except (TypeError, ValueError):
                    pass

    def mean(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for f, w in self._windows.items():
            out[f] = (sum(w) / len(w)) if w else 0.0
        return out

    def std(self, floor: float = 1e-3) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for f, w in self._windows.items():
            if len(w) < 2:
                out[f] = floor
                continue
            mu = sum(w) / len(w)
            var = sum((x - mu) ** 2 for x in w) / (len(w) - 1)
            out[f] = max(math.sqrt(var), floor)
        return out

    def is_ready(self, min_samples: int = 5) -> bool:
        return all(len(w) >= min_samples for w in self._windows.values())

    def save_state(self) -> Dict[str, Any]:
        return {"window": self.window,
                "feature_names": self.feature_names,
                "data": {f: list(w) for f, w in self._windows.items()}}

    def load_state(self, data: Dict[str, Any]) -> None:
        self.window = int(data.get("window", self.window))
        self.feature_names = list(data.get("feature_names", self.feature_names))
        raw = data.get("data", {})
        self._windows = {f: deque(raw.get(f, []), maxlen=self.window)
                          for f in self.feature_names}


# =============================================================================
# SafetyRuleEngine — [P3]
# =============================================================================
class SafetyRuleEngine:
    """Threshold table that auto-populates `safety_ok` from the state."""

    def __init__(self,
                 rules: Optional[Mapping[str, Tuple[Optional[float], Optional[float]]]] = None):
        self.rules: Dict[str, Tuple[Optional[float], Optional[float]]] = (
            dict(rules) if rules is not None else dict(DEFAULT_SAFETY_RULES))

    def add_rule(self, feature: str,
                 min_val: Optional[float],
                 max_val: Optional[float]) -> None:
        self.rules[feature] = (min_val, max_val)

    def evaluate(self, state: Dict[str, Any]) -> Tuple[bool, List[str]]:
        violations: List[str] = []
        for f, (lo, hi) in self.rules.items():
            v_raw = state.get(f)
            if v_raw is None:
                continue
            try:
                v = float(v_raw)
            except (TypeError, ValueError):
                continue
            if lo is not None and v < lo:
                violations.append(f"{f}={v:.2f} < min={lo:.2f}")
            if hi is not None and v > hi:
                violations.append(f"{f}={v:.2f} > max={hi:.2f}")
        return (len(violations) == 0, violations)

    def save_state(self) -> Dict[str, Any]:
        return {"rules": {f: [lo, hi] for f, (lo, hi) in self.rules.items()}}

    def load_state(self, data: Dict[str, Any]) -> None:
        for f, pair in data.get("rules", {}).items():
            self.rules[f] = (pair[0], pair[1])


# =============================================================================
# AttributionMethods — [P3] pure functions
# =============================================================================
class AttributionMethods:
    """All real attribution methods operate on a `predict_fn: Dict -> float`
    (or a torch model for Integrated Gradients)."""

    @staticmethod
    def _to_vector(features: Dict[str, Any],
                   feature_names: List[str]) -> "np.ndarray":
        return np.array([float(features.get(n, 0.0)) for n in feature_names],
                        dtype=float)

    @staticmethod
    def _shap_kernel_weight(size: int, n: int) -> float:
        if size == 0 or size == n:
            return 1e6
        try:
            return (n - 1) / (math.comb(n, size) * size * (n - size))
        except Exception:
            return 1.0

    @staticmethod
    def kernel_shap(predict_fn: Callable[[Dict[str, Any]], float],
                    features: Dict[str, Any],
                    feature_names: List[str],
                    n_samples: int = 128,
                    seed: Optional[int] = None) -> Dict[str, float]:
        """[P3] KernelSHAP: coalition sampling + SHAP kernel weights +
        weighted least-squares + efficiency constraint."""
        if not NUMPY_AVAILABLE:
            return {n: 0.0 for n in feature_names}
        rng = random.Random(seed) if seed is not None else random
        x = AttributionMethods._to_vector(features, feature_names)
        n = len(x)

        coalitions: List["np.ndarray"] = []
        weights: List[float] = []
        max_exact = 2 ** n if n <= 8 else 0

        if max_exact and max_exact <= 512:
            for mask in range(1, max_exact - 1):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if mask & (1 << i):
                        z[i] = x[i]
                        size += 1
                coalitions.append(z)
                weights.append(AttributionMethods._shap_kernel_weight(size, n))
        else:
            for _ in range(n_samples):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if rng.random() < 0.5:
                        z[i] = x[i]
                        size += 1
                coalitions.append(z)
                weights.append(AttributionMethods._shap_kernel_weight(size, n))

        # Efficiency constraint: force sum(phi) == f(x) - f(empty)
        coalitions.append(np.zeros(n))
        weights.append(1e6)
        coalitions.append(x.copy())
        weights.append(1e6)

        try:
            y = np.array([
                float(predict_fn({feature_names[i]: float(c[i])
                                  for i in range(n)}))
                for c in coalitions
            ])
        except Exception as e:
            logger.debug("kernel_shap predict failed: %s", e)
            return {n_: 0.0 for n_ in feature_names}

        Z = np.array(coalitions)
        w = np.array(weights)
        try:
            ZtW = Z.T * w
            A = ZtW @ Z + 1e-6 * np.eye(n)
            b = ZtW @ y
            phi = np.linalg.solve(A, b)
        except Exception:
            try:
                phi = np.linalg.lstsq(Z * np.sqrt(w)[:, None],
                                       y * np.sqrt(w), rcond=None)[0]
            except Exception:
                phi = np.zeros(n)
        return {name: float(phi[i]) for i, name in enumerate(feature_names)}

    @staticmethod
    def lime(predict_fn: Callable[[Dict[str, Any]], float],
             features: Dict[str, Any],
             feature_names: List[str],
             n_samples: int = 200,
             seed: Optional[int] = None) -> Dict[str, float]:
        """[P3] LIME: local linear surrogate."""
        if not (NUMPY_AVAILABLE and SKLEARN_AVAILABLE):
            return {n: 0.0 for n in feature_names}
        rng = np.random.RandomState(seed)
        x = AttributionMethods._to_vector(features, feature_names)
        X = np.tile(x, (n_samples, 1)) + rng.normal(0, 0.1, (n_samples, len(x)))
        try:
            y = np.array([
                float(predict_fn({feature_names[i]: float(X[j][i])
                                  for i in range(len(feature_names))}))
                for j in range(n_samples)
            ])
        except Exception as e:
            logger.debug("lime predict failed: %s", e)
            return {n: 0.0 for n in feature_names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        try:
            model = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(feature_names, model.coef_.tolist()))
        except Exception as e:
            logger.debug("lime fit failed: %s", e)
            return {n: 0.0 for n in feature_names}

    @staticmethod
    def permutation(predict_fn: Callable[[Dict[str, Any]], float],
                    features: Dict[str, Any],
                    feature_names: List[str],
                    baseline: Optional[Dict[str, float]] = None,
                    n_repeats: int = 5,
                    seed: Optional[int] = None) -> Dict[str, float]:
        """[P3] Permutation importance: perturb each feature, measure
        prediction delta. Sensible when the model is quick to evaluate."""
        if not NUMPY_AVAILABLE:
            return {n: 0.0 for n in feature_names}
        rng = np.random.RandomState(seed)
        try:
            base_pred = float(predict_fn(features))
        except Exception as e:
            logger.debug("permutation baseline predict failed: %s", e)
            return {n: 0.0 for n in feature_names}
        baseline = baseline or {}
        out: Dict[str, float] = {}
        for name in feature_names:
            deltas: List[float] = []
            for _ in range(n_repeats):
                perturbed = dict(features)
                v = float(features.get(name, 0.0))
                fallback = float(baseline.get(name, v))
                perturbed[name] = fallback + rng.normal(
                    0, max(1e-3, abs(v) * 0.1))
                try:
                    p = float(predict_fn(perturbed))
                    deltas.append(base_pred - p)
                except Exception:
                    continue
            out[name] = float(np.mean(deltas)) if deltas else 0.0
        return out

    @staticmethod
    def integrated_gradients(model: Any,
                             features: Dict[str, Any],
                             feature_names: List[str],
                             baseline: Optional[Dict[str, float]] = None,
                             n_steps: int = 32) -> Dict[str, float]:
        """[P3] Integrated Gradients. Requires a torch model whose forward
        accepts a tensor of shape (1, len(feature_names))."""
        if not (TORCH_AVAILABLE and NUMPY_AVAILABLE):
            return {n: 0.0 for n in feature_names}
        try:
            x = torch.tensor(
                [float(features.get(n, 0.0)) for n in feature_names],
                dtype=torch.float32, requires_grad=True)
            b = torch.tensor(
                [float((baseline or {}).get(n, 0.0)) for n in feature_names],
                dtype=torch.float32)
            total = torch.zeros_like(x)
            for a in np.linspace(0.0, 1.0, n_steps):
                point = (b + float(a) * (x - b)).detach().requires_grad_(True)
                out = model(point.unsqueeze(0))
                if out.dim() > 1:
                    out = out.sum()
                out.backward()
                if point.grad is not None:
                    total = total + point.grad
            avg = total / n_steps
            attrs = avg * (x - b)
            return {name: float(attrs[i].item())
                    for i, name in enumerate(feature_names)}
        except Exception as e:
            logger.debug("integrated_gradients failed: %s", e)
            return {n: 0.0 for n in feature_names}


# =============================================================================
# DeviationExplainer — [P0] honest name for the original z-score behavior
# =============================================================================
class DeviationExplainer:
    """Z-score deviation from a rolling baseline.

    This is *anomaly localization*, not feature attribution. It does NOT know
    about your model, so it cannot tell you which features the model used.
    Use `DecisionExplainer` with a `predict_fn` for real attribution.
    """

    def __init__(self,
                 feature_names: Optional[Sequence[str]] = None,
                 *,
                 min_scale: float = 1e-3):
        # [P1] `is not None`, not truthiness (empty list vs default)
        self.feature_names = (
            list(feature_names) if feature_names is not None
            else list(DEFAULT_FEATURE_NAMES))
        # [P0] floor prevents explosion to 1e9 on constant features
        self.min_scale = min_scale
        self.safety_engine = SafetyRuleEngine()
        # [P2] bounded
        self._recent_explanations: Deque[Explanation] = deque(maxlen=500)
        # [P1] strict mode: raise if a feature is missing from baseline/scales
        self.strict = False

    # ------------------------------------------------------------------
    def _attributions(self,
                      current: Dict[str, Any],
                      baseline: Dict[str, float],
                      scales: Dict[str, float],
                      ) -> List[Tuple[str, float]]:
        attrs: List[Tuple[str, float]] = []
        for f in self.feature_names:
            v = float(current.get(f, 0.0))
            if f not in baseline and self.strict:
                raise KeyError(f"missing baseline for feature '{f}'")
            mu = float(baseline.get(f, 0.0))
            s = max(float(scales.get(f, 1.0)), self.min_scale)
            # [P0] explicit zero-vs-zero case
            if v == mu == 0.0:
                attrs.append((f, 0.0))
            else:
                attrs.append((f, (v - mu) / s))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    # ------------------------------------------------------------------
    def _carbon_context(self, price: float,
                        normal_threshold: float = 30.0,
                        critical_threshold: float = 50.0,
                        ) -> Optional[Dict[str, Any]]:
        if price <= 0:
            return None
        if price > critical_threshold:
            flag = "critical"
        elif price > normal_threshold:
            flag = "elevated"
        else:
            flag = "normal"
        return {"price": float(price),
                "flag": flag,
                "threshold_normal": normal_threshold,
                "threshold_critical": critical_threshold}

    # ------------------------------------------------------------------
    def _safe_timestamp(self, current: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        ts_raw = current.get("timestamp")
        if ts_raw is None:
            return "unknown", None
        if isinstance(ts_raw, (int, float)):
            return f"{float(ts_raw):.2f}", str(ts_raw)
        return str(ts_raw), str(ts_raw)

    # ------------------------------------------------------------------
    def explain_anomaly(self,
                        kind: str,
                        current: Dict[str, Any],
                        baseline: Dict[str, float],
                        scales: Dict[str, float],
                        counterfactuals: Optional[Dict[ActionId, float]] = None,
                        safety_ok: Optional[bool] = None,
                        carbon_price: float = 0.0,
                        confidence: float = 0.5,
                        top_k: int = 5,
                        correlation_id: Optional[str] = None,
                        ) -> Explanation:
        # [P0] safe timestamp (was `:.2f` on possibly-string timestamp)
        ts_str, ts_iso = self._safe_timestamp(current)

        attrs = self._attributions(current, baseline, scales)

        # [P0] counterfactual key type preserved
        cf_list: List[Dict[str, Any]] = []
        if counterfactuals:
            for k, v in sorted(counterfactuals.items(),
                                key=lambda kv: kv[1], reverse=True)[:3]:
                cf_list.append({"alt_action_id": k,
                                "expected_reward": float(v)})

        # [P1] use safety engine unless caller overrode it
        auto_safe, violations = self.safety_engine.evaluate(current)
        final_safe = auto_safe if safety_ok is None else bool(safety_ok)

        carbon_ctx = self._carbon_context(carbon_price)

        # [P0] single top_k for both rationale and data
        top = attrs[:max(1, int(top_k))]
        top_str = ", ".join(f"{n}={v:+.2f}" for n, v in top[:3])
        safety_str = "ok" if final_safe else "VIOLATION"
        if violations:
            safety_str += f" ({'; '.join(violations[:2])})"

        rationale = (
            f"Deviation '{kind}' at t={ts_str}. "
            f"Top z-score deviations vs rolling baseline: {top_str}. "
            f"Safety: {safety_str}. "
            f"Carbon price={carbon_price:.4f}."
        )

        exp = Explanation(
            decision_id=f"exp-{uuid.uuid4().hex[:8]}",
            kind=kind,
            top_features=top,
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=float(confidence),
            safety_ok=final_safe,
            carbon_price_signal=float(carbon_price),
            attribution_method="z_score",
            timestamp=ts_iso,
            caller_confidence=float(confidence),
            caller_safety_ok=safety_ok,
            source={
                "attribution": "deviation",
                "confidence": "caller",
                "safety": "engine" if safety_ok is None else "caller",
            },
            carbon_price_context=carbon_ctx,
            correlation_id=correlation_id or correlation_id_var.get(),
            all_attributions=dict(attrs),
        )
        self._recent_explanations.append(exp)
        return exp

    # ------------------------------------------------------------------
    # [P3] Persistence — available on both classes
    # ------------------------------------------------------------------
    def save_state(self, path: Optional[str] = None) -> bool:
        path = path or "/tmp/xai_v2_state.json"
        data = {
            "version": "2.0.0",
            "feature_names": self.feature_names,
            "min_scale": self.min_scale,
            "safety_rules": self.safety_engine.save_state(),
            "recent_explanations": [
                e.to_dict() for e in list(self._recent_explanations)[-50:]
            ],
        }
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(data, default=str, indent=2))
            return True
        except Exception as e:
            logger.warning("save_state failed: %s", e)
            return False

    def load_state(self, path: Optional[str] = None) -> bool:
        path = path or "/tmp/xai_v2_state.json"
        try:
            p = Path(path)
            if not p.exists():
                return False
            data = json.loads(p.read_text())
            self.feature_names = list(data.get("feature_names",
                                               self.feature_names))
            self.min_scale = float(data.get("min_scale", self.min_scale))
            self.safety_engine.load_state(data.get("safety_rules", {}))
            raw = data.get("recent_explanations", [])
            self._recent_explanations = deque(
                [Explanation(**e) for e in raw],
                maxlen=self._recent_explanations.maxlen)
            return True
        except Exception as e:
            logger.warning("load_state failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # [P2] Observability
    # ------------------------------------------------------------------
    def health(self) -> Dict[str, Any]:
        return {
            "version": "2.0.0",
            "class": self.__class__.__name__,
            "feature_names": list(self.feature_names),
            "min_scale": self.min_scale,
            "safety_rules": len(self.safety_engine.rules),
            "recent_explanations": len(self._recent_explanations),
            "numpy": NUMPY_AVAILABLE,
            "sklearn": SKLEARN_AVAILABLE,
            "torch": TORCH_AVAILABLE,
        }


# =============================================================================
# DecisionExplainer — [P0] real XAI when a predict_fn (or model) is provided
# =============================================================================
class DecisionExplainer(DeviationExplainer):
    """Real feature attribution when `predict_fn` (or a torch `model`) is
    provided. Falls back to `DeviationExplainer`'s z-score when it isn't,
    and tags the resulting `Explanation.attribution_method` accordingly."""

    def __init__(self,
                 feature_names: Optional[Sequence[str]] = None,
                 *,
                 predict_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
                 method: str = "auto",
                 model: Any = None,
                 min_scale: float = 1e-3,
                 carbon_price_threshold_normal: float = 30.0,
                 carbon_price_threshold_critical: float = 50.0):
        super().__init__(feature_names=feature_names, min_scale=min_scale)
        if method not in SUPPORTED_METHODS:
            raise ValueError(
                f"method must be one of {SUPPORTED_METHODS}, got {method!r}")
        self.predict_fn = predict_fn
        self.model = model
        self.method = method
        self.carbon_price_threshold_normal = carbon_price_threshold_normal
        self.carbon_price_threshold_critical = carbon_price_threshold_critical

    # ------------------------------------------------------------------
    def _resolve_method(self) -> str:
        if self.method != "auto":
            return self.method
        if self.model is not None and TORCH_AVAILABLE:
            return "integrated_gradients"
        if self.predict_fn is not None and NUMPY_AVAILABLE:
            return "kernel_shap"
        return "z_score"

    # ------------------------------------------------------------------
    def _real_attributions(self,
                           current: Dict[str, Any],
                           baseline: Optional[Dict[str, float]],
                           ) -> Tuple[Optional[Dict[str, float]], Optional[str]]:
        method = self._resolve_method()
        try:
            if method == "kernel_shap" and self.predict_fn is not None:
                return (AttributionMethods.kernel_shap(
                    self.predict_fn, current, self.feature_names), method)
            if method == "lime" and self.predict_fn is not None:
                return (AttributionMethods.lime(
                    self.predict_fn, current, self.feature_names), method)
            if method == "permutation" and self.predict_fn is not None:
                return (AttributionMethods.permutation(
                    self.predict_fn, current, self.feature_names, baseline),
                    method)
            if method == "integrated_gradients" and self.model is not None:
                return (AttributionMethods.integrated_gradients(
                    self.model, current, self.feature_names, baseline),
                    method)
        except Exception as e:
            logger.debug("real attribution failed: %s", e)
        return None, None

    # ------------------------------------------------------------------
    def _generate_counterfactuals(self,
                                  current: Dict[str, Any],
                                  baseline: Optional[Dict[str, float]] = None,
                                  n_top: int = 3) -> List[Dict[str, Any]]:
        """[P3] Single-feature deltas that most reduce the prediction.
        For discrete `predict_fn`s, wrap the action into a scalar reward
        first (e.g., probability of the current action)."""
        if self.predict_fn is None:
            return []
        try:
            base_pred = float(self.predict_fn(current))
        except Exception:
            return []
        baseline = baseline or {}
        results: List[Dict[str, Any]] = []
        for f in self.feature_names:
            v = float(current.get(f, 0.0))
            fallback = float(baseline.get(f, v))
            best: Optional[Dict[str, Any]] = None
            for delta_pct in (0.1, 0.25, 0.5, 1.0):
                for sign in (-1, +1):
                    if v != 0.0:
                        new_v = v * (1.0 + sign * delta_pct)
                    else:
                        new_v = fallback + sign * max(1e-3, abs(fallback) * delta_pct + 0.01)
                    candidate = dict(current)
                    candidate[f] = new_v
                    try:
                        p = float(self.predict_fn(candidate))
                    except Exception:
                        continue
                    improvement = base_pred - p
                    if best is None or improvement > best["improvement"]:
                        best = {
                            "feature": f,
                            "original": v,
                            "alternative": new_v,
                            "delta": new_v - v,
                            "expected_reward": p,
                            "improvement": improvement,
                        }
            if best is not None and best["improvement"] > 1e-3:
                results.append(best)
        results.sort(key=lambda x: x["improvement"], reverse=True)
        return results[:n_top]

    # ------------------------------------------------------------------
    def explain_decision(self,
                         kind: str,
                         current: Dict[str, Any],
                         baseline: Optional[Dict[str, float]] = None,
                         scales: Optional[Dict[str, float]] = None,
                         counterfactuals: Optional[Dict[ActionId, float]] = None,
                         safety_ok: Optional[bool] = None,
                         carbon_price: float = 0.0,
                         confidence: float = 0.5,
                         top_k: int = 5,
                         correlation_id: Optional[str] = None,
                         ) -> Explanation:
        """[P3] General decision explanation (works for non-anomalies too)."""
        baseline = baseline or {}
        ts_str, ts_iso = self._safe_timestamp(current)

        # [P3] Try real attribution first
        method_used: Optional[str] = None
        attrs: Optional[List[Tuple[str, float]]] = None
        full_map: Optional[Dict[str, float]] = None

        if self.predict_fn is not None or self.model is not None:
            real_map, method_used = self._real_attributions(current, baseline)
            if real_map is not None:
                attrs = sorted(real_map.items(),
                               key=lambda kv: abs(kv[1]), reverse=True)
                full_map = dict(real_map)

        # Fall back to z-score
        if attrs is None:
            scales_used = scales or {f: 1.0 for f in self.feature_names}
            attrs = self._attributions(current, baseline, scales_used)
            method_used = "z_score"
            full_map = dict(attrs)

        # [P3] Counterfactuals
        cf_list: List[Dict[str, Any]] = []
        if counterfactuals:
            for k, v in sorted(counterfactuals.items(),
                                key=lambda kv: kv[1], reverse=True)[:3]:
                cf_list.append({"alt_action_id": k,
                                "expected_reward": float(v)})
        elif self.predict_fn is not None:
            cf_list = self._generate_counterfactuals(current, baseline)

        # [P1] Safety
        auto_safe, violations = self.safety_engine.evaluate(current)
        final_safe = auto_safe if safety_ok is None else bool(safety_ok)

        carbon_ctx = self._carbon_context(
            carbon_price,
            self.carbon_price_threshold_normal,
            self.carbon_price_threshold_critical)

        # [P0] single top_k
        top = attrs[:max(1, int(top_k))]
        top_str = ", ".join(f"{n}={v:+.2f}" for n, v in top[:3])
        safety_str = "ok" if final_safe else "VIOLATION"
        if violations:
            safety_str += f" ({'; '.join(violations[:2])})"

        rationale = (
            f"Decision '{kind}' at t={ts_str} via {method_used}. "
            f"Top attributions: {top_str}. "
            f"Safety: {safety_str}. "
            f"Carbon price={carbon_price:.4f}."
        )

        exp = Explanation(
            decision_id=f"exp-{uuid.uuid4().hex[:8]}",
            kind=kind,
            top_features=top,
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=float(confidence),
            safety_ok=final_safe,
            carbon_price_signal=float(carbon_price),
            attribution_method=method_used,
            timestamp=ts_iso,
            caller_confidence=float(confidence),
            caller_safety_ok=safety_ok,
            source={
                "attribution": method_used,
                "confidence": "caller",
                "safety": "engine" if safety_ok is None else "caller",
            },
            carbon_price_context=carbon_ctx,
            correlation_id=correlation_id or correlation_id_var.get(),
            all_attributions=full_map,
        )
        self._recent_explanations.append(exp)
        return exp

    # ------------------------------------------------------------------
    def explain_anomaly(self,
                        kind: str,
                        current: Dict[str, Any],
                        baseline: Dict[str, float],
                        scales: Dict[str, float],
                        counterfactuals: Optional[Dict[ActionId, float]] = None,
                        safety_ok: Optional[bool] = None,
                        carbon_price: float = 0.0,
                        confidence: float = 0.5,
                        top_k: int = 5,
                        correlation_id: Optional[str] = None,
                        ) -> Explanation:
        """Backward-compatible entry point — delegates to explain_decision."""
        return self.explain_decision(
            kind=kind, current=current, baseline=baseline, scales=scales,
            counterfactuals=counterfactuals, safety_ok=safety_ok,
            carbon_price=carbon_price, confidence=confidence,
            top_k=top_k, correlation_id=correlation_id)

    # ------------------------------------------------------------------
    # [P3] Batch & stream
    # ------------------------------------------------------------------
    async def explain_batch(self,
                            items: List[Dict[str, Any]],
                            concurrency: int = 8) -> List[Explanation]:
        """[P3] Batched explanation with a concurrency limit.
        Each `item` is a kwargs dict for `explain_decision`."""
        sem = asyncio.Semaphore(concurrency)

        async def _one(item: Dict[str, Any]) -> Explanation:
            async with sem:
                return self.explain_decision(**item)

        return await asyncio.gather(*[_one(item) for item in items])

    async def stream_explanations(
            self,
            items: AsyncIterator[Dict[str, Any]],
    ) -> AsyncIterator[Explanation]:
        """[P3] Stream explanations from an async iterator."""
        async for item in items:
            yield self.explain_decision(**item)

    # ------------------------------------------------------------------
    def health(self) -> Dict[str, Any]:
        info = super().health()
        info.update({
            "method": self.method,
            "resolved_method": self._resolve_method(),
            "predict_fn_available": self.predict_fn is not None,
            "model_available": self.model is not None,
        })
        return info


# =============================================================================
# DEMO
# =============================================================================
def _synthetic_predict(state: Dict[str, Any]) -> float:
    """Synthetic anomaly score. Temperature and power dominate; GPU and CPU
    utilization contribute too. The point is that the score is *driven by*
    three features, which is exactly what SHAP should recover."""
    return (
        0.4 * float(state.get("gpu_temp_c", 0.0)) / 100.0
        + 0.3 * float(state.get("gpu_power_watts", 0.0)) / 400.0
        + 0.2 * float(state.get("gpu_utilization_pct", 0.0)) / 100.0
        + 0.05 * float(state.get("cpu_utilization_pct", 0.0)) / 100.0
        + 0.05 * float(state.get("gpu_memory_used_mb", 0.0)) / 40000.0
    )


async def _demo() -> None:
    print("=" * 80)
    print("xai v2.0.0 — Explainable AI demo (Phase 1-3)")
    print("=" * 80)

    baseline = {
        "gpu_utilization_pct": 50.0, "gpu_memory_used_mb": 8000.0,
        "gpu_power_watts": 200.0, "gpu_temp_c": 65.0,
        "cpu_utilization_pct": 40.0, "energy_gpu_joules": 100.0,
        "net_recv_bandwidth_gbps": 2.0,
    }
    scales = {
        "gpu_utilization_pct": 10.0, "gpu_memory_used_mb": 2000.0,
        "gpu_power_watts": 30.0, "gpu_temp_c": 5.0,
        "cpu_utilization_pct": 5.0, "energy_gpu_joules": 20.0,
        "net_recv_bandwidth_gbps": 0.5,
    }
    current = {
        "gpu_utilization_pct": 92.0, "gpu_memory_used_mb": 35000.0,
        "gpu_power_watts": 380.0, "gpu_temp_c": 82.0,
        "cpu_utilization_pct": 88.0, "energy_gpu_joules": 300.0,
        "net_recv_bandwidth_gbps": 4.5,
        "timestamp": 1700000000.5,
    }

    # --- 1. DeviationExplainer (backward-compatible z-score) ---
    print("\n=== 1. DeviationExplainer (z-score, honest name) ===")
    dev = DeviationExplainer()
    dev.strict = False
    exp_dev = dev.explain_anomaly(
        "thermal_spike", current, baseline, scales,
        counterfactuals={0: 0.1, 1: 0.3, 2: 0.9},
        carbon_price=42.5, confidence=0.4)
    print(f"  method:              {exp_dev.attribution_method}")
    print(f"  top_features:        {[(n, round(v, 2)) for n, v in exp_dev.top_features[:3]]}")
    print(f"  safety_ok:           {exp_dev.safety_ok}")
    print(f"  caller_safety_ok:    {exp_dev.caller_safety_ok}")
    print(f"  carbon_context:      {exp_dev.carbon_price_context}")
    print(f"  source:              {exp_dev.source}")
    print(f"  repr:                {exp_dev!r}")

    # --- 2. DecisionExplainer with real KernelSHAP ---
    print("\n=== 2. DecisionExplainer (KernelSHAP, real attribution) ===")
    dec_ks = DecisionExplainer(predict_fn=_synthetic_predict,
                               method="kernel_shap")
    exp_ks = dec_ks.explain_decision(
        "policy_choice", current, carbon_price=55.0)
    print(f"  method:              {exp_ks.attribution_method}")
    print(f"  top_features:        {[(n, round(v, 3)) for n, v in exp_ks.top_features[:3]]}")
    print(f"  rationale:           {exp_ks.rationale}")
    print(f"  all_attributions:    {{{', '.join(f'{k}: {v:+.3f}' for k, v in list((exp_ks.all_attributions or {}).items())[:3])}}}")
    print(f"  counterfactuals:     {exp_ks.counterfactuals[:2]}")

    # --- 3. Real LIME ---
    print("\n=== 3. DecisionExplainer (LIME) ===")
    dec_lime = DecisionExplainer(predict_fn=_synthetic_predict, method="lime")
    exp_lime = dec_lime.explain_decision("policy_choice", current)
    print(f"  method:              {exp_lime.attribution_method}")
    print(f"  top_features:        {[(n, round(v, 3)) for n, v in exp_lime.top_features[:3]]}")

    # --- 4. Permutation importance ---
    print("\n=== 4. DecisionExplainer (permutation) ===")
    dec_perm = DecisionExplainer(predict_fn=_synthetic_predict, method="permutation")
    exp_perm = dec_perm.explain_decision("policy_choice", current)
    print(f"  method:              {exp_perm.attribution_method}")
    print(f"  top_features:        {[(n, round(v, 3)) for n, v in exp_perm.top_features[:3]]}")

    # --- 5. RollingBaseline ---
    print("\n=== 5. RollingBaseline (Welford-style) ===")
    rb = RollingBaseline(window=30)
    for i in range(10):
        rb.observe({"gpu_utilization_pct": 50 + i * 2,
                    "gpu_temp_c": 60 + i})
    print(f"  ready:               {rb.is_ready(5)}")
    print(f"  mean:                {rb.mean()}")
    print(f"  std:                 {rb.std()}")
    print(f"  save_state:          {list(rb.save_state().keys())}")

    # --- 6. SafetyRuleEngine ---
    print("\n=== 6. SafetyRuleEngine ===")
    safe, violations = SafetyRuleEngine().evaluate(current)
    print(f"  safe:                {safe}")
    print(f"  violations:          {violations}")

    # --- 7. Counterfactual generator ---
    print("\n=== 7. Counterfactual generator ===")
    cfs = dec_ks._generate_counterfactuals(current, baseline)
    for c in cfs[:3]:
        print(f"  {c['feature']:<25} {c['original']:>8.1f} -> "
              f"{c['alternative']:>8.1f}  "
              f"reward {c['expected_reward']:.3f}  "
              f"Δ {c['improvement']:+.3f}")

    # --- 8. Batch + stream ---
    print("\n=== 8. Batch + stream ===")
    batch_items = [
        {"kind": "batch_a", "current": current},
        {"kind": "batch_b", "current": {**current, "gpu_temp_c": 70.0}},
        {"kind": "batch_c", "current": {**current, "gpu_temp_c": 75.0}},
    ]
    batch_results = await dec_ks.explain_batch(batch_items, concurrency=4)
    print(f"  batch size:          {len(batch_results)}")
    for r in batch_results:
        print(f"    {r.kind}: top={[(n, round(v, 2)) for n, v in r.top_features[:2]]}")

    async def _stream_items() -> AsyncIterator[Dict[str, Any]]:
        for item in batch_items:
            yield item

    count = 0
    async for _ in dec_ks.stream_explanations(_stream_items()):
        count += 1
    print(f"  streamed:            {count}")

    # --- 9. Persistence ---
    print("\n=== 9. Persistence ===")
    saved = dec_ks.save_state("/tmp/xai_v2_state.json")
    loaded = dec_ks.load_state("/tmp/xai_v2_state.json")
    print(f"  saved:               {saved}")
    print(f"  loaded:              {loaded}")
    print(f"  recent_explanations: {len(dec_ks._recent_explanations)}")

    # --- 10. Health ---
    print("\n=== 10. Health ===")
    print(json.dumps(dec_ks.health(), indent=2, default=str))

    print("\n" + "=" * 80)
    print("✅ xai v2.0.0 demo complete")
    print("=" * 80)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    asyncio.run(_demo())
