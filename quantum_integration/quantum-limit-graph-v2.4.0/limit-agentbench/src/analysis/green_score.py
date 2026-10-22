# src/analysis/optimization/green_score.py
"""
Green score computation combining accuracy and efficiency (Enhanced, v2.0.0)
=============================================================================

The Green Score is a single-number summary of an agent's sustainability
performance. It is used by leaderboards, agent-selection policies, and
Pareto-frontier ranking.

Original API preserved (all signatures, all return shapes):
    score = compute_green_score(accuracy, energy, latency,
                                alpha=0.6, beta=0.2)
    result = compute_green_score_detailed(accuracy, energy, latency,
                                          alpha=0.6, beta=0.2, ...)
    calc = GreenScoreCalculator(...)
    result = calc.compute(accuracy, energy, latency, carbon_kg=..., ...)
    result.score / result.to_float()

Enhanced API:
    result.inputs_valid        # False if any input was None/NaN/Inf/non-numeric
    result.severity            # "info" / "warning" / "critical" / "emergency"
    result.raw_accuracy        # pre-normalisation inputs are always available
    result.raw_energy_wh       #   so `to_decision_record()` can use them
    result.raw_latency_ms
    result.raw_carbon_kg
    result.raw_helium_units
    result.raw_score_full      # includes carbon + helium terms (when enabled)
    result.to_dict(safe=True)  # JSON-safe serialisation

    calc.get_statistics()      # instance-scoped
    calc.reset_statistics()
    calc.set_hitl_callback(None)   # clear callback
    calc.contribute_federated(workload_class=..., dedupe=True)

Enhancements implemented (v2.0.0):
  1.  Raw inputs on the result — `raw_*` fields carry the pre-normalisation
                                  values so DecisionRecord emission uses
                                  physical units (v1 wrote normalised values
                                  into latency_ms / energy_kwh — fixed).
  2.  Input validity tracking — `inputs_valid` distinguishes "garbage input"
                                 from "valid but out-of-range" (v1 conflated
                                 them and never set `_STATS["invalid"]`).
  3.  Mode-aware HITL        — normalised mode uses absolute near-extremum
                                thresholds; NONE mode uses z-score against
                                history. v1 fired on every non-normalised run
                                and never on normalised ones.
  4.  Instance statistics    — `_StatsCounter` with `RLock`; module-level
                                shims preserved for v1 compat.
  5.  Federated timestamp fix— `FederatedGreenProfile.timestamp` uses a
                                proper factory (v1 froze it at import time).
  6.  Corrected federated std— law-of-total-variance formula (within +
                                between); v1 averaged stds.
  7.  Idempotent federated   — `contribute_federated(dedupe=True)` skips
                                re-pushes when no new computations happened.
  8.  MANUAL carbon/helium   — caller-supplied ranges are now applied
                                (v1 left them unbounded).
  9.  Honoured flags         — `bounds_check`, `provenance`, `statistics`
                                are consulted (v1 declared and ignored them).
 10.  Carbon warning         — warn once when `carbon_kg>0` while
                                `features['carbon']=False` (v1 silently
                                dropped the value).
 11.  HITL annotation        — callback returning False annotates
                                `review_reason` with `[HITL: denied]`.
 12.  Precision scoring      — opt-in `features['precision_scoring']`
                                shrinks energy/latency penalty weight at
                                lower precision (relative indicator only).
 13.  DecisionRecord filter  — `inspect.signature` filter against the
                                installed DecisionRecord (v1 raised on
                                unexpected fields).
 14.  JSON-safe serialisation— `to_dict(safe=True)`.
 15.  `reset_statistics()`   — instance method.
 16.  Structured logging     — `green_score.computed`,
                                `green_score.invalid_input`,
                                `green_score.carbon_ignored` events.
 17.  Thread safety          — `RLock` around the mutable surface.
 18.  Backward compatibility — every v1 public symbol is preserved.

Backward compatibility notes:
  * `GreenScoreCalculator(alpha=..., beta=..., normalization=..., features=...)`
    works exactly as before; `features` defaults are unchanged.
  * `compute(...)` returns a `GreenScoreResult` whose `.score` equals the v1
    score under the same features.
  * `to_decision_record(result)` now emits physical units (v1's
    `latency_ms` / `energy_kwh` were the normalised values — behaviour change
    documented above).
  * `compute_green_score_detailed(...)` gains `features=` and
    `deployment_id=` kwargs (additive).
  * `features['normalization']` is retained in `DEFAULT_FEATURES` for
    compatibility but ignored; the `normalization=` constructor arg controls
    behaviour (v1 did the same — the flag was dead).
"""

from __future__ import annotations

import inspect
import logging
import math
import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "GreenScoreCalculator",
    "GreenScoreResult",
    "GreenScoreExplainer",
    "FederatedGreenAggregator",
    "FederatedGreenProfile",
    "NormalizationMethod",
    "Severity",
    "compute_green_score",
    "compute_green_score_detailed",
    "get_statistics",
    "reset_statistics",
    "DEFAULT_NORMALIZATION_RANGES",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class NormalizationMethod(Enum):
    """How to normalize unbounded inputs before scoring."""
    NONE = "none"
    MINMAX = "minmax"
    LOG1P = "log1p"
    MANUAL = "manual"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


_VALID_SEVERITIES = frozenset(s.value for s in Severity)


# =============================================================================
# Defaults
# =============================================================================

DEFAULT_NORMALIZATION_RANGES: Dict[str, Tuple[float, float]] = {
    "energy": (0.0, 100.0),       # Wh
    "latency": (0.0, 10_000.0),   # ms
    "carbon": (0.0, 10.0),        # kgCO2e
    "helium": (0.0, 1.0),         # units
}

#: Opt-in precision penalty scale. At lower precision the energy/latency
#: penalty weight is *shrunk* (toward zero) — a relative indicator that
#: reflects higher measurement uncertainty at lower bitwidth, not a claim
#: that quantised runs actually consume less energy.
_PRECISION_PENALTY_SCALE: Dict[str, float] = {
    "fp64": 1.0,
    "fp32": 1.0,
    "tf32": 0.95,
    "fp16": 0.85,
    "bf16": 0.80,
    "int8": 0.60,
    "int4": 0.45,
    "quantum_distilled": 0.45,
}


# =============================================================================
# Helpers
# =============================================================================

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce to a finite float; return `default` on any failure."""
    if isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return f if math.isfinite(f) else default


def _validate_input(
    value: Any, name: str,
) -> Tuple[float, Optional[str]]:
    """Return `(coerced_value, error_code_or_None)`."""
    if value is None:
        return 0.0, f"{name}_missing"
    if isinstance(value, bool):
        return 0.0, f"{name}_bool"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0, f"{name}_non_numeric"
    if math.isnan(f):
        return 0.0, f"{name}_nan"
    if math.isinf(f):
        return 0.0, f"{name}_inf"
    return f, None


def _finite_nonneg(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if math.isnan(f) or math.isinf(f) or f < 0:
        return None
    return f


def _json_safe(x: Any) -> Any:
    """Recursively coerce to a JSON-serialisable primitive."""
    if x is None or isinstance(x, (bool, int, str)):
        return x
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, datetime):
        return x.isoformat()
    if isinstance(x, Enum):
        return x.value
    if isinstance(x, dict):
        return {str(k): _json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set, frozenset)):
        return [_json_safe(v) for v in x]
    return repr(x)


def _filter_decision_record_kwargs(
    DecisionRecord: Any, candidates: Dict[str, Any],
) -> Dict[str, Any]:
    """Restrict `candidates` to the installed DecisionRecord signature."""
    try:
        sig = inspect.signature(DecisionRecord)
        accepted = set(sig.parameters.keys())
        return {k: v for k, v in candidates.items() if k in accepted}
    except (ValueError, TypeError):
        return dict(candidates)


def _check_bounds(
    accuracy: float, energy: float, latency: float,
    carbon_kg: float, helium_units: float,
    inputs_valid: bool,
) -> Tuple[bool, List[str]]:
    """Return `(bounds_ok, violations)`.

    If `inputs_valid` is False the caller has already collected validity
    issues; this function only adds *range* violations for the values that
    were parseable.
    """
    bad: List[str] = []
    if not inputs_valid:
        bad.append("inputs_invalid")
    if not 0.0 <= accuracy <= 1.0:
        bad.append(f"accuracy_out_of_range({accuracy:.4f})")
    for name, val in (
        ("energy", energy),
        ("latency", latency),
        ("carbon_kg", carbon_kg),
        ("helium_units", helium_units),
    ):
        if val < 0:
            bad.append(f"{name}_negative({val:.6f})")
    return (len(bad) == 0, bad)


def _normalize_minmax(
    value: float, lo: float, hi: float,
) -> float:
    if not (math.isfinite(lo) and math.isfinite(hi)) or hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def _normalize_log1p(value: float, max_value: float) -> float:
    if not math.isfinite(max_value) or max_value <= 0:
        return 0.0
    denom = math.log1p(max_value)
    if denom <= 0:
        return 0.0
    return max(0.0, min(1.0, math.log1p(max(0.0, value)) / denom))


def _validate_range(
    value: Any, name: str,
) -> Optional[Tuple[float, float]]:
    """Validate a `(lo, hi)` range entry; returns None if invalid."""
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        logger.warning(
            "normalization_ranges[%r] must be a (lo, hi) pair; got %r",
            name, value,
        )
        return None
    try:
        lo = float(value[0])
        hi = float(value[1])
    except (TypeError, ValueError):
        logger.warning(
            "normalization_ranges[%r] values must be numeric; got %r",
            name, value,
        )
        return None
    if not (math.isfinite(lo) and math.isfinite(hi)) or lo >= hi:
        logger.warning(
            "normalization_ranges[%r] must be finite with lo < hi; "
            "got (%s, %s)", name, lo, hi,
        )
        return None
    return (lo, hi)


# =============================================================================
# Thread-safe statistics counter
# =============================================================================

class _StatsCounter:
    """Minimal thread-safe counter with a bounded score history."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counts: Counter = Counter()
        self._scores: Deque[float] = deque(maxlen=1024)

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[key] += amount

    def record_score(self, score: float) -> None:
        with self._lock:
            self._scores.append(score)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            out: Dict[str, Any] = dict(self._counts)
            scores = list(self._scores)
        if scores:
            mean_s = sum(scores) / len(scores)
            out["score_stats"] = {
                "mean": mean_s,
                "min": min(scores),
                "max": max(scores),
                "window_size": len(scores),
            }
        return out

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()
            self._scores.clear()

    def recent_scores(self) -> List[float]:
        with self._lock:
            return list(self._scores)


#: Global counter for v1 module-level `get_statistics()` compatibility.
_GLOBAL_STATS = _StatsCounter()


# =============================================================================
# Federated aggregation (bounded + correct std + cached)
# =============================================================================

@dataclass
class FederatedGreenProfile:
    deployment_id: str
    workload_class: str
    mean_score: float
    std_score: float
    sample_count: int
    #: FIX: v1 used `default_factory=datetime.now().timestamp` which froze
    #: the value at class-definition time. `time.time` is a proper factory.
    timestamp: float = field(default_factory=time.time)


class FederatedGreenAggregator:
    """
    Aggregate anonymised green-score profiles across deployments.

    Combines variances with the law of total variance (within + between),
    not by averaging stds (v1 bug). Bounded; thread-safe; cached.
    """

    def __init__(self, max_profiles: int = 10_000) -> None:
        self._profiles: Deque[FederatedGreenProfile] = deque(maxlen=max_profiles)
        self._cache: Optional[Dict[str, Dict[str, float]]] = None
        self._lock = threading.RLock()

    @property
    def profiles(self) -> List[FederatedGreenProfile]:
        with self._lock:
            return list(self._profiles)

    def push(self, p: FederatedGreenProfile) -> None:
        with self._lock:
            self._profiles.append(p)
            self._cache = None

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            if self._cache is not None:
                return self._cache

            grouped: Dict[str, List[FederatedGreenProfile]] = defaultdict(list)
            for p in self._profiles:
                grouped[p.workload_class].append(p)

            result: Dict[str, Dict[str, float]] = {}
            for wc, profiles in grouped.items():
                total_w = sum(max(0, p.sample_count) for p in profiles)
                if total_w <= 0:
                    continue
                mean = sum(
                    p.mean_score * max(0, p.sample_count) for p in profiles
                ) / total_w
                within = sum(
                    max(0, p.sample_count) * (p.std_score ** 2)
                    for p in profiles
                )
                between = sum(
                    max(0, p.sample_count) * ((p.mean_score - mean) ** 2)
                    for p in profiles
                )
                var = max(0.0, (within + between) / total_w)
                result[wc] = {
                    "mean_score": mean,
                    "std_score": math.sqrt(var),
                    "sample_count": float(total_w),
                }
            self._cache = result
            return result


# =============================================================================
# GreenScoreResult
# =============================================================================

@dataclass
class GreenScoreResult:
    """
    Rich result of a green score computation.

    `score` holds the *effective* score (post-normalisation, including
    carbon/helium terms when enabled). `raw_score` preserves the v1
    formula applied to *raw* inputs, so ``compute_green_score(...)`` and
    ``calc.compute(...).score`` remain comparable when normalisation is
    disabled.

    The `raw_*` fields are always present so downstream consumers
    (notably `to_decision_record`) can emit physical units regardless of
    the normalisation mode.
    """
    # --- Required (v1 compat) ---
    score: float
    raw_score: float
    normalized: bool
    accuracy: float        # post-normalisation
    energy: float          # post-normalisation
    latency: float         # post-normalisation

    # --- Post-normalisation carbon / helium ---
    carbon_kg: float = 0.0
    helium_units: float = 0.0

    # --- Raw (pre-normalisation) inputs ---
    raw_accuracy: float = 0.0
    raw_energy_wh: float = 0.0
    raw_latency_ms: float = 0.0
    raw_carbon_kg: float = 0.0
    raw_helium_units: float = 0.0

    # --- Extended score variants ---
    raw_score_full: float = 0.0   # raw inputs, carbon + helium included

    # --- Input validity ---
    inputs_valid: bool = True

    # --- Weights ---
    alpha: float = 0.6
    beta: float = 0.2
    gamma: float = 0.0
    delta: float = 0.0

    # --- Uncertainty ---
    uncertainty: float = 0.0

    # --- Provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    source: str = "unknown"
    simulated: bool = False
    precision: Optional[str] = None
    deployment_id: Optional[str] = None

    # --- Verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)

    # --- Severity ---
    severity: str = Severity.INFO.value

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_float(self) -> float:
        """Backward-compatible float view (returns `score`)."""
        return self.score

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        if safe:
            for key in list(out.keys()):
                if isinstance(out[key], float) and not math.isfinite(out[key]):
                    out[key] = None
            if isinstance(out.get("explanation"), dict):
                out["explanation"] = _json_safe(out["explanation"])
            if isinstance(out.get("bounds_violations"), list):
                out["bounds_violations"] = [
                    str(v) for v in out["bounds_violations"]
                ]
        return out


# =============================================================================
# XAI
# =============================================================================

class GreenScoreExplainer:
    @staticmethod
    def explain(
        accuracy: float, energy: float, latency: float,
        alpha: float, beta: float,
        accuracy_term: float, energy_term: float, latency_term: float,
        score: float,
        carbon_term: float = 0.0,
        helium_term: float = 0.0,
        normalized: bool = False,
        normalization_method: str = NormalizationMethod.NONE.value,
        precision_multiplier: float = 1.0,
    ) -> Dict[str, Any]:
        reasons: List[str] = [
            "Score = α·accuracy − (1−α)·energy − β·latency "
            "− γ·carbon − δ·helium",
            f"α={alpha:.3f}, β={beta:.3f}.",
            f"accuracy term = {accuracy_term:+.6f} "
            f"({alpha:.3f} × {accuracy:.4f})",
            f"energy term   = {energy_term:+.6f} "
            f"({(1 - alpha) * precision_multiplier:.3f} × {energy:.4f})",
            f"latency term  = {latency_term:+.6f} "
            f"({beta * precision_multiplier:.3f} × {latency:.4f})",
        ]
        if carbon_term:
            reasons.append(f"carbon term   = {carbon_term:+.6f}")
        if helium_term:
            reasons.append(f"helium term   = {helium_term:+.6f}")
        reasons.append(f"final score   = {score:+.6f}")

        if precision_multiplier != 1.0:
            reasons.append(
                f"Precision scoring applied: energy/latency penalty "
                f"scaled ×{precision_multiplier:.2f} "
                f"(relative indicator only)."
            )

        if normalized:
            reasons.append(
                f"Inputs were normalized via '{normalization_method}' "
                "before scoring (fixes the unbounded-scale problem)."
            )
        else:
            reasons.append(
                "WARNING: normalization disabled — unbounded inputs "
                "(energy, latency) may dominate the score."
            )

        terms = {
            "accuracy": accuracy_term,
            "energy": energy_term,
            "latency": latency_term,
            "carbon": carbon_term,
            "helium": helium_term,
        }
        dominant = max(terms, key=lambda k: abs(terms[k]))
        reasons.append(f"Dominant contribution: '{dominant}'.")

        return {
            "headline": f"green_score = {score:+.6f}",
            "rationale": reasons,
            "contributing_factors": {
                "accuracy_term": accuracy_term,
                "energy_term": energy_term,
                "latency_term": latency_term,
                "carbon_term": carbon_term,
                "helium_term": helium_term,
                "precision_multiplier": precision_multiplier,
            },
            "confidence": 0.95 if normalized else 0.5,
            "normalized": normalized,
            "normalization_method": normalization_method,
        }


# =============================================================================
# ORIGINAL FUNCTION — preserved exactly
# =============================================================================

def compute_green_score(
    accuracy: float,
    energy: float,
    latency: float,
    alpha: float = 0.6,
    beta: float = 0.2,
) -> float:
    """
    Compute a green score. Higher is better.

    Backward-compatible: same signature, same formula, same return type.
    """
    return (alpha * accuracy) - ((1 - alpha) * energy) - (beta * latency)


# =============================================================================
# GreenScoreCalculator
# =============================================================================

class GreenScoreCalculator:
    """
    Enhanced green score calculator.

    Preserves the original formula exactly when all features are disabled.
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "normalization": False,       # retained for compat; ignored (see docstring)
        "carbon": False,
        "helium": False,
        "xai": True,
        "provenance": True,
        "uncertainty": True,
        "bounds_check": True,
        "hitl": True,
        "federated": True,
        "statistics": True,
        "precision_scoring": False,
    }

    #: HITL near-extremum margin (fraction of the achievable range).
    HITL_EXTREMUM_MARGIN = 0.05
    #: HITL z-score threshold for NONE-mode history-based review.
    HITL_ZSCORE_THRESHOLD = 3.0
    #: Minimum history size before z-score review is meaningful.
    HITL_MIN_HISTORY = 10

    def __init__(
        self,
        *,
        alpha: float = 0.6,
        beta: float = 0.2,
        gamma: float = 0.2,
        delta: float = 0.2,
        normalization: str = NormalizationMethod.NONE.value,
        normalization_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Weight validation (raise on truly invalid configurations) ---
        def _finite_weight(name: str, v: Any) -> float:
            if isinstance(v, bool):
                raise TypeError(f"{name} must be numeric, got bool")
            try:
                f = float(v)
            except (TypeError, ValueError):
                raise TypeError(
                    f"{name} must be numeric, got {type(v).__name__}"
                )
            if not math.isfinite(f):
                raise ValueError(f"{name} must be finite, got {f!r}")
            if f < 0:
                raise ValueError(f"{name} must be >= 0, got {f}")
            return f

        self.alpha = _finite_weight("alpha", alpha)
        self.beta = _finite_weight("beta", beta)
        self.gamma = _finite_weight("gamma", gamma)
        self.delta = _finite_weight("delta", delta)

        if self.alpha > 1.0:
            raise ValueError(
                f"alpha must be in [0, 1] for a well-formed score; "
                f"got {self.alpha}"
            )

        # --- Normalisation mode ---
        if normalization not in {m.value for m in NormalizationMethod}:
            raise ValueError(
                f"normalization must be one of "
                f"{[m.value for m in NormalizationMethod]}, "
                f"got {normalization!r}"
            )
        self.normalization = normalization

        # --- Normalisation ranges (validated) ---
        merged = {**DEFAULT_NORMALIZATION_RANGES}
        if normalization_ranges:
            for k, v in normalization_ranges.items():
                if k not in DEFAULT_NORMALIZATION_RANGES:
                    logger.warning(
                        "Unknown normalization range key: %r", k,
                    )
                    continue
                validated = _validate_range(v, k)
                if validated is not None:
                    merged[k] = validated
        self.normalization_ranges = merged

        # --- Other config ---
        self.deployment_id = deployment_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "GreenScoreCalculator: unknown feature flag(s) ignored: %s",
                sorted(unknown),
            )

        # --- Federated ---
        self.federated = FederatedGreenAggregator()

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[GreenScoreResult], Optional[bool]]
        ] = None

        # --- Statistics (instance-scoped) ---
        self._stats = _StatsCounter()
        self._history: Deque[GreenScoreResult] = deque(maxlen=1024)
        self._computations: int = 0
        self._last_contributed_computations: int = 0

        # --- One-shot warnings ---
        self._warned_carbon_ignored = False
        self._warned_helium_ignored = False

        # --- Concurrency ---
        self._lock = threading.RLock()

        logger.debug(
            "GreenScoreCalculator initialised (alpha=%s, beta=%s, "
            "normalization=%s, deployment=%s)",
            self.alpha, self.beta, self.normalization, self.deployment_id,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute(
        self,
        accuracy: float,
        energy: float,
        latency: float,
        *,
        carbon_kg: float = 0.0,
        helium_units: float = 0.0,
        accuracy_std: float = 0.0,
        energy_std: float = 0.0,
        latency_std: float = 0.0,
        precision: Optional[str] = None,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        source: str = "unknown",
        simulated: bool = False,
        workload_class: str = "default",
    ) -> GreenScoreResult:
        """
        Compute the enriched green score.

        With all features disabled, `result.score` equals the value the
        original function would have returned.
        """
        # --- Validate inputs (before coercion) ---
        acc, acc_err = _validate_input(accuracy, "accuracy")
        en, en_err = _validate_input(energy, "energy")
        lat, lat_err = _validate_input(latency, "latency")
        carb, carb_err = _validate_input(carbon_kg, "carbon_kg")
        he, he_err = _validate_input(helium_units, "helium_units")

        validity_errors = [
            e for e in (acc_err, en_err, lat_err, carb_err, he_err) if e
        ]
        inputs_valid = len(validity_errors) == 0

        # --- Bounds verification (opt-out via features['bounds_check']) ---
        if self.features.get("bounds_check", True):
            bounds_ok, violations = _check_bounds(
                acc, en, lat, carb, he, inputs_valid,
            )
        else:
            bounds_ok, violations = inputs_valid, (
                ["bounds_check_disabled"] if not inputs_valid else []
            )

        # --- Carbon / helium supplied-but-feature-off warning ---
        carbon_enabled = self.features.get("carbon", False)
        helium_enabled = self.features.get("helium", False)
        if carb > 0 and not carbon_enabled and not self._warned_carbon_ignored:
            logger.warning(
                "green_score.carbon_ignored",
                extra={
                    "carbon_kg": carb,
                    "hint": "set features={'carbon': True} to include "
                            "the carbon penalty in the score",
                },
            )
            self._warned_carbon_ignored = True
        if he > 0 and not helium_enabled and not self._warned_helium_ignored:
            logger.warning(
                "green_score.helium_ignored",
                extra={
                    "helium_units": he,
                    "hint": "set features={'helium': True} to include "
                            "the helium penalty in the score",
                },
            )
            self._warned_helium_ignored = True

        # --- Normalisation (opt-in via constructor arg) ---
        normalized = False
        norm_acc, norm_en, norm_lat, norm_carb, norm_he = acc, en, lat, carb, he

        if self.normalization == NormalizationMethod.MINMAX.value:
            normalized = True
            norm_en = _normalize_minmax(
                en, *self.normalization_ranges["energy"]
            )
            norm_lat = _normalize_minmax(
                lat, *self.normalization_ranges["latency"]
            )
            norm_carb = _normalize_minmax(
                carb, *self.normalization_ranges["carbon"]
            )
            norm_he = _normalize_minmax(
                he, *self.normalization_ranges["helium"]
            )
        elif self.normalization == NormalizationMethod.LOG1P.value:
            normalized = True
            norm_en = _normalize_log1p(
                en, self.normalization_ranges["energy"][1]
            )
            norm_lat = _normalize_log1p(
                lat, self.normalization_ranges["latency"][1]
            )
            norm_carb = _normalize_log1p(
                carb, self.normalization_ranges["carbon"][1]
            )
            norm_he = _normalize_log1p(
                he, self.normalization_ranges["helium"][1]
            )
        elif self.normalization == NormalizationMethod.MANUAL.value:
            normalized = True
            # FIX: MANUAL mode now normalises carbon and helium too
            # (v1 left them unbounded).
            norm_en = _normalize_minmax(
                en, *self.normalization_ranges["energy"]
            )
            norm_lat = _normalize_minmax(
                lat, *self.normalization_ranges["latency"]
            )
            norm_carb = _normalize_minmax(
                carb, *self.normalization_ranges["carbon"]
            )
            norm_he = _normalize_minmax(
                he, *self.normalization_ranges["helium"]
            )

        # --- Precision scoring (opt-in) ---
        precision_multiplier = 1.0
        if (
            self.features.get("precision_scoring", False)
            and isinstance(precision, str)
        ):
            precision_multiplier = _PRECISION_PENALTY_SCALE.get(
                precision.strip().lower(), 1.0,
            )

        # --- Score terms ---
        effective_energy_weight = (
            (1.0 - self.alpha) * precision_multiplier
        )
        effective_beta = self.beta * precision_multiplier

        accuracy_term = self.alpha * norm_acc
        energy_term = -(effective_energy_weight * norm_en)
        latency_term = -(effective_beta * norm_lat)
        carbon_term = -(self.gamma * norm_carb) if carbon_enabled else 0.0
        helium_term = -(self.delta * norm_he) if helium_enabled else 0.0

        score = (
            accuracy_term + energy_term + latency_term
            + carbon_term + helium_term
        )

        # --- Raw score (v1 formula on raw inputs) ---
        raw_score = (
            self.alpha * acc
            - (1.0 - self.alpha) * en
            - self.beta * lat
        )
        # --- Raw score with carbon / helium (when enabled) ---
        raw_score_full = (
            self.alpha * acc
            - (1.0 - self.alpha) * en
            - self.beta * lat
            - (self.gamma * carb if carbon_enabled else 0.0)
            - (self.delta * he if helium_enabled else 0.0)
        )

        # --- Uncertainty propagation ---
        uncertainty = 0.0
        if self.features.get("uncertainty", True):
            a_std = _safe_float(accuracy_std, 0.0)
            e_std = _safe_float(energy_std, 0.0)
            l_std = _safe_float(latency_std, 0.0)
            uncertainty = math.sqrt(
                (self.alpha * a_std) ** 2
                + (effective_energy_weight * e_std) ** 2
                + (effective_beta * l_std) ** 2
            )

        # --- Mode-aware HITL ---
        needs_review, review_reason = self._compute_hitl(
            score=score,
            normalized=normalized,
            bounds_ok=bounds_ok,
            violations=violations,
        )

        # --- Severity ---
        if not inputs_valid:
            severity = Severity.EMERGENCY.value
        elif not bounds_ok:
            severity = Severity.CRITICAL.value
        elif needs_review:
            severity = Severity.WARNING.value
        else:
            severity = Severity.INFO.value

        # --- Provenance (opt-out via features['provenance']) ---
        if not self.features.get("provenance", True):
            run_id = None
            task_id = None
            agent_id = None
            source = "unknown"
            simulated = False

        # --- Build result ---
        result = GreenScoreResult(
            score=score,
            raw_score=raw_score,
            normalized=normalized,
            accuracy=norm_acc,
            energy=norm_en,
            latency=norm_lat,
            carbon_kg=norm_carb,
            helium_units=norm_he,
            raw_accuracy=acc,
            raw_energy_wh=en,
            raw_latency_ms=lat,
            raw_carbon_kg=carb,
            raw_helium_units=he,
            raw_score_full=raw_score_full,
            inputs_valid=inputs_valid,
            alpha=self.alpha,
            beta=self.beta,
            gamma=self.gamma if carbon_enabled else 0.0,
            delta=self.delta if helium_enabled else 0.0,
            uncertainty=uncertainty,
            run_id=run_id,
            task_id=task_id,
            agent_id=agent_id,
            source=source,
            simulated=simulated,
            precision=precision,
            deployment_id=self.deployment_id,
            bounds_ok=bounds_ok,
            bounds_violations=violations,
            severity=severity,
            needs_review=needs_review,
            review_reason=review_reason,
        )

        # --- XAI ---
        if self.features.get("xai", True):
            result.explanation = GreenScoreExplainer.explain(
                accuracy=norm_acc, energy=norm_en, latency=norm_lat,
                alpha=self.alpha, beta=self.beta,
                accuracy_term=accuracy_term,
                energy_term=energy_term,
                latency_term=latency_term,
                score=score,
                carbon_term=carbon_term,
                helium_term=helium_term,
                normalized=normalized,
                normalization_method=self.normalization,
                precision_multiplier=precision_multiplier,
            )

        # --- HITL callback (annotating) ---
        if (
            needs_review
            and self.features.get("hitl", True)
            and self._hitl_callback is not None
        ):
            try:
                approved = self._hitl_callback(result)
                if approved is False:
                    result.review_reason = (
                        (result.review_reason or "")
                        + " [HITL: denied]"
                    )
                    logger.info(
                        "green_score.hitl_denied",
                        extra={"score": score, "severity": severity},
                    )
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)

        # --- Statistics (opt-out via features['statistics']) ---
        with self._lock:
            self._computations += 1
            if self.features.get("statistics", True):
                self._stats.increment("computations")
                if normalized:
                    self._stats.increment("normalized")
                if not inputs_valid:
                    self._stats.increment("invalid_inputs")
                self._stats.record_score(score)
                self._history.append(result)

            # --- Federated contribution (auto, throttled) ---
            if (
                self.features.get("federated", True)
                and self._computations % 25 == 0
            ):
                self._contribute_federated_locked(workload_class, dedupe=False)

        # --- Global shim counters (v1 compat) ---
        _GLOBAL_STATS.increment("computations")
        if normalized:
            _GLOBAL_STATS.increment("normalized")
        if not inputs_valid:
            _GLOBAL_STATS.increment("invalid")
        _GLOBAL_STATS.record_score(score)

        # --- Structured logging ---
        log_extra = {
            "score": score,
            "raw_score": raw_score,
            "raw_score_full": raw_score_full,
            "normalized": normalized,
            "inputs_valid": inputs_valid,
            "bounds_ok": bounds_ok,
            "needs_review": needs_review,
            "severity": severity,
            "alpha": self.alpha,
            "beta": self.beta,
            "precision": precision,
            "precision_multiplier": precision_multiplier,
            "deployment_id": self.deployment_id,
            "run_id": run_id,
            "task_id": task_id,
            "agent_id": agent_id,
        }
        if not inputs_valid:
            logger.warning(
                "green_score.invalid_input",
                extra={**log_extra, "errors": validity_errors},
            )
        elif needs_review:
            logger.info("green_score.hitl_review", extra=log_extra)
        else:
            logger.info("green_score.computed", extra=log_extra)

        return result

    # ------------------------------------------------------------------
    # HITL
    # ------------------------------------------------------------------

    def _compute_hitl(
        self,
        *,
        score: float,
        normalized: bool,
        bounds_ok: bool,
        violations: List[str],
    ) -> Tuple[bool, Optional[str]]:
        """Return `(needs_review, review_reason)` with mode-aware logic."""
        reasons: List[str] = []
        if not bounds_ok:
            reasons.append(f"bounds: {'; '.join(violations)}")

        if self.features.get("hitl", True):
            if normalized:
                min_v, max_v = self._mode_score_range()
                rng = max_v - min_v
                if rng > 0:
                    margin = self.HITL_EXTREMUM_MARGIN * rng
                    if score < min_v + margin:
                        reasons.append(
                            f"score {score:+.4f} near minimum "
                            f"{min_v:+.4f}"
                        )
                    elif score > max_v - margin:
                        reasons.append(
                            f"score {score:+.4f} near maximum "
                            f"{max_v:+.4f}"
                        )
            else:
                # Z-score against instance history (v1: fixed thresholds
                # fired on every non-normalised run).
                with self._lock:
                    history = [r.score for r in self._history]
                if len(history) >= self.HITL_MIN_HISTORY:
                    mean = sum(history) / len(history)
                    var = sum((s - mean) ** 2 for s in history) / len(history)
                    std = math.sqrt(var)
                    if std > 1e-9:
                        z = (score - mean) / std
                        if abs(z) > self.HITL_ZSCORE_THRESHOLD:
                            reasons.append(
                                f"score z={z:+.2f} vs history "
                                f"(mean={mean:+.4f})"
                            )

        return bool(reasons), ("; ".join(reasons) if reasons else None)

    def _mode_score_range(self) -> Tuple[float, float]:
        """Achievable (min, max) for the current mode and weights."""
        max_v = self.alpha  # perfect accuracy, zero costs
        min_v = -(1.0 - self.alpha) - self.beta
        if self.features.get("carbon", False):
            min_v -= self.gamma
        if self.features.get("helium", False):
            min_v -= self.delta
        return (min_v, max_v)

    # ------------------------------------------------------------------
    # Statistics / accessors
    # ------------------------------------------------------------------

    def compute_float(
        self,
        accuracy: float,
        energy: float,
        latency: float,
        *,
        mode: str = "score",
        **kwargs: Any,
    ) -> float:
        """
        Convenience: return only the float score.

        `mode="score"` (default) returns the effective score. `mode="raw"`
        returns the v1 formula applied to raw inputs (equivalent to
        `compute_green_score(accuracy, energy, latency)`).
        """
        result = self.compute(accuracy, energy, latency, **kwargs)
        if mode == "raw":
            return result.raw_score
        if mode == "score":
            return result.score
        raise ValueError(f"mode must be 'score' or 'raw', got {mode!r}")

    def get_statistics(self) -> Dict[str, Any]:
        """Instance-scoped statistics + configuration."""
        with self._lock:
            snap = self._stats.snapshot()
            history_len = len(self._history)
            hist_mean = (
                sum(r.score for r in self._history) / history_len
                if history_len else 0.0
            )
        out = {
            "analyzer_version": __version__,
            "deployment_id": self.deployment_id,
            "computations": snap.get("computations", 0),
            "normalized_computations": snap.get("normalized", 0),
            "invalid_inputs": snap.get("invalid_inputs", 0),
            "alpha": self.alpha,
            "beta": self.beta,
            "gamma": self.gamma,
            "delta": self.delta,
            "normalization": self.normalization,
            "features": dict(self.features),
            "history_len": history_len,
            "history_mean_score": hist_mean,
            "federated_profiles_pushed": len(self.federated.profiles),
        }
        if "score_stats" in snap:
            out["recent_score_stats"] = snap["score_stats"]
        return out

    def reset_statistics(self) -> None:
        """Clear this calculator's counters + history."""
        with self._lock:
            self._stats.reset()
            self._history.clear()
            self._computations = 0
            self._last_contributed_computations = 0

    def export(self) -> Dict[str, Any]:
        with self._lock:
            history = [r.to_dict(safe=True) for r in self._history]
        return {
            "analyzer_version": __version__,
            "statistics": self.get_statistics(),
            "history": history,
            "federated_aggregate": self.federated.aggregate(),
        }

    # ------------------------------------------------------------------
    # HITL / federated
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self,
        callback: Optional[Callable[[GreenScoreResult], Optional[bool]]],
    ) -> None:
        """
        Register a reviewer for extreme scores or bounds violations.

        The callback receives the *returned* `GreenScoreResult` and may
        mutate it (e.g. append to `review_reason`). Returning `False`
        appends `[HITL: denied]` to `review_reason`.

        Passing `None` clears the callback.
        """
        self._hitl_callback = callback

    def contribute_federated(
        self, workload_class: str = "default", *, dedupe: bool = True,
    ) -> None:
        """
        Push the current history as a federated profile.

        `dedupe=True` (default) skips the push when no new computations
        have occurred since the last contribution for this calculator,
        preventing duplicate profiles from repeated calls.

        The `std_score` written is the population std of the score
        history — NOT the propagated input uncertainty (v1 conflated the
        two).
        """
        with self._lock:
            self._contribute_federated_locked(workload_class, dedupe=dedupe)

    def _contribute_federated_locked(
        self, workload_class: str, *, dedupe: bool,
    ) -> None:
        if not self._history:
            return
        if dedupe and self._computations <= self._last_contributed_computations:
            return

        scores = [r.score for r in self._history]
        n = len(scores)
        if n == 0:
            return
        mean = sum(scores) / n
        var = sum((s - mean) ** 2 for s in scores) / n if n > 1 else 0.0
        std = math.sqrt(var)

        self.federated.push(FederatedGreenProfile(
            deployment_id=self.deployment_id,
            workload_class=workload_class,
            mean_score=mean,
            std_score=std,
            sample_count=n,
        ))
        self._last_contributed_computations = self._computations

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate()

    # ------------------------------------------------------------------
    # DecisionRecord emission
    # ------------------------------------------------------------------

    def to_decision_record(
        self,
        result: GreenScoreResult,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """
        Emit a DecisionRecord for a score result.

        Uses the RAW (`raw_*`) inputs so latency and energy are recorded in
        physical units — v1 wrote the *normalised* values into
        `latency_ms` / `energy_kwh`, silently corrupting every downstream
        aggregation in MINMAX mode.
        """
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug(
                    "DecisionRecord not importable; skipping emission "
                    "for %s", result.agent_id,
                )
                return None

        candidates: Dict[str, Any] = {
            "run_id": result.run_id or "",
            "timestamp": result.at,
            "task_id": result.task_id or "",
            "selected_action": f"green_score({result.score:+.4f})",
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": result.agent_id or "",
            "precision": result.precision,
            "quality_score": result.raw_accuracy,
            "latency_ms": result.raw_latency_ms,
            "energy_kwh": result.raw_energy_wh / 1000.0,
            "carbon_operational_kg": result.raw_carbon_kg,
            "explanation": (
                dict(result.explanation) if result.explanation else {}
            ),
            "provenance": {
                "source": result.source,
                "simulated": result.simulated,
                "normalized": result.normalized,
                "alpha": result.alpha,
                "beta": result.beta,
                "analyzer_version": __version__,
                "deployment_id": result.deployment_id,
                "severity": result.severity,
                "inputs_valid": result.inputs_valid,
            },
        }

        filtered = _filter_decision_record_kwargs(
            DecisionRecord, candidates,
        )
        try:
            return DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "DecisionRecord construction failed for %s: %s",
                result.agent_id, e,
            )
            return None


# =============================================================================
# Convenience: one-shot detailed score
# =============================================================================

def compute_green_score_detailed(
    accuracy: float,
    energy: float,
    latency: float,
    alpha: float = 0.6,
    beta: float = 0.2,
    *,
    carbon_kg: float = 0.0,
    helium_units: float = 0.0,
    normalization: str = NormalizationMethod.NONE.value,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    agent_id: Optional[str] = None,
    features: Optional[Dict[str, bool]] = None,
    deployment_id: str = "local",
) -> GreenScoreResult:
    """
    One-shot enriched score using a fresh calculator.

    `features` (new): caller-supplied feature-flag overrides. When
    `carbon_kg > 0` or `helium_units > 0` is supplied and the corresponding
    flag isn't explicitly set, it is auto-enabled (matching v1's implicit
    behaviour).
    """
    resolved_features: Dict[str, bool] = dict(features or {})
    if "carbon" not in resolved_features:
        resolved_features["carbon"] = carbon_kg > 0
    if "helium" not in resolved_features:
        resolved_features["helium"] = helium_units > 0

    calc = GreenScoreCalculator(
        alpha=alpha,
        beta=beta,
        normalization=normalization,
        deployment_id=deployment_id,
        features=resolved_features,
    )
    return calc.compute(
        accuracy, energy, latency,
        carbon_kg=carbon_kg,
        helium_units=helium_units,
        run_id=run_id,
        task_id=task_id,
        agent_id=agent_id,
    )


# =============================================================================
# Module-level backward-compatible shims
# =============================================================================

def get_statistics() -> Dict[str, Any]:
    """Global statistics (v1 compat)."""
    snap = _GLOBAL_STATS.snapshot()
    out: Dict[str, Any] = {
        "analyzer_version": __version__,
        "total_computations": snap.get("computations", 0),
        "normalized_computations": snap.get("normalized", 0),
        "invalid_inputs": snap.get("invalid", 0),
    }
    if "score_stats" in snap:
        out["score_stats"] = snap["score_stats"]
    return out


def reset_statistics() -> None:
    """Clear the global statistics counter (v1 compat)."""
    _GLOBAL_STATS.reset()


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import sys
    import types

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    print(f"  compute_green_score(0.95, 0.1, 0.5) = "
          f"{compute_green_score(0.95, 0.1, 0.5):+.4f}")
    print(f"  compute_green_score(0.95, 50.0, 5000.0) = "
          f"{compute_green_score(0.95, 50.0, 5000.0):+.4f}  "
          f"(scale bug: latency dominates)")

    calc_v1 = GreenScoreCalculator()
    r_v1 = calc_v1.compute(0.95, 50.0, 5000.0)
    assert abs(r_v1.score - compute_green_score(0.95, 50.0, 5000.0)) < 1e-9
    print(f"  calc.compute().score matches original: "
          f"{r_v1.score:+.4f}")

    # =================================================================
    # 2. NaN / None handling (inputs_valid=False, not silently 0.0)
    # =================================================================
    print("\n=== 2. Invalid input handling ===")
    p_inv = GreenScoreCalculator()
    r_nan = p_inv.compute(float("nan"), 10.0, 100.0)
    print(f"  NaN accuracy → inputs_valid={r_nan.inputs_valid}, "
          f"bounds_ok={r_nan.bounds_ok}, severity={r_nan.severity}")
    print(f"  bounds_violations: {r_nan.bounds_violations}")
    r_none = p_inv.compute(None, None, None)
    print(f"  All None → inputs_valid={r_none.inputs_valid}, "
          f"severity={r_none.severity}")

    # =================================================================
    # 3. Normalization (the fix)
    # =================================================================
    print("\n=== 3. Normalization ===")
    calc_minmax = GreenScoreCalculator(normalization="minmax")
    r_mm = calc_minmax.compute(0.95, 50.0, 5000.0)
    print(f"  normalized score: {r_mm.score:+.4f}  (balanced)")
    print(f"  raw score (v1):   {r_mm.raw_score:+.4f}  (latency dominates)")
    print(f"  raw_latency_ms:   {r_mm.raw_latency_ms}  (physical unit)")
    print(f"  latency (norm):   {r_mm.latency:.4f}  (0-1 fraction)")

    # =================================================================
    # 4. to_decision_record uses RAW physical units
    # =================================================================
    print("\n=== 4. DecisionRecord uses RAW units ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id, selected_action,
                     latency_ms, energy_kwh, quality_score,
                     carbon_operational_kg, explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.latency_ms = latency_ms
            self.energy_kwh = energy_kwh
            self.quality_score = quality_score
            self.carbon_operational_kg = carbon_operational_kg
            self.explanation = explanation
            self.provenance = provenance

    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    sys.modules["analysis"] = fake_mod
    try:
        r_dr = calc_minmax.compute(0.95, 50.0, 5000.0, run_id="run-1")
        dr = calc_minmax.to_decision_record(r_dr)
        print(f"  latency_ms in DR: {dr.latency_ms}  (expected 5000.0, "
              f"NOT {r_dr.latency})")
        print(f"  energy_kwh in DR: {dr.energy_kwh}  (expected 0.05, "
              f"NOT {r_dr.energy})")
        print(f"  quality_score:    {dr.quality_score}  (raw accuracy)")
    finally:
        del sys.modules["analysis"]

    # =================================================================
    # 5. HITL — NONE mode uses z-score (v1 fired on every run)
    # =================================================================
    print("\n=== 5. HITL (NONE mode: z-score vs history) ===")
    p_z = GreenScoreCalculator()
    for _ in range(15):
        p_z.compute(0.9, 10.0, 100.0)   # tight cluster
    r_outlier = p_z.compute(0.9, 50.0, 500.0)   # very different
    print(f"  outlier score: {r_outlier.score:+.4f}")
    print(f"  needs_review:  {r_outlier.needs_review}  (expected True)")
    print(f"  reason:        {r_outlier.review_reason}")

    # =================================================================
    # 6. HITL — normalised mode uses near-extremum thresholds
    # =================================================================
    print("\n=== 6. HITL (normalised mode: near-extremum) ===")
    p_ext = GreenScoreCalculator(normalization="minmax")
    # Achievable range: [-0.6, 0.6]. Near-maximum:
    r_good = p_ext.compute(1.0, 0.0, 0.0)   # perfect on all
    print(f"  perfect score: {r_good.score:+.4f}, "
          f"needs_review={r_good.needs_review}")
    # Typical range:
    r_typ = p_ext.compute(0.8, 30.0, 3000.0)
    print(f"  typical score: {r_typ.score:+.4f}, "
          f"needs_review={r_typ.needs_review}")

    # =================================================================
    # 7. MANUAL mode normalises carbon/helium (v1 bug)
    # =================================================================
    print("\n=== 7. MANUAL mode carbon/helium normalisation ===")
    p_manual = GreenScoreCalculator(
        normalization="manual",
        normalization_ranges={
            "carbon": (0.0, 1.0),      # 1 kgCO2e max
            "helium": (0.0, 0.1),      # 0.1 units max
        },
        features={"carbon": True, "helium": True},
    )
    r_m = p_manual.compute(0.9, 20.0, 1000.0,
                           carbon_kg=0.5, helium_units=0.05)
    print(f"  carbon_kg (norm):  {r_m.carbon_kg:.4f}  "
          f"(expected 0.5 — normalised against [0,1])")
    print(f"  helium_units (norm): {r_m.helium_units:.4f}  "
          f"(expected 0.5 — normalised against [0, 0.1])")

    # =================================================================
    # 8. Feature flag: bounds_check=False, provenance=False
    # =================================================================
    print("\n=== 8. Feature flags honoured ===")
    p_nf = GreenScoreCalculator(
        features={"bounds_check": False, "provenance": False},
    )
    r_nf = p_nf.compute(2.5, -5.0, 100.0, run_id="run-2", agent_id="A")
    print(f"  bounds_check=False → bounds_ok={r_nf.bounds_ok}  "
          f"(expected True, check skipped)")
    print(f"  provenance=False   → run_id={r_nf.run_id}  "
          f"(expected None)")

    # =================================================================
    # 9. Carbon warning when feature off but value supplied
    # =================================================================
    print("\n=== 9. Carbon warning when feature off ===")
    p_cw = GreenScoreCalculator()
    # First call triggers the warning
    r_cw = p_cw.compute(0.9, 10.0, 100.0, carbon_kg=5.0)
    print(f"  carbon_kg supplied but features['carbon']=False:")
    print(f"    score: {r_cw.score:+.4f} (carbon dropped)")
    print(f"    raw_carbon_kg preserved: {r_cw.raw_carbon_kg}")

    # =================================================================
    # 10. Precision scoring (opt-in)
    # =================================================================
    print("\n=== 10. Precision scoring ===")
    p_fp32 = GreenScoreCalculator(
        features={"precision_scoring": True},
    )
    r_fp32 = p_fp32.compute(0.9, 50.0, 5000.0, precision="fp32")
    r_int8 = p_fp32.compute(0.9, 50.0, 5000.0, precision="int8")
    print(f"  fp32 score: {r_fp32.score:+.4f}  (mult=1.0)")
    print(f"  int8 score: {r_int8.score:+.4f}  (mult=0.60, less penalty)")

    # =================================================================
    # 11. Instance stats isolation
    # =================================================================
    print("\n=== 11. Instance stats isolation ===")
    ca = GreenScoreCalculator()
    cb = GreenScoreCalculator()
    ca.compute(0.9, 10, 100)
    ca.compute(0.9, 10, 100)
    cb.compute(0.9, 10, 100)
    print(f"  ca.computations: {ca.get_statistics()['computations']}")
    print(f"  cb.computations: {cb.get_statistics()['computations']}")
    print(f"  global shim:     {get_statistics()['total_computations']}")

    # =================================================================
    # 12. Federated std aggregation (law of total variance)
    # =================================================================
    print("\n=== 12. Federated std aggregation ===")
    p_fed = GreenScoreCalculator()
    # Two clusters with distinct means
    for _ in range(10):
        p_fed.compute(0.9, 10.0, 100.0)   # mean ~ -4
    p_fed.contribute_federated("cluster-A")
    # Different cluster
    p_fed2 = GreenScoreCalculator(deployment_id="other")
    for _ in range(10):
        p_fed2.compute(0.9, 50.0, 5000.0)   # mean ~ very different
    p_fed2.contribute_federated("cluster-A")
    # Merge by sharing the aggregator
    p_fed.federated = p_fed2.federated
    agg = p_fed.get_federated_aggregate()
    print(f"  cluster-A mean: {agg['cluster-A']['mean_score']:+.4f}")
    print(f"  cluster-A std:  {agg['cluster-A']['std_score']:.4f}  "
          f"(combined via total variance — NOT averaged)")

    # =================================================================
    # 13. to_dict(safe=True) with non-serializable explanation
    # =================================================================
    print("\n=== 13. to_dict(safe=True) ===")
    p_safe = GreenScoreCalculator()
    r_safe = p_safe.compute(0.9, 10.0, 100.0)
    r_safe.explanation["custom_field"] = object()   # poison it
    try:
        json.dumps(r_safe.to_dict(safe=True))
        print("  json.dumps(to_dict(safe=True)): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 14. HITL callback denial annotation
    # =================================================================
    print("\n=== 14. HITL callback denial ===")
    p_h = GreenScoreCalculator(normalization="minmax")
    p_h.set_hitl_callback(lambda r: False)  # deny
    r_h = p_h.compute(1.0, 0.0, 0.0)   # near-max, triggers review
    print(f"  needs_review: {r_h.needs_review}")
    print(f"  reason: {r_h.review_reason}")
    print(f"  contains '[HITL: denied]': "
          f"{'[HITL: denied]' in (r_h.review_reason or '')}")

    # =================================================================
    # 15. Full statistics dump
    # =================================================================
    print("\n=== 15. Statistics ===")
    print(json.dumps(p_fed.get_statistics(), indent=2, default=str))
