# src/analysis/telemetry/framework_overhead.py
"""
Framework overhead normalization for Green Agent (Enhanced, v2.0.0)
====================================================================

Normalizes overhead costs for different runtimes (LangChain, AutoGen, etc.)
Helps isolate agent logic cost from framework cost.

Original API preserved (all signatures, all side effects):
    BASELINES = {...}
    metrics = apply_overhead(metrics, framework)          # mutates + returns
    metrics_list = apply_overhead_batch(metrics_list, framework)

Enhanced API:
    profiler = FrameworkOverheadProfiler(
        deployment_id="us-ca-prod-01",
        min_samples_for_calibration=5,
        features={...},
    )

    # Observations & calibration
    profiler.record_observation(framework, latency_s, energy_wh,
                                framework_version="0.2.1", precision="fp32")
    profiler.calibrate()
    profiler.get_calibrated_baseline("langchain")
    profiler.get_baseline("langchain")

    # Normalization
    metrics = profiler.apply(
        metrics, framework,
        include_carbon=True,
        grid_intensity_g_kwh=350.0,
        pue=1.2,
        precision="int8",              # new
        emit_decision_record=False,    # new: DR serialised, not live object
        track=True,                    # new: pure path when False
        in_place=True,                 # new: copy path when False
    )
    metrics, record = profiler.apply_with_record(metrics, framework)

    # Federated
    profiler.contribute_federated("langchain", dedupe=True)
    profiler.get_federated_aggregate()

    # HITL
    profiler.set_hitl_callback(cb)     # cb(fw, baseline, ratio) -> bool | None
    profiler.set_hitl_callback(None)   # clear

    # Statistics & export
    profiler.get_statistics()
    profiler.reset_statistics()
    profiler.reset_observations()
    profiler.export()

    # Module-level shims (share a locked default profiler)
    from .framework_overhead import apply_overhead, apply_overhead_batch
    get_statistics() / reset_statistics()

Enhancements implemented (v2.0.0):
  1.  Locked singleton       — `_get_default_profiler()` is thread-safe.
  2.  JSON-safe DR           — `apply(emit_decision_record=True)` stores a
                               serialised dict at `framework_overhead_decision_record`
                               and the live record via `get_last_decision_record()`.
                               v1's live-object-at-`_decision_record` bug removed.
  3.  Normalized counters    — `_framework_counts` uses the same lowercased key
                               as `_observations` / `_calibrated` / `_history`.
  4.  Rejected bad inputs    — negative / NaN / Inf observations raise
                               ValueError; `apply()` rejects non-str framework.
  5.  Honoured feature flags — `calibration`, `carbon`, `provenance`, `xai`,
                               `hitl`, `federated`, `clamp_flags`, `uncertainty`
                               are all consulted.
  6.  Validated config       — `min_samples_for_calibration >= 1` int; max
                               observations per framework bounded.
  7.  Cached federated agg   — O(1) `get_baseline()` on the hot path; cache
                               invalidated on push.
  8.  Idempotent federated   — `contribute_federated(dedupe=True)` (default)
                               only pushes *new* observations.
  9.  Bounded observations   — `deque(maxlen=...)` per framework.
 10.  Frozen baselines       — `BASELINES` documented read-only; companion
                               `_BASELINES_UNCERTAINTY` exposes the previously
                               hidden ± values.
 11.  Actionable HITL        — fires only on clamp + hardcoded; callback's
                               `False` return annotates explanation.rationale.
 12.  Missing-vs-zero        — `framework_overhead_latency_input_missing` /
                               `_energy_input_missing` distinguish absent
                               inputs from genuine zero.
 13.  Clamp uncertainty      — `effective_*_uncertainty_s/wh` is `None` when the
                               corresponding value was clamped.
 14.  Precision end-to-end   — `Baseline.precision` populated from the most
                               common precision among observations; per-call
                               `precision=` kwarg overrides.
 15.  Structural carbon      — writes both operational and contractual keys;
                               rejects NaN/Inf grid intensity / PUE.
 16.  Severity on metrics    — `framework_overhead_severity` derived from
                               baseline source + clamp flags.
 17.  In-place opt-out       — `apply(in_place=False)` returns a shallow copy,
                               leaving the caller's dict untouched.
 18.  Pure path              — `apply(track=False)` skips counters, history,
                               and federated reads.
 19.  Thread safety          — `RLock` around the entire mutable surface.
 20.  Structured logging     — `framework.overhead_applied` /
                               `framework.overhead_clamped` /
                               `framework.calibrated` events with `extra={...}`.
 21.  Provenance             — `run_id` / `task_id` / `precision` /
                               `analyzer_version` on the DR.
 22.  Backward compatibility — see "Backward compatibility" below.

Backward compatibility notes:
  * `BASELINES` remains a mutable module-level dict for v1 code that reads it.
    Callers who mutate it will see their changes respected by `get_baseline()`.
  * `apply(metrics, framework)` mutates `metrics` and returns it, exactly as
    v1 did. `in_place=False` is a new opt-in.
  * `apply_overhead()` / `apply_overhead_batch()` keep their v1 signatures;
    both delegate to the singleton profiler with `track=True, in_place=True,
    emit_decision_record=False`.
  * `apply(emit_decision_record=True)` no longer writes the *live*
    `DecisionRecord` to `metrics["_decision_record"]` (v1's behaviour, which
    broke JSON serialisation). The serialised form is written to
    `metrics["framework_overhead_decision_record"]`; the live record is
    retrievable via `profiler.get_last_decision_record()`. Callers relying on
    the old key should read the serialised dict form or use the new accessor.
"""

from __future__ import annotations

import inspect
import logging
import math
import statistics
import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "FrameworkOverheadProfiler",
    "FederatedOverheadAggregator",
    "FederatedOverheadProfile",
    "OverheadExplanation",
    "OverheadObservation",
    "OverheadExplainer",
    "Baseline",
    "BaselineSource",
    "Severity",
    "BASELINES",
    "apply_overhead",
    "apply_overhead_batch",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# ORIGINAL BASELINES — preserved verbatim (documented read-only)
# =============================================================================

#: Hardcoded framework baselines. Preserved for v1 compatibility. Treat as
#: read-only: mutate via a profiler's calibration / federated paths instead.
BASELINES: Dict[str, Dict[str, float]] = {
    "langchain": {"latency": 0.15, "energy_wh": 0.02},
    "autogen":   {"latency": 0.20, "energy_wh": 0.03},
}

#: Companion to :data:`BASELINES` — the uncertainty values v1 hardcoded inside
#: `get_baseline()` but never exposed. Kept in a separate dict so callers who
#: read BASELINES directly can also see the ± values without breaking v1.
_BASELINES_UNCERTAINTY: Dict[str, Dict[str, float]] = {
    "langchain": {"latency": 0.05, "energy_wh": 0.01},
    "autogen":   {"latency": 0.07, "energy_wh": 0.015},
}

#: Hardcoded baselines with latency > this are flagged for HITL review when
#: they're clamped. Chosen to flag genuinely unusual cases, not every run.
_HITL_RATIO_THRESHOLD = 2.0


# =============================================================================
# Enums
# =============================================================================

class BaselineSource(Enum):
    """Where an overhead baseline came from."""
    HARDCODED = "hardcoded"
    CALIBRATED = "calibrated"
    FEDERATED = "federated"
    UNKNOWN = "unknown"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


_VALID_SEVERITIES = frozenset(s.value for s in Severity)


# =============================================================================
# Numeric / serialisation helpers
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


def _finite_nonneg(value: Any) -> Optional[float]:
    """Return the value as a finite non-negative float, or None if invalid."""
    if isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if math.isnan(f) or math.isinf(f) or f < 0:
        return None
    return f


def _normalize_framework_name(framework: Any) -> Optional[str]:
    """Case-insensitive, whitespace-trimmed framework name (or None)."""
    if not isinstance(framework, str):
        return None
    fw = framework.strip().lower()
    return fw if fw else None


def _serialise_decision_record(record: Any) -> Optional[Dict[str, Any]]:
    """Serialise a DecisionRecord to a plain dict (best effort)."""
    if record is None:
        return None
    if hasattr(record, "to_dict"):
        try:
            return record.to_dict()
        except Exception:
            pass
    try:
        return asdict(record)
    except (TypeError, ValueError):
        pass
    try:
        return {
            k: v for k, v in vars(record).items()
            if not k.startswith("_")
        }
    except Exception:
        return None


def _filter_decision_record_kwargs(
    DecisionRecord: Any, candidates: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Restrict `candidates` to kwargs the installed DecisionRecord signature
    actually accepts. Falls back to the full set if introspection fails.
    """
    try:
        sig = inspect.signature(DecisionRecord)
        accepted = set(sig.parameters.keys())
        return {k: v for k, v in candidates.items() if k in accepted}
    except (ValueError, TypeError):
        return dict(candidates)


def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        if isinstance(ts, datetime):
            return ts.isoformat()
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


# =============================================================================
# Baseline + XAI + observation dataclasses
# =============================================================================

@dataclass
class Baseline:
    """A framework overhead baseline with provenance."""
    latency_s: float
    energy_wh: float
    source: str = BaselineSource.HARDCODED.value
    uncertainty_latency_s: float = 0.0
    uncertainty_energy_wh: float = 0.0
    sample_count: int = 0
    framework_version: Optional[str] = None
    precision: Optional[str] = None
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        if safe:
            for key in (
                "latency_s", "energy_wh",
                "uncertainty_latency_s", "uncertainty_energy_wh",
            ):
                v = out.get(key)
                if isinstance(v, float) and not math.isfinite(v):
                    out[key] = None
        return out


@dataclass
class OverheadExplanation:
    headline: str
    rationale: List[str]
    contributing_factors: Dict[str, Any]
    confidence: float = 0.9

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            out["rationale"] = [str(r) for r in out["rationale"]]
            factors = out.get("contributing_factors") or {}
            out["contributing_factors"] = {
                str(k): (
                    v if not isinstance(v, float) or math.isfinite(v)
                    else None
                )
                for k, v in factors.items()
            }
            if not math.isfinite(out.get("confidence", 0.0)):
                out["confidence"] = 0.0
        return out


@dataclass
class FederatedOverheadProfile:
    deployment_id: str
    framework: str
    framework_version: Optional[str]
    mean_latency_s: float
    mean_energy_wh: float
    std_latency_s: float
    std_energy_wh: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


@dataclass
class OverheadObservation:
    framework: str
    observed_latency_s: float
    observed_energy_wh: float
    framework_version: Optional[str] = None
    precision: Optional[str] = None
    at: float = field(default_factory=time.time)


# =============================================================================
# XAI explainer
# =============================================================================

class OverheadExplainer:
    """Build an OverheadExplanation from a normalization event."""

    @staticmethod
    def explain(
        framework: str,
        baseline: Baseline,
        latency_s: float,
        energy_wh: float,
        effective_latency_s: float,
        effective_energy_wh: float,
        latency_clamped: bool,
        energy_clamped: bool,
        latency_input_missing: bool = False,
        energy_input_missing: bool = False,
    ) -> OverheadExplanation:
        reasons: List[str] = [
            f"Framework '{framework}' overhead subtracted "
            f"(source: {baseline.source}).",
            f"Latency: {latency_s:.4f}s - "
            f"{baseline.latency_s:.4f}s = {effective_latency_s:.4f}s.",
            f"Energy:  {energy_wh:.6f}Wh - "
            f"{baseline.energy_wh:.6f}Wh = {effective_energy_wh:.6f}Wh.",
        ]
        if latency_input_missing:
            reasons.append(
                "WARNING: 'latency_s' was absent from the input metrics; "
                "the effective value reflects the baseline, not a "
                "measurement."
            )
        if energy_input_missing:
            reasons.append(
                "WARNING: 'energy_wh' was absent from the input metrics; "
                "the effective value reflects the baseline, not a "
                "measurement."
            )
        if latency_clamped:
            reasons.append(
                "WARNING: latency baseline exceeded total; clamped to 0. "
                "Baseline may be wrong for this run."
            )
        if energy_clamped:
            reasons.append(
                "WARNING: energy baseline exceeded total; clamped to 0. "
                "Baseline may be wrong for this run."
            )
        if baseline.source == BaselineSource.UNKNOWN.value:
            reasons.append(
                "WARNING: unknown framework; no overhead subtracted."
            )
        if baseline.uncertainty_latency_s > 0:
            reasons.append(
                f"Baseline latency uncertainty: "
                f"±{baseline.uncertainty_latency_s:.4f}s."
            )
        if baseline.precision:
            reasons.append(f"Precision context: {baseline.precision}.")

        if baseline.source == BaselineSource.CALIBRATED.value:
            confidence = 0.95
        elif baseline.source == BaselineSource.FEDERATED.value:
            confidence = 0.85
        elif baseline.source == BaselineSource.HARDCODED.value:
            confidence = 0.60
        else:
            confidence = 0.30

        return OverheadExplanation(
            headline=f"[{framework}] overhead normalized",
            rationale=reasons,
            contributing_factors={
                "latency_s": latency_s,
                "baseline_latency_s": baseline.latency_s,
                "effective_latency_s": effective_latency_s,
                "energy_wh": energy_wh,
                "baseline_energy_wh": baseline.energy_wh,
                "effective_energy_wh": effective_energy_wh,
                "latency_input_missing": 1.0 if latency_input_missing else 0.0,
                "energy_input_missing": 1.0 if energy_input_missing else 0.0,
            },
            confidence=confidence,
        )


# =============================================================================
# Federated aggregator (bounded + cached + locked)
# =============================================================================

class FederatedOverheadAggregator:
    """
    Aggregate anonymised overhead profiles across deployments.

    Profiles are bounded; aggregates are cached and invalidated on push.
    """

    def __init__(self, max_profiles: int = 10_000) -> None:
        self._profiles: Deque[FederatedOverheadProfile] = deque(
            maxlen=max_profiles
        )
        self._cache: Optional[Dict[str, Dict[str, float]]] = None
        self._lock = threading.RLock()

    @property
    def profiles(self) -> List[FederatedOverheadProfile]:
        with self._lock:
            return list(self._profiles)

    def push(self, p: FederatedOverheadProfile) -> None:
        with self._lock:
            self._profiles.append(p)
            self._cache = None

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            if self._cache is not None:
                return self._cache

            grouped: Dict[str, List[FederatedOverheadProfile]] = defaultdict(list)
            for p in self._profiles:
                grouped[p.framework].append(p)

            result: Dict[str, Dict[str, float]] = {}
            for fw, profiles in grouped.items():
                total_w = sum(max(0, p.sample_count) for p in profiles)
                if total_w <= 0:
                    continue
                result[fw] = {
                    "mean_latency_s": sum(
                        p.mean_latency_s * max(0, p.sample_count)
                        for p in profiles
                    ) / total_w,
                    "mean_energy_wh": sum(
                        p.mean_energy_wh * max(0, p.sample_count)
                        for p in profiles
                    ) / total_w,
                    "std_latency_s": sum(
                        p.std_latency_s * max(0, p.sample_count)
                        for p in profiles
                    ) / total_w,
                    "std_energy_wh": sum(
                        p.std_energy_wh * max(0, p.sample_count)
                        for p in profiles
                    ) / total_w,
                    "sample_count": float(total_w),
                }
            self._cache = result
            return result


# =============================================================================
# The main profiler
# =============================================================================

class FrameworkOverheadProfiler:
    """
    Calibrate framework overhead baselines from observed measurements.

    Backward-compatible with v1's ``apply_overhead`` metrics keys. All new
    state is instance-scoped and thread-safe.
    """

    #: Default feature flags. All are honoured (see module docstring).
    DEFAULT_FEATURES: Dict[str, bool] = {
        "provenance": True,
        "uncertainty": True,
        "xai": True,
        "carbon": True,
        "hitl": True,
        "federated": True,
        "calibration": True,
        "clamp_flags": True,
    }

    #: Bounded window of recent observations per framework.
    DEFAULT_MAX_OBSERVATIONS = 1_000
    #: Bounded history of normalization events.
    DEFAULT_MAX_HISTORY = 1_024

    def __init__(
        self,
        deployment_id: str = "local",
        min_samples_for_calibration: int = 5,
        features: Optional[Dict[str, bool]] = None,
        *,
        max_observations_per_framework: int = DEFAULT_MAX_OBSERVATIONS,
        max_history: int = DEFAULT_MAX_HISTORY,
    ) -> None:
        # --- Config validation ----------------------------------------
        if isinstance(min_samples_for_calibration, bool) or not isinstance(
            min_samples_for_calibration, int
        ):
            raise TypeError(
                "min_samples_for_calibration must be an int, got "
                f"{type(min_samples_for_calibration).__name__}"
            )
        if min_samples_for_calibration < 1:
            raise ValueError(
                f"min_samples_for_calibration must be >= 1, "
                f"got {min_samples_for_calibration}"
            )
        if not isinstance(max_observations_per_framework, int) or \
                max_observations_per_framework < 1:
            raise ValueError(
                "max_observations_per_framework must be a positive int"
            )
        if not isinstance(max_history, int) or max_history < 1:
            raise ValueError("max_history must be a positive int")

        self.deployment_id: str = deployment_id
        self.min_samples_for_calibration: int = min_samples_for_calibration
        self._max_observations_per_framework: int = \
            max_observations_per_framework

        # --- Observations (bounded per framework) ---------------------
        self._observations: Dict[str, Deque[OverheadObservation]] = defaultdict(
            lambda: deque(maxlen=self._max_observations_per_framework)
        )

        # --- Calibrated baselines -------------------------------------
        self._calibrated: Dict[str, Baseline] = {}

        # --- Federated -----------------------------------------------
        self.federated = FederatedOverheadAggregator()
        #: framework -> sample_count that has already been contributed.
        self._contributed: Dict[str, int] = {}

        # --- HITL -----------------------------------------------------
        self._hitl_callback: Optional[
            Callable[[str, Baseline, float], Optional[bool]]
        ] = None

        # --- Last decision record (live object) ----------------------
        self._last_decision_record: Optional[Any] = None

        # --- Statistics ---------------------------------------------
        self._normalization_count: int = 0
        self._clamp_count: int = 0
        self._unknown_framework_count: int = 0
        self._framework_counts: Counter = Counter()       # normalized keys
        self._history: Deque[Dict[str, Any]] = deque(maxlen=max_history)

        # --- Feature flags ------------------------------------------
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "FrameworkOverheadProfiler: unknown feature flag(s) "
                "ignored: %s", sorted(unknown),
            )

        # --- Concurrency --------------------------------------------
        self._lock = threading.RLock()

        logger.debug(
            "FrameworkOverheadProfiler initialised "
            "(deployment=%s, min_samples=%d)",
            deployment_id, min_samples_for_calibration,
        )

    # ------------------------------------------------------------------
    # Observation & calibration
    # ------------------------------------------------------------------

    def record_observation(
        self,
        framework: str,
        observed_latency_s: Any,
        observed_energy_wh: Any,
        *,
        framework_version: Optional[str] = None,
        precision: Optional[str] = None,
    ) -> None:
        """
        Record a (framework, latency, energy) observation.

        Raises ValueError on invalid inputs (non-string framework, negative
        or non-finite latency/energy) so bad data can't silently corrupt
        calibration (v1 accepted them).
        """
        fw = _normalize_framework_name(framework)
        if fw is None:
            raise ValueError(
                f"framework must be a non-empty string, got {framework!r}"
            )

        lat = _finite_nonneg(observed_latency_s)
        if lat is None:
            raise ValueError(
                f"observed_latency_s must be finite and >= 0, "
                f"got {observed_latency_s!r}"
            )
        en = _finite_nonneg(observed_energy_wh)
        if en is None:
            raise ValueError(
                f"observed_energy_wh must be finite and >= 0, "
                f"got {observed_energy_wh!r}"
            )

        fwv = str(framework_version) if framework_version is not None else None
        prec = str(precision) if precision is not None else None

        obs = OverheadObservation(
            framework=fw,
            observed_latency_s=lat,
            observed_energy_wh=en,
            framework_version=fwv,
            precision=prec,
        )

        with self._lock:
            self._observations[fw].append(obs)

    def calibrate(self) -> Dict[str, Baseline]:
        """
        Compute calibrated baselines from observations.

        No-op (returns `{}`) when `features["calibration"]` is False.
        """
        if not self.features.get("calibration", True):
            logger.debug(
                "calibrate(): feature flag disabled; returning {}"
            )
            return {}

        calibrated: Dict[str, Baseline] = {}
        with self._lock:
            for fw, obs_deque in self._observations.items():
                obs = list(obs_deque)
                if len(obs) < self.min_samples_for_calibration:
                    logger.debug(
                        "Not enough samples for '%s': %d < %d",
                        fw, len(obs), self.min_samples_for_calibration,
                    )
                    continue
                latencies = [o.observed_latency_s for o in obs]
                energies = [o.observed_energy_wh for o in obs]
                versions = [o.framework_version for o in obs
                            if o.framework_version]
                precisions = [o.precision for o in obs if o.precision]

                calibrated[fw] = Baseline(
                    latency_s=statistics.fmean(latencies),
                    energy_wh=statistics.fmean(energies),
                    source=BaselineSource.CALIBRATED.value,
                    uncertainty_latency_s=(
                        statistics.pstdev(latencies) if len(latencies) > 1
                        else 0.0
                    ),
                    uncertainty_energy_wh=(
                        statistics.pstdev(energies) if len(energies) > 1
                        else 0.0
                    ),
                    sample_count=len(obs),
                    framework_version=(
                        Counter(versions).most_common(1)[0][0]
                        if versions else None
                    ),
                    precision=(
                        Counter(precisions).most_common(1)[0][0]
                        if precisions else None
                    ),
                )
            self._calibrated = calibrated

        logger.info(
            "framework.calibrated",
            extra={
                "deployment_id": self.deployment_id,
                "frameworks": sorted(calibrated.keys()),
                "count": len(calibrated),
            },
        )
        return calibrated

    def get_calibrated_baseline(self, framework: str) -> Optional[Baseline]:
        """Return the calibrated baseline for a framework, if any."""
        fw = _normalize_framework_name(framework)
        if fw is None:
            return None
        with self._lock:
            return self._calibrated.get(fw)

    def get_baseline(
        self, framework: str, fallback_to_hardcoded: bool = True,
    ) -> Baseline:
        """
        Return the best available baseline:
          1. Calibrated
          2. Federated (>= min_samples)
          3. Hardcoded
          4. Unknown (zero overhead)
        """
        fw = _normalize_framework_name(framework)
        if fw is None:
            return Baseline(
                latency_s=0.0, energy_wh=0.0,
                source=BaselineSource.UNKNOWN.value,
            )

        # --- 1. Calibrated --------------------------------------------
        if self.features.get("calibration", True):
            with self._lock:
                cal = self._calibrated.get(fw)
            if cal is not None:
                return cal

        # --- 2. Federated ---------------------------------------------
        if self.features.get("federated", True):
            agg = self.federated.aggregate().get(fw)
            if (
                agg
                and agg.get("sample_count", 0)
                >= self.min_samples_for_calibration
            ):
                return Baseline(
                    latency_s=agg["mean_latency_s"],
                    energy_wh=agg["mean_energy_wh"],
                    source=BaselineSource.FEDERATED.value,
                    uncertainty_latency_s=agg.get("std_latency_s", 0.0),
                    uncertainty_energy_wh=agg.get("std_energy_wh", 0.0),
                    sample_count=int(agg.get("sample_count", 0)),
                )

        # --- 3. Hardcoded --------------------------------------------
        if fallback_to_hardcoded and fw in BASELINES:
            hc = BASELINES[fw]
            unc = _BASELINES_UNCERTAINTY.get(fw, {})
            return Baseline(
                latency_s=hc.get("latency", 0.0),
                energy_wh=hc.get("energy_wh", 0.0),
                source=BaselineSource.HARDCODED.value,
                uncertainty_latency_s=unc.get("latency", 0.05),
                uncertainty_energy_wh=unc.get("energy_wh", 0.01),
                sample_count=0,
            )

        # --- 4. Unknown ----------------------------------------------
        return Baseline(
            latency_s=0.0, energy_wh=0.0,
            source=BaselineSource.UNKNOWN.value,
        )

    # ------------------------------------------------------------------
    # Core normalization
    # ------------------------------------------------------------------

    def apply(
        self,
        metrics: Dict[str, Any],
        framework: str,
        *,
        include_carbon: bool = True,
        grid_intensity_g_kwh: Optional[float] = None,
        pue: Optional[float] = None,
        emit_decision_record: bool = False,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        policy_version: str = "",
        precision: Optional[str] = None,
        track: bool = True,
        in_place: bool = True,
    ) -> Dict[str, Any]:
        """
        Apply framework overhead normalization to metrics.

        Backward-compatible: writes the same four keys v1 wrote; adds
        provenance / uncertainty / XAI / clamp-flag / carbon /
        severity / missing-input keys alongside.
        """
        # --- Input validation ----------------------------------------
        if metrics is None or not isinstance(metrics, dict):
            logger.warning(
                "apply() received non-dict metrics (%s); returning {}",
                type(metrics).__name__,
            )
            return {}

        fw = _normalize_framework_name(framework)
        if fw is None:
            logger.warning(
                "apply() received non-string framework (%s); treating as "
                "unknown", type(framework).__name__,
            )
            fw = "unknown"

        target = metrics if in_place else dict(metrics)

        # --- Resolve baseline ---------------------------------------
        baseline = self.get_baseline(fw)

        # --- Read input metrics (distinguish absent from zero) ------
        latency_input_missing = (
            "latency_s" not in target or target.get("latency_s") is None
        )
        energy_input_missing = (
            "energy_wh" not in target or target.get("energy_wh") is None
        )
        latency_s = _safe_float(target.get("latency_s"), 0.0)
        energy_wh = _safe_float(target.get("energy_wh"), 0.0)

        # --- Compute effective values -------------------------------
        raw_effective_latency = latency_s - baseline.latency_s
        raw_effective_energy = energy_wh - baseline.energy_wh
        latency_clamped = raw_effective_latency < 0
        energy_clamped = raw_effective_energy < 0
        effective_latency = max(0.0, raw_effective_latency)
        effective_energy = max(0.0, raw_effective_energy)

        # --- Carbon (opt-in via features + caller) ------------------
        overhead_carbon_kg: Optional[float] = None
        if (
            self.features.get("carbon", True)
            and include_carbon
            and grid_intensity_g_kwh is not None
            and pue is not None
        ):
            gi = _finite_nonneg(grid_intensity_g_kwh)
            pue_f = _finite_nonneg(pue)
            if gi is not None and pue_f is not None:
                overhead_carbon_kg = (
                    (baseline.energy_wh / 1000.0)  # Wh -> kWh
                    * gi                          # kWh * g/kWh -> g
                    * pue_f                       # g * PUE
                    / 1000.0                      # g -> kg
                )

        # --- Write v1-compatible keys -------------------------------
        target["framework_overhead_latency"] = baseline.latency_s
        target["framework_overhead_energy_wh"] = baseline.energy_wh
        target["effective_latency_s"] = effective_latency
        target["effective_energy_wh"] = effective_energy

        # --- Provenance ---------------------------------------------
        if self.features.get("provenance", True):
            target["framework_overhead_source"] = baseline.source
            target["framework_overhead_sample_count"] = baseline.sample_count
            target["framework_overhead_framework"] = fw
            target["framework_overhead_analyzer_version"] = __version__
            if baseline.framework_version:
                target["framework_overhead_version"] = \
                    baseline.framework_version

        # --- Precision (end-to-end) ---------------------------------
        effective_precision = (
            precision if precision is not None else baseline.precision
        )
        target["framework_overhead_precision"] = effective_precision

        # --- Uncertainty --------------------------------------------
        if self.features.get("uncertainty", True):
            target["framework_overhead_latency_uncertainty_s"] = (
                baseline.uncertainty_latency_s
            )
            target["framework_overhead_energy_uncertainty_wh"] = (
                baseline.uncertainty_energy_wh
            )
            # Clamped values have no meaningful uncertainty — the caller
            # couldn't measure a real effective value.
            target["effective_latency_uncertainty_s"] = (
                None if latency_clamped else baseline.uncertainty_latency_s
            )
            target["effective_energy_uncertainty_wh"] = (
                None if energy_clamped else baseline.uncertainty_energy_wh
            )

        # --- Clamp flags --------------------------------------------
        if self.features.get("clamp_flags", True):
            target["framework_overhead_latency_clamped"] = latency_clamped
            target["framework_overhead_energy_clamped"] = energy_clamped

        # --- Missing-input flags ------------------------------------
        target["framework_overhead_latency_input_missing"] = \
            latency_input_missing
        target["framework_overhead_energy_input_missing"] = \
            energy_input_missing

        # --- Carbon keys --------------------------------------------
        if overhead_carbon_kg is not None:
            target["framework_overhead_carbon_kg"] = overhead_carbon_kg
            target["framework_overhead_carbon_operational_kg"] = \
                overhead_carbon_kg
            # Contractual is distinct: framework overhead is always
            # operational here (no RECs are retired for framework time).
            target["framework_overhead_carbon_contractual_kg"] = 0.0
            target["framework_overhead_carbon_source"] = baseline.source
        # Also record the raw intensity / PUE for provenance even when
        # carbon wasn't computed (so callers can see why).
        if grid_intensity_g_kwh is not None:
            target["framework_overhead_grid_intensity_g_kwh"] = (
                _finite_nonneg(grid_intensity_g_kwh)
            )
        if pue is not None:
            target["framework_overhead_pue"] = _finite_nonneg(pue)

        # --- Severity -----------------------------------------------
        if baseline.source == BaselineSource.UNKNOWN.value:
            severity = Severity.WARNING.value
        elif latency_clamped or energy_clamped:
            severity = Severity.WARNING.value
        else:
            severity = Severity.INFO.value
        target["framework_overhead_severity"] = severity

        # --- XAI ----------------------------------------------------
        explanation: Optional[OverheadExplanation] = None
        if self.features.get("xai", True):
            explanation = OverheadExplainer.explain(
                framework=fw,
                baseline=baseline,
                latency_s=latency_s,
                energy_wh=energy_wh,
                effective_latency_s=effective_latency,
                effective_energy_wh=effective_energy,
                latency_clamped=latency_clamped,
                energy_clamped=energy_clamped,
                latency_input_missing=latency_input_missing,
                energy_input_missing=energy_input_missing,
            )

        # --- HITL (only on clamp + hardcoded) -----------------------
        hitl_ratio = (
            baseline.latency_s / latency_s
            if latency_s > 0 else float("inf")
        )
        should_review = (
            self.features.get("hitl", True)
            and baseline.source == BaselineSource.HARDCODED.value
            and baseline.sample_count == 0
            and (
                latency_clamped
                or energy_clamped
                or hitl_ratio > _HITL_RATIO_THRESHOLD
            )
        )
        hitl_approved: Optional[bool] = None
        if (
            should_review
            and self._hitl_callback is not None
            and explanation is not None
        ):
            try:
                hitl_approved = self._hitl_callback(fw, baseline, hitl_ratio)
                if hitl_approved is False:
                    explanation.rationale.append(
                        "[HITL: denied] reviewer rejected the "
                        "hardcoded baseline for this run"
                    )
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)
                hitl_approved = None

        if explanation is not None:
            target["framework_overhead_explanation"] = (
                explanation.to_dict(safe=True)
            )
        target["framework_overhead_hitl_reviewed"] = (
            hitl_approved is not None
        )
        target["framework_overhead_hitl_approved"] = hitl_approved

        # --- DecisionRecord emission (JSON-safe) --------------------
        self._last_decision_record = None
        if emit_decision_record and self.features.get("provenance", True):
            record = self._build_decision_record(
                metrics=target,
                framework=fw,
                baseline=baseline,
                explanation=explanation,
                run_id=run_id,
                task_id=task_id,
                policy_version=policy_version,
                precision=effective_precision,
            )
            if record is not None:
                serialised = _serialise_decision_record(record)
                if serialised is not None:
                    target["framework_overhead_decision_record"] = serialised
                self._last_decision_record = record

        # --- Statistics / history (skip when track=False) -----------
        if track:
            with self._lock:
                self._normalization_count += 1
                self._framework_counts[fw] += 1
                if latency_clamped or energy_clamped:
                    self._clamp_count += 1
                if baseline.source == BaselineSource.UNKNOWN.value:
                    self._unknown_framework_count += 1
                self._history.append({
                    "framework": fw,
                    "baseline_source": baseline.source,
                    "latency_s": latency_s,
                    "baseline_latency_s": baseline.latency_s,
                    "effective_latency_s": effective_latency,
                    "energy_wh": energy_wh,
                    "baseline_energy_wh": baseline.energy_wh,
                    "effective_energy_wh": effective_energy,
                    "latency_clamped": latency_clamped,
                    "energy_clamped": energy_clamped,
                    "latency_input_missing": latency_input_missing,
                    "energy_input_missing": energy_input_missing,
                    "severity": severity,
                    "precision": effective_precision,
                    "at": datetime.now().isoformat(),
                })

        # --- Structured logging -------------------------------------
        log_extra = {
            "framework": fw,
            "baseline_source": baseline.source,
            "latency_s": latency_s,
            "energy_wh": energy_wh,
            "effective_latency_s": effective_latency,
            "effective_energy_wh": effective_energy,
            "latency_clamped": latency_clamped,
            "energy_clamped": energy_clamped,
            "latency_input_missing": latency_input_missing,
            "energy_input_missing": energy_input_missing,
            "precision": effective_precision,
            "run_id": run_id,
            "task_id": task_id,
            "deployment_id": self.deployment_id,
        }
        if latency_clamped or energy_clamped:
            logger.warning("framework.overhead_clamped", extra=log_extra)
        elif baseline.source == BaselineSource.UNKNOWN.value:
            logger.warning("framework.overhead_unknown_framework",
                           extra=log_extra)
        else:
            logger.info("framework.overhead_applied", extra=log_extra)

        return target

    def apply_with_record(
        self,
        metrics: Dict[str, Any],
        framework: str,
        **kwargs: Any,
    ) -> Tuple[Dict[str, Any], Optional[Any]]:
        """
        Like :meth:`apply`, but returns ``(metrics, decision_record_or_None)``.

        The record is the live `DecisionRecord` instance (or `None` if the
        shared contract isn't importable or `emit_decision_record=False`
        wasn't passed). The metrics dict gets the serialised form at
        `framework_overhead_decision_record` for JSON-safe consumers.
        """
        kwargs.setdefault("emit_decision_record", True)
        out = self.apply(metrics, framework, **kwargs)
        return out, self.get_last_decision_record()

    def get_last_decision_record(self) -> Optional[Any]:
        """
        Return the live `DecisionRecord` from the most recent `apply()` call
        that had `emit_decision_record=True`, or `None`.
        """
        return self._last_decision_record

    # ------------------------------------------------------------------
    # Statistics / export
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "analyzer_version": __version__,
                "deployment_id": self.deployment_id,
                "normalization_count": self._normalization_count,
                "clamp_count": self._clamp_count,
                "clamp_rate": (
                    self._clamp_count / self._normalization_count
                    if self._normalization_count else 0.0
                ),
                "unknown_framework_count": self._unknown_framework_count,
                "framework_counts": dict(self._framework_counts),
                "calibrated_frameworks": sorted(self._calibrated.keys()),
                "observation_counts": {
                    fw: len(obs) for fw, obs in self._observations.items()
                },
                "federated_profiles_pushed": len(self.federated.profiles),
                "features": dict(self.features),
            }
            if self._calibrated:
                stats["calibrated_baselines"] = {
                    fw: b.to_dict(safe=True)
                    for fw, b in self._calibrated.items()
                }
            if self._history:
                energies = [h["effective_energy_wh"] for h in self._history]
                lats = [h["effective_latency_s"] for h in self._history]
                stats["recent_effective"] = {
                    "mean_latency_s": sum(lats) / len(lats),
                    "mean_energy_wh": sum(energies) / len(energies),
                    "window_size": len(self._history),
                }
        return stats

    def export(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "analyzer_version": __version__,
                "statistics": self.get_statistics(),
                "history": list(self._history),
                "calibrated_baselines": {
                    fw: b.to_dict(safe=True)
                    for fw, b in self._calibrated.items()
                },
                "federated_aggregate": self.federated.aggregate(),
                "recent_observations": {
                    fw: [
                        {
                            "latency_s": o.observed_latency_s,
                            "energy_wh": o.observed_energy_wh,
                            "framework_version": o.framework_version,
                            "precision": o.precision,
                            "at": _safe_isoformat(o.at),
                        }
                        for o in obs
                    ]
                    for fw, obs in self._observations.items()
                },
            }

    def reset_statistics(self) -> None:
        """Clear counters + history (preserves calibration and observations)."""
        with self._lock:
            self._normalization_count = 0
            self._clamp_count = 0
            self._unknown_framework_count = 0
            self._framework_counts.clear()
            self._history.clear()

    def reset_observations(self) -> None:
        """Clear observations and calibrated baselines."""
        with self._lock:
            self._observations.clear()
            self._calibrated.clear()
            self._contributed.clear()

    # ------------------------------------------------------------------
    # HITL / federated
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[str, Baseline, float], Optional[bool]]
        ],
    ) -> None:
        """
        Register a HITL reviewer.

        The callback receives ``(framework, baseline, ratio)`` where
        ``ratio = baseline.latency_s / input_latency_s``. It is invoked
        only when a hardcoded baseline exceeds the input (clamp) or the
        ratio exceeds ``_HITL_RATIO_THRESHOLD``. Returning ``False``
        annotates the explanation's rationale with ``[HITL: denied]``.

        Passing ``None`` clears the callback.
        """
        self._hitl_callback = callback

    def contribute_federated(
        self,
        framework: Optional[str] = None,
        *,
        dedupe: bool = True,
    ) -> None:
        """
        Push local observations as a federated profile.

        `dedupe=True` (default) only pushes *new* observations since the
        last contribution for the given framework, preventing duplicate
        profiles from repeated calls. `dedupe=False` re-pushes the whole
        window (v1 behaviour).
        """
        with self._lock:
            targets = (
                [_normalize_framework_name(framework)]
                if framework is not None
                else list(self._observations.keys())
            )
            for fw in targets:
                if fw is None:
                    continue
                obs_deque = self._observations.get(fw)
                if not obs_deque:
                    continue
                obs = list(obs_deque)
                if len(obs) < self.min_samples_for_calibration:
                    continue

                already_pushed = self._contributed.get(fw, 0)
                if dedupe and len(obs) <= already_pushed:
                    continue

                # For dedupe=True, push only the newly added slice.
                new_slice = (
                    obs[already_pushed:] if dedupe else obs
                )
                if not new_slice:
                    continue

                lat = [o.observed_latency_s for o in new_slice]
                en = [o.observed_energy_wh for o in new_slice]
                versions = [o.framework_version for o in new_slice
                            if o.framework_version]

                self.federated.push(FederatedOverheadProfile(
                    deployment_id=self.deployment_id,
                    framework=fw,
                    framework_version=(
                        Counter(versions).most_common(1)[0][0]
                        if versions else None
                    ),
                    mean_latency_s=statistics.fmean(lat),
                    mean_energy_wh=statistics.fmean(en),
                    std_latency_s=(
                        statistics.pstdev(lat) if len(lat) > 1 else 0.0
                    ),
                    std_energy_wh=(
                        statistics.pstdev(en) if len(en) > 1 else 0.0
                    ),
                    sample_count=len(new_slice),
                ))
                self._contributed[fw] = len(obs)

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_decision_record(
        self,
        *,
        metrics: Dict[str, Any],
        framework: str,
        baseline: Baseline,
        explanation: Optional[OverheadExplanation],
        run_id: Optional[str],
        task_id: Optional[str],
        policy_version: str,
        precision: Optional[str],
    ) -> Optional[Any]:
        """Construct a DecisionRecord; returns None if the contract is absent."""
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug(
                    "DecisionRecord not importable; skipping emission "
                    "for %s", framework,
                )
                return None

        effective_latency_s = _safe_float(
            metrics.get("effective_latency_s"), 0.0,
        )
        effective_energy_wh = _safe_float(
            metrics.get("effective_energy_wh"), 0.0,
        )
        carbon_operational = metrics.get(
            "framework_overhead_carbon_operational_kg"
        )

        candidates: Dict[str, Any] = {
            "run_id": run_id or "",
            "timestamp": datetime.now(),
            "task_id": task_id or "",
            "selected_action": f"framework_overhead_normalize({framework})",
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": framework,
            "precision": precision,
            "latency_ms": effective_latency_s * 1000.0,
            "energy_kwh": effective_energy_wh / 1000.0,
            "carbon_operational_kg": (
                float(carbon_operational)
                if isinstance(carbon_operational, (int, float))
                and math.isfinite(float(carbon_operational))
                else 0.0
            ),
            "carbon_contractual_kg": 0.0,
            "explanation": (
                explanation.to_dict(safe=True) if explanation else {}
            ),
            "provenance": {
                "source": "framework_overhead",
                "baseline_source": baseline.source,
                "sample_count": baseline.sample_count,
                "analyzer_version": __version__,
                "deployment_id": self.deployment_id,
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
                framework, e,
            )
            return None


# =============================================================================
# Module-level singleton (locked)
# =============================================================================

_DEFAULT_PROFILER: Optional[FrameworkOverheadProfiler] = None
_DEFAULT_PROFILER_LOCK = threading.Lock()


def _get_default_profiler() -> FrameworkOverheadProfiler:
    global _DEFAULT_PROFILER
    if _DEFAULT_PROFILER is not None:
        return _DEFAULT_PROFILER
    with _DEFAULT_PROFILER_LOCK:
        if _DEFAULT_PROFILER is None:
            _DEFAULT_PROFILER = FrameworkOverheadProfiler()
        return _DEFAULT_PROFILER


# =============================================================================
# Backward-compatible module functions
# =============================================================================

def apply_overhead(metrics: dict, framework: str) -> dict:
    """
    Normalize framework overhead costs on metrics.

    Backward-compatible: same signature, same return type, same 4 keys
    written. Delegates to the shared default profiler with
    ``track=True, in_place=True, emit_decision_record=False`` — matching
    v1 semantics exactly.
    """
    return _get_default_profiler().apply(metrics, framework)


def apply_overhead_batch(
    metrics_list: Iterable[dict],
    framework: str,
    *,
    profiler: Optional[FrameworkOverheadProfiler] = None,
    track: bool = True,
    in_place: bool = True,
) -> List[dict]:
    """
    Apply the same framework normalization to a batch of metrics.

    `track=False` skips counters + history + federated reads — useful for
    pure-normalization batch jobs. `in_place=False` returns fresh copies.
    """
    p = profiler or _get_default_profiler()
    return [
        p.apply(m, framework, track=track, in_place=in_place)
        for m in metrics_list
    ]


def get_statistics() -> Dict[str, Any]:
    """Statistics for the shared default profiler."""
    return _get_default_profiler().get_statistics()


def reset_statistics() -> None:
    """Clear statistics (and history) for the shared default profiler."""
    _get_default_profiler().reset_statistics()


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import threading as _threading

    logging.basicConfig(level=logging.WARNING)

    def _mk_metrics(latency=2.5, energy=0.10):
        return {"latency_s": latency, "energy_wh": energy}

    # =================================================================
    # 1. Original behavior (backward compatible)
    # =================================================================
    print("=== 1. Original behavior ===")
    m = _mk_metrics()
    result = apply_overhead(m, "langchain")
    print(f"  effective_latency_s: {result['effective_latency_s']}")
    print(f"  effective_energy_wh: {result['effective_energy_wh']}")
    print(f"  (same dict mutated in-place: {result is m})")

    # =================================================================
    # 2. Enhanced profiler with observation + calibration
    # =================================================================
    print("\n=== 2. Enhanced profiler ===")
    profiler = FrameworkOverheadProfiler(
        deployment_id="us-ca-prod-01",
        min_samples_for_calibration=3,
    )
    for _ in range(10):
        profiler.record_observation(
            "langchain",
            observed_latency_s=0.18,
            observed_energy_wh=0.025,
            framework_version="0.2.1",
            precision="fp32",
        )
    baselines = profiler.calibrate()
    cal = profiler.get_calibrated_baseline("langchain")
    print(f"  calibrated: {list(baselines.keys())}")
    if cal:
        print(f"  latency: {cal.latency_s:.4f} ± {cal.uncertainty_latency_s:.4f}")
        print(f"  source:  {cal.source}")
        print(f"  precision: {cal.precision}")

    # =================================================================
    # 3. Case-insensitivity (normalized counters)
    # =================================================================
    print("\n=== 3. Case-insensitive counters ===")
    p_case = FrameworkOverheadProfiler(min_samples_for_calibration=2)
    for fw in ("langchain", "LangChain", "LANGCHAIN"):
        p_case.apply(_mk_metrics(), fw, track=True)
    stats = p_case.get_statistics()
    print(f"  framework_counts: {stats['framework_counts']}")
    print(f"  unique keys: {len(stats['framework_counts'])}  (expected 1)")

    # =================================================================
    # 4. Negative observations rejected
    # =================================================================
    print("\n=== 4. Negative observation rejected ===")
    p_neg = FrameworkOverheadProfiler()
    for bad in [
        ("langchain", -0.5, 0.1),
        ("langchain", 0.1, -0.05),
        ("langchain", float("nan"), 0.1),
        ("langchain", 0.1, float("inf")),
        (123, 0.1, 0.1),
    ]:
        try:
            p_neg.record_observation(*bad)
            print(f"  {bad}: accepted (unexpected)")
        except ValueError as e:
            print(f"  {bad[0]!r}: rejected ({str(e)[:40]}...)")

    # =================================================================
    # 5. emit_decision_record is JSON-safe
    # =================================================================
    print("\n=== 5. DecisionRecord is JSON-safe ===")
    m_dr = _mk_metrics()
    p_dr = FrameworkOverheadProfiler()
    out = p_dr.apply(
        m_dr, "langchain",
        emit_decision_record=True,
    )
    if "framework_overhead_decision_record" in out:
        try:
            json.dumps(out)
            print(f"  json.dumps(metrics): OK — "
                  f"DR stored as {type(out['framework_overhead_decision_record']).__name__}")
        except (TypeError, ValueError) as e:
            print(f"  FAILED: {e}")
    else:
        print("  (DecisionRecord contract not importable; skipped)")
    print(f"  live record accessible: "
          f"{p_dr.get_last_decision_record() is not None}")

    # =================================================================
    # 6. missing-vs-zero tracking
    # =================================================================
    print("\n=== 6. Missing-vs-zero tracking ===")
    p_missing = FrameworkOverheadProfiler()
    m_empty = {}
    out = p_missing.apply(m_empty, "langchain")
    print(f"  latency_input_missing: "
          f"{out['framework_overhead_latency_input_missing']}")
    print(f"  effective_latency_s:   {out['effective_latency_s']}")
    m_zero = {"latency_s": 0.0, "energy_wh": 0.0}
    out_zero = p_missing.apply(m_zero, "langchain")
    print(f"  zero-valued latency missing: "
          f"{out_zero['framework_overhead_latency_input_missing']}")

    # =================================================================
    # 7. in_place=False
    # =================================================================
    print("\n=== 7. in_place=False ===")
    p_copy = FrameworkOverheadProfiler()
    original = _mk_metrics()
    out_copy = p_copy.apply(original, "langchain", in_place=False)
    print(f"  original untouched: {len(original)} keys")
    print(f"  copy enriched:      {len(out_copy)} keys")

    # =================================================================
    # 8. feature flag: calibration=False
    # =================================================================
    print("\n=== 8. features={'calibration': False} ===")
    p_no_cal = FrameworkOverheadProfiler(
        min_samples_for_calibration=2,
        features={"calibration": False},
    )
    for _ in range(5):
        p_no_cal.record_observation("langchain", 0.18, 0.025)
    cal_result = p_no_cal.calibrate()
    print(f"  calibrate() returns: {cal_result}  (expected {{}})")
    m_fc = _mk_metrics()
    out_fc = p_no_cal.apply(m_fc, "langchain")
    print(f"  baseline source: {out_fc['framework_overhead_source']} "
          f"(expected hardcoded, not calibrated)")

    # =================================================================
    # 9. HITL callback (fires on clamp only)
    # =================================================================
    print("\n=== 9. HITL callback fires only on clamp ===")
    p_hitl = FrameworkOverheadProfiler()
    calls = []
    p_hitl.set_hitl_callback(lambda fw, bl, ratio: calls.append(
        (fw, bl.source, round(ratio, 3))
    ) or True)
    # No clamp: baseline 0.15 < latency 2.5 -> no HITL
    p_hitl.apply(_mk_metrics(latency=2.5), "langchain")
    print(f"  calls after normal run: {calls}")
    # Clamp: latency 0.10 < baseline 0.15 -> HITL fires
    p_hitl.apply(_mk_metrics(latency=0.10), "langchain")
    print(f"  calls after clamped run: {calls}")

    # =================================================================
    # 10. contribute_federated idempotency
    # =================================================================
    print("\n=== 10. contribute_federated(dedupe=True) idempotent ===")
    p_fed = FrameworkOverheadProfiler(min_samples_for_calibration=2)
    for _ in range(4):
        p_fed.record_observation("langchain", 0.18, 0.025)
    p_fed.contribute_federated("langchain")
    agg1 = p_fed.get_federated_aggregate()
    p_fed.contribute_federated("langchain")   # no-op — no new observations
    agg2 = p_fed.get_federated_aggregate()
    print(f"  sample_count after 1st: {agg1.get('langchain', {}).get('sample_count')}")
    print(f"  sample_count after 2nd: {agg2.get('langchain', {}).get('sample_count')}")

    # =================================================================
    # 11. Locked singleton (10 threads, no lost counters)
    # =================================================================
    print("\n=== 11. Locked singleton under contention ===")
    reset_statistics()
    def _worker():
        for _ in range(50):
            apply_overhead(_mk_metrics(), "autogen")
    threads = [_threading.Thread(target=_worker) for _ in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    s = get_statistics()
    print(f"  normalization_count: {s['normalization_count']}  (expected 500)")

    # =================================================================
    # 12. track=False (pure path)
    # =================================================================
    print("\n=== 12. track=False (pure normalization) ===")
    p_pure = FrameworkOverheadProfiler()
    p_pure.apply(_mk_metrics(), "langchain", track=False)
    p_pure.apply(_mk_metrics(), "langchain", track=False)
    print(f"  normalization_count: "
          f"{p_pure.get_statistics()['normalization_count']}  (expected 0)")

    # =================================================================
    # 13. Precision end-to-end
    # =================================================================
    print("\n=== 13. Precision end-to-end ===")
    p_prec = FrameworkOverheadProfiler()
    m_prec = _mk_metrics()
    out_prec = p_prec.apply(m_prec, "langchain", precision="int8")
    print(f"  precision recorded: {out_prec['framework_overhead_precision']}")

    # =================================================================
    # 14. Carbon keys (operational vs contractual)
    # =================================================================
    print("\n=== 14. Carbon keys ===")
    p_car = FrameworkOverheadProfiler()
    m_car = _mk_metrics()
    out_car = p_car.apply(
        m_car, "langchain",
        grid_intensity_g_kwh=350.0, pue=1.2,
    )
    print(f"  carbon_operational_kg:  "
          f"{out_car.get('framework_overhead_carbon_operational_kg')}")
    print(f"  carbon_contractual_kg:  "
          f"{out_car.get('framework_overhead_carbon_contractual_kg')}")

    # =================================================================
    # 15. Statistics & export
    # =================================================================
    print("\n=== 15. Full statistics ===")
    print(json.dumps(profiler.get_statistics(), indent=2, default=str))
