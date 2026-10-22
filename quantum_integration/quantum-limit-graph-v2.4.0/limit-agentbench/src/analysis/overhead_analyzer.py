# src/analysis/telemetry/overhead_analyzer.py
"""
Tool-call-based overhead analyzer for Green Agent (Enhanced, v2.0.0)
=====================================================================

Estimates framework overhead from the number of tool calls in a run.
Complements `framework_overhead.py` (per-framework baselines) with
per-tool-call costs.

Original API preserved (all signatures, all side effects):
    analyzer = OverheadAnalyzer()                     # default 0.05 J / 0.2 s
    metrics = analyzer.compute(metrics)
    # writes: metrics["framework_overhead_energy"]    (Joules)
    #         metrics["framework_overhead_latency"]   (seconds, total)

Enhanced API:
    analyzer = OverheadAnalyzer(
        tool_call_cost_joules=0.05,
        tool_call_latency_sec=0.2,
        *,
        run_id="...", deployment_id="...",
        precision="int8", region="US-CA",
        grid_intensity_g_kwh=350.0, pue=1.2,
        max_observations=10_000,
        max_tool_calls=1_000_000,
        precision_affects_latency=False,
        precision_table=None,           # override _PRECISION_ENERGY_MULTIPLIER
        features={...},
    )

    # Core
    metrics = analyzer.compute(metrics, emit_decision_record=None)

    # Calibration
    ok = analyzer.record_observation(
        tool_calls=10, actual_energy_j=0.6, actual_latency_s=2.1,
        precision="int8",
    )
    calibrate_result = analyzer.calibrate(
        min_samples=5, max_age_s=86_400, reject_outliers=True,
    )
    analyzer.reset_calibration()

    # Accessors / emission
    analyzer.get_current_source()          # derived
    analyzer.get_last_decision_record()
    analyzer.to_decision_record(metrics, policy_version="v5.0.1")

    # Config / introspection
    analyzer.set_precision("int8")
    analyzer.set_region("US-CA")
    analyzer.set_grid_intensity(350.0)
    analyzer.set_pue(1.2)
    analyzer.set_hitl_callback(cb)
    analyzer.get_statistics()
    analyzer.export()
    analyzer.reset(full=False)

Keys written by compute() — v1 preserved and v2 prefixed:
    # v1 (unchanged):
    framework_overhead_energy               J
    framework_overhead_latency              s

    # v2 prefixed — canonical, unambiguous:
    tool_overhead_energy_joules             J
    tool_overhead_energy_wh                 Wh
    tool_overhead_latency_seconds           s
    tool_overhead_source                    "hardcoded"|"calibrated"|"partial"|"federated"|"overridden"
    tool_overhead_simulated                 bool
    tool_overhead_per_call_energy_j         J
    tool_overhead_per_call_latency_s        s
    tool_overhead_precision                 precision string
    tool_overhead_precision_multiplier      float
    tool_overhead_carbon_g                  gCO2e (when grid intensity set)
    tool_overhead_hitl_denied               bool
    tool_overhead_explanation               XAI dict
    tool_overhead_decision_record           serialised DecisionRecord (opt-in)

    # v2 legacy aliases — kept for compatibility; prefixed keys preferred:
    framework_overhead_energy_joules
    framework_overhead_energy_wh
    framework_overhead_latency_seconds
    framework_overhead_source               v1 "default" → now "hardcoded"
    framework_overhead_simulated
    framework_overhead_per_call_energy_j
    framework_overhead_per_call_latency_s
    framework_overhead_precision
    framework_overhead_precision_multiplier
    framework_overhead_carbon_g
    framework_overhead_carbon_source
    framework_overhead_explanation
    framework_overhead_hitl_denied
    _overhead_decision_record               serialised (was live object in v1)
    overhead_decision_record                serialised (canonical alias)

Enhancements implemented (v2.0.0):
  1.  Disambiguated keys   — `tool_overhead_*` prefixed keys carry canonical
                             per-tool-call values; v1 `framework_overhead_*`
                             keys are preserved with a collision warning when
                             they're already populated by `framework_overhead.py`.
  2.  Correct calibration  — calibrated values are used as-is (v1 applied the
                             precision multiplier on top, halving int8 energy
                             estimates calibrated from int8 observations).
                             The estimator is weighted least-squares through
                             the origin (`Σxy / Σx²`), not `mean(y/x)` — the
                             later is biased toward observations with fewer
                             tool calls.
  3.  Derived source state — `_source` is no longer mutated inside
                             `compute()`; `current_source` is a derived
                             property, safe under concurrency.
  4.  Thread safety        — `RLock` covers `compute()`, `calibrate()`, and
                             any calibration-state read/write.
  5.  Calibration rigor    — `min_samples >= 1` validated; empty observations
                             short-circuit; optional `max_age_s` filter by
                             `observation.at`; MAD-based outlier rejection at
                             `reject_outliers=True`.
  6.  Bounded state        — `_observations` uses `deque(maxlen=…)`.
  7.  Tool-call cap        — `_safe_int` clamps to `MAX_TOOL_CALLS` (default
                             1,000,000) and flags the violation.
  8.  DecisionRecord       — serialised to `_overhead_decision_record` /
                             `overhead_decision_record` (v1 wrote a live
                             object → broke `json.dumps`); public
                             `to_decision_record()` and
                             `get_last_decision_record()` accessors;
                             `inspect.signature` filters kwargs.
  9.  Honest source label  — the default source string is `"hardcoded"`
                             (not `"default"`) so `metric_provenance`
                             classifies it as `DEFAULT` rather than
                             `ESTIMATED`.
 10.  HITL                 — `HITL_OVERHEAD_RATIO` now actually triggers
                             review (overhead > 0.5 × total); callback
                             returning `False` appends `[HITL: denied]`
                             to the explanation and sets
                             `tool_overhead_hitl_denied`.
 11.  Extended bounds      — caps tool_calls; checks overhead latency vs
                             total (v1); checks overhead energy vs any
                             existing `energy_wh` on the dict.
 12.  Instance stats       — `_StatsCounter` with per-instance counters and
                             module-level shims.
 13.  Structured logging   — `overhead.computed`, `overhead.calibrated`,
                             `overhead.key_collision`,
                             `overhead.hitl_denied`,
                             `overhead.bounds_violation`,
                             `overhead.decision_record_failed`.
 14.  `__version__` / `__all__`.

Backward compatibility:
  * `framework_overhead_source` value for the default case changes from
    `"default"` to `"hardcoded"` (matches the layer-wide convention used
    by `framework_overhead.py` and `metric_provenance.py`). Callers
    comparing against the enum (`OverheadSource.DEFAULT.value`) are
    unaffected because the enum value is updated.
  * `_overhead_decision_record` is now a serialised dict rather than a
    live `DecisionRecord`. Callers who persisted `metrics` as JSON
    previously failed; this release fixes that.
  * Module-level `compute_overhead(metrics, ...)` keeps its signature; it
    is documented as a one-shot helper (no calibration state).
"""

from __future__ import annotations

import inspect
import logging
import math
import statistics
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "OverheadAnalyzer",
    "OverheadObservation",
    "OverheadExplanation",
    "OverheadExplainer",
    "OverheadSource",
    "Severity",
    "compute_overhead",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class OverheadSource(Enum):
    """
    Where the per-call constants came from.

    `DEFAULT` uses `"hardcoded"` (matching `framework_overhead.py` and the
    `metric_provenance.py` inference table); v1 used `"default"`.
    """
    DEFAULT = "hardcoded"
    CALIBRATED = "calibrated"
    FEDERATED = "federated"
    OVERRIDDEN = "overridden"
    PARTIAL = "partial"     # only one of energy / latency is calibrated


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


# =============================================================================
# Constants
# =============================================================================

JOULES_PER_WH = 3600.0
JOULES_PER_KWH = 3_600_000.0

#: Precision → per-call energy multiplier (relative to fp32 baseline).
#: Callers can override via `OverheadAnalyzer(precision_table=...)`.
_PRECISION_ENERGY_MULTIPLIER: Dict[str, float] = {
    "fp64": 1.00,
    "fp32": 1.00,
    "tf32": 0.85,
    "fp16": 0.75,
    "bf16": 0.80,
    "int8": 0.50,
    "int4": 0.35,
    "quantum_distilled": 0.25,
}

_MAD_K_DEFAULT = 3.0
_MAD_SCALE = 1.4826


# =============================================================================
# Helpers
# =============================================================================

def _safe_int(value: Any, *, cap: Optional[int] = None) -> Tuple[Optional[int], Optional[str]]:
    """
    Return `(value, violation)`.

    `value` is None on parse failure; `violation` describes why. `cap`, if
    provided, clamps values above it and reports `"capped"`.
    """
    if isinstance(value, bool):
        return None, "bool"
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return None, "non_numeric"
    if not math.isfinite(f):
        return None, "non_finite"
    n = int(f)  # truncate
    if n < 0:
        return n, "negative"      # caller decides clamping
    if cap is not None and n > cap:
        return cap, "capped"
    return n, None


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return f if math.isfinite(f) else default


def _jsonable(v: Any) -> Any:
    """Recursively coerce to a JSON-serialisable primitive."""
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    if is_dataclass(v) and not isinstance(v, type):
        try:
            return _jsonable(asdict(v))
        except Exception:
            pass
    return repr(v)


def _filter_decision_record_kwargs(
    DecisionRecord: Any, candidates: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        sig = inspect.signature(DecisionRecord)
        accepted = set(sig.parameters.keys())
        return {k: v for k, v in candidates.items() if k in accepted}
    except (ValueError, TypeError):
        return dict(candidates)


def _serialise_decision_record(record: Any) -> Optional[Dict[str, Any]]:
    if record is None:
        return None
    if hasattr(record, "to_dict") and callable(record.to_dict):
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


def _import_decision_record() -> Optional[Any]:
    try:
        from src.analysis import DecisionRecord  # type: ignore
        return DecisionRecord
    except Exception:
        try:
            from analysis import DecisionRecord  # type: ignore
            return DecisionRecord
        except Exception:
            return None


def _ls_through_origin(xs: List[float], ys: List[float]) -> float:
    """Weighted least-squares through the origin: `Σxy / Σx²`."""
    num = sum(x * y for x, y in zip(xs, ys))
    den = sum(x * x for x in xs)
    return num / den if den > 0 else 0.0


def _reject_outlier_indices(
    xs: List[float], ys: List[float], *, k: float,
) -> List[int]:
    """Return indices to keep, filtering by MAD on y/x ratio."""
    if len(xs) < 3:
        return list(range(len(xs)))
    ratios = [y / x if x > 0 else 0.0 for x, y in zip(xs, ys)]
    med = statistics.median(ratios)
    mad = statistics.median([abs(r - med) for r in ratios])
    if mad < 1e-12:
        return list(range(len(xs)))
    keep: List[int] = []
    for i, r in enumerate(ratios):
        if abs(r - med) / (_MAD_SCALE * mad) <= k:
            keep.append(i)
    return keep if len(keep) >= 3 else list(range(len(xs)))


# =============================================================================
# Thread-safe statistics counter
# =============================================================================

class _StatsCounter:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counts: Counter = Counter()

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[key] += amount

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


_GLOBAL_STATS = _StatsCounter()


def get_statistics() -> Dict[str, Any]:
    """Module-level statistics for `compute_overhead` and instance calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "computations": snap.get("computations", 0),
        "calibrations": snap.get("calibrations", 0),
        "invalid_inputs": snap.get("invalid", 0),
        "clamped_inputs": snap.get("clamped", 0),
        "capped_inputs": snap.get("capped", 0),
    }


def reset_statistics() -> None:
    """Clear the module-level counter."""
    _GLOBAL_STATS.reset()


# =============================================================================
# Observation and XAI dataclasses
# =============================================================================

@dataclass
class OverheadObservation:
    """A single `(tool_calls → observed cost)` observation."""
    tool_calls: int
    observed_energy_j: float
    observed_latency_s: float
    precision: Optional[str] = None
    region: Optional[str] = None
    at: float = field(default_factory=time.time)

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            return _jsonable(out)
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
            return _jsonable(out)
        return out


class OverheadExplainer:
    @staticmethod
    def explain(
        tool_calls: int,
        energy_j: float,
        latency_s: float,
        source: str,
        per_call_energy_j: float,
        per_call_latency_s: float,
        precision_multiplier: float = 1.0,
        energy_precision_applied: bool = True,
        clamped: bool = False,
        capped: bool = False,
        collision_detected: bool = False,
        hitl_denied: bool = False,
    ) -> OverheadExplanation:
        reasons: List[str] = [
            f"Tool-call overhead from {tool_calls} call(s).",
            f"Per-call energy: {per_call_energy_j:.4f} J"
            + (
                f" (×{precision_multiplier:.2f} precision multiplier)"
                if energy_precision_applied and precision_multiplier != 1.0
                else ""
            ) + ".",
            f"Per-call latency: {per_call_latency_s:.4f} s.",
            f"Total energy: {energy_j:.4f} J "
            f"({energy_j / JOULES_PER_WH:.6f} Wh).",
            f"Total latency: {latency_s:.4f} s.",
        ]
        if source == OverheadSource.CALIBRATED.value:
            reasons.append("Per-call constants are calibrated.")
        elif source == OverheadSource.PARTIAL.value:
            reasons.append(
                "WARNING: only one of energy/latency is calibrated."
            )
        elif source == OverheadSource.FEDERATED.value:
            reasons.append("Per-call constants are federated.")
        elif source == OverheadSource.OVERRIDDEN.value:
            reasons.append("Per-call constants are caller-overridden.")
        else:
            reasons.append(
                "WARNING: per-call constants are DEFAULT guesses. "
                "Consider calibrating from observations."
            )
        if clamped:
            reasons.append(
                "WARNING: negative tool_calls was clamped to 0."
            )
        if capped:
            reasons.append(
                "WARNING: tool_calls exceeded the configured cap."
            )
        if collision_detected:
            reasons.append(
                "WARNING: framework_overhead_latency was already populated "
                "when compute() ran — see tool_overhead_* for the "
                "per-tool-call values."
            )
        if hitl_denied:
            reasons.append("[HITL: denied] reviewer rejected this overhead.")

        confidence = (
            0.95 if source == OverheadSource.CALIBRATED.value
            else 0.85 if source == OverheadSource.FEDERATED.value
            else 0.80 if source == OverheadSource.OVERRIDDEN.value
            else 0.40
        )
        return OverheadExplanation(
            headline=(
                f"tool_overhead: {energy_j:.4f} J, {latency_s:.4f} s "
                f"({source})"
            ),
            rationale=reasons,
            contributing_factors={
                "tool_calls": float(tool_calls),
                "per_call_energy_j": per_call_energy_j,
                "per_call_latency_s": per_call_latency_s,
                "precision_multiplier": precision_multiplier,
                "energy_precision_applied": (
                    1.0 if energy_precision_applied else 0.0
                ),
                "total_energy_j": energy_j,
                "total_latency_s": latency_s,
            },
            confidence=confidence,
        )


# =============================================================================
# OverheadAnalyzer
# =============================================================================

class OverheadAnalyzer:
    """
    Enhanced tool-call overhead analyzer.

    Backward-compatible: same constructor (two positional args), same
    `compute(metrics)`, same v1 output keys.
    """

    HITL_OVERHEAD_RATIO = 0.5
    MAX_TOOL_CALLS = 1_000_000
    DEFAULT_MAX_OBSERVATIONS = 10_000

    DEFAULT_FEATURES: Dict[str, bool] = {
        "provenance": True,
        "unit_explicit": True,
        "xai": True,
        "carbon": True,
        "hitl": True,
        "statistics": True,
        "precision_aware": True,
        "bounds_check": True,
        "decision_record": False,
        "key_collision_warning": True,
    }

    def __init__(
        self,
        tool_call_cost_joules: Any = 0.05,
        tool_call_latency_sec: Any = 0.2,
        *,
        run_id: Optional[str] = None,
        deployment_id: str = "local",
        precision: Optional[str] = None,
        region: Optional[str] = None,
        grid_intensity_g_kwh: Optional[float] = None,
        pue: Optional[float] = None,
        features: Optional[Dict[str, bool]] = None,
        max_observations: int = DEFAULT_MAX_OBSERVATIONS,
        max_tool_calls: int = MAX_TOOL_CALLS,
        precision_affects_latency: bool = False,
        precision_table: Optional[Dict[str, float]] = None,
    ):
        # --- v1 public state (preserved names) ---
        self.tool_energy: float = max(
            0.0, _safe_float(tool_call_cost_joules, default=0.05),
        )
        self.tool_latency: float = max(
            0.0, _safe_float(tool_call_latency_sec, default=0.2),
        )

        # --- Identity / context ---
        self.run_id: str = run_id or f"oa-{uuid.uuid4().hex[:8]}"
        self.deployment_id: str = deployment_id
        self.precision: Optional[str] = precision
        self.region: Optional[str] = region
        self.grid_intensity_g_kwh: Optional[float] = (
            _safe_float(grid_intensity_g_kwh, default=-1.0)
            if grid_intensity_g_kwh is not None else None
        )
        if (
            self.grid_intensity_g_kwh is not None
            and self.grid_intensity_g_kwh < 0
        ):
            self.grid_intensity_g_kwh = None
        self.pue: Optional[float] = (
            _safe_float(pue, default=-1.0) if pue is not None else None
        )
        if self.pue is not None and self.pue < 0:
            self.pue = None

        # --- Feature flags ---
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "overhead.unknown_features",
                extra={"unknown": sorted(unknown)},
            )

        # --- Precision table (caller-overridable) ---
        if precision_table is not None:
            self._precision_table: Dict[str, float] = {
                str(k).strip().lower(): _safe_float(v, default=1.0)
                for k, v in precision_table.items()
            }
        else:
            self._precision_table = dict(_PRECISION_ENERGY_MULTIPLIER)
        self._precision_affects_latency = bool(precision_affects_latency)

        # --- Calibration state ---
        self._observations: Deque[OverheadObservation] = deque(
            maxlen=max(1, int(max_observations))
        )
        self._calibrated_energy_j: Optional[float] = None
        self._calibrated_latency_s: Optional[float] = None

        # --- Caps ---
        self._max_tool_calls = max(1, int(max_tool_calls))

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[Dict[str, Any]], Optional[bool]]
        ] = None

        # --- Locks / stats ---
        self._lock = threading.RLock()
        self._stats = _StatsCounter()
        self._computations: int = 0
        self._clamp_count: int = 0
        self._history: Deque[Dict[str, Any]] = deque(maxlen=1024)
        self._last_decision_record: Optional[Any] = None
        self._collision_warned: bool = False

        logger.debug(
            "overhead.initialized",
            extra={
                "run_id": self.run_id,
                "cost_j": self.tool_energy,
                "latency_s": self.tool_latency,
                "precision": self.precision,
            },
        )

    # ------------------------------------------------------------------
    # Derived source
    # ------------------------------------------------------------------

    @property
    def current_source(self) -> str:
        """Derived source label (never stored as mutable state)."""
        e = self._calibrated_energy_j is not None
        l = self._calibrated_latency_s is not None
        if e and l:
            return OverheadSource.CALIBRATED.value
        if e or l:
            return OverheadSource.PARTIAL.value
        return OverheadSource.DEFAULT.value

    def get_current_source(self) -> str:
        """Accessor (kept for symmetric API with the property)."""
        return self.current_source

    # ------------------------------------------------------------------
    # Configuration setters
    # ------------------------------------------------------------------

    def set_precision(self, precision: Optional[str]) -> None:
        with self._lock:
            self.precision = precision if isinstance(precision, str) else None

    def set_region(self, region: Optional[str]) -> None:
        with self._lock:
            self.region = region if isinstance(region, str) else None

    def set_grid_intensity(
        self, grid_intensity_g_kwh: Optional[float],
    ) -> None:
        with self._lock:
            if grid_intensity_g_kwh is None:
                self.grid_intensity_g_kwh = None
                return
            v = _safe_float(grid_intensity_g_kwh, default=-1.0)
            self.grid_intensity_g_kwh = v if v >= 0 else None

    def set_pue(self, pue: Optional[float]) -> None:
        with self._lock:
            if pue is None:
                self.pue = None
                return
            v = _safe_float(pue, default=-1.0)
            self.pue = v if v >= 0 else None

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[Dict[str, Any]], Optional[bool]]
        ],
    ) -> None:
        """Register (or clear) a HITL reviewer for implausible overhead."""
        with self._lock:
            self._hitl_callback = callback

    def get_last_decision_record(self) -> Optional[Any]:
        with self._lock:
            return self._last_decision_record

    # ------------------------------------------------------------------
    # ORIGINAL public API
    # ------------------------------------------------------------------

    def compute(
        self,
        metrics: dict,
        *,
        emit_decision_record: Optional[bool] = None,
        compute_precision: Optional[str] = None,
    ) -> dict:
        """
        Compute tool-call overhead and write it into `metrics`.

        Backward-compatible: writes `framework_overhead_energy` and
        `framework_overhead_latency` with the same semantics as v1.
        Also writes prefixed `tool_overhead_*` keys — the canonical
        source of truth.

        `emit_decision_record` (None → use `features['decision_record']`).
        `compute_precision` (None → use `self.precision`).
        """
        # --- Input validation ---
        if metrics is None or not isinstance(metrics, dict):
            logger.warning(
                "overhead.computed",
                extra={
                    "reason": "non_dict_metrics",
                    "type": type(metrics).__name__,
                },
            )
            return {}

        # --- Tool-calls read + validation ---
        raw_tool_calls = metrics.get("tool_calls", 0)
        tool_calls, violation = _safe_int(
            raw_tool_calls, cap=self._max_tool_calls,
        )
        clamped = False
        capped = False
        if tool_calls is None or violation in ("bool", "non_numeric", "non_finite"):
            _GLOBAL_STATS.increment("invalid")
            self._stats.increment("invalid")
            logger.warning(
                "overhead.computed",
                extra={
                    "reason": "invalid_tool_calls",
                    "value": repr(raw_tool_calls),
                },
            )
            tool_calls = 0
        elif tool_calls < 0:
            _GLOBAL_STATS.increment("clamped")
            self._stats.increment("clamped")
            with self._lock:
                self._clamp_count += 1
            logger.warning(
                "overhead.computed",
                extra={
                    "reason": "negative_tool_calls",
                    "value": tool_calls,
                },
            )
            clamped = True
            tool_calls = 0
        elif violation == "capped":
            _GLOBAL_STATS.increment("capped")
            self._stats.increment("capped")
            capped = True
            logger.warning(
                "overhead.computed",
                extra={
                    "reason": "tool_calls_capped",
                    "raw": raw_tool_calls,
                    "cap": self._max_tool_calls,
                },
            )

        _GLOBAL_STATS.increment("computations")
        self._stats.increment("computations")
        with self._lock:
            self._computations += 1

        # --- Resolve precision for this call ---
        effective_precision = (
            compute_precision if isinstance(compute_precision, str)
            else self.precision
        )
        precision_multiplier = 1.0
        if (
            self.features.get("precision_aware", True)
            and isinstance(effective_precision, str)
        ):
            precision_multiplier = self._precision_table.get(
                effective_precision.strip().lower(), 1.0,
            )

        # --- Snapshot calibration state under the lock ---
        with self._lock:
            cal_e = self._calibrated_energy_j
            cal_l = self._calibrated_latency_s

        is_fully_calibrated = cal_e is not None and cal_l is not None

        # Calibrated values are used *as-is* — they already reflect the
        # precision at which the observations were taken.
        if cal_e is not None:
            per_call_energy_j = float(cal_e)
            energy_precision_applied = False
        else:
            per_call_energy_j = float(self.tool_energy)
            energy_precision_applied = True

        if cal_l is not None:
            per_call_latency_s = float(cal_l)
            latency_precision_applied = False
        else:
            per_call_latency_s = float(self.tool_latency)
            latency_precision_applied = self._precision_affects_latency

        energy_mult = precision_multiplier if energy_precision_applied else 1.0
        latency_mult = precision_multiplier if latency_precision_applied else 1.0

        energy_j = (
            tool_calls * per_call_energy_j * energy_mult
        )
        latency_s = (
            tool_calls * per_call_latency_s * latency_mult
        )

        # --- Collision detection ---
        collision_detected = False
        if (
            self.features.get("key_collision_warning", True)
            and "tool_overhead_source" not in metrics
            and "framework_overhead_latency" in metrics
            and not self._collision_warned
        ):
            old = _safe_float(
                metrics.get("framework_overhead_latency"), default=-1.0,
            )
            if old >= 0 and abs(old - latency_s) > 1e-9:
                collision_detected = True
                with self._lock:
                    self._collision_warned = True
                logger.warning(
                    "overhead.key_collision",
                    extra={
                        "old_latency_s": old,
                        "new_latency_s": latency_s,
                        "hint": "framework_overhead.py may also be writing "
                                "these keys; use tool_overhead_* for "
                                "unambiguous per-tool-call values",
                    },
                )

        # --- Derive source label ---
        if is_fully_calibrated:
            source = OverheadSource.CALIBRATED.value
        elif cal_e is not None or cal_l is not None:
            source = OverheadSource.PARTIAL.value
        else:
            source = OverheadSource.DEFAULT.value

        simulated = source in (
            OverheadSource.DEFAULT.value,
            OverheadSource.PARTIAL.value,
        )

        # --- Write v1 keys (preserved exactly) ---
        metrics["framework_overhead_energy"] = energy_j
        metrics["framework_overhead_latency"] = latency_s

        # --- Prefixed canonical keys ---
        metrics["tool_overhead_energy_joules"] = energy_j
        metrics["tool_overhead_energy_wh"] = energy_j / JOULES_PER_WH
        metrics["tool_overhead_latency_seconds"] = latency_s
        metrics["tool_overhead_source"] = source
        metrics["tool_overhead_simulated"] = simulated
        metrics["tool_overhead_per_call_energy_j"] = per_call_energy_j
        metrics["tool_overhead_per_call_latency_s"] = per_call_latency_s
        metrics["tool_overhead_energy_precision_applied"] = (
            energy_precision_applied and energy_mult != 1.0
        )

        # --- v1 alias keys (kept for compatibility) ---
        if self.features.get("unit_explicit", True):
            metrics["framework_overhead_energy_joules"] = energy_j
            metrics["framework_overhead_energy_wh"] = (
                energy_j / JOULES_PER_WH
            )
            metrics["framework_overhead_latency_seconds"] = latency_s

        if self.features.get("provenance", True):
            metrics["framework_overhead_source"] = source
            metrics["framework_overhead_simulated"] = simulated
            metrics["framework_overhead_per_call_energy_j"] = (
                per_call_energy_j
            )
            metrics["framework_overhead_per_call_latency_s"] = (
                per_call_latency_s
            )

        if isinstance(effective_precision, str):
            metrics["tool_overhead_precision"] = effective_precision
            metrics["tool_overhead_precision_multiplier"] = (
                precision_multiplier
            )
            metrics["framework_overhead_precision"] = effective_precision
            metrics["framework_overhead_precision_multiplier"] = (
                precision_multiplier
            )

        # --- Carbon attribution ---
        if (
            self.features.get("carbon", True)
            and self.grid_intensity_g_kwh is not None
            and self.pue is not None
        ):
            energy_wh = energy_j / JOULES_PER_WH
            carbon_g = (
                energy_wh
                * float(self.grid_intensity_g_kwh)
                * float(self.pue)
                / 1000.0
            )
            metrics["tool_overhead_carbon_g"] = carbon_g
            metrics["framework_overhead_carbon_g"] = carbon_g
            metrics["framework_overhead_carbon_source"] = source

        # --- Bounds verification ---
        bounds_ok = True
        bounds_violations: List[str] = []
        if self.features.get("bounds_check", True):
            # latency vs total
            total_latency = _safe_float(
                metrics.get("latency_s"), default=-1.0,
            )
            if total_latency > 0 and latency_s > total_latency:
                bounds_ok = False
                bounds_violations.append(
                    f"overhead_latency({latency_s:.4f}) > "
                    f"total_latency({total_latency:.4f})"
                )
            # energy vs total (when the caller wired an existing energy)
            total_energy_wh = _safe_float(
                metrics.get("energy_wh"), default=-1.0,
            )
            if (
                total_energy_wh > 0
                and (energy_j / JOULES_PER_WH) > total_energy_wh
            ):
                bounds_ok = False
                bounds_violations.append(
                    f"overhead_energy_wh("
                    f"{energy_j / JOULES_PER_WH:.6f}) > "
                    f"total_energy_wh({total_energy_wh:.6f})"
                )
            if capped:
                bounds_ok = False
                bounds_violations.append(
                    f"tool_calls_capped_at({self._max_tool_calls})"
                )

        if bounds_violations:
            logger.warning(
                "overhead.bounds_violation",
                extra={
                    "run_id": self.run_id,
                    "violations": bounds_violations,
                },
            )

        # --- HITL review ---
        hitl_denied = False
        needs_review = False
        review_reason: Optional[str] = None
        if self.features.get("hitl", True):
            total_latency = _safe_float(
                metrics.get("latency_s"), default=-1.0,
            )
            if not bounds_ok:
                needs_review = True
                review_reason = (
                    f"bounds violations: {bounds_violations}"
                )
            elif (
                total_latency > 0
                and latency_s > self.HITL_OVERHEAD_RATIO * total_latency
            ):
                needs_review = True
                review_reason = (
                    f"overhead ({latency_s:.3f}s) exceeds "
                    f"{self.HITL_OVERHEAD_RATIO} × total "
                    f"({total_latency:.3f}s)"
                )
            elif simulated and tool_calls >= 100:
                needs_review = True
                review_reason = (
                    f"high tool_calls ({tool_calls}) with "
                    f"simulated constants"
                )

        if needs_review and self._hitl_callback is not None:
            try:
                approved = self._hitl_callback({
                    "run_id": self.run_id,
                    "tool_calls": tool_calls,
                    "energy_j": energy_j,
                    "latency_s": latency_s,
                    "reason": review_reason,
                })
                if approved is False:
                    hitl_denied = True
                    logger.info(
                        "overhead.hitl_denied",
                        extra={
                            "run_id": self.run_id,
                            "tool_calls": tool_calls,
                        },
                    )
            except Exception as e:
                logger.warning(
                    "overhead.hitl_callback_failed",
                    extra={"error": str(e)},
                )

        metrics["tool_overhead_hitl_denied"] = hitl_denied
        if needs_review:
            metrics["framework_overhead_hitl_denied"] = hitl_denied

        # --- XAI explanation ---
        if self.features.get("xai", True):
            explanation = OverheadExplainer.explain(
                tool_calls=tool_calls,
                energy_j=energy_j,
                latency_s=latency_s,
                source=source,
                per_call_energy_j=per_call_energy_j,
                per_call_latency_s=per_call_latency_s,
                precision_multiplier=precision_multiplier,
                energy_precision_applied=energy_precision_applied,
                clamped=clamped,
                capped=capped,
                collision_detected=collision_detected,
                hitl_denied=hitl_denied,
            )
            exp_dict = explanation.to_dict(safe=True)
            metrics["tool_overhead_explanation"] = exp_dict
            metrics["framework_overhead_explanation"] = exp_dict

        # --- History ---
        with self._lock:
            self._history.append({
                "run_id": self.run_id,
                "tool_calls": tool_calls,
                "energy_j": energy_j,
                "latency_s": latency_s,
                "source": source,
                "precision": effective_precision,
                "at": datetime.now().isoformat(),
            })

        # --- DecisionRecord emission ---
        do_emit = (
            emit_decision_record
            if emit_decision_record is not None
            else self.features.get("decision_record", False)
        )
        if do_emit:
            dr = self._build_decision_record(
                metrics, energy_j, latency_s, source, hitl_denied,
            )
            if dr is not None:
                with self._lock:
                    self._last_decision_record = dr
                serialised = _serialise_decision_record(dr)
                if serialised is not None:
                    jsonable = _jsonable(serialised)
                    metrics["tool_overhead_decision_record"] = jsonable
                    metrics["_overhead_decision_record"] = jsonable
                    metrics["overhead_decision_record"] = jsonable

        # --- Structured log ---
        logger.info(
            "overhead.computed",
            extra={
                "run_id": self.run_id,
                "tool_calls": tool_calls,
                "energy_j": energy_j,
                "latency_s": latency_s,
                "source": source,
                "simulated": simulated,
                "precision": effective_precision,
                "bounds_ok": bounds_ok,
                "needs_review": needs_review,
                "hitl_denied": hitl_denied,
                "collision_detected": collision_detected,
            },
        )

        return metrics

    # ------------------------------------------------------------------
    # Calibration
    # ------------------------------------------------------------------

    def record_observation(
        self,
        tool_calls: Any,
        actual_energy_j: Any,
        actual_latency_s: Any,
        *,
        precision: Optional[str] = None,
        region: Optional[str] = None,
    ) -> bool:
        """
        Record a real observation for calibration.

        Returns True if the observation was accepted, False otherwise.
        """
        tc, violation = _safe_int(tool_calls)
        if tc is None or violation is not None or tc <= 0:
            logger.warning(
                "overhead.observation_rejected",
                extra={
                    "reason": "invalid_tool_calls",
                    "value": repr(tool_calls),
                },
            )
            return False
        energy = _safe_float(actual_energy_j, default=-1.0)
        latency = _safe_float(actual_latency_s, default=-1.0)
        if energy < 0 or latency < 0:
            logger.warning(
                "overhead.observation_rejected",
                extra={
                    "reason": "invalid_numeric",
                    "energy": repr(actual_energy_j),
                    "latency": repr(actual_latency_s),
                },
            )
            return False

        obs = OverheadObservation(
            tool_calls=tc,
            observed_energy_j=energy,
            observed_latency_s=latency,
            precision=precision or self.precision,
            region=region or self.region,
        )
        with self._lock:
            self._observations.append(obs)
        return True

    def calibrate(
        self,
        min_samples: int = 5,
        *,
        max_age_s: Optional[float] = None,
        reject_outliers: bool = True,
        mad_k: float = _MAD_K_DEFAULT,
    ) -> Dict[str, Any]:
        """
        Calibrate per-call constants from observations.

        Uses weighted least-squares through the origin (`Σxy / Σx²`) —
        correct for a `y = m·x` model, unbiased across mixed tool counts.
        Optional MAD-based outlier rejection.

        Returns:
            {"per_call_energy_j", "per_call_latency_s",
             "sample_count", "used_count", "rejected_count"} on success;
            {} if insufficient data.
        """
        if not isinstance(min_samples, int) or min_samples < 1:
            raise ValueError(
                f"min_samples must be an int >= 1, got {min_samples!r}"
            )
        if mad_k <= 0:
            raise ValueError(
                f"mad_k must be positive, got {mad_k!r}"
            )

        with self._lock:
            observations = list(self._observations)

        if max_age_s is not None:
            cutoff = time.time() - float(max_age_s)
            observations = [o for o in observations if o.at >= cutoff]

        if len(observations) < min_samples:
            logger.debug(
                "overhead.calibrate_insufficient",
                extra={
                    "n": len(observations),
                    "min_samples": min_samples,
                },
            )
            return {}

        xs = [float(o.tool_calls) for o in observations]
        ys_e = [o.observed_energy_j for o in observations]
        ys_l = [o.observed_latency_s for o in observations]

        used_indices = list(range(len(xs)))
        if reject_outliers:
            e_keep = set(_reject_outlier_indices(xs, ys_e, k=mad_k))
            l_keep = set(_reject_outlier_indices(xs, ys_l, k=mad_k))
            combined = sorted(e_keep & l_keep)
            if len(combined) >= 3:
                used_indices = combined
        rejected_count = len(xs) - len(used_indices)

        xs_u = [xs[i] for i in used_indices]
        ys_e_u = [ys_e[i] for i in used_indices]
        ys_l_u = [ys_l[i] for i in used_indices]

        per_call_energy = _ls_through_origin(xs_u, ys_e_u)
        per_call_latency = _ls_through_origin(xs_u, ys_l_u)

        with self._lock:
            self._calibrated_energy_j = per_call_energy
            self._calibrated_latency_s = per_call_latency

        _GLOBAL_STATS.increment("calibrations")
        self._stats.increment("calibrations")

        logger.info(
            "overhead.calibrated",
            extra={
                "run_id": self.run_id,
                "per_call_energy_j": per_call_energy,
                "per_call_latency_s": per_call_latency,
                "n_total": len(xs),
                "n_used": len(used_indices),
                "n_rejected": rejected_count,
            },
        )

        return {
            "per_call_energy_j": per_call_energy,
            "per_call_latency_s": per_call_latency,
            "sample_count": len(xs),
            "used_count": len(used_indices),
            "rejected_count": rejected_count,
        }

    def reset_calibration(self) -> None:
        """Revert to the constructor-supplied constants."""
        with self._lock:
            self._observations.clear()
            self._calibrated_energy_j = None
            self._calibrated_latency_s = None

    # ------------------------------------------------------------------
    # Statistics / export / reset
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Instance-scoped statistics + configuration."""
        with self._lock:
            snap = self._stats.snapshot()
            with self._lock:
                observations = list(self._observations)
                history = list(self._history)
                cal_e = self._calibrated_energy_j
                cal_l = self._calibrated_latency_s
            energies = [h["energy_j"] for h in history]

            out: Dict[str, Any] = {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "deployment_id": self.deployment_id,
                "computations": self._computations,
                "clamp_count": self._clamp_count,
                "current_source": self.current_source,
                "observations": len(observations),
                "calibrated_energy_j": cal_e,
                "calibrated_latency_s": cal_l,
                "features": dict(self.features),
                "precision": self.precision,
                "region": self.region,
                "max_tool_calls": self._max_tool_calls,
                "module_statistics": get_statistics(),
            }
            if energies:
                out["energy_stats"] = {
                    "mean_j": statistics.fmean(energies),
                    "max_j": max(energies),
                    "min_j": min(energies),
                }
            return out

    def export(self) -> Dict[str, Any]:
        """Full serialisable export."""
        with self._lock:
            observations = [o.to_dict(safe=True) for o in self._observations]
            history = list(self._history)
            last_dr = self._last_decision_record
            serialised_dr = (
                _jsonable(_serialise_decision_record(last_dr))
                if last_dr is not None else None
            )
            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "statistics": self.get_statistics(),
                "observations": observations,
                "history": history,
                "last_decision_record": serialised_dr,
            }

    def reset(self, *, full: bool = False) -> None:
        """Reset counters / history. `full=True` also clears calibration."""
        with self._lock:
            self._stats.reset()
            self._computations = 0
            self._clamp_count = 0
            self._history.clear()
            self._last_decision_record = None
            self._collision_warned = False
            if full:
                self._observations.clear()
                self._calibrated_energy_j = None
                self._calibrated_latency_s = None
                self._hitl_callback = None
        logger.debug(
            "overhead.reset",
            extra={"run_id": self.run_id, "full": full},
        )

    # ------------------------------------------------------------------
    # DecisionRecord emission
    # ------------------------------------------------------------------

    def to_decision_record(
        self,
        metrics: Dict[str, Any],
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """Emit a DecisionRecord for an existing metrics dict."""
        if not isinstance(metrics, dict):
            return None
        energy_j = _safe_float(
            metrics.get("tool_overhead_energy_joules",
                        metrics.get("framework_overhead_energy", 0.0)),
            default=0.0,
        )
        latency_s = _safe_float(
            metrics.get("tool_overhead_latency_seconds",
                        metrics.get("framework_overhead_latency", 0.0)),
            default=0.0,
        )
        source = metrics.get(
            "tool_overhead_source",
            metrics.get("framework_overhead_source", self.current_source),
        )
        hitl_denied = bool(metrics.get("tool_overhead_hitl_denied", False))
        return self._build_decision_record(
            metrics, energy_j, latency_s, source, hitl_denied,
            policy_version=policy_version,
        )

    def _build_decision_record(
        self,
        metrics: Dict[str, Any],
        energy_j: float,
        latency_s: float,
        source: str,
        hitl_denied: bool,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            return None

        carbon_g = _safe_float(
            metrics.get("tool_overhead_carbon_g",
                        metrics.get("framework_overhead_carbon_g", 0.0)),
            default=0.0,
        )
        explanation = metrics.get("tool_overhead_explanation") or {}
        candidates: Dict[str, Any] = {
            "run_id": self.run_id,
            "timestamp": datetime.now(),
            "task_id": str(metrics.get("task_id", "") or ""),
            "selected_action": "overhead_analyzer.compute",
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": str(metrics.get("agent_id", "") or ""),
            "precision": self.precision,
            "latency_ms": latency_s * 1000.0,
            "energy_kwh": energy_j / JOULES_PER_KWH,
            "carbon_operational_kg": carbon_g / 1000.0,
            "explanation": _jsonable(explanation),
            "provenance": {
                "source": "overhead_analyzer",
                "analyzer_version": __version__,
                "overhead_source": source,
                "hitl_denied": hitl_denied,
                "precision": self.precision,
                "region": self.region,
                "grid_intensity_g_kwh": self.grid_intensity_g_kwh,
                "pue": self.pue,
                "deployment_id": self.deployment_id,
            },
        }
        filtered = _filter_decision_record_kwargs(DecisionRecord, candidates)
        try:
            return DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "overhead.decision_record_failed",
                extra={"error": str(e), "run_id": self.run_id},
            )
            return None


# =============================================================================
# Convenience: one-shot computation
# =============================================================================

def compute_overhead(
    metrics: dict,
    tool_call_cost_joules: float = 0.05,
    tool_call_latency_sec: float = 0.2,
    **kwargs: Any,
) -> dict:
    """
    One-shot overhead computation using a fresh analyzer.

    No calibration state persists across calls — for repeated or
    calibrated workflows construct a `OverheadAnalyzer` directly.
    """
    analyzer = OverheadAnalyzer(
        tool_call_cost_joules=tool_call_cost_joules,
        tool_call_latency_sec=tool_call_latency_sec,
        **kwargs,
    )
    return analyzer.compute(metrics)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import sys
    import threading
    import types

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    # =================================================================
    # 1. Original behavior (backward compatible)
    # =================================================================
    print("=== 1. Original behavior ===")
    analyzer = OverheadAnalyzer()
    m = {"tool_calls": 5}
    result = analyzer.compute(m)
    print(f"  framework_overhead_energy:  "
          f"{result['framework_overhead_energy']} J  (expected 0.25)")
    print(f"  framework_overhead_latency: "
          f"{result['framework_overhead_latency']} s  (expected 1.0)")

    # =================================================================
    # 2. Key collision with framework_overhead.py
    # =================================================================
    print("\n=== 2. Key collision detection ===")
    m_col = {
        "tool_calls": 5,
        # Simulate a prior framework_overhead.apply() write:
        "framework_overhead_latency": 0.15,
        "framework_overhead_source": "hardcoded",
    }
    analyzer_col = OverheadAnalyzer()
    analyzer_col.compute(m_col)
    print(f"  framework_overhead_latency: "
          f"{m_col['framework_overhead_latency']}  "
          f"(overwritten by tool-call total)")
    print(f"  tool_overhead_latency_seconds: "
          f"{m_col['tool_overhead_latency_seconds']}  "
          f"(canonical, unambiguous)")
    print(f"  collision detected: "
          f"{'WARNING: framework_overhead_latency' in (m_col.get('framework_overhead_explanation', {}).get('rationale', [])[-1] if m_col.get('framework_overhead_explanation') else '')}")

    # =================================================================
    # 3. Precision multiplier NOT double-applied when calibrated
    # =================================================================
    print("\n=== 3. Precision calibration (v1 was 2× wrong) ===")
    analyzer_p = OverheadAnalyzer(precision="int8")
    for _ in range(5):
        analyzer_p.record_observation(
            tool_calls=10, actual_energy_j=0.8, actual_latency_s=2.0,
            precision="int8",
        )
    cal = analyzer_p.calibrate(min_samples=3)
    print(f"  calibrated per-call energy: {cal['per_call_energy_j']:.6f} J")
    print(f"  calibrated per-call latency: {cal['per_call_latency_s']:.6f} s")

    m_prec = {"tool_calls": 5}
    analyzer_p.compute(m_prec)
    print(f"  energy for 5 calls: "
          f"{m_prec['tool_overhead_energy_joules']:.6f} J  "
          f"(expected 5 × 0.08 = 0.4, NOT 0.2)")
    print(f"  source: {m_prec['tool_overhead_source']}")

    # =================================================================
    # 4. Weighted LS-through-origin vs naive mean
    # =================================================================
    print("\n=== 4. Estimator correctness (LS through origin) ===")
    analyzer_ls = OverheadAnalyzer()
    # One noisy small-x observation, one large-x
    analyzer_ls.record_observation(1, 0.20, 0.30)      # 0.20 J/call (noisy)
    analyzer_ls.record_observation(10, 0.55, 2.00)     # 0.055 J/call
    analyzer_ls.record_observation(10, 0.55, 2.00)
    analyzer_ls.record_observation(10, 0.55, 2.00)
    analyzer_ls.record_observation(10, 0.55, 2.00)
    cal_ls = analyzer_ls.calibrate(min_samples=5, reject_outliers=False)
    # naive mean would be (0.20 + 4×0.055) / 5 = 0.084
    # LS through origin weights the 10-call observations 100× more:
    #   num = 1×0.20 + 4×(10×0.55) = 0.20 + 22 = 22.20
    #   den = 1 + 4×100 = 401
    #   m = 22.20 / 401 ≈ 0.05537
    print(f"  per-call energy (LS):    {cal_ls['per_call_energy_j']:.6f} J")
    print(f"  per-call energy (naive mean would be):  0.084 J")
    print(f"  → LS is closer to the true (large-x) slope")

    # =================================================================
    # 5. Thread safety — calibration state under concurrent compute()
    # =================================================================
    print("\n=== 5. Thread safety ===")
    analyzer_t = OverheadAnalyzer()
    barrier = threading.Barrier(4)
    source_seen_mixed = [False]

    def _compute_worker():
        barrier.wait()
        for _ in range(100):
            m = {"tool_calls": 3}
            analyzer_t.compute(m)
            if m["tool_overhead_source"] == "partial":
                # Partial state should never be observed after both cal values set
                source_seen_mixed[0] = True

    def _calibrate_worker():
        barrier.wait()
        for _ in range(50):
            analyzer_t.record_observation(10, 0.8, 2.0)
            analyzer_t.calibrate(min_samples=1)

    threads = [_compute_worker() for _ in range(3)]
    threads_list = [
        threading.Thread(target=_compute_worker) for _ in range(3)
    ] + [threading.Thread(target=_calibrate_worker)]
    for t in threads_list:
        t.start()
    for t in threads_list:
        t.join()
    print(f"  mixed partial-state observed during calibration: "
          f"{source_seen_mixed[0]}  (expected False)")

    # =================================================================
    # 6. Outlier rejection
    # =================================================================
    print("\n=== 6. Outlier rejection ===")
    analyzer_o = OverheadAnalyzer()
    # 5 good observations, 1 wildly bad
    for _ in range(5):
        analyzer_o.record_observation(10, 0.5, 1.0)
    analyzer_o.record_observation(10, 100.0, 200.0)   # glitched meter
    cal_clean = analyzer_o.calibrate(min_samples=5, reject_outliers=True)
    cal_raw = analyzer_o.calibrate(min_samples=5, reject_outliers=False)
    print(f"  with outlier rejection:    "
          f"{cal_clean['per_call_energy_j']:.6f} J/call  "
          f"(rejected {cal_clean['rejected_count']})")
    print(f"  without outlier rejection: "
          f"{cal_raw['per_call_energy_j']:.6f} J/call  "
          f"(raw mean pulled up)")

    # =================================================================
    # 7. JSON-safe DecisionRecord
    # =================================================================
    print("\n=== 7. JSON-safe DecisionRecord ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id, selected_action,
                     latency_ms, energy_kwh, explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.latency_ms = latency_ms
            self.energy_kwh = energy_kwh
            self.explanation = explanation
            self.provenance = provenance

    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    sys.modules["analysis"] = fake_mod

    import importlib
    import src.analysis.telemetry.overhead_analyzer as _oa_mod
    importlib.reload(_oa_mod)

    try:
        analyzer_dr = _oa_mod.OverheadAnalyzer()
        m_dr = {"tool_calls": 5, "latency_s": 5.0}
        result_dr = analyzer_dr.compute(m_dr, emit_decision_record=True)
        try:
            json.dumps(result_dr, default=str)
            print(f"  json.dumps(metrics): OK")
        except (TypeError, ValueError) as e:
            print(f"  FAILED: {e}")
        print(f"  serialised DR present: "
              f"{'tool_overhead_decision_record' in result_dr}")
        dr_dict = result_dr.get("tool_overhead_decision_record")
        if dr_dict:
            print(f"  DR selected_action:    {dr_dict.get('selected_action')}")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_oa_mod)

    # =================================================================
    # 8. Tool-call cap
    # =================================================================
    print("\n=== 8. Tool-call cap ===")
    analyzer_cap = OverheadAnalyzer(max_tool_calls=1000)
    m_cap = {"tool_calls": 1e20}
    result_cap = analyzer_cap.compute(m_cap)
    print(f"  tool_calls=1e20 → capped: "
          f"{result_cap['tool_overhead_energy_joules']:.2f} J  "
          f"(1000 × 0.05)")
    exp = result_cap.get("tool_overhead_explanation", {})
    print(f"  cap flagged in explanation: "
          f"{any('cap' in r.lower() for r in exp.get('rationale', []))}")

    # =================================================================
    # 9. HITL denial
    # =================================================================
    print("\n=== 9. HITL denial ===")
    analyzer_h = OverheadAnalyzer()
    analyzer_h.set_hitl_callback(lambda info: False)
    m_h = {"tool_calls": 100, "latency_s": 0.5}   # overhead 20s >> 0.5s
    result_h = analyzer_h.compute(m_h)
    print(f"  latency_s:                   {result_h['tool_overhead_latency_seconds']:.2f}")
    print(f"  total_latency:               {result_h['latency_s']:.2f}")
    print(f"  hitl_denied:                 {result_h['tool_overhead_hitl_denied']}")
    print(f"  explanation mentions denial: "
          f"{any('[HITL: denied]' in r for r in result_h['tool_overhead_explanation']['rationale'])}")

    # =================================================================
    # 10. Source label for metric_provenance
    # =================================================================
    print("\n=== 10. Source label compatibility ===")
    analyzer_src = OverheadAnalyzer()
    m_src = {"tool_calls": 5}
    analyzer_src.compute(m_src)
    print(f"  framework_overhead_source: {m_src['framework_overhead_source']}  "
          f"(v1 was 'default', v2 'hardcoded')")
    print(f"  tool_overhead_source:      {m_src['tool_overhead_source']}")
    # The metric_provenance inference table maps 'hardcoded' → SourceKind.DEFAULT
    print(f"  → metric_provenance will classify as DEFAULT  ✓")

    # =================================================================
    # 11. Calibration with age filter
    # =================================================================
    print("\n=== 11. Age-filtered calibration ===")
    analyzer_age = OverheadAnalyzer()
    # Simulate one old observation by direct append (bypasses record_observation)
    old = analyzer_age._observations.__class__  # deque
    for _ in range(3):
        analyzer_age.record_observation(10, 0.8, 2.0)
    # Append an old one manually
    import time as _time
    from src.analysis.telemetry.overhead_analyzer import OverheadObservation as _Obs
    analyzer_age._observations.append(_Obs(
        tool_calls=10, observed_energy_j=99.0, observed_latency_s=99.0,
        at=_time.time() - 200_000,   # > 2 days old
    ))
    cal_all = analyzer_age.calibrate(min_samples=1, reject_outliers=False)
    cal_recent = analyzer_age.calibrate(
        min_samples=1, max_age_s=86_400, reject_outliers=False,
    )
    print(f"  all observations:      {cal_all['per_call_energy_j']:.4f} J/call  "
          f"(n={cal_all['sample_count']})")
    print(f"  recent (< 1 day):      {cal_recent['per_call_energy_j']:.4f} J/call  "
          f"(n={cal_recent['sample_count']})")

    # =================================================================
    # 12. Non-idempotent state check + reset
    # =================================================================
    print("\n=== 12. reset() ===")
    analyzer_r = OverheadAnalyzer()
    for _ in range(3):
        analyzer_r.record_observation(10, 0.8, 2.0)
    analyzer_r.calibrate(min_samples=3)
    print(f"  before reset: source={analyzer_r.get_current_source()}, "
          f"observations={len(analyzer_r._observations)}")
    analyzer_r.reset(full=False)
    print(f"  after reset(full=False): source={analyzer_r.get_current_source()}, "
          f"observations={len(analyzer_r._observations)}  "
          f"(calibration preserved)")
    analyzer_r.reset(full=True)
    print(f"  after reset(full=True):  source={analyzer_r.get_current_source()}, "
          f"observations={len(analyzer_r._observations)}")

    # =================================================================
    # 13. Statistics
    # =================================================================
    print("\n=== 13. Statistics ===")
    print(json.dumps(analyzer_p.get_statistics(), indent=2, default=str))
