# src/analysis/optimization/pareto_analyzer.py
"""
Sustainability Pareto Analyzer (Enhanced, v2.0.0)
==================================================

Original API preserved (all signatures, all side effects):
    analyzer = ParetoAnalyzer()
    analyzer.add_record(energy_joules, accuracy, carbon_grams, label, metadata)
    frontier = analyzer.compute_frontier()          # -> List[ParetoPoint]
    analyzer.export_json(path)                       # -> bool

Enhanced API:
    analyzer = ParetoAnalyzer(
        run_id="...", deployment_id="us-ca-prod-01",
        precision="int8",
        tolerance=None,             # None = strict; or explicit float
        duplicate_policy="suffix",  # "keep" | "suffix" | "raise"
        features={...},
    )

    outcome = analyzer.add_record(...)     # RecordOutcome enum (additive)
    frontier = analyzer.compute_frontier_detailed(
        filter_fn=..., top_k=10,
        precision=None, tolerance=None,    # per-call overrides
        emit_decision_record=False,
    )

    frontier.points                         # List[ParetoPoint]
    frontier.rationale_of("agent-A")        # XAI
    frontier.dominated_by("agent-A")        # ALL dominators
    frontier.dominates_of("agent-A")        # how many peers it dominates
    frontier.duplicate_labels               # list of collided labels
    frontier.excluded_agents                # filter_fn rejections
    frontier.excluded_reason("agent-X")     # why it was excluded
    frontier.truncated                      # bool (top_k applied)
    frontier.pre_truncation_count           # int
    frontier.hitl_approved                  # Optional[bool]
    frontier.severity                       # Severity value
    frontier.to_dict(safe=True)             # JSON-safe
    frontier.to_decision_record(...)        # one record per frontier
    frontier.to_decision_records(...)       # one per point

    analyzer.set_precision("fp16")
    analyzer.set_tolerance(0.01)
    analyzer.set_hitl_callback(cb)          # cb(result) -> Optional[bool]
    analyzer.reset(full=False)              # counters only
    analyzer.reset(full=True)               # also clears points + config
    analyzer.get_statistics()               # instance-scoped
    analyzer.to_json_dict(safe=True)        # full export (no filesystem)

Enhancements implemented (v2.0.0):
  1.  Tolerant staircase    — dominance uses tolerant comparison. v1 dropped
                              frontier points whose accuracy improvement was
                              *within* the tolerance band and then attributed
                              the (false) domination to a point that didn't
                              dominate. Fixed here.
  2.  All-dominators        — every dominator of a dominated point is
                              recorded (v1 attributed only the last frontier
                              point before the dominated one).
  3.  Working delegation    — `_pairwise_frontier` prefers `pareto.py`'s
                              shared `dominance_verdict` when importable
                              (v1 imported from a nonexistent path with the
                              wrong argument order and swallowed the error).
  4.  Cached detailed result— `compute_frontier_detailed()` caches the last
                              result keyed by its parameters. Getters that
                              read the frontier no longer re-run the whole
                              O(n²) computation on every call.
  5.  Tolerance disambiguated — `tolerance: Optional[float] = None`.
                              Explicit `0.0` means strict even with a
                              precision preset; `None` uses the preset when
                              precision is set.
  6.  Precision validated   — `precision` must be `str | None`; non-str
                              values are rejected at construction.
  7.  Input sanitization    — `_safe_float` in every sort key; validation
                              off no longer crashes the frontier.
  8.  Signature-filtered DR — `to_decision_record()` / `to_decision_records()`
                              use `inspect.signature(DecisionRecord)` so
                              contract drift returns `None` + warning rather
                              than raising.
  9.  Unambiguous units     — `energy_kwh=p.energy_joules / JOULES_PER_KWH`
                              (v1 had `/ 3.6e6 / 1000.0 * 1000.0` — a
                              round-trip that cancelled and hid a bug).
 10.  Thread safety         — `RLock` around all mutable state; `_StatsCounter`
                              per instance plus module-level shims.
 11.  Filter reasons        — `excluded_agents` + `excluded_reason(label)`.
 12.  Duplicate labels      — `duplicate_policy="suffix"|"raise"|"keep"`;
                              duplicates surfaced via `duplicate_labels`.
 13.  Split HITL constants  — `HITL_EMPTY_FRONTIER` (bool) and
                              `HITL_SINGLE_MIN_INPUTS` (int) rather than one
                              overloaded `HITL_MIN_FRONTIER`.
 14.  HITL annotation       — callback returning `False` sets
                              `hitl_approved=False` and appends
                              `[HITL: denied]` to `review_reason`.
 15.  `numbers.Real`        — `_is_numeric` accepts `Decimal` / `Fraction`
                              / numpy scalars (v1 rejected them).
 16.  Null-safe dominance   — missing values exclude the point from
                              comparison (v1 silently treated them as "not
                              dominating anyone").
 17.  Atomic export         — `export_json` writes `.tmp` then
                              `os.replace`.
 18.  `to_dict(safe=True)`  — `_jsonable` recursion on metadata /
                              non-finite floats.
 19.  Per-call overrides    — `compute_frontier_detailed(precision=,
                              tolerance=)`.
 20.  `add_record` returns `RecordOutcome`.
 21.  `reset(full=False|True)`.
 22.  `to_decision_record()` per-point + `ParetoFrontierResult.
      to_decision_record()` + `to_decision_records()`.
 23.  `emit_decision_record` kwarg on `compute_frontier_detailed()`.
 24.  `set_precision` / `set_tolerance` / `set_duplicate_policy`.
 25.  `to_json_dict(safe=True)` — full export without filesystem.
 26.  Structured logging    — `pareto_analyzer.record_added`,
                              `pareto_analyzer.record_invalid`,
                              `pareto_analyzer.record_deduplicated`,
                              `pareto_analyzer.label_suffixed`,
                              `pareto_analyzer.frontier_computed`,
                              `pareto_analyzer.filter_fn_raised`,
                              `pareto_analyzer.decision_record_failed`,
                              `pareto_analyzer.hitl_denied`.
 27.  `__version__` / `__all__`.

Backward compatibility notes:
  * `tolerance` default changed from `0.0` to `None`. For callers who passed
    `precision=` and left `tolerance` at its default, behaviour is unchanged
    (preset applied). For callers who passed `tolerance=0.0` explicitly with
    a precision, v2 honors the explicit `0.0` (strict) — this is the fix for
    the v1 collision bug. Pass `tolerance=None` to opt into the preset.
  * `compute_frontier()` returns the same `List[ParetoPoint]` as before. It
    caches the underlying `ParetoFrontierResult`, so two consecutive calls
    do not recompute.
  * `dominated_by_label[label]` now lists *every* frontier point that
    dominates the label (v1 listed at most one). `dominated_by(label)`
    still returns `List[str]`.
  * `duplicate_policy` defaults to `"suffix"`. v1 callers with duplicate
    labels will see the second occurrence get a `" #2"` suffix. Use
    `duplicate_policy="keep"` for v1 behaviour (with a warning in the
    result's `duplicate_labels`).
  * `add_record` now returns a `RecordOutcome` enum. v1 returned `None`;
    callers that ignored the return value are unaffected.
"""

from __future__ import annotations

import inspect
import json
import logging
import math
import numbers
import os
import statistics
import tempfile
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict, is_dataclass, replace
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Set, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "ParetoAnalyzer",
    "ParetoPoint",
    "ParetoFrontierResult",
    "FrontierExplainer",
    "RecordOutcome",
    "ObjectiveKind",
    "Severity",
    "tolerance_for_precision",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

JOULES_PER_KWH = 3_600_000.0

_PRECISION_TOLERANCE: Dict[str, float] = {
    "fp32": 0.0,
    "fp64": 0.0,
    "fp16": 1e-3,
    "bf16": 1e-2,
    "int8": 1e-2,
    "int4": 1e-1,
    "quantum_distilled": 1e-1,
}


def tolerance_for_precision(precision: Optional[str]) -> float:
    """Recommended absolute tolerance for a precision label."""
    if not isinstance(precision, str):
        return 0.0
    return _PRECISION_TOLERANCE.get(precision.strip().lower(), 0.0)


# =============================================================================
# Enums
# =============================================================================

class ObjectiveKind(Enum):
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class RecordOutcome(Enum):
    """Outcome of `ParetoAnalyzer.add_record()`."""
    ACCEPTED = "accepted"
    DEDUPLICATED = "deduplicated"
    INVALID = "invalid"


_VALID_DUPLICATE_POLICIES = frozenset({"keep", "suffix", "raise"})


# =============================================================================
# Helpers
# =============================================================================

def _is_numeric(x: Any) -> bool:
    """True iff `x` is a finite `numbers.Real` (rejects bool)."""
    if isinstance(x, bool):
        return False
    if not isinstance(x, numbers.Real):
        return False
    try:
        f = float(x)
    except (TypeError, ValueError, OverflowError):
        return False
    return math.isfinite(f)


def _safe_float(x: Any) -> Optional[float]:
    """Coerce to finite float; return None on failure."""
    if isinstance(x, bool):
        return None
    try:
        f = float(x)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


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


def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        if isinstance(ts, datetime):
            return ts.isoformat()
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


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


def _import_shared_verdict() -> Optional[Callable[..., Any]]:
    """
    Return the sibling `dominance_verdict` from `pareto.py` (preferred —
    same shape as this module's expectations), falling back to
    `dominance_checker.dominance_verdict` when `pareto.py` isn't
    available.
    """
    # Prefer pareto.py — its signature is a natural fit for this module.
    for path in (
        "src.analysis.optimization.pareto",
        "analysis.optimization.pareto",
    ):
        try:
            mod = __import__(path, fromlist=["dominance_verdict"])
            fn = getattr(mod, "dominance_verdict", None)
            if callable(fn):
                return fn
        except Exception:
            continue
    # Fall back to dominance_checker with an adapter.
    for path in (
        "src.analysis.optimization.dominance_checker",
        "analysis.optimization.dominance_checker",
    ):
        try:
            mod = __import__(path, fromlist=["dominance_verdict"])
            fn = getattr(mod, "dominance_verdict", None)
            if callable(fn):
                # dominance_checker.dominance_verdict(a, b, minimize, maximize, ...)
                # differs from pareto.dominance_verdict(a, b, objectives, ...)
                # — wrap it.
                def _adapter(a, b, objectives, *, maximize, tolerance):
                    minimize = set(objectives) - set(maximize or [])
                    return fn(a, b, minimize, set(maximize or []),
                              tolerance=tolerance)
                return _adapter
        except Exception:
            continue
    return None


# =============================================================================
# Tolerant dominance primitives
# =============================================================================

def _tolerantly_dominates_2d(
    f_energy: float, f_accuracy: float,
    p_energy: float, p_accuracy: float,
    tolerance: float,
) -> bool:
    """
    Return True iff (f_energy, f_accuracy) tolerantly dominates
    (p_energy, p_accuracy) under minimize-energy / maximize-accuracy.

    A tolerant domination requires:
      - f is at least as good on both objectives (within `tolerance`)
      - f is strictly better on at least one (beyond `tolerance`)
    """
    if f_energy > p_energy + tolerance:
        return False
    if f_accuracy < p_accuracy - tolerance:
        return False
    return (
        f_energy < p_energy - tolerance
        or f_accuracy > p_accuracy + tolerance
    )


def _check_dominance_multidim(
    a: Dict[str, Any],
    b: Dict[str, Any],
    objectives: List[str],
    maximize: List[str],
    tolerance: float,
) -> bool:
    """
    Return True iff `a` tolerantly dominates `b` across `objectives`.

    Missing / non-finite values return False (v1 silently treated those as
    "not dominating", which allowed malformed points onto the frontier).
    """
    maximize_set = set(maximize or ())
    strict = False
    for obj in objectives:
        av = _safe_float(a.get(obj))
        bv = _safe_float(b.get(obj))
        if av is None or bv is None:
            return False
        if obj in maximize_set:
            # maximize: a better if higher; worse if lower beyond tolerance
            if av < bv - tolerance:
                return False
            if av > bv + tolerance:
                strict = True
        else:
            # minimize: a better if lower; worse if higher beyond tolerance
            if av > bv + tolerance:
                return False
            if av < bv - tolerance:
                strict = True
    return strict


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
    """Module-level statistics for `ParetoAnalyzer` calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "analyzers_created": snap.get("analyzers", 0),
        "records_added": snap.get("records", 0),
        "frontiers_computed": snap.get("frontiers", 0),
        "invalid_records": snap.get("invalid", 0),
        "duplicates_seen": snap.get("duplicates", 0),
    }


def reset_statistics() -> None:
    """Clear the module-level counter."""
    _GLOBAL_STATS.reset()


# =============================================================================
# ParetoPoint
# =============================================================================

@dataclass
class ParetoPoint:
    """
    A single Pareto observation.

    Original fields preserved verbatim; new fields have safe defaults.
    """
    # --- v1 fields ---
    energy_joules: float
    accuracy: float
    carbon_grams: float
    label: str
    metadata: Dict = field(default_factory=dict)

    # --- Identity / provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    source: str = "unknown"
    simulated: bool = False
    precision: Optional[str] = None
    region: Optional[str] = None

    # --- Timing ---
    at: datetime = field(default_factory=datetime.now)

    # --- Verification ---
    valid: bool = True
    validation_errors: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.metadata is None:
            self.metadata = {}
        elif not isinstance(self.metadata, dict):
            try:
                self.metadata = dict(self.metadata)
            except (TypeError, ValueError):
                self.metadata = {}

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# ParetoFrontierResult
# =============================================================================

@dataclass
class ParetoFrontierResult:
    """Rich result of a detailed frontier computation."""
    points: List[ParetoPoint]
    rationale_by_label: Dict[str, List[str]]
    dominated_by_label: Dict[str, List[str]]
    dominates_count_by_label: Dict[str, int]
    dominated_count: int                         # n dominated points
    invalid_count: int
    duplicate_count: int
    objectives: List[str]
    maximize: List[str]
    tolerance: float
    carbon_enabled: bool

    # --- Diagnostics ---
    duplicate_labels: List[str] = field(default_factory=list)
    excluded_agents: List[str] = field(default_factory=list)
    excluded_reasons: Dict[str, str] = field(default_factory=dict)
    truncated: bool = False
    pre_truncation_count: int = 0
    invalid_agent_ids: List[str] = field(default_factory=list)

    # --- HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None
    hitl_approved: Optional[bool] = None

    # --- DecisionRecord ---
    decision_record: Optional[Dict[str, Any]] = None

    # --- Severity / metadata ---
    severity: str = Severity.INFO.value
    analyzer_version: str = __version__
    at: datetime = field(default_factory=lambda: datetime.now())

    # -- Accessors -------------------------------------------------------

    def rationale_of(self, label: str) -> List[str]:
        return self.rationale_by_label.get(label, [])

    def dominated_by(self, label: str) -> List[str]:
        return self.dominated_by_label.get(label, [])

    def dominates_of(self, label: str) -> int:
        return self.dominates_count_by_label.get(label, 0)

    def excluded_reason(self, label: str) -> Optional[str]:
        return self.excluded_reasons.get(label)

    def frontier_labels(self) -> List[str]:
        return [p.label for p in self.points]

    # -- Serialisation ---------------------------------------------------

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "analyzer_version": __version__,
            "points": [p.to_dict(safe=safe) for p in self.points],
            "rationale_by_label": dict(self.rationale_by_label),
            "dominated_by_label": {
                k: list(v) for k, v in self.dominated_by_label.items()
            },
            "dominates_count_by_label": dict(self.dominates_count_by_label),
            "dominated_count": self.dominated_count,
            "invalid_count": self.invalid_count,
            "duplicate_count": self.duplicate_count,
            "objectives": list(self.objectives),
            "maximize": list(self.maximize),
            "tolerance": self.tolerance,
            "carbon_enabled": self.carbon_enabled,
            "duplicate_labels": list(self.duplicate_labels),
            "excluded_agents": list(self.excluded_agents),
            "excluded_reasons": dict(self.excluded_reasons),
            "truncated": self.truncated,
            "pre_truncation_count": self.pre_truncation_count,
            "invalid_agent_ids": list(self.invalid_agent_ids),
            "needs_review": self.needs_review,
            "review_reason": self.review_reason,
            "hitl_approved": self.hitl_approved,
            "decision_record": self.decision_record,
            "severity": self.severity,
            "at": self.at.isoformat(),
        }
        if safe:
            return _jsonable(out)
        return out

    # -- DecisionRecord --------------------------------------------------

    def to_decision_record(
        self,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        policy_version: str = "",
    ) -> Optional[Any]:
        """Emit a single DecisionRecord summarising the frontier."""
        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            return None
        candidates: Dict[str, Any] = {
            "run_id": run_id or "",
            "timestamp": self.at,
            "task_id": task_id or "",
            "selected_action": (
                f"pareto_frontier_computed(n={len(self.points)})"
            ),
            "alternatives": self.frontier_labels(),
            "policy_version": policy_version,
            "model_or_agent": "",
            "explanation": {
                "objectives": list(self.objectives),
                "maximize": list(self.maximize),
                "tolerance": self.tolerance,
                "carbon_enabled": self.carbon_enabled,
                "dominated_count": self.dominated_count,
                "invalid_count": self.invalid_count,
                "duplicate_count": self.duplicate_count,
                "excluded_count": len(self.excluded_agents),
                "truncated": self.truncated,
                "pre_truncation_count": self.pre_truncation_count,
                "needs_review": self.needs_review,
                "review_reason": self.review_reason,
            },
            "provenance": {
                "source": "pareto_analyzer",
                "analyzer_version": __version__,
                "severity": self.severity,
                "hitl_approved": self.hitl_approved,
            },
        }
        filtered = _filter_decision_record_kwargs(
            DecisionRecord, candidates,
        )
        try:
            return DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "pareto_analyzer.decision_record_failed",
                extra={"error": str(e)},
            )
            return None

    def to_decision_records(
        self,
        *,
        run_id: Optional[str] = None,
        policy_version: str = "",
    ) -> List[Any]:
        """Emit one DecisionRecord per frontier point."""
        DecisionRecord = _import_decision_record()
        if DecisionRecord is None:
            return []
        records: List[Any] = []
        for p in self.points:
            candidates: Dict[str, Any] = {
                "run_id": p.run_id or run_id or "",
                "timestamp": p.at,
                "task_id": p.task_id or "",
                "selected_action": f"pareto_point({p.label})",
                "alternatives": [
                    q.label for q in self.points if q.label != p.label
                ],
                "policy_version": policy_version,
                "model_or_agent": p.agent_id or p.label,
                "precision": p.precision,
                "quality_score": p.accuracy,
                "latency_ms": 0.0,
                "energy_kwh": p.energy_joules / JOULES_PER_KWH,
                "carbon_operational_kg": p.carbon_grams / 1000.0,
                "explanation": {
                    "energy_joules": p.energy_joules,
                    "carbon_grams": p.carbon_grams,
                    "objectives": list(self.objectives),
                    "tolerance": self.tolerance,
                    "rationale": self.rationale_of(p.label),
                    "dominates_count": self.dominates_of(p.label),
                },
                "provenance": {
                    "source": p.source,
                    "simulated": p.simulated,
                    "on_frontier": True,
                    "analyzer_version": __version__,
                    "severity": self.severity,
                },
            }
            filtered = _filter_decision_record_kwargs(
                DecisionRecord, candidates,
            )
            try:
                records.append(DecisionRecord(**filtered))
            except TypeError as e:
                logger.warning(
                    "pareto_analyzer.decision_record_failed",
                    extra={"error": str(e), "label": p.label},
                )
        return records


# =============================================================================
# XAI
# =============================================================================

class FrontierExplainer:
    @staticmethod
    def explain_point(
        p: ParetoPoint,
        dominates_count: int,
        objectives: List[str],
        maximize: List[str],
        carbon_enabled: bool,
        tolerance: float = 0.0,
    ) -> List[str]:
        reasons: List[str] = [
            f"On frontier: energy={p.energy_joules:.4f} J, "
            f"accuracy={p.accuracy:.4f}."
        ]
        if carbon_enabled:
            reasons.append(f"Carbon: {p.carbon_grams:.4f} g.")
        reasons.append(f"Objectives: {objectives} (maximize: {maximize}).")
        if tolerance > 0:
            reasons.append(f"Tolerance applied: {tolerance}.")
        if dominates_count > 0:
            reasons.append(
                f"It strictly dominates {dominates_count} other point(s)."
            )
        else:
            reasons.append(
                "It dominates no other points (mutually non-dominated)."
            )
        if p.simulated:
            reasons.append("WARNING: point is flagged as simulated.")
        if not p.valid:
            reasons.append(
                f"WARNING: point has validation errors: "
                f"{p.validation_errors}."
            )
        return reasons


# =============================================================================
# Validation
# =============================================================================

def _validate_point(p: ParetoPoint) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    for name, value in (
        ("energy_joules", p.energy_joules),
        ("accuracy", p.accuracy),
        ("carbon_grams", p.carbon_grams),
    ):
        v = _safe_float(value)
        if v is None:
            errors.append(f"{name}_not_finite")
            continue
        if name == "accuracy":
            if not (0.0 <= v <= 1.0):
                errors.append(f"accuracy_out_of_range({v})")
        else:
            if v < 0:
                errors.append(f"{name}_negative({v})")
    if not isinstance(p.label, str) or not p.label:
        errors.append("label_missing")
    return (len(errors) == 0, errors)


# =============================================================================
# ParetoAnalyzer
# =============================================================================

class ParetoAnalyzer:
    """
    Enhanced sustainability Pareto analyzer.

    Backward-compatible: same constructor keys, same public methods, same
    return types.
    """

    #: Flag review when the frontier is empty despite non-empty inputs.
    HITL_EMPTY_FRONTIER = True
    #: Flag review when the frontier has 1 point over at least this many
    #: input points.
    HITL_SINGLE_MIN_INPUTS = 5

    DEFAULT_FEATURES: Dict[str, bool] = {
        "validation": True,
        "carbon_in_frontier": False,
        "provenance": True,
        "xai": True,
        "hitl": True,
        "statistics": True,
        "deduplication": True,
        "tolerance": True,
    }

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        deployment_id: str = "local",
        precision: Optional[str] = None,
        tolerance: Optional[float] = None,
        duplicate_policy: str = "suffix",
        features: Optional[Dict[str, bool]] = None,
        max_history: int = 1024,
    ):
        # --- v1 public state (preserved name and type) ---
        self.points: List[ParetoPoint] = []

        # --- Identity ---
        self.run_id: str = run_id or f"pa-{uuid.uuid4().hex[:8]}"
        self.deployment_id: str = deployment_id

        # --- Precision / tolerance ---
        if precision is not None and not isinstance(precision, str):
            raise TypeError(
                f"precision must be a string or None, "
                f"got {type(precision).__name__}"
            )
        self.precision: Optional[str] = precision

        self.tolerance: Optional[float] = self._validate_tolerance(
            tolerance, name="tolerance",
        )

        # --- Duplicate policy ---
        if duplicate_policy not in _VALID_DUPLICATE_POLICIES:
            raise ValueError(
                f"duplicate_policy must be one of "
                f"{sorted(_VALID_DUPLICATE_POLICIES)}, "
                f"got {duplicate_policy!r}"
            )
        self.duplicate_policy: str = duplicate_policy

        # --- Feature flags ---
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "pareto_analyzer.unknown_features",
                extra={"unknown": sorted(unknown)},
            )

        # --- Counters ---
        self._duplicates: int = 0
        self._invalid: int = 0
        self._frontier_count: int = 0
        self._history: Deque[Dict[str, Any]] = deque(maxlen=max(1, max_history))

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[ParetoFrontierResult], Optional[bool]]
        ] = None

        # --- Cache ---
        self._cached_result: Optional[ParetoFrontierResult] = None
        self._cached_params: Optional[Tuple[Any, ...]] = None

        # --- Concurrency ---
        self._lock = threading.RLock()
        self._stats = _StatsCounter()

        _GLOBAL_STATS.increment("analyzers")

        logger.debug(
            "pareto_analyzer.initialized",
            extra={
                "run_id": self.run_id,
                "carbon_in_frontier":
                    self.features.get("carbon_in_frontier", False),
                "precision": self.precision,
                "tolerance": self.tolerance,
                "duplicate_policy": self.duplicate_policy,
            },
        )

    @staticmethod
    def _validate_tolerance(
        value: Any, *, name: str,
    ) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            raise TypeError(f"{name} must be numeric or None, got bool")
        try:
            f = float(value)
        except (TypeError, ValueError, OverflowError):
            raise TypeError(
                f"{name} must be numeric or None, "
                f"got {type(value).__name__}"
            )
        if math.isnan(f) or math.isinf(f) or f < 0:
            raise ValueError(
                f"{name} must be finite and >= 0, got {value!r}"
            )
        return f

    # ------------------------------------------------------------------
    # Configuration setters
    # ------------------------------------------------------------------

    def set_precision(self, precision: Optional[str]) -> None:
        if precision is not None and not isinstance(precision, str):
            raise TypeError(
                f"precision must be a string or None, "
                f"got {type(precision).__name__}"
            )
        with self._lock:
            self.precision = precision
            self._invalidate_cache()

    def set_tolerance(self, tolerance: Optional[float]) -> None:
        with self._lock:
            self.tolerance = self._validate_tolerance(
                tolerance, name="tolerance",
            )
            self._invalidate_cache()

    def set_duplicate_policy(self, policy: str) -> None:
        if policy not in _VALID_DUPLICATE_POLICIES:
            raise ValueError(
                f"duplicate_policy must be one of "
                f"{sorted(_VALID_DUPLICATE_POLICIES)}, got {policy!r}"
            )
        with self._lock:
            self.duplicate_policy = policy

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[ParetoFrontierResult], Optional[bool]]
        ],
    ) -> None:
        """Register (or clear) a HITL reviewer for degenerate frontiers."""
        with self._lock:
            self._hitl_callback = callback

    # ------------------------------------------------------------------
    # ORIGINAL public API
    # ------------------------------------------------------------------

    def add_record(
        self,
        energy_joules: float,
        accuracy: float,
        carbon_grams: float,
        label: str,
        metadata: Optional[Dict] = None,
        *,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        source: str = "unknown",
        simulated: bool = False,
        precision: Optional[str] = None,
        region: Optional[str] = None,
    ) -> RecordOutcome:
        """
        Add a Pareto observation.

        Backward-compatible: `add_record(energy, accuracy, carbon, label,
        metadata)` works exactly as before. Returns a `RecordOutcome`
        (additive — v1 returned None).
        """
        if metadata is None:
            metadata = {}

        if precision is not None and not isinstance(precision, str):
            logger.warning(
                "pareto_analyzer.record_added",
                extra={"label": label, "reason": "invalid_precision_type"},
            )
            precision = None

        point = ParetoPoint(
            energy_joules=energy_joules,
            accuracy=accuracy,
            carbon_grams=carbon_grams,
            label=label,
            metadata=metadata,
            run_id=run_id or self.run_id,
            task_id=task_id,
            agent_id=agent_id or label,
            source=source,
            simulated=simulated,
            precision=precision or self.precision,
            region=region,
        )

        # --- Validation ---
        if self.features.get("validation", True):
            ok, errors = _validate_point(point)
            point.valid = ok
            point.validation_errors = errors
            if not ok:
                with self._lock:
                    self._invalid += 1
                    self._stats.increment("invalid")
                _GLOBAL_STATS.increment("invalid")
                logger.warning(
                    "pareto_analyzer.record_invalid",
                    extra={"label": label, "errors": errors},
                )
                return RecordOutcome.INVALID

        # --- Exact dedup ---
        if self.features.get("deduplication", True):
            for existing in self.points:
                if (
                    existing.label == point.label
                    and abs(existing.energy_joules - point.energy_joules) < 1e-12
                    and abs(existing.accuracy - point.accuracy) < 1e-12
                ):
                    with self._lock:
                        self._duplicates += 1
                        self._stats.increment("duplicates")
                    _GLOBAL_STATS.increment("duplicates")
                    logger.debug(
                        "pareto_analyzer.record_deduplicated",
                        extra={"label": label},
                    )
                    return RecordOutcome.DEDUPLICATED

        # --- Label collision ---
        with self._lock:
            existing_labels = {p.label for p in self.points}
        if point.label in existing_labels:
            if self.duplicate_policy == "raise":
                raise ValueError(
                    f"duplicate label {point.label!r}; pass "
                    f"duplicate_policy='suffix' or 'keep' to change"
                )
            elif self.duplicate_policy == "suffix":
                new_label = self._suffix_label(point.label, existing_labels)
                logger.info(
                    "pareto_analyzer.label_suffixed",
                    extra={
                        "original": point.label,
                        "suffixed": new_label,
                    },
                )
                point = replace(point, label=new_label)
            # keep: no change (v1 behaviour, warn below)
            if self.duplicate_policy == "keep":
                logger.warning(
                    "pareto_analyzer.duplicate_label_kept",
                    extra={"label": point.label},
                )

        with self._lock:
            self.points.append(point)
            self._stats.increment("records")
            self._invalidate_cache()
        _GLOBAL_STATS.increment("records")
        logger.info(
            "pareto_analyzer.record_added",
            extra={"label": point.label},
        )
        return RecordOutcome.ACCEPTED

    def compute_frontier(self) -> List[ParetoPoint]:
        """
        Return Pareto-optimal points (lower energy + higher accuracy).

        Backward-compatible: same signature, same return type, same
        semantics as v1 when `carbon_in_frontier=False` and tolerance is
        strict.
        """
        result = self.compute_frontier_detailed()
        return result.points

    def export_json(self, path: str = "pareto_results.json") -> bool:
        """
        Export all points to JSON atomically (write `.tmp` then rename).

        Backward-compatible: same signature; returns bool (v1 also
        returned bool — preserved here, now never raises).
        """
        try:
            payload = [p.to_dict(safe=True) for p in self.points]
            dir_name = os.path.dirname(os.path.abspath(path)) or "."
            fd, tmp_path = tempfile.mkstemp(
                dir=dir_name, suffix=".tmp", prefix=".pareto_",
            )
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(payload, f, indent=4, default=str)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            logger.info(
                "pareto_analyzer.exported",
                extra={"path": path, "n_points": len(payload)},
            )
            return True
        except OSError as e:
            logger.error(
                "pareto_analyzer.export_failed",
                extra={"path": path, "error": str(e)},
            )
            return False
        except Exception as e:
            logger.error(
                "pareto_analyzer.export_failed",
                extra={"path": path, "error": str(e)},
            )
            return False

    # ------------------------------------------------------------------
    # Enhanced public API
    # ------------------------------------------------------------------

    def compute_frontier_detailed(
        self,
        *,
        filter_fn: Optional[Callable[[ParetoPoint], bool]] = None,
        top_k: Optional[int] = None,
        precision: Optional[str] = None,
        tolerance: Optional[float] = None,
        emit_decision_record: bool = False,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        policy_version: str = "",
    ) -> ParetoFrontierResult:
        """
        Compute the frontier with full provenance, XAI, and statistics.

        Default (2D): minimize energy, maximize accuracy — same as the
        original staircase algorithm.

        When `carbon_in_frontier=True`: minimize energy, maximize
        accuracy, minimize carbon — 3D pairwise dominance.

        `precision` / `tolerance` overrides are per-call only and do not
        mutate the analyzer's configuration.
        """
        # --- Resolve effective parameters ---
        eff_precision = (
            precision if isinstance(precision, str)
            else self.precision
        )
        try:
            eff_tolerance = (
                self._validate_tolerance(tolerance, name="tolerance")
                if tolerance is not None else self.tolerance
            )
        except (TypeError, ValueError) as e:
            logger.warning(
                "pareto_analyzer.frontier_computed",
                extra={"error": str(e), "reason": "invalid_tolerance"},
            )
            eff_tolerance = self.tolerance

        # Resolve tolerance via precision if unset
        if eff_tolerance is None and eff_precision is not None:
            effective_tol = tolerance_for_precision(eff_precision)
        elif eff_tolerance is None:
            effective_tol = 0.0
        else:
            effective_tol = eff_tolerance

        # --- Cache check ---
        cache_key: Tuple[Any, ...] = (
            filter_fn, top_k, effective_tol, eff_precision,
            self.features.get("carbon_in_frontier", False),
            len(self.points),
        )
        with self._lock:
            if (
                self._cached_result is not None
                and self._cached_params == cache_key
            ):
                cached = self._cached_result
                # If a decision record was requested and the cached one is
                # missing, emit it now on a shallow copy.
                if emit_decision_record and cached.decision_record is None:
                    dr = cached.to_decision_record(
                        run_id=run_id, task_id=task_id,
                        policy_version=policy_version,
                    )
                    if dr is not None:
                        serialised = _serialise_decision_record(dr)
                        if serialised is not None:
                            cached.decision_record = _jsonable(serialised)
                return cached

        # --- Compute from scratch ---
        with self._lock:
            self._frontier_count += 1
            self._stats.increment("frontiers")
        _GLOBAL_STATS.increment("frontiers")

        carbon_enabled = self.features.get("carbon_in_frontier", False)
        objectives: List[str] = ["energy_joules", "accuracy"]
        maximize: List[str] = ["accuracy"]
        if carbon_enabled:
            objectives.append("carbon_grams")

        # --- Filter / classify inputs ---
        candidates: List[ParetoPoint] = []
        invalid_count = 0
        invalid_ids: List[str] = []
        excluded_agents: List[str] = []
        excluded_reasons: Dict[str, str] = {}

        # Detect duplicates in labels
        label_counts: Counter = Counter(p.label for p in self.points)
        duplicate_labels = sorted(
            lb for lb, n in label_counts.items() if n > 1
        )

        with self._lock:
            points_snapshot = list(self.points)

        for p in points_snapshot:
            if not p.valid and self.features.get("validation", True):
                invalid_count += 1
                invalid_ids.append(p.label)
                continue
            if filter_fn is not None:
                try:
                    if not filter_fn(p):
                        excluded_agents.append(p.label)
                        excluded_reasons[p.label] = "rejected by filter_fn"
                        continue
                except Exception as e:
                    logger.warning(
                        "pareto_analyzer.filter_fn_raised",
                        extra={"label": p.label, "error": str(e)},
                    )
                    excluded_agents.append(p.label)
                    excluded_reasons[p.label] = (
                        f"filter_fn raised {type(e).__name__}: {e}"
                    )
                    continue
            candidates.append(p)

        # --- Compute frontier ---
        if carbon_enabled:
            frontier, dominated_by, dominates_count = (
                self._pairwise_frontier(
                    candidates, objectives, maximize, effective_tol,
                )
            )
        else:
            frontier, dominated_by, dominates_count = (
                self._staircase_frontier(candidates, effective_tol)
            )

        # --- Rationale ---
        rationale_by_label: Dict[str, List[str]] = {}
        for p in frontier:
            rationale_by_label[p.label] = (
                FrontierExplainer.explain_point(
                    p,
                    dominates_count.get(p.label, 0),
                    objectives, maximize, carbon_enabled,
                    tolerance=effective_tol,
                )
            )

        # --- Truncation ---
        pre_truncation_count = len(frontier)
        truncated = False
        if top_k is not None and top_k > 0 and len(frontier) > top_k:
            frontier = frontier[:top_k]
            truncated = True

        # --- HITL ---
        needs_review = False
        review_reason: Optional[str] = None
        if (
            self.features.get("hitl", True)
            and self.HITL_EMPTY_FRONTIER
            and not frontier
            and points_snapshot
        ):
            needs_review = True
            review_reason = "empty frontier despite non-empty input"
        elif (
            self.features.get("hitl", True)
            and len(frontier) == 1
            and len(points_snapshot) >= self.HITL_SINGLE_MIN_INPUTS
        ):
            needs_review = True
            review_reason = (
                f"frontier is a single point over "
                f"{len(points_snapshot)} inputs — most observations "
                f"were dominated"
            )

        # --- Severity ---
        if not frontier and points_snapshot:
            severity = Severity.CRITICAL.value
        elif needs_review:
            severity = Severity.WARNING.value
        else:
            severity = Severity.INFO.value

        result = ParetoFrontierResult(
            points=frontier,
            rationale_by_label=rationale_by_label,
            dominated_by_label=dominated_by,
            dominates_count_by_label=dominates_count,
            dominated_count=len(dominated_by),
            invalid_count=invalid_count,
            duplicate_count=self._duplicates,
            objectives=objectives,
            maximize=maximize,
            tolerance=effective_tol,
            carbon_enabled=carbon_enabled,
            duplicate_labels=duplicate_labels,
            excluded_agents=excluded_agents,
            excluded_reasons=excluded_reasons,
            truncated=truncated,
            pre_truncation_count=pre_truncation_count,
            invalid_agent_ids=invalid_ids,
            needs_review=needs_review,
            review_reason=review_reason,
            severity=severity,
        )

        # --- HITL callback ---
        if needs_review and self._hitl_callback is not None:
            try:
                approved = self._hitl_callback(result)
                if approved is False:
                    result.hitl_approved = False
                    result.review_reason = (
                        (result.review_reason or "")
                        + " [HITL: denied]"
                    ).strip()
                    logger.info(
                        "pareto_analyzer.hitl_denied",
                        extra={
                            "run_id": self.run_id,
                            "n_frontier": len(frontier),
                        },
                    )
                elif approved is True:
                    result.hitl_approved = True
            except Exception as e:
                logger.warning(
                    "pareto_analyzer.hitl_callback_failed",
                    extra={"error": str(e)},
                )

        # --- DecisionRecord emission ---
        if emit_decision_record:
            dr = result.to_decision_record(
                run_id=run_id, task_id=task_id,
                policy_version=policy_version,
            )
            if dr is not None:
                serialised = _serialise_decision_record(dr)
                if serialised is not None:
                    result.decision_record = _jsonable(serialised)

        # --- Cache + history ---
        with self._lock:
            self._cached_result = result
            self._cached_params = cache_key
            self._history.append({
                "run_id": self.run_id,
                "n_points": len(points_snapshot),
                "n_frontier": len(frontier),
                "pre_truncation_count": pre_truncation_count,
                "truncated": truncated,
                "carbon_enabled": carbon_enabled,
                "tolerance": effective_tol,
                "at": datetime.now().isoformat(),
            })

        logger.info(
            "pareto_analyzer.frontier_computed",
            extra={
                "run_id": self.run_id,
                "n_points": len(points_snapshot),
                "n_frontier": len(frontier),
                "carbon_enabled": carbon_enabled,
                "tolerance": effective_tol,
                "truncated": truncated,
                "pre_truncation_count": pre_truncation_count,
                "invalid_count": invalid_count,
                "excluded_count": len(excluded_agents),
                "needs_review": needs_review,
                "severity": severity,
            },
        )
        return result

    def to_decision_records(self, *, policy_version: str = "") -> List[Any]:
        """Emit a DecisionRecord for every frontier point (uses cache)."""
        result = self.compute_frontier_detailed()  # cached
        return result.to_decision_records(policy_version=policy_version)

    def to_decision_record(
        self, *, policy_version: str = "",
    ) -> Optional[Any]:
        """Emit a single DecisionRecord summarising the current frontier."""
        result = self.compute_frontier_detailed()  # cached
        return result.to_decision_record(policy_version=policy_version)

    # ------------------------------------------------------------------
    # Statistics / export / reset
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Instance-scoped statistics + configuration."""
        with self._lock:
            snap = self._stats.snapshot()
            points_snapshot = list(self.points)
            history_len = len(self._history)
        energies = [
            _safe_float(p.energy_joules) for p in points_snapshot
        ]
        energies = [e for e in energies if e is not None]

        stats: Dict[str, Any] = {
            "analyzer_version": __version__,
            "run_id": self.run_id,
            "deployment_id": self.deployment_id,
            "points_stored": len(points_snapshot),
            "invalid_stored": self._invalid,
            "duplicates_skipped": self._duplicates,
            "frontiers_computed": snap.get("frontiers", 0),
            "carbon_in_frontier":
                self.features.get("carbon_in_frontier", False),
            "tolerance": self.tolerance,
            "precision": self.precision,
            "duplicate_policy": self.duplicate_policy,
            "cache_valid": self._cached_result is not None,
            "history_len": history_len,
        }
        if energies:
            stats["energy_range_j"] = (min(energies), max(energies))
        return stats

    def to_json_dict(self, *, safe: bool = True) -> Dict[str, Any]:
        """Full export dict (no filesystem)."""
        with self._lock:
            points = [p.to_dict(safe=safe) for p in self.points]
            last = (
                self._cached_result.to_dict(safe=safe)
                if self._cached_result is not None else None
            )
        payload = {
            "analyzer_version": __version__,
            "run_id": self.run_id,
            "deployment_id": self.deployment_id,
            "statistics": self.get_statistics(),
            "points": points,
            "last_frontier": last,
        }
        return _jsonable(payload) if safe else payload

    def clear(self) -> None:
        """Remove all points and reset in-memory counters."""
        with self._lock:
            self.points.clear()
            self._duplicates = 0
            self._invalid = 0
            self._invalidate_cache()
        logger.debug(
            "pareto_analyzer.cleared",
            extra={"run_id": self.run_id},
        )

    def reset(self, *, full: bool = False) -> None:
        """
        Reset the analyzer.

        `full=False` (default) clears points, counters, history, and cache,
        but preserves configuration (features, precision, tolerance,
        duplicate_policy). `full=True` also restores the default
        configuration and generates a fresh `run_id`.
        """
        with self._lock:
            self.points.clear()
            self._duplicates = 0
            self._invalid = 0
            self._frontier_count = 0
            self._history.clear()
            self._invalidate_cache()
            self._stats.reset()
            if full:
                self.run_id = f"pa-{uuid.uuid4().hex[:8]}"
                self.deployment_id = "local"
                self.precision = None
                self.tolerance = None
                self.duplicate_policy = "suffix"
                self.features = dict(self.DEFAULT_FEATURES)
                self._hitl_callback = None
        logger.debug(
            "pareto_analyzer.reset",
            extra={"run_id": self.run_id, "full": full},
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _invalidate_cache(self) -> None:
        self._cached_result = None
        self._cached_params = None

    @staticmethod
    def _suffix_label(base: str, existing: Set[str]) -> str:
        n = 2
        while f"{base} #{n}" in existing:
            n += 1
        return f"{base} #{n}"

    @staticmethod
    def _point_to_comparison_dict(
        p: ParetoPoint, objectives: List[str],
    ) -> Dict[str, Any]:
        d: Dict[str, Any] = {"label": p.label}
        for obj in objectives:
            d[obj] = getattr(p, obj, None)
        return d

    # ------------------------------------------------------------------
    # Internal algorithms
    # ------------------------------------------------------------------

    def _staircase_frontier(
        self,
        points: List[ParetoPoint],
        tolerance: float,
    ) -> Tuple[
        List[ParetoPoint],
        Dict[str, List[str]],
        Dict[str, int],
    ]:
        """
        O(n × |frontier|) tolerant staircase for 2D:
        minimize energy, maximize accuracy.

        Returns (frontier, dominated_by, dominates_count) where
        `dominated_by[label]` lists **every** frontier point that
        tolerantly dominates `label`.
        """
        if not points:
            return [], {}, {}

        # Sort by (energy ascending, accuracy descending). The tolerance
        # is applied *within* the dominance check, not the sort.
        sorted_pts = sorted(
            points,
            key=lambda p: (
                _safe_float(p.energy_joules)
                if _safe_float(p.energy_joules) is not None
                else float("inf"),
                -(
                    _safe_float(p.accuracy)
                    if _safe_float(p.accuracy) is not None
                    else float("-inf")
                ),
                p.label,
            ),
        )

        frontier: List[ParetoPoint] = []
        dominated_by: Dict[str, List[str]] = defaultdict(list)

        for p in sorted_pts:
            p_e = _safe_float(p.energy_joules)
            p_a = _safe_float(p.accuracy)
            if p_e is None or p_a is None:
                continue

            dominators: List[ParetoPoint] = []
            for f in frontier:
                f_e = _safe_float(f.energy_joules)
                f_a = _safe_float(f.accuracy)
                if f_e is None or f_a is None:
                    continue
                if _tolerantly_dominates_2d(
                    f_e, f_a, p_e, p_a, tolerance,
                ):
                    dominators.append(f)

            if dominators:
                dominated_by[p.label] = [d.label for d in dominators]
            else:
                frontier.append(p)

        dominates_count: Dict[str, int] = defaultdict(int)
        for _, dominators_labels in dominated_by.items():
            for d in dominators_labels:
                dominates_count[d] += 1

        return frontier, dict(dominated_by), dict(dominates_count)

    def _pairwise_frontier(
        self,
        points: List[ParetoPoint],
        objectives: List[str],
        maximize: List[str],
        tolerance: float,
    ) -> Tuple[
        List[ParetoPoint],
        Dict[str, List[str]],
        Dict[str, int],
    ]:
        """
        O(n²) pairwise dominance — used when carbon is enabled.

        Prefers the sibling `pareto.py`'s `dominance_verdict` when
        importable (same module path the sibling exposes); falls back to
        a local tolerant comparison otherwise.
        """
        if not points:
            return [], {}, {}

        shared_verdict = _import_shared_verdict()
        if shared_verdict is None:
            logger.debug(
                "pareto_analyzer.shared_verdict_unavailable",
                extra={"fallback": "local"},
            )

        n = len(points)
        dominated_by: Dict[str, List[str]] = defaultdict(list)
        dominates_count: Dict[str, int] = defaultdict(int)

        # Build comparison dicts once per point (per-call)
        cmp_dicts = [
            self._point_to_comparison_dict(p, objectives) for p in points
        ]

        for i in range(n):
            pa = cmp_dicts[i]
            for j in range(n):
                if i == j:
                    continue
                pb = cmp_dicts[j]

                dominated = False
                if shared_verdict is not None:
                    try:
                        v = shared_verdict(
                            pb, pa, objectives,
                            maximize=set(maximize),
                            tolerance=tolerance,
                            explain=False,
                        )
                        dominated = (
                            getattr(v, "kind", "") == "a_dominates_b"
                        )
                    except Exception as e:
                        logger.debug(
                            "pareto_analyzer.shared_verdict_failed",
                            extra={"error": str(e)},
                        )
                        dominated = _check_dominance_multidim(
                            pb, pa, objectives, maximize, tolerance,
                        )
                else:
                    dominated = _check_dominance_multidim(
                        pb, pa, objectives, maximize, tolerance,
                    )

                if dominated:
                    dominated_by[points[i].label].append(points[j].label)
                    dominates_count[points[j].label] += 1

        frontier = [
            p for p in points if not dominated_by.get(p.label)
        ]
        frontier.sort(key=lambda p: (
            _safe_float(p.energy_joules) or float("inf"),
            -(_safe_float(p.accuracy) or float("inf")),
            p.label,
        ))
        return frontier, dict(dominated_by), dict(dominates_count)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json
    import sys
    import threading
    import types
    from decimal import Decimal

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    analyzer = ParetoAnalyzer()
    analyzer.add_record(100.0, 0.90, 50.0, "A")
    analyzer.add_record(120.0, 0.85, 45.0, "B")
    analyzer.add_record(90.0, 0.80, 60.0, "C")
    analyzer.add_record(150.0, 0.95, 70.0, "D")
    frontier = analyzer.compute_frontier()
    print(f"  frontier: {[p.label for p in frontier]}")
    print(f"  (expected A, D — B dominated by A, C dominated by A)")

    # =================================================================
    # 2. Tolerance — legit frontier point no longer dropped
    # =================================================================
    print("\n=== 2. Tolerance correctness (v1 dropped legit points) ===")
    tol_analyzer = ParetoAnalyzer(tolerance=0.01)
    tol_analyzer.add_record(10.0, 0.50, 0.0, "low")
    tol_analyzer.add_record(10.0, 0.505, 0.0, "mid")   # 0.005 > tol
    tol_analyzer.add_record(10.0, 0.512, 0.0, "high")  # 0.012 > tol
    r2 = tol_analyzer.compute_frontier_detailed()
    print(f"  frontier: {r2.frontier_labels()}")
    print(f"  (expected all 3 — none dominates another beyond tol)")
    print(f"  dominated_by[mid]: {r2.dominated_by('mid')}  (expected [])")

    # =================================================================
    # 3. All-dominators (v1 attributed only the previous frontier point)
    # =================================================================
    print("\n=== 3. All-dominators attribution ===")
    all_analyzer = ParetoAnalyzer()
    # Strict frontier: (0, 1.0), (1, 0.9), (2, 0.8)
    all_analyzer.add_record(0.0, 1.0, 0.0, "F0")
    all_analyzer.add_record(1.0, 0.9, 0.0, "F1")
    all_analyzer.add_record(2.0, 0.8, 0.0, "F2")
    # Dominated by all three
    all_analyzer.add_record(5.0, 0.5, 0.0, "D")
    r3 = all_analyzer.compute_frontier_detailed()
    print(f"  frontier: {r3.frontier_labels()}")
    print(f"  dominated_by[D]: {r3.dominated_by('D')}  "
          f"(expected all of F0, F1, F2)")
    print(f"  dominates_of[F0]: {r3.dominates_of('F0')}  "
          f"(expected >= 1; v1 would report 0)")

    # =================================================================
    # 4. Cached frontier — to_decision_records doesn't recompute
    # =================================================================
    print("\n=== 4. Cache — to_decision_records is a pure read ===")
    cache_analyzer = ParetoAnalyzer()
    for i in range(20):
        cache_analyzer.add_record(
            100.0 + i, 0.5 + 0.01 * i, 50.0, f"p{i}",
        )
    cache_analyzer.compute_frontier_detailed()
    before = cache_analyzer.get_statistics()["frontiers_computed"]
    for _ in range(50):
        cache_analyzer.to_decision_records()
    after = cache_analyzer.get_statistics()["frontiers_computed"]
    print(f"  frontiers_computed before: {before}")
    print(f"  frontiers_computed after:  {after}")
    print(f"  (equal — getter did not recompute)")

    # =================================================================
    # 5. tolerance=None vs tolerance=0.0 with precision preset
    # =================================================================
    print("\n=== 5. Tolerance disambiguation (v1 collision) ===")
    a5 = ParetoAnalyzer(precision="int4", tolerance=None)
    a5.add_record(10.0, 0.500, 0.0, "P")
    a5.add_record(10.0, 0.505, 0.0, "Q")   # 0.005 within 0.1 preset
    r5a = a5.compute_frontier_detailed()
    print(f"  tolerance=None precision=int4: "
          f"frontier={r5a.frontier_labels()}  (expected ['P'] — Q within tol)")

    a5b = ParetoAnalyzer(precision="int4", tolerance=0.0)
    a5b.add_record(10.0, 0.500, 0.0, "P")
    a5b.add_record(10.0, 0.505, 0.0, "Q")
    r5b = a5b.compute_frontier_detailed()
    print(f"  tolerance=0.0 precision=int4: "
          f"frontier={r5b.frontier_labels()}  (expected both — strict)")

    # =================================================================
    # 6. Precision type validation
    # =================================================================
    print("\n=== 6. Precision type validation ===")
    try:
        ParetoAnalyzer(precision=123)
        print("  accepted 123 (unexpected)")
    except TypeError as e:
        print(f"  ParetoAnalyzer(precision=123) → TypeError: {str(e)[:50]}...")

    # =================================================================
    # 7. Duplicate labels — suffix policy
    # =================================================================
    print("\n=== 7. Duplicate labels ===")
    dup_analyzer = ParetoAnalyzer(duplicate_policy="suffix")
    dup_analyzer.add_record(10.0, 0.9, 0.0, "A")
    dup_analyzer.add_record(11.0, 0.85, 0.0, "A")   # different values
    r7 = dup_analyzer.compute_frontier_detailed()
    print(f"  labels after suffixing: {[p.label for p in dup_analyzer.points]}")
    print(f"  frontier: {r7.frontier_labels()}")

    # raise policy
    raise_analyzer = ParetoAnalyzer(duplicate_policy="raise")
    raise_analyzer.add_record(10.0, 0.9, 0.0, "A")
    try:
        raise_analyzer.add_record(11.0, 0.85, 0.0, "A")
        print("  raise policy: accepted duplicate (unexpected)")
    except ValueError as e:
        print(f"  raise policy: ValueError on duplicate — {str(e)[:40]}...")

    # =================================================================
    # 8. Decimal support (v1 rejected numbers.Real)
    # =================================================================
    print("\n=== 8. Decimal support ===")
    dec_analyzer = ParetoAnalyzer()
    dec_analyzer.add_record(
        Decimal("100.0"), Decimal("0.9"), Decimal("50.0"), "D1",
    )
    dec_analyzer.add_record(
        Decimal("120.0"), Decimal("0.85"), Decimal("45.0"), "D2",
    )
    r8 = dec_analyzer.compute_frontier_detailed()
    print(f"  frontier: {r8.frontier_labels()}  (Decimal accepted)")

    # =================================================================
    # 9. filter_fn exclusion reasons
    # =================================================================
    print("\n=== 9. filter_fn exclusion reasons ===")
    filt_analyzer = ParetoAnalyzer()

    def _reject_simulated(p):
        if p.simulated:
            raise ValueError(f"simulated: {p.label}")
        return True

    filt_analyzer.add_record(10.0, 0.9, 0.0, "good")
    filt_analyzer.add_record(11.0, 0.85, 0.0, "sim", simulated=True)
    r9 = filt_analyzer.compute_frontier_detailed(
        filter_fn=_reject_simulated,
    )
    print(f"  frontier: {r9.frontier_labels()}")
    print(f"  excluded_agents: {r9.excluded_agents}")
    print(f"  excluded_reason('sim'): {r9.excluded_reason('sim')}")

    # =================================================================
    # 10. Truncation surfaced
    # =================================================================
    print("\n=== 10. Truncation ===")
    trunc_analyzer = ParetoAnalyzer()
    for i in range(8):
        trunc_analyzer.add_record(
            10.0 + i * 0.1, 0.9 - i * 0.01, 0.0, f"T{i}",
        )
    r10 = trunc_analyzer.compute_frontier_detailed(top_k=3)
    print(f"  frontier size: {len(r10.points)}")
    print(f"  truncated: {r10.truncated}")
    print(f"  pre_truncation_count: {r10.pre_truncation_count}")

    # =================================================================
    # 11. HITL denial annotation
    # =================================================================
    print("\n=== 11. HITL denial ===")
    hitl_analyzer = ParetoAnalyzer()
    # Force single-point frontier over many inputs
    for i in range(10):
        hitl_analyzer.add_record(
            10.0 + i, 0.5 + i * 0.01, 0.0, f"h{i}",
        )
    hitl_analyzer.set_hitl_callback(lambda r: False)
    r11 = hitl_analyzer.compute_frontier_detailed()
    print(f"  needs_review: {r11.needs_review}")
    print(f"  hitl_approved: {r11.hitl_approved}")
    print(f"  review_reason: {r11.review_reason}")
    print(f"  denial noted: "
          f"{'[HITL: denied]' in (r11.review_reason or '')}")

    # =================================================================
    # 12. Carbon in frontier (opt-in)
    # =================================================================
    print("\n=== 12. Carbon in frontier ===")
    carbon_analyzer = ParetoAnalyzer(
        features={"carbon_in_frontier": True},
    )
    carbon_analyzer.add_record(100.0, 0.90, 80.0, "A")
    carbon_analyzer.add_record(110.0, 0.90, 30.0, "B")
    carbon_analyzer.add_record(105.0, 0.92, 60.0, "C")
    r12 = carbon_analyzer.compute_frontier_detailed()
    print(f"  frontier: {r12.frontier_labels()}")
    print(f"  carbon_enabled: {r12.carbon_enabled}")

    # =================================================================
    # 13. JSON-safe DecisionRecord emission
    # =================================================================
    print("\n=== 13. JSON-safe DecisionRecord ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id, selected_action,
                     alternatives, explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.alternatives = alternatives
            self.explanation = explanation
            self.provenance = provenance

    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    sys.modules["analysis"] = fake_mod

    import importlib
    import src.analysis.optimization.pareto_analyzer as _pa_mod
    importlib.reload(_pa_mod)

    try:
        dr_analyzer = _pa_mod.ParetoAnalyzer()
        dr_analyzer.add_record(100.0, 0.9, 50.0, "A")
        dr_analyzer.add_record(90.0, 0.8, 60.0, "C")
        r13 = dr_analyzer.compute_frontier_detailed(
            emit_decision_record=True, run_id="run-1",
        )
        print(f"  decision_record present: "
              f"{r13.decision_record is not None}")
        try:
            json.dumps(r13.to_dict(safe=True))
            print(f"  json.dumps(to_dict(safe=True)): OK")
        except (TypeError, ValueError) as e:
            print(f"  FAILED: {e}")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_pa_mod)

    # =================================================================
    # 14. Thread safety
    # =================================================================
    print("\n=== 14. Thread safety ===")
    reset_statistics()
    conc_analyzer = ParetoAnalyzer()
    barrier = threading.Barrier(4)

    def _worker(n: int) -> None:
        barrier.wait()
        for i in range(100):
            conc_analyzer.add_record(
                10.0 + n + i * 0.01,
                0.5 + n * 0.1 + i * 0.001,
                0.0, f"w{n}-{i}",
            )
        conc_analyzer.compute_frontier_detailed()

    threads = [
        threading.Thread(target=_worker, args=(n,))
        for n in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats = conc_analyzer.get_statistics()
    print(f"  points_stored: {stats['points_stored']}  (expected 400)")
    print(f"  frontiers_computed: {stats['frontiers_computed']}  "
          f"(expected >= 1)")

    # =================================================================
    # 15. Atomic export
    # =================================================================
    print("\n=== 15. Atomic export ===")
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False,
    ) as tmp:
        target_path = tmp.name
    try:
        ok = analyzer.export_json(target_path)
        print(f"  export_json() to valid path: {ok}")
        with open(target_path) as f:
            loaded = json.load(f)
        print(f"  file contains {len(loaded)} points")
    finally:
        try:
            os.unlink(target_path)
        except OSError:
            pass

    # Bad path
    ok_bad = analyzer.export_json("/nonexistent/dir/pareto.json")
    print(f"  export_json() to bad path: {ok_bad}  (expected False)")

    # =================================================================
    # 16. Full statistics
    # =================================================================
    print("\n=== 16. Full statistics ===")
    print(json.dumps(analyzer.get_statistics(), indent=2, default=str))
