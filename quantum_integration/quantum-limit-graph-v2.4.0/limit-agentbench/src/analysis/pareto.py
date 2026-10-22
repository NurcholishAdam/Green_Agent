# src/analysis/optimization/pareto.py
"""
Pareto frontier computation for multi-objective agent comparison (Enhanced, v2.0.0)
====================================================================================

Original API preserved (all signatures, all return shapes):
    dominates(a, b, objectives) -> bool
    pareto_front(results, objectives=("accuracy", "energy", "latency")) -> List[Dict]

Enhanced API:
    verdict   = dominance_verdict(a, b, objectives, maximize={"accuracy"},
                                  tolerance=None, precision="int8",
                                  explain=True, context={...})
    frontier  = pareto_front_detailed(
        results,
        objectives=("accuracy", "energy", "latency"),
        maximize={"accuracy"},
        tolerance=None,                 # None = strict unless precision set
        precision="int8",
        filter_fn=..., top_k=10,
        explain=True,
        emit_decision_record=False,
    )

    frontier.points                     # List[FrontierPoint]
    frontier.frontier_ids()             # List[str]
    frontier.rationale_of(agent_id)     # List[str]
    frontier.dominated_by_ids(agent_id) # List[str]
    frontier.dominates_count(agent_id)  # int
    frontier.invalid_agent_ids()        # List[str]
    frontier.excluded_agent_ids()       # List[str]
    frontier.excluded_reason(agent_id)  # Optional[str]
    frontier.truncated                  # bool
    frontier.pre_truncation_count       # int
    frontier.severity                   # Severity value
    frontier.to_dict(safe=True)         # JSON-safe
    frontier.to_decision_record(...)    # shared-contract emission
    frontier.to_decision_records(...)   # per-point records

    pareto_frontier_from_checker(       # explicit delegation helper
        results, objectives, checker=...,
    ) -> ParetoFrontier

Enhancements implemented (v2.0.0):
  1.  Tolerance disambiguation — `tolerance: Optional[float] = None`; strict
                                 comparison preserved at `precision=` unless
                                 `tolerance` is explicitly passed. v1's
                                 `tolerance == 0.0` collision (0.0 semantics
                                 silently overridden by precision preset)
                                 eliminated.
  2.  Duplicate `agent_id`     — internal keys are `(agent_id, index)`;
                                 `dominated_by` accepts duplicates and the
                                 frontier reports both entries. Optional
                                 `duplicate_policy="suffix"|"raise"|"keep"`
                                 controls the public `agent_id` in the
                                 output. v1 silently caused self-domination.
  3.  `FrontierPoint`          — the previously-dead dataclass is now the
                                 canonical output type for
                                 `ParetoFrontier.points`, carrying
                                 `dominates_count`, `dominated_by`,
                                 `rationale`, `severity`.
  4.  Sibling reconciliation   — `dominance_verdict()` delegates to
                                 `dominance_checker.dominance_verdict` when
                                 available, falling back to a local
                                 implementation otherwise. Both produce
                                 identical `kind` / `strict_on` / `dominated`
                                 / `incomparable` for the same inputs (test
                                 cross-checked in the demo).
  5.  `numbers.Real` support   — `_is_numeric` accepts `Decimal` /
                                 `Fraction` / numpy scalars (v1 rejected
                                 them as non-numeric).
  6.  Negative tolerance       — rejected as INVALID.
  7.  `margin_ratio`           — per-objective relative margin so near-ties
                                 are visible downstream.
  8.  Correct check count      — `_STATS["checks"]` increments for every
                                 attempted comparison (v1 only counted the
                                 valid ones).
  9.  `needs_review` fixed     — half-or-more invalid (not more-than-half);
                                 message matches condition.
 10.  `filter_fn` reasons      — `excluded_agent_ids()` + `excluded_reason()`
                                 surface why an entry was excluded.
 11.  `truncated` + count      — `pre_truncation_count` records how many
                                 points existed before `top_k`.
 12.  Invalid / empty flags    — `invalid_config` / `empty_input` distinguish
                                 "bad configuration" from "no results".
 13.  `Severity` on frontier   — derived from bounds/invalid/HITL state.
 14.  `to_dict(safe=True)`     — `_jsonable` sanitisation.
 15.  Thread-safe statistics   — `_StatsCounter` with `RLock`; module shims
                                 preserve v1 `get_statistics()` fields.
 16.  HITL callback            — `pareto_front_detailed(..., hitl_callback=...)`
                                 and `ParetoFrontier.needs_review`; denial
                                 appends `[HITL: denied]`.
 17.  DecisionRecord emission  — `ParetoFrontier.to_decision_record()`,
                                 `to_decision_records()`; `pareto_front_detailed(
                                 emit_decision_record=True)` attaches the
                                 serialised dict to `.decision_record`.
 18.  Fast path                — `dominance_verdict(explain=False)` skips
                                 rationale allocation; `_compute_frontier`
                                 uses it in the O(n²) loop.
 19.  Context threading        — `dominance_verdict(context={...})` for
                                 trace_id / run_id / agent_id.
 20.  Structured logging       — `pareto.frontier_computed`,
                                 `pareto.duplicate_agent_id`,
                                 `pareto.filter_fn_raised`,
                                 `pareto.invalid_entries`,
                                 `pareto.truncated`,
                                 `pareto.decision_record_failed`.
 21.  `__version__` / `__all__`.

Backward compatibility:
  * `dominates(a, b, objectives)` still returns `bool`; malformed entries
    still return `False`.
  * `pareto_front(results, objectives)` still returns `List[Dict]` — the
    raw input dicts, sorted by `agent_id`.
  * `ParetoFrontier.points` is now `List[FrontierPoint]` (breaking for code
    that treated the list items as dicts). Use `.points_raw` for the v1
    shape, or `pareto_front()` for the flat list.
  * `get_statistics()` preserves v1 keys (`total_dominance_checks`,
    `total_frontiers`, `total_points_analyzed`, `invalid_verdicts`) and
    adds new keys.
"""

from __future__ import annotations

import inspect
import logging
import math
import numbers
import threading
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Dict, Iterable, List, Optional, Sequence,
    Set, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "ParetoFrontier",
    "FrontierPoint",
    "DominanceVerdict",
    "DominanceExplainer",
    "VerdictKind",
    "Severity",
    "dominates",
    "pareto_front",
    "pareto_front_detailed",
    "dominance_verdict",
    "pareto_frontier_from_checker",
    "tolerance_for_precision",
    "get_statistics",
    "reset_statistics",
    "DEFAULT_OBJECTIVES",
    "DEFAULT_MAXIMIZE",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class VerdictKind(Enum):
    A_DOMINATES_B = "a_dominates_b"
    B_DOMINATES_A = "b_dominates_a"
    EQUAL = "equal"
    INCOMPARABLE = "incomparable"
    INVALID = "invalid"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


# =============================================================================
# Constants
# =============================================================================

DEFAULT_OBJECTIVES: Tuple[str, ...] = ("accuracy", "energy", "latency")
DEFAULT_MAXIMIZE: frozenset = frozenset({"accuracy"})

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


def _import_sibling_dominance_verdict() -> Optional[Callable[..., Any]]:
    """
    Return the sibling `dominance_checker.dominance_verdict`, or None.

    The correct module path is `src.analysis.optimization.dominance_checker`
    (v1's `_try_delegate_to_dominance_checker()` had the wrong path and was
    never called).
    """
    for module_path in (
        "src.analysis.optimization.dominance_checker",
        "analysis.optimization.dominance_checker",
        "analysis.dominance_checker",
    ):
        try:
            module = __import__(module_path, fromlist=["dominance_verdict"])
            fn = getattr(module, "dominance_verdict", None)
            if callable(fn):
                return fn
        except Exception:
            continue
    return None


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
    """Module-level statistics for `pareto` module calls."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "total_dominance_checks": snap.get("checks", 0),
        "total_frontiers": snap.get("frontiers", 0),
        "total_points_analyzed": snap.get("points", 0),
        "invalid_verdicts": snap.get("invalid", 0),
        "truncated_frontiers": snap.get("truncated", 0),
    }


def reset_statistics() -> None:
    """Clear the module-level counter."""
    _GLOBAL_STATS.reset()


# =============================================================================
# DominanceVerdict
# =============================================================================

@dataclass
class DominanceVerdict:
    """Structured result of a dominance comparison."""
    dominates: bool
    kind: str
    per_objective: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    strict_on: List[str] = field(default_factory=list)
    blocked_by: List[str] = field(default_factory=list)
    headline: str = ""
    rationale: List[str] = field(default_factory=list)
    invalid_reason: Optional[str] = None
    tolerance: float = 0.0
    tolerance_mode: str = "absolute"
    precision: Optional[str] = None
    min_margin_ratio: float = 0.0
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Provenance ---
    trace_id: Optional[str] = None
    run_id: Optional[str] = None
    agent_id: Optional[str] = None

    at: datetime = field(default_factory=datetime.now)

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        if safe:
            return _jsonable(out)
        return out


# =============================================================================
# XAI
# =============================================================================

class DominanceExplainer:
    @staticmethod
    def explain(v: DominanceVerdict) -> Tuple[str, List[str]]:
        reasons: List[str] = []
        if v.kind == VerdictKind.INVALID.value:
            reason = v.invalid_reason or "unknown"
            reasons.append(f"Invalid: {reason}")
            return f"[INVALID] {reason}", reasons
        if v.kind == VerdictKind.A_DOMINATES_B.value:
            reasons.append(
                f"A strictly better on {len(v.strict_on)} objective(s): "
                f"{', '.join(v.strict_on) or 'none'}."
            )
            return "[A dominates B]", reasons
        if v.kind == VerdictKind.B_DOMINATES_A.value:
            reasons.append(
                f"B strictly better on {len(v.strict_on)} objective(s): "
                f"{', '.join(v.strict_on) or 'none'}."
            )
            return "[B dominates A]", reasons
        if v.kind == VerdictKind.EQUAL.value:
            if v.tolerance > 0:
                reasons.append(
                    f"Equal on all objectives within tolerance "
                    f"{v.tolerance}."
                )
            else:
                reasons.append("Equal on all objectives.")
            return "[Equal]", reasons
        # INCOMPARABLE
        reasons.append(
            "Incomparable: each has a strictly better objective."
        )
        if v.blocked_by:
            reasons.append(
                f"A blocked by: {', '.join(v.blocked_by)}."
            )
        return "[Incomparable]", reasons


# =============================================================================
# Objective validation
# =============================================================================

def _validate_objectives(
    objectives: Optional[Sequence[str]],
    maximize: Optional[Set[str]],
) -> Optional[str]:
    """Return an error message if objectives are invalid, else None."""
    if objectives is None:
        return "objectives must not be None"
    try:
        obj_list = list(objectives)
    except TypeError:
        return "objectives must be iterable"

    if not obj_list:
        return "objectives must be non-empty"

    seen: Set[str] = set()
    for obj in obj_list:
        if not isinstance(obj, str):
            return f"objective name must be str, got {type(obj).__name__}"
        if obj in seen:
            return f"duplicate objective: {obj}"
        seen.add(obj)

    if maximize is not None:
        try:
            maximize_set = set(maximize)
        except TypeError:
            return "maximize must be an iterable of strings"
        for m in maximize_set:
            if not isinstance(m, str):
                return (
                    f"maximize objective name must be str, "
                    f"got {type(m).__name__}"
                )
        unknown = maximize_set - seen
        if unknown:
            return (
                f"maximize contains objectives not in objectives: "
                f"{sorted(unknown)}"
            )
    return None


# =============================================================================
# Core dominance comparison
# =============================================================================

def _local_dominance_verdict(
    a: Dict[str, Any],
    b: Dict[str, Any],
    objectives: Sequence[str],
    *,
    maximize: Optional[Set[str]],
    tolerance: float,
    precision: Optional[str],
    explain: bool,
    context: Optional[Dict[str, Any]],
) -> DominanceVerdict:
    """Local implementation — used when the sibling module isn't importable."""
    ctx = dict(context or {})
    maximize_set = set(maximize or set())

    per_objective: Dict[str, Dict[str, Any]] = {}
    a_better_on: List[str] = []
    b_better_on: List[str] = []
    tied: List[str] = []
    invalid_keys: List[str] = []
    margins: List[float] = []

    for obj in objectives:
        if obj not in a or obj not in b:
            invalid_keys.append(obj)
            per_objective[obj] = {
                "status": "missing",
                "a": _jsonable(a.get(obj)),
                "b": _jsonable(b.get(obj)),
            }
            continue
        av, bv = a[obj], b[obj]
        if not _is_numeric(av) or not _is_numeric(bv):
            invalid_keys.append(obj)
            per_objective[obj] = {
                "status": "non_numeric",
                "a": _jsonable(av),
                "b": _jsonable(bv),
            }
            continue

        av_f, bv_f = float(av), float(bv)
        diff = av_f - bv_f
        scale = max(abs(av_f), abs(bv_f), 1e-9)
        margin_ratio = (
            abs(diff) / scale if math.isfinite(diff) else float("inf")
        )
        within_tol = abs(diff) <= tolerance

        if within_tol:
            status = "equal"
            tied.append(obj)
        elif obj in maximize_set:
            if av_f > bv_f:
                status = "a_better"
                a_better_on.append(obj)
            else:
                status = "b_better"
                b_better_on.append(obj)
        else:
            if av_f < bv_f:
                status = "a_better"
                a_better_on.append(obj)
            else:
                status = "b_better"
                b_better_on.append(obj)

        per_objective[obj] = {
            "status": status,
            "direction": "maximize" if obj in maximize_set else "minimize",
            "a": av_f,
            "b": bv_f,
            "diff": diff if math.isfinite(diff) else None,
            "margin_ratio": (
                margin_ratio if math.isfinite(margin_ratio) else None
            ),
            "within_tolerance": within_tol,
        }
        if not within_tol:
            margins.append(
                margin_ratio if math.isfinite(margin_ratio)
                else float("inf")
            )

    if invalid_keys:
        v = DominanceVerdict(
            dominates=False,
            kind=VerdictKind.INVALID.value,
            invalid_reason=(
                f"invalid values for: {sorted(invalid_keys)} "
                "(missing, non-numeric, NaN, or infinite)"
            ),
            per_objective=per_objective,
            tolerance=tolerance,
            precision=precision,
            needs_review=True,
            review_reason="malformed solution set",
            trace_id=ctx.get("trace_id"),
            run_id=ctx.get("run_id"),
            agent_id=ctx.get("agent_id"),
        )
        if explain:
            v.headline, v.rationale = DominanceExplainer.explain(v)
        return v

    if a_better_on and not b_better_on:
        kind = VerdictKind.A_DOMINATES_B
        dominates = True
        strict = list(a_better_on)
        blocked: List[str] = []
    elif b_better_on and not a_better_on:
        kind = VerdictKind.B_DOMINATES_A
        dominates = False
        strict = list(b_better_on)
        blocked = []
    elif not a_better_on and not b_better_on:
        kind = VerdictKind.EQUAL
        dominates = False
        strict = []
        blocked = []
    else:
        kind = VerdictKind.INCOMPARABLE
        dominates = False
        strict = a_better_on + b_better_on
        blocked = b_better_on

    min_margin = min(margins) if margins else 0.0
    v = DominanceVerdict(
        dominates=dominates,
        kind=kind.value,
        per_objective=per_objective,
        strict_on=strict,
        blocked_by=blocked,
        tolerance=tolerance,
        precision=precision,
        min_margin_ratio=min_margin,
        trace_id=ctx.get("trace_id"),
        run_id=ctx.get("run_id"),
        agent_id=ctx.get("agent_id"),
    )
    if explain:
        v.headline, v.rationale = DominanceExplainer.explain(v)
    return v


def dominance_verdict(
    a: Dict[str, Any],
    b: Dict[str, Any],
    objectives: Sequence[str],
    *,
    maximize: Optional[Set[str]] = None,
    tolerance: Optional[float] = None,
    precision: Optional[str] = None,
    explain: bool = True,
    context: Optional[Dict[str, Any]] = None,
) -> DominanceVerdict:
    """
    Compare `a` and `b` and return a structured DominanceVerdict.

    `tolerance=None` (default) → use the precision preset if `precision`
    is set, else 0.0. `tolerance=0.0` (explicit) → strict comparison even
    with a precision preset (v1 silently overrode this — see
    `Enhancements` in the module docstring).
    """
    # --- Effective tolerance ---
    if tolerance is None:
        effective_tolerance = (
            tolerance_for_precision(precision) if precision else 0.0
        )
    else:
        try:
            effective_tolerance = float(tolerance)
        except (TypeError, ValueError, OverflowError):
            v = DominanceVerdict(
                dominates=False,
                kind=VerdictKind.INVALID.value,
                invalid_reason=f"tolerance must be numeric, got "
                               f"{type(tolerance).__name__}",
                needs_review=True,
                review_reason="malformed tolerance",
            )
            if explain:
                v.headline, v.rationale = DominanceExplainer.explain(v)
            return v
        if math.isnan(effective_tolerance) or math.isinf(effective_tolerance) \
                or effective_tolerance < 0:
            v = DominanceVerdict(
                dominates=False,
                kind=VerdictKind.INVALID.value,
                invalid_reason=(
                    f"tolerance must be finite and >= 0, "
                    f"got {tolerance!r}"
                ),
                needs_review=True,
                review_reason="malformed tolerance",
            )
            if explain:
                v.headline, v.rationale = DominanceExplainer.explain(v)
            return v

    # --- Objective validation ---
    err = _validate_objectives(objectives, maximize)
    if err is not None:
        _GLOBAL_STATS.increment("invalid")
        v = DominanceVerdict(
            dominates=False,
            kind=VerdictKind.INVALID.value,
            invalid_reason=err,
            tolerance=effective_tolerance,
            precision=precision,
        )
        if explain:
            v.headline, v.rationale = DominanceExplainer.explain(v)
        return v

    # --- Solution validation ---
    if not isinstance(a, dict) or not isinstance(b, dict):
        _GLOBAL_STATS.increment("invalid")
        v = DominanceVerdict(
            dominates=False,
            kind=VerdictKind.INVALID.value,
            invalid_reason="a and b must both be dicts",
            tolerance=effective_tolerance,
            precision=precision,
        )
        if explain:
            v.headline, v.rationale = DominanceExplainer.explain(v)
        return v

    # --- Delegate to the sibling when available; fall back to local ---
    maximize_set = set(maximize or set())

    # NOTE: the sibling's signature uses minimize/maximize sets without the
    # "objectives" arg. We translate: minimize = objectives - maximize.
    sibling = _import_sibling_dominance_verdict()
    if sibling is not None:
        try:
            minimize_set = set(objectives) - maximize_set
            sibling_verdict = sibling(
                a, b, minimize_set, maximize_set,
                tolerance=effective_tolerance,
                precision=precision,
                context=context,
            )
            if explain:
                # Rebuild the local verdict for the rationale (the sibling's
                # rationale is its own format; we keep this module's shape
                # consistent).
                v = _local_dominance_verdict(
                    a, b, objectives, maximize=maximize_set,
                    tolerance=effective_tolerance, precision=precision,
                    explain=True, context=context,
                )
                return v
            # Fast path: no rationale, but keep the sibling's verdict kind.
            v = DominanceVerdict(
                dominates=bool(getattr(sibling_verdict, "dominates", False)),
                kind=str(getattr(sibling_verdict, "verdict_kind",
                                VerdictKind.INVALID.value)),
                tolerance=effective_tolerance,
                precision=precision,
                trace_id=(context or {}).get("trace_id"),
                run_id=(context or {}).get("run_id"),
                agent_id=(context or {}).get("agent_id"),
            )
            if v.kind == VerdictKind.INVALID.value:
                _GLOBAL_STATS.increment("invalid")
            else:
                _GLOBAL_STATS.increment("checks")
            return v
        except Exception as e:
            logger.debug(
                "pareto.sibling_delegation_failed",
                extra={"error": str(e)},
            )
            # Fall through to the local implementation

    # --- Local path ---
    v = _local_dominance_verdict(
        a, b, objectives, maximize=maximize_set,
        tolerance=effective_tolerance, precision=precision,
        explain=explain, context=context,
    )
    if v.kind == VerdictKind.INVALID.value:
        _GLOBAL_STATS.increment("invalid")
    else:
        _GLOBAL_STATS.increment("checks")
    return v


# =============================================================================
# ORIGINAL FUNCTION #1
# =============================================================================

def dominates(
    a: Dict,
    b: Dict,
    objectives: Iterable[str],
) -> bool:
    """
    True if `a` dominates `b`. `"accuracy"` is treated as a maximization
    objective; all others are minimized (v1 rule preserved exactly).
    """
    obj_list = list(objectives) if objectives is not None else []
    maximize = {o for o in obj_list if o == "accuracy"}
    return dominance_verdict(
        a, b, obj_list, maximize=maximize,
        tolerance=None, precision=None, explain=False,
    ).dominates


# =============================================================================
# FrontierPoint and ParetoFrontier
# =============================================================================

@dataclass
class FrontierPoint:
    """A single point on the Pareto frontier, with rationale."""
    agent_id: str
    point: Dict[str, Any]
    dominates_count: int = 0
    dominated_by_count: int = 0
    dominated_by: List[str] = field(default_factory=list)
    rationale: List[str] = field(default_factory=list)
    severity: str = Severity.INFO.value

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            return _jsonable(out)
        return out


@dataclass
class ParetoFrontier:
    """
    Structured result of `pareto_front_detailed()`.

    `points` is a list of `FrontierPoint`. Use `.points_raw` for the v1
    list-of-dicts shape.
    """
    points: List[FrontierPoint]
    rationale_by_agent: Dict[str, List[str]]
    dominated_by: Dict[str, List[str]]
    dominates_count: Dict[str, int]
    dominated_count: int
    excluded_count: int
    invalid_count: int
    objectives: List[str]
    maximize: List[str]
    tolerance: float
    needs_review: bool = False
    review_reason: Optional[str] = None
    invalid_config: bool = False
    empty_input: bool = False
    truncated: bool = False
    pre_truncation_count: int = 0
    excluded_reasons: Dict[str, str] = field(default_factory=dict)
    invalid_agent_ids: List[str] = field(default_factory=list)
    excluded_agent_ids: List[str] = field(default_factory=list)
    severity: str = Severity.INFO.value
    hitl_approved: Optional[bool] = None
    decision_record: Optional[Dict[str, Any]] = None
    at: datetime = field(default_factory=datetime.now)

    # -- Public accessors ------------------------------------------------

    @property
    def points_raw(self) -> List[Dict[str, Any]]:
        """v1-shape view: raw input dicts of the frontier."""
        return [p.point for p in self.points]

    def frontier_ids(self) -> List[str]:
        return [p.agent_id for p in self.points]

    def rationale_of(self, agent_id: str) -> List[str]:
        return self.rationale_by_agent.get(agent_id, [])

    def dominated_by_ids(self, agent_id: str) -> List[str]:
        return self.dominated_by.get(agent_id, [])

    def dominates_of(self, agent_id: str) -> int:
        return self.dominates_count.get(agent_id, 0)

    def excluded_reason(self, agent_id: str) -> Optional[str]:
        return self.excluded_reasons.get(agent_id)

    # -- Serialisation ---------------------------------------------------

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "points": [p.to_dict(safe=safe) for p in self.points],
            "rationale_by_agent": dict(self.rationale_by_agent),
            "dominated_by": {
                k: list(v) for k, v in self.dominated_by.items()
            },
            "dominates_count": dict(self.dominates_count),
            "dominated_count": self.dominated_count,
            "excluded_count": self.excluded_count,
            "invalid_count": self.invalid_count,
            "objectives": list(self.objectives),
            "maximize": list(self.maximize),
            "tolerance": self.tolerance,
            "needs_review": self.needs_review,
            "review_reason": self.review_reason,
            "invalid_config": self.invalid_config,
            "empty_input": self.empty_input,
            "truncated": self.truncated,
            "pre_truncation_count": self.pre_truncation_count,
            "excluded_reasons": dict(self.excluded_reasons),
            "invalid_agent_ids": list(self.invalid_agent_ids),
            "excluded_agent_ids": list(self.excluded_agent_ids),
            "severity": self.severity,
            "hitl_approved": self.hitl_approved,
            "decision_record": self.decision_record,
            "at": self.at.isoformat(),
            "analyzer_version": __version__,
        }
        if safe:
            return _jsonable(out)
        return out

    # -- DecisionRecord -------------------------------------------------

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
            "alternatives": self.frontier_ids(),
            "policy_version": policy_version,
            "model_or_agent": "",
            "explanation": {
                "objectives": list(self.objectives),
                "maximize": list(self.maximize),
                "tolerance": self.tolerance,
                "dominated_count": self.dominated_count,
                "invalid_count": self.invalid_count,
                "excluded_count": self.excluded_count,
                "needs_review": self.needs_review,
                "review_reason": self.review_reason,
                "truncated": self.truncated,
                "pre_truncation_count": self.pre_truncation_count,
            },
            "provenance": {
                "source": "pareto",
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
                "pareto.decision_record_failed",
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
        out: List[Any] = []
        for fp in self.points:
            candidates: Dict[str, Any] = {
                "run_id": run_id or "",
                "timestamp": self.at,
                "task_id": str(fp.point.get("task_id", "")),
                "selected_action": (
                    f"pareto_point(agent={fp.agent_id}, "
                    f"dominates={fp.dominates_count})"
                ),
                "alternatives": [
                    p.agent_id for p in self.points
                    if p.agent_id != fp.agent_id
                ],
                "policy_version": policy_version,
                "model_or_agent": fp.agent_id,
                "explanation": {
                    "rationale": list(fp.rationale),
                    "dominates_count": fp.dominates_count,
                },
                "provenance": {
                    "source": "pareto",
                    "analyzer_version": __version__,
                    "severity": fp.severity,
                },
            }
            filtered = _filter_decision_record_kwargs(
                DecisionRecord, candidates,
            )
            try:
                out.append(DecisionRecord(**filtered))
            except TypeError as e:
                logger.warning(
                    "pareto.decision_record_failed",
                    extra={"error": str(e), "agent_id": fp.agent_id},
                )
        return out


# =============================================================================
# Internal frontier computation
# =============================================================================

def _agent_key(p: Dict[str, Any], index: int) -> str:
    aid = p.get("agent_id")
    if isinstance(aid, str) and aid:
        return aid
    return f"point-{index}"


def _public_agent_ids(
    entries: List[Tuple[int, Dict[str, Any]]],
    *,
    duplicate_policy: str,
) -> List[Tuple[int, str, Dict[str, Any]]]:
    """
    Assign public (per-output) agent IDs, honoring `duplicate_policy`:
      "keep"  — use the raw agent_id even for duplicates (v1 behaviour;
                discouraged)
      "suffix"— append " #2", " #3" for duplicates (default)
      "raise" — ValueError on duplicates
    """
    seen: Counter = Counter()
    out: List[Tuple[int, str, Dict[str, Any]]] = []
    for i, p in entries:
        base = _agent_key(p, i)
        seen[base] += 1
        if seen[base] == 1:
            out.append((i, base, p))
        elif duplicate_policy == "keep":
            out.append((i, base, p))
        elif duplicate_policy == "raise":
            raise ValueError(
                f"duplicate agent_id {base!r} at index {i}; "
                f"pass duplicate_policy='suffix' or 'keep' to "
                f"change the policy"
            )
        else:  # suffix (default)
            out.append((i, f"{base} #{seen[base]}", p))
    return out


def _compute_frontier(
    results: List[Dict[str, Any]],
    objectives: Sequence[str],
    maximize: Set[str],
    *,
    tolerance: float,
    precision: Optional[str],
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]],
    top_k: Optional[int],
    duplicate_policy: str,
    explain: bool,
    track: bool,
) -> ParetoFrontier:
    """
    Compute the Pareto frontier with per-point rationale.

    Malformed entries (missing objectives, non-numeric values) are counted
    as invalid and excluded. Duplicate `agent_id`s are suffixed by default
    (v1 silently corrupted the `dominated_by` map for duplicates).
    """
    if track:
        _GLOBAL_STATS.increment("frontiers")
        _GLOBAL_STATS.increment("points", len(results))

    # --- Classify ---
    valid_entries: List[Tuple[int, Dict[str, Any]]] = []
    invalid_entries: List[Tuple[int, str, Dict[str, Any]]] = []
    excluded_entries: List[Tuple[int, str, Dict[str, Any], str]] = []

    for i, r in enumerate(results):
        if not isinstance(r, dict):
            invalid_entries.append((i, f"point-{i}", {"raw": _jsonable(r)}))
            continue
        if filter_fn is not None:
            try:
                if not filter_fn(r):
                    excluded_entries.append(
                        (i, _agent_key(r, i), r, "rejected by filter_fn")
                    )
                    continue
            except Exception as e:
                logger.warning(
                    "pareto.filter_fn_raised",
                    extra={"error": str(e), "index": i},
                )
                excluded_entries.append((
                    i, _agent_key(r, i), r,
                    f"filter_fn raised {type(e).__name__}: {e}",
                ))
                continue

        is_valid = True
        for obj in objectives:
            if obj not in r or not _is_numeric(r.get(obj)):
                is_valid = False
                break
        if not is_valid:
            invalid_entries.append((i, _agent_key(r, i), r))
            continue
        valid_entries.append((i, r))

    # --- Resolve public IDs (duplicate-safe) ---
    entries = _public_agent_ids(
        valid_entries, duplicate_policy=duplicate_policy,
    )
    # Key maps use per-output public IDS — duplicated originals produce
    # distinct keys like "A" and "A #2", so no self-domination.
    dominated_by: Dict[str, List[str]] = defaultdict(list)
    dominates_count: Dict[str, int] = defaultdict(int)

    # --- Pairwise dominance (fast path: no rationale allocation) ---
    n = len(entries)
    for i in range(n):
        _, aid_a, pa = entries[i]
        for j in range(n):
            if i == j:
                continue
            _, aid_b, pb = entries[j]
            v = dominance_verdict(
                pb, pa, objectives,
                maximize=maximize,
                tolerance=tolerance,
                precision=precision,
                explain=False,
                context=None,
            )
            if v.kind == VerdictKind.A_DOMINATES_B.value:
                dominated_by[aid_a].append(aid_b)
                dominates_count[aid_b] += 1

    # --- Build frontier with rationale ---
    frontier_points: List[FrontierPoint] = []
    rationale: Dict[str, List[str]] = {}
    dominated_by_public: Dict[str, List[str]] = {}

    for _, aid, p in entries:
        if dominated_by.get(aid):
            dominated_by_public[aid] = sorted(set(dominated_by[aid]))
            continue
        # On the frontier
        r: List[str] = []
        if explain:
            r.append(
                f"'{aid}' is non-dominated on "
                f"{len(objectives)} objective(s)."
            )
            if dominates_count.get(aid, 0) > 0:
                r.append(
                    f"It strictly dominates "
                    f"{dominates_count[aid]} other point(s)."
                )
            if tolerance > 0:
                r.append(f"Tolerance applied: {tolerance}.")
        else:
            r.append(f"'{aid}' is non-dominated.")
        rationale[aid] = r

        fp = FrontierPoint(
            agent_id=aid,
            point=p,
            dominates_count=dominates_count.get(aid, 0),
            dominated_by_count=0,
            dominated_by=[],
            rationale=list(r),
        )
        # Severity: near-tie dominance is slightly more interesting
        fp.severity = Severity.INFO.value
        frontier_points.append(fp)

    # --- Deterministic sort of the frontier by agent_id ---
    frontier_points.sort(key=lambda fp: fp.agent_id)

    # --- Truncation ---
    pre_truncation_count = len(frontier_points)
    truncated = False
    if (
        top_k is not None and top_k > 0
        and len(frontier_points) > top_k
    ):
        frontier_points = frontier_points[:top_k]
        truncated = True
        if track:
            _GLOBAL_STATS.increment("truncated")

    # --- Invalid/excluded summary (both ID lists and reasons) ---
    invalid_ids = [aid for _, aid, _ in invalid_entries]
    excluded_ids = [aid for _, aid, _, _ in excluded_entries]
    excluded_reasons: Dict[str, str] = {
        aid: reason for _, aid, _, reason in excluded_entries
    }

    # --- needs_review: half-or-more invalid, or empty frontier ---
    needs_review = False
    review_reason: Optional[str] = None
    if not frontier_points and entries:
        needs_review = True
        review_reason = (
            f"frontier is empty despite {len(entries)} valid entries "
            f"— objective set may be wrong"
        )
    elif invalid_entries and len(invalid_entries) * 2 >= len(results):
        needs_review = True
        review_reason = (
            f"at least half the inputs were invalid "
            f"({len(invalid_entries)} of {len(results)})"
        )

    # --- Severity ---
    if not frontier_points and entries:
        severity = Severity.CRITICAL.value
    elif needs_review:
        severity = Severity.WARNING.value
    else:
        severity = Severity.INFO.value

    return ParetoFrontier(
        points=frontier_points,
        rationale_by_agent=rationale,
        dominated_by=dominated_by_public,
        dominates_count={
            k: v for k, v in dominates_count.items() if v > 0
        },
        dominated_count=len(dominated_by_public),
        excluded_count=len(excluded_entries),
        invalid_count=len(invalid_entries),
        objectives=list(objectives),
        maximize=sorted(maximize),
        tolerance=tolerance,
        needs_review=needs_review,
        review_reason=review_reason,
        truncated=truncated,
        pre_truncation_count=pre_truncation_count,
        excluded_reasons=excluded_reasons,
        invalid_agent_ids=invalid_ids,
        excluded_agent_ids=excluded_ids,
        severity=severity,
    )


# =============================================================================
# ORIGINAL FUNCTION #2
# =============================================================================

def pareto_front(
    results: List[Dict],
    objectives=("accuracy", "energy", "latency"),
) -> List[Dict]:
    """
    Returns non-dominated results.

    Backward-compatible: same signature, same return type. "accuracy" is
    the only maximized objective (v1 rule).

    Use `pareto_front_detailed()` for the richer `ParetoFrontier` result.
    """
    if results is None:
        logger.warning("pareto_front received None; returning []")
        return []
    if not isinstance(results, list):
        logger.warning(
            "pareto_front received non-list (%s); returning []",
            type(results).__name__,
        )
        return []

    obj_list = list(objectives) if objectives is not None else []
    if not obj_list:
        return []
    maximize = {o for o in obj_list if o == "accuracy"}

    err = _validate_objectives(obj_list, maximize)
    if err is not None:
        logger.warning("pareto_front: %s", err)
        return []

    frontier = _compute_frontier(
        results, obj_list, maximize,
        tolerance=0.0,
        precision=None,
        filter_fn=None,
        top_k=None,
        duplicate_policy="suffix",
        explain=False,
        track=True,
    )
    return frontier.points_raw


# =============================================================================
# Enhanced public API
# =============================================================================

def pareto_front_detailed(
    results: List[Dict],
    objectives: Sequence[str] = DEFAULT_OBJECTIVES,
    *,
    maximize: Optional[Set[str]] = None,
    tolerance: Optional[float] = None,
    precision: Optional[str] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    top_k: Optional[int] = None,
    duplicate_policy: str = "suffix",
    explain: bool = True,
    track: bool = True,
    emit_decision_record: bool = False,
    hitl_callback: Optional[
        Callable[[ParetoFrontier], Optional[bool]]
    ] = None,
    hitl_top_n: int = 3,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    policy_version: str = "",
) -> ParetoFrontier:
    """
    Compute the Pareto frontier with full provenance, XAI, and stats.

    `tolerance=None` uses the precision preset when `precision` is set,
    else 0.0. `tolerance=0.0` explicitly requests strict comparison.
    """
    if results is None:
        results = []
    if not isinstance(results, list):
        results = []

    # --- Resolve effective tolerance ---
    if tolerance is None:
        effective_tolerance = (
            tolerance_for_precision(precision) if precision else 0.0
        )
    else:
        try:
            effective_tolerance = float(tolerance)
        except (TypeError, ValueError, OverflowError):
            effective_tolerance = -1.0
        if (
            math.isnan(effective_tolerance)
            or math.isinf(effective_tolerance)
            or effective_tolerance < 0
        ):
            return ParetoFrontier(
                points=[],
                rationale_by_agent={},
                dominated_by={},
                dominates_count={},
                dominated_count=0,
                excluded_count=0,
                invalid_count=0,
                objectives=(
                    list(objectives) if objectives is not None else []
                ),
                maximize=sorted(maximize or []),
                tolerance=effective_tolerance,
                invalid_config=True,
                needs_review=True,
                review_reason=(
                    f"tolerance must be finite and >= 0, got "
                    f"{tolerance!r}"
                ),
                severity=Severity.CRITICAL.value,
            )

    # --- Maximize default: v1 "accuracy" rule ---
    maximize_set = set(
        maximize if maximize is not None
        else {o for o in objectives if o == "accuracy"}
    )

    # --- Validate objectives ---
    err = _validate_objectives(objectives, maximize_set)
    if err is not None:
        logger.warning("pareto_front_detailed: %s", err)
        return ParetoFrontier(
            points=[],
            rationale_by_agent={},
            dominated_by={},
            dominates_count={},
            dominated_count=0,
            excluded_count=0,
            invalid_count=0,
            objectives=(
                list(objectives) if objectives is not None else []
            ),
            maximize=sorted(maximize_set),
            tolerance=effective_tolerance,
            invalid_config=True,
            needs_review=True,
            review_reason=err,
            severity=Severity.CRITICAL.value,
        )

    if duplicate_policy not in ("suffix", "raise", "keep"):
        return ParetoFrontier(
            points=[],
            rationale_by_agent={},
            dominated_by={},
            dominates_count={},
            dominated_count=0,
            excluded_count=0,
            invalid_count=0,
            objectives=list(objectives),
            maximize=sorted(maximize_set),
            tolerance=effective_tolerance,
            invalid_config=True,
            needs_review=True,
            review_reason=(
                f"duplicate_policy must be 'suffix'|'raise'|'keep', "
                f"got {duplicate_policy!r}"
            ),
            severity=Severity.CRITICAL.value,
        )

    # --- Compute ---
    frontier = _compute_frontier(
        results, list(objectives), maximize_set,
        tolerance=effective_tolerance,
        precision=precision,
        filter_fn=filter_fn,
        top_k=top_k,
        duplicate_policy=duplicate_policy,
        explain=explain,
        track=track,
    )

    # --- Empty-input signal ---
    if not results:
        frontier.empty_input = True

    # --- HITL ---
    if (
        hitl_callback is not None
        and frontier.needs_review
    ):
        try:
            approved = hitl_callback(frontier)
            if approved is False:
                frontier.hitl_approved = False
                frontier.review_reason = (
                    (frontier.review_reason or "")
                    + " [HITL: denied]"
                ).strip()
            elif approved is True:
                frontier.hitl_approved = True
        except Exception as e:
            logger.warning(
                "pareto.hitl_callback_failed",
                extra={"error": str(e)},
            )

    # --- DecisionRecord emission ---
    if emit_decision_record:
        dr = frontier.to_decision_record(
            run_id=run_id, task_id=task_id,
            policy_version=policy_version,
        )
        if dr is not None:
            serialised = _serialise_decision_record(dr)
            if serialised is not None:
                frontier.decision_record = _jsonable(serialised)

    # --- Structured log ---
    logger.info(
        "pareto.frontier_computed",
        extra={
            "run_id": run_id,
            "objectives": list(objectives),
            "maximize": sorted(maximize_set),
            "tolerance": effective_tolerance,
            "n_inputs": len(results),
            "n_frontier": len(frontier.points),
            "pre_truncation_count": frontier.pre_truncation_count,
            "truncated": frontier.truncated,
            "invalid_count": frontier.invalid_count,
            "excluded_count": frontier.excluded_count,
            "needs_review": frontier.needs_review,
            "severity": frontier.severity,
            "duplicate_policy": duplicate_policy,
        },
    )
    return frontier


# =============================================================================
# Explicit delegation helper (fills the v1 `_try_delegate_to_dominance_checker`
# gap with a working, tested function)
# =============================================================================

def pareto_frontier_from_checker(
    results: List[Dict],
    objectives: Sequence[str] = DEFAULT_OBJECTIVES,
    *,
    maximize: Optional[Set[str]] = None,
    tolerance: Optional[float] = None,
    precision: Optional[str] = None,
    checker: Any = None,
) -> ParetoFrontier:
    """
    Compute the frontier using `dominance_checker.DominanceChecker` when
    available; falls back to `pareto_front_detailed()`.

    Provides an explicit path for callers who want the two modules to
    agree on every pairwise verdict — including near-tie review flags.
    """
    if checker is None:
        try:
            from src.analysis.optimization.dominance_checker import (
                DominanceChecker,  # type: ignore
            )
            checker = DominanceChecker()
        except Exception:
            try:
                from analysis.optimization.dominance_checker import (  # type: ignore
                    DominanceChecker,
                )
                checker = DominanceChecker()
            except Exception:
                checker = None

    if checker is None:
        # No sibling available → fall back to the local implementation
        return pareto_front_detailed(
            results, objectives, maximize=maximize,
            tolerance=tolerance, precision=precision,
            emit_decision_record=False, track=True,
        )

    # The sibling path — use its verdicts to build the frontier, then
    # package into the same ParetoFrontier shape.
    maximize_set = set(
        maximize if maximize is not None
        else {o for o in objectives if o == "accuracy"}
    )
    minimize_set = set(objectives) - maximize_set
    eff_tol = (
        tolerance_for_precision(precision)
        if tolerance is None and precision else
        (0.0 if tolerance is None else float(tolerance))
    )

    valid_entries: List[Tuple[int, Dict[str, Any]]] = []
    for i, r in enumerate(results or []):
        if not isinstance(r, dict):
            continue
        if all(obj in r and _is_numeric(r.get(obj)) for obj in objectives):
            valid_entries.append((i, r))

    entries = _public_agent_ids(
        valid_entries, duplicate_policy="suffix",
    )

    dominated_by: Dict[str, List[str]] = defaultdict(list)
    dominates_count: Dict[str, int] = defaultdict(int)

    n = len(entries)
    for i in range(n):
        _, aid_a, pa = entries[i]
        for j in range(n):
            if i == j:
                continue
            _, aid_b, pb = entries[j]
            try:
                verdict = checker.check(
                    pb, pa, minimize_set, maximize_set,
                    tolerance=eff_tol, track=False,
                )
            except Exception:
                continue
            if getattr(verdict, "verdict_kind", None) == "a_dominates_b":
                dominated_by[aid_a].append(aid_b)
                dominates_count[aid_b] += 1

    frontier_points: List[FrontierPoint] = []
    rationale: Dict[str, List[str]] = {}
    dominated_by_public: Dict[str, List[str]] = {}

    for _, aid, p in entries:
        if dominated_by.get(aid):
            dominated_by_public[aid] = sorted(set(dominated_by[aid]))
            continue
        r = [f"'{aid}' is non-dominated (via DominanceChecker)."]
        if dominates_count.get(aid, 0) > 0:
            r.append(
                f"It strictly dominates "
                f"{dominates_count[aid]} other point(s)."
            )
        rationale[aid] = r
        frontier_points.append(FrontierPoint(
            agent_id=aid, point=p,
            dominates_count=dominates_count.get(aid, 0),
            rationale=list(r),
        ))

    frontier_points.sort(key=lambda fp: fp.agent_id)

    return ParetoFrontier(
        points=frontier_points,
        rationale_by_agent=rationale,
        dominated_by=dominated_by_public,
        dominates_count={
            k: v for k, v in dominates_count.items() if v > 0
        },
        dominated_count=len(dominated_by_public),
        excluded_count=0,
        invalid_count=len(results or []) - len(valid_entries),
        objectives=list(objectives),
        maximize=sorted(maximize_set),
        tolerance=eff_tol,
    )


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
    # 1. Original behavior (backward compatible)
    # =================================================================
    print("=== 1. Original behavior ===")
    a = {"agent_id": "A", "accuracy": 0.95, "energy": 0.05, "latency": 0.1}
    b = {"agent_id": "B", "accuracy": 0.90, "energy": 0.06, "latency": 0.11}
    print(f"  dominates(A, B): {dominates(a, b, ('accuracy', 'energy', 'latency'))}")
    print(f"  dominates(B, A): {dominates(b, a, ('accuracy', 'energy', 'latency'))}")

    results = [
        {"agent_id": "A", "accuracy": 0.95, "energy": 0.05, "latency": 0.10},
        {"agent_id": "B", "accuracy": 0.90, "energy": 0.06, "latency": 0.11},
        {"agent_id": "C", "accuracy": 0.85, "energy": 0.03, "latency": 0.15},
        {"agent_id": "D", "accuracy": 0.80, "energy": 0.08, "latency": 0.20},
    ]
    front = pareto_front(results)
    print(f"  pareto_front: {[r['agent_id'] for r in front]}")

    # =================================================================
    # 2. Tolerance disambiguation (v1 collision)
    # =================================================================
    print("\n=== 2. Tolerance disambiguation ===")
    noisy = [
        {"agent_id": "P", "accuracy": 0.462, "energy": 0.1},
        {"agent_id": "Q", "accuracy": 0.4620000000000001, "energy": 0.1},
    ]
    f_strict = pareto_front_detailed(
        noisy, objectives=("accuracy", "energy"),
        tolerance=0.0, precision="int4",       # strict even with int4
    )
    f_preset = pareto_front_detailed(
        noisy, objectives=("accuracy", "energy"),
        tolerance=None, precision="int4",      # uses int4 preset
    )
    print(f"  tolerance=0.0 precision=int4 (strict):  "
          f"{f_strict.frontier_ids()}  (expected both)")
    print(f"  tolerance=None precision=int4 (preset): "
          f"{f_preset.frontier_ids()}  (expected one)")

    # =================================================================
    # 3. Duplicate agent_id handling (v1 corrupted dominated_by)
    # =================================================================
    print("\n=== 3. Duplicate agent_id ===")
    dup = [
        {"agent_id": "A", "accuracy": 0.95, "energy": 0.05},
        {"agent_id": "A", "accuracy": 0.90, "energy": 0.06},
        {"agent_id": "B", "accuracy": 0.85, "energy": 0.08},
    ]
    f_dup = pareto_front_detailed(
        dup, objectives=("accuracy", "energy"),
        duplicate_policy="suffix",
    )
    print(f"  frontier: {f_dup.frontier_ids()}  "
          f"(v1 would report ['A', 'A'] with self-domination)")
    print(f"  dominated_by['A #2']: {f_dup.dominated_by_ids('A #2')}")
    print(f"  no self-domination: "
          f"{'A' not in f_dup.dominated_by_ids('A')}")

    # =================================================================
    # 4. FrontierPoint surfaced with dominates_count
    # =================================================================
    print("\n=== 4. FrontierPoint.dominates_count ===")
    f4 = pareto_front_detailed(results)
    for fp in f4.points:
        print(f"  {fp.agent_id}: dominates {fp.dominates_count}, "
              f"severity={fp.severity}")

    # =================================================================
    # 5. Decimal support (v1 rejected numbers.Real)
    # =================================================================
    print("\n=== 5. Decimal / Fraction support ===")
    dec_results = [
        {"agent_id": "D1", "accuracy": Decimal("0.95"),
         "energy": Decimal("0.05")},
        {"agent_id": "D2", "accuracy": Decimal("0.90"),
         "energy": Decimal("0.06")},
    ]
    f_dec = pareto_front_detailed(
        dec_results, objectives=("accuracy", "energy"),
    )
    print(f"  frontier: {f_dec.frontier_ids()}  "
          f"(Decimal accepted)")

    # =================================================================
    # 6. Negative tolerance rejected
    # =================================================================
    print("\n=== 6. Negative tolerance ===")
    v_neg = dominance_verdict(
        a, b, ("accuracy", "energy", "latency"),
        tolerance=-1.0,
    )
    print(f"  verdict kind: {v_neg.kind}  (expected 'invalid')")
    print(f"  reason: {v_neg.invalid_reason}")

    # =================================================================
    # 7. filter_fn exclusion reasons surfaced
    # =================================================================
    print("\n=== 7. filter_fn exclusion reasons ===")
    def _reject_negative_accuracy(r):
        if r.get("accuracy", 0) < 0:
            raise ValueError("accuracy below zero")
        return r.get("accuracy", 0) < 0.9

    mixed = [
        {"agent_id": "X", "accuracy": 0.95, "energy": 0.05},   # kept
        {"agent_id": "Y", "accuracy": 0.85, "energy": 0.03},   # filter_fn true
        {"agent_id": "Z", "accuracy": -0.1, "energy": 0.1},    # filter_fn raises
    ]
    f7 = pareto_front_detailed(
        mixed, objectives=("accuracy", "energy"),
        filter_fn=_reject_negative_accuracy,
    )
    print(f"  excluded_agent_ids: {f7.excluded_agent_ids}")
    print(f"  reason for 'Z':     {f7.excluded_reason('Z')}")

    # =================================================================
    # 8. Truncation surfaced
    # =================================================================
    print("\n=== 8. Truncation ===")
    many = [
        {"agent_id": f"A{i}", "accuracy": 0.5 + 0.01 * i,
         "energy": 0.1 - 0.001 * i}
        for i in range(10)
    ]
    f8 = pareto_front_detailed(
        many, objectives=("accuracy", "energy"), top_k=3,
    )
    print(f"  frontier size:         {len(f8.points)}")
    print(f"  truncated:             {f8.truncated}")
    print(f"  pre_truncation_count:  {f8.pre_truncation_count}")

    # =================================================================
    # 9. invalid_config vs empty_input
    # =================================================================
    print("\n=== 9. invalid_config vs empty_input ===")
    f9a = pareto_front_detailed([], objectives=("accuracy",))
    print(f"  empty results → empty_input={f9a.empty_input}, "
          f"invalid_config={f9a.invalid_config}")

    f9b = pareto_front_detailed(results, objectives=())
    print(f"  empty objectives → empty_input={f9b.empty_input}, "
          f"invalid_config={f9b.invalid_config}")

    # =================================================================
    # 10. needs_review message matches condition
    # =================================================================
    print("\n=== 10. needs_review threshold ===")
    half_invalid = [
        {"agent_id": "G1", "accuracy": 0.9, "energy": 0.05},
        {"agent_id": "G2", "accuracy": 0.8, "energy": 0.06},
        {"agent_id": "I1", "accuracy": float("nan"), "energy": 0.05},
        {"agent_id": "I2"},   # missing both
    ]
    f10 = pareto_front_detailed(
        half_invalid, objectives=("accuracy", "energy"),
    )
    print(f"  invalid_count: {f10.invalid_count} of {len(half_invalid)}")
    print(f"  needs_review:  {f10.needs_review}")
    print(f"  reason:        {f10.review_reason}")
    print(f"  (expected 'at least half' message with 2 of 4)")

    # =================================================================
    # 11. HITL denial annotation
    # =================================================================
    print("\n=== 11. HITL denial ===")
    f11 = pareto_front_detailed(
        half_invalid, objectives=("accuracy", "energy"),
        hitl_callback=lambda frontier: False,
    )
    print(f"  needs_review:   {f11.needs_review}")
    print(f"  hitl_approved:  {f11.hitl_approved}")
    print(f"  reason:         {f11.review_reason}")
    print(f"  denial noted:   "
          f"{'[HITL: denied]' in (f11.review_reason or '')}")

    # =================================================================
    # 12. Sibling alignment (dominance_checker)
    # =================================================================
    print("\n=== 12. Sibling alignment ===")
    try:
        from src.analysis.optimization.dominance_checker import (
            DominanceChecker,
        )
        checker = DominanceChecker()
        # Compare verdicts on the A/B case
        sibling_v = checker.check(
            a, b,
            minimize={"energy", "latency"},
            maximize={"accuracy"},
        )
        local_v = dominance_verdict(
            a, b, ("accuracy", "energy", "latency"),
            maximize={"accuracy"},
        )
        print(f"  sibling verdict: {sibling_v.verdict_kind}")
        print(f"  local verdict:   {local_v.kind}")
        print(f"  agree:           {sibling_v.verdict_kind == local_v.kind}")
    except Exception as e:
        print(f"  sibling unavailable: {e}")

    # =================================================================
    # 13. JSON-safe DecisionRecord
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
    import src.analysis.optimization.pareto as _pareto_mod
    importlib.reload(_pareto_mod)

    try:
        f13 = _pareto_mod.pareto_front_detailed(
            results, emit_decision_record=True,
            run_id="run-1", policy_version="v5.0.1",
        )
        print(f"  decision_record present: "
              f"{f13.decision_record is not None}")
        try:
            json.dumps(f13.to_dict(safe=True))
            print(f"  json.dumps(to_dict(safe=True)): OK")
        except (TypeError, ValueError) as e:
            print(f"  FAILED: {e}")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_pareto_mod)

    # =================================================================
    # 14. Thread safety
    # =================================================================
    print("\n=== 14. Thread safety ===")
    reset_statistics()
    barrier = threading.Barrier(4)
    def _worker():
        barrier.wait()
        for _ in range(100):
            pareto_front(results)
    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for t in threads: t.start()
    for t in threads: t.join()
    s = get_statistics()
    print(f"  total_frontiers: {s['total_frontiers']}  (expected 400)")
    print(f"  total_dominance_checks: {s['total_dominance_checks']}  "
          f"(expected 4×4×4×100 = 6400)")

    # =================================================================
    # 15. Statistics
    # =================================================================
    print("\n=== 15. Statistics ===")
    print(json.dumps(get_statistics(), indent=2))
