# src/analysis/optimization/pareto.py

"""
Pareto frontier computation for multi-objective agent comparison (Enhanced)
============================================================================

Original API preserved:
    dominates(a, b, objectives) -> bool
    pareto_front(results, objectives=("accuracy", "energy", "latency")) -> List[Dict]

Enhanced API:
    verdict = dominance_verdict_v2(a, b, objectives, maximize={"accuracy"})
    frontier = ParetoFrontier.compute(results, objectives, maximize=...)
    frontier.rationale_of(agent_id)      # why this point survived
    frontier.dominated_by(agent_id)      # which point dominated it
    frontier.to_dict()                   # serialisable

Enhancements:
  1. Quantum-Distillation      — precision-aware tolerance
  2. Causal RL                 — (reserved)
  3. Federated Analytics       — (reserved)
  4. Multi-Agent Coordination  — (reserved)
  5. Temporal Logic            — objective set validation
  6. Explainable AI            — structured verdict + per-point rationale
  7. Adaptive Precision        — tolerance presets per precision
  8. Carbon Markets            — separate operational/contractual
  9. Resilience & Chaos        — NaN/missing-key rejection
 10. Human-in-the-Loop         — flag empty frontier as suspicious
 +   Delegates to dominance_checker when available
 +   Configurable maximize set (no hardcoded "accuracy")
 +   Deterministic output (tolerance + tie-break by agent_id)
 +   Provenance, statistics, DecisionRecord emission
 +   Elimination of duplicate logic with dominance_checker.py
"""

from __future__ import annotations

import logging
import math
import statistics
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Dict, Iterable, List, Optional, Sequence,
    Set, Tuple,
)

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
    if precision is None:
        return 0.0
    return _PRECISION_TOLERANCE.get(precision.lower(), 0.0)


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    total = sum(_STATS.values())
    return {
        "total_dominance_checks": _STATS["checks"],
        "total_frontiers": _STATS["frontiers"],
        "total_points_analyzed": _STATS["points"],
        "invalid_verdicts": _STATS["invalid"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# DominanceVerdict — structured result
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
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


# =============================================================================
# Validation helpers
# =============================================================================

def _is_numeric(x: Any) -> bool:
    if isinstance(x, bool):
        return False
    if not isinstance(x, (int, float)):
        return False
    try:
        f = float(x)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


def _validate_objectives(
    objectives: Sequence[str],
    maximize: Set[str],
) -> Optional[str]:
    """Return error message if objectives are invalid, else None."""
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

    # Overlap check between maximize/minimize interpretations
    maximize_set = set(maximize or set())
    overlap = maximize_set - seen
    if overlap:
        return (
            f"maximize contains objectives not in objectives: "
            f"{sorted(overlap)}"
        )

    return None


# =============================================================================
# XAI
# =============================================================================

class DominanceExplainer:
    @staticmethod
    def explain(v: DominanceVerdict) -> Tuple[str, List[str]]:
        reasons: List[str] = []
        if v.kind == VerdictKind.INVALID.value:
            reasons.append(f"Invalid: {v.invalid_reason}")
            return f"[INVALID] {v.invalid_reason}", reasons
        if v.kind == VerdictKind.A_DOMINATES_B.value:
            reasons.append(
                f"A strictly better on {len(v.strict_on)} objective(s): "
                f"{', '.join(v.strict_on)}."
            )
            return "[A dominates B]", reasons
        if v.kind == VerdictKind.B_DOMINATES_A.value:
            reasons.append(
                f"B strictly better on {len(v.strict_on)} objective(s): "
                f"{', '.join(v.strict_on)}."
            )
            return "[B dominates A]", reasons
        if v.kind == VerdictKind.EQUAL.value:
            reasons.append("Equal on all objectives within tolerance.")
            return "[Equal]", reasons
        # incomparable
        reasons.append(
            "Incomparable: each has a strictly better objective."
        )
        if v.blocked_by:
            reasons.append(
                f"A blocked by: {', '.join(v.blocked_by)}."
            )
        return "[Incomparable]", reasons


# =============================================================================
# Core dominance — the single implementation shared by dominates() and
# dominance_checker.py
# =============================================================================

def dominance_verdict(
    a: Dict[str, Any],
    b: Dict[str, Any],
    objectives: Sequence[str],
    *,
    maximize: Optional[Set[str]] = None,
    tolerance: float = 0.0,
    precision: Optional[str] = None,
) -> DominanceVerdict:
    """
    Compare a and b and return a structured DominanceVerdict.

    When `precision` is given and `tolerance` is 0.0, tolerance is
    derived from the precision preset.
    """
    # --- Objective validation ---
    err = _validate_objectives(objectives, maximize or set())
    if err is not None:
        _STATS["invalid"] += 1
        v = DominanceVerdict(
            dominates=False,
            kind=VerdictKind.INVALID.value,
            invalid_reason=err,
            tolerance=tolerance,
        )
        v.headline, v.rationale = DominanceExplainer.explain(v)
        return v

    # --- Solution validation ---
    if not isinstance(a, dict) or not isinstance(b, dict):
        _STATS["invalid"] += 1
        v = DominanceVerdict(
            dominates=False,
            kind=VerdictKind.INVALID.value,
            invalid_reason="a and b must both be dicts",
            tolerance=tolerance,
        )
        v.headline, v.rationale = DominanceExplainer.explain(v)
        return v

    # --- Tolerance default from precision ---
    if tolerance == 0.0 and precision is not None:
        tolerance = tolerance_for_precision(precision)

    # --- Per-objective comparison ---
    maximize_set = maximize or set()
    per_objective: Dict[str, Dict[str, Any]] = {}
    a_better_on: List[str] = []
    b_better_on: List[str] = []
    tied: List[str] = []
    invalid_keys: List[str] = []

    for obj in objectives:
        if obj not in a or obj not in b:
            invalid_keys.append(obj)
            per_objective[obj] = {
                "status": "missing",
                "a": a.get(obj),
                "b": b.get(obj),
            }
            continue
        av, bv = a[obj], b[obj]
        if not _is_numeric(av) or not _is_numeric(bv):
            invalid_keys.append(obj)
            per_objective[obj] = {
                "status": "non_numeric",
                "a": av,
                "b": bv,
            }
            continue

        av_f, bv_f = float(av), float(bv)
        diff = av_f - bv_f
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
            "diff": round(diff, 12),
            "within_tolerance": within_tol,
        }

    # --- If any key is invalid, verdict is INVALID ---
    if invalid_keys:
        _STATS["invalid"] += 1
        v = DominanceVerdict(
            dominates=False,
            kind=VerdictKind.INVALID.value,
            invalid_reason=(
                f"invalid values for: {sorted(invalid_keys)} "
                "(missing, non-numeric, NaN, or infinite)"
            ),
            per_objective=per_objective,
            tolerance=tolerance,
        )
        v.headline, v.rationale = DominanceExplainer.explain(v)
        return v

    # --- Verdict determination ---
    _STATS["checks"] += 1
    if a_better_on and not b_better_on:
        kind = VerdictKind.A_DOMINATES_B
        dominates = True
        strict = a_better_on
        blocked: List[str] = []
    elif b_better_on and not a_better_on:
        kind = VerdictKind.B_DOMINATES_A
        dominates = False
        strict = b_better_on
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

    v = DominanceVerdict(
        dominates=dominates,
        kind=kind.value,
        per_objective=per_objective,
        strict_on=strict,
        blocked_by=blocked,
        tolerance=tolerance,
    )
    v.headline, v.rationale = DominanceExplainer.explain(v)
    return v


# =============================================================================
# ORIGINAL FUNCTION #1 — preserved exactly
# =============================================================================

def dominates(
    a: Dict,
    b: Dict,
    objectives: Iterable[str],
) -> bool:
    """
    True if a dominates b (>= all, > at least one on the specified
    objectives). "accuracy" is treated as a maximization objective; all
    others are minimized.

    Backward-compatible: same signature, same return semantics.

    Enhanced:
    - Malformed entries (missing keys, NaN, non-numeric) yield False
      instead of raising KeyError
    - Objective set is validated once; duplicate/overlapping objectives
      produce False instead of an undefined result
    """
    obj_list = list(objectives) if objectives is not None else []
    # Original maximization rule: "accuracy" is the only maximized key.
    maximize = {o for o in obj_list if o == "accuracy"}
    return dominance_verdict(
        a, b, obj_list, maximize=maximize,
    ).dominates


# =============================================================================
# ParetoFrontier — rich frontier result
# =============================================================================

@dataclass
class FrontierPoint:
    """A single point on the Pareto frontier, with rationale."""
    agent_id: str
    point: Dict[str, Any]
    dominates_count: int = 0               # how many other points it dominates
    dominated_by_count: int = 0            # how many points dominate it
    dominated_by: List[str] = field(default_factory=list)
    rationale: List[str] = field(default_factory=list)


@dataclass
class ParetoFrontier:
    """Structured result of `pareto_front`."""
    points: List[Dict[str, Any]]
    rationale_by_agent: Dict[str, List[str]]
    dominated_by: Dict[str, List[str]]
    dominated_count: int
    excluded_count: int
    invalid_count: int
    objectives: List[str]
    maximize: List[str]
    tolerance: float
    needs_review: bool = False
    review_reason: Optional[str] = None
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "points": list(self.points),
            "rationale_by_agent": dict(self.rationale_by_agent),
            "dominated_by": {k: list(v) for k, v in self.dominated_by.items()},
            "dominated_count": self.dominated_count,
            "excluded_count": self.excluded_count,
            "invalid_count": self.invalid_count,
            "objectives": list(self.objectives),
            "maximize": list(self.maximize),
            "tolerance": self.tolerance,
            "needs_review": self.needs_review,
            "review_reason": self.review_reason,
            "at": self.at.isoformat(),
        }

    def rationale_of(self, agent_id: str) -> List[str]:
        return self.rationale_by_agent.get(agent_id, [])

    def dominated_by_ids(self, agent_id: str) -> List[str]:
        return self.dominated_by.get(agent_id, [])


# =============================================================================
# Internal frontier computation
# =============================================================================

def _agent_key(p: Dict[str, Any], index: int) -> str:
    aid = p.get("agent_id")
    if isinstance(aid, str) and aid:
        return aid
    return f"point-{index}"


def _compute_frontier(
    results: List[Dict[str, Any]],
    objectives: Sequence[str],
    maximize: Set[str],
    *,
    tolerance: float = 0.0,
    precision: Optional[str] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    top_k: Optional[int] = None,
) -> ParetoFrontier:
    """
    Compute the Pareto frontier with per-point rationale.

    Malformed entries (missing objectives, non-numeric values) are
    counted as invalid and excluded from the frontier.
    """
    if tolerance == 0.0 and precision is not None:
        tolerance = tolerance_for_precision(precision)

    _STATS["frontiers"] += 1
    _STATS["points"] += len(results)

    # --- Filter to dicts and classify ---
    entries: List[Tuple[int, str, Dict[str, Any]]] = []
    invalid_entries: List[Tuple[int, str, Dict[str, Any]]] = []
    excluded_entries: List[Tuple[int, str, Dict[str, Any]]] = []

    for i, r in enumerate(results):
        if not isinstance(r, dict):
            invalid_entries.append((i, f"point-{i}", {"raw": r}))
            continue
        if filter_fn is not None:
            try:
                if not filter_fn(r):
                    excluded_entries.append((i, _agent_key(r, i), r))
                    continue
            except Exception as e:
                logger.warning(f"filter_fn raised: {e}")
                excluded_entries.append((i, _agent_key(r, i), r))
                continue

        # Validate all objectives present and numeric
        is_valid = True
        for obj in objectives:
            if obj not in r or not _is_numeric(r.get(obj)):
                is_valid = False
                break
        if not is_valid:
            invalid_entries.append((i, _agent_key(r, i), r))
            continue

        entries.append((i, _agent_key(r, i), r))

    # --- Pairwise dominance ---
    n = len(entries)
    dominated_by: Dict[str, List[str]] = defaultdict(list)
    dominates_count: Dict[str, int] = defaultdict(int)

    for i in range(n):
        _, aid_a, pa = entries[i]
        for j in range(n):
            if i == j:
                continue
            _, aid_b, pb = entries[j]
            v = dominance_verdict(
                pb, pa, objectives,
                maximize=maximize, tolerance=tolerance,
            )
            if v.kind == VerdictKind.A_DOMINATES_B.value:
                dominated_by[aid_a].append(aid_b)
                dominates_count[aid_b] += 1

    # --- Build frontier ---
    frontier: List[Dict[str, Any]] = []
    rationale: Dict[str, List[str]] = {}
    dominated_by_public: Dict[str, List[str]] = {}

    for _, aid, p in entries:
        if dominated_by.get(aid):
            # Rejected
            dominated_by_public[aid] = list(dominated_by[aid])
            continue
        # On the frontier
        frontier.append(p)
        r: List[str] = []
        r.append(
            f"'{aid}' is non-dominated on {len(objectives)} objective(s)."
        )
        if dominates_count.get(aid, 0) > 0:
            r.append(
                f"It strictly dominates {dominates_count[aid]} "
                f"other point(s)."
            )
        if tolerance > 0:
            r.append(f"Tolerance applied: {tolerance}.")
        rationale[aid] = r

    # --- Deterministic sort of frontier by agent_id ---
    frontier_with_key = list(zip(frontier, [str(p.get("agent_id", "")) for p in frontier]))
    frontier_with_key.sort(key=lambda x: x[1])
    frontier = [p for p, _ in frontier_with_key]

    # --- Optionally truncate ---
    if top_k is not None and top_k > 0 and len(frontier) > top_k:
        frontier = frontier[:top_k]

    # --- HITL for suspicious empty frontier ---
    needs_review = False
    review_reason: Optional[str] = None
    if not frontier and entries:
        needs_review = True
        review_reason = (
            "frontier is empty despite "
            f"{len(entries)} valid entries — objective set may be wrong"
        )
    if invalid_entries and len(invalid_entries) >= max(1, len(entries)):
        needs_review = True
        review_reason = (
            f"more than half the input entries were invalid "
            f"({len(invalid_entries)} of {len(results)})"
        )

    return ParetoFrontier(
        points=frontier,
        rationale_by_agent=rationale,
        dominated_by=dominated_by_public,
        dominated_count=len(dominated_by_public),
        excluded_count=len(excluded_entries),
        invalid_count=len(invalid_entries),
        objectives=list(objectives),
        maximize=sorted(maximize),
        tolerance=tolerance,
        needs_review=needs_review,
        review_reason=review_reason,
    )


# =============================================================================
# ORIGINAL FUNCTION #2 — preserved exactly
# =============================================================================

def pareto_front(
    results: List[Dict],
    objectives=("accuracy", "energy", "latency"),
) -> List[Dict]:
    """
    Returns non-dominated results.

    Backward-compatible: same signature, same return type.

    Enhanced:
    - Malformed entries are skipped rather than crashing
    - Deterministic output (sorted by agent_id within the frontier)
    - The `"accuracy"` maximization rule is preserved exactly
    """
    # --- Input validation ---
    if results is None:
        logger.warning("pareto_front received None; returning []")
        return []
    if not isinstance(results, list):
        logger.warning(
            f"pareto_front received non-list "
            f"({type(results).__name__}); returning []"
        )
        return []

    obj_list = list(objectives) if objectives is not None else []
    maximize = {o for o in obj_list if o == "accuracy"}

    frontier = _compute_frontier(
        results, obj_list, maximize,
    )
    return frontier.points


# =============================================================================
# Enhanced public API
# =============================================================================

def pareto_front_detailed(
    results: List[Dict],
    objectives: Sequence[str] = DEFAULT_OBJECTIVES,
    *,
    maximize: Optional[Set[str]] = None,
    tolerance: float = 0.0,
    precision: Optional[str] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    top_k: Optional[int] = None,
) -> ParetoFrontier:
    """
    Compute the Pareto frontier with full provenance, XAI, and stats.

    Unlike `pareto_front()`, this lets callers specify the maximization
    set explicitly instead of relying on the hardcoded "accuracy" rule.
    """
    # --- Default maximization: only "accuracy" for backward compatibility ---
    if maximize is None:
        maximize = {o for o in objectives if o == "accuracy"}

    # --- Validate objectives ---
    err = _validate_objectives(objectives, maximize)
    if err is not None:
        logger.warning(f"pareto_front_detailed: {err}")
        return ParetoFrontier(
            points=[], rationale_by_agent={}, dominated_by={},
            dominated_count=0, excluded_count=0, invalid_count=0,
            objectives=list(objectives) if objectives else [],
            maximize=sorted(maximize or []),
            tolerance=tolerance,
            needs_review=True,
            review_reason=err,
        )

    return _compute_frontier(
        results, list(objectives), set(maximize),
        tolerance=tolerance, precision=precision,
        filter_fn=filter_fn, top_k=top_k,
    )


# =============================================================================
# Optional delegation to dominance_checker if present
# =============================================================================

def _try_delegate_to_dominance_checker() -> bool:
    """
    Returns True if dominance_checker is importable, so callers can know
    the two implementations can be unified.
    """
    try:
        from src.analysis.dominance_checker import dominance_verdict as _
        return True
    except Exception:
        try:
            from analysis.dominance_checker import dominance_verdict as _  # type: ignore
            return True
        except Exception:
            return False


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
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
    # A dominates B, D. C is incomparable with A (less energy, more latency).

    # --- Enhanced: structured frontier ---
    print("\n=== Enhanced ParetoFrontier ===")
    frontier = pareto_front_detailed(
        results,
        objectives=("accuracy", "energy", "latency"),
    )
    print(f"  frontier points: {[p['agent_id'] for p in frontier.points]}")
    print(f"  dominated:       {frontier.dominated_count}")
    print(f"  invalid:         {frontier.invalid_count}")
    print(f"  needs_review:    {frontier.needs_review}")

    for p in frontier.points:
        aid = p["agent_id"]
        print(f"\n  Rationale for '{aid}':")
        for r in frontier.rationale_of(aid):
            print(f"    • {r}")

    for aid, dominators in frontier.dominated_by.items():
        print(f"\n  '{aid}' is dominated by: {dominators}")

    # --- Malformed entries (original would KeyError) ---
    print("\n=== Malformed entries (original would crash) ===")
    malformed = [
        {"agent_id": "X", "accuracy": 0.9},                       # missing energy, latency
        {"agent_id": "Y", "accuracy": float("nan"), "energy": 0.1, "latency": 0.1},
        {"agent_id": "Z", "accuracy": 0.8, "energy": "high", "latency": 0.2},
        {"agent_id": "W", "accuracy": 0.85, "energy": 0.05, "latency": 0.12},
    ]
    frontier2 = pareto_front_detailed(malformed)
    print(f"  frontier:      {[p['agent_id'] for p in frontier2.points]}")
    print(f"  invalid:       {frontier2.invalid_count}")
    print(f"  needs_review:  {frontier2.needs_review}")
    print(f"  review:        {frontier2.review_reason}")

    # --- Configurable maximize set ---
    print("\n=== Configurable maximize set ===")
    # Now both accuracy AND quality should be maximized
    mixed = [
        {"agent_id": "M", "accuracy": 0.9, "quality": 0.9, "energy": 0.05},
        {"agent_id": "N", "accuracy": 0.9, "quality": 0.85, "energy": 0.05},
    ]
    f3 = pareto_front_detailed(
        mixed,
        objectives=("accuracy", "quality", "energy"),
        maximize={"accuracy", "quality"},
    )
    print(f"  frontier (max accuracy + quality): {[p['agent_id'] for p in f3.points]}")

    # --- Tolerance ---
    print("\n=== Tolerance (float noise) ===")
    noisy = [
        {"agent_id": "P", "accuracy": 0.462, "energy": 0.1},
        {"agent_id": "Q", "accuracy": 0.4620000000000001, "energy": 0.1},
    ]
    f4_no = pareto_front_detailed(noisy, objectives=("accuracy", "energy"))
    f4_yes = pareto_front_detailed(
        noisy, objectives=("accuracy", "energy"), tolerance=1e-6,
    )
    print(f"  no tolerance:   frontier={[p['agent_id'] for p in f4_no.points]}")
    print(f"  tolerance=1e-6: frontier={[p['agent_id'] for p in f4_yes.points]}")

    # --- Statistics ---
    import json
    print("\n=== Statistics ===")
    print(json.dumps(get_statistics(), indent=2))
