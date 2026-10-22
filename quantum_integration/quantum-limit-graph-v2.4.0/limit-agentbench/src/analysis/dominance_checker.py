# src/analysis/optimization/dominance_checker.py
"""
Pareto dominance checking for Green Agent (Enhanced, v2.0.0)
=============================================================

Determines whether one solution Pareto-dominates another across a mixed
set of minimize / maximize objectives.

Original API preserved (all signatures & semantics):
    dominates(a, b, minimize={"energy"}, maximize={"quality"}) -> bool
    get_statistics() -> dict
    reset_statistics() -> None
    filter_dominated(solutions, minimize, maximize) -> list

Enhanced API:
    # Scoped, thread-safe checker (recommended for services/tests)
    checker = DominanceChecker()
    verdict = checker.check(a, b, minimize, maximize,
                            tolerance=None,          # None => precision preset
                            precision="int4",
                            strict_equality=False,
                            tolerance_mode="absolute",  # or "relative"
                            context={"trace_id": "...", "agent_id": "..."})
    verdict.dominates
    verdict.verdict_kind       # "a_dominates_b" | "b_dominates_a"
                               # | "equal" | "incomparable" | "invalid"
    verdict.per_objective      # per-objective attribution + margin
    verdict.rationale          # witness-rich XAI
    checker.get_statistics()
    checker.set_hitl_callback(cb)

    # Backward-compatible module-level shims (share a default checker)
    dominance_verdict(a, b, minimize, maximize, ...) -> DominanceVerdict

    # Non-dominated sorting (layered Pareto frontier)
    frontier = ParetoFrontier(solutions, minimize, maximize)
    frontier.frontier         # layer 0
    frontier.layers           # all layers
    frontier.ranks            # per-solution layer index

Enhancements implemented (v2.0.0):
  1.  Input sanitisation      — NaN/None/non-numeric values rejected cleanly
  2.  Adaptive precision      — tolerance presets per precision level
  3.  Strict Pareto mode      — strict_equality=True bypasses the tolerance band
  4.  Relative tolerance      — scale-aware equality check
  5.  Per-side attribution    — a_better_on / b_better_on / blocks_a / blocks_b
  6.  Sensitivity margins     — min_margin_ratio for near-tie / narrow-win detection
  7.  Structured XAI          — headline, rationale, witness values
  8.  HITL                    — near-tie / narrow-domination flags + callback
  9.  Scoped statistics       — DominanceChecker (thread-safe, per-instance)
 10.  Provenance              — trace_id / run_id / solution ids / agent_id
 11.  Non-dominated sorting   — ParetoFrontier (layered, O(n²) Deb et al.)
 12.  JSON-safe serialisation — to_dict(safe=True) sanitises non-primitive values
 13.  Backward compatibility  — dominates() and module helpers preserved
 14.  Empty/negative guards   — empty objectives, negative tolerance, bad modes
"""

from __future__ import annotations

import logging
import math
import numbers
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

__version__ = "2.0.0"

__all__ = [
    "DominanceChecker",
    "DominanceVerdict",
    "VerdictKind",
    "ObjectiveDirection",
    "DominanceExplainer",
    "ParetoFrontier",
    "tolerance_for_precision",
    "dominance_verdict",
    "dominates",
    "filter_dominated",
    "pareto_layers",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class VerdictKind(Enum):
    """Classification of a dominance comparison."""
    A_DOMINATES_B = "a_dominates_b"
    B_DOMINATES_A = "b_dominates_a"
    EQUAL = "equal"
    INCOMPARABLE = "incomparable"
    INVALID = "invalid"          # missing keys, NaN, non-numeric, overlap, empty

    @classmethod
    def from_str(cls, s: str) -> "VerdictKind":
        """Parse a string to a VerdictKind, raising KeyError on unknown values."""
        for member in cls:
            if member.value == s:
                return member
        raise KeyError(f"Unknown VerdictKind string: {s!r}")


class ObjectiveDirection(Enum):
    """Direction of an objective."""
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"


# =============================================================================
# Precision tolerance presets
# =============================================================================

# Suggested floating-point tolerance per precision level. At lower precision
# values are quantised more coarsely, so "equal" should cover a wider band.
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
    """Return the recommended tolerance for a given precision level."""
    if not isinstance(precision, str):
        return 0.0
    return _PRECISION_TOLERANCE.get(precision.lower(), 0.0)


# =============================================================================
# Helpers
# =============================================================================

_VALID_CTX_KEYS = {
    "trace_id", "run_id", "task_id",
    "solution_id_a", "solution_id_b", "agent_id",
}


def _is_numeric(x: Any) -> bool:
    """True iff x is a finite real number (explicitly rejects bool)."""
    if isinstance(x, bool):
        return False
    if not isinstance(x, numbers.Real):
        return False
    try:
        f = float(x)
    except (TypeError, ValueError, OverflowError):
        return False
    return math.isfinite(f)


def _json_safe(x: Any) -> Any:
    """Coerce x to a JSON-serialisable primitive; non-primitives → repr(x)."""
    if x is None or isinstance(x, (bool, int, str)):
        return x
    if isinstance(x, float):
        return x if math.isfinite(x) else str(x)
    return repr(x)


def _extract_context(context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Keep only the recognised provenance keys, warn about the rest."""
    if not context or not isinstance(context, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in context.items():
        if k in _VALID_CTX_KEYS:
            out[k] = v
        else:
            logger.debug("Ignoring unknown context key: %r", k)
    return out


def _validate_objectives(
    minimize: Optional[Set[str]],
    maximize: Optional[Set[str]],
) -> Optional[str]:
    """
    Return an error message if objectives are invalid, else None.

    Checks:
      - minimize and maximize are not None
      - both are iterable sets of strings (hashable)
      - they do not overlap
      - at least one objective is specified
    """
    if minimize is None or maximize is None:
        return "minimize and maximize must not be None"
    try:
        min_set = set(minimize)
        max_set = set(maximize)
    except TypeError:
        return "minimize and maximize must be iterable containers of strings"

    for s in min_set | max_set:
        if not isinstance(s, str):
            return f"objective name must be str, got {type(s).__name__}"

    overlap = min_set & max_set
    if overlap:
        return (
            f"objective(s) declared in both minimize and maximize: "
            f"{sorted(overlap)}"
        )
    if not min_set and not max_set:
        return "no objectives specified"
    return None


def _validate_solution(
    solution: Any, objectives: List[str],
) -> Optional[str]:
    """Return an error message if `solution` is malformed w.r.t. `objectives`."""
    if not isinstance(solution, dict):
        return f"solution must be a dict, got {type(solution).__name__}"
    for obj in objectives:
        if obj not in solution:
            return f"missing objective: {obj!r}"
        if not _is_numeric(solution[obj]):
            return f"non-numeric value for {obj!r}: {solution[obj]!r}"
    return None


# =============================================================================
# DominanceVerdict
# =============================================================================

@dataclass
class DominanceVerdict:
    """
    Structured result of a dominance comparison.

    `dominates` mirrors the legacy boolean. `strict_objectives` and
    `blocked_by` preserve the original (asymmetric) semantics; the newer
    `winner_objectives`, `difference_objectives`, `a_better_on`,
    `b_better_on`, `blocks_a`, `blocks_b` give a symmetric view.
    """
    dominates: bool
    verdict_kind: str

    # --- Per-objective attribution ---
    per_objective: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # --- Legacy (asymmetric) attribution ---
    strict_objectives: List[str] = field(default_factory=list)
    blocked_by: List[str] = field(default_factory=list)

    # --- New, symmetric attribution ---
    winner_objectives: List[str] = field(default_factory=list)
    difference_objectives: List[str] = field(default_factory=list)
    a_better_on: List[str] = field(default_factory=list)
    b_better_on: List[str] = field(default_factory=list)
    tied_objectives: List[str] = field(default_factory=list)
    blocks_a: List[str] = field(default_factory=list)
    blocks_b: List[str] = field(default_factory=list)

    # --- XAI ---
    headline: str = ""
    rationale: List[str] = field(default_factory=list)

    # --- Context ---
    tolerance: float = 0.0
    tolerance_mode: str = "absolute"
    precision: Optional[str] = None
    minimize: List[str] = field(default_factory=list)
    maximize: List[str] = field(default_factory=list)
    min_margin_ratio: float = 0.0

    # --- HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Invalid ---
    invalid_reason: Optional[str] = None

    # --- Provenance ---
    trace_id: Optional[str] = None
    run_id: Optional[str] = None
    solution_id_a: Optional[str] = None
    solution_id_b: Optional[str] = None
    agent_id: Optional[str] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """
        Convert to dictionary.

        `safe=True` sanitises `per_objective` values and NaN/Inf diffs so
        the output is guaranteed JSON-serialisable.
        """
        out = asdict(self)
        out["at"] = self.at.isoformat()

        if safe:
            for k, info in out.get("per_objective", {}).items():
                if not isinstance(info, dict):
                    continue
                for side in ("a", "b"):
                    if side in info and not _is_json_primitive(info[side]):
                        info[side] = repr(info[side])
                diff = info.get("diff")
                if isinstance(diff, float) and not math.isfinite(diff):
                    info["diff"] = None
                margin = info.get("margin_ratio")
                if isinstance(margin, float) and not math.isfinite(margin):
                    info["margin_ratio"] = None

            if isinstance(out.get("min_margin_ratio"), float) and \
                    not math.isfinite(out["min_margin_ratio"]):
                out["min_margin_ratio"] = None
        return out


def _is_json_primitive(x: Any) -> bool:
    if x is None or isinstance(x, (bool, int, str)):
        return True
    if isinstance(x, float):
        return math.isfinite(x)
    return False


# =============================================================================
# XAI explainer
# =============================================================================

class DominanceExplainer:
    """Produce a headline + rationale for a DominanceVerdict."""

    @staticmethod
    def explain(verdict: DominanceVerdict) -> Tuple[str, List[str]]:
        reasons: List[str] = []
        kind = verdict.verdict_kind

        if kind == VerdictKind.INVALID.value:
            reason = verdict.invalid_reason or "unknown reason"
            reasons.append(f"Comparison invalid: {reason}.")
            return f"[INVALID] {reason}", reasons

        if kind == VerdictKind.A_DOMINATES_B.value:
            reasons.append(
                f"A dominates B on {len(verdict.winner_objectives)} "
                f"objective(s): {', '.join(verdict.winner_objectives) or 'none'}."
            )
            reasons.extend(_witness_lines(
                verdict, "A", verdict.a_better_on, "b",
            ))
            return "[A dominates B]", reasons

        if kind == VerdictKind.B_DOMINATES_A.value:
            reasons.append(
                f"B dominates A on {len(verdict.winner_objectives)} "
                f"objective(s): {', '.join(verdict.winner_objectives) or 'none'}."
            )
            reasons.extend(_witness_lines(
                verdict, "B", verdict.b_better_on, "a",
            ))
            return "[B dominates A]", reasons

        if kind == VerdictKind.EQUAL.value:
            if verdict.tolerance > 0:
                reasons.append(
                    f"A and B are equal on all objectives "
                    f"(within {verdict.tolerance_mode} tolerance "
                    f"{verdict.tolerance})."
                )
            else:
                reasons.append("A and B are equal on all objectives.")
            return "[Equal]", reasons

        # INCOMPARABLE — include witness values on both sides.
        reasons.append(
            "Neither solution dominates: each is strictly better on at "
            "least one objective and strictly worse on another."
        )
        reasons.extend(_witness_lines(verdict, "A", verdict.a_better_on, "b"))
        reasons.extend(_witness_lines(verdict, "B", verdict.b_better_on, "a"))
        if verdict.blocked_by:
            reasons.append(
                f"A blocked on: {', '.join(verdict.blocked_by)}."
            )
        return "[Incomparable]", reasons


def _witness_lines(
    verdict: DominanceVerdict,
    label: str,
    objective_keys: List[str],
    worse_side: str,
) -> List[str]:
    lines: List[str] = []
    for key in objective_keys[:3]:  # cap rationale length
        info = verdict.per_objective.get(key, {})
        better_val = info.get("a" if label == "A" else "b")
        worse_val = info.get(worse_side)
        lines.append(
            f"{label} is better on '{key}' "
            f"({better_val} vs {worse_val})."
        )
    return lines


# =============================================================================
# The scoped, thread-safe checker
# =============================================================================

class DominanceChecker:
    """
    Scoped dominance checker. Owns its statistics, thread-safe.

    A module-level default instance is used by the legacy module-level
    `dominance_verdict` / `dominates` / `filter_dominated` shims.
    """

    # Relative-margin thresholds for HITL review.
    NEAR_TIE_INCOMPARABLE = 0.01
    NEAR_DOMINATION = 0.001

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stats: Counter = Counter()
        self._invalid_reasons: Counter = Counter()
        self._per_agent: Dict[str, Counter] = defaultdict(Counter)
        self._per_precision: Dict[str, Counter] = defaultdict(Counter)
        self._margin_sum = 0.0
        self._margin_count = 0
        self._hitl_callback: Optional[
            Callable[[DominanceVerdict], Optional[bool]]
        ] = None

    # -- configuration ----------------------------------------------------

    def set_hitl_callback(
        self, callback: Callable[[DominanceVerdict], Optional[bool]],
    ) -> None:
        """
        Register a reviewer invoked whenever a verdict has
        `needs_review=True`. The callback receives the returned verdict and
        may mutate it. Returning False annotates the verdict as escalated.
        Exceptions are logged and swallowed.
        """
        self._hitl_callback = callback

    # -- core comparison --------------------------------------------------

    def check(
        self,
        a: Dict[str, Any],
        b: Dict[str, Any],
        minimize: Set[str],
        maximize: Set[str],
        *,
        tolerance: Optional[float] = None,
        precision: Optional[str] = None,
        strict_equality: bool = False,
        tolerance_mode: str = "absolute",
        context: Optional[Dict[str, Any]] = None,
        track: bool = True,
    ) -> DominanceVerdict:
        """
        Compare `a` and `b` and return a structured DominanceVerdict.

        Parameters
        ----------
        tolerance
            Absolute (or relative, per `tolerance_mode`) equality band.
            `None` (default) falls back to `tolerance_for_precision(precision)`.
            `0.0` disables tolerance, even if `precision` is set.
        precision
            Optional precision label used only when `tolerance is None`.
        strict_equality
            When True, only bit-exact equality counts — `tolerance` is
            ignored and `tolerance_mode` is not consulted.
        tolerance_mode
            "absolute" (default) or "relative" (scale-aware).
        context
            Optional provenance dict: trace_id, run_id, task_id,
            solution_id_a, solution_id_b, agent_id.
        track
            Update the checker's statistics. `False` for internal batched
            comparisons inside `filter_dominated` / `pareto_layers`.
        """
        ctx = _extract_context(context)

        # --- Tolerance validation -------------------------------------
        if tolerance is not None:
            if not _is_numeric(tolerance) or tolerance < 0:
                return self._invalid(
                    f"tolerance must be a non-negative finite number, "
                    f"got {tolerance!r}",
                    minimize, maximize, 0.0, "absolute",
                    precision if isinstance(precision, str) else None,
                    ctx, track,
                )

        # --- Precision validation -------------------------------------
        if precision is not None and not isinstance(precision, str):
            return self._invalid(
                f"precision must be a string or None, "
                f"got {type(precision).__name__}",
                minimize, maximize,
                tolerance if isinstance(tolerance, (int, float)) else 0.0,
                tolerance_mode if tolerance_mode in ("absolute", "relative")
                else "absolute",
                None, ctx, track,
            )

        # --- tolerance_mode validation --------------------------------
        if tolerance_mode not in ("absolute", "relative"):
            return self._invalid(
                f"tolerance_mode must be 'absolute' or 'relative', "
                f"got {tolerance_mode!r}",
                minimize, maximize,
                tolerance if isinstance(tolerance, (int, float)) else 0.0,
                "absolute",
                precision if isinstance(precision, str) else None,
                ctx, track,
            )

        # --- Objective validation -------------------------------------
        err = _validate_objectives(minimize, maximize)
        if err is not None:
            return self._invalid(
                err, minimize, maximize,
                tolerance if isinstance(tolerance, (int, float)) else 0.0,
                tolerance_mode,
                precision if isinstance(precision, str) else None,
                ctx, track,
            )

        min_set = set(minimize)
        max_set = set(maximize)

        # --- Solution validation --------------------------------------
        if not isinstance(a, dict) or not isinstance(b, dict):
            return self._invalid(
                "a and b must both be dicts",
                min_set, max_set,
                tolerance if isinstance(tolerance, (int, float)) else 0.0,
                tolerance_mode,
                precision if isinstance(precision, str) else None,
                ctx, track,
            )

        # --- Resolve tolerance ----------------------------------------
        if tolerance is None:
            tolerance = tolerance_for_precision(precision)

        # --- Per-objective comparison ---------------------------------
        per_objective: Dict[str, Dict[str, Any]] = {}
        a_better_on: List[str] = []
        b_better_on: List[str] = []
        tied: List[str] = []
        invalid_keys: List[str] = []
        decisive_margins: List[float] = []

        all_objectives: List[Tuple[str, ObjectiveDirection]] = (
            [(k, ObjectiveDirection.MINIMIZE) for k in min_set]
            + [(k, ObjectiveDirection.MAXIMIZE) for k in max_set]
        )

        for key, direction in all_objectives:
            if key not in a or key not in b:
                invalid_keys.append(key)
                per_objective[key] = {
                    "direction": direction.value,
                    "status": "missing",
                    "a": _json_safe(a.get(key)),
                    "b": _json_safe(b.get(key)),
                }
                continue
            av, bv = a[key], b[key]
            if not _is_numeric(av) or not _is_numeric(bv):
                invalid_keys.append(key)
                per_objective[key] = {
                    "direction": direction.value,
                    "status": "non_numeric",
                    "a": _json_safe(av),
                    "b": _json_safe(bv),
                }
                continue

            av_f, bv_f = float(av), float(bv)
            diff = av_f - bv_f
            scale = max(abs(av_f), abs(bv_f), 1e-9)
            margin_ratio = (
                abs(diff) / scale if math.isfinite(diff) else float("inf")
            )

            # --- equality check ---
            if strict_equality:
                within = (diff == 0.0)
            elif tolerance_mode == "relative":
                within = abs(diff) <= tolerance * scale
            else:  # absolute
                within = abs(diff) <= tolerance

            if within:
                status = "equal"
                tied.append(key)
            elif direction is ObjectiveDirection.MINIMIZE:
                if av_f < bv_f:
                    status = "a_better"
                    a_better_on.append(key)
                else:
                    status = "b_better"
                    b_better_on.append(key)
            else:  # MAXIMIZE
                if av_f > bv_f:
                    status = "a_better"
                    a_better_on.append(key)
                else:
                    status = "b_better"
                    b_better_on.append(key)

            per_objective[key] = {
                "direction": direction.value,
                "status": status,
                "a": av_f,
                "b": bv_f,
                "diff": diff if math.isfinite(diff) else None,
                "margin_ratio": (
                    margin_ratio if math.isfinite(margin_ratio) else None
                ),
                "within_tolerance": within,
            }
            if not within:
                decisive_margins.append(
                    margin_ratio if math.isfinite(margin_ratio)
                    else float("inf")
                )

        if invalid_keys:
            return self._invalid(
                f"invalid values for objective(s): {sorted(invalid_keys)} "
                "(missing, non-numeric, NaN, or infinite)",
                min_set, max_set, tolerance, tolerance_mode,
                precision, ctx, track,
            )

        # --- Verdict determination ------------------------------------
        min_margin_ratio = (
            min(decisive_margins) if decisive_margins else 0.0
        )

        if a_better_on and not b_better_on:
            kind = VerdictKind.A_DOMINATES_B
            dominates = True
            winner_obj = list(a_better_on)
            strict_obj = list(a_better_on)
            blocked: List[str] = []
            blocks_a: List[str] = []
            blocks_b: List[str] = []
        elif b_better_on and not a_better_on:
            kind = VerdictKind.B_DOMINATES_A
            dominates = False
            winner_obj = list(b_better_on)
            strict_obj = list(b_better_on)
            blocked = []
            blocks_a = []
            blocks_b = []
        elif not a_better_on and not b_better_on:
            kind = VerdictKind.EQUAL
            dominates = False
            winner_obj = []
            strict_obj = []
            blocked = []
            blocks_a = []
            blocks_b = []
        else:
            kind = VerdictKind.INCOMPARABLE
            dominates = False
            winner_obj = []
            # Preserve original union semantics for `strict_objectives`.
            strict_obj = list(a_better_on) + list(b_better_on)
            blocked = list(b_better_on)  # what stops A dominating
            blocks_a = list(b_better_on)
            blocks_b = list(a_better_on)

        # --- HITL flagging (single source of truth) -------------------
        needs_review = False
        review_reason: Optional[str] = None
        if kind is VerdictKind.INCOMPARABLE and \
                min_margin_ratio < self.NEAR_TIE_INCOMPARABLE:
            needs_review = True
            review_reason = (
                f"incomparable near-tie "
                f"(min relative margin {min_margin_ratio:.4f})"
            )
        elif kind in (VerdictKind.A_DOMINATES_B, VerdictKind.B_DOMINATES_A) \
                and min_margin_ratio < self.NEAR_DOMINATION:
            needs_review = True
            review_reason = (
                f"narrow domination "
                f"(min relative margin {min_margin_ratio:.4f})"
            )

        verdict = DominanceVerdict(
            dominates=dominates,
            verdict_kind=kind.value,
            per_objective=per_objective,
            strict_objectives=strict_obj,
            blocked_by=blocked,
            winner_objectives=winner_obj,
            difference_objectives=list(a_better_on) + list(b_better_on),
            a_better_on=list(a_better_on),
            b_better_on=list(b_better_on),
            tied_objectives=list(tied),
            blocks_a=blocks_a,
            blocks_b=blocks_b,
            tolerance=float(tolerance),
            tolerance_mode=tolerance_mode,
            precision=precision,
            minimize=sorted(min_set),
            maximize=sorted(max_set),
            min_margin_ratio=min_margin_ratio,
            needs_review=needs_review,
            review_reason=review_reason,
            trace_id=ctx.get("trace_id"),
            run_id=ctx.get("run_id"),
            solution_id_a=ctx.get("solution_id_a"),
            solution_id_b=ctx.get("solution_id_b"),
            agent_id=ctx.get("agent_id"),
        )
        verdict.headline, verdict.rationale = DominanceExplainer.explain(verdict)

        # --- Statistics -----------------------------------------------
        if track:
            self._record(kind.value, verdict)

        # --- HITL callback (in-place, matches sibling analyzer) -------
        if verdict.needs_review and self._hitl_callback is not None:
            try:
                approved = self._hitl_callback(verdict)
                if approved is False:
                    verdict.rationale.append(
                        "HITL reviewer returned False — "
                        "verdict marked for escalation."
                    )
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)

        logger.info(
            "dominance.verdict",
            extra={
                "verdict_kind": kind.value,
                "tolerance": verdict.tolerance,
                "tolerance_mode": tolerance_mode,
                "precision": precision,
                "min_margin_ratio": min_margin_ratio,
                "needs_review": verdict.needs_review,
                **ctx,
            },
        )
        return verdict

    # -- statistics -------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            total = sum(self._stats.values())
            mean_margin = (
                self._margin_sum / self._margin_count
                if self._margin_count else 0.0
            )
            return {
                "analyzer_version": __version__,
                "total_checks": total,
                "by_verdict": dict(self._stats),
                "a_dominates_rate": (
                    self._stats[VerdictKind.A_DOMINATES_B.value] / total
                    if total else 0.0
                ),
                "incomparable_rate": (
                    self._stats[VerdictKind.INCOMPARABLE.value] / total
                    if total else 0.0
                ),
                "invalid_rate": (
                    self._stats[VerdictKind.INVALID.value] / total
                    if total else 0.0
                ),
                "by_invalid_reason": dict(self._invalid_reasons),
                "by_agent": {a: dict(c) for a, c in self._per_agent.items()},
                "by_precision": {
                    p: dict(c) for p, c in self._per_precision.items()
                },
                "mean_min_margin_ratio": mean_margin,
            }

    def reset(self) -> None:
        with self._lock:
            self._stats.clear()
            self._invalid_reasons.clear()
            self._per_agent.clear()
            self._per_precision.clear()
            self._margin_sum = 0.0
            self._margin_count = 0

    # -- internals --------------------------------------------------------

    def _invalid(
        self,
        reason: str,
        minimize: Any,
        maximize: Any,
        tolerance: float,
        tolerance_mode: str,
        precision: Optional[str],
        ctx: Dict[str, Any],
        track: bool,
    ) -> DominanceVerdict:
        """
        Build a fully-populated INVALID verdict. Same shape for every
        invalid path (objectives, solutions, values, configuration).
        """
        # Defensively coerce legacy inputs.
        safe_min = (
            sorted(x for x in (minimize or []) if isinstance(x, str))
            if isinstance(minimize, (list, tuple, set, frozenset))
            else []
        )
        safe_max = (
            sorted(x for x in (maximize or []) if isinstance(x, str))
            if isinstance(maximize, (list, tuple, set, frozenset))
            else []
        )
        safe_tol = (
            float(tolerance)
            if isinstance(tolerance, (int, float))
            and math.isfinite(tolerance) and tolerance >= 0
            else 0.0
        )
        safe_mode = (
            tolerance_mode
            if tolerance_mode in ("absolute", "relative")
            else "absolute"
        )
        safe_precision = precision if isinstance(precision, str) else None

        if track:
            with self._lock:
                self._stats[VerdictKind.INVALID.value] += 1
                self._invalid_reasons[reason[:120]] += 1
                agent = ctx.get("agent_id")
                if isinstance(agent, str):
                    self._per_agent[agent][VerdictKind.INVALID.value] += 1
                if safe_precision:
                    self._per_precision[safe_precision][
                        VerdictKind.INVALID.value
                    ] += 1

        verdict = DominanceVerdict(
            dominates=False,
            verdict_kind=VerdictKind.INVALID.value,
            invalid_reason=reason,
            headline=f"[INVALID] {reason}",
            rationale=[f"Comparison invalid: {reason}."],
            needs_review=True,
            review_reason="malformed input",
            tolerance=safe_tol,
            tolerance_mode=safe_mode,
            precision=safe_precision,
            minimize=safe_min,
            maximize=safe_max,
            trace_id=ctx.get("trace_id"),
            run_id=ctx.get("run_id"),
            solution_id_a=ctx.get("solution_id_a"),
            solution_id_b=ctx.get("solution_id_b"),
            agent_id=ctx.get("agent_id"),
        )
        logger.warning("dominance.verdict invalid: %s", reason)
        return verdict

    def _record(self, kind_value: str, verdict: DominanceVerdict) -> None:
        with self._lock:
            self._stats[kind_value] += 1
            if isinstance(verdict.agent_id, str):
                self._per_agent[verdict.agent_id][kind_value] += 1
            if verdict.precision:
                self._per_precision[verdict.precision][kind_value] += 1
            if (
                kind_value != VerdictKind.EQUAL.value
                and verdict.min_margin_ratio is not None
            ):
                self._margin_sum += verdict.min_margin_ratio
                self._margin_count += 1


# =============================================================================
# Default checker + module-level backward-compatible API
# =============================================================================

_DEFAULT_CHECKER = DominanceChecker()


def dominance_verdict(
    a: Dict[str, Any],
    b: Dict[str, Any],
    minimize: Set[str],
    maximize: Set[str],
    *,
    tolerance: Optional[float] = None,
    precision: Optional[str] = None,
    strict_equality: bool = False,
    tolerance_mode: str = "absolute",
    context: Optional[Dict[str, Any]] = None,
) -> DominanceVerdict:
    """
    Module-level dominance verdict (uses the default checker).

    Backward-compatible with the original signature — the new `tolerance`,
    `precision`, `strict_equality`, `tolerance_mode`, `context` keywords are
    additive. Passing `tolerance=0.0` is now STRICT even when `precision`
    is set (in v1.x this silently widened to the preset).
    """
    return _DEFAULT_CHECKER.check(
        a, b, minimize, maximize,
        tolerance=tolerance,
        precision=precision,
        strict_equality=strict_equality,
        tolerance_mode=tolerance_mode,
        context=context,
    )


def dominates(
    a: Dict,
    b: Dict,
    minimize: Set[str],
    maximize: Set[str],
) -> bool:
    """
    True iff solution `a` Pareto-dominates solution `b` (strict, no tolerance).

    Backward-compatible: same signature, same semantics. Invalid inputs
    return False (as in v1.x) but are now logged at WARNING level so
    malformed comparisons are visible in telemetry.
    """
    v = _DEFAULT_CHECKER.check(
        a, b, minimize, maximize, tolerance=0.0, strict_equality=True,
    )
    if v.verdict_kind == VerdictKind.INVALID.value:
        logger.warning(
            "dominates() received invalid input (%s); returning False",
            v.invalid_reason,
        )
    return v.dominates


def get_statistics() -> Dict[str, Any]:
    """Cumulative dominance-check statistics for the default checker."""
    return _DEFAULT_CHECKER.get_statistics()


def reset_statistics() -> None:
    """Clear the module-level statistics (useful for tests)."""
    _DEFAULT_CHECKER.reset()


# =============================================================================
# filter_dominated
# =============================================================================

def filter_dominated(
    solutions: List[Dict],
    minimize: Set[str],
    maximize: Set[str],
    *,
    tolerance: Optional[float] = None,
    precision: Optional[str] = None,
    on_invalid: str = "skip",
) -> List[Dict]:
    """
    Return the subset of solutions that are not dominated by any other.

    `on_invalid`:
      - "skip"  (default) — malformed solutions are excluded from the frontier
      - "raise" — raise ValueError on the first malformed solution
      - "include" — include malformed solutions (v1.x behaviour; not recommended)

    Does not update the public dominance-check statistics.
    """
    if not solutions:
        return []

    err = _validate_objectives(minimize, maximize)
    if err is not None:
        if on_invalid == "raise":
            raise ValueError(err)
        logger.warning("filter_dominated: invalid objectives (%s)", err)
        return []

    if on_invalid not in ("skip", "raise", "include"):
        raise ValueError(
            f"on_invalid must be 'skip'|'raise'|'include', got {on_invalid!r}"
        )

    objectives = list(minimize) + list(maximize)
    valid_indices: List[int] = []
    for i, s in enumerate(solutions):
        e = _validate_solution(s, objectives)
        if e is None:
            valid_indices.append(i)
        elif on_invalid == "raise":
            raise ValueError(f"Solution {i} invalid: {e}")
        # else: skip (handled below by only iterating valid_indices)

    nondominated_indices: List[int] = []
    for i in valid_indices:
        dominated = False
        for j in valid_indices:
            if i == j:
                continue
            v = _DEFAULT_CHECKER.check(
                solutions[j], solutions[i], minimize, maximize,
                tolerance=tolerance, precision=precision, track=False,
            )
            if v.verdict_kind == VerdictKind.A_DOMINATES_B.value:
                dominated = True
                break
        if not dominated:
            nondominated_indices.append(i)

    if on_invalid == "include":
        nondominated_indices.extend(
            i for i in range(len(solutions)) if i not in valid_indices
        )
        nondominated_indices.sort()

    return [solutions[i] for i in nondominated_indices]


# =============================================================================
# Pareto frontier — layered non-dominated sorting
# =============================================================================

def pareto_layers(
    solutions: List[Dict],
    minimize: Set[str],
    maximize: Set[str],
    *,
    tolerance: Optional[float] = None,
    precision: Optional[str] = None,
    on_invalid: str = "skip",
) -> Dict[str, Any]:
    """
    Compute non-dominated layers (Deb et al., O(n²)).

    Returns:
        ranks:           per-solution layer index (None for invalid solutions)
        layers:          list of layers; each layer is a list of original indices
        dominated_by:    per-solution list of indices that dominate it
        invalid_indices: indices of solutions that failed validation
    """
    if not solutions:
        return {
            "ranks": [], "layers": [],
            "dominated_by": [], "invalid_indices": [],
        }

    err = _validate_objectives(minimize, maximize)
    if err is not None:
        raise ValueError(err)

    if on_invalid not in ("skip", "raise"):
        raise ValueError(
            f"on_invalid must be 'skip'|'raise', got {on_invalid!r}"
        )

    objectives = list(minimize) + list(maximize)
    valid_indices: List[int] = []
    invalid_indices: List[int] = []
    for i, s in enumerate(solutions):
        e = _validate_solution(s, objectives)
        if e is None:
            valid_indices.append(i)
        elif on_invalid == "raise":
            raise ValueError(f"Solution {i} invalid: {e}")
        else:
            invalid_indices.append(i)

    n = len(valid_indices)
    # dominates_matrix[i][j] = True iff valid_indices[i] dominates valid_indices[j]
    dominates_matrix = [[False] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            v = _DEFAULT_CHECKER.check(
                solutions[valid_indices[i]], solutions[valid_indices[j]],
                minimize, maximize,
                tolerance=tolerance, precision=precision, track=False,
            )
            dominates_matrix[i][j] = (
                v.verdict_kind == VerdictKind.A_DOMINATES_B.value
            )

    # Deb et al. fast non-dominated sort
    dominated_count = [0] * n
    dominated_list: List[List[int]] = [[] for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j and dominates_matrix[i][j]:
                dominated_list[i].append(j)
                dominated_count[j] += 1

    ranks_local: List[Optional[int]] = [None] * n
    layers_local: List[List[int]] = []
    current_layer = [i for i in range(n) if dominated_count[i] == 0]
    current_rank = 0
    while current_layer:
        layers_local.append(current_layer)
        next_layer: List[int] = []
        for i in current_layer:
            ranks_local[i] = current_rank
            for j in dominated_list[i]:
                dominated_count[j] -= 1
                if dominated_count[j] == 0:
                    next_layer.append(j)
        current_layer = next_layer
        current_rank += 1

    # Map back to original indices
    ranks_full: List[Optional[int]] = [None] * len(solutions)
    for local_i, orig_i in enumerate(valid_indices):
        ranks_full[orig_i] = ranks_local[local_i]

    dominated_by_full: List[List[int]] = [[] for _ in solutions]
    for local_j, orig_j in enumerate(valid_indices):
        dominated_by_full[orig_j] = [
            valid_indices[i] for i in range(n) if dominates_matrix[i][local_j]
        ]

    layers_full = [
        [valid_indices[local_i] for local_i in layer]
        for layer in layers_local
    ]

    return {
        "ranks": ranks_full,
        "layers": layers_full,
        "dominated_by": dominated_by_full,
        "invalid_indices": invalid_indices,
    }


class ParetoFrontier:
    """
    Layered Pareto frontier with per-solution ranks and dominators.

    `frontier` is the first (non-dominated) layer; `layers` is the full
    sorted hierarchy; `ranks[i]` is the layer index of solution i.
    """

    def __init__(
        self,
        solutions: List[Dict],
        minimize: Set[str],
        maximize: Set[str],
        *,
        tolerance: Optional[float] = None,
        precision: Optional[str] = None,
        on_invalid: str = "skip",
    ) -> None:
        result = pareto_layers(
            solutions, minimize, maximize,
            tolerance=tolerance, precision=precision,
            on_invalid=on_invalid,
        )
        self._solutions: List[Dict] = list(solutions)
        self._ranks: List[Optional[int]] = result["ranks"]
        self._layers_idx: List[List[int]] = result["layers"]
        self._dominated_by_idx: List[List[int]] = result["dominated_by"]
        self._invalid_indices: List[int] = result["invalid_indices"]

    @property
    def frontier(self) -> List[Dict]:
        if not self._layers_idx:
            return []
        return [self._solutions[i] for i in self._layers_idx[0]]

    @property
    def layers(self) -> List[List[Dict]]:
        return [
            [self._solutions[i] for i in layer]
            for layer in self._layers_idx
        ]

    @property
    def ranks(self) -> List[Optional[int]]:
        return list(self._ranks)

    @property
    def invalid_indices(self) -> List[int]:
        return list(self._invalid_indices)

    def dominated_by(self, index: int) -> List[Dict]:
        return [self._solutions[i] for i in self._dominated_by_idx[index]]


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    # --- Original behavior ---
    print("=== Original behavior ===")
    a = {"energy": 5.0, "quality": 0.90}
    b = {"energy": 6.0, "quality": 0.85}
    print(f"  dominates(a, b) = "
          f"{dominates(a, b, minimize={'energy'}, maximize={'quality'})}")

    # --- Enhanced: structured verdict ---
    print("\n=== Enhanced verdict ===")
    v = dominance_verdict(a, b, minimize={"energy"}, maximize={"quality"})
    print(f"  dominates:     {v.dominates}")
    print(f"  verdict_kind:  {v.verdict_kind}")
    print(f"  headline:      {v.headline}")
    print(f"  min_margin:    {v.min_margin_ratio:.4f}")
    print(f"  needs_review:  {v.needs_review}")
    for r in v.rationale:
        print(f"    • {r}")

    # --- Incomparable with witness values ---
    print("\n=== Incomparable case (witness rich) ===")
    c = {"energy": 4.0, "quality": 0.80}
    d = {"energy": 6.0, "quality": 0.95}
    v2 = dominance_verdict(c, d, minimize={"energy"}, maximize={"quality"})
    print(f"  verdict_kind:  {v2.verdict_kind}")
    print(f"  a_better_on:   {v2.a_better_on}")
    print(f"  b_better_on:   {v2.b_better_on}")
    for r in v2.rationale:
        print(f"    • {r}")

    # --- strict_equality vs tolerance ---
    print("\n=== strict_equality (bit-exact vs tolerance) ===")
    a1 = {"energy": 0.462}
    a2 = {"energy": 0.4620000000000001}
    strict_v = dominance_verdict(
        a1, a2, {"energy"}, set(), tolerance=1e-6, strict_equality=True,
    )
    loose_v = dominance_verdict(
        a1, a2, {"energy"}, set(), tolerance=1e-6,
    )
    print(f"  strict_equality=True:  {strict_v.verdict_kind}")
    print(f"  tolerance=1e-6:        {loose_v.verdict_kind}")

    # --- relative tolerance ---
    print("\n=== Relative tolerance ===")
    x = {"energy": 1000.0}
    y = {"energy": 1000.5}
    print(f"  absolute 1.0:  "
          f"{dominance_verdict(x, y, {'energy'}, set(), tolerance=1.0).verdict_kind}")
    print(f"  relative 1e-3: "
          f"{dominance_verdict(x, y, {'energy'}, set(), tolerance=1e-3, tolerance_mode='relative').verdict_kind}")

    # --- Invalid cases ---
    print("\n=== Invalid cases ===")
    print(f"  empty objectives:  "
          f"{dominance_verdict(a, b, set(), set()).verdict_kind}")
    print(f"  negative tol:      "
          f"{dominance_verdict(a, b, {'energy'}, {'quality'}, tolerance=-1.0).verdict_kind}")
    print(f"  overlap:           "
          f"{dominance_verdict(a, b, {'energy'}, {'energy'}).verdict_kind}")
    print(f"  missing key:       "
          f"{dominance_verdict({'energy': 5.0}, {'quality': 0.9}, {'energy'}, {'quality'}).verdict_kind}")
    print(f"  NaN value:         "
          f"{dominance_verdict({'energy': float('nan')}, {'energy': 5.0}, {'energy'}, set()).verdict_kind}")
    print(f"  bad tolerance:     "
          f"{dominance_verdict(a, b, {'energy'}, {'quality'}, tolerance='lol').verdict_kind}")

    # --- filter_dominated with a NaN solution (previously contaminated the frontier) ---
    print("\n=== filter_dominated (skips invalid by default) ===")
    solutions = [
        {"name": "s1", "energy": 5.0, "quality": 0.90},
        {"name": "s2", "energy": 6.0, "quality": 0.85},  # dominated by s1
        {"name": "s3", "energy": 4.0, "quality": 0.80},  # incomparable with s1
        {"name": "s4", "energy": float("nan"), "quality": 0.99},
    ]
    frontier = filter_dominated(
        solutions, minimize={"energy"}, maximize={"quality"},
    )
    print(f"  frontier size: {len(frontier)} (NaN solution excluded)")
    for s in frontier:
        print(f"    {s['name']}: energy={s['energy']}, quality={s['quality']}")

    # --- Non-dominated sorting ---
    print("\n=== ParetoFrontier (layered) ===")
    pf = ParetoFrontier(
        solutions[:3], minimize={"energy"}, maximize={"quality"},
    )
    print(f"  ranks: {pf.ranks}")
    for i, layer in enumerate(pf.layers):
        print(f"  layer {i}: {[s['name'] for s in layer]}")

    # --- HITL callback ---
    print("\n=== HITL callback (near-tie incomparable) ===")
    hitl_checker = DominanceChecker()
    hitl_checker.set_hitl_callback(
        lambda vd: vd.rationale.append("reviewed by ops") or True
    )
    near = hitl_checker.check(
        {"energy": 5.0, "quality": 0.900},
        {"energy": 5.005, "quality": 0.8995},
        {"energy"}, {"quality"},
    )
    print(f"  verdict:      {near.verdict_kind}")
    print(f"  needs_review: {near.needs_review}")
    print(f"  rationale:    {near.rationale}")

    # --- Statistics isolation ---
    print("\n=== Statistics isolation ===")
    isa = DominanceChecker()
    isb = DominanceChecker()
    isa.check(a, b, {"energy"}, {"quality"})
    isa.check(a, b, {"energy"}, {"quality"})
    isb.check(a, b, {"energy"}, {"quality"})
    print(f"  checker A total_checks: "
          f"{isa.get_statistics()['total_checks']}")
    print(f"  checker B total_checks: "
          f"{isb.get_statistics()['total_checks']}")
    print(f"  default total_checks:   "
          f"{get_statistics()['total_checks']}")

    # --- JSON-safe serialization ---
    print("\n=== JSON-safe serialisation ===")
    weird = dominance_verdict(
        {"energy": object()}, {"energy": 1.0}, {"energy"}, set(),
    )
    print(f"  verdict: {weird.verdict_kind}  "
          f"(raw a → {type(weird.per_objective.get('energy', {}).get('a')).__name__})")
    json.dumps(weird.to_dict(safe=True))  # must not raise
    print("  json.dumps(to_dict(safe=True)): OK")

    # --- Statistics ---
    print("\n=== Statistics (default checker) ===")
    print(json.dumps(get_statistics(), indent=2))
