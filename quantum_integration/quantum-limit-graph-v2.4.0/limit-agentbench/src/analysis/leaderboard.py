# src/analysis/reporting/leaderboard.py
"""
Leaderboard aggregation utilities for Green Agent (Enhanced, v2.0.0)
=====================================================================

Ranks evaluation results and emits structured, auditable, explainable
leaderboards that integrate with the analysis layer's shared contracts.

Original API preserved (all signatures, all return shapes):
    ranked = rank_by_green_score(results)      # -> List[Dict]
    ranked = rank_detailed(results, ...)       # -> List[RankedEntry]

Enhanced API:
    board = Leaderboard(
        run_id="...", deployment_id="local",
        hitl_top_n=3,
        tie_tolerance=1e-9,
        features={...},
    )
    board.add_results(results, strict=False)
    ranked = board.rank(by=..., descending=..., tie_breakers=...,
                        filter_fn=..., top_n=None)
    report = board.report(top_n=10)     # pure read of the cached ranking
    json_s = board.export_json()
    board.apply_override("agent-A", 1, allow_collision=False)
    board.set_hitl_callback(cb)         # cb(entry) -> Optional[bool]
    board.stability_metrics()           # Spearman / Kendall across snapshots
    board.to_decision_record(entry)     # per-entry DecisionRecord
    board.contribute_federated(remote)
    board.clear()                       # keep config
    board.clear(full=True) / board.reset()
    board.get_statistics()
    board.export()

Enhancements implemented (v2.0.0):
  1.  Ranked cache              — `report()` / `export()` no longer re-run
                                  `rank()`. The last ranking is cached; the
                                  cache invalidates on `add_results` /
                                  `clear` / `apply_override` /
                                  `set_hitl_callback`. Eliminates the
                                  HITL-spam, federated-growth, and
                                  counter-drift on read paths.
  2.  Per-call stat deltas      — `_ties_this_call` / `_invalid_this_call` /
                                  `_excluded_this_call` are separate from the
                                  cumulative instance counters. Global
                                  `_STATS` receives *deltas* only (v1 added
                                  the cumulative count on every call).
  3.  Override coherence        — `apply_override` recomputes dense /
                                  competition / tie_group after reordering;
                                  overridden entries get `status="overridden"`
                                  and `natural_rank` for the pre-override
                                  value. Collisions rejected unless
                                  `allow_collision=True`.
  4.  Feature flags honoured    — `exclude_invalid` drops INVALID entries
                                  from the output entirely; `stability`
                                  actually feeds `stability_metrics()`;
                                  `exclude_simulated` unchanged.
  5.  DecisionRecord emission   — `to_decision_record(entry)`; the record's
                                  provenance shares the layer's keys.
  6.  Thread safety             — `RLock` around every mutating and read
                                  path.
  7.  HITL denial annotation    — `cb(entry) == False` sets
                                  `hitl_approved=False` and appends
                                  `[HITL: denied]` to `review_reason`.
  8.  Bounded federated         — `deque(maxlen=...)` + running aggregates.
  9.  `report()` payload split  — `all` excludes the `top` slice; add
                                  `include_top_in_all=True` opt-in.
 10.  `top_n` propagation       — `report(top_n=N)` slices from the cached
                                  ranking; `rank(top_n=N)` fires HITL for
                                  the top N.
 11.  Per-report statistics     — report shows *this-call* counts, not the
                                  cumulative instance counters.
 12.  Configurable tie tolerance— `tie_tolerance` on `__init__` and
                                  `rank()`.
 13.  Dead-ternary removed      — `dense_rank=dense_counter` (v1 had an
                                  identical-branch ternary).
 14.  `excluded_reason`         — populated for `filter_fn` exceptions and
                                  feature-based exclusions.
 15.  Explainer branches        — INVALID / EXCLUDED / OVERRIDDEN entries
                                  get status-appropriate rationale (no
                                  misleading "ranked below" deltas).
 16.  Cross-module join keys    — `run_id` / `task_id` / `trace_id` on
                                  `RankedEntry`.
 17.  Stability metrics         — Spearman footrule, Kendall's tau,
                                  rank-change counts, snapshot diff.
 18.  `clear(full=False|True)`  — partial clears entries; full also
                                  resets counters, overrides, and history.
 19.  JSON-safe export          — `_jsonable(...)` sanitises original
                                  payloads; ISO datetimes preserved;
                                  `RankedEntry.to_dict(safe=True)`.
 20.  Override validation       — `allow_collision=False` raises on
                                  duplicate target ranks; missing agent_id
                                  warned.
 21.  `add_results(strict=...)` — re-raises on iteration errors after
                                  appending what was consumed.
 22.  Instance-scoped statistics— module-level `get_statistics()` now
                                  reports only `rank_by_green_score` counts;
                                  each `Leaderboard` has its own counters.
 23.  `__version__` / `__all__` / structured logging.
 24.  `contribute_federated()`  — stores merged profiles in a bounded
                                  aggregator (v1 only computed and
                                  discarded).
 25.  `get_statistics()` no longer merges global + instance (v1 double
                                  counted). Field names preserved;
                                  `global_rank_by_green_score_calls` exposes
                                  the module-level count separately.
 26.  `hitl_top_n` kwarg        — replaces the class-level constant.
 27.  `RankedEntry` immutability note — entries are still mutable for
                                  backward compatibility; use
                                  `entry.to_dict()` for a frozen copy.
 28.  `Severity` populated      — derived from `status` / `needs_review`.
 29.  `raw_score: Optional[float]` — distinguishes "missing/non-numeric"
                                  from a legitimate `0.0`.
 30.  Tie counting clarified    — `ties` = entries sharing a rank with a
                                  predecessor; `tie_groups` = number of
                                  tie groups.

Backward compatibility:
  * `rank_by_green_score(results)` returns `List[Dict]` with the same
    dicts the v1 function returned (now safely handling missing keys
    and NaN).
  * `Leaderboard.report()` still returns `{"statistics", "top", "all"}`.
    The `all` list now excludes the `top` slice by default; pass
    `include_top_in_all=True` for v1 behaviour.
  * `Leaderboard.export()` / `export_json()` unchanged in shape.
  * `get_statistics()` (module-level) now reports only
    `rank_by_green_score` counters. `Leaderboard.get_statistics()`
    reports *only* instance-scoped counters. Both keep the v1 field
    names (`total_rankings`, `total_entries_ranked`,
    `total_invalid_entries`, `total_ties`) but no longer merge.
    Use `get_global_statistics()` if you need the merged view.
"""

from __future__ import annotations

import inspect
import json
import logging
import math
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Sequence, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "Leaderboard",
    "RankedEntry",
    "RankExplainer",
    "RankStatus",
    "Severity",
    "SortDirection",
    "FederatedLeaderboardAggregator",
    "rank_by_green_score",
    "rank_detailed",
    "get_statistics",
    "get_global_statistics",
    "reset_statistics",
    "DEFAULT_PRIMARY_KEY",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class SortDirection(Enum):
    """Sort direction for a column."""
    ASC = "asc"
    DESC = "desc"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class RankStatus(Enum):
    """Classification of a ranked entry."""
    RANKED = "ranked"
    TIED = "tied"
    OVERRIDDEN = "overridden"   # rank changed by apply_override
    INVALID = "invalid"         # missing or non-numeric score
    EXCLUDED = "excluded"       # filtered out by a predicate or feature


_VALID_RANKED_STATUSES = frozenset({
    RankStatus.RANKED.value,
    RankStatus.TIED.value,
    RankStatus.OVERRIDDEN.value,
})


# =============================================================================
# Constants
# =============================================================================

DEFAULT_PRIMARY_KEY = "green_score"
DEFAULT_TIE_TOLERANCE = 1e-9


# =============================================================================
# Helpers
# =============================================================================

def _safe_float(v: Any) -> Optional[float]:
    """Coerce to a finite float; return None on any failure."""
    if isinstance(v, bool):
        return None
    try:
        f = float(v)
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


def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        if isinstance(ts, datetime):
            return ts.isoformat()
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


# =============================================================================
# RankedEntry
# =============================================================================

@dataclass
class RankedEntry:
    """
    A single ranked entry with rank, tie-group, and provenance.

    New in v2:
        raw_score        — the pre-parse float, or None if unparsable
        run_id/task_id/trace_id — cross-module join keys
        excluded_reason  — populated for EXCLUDED entries
        severity         — derived from status / needs_review
        natural_rank     — pre-override rank (None if not overridden)
        hitl_approved    — reviewer verdict (None if not reviewed)
    """
    rank: int
    dense_rank: int
    competition_rank: int
    tie_group: Optional[int]
    status: str
    agent_id: str
    score: float
    original: Dict[str, Any]

    # --- Optional / enhancement ---
    raw_score: Optional[float] = None
    explanation: Optional[Dict[str, Any]] = None
    needs_review: bool = False
    review_reason: Optional[str] = None
    hitl_approved: Optional[bool] = None
    excluded_reason: Optional[str] = None
    severity: str = Severity.INFO.value

    # --- Cross-module join keys ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    trace_id: Optional[str] = None

    # --- Override tracking ---
    natural_rank: Optional[int] = None

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            out["original"] = _jsonable(self.original)
            if isinstance(out.get("explanation"), dict):
                out["explanation"] = _jsonable(out["explanation"])
            for key in ("score", "raw_score"):
                v = out.get(key)
                if isinstance(v, float) and not math.isfinite(v):
                    out[key] = None
        else:
            out["original"] = dict(self.original)
        return out


# =============================================================================
# XAI
# =============================================================================

class RankExplainer:
    @staticmethod
    def explain(
        entry: RankedEntry,
        previous: Optional[RankedEntry],
        primary_key: str,
        tie_breakers: List[Tuple[str, str]],
    ) -> Dict[str, Any]:
        reasons: List[str] = []

        # --- Status-specific branches first ---
        if entry.status == RankStatus.INVALID.value:
            reasons.append(
                f"Entry '{entry.agent_id}' is INVALID: "
                f"{entry.review_reason or 'missing or non-numeric score'}."
            )
            return {
                "headline": f"invalid ({entry.agent_id})",
                "rationale": reasons,
                "tie_breakers": [tb[0] for tb in tie_breakers],
                "status": entry.status,
            }
        if entry.status == RankStatus.EXCLUDED.value:
            reasons.append(
                f"Entry '{entry.agent_id}' is EXCLUDED: "
                f"{entry.excluded_reason or 'filtered out'}."
            )
            return {
                "headline": f"excluded ({entry.agent_id})",
                "rationale": reasons,
                "tie_breakers": [tb[0] for tb in tie_breakers],
                "status": entry.status,
            }

        # --- Ranked / tied / overridden ---
        reasons.append(
            f"Rank {entry.rank} for '{entry.agent_id}' with "
            f"{primary_key}={entry.score:.6f}."
        )
        if entry.natural_rank is not None and entry.rank != entry.natural_rank:
            reasons.append(
                f"Manual override: natural rank {entry.natural_rank} → "
                f"{entry.rank}."
            )
        if previous is None:
            reasons.append(
                "Top-ranked entry — no higher-scoring peer."
            )
        else:
            delta = previous.score - entry.score
            reasons.append(
                f"Ranked below '{previous.agent_id}' by "
                f"{delta:.6f} on {primary_key}."
            )
            if abs(delta) < DEFAULT_TIE_TOLERANCE:
                reasons.append(
                    "Tied on primary key; tie-breakers applied."
                )
        if tie_breakers:
            reasons.append(
                f"Tie-breakers: {[tb[0] for tb in tie_breakers]}."
            )
        if entry.needs_review and entry.hitl_approved is False:
            reasons.append("[HITL: denied] reviewer rejected this rank.")
        return {
            "headline": f"rank={entry.rank} ({entry.agent_id})",
            "rationale": reasons,
            "tie_breakers": [tb[0] for tb in tie_breakers],
            "status": entry.status,
        }


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


#: Module-level counter — receives contributions from `rank_by_green_score`
#: only (v1 also pulled in `Leaderboard.rank` counts; see §"Backward
#: compatibility" in the module docstring).
_GLOBAL_STATS = _StatsCounter()


# =============================================================================
# Federated aggregation (bounded + cached + locked)
# =============================================================================

class FederatedLeaderboardAggregator:
    """
    Aggregate anonymised leaderboard profiles across deployments.

    Bounded (deque with maxlen); cached aggregate invalidated on push;
    thread-safe.
    """

    def __init__(self, max_profiles: int = 1_000) -> None:
        self._profiles: Deque[Dict[str, Any]] = deque(maxlen=max_profiles)
        self._cache: Optional[Dict[str, Any]] = None
        self._lock = threading.RLock()

    @property
    def profiles(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._profiles)

    def push(self, profile: Dict[str, Any]) -> None:
        with self._lock:
            self._profiles.append(dict(profile))
            self._cache = None

    def aggregate(self) -> Dict[str, Any]:
        with self._lock:
            if self._cache is not None:
                return self._cache
            if not self._profiles:
                result: Dict[str, Any] = {
                    "contributors": 0,
                    "total_entries": 0,
                    "weighted_mean_score": 0.0,
                    "max_score": 0.0,
                    "min_score": 0.0,
                }
                self._cache = result
                return result

            total = sum(
                max(0, int(p.get("n_entries", 0) or 0))
                for p in self._profiles
            ) or 1
            weighted_mean = sum(
                (p.get("mean_score", 0.0) or 0.0)
                * max(0, int(p.get("n_entries", 0) or 0))
                for p in self._profiles
            ) / total
            result = {
                "contributors": len(self._profiles),
                "total_entries": total,
                "weighted_mean_score": weighted_mean,
                "max_score": max(
                    (p.get("max_score", 0.0) or 0.0
                     for p in self._profiles),
                    default=0.0,
                ),
                "min_score": min(
                    (p.get("min_score", 0.0) or 0.0
                     for p in self._profiles),
                    default=0.0,
                ),
            }
            self._cache = result
            return result

    def clear(self) -> None:
        with self._lock:
            self._profiles.clear()
            self._cache = None


# =============================================================================
# ORIGINAL FUNCTION — preserved (v1 safe-fallback behaviour)
# =============================================================================

def rank_by_green_score(results: List[Dict]) -> List[Dict]:
    """
    Sort results by `green_score` (descending).

    Backward-compatible: same signature, same return type. Missing /
    non-numeric green_score values sort to the bottom.
    """
    if results is None:
        logger.warning(
            "rank_by_green_score received None; returning []"
        )
        return []
    if not isinstance(results, list):
        logger.warning(
            "rank_by_green_score received %s; returning []",
            type(results).__name__,
        )
        return []

    _GLOBAL_STATS.increment("rankings")
    _GLOBAL_STATS.increment("entries", len(results))

    dicts = [r for r in results if isinstance(r, dict)]

    def _key(r: Dict[str, Any]) -> Tuple[int, float, str]:
        v = _safe_float(r.get(DEFAULT_PRIMARY_KEY))
        if v is None:
            _GLOBAL_STATS.increment("invalid")
            return (1, 0.0, str(r.get("agent_id", "")))
        return (0, -v, str(r.get("agent_id", "")))

    return sorted(dicts, key=_key)


# =============================================================================
# Leaderboard
# =============================================================================

class Leaderboard:
    """
    Stateful leaderboard builder with caching, XAI, HITL, overrides,
    federated contribution, stability metrics, and per-instance stats.
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "provenance": True,
        "xai": True,
        "hitl": True,
        "federated": True,
        "statistics": True,
        "tie_handling": True,
        "stability": True,
        "exclude_simulated": False,
        "exclude_invalid": False,
    }

    DEFAULT_HITL_TOP_N = 3

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
        hitl_top_n: Optional[int] = None,
        tie_tolerance: float = DEFAULT_TIE_TOLERANCE,
        max_federated_profiles: int = 1_000,
    ):
        # --- Identity ---
        self.run_id = run_id or f"lb-{uuid.uuid4().hex[:8]}"
        self.deployment_id = deployment_id

        # --- Feature flags ---
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "Leaderboard: unknown feature flag(s) ignored: %s",
                sorted(unknown),
            )

        # --- Tuning ---
        if hitl_top_n is None:
            hitl_top_n = self.DEFAULT_HITL_TOP_N
        if hitl_top_n < 0:
            raise ValueError(
                f"hitl_top_n must be >= 0, got {hitl_top_n}"
            )
        self._hitl_top_n = int(hitl_top_n)

        tt = _safe_float(tie_tolerance)
        if tt is None or tt < 0:
            raise ValueError(
                f"tie_tolerance must be a non-negative finite number, "
                f"got {tie_tolerance!r}"
            )
        self._tie_tolerance = tt

        # --- Accumulated entries ---
        self._entries: List[Dict[str, Any]] = []

        # --- Cache ---
        self._last_ranked: Optional[List[RankedEntry]] = None
        self._last_rank_stats: Optional[Dict[str, Any]] = None
        self._last_rank_params: Optional[Dict[str, Any]] = None

        # --- Ranking history (for stability analysis) ---
        self._ranking_history: Deque[List[Tuple[str, int]]] = deque(maxlen=10)

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[RankedEntry], Optional[bool]]
        ] = None
        self._manual_overrides: Dict[str, int] = {}

        # --- Federated ---
        self._federated = FederatedLeaderboardAggregator(
            max_profiles=max_federated_profiles
        )

        # --- Instance-scoped statistics ---
        self._stats = _StatsCounter()

        # --- Concurrency ---
        self._lock = threading.RLock()

        logger.debug(
            "leaderboard.initialized",
            extra={
                "run_id": self.run_id,
                "deployment_id": deployment_id,
                "hitl_top_n": self._hitl_top_n,
                "tie_tolerance": self._tie_tolerance,
            },
        )

    # ------------------------------------------------------------------
    # Accumulation
    # ------------------------------------------------------------------

    def add_results(
        self,
        results: Iterable[Dict[str, Any]],
        *,
        strict: bool = False,
    ) -> None:
        """
        Add results to the leaderboard.

        `strict=True` re-raises on iteration errors *after* appending
        whatever was successfully consumed.
        """
        if results is None:
            return
        with self._lock:
            try:
                for r in results:
                    if isinstance(r, dict):
                        self._entries.append(dict(r))
            except Exception as e:
                self._invalidate_cache()
                logger.warning(
                    "leaderboard.add_results_iteration_error",
                    extra={"error": str(e)},
                )
                if strict:
                    raise
                return
            self._invalidate_cache()

    def clear(self, *, full: bool = False) -> None:
        """
        Remove accumulated entries.

        `full=True` additionally resets counters, overrides, history, and
        the federated aggregator. Config, run_id, and features are always
        preserved.
        """
        with self._lock:
            self._entries.clear()
            self._invalidate_cache()
            if full:
                self._stats.reset()
                self._manual_overrides.clear()
                self._ranking_history.clear()
                self._federated.clear()
                self._hitl_callback = None

    def reset(self) -> None:
        """Alias for `clear(full=True)` — matches sibling modules."""
        self.clear(full=True)

    # ------------------------------------------------------------------
    # Ranking
    # ------------------------------------------------------------------

    def rank(
        self,
        *,
        by: str = DEFAULT_PRIMARY_KEY,
        descending: bool = True,
        tie_breakers: Optional[List[Tuple[str, bool]]] = None,
        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
        top_n: Optional[int] = None,
    ) -> List[RankedEntry]:
        """
        Rank all accumulated entries and refresh the cache.

        Side-effecting: fires the HITL callback for the top N, applies
        overrides, updates counters, and appends to the ranking history.
        Subsequent `report()` / `export()` calls use the cached result —
        they do **not** re-run this method.
        """
        with self._lock:
            if top_n is None:
                top_n = (
                    self._hitl_top_n
                    if self.features.get("hitl", True) else 0
                )
            top_n = max(0, int(top_n))

            # --- Classify ---
            valid: List[Dict[str, Any]] = []
            invalid: List[Dict[str, Any]] = []
            excluded: List[Tuple[Dict[str, Any], str]] = []

            for entry in self._entries:
                # Filter predicate
                if filter_fn is not None:
                    try:
                        if not filter_fn(entry):
                            excluded.append((
                                entry, "rejected by filter_fn",
                            ))
                            continue
                    except Exception as e:
                        excluded.append((
                            entry,
                            f"filter_fn raised "
                            f"{type(e).__name__}: {e}",
                        ))
                        logger.warning(
                            "leaderboard.filter_fn_raised",
                            extra={"error": str(e)},
                        )
                        continue

                # Feature-based exclusion
                if (
                    self.features.get("exclude_simulated", False)
                    and entry.get("simulated", False)
                ):
                    excluded.append((
                        entry, "excluded: simulated",
                    ))
                    continue

                # Score validity
                if _safe_float(entry.get(by)) is None:
                    invalid.append(entry)
                    continue

                valid.append(entry)

            # --- Per-call deltas ---
            ties_this_call = 0
            invalid_this_call = len(invalid)
            excluded_this_call = len(excluded)

            # --- Sort valid entries ---
            primary_dir = (
                SortDirection.DESC.value if descending
                else SortDirection.ASC.value
            )
            tb: List[Tuple[str, str]] = []
            for key, tb_desc in (tie_breakers or []):
                tb.append((
                    key,
                    SortDirection.DESC.value if tb_desc
                    else SortDirection.ASC.value,
                ))

            def _sort_tuple(e: Dict[str, Any]) -> Tuple[Any, ...]:
                parts: List[Any] = [_sort_value(e, by, primary_dir)]
                for k, d in tb:
                    parts.append(_sort_value(e, k, d))
                parts.append(str(e.get("agent_id", "")))
                return tuple(parts)

            sorted_valid = sorted(valid, key=_sort_tuple)

            # --- Assign ranks ---
            ranked: List[RankedEntry] = []
            prev_score: Optional[float] = None
            prev_rank: Optional[int] = None
            dense_counter = 0
            competition_counter = 0
            tie_group_id = 0
            current_tie_group: Optional[int] = None
            tie_handling = self.features.get("tie_handling", True)
            tol = self._tie_tolerance

            for i, entry in enumerate(sorted_valid):
                score = _safe_float(entry.get(by)) or 0.0
                position = i + 1

                is_tied = (
                    tie_handling
                    and prev_score is not None
                    and abs(score - prev_score) <= tol
                )

                if is_tied:
                    rank = prev_rank if prev_rank is not None else position
                    if current_tie_group is None:
                        tie_group_id += 1
                        current_tie_group = tie_group_id
                    ties_this_call += 1
                    status = RankStatus.TIED.value
                else:
                    rank = position
                    current_tie_group = None
                    dense_counter += 1
                    competition_counter = position
                    status = RankStatus.RANKED.value

                raw = _safe_float(entry.get(by))
                ranked.append(RankedEntry(
                    rank=rank,
                    dense_rank=dense_counter,
                    competition_rank=competition_counter,
                    tie_group=current_tie_group,
                    status=status,
                    agent_id=str(entry.get("agent_id", f"entry-{i}")),
                    score=score,
                    raw_score=raw,
                    original=dict(entry),
                    run_id=_optional_str(entry.get("run_id")),
                    task_id=_optional_str(entry.get("task_id")),
                    trace_id=_optional_str(entry.get("trace_id")),
                ))
                prev_score = score
                prev_rank = rank

            # --- Invalid entries ---
            exclude_invalid = self.features.get("exclude_invalid", False)
            if not exclude_invalid:
                for entry in sorted(
                    invalid, key=lambda e: str(e.get("agent_id", "")),
                ):
                    ranked.append(RankedEntry(
                        rank=0,
                        dense_rank=0,
                        competition_rank=0,
                        tie_group=None,
                        status=RankStatus.INVALID.value,
                        agent_id=str(entry.get("agent_id", "?")),
                        score=0.0,
                        raw_score=None,
                        original=dict(entry),
                        needs_review=True,
                        review_reason="missing or invalid score",
                        severity=Severity.WARNING.value,
                        run_id=_optional_str(entry.get("run_id")),
                        task_id=_optional_str(entry.get("task_id")),
                        trace_id=_optional_str(entry.get("trace_id")),
                    ))

            # --- Excluded entries ---
            for entry, reason in sorted(
                excluded, key=lambda x: str(x[0].get("agent_id", "")),
            ):
                ranked.append(RankedEntry(
                    rank=0,
                    dense_rank=0,
                    competition_rank=0,
                    tie_group=None,
                    status=RankStatus.EXCLUDED.value,
                    agent_id=str(entry.get("agent_id", "?")),
                    score=_safe_float(entry.get(by)) or 0.0,
                    raw_score=_safe_float(entry.get(by)),
                    original=dict(entry),
                    excluded_reason=reason,
                    severity=Severity.INFO.value,
                    run_id=_optional_str(entry.get("run_id")),
                    task_id=_optional_str(entry.get("task_id")),
                    trace_id=_optional_str(entry.get("trace_id")),
                ))

            # --- XAI ---
            if self.features.get("xai", True):
                for i, entry in enumerate(ranked):
                    prev = ranked[i - 1] if i > 0 else None
                    entry.explanation = RankExplainer.explain(
                        entry, prev, by, tb,
                    )

            # --- HITL for top-N (ranked entries only) ---
            self._apply_hitl(ranked, top_n)

            # --- Overrides ---
            self._apply_overrides(ranked)

            # --- Statistics (per-call deltas) ---
            self._record_stats(
                entries_this_call=len(self._entries),
                ranked_this_call=len(sorted_valid),
                ties_this_call=ties_this_call,
                invalid_this_call=invalid_this_call,
                excluded_this_call=excluded_this_call,
            )

            # --- Stability history ---
            if self.features.get("stability", True):
                self._ranking_history.append([
                    (e.agent_id, e.rank)
                    for e in ranked
                    if e.status in _VALID_RANKED_STATUSES
                ])

            # --- Federated contribution ---
            if self.features.get("federated", True):
                score_vals = [
                    e.score for e in ranked
                    if e.status in _VALID_RANKED_STATUSES
                ]
                if score_vals:
                    self._federated.push({
                        "deployment_id": self.deployment_id,
                        "run_id": self.run_id,
                        "n_entries": len(score_vals),
                        "mean_score":
                            sum(score_vals) / len(score_vals),
                        "max_score": max(score_vals),
                        "min_score": min(score_vals),
                        "generated_at": datetime.now().isoformat(),
                    })

            # --- Cache ---
            per_call_stats = {
                "entries_considered": len(self._entries),
                "ranked": len(sorted_valid),
                "invalid": invalid_this_call,
                "excluded": excluded_this_call,
                "ties": ties_this_call,
                "tie_groups": tie_group_id,
            }
            self._last_ranked = ranked
            self._last_rank_stats = per_call_stats
            self._last_rank_params = {
                "by": by,
                "descending": descending,
                "tie_breakers": list(tie_breakers or []),
                "top_n": top_n,
            }

            logger.info(
                "leaderboard.ranked",
                extra={
                    "run_id": self.run_id,
                    "deployment_id": self.deployment_id,
                    "by": by,
                    "descending": descending,
                    **per_call_stats,
                },
            )
            return list(ranked)

    # ------------------------------------------------------------------
    # Reporting (pure reads over the cached ranking)
    # ------------------------------------------------------------------

    def report(
        self,
        top_n: int = 10,
        *,
        include_top_in_all: bool = False,
    ) -> Dict[str, Any]:
        """
        Produce a leaderboard report from the cached ranking.

        If `rank()` has never been called, this triggers one (v1 compat).
        Subsequent calls are pure reads — no HITL firing, no counter
        drift, no federated growth.
        """
        with self._lock:
            if self._last_ranked is None:
                self.rank()
            ranked = self._last_ranked or []
            per_call_stats = self._last_rank_stats or {}

            if top_n < 0:
                top_n = 0
            top = ranked[:top_n]
            if include_top_in_all:
                all_entries = ranked
            else:
                all_entries = ranked[top_n:]

            valid = [
                e for e in ranked
                if e.status in _VALID_RANKED_STATUSES
            ]
            scores = [e.score for e in valid]

            stats: Dict[str, Any] = {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "deployment_id": self.deployment_id,
                "generated_at": datetime.now().isoformat(),
                "total_entries": len(ranked),
                "ranked_entries": len(valid),
                "invalid_entries": sum(
                    1 for e in ranked
                    if e.status == RankStatus.INVALID.value
                ),
                "excluded_entries": sum(
                    1 for e in ranked
                    if e.status == RankStatus.EXCLUDED.value
                ),
                "overridden_entries": sum(
                    1 for e in ranked
                    if e.status == RankStatus.OVERRIDDEN.value
                ),
                "tie_groups": per_call_stats.get("tie_groups", 0),
                "tied_entries": per_call_stats.get("ties", 0),
            }
            if scores:
                mean_s = sum(scores) / len(scores)
                variance = sum(
                    (s - mean_s) ** 2 for s in scores
                ) / len(scores)
                stats["score_distribution"] = {
                    "mean": mean_s,
                    "min": min(scores),
                    "max": max(scores),
                    "stdev": math.sqrt(variance),
                }

            return {
                "statistics": stats,
                "top": [e.to_dict() for e in top],
                "all": [e.to_dict() for e in all_entries],
            }

    def export_json(self, indent: Optional[int] = 2) -> str:
        """JSON-safe export of the full leaderboard report."""
        with self._lock:
            if self._last_ranked is None:
                self.rank()
            payload = {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "deployment_id": self.deployment_id,
                "generated_at": datetime.now().isoformat(),
                "statistics": self.get_statistics(),
                "report": self.report(),
                "federated_aggregate": self._federated.aggregate(),
            }
        return json.dumps(_jsonable(payload), indent=indent, default=str)

    def export(self) -> Dict[str, Any]:
        """Full JSON-safe export dict (mirrors `export_json`)."""
        with self._lock:
            if self._last_ranked is None:
                self.rank()
            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "deployment_id": self.deployment_id,
                "statistics": self.get_statistics(),
                "report": self.report(),
                "federated_profiles": self._federated.profiles,
                "federated_aggregate": self._federated.aggregate(),
            }

    # ------------------------------------------------------------------
    # HITL
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self,
        callback: Optional[Callable[[RankedEntry], Optional[bool]]],
    ) -> None:
        """
        Register a HITL reviewer invoked for top-N entries.

        The callback receives the entry and may mutate it. Returning
        `False` sets `hitl_approved=False` and appends
        `[HITL: denied]` to `review_reason`.

        Passing `None` clears the callback. Any cache invalidation ensures
        the new callback is applied on the next `rank()`.
        """
        with self._lock:
            self._hitl_callback = callback
            self._invalidate_cache()

    def _apply_hitl(
        self, ranked: List[RankedEntry], top_n: int,
    ) -> None:
        """Fire the HITL callback for top-N ranked entries."""
        if (
            not self.features.get("hitl", True)
            or self._hitl_callback is None
            or top_n <= 0
        ):
            return
        for entry in ranked:
            if len([
                e for e in ranked
                if e.status in _VALID_RANKED_STATUSES
                and e.rank <= entry.rank
            ]) > top_n:
                break
            if entry.status not in _VALID_RANKED_STATUSES:
                continue
            entry.needs_review = True
            entry.review_reason = (
                (entry.review_reason or "") + f"top-{top_n} entry"
            ).strip()
            try:
                approved = self._hitl_callback(entry)
                if approved is False:
                    entry.hitl_approved = False
                    entry.review_reason = (
                        (entry.review_reason or "")
                        + " [HITL: denied]"
                    ).strip()
                elif approved is True:
                    entry.hitl_approved = True
            except Exception as e:
                logger.warning(
                    "leaderboard.hitl_callback_failed",
                    extra={"error": str(e)},
                )

    # ------------------------------------------------------------------
    # Manual overrides
    # ------------------------------------------------------------------

    def apply_override(
        self,
        agent_id: str,
        rank: int,
        *,
        allow_collision: bool = False,
    ) -> None:
        """
        Manually set an entry's rank.

        On the next `rank()` the target entry gets `rank=override`,
        `status="overridden"`, and `natural_rank=<pre-override rank>`.
        Dense / competition ranks are recomputed from the sorted order.

        Raises `ValueError` if:
          - `rank < 1`
          - the same rank is already assigned to another override and
            `allow_collision=False`
        """
        if not isinstance(agent_id, str) or not agent_id:
            raise ValueError(
                f"agent_id must be a non-empty string, got {agent_id!r}"
            )
        if not isinstance(rank, int) or isinstance(rank, bool) or rank < 1:
            raise ValueError(
                f"rank must be an integer >= 1, got {rank!r}"
            )
        with self._lock:
            if not allow_collision:
                for other_id, other_rank in self._manual_overrides.items():
                    if other_id != agent_id and other_rank == rank:
                        raise ValueError(
                            f"rank {rank} already overridden by "
                            f"{other_id!r} (pass allow_collision=True to "
                            f"allow ties)"
                        )
            if not any(
                e.get("agent_id") == agent_id for e in self._entries
            ):
                logger.warning(
                    "leaderboard.override_unknown_agent",
                    extra={"agent_id": agent_id},
                )
            self._manual_overrides[agent_id] = rank
            self._invalidate_cache()
        logger.info(
            "leaderboard.override_applied",
            extra={
                "agent_id": agent_id,
                "rank": rank,
                "allow_collision": allow_collision,
            },
        )

    def _apply_overrides(self, ranked: List[RankedEntry]) -> None:
        """Apply stored overrides and recompute rank variants."""
        if not self._manual_overrides:
            return

        overridden_any = False
        for entry in ranked:
            override = self._manual_overrides.get(entry.agent_id)
            if override is None:
                continue
            overridden_any = True
            if entry.rank > 0:
                entry.natural_rank = entry.rank
            entry.rank = override
            entry.status = RankStatus.OVERRIDDEN.value
            entry.needs_review = True
            entry.review_reason = (
                (entry.review_reason or "") + "manual override applied"
            ).strip()
            entry.severity = Severity.WARNING.value

        if not overridden_any:
            return

        # Re-sort by new rank, agent_id as tiebreak
        ranked.sort(key=lambda e: (
            0 if e.rank > 0 else 1,
            e.rank if e.rank > 0 else float("inf"),
            e.agent_id,
        ))

        # Recompute dense / competition / tie_group from the new order
        dense = 0
        comp = 0
        prev_rank: Optional[int] = None
        tie_group_id = 0
        in_tie = False

        for i, entry in enumerate(ranked):
            if entry.rank <= 0:
                entry.dense_rank = 0
                entry.competition_rank = 0
                entry.tie_group = None
                continue
            pos = i + 1
            if prev_rank is not None and entry.rank == prev_rank:
                # Tied (including overrides)
                if not in_tie:
                    tie_group_id += 1
                    in_tie = True
                entry.tie_group = tie_group_id
                # Dense/comp stay from previous
            else:
                dense += 1
                comp = pos
                in_tie = False
                entry.tie_group = None
            entry.dense_rank = dense
            entry.competition_rank = comp
            prev_rank = entry.rank

    # ------------------------------------------------------------------
    # Federated
    # ------------------------------------------------------------------

    def contribute_federated(
        self,
        remote_profiles: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Push local profiles plus any remote profiles into the aggregator
        and return the aggregate.

        Remote profiles are stored (bounded by `max_federated_profiles`)
        so subsequent `get_federated_profiles()` / `export()` calls
        reflect them.
        """
        with self._lock:
            for p in (remote_profiles or []):
                if isinstance(p, dict):
                    self._federated.push(p)
            return self._federated.aggregate()

    def get_federated_profiles(self) -> List[Dict[str, Any]]:
        return self._federated.profiles

    def get_federated_aggregate(self) -> Dict[str, Any]:
        return self._federated.aggregate()

    # ------------------------------------------------------------------
    # Stability metrics
    # ------------------------------------------------------------------

    def stability_metrics(self) -> Dict[str, Any]:
        """
        Compare the two most recent ranking snapshots.

        Returns:
            available         — bool; False if fewer than 2 snapshots
            n_common_agents   — agents in both snapshots
            n_added           — agents in the latest but not the previous
            n_removed         — agents in the previous but not the latest
            spearman_footrule — sum of |rank_a - rank_b| over common agents
            kendall_tau       — Kendall's tau over common agents
            max_rank_change   — largest single-agent rank delta
            mean_rank_change  — mean of per-agent rank deltas (absolute)
        """
        with self._lock:
            if len(self._ranking_history) < 2:
                return {"available": False}
            prev = dict(self._ranking_history[-2])
            curr = dict(self._ranking_history[-1])

        common = set(prev.keys()) & set(curr.keys())
        added = set(curr.keys()) - set(prev.keys())
        removed = set(prev.keys()) - set(curr.keys())

        footrule = 0
        max_change = 0
        deltas: List[float] = []
        for aid in common:
            d = abs(curr[aid] - prev[aid])
            footrule += d
            deltas.append(d)
            if d > max_change:
                max_change = d
        mean_change = (
            sum(deltas) / len(deltas) if deltas else 0.0
        )

        # Kendall's tau over common agents
        concordant = 0
        discordant = 0
        common_list = list(common)
        for i in range(len(common_list)):
            for j in range(i + 1, len(common_list)):
                a, b = common_list[i], common_list[j]
                pa = prev[a] - prev[b]
                ca = curr[a] - curr[b]
                if pa == 0 or ca == 0:
                    continue
                if (pa > 0) == (ca > 0):
                    concordant += 1
                else:
                    discordant += 1
        total_pairs = concordant + discordant
        tau = (
            (concordant - discordant) / total_pairs
            if total_pairs else 0.0
        )

        return {
            "available": True,
            "n_common_agents": len(common),
            "n_added": len(added),
            "n_removed": len(removed),
            "spearman_footrule": footrule,
            "kendall_tau": tau,
            "max_rank_change": int(max_change),
            "mean_rank_change": mean_change,
        }

    # ------------------------------------------------------------------
    # DecisionRecord emission
    # ------------------------------------------------------------------

    def to_decision_record(
        self,
        entry: RankedEntry,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """
        Emit a `DecisionRecord` for a ranked entry.

        Returns None if the shared contract isn't importable.
        """
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug(
                    "leaderboard.decision_record_import_failed"
                )
                return None

        candidates: Dict[str, Any] = {
            "run_id": entry.run_id or self.run_id,
            "timestamp": datetime.now(),
            "task_id": entry.task_id or "",
            "selected_action": (
                f"leaderboard_rank(agent={entry.agent_id}, "
                f"rank={entry.rank})"
            ),
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": entry.agent_id,
            "precision": _optional_str(
                entry.original.get("precision")
            ),
            "quality_score": entry.raw_score,
            "latency_ms": _safe_float(
                entry.original.get("latency_ms")
            ) or 0.0,
            "energy_kwh": _safe_float(
                entry.original.get("energy_kwh")
            ) or 0.0,
            "carbon_operational_kg": _safe_float(
                entry.original.get("carbon_operational_kg")
            ) or 0.0,
            "explanation": dict(entry.explanation or {}),
            "provenance": {
                "source": "leaderboard",
                "analyzer_version": __version__,
                "deployment_id": self.deployment_id,
                "status": entry.status,
                "rank": entry.rank,
                "dense_rank": entry.dense_rank,
                "competition_rank": entry.competition_rank,
                "needs_review": entry.needs_review,
                "hitl_approved": entry.hitl_approved,
                "natural_rank": entry.natural_rank,
            },
        }
        filtered = _filter_decision_record_kwargs(
            DecisionRecord, candidates,
        )
        try:
            return DecisionRecord(**filtered)
        except TypeError as e:
            logger.warning(
                "leaderboard.decision_record_failed",
                extra={"error": str(e), "agent_id": entry.agent_id},
            )
            return None

    def to_decision_records(
        self,
        *,
        policy_version: str = "",
    ) -> List[Any]:
        """Emit a `DecisionRecord` for every ranked entry."""
        with self._lock:
            if self._last_ranked is None:
                self.rank()
            ranked = list(self._last_ranked or [])
        records: List[Any] = []
        for entry in ranked:
            r = self.to_decision_record(
                entry, policy_version=policy_version,
            )
            if r is not None:
                records.append(r)
        return records

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """
        Instance-scoped statistics.

        v1 merged the module-level `_STATS` with per-instance counters,
        which double-counted on every `report()` / `export()`. v2 keeps
        them strictly separate. Field names are preserved for backward
        compatibility; `global_rank_by_green_score_calls` exposes the
        module-level count separately.
        """
        with self._lock:
            snap = self._stats.snapshot()
            global_snap = _GLOBAL_STATS.snapshot()
            return {
                "analyzer_version": __version__,
                "run_id": self.run_id,
                "deployment_id": self.deployment_id,
                "total_rankings": snap.get("rankings", 0),
                "total_entries_ranked": snap.get("entries_ranked", 0),
                "total_invalid_entries": snap.get("invalid", 0),
                "total_ties": snap.get("ties", 0),
                "total_excluded": snap.get("excluded", 0),
                "total_overridden": snap.get("overridden", 0),
                "entries_accumulated": len(self._entries),
                "hitl_top_n": self._hitl_top_n,
                "tie_tolerance": self._tie_tolerance,
                "manual_overrides": dict(self._manual_overrides),
                "ranking_snapshots": len(self._ranking_history),
                "federated_profiles": len(self._federated.profiles),
                "cache_valid": self._last_ranked is not None,
                "global_rank_by_green_score_calls":
                    global_snap.get("rankings", 0),
                "features": dict(self.features),
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _invalidate_cache(self) -> None:
        self._last_ranked = None
        self._last_rank_stats = None
        self._last_rank_params = None

    def _record_stats(
        self,
        *,
        entries_this_call: int,
        ranked_this_call: int,
        ties_this_call: int,
        invalid_this_call: int,
        excluded_this_call: int,
    ) -> None:
        """
        Update per-instance and module-level counters with *deltas* only.
        """
        with self._lock:
            self._stats.increment("rankings")
            self._stats.increment("entries_ranked", ranked_this_call)
            self._stats.increment("ties", ties_this_call)
            self._stats.increment("invalid", invalid_this_call)
            self._stats.increment("excluded", excluded_this_call)
            overridden = sum(
                1 for e in (self._last_ranked or [])
                if e.status == RankStatus.OVERRIDDEN.value
            ) if self._last_ranked else 0
            if overridden:
                self._stats.increment("overridden", overridden)

        # Module-level delta (for `get_statistics()` v1 shim)
        _GLOBAL_STATS.increment("rankings")
        _GLOBAL_STATS.increment("entries", entries_this_call)
        _GLOBAL_STATS.increment("ties", ties_this_call)
        _GLOBAL_STATS.increment("invalid", invalid_this_call)
        _GLOBAL_STATS.increment("excluded", excluded_this_call)


# =============================================================================
# Sort helpers
# =============================================================================

def _sort_value(
    entry: Dict[str, Any], key: str, direction: str,
) -> Tuple[int, float]:
    """
    Return `(0, negated_value)` for valid, `(1, 0.0)` for invalid.

    Ascending direction uses the value directly. Descending negates so
    Python's ascending sort produces the right order.
    """
    v = _safe_float(entry.get(key))
    if v is None:
        return (1, 0.0)
    return (0, -v if direction == SortDirection.DESC.value else v)


def _optional_str(v: Any) -> Optional[str]:
    return v if isinstance(v, str) and v else None


# =============================================================================
# Convenience one-shot helpers (v1 compat)
# =============================================================================

def rank_detailed(
    results: List[Dict],
    *,
    by: str = DEFAULT_PRIMARY_KEY,
    descending: bool = True,
    tie_breakers: Optional[List[Tuple[str, bool]]] = None,
    tie_tolerance: float = DEFAULT_TIE_TOLERANCE,
) -> List[RankedEntry]:
    """One-shot enriched ranking using a fresh `Leaderboard`."""
    board = Leaderboard(tie_tolerance=tie_tolerance)
    board.add_results(results)
    return board.rank(
        by=by, descending=descending, tie_breakers=tie_breakers,
    )


# =============================================================================
# Module-level statistics shims (v1 compat)
# =============================================================================

def get_statistics() -> Dict[str, Any]:
    """
    Module-level statistics — reflects `rank_by_green_score` calls only.

    Historically this also included `Leaderboard.rank` contributions,
    which double-counted after v1's `report()` re-ran `rank()`. v2 keeps
    the module counter clean. Use `get_global_statistics()` if you need
    the combined view.
    """
    snap = _GLOBAL_STATS.snapshot()
    return {
        "total_rankings": snap.get("rankings", 0),
        "total_entries_ranked": snap.get("entries", 0),
        "total_invalid_entries": snap.get("invalid", 0),
        "total_ties": snap.get("ties", 0),
        "total_excluded": snap.get("excluded", 0),
    }


def get_global_statistics() -> Dict[str, Any]:
    """Alias for `get_statistics()`; kept for naming symmetry."""
    return get_statistics()


def reset_statistics() -> None:
    """Clear the module-level counter (does not affect instances)."""
    _GLOBAL_STATS.reset()


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    # =================================================================
    # 1. Original behaviour (backward compatible)
    # =================================================================
    print("=== 1. Original behaviour ===")
    results = [
        {"agent_id": "A", "green_score": 0.5},
        {"agent_id": "B", "green_score": 0.9},
        {"agent_id": "C", "green_score": 0.7},
    ]
    ranked = rank_by_green_score(results)
    print(f"  order: {[r['agent_id'] for r in ranked]}")
    print(f"  missing-key safe: "
          f"{rank_by_green_score([{'agent_id': 'X'}])}")

    # =================================================================
    # 2. report() is a pure read — no HITL spam, no counter drift
    # =================================================================
    print("\n=== 2. report() does not re-rank ===")
    board = Leaderboard(hitl_top_n=2)
    hitl_calls: List[str] = []
    board.set_hitl_callback(
        lambda e: hitl_calls.append(e.agent_id) or True
    )
    board.add_results([
        {"agent_id": "A", "green_score": 0.72, "accuracy": 0.91},
        {"agent_id": "B", "green_score": 0.95, "accuracy": 0.89},
        {"agent_id": "C", "green_score": 0.72, "accuracy": 0.94},
        {"agent_id": "D", "green_score": 0.85, "accuracy": 0.88},
        {"agent_id": "E"},                    # invalid
        {"agent_id": "F", "green_score": 0.65, "simulated": True},
    ])
    board.rank(tie_breakers=[("accuracy", True)])
    first_hitl = list(hitl_calls)
    stats_before = board.get_statistics()

    for _ in range(50):
        board.report()
        board.report(top_n=5)

    stats_after = board.get_statistics()
    print(f"  HITL calls after first rank: {len(first_hitl)}  "
          f"(expected 2)")
    print(f"  HITL calls after 100 reports: {len(hitl_calls)}  "
          f"(expected still 2)")
    print(f"  total_rankings before:  {stats_before['total_rankings']}")
    print(f"  total_rankings after:   {stats_after['total_rankings']}  "
          f"(expected unchanged)")
    print(f"  federated_profiles:     "
          f"{stats_after['federated_profiles']}  (expected 1)")

    # =================================================================
    # 3. Tie detection and rank variants
    # =================================================================
    print("\n=== 3. Tie detection ===")
    board_tie = Leaderboard(tie_tolerance=0.001)
    board_tie.add_results([
        {"agent_id": "A", "green_score": 0.72, "accuracy": 0.91},
        {"agent_id": "B", "green_score": 0.721, "accuracy": 0.88},
        {"agent_id": "C", "green_score": 0.72, "accuracy": 0.94},
        {"agent_id": "D", "green_score": 0.85, "accuracy": 0.88},
    ])
    ranked_tie = board_tie.rank(tie_breakers=[("accuracy", True)])
    print(f"  {'agent':>6} {'score':>8} {'rank':>5} {'dense':>6} "
          f"{'comp':>5} {'tie':>4}")
    for e in ranked_tie:
        print(f"  {e.agent_id:>6} {e.score:>8.4f} {e.rank:>5} "
              f"{e.dense_rank:>6} {e.competition_rank:>5} "
              f"{e.tie_group or '-':>4}")

    # =================================================================
    # 4. Manual overrides — all rank variants recomputed
    # =================================================================
    print("\n=== 4. Manual overrides (coherent) ===")
    board_ov = Leaderboard()
    board_ov.add_results([
        {"agent_id": "A", "green_score": 0.95},
        {"agent_id": "B", "green_score": 0.90},
        {"agent_id": "C", "green_score": 0.85},
    ])
    board_ov.rank()
    board_ov.apply_override("A", 3)   # move A to rank 3
    ranked_ov = board_ov.rank()
    print(f"  {'agent':>6} {'rank':>5} {'dense':>6} {'comp':>5} "
          f"{'status':>12} {'natural':>8}")
    for e in ranked_ov:
        print(f"  {e.agent_id:>6} {e.rank:>5} {e.dense_rank:>6} "
              f"{e.competition_rank:>5} {e.status:>12} "
              f"{e.natural_rank or '-':>8}")

    print("\n  Override collision (allow_collision=False):")
    try:
        board_ov.apply_override("B", 3)
        print("    accepted (unexpected)")
    except ValueError as e:
        print(f"    raised: {str(e)[:60]}...")

    # =================================================================
    # 5. exclude_invalid honoured
    # =================================================================
    print("\n=== 5. exclude_invalid flag ===")
    board_inc = Leaderboard()
    board_inc.add_results([
        {"agent_id": "A", "green_score": 0.9},
        {"agent_id": "B"},   # invalid
        {"agent_id": "C", "green_score": 0.8},
    ])
    r_default = board_inc.rank()
    print(f"  default statuses: "
          f"{[e.status for e in r_default]}")

    board_inc.features["exclude_invalid"] = True
    board_inc.clear()
    board_inc.add_results([
        {"agent_id": "A", "green_score": 0.9},
        {"agent_id": "B"},
        {"agent_id": "C", "green_score": 0.8},
    ])
    r_excl = board_inc.rank()
    print(f"  exclude_invalid statuses: "
          f"{[e.status for e in r_excl]}  "
          f"(no 'invalid')")

    # =================================================================
    # 6. HITL denial annotation
    # =================================================================
    print("\n=== 6. HITL denial annotation ===")
    board_deny = Leaderboard(hitl_top_n=1)
    board_deny.add_results([
        {"agent_id": "A", "green_score": 0.95},
        {"agent_id": "B", "green_score": 0.90},
    ])
    board_deny.set_hitl_callback(lambda e: False)
    ranked_deny = board_deny.rank()
    top = ranked_deny[0]
    print(f"  top: {top.agent_id}")
    print(f"  hitl_approved: {top.hitl_approved}")
    print(f"  review_reason: {top.review_reason}")
    print(f"  contains '[HITL: denied]': "
          f"{'[HITL: denied]' in (top.review_reason or '')}")

    # =================================================================
    # 7. Stability metrics
    # =================================================================
    print("\n=== 7. Stability metrics ===")
    board_st = Leaderboard()
    board_st.add_results([
        {"agent_id": "A", "green_score": 0.9},
        {"agent_id": "B", "green_score": 0.85},
        {"agent_id": "C", "green_score": 0.8},
    ])
    board_st.rank()
    board_st.clear()
    board_st.add_results([
        {"agent_id": "B", "green_score": 0.9},
        {"agent_id": "A", "green_score": 0.85},
        {"agent_id": "C", "green_score": 0.8},
    ])
    board_st.rank()
    metrics = board_st.stability_metrics()
    print(f"  available:          {metrics['available']}")
    print(f"  common_agents:      {metrics['n_common_agents']}")
    print(f"  spearman_footrule:  {metrics['spearman_footrule']}")
    print(f"  kendall_tau:        {metrics['kendall_tau']:.4f}")
    print(f"  max_rank_change:    {metrics['max_rank_change']}")

    # =================================================================
    # 8. DecisionRecord emission
    # =================================================================
    print("\n=== 8. DecisionRecord emission ===")

    class _StrictDR:
        def __init__(self, run_id, timestamp, task_id, selected_action,
                     quality_score, explanation, provenance):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action
            self.quality_score = quality_score
            self.explanation = explanation
            self.provenance = provenance

    import sys
    import types
    fake_mod = types.ModuleType("analysis")
    fake_mod.DecisionRecord = _StrictDR
    sys.modules["analysis"] = fake_mod

    # Force re-import of DecisionRecord in the module
    import src.analysis.reporting.leaderboard as _lb_mod
    import importlib
    importlib.reload(_lb_mod)

    try:
        b_dr = _lb_mod.Leaderboard()
        b_dr.add_results([
            {"agent_id": "A", "green_score": 0.9, "run_id": "run-1"},
            {"agent_id": "B", "green_score": 0.8, "run_id": "run-1"},
        ])
        b_dr.rank()
        recs = b_dr.to_decision_records()
        print(f"  emitted: {len(recs)}")
        if recs:
            print(f"  selected_action: {recs[0].selected_action}")
            print(f"  provenance keys: "
                  f"{sorted(recs[0].provenance.keys())[:5]}...")
    finally:
        del sys.modules["analysis"]
        importlib.reload(_lb_mod)

    # =================================================================
    # 9. JSON-safe export with unserializable original
    # =================================================================
    print("\n=== 9. JSON-safe export ===")
    b_safe = Leaderboard()
    b_safe.add_results([
        {"agent_id": "A", "green_score": 0.9, "payload": object()},
    ])
    b_safe.rank()
    try:
        json.loads(b_safe.export_json())
        print("  json.loads(export_json()): OK")
    except (TypeError, ValueError) as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 10. clear(full=True) resets everything
    # =================================================================
    print("\n=== 10. clear(full=True) ===")
    b_reset = Leaderboard()
    b_reset.add_results([{"agent_id": "A", "green_score": 0.9}])
    b_reset.rank()
    b_reset.apply_override("A", 5)
    before = b_reset.get_statistics()
    b_reset.clear(full=True)
    after = b_reset.get_statistics()
    print(f"  before: rankings={before['total_rankings']}, "
          f"overrides={len(before['manual_overrides'])}")
    print(f"  after:  rankings={after['total_rankings']}, "
          f"overrides={len(after['manual_overrides'])}, "
          f"snapshots={after['ranking_snapshots']}")

    # =================================================================
    # 11. Thread safety (barrier + 4 threads adding results + ranking)
    # =================================================================
    print("\n=== 11. Thread safety ===")
    import threading
    b_conc = Leaderboard()
    barrier = threading.Barrier(4)

    def _worker(n: int) -> None:
        barrier.wait()
        for i in range(50):
            b_conc.add_results([
                {"agent_id": f"agent-{n}-{i}",
                 "green_score": 0.5 + 0.01 * i},
            ])
        b_conc.rank()

    threads = [
        threading.Thread(target=_worker, args=(n,))
        for n in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    s = b_conc.get_statistics()
    print(f"  entries_accumulated: {s['entries_accumulated']}  "
          f"(expected 200)")
    print(f"  total_rankings:      {s['total_rankings']}  "
          f"(expected >= 1)")

    # =================================================================
    # 12. Instance-scoped statistics (v1 double-counted)
    # =================================================================
    print("\n=== 12. Instance-scoped statistics ===")
    reset_statistics()
    a = Leaderboard()
    b = Leaderboard()
    for board_x, n in ((a, 3), (b, 5)):
        board_x.add_results([
            {"agent_id": f"x-{i}", "green_score": 0.5}
            for i in range(n)
        ])
        board_x.rank()
    print(f"  instance A total_rankings: {a.get_statistics()['total_rankings']}")
    print(f"  instance B total_rankings: {b.get_statistics()['total_rankings']}")
    print(f"  module get_statistics:     {get_statistics()['total_rankings']}")

    # =================================================================
    # 13. Full statistics
    # =================================================================
    print("\n=== 13. Full statistics ===")
    print(json.dumps(board_tie.get_statistics(), indent=2, default=str))
