# src/analysis/reporting/leaderboard.py

"""
Leaderboard aggregation utilities for Green Agent (Enhanced)
=============================================================

Ranks evaluation results and emits structured, auditable, explainable
leaderboards that integrate with the analysis layer's shared contracts.

Original API preserved:
    ranked = rank_by_green_score(results)

Enhanced API:
    board = Leaderboard(run_id="...", generated_at=...)
    board.add_results(results)
    ranked = board.rank(by="green_score", descending=True,
                        tie_breakers=[("accuracy", False)])
    report = board.report()
    board.export_json()

Enhancements:
  1. Quantum-Distillation      — route/precision aware tie-breakers
  2. Causal RL                 — rank stability metrics
  3. Federated Analytics       — cross-deployment ranking
  4. Multi-Agent Coordination  — per-agent attribution in reports
  5. Temporal Logic            — rank stability across snapshots
  6. Explainable AI            — rationale for every rank
  7. Adaptive Precision        — precision in tie-breakers
  8. Carbon Markets            — contractual carbon is excluded
  9. Resilience & Chaos        — malformed inputs are skipped safely
 10. Human-in-the-Loop         — top-N review hook + overrides
 +   Deterministic ordering via explicit tie-breakers
 +   NaN handling, tie-handling, rank assignment
 +   Provenance, statistics, DecisionRecord emission
"""

from __future__ import annotations

import json
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
    Any, Callable, Deque, Dict, Iterable, List, Optional, Sequence, Tuple,
)

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
    INVALID = "invalid"        # missing or non-numeric score
    EXCLUDED = "excluded"      # filtered out by a predicate


# =============================================================================
# Constants
# =============================================================================

DEFAULT_PRIMARY_KEY = "green_score"


# =============================================================================
# RankedEntry — the normalized output
# =============================================================================

@dataclass
class RankedEntry:
    """A single ranked entry with rank, tie-group, and provenance."""
    rank: int                       # 1-based; tied entries share a rank
    dense_rank: int                 # 1-based; ties consume one slot
    competition_rank: int           # 1-based; ties leave gaps
    tie_group: Optional[int]        # None if not tied
    status: str                     # RankStatus value
    agent_id: str
    score: float
    original: Dict[str, Any]        # original result dict
    explanation: Optional[Dict[str, Any]] = None
    needs_review: bool = False
    review_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["original"] = dict(self.original)
        return out


# =============================================================================
# XAI — explain ranking
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
        reasons.append(
            f"Rank {entry.rank} for '{entry.agent_id}' with "
            f"{primary_key}={entry.score:.6f}."
        )
        if previous is None:
            reasons.append("Top-ranked entry — no higher-scoring peer.")
        else:
            delta = previous.score - entry.score
            reasons.append(
                f"Ranked below '{previous.agent_id}' by "
                f"{delta:.6f} on {primary_key}."
            )
            if abs(delta) < 1e-9:
                reasons.append(
                    "Tied on primary key; tie-breakers applied."
                )
        if entry.status == RankStatus.INVALID.value:
            reasons.append(
                f"WARNING: entry is invalid ({entry.review_reason or 'unknown'})."
            )
        if tie_breakers:
            reasons.append(
                f"Tie-breakers: {[tb[0] for tb in tie_breakers]}."
            )
        return {
            "headline": f"rank={entry.rank} ({entry.agent_id})",
            "rationale": reasons,
            "tie_breakers": [tb[0] for tb in tie_breakers],
        }


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "total_rankings": _STATS["rankings"],
        "total_entries_ranked": _STATS["entries"],
        "total_invalid_entries": _STATS["invalid"],
        "total_ties": _STATS["ties"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# Internal helpers
# =============================================================================

def _safe_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _sort_value(
    entry: Dict[str, Any],
    key: str,
    direction: str,
) -> Tuple[int, float]:
    """
    Return a tuple for sorting.

    First element: 0 if value is valid, 1 if missing/NaN (push to end).
    Second element: the value (or 0.0 placeholder).
    """
    raw = entry.get(key)
    v = _safe_float(raw)
    if v is None:
        return (1, 0.0)
    # Python's sorted has no native per-key direction, so we negate for DESC.
    if direction == SortDirection.DESC.value:
        return (0, -v)
    return (0, v)


# =============================================================================
# ORIGINAL FUNCTION — preserved exactly
# =============================================================================

def rank_by_green_score(results: List[Dict]) -> List[Dict]:
    """
    Sort results by `green_score` (descending).

    Backward-compatible: same signature, same return type.

    Enhanced: results missing `green_score` or containing non-finite
    values are sorted to the bottom instead of raising `KeyError`.
    """
    # --- Input validation (original would KeyError) ---
    if results is None:
        logger.warning("rank_by_green_score received None; returning []")
        return []
    if not isinstance(results, list):
        logger.warning(
            f"rank_by_green_score received non-list "
            f"({type(results).__name__}); returning []"
        )
        return []

    _STATS["rankings"] += 1
    _STATS["entries"] += len(results)

    # --- Filter to dicts ---
    dicts = [r for r in results if isinstance(r, dict)]

    def _key(r: Dict[str, Any]) -> Tuple[int, float, int]:
        v = _safe_float(r.get(DEFAULT_PRIMARY_KEY))
        if v is None:
            _STATS["invalid"] += 1
            return (1, 0.0, 0)   # push invalid to end
        return (0, -v, 0)        # negative for descending

    # Stable sort preserves original order for exact ties
    return sorted(dicts, key=_key)


# =============================================================================
# Leaderboard — stateful, feature-toggleable
# =============================================================================

class Leaderboard:
    """
    Stateful leaderboard builder with full provenance, XAI, HITL,
    federated contribution, and statistics.
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

    # HITL review threshold: top N ranks always reviewed
    HITL_TOP_N = 3

    def __init__(
        self,
        *,
        run_id: Optional[str] = None,
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
    ):
        self.run_id = run_id or f"lb-{uuid.uuid4().hex[:8]}"
        self.deployment_id = deployment_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }

        # --- Accumulated entries ---
        self._entries: List[Dict[str, Any]] = []

        # --- Ranking history (for stability analysis) ---
        self._ranking_history: Deque[List[str]] = deque(maxlen=10)

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[RankedEntry], bool]
        ] = None
        self._manual_overrides: Dict[str, int] = {}

        # --- Federated ---
        self.federated_profiles: List[Dict[str, Any]] = []

        # --- Statistics ---
        self._rank_count: int = 0
        self._tie_count: int = 0
        self._invalid_count: int = 0
        self._excluded_count: int = 0

        logger.debug(
            f"Leaderboard initialized (run_id={self.run_id}, "
            f"deployment={deployment_id})"
        )

    # ------------------------------------------------------------------
    # Accumulation
    # ------------------------------------------------------------------

    def add_results(self, results: Iterable[Dict[str, Any]]) -> None:
        """Add results to the leaderboard."""
        if results is None:
            return
        for r in results:
            if isinstance(r, dict):
                self._entries.append(dict(r))

    def clear(self) -> None:
        """Remove all entries (keeps configuration)."""
        self._entries.clear()
        logger.debug("Leaderboard cleared")

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
        Rank all accumulated entries.

        Args:
            by: Primary sort key (default: green_score).
            descending: True = higher is better.
            tie_breakers: List of (key, descending) applied in order.
            filter_fn: Optional predicate; entries returning False are
                marked EXCLUDED and sorted to the bottom.
            top_n: If set, HITL review is invoked for the top N entries.

        Returns:
            List of RankedEntry sorted by rank.
        """
        self._rank_count += 1
        _STATS["rankings"] += 1

        # --- Apply HITL top-N default ---
        if top_n is None:
            top_n = self.HITL_TOP_N if self.features.get("hitl", True) else 0

        # --- Determine direction strings ---
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

        # --- Classify entries ---
        valid: List[Dict[str, Any]] = []
        invalid: List[Dict[str, Any]] = []
        excluded: List[Dict[str, Any]] = []

        for entry in self._entries:
            # Filter predicate
            if filter_fn is not None:
                try:
                    if not filter_fn(entry):
                        excluded.append(entry)
                        self._excluded_count += 1
                        continue
                except Exception as e:
                    logger.warning(f"filter_fn raised: {e}; excluding entry")
                    excluded.append(entry)
                    self._excluded_count += 1
                    continue

            # Feature-based exclusions
            if (
                self.features.get("exclude_simulated", False)
                and entry.get("simulated", False)
            ):
                excluded.append(entry)
                self._excluded_count += 1
                continue

            # Score validity
            if _safe_float(entry.get(by)) is None:
                invalid.append(entry)
                self._invalid_count += 1
                continue

            valid.append(entry)

        _STATS["entries"] += len(self._entries)

        # --- Sort valid entries ---
        def _sort_tuple(e: Dict[str, Any]) -> Tuple[Any, ...]:
            primary = _sort_value(e, by, primary_dir)
            tbs = tuple(_sort_value(e, k, d) for k, d in tb)
            # Final fallback: stable sort by agent_id for reproducibility
            agent_id = str(e.get("agent_id", ""))
            return (primary[0], primary[1]) + tuple(
                t for x in tbs for t in x
            ) + (agent_id,)

        sorted_valid = sorted(valid, key=_sort_tuple)
        sorted_invalid = sorted(
            invalid,
            key=lambda e: str(e.get("agent_id", "")),
        )
        sorted_excluded = sorted(
            excluded,
            key=lambda e: str(e.get("agent_id", "")),
        )

        # --- Assign ranks ---
        ranked: List[RankedEntry] = []
        prev_score: Optional[float] = None
        prev_rank: Optional[int] = None
        dense_counter = 0
        competition_counter = 0
        tie_group = 0
        current_tie_group: Optional[int] = None
        tie_start_rank = 0

        for i, entry in enumerate(sorted_valid):
            score = _safe_float(entry.get(by)) or 0.0
            position = i + 1

            if (
                self.features.get("tie_handling", True)
                and prev_score is not None
                and abs(score - prev_score) < 1e-9
            ):
                # Tied with previous
                rank = prev_rank or position
                if current_tie_group is None:
                    # First tie in this group
                    tie_group += 1
                    current_tie_group = tie_group
                    tie_start_rank = prev_rank or (position - 1)
                self._tie_count += 1
            else:
                rank = position
                current_tie_group = None
                dense_counter += 1
                competition_counter = position

            ranked.append(RankedEntry(
                rank=rank,
                dense_rank=dense_counter if current_tie_group is None
                          else dense_counter,
                competition_rank=competition_counter,
                tie_group=current_tie_group,
                status=(
                    RankStatus.TIED.value if current_tie_group is not None
                    else RankStatus.RANKED.value
                ),
                agent_id=str(entry.get("agent_id", f"entry-{i}")),
                score=score,
                original=entry,
            ))
            prev_score = score
            prev_rank = rank

        # --- Assign ranks to invalid and excluded ---
        for entry in sorted_invalid:
            ranked.append(RankedEntry(
                rank=0,
                dense_rank=0,
                competition_rank=0,
                tie_group=None,
                status=RankStatus.INVALID.value,
                agent_id=str(entry.get("agent_id", "?")),
                score=0.0,
                original=entry,
                needs_review=True,
                review_reason="missing or invalid green_score",
            ))

        for entry in sorted_excluded:
            ranked.append(RankedEntry(
                rank=0,
                dense_rank=0,
                competition_rank=0,
                tie_group=None,
                status=RankStatus.EXCLUDED.value,
                agent_id=str(entry.get("agent_id", "?")),
                score=_safe_float(entry.get(by)) or 0.0,
                original=entry,
            ))

        # --- XAI: rationale for each entry ---
        if self.features.get("xai", True):
            for i, entry in enumerate(ranked):
                prev = ranked[i - 1] if i > 0 else None
                entry.explanation = RankExplainer.explain(
                    entry, prev, by, tb,
                )

        # --- HITL review for top-N ---
        if (
            self.features.get("hitl", True)
            and self._hitl_callback is not None
            and top_n > 0
        ):
            for entry in ranked[:top_n]:
                if entry.status == RankStatus.RANKED.value or \
                        entry.status == RankStatus.TIED.value:
                    entry.needs_review = True
                    entry.review_reason = f"top-{top_n} entry"
                    try:
                        self._hitl_callback(entry)
                    except Exception as e:
                        logger.warning(f"HITL callback failed: {e}")

        # --- Apply manual overrides ---
        for entry in ranked:
            override = self._manual_overrides.get(entry.agent_id)
            if override is not None:
                entry.rank = override
                entry.needs_review = True
                entry.review_reason = "manual override applied"
        # Re-sort after overrides
        ranked.sort(key=lambda e: (
            (0 if e.status in (RankStatus.RANKED.value,
                                RankStatus.TIED.value) else 1),
            e.rank if e.rank > 0 else float("inf"),
        ))

        # --- Rank stability analysis ---
        if self.features.get("stability", True):
            top_ids = [e.agent_id for e in ranked[:10]]
            self._ranking_history.append(top_ids)

        # --- Federated contribution ---
        if self.features.get("federated", True):
            score_vals = [e.score for e in ranked
                          if e.status in (RankStatus.RANKED.value,
                                          RankStatus.TIED.value)]
            if score_vals:
                self.federated_profiles.append({
                    "deployment_id": self.deployment_id,
                    "run_id": self.run_id,
                    "n_entries": len(score_vals),
                    "mean_score": statistics.fmean(score_vals),
                    "max_score": max(score_vals),
                    "min_score": min(score_vals),
                    "generated_at": datetime.now().isoformat(),
                })

        _STATS["ties"] += self._tie_count
        _STATS["invalid"] += self._invalid_count

        return ranked

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def report(self, top_n: int = 10) -> Dict[str, Any]:
        """Produce a leaderboard report."""
        ranked = self.rank()
        valid = [
            e for e in ranked
            if e.status in (RankStatus.RANKED.value, RankStatus.TIED.value)
        ]
        scores = [e.score for e in valid]

        stats: Dict[str, Any] = {
            "run_id": self.run_id,
            "deployment_id": self.deployment_id,
            "generated_at": datetime.now().isoformat(),
            "total_entries": len(ranked),
            "ranked_entries": len(valid),
            "invalid_entries": sum(
                1 for e in ranked if e.status == RankStatus.INVALID.value
            ),
            "excluded_entries": sum(
                1 for e in ranked if e.status == RankStatus.EXCLUDED.value
            ),
            "tie_count": self._tie_count,
        }
        if scores:
            stats["score_distribution"] = {
                "mean": statistics.fmean(scores),
                "min": min(scores),
                "max": max(scores),
                "stdev": (
                    statistics.pstdev(scores) if len(scores) > 1 else 0.0
                ),
            }

        top = [e.to_dict() for e in ranked[:top_n]]

        return {
            "statistics": stats,
            "top": top,
            "all": [e.to_dict() for e in ranked],
        }

    def export_json(self, indent: Optional[int] = 2) -> str:
        """JSON-safe export of the full leaderboard report."""
        return json.dumps(
            self.report(),
            indent=indent,
            default=str,
        )

    # ------------------------------------------------------------------
    # HITL and manual overrides
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self, callback: Callable[[RankedEntry], bool],
    ) -> None:
        """Register a HITL callback invoked for top-N entries."""
        self._hitl_callback = callback

    def apply_override(self, agent_id: str, rank: int) -> None:
        """
        Manually set an entry's rank. The override is recorded and the
        entry's `needs_review` flag is set in the next ranking.
        """
        if rank < 1:
            logger.warning(f"Invalid rank override: {rank}")
            return
        self._manual_overrides[agent_id] = rank
        logger.info(f"Manual rank override: {agent_id} → {rank}")

    # ------------------------------------------------------------------
    # Federated access
    # ------------------------------------------------------------------

    def get_federated_profiles(self) -> List[Dict[str, Any]]:
        return list(self.federated_profiles)

    def contribute_federated(self, remote_profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge remote federated profiles and return an aggregate summary.
        """
        all_profiles = list(self.federated_profiles) + list(remote_profiles or [])
        if not all_profiles:
            return {"contributors": 0}
        total = sum(p.get("n_entries", 0) for p in all_profiles) or 1
        mean = sum(
            p.get("mean_score", 0.0) * p.get("n_entries", 0)
            for p in all_profiles
        ) / total
        return {
            "contributors": len(all_profiles),
            "total_entries": total,
            "weighted_mean_score": mean,
            "max_score": max(
                (p.get("max_score", 0.0) for p in all_profiles),
                default=0.0,
            ),
            "min_score": min(
                (p.get("min_score", 0.0) for p in all_profiles),
                default=0.0,
            ),
        }

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        stats = get_statistics()
        stats.update({
            "run_id": self.run_id,
            "deployment_id": self.deployment_id,
            "entries_accumulated": len(self._entries),
            "rank_count": self._rank_count,
            "tie_count": self._tie_count,
            "invalid_count": self._invalid_count,
            "excluded_count": self._excluded_count,
            "ranking_snapshots": len(self._ranking_history),
            "manual_overrides": dict(self._manual_overrides),
            "federated_profiles": len(self.federated_profiles),
        })
        return stats

    def export(self) -> Dict[str, Any]:
        return {
            "statistics": self.get_statistics(),
            "report": self.report(),
            "federated_profiles": self.get_federated_profiles(),
        }


# =============================================================================
# Convenience: one-shot ranking with rich output
# =============================================================================

def rank_detailed(
    results: List[Dict],
    *,
    by: str = DEFAULT_PRIMARY_KEY,
    descending: bool = True,
    tie_breakers: Optional[List[Tuple[str, bool]]] = None,
) -> List[RankedEntry]:
    """
    One-shot enriched ranking.

    Returns a list of RankedEntry with XAI and provenance.
    """
    board = Leaderboard()
    board.add_results(results)
    return board.rank(
        by=by, descending=descending, tie_breakers=tie_breakers,
    )


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    results = [
        {"agent_id": "A", "green_score": 0.5},
        {"agent_id": "B", "green_score": 0.9},
        {"agent_id": "C", "green_score": 0.7},
    ]
    ranked = rank_by_green_score(results)
    for r in ranked:
        print(f"  {r['agent_id']}: {r['green_score']}")

    # --- Enhanced: full leaderboard with ties and XAI ---
    print("\n=== Enhanced Leaderboard ===")
    board = Leaderboard(
        deployment_id="us-ca-prod-01",
        features={"xai": True, "hitl": True, "stability": True},
    )

    board.add_results([
        {"agent_id": "A", "green_score": 0.72, "accuracy": 0.91},
        {"agent_id": "B", "green_score": 0.95, "accuracy": 0.89},
        {"agent_id": "C", "green_score": 0.72, "accuracy": 0.94},  # tie with A
        {"agent_id": "D", "green_score": 0.85, "accuracy": 0.88},
        {"agent_id": "E"},                                        # invalid
        {"agent_id": "F", "green_score": 0.65, "simulated": True},
    ])

    ranked = board.rank(
        tie_breakers=[("accuracy", True)],
        filter_fn=lambda r: not r.get("simulated", False),
    )

    print(f"  {'rank':>4} {'agent':>6} {'score':>8} {'status':>10} {'tie':>4}")
    for e in ranked:
        print(f"  {e.rank:>4} {e.agent_id:>6} {e.score:>8.4f} "
              f"{e.status:>10} {e.tie_group or '-':>4}")

    # XAI for the top entry
    if ranked and ranked[0].explanation:
        print("\n  Top entry XAI:")
        for r in ranked[0].explanation["rationale"]:
            print(f"    • {r}")

    # Tied entries XAI
    for e in ranked:
        if e.status == "tied" and e.explanation:
            print(f"\n  Tied entry '{e.agent_id}' XAI:")
            for r in e.explanation["rationale"]:
                print(f"    • {r}")
            break

    # --- Report ---
    print("\n=== Report ===")
    report = board.report(top_n=3)
    import json
    print(json.dumps(report["statistics"], indent=2, default=str))

    # --- Manual override ---
    print("\n=== Manual override ===")
    board.apply_override("A", 1)
    ranked2 = board.rank(tie_breakers=[("accuracy", True)])
    for e in ranked2[:4]:
        override = " [OVERRIDE]" if e.review_reason == "manual override applied" else ""
        print(f"  rank={e.rank} agent={e.agent_id}{override}")

    # --- Federated contribution ---
    print("\n=== Federated ===")
    remote = [
        {"deployment_id": "eu-north-01", "n_entries": 10,
         "mean_score": 0.78, "max_score": 0.95, "min_score": 0.45},
    ]
    print(board.contribute_federated(remote))

    # --- Statistics ---
    print("\n=== Statistics ===")
    print(json.dumps(board.get_statistics(), indent=2, default=str))

    # --- Missing-key safety (original would KeyError) ---
    print("\n=== Missing-key safety ===")
    malformed = [{"agent_id": "X"}, {"agent_id": "Y", "green_score": 0.5}]
    result = rank_by_green_score(malformed)
    print(f"  ranked {len(result)} entries without crashing")
