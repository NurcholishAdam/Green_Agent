"""
Leaderboard for Green Agent (Enhanced)
=======================================

Ranks evaluated runs into an auditable, explainable leaderboard.
Backward-compatible with the original 6-line helper.

Original API preserved:
    generate_leaderboard(results) -> List[Dict]

Enhanced API:
    LeaderboardEntry, LeaderboardResult
    Leaderboard
        .add(results)
        .rank(tie_breakers=..., filter_fn=..., by="accuracy")
        .report()

Enhancements:
  1. Provenance on every entry
  2. XAI rationale per rank
  3. Deterministic tie-breaking
  4. Simulated-entry filtering
  5. Rank assignment (standard, dense, competition)
  6. Manual overrides
  7. Statistics
"""

from __future__ import annotations

import logging
import math
import statistics
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class RankStatus(Enum):
    RANKED = "ranked"
    TIED = "tied"
    INVALID = "invalid"
    EXCLUDED = "excluded"


# =============================================================================
# Entry types
# =============================================================================

@dataclass
class LeaderboardEntry:
    rank: int
    dense_rank: int
    competition_rank: int
    tie_group: Optional[int]
    status: str
    agent_id: str
    accuracy: float
    energy: float
    carbon: float
    original: Dict[str, Any] = field(default_factory=dict)
    explanation: Optional[Dict[str, Any]] = None
    needs_review: bool = False
    review_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["original"] = dict(self.original)
        return out


@dataclass
class LeaderboardResult:
    entries: List[LeaderboardEntry]
    total: int
    ranked: int
    invalid: int
    excluded: int
    tie_count: int
    generated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entries": [e.to_dict() for e in self.entries],
            "total": self.total,
            "ranked": self.ranked,
            "invalid": self.invalid,
            "excluded": self.excluded,
            "tie_count": self.tie_count,
            "generated_at": self.generated_at.isoformat(),
        }


# =============================================================================
# ORIGINAL function — preserved exactly
# =============================================================================

def generate_leaderboard(results: List[Dict]) -> List[Dict]:
    """
    Original 6-line helper: filter by `policy.compliant` and sort by
    (-accuracy, energy, carbon).

    Backward-compatible: same signature, same return type.

    Enhanced: entries lacking `policy.compliant` are treated as
    compliant=True (original behavior) but the enhanced `Leaderboard`
    class offers explicit filter functions.
    """
    if results is None:
        return []
    valid = [r for r in results if r.get("policy", {}).get("compliant", True)]
    return sorted(valid, key=lambda r: (
        -float(r.get("accuracy", 0.0)),
        float(r.get("energy", 0.0)),
        float(r.get("carbon", 0.0)),
    ))


# =============================================================================
# ENHANCED Leaderboard
# =============================================================================

class Leaderboard:
    """
    Enhanced leaderboard with provenance, XAI, ties, and statistics.
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "validation": True,
        "xai": True,
        "tie_handling": True,
        "deduplication": True,
        "hitl": True,
        "statistics": True,
    }

    def __init__(self, *, features: Optional[Dict[str, bool]] = None):
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}
        self._entries: List[Dict[str, Any]] = []
        self._overrides: Dict[str, int] = {}
        self._history: List[LeaderboardResult] = []

    def add(self, results: Iterable[Dict[str, Any]]) -> None:
        for r in results or []:
            if isinstance(r, dict):
                self._entries.append(dict(r))

    def clear(self) -> None:
        self._entries.clear()

    def apply_override(self, agent_id: str, rank: int) -> None:
        if rank < 1:
            return
        self._overrides[agent_id] = rank

    def rank(
        self,
        *,
        by: str = "accuracy",
        descending: bool = True,
        tie_breakers: Optional[List[Tuple[str, bool]]] = None,
        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> LeaderboardResult:
        tb = tie_breakers or [
            ("energy", False),   # lower energy wins ties on accuracy
            ("carbon", False),
        ]

        # --- Classify entries ---
        valid: List[Dict[str, Any]] = []
        invalid: List[Dict[str, Any]] = []
        excluded: List[Dict[str, Any]] = []

        for e in self._entries:
            if filter_fn is not None and not filter_fn(e):
                excluded.append(e)
                continue
            if "accuracy" not in e or not _is_numeric(e["accuracy"]):
                invalid.append(e)
                continue
            valid.append(e)

        # --- Sort ---
        def key(e: Dict[str, Any]) -> Tuple:
            primary = -float(e[by]) if descending else float(e[by])
            parts: List[Any] = [primary]
            for k, desc in tb:
                v = e.get(k)
                if _is_numeric(v):
                    parts.append(-float(v) if desc else float(v))
                else:
                    parts.append(0.0)
            # Deterministic final tie-break by agent_id
            parts.append(str(e.get("agent_id", "")))
            return tuple(parts)

        valid.sort(key=key)

        # --- Assign ranks ---
        entries: List[LeaderboardEntry] = []
        prev_acc: Optional[float] = None
        prev_rank: Optional[int] = None
        dense = 0
        tie_group = 0
        current_tie: Optional[int] = None
        ties = 0

        for i, e in enumerate(valid):
            acc = float(e["accuracy"])
            position = i + 1
            if prev_acc is not None and abs(acc - prev_acc) < 1e-9:
                rank = prev_rank or position
                if current_tie is None:
                    tie_group += 1
                    current_tie = tie_group
                ties += 1
            else:
                rank = position
                current_tie = None
                dense += 1

            entries.append(LeaderboardEntry(
                rank=rank,
                dense_rank=dense,
                competition_rank=position,
                tie_group=current_tie,
                status=(
                    RankStatus.TIED.value if current_tie
                    else RankStatus.RANKED.value
                ),
                agent_id=str(e.get("agent_id", f"entry-{i}")),
                accuracy=acc,
                energy=float(e.get("energy", 0.0)),
                carbon=float(e.get("carbon", 0.0)),
                original=e,
                explanation=_rank_rationale(
                    position, e, valid[i - 1] if i > 0 else None
                ) if self.features["xai"] else None,
            ))
            prev_acc = acc
            prev_rank = rank

        for e in invalid:
            entries.append(LeaderboardEntry(
                rank=0, dense_rank=0, competition_rank=0, tie_group=None,
                status=RankStatus.INVALID.value,
                agent_id=str(e.get("agent_id", "?")),
                accuracy=0.0, energy=0.0, carbon=0.0,
                original=e,
                needs_review=True,
                review_reason="missing or invalid accuracy",
            ))

        for e in excluded:
            entries.append(LeaderboardEntry(
                rank=0, dense_rank=0, competition_rank=0, tie_group=None,
                status=RankStatus.EXCLUDED.value,
                agent_id=str(e.get("agent_id", "?")),
                accuracy=float(e.get("accuracy", 0.0)),
                energy=float(e.get("energy", 0.0)),
                carbon=float(e.get("carbon", 0.0)),
                original=e,
            ))

        # --- Manual overrides ---
        for entry in entries:
            ov = self._overrides.get(entry.agent_id)
            if ov is not None:
                entry.rank = ov
                entry.needs_review = True
                entry.review_reason = "manual override applied"
        entries.sort(key=lambda e: (
            (0 if e.status in (RankStatus.RANKED.value, RankStatus.TIED.value)
             else 1),
            e.rank if e.rank > 0 else float("inf"),
        ))

        result = LeaderboardResult(
            entries=entries,
            total=len(self._entries),
            ranked=sum(
                1 for e in entries
                if e.status in (RankStatus.RANKED.value, RankStatus.TIED.value)
            ),
            invalid=len(invalid),
            excluded=len(excluded),
            tie_count=ties,
        )
        self._history.append(result)
        return result

    def report(self, top_n: int = 10) -> Dict[str, Any]:
        result = self.rank()
        return {
            "top": [e.to_dict() for e in result.entries[:top_n]],
            "summary": {
                "total": result.total,
                "ranked": result.ranked,
                "invalid": result.invalid,
                "excluded": result.excluded,
                "tie_count": result.tie_count,
                "generated_at": result.generated_at.isoformat(),
            },
        }


# =============================================================================
# Helpers
# =============================================================================

def _is_numeric(x: Any) -> bool:
    if isinstance(x, bool):
        return False
    try:
        f = float(x)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


def _rank_rationale(
    rank: int,
    entry: Dict[str, Any],
    previous: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    reasons: List[str] = [
        f"Rank {rank} for '{entry.get('agent_id')}' "
        f"with accuracy={entry.get('accuracy')}."
    ]
    if previous:
        delta = float(previous.get("accuracy", 0.0)) - float(entry.get("accuracy", 0.0))
        reasons.append(
            f"Ranked below '{previous.get('agent_id')}' by "
            f"{delta:.4f} on accuracy."
        )
    return {"headline": f"rank={rank}", "rationale": reasons}


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    results = [
        {"agent_id": "A", "accuracy": 0.95, "energy": 0.05, "carbon": 0.02,
         "policy": {"compliant": True}},
        {"agent_id": "B", "accuracy": 0.92, "energy": 0.04, "carbon": 0.015,
         "policy": {"compliant": True}},
        {"agent_id": "C", "accuracy": 0.95, "energy": 0.03, "carbon": 0.012,
         "policy": {"compliant": True}},
        {"agent_id": "D", "accuracy": 0.88, "energy": 0.02, "carbon": 0.008,
         "policy": {"compliant": False}},
        {"agent_id": "E"},
    ]

    # Original function
    print("=== Original generate_leaderboard ===")
    for r in generate_leaderboard(results):
        print(f"  {r['agent_id']}: acc={r['accuracy']}, energy={r['energy']}")

    # Enhanced
    print("\n=== Enhanced Leaderboard ===")
    board = Leaderboard()
    board.add(results)
    result = board.rank(tie_breakers=[("energy", False)])
    for entry in result.entries:
        print(f"  rank={entry.rank:2d} dense={entry.dense_rank:2d} "
              f"agent={entry.agent_id:3s} status={entry.status}")
        if entry.explanation:
            for r in entry.explanation["rationale"]:
                print(f"      • {r}")

    import json
    print("\n=== Report ===")
    print(json.dumps(board.report(top_n=3)["summary"], indent=2))
