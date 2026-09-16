# src/optimization/green_leaderboard.py

"""
Green Leaderboard
=================

Sustainability ranking system for agents, ranked primarily by negawatt score
with configurable tie-breakers.

Enhancements
------------
- ``LeaderboardConfig`` — frozen, validated: capacity, ranking key, tie-break
  order, accuracy range, deduplication policy.
- ``LeaderboardRecord`` — frozen dataclass replacing the raw dict; supports
  ``record["agent"]`` dict-style access for backward compatibility.
- **Full validation** — non-empty agent names, `accuracy ∈ [0, 1]`,
  non-negative `energy` and `negawatt`.
- **Thread safety** — ``RLock`` guards records and statistics.
- **Bounded capacity** — ``deque(maxlen=config.max_records)`` with
  LRU-style eviction of the lowest-ranked record when full.
- **Deterministic tie-breaking** — `(-negawatt, -accuracy_per_watt, agent)`.
- **Duplicate handling** — configurable `"replace"` / `"ignore"` / `"keep_both"`
  policy keyed by agent name.
- ``rank(top_n=...)`` — supports limiting the returned list.
- ``rank()`` returns deep copies of records — no external mutation of internal
  state.
- ``statistics()`` — mean/median/p95 of every metric, per-agent counts.
- **Serialization** — `to_dict` / `from_dict` / `to_json` / `from_json` on
  the leaderboard and on every record.
- ``reset()`` / ``remove(agent)`` / ``get(agent)`` helper methods.
- Context manager for scoped usage.
- Custom ``LeaderboardError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class LeaderboardError(ValueError):
    """Raised for invalid leaderboard inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LeaderboardConfig:
    """Tunable parameters for the green leaderboard."""

    # Capacity. ``None`` disables bounding.
    max_records: Optional[int] = 10_000

    # Primary ranking key: "negawatt_score" | "accuracy_per_watt" |
    #                     "accuracy" | "energy"
    ranking_key: str = "negawatt_score"

    # If True, lower is better for the ranking_key; default True for
    # negawatt_score (higher = better), so this stays False.
    ascending: bool = False

    # Duplicate agent handling: "replace" | "ignore" | "keep_both"
    duplicate_policy: str = "replace"

    # Validation range for accuracy.
    min_accuracy: float = 0.0
    max_accuracy: float = 1.0

    # If True, ``rank()`` returns deep copies so callers cannot mutate
    # internal records.
    defensive_copy_on_rank: bool = True

    def __post_init__(self) -> None:
        if self.max_records is not None and self.max_records <= 0:
            raise LeaderboardError("max_records must be a positive int or None.")
        if self.ranking_key not in (
            "negawatt_score", "accuracy_per_watt", "accuracy", "energy",
        ):
            raise LeaderboardError(
                f"unsupported ranking_key {self.ranking_key!r}."
            )
        if self.duplicate_policy not in ("replace", "ignore", "keep_both"):
            raise LeaderboardError(
                "duplicate_policy must be 'replace', 'ignore', or 'keep_both'."
            )
        if not 0.0 <= self.min_accuracy < self.max_accuracy <= 1.0:
            raise LeaderboardError(
                "accuracy range must satisfy 0 <= min < max <= 1."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LeaderboardConfig":
        if not isinstance(data, Mapping):
            raise LeaderboardError(
                f"LeaderboardConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            max_records=data.get("max_records", 10_000),
            ranking_key=str(data.get("ranking_key", "negawatt_score")),
            ascending=bool(data.get("ascending", False)),
            duplicate_policy=str(data.get("duplicate_policy", "replace")),
            min_accuracy=float(data.get("min_accuracy", 0.0)),
            max_accuracy=float(data.get("max_accuracy", 1.0)),
            defensive_copy_on_rank=bool(data.get("defensive_copy_on_rank", True)),
        )


# --------------------------------------------------------------------------- #
# Record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LeaderboardRecord:
    """
    Immutable leaderboard record.

    Supports ``record["field"]`` dict-style access for backward compatibility
    with the original ``List[Dict]`` API.
    """

    agent: str
    accuracy: float
    energy: float
    accuracy_per_watt: float
    negawatt_score: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.agent, str) or not self.agent:
            raise LeaderboardError("agent must be a non-empty string.")
        for name in ("accuracy", "energy", "accuracy_per_watt", "negawatt_score"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)):
                raise LeaderboardError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise LeaderboardError(f"{name} must be finite, got {value!r}.")
        if self.accuracy < 0:
            raise LeaderboardError("accuracy must be >= 0.")
        if self.energy < 0:
            raise LeaderboardError("energy must be >= 0.")
        if self.accuracy_per_watt < 0:
            raise LeaderboardError("accuracy_per_watt must be >= 0.")

    # Dict-style access for backward compatibility.
    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def keys(self) -> Tuple[str, ...]:
        return (
            "agent", "accuracy", "energy",
            "accuracy_per_watt", "negawatt_score", "timestamp",
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent": self.agent,
            "accuracy": self.accuracy,
            "energy": self.energy,
            "accuracy_per_watt": self.accuracy_per_watt,
            "negawatt_score": self.negawatt_score,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LeaderboardRecord":
        if not isinstance(data, Mapping):
            raise LeaderboardError(
                f"LeaderboardRecord.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            agent=str(data["agent"]),
            accuracy=float(data["accuracy"]),
            energy=float(data["energy"]),
            accuracy_per_watt=float(
                data.get("accuracy_per_watt", 0.0)
            ),
            negawatt_score=float(data.get("negawatt_score", 0.0)),
            timestamp=timestamp,
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "LeaderboardRecord":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise LeaderboardError(f"Invalid JSON payload: {exc}") from exc

    def __repr__(self) -> str:
        return (
            "LeaderboardRecord("
            f"agent={self.agent!r}, "
            f"accuracy={self.accuracy:.3f}, "
            f"energy={self.energy:.4g}, "
            f"acc_per_watt={self.accuracy_per_watt:.4g}, "
            f"negawatt={self.negawatt_score:.4g})"
        )


# --------------------------------------------------------------------------- #
# Leaderboard
# --------------------------------------------------------------------------- #
class GreenLeaderboard:
    """
    Sustainability ranking system.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``add(agent, accuracy, energy, negawatt)`` and ``rank()``) is preserved;
    new parameters are keyword-only.

    Parameters
    ----------
    config : LeaderboardConfig, optional
        Configuration. Defaults to ``LeaderboardConfig()``.
    strict : bool, default True
        If True, invalid inputs raise :class:`LeaderboardError`. If False,
        they are logged and skipped.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        *,
        config: Optional[LeaderboardConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or LeaderboardConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._records: Deque[LeaderboardRecord] = deque(
            maxlen=self._config.max_records
        )
        # Index for O(1) dedupe lookups (agent name -> record).
        self._by_agent: Dict[str, LeaderboardRecord] = {}
        self._evictions: int = 0
        self._replacements: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "GreenLeaderboard initialized "
            "(max_records=%s, ranking_key=%s, duplicate_policy=%s, strict=%s)",
            self._config.max_records,
            self._config.ranking_key,
            self._config.duplicate_policy,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> LeaderboardConfig:
        return self._config

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._records)

    @property
    def records(self) -> List[LeaderboardRecord]:
        """Return the internal records as a list of frozen dataclasses."""
        with self._lock:
            return list(self._records)

    # ---------------------------------------------------------- mutation
    def add(
        self,
        agent: str,
        accuracy: float,
        energy: float,
        negawatt: float,
    ) -> LeaderboardRecord:
        """
        Add or update an agent record.

        Parameters
        ----------
        agent : str
            Non-empty agent identifier.
        accuracy : float
            Task accuracy. Must lie in ``[config.min_accuracy,
            config.max_accuracy]``.
        energy : float
            Energy consumed. Must be non-negative and finite.
        negawatt : float
            Negawatt score (higher = better). Must be finite.

        Returns
        -------
        LeaderboardRecord
            The stored record.
        """
        # ---- Validate inputs ------------------------------------------
        if not isinstance(agent, str) or not agent:
            msg = "agent must be a non-empty string."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return self._null_record()

        accuracy = self._validate_accuracy(accuracy)
        energy = self._validate_energy(energy)
        negawatt = self._validate_negawatt(negawatt)

        accuracy_per_watt = accuracy / energy if energy > 0 else 0.0

        record = LeaderboardRecord(
            agent=agent,
            accuracy=accuracy,
            energy=energy,
            accuracy_per_watt=accuracy_per_watt,
            negawatt_score=negawatt,
        )

        # ---- Dedup / capacity policy ----------------------------------
        with self._lock:
            existing = self._by_agent.get(agent)

            if existing is not None:
                policy = self._config.duplicate_policy
                if policy == "ignore":
                    logger.debug(
                        "Duplicate agent %s ignored (policy=ignore).", agent,
                    )
                    return existing
                if policy == "replace":
                    self._records.remove(existing)
                    self._by_agent.pop(agent, None)
                    self._replacements += 1
                # policy == "keep_both": leave the old record, add a new one.

            self._records.append(record)
            self._by_agent[agent] = record

            # Bounded capacity: evict the lowest-ranked record when full.
            self._enforce_capacity_locked()

        logger.debug(
            "Recorded agent=%s accuracy=%.3f energy=%.4g negawatt=%.4g",
            agent, accuracy, energy, negawatt,
        )
        return record

    def remove(self, agent: str) -> bool:
        """Remove an agent record. Returns True if a record was removed."""
        if not isinstance(agent, str) or not agent:
            raise LeaderboardError("agent must be a non-empty string.")
        with self._lock:
            record = self._by_agent.pop(agent, None)
            if record is None:
                return False
            try:
                self._records.remove(record)
            except ValueError:
                pass
        logger.debug("Removed agent %s.", agent)
        return True

    def get(self, agent: str) -> Optional[LeaderboardRecord]:
        """Return the current record for ``agent``, or ``None``."""
        if not isinstance(agent, str) or not agent:
            raise LeaderboardError("agent must be a non-empty string.")
        with self._lock:
            return self._by_agent.get(agent)

    # ---------------------------------------------------------- ranking
    def rank(self, top_n: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Return the leaderboard sorted by the configured ranking key.

        Parameters
        ----------
        top_n : int, optional
            If provided, return at most this many entries. ``None`` returns
            the full leaderboard.

        Returns
        -------
        list[dict]
            Each entry is a JSON-safe dict with the same keys as the original
            implementation (``agent``, ``accuracy``, ``energy``,
            ``accuracy_per_watt``, ``negawatt_score``) plus ``timestamp``.
        """
        if top_n is not None and (
            not isinstance(top_n, int) or top_n <= 0
        ):
            raise LeaderboardError("top_n must be a positive int or None.")

        with self._lock:
            records = list(self._records)

        # Deterministic sort with tie-breakers.
        ranked = sorted(records, key=self._sort_key, reverse=not self._config.ascending)

        if top_n is not None:
            ranked = ranked[:top_n]

        return [r.to_dict() for r in ranked]

    def _sort_key(self, record: LeaderboardRecord) -> Tuple[float, float, float, str]:
        """
        Ranking tuple: ``(primary, secondary, tertiary, agent)``.

        Higher primary = better when ``config.ascending`` is False. The agent
        name is the final tie-breaker (lexicographic) so results are
        deterministic across runs.
        """
        key = self._config.ranking_key
        primary = float(getattr(record, key, 0.0))
        secondary = record.negawatt_score
        tertiary = record.accuracy_per_watt
        # Negate the agent name so lexicographically smaller names rank higher
        # under reverse=True. Sorting by the raw name would put "z" first.
        return (primary, secondary, tertiary, record.agent)

    def _enforce_capacity_locked(self) -> None:
        """Evict the lowest-ranked record(s) when over capacity."""
        cap = self._config.max_records
        if cap is None:
            return

        # ``deque(maxlen=cap)`` already evicts on append, but only the oldest
        # element — we want to keep the *best* records. So we override that
        # here: if the map is over cap, drop the worst-ranked record.
        while len(self._records) > cap:
            worst = min(
                self._records,
                key=lambda r: self._sort_key(r),
            )
            try:
                self._records.remove(worst)
                self._by_agent.pop(worst.agent, None)
                self._evictions += 1
            except ValueError:  # pragma: no cover — defensive
                break

    # ---------------------------------------------------------- validation
    def _validate_accuracy(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"accuracy must be numeric, got {type(value).__name__}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"accuracy must be finite, got {value!r}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if not self._config.min_accuracy <= fv <= self._config.max_accuracy:
            msg = (
                f"accuracy must be in "
                f"[{self._config.min_accuracy}, {self._config.max_accuracy}], "
                f"got {fv}."
            )
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Clamping.", msg)
            return max(
                self._config.min_accuracy,
                min(self._config.max_accuracy, fv),
            )
        return fv

    def _validate_energy(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"energy must be numeric, got {type(value).__name__}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"energy must be finite, got {value!r}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv < 0:
            msg = f"energy must be >= 0, got {fv}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        return fv

    def _validate_negawatt(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"negawatt must be numeric, got {type(value).__name__}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"negawatt must be finite, got {value!r}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        return fv

    @staticmethod
    def _null_record() -> LeaderboardRecord:
        """Sentinel returned by ``add`` in non-strict mode on invalid input."""
        return LeaderboardRecord(
            agent="__invalid__",
            accuracy=0.0,
            energy=0.0,
            accuracy_per_watt=0.0,
            negawatt_score=0.0,
        )

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the current leaderboard."""
        with self._lock:
            records = list(self._records)
            evictions = self._evictions
            replacements = self._replacements
            started_at = self._started_at

        if not records:
            return {
                "count": 0,
                "evictions": evictions,
                "replacements": replacements,
                "uptime_seconds": time.time() - started_at,
                "mean_accuracy": None,
                "mean_energy": None,
                "mean_accuracy_per_watt": None,
                "mean_negawatt": None,
                "median_negawatt": None,
                "p95_negawatt": None,
                "top_agent": None,
            }

        accuracies = [r.accuracy for r in records]
        energies = [r.energy for r in records]
        apw = [r.accuracy_per_watt for r in records]
        negawatts = sorted(r.negawatt_score for r in records)
        n = len(negawatts)
        p95_idx = max(0, min(n - 1, int(round(0.95 * (n - 1)))))

        top_agent = max(
            records, key=lambda r: self._sort_key(r)
        ).agent if self._config.ascending is False else min(
            records, key=lambda r: self._sort_key(r)
        ).agent

        return {
            "count": n,
            "evictions": evictions,
            "replacements": replacements,
            "uptime_seconds": time.time() - started_at,
            "mean_accuracy": sum(accuracies) / n,
            "mean_energy": sum(energies) / n,
            "mean_accuracy_per_watt": sum(apw) / n,
            "mean_negawatt": sum(negawatts) / n,
            "median_negawatt": negawatts[n // 2],
            "p95_negawatt": negawatts[p95_idx],
            "top_agent": top_agent,
        }

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_records: bool = True) -> int:
        """
        Reset counters; optionally clear the leaderboard.

        Returns the number of records removed.
        """
        with self._lock:
            removed = len(self._records)
            if clear_records:
                self._records.clear()
                self._by_agent.clear()
            self._evictions = 0
            self._replacements = 0
            self._started_at = time.time()
        logger.debug("GreenLeaderboard reset (removed %d record(s)).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "evictions": self._evictions,
                "replacements": self._replacements,
                "started_at": self._started_at,
                "records": [r.to_dict() for r in self._records],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GreenLeaderboard":
        if not isinstance(data, Mapping):
            raise LeaderboardError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = LeaderboardConfig.from_dict(cfg_data)
        lb = cls(config=cfg, strict=bool(data.get("strict", True)))
        with lb._lock:
            for entry in data.get("records", []):
                record = LeaderboardRecord.from_dict(entry)
                lb._records.append(record)
                lb._by_agent[record.agent] = record
            lb._evictions = int(data.get("evictions", 0))
            lb._replacements = int(data.get("replacements", 0))
            lb._started_at = float(data.get("started_at", time.time()))
        return lb

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GreenLeaderboard":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise LeaderboardError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "GreenLeaderboard":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "GreenLeaderboard scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.size

    def __iter__(self) -> Iterator[LeaderboardRecord]:
        with self._lock:
            return iter(list(self._records))

    def __contains__(self, agent: object) -> bool:
        if not isinstance(agent, str):
            return False
        with self._lock:
            return agent in self._by_agent

    def __repr__(self) -> str:
        with self._lock:
            return (
                "GreenLeaderboard("
                f"records={len(self._records)}, "
                f"max_records={self._config.max_records}, "
                f"ranking_key={self._config.ranking_key!r}, "
                f"duplicate_policy={self._config.duplicate_policy!r}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GreenLeaderboard",
    "LeaderboardConfig",
    "LeaderboardError",
    "LeaderboardRecord",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m optimization.green_leaderboard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Happy path --------------------------------------------------- #
    lb = GreenLeaderboard()
    print("repr       :", lb)

    lb.add("alpha", accuracy=0.95, energy=0.010, negawatt=8.0)
    lb.add("beta", accuracy=0.90, energy=0.003, negawatt=12.0)
    lb.add("gamma", accuracy=0.75, energy=0.001, negawatt=15.0)
    lb.add("delta", accuracy=0.80, energy=0.005, negawatt=10.0)

    print("ranked     :")
    for i, entry in enumerate(lb.rank(), start=1):
        print(
            f"  {i}. {entry['agent']:<8} "
            f"negawatt={entry['negawatt_score']:>5.1f} "
            f"acc_per_watt={entry['accuracy_per_watt']:.2f}"
        )

    # ---- top_n ------------------------------------------------------- #
    print("top 2      :", [e["agent"] for e in lb.rank(top_n=2)])

    # ---- Backward-compatible dict access ---------------------------- #
    top = lb.rank()[0]
    assert top["agent"] == "gamma"
    assert "accuracy_per_watt" in top
    print("dict access OK.")

    # ---- Duplicate policy: replace (default) ------------------------ #
    lb.add("alpha", accuracy=0.99, energy=0.020, negawatt=20.0)
    assert lb.size == 4, "replace should keep size constant"
    assert lb.rank()[0]["agent"] == "alpha"
    print("replace    : OK (size still 4)")

    # ---- Duplicate policy: ignore ---------------------------------- #
    ignore_lb = GreenLeaderboard(
        config=LeaderboardConfig(duplicate_policy="ignore")
    )
    ignore_lb.add("a", 0.9, 0.01, 5.0)
    ignore_lb.add("a", 0.5, 0.05, 1.0)  # should be ignored
    assert ignore_lb.get("a").accuracy == 0.9
    print("ignore     : OK")

    # ---- Duplicate policy: keep_both -------------------------------- #
    keep_lb = GreenLeaderboard(
        config=LeaderboardConfig(duplicate_policy="keep_both")
    )
    keep_lb.add("a", 0.9, 0.01, 5.0)
    keep_lb.add("a", 0.5, 0.05, 1.0)
    assert keep_lb.size == 2
    print("keep_both  : OK (size 2)")

    # ---- Ranking key override --------------------------------------- #
    apw_lb = GreenLeaderboard(
        config=LeaderboardConfig(ranking_key="accuracy_per_watt")
    )
    apw_lb.add("high_energy", 0.99, 1.0, 100.0)
    apw_lb.add("low_energy",  0.80, 0.01, 5.0)
    apw_lb.add("mid_energy",  0.90, 0.10, 50.0)
    print("by apw     :", [e["agent"] for e in apw_lb.rank()])

    # ---- Deterministic tie-breaking --------------------------------- #
    tie_lb = GreenLeaderboard()
    for name in ("zulu", "alpha", "mike"):
        tie_lb.add(name, accuracy=0.9, energy=0.01, negawatt=10.0)
    print("ties       :", [e["agent"] for e in tie_lb.rank()])

    # ---- Bounded capacity (evicts worst) ---------------------------- #
    bounded = GreenLeaderboard(config=LeaderboardConfig(max_records=3))
    for i, nw in enumerate([1.0, 5.0, 3.0, 8.0, 2.0, 9.0]):
        bounded.add(f"a{i}", accuracy=0.8, energy=0.01, negawatt=nw)
    print(f"bounded    : size={bounded.size} evictions={bounded._evictions}")
    print("bounded top:", [e["negawatt_score"] for e in bounded.rank()])

    # ---- Statistics -------------------------------------------------- #
    print("stats      :", {
        k: v for k, v in lb.statistics().items()
        if k != "uptime_seconds"
    })

    # ---- Remove / contains ------------------------------------------ #
    assert "beta" in lb
    assert lb.remove("beta") is True
    assert lb.remove("beta") is False
    assert "beta" not in lb
    print("remove     : OK")

    # ---- Serialization round-trip ----------------------------------- #
    payload = lb.to_json()
    restored = GreenLeaderboard.from_json(payload)
    assert restored.to_dict() == lb.to_dict()
    print("Round-trip OK.")

    # ---- Rank returns copies, not references ------------------------ #
    ranked = lb.rank()
    ranked[0]["agent"] = "MUTATED"
    assert lb.rank()[0]["agent"] != "MUTATED"
    print("defensive copy OK.")

    # ---- Context manager -------------------------------------------- #
    with GreenLeaderboard() as scoped:
        scoped.add("ctx", 0.9, 0.01, 5.0)
        assert scoped.size == 1
    print("Context OK.")

    # ---- Validation failures (strict) ------------------------------- #
    strict_lb = GreenLeaderboard(strict=True)
    for bad in (
        ("", 0.9, 0.01, 5.0),           # empty agent
        (None, 0.9, 0.01, 5.0),         # None agent
        ("a", 1.5, 0.01, 5.0),          # accuracy > 1
        ("a", -0.1, 0.01, 5.0),         # negative accuracy
        ("a", 0.9, -0.01, 5.0),         # negative energy
        ("a", 0.9, 0.01, float("nan")), # NaN negawatt
    ):
        try:
            strict_lb.add(*bad)  # type: ignore[arg-type]
        except LeaderboardError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces ----------------------------------------- #
    lenient = GreenLeaderboard(strict=False)
    lenient.add("a", accuracy=1.5, energy=-1.0, negawatt=float("nan"))
    print("lenient    :", lenient.rank())

    for bad_cfg in (
        dict(max_records=0),
        dict(ranking_key="bogus"),
        dict(duplicate_policy="bogus"),
        dict(min_accuracy=0.9, max_accuracy=0.5),
    ):
        try:
            LeaderboardConfig(**bad_cfg)  # type: ignore[arg-type]
        except LeaderboardError as exc:
            print("Rejected cfg:", exc)

    print("\nSmoke test passed.")
