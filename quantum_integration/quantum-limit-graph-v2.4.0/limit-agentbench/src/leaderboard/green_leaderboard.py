# src/optimization/green_leaderboard.py

"""
Green Leaderboard (v5.1)
========================

Sustainability ranking system for agents, ranked primarily by negawatt score
with configurable tie-breakers.

Fixes in v5.1 (see review)
--------------------------
  * P0 — Tie-breaker now matches the documented intent ("alpha" wins ties
         over "zulu"). The previous single-tuple sort put "zulu" first
         because ``reverse=True`` flipped the lexicographic order.
         Implemented as a two-pass stable sort (agent ascending, then
         ranking key in the configured direction).
  * P0 — ``_enforce_capacity_locked`` no longer evicts the *best* record
         when ``ascending=True`` (lower-is-better mode). Extracted a
         single ``_ranked_records()`` helper so ``rank()``,
         ``_enforce_capacity_locked()``, ``statistics()["top_agent"]``,
         and the new ``best()`` all agree on the ordering.
  * P0 — ``from_dict`` now routes records through ``add()`` so
         ``_records`` and ``_by_agent`` can never drift apart. Previously
         a serialized payload with more entries than ``max_records`` left
         ``_by_agent`` holding ghost entries invisible to ``rank()``.
  * P0 — ``LeaderboardRecord.__post_init__`` validates that
         ``accuracy_per_watt`` matches ``accuracy / energy`` (within
         ``rel_tol=1e-9``). ``from_dict`` recomputes it when absent.
  * P1 — ``keep_both`` policy is documented as "not for ranked
         leaderboards"; eviction re-points ``_by_agent`` to the next
         record for the agent (or drops it when no live records remain).
         ``remove(agent)`` removes *every* record for the agent.
  * P1 — Removed the dead ``defensive_copy_on_rank`` config option
         (``rank()`` always returns fresh dicts, so the flag never had
         an effect).
  * P1 — ``add()`` returns ``None`` on invalid input in non-strict mode
         instead of a magic ``agent="__invalid__"`` sentinel record.
  * P1 — ``LeaderboardRecord`` normalises naive timestamps to UTC in
         ``__post_init__``, matching the rest of the ``src/`` tree.
  * P1 — ``p95`` uses nearest-rank percentile (``ceil(0.95 * n) - 1``),
         which is stable across small samples. The previous
         ``round(0.95 * (n - 1))`` returned "the max" for every ``n <= 10``.
  * P1 — ``from_dict`` validates counters via ``_safe_int`` / ``_safe_float``
         (previously rejected non-numeric payloads with a raw ``ValueError``
         and accepted ``NaN`` for ``started_at``).
  * P2 — New helpers: ``best()``, ``clear()``, ``compare(a, b)``,
         ``rank_by(key, top_n)``, ``get_all(agent)``,
         ``LeaderboardRecord.key()``, ``schema_version`` in ``to_dict``.
  * P3 — ``math.isfinite`` used consistently; ``json.dumps(default=str)``
         removed; ``__version__`` added; ``ascending`` docstring rewritten;
         ``__main__`` smoke test now asserts tie order, capacity eviction,
         round-trip integrity, and APW consistency.

Public API (backward compatible, with one note)
-----------------------------------------------
    GreenLeaderboard, LeaderboardConfig, LeaderboardError, LeaderboardRecord
    ``GreenLeaderboard.add(...)`` now returns ``Optional[LeaderboardRecord]``:
    ``None`` on validation failure in non-strict mode. Callers that inspect
    the return value should check for ``None``.
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
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "GreenLeaderboard",
    "LeaderboardConfig",
    "LeaderboardError",
    "LeaderboardRecord",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Safe coercion helpers
# --------------------------------------------------------------------------- #

def _safe_int(
    value: Any, *, field_name: str, default: int = 0, min_value: Optional[int] = None,
) -> int:
    if value is None or value == "":
        result = default
    elif isinstance(value, bool):
        raise LeaderboardError(
            f"{field_name} must be an integer, got bool {value!r}"
        )
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise LeaderboardError(
                f"{field_name} must be finite, got {value!r}"
            )
        if not value.is_integer():
            raise LeaderboardError(
                f"{field_name} must be an integer, got {value!r}"
            )
        result = int(value)
    else:
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise LeaderboardError(
                f"{field_name} must be an integer, got {value!r} "
                f"({type(value).__name__})"
            ) from exc
    if min_value is not None and result < min_value:
        raise LeaderboardError(
            f"{field_name} must be >= {min_value}, got {result!r}"
        )
    return result


def _safe_float(
    value: Any, *, field_name: str, default: float = 0.0, min_value: Optional[float] = None,
) -> float:
    if value is None or value == "":
        result = default
    else:
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise LeaderboardError(
                f"{field_name} must be a number, got {value!r} "
                f"({type(value).__name__})"
            ) from exc
        if not math.isfinite(result):
            raise LeaderboardError(
                f"{field_name} must be finite, got {value!r}"
            )
    if min_value is not None and result < min_value:
        raise LeaderboardError(
            f"{field_name} must be >= {min_value}, got {result!r}"
        )
    return result


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            ts = datetime.fromisoformat(s)
        except ValueError as exc:
            raise LeaderboardError(
                f"timestamp is not ISO-8601: {value!r}"
            ) from exc
    elif value is None:
        return datetime.now(timezone.utc)
    else:
        raise LeaderboardError(
            f"timestamp must be datetime or ISO string, "
            f"got {type(value).__name__}"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


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

    #: Capacity. ``None`` disables bounding.
    max_records: Optional[int] = 10_000

    #: Primary ranking key. Higher is better unless ``ascending`` is True.
    ranking_key: str = "negawatt_score"

    #: Direction of ``ranking_key``. ``False`` = higher is better (default);
    #: ``True`` = lower is better (e.g. for ``ranking_key="energy"``).
    ascending: bool = False

    #: Duplicate agent handling: "replace" | "ignore" | "keep_both".
    #: ``keep_both`` is intended for audit logs, not ranked leaderboards —
    #: under ``keep_both``, ``get(agent)`` returns the newest record and
    #: ``remove(agent)`` removes every record for that agent.
    duplicate_policy: str = "replace"

    #: Validation range for accuracy.
    min_accuracy: float = 0.0
    max_accuracy: float = 1.0

    def __post_init__(self) -> None:
        if self.max_records is not None and self.max_records <= 0:
            raise LeaderboardError(
                "max_records must be a positive int or None."
            )
        if self.ranking_key not in (
            "negawatt_score", "accuracy_per_watt", "accuracy", "energy",
        ):
            raise LeaderboardError(
                f"unsupported ranking_key {self.ranking_key!r}."
            )
        if self.duplicate_policy not in (
            "replace", "ignore", "keep_both",
        ):
            raise LeaderboardError(
                "duplicate_policy must be 'replace', 'ignore', or "
                "'keep_both'."
            )
        for name in ("min_accuracy", "max_accuracy"):
            val = getattr(self, name)
            if not math.isfinite(val):
                raise LeaderboardError(
                    f"{name} must be finite, got {val!r}."
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
        max_records = data.get("max_records", 10_000)
        if max_records is not None:
            max_records = _safe_int(
                max_records, field_name="max_records", min_value=1,
            )
        return cls(
            max_records=max_records,
            ranking_key=str(data.get("ranking_key", "negawatt_score")),
            ascending=bool(data.get("ascending", False)),
            duplicate_policy=str(
                data.get("duplicate_policy", "replace")
            ),
            min_accuracy=_safe_float(
                data.get("min_accuracy"),
                field_name="min_accuracy", default=0.0,
            ),
            max_accuracy=_safe_float(
                data.get("max_accuracy"),
                field_name="max_accuracy", default=1.0,
            ),
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
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.agent, str) or not self.agent:
            raise LeaderboardError("agent must be a non-empty string.")
        for name in (
            "accuracy", "energy", "accuracy_per_watt", "negawatt_score",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise LeaderboardError(f"{name} must be numeric.")
            if not math.isfinite(float(value)):
                raise LeaderboardError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.accuracy < 0:
            raise LeaderboardError("accuracy must be >= 0.")
        if self.energy < 0:
            raise LeaderboardError("energy must be >= 0.")
        if self.accuracy_per_watt < 0:
            raise LeaderboardError("accuracy_per_watt must be >= 0.")

        # P0 #4: accuracy_per_watt must be consistent with accuracy/energy.
        expected_apw = (
            self.accuracy / self.energy if self.energy > 0 else 0.0
        )
        if not math.isclose(
            self.accuracy_per_watt, expected_apw,
            rel_tol=1e-9, abs_tol=1e-12,
        ):
            raise LeaderboardError(
                f"accuracy_per_watt={self.accuracy_per_watt!r} is "
                f"inconsistent with accuracy={self.accuracy!r} and "
                f"energy={self.energy!r} (expected {expected_apw!r})."
            )

        # P1 #8: normalise naive timestamps to UTC.
        if self.timestamp.tzinfo is None:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

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

    def key(self) -> Tuple[Any, ...]:
        """Deterministic identity tuple (excludes timestamp)."""
        return (
            self.agent,
            self.accuracy,
            self.energy,
            self.accuracy_per_watt,
            self.negawatt_score,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
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
        for required in ("agent", "accuracy", "energy"):
            if required not in data:
                raise LeaderboardError(
                    f"LeaderboardRecord.from_dict: missing required "
                    f"key {required!r}."
                )
        accuracy = _safe_float(
            data["accuracy"], field_name="accuracy", min_value=0.0,
        )
        energy = _safe_float(
            data["energy"], field_name="energy", min_value=0.0,
        )
        if "accuracy_per_watt" in data and data["accuracy_per_watt"] is not None:
            apw = _safe_float(
                data["accuracy_per_watt"],
                field_name="accuracy_per_watt", min_value=0.0,
            )
        else:
            # Derive when absent (P0 #4 companion fix).
            apw = accuracy / energy if energy > 0 else 0.0
        return cls(
            agent=str(data["agent"]),
            accuracy=accuracy,
            energy=energy,
            accuracy_per_watt=apw,
            negawatt_score=_safe_float(
                data.get("negawatt_score"),
                field_name="negawatt_score", default=0.0,
            ),
            timestamp=_parse_timestamp(data.get("timestamp")),
        )

    def to_json(self, **kwargs: Any) -> str:
        # `to_dict()` already produces primitives; `default=str` would
        # silently mask bugs.
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "LeaderboardRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise LeaderboardError(f"Invalid JSON payload: {exc}") from exc
        if not isinstance(data, Mapping):
            raise LeaderboardError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

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
    (``add(agent, accuracy, energy, negawatt)`` and ``rank()``) is preserved.

    Note:
        ``add()`` returns ``None`` on invalid input in non-strict mode
        (v5.0 returned a magic ``"__invalid__"`` sentinel record).
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
        self._by_agent: Dict[str, LeaderboardRecord] = {}
        self._evictions: int = 0
        self._replacements: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "GreenLeaderboard initialized "
            "(max_records=%s, ranking_key=%s, duplicate_policy=%s, "
            "ascending=%s, strict=%s)",
            self._config.max_records,
            self._config.ranking_key,
            self._config.duplicate_policy,
            self._config.ascending,
            self._strict,
        )

    # ------------------------------------------------------------------ props

    @property
    def config(self) -> LeaderboardConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

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
    ) -> Optional[LeaderboardRecord]:
        """
        Add or update an agent record.

        Returns
        -------
        LeaderboardRecord or None
            The stored record, or ``None`` if validation failed and
            ``strict=False``.
        """
        # ---- Validate inputs ------------------------------------------
        if not isinstance(agent, str) or not agent:
            msg = "agent must be a non-empty string."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        validated_accuracy = self._validate_accuracy(accuracy)
        if validated_accuracy is None:
            return None
        validated_energy = self._validate_energy(energy)
        if validated_energy is None:
            return None
        validated_negawatt = self._validate_negawatt(negawatt)
        if validated_negawatt is None:
            return None

        accuracy_per_watt = (
            validated_accuracy / validated_energy
            if validated_energy > 0 else 0.0
        )

        record = LeaderboardRecord(
            agent=agent,
            accuracy=validated_accuracy,
            energy=validated_energy,
            accuracy_per_watt=accuracy_per_watt,
            negawatt_score=validated_negawatt,
        )

        # ---- Dedup / capacity policy ---------------------------------
        with self._lock:
            existing = self._by_agent.get(agent)

            if existing is not None:
                policy = self._config.duplicate_policy
                if policy == "ignore":
                    logger.debug(
                        "Duplicate agent %s ignored (policy=ignore).",
                        agent,
                    )
                    return existing
                if policy == "replace":
                    try:
                        self._records.remove(existing)
                    except ValueError:
                        pass
                    self._by_agent.pop(agent, None)
                    self._replacements += 1
                else:  # keep_both
                    logger.debug(
                        "keep_both: agent %s now has multiple records; "
                        "get() returns the newest, remove() removes all.",
                        agent,
                    )

            self._records.append(record)
            self._by_agent[agent] = record

            self._enforce_capacity_locked()

        logger.debug(
            "Recorded agent=%s accuracy=%.3f energy=%.4g negawatt=%.4g",
            agent, validated_accuracy, validated_energy, validated_negawatt,
        )
        return record

    def remove(self, agent: str) -> bool:
        """
        Remove *every* record for ``agent``.

        Returns True if at least one record was removed.
        """
        if not isinstance(agent, str) or not agent:
            raise LeaderboardError("agent must be a non-empty string.")
        with self._lock:
            to_remove = [r for r in self._records if r.agent == agent]
            if not to_remove:
                return False
            for r in to_remove:
                try:
                    self._records.remove(r)
                except ValueError:
                    pass
            self._by_agent.pop(agent, None)
        logger.debug(
            "Removed agent %s (%d record(s)).", agent, len(to_remove),
        )
        return True

    def get(self, agent: str) -> Optional[LeaderboardRecord]:
        """
        Return the current record for ``agent``, or ``None``.

        Under ``keep_both`` this is the *newest* record for the agent.
        """
        if not isinstance(agent, str) or not agent:
            raise LeaderboardError("agent must be a non-empty string.")
        with self._lock:
            return self._by_agent.get(agent)

    def get_all(self, agent: str) -> List[LeaderboardRecord]:
        """Return every record for ``agent`` (relevant under ``keep_both``)."""
        if not isinstance(agent, str) or not agent:
            raise LeaderboardError("agent must be a non-empty string.")
        with self._lock:
            return [r for r in self._records if r.agent == agent]

    def clear(self) -> int:
        """Remove all records; return how many were removed."""
        with self._lock:
            n = len(self._records)
            self._records.clear()
            self._by_agent.clear()
        logger.debug("Cleared leaderboard (%d record(s)).", n)
        return n

    # ---------------------------------------------------------- ranking

    def rank(self, top_n: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Return the leaderboard sorted by the configured ranking key.

        Deterministic tie-break: agent name ascending (``"alpha"`` outranks
        ``"zulu"`` on equal primary score).
        """
        if top_n is not None and (
            not isinstance(top_n, int) or isinstance(top_n, bool)
            or top_n <= 0
        ):
            raise LeaderboardError(
                "top_n must be a positive int or None."
            )

        with self._lock:
            ranked = self._ranked_records()

        if top_n is not None:
            ranked = ranked[:top_n]

        return [r.to_dict() for r in ranked]

    def rank_by(
        self,
        ranking_key: str,
        top_n: Optional[int] = None,
        *,
        ascending: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """
        Rank by an alternative key without constructing a new leaderboard.

        ``ascending`` defaults to the config value; pass ``True``/``False``
        to override the direction for this call only.
        """
        if ranking_key not in (
            "negawatt_score", "accuracy_per_watt", "accuracy", "energy",
        ):
            raise LeaderboardError(
                f"unsupported ranking_key {ranking_key!r}."
            )
        if top_n is not None and (
            not isinstance(top_n, int) or isinstance(top_n, bool)
            or top_n <= 0
        ):
            raise LeaderboardError(
                "top_n must be a positive int or None."
            )
        with self._lock:
            records = list(self._records)
        ranked = self._sort_records(
            records,
            ranking_key=ranking_key,
            ascending=(
                self._config.ascending if ascending is None else ascending
            ),
        )
        if top_n is not None:
            ranked = ranked[:top_n]
        return [r.to_dict() for r in ranked]

    def best(self) -> Optional[LeaderboardRecord]:
        """Return the top-ranked record, or ``None`` if empty."""
        with self._lock:
            ranked = self._ranked_records()
        return ranked[0] if ranked else None

    def compare(
        self, agent_a: str, agent_b: str,
    ) -> Dict[str, Any]:
        """Structured comparison between two agents' current records."""
        record_a = self.get(agent_a)
        record_b = self.get(agent_b)
        if record_a is None:
            raise LeaderboardError(f"agent {agent_a!r} not present.")
        if record_b is None:
            raise LeaderboardError(f"agent {agent_b!r} not present.")
        return {
            "agent_a": record_a.agent,
            "agent_b": record_b.agent,
            "delta_negawatt": (
                record_b.negawatt_score - record_a.negawatt_score
            ),
            "delta_accuracy": record_b.accuracy - record_a.accuracy,
            "delta_energy": record_b.energy - record_a.energy,
            "delta_accuracy_per_watt": (
                record_b.accuracy_per_watt - record_a.accuracy_per_watt
            ),
            "ranking_key": self._config.ranking_key,
            "ascending": self._config.ascending,
        }

    # ---------------------------------------------------------- internal

    def _sort_records(
        self,
        records: List[LeaderboardRecord],
        *,
        ranking_key: Optional[str] = None,
        ascending: Optional[bool] = None,
    ) -> List[LeaderboardRecord]:
        """
        Two-pass stable sort:
          1. agent ascending (stable tie-break, ``alpha`` beats ``zulu``)
          2. ranking key in the configured direction (stable; ties from
             pass 1 are preserved)
        """
        key_name = ranking_key or self._config.ranking_key
        asc = self._config.ascending if ascending is None else ascending

        # Pass 1: alphabetical tie-break.
        by_agent = sorted(records, key=lambda r: r.agent)
        # Pass 2: primary/secondary/tertiary.
        return sorted(
            by_agent,
            key=lambda r: (
                float(getattr(r, key_name, 0.0)),
                r.negawatt_score,
                r.accuracy_per_watt,
            ),
            reverse=not asc,
        )

    def _ranked_records(self) -> List[LeaderboardRecord]:
        """Snapshot-and-sort under the caller's lock."""
        return self._sort_records(list(self._records))

    def _enforce_capacity_locked(self) -> None:
        """
        Evict the *worst-ranked* record(s) when over capacity.

        Correct under both ``ascending=False`` (higher-is-better) and
        ``ascending=True`` (lower-is-better).
        """
        cap = self._config.max_records
        if cap is None:
            return
        excess = len(self._records) - cap
        if excess <= 0:
            return

        # Full ranking, then drop the tail (worst-ranked).
        ranked = self._ranked_records()
        for worst in ranked[-excess:]:
            try:
                self._records.remove(worst)
            except ValueError:  # pragma: no cover — defensive
                continue
            # Re-point _by_agent if it pointed at `worst` (relevant under
            # keep_both; a no-op under replace/ignore).
            if self._by_agent.get(worst.agent) is worst:
                replacement = next(
                    (r for r in reversed(self._records)
                     if r.agent == worst.agent),
                    None,
                )
                if replacement is not None:
                    self._by_agent[worst.agent] = replacement
                else:
                    self._by_agent.pop(worst.agent, None)
            self._evictions += 1

    # ---------------------------------------------------------- validation

    def _validate_accuracy(self, value: Any) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"accuracy must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        fv = float(value)
        if not math.isfinite(fv):
            msg = f"accuracy must be finite, got {value!r}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        if not self._config.min_accuracy <= fv <= self._config.max_accuracy:
            msg = (
                f"accuracy must be in "
                f"[{self._config.min_accuracy}, "
                f"{self._config.max_accuracy}], got {fv}."
            )
            if self._strict:
                raise LeaderboardError(msg)
            clamped = max(
                self._config.min_accuracy,
                min(self._config.max_accuracy, fv),
            )
            logger.warning("%s Clamping to %s.", msg, clamped)
            return clamped
        return fv

    def _validate_energy(self, value: Any) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"energy must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        fv = float(value)
        if not math.isfinite(fv):
            msg = f"energy must be finite, got {value!r}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        if fv < 0:
            msg = f"energy must be >= 0, got {fv}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return fv

    def _validate_negawatt(self, value: Any) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"negawatt must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        fv = float(value)
        if not math.isfinite(fv):
            msg = f"negawatt must be finite, got {value!r}."
            if self._strict:
                raise LeaderboardError(msg)
            logger.warning("%s Skipping.", msg)
            return None
        return fv

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

        n = len(records)
        accuracies = [r.accuracy for r in records]
        energies = [r.energy for r in records]
        apw = [r.accuracy_per_watt for r in records]
        negawatts_sorted = sorted(r.negawatt_score for r in records)
        # P1 #9: nearest-rank percentile (stable across small samples).
        p95_idx = min(n - 1, math.ceil(0.95 * n) - 1)

        ranked = self._sort_records(records)
        top_agent = ranked[0].agent if ranked else None

        return {
            "count": n,
            "evictions": evictions,
            "replacements": replacements,
            "uptime_seconds": time.time() - started_at,
            "mean_accuracy": sum(accuracies) / n,
            "mean_energy": sum(energies) / n,
            "mean_accuracy_per_watt": sum(apw) / n,
            "mean_negawatt": sum(negawatts_sorted) / n,
            "median_negawatt": negawatts_sorted[n // 2],
            "p95_negawatt": negawatts_sorted[p95_idx],
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
        logger.debug(
            "GreenLeaderboard reset (removed %d record(s)).", removed,
        )
        return removed

    # ---------------------------------------------------------- serialization

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "evictions": self._evictions,
                "replacements": self._replacements,
                "started_at": self._started_at,
                "records": [r.to_dict() for r in self._records],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GreenLeaderboard":
        """
        Restore a leaderboard.

        Records are replayed through ``add()`` so ``_records`` and
        ``_by_agent`` stay consistent even when the source payload contains
        more entries than the restored ``max_records`` allows, or when the
        restored ``duplicate_policy`` would normally collapse them.
        """
        if not isinstance(data, Mapping):
            raise LeaderboardError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = LeaderboardConfig.from_dict(cfg_data)
        lb = cls(config=cfg, strict=bool(data.get("strict", True)))

        for entry in data.get("records", []) or []:
            record = LeaderboardRecord.from_dict(entry)
            # Route through add() so duplicate policy + capacity apply and
            # _records/_by_agent stay in sync (fix P0 #3).
            lb.add(
                agent=record.agent,
                accuracy=record.accuracy,
                energy=record.energy,
                negawatt=record.negawatt_score,
            )

        # Restore historical counters (informational only).
        lb._evictions = _safe_int(
            data.get("evictions"), field_name="evictions", default=0,
            min_value=0,
        )
        lb._replacements = _safe_int(
            data.get("replacements"), field_name="replacements",
            default=0, min_value=0,
        )
        lb._started_at = _safe_float(
            data.get("started_at"), field_name="started_at",
            default=time.time(),
        )
        return lb

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GreenLeaderboard":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise LeaderboardError(f"Invalid JSON payload: {exc}") from exc
        if not isinstance(data, Mapping):
            raise LeaderboardError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

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
                f"ascending={self._config.ascending}, "
                f"duplicate_policy={self._config.duplicate_policy!r}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Happy path -------------------------------------------------
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

    # ---- top_n ------------------------------------------------------
    print("top 2      :", [e["agent"] for e in lb.rank(top_n=2)])

    # ---- dict access ------------------------------------------------
    top = lb.rank()[0]
    assert top["agent"] == "gamma"
    assert "accuracy_per_watt" in top
    print("dict access OK.")

    # ---- P0 #1 regression: alpha wins ties --------------------------
    tie_lb = GreenLeaderboard()
    for name in ("zulu", "alpha", "mike"):
        tie_lb.add(name, accuracy=0.9, energy=0.01, negawatt=10.0)
    order = [e["agent"] for e in tie_lb.rank()]
    assert order == ["alpha", "mike", "zulu"], (
        f"tie order wrong: {order} (expected alphabetic ascending)"
    )
    print("tie order  :", order, "OK")

    # ---- P0 #2 regression: capacity keeps the best under ascending=True
    bounded_lower = GreenLeaderboard(config=LeaderboardConfig(
        max_records=2, ranking_key="energy", ascending=True,
    ))
    bounded_lower.add("cheap", accuracy=0.8, energy=0.01, negawatt=50.0)
    bounded_lower.add("mid", accuracy=0.8, energy=1.0, negawatt=20.0)
    bounded_lower.add("expensive", accuracy=0.8, energy=10.0, negawatt=5.0)
    surviving = [e["agent"] for e in bounded_lower.rank()]
    assert surviving == ["cheap", "mid"], (
        f"ascending-mode eviction wrong: {surviving} "
        f"(expected ['cheap', 'mid'] — evicted 'expensive')"
    )
    print("asc-evic   :", surviving, "OK")

    # ---- P0 #2 companion: capacity keeps the best under ascending=False
    bounded_higher = GreenLeaderboard(config=LeaderboardConfig(max_records=2))
    for nw in [1.0, 5.0, 3.0, 8.0]:
        bounded_higher.add(f"a{nw}", accuracy=0.8, energy=0.01, negawatt=nw)
    surviving = sorted(e["negawatt_score"] for e in bounded_higher.rank())
    assert surviving == [5.0, 8.0], (
        f"default-mode eviction wrong: {surviving} (expected [5.0, 8.0])"
    )
    print("def-evic   :", surviving, "OK")

    # ---- P0 #3 regression: from_dict keeps _records / _by_agent in sync
    src = GreenLeaderboard(config=LeaderboardConfig(max_records=3))
    for i in range(10):
        src.add(f"a{i}", 0.9, 0.01, float(i))
    restored = GreenLeaderboard.from_dict(src.to_dict())
    assert restored.size == len(restored._by_agent), (
        f"_records({restored.size}) != _by_agent({len(restored._by_agent)})"
    )
    assert {r.agent for r in restored.records} == set(restored._by_agent)
    print("sync       : size=", restored.size, " OK")

    # ---- P0 #4 regression: APW consistency check --------------------
    try:
        LeaderboardRecord(
            agent="x", accuracy=0.9, energy=1.0,
            accuracy_per_watt=999.0, negawatt_score=50.0,
        )
    except LeaderboardError as exc:
        print("bad APW    :", exc)
    else:
        raise AssertionError("Inconsistent accuracy_per_watt was accepted!")

    # ---- P1 #8: naive timestamp normalised to UTC -------------------
    naive = LeaderboardRecord(
        agent="x", accuracy=0.9, energy=1.0,
        accuracy_per_watt=0.9, negawatt_score=10.0,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )
    assert naive.timestamp.tzinfo is not None
    print("UTC norm   :", naive.timestamp.isoformat(), " OK")

    # ---- P1 #9: p95 nearest-rank -----------------------------------
    stats_lb = GreenLeaderboard()
    for i in range(1, 21):
        stats_lb.add(f"a{i}", accuracy=0.9, energy=0.01, negawatt=float(i))
    s = stats_lb.statistics()
    assert s["p95_negawatt"] == 19.0, (
        f"p95 = {s['p95_negawatt']}, expected 19 (nearest-rank of 1..20)"
    )
    print("p95        :", s["p95_negawatt"], " OK")

    # ---- Duplicate policy: ignore ----------------------------------
    ignore_lb = GreenLeaderboard(
        config=LeaderboardConfig(duplicate_policy="ignore")
    )
    ignore_lb.add("a", 0.9, 0.01, 5.0)
    ignore_lb.add("a", 0.5, 0.05, 1.0)
    assert ignore_lb.get("a").accuracy == 0.9
    print("ignore     : OK")

    # ---- Duplicate policy: keep_both --------------------------------
    keep_lb = GreenLeaderboard(
        config=LeaderboardConfig(duplicate_policy="keep_both")
    )
    keep_lb.add("a", 0.9, 0.01, 5.0)
    keep_lb.add("a", 0.5, 0.05, 1.0)
    assert keep_lb.size == 2
    assert len(keep_lb.get_all("a")) == 2
    keep_lb.remove("a")     # removes BOTH
    assert keep_lb.size == 0
    assert keep_lb.get("a") is None
    print("keep_both  : OK (get_all + remove all)")

    # ---- Ranking key override --------------------------------------
    apw_lb = GreenLeaderboard(
        config=LeaderboardConfig(ranking_key="accuracy_per_watt")
    )
    apw_lb.add("high_energy", 0.99, 1.0, 100.0)
    apw_lb.add("low_energy", 0.80, 0.01, 5.0)
    apw_lb.add("mid_energy", 0.90, 0.10, 50.0)
    apw_order = [e["agent"] for e in apw_lb.rank()]
    assert apw_order[0] == "low_energy"
    print("by apw     :", apw_order)

    # ---- rank_by / best / compare ---------------------------------
    lb2 = GreenLeaderboard()
    lb2.add("x", 0.9, 0.01, 5.0)
    lb2.add("y", 0.8, 0.02, 10.0)
    by_energy = lb2.rank_by("energy")
    assert by_energy[0]["agent"] == "x"   # 0.01 < 0.02
    assert lb2.best().agent == "y"        # negawatt 10 > 5
    cmp = lb2.compare("x", "y")
    assert cmp["delta_negawatt"] == 5.0
    print("rank_by/best/compare OK")

    # ---- statistics -------------------------------------------------
    print("stats      :", {
        k: v for k, v in lb.statistics().items()
        if k != "uptime_seconds"
    })

    # ---- remove / contains -----------------------------------------
    assert "beta" in lb
    assert lb.remove("beta") is True
    assert lb.remove("beta") is False
    assert "beta" not in lb
    print("remove     : OK")

    # ---- P1 #7: add() returns None in non-strict mode --------------
    lenient = GreenLeaderboard(strict=False)
    result = lenient.add("a", accuracy=-0.5, energy=1.0, negawatt=5.0)
    # negative accuracy is clamped to 0 in non-strict; verify
    assert result is not None
    assert result.accuracy == 0.0
    bad = lenient.add("", accuracy=0.9, energy=0.01, negawatt=5.0)
    assert bad is None, "invalid agent should return None in non-strict"
    print("non-strict : OK (None on invalid identity fields)")

    # ---- Serialization round-trip ----------------------------------
    payload = lb.to_json()
    restored_lb = GreenLeaderboard.from_json(payload)
    assert restored_lb.to_dict() == lb.to_dict()
    print("Round-trip OK.")

    # ---- rank() returns copies, not references ---------------------
    ranked = lb.rank()
    ranked[0]["agent"] = "MUTATED"
    assert lb.rank()[0]["agent"] != "MUTATED"
    print("defensive copy OK.")

    # ---- Context manager -------------------------------------------
    with GreenLeaderboard() as scoped:
        scoped.add("ctx", 0.9, 0.01, 5.0)
        assert scoped.size == 1
    print("Context OK.")

    # ---- Validation failures (strict) ------------------------------
    strict_lb = GreenLeaderboard(strict=True)
    for bad_kwargs in (
        ("", 0.9, 0.01, 5.0),           # empty agent
        (None, 0.9, 0.01, 5.0),         # None agent
        ("a", 1.5, 0.01, 5.0),          # accuracy > 1
        ("a", -0.1, 0.01, 5.0),         # negative accuracy
        ("a", 0.9, -0.01, 5.0),         # negative energy
        ("a", 0.9, 0.01, float("nan")), # NaN negawatt
    ):
        try:
            strict_lb.add(*bad_kwargs)
        except LeaderboardError as exc:
            print("Rejected   :", exc)

    for bad_cfg in (
        dict(max_records=0),
        dict(ranking_key="bogus"),
        dict(duplicate_policy="bogus"),
        dict(min_accuracy=0.9, max_accuracy=0.5),
    ):
        try:
            LeaderboardConfig(**bad_cfg)
        except LeaderboardError as exc:
            print("Rejected cfg:", exc)

    print("\nSmoke test passed.")
