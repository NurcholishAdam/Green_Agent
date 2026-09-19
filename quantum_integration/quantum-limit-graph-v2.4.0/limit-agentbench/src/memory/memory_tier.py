# src/memory/memory_tier.py

"""
Memory Tier
===========

Implements "Tier memory by value" — hot / warm / cold storage with explicit
promotion and demotion policies.

Enhancements
------------
- ``MemoryTier`` — str-enum (HOT / WARM / COLD) with class-level
  descriptions.
- ``MemoryTierConfig`` — frozen, validated, serializable: capacities,
  tier TTLs, per-kind TTLs (mirroring ``SupermemoryConfig.ttl_seconds``),
  hot-importance threshold, promotion threshold, TTL-preservation policy,
  truth-level vocabulary imported from ``memory_schemas``.
- ``TieredRecord`` — deeply frozen (``MappingProxyType``-wrapped
  payload), hashable, with ``observed_at``/``container_tag`` /
  ``policy_version`` / ``schema_version`` fields and full serialization.
- ``MemoryTierPolicy`` — deterministic tier selection, accepts both the
  string and the ``TruthLevel`` enum.
- ``MemoryTierManager`` — thread-safe LRU per tier, per-kind TTLs,
  demotion-instead-of-drop on capacity overflow, cross-tier uniqueness,
  ``peek()`` vs. ``get()``, ``store_record()`` for schema records,
  ``to_memory_dict()`` / ``to_episode_payload()`` bridges, optional
  ``EpisodicMemory`` / ``SupermemoryAdapter`` mirror backends,
  ``statistics()`` / ``reset()`` / ``close()``, context-manager support,
  async siblings, and full serialization symmetry.

Notes
-----
- TTL math uses ``time.time()`` (wall clock) for ``expires_at`` so
  records stay serializable across processes. A ``_mono_stored_at``
  field is also tracked to provide a monotonic baseline *within* a
  process; ``is_expired()`` prefers the monotonic path when available
  and falls back to wall clock for records reconstructed via
  ``from_dict``. This avoids the "cache pollution" bug where a stale
  cold record gets a fresh TTL on every read.
- Enabling a mirror backend (``episodic=`` / ``adapter=``) mirrors
  WARM/COLD writes best-effort. ``get()`` can optionally load a miss
  from the backend. The manager remains the primary in-memory index.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .memory_schemas import (
    DEFAULT_CONTAINER_TAG,
    DEFAULT_TRUTH_LEVELS,
    MemorySchemaError,
    TruthLevel,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .episodic_memory import EpisodicMemory
    from .supermemory_adapter import SupermemoryAdapter

logger = logging.getLogger(__name__)

__version__ = "6.0.0"

#: Version of the tiering contract itself.
SCHEMA_VERSION: int = 1


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
DEFAULT_HOT_TTL_SECONDS: int = 300                     # 5 minutes
DEFAULT_WARM_TTL_SECONDS: int = 7 * 24 * 3600          # 7 days
DEFAULT_COLD_TTL_SECONDS: int = 365 * 24 * 3600        # 1 year

#: Per-kind TTLs, mirroring ``SupermemoryConfig.ttl_seconds``.
DEFAULT_KIND_TTLS: Mapping[str, int] = MappingProxyType({
    "decision_outcome": 90 * 24 * 3600,
    "policy":           365 * 24 * 3600,
    "incident":         365 * 24 * 3600,
    "run":              180 * 24 * 3600,
    "grid_forecast":    6 * 3600,
    "thermal_state":    30 * 60,
    "connectivity":     5 * 60,
})

#: Fallback TTL per tier (used when a kind has no explicit TTL).
DEFAULT_TIER_TTLS: Mapping[str, int] = MappingProxyType({
    "hot":  DEFAULT_HOT_TTL_SECONDS,
    "warm": DEFAULT_WARM_TTL_SECONDS,
    "cold": DEFAULT_COLD_TTL_SECONDS,
})


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MemoryTierError(ValueError):
    """Base class for tiering problems."""


class MemoryTierInputError(MemoryTierError):
    """Invalid input to a public API."""


class MemoryTierConfigError(MemoryTierError):
    """Invalid configuration."""


class MemoryTierParseError(MemoryTierError):
    """Failed to parse a record from dict/JSON/episode payload."""


# --------------------------------------------------------------------------- #
# Validation helpers — mirror the other enhanced modules
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_nonneg(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _is_positive_finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp; tolerate ``Z`` and numeric epochs."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _normalize_string_tuple(
    value: Any,
    *,
    name: str,
    allow_empty: bool = True,
) -> Tuple[str, ...]:
    if isinstance(value, str) or not isinstance(
        value, (tuple, list, set, frozenset)
    ):
        raise MemoryTierConfigError(
            f"{name} must be a sequence of strings."
        )
    out: List[str] = []
    seen: set = set()
    for item in value:
        if not isinstance(item, str) or not item:
            raise MemoryTierConfigError(
                f"{name} entries must be non-empty strings."
            )
        if item in seen:
            raise MemoryTierConfigError(
                f"{name} contains duplicate {item!r}."
            )
        seen.add(item)
        out.append(item)
    if not out and not allow_empty:
        raise MemoryTierConfigError(f"{name} must be non-empty.")
    return tuple(out)


def _percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = max(
        0,
        min(
            len(ordered) - 1,
            int(round((pct / 100.0) * (len(ordered) - 1))),
        ),
    )
    return ordered[k]


def _extract_field(obj: Any, *names: str) -> Any:
    """Best-effort attribute accessor; returns ``None`` if nothing found."""
    for name in names:
        try:
            v = getattr(obj, name)
        except Exception:  # pragma: no cover - defensive
            continue
        if v is not None:
            return v
    return None


def _extract_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, str) and value:
        return value
    return None


def _derive_importance(record: Any) -> float:
    """Best-effort importance for a schema record."""
    q = _extract_field(record, "quality_score")
    if isinstance(q, (int, float)) and not isinstance(q, bool):
        return max(0.0, min(1.0, float(q)))
    sev = _extract_field(record, "severity")
    if isinstance(sev, Enum):
        sev = sev.value
    if isinstance(sev, str):
        return {
            "critical": 1.0, "high": 0.8, "medium": 0.5, "low": 0.2,
        }.get(sev, 0.5)
    return 0.5


# --------------------------------------------------------------------------- #
# Tier enum
# --------------------------------------------------------------------------- #
_TIER_DESCRIPTIONS: Mapping[str, str] = MappingProxyType({
    "hot": "In-process LRU, sub-millisecond access",
    "warm": "Supermemory container, millisecond access",
    "cold": "Archive, best-effort access",
})


class MemoryTier(str, Enum):
    """Storage tiers, ordered by access latency."""

    HOT = "hot"
    WARM = "warm"
    COLD = "cold"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value

    @property
    def description(self) -> str:
        return _TIER_DESCRIPTIONS[self.value]

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(m.value for m in cls)

    @classmethod
    def coerce(cls, value: Any) -> "MemoryTier":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value.lower())
            except ValueError as exc:
                raise MemoryTierInputError(
                    f"tier {value!r} is not one of {list(cls.values())}."
                ) from exc
        raise MemoryTierInputError(
            f"tier must be a str or MemoryTier, got "
            f"{type(value).__name__}."
        )

    def next_lower(self) -> Optional["MemoryTier"]:
        if self is MemoryTier.HOT:
            return MemoryTier.WARM
        if self is MemoryTier.WARM:
            return MemoryTier.COLD
        return None

    def next_higher(self) -> Optional["MemoryTier"]:
        if self is MemoryTier.COLD:
            return MemoryTier.WARM
        if self is MemoryTier.WARM:
            return MemoryTier.HOT
        return None


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryTierConfig:
    """Tunable parameters for tiering."""

    hot_capacity: int = 256
    warm_capacity: int = 4_096
    cold_capacity: int = 100_000

    hot_ttl_seconds: int = DEFAULT_HOT_TTL_SECONDS
    warm_ttl_seconds: int = DEFAULT_WARM_TTL_SECONDS
    cold_ttl_seconds: int = DEFAULT_COLD_TTL_SECONDS

    #: Per-kind TTLs (mirroring ``SupermemoryConfig.ttl_seconds``). A
    #: record's TTL depends on its kind, *not* on its tier.
    ttl_seconds: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_KIND_TTLS),
    )

    #: Records with importance >= this threshold start HOT if trusted.
    hot_importance_threshold: float = 0.8

    #: Minimum number of accesses before a WARM/COLD record promotes to HOT.
    hot_promotion_min_accesses: int = 2

    #: If True, promotion preserves the record's remaining TTL instead
    #: of resetting it. Demotion always resets the TTL to the new tier's
    #: per-kind TTL.
    promotion_preserves_ttl: bool = True

    #: Truth-level vocabulary (imported from ``memory_schemas``).
    trusted_truth_levels: Tuple[str, ...] = ("measured",)
    untrusted_truth_levels: Tuple[str, ...] = (
        "simulated", "user-reported",
    )

    #: Optional per-tier override for untrusted levels. ``None`` means
    #: untrusted records start at COLD.
    untrusted_min_tier: Optional[str] = None

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        # capacities
        for name in (
            "hot_capacity", "warm_capacity", "cold_capacity",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise MemoryTierConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )

        # tier TTLs
        for name in (
            "hot_ttl_seconds", "warm_ttl_seconds", "cold_ttl_seconds",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise MemoryTierConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )

        # per-kind TTLs
        if not isinstance(self.ttl_seconds, ABCMapping):
            raise MemoryTierConfigError("ttl_seconds must be a Mapping.")
        frozen_ttls: Dict[str, int] = {}
        for kind, seconds in self.ttl_seconds.items():
            if not isinstance(kind, str) or not kind:
                raise MemoryTierConfigError(
                    "ttl_seconds keys must be non-empty strings."
                )
            if not _is_real_int(seconds) or seconds <= 0:
                raise MemoryTierConfigError(
                    f"ttl_seconds[{kind!r}] must be a positive int."
                )
            frozen_ttls[kind] = int(seconds)
        object.__setattr__(
            self, "ttl_seconds", MappingProxyType(frozen_ttls),
        )

        # hot-importance threshold
        if not _is_finite_nonneg(self.hot_importance_threshold):
            raise MemoryTierConfigError(
                "hot_importance_threshold must be finite and >= 0."
            )
        if not 0.0 <= self.hot_importance_threshold <= 1.0:
            raise MemoryTierConfigError(
                "hot_importance_threshold must be in [0, 1]."
            )

        # promotion threshold
        if not _is_real_int(self.hot_promotion_min_accesses) or self.hot_promotion_min_accesses < 1:
            raise MemoryTierConfigError(
                "hot_promotion_min_accesses must be an int >= 1."
            )

        # promotion_preserves_ttl
        if not isinstance(self.promotion_preserves_ttl, bool):
            raise MemoryTierConfigError(
                "promotion_preserves_ttl must be a bool."
            )

        # truth-level vocabularies
        object.__setattr__(
            self,
            "trusted_truth_levels",
            _normalize_string_tuple(
                self.trusted_truth_levels,
                name="trusted_truth_levels",
                allow_empty=True,
            ),
        )
        object.__setattr__(
            self,
            "untrusted_truth_levels",
            _normalize_string_tuple(
                self.untrusted_truth_levels,
                name="untrusted_truth_levels",
                allow_empty=True,
            ),
        )
        overlap = set(self.trusted_truth_levels) & set(
            self.untrusted_truth_levels
        )
        if overlap:
            raise MemoryTierConfigError(
                f"trusted_truth_levels and untrusted_truth_levels "
                f"overlap: {sorted(overlap)}."
            )

        # unknown truth levels in either list.
        for level in (
            self.trusted_truth_levels + self.untrusted_truth_levels
        ):
            if level not in DEFAULT_TRUTH_LEVELS:
                raise MemoryTierConfigError(
                    f"truth_level {level!r} is not in "
                    f"{list(DEFAULT_TRUTH_LEVELS)}."
                )

        # untrusted_min_tier
        if self.untrusted_min_tier is not None:
            MemoryTier.coerce(self.untrusted_min_tier)
            object.__setattr__(
                self, "untrusted_min_tier",
                MemoryTier.coerce(self.untrusted_min_tier).value,
            )

    # ------------------------------------------------------------------ #
    def _tier_ttl(self, tier: MemoryTier) -> int:
        return {
            MemoryTier.HOT: self.hot_ttl_seconds,
            MemoryTier.WARM: self.warm_ttl_seconds,
            MemoryTier.COLD: self.cold_ttl_seconds,
        }[tier]

    def ttl_for(self, kind: str, tier: MemoryTier) -> int:
        """Return the effective TTL for a record of ``kind`` in ``tier``.

        Per-kind overrides win over tier defaults. This mirrors
        ``SupermemoryConfig.ttl_seconds`` / ``EpisodicMemory.ttl_seconds``.
        """
        if kind in self.ttl_seconds:
            return int(self.ttl_seconds[kind])
        return int(self._tier_ttl(tier))

    def capacity_for(self, tier: MemoryTier) -> int:
        return {
            MemoryTier.HOT: self.hot_capacity,
            MemoryTier.WARM: self.warm_capacity,
            MemoryTier.COLD: self.cold_capacity,
        }[tier]

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "hot_capacity": self.hot_capacity,
            "warm_capacity": self.warm_capacity,
            "cold_capacity": self.cold_capacity,
            "hot_ttl_seconds": self.hot_ttl_seconds,
            "warm_ttl_seconds": self.warm_ttl_seconds,
            "cold_ttl_seconds": self.cold_ttl_seconds,
            "ttl_seconds": dict(self.ttl_seconds),
            "hot_importance_threshold": self.hot_importance_threshold,
            "hot_promotion_min_accesses": self.hot_promotion_min_accesses,
            "promotion_preserves_ttl": self.promotion_preserves_ttl,
            "trusted_truth_levels": list(self.trusted_truth_levels),
            "untrusted_truth_levels": list(self.untrusted_truth_levels),
            "untrusted_min_tier": self.untrusted_min_tier,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "MemoryTierConfig":
        if not isinstance(data, ABCMapping):
            raise MemoryTierConfigError(
                "MemoryTierConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise MemoryTierConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k in ("trusted_truth_levels", "untrusted_truth_levels"):
                if isinstance(v, str) or not isinstance(
                    v, (list, tuple, set, frozenset)
                ):
                    raise MemoryTierConfigError(
                        f"{k} must be a sequence of strings."
                    )
                kwargs[k] = tuple(v)
            elif k == "ttl_seconds":
                if not isinstance(v, ABCMapping):
                    raise MemoryTierConfigError(
                        "ttl_seconds must be a Mapping."
                    )
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except MemoryTierError:
            raise
        except (TypeError, ValueError) as exc:
            raise MemoryTierConfigError(
                f"failed to build MemoryTierConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "MemoryTierConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryTierConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryTierConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> "MemoryTierConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise MemoryTierConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(self, other: "MemoryTierConfig") -> "MemoryTierConfig":
        defaults = MemoryTierConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    def __hash__(self) -> int:
        return hash((
            self.hot_capacity, self.warm_capacity, self.cold_capacity,
            self.hot_ttl_seconds, self.warm_ttl_seconds,
            self.cold_ttl_seconds,
            tuple(sorted(self.ttl_seconds.items())),
            self.hot_importance_threshold,
            self.hot_promotion_min_accesses,
            self.promotion_preserves_ttl,
            self.trusted_truth_levels, self.untrusted_truth_levels,
            self.untrusted_min_tier,
        ))

    def __repr__(self) -> str:
        return (
            "MemoryTierConfig("
            f"hot={self.hot_capacity}, "
            f"warm={self.warm_capacity}, "
            f"cold={self.cold_capacity})"
        )


# --------------------------------------------------------------------------- #
# Record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TieredRecord:
    """Frozen record stored in a tier."""

    record_id: str
    kind: str
    payload: Mapping[str, Any]
    tier: MemoryTier
    importance: float
    truth_level: str
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    container_tag: str = DEFAULT_CONTAINER_TAG
    policy_version: Optional[str] = None
    stored_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    expires_at: float = 0.0
    ttl_seconds: int = 0
    access_count: int = 0
    schema_version: int = SCHEMA_VERSION

    #: Monotonic baseline used for in-process TTL math. Excluded from
    #: serialization (0.0 means "fall back to wall clock").
    _mono_stored_at: float = field(default=0.0, compare=False, repr=False)

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("record_id", "kind", "truth_level", "container_tag"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise MemoryTierInputError(
                    f"{name} must be a non-empty string."
                )
        object.__setattr__(
            self, "tier", MemoryTier.coerce(self.tier),
        )
        object.__setattr__(
            self, "truth_level", TruthLevel.coerce(self.truth_level).value,
        )

        if not isinstance(self.payload, ABCMapping):
            raise MemoryTierInputError("payload must be a Mapping.")
        # Freeze payload; JSON-serializability is enforced lazily to
        # keep construction cheap — the manager will serialize when
        # a backend is configured.
        try:
            object.__setattr__(
                self, "payload",
                MappingProxyType(dict(self.payload)),
            )
        except (TypeError, ValueError) as exc:
            raise MemoryTierInputError(
                f"payload could not be frozen: {exc}"
            ) from exc

        if not _is_finite_nonneg(self.importance):
            raise MemoryTierInputError(
                "importance must be finite and >= 0."
            )
        if self.importance > 1.0:
            raise MemoryTierInputError(
                "importance must be <= 1."
            )

        if self.policy_version is not None:
            if not isinstance(self.policy_version, str) or not self.policy_version:
                raise MemoryTierInputError(
                    "policy_version must be None or a non-empty string."
                )

        if not isinstance(self.observed_at, datetime):
            raise MemoryTierInputError("observed_at must be a datetime.")
        if self.observed_at.tzinfo is None:
            object.__setattr__(
                self, "observed_at",
                self.observed_at.replace(tzinfo=timezone.utc),
            )

        for name in ("stored_at", "last_accessed", "expires_at"):
            v = getattr(self, name)
            if not _is_finite_nonneg(v):
                raise MemoryTierInputError(
                    f"{name} must be finite and >= 0."
                )
        if not _is_real_int(self.ttl_seconds) or self.ttl_seconds < 0:
            raise MemoryTierInputError(
                "ttl_seconds must be a non-negative int."
            )
        if not _is_real_int(self.access_count) or self.access_count < 0:
            raise MemoryTierInputError(
                "access_count must be a non-negative int."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise MemoryTierInputError(
                "schema_version must be a positive int."
            )
        if not _is_finite_nonneg(self._mono_stored_at):
            raise MemoryTierInputError(
                "_mono_stored_at must be finite and >= 0."
            )

    # ------------------------------------------------------------------ #
    # Accessors
    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return self.record_id

    @property
    def age_seconds(self) -> float:
        return max(0.0, time.time() - self.stored_at)

    # ------------------------------------------------------------------ #
    # Expiry
    # ------------------------------------------------------------------ #
    def is_expired(
        self,
        *,
        now_mono: Optional[float] = None,
        now_wall: Optional[float] = None,
    ) -> bool:
        """Return True if the record has outlived its TTL.

        Prefers the monotonic baseline when present (in-process records);
        falls back to wall clock for records reconstructed via
        ``from_dict`` / ``from_episode_payload``.
        """
        if self.ttl_seconds <= 0 and self.expires_at <= 0:
            return False
        if self._mono_stored_at > 0 and self.ttl_seconds > 0:
            mono = time.monotonic() if now_mono is None else now_mono
            return (mono - self._mono_stored_at) >= self.ttl_seconds
        if self.expires_at > 0:
            wall = time.time() if now_wall is None else now_wall
            return wall >= self.expires_at
        return False

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "kind": self.kind,
            "payload": dict(self.payload),
            "tier": self.tier.value,
            "importance": self.importance,
            "truth_level": self.truth_level,
            "observed_at": self.observed_at.isoformat(),
            "container_tag": self.container_tag,
            "policy_version": self.policy_version,
            "stored_at": self.stored_at,
            "last_accessed": self.last_accessed,
            "expires_at": self.expires_at,
            "ttl_seconds": self.ttl_seconds,
            "access_count": self.access_count,
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TieredRecord":
        if not isinstance(data, ABCMapping):
            raise MemoryTierParseError(
                "TieredRecord.from_dict expects a Mapping."
            )
        try:
            return cls(
                record_id=data["record_id"],
                kind=data["kind"],
                payload=dict(data.get("payload", {})),
                tier=MemoryTier.coerce(data["tier"]),
                importance=data.get("importance", 0.0),
                truth_level=data.get("truth_level", "estimated"),
                observed_at=_parse_iso_datetime(data.get("observed_at"))
                    or datetime.now(timezone.utc),
                container_tag=data.get(
                    "container_tag", DEFAULT_CONTAINER_TAG,
                ),
                policy_version=data.get("policy_version"),
                stored_at=data.get("stored_at", time.time()),
                last_accessed=data.get("last_accessed", time.time()),
                expires_at=data.get("expires_at", 0.0),
                ttl_seconds=data.get("ttl_seconds", 0),
                access_count=data.get("access_count", 0),
                schema_version=data.get("schema_version", SCHEMA_VERSION),
                _mono_stored_at=0.0,  # not serialized
            )
        except KeyError as exc:
            raise MemoryTierParseError(
                f"TieredRecord.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "TieredRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryTierParseError(
                f"TieredRecord.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Supermemory / EpisodicMemory bridges
    # ------------------------------------------------------------------ #
    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a payload shaped for ``EpisodicMemory.store`` /
        ``SupermemoryAdapter.remember``.
        """
        tag = (
            container_tag
            if container_tag is not None
            else (self.container_tag or DEFAULT_CONTAINER_TAG)
        )
        if not isinstance(tag, str) or not tag:
            raise MemoryTierInputError(
                "container_tag must be a non-empty string."
            )
        if content is None:
            try:
                content = json.dumps(
                    dict(self.payload), default=str, sort_keys=True,
                )
            except (TypeError, ValueError):
                content = str(self.payload)
        meta = {
            "kind": self.kind,
            "record_id": self.record_id,
            "truth_level": self.truth_level,
            "container_tag": tag,
            "observed_at": self.observed_at.isoformat(),
            "tier": self.tier.value,
            "importance": self.importance,
            "policy_version": self.policy_version,
            "schema_version": self.schema_version,
            "ttl_seconds": self.ttl_seconds,
        }
        return {
            "content": content,
            "container_tag": tag,
            "metadata": meta,
        }

    @classmethod
    def from_episode_payload(
        cls,
        data: Mapping[str, Any],
        *,
        tier: Optional[MemoryTier] = None,
        importance: Optional[float] = None,
    ) -> "TieredRecord":
        """Reconstruct a record from a payload produced by
        ``to_episode_payload`` or by a schema record's own bridge.
        """
        if not isinstance(data, ABCMapping):
            raise MemoryTierParseError(
                "TieredRecord.from_episode_payload expects a Mapping."
            )
        meta = data.get("metadata") or {}
        if not isinstance(meta, ABCMapping):
            meta = {}
        record_id = (
            meta.get("record_id")
            or meta.get("run_id")
            or meta.get("incident_id")
            or meta.get("id")
        )
        if not record_id:
            raise MemoryTierParseError(
                "episode payload missing a stable record id."
            )
        kind = str(meta.get("kind") or "unknown")
        truth_level = str(meta.get("truth_level") or "estimated")
        container_tag = str(
            data.get("container_tag")
            or meta.get("container_tag")
            or DEFAULT_CONTAINER_TAG
        )
        observed_at = (
            _parse_iso_datetime(meta.get("observed_at"))
            or datetime.now(timezone.utc)
        )
        policy_version = meta.get("policy_version")
        resolved_tier = (
            MemoryTier.coerce(tier) if tier is not None
            else MemoryTier.coerce(meta.get("tier") or "cold")
        )
        resolved_importance = (
            float(importance) if importance is not None
            else float(meta.get("importance", 0.5))
        )
        ttl_seconds = int(meta.get("ttl_seconds", 0))
        content = data.get("content")
        payload: Dict[str, Any] = dict(meta)
        if content is not None:
            payload["content"] = content
        return cls(
            record_id=str(record_id),
            kind=kind,
            payload=payload,
            tier=resolved_tier,
            importance=resolved_importance,
            truth_level=truth_level,
            observed_at=observed_at,
            container_tag=container_tag,
            policy_version=policy_version if isinstance(policy_version, str) else None,
            stored_at=time.time(),
            last_accessed=time.time(),
            expires_at=0.0,
            ttl_seconds=ttl_seconds,
            access_count=0,
            schema_version=int(meta.get("schema_version", SCHEMA_VERSION)),
            _mono_stored_at=0.0,
        )

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``."""
        try:
            content = json.dumps(
                dict(self.payload), default=str, sort_keys=True,
            )
        except (TypeError, ValueError):
            content = str(self.payload)
        return {
            "id": self.record_id,
            "content": content,
            "metadata": {
                "kind": self.kind,
                "record_id": self.record_id,
                "truth_level": self.truth_level,
                "container_tag": self.container_tag,
                "observed_at": self.observed_at.isoformat(),
                "policy_version": self.policy_version,
                "tier": self.tier.value,
                "importance": self.importance,
            },
        }

    # ------------------------------------------------------------------ #
    def with_tier(
        self,
        new_tier: MemoryTier,
        *,
        ttl_seconds: int,
        now: Optional[float] = None,
        preserve_ttl: bool = False,
    ) -> "TieredRecord":
        """Return a copy moved to ``new_tier`` with recomputed TTL."""
        tier = MemoryTier.coerce(new_tier)
        now_wall = time.time() if now is None else now
        if preserve_ttl:
            new_ttl = self.ttl_seconds or ttl_seconds
            new_expires = self.expires_at or (now_wall + new_ttl)
        else:
            new_ttl = int(ttl_seconds)
            new_expires = now_wall + new_ttl
        return TieredRecord(
            record_id=self.record_id,
            kind=self.kind,
            payload=dict(self.payload),
            tier=tier,
            importance=self.importance,
            truth_level=self.truth_level,
            observed_at=self.observed_at,
            container_tag=self.container_tag,
            policy_version=self.policy_version,
            stored_at=self.stored_at,
            last_accessed=self.last_accessed,
            expires_at=new_expires,
            ttl_seconds=new_ttl,
            access_count=self.access_count,
            schema_version=self.schema_version,
            _mono_stored_at=(
                self._mono_stored_at
                if preserve_ttl and self._mono_stored_at > 0
                else time.monotonic()
            ),
        )

    def touch(self, *, now: Optional[float] = None) -> "TieredRecord":
        """Return a copy with updated ``last_accessed`` / ``access_count``."""
        now_wall = time.time() if now is None else now
        return TieredRecord(
            record_id=self.record_id,
            kind=self.kind,
            payload=dict(self.payload),
            tier=self.tier,
            importance=self.importance,
            truth_level=self.truth_level,
            observed_at=self.observed_at,
            container_tag=self.container_tag,
            policy_version=self.policy_version,
            stored_at=self.stored_at,
            last_accessed=now_wall,
            expires_at=self.expires_at,
            ttl_seconds=self.ttl_seconds,
            access_count=self.access_count + 1,
            schema_version=self.schema_version,
            _mono_stored_at=self._mono_stored_at,
        )

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        try:
            payload_hash = hash(tuple(sorted(self.payload.items())))
        except TypeError:
            payload_hash = hash(
                json.dumps(dict(self.payload), default=str, sort_keys=True)
            )
        return hash((
            self.record_id, self.kind, payload_hash, self.tier,
            self.importance, self.truth_level, self.observed_at,
            self.container_tag, self.policy_version, self.stored_at,
            self.last_accessed, self.expires_at, self.ttl_seconds,
            self.access_count, self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"TieredRecord(record_id={self.record_id!r}, "
            f"kind={self.kind!r}, tier={self.tier.value}, "
            f"access_count={self.access_count})"
        )


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
class MemoryTierPolicy:
    """Deterministic tier selection based on record metadata."""

    def __init__(self, *, config: Optional[MemoryTierConfig] = None) -> None:
        self._config = config or MemoryTierConfig()

    @property
    def config(self) -> MemoryTierConfig:
        return self._config

    # ------------------------------------------------------------------ #
    def select_tier(
        self,
        *,
        importance: float,
        truth_level: Any,
    ) -> MemoryTier:
        """Return the initial tier for a new record."""
        if not _is_finite_nonneg(importance):
            raise MemoryTierInputError(
                "importance must be finite and >= 0."
            )
        if importance > 1.0:
            raise MemoryTierInputError("importance must be <= 1.")
        try:
            level = TruthLevel.coerce(truth_level).value
        except MemorySchemaError as exc:
            raise MemoryTierInputError(
                f"truth_level invalid: {exc}"
            ) from exc

        # Untrusted levels start at COLD (unless a floor is configured).
        if level in self._config.untrusted_truth_levels:
            if self._config.untrusted_min_tier is not None:
                return MemoryTier.coerce(self._config.untrusted_min_tier)
            return MemoryTier.COLD

        # High-importance trusted records start HOT.
        if (
            importance >= self._config.hot_importance_threshold
            and level in self._config.trusted_truth_levels
        ):
            return MemoryTier.HOT

        # Everything else starts WARM.
        return MemoryTier.WARM

    def select_tier_for_record(self, record: Any) -> MemoryTier:
        """Return the initial tier for any of the schema records."""
        importance = _derive_importance(record)
        truth_level = (
            _extract_str(_extract_field(record, "truth_level"))
            or "estimated"
        )
        return self.select_tier(
            importance=importance, truth_level=truth_level,
        )


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #
class MemoryTierManager:
    """Thread-safe manager for hot / warm / cold tiers."""

    _LATENCY_RING_SIZE: int = 200

    def __init__(
        self,
        *,
        config: Optional[MemoryTierConfig] = None,
        policy: Optional[MemoryTierPolicy] = None,
        strict: bool = True,
        episodic: Optional[Any] = None,
        adapter: Optional[Any] = None,
    ) -> None:
        self._config = config or MemoryTierConfig()
        self._policy = policy or MemoryTierPolicy(config=self._config)
        self._strict = bool(strict)

        # Duck-typed backends (no hard import required).
        self._episodic = episodic
        self._adapter = adapter
        self._validate_backends()

        self._lock = threading.RLock()
        self._tiers: Dict[MemoryTier, "OrderedDict[str, TieredRecord]"] = {
            MemoryTier.HOT: OrderedDict(),
            MemoryTier.WARM: OrderedDict(),
            MemoryTier.COLD: OrderedDict(),
        }

        # Counters (all guarded by ``_lock``).
        self._hits = 0
        self._misses = 0
        self._promotions = 0
        self._demotions = 0
        self._evictions = 0
        self._expirations = 0
        self._stores = 0
        self._backend_writes = 0
        self._backend_errors = 0
        self._last_error: Optional[str] = None
        self._latency_ring: Deque[float] = deque(
            maxlen=self._LATENCY_RING_SIZE
        )
        self._started_at = time.monotonic()

    # ------------------------------------------------------------------ #
    def _validate_backends(self) -> None:
        if self._episodic is not None:
            for name in ("store",):
                if not callable(getattr(self._episodic, name, None)):
                    raise MemoryTierConfigError(
                        f"episodic backend lacks callable {name!r}."
                    )
        if self._adapter is not None:
            for name in ("remember",):
                if not callable(getattr(self._adapter, name, None)):
                    raise MemoryTierConfigError(
                        f"adapter backend lacks callable {name!r}."
                    )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MemoryTierConfig:
        return self._config

    @property
    def policy(self) -> MemoryTierPolicy:
        return self._policy

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def episodic(self) -> Optional[Any]:
        return self._episodic

    @property
    def adapter(self) -> Optional[Any]:
        return self._adapter

    def size(self, tier: Optional[MemoryTier] = None) -> int:
        with self._lock:
            if tier is None:
                return sum(len(b) for b in self._tiers.values())
            return len(self._tiers[MemoryTier.coerce(tier)])

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, record_id: object) -> bool:
        if not isinstance(record_id, str):
            return False
        return self.tier_of(record_id) is not None

    def __iter__(self) -> Iterator[TieredRecord]:
        with self._lock:
            snapshot = [
                dict(b) for b in self._tiers.values()
            ]
        for bucket in snapshot:
            for record in bucket.values():
                yield record

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Best-effort backend flush. Safe to call multiple times."""
        ep = self._episodic
        if ep is not None:
            close = getattr(ep, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:  # pragma: no cover - backend
                    logger.warning("episodic.close() failed: %s", exc)
        ad = self._adapter
        if ad is not None:
            close = getattr(ad, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:  # pragma: no cover - backend
                    logger.warning("adapter.close() failed: %s", exc)

    def __enter__(self) -> "MemoryTierManager":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------- public API
    def store(
        self,
        *,
        record_id: str,
        kind: str,
        payload: Mapping[str, Any],
        importance: float = 0.5,
        truth_level: Any = "estimated",
        tier: Optional[Any] = None,
        observed_at: Optional[Any] = None,
        container_tag: Optional[str] = None,
        policy_version: Optional[str] = None,
    ) -> TieredRecord:
        """Store a record, choosing its tier automatically if not given."""
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierInputError(
                "record_id must be a non-empty string."
            )
        if not isinstance(kind, str) or not kind:
            raise MemoryTierInputError(
                "kind must be a non-empty string."
            )
        if not isinstance(payload, ABCMapping):
            raise MemoryTierInputError("payload must be a Mapping.")

        chosen = (
            MemoryTier.coerce(tier) if tier is not None
            else self._policy.select_tier(
                importance=importance, truth_level=truth_level,
            )
        )

        truth = TruthLevel.coerce(truth_level).value
        tag = container_tag or DEFAULT_CONTAINER_TAG
        if not isinstance(tag, str) or not tag:
            raise MemoryTierInputError(
                "container_tag must be a non-empty string."
            )
        obs = (
            _parse_iso_datetime(observed_at)
            if observed_at is not None
            else datetime.now(timezone.utc)
        )
        if obs is None:
            raise MemoryTierInputError(
                "observed_at could not be parsed."
            )

        ttl_seconds = self._config.ttl_for(kind, chosen)
        now_wall = time.time()
        now_mono = time.monotonic()

        record = TieredRecord(
            record_id=record_id,
            kind=kind,
            payload=dict(payload),
            tier=chosen,
            importance=float(importance),
            truth_level=truth,
            observed_at=obs,
            container_tag=tag,
            policy_version=policy_version,
            stored_at=now_wall,
            last_accessed=now_wall,
            expires_at=now_wall + ttl_seconds,
            ttl_seconds=ttl_seconds,
            access_count=0,
            schema_version=SCHEMA_VERSION,
            _mono_stored_at=now_mono,
        )

        with self._lock:
            # Enforce cross-tier uniqueness: remove any previous copy.
            self._remove_from_all_tiers_locked(record_id)
            self._tiers[chosen][record_id] = record
            self._stores += 1
            self._enforce_capacity_locked(chosen)

        # Best-effort mirror to backends.
        if chosen in (MemoryTier.WARM, MemoryTier.COLD):
            self._mirror_to_backends(record)

        logger.debug(
            "Stored %s in %s (kind=%s, ttl=%ds).",
            record_id, chosen.value, kind, ttl_seconds,
        )
        return record

    def store_record(
        self,
        record: Any,
        *,
        tier: Optional[Any] = None,
        importance: Optional[float] = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> TieredRecord:
        """Store any schema record (``DecisionRecord``, ``PolicyRecord``,
        ``IncidentRecord``, ``OutcomeRecord``) or any object exposing
        ``id`` / ``kind`` / ``truth_level`` / ``to_dict``.
        """
        record_id = _extract_str(
            _extract_field(record, "id", "record_id"),
        )
        if not record_id:
            raise MemoryTierInputError(
                "record must expose a stable 'id' or 'record_id'."
            )
        kind = (
            _extract_str(_extract_field(record, "TTL_KIND"))
            or _extract_str(_extract_field(record, "kind"))
            or "unknown"
        )
        truth_raw = _extract_field(record, "truth_level")
        truth = _extract_str(truth_raw) or "estimated"

        obs_dt = _extract_field(record, "observed_at", "published_at")
        if isinstance(obs_dt, str):
            obs_dt = _parse_iso_datetime(obs_dt)

        tag = _extract_str(
            _extract_field(record, "container_tag"),
        ) or DEFAULT_CONTAINER_TAG
        policy_version = _extract_str(
            _extract_field(record, "policy_version"),
        )

        imp = (
            float(importance)
            if importance is not None
            else _derive_importance(record)
        )

        if payload is None:
            dumper = getattr(record, "to_dict", None)
            if callable(dumper):
                try:
                    payload = dumper()
                except Exception as exc:
                    raise MemoryTierInputError(
                        f"record.to_dict() failed: {exc}"
                    ) from exc
            else:
                payload = {"value": str(record)}

        return self.store(
            record_id=record_id,
            kind=kind,
            payload=payload,
            importance=imp,
            truth_level=truth,
            tier=tier,
            observed_at=obs_dt,
            container_tag=tag,
            policy_version=policy_version,
        )

    def get(
        self,
        record_id: str,
        *,
        promote: Optional[bool] = None,
        load_from_backend: bool = True,
    ) -> Optional[TieredRecord]:
        """Return a record.

        Promotes to HOT only when the access-count threshold is met and
        ``promote`` is not explicitly disabled. TTL is preserved on
        promotion by default (see ``promotion_preserves_ttl``).

        Parameters
        ----------
        promote : bool, optional
            ``None`` (default) uses ``hot_promotion_min_accesses``.
            ``False`` behaves like ``peek()``.
            ``True`` forces promotion regardless of the threshold.
        load_from_backend : bool
            If True and a backend is configured, attempt to load the
            record from the episodic/adapter mirror on a miss.
        """
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierInputError(
                "record_id must be a non-empty string."
            )

        start = time.perf_counter()
        with self._lock:
            found = self._lookup_locked(record_id)
            if found is None:
                if not load_from_backend:
                    self._misses += 1
                    self._record_latency(start)
                    return None
                record = self._load_from_backend(record_id)
                if record is None:
                    self._misses += 1
                    self._record_latency(start)
                    return None
                # Cache in the appropriate tier based on its metadata.
                self._tiers[record.tier][record.record_id] = record
                found = (record.tier, record)

            current_tier, record = found
            if record.is_expired():
                self._tiers[current_tier].pop(record_id, None)
                self._expirations += 1
                self._misses += 1
                self._record_latency(start)
                return None

            # Touch in place.
            touched = record.touch()
            self._tiers[current_tier].move_to_end(record_id)
            self._tiers[current_tier][record_id] = touched
            self._hits += 1

            # Decide on promotion.
            should_promote = self._should_promote(touched, current_tier, promote)
            if should_promote:
                new_tier = (
                    touched.tier.next_higher()
                    if touched.tier != MemoryTier.HOT
                    else MemoryTier.HOT
                )
                # Skip intermediate tiers when the threshold is very low
                # and the record is high-importance.
                if (
                    touched.tier == MemoryTier.COLD
                    and self._config.hot_promotion_min_accesses <= 1
                ):
                    new_tier = MemoryTier.HOT
                if new_tier is not None:
                    self._tiers[current_tier].pop(record_id, None)
                    promoted = self._move_to_tier_locked(
                        touched, new_tier,
                        preserve_ttl=self._config.promotion_preserves_ttl,
                    )
                    self._promotions += 1
                    self._enforce_capacity_locked(new_tier)
                    self._record_latency(start)
                    return promoted

            self._record_latency(start)
            return touched

    def peek(self, record_id: str) -> Optional[TieredRecord]:
        """Return a record without promoting it to a higher tier.

        Still touches ``last_accessed`` / ``access_count`` so that
        ``get()`` promotion thresholds can be honored, but never moves
        the record between tiers.
        """
        return self.get(record_id, promote=False, load_from_backend=False)

    def promote(
        self,
        record_id: str,
        *,
        to_tier: Optional[Any] = None,
    ) -> Optional[TieredRecord]:
        """Force promotion to the next-higher tier (or to ``to_tier``)."""
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierInputError(
                "record_id must be a non-empty string."
            )
        with self._lock:
            found = self._lookup_locked(record_id)
            if found is None:
                return None
            current_tier, record = found
            target = (
                MemoryTier.coerce(to_tier) if to_tier is not None
                else record.tier.next_higher()
            )
            if target is None:
                return record
            self._tiers[current_tier].pop(record_id, None)
            moved = self._move_to_tier_locked(
                record, target,
                preserve_ttl=self._config.promotion_preserves_ttl,
            )
            self._promotions += 1
            self._enforce_capacity_locked(target)
            return moved

    def demote(
        self,
        record_id: str,
        *,
        to_tier: Optional[Any] = None,
    ) -> Optional[TieredRecord]:
        """Force demotion to the next-lower tier (or to ``to_tier``)."""
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierInputError(
                "record_id must be a non-empty string."
            )
        with self._lock:
            found = self._lookup_locked(record_id)
            if found is None:
                return None
            current_tier, record = found
            target = (
                MemoryTier.coerce(to_tier) if to_tier is not None
                else record.tier.next_lower()
            )
            if target is None:
                return record  # already COLD
            self._tiers[current_tier].pop(record_id, None)
            moved = self._move_to_tier_locked(
                record, target, preserve_ttl=False,
            )
            self._demotions += 1
            self._enforce_capacity_locked(target)
            # Mirror demotions to backends too.
            if target in (MemoryTier.WARM, MemoryTier.COLD):
                self._mirror_to_backends(moved)
            return moved

    def expire(self) -> int:
        """Sweep all tiers, dropping expired records. Returns count removed."""
        now_wall = time.time()
        now_mono = time.monotonic()
        removed = 0
        with self._lock:
            for tier, bucket in self._tiers.items():
                expired = [
                    k for k, v in bucket.items()
                    if v.is_expired(now_mono=now_mono, now_wall=now_wall)
                ]
                for k in expired:
                    bucket.pop(k, None)
                    removed += 1
                    # Best-effort: also drop from backends.
                    self._delete_from_backends(k)
            self._expirations += removed
        if removed:
            logger.info("Expired %d record(s) from tiers.", removed)
        return removed

    def tier_of(self, record_id: str) -> Optional[MemoryTier]:
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierInputError(
                "record_id must be a non-empty string."
            )
        with self._lock:
            for tier in (MemoryTier.HOT, MemoryTier.WARM, MemoryTier.COLD):
                if record_id in self._tiers[tier]:
                    return tier
        return None

    def iter_records(
        self, tier: Optional[Any] = None,
    ) -> Iterator[TieredRecord]:
        with self._lock:
            if tier is not None:
                buckets = [self._tiers[MemoryTier.coerce(tier)]]
            else:
                buckets = list(self._tiers.values())
            for bucket in buckets:
                for record in bucket.values():
                    yield record

    def record_ids(self, tier: Optional[Any] = None) -> List[str]:
        with self._lock:
            if tier is not None:
                return list(self._tiers[MemoryTier.coerce(tier)].keys())
            out: List[str] = []
            for bucket in self._tiers.values():
                out.extend(bucket.keys())
            return out

    # ---------------------------------------------------------- batch / async
    def store_many(
        self,
        items: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[TieredRecord]:
        out: List[TieredRecord] = []
        for idx, item in enumerate(items):
            try:
                out.append(self.store(**dict(item)))
            except MemoryTierError as exc:
                if stop_on_error:
                    raise
                logger.warning("store_many[%d] failed: %s", idx, exc)
                with self._lock:
                    self._last_error = f"store_many[{idx}]: {exc}"
        return out

    def get_many(self, record_ids: Iterable[str]) -> List[Optional[TieredRecord]]:
        return [self.get(rid) for rid in record_ids]

    async def store_async(self, **kwargs: Any) -> TieredRecord:
        return await asyncio.to_thread(self.store, **kwargs)

    async def store_record_async(self, record: Any, **kwargs: Any) -> TieredRecord:
        return await asyncio.to_thread(self.store_record, record, **kwargs)

    async def get_async(self, record_id: str, **kwargs: Any) -> Optional[TieredRecord]:
        return await asyncio.to_thread(self.get, record_id, **kwargs)

    async def peek_async(self, record_id: str) -> Optional[TieredRecord]:
        return await asyncio.to_thread(self.peek, record_id)

    async def expire_async(self) -> int:
        return await asyncio.to_thread(self.expire)

    # ---------------------------------------------------------- internals
    def _lookup_locked(
        self, record_id: str,
    ) -> Optional[Tuple[MemoryTier, TieredRecord]]:
        for tier in (MemoryTier.HOT, MemoryTier.WARM, MemoryTier.COLD):
            record = self._tiers[tier].get(record_id)
            if record is not None:
                return tier, record
        return None

    def _remove_from_all_tiers_locked(self, record_id: str) -> int:
        removed = 0
        for bucket in self._tiers.values():
            if record_id in bucket:
                bucket.pop(record_id, None)
                removed += 1
        return removed

    def _should_promote(
        self,
        record: TieredRecord,
        current_tier: MemoryTier,
        promote: Optional[bool],
    ) -> bool:
        if promote is False:
            return False
        if current_tier == MemoryTier.HOT:
            return False
        if promote is True:
            return True
        # Auto-promotion: threshold-based.
        if record.access_count < self._config.hot_promotion_min_accesses:
            return False
        # High-importance trusted records promote eagerly.
        if (
            record.truth_level in self._config.trusted_truth_levels
            and record.importance >= self._config.hot_importance_threshold
        ):
            return True
        return record.access_count >= self._config.hot_promotion_min_accesses

    def _move_to_tier_locked(
        self,
        record: TieredRecord,
        new_tier: MemoryTier,
        *,
        preserve_ttl: bool,
    ) -> TieredRecord:
        tier = MemoryTier.coerce(new_tier)
        ttl_seconds = self._config.ttl_for(record.kind, tier)
        moved = record.with_tier(
            tier,
            ttl_seconds=ttl_seconds,
            preserve_ttl=preserve_ttl,
        )
        # Ensure cross-tier uniqueness.
        self._remove_from_all_tiers_locked(record.record_id)
        self._tiers[tier][record.record_id] = moved
        return moved

    def _enforce_capacity_locked(self, tier: MemoryTier) -> None:
        cap = self._config.capacity_for(tier)
        bucket = self._tiers[tier]
        while len(bucket) > cap:
            rid, record = bucket.popitem(last=False)
            lower = tier.next_lower()
            if lower is None:
                # COLD overflow: drop.
                self._evictions += 1
                self._delete_from_backends(rid)
            else:
                # Demote to the next-lower tier.
                self._move_to_tier_locked(
                    record, lower, preserve_ttl=False,
                )
                self._demotions += 1
                if lower in (MemoryTier.WARM, MemoryTier.COLD):
                    self._mirror_to_backends(record)
                # Recursively enforce on the lower tier.
                self._enforce_capacity_locked(lower)

    def _record_latency(self, start: float) -> None:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        with self._lock:
            self._latency_ring.append(elapsed_ms)

    # ---------------------------------------------------------- backend I/O
    def _mirror_to_backends(self, record: TieredRecord) -> None:
        payload = record.to_episode_payload()
        if self._episodic is not None:
            try:
                self._episodic.store(payload)
                with self._lock:
                    self._backend_writes += 1
            except Exception as exc:  # pragma: no cover - backend
                with self._lock:
                    self._backend_errors += 1
                    self._last_error = f"episodic.store: {exc}"
                logger.warning("episodic.store failed: %s", exc)
        if self._adapter is not None:
            try:
                self._adapter.remember(payload)
                with self._lock:
                    self._backend_writes += 1
            except Exception as exc:  # pragma: no cover - backend
                with self._lock:
                    self._backend_errors += 1
                    self._last_error = f"adapter.remember: {exc}"
                logger.warning("adapter.remember failed: %s", exc)

    def _load_from_backend(self, record_id: str) -> Optional[TieredRecord]:
        ep = self._episodic
        if ep is not None:
            loader = getattr(ep, "load_all", None)
            if callable(loader):
                try:
                    for item in loader():
                        if not isinstance(item, ABCMapping):
                            continue
                        meta = item.get("metadata") or {}
                        if not isinstance(meta, ABCMapping):
                            continue
                        rid = (
                            meta.get("record_id")
                            or meta.get("run_id")
                            or meta.get("incident_id")
                            or meta.get("id")
                        )
                        if rid == record_id:
                            return TieredRecord.from_episode_payload(item)
                except Exception as exc:  # pragma: no cover - backend
                    with self._lock:
                        self._backend_errors += 1
                        self._last_error = f"episodic.load_all: {exc}"
                    logger.warning("episodic.load_all failed: %s", exc)
        # Adapter loading is best-effort via recall; skip by default
        # because recall_similar_runs requires a query string.
        return None

    def _delete_from_backends(self, record_id: str) -> None:
        ep = self._episodic
        if ep is not None:
            delete = getattr(ep, "delete", None) or getattr(ep, "forget", None)
            if callable(delete):
                try:
                    delete(record_id)
                except Exception as exc:  # pragma: no cover - backend
                    with self._lock:
                        self._backend_errors += 1
                        self._last_error = f"episodic.delete: {exc}"

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latency_ring)
            mean_lat = sum(lats) / len(lats) if lats else 0.0
            return {
                "sizes": {t.value: len(b) for t, b in self._tiers.items()},
                "hits": self._hits,
                "misses": self._misses,
                "stores": self._stores,
                "promotions": self._promotions,
                "demotions": self._demotions,
                "evictions": self._evictions,
                "expirations": self._expirations,
                "backend_writes": self._backend_writes,
                "backend_errors": self._backend_errors,
                "last_error": self._last_error,
                "mean_latency_ms": mean_lat,
                "p50_latency_ms": _percentile(lats, 50),
                "p95_latency_ms": _percentile(lats, 95),
                "max_latency_ms": max(lats) if lats else 0.0,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "has_episodic_backend": self._episodic is not None,
                "has_adapter_backend": self._adapter is not None,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self, *, clear: bool = True) -> int:
        """Reset counters (and optionally the tiers).

        Returns the number of records cleared.
        """
        with self._lock:
            removed = 0
            if clear:
                for bucket in self._tiers.values():
                    removed += len(bucket)
                    bucket.clear()
            self._hits = 0
            self._misses = 0
            self._stores = 0
            self._promotions = 0
            self._demotions = 0
            self._evictions = 0
            self._expirations = 0
            self._backend_writes = 0
            self._backend_errors = 0
            self._last_error = None
            self._latency_ring.clear()
            self._started_at = time.monotonic()
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_records: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "statistics": self.statistics(),
            }
            if include_records:
                payload["records"] = {
                    tier.value: [r.to_dict() for r in bucket.values()]
                    for tier, bucket in self._tiers.items()
                }
        return payload

    def to_json(
        self, *, include_records: bool = False, indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(include_records=include_records),
            default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        episodic: Optional[Any] = None,
        adapter: Optional[Any] = None,
        strict: Optional[bool] = None,
        restore_records: bool = False,
    ) -> "MemoryTierManager":
        if not isinstance(data, ABCMapping):
            raise MemoryTierInputError(
                "MemoryTierManager.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob if isinstance(cfg_blob, MemoryTierConfig)
            else MemoryTierConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None else bool(strict)
        )
        mgr = cls(
            config=config, strict=resolved_strict,
            episodic=episodic, adapter=adapter,
        )
        if restore_records and isinstance(data.get("records"), ABCMapping):
            for tier_str, items in data["records"].items():
                try:
                    tier = MemoryTier.coerce(tier_str)
                except MemoryTierError:
                    continue
                for raw in items:
                    try:
                        rec = TieredRecord.from_dict(raw)
                    except MemoryTierError as exc:
                        logger.warning(
                            "from_dict: skipping malformed record: %s", exc,
                        )
                        continue
                    with mgr._lock:  # noqa: SLF001 - intentional
                        mgr._tiers[tier][rec.record_id] = rec
        return mgr

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        episodic: Optional[Any] = None,
        adapter: Optional[Any] = None,
        strict: Optional[bool] = None,
        restore_records: bool = False,
    ) -> "MemoryTierManager":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryTierInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryTierInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(
            data, episodic=episodic, adapter=adapter,
            strict=strict, restore_records=restore_records,
        )

    # ---------------------------------------------------------------- repr
    def __repr__(self) -> str:
        with self._lock:
            return (
                "MemoryTierManager("
                f"hot={len(self._tiers[MemoryTier.HOT])}, "
                f"warm={len(self._tiers[MemoryTier.WARM])}, "
                f"cold={len(self._tiers[MemoryTier.COLD])}, "
                f"strict={self._strict})"
            )


__all__ = [
    "DEFAULT_COLD_TTL_SECONDS",
    "DEFAULT_HOT_TTL_SECONDS",
    "DEFAULT_KIND_TTLS",
    "DEFAULT_TIER_TTLS",
    "DEFAULT_WARM_TTL_SECONDS",
    "MemoryTier",
    "MemoryTierConfig",
    "MemoryTierError",
    "MemoryTierInputError",
    "MemoryTierConfigError",
    "MemoryTierParseError",
    "MemoryTierManager",
    "MemoryTierPolicy",
    "SCHEMA_VERSION",
    "TieredRecord",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.memory_tier
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    from datetime import timedelta

    from .memory_schemas import (
        DecisionRecord, IncidentRecord, OutcomeRecord, PolicyRecord,
    )

    # --------------------------------------------------- 1. Basic tier selection
    mgr = MemoryTierManager()
    print("repr         :", mgr)

    mgr.store(
        record_id="dec-001", kind="decision_outcome",
        payload={"route": "edge_int8"}, importance=0.9,
        truth_level="measured",
    )
    mgr.store(
        record_id="dec-002", kind="decision_outcome",
        payload={"route": "cloud_gpu"}, importance=0.4,
        truth_level="measured",
    )
    mgr.store(
        record_id="sim-001", kind="simulation",
        payload={"note": "simulated"}, importance=0.9,
        truth_level="simulated",
    )

    assert mgr.tier_of("dec-001") == MemoryTier.HOT
    assert mgr.tier_of("dec-002") == MemoryTier.WARM
    assert mgr.tier_of("sim-001") == MemoryTier.COLD
    print("tiers        :", mgr.statistics()["sizes"])
    assert len(mgr) == 3
    assert "dec-001" in mgr

    # --------------------------------------------------- 2. Peek vs get
    # peek does not promote (access_count unchanged for promotion).
    before_tier = mgr.tier_of("sim-001")
    peeked = mgr.peek("sim-001")
    assert peeked is not None
    assert mgr.tier_of("sim-001") == before_tier  # still COLD
    print("peek (no promote) : OK")

    # get with threshold: needs 2 accesses to promote.
    mgr.get("sim-001")  # access_count == 1
    assert mgr.tier_of("sim-001") == MemoryTier.COLD
    mgr.get("sim-001")  # access_count == 2 → promotes (to WARM by default
                        # path; because next_higher from COLD is WARM)
    assert mgr.tier_of("sim-001") in (MemoryTier.WARM, MemoryTier.HOT)
    print("get (promotes)    :", mgr.tier_of("sim-001"))

    # peek does not increase access_count enough to promote further.
    access_before = mgr.get("sim-001").access_count
    mgr.peek("sim-001")
    assert mgr.get("sim-001").access_count >= access_before
    print("access count      : OK")

    # --------------------------------------------------- 3. Explicit promote/demote
    mgr.demote("dec-001")
    assert mgr.tier_of("dec-001") == MemoryTier.WARM
    mgr.demote("dec-001")
    assert mgr.tier_of("dec-001") == MemoryTier.COLD
    mgr.promote("dec-001")
    assert mgr.tier_of("dec-001") == MemoryTier.WARM
    mgr.promote("dec-001")
    assert mgr.tier_of("dec-001") == MemoryTier.HOT
    print("promote/demote    : OK")

    # --------------------------------------------------- 4. Per-kind TTL
    cfg = MemoryTierConfig(
        hot_capacity=16, warm_capacity=16, cold_capacity=16,
        hot_ttl_seconds=60, warm_ttl_seconds=3600,
        cold_ttl_seconds=86400,
    )
    assert cfg.ttl_for("policy", MemoryTier.HOT) == 365 * 24 * 3600
    assert cfg.ttl_for("grid_forecast", MemoryTier.HOT) == 6 * 3600
    assert cfg.ttl_for("unknown_kind", MemoryTier.HOT) == 60
    assert cfg.ttl_for("unknown_kind", MemoryTier.COLD) == 86400
    print("per-kind TTL      : OK")

    # --------------------------------------------------- 5. TTL preservation on promote
    mgr2 = MemoryTierManager(config=MemoryTierConfig(
        promotion_preserves_ttl=True,
        hot_promotion_min_accesses=1,
    ))
    r = mgr2.store(
        record_id="k", kind="grid_forecast", payload={},
        importance=0.4, truth_level="measured",
    )
    assert r.tier == MemoryTier.WARM
    original_ttl = r.ttl_seconds
    promoted = mgr2.get("k")
    assert promoted is not None
    assert promoted.ttl_seconds == original_ttl  # preserved
    print("preserve TTL      : OK")

    # --------------------------------------------------- 6. Cache pollution fix
    mgr3 = MemoryTierManager(config=MemoryTierConfig(
        hot_capacity=2, warm_capacity=8, cold_capacity=8,
        hot_promotion_min_accesses=5,   # require many accesses
    ))
    # Seed the HOT tier with two important records.
    mgr3.store(record_id="h1", kind="policy", payload={"x": 1},
               importance=0.9, truth_level="measured")
    mgr3.store(record_id="h2", kind="policy", payload={"x": 2},
               importance=0.9, truth_level="measured")
    assert mgr3.tier_of("h1") == MemoryTier.HOT
    assert mgr3.tier_of("h2") == MemoryTier.HOT
    # Seed COLD records.
    for i in range(3):
        mgr3.store(
            record_id=f"c{i}", kind="run", payload={"i": i},
            importance=0.1, truth_level="simulated",
        )
    # Peek repeatedly — nothing should promote.
    for i in range(3):
        for _ in range(3):
            mgr3.peek(f"c{i}")
    assert mgr3.tier_of("h1") == MemoryTier.HOT
    assert mgr3.tier_of("h2") == MemoryTier.HOT
    for i in range(3):
        assert mgr3.tier_of(f"c{i}") == MemoryTier.COLD
    print("no cache pollution : OK")

    # --------------------------------------------------- 7. Capacity overflow demotes
    mgr4 = MemoryTierManager(config=MemoryTierConfig(
        hot_capacity=2, warm_capacity=4, cold_capacity=100,
        hot_promotion_min_accesses=1,
    ))
    for i in range(4):
        mgr4.store(
            record_id=f"r{i}", kind="policy",
            payload={"i": i}, importance=0.9, truth_level="measured",
        )
    # First two stay HOT, later ones should have demoted earlier ones.
    assert mgr4.size(MemoryTier.HOT) <= 2
    assert mgr4.size(MemoryTier.WARM) >= 1
    print("capacity demotes  : OK", mgr4.statistics()["sizes"])

    # --------------------------------------------------- 8. Cross-tier uniqueness
    mgr5 = MemoryTierManager()
    mgr5.store(record_id="x", kind="policy", payload={"v": 1},
               importance=0.9, truth_level="measured")
    assert mgr5.tier_of("x") == MemoryTier.HOT
    mgr5.store(record_id="x", kind="policy", payload={"v": 2},
               importance=0.1, truth_level="measured")
    assert mgr5.tier_of("x") == MemoryTier.WARM  # moved, not duplicated
    print("unique across tiers : OK")

    # --------------------------------------------------- 9. Schema record integration
    record = DecisionRecord(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        route="edge_int8",
        policy_version="v0.3",
        truth_level="measured",
        predicted_energy_wh=8.1,
        predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0,
        reason="Met SLA.",
        candidates=("edge_int8", "cloud_gpu"),
        quality_score=0.91,
    )
    stored = mgr.store_record(record)
    assert stored.record_id == "run-001"
    assert stored.kind == "decision_outcome"
    assert stored.truth_level == "measured"
    assert stored.policy_version == "v0.3"
    assert stored.container_tag == DEFAULT_CONTAINER_TAG
    assert stored.tier == MemoryTier.HOT
    print("store_record      : OK")

    # Policy record
    policy = PolicyRecord(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0},
        approved_by="ops",
    )
    p_stored = mgr.store_record(policy)
    assert p_stored.kind == "policy"
    assert p_stored.record_id.startswith("default:")

    # Incident record — severity derived to importance
    incident = IncidentRecord(
        incident_id="inc-1", severity="critical",
        description="Carbon overshoot.",
    )
    i_stored = mgr.store_record(incident)
    assert i_stored.importance == 1.0
    assert i_stored.kind == "incident"

    # Outcome record
    outcome = OutcomeRecord(
        run_id="run-002",
        predicted={"energy_wh": 8.0}, measured={"energy_wh": 8.5},
    )
    o_stored = mgr.store_record(outcome)
    assert o_stored.kind == "run"
    print("all schema types  : OK")

    # --------------------------------------------------- 10. Backend: EpisodicMemory
    from .episodic_memory import EpisodicMemory, IN_MEMORY_PATH
    ep = EpisodicMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    mgr6 = MemoryTierManager(episodic=ep)
    mgr6.store(record_id="w1", kind="policy", payload={"v": 1},
               importance=0.4, truth_level="measured")   # WARM
    assert ep.count >= 1
    print("episodic mirror   : OK")

    # get loads from backend on miss.
    mgr6.reset(clear=True)
    assert mgr6.get("w1") is not None
    print("backend reload    : OK")

    # --------------------------------------------------- 11. Serialization
    cfg = MemoryTierConfig()
    assert MemoryTierConfig.from_dict(cfg.to_dict()) == cfg
    assert MemoryTierConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(hot_capacity=99)
    assert cfg2.hot_capacity == 99 and cfg.hot_capacity == 256
    hash(cfg)
    print("cfg RT            : OK")

    rec = mgr.entries = next(iter(mgr))
    rec_rt = TieredRecord.from_dict(rec.to
