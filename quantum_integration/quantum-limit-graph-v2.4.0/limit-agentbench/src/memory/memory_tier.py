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
- ``TieredRecord`` — **deeply frozen** (recursive ``MappingProxyType`` /
  tuple wrapping via ``_deep_freeze()``), hashable via ``_hashable()``
  (no JSON fallback), with ``observed_at`` / ``container_tag`` /
  ``policy_version`` / ``schema_version`` fields, full serialization
  symmetry, and wrapped ``from_dict`` / ``from_episode_payload`` casts
  that reject ``bool`` and raise ``MemoryTierParseError``.
- ``MemoryTierPolicy`` — deterministic tier selection, accepts both the
  string and the ``TruthLevel`` enum; fully serializable.
- ``MemoryTierManager`` — thread-safe LRU per tier, per-kind TTLs,
  demotion-instead-of-drop on capacity overflow, cross-tier uniqueness,
  ``peek()`` vs. ``get()``, ``store_record()`` for schema records (with
  ``container_tag=`` / ``policy_version=`` overrides), ``from_config()``
  / ``from_pipeline()`` / ``from_memory_dicts()`` constructors,
  ``to_memory_dict()`` / ``to_episode_payload()`` bridges,
  optional ``EpisodicMemory`` / ``SupermemoryAdapter`` mirror backends,
  ``statistics()`` / ``reset()`` / ``close()``, context-manager support,
  async siblings, snapshot-before-yield iteration, and full
  serialization symmetry with ``schema_version`` stamped everywhere.

Notes
-----
- TTL math uses ``time.time()`` (wall clock) for ``expires_at`` so
  records stay serializable across processes. A ``_mono_stored_at``
  field is tracked to provide a monotonic baseline *within* a process;
  ``is_expired()`` prefers the monotonic path when available and falls
  back to wall clock for records reconstructed via ``from_dict``.
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

__version__ = "6.1.0"

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
# Shared freeze / hash / plain / cast helpers — mirror the patched modules
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


def _deep_freeze(value: Any, *, depth: int = 0) -> Any:
    """Recursively wrap mappings in ``MappingProxyType`` and sequences in
    tuples. Used so that ``TieredRecord.payload`` is truly immutable.
    """
    if depth > 32:
        return value
    if isinstance(value, ABCMapping):
        return MappingProxyType({
            str(k): _deep_freeze(v, depth=depth + 1)
            for k, v in value.items()
        })
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_deep_freeze(v, depth=depth + 1) for v in value)
    return value


def _hashable(value: Any, *, depth: int = 0) -> Any:
    """Convert nested mappings / sequences to hashable tuples."""
    if depth > 32:
        return "<truncated>"
    if isinstance(value, ABCMapping):
        return tuple(sorted(
            (str(k), _hashable(v, depth=depth + 1))
            for k, v in value.items()
        ))
    if isinstance(value, (list, tuple)):
        return tuple(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


def _to_plain(value: Any, *, depth: int = 0) -> Any:
    """Convert frozen structures back to plain dicts / lists."""
    if depth > 32:
        return value
    if isinstance(value, ABCMapping):
        return {
            str(k): _to_plain(v, depth=depth + 1) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    if isinstance(value, (set, frozenset)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    return value


def _coerce_float(name: str, value: Any, *, non_negative: bool = False) -> float:
    """Coerce to a finite float; reject ``bool`` / non-numerics."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MemoryTierParseError(
            f"{name} must be numeric (got {type(value).__name__})."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise MemoryTierParseError(
            f"{name} must be finite (got {value!r})."
        )
    if non_negative and fv < 0:
        raise MemoryTierParseError(f"{name} must be >= 0.")
    return fv


def _coerce_int(name: str, value: Any, *, positive: bool = False) -> int:
    """Coerce to an int; reject ``bool``."""
    if isinstance(value, bool):
        raise MemoryTierParseError(f"{name} must be an int.")
    if isinstance(value, int):
        iv = int(value)
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        iv = int(value)
    elif isinstance(value, str):
        s = value.strip()
        try:
            iv = int(s)
        except ValueError as exc:
            raise MemoryTierParseError(
                f"{name} must be an int (got {value!r})."
            ) from exc
    else:
        raise MemoryTierParseError(
            f"{name} must be an int (got {type(value).__name__})."
        )
    if positive and iv <= 0:
        raise MemoryTierParseError(f"{name} must be a positive int.")
    return iv


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


def _derive_importance(record: Any, *, default: float = 0.5) -> float:
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
        }.get(sev, default)
    return default


# --------------------------------------------------------------------------- #
# Tier enum
# --------------------------------------------------------------------------- #
_TIER_DESCRIPTIONS: Mapping[str, str] = MappingProxyType({
    "hot": "In-process LRU, sub-millisecond access",
    "warm": "Supermemory container, millisecond access",
    "cold": "Archive, subject to eviction, may require reloading",
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

    schema_version: int = SCHEMA_VERSION
    hot_capacity: int = 256
    warm_capacity: int = 4_096
    cold_capacity: int = 100_000

    hot_ttl_seconds: int = DEFAULT_HOT_TTL_SECONDS
    warm_ttl_seconds: int = DEFAULT_WARM_TTL_SECONDS
    cold_ttl_seconds: int = DEFAULT_COLD_TTL_SECONDS

    ttl_seconds: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_KIND_TTLS),
    )

    hot_importance_threshold: float = 0.8
    hot_promotion_min_accesses: int = 2
    promotion_preserves_ttl: bool = True

    trusted_truth_levels: Tuple[str, ...] = ("measured",)
    untrusted_truth_levels: Tuple[str, ...] = (
        "simulated", "user-reported",
    )

    untrusted_min_tier: Optional[str] = None

    #: Default importance for ``store_record`` when a record has neither
    #: ``quality_score`` nor ``severity``.
    default_importance: float = 0.5

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise MemoryTierConfigError(
                "schema_version must be a positive int."
            )
        for name in ("hot_capacity", "warm_capacity", "cold_capacity"):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise MemoryTierConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
        for name in (
            "hot_ttl_seconds", "warm_ttl_seconds", "cold_ttl_seconds",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise MemoryTierConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
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
        if not _is_finite_nonneg(self.hot_importance_threshold):
            raise MemoryTierConfigError(
                "hot_importance_threshold must be finite and >= 0."
            )
        if not 0.0 <= self.hot_importance_threshold <= 1.0:
            raise MemoryTierConfigError(
                "hot_importance_threshold must be in [0, 1]."
            )
        if not _is_real_int(self.hot_promotion_min_accesses) or self.hot_promotion_min_accesses < 1:
            raise MemoryTierConfigError(
                "hot_promotion_min_accesses must be an int >= 1."
            )
        if not isinstance(self.promotion_preserves_ttl, bool):
            raise MemoryTierConfigError(
                "promotion_preserves_ttl must be a bool."
            )
        if not _is_finite_nonneg(self.default_importance):
            raise MemoryTierConfigError(
                "default_importance must be finite and >= 0."
            )
        if self.default_importance > 1.0:
            raise MemoryTierConfigError(
                "default_importance must be <= 1."
            )
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
        for level in (
            self.trusted_truth_levels + self.untrusted_truth_levels
        ):
            if level not in DEFAULT_TRUTH_LEVELS:
                raise MemoryTierConfigError(
                    f"truth_level {level!r} is not in "
                    f"{list(DEFAULT_TRUTH_LEVELS)}."
                )
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
            "schema_version": self.schema_version,
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
            "default_importance": self.default_importance,
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
            self.schema_version,
            self.hot_capacity, self.warm_capacity, self.cold_capacity,
            self.hot_ttl_seconds, self.warm_ttl_seconds,
            self.cold_ttl_seconds,
            tuple(sorted(self.ttl_seconds.items())),
            self.hot_importance_threshold,
            self.hot_promotion_min_accesses,
            self.promotion_preserves_ttl,
            self.trusted_truth_levels, self.untrusted_truth_levels,
            self.untrusted_min_tier,
            self.default_importance,
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
    """Frozen record stored in a tier.

    ``payload`` is recursively frozen — nested mappings become
    ``MappingProxyType`` and nested sequences become tuples — so the
    record is truly immutable at every level.
    """

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
        # Item fix #1: recursively freeze the payload.
        try:
            object.__setattr__(
                self, "payload", _deep_freeze(dict(self.payload)),
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
            raise MemoryTierInputError("importance must be <= 1.")
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

    @classmethod
    def assert_compatible(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        """Raise ``MemoryTierParseError`` if the payload's schema is
        incompatible with the current contract."""
        if not isinstance(data, ABCMapping):
            raise MemoryTierParseError(
                "TieredRecord.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise MemoryTierParseError(
                f"invalid schema_version {v!r} in TieredRecord payload."
            )
        if v > SCHEMA_VERSION:
            raise MemoryTierParseError(
                f"TieredRecord payload schema_version {v} is newer than "
                f"the current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise MemoryTierParseError(
                f"TieredRecord payload schema_version {v} is older than "
                f"the current contract {SCHEMA_VERSION}."
            )

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
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "kind": self.kind,
            "payload": _to_plain(self.payload),
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
            record_id = data["record_id"]
            kind = data["kind"]
            tier = data["tier"]
        except KeyError as exc:
            raise MemoryTierParseError(
                f"TieredRecord.from_dict missing key {exc.args[0]!r}."
            ) from exc
        # Item fix #3: wrapped casts.
        payload_raw = data.get("payload", {})
        if not isinstance(payload_raw, ABCMapping):
            raise MemoryTierParseError(
                "TieredRecord.payload must be a Mapping."
            )
        return cls(
            record_id=str(record_id),
            kind=str(kind),
            payload=dict(payload_raw),
            tier=MemoryTier.coerce(tier),
            importance=_coerce_float(
                "importance", data.get("importance", 0.0), non_negative=True,
            ),
            truth_level=str(data.get("truth_level", "estimated")),
            observed_at=(
                _parse_iso_datetime(data.get("observed_at"))
                or datetime.now(timezone.utc)
            ),
            container_tag=str(
                data.get("container_tag", DEFAULT_CONTAINER_TAG)
            ),
            policy_version=(
                str(data["policy_version"])
                if isinstance(data.get("policy_version"), str)
                else None
            ),
            stored_at=_coerce_float(
                "stored_at", data.get("stored_at", time.time()),
                non_negative=True,
            ),
            last_accessed=_coerce_float(
                "last_accessed", data.get("last_accessed", time.time()),
                non_negative=True,
            ),
            expires_at=_coerce_float(
                "expires_at", data.get("expires_at", 0.0),
                non_negative=True,
            ),
            ttl_seconds=_coerce_int(
                "ttl_seconds", data.get("ttl_seconds", 0),
            ),
            access_count=_coerce_int(
                "access_count", data.get("access_count", 0),
            ),
            schema_version=_coerce_int(
                "schema_version",
                data.get("schema_version", SCHEMA_VERSION),
                positive=True,
            ),
            _mono_stored_at=0.0,
        )

    @classmethod
    def from_json(cls, payload: str) -> "TieredRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryTierParseError(
                f"TieredRecord.from_json invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryTierParseError(
                "TieredRecord.from_json expected a JSON object."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Bridges
    # ------------------------------------------------------------------ #
    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
        truth_level: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a payload shaped for ``EpisodicMemory.store`` /
        ``SupermemoryAdapter.remember``.

        Item fix #6: supports a ``truth_level=`` override.
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
                    _to_plain(self.payload), default=str, sort_keys=True,
                )
            except (TypeError, ValueError):
                content = str(self.payload)
        resolved_truth = (
            TruthLevel.coerce(truth_level).value
            if truth_level is not None
            else self.truth_level
        )
        meta = {
            "kind": self.kind,
            "record_id": self.record_id,
            "truth_level": resolved_truth,
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
        meta_raw = data.get("metadata") or {}
        meta: Mapping[str, Any] = (
            meta_raw if isinstance(meta_raw, ABCMapping) else {}
        )
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
        # Item fix #4: wrapped casts.
        if importance is not None:
            resolved_importance = _coerce_float(
                "importance", importance, non_negative=True,
            )
        else:
            resolved_importance = _coerce_float(
                "importance", meta.get("importance", 0.5),
                non_negative=True,
            )
        ttl_seconds = _coerce_int(
            "ttl_seconds", meta.get("ttl_seconds", 0),
        )
        schema_version = _coerce_int(
            "schema_version",
            meta.get("schema_version", SCHEMA_VERSION),
            positive=True,
        )
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
            policy_version=(
                policy_version
                if isinstance(policy_version, str) else None
            ),
            stored_at=time.time(),
            last_accessed=time.time(),
            expires_at=0.0,
            ttl_seconds=ttl_seconds,
            access_count=0,
            schema_version=schema_version,
            _mono_stored_at=0.0,
        )

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``.

        Item fix #5: includes ``schema_version`` in metadata.
        """
        try:
            content = json.dumps(
                _to_plain(self.payload), default=str, sort_keys=True,
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
                "schema_version": self.schema_version,
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
        # Item fix #2: recursive hashing — no JSON fallback on nested values.
        return hash((
            self.record_id, self.kind, _hashable(self.payload), self.tier,
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
        if config is None:
            config = MemoryTierConfig()
        elif not isinstance(config, MemoryTierConfig):
            raise MemoryTierInputError(
                "config must be a MemoryTierConfig or None."
            )
        self._config = config

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
        """Return the initial tier for a new record.

        Untrusted levels default to COLD (or ``untrusted_min_tier``).
        High-importance trusted records default to HOT. Everything else,
        including unclassified levels such as ``estimated``, defaults to
        WARM.
        """
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

        if level in self._config.untrusted_truth_levels:
            if self._config.untrusted_min_tier is not None:
                return MemoryTier.coerce(self._config.untrusted_min_tier)
            return MemoryTier.COLD

        if (
            importance >= self._config.hot_importance_threshold
            and level in self._config.trusted_truth_levels
        ):
            return MemoryTier.HOT

        return MemoryTier.WARM

    def select_tier_for_record(self, record: Any) -> MemoryTier:
        """Return the initial tier for any of the schema records."""
        importance = _derive_importance(
            record, default=self._config.default_importance,
        )
        truth_level = (
            _extract_str(_extract_field(record, "truth_level"))
            or "estimated"
        )
        return self.select_tier(
            importance=importance, truth_level=truth_level,
        )

    # ------------------------------------------------------------------ #
    # Serialization symmetry
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "config": self._config.to_dict(),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.to_dict(), default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "MemoryTierPolicy":
        if not isinstance(data, ABCMapping):
            raise MemoryTierParseError(
                "MemoryTierPolicy.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        if isinstance(cfg_blob, MemoryTierConfig):
            config = cfg_blob
        else:
            if not isinstance(cfg_blob, ABCMapping):
                raise MemoryTierParseError(
                    "MemoryTierPolicy.config must be a Mapping."
                )
            config = MemoryTierConfig.from_dict(cfg_blob, strict=strict)
        return cls(config=config)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "MemoryTierPolicy":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryTierParseError(
                f"MemoryTierPolicy.from_json invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryTierParseError(
                "MemoryTierPolicy.from_json expected a JSON object."
            )
        return cls.from_dict(data, strict=strict)

    def __repr__(self) -> str:
        return f"MemoryTierPolicy(config={self._config!r})"


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #
class MemoryTierManager:
    """Thread-safe manager for hot / warm / cold tiers."""

    def __init__(
        self,
        *,
        config: Optional[MemoryTierConfig] = None,
        policy: Optional[MemoryTierPolicy] = None,
        strict: bool = True,
        episodic: Optional[Any] = None,
        adapter: Optional[Any] = None,
    ) -> None:
        if config is None:
            config = MemoryTierConfig()
        elif not isinstance(config, MemoryTierConfig):
            raise MemoryTierInputError(
                "config must be a MemoryTierConfig or None."
            )
        if policy is None:
            policy = MemoryTierPolicy(config=config)
        elif not isinstance(policy, MemoryTierPolicy):
            raise MemoryTierInputError(
                "policy must be a MemoryTierPolicy or None."
            )
        self._config = config
        self._policy = policy
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
        self._latency_ring: Deque[float] = deque(maxlen=200)
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
            snapshot = [list(b.values()) for b in self._tiers.values()]
        for bucket in snapshot:
            for record in bucket:
                yield record

    # ---------------------------------------------------------- constructors
    @classmethod
    def from_config(
        cls,
        config: MemoryTierConfig,
        *,
        strict: bool = True,
        episodic: Optional[Any] = None,
        adapter: Optional[Any] = None,
    ) -> "MemoryTierManager":
        return cls(
            config=config, strict=strict,
            episodic=episodic, adapter=adapter,
        )

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Any,
        *,
        config: Optional[MemoryTierConfig] = None,
        strict: Optional[bool] = None,
    ) -> "MemoryTierManager":
        """Return ``pipeline.tier_manager`` if present, else build fresh."""
        mgr = getattr(pipeline, "tier_manager", None)
        if isinstance(mgr, cls):
            return mgr
        resolved = True if strict is None else bool(strict)
        return cls(
            config=config,
            strict=resolved,
            episodic=getattr(pipeline, "episodic", None),
            adapter=getattr(pipeline, "adapter", None),
        )

    @classmethod
    def from_memory_dicts(
        cls,
        entries: Iterable[Mapping[str, Any]],
        *,
        config: Optional[MemoryTierConfig] = None,
        strict: bool = True,
        episodic: Optional[Any] = None,
        adapter: Optional[Any] = None,
    ) -> "MemoryTierManager":
        """Build a manager and populate it from ``{"id", "content",
        "metadata"}`` shapes (as produced by ``to_memory_dict``)."""
        mgr = cls(
            config=config, strict=strict,
            episodic=episodic, adapter=adapter,
        )
        for e in entries:
            if not isinstance(e, ABCMapping):
                continue
            meta_raw = e.get("metadata") or {}
            meta = meta_raw if isinstance(meta_raw, ABCMapping) else {}
            record_id = e.get("id") or meta.get("record_id")
            if not record_id:
                continue
            try:
                mgr.store(
                    record_id=str(record_id),
                    kind=str(meta.get("kind") or "unknown"),
                    payload={"content": e.get("content")},
                    importance=_coerce_float(
                        "importance", meta.get("importance", 0.5),
                        non_negative=True,
                    ),
                    truth_level=meta.get("truth_level") or "estimated",
                    container_tag=meta.get("container_tag"),
                    observed_at=meta.get("observed_at"),
                    policy_version=meta.get("policy_version"),
                )
            except MemoryTierError as exc:
                logger.warning(
                    "from_memory_dicts: skipping malformed entry: %s", exc,
                )
        return mgr

    # ---------------------------------------------------------- lifecycle
    def close(self, *, flush: bool = False) -> None:
        """Close the manager.

        Parameters
        ----------
        flush : bool
            If True, flush in-memory WARM / COLD records to the backend
            before shutting it down.
        """
        if flush:
            with self._lock:
                snapshot = [
                    r for tier in (MemoryTier.WARM, MemoryTier.COLD)
                    for r in self._tiers[tier].values()
                ]
            for record in snapshot:
                self._mirror_to_backends(record)
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

    async def __aenter__(self) -> "MemoryTierManager":
        return self

    async def __aexit__(self, *exc: Any) -> None:
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
            raise MemoryTierInputError("kind must be a non-empty string.")
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
            raise MemoryTierInputError("observed_at could not be parsed.")

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
            self._remove_from_all_tiers_locked(record_id)
            self._tiers[chosen][record_id] = record
            self._stores += 1
            self._enforce_capacity_locked(chosen)

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
        container_tag: Optional[str] = None,
        policy_version: Optional[str] = None,
    ) -> TieredRecord:
        """Store any schema record (``DecisionRecord``, ``PolicyRecord``,
        ``IncidentRecord``, ``OutcomeRecord``) or any object exposing
        ``id`` / ``kind`` / ``truth_level`` / ``to_dict``.

        Item fixes #7 / #8: ``container_tag=`` and ``policy_version=``
        overrides are accepted, and ``PolicyRecord.version`` is used as
        the default ``policy_version`` when the record exposes it.
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
        truth = _extract_str(
            _extract_field(record, "truth_level")
        ) or "estimated"

        obs_dt = _extract_field(record, "observed_at", "published_at")
        if isinstance(obs_dt, str):
            obs_dt = _parse_iso_datetime(obs_dt)

        # Item fix #7: container_tag override.
        resolved_tag = (
            _extract_str(container_tag)
            or _extract_str(_extract_field(record, "container_tag"))
            or DEFAULT_CONTAINER_TAG
        )
        # Item fix #8: extract policy_version, falling back to
        # PolicyRecord.version.
        resolved_pv = _extract_str(policy_version)
        if resolved_pv is None:
            resolved_pv = _extract_str(
                _extract_field(record, "policy_version")
            )
        if resolved_pv is None:
            resolved_pv = _extract_str(_extract_field(record, "version"))

        imp = (
            float(importance)
            if importance is not None
            else _derive_importance(
                record, default=self._config.default_importance,
            )
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
            container_tag=resolved_tag,
            policy_version=resolved_pv,
        )

    def get(
        self,
        record_id: str,
        *,
        promote: Optional[bool] = None,
        load_from_backend: bool = True,
    ) -> Optional[TieredRecord]:
        """Return a record."""
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
                self._tiers[record.tier][record.record_id] = record
                found = (record.tier, record)

            current_tier, record = found
            if record.is_expired():
                self._tiers[current_tier].pop(record_id, None)
                self._expirations += 1
                self._misses += 1
                self._record_latency(start)
                return None

            touched = record.touch()
            self._tiers[current_tier].move_to_end(record_id)
            self._tiers[current_tier][record_id] = touched
            self._hits += 1

            should_promote = self._should_promote(
                touched, current_tier, promote,
            )
            if should_promote:
                new_tier = (
                    touched.tier.next_higher()
                    if touched.tier != MemoryTier.HOT
                    else MemoryTier.HOT
                )
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
        """Return a record without promoting it to a higher tier."""
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
                return record
            self._tiers[current_tier].pop(record_id, None)
            moved = self._move_to_tier_locked(
                record, target, preserve_ttl=False,
            )
            self._demotions += 1
            self._enforce_capacity_locked(target)
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
        """Item fix #11: snapshot before yielding, so consumers that
        iterate slowly don't hold the lock."""
        with self._lock:
            if tier is not None:
                snapshot = list(self._tiers[MemoryTier.coerce(tier)].values())
            else:
                snapshot = [
                    record
                    for bucket in self._tiers.values()
                    for record in bucket.values()
                ]
        for record in snapshot:
            yield record

    def record_ids(self, tier: Optional[Any] = None) -> List[str]:
        with self._lock:
            if tier is not None:
                return list(self._tiers[MemoryTier.coerce(tier)].keys())
            out: List[str] = []
            for bucket in self._tiers.values():
                out.extend(bucket.keys())
            return out

    def snapshot(self) -> Dict[str, Any]:
        """Return a read-only snapshot of the current tier contents."""
        with self._lock:
            return {
                "schema_version": SCHEMA_VERSION,
                "sizes": {
                    t.value: len(b) for t, b in self._tiers.items()
                },
                "records": {
                    t.value: [r.to_dict() for r in b.values()]
                    for t, b in self._tiers.items()
                },
            }

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
        """Item fix #9: the previous version had unreachable code.

        Semantics:
        - ``promote=False`` behaves like peek.
        - ``promote=True`` forces promotion.
        - ``None`` uses the threshold policy: promote if the record is
          trusted and has reached the minimum access count, or if it is
          trusted, high-importance, and has been accessed at least once.
        """
        if promote is False:
            return False
        if current_tier == MemoryTier.HOT:
            return False
        if promote is True:
            return True

        # Auto-promotion: threshold-based.
        if record.access_count >= self._config.hot_promotion_min_accesses:
            if record.truth_level in self._config.trusted_truth_levels:
                return True
            return True
        # High-importance trusted records promote eagerly.
        if (
            record.truth_level in self._config.trusted_truth_levels
            and record.importance >= self._config.hot_importance_threshold
            and record.access_count >= 1
        ):
            return True
        return False

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
                self._evictions += 1
                self._delete_from_backends(rid)
            else:
                self._move_to_tier_locked(
                    record, lower, preserve_ttl=False,
                )
                self._demotions += 1
                if lower in (MemoryTier.WARM, MemoryTier.COLD):
                    self._mirror_to_backends(record)
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
        """Item fix #12: ``schema_version`` stamped on statistics."""
        with self._lock:
            lats = list(self._latency_ring)
            mean_lat = sum(lats) / len(lats) if lats else 0.0
            tag_dist: Dict[str, int] = {}
            for bucket in self._tiers.values():
                for record in bucket.values():
                    tag_dist[record.container_tag] = (
                        tag_dist.get(record.container_tag, 0) + 1
                    )
            return {
                "schema_version": SCHEMA_VERSION,
                "sizes": {t.value: len(b) for t, b in self._tiers.items()},
                "container_tag_mix": tag_dist,
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
        """Item fix #12: ``schema_version`` stamped on ``to_dict()``."""
        with self._lock:
            payload: Dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
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
                if not isinstance(items, (list, tuple)):
                    continue
                for raw in items:
                    try:
                        rec = TieredRecord.from_dict(raw)
                    except MemoryTierError as exc:
                        logger.warning(
                            "from_dict: skipping malformed record: %s", exc,
                        )
                        continue
                    # Item fix #10: enforce capacity and mirror to
                    # backends on restore.
                    with mgr._lock:  # noqa: SLF001 - intentional
                        mgr._tiers[tier][rec.record_id] = rec
                    # Mirroring backends is best-effort.
                    if tier in (MemoryTier.WARM, MemoryTier.COLD):
                        mgr._mirror_to_backends(rec)  # noqa: SLF001
            # Enforce capacity per tier after all inserts.
            for tier in (MemoryTier.HOT, MemoryTier.WARM, MemoryTier.COLD):
                with mgr._lock:  # noqa: SLF001
                    mgr._enforce_capacity_locked(tier)  # noqa: SLF001
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

    # --------------------------------------------------- 2. Deep-freeze (item #1)
    rec = mgr.get("dec-001")
    assert rec is not None
    try:
        rec.payload["route"] = "hacked"  # type: ignore[index]
    except TypeError:
        pass
    # Nested payload test.
    mgr.store(
        record_id="nested-1", kind="run",
        payload={"metrics": {"accuracy": 0.9, "values": [1, 2, 3]}},
        importance=0.5, truth_level="measured",
    )
    nested = mgr.get("nested-1")
    assert nested is not None
    try:
        nested.payload["metrics"]["accuracy"] = 0.0  # type: ignore[index]
    except TypeError:
        print("nested frozen: OK")
    else:
        raise AssertionError("nested payload should be frozen")

    # --------------------------------------------------- 3. Nested hash (item #2)
    hash(nested)
    print("nested hash  : OK")

    # --------------------------------------------------- 4. to_memory_dict includes schema_version (item #5)
    md = nested.to_memory_dict()
    assert md["metadata"]["schema_version"] == SCHEMA_VERSION
    print("md schema    : OK")

    # --------------------------------------------------- 5. truth_level override (item #6)
    ep = nested.to_episode_payload(truth_level="estimated")
    assert ep["metadata"]["truth_level"] == "estimated"
    print("truth_level override : OK")

    # --------------------------------------------------- 6. store_record with overrides (items #7, #8)
    policy = PolicyRecord(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0},
        approved_by="ops",
    )
    p_stored = mgr.store_record(policy, container_tag="org:archive")
    assert p_stored.container_tag == "org:archive"
    assert p_stored.policy_version == "v0.3"   # item #8
    print("store_record overrides : OK")

    # --------------------------------------------------- 7. _should_promote correctness (item #9)
    mgr_promote = MemoryTierManager(
        config=MemoryTierConfig(hot_promotion_min_accesses=2),
    )
    mgr_promote.store(
        record_id="promo-1", kind="run", payload={},
        importance=0.5, truth_level="measured",
    )
    assert mgr_promote.tier_of("promo-1") == MemoryTier.WARM
    mgr_promote.get("promo-1")   # access_count=1
    assert mgr_promote.tier_of("promo-1") == MemoryTier.WARM
    mgr_promote.get("promo-1")   # access_count=2 → promotes
    assert mgr_promote.tier_of("promo-1") == MemoryTier.HOT
    print("promote logic : OK")

    # --------------------------------------------------- 8. Restore enforces capacity (item #10)
    small = MemoryTierManager(
        config=MemoryTierConfig(hot_capacity=2, warm_capacity=2, cold_capacity=100),
    )
    snapshot = {
        "config": small.config.to_dict(),
        "strict": True,
        "records": {
            "hot": [
                {
                    "schema_version": SCHEMA_VERSION,
                    "record_id": f"r{i}",
                    "kind": "run",
                    "payload": {},
                    "tier": "hot",
                    "importance": 0.5,
                    "truth_level": "measured",
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "container_tag": DEFAULT_CONTAINER_TAG,
                }
                for i in range(5)
            ],
        },
    }
    restored = MemoryTierManager.from_dict(
        snapshot, restore_records=True,
    )
    assert restored.size(MemoryTier.HOT) <= 2
    print("restore cap   : OK")

    # --------------------------------------------------- 9. iter_records snapshots (item #11)
    big = MemoryTierManager()
    for i in range(10):
        big.store(
            record_id=f"b{i}", kind="run", payload={},
            importance=0.5, truth_level="measured",
        )
    count = 0
    for _ in big.iter_records():
        count += 1
    assert count == 10
    print("iter snapshot : OK")

    # --------------------------------------------------- 10. schema_version everywhere (item #12)
    assert big.statistics()["schema_version"] == SCHEMA_VERSION
    assert big.to_dict()["schema_version"] == SCHEMA_VERSION
    print("schema ver    : OK")

    # --------------------------------------------------- 11. from_config / from_pipeline / from_memory_dicts
    from_cfg = MemoryTierManager.from_config(MemoryTierConfig())
    assert isinstance(from_cfg, MemoryTierManager)
    class _FakePipeline:
        tier_manager = big
    from_pipe = MemoryTierManager.from_pipeline(_FakePipeline())
    assert from_pipe is big
    from_dicts = MemoryTierManager.from_memory_dicts([
        nested.to_memory_dict(),
    ])
    assert from_dicts.size() == 1
    print("from_*        : OK")

    # --------------------------------------------------- 12. MemoryTierPolicy serialization
    pol = MemoryTierPolicy(config=MemoryTierConfig())
    pol_rt = MemoryTierPolicy.from_dict(pol.to_dict())
    assert pol_rt.config == pol.config
    print("policy RT     : OK")

    # --------------------------------------------------- 13. assert_compatible
    TieredRecord.assert_compatible({"schema_version": SCHEMA_VERSION})
    try:
        TieredRecord.assert_compatible(
            {"schema_version": SCHEMA_VERSION + 1},
        )
    except MemoryTierParseError:
        print("assert_compat : OK")

    # --------------------------------------------------- 14. from_dict wrapped casts (items #3, #4)
    try:
        TieredRecord.from_dict({
            "record_id": "x", "kind": "run",
            "tier": "hot", "importance": "abc",
        })
    except MemoryTierParseError as exc:
        print("cast error    : OK ->", exc)

    try:
        TieredRecord.from_dict({
            "record_id": "x", "kind": "run",
            "tier": "hot", "payload": "not a mapping",
        })
    except MemoryTierParseError:
        print("payload cast  : OK")

    # --------------------------------------------------- 15. Backend mirror on restore (item #10)
    from .episodic_memory import EpisodicMemory, IN_MEMORY_PATH
    ep = EpisodicMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    mgr_ep = MemoryTierManager(
        episodic=ep,
        config=MemoryTierConfig(hot_capacity=2, warm_capacity=2, cold_capacity=100),
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "config": mgr_ep.config.to_dict(),
        "strict": True,
        "records": {
            "warm": [{
                "schema_version": SCHEMA_VERSION,
                "record_id": "w-1", "kind": "policy",
                "payload": {}, "tier": "warm",
                "importance": 0.4, "truth_level": "measured",
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "container_tag": DEFAULT_CONTAINER_TAG,
            }],
        },
    }
    restored_ep = MemoryTierManager.from_dict(
        payload, episodic=ep, restore_records=True,
    )
    assert ep.count >= 1
    print("restore mirror : OK")

    # --------------------------------------------------- 16. config fields
    cfg = MemoryTierConfig()
    assert cfg.schema_version == SCHEMA_VERSION
    assert cfg.default_importance == 0.5
    assert MemoryTierConfig.from_dict(cfg.to_dict()) == cfg
    assert MemoryTierConfig.from_json(cfg.to_json()) == cfg
    print("cfg RT        : OK")

    # --------------------------------------------------- 17. close(flush=True)
    mgr_ep.close(flush=True)
    print("close flush   : OK")

    print("\nSmoke test passed.")
