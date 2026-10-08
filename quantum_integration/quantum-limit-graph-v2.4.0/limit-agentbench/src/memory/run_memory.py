# src/memory/run_memory.py

"""
Run Memory Module
=================

A memory system for sustained reflection across multiple runs. Tracks
agent performance history and generates meta-policies.

Enhancements
------------
- ``MemoryConfig`` — frozen, validated, fully serializable: trend
  thresholds, ring-buffer caps, module-aligned metric names + aliases,
  per-kind TTLs, truth-level vocabulary, adapter-limit fields
  (``latency_ring_size``, ``write_retry_backoff_seconds``,
  ``default_importance``), ``MappingProxyType``-frozen mappings,
  ``with_overrides`` / ``merge`` / ``from_dict`` / ``from_json``.
- ``RunSample`` — **deeply frozen** via ``_deep_freeze()``; hashable via
  ``_hashable()``; wrapped ``from_dict`` casts; ``truth_level=``
  override on ``to_episode_payload()``; ``metrics.*`` flattened into
  metadata so ``FeedbackLoopGuard`` / ``BoundedRecall`` can read the
  nested telemetry keys directly; ``assert_compatible()`` helper.
- ``RunMemory`` — thread-safe, ``:memory:``-aware, UTC ISO timestamps,
  reentrant context manager, sync + async context manager, async
  siblings, manager-level ``statistics()`` (with ``schema_version`` and
  per-kind / per-container mixes), ``reset()``, ``close(flush=)``,
  ``snapshot()``, ``prune()``, ``resize()``, ``count_by_kind()``,
  ``count_by_container_tag()``, ``from_config()`` / ``from_pipeline()``
  constructors, and split ``last_write_error`` / ``last_read_error``.
- Caller-supplied ``run_id`` **must be a string** — non-strings are
  rejected with a clear error rather than silently coerced.
- ``add_run`` no longer clobbers a caller-supplied ``run_id`` /
  ``timestamp``; ``run_data`` is validated for JSON-serializability up
  front so a failed ``add_run`` never leaves in-memory state diverging
  from disk.
- ``_mirror_to_backends`` derives ``importance`` from the payload's
  ``quality_score`` (or ``metrics.quality_score``) when present, so
  high-quality runs land in the right tier instead of always WARM.
- Structured error hierarchy:
  ``RunMemoryError`` → ``RunMemoryInputError``,
  ``RunMemoryConfigError``, ``RunMemoryFileError``,
  ``RunMemoryCorruptionError``, ``RunMemoryParseError``.
- ``SCHEMA_VERSION`` / ``IN_MEMORY_PATH`` / ``DEFAULT_*`` exported via
  ``__all__``.
- ``__main__`` smoke test covers happy paths, backends, bridges, async,
  sync + async context managers, ring trimming, metric aliasing,
  serialization, prune, resize, snapshot, and validation failure paths.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import tempfile
import threading
import time
from collections import deque
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
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

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .episodic_memory import EpisodicMemory
    from .memory_tier import MemoryTierManager
    from .policy_memory import PolicyMemory

logger = logging.getLogger(__name__)

__version__ = "6.1.0"


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
SCHEMA_VERSION: int = 1
IN_MEMORY_PATH: str = ":memory:"
DEFAULT_MEMORY_FILE: str = "run_memory.json"
DEFAULT_KIND: str = "run"
DEFAULT_CONTAINER_TAG: str = "org:green-agent"

#: Canonical metric names aligned with ``MemoryBenchmarkConfig.metric_names``.
DEFAULT_METRICS: Tuple[str, ...] = (
    "quality",
    "accuracy",
    "energy_wh",
    "carbon_gco2e",
    "latency_ms",
)

#: Direction map for the canonical metrics + a few extras.
DEFAULT_HIGHER_IS_BETTER: Mapping[str, bool] = MappingProxyType({
    "quality":              True,
    "accuracy":             True,
    "energy_wh":            False,
    "carbon_gco2e":         False,
    "latency_ms":           False,
    "tokens":               False,
    "decision_consistency": True,
})

#: Legacy names that map onto canonical metrics.
DEFAULT_METRIC_ALIASES: Mapping[str, Tuple[str, ...]] = MappingProxyType({
    "quality":              ("final_score", "score"),
    "accuracy":             ("acc",),
    "energy_wh":            ("energy_consumption", "energy"),
    "carbon_gco2e":         ("carbon_emissions", "carbon_g", "carbon"),
    "latency_ms":           ("latency", "elapsed_ms"),
    "tokens":               ("token_count",),
    "decision_consistency": ("consistency",),
})

#: Per-kind TTLs aligned with ``SupermemoryConfig.ttl_seconds``.
DEFAULT_TTL_SECONDS: Mapping[str, int] = MappingProxyType({
    "decision_outcome": 90 * 24 * 3600,
    "policy":           365 * 24 * 3600,
    "incident":         365 * 24 * 3600,
    "run":              180 * 24 * 3600,
    "grid_forecast":    6 * 3600,
    "thermal_state":    30 * 60,
    "connectivity":     5 * 60,
})

#: Truth-level vocabulary imported semantics from ``memory_schemas``.
DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)

_DEFAULT_LATENCY_RING_SIZE: int = 200
_DEFAULT_WRITE_RETRY_BACKOFF_SECONDS: float = 0.25
_DEFAULT_IMPORTANCE: float = 0.5


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RunMemoryError(ValueError):
    """Base class for run-memory problems."""


class RunMemoryInputError(RunMemoryError):
    """Invalid input to a public API."""


class RunMemoryConfigError(RunMemoryError):
    """Invalid configuration."""


class RunMemoryFileError(RunMemoryError):
    """Disk I/O failure (read, write, permission, missing dir)."""


class RunMemoryCorruptionError(RunMemoryError):
    """The backing file is not valid JSON, or entries are malformed."""


class RunMemoryParseError(RunMemoryError):
    """Failed to parse a record / config / sample from dict or JSON."""


# --------------------------------------------------------------------------- #
# Shared validation / freeze / hash helpers — mirror the patched modules
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
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _iso_now() -> str:
    """Return a UTC ISO 8601 timestamp with an offset."""
    return datetime.now(timezone.utc).isoformat()


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


def _normalize_string_tuple(
    value: Any,
    *,
    name: str,
    allow_empty: bool = False,
) -> Tuple[str, ...]:
    if isinstance(value, str) or not isinstance(
        value, (tuple, list, set, frozenset)
    ):
        raise RunMemoryConfigError(
            f"{name} must be a sequence of strings."
        )
    out: List[str] = []
    seen: set = set()
    for item in value:
        if not isinstance(item, str) or not item:
            raise RunMemoryConfigError(
                f"{name} entries must be non-empty strings."
            )
        if item in seen:
            raise RunMemoryConfigError(
                f"{name} contains duplicate {item!r}."
            )
        seen.add(item)
        out.append(item)
    if not out and not allow_empty:
        raise RunMemoryConfigError(f"{name} must be non-empty.")
    return tuple(out)


def _freeze_bool_map(
    name: str, value: Any,
) -> Mapping[str, bool]:
    if not isinstance(value, ABCMapping):
        raise RunMemoryConfigError(f"{name} must be a Mapping.")
    out: Dict[str, bool] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise RunMemoryConfigError(
                f"{name} keys must be non-empty strings."
            )
        if not isinstance(v, bool):
            raise RunMemoryConfigError(
                f"{name}[{k!r}] must be a bool."
            )
        out[k] = v
    return MappingProxyType(out)


def _freeze_tuple_map(
    name: str, value: Any,
) -> Mapping[str, Tuple[str, ...]]:
    if not isinstance(value, ABCMapping):
        raise RunMemoryConfigError(f"{name} must be a Mapping.")
    out: Dict[str, Tuple[str, ...]] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise RunMemoryConfigError(
                f"{name} keys must be non-empty strings."
            )
        if isinstance(v, str) or not isinstance(
            v, (tuple, list, set, frozenset)
        ):
            raise RunMemoryConfigError(
                f"{name}[{k!r}] must be a sequence of strings."
            )
        items: List[str] = []
        for item in v:
            if not isinstance(item, str) or not item:
                raise RunMemoryConfigError(
                    f"{name}[{k!r}] entries must be non-empty strings."
                )
            items.append(item)
        out[k] = tuple(items)
    return MappingProxyType(out)


def _freeze_int_map(name: str, value: Any) -> Mapping[str, int]:
    if not isinstance(value, ABCMapping):
        raise RunMemoryConfigError(f"{name} must be a Mapping.")
    out: Dict[str, int] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise RunMemoryConfigError(
                f"{name} keys must be non-empty strings."
            )
        if not _is_real_int(v) or v <= 0:
            raise RunMemoryConfigError(
                f"{name}[{k!r}] must be a positive int."
            )
        out[k] = int(v)
    return MappingProxyType(out)


def _deep_freeze(value: Any, *, depth: int = 0) -> Any:
    """Recursively wrap mappings in ``MappingProxyType`` and sequences in
    tuples, so "frozen" records are truly immutable at every level.
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
    """Convert frozen structures back to plain dicts / lists so the
    returned data is JSON-serializable and freely mutable by the caller.
    """
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


def _coerce_int(name: str, value: Any, *, positive: bool = False) -> int:
    """Coerce to int; reject ``bool`` and non-integers."""
    if isinstance(value, bool):
        raise RunMemoryParseError(f"{name} must be an int.")
    if isinstance(value, int):
        iv = int(value)
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        iv = int(value)
    elif isinstance(value, str):
        s = value.strip()
        try:
            iv = int(s)
        except ValueError as exc:
            raise RunMemoryParseError(
                f"{name} must be an int (got {value!r})."
            ) from exc
    else:
        raise RunMemoryParseError(
            f"{name} must be an int (got {type(value).__name__})."
        )
    if positive and iv <= 0:
        raise RunMemoryParseError(f"{name} must be a positive int.")
    return iv


def _metric_family(
    name: str,
    aliases: Mapping[str, Tuple[str, ...]],
) -> Tuple[str, ...]:
    """Return the set of names that should be treated as equivalent."""
    family: set = {name}
    if name in aliases:
        family.update(aliases[name])
    for primary, alts in aliases.items():
        if name in alts:
            family.add(primary)
            family.update(alts)
    return tuple(family)


def _extract_metric(
    run: Mapping[str, Any],
    name: str,
    aliases: Mapping[str, Tuple[str, ...]],
) -> Optional[float]:
    """Return a finite float for ``name`` or any of its aliases, or None."""
    family = _metric_family(name, aliases)
    for container in (
        run,
        run.get("metrics") if isinstance(run, ABCMapping) else None,
    ):
        if not isinstance(container, ABCMapping):
            continue
        for candidate in family:
            if candidate in container:
                value = container[candidate]
                if isinstance(value, bool):
                    continue
                if isinstance(value, (int, float)) and math.isfinite(float(value)):
                    return float(value)
    return None


def _derive_importance_from_run(
    run_data: Mapping[str, Any],
    *,
    default: float = _DEFAULT_IMPORTANCE,
) -> float:
    """Derive an importance in [0, 1] from a run's metrics.

    Looks for ``quality_score`` at the top level, then under ``metrics``.
    Falls back to ``default`` when neither is present or valid.
    """
    def _try(mapping: Any) -> Optional[float]:
        if not isinstance(mapping, ABCMapping):
            return None
        q = mapping.get("quality_score")
        if isinstance(q, (int, float)) and not isinstance(q, bool):
            if math.isfinite(float(q)):
                return max(0.0, min(1.0, float(q)))
        return None

    v = _try(run_data)
    if v is not None:
        return v
    v = _try(run_data.get("metrics"))
    if v is not None:
        return v
    return default


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryConfig:
    """Tunable parameters for trend analysis, caps and metric vocabulary."""

    trend_stability_band: float = 0.05
    min_runs_for_trend: int = 2
    min_runs_for_policy: int = 3

    #: Ring-buffer cap. ``None`` means unlimited.
    max_runs: Optional[int] = 1_000
    #: Ring-buffer cap for meta-policies.
    max_policies: int = 100

    metrics_to_track: Tuple[str, ...] = DEFAULT_METRICS
    higher_is_better: Mapping[str, bool] = field(
        default_factory=lambda: dict(DEFAULT_HIGHER_IS_BETTER),
    )
    metric_aliases: Mapping[str, Tuple[str, ...]] = field(
        default_factory=lambda: dict(DEFAULT_METRIC_ALIASES),
    )

    ttl_seconds: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_TTL_SECONDS),
    )
    truth_levels: Tuple[str, ...] = DEFAULT_TRUTH_LEVELS
    default_truth_level: str = "measured"
    default_container_tag: str = DEFAULT_CONTAINER_TAG
    default_kind: str = DEFAULT_KIND

    schema_version: int = SCHEMA_VERSION
    write_retries: int = 2

    #: Adapter-level limits — moved out of hard-coded module constants.
    latency_ring_size: int = _DEFAULT_LATENCY_RING_SIZE
    write_retry_backoff_seconds: float = _DEFAULT_WRITE_RETRY_BACKOFF_SECONDS

    #: Default importance passed to the tier mirror when a run has no
    #: ``quality_score``.
    default_importance: float = _DEFAULT_IMPORTANCE

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not _is_positive_finite(self.trend_stability_band):
            raise RunMemoryConfigError(
                "trend_stability_band must be a finite number > 0."
            )
        if not (0.0 < self.trend_stability_band < 1.0):
            raise RunMemoryConfigError(
                "trend_stability_band must be in (0, 1)."
            )
        if not _is_real_int(self.min_runs_for_trend) or self.min_runs_for_trend < 2:
            raise RunMemoryConfigError(
                "min_runs_for_trend must be an int >= 2."
            )
        if not _is_real_int(self.min_runs_for_policy) or self.min_runs_for_policy < self.min_runs_for_trend:
            raise RunMemoryConfigError(
                "min_runs_for_policy must be an int >= min_runs_for_trend."
            )
        if self.max_runs is not None:
            if not _is_real_int(self.max_runs) or self.max_runs <= 0:
                raise RunMemoryConfigError(
                    "max_runs must be None or a positive int."
                )
        if not _is_real_int(self.max_policies) or self.max_policies <= 0:
            raise RunMemoryConfigError(
                "max_policies must be a positive int."
            )
        if not _is_real_int(self.write_retries) or self.write_retries < 0:
            raise RunMemoryConfigError(
                "write_retries must be a non-negative int."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise RunMemoryConfigError(
                "schema_version must be a positive int."
            )
        if not _is_real_int(self.latency_ring_size) or self.latency_ring_size <= 0:
            raise RunMemoryConfigError(
                "latency_ring_size must be a positive int."
            )
        if not _is_positive_finite(self.write_retry_backoff_seconds):
            raise RunMemoryConfigError(
                "write_retry_backoff_seconds must be a finite number > 0."
            )
        if not _is_finite_nonneg(self.default_importance):
            raise RunMemoryConfigError(
                "default_importance must be finite and >= 0."
            )
        if self.default_importance > 1.0:
            raise RunMemoryConfigError(
                "default_importance must be <= 1."
            )

        object.__setattr__(
            self,
            "metrics_to_track",
            _normalize_string_tuple(
                self.metrics_to_track, name="metrics_to_track",
            ),
        )
        object.__setattr__(
            self,
            "truth_levels",
            _normalize_string_tuple(
                self.truth_levels, name="truth_levels",
            ),
        )
        if self.default_truth_level not in self.truth_levels:
            raise RunMemoryConfigError(
                f"default_truth_level {self.default_truth_level!r} not in "
                f"truth_levels {list(self.truth_levels)!r}."
            )
        for sname in ("default_container_tag", "default_kind"):
            v = getattr(self, sname)
            if not isinstance(v, str) or not v:
                raise RunMemoryConfigError(
                    f"{sname} must be a non-empty string."
                )

        object.__setattr__(
            self,
            "higher_is_better",
            _freeze_bool_map("higher_is_better", self.higher_is_better),
        )
        for metric in self.metrics_to_track:
            if metric not in self.higher_is_better:
                raise RunMemoryConfigError(
                    f"metric {metric!r} missing from higher_is_better."
                )
        object.__setattr__(
            self,
            "metric_aliases",
            _freeze_tuple_map("metric_aliases", self.metric_aliases),
        )
        object.__setattr__(
            self,
            "ttl_seconds",
            _freeze_int_map("ttl_seconds", self.ttl_seconds),
        )

    # ------------------------------------------------------------------ #
    def resolve_metric(self, name: str) -> str:
        """Resolve a legacy alias to its canonical metric name."""
        if name in self.metric_aliases:
            return name
        for primary, alts in self.metric_aliases.items():
            if name in alts:
                return primary
        return name

    def ttl_for(self, kind: str) -> int:
        if kind in self.ttl_seconds:
            return int(self.ttl_seconds[kind])
        return int(self.ttl_seconds.get("run", 180 * 24 * 3600))

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "trend_stability_band": self.trend_stability_band,
            "min_runs_for_trend": self.min_runs_for_trend,
            "min_runs_for_policy": self.min_runs_for_policy,
            "max_runs": self.max_runs,
            "max_policies": self.max_policies,
            "metrics_to_track": list(self.metrics_to_track),
            "higher_is_better": dict(self.higher_is_better),
            "metric_aliases": {
                k: list(v) for k, v in self.metric_aliases.items()
            },
            "ttl_seconds": dict(self.ttl_seconds),
            "truth_levels": list(self.truth_levels),
            "default_truth_level": self.default_truth_level,
            "default_container_tag": self.default_container_tag,
            "default_kind": self.default_kind,
            "write_retries": self.write_retries,
            "latency_ring_size": self.latency_ring_size,
            "write_retry_backoff_seconds": self.write_retry_backoff_seconds,
            "default_importance": self.default_importance,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = False,
    ) -> "MemoryConfig":
        if not isinstance(data, ABCMapping):
            raise RunMemoryConfigError(
                "MemoryConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise RunMemoryConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k in ("metrics_to_track", "truth_levels"):
                if isinstance(v, str) or not isinstance(
                    v, (list, tuple, set, frozenset)
                ):
                    raise RunMemoryConfigError(
                        f"{k} must be a sequence of strings."
                    )
                kwargs[k] = tuple(v)
            elif k == "higher_is_better":
                if not isinstance(v, ABCMapping):
                    raise RunMemoryConfigError(
                        "higher_is_better must be a Mapping."
                    )
                kwargs[k] = dict(v)
            elif k == "metric_aliases":
                if not isinstance(v, ABCMapping):
                    raise RunMemoryConfigError(
                        "metric_aliases must be a Mapping."
                    )
                kwargs[k] = {kk: tuple(vv) for kk, vv in v.items()}
            elif k == "ttl_seconds":
                if not isinstance(v, ABCMapping):
                    raise RunMemoryConfigError(
                        "ttl_seconds must be a Mapping."
                    )
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except RunMemoryError:
            raise
        except (TypeError, ValueError) as exc:
            raise RunMemoryConfigError(
                f"failed to build MemoryConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "MemoryConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RunMemoryConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise RunMemoryConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    def with_overrides(self, **kwargs: Any) -> "MemoryConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise RunMemoryConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(self, other: "MemoryConfig") -> "MemoryConfig":
        defaults = MemoryConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    @classmethod
    def assert_compatible(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        if not isinstance(data, ABCMapping):
            raise RunMemoryParseError(
                "MemoryConfig.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise RunMemoryParseError(
                f"invalid schema_version {v!r} in MemoryConfig payload."
            )
        if v > SCHEMA_VERSION:
            raise RunMemoryParseError(
                f"MemoryConfig payload schema_version {v} is newer than "
                f"the current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise RunMemoryParseError(
                f"MemoryConfig payload schema_version {v} is older than "
                f"the current contract {SCHEMA_VERSION}."
            )

    def __hash__(self) -> int:
        return hash((
            self.trend_stability_band,
            self.min_runs_for_trend,
            self.min_runs_for_policy,
            self.max_runs,
            self.max_policies,
            self.metrics_to_track,
            tuple(sorted(self.higher_is_better.items())),
            tuple(
                sorted((k, tuple(v)) for k, v in self.metric_aliases.items())
            ),
            tuple(sorted(self.ttl_seconds.items())),
            self.truth_levels,
            self.default_truth_level,
            self.default_container_tag,
            self.default_kind,
            self.schema_version,
            self.write_retries,
            self.latency_ring_size,
            self.write_retry_backoff_seconds,
            self.default_importance,
        ))

    def __repr__(self) -> str:
        return (
            "MemoryConfig("
            f"band={self.trend_stability_band}, "
            f"max_runs={self.max_runs}, "
            f"metrics={len(self.metrics_to_track)})"
        )


# --------------------------------------------------------------------------- #
# RunSample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RunSample:
    """Immutable snapshot of a single completed run.

    ``run_data`` is recursively frozen — nested mappings become
    ``MappingProxyType`` and nested sequences become tuples — so the
    sample is truly immutable at every level.
    """

    run_id: str
    sequence: int
    timestamp: str
    observed_at: datetime
    kind: str
    truth_level: str
    container_tag: str
    run_data: Mapping[str, Any]
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in (
            "run_id", "timestamp", "kind",
            "truth_level", "container_tag",
        ):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise RunMemoryInputError(
                    f"{name} must be a non-empty string."
                )
        if not _is_real_int(self.sequence) or self.sequence < 0:
            raise RunMemoryInputError(
                "sequence must be a non-negative int."
            )
        if not isinstance(self.observed_at, datetime):
            raise RunMemoryInputError(
                "observed_at must be a datetime."
            )
        if self.observed_at.tzinfo is None:
            object.__setattr__(
                self, "observed_at",
                self.observed_at.replace(tzinfo=timezone.utc),
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise RunMemoryInputError(
                "schema_version must be a positive int."
            )
        if not isinstance(self.run_data, ABCMapping):
            raise RunMemoryInputError(
                "run_data must be a Mapping."
            )
        # Item fix #1: deep-freeze run_data.
        object.__setattr__(
            self, "run_data", _deep_freeze(dict(self.run_data)),
        )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return self.run_id

    # ------------------------------------------------------------------ #
    @classmethod
    def assert_compatible(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        if not isinstance(data, ABCMapping):
            raise RunMemoryParseError(
                "RunSample.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise RunMemoryParseError(
                f"invalid schema_version {v!r} in RunSample payload."
            )
        if v > SCHEMA_VERSION:
            raise RunMemoryParseError(
                f"RunSample payload schema_version {v} is newer than "
                f"the current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise RunMemoryParseError(
                f"RunSample payload schema_version {v} is older than "
                f"the current contract {SCHEMA_VERSION}."
            )

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        # Item fix #1: use `_to_plain` so nested frozen structures are
        # converted back to plain dicts/lists for JSON serialization.
        return {
            "run_id": self.run_id,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "observed_at": self.observed_at.isoformat(),
            "kind": self.kind,
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "run_data": _to_plain(self.run_data),
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RunSample":
        if not isinstance(data, ABCMapping):
            raise RunMemoryParseError(
                "RunSample.from_dict expects a Mapping."
            )
        # New wrapped format.
        if "run_data" in data:
            raw_run_id = data.get("run_id")
            run_data = data["run_data"]
            if not isinstance(run_data, ABCMapping):
                raise RunMemoryParseError(
                    "RunSample.run_data must be a Mapping."
                )
            # Item fix #3: wrapped casts.
            sequence = _coerce_int("sequence", data.get("sequence", 0))
            run_id = (
                str(raw_run_id) if raw_run_id is not None
                else f"run-{sequence:06d}"
            )
            timestamp = str(data.get("timestamp") or _iso_now())
            observed_at = (
                _parse_iso_datetime(data.get("observed_at"))
                or _parse_iso_datetime(timestamp)
                or datetime.now(timezone.utc)
            )
            kind = str(data.get("kind", DEFAULT_KIND))
            truth_level = str(data.get("truth_level", "measured"))
            container_tag = str(
                data.get("container_tag", DEFAULT_CONTAINER_TAG)
            )
            schema_version = _coerce_int(
                "schema_version",
                data.get("schema_version", SCHEMA_VERSION),
                positive=True,
            )
            return cls(
                run_id=run_id,
                sequence=sequence,
                timestamp=timestamp,
                observed_at=observed_at,
                kind=kind,
                truth_level=truth_level,
                container_tag=container_tag,
                run_data=dict(run_data),
                schema_version=schema_version,
            )
        # Legacy inline format: the whole dict is the run payload.
        payload = dict(data)
        raw_run_id = payload.pop("run_id", None)
        if raw_run_id is None:
            raise RunMemoryParseError(
                "legacy RunSample is missing 'run_id'."
            )
        run_id = str(raw_run_id)
        # Item fix #3: use a wrapped cast for legacy numeric run_id.
        sequence = (
            _coerce_int("run_id", raw_run_id)
            if isinstance(raw_run_id, int) and not isinstance(raw_run_id, bool)
            else 0
        )
        timestamp = str(payload.pop("timestamp", None) or _iso_now())
        observed_at = (
            _parse_iso_datetime(timestamp) or datetime.now(timezone.utc)
        )
        return cls(
            run_id=run_id,
            sequence=sequence,
            timestamp=timestamp,
            observed_at=observed_at,
            kind=DEFAULT_KIND,
            truth_level="measured",
            container_tag=DEFAULT_CONTAINER_TAG,
            run_data=payload,
            schema_version=SCHEMA_VERSION,
        )

    @classmethod
    def from_json(cls, payload: str) -> "RunSample":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RunMemoryParseError(
                f"RunSample.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Persistence bridges
    # ------------------------------------------------------------------ #
    def _render_content(self) -> str:
        # Item fix #1: use `_to_plain` so nested frozen structures are
        # JSON-serializable.
        return json.dumps(_to_plain(self.run_data), default=str, sort_keys=True)

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "kind": self.kind,
            "type": self.kind,
            "record_id": self.run_id,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "truth_level": self.truth_level,
            "container_tag": container_tag,
            "observed_at": self.observed_at.isoformat(),
            "timestamp": self.timestamp,
            "schema_version": self.schema_version,
        }
        # Surface scalar run_data fields.
        for k, v in self.run_data.items():
            if k in meta:
                continue
            if isinstance(v, (int, float, str, bool)) or v is None:
                meta.setdefault(k, v)
        # Item fix #11: also flatten nested `metrics.*` scalars so
        # FeedbackLoopGuard / BoundedRecall can read the common telemetry
        # keys without parsing content.
        nested = self.run_data.get("metrics")
        if isinstance(nested, ABCMapping):
            for k, v in nested.items():
                if k in meta:
                    continue
                if isinstance(v, (int, float, str, bool)) or v is None:
                    meta.setdefault(k, v)
        return meta

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
        truth_level: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Item fix #4: supports a ``truth_level=`` override."""
        tag = container_tag or self.container_tag
        if not isinstance(tag, str) or not tag:
            raise RunMemoryInputError(
                "container_tag must be a non-empty string."
            )
        meta = self._metadata(container_tag=tag)
        if truth_level is not None:
            if not isinstance(truth_level, str) or not truth_level:
                raise RunMemoryInputError(
                    "truth_level must be None or a non-empty string."
                )
            meta["truth_level"] = truth_level
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": tag,
            "metadata": meta,
        }

    def to_supermemory_payload(self) -> Dict[str, Any]:
        """Item fix #12: alias for ``to_episode_payload()`` — matches the
        sibling schema records."""
        return self.to_episode_payload()

    def to_memory_dict(self) -> Dict[str, Any]:
        return {
            "id": self.run_id,
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=self.container_tag),
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        # Item fix #2: recursive hashing — no JSON fallback.
        return hash((
            self.run_id, self.sequence, self.timestamp, self.observed_at,
            self.kind, self.truth_level, self.container_tag,
            _hashable(self.run_data), self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"RunSample(run_id={self.run_id!r}, "
            f"sequence={self.sequence}, kind={self.kind!r})"
        )


# --------------------------------------------------------------------------- #
# RunMemory
# --------------------------------------------------------------------------- #
_UNSET: Any = object()


class RunMemory:
    """Memory system for tracking agent performance across runs.

    Parameters
    ----------
    memory_file : str
        Path to the persistent memory file. Pass ``":memory:"`` to disable
        all disk I/O.
    config : MemoryConfig, optional
    max_runs : int or None, optional
        Overrides ``config.max_runs`` when provided. ``None`` means
        unlimited.
    max_policies : int, optional
        Overrides ``config.max_policies`` when provided.
    strict : bool
        If True, corrupt files / write failures / parse errors raise.
    autosave : bool
        If True, mutating methods persist immediately.
    auto_load : bool
        If True, load existing memory from disk on construction.
    write_retries : int, optional
        Overrides ``config.write_retries`` when provided.
    episodic, tier_manager, policy_memory
        Optional duck-typed mirror backends.
    """

    def __init__(
        self,
        memory_file: str = DEFAULT_MEMORY_FILE,
        *,
        config: Optional[MemoryConfig] = None,
        max_runs: Any = _UNSET,
        max_policies: Optional[int] = None,
        strict: bool = False,
        autosave: bool = True,
        auto_load: bool = True,
        write_retries: Optional[int] = None,
        episodic: Optional[Any] = None,
        tier_manager: Optional[Any] = None,
        policy_memory: Optional[Any] = None,
    ) -> None:
        if not isinstance(memory_file, str) or not memory_file:
            raise RunMemoryInputError(
                "memory_file must be a non-empty string."
            )
        if config is None:
            config = MemoryConfig()
        elif not isinstance(config, MemoryConfig):
            raise RunMemoryInputError(
                "config must be a MemoryConfig or None."
            )
        if max_runs is not _UNSET:
            config = config.with_overrides(max_runs=max_runs)
        if max_policies is not None:
            config = config.with_overrides(max_policies=max_policies)
        if write_retries is not None:
            config = config.with_overrides(write_retries=write_retries)

        self.memory_file: str = memory_file
        self._in_memory: bool = (memory_file == IN_MEMORY_PATH)
        self._config = config
        self._strict = bool(strict)
        self._autosave = bool(autosave)

        self._lock = threading.RLock()
        self._runs: List[RunSample] = []
        self._meta_policies: List[Dict[str, Any]] = []
        self._next_sequence: int = 0
        self._last_updated: Optional[str] = None

        # Reentrant context manager
        self._ctx_depth: int = 0
        self._ctx_start: Optional[float] = None

        # Counters
        self._write_successes: int = 0
        self._write_errors: int = 0
        self._read_errors: int = 0
        self._backend_writes: int = 0
        self._backend_errors: int = 0
        self._last_error: Optional[str] = None
        self._last_write_error: Optional[str] = None
        self._last_read_error: Optional[str] = None
        self._latency_ring: Deque[float] = deque(
            maxlen=self._config.latency_ring_size
        )
        self._started_at: float = time.monotonic()

        # Optional backends
        self._episodic = episodic
        self._tier_manager = tier_manager
        self._policy_memory = policy_memory

        if auto_load and not self._in_memory:
            self._load_memory()

        logger.debug(
            "RunMemory ready (file=%s, in_memory=%s, max_runs=%s, "
            "strict=%s, autosave=%s, runs=%d)",
            self.memory_file, self._in_memory,
            self._config.max_runs, self._strict, self._autosave,
            len(self._runs),
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MemoryConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def autosave(self) -> bool:
        return self._autosave

    @property
    def in_memory(self) -> bool:
        return self._in_memory

    @property
    def runs(self) -> List[Dict[str, Any]]:
        """Stored runs as plain dicts (backward-compatible view)."""
        with self._lock:
            return [_to_plain(s.run_data) for s in self._runs]

    @property
    def samples(self) -> List[RunSample]:
        with self._lock:
            return list(self._runs)

    @property
    def meta_policies(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(p) for p in self._meta_policies]

    @property
    def last_updated(self) -> Optional[str]:
        with self._lock:
            return self._last_updated

    @property
    def next_sequence(self) -> int:
        with self._lock:
            return self._next_sequence

    def __len__(self) -> int:
        with self._lock:
            return len(self._runs)

    def __contains__(self, run_id: object) -> bool:
        if not isinstance(run_id, str) or not run_id:
            return False
        with self._lock:
            return any(s.run_id == run_id for s in self._runs)

    def __iter__(self) -> Iterator[RunSample]:
        with self._lock:
            return iter(list(self._runs))

    # ---------------------------------------------------------- constructors
    @classmethod
    def from_config(
        cls,
        config: MemoryConfig,
        *,
        memory_file: str = IN_MEMORY_PATH,
        strict: bool = False,
        autosave: bool = False,
        auto_load: bool = False,
        episodic: Optional[Any] = None,
        tier_manager: Optional[Any] = None,
        policy_memory: Optional[Any] = None,
    ) -> "RunMemory":
        return cls(
            memory_file=memory_file,
            config=config,
            strict=strict,
            autosave=autosave,
            auto_load=auto_load,
            episodic=episodic,
            tier_manager=tier_manager,
            policy_memory=policy_memory,
        )

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Any,
        *,
        config: Optional[MemoryConfig] = None,
        strict: Optional[bool] = None,
        memory_file: str = IN_MEMORY_PATH,
        autosave: bool = False,
    ) -> "RunMemory":
        """Return ``pipeline.run_memory`` if present, else build fresh."""
        existing = getattr(pipeline, "run_memory", None)
        if isinstance(existing, cls):
            return existing
        resolved = False if strict is None else bool(strict)
        return cls(
            memory_file=memory_file,
            config=config,
            strict=resolved,
            autosave=autosave,
            auto_load=False,
            episodic=getattr(pipeline, "episodic", None),
            tier_manager=getattr(pipeline, "tier_manager", None),
            policy_memory=getattr(pipeline, "policy_memory", None),
        )

    # ---------------------------------------------------------- lifecycle
    def close(self, *, flush: bool = False) -> None:
        """Flush to disk and shut down best-effort.

        Parameters
        ----------
        flush : bool
            If True, force a persist even in memory-only mode (no-op
            for ``:memory:``). Default False is a no-op when
            ``autosave=False`` — the caller should pass
            ``flush=True`` to force a save on shutdown.
        """
        if self._in_memory:
            return
        if not flush and self._autosave:
            # Nothing to do — autosave already persisted.
            return
        try:
            self.save_memory()
        except RunMemoryError:
            logger.exception("RunMemory.close() failed to persist.")

    def __enter__(self) -> "RunMemory":
        with self._lock:
            if self._ctx_depth == 0:
                self._ctx_start = time.perf_counter()
            self._ctx_depth += 1
        logger.debug(
            "Entering scoped RunMemory session (depth=%d).", self._ctx_depth,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            self._ctx_depth -= 1
            if self._ctx_depth > 0:
                return
            started = self._ctx_start
            self._ctx_start = None
        elapsed = (
            time.perf_counter() - started if started is not None else 0.0
        )

        if exc_type is not None:
            logger.warning(
                "RunMemory context exited with %s after %.4fs; "
                "not saving.",
                exc_type.__name__, elapsed,
            )
            return

        if self._in_memory:
            logger.info(
                "RunMemory in-memory context closed cleanly in "
                "%.4fs (%d runs).", elapsed, len(self._runs),
            )
            return

        try:
            self.save_memory()
        except RunMemoryError:
            logger.exception("Failed to persist RunMemory on context exit.")
        finally:
            logger.info(
                "RunMemory context closed cleanly in %.4fs (%d runs).",
                elapsed, len(self._runs),
            )

    async def __aenter__(self) -> "RunMemory":
        return self.__enter__()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.__exit__(exc_type, exc, tb)

    # ---------------------------------------------------------- persistence
    def _load_memory(self) -> None:
        path = Path(self.memory_file)
        if not path.exists():
            logger.debug(
                "Memory file %s not found; starting empty.", path,
            )
            return
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            with self._lock:
                self._read_errors += 1
                self._last_error = str(exc)
                self._last_read_error = str(exc)
            logger.error("Corrupted memory file %s: %s", path, exc)
            if self._strict:
                raise RunMemoryCorruptionError(
                    f"Corrupted memory file: {path}"
                ) from exc
            return
        except OSError as exc:
            with self._lock:
                self._read_errors += 1
                self._last_error = str(exc)
                self._last_read_error = str(exc)
            logger.error("Could not read memory file %s: %s", path, exc)
            if self._strict:
                raise RunMemoryFileError(
                    f"Could not read memory file: {path}"
                ) from exc
            return

        if not isinstance(data, ABCMapping):
            msg = (
                f"Memory file root must be a Mapping, got "
                f"{type(data).__name__}."
            )
            with self._lock:
                self._read_errors += 1
                self._last_error = msg
                self._last_read_error = msg
            if self._strict:
                raise RunMemoryCorruptionError(msg)
            logger.warning(msg)
            return

        # Item fix #10: verify forward-compatibility of the whole file.
        try:
            self.assert_compatible(data, strict=self._strict)
        except RunMemoryError:
            if self._strict:
                raise
            logger.warning(
                "Memory file %s has an incompatible schema_version; "
                "reading leniently.", path,
            )

        with self._lock:
            raw_runs = data.get("runs", []) or []
            if not isinstance(raw_runs, list):
                if self._strict:
                    raise RunMemoryCorruptionError(
                        "'runs' must be a list."
                    )
                logger.warning("'runs' is not a list; starting empty.")
                raw_runs = []

            loaded: List[RunSample] = []
            for i, entry in enumerate(raw_runs):
                try:
                    loaded.append(RunSample.from_dict(entry))
                except (RunMemoryError, TypeError, ValueError) as exc:
                    with self._lock:
                        self._read_errors += 1
                        self._last_error = str(exc)
                        self._last_read_error = str(exc)
                    if self._strict:
                        raise RunMemoryCorruptionError(
                            f"Malformed run entry at index {i}: {exc}"
                        ) from exc
                    logger.warning(
                        "Skipping malformed run entry at index %d: %s",
                        i, exc,
                    )

            self._runs = loaded
            raw_policies = data.get("meta_policies", []) or []
            if isinstance(raw_policies, list):
                self._meta_policies = [dict(p) for p in raw_policies
                                       if isinstance(p, ABCMapping)]
            self._last_updated = data.get("last_updated")
            self._next_sequence = max(
                (s.sequence for s in loaded), default=-1
            ) + 1

        logger.info(
            "Loaded %d run(s), %d meta-policy(ies) from %s",
            len(self._runs), len(self._meta_policies), path,
        )

    def save_memory(self) -> None:
        """Persist memory to disk atomically."""
        if self._in_memory:
            return
        with self._lock:
            payload = {
                "schema_version": self._config.schema_version,
                "config": self._config.to_dict(),
                "runs": [s.to_dict() for s in self._runs],
                "meta_policies": [dict(p) for p in self._meta_policies],
                "next_sequence": self._next_sequence,
                "last_updated": _iso_now(),
            }

        path = Path(self.memory_file)
        attempts = self._config.write_retries + 1
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                self._atomic_write_once(payload, path)
                with self._lock:
                    self._write_successes += 1
                    self._last_updated = payload["last_updated"]
                    self._last_error = None
                    self._last_write_error = None
                logger.debug(
                    "Saved %d run(s) to %s", len(payload["runs"]), path,
                )
                return
            except (TypeError, ValueError) as exc:
                with self._lock:
                    self._write_errors += 1
                    self._last_error = str(exc)
                    self._last_write_error = str(exc)
                logger.error("Payload not JSON-serializable: %s", exc)
                if self._strict:
                    raise RunMemoryInputError(
                        f"payload is not JSON-serializable: {exc}"
                    ) from exc
                return
            except OSError as exc:
                last_exc = exc
                logger.warning(
                    "Write attempt %d/%d failed: %s",
                    attempt, attempts, exc,
                )
                if attempt < attempts:
                    # Item fix #9: config-driven backoff.
                    time.sleep(
                        self._config.write_retry_backoff_seconds * attempt
                    )

        with self._lock:
            self._write_errors += 1
            self._last_error = str(last_exc)
            self._last_write_error = str(last_exc)
        if self._strict:
            raise RunMemoryFileError(
                f"write failed after {attempts} attempts: {last_exc}"
            ) from last_exc

    @staticmethod
    def _atomic_write_once(
        payload: Mapping[str, Any], path: Path,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=str)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # --------------------------------------------------------------- mutation
    def add_run(
        self,
        run_data: Mapping[str, Any],
        *,
        run_id: Optional[str] = None,
        kind: Optional[str] = None,
        truth_level: Optional[str] = None,
        container_tag: Optional[str] = None,
        observed_at: Optional[Any] = None,
    ) -> str:
        """Add a completed run. Returns the assigned ``run_id``.

        A caller-supplied ``run_id`` (either in ``run_data['run_id']`` or
        as the ``run_id=`` kwarg) is preserved when it is a string. A
        caller-supplied ``run_data['timestamp']`` is likewise preserved.

        Item fix #7: a non-string ``run_id`` raises instead of being
        silently coerced.
        """
        if not isinstance(run_data, ABCMapping):
            raise RunMemoryInputError(
                "run_data must be a Mapping."
            )
        payload: Dict[str, Any] = dict(run_data)

        # Validate JSON-serializability up front so failures don't leave
        # in-memory state diverging from what can be persisted.
        try:
            json.dumps(payload, default=str)
        except (TypeError, ValueError) as exc:
            raise RunMemoryInputError(
                f"run_data is not JSON-serializable: {exc}"
            ) from exc

        # Truth level
        if truth_level is None:
            truth_level = self._config.default_truth_level
        if truth_level not in self._config.truth_levels:
            raise RunMemoryInputError(
                f"truth_level {truth_level!r} not in "
                f"{list(self._config.truth_levels)!r}."
            )

        # Kind and container tag
        resolved_kind = kind or self._config.default_kind
        if not isinstance(resolved_kind, str) or not resolved_kind:
            raise RunMemoryInputError(
                "kind must be a non-empty string."
            )
        resolved_tag = container_tag or self._config.default_container_tag
        if not isinstance(resolved_tag, str) or not resolved_tag:
            raise RunMemoryInputError(
                "container_tag must be a non-empty string."
            )

        # Observed-at timestamp
        obs = (
            _parse_iso_datetime(observed_at)
            if observed_at is not None
            else None
        )
        if observed_at is not None and obs is None:
            raise RunMemoryInputError(
                "observed_at could not be parsed."
            )

        # Item fix #7: reject non-string run_id rather than coercing.
        if run_id is not None and not isinstance(run_id, str):
            raise RunMemoryInputError(
                f"run_id must be a string or None, got "
                f"{type(run_id).__name__}."
            )
        payload_run_id = payload.get("run_id")
        if payload_run_id is not None and not isinstance(payload_run_id, str):
            raise RunMemoryInputError(
                f"run_data['run_id'] must be a string or None, got "
                f"{type(payload_run_id).__name__}."
            )

        with self._lock:
            candidate = run_id if run_id is not None else payload_run_id
            if candidate is None:
                candidate = f"run-{self._next_sequence:06d}"
            resolved_run_id = candidate

            # Timestamp: preserve caller's if present.
            if "timestamp" not in payload or not payload["timestamp"]:
                payload["timestamp"] = _iso_now()
            payload["run_id"] = resolved_run_id
            timestamp = str(payload["timestamp"])

            observed = (
                obs or _parse_iso_datetime(timestamp)
                or datetime.now(timezone.utc)
            )
            sequence = self._next_sequence
            self._next_sequence += 1

            sample = RunSample(
                run_id=resolved_run_id,
                sequence=sequence,
                timestamp=timestamp,
                observed_at=observed,
                kind=resolved_kind,
                truth_level=truth_level,
                container_tag=resolved_tag,
                run_data=payload,
                schema_version=self._config.schema_version,
            )
            self._runs.append(sample)
            cap = self._config.max_runs
            if cap is not None and len(self._runs) > cap:
                del self._runs[: len(self._runs) - cap]

        if self._autosave:
            self.save_memory()

        self._mirror_to_backends(sample)
        logger.debug(
            "Added run_id=%s sequence=%d (total=%d)",
            resolved_run_id, sequence, len(self._runs),
        )
        return resolved_run_id

    async def add_run_async(
        self,
        run_data: Mapping[str, Any],
        **kwargs: Any,
    ) -> str:
        return await asyncio.to_thread(self.add_run, run_data, **kwargs)

    def clear_memory(
        self,
        *,
        autosave: Optional[bool] = None,
        reset_sequence: bool = False,
    ) -> int:
        """Clear all memory. Returns the number of runs removed."""
        with self._lock:
            removed = len(self._runs)
            self._runs.clear()
            self._meta_policies.clear()
            if reset_sequence:
                self._next_sequence = 0
            self._last_updated = None
        if autosave if autosave is not None else self._autosave:
            self.save_memory()
        logger.info("RunMemory cleared (%d runs removed).", removed)
        return removed

    async def clear_memory_async(self, **kwargs: Any) -> int:
        return await asyncio.to_thread(self.clear_memory, **kwargs)

    def remove_run(self, run_id: str) -> bool:
        """Remove a single run by ``run_id``. Returns True if removed."""
        if not isinstance(run_id, str) or not run_id:
            raise RunMemoryInputError(
                "run_id must be a non-empty string."
            )
        with self._lock:
            removed = False
            for i, sample in enumerate(self._runs):
                if sample.run_id == run_id:
                    del self._runs[i]
                    removed = True
                    break
        if not removed:
            return False
        if self._autosave:
            self.save_memory()
        return True

    def resize(self, max_runs: Optional[int]) -> int:
        """Change the ring-buffer cap at runtime. Returns removed count.

        ``None`` disables trimming.
        """
        if max_runs is not None:
            if not _is_real_int(max_runs) or max_runs <= 0:
                raise RunMemoryInputError(
                    f"max_runs must be None or a positive int "
                    f"(got {max_runs!r})."
                )
        with self._lock:
            self._config = self._config.with_overrides(max_runs=max_runs)
            removed = 0
            if max_runs is not None and len(self._runs) > max_runs:
                removed = len(self._runs) - max_runs
                del self._runs[:removed]
        if removed and self._autosave:
            self.save_memory()
        return removed

    def prune(
        self,
        *,
        now: Optional[float] = None,
        autosave: Optional[bool] = None,
    ) -> int:
        """Drop samples whose per-kind TTL has expired.

        Returns the number of samples removed.
        """
        if now is None:
            now = time.time()
        elif not _is_finite_nonneg(now):
            raise RunMemoryInputError(
                "now must be a finite non-negative number."
            )
        with self._lock:
            kept: List[RunSample] = []
            removed = 0
            for sample in self._runs:
                ttl = self._config.ttl_for(sample.kind)
                age = max(0.0, now - sample.observed_at.timestamp())
                if age > ttl:
                    removed += 1
                else:
                    kept.append(sample)
            if removed == 0:
                return 0
            self._runs = kept
            self._next_sequence = max(
                (s.sequence for s in kept), default=-1
            ) + 1
        if autosave if autosave is not None else self._autosave:
            self.save_memory()
        logger.info("RunMemory pruned %d expired run(s).", removed)
        return removed

    # ------------------------------------------------------------------ reads
    def get_recent_runs(self, n: int = 5) -> List[Dict[str, Any]]:
        """Return the ``n`` most recent runs as plain dicts."""
        if not _is_real_int(n):
            raise RunMemoryInputError(
                f"n must be an int, got {type(n).__name__}."
            )
        if n <= 0:
            return []
        with self._lock:
            return [_to_plain(s.run_data) for s in self._runs[-n:]]

    def get(self, run_id: str) -> Optional[RunSample]:
        if not isinstance(run_id, str) or not run_id:
            raise RunMemoryInputError(
                "run_id must be a non-empty string."
            )
        with self._lock:
            for s in self._runs:
                if s.run_id == run_id:
                    return s
        return None

    def get_best_run(
        self, metric: str = "quality",
    ) -> Optional[Dict[str, Any]]:
        """Return the run with the best value for ``metric``, or None."""
        resolved = self._config.resolve_metric(metric)
        direction = self._config.higher_is_better.get(resolved, True)
        with self._lock:
            scored: List[Tuple[RunSample, float]] = []
            for s in self._runs:
                v = _extract_metric(
                    s.run_data, resolved, self._config.metric_aliases,
                )
                if v is not None:
                    scored.append((s, v))
        if not scored:
            return None
        best = max(scored, key=lambda t: t[1]) if direction else min(
            scored, key=lambda t: t[1]
        )
        return _to_plain(best[0].run_data)

    # ------------------------------------------------------------ counts
    def count_by_kind(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for s in self._runs:
                out[s.kind] = out.get(s.kind, 0) + 1
            return out

    def count_by_container_tag(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for s in self._runs:
                out[s.container_tag] = out.get(s.container_tag, 0) + 1
            return out

    def count_by_truth_level(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = {}
            for s in self._runs:
                out[s.truth_level] = out.get(s.truth_level, 0) + 1
            return out

    # ------------------------------------------------------------ trend / stats
    def get_performance_trend(
        self, metric: str = "quality",
    ) -> Dict[str, Any]:
        """Analyse the trend of a metric over stored runs."""
        if not isinstance(metric, str) or not metric:
            raise RunMemoryInputError(
                "metric must be a non-empty string."
            )
        resolved = self._config.resolve_metric(metric)
        with self._lock:
            values = [
                v for v in (
                    _extract_metric(
                        s.run_data, resolved,
                        self._config.metric_aliases,
                    )
                    for s in self._runs
                )
                if v is not None
            ]

        if len(values) < self._config.min_runs_for_trend:
            return {
                "trend": "insufficient_data",
                "metric": resolved,
                "values": values,
            }

        midpoint = len(values) // 2
        first_half = values[:midpoint] if midpoint > 0 else values[:1]
        second_half = values[midpoint:] if midpoint > 0 else values[1:]
        first_avg = sum(first_half) / len(first_half)
        second_avg = sum(second_half) / len(second_half)

        band = self._config.trend_stability_band
        higher_is_better = self._config.higher_is_better.get(resolved, True)

        if first_avg == 0:
            if second_avg == 0:
                trend = "stable"
            else:
                trend = (
                    "improving"
                    if (second_avg > 0) == higher_is_better
                    else "declining"
                )
        else:
            ratio = second_avg / first_avg
            if ratio > 1.0 + band:
                trend = "improving" if higher_is_better else "declining"
            elif ratio < 1.0 - band:
                trend = "declining" if higher_is_better else "improving"
            else:
                trend = "stable"

        raw_change = second_avg - first_avg
        improvement = raw_change if higher_is_better else -raw_change
        return {
            "trend": trend,
            "metric": resolved,
            "values": values,
            "first_half_avg": first_avg,
            "second_half_avg": second_avg,
            "improvement": improvement,
            "raw_change": raw_change,
        }

    def metric_statistics(
        self, metric: str = "quality",
    ) -> Dict[str, Optional[float]]:
        """Return count / mean / median / min / max / p95 for a metric."""
        if not isinstance(metric, str) or not metric:
            raise RunMemoryInputError(
                "metric must be a non-empty string."
            )
        resolved = self._config.resolve_metric(metric)
        with self._lock:
            values = [
                v for v in (
                    _extract_metric(
                        s.run_data, resolved,
                        self._config.metric_aliases,
                    )
                    for s in self._runs
                )
                if v is not None
            ]
        if not values:
            return {
                "count": 0, "mean": None, "median": None,
                "min": None, "max": None, "p95": None,
            }
        ordered = sorted(values)
        n = len(ordered)
        if n % 2 == 0:
            median = (ordered[n // 2 - 1] + ordered[n // 2]) / 2.0
        else:
            median = ordered[n // 2]
        return {
            "count": n,
            "mean": sum(ordered) / n,
            "median": median,
            "min": ordered[0],
            "max": ordered[-1],
            "p95": _percentile(ordered, 95),
        }

    # -------------------------------------------------------- meta-policy
    def generate_meta_policy(
        self, *, save: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Generate a meta-policy from historical performance."""
        with self._lock:
            snapshot = list(self._runs)

        if len(snapshot) < self._config.min_runs_for_policy:
            return {
                "policy": "insufficient_data",
                "recommendations": [],
                "based_on_runs": len(snapshot),
                "schema_version": self._config.schema_version,
            }

        trends: Dict[str, Dict[str, Any]] = {}
        for metric in self._config.metrics_to_track:
            trends[metric] = self.get_performance_trend(metric)

        recommendations: List[str] = []

        score_trend = trends.get("quality", {}).get("trend")
        if score_trend == "declining":
            recommendations.append(
                "Performance is declining - review recent changes"
            )
        elif score_trend == "improving":
            recommendations.append(
                "Performance improving - continue current strategy"
            )

        energy_trend = trends.get("energy_wh", {}).get("trend")
        if energy_trend == "declining":
            recommendations.append(
                "Energy consumption is increasing - optimize resource usage"
            )
        elif energy_trend == "improving":
            recommendations.append(
                "Energy consumption decreasing - efficiency gains detected"
            )

        carbon_trend = trends.get("carbon_gco2e", {}).get("trend")
        if carbon_trend == "declining":
            recommendations.append(
                "Carbon emissions rising - consider greener execution targets"
            )
        elif carbon_trend == "improving":
            recommendations.append(
                "Carbon emissions decreasing - sustainability improving"
            )

        policy: Dict[str, Any] = {
            "policy": "generated",
            "recommendations": recommendations,
            "trends": trends,
            "based_on_runs": len(snapshot),
            "generated_at": _iso_now(),
            "schema_version": self._config.schema_version,
        }

        published: Optional[str] = None
        if self._policy_memory is not None:
            publisher = getattr(self._policy_memory, "publish", None)
            if callable(publisher):
                try:
                    published = publisher(
                        {"recommendations": recommendations, "trends": trends},
                        kind="meta_policy",
                        approved_by="run_memory",
                        bump="patch",
                    )
                    with self._lock:
                        self._backend_writes += 1
                except Exception as exc:  # pragma: no cover - backend
                    with self._lock:
                        self._backend_errors += 1
                        self._last_error = f"policy_memory.publish: {exc}"
                    logger.warning(
                        "policy_memory.publish failed: %s", exc,
                    )
                    if self._strict:
                        raise

        with self._lock:
            self._meta_policies.append(dict(policy))
            cap = self._config.max_policies
            if len(self._meta_policies) > cap:
                del self._meta_policies[: len(self._meta_policies) - cap]

        if save if save is not None else self._autosave:
            self.save_memory()

        logger.info(
            "Generated meta-policy from %d run(s) with %d recommendation(s).",
            len(snapshot), len(recommendations),
        )
        if published is not None:
            policy["published_version"] = published
        return policy

    async def generate_meta_policy_async(
        self, **kwargs: Any,
    ) -> Dict[str, Any]:
        return await asyncio.to_thread(self.generate_meta_policy, **kwargs)

    # ---------------------------------------------------------- backends
    def _mirror_to_backends(self, sample: RunSample) -> None:
        if self._episodic is not None:
            storer = getattr(self._episodic, "store", None)
            if callable(storer):
                try:
                    storer(sample.to_episode_payload())
                    with self._lock:
                        self._backend_writes += 1
                except Exception as exc:  # pragma: no cover - backend
                    with self._lock:
                        self._backend_errors += 1
                        self._last_error = f"episodic.store: {exc}"
                    logger.warning("episodic.store failed: %s", exc)
        if self._tier_manager is not None:
            storer = getattr(self._tier_manager, "store", None)
            if callable(storer):
                # Item fix #8: derive importance from the payload's
                # quality_score when present instead of hard-coding 0.5.
                importance = _derive_importance_from_run(
                    sample.run_data,
                    default=self._config.default_importance,
                )
                try:
                    storer(
                        record_id=sample.run_id,
                        kind=sample.kind,
                        payload=_to_plain(sample.run_data),
                        importance=importance,
                        truth_level=sample.truth_level,
                        container_tag=sample.container_tag,
                        observed_at=sample.observed_at,
                    )
                    with self._lock:
                        self._backend_writes += 1
                except Exception as exc:  # pragma: no cover - backend
                    with self._lock:
                        self._backend_errors += 1
                        self._last_error = f"tier_manager.store: {exc}"
                    logger.warning("tier_manager.store failed: %s", exc)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Item fix #5: stamp ``schema_version`` and report per-kind /
        per-container distributions alongside the counters."""
        with self._lock:
            lats = list(self._latency_ring)
            mean_lat = sum(lats) / len(lats) if lats else 0.0
            kind_mix: Dict[str, int] = {}
            tag_mix: Dict[str, int] = {}
            truth_mix: Dict[str, int] = {}
            for s in self._runs:
                kind_mix[s.kind] = kind_mix.get(s.kind, 0) + 1
                tag_mix[s.container_tag] = tag_mix.get(s.container_tag, 0) + 1
                truth_mix[s.truth_level] = (
                    truth_mix.get(s.truth_level, 0) + 1
                )
            return {
                "schema_version": SCHEMA_VERSION,
                "runs": len(self._runs),
                "meta_policies": len(self._meta_policies),
                "next_sequence": self._next_sequence,
                "last_updated": self._last_updated,
                "max_runs": self._config.max_runs,
                "max_policies": self._config.max_policies,
                "kind_mix": kind_mix,
                "container_tag_mix": tag_mix,
                "truth_level_mix": truth_mix,
                "in_memory": self._in_memory,
                "strict": self._strict,
                "autosave": self._autosave,
                "write_successes": self._write_successes,
                "write_errors": self._write_errors,
                "read_errors": self._read_errors,
                "backend_writes": self._backend_writes,
                "backend_errors": self._backend_errors,
                "last_error": self._last_error,
                "last_write_error": self._last_write_error,
                "last_read_error": self._last_read_error,
                "mean_latency_ms": mean_lat,
                "p50_latency_ms": _percentile(lats, 50),
                "p95_latency_ms": _percentile(lats, 95),
                "max_latency_ms": max(lats) if lats else 0.0,
                "config": self._config.to_dict(),
                "has_episodic_backend": self._episodic is not None,
                "has_tier_backend": self._tier_manager is not None,
                "has_policy_backend": self._policy_memory is not None,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self, *, clear_runs: bool = True) -> int:
        """Reset counters (and optionally the run history)."""
        with self._lock:
            removed = 0
            if clear_runs:
                removed = len(self._runs)
                self._runs.clear()
                self._meta_policies.clear()
                self._next_sequence = 0
                self._last_updated = None
            self._write_successes = 0
            self._write_errors = 0
            self._read_errors = 0
            self._backend_writes = 0
            self._backend_errors = 0
            self._last_error = None
            self._last_write_error = None
            self._last_read_error = None
            self._latency_ring.clear()
            self._started_at = time.monotonic()
        return removed

    # -------------------------------------------------- Supermemory bridge
    def to_supermemory_payloads(
        self,
        *,
        container_tag: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return payloads shaped for ``SupermemoryAdapter.remember()``."""
        with self._lock:
            snapshot = list(self._runs)
        return [
            s.to_episode_payload(container_tag=container_tag)
            for s in snapshot
        ]

    def to_memory_dicts(self) -> List[Dict[str, Any]]:
        """Return entries shaped for ``BoundedRecall`` / ``RecallBundle``."""
        with self._lock:
            snapshot = list(self._runs)
        return [s.to_memory_dict() for s in snapshot]

    @classmethod
    def from_supermemory_results(
        cls,
        results: Iterable[Mapping[str, Any]],
        *,
        memory_file: str = IN_MEMORY_PATH,
        config: Optional[MemoryConfig] = None,
        strict: bool = False,
        autosave: bool = False,
    ) -> "RunMemory":
        """Build a store from a sequence of Supermemory-style results."""
        mem = cls(
            memory_file=memory_file,
            config=config,
            strict=strict,
            autosave=autosave,
            auto_load=False,
        )
        for r in results:
            if not isinstance(r, ABCMapping):
                continue
            meta = r.get("metadata") or {}
            if not isinstance(meta, ABCMapping):
                meta = {}
            run_id_raw = (
                meta.get("run_id") or meta.get("record_id") or r.get("id")
            )
            run_id = str(run_id_raw) if run_id_raw else None
            truth = meta.get("truth_level")
            if not isinstance(truth, str) or not truth:
                truth = None
            kind = meta.get("kind") if isinstance(meta.get("kind"), str) else None
            tag = r.get("container_tag") or meta.get("container_tag")
            observed = meta.get("observed_at")

            run_data: Dict[str, Any] = dict(meta)
            content = r.get("content")
            if isinstance(content, str) and content.strip():
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, ABCMapping):
                        for k, v in parsed.items():
                            run_data.setdefault(k, v)
                except (json.JSONDecodeError, TypeError, ValueError):
                    run_data.setdefault("content", content)
            elif isinstance(content, ABCMapping):
                for k, v in content.items():
                    run_data.setdefault(k, v)

            try:
                mem.add_run(
                    run_data,
                    run_id=run_id,
                    truth_level=truth,
                    container_tag=tag if isinstance(tag, str) else None,
                    kind=kind,
                    observed_at=observed,
                )
            except RunMemoryError as exc:
                logger.warning(
                    "from_supermemory_results: skipping malformed result: %s",
                    exc,
                )
        return mem

    # ---------------------------------------------------------- serialization
    @classmethod
    def assert_compatible(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        """Item fix #6 / #10: verify a persisted payload's schema."""
        if not isinstance(data, ABCMapping):
            raise RunMemoryParseError(
                "RunMemory.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise RunMemoryParseError(
                f"invalid schema_version {v!r} in RunMemory payload."
            )
        if v > SCHEMA_VERSION:
            raise RunMemoryParseError(
                f"RunMemory payload schema_version {v} is newer than "
                f"the current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise RunMemoryParseError(
                f"RunMemory payload schema_version {v} is older than "
                f"the current contract {SCHEMA_VERSION}."
            )

    def snapshot(self) -> Dict[str, Any]:
        """Return a structured, plain-dict view of the current state."""
        with self._lock:
            runs = [s.to_dict() for s in self._runs]
            policies = [dict(p) for p in self._meta_policies]
        return {
            "schema_version": SCHEMA_VERSION,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "autosave": self._autosave,
            "in_memory": self._in_memory,
            "memory_file": self.memory_file,
            "next_sequence": self._next_sequence,
            "last_updated": self._last_updated,
            "runs": runs,
            "meta_policies": policies,
            "statistics": self.statistics(),
        }

    def to_dict(self, *, include_runs: bool = True) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "autosave": self._autosave,
                "next_sequence": self._next_sequence,
                "last_updated": self._last_updated,
                "statistics": self.statistics(),
            }
            if include_runs:
                payload["runs"] = [s.to_dict() for s in self._runs]
                payload["meta_policies"] = [
                    dict(p) for p in self._meta_policies
                ]
        return payload

    def to_json(
        self,
        *,
        include_runs: bool = True,
        indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(include_runs=include_runs),
            default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        memory_file: str = IN_MEMORY_PATH,
        autosave: bool = False,
        auto_load: bool = False,
        restore_runs: bool = True,
        episodic: Optional[Any] = None,
        tier_manager: Optional[Any] = None,
        policy_memory: Optional[Any] = None,
        strict: Optional[bool] = None,
    ) -> "RunMemory":
        if not isinstance(data, ABCMapping):
            raise RunMemoryParseError(
                "RunMemory.from_dict expects a Mapping."
            )
        # Item fix #6: verify schema compatibility before reconstructing.
        cls.assert_compatible(data, strict=False)

        cfg_blob = data.get("config", {})
        config = (
            cfg_blob if isinstance(cfg_blob, MemoryConfig)
            else MemoryConfig.from_dict(cfg_blob)
        )
        # Item fix #12: default to `True` when absent — matches siblings.
        resolved_strict = (
            bool(data.get("strict", True)) if strict is None else bool(strict)
        )
        mem = cls(
            memory_file=memory_file,
            config=config,
            strict=resolved_strict,
            autosave=autosave,
            auto_load=auto_load,
            episodic=episodic,
            tier_manager=tier_manager,
            policy_memory=policy_memory,
        )
        if restore_runs:
            raw_runs = data.get("runs", []) or []
            if not isinstance(raw_runs, list):
                raise RunMemoryParseError("'runs' must be a list.")
            loaded: List[RunSample] = []
            for i, entry in enumerate(raw_runs):
                try:
                    loaded.append(RunSample.from_dict(entry))
                except (RunMemoryError, TypeError, ValueError) as exc:
                    if mem._strict:
                        raise RunMemoryCorruptionError(
                            f"Malformed run entry at index {i}: {exc}"
                        ) from exc
                    logger.warning(
                        "from_dict: skipping malformed run at index %d: %s",
                        i, exc,
                    )
            with mem._lock:
                mem._runs = loaded
                raw_policies = data.get("meta_policies", []) or []
                if isinstance(raw_policies, list):
                    mem._meta_policies = [
                        dict(p) for p in raw_policies
                        if isinstance(p, ABCMapping)
                    ]
                mem._last_updated = data.get("last_updated")
                mem._next_sequence = _coerce_int(
                    "next_sequence",
                    data.get(
                        "next_sequence",
                        max((s.sequence for s in loaded), default=-1) + 1,
                    ),
                )
        return mem

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        memory_file: str = IN_MEMORY_PATH,
        autosave: bool = False,
        auto_load: bool = False,
        restore_runs: bool = True,
        episodic: Optional[Any] = None,
        tier_manager: Optional[Any] = None,
        policy_memory: Optional[Any] = None,
        strict: Optional[bool] = None,
    ) -> "RunMemory":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RunMemoryParseError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise RunMemoryParseError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(
            data,
            memory_file=memory_file,
            autosave=autosave,
            auto_load=auto_load,
            restore_runs=restore_runs,
            episodic=episodic,
            tier_manager=tier_manager,
            policy_memory=policy_memory,
            strict=strict,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "RunMemory("
                f"file={self.memory_file!r}, "
                f"runs={len(self._runs)}, "
                f"policies={len(self._meta_policies)}, "
                f"max_runs={self._config.max_runs}, "
                f"strict={self._strict})"
            )


__all__ = [
    "DEFAULT_CONTAINER_TAG",
    "DEFAULT_HIGHER_IS_BETTER",
    "DEFAULT_KIND",
    "DEFAULT_MEMORY_FILE",
    "DEFAULT_METRICS",
    "DEFAULT_METRIC_ALIASES",
    "DEFAULT_TTL_SECONDS",
    "DEFAULT_TRUTH_LEVELS",
    "IN_MEMORY_PATH",
    "MemoryConfig",
    "RunMemory",
    "RunMemoryError",
    "RunMemoryInputError",
    "RunMemoryConfigError",
    "RunMemoryFileError",
    "RunMemoryCorruptionError",
    "RunMemoryParseError",
    "RunSample",
    "SCHEMA_VERSION",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.run_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    import tempfile as _tf

    # --------------------------------------------------- 1. Basic + ring trim
    tmpdir = _tf.mkdtemp(prefix="run_memory_smoke_")
    mem_file = os.path.join(tmpdir, "run_memory.json")

    mem = RunMemory(memory_file=mem_file, max_runs=5, strict=False)
    assert mem.config.max_runs == 5
    assert not mem.in_memory
    print("repr         :", mem)

    for i in range(6):
        run_id = mem.add_run(
            {
                "quality": 0.5 + 0.05 * i,
                "metrics": {
                    "accuracy": 0.7 + 0.02 * i,
                    "energy_wh": 100.0 + 10 * i,
                    "carbon_gco2e": 20.0 + 3 * i,
                },
                "notes": f"run-{i}",
            },
        )
        assert run_id.startswith("run-"), run_id
    assert len(mem) == 5
    print("ring trim    :", len(mem), "(max_runs=5)")

    # --------------------------------------------------- 2. Deep freeze
    sample = mem.samples[0]
    try:
        sample.run_data["notes"] = "hacked"  # type: ignore[index]
    except TypeError:
        pass
    else:
        raise AssertionError("top-level run_data should be frozen")
    # Nested metadata is also frozen.
    try:
        sample.run_data["metrics"]["accuracy"] = 0.0  # type: ignore[index]
    except TypeError:
        print("deep freeze  : OK")
    else:
        raise AssertionError("nested metrics should be frozen")

    # --------------------------------------------------- 3. Nested hash
    hash(sample)
    print("nested hash  : OK")

    # --------------------------------------------------- 4. Metric aliases
    trend_legacy = mem.get_performance_trend("final_score")
    trend_canonical = mem.get_performance_trend("quality")
    assert trend_legacy["metric"] == "quality"
    assert trend_legacy["trend"] == trend_canonical["trend"]
    print("alias        :", trend_legacy["trend"])

    # --------------------------------------------------- 5. Trend / stats
    trend_energy = mem.get_performance_trend("energy_consumption")
    assert trend_energy["metric"] == "energy_wh"
    assert trend_energy["trend"] in ("declining", "stable")
    print("trend energy :", trend_energy["trend"])

    stats = mem.metric_statistics("quality")
    assert stats["count"] == 5
    print("metric stats :", stats["count"], "values, mean=",
          f"{stats['mean']:.3f}")

    # --------------------------------------------------- 6. Caller-supplied run_id + timestamp
    kept_id = mem.add_run({"quality": 0.99}, run_id="explicit-1")
    assert kept_id == "explicit-1"
    assert mem.get("explicit-1") is not None
    ts = "2024-01-01T00:00:00+00:00"
    mem.add_run({"quality": 0.5, "timestamp": ts, "run_id": "ts-1"})
    assert mem.get("ts-1").timestamp == ts
    print("preserve     : OK")

    # Item fix #7: non-string run_id rejected.
    try:
        mem.add_run({"quality": 0.5}, run_id=42)  # type: ignore[arg-type]
    except RunMemoryInputError as exc:
        print("int run_id   : rejected ->", exc)
    else:
        raise AssertionError("expected RunMemoryInputError")

    # --------------------------------------------------- 7. Bridges + metrics flattening
    payloads = mem.to_supermemory_payloads()
    assert all(
        {"content", "container_tag", "metadata"} <= set(p) for p in payloads
    )
    # Item fix #11: nested `metrics.energy_wh` surfaces at top level of metadata.
    first_meta = payloads[-1]["metadata"]
    assert "energy_wh" in first_meta or "accuracy" in first_meta
    dicts = mem.to_memory_dicts()
    assert all({"id", "content", "metadata"} <= set(d) for d in dicts)
    print("bridges      :", len(payloads), "payloads,", len(dicts), "dicts")

    # --------------------------------------------------- 8. from_supermemory_results
    restored = RunMemory.from_supermemory_results(payloads)
    assert len(restored) == len(payloads)
    print("from_sm      :", len(restored), "runs restored")

    # --------------------------------------------------- 9. Backends
    from .episodic_memory import EpisodicMemory, IN_MEMORY_PATH
    from .memory_tier import MemoryTierConfig, MemoryTierManager
    from .policy_memory import PolicyMemory
    from .supermemory_adapter import SupermemoryAdapter
    from .write_governor import WriteGovernor

    adapter = SupermemoryAdapter()
    governor = WriteGovernor()
    governor.register_writer(
        "run_memory", capabilities={"write:policy"},
    )
    episodic = EpisodicMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    tier_manager = MemoryTierManager(
        config=MemoryTierConfig(hot_capacity=16, warm_capacity=16),
    )
    policy_memory = PolicyMemory(adapter=adapter, governor=governor)

    mem2 = RunMemory(
        memory_file=IN_MEMORY_PATH,
        max_runs=100,
        episodic=episodic,
        tier_manager=tier_manager,
        policy_memory=policy_memory,
    )
    # Item fix #8: importance derived from `quality_score`.
    mem2.add_run({"quality_score": 0.95, "energy_wh": 90})
    mem2.add_run({"quality_score": 0.4, "energy_wh": 110})
    mem2.add_run({"quality_score": 0.3, "energy_wh": 120})
    mem2.add_run({"quality_score": 0.2, "energy_wh": 130})
    assert episodic.count >= 4
    assert tier_manager.size() >= 4
    hot_ids = tier_manager.record_ids(MemoryTier.HOT) \
        if hasattr(tier_manager, "record_ids") else []
    print("backends     : episodic=%d, tiers=%d, hot=%d" % (
        episodic.count, tier_manager.size(), len(hot_ids),
    ))

    # Meta-policy published via PolicyMemory.
    policy = mem2.generate_meta_policy()
    assert policy["policy"] == "generated"
    assert "published_version" in policy
    assert policy_memory.current_version == policy["published_version"]
    print("meta-policy  : published", policy["published_version"])

    # --------------------------------------------------- 10. :memory: sentinel
    mem3 = RunMemory(memory_file=IN_MEMORY_PATH)
    mem3.add_run({"quality": 0.7})
    assert mem3.in_memory
    assert not Path(IN_MEMORY_PATH).exists()
    print(":memory:     : OK")

    # --------------------------------------------------- 11. Serialization round-trip
    payload = mem.to_json()
    restored_mem = RunMemory.from_json(payload)
    assert len(restored_mem) == len(mem)
    assert restored_mem.to_dict() == mem.to_dict()
    print("serialize RT : OK")

    cfg = MemoryConfig()
    assert MemoryConfig.from_dict(cfg.to_dict()) == cfg
    assert MemoryConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(max_runs=99)
    assert cfg2.max_runs == 99 and cfg.max_runs == 1_000
    hash(cfg)
    print("cfg RT       : OK (hashable)")

    sample_rt = RunSample.from_dict(sample.to_dict())
    assert sample_rt.run_id == sample.run_id
    assert dict(sample_rt.run_data) == dict(sample.run_data)
    hash(sample_rt)
    print("sample RT    : OK")

    # --------------------------------------------------- 12. Async
    async def _run_async():
        rid = await mem3.add_run_async({"quality": 0.6})
        policy = await mem3.generate_meta_policy_async()
        return rid, policy

    rid, policy = asyncio.run(_run_async())
    assert rid.startswith("run-")
    print("async        :", rid)

    # --------------------------------------------------- 13. Async context manager
    async def _async_ctx():
        async with RunMemory(memory_file=IN_MEMORY_PATH) as m:
            await m.add_run_async({"quality": 0.8})
            return len(m)

    n = asyncio.run(_async_ctx())
    assert n == 1
    print("async ctx    : OK")

    # --------------------------------------------------- 14. Sync reentrant context manager
    with RunMemory(memory_file=os.path.join(tmpdir, "ctx.json")) as outer:
        outer.add_run({"quality": 0.9})
        with outer as inner:
            inner.add_run({"quality": 0.91})
        assert outer._ctx_depth == 1
    print("ctx mgr      : OK (reentrant)")

    # --------------------------------------------------- 15. Statistics / reset
    stats = mem.statistics()
    assert stats["schema_version"] == SCHEMA_VERSION
    assert "container_tag_mix" in stats
    assert "kind_mix" in stats
    assert "truth_level_mix" in stats
    assert stats["last_write_error"] is None
    print("statistics   : OK")
    cleared = mem.reset()
    assert cleared > 0
    assert mem.statistics()["runs"] == 0
    print("reset        :", cleared, "runs cleared")

    # --------------------------------------------------- 16. prune
    import time as _t
    mem4 = RunMemory(
        memory_file=IN_MEMORY_PATH, max_runs=100, autosave=False,
    )
    mem4.add_run({"quality": 0.9}, kind="run")
    # Backdate the sample to be older than the run TTL.
    old_ts = _t.time() - (200 * 24 * 3600)   # 200 days ago
    with mem4._lock:
        s = mem4._runs[-1]
        backdated = RunSample(
            run_id=s.run_id, sequence=s.sequence,
            timestamp=s.timestamp,
            observed_at=datetime.fromtimestamp(old_ts, tz=timezone.utc),
            kind=s.kind, truth_level=s.truth_level,
            container_tag=s.container_tag, run_data=dict(s.run_data),
            schema_version=s.schema_version,
        )
        mem4._runs[-1] = backdated
    removed = mem4.prune()
    assert removed == 1
    print("prune        :", removed)

    # --------------------------------------------------- 17. resize
    resized = mem4.resize(2)
    print("resize       : OK ->", resized)

    # --------------------------------------------------- 18. snapshot
    snap = mem4.snapshot()
    assert snap["schema_version"] == SCHEMA_VERSION
    assert "runs" in snap and "statistics" in snap
    print("snapshot     : OK")

    # --------------------------------------------------- 19. count_by_*
    mem5 = RunMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    mem5.add_run({"quality": 0.9}, kind="run")
    mem5.add_run({"quality": 0.9}, kind="run")
    mem5.add_run({"quality": 0.9}, kind="policy")
    assert mem5.count_by_kind() == {"run": 2, "policy": 1}
    assert mem5.count_by_container_tag() == {DEFAULT_CONTAINER_TAG: 3}
    assert mem5.count_by_truth_level() == {"measured": 3}
    print("count_by_*   : OK")

    # --------------------------------------------------- 20. from_config / from_pipeline
    from_cfg = RunMemory.from_config(MemoryConfig(), autosave=False)
    assert isinstance(from_cfg, RunMemory)

    class _FakePipeline:
        run_memory = mem5

    from_pipe = RunMemory.from_pipeline(_FakePipeline())
    assert from_pipe is mem5
    print("from_*       : OK")

    # --------------------------------------------------- 21. assert_compatible
    RunMemory.assert_compatible({"schema_version": SCHEMA_VERSION})
    try:
        RunMemory.assert_compatible(
            {"schema_version": SCHEMA_VERSION + 1},
        )
    except RunMemoryParseError:
        print("assert_compat: OK")

    # --------------------------------------------------- 22. Validation
    for bad in (
        lambda: mem.add_run("not-a-mapping"),          # type: ignore[arg-type]
        lambda: mem.add_run({"quality": float("nan")}),
        lambda: mem.add_run({"quality": 0.5}, truth_level="bogus"),
        lambda: mem.add_run({"bad": object()}),
        lambda: mem.get_recent_runs(5.5),              # type: ignore[arg-type]
        lambda: mem.get_recent_runs("5"),              # type: ignore[arg-type]
        lambda: mem.get_performance_trend(""),
        lambda: mem.resize(0),
        lambda: MemoryConfig(trend_stability_band=0.0),
        lambda: MemoryConfig(trend_stability_band=1.0),
        lambda: MemoryConfig(min_runs_for_trend=1),
        lambda: MemoryConfig(min_runs_for_trend=5, min_runs_for_policy=3),
        lambda: MemoryConfig(max_runs=True),
        lambda: MemoryConfig(max_runs=-1),
        lambda: MemoryConfig(max_policies=0),
        lambda: MemoryConfig(metrics_to_track=()),
        lambda: MemoryConfig(metrics_to_track=("quality", "quality")),
        lambda: MemoryConfig(metrics_to_track="quality"),
        lambda: MemoryConfig(higher_is_better={"quality": "yes"}),
        lambda: MemoryConfig(metric_aliases={"quality": "score"}),
        lambda: MemoryConfig(truth_levels=()),
        lambda: MemoryConfig(default_truth_level="bogus"),
        lambda: MemoryConfig(write_retries=-1),
        lambda: MemoryConfig(schema_version=0),
        lambda: MemoryConfig(latency_ring_size=0),
        lambda: MemoryConfig(write_retry_backoff_seconds=0),
        lambda: MemoryConfig(default_importance=1.5),
    ):
        try:
            bad()
        except RunMemoryError as exc:
            print(f"reject       : {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    # --------------------------------------------------- 23. Corruption handling
    corrupt_path = os.path.join(tmpdir, "corrupt.json")
    with open(corrupt_path, "w") as f:
        f.write("{not valid json")
    lenient = RunMemory(memory_file=corrupt_path, strict=False)
    assert len(lenient) == 0
    try:
        RunMemory(memory_file=corrupt_path, strict=True)
    except RunMemoryCorruptionError as exc:
        print("strict read  : OK ->", exc)
    else:
        raise AssertionError("expected RunMemoryCorruptionError")

    # --------------------------------------------------- 24. close(flush=True)
    flusher = RunMemory(memory_file=os.path.join(tmpdir, "flush.json"),
                        autosave=False)
    flusher.add_run({"quality": 0.9})
    flusher.close(flush=True)
    assert os.path.exists(flusher.memory_file)
    print("close flush  : OK")

    print("\nSmoke test passed.")
