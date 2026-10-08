# src/memory/memory_benchmark.py

"""
Memory Benchmark
================

Implements the MVP's "Log a baseline without memory and compare task
quality, latency, tokens, energy, CO2e, and decision consistency."

Enhancements
------------
- ``MemoryBenchmarkConfig`` — frozen, validated, fully serializable:
  seeds, bootstrap samples, metric names, direction map, timeout,
  runner-error policy, verdict policy, truth-level vocabulary, container
  tag expectation, per-task breakdown toggle, latency-ring size.
- ``MetricDirection`` — str-enum (``IMPROVES`` / ``HURTS`` /
  ``INCONCLUSIVE``) with ``coerce`` / ``values``.
- ``MetricStats`` — frozen per-metric statistics with effect size
  (Cohen's d), relative delta, significance, and direction. Deeply
  frozen, hashable, full serialization with wrapped casts.
- ``TaskResult`` — frozen per-task record with baseline/memory metrics,
  extras pulled from the recall bundle and guard verdict. Deeply
  frozen, hashable, wrapped casts.
- ``BenchmarkReport`` — deeply frozen, hashable, fully serializable,
  with ``to_memory_dict()`` and ``to_episode_payload()`` bridges plus an
  ``id`` property. ``schema_version`` is stamped on every payload.
- Working ``strict=False`` path via ``on_runner_error="skip"``.
- Guarded bootstrap CI indices (never inverted).
- Rejects ``bool`` for numeric config; ``NaN``/``inf`` in runner output.
- Frozen ``higher_is_better`` / ``truth_level_weights`` mappings;
  whole config and report are hashable.
- Truth-level awareness: ``truth_levels`` vocabulary,
  ``trusted_truth_levels``, per-task extractor, ``truth_level_mix``
  aggregated in the report.
- Container-tag awareness: ``expected_container_tag`` validated per task;
  ``container_tag_mix`` reported.
- Bundle and verdict extractors: pass
  ``bundle_extractor=`` / ``verdict_extractor=`` to harvest standard
  metrics from ``RecallBundle`` and ``GuardVerdict`` automatically.
- Async sibling (``run_pair_async``), batch helper (``run_pairs``), and
  async batch (``run_pairs_async``).
- Per-runner timeout enforcement via a shared ``ThreadPoolExecutor``
  with executor-shutdown race protection.
- ``from_config()`` / ``from_pipeline()`` constructors.
- ``statistics()`` / ``reset()`` / ``close()`` / context manager.
- Structured error hierarchy:
  ``MemoryBenchmarkError`` → ``MemoryBenchmarkInputError``,
  ``MemoryBenchmarkRunnerError``, ``MemoryBenchmarkConfigError``.
- Verdict policies: ``score``, ``majority``, ``all_significant``,
  ``weighted``.
- ``__version__`` exported via ``__all__``.
- ``__main__`` smoke test covering the strict/skip paths, all extractors,
  truth-level awareness, async, per-task breakdown, and round-trips.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import statistics
import threading
import time
from collections import deque
from collections.abc import Mapping as ABCMapping
from concurrent.futures import (
    CancelledError as _FuturesCancelledError,
    ThreadPoolExecutor,
    TimeoutError as _FuturesTimeoutError,
)
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .bounded_recall import RecallBundle
from .feedback_loop_guard import GuardVerdict

logger = logging.getLogger(__name__)

__version__ = "6.1.0"

#: Version of the benchmark contract itself.
SCHEMA_VERSION: int = 1

#: Default container tag (matches ``SupermemoryConfig.default_container_tag``
#: and ``memory_schemas.DEFAULT_CONTAINER_TAG``).
DEFAULT_CONTAINER_TAG: str = "org:green-agent"


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
_DEFAULT_METRIC_NAMES: Tuple[str, ...] = (
    "quality", "latency_ms", "tokens", "energy_wh", "carbon_gco2e",
    "decision_consistency",
)

# Bug fix #9: immutable default mapping.
_DEFAULT_HIGHER_IS_BETTER: Mapping[str, bool] = MappingProxyType({
    "quality": True,
    "latency_ms": False,
    "tokens": False,
    "energy_wh": False,
    "carbon_gco2e": False,
    "decision_consistency": True,
})

_DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)

_DEFAULT_TRUSTED_TRUTH_LEVELS: Tuple[str, ...] = ("measured",)

_ON_RUNNER_ERROR_POLICIES: Tuple[str, ...] = ("raise", "skip")
_VERDICT_POLICIES: Tuple[str, ...] = (
    "score", "majority", "all_significant", "weighted",
)

_DEFAULT_RUNNER_TIMEOUT_SECONDS: Optional[float] = None
_DEFAULT_DURATION_RING_SIZE: int = 200


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MemoryBenchmarkError(ValueError):
    """Base class for benchmark problems."""


class MemoryBenchmarkInputError(MemoryBenchmarkError):
    """Invalid input to a public API (bad tasks, bad config, etc.)."""


class MemoryBenchmarkRunnerError(MemoryBenchmarkError):
    """A runner returned malformed output, raised, or timed out."""


class MemoryBenchmarkConfigError(MemoryBenchmarkError):
    """Invalid benchmark configuration."""


class MemoryBenchmarkParseError(MemoryBenchmarkError):
    """Failed to parse a config, metric, task or report from dict/JSON."""


# --------------------------------------------------------------------------- #
# Shared validation + freeze helpers — mirror the patched modules
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
    """Parse an ISO 8601 timestamp; tolerate trailing ``Z`` and epochs."""
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


def _normalize_string_tuple(
    value: Any,
    *,
    name: str,
    allow_empty: bool = False,
) -> Tuple[str, ...]:
    if isinstance(value, str) or not isinstance(
        value, (tuple, list, set, frozenset)
    ):
        raise MemoryBenchmarkConfigError(
            f"{name} must be a sequence of strings."
        )
    out: List[str] = []
    seen: set = set()
    for item in value:
        if not isinstance(item, str) or not item:
            raise MemoryBenchmarkConfigError(
                f"{name} entries must be non-empty strings."
            )
        if item in seen:
            raise MemoryBenchmarkConfigError(
                f"{name} contains duplicate {item!r}."
            )
        seen.add(item)
        out.append(item)
    if not out and not allow_empty:
        raise MemoryBenchmarkConfigError(f"{name} must be non-empty.")
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


# Bug fix #3 / #4 / #5: wrapped casts used by every from_dict.
def _coerce_float(name: str, value: Any) -> float:
    """Coerce a value to a finite float; reject ``bool`` / non-numerics."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MemoryBenchmarkParseError(
            f"{name} must be numeric (got {type(value).__name__})."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise MemoryBenchmarkParseError(
            f"{name} must be finite (got {value!r})."
        )
    return fv


def _coerce_int(name: str, value: Any) -> int:
    """Coerce a value to an int; reject ``bool`` / non-integers."""
    if isinstance(value, bool):
        raise MemoryBenchmarkParseError(f"{name} must be an int.")
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        try:
            return int(s)
        except ValueError as exc:
            raise MemoryBenchmarkParseError(
                f"{name} must be an int (got {value!r})."
            ) from exc
    raise MemoryBenchmarkParseError(
        f"{name} must be an int (got {type(value).__name__})."
    )


def _coerce_str(name: str, value: Any) -> str:
    if not isinstance(value, str):
        raise MemoryBenchmarkParseError(f"{name} must be a string.")
    return value


def _coerce_float_map(
    name: str, value: Any,
) -> Mapping[str, float]:
    """Validate a mapping of finite floats and freeze it."""
    if not isinstance(value, ABCMapping):
        raise MemoryBenchmarkParseError(f"{name} must be a Mapping.")
    out: Dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise MemoryBenchmarkParseError(
                f"{name} keys must be non-empty strings."
            )
        out[k] = _coerce_float(f"{name}[{k!r}]", v)
    return MappingProxyType(out)


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class MetricDirection(str, Enum):
    """Direction of a metric delta relative to the baseline."""

    IMPROVES = "improves"
    HURTS = "hurts"
    INCONCLUSIVE = "inconclusive"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(m.value for m in cls)

    @classmethod
    def coerce(cls, value: Any) -> "MetricDirection":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError as exc:
                raise MemoryBenchmarkInputError(
                    f"direction {value!r} is not one of "
                    f"{list(cls.values())}."
                ) from exc
        raise MemoryBenchmarkInputError(
            f"direction must be a str or MetricDirection, got "
            f"{type(value).__name__}."
        )


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryBenchmarkConfig:
    """Tunable parameters for the benchmark harness."""

    schema_version: int = SCHEMA_VERSION
    seed: int = 42
    bootstrap_samples: int = 500
    confidence_level: float = 0.95
    min_task_count: int = 5
    max_task_count: int = 100_000

    metric_names: Tuple[str, ...] = _DEFAULT_METRIC_NAMES
    higher_is_better: Mapping[str, bool] = field(
        default_factory=lambda: dict(_DEFAULT_HIGHER_IS_BETTER),
    )

    # Timeout for a single runner call. ``None`` disables enforcement.
    runner_timeout_seconds: Optional[float] = (
        _DEFAULT_RUNNER_TIMEOUT_SECONDS
    )

    # Size of the latency ring used for percentiles.
    latency_ring_size: int = _DEFAULT_DURATION_RING_SIZE

    # Behavior when a runner raises or returns malformed output.
    on_runner_error: Literal["raise", "skip"] = "raise"

    # How the aggregate verdict is derived from per-metric votes.
    verdict_policy: Literal[
        "score", "majority", "all_significant", "weighted"
    ] = "score"

    # Truth-level vocabulary + trust policy.
    truth_levels: Tuple[str, ...] = _DEFAULT_TRUTH_LEVELS
    trusted_truth_levels: Tuple[str, ...] = _DEFAULT_TRUSTED_TRUTH_LEVELS
    weight_by_truth_level: bool = False
    truth_level_weights: Mapping[str, float] = field(default_factory=dict)

    # Container-tag alignment. ``None`` disables the check.
    expected_container_tag: Optional[str] = None

    # Whether to keep the per-task breakdown in the report.
    include_per_task_breakdown: bool = True

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise MemoryBenchmarkConfigError(
                "schema_version must be a positive int."
            )
        if not _is_real_int(self.seed) or self.seed < 0:
            raise MemoryBenchmarkConfigError(
                "seed must be a non-negative int."
            )
        if not _is_real_int(self.bootstrap_samples) or self.bootstrap_samples <= 0:
            raise MemoryBenchmarkConfigError(
                "bootstrap_samples must be a positive int."
            )
        if not _is_real_int(self.min_task_count) or self.min_task_count < 2:
            raise MemoryBenchmarkConfigError(
                "min_task_count must be an int >= 2."
            )
        if not _is_real_int(self.max_task_count) or self.max_task_count <= self.min_task_count:
            raise MemoryBenchmarkConfigError(
                "max_task_count must be > min_task_count."
            )
        if not _is_real_int(self.latency_ring_size) or self.latency_ring_size <= 0:
            raise MemoryBenchmarkConfigError(
                "latency_ring_size must be a positive int."
            )
        if not (
            isinstance(self.confidence_level, (int, float))
            and not isinstance(self.confidence_level, bool)
            and math.isfinite(self.confidence_level)
        ):
            raise MemoryBenchmarkConfigError(
                "confidence_level must be a finite number."
            )
        if not 0.0 < self.confidence_level < 1.0:
            raise MemoryBenchmarkConfigError(
                "confidence_level must be in (0, 1)."
            )
        if self.runner_timeout_seconds is not None:
            if not _is_positive_finite(self.runner_timeout_seconds):
                raise MemoryBenchmarkConfigError(
                    "runner_timeout_seconds must be None or a finite > 0."
                )
        object.__setattr__(
            self,
            "metric_names",
            _normalize_string_tuple(self.metric_names, name="metric_names"),
        )
        if not isinstance(self.higher_is_better, ABCMapping):
            raise MemoryBenchmarkConfigError(
                "higher_is_better must be a Mapping."
            )
        hib: Dict[str, bool] = {}
        for k, v in self.higher_is_better.items():
            if not isinstance(k, str) or not k:
                raise MemoryBenchmarkConfigError(
                    "higher_is_better keys must be non-empty strings."
                )
            if not isinstance(v, bool):
                raise MemoryBenchmarkConfigError(
                    f"higher_is_better[{k!r}] must be a bool."
                )
            hib[k] = v
        for metric in self.metric_names:
            if metric not in hib:
                raise MemoryBenchmarkConfigError(
                    f"metric {metric!r} missing from higher_is_better."
                )
        object.__setattr__(
            self, "higher_is_better", MappingProxyType(hib),
        )
        if self.on_runner_error not in _ON_RUNNER_ERROR_POLICIES:
            raise MemoryBenchmarkConfigError(
                f"on_runner_error must be one of "
                f"{_ON_RUNNER_ERROR_POLICIES}."
            )
        if self.verdict_policy not in _VERDICT_POLICIES:
            raise MemoryBenchmarkConfigError(
                f"verdict_policy must be one of {_VERDICT_POLICIES}."
            )
        object.__setattr__(
            self,
            "truth_levels",
            _normalize_string_tuple(self.truth_levels, name="truth_levels"),
        )
        object.__setattr__(
            self,
            "trusted_truth_levels",
            _normalize_string_tuple(
                self.trusted_truth_levels, name="trusted_truth_levels",
            ),
        )
        for level in self.trusted_truth_levels:
            if level not in self.truth_levels:
                raise MemoryBenchmarkConfigError(
                    f"trusted_truth_levels entry {level!r} not in "
                    f"truth_levels."
                )
        if not isinstance(self.truth_level_weights, ABCMapping):
            raise MemoryBenchmarkConfigError(
                "truth_level_weights must be a Mapping."
            )
        weights: Dict[str, float] = {}
        for k, v in self.truth_level_weights.items():
            if not isinstance(k, str) or not k:
                raise MemoryBenchmarkConfigError(
                    "truth_level_weights keys must be non-empty strings."
                )
            if k not in self.truth_levels:
                raise MemoryBenchmarkConfigError(
                    f"truth_level_weights key {k!r} not in truth_levels."
                )
            if not _is_finite_nonneg(v):
                raise MemoryBenchmarkConfigError(
                    f"truth_level_weights[{k!r}] must be a finite >= 0 "
                    f"number."
                )
            weights[k] = float(v)
        object.__setattr__(
            self, "truth_level_weights", MappingProxyType(weights),
        )
        if not isinstance(self.weight_by_truth_level, bool):
            raise MemoryBenchmarkConfigError(
                "weight_by_truth_level must be a bool."
            )
        if self.weight_by_truth_level and not self.truth_level_weights:
            defaults = {
                level: (
                    1.0 if level in self.trusted_truth_levels else 0.5
                )
                for level in self.truth_levels
            }
            object.__setattr__(
                self,
                "truth_level_weights",
                MappingProxyType(defaults),
            )
        if self.expected_container_tag is not None:
            if (
                not isinstance(self.expected_container_tag, str)
                or not self.expected_container_tag
            ):
                raise MemoryBenchmarkConfigError(
                    "expected_container_tag must be None or a non-empty "
                    "string."
                )
        if not isinstance(self.include_per_task_breakdown, bool):
            raise MemoryBenchmarkConfigError(
                "include_per_task_breakdown must be a bool."
            )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "seed": self.seed,
            "bootstrap_samples": self.bootstrap_samples,
            "confidence_level": self.confidence_level,
            "min_task_count": self.min_task_count,
            "max_task_count": self.max_task_count,
            "metric_names": list(self.metric_names),
            "higher_is_better": dict(self.higher_is_better),
            "runner_timeout_seconds": self.runner_timeout_seconds,
            "latency_ring_size": self.latency_ring_size,
            "on_runner_error": self.on_runner_error,
            "verdict_policy": self.verdict_policy,
            "truth_levels": list(self.truth_levels),
            "trusted_truth_levels": list(self.trusted_truth_levels),
            "weight_by_truth_level": self.weight_by_truth_level,
            "truth_level_weights": dict(self.truth_level_weights),
            "expected_container_tag": self.expected_container_tag,
            "include_per_task_breakdown": self.include_per_task_breakdown,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "MemoryBenchmarkConfig":
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkConfigError(
                "MemoryBenchmarkConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise MemoryBenchmarkConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k in (
                "metric_names", "truth_levels", "trusted_truth_levels",
            ):
                if isinstance(v, str) or not isinstance(
                    v, (list, tuple, set, frozenset)
                ):
                    raise MemoryBenchmarkConfigError(
                        f"{k} must be a sequence of strings."
                    )
                kwargs[k] = tuple(v)
            elif k in ("higher_is_better", "truth_level_weights"):
                if not isinstance(v, ABCMapping):
                    raise MemoryBenchmarkConfigError(
                        f"{k} must be a Mapping."
                    )
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except MemoryBenchmarkError:
            raise
        except (TypeError, ValueError) as exc:
            raise MemoryBenchmarkConfigError(
                f"failed to build MemoryBenchmarkConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "MemoryBenchmarkConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryBenchmarkConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> "MemoryBenchmarkConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise MemoryBenchmarkConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(
        self, other: "MemoryBenchmarkConfig"
    ) -> "MemoryBenchmarkConfig":
        defaults = MemoryBenchmarkConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.schema_version,
            self.seed,
            self.bootstrap_samples,
            self.confidence_level,
            self.min_task_count,
            self.max_task_count,
            self.metric_names,
            tuple(sorted(self.higher_is_better.items())),
            self.runner_timeout_seconds,
            self.latency_ring_size,
            self.on_runner_error,
            self.verdict_policy,
            self.truth_levels,
            self.trusted_truth_levels,
            self.weight_by_truth_level,
            tuple(sorted(self.truth_level_weights.items())),
            self.expected_container_tag,
            self.include_per_task_breakdown,
        ))

    def __repr__(self) -> str:
        return (
            "MemoryBenchmarkConfig("
            f"seed={self.seed}, "
            f"samples={self.bootstrap_samples}, "
            f"metrics={len(self.metric_names)}, "
            f"policy={self.on_runner_error})"
        )


# --------------------------------------------------------------------------- #
# Per-metric stats
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MetricStats:
    """Frozen statistics for a single metric."""

    baseline: float
    memory: float
    delta: float
    relative_delta: float
    ci_low: float
    ci_high: float
    cohen_d: float
    significant: bool
    direction: str = MetricDirection.INCONCLUSIVE.value
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in (
            "baseline", "memory", "delta", "relative_delta",
            "ci_low", "ci_high", "cohen_d",
        ):
            v = getattr(self, name)
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                raise MemoryBenchmarkInputError(
                    f"{name} must be numeric (got {v!r})."
                )
            if not math.isfinite(float(v)):
                raise MemoryBenchmarkInputError(
                    f"{name} must be finite (got {v!r})."
                )
        if not isinstance(self.significant, bool):
            raise MemoryBenchmarkInputError("significant must be a bool.")
        object.__setattr__(
            self, "direction",
            MetricDirection.coerce(self.direction).value,
        )
        if self.ci_low > self.ci_high:
            raise MemoryBenchmarkInputError(
                "ci_low must be <= ci_high."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise MemoryBenchmarkInputError(
                "schema_version must be a positive int."
            )

    @property
    def direction_enum(self) -> MetricDirection:
        return MetricDirection.coerce(self.direction)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "baseline": self.baseline,
            "memory": self.memory,
            "delta": self.delta,
            "relative_delta": self.relative_delta,
            "ci_low": self.ci_low,
            "ci_high": self.ci_high,
            "cohen_d": self.cohen_d,
            "significant": self.significant,
            "direction": self.direction,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MetricStats":
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkParseError(
                "MetricStats.from_dict expects a Mapping."
            )
        return cls(
            baseline=_coerce_float("baseline", data.get("baseline", 0.0)),
            memory=_coerce_float("memory", data.get("memory", 0.0)),
            delta=_coerce_float("delta", data.get("delta", 0.0)),
            relative_delta=_coerce_float(
                "relative_delta", data.get("relative_delta", 0.0),
            ),
            ci_low=_coerce_float("ci_low", data.get("ci_low", 0.0)),
            ci_high=_coerce_float("ci_high", data.get("ci_high", 0.0)),
            cohen_d=_coerce_float("cohen_d", data.get("cohen_d", 0.0)),
            significant=bool(data.get("significant", False)),
            direction=str(
                data.get("direction", MetricDirection.INCONCLUSIVE.value)
            ),
            schema_version=_coerce_int(
                "schema_version", data.get("schema_version", SCHEMA_VERSION),
            ),
        )

    @classmethod
    def from_json(cls, payload: str) -> "MetricStats":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryBenchmarkParseError(
                f"MetricStats.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    def __hash__(self) -> int:
        return hash((
            self.baseline, self.memory, self.delta, self.relative_delta,
            self.ci_low, self.ci_high, self.cohen_d,
            self.significant, self.direction, self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"MetricStats(delta={self.delta:+.4g}, "
            f"ci=[{self.ci_low:+.4g}, {self.ci_high:+.4g}], "
            f"direction={self.direction!r})"
        )


# --------------------------------------------------------------------------- #
# Per-task result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TaskResult:
    """Frozen per-task record of baseline vs. memory metrics."""

    index: int
    baseline: Mapping[str, float]
    memory: Mapping[str, float]
    delta: Mapping[str, float]
    truth_level: Optional[str] = None
    container_tag: Optional[str] = None
    bundle_extras: Mapping[str, float] = field(default_factory=dict)
    verdict_extras: Mapping[str, float] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not _is_real_int(self.index) or self.index < 0:
            raise MemoryBenchmarkInputError(
                "index must be a non-negative int."
            )
        for name in ("baseline", "memory", "delta",
                     "bundle_extras", "verdict_extras"):
            v = getattr(self, name)
            if not isinstance(v, ABCMapping):
                raise MemoryBenchmarkInputError(f"{name} must be a Mapping.")
            object.__setattr__(
                self, name,
                MappingProxyType({str(k): float(val) for k, val in v.items()}),
            )
        if self.truth_level is not None and not isinstance(
            self.truth_level, str
        ):
            raise MemoryBenchmarkInputError(
                "truth_level must be None or a string."
            )
        if self.container_tag is not None and not isinstance(
            self.container_tag, str
        ):
            raise MemoryBenchmarkInputError(
                "container_tag must be None or a string."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise MemoryBenchmarkInputError(
                "schema_version must be a positive int."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "index": self.index,
            "baseline": dict(self.baseline),
            "memory": dict(self.memory),
            "delta": dict(self.delta),
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "bundle_extras": dict(self.bundle_extras),
            "verdict_extras": dict(self.verdict_extras),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TaskResult":
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkParseError(
                "TaskResult.from_dict expects a Mapping."
            )
        return cls(
            index=_coerce_int("index", data.get("index", 0)),
            baseline=_coerce_float_map(
                "baseline", data.get("baseline", {}),
            ),
            memory=_coerce_float_map("memory", data.get("memory", {})),
            delta=_coerce_float_map("delta", data.get("delta", {})),
            truth_level=(
                _coerce_str("truth_level", data["truth_level"])
                if isinstance(data.get("truth_level"), str) else None
            ),
            container_tag=(
                _coerce_str("container_tag", data["container_tag"])
                if isinstance(data.get("container_tag"), str) else None
            ),
            bundle_extras=_coerce_float_map(
                "bundle_extras", data.get("bundle_extras", {}),
            ),
            verdict_extras=_coerce_float_map(
                "verdict_extras", data.get("verdict_extras", {}),
            ),
            schema_version=_coerce_int(
                "schema_version", data.get("schema_version", SCHEMA_VERSION),
            ),
        )

    @classmethod
    def from_json(cls, payload: str) -> "TaskResult":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryBenchmarkParseError(
                f"TaskResult.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``."""
        lines = [
            f"Benchmark task {self.index}",
            f"  truth_level={self.truth_level!r}",
            f"  container_tag={self.container_tag!r}",
            "  baseline: "
            + json.dumps(dict(self.baseline), default=str, sort_keys=True),
            "  memory: "
            + json.dumps(dict(self.memory), default=str, sort_keys=True),
            "  delta: "
            + json.dumps(dict(self.delta), default=str, sort_keys=True),
        ]
        return {
            "id": f"task:{self.index}",
            "content": "\n".join(lines),
            "metadata": {
                "kind": "benchmark_task",
                "record_id": f"task:{self.index}",
                "truth_level": self.truth_level or "estimated",
                "container_tag": self.container_tag or DEFAULT_CONTAINER_TAG,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "schema_version": self.schema_version,
                "index": self.index,
            },
        }

    def __hash__(self) -> int:
        return hash((
            self.index,
            tuple(sorted(self.baseline.items())),
            tuple(sorted(self.memory.items())),
            tuple(sorted(self.delta.items())),
            self.truth_level,
            self.container_tag,
            tuple(sorted(self.bundle_extras.items())),
            tuple(sorted(self.verdict_extras.items())),
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"TaskResult(index={self.index}, "
            f"truth_level={self.truth_level!r}, "
            f"container_tag={self.container_tag!r})"
        )


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BenchmarkReport:
    """Frozen benchmark report."""

    tasks: int
    tasks_succeeded: int
    tasks_failed: int
    metrics: Mapping[str, MetricStats]
    extras: Mapping[str, float] = field(default_factory=dict)
    per_task: Tuple[TaskResult, ...] = ()
    verdict: str = "memory_neutral"
    score: int = 0
    seed: int = 0
    truth_level_mix: Mapping[str, int] = field(default_factory=dict)
    container_tag_mix: Mapping[str, int] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("tasks", "tasks_succeeded", "tasks_failed", "seed"):
            v = getattr(self, name)
            if not _is_real_int(v) or v < 0:
                raise MemoryBenchmarkInputError(
                    f"{name} must be a non-negative int."
                )
        if self.tasks_succeeded + self.tasks_failed > self.tasks:
            raise MemoryBenchmarkInputError(
                "tasks_succeeded + tasks_failed must be <= tasks."
            )
        if not _is_real_int(self.score):
            raise MemoryBenchmarkInputError("score must be an int.")
        if self.verdict not in (
            "memory_helps", "memory_neutral", "memory_hurts",
        ):
            raise MemoryBenchmarkInputError(
                f"verdict must be one of "
                f"('memory_helps', 'memory_neutral', 'memory_hurts'), "
                f"got {self.verdict!r}."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise MemoryBenchmarkInputError(
                "schema_version must be a positive int."
            )
        if not isinstance(self.metrics, ABCMapping):
            raise MemoryBenchmarkInputError("metrics must be a Mapping.")
        frozen_metrics: Dict[str, MetricStats] = {}
        for k, v in self.metrics.items():
            if not isinstance(k, str) or not k:
                raise MemoryBenchmarkInputError(
                    "metrics keys must be non-empty strings."
                )
            if not isinstance(v, MetricStats):
                raise MemoryBenchmarkInputError(
                    f"metrics[{k!r}] must be a MetricStats."
                )
            frozen_metrics[k] = v
        object.__setattr__(
            self, "metrics", MappingProxyType(frozen_metrics),
        )
        if not isinstance(self.extras, ABCMapping):
            raise MemoryBenchmarkInputError("extras must be a Mapping.")
        object.__setattr__(
            self, "extras",
            MappingProxyType(
                {str(k): float(v) for k, v in self.extras.items()}
            ),
        )
        if not isinstance(self.per_task, tuple):
            object.__setattr__(self, "per_task", tuple(self.per_task))
        for tr in self.per_task:
            if not isinstance(tr, TaskResult):
                raise MemoryBenchmarkInputError(
                    "per_task entries must be TaskResult instances."
                )
        for name in ("truth_level_mix", "container_tag_mix"):
            v = getattr(self, name)
            if not isinstance(v, ABCMapping):
                raise MemoryBenchmarkInputError(f"{name} must be a Mapping.")
            normalized: Dict[str, int] = {}
            for k, val in v.items():
                if not isinstance(k, str) or not k:
                    raise MemoryBenchmarkInputError(
                        f"{name} keys must be non-empty strings."
                    )
                if not _is_real_int(val) or val < 0:
                    raise MemoryBenchmarkInputError(
                        f"{name}[{k!r}] must be a non-negative int."
                    )
                normalized[k] = int(val)
            object.__setattr__(
                self, name, MappingProxyType(normalized),
            )
        if not isinstance(self.timestamp, datetime):
            raise MemoryBenchmarkInputError(
                "timestamp must be a datetime."
            )
        if self.timestamp.tzinfo is None:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return f"report:{self.timestamp.isoformat()}"

    def _summarize(self) -> str:
        improved = sum(
            1 for v in self.metrics.values()
            if v.direction == MetricDirection.IMPROVES.value
        )
        hurt = sum(
            1 for v in self.metrics.values()
            if v.direction == MetricDirection.HURTS.value
        )
        return (
            f"Memory benchmark: {self.verdict} "
            f"(score={self.score:+d}, "
            f"{self.tasks_succeeded}/{self.tasks} tasks, "
            f"improved={improved}, hurt={hurt})"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "tasks": self.tasks,
            "tasks_succeeded": self.tasks_succeeded,
            "tasks_failed": self.tasks_failed,
            "metrics": {k: v.to_dict() for k, v in self.metrics.items()},
            "extras": dict(self.extras),
            "per_task": [t.to_dict() for t in self.per_task],
            "verdict": self.verdict,
            "score": self.score,
            "seed": self.seed,
            "truth_level_mix": dict(self.truth_level_mix),
            "container_tag_mix": dict(self.container_tag_mix),
            "timestamp": self.timestamp.isoformat(),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.to_dict(), default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkReport":
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkParseError(
                "BenchmarkReport.from_dict expects a Mapping."
            )
        ts_raw = data.get("timestamp")
        ts = _parse_iso_datetime(ts_raw) if ts_raw else None
        if ts is None:
            ts = datetime.now(timezone.utc)
        raw_metrics = data.get("metrics", {})
        if not isinstance(raw_metrics, ABCMapping):
            raise MemoryBenchmarkParseError(
                "'metrics' must be a Mapping."
            )
        metrics = {
            k: MetricStats.from_dict(v) for k, v in raw_metrics.items()
        }
        raw_per_task = data.get("per_task", [])
        if not isinstance(raw_per_task, (list, tuple)):
            raise MemoryBenchmarkParseError("'per_task' must be a sequence.")
        per_task = tuple(TaskResult.from_dict(t) for t in raw_per_task)
        raw_tlm = data.get("truth_level_mix", {}) or {}
        raw_ctm = data.get("container_tag_mix", {}) or {}
        if not isinstance(raw_tlm, ABCMapping):
            raise MemoryBenchmarkParseError(
                "truth_level_mix must be a Mapping."
            )
        if not isinstance(raw_ctm, ABCMapping):
            raise MemoryBenchmarkParseError(
                "container_tag_mix must be a Mapping."
            )
        return cls(
            tasks=_coerce_int("tasks", data.get("tasks", 0)),
            tasks_succeeded=_coerce_int(
                "tasks_succeeded", data.get("tasks_succeeded", 0),
            ),
            tasks_failed=_coerce_int(
                "tasks_failed", data.get("tasks_failed", 0),
            ),
            metrics=metrics,
            extras=dict(data.get("extras", {})),
            per_task=per_task,
            verdict=str(data.get("verdict", "memory_neutral")),
            score=_coerce_int("score", data.get("score", 0)),
            seed=_coerce_int("seed", data.get("seed", 0)),
            truth_level_mix={str(k): int(v) for k, v in raw_tlm.items()},
            container_tag_mix={str(k): int(v) for k, v in raw_ctm.items()},
            timestamp=ts,
            schema_version=_coerce_int(
                "schema_version", data.get("schema_version", SCHEMA_VERSION),
            ),
        )

    @classmethod
    def from_json(cls, payload: str) -> "BenchmarkReport":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryBenchmarkParseError(
                f"BenchmarkReport.from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkParseError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Persistence bridges
    # ------------------------------------------------------------------ #
    def _render_content(self) -> str:
        lines = [self._summarize()]
        if self.metrics:
            lines.append("Metrics:")
            for name, stats in sorted(self.metrics.items()):
                lines.append(
                    f"  {name}: delta={stats.delta:+.4g} "
                    f"ci=[{stats.ci_low:+.4g}, {stats.ci_high:+.4g}] "
                    f"d={stats.cohen_d:+.2f} {stats.direction}"
                )
        if self.truth_level_mix:
            mix = ", ".join(
                f"{k}={v}" for k, v in sorted(self.truth_level_mix.items())
            )
            lines.append(f"truth_level_mix: {mix}")
        if self.container_tag_mix:
            mix = ", ".join(
                f"{k}={v}" for k, v in sorted(self.container_tag_mix.items())
            )
            lines.append(f"container_tag_mix: {mix}")
        return "\n".join(lines)

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        return {
            "kind": "benchmark_report",
            "type": "benchmark_report",
            "record_id": self.id,
            "truth_level": "estimated",
            "container_tag": container_tag,
            "observed_at": self.timestamp.isoformat(),
            "schema_version": self.schema_version,
            "tasks": self.tasks,
            "tasks_succeeded": self.tasks_succeeded,
            "tasks_failed": self.tasks_failed,
            "verdict": self.verdict,
            "score": self.score,
            "seed": self.seed,
            "truth_level_mix": dict(self.truth_level_mix),
            "container_tag_mix": dict(self.container_tag_mix),
            "metrics_summary": {
                k: {
                    "delta": v.delta,
                    "relative_delta": v.relative_delta,
                    "cohen_d": v.cohen_d,
                    "direction": v.direction,
                    "significant": v.significant,
                }
                for k, v in self.metrics.items()
            },
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a payload shaped for ``EpisodicMemory.store`` /
        ``SupermemoryAdapter.remember``.

        Bug fix #1: resolves ``container_tag`` to ``DEFAULT_CONTAINER_TAG``
        when not provided (matching every other record class in the module).
        """
        resolved_tag = container_tag or DEFAULT_CONTAINER_TAG
        if not isinstance(resolved_tag, str) or not resolved_tag:
            raise MemoryBenchmarkInputError(
                "container_tag must be a non-empty string."
            )
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": resolved_tag,
            "metadata": self._metadata(container_tag=resolved_tag),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``."""
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=DEFAULT_CONTAINER_TAG),
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.schema_version,
            self.tasks,
            self.tasks_succeeded,
            self.tasks_failed,
            tuple(sorted((k, hash(v)) for k, v in self.metrics.items())),
            tuple(sorted(self.extras.items())),
            self.per_task,
            self.verdict,
            self.score,
            self.seed,
            tuple(sorted(self.truth_level_mix.items())),
            tuple(sorted(self.container_tag_mix.items())),
            self.timestamp,
        ))

    def __repr__(self) -> str:
        return (
            f"BenchmarkReport(tasks={self.tasks_succeeded}/{self.tasks}, "
            f"verdict={self.verdict!r}, score={self.score:+d})"
        )


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #
class MemoryBenchmark:
    """Paired benchmark: without-memory vs. with-memory."""

    def __init__(
        self,
        *,
        config: Optional[MemoryBenchmarkConfig] = None,
        strict: Optional[bool] = None,
    ) -> None:
        if config is None:
            config = MemoryBenchmarkConfig()
        elif not isinstance(config, MemoryBenchmarkConfig):
            raise MemoryBenchmarkInputError(
                "config must be a MemoryBenchmarkConfig or None."
            )
        if strict is not None:
            config = config.with_overrides(
                on_runner_error="raise" if strict else "skip",
            )
        self._config = config

        self._lock = threading.RLock()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._owns_executor: bool = False
        self._executor_guard = threading.Lock()

        self._total_runs = 0
        self._total_task_pairs = 0
        self._total_task_failures = 0
        self._runner_errors = 0
        self._duration_ring: Deque[float] = deque(
            maxlen=self._config.latency_ring_size
        )
        self._last_report: Optional[BenchmarkReport] = None
        self._last_error: Optional[str] = None
        self._started_at = time.monotonic()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MemoryBenchmarkConfig:
        return self._config

    @property
    def last_report(self) -> Optional[BenchmarkReport]:
        with self._lock:
            return self._last_report

    # ---------------------------------------------------------- constructors
    @classmethod
    def from_config(
        cls,
        config: MemoryBenchmarkConfig,
        *,
        strict: Optional[bool] = None,
    ) -> "MemoryBenchmark":
        return cls(config=config, strict=strict)

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Any,
        *,
        config: Optional[MemoryBenchmarkConfig] = None,
        strict: Optional[bool] = None,
    ) -> "MemoryBenchmark":
        """Return ``pipeline.benchmark`` if present, else build a fresh one."""
        bench = getattr(pipeline, "benchmark", None)
        if isinstance(bench, cls):
            return bench
        return cls(config=config, strict=strict)

    # -------------------------------------------------------------- executor
    def _acquire_executor(self) -> ThreadPoolExecutor:
        """Return the shared executor, creating it if necessary.

        Caller must hold ``self._executor_guard``.
        """
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=2,
                thread_name_prefix="memory-benchmark",
            )
            self._owns_executor = True
        return self._executor

    def close(self) -> None:
        """Shut down the owned executor. Safe to call multiple times."""
        with self._executor_guard:
            if self._executor is not None and self._owns_executor:
                self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

    def __enter__(self) -> "MemoryBenchmark":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------- public API
    def run_pair(
        self,
        *,
        tasks: Sequence[Any],
        without_memory: Callable[[Any], Mapping[str, float]],
        with_memory: Callable[[Any], Mapping[str, float]],
        bundle_extractor: Optional[
            Callable[[Any], Optional[RecallBundle]]
        ] = None,
        verdict_extractor: Optional[
            Callable[[Any], Optional[GuardVerdict]]
        ] = None,
        task_truth_level: Optional[
            Callable[[Any], Optional[str]]
        ] = None,
        task_container_tag: Optional[
            Callable[[Any], Optional[str]]
        ] = None,
        now: Optional[float] = None,
    ) -> BenchmarkReport:
        """Run the paired comparison on ``tasks``.

        Each runner returns a mapping of the configured metric names to
        finite floats. Optional extractors harvest additional memory-side
        signals from ``RecallBundle`` and ``GuardVerdict``.
        """
        if not isinstance(tasks, (list, tuple)):
            raise MemoryBenchmarkInputError(
                "tasks must be a sequence."
            )
        if not tasks:
            raise MemoryBenchmarkInputError(
                "tasks must be a non-empty sequence."
            )
        if not callable(without_memory):
            raise MemoryBenchmarkInputError(
                "without_memory must be callable."
            )
        if not callable(with_memory):
            raise MemoryBenchmarkInputError(
                "with_memory must be callable."
            )
        for name, fn in (
            ("bundle_extractor", bundle_extractor),
            ("verdict_extractor", verdict_extractor),
            ("task_truth_level", task_truth_level),
            ("task_container_tag", task_container_tag),
        ):
            if fn is not None and not callable(fn):
                raise MemoryBenchmarkInputError(f"{name} must be callable.")
        if len(tasks) < self._config.min_task_count:
            raise MemoryBenchmarkInputError(
                f"at least {self._config.min_task_count} tasks required."
            )
        if len(tasks) > self._config.max_task_count:
            raise MemoryBenchmarkInputError(
                f"tasks exceeds max_task_count="
                f"{self._config.max_task_count}."
            )
        if now is None:
            now = time.time()
        elif not _is_finite_nonneg(now):
            raise MemoryBenchmarkInputError(
                "now must be a finite non-negative number."
            )

        start = time.perf_counter()
        rng = random.Random(self._config.seed)

        metric_names = self._config.metric_names
        baseline_runs: Dict[str, List[float]] = {m: [] for m in metric_names}
        memory_runs: Dict[str, List[float]] = {m: [] for m in metric_names}
        per_task: List[TaskResult] = []
        bundle_extras_all: Dict[str, List[float]] = {}
        verdict_extras_all: Dict[str, List[float]] = {}
        truth_level_mix: Dict[str, int] = {}
        container_tag_mix: Dict[str, int] = {}

        tasks_succeeded = 0
        tasks_failed = 0

        for i, task in enumerate(tasks):
            b = self._run_runner(
                without_memory, task, name=f"baseline[{i}]",
            )
            m = self._run_runner(
                with_memory, task, name=f"memory[{i}]",
            )
            if b is None or m is None:
                tasks_failed += 1
                if self._config.on_runner_error == "skip":
                    continue
                raise MemoryBenchmarkRunnerError(  # pragma: no cover
                    f"runner failed at task {i}"
                )

            missing = [
                metric for metric in metric_names
                if metric not in b or metric not in m
            ]
            if missing:
                msg = (
                    f"runner did not return metric(s) "
                    f"{missing!r} at task {i}"
                )
                if self._config.on_runner_error == "raise":
                    raise MemoryBenchmarkRunnerError(msg)
                logger.warning("%s; skipping task.", msg)
                tasks_failed += 1
                continue

            truth_level: Optional[str] = None
            if task_truth_level is not None:
                truth_level = task_truth_level(task)
                if truth_level is not None:
                    if not isinstance(truth_level, str) or not truth_level:
                        raise MemoryBenchmarkInputError(
                            f"task_truth_level(task) must return a "
                            f"non-empty string or None (task {i})."
                        )
                    if truth_level not in self._config.truth_levels:
                        raise MemoryBenchmarkInputError(
                            f"task truth_level {truth_level!r} not in "
                            f"configured truth_levels (task {i})."
                        )
                    truth_level_mix[truth_level] = (
                        truth_level_mix.get(truth_level, 0) + 1
                    )

            container_tag: Optional[str] = None
            if task_container_tag is not None:
                container_tag = task_container_tag(task)
                if container_tag is not None:
                    if (
                        not isinstance(container_tag, str)
                        or not container_tag
                    ):
                        raise MemoryBenchmarkInputError(
                            f"task_container_tag(task) must return a "
                            f"non-empty string or None (task {i})."
                        )
                    expected = self._config.expected_container_tag
                    if expected is not None and container_tag != expected:
                        raise MemoryBenchmarkInputError(
                            f"task container_tag {container_tag!r} "
                            f"!= expected {expected!r} (task {i})."
                        )
                    container_tag_mix[container_tag] = (
                        container_tag_mix.get(container_tag, 0) + 1
                    )

            bundle_extras: Dict[str, float] = {}
            if bundle_extractor is not None:
                bundle = bundle_extractor(task)
                if bundle is not None and isinstance(bundle, RecallBundle):
                    bundle_extras = self._bundle_extras(bundle)
                for k, v in bundle_extras.items():
                    bundle_extras_all.setdefault(k, []).append(v)

            verdict_extras: Dict[str, float] = {}
            if verdict_extractor is not None:
                verdict = verdict_extractor(task)
                if verdict is not None and isinstance(verdict, GuardVerdict):
                    verdict_extras = self._verdict_extras(verdict)
                for k, v in verdict_extras.items():
                    verdict_extras_all.setdefault(k, []).append(v)

            delta: Dict[str, float] = {}
            for metric in metric_names:
                bv = b[metric]
                mv = m[metric]
                baseline_runs[metric].append(bv)
                memory_runs[metric].append(mv)
                delta[metric] = mv - bv

            per_task.append(TaskResult(
                index=i,
                baseline=b,
                memory=m,
                delta=delta,
                truth_level=truth_level,
                container_tag=container_tag,
                bundle_extras=bundle_extras,
                verdict_extras=verdict_extras,
            ))
            tasks_succeeded += 1

        if tasks_succeeded < self._config.min_task_count:
            raise MemoryBenchmarkRunnerError(
                f"only {tasks_succeeded} task(s) succeeded; "
                f"at least {self._config.min_task_count} required."
            )

        # ------------------------------------------------------- aggregate
        metrics: Dict[str, MetricStats] = {}
        votes: List[int] = []
        weights: List[float] = []
        for metric in metric_names:
            b_vals = baseline_runs[metric]
            m_vals = memory_runs[metric]
            if not b_vals or not m_vals:
                continue
            b_mean = statistics.fmean(b_vals)
            m_mean = statistics.fmean(m_vals)
            delta = m_mean - b_mean
            relative = delta / abs(b_mean) if abs(b_mean) > 1e-12 else 0.0
            ci_low, ci_high = self._bootstrap_ci(
                m_vals, b_vals, rng=rng,
            )
            diffs = [mv - bv for mv, bv in zip(m_vals, b_vals)]
            cohen_d = self._cohen_d(diffs)

            higher = self._config.higher_is_better[metric]
            if ci_low > 0.0:
                sig_dir = (
                    MetricDirection.IMPROVES.value if higher
                    else MetricDirection.HURTS.value
                )
                significant = True
            elif ci_high < 0.0:
                sig_dir = (
                    MetricDirection.HURTS.value if higher
                    else MetricDirection.IMPROVES.value
                )
                significant = True
            else:
                sig_dir = MetricDirection.INCONCLUSIVE.value
                significant = False

            metrics[metric] = MetricStats(
                baseline=b_mean,
                memory=m_mean,
                delta=delta,
                relative_delta=relative,
                ci_low=ci_low,
                ci_high=ci_high,
                cohen_d=cohen_d,
                significant=significant,
                direction=sig_dir,
                schema_version=SCHEMA_VERSION,
            )

            if sig_dir == MetricDirection.IMPROVES.value:
                votes.append(+1)
                weights.append(abs(relative))
            elif sig_dir == MetricDirection.HURTS.value:
                votes.append(-1)
                weights.append(abs(relative))
            else:
                votes.append(0)
                weights.append(0.0)

        score = sum(votes)
        verdict = self._decide_verdict(votes, weights, metrics)

        extras: Dict[str, float] = {}
        for k, vs in bundle_extras_all.items():
            extras[k] = statistics.fmean(vs) if vs else 0.0
        for k, vs in verdict_extras_all.items():
            extras[k] = statistics.fmean(vs) if vs else 0.0

        duration_ms = (time.perf_counter() - start) * 1000.0

        report = BenchmarkReport(
            tasks=len(tasks),
            tasks_succeeded=tasks_succeeded,
            tasks_failed=tasks_failed,
            metrics=metrics,
            extras=extras,
            per_task=(
                tuple(per_task)
                if self._config.include_per_task_breakdown else ()
            ),
            verdict=verdict,
            score=score,
            seed=self._config.seed,
            truth_level_mix=truth_level_mix,
            container_tag_mix=container_tag_mix,
            schema_version=self._config.schema_version,
        )

        with self._lock:
            self._total_runs += 1
            self._total_task_pairs += tasks_succeeded
            self._total_task_failures += tasks_failed
            self._duration_ring.append(duration_ms)
            self._last_report = report
            self._last_error = None

        logger.debug(
            "Benchmark verdict=%s score=%+d succeeded=%d failed=%d",
            verdict, score, tasks_succeeded, tasks_failed,
        )
        return report

    async def run_pair_async(
        self,
        *,
        tasks: Sequence[Any],
        without_memory: Callable[[Any], Mapping[str, float]],
        with_memory: Callable[[Any], Mapping[str, float]],
        bundle_extractor: Optional[
            Callable[[Any], Optional[RecallBundle]]
        ] = None,
        verdict_extractor: Optional[
            Callable[[Any], Optional[GuardVerdict]]
        ] = None,
        task_truth_level: Optional[
            Callable[[Any], Optional[str]]
        ] = None,
        task_container_tag: Optional[
            Callable[[Any], Optional[str]]
        ] = None,
        now: Optional[float] = None,
    ) -> BenchmarkReport:
        return await asyncio.to_thread(
            self.run_pair,
            tasks=tasks,
            without_memory=without_memory,
            with_memory=with_memory,
            bundle_extractor=bundle_extractor,
            verdict_extractor=verdict_extractor,
            task_truth_level=task_truth_level,
            task_container_tag=task_container_tag,
            now=now,
        )

    def run_pairs(
        self,
        pairs: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[BenchmarkReport]:
        """Run ``run_pair`` for each item."""
        reports: List[BenchmarkReport] = []
        for idx, item in enumerate(pairs):
            if not isinstance(item, ABCMapping):
                if stop_on_error:
                    raise MemoryBenchmarkInputError(
                        f"run_pairs[{idx}] must be a Mapping."
                    )
                logger.warning(
                    "run_pairs[%d] is not a Mapping; skipping.", idx,
                )
                continue
            try:
                kwargs = {
                    "tasks": item["tasks"],
                    "without_memory": item["without_memory"],
                    "with_memory": item["with_memory"],
                    "bundle_extractor": item.get("bundle_extractor"),
                    "verdict_extractor": item.get("verdict_extractor"),
                    "task_truth_level": item.get("task_truth_level"),
                    "task_container_tag": item.get("task_container_tag"),
                    "now": item.get("now"),
                }
            except KeyError as exc:
                if stop_on_error:
                    raise MemoryBenchmarkInputError(
                        f"run_pairs[{idx}] missing key {exc.args[0]!r}."
                    ) from exc
                logger.warning(
                    "run_pairs[%d] missing key %r; skipping.",
                    idx, exc.args[0],
                )
                continue
            try:
                reports.append(self.run_pair(**kwargs))
            except MemoryBenchmarkError as exc:
                if stop_on_error:
                    raise
                logger.warning("run_pairs[%d] failed: %s", idx, exc)
                with self._lock:
                    self._last_error = f"run_pairs[{idx}]: {exc}"
        return reports

    async def run_pairs_async(
        self,
        pairs: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[BenchmarkReport]:
        """Run ``run_pairs`` in a worker thread."""
        return await asyncio.to_thread(
            self.run_pairs, pairs, stop_on_error=stop_on_error,
        )

    # ---------------------------------------------------------- internals
    def _run_runner(
        self,
        runner: Callable[[Any], Mapping[str, float]],
        task: Any,
        *,
        name: str,
    ) -> Optional[Mapping[str, float]]:
        """Call ``runner`` with optional timeout. Returns None on skip."""
        try:
            result = self._call_with_timeout(runner, task, name=name)
        except MemoryBenchmarkRunnerError:
            raise
        except Exception as exc:
            if self._config.on_runner_error == "raise":
                raise MemoryBenchmarkRunnerError(
                    f"{name} failed: {exc}"
                ) from exc
            logger.warning("%s failed: %s", name, exc)
            with self._lock:
                self._runner_errors += 1
                self._last_error = f"{name}: {exc}"
            return None

        if not isinstance(result, ABCMapping):
            msg = (
                f"{name} returned {type(result).__name__}, "
                f"expected Mapping."
            )
            if self._config.on_runner_error == "raise":
                raise MemoryBenchmarkRunnerError(msg)
            logger.warning("%s; skipping.", msg)
            with self._lock:
                self._runner_errors += 1
                self._last_error = msg
            return None

        out: Dict[str, float] = {}
        for k, v in result.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                msg = f"{name}[{k!r}] must be numeric."
                if self._config.on_runner_error == "raise":
                    raise MemoryBenchmarkRunnerError(msg)
                logger.warning("%s; skipping.", msg)
                with self._lock:
                    self._runner_errors += 1
                    self._last_error = msg
                return None
            fv = float(v)
            if not math.isfinite(fv):
                msg = f"{name}[{k!r}] must be finite."
                if self._config.on_runner_error == "raise":
                    raise MemoryBenchmarkRunnerError(msg)
                logger.warning("%s; skipping.", msg)
                with self._lock:
                    self._runner_errors += 1
                    self._last_error = msg
                return None
            out[str(k)] = fv
        return out

    def _call_with_timeout(
        self,
        fn: Callable[[Any], Any],
        arg: Any,
        *,
        name: str,
    ) -> Any:
        timeout = self._config.runner_timeout_seconds
        if timeout is None:
            return fn(arg)

        # Bug fix #6: hold the guard during submit so close() cannot
        # race between _acquire_executor and submit.
        with self._executor_guard:
            executor = self._acquire_executor()
            try:
                future = executor.submit(fn, arg)
            except RuntimeError as exc:
                if not self._owns_executor:
                    raise MemoryBenchmarkRunnerError(
                        f"executor is shutting down: {exc}"
                    ) from exc
                # Executor was shut down between guard checks; rebuild once.
                self._executor = ThreadPoolExecutor(
                    max_workers=2,
                    thread_name_prefix="memory-benchmark",
                )
                future = self._executor.submit(fn, arg)

        try:
            return future.result(timeout=timeout)
        except _FuturesTimeoutError as exc:
            future.cancel()
            raise MemoryBenchmarkRunnerError(
                f"{name} timed out after {timeout}s."
            ) from exc
        except _FuturesCancelledError as exc:
            # Bug fix #7: executor shutdown cancels in-flight futures.
            raise MemoryBenchmarkRunnerError(
                f"{name} was cancelled (executor shutting down)."
            ) from exc

    def _bootstrap_ci(
        self,
        memory_vals: Sequence[float],
        baseline_vals: Sequence[float],
        *,
        rng: random.Random,
    ) -> Tuple[float, float]:
        n = len(memory_vals)
        if n < 2:
            delta = statistics.fmean(memory_vals) - statistics.fmean(
                baseline_vals
            )
            return delta, delta
        deltas: List[float] = []
        for _ in range(self._config.bootstrap_samples):
            idx = [rng.randrange(n) for _ in range(n)]
            m = statistics.fmean(memory_vals[i] for i in idx)
            b = statistics.fmean(baseline_vals[i] for i in idx)
            deltas.append(m - b)
        deltas.sort()
        alpha = 1.0 - self._config.confidence_level
        last = len(deltas) - 1
        # Refinement: integer arithmetic on the index computation.
        lo_idx = max(0, min(last, int(alpha / 2 * len(deltas))))
        hi_idx = max(
            0,
            min(last, int((1 - alpha / 2) * len(deltas)) - 1),
        )
        if lo_idx > hi_idx:
            lo_idx, hi_idx = hi_idx, lo_idx
        return deltas[lo_idx], deltas[hi_idx]

    @staticmethod
    def _cohen_d(diffs: Sequence[float]) -> float:
        if len(diffs) < 2:
            return 0.0
        mean_diff = statistics.fmean(diffs)
        try:
            std_diff = statistics.stdev(diffs)
        except statistics.StatisticsError:
            return 0.0
        if std_diff == 0.0:
            return 0.0
        return mean_diff / std_diff

    def _decide_verdict(
        self,
        votes: Sequence[int],
        weights: Sequence[float],
        metrics: Mapping[str, MetricStats],
    ) -> str:
        policy = self._config.verdict_policy
        if policy == "score":
            score = sum(votes)
            if score > 0:
                return "memory_helps"
            if score < 0:
                return "memory_hurts"
            return "memory_neutral"
        if policy == "majority":
            # Strict net majority: positive votes must exceed half of
            # all metrics (including abstentions).
            score = sum(votes)
            threshold = len(votes) / 2.0
            if score > threshold:
                return "memory_helps"
            if score < -threshold:
                return "memory_hurts"
            return "memory_neutral"
        if policy == "all_significant":
            nonzero = [v for v in votes if v != 0]
            if not nonzero:
                return "memory_neutral"
            if all(v > 0 for v in nonzero) and len(nonzero) == len(votes):
                return "memory_helps"
            if all(v < 0 for v in nonzero) and len(nonzero) == len(votes):
                return "memory_hurts"
            return "memory_neutral"
        if policy == "weighted":
            score = sum(v * w for v, w in zip(votes, weights))
            if score > 0:
                return "memory_helps"
            if score < 0:
                return "memory_hurts"
            return "memory_neutral"
        raise MemoryBenchmarkConfigError(  # pragma: no cover
            f"Unknown verdict_policy: {policy!r}."
        )

    def _bundle_extras(self, bundle: RecallBundle) -> Dict[str, float]:
        extras = {
            "recall_token_cost": float(bundle.token_cost),
            "recall_latency_ms": float(bundle.latency_ms),
            "recall_adapter_latency_ms": float(bundle.adapter_latency_ms),
            "recall_evidence_count": float(len(bundle.memories)),
            "recall_truncated": float(bundle.truncated),
            "recall_clamped_k": float(bundle.clamped_k),
            "recall_candidates_considered": float(
                bundle.candidates_considered
            ),
        }
        trusted = sum(
            v for k, v in bundle.truth_level_mix.items()
            if k in self._config.trusted_truth_levels
        )
        extras["recall_trusted_count"] = float(trusted)
        return extras

    def _verdict_extras(self, verdict: GuardVerdict) -> Dict[str, float]:
        return {
            "guard_allowed": float(verdict.allowed),
            "guard_requires_human": float(verdict.requires_human),
            "guard_telemetry_drift": float(verdict.telemetry_drift),
            "guard_evidence_count": float(verdict.evidence_count),
            "guard_trusted_count": float(verdict.trusted_evidence_count),
            "guard_policy_drift": float(verdict.policy_drift),
        }

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            durations = list(self._duration_ring)
            mean_dur = sum(durations) / len(durations) if durations else 0.0
            return {
                "schema_version": SCHEMA_VERSION,
                "runs": self._total_runs,
                "task_pairs": self._total_task_pairs,
                "task_failures": self._total_task_failures,
                "runner_errors": self._runner_errors,
                "mean_duration_ms": mean_dur,
                "p50_duration_ms": _percentile(durations, 50),
                "p95_duration_ms": _percentile(durations, 95),
                "max_duration_ms": max(durations) if durations else 0.0,
                "last_report": (
                    self._last_report.to_dict()
                    if self._last_report is not None else None
                ),
                "last_error": self._last_error,
                "config": self._config.to_dict(),
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self) -> int:
        """Reset counters. Returns the number of runs cleared."""
        with self._lock:
            cleared = self._total_runs
            self._total_runs = 0
            self._total_task_pairs = 0
            self._total_task_failures = 0
            self._runner_errors = 0
            self._duration_ring.clear()
            self._last_report = None
            self._last_error = None
            self._started_at = time.monotonic()
        return cleared

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "config": self._config.to_dict(),
            "statistics": self.statistics(),
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
        strict: Optional[bool] = None,
    ) -> "MemoryBenchmark":
        """Rebuild a ``MemoryBenchmark`` from a ``to_dict()`` payload.

        ``strict`` overrides the serialized ``strict`` value when
        provided; otherwise the payload's ``strict`` field is used
        (defaulting to ``None`` to mean "not set").
        """
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkParseError(
                "MemoryBenchmark.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob
            if isinstance(cfg_blob, MemoryBenchmarkConfig)
            else MemoryBenchmarkConfig.from_dict(cfg_blob)
        )
        # Bug fix #8: honour the payload's strict flag instead of the
        # dead no-op branch.
        if strict is None and "strict" in data:
            strict = bool(data.get("strict"))
        return cls(config=config, strict=strict)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
    ) -> "MemoryBenchmark":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryBenchmarkParseError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkParseError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ---------------------------------------------------------------- repr
    def __repr__(self) -> str:
        with self._lock:
            return (
                "MemoryBenchmark("
                f"seed={self._config.seed}, "
                f"samples={self._config.bootstrap_samples}, "
                f"runs={self._total_runs}, "
                f"policy={self._config.on_runner_error!r})"
            )


__all__ = [
    "BenchmarkReport",
    "DEFAULT_CONTAINER_TAG",
    "MemoryBenchmark",
    "MemoryBenchmarkConfig",
    "MemoryBenchmarkError",
    "MemoryBenchmarkInputError",
    "MemoryBenchmarkRunnerError",
    "MemoryBenchmarkConfigError",
    "MemoryBenchmarkParseError",
    "MetricDirection",
    "MetricStats",
    "SCHEMA_VERSION",
    "TaskResult",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.memory_benchmark
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .bounded_recall import RecallBundle
    from .feedback_loop_guard import GuardVerdict

    bench = MemoryBenchmark()
    print("repr         :", bench)

    tasks = [{"id": i} for i in range(20)]

    def without_memory(task):
        rng = random.Random(task["id"] + 100)
        return {
            "quality": 0.80 + rng.random() * 0.05,
            "latency_ms": 400 + rng.random() * 60,
            "tokens": 1_800 + rng.randint(-100, 100),
            "energy_wh": 9.0 + rng.random() * 0.5,
            "carbon_gco2e": 4.2 + rng.random() * 0.3,
            "decision_consistency": 0.55 + rng.random() * 0.1,
        }

    def with_memory(task):
        rng = random.Random(task["id"] + 200)
        return {
            "quality": 0.84 + rng.random() * 0.05,
            "latency_ms": 380 + rng.random() * 50,
            "tokens": 1_400 + rng.randint(-100, 100),
            "energy_wh": 8.6 + rng.random() * 0.4,
            "carbon_gco2e": 4.0 + rng.random() * 0.25,
            "decision_consistency": 0.72 + rng.random() * 0.08,
        }

    # --------------------------------------------------- 1. basic
    report = bench.run_pair(
        tasks=tasks,
        without_memory=without_memory,
        with_memory=with_memory,
    )
    print("verdict      :", report.verdict, f"(score={report.score:+d})")
    assert report.verdict == "memory_helps"
    assert report.tasks_succeeded == 20
    assert report.tasks_failed == 0
    assert len(report.per_task) == 20
    assert report.id.startswith("report:")

    # --------------------------------------------------- 2. MetricDirection enum
    for stats in report.metrics.values():
        assert stats.direction in MetricDirection.values()
        _ = stats.direction_enum  # exercises the property
    print("direction    : OK")

    # --------------------------------------------------- 3. deep-freeze + hash
    first_task = report.per_task[0]
    try:
        first_task.baseline["hacked"] = 1.0  # type: ignore[index]
    except TypeError:
        print("task frozen  : OK")
    try:
        report.metrics["quality"].significant = True  # type: ignore[misc]
    except Exception:
        # frozen dataclass raises FrozenInstanceError; acceptable.
        pass
    hash(report)
    hash(first_task)
    hash(report.metrics["quality"])
    print("hashable     : OK")

    # --------------------------------------------------- 4. persistence bridge (bug fix #1)
    payload = report.to_episode_payload()
    assert payload["container_tag"] == DEFAULT_CONTAINER_TAG
    assert payload["metadata"]["container_tag"] == DEFAULT_CONTAINER_TAG
    assert payload["metadata"]["schema_version"] == SCHEMA_VERSION
    explicit = report.to_episode_payload(container_tag="org:custom")
    assert explicit["container_tag"] == "org:custom"
    md = report.to_memory_dict()
    assert md["id"].startswith("report:")
    assert md["metadata"]["kind"] == "benchmark_report"
    print("episode pld  : OK")

    # --------------------------------------------------- 5. TaskResult.to_memory_dict
    t_md = first_task.to_memory_dict()
    assert {"id", "content", "metadata"} <= set(t_md)
    assert t_md["id"] == "task:0"
    print("task memory  : OK")

    # --------------------------------------------------- 6. from_dict wrapped casts (bugs 3/4/5)
    bad_metrics = {
        "baseline": "not-a-number",
        "memory": 1.0, "delta": 0.0, "relative_delta": 0.0,
        "ci_low": 0.0, "ci_high": 0.0, "cohen_d": 0.0,
        "significant": False, "direction": "improves",
    }
    try:
        MetricStats.from_dict(bad_metrics)
    except MemoryBenchmarkParseError as exc:
        print("metric cast  : OK ->", exc)
    else:
        raise AssertionError("expected MemoryBenchmarkParseError")

    try:
        TaskResult.from_dict({"index": "not-an-int"})
    except MemoryBenchmarkParseError as exc:
        print("task cast    : OK ->", exc)
    else:
        raise AssertionError("expected MemoryBenchmarkParseError")

    try:
        BenchmarkReport.from_dict({"tasks": "abc"})
    except MemoryBenchmarkParseError as exc:
        print("report cast  : OK ->", exc)
    else:
        raise AssertionError("expected MemoryBenchmarkParseError")

    # Bool slips through numeric casts are rejected.
    try:
        MetricStats.from_dict({**bad_metrics, "baseline": True})
    except MemoryBenchmarkParseError:
        print("bool cast    : OK")

    # --------------------------------------------------- 7. from_dict strict round trip (bug fix #8)
    skipped = MemoryBenchmark(strict=False)
    skipped.run_pair(
        tasks=tasks,
        without_memory=without_memory,
        with_memory=with_memory,
    )
    payload_dict = skipped.to_dict()
    payload_dict["strict"] = False
    restored = MemoryBenchmark.from_dict(payload_dict)
    assert restored.config.on_runner_error == "skip"
    print("strict rt    : OK")

    # --------------------------------------------------- 8. from_config / from_pipeline
    rebuilt = MemoryBenchmark.from_config(
        MemoryBenchmarkConfig(), strict=True,
    )
    assert isinstance(rebuilt, MemoryBenchmark)
    class _FakePipeline:
        benchmark = bench
    from_pipe = MemoryBenchmark.from_pipeline(_FakePipeline())
    assert from_pipe is bench
    print("from_*       : OK")

    # --------------------------------------------------- 9. batch + async
    batches = bench.run_pairs([
        {"tasks": tasks, "without_memory": without_memory,
         "with_memory": with_memory},
        {"tasks": tasks[:8], "without_memory": without_memory,
         "with_memory": with_memory},
    ])
    assert len(batches) == 2

    async def _async_batch():
        a = await bench.run_pair_async(
            tasks=tasks, without_memory=without_memory,
            with_memory=with_memory,
        )
        b = await bench.run_pairs_async([
            {"tasks": tasks, "without_memory": without_memory,
             "with_memory": with_memory},
        ])
        return a, b

    a_report, b_reports = asyncio.run(_async_batch())
    assert a_report.verdict == "memory_helps"
    assert len(b_reports) == 1
    print("async        : OK")

    # --------------------------------------------------- 10. extractors + truth/container
    def bundle_extractor(task):
        i = task["id"]
        return RecallBundle(
            query=f"task-{i}",
            memories=tuple(
                {
                    "id": f"m-{i}-{j}",
                    "content": f"content {i}",
                    "metadata": {
                        "run_id": f"run-{i}-{j}",
                        "truth_level": "measured",
                    },
                }
                for j in range(3)
            ),
            scores=(0.9, 0.85, 0.8),
            citations=tuple(f"run-{i}-{j}" for j in range(3)),
            token_cost=42,
            latency_ms=8.0,
            truth_level_mix={"measured": 3},
            adapter_latency_ms=6.5,
            truncated=(i % 7 == 0),
            clamped_k=False,
        )

    def verdict_extractor(task):
        return GuardVerdict(
            allowed=(task["id"] % 3 != 0),
            reasons=("telemetry_drift",),
            reason_codes=("telemetry_drift",),
            staleness_seconds=10.0,
            staleness_known=True,
            policy_drift=False,
            telemetry_drift=0.3,
            requires_human=(task["id"] % 3 == 0),
            evidence_count=3,
            trusted_evidence_count=3,
            untrusted_evidence_count=0,
        )

    report2 = bench.run_pair(
        tasks=tasks,
        without_memory=without_memory,
        with_memory=with_memory,
        bundle_extractor=bundle_extractor,
        verdict_extractor=verdict_extractor,
        task_truth_level=(
            lambda t: "measured" if t["id"] % 2 == 0 else "simulated"
        ),
        task_container_tag=lambda t: "org:green-agent",
    )
    assert "recall_token_cost" in report2.extras
    assert "guard_allowed" in report2.extras
    assert report2.truth_level_mix == {"measured": 10, "simulated": 10}
    assert report2.container_tag_mix == {"org:green-agent": 20}
    print("extractors   : OK")

    # --------------------------------------------------- 11. strict skip path
    skipped2 = MemoryBenchmark(strict=False)
    state = {"count": 0}

    def sometimes_failing(task):
        state["count"] += 1
        if state["count"] == 3:
            raise RuntimeError("simulated runner failure")
        return without_memory(task)

    report3 = skipped2.run_pair(
        tasks=tasks,
        without_memory=sometimes_failing,
        with_memory=with_memory,
    )
    assert report3.tasks_failed >= 1
    assert report3.tasks_succeeded < 20
    skipped2.close()
    print("skip path    : OK")

    # --------------------------------------------------- 12. timeout
    slow_bench = MemoryBenchmark(
        config=MemoryBenchmarkConfig(
            min_task_count=2, runner_timeout_seconds=0.05,
        ),
    )

    def slow_runner(task):
        time.sleep(0.5)
        return without_memory(task)

    try:
        slow_bench.run_pair(
            tasks=[{"id": 0}, {"id": 1}],
            without_memory=slow_runner,
            with_memory=slow_runner,
        )
    except MemoryBenchmarkRunnerError as exc:
        print("timeout      : OK ->", exc)
    else:
        raise AssertionError("expected a timeout")
    slow_bench.close()

    # --------------------------------------------------- 13. executor/close race (bug fix #6/7)
    import threading as _t

    race_bench = MemoryBenchmark(
        config=MemoryBenchmarkConfig(
            min_task_count=2, runner_timeout_seconds=5.0,
        ),
    )
    stop = _t.Event()
    errors: List[BaseException] = []

    def _hammer():
        while not stop.is_set():
            try:
                race_bench.run_pair(
                    tasks=[{"id": 0}, {"id": 1}],
                    without_memory=without_memory,
                    with_memory=with_memory,
                )
            except BaseException as exc:
                errors.append(exc)
                return

    threads = [_t.Thread(target=_hammer) for _ in range(2)]
    for t in threads:
        t.start()
    for _ in range(5):
        race_bench.close()
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors
    print("close race   : OK")

    # --------------------------------------------------- 14. config validation
    bad_configs = [
        dict(seed=True),
        dict(seed=-1),
        dict(bootstrap_samples=0),
        dict(confidence_level=0.0),
        dict(confidence_level=1.0),
        dict(confidence_level=float("nan")),
        dict(min_task_count=1),
        dict(max_task_count=3, min_task_count=10),
        dict(runner_timeout_seconds=0),
        dict(runner_timeout_seconds=float("inf")),
        dict(latency_ring_size=0),
        dict(metric_names="quality"),
        dict(metric_names=()),
        dict(metric_names=("quality", "quality")),
        dict(higher_is_better={"quality": "yes"}),
        dict(on_runner_error="bogus"),
        dict(verdict_policy="bogus"),
        dict(truth_levels=()),
        dict(trusted_truth_levels=("unknown",)),
        dict(truth_level_weights={"unknown": 1.0}),
        dict(expected_container_tag=""),
        dict(include_per_task_breakdown="yes"),
        dict(schema_version=0),
    ]
    for bad in bad_configs:
        try:
            MemoryBenchmarkConfig(**bad)  # type: ignore[arg-type]
        except MemoryBenchmarkError as exc:
            print(f"reject cfg   : {list(bad)[0]} -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    # --------------------------------------------------- 15. input validation
    for bad in (
        lambda: bench.run_pair(
            tasks=[], without_memory=lambda t: {},
            with_memory=lambda t: {},
        ),
        lambda: bench.run_pair(
            tasks=tasks[:2], without_memory=lambda t: {},
            with_memory=lambda t: {},
        ),
        lambda: bench.run_pair(
            tasks=tasks, without_memory="not-callable",
            with_memory=lambda t: {},
        ),
        lambda: bench.run_pair(
            tasks=tasks,
            without_memory=lambda t: {"quality": float("nan")},
            with_memory=lambda t: {"quality": 1.0},
        ),
        lambda: bench.run_pair(
            tasks=tasks,
            without_memory=lambda t: {"quality": True},
            with_memory=lambda t: {"quality": 1.0},
        ),
    ):
        try:
            bad()
        except MemoryBenchmarkError as exc:
            print("reject input :", exc)

    # --------------------------------------------------- 16. stats / reset / last_report
    stats = bench.statistics()
    assert "schema_version" in stats
    assert "last_error" in stats
    assert bench.last_report is not None
    cleared = bench.reset()
    assert cleared >= 1 and bench.statistics()["runs"] == 0
    assert bench.last_report is None
    print("stats/reset  : OK")

    # --------------------------------------------------- 17. MetricStats / TaskResult RT
    ms = report.metrics["quality"]
    ms_rt = MetricStats.from_dict(ms.to_dict())
    assert ms_rt == ms
    tr_rt = TaskResult.from_dict(first_task.to_dict())
    assert tr_rt == first_task
    report_rt = BenchmarkReport.from_dict(report.to_dict())
    assert report_rt.verdict == report.verdict
    assert report_rt.schema_version == SCHEMA_VERSION
    assert report_rt.tasks_succeeded == report.tasks_succeeded
    hash(report_rt)
    print("RT           : OK")

    # --------------------------------------------------- 18. context manager
    with MemoryBenchmark() as ctx:
        ctx.run_pair(
            tasks=tasks,
            without_memory=without_memory,
            with_memory=with_memory,
        )
    print("ctx mgr      : OK")

    bench.close()
    print("\nSmoke test passed.")
