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
  tag expectation, per-task breakdown toggle.
- ``MetricStats`` — frozen per-metric statistics with effect size
  (Cohen's d), relative delta, significance, and direction.
- ``TaskResult`` — frozen per-task record with baseline/memory metrics,
  extras pulled from the recall bundle and guard verdict.
- ``BenchmarkReport`` — deeply frozen, hashable, fully serializable,
  with ``to_episode_payload()`` for persistence via ``EpisodicMemory``
  or ``SupermemoryAdapter``.
- Working ``strict=False`` path via ``on_runner_error="skip"`` (the
  previous release accepted the flag but always raised anyway).
- Guarded bootstrap CI indices (previous release could invert bounds).
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
- Async sibling (``run_pair_async``) and batch helper (``run_pairs``).
- Per-runner timeout enforcement via a shared ``ThreadPoolExecutor``.
- ``statistics()`` / ``reset()`` / ``close()`` / context manager.
- Structured error hierarchy:
  ``MemoryBenchmarkError`` → ``MemoryBenchmarkInputError``,
  ``MemoryBenchmarkRunnerError``, ``MemoryBenchmarkConfigError``.
- Verdict policies: ``score``, ``majority``, ``all_significant``,
  ``weighted`` — configurable to reflect different risk appetites.
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
    ThreadPoolExecutor,
    TimeoutError as _FuturesTimeoutError,
)
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
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

__version__ = "6.0.0"


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
_DEFAULT_METRIC_NAMES: Tuple[str, ...] = (
    "quality", "latency_ms", "tokens", "energy_wh", "carbon_gco2e",
    "decision_consistency",
)

_DEFAULT_HIGHER_IS_BETTER: Mapping[str, bool] = {
    "quality": True,
    "latency_ms": False,
    "tokens": False,
    "energy_wh": False,
    "carbon_gco2e": False,
    "decision_consistency": True,
}

_DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)

_DEFAULT_TRUSTED_TRUTH_LEVELS: Tuple[str, ...] = ("measured",)

_ON_RUNNER_ERROR_POLICIES: Tuple[str, ...] = ("raise", "skip")
_VERDICT_POLICIES: Tuple[str, ...] = (
    "score", "majority", "all_significant", "weighted",
)

_DEFAULT_RUNNER_TIMEOUT_SECONDS: Optional[float] = None


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
    """Parse an ISO 8601 timestamp; tolerate trailing ``Z`` and epochs."""
    if value is None:
        return None
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


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryBenchmarkConfig:
    """Tunable parameters for the benchmark harness."""

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
        # seed / counts
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

        # confidence
        if not (isinstance(self.confidence_level, (int, float))
                and not isinstance(self.confidence_level, bool)
                and math.isfinite(self.confidence_level)):
            raise MemoryBenchmarkConfigError(
                "confidence_level must be a finite number."
            )
        if not 0.0 < self.confidence_level < 1.0:
            raise MemoryBenchmarkConfigError(
                "confidence_level must be in (0, 1)."
            )

        # timeout
        if self.runner_timeout_seconds is not None:
            if not _is_positive_finite(self.runner_timeout_seconds):
                raise MemoryBenchmarkConfigError(
                    "runner_timeout_seconds must be None or a finite > 0."
                )

        # metric names
        object.__setattr__(
            self,
            "metric_names",
            _normalize_string_tuple(self.metric_names, name="metric_names"),
        )

        # higher_is_better: a Mapping[str, bool] with the same keys as
        # metric_names.
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

        # policies
        if self.on_runner_error not in _ON_RUNNER_ERROR_POLICIES:
            raise MemoryBenchmarkConfigError(
                f"on_runner_error must be one of "
                f"{_ON_RUNNER_ERROR_POLICIES}."
            )
        if self.verdict_policy not in _VERDICT_POLICIES:
            raise MemoryBenchmarkConfigError(
                f"verdict_policy must be one of {_VERDICT_POLICIES}."
            )

        # truth levels
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

        # truth_level_weights
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

        # weight_by_truth_level
        if not isinstance(self.weight_by_truth_level, bool):
            raise MemoryBenchmarkConfigError(
                "weight_by_truth_level must be a bool."
            )
        if self.weight_by_truth_level and not self.truth_level_weights:
            # Fall back to 1.0 for trusted, 0.5 for others, to keep it
            # meaningful without requiring the caller to pass weights.
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

        # container tag
        if self.expected_container_tag is not None:
            if (
                not isinstance(self.expected_container_tag, str)
                or not self.expected_container_tag
            ):
                raise MemoryBenchmarkConfigError(
                    "expected_container_tag must be None or a non-empty "
                    "string."
                )

        # per-task breakdown
        if not isinstance(self.include_per_task_breakdown, bool):
            raise MemoryBenchmarkConfigError(
                "include_per_task_breakdown must be a bool."
            )

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "seed": self.seed,
            "bootstrap_samples": self.bootstrap_samples,
            "confidence_level": self.confidence_level,
            "min_task_count": self.min_task_count,
            "max_task_count": self.max_task_count,
            "metric_names": list(self.metric_names),
            "higher_is_better": dict(self.higher_is_better),
            "runner_timeout_seconds": self.runner_timeout_seconds,
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
    # Mutation helpers
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
            self.seed,
            self.bootstrap_samples,
            self.confidence_level,
            self.min_task_count,
            self.max_task_count,
            self.metric_names,
            tuple(sorted(self.higher_is_better.items())),
            self.runner_timeout_seconds,
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
    direction: str  # "improves" | "hurts" | "inconclusive"

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
        if self.direction not in ("improves", "hurts", "inconclusive"):
            raise MemoryBenchmarkInputError(
                f"direction must be one of "
                f"('improves', 'hurts', 'inconclusive'), got "
                f"{self.direction!r}."
            )
        if self.ci_low > self.ci_high:
            raise MemoryBenchmarkInputError(
                "ci_low must be <= ci_high."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
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
            raise MemoryBenchmarkInputError(
                "MetricStats.from_dict expects a Mapping."
            )
        return cls(
            baseline=float(data.get("baseline", 0.0)),
            memory=float(data.get("memory", 0.0)),
            delta=float(data.get("delta", 0.0)),
            relative_delta=float(data.get("relative_delta", 0.0)),
            ci_low=float(data.get("ci_low", 0.0)),
            ci_high=float(data.get("ci_high", 0.0)),
            cohen_d=float(data.get("cohen_d", 0.0)),
            significant=bool(data.get("significant", False)),
            direction=str(data.get("direction", "inconclusive")),
        )

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

    def to_dict(self) -> Dict[str, Any]:
        return {
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
            raise MemoryBenchmarkInputError(
                "TaskResult.from_dict expects a Mapping."
            )
        return cls(
            index=int(data.get("index", 0)),
            baseline=dict(data.get("baseline", {})),
            memory=dict(data.get("memory", {})),
            delta=dict(data.get("delta", {})),
            truth_level=data.get("truth_level"),
            container_tag=data.get("container_tag"),
            bundle_extras=dict(data.get("bundle_extras", {})),
            verdict_extras=dict(data.get("verdict_extras", {})),
        )

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

    # ------------------------------------------------------------------ #
    def _summarize(self) -> str:
        improved = sum(
            1 for v in self.metrics.values() if v.direction == "improves"
        )
        hurt = sum(
            1 for v in self.metrics.values() if v.direction == "hurts"
        )
        return (
            f"Memory benchmark: {self.verdict} "
            f"(score={self.score:+d}, "
            f"{self.tasks_succeeded}/{self.tasks} tasks, "
            f"improved={improved}, hurt={hurt})"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
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
            raise MemoryBenchmarkInputError(
                "BenchmarkReport.from_dict expects a Mapping."
            )
        ts_raw = data.get("timestamp")
        ts = _parse_iso_datetime(ts_raw) if ts_raw else None
        if ts is None:
            ts = datetime.now(timezone.utc)
        raw_metrics = data.get("metrics", {})
        if not isinstance(raw_metrics, ABCMapping):
            raise MemoryBenchmarkInputError(
                "'metrics' must be a Mapping."
            )
        metrics = {
            k: MetricStats.from_dict(v) for k, v in raw_metrics.items()
        }
        raw_per_task = data.get("per_task", [])
        if not isinstance(raw_per_task, (list, tuple)):
            raise MemoryBenchmarkInputError("'per_task' must be a sequence.")
        per_task = tuple(TaskResult.from_dict(t) for t in raw_per_task)
        return cls(
            tasks=int(data.get("tasks", 0)),
            tasks_succeeded=int(data.get("tasks_succeeded", 0)),
            tasks_failed=int(data.get("tasks_failed", 0)),
            metrics=metrics,
            extras=dict(data.get("extras", {})),
            per_task=per_task,
            verdict=str(data.get("verdict", "memory_neutral")),
            score=int(data.get("score", 0)),
            seed=int(data.get("seed", 0)),
            truth_level_mix=dict(data.get("truth_level_mix", {})),
            container_tag_mix=dict(data.get("container_tag_mix", {})),
            timestamp=ts,
        )

    @classmethod
    def from_json(cls, payload: str) -> "BenchmarkReport":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryBenchmarkInputError(
                f"BenchmarkReport.from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Persistence bridge
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
        meta: Dict[str, Any] = {
            "kind": "benchmark_report",
            "observed_at": self.timestamp.isoformat(),
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
        if container_tag is not None:
            if not isinstance(container_tag, str) or not container_tag:
                raise MemoryBenchmarkInputError(
                    "container_tag must be None or a non-empty string."
                )
            meta["container_tag"] = container_tag
        return {
            "content": content if content is not None else self._summarize(),
            "container_tag": container_tag,
            "metadata": meta,
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
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

    _DURATION_RING_SIZE: int = 200

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
        # Backward compat: ``strict`` maps to ``on_runner_error``.
        if strict is not None:
            config = config.with_overrides(
                on_runner_error="raise" if strict else "skip",
            )
        self._config = config

        self._lock = threading.RLock()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._owns_executor: bool = False
        self._executor_guard = threading.Lock()

        # Counters
        self._total_runs = 0
        self._total_task_pairs = 0
        self._total_task_failures = 0
        self._runner_errors = 0
        self._duration_ring: Deque[float] = deque(
            maxlen=self._DURATION_RING_SIZE
        )
        self._last_report: Optional[BenchmarkReport] = None
        self._last_error: Optional[str] = None
        self._started_at = time.monotonic()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MemoryBenchmarkConfig:
        return self._config

    # -------------------------------------------------------------- executor
    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is not None:
            return self._executor
        with self._executor_guard:
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
        # ------------------------------------------------------- validate
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
                # Unreachable: ``_run_runner`` raises on "raise".
                raise MemoryBenchmarkRunnerError(  # pragma: no cover
                    f"runner failed at task {i}"
                )

            # Every configured metric must be present.
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

            # Truth level and container tag extraction.
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

            # Extras: bundle and verdict.
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

            # Collect.
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
                # Should never happen after the success check above.
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
                sig_dir = "improves" if higher else "hurts"
                significant = True
            elif ci_high < 0.0:
                sig_dir = "hurts" if higher else "improves"
                significant = True
            else:
                sig_dir = "inconclusive"
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
            )

            if sig_dir == "improves":
                votes.append(+1)
                weights.append(abs(relative))
            elif sig_dir == "hurts":
                votes.append(-1)
                weights.append(abs(relative))
            else:
                votes.append(0)
                weights.append(0.0)

        score = sum(votes)
        verdict = self._decide_verdict(votes, weights, metrics)

        # Extras aggregated as memory-side means.
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
        """Run ``run_pair`` for each item.

        Each item must be a Mapping with keys ``tasks``,
        ``without_memory``, ``with_memory`` and optional
        ``bundle_extractor``, ``verdict_extractor``,
        ``task_truth_level``, ``task_container_tag``, ``now``.
        """
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
        executor = self._get_executor()
        future = executor.submit(fn, arg)
        try:
            return future.result(timeout=timeout)
        except _FuturesTimeoutError as exc:
            future.cancel()
            raise MemoryBenchmarkRunnerError(
                f"{name} timed out after {timeout}s."
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
            # Degenerate: CI collapses to the point estimate.
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
        lo_idx = max(0, min(last, int(math.floor(alpha / 2 * len(deltas)))))
        hi_idx = max(
            0,
            min(
                last,
                int(math.ceil((1 - alpha / 2) * len(deltas))) - 1,
            ),
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
        # Unreachable
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
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkInputError(
                "MemoryBenchmark.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob
            if isinstance(cfg_blob, MemoryBenchmarkConfig)
            else MemoryBenchmarkConfig.from_dict(cfg_blob)
        )
        if strict is None:
            strict = None
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
            raise MemoryBenchmarkInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryBenchmarkInputError(
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
    "MemoryBenchmark",
    "MemoryBenchmarkConfig",
    "MemoryBenchmarkError",
    "MemoryBenchmarkInputError",
    "MemoryBenchmarkRunnerError",
    "MemoryBenchmarkConfigError",
    "MetricStats",
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
    print("repr       :", bench)

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
    print("verdict    :", report.verdict, f"(score={report.score:+d})")
    for metric, stats in report.metrics.items():
        print(
            f"  {metric:<22} delta={stats.delta:+.4g} "
            f"ci=[{stats.ci_low:+.4g}, {stats.ci_high:+.4g}] "
            f"d={stats.cohen_d:+.2f} {stats.direction}"
        )
    assert report.verdict == "memory_helps"
    assert report.tasks_succeeded == 20
    assert report.tasks_failed == 0
    assert len(report.per_task) == 20

    # --------------------------------------------------- 2. per-task breakdown
    first = report.per_task[0]
    print("task[0]     :", first)

    # --------------------------------------------------- 3. persistence bridge
    payload = report.to_episode_payload(
        container_tag="org:green-agent",
    )
    assert payload["metadata"]["kind"] == "benchmark_report"
    print("ep payload :", payload["content"])

    # --------------------------------------------------- 4. extractors
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

    def truth_extractor(task):
        return "measured" if task["id"] % 2 == 0 else "simulated"

    def tag_extractor(task):
        return "org:green-agent"

    report2 = bench.run_pair(
        tasks=tasks,
        without_memory=without_memory,
        with_memory=with_memory,
        bundle_extractor=bundle_extractor,
        verdict_extractor=verdict_extractor,
        task_truth_level=truth_extractor,
        task_container_tag=tag_extractor,
    )
    print("extras     :", {
        k: round(v, 3) for k, v in report2.extras.items()
    })
    assert "recall_token_cost" in report2.extras
    assert "guard_allowed" in report2.extras
    assert report2.truth_level_mix == {"measured": 10, "simulated": 10}
    assert report2.container_tag_mix == {"org:green-agent": 20}

    # --------------------------------------------------- 5. strict=False (skip)
    skipped_bench = MemoryBenchmark(strict=False)
    state = {"count": 0}

    def sometimes_failing(task):
        state["count"] += 1
        if state["count"] == 3:
            raise RuntimeError("simulated runner failure")
        return without_memory(task)

    report3 = skipped_bench.run_pair(
        tasks=tasks,
        without_memory=sometimes_failing,
        with_memory=with_memory,
    )
    # 20 tasks, but task index 2's *baseline* failed → skipped.
    print(
        "skip path  : succeeded=%d failed=%d"
        % (report3.tasks_succeeded, report3.tasks_failed)
    )
    assert report3.tasks_failed >= 1
    assert report3.tasks_succeeded < 20
    skipped_bench.close()

    # --------------------------------------------------- 6. strict=True raises
    strict_bench = MemoryBenchmark(strict=True)
    state["count"] = 0
    try:
        strict_bench.run_pair(
            tasks=tasks,
            without_memory=sometimes_failing,
            with_memory=with_memory,
        )
    except MemoryBenchmarkRunnerError as exc:
        print("strict     : OK ->", exc)
    else:
        raise AssertionError("expected a runner error")
    strict_bench.close()

    # --------------------------------------------------- 7. timeout enforcement
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
        print("timeout    : OK ->", exc)
    else:
        raise AssertionError("expected a timeout")
    slow_bench.close()

    # --------------------------------------------------- 8. async
    async def _run_async():
        return await bench.run_pair_async(
            tasks=tasks,
            without_memory=without_memory,
            with_memory=with_memory,
        )

    async_report = asyncio.run(_run_async())
    print("async      :", async_report.verdict)

    # --------------------------------------------------- 9. run_pairs batch
    batches = bench.run_pairs([
        {
            "tasks": tasks,
            "without_memory": without_memory,
            "with_memory": with_memory,
        },
        {
            "tasks": tasks[:8],
            "without_memory": without_memory,
            "with_memory": with_memory,
        },
    ])
    print("batch      :", [b.verdict for b in batches])
    assert len(batches) == 2

    # --------------------------------------------------- 10. verdict policies
    for policy in ("score", "majority", "all_significant", "weighted"):
        p = MemoryBenchmark(
            config=MemoryBenchmarkConfig(verdict_policy=policy),
        )
        r = p.run_pair(
            tasks=tasks,
            without_memory=without_memory,
            with_memory=with_memory,
        )
        print(f"policy {policy:<18}: {r.verdict}")
        p.close()

    # --------------------------------------------------- 11. serialization
    cfg = MemoryBenchmarkConfig()
    assert MemoryBenchmarkConfig.from_dict(cfg.to_dict()) == cfg
    assert MemoryBenchmarkConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(seed=123)
    assert cfg2.seed == 123 and cfg.seed == 42
    hash(cfg)
    print("cfg RT     : OK")

    report_dict = report.to_dict()
    report_rt = BenchmarkReport.from_dict(report_dict)
    assert report_rt.verdict == report.verdict
    assert report_rt.tasks_succeeded == report.tasks_succeeded
    assert len(report_rt.per_task) == len(report.per_task)
    assert len(report_rt.metrics) == len(report.metrics)
    hash(report)
    print("report RT  : OK (hashable)")

    report_json = report.to_json()
    assert BenchmarkReport.from_json(report_json).verdict == report.verdict
    print("report JSON: OK")

    bench_dict = bench.to_dict()
    restored = MemoryBenchmark.from_dict(bench_dict)
    assert restored.config == bench.config
    print("bench RT   : OK")

    # --------------------------------------------------- 12. stats / reset
    stats = bench.statistics()
    print("statistics :", {
        k: v for k, v in stats.items()
        if k not in ("config", "last_report")
    })
    cleared = bench.reset()
    assert cleared >= 1 and bench.statistics()["runs"] == 0
    print("reset      :", cleared, "runs cleared")

    # --------------------------------------------------- 13. config validation
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
    ]
    for bad in bad_configs:
        try:
            MemoryBenchmarkConfig(**bad)
        except MemoryBenchmarkError as exc:
            print(f"reject cfg : {list(bad)[0]} -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    # --------------------------------------------------- 14. input validation
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
            print("reject inp :", exc)

    # --------------------------------------------------- 15. context manager
    with MemoryBenchmark() as ctx:
        ctx.run_pair(
            tasks=tasks,
            without_memory=without_memory,
            with_memory=with_memory,
        )
    print("ctx mgr    : OK")

    bench.close()
    print("\nSmoke test passed.")
