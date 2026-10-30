# src/optimization/ml_optimizer.py

"""
Helium-Aware ML Optimizer (v5.1)
================================

ML and data-pipeline optimization that adapts to helium scarcity zones:

- :class:`HeliumAwareMLOptimizer` — selects quantization, pruning, distillation,
  and target hardware based on the current helium zone.
- :class:`HeliumAwareDataOptimizer` — tunes batch size, cache TTL, memory
  mapping, and compression based on the current helium zone.

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``MLOptimizerConfig.__post_init__`` validates every numeric field
         and enforces ``quantization_savings + pruning_savings +
         distillation_savings <= 1.0``. Previously a config with per-step
         savings summing to 1.8 was accepted, producing an
         ``OptimizationReport`` whose ``estimated_helium_savings`` (clamped
         to 1.0) contradicted the sum of its own steps.
  * P0 — ``OptimizationReport.__post_init__`` validates every field:
         finiteness, ``optimized_model_size_mb <= original_model_size_mb``,
         ``estimated_helium_savings`` in [0, 1], non-negative
         ``estimated_accuracy_impact``, valid ``target_hardware``, and a
         timezone-aware ``timestamp``. Previously ``NaN``/``Inf`` and
         physically impossible values were accepted silently.
  * P0 — ``optimize_model()`` returns a JSON-safe payload. The caller's
         model is accessible via :meth:`get_last_model` (and the full
         ``OptimizationReport`` via :meth:`get_last_report`). Passing
         ``include_model=True`` restores the old behaviour for backward
         compatibility.
  * P0 — ``_default_size_fn`` detects torch-style models via
         ``get_memory_footprint()`` and ``parameters()`` before falling
         back to ``sys.getsizeof``. A warning is logged when the fallback
         returns an implausibly small value (< 1 KB), pointing the caller
         at ``size_fn=``.
  * P0 — ``set_helium_mode`` normalizes zone aliases (``"critical"`` ==
         ``"helium_critical"``, etc.) and fails *closed*: unknown zones
         now default to ``LIGHT`` (conservative) and log a warning,
         instead of silently falling through to ``NONE`` (do nothing).
  * P1 — History uses ``deque(maxlen=...)``; eviction is O(1) and the
         bound is enforced on restore with a warning. Previously the list
         briefly held ``max_history + 1`` entries and restore could leave
         the list permanently over capacity.
  * P1 — ``OptimizationReport.from_dict`` routes every value through
         ``_safe_*`` helpers; ``KeyError``/``TypeError``/``ValueError``
         no longer escape. ``steps``, ``timestamp``, and ``target_hardware``
         are validated.
  * P1 — ``MLOptimizerConfig.from_dict`` and ``DataOptimizerConfig.from_dict``
         rebuild from the set of dataclass fields (adding a field no longer
         requires editing the loader).
  * P1 — ``to_json`` no longer passes ``default=str``; serialization
         errors are wrapped in the module's error types.
  * P1 — ``OptimizationStep`` and ``DataPipelinePlan`` validate every
         field in ``__post_init__``. ``DataPipelinePlan`` gains
         ``from_dict`` and round-trips through JSON.
  * P1 — ``HeliumAwareDataOptimizer.set_helium_zone(zone)`` sets a
         persistent zone used when no ``execution_decision`` is provided.
  * P1 — ``base_batch_size_fn`` returning a bool is treated as invalid
         (Python's ``True`` is an ``int``).
  * P1 — ``__version__ = "5.1.0"`` and ``schema_version`` embedded in
         every serialized payload.
  * P1 — ``set_helium_mode``'s state is resolved atomically inside
         ``optimize_model``'s lock so concurrent calls with different
         zones cannot leak a stale mode across threads.
  * P2 — New helpers: ``test_smoke()``, ``health()``,
         ``OptimizationReport.key()``, ``OptimizationReport.from_json()``,
         ``get_last_report()``, ``get_last_model()``,
         ``optimize_models_batch()``, per-kind statistics.
  * P3 — ``_zone_str`` unwraps nested ``.value`` attributes; the
         ``strategies`` dict is exposed as a read-only ``MappingProxyType``;
         expired cache entries are counted separately (not pruned inline).

Backward compatibility
----------------------
- ``HeliumAwareMLOptimizer(config, *, optimizer_config=..., strict=...,
  size_fn=...)`` signature unchanged.
- ``HeliumAwareDataOptimizer(config, *, data_config=..., strict=...,
  base_batch_size_fn=...)`` signature unchanged.
- ``set_helium_mode``, ``optimize_model``, ``optimize_data_pipeline``,
  ``cache_result``, ``get_cached_result``, ``get_cache_stats``,
  ``statistics``, ``reset``, ``to_dict``, ``from_dict``, ``to_json``,
  ``from_json`` — all keep their signatures.
- ``optimize_model`` **no longer includes** ``payload["model"]`` by
  default. Callers who relied on it must pass ``include_model=True`` or
  call :meth:`get_last_model`. The payload is now JSON-serializable.
- ``MLOptimizerConfig`` and ``OptimizationReport`` now reject values they
  previously accepted silently (NaN/Inf, sum > 1, inverted sizes).
"""

from __future__ import annotations

import json
import logging
import math
import sys
import threading
import time
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "DataOptimizerConfig",
    "DataOptimizerError",
    "DataPipelinePlan",
    "HeliumAwareDataOptimizer",
    "HeliumAwareMLOptimizer",
    "HeliumOptimizationMode",
    "HeliumOptimizationStrategy",
    "MLOptimizerConfig",
    "MLOptimizerError",
    "OptimizationReport",
    "OptimizationStep",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MLOptimizerError(ValueError):
    """Raised for invalid ML optimizer inputs or configuration."""


class DataOptimizerError(ValueError):
    """Raised for invalid data optimizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class HeliumOptimizationMode(Enum):
    """Optimization modes based on helium availability."""
    AGGRESSIVE = "aggressive"   # Severe helium scarcity
    MODERATE = "moderate"       # Helium caution / critical
    LIGHT = "light"             # Normal conditions
    NONE = "none"               # Helium green


# --------------------------------------------------------------------------- #
# Safe coercion helpers
# --------------------------------------------------------------------------- #
def _safe_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"{name} must be a non-empty string, "
            f"got {type(value).__name__}"
        )
    return value


def _safe_float(
    name: str, value: Any, *, min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"{name} must be numeric, got {type(value).__name__}"
        )
    v = float(value)
    if not math.isfinite(v):
        raise ValueError(
            f"{name} must be finite, got {value!r}"
        )
    if min_value is not None and v < min_value:
        raise ValueError(
            f"{name} must be >= {min_value}, got {v!r}"
        )
    if max_value is not None and v > max_value:
        raise ValueError(
            f"{name} must be <= {max_value}, got {v!r}"
        )
    return v


def _safe_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"{name} must be an integer, got {type(value).__name__}"
        )
    if value <= 0:
        raise ValueError(
            f"{name} must be > 0, got {value!r}"
        )
    return value


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
            raise ValueError(
                f"timestamp is not ISO-8601: {value!r}"
            ) from exc
    elif value is None:
        return datetime.now(timezone.utc)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            ts = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(
                f"timestamp could not be converted: {value!r}"
            ) from exc
    else:
        raise ValueError(
            f"timestamp must be datetime, ISO str, or epoch number; "
            f"got {type(value).__name__}"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _zone_str(value: Any) -> str:
    """
    Return a lowercase zone string from an Enum, string, or object.

    Unwraps nested ``.value`` attributes up to two levels so that
    ``Mock(value=SomeEnum.RED)`` and ``SomeEnum.RED`` both resolve.
    """
    if value is None:
        return ""
    # Unwrap up to two levels of `.value`
    for _ in range(2):
        if hasattr(value, "value") and not isinstance(value, str):
            value = value.value
        else:
            break
    return str(value).lower()


# --------------------------------------------------------------------------- #
# Zone → mode mapping
# --------------------------------------------------------------------------- #
_ZONE_TO_MODE: Dict[str, HeliumOptimizationMode] = {
    "helium_critical": HeliumOptimizationMode.AGGRESSIVE,
    "critical": HeliumOptimizationMode.AGGRESSIVE,
    "helium_red": HeliumOptimizationMode.MODERATE,
    "red": HeliumOptimizationMode.MODERATE,
    "helium_yellow": HeliumOptimizationMode.LIGHT,
    "yellow": HeliumOptimizationMode.LIGHT,
    "helium_green": HeliumOptimizationMode.NONE,
    "green": HeliumOptimizationMode.NONE,
    "normal": HeliumOptimizationMode.NONE,
}


def _mode_for_zone(helium_zone: Any) -> HeliumOptimizationMode:
    """
    Pure function: map a zone (Enum / string / .value object) to a mode.

    Unknown zones log a warning and return ``LIGHT`` (conservative), not
    ``NONE``. A green agent that does *some* optimization under an
    ambiguous signal is safer than one that does nothing.
    """
    zone = _zone_str(helium_zone)
    if zone in _ZONE_TO_MODE:
        return _ZONE_TO_MODE[zone]
    logger.warning(
        "Unknown helium zone %r; defaulting to LIGHT mode. "
        "Known zones: %s.",
        helium_zone, sorted(_ZONE_TO_MODE),
    )
    return HeliumOptimizationMode.LIGHT


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HeliumOptimizationStrategy:
    """Per-mode optimization strategy."""
    quantization_precision: str
    pruning_ratio: float
    use_distillation: bool
    distillation_temperature: float
    target_hardware: str
    batch_size_multiplier: float
    cache_ttl_hours: float

    def __post_init__(self) -> None:
        if self.quantization_precision not in (
            "fp32", "fp16", "int8", "int4",
        ):
            raise MLOptimizerError(
                f"unsupported quantization_precision "
                f"{self.quantization_precision!r}."
            )
        _safe_float(
            "pruning_ratio", self.pruning_ratio,
            min_value=0.0, max_value=1.0,
        )
        if not isinstance(self.use_distillation, bool):
            raise MLOptimizerError(
                "use_distillation must be bool, "
                f"got {type(self.use_distillation).__name__}."
            )
        _safe_float(
            "distillation_temperature", self.distillation_temperature,
            min_value=1e-9,
        )
        if self.target_hardware not in (
            "cpu", "gpu", "gpu_mixed", "tpu", "edge",
        ):
            raise MLOptimizerError(
                f"unsupported target_hardware {self.target_hardware!r}."
            )
        _safe_float(
            "batch_size_multiplier", self.batch_size_multiplier,
            min_value=1e-9,
        )
        _safe_float(
            "cache_ttl_hours", self.cache_ttl_hours, min_value=1e-9,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "HeliumOptimizationStrategy":
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"HeliumOptimizationStrategy.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        known = {f.name for f in fields(cls)}
        missing = [
            k for k in known if k not in data
        ]
        if missing:
            raise MLOptimizerError(
                f"missing required field(s): {missing}"
            )
        return cls(**{k: data[k] for k in known})


@dataclass(frozen=True)
class MLOptimizerConfig:
    """Tunable parameters for the helium-aware ML optimizer."""

    aggressive: HeliumOptimizationStrategy = HeliumOptimizationStrategy(
        quantization_precision="int4",
        pruning_ratio=0.5,
        use_distillation=True,
        distillation_temperature=2.5,
        target_hardware="cpu",
        batch_size_multiplier=2.0,
        cache_ttl_hours=72.0,
    )
    moderate: HeliumOptimizationStrategy = HeliumOptimizationStrategy(
        quantization_precision="int8",
        pruning_ratio=0.3,
        use_distillation=True,
        distillation_temperature=1.5,
        target_hardware="gpu_mixed",
        batch_size_multiplier=1.5,
        cache_ttl_hours=24.0,
    )
    light: HeliumOptimizationStrategy = HeliumOptimizationStrategy(
        quantization_precision="fp16",
        pruning_ratio=0.1,
        use_distillation=False,
        distillation_temperature=1.0,
        target_hardware="gpu",
        batch_size_multiplier=1.0,
        cache_ttl_hours=12.0,
    )
    none: HeliumOptimizationStrategy = HeliumOptimizationStrategy(
        quantization_precision="fp32",
        pruning_ratio=0.0,
        use_distillation=False,
        distillation_temperature=1.0,
        target_hardware="gpu",
        batch_size_multiplier=1.0,
        cache_ttl_hours=6.0,
    )

    quantization_savings: float = 0.30
    pruning_savings: float = 0.20
    distillation_savings: float = 0.25
    pruning_accuracy_coefficient: float = 0.05
    max_history: int = 1000

    def __post_init__(self) -> None:
        # Per-step savings must be in [0, 1] and must sum to <= 1.0
        for name in (
            "quantization_savings", "pruning_savings",
            "distillation_savings",
        ):
            _safe_float(
                name, getattr(self, name),
                min_value=0.0, max_value=1.0,
            )
        total = (
            self.quantization_savings
            + self.pruning_savings
            + self.distillation_savings
        )
        if total > 1.0 + 1e-9:
            raise MLOptimizerError(
                f"quantization_savings + pruning_savings + "
                f"distillation_savings = {total:.3f}; "
                f"must be <= 1.0 so per-step savings sum to the "
                f"reported aggregate."
            )
        _safe_float(
            "pruning_accuracy_coefficient",
            self.pruning_accuracy_coefficient,
            min_value=0.0, max_value=1.0,
        )
        _safe_positive_int("max_history", self.max_history)
        # Every strategy instance must validate itself
        for name in ("aggressive", "moderate", "light", "none"):
            strat = getattr(self, name)
            if not isinstance(strat, HeliumOptimizationStrategy):
                raise MLOptimizerError(
                    f"{name} must be HeliumOptimizationStrategy, "
                    f"got {type(strat).__name__}."
                )

    def strategy_for(
        self, mode: HeliumOptimizationMode,
    ) -> HeliumOptimizationStrategy:
        return {
            HeliumOptimizationMode.AGGRESSIVE: self.aggressive,
            HeliumOptimizationMode.MODERATE: self.moderate,
            HeliumOptimizationMode.LIGHT: self.light,
            HeliumOptimizationMode.NONE: self.none,
        }[mode]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "aggressive": self.aggressive.to_dict(),
            "moderate": self.moderate.to_dict(),
            "light": self.light.to_dict(),
            "none": self.none.to_dict(),
            "quantization_savings": self.quantization_savings,
            "pruning_savings": self.pruning_savings,
            "distillation_savings": self.distillation_savings,
            "pruning_accuracy_coefficient": self.pruning_accuracy_coefficient,
            "max_history": self.max_history,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MLOptimizerConfig":
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"MLOptimizerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        strategy_names = ("aggressive", "moderate", "light", "none")
        strategies: Dict[str, HeliumOptimizationStrategy] = {}
        for name in strategy_names:
            if name not in data:
                raise MLOptimizerError(
                    f"optimizer_config missing strategy {name!r}."
                )
            strategies[name] = (
                HeliumOptimizationStrategy.from_dict(data[name])
            )

        scalar_names = (
            "quantization_savings", "pruning_savings",
            "distillation_savings", "pruning_accuracy_coefficient",
            "max_history",
        )
        scalars = {k: data[k] for k in scalar_names if k in data}
        return cls(**strategies, **scalars)


@dataclass(frozen=True)
class DataOptimizerConfig:
    """Tunable parameters for the helium-aware data optimizer."""

    critical_batch_multiplier: float = 2.5
    red_batch_multiplier: float = 2.0
    yellow_batch_multiplier: float = 1.5
    normal_batch_multiplier: float = 1.0

    critical_cache_ttl_seconds: int = 3600 * 72
    red_cache_ttl_seconds: int = 3600 * 48
    yellow_cache_ttl_seconds: int = 3600 * 24
    normal_cache_ttl_seconds: int = 3600 * 6

    default_base_batch_size: int = 32
    max_cache_entries: int = 10_000
    batched_savings: float = 0.20

    def __post_init__(self) -> None:
        for name in (
            "critical_batch_multiplier", "red_batch_multiplier",
            "yellow_batch_multiplier", "normal_batch_multiplier",
        ):
            _safe_float(name, getattr(self, name), min_value=1e-9)
        for name in (
            "critical_cache_ttl_seconds", "red_cache_ttl_seconds",
            "yellow_cache_ttl_seconds", "normal_cache_ttl_seconds",
        ):
            _safe_positive_int(name, getattr(self, name))
        _safe_positive_int(
            "default_base_batch_size", self.default_base_batch_size,
        )
        _safe_positive_int(
            "max_cache_entries", self.max_cache_entries,
        )
        _safe_float(
            "batched_savings", self.batched_savings,
            min_value=0.0, max_value=1.0,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            **asdict(self),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DataOptimizerConfig":
        if not isinstance(data, Mapping):
            raise DataOptimizerError(
                f"DataOptimizerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        known = {f.name for f in fields(cls)}
        kwargs = {k: data[k] for k in known if k in data}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Result records
# --------------------------------------------------------------------------- #
_VALID_STEP_KINDS = ("quantization", "pruning", "distillation")


@dataclass(frozen=True)
class OptimizationStep:
    """One optimization applied (or planned) on the model."""
    kind: str
    detail: str
    estimated_savings: float
    estimated_accuracy_impact: float

    def __post_init__(self) -> None:
        if self.kind not in _VALID_STEP_KINDS:
            raise MLOptimizerError(
                f"unsupported step kind {self.kind!r}; "
                f"expected one of {_VALID_STEP_KINDS}."
            )
        if not isinstance(self.detail, str) or not self.detail:
            raise MLOptimizerError(
                "detail must be a non-empty string."
            )
        _safe_float(
            "estimated_savings", self.estimated_savings,
            min_value=0.0, max_value=1.0,
        )
        _safe_float(
            "estimated_accuracy_impact", self.estimated_accuracy_impact,
            min_value=0.0,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OptimizationStep":
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"OptimizationStep.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        for required in ("kind", "detail"):
            if required not in data:
                raise MLOptimizerError(
                    f"missing required field {required!r}."
                )
        return cls(
            kind=_safe_str("kind", data["kind"]),
            detail=_safe_str("detail", data["detail"]),
            estimated_savings=_safe_float(
                "estimated_savings",
                data.get("estimated_savings", 0.0),
                min_value=0.0, max_value=1.0,
            ),
            estimated_accuracy_impact=_safe_float(
                "estimated_accuracy_impact",
                data.get("estimated_accuracy_impact", 0.0),
                min_value=0.0,
            ),
        )


@dataclass(frozen=True)
class OptimizationReport:
    """Full report of a model optimization pass."""
    mode: str
    original_model_size_mb: float
    optimized_model_size_mb: float
    steps: Tuple[OptimizationStep, ...]
    estimated_helium_savings: float
    estimated_accuracy_impact: float
    target_hardware: str
    batch_size_multiplier: float
    cache_ttl_hours: float
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    def __post_init__(self) -> None:
        if not isinstance(self.mode, str) or not self.mode:
            raise MLOptimizerError("mode must be a non-empty string.")
        for name in (
            "original_model_size_mb", "optimized_model_size_mb",
            "estimated_helium_savings", "estimated_accuracy_impact",
            "batch_size_multiplier", "cache_ttl_hours",
        ):
            _safe_float(
                name, getattr(self, name), min_value=0.0,
            )
        if (
            self.optimized_model_size_mb
            > self.original_model_size_mb + 1e-9
        ):
            raise MLOptimizerError(
                f"optimized_model_size_mb ({self.optimized_model_size_mb}) "
                f"cannot exceed original_model_size_mb "
                f"({self.original_model_size_mb}); optimization must not "
                f"increase model size."
            )
        if self.estimated_helium_savings > 1.0 + 1e-9:
            raise MLOptimizerError(
                f"estimated_helium_savings must be in [0, 1], "
                f"got {self.estimated_helium_savings!r}."
            )
        if self.batch_size_multiplier <= 0:
            raise MLOptimizerError(
                "batch_size_multiplier must be > 0."
            )
        if self.cache_ttl_hours <= 0:
            raise MLOptimizerError("cache_ttl_hours must be > 0.")
        if self.target_hardware not in (
            "cpu", "gpu", "gpu_mixed", "tpu", "edge",
        ):
            raise MLOptimizerError(
                f"unsupported target_hardware {self.target_hardware!r}."
            )
        if not isinstance(self.steps, tuple):
            raise MLOptimizerError("steps must be a tuple.")
        for i, step in enumerate(self.steps):
            if not isinstance(step, OptimizationStep):
                raise MLOptimizerError(
                    f"steps[{i}] must be OptimizationStep, "
                    f"got {type(step).__name__}."
                )
        if not isinstance(self.timestamp, datetime):
            raise MLOptimizerError(
                "timestamp must be a datetime."
            )
        if self.timestamp.tzinfo is None:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

    def key(self) -> Tuple[Any, ...]:
        """Deterministic identity tuple (excludes timestamp)."""
        return (
            self.mode,
            round(self.original_model_size_mb, 9),
            round(self.optimized_model_size_mb, 9),
            tuple(
                (s.kind, s.detail, s.estimated_savings)
                for s in self.steps
            ),
            round(self.estimated_helium_savings, 9),
            round(self.estimated_accuracy_impact, 9),
            self.target_hardware,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "mode": self.mode,
            "original_model_size_mb": self.original_model_size_mb,
            "optimized_model_size_mb": self.optimized_model_size_mb,
            "steps": [s.to_dict() for s in self.steps],
            "optimizations_applied": [
                f"{s.kind}_{s.detail}" for s in self.steps
            ],
            "estimated_helium_savings": self.estimated_helium_savings,
            "estimated_accuracy_impact": self.estimated_accuracy_impact,
            "target_hardware": self.target_hardware,
            "batch_size_multiplier": self.batch_size_multiplier,
            "cache_ttl_hours": self.cache_ttl_hours,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OptimizationReport":
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"OptimizationReport.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        if "mode" not in data:
            raise MLOptimizerError(
                "OptimizationReport.from_dict: missing field 'mode'."
            )
        raw_steps = data.get("steps", []) or []
        if not isinstance(raw_steps, list):
            raise MLOptimizerError("'steps' must be a list.")
        parsed_steps: List[OptimizationStep] = []
        for i, s in enumerate(raw_steps):
            try:
                parsed_steps.append(OptimizationStep.from_dict(s))
            except MLOptimizerError as exc:
                raise MLOptimizerError(
                    f"steps[{i}]: {exc}"
                ) from exc

        try:
            return cls(
                mode=_safe_str("mode", data["mode"]),
                original_model_size_mb=_safe_float(
                    "original_model_size_mb",
                    data.get("original_model_size_mb", 0.0),
                    min_value=0.0,
                ),
                optimized_model_size_mb=_safe_float(
                    "optimized_model_size_mb",
                    data.get("optimized_model_size_mb", 0.0),
                    min_value=0.0,
                ),
                steps=tuple(parsed_steps),
                estimated_helium_savings=_safe_float(
                    "estimated_helium_savings",
                    data.get("estimated_helium_savings", 0.0),
                    min_value=0.0, max_value=1.0,
                ),
                estimated_accuracy_impact=_safe_float(
                    "estimated_accuracy_impact",
                    data.get("estimated_accuracy_impact", 0.0),
                    min_value=0.0,
                ),
                target_hardware=_safe_str(
                    "target_hardware",
                    data.get("target_hardware", "gpu"),
                ),
                batch_size_multiplier=_safe_float(
                    "batch_size_multiplier",
                    data.get("batch_size_multiplier", 1.0),
                    min_value=1e-9,
                ),
                cache_ttl_hours=_safe_float(
                    "cache_ttl_hours",
                    data.get("cache_ttl_hours", 1.0),
                    min_value=1e-9,
                ),
                timestamp=_parse_timestamp(data.get("timestamp")),
            )
        except ValueError as exc:
            if isinstance(exc, MLOptimizerError):
                raise
            raise MLOptimizerError(
                f"OptimizationReport.from_dict: {exc}"
            ) from exc

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise MLOptimizerError(
                f"OptimizationReport is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "OptimizationReport":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MLOptimizerError(
                f"invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

    def __repr__(self) -> str:
        return (
            "OptimizationReport("
            f"mode={self.mode}, "
            f"steps={len(self.steps)}, "
            f"savings={self.estimated_helium_savings:.3f}, "
            f"accuracy_impact={self.estimated_accuracy_impact:.3f})"
        )


@dataclass(frozen=True)
class DataPipelinePlan:
    """Result of optimizing a data pipeline."""
    batch_size: int
    cache_ttl_seconds: int
    use_memory_mapping: bool
    compression_enabled: bool
    estimated_savings: float
    zone: str

    def __post_init__(self) -> None:
        _safe_positive_int("batch_size", self.batch_size)
        _safe_positive_int(
            "cache_ttl_seconds", self.cache_ttl_seconds,
        )
        if not isinstance(self.use_memory_mapping, bool):
            raise DataOptimizerError(
                "use_memory_mapping must be bool, "
                f"got {type(self.use_memory_mapping).__name__}."
            )
        if not isinstance(self.compression_enabled, bool):
            raise DataOptimizerError(
                "compression_enabled must be bool, "
                f"got {type(self.compression_enabled).__name__}."
            )
        _safe_float(
            "estimated_savings", self.estimated_savings,
            min_value=0.0, max_value=1.0,
        )
        if not isinstance(self.zone, str) or not self.zone:
            raise DataOptimizerError(
                "zone must be a non-empty string."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            **asdict(self),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DataPipelinePlan":
        if not isinstance(data, Mapping):
            raise DataOptimizerError(
                f"DataPipelinePlan.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        for required in (
            "batch_size", "cache_ttl_seconds", "use_memory_mapping",
            "compression_enabled", "estimated_savings", "zone",
        ):
            if required not in data:
                raise DataOptimizerError(
                    f"missing required field {required!r}."
                )
        return cls(
            batch_size=int(data["batch_size"]),
            cache_ttl_seconds=int(data["cache_ttl_seconds"]),
            use_memory_mapping=bool(data["use_memory_mapping"]),
            compression_enabled=bool(data["compression_enabled"]),
            estimated_savings=float(data["estimated_savings"]),
            zone=str(data["zone"]),
        )

    def __repr__(self) -> str:
        return (
            "DataPipelinePlan("
            f"batch={self.batch_size}, "
            f"ttl={self.cache_ttl_seconds}s, "
            f"mmap={self.use_memory_mapping}, "
            f"compression={self.compression_enabled})"
        )


# --------------------------------------------------------------------------- #
# ML optimizer
# --------------------------------------------------------------------------- #
class HeliumAwareMLOptimizer:
    """
    ML optimization engine that adjusts strategies based on helium scarcity.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        optimizer_config: Optional[MLOptimizerConfig] = None,
        strict: bool = True,
        size_fn: Optional[Callable[[Any], float]] = None,
    ) -> None:
        self._optimizer_config = optimizer_config or MLOptimizerConfig()
        self._strict = bool(strict)
        self._size_fn = size_fn or self._default_size_fn

        # Legacy flat config attribute.
        self.config: Dict[str, Any] = dict(config or {})

        # Read-only view of the strategy table.
        self._strategies: Dict[
            HeliumOptimizationMode, HeliumOptimizationStrategy,
        ] = {
            HeliumOptimizationMode.AGGRESSIVE:
                self._optimizer_config.aggressive,
            HeliumOptimizationMode.MODERATE:
                self._optimizer_config.moderate,
            HeliumOptimizationMode.LIGHT:
                self._optimizer_config.light,
            HeliumOptimizationMode.NONE:
                self._optimizer_config.none,
        }

        self._lock = threading.RLock()
        self.current_mode: HeliumOptimizationMode = (
            HeliumOptimizationMode.NONE
        )
        self._history: Deque[OptimizationReport] = deque(
            maxlen=self._optimizer_config.max_history,
        )
        self._last_report: Optional[OptimizationReport] = None
        self._last_model: Optional[Any] = None

        logger.debug(
            "HeliumAwareMLOptimizer initialized (strict=%s)",
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def optimizer_config(self) -> MLOptimizerConfig:
        return self._optimizer_config

    @property
    def strategies(
        self,
    ) -> Mapping[HeliumOptimizationMode, HeliumOptimizationStrategy]:
        return MappingProxyType(self._strategies)

    @property
    def history(self) -> List[OptimizationReport]:
        with self._lock:
            return list(self._history)

    def get_last_report(self) -> Optional[OptimizationReport]:
        """Most recent optimization report (the full dataclass)."""
        with self._lock:
            return self._last_report

    def get_last_model(self) -> Optional[Any]:
        """The model object passed to the most recent optimize_model() call."""
        with self._lock:
            return self._last_model

    # ---------------------------------------------------------- mode selection
    def set_helium_mode(self, helium_zone: Any) -> HeliumOptimizationMode:
        """
        Set the *persistent* optimization mode from a helium zone.

        Unknown zones warn and default to ``LIGHT`` (conservative). Aliases
        (``"critical"`` / ``"helium_critical"``) map to the same mode.
        """
        mode = _mode_for_zone(helium_zone)
        with self._lock:
            previous = self.current_mode
            self.current_mode = mode
        if previous != mode:
            logger.info(
                "Helium optimization mode: %s -> %s (zone=%s)",
                previous.value, mode.value,
                _zone_str(helium_zone) or "unknown",
            )
        return mode

    # ---------------------------------------------------------- model optimization
    def optimize_model(
        self,
        model: Any,
        execution_decision: Any = None,
        *,
        include_model: bool = False,
    ) -> Dict[str, Any]:
        """
        Compute the helium-aware optimization plan for ``model``.

        The caller's ``model`` object is **not** mutated. The returned dict
        is JSON-serializable by default; pass ``include_model=True`` to
        embed the original model object (which makes the payload
        non-serializable, as in v5.0). Retrieve the model via
        :meth:`get_last_model` regardless.
        """
        if model is None:
            raise MLOptimizerError("model must not be None.")

        # ---- Resolve the mode atomically under the lock ---------------
        with self._lock:
            if execution_decision is not None:
                helium_aware = bool(
                    getattr(
                        execution_decision, "helium_aware_flag", False,
                    )
                )
                helium_zone = getattr(
                    execution_decision, "helium_zone", None,
                )
                if helium_aware and helium_zone is not None:
                    mode = _mode_for_zone(helium_zone)
                    self.current_mode = mode
                else:
                    mode = self.current_mode
            else:
                mode = self.current_mode
            strategy = self._strategies[mode]
            self._last_model = model

        # ---- Compute plan -------------------------------------------
        original_size = self._get_model_size(model)
        steps: List[OptimizationStep] = []
        total_savings = 0.0
        total_accuracy_impact = 0.0

        if strategy.quantization_precision != "fp32":
            step_savings = self._optimizer_config.quantization_savings
            steps.append(OptimizationStep(
                kind="quantization",
                detail=strategy.quantization_precision,
                estimated_savings=step_savings,
                estimated_accuracy_impact=0.0,
            ))
            total_savings += step_savings

        if strategy.pruning_ratio > 0:
            step_savings = self._optimizer_config.pruning_savings
            impact = (
                strategy.pruning_ratio
                * self._optimizer_config.pruning_accuracy_coefficient
            )
            steps.append(OptimizationStep(
                kind="pruning",
                detail=f"{strategy.pruning_ratio * 100:.0f}%",
                estimated_savings=step_savings,
                estimated_accuracy_impact=impact,
            ))
            total_savings += step_savings
            total_accuracy_impact += impact

        if strategy.use_distillation:
            step_savings = self._optimizer_config.distillation_savings
            steps.append(OptimizationStep(
                kind="distillation",
                detail=f"temp={strategy.distillation_temperature}",
                estimated_savings=step_savings,
                estimated_accuracy_impact=0.0,
            ))
            total_savings += step_savings

        optimized_size = max(
            0.0, original_size * (1.0 - min(total_savings, 1.0)),
        )

        report = OptimizationReport(
            mode=mode.value,
            original_model_size_mb=original_size,
            optimized_model_size_mb=optimized_size,
            steps=tuple(steps),
            estimated_helium_savings=min(total_savings, 1.0),
            estimated_accuracy_impact=total_accuracy_impact,
            target_hardware=strategy.target_hardware,
            batch_size_multiplier=strategy.batch_size_multiplier,
            cache_ttl_hours=strategy.cache_ttl_hours,
        )

        with self._lock:
            self._history.append(report)
            self._last_report = report

        logger.debug(
            "optimize_model: mode=%s steps=%d savings=%.3f",
            mode.value, len(steps),
            report.estimated_helium_savings,
        )

        payload = report.to_dict()
        payload["original_model_size"] = report.original_model_size_mb
        payload["optimized_model_size"] = report.optimized_model_size_mb
        if include_model:
            payload["model"] = model
        return payload

    def optimize_models_batch(
        self,
        models: List[Any],
        execution_decision: Any = None,
        *,
        include_model: bool = False,
    ) -> List[Dict[str, Any]]:
        """Apply :meth:`optimize_model` to a batch of models."""
        return [
            self.optimize_model(
                m, execution_decision, include_model=include_model,
            )
            for m in models
        ]

    # ---------------------------------------------------------- size estimation
    @staticmethod
    def _default_size_fn(model: Any) -> float:
        """
        Size estimator that prefers real tensors over the wrapper.

        Tries (in order):

        1. ``model.get_memory_footprint()`` (torch)
        2. ``sum(p.numel() * p.element_size() for p in model.parameters())``
        3. ``sys.getsizeof(model)`` (fallback — logs a warning if the
           result is implausibly small for an ML model)
        """
        # 1) Torch memory footprint
        try:
            footprint = getattr(model, "get_memory_footprint", None)
            if callable(footprint):
                bytes_ = float(footprint())
                if math.isfinite(bytes_) and bytes_ >= 0:
                    return bytes_ / (1024.0 * 1024.0)
        except Exception:  # noqa: BLE001
            pass

        # 2) Torch parameter walk
        try:
            params = getattr(model, "parameters", None)
            if callable(params):
                total_bytes = 0
                for p in params():
                    if (
                        hasattr(p, "numel")
                        and hasattr(p, "element_size")
                    ):
                        total_bytes += p.numel() * p.element_size()
                if total_bytes > 0:
                    return float(total_bytes) / (1024.0 * 1024.0)
        except Exception:  # noqa: BLE001
            pass

        # 3) Fallback: sys.getsizeof, with a warning if implausibly small
        try:
            bytes_ = float(sys.getsizeof(model))
            mb = bytes_ / (1024.0 * 1024.0)
            if 0 < mb < 0.001:   # < 1 KB is almost certainly wrong
                logger.warning(
                    "_default_size_fn returned %.6f MB for %s; the "
                    "model likely wraps tensors not exposed by the "
                    "Python object. Inject a real size_fn (e.g. "
                    "sum(p.numel() * p.element_size() for p in "
                    "model.parameters())) for accurate reporting.",
                    mb, type(model).__name__,
                )
            return mb
        except Exception as exc:  # noqa: BLE001
            logger.debug("size_fn default failed: %s", exc)
            return 0.0

    def _get_model_size(self, model: Any) -> float:
        """Return model size in MB using the configured size function."""
        try:
            value = self._size_fn(model)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "size_fn raised %s; using 0.0.", exc,
            )
            return 0.0
        if value is None:
            return 0.0
        try:
            fvalue = float(value)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(fvalue) or fvalue < 0:
            return 0.0
        return fvalue

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Aggregate statistics over the optimization history."""
        with self._lock:
            history = list(self._history)
            current_mode = self.current_mode

        if not history:
            return {
                "optimizations": 0,
                "current_mode": current_mode.value,
                "by_mode": {},
                "by_step_kind": {},
                "mean_savings": None,
                "mean_accuracy_impact": None,
                "total_steps_applied": 0,
            }

        by_mode: Dict[str, int] = {}
        by_kind: Dict[str, int] = {}
        for r in history:
            by_mode[r.mode] = by_mode.get(r.mode, 0) + 1
            for s in r.steps:
                by_kind[s.kind] = by_kind.get(s.kind, 0) + 1

        return {
            "optimizations": len(history),
            "current_mode": current_mode.value,
            "by_mode": by_mode,
            "by_step_kind": by_kind,
            "mean_savings": (
                sum(r.estimated_helium_savings for r in history)
                / len(history)
            ),
            "mean_accuracy_impact": (
                sum(r.estimated_accuracy_impact for r in history)
                / len(history)
            ),
            "total_steps_applied": sum(
                len(r.steps) for r in history
            ),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset the optimizer state; optionally clear the history."""
        with self._lock:
            self.current_mode = HeliumOptimizationMode.NONE
            if clear_history:
                self._history.clear()
                self._last_report = None
                self._last_model = None
        logger.debug("HeliumAwareMLOptimizer reset.")

    # ---------------------------------------------------------- diagnostics
    def test_smoke(self) -> Dict[str, Any]:
        """Structured health check. Never raises."""
        results: Dict[str, Any] = {
            "ok": False, "steps": [],
        }
        try:
            class _M:
                size_mb = 100.0
            model = _M()
            report = self.optimize_model(model)
            results["steps"].append({
                "name": "optimize_model",
                "ok": (
                    isinstance(report, dict)
                    and "mode" in report
                    and report["original_model_size"] >= 0
                ),
            })
            results["steps"].append({
                "name": "history_bounded",
                "ok": self._history.maxlen is not None,
            })
        except Exception as exc:  # noqa: BLE001
            results["error"] = f"{type(exc).__name__}: {exc}"
        results["ok"] = (
            len(results["steps"]) > 0
            and all(s.get("ok") for s in results["steps"])
        )
        return results

    def health(self) -> Dict[str, Any]:
        """Compact health report."""
        smoke = self.test_smoke()
        return {
            "ok": smoke["ok"],
            "package_version": __version__,
            "current_mode": self.current_mode.value,
            "history_size": len(self._history),
            "statistics": self.statistics(),
            "steps": smoke.get("steps", []),
            "error": smoke.get("error"),
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "optimizer_config": self._optimizer_config.to_dict(),
                "strict": self._strict,
                "current_mode": self.current_mode.value,
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        size_fn: Optional[Callable[[Any], float]] = None,
    ) -> "HeliumAwareMLOptimizer":
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        cfg = MLOptimizerConfig.from_dict(
            dict(data.get("optimizer_config", {}) or {}),
        )
        opt = cls(
            optimizer_config=cfg,
            strict=bool(data.get("strict", True)),
            size_fn=size_fn,
        )

        try:
            mode = HeliumOptimizationMode(
                str(data.get("current_mode", "none")),
            )
        except ValueError as exc:
            raise MLOptimizerError(
                f"unknown current_mode: "
                f"{data.get('current_mode')!r}"
            ) from exc

        raw_history = data.get("history", []) or []
        if not isinstance(raw_history, list):
            raise MLOptimizerError("'history' must be a list.")

        max_h = cfg.max_history
        if len(raw_history) > max_h:
            logger.warning(
                "Restoring %d history entries into a max_history=%d "
                "buffer; keeping the most recent %d.",
                len(raw_history), max_h, max_h,
            )
            raw_history = raw_history[-max_h:]

        with opt._lock:
            opt.current_mode = mode
            for i, entry in enumerate(raw_history):
                try:
                    opt._history.append(
                        OptimizationReport.from_dict(entry),
                    )
                except MLOptimizerError as exc:
                    raise MLOptimizerError(
                        f"history[{i}]: {exc}"
                    ) from exc
        return opt

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise MLOptimizerError(
                f"optimizer is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        size_fn: Optional[Callable[[Any], float]] = None,
    ) -> "HeliumAwareMLOptimizer":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MLOptimizerError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data, size_fn=size_fn)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "HeliumAwareMLOptimizer("
                f"mode={self.current_mode.value}, "
                f"history={len(self._history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Data optimizer
# --------------------------------------------------------------------------- #
class HeliumAwareDataOptimizer:
    """
    Data optimization with helium-aware caching and batching.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        data_config: Optional[DataOptimizerConfig] = None,
        strict: bool = True,
        base_batch_size_fn: Optional[Callable[[Any], int]] = None,
    ) -> None:
        self._data_config = data_config or DataOptimizerConfig()
        self._strict = bool(strict)
        self._base_batch_size_fn = base_batch_size_fn

        # Legacy flat config attribute.
        self.config: Dict[str, Any] = dict(config or {})

        self._lock = threading.RLock()
        self.cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._evictions: int = 0
        self._current_zone: str = "normal"

        logger.debug(
            "HeliumAwareDataOptimizer initialized "
            "(strict=%s, max_cache=%d)",
            self._strict, self._data_config.max_cache_entries,
        )

    # ------------------------------------------------------------------ props
    @property
    def data_config(self) -> DataOptimizerConfig:
        return self._data_config

    @property
    def current_zone(self) -> str:
        with self._lock:
            return self._current_zone

    def set_helium_zone(self, zone: Any) -> str:
        """
        Set the persistent helium zone used when no ``execution_decision``
        is passed to :meth:`optimize_data_pipeline`.
        """
        z = _zone_str(zone) or "normal"
        with self._lock:
            self._current_zone = z
        return z

    # ---------------------------------------------------------- pipeline optimization
    def optimize_data_pipeline(
        self, dataset: Any, execution_decision: Any = None,
    ) -> Dict[str, Any]:
        """Compute the helium-aware data pipeline plan for ``dataset``."""
        cfg = self._data_config

        # ---- Resolve zone --------------------------------------------
        zone = ""
        if execution_decision is not None:
            helium_aware = bool(
                getattr(execution_decision, "helium_aware_flag", False),
            )
            if helium_aware:
                zone = _zone_str(
                    getattr(execution_decision, "helium_zone", None),
                )
        if not zone:
            with self._lock:
                zone = self._current_zone

        # ---- Map zone to policy --------------------------------------
        if zone == "helium_critical" or zone == "critical":
            batch_multiplier = cfg.critical_batch_multiplier
            cache_ttl = cfg.critical_cache_ttl_seconds
            use_mmap = True
        elif zone == "helium_red" or zone == "red":
            batch_multiplier = cfg.red_batch_multiplier
            cache_ttl = cfg.red_cache_ttl_seconds
            use_mmap = True
        elif zone == "helium_yellow" or zone == "yellow":
            batch_multiplier = cfg.yellow_batch_multiplier
            cache_ttl = cfg.yellow_cache_ttl_seconds
            use_mmap = False
        else:
            batch_multiplier = cfg.normal_batch_multiplier
            cache_ttl = cfg.normal_cache_ttl_seconds
            use_mmap = False

        base_batch = self._get_optimal_batch_size(dataset)
        plan = DataPipelinePlan(
            batch_size=max(1, int(round(base_batch * batch_multiplier))),
            cache_ttl_seconds=int(cache_ttl),
            use_memory_mapping=bool(use_mmap),
            compression_enabled=bool(batch_multiplier > 1.0),
            estimated_savings=(
                cfg.batched_savings if batch_multiplier > 1.0 else 0.0
            ),
            zone=zone or "normal",
        )

        logger.debug(
            "optimize_data_pipeline: zone=%s batch=%d ttl=%ds mmap=%s",
            plan.zone, plan.batch_size, plan.cache_ttl_seconds,
            plan.use_memory_mapping,
        )
        return plan.to_dict()

    # ---------------------------------------------------------- cache
    def cache_result(self, key: str, value: Any, ttl_seconds: int) -> None:
        """Cache a result with a TTL. Enforces LRU cap."""
        _safe_str("key", key)
        _safe_positive_int("ttl_seconds", ttl_seconds)

        now = time.time()
        with self._lock:
            self.cache[key] = {
                "value": value,
                "expires_at": now + ttl_seconds,
                "cached_at": now,
            }
            self.cache.move_to_end(key)
            while len(self.cache) > self._data_config.max_cache_entries:
                self.cache.popitem(last=False)
                self._evictions += 1

    def get_cached_result(self, key: str) -> Any:
        """Retrieve a cached value if present and not expired."""
        _safe_str("key", key)
        with self._lock:
            entry = self.cache.get(key)
            if entry is None:
                self.cache_misses += 1
                return None
            if time.time() >= entry["expires_at"]:
                self.cache.pop(key, None)
                self.cache_misses += 1
                return None
            self.cache.move_to_end(key)
            self.cache_hits += 1
            return entry["value"]

    def clear_cache(self) -> int:
        with self._lock:
            n = len(self.cache)
            self.cache.clear()
            return n

    # ---------------------------------------------------------- statistics
    def get_cache_stats(self) -> Dict[str, Any]:
        with self._lock:
            hits = self.cache_hits
            misses = self.cache_misses
            entries = len(self.cache)
            evictions = self._evictions
            now = time.time()
            expired = sum(
                1 for e in self.cache.values() if now >= e["expires_at"]
            )
        total = hits + misses
        return {
            "hits": hits,
            "misses": misses,
            "hit_rate": (hits / total) if total > 0 else 0.0,
            "entries": entries,
            "evictions": evictions,
            "expired_entries": expired,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_cache_stats()

    def reset(self, *, clear_cache: bool = True) -> None:
        with self._lock:
            if clear_cache:
                self.cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self._evictions = 0
        logger.debug("HeliumAwareDataOptimizer reset.")

    # ---------------------------------------------------------- base batch
    def _get_optimal_batch_size(self, dataset: Any) -> int:
        if self._base_batch_size_fn is not None:
            try:
                value = self._base_batch_size_fn(dataset)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "base_batch_size_fn raised %s; using default.", exc,
                )
                return self._data_config.default_base_batch_size
            # Reject bool: Python's True is an int, silently coercible.
            if isinstance(value, bool):
                logger.warning(
                    "base_batch_size_fn returned bool %r; using default.",
                    value,
                )
                return self._data_config.default_base_batch_size
            if isinstance(value, int) and value > 0:
                return value
            logger.warning(
                "base_batch_size_fn returned %r; using default.", value,
            )
        return self._data_config.default_base_batch_size

    # ---------------------------------------------------------- diagnostics
    def test_smoke(self) -> Dict[str, Any]:
        results: Dict[str, Any] = {"ok": False, "steps": []}
        try:
            plan = self.optimize_data_pipeline({"size": 1000})
            results["steps"].append({
                "name": "plan", "ok": (
                    isinstance(plan, dict)
                    and plan.get("batch_size", 0) > 0
                ),
            })
            self.cache_result("smoke", 42, 60)
            hit = self.get_cached_result("smoke")
            results["steps"].append({
                "name": "cache_round_trip", "ok": hit == 42,
            })
        except Exception as exc:  # noqa: BLE001
            results["error"] = f"{type(exc).__name__}: {exc}"
        results["ok"] = (
            len(results["steps"]) > 0
            and all(s.get("ok") for s in results["steps"])
        )
        return results

    def health(self) -> Dict[str, Any]:
        smoke = self.test_smoke()
        return {
            "ok": smoke["ok"],
            "package_version": __version__,
            "cache_stats": self.get_cache_stats(),
            "current_zone": self.current_zone,
            "steps": smoke.get("steps", []),
            "error": smoke.get("error"),
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "data_config": self._data_config.to_dict(),
                "strict": self._strict,
                "current_zone": self._current_zone,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "evictions": self._evictions,
                "cache_keys": list(self.cache.keys()),
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "HeliumAwareDataOptimizer":
        if not isinstance(data, Mapping):
            raise DataOptimizerError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        cfg = DataOptimizerConfig.from_dict(
            dict(data.get("data_config", {}) or {}),
        )
        opt = cls(
            data_config=cfg,
            strict=bool(data.get("strict", True)),
        )
        with opt._lock:
            opt._current_zone = str(
                data.get("current_zone", "normal"),
            ) or "normal"
            opt.cache_hits = int(data.get("cache_hits", 0))
            opt.cache_misses = int(data.get("cache_misses", 0))
            opt._evictions = int(data.get("evictions", 0))
        return opt

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise DataOptimizerError(
                f"data optimizer is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "HeliumAwareDataOptimizer":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise DataOptimizerError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise DataOptimizerError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "HeliumAwareDataOptimizer("
                f"cache_entries={len(self.cache)}, "
                f"hits={self.cache_hits}, "
                f"misses={self.cache_misses}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    class _Zone(Enum):
        GREEN = "helium_green"
        YELLOW = "helium_yellow"
        RED = "helium_red"
        CRITICAL = "helium_critical"

    class _Decision:
        def __init__(self, zone, helium_aware=True) -> None:
            self.helium_zone = zone
            self.helium_aware_flag = helium_aware
            self.power_budget = 1.0

    class _Model:
        def __init__(self, name: str, size_mb: float) -> None:
            self.name = name
            self.size_mb = size_mb
        def __repr__(self) -> str:
            return f"_Model({self.name}, {self.size_mb}MB)"

    def _size_fn(model: Any) -> float:
        return float(getattr(model, "size_mb", 0.0))

    # ---- Happy path across zones --------------------------------------
    optimizer = HeliumAwareMLOptimizer(size_fn=_size_fn)
    print("repr       :", optimizer)

    model = _Model("bert-base", 440.0)
    for zone in (_Zone.GREEN, _Zone.YELLOW, _Zone.RED, _Zone.CRITICAL):
        report = optimizer.optimize_model(model, _Decision(zone))
        print(
            f"  zone={zone.value:<16} mode={report['mode']:<10} "
            f"steps={report['optimizations_applied']} "
            f"savings={report['estimated_helium_savings']:.2f}"
        )

    # ---- Model is not mutated; JSON-safe payload ---------------------
    assert model.name == "bert-base"
    json.dumps(report)  # must not raise
    assert optimizer.get_last_model() is model
    assert optimizer.get_last_report() is not None
    print("model intact; payload JSON-safe")

    # ---- Zone aliases and unknown zones ------------------------------
    opt2 = HeliumAwareMLOptimizer()
    assert opt2.set_helium_mode(_Zone.RED).value == "moderate"
    assert opt2.set_helium_mode("helium_critical").value == "aggressive"
    assert opt2.set_helium_mode("critical").value == "aggressive"
    # Unknown zone → LIGHT (conservative), not NONE
    assert opt2.set_helium_mode("helium_unknown").value == "light"
    print("zone aliases + fail-closed: OK")

    # ---- Config validation -------------------------------------------
    try:
        MLOptimizerConfig(
            quantization_savings=0.6,
            pruning_savings=0.6,
            distillation_savings=0.6,
        )
    except MLOptimizerError as exc:
        print("config sum > 1 rejected:", exc)
    else:
        raise AssertionError(
            "config sum > 1 should have been rejected"
        )

    # ---- Report validation -------------------------------------------
    try:
        OptimizationReport(
            mode="aggressive",
            original_model_size_mb=100.0,
            optimized_model_size_mb=200.0,   # larger than original
            steps=(),
            estimated_helium_savings=0.5,
            estimated_accuracy_impact=0.0,
            target_hardware="gpu",
            batch_size_multiplier=1.0,
            cache_ttl_hours=1.0,
        )
    except MLOptimizerError as exc:
        print("inverted size rejected:", exc)
    else:
        raise AssertionError(
            "inverted size should have been rejected"
        )

    # ---- Statistics -------------------------------------------------
    print("stats      :", optimizer.statistics())

    # ---- Data optimizer ---------------------------------------------
    data_opt = HeliumAwareDataOptimizer()
    print("data repr  :", data_opt)

    for zone in (_Zone.GREEN, _Zone.YELLOW, _Zone.RED, _Zone.CRITICAL):
        plan = data_opt.optimize_data_pipeline(
            {"size": 1_000_000}, _Decision(zone),
        )
        print(
            f"  zone={plan['zone']:<16} batch={plan['batch_size']} "
            f"ttl={plan['cache_ttl_seconds']}s "
            f"mmap={plan['use_memory_mapping']}"
        )

    # ---- Data set_helium_zone ---------------------------------------
    data_opt.set_helium_zone(_Zone.RED)
    plan = data_opt.optimize_data_pipeline({"size": 1000})
    assert plan["zone"] == "helium_red", plan
    print("persistent zone:", plan["zone"])

    # ---- Cache: hit, miss, TTL expiry -------------------------------
    data_opt.cache_result("k1", {"answer": 42}, ttl_seconds=1)
    print("hit        :", data_opt.get_cached_result("k1"))
    print("miss       :", data_opt.get_cached_result("missing"))
    time.sleep(1.1)
    print("expired    :", data_opt.get_cached_result("k1"))
    print("cache stats:", data_opt.get_cache_stats())

    # ---- Bounded cache (LRU) ----------------------------------------
    small = HeliumAwareDataOptimizer(
        data_config=DataOptimizerConfig(max_cache_entries=3),
    )
    for i in range(10):
        small.cache_result(f"k{i}", i, ttl_seconds=60)
    assert len(small.cache) == 3
    assert small.get_cache_stats()["evictions"] == 7
    print("lru size   :", len(small.cache), "(max=3)")

    # ---- Serialization round-trip -----------------------------------
    payload = optimizer.to_json()
    restored = HeliumAwareMLOptimizer.from_json(
        payload, size_fn=_size_fn,
    )
    assert restored.to_dict() == optimizer.to_dict()
    print("ML round-trip OK.")

    data_payload = data_opt.to_json()
    restored_data = HeliumAwareDataOptimizer.from_json(data_payload)
    assert restored_data.to_dict() == data_opt.to_dict()
    print("Data round-trip OK.")

    # ---- History bound on restore ----------------------------------
    from dataclasses import replace as _replace
    reports = [report for _ in range(20)]
    fake_payload = {
        "optimizer_config": _replace(
            MLOptimizerConfig(), max_history=5,
        ).to_dict(),
        "history": [
            optimizer.optimize_model(
                model, _Decision(_Zone.RED),
            ) for _ in range(10)
        ],
        "current_mode": "moderate",
    }
    # The history entries are dicts; from_dict expects OptimizationReport dicts.
    # Reconstruct properly:
    fake_payload["history"] = [
        OptimizationReport.from_dict(r).to_dict()
        for r in fake_payload["history"]
    ]
    bounded = HeliumAwareMLOptimizer.from_dict(fake_payload)
    assert len(bounded.history) == 5, len(bounded.history)
    print("history bound on restore:", len(bounded.history))

    # ---- Validation failures ----------------------------------------
    for bad in (
        lambda: optimizer.optimize_model(None),
        lambda: data_opt.cache_result("", 1, 60),
        lambda: data_opt.cache_result("k", 1, 0),
        lambda: data_opt.get_cached_result(""),
    ):
        try:
            bad()
        except (MLOptimizerError, DataOptimizerError) as exc:
            print("Rejected   :", exc)

    for bad_cfg in (
        dict(default_base_batch_size=0),
        dict(max_cache_entries=0),
        dict(batched_savings=1.5),
    ):
        try:
            DataOptimizerConfig(**bad_cfg)  # type: ignore[arg-type]
        except DataOptimizerError as exc:
            print("Rejected cfg:", exc)

    # ---- test_smoke / health ----------------------------------------
    print("ML smoke   :", optimizer.test_smoke())
    print("Data smoke :", data_opt.test_smoke())

    print("\nSmoke test passed.")
