# src/optimization/ml_optimizer.py

"""
Helium-Aware ML Optimizer
=========================

ML and data-pipeline optimization that adapts to helium scarcity zones:

- :class:`HeliumAwareMLOptimizer` — selects quantization, pruning, distillation,
  and target hardware based on the current helium zone.
- :class:`HeliumAwareDataOptimizer` — tunes batch size, cache TTL, memory
  mapping, and compression based on the current helium zone.

Enhancements
------------
- ``MLOptimizerConfig`` / ``DataOptimizerConfig`` — frozen, validated; every
  previously hardcoded coefficient is now configurable.
- ``HeliumOptimizationStrategy`` / ``OptimizationStep`` — frozen dataclasses
  describing what was applied and the estimated savings.
- **Thread safety** — ``RLock`` guards mode, cache, and hit/miss counters.
- **Bounded LRU cache with enforced TTL** — eviction on both write and read.
- **Robust zone extraction** — accepts ``HeliumZone`` enums, plain strings,
  and objects with ``.value``.
- **Non-destructive optimization** — the caller's model object is never
  mutated; the returned report describes the transformations to apply.
- Serialization on both classes and on every dataclass.
- ``statistics()``, ``__repr__``, custom ``*Error(ValueError)``,
  lazy ``%s`` logging, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import sys
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


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
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HeliumOptimizationStrategy:
    """Per-mode optimization strategy (was a nested dict in the original)."""
    quantization_precision: str          # "fp32" | "fp16" | "int8" | "int4"
    pruning_ratio: float                 # 0.0 – 1.0
    use_distillation: bool
    distillation_temperature: float
    target_hardware: str                 # "cpu" | "gpu" | "gpu_mixed"
    batch_size_multiplier: float
    cache_ttl_hours: float

    def __post_init__(self) -> None:
        if self.quantization_precision not in ("fp32", "fp16", "int8", "int4"):
            raise MLOptimizerError(
                f"unsupported quantization_precision "
                f"{self.quantization_precision!r}."
            )
        if not 0.0 <= self.pruning_ratio <= 1.0:
            raise MLOptimizerError("pruning_ratio must be in [0, 1].")
        if self.use_distillation and self.distillation_temperature <= 0:
            raise MLOptimizerError(
                "distillation_temperature must be > 0 when distillation is on."
            )
        if self.target_hardware not in ("cpu", "gpu", "gpu_mixed", "tpu", "edge"):
            raise MLOptimizerError(
                f"unsupported target_hardware {self.target_hardware!r}."
            )
        if self.batch_size_multiplier <= 0:
            raise MLOptimizerError("batch_size_multiplier must be > 0.")
        if self.cache_ttl_hours <= 0:
            raise MLOptimizerError("cache_ttl_hours must be > 0.")


@dataclass(frozen=True)
class MLOptimizerConfig:
    """Tunable parameters for the helium-aware ML optimizer."""

    # Per-mode strategies (mode -> strategy).
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

    # Estimated helium savings contributed by each optimization kind.
    quantization_savings: float = 0.30
    pruning_savings: float = 0.20
    distillation_savings: float = 0.25

    # Accuracy impact coefficients.
    pruning_accuracy_coefficient: float = 0.05

    # Bounded history of optimization reports.
    max_history: int = 1000

    def strategy_for(self, mode: HeliumOptimizationMode) -> HeliumOptimizationStrategy:
        return {
            HeliumOptimizationMode.AGGRESSIVE: self.aggressive,
            HeliumOptimizationMode.MODERATE: self.moderate,
            HeliumOptimizationMode.LIGHT: self.light,
            HeliumOptimizationMode.NONE: self.none,
        }[mode]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "aggressive": asdict(self.aggressive),
            "moderate": asdict(self.moderate),
            "light": asdict(self.light),
            "none": asdict(self.none),
            "quantization_savings": self.quantization_savings,
            "pruning_savings": self.pruning_savings,
            "distillation_savings": self.distillation_savings,
            "pruning_accuracy_coefficient": self.pruning_accuracy_coefficient,
            "max_history": self.max_history,
        }


@dataclass(frozen=True)
class DataOptimizerConfig:
    """Tunable parameters for the helium-aware data optimizer."""

    # Per-zone batch multipliers (zone string -> multiplier).
    critical_batch_multiplier: float = 2.5
    red_batch_multiplier: float = 2.0
    yellow_batch_multiplier: float = 1.5
    normal_batch_multiplier: float = 1.0

    # Per-zone cache TTLs (seconds).
    critical_cache_ttl_seconds: int = 3600 * 72
    red_cache_ttl_seconds: int = 3600 * 48
    yellow_cache_ttl_seconds: int = 3600 * 24
    normal_cache_ttl_seconds: int = 3600 * 6

    # Default base batch size when no estimator is supplied.
    default_base_batch_size: int = 32

    # LRU cap for the response cache.
    max_cache_entries: int = 10_000

    # Estimated savings fraction when batching is increased.
    batched_savings: float = 0.20

    def __post_init__(self) -> None:
        for name in (
            "critical_batch_multiplier", "red_batch_multiplier",
            "yellow_batch_multiplier", "normal_batch_multiplier",
        ):
            if getattr(self, name) <= 0:
                raise DataOptimizerError(f"{name} must be > 0.")
        for name in (
            "critical_cache_ttl_seconds", "red_cache_ttl_seconds",
            "yellow_cache_ttl_seconds", "normal_cache_ttl_seconds",
        ):
            if not isinstance(getattr(self, name), int) or getattr(self, name) <= 0:
                raise DataOptimizerError(f"{name} must be a positive int.")
        if self.default_base_batch_size <= 0:
            raise DataOptimizerError("default_base_batch_size must be > 0.")
        if self.max_cache_entries <= 0:
            raise DataOptimizerError("max_cache_entries must be > 0.")
        if not 0.0 <= self.batched_savings <= 1.0:
            raise DataOptimizerError("batched_savings must be in [0, 1].")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Shared helper — safe zone extraction
# --------------------------------------------------------------------------- #
def _zone_str(value: Any) -> str:
    """Return a lowercase zone string from an Enum, string, or object."""
    if value is None:
        return ""
    if hasattr(value, "value"):
        value = value.value
    return str(value).lower()


# --------------------------------------------------------------------------- #
# Result records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class OptimizationStep:
    """One optimization applied (or planned) on the model."""
    kind: str                # "quantization" | "pruning" | "distillation"
    detail: str              # e.g. "int8", "30%", "temp=1.5"
    estimated_savings: float
    estimated_accuracy_impact: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


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
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "original_model_size_mb": self.original_model_size_mb,
            "optimized_model_size_mb": self.optimized_model_size_mb,
            "steps": [s.to_dict() for s in self.steps],
            "optimizations_applied": [f"{s.kind}_{s.detail}" for s in self.steps],
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
            raise MLOptimizerError("OptimizationReport.from_dict expects a Mapping.")
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
            mode=str(data["mode"]),
            original_model_size_mb=float(data.get("original_model_size_mb", 0.0)),
            optimized_model_size_mb=float(data.get("optimized_model_size_mb", 0.0)),
            steps=tuple(
                OptimizationStep(
                    kind=str(s["kind"]),
                    detail=str(s["detail"]),
                    estimated_savings=float(s.get("estimated_savings", 0.0)),
                    estimated_accuracy_impact=float(
                        s.get("estimated_accuracy_impact", 0.0)
                    ),
                )
                for s in data.get("steps", [])
            ),
            estimated_helium_savings=float(
                data.get("estimated_helium_savings", 0.0)
            ),
            estimated_accuracy_impact=float(
                data.get("estimated_accuracy_impact", 0.0)
            ),
            target_hardware=str(data.get("target_hardware", "gpu")),
            batch_size_multiplier=float(data.get("batch_size_multiplier", 1.0)),
            cache_ttl_hours=float(data.get("cache_ttl_hours", 0.0)),
            timestamp=timestamp,
        )

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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __repr__(self) -> str:
        return (
            "DataPipelinePlan("
            f"batch={self.batch_size}, ttl={self.cache_ttl_seconds}s, "
            f"mmap={self.use_memory_mapping}, "
            f"compression={self.compression_enabled})"
        )


# --------------------------------------------------------------------------- #
# ML optimizer
# --------------------------------------------------------------------------- #
class HeliumAwareMLOptimizer:
    """
    ML optimization engine that adjusts strategies based on helium scarcity.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``set_helium_mode``, ``optimize_model``) is preserved; new parameters are
    keyword-only.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        optimizer_config: Optional[MLOptimizerConfig] = None,
        strict: bool = True,
        size_fn: Optional[Callable[[Any], float]] = None,
    ) -> None:
        """
        Parameters
        ----------
        config : dict, optional
            Legacy flat config (kept for backward compatibility).
        optimizer_config : MLOptimizerConfig, optional
            Typed configuration. Takes precedence over ``config``.
        strict : bool, default True
            If True, invalid inputs raise :class:`MLOptimizerError`.
        size_fn : callable, optional
            ``model -> size_mb`` callable. Defaults to ``sys.getsizeof``
            (matching the original behavior) but callers should inject a
            real size estimator (e.g. ``sum(p.numel() * p.element_size())``).
        """
        self._optimizer_config = optimizer_config or MLOptimizerConfig()
        self._strict = bool(strict)
        self._size_fn = size_fn or self._default_size_fn

        # Legacy attribute preserved for backward compatibility.
        self.config: Dict[str, Any] = dict(config or {})

        # Strategy table (immutable snapshots).
        self.strategies: Dict[HeliumOptimizationMode, HeliumOptimizationStrategy] = {
            HeliumOptimizationMode.AGGRESSIVE: self._optimizer_config.aggressive,
            HeliumOptimizationMode.MODERATE:   self._optimizer_config.moderate,
            HeliumOptimizationMode.LIGHT:      self._optimizer_config.light,
            HeliumOptimizationMode.NONE:       self._optimizer_config.none,
        }

        self._lock = threading.RLock()
        self.current_mode: HeliumOptimizationMode = HeliumOptimizationMode.NONE
        self._history: List[OptimizationReport] = []

        logger.debug(
            "HeliumAwareMLOptimizer initialized (strict=%s)", self._strict
        )

    # ------------------------------------------------------------------ props
    @property
    def optimizer_config(self) -> MLOptimizerConfig:
        return self._optimizer_config

    @property
    def history(self) -> List[OptimizationReport]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- mode selection
    def set_helium_mode(self, helium_zone: Any) -> HeliumOptimizationMode:
        """
        Set the optimization mode based on the helium zone.

        Accepts a :class:`HeliumZone`-like enum, a plain string (``"helium_red"``),
        or an object exposing ``.value``.
        """
        zone = _zone_str(helium_zone)

        if zone == "helium_critical":
            mode = HeliumOptimizationMode.AGGRESSIVE
        elif zone == "helium_red":
            mode = HeliumOptimizationMode.MODERATE
        elif zone == "helium_yellow":
            mode = HeliumOptimizationMode.LIGHT
        else:
            mode = HeliumOptimizationMode.NONE

        with self._lock:
            previous = self.current_mode
            self.current_mode = mode

        if previous != mode:
            logger.info(
                "Helium optimization mode: %s -> %s (zone=%s)",
                previous.value, mode.value, zone or "unknown",
            )
        return mode

    # ---------------------------------------------------------- model optimization
    def optimize_model(
        self, model: Any, execution_decision: Any = None
    ) -> Dict[str, Any]:
        """
        Compute the helium-aware optimization plan for ``model``.

        The caller's ``model`` object is **not mutated**; the returned dict
        describes the transformations to apply. Concrete transformations are
        performed by the DPQ stack (``dpq.ModelConverter``) or by the caller.

        Returns
        -------
        dict
            Keys: ``mode``, ``original_model_size``, ``optimized_model_size``,
            ``optimizations_applied``, ``steps``, ``estimated_helium_savings``,
            ``estimated_accuracy_impact``, ``target_hardware``,
            ``batch_size_multiplier``, ``cache_ttl_hours``, ``model``
            (the original object, unchanged).
        """
        if model is None:
            raise MLOptimizerError("model must not be None.")

        # ---- Determine mode ------------------------------------------
        if execution_decision is not None:
            helium_aware = bool(
                getattr(execution_decision, "helium_aware_flag", False)
            )
            helium_zone = getattr(execution_decision, "helium_zone", None)
            if helium_aware and helium_zone is not None:
                self.set_helium_mode(helium_zone)

        with self._lock:
            mode = self.current_mode
            strategy = self.strategies[mode]

        # ---- Compute plan -------------------------------------------
        original_size = self._get_model_size(model)
        steps: List[OptimizationStep] = []
        total_savings = 0.0
        total_accuracy_impact = 0.0

        if strategy.quantization_precision != "fp32":
            steps.append(OptimizationStep(
                kind="quantization",
                detail=strategy.quantization_precision,
                estimated_savings=self._optimizer_config.quantization_savings,
                estimated_accuracy_impact=0.0,
            ))
            total_savings += self._optimizer_config.quantization_savings

        if strategy.pruning_ratio > 0:
            steps.append(OptimizationStep(
                kind="pruning",
                detail=f"{strategy.pruning_ratio * 100:.0f}%",
                estimated_savings=self._optimizer_config.pruning_savings,
                estimated_accuracy_impact=(
                    strategy.pruning_ratio
                    * self._optimizer_config.pruning_accuracy_coefficient
                ),
            ))
            total_savings += self._optimizer_config.pruning_savings
            total_accuracy_impact += (
                strategy.pruning_ratio
                * self._optimizer_config.pruning_accuracy_coefficient
            )

        if strategy.use_distillation:
            steps.append(OptimizationStep(
                kind="distillation",
                detail=f"temp={strategy.distillation_temperature}",
                estimated_savings=self._optimizer_config.distillation_savings,
                estimated_accuracy_impact=0.0,
            ))
            total_savings += self._optimizer_config.distillation_savings

        # Estimated post-optimization size: original * (1 - total_savings), floored at 0.
        optimized_size = max(0.0, original_size * (1.0 - min(total_savings, 1.0)))

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
            if len(self._history) > self._optimizer_config.max_history:
                del self._history[0]

        logger.info(
            "optimize_model: mode=%s steps=%d helium_savings=%.3f "
            "accuracy_impact=%.3f",
            mode.value, len(steps), report.estimated_helium_savings,
            report.estimated_accuracy_impact,
        )

        # Return a superset of the original dict shape.
        payload = report.to_dict()
        payload["original_model_size"] = report.original_model_size_mb
        payload["optimized_model_size"] = report.optimized_model_size_mb
        payload["model"] = model  # caller's object, unchanged
        return payload

    # ---------------------------------------------------------- size estimation
    @staticmethod
    def _default_size_fn(model: Any) -> float:
        """
        Default size estimator — matches the original ``sys.getsizeof`` behavior.

        Callers with real tensors should inject ``size_fn`` that returns bytes
        across the parameter set (e.g. ``sum(p.numel() * p.element_size())``).
        """
        try:
            return float(sys.getsizeof(model)) / (1024.0 * 1024.0)
        except Exception as exc:
            logger.debug("size_fn default failed: %s", exc)
            return 0.0

    def _get_model_size(self, model: Any) -> float:
        """Return model size in MB using the configured size function."""
        try:
            value = self._size_fn(model)
        except Exception as exc:
            logger.warning("size_fn raised %s; using 0.0.", exc)
            return 0.0
        if value is None:
            return 0.0
        try:
            fvalue = float(value)
        except (TypeError, ValueError):
            return 0.0
        if math.isnan(fvalue) or math.isinf(fvalue) or fvalue < 0:
            return 0.0
        return fvalue

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the optimization history."""
        with self._lock:
            history = list(self._history)
            current_mode = self.current_mode

        if not history:
            return {
                "optimizations": 0,
                "current_mode": current_mode.value,
                "by_mode": {},
                "mean_savings": None,
                "mean_accuracy_impact": None,
            }

        by_mode: Dict[str, int] = {}
        for r in history:
            by_mode[r.mode] = by_mode.get(r.mode, 0) + 1

        return {
            "optimizations": len(history),
            "current_mode": current_mode.value,
            "by_mode": by_mode,
            "mean_savings": sum(r.estimated_helium_savings for r in history)
            / len(history),
            "mean_accuracy_impact": sum(
                r.estimated_accuracy_impact for r in history
            ) / len(history),
            "total_steps_applied": sum(len(r.steps) for r in history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset the optimizer state; optionally clear the history."""
        with self._lock:
            self.current_mode = HeliumOptimizationMode.NONE
            if clear_history:
                self._history.clear()
        logger.debug("HeliumAwareMLOptimizer reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "optimizer_config": self._optimizer_config.to_dict(),
                "strict": self._strict,
                "current_mode": self.current_mode.value,
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, size_fn: Optional[Callable[[Any], float]] = None
    ) -> "HeliumAwareMLOptimizer":
        if not isinstance(data, Mapping):
            raise MLOptimizerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("optimizer_config", {}) or {})
        cfg = MLOptimizerConfig(
            aggressive=HeliumOptimizationStrategy(**cfg_data["aggressive"]),
            moderate=HeliumOptimizationStrategy(**cfg_data["moderate"]),
            light=HeliumOptimizationStrategy(**cfg_data["light"]),
            none=HeliumOptimizationStrategy(**cfg_data["none"]),
            quantization_savings=float(cfg_data.get("quantization_savings", 0.30)),
            pruning_savings=float(cfg_data.get("pruning_savings", 0.20)),
            distillation_savings=float(cfg_data.get("distillation_savings", 0.25)),
            pruning_accuracy_coefficient=float(
                cfg_data.get("pruning_accuracy_coefficient", 0.05)
            ),
            max_history=int(cfg_data.get("max_history", 1000)),
        )
        opt = cls(optimizer_config=cfg, strict=bool(data.get("strict", True)),
                  size_fn=size_fn)
        with opt._lock:
            opt.current_mode = HeliumOptimizationMode(
                str(data.get("current_mode", "none"))
            )
            for entry in data.get("history", []):
                opt._history.append(OptimizationReport.from_dict(entry))
        return opt

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, size_fn: Optional[Callable[[Any], float]] = None
    ) -> "HeliumAwareMLOptimizer":
        try:
            return cls.from_dict(json.loads(payload), size_fn=size_fn)
        except json.JSONDecodeError as exc:
            raise MLOptimizerError(f"Invalid JSON payload: {exc}") from exc

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

    Thread-safe, serializable, and bounded in memory. The original public API
    (``optimize_data_pipeline``, ``cache_result``, ``get_cached_result``,
    ``get_cache_stats``) is preserved; new parameters are keyword-only.
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

        # Legacy attribute preserved.
        self.config: Dict[str, Any] = dict(config or {})

        self._lock = threading.RLock()
        # LRU via OrderedDict; bounded by ``max_cache_entries``.
        self.cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._evictions: int = 0

        logger.debug(
            "HeliumAwareDataOptimizer initialized (strict=%s, max_cache=%d)",
            self._strict,
            self._data_config.max_cache_entries,
        )

    # ------------------------------------------------------------------ props
    @property
    def data_config(self) -> DataOptimizerConfig:
        return self._data_config

    # ---------------------------------------------------------- pipeline optimization
    def optimize_data_pipeline(
        self, dataset: Any, execution_decision: Any = None
    ) -> Dict[str, Any]:
        """
        Compute the helium-aware data pipeline plan for ``dataset``.

        Returns a dict with the same keys as the original (``batch_size``,
        ``cache_ttl_seconds``, ``use_memory_mapping``, ``compression_enabled``,
        ``estimated_savings``) plus a ``zone`` field identifying which
        branch was taken.
        """
        cfg = self._data_config

        # ---- Resolve zone --------------------------------------------
        zone = ""
        if execution_decision is not None:
            helium_aware = bool(
                getattr(execution_decision, "helium_aware_flag", False)
            )
            if helium_aware:
                zone = _zone_str(getattr(execution_decision, "helium_zone", None))

        # ---- Map zone to policy --------------------------------------
        if zone == "helium_critical":
            batch_multiplier = cfg.critical_batch_multiplier
            cache_ttl = cfg.critical_cache_ttl_seconds
            use_mmap = True
        elif zone == "helium_red":
            batch_multiplier = cfg.red_batch_multiplier
            cache_ttl = cfg.red_cache_ttl_seconds
            use_mmap = True
        elif zone == "helium_yellow":
            batch_multiplier = cfg.yellow_batch_multiplier
            cache_ttl = cfg.yellow_cache_ttl_seconds
            use_mmap = False
        else:
            batch_multiplier = cfg.normal_batch_multiplier
            cache_ttl = cfg.normal_cache_ttl_seconds
            use_mmap = False

        # ---- Compute plan --------------------------------------------
        base_batch = self._get_optimal_batch_size(dataset)
        plan = DataPipelinePlan(
            batch_size=int(max(1, round(base_batch * batch_multiplier))),
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
        """
        Cache a result with a TTL.

        Enforces the LRU cap; expired entries are evicted on read (not on
        write, so callers can still read a just-expired value if they check
        immediately).
        """
        if not isinstance(key, str) or not key:
            raise DataOptimizerError("key must be a non-empty string.")
        if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
            raise DataOptimizerError("ttl_seconds must be a positive int.")

        with self._lock:
            self.cache[key] = {
                "value": value,
                "expires_at": time.time() + ttl_seconds,
                "cached_at": time.time(),
            }
            # Move to end (most recently used).
            self.cache.move_to_end(key)
            # LRU eviction.
            while len(self.cache) > self._data_config.max_cache_entries:
                self.cache.popitem(last=False)
                self._evictions += 1

    def get_cached_result(self, key: str) -> Any:
        """
        Retrieve a cached value if present **and** not expired.

        Expired entries are removed from the cache; the hit/miss counters
        reflect the effective outcome.
        """
        if not isinstance(key, str) or not key:
            raise DataOptimizerError("key must be a non-empty string.")

        with self._lock:
            entry = self.cache.get(key)
            if entry is None:
                self.cache_misses += 1
                return None
            if time.time() >= entry["expires_at"]:
                self.cache.pop(key, None)
                self.cache_misses += 1
                return None
            # Refresh LRU position.
            self.cache.move_to_end(key)
            self.cache_hits += 1
            return entry["value"]

    def clear_cache(self) -> int:
        """Remove every cache entry. Returns the number removed."""
        with self._lock:
            n = len(self.cache)
            self.cache.clear()
            return n

    # ---------------------------------------------------------- statistics
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Return cache hit/miss statistics.

        Backward-compatible keys (``hits``, ``misses``, ``hit_rate``) plus
        additive fields (``entries``, ``evictions``, ``expired_entries``).
        """
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
        """Alias for :meth:`get_cache_stats` (consistent with other modules)."""
        return self.get_cache_stats()

    def reset(self, *, clear_cache: bool = True) -> None:
        """Reset counters; optionally clear the cache."""
        with self._lock:
            if clear_cache:
                self.cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self._evictions = 0
        logger.debug("HeliumAwareDataOptimizer reset.")

    # ---------------------------------------------------------- base batch
    def _get_optimal_batch_size(self, dataset: Any) -> int:
        """Return the base batch size for ``dataset`` (configurable)."""
        if self._base_batch_size_fn is not None:
            try:
                value = self._base_batch_size_fn(dataset)
            except Exception as exc:
                logger.warning("base_batch_size_fn raised %s; using default.", exc)
                return self._data_config.default_base_batch_size
            if isinstance(value, int) and value > 0:
                return value
            logger.warning(
                "base_batch_size_fn returned %r; using default.", value
            )
        return self._data_config.default_base_batch_size

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "data_config": self._data_config.to_dict(),
                "strict": self._strict,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "evictions": self._evictions,
                "cache_keys": list(self.cache.keys()),
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HeliumAwareDataOptimizer":
        if not isinstance(data, Mapping):
            raise DataOptimizerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("data_config", {}) or {})
        cfg = DataOptimizerConfig(
            critical_batch_multiplier=float(
                cfg_data.get("critical_batch_multiplier", 2.5)
            ),
            red_batch_multiplier=float(cfg_data.get("red_batch_multiplier", 2.0)),
            yellow_batch_multiplier=float(
                cfg_data.get("yellow_batch_multiplier", 1.5)
            ),
            normal_batch_multiplier=float(
                cfg_data.get("normal_batch_multiplier", 1.0)
            ),
            critical_cache_ttl_seconds=int(
                cfg_data.get("critical_cache_ttl_seconds", 3600 * 72)
            ),
            red_cache_ttl_seconds=int(
                cfg_data.get("red_cache_ttl_seconds", 3600 * 48)
            ),
            yellow_cache_ttl_seconds=int(
                cfg_data.get("yellow_cache_ttl_seconds", 3600 * 24)
            ),
            normal_cache_ttl_seconds=int(
                cfg_data.get("normal_cache_ttl_seconds", 3600 * 6)
            ),
            default_base_batch_size=int(
                cfg_data.get("default_base_batch_size", 32)
            ),
            max_cache_entries=int(cfg_data.get("max_cache_entries", 10_000)),
            batched_savings=float(cfg_data.get("batched_savings", 0.20)),
        )
        opt = cls(data_config=cfg, strict=bool(data.get("strict", True)))
        with opt._lock:
            opt.cache_hits = int(data.get("cache_hits", 0))
            opt.cache_misses = int(data.get("cache_misses", 0))
            opt._evictions = int(data.get("evictions", 0))
            # We intentionally do not restore cache values (they may be stale
            # or non-JSON-serializable).
        return opt

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "HeliumAwareDataOptimizer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise DataOptimizerError(f"Invalid JSON payload: {exc}") from exc

    def __repr__(self) -> str:
        with self._lock:
            return (
                "HeliumAwareDataOptimizer("
                f"cache_entries={len(self.cache)}, "
                f"hits={self.cache_hits}, misses={self.cache_misses}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "HeliumOptimizationMode",
    "HeliumOptimizationStrategy",
    "MLOptimizerConfig",
    "DataOptimizerConfig",
    "OptimizationStep",
    "OptimizationReport",
    "DataPipelinePlan",
    "HeliumAwareMLOptimizer",
    "HeliumAwareDataOptimizer",
    "MLOptimizerError",
    "DataOptimizerError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m optimization.ml_optimizer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Mock execution decisions covering every zone ---------------- #
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

    # A realistic size estimator.
    def _size_fn(model: Any) -> float:
        return float(getattr(model, "size_mb", 0.0))

    # ---- Happy path across zones -------------------------------------- #
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

    # ---- Model is not mutated ----------------------------------------- #
    assert model.name == "bert-base"
    print("model intact:", model)

    # ---- set_helium_mode with Enum, string, and object ---------------- #
    opt2 = HeliumAwareMLOptimizer()
    print("enum zone  :", opt2.set_helium_mode(_Zone.RED).value)
    print("string zone:", opt2.set_helium_mode("helium_critical").value)
    print("bogus zone :", opt2.set_helium_mode("helium_unknown").value)

    # ---- Statistics --------------------------------------------------- #
    print("stats      :", optimizer.statistics())

    # ---- Data optimizer ----------------------------------------------- #
    data_opt = HeliumAwareDataOptimizer()
    print("data repr  :", data_opt)

    for zone in (_Zone.GREEN, _Zone.YELLOW, _Zone.RED, _Zone.CRITICAL):
        plan = data_opt.optimize_data_pipeline(
            {"size": 1_000_000}, _Decision(zone)
        )
        print(
            f"  zone={plan['zone']:<16} batch={plan['batch_size']} "
            f"ttl={plan['cache_ttl_seconds']}s "
            f"mmap={plan['use_memory_mapping']}"
        )

    # ---- Cache: hit, miss, TTL expiry -------------------------------- #
    data_opt.cache_result("k1", {"answer": 42}, ttl_seconds=1)
    print("hit        :", data_opt.get_cached_result("k1"))
    print("miss       :", data_opt.get_cached_result("missing"))
    time.sleep(1.1)
    print("expired    :", data_opt.get_cached_result("k1"))
    print("cache stats:", data_opt.get_cache_stats())

    # ---- Bounded cache (LRU) ------------------------------------------ #
    small = HeliumAwareDataOptimizer(
        data_config=DataOptimizerConfig(max_cache_entries=3)
    )
    for i in range(10):
        small.cache_result(f"k{i}", i, ttl_seconds=60)
    print("lru size   :", len(small.cache), "(max=3)")
    print("lru keys   :", list(small.cache.keys()))
    print("evictions  :", small.get_cache_stats()["evictions"])

    # ---- Serialization round-trip ------------------------------------- #
    payload = optimizer.to_json()
    restored = HeliumAwareMLOptimizer.from_json(payload, size_fn=_size_fn)
    assert restored.to_dict() == optimizer.to_dict()
    print("ML round-trip OK.")

    data_payload = data_opt.to_json()
    restored_data = HeliumAwareDataOptimizer.from_json(data_payload)
    assert restored_data.to_dict() == data_opt.to_dict()
    print("Data round-trip OK.")

    # ---- Validation failures ------------------------------------------ #
    for bad in (
        lambda: HeliumAwareMLOptimizer(size_fn=_size_fn).optimize_model(None),
        lambda: HeliumAwareDataOptimizer().cache_result("", 1, 60),
        lambda: HeliumAwareDataOptimizer().cache_result("k", 1, 0),
        lambda: HeliumAwareDataOptimizer().get_cached_result(""),
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

    print("\nSmoke test passed.")
