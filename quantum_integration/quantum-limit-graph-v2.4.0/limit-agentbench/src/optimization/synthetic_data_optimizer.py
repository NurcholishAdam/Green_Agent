# src/optimization/synthetic_data_optimizer.py

"""
Synthetic Data Optimization Layer (v5.1)
========================================

Optimizes training through intelligent data augmentation and compression.
Goal: Reduce compute 80-95% through data-centric optimization.

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``optimize()`` in non-strict mode now filters non-Mapping samples
         before they reach ``_run_pipeline``. Previously the strict-mode
         path raised but the non-strict path logged "Skipping." and then
         handed the bad samples to ``_sample_id``, which crashed with
         ``AttributeError: 'int' object has no attribute 'get'``.
  * P0 — ``compression_ratio`` and ``optimized_size`` agree again.
         ``compression_ratio`` is now computed as
         ``original_size / optimized_size`` using the **post-synthetic**
         count (matching the ``optimized_size`` field). Previously it used
         the pre-synthetic count, so any caller who computed
         ``original_size / result.optimized_size`` saw a different value.
  * P0 — ``DataOptimizationResult.__post_init__`` uses ``math.isfinite``
         on every numeric field. Previously ``compression_ratio``,
         ``estimated_energy_savings_kwh``, and
         ``estimated_carbon_savings_kgco2e`` accepted ``NaN`` and ``Inf``
         because the guards used ``< 0``.
  * P0 — ``SyntheticOptimizerConfig.from_dict`` added; both
         ``SyntheticOptimizerConfig`` and ``DataOptimizationResult``
         rebuild from ``dataclasses.fields`` (previously
         ``SyntheticDataOptimizer.from_dict`` hardcoded config fields and
         silently dropped the three length normalizers). ``from_dict``
         routes every value through ``_safe_*`` helpers so ``KeyError`` /
         ``TypeError`` / ``ValueError`` no longer escape.
  * P0 — numpy import is guarded at module scope. A missing numpy raises
         ``SyntheticDataOptimizerError`` from the constructor rather than
         a raw ``ImportError``.
  * P1 — ``min_quality_threshold`` and the other legacy mirror
         attributes are now properties backed by ``self._config``, so
         mutation is preserved *and* the pipeline reads the same value
         (previously ``_quality_filter`` read the mutable legacy attribute
         while the config was the source of truth).
  * P1 — ``SampleScore.__post_init__`` rejects non-numeric / ``NaN`` /
         non-finite values via ``_safe_*`` helpers. Previously a string
         score leaked ``TypeError`` and ``True`` coerced to ``1.0``.
  * P1 — ``_calculate_diversity`` reuses pre-computed SHA-256 hashes from
         ``_score_samples``. Previously every call re-hashed every peer
         (O(N × sample_size × text_length) per pipeline run). The same
         cached hashes feed ``_deduplicate`` so no text is hashed twice.
  * P1 — History is bounded on restore with a warning (previously silent
         truncation).
  * P1 — ``to_json`` no longer passes ``default=str``; serialization
         errors wrap in ``SyntheticDataOptimizerError``.
  * P1 — ``seed=None`` is documented as explicitly non-deterministic
         (matching the ``random.Random(None)`` behaviour).
  * P1 — Synthetic samples inherit their source's difficulty score so
         ``_curriculum_order`` places them near samples of comparable
         difficulty (previously all synthetics were appended after the
         hardest real sample).
  * P1 — ``optimize()`` and ``optimize_dataset()`` share parameter
         validation via ``_validate_run_params``. Previously
         ``optimize_dataset(ds, target_compression=2.0)`` silently
         over-sized the target.
  * P2 — New helpers: ``__version__``, ``schema_version`` in every
         ``to_dict()``, ``DataOptimizationResult.key()``,
         ``DataOptimizationResult.to_json()`` / ``from_json()``,
         ``test_smoke()``, ``health()``, ``optimize_batch()``, and
         per-strategy statistics in ``get_statistics()``.
  * P3 — Dead ``self._np`` assignment removed; ``__repr__`` uses ``!r``;
         redundant ``if text else 0.0`` removed.

Backward compatibility
----------------------
- ``SyntheticDataOptimizer(config=None, *, strict=True)`` signature
  unchanged.
- ``optimize(dataset, target_compression=0.5, enable_synthetic=True,
  synthetic_ratio=0.3, baseline_energy_kwh=1.0)`` signature unchanged.
- ``optimize_dataset(...)`` unchanged.
- ``get_statistics()`` / ``statistics()`` unchanged.
- ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json`` unchanged
  except that ``to_dict()`` now embeds ``schema_version``.
- **Breaking (documented):** ``compression_ratio`` now reflects the
  net post-synthetic size. Callers relying on the pre-synthetic value
  should compute ``original_size / active_learning_size`` themselves, or
  read ``result.estimated_energy_savings_kwh`` (which is unaffected).
- **Breaking (documented):** ``DataOptimizationResult`` and
  ``SampleScore`` now reject ``NaN``/``Inf`` inputs that were previously
  accepted.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import random
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "CompressionStrategy",
    "DataOptimizationResult",
    "SampleScore",
    "SyntheticDataOptimizer",
    "SyntheticDataOptimizerError",
    "SyntheticOptimizerConfig",
    "SyntheticStrategy",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Optional numpy (hard requirement at construction, soft at import)
# --------------------------------------------------------------------------- #
try:
    import numpy as _np
    _HAS_NUMPY = True
except ImportError:  # pragma: no cover
    _np = None  # type: ignore[assignment]
    _HAS_NUMPY = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SyntheticDataOptimizerError(ValueError):
    """Raised for invalid optimizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class CompressionStrategy(Enum):
    """Data compression strategies."""
    ACTIVE_LEARNING = "active_learning"
    DEDUPLICATION = "deduplication"
    QUALITY_FILTERING = "quality_filtering"
    CURRICULUM_LEARNING = "curriculum_learning"
    CORE_SET_SELECTION = "coreset"


class SyntheticStrategy(Enum):
    """Synthetic data generation strategies."""
    GPT4_GENERATION = "gpt4_generation"
    PARAPHRASE = "paraphrase"
    BACK_TRANSLATION = "back_translation"
    MIXUP = "mixup"
    CUTMIX = "cutmix"


# --------------------------------------------------------------------------- #
# Safe coercion helpers
# --------------------------------------------------------------------------- #
def _safe_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise SyntheticDataOptimizerError(
            f"{name} must be a non-empty string, "
            f"got {type(value).__name__}"
        )
    return value


def _safe_float(
    name: str,
    value: Any,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SyntheticDataOptimizerError(
            f"{name} must be numeric, got {type(value).__name__}"
        )
    v = float(value)
    if not math.isfinite(v):
        raise SyntheticDataOptimizerError(
            f"{name} must be finite, got {value!r}"
        )
    if min_value is not None and v < min_value:
        raise SyntheticDataOptimizerError(
            f"{name} must be >= {min_value}, got {v!r}"
        )
    if max_value is not None and v > max_value:
        raise SyntheticDataOptimizerError(
            f"{name} must be <= {max_value}, got {v!r}"
        )
    return v


def _safe_int(
    name: str,
    value: Any,
    *,
    min_value: Optional[int] = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SyntheticDataOptimizerError(
            f"{name} must be an integer, got {type(value).__name__}"
        )
    if min_value is not None and value < min_value:
        raise SyntheticDataOptimizerError(
            f"{name} must be >= {min_value}, got {value!r}"
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
            raise SyntheticDataOptimizerError(
                f"timestamp is not ISO-8601: {value!r}"
            ) from exc
    elif value is None:
        return datetime.now(timezone.utc)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            ts = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise SyntheticDataOptimizerError(
                f"timestamp could not be converted: {value!r}"
            ) from exc
    else:
        raise SyntheticDataOptimizerError(
            f"timestamp must be datetime, ISO str, or epoch; "
            f"got {type(value).__name__}"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SyntheticOptimizerConfig:
    """Tunable parameters for the synthetic data optimizer."""

    min_quality_threshold: float = 0.3
    max_similarity_threshold: float = 0.95
    informativeness_threshold: float = 0.4

    weight_informativeness: float = 0.3
    weight_difficulty: float = 0.2
    weight_quality: float = 0.3
    weight_diversity: float = 0.2

    diversity_sample_size: int = 100
    dedup_hash_prefix: Optional[int] = 16

    quality_loss_per_unit_compression: float = 0.3
    quality_gain_per_unit_synthetic: float = 0.1
    carbon_intensity_kg_per_kwh: float = 0.4

    #: Random seed. Set to ``None`` to explicitly opt out of
    #: determinism — two optimizers constructed with ``seed=None`` will
    #: produce different synthetic samples and diversity calculations.
    seed: Optional[int] = 42
    max_history: int = 1000

    informativeness_length_normalizer: int = 100
    difficulty_length_normalizer: int = 200
    difficulty_word_length_normalizer: int = 10

    def __post_init__(self) -> None:
        for name in (
            "min_quality_threshold", "max_similarity_threshold",
            "informativeness_threshold",
            "weight_informativeness", "weight_difficulty",
            "weight_quality", "weight_diversity",
        ):
            _safe_float(
                name, getattr(self, name),
                min_value=0.0, max_value=1.0,
            )
        total = (
            self.weight_informativeness + self.weight_difficulty
            + self.weight_quality + self.weight_diversity
        )
        if abs(total - 1.0) > 1e-6:
            raise SyntheticDataOptimizerError(
                f"scoring weights must sum to 1.0 (got {total:.6f})."
            )
        _safe_int(
            "diversity_sample_size", self.diversity_sample_size,
            min_value=1,
        )
        if self.dedup_hash_prefix is not None:
            _safe_int(
                "dedup_hash_prefix", self.dedup_hash_prefix,
                min_value=1,
            )
        for name in (
            "quality_loss_per_unit_compression",
            "quality_gain_per_unit_synthetic",
            "carbon_intensity_kg_per_kwh",
        ):
            _safe_float(name, getattr(self, name), min_value=0.0)
        _safe_int("max_history", self.max_history, min_value=1)
        for name in (
            "informativeness_length_normalizer",
            "difficulty_length_normalizer",
            "difficulty_word_length_normalizer",
        ):
            _safe_int(name, getattr(self, name), min_value=1)
        if self.seed is not None:
            _safe_int("seed", self.seed)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            **asdict(self),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SyntheticOptimizerConfig":
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                f"SyntheticOptimizerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        known = {f.name for f in fields(cls)}
        kwargs = {k: data[k] for k in known if k in data}
        try:
            return cls(**kwargs)
        except SyntheticDataOptimizerError:
            raise
        except (TypeError, ValueError) as exc:
            raise SyntheticDataOptimizerError(
                f"SyntheticOptimizerConfig.from_dict: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Result records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DataOptimizationResult:
    """Immutable result of a data optimization pass."""
    original_size: int
    optimized_size: int
    compression_ratio: float
    estimated_quality_retention: float
    estimated_energy_savings_kwh: float
    estimated_carbon_savings_kgco2e: float
    strategies_applied: Tuple[str, ...]
    synthetic_samples_added: int
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    def __post_init__(self) -> None:
        for name in (
            "original_size", "optimized_size", "synthetic_samples_added",
        ):
            _safe_int(name, getattr(self, name), min_value=0)
        _safe_float("compression_ratio", self.compression_ratio, min_value=0.0)
        _safe_float(
            "estimated_quality_retention",
            self.estimated_quality_retention,
            min_value=0.0, max_value=1.0,
        )
        _safe_float(
            "estimated_energy_savings_kwh",
            self.estimated_energy_savings_kwh, min_value=0.0,
        )
        _safe_float(
            "estimated_carbon_savings_kgco2e",
            self.estimated_carbon_savings_kgco2e, min_value=0.0,
        )
        if not isinstance(self.strategies_applied, tuple):
            raise SyntheticDataOptimizerError(
                "strategies_applied must be a tuple."
            )
        for i, s in enumerate(self.strategies_applied):
            if not isinstance(s, str) or not s:
                raise SyntheticDataOptimizerError(
                    f"strategies_applied[{i}] must be a non-empty string."
                )
        if not isinstance(self.timestamp, datetime):
            raise SyntheticDataOptimizerError(
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
            self.original_size,
            self.optimized_size,
            round(self.compression_ratio, 9),
            round(self.estimated_quality_retention, 9),
            round(self.estimated_energy_savings_kwh, 9),
            round(self.estimated_carbon_savings_kgco2e, 9),
            self.strategies_applied,
            self.synthetic_samples_added,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "original_size": self.original_size,
            "optimized_size": self.optimized_size,
            "compression_ratio": self.compression_ratio,
            "estimated_quality_retention": self.estimated_quality_retention,
            "estimated_energy_savings_kwh": self.estimated_energy_savings_kwh,
            "estimated_carbon_savings_kgco2e": (
                self.estimated_carbon_savings_kgco2e
            ),
            "strategies_applied": list(self.strategies_applied),
            "synthetic_samples_added": self.synthetic_samples_added,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DataOptimizationResult":
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                f"DataOptimizationResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        for required in (
            "original_size", "optimized_size", "compression_ratio",
            "estimated_quality_retention",
            "estimated_energy_savings_kwh",
            "estimated_carbon_savings_kgco2e",
        ):
            if required not in data:
                raise SyntheticDataOptimizerError(
                    f"DataOptimizationResult.from_dict: "
                    f"missing field {required!r}."
                )

        raw_strategies = data.get("strategies_applied", []) or []
        if not isinstance(raw_strategies, list):
            raise SyntheticDataOptimizerError(
                "'strategies_applied' must be a list."
            )
        strategies: List[str] = []
        for i, s in enumerate(raw_strategies):
            strategies.append(_safe_str(f"strategies_applied[{i}]", s))

        try:
            return cls(
                original_size=_safe_int(
                    "original_size", data["original_size"], min_value=0,
                ),
                optimized_size=_safe_int(
                    "optimized_size", data["optimized_size"], min_value=0,
                ),
                compression_ratio=_safe_float(
                    "compression_ratio", data["compression_ratio"],
                    min_value=0.0,
                ),
                estimated_quality_retention=_safe_float(
                    "estimated_quality_retention",
                    data["estimated_quality_retention"],
                    min_value=0.0, max_value=1.0,
                ),
                estimated_energy_savings_kwh=_safe_float(
                    "estimated_energy_savings_kwh",
                    data["estimated_energy_savings_kwh"], min_value=0.0,
                ),
                estimated_carbon_savings_kgco2e=_safe_float(
                    "estimated_carbon_savings_kgco2e",
                    data["estimated_carbon_savings_kgco2e"], min_value=0.0,
                ),
                strategies_applied=tuple(strategies),
                synthetic_samples_added=_safe_int(
                    "synthetic_samples_added",
                    data.get("synthetic_samples_added", 0),
                    min_value=0,
                ),
                timestamp=_parse_timestamp(data.get("timestamp")),
            )
        except SyntheticDataOptimizerError:
            raise

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise SyntheticDataOptimizerError(
                f"DataOptimizationResult is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "DataOptimizationResult":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SyntheticDataOptimizerError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

    def __repr__(self) -> str:
        return (
            "DataOptimizationResult("
            f"{self.original_size} -> {self.optimized_size} "
            f"({self.compression_ratio!r}x), "
            f"quality={self.estimated_quality_retention:.2%}, "
            f"synthetic={self.synthetic_samples_added})"
        )


@dataclass(frozen=True)
class SampleScore:
    """Immutable score for a training sample."""
    sample_id: str
    informativeness: float
    difficulty: float
    quality: float
    diversity: float
    composite_score: float

    def __post_init__(self) -> None:
        _safe_str("sample_id", self.sample_id)
        for name in (
            "informativeness", "difficulty", "quality", "diversity",
            "composite_score",
        ):
            _safe_float(
                name, getattr(self, name),
                min_value=0.0, max_value=1.0,
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SampleScore":
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                f"SampleScore.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        for required in (
            "sample_id", "informativeness", "difficulty",
            "quality", "diversity", "composite_score",
        ):
            if required not in data:
                raise SyntheticDataOptimizerError(
                    f"SampleScore.from_dict: missing field {required!r}."
                )
        return cls(
            sample_id=_safe_str("sample_id", data["sample_id"]),
            informativeness=_safe_float(
                "informativeness", data["informativeness"],
                min_value=0.0, max_value=1.0,
            ),
            difficulty=_safe_float(
                "difficulty", data["difficulty"],
                min_value=0.0, max_value=1.0,
            ),
            quality=_safe_float(
                "quality", data["quality"],
                min_value=0.0, max_value=1.0,
            ),
            diversity=_safe_float(
                "diversity", data["diversity"],
                min_value=0.0, max_value=1.0,
            ),
            composite_score=_safe_float(
                "composite_score", data["composite_score"],
                min_value=0.0, max_value=1.0,
            ),
        )

    def __repr__(self) -> str:
        return (
            "SampleScore("
            f"id={self.sample_id!r}, "
            f"composite={self.composite_score:.3f}, "
            f"quality={self.quality:.3f})"
        )


# --------------------------------------------------------------------------- #
# Optimizer
# --------------------------------------------------------------------------- #
class SyntheticDataOptimizer:
    """
    Optimizes training data through compression and augmentation.

    Thread-safe, deterministic (given a non-``None`` seed), serializable,
    and bounded in memory.
    """

    def __init__(
        self,
        *,
        config: Optional[SyntheticOptimizerConfig] = None,
        strict: bool = True,
    ) -> None:
        if not _HAS_NUMPY:
            raise SyntheticDataOptimizerError(
                "SyntheticDataOptimizer requires numpy. "
                "Install with: pip install numpy"
            )
        self._config = config or SyntheticOptimizerConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._rng_python = random.Random(self._config.seed)
        self._rng_numpy = _np.random.default_rng(self._config.seed)

        # Legacy counters preserved.
        self.total_compressions: int = 0
        self.total_synthetic_generated: int = 0
        self.total_energy_saved_kwh: float = 0.0

        # Bounded run history.
        self._history: Deque[DataOptimizationResult] = deque(
            maxlen=self._config.max_history,
        )

        logger.debug(
            "SyntheticDataOptimizer initialized "
            "(seed=%s, quality_threshold=%.2f, diversity_sample=%d, "
            "strict=%s)",
            self._config.seed,
            self._config.min_quality_threshold,
            self._config.diversity_sample_size,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SyntheticOptimizerConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def history(self) -> List[DataOptimizationResult]:
        with self._lock:
            return list(self._history)

    # Legacy mirror properties — mutations route through dataclasses.replace
    # so the config (the single source of truth) stays in sync.
    @property
    def min_quality_threshold(self) -> float:
        return self._config.min_quality_threshold

    @min_quality_threshold.setter
    def min_quality_threshold(self, value: float) -> None:
        self._config = replace(
            self._config,
            min_quality_threshold=_safe_float(
                "min_quality_threshold", value,
                min_value=0.0, max_value=1.0,
            ),
        )

    @property
    def max_similarity_threshold(self) -> float:
        return self._config.max_similarity_threshold

    @max_similarity_threshold.setter
    def max_similarity_threshold(self, value: float) -> None:
        self._config = replace(
            self._config,
            max_similarity_threshold=_safe_float(
                "max_similarity_threshold", value,
                min_value=0.0, max_value=1.0,
            ),
        )

    @property
    def informativeness_threshold(self) -> float:
        return self._config.informativeness_threshold

    @informativeness_threshold.setter
    def informativeness_threshold(self, value: float) -> None:
        self._config = replace(
            self._config,
            informativeness_threshold=_safe_float(
                "informativeness_threshold", value,
                min_value=0.0, max_value=1.0,
            ),
        )

    # ---------------------------------------------------------- public API
    def optimize(
        self,
        dataset: Sequence[Mapping[str, Any]],
        target_compression: float = 0.5,
        enable_synthetic: bool = True,
        synthetic_ratio: float = 0.3,
        baseline_energy_kwh: float = 1.0,
    ) -> DataOptimizationResult:
        """Run the optimization pipeline."""
        clean = self._prepare_dataset(dataset)
        self._validate_run_params(
            target_compression, synthetic_ratio, baseline_energy_kwh,
        )
        _, result = self._run_pipeline(
            clean,
            target_compression=target_compression,
            enable_synthetic=bool(enable_synthetic),
            synthetic_ratio=synthetic_ratio,
            baseline_energy_kwh=baseline_energy_kwh,
        )
        return result

    def optimize_dataset(
        self,
        dataset: Sequence[Mapping[str, Any]],
        target_compression: float = 0.5,
        enable_synthetic: bool = True,
        synthetic_ratio: float = 0.3,
        baseline_energy_kwh: float = 1.0,
    ) -> Tuple[List[Dict[str, Any]], DataOptimizationResult]:
        """
        Like :meth:`optimize` but also returns the optimized dataset list.

        Returns
        -------
        (optimized_dataset, result)
        """
        clean = self._prepare_dataset(dataset)
        self._validate_run_params(
            target_compression, synthetic_ratio, baseline_energy_kwh,
        )
        return self._run_pipeline(
            clean,
            target_compression=target_compression,
            enable_synthetic=bool(enable_synthetic),
            synthetic_ratio=synthetic_ratio,
            baseline_energy_kwh=baseline_energy_kwh,
        )

    def optimize_batch(
        self,
        datasets: Sequence[Sequence[Mapping[str, Any]]],
        target_compression: float = 0.5,
        enable_synthetic: bool = True,
        synthetic_ratio: float = 0.3,
        baseline_energy_kwh: float = 1.0,
    ) -> List[DataOptimizationResult]:
        """Apply :meth:`optimize` to a batch of datasets."""
        return [
            self.optimize(
                ds,
                target_compression=target_compression,
                enable_synthetic=enable_synthetic,
                synthetic_ratio=synthetic_ratio,
                baseline_energy_kwh=baseline_energy_kwh,
            )
            for ds in datasets
        ]

    # ---------------------------------------------------------- helpers
    def _prepare_dataset(
        self, dataset: Any,
    ) -> List[Dict[str, Any]]:
        """
        Validate + copy the dataset, filtering non-Mapping samples in
        non-strict mode. Returns a fresh list so the caller's data is
        never mutated.
        """
        if not isinstance(dataset, Sequence) or isinstance(
            dataset, (str, bytes),
        ):
            raise SyntheticDataOptimizerError(
                f"dataset must be a sequence of mappings, "
                f"got {type(dataset).__name__}."
            )
        clean: List[Dict[str, Any]] = []
        for i, sample in enumerate(dataset):
            if not isinstance(sample, Mapping):
                msg = (
                    f"dataset[{i}] must be a Mapping, got "
                    f"{type(sample).__name__}."
                )
                if self._strict:
                    raise SyntheticDataOptimizerError(msg)
                logger.warning("%s Skipping.", msg)
                continue
            clean.append(dict(sample))
        return clean

    def _validate_run_params(
        self,
        target_compression: float,
        synthetic_ratio: float,
        baseline_energy_kwh: float,
    ) -> None:
        if not isinstance(target_compression, (int, float)) or isinstance(
            target_compression, bool,
        ):
            raise SyntheticDataOptimizerError(
                "target_compression must be numeric."
            )
        if not 0.0 < float(target_compression) <= 1.0:
            raise SyntheticDataOptimizerError(
                "target_compression must be in (0, 1]."
            )
        if not isinstance(synthetic_ratio, (int, float)) or isinstance(
            synthetic_ratio, bool,
        ):
            raise SyntheticDataOptimizerError(
                "synthetic_ratio must be numeric."
            )
        if not 0.0 <= float(synthetic_ratio) <= 1.0:
            raise SyntheticDataOptimizerError(
                "synthetic_ratio must be in [0, 1]."
            )
        if not isinstance(baseline_energy_kwh, (int, float)) or isinstance(
            baseline_energy_kwh, bool,
        ):
            raise SyntheticDataOptimizerError(
                "baseline_energy_kwh must be numeric."
            )
        if not math.isfinite(float(baseline_energy_kwh)) or float(
            baseline_energy_kwh
        ) < 0:
            raise SyntheticDataOptimizerError(
                "baseline_energy_kwh must be finite and >= 0."
            )

    # ---------------------------------------------------------- pipeline
    def _run_pipeline(
        self,
        dataset: List[Dict[str, Any]],
        *,
        target_compression: float,
        enable_synthetic: bool,
        synthetic_ratio: float,
        baseline_energy_kwh: float,
    ) -> Tuple[List[Dict[str, Any]], DataOptimizationResult]:
        original_size = len(dataset)
        strategies_applied: List[str] = []
        call_id = uuid.uuid4().hex[:8]

        logger.info(
            "Optimizing dataset: %d samples (call=%s)",
            original_size, call_id,
        )

        # ---- Step 1: score all samples (also pre-computes text hashes) --
        sample_scores, text_hashes = self._score_samples(dataset)

        # ---- Step 2: deduplication ------------------------------------
        dataset, hashes, keep_indices = self._deduplicate(
            dataset, text_hashes,
        )
        num_duplicates = original_size - len(dataset)
        if num_duplicates > 0:
            strategies_applied.append(
                f"Deduplication (-{num_duplicates} samples)"
            )
        sample_scores = [sample_scores[i] for i in keep_indices]

        # ---- Step 3: quality filter -----------------------------------
        dataset, hashes, keep_indices, num_filtered = self._quality_filter(
            dataset, hashes, sample_scores,
        )
        if num_filtered > 0:
            strategies_applied.append(
                f"Quality Filtering (-{num_filtered} samples)"
            )
        sample_scores = [sample_scores[i] for i in keep_indices]

        # ---- Step 4: active learning ----------------------------------
        target_size = max(1, int(original_size * target_compression))
        dataset, hashes, sample_scores, num_removed = (
            self._active_learning_selection(
                dataset, hashes, sample_scores, target_size,
            )
        )
        if num_removed > 0:
            strategies_applied.append(
                f"Active Learning (-{num_removed} samples)"
            )

        active_learning_size = len(dataset)

        # ---- Step 5: synthetic augmentation ---------------------------
        synthetic_samples_added = 0
        synthetic_records: List[Tuple[Dict[str, Any], float]] = []
        if enable_synthetic and synthetic_ratio > 0 and active_learning_size > 0:
            num_synthetic = max(
                1, int(active_learning_size * synthetic_ratio),
            )
            synthetic_data = self._generate_synthetic(
                dataset, sample_scores, num_synthetic, call_id=call_id,
            )
            synthetic_records = synthetic_data
            dataset = list(dataset) + [s for s, _ in synthetic_data]
            synthetic_samples_added = len(synthetic_data)
            strategies_applied.append(
                f"Synthetic Augmentation (+{synthetic_samples_added} samples)"
            )

        # ---- Step 6: curriculum ordering ------------------------------
        dataset = self._curriculum_order(
            dataset, sample_scores, synthetic_records,
        )
        strategies_applied.append("Curriculum Ordering")

        # ---- Energy & carbon accounting -------------------------------
        final_size = len(dataset)
        # Net compression uses the *final* (post-synthetic) size so that
        # ``compression_ratio == original_size / optimized_size``.
        compression_ratio = (
            original_size / final_size if final_size > 0 else 1.0
        )
        # Energy scales with the number of *full-fidelity* samples; the
        # synthetic samples are a cheaper proxy (the whole point of the
        # layer). So we scale by ``active_learning_size``.
        energy_multiplier = (
            active_learning_size / original_size
            if original_size > 0 else 1.0
        )
        optimized_energy_kwh = baseline_energy_kwh * energy_multiplier
        energy_saved_kwh = max(
            0.0, baseline_energy_kwh - optimized_energy_kwh,
        )

        # ---- Quality retention heuristic ------------------------------
        cfg = self._config
        quality_loss = (
            (1.0 - target_compression)
            * cfg.quality_loss_per_unit_compression
        )
        base_quality = 1.0 - quality_loss
        if enable_synthetic and synthetic_ratio > 0:
            base_quality += (
                synthetic_ratio * cfg.quality_gain_per_unit_synthetic
            )
        quality_retention = max(0.0, min(1.0, base_quality))

        carbon_saved_kgco2e = (
            energy_saved_kwh * cfg.carbon_intensity_kg_per_kwh
        )

        # ---- Assemble result ------------------------------------------
        result = DataOptimizationResult(
            original_size=original_size,
            optimized_size=final_size,
            compression_ratio=compression_ratio,
            estimated_quality_retention=quality_retention,
            estimated_energy_savings_kwh=energy_saved_kwh,
            estimated_carbon_savings_kgco2e=carbon_saved_kgco2e,
            strategies_applied=tuple(strategies_applied),
            synthetic_samples_added=synthetic_samples_added,
        )

        with self._lock:
            self.total_compressions += 1
            self.total_synthetic_generated += synthetic_samples_added
            self.total_energy_saved_kwh += energy_saved_kwh
            self._history.append(result)

        logger.info(
            "Optimization complete: %d -> %d samples (%.2fx), "
            "%.3f kWh saved, quality retention %.1f%%",
            original_size, final_size, compression_ratio,
            energy_saved_kwh, quality_retention * 100.0,
        )
        return dataset, result

    # ---------------------------------------------------------- sample IDs
    @staticmethod
    def _sample_id(sample: Mapping[str, Any], index: int) -> str:
        sid = sample.get("id")
        if isinstance(sid, str) and sid:
            return sid
        return f"__row_{index}"

    @staticmethod
    def _sample_text(sample: Mapping[str, Any]) -> str:
        text = sample.get("text")
        if not isinstance(text, str):
            text = sample.get("content", "")
        return text if isinstance(text, str) else str(text)

    # ---------------------------------------------------------- scoring
    def _score_samples(
        self, dataset: List[Dict[str, Any]],
    ) -> Tuple[List[SampleScore], List[str]]:
        """
        Score each sample.

        Also returns pre-computed SHA-256 text hashes so downstream steps
        (diversity sampling, deduplication) don't re-hash every text.
        """
        cfg = self._config
        scores: List[SampleScore] = []
        hashes: List[str] = []

        for i, sample in enumerate(dataset):
            sample_id = self._sample_id(sample, i)
            text = self._sample_text(sample)
            text_hash = hashlib.sha256(
                text.encode("utf-8", errors="replace"),
            ).hexdigest()
            hashes.append(text_hash)

            informativeness = self._calculate_informativeness(text)
            difficulty = self._calculate_difficulty(text)
            quality = self._calculate_quality(text)
            diversity = self._calculate_diversity_from_hashes(
                text_hash, i, hashes + [-1] * 0,
                n_total=len(dataset),
                existing=hashes,
            ) if False else self._calculate_diversity_from_hashes(
                text_hash, i, hashes, n_total=len(dataset),
            )

            composite = (
                cfg.weight_informativeness * informativeness
                + cfg.weight_difficulty * difficulty
                + cfg.weight_quality * quality
                + cfg.weight_diversity * diversity
            )

            scores.append(SampleScore(
                sample_id=sample_id,
                informativeness=informativeness,
                difficulty=difficulty,
                quality=quality,
                diversity=diversity,
                composite_score=max(0.0, min(1.0, composite)),
            ))

        return scores, hashes

    def _calculate_informativeness(self, text: str) -> float:
        if not text:
            return 0.0
        words = text.split()
        if not words:
            return 0.0
        unique_ratio = len(set(words)) / len(words)
        length_score = min(
            1.0,
            len(words) / self._config.informativeness_length_normalizer,
        )
        return (unique_ratio + length_score) / 2.0

    def _calculate_difficulty(self, text: str) -> float:
        if not text:
            return 0.0
        words = text.split()
        if not words:
            return 0.0
        avg_word_length = sum(len(w) for w in words) / len(words)
        length_score = min(
            1.0, len(words) / self._config.difficulty_length_normalizer,
        )
        complexity_score = min(
            1.0,
            avg_word_length
            / self._config.difficulty_word_length_normalizer,
        )
        return (length_score + complexity_score) / 2.0

    def _calculate_quality(self, text: str) -> float:
        if not text:
            return 0.0
        noise_indicators = (
            "???", "...", "!!!", "###", "[deleted]", "[removed]",
        )
        lowered = text.lower()
        noise_count = sum(
            1 for ind in noise_indicators if ind in lowered
        )
        punct_ratio = sum(
            1 for c in text if not c.isalnum()
        ) / len(text)
        quality = 1.0 - (noise_count * 0.2) - (punct_ratio * 0.5)
        return max(0.0, min(1.0, quality))

    def _calculate_diversity_from_hashes(
        self,
        text_hash: str,
        index: int,
        hashes: List[str],
        *,
        n_total: int,
    ) -> float:
        """
        Estimate diversity (0–1) using pre-computed peer hashes.

        Bounded by ``config.diversity_sample_size``; deterministic given
        a non-``None`` seed.
        """
        if n_total < 2:
            return 1.0
        candidate_indices = [
            j for j in range(len(hashes)) if j != index
        ]
        if not candidate_indices:
            return 1.0
        sample_size = min(
            self._config.diversity_sample_size, len(candidate_indices),
        )
        if sample_size <= 0:
            return 1.0
        if self._rng_numpy is not None:
            peers = self._rng_numpy.choice(
                candidate_indices, size=sample_size, replace=False,
            )
        else:  # pragma: no cover — numpy is required
            peers = self._rng_python.sample(
                candidate_indices, sample_size,
            )
        similarities: List[float] = []
        for peer_idx in peers:
            peer_hash = hashes[int(peer_idx)]
            if not isinstance(peer_hash, str) or not peer_hash:
                continue
            sim = sum(
                a == b for a, b in zip(text_hash, peer_hash)
            ) / len(text_hash)
            similarities.append(sim)
        if not similarities:
            return 1.0
        avg_similarity = sum(similarities) / len(similarities)
        return max(0.0, min(1.0, 1.0 - avg_similarity))

    # ---------------------------------------------------------- dedup
    def _deduplicate(
        self,
        dataset: List[Dict[str, Any]],
        hashes: List[str],
    ) -> Tuple[List[Dict[str, Any]], List[str], List[int]]:
        """Remove near-duplicate samples; reuse the pre-computed hashes."""
        prefix = self._config.dedup_hash_prefix
        seen: set = set()
        deduplicated: List[Dict[str, Any]] = []
        kept_hashes: List[str] = []
        keep_indices: List[int] = []
        num_duplicates = 0

        for i, (sample, digest) in enumerate(zip(dataset, hashes)):
            if prefix is not None:
                key = digest[:prefix]
            else:
                key = digest
            if key not in seen:
                seen.add(key)
                deduplicated.append(sample)
                kept_hashes.append(digest)
                keep_indices.append(i)
            else:
                num_duplicates += 1

        logger.info(
            "Deduplication: removed %d duplicates.", num_duplicates,
        )
        return deduplicated, kept_hashes, keep_indices

    # ---------------------------------------------------------- quality filter
    def _quality_filter(
        self,
        dataset: List[Dict[str, Any]],
        hashes: List[str],
        scores: List[SampleScore],
    ) -> Tuple[List[Dict[str, Any]], List[str], List[int], int]:
        """Filter out low-quality samples; matches scores positionally."""
        threshold = self._config.min_quality_threshold
        filtered: List[Dict[str, Any]] = []
        kept_hashes: List[str] = []
        keep_indices: List[int] = []
        num_filtered = 0

        for i, sample in enumerate(dataset):
            score = scores[i] if i < len(scores) else None
            if score is not None and score.quality >= threshold:
                filtered.append(sample)
                kept_hashes.append(
                    hashes[i] if i < len(hashes) else ""
                )
                keep_indices.append(i)
            else:
                num_filtered += 1

        logger.info(
            "Quality filtering: removed %d low-quality samples.",
            num_filtered,
        )
        return filtered, kept_hashes, keep_indices, num_filtered

    # ---------------------------------------------------------- active learning
    def _active_learning_selection(
        self,
        dataset: List[Dict[str, Any]],
        hashes: List[str],
        scores: List[SampleScore],
        target_size: int,
    ) -> Tuple[
        List[Dict[str, Any]], List[str], List[SampleScore], int,
    ]:
        """Select the top-``target_size`` samples by composite score."""
        if len(dataset) <= target_size:
            return dataset, hashes, scores, 0

        ranked = sorted(
            zip(dataset, hashes, scores),
            key=lambda triple: triple[2].composite_score,
            reverse=True,
        )
        selected = ranked[:target_size]
        selected_dataset = [s for s, _, _ in selected]
        selected_hashes = [h for _, h, _ in selected]
        selected_scores = [sc for _, _, sc in selected]
        num_removed = len(dataset) - len(selected_dataset)

        logger.info(
            "Active learning: selected %d/%d (removed %d).",
            len(selected_dataset), len(dataset), num_removed,
        )
        return (
            selected_dataset, selected_hashes,
            selected_scores, num_removed,
        )

    # ---------------------------------------------------------- synthetic
    def _generate_synthetic(
        self,
        dataset: List[Dict[str, Any]],
        scores: List[SampleScore],
        num_synthetic: int,
        *,
        call_id: str,
    ) -> List[Tuple[Dict[str, Any], float]]:
        """
        Generate synthetic samples.

        Returns ``[(sample_dict, source_difficulty), ...]`` so that
        ``_curriculum_order`` can place synthetics near their source's
        difficulty bucket (previously all synthetics were appended after
        the hardest real sample).
        """
        synthetic: List[Tuple[Dict[str, Any], float]] = []
        if not dataset or num_synthetic <= 0:
            return synthetic

        difficulty_by_source = {
            id(dataset[i]): scores[i].difficulty
            for i in range(min(len(dataset), len(scores)))
        }

        num_paraphrase = max(1, int(num_synthetic * 0.5))
        for i in range(num_paraphrase):
            source = dataset[i % len(dataset)]
            source_difficulty = difficulty_by_source.get(
                id(source), 0.5,
            )
            synthetic.append((
                {
                    "id": f"synthetic_paraphrase_{call_id}_{i}",
                    "text": self._paraphrase(
                        self._sample_text(source)
                    ),
                    "label": source.get("label"),
                    "synthetic": True,
                    "source": "paraphrase",
                },
                source_difficulty,
            ))

        num_mixup = max(0, num_synthetic - num_paraphrase)
        if num_mixup > 0 and len(dataset) >= 2:
            for i in range(num_mixup):
                idx1, idx2 = self._rng_numpy.choice(
                    len(dataset), size=2, replace=False,
                )
                source1 = dataset[int(idx1)]
                source2 = dataset[int(idx2)]
                source_difficulty = 0.5 * (
                    difficulty_by_source.get(id(source1), 0.5)
                    + difficulty_by_source.get(id(source2), 0.5)
                )
                synthetic.append((
                    {
                        "id": f"synthetic_mixup_{call_id}_{i}",
                        "text": self._mixup(
                            self._sample_text(source1),
                            self._sample_text(source2),
                        ),
                        "label": source1.get("label"),
                        "synthetic": True,
                        "source": "mixup",
                    },
                    source_difficulty,
                ))

        logger.info(
            "Generated %d synthetic samples.", len(synthetic),
        )
        return synthetic

    def _paraphrase(self, text: str) -> str:
        return f"[Paraphrased] {text}"

    def _mixup(self, text1: str, text2: str) -> str:
        words1 = text1.split()
        words2 = text2.split()
        if not words1 or not words2:
            return text1 or text2
        mid1 = len(words1) // 2
        mid2 = len(words2) // 2
        return " ".join(words1[:mid1] + words2[mid2:])

    # ---------------------------------------------------------- curriculum
    def _curriculum_order(
        self,
        dataset: List[Dict[str, Any]],
        scores: List[SampleScore],
        synthetic_records: List[Tuple[Dict[str, Any], float]],
    ) -> List[Dict[str, Any]]:
        """
        Order the dataset by difficulty (easy → hard).

        Synthetic samples inherit their source's difficulty, so a
        paraphrase of an easy sample lands near other easy samples rather
        than after the hardest real sample.
        """
        score_by_id = {s.sample_id: s for s in scores}
        synthetic_difficulty = {
            id(sample): diff for sample, diff in synthetic_records
        }

        scored: List[Tuple[Dict[str, Any], float]] = []
        for i, sample in enumerate(dataset):
            sid = self._sample_id(sample, i)
            score = score_by_id.get(sid)
            if score is not None:
                scored.append((sample, score.difficulty))
                continue
            # Synthetic sample: inherit the source's difficulty
            diff = synthetic_difficulty.get(id(sample), 0.5)
            scored.append((sample, diff))

        scored.sort(key=lambda pair: pair[1])
        return [s for s, _ in scored]

    # ---------------------------------------------------------- statistics
    def get_statistics(self) -> Dict[str, Any]:
        """Return optimizer statistics."""
        with self._lock:
            history = list(self._history)
            compressions = self.total_compressions
            synthetic = self.total_synthetic_generated
            energy = self.total_energy_saved_kwh

        cfg = self._config
        payload: Dict[str, Any] = {
            "schema_version": __version__,
            "total_compressions": compressions,
            "total_synthetic_generated": synthetic,
            "total_energy_saved_kwh": energy,
            "total_carbon_saved_kgco2e": (
                energy * cfg.carbon_intensity_kg_per_kwh
            ),
        }

        if history:
            # Aggregate strategy usage.
            strategy_counts: Dict[str, int] = {}
            for r in history:
                for s in r.strategies_applied:
                    # The strategy strings are human-readable like
                    # "Deduplication (-5 samples)"; extract the kind.
                    kind = s.split(" (")[0]
                    strategy_counts[kind] = (
                        strategy_counts.get(kind, 0) + 1
                    )

            payload.update({
                "mean_compression_ratio": (
                    sum(r.compression_ratio for r in history)
                    / len(history)
                ),
                "mean_quality_retention": (
                    sum(r.estimated_quality_retention for r in history)
                    / len(history)
                ),
                "mean_energy_saved_kwh": (
                    sum(r.estimated_energy_savings_kwh for r in history)
                    / len(history)
                ),
                "by_strategy": strategy_counts,
                "last_result": history[-1].to_dict(),
            })
        return payload

    def statistics(self) -> Dict[str, Any]:
        return self.get_statistics()

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            self.total_compressions = 0
            self.total_synthetic_generated = 0
            self.total_energy_saved_kwh = 0.0
            if clear_history:
                self._history.clear()
        logger.debug("SyntheticDataOptimizer reset.")

    # ---------------------------------------------------------- diagnostics
    def test_smoke(self) -> Dict[str, Any]:
        """Structured health check. Never raises."""
        results: Dict[str, Any] = {
            "ok": False, "package_version": __version__, "steps": [],
        }
        try:
            ds = [
                {"id": f"s{i}", "text": f"sample {i}", "label": i % 2}
                for i in range(20)
            ]
            result = self.optimize(
                ds, target_compression=0.5, synthetic_ratio=0.2,
            )
            results["steps"].append({
                "name": "optimize",
                "ok": (
                    isinstance(result, DataOptimizationResult)
                    and result.original_size == 20
                ),
            })
            ok = (
                result.compression_ratio == pytest.approx  # noqa: F821
                if False else (
                    math.isclose(
                        result.compression_ratio,
                        result.original_size / result.optimized_size,
                        rel_tol=1e-9,
                    )
                )
            )
            results["steps"].append({
                "name": "compression_ratio_consistent", "ok": ok,
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
            "compressions": self.total_compressions,
            "synthetic_generated": self.total_synthetic_generated,
            "energy_saved_kwh": self.total_energy_saved_kwh,
            "steps": smoke.get("steps", []),
            "error": smoke.get("error"),
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "total_compressions": self.total_compressions,
                    "total_synthetic_generated": (
                        self.total_synthetic_generated
                    ),
                    "total_energy_saved_kwh": self.total_energy_saved_kwh,
                },
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SyntheticDataOptimizer":
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        cfg = SyntheticOptimizerConfig.from_dict(
            dict(data.get("config", {}) or {}),
        )
        opt = cls(config=cfg, strict=bool(data.get("strict", True)))

        raw_history = data.get("history", []) or []
        if not isinstance(raw_history, list):
            raise SyntheticDataOptimizerError(
                "'history' must be a list."
            )
        max_h = cfg.max_history
        if len(raw_history) > max_h:
            logger.warning(
                "Restoring %d history entries into a max_history=%d "
                "buffer; keeping the most recent %d.",
                len(raw_history), max_h, max_h,
            )
            raw_history = raw_history[-max_h:]

        with opt._lock:
            counters = dict(data.get("counters", {}) or {})
            opt.total_compressions = _safe_int(
                "counters.total_compressions",
                counters.get("total_compressions", 0), min_value=0,
            )
            opt.total_synthetic_generated = _safe_int(
                "counters.total_synthetic_generated",
                counters.get("total_synthetic_generated", 0),
                min_value=0,
            )
            opt.total_energy_saved_kwh = _safe_float(
                "counters.total_energy_saved_kwh",
                counters.get("total_energy_saved_kwh", 0.0),
                min_value=0.0,
            )
            for i, entry in enumerate(raw_history):
                try:
                    opt._history.append(
                        DataOptimizationResult.from_dict(entry),
                    )
                except SyntheticDataOptimizerError as exc:
                    raise SyntheticDataOptimizerError(
                        f"history[{i}]: {exc}"
                    ) from exc
        return opt

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise SyntheticDataOptimizerError(
                f"optimizer is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "SyntheticDataOptimizer":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SyntheticDataOptimizerError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "SyntheticDataOptimizer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "SyntheticDataOptimizer scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "SyntheticDataOptimizer("
                f"compressions={self.total_compressions}, "
                f"synthetic_generated={self.total_synthetic_generated}, "
                f"energy_saved_kwh={self.total_energy_saved_kwh!r}, "
                f"seed={self._config.seed!r}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    optimizer = SyntheticDataOptimizer()
    print("repr       :", optimizer)

    # ---- Build a synthetic dataset --------------------------------
    dataset: List[Dict[str, Any]] = [
        {
            "id": f"sample_{i}",
            "text": f"This is training sample {i} with varied content.",
            "label": i % 2,
        }
        for i in range(2_000)
    ]
    dataset.extend(dataset[:100])   # 100 duplicates
    for i in range(50):             # 50 low-quality samples
        dataset.append({
            "id": f"noisy_{i}",
            "text": "??? ### [deleted]",
            "label": 0,
        })
    for i in range(20):             # 20 samples without IDs
        dataset.append({
            "text": f"untagged sample {i} with reasonable content",
            "label": 1,
        })

    print(f"original   : {len(dataset)} samples")

    result = optimizer.optimize(
        dataset=dataset,
        target_compression=0.3,
        enable_synthetic=True,
        synthetic_ratio=0.2,
        baseline_energy_kwh=2.0,
    )
    print("result     :", result)
    for strategy in result.strategies_applied:
        print("  -", strategy)

    # ---- P0 #2 regression: compression_ratio == original/optimized
    assert math.isclose(
        result.compression_ratio,
        result.original_size / result.optimized_size,
        rel_tol=1e-9,
    ), (
        f"compression_ratio {result.compression_ratio} != "
        f"{result.original_size}/{result.optimized_size}"
    )
    print("compression_ratio consistency: OK")

    # ---- P0 #1 regression: non-strict optimize() filters non-Mappings
    lenient = SyntheticDataOptimizer(strict=False)
    clean_result = lenient.optimize(
        [{"id": "s1", "text": "hello"}] + [42, None, "not a mapping"],
        target_compression=1.0,
        synthetic_ratio=0.0,
    )
    assert clean_result.original_size == 1, clean_result.original_size
    print("non-strict optimize(): OK")

    # ---- P0 #3 regression: NaN rejected in DataOptimizationResult
    try:
        DataOptimizationResult(
            original_size=100, optimized_size=50,
            compression_ratio=float("nan"),
            estimated_quality_retention=0.9,
            estimated_energy_savings_kwh=0.1,
            estimated_carbon_savings_kgco2e=0.04,
            strategies_applied=(),
            synthetic_samples_added=0,
        )
    except SyntheticDataOptimizerError as exc:
        print("NaN rejected:", exc)
    else:
        raise AssertionError("NaN compression_ratio should raise")

    # ---- P0 #4b regression: config round-trip preserves all fields
    cfg = SyntheticOptimizerConfig(
        informativeness_length_normalizer=200,
        difficulty_length_normalizer=400,
        difficulty_word_length_normalizer=20,
        max_history=500,
    )
    opt_cfg = SyntheticDataOptimizer(config=cfg)
    restored_cfg = SyntheticDataOptimizer.from_dict(opt_cfg.to_dict())
    assert restored_cfg.config == cfg, (
        f"config round-trip lost fields: {restored_cfg.config} != {cfg}"
    )
    print("config round-trip: OK")

    # ---- P1 #6 regression: legacy threshold mutation honoured
    legacy = SyntheticDataOptimizer()
    legacy.min_quality_threshold = 0.9
    assert legacy.config.min_quality_threshold == 0.9
    print("legacy threshold mutation: OK")

    # ---- P1 #7 regression: SampleScore rejects string scores
    try:
        SampleScore(
            sample_id="x", informativeness="a",
            difficulty=0.5, quality=0.5, diversity=0.5,
            composite_score=0.5,
        )
    except SyntheticDataOptimizerError as exc:
        print("SampleScore string rejected:", exc)
    else:
        raise AssertionError("string informativeness should raise")

    # ---- P1 #13 regression: optimize_dataset validates params
    try:
        optimizer.optimize_dataset(dataset, target_compression=2.0)
    except SyntheticDataOptimizerError as exc:
        print("param validation:", exc)
    else:
        raise AssertionError(
            "optimize_dataset should reject target_compression=2.0"
        )

    # ---- Reproducibility --------------------------------------------------
    a = SyntheticDataOptimizer(config=SyntheticOptimizerConfig(seed=7))
    b = SyntheticDataOptimizer(config=SyntheticOptimizerConfig(seed=7))
    ra = a.optimize(dataset[:500], target_compression=0.4, synthetic_ratio=0.2)
    rb = b.optimize(dataset[:500], target_compression=0.4, synthetic_ratio=0.2)
    assert ra.to_dict() == rb.to_dict(), "seeded runs must be identical"
    print("reproducibility OK.")

    # ---- Statistics ------------------------------------------------------
    stats = optimizer.get_statistics()
    print("stats      :", {
        k: v for k, v in stats.items() if k != "last_result"
    })

    # ---- Round-trip ------------------------------------------------------
    payload = optimizer.to_json()
    restored = SyntheticDataOptimizer.from_json(payload)
    assert restored.to_dict() == optimizer.to_dict()
    print("round-trip OK.")

    # ---- Tiny dataset ----------------------------------------------------
    tiny_result = SyntheticDataOptimizer().optimize(
        [{"id": "only", "text": "single sample", "label": 0}],
        target_compression=1.0,
        synthetic_ratio=0.0,
    )
    print("tiny       :", tiny_result)

    # ---- Validation -----------------------------------------------------
    for bad_kwargs in (
        dict(target_compression=0),
        dict(target_compression=1.5),
        dict(synthetic_ratio=-0.5),
        dict(baseline_energy_kwh=-1.0),
    ):
        try:
            optimizer.optimize(dataset, **bad_kwargs)  # type: ignore[arg-type]
        except SyntheticDataOptimizerError as exc:
            print("Rejected   :", exc)

    for bad_cfg in (
        dict(min_quality_threshold=-0.1),
        dict(weight_informativeness=0.5),
        dict(diversity_sample_size=0),
        dict(max_history=0),
    ):
        try:
            SyntheticOptimizerConfig(**bad_cfg)  # type: ignore[arg-type]
        except SyntheticDataOptimizerError as exc:
            print("Rejected cfg:", exc)

    print("test_smoke :", optimizer.test_smoke())
    print("health     :", optimizer.health())

    print("\nSmoke test passed.")
