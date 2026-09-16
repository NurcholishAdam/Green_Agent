# src/optimization/synthetic_data_optimizer.py

"""
Synthetic Data Optimization Layer
==================================

Optimizes training through intelligent data augmentation and compression.

Goal: Reduce compute 80–95% through data-centric optimization.

Enhancements
------------
- ``SyntheticOptimizerConfig`` — frozen, validated; every previously
  hardcoded coefficient is now configurable.
- **Deterministic randomness** — a seeded ``random.Random`` /
  ``numpy.random.Generator`` per optimizer instance.
- **Fixed O(N²) diversity calculation** — bounded sampling via
  ``diversity_sample_size``.
- **Fixed crash on tiny datasets** — the ``np.random.choice`` call is now
  guarded before the loop.
- **Fixed ID-collision bug** — synthetic sample IDs include a per-call UUID
  prefix.
- **Fixed filter-drift bugs** — samples without IDs are handled via index
  fallback; un-scored samples in ``_curriculum_order`` are retained.
- **Non-destructive** — the caller's dataset list is never mutated; a new
  list is returned.
- **Thread safety** via ``RLock``; bounded run history.
- Serialization on both classes and on every dataclass.
- ``statistics()``, ``__repr__``, custom ``SyntheticDataOptimizerError``,
  lazy ``%s`` logging, and a comprehensive ``__main__`` smoke test.
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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


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
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SyntheticOptimizerConfig:
    """Tunable parameters for the synthetic data optimizer."""

    # Quality / similarity thresholds.
    min_quality_threshold: float = 0.3
    max_similarity_threshold: float = 0.95
    informativeness_threshold: float = 0.4

    # Sample scoring composite weights (must sum to 1.0).
    weight_informativeness: float = 0.3
    weight_difficulty: float = 0.2
    weight_quality: float = 0.3
    weight_diversity: float = 0.2

    # Diversity sampling — sample at most this many peers per sample.
    diversity_sample_size: int = 100

    # Deduplication hash prefix (in hex chars). ``None`` uses full SHA-256.
    dedup_hash_prefix: Optional[int] = 16

    # Quality retention heuristic coefficients.
    quality_loss_per_unit_compression: float = 0.3
    quality_gain_per_unit_synthetic: float = 0.1

    # Carbon intensity used to translate energy savings (kg CO2e / kWh).
    carbon_intensity_kg_per_kwh: float = 0.4

    # Random seed for reproducible selection / diversity sampling.
    seed: Optional[int] = 42

    # Bounded per-run history.
    max_history: int = 1000

    # Text-length scoring normalizers.
    informativeness_length_normalizer: int = 100
    difficulty_length_normalizer: int = 200
    difficulty_word_length_normalizer: int = 10

    def __post_init__(self) -> None:
        if not 0.0 <= self.min_quality_threshold <= 1.0:
            raise SyntheticDataOptimizerError(
                "min_quality_threshold must be in [0, 1]."
            )
        if not 0.0 <= self.max_similarity_threshold <= 1.0:
            raise SyntheticDataOptimizerError(
                "max_similarity_threshold must be in [0, 1]."
            )
        if not 0.0 <= self.informativeness_threshold <= 1.0:
            raise SyntheticDataOptimizerError(
                "informativeness_threshold must be in [0, 1]."
            )
        weights = (
            self.weight_informativeness + self.weight_difficulty
            + self.weight_quality + self.weight_diversity
        )
        if abs(weights - 1.0) > 1e-6:
            raise SyntheticDataOptimizerError(
                f"scoring weights must sum to 1.0 (got {weights:.6f})."
            )
        for name in (
            "weight_informativeness", "weight_difficulty",
            "weight_quality", "weight_diversity",
        ):
            if getattr(self, name) < 0:
                raise SyntheticDataOptimizerError(f"{name} must be >= 0.")
        if self.diversity_sample_size <= 0:
            raise SyntheticDataOptimizerError(
                "diversity_sample_size must be > 0."
            )
        if self.dedup_hash_prefix is not None and self.dedup_hash_prefix <= 0:
            raise SyntheticDataOptimizerError(
                "dedup_hash_prefix must be a positive int or None."
            )
        if self.quality_loss_per_unit_compression < 0:
            raise SyntheticDataOptimizerError(
                "quality_loss_per_unit_compression must be >= 0."
            )
        if self.quality_gain_per_unit_synthetic < 0:
            raise SyntheticDataOptimizerError(
                "quality_gain_per_unit_synthetic must be >= 0."
            )
        if self.carbon_intensity_kg_per_kwh < 0:
            raise SyntheticDataOptimizerError(
                "carbon_intensity_kg_per_kwh must be >= 0."
            )
        if self.max_history <= 0:
            raise SyntheticDataOptimizerError("max_history must be > 0.")
        for name in (
            "informativeness_length_normalizer",
            "difficulty_length_normalizer",
            "difficulty_word_length_normalizer",
        ):
            if getattr(self, name) <= 0:
                raise SyntheticDataOptimizerError(f"{name} must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


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
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if self.original_size < 0:
            raise SyntheticDataOptimizerError("original_size must be >= 0.")
        if self.optimized_size < 0:
            raise SyntheticDataOptimizerError("optimized_size must be >= 0.")
        if self.compression_ratio < 0:
            raise SyntheticDataOptimizerError("compression_ratio must be >= 0.")
        if not 0.0 <= self.estimated_quality_retention <= 1.0:
            raise SyntheticDataOptimizerError(
                "estimated_quality_retention must be in [0, 1]."
            )
        if self.estimated_energy_savings_kwh < 0:
            raise SyntheticDataOptimizerError(
                "estimated_energy_savings_kwh must be >= 0."
            )
        if self.estimated_carbon_savings_kgco2e < 0:
            raise SyntheticDataOptimizerError(
                "estimated_carbon_savings_kgco2e must be >= 0."
            )
        if self.synthetic_samples_added < 0:
            raise SyntheticDataOptimizerError(
                "synthetic_samples_added must be >= 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_size": self.original_size,
            "optimized_size": self.optimized_size,
            "compression_ratio": self.compression_ratio,
            "estimated_quality_retention": self.estimated_quality_retention,
            "estimated_energy_savings_kwh": self.estimated_energy_savings_kwh,
            "estimated_carbon_savings_kgco2e": self.estimated_carbon_savings_kgco2e,
            "strategies_applied": list(self.strategies_applied),
            "synthetic_samples_added": self.synthetic_samples_added,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DataOptimizationResult":
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                "DataOptimizationResult.from_dict expects a Mapping."
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
            original_size=int(data["original_size"]),
            optimized_size=int(data["optimized_size"]),
            compression_ratio=float(data["compression_ratio"]),
            estimated_quality_retention=float(
                data["estimated_quality_retention"]
            ),
            estimated_energy_savings_kwh=float(
                data["estimated_energy_savings_kwh"]
            ),
            estimated_carbon_savings_kgco2e=float(
                data["estimated_carbon_savings_kgco2e"]
            ),
            strategies_applied=tuple(data.get("strategies_applied", [])),
            synthetic_samples_added=int(data.get("synthetic_samples_added", 0)),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "DataOptimizationResult("
            f"{self.original_size} -> {self.optimized_size} "
            f"({self.compression_ratio:.2f}x), "
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
        if not isinstance(self.sample_id, str) or not self.sample_id:
            raise SyntheticDataOptimizerError(
                "sample_id must be a non-empty string."
            )
        for name in (
            "informativeness", "difficulty", "quality", "diversity",
            "composite_score",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise SyntheticDataOptimizerError(
                    f"{name} must be in [0, 1], got {value}."
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SampleScore":
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                "SampleScore.from_dict expects a Mapping."
            )
        return cls(
            sample_id=str(data["sample_id"]),
            informativeness=float(data["informativeness"]),
            difficulty=float(data["difficulty"]),
            quality=float(data["quality"]),
            diversity=float(data["diversity"]),
            composite_score=float(data["composite_score"]),
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

    Thread-safe, deterministic (given a seed), serializable, and bounded in
    memory. The original public API (``optimize`` and ``get_statistics``) is
    preserved; new parameters are keyword-only.

    Parameters
    ----------
    config : SyntheticOptimizerConfig, optional
        Configuration. Defaults to ``SyntheticOptimizerConfig()``.
    strict : bool, default True
        If True, invalid inputs raise :class:`SyntheticDataOptimizerError`.
        If False, invalid inputs are logged and coerced.
    """

    def __init__(
        self,
        *,
        config: Optional[SyntheticOptimizerConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or SyntheticOptimizerConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        # Seeded randomness for reproducibility.
        self._rng_python = random.Random(self._config.seed)
        import numpy as np  # local import to keep the module importable without numpy? — see notes
        self._np = np
        self._rng_numpy = np.random.default_rng(self._config.seed)

        # Legacy attribute mirrors (kept for backward compatibility).
        self.min_quality_threshold = self._config.min_quality_threshold
        self.max_similarity_threshold = self._config.max_similarity_threshold
        self.informativeness_threshold = self._config.informativeness_threshold

        # Legacy counters preserved.
        self.total_compressions: int = 0
        self.total_synthetic_generated: int = 0
        self.total_energy_saved_kwh: float = 0.0

        # Bounded run history.
        self._history: Deque[DataOptimizationResult] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "SyntheticDataOptimizer initialized "
            "(seed=%s, quality_threshold=%.2f, diversity_sample=%d, strict=%s)",
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
    def history(self) -> List[DataOptimizationResult]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def optimize(
        self,
        dataset: Sequence[Mapping[str, Any]],
        target_compression: float = 0.5,
        enable_synthetic: bool = True,
        synthetic_ratio: float = 0.3,
        baseline_energy_kwh: float = 1.0,
    ) -> DataOptimizationResult:
        """
        Run the optimization pipeline.

        See original docstring for parameter meanings. The caller's
        ``dataset`` is **not mutated**; the returned result is a frozen
        dataclass. Use :meth:`optimize_dataset` if you also want the
        optimized list back.
        """
        if not isinstance(dataset, Sequence) or isinstance(dataset, (str, bytes)):
            raise SyntheticDataOptimizerError(
                f"dataset must be a sequence of mappings, "
                f"got {type(dataset).__name__}."
            )
        for i, sample in enumerate(dataset):
            if not isinstance(sample, Mapping):
                msg = (
                    f"dataset[{i}] must be a Mapping, got "
                    f"{type(sample).__name__}."
                )
                if self._strict:
                    raise SyntheticDataOptimizerError(msg)
                logger.warning("%s Skipping.", msg)

        if not 0.0 < target_compression <= 1.0:
            raise SyntheticDataOptimizerError(
                "target_compression must be in (0, 1]."
            )
        if not 0.0 <= synthetic_ratio <= 1.0:
            raise SyntheticDataOptimizerError(
                "synthetic_ratio must be in [0, 1]."
            )
        if baseline_energy_kwh < 0:
            raise SyntheticDataOptimizerError(
                "baseline_energy_kwh must be >= 0."
            )

        _, result = self._run_pipeline(
            list(dataset),
            target_compression=target_compression,
            enable_synthetic=enable_synthetic,
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
        if not isinstance(dataset, Sequence) or isinstance(dataset, (str, bytes)):
            raise SyntheticDataOptimizerError(
                f"dataset must be a sequence of mappings, "
                f"got {type(dataset).__name__}."
            )
        return self._run_pipeline(
            [dict(s) for s in dataset if isinstance(s, Mapping)],
            target_compression=target_compression,
            enable_synthetic=enable_synthetic,
            synthetic_ratio=synthetic_ratio,
            baseline_energy_kwh=baseline_energy_kwh,
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
            "Optimizing dataset: %d samples (call=%s)", original_size, call_id,
        )

        # ---- Step 1: score all samples ---------------------------------
        sample_scores = self._score_samples(dataset)
        # Index scores by the *position* so subsequent filters never lose
        # samples that lack an ``id`` field.
        score_by_index = {
            i: sample_scores[i]
            for i in range(len(sample_scores))
        }

        # ---- Step 2: deduplication ------------------------------------
        dataset, keep_indices = self._deduplicate(dataset)
        num_duplicates = original_size - len(dataset)
        if num_duplicates > 0:
            strategies_applied.append(f"Deduplication (-{num_duplicates} samples)")
        sample_scores = [score_by_index[i] for i in keep_indices]

        # ---- Step 3: quality filter -----------------------------------
        dataset, keep_indices, num_filtered = self._quality_filter(
            dataset, sample_scores
        )
        if num_filtered > 0:
            strategies_applied.append(f"Quality Filtering (-{num_filtered} samples)")
        sample_scores = [sample_scores[i] for i in keep_indices]

        # ---- Step 4: active learning ----------------------------------
        target_size = max(1, int(original_size * target_compression))
        dataset, sample_scores, num_removed = self._active_learning_selection(
            dataset, sample_scores, target_size
        )
        if num_removed > 0:
            strategies_applied.append(f"Active Learning (-{num_removed} samples)")

        optimized_size = len(dataset)

        # ---- Step 5: synthetic augmentation ---------------------------
        synthetic_samples_added = 0
        if enable_synthetic and synthetic_ratio > 0 and optimized_size > 0:
            num_synthetic = max(1, int(optimized_size * synthetic_ratio))
            synthetic_data = self._generate_synthetic(
                dataset, num_synthetic, call_id=call_id
            )
            dataset = list(dataset) + synthetic_data
            synthetic_samples_added = len(synthetic_data)
            strategies_applied.append(
                f"Synthetic Augmentation (+{synthetic_samples_added} samples)"
            )

        # ---- Step 6: curriculum ordering ------------------------------
        dataset = self._curriculum_order(dataset, sample_scores)
        strategies_applied.append("Curriculum Ordering")

        # ---- Energy & carbon accounting -------------------------------
        final_size = len(dataset)
        compression_ratio = (
            original_size / optimized_size if optimized_size > 0 else 1.0
        )
        # Energy scales with *training-set size*, not with final size — the
        # synthetic samples are cheaper to consume than full-fidelity data,
        # but for the conservative estimate we scale by ``optimized_size``.
        energy_multiplier = (
            optimized_size / original_size if original_size > 0 else 1.0
        )
        optimized_energy_kwh = baseline_energy_kwh * energy_multiplier
        energy_saved_kwh = max(0.0, baseline_energy_kwh - optimized_energy_kwh)

        # ---- Quality retention heuristic ------------------------------
        cfg = self._config
        quality_loss = (1.0 - target_compression) * cfg.quality_loss_per_unit_compression
        base_quality = 1.0 - quality_loss
        if enable_synthetic and synthetic_ratio > 0:
            base_quality += synthetic_ratio * cfg.quality_gain_per_unit_synthetic
        quality_retention = max(0.0, min(1.0, base_quality))

        carbon_saved_kgco2e = energy_saved_kwh * cfg.carbon_intensity_kg_per_kwh

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
            original_size,
            final_size,
            compression_ratio,
            energy_saved_kwh,
            quality_retention * 100.0,
        )
        return dataset, result

    # ---------------------------------------------------------- sample IDs
    @staticmethod
    def _sample_id(sample: Mapping[str, Any], index: int) -> str:
        """
        Return the caller-supplied ``id`` when available, else a stable
        index-based ID. Using the index fallback keeps every sample
        addressable across the pipeline — previously, samples without an
        ``id`` were silently dropped by the quality filter.
        """
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
        self, dataset: List[Dict[str, Any]]
    ) -> List[SampleScore]:
        """Score each sample for informativeness, difficulty, quality, diversity."""
        cfg = self._config
        scores: List[SampleScore] = []

        for i, sample in enumerate(dataset):
            sample_id = self._sample_id(sample, i)
            text = self._sample_text(sample)

            informativeness = self._calculate_informativeness(text)
            difficulty = self._calculate_difficulty(text)
            quality = self._calculate_quality(text)
            diversity = self._calculate_diversity(text, i, dataset)

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

        return scores

    def _calculate_informativeness(self, text: str) -> float:
        """Estimate informativeness of sample (0–1)."""
        if not text:
            return 0.0
        words = text.split()
        if not words:
            return 0.0
        unique_ratio = len(set(words)) / len(words)
        length_score = min(
            1.0, len(words) / self._config.informativeness_length_normalizer
        )
        return (unique_ratio + length_score) / 2.0

    def _calculate_difficulty(self, text: str) -> float:
        """Estimate difficulty of sample (0–1)."""
        if not text:
            return 0.0
        words = text.split()
        if not words:
            return 0.0
        avg_word_length = sum(len(w) for w in words) / len(words)
        length_score = min(
            1.0, len(words) / self._config.difficulty_length_normalizer
        )
        complexity_score = min(
            1.0,
            avg_word_length / self._config.difficulty_word_length_normalizer,
        )
        return (length_score + complexity_score) / 2.0

    def _calculate_quality(self, text: str) -> float:
        """Estimate quality of sample (0–1)."""
        if not text:
            return 0.0
        noise_indicators = (
            "???", "...", "!!!", "###", "[deleted]", "[removed]",
        )
        lowered = text.lower()
        noise_count = sum(1 for ind in noise_indicators if ind in lowered)
        punct_ratio = (
            sum(1 for c in text if not c.isalnum()) / len(text)
            if text else 0.0
        )
        quality = 1.0 - (noise_count * 0.2) - (punct_ratio * 0.5)
        return max(0.0, min(1.0, quality))

    def _calculate_diversity(
        self,
        text: str,
        index: int,
        dataset: List[Dict[str, Any]],
    ) -> float:
        """
        Estimate diversity (0–1) via a bounded random peer sample.

        Previously this sampled the entire dataset per call — O(N²) total.
        Now bounded by ``config.diversity_sample_size``.
        """
        if not text:
            return 0.0
        if len(dataset) < 2:
            return 1.0

        text_hash = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()

        sample_size = min(self._config.diversity_sample_size, len(dataset) - 1)
        if sample_size <= 0:
            return 1.0

        # Deterministic peer selection via the seeded generator.
        peers = self._rng_numpy.choice(
            [j for j in range(len(dataset)) if j != index],
            size=sample_size,
            replace=False,
        )

        similarities: List[float] = []
        for peer in peers:
            peer_text = self._sample_text(dataset[int(peer)])
            peer_hash = hashlib.sha256(
                peer_text.encode("utf-8", errors="replace")
            ).hexdigest()
            similarity = sum(
                a == b for a, b in zip(text_hash, peer_hash)
            ) / len(text_hash)
            similarities.append(similarity)

        avg_similarity = (
            sum(similarities) / len(similarities) if similarities else 0.0
        )
        return max(0.0, min(1.0, 1.0 - avg_similarity))

    # ---------------------------------------------------------- dedup
    def _deduplicate(
        self, dataset: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[int]]:
        """
        Remove near-duplicate samples.

        Returns ``(deduplicated, keep_indices)`` where ``keep_indices`` maps
        each retained sample back to its original position. Returns full
        SHA-256 by default; ``config.dedup_hash_prefix`` truncates.
        """
        prefix = self._config.dedup_hash_prefix
        seen: set = set()
        deduplicated: List[Dict[str, Any]] = []
        keep_indices: List[int] = []
        num_duplicates = 0

        for i, sample in enumerate(dataset):
            text = self._sample_text(sample)
            digest = hashlib.sha256(
                text.encode("utf-8", errors="replace")
            ).hexdigest()
            if prefix is not None:
                digest = digest[:prefix]
            if digest not in seen:
                seen.add(digest)
                deduplicated.append(sample)
                keep_indices.append(i)
            else:
                num_duplicates += 1

        logger.info("Deduplication: removed %d duplicates.", num_duplicates)
        return deduplicated, keep_indices

    # ---------------------------------------------------------- quality filter
    def _quality_filter(
        self,
        dataset: List[Dict[str, Any]],
        scores: List[SampleScore],
    ) -> Tuple[List[Dict[str, Any]], List[int], int]:
        """
        Filter out low-quality samples.

        Returns ``(filtered, keep_indices, num_filtered)``. Matches scores
        positionally so samples without an ``id`` are not silently dropped.
        """
        filtered: List[Dict[str, Any]] = []
        keep_indices: List[int] = []
        num_filtered = 0

        for i, sample in enumerate(dataset):
            score = scores[i] if i < len(scores) else None
            if score is not None and score.quality >= self.min_quality_threshold:
                filtered.append(sample)
                keep_indices.append(i)
            else:
                num_filtered += 1

        logger.info(
            "Quality filtering: removed %d low-quality samples.", num_filtered
        )
        return filtered, keep_indices, num_filtered

    # ---------------------------------------------------------- active learning
    def _active_learning_selection(
        self,
        dataset: List[Dict[str, Any]],
        scores: List[SampleScore],
        target_size: int,
    ) -> Tuple[List[Dict[str, Any]], List[SampleScore], int]:
        """Select the top-``target_size`` samples by composite score."""
        if len(dataset) <= target_size:
            return dataset, scores, 0

        ranked = sorted(
            zip(dataset, scores),
            key=lambda pair: pair[1].composite_score,
            reverse=True,
        )
        selected_pairs = ranked[:target_size]
        selected_dataset = [s for s, _ in selected_pairs]
        selected_scores = [sc for _, sc in selected_pairs]
        num_removed = len(dataset) - len(selected_dataset)

        logger.info(
            "Active learning: selected %d/%d (removed %d).",
            len(selected_dataset), len(dataset), num_removed,
        )
        return selected_dataset, selected_scores, num_removed

    # ---------------------------------------------------------- synthetic
    def _generate_synthetic(
        self,
        dataset: List[Dict[str, Any]],
        num_synthetic: int,
        *,
        call_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Generate synthetic training samples.

        IDs are suffixed with ``call_id`` to prevent collisions across
        repeated ``optimize()`` invocations on the same dataset.
        """
        synthetic: List[Dict[str, Any]] = []
        if not dataset or num_synthetic <= 0:
            return synthetic

        num_paraphrase = max(1, int(num_synthetic * 0.5))
        for i in range(num_paraphrase):
            source = dataset[i % len(dataset)]
            synthetic.append({
                "id": f"synthetic_paraphrase_{call_id}_{i}",
                "text": self._paraphrase(self._sample_text(source)),
                "label": source.get("label"),
                "synthetic": True,
                "source": "paraphrase",
            })

        num_mixup = max(0, num_synthetic - num_paraphrase)
        if num_mixup > 0 and len(dataset) >= 2:
            for i in range(num_mixup):
                idx1, idx2 = self._rng_numpy.choice(
                    len(dataset), size=2, replace=False
                )
                source1 = dataset[int(idx1)]
                source2 = dataset[int(idx2)]
                synthetic.append({
                    "id": f"synthetic_mixup_{call_id}_{i}",
                    "text": self._mixup(
                        self._sample_text(source1),
                        self._sample_text(source2),
                    ),
                    "label": source1.get("label"),
                    "synthetic": True,
                    "source": "mixup",
                })

        logger.info("Generated %d synthetic samples.", len(synthetic))
        return synthetic

    def _paraphrase(self, text: str) -> str:
        """Simple paraphrase placeholder (swap in an LLM in production)."""
        return f"[Paraphrased] {text}"

    def _mixup(self, text1: str, text2: str) -> str:
        """Simple mixup placeholder (swap in an LLM in production)."""
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
    ) -> List[Dict[str, Any]]:
        """
        Order the dataset by difficulty (easy → hard).

        Synthetic samples have no score and are appended after the scored
        prefix. Previously they were silently dropped.
        """
        score_by_id = {s.sample_id: s for s in scores}

        scored: List[Tuple[Dict[str, Any], float]] = []
        unscored: List[Dict[str, Any]] = []

        for i, sample in enumerate(dataset):
            sid = self._sample_id(sample, i)
            score = score_by_id.get(sid)
            if score is None:
                unscored.append(sample)
            else:
                scored.append((sample, score.difficulty))

        scored.sort(key=lambda pair: pair[1])
        ordered = [s for s, _ in scored] + unscored

        logger.info(
            "Curriculum ordering: %d scored + %d unscored.",
            len(scored), len(unscored),
        )
        return ordered

    # ---------------------------------------------------------- statistics
    def get_statistics(self) -> Dict[str, Any]:
        """
        Return optimizer statistics.

        Backward-compatible keys (``total_compressions``,
        ``total_synthetic_generated``, ``total_energy_saved_kwh``,
        ``total_carbon_saved_kgco2e``) plus additive aggregates.
        """
        with self._lock:
            history = list(self._history)
            compressions = self.total_compressions
            synthetic = self.total_synthetic_generated
            energy = self.total_energy_saved_kwh

        cfg = self._config
        payload: Dict[str, Any] = {
            "total_compressions": compressions,
            "total_synthetic_generated": synthetic,
            "total_energy_saved_kwh": energy,
            "total_carbon_saved_kgco2e": energy * cfg.carbon_intensity_kg_per_kwh,
        }

        if history:
            payload.update({
                "mean_compression_ratio": (
                    sum(r.compression_ratio for r in history) / len(history)
                ),
                "mean_quality_retention": (
                    sum(r.estimated_quality_retention for r in history)
                    / len(history)
                ),
                "mean_energy_saved_kwh": (
                    sum(r.estimated_energy_savings_kwh for r in history)
                    / len(history)
                ),
                "last_result": history[-1].to_dict(),
            })
        return payload

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_statistics` (consistent with other modules)."""
        return self.get_statistics()

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset counters; optionally clear the run history."""
        with self._lock:
            self.total_compressions = 0
            self.total_synthetic_generated = 0
            self.total_energy_saved_kwh = 0.0
            if clear_history:
                self._history.clear()
        logger.debug("SyntheticDataOptimizer reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "total_compressions": self.total_compressions,
                    "total_synthetic_generated": self.total_synthetic_generated,
                    "total_energy_saved_kwh": self.total_energy_saved_kwh,
                },
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SyntheticDataOptimizer":
        if not isinstance(data, Mapping):
            raise SyntheticDataOptimizerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = SyntheticOptimizerConfig(
            min_quality_threshold=float(
                cfg_data.get("min_quality_threshold", 0.3)
            ),
            max_similarity_threshold=float(
                cfg_data.get("max_similarity_threshold", 0.95)
            ),
            informativeness_threshold=float(
                cfg_data.get("informativeness_threshold", 0.4)
            ),
            weight_informativeness=float(
                cfg_data.get("weight_informativeness", 0.3)
            ),
            weight_difficulty=float(cfg_data.get("weight_difficulty", 0.2)),
            weight_quality=float(cfg_data.get("weight_quality", 0.3)),
            weight_diversity=float(cfg_data.get("weight_diversity", 0.2)),
            diversity_sample_size=int(cfg_data.get("diversity_sample_size", 100)),
            dedup_hash_prefix=cfg_data.get("dedup_hash_prefix", 16),
            quality_loss_per_unit_compression=float(
                cfg_data.get("quality_loss_per_unit_compression", 0.3)
            ),
            quality_gain_per_unit_synthetic=float(
                cfg_data.get("quality_gain_per_unit_synthetic", 0.1)
            ),
            carbon_intensity_kg_per_kwh=float(
                cfg_data.get("carbon_intensity_kg_per_kwh", 0.4)
            ),
            seed=cfg_data.get("seed", 42),
            max_history=int(cfg_data.get("max_history", 1000)),
        )
        opt = cls(config=cfg, strict=bool(data.get("strict", True)))
        with opt._lock:
            counters = dict(data.get("counters", {}) or {})
            opt.total_compressions = int(counters.get("total_compressions", 0))
            opt.total_synthetic_generated = int(
                counters.get("total_synthetic_generated", 0)
            )
            opt.total_energy_saved_kwh = float(
                counters.get("total_energy_saved_kwh", 0.0)
            )
            for entry in data.get("history", []):
                opt._history.append(DataOptimizationResult.from_dict(entry))
        return opt

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SyntheticDataOptimizer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise SyntheticDataOptimizerError(
                f"Invalid JSON payload: {exc}"
            ) from exc

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
                f"energy_saved_kwh={self.total_energy_saved_kwh:.3f}, "
                f"seed={self._config.seed}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CompressionStrategy",
    "SyntheticStrategy",
    "SyntheticOptimizerConfig",
    "DataOptimizationResult",
    "SampleScore",
    "SyntheticDataOptimizer",
    "SyntheticDataOptimizerError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m optimization.synthetic_data_optimizer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    optimizer = SyntheticDataOptimizer()
    print("repr       :", optimizer)

    # ---- Build a synthetic dataset -------------------------------- #
    dataset: List[Dict[str, Any]] = [
        {
            "id": f"sample_{i}",
            "text": f"This is training sample {i} with some varied content.",
            "label": i % 2,
        }
        for i in range(5_000)
    ]
    # Add duplicates.
    dataset.extend(dataset[:100])
    # Add low-quality samples.
    for i in range(50):
        dataset.append({
            "id": f"noisy_{i}",
            "text": "??? ### [deleted]",
            "label": 0,
        })
    # Add samples with *no* id — previously dropped by the quality filter.
    for i in range(20):
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

    # ---- optimize_dataset (returns the list too) ------------------ #
    optimized, result2 = optimizer.optimize_dataset(
        dataset=dataset,
        target_compression=0.3,
        synthetic_ratio=0.1,
    )
    print(f"optimized  : {len(optimized)} samples (id in first: "
          f"{'id' in optimized[0] if optimized else False})")
    print(f"result2    : {result2}")

    # ---- Reproducibility (seeded) --------------------------------- #
    a = SyntheticDataOptimizer(config=SyntheticOptimizerConfig(seed=7))
    b = SyntheticDataOptimizer(config=SyntheticOptimizerConfig(seed=7))
    ra = a.optimize(dataset, target_compression=0.4, synthetic_ratio=0.2)
    rb = b.optimize(dataset, target_compression=0.4, synthetic_ratio=0.2)
    assert ra.to_dict() == rb.to_dict(), "seeded runs must be identical"
    print("reproducibility OK.")

    # ---- Synthetic ID uniqueness across calls --------------------- #
    opt_ids = SyntheticDataOptimizer()
    _, r1 = opt_ids.optimize_dataset(dataset, synthetic_ratio=0.1)
    _, r2 = opt_ids.optimize_dataset(dataset, synthetic_ratio=0.1)
    ids_1 = {s["id"] for s in r1 if s.get("synthetic")}
    ids_2 = {s["id"] for s in r2 if s.get("synthetic")}
    assert not (ids_1 & ids_2), "synthetic IDs must not collide"
    print("synthetic ID uniqueness OK.")

    # ---- Statistics ----------------------------------------------- #
    stats = optimizer.get_statistics()
    print("stats      :", {
        k: v for k, v in stats.items() if k != "last_result"
    })

    # ---- Serialization round-trip --------------------------------- #
    payload = optimizer.to_json()
    restored = SyntheticDataOptimizer.from_json(payload)
    assert restored.to_dict() == optimizer.to_dict()
    print("Round-trip OK.")

    # ---- Tiny dataset (previously crashed) ------------------------ #
    tiny = SyntheticDataOptimizer()
    tiny_result = tiny.optimize(
        [{"id": "only", "text": "single sample", "label": 0}],
        target_compression=1.0,
        synthetic_ratio=0.0,
    )
    print("tiny       :", tiny_result)

    # ---- Validation failures -------------------------------------- #
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
        dict(weight_informativeness=0.5),  # weights won't sum to 1.0
        dict(diversity_sample_size=0),
        dict(max_history=0),
    ):
        try:
            SyntheticOptimizerConfig(**bad_cfg)  # type: ignore[arg-type]
        except SyntheticDataOptimizerError as exc:
            print("Rejected cfg:", exc)

    print("\nSmoke test passed.")
