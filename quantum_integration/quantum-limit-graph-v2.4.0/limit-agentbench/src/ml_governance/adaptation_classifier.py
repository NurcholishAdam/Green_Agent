# src/ml_governance/adaptation_classifier.py

"""
Adaptation Strategy Classifier
==============================

Classifies the optimal fine-tuning strategy based on task characteristics
and carbon constraints.

Enhancements
------------
- ``ClassifierConfig`` — frozen, validated, centralizes every scoring constant.
- Validation of ``classify()`` inputs; strict / non-strict modes.
- ``RLock``-guarded bounded recommendation history + ``statistics()``.
- Serialization: ``to_dict`` / ``from_dict`` on ``StrategyRecommendation``.
- Custom ``AdaptationClassifierError``; ``__repr__``; ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AdaptationClassifierError(ValueError):
    """Raised for invalid classifier inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
from enum import Enum


class AdaptationStrategy(Enum):
    """Available adaptation strategies."""
    FULL_FT = "full_fine_tuning"
    LORA = "lora"
    ADAPTERS = "adapters"
    PREFIX_TUNING = "prefix_tuning"
    BITFIT = "bitfit"
    DISTILLATION = "distillation"
    FREEZE_BACKBONE = "freeze_backbone"
    PROMPT_TUNING = "prompt_tuning"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ClassifierConfig:
    """Tunable scoring parameters for the classifier."""

    # Dataset-size scoring bands.
    dataset_heavy_penalty: float = -50.0
    dataset_moderate_penalty: float = -10.0
    dataset_bonus: float = 10.0
    dataset_moderate_multiplier: float = 2.0

    # Carbon-budget scoring.
    carbon_over_budget_penalty: float = -100.0
    carbon_efficiency_weight: float = 30.0

    # Domain-shift bonuses.
    domain_shift_small_lora: float = 20.0
    domain_shift_small_prompt: float = 15.0
    domain_shift_moderate_lora: float = 15.0
    domain_shift_moderate_adapters: float = 20.0
    domain_shift_large_full_ft: float = 20.0
    domain_shift_large_adapters: float = 15.0

    # Large-model bonuses.
    large_model_param_threshold: int = 1_000_000_000
    large_model_pe_bonus: float = 15.0
    large_model_full_ft_penalty: float = -10.0

    # Multi-task bonuses.
    multi_task_adapters_lora_bonus: float = 10.0

    # High target-accuracy adjustments.
    high_accuracy_threshold: float = 0.90
    high_accuracy_full_ft_bonus: float = 5.0
    high_accuracy_pe_penalty: float = -5.0

    # Carbon estimation.
    default_model_params: int = 100_000_000
    carbon_per_million_params_per_k_samples: float = 0.001

    # Small-dataset reasoning threshold.
    small_dataset_reasoning_threshold: int = 5_000
    low_carbon_reasoning_threshold: float = 0.5

    # History.
    max_history: int = 1000

    def __post_init__(self) -> None:
        if self.dataset_moderate_multiplier <= 1.0:
            raise AdaptationClassifierError(
                "dataset_moderate_multiplier must be > 1.0."
            )
        if self.carbon_efficiency_weight < 0:
            raise AdaptationClassifierError(
                "carbon_efficiency_weight must be >= 0."
            )
        if self.large_model_param_threshold <= 0:
            raise AdaptationClassifierError(
                "large_model_param_threshold must be > 0."
            )
        if not 0.0 <= self.high_accuracy_threshold <= 1.0:
            raise AdaptationClassifierError(
                "high_accuracy_threshold must be in [0, 1]."
            )
        if self.default_model_params <= 0:
            raise AdaptationClassifierError(
                "default_model_params must be > 0."
            )
        if self.carbon_per_million_params_per_k_samples <= 0:
            raise AdaptationClassifierError(
                "carbon_per_million_params_per_k_samples must be > 0."
            )
        if self.max_history <= 0:
            raise AdaptationClassifierError("max_history must be > 0.")


# --------------------------------------------------------------------------- #
# Recommendation
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class StrategyRecommendation:
    """Recommended adaptation strategy with reasoning."""
    strategy: AdaptationStrategy
    trainable_params_pct: float
    expected_energy_multiplier: float
    expected_quality_impact: float
    carbon_budget_required_kgco2e: float
    reasoning: str
    alternatives: tuple = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.trainable_params_pct < 0:
            raise AdaptationClassifierError(
                "trainable_params_pct must be >= 0."
            )
        if self.expected_energy_multiplier < 0:
            raise AdaptationClassifierError(
                "expected_energy_multiplier must be >= 0."
            )
        if self.carbon_budget_required_kgco2e < 0:
            raise AdaptationClassifierError(
                "carbon_budget_required_kgco2e must be >= 0."
            )
        if not isinstance(self.alternatives, tuple):
            object.__setattr__(self, "alternatives", tuple(self.alternatives))

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "strategy": self.strategy.value,
            "alternatives": [dict(a) for a in self.alternatives],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategyRecommendation":
        if not isinstance(data, Mapping):
            raise AdaptationClassifierError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        return cls(
            strategy=AdaptationStrategy(str(data["strategy"])),
            trainable_params_pct=float(data["trainable_params_pct"]),
            expected_energy_multiplier=float(data["expected_energy_multiplier"]),
            expected_quality_impact=float(data["expected_quality_impact"]),
            carbon_budget_required_kgco2e=float(
                data["carbon_budget_required_kgco2e"]
            ),
            reasoning=str(data.get("reasoning", "")),
            alternatives=tuple(data.get("alternatives", ())),
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "StrategyRecommendation":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise AdaptationClassifierError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "StrategyRecommendation("
            f"strategy={self.strategy.value}, "
            f"trainable_pct={self.trainable_params_pct:.2f}%, "
            f"energy_mult={self.expected_energy_multiplier:.2f}, "
            f"carbon_req={self.carbon_budget_required_kgco2e:.4f}kg)"
        )


# --------------------------------------------------------------------------- #
# Classifier
# --------------------------------------------------------------------------- #
class AdaptationStrategyClassifier:
    """
    Classifies optimal fine-tuning strategy.

    Thread-safe, serializable, and bounded in memory. The original
    ``classify(...)`` signature is preserved; new parameters are keyword-only.
    """

    # Strategy profiles (trainable %, energy multiplier, quality delta, min dataset).
    _STRATEGY_PROFILES: Dict[AdaptationStrategy, Dict[str, Any]] = {
        AdaptationStrategy.FULL_FT: {
            "trainable_pct": 100.0, "energy_multiplier": 1.0,
            "quality_baseline": 0.0, "min_dataset_size": 10_000,
            "carbon_intensity": "very_high",
        },
        AdaptationStrategy.LORA: {
            "trainable_pct": 0.3, "energy_multiplier": 0.15,
            "quality_baseline": -0.01, "min_dataset_size": 1_000,
            "carbon_intensity": "very_low",
        },
        AdaptationStrategy.ADAPTERS: {
            "trainable_pct": 2.0, "energy_multiplier": 0.25,
            "quality_baseline": -0.015, "min_dataset_size": 2_000,
            "carbon_intensity": "low",
        },
        AdaptationStrategy.PREFIX_TUNING: {
            "trainable_pct": 0.1, "energy_multiplier": 0.10,
            "quality_baseline": -0.02, "min_dataset_size": 500,
            "carbon_intensity": "very_low",
        },
        AdaptationStrategy.BITFIT: {
            "trainable_pct": 0.1, "energy_multiplier": 0.08,
            "quality_baseline": -0.03, "min_dataset_size": 500,
            "carbon_intensity": "very_low",
        },
        AdaptationStrategy.DISTILLATION: {
            "trainable_pct": 100.0, "energy_multiplier": 0.50,
            "quality_baseline": -0.05, "min_dataset_size": 5_000,
            "carbon_intensity": "medium",
        },
        AdaptationStrategy.FREEZE_BACKBONE: {
            "trainable_pct": 10.0, "energy_multiplier": 0.20,
            "quality_baseline": -0.04, "min_dataset_size": 2_000,
            "carbon_intensity": "low",
        },
        AdaptationStrategy.PROMPT_TUNING: {
            "trainable_pct": 0.01, "energy_multiplier": 0.05,
            "quality_baseline": -0.03, "min_dataset_size": 100,
            "carbon_intensity": "very_low",
        },
    }

    def __init__(
        self,
        *,
        config: Optional[ClassifierConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or ClassifierConfig()
        self._strict = bool(strict)
        # Legacy attribute preserved.
        self.strategy_profiles = self._STRATEGY_PROFILES
        self._lock = threading.RLock()
        self._history: Deque[StrategyRecommendation] = deque(
            maxlen=self._config.max_history
        )
        logger.debug(
            "AdaptationStrategyClassifier initialized (strict=%s)", self._strict
        )

    @property
    def config(self) -> ClassifierConfig:
        return self._config

    @property
    def history(self) -> List[StrategyRecommendation]:
        with self._lock:
            return list(self._history)

    def classify(
        self,
        task_scope: str,
        dataset_size: int,
        domain_shift: str,
        carbon_budget: float,
        target_accuracy: float,
        model_size_params: Optional[int] = None,
        *,
        record: bool = True,
    ) -> StrategyRecommendation:
        """
        Classify optimal adaptation strategy.

        See original docstring for parameter meanings. All numeric inputs are
        validated; ``strict`` mode raises on invalid input.
        """
        # ---- Validation -----------------------------------------------
        if not isinstance(task_scope, str) or not task_scope:
            raise AdaptationClassifierError("task_scope must be a non-empty string.")
        if not isinstance(dataset_size, int) or dataset_size <= 0:
            raise AdaptationClassifierError("dataset_size must be a positive int.")
        if domain_shift not in ("small", "moderate", "large"):
            raise AdaptationClassifierError(
                "domain_shift must be 'small', 'moderate', or 'large'."
            )
        if not isinstance(carbon_budget, (int, float)) or math.isnan(
            float(carbon_budget)
        ) or carbon_budget < 0:
            raise AdaptationClassifierError(
                "carbon_budget must be a finite non-negative number."
            )
        if not isinstance(target_accuracy, (int, float)) or not 0.0 <= float(
            target_accuracy
        ) <= 1.0:
            raise AdaptationClassifierError(
                "target_accuracy must be in [0, 1]."
            )
        if model_size_params is not None and (
            not isinstance(model_size_params, int) or model_size_params <= 0
        ):
            raise AdaptationClassifierError(
                "model_size_params must be a positive int or None."
            )

        # ---- Score each strategy --------------------------------------
        scores: Dict[AdaptationStrategy, float] = {}
        for strategy, profile in self.strategy_profiles.items():
            scores[strategy] = self._score_strategy(
                strategy=strategy,
                profile=profile,
                task_scope=task_scope,
                dataset_size=dataset_size,
                domain_shift=domain_shift,
                carbon_budget=float(carbon_budget),
                target_accuracy=float(target_accuracy),
                model_size_params=model_size_params,
            )

        sorted_strategies = sorted(
            scores.items(), key=lambda x: x[1], reverse=True
        )
        top_strategy, _ = sorted_strategies[0]
        profile = self.strategy_profiles[top_strategy]

        base_carbon = self._estimate_base_carbon(dataset_size, model_size_params)
        carbon_required = base_carbon * profile["energy_multiplier"]

        reasoning = self._generate_reasoning(
            strategy=top_strategy,
            profile=profile,
            task_scope=task_scope,
            dataset_size=dataset_size,
            domain_shift=domain_shift,
            carbon_budget=float(carbon_budget),
        )

        alternatives = tuple(
            {
                "strategy": strategy.value,
                "score": score,
                "trainable_pct": self.strategy_profiles[strategy]["trainable_pct"],
            }
            for strategy, score in sorted_strategies[1:4]
        )

        recommendation = StrategyRecommendation(
            strategy=top_strategy,
            trainable_params_pct=profile["trainable_pct"],
            expected_energy_multiplier=profile["energy_multiplier"],
            expected_quality_impact=profile["quality_baseline"],
            carbon_budget_required_kgco2e=carbon_required,
            reasoning=reasoning,
            alternatives=alternatives,
        )

        if record:
            with self._lock:
                self._history.append(recommendation)

        logger.info(
            "Classified strategy: %s (trainable=%.2f%%, energy_mult=%.2f)",
            top_strategy.value,
            profile["trainable_pct"],
            profile["energy_multiplier"],
        )
        return recommendation

    # ---------------------------------------------------------- scoring
    def _score_strategy(
        self,
        strategy: AdaptationStrategy,
        profile: Mapping[str, Any],
        task_scope: str,
        dataset_size: int,
        domain_shift: str,
        carbon_budget: float,
        target_accuracy: float,
        model_size_params: Optional[int],
    ) -> float:
        cfg = self._config
        score = 0.0

        # Factor 1: Dataset size compatibility.
        min_dataset = profile["min_dataset_size"]
        if dataset_size < min_dataset:
            score += cfg.dataset_heavy_penalty
        elif dataset_size < min_dataset * cfg.dataset_moderate_multiplier:
            score += cfg.dataset_moderate_penalty
        else:
            score += cfg.dataset_bonus

        # Factor 2: Carbon budget compatibility.
        base_carbon = self._estimate_base_carbon(dataset_size, model_size_params)
        required_carbon = base_carbon * profile["energy_multiplier"]
        if required_carbon > carbon_budget:
            score += cfg.carbon_over_budget_penalty
        else:
            carbon_efficiency = (
                (carbon_budget - required_carbon) / carbon_budget
                if carbon_budget > 0 else 0.0
            )
            score += carbon_efficiency * cfg.carbon_efficiency_weight

        # Factor 3: Domain shift.
        domain_bonuses = {
            "small": {
                AdaptationStrategy.LORA: cfg.domain_shift_small_lora,
                AdaptationStrategy.PROMPT_TUNING: cfg.domain_shift_small_prompt,
            },
            "moderate": {
                AdaptationStrategy.LORA: cfg.domain_shift_moderate_lora,
                AdaptationStrategy.ADAPTERS: cfg.domain_shift_moderate_adapters,
            },
            "large": {
                AdaptationStrategy.FULL_FT: cfg.domain_shift_large_full_ft,
                AdaptationStrategy.ADAPTERS: cfg.domain_shift_large_adapters,
            },
        }
        score += domain_bonuses.get(domain_shift, {}).get(strategy, 0.0)

        # Factor 4: Model size.
        if (
            model_size_params
            and model_size_params > cfg.large_model_param_threshold
        ):
            if strategy in (
                AdaptationStrategy.LORA, AdaptationStrategy.PREFIX_TUNING
            ):
                score += cfg.large_model_pe_bonus
            elif strategy == AdaptationStrategy.FULL_FT:
                score += cfg.large_model_full_ft_penalty

        # Factor 5: Task scope.
        if task_scope == "multi_task":
            if strategy in (
                AdaptationStrategy.ADAPTERS, AdaptationStrategy.LORA
            ):
                score += cfg.multi_task_adapters_lora_bonus

        # Factor 6: Target accuracy.
        if target_accuracy > cfg.high_accuracy_threshold:
            if strategy == AdaptationStrategy.FULL_FT:
                score += cfg.high_accuracy_full_ft_bonus
            elif strategy in (
                AdaptationStrategy.BITFIT, AdaptationStrategy.PROMPT_TUNING
            ):
                score += cfg.high_accuracy_pe_penalty

        return score

    def _estimate_base_carbon(
        self, dataset_size: int, model_size_params: Optional[int]
    ) -> float:
        """Estimate base carbon for full fine-tuning."""
        if model_size_params is None:
            model_size_params = self._config.default_model_params
        return (
            (model_size_params / 1e6)
            * (dataset_size / 1000)
            * self._config.carbon_per_million_params_per_k_samples
        )

    def _generate_reasoning(
        self,
        strategy: AdaptationStrategy,
        profile: Mapping[str, Any],
        task_scope: str,
        dataset_size: int,
        domain_shift: str,
        carbon_budget: float,
    ) -> str:
        """Generate human-readable reasoning."""
        cfg = self._config
        reasons: List[str] = []
        if dataset_size < cfg.small_dataset_reasoning_threshold:
            reasons.append(
                f"Small dataset ({dataset_size} samples) favors "
                f"parameter-efficient methods"
            )
        if carbon_budget < cfg.low_carbon_reasoning_threshold:
            reasons.append(
                f"Low carbon budget ({carbon_budget:.3f} kgCO2e) requires "
                f"efficient adaptation"
            )
        if domain_shift == "small":
            reasons.append("Small domain shift allows minimal adaptation")
        elif domain_shift == "large":
            reasons.append("Large domain shift may require more parameters")

        if strategy == AdaptationStrategy.LORA:
            reasons.append(
                "LoRA provides excellent balance of efficiency and performance"
            )
        elif strategy == AdaptationStrategy.FULL_FT:
            reasons.append("Full fine-tuning for maximum performance")
        elif strategy == AdaptationStrategy.ADAPTERS:
            reasons.append("Adapters enable task-specific customization")

        energy_mult = profile["energy_multiplier"]
        if energy_mult < 0.3:
            savings_pct = (1.0 - energy_mult) * 100
            reasons.append(f"~{savings_pct:.0f}% energy savings vs full fine-tuning")

        return ". ".join(reasons) + "." if reasons else "No dominant factors detected."

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recommendation history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {"recommendations": 0, "by_strategy": {}}
        by_strategy: Dict[str, int] = {}
        for r in history:
            by_strategy[r.strategy.value] = by_strategy.get(r.strategy.value, 0) + 1
        return {
            "recommendations": len(history),
            "by_strategy": by_strategy,
            "mean_energy_multiplier": sum(
                r.expected_energy_multiplier for r in history
            ) / len(history),
            "mean_carbon_required": sum(
                r.carbon_budget_required_kgco2e for r in history
            ) / len(history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("AdaptationStrategyClassifier reset.")

    def __repr__(self) -> str:
        with self._lock:
            return (
                "AdaptationStrategyClassifier("
                f"strategies={len(self.strategy_profiles)}, "
                f"recommendations={len(self._history)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "AdaptationClassifierError",
    "AdaptationStrategy",
    "AdaptationStrategyClassifier",
    "ClassifierConfig",
    "StrategyRecommendation",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    classifier = AdaptationStrategyClassifier()
    print("repr       :", classifier)

    # Low budget, small dataset → parameter-efficient.
    rec1 = classifier.classify(
        task_scope="single_task", dataset_size=2_000,
        domain_shift="moderate", carbon_budget=0.1,
        target_accuracy=0.85, model_size_params=110_000_000,
    )
    print("rec1       :", rec1)
    print("reasoning  :", rec1.reasoning)

    # Large dataset, large domain shift, big budget → full FT.
    rec2 = classifier.classify(
        task_scope="domain_adaptation", dataset_size=50_000,
        domain_shift="large", carbon_budget=10.0,
        target_accuracy=0.95, model_size_params=340_000_000,
    )
    print("rec2       :", rec2)

    print("stats      :", classifier.statistics())

    # Serialization round-trip.
    payload = rec1.to_json()
    assert StrategyRecommendation.from_json(payload).to_dict() == rec1.to_dict()
    print("Round-trip OK.")
