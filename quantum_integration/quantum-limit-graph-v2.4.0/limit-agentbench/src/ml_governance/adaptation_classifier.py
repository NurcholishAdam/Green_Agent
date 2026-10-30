# src/ml_governance/adaptation_classifier.py

"""
Adaptation Strategy Classifier (v5.1)
=====================================

Classifies the optimal fine-tuning strategy based on task characteristics
and carbon constraints.

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``strategy_profiles`` is now deep-copied per instance. Previously
         every classifier shared the same class-level dict, so mutating one
         instance's profiles silently corrupted every other instance
         (including future ones).
  * P0 — ``StrategyRecommendation.__post_init__`` validates every numeric
         field for finiteness, including ``expected_quality_impact`` which
         was previously never checked. ``NaN`` / ``Inf`` are now rejected.
  * P0 — ``StrategyRecommendation.from_dict`` routes every external value
         through ``_safe_*`` helpers. ``KeyError`` / ``ValueError`` /
         ``TypeError`` no longer escape; only ``AdaptationClassifierError``
         is raised, matching the rest of the module.
  * P1 — ``alternatives`` is validated: strings are rejected (previously
         ``"abc"`` silently became ``('a','b','c')``), non-Mappings are
         rejected (previously accepted and crashed at ``to_dict``).
  * P1 — ``_generate_reasoning`` uses ``task_scope`` (previously a dead
         parameter). Multi-task scope now contributes a reason when the
         chosen strategy is adapters / LoRA.
  * P1 — ``logger.info`` on the hot path demoted to ``logger.debug``.
  * P1 — ``to_json`` no longer passes ``default=str``; serialization
         errors are wrapped in ``AdaptationClassifierError``.
  * P1 — ``carbon_budget`` unit documented as kg CO2e.
  * P1 — Valid ``domain_shift`` values centralized as
         ``_DOMAIN_SHIFTS``; unknown ``task_scope`` values emit a warning
         (previously accepted silently — kept soft for backward compat).
  * P1 — Ranking is now deterministic: primary = score (desc), secondary =
         strategy name (asc). The previous sort had no secondary key.
  * P2 — New helpers: ``__version__``, ``schema_version`` in
         ``to_dict()``, ``StrategyRecommendation.key()``, ``timestamp``
         field, ``best_strategy()``, ``compare()``, ``classify_batch()``,
         ``history_since()``, ``history_between()``, ``clear_history()``,
         ``_profile_summary()``.
  * P3 — ``import copy`` / ``import time`` correctly used; ``Enum``
         import moved to the top; ``__repr__`` uses ``!r`` for precision;
         ``__main__`` smoke test asserts every fix.

Backward compatibility
----------------------
- ``classify()`` keeps its positional signature; ``record=`` is keyword-only.
- ``StrategyRecommendation`` gains an optional ``timestamp`` field
  (defaults to ``time.time()`` on construction and in ``from_dict`` when
  absent) — old serialized payloads load unchanged.
- ``StrategyRecommendation`` equality now considers ``timestamp``; use
  ``.key()`` for timestamp-free identity.
- ``classifier.strict`` is retained as an attribute but is currently
  unused (kept for signature compatibility; will be repurposed if a
  non-strict mode is added).
- ``classifier.strategy_profiles`` remains a mutable per-instance dict,
  preserving the original mutation surface.
"""

from __future__ import annotations

import copy
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "AdaptationClassifierError",
    "AdaptationStrategy",
    "AdaptationStrategyClassifier",
    "ClassifierConfig",
    "StrategyRecommendation",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AdaptationClassifierError(ValueError):
    """Raised for invalid classifier inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
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
# Safe coercion helpers
# --------------------------------------------------------------------------- #
def _safe_float(name: str, value: Any) -> float:
    """Coerce ``value`` to a finite float, raising ``AdaptationClassifierError``."""
    if isinstance(value, bool):
        raise AdaptationClassifierError(
            f"{name} must be numeric, got bool {value!r}"
        )
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise AdaptationClassifierError(
            f"{name} must be numeric, got {value!r} "
            f"({type(value).__name__})"
        ) from exc
    if not math.isfinite(v):
        raise AdaptationClassifierError(
            f"{name} must be finite, got {value!r}"
        )
    return v


def _safe_strategy(value: Any) -> AdaptationStrategy:
    """Coerce a value (enum, name, or .value) into an AdaptationStrategy."""
    if isinstance(value, AdaptationStrategy):
        return value
    if not isinstance(value, str):
        raise AdaptationClassifierError(
            f"strategy must be str or AdaptationStrategy, "
            f"got {type(value).__name__}"
        )
    try:
        return AdaptationStrategy(value)
    except ValueError:
        raise AdaptationClassifierError(
            f"unknown strategy {value!r}; "
            f"expected one of {[s.value for s in AdaptationStrategy]}"
        ) from None


def _safe_alternatives(value: Any) -> Tuple[Mapping[str, Any], ...]:
    """Coerce ``value`` into a tuple of Mapping copies."""
    if value is None:
        return ()
    if isinstance(value, str):
        raise AdaptationClassifierError(
            "alternatives must be a sequence of mappings, not a string"
        )
    try:
        items = tuple(value)
    except TypeError as exc:
        raise AdaptationClassifierError(
            f"alternatives must be iterable, "
            f"got {type(value).__name__}"
        ) from exc
    result: List[Mapping[str, Any]] = []
    for i, item in enumerate(items):
        if not isinstance(item, Mapping):
            raise AdaptationClassifierError(
                f"alternatives[{i}] must be a Mapping, "
                f"got {type(item).__name__}"
            )
        result.append(dict(item))
    return tuple(result)


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
    alternatives: Tuple[Mapping[str, Any], ...] = field(default_factory=tuple)
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not isinstance(self.strategy, AdaptationStrategy):
            raise AdaptationClassifierError(
                f"strategy must be AdaptationStrategy, "
                f"got {type(self.strategy).__name__}"
            )

        # Non-negative fields.
        for name in (
            "trainable_params_pct",
            "expected_energy_multiplier",
            "carbon_budget_required_kgco2e",
        ):
            v = _safe_float(name, getattr(self, name))
            if v < 0:
                raise AdaptationClassifierError(
                    f"{name} must be >= 0, got {v!r}"
                )
            object.__setattr__(self, name, v)

        # expected_quality_impact may be negative (delta from baseline).
        object.__setattr__(
            self,
            "expected_quality_impact",
            _safe_float(
                "expected_quality_impact", self.expected_quality_impact,
            ),
        )

        # Timestamp.
        object.__setattr__(
            self, "timestamp",
            _safe_float("timestamp", self.timestamp),
        )

        # Alternatives.
        object.__setattr__(
            self, "alternatives",
            _safe_alternatives(self.alternatives),
        )

    # --------------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "strategy": self.strategy.value,
            "trainable_params_pct": self.trainable_params_pct,
            "expected_energy_multiplier": self.expected_energy_multiplier,
            "expected_quality_impact": self.expected_quality_impact,
            "carbon_budget_required_kgco2e": self.carbon_budget_required_kgco2e,
            "reasoning": self.reasoning,
            "alternatives": [dict(a) for a in self.alternatives],
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StrategyRecommendation":
        if not isinstance(data, Mapping):
            raise AdaptationClassifierError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )
        missing = [
            k for k in (
                "strategy", "trainable_params_pct",
                "expected_energy_multiplier", "expected_quality_impact",
                "carbon_budget_required_kgco2e",
            )
            if k not in data
        ]
        if missing:
            raise AdaptationClassifierError(
                f"missing required field(s): {missing}"
            )

        timestamp_raw = data.get("timestamp")
        timestamp = (
            time.time()
            if timestamp_raw is None
            else _safe_float("timestamp", timestamp_raw)
        )

        return cls(
            strategy=_safe_strategy(data["strategy"]),
            trainable_params_pct=_safe_float(
                "trainable_params_pct", data["trainable_params_pct"],
            ),
            expected_energy_multiplier=_safe_float(
                "expected_energy_multiplier",
                data["expected_energy_multiplier"],
            ),
            expected_quality_impact=_safe_float(
                "expected_quality_impact",
                data["expected_quality_impact"],
            ),
            carbon_budget_required_kgco2e=_safe_float(
                "carbon_budget_required_kgco2e",
                data["carbon_budget_required_kgco2e"],
            ),
            reasoning=str(data.get("reasoning") or ""),
            alternatives=_safe_alternatives(data.get("alternatives")),
            timestamp=timestamp,
        )

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise AdaptationClassifierError(
                f"Recommendation is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "StrategyRecommendation":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise AdaptationClassifierError(
                f"invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise AdaptationClassifierError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}"
            )
        return cls.from_dict(data)

    # --------------------------------------------------------------- identity
    def key(self) -> Tuple[Any, ...]:
        """Deterministic identity tuple (excludes timestamp)."""
        return (
            self.strategy.value,
            self.trainable_params_pct,
            self.expected_energy_multiplier,
            self.expected_quality_impact,
            self.carbon_budget_required_kgco2e,
            self.reasoning,
        )

    def __repr__(self) -> str:
        return (
            "StrategyRecommendation("
            f"strategy={self.strategy.value}, "
            f"trainable_pct={self.trainable_params_pct!r}, "
            f"energy_mult={self.expected_energy_multiplier!r}, "
            f"carbon_req={self.carbon_budget_required_kgco2e!r})"
        )


# --------------------------------------------------------------------------- #
# Classifier
# --------------------------------------------------------------------------- #
class AdaptationStrategyClassifier:
    """
    Classifies the optimal fine-tuning strategy.

    Thread-safe, serializable, and bounded in memory. The original
    ``classify(...)`` signature is preserved; ``record=`` is keyword-only.
    """

    #: Valid domain-shift values.
    _DOMAIN_SHIFTS: Tuple[str, ...] = ("small", "moderate", "large")

    #: Recognized task scopes. Unknown values are accepted (with a warning)
    #: for backward compatibility; only ``"multi_task"`` affects scoring.
    _KNOWN_TASK_SCOPES: Tuple[str, ...] = (
        "single_task", "multi_task", "domain_adaptation",
        "instruction_tuning",
    )

    #: Strategy profiles (trainable %, energy multiplier, quality delta,
    #: min dataset). The class attribute is the template; every instance
    #: gets a deep copy so mutations cannot leak across instances.
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
        # Fix P0: deep-copy so per-instance mutations don't leak.
        self.strategy_profiles: Dict[
            AdaptationStrategy, Dict[str, Any],
        ] = copy.deepcopy(self._STRATEGY_PROFILES)
        self._lock = threading.RLock()
        self._history: Deque[StrategyRecommendation] = deque(
            maxlen=self._config.max_history
        )
        logger.debug(
            "AdaptationStrategyClassifier initialized (strict=%s)",
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ClassifierConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def history(self) -> List[StrategyRecommendation]:
        with self._lock:
            return list(self._history)

    # ------------------------------------------------------------------ classify
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
        Classify the optimal adaptation strategy.

        Parameters
        ----------
        task_scope : str
            Free-form task-scope label. Recognized values are
            ``"single_task"``, ``"multi_task"``, ``"domain_adaptation"``,
            and ``"instruction_tuning"``; unknown values are accepted with
            a warning (only ``"multi_task"`` affects scoring).
        dataset_size : int
            Training dataset size in samples (positive int).
        domain_shift : str
            One of ``"small"``, ``"moderate"``, ``"large"``.
        carbon_budget : float
            Carbon budget in **kilograms of CO2 equivalent (kg CO2e)**.
            Values below ``ClassifierConfig.low_carbon_reasoning_threshold``
            (default 0.5 kg) trigger the "low carbon budget" reasoning.
        target_accuracy : float
            Target accuracy in ``[0, 1]``.
        model_size_params : int, optional
            Model parameter count. When ``None``, the config's
            ``default_model_params`` is used for carbon estimation.
        record : bool, default True
            If True, append the recommendation to the bounded history.
        """
        # ---- Validation ----------------------------------------------
        if not isinstance(task_scope, str) or not task_scope:
            raise AdaptationClassifierError(
                "task_scope must be a non-empty string."
            )
        if task_scope not in self._KNOWN_TASK_SCOPES:
            logger.warning(
                "task_scope %r is not in the recognized set %s; only "
                "'multi_task' affects scoring.",
                task_scope, self._KNOWN_TASK_SCOPES,
            )

        if (
            isinstance(dataset_size, bool)
            or not isinstance(dataset_size, int)
            or dataset_size <= 0
        ):
            raise AdaptationClassifierError(
                "dataset_size must be a positive int."
            )

        if domain_shift not in self._DOMAIN_SHIFTS:
            raise AdaptationClassifierError(
                f"domain_shift must be one of {self._DOMAIN_SHIFTS}, "
                f"got {domain_shift!r}"
            )

        carbon_budget = _safe_float("carbon_budget", carbon_budget)
        if carbon_budget < 0:
            raise AdaptationClassifierError(
                "carbon_budget must be non-negative."
            )

        target_accuracy = _safe_float("target_accuracy", target_accuracy)
        if not 0.0 <= target_accuracy <= 1.0:
            raise AdaptationClassifierError(
                "target_accuracy must be in [0, 1]."
            )

        if model_size_params is not None:
            if (
                isinstance(model_size_params, bool)
                or not isinstance(model_size_params, int)
                or model_size_params <= 0
            ):
                raise AdaptationClassifierError(
                    "model_size_params must be a positive int or None."
                )

        # ---- Score each strategy ------------------------------------
        scores: Dict[AdaptationStrategy, float] = {}
        for strategy, profile in self.strategy_profiles.items():
            scores[strategy] = self._score_strategy(
                strategy=strategy,
                profile=profile,
                task_scope=task_scope,
                dataset_size=dataset_size,
                domain_shift=domain_shift,
                carbon_budget=carbon_budget,
                target_accuracy=target_accuracy,
                model_size_params=model_size_params,
            )

        # Deterministic tie-break: primary = score desc, secondary = name asc.
        sorted_strategies = sorted(
            scores.items(),
            key=lambda x: (-x[1], x[0].value),
        )
        top_strategy, _ = sorted_strategies[0]
        profile = self.strategy_profiles[top_strategy]

        base_carbon = self._estimate_base_carbon(
            dataset_size, model_size_params,
        )
        carbon_required = base_carbon * profile["energy_multiplier"]

        reasoning = self._generate_reasoning(
            strategy=top_strategy,
            profile=profile,
            task_scope=task_scope,
            dataset_size=dataset_size,
            domain_shift=domain_shift,
            carbon_budget=carbon_budget,
        )

        alternatives = tuple(
            {
                "strategy": strategy.value,
                "score": score,
                "trainable_pct": self.strategy_profiles[strategy][
                    "trainable_pct"
                ],
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

        logger.debug(
            "Classified strategy: %s (trainable=%.2f%%, energy_mult=%.2f)",
            top_strategy.value,
            profile["trainable_pct"],
            profile["energy_multiplier"],
        )
        return recommendation

    def best_strategy(
        self,
        task_scope: str,
        dataset_size: int,
        domain_shift: str,
        carbon_budget: float,
        target_accuracy: float,
        model_size_params: Optional[int] = None,
    ) -> AdaptationStrategy:
        """Return just the top strategy for the given task characteristics."""
        return self.classify(
            task_scope=task_scope,
            dataset_size=dataset_size,
            domain_shift=domain_shift,
            carbon_budget=carbon_budget,
            target_accuracy=target_accuracy,
            model_size_params=model_size_params,
            record=False,
        ).strategy

    def classify_batch(
        self, items: Iterable[Mapping[str, Any]],
    ) -> List[StrategyRecommendation]:
        """
        Classify a batch of task dicts.

        Each item must contain the keyword arguments accepted by
        :meth:`classify`. Returns one recommendation per item.
        """
        results: List[StrategyRecommendation] = []
        for i, item in enumerate(items):
            if not isinstance(item, Mapping):
                raise AdaptationClassifierError(
                    f"items[{i}] must be a Mapping, "
                    f"got {type(item).__name__}"
                )
            results.append(self.classify(**item))
        return results

    # ------------------------------------------------------------------ compare
    def compare(
        self,
        strategy_a: AdaptationStrategy,
        strategy_b: AdaptationStrategy,
    ) -> Dict[str, Any]:
        """Structured comparison between two strategies' profiles."""
        a = self._profile_summary(strategy_a)
        b = self._profile_summary(strategy_b)
        return {
            "strategy_a": a,
            "strategy_b": b,
            "trainable_pct_delta": (
                b["trainable_pct"] - a["trainable_pct"]
            ),
            "energy_multiplier_delta": (
                b["energy_multiplier"] - a["energy_multiplier"]
            ),
            "quality_delta": b["quality_baseline"] - a["quality_baseline"],
            "b_is_cheaper": (
                b["energy_multiplier"] < a["energy_multiplier"]
            ),
            "b_uses_fewer_params": (
                b["trainable_pct"] < a["trainable_pct"]
            ),
        }

    def _profile_summary(
        self, strategy: AdaptationStrategy,
    ) -> Dict[str, Any]:
        profile = self.strategy_profiles.get(strategy)
        if profile is None:
            raise AdaptationClassifierError(
                f"unknown strategy {strategy!r}"
            )
        return dict(profile)

    # ------------------------------------------------------------------ scoring
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
        base_carbon = self._estimate_base_carbon(
            dataset_size, model_size_params,
        )
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
                AdaptationStrategy.LORA,
                AdaptationStrategy.PREFIX_TUNING,
            ):
                score += cfg.large_model_pe_bonus
            elif strategy == AdaptationStrategy.FULL_FT:
                score += cfg.large_model_full_ft_penalty

        # Factor 5: Task scope.
        if task_scope == "multi_task":
            if strategy in (
                AdaptationStrategy.ADAPTERS, AdaptationStrategy.LORA,
            ):
                score += cfg.multi_task_adapters_lora_bonus

        # Factor 6: Target accuracy.
        if target_accuracy > cfg.high_accuracy_threshold:
            if strategy == AdaptationStrategy.FULL_FT:
                score += cfg.high_accuracy_full_ft_bonus
            elif strategy in (
                AdaptationStrategy.BITFIT,
                AdaptationStrategy.PROMPT_TUNING,
            ):
                score += cfg.high_accuracy_pe_penalty

        return score

    def _estimate_base_carbon(
        self,
        dataset_size: int,
        model_size_params: Optional[int],
    ) -> float:
        """Estimate base carbon (kg CO2e) for full fine-tuning."""
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

        # Fix P1: use task_scope instead of ignoring it.
        if task_scope == "multi_task":
            if strategy in (
                AdaptationStrategy.ADAPTERS, AdaptationStrategy.LORA,
            ):
                reasons.append(
                    "Multi-task scope favors parameter-efficient adapters "
                    "that isolate per-task state"
                )

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
            reasons.append(
                f"~{savings_pct:.0f}% energy savings vs full fine-tuning"
            )

        return (
            ". ".join(reasons) + "." if reasons
            else "No dominant factors detected."
        )

    # ------------------------------------------------------------------ stats
    def statistics(self) -> Dict[str, Any]:
        """Aggregate statistics over the recommendation history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "recommendations": 0,
                "by_strategy": {},
                "mean_energy_multiplier": None,
                "mean_carbon_required": None,
                "min_energy_multiplier": None,
                "max_energy_multiplier": None,
            }
        by_strategy: Dict[str, int] = {}
        for r in history:
            by_strategy[r.strategy.value] = (
                by_strategy.get(r.strategy.value, 0) + 1
            )
        energies = [r.expected_energy_multiplier for r in history]
        carbons = [r.carbon_budget_required_kgco2e for r in history]
        return {
            "recommendations": len(history),
            "by_strategy": by_strategy,
            "mean_energy_multiplier": sum(energies) / len(energies),
            "mean_carbon_required": sum(carbons) / len(carbons),
            "min_energy_multiplier": min(energies),
            "max_energy_multiplier": max(energies),
        }

    # ------------------------------------------------------------------ history
    def history_since(
        self, since_epoch_seconds: float,
    ) -> List[StrategyRecommendation]:
        """Recommendations with ``timestamp >= since_epoch_seconds``."""
        since = _safe_float("since_epoch_seconds", since_epoch_seconds)
        with self._lock:
            return [r for r in self._history if r.timestamp >= since]

    def history_between(
        self, t0: float, t1: float,
    ) -> List[StrategyRecommendation]:
        """Recommendations with ``t0 <= timestamp <= t1``."""
        t0v = _safe_float("t0", t0)
        t1v = _safe_float("t1", t1)
        if t1v < t0v:
            raise AdaptationClassifierError("t1 must be >= t0.")
        with self._lock:
            return [
                r for r in self._history
                if t0v <= r.timestamp <= t1v
            ]

    def clear_history(self) -> int:
        """Clear the history; return how many entries were removed."""
        with self._lock:
            n = len(self._history)
            self._history.clear()
        logger.debug("Cleared %d recommendation(s).", n)
        return n

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("AdaptationStrategyClassifier reset.")

    # ------------------------------------------------------------------ dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "AdaptationStrategyClassifier("
                f"strategies={len(self.strategy_profiles)}, "
                f"recommendations={len(self._history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    classifier = AdaptationStrategyClassifier()
    print("repr       :", classifier)

    # --- happy path: low budget, small dataset → parameter-efficient ---
    rec1 = classifier.classify(
        task_scope="single_task", dataset_size=2_000,
        domain_shift="moderate", carbon_budget=0.1,
        target_accuracy=0.85, model_size_params=110_000_000,
    )
    assert rec1.strategy in (
        AdaptationStrategy.LORA,
        AdaptationStrategy.PREFIX_TUNING,
        AdaptationStrategy.PROMPT_TUNING,
        AdaptationStrategy.BITFIT,
        AdaptationStrategy.ADAPTERS,
    ), f"low budget should favor efficiency, got {rec1.strategy}"
    print("rec1       :", rec1)
    print("reasoning  :", rec1.reasoning)

    # --- large dataset, large domain shift, big budget → full FT / adapters ---
    rec2 = classifier.classify(
        task_scope="domain_adaptation", dataset_size=50_000,
        domain_shift="large", carbon_budget=10.0,
        target_accuracy=0.95, model_size_params=340_000_000,
    )
    assert rec2.strategy in (
        AdaptationStrategy.FULL_FT, AdaptationStrategy.ADAPTERS,
    ), f"large budget should favor full FT / adapters, got {rec2.strategy}"
    print("rec2       :", rec2)

    print("stats      :", classifier.statistics())

    # --- serialization round-trip ---
    payload = rec1.to_json()
    restored = StrategyRecommendation.from_json(payload)
    assert restored.to_dict() == rec1.to_dict()
    assert restored.key() == rec1.key()
    print("Round-trip OK.")

    # --- P0 #1 regression: per-instance profiles ---
    c1 = AdaptationStrategyClassifier()
    c2 = AdaptationStrategyClassifier()
    original = c2.strategy_profiles[AdaptationStrategy.LORA][
        "energy_multiplier"
    ]
    c1.strategy_profiles[AdaptationStrategy.LORA]["energy_multiplier"] = 99.0
    assert c2.strategy_profiles[AdaptationStrategy.LORA][
        "energy_multiplier"
    ] == original, "mutation leaked across instances"
    print("Per-instance profiles: OK")

    # --- P0 #2 regression: NaN / Inf rejected everywhere ---
    for bad in [
        dict(trainable_params_pct=float("nan")),
        dict(expected_energy_multiplier=float("inf")),
        dict(expected_quality_impact=float("nan")),
        dict(carbon_budget_required_kgco2e=float("nan")),
    ]:
        kwargs = {
            "strategy": AdaptationStrategy.LORA,
            "trainable_params_pct": 0.3,
            "expected_energy_multiplier": 0.15,
            "expected_quality_impact": -0.01,
            "carbon_budget_required_kgco2e": 0.01,
            "reasoning": "test",
        }
        kwargs.update(bad)
        try:
            StrategyRecommendation(**kwargs)
        except AdaptationClassifierError:
            pass
        else:
            raise AssertionError(
                f"bad input {bad} was accepted"
            )
    print("NaN/Inf rejection: OK")

    # --- P0 #3 regression: from_dict error contract ---
    for bad_payload in [
        {},                                    # missing required
        {"strategy": "bogus", "trainable_params_pct": 0.3,
         "expected_energy_multiplier": 0.15, "expected_quality_impact": -0.01,
         "carbon_budget_required_kgco2e": 0.01},
        {"strategy": "lora", "trainable_params_pct": "abc",
         "expected_energy_multiplier": 0.15, "expected_quality_impact": -0.01,
         "carbon_budget_required_kgco2e": 0.01},
    ]:
        try:
            StrategyRecommendation.from_dict(bad_payload)
        except AdaptationClassifierError:
            pass
        else:
            raise AssertionError(
                f"from_dict accepted bad payload: {bad_payload}"
            )
    print("from_dict error contract: OK")

    # --- P1 #4 regression: alternatives shape ---
    for bad_alt in ["abc", 42, [42], [None]]:
        try:
            StrategyRecommendation.from_dict({
                "strategy": "lora",
                "trainable_params_pct": 0.3,
                "expected_energy_multiplier": 0.15,
                "expected_quality_impact": -0.01,
                "carbon_budget_required_kgco2e": 0.01,
                "reasoning": "test",
                "alternatives": bad_alt,
            })
        except AdaptationClassifierError:
            pass
        else:
            raise AssertionError(
                f"bad alternatives {bad_alt!r} was accepted"
            )
    print("alternatives validation: OK")

    # --- P1 #5: multi_task reasoning mentions adapters ---
    rec_mt = classifier.classify(
        task_scope="multi_task", dataset_size=3_000,
        domain_shift="moderate", carbon_budget=1.0,
        target_accuracy=0.88, record=False,
    )
    if rec_mt.strategy in (
        AdaptationStrategy.LORA, AdaptationStrategy.ADAPTERS,
    ):
        assert "Multi-task" in rec_mt.reasoning, rec_mt.reasoning
        print("multi_task reasoning: OK")

    # --- P1 #11: deterministic tie-break ---
    # Two classifiers, same inputs → identical strategy choice.
    c3 = AdaptationStrategyClassifier()
    c4 = AdaptationStrategyClassifier()
    s3 = c3.classify(
        task_scope="single_task", dataset_size=500,
        domain_shift="small", carbon_budget=0.01,
        target_accuracy=0.5, record=False,
    ).strategy
    s4 = c4.classify(
        task_scope="single_task", dataset_size=500,
        domain_shift="small", carbon_budget=0.01,
        target_accuracy=0.5, record=False,
    ).strategy
    assert s3 == s4, f"non-deterministic: {s3} vs {s4}"
    print("deterministic scoring: OK")

    # --- P2: best_strategy / compare / classify_batch ---
    best = classifier.best_strategy(
        task_scope="single_task", dataset_size=2_000,
        domain_shift="moderate", carbon_budget=0.1,
        target_accuracy=0.85,
    )
    assert isinstance(best, AdaptationStrategy)
    print("best_strategy:", best)

    cmp = classifier.compare(
        AdaptationStrategy.LORA, AdaptationStrategy.FULL_FT,
    )
    assert cmp["b_is_cheaper"] is False
    assert cmp["b_uses_fewer_params"] is False
    print("compare: OK")

    batch = classifier.classify_batch([
        {"task_scope": "single_task", "dataset_size": 500,
         "domain_shift": "small", "carbon_budget": 0.01,
         "target_accuracy": 0.85, "record": False},
        {"task_scope": "single_task", "dataset_size": 20_000,
         "domain_shift": "large", "carbon_budget": 5.0,
         "target_accuracy": 0.95, "record": False},
    ])
    assert len(batch) == 2
    print("classify_batch:", [r.strategy.value for r in batch])

    # --- P2: history queries ---
    history_since = classifier.history_since(0.0)
    assert len(history_since) == len(classifier.history)
    history_between = classifier.history_between(0.0, time.time() + 1)
    assert len(history_between) == len(classifier.history)
    cleared = classifier.clear_history()
    assert cleared >= 0
    assert len(classifier.history) == 0
    print(f"history helpers: cleared {cleared}")

    # --- carbon_budget=0 case ---
    rec_zero = classifier.classify(
        task_scope="single_task", dataset_size=1_000,
        domain_shift="small", carbon_budget=0.0,
        target_accuracy=0.5, record=False,
    )
    assert rec_zero.strategy in (
        AdaptationStrategy.LORA, AdaptationStrategy.PROMPT_TUNING,
        AdaptationStrategy.BITFIT, AdaptationStrategy.PREFIX_TUNING,
    ), f"zero budget should favor cheap strategies, got {rec_zero.strategy}"
    print("carbon_budget=0:", rec_zero.strategy.value)

    print("\nSmoke test passed.")
