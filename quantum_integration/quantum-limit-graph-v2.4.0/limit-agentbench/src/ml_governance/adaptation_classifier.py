"""
Adaptation Strategy Classifier
===============================

Classifies the optimal fine-tuning strategy based on task characteristics
and carbon constraints.

Location: src/ml_governance/adaptation_classifier.py
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AdaptationStrategy(Enum):
    """Available adaptation strategies"""
    FULL_FT = "full_fine_tuning"          # Train all parameters
    LORA = "lora"                         # Low-Rank Adaptation
    ADAPTERS = "adapters"                 # Bottleneck adapters
    PREFIX_TUNING = "prefix_tuning"       # Learnable prompts
    BITFIT = "bitfit"                     # Bias-only fine-tuning
    DISTILLATION = "distillation"         # Knowledge distillation
    FREEZE_BACKBONE = "freeze_backbone"   # Train head only
    PROMPT_TUNING = "prompt_tuning"       # Soft prompts


@dataclass
class StrategyRecommendation:
    """Recommended adaptation strategy with reasoning"""
    strategy: AdaptationStrategy
    trainable_params_pct: float  # % of total parameters
    expected_energy_multiplier: float  # vs full fine-tuning
    expected_quality_impact: float  # Expected accuracy change
    carbon_budget_required_kgco2e: float
    reasoning: str
    alternatives: list


class AdaptationStrategyClassifier:
    """
    Classifies optimal fine-tuning strategy
    
    Decision Factors:
    1. Dataset size (small → parameter-efficient methods)
    2. Domain shift (large → more parameters needed)
    3. Carbon budget (low → LoRA/adapters)
    4. Target accuracy (high → possibly full fine-tuning)
    5. Model size (large → prefer parameter-efficient)
    """
    
    def __init__(self):
        # Strategy characteristics
        self.strategy_profiles = {
            AdaptationStrategy.FULL_FT: {
                "trainable_pct": 100.0,
                "energy_multiplier": 1.0,
                "quality_baseline": 0.0,
                "min_dataset_size": 10_000,
                "carbon_intensity": "very_high"
            },
            AdaptationStrategy.LORA: {
                "trainable_pct": 0.3,
                "energy_multiplier": 0.15,
                "quality_baseline": -0.01,  # Usually -1% vs full FT
                "min_dataset_size": 1_000,
                "carbon_intensity": "very_low"
            },
            AdaptationStrategy.ADAPTERS: {
                "trainable_pct": 2.0,
                "energy_multiplier": 0.25,
                "quality_baseline": -0.015,
                "min_dataset_size": 2_000,
                "carbon_intensity": "low"
            },
            AdaptationStrategy.PREFIX_TUNING: {
                "trainable_pct": 0.1,
                "energy_multiplier": 0.10,
                "quality_baseline": -0.02,
                "min_dataset_size": 500,
                "carbon_intensity": "very_low"
            },
            AdaptationStrategy.BITFIT: {
                "trainable_pct": 0.1,
                "energy_multiplier": 0.08,
                "quality_baseline": -0.03,
                "min_dataset_size": 500,
                "carbon_intensity": "very_low"
            },
            AdaptationStrategy.DISTILLATION: {
                "trainable_pct": 100.0,  # Train student model fully
                "energy_multiplier": 0.50,  # One-time cost, then saves energy
                "quality_baseline": -0.05,  # Depends on student size
                "min_dataset_size": 5_000,
                "carbon_intensity": "medium"
            },
            AdaptationStrategy.FREEZE_BACKBONE: {
                "trainable_pct": 10.0,
                "energy_multiplier": 0.20,
                "quality_baseline": -0.04,
                "min_dataset_size": 2_000,
                "carbon_intensity": "low"
            },
            AdaptationStrategy.PROMPT_TUNING: {
                "trainable_pct": 0.01,
                "energy_multiplier": 0.05,
                "quality_baseline": -0.03,
                "min_dataset_size": 100,
                "carbon_intensity": "very_low"
            }
        }
        
        logger.info("Adaptation strategy classifier initialized")
    
    def classify(
        self,
        task_scope: str,  # "single_task", "multi_task", "domain_adaptation"
        dataset_size: int,
        domain_shift: str,  # "small", "moderate", "large"
        carbon_budget: float,  # kgCO2e available
        target_accuracy: float,  # Target accuracy (0-1)
        model_size_params: Optional[int] = None
    ) -> StrategyRecommendation:
        """
        Classify optimal adaptation strategy
        
        Args:
            task_scope: Scope of adaptation
            dataset_size: Number of training samples
            domain_shift: Amount of domain shift from pre-training
            carbon_budget: Available carbon budget (kgCO2e)
            target_accuracy: Target accuracy
            model_size_params: Model size in parameters
        
        Returns:
            StrategyRecommendation with chosen strategy and alternatives
        """
        
        # Score each strategy
        scores = {}
        for strategy, profile in self.strategy_profiles.items():
            score = self._score_strategy(
                strategy=strategy,
                profile=profile,
                task_scope=task_scope,
                dataset_size=dataset_size,
                domain_shift=domain_shift,
                carbon_budget=carbon_budget,
                target_accuracy=target_accuracy,
                model_size_params=model_size_params
            )
            scores[strategy] = score
        
        # Sort strategies by score (higher is better)
        sorted_strategies = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        # Get top strategy
        top_strategy, top_score = sorted_strategies[0]
        profile = self.strategy_profiles[top_strategy]
        
        # Calculate carbon budget required
        # Simplified: assume base energy cost and multiply by strategy energy multiplier
        base_carbon = self._estimate_base_carbon(dataset_size, model_size_params)
        carbon_required = base_carbon * profile["energy_multiplier"]
        
        # Generate reasoning
        reasoning = self._generate_reasoning(
            strategy=top_strategy,
            profile=profile,
            task_scope=task_scope,
            dataset_size=dataset_size,
            domain_shift=domain_shift,
            carbon_budget=carbon_budget
        )
        
        # Get top 3 alternatives
        alternatives = [
            {
                "strategy": strategy.value,
                "score": score,
                "trainable_pct": self.strategy_profiles[strategy]["trainable_pct"]
            }
            for strategy, score in sorted_strategies[1:4]
        ]
        
        return StrategyRecommendation(
            strategy=top_strategy,
            trainable_params_pct=profile["trainable_pct"],
            expected_energy_multiplier=profile["energy_multiplier"],
            expected_quality_impact=profile["quality_baseline"],
            carbon_budget_required_kgco2e=carbon_required,
            reasoning=reasoning,
            alternatives=alternatives
        )
    
    def _score_strategy(
        self,
        strategy: AdaptationStrategy,
        profile: Dict[str, Any],
        task_scope: str,
        dataset_size: int,
        domain_shift: str,
        carbon_budget: float,
        target_accuracy: float,
        model_size_params: Optional[int]
    ) -> float:
        """Score a strategy based on task characteristics"""
        
        score = 0.0
        
        # Factor 1: Dataset size compatibility
        min_dataset = profile["min_dataset_size"]
        if dataset_size < min_dataset:
            score -= 50.0  # Heavy penalty
        elif dataset_size < min_dataset * 2:
            score -= 10.0  # Moderate penalty
        else:
            score += 10.0  # Bonus for sufficient data
        
        # Factor 2: Carbon budget compatibility
        base_carbon = self._estimate_base_carbon(dataset_size, model_size_params)
        required_carbon = base_carbon * profile["energy_multiplier"]
        
        if required_carbon > carbon_budget:
            score -= 100.0  # Cannot afford this strategy
        else:
            carbon_efficiency = (carbon_budget - required_carbon) / carbon_budget
            score += carbon_efficiency * 30.0  # Bonus for staying under budget
        
        # Factor 3: Domain shift
        domain_shift_scores = {
            "small": {AdaptationStrategy.LORA: 20, AdaptationStrategy.PROMPT_TUNING: 15},
            "moderate": {AdaptationStrategy.LORA: 15, AdaptationStrategy.ADAPTERS: 20},
            "large": {AdaptationStrategy.FULL_FT: 20, AdaptationStrategy.ADAPTERS: 15}
        }
        score += domain_shift_scores.get(domain_shift, {}).get(strategy, 0)
        
        # Factor 4: Model size (large models → prefer parameter-efficient)
        if model_size_params and model_size_params > 1_000_000_000:  # >1B params
            if strategy in [AdaptationStrategy.LORA, AdaptationStrategy.PREFIX_TUNING]:
                score += 15.0
            elif strategy == AdaptationStrategy.FULL_FT:
                score -= 10.0
        
        # Factor 5: Task scope
        if task_scope == "multi_task":
            if strategy in [AdaptationStrategy.ADAPTERS, AdaptationStrategy.LORA]:
                score += 10.0  # Good for multi-task
        
        # Factor 6: Target accuracy (high accuracy may need more parameters)
        if target_accuracy > 0.90:
            if strategy == AdaptationStrategy.FULL_FT:
                score += 5.0
            elif strategy in [AdaptationStrategy.BITFIT, AdaptationStrategy.PROMPT_TUNING]:
                score -= 5.0
        
        return score
    
    def _estimate_base_carbon(
        self,
        dataset_size: int,
        model_size_params: Optional[int]
    ) -> float:
        """Estimate base carbon for full fine-tuning"""
        
        # Simplified heuristic
        if model_size_params is None:
            model_size_params = 100_000_000  # Assume 100M params
        
        # Base: 0.001 kgCO2e per 1M params per 1K samples
        base_carbon = (model_size_params / 1e6) * (dataset_size / 1000) * 0.001
        
        return base_carbon
    
    def _generate_reasoning(
        self,
        strategy: AdaptationStrategy,
        profile: Dict[str, Any],
        task_scope: str,
        dataset_size: int,
        domain_shift: str,
        carbon_budget: float
    ) -> str:
        """Generate human-readable reasoning"""
        
        reasons = []
        
        # Dataset size
        if dataset_size < 5_000:
            reasons.append(f"Small dataset ({dataset_size} samples) favors parameter-efficient methods")
        
        # Carbon budget
        if carbon_budget < 0.5:
            reasons.append(f"Low carbon budget ({carbon_budget:.3f} kgCO2e) requires efficient adaptation")
        
        # Domain shift
        if domain_shift == "small":
            reasons.append("Small domain shift allows minimal adaptation")
        elif domain_shift == "large":
            reasons.append("Large domain shift may require more parameters")
        
        # Strategy benefits
        if strategy == AdaptationStrategy.LORA:
            reasons.append("LoRA provides excellent balance of efficiency and performance")
        elif strategy == AdaptationStrategy.FULL_FT:
            reasons.append("Full fine-tuning for maximum performance")
        elif strategy == AdaptationStrategy.ADAPTERS:
            reasons.append("Adapters enable task-specific customization")
        
        # Energy savings
        energy_mult = profile["energy_multiplier"]
        if energy_mult < 0.3:
            savings_pct = (1.0 - energy_mult) * 100
            reasons.append(f"~{savings_pct:.0f}% energy savings vs full fine-tuning")
        
        return ". ".join(reasons) + "."


if __name__ == "__main__":
    classifier = AdaptationStrategyClassifier()
    
    # Example: Low carbon budget, small dataset
    recommendation = classifier.classify(
        task_scope="single_task",
        dataset_size=5_000,
        domain_shift="moderate",
        carbon_budget=0.1,  # Low budget
        target_accuracy=0.88,
        model_size_params=110_000_000  # BERT-base
    )
    
    print(f"Recommended: {recommendation.strategy.value}")
    print(f"Trainable params: {recommendation.trainable_params_pct:.1f}%")
    print(f"Energy multiplier: {recommendation.expected_energy_multiplier:.2f}x")
    print(f"Quality impact: {recommendation.expected_quality_impact:+.2%}")
    print(f"Carbon required: {recommendation.carbon_budget_required_kgco2e:.4f} kgCO2e")
    print(f"Reasoning: {recommendation.reasoning}")
    print(f"Alternatives: {[alt['strategy'] for alt in recommendation.alternatives]}")
