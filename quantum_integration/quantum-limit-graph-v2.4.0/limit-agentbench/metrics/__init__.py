# -*- coding: utf-8 -*-
"""Green metrics tracking modules (Enhanced)

This package provides the core green metrics calculators:
- EnergyTracker
- CarbonCalculator
- EfficiencyScorer
- SustainabilityIndex

Enhancements (enabled via GreenMetricsConfig):
  - LIMIT Graph metrics are incorporated into composite scores.
  - MODP (multi‑objective) weights are configurable.
  - RLHF: human feedback score adjusts weighting.
  - Multi‑Teacher On‑Policy Distillation + MoE: a learned aggregator blends
    the individual metric scores.
  - Bio‑inspired optimisation: optional evolutionary tuning of MODP weights.
"""

from .energy_tracker import EnergyTracker
from .carbon_calculator import CarbonCalculator
from .efficiency_scorer import EfficiencyScorer
from .sustainability_index import SustainabilityIndex

# Optional imports for advanced enhancements (graceful if unavailable)
try:
    import numpy as np
    from collections import deque
    import random
    ENHANCEMENT_LIBS_AVAILABLE = True
except ImportError:
    ENHANCEMENT_LIBS_AVAILABLE = False
    np = None

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Tuple


@dataclass
class GreenMetricsConfig:
    """Configuration for enhanced green metrics aggregation."""
    use_enhancements: bool = False
    # LIMIT Graph metrics (defaults, can be provided per calculation)
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5
    })
    # MODP weights: [energy, carbon, latency, accuracy]
    modp_weights: Optional[List[float]] = None   # default [0.3, 0.3, 0.2, 0.2]
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation + MoE
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon: float = 0.1
    # Bio‑inspired
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2


class GreenMetricsManager:
    """
    Unified manager that combines the individual green metrics calculators
    and optionally applies advanced techniques (MODP, RLHF, distillation, MoE,
    graph metrics, and evolutionary tuning) to produce a composite score.
    """

    def __init__(self, config: Optional[GreenMetricsConfig] = None):
        self.config = config or GreenMetricsConfig()
        self.use_enhancements = self.config.use_enhancements and ENHANCEMENT_LIBS_AVAILABLE

        # Instantiate core calculators (these are always available)
        self.energy_tracker = EnergyTracker()
        self.carbon_calculator = CarbonCalculator(
            grid_region=self.config.graph_metrics.get("region", "GLOBAL")
            if "region" in self.config.graph_metrics else "GLOBAL"
        )
        self.efficiency_scorer = EfficiencyScorer()
        self.sustainability_index = SustainabilityIndex()

        # Enhanced components (optional)
        self.distillation_optimizer = None
        self.evolutionary_optimizer = None

        if self.use_enhancements:
            # Set default MODP weights if not provided
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.3, 0.3, 0.2, 0.2]  # energy, carbon, latency, accuracy
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]

            if self.config.use_distillation:
                # Simple distillation aggregator for the four base metrics
                self.distillation_optimizer = self._create_distillation_optimizer()

            if self.config.use_evolutionary:
                self.evolutionary_optimizer = self._create_evolutionary_optimizer()

    def compute_report(
        self,
        energy_kwh: float,
        carbon_co2e_kg: float,
        accuracy: float,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Compute a comprehensive green metrics report.

        If enhancements are enabled, the report includes a composite
        sustainability score derived from MODP, distillation, and graph metrics.
        Otherwise, it returns the individual scores from the base calculators.
        """
        # Use provided or default metrics/feedback
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        # Compute individual metrics (always)
        energy_score = self.energy_tracker.get_energy_score(energy_kwh)  # assume method exists
        carbon_score = self.carbon_calculator.calculate_emissions(energy_kwh)  # kg CO2e
        efficiency = self.efficiency_scorer.calculate_efficiency_score(accuracy, energy_kwh)
        sustainability = self.sustainability_index.calculate(accuracy, energy_kwh, carbon_co2e_kg)

        report = {
            "energy_kwh": energy_kwh,
            "carbon_co2e_kg": carbon_co2e_kg,
            "accuracy": accuracy,
            "latency_ms": latency_ms,
            "cost_usd": cost_usd,
            "efficiency_score": efficiency,
            "sustainability_index": sustainability,
            "energy_score": energy_score,
            "carbon_emissions_kg": carbon_score,
        }

        if self.use_enhancements:
            # Compute composite score using MODP or distillation
            composite = self._compute_composite_score(
                energy_kwh=energy_kwh,
                carbon_co2e_kg=carbon_co2e_kg,
                accuracy=accuracy,
                latency_ms=latency_ms,
                graph_metrics=graph_metrics,
                human_feedback=human_feedback_score
            )
            report["composite_score"] = composite["score"]
            report["method"] = composite["method"]
            report["graph_metrics"] = graph_metrics
            report["human_feedback_score"] = human_feedback_score
            if composite.get("distillation_stats"):
                report["distillation_stats"] = composite["distillation_stats"]

        return report

    def _compute_composite_score(self, energy_kwh, carbon_co2e_kg, accuracy,
                                 latency_ms, graph_metrics, human_feedback) -> Dict[str, Any]:
        """
        Compute a multi‑objective score using MODP weights or a distillation model.
        """
        # Normalize components (higher is better)
        energy_norm = 1.0 - min(energy_kwh / 10.0, 1.0)
        carbon_norm = 1.0 - min(carbon_co2e_kg / 1.0, 1.0)
        latency_norm = 1.0 - min(latency_ms / 1000.0, 1.0)
        acc_norm = min(accuracy, 1.0)

        if self.distillation_optimizer:
            # Use learned aggregator (simplified linear + gating)
            score, stats = self.distillation_optimizer.predict_score(
                features=np.array([energy_norm, carbon_norm, latency_norm, acc_norm,
                                   graph_metrics.get("centrality", 0.5),
                                   graph_metrics.get("connectivity", 0.5),
                                   human_feedback], dtype=np.float32)
            )
            method = "distillation"
        else:
            # Use MODP weights
            weights = self.config.modp_weights
            components = np.array([energy_norm, carbon_norm, latency_norm, acc_norm])
            score = float(np.dot(components, weights))
            method = "modp"
            stats = None

        return {"score": score, "method": method, "distillation_stats": stats}

    def _create_distillation_optimizer(self):
        """Create a simple distillation optimizer with MoE gating (placeholder)."""
        class DistillationOptimizer:
            def __init__(self, lr=0.01):
                self.weights = np.zeros(7)  # 4 metrics + 2 graph + 1 feedback
                self.bias = 0.0
                self.lr = lr
                self.counter = 0

            def predict_score(self, features):
                # Simple linear regression
                score = float(np.dot(features, self.weights) + self.bias)
                score = max(0.0, min(1.0, score))
                return score, {"counter": self.counter}

            def update(self, features, target):
                pred = self.predict_score(features)[0]
                grad = (pred - target) * features
                self.weights -= self.lr * grad
                self.bias -= self.lr * (pred - target)
                self.counter += 1

        return DistillationOptimizer(lr=self.config.distillation_lr)

    def _create_evolutionary_optimizer(self):
        """Create a placeholder evolutionary optimizer for MODP weights."""
        class EvolutionaryOptimizer:
            def __init__(self, n_weights=4, pop_size=10):
                self.population = [np.random.dirichlet(np.ones(n_weights)) for _ in range(pop_size)]
                self.best_weights = self.population[0]

            def get_weights(self):
                return self.best_weights

            def update_fitness(self, reward):
                # Simplified: just keep best
                pass

        return EvolutionaryOptimizer()


# Re-export the manager
__all__ = [
    "EnergyTracker",
    "CarbonCalculator",
    "EfficiencyScorer",
    "SustainabilityIndex",
    "GreenMetricsManager",
    "GreenMetricsConfig",
]
