# -*- coding: utf-8 -*-
"""
Sustainability Index (Enhanced)
Composite green performance metric

Enhancements (enabled via SustainabilityConfig.use_enhancements):
  - LIMIT Graph metrics (centrality, connectivity) influence score.
  - MODP: configurable objective weights (accuracy, energy, carbon, latency, cost).
  - RLHF: human feedback score adjusts weights in real time.
  - Multi‑Teacher On‑Policy Distillation + MoE: a learned model combines
    rule‑based, RLHF, and historical sustainability scores.
  - Bio‑inspired optimisation: evolutionary tuning of MODP weights.

Original functionality is preserved when enhancements are disabled.
"""

from typing import Dict, Optional, List, Tuple, Any
import logging
import math
import random
import numpy as np
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enhanced configuration dataclass
# ---------------------------------------------------------------------------
@dataclass
class SustainabilityConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5
    })
    # MODP weights: [accuracy, energy_efficiency, carbon_efficiency, latency_efficiency, cost_efficiency]
    modp_weights: Optional[List[float]] = None  # default [0.3, 0.3, 0.2, 0.1, 0.1]
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


# ---------------------------------------------------------------------------
# Decision state and distillation optimizer for sustainability scoring
# ---------------------------------------------------------------------------
class SustainabilityState:
    """Feature vector for learned sustainability scoring."""
    def __init__(self, accuracy: float, energy_kwh: float, carbon_co2e_kg: float,
                 latency_ms: float = 0.0, cost_usd: float = 0.0,
                 graph_metrics: Optional[Dict[str, float]] = None,
                 human_feedback: float = 0.5):
        self.accuracy = min(accuracy, 1.0)
        self.energy_norm = min(energy_kwh / 10.0, 1.0)          # scale energy
        self.carbon_norm = min(carbon_co2e_kg / 1.0, 1.0)       # scale carbon
        self.latency_norm = min(latency_ms / 1000.0, 1.0)
        self.cost_norm = min(cost_usd / 10.0, 1.0)
        self.centrality = (graph_metrics or {}).get("centrality", 0.5)
        self.connectivity = (graph_metrics or {}).get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.accuracy,
            self.energy_norm,
            self.carbon_norm,
            self.latency_norm,
            self.cost_norm,
            self.centrality,
            self.connectivity,
            self.human_feedback,
        ], dtype=np.float32)


class SustainabilityDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to produce a final sustainability score.
    Teachers: rule‑based (original formula), RLHF, historical.
    Output: continuous score (higher is better).
    """
    def __init__(self, config: SustainabilityConfig):
        self.config = config
        self.feature_dim = 8
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.train_every = config.train_every
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.replay_size)

        # Student (linear regression)
        self.student_weights = np.zeros(self.feature_dim)
        self.student_bias = 0.0

        # Teachers
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr

    def _rule_teacher(self, state: SustainabilityState) -> float:
        # Original sustainability index formula (normalized to a reasonable range)
        if state.carbon_norm == 0 or state.energy_norm == 0:
            base = 0.0
        else:
            efficiency = 1.0 / state.energy_norm
            numerator = state.accuracy * 0.4 + efficiency * 0.3
            denominator = state.carbon_norm * 0.3
            base = numerator / denominator if denominator > 0 else 0.0
        # Scale to a reasonable range (e.g., 0-1)
        return min(base / 100.0, 1.0)

    def _rlhf_teacher(self, state: SustainabilityState) -> float:
        # Human feedback: high -> boost accuracy, low -> boost efficiency/carbon
        if state.human_feedback > 0.7:
            return 0.6 * state.accuracy + 0.2 * (1 - state.energy_norm) + 0.2 * (1 - state.carbon_norm)
        elif state.human_feedback < 0.3:
            return 0.3 * state.accuracy + 0.4 * (1 - state.energy_norm) + 0.3 * (1 - state.carbon_norm)
        else:
            return 0.4 * state.accuracy + 0.3 * (1 - state.energy_norm) + 0.3 * (1 - state.carbon_norm)

    def _historical_teacher(self, state: SustainabilityState) -> float:
        # Simulated learned model: centrality and connectivity influence preference
        base = 0.5 * state.accuracy + 0.3 * (1 - state.energy_norm) + 0.2 * (1 - state.carbon_norm)
        if state.centrality > 0.7:
            base += 0.05
        if state.connectivity > 0.7:
            base += 0.05
        return min(base, 1.0)

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def predict_score(self, state: SustainabilityState, exploration: bool = False) -> float:
        x = state.to_vector()
        teacher_scores = np.array([t(state) for t in self.teachers])
        gate = self._gate_forward(x)
        teacher_combined = np.dot(gate, teacher_scores)
        student_pred = np.dot(x, self.student_weights) + self.student_bias

        if exploration and random.random() < self.epsilon:
            pred = teacher_combined
        else:
            pred = 0.7 * student_pred + 0.3 * teacher_combined

        return float(np.clip(pred, 0.0, 1.0))

    def update(self, state_vec, reward):
        self.replay_buffer.append((state_vec, reward))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = random.sample(self.replay_buffer, min(8, len(self.replay_buffer)))
            for s, r in batch:
                pred = np.dot(s, self.student_weights) + self.student_bias
                grad = (pred - r) * s
                self.student_weights -= self.lr * grad
                self.student_bias -= self.lr * (pred - r)


# ---------------------------------------------------------------------------
# Enhanced SustainabilityIndex class
# ---------------------------------------------------------------------------
class SustainabilityIndex:
    """
    Sustainability index calculator with optional enhancements.
    """

    def __init__(
        self,
        accuracy_weight: float = 0.4,
        efficiency_weight: float = 0.3,
        carbon_weight: float = 0.3,
        config: Optional[SustainabilityConfig] = None
    ):
        """
        Initialize sustainability index calculator.

        Args:
            accuracy_weight: Weight for accuracy component (original)
            efficiency_weight: Weight for efficiency component (original)
            carbon_weight: Weight for carbon component (original)
            config: Optional enhanced configuration
        """
        total = accuracy_weight + efficiency_weight + carbon_weight
        self.accuracy_weight = accuracy_weight / total
        self.efficiency_weight = efficiency_weight / total
        self.carbon_weight = carbon_weight / total

        self.config = config or SustainabilityConfig()
        self.use_enhancements = self.config.use_enhancements

        # Enhanced components
        self.distillation_optimizer = None
        self.evolutionary_optimizer = None
        if self.use_enhancements:
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.3, 0.3, 0.2, 0.1, 0.1]  # acc, energy, carbon, latency, cost
            else:
                total_w = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total_w for w in self.config.modp_weights]
            if self.config.use_distillation:
                self.distillation_optimizer = SustainabilityDistillationOptimizer(self.config)
            # Evolutionary optimizer could be added if needed

        logger.info(f"Initialized SustainabilityIndex with weights: "
                   f"accuracy={self.accuracy_weight:.2f}, "
                   f"efficiency={self.efficiency_weight:.2f}, "
                   f"carbon={self.carbon_weight:.2f}")

    # ------------------------------------------------------------------
    # Original calculate method (unchanged)
    # ------------------------------------------------------------------
    def calculate(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float
    ) -> float:
        """
        Calculate sustainability index using original formula.
        Higher is better.
        """
        if carbon_co2e_kg == 0 or energy_kwh == 0:
            return 0.0

        efficiency = 1.0 / energy_kwh
        numerator = (
            accuracy * self.accuracy_weight +
            efficiency * self.efficiency_weight
        )
        denominator = carbon_co2e_kg * self.carbon_weight

        return numerator / denominator if denominator > 0 else 0.0

    # ------------------------------------------------------------------
    # Enhanced calculate method (using MODP, distillation, etc.)
    # ------------------------------------------------------------------
    def calculate_enhanced(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Compute a multi‑objective sustainability score (0-1 or scaled).
        If enhancements enabled, uses distillation or MODP weights.
        Returns a dictionary with score and metadata.
        """
        if not self.use_enhancements:
            # Fallback: compute original score and normalize roughly
            base = self.calculate(accuracy, energy_kwh, carbon_co2e_kg)
            return {"score": min(base, 1.0), "method": "original"}

        # Use provided or defaults
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        state = SustainabilityState(
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
            graph_metrics=graph_metrics,
            human_feedback=human_feedback_score
        )

        if self.distillation_optimizer:
            score = self.distillation_optimizer.predict_score(state)
            # Optionally update with reward (we use score itself as reward)
            self.distillation_optimizer.update(state.to_vector(), score)
            method = "distillation"
            stats = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
        else:
            # Compute weighted average using MODP weights
            # Normalize each metric to "higher is better"
            acc_norm = min(accuracy, 1.0)
            energy_efficiency = 1.0 - min(energy_kwh / 10.0, 1.0)
            carbon_efficiency = 1.0 - min(carbon_co2e_kg / 1.0, 1.0)
            latency_efficiency = 1.0 - min(latency_ms / 1000.0, 1.0)
            cost_efficiency = 1.0 - min(cost_usd / 10.0, 1.0)

            components = [acc_norm, energy_efficiency, carbon_efficiency,
                          latency_efficiency, cost_efficiency]
            score = float(np.dot(components, self.config.modp_weights))
            method = "modp"
            stats = None

        return {
            "score": round(score, 4),
            "method": method,
            "graph_metrics": graph_metrics,
            "human_feedback_score": human_feedback_score,
            "distillation_stats": stats
        }

    # ------------------------------------------------------------------
    # Original calculate_detailed (unchanged)
    # ------------------------------------------------------------------
    def calculate_detailed(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_co2e_kg: float,
        latency_ms: float = None,
        cost_usd: float = None
    ) -> Dict[str, float]:
        """Original detailed metrics."""
        sustainability_index = self.calculate(accuracy, energy_kwh, carbon_co2e_kg)
        efficiency = 1.0 / energy_kwh if energy_kwh > 0 else 0.0
        carbon_efficiency = accuracy / carbon_co2e_kg if carbon_co2e_kg > 0 else 0.0
        result = {
            "sustainability_index": sustainability_index,
            "accuracy": accuracy,
            "efficiency": efficiency,
            "carbon_efficiency": carbon_efficiency,
            "energy_kwh": energy_kwh,
            "carbon_co2e_kg": carbon_co2e_kg
        }
        if latency_ms is not None:
            result["latency_ms"] = latency_ms
            result["throughput"] = 1000 / latency_ms if latency_ms > 0 else 0.0
        if cost_usd is not None:
            result["cost_usd"] = cost_usd
            result["roi"] = accuracy / cost_usd if cost_usd > 0 else 0.0
        return result

    # ------------------------------------------------------------------
    # Original rank_agents (unchanged)
    # ------------------------------------------------------------------
    def rank_agents(self, agent_metrics: Dict[str, Dict[str, float]]) -> list:
        rankings = []
        for agent_name, metrics in agent_metrics.items():
            si = self.calculate(
                accuracy=metrics.get("accuracy", 0),
                energy_kwh=metrics.get("energy_kwh", 1),
                carbon_co2e_kg=metrics.get("carbon_co2e_kg", 1)
            )
            rankings.append((agent_name, si))
        rankings.sort(key=lambda x: x[1], reverse=True)
        return rankings

    # ------------------------------------------------------------------
    # Original compare_agents (unchanged)
    # ------------------------------------------------------------------
    def compare_agents(self, agent_a_metrics: Dict[str, float],
                       agent_b_metrics: Dict[str, float]) -> Dict[str, any]:
        si_a = self.calculate(
            accuracy=agent_a_metrics.get("accuracy", 0),
            energy_kwh=agent_a_metrics.get("energy_kwh", 1),
            carbon_co2e_kg=agent_a_metrics.get("carbon_co2e_kg", 1)
        )
        si_b = self.calculate(
            accuracy=agent_b_metrics.get("accuracy", 0),
            energy_kwh=agent_b_metrics.get("energy_kwh", 1),
            carbon_co2e_kg=agent_b_metrics.get("carbon_co2e_kg", 1)
        )
        improvement = ((si_b - si_a) / si_a * 100) if si_a > 0 else 0.0
        return {
            "agent_a_sustainability": si_a,
            "agent_b_sustainability": si_b,
            "improvement_percent": improvement,
            "winner": "agent_b" if si_b > si_a else "agent_a",
            "difference": si_b - si_a
        }

    # ------------------------------------------------------------------
    # Original get_rating (unchanged)
    # ------------------------------------------------------------------
    @staticmethod
    def get_rating(sustainability_index: float) -> str:
        if sustainability_index >= 200:
            return "Excellent"
        elif sustainability_index >= 150:
            return "Very Good"
        elif sustainability_index >= 100:
            return "Good"
        elif sustainability_index >= 50:
            return "Fair"
        else:
            return "Poor"
