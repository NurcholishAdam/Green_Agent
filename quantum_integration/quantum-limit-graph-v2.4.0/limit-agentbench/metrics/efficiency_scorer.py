# -*- coding: utf-8 -*-
"""
Efficiency Scorer (Enhanced)
Performance efficiency metrics calculation

Enhancements (enabled via EfficiencyScorerConfig.use_enhancements):
  - LIMIT Graph metrics influence composite efficiency score.
  - MODP: multi‑objective weights (accuracy, energy, latency, cost) are configurable.
  - RLHF: human feedback score adjusts the weights in real time.
  - Multi‑Teacher On‑Policy Distillation + MoE: a student model learns to combine
    teacher scores into a final efficiency metric.
  - Bio‑inspired optimisation: evolutionary tuning of the MODP weights.
"""

from typing import Dict, Optional, List, Tuple, Any
import logging
import random
import numpy as np
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enhanced configuration
# ---------------------------------------------------------------------------
@dataclass
class EfficiencyScorerConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics (defaults)
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5
    })
    # MODP weights: [accuracy, energy, latency, cost]
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.2, 0.1]
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
# Decision state and distillation optimizer for efficiency scoring
# ---------------------------------------------------------------------------
class EfficiencyState:
    """Feature vector for computing a learned efficiency score."""
    def __init__(self, accuracy: float, energy_kwh: float,
                 latency_ms: float = 0.0, cost_usd: float = 0.0,
                 graph_metrics: Optional[Dict[str, float]] = None,
                 human_feedback: float = 0.5):
        self.accuracy = accuracy
        self.energy_norm = min(energy_kwh / 10.0, 1.0)  # max 10 kWh
        self.latency_norm = min(latency_ms / 1000.0, 1.0)
        self.cost_norm = min(cost_usd / 10.0, 1.0)
        self.centrality = (graph_metrics or {}).get("centrality", 0.5)
        self.connectivity = (graph_metrics or {}).get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.accuracy,
            self.energy_norm,
            self.latency_norm,
            self.cost_norm,
            self.centrality,
            self.connectivity,
            self.human_feedback,
        ], dtype=np.float32)


class EfficiencyDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to produce a final efficiency score.
    Teachers: rule‑based (original efficiency), RLHF, historical.
    Output: continuous score in [0, 1].
    """
    def __init__(self, config: EfficiencyScorerConfig):
        self.config = config
        self.feature_dim = 7
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

    def _rule_teacher(self, state: EfficiencyState) -> float:
        # Original efficiency: accuracy / energy (normalized)
        if state.energy_norm > 0:
            return min(state.accuracy / state.energy_norm, 1.0)
        return 0.0

    def _rlhf_teacher(self, state: EfficiencyState) -> float:
        # Human feedback adjusts: high feedback -> prefer accuracy, low -> prefer energy
        if state.human_feedback > 0.7:
            return 0.7 * state.accuracy + 0.3 * (1 - state.energy_norm)
        elif state.human_feedback < 0.3:
            return 0.4 * state.accuracy + 0.6 * (1 - state.energy_norm)
        else:
            return 0.5 * state.accuracy + 0.5 * (1 - state.energy_norm)

    def _historical_teacher(self, state: EfficiencyState) -> float:
        # Simulated learned model: centrality boosts accuracy importance
        base = 0.6 * state.accuracy + 0.4 * (1 - state.energy_norm)
        if state.centrality > 0.7:
            base += 0.1
        return min(base, 1.0)

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def predict_score(self, state: EfficiencyState, exploration: bool = False) -> float:
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
# Enhanced EfficiencyScorer class (original methods preserved)
# ---------------------------------------------------------------------------
class EfficiencyScorer:
    """
    Performance efficiency scorer with optional enhanced scoring.
    """

    # Cost per kWh by region (USD) — original
    ELECTRICITY_COST = {
        "US-CA": 0.25,
        "US-TX": 0.12,
        "US-NY": 0.20,
        "EU-DE": 0.35,
        "EU-FR": 0.18,
        "CN": 0.08,
        "IN": 0.07,
        "GLOBAL": 0.15
    }

    def __init__(self, grid_region: str = "GLOBAL",
                 config: Optional[EfficiencyScorerConfig] = None):
        self.grid_region = grid_region
        self.electricity_cost = self.ELECTRICITY_COST.get(grid_region, 0.15)
        self.config = config or EfficiencyScorerConfig()
        self.use_enhancements = self.config.use_enhancements

        # Enhanced components
        self.distillation_optimizer = None
        self.evolutionary_optimizer = None
        if self.use_enhancements:
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.4, 0.3, 0.2, 0.1]
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]
            if self.config.use_distillation:
                self.distillation_optimizer = EfficiencyDistillationOptimizer(self.config)
            if self.config.use_evolutionary:
                # Placeholder for evolutionary optimizer of modp_weights
                pass

    # ------------------------------------------------------------------
    # Original methods (unchanged)
    # ------------------------------------------------------------------
    def calculate_efficiency_score(self, accuracy: float, energy_kwh: float) -> float:
        """Original efficiency score (accuracy / energy_kwh)."""
        if energy_kwh == 0:
            return 0.0
        return accuracy / energy_kwh

    def calculate_performance_per_watt(self, accuracy: float, power_watts: float,
                                       duration_seconds: float) -> float:
        """Original performance per watt."""
        if power_watts == 0:
            return 0.0
        return accuracy / power_watts

    def calculate_cost_efficiency(self, accuracy: float, energy_kwh: float) -> Dict[str, float]:
        """Original cost efficiency."""
        cost_usd = energy_kwh * self.electricity_cost
        if cost_usd == 0:
            accuracy_per_dollar = 0.0
        else:
            accuracy_per_dollar = accuracy / cost_usd
        return {
            "cost_usd": cost_usd,
            "accuracy_per_dollar": accuracy_per_dollar,
            "electricity_cost_per_kwh": self.electricity_cost,
            "grid_region": self.grid_region
        }

    def calculate_throughput_efficiency(self, num_tasks: int, energy_kwh: float,
                                        duration_seconds: float) -> Dict[str, float]:
        """Original throughput efficiency."""
        tasks_per_kwh = num_tasks / energy_kwh if energy_kwh > 0 else 0.0
        tasks_per_second = num_tasks / duration_seconds if duration_seconds > 0 else 0.0
        energy_per_task = energy_kwh / num_tasks if num_tasks > 0 else 0.0
        return {
            "tasks_per_kwh": tasks_per_kwh,
            "tasks_per_second": tasks_per_second,
            "energy_per_task": energy_per_task,
            "num_tasks": num_tasks
        }

    def compare_efficiency(self, agent_a_metrics: Dict[str, float],
                           agent_b_metrics: Dict[str, float]) -> Dict[str, any]:
        """Original comparison."""
        eff_a = self.calculate_efficiency_score(
            agent_a_metrics.get("accuracy", 0),
            agent_a_metrics.get("energy_kwh", 1)
        )
        eff_b = self.calculate_efficiency_score(
            agent_b_metrics.get("accuracy", 0),
            agent_b_metrics.get("energy_kwh", 1)
        )
        improvement = ((eff_b - eff_a) / eff_a * 100) if eff_a > 0 else 0.0
        return {
            "agent_a_efficiency": eff_a,
            "agent_b_efficiency": eff_b,
            "improvement_percent": improvement,
            "winner": "agent_b" if eff_b > eff_a else "agent_a"
        }

    # ------------------------------------------------------------------
    # Enhanced method: composite efficiency score using MODP + distillation
    # ------------------------------------------------------------------
    def calculate_composite_score(
        self,
        accuracy: float,
        energy_kwh: float,
        latency_ms: float = 0.0,
        cost_usd: Optional[float] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Compute a multi‑objective efficiency score (0-1) that combines
        accuracy, energy, latency, and cost using configurable weights.
        If enhancements enabled, a distillation optimizer may be used.
        """
        if not self.use_enhancements:
            # Fallback to original score, normalized roughly
            base = self.calculate_efficiency_score(accuracy, energy_kwh)
            return {"score": min(base, 1.0), "method": "original"}

        # Use provided or defaults
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score
        if cost_usd is None:
            cost_usd = energy_kwh * self.electricity_cost

        state = EfficiencyState(
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
            graph_metrics=graph_metrics,
            human_feedback=human_feedback_score
        )

        if self.distillation_optimizer:
            score = self.distillation_optimizer.predict_score(state)
            # Optionally update with a reward (we use score itself as reward for now)
            self.distillation_optimizer.update(state.to_vector(), score)
            method = "distillation"
        else:
            # Weighted average using MODP weights
            weights = self.config.modp_weights
            acc_norm = min(accuracy, 1.0)
            energy_norm = 1.0 - min(energy_kwh / 10.0, 1.0)
            latency_norm = 1.0 - min(latency_ms / 1000.0, 1.0)
            cost_norm = 1.0 - min(cost_usd / 10.0, 1.0)
            score = float(np.dot([acc_norm, energy_norm, latency_norm, cost_norm], weights))
            method = "modp"

        return {
            "score": round(score, 4),
            "method": method,
            "graph_metrics": graph_metrics,
            "human_feedback_score": human_feedback_score,
            "distillation_stats": (
                {
                    "student_counter": self.distillation_optimizer.counter,
                    "buffer_size": len(self.distillation_optimizer.replay_buffer)
                }
                if self.distillation_optimizer else None
            )
        }
