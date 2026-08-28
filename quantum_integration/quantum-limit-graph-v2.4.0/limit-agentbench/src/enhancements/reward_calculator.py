"""
reward_calculator.py

Enhanced reward calculator with MODP, bio_inspired, moe_system, LIMIT Graph, RLHF, and Multi-Teacher Policy Distillation integration.

Features:
- MODP‑based multi‑objective evaluation (with fallback to weighted sum).
- Bio‑inspired weight adaptation via genetic algorithm.
- MoE context‑aware dynamic weighting.
- LIMIT Graph for constraint propagation and decision support.
- RLHF (Reinforcement Learning from Human Feedback) for reward model updates.
- Multi‑Teacher Policy Distillation to combine teacher policies into a student policy.
- Persistence of weights to JSON.
- Configurable objectives.
- Full objective vector retrieval for MODP and bio modules.
"""

import json
import os
import time
import logging
import numpy as np
from typing import Dict, Any, Optional, List, Callable, Tuple

# Optional imports with fallback stubs
try:
    from .MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

try:
    from .bio_inspired import GeneticOptimizer
except ImportError:
    class GeneticOptimizer:
        def adapt(self, context, reward, weights):
            return weights  # no change

try:
    from .moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def encode(self, task):
            return {}  # no context
        def get_weights(self, task):
            return {}


# =============================================================================
# NEW: LIMIT Graph Manager
# =============================================================================
class LimitGraphManager:
    """
    Maintains a graph of system constraints (carbon, cost, latency, etc.)
    and provides path evaluation for decision support.
    """

    def __init__(self, graph: Optional[Dict[str, Dict[str, float]]] = None):
        self.graph = graph or {
            'carbon': {'cost': 0.8},
            'cost': {'latency': 0.2},
            'latency': {'throughput': -0.5},
            'throughput': {'diversity': 0.1},
            'diversity': {'carbon': -0.3}
        }
        self.constraints: Dict[str, float] = {}

    def update_constraint(self, name: str, value: float):
        self.constraints[name] = value

    def get_constraint(self, name: str) -> float:
        return self.constraints.get(name, 0.0)

    def evaluate_path(self, start: str, end: str) -> float:
        """Compute influence score from start to end using BFS."""
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited = set()
        queue = [(start, 1.0)]
        while queue:
            node, weight = queue.pop(0)
            if node == end:
                return weight
            visited.add(node)
            for neighbor, w in self.graph[node].items():
                if neighbor not in visited:
                    queue.append((neighbor, weight * w))
        return 0.0

    def get_graph_summary(self) -> Dict:
        return {
            'nodes': list(self.graph.keys()),
            'constraints': self.constraints,
            'edge_count': sum(len(v) for v in self.graph.values())
        }


# =============================================================================
# NEW: RLHF Manager
# =============================================================================
class RLHFManager:
    """
    Reinforcement Learning from Human Feedback: learns a reward model from
    feedback events and uses it to adjust reward computation.
    """

    def __init__(self, model: Optional[Any] = None):
        self.feedback_buffer: List[Dict] = []
        self.reward_model = model
        self.policy_weights = np.array([0.25, 0.25, 0.25, 0.25, 0.25])  # matching objective count
        self._trained = model is not None
        self._lock = None  # optional threading lock

    def record_feedback(self, state: Dict, action: str, reward: float):
        self.feedback_buffer.append({
            'state': self._state_to_features(state),
            'action': self._action_to_index(action),
            'reward': reward
        })

    def _state_to_features(self, state: Dict) -> List[float]:
        # Convert state dict to fixed-length vector.
        return [
            state.get('quality', 0.5),
            state.get('throughput', 0.5),
            state.get('energy_efficiency', 0.5),
            state.get('carbon_efficiency', 0.5),
            state.get('memory_efficiency', 0.5),
        ]

    def _action_to_index(self, action: str) -> int:
        actions = ['quality', 'throughput', 'energy_efficiency', 'carbon_efficiency', 'memory_efficiency']
        return actions.index(action) if action in actions else 0

    def train(self):
        """Train reward model using collected feedback (if model supports fit)."""
        if self.reward_model is None or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        try:
            self.reward_model.fit(X, y)
            self._trained = True
            self.feedback_buffer.clear()
        except Exception:
            pass

    def predict_reward(self, state: Dict) -> float:
        """Predict reward using trained model; fallback to weighted sum."""
        if self._trained and self.reward_model is not None:
            features = self._state_to_features(state)
            try:
                return float(self.reward_model.predict([features])[0])
            except Exception:
                pass
        # Fallback: weighted sum of state values.
        return sum(state.get(k, 0) * self.policy_weights[i]
                   for i, k in enumerate(['quality', 'throughput', 'energy_efficiency',
                                          'carbon_efficiency', 'memory_efficiency']))

    def get_policy_probs(self) -> List[float]:
        return self.policy_weights.tolist()


# =============================================================================
# NEW: Multi‑Teacher Policy Distillation
# =============================================================================
class MultiTeacherPolicyDistillation:
    """
    Distills multiple teacher policies (e.g., MODP, bio, MoE) into a single
    student policy that provides a unified set of weights.
    """

    def __init__(self, temperature: float = 2.0, alpha: float = 0.5):
        self.temperature = temperature
        self.alpha = alpha
        self.student_policy = np.array([0.2, 0.2, 0.2, 0.2, 0.2])  # initial uniform
        self.history = []
        self._lock = None

    def distill(self, teacher_policies: List[List[float]]):
        """
        Perform one distillation step from teacher policies.
        teacher_policies: list of probability distributions (each list sums to 1).
        """
        if not teacher_policies:
            return
        # Average teacher distributions (simple, can be improved)
        teacher_avg = np.mean([np.array(p) for p in teacher_policies], axis=0)
        teacher_avg /= teacher_avg.sum()

        # Apply temperature scaling
        softened = np.exp(np.log(teacher_avg + 1e-8) / self.temperature)
        softened /= softened.sum()

        # Gradient update of student policy (simplified)
        loss = -np.sum(softened * np.log(self.student_policy + 1e-8))
        grad = -softened / (self.student_policy + 1e-8)
        lr = 0.01
        self.student_policy -= lr * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        self.history.append(loss)

    def get_student_probs(self) -> List[float]:
        return self.student_policy.tolist()


# =============================================================================
# Enhanced RewardCalculator
# =============================================================================
class RewardCalculator:
    """
    Enhanced reward calculator with MODP, bio, MoE, LIMIT Graph, RLHF,
    and Multi‑Teacher Policy Distillation.
    """

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        modp_optimizer: Optional[Any] = None,
        bio_optimizer: Optional[Any] = None,
        moe_router: Optional[Any] = None,
        limit_graph: Optional[LimitGraphManager] = None,
        rlhf: Optional[RLHFManager] = None,
        distillation: Optional[MultiTeacherPolicyDistillation] = None,
        persistence_file: Optional[str] = "reward_weights.json",
        enable_adaptation: bool = True,
        min_quality_threshold: float = 0.5,
        max_latency_ms: float = 1e9,
    ):
        """
        Args:
            weights: Initial weights for objectives.
            modp_optimizer: MODP optimizer instance (default: ParetoOptimizer).
            bio_optimizer: Bio‑inspired optimizer for weight adaptation (default: GeneticOptimizer).
            moe_router: MoE router for context‑aware weighting (default: ExpertRouter).
            limit_graph: LIMIT Graph manager for constraint propagation.
            rlhf: RLHF manager for reward model integration.
            distillation: Multi‑Teacher Policy Distillation instance.
            persistence_file: Path to save/load weights.
            enable_adaptation: Whether to allow bio‑inspired weight updates.
            min_quality_threshold: Minimum quality to avoid penalty.
            max_latency_ms: Maximum latency before penalty.
        """
        self.logger = logging.getLogger(__name__)

        # Modules (fallback to stubs)
        self.modp = modp_optimizer if modp_optimizer else ParetoOptimizer()
        self.bio = bio_optimizer if bio_optimizer else GeneticOptimizer()
        self.moe = moe_router if moe_router else ExpertRouter()

        # New modules
        self.limit_graph = limit_graph if limit_graph else LimitGraphManager()
        self.rlhf = rlhf if rlhf else RLHFManager()
        self.distillation = distillation if distillation else MultiTeacherPolicyDistillation()

        # Objectives and weights
        self.objective_names = [
            "quality",
            "throughput",
            "energy_efficiency",
            "carbon_efficiency",
            "memory_efficiency"
        ]
        self.weights = weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }
        for obj in self.objective_names:
            if obj not in self.weights:
                self.weights[obj] = 0.0

        self.persistence_file = persistence_file
        self.enable_adaptation = enable_adaptation
        self.min_quality = min_quality_threshold
        self.max_latency_ms = max_latency_ms

        # Load persisted weights
        self._load_weights()

    # --------------------- Persistence ---------------------
    def _load_weights(self):
        if not self.persistence_file or not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
                if "weights" in data:
                    self.weights.update(data["weights"])
                if "last_update" in data:
                    self.last_update = data["last_update"]
            self.logger.info("Loaded weights from %s", self.persistence_file)
        except Exception as e:
            self.logger.warning("Failed to load weights: %s", e)

    def _save_weights(self):
        if not self.persistence_file:
            return
        try:
            data = {
                "weights": self.weights,
                "last_update": time.time(),
            }
            with open(self.persistence_file, "w") as f:
                json.dump(data, f)
            self.logger.debug("Weights saved.")
        except Exception as e:
            self.logger.warning("Failed to save weights: %s", e)

    # --------------------- Objective Extraction ---------------------
    def _extract_objectives(
        self,
        aggregated_metrics: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> Dict[str, float]:
        quality = aggregated_metrics.get("quality_score", 1.0)
        throughput = aggregated_metrics.get("tokens_per_sec", 0.0)
        total_energy_kwh = aggregated_metrics.get("total_energy_kwh", 0.0)
        mem_eff = aggregated_metrics.get("memory_efficiency", 0.0)

        if throughput > 0 and total_energy_kwh > 0:
            carbon_per_token = (total_energy_kwh * carbon_intensity_gco2_kwh) / throughput
            carbon_eff = max(0.0, 1.0 - (carbon_per_token / 100.0))
        else:
            carbon_eff = 0.0

        if total_energy_kwh > 0 and throughput > 0:
            energy_eff = min(1.0, throughput / (total_energy_kwh * 1000))
        else:
            energy_eff = 0.0

        return {
            "quality": min(1.0, max(0.0, quality)),
            "throughput": min(1.0, throughput / 100.0),
            "energy_efficiency": energy_eff,
            "carbon_efficiency": carbon_eff,
            "memory_efficiency": min(1.0, max(0.0, mem_eff)),
        }

    # --------------------- Compute Reward ---------------------
    def compute(
        self,
        aggregated_metrics: Dict[str, Any],
        constraints: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> float:
        # 1. Extract objectives
        objectives = self._extract_objectives(aggregated_metrics, carbon_intensity_gco2_kwh)

        # 2. Determine effective weights
        effective_weights = self.weights.copy()

        # 2a. LIMIT Graph adjustments: if carbon->cost influence is high, boost carbon weight
        carbon_influence = self.limit_graph.evaluate_path('carbon', 'cost')
        if carbon_influence > 0.5:
            # Increase carbon_efficiency weight proportionally
            boost = min(0.2, (carbon_influence - 0.5) * 0.4)
            effective_weights['carbon_efficiency'] += boost
            # Normalize weights
            total = sum(effective_weights.values())
            if total > 0:
                effective_weights = {k: v / total for k, v in effective_weights.items()}

        # 2b. RLHF adjustments: if RLHF model is trained, blend its predicted reward into utility
        rlhf_adjustment = 0.0
        if self.rlhf._trained:
            predicted_reward = self.rlhf.predict_reward(objectives)
            # Scale between 0 and 1, blend with base utility
            rlhf_adjustment = predicted_reward * 0.1  # small influence

        # 2c. Distillation: use student policy as weights if available
        student_probs = self.distillation.get_student_probs()
        if len(student_probs) == len(self.objective_names):
            # Blend student policy with current weights
            blend_factor = 0.3
            effective_weights = {
                k: (1 - blend_factor) * effective_weights[k] + blend_factor * student_probs[i]
                for i, k in enumerate(self.objective_names)
            }

        # 3. Compute base utility using MODP
        utility = self.modp.evaluate(objectives, effective_weights)

        # Add RLHF adjustment
        utility += rlhf_adjustment

        # 4. Penalties
        penalty = 0.0
        if aggregated_metrics.get("gpu_oom", False):
            penalty -= 10.0

        max_latency = constraints.get("max_latency_ms", self.max_latency_ms)
        if aggregated_metrics.get("elapsed_sec", 0) * 1000 > max_latency:
            penalty -= 5.0

        min_quality = constraints.get("min_quality", self.min_quality)
        if objectives["quality"] < min_quality:
            penalty -= 5.0

        # 5. Final reward
        reward = utility + penalty
        reward = max(-10.0, min(10.0, reward))

        return reward

    # --------------------- Adaptation (Bio‑inspired) ---------------------
    def adapt_weights(self, context: Dict[str, Any], reward: float):
        if not self.enable_adaptation:
            return

        moe_context = self.moe.encode(context)
        if moe_context:
            context.update(moe_context)

        new_weights = self.bio.adapt(context, reward, self.weights)
        if new_weights:
            self.weights.update(new_weights)
            self._save_weights()
            self.logger.info("Weights adapted via bio‑inspired optimizer.")

        # Also update distillation with current teacher policies
        # In a real system, you would collect teacher policies from MODP, bio, MoE.
        # Here we approximate by using current weights as a teacher.
        self._update_distillation()

    def _update_distillation(self):
        """Update distillation using current weights and MoE weights."""
        teacher_policies = []
        # Teacher 1: MODP weights (normalized)
        modp_weights = list(self.weights.values())
        modp_weights = np.array(modp_weights) / (sum(modp_weights) + 1e-8)
        teacher_policies.append(modp_weights)

        # Teacher 2: MoE weights if available
        moe_weights = self.moe.get_weights({})  # dummy task
        if moe_weights:
            moe_list = [moe_weights.get(obj, 0.0) for obj in self.objective_names]
            moe_list = np.array(moe_list) / (sum(moe_list) + 1e-8)
            teacher_policies.append(moe_list)

        # Teacher 3: RLHF policy (if trained)
        if self.rlhf._trained:
            teacher_policies.append(self.rlhf.get_policy_probs())

        if teacher_policies:
            self.distillation.distill(teacher_policies)

    # --------------------- Context‑Aware Weight Adjustment (MoE) ---------------------
    def adjust_weights_for_context(self, task: Dict[str, Any]):
        priority = task.get("priority", "normal")
        if priority == "eco":
            self.weights["carbon_efficiency"] = 0.4
            self.weights["throughput"] = 0.1
        elif priority == "speed":
            self.weights["throughput"] = 0.5
            self.weights["carbon_efficiency"] = 0.1

        # MoE dynamic weights (if available)
        moe_weights = self.moe.get_weights(task)
        if moe_weights:
            self.weights.update(moe_weights)

        self._save_weights()

    # --------------------- Utility Methods ---------------------
    def get_objectives(
        self,
        aggregated_metrics: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> Dict[str, float]:
        return self._extract_objectives(aggregated_metrics, carbon_intensity_gco2_kwh)

    def get_weights(self) -> Dict[str, float]:
        return self.weights.copy()

    def reset_weights(self, weights: Optional[Dict[str, float]] = None):
        if weights:
            self.weights.update(weights)
        else:
            self.weights = {
                "quality": 0.30,
                "throughput": 0.25,
                "energy_efficiency": 0.20,
                "carbon_efficiency": 0.15,
                "memory_efficiency": 0.10,
            }
        self._save_weights()

    def record_feedback(self, state: Dict, action: str, reward: float):
        """Record human feedback for RLHF."""
        self.rlhf.record_feedback(state, action, reward)

    def train_rlhf(self):
        """Train RLHF model on collected feedback."""
        self.rlhf.train()

    def get_distillation_probs(self) -> List[float]:
        return self.distillation.get_student_probs()
