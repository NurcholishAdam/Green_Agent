# src/enhancements/cost_function/sustainability_cost.py
"""
Enhanced Sustainability Cost Function v2.4.0
=============================================
Multi‑objective sustainability cost function with Multi‑Teacher On‑Policy Distillation
for adaptive weight selection, plus an optional Multi‑Objective Evolutionary Optimizer (MOEA)
to globally tune the six weights. The MOEA (NSGA‑II) maintains a Pareto front of
non‑dominated weight vectors, and MODP (scalarization with dynamic weights) selects
the best compromise based on current system state.

ENHANCEMENTS OVER v2.3.0:
- Added LIMIT Graph manager for cost component relationships.
- Added explicit MODP solver wrapper.
- Added RLHF trainer for human preference collection.
- Added MoE gating network (mixture-of-experts) for weight strategy blending.
- Integration with central Storage (optional) for persisting new data.
- New configuration flags for enabling/disabling each component.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Union, List, Tuple
from datetime import datetime, timedelta
from collections import OrderedDict, deque
import numpy as np
import random
import copy
from abc import ABC, abstractmethod
from pathlib import Path
import uuid
import time

# ---------- Local imports ----------
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector
from ..expert_registry import ExpertProfile  # optional

# ---------- Optional central components ----------
try:
    from ...storage import Storage  # Adjust path if needed
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    CENTRAL_STORAGE_AVAILABLE = False
    Storage = None

# ---------- Optional adaptive cost function ----------
try:
    from ..adaptive_cost_function import AdaptiveCostFunction
    ADAPTIVE_AVAILABLE = True
except ImportError:
    ADAPTIVE_AVAILABLE = False

# ---------- Prometheus metrics ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

logger = logging.getLogger(__name__)

# ---------- Configuration (Pydantic) ----------
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

if PYDANTIC_AVAILABLE:
    class CostConfig(BaseModel):
        """Configuration for the sustainability cost function."""
        # Weights (initial)
        energy_weight: float = Field(0.2, ge=0, le=1)
        carbon_weight: float = Field(0.3, ge=0, le=1)
        helium_weight: float = Field(0.15, ge=0, le=1)
        material_weight: float = Field(0.15, ge=0, le=1)
        latency_weight: float = Field(0.1, ge=0, le=1)
        accuracy_weight: float = Field(0.1, ge=0, le=1)
        # Normalization baselines and maxima
        latency_baseline_ms: float = Field(1000.0, gt=0)
        latency_max_ms: float = Field(5000.0, gt=0)
        accuracy_baseline: float = Field(0.9, gt=0, le=1)
        accuracy_max: float = Field(1.0, gt=0, le=1)
        energy_baseline_joules: float = Field(0.0001, gt=0)
        energy_max_joules: float = Field(0.001, gt=0)
        carbon_intensity_baseline_kg_per_kwh: float = Field(0.4, gt=0)
        carbon_intensity_max_kg_per_kwh: float = Field(1.0, gt=0)
        helium_scarcity_threshold: float = Field(0.7, ge=0, le=1)
        helium_max_scarcity: float = Field(1.0, ge=0, le=1)
        material_embodied_norm: float = Field(200.0, gt=0)
        material_rare_earth_norm: float = Field(0.01, gt=0)
        material_max_composite: float = Field(1.0, gt=0)
        # Carbon caching
        carbon_cache_ttl_seconds: int = Field(300, ge=0)
        carbon_cache_max_size: int = Field(100, ge=1)
        # Integration flags
        use_adaptive_weights: bool = Field(False)
        integrate_anomaly_detection: bool = Field(False)
        integrate_predictive_maintenance: bool = Field(False)

        # Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # NEW: MOEA parameters
        moea_enabled: bool = Field(True)
        moea_interval_seconds: int = Field(300, ge=60)
        moea_population_size: int = Field(30, ge=10)
        moea_generations: int = Field(10, ge=2)
        moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        moea_tournament_size: int = Field(3, ge=2)
        moea_objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'energy': 0.2,
                'carbon': 0.3,
                'helium': 0.15,
                'material': 0.15,
                'latency': 0.1,
                'accuracy': 0.1,
            }
        )
        moea_dynamic_weights: bool = Field(True)

        # NEW v2.4.0 flags
        enable_limit_graph: bool = Field(True)
        enable_modp: bool = Field(True)
        enable_rlhf: bool = Field(True)
        enable_moe: bool = Field(True)
        moe_expert_count: int = Field(4, ge=2)

        @validator('energy_weight', 'carbon_weight', 'helium_weight', 'material_weight', 'latency_weight', 'accuracy_weight')
        def weights_sum_one(cls, v, values):
            weights = [v] + [values.get(k, 0) for k in ['carbon_weight', 'helium_weight', 'material_weight', 'latency_weight', 'accuracy_weight']]
            total = sum(weights)
            if abs(total - 1.0) > 1e-6:
                raise ValueError("All weights must sum to 1")
            return v

        class Config:
            env_prefix = "COST_"
else:
    COST_CONFIG = {
        "energy_weight": 0.2,
        "carbon_weight": 0.3,
        "helium_weight": 0.15,
        "material_weight": 0.15,
        "latency_weight": 0.1,
        "accuracy_weight": 0.1,
        "latency_baseline_ms": 1000.0,
        "latency_max_ms": 5000.0,
        "accuracy_baseline": 0.9,
        "accuracy_max": 1.0,
        "energy_baseline_joules": 0.0001,
        "energy_max_joules": 0.001,
        "carbon_intensity_baseline_kg_per_kwh": 0.4,
        "carbon_intensity_max_kg_per_kwh": 1.0,
        "helium_scarcity_threshold": 0.7,
        "helium_max_scarcity": 1.0,
        "material_embodied_norm": 200.0,
        "material_rare_earth_norm": 0.01,
        "material_max_composite": 1.0,
        "carbon_cache_ttl_seconds": 300,
        "carbon_cache_max_size": 100,
        "use_adaptive_weights": False,
        "integrate_anomaly_detection": False,
        "integrate_predictive_maintenance": False,
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "moea_enabled": True,
        "moea_interval_seconds": 300,
        "moea_population_size": 30,
        "moea_generations": 10,
        "moea_mutation_rate": 0.2,
        "moea_crossover_rate": 0.8,
        "moea_tournament_size": 3,
        "moea_objective_weights": {
            'energy': 0.2,
            'carbon': 0.3,
            'helium': 0.15,
            'material': 0.15,
            'latency': 0.1,
            'accuracy': 0.1,
        },
        "moea_dynamic_weights": True,
        "enable_limit_graph": True,
        "enable_modp": True,
        "enable_rlhf": True,
        "enable_moe": True,
        "moe_expert_count": 4,
    }


# ============================================================================
# NEW: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """
    Manages a graph of cost component dependencies for LIMIT.
    Nodes are cost components (energy, carbon, helium, material, latency, accuracy),
    edges represent trade-offs or correlations.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.graphs = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ============================================================================
# NEW: MODP Optimizer
# ============================================================================
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that can be used to
    combine Pareto front with dynamic weights.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.states = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_transition(self, transition_id: str, problem_id: str, from_state: str,
                       to_state: str, action: str, cost: float,
                       objective_deltas: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_transition'):
            self.storage.save_modp_transition(transition_id, problem_id, from_state, to_state, action, cost, objective_deltas)

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_transitions(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_transitions'):
            return self.storage.get_modp_transitions(problem_id)
        return []

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []

    async def solve(self, problem_id: str, initial_state: Dict[str, Any], max_stages: int = 5) -> Dict[str, Any]:
        """Simplified DP solver; just stores initial state and returns empty front."""
        self.add_state(
            state_id=f"{problem_id}_init",
            problem_id=problem_id,
            state_attributes=initial_state,
            objective_values={"energy": 0.0, "carbon": 0.0, "helium": 0.0,
                              "material": 0.0, "latency": 0.0, "accuracy": 0.0},
            stage=0
        )
        return {"status": "solved", "pareto_front": []}


# ============================================================================
# NEW: RLHF Trainer
# ============================================================================
class RLHFTrainer:
    """
    Collects human preference pairs for cost weight strategies.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")


# ============================================================================
# NEW: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating for weight strategy selection.
    Experts are specialized strategies: carbon_focus, energy_focus, helium_focus,
    material_focus, adaptive. The gating network learns to blend them.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.num_experts = self.config.get('moe_expert_count', 4)
        # Expert names: we'll use the same action space as distillation
        self.expert_names = ['carbon_focus', 'energy_focus', 'helium_focus', 'adaptive'][:self.num_experts]
        # Gating weights: (num_experts, 12) because state dimension is 12
        self.gating_weights = np.random.randn(self.num_experts, 12)
        self._training_samples = []

    def _encode_state(self, state: Union['CostOptimizationState', Dict]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                min(state.get('carbon_intensity', 0) / 1.0, 1.0),
                state.get('node_health', 1.0),
                min(state.get('workload_tokens', 0) / 10000.0, 1.0),
                min(state.get('latency_target', 0) / 5000.0, 1.0),
                state.get('anomaly_severity', 0),
                state.get('avg_cost_trend', 0),
                min(state.get('cost_variance', 0) / 0.5, 1.0),
                state.get('weight_carbon', 0.3),
                state.get('weight_energy', 0.2),
                state.get('weight_helium', 0.15),
                state.get('hour_of_day', 0) / 24.0,
            ]
        else:
            features = [
                min(state.carbon_intensity / 1.0, 1.0),
                state.node_health,
                min(state.workload_tokens / 10000.0, 1.0),
                min(state.latency_target / 5000.0, 1.0),
                state.anomaly_severity,
                state.avg_cost_trend,
                min(state.cost_variance / 0.5, 1.0),
                state.weight_carbon,
                state.weight_energy,
                state.weight_helium,
                state.hour_of_day / 24.0,
            ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state: Union['CostOptimizationState', Dict]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        # Log routing if storage available
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, state: Union['CostOptimizationState', Dict], selected_expert: str, reward: float):
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ============================================================================
# Distillation Components for Weight Selection
# ============================================================================
@dataclass
class CostOptimizationState:
    """State for the distillation agent."""
    carbon_intensity: float
    node_health: float
    workload_tokens: float
    latency_target: float
    anomaly_severity: float
    avg_cost_trend: float
    cost_variance: float
    weight_carbon: float
    weight_energy: float
    weight_helium: float
    hour_of_day: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 12‑dim numeric feature vector."""
        features = [
            min(self.carbon_intensity / 1.0, 1.0),
            self.node_health,
            min(self.workload_tokens / 10000.0, 1.0),
            min(self.latency_target / 5000.0, 1.0),
            self.anomaly_severity,
            self.avg_cost_trend,
            min(self.cost_variance / 0.5, 1.0),
            self.weight_carbon,
            self.weight_energy,
            self.weight_helium,
            self.hour_of_day / 24.0,
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CostOptimizationState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: CostOptimizationState) -> float:
        pass


class CostRuleBasedTeacher(Teacher):
    ACTION_SPACE = ['standard', 'carbon_focus', 'energy_focus', 'helium_focus', 'adaptive']

    def predict(self, state: CostOptimizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.anomaly_severity > 0.7:
            probs[1] = 0.8
        elif state.carbon_intensity > 0.8:
            probs[1] = 0.7
        elif state.workload_tokens > 5000:
            probs[2] = 0.6
        elif state.node_health < 0.5:
            probs[3] = 0.6
        else:
            probs[4] = 0.6
        return probs / probs.sum()

    def confidence(self, state: CostOptimizationState) -> float:
        if state.anomaly_severity > 0.7:
            return 0.6
        return 0.4


class CostHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_ML:
            try:
                import joblib
                self.model = joblib.load(model_path)
            except ImportError:
                logger.warning("joblib not available; historical ML teacher disabled")
                self.model = None

    def predict(self, state: CostOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: CostOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class CostStatefulQTeacher(Teacher):
    def __init__(self, cost_func: 'SustainabilityCostFunction', lr: float = 0.1):
        self.cost_func = cost_func
        self.lr = lr
        self.weights = np.zeros((12, 5))  # 12 features, 5 actions

    def predict(self, state: CostOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: CostOptimizationState) -> float:
        return 0.5

    def update(self, state: CostOptimizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationCostOptimizer:
    ACTION_SPACE = ['standard', 'carbon_focus', 'energy_focus', 'helium_focus', 'adaptive']

    def __init__(self, cost_func: 'SustainabilityCostFunction', config: Dict[str, Any]):
        self.cost_func = cost_func
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            CostRuleBasedTeacher(),
            CostHistoricalMLTeacher(),
            CostStatefulQTeacher(cost_func)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: CostOptimizationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(5) / 5

        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# NEW: Multi‑Objective Evolutionary Optimizer (NSGA‑II)
# ============================================================================
@dataclass
class MOPDPoint:
    policy_id: str
    parameters: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'policy_id': self.policy_id,
            'parameters': self.parameters,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }


class NSGAIIOptimizer:
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
                 parameter_bounds: Dict[str, Tuple[float, float]],
                 population_size: int = 30,
                 generations: int = 10,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}
        self._all_points: List[MOPDPoint] = []

    def _random_individual(self) -> Dict[str, float]:
        ind = {}
        for name, (low, high) in self.parameter_bounds.items():
            ind[name] = random.uniform(low, high)
        total = sum(ind.values())
        if total > 0:
            ind = {k: v / total for k, v in ind.items()}
        return ind

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for name in self.parameter_bounds:
            if random.random() < 0.5:
                low, high = self.parameter_bounds[name]
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(low, min(high, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[name] = mutant[name] + delta * (high - low)
                mutant[name] = max(low, min(high, mutant[name]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]) -> List[List[MOPDPoint]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}

        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1

            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)

        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDPoint]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDPoint]],
                              crowding: Dict[int, float]) -> Dict:
        candidates = random.sample(population, self.tournament_size)
        ind_to_point = {}
        for ind, point in zip(population, self._all_points):
            ind_to_point[id(ind)] = point

        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            point = ind_to_point.get(id(cand))
            if not point:
                continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi
                    break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys = list(weights.keys())
        if not obj_keys:
            return weights
        avg = {k: np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val = {k: np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k] > 0 and avg[k] < 0.5 * max_val[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto: List[MOPDPoint], weights: Dict[str, float]) -> Optional[MOPDPoint]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self) -> List[MOPDPoint]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        for ind in population:
            obj = await self.evaluate_func(ind)
            point = MOPDPoint(
                policy_id=str(uuid.uuid4()),
                parameters=ind,
                objectives=obj
            )
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj

        self._all_points = points
        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)

            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population, fronts, crowding)
                parent2 = self._tournament_selection(population, fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)

            child_points = []
            for ind in offspring:
                key = tuple(sorted(ind.items()))
                if key in self._eval_cache:
                    obj = self._eval_cache[key]
                else:
                    obj = await self.evaluate_func(ind)
                    self._eval_cache[key] = obj
                point = MOPDPoint(
                    policy_id=str(uuid.uuid4()),
                    parameters=ind,
                    objectives=obj
                )
                child_points.append(point)

            combined_inds = population + offspring
            combined_points = points + child_points
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")

        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.parameters
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# MAIN COST FUNCTION (Enhanced with new components)
# ============================================================================
class SustainabilityCostFunction:
    """
    Enhanced multi‑objective sustainability cost function with adaptive weight selection
    via multi‑teacher distillation, optional MoE gating, MOEA refinement, LIMIT Graph,
    MODP solver, and RLHF preference collection.
    """

    def __init__(
        self,
        carbon_fetcher: CarbonIntensityFetcher,
        material_updater: MaterialFootprintUpdater,
        helium_collector: HeliumCollector,
        config: Optional[Union[Dict[str, Any], CostConfig]] = None,
        adaptive_cost_function: Optional['AdaptiveCostFunction'] = None,
        anomaly_detector: Optional[Any] = None,
        predictive_maintenance: Optional[Any] = None,
        storage: Optional[Storage] = None,
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        moe_expert_count: int = 4,
    ):
        """
        Initialize the cost function with optional new components.

        Args:
            ... (existing arguments)
            storage: Central Storage instance (optional).
            enable_limit_graph: Enable LIMIT Graph management.
            enable_modp: Enable MODP solver.
            enable_rlhf: Enable RLHF preference collection.
            enable_moe: Enable MoE gating network.
            moe_expert_count: Number of experts in MoE.
        """
        # Configuration
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = CostConfig()
            else:
                self.config = COST_CONFIG.copy()
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = CostConfig(**config)
            else:
                self.config = config.copy()
        else:
            self.config = config

        # Dependencies
        self.carbon = carbon_fetcher
        self.material = material_updater
        self.helium = helium_collector
        self.adaptive_cost = adaptive_cost_function
        self.anomaly_detector = anomaly_detector
        self.predictive_maintenance = predictive_maintenance

        # Base weights
        self._base_weights = self._get_initial_weights()
        self._current_weights = self._base_weights.copy()

        # Carbon cache
        self._carbon_cache: OrderedDict[str, Tuple[float, datetime]] = OrderedDict()
        self._carbon_cache_ttl = self._get_config('carbon_cache_ttl_seconds', 300)
        self._carbon_cache_max_size = self._get_config('carbon_cache_max_size', 100)

        # Metrics
        if PROMETHEUS_AVAILABLE:
            self.metrics = {
                'energy': Histogram('cost_energy', 'Energy cost component (normalized)'),
                'carbon': Histogram('cost_carbon', 'Carbon cost component (normalized)'),
                'helium': Histogram('cost_helium', 'Helium cost component (normalized)'),
                'material': Histogram('cost_material', 'Material cost component (normalized)'),
                'latency': Histogram('cost_latency', 'Latency cost component (normalized)'),
                'accuracy': Histogram('cost_accuracy', 'Accuracy cost component (normalized)'),
                'total': Histogram('cost_total', 'Total sustainability cost'),
                'weights': Gauge('cost_weights', 'Current weights', ['component']),
                'anomaly_triggered': Counter('cost_anomaly_triggered', 'Anomaly triggered weight adjustments'),
                'moea_pareto_front': Gauge('cost_moea_pareto_front', 'MOEA Pareto front size'),
            }
        else:
            self.metrics = None

        self._last_anomaly_time: Optional[datetime] = None
        self._anomaly_cooldown = timedelta(seconds=300)

        # Distillation optimizer
        self.distillation_config = {
            'distillation_epsilon': self._get_config('distillation_epsilon', 0.1),
            'distillation_train_every': self._get_config('distillation_train_every', 10),
            'distillation_replay_size': self._get_config('distillation_replay_size', 2000),
            'distillation_learning_rate': self._get_config('distillation_learning_rate', 0.01),
            'distill_weight': self._get_config('distill_weight', 0.7),
            'rl_weight': self._get_config('rl_weight', 0.3),
        }
        self.policy_optimizer = DistillationCostOptimizer(self, self.distillation_config)

        self._last_total_cost: Optional[float] = None
        self._cost_history = deque(maxlen=50)

        # MOEA attributes
        self.moea_enabled = self._get_config('moea_enabled', True)
        self.moea_interval_seconds = self._get_config('moea_interval_seconds', 300)
        self.moea_optimizer: Optional[NSGAIIOptimizer] = None
        self.moea_pareto_front: List[MOPDPoint] = []
        self.moea_best_weights: Optional[Dict[str, float]] = None
        self._moea_task: Optional[asyncio.Task] = None
        self._last_node_desc: Optional[NodeDescriptor] = None
        self._last_workload: Optional[WorkloadDescriptor] = None
        self._last_expert_profile: Optional[ExpertProfile] = None

        # NEW v2.4.0 components
        self.storage = storage
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(storage, {'moe_expert_count': moe_expert_count}) if enable_moe else None

        logger.info("SustainabilityCostFunction v2.4.0 initialized with config: %s", self.config)

        # Start MOEA background task if enabled
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        # Initialize LIMIT Graph if enabled
        if self.limit_graph_manager:
            self._init_limit_graph()

    def _init_limit_graph(self):
        """Create a default cost component graph."""
        graph_id = "cost_components"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Sustainability Cost Component Dependencies", {})
            # Add nodes
            for comp in ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']:
                self.limit_graph_manager.add_node(graph_id, f"node_{comp}", comp, {"weight": self._base_weights.get(comp, 0.1)})
            # Add edges (trade-offs)
            self.limit_graph_manager.add_edge(graph_id, "edge_energy_carbon", "node_energy", "node_carbon", 0.8, {})
            self.limit_graph_manager.add_edge(graph_id, "edge_carbon_helium", "node_carbon", "node_helium", 0.3, {})
            logger.info("Initialized LIMIT Graph for cost components.")

    def _get_config(self, key: str, default: Any = None) -> Any:
        """Safely get a config value."""
        if hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        return self.config.get(key, default)

    def _get_initial_weights(self) -> Dict[str, float]:
        """Extract initial weights from config and normalize."""
        weights = {
            'energy': self._get_config('energy_weight', 0.2),
            'carbon': self._get_config('carbon_weight', 0.3),
            'helium': self._get_config('helium_weight', 0.15),
            'material': self._get_config('material_weight', 0.15),
            'latency': self._get_config('latency_weight', 0.1),
            'accuracy': self._get_config('accuracy_weight', 0.1),
        }
        total = sum(weights.values())
        if total != 1.0 and total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    # ---------- Normalization helpers ----------
    def _normalize_energy(self, energy_joules: float) -> float:
        baseline = self._get_config('energy_baseline_joules', 0.0001)
        max_val = self._get_config('energy_max_joules', 0.001)
        val = max(0.0, min(max_val, energy_joules))
        if max_val == baseline:
            return 0.0
        return (val - baseline) / (max_val - baseline)

    def _normalize_carbon(self, carbon_kg: float) -> float:
        baseline = self._get_config('carbon_intensity_baseline_kg_per_kwh', 0.4)
        max_val = self._get_config('carbon_intensity_max_kg_per_kwh', 1.0)
        val = max(0.0, min(max_val, carbon_kg))
        if max_val == baseline:
            return 0.0
        return (val - baseline) / (max_val - baseline)

    def _normalize_helium(self, helium_cost: float) -> float:
        max_val = self._get_config('helium_max_scarcity', 1.0)
        val = max(0.0, min(max_val, helium_cost))
        return val / max_val if max_val > 0 else 0.0

    def _normalize_material(self, material_composite: float) -> float:
        max_val = self._get_config('material_max_composite', 1.0)
        val = max(0.0, min(max_val, material_composite))
        return val / max_val if max_val > 0 else 0.0

    def _normalize_latency(self, latency_ms: float) -> float:
        baseline = self._get_config('latency_baseline_ms', 1000.0)
        max_val = self._get_config('latency_max_ms', 5000.0)
        val = max(0.0, min(max_val, latency_ms))
        if max_val == baseline:
            return 0.0
        return (val - baseline) / (max_val - baseline)

    def _normalize_accuracy(self, accuracy: float) -> float:
        baseline = self._get_config('accuracy_baseline', 0.9)
        max_val = self._get_config('accuracy_max', 1.0)
        val = max(baseline, min(max_val, accuracy))
        if max_val == baseline:
            return 0.0
        return (max_val - val) / (max_val - baseline)

    # ---------- Core computation ----------
    async def compute(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> float:
        """
        Compute the sustainability cost for a given node and workload.
        Uses MoE gating if enabled, else distillation to select the weight strategy.
        """
        # Store last inputs for MOEA evaluation
        self._last_node_desc = node_desc
        self._last_workload = workload
        self._last_expert_profile = expert_profile

        # --- Energy cost ---
        if self._get_config('integrate_predictive_maintenance', False) and self.predictive_maintenance:
            try:
                eff_factor = await self.predictive_maintenance.get_efficiency_factor(node_desc.id)
                if eff_factor is not None and eff_factor > 0:
                    energy_used = node_desc.energy_per_token * workload.tokens / eff_factor
                else:
                    energy_used = node_desc.energy_per_token * workload.tokens
            except Exception as e:
                logger.warning(f"Predictive maintenance efficiency factor failed: {e}")
                energy_used = node_desc.energy_per_token * workload.tokens
        else:
            energy_used = node_desc.energy_per_token * workload.tokens

        energy_cost = self._normalize_energy(energy_used)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['energy'].observe(energy_cost)

        # --- Carbon cost ---
        carbon_intensity = await self._get_carbon_intensity(node_desc.region)
        carbon_kg = energy_used * carbon_intensity
        carbon_cost = self._normalize_carbon(carbon_kg)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['carbon'].observe(carbon_cost)

        # --- Helium cost ---
        helium_scarcity = await self._get_helium_scarcity(node_desc)
        helium_base = (1 - node_desc.helium_connectivity_score) * 0.5
        if helium_scarcity > self._get_config('helium_scarcity_threshold', 0.7):
            helium_base *= (1 + helium_scarcity)
        helium_cost = self._normalize_helium(helium_base)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['helium'].observe(helium_cost)

        # --- Material cost ---
        material_composite = await self._get_material_composite(node_desc)
        material_cost = self._normalize_material(material_composite)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['material'].observe(material_cost)

        # --- Latency cost ---
        latency_cost = self._normalize_latency(workload.latency_target)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['latency'].observe(latency_cost)

        # --- Accuracy cost ---
        if expert_profile:
            acc = expert_profile.accuracy_score
        else:
            acc = self._get_config('accuracy_baseline', 0.9)
        accuracy_cost = self._normalize_accuracy(acc)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['accuracy'].observe(accuracy_cost)

        # --- Get weights via MoE or distillation ---
        weights = await self._get_weights(node_desc, workload, anomaly_detected=False)

        # --- Total cost ---
        total = (
            weights['energy'] * energy_cost +
            weights['carbon'] * carbon_cost +
            weights['helium'] * helium_cost +
            weights['material'] * material_cost +
            weights['latency'] * latency_cost +
            weights['accuracy'] * accuracy_cost
        )
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['total'].observe(total)
            for k, v in weights.items():
                self.metrics['weights'].labels(component=k).set(v)

        # --- Compute reward and update agent ---
        baseline_weights = self._base_weights
        baseline_total = (
            baseline_weights['energy'] * energy_cost +
            baseline_weights['carbon'] * carbon_cost +
            baseline_weights['helium'] * helium_cost +
            baseline_weights['material'] * material_cost +
            baseline_weights['latency'] * latency_cost +
            baseline_weights['accuracy'] * accuracy_cost
        )
        if baseline_total > 0:
            reward = (baseline_total - total) / baseline_total
        else:
            reward = 0.0
        reward = max(0.0, min(1.0, reward))

        self._cost_history.append(total)
        self._last_total_cost = total

        state = self._build_optimization_state(node_desc, workload, expert_profile)

        # Update the appropriate model
        if self.moe_gating:
            # If MoE used, we need the selected expert from _get_weights
            expert_name = self._last_selected_expert
            await self.moe_gating.add_training_sample(state, expert_name, reward)
        else:
            # Update distillation
            next_state = state
            asyncio.create_task(self.policy_optimizer.update(
                state.to_feature_vector(),
                self._last_action_idx,
                reward,
                next_state.to_feature_vector(),
                self._last_teacher_probs
            ))

        # --- RLHF preference recording (occasional) ---
        if self.rlhf_trainer and random.random() < 0.05:
            chosen_strategy = self._last_strategy
            rejected_strategy = random.choice([s for s in DistillationCostOptimizer.ACTION_SPACE if s != chosen_strategy])
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Which weight strategy is better for current conditions?",
                chosen=chosen_strategy,
                rejected=rejected_strategy,
                reward_diff=reward,
                metadata={'node_id': node_desc.id, 'workload_tokens': workload.tokens}
            )

        # --- Update LIMIT Graph with latest weights ---
        if self.limit_graph_manager:
            for comp, w in weights.items():
                self.limit_graph_manager.add_node(
                    "cost_components", f"node_{comp}", comp,
                    {"weight": w, "timestamp": datetime.now().isoformat()}
                )

        logger.debug(
            "Cost components (normalized): energy=%.4f, carbon=%.4f, helium=%.4f, material=%.4f, latency=%.4f, accuracy=%.4f, total=%.4f",
            energy_cost, carbon_cost, helium_cost, material_cost, latency_cost, accuracy_cost, total
        )
        return total

    # ---------- Weight selection ----------
    async def _get_weights(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        anomaly_detected: bool = False,
    ) -> Dict[str, float]:
        state = self._build_optimization_state(node_desc, workload, None)

        # If MoE gating is available, use it as primary selector
        if self.moe_gating:
            expert_name, _ = await self.moe_gating.select_expert(state)
            self._last_selected_expert = expert_name
            # Map expert to strategy
            strategy_map = {
                'carbon_focus': 'carbon_focus',
                'energy_focus': 'energy_focus',
                'helium_focus': 'helium_focus',
                'adaptive': 'adaptive',
            }
            strategy = strategy_map.get(expert_name, 'adaptive')
            self._last_strategy = strategy
            action_idx = DistillationCostOptimizer.ACTION_SPACE.index(strategy)
            self._last_action_idx = action_idx
            self._last_teacher_probs = np.ones(5) / 5
            # Apply weight adjustments based on strategy
            weights = self._base_weights.copy()
            if strategy == 'carbon_focus':
                weights['carbon'] *= 1.2
            elif strategy == 'energy_focus':
                weights['energy'] *= 1.2
            elif strategy == 'helium_focus':
                weights['helium'] *= 1.2
            elif strategy == 'adaptive':
                if self._get_config('use_adaptive_weights', False) and self.adaptive_cost and ADAPTIVE_AVAILABLE:
                    try:
                        if hasattr(self.adaptive_cost, 'get_weights'):
                            adaptive_weights = await self.adaptive_cost.get_weights()
                        else:
                            adaptive_weights = self.adaptive_cost.weights
                        mapping = {
                            'alpha': 'energy',
                            'beta': 'carbon',
                            'gamma': 'helium',
                            'delta': 'material',
                            'epsilon': 'latency',
                            'zeta': 'accuracy',
                        }
                        for ad_key, comp in mapping.items():
                            if ad_key in adaptive_weights:
                                weights[comp] = adaptive_weights[ad_key]
                    except Exception as e:
                        logger.warning(f"Adaptive weight update failed: {e}")
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
            self._current_weights = weights
            return weights

        # Fallback to distillation optimizer
        strategy, action_idx, state_vec, teacher_probs = await self.policy_optimizer.select_strategy(state, exploration=True)
        self._last_strategy = strategy
        self._last_action_idx = action_idx
        self._last_teacher_probs = teacher_probs
        weights = self._base_weights.copy()
        if strategy == 'standard':
            pass
        elif strategy == 'carbon_focus':
            weights['carbon'] *= 1.2
        elif strategy == 'energy_focus':
            weights['energy'] *= 1.2
        elif strategy == 'helium_focus':
            weights['helium'] *= 1.2
        elif strategy == 'adaptive':
            if self._get_config('use_adaptive_weights', False) and self.adaptive_cost and ADAPTIVE_AVAILABLE:
                try:
                    if hasattr(self.adaptive_cost, 'get_weights'):
                        adaptive_weights = await self.adaptive_cost.get_weights()
                    else:
                        adaptive_weights = self.adaptive_cost.weights
                    mapping = {
                        'alpha': 'energy',
                        'beta': 'carbon',
                        'gamma': 'helium',
                        'delta': 'material',
                        'epsilon': 'latency',
                        'zeta': 'accuracy',
                    }
                    for ad_key, comp in mapping.items():
                        if ad_key in adaptive_weights:
                            weights[comp] = adaptive_weights[ad_key]
                except Exception as e:
                    logger.warning(f"Adaptive weight update failed: {e}")
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        self._current_weights = weights
        return weights

    def _build_optimization_state(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> CostOptimizationState:
        """Build state for the distillation agent."""
        carbon_intensity = self._get_carbon_intensity_sync(node_desc.region)
        node_health = 1.0
        if self._get_config('integrate_predictive_maintenance', False) and self.predictive_maintenance:
            try:
                node_health = 0.8  # placeholder
            except Exception:
                pass
        tokens = workload.tokens
        latency = workload.latency_target
        anomaly_severity = 0.0
        if self._get_config('integrate_anomaly_detection', False) and self.anomaly_detector:
            try:
                if hasattr(self.anomaly_detector, 'get_latest_severity'):
                    anomaly_severity = await self.anomaly_detector.get_latest_severity()
            except Exception:
                pass
        if len(self._cost_history) >= 5:
            recent = list(self._cost_history)[-5:]
            trend = (recent[-1] - recent[0]) / (len(recent) - 1) if len(recent) > 1 else 0.0
            avg_cost_trend = trend
            cost_variance = np.var(recent)
        else:
            avg_cost_trend = 0.0
            cost_variance = 0.0
        weight_carbon = self._base_weights['carbon']
        weight_energy = self._base_weights['energy']
        weight_helium = self._base_weights['helium']
        hour = datetime.now().hour
        return CostOptimizationState(
            carbon_intensity=carbon_intensity,
            node_health=node_health,
            workload_tokens=tokens,
            latency_target=latency,
            anomaly_severity=anomaly_severity,
            avg_cost_trend=avg_cost_trend,
            cost_variance=cost_variance,
            weight_carbon=weight_carbon,
            weight_energy=weight_energy,
            weight_helium=weight_helium,
            hour_of_day=hour,
        )

    def _get_carbon_intensity_sync(self, region: str) -> float:
        now = datetime.now()
        if region in self._carbon_cache:
            value, timestamp = self._carbon_cache[region]
            if (now - timestamp).total_seconds() < self._carbon_cache_ttl:
                return value
        return self._get_config('carbon_intensity_baseline_kg_per_kwh', 0.4)

    # ---------- Helper methods ----------
    async def _get_carbon_intensity(self, region: str) -> float:
        now = datetime.now()
        if region in self._carbon_cache:
            value, timestamp = self._carbon_cache[region]
            if (now - timestamp).total_seconds() < self._carbon_cache_ttl:
                self._carbon_cache.move_to_end(region)
                return value
            else:
                del self._carbon_cache[region]
        try:
            intensity = await self.carbon.get_intensity(region)
        except Exception as e:
            logger.error(f"Carbon intensity fetch failed: {e}")
            intensity = self._get_config('carbon_intensity_baseline_kg_per_kwh', 0.4)
        self._carbon_cache[region] = (intensity, now)
        if len(self._carbon_cache) > self._carbon_cache_max_size:
            self._carbon_cache.popitem(last=False)
        return intensity

    async def _get_helium_scarcity(self, node_desc: NodeDescriptor) -> float:
        return 1.0 - node_desc.helium_connectivity_score

    async def _get_material_composite(self, node_desc: NodeDescriptor) -> float:
        if not node_desc.material_footprint_id:
            return 0.0
        fp = self.material.get_footprint(node_desc.material_footprint_id)
        if not fp:
            return 0.0
        embodied = fp.get('embodied_carbon_kg', 0)
        rare_earth = fp.get('rare_earth_kg', 0)
        embodied_norm = self._get_config('material_embodied_norm', 200.0)
        rare_earth_norm = self._get_config('material_rare_earth_norm', 0.01)
        normalized_embodied = embodied / embodied_norm if embodied_norm > 0 else 0.0
        normalized_rare = rare_earth / rare_earth_norm if rare_earth_norm > 0 else 0.0
        composite = (normalized_embodied * 0.7 + normalized_rare * 0.3)
        max_composite = self._get_config('material_max_composite', 1.0)
        return min(max_composite, composite)

    # ---------- Integration callbacks ----------
    async def on_anomaly_detected(self, anomaly_severity: float):
        if not self._get_config('integrate_anomaly_detection', False):
            return
        self._last_anomaly_time = datetime.now()
        logger.info(f"Anomaly detected with severity {anomaly_severity}, will be reflected in next strategy selection.")

    async def update_from_predictive_maintenance(self, node_id: str, efficiency_factor: float):
        if not self._get_config('integrate_predictive_maintenance', False):
            return
        logger.debug(f"Predictive maintenance update for node {node_id}: factor={efficiency_factor}")

    # ---------- Utility methods ----------
    async def get_weights(self) -> Dict[str, float]:
        return self._current_weights.copy()

    async def set_weights(self, new_weights: Dict[str, float]) -> None:
        total = sum(new_weights.values())
        if total == 0:
            raise ValueError("Weights sum cannot be zero")
        self._base_weights = {k: v / total for k, v in new_weights.items()}
        logger.info(f"Base weights set manually: {self._base_weights}")

    async def reset_weights(self) -> None:
        self._base_weights = self._get_initial_weights()
        logger.info("Weights reset to initial configuration")

    async def reset_carbon_cache(self) -> None:
        self._carbon_cache.clear()
        logger.info("Carbon cache cleared")

    async def get_cost_breakdown(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> Dict[str, Any]:
        energy_used = node_desc.energy_per_token * workload.tokens
        energy_cost = self._normalize_energy(energy_used)
        carbon_intensity = await self._get_carbon_intensity(node_desc.region)
        carbon_kg = energy_used * carbon_intensity
        carbon_cost = self._normalize_carbon(carbon_kg)
        helium_scarcity = await self._get_helium_scarcity(node_desc)
        helium_base = (1 - node_desc.helium_connectivity_score) * 0.5
        if helium_scarcity > self._get_config('helium_scarcity_threshold', 0.7):
            helium_base *= (1 + helium_scarcity)
        helium_cost = self._normalize_helium(helium_base)
        material_composite = await self._get_material_composite(node_desc)
        material_cost = self._normalize_material(material_composite)
        latency_cost = self._normalize_latency(workload.latency_target)
        if expert_profile:
            acc = expert_profile.accuracy_score
        else:
            acc = self._get_config('accuracy_baseline', 0.9)
        accuracy_cost = self._normalize_accuracy(acc)
        weights = await self.get_weights()
        total = (
            weights['energy'] * energy_cost +
            weights['carbon'] * carbon_cost +
            weights['helium'] * helium_cost +
            weights['material'] * material_cost +
            weights['latency'] * latency_cost +
            weights['accuracy'] * accuracy_cost
        )
        return {
            'energy': {'raw': energy_used, 'normalized': energy_cost},
            'carbon': {'raw': carbon_kg, 'normalized': carbon_cost},
            'helium': {'raw': helium_base, 'normalized': helium_cost},
            'material': {'raw': material_composite, 'normalized': material_cost},
            'latency': {'raw': workload.latency_target, 'normalized': latency_cost},
            'accuracy': {'raw': acc, 'normalized': accuracy_cost},
            'total': total,
            'weights': weights,
        }

    async def close(self):
        if self._moea_task:
            self._moea_task.cancel()
            await asyncio.gather(self._moea_task, return_exceptions=True)

    async def get_distillation_stats(self) -> Dict:
        return self.policy_optimizer.get_stats()

    # ============================================================================
    # MOEA Integration Methods
    # ============================================================================
    async def _moea_loop(self):
        """Periodically run MOEA to refine weights."""
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_moea_optimization()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop failed: {e}")
                await asyncio.sleep(60)

    async def run_moea_optimization(self):
        """Run NSGA-II to evolve weight vectors based on the latest scenario."""
        if not self.moea_enabled:
            logger.info("MOEA is disabled.")
            return

        if not self._last_node_desc or not self._last_workload:
            logger.warning("No scenario available for MOEA; skipping.")
            return

        # Define parameter bounds: each weight in [0.01, 0.99]
        param_bounds = {
            'energy': (0.01, 0.99),
            'carbon': (0.01, 0.99),
            'helium': (0.01, 0.99),
            'material': (0.01, 0.99),
            'latency': (0.01, 0.99),
            'accuracy': (0.01, 0.99),
        }

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            """Compute objectives (benefits) for a weight vector using the last scenario."""
            old_weights = self._current_weights.copy()
            self._current_weights = weights

            # Compute cost components using the last scenario
            node_desc = self._last_node_desc
            workload = self._last_workload
            expert_profile = self._last_expert_profile

            energy_used = node_desc.energy_per_token * workload.tokens
            energy_cost = self._normalize_energy(energy_used)
            carbon_intensity = await self._get_carbon_intensity(node_desc.region)
            carbon_kg = energy_used * carbon_intensity
            carbon_cost = self._normalize_carbon(carbon_kg)
            helium_scarcity = await self._get_helium_scarcity(node_desc)
            helium_base = (1 - node_desc.helium_connectivity_score) * 0.5
            if helium_scarcity > self._get_config('helium_scarcity_threshold', 0.7):
                helium_base *= (1 + helium_scarcity)
            helium_cost = self._normalize_helium(helium_base)
            material_composite = await self._get_material_composite(node_desc)
            material_cost = self._normalize_material(material_composite)
            latency_cost = self._normalize_latency(workload.latency_target)
            if expert_profile:
                acc = expert_profile.accuracy_score
            else:
                acc = self._get_config('accuracy_baseline', 0.9)
            accuracy_cost = self._normalize_accuracy(acc)

            # Restore weights
            self._current_weights = old_weights

            # Convert costs to benefits (higher is better)
            return {
                'energy': 1.0 - energy_cost,
                'carbon': 1.0 - carbon_cost,
                'helium': 1.0 - helium_cost,
                'material': 1.0 - material_cost,
                'latency': 1.0 - latency_cost,
                'accuracy': 1.0 - accuracy_cost,
            }

        # Create NSGA-II optimizer
        self.moea_optimizer = NSGAIIOptimizer(
            evaluate_func=evaluate,
            parameter_bounds=param_bounds,
            population_size=self._get_config('moea_population_size', 30),
            generations=self._get_config('moea_generations', 10),
            mutation_rate=self._get_config('moea_mutation_rate', 0.2),
            crossover_rate=self._get_config('moea_crossover_rate', 0.8),
            tournament_size=self._get_config('moea_tournament_size', 3),
            objective_weights=self._get_dynamic_moea_weights(),
            dynamic_weights=self._get_config('moea_dynamic_weights', True),
        )

        pareto = await self.moea_optimizer.evolve()
        self.moea_pareto_front = pareto

        # Select best using MODP (scalarization with current dynamic weights)
        if pareto:
            weights = self._get_dynamic_moea_weights()
            best_point = self.moea_optimizer._select_best_from_pareto(pareto, weights)
            if best_point:
                self.moea_best_weights = best_point.parameters
                # Update base weights (or current weights) with the best found
                self._base_weights = best_point.parameters
                logger.info(f"MOEA selected best weights: {self._base_weights}")
                if self.metrics:
                    self.metrics['moea_pareto_front'].set(len(pareto))
                # Also store in MODP if enabled
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{time.time()}",
                        problem_id="cost_weight_optimization",
                        state_attributes={'weights': self.moea_best_weights},
                        objective_values={k: 1.0 - v for k, v in best_point.objectives.items()},
                        stage=0
                    )

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        """Compute dynamic objective weights for MODP selection."""
        weights = self._get_config('moea_objective_weights', {
            'energy': 0.2,
            'carbon': 0.3,
            'helium': 0.15,
            'material': 0.15,
            'latency': 0.1,
            'accuracy': 0.1,
        }).copy()

        # Adjust based on current system state
        if self._carbon_cache:
            try:
                latest_carbon = max(
                    value for value, ts in self._carbon_cache.values()
                    if (datetime.now() - ts).total_seconds() < self._carbon_cache_ttl
                )
                if latest_carbon > 0.8:
                    weights['carbon'] = min(0.6, weights.get('carbon', 0.3) * 1.5)
            except ValueError:
                pass

        # Normalize
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    async def get_pareto_front(self) -> List[Dict]:
        """Return the current Pareto front as a list of dictionaries."""
        return [p.to_dict() for p in self.moea_pareto_front]

    async def apply_moea_weights(self, weights: Dict[str, float]):
        """Manually apply a weight vector (e.g., from Pareto front selection)."""
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        self._base_weights = weights
        self._current_weights = weights
        logger.info(f"Applied MOEA weights: {weights}")

    # ---------- New public methods for enhancements ----------
    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def record_rlhf_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)

    async def get_limit_graph(self, graph_id: str = "cost_components") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return self.moe_gating.expert_names
        return []


# ============================================================================
# Convenience factory
# ============================================================================
def create_cost_function(
    carbon_fetcher: CarbonIntensityFetcher,
    material_updater: MaterialFootprintUpdater,
    helium_collector: HeliumCollector,
    config: Optional[Dict[str, Any]] = None,
    adaptive_cost_function: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
    predictive_maintenance: Optional[Any] = None,
    storage: Optional[Storage] = None,
) -> SustainabilityCostFunction:
    return SustainabilityCostFunction(
        carbon_fetcher=carbon_fetcher,
        material_updater=material_updater,
        helium_collector=helium_collector,
        config=config,
        adaptive_cost_function=adaptive_cost_function,
        anomaly_detector=anomaly_detector,
        predictive_maintenance=predictive_maintenance,
        storage=storage,
    )


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')

    # Mock dependencies for testing
    class MockCarbonFetcher:
        async def get_intensity(self, region: str) -> float:
            return 0.42

    class MockMaterialUpdater:
        def get_footprint(self, product_id: str) -> Dict:
            return {'embodied_carbon_kg': 200, 'rare_earth_kg': 0.01}

    class MockHeliumCollector:
        async def get_connectivity_score(self, hotspot_id: str) -> float:
            return 0.8

    class MockExpertProfile:
        def __init__(self):
            self.accuracy_score = 0.95

    async def main():
        carbon = MockCarbonFetcher()
        material = MockMaterialUpdater()
        helium = MockHeliumCollector()
        cost_func = create_cost_function(carbon, material, helium)

        node_desc = NodeDescriptor(
            id="test",
            type="edge",
            region="us-east",
            region_carbon_intensity=0.42,
            energy_per_token=0.00005,
            helium_connectivity_score=0.9,
            material_footprint_id="gpu-a100"
        )
        workload = WorkloadDescriptor(
            task_type="inference",
            tokens=512,
            latency_target=200.0,
            sector_emission_factor=0.03,
            bio_mode="none",
            priority="balanced"
        )
        expert = MockExpertProfile()
        cost = await cost_func.compute(node_desc, workload, expert)
        print(f"Total cost: {cost}")

        # Trigger MOEA optimization manually
        await cost_func.run_moea_optimization()
        pareto = await cost_func.get_pareto_front()
        print(f"Pareto front size: {len(pareto)}")
        if pareto:
            print("Best weights:", cost_func.moea_best_weights)

        # Get LIMIT Graph info
        graph_info = await cost_func.get_limit_graph()
        print(f"LIMIT Graph nodes: {len(graph_info.get('nodes', []))}")

        # Get MoE experts
        experts = await cost_func.get_moe_experts()
        print(f"MoE experts: {experts}")

        breakdown = await cost_func.get_cost_breakdown(node_desc, workload, expert)
        print("Cost breakdown:", breakdown)

        await cost_func.close()

    asyncio.run(main())
