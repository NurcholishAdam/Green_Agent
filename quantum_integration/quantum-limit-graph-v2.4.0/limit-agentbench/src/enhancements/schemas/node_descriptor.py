"""
Enhanced Node Descriptor v2.2.0
================================
Defines the structure of a compute node with adaptive routing strategy selection
via Multi‑Teacher On‑Policy Distillation.

Features (including v2.2.0 enhancements):
- Expanded node types as Enum.
- Fields for performance, cooling, location, cost, health.
- Sustainability metrics: carbon, helium, renewable, material.
- Helper methods for cost estimation and health evaluation.
- Pydantic validation with custom validators.
- Adaptive routing strategy selection (carbon_first, latency_first, cost_first, balanced, adaptive).
- Online learning from routing outcomes.
- Teachers: rule‑based, historical ML, stateful Q, RLHF, and optional Quantum teacher.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights and interaction logs (CSV + JSON).
- Offline training for historical ML teacher from logs with state vectors.
- Unit tests for distillation components.
- Integration with FeedbackEvent schema for audit trails.
- Asynchronous persistence and improved batch updates.
- Optional causal reward shaping using CausalGraph.
"""

from enum import Enum
from typing import Optional, Dict, Any, List, Tuple, Union
from datetime import datetime
from collections import deque
from pathlib import Path
import json
import random
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd
from dataclasses import dataclass, asdict

from pydantic import BaseModel, Field, field_validator, ConfigDict, PrivateAttr

# Import logger
try:
    from ..logger import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# Optional import of FeedbackEvent for integration
try:
    from .feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

# Import new enhancements
try:
    from ..quantum_teacher import QuantumTeacher
except ImportError:
    QuantumTeacher = None

try:
    from ..causal_reward_shaper import CausalRewardShaper
except ImportError:
    CausalRewardShaper = None

# ============================================================================
# Enums
# ============================================================================

class NodeType(str, Enum):
    EDGE = "edge"
    HOTSPOT = "hotspot"
    CLOUD = "cloud"
    LAB = "lab"
    ON_PREM = "on_prem"
    VM = "vm"
    CONTAINER = "container"

class CoolingType(str, Enum):
    AIR = "air"
    LIQUID = "liquid"
    CRYOGENIC = "cryogenic"
    NONE = "none"

class MaintenanceStatus(str, Enum):
    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    OFFLINE = "offline"

class RoutingStrategy(str, Enum):
    CARBON_FIRST = "carbon_first"
    LATENCY_FIRST = "latency_first"
    COST_FIRST = "cost_first"
    BALANCED = "balanced"
    ADAPTIVE = "adaptive"

# ============================================================================
# DISTILLATION COMPONENTS
# ============================================================================

@dataclass
class NodeState:
    """State for the distillation agent."""
    carbon_intensity: float
    renewable_fraction: float
    helium_connectivity: float
    uptime: float
    efficiency_score: float
    health_score: float
    cost_per_hour: float
    energy_per_token: float
    recent_success_rate: float
    avg_reward: float
    human_feedback_score: float = 0.5

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 11‑dim numeric feature vector."""
        features = [
            min(self.carbon_intensity / 1.0, 1.0),
            self.renewable_fraction,
            self.helium_connectivity,
            self.uptime,
            self.efficiency_score,
            self.health_score,
            min(self.cost_per_hour / 10.0, 1.0),
            min(self.energy_per_token / 0.0001, 1.0),
            self.recent_success_rate,
            self.avg_reward,
            self.human_feedback_score,
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: NodeState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: NodeState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class RoutingRuleBasedTeacher(Teacher):
    """Rule‑based expert using original heuristics."""
    STRATEGIES = ['carbon_first', 'latency_first', 'cost_first', 'balanced', 'adaptive']

    def predict(self, state: NodeState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.carbon_intensity > 0.5:
            probs[0] = 0.8
        elif state.health_score < 0.6:
            probs[1] = 0.7
        elif state.cost_per_hour > 5.0:
            probs[2] = 0.6
        elif state.renewable_fraction > 0.7:
            probs[3] = 0.6
        else:
            probs[4] = 0.6
        return probs / probs.sum()

    def confidence(self, state: NodeState) -> float:
        return 0.6 if state.carbon_intensity > 0.5 else 0.4


class RoutingHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path("./routing_historical_model.pkl")
        self._load_model()

    def _load_model(self):
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: NodeState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        if hasattr(self.model, 'predict_proba'):
            probs = self.model.predict_proba(x)[0]
            classes = self.label_encoder.classes_
            target_order = RoutingStrategy._member_names_
            idx_map = {cls: i for i, cls in enumerate(classes)}
            new_probs = np.zeros(5)
            for i, strat in enumerate(target_order):
                if strat in idx_map:
                    new_probs[i] = probs[idx_map[strat]]
            probs = new_probs / new_probs.sum() if new_probs.sum() > 0 else np.ones(5)/5
            return probs
        else:
            return np.ones(5) / 5

    def confidence(self, state: NodeState) -> float:
        return 0.7 if self.model is not None else 0.0

    @classmethod
    def train_from_logs(cls, log_paths: List[Path], model_path: Path,
                        state_col: str = 'state_vec', label_col: str = 'strategy'):
        try:
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.preprocessing import LabelEncoder
        except ImportError:
            logger.error("scikit-learn is required for historical model training.")
            return None

        all_dfs = []
        for path in log_paths:
            if path.exists():
                df = pd.read_csv(path)
                all_dfs.append(df)
        if not all_dfs:
            logger.warning("No logs found for training.")
            return None

        df = pd.concat(all_dfs, ignore_index=True)
        if len(df) < 10:
            logger.warning("Not enough logs to train historical model.")
            return None

        def parse_state(s):
            try:
                return np.fromstring(s, sep=',')
            except:
                return None

        valid_indices = [i for i, s in enumerate(df[state_col]) if parse_state(s) is not None]
        X = np.array([parse_state(df[state_col].iloc[i]) for i in valid_indices])
        y = df[label_col].iloc[valid_indices].values

        if len(X) < 5:
            logger.warning("Too few valid samples after parsing.")
            return None

        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        with open(model_path, 'wb') as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained historical model and saved to {model_path}")
        return model_path


class RoutingStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None, feature_dim: int = 11):
        self.lr = lr
        self.weights_path = weights_path or Path("./routing_q_weights.json")
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, 5))
        self._load_state()

    def _load_state(self):
        if self.weights_path.exists():
            try:
                with open(self.weights_path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                if self.weights.shape[0] != self.feature_dim:
                    logger.warning(f"Loaded Q‑weights shape {self.weights.shape}, expected ({self.feature_dim},5). Reinitializing.")
                    self.weights = np.zeros((self.feature_dim, 5))
                logger.info(f"Loaded Q‑teacher weights from {self.weights_path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        with open(self.weights_path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: NodeState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: NodeState) -> float:
        return 0.5

    def update(self, state: NodeState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class RLHFTeacher(Teacher):
    """Teacher influenced by human feedback."""
    def predict(self, state: NodeState) -> np.ndarray:
        probs = np.ones(5) / 5
        if state.human_feedback_score > 0.7:
            probs[0] += 0.1  # carbon first (sustainability)
            probs[1] += 0.1  # latency first (responsive)
        elif state.human_feedback_score < 0.3:
            probs[2] += 0.1  # cost first
        return probs / probs.sum()

    def confidence(self, state: NodeState) -> float:
        return 0.7 if abs(state.human_feedback_score - 0.5) > 0.3 else 0.4


class DistillationStudent:
    def __init__(self, feature_dim: int = 11, n_classes: int = 5, lr: float = 0.01):
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int = None) -> np.ndarray:
        if num_classes is None:
            num_classes = self.n_classes
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.feature_dim, num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
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

    def push(self, state_vec, action, reward, next_state_vec, teacher_probs):
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


class MoEGatingNetwork:
    """Learnable gating network for combining teacher outputs."""
    def __init__(self, feature_dim: int = 11, n_experts: int = 5, lr: float = 0.005):
        self.feature_dim = feature_dim
        self.n_experts = n_experts
        self.lr = lr
        self.weights = np.random.randn(feature_dim, n_experts) * 0.01
        self.bias = np.zeros(n_experts)

    def forward(self, state_vec) -> np.ndarray:
        logits = state_vec @ self.weights + self.bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vec, teacher_outputs, student_probs):
        gate_weights = self.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        error = combined - student_probs
        grad_gate = np.dot(teacher_outputs, error)
        self.weights -= self.lr * np.outer(state_vec, grad_gate)
        self.bias -= self.lr * grad_gate


class DistillationRoutingOptimizer:
    """Multi‑teacher on‑policy distillation agent for routing strategy selection."""
    STRATEGIES = ['carbon_first', 'latency_first', 'cost_first', 'balanced', 'adaptive']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.n_actions = 5
        self.feature_dim = 11
        self.student = DistillationStudent(feature_dim=self.feature_dim,
                                           lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            RoutingRuleBasedTeacher(),
            RoutingHistoricalMLTeacher(model_path=config.get('historical_model_path')),
            RoutingStatefulQTeacher(lr=config.get('q_learning_rate', 0.1),
                                    weights_path=config.get('q_weights_path'),
                                    feature_dim=self.feature_dim),
            RLHFTeacher()
        ]
        # Optionally add quantum teacher
        if config.get('use_quantum_teacher', False) and QuantumTeacher is not None:
            self.teachers.append(QuantumTeacher(
                n_actions=self.n_actions,
                n_qubits=config.get('quantum_teacher_qubits', 4),
                n_layers=config.get('quantum_teacher_layers', 2)
            ))

        self.n_teachers = len(self.teachers)
        self.gating = MoEGatingNetwork(feature_dim=self.feature_dim,
                                       n_experts=self.n_teachers,
                                       lr=config.get('gating_learning_rate', 0.005))
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
        self.distill_weight = config.get('distill_weight', 0.7)
        self.rl_weight = config.get('rl_weight', 0.3)
        self.batch_update_size = config.get('batch_update_size', 8)

    def _compute_teacher_probs(self, state: NodeState) -> Tuple[np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher.predict(state)
            if len(prob) != self.n_actions:
                if len(prob) < self.n_actions:
                    prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')
                else:
                    prob = prob[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate_weights = self.gating.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        combined = combined / combined.sum()
        return combined, gate_weights

    async def select_strategy(self, state: NodeState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs, gate_weights = self._compute_teacher_probs(state)

        student_probs = self.student.predict_proba(state_vec, self.n_actions)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= self.batch_update_size:
            batch = self.replay_buffer.sample(self.batch_update_size)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i],
                                    distill_weight=self.distill_weight, rl_weight=self.rl_weight)
                # Update gating
                student_out = self.student.predict_proba(states[i])
                teacher_outputs = np.tile(teacher_probs_batch[i], (self.n_teachers, 1))
                self.gating.update(states[i], teacher_outputs, student_out)

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'n_teachers': self.n_teachers
        }


# ============================================================================
# Enhanced NodeDescriptor
# ============================================================================

class NodeDescriptor(BaseModel):
    """Descriptor for a compute node with adaptive routing strategy selection."""
    model_config = ConfigDict(extra='allow', arbitrary_types_allowed=True)

    id: str = Field(..., description="Unique node identifier")
    type: NodeType = Field(..., description="Type of the node")
    region: str = Field(..., description="Geographic region")
    location_lat: Optional[float] = Field(None, description="Latitude of the node")
    location_lon: Optional[float] = Field(None, description="Longitude of the node")
    availability_zone: Optional[str] = Field(None, description="Cloud availability zone")

    region_carbon_intensity: float = Field(..., ge=0, description="kg CO₂/kWh")
    carbon_intensity_source: Optional[str] = Field(None, description="Source of carbon intensity data")
    energy_per_token: float = Field(..., gt=0, description="Joules per token")
    helium_connectivity_score: float = Field(0.5, ge=0, le=1, description="Connectivity score for helium-based nodes")
    material_footprint_id: Optional[str] = Field(None, description="Reference to material footprint catalog")
    renewable_fraction: float = Field(0.0, ge=0, le=1, description="Fraction of energy from renewables")
    efficiency_score: Optional[float] = Field(None, ge=0, le=1, description="Overall sustainability efficiency score")

    flops: Optional[float] = Field(None, gt=0, description="Estimated FLOPs per second")
    memory_gb: Optional[float] = Field(None, gt=0, description="Available memory in GB")
    storage_gb: Optional[float] = Field(None, gt=0, description="Available storage in GB")

    cooling_type: CoolingType = Field(CoolingType.AIR, description="Cooling system type")
    network_latency_ms: Optional[float] = Field(None, gt=0, description="Average network latency in milliseconds")

    uptime: float = Field(1.0, ge=0, le=1, description="Fraction of time the node is available")
    cost_per_hour_usd: Optional[float] = Field(None, ge=0, description="Cost per hour in USD")
    maintenance_status: MaintenanceStatus = Field(MaintenanceStatus.OPERATIONAL, description="Maintenance status")
    last_updated: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of last update")

    hardware_model: Optional[str] = Field(None, description="Hardware model identifier")
    manufacturer: Optional[str] = Field(None, description="Hardware manufacturer")

    routing_strategy: RoutingStrategy = Field(RoutingStrategy.BALANCED, description="Current routing strategy")
    performance_history: List[Dict[str, Any]] = Field(default_factory=list, description="Recent routing outcomes (max 100 entries)")
    max_history_length: int = Field(100, ge=1, description="Maximum number of history entries to keep")

    # Additional fields for enhancements
    graph_metrics: Optional[Dict[str, float]] = Field(None, description="LIMIT Graph metrics")
    human_feedback_score: float = Field(0.5, ge=0, le=1, description="RLHF human feedback score")
    use_evolutionary: bool = Field(False, description="Enable evolutionary optimisation")
    evolutionary_population_size: int = Field(20, ge=2)
    evolutionary_mutation_rate: float = Field(0.1, ge=0, le=1)
    evolutionary_crossover_rate: float = Field(0.7, ge=0, le=1)
    evolutionary_elitism: int = Field(2, ge=1)
    use_quantum_teacher: bool = Field(False, description="Enable quantum teacher")

    version: str = Field("2.2.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    _routing_optimizer: Optional[DistillationRoutingOptimizer] = PrivateAttr(default=None)
    _last_decision: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _causal_shaper: Optional[CausalRewardShaper] = PrivateAttr(default=None)
    _evolutionary_optimizer: Optional[Any] = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator('region_carbon_intensity')
    @classmethod
    def validate_carbon_intensity(cls, v):
        if v < 0:
            raise ValueError("region_carbon_intensity must be non‑negative")
        return v

    @field_validator('energy_per_token')
    @classmethod
    def validate_energy_per_token(cls, v):
        if v <= 0:
            raise ValueError("energy_per_token must be positive")
        return v

    @field_validator('location_lat')
    @classmethod
    def validate_latitude(cls, v):
        if v is not None and not (-90 <= v <= 90):
            raise ValueError("latitude must be between -90 and 90")
        return v

    @field_validator('location_lon')
    @classmethod
    def validate_longitude(cls, v):
        if v is not None and not (-180 <= v <= 180):
            raise ValueError("longitude must be between -180 and 180")
        return v

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def compute_energy_cost(self, tokens: int) -> float:
        return self.energy_per_token * tokens

    def compute_carbon_cost(self, energy_joules: float) -> float:
        energy_kwh = energy_joules * 2.7778e-7
        return energy_kwh * self.region_carbon_intensity

    def get_health_score(self) -> float:
        base = self.uptime
        if self.maintenance_status == MaintenanceStatus.OPERATIONAL:
            base *= 1.0
        elif self.maintenance_status == MaintenanceStatus.DEGRADED:
            base *= 0.7
        elif self.maintenance_status == MaintenanceStatus.MAINTENANCE:
            base *= 0.3
        else:
            base *= 0.0
        if self.efficiency_score is not None:
            base *= self.efficiency_score
        return max(0.0, min(1.0, base))

    def is_available(self) -> bool:
        return self.maintenance_status in (MaintenanceStatus.OPERATIONAL, MaintenanceStatus.DEGRADED)

    def to_dict(self, exclude_none: bool = False) -> Dict[str, Any]:
        data = self.model_dump()
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeDescriptor":
        return cls(**data)

    def _ensure_optimizer(self):
        if self._routing_optimizer is None:
            self._routing_optimizer = DistillationRoutingOptimizer({
                'distillation_epsilon': self.metadata.get('distillation_epsilon', 0.1),
                'distillation_train_every': self.metadata.get('distillation_train_every', 10),
                'distillation_replay_size': self.metadata.get('distillation_replay_size', 2000),
                'distillation_learning_rate': self.metadata.get('distillation_learning_rate', 0.01),
                'historical_model_path': self.metadata.get('historical_model_path'),
                'q_learning_rate': self.metadata.get('q_learning_rate', 0.1),
                'q_weights_path': self.metadata.get('q_weights_path', Path(f"./routing_q_weights_{self.id}.json")),
                'distill_weight': self.metadata.get('distill_weight', 0.7),
                'rl_weight': self.metadata.get('rl_weight', 0.3),
                'batch_update_size': self.metadata.get('batch_update_size', 8),
                'gating_learning_rate': self.metadata.get('gating_learning_rate', 0.005),
                'use_quantum_teacher': self.use_quantum_teacher,
                'quantum_teacher_qubits': self.metadata.get('quantum_teacher_qubits', 4),
                'quantum_teacher_layers': self.metadata.get('quantum_teacher_layers', 2),
            })
        if self.use_evolutionary and self._evolutionary_optimizer is None:
            # Simple evolutionary optimizer placeholder
            from ..evolutionary_optimizer import EvolutionaryOptimizer
            self._evolutionary_optimizer = EvolutionaryOptimizer(
                n_actions=5,
                population_size=self.evolutionary_population_size,
                mutation_rate=self.evolutionary_mutation_rate,
                crossover_rate=self.evolutionary_crossover_rate,
                elitism=self.evolutionary_elitism
            )

    # ------------------------------------------------------------------
    # Distillation methods
    # ------------------------------------------------------------------
    async def select_routing_strategy(
        self,
        exploration: bool = True,
        carbon_saved: Optional[float] = None,
        latency_ms: Optional[float] = None,
        cost_usd: Optional[float] = None,
    ) -> RoutingStrategy:
        self._ensure_optimizer()

        state = self._build_state()
        strategy, action_idx, state_vec, teacher_probs = await self._routing_optimizer.select_strategy(
            state, exploration=exploration
        )

        # Evolutionary blending (optional)
        if self.use_evolutionary and self._evolutionary_optimizer is not None:
            evo_probs = self._evolutionary_optimizer.get_probs()
            combined = 0.7 * teacher_probs + 0.3 * evo_probs
            action_idx = int(np.argmax(combined))
            strategy = self._routing_optimizer.STRATEGIES[action_idx]

        self._last_decision = {
            'state_vec': state_vec,
            'action_idx': action_idx,
            'teacher_probs': teacher_probs,
        }
        self.routing_strategy = RoutingStrategy(strategy)

        if carbon_saved is not None and latency_ms is not None and cost_usd is not None:
            await self.record_outcome(carbon_saved, latency_ms, cost_usd)

        return self.routing_strategy

    async def record_outcome(self, carbon_saved_kg: float, latency_ms: float, cost_usd: float):
        """Record the outcome of a routing decision and update the distillation agent."""
        # Compute reward
        carbon_norm = min(1.0, carbon_saved_kg / 0.1)
        latency_norm = 1.0 - min(1.0, latency_ms / 500.0)
        cost_norm = 1.0 - min(1.0, cost_usd / 10.0)
        reward = 0.5 * carbon_norm + 0.3 * latency_norm + 0.2 * cost_norm
        reward = max(0.0, min(1.0, reward))

        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'strategy': self.routing_strategy.value,
            'reward': reward,
            'carbon_saved_kg': carbon_saved_kg,
            'latency_ms': latency_ms,
            'cost_usd': cost_usd,
        }
        self.performance_history.append(entry)
        if len(self.performance_history) > self.max_history_length:
            self.performance_history = self.performance_history[-self.max_history_length:]

        if self._last_decision is not None:
            state_vec = self._last_decision['state_vec']
            action_idx = self._last_decision['action_idx']
            teacher_probs = self._last_decision['teacher_probs']

            next_state = self._build_state()
            next_state_vec = next_state.to_feature_vector()

            # Optional causal reward shaping
            if self._causal_shaper is not None:
                # Snapshots not available here; use current state as proxy
                prev_snapshot = self.metadata.get('prev_snapshot', {})
                current_snapshot = self.metadata.get('current_snapshot', {})
                reward = self._causal_shaper.shape_reward(action_idx, reward, prev_snapshot, current_snapshot)

            await self._routing_optimizer.update(
                state_vec,
                action_idx,
                reward,
                next_state_vec,
                teacher_probs
            )

            entry['state_vec'] = ','.join(map(str, state_vec))
            self._last_decision = None

        # Persist logs
        log_path = Path(f"./node_{self.id}_routing_logs.csv")
        df = pd.DataFrame(self.performance_history)
        df.to_csv(log_path, index=False)

        json_path = Path(f"./node_{self.id}_routing_logs.json")
        with open(json_path, 'w') as f:
            json.dump(self.performance_history, f, indent=2)

        # Create FeedbackEvent if available
        if FeedbackEvent is not None:
            try:
                event = FeedbackEvent(
                    source="node_descriptor",
                    feedback_type="routing",
                    task_id=str(self.id),
                    context={"node_id": self.id},
                    action={"selected_action": self.routing_strategy.value,
                            "selected_rank": 1,
                            "confidence_score": self.metadata.get('confidence', 0.5)},
                    performance={"quality_score": reward,
                                 "latency_ms": latency_ms,
                                 "energy_joules": 0,
                                 "carbon_g": carbon_saved_kg * 1000,
                                 "helium_cost": 0,
                                 "duration_ms": 0},
                    adaptive_cost_value=reward,
                    tags=["node", "routing", self.routing_strategy.value],
                )
                logger.debug(f"FeedbackEvent created: {event.event_id}")
            except Exception as e:
                logger.warning(f"Failed to create FeedbackEvent: {e}")

    def _build_state(self) -> NodeState:
        """Build state from current node metrics and history."""
        if self.performance_history:
            recent = self.performance_history[-20:]
            success_rate = sum(1 for r in recent if r.get('reward', 0) > 0.5) / max(len(recent), 1)
            avg_reward = np.mean([r.get('reward', 0) for r in recent]) if recent else 0.0
        else:
            success_rate = 0.5
            avg_reward = 0.5

        graph_health = (self.graph_metrics or {}).get('centrality', 0.5)
        health_score = self.get_health_score() * (0.8 + 0.2 * graph_health)

        return NodeState(
            carbon_intensity=self.region_carbon_intensity,
            renewable_fraction=self.renewable_fraction,
            helium_connectivity=self.helium_connectivity_score,
            uptime=self.uptime,
            efficiency_score=self.efficiency_score or 0.5,
            health_score=health_score,
            cost_per_hour=self.cost_per_hour_usd or 1.0,
            energy_per_token=self.energy_per_token,
            recent_success_rate=success_rate,
            avg_reward=avg_reward,
            human_feedback_score=self.human_feedback_score,
        )

    def set_causal_graph(self, causal_graph):
        """Attach a causal graph for reward shaping."""
        if CausalRewardShaper is not None:
            self._causal_shaper = CausalRewardShaper(causal_graph, influence_weight=0.3)


# ============================================================================
# Convenience factory
# ============================================================================

def create_node_descriptor(
    id: str,
    node_type: NodeType,
    region: str,
    region_carbon_intensity: float,
    energy_per_token: float,
    **kwargs
) -> NodeDescriptor:
    """Factory function to create a NodeDescriptor with sensible defaults."""
    return NodeDescriptor(
        id=id,
        type=node_type,
        region=region,
        region_carbon_intensity=region_carbon_intensity,
        energy_per_token=energy_per_token,
        **kwargs
    )
