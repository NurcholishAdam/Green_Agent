"""
Enhanced Workload Descriptor v2.3.0
====================================
Defines the structure of a workload/task with adaptive priority selection
via Multi‑Teacher On‑Policy Distillation.

Changes from v2.2.0:
- Fixed missing deque import.
- Implemented historical ML training from logs.
- Corrected gating update (stores full teacher outputs in replay buffer).
- Added asyncio.Lock and epsilon decay to optimizer.
- Asynchronous persistence for logs.
- Causal reward shaping with snapshot mechanism.
- Federated learning support for student weights.
- Improved feature scaling and robust handling of n_classes.
- Added __init__ to initialise lock.
"""

from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from pathlib import Path
from collections import deque
import json
import random
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd
from dataclasses import dataclass
import asyncio

from pydantic import BaseModel, Field, field_validator, ConfigDict, PrivateAttr

import logging
logger = logging.getLogger(__name__)

# Optional FeedbackEvent import
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

class TaskType(str, Enum):
    TRAINING = "training"
    INFERENCE = "inference"
    EDGE_SENSING = "edge_sensing"
    FEDERATED_ROUND = "federated_round"
    MULTIMODAL = "multimodal"
    DATA_PROCESSING = "data_processing"
    QUERY = "query"
    INFERENCE_BATCH = "inference_batch"
    TRAINING_DISTRIBUTED = "training_distributed"
    EDGE_COMPUTE = "edge_compute"

class Urgency(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class Priority(str, Enum):
    ACCURACY = "accuracy"
    GREEN = "green"
    BALANCED = "balanced"

class BioMode(str, Enum):
    PHOTOSYNTHETIC = "photosynthetic"
    CHEMOTACTIC = "chemotactic"
    NONE = "none"

# ============================================================================
# Distillation components
# ============================================================================

@dataclass
class WorkloadState:
    """State for the distillation agent."""
    tokens: float
    latency_target: float
    urgency: float  # 0-3
    task_type_training: float = 0.0
    task_type_inference: float = 0.0
    task_type_edge_sensing: float = 0.0
    task_type_federated_round: float = 0.0
    task_type_multimodal: float = 0.0
    task_type_data_processing: float = 0.0
    task_type_query: float = 0.0
    task_type_inference_batch: float = 0.0
    task_type_training_distributed: float = 0.0
    task_type_edge_compute: float = 0.0
    estimated_energy: float = 0.0
    estimated_carbon: float = 0.0
    helium_units: float = 0.0
    sector_emission_factor: float = 0.0
    bio_mode_photosynthetic: float = 0.0
    bio_mode_chemotactic: float = 0.0
    recent_success_rate: float = 0.5
    avg_reward: float = 0.5
    human_feedback_score: float = 0.5
    graph_centrality: float = 0.5

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.tokens / 10000.0, 1.0),
            min(self.latency_target / 1000.0, 1.0),
            self.urgency / 3.0,
            self.task_type_training,
            self.task_type_inference,
            self.task_type_edge_sensing,
            self.task_type_federated_round,
            self.task_type_multimodal,
            self.task_type_data_processing,
            self.task_type_query,
            self.task_type_inference_batch,
            self.task_type_training_distributed,
            self.task_type_edge_compute,
            min(self.estimated_energy / 10.0, 1.0),
            min(self.estimated_carbon / 0.1, 1.0),
            min(self.helium_units / 0.1, 1.0),
            self.sector_emission_factor / 10.0,
            self.bio_mode_photosynthetic,
            self.bio_mode_chemotactic,
            self.recent_success_rate,
            self.avg_reward,
            self.human_feedback_score,
            self.graph_centrality,
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: WorkloadState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: WorkloadState) -> float:
        pass


class PriorityRuleBasedTeacher(Teacher):
    PRIORITIES = ['accuracy', 'green', 'balanced']

    def predict(self, state: WorkloadState) -> np.ndarray:
        probs = np.ones(3) * 0.1
        if state.urgency >= 2.0:
            probs[0] = 0.8
        elif state.estimated_carbon > 0.01 or state.helium_units > 0.01:
            probs[1] = 0.7
        elif state.latency_target < 100:
            probs[0] = 0.6
        else:
            probs[2] = 0.6
        return probs / probs.sum()

    def confidence(self, state: WorkloadState) -> float:
        # Use entropy of output for confidence
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-9))
        max_entropy = np.log(3)
        return 1.0 - entropy / max_entropy


class PriorityHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path("./priority_historical_model.pkl")
        self._load_model()

    def _load_model(self):
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: WorkloadState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(3) / 3
        x = state.to_feature_vector().reshape(1, -1)
        if hasattr(self.model, 'predict_proba'):
            probs = self.model.predict_proba(x)[0]
            # Ensure order matches Priority enum order
            class_order = [c for c in self.label_encoder.classes_]
            target_order = [p.value for p in Priority]
            new_probs = np.zeros(3)
            for i, p in enumerate(target_order):
                if p in class_order:
                    idx = class_order.index(p)
                    new_probs[i] = probs[idx]
            probs = new_probs / new_probs.sum() if new_probs.sum() > 0 else np.ones(3)/3
            return probs
        else:
            return np.ones(3) / 3

    def confidence(self, state: WorkloadState) -> float:
        if self.model is None:
            return 0.0
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-9))
        max_entropy = np.log(3)
        return 1.0 - entropy / max_entropy

    @classmethod
    def train_from_logs(cls, log_paths: List[Path], model_path: Path,
                        state_col: str = 'state_vec', label_col: str = 'priority'):
        """Train a RandomForest model from historical logs."""
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


class PriorityStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None, feature_dim: int = 23):
        self.lr = lr
        self.weights_path = weights_path or Path("./priority_q_weights.json")
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, 3))
        self._load_state()

    def _load_state(self):
        if self.weights_path.exists():
            try:
                with open(self.weights_path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                if self.weights.shape[0] != self.feature_dim:
                    logger.warning(f"Loaded Q‑weights shape {self.weights.shape}, expected ({self.feature_dim},3). Reinitializing.")
                    self.weights = np.zeros((self.feature_dim, 3))
                logger.info(f"Loaded Q‑teacher weights from {self.weights_path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        # Simple synchronous save; can be made async if needed
        with open(self.weights_path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: WorkloadState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: WorkloadState) -> float:
        # Could be based on Q-value spread, but fixed for now
        return 0.5

    def update(self, state: WorkloadState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class RLHFPriorityTeacher(Teacher):
    def predict(self, state: WorkloadState) -> np.ndarray:
        probs = np.ones(3) / 3
        if state.human_feedback_score > 0.7:
            probs[0] += 0.2  # accuracy
        elif state.human_feedback_score < 0.3:
            probs[1] += 0.2  # green
        return probs / probs.sum()

    def confidence(self, state: WorkloadState) -> float:
        return 0.7 if abs(state.human_feedback_score - 0.5) > 0.3 else 0.4


class DistillationStudent:
    def __init__(self, feature_dim: int = 23, n_classes: int = 3, lr: float = 0.01):
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0
        self.grad_clip = 1.0

    def predict_proba(self, state_vector, num_classes=None):
        if num_classes is None:
            num_classes = self.n_classes
        if num_classes != self.n_classes:
            # Resize only if explicitly requested
            if num_classes > self.n_classes:
                new_weights = np.zeros((self.feature_dim, num_classes))
                new_biases = np.zeros(num_classes)
                new_weights[:, :self.n_classes] = self.weights
                new_biases[:self.n_classes] = self.biases
                self.weights = new_weights
                self.biases = new_biases
                self.n_classes = num_classes
            else:
                self.weights = self.weights[:, :num_classes]
                self.biases = self.biases[:num_classes]
                self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vector, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        # Clip gradient
        grad = np.clip(grad, -self.grad_clip, self.grad_clip)
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

    def export_weights(self) -> Dict[str, np.ndarray]:
        return {"weights": self.weights.copy(), "biases": self.biases.copy()}

    def import_weights(self, weight_dict: Dict[str, np.ndarray]):
        self.weights = weight_dict["weights"].copy()
        self.biases = weight_dict["biases"].copy()


class ReplayBuffer:
    def __init__(self, max_size=2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec, action, reward, next_state_vec, teacher_outputs):
        """teacher_outputs: matrix (n_teachers, n_actions)"""
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_outputs))

    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_outputs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_outputs))

    def __len__(self):
        return len(self.buffer)


class MoEGatingNetwork:
    def __init__(self, feature_dim=23, n_experts=5, lr=0.005):
        self.feature_dim = feature_dim
        self.n_experts = n_experts
        self.lr = lr
        self.weights = np.random.randn(feature_dim, n_experts) * 0.01
        self.bias = np.zeros(n_experts)

    def forward(self, state_vec):
        logits = state_vec @ self.weights + self.bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vec, teacher_outputs, student_probs, current_gate_weights):
        """
        Correct update using actual teacher outputs and current gate weights.
        teacher_outputs: (n_teachers, n_actions)
        current_gate_weights: (n_teachers,)
        """
        combined = np.sum(current_gate_weights[:, None] * teacher_outputs, axis=0)
        error = combined - student_probs  # (n_actions,)
        # Gradient w.r.t. gate logits
        grad_gate = teacher_outputs @ error  # (n_teachers,)
        self.weights -= self.lr * np.outer(state_vec, grad_gate)
        self.bias -= self.lr * grad_gate


class DistillationPriorityOptimizer:
    PRIORITIES = ['accuracy', 'green', 'balanced']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.feature_dim = 23
        self.n_actions = 3
        self.student = DistillationStudent(feature_dim=self.feature_dim,
                                           lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            PriorityRuleBasedTeacher(),
            PriorityHistoricalMLTeacher(model_path=config.get('historical_model_path')),
            PriorityStatefulQTeacher(lr=config.get('q_learning_rate', 0.1),
                                     weights_path=config.get('q_weights_path'),
                                     feature_dim=self.feature_dim),
            RLHFPriorityTeacher()
        ]
        # Optional quantum teacher
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
        self.epsilon_min = config.get('distillation_epsilon_min', 0.01)
        self.epsilon_decay = config.get('distillation_epsilon_decay', 0.995)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
        self.distill_weight = config.get('distill_weight', 0.7)
        self.rl_weight = config.get('rl_weight', 0.3)
        self.batch_update_size = config.get('batch_update_size', 8)
        self.lock = asyncio.Lock()

    def _compute_teacher_probs(self, state) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Returns (combined_probs, gate_weights, teacher_outputs_matrix)."""
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
        teacher_outputs = np.array(teacher_outputs)  # (n_teachers, n_actions)
        gate_weights = self.gating.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        combined = combined / combined.sum()
        return combined, gate_weights, teacher_outputs

    async def select_priority(self, state, exploration=True):
        async with self.lock:
            state_vec = state.to_feature_vector()
            teacher_probs, gate_weights, teacher_outputs = self._compute_teacher_probs(state)
            student_probs = self.student.predict_proba(state_vec, self.n_actions)
            if exploration and random.random() < self.epsilon:
                action_idx = random.randint(0, self.n_actions - 1)
            else:
                combined = 0.8 * student_probs + 0.2 * teacher_probs
                action_idx = int(np.argmax(combined))
            # Decay epsilon
            self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
            return self.PRIORITIES[action_idx], action_idx, state_vec, teacher_outputs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_outputs):
        async with self.lock:
            self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_outputs)
            self.counter += 1
            if self.counter % self.train_every == 0 and len(self.replay_buffer) >= self.batch_update_size:
                batch = self.replay_buffer.sample(self.batch_update_size)
                states, actions, rewards, _, teacher_outputs_batch = batch
                for i in range(len(states)):
                    # Use average teacher probs as distillation target
                    avg_teacher_probs = teacher_outputs_batch[i].mean(axis=0)
                    self.student.update(states[i], avg_teacher_probs, rewards[i], actions[i],
                                        distill_weight=self.distill_weight, rl_weight=self.rl_weight)
                    # Update gating with actual teacher outputs and current gate weights
                    current_gate = self.gating.forward(states[i])
                    student_out = self.student.predict_proba(states[i])
                    self.gating.update(states[i], teacher_outputs_batch[i], student_out, current_gate)

    async def export_student_weights(self) -> Dict[str, np.ndarray]:
        async with self.lock:
            return self.student.export_weights()

    async def import_student_weights(self, weights: Dict[str, np.ndarray]):
        async with self.lock:
            self.student.import_weights(weights)

    def get_stats(self):
        return {'student_counter': self.student.counter,
                'buffer_size': len(self.replay_buffer),
                'epsilon': self.epsilon}


# ============================================================================
# Enhanced WorkloadDescriptor
# ============================================================================

class WorkloadDescriptor(BaseModel):
    model_config = ConfigDict(extra='allow', arbitrary_types_allowed=True)

    task_id: Optional[str] = Field(None, description="Unique task identifier")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")

    task_type: TaskType = Field(..., description="Type of the workload")
    tokens: int = Field(..., ge=1, description="Number of tokens")
    latency_target: float = Field(..., gt=0, description="Target latency in ms")
    deadline: Optional[datetime] = Field(None, description="Hard deadline")
    urgency: Urgency = Field(Urgency.MEDIUM, description="Urgency level")

    sector_emission_factor: Optional[float] = Field(None, ge=0, description="kg CO₂ per $ revenue")
    estimated_energy_joules: Optional[float] = Field(None, ge=0, description="Estimated energy (J)")
    estimated_carbon_kg: Optional[float] = Field(None, ge=0, description="Estimated carbon (kg CO₂)")
    helium_units: Optional[float] = Field(None, ge=0, description="Estimated helium units")

    data_size_bytes: Optional[int] = Field(None, ge=0, description="Input data size (bytes)")
    model_size_bytes: Optional[int] = Field(None, ge=0, description="Model size (bytes)")

    user_id: Optional[str] = Field(None, description="User identifier")
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")

    bio_mode: BioMode = Field(BioMode.NONE, description="Bio‑inspired operation mode")

    adaptive_priority: Priority = Field(Priority.BALANCED, description="Current adaptive priority")
    performance_history: List[Dict[str, Any]] = Field(default_factory=list, description="Recent scheduling outcomes")
    max_history_length: int = Field(100, ge=1, description="Maximum history entries")

    # New fields for enhancements
    graph_metrics: Optional[Dict[str, float]] = Field(None, description="LIMIT Graph metrics")
    human_feedback_score: float = Field(0.5, ge=0, le=1, description="RLHF human feedback score")
    use_evolutionary: bool = Field(False, description="Enable evolutionary optimisation")
    evolutionary_population_size: int = Field(20, ge=2)
    evolutionary_mutation_rate: float = Field(0.1, ge=0, le=1)
    evolutionary_crossover_rate: float = Field(0.7, ge=0, le=1)
    evolutionary_elitism: int = Field(2, ge=1)
    use_quantum_teacher: bool = Field(False, description="Enable quantum teacher")

    version: str = Field("2.3.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    _priority_optimizer: Optional[DistillationPriorityOptimizer] = PrivateAttr(default=None)
    _last_decision: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _causal_shaper: Optional[CausalRewardShaper] = PrivateAttr(default=None)
    _lock: Any = PrivateAttr(default=None)
    _prev_snapshot: Dict[str, float] = PrivateAttr(default_factory=dict)
    _current_snapshot: Dict[str, float] = PrivateAttr(default_factory=dict)

    def __init__(self, **data):
        super().__init__(**data)
        self._lock = asyncio.Lock()  # Initialize lock immediately

    @field_validator('sector_emission_factor')
    def validate_sector_emission_factor(cls, v):
        if v is not None and v < 0:
            raise ValueError("sector_emission_factor must be non‑negative")
        return v

    @field_validator('latency_target')
    def validate_latency_target(cls, v):
        if v <= 0:
            raise ValueError("latency_target must be positive")
        return v

    def compute_energy_cost(self, energy_per_token: float) -> float:
        return energy_per_token * self.tokens

    def compute_carbon_cost(self, carbon_intensity_kg_per_kwh: float) -> float:
        energy_kwh = self.tokens * 0.00001
        return energy_kwh * carbon_intensity_kg_per_kwh

    def to_dict(self, exclude_none=False):
        data = self.model_dump()
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def is_critical(self):
        return self.urgency == Urgency.CRITICAL

    def is_high_priority(self):
        return self.urgency in (Urgency.HIGH, Urgency.CRITICAL)

    def _ensure_optimizer(self):
        if self._priority_optimizer is None:
            self._priority_optimizer = DistillationPriorityOptimizer({
                'distillation_epsilon': self.metadata.get('distillation_epsilon', 0.1),
                'distillation_epsilon_min': self.metadata.get('distillation_epsilon_min', 0.01),
                'distillation_epsilon_decay': self.metadata.get('distillation_epsilon_decay', 0.995),
                'distillation_train_every': self.metadata.get('distillation_train_every', 10),
                'distillation_replay_size': self.metadata.get('distillation_replay_size', 2000),
                'distillation_learning_rate': self.metadata.get('distillation_learning_rate', 0.01),
                'historical_model_path': self.metadata.get('historical_model_path'),
                'q_learning_rate': self.metadata.get('q_learning_rate', 0.1),
                'q_weights_path': self.metadata.get('q_weights_path', Path(f"./priority_q_weights_{self.task_id}.json")),
                'distill_weight': self.metadata.get('distill_weight', 0.7),
                'rl_weight': self.metadata.get('rl_weight', 0.3),
                'batch_update_size': self.metadata.get('batch_update_size', 8),
                'gating_learning_rate': self.metadata.get('gating_learning_rate', 0.005),
                'use_quantum_teacher': self.use_quantum_teacher,
                'quantum_teacher_qubits': self.metadata.get('quantum_teacher_qubits', 4),
                'quantum_teacher_layers': self.metadata.get('quantum_teacher_layers', 2),
            })
        if self.use_evolutionary and not hasattr(self, '_evolutionary_optimizer'):
            # Evolutionary optimizer placeholder - can be added if needed
            pass

    async def select_priority(self, exploration=True, latency_achieved_ms=None,
                              carbon_saved_kg=None, energy_used_joules=None,
                              snapshot_before: Optional[Dict[str, float]] = None):
        async with self._lock:
            self._ensure_optimizer()
            if snapshot_before:
                self._prev_snapshot = snapshot_before.copy()
            state = self._build_state()
            priority, action_idx, state_vec, teacher_outputs = await self._priority_optimizer.select_priority(
                state, exploration=exploration
            )
            self._last_decision = {
                'state_vec': state_vec,
                'action_idx': action_idx,
                'teacher_outputs': teacher_outputs,
            }
            self.adaptive_priority = Priority(priority)

            if latency_achieved_ms is not None and carbon_saved_kg is not None and energy_used_joules is not None:
                await self._record_outcome_locked(latency_achieved_ms, carbon_saved_kg, energy_used_joules)

            return self.adaptive_priority

    async def record_outcome(self, latency_achieved_ms, carbon_saved_kg, energy_used_joules,
                             snapshot_after: Optional[Dict[str, float]] = None):
        async with self._lock:
            await self._record_outcome_locked(latency_achieved_ms, carbon_saved_kg, energy_used_joules, snapshot_after)

    async def _record_outcome_locked(self, latency_achieved_ms, carbon_saved_kg, energy_used_joules,
                                     snapshot_after: Optional[Dict[str, float]] = None):
        # Compute reward
        latency_score = 1.0 - min(1.0, abs(latency_achieved_ms - self.latency_target) / self.latency_target)
        carbon_norm = min(1.0, carbon_saved_kg / 0.1)
        energy_norm = 1.0 - min(1.0, energy_used_joules / (self.estimated_energy_joules or 0.1))
        reward = 0.4 * latency_score + 0.3 * carbon_norm + 0.3 * energy_norm
        reward = max(0.0, min(1.0, reward))

        # Causal shaping with snapshots if available
        if self._causal_shaper is not None and self._prev_snapshot and (snapshot_after or self._current_snapshot):
            snap_after = snapshot_after or self._current_snapshot
            if self._last_decision is not None:
                reward = self._causal_shaper.shape_reward(
                    self._last_decision['action_idx'], reward,
                    self._prev_snapshot, snap_after
                )

        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'priority': self.adaptive_priority.value,
            'reward': reward,
            'latency_achieved_ms': latency_achieved_ms,
            'carbon_saved_kg': carbon_saved_kg,
            'energy_used_joules': energy_used_joules,
        }
        if self._last_decision is not None:
            state_vec = self._last_decision['state_vec']
            action_idx = self._last_decision['action_idx']
            teacher_outputs = self._last_decision['teacher_outputs']
            next_state = self._build_state()
            next_state_vec = next_state.to_feature_vector()
            await self._priority_optimizer.update(state_vec, action_idx, reward, next_state_vec, teacher_outputs)
            entry['state_vec'] = ','.join(map(str, state_vec))
            self._last_decision = None

        self.performance_history.append(entry)
        if len(self.performance_history) > self.max_history_length:
            self.performance_history = self.performance_history[-self.max_history_length:]

        # Asynchronous persistence
        await asyncio.to_thread(self._persist_logs_sync)

        # FeedbackEvent
        if FeedbackEvent is not None:
            try:
                event = FeedbackEvent(
                    source="workload_descriptor",
                    feedback_type="scheduling",
                    task_id=self.task_id or "",
                    context={"workload_id": self.task_id},
                    action={"selected_action": self.adaptive_priority.value, "selected_rank": 1},
                    performance={"quality_score": reward,
                                 "latency_ms": latency_achieved_ms,
                                 "energy_joules": energy_used_joules,
                                 "carbon_g": carbon_saved_kg * 1000,
                                 "helium_cost": 0,
                                 "duration_ms": 0},
                    adaptive_cost_value=reward,
                    tags=["workload", "scheduling", self.adaptive_priority.value],
                )
                logger.debug(f"FeedbackEvent created: {event.event_id}")
            except Exception as e:
                logger.warning(f"Failed to create FeedbackEvent: {e}")

    def _persist_logs_sync(self):
        log_path = Path(f"./workload_{self.task_id or 'unknown'}_logs.csv")
        df = pd.DataFrame(self.performance_history)
        df.to_csv(log_path, index=False)
        json_path = Path(f"./workload_{self.task_id or 'unknown'}_logs.json")
        with open(json_path, 'w') as f:
            json.dump(self.performance_history, f, indent=2)

    def _build_state(self):
        urgency_map = {Urgency.LOW: 0, Urgency.MEDIUM: 1, Urgency.HIGH: 2, Urgency.CRITICAL: 3}
        urgency_val = urgency_map.get(self.urgency, 1)

        task_type_onehot = {t: 0.0 for t in TaskType}
        task_type_onehot[self.task_type] = 1.0

        bio_mode_photosynthetic = 1.0 if self.bio_mode == BioMode.PHOTOSYNTHETIC else 0.0
        bio_mode_chemotactic = 1.0 if self.bio_mode == BioMode.CHEMOTACTIC else 0.0

        if self.performance_history:
            recent = self.performance_history[-20:]
            success_rate = sum(1 for r in recent if r.get('reward', 0) > 0.5) / max(len(recent), 1)
            avg_reward = np.mean([r.get('reward', 0) for r in recent]) if recent else 0.0
        else:
            success_rate = 0.5
            avg_reward = 0.5

        graph_centrality = (self.graph_metrics or {}).get('centrality', 0.5)

        return WorkloadState(
            tokens=self.tokens,
            latency_target=self.latency_target,
            urgency=urgency_val,
            task_type_training=task_type_onehot[TaskType.TRAINING],
            task_type_inference=task_type_onehot[TaskType.INFERENCE],
            task_type_edge_sensing=task_type_onehot[TaskType.EDGE_SENSING],
            task_type_federated_round=task_type_onehot[TaskType.FEDERATED_ROUND],
            task_type_multimodal=task_type_onehot[TaskType.MULTIMODAL],
            task_type_data_processing=task_type_onehot[TaskType.DATA_PROCESSING],
            task_type_query=task_type_onehot[TaskType.QUERY],
            task_type_inference_batch=task_type_onehot[TaskType.INFERENCE_BATCH],
            task_type_training_distributed=task_type_onehot[TaskType.TRAINING_DISTRIBUTED],
            task_type_edge_compute=task_type_onehot[TaskType.EDGE_COMPUTE],
            estimated_energy=self.estimated_energy_joules or 0.0,
            estimated_carbon=self.estimated_carbon_kg or 0.0,
            helium_units=self.helium_units or 0.0,
            sector_emission_factor=self.sector_emission_factor or 0.0,
            bio_mode_photosynthetic=bio_mode_photosynthetic,
            bio_mode_chemotactic=bio_mode_chemotactic,
            recent_success_rate=success_rate,
            avg_reward=avg_reward,
            human_feedback_score=self.human_feedback_score,
            graph_centrality=graph_centrality,
        )

    def set_causal_graph(self, causal_graph):
        if CausalRewardShaper is not None:
            self._causal_shaper = CausalRewardShaper(causal_graph, influence_weight=0.3)

    async def export_federated_weights(self) -> Dict[str, np.ndarray]:
        self._ensure_optimizer()
        return await self._priority_optimizer.export_student_weights()

    async def import_federated_weights(self, weights: Dict[str, np.ndarray]):
        self._ensure_optimizer()
        await self._priority_optimizer.import_student_weights(weights)


def create_workload_descriptor(
    task_type: TaskType,
    tokens: int,
    latency_target: float,
    **kwargs
) -> WorkloadDescriptor:
    return WorkloadDescriptor(
        task_type=task_type,
        tokens=tokens,
        latency_target=latency_target,
        **kwargs
    )
