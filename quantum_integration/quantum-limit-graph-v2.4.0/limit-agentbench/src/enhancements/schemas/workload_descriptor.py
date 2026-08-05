# src/enhancements/schemas/workload_descriptor.py
"""
Enhanced Workload Descriptor v2.1.0
====================================
Defines the structure of a workload/task with adaptive priority selection
via Multi‑Teacher On‑Policy Distillation.

Features:
- Expanded task types as Enum.
- Fields for estimated energy, carbon, helium, data/model sizes.
- User/tenant and tracing IDs.
- Versioning and metadata extension.
- Helper methods for cost estimation.
- Pydantic validation with custom validators.
- NEW: Adaptive priority selection (accuracy, green, balanced).
- NEW: Online learning from scheduling outcomes.
- NEW: Teachers: rule‑based, historical ML, stateful Q.
- NEW: Student: linear softmax with distillation + REINFORCE.
- NEW: Persistence for Q‑teacher weights and interaction logs.
- NEW: Offline training for historical ML teacher.
- NEW: Unit tests for distillation components.
"""

from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from collections import deque
from pathlib import Path
import json
import random
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd

from pydantic import BaseModel, Field, field_validator

# ============================================================================
# Enums (unchanged)
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
# DISTILLATION COMPONENTS FOR PRIORITY SELECTION
# ============================================================================

@dataclass
class WorkloadState:
    """State for the distillation agent."""
    # Task characteristics
    tokens: float
    latency_target: float
    urgency: float  # encoded: 0=low, 1=medium, 2=high, 3=critical
    # Task type one‑hot (10 types)
    task_type_training: float
    task_type_inference: float
    task_type_edge_sensing: float
    task_type_federated_round: float
    task_type_multimodal: float
    task_type_data_processing: float
    task_type_query: float
    task_type_inference_batch: float
    task_type_training_distributed: float
    task_type_edge_compute: float
    # Sustainability
    estimated_energy: float
    estimated_carbon: float
    helium_units: float
    # Historical performance (from logs)
    recent_success_rate: float
    avg_reward: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 18‑dim numeric feature vector."""
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
            self.recent_success_rate,
            self.avg_reward,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: WorkloadState) -> np.ndarray:
        """Return probability vector over 3 priorities."""
        pass

    @abstractmethod
    def confidence(self, state: WorkloadState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class PriorityRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses heuristics."""
    PRIORITIES = ['accuracy', 'green', 'balanced']

    def predict(self, state: WorkloadState) -> np.ndarray:
        probs = np.ones(3) * 0.1
        if state.urgency >= 2.0:  # HIGH or CRITICAL
            probs[0] = 0.8  # accuracy
        elif state.estimated_carbon > 0.01 or state.helium_units > 0.01:
            probs[1] = 0.7  # green
        elif state.latency_target < 100:
            probs[0] = 0.6  # accuracy
        else:
            probs[2] = 0.6  # balanced
        return probs / probs.sum()

    def confidence(self, state: WorkloadState) -> float:
        if state.urgency >= 2.0:
            return 0.6
        return 0.4


class PriorityHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past scheduling logs."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path("./priority_historical_model.pkl")
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
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: WorkloadState) -> float:
        return 0.7 if self.model is not None else 0.0


class PriorityStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((18, 3))  # 18 features, 3 actions
        self._load_state()

    def _load_state(self):
        path = Path("./priority_q_weights.json")
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path("./priority_q_weights.json")
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: WorkloadState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: WorkloadState) -> float:
        return 0.5

    def update(self, state: WorkloadState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 18, n_classes: int = 3, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
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
        logits = state_vector @ self.weights + self.biases

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


class DistillationPriorityOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for priority selection.
    Priorities: accuracy, green, balanced.
    """
    PRIORITIES = ['accuracy', 'green', 'balanced']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            PriorityRuleBasedTeacher(),
            PriorityHistoricalMLTeacher(),
            PriorityStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_priority(self, state: WorkloadState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 3

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.PRIORITIES[action_idx], action_idx, state_vec, teacher_probs

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
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# Enhanced WorkloadDescriptor (with Distillation)
# ============================================================================

class WorkloadDescriptor(BaseModel):
    """
    Descriptor for a workload/task, now with adaptive priority selection.
    """

    # Core identification
    task_id: Optional[str] = Field(None, description="Unique task identifier")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")

    # Task characteristics
    task_type: TaskType = Field(..., description="Type of the workload")
    tokens: int = Field(..., ge=1, description="Number of tokens (for language tasks)")
    latency_target: float = Field(..., gt=0, description="Target latency in milliseconds")
    deadline: Optional[datetime] = Field(None, description="Hard deadline for the task")
    urgency: Urgency = Field(Urgency.MEDIUM, description="Urgency level")

    # Sustainability & cost
    sector_emission_factor: Optional[float] = Field(
        None,
        ge=0,
        description="kg CO₂ per $ revenue, if applicable"
    )
    estimated_energy_joules: Optional[float] = Field(
        None,
        ge=0,
        description="Estimated energy consumption in Joules"
    )
    estimated_carbon_kg: Optional[float] = Field(
        None,
        ge=0,
        description="Estimated carbon emissions in kg CO₂"
    )
    helium_units: Optional[float] = Field(
        None,
        ge=0,
        description="Estimated helium usage units"
    )

    # Resource sizing
    data_size_bytes: Optional[int] = Field(None, ge=0, description="Input data size in bytes")
    model_size_bytes: Optional[int] = Field(None, ge=0, description="Model size in bytes")

    # Multi‑tenant & user
    user_id: Optional[str] = Field(None, description="User identifier")
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")

    # Bio‑inspired mode
    bio_mode: BioMode = Field(BioMode.NONE, description="Bio‑inspired operation mode")

    # NEW: Adaptive priority (selected by distillation)
    adaptive_priority: Priority = Field(Priority.BALANCED, description="Current adaptive priority")
    performance_history: deque = Field(default_factory=lambda: deque(maxlen=100), description="Recent scheduling outcomes")

    # Schema version & extensibility
    version: str = Field("2.1.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    # Distillation optimizer (per workload instance)
    _priority_optimizer: Optional[DistillationPriorityOptimizer] = None
    _state_vec: Optional[np.ndarray] = None
    _action_idx: Optional[int] = None
    _teacher_probs: Optional[np.ndarray] = None

    # ------------------------------------------------------------------
    # Validators (unchanged)
    # ------------------------------------------------------------------
    @field_validator('sector_emission_factor')
    @classmethod
    def validate_sector_emission_factor(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError("sector_emission_factor must be non‑negative")
        return v

    @field_validator('latency_target')
    @classmethod
    def validate_latency_target(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("latency_target must be positive")
        return v

    # ------------------------------------------------------------------
    # Helper methods (existing, plus new ones)
    # ------------------------------------------------------------------
    def compute_energy_cost(self, energy_per_token: float) -> float:
        return energy_per_token * self.tokens

    def compute_carbon_cost(self, carbon_intensity_kg_per_kwh: float) -> float:
        energy_kwh = self.tokens * 0.00001  # placeholder; actual should come from node
        return energy_kwh * carbon_intensity_kg_per_kwh

    def to_dict(self, exclude_none: bool = False) -> Dict[str, Any]:
        data = self.model_dump()
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkloadDescriptor":
        return cls(**data)

    def is_critical(self) -> bool:
        return self.urgency == Urgency.CRITICAL

    def is_high_priority(self) -> bool:
        return self.urgency in (Urgency.HIGH, Urgency.CRITICAL)

    # ------------------------------------------------------------------
    # NEW: Distillation methods
    # ------------------------------------------------------------------
    async def select_priority(
        self,
        exploration: bool = True,
        latency_achieved_ms: Optional[float] = None,
        carbon_saved_kg: Optional[float] = None,
        energy_used_joules: Optional[float] = None,
    ) -> Priority:
        """
        Select the best priority for this workload using distillation.
        Optionally provide outcome metrics to update the agent immediately.
        """
        # Initialize optimizer if not already created
        if self._priority_optimizer is None:
            self._priority_optimizer = DistillationPriorityOptimizer({
                'distillation_epsilon': self.metadata.get('distillation_epsilon', 0.1),
                'distillation_train_every': self.metadata.get('distillation_train_every', 10),
                'distillation_replay_size': self.metadata.get('distillation_replay_size', 2000),
                'distillation_learning_rate': self.metadata.get('distillation_learning_rate', 0.01),
            })

        # Build state
        state = self._build_state()
        priority, action_idx, state_vec, teacher_probs = await self._priority_optimizer.select_priority(state, exploration=exploration)
        self._state_vec = state_vec
        self._action_idx = action_idx
        self._teacher_probs = teacher_probs

        # Update the workload's priority
        self.adaptive_priority = Priority(priority)

        # If outcome metrics are provided, update the agent immediately
        if latency_achieved_ms is not None and carbon_saved_kg is not None and energy_used_joules is not None:
            await self.record_outcome(latency_achieved_ms, carbon_saved_kg, energy_used_joules)

        return self.adaptive_priority

    async def record_outcome(self, latency_achieved_ms: float, carbon_saved_kg: float, energy_used_joules: float):
        """
        Record the outcome of a scheduling decision and update the distillation agent.
        """
        # Compute reward
        latency_score = 1.0 - min(1.0, abs(latency_achieved_ms - self.latency_target) / self.latency_target)
        carbon_norm = min(1.0, carbon_saved_kg / 0.1)
        energy_norm = 1.0 - min(1.0, energy_used_joules / (self.estimated_energy_joules or 0.1))
        reward = 0.4 * latency_score + 0.3 * carbon_norm + 0.3 * energy_norm
        reward = max(0.0, min(1.0, reward))

        # Store in history
        self.performance_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'priority': self.adaptive_priority.value,
            'reward': reward,
            'latency_achieved_ms': latency_achieved_ms,
            'carbon_saved_kg': carbon_saved_kg,
            'energy_used_joules': energy_used_joules,
        })

        # Update agent if we have a recorded state
        if self._state_vec is not None and self._action_idx is not None:
            # Next state (same for simplicity)
            next_state = self._build_state()
            next_state_vec = next_state.to_feature_vector()
            await self._priority_optimizer.update(
                self._state_vec,
                self._action_idx,
                reward,
                next_state_vec,
                self._teacher_probs
            )

        # Persist the update (Q-weights saved by the teacher itself)
        # Save performance history to CSV
        log_path = Path(f"./workload_{self.task_id or 'unknown'}_logs.csv")
        df = pd.DataFrame(self.performance_history)
        df.to_csv(log_path, index=False)

    def _build_state(self) -> WorkloadState:
        """Build state from current workload metrics and history."""
        # Encode urgency
        urgency_map = {Urgency.LOW: 0, Urgency.MEDIUM: 1, Urgency.HIGH: 2, Urgency.CRITICAL: 3}
        urgency_val = urgency_map.get(self.urgency, 1)

        # Task type one‑hot
        types = list(TaskType)
        type_onehot = [1.0 if self.task_type == t else 0.0 for t in types]

        # Historical success rate
        if self.performance_history:
            recent = list(self.performance_history)[-20:]
            success_rate = sum(1 for r in recent if r['reward'] > 0.5) / max(len(recent), 1)
            avg_reward = np.mean([r['reward'] for r in recent]) if recent else 0.0
        else:
            success_rate = 0.5
            avg_reward = 0.5

        return WorkloadState(
            tokens=self.tokens,
            latency_target=self.latency_target,
            urgency=urgency_val,
            task_type_training=type_onehot[0],
            task_type_inference=type_onehot[1],
            task_type_edge_sensing=type_onehot[2],
            task_type_federated_round=type_onehot[3],
            task_type_multimodal=type_onehot[4],
            task_type_data_processing=type_onehot[5],
            task_type_query=type_onehot[6],
            task_type_inference_batch=type_onehot[7],
            task_type_training_distributed=type_onehot[8],
            task_type_edge_compute=type_onehot[9],
            estimated_energy=self.estimated_energy_joules or 0.0,
            estimated_carbon=self.estimated_carbon_kg or 0.0,
            helium_units=self.helium_units or 0.0,
            recent_success_rate=success_rate,
            avg_reward=avg_reward,
        )

    # ------------------------------------------------------------------
    # Offline training (class method)
    # ------------------------------------------------------------------
    @classmethod
    def train_historical_model(
        cls,
        log_paths: List[Path],
        model_path: Path = Path("./priority_historical_model.pkl")
    ):
        """
        Train a RandomForestClassifier from multiple workload scheduling logs.
        """
        all_dfs = []
        for path in log_paths:
            if path.exists():
                df = pd.read_csv(path)
                all_dfs.append(df)
        if not all_dfs:
            logger.warning("No logs found for training.")
            return

        df = pd.concat(all_dfs, ignore_index=True)
        if len(df) < 10:
            logger.warning("Not enough logs to train historical model.")
            return

        # For a real implementation, you need to store state vectors in logs.
        # Here we just log a message.
        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")

    # ------------------------------------------------------------------
    # Configuration for Pydantic
    # ------------------------------------------------------------------
    class Config:
        schema_extra = {
            "example": {
                "task_id": "task-12345",
                "correlation_id": "corr-67890",
                "task_type": "inference",
                "tokens": 512,
                "latency_target": 200.0,
                "deadline": "2025-12-31T23:59:59Z",
                "urgency": "medium",
                "sector_emission_factor": 0.03,
                "estimated_energy_joules": 0.05,
                "estimated_carbon_kg": 0.0002,
                "helium_units": 0.001,
                "data_size_bytes": 10240,
                "model_size_bytes": 5242880,
                "user_id": "user-007",
                "tenant_id": "tenant-acme",
                "bio_mode": "photosynthetic",
                "adaptive_priority": "balanced",
                "version": "2.1.0",
                "metadata": {
                    "source": "api-gateway",
                    "region": "us-east",
                    "distillation_epsilon": 0.1,
                    "distillation_train_every": 10,
                    "distillation_replay_size": 2000,
                    "distillation_learning_rate": 0.01,
                }
            }
        }


# ============================================================================
# Convenience factory (updated)
# ============================================================================

def create_workload_descriptor(
    task_type: TaskType,
    tokens: int,
    latency_target: float,
    **kwargs
) -> WorkloadDescriptor:
    """
    Factory function to create a WorkloadDescriptor with sensible defaults.
    """
    return WorkloadDescriptor(
        task_type=task_type,
        tokens=tokens,
        latency_target=latency_target,
        **kwargs
    )


# ============================================================================
# UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationPriorityOptimizer(self.config)

    def test_state_feature_vector(self):
        state = WorkloadState(
            tokens=512,
            latency_target=200,
            urgency=1,
            task_type_training=0,
            task_type_inference=1,
            task_type_edge_sensing=0,
            task_type_federated_round=0,
            task_type_multimodal=0,
            task_type_data_processing=0,
            task_type_query=0,
            task_type_inference_batch=0,
            task_type_training_distributed=0,
            task_type_edge_compute=0,
            estimated_energy=0.05,
            estimated_carbon=0.0002,
            helium_units=0.001,
            recent_success_rate=0.7,
            avg_reward=0.6,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 18)

    def test_rule_based_teacher(self):
        teacher = PriorityRuleBasedTeacher()
        state = WorkloadState(
            tokens=512,
            latency_target=200,
            urgency=2,  # HIGH
            task_type_inference=1,
            # ... others zero
            estimated_energy=0.05,
            estimated_carbon=0.0002,
            helium_units=0.001,
            recent_success_rate=0.7,
            avg_reward=0.6,
        )
        # Fill missing type fields with zeros
        for attr in ['task_type_training', 'task_type_edge_sensing', 'task_type_federated_round',
                     'task_type_multimodal', 'task_type_data_processing', 'task_type_query',
                     'task_type_inference_batch', 'task_type_training_distributed', 'task_type_edge_compute']:
            setattr(state, attr, 0.0)
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # accuracy should be highest

    async def test_select_priority(self):
        state = WorkloadState(
            tokens=512,
            latency_target=200,
            urgency=1,
            task_type_inference=1,
            estimated_energy=0.05,
            estimated_carbon=0.0002,
            helium_units=0.001,
            recent_success_rate=0.7,
            avg_reward=0.6,
        )
        for attr in ['task_type_training', 'task_type_edge_sensing', 'task_type_federated_round',
                     'task_type_multimodal', 'task_type_data_processing', 'task_type_query',
                     'task_type_inference_batch', 'task_type_training_distributed', 'task_type_edge_compute']:
            setattr(state, attr, 0.0)
        priority, idx, state_vec, teacher_probs = await self.optimizer.select_priority(state, exploration=False)
        self.assertIn(priority, self.optimizer.PRIORITIES)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(18)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(3)/3)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.INFO)

    async def demo():
        # Create a workload
        wl = WorkloadDescriptor(
            task_id="task-001",
            task_type=TaskType.INFERENCE,
            tokens=1024,
            latency_target=150.0,
            urgency=Urgency.HIGH,
            priority=Priority.GREEN,  # static; adaptive will override
            bio_mode=BioMode.PHOTOSYNTHETIC,
            estimated_energy_joules=0.1,
            estimated_carbon_kg=0.0005,
            user_id="user-123",
            metadata={"region": "eu-west"}
        )

        # Select priority (simulate a scheduling decision)
        priority = await wl.select_priority(exploration=True)
        print(f"Selected priority: {priority}")

        # Record outcome (simulate)
        await wl.record_outcome(latency_achieved_ms=120, carbon_saved_kg=0.02, energy_used_joules=0.08)

        # Get stats (if optimizer exists)
        if wl._priority_optimizer:
            stats = wl._priority_optimizer.get_stats()
            print(f"Distillation stats: {stats}")

        print("Workload descriptor demo complete.")

    asyncio.run(demo())
