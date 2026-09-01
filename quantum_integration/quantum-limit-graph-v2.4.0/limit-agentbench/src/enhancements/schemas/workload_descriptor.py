# src/enhancements/schemas/workload_descriptor.py
"""
Enhanced Workload Descriptor v2.2.0
====================================
Defines the structure of a workload/task with adaptive priority selection
via Multi‑Teacher On‑Policy Distillation, augmented with:

- LIMIT Graph integration (graph ID, embedding, metrics).
- MODP (Multi‑Objective Decision Process) with tunable weights.
- RLHF (Reinforcement Learning from Human Feedback) via a dedicated teacher and
  human feedback score in the state.
- Multi‑Teacher On‑Policy Distillation with a learned Mixture‑of‑Experts (MoE)
  gating network.
- Bio‑inspired optimisation (evolutionary algorithm) as an optional complement.
- MoE expert support (gating network over teachers).
- All original features (task types, bio‑modes, persistence, etc.) remain.

Version: 2.2.0
"""

from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from pathlib import Path
import json
import random
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd
from dataclasses import dataclass

from pydantic import BaseModel, Field, field_validator, ConfigDict, PrivateAttr

# Logger setup
import logging
logger = logging.getLogger(__name__)

# Optional FeedbackEvent import
try:
    from .feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

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
# CONSTANTS
# ============================================================================
# Feature dimension: original 21 + human_feedback_score = 22
FEATURE_DIM = 22

# ============================================================================
# Distillation components (ENHANCED)
# ============================================================================

@dataclass
class WorkloadState:
    """State for the distillation agent."""
    # Task characteristics
    tokens: float
    latency_target: float
    urgency: float  # 0-3
    # Task type one-hot (10)
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
    # Sustainability
    estimated_energy: float = 0.0
    estimated_carbon: float = 0.0
    helium_units: float = 0.0
    # Additional features
    sector_emission_factor: float = 0.0
    bio_mode_photosynthetic: float = 0.0
    bio_mode_chemotactic: float = 0.0
    # Historical performance
    recent_success_rate: float = 0.5
    avg_reward: float = 0.5
    # NEW for RLHF
    human_feedback_score: float = 0.5  # 0..1

    def to_feature_vector(self) -> np.ndarray:
        """Convert to numeric feature vector (22 dims)."""
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
            self.human_feedback_score,  # NEW
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
        return 0.6 if state.urgency >= 2.0 else 0.4


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
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: WorkloadState) -> float:
        return 0.7 if self.model is not None else 0.0

    @classmethod
    def train_from_logs(cls, log_paths: List[Path], model_path: Path,
                        state_col: str = 'state_vec', label_col: str = 'priority'):
        """Train a RandomForestClassifier from logs containing state vectors and priority labels."""
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
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None, feature_dim: int = FEATURE_DIM):
        self.lr = lr
        self.weights_path = weights_path or Path("./priority_q_weights.json")
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, 3))  # 3 actions
        self._load_state()

    def _load_state(self):
        if self.weights_path.exists():
            try:
                with open(self.weights_path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                if self.weights.shape[0] != self.feature_dim:
                    logger.warning(f"Loaded Q‑weights have shape {self.weights.shape}, expected ({self.feature_dim},3). Reinitializing.")
                    self.weights = np.zeros((self.feature_dim, 3))
                logger.info(f"Loaded Q‑teacher weights from {self.weights_path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        with open(self.weights_path, 'w') as f:
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


class RLHFPriorityTeacher(Teacher):
    """
    Teacher that incorporates human feedback to adjust priority probabilities.
    High human feedback prefers 'balanced', low prefers 'green' (sustainability).
    """
    def __init__(self, feedback_weight: float = 0.3):
        self.feedback_weight = feedback_weight

    def predict(self, state: WorkloadState) -> np.ndarray:
        probs = np.ones(3) / 3
        if state.human_feedback_score > 0.7:
            probs[2] += 0.2  # balanced
        elif state.human_feedback_score < 0.3:
            probs[1] += 0.2  # green
        else:
            probs[0] += 0.1  # accuracy
            probs[1] += 0.1
            probs[2] += 0.1
        return probs / probs.sum()

    def confidence(self, state: WorkloadState) -> float:
        return min(0.8, abs(state.human_feedback_score - 0.5) * 2 + 0.3)


class DistillationStudent:
    def __init__(self, feature_dim: int = FEATURE_DIM, n_classes: int = 3, lr: float = 0.01):
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


class MoEGatingNetwork:
    """
    Learnable gating network for combining teacher outputs (Mixture‑of‑Experts).
    Outputs a weight vector for each teacher based on the state.
    """
    def __init__(self, feature_dim: int = FEATURE_DIM, n_experts: int = 4, lr: float = 0.01):
        self.feature_dim = feature_dim
        self.n_experts = n_experts
        self.lr = lr
        self.weights = np.random.randn(feature_dim, n_experts) * 0.01
        self.bias = np.zeros(n_experts)

    def forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.weights + self.bias
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / exp_logits.sum()

    def update(self, state_vec: np.ndarray, teacher_probs: np.ndarray, student_probs: np.ndarray):
        """
        Update gating network to reduce difference between combined teacher output
        and student output (distillation signal).
        """
        gate_weights = self.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_probs, axis=0)
        error = combined - student_probs
        grad_gate = np.dot(teacher_probs, error)  # shape (n_experts,)
        self.weights -= self.lr * np.outer(state_vec, grad_gate)
        self.bias -= self.lr * grad_gate


class DistillationPriorityOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for priority selection.
    Priorities: accuracy, green, balanced.
    Enhanced with MoE gating and RLHF teacher.
    """
    PRIORITIES = ['accuracy', 'green', 'balanced']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.feature_dim = FEATURE_DIM
        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            lr=config.get('distillation_learning_rate', 0.01)
        )
        # Teachers (including RLHF)
        self.teachers: List[Teacher] = [
            PriorityRuleBasedTeacher(),
            PriorityHistoricalMLTeacher(model_path=config.get('historical_model_path')),
            PriorityStatefulQTeacher(
                lr=config.get('q_learning_rate', 0.1),
                weights_path=config.get('q_weights_path'),
                feature_dim=self.feature_dim
            ),
            RLHFPriorityTeacher(feedback_weight=config.get('rlhf_feedback_weight', 0.3))  # NEW
        ]
        self.n_teachers = len(self.teachers)
        # MoE gating network
        self.gating = MoEGatingNetwork(feature_dim=self.feature_dim,
                                       n_experts=self.n_teachers,
                                       lr=config.get('gating_learning_rate', 0.005))
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
        self.distill_weight = config.get('distillation_weight', 0.7)
        self.rl_weight = config.get('rl_weight', 0.3)
        self.batch_update_size = config.get('batch_update_size', 8)

    def _compute_teacher_probs(self, state: WorkloadState) -> Tuple[np.ndarray, np.ndarray]:
        """
        Return (teacher_probs, gate_weights) using the MoE gating network.
        teacher_probs is the weighted combination of individual teacher outputs.
        gate_weights are the learned gate values for each teacher.
        """
        state_vec = state.to_feature_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher.predict(state)
            if len(prob) != 3:
                if len(prob) < 3:
                    prob = np.pad(prob, (0, 3 - len(prob)), 'constant')
                else:
                    prob = prob[:3]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)  # shape (n_teachers, 3)
        gate_weights = self.gating.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        combined = combined / combined.sum()
        return combined, gate_weights

    async def select_priority(self, state: WorkloadState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = len(self.PRIORITIES)

        teacher_probs, gate_weights = self._compute_teacher_probs(state)

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
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= self.batch_update_size:
            batch = self.replay_buffer.sample(self.batch_update_size)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                # Update student
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i],
                                    distill_weight=self.distill_weight, rl_weight=self.rl_weight)
                # Update gating network using current student output
                student_out = self.student.predict_proba(states[i])
                self.gating.update(states[i], teacher_probs_batch[i], student_out)

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'n_teachers': self.n_teachers
        }


class EvolutionaryPriorityOptimizer:
    """
    Simple evolutionary algorithm for priority selection (bio‑inspired).
    Maintains a population of candidate priority distributions and evolves them.
    """
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.7, elitism: int = 2):
        self.population_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elitism = elitism
        self.population = [np.random.dirichlet(np.ones(3)) for _ in range(population_size)]
        self.fitness = np.zeros(population_size)
        self.best_individual = self.population[0]
        self.best_index = 0

    def select_priority(self, state_vec: np.ndarray) -> Tuple[int, np.ndarray]:
        """Return action index and the probabilities of the best individual."""
        best_probs = self.best_individual
        action_idx = np.argmax(best_probs)
        return action_idx, best_probs

    def update_fitness(self, reward: float, index: int = 0):
        """Update fitness of the best individual (simplified)."""
        self.fitness[index] = reward
        best_idx = np.argmax(self.fitness)
        self.best_individual = self.population[best_idx]
        self.best_index = best_idx
        self._evolve()

    def _evolve(self):
        sorted_indices = np.argsort(self.fitness)[::-1]
        new_population = [self.population[i] for i in sorted_indices[:self.elitism]]
        while len(new_population) < self.population_size:
            parent1 = self._tournament_selection()
            parent2 = self._tournament_selection()
            if random.random() < self.crossover_rate:
                child = self._crossover(parent1, parent2)
            else:
                child = parent1.copy()
            child = self._mutate(child)
            new_population.append(child)
        self.population = new_population
        self.fitness = np.zeros(self.population_size)

    def _tournament_selection(self, k=3):
        candidates = random.sample(range(self.population_size), k)
        best = max(candidates, key=lambda i: self.fitness[i])
        return self.population[best]

    def _crossover(self, p1, p2):
        alpha = random.random()
        return alpha * p1 + (1 - alpha) * p2

    def _mutate(self, individual):
        mutated = individual.copy()
        for i in range(len(mutated)):
            if random.random() < self.mutation_rate:
                mutated[i] += random.gauss(0, 0.1)
                mutated[i] = max(0.01, mutated[i])
        return mutated / mutated.sum()


# ============================================================================
# Enhanced WorkloadDescriptor
# ============================================================================

class WorkloadDescriptor(BaseModel):
    """Descriptor for a workload/task with adaptive priority selection."""
    model_config = ConfigDict(extra='allow', arbitrary_types_allowed=True)

    # Core identification
    task_id: Optional[str] = Field(None, description="Unique task identifier")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")

    # Task characteristics
    task_type: TaskType = Field(..., description="Type of the workload")
    tokens: int = Field(..., ge=1, description="Number of tokens")
    latency_target: float = Field(..., gt=0, description="Target latency in ms")
    deadline: Optional[datetime] = Field(None, description="Hard deadline")
    urgency: Urgency = Field(Urgency.MEDIUM, description="Urgency level")

    # Sustainability & cost
    sector_emission_factor: Optional[float] = Field(None, ge=0, description="kg CO₂ per $ revenue")
    estimated_energy_joules: Optional[float] = Field(None, ge=0, description="Estimated energy (J)")
    estimated_carbon_kg: Optional[float] = Field(None, ge=0, description="Estimated carbon (kg CO₂)")
    helium_units: Optional[float] = Field(None, ge=0, description="Estimated helium units")

    # Resource sizing
    data_size_bytes: Optional[int] = Field(None, ge=0, description="Input data size (bytes)")
    model_size_bytes: Optional[int] = Field(None, ge=0, description="Model size (bytes)")

    # Multi‑tenant & user
    user_id: Optional[str] = Field(None, description="User identifier")
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")

    # Bio‑inspired mode
    bio_mode: BioMode = Field(BioMode.NONE, description="Bio‑inspired operation mode")

    # ----- NEW FIELDS for LIMIT Graph, RLHF, evolutionary -----
    # LIMIT Graph integration
    graph_id: Optional[str] = Field(None, description="Identifier of the LIMIT graph node")
    graph_embedding: Optional[List[float]] = Field(None, description="Embedding vector in LIMIT graph")
    graph_metrics: Optional[Dict[str, float]] = Field(None, description="Metrics from LIMIT graph (e.g., centrality)")

    # RLHF
    human_feedback_score: float = Field(0.5, ge=0.0, le=1.0, description="Score from human feedback (1=high preference for balanced)")

    # Evolutionary optimisation flags
    use_evolutionary: bool = Field(False, description="Whether to use evolutionary optimisation for priority")
    evolutionary_population_size: int = Field(20, ge=2)
    evolutionary_mutation_rate: float = Field(0.1, ge=0.0, le=1.0)
    evolutionary_crossover_rate: float = Field(0.7, ge=0.0, le=1.0)

    # Adaptive priority
    adaptive_priority: Priority = Field(Priority.BALANCED, description="Current adaptive priority")
    performance_history: List[Dict[str, Any]] = Field(default_factory=list, description="Recent scheduling outcomes")
    max_history_length: int = Field(100, ge=1, description="Maximum history entries")

    # Schema version & extensibility
    version: str = Field("2.2.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    # Distillation optimizer and evolutionary optimizer (not serialized)
    _priority_optimizer: Optional[DistillationPriorityOptimizer] = PrivateAttr(default=None)
    _evolutionary_optimizer: Optional[EvolutionaryPriorityOptimizer] = PrivateAttr(default=None)
    _last_decision: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _lock: Any = PrivateAttr(default=None)  # asyncio.Lock

    # ------------------------------------------------------------------
    # Validators
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
    # Helper methods
    # ------------------------------------------------------------------
    def compute_energy_cost(self, energy_per_token: float) -> float:
        return energy_per_token * self.tokens

    def compute_carbon_cost(self, carbon_intensity_kg_per_kwh: float) -> float:
        energy_kwh = self.tokens * 0.00001  # placeholder
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
    # Distillation methods
    # ------------------------------------------------------------------
    def _ensure_optimizer(self):
        if self._priority_optimizer is None:
            workload_id = self.task_id or "unknown"
            self._priority_optimizer = DistillationPriorityOptimizer({
                'distillation_epsilon': self.metadata.get('distillation_epsilon', 0.1),
                'distillation_train_every': self.metadata.get('distillation_train_every', 10),
                'distillation_replay_size': self.metadata.get('distillation_replay_size', 2000),
                'distillation_learning_rate': self.metadata.get('distillation_learning_rate', 0.01),
                'historical_model_path': self.metadata.get('historical_model_path'),
                'q_learning_rate': self.metadata.get('q_learning_rate', 0.1),
                'q_weights_path': self.metadata.get('q_weights_path', Path(f"./priority_q_weights_{workload_id}.json")),
                'distillation_weight': self.metadata.get('distillation_weight', 0.7),
                'rl_weight': self.metadata.get('rl_weight', 0.3),
                'batch_update_size': self.metadata.get('batch_update_size', 8),
                'gating_learning_rate': self.metadata.get('gating_learning_rate', 0.005),
                'rlhf_feedback_weight': self.metadata.get('rlhf_feedback_weight', 0.3),
                'feature_dim': FEATURE_DIM,
            })
        if self.use_evolutionary and self._evolutionary_optimizer is None:
            self._evolutionary_optimizer = EvolutionaryPriorityOptimizer(
                population_size=self.evolutionary_population_size,
                mutation_rate=self.evolutionary_mutation_rate,
                crossover_rate=self.evolutionary_crossover_rate
            )

    async def _get_lock(self):
        if self._lock is None:
            import asyncio
            self._lock = asyncio.Lock()
        return self._lock

    async def select_priority(
        self,
        exploration: bool = True,
        latency_achieved_ms: Optional[float] = None,
        carbon_saved_kg: Optional[float] = None,
        energy_used_joules: Optional[float] = None,
    ) -> Priority:
        """Select the best priority using distillation, optionally update immediately."""
        lock = await self._get_lock()
        async with lock:
            self._ensure_optimizer()
            state = self._build_state()
            priority, action_idx, state_vec, teacher_probs = await self._priority_optimizer.select_priority(
                state, exploration=exploration
            )

            # Evolutionary blending if enabled
            if self.use_evolutionary and self._evolutionary_optimizer is not None:
                evo_action, evo_probs = self._evolutionary_optimizer.select_priority(state_vec)
                combined_probs = 0.7 * teacher_probs + 0.3 * evo_probs
                action_idx = np.argmax(combined_probs)
                priority = self._priority_optimizer.PRIORITIES[action_idx]

            self._last_decision = {
                'state_vec': state_vec,
                'action_idx': action_idx,
                'teacher_probs': teacher_probs,
            }
            self.adaptive_priority = Priority(priority)

            if latency_achieved_ms is not None and carbon_saved_kg is not None and energy_used_joules is not None:
                await self._record_outcome_locked(latency_achieved_ms, carbon_saved_kg, energy_used_joules)

            return self.adaptive_priority

    async def record_outcome(self, latency_achieved_ms: float, carbon_saved_kg: float, energy_used_joules: float):
        """Public method to record outcome (acquires lock)."""
        lock = await self._get_lock()
        async with lock:
            await self._record_outcome_locked(latency_achieved_ms, carbon_saved_kg, energy_used_joules)

    async def _record_outcome_locked(self, latency_achieved_ms: float, carbon_saved_kg: float, energy_used_joules: float):
        """Record outcome and update the distillation agent (assumes lock held)."""
        # MODP: use configurable objective weights from metadata
        latency_weight = self.metadata.get('latency_weight', 0.4)
        carbon_weight = self.metadata.get('carbon_weight', 0.3)
        energy_weight = self.metadata.get('energy_weight', 0.3)
        total_weight = latency_weight + carbon_weight + energy_weight
        if total_weight <= 0:
            total_weight = 1.0
        latency_weight /= total_weight
        carbon_weight /= total_weight
        energy_weight /= total_weight

        latency_score = 1.0 - min(1.0, abs(latency_achieved_ms - self.latency_target) / self.latency_target)
        carbon_norm = min(1.0, carbon_saved_kg / 0.1)
        energy_norm = 1.0 - min(1.0, energy_used_joules / (self.estimated_energy_joules or 0.1))
        objective_reward = latency_weight * latency_score + carbon_weight * carbon_norm + energy_weight * energy_norm

        # RLHF: incorporate human feedback (10% weight)
        human_alignment = self.human_feedback_score
        reward = 0.9 * objective_reward + 0.1 * human_alignment
        reward = max(0.0, min(1.0, reward))

        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'priority': self.adaptive_priority.value,
            'reward': reward,
            'latency_achieved_ms': latency_achieved_ms,
            'carbon_saved_kg': carbon_saved_kg,
            'energy_used_joules': energy_used_joules,
            'human_feedback_score': self.human_feedback_score,
        }

        if self._last_decision is not None:
            state_vec = self._last_decision['state_vec']
            action_idx = self._last_decision['action_idx']
            teacher_probs = self._last_decision['teacher_probs']

            next_state = self._build_state()
            next_state_vec = next_state.to_feature_vector()

            self._ensure_optimizer()
            await self._priority_optimizer.update(
                state_vec,
                action_idx,
                reward,
                next_state_vec,
                teacher_probs
            )

            # Evolutionary update
            if self.use_evolutionary and self._evolutionary_optimizer is not None:
                self._evolutionary_optimizer.update_fitness(reward, index=0)

            entry['state_vec'] = ','.join(map(str, state_vec))
            self._last_decision = None
        else:
            logger.debug("No decision context available for update.")

        self.performance_history.append(entry)
        if len(self.performance_history) > self.max_history_length:
            self.performance_history = self.performance_history[-self.max_history_length:]

        # Persist logs
        log_path = Path(f"./workload_{self.task_id or 'unknown'}_logs.csv")
        df = pd.DataFrame(self.performance_history)
        df.to_csv(log_path, index=False)

        json_path = Path(f"./workload_{self.task_id or 'unknown'}_logs.json")
        with open(json_path, 'w') as f:
            json.dump(self.performance_history, f, indent=2)

        # Emit FeedbackEvent
        if FeedbackEvent is not None:
            try:
                event = FeedbackEvent(
                    source="workload_descriptor",
                    feedback_type="scheduling",
                    task_id=self.task_id or "",
                    context={"workload_id": self.task_id},
                    action={"selected_action": self.adaptive_priority.value,
                            "selected_rank": 1,
                            "confidence_score": self.metadata.get('confidence', 0.5)},
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

    def _build_state(self) -> WorkloadState:
        """Build state from current workload metrics and history (now includes human feedback and graph health)."""
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

        # Graph health factor (if graph_metrics present)
        graph_health = 0.5
        if self.graph_metrics:
            graph_health = self.graph_metrics.get('centrality', 0.5)

        # Incorporate graph health into state? Could be added as a separate feature,
        # but to keep feature dim consistent we just use it to adjust avg_reward slightly.
        # We'll multiply avg_reward by a factor based on graph_health.
        adjusted_avg_reward = avg_reward * (0.8 + 0.2 * graph_health)

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
            avg_reward=adjusted_avg_reward,
            human_feedback_score=self.human_feedback_score,
        )

    # ------------------------------------------------------------------
    # Offline training (class method)
    # ------------------------------------------------------------------
    @classmethod
    def train_historical_model(
        cls,
        log_paths: List[Path],
        model_path: Path = Path("./priority_historical_model.pkl"),
        state_col: str = 'state_vec',
        label_col: str = 'priority'
    ):
        """Train a RandomForestClassifier from workload scheduling logs."""
        return PriorityHistoricalMLTeacher.train_from_logs(log_paths, model_path, state_col, label_col)


# ============================================================================
# Convenience factory
# ============================================================================

def create_workload_descriptor(
    task_type: TaskType,
    tokens: int,
    latency_target: float,
    **kwargs
) -> WorkloadDescriptor:
    """Factory function to create a WorkloadDescriptor with sensible defaults."""
    return WorkloadDescriptor(
        task_type=task_type,
        tokens=tokens,
        latency_target=latency_target,
        **kwargs
    )


# ============================================================================
# UNIT TESTS (updated & extended)
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
            'feature_dim': FEATURE_DIM,
            'gating_learning_rate': 0.005,
            'rlhf_feedback_weight': 0.3,
        }
        self.optimizer = DistillationPriorityOptimizer(self.config)

    def test_state_feature_vector(self):
        state = WorkloadState(tokens=512, latency_target=200, urgency=1,
                              task_type_inference=1.0, estimated_energy=0.05,
                              estimated_carbon=0.0002, helium_units=0.001,
                              human_feedback_score=0.7)
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), FEATURE_DIM)

    def test_rule_based_teacher(self):
        teacher = PriorityRuleBasedTeacher()
        state = WorkloadState(tokens=512, latency_target=200, urgency=2,
                              task_type_inference=1.0, estimated_energy=0.05,
                              estimated_carbon=0.0002, helium_units=0.001,
                              human_feedback_score=0.5)
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])

    def test_rlhf_teacher(self):
        teacher = RLHFPriorityTeacher()
        state_high = WorkloadState(tokens=512, latency_target=200, urgency=1,
                                   task_type_inference=1.0, estimated_energy=0.05,
                                   estimated_carbon=0.0002, helium_units=0.001,
                                   human_feedback_score=0.9)
        probs_high = teacher.predict(state_high)
        self.assertGreater(probs_high[2], probs_high[0])

    async def test_select_priority(self):
        state = WorkloadState(tokens=512, latency_target=200, urgency=1,
                              task_type_inference=1.0, estimated_energy=0.05,
                              estimated_carbon=0.0002, helium_units=0.001,
                              human_feedback_score=0.5)
        priority, idx, state_vec, teacher_probs = await self.optimizer.select_priority(state, exploration=False)
        self.assertIn(priority, self.optimizer.PRIORITIES)
        self.assertEqual(len(state_vec), FEATURE_DIM)
        self.assertEqual(len(teacher_probs), 3)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(FEATURE_DIM)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(3)/3)
        self.assertEqual(len(buffer), 1)

    def test_moe_gating(self):
        gate = MoEGatingNetwork(feature_dim=FEATURE_DIM, n_experts=4)
        state_vec = np.random.randn(FEATURE_DIM)
        weights = gate.forward(state_vec)
        self.assertEqual(len(weights), 4)
        self.assertAlmostEqual(sum(weights), 1.0)

    def test_evolutionary_optimizer(self):
        evo = EvolutionaryPriorityOptimizer(population_size=10)
        state_vec = np.random.randn(FEATURE_DIM)
        action, probs = evo.select_priority(state_vec)
        self.assertIn(action, range(3))
        self.assertEqual(len(probs), 3)
        evo.update_fitness(0.8, index=0)
        self.assertIsNotNone(evo.best_individual)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def demo():
        wl = WorkloadDescriptor(
            task_id="task-001",
            task_type=TaskType.INFERENCE,
            tokens=1024,
            latency_target=150.0,
            urgency=Urgency.HIGH,
            bio_mode=BioMode.PHOTOSYNTHETIC,
            estimated_energy_joules=0.1,
            estimated_carbon_kg=0.0005,
            user_id="user-123",
            metadata={"region": "eu-west", "latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2},
            # New fields
            graph_id="graph-task-001",
            graph_embedding=[0.1, 0.2, 0.3],
            graph_metrics={"centrality": 0.75},
            human_feedback_score=0.6,
            use_evolutionary=False,
        )

        priority = await wl.select_priority(exploration=True)
        print(f"Selected priority: {priority}")

        await wl.record_outcome(latency_achieved_ms=120, carbon_saved_kg=0.02, energy_used_joules=0.08)

        if wl._priority_optimizer:
            stats = wl._priority_optimizer.get_stats()
            print(f"Distillation stats: {stats}")

        print("Workload descriptor demo complete.")

    asyncio.run(demo())
