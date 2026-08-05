# src/enhancements/schemas/node_descriptor.py
"""
Enhanced Node Descriptor v2.1.0
================================
Defines the structure of a compute node with adaptive routing strategy selection
via Multi‑Teacher On‑Policy Distillation.

Features:
- Expanded node types as Enum.
- Fields for performance, cooling, location, cost, health.
- Sustainability metrics: carbon, helium, renewable, material.
- Helper methods for cost estimation and health evaluation.
- Pydantic validation with custom validators.
- NEW: Adaptive routing strategy selection (carbon_first, latency_first, cost_first, balanced, adaptive).
- NEW: Online learning from routing outcomes.
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
# Enums (expanded)
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

# NEW: Routing strategy enum
class RoutingStrategy(str, Enum):
    CARBON_FIRST = "carbon_first"
    LATENCY_FIRST = "latency_first"
    COST_FIRST = "cost_first"
    BALANCED = "balanced"
    ADAPTIVE = "adaptive"

# ============================================================================
# DISTILLATION COMPONENTS FOR ROUTING STRATEGY SELECTION
# ============================================================================

@dataclass
class NodeState:
    """State for the distillation agent."""
    # Sustainability
    carbon_intensity: float
    renewable_fraction: float
    helium_connectivity: float
    # Performance
    uptime: float
    efficiency_score: float
    health_score: float
    # Cost
    cost_per_hour: float
    energy_per_token: float
    # Historical performance (from logs)
    recent_success_rate: float
    avg_reward: float

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
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
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
    """Rule‑based expert: uses original heuristics."""
    STRATEGIES = ['carbon_first', 'latency_first', 'cost_first', 'balanced', 'adaptive']

    def predict(self, state: NodeState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.carbon_intensity > 0.5:
            probs[0] = 0.8  # carbon_first
        elif state.health_score < 0.6:
            probs[1] = 0.7  # latency_first (avoid degraded nodes)
        elif state.cost_per_hour > 5.0:
            probs[2] = 0.6  # cost_first
        elif state.renewable_fraction > 0.7:
            probs[3] = 0.6  # balanced (already green)
        else:
            probs[4] = 0.6  # adaptive
        return probs / probs.sum()

    def confidence(self, state: NodeState) -> float:
        if state.carbon_intensity > 0.5:
            return 0.6
        return 0.4


class RoutingHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past routing logs."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path("./routing_historical_model.pkl")
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
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: NodeState) -> float:
        return 0.7 if self.model is not None else 0.0


class RoutingStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((10, 5))  # 10 features, 5 actions
        self._load_state()

    def _load_state(self):
        path = Path("./routing_q_weights.json")
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path("./routing_q_weights.json")
        with open(path, 'w') as f:
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


class DistillationStudent:
    def __init__(self, feature_dim: int = 10, n_classes: int = 5, lr: float = 0.01):
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


class DistillationRoutingOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for routing strategy selection.
    Strategies: carbon_first, latency_first, cost_first, balanced, adaptive.
    """
    STRATEGIES = ['carbon_first', 'latency_first', 'cost_first', 'balanced', 'adaptive']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            RoutingRuleBasedTeacher(),
            RoutingHistoricalMLTeacher(),
            RoutingStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: NodeState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 5

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

        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

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
# Enhanced NodeDescriptor (with Distillation)
# ============================================================================

class NodeDescriptor(BaseModel):
    """
    Descriptor for a compute node, now with adaptive routing strategy selection.
    """

    # Core identification
    id: str = Field(..., description="Unique node identifier")

    # Node type and location
    type: NodeType = Field(..., description="Type of the node")
    region: str = Field(..., description="Geographic region")
    location_lat: Optional[float] = Field(None, description="Latitude of the node")
    location_lon: Optional[float] = Field(None, description="Longitude of the node")
    availability_zone: Optional[str] = Field(None, description="Cloud availability zone")

    # Sustainability metrics
    region_carbon_intensity: float = Field(..., ge=0, description="kg CO₂/kWh")
    carbon_intensity_source: Optional[str] = Field(None, description="Source of carbon intensity data")
    energy_per_token: float = Field(..., gt=0, description="Joules per token")
    helium_connectivity_score: float = Field(0.5, ge=0, le=1, description="Connectivity score for helium-based nodes")
    material_footprint_id: Optional[str] = Field(None, description="Reference to material footprint catalog")
    renewable_fraction: float = Field(0.0, ge=0, le=1, description="Fraction of energy from renewables")
    efficiency_score: Optional[float] = Field(None, ge=0, le=1, description="Overall sustainability efficiency score")

    # Performance and resources
    flops: Optional[float] = Field(None, gt=0, description="Estimated FLOPs per second")
    memory_gb: Optional[float] = Field(None, gt=0, description="Available memory in GB")
    storage_gb: Optional[float] = Field(None, gt=0, description="Available storage in GB")

    # Cooling and network
    cooling_type: CoolingType = Field(CoolingType.AIR, description="Cooling system type")
    network_latency_ms: Optional[float] = Field(None, gt=0, description="Average network latency in milliseconds")

    # Operational metrics
    uptime: float = Field(1.0, ge=0, le=1, description="Fraction of time the node is available")
    cost_per_hour_usd: Optional[float] = Field(None, ge=0, description="Cost per hour in USD")
    maintenance_status: MaintenanceStatus = Field(MaintenanceStatus.OPERATIONAL, description="Maintenance status")
    last_updated: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of last update")

    # Hardware information
    hardware_model: Optional[str] = Field(None, description="Hardware model identifier")
    manufacturer: Optional[str] = Field(None, description="Hardware manufacturer")

    # NEW: Routing strategy and learning
    routing_strategy: RoutingStrategy = Field(RoutingStrategy.BALANCED, description="Current routing strategy")
    performance_history: deque = Field(default_factory=lambda: deque(maxlen=100), description="Recent routing outcomes")

    # Schema version & extensibility
    version: str = Field("2.1.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    # Distillation optimizer (per node instance)
    _routing_optimizer: Optional[DistillationRoutingOptimizer] = None
    _state_vec: Optional[np.ndarray] = None
    _action_idx: Optional[int] = None
    _teacher_probs: Optional[np.ndarray] = None

    # ------------------------------------------------------------------
    # Validators (unchanged)
    # ------------------------------------------------------------------
    @field_validator('region_carbon_intensity')
    @classmethod
    def validate_carbon_intensity(cls, v: float) -> float:
        if v < 0:
            raise ValueError("region_carbon_intensity must be non‑negative")
        return v

    @field_validator('energy_per_token')
    @classmethod
    def validate_energy_per_token(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("energy_per_token must be positive")
        return v

    @field_validator('location_lat')
    @classmethod
    def validate_latitude(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-90 <= v <= 90):
            raise ValueError("latitude must be between -90 and 90")
        return v

    @field_validator('location_lon')
    @classmethod
    def validate_longitude(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-180 <= v <= 180):
            raise ValueError("longitude must be between -180 and 180")
        return v

    # ------------------------------------------------------------------
    # Helper methods (existing, plus new ones)
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

    # ------------------------------------------------------------------
    # NEW: Distillation methods
    # ------------------------------------------------------------------
    async def select_routing_strategy(
        self,
        exploration: bool = True,
        carbon_saved: Optional[float] = None,
        latency_ms: Optional[float] = None,
        cost_usd: Optional[float] = None,
    ) -> RoutingStrategy:
        """
        Select the best routing strategy for this node using distillation.
        Optionally provide outcome metrics to update the agent immediately.
        """
        # Initialize optimizer if not already created
        if self._routing_optimizer is None:
            self._routing_optimizer = DistillationRoutingOptimizer({
                'distillation_epsilon': self.metadata.get('distillation_epsilon', 0.1),
                'distillation_train_every': self.metadata.get('distillation_train_every', 10),
                'distillation_replay_size': self.metadata.get('distillation_replay_size', 2000),
                'distillation_learning_rate': self.metadata.get('distillation_learning_rate', 0.01),
            })

        # Build state
        state = self._build_state()
        strategy, action_idx, state_vec, teacher_probs = await self._routing_optimizer.select_strategy(state, exploration=exploration)
        self._state_vec = state_vec
        self._action_idx = action_idx
        self._teacher_probs = teacher_probs

        # Update the node's strategy
        self.routing_strategy = RoutingStrategy(strategy)

        # If outcome metrics are provided, update the agent immediately
        if carbon_saved is not None and latency_ms is not None and cost_usd is not None:
            await self.record_outcome(carbon_saved, latency_ms, cost_usd)

        return self.routing_strategy

    async def record_outcome(self, carbon_saved_kg: float, latency_ms: float, cost_usd: float):
        """
        Record the outcome of a routing decision and update the distillation agent.
        """
        # Compute reward
        carbon_norm = min(1.0, carbon_saved_kg / 0.1)
        latency_norm = 1.0 - min(1.0, latency_ms / 500.0)
        cost_norm = 1.0 - min(1.0, cost_usd / 10.0)
        reward = 0.5 * carbon_norm + 0.3 * latency_norm + 0.2 * cost_norm
        reward = max(0.0, min(1.0, reward))

        # Store in history
        self.performance_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'strategy': self.routing_strategy.value,
            'reward': reward,
            'carbon_saved_kg': carbon_saved_kg,
            'latency_ms': latency_ms,
            'cost_usd': cost_usd,
        })

        # Update agent if we have a recorded state
        if self._state_vec is not None and self._action_idx is not None:
            # Next state (same for simplicity)
            next_state = self._build_state()
            next_state_vec = next_state.to_feature_vector()
            await self._routing_optimizer.update(
                self._state_vec,
                self._action_idx,
                reward,
                next_state_vec,
                self._teacher_probs
            )

        # Persist the update (Q-weights saved by the teacher itself)
        # Save performance history to CSV
        log_path = Path(f"./node_{self.id}_routing_logs.csv")
        df = pd.DataFrame(self.performance_history)
        df.to_csv(log_path, index=False)

    def _build_state(self) -> NodeState:
        """Build state from current node metrics and history."""
        # Compute recent success rate from performance history
        if self.performance_history:
            recent = list(self.performance_history)[-20:]
            success_rate = sum(1 for r in recent if r['reward'] > 0.5) / max(len(recent), 1)
            avg_reward = np.mean([r['reward'] for r in recent]) if recent else 0.0
        else:
            success_rate = 0.5
            avg_reward = 0.5

        return NodeState(
            carbon_intensity=self.region_carbon_intensity,
            renewable_fraction=self.renewable_fraction,
            helium_connectivity=self.helium_connectivity_score,
            uptime=self.uptime,
            efficiency_score=self.efficiency_score or 0.5,
            health_score=self.get_health_score(),
            cost_per_hour=self.cost_per_hour_usd or 1.0,
            energy_per_token=self.energy_per_token,
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
        model_path: Path = Path("./routing_historical_model.pkl")
    ):
        """
        Train a RandomForestClassifier from multiple node routing logs.
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
                "id": "node-123",
                "type": "edge",
                "region": "us-east",
                "location_lat": 40.7128,
                "location_lon": -74.0060,
                "availability_zone": "us-east-1a",
                "region_carbon_intensity": 0.42,
                "carbon_intensity_source": "OS-Climate",
                "energy_per_token": 0.00005,
                "helium_connectivity_score": 0.92,
                "material_footprint_id": "gpu-a100",
                "renewable_fraction": 0.3,
                "efficiency_score": 0.85,
                "flops": 1.5e12,
                "memory_gb": 64,
                "storage_gb": 1024,
                "cooling_type": "liquid",
                "network_latency_ms": 50.0,
                "uptime": 0.99,
                "cost_per_hour_usd": 2.50,
                "maintenance_status": "operational",
                "hardware_model": "A100",
                "manufacturer": "NVIDIA",
                "routing_strategy": "balanced",
                "version": "2.1.0",
                "metadata": {
                    "owner": "team-alpha",
                    "environment": "production",
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

def create_node_descriptor(
    id: str,
    node_type: NodeType,
    region: str,
    region_carbon_intensity: float,
    energy_per_token: float,
    **kwargs
) -> NodeDescriptor:
    """
    Factory function to create a NodeDescriptor with sensible defaults.
    """
    return NodeDescriptor(
        id=id,
        type=node_type,
        region=region,
        region_carbon_intensity=region_carbon_intensity,
        energy_per_token=energy_per_token,
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
        self.optimizer = DistillationRoutingOptimizer(self.config)

    def test_state_feature_vector(self):
        state = NodeState(
            carbon_intensity=0.4,
            renewable_fraction=0.3,
            helium_connectivity=0.9,
            uptime=0.99,
            efficiency_score=0.85,
            health_score=0.8,
            cost_per_hour=2.5,
            energy_per_token=0.00005,
            recent_success_rate=0.7,
            avg_reward=0.6,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 10)

    def test_rule_based_teacher(self):
        teacher = RoutingRuleBasedTeacher()
        state = NodeState(
            carbon_intensity=0.6,
            renewable_fraction=0.3,
            helium_connectivity=0.9,
            uptime=0.99,
            efficiency_score=0.85,
            health_score=0.8,
            cost_per_hour=2.5,
            energy_per_token=0.00005,
            recent_success_rate=0.7,
            avg_reward=0.6,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # carbon_first should be highest

    async def test_select_strategy(self):
        state = NodeState(
            carbon_intensity=0.4,
            renewable_fraction=0.3,
            helium_connectivity=0.9,
            uptime=0.99,
            efficiency_score=0.85,
            health_score=0.8,
            cost_per_hour=2.5,
            energy_per_token=0.00005,
            recent_success_rate=0.7,
            avg_reward=0.6,
        )
        strategy, idx, state_vec, teacher_probs = await self.optimizer.select_strategy(state, exploration=False)
        self.assertIn(strategy, self.optimizer.STRATEGIES)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(10)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(5)/5)
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
        # Create a node
        node = NodeDescriptor(
            id="node-001",
            type=NodeType.EDGE,
            region="us-east",
            region_carbon_intensity=0.42,
            energy_per_token=0.00005,
            helium_connectivity_score=0.92,
            uptime=0.98,
            renewable_fraction=0.4,
            cooling_type=CoolingType.LIQUID,
            hardware_model="A100",
            metadata={"rack": "R12"}
        )

        # Select strategy (simulate a routing decision)
        strategy = await node.select_routing_strategy(exploration=True)
        print(f"Selected strategy: {strategy}")

        # Record outcome (simulate)
        await node.record_outcome(carbon_saved_kg=0.05, latency_ms=120, cost_usd=3.50)

        # Get stats (if optimizer exists)
        if node._routing_optimizer:
            stats = node._routing_optimizer.get_stats()
            print(f"Distillation stats: {stats}")

        print("Node descriptor demo complete.")

    asyncio.run(demo())
