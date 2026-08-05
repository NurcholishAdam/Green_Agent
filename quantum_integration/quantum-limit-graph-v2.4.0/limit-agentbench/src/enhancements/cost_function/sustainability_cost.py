# src/enhancements/cost_function/sustainability_cost.py
"""
Enhanced Sustainability Cost Function v2.2.0
=============================================
Multi‑objective sustainability cost function with Multi‑Teacher On‑Policy Distillation
for adaptive weight selection.

ENHANCEMENTS OVER v2.1.0:
- Replaced static weight adjustments with a multi‑teacher distillation agent.
- Agent selects among 5 strategies (standard, carbon_focus, energy_focus, helium_focus, adaptive).
- Learns from the effectiveness of past decisions via reward (cost reduction vs baseline).
- State includes carbon intensity, node health, workload, anomaly severity, historical cost trend.
- New configuration parameters for distillation (epsilon, train_every, replay_size, learning_rate).
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, timedelta
from collections import OrderedDict, deque
import numpy as np
import random
from abc import ABC, abstractmethod
from pathlib import Path

# ---------- Local imports ----------
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector
from ..expert_registry import ExpertProfile  # optional

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
        material_embodied_norm: float = Field(200.0, gt=0)  # kg CO2 equivalent
        material_rare_earth_norm: float = Field(0.01, gt=0)  # kg
        material_max_composite: float = Field(1.0, gt=0)
        # Carbon caching
        carbon_cache_ttl_seconds: int = Field(300, ge=0)
        carbon_cache_max_size: int = Field(100, ge=1)
        # Integration flags
        use_adaptive_weights: bool = Field(False)
        integrate_anomaly_detection: bool = Field(False)
        integrate_predictive_maintenance: bool = Field(False)

        # NEW: Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

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
    # Fallback dict
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
        # Distillation defaults
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
    }


# ============================================================================
# NEW: DISTILLATION COMPONENTS FOR WEIGHT SELECTION
# ============================================================================

@dataclass
class CostOptimizationState:
    """State for the distillation agent."""
    # Environmental
    carbon_intensity: float
    node_health: float  # from predictive maintenance (0-1)
    workload_tokens: float
    latency_target: float
    # Anomaly
    anomaly_severity: float
    # Historical
    avg_cost_trend: float  # positive = increasing cost
    cost_variance: float
    # Current weights (sum to 1)
    weight_carbon: float
    weight_energy: float
    weight_helium: float
    # Time
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


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CostOptimizationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: CostOptimizationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class CostRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses heuristics."""
    ACTION_SPACE = ['standard', 'carbon_focus', 'energy_focus', 'helium_focus', 'adaptive']

    def predict(self, state: CostOptimizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.anomaly_severity > 0.7:
            probs[1] = 0.8   # carbon_focus
        elif state.carbon_intensity > 0.8:
            probs[1] = 0.7   # carbon_focus
        elif state.workload_tokens > 5000:
            probs[2] = 0.6   # energy_focus
        elif state.node_health < 0.5:
            probs[3] = 0.6   # helium_focus (if helium is critical)
        else:
            probs[4] = 0.6   # adaptive
        return probs / probs.sum()

    def confidence(self, state: CostOptimizationState) -> float:
        if state.anomaly_severity > 0.7:
            return 0.6
        return 0.4


class CostHistoricalMLTeacher(Teacher):
    """Offline trained classifier on historical optimal strategies."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_ML:
            import joblib
            self.model = joblib.load(model_path)

    def predict(self, state: CostOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: CostOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class CostStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, cost_func: 'SustainabilityCostFunction', lr: float = 0.1):
        self.cost_func = cost_func
        self.lr = lr
        self.weights = np.zeros((12, 5))  # 12 features, 5 actions
        self._load_state()

    def _load_state(self):
        # We'll persist in the cost_func's internal state (e.g., a dict)
        pass

    def _save_state(self):
        pass

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
    """Linear softmax student updated via distillation + policy gradient."""
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
        logits = state_vector @ self.weights + self.biases

        # Distillation gradient (KL divergence)
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient (REINFORCE)
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
    """
    Multi‑teacher on‑policy distillation agent for cost weight selection.
    """
    ACTION_SPACE = ['standard', 'carbon_focus', 'energy_focus', 'helium_focus', 'adaptive']

    def __init__(self, cost_func: 'SustainabilityCostFunction', config: Dict[str, Any]):
        self.cost_func = cost_func
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            CostRuleBasedTeacher(),
            CostHistoricalMLTeacher(),  # optionally load model
            CostStatefulQTeacher(cost_func)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: CostOptimizationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()

        # Ensemble teachers
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

        # Update Q-teacher if we have the original state
        # We'll do that separately in the main loop.

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# MAIN COST FUNCTION (Enhanced)
# ============================================================================

class SustainabilityCostFunction:
    """
    Enhanced multi‑objective sustainability cost function with adaptive weight selection
    via multi‑teacher distillation.

    Computes a weighted sum of six normalized cost components (each in [0,1]):
        - Energy: joules per token * tokens, normalized by max energy.
        - Carbon: energy * carbon intensity, normalized by max carbon.
        - Helium: inverse of connectivity score, adjusted by scarcity, normalized by max.
        - Material: composite of embodied carbon and rare earth, normalized.
        - Latency: latency target normalized by baseline and max.
        - Accuracy: 1 - accuracy score, normalized by max.

    The weight selection is learned by the distillation agent.
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
    ):
        """
        Initialize the cost function.

        Args:
            carbon_fetcher: Carbon intensity data source.
            material_updater: Material footprint data source.
            helium_collector: Helium connectivity data source.
            config: Configuration (dict or Pydantic model).
            adaptive_cost_function: Optional adaptive cost function for dynamic weights.
            anomaly_detector: Optional anomaly detection module.
            predictive_maintenance: Optional predictive maintenance engine.
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

        # Base weights (initial from config)
        self._base_weights = self._get_initial_weights()
        # Current weights (may be adjusted by strategy)
        self._current_weights = self._base_weights.copy()

        # Carbon intensity cache (LRU)
        self._carbon_cache: OrderedDict[str, Tuple[float, datetime]] = OrderedDict()
        self._carbon_cache_ttl = self._get_config('carbon_cache_ttl_seconds', 300)
        self._carbon_cache_max_size = self._get_config('carbon_cache_max_size', 100)

        # Metrics (Prometheus)
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
            }
        else:
            self.metrics = None

        # State for anomaly cooldown (still used)
        self._last_anomaly_time: Optional[datetime] = None
        self._anomaly_cooldown = timedelta(seconds=300)

        # NEW: Distillation optimizer
        self.distillation_config = {
            'distillation_epsilon': self._get_config('distillation_epsilon', 0.1),
            'distillation_train_every': self._get_config('distillation_train_every', 10),
            'distillation_replay_size': self._get_config('distillation_replay_size', 2000),
            'distillation_learning_rate': self._get_config('distillation_learning_rate', 0.01),
            'distill_weight': self._get_config('distill_weight', 0.7),
            'rl_weight': self._get_config('rl_weight', 0.3),
        }
        self.policy_optimizer = DistillationCostOptimizer(self, self.distillation_config)

        # History for reward computation
        self._last_total_cost: Optional[float] = None
        self._cost_history = deque(maxlen=50)

        logger.info("SustainabilityCostFunction v2.2.0 initialized with config: %s", self.config)

    def _get_config(self, key: str, default: Any = None) -> Any:
        """Safely get a config value, supporting both dict and Pydantic."""
        if hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        return self.config.get(key, default)

    def _get_initial_weights(self) -> Dict[str, float]:
        """Extract initial weights from config and ensure they sum to 1."""
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

    # ---------- Normalization helpers (unchanged) ----------
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

    # ---------- Core computation (enhanced) ----------
    async def compute(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> float:
        """
        Compute the sustainability cost for a given node and workload.
        Uses distillation to select the weight strategy.
        """
        # --- Energy cost ---
        # Apply predictive maintenance efficiency factor if enabled
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

        # --- Get weights via distillation ---
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

        # --- Compute reward and update distillation agent ---
        # Baseline cost using base weights
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

        # Store cost history for state building
        self._cost_history.append(total)
        self._last_total_cost = total

        # Build state for update (next state could be same, but we'll use current state)
        state = self._build_optimization_state(node_desc, workload, expert_profile)
        next_state = state  # in this simple version, we treat next state as same

        # Update agent asynchronously
        asyncio.create_task(self.policy_optimizer.update(
            state.to_feature_vector(),
            self._last_action_idx,
            reward,
            next_state.to_feature_vector(),
            self._last_teacher_probs
        ))

        logger.debug(
            "Cost components (normalized): energy=%.4f, carbon=%.4f, helium=%.4f, material=%.4f, latency=%.4f, accuracy=%.4f, total=%.4f",
            energy_cost, carbon_cost, helium_cost, material_cost, latency_cost, accuracy_cost, total
        )
        return total

    # ---------- New: Build optimization state ----------
    def _build_optimization_state(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> CostOptimizationState:
        """Build state for the distillation agent."""
        # Carbon intensity
        carbon_intensity = self._get_carbon_intensity_sync(node_desc.region)

        # Node health (from predictive maintenance if enabled)
        node_health = 1.0
        if self._get_config('integrate_predictive_maintenance', False) and self.predictive_maintenance:
            try:
                # We'll use a synchronous method or a cached value
                # For simplicity, we assume a method get_efficiency_factor is async; we'll use a default.
                node_health = 0.8  # placeholder
            except Exception:
                pass

        # Workload characteristics
        tokens = workload.tokens
        latency = workload.latency_target

        # Anomaly severity
        anomaly_severity = 0.0
        if self._get_config('integrate_anomaly_detection', False) and self.anomaly_detector:
            try:
                if hasattr(self.anomaly_detector, 'get_latest_severity'):
                    anomaly_severity = await self.anomaly_detector.get_latest_severity()
            except Exception:
                pass

        # Historical cost trend
        if len(self._cost_history) >= 5:
            recent = list(self._cost_history)[-5:]
            trend = (recent[-1] - recent[0]) / (len(recent) - 1) if len(recent) > 1 else 0.0
            avg_cost_trend = trend
            cost_variance = np.var(recent)
        else:
            avg_cost_trend = 0.0
            cost_variance = 0.0

        # Current weights (we use base weights for state, or we could use current)
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
        """Synchronous version of _get_carbon_intensity (for state building)."""
        # Use cache if available
        now = datetime.now()
        if region in self._carbon_cache:
            value, timestamp = self._carbon_cache[region]
            if (now - timestamp).total_seconds() < self._carbon_cache_ttl:
                return value
        # Otherwise return baseline
        return self._get_config('carbon_intensity_baseline_kg_per_kwh', 0.4)

    # ---------- Modified _get_weights ----------
    async def _get_weights(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        anomaly_detected: bool = False,
    ) -> Dict[str, float]:
        """
        Return current weights.
        Uses distillation agent to select a strategy, then applies it.
        Stores the selected action for reward update.
        """
        # Build state
        state = self._build_optimization_state(node_desc, workload, None)

        # Select strategy via distillation
        strategy, action_idx, state_vec, teacher_probs = await self.policy_optimizer.select_strategy(state, exploration=True)

        # Store for update
        self._last_action_idx = action_idx
        self._last_teacher_probs = teacher_probs

        # Apply strategy
        weights = self._base_weights.copy()
        if strategy == 'standard':
            pass  # use base weights
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

        # Normalize weights to sum to 1
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}

        # Store current weights for metrics
        self._current_weights = weights
        return weights

    # ---------- Helper methods (unchanged) ----------
    async def _get_carbon_intensity(self, region: str) -> float:
        """Fetch carbon intensity with LRU caching."""
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

    # ---------- Integration callbacks (unchanged) ----------
    async def on_anomaly_detected(self, anomaly_severity: float):
        """Callback from anomaly detection module."""
        if not self._get_config('integrate_anomaly_detection', False):
            return
        self._last_anomaly_time = datetime.now()
        # The distillation agent will handle weight adjustments; we don't need to do anything.
        logger.info(f"Anomaly detected with severity {anomaly_severity}, will be reflected in next strategy selection.")

    async def update_from_predictive_maintenance(self, node_id: str, efficiency_factor: float):
        """Update cost based on predictive maintenance feedback."""
        if not self._get_config('integrate_predictive_maintenance', False):
            return
        logger.debug(f"Predictive maintenance update for node {node_id}: factor={efficiency_factor}")

    # ---------- Utility methods ----------
    async def get_weights(self) -> Dict[str, float]:
        """Return current weights."""
        return self._current_weights.copy()

    async def set_weights(self, new_weights: Dict[str, float]) -> None:
        """Manually set base weights (overrides config)."""
        total = sum(new_weights.values())
        if total == 0:
            raise ValueError("Weights sum cannot be zero")
        self._base_weights = {k: v / total for k, v in new_weights.items()}
        logger.info(f"Base weights set manually: {self._base_weights}")

    async def reset_weights(self) -> None:
        """Reset weights to initial config values."""
        self._base_weights = self._get_initial_weights()
        logger.info("Weights reset to initial configuration")

    async def reset_carbon_cache(self) -> None:
        """Clear the carbon intensity cache."""
        self._carbon_cache.clear()
        logger.info("Carbon cache cleared")

    async def get_cost_breakdown(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> Dict[str, Any]:
        """
        Return a breakdown of cost components (for dashboard/explanation).
        """
        # Compute raw values (similar to compute but without distillation)
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
        """Clean up resources."""
        pass

    # ---------- Distillation stats ----------
    async def get_distillation_stats(self) -> Dict:
        return self.policy_optimizer.get_stats()


# ============================================================================
# Convenience factory (unchanged)
# ============================================================================
def create_cost_function(
    carbon_fetcher: CarbonIntensityFetcher,
    material_updater: MaterialFootprintUpdater,
    helium_collector: HeliumCollector,
    config: Optional[Dict[str, Any]] = None,
    adaptive_cost_function: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
    predictive_maintenance: Optional[Any] = None,
) -> SustainabilityCostFunction:
    return SustainabilityCostFunction(
        carbon_fetcher=carbon_fetcher,
        material_updater=material_updater,
        helium_collector=helium_collector,
        config=config,
        adaptive_cost_function=adaptive_cost_function,
        anomaly_detector=anomaly_detector,
        predictive_maintenance=predictive_maintenance,
    )


# ============================================================================
# Example usage (if run directly)
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
        breakdown = await cost_func.get_cost_breakdown(node_desc, workload, expert)
        print("Cost breakdown:", breakdown)
        stats = await cost_func.get_distillation_stats()
        print("Distillation stats:", stats)

    asyncio.run(main())
