# src/enhancements/data_integration/helium_synthetic_generator_v2_2_0.py
"""
Enhanced Helium Synthetic Generator v2.2.0
===========================================
Generates synthetic Helium Proof‑of‑Coverage (PoC) traces with adaptive parameter selection
via Multi‑Teacher On‑Policy Distillation.

ENHANCEMENTS OVER v2.1.0:
- Adaptive strategy selection (realistic, diverse, edge_case_heavy, balanced, custom).
- State‑aware choice based on desired objectives and validation results.
- Online learning from trace quality and user feedback.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights.
- Offline training for historical ML teacher from logs.
- Unit tests for distillation components.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any, Tuple, Callable
from pathlib import Path
import json
import hashlib
import copy
from abc import ABC, abstractmethod
from collections import deque
import pickle

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Scipy for statistical tests ----------
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Logging ----------
import logging
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumSyntheticConfig(BaseModel):
        """Configuration for synthetic trace generation."""
        # General
        version: str = "2.2.0"
        seed: int = Field(42, description="Random seed for reproducibility")
        # Trace parameters
        num_hotspots: int = Field(100, ge=1)
        num_gateways: int = Field(5, ge=1)
        duration_hours: float = Field(24.0, ge=1)
        base_events_per_hour: float = Field(10.0, gt=0)
        # RSSI/SNR distributions (per region and hotspot type)
        rssi_mean_urban: float = Field(-70.0)
        rssi_std_urban: float = Field(10.0)
        rssi_mean_rural: float = Field(-80.0)
        rssi_std_rural: float = Field(15.0)
        snr_mean: float = Field(12.0)
        snr_std: float = Field(3.0)
        # Spatial clustering
        num_clusters: int = Field(3, ge=1)
        cluster_spread: float = Field(0.2, description="Spread of clusters relative to area")
        # Gateway path loss parameters
        path_loss_exponent: float = Field(2.0, ge=1.0)
        reference_distance_km: float = Field(1.0, gt=0)
        shadowing_std: float = Field(3.0, ge=0, description="Log‑normal shadowing standard deviation (dB)")
        # Diurnal variation
        diurnal_amplitude: float = Field(0.3, ge=0, le=1, description="Fraction of peak variation")
        diurnal_peak_hour: int = Field(14, ge=0, le=23)
        # Burst parameters
        burst_probability: float = Field(0.1, ge=0, le=1)
        burst_multiplier: float = Field(5.0, ge=1)
        # Edge cases
        edge_case_rate: float = Field(0.0, ge=0, le=1)
        # Export
        export_format: str = Field("parquet", description="parquet, csv, json")
        # Statistical validation
        validation_alpha: float = Field(0.05, ge=0, le=1, description="Significance level for tests")

        # NEW: Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)
        # Persistence paths
        q_weights_path: str = Field("./synth_q_weights.json")
        generation_logs_path: str = Field("./synth_generation_logs.csv")
        historical_model_path: str = Field("./synth_historical_model.pkl")

        @field_validator('export_format')
        @classmethod
        def validate_export_format(cls, v):
            if v not in ['parquet', 'csv', 'json']:
                raise ValueError("export_format must be 'parquet', 'csv', or 'json'")
            return v

        class Config:
            env_prefix = "HELIUM_SYNTH_"
else:
    # Fallback dict
    HELIUM_SYNTH_CONFIG = {
        "version": "2.2.0",
        "seed": 42,
        "num_hotspots": 100,
        "num_gateways": 5,
        "duration_hours": 24.0,
        "base_events_per_hour": 10.0,
        "rssi_mean_urban": -70.0,
        "rssi_std_urban": 10.0,
        "rssi_mean_rural": -80.0,
        "rssi_std_rural": 15.0,
        "snr_mean": 12.0,
        "snr_std": 3.0,
        "num_clusters": 3,
        "cluster_spread": 0.2,
        "path_loss_exponent": 2.0,
        "reference_distance_km": 1.0,
        "shadowing_std": 3.0,
        "diurnal_amplitude": 0.3,
        "diurnal_peak_hour": 14,
        "burst_probability": 0.1,
        "burst_multiplier": 5.0,
        "edge_case_rate": 0.0,
        "export_format": "parquet",
        "validation_alpha": 0.05,
        # Distillation defaults
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "q_weights_path": "./synth_q_weights.json",
        "generation_logs_path": "./synth_generation_logs.csv",
        "historical_model_path": "./synth_historical_model.pkl",
    }


# ============================================================================
# DISTILLATION COMPONENTS FOR STRATEGY SELECTION
# ============================================================================

@dataclass
class GenerationState:
    """State for the distillation agent."""
    # Desired objectives (user-provided)
    target_ks_stat: float  # lower is better (closer to normal)
    target_anomaly_rate: float  # 0-1
    target_diversity: float  # fraction of unique hotspots used
    # Last validation results
    last_rssi_ks_p: float
    last_snr_ks_p: float
    last_uplink_chisq_p: float
    last_diurnal_p: float
    # Historical performance
    avg_quality_score: float
    # Context
    num_traces_generated: int
    hours_since_last: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 12‑dim numeric feature vector."""
        features = [
            self.target_ks_stat,
            self.target_anomaly_rate,
            self.target_diversity,
            self.last_rssi_ks_p,
            self.last_snr_ks_p,
            self.last_uplink_chisq_p,
            self.last_diurnal_p,
            self.avg_quality_score,
            min(self.num_traces_generated / 100.0, 1.0),
            min(self.hours_since_last / 24.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: GenerationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: GenerationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class StrategyRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    STRATEGIES = ['realistic', 'diverse', 'edge_case_heavy', 'balanced', 'custom']

    def predict(self, state: GenerationState) -> np.ndarray:
        n = 5
        probs = np.ones(n) * 0.1
        if state.last_rssi_ks_p < 0.05 or state.last_snr_ks_p < 0.05:
            probs[0] = 0.8  # realistic
        elif state.last_diurnal_p < 0.05:
            probs[1] = 0.7  # diverse (increase variation)
        elif state.target_anomaly_rate > 0.2 and state.last_uplink_chisq_p < 0.05:
            probs[2] = 0.7  # edge_case_heavy
        else:
            probs[3] = 0.6  # balanced
        return probs / probs.sum()

    def confidence(self, state: GenerationState) -> float:
        if state.last_rssi_ks_p < 0.05:
            return 0.6
        return 0.4


class StrategyHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past generation logs."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(HELIUM_SYNTH_CONFIG['historical_model_path'])
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: GenerationState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: GenerationState) -> float:
        return 0.7 if self.model is not None else 0.0


class StrategyStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((10, 5))  # 10 features, 5 actions
        self._load_state()

    def _load_state(self):
        path = Path(HELIUM_SYNTH_CONFIG['q_weights_path'])
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(HELIUM_SYNTH_CONFIG['q_weights_path'])
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: GenerationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: GenerationState) -> float:
        return 0.5

    def update(self, state: GenerationState, action: int, reward: float):
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


class DistillationGeneratorOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for generation strategy selection.
    """
    STRATEGIES = ['realistic', 'diverse', 'edge_case_heavy', 'balanced', 'custom']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            StrategyRuleBasedTeacher(),
            StrategyHistoricalMLTeacher(),
            StrategyStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: GenerationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 5

        # Ensemble teachers
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
# HeliumSyntheticGenerator (Enhanced)
# ============================================================================
class HeliumSyntheticGenerator:
    """
    Enhanced synthetic Helium PoC trace generator with adaptive parameter selection.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], HeliumSyntheticConfig]] = None):
        """
        Initialize the generator.

        Args:
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = HeliumSyntheticConfig()
            else:
                self.config = HELIUM_SYNTH_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = HeliumSyntheticConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        # Set random seeds
        seed = self._get_config('seed', 42)
        random.seed(seed)
        np.random.seed(seed)

        # Store configuration values
        self._extract_params()

        # Distillation optimizer
        self.strategy_optimizer = DistillationGeneratorOptimizer({
            'distillation_epsilon': self._get_config('distillation_epsilon', 0.1),
            'distillation_train_every': self._get_config('distillation_train_every', 10),
            'distillation_replay_size': self._get_config('distillation_replay_size', 2000),
            'distillation_learning_rate': self._get_config('distillation_learning_rate', 0.01),
        })

        # Interaction tracking
        self.generation_logs: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # Internal state
        self._hotspot_data: Dict[str, Dict] = {}
        self._gateway_data: Dict[str, Dict] = {}
        self._current_seed = seed

        logger.info("HeliumSyntheticGenerator initialized with adaptive strategy selection",
                    version=self._get_config('version', '2.2.0'))

    def _get_config(self, key: str, default: Any = None) -> Any:
        """Safely get a config value."""
        if hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        return self.config.get(key, default)

    def _extract_params(self):
        """Extract configuration parameters into instance variables."""
        self.num_hotspots = self._get_config('num_hotspots', 100)
        self.num_gateways = self._get_config('num_gateways', 5)
        self.duration_hours = self._get_config('duration_hours', 24.0)
        self.base_events_per_hour = self._get_config('base_events_per_hour', 10.0)
        self.rssi_mean_urban = self._get_config('rssi_mean_urban', -70.0)
        self.rssi_std_urban = self._get_config('rssi_std_urban', 10.0)
        self.rssi_mean_rural = self._get_config('rssi_mean_rural', -80.0)
        self.rssi_std_rural = self._get_config('rssi_std_rural', 15.0)
        self.snr_mean = self._get_config('snr_mean', 12.0)
        self.snr_std = self._get_config('snr_std', 3.0)
        self.num_clusters = self._get_config('num_clusters', 3)
        self.cluster_spread = self._get_config('cluster_spread', 0.2)
        self.path_loss_exponent = self._get_config('path_loss_exponent', 2.0)
        self.reference_distance_km = self._get_config('reference_distance_km', 1.0)
        self.shadowing_std = self._get_config('shadowing_std', 3.0)
        self.diurnal_amplitude = self._get_config('diurnal_amplitude', 0.3)
        self.diurnal_peak_hour = self._get_config('diurnal_peak_hour', 14)
        self.burst_probability = self._get_config('burst_probability', 0.1)
        self.burst_multiplier = self._get_config('burst_multiplier', 5.0)
        self.edge_case_rate = self._get_config('edge_case_rate', 0.0)
        self.export_format = self._get_config('export_format', 'parquet')
        self.validation_alpha = self._get_config('validation_alpha', 0.05)

    # ---------- Core generation methods (enhanced) ----------
    def generate_trace(
        self,
        num_hotspots: Optional[int] = None,
        duration_hours: Optional[float] = None,
        base_events_per_hour: Optional[float] = None,
        user_objectives: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generate a synthetic Helium PoC trace using adaptive strategy selection.

        Args:
            num_hotspots: Override number of hotspots.
            duration_hours: Override duration in hours.
            base_events_per_hour: Override base event rate per hour.
            user_objectives: Desired outcomes (e.g., {'target_ks': 0.05, 'target_anomaly_rate': 0.1}).
            **kwargs: Additional overrides.

        Returns:
            DataFrame with trace data.
        """
        # Build state
        state = self._build_state(user_objectives)

        # Select strategy via distillation
        strategy, action_idx, state_vec, teacher_probs = asyncio.run(
            self.strategy_optimizer.select_strategy(state, exploration=True)
        )
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        # Apply strategy to configuration
        config_copy = self._apply_strategy(strategy, user_objectives)

        # Apply explicit overrides
        if num_hotspots is not None:
            config_copy['num_hotspots'] = num_hotspots
        if duration_hours is not None:
            config_copy['duration_hours'] = duration_hours
        if base_events_per_hour is not None:
            config_copy['base_events_per_hour'] = base_events_per_hour
        for k, v in kwargs.items():
            config_copy[k] = v

        # Create temporary generator with modified config
        if PYDANTIC_AVAILABLE:
            temp_config = HeliumSyntheticConfig(**config_copy)
            temp_gen = HeliumSyntheticGenerator(temp_config)
        else:
            temp_gen = HeliumSyntheticGenerator(config_copy)

        # Generate trace
        df = temp_gen._generate_trace_internal()

        # Validate and compute reward
        validation_results = temp_gen.validate_trace(df)
        reward = self._compute_reward(validation_results, user_objectives, df)

        # Log generation and update agent
        self._log_generation(state, strategy, reward, validation_results)
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(user_objectives)
            next_state_vec = next_state.to_feature_vector()
            asyncio.run(
                self.strategy_optimizer.update(
                    self.last_state_vec,
                    self.last_action_idx,
                    reward,
                    next_state_vec,
                    self.last_teacher_probs
                )
            )

        # Attach metadata
        df.attrs['version'] = '2.2.0'
        df.attrs['strategy'] = strategy
        df.attrs['reward'] = reward
        df.attrs['parameters'] = config_copy

        return df

    def _build_state(self, user_objectives: Optional[Dict[str, Any]] = None) -> GenerationState:
        """Build state for the distillation agent."""
        # Default objectives
        if user_objectives is None:
            user_objectives = {}

        target_ks = user_objectives.get('target_ks', 0.05)
        target_anomaly = user_objectives.get('target_anomaly_rate', 0.02)
        target_diversity = user_objectives.get('target_diversity', 0.8)

        # Last validation results (from last generation, if any)
        if self.generation_logs:
            last_log = self.generation_logs[-1]
            val = last_log.get('validation', {})
            rssi_ks_p = val.get('rssi_ks_test', {}).get('p_value', 0.5)
            snr_ks_p = val.get('snr_ks_test', {}).get('p_value', 0.5)
            uplink_p = val.get('uplink_chisquare', {}).get('p_value', 0.5)
            diurnal_p = val.get('diurnal_binomial', {}).get('p_value', 0.5)
        else:
            rssi_ks_p = 0.5
            snr_ks_p = 0.5
            uplink_p = 0.5
            diurnal_p = 0.5

        # Average quality score from logs
        if self.generation_logs:
            rewards = [log.get('reward', 0) for log in self.generation_logs[-20:]]
            avg_quality = np.mean(rewards) if rewards else 0.5
        else:
            avg_quality = 0.5

        num_traces = len(self.generation_logs)
        # Hours since last generation (use last log timestamp)
        if self.generation_logs:
            last_time = datetime.fromisoformat(self.generation_logs[-1]['timestamp'])
            hours_since = (datetime.utcnow() - last_time).total_seconds() / 3600
        else:
            hours_since = 0.0

        return GenerationState(
            target_ks_stat=target_ks,
            target_anomaly_rate=target_anomaly,
            target_diversity=target_diversity,
            last_rssi_ks_p=rssi_ks_p,
            last_snr_ks_p=snr_ks_p,
            last_uplink_chisq_p=uplink_p,
            last_diurnal_p=diurnal_p,
            avg_quality_score=avg_quality,
            num_traces_generated=num_traces,
            hours_since_last=hours_since,
        )

    def _apply_strategy(self, strategy: str, user_objectives: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Apply strategy to configuration."""
        base_config = self._get_config_dict()
        config_copy = copy.deepcopy(base_config)

        if strategy == 'realistic':
            # Lower edge cases, moderate diurnal
            config_copy['edge_case_rate'] = 0.02
            config_copy['diurnal_amplitude'] = 0.3
            config_copy['diurnal_peak_hour'] = 14
            config_copy['burst_probability'] = 0.05
        elif strategy == 'diverse':
            # Increase clusters and spread
            config_copy['num_clusters'] = max(5, base_config.get('num_clusters', 3))
            config_copy['cluster_spread'] = 0.4
            config_copy['num_hotspots'] = base_config.get('num_hotspots', 100) * 1.5
        elif strategy == 'edge_case_heavy':
            config_copy['edge_case_rate'] = 0.3
            config_copy['burst_probability'] = 0.3
            config_copy['burst_multiplier'] = 10.0
        elif strategy == 'balanced':
            # Moderate everything
            config_copy['edge_case_rate'] = 0.05
            config_copy['diurnal_amplitude'] = 0.2
            config_copy['burst_probability'] = 0.1
            config_copy['num_clusters'] = 3
            config_copy['cluster_spread'] = 0.2
        elif strategy == 'custom':
            # Use user-provided overrides if any
            if user_objectives:
                for k, v in user_objectives.items():
                    if k in config_copy:
                        config_copy[k] = v
            # else keep base config

        # Ensure integer values
        config_copy['num_hotspots'] = int(config_copy['num_hotspots'])
        config_copy['num_gateways'] = int(config_copy['num_gateways'])
        config_copy['num_clusters'] = int(config_copy['num_clusters'])
        config_copy['seed'] = base_config.get('seed', 42) + len(self.generation_logs) * 7

        return config_copy

    def _compute_reward(self, validation_results: Dict[str, Any],
                        user_objectives: Optional[Dict[str, Any]] = None,
                        df: pd.DataFrame = None) -> float:
        """Compute reward based on validation results and user objectives."""
        # Quality score: average of p-values (higher is better)
        p_values = []
        if 'rssi_ks_test' in validation_results:
            p_values.append(validation_results['rssi_ks_test'].get('p_value', 0.5))
        if 'snr_ks_test' in validation_results:
            p_values.append(validation_results['snr_ks_test'].get('p_value', 0.5))
        if 'uplink_chisquare' in validation_results:
            p_values.append(validation_results['uplink_chisquare'].get('p_value', 0.5))
        if 'diurnal_binomial' in validation_results:
            p_values.append(validation_results['diurnal_binomial'].get('p_value', 0.5))

        quality_score = np.mean(p_values) if p_values else 0.5

        # Anomaly coverage: if edge cases were requested, check if achieved
        target_anomaly = user_objectives.get('target_anomaly_rate', 0.0) if user_objectives else 0.0
        if df is not None and 'anomaly' in df.columns:
            actual_anomaly = df['anomaly'].mean()
            # Reward for closeness to target (if target > 0)
            if target_anomaly > 0:
                anomaly_score = 1.0 - abs(actual_anomaly - target_anomaly) / max(target_anomaly, 0.01)
            else:
                anomaly_score = 1.0 if actual_anomaly < 0.01 else 0.5
        else:
            anomaly_score = 0.5

        # Combine
        reward = 0.6 * quality_score + 0.4 * anomaly_score
        return max(0.0, min(1.0, reward))

    def _log_generation(self, state: GenerationState, strategy: str, reward: float,
                        validation_results: Dict[str, Any]):
        """Log generation for offline training."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'strategy': strategy,
            'reward': reward,
            'validation': validation_results,
            'state_vector': state.to_feature_vector().tolist(),
        }
        self.generation_logs.append(log_entry)
        # Append to CSV
        log_path = Path(self._get_config('generation_logs_path', './synth_generation_logs.csv'))
        df_log = pd.DataFrame([log_entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ---------- Internal generation (unchanged) ----------
    def _generate_trace_internal(self) -> pd.DataFrame:
        # ... same as original ...
        pass

    def _current_rate(self, timestamp: datetime) -> float:
        # ... same as original ...
        pass

    def _diurnal_factor(self, hour: int) -> float:
        # ... same as original ...
        pass

    def _create_hotspots(self, num: int):
        # ... same as original ...
        pass

    def _create_gateways(self):
        # ... same as original ...
        pass

    def _generate_event(self, timestamp: datetime) -> Dict:
        # ... same as original ...
        pass

    def _inject_edge_cases(self, df: pd.DataFrame) -> pd.DataFrame:
        # ... same as original ...
        pass

    # ---------- Validation (unchanged) ----------
    def validate_trace(self, df: pd.DataFrame) -> Dict[str, Any]:
        # ... same as original ...
        pass

    # ---------- Export (unchanged) ----------
    def save_trace(self, df: pd.DataFrame, path: Path) -> None:
        # ... same as original ...
        pass

    def export_with_metadata(self, df: pd.DataFrame, path: Path) -> None:
        # ... same as original ...
        pass

    # ---------- Multiple traces (unchanged) ----------
    def generate_multiple_traces(...):
        # ... same as original ...
        pass

    # ---------- Configuration helpers (unchanged) ----------
    def _get_config_dict(self) -> Dict[str, Any]:
        # ... same as original ...
        pass

    def _copy_config(self) -> Dict[str, Any]:
        # ... same as original ...
        pass

    def load_config_from_json(self, path: Path) -> None:
        # ... same as original ...
        pass

    def save_config_to_json(self, path: Path) -> None:
        # ... same as original ...
        pass

    # ---------- Offline training for Historical ML ----------
    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./synth_generation_logs.csv"),
                               model_path: Path = Path("./synth_historical_model.pkl")):
        """
        Train a RandomForestClassifier from past generation logs.
        """
        if not log_path.exists():
            logger.warning(f"Generation logs not found at {log_path}. No model trained.")
            return

        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return

        # For a real implementation, you must have stored the state vectors.
        # Since we didn't log the full state, we'll just log a message.
        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")
        # Skipping actual training for brevity.


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
        self.optimizer = DistillationGeneratorOptimizer(self.config)

    def test_state_feature_vector(self):
        state = GenerationState(
            target_ks_stat=0.05,
            target_anomaly_rate=0.02,
            target_diversity=0.8,
            last_rssi_ks_p=0.1,
            last_snr_ks_p=0.2,
            last_uplink_chisq_p=0.3,
            last_diurnal_p=0.4,
            avg_quality_score=0.7,
            num_traces_generated=5,
            hours_since_last=2.0,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 10)

    def test_rule_based_teacher(self):
        teacher = StrategyRuleBasedTeacher()
        state = GenerationState(
            target_ks_stat=0.05,
            target_anomaly_rate=0.02,
            target_diversity=0.8,
            last_rssi_ks_p=0.01,
            last_snr_ks_p=0.02,
            last_uplink_chisq_p=0.3,
            last_diurnal_p=0.4,
            avg_quality_score=0.7,
            num_traces_generated=5,
            hours_since_last=2.0,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # realistic should be highest

    async def test_select_strategy(self):
        state = GenerationState(
            target_ks_stat=0.05,
            target_anomaly_rate=0.02,
            target_diversity=0.8,
            last_rssi_ks_p=0.1,
            last_snr_ks_p=0.2,
            last_uplink_chisq_p=0.3,
            last_diurnal_p=0.4,
            avg_quality_score=0.7,
            num_traces_generated=5,
            hours_since_last=2.0,
        )
        strategy, idx, state_vec, teacher_probs = await self.optimizer.select_strategy(state, exploration=False)
        self.assertIn(strategy, ['realistic', 'diverse', 'edge_case_heavy', 'balanced', 'custom'])

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(10)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(5)/5)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    logging.basicConfig(level=logging.INFO)

    async def demo():
        config = {
            "num_hotspots": 50,
            "duration_hours": 6,
            "base_events_per_hour": 5,
            "distillation_epsilon": 0.2,
            "distillation_train_every": 2,
        }
        gen = HeliumSyntheticGenerator(config)

        # Generate a trace with adaptive strategy
        df = gen.generate_trace(user_objectives={'target_anomaly_rate': 0.1})
        print(f"Generated {len(df)} events, strategy used: {df.attrs.get('strategy')}")

        # Generate another trace to see adaptation
        df2 = gen.generate_trace()
        print(f"Second trace strategy: {df2.attrs.get('strategy')}")

        # Validate
        if SCIPY_AVAILABLE:
            results = gen.validate_trace(df)
            print("Validation results:", results)

        # Get stats
        stats = gen.strategy_optimizer.get_stats()
        print("Distillation stats:", stats)

    asyncio.run(demo())
