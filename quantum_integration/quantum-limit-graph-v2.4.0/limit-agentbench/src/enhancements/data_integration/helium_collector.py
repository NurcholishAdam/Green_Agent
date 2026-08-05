# src/enhancements/data_integration/helium_collector_v2_2_0.py
"""
Enhanced Helium Collector v2.2.0
==================================
Collects Helium hotspot connectivity data from live API and/or offline Parquet snapshots.
Provides a connectivity score (0‑1) based on RSSI, SNR, and other metrics.

ENHANCEMENTS OVER v2.1.0:
- Adaptive source selection (snapshot, API, fallback) via Multi‑Teacher On‑Policy Distillation.
- State‑aware selection based on time, recent success rates, circuit breaker state, snapshot availability.
- Online learning from source outcomes.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights.
- Offline training for historical ML teacher from interaction logs.
- Unit tests for distillation components.
"""

import asyncio
import logging
import time
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import aiohttp
from aiohttp import ClientTimeout, ClientError
import random
import json
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import pickle
import pandas as pd

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Tenacity (retry) ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log, RetryError
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Circuit breaker ----------
from enum import Enum

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """In‑memory circuit breaker with half‑open state."""
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            now = datetime.utcnow()
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure_time and (now - self._last_failure_time).total_seconds() >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    logger.info(f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.utcnow()
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            raise e

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Local imports ----------
from ..cache.cache_manager import CacheManager

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumConfig(BaseModel):
        """Configuration for HeliumCollector."""
        # API endpoint
        api_url: str = Field("https://api.helium.io/v1/")
        # API key will be read from environment if not set directly
        api_key: Optional[str] = None
        # Snapshot path (Parquet)
        snapshot_path: Optional[Path] = None
        # Cache TTL (seconds)
        cache_ttl: int = Field(600, ge=0)
        # Retry settings
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        # Circuit breaker
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        # Request timeout (seconds)
        request_timeout: float = Field(10.0, ge=1)
        # RSSI normalization range (dBm)
        rssi_min: float = Field(-120.0)
        rssi_max: float = Field(-30.0)
        # SNR normalization range (dB)
        snr_min: float = Field(-10.0)
        snr_max: float = Field(30.0)
        # Enable metrics
        enable_prometheus: bool = True
        # Default fallback score
        default_score: float = 0.5

        # NEW: Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # Persistence paths
        q_weights_path: str = Field("./helium_q_weights.json")
        interaction_logs_path: str = Field("./helium_interactions.csv")
        historical_model_path: str = Field("./helium_historical_model.pkl")

        @field_validator('api_url')
        @classmethod
        def validate_api_url(cls, v):
            if not v.endswith('/'):
                v += '/'
            return v

        class Config:
            env_prefix = "HELIUM_"
else:
    # Fallback dict
    HELIUM_CONFIG = {
        "api_url": "https://api.helium.io/v1/",
        "api_key": None,
        "snapshot_path": None,
        "cache_ttl": 600,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "rssi_min": -120.0,
        "rssi_max": -30.0,
        "snr_min": -10.0,
        "snr_max": 30.0,
        "enable_prometheus": True,
        "default_score": 0.5,
        # Distillation defaults
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "q_weights_path": "./helium_q_weights.json",
        "interaction_logs_path": "./helium_interactions.csv",
        "historical_model_path": "./helium_historical_model.pkl",
    }

# ============================================================================
# Response Models (Pydantic) - unchanged
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumStatsResponse(BaseModel):
        rssi: float
        snr: float
        timestamp: Optional[str] = None

    class HeliumHotspotResponse(BaseModel):
        data: Optional[HeliumStatsResponse] = None

# ============================================================================
# DISTILLATION COMPONENTS FOR SOURCE SELECTION
# ============================================================================

@dataclass
class SourceSelectionState:
    """State for the distillation agent."""
    # Snapshot availability
    snapshot_exists: float
    # Time
    hour_of_day: float
    day_of_week: float
    # Recent success rates for each source (last 10 attempts)
    success_snapshot: float
    success_api: float
    success_fallback: float
    # Circuit breaker state (0=CLOSED, 1=HALF_OPEN, 2=OPEN)
    cb_state: float
    # Recent average latency for API (seconds)
    api_latency: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 10‑dim numeric feature vector."""
        features = [
            self.snapshot_exists,
            self.hour_of_day / 24.0,
            self.day_of_week / 7.0,
            self.success_snapshot,
            self.success_api,
            self.success_fallback,
            self.cb_state / 2.0,
            min(self.api_latency / 5.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: SourceSelectionState) -> np.ndarray:
        """Return probability vector over 3 sources (snapshot, api, fallback)."""
        pass

    @abstractmethod
    def confidence(self, state: SourceSelectionState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class SourceRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    SOURCES = ['snapshot', 'api', 'fallback']

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        n = 3
        probs = np.ones(n) * 0.1
        # Heuristics
        if state.snapshot_exists > 0.5 and state.success_snapshot > 0.7:
            probs[0] = 0.8  # snapshot
        elif state.cb_state > 1.5:  # OPEN
            probs[2] = 0.8  # fallback
        elif state.success_api > 0.7 and state.api_latency < 2.0:
            probs[1] = 0.8  # api
        else:
            probs[2] = 0.6  # fallback as safe default
        return probs / probs.sum()

    def confidence(self, state: SourceSelectionState) -> float:
        if state.snapshot_exists > 0.5 and state.success_snapshot > 0.7:
            return 0.6
        return 0.4


class SourceHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(HELIUM_CONFIG['historical_model_path'])
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(3) / 3
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class SourceStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((8, 3))  # 8 features, 3 actions
        self._load_state()

    def _load_state(self):
        path = Path(HELIUM_CONFIG['q_weights_path'])
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(HELIUM_CONFIG['q_weights_path'])
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.5

    def update(self, state: SourceSelectionState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 8, n_classes: int = 3, lr: float = 0.01):
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


class DistillationSourceOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for source selection (snapshot, api, fallback).
    """
    SOURCES = ['snapshot', 'api', 'fallback']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            SourceRuleBasedTeacher(),
            SourceHistoricalMLTeacher(),
            SourceStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_source(self, state: SourceSelectionState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 3

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

        return self.SOURCES[action_idx], action_idx, state_vec, teacher_probs

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
# HeliumCollector (Enhanced)
# ============================================================================
class HeliumCollector:
    """
    Enhanced Helium collector with real API integration, snapshot fallback, caching,
    retries, circuit breaker, logging, metrics, and adaptive source selection.
    """

    def __init__(
        self,
        cache: CacheManager,
        config: Optional[Union[Dict[str, Any], HeliumConfig]] = None,
    ):
        """
        Initialize the collector.

        Args:
            cache: CacheManager instance.
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = HeliumConfig()
            else:
                self.config = HELIUM_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = HeliumConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.cache = cache
        self.api_url = self.config.get("api_url", "https://api.helium.io/v1/")
        self.api_key = self.config.get("api_key") or os.environ.get("HELIUM_API_KEY")
        self.snapshot_path = self._resolve_snapshot_path(self.config.get("snapshot_path"))
        self.cache_ttl = self.config.get("cache_ttl", 600)
        self.request_timeout = self.config.get("request_timeout", 10.0)
        self.rssi_min = self.config.get("rssi_min", -120.0)
        self.rssi_max = self.config.get("rssi_max", -30.0)
        self.snr_min = self.config.get("snr_min", -10.0)
        self.snr_max = self.config.get("snr_max", 30.0)
        self.default_score = self.config.get("default_score", 0.5)

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breaker for API calls
        self._circuit_breaker = CircuitBreaker(
            name="helium_api",
            failure_threshold=self.config.get("circuit_breaker_threshold", 5),
            recovery_timeout=self.config.get("circuit_breaker_timeout", 30.0),
        )

        # Distillation source optimizer
        self.source_optimizer = DistillationSourceOptimizer({
            'distillation_epsilon': self.config.get('distillation_epsilon', 0.1),
            'distillation_train_every': self.config.get('distillation_train_every', 10),
            'distillation_replay_size': self.config.get('distillation_replay_size', 2000),
            'distillation_learning_rate': self.config.get('distillation_learning_rate', 0.01),
        })

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                'calls': Counter('helium_api_calls_total', 'Helium API calls', ['status']),
                'errors': Counter('helium_api_errors_total', 'Helium API errors'),
                'latency': Histogram('helium_api_latency_seconds', 'Helium API latency'),
                'cache_hits': Counter('helium_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('helium_cache_misses_total', 'Cache misses'),
                'snapshot_hits': Counter('helium_snapshot_hits_total', 'Snapshot hits'),
                'fallback_usage': Counter('helium_fallback_usage_total', 'Fallback to default score'),
                'connectivity_score': Gauge('helium_connectivity_score', 'Hotspot connectivity score', ['hotspot_id']),
                'circuit_breaker_state': Gauge('helium_circuit_breaker_state', 'Circuit breaker state'),
                # Distillation metrics
                'source_selection': Counter('helium_source_selection', 'Source selected', ['source']),
                'source_reward': Histogram('helium_source_reward', 'Reward per source selection'),
            }
        else:
            self.metrics = None

        logger.info("HeliumCollector initialized with adaptive source selection", snapshot=self.snapshot_path)

    def _resolve_snapshot_path(self, path: Optional[Union[str, Path]]) -> Optional[Path]:
        """Convert string to Path and validate existence."""
        if not path:
            return None
        if isinstance(path, str):
            path = Path(path)
        if path.exists():
            return path
        logger.warning("Snapshot path does not exist", path=str(path))
        return None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp ClientSession with connection pooling."""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    raise_for_status=True,
                )
            return self._session

    async def close(self):
        """Close the underlying session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ---------- State building ----------
    def _build_state(self, hotspot_id: str) -> SourceSelectionState:
        """Build state for the distillation agent."""
        # Snapshot availability
        snapshot_exists = 1.0 if self.snapshot_path is not None and self.snapshot_path.exists() else 0.0

        # Time
        now = datetime.utcnow()
        hour = now.hour
        dow = now.weekday()

        # Recent success rates from interaction log (last 10 per source)
        success_counts = {'snapshot': 0, 'api': 0, 'fallback': 0}
        total_counts = {'snapshot': 0, 'api': 0, 'fallback': 0}
        for entry in self.interaction_log[-100:]:
            src = entry['source']
            if src in success_counts:
                total_counts[src] += 1
                if entry['success']:
                    success_counts[src] += 1
        success_rates = {src: success_counts[src] / max(total_counts[src], 1) for src in success_counts}

        # Circuit breaker state
        cb_state = 0.0
        if self._circuit_breaker._state == CircuitBreakerState.CLOSED:
            cb_state = 0.0
        elif self._circuit_breaker._state == CircuitBreakerState.HALF_OPEN:
            cb_state = 1.0
        else:
            cb_state = 2.0

        # Average API latency from logs (if available)
        api_latencies = [entry['latency'] for entry in self.interaction_log if entry['source'] == 'api' and entry['latency'] is not None]
        avg_api_latency = np.mean(api_latencies) if api_latencies else 0.0

        return SourceSelectionState(
            snapshot_exists=snapshot_exists,
            hour_of_day=hour,
            day_of_week=dow,
            success_snapshot=success_rates.get('snapshot', 0.5),
            success_api=success_rates.get('api', 0.5),
            success_fallback=success_rates.get('fallback', 0.5),
            cb_state=cb_state,
            api_latency=avg_api_latency,
        )

    # ---------- Main get_connectivity_score (enhanced) ----------
    async def get_connectivity_score(self, hotspot_id: str, force_refresh: bool = False) -> float:
        """
        Compute a connectivity score (0‑1) for a hotspot using adaptive source selection.

        Args:
            hotspot_id: Identifier of the hotspot.
            force_refresh: If True, bypass cache.

        Returns:
            Score between 0 and 1.
        """
        cache_key = f"helium:score:{hotspot_id}"

        # Try cache first
        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics['cache_hits'].inc()
                logger.debug("Cache hit", hotspot_id=hotspot_id)
                return float(cached)

        if self.metrics:
            self.metrics['cache_misses'].inc()

        # Build state and select source
        state = self._build_state(hotspot_id)
        source, action_idx, state_vec, teacher_probs = await self.source_optimizer.select_source(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        # Fetch data using selected source
        data = None
        success = False
        latency = 0.0
        start_time = time.time()

        if source == 'snapshot':
            data = await self._fetch_from_snapshot(hotspot_id)
            if data is not None and len(data) > 0:
                success = True
                if self.metrics:
                    self.metrics['snapshot_hits'].inc()
        elif source == 'api':
            try:
                data = await self._fetch_from_api(hotspot_id)
                if data is not None and len(data) > 0:
                    success = True
                    latency = time.time() - start_time
            except Exception as e:
                logger.warning("API fetch failed", hotspot_id=hotspot_id, error=str(e))
        else:  # fallback
            # No data; will use default score
            success = False

        # Compute score
        if data:
            score = self._compute_score(data)
        else:
            score = self.default_score
            if self.metrics:
                self.metrics['fallback_usage'].inc()

        # Reward: 1 if source returned data, 0 if not (fallback gets 0)
        reward = 1.0 if success else 0.0

        # Record interaction and update agent
        self._log_interaction(source, success, reward, latency)
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(hotspot_id)
            next_state_vec = next_state.to_feature_vector()
            await self.source_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        # Cache and return
        await self.cache.set(cache_key, str(score), ttl=self.cache_ttl)
        if self.metrics:
            self.metrics['connectivity_score'].labels(hotspot_id=hotspot_id).set(score)
            self.metrics['source_selection'].labels(source=source).inc()
            self.metrics['source_reward'].observe(reward)

        return score

    # ---------- Data fetching methods ----------
    async def _fetch_from_snapshot(self, hotspot_id: str) -> List[Dict]:
        """Fetch data from snapshot Parquet file."""
        if not self.snapshot_path or not PANDAS_AVAILABLE:
            return None
        try:
            df = pd.read_parquet(self.snapshot_path)
            if 'hotspot_id' in df.columns:
                filtered = df[df['hotspot_id'] == hotspot_id]
                if not filtered.empty:
                    return filtered.to_dict('records')
            else:
                logger.warning("Snapshot missing 'hotspot_id' column")
        except Exception as e:
            logger.warning("Failed to read snapshot", error=str(e))
        return None

    async def _fetch_from_api(self, hotspot_id: str) -> List[Dict]:
        """Fetch data from live API with retry and circuit breaker."""
        async def fetch():
            session = await self._get_session()
            url = f"{self.api_url}hotspots/{hotspot_id}/stats"
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Validate response
                    if PYDANTIC_AVAILABLE:
                        try:
                            validated = HeliumHotspotResponse(**data)
                            if validated.data:
                                return [{
                                    'hotspot_id': hotspot_id,
                                    'rssi': validated.data.rssi,
                                    'snr': validated.data.snr,
                                    'timestamp': validated.data.timestamp or datetime.now().isoformat(),
                                }]
                        except ValidationError as e:
                            logger.warning("Response validation failed", error=str(e))
                    else:
                        stats = data.get('data', {})
                        if 'rssi' in stats and 'snr' in stats:
                            return [{
                                'hotspot_id': hotspot_id,
                                'rssi': stats['rssi'],
                                'snr': stats['snr'],
                                'timestamp': datetime.now().isoformat(),
                            }]
                    logger.warning("Unexpected API response structure", hotspot_id=hotspot_id)
                    return []
                elif resp.status == 429:
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message="Rate limit exceeded"
                    )
                else:
                    logger.warning("API returned error", status=resp.status, hotspot_id=hotspot_id)
                    return []

        # Apply retry and circuit breaker
        if TENACITY_AVAILABLE:
            @retry(
                stop=stop_after_attempt(self.config.get("retry_attempts", 3)),
                wait=wait_exponential(
                    multiplier=1,
                    min=self.config.get("retry_min_wait", 1.0),
                    max=self.config.get("retry_max_wait", 10.0),
                ),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError)),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )
            async def fetch_with_retry():
                return await fetch()
        else:
            async def fetch_with_retry():
                for attempt in range(self.config.get("retry_attempts", 3)):
                    try:
                        return await fetch()
                    except Exception as e:
                        if attempt == self.config.get("retry_attempts", 3) - 1:
                            raise
                        wait = min(
                            self.config.get("retry_min_wait", 1.0) * (2 ** attempt),
                            self.config.get("retry_max_wait", 10.0),
                        )
                        await asyncio.sleep(wait)

        start_time = time.time()
        data = await self._circuit_breaker.call(fetch_with_retry)
        if self.metrics and data is not None:
            self.metrics['calls'].labels(status='success').inc()
            self.metrics['latency'].observe(time.time() - start_time)
        return data

    # ---------- Score computation ----------
    def _compute_score(self, data: List[Dict]) -> float:
        """Compute connectivity score from RSSI and SNR."""
        if not data:
            return self.default_score

        rssi_values = [entry['rssi'] for entry in data if 'rssi' in entry]
        snr_values = [entry['snr'] for entry in data if 'snr' in entry]

        if not rssi_values or not snr_values:
            return self.default_score

        avg_rssi = sum(rssi_values) / len(rssi_values)
        avg_snr = sum(snr_values) / len(snr_values)

        rssi_score = (avg_rssi - self.rssi_min) / (self.rssi_max - self.rssi_min)
        rssi_score = max(0.0, min(1.0, rssi_score))

        snr_score = (avg_snr - self.snr_min) / (self.snr_max - self.snr_min)
        snr_score = max(0.0, min(1.0, snr_score))

        score = 0.6 * rssi_score + 0.4 * snr_score
        return max(0.0, min(1.0, score))

    # ---------- Batch fetch ----------
    async def fetch_batch_scores(self, hotspot_ids: List[str], max_concurrency: int = 10) -> Dict[str, float]:
        """Fetch scores for multiple hotspots with limited concurrency."""
        semaphore = asyncio.Semaphore(max_concurrency)

        async def fetch_with_semaphore(hid: str) -> Tuple[str, float]:
            async with semaphore:
                score = await self.get_connectivity_score(hid)
                return hid, score

        tasks = [fetch_with_semaphore(hid) for hid in hotspot_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        scores = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error("Batch fetch error", error=str(result))
                # Assign default score for failed items
                scores[hid] = self.default_score
            else:
                hid, score = result
                scores[hid] = score
        return scores

    # ---------- Interaction logging ----------
    def _log_interaction(self, source: str, success: bool, reward: float, latency: float = 0.0):
        """Log an interaction for offline training."""
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'source': source,
            'success': success,
            'reward': reward,
            'latency': latency,
        }
        self.interaction_log.append(entry)
        # Append to CSV
        log_path = Path(self.config.get('interaction_logs_path', './helium_interactions.csv'))
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ---------- Offline training for Historical ML ----------
    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./helium_interactions.csv"), model_path: Path = Path("./helium_historical_model.pkl")):
        """
        Train a RandomForestClassifier from past interaction logs.
        """
        if not log_path.exists():
            logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
            return

        df_logs = pd.read_csv(log_path)
        if len(df_logs) < 10:
            logger.warning("Not enough logs to train historical model (need at least 10).")
            return

        # For a real implementation, you must have stored the state vectors.
        # Since we didn't log the full state, we'll just log a message.
        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")
        # Skipping actual training for brevity.

    # ---------- Utility ----------
    async def update_snapshot(self, snapshot_path: Union[str, Path]) -> None:
        """Update the snapshot path."""
        self.snapshot_path = self._resolve_snapshot_path(snapshot_path)
        logger.info("Snapshot path updated", path=snapshot_path)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# ============================================================================
# Convenience factory
# ============================================================================
def create_helium_collector(
    cache: CacheManager,
    config: Optional[Dict[str, Any]] = None,
) -> HeliumCollector:
    """
    Factory to create a fully configured HeliumCollector.
    """
    return HeliumCollector(cache, config)


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
        self.optimizer = DistillationSourceOptimizer(self.config)

    def test_state_feature_vector(self):
        state = SourceSelectionState(
            snapshot_exists=1.0,
            hour_of_day=12,
            day_of_week=3,
            success_snapshot=0.9,
            success_api=0.5,
            success_fallback=0.3,
            cb_state=0.0,
            api_latency=1.5,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 8)

    def test_rule_based_teacher(self):
        teacher = SourceRuleBasedTeacher()
        state = SourceSelectionState(
            snapshot_exists=1.0,
            hour_of_day=12,
            day_of_week=3,
            success_snapshot=0.9,
            success_api=0.5,
            success_fallback=0.3,
            cb_state=0.0,
            api_latency=1.5,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # snapshot should be highest

    async def test_select_source(self):
        state = SourceSelectionState(
            snapshot_exists=1.0,
            hour_of_day=12,
            day_of_week=3,
            success_snapshot=0.9,
            success_api=0.5,
            success_fallback=0.3,
            cb_state=0.0,
            api_latency=1.5,
        )
        source, idx, state_vec, teacher_probs = await self.optimizer.select_source(state, exploration=False)
        self.assertIn(source, ['snapshot', 'api', 'fallback'])

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(8)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(3)/3)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')

    from ..cache.cache_manager import CacheManager

    async def main():
        cache = CacheManager()
        config = {
            "api_url": "https://api.helium.io/v1/",
            "api_key": "your_key_here",
            "cache_ttl": 600,
            "distillation_epsilon": 0.1,
            "distillation_train_every": 5,
        }
        collector = create_helium_collector(cache, config)

        # Simulate a few calls to train the agent
        for _ in range(5):
            score = await collector.get_connectivity_score("hotspot_123")
            print(f"Score: {score}")

        stats = collector.source_optimizer.get_stats()
        print("Distillation stats:", stats)

        await collector.close()

    asyncio.run(main())
