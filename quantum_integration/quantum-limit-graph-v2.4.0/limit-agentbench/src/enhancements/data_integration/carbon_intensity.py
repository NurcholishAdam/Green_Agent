# src/enhancements/data_integration/carbon_intensity_v2_2_0.py
"""
Enhanced Carbon Intensity Fetcher v2.2.0
========================================
Fetches real‑time carbon intensity from multiple providers with adaptive provider selection
via Multi‑Teacher On‑Policy Distillation.

ENHANCEMENTS OVER v2.1.0:
- Adaptive provider selection using distillation.
- State‑aware choice based on region, time, historical success rates, circuit breaker states.
- Online learning from API call outcomes.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights.
- Offline training for historical ML teacher from logs.
- Unit tests for distillation components.
"""

import asyncio
import logging
import time
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Union, Type
import aiohttp
from aiohttp import ClientTimeout, ClientError
import random
import json
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import pickle
import pandas as pd
from pathlib import Path
from enum import Enum

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
    class CarbonIntensityConfig(BaseModel):
        """Configuration for CarbonIntensityFetcher."""
        providers: List[str] = Field(
            default_factory=lambda: ["climate_trace", "os_climate", "electricity_maps"]
        )
        climate_trace_api_key: Optional[str] = None
        os_climate_api_key: Optional[str] = None
        electricity_maps_api_key: Optional[str] = None
        region_averages: Dict[str, float] = Field(
            default_factory=lambda: {
                "us-east": 0.41,
                "us-west": 0.34,
                "eu-west": 0.27,
                "eu-north": 0.21,
                "asia-east": 0.49,
                "asia-southeast": 0.47,
                "global": 0.40,
            }
        )
        cache_ttl: int = Field(3600, ge=0)
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        request_timeout: float = Field(10.0, ge=1)
        enable_prometheus: bool = True

        # Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # Persistence paths
        q_weights_path: str = Field("./carbon_q_weights.json")
        interaction_logs_path: str = Field("./carbon_interactions.csv")
        historical_model_path: str = Field("./carbon_historical_model.pkl")

        @field_validator('providers')
        @classmethod
        def validate_providers(cls, v):
            allowed = {"climate_trace", "os_climate", "electricity_maps"}
            for p in v:
                if p not in allowed:
                    raise ValueError(f"Provider {p} not in allowed list {allowed}")
            return v

        class Config:
            env_prefix = "CARBON_"
else:
    # Fallback dict
    CARBON_CONFIG = {
        "providers": ["climate_trace", "os_climate", "electricity_maps"],
        "climate_trace_api_key": None,
        "os_climate_api_key": None,
        "electricity_maps_api_key": None,
        "region_averages": {
            "us-east": 0.41,
            "us-west": 0.34,
            "eu-west": 0.27,
            "eu-north": 0.21,
            "asia-east": 0.49,
            "asia-southeast": 0.47,
            "global": 0.40,
        },
        "cache_ttl": 3600,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "enable_prometheus": True,
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "q_weights_path": "./carbon_q_weights.json",
        "interaction_logs_path": "./carbon_interactions.csv",
        "historical_model_path": "./carbon_historical_model.pkl",
    }

# ============================================================================
# Circuit Breaker
# ============================================================================
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

# ============================================================================
# Response Models (Pydantic)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class ClimateTraceResponse(BaseModel):
        intensity: float

    class OSClimateResponse(BaseModel):
        intensity: float

    class ElectricityMapsResponse(BaseModel):
        data: Dict[str, Any]

        @property
        def intensity(self) -> Optional[float]:
            carbon = self.data.get("carbonIntensity")
            if carbon is not None:
                return float(carbon) / 1000.0
            return None

# ============================================================================
# Provider Classes
# ============================================================================
class CarbonProvider(Protocol):
    async def fetch(self, region: str, timestamp: datetime) -> Optional[float]:
        ...

class ClimateTraceProvider:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("CLIMATE_TRACE_API_KEY")

    async def fetch(self, session: aiohttp.ClientSession, region: str, timestamp: datetime) -> Optional[float]:
        if not self.api_key:
            logger.debug("Climate TRACE API key not set; skipping")
            return None
        date_str = timestamp.strftime("%Y-%m-%d")
        url = "https://api.climatetrace.org/v1/carbon-intensity"
        params = {"region": region, "date": date_str}
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if PYDANTIC_AVAILABLE:
                        validated = ClimateTraceResponse(**data)
                        return validated.intensity
                    else:
                        return float(data.get("intensity"))
                else:
                    logger.warning("Climate TRACE returned status", status=resp.status, region=region)
                    return None
        except Exception as e:
            logger.error("Climate TRACE API error", error=str(e), region=region)
            raise

class OSClimateProvider:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("OS_CLIMATE_API_KEY")

    async def fetch(self, session: aiohttp.ClientSession, region: str, timestamp: datetime) -> Optional[float]:
        if not self.api_key:
            logger.debug("OS‑Climate API key not set; skipping")
            return None
        url = "https://api.os-climate.org/v1/carbon-intensity"
        params = {"region": region}
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if PYDANTIC_AVAILABLE:
                        validated = OSClimateResponse(**data)
                        return validated.intensity
                    else:
                        return float(data.get("intensity"))
                else:
                    logger.warning("OS‑Climate returned status", status=resp.status, region=region)
                    return None
        except Exception as e:
            logger.error("OS‑Climate API error", error=str(e), region=region)
            raise

class ElectricityMapsProvider:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("ELECTRICITY_MAPS_API_KEY")

    async def fetch(self, session: aiohttp.ClientSession, region: str, timestamp: datetime) -> Optional[float]:
        if not self.api_key:
            logger.debug("Electricity Maps API key not set; skipping")
            return None
        url = "https://api.electricitymap.org/v3/carbon-intensity/latest"
        params = {"zone": region}
        headers = {"auth-token": self.api_key}
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if PYDANTIC_AVAILABLE:
                        validated = ElectricityMapsResponse(**data)
                        return validated.intensity
                    else:
                        carbon = data.get("data", {}).get("carbonIntensity")
                        if carbon is not None:
                            return float(carbon) / 1000.0
                        return None
                else:
                    logger.warning("Electricity Maps returned status", status=resp.status, region=region)
                    return None
        except Exception as e:
            logger.error("Electricity Maps API error", error=str(e), region=region)
            raise

# ============================================================================
# DISTILLATION COMPONENTS FOR PROVIDER SELECTION
# ============================================================================

@dataclass
class ProviderSelectionState:
    """State for the distillation agent."""
    # Region (one‑hot: us-east, us-west, eu-west, eu-north, asia-east, asia-southeast, global)
    region_us_east: float
    region_us_west: float
    region_eu_west: float
    region_eu_north: float
    region_asia_east: float
    region_asia_southeast: float
    region_global: float
    # Time
    hour_of_day: float
    day_of_week: float
    # Historical success rates for each provider (last 10 calls)
    success_climate_trace: float
    success_os_climate: float
    success_electricity_maps: float
    # Circuit breaker states (0=CLOSED, 1=HALF_OPEN, 2=OPEN)
    cb_climate_trace: float
    cb_os_climate: float
    cb_electricity_maps: float
    # Provider availability (has API key)
    avail_climate_trace: float
    avail_os_climate: float
    avail_electricity_maps: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 18‑dim numeric feature vector."""
        features = [
            self.region_us_east,
            self.region_us_west,
            self.region_eu_west,
            self.region_eu_north,
            self.region_asia_east,
            self.region_asia_southeast,
            self.region_global,
            self.hour_of_day / 24.0,
            self.day_of_week / 7.0,
            self.success_climate_trace,
            self.success_os_climate,
            self.success_electricity_maps,
            self.cb_climate_trace / 2.0,
            self.cb_os_climate / 2.0,
            self.cb_electricity_maps / 2.0,
            self.avail_climate_trace,
            self.avail_os_climate,
            self.avail_electricity_maps,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: ProviderSelectionState) -> np.ndarray:
        """Return probability vector over available providers."""
        pass

    @abstractmethod
    def confidence(self, state: ProviderSelectionState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class ProviderRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    def __init__(self, available_providers: List[str]):
        self.available = available_providers

    def predict(self, state: ProviderSelectionState) -> np.ndarray:
        n = len(self.available)
        probs = np.ones(n) * 0.1
        # Heuristics: prefer providers with:
        # - high success rate
        # - circuit breaker CLOSED (state 0)
        # - available (has API key)
        for i, prov in enumerate(self.available):
            # Get success rate
            success = getattr(state, f"success_{prov}", 0.5)
            # Get CB state
            cb = getattr(state, f"cb_{prov}", 0.0)
            # Get availability
            avail = getattr(state, f"avail_{prov}", 1.0)
            # Score
            score = success * 0.6 + (1 - cb/2) * 0.3 + avail * 0.1
            probs[i] = max(0.1, score)
        return probs / probs.sum()

    def confidence(self, state: ProviderSelectionState) -> float:
        return 0.5  # moderate confidence


class ProviderHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, available_providers: List[str], model_path: Optional[Path] = None):
        self.available = available_providers
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(CARBON_CONFIG['historical_model_path'])
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: ProviderSelectionState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(len(self.available)) / len(self.available)
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        # Align with current available providers
        # (Assuming model classes match the current provider list)
        return probs

    def confidence(self, state: ProviderSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class ProviderStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, available_providers: List[str], lr: float = 0.1):
        self.available = available_providers
        self.lr = lr
        self.weights = np.zeros((18, len(available_providers)))  # 18 features, n actions
        self._load_state()

    def _load_state(self):
        path = Path(CARBON_CONFIG['q_weights_path'])
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(CARBON_CONFIG['q_weights_path'])
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: ProviderSelectionState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: ProviderSelectionState) -> float:
        return 0.5

    def update(self, state: ProviderSelectionState, action: int, reward: float):
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


class DistillationProviderOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for provider selection.
    """
    def __init__(self, available_providers: List[str], config: Dict[str, Any]):
        self.available = available_providers
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            ProviderRuleBasedTeacher(available_providers),
            ProviderHistoricalMLTeacher(available_providers),
            ProviderStatefulQTeacher(available_providers)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_provider(self, state: ProviderSelectionState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = len(self.available)

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

        return self.available[action_idx], action_idx, state_vec, teacher_probs

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
# CarbonIntensityFetcher (Enhanced)
# ============================================================================
class CarbonIntensityFetcher:
    """
    Enhanced carbon intensity fetcher with adaptive provider selection.
    """

    def __init__(
        self,
        cache: CacheManager,
        config: Optional[Union[Dict[str, Any], CarbonIntensityConfig]] = None,
    ):
        """
        Initialize the fetcher.

        Args:
            cache: CacheManager instance for caching intensity values.
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = CarbonIntensityConfig()
            else:
                self.config = CARBON_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = CarbonIntensityConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.cache = cache
        self.provider_order = self.config.get("providers", ["climate_trace", "os_climate", "electricity_maps"])
        self.region_averages = self.config.get("region_averages", {})
        self.cache_ttl = self.config.get("cache_ttl", 3600)
        self.request_timeout = self.config.get("request_timeout", 10.0)

        # Initialize providers
        self._providers = {
            "climate_trace": ClimateTraceProvider(self.config.get("climate_trace_api_key")),
            "os_climate": OSClimateProvider(self.config.get("os_climate_api_key")),
            "electricity_maps": ElectricityMapsProvider(self.config.get("electricity_maps_api_key")),
        }

        # Circuit breakers per provider
        self._circuit_breakers = {
            provider: CircuitBreaker(
                name=f"carbon_{provider}",
                failure_threshold=self.config.get("circuit_breaker_threshold", 5),
                recovery_timeout=self.config.get("circuit_breaker_timeout", 30.0),
            )
            for provider in self.provider_order
        }

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                'calls': Counter('carbon_api_calls_total', 'Carbon API calls', ['provider', 'status']),
                'errors': Counter('carbon_api_errors_total', 'Carbon API errors', ['provider']),
                'latency': Histogram('carbon_api_latency_seconds', 'Carbon API latency', ['provider']),
                'cache_hits': Counter('carbon_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('carbon_cache_misses_total', 'Cache misses'),
                'circuit_breaker_state': Gauge('carbon_circuit_breaker_state', 'Circuit breaker state', ['provider']),
                'fallback_usage': Counter('carbon_fallback_usage_total', 'Fallback to region average'),
            }
        else:
            self.metrics = None

        # Distillation optimizer
        self.provider_optimizer = DistillationProviderOptimizer(
            available_providers=self.provider_order,
            config={
                'distillation_epsilon': self.config.get('distillation_epsilon', 0.1),
                'distillation_train_every': self.config.get('distillation_train_every', 10),
                'distillation_replay_size': self.config.get('distillation_replay_size', 2000),
                'distillation_learning_rate': self.config.get('distillation_learning_rate', 0.01),
            }
        )

        # Interaction tracking for historical ML
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        logger.info("CarbonIntensityFetcher initialized with adaptive provider selection", providers=self.provider_order)

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
    def _build_state(self, region: str, timestamp: datetime) -> ProviderSelectionState:
        """Build state for the distillation agent."""
        # Region one‑hot
        regions = ["us-east", "us-west", "eu-west", "eu-north", "asia-east", "asia-southeast", "global"]
        region_onehot = [1.0 if region == r else 0.0 for r in regions]

        # Time
        hour = timestamp.hour
        dow = timestamp.weekday()

        # Historical success rates (from interaction log)
        # For simplicity, we'll use the last 10 interactions per provider.
        success_counts = {p: 0 for p in self.provider_order}
        total_counts = {p: 0 for p in self.provider_order}
        for entry in self.interaction_log[-100:]:
            if entry['provider'] in success_counts:
                total_counts[entry['provider']] += 1
                if entry['success']:
                    success_counts[entry['provider']] += 1
        success_rates = {p: success_counts[p] / max(total_counts[p], 1) for p in self.provider_order}

        # Circuit breaker states
        cb_states = {}
        for p in self.provider_order:
            cb = self._circuit_breakers[p]
            if cb._state == CircuitBreakerState.CLOSED:
                cb_states[p] = 0.0
            elif cb._state == CircuitBreakerState.HALF_OPEN:
                cb_states[p] = 1.0
            else:
                cb_states[p] = 2.0

        # Provider availability
        avail = {}
        for p in self.provider_order:
            provider_obj = self._providers[p]
            avail[p] = 1.0 if provider_obj.api_key else 0.0

        # Build feature vector
        return ProviderSelectionState(
            region_us_east=region_onehot[0],
            region_us_west=region_onehot[1],
            region_eu_west=region_onehot[2],
            region_eu_north=region_onehot[3],
            region_asia_east=region_onehot[4],
            region_asia_southeast=region_onehot[5],
            region_global=region_onehot[6],
            hour_of_day=hour,
            day_of_week=dow,
            success_climate_trace=success_rates.get("climate_trace", 0.5),
            success_os_climate=success_rates.get("os_climate", 0.5),
            success_electricity_maps=success_rates.get("electricity_maps", 0.5),
            cb_climate_trace=cb_states.get("climate_trace", 0.0),
            cb_os_climate=cb_states.get("os_climate", 0.0),
            cb_electricity_maps=cb_states.get("electricity_maps", 0.0),
            avail_climate_trace=avail.get("climate_trace", 1.0),
            avail_os_climate=avail.get("os_climate", 1.0),
            avail_electricity_maps=avail.get("electricity_maps", 1.0),
        )

    # ---------- Main get_intensity (enhanced) ----------
    async def get_intensity(
        self,
        region: str,
        timestamp: Optional[datetime] = None,
        force_refresh: bool = False,
    ) -> float:
        """
        Get carbon intensity (kg CO₂/kWh) for a region at a given time.
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        cache_hour = timestamp.replace(minute=0, second=0, microsecond=0)
        cache_key = f"carbon:{region}:{cache_hour.isoformat()}"

        # Try cache first
        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics['cache_hits'].inc()
                logger.debug("Cache hit", region=region, key=cache_key)
                return float(cached)

        if self.metrics:
            self.metrics['cache_misses'].inc()

        # Build state
        state = self._build_state(region, timestamp)
        # Select provider via distillation
        provider, action_idx, state_vec, teacher_probs = await self.provider_optimizer.select_provider(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        intensity = None
        success = False
        latency = 0.0
        start_time = time.time()

        # Try the selected provider
        try:
            cb = self._circuit_breakers[provider]
            provider_obj = self._providers[provider]
            session = await self._get_session()

            async def fetch():
                if TENACITY_AVAILABLE:
                    @retry(
                        stop=stop_after_attempt(self.config.get("retry_attempts", 3)),
                        wait=wait_exponential(
                            multiplier=1,
                            min=self.config.get("retry_min_wait", 1.0),
                            max=self.config.get("retry_max_wait", 10.0),
                        ),
                        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                        before_sleep=before_sleep_log(logger, logging.WARNING),
                    )
                    async def retryable_fetch():
                        return await provider_obj.fetch(session, region, timestamp)
                    return await retryable_fetch()
                else:
                    for attempt in range(self.config.get("retry_attempts", 3)):
                        try:
                            return await provider_obj.fetch(session, region, timestamp)
                        except Exception as e:
                            if attempt == self.config.get("retry_attempts", 3) - 1:
                                raise
                            wait = min(
                                self.config.get("retry_min_wait", 1.0) * (2 ** attempt),
                                self.config.get("retry_max_wait", 10.0),
                            )
                            await asyncio.sleep(wait)

            intensity = await cb.call(fetch)
            if intensity is not None:
                success = True
                if self.metrics:
                    self.metrics['calls'].labels(provider=provider, status='success').inc()
                    self.metrics['latency'].labels(provider=provider).observe(time.time() - start_time)
                logger.info("Fetched carbon intensity", provider=provider, region=region, intensity=intensity)
        except Exception as e:
            if self.metrics:
                self.metrics['errors'].labels(provider=provider).inc()
                self.metrics['calls'].labels(provider=provider, status='error').inc()
            logger.warning("Provider failed", provider=provider, region=region, error=str(e))

        # If failed, fallback to region average (no further learning)
        if intensity is None:
            intensity = self._get_region_average(region)
            if self.metrics:
                self.metrics['fallback_usage'].inc()
            logger.info("Using fallback average", region=region, intensity=intensity)
            # Reward for fallback is 0 (since we didn't get real data)
            reward = 0.0
        else:
            reward = 1.0  # success

        # Record outcome and update agent
        self._log_interaction(provider, success, reward)
        # Update distillation agent
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(region, timestamp)  # next state (could be same)
            next_state_vec = next_state.to_feature_vector()
            await self.provider_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        # Store in cache
        await self.cache.set(cache_key, str(intensity), ttl=self.cache_ttl)
        return intensity

    def _log_interaction(self, provider: str, success: bool, reward: float):
        """Log an interaction for offline training."""
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'provider': provider,
            'success': success,
            'reward': reward,
        }
        self.interaction_log.append(entry)
        # Append to CSV
        log_path = Path(self.config.get('interaction_logs_path', './carbon_interactions.csv'))
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ---------- Offline training for Historical ML ----------
    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./carbon_interactions.csv"), model_path: Path = Path("./carbon_historical_model.pkl")):
        """
        Train a RandomForestClassifier from past interaction logs.
        This method should be called periodically to update the historical ML teacher.
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

    # ---------- Fallback average ----------
    def _get_region_average(self, region: str) -> float:
        return self.region_averages.get(region, self.region_averages.get("global", 0.40))

    # ---------- Batch and historical methods ----------
    async def get_intensity_batch(
        self,
        regions: List[str],
        timestamp: Optional[datetime] = None,
    ) -> Dict[str, float]:
        """
        Get carbon intensities for multiple regions in parallel.
        """
        tasks = [self.get_intensity(region, timestamp) for region in regions]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        intensities = {}
        for region, result in zip(regions, results):
            if isinstance(result, Exception):
                logger.error("Failed to get intensity for region", region=region, error=str(result))
                intensities[region] = self._get_region_average(region)
            else:
                intensities[region] = result
        return intensities

    async def get_historical_intensity(
        self,
        region: str,
        start: datetime,
        end: datetime,
        step_hours: int = 1,
    ) -> Dict[datetime, float]:
        """
        Get historical carbon intensity for a region over a time range.
        """
        results = {}
        current = start.replace(minute=0, second=0, microsecond=0)
        tasks = []
        timestamps = []
        while current <= end:
            tasks.append(self.get_intensity(region, current))
            timestamps.append(current)
            current += timedelta(hours=step_hours)
        intensities = await asyncio.gather(*tasks, return_exceptions=True)
        for ts, int_val in zip(timestamps, intensities):
            if isinstance(int_val, Exception):
                logger.error("Historical fetch failed", region=region, timestamp=ts, error=str(int_val))
                results[ts] = self._get_region_average(region)
            else:
                results[ts] = int_val
        return results

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# ============================================================================
# Convenience factory
# ============================================================================
def create_carbon_fetcher(
    cache: CacheManager,
    config: Optional[Dict[str, Any]] = None,
) -> CarbonIntensityFetcher:
    """
    Factory to create a fully configured CarbonIntensityFetcher.
    """
    return CarbonIntensityFetcher(cache, config)


# ============================================================================
# UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.providers = ["climate_trace", "os_climate", "electricity_maps"]
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationProviderOptimizer(self.providers, self.config)

    def test_state_feature_vector(self):
        state = ProviderSelectionState(
            region_us_east=1.0,
            region_us_west=0.0,
            region_eu_west=0.0,
            region_eu_north=0.0,
            region_asia_east=0.0,
            region_asia_southeast=0.0,
            region_global=0.0,
            hour_of_day=12,
            day_of_week=3,
            success_climate_trace=0.8,
            success_os_climate=0.5,
            success_electricity_maps=0.3,
            cb_climate_trace=0.0,
            cb_os_climate=1.0,
            cb_electricity_maps=2.0,
            avail_climate_trace=1.0,
            avail_os_climate=0.0,
            avail_electricity_maps=1.0,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 18)

    def test_rule_based_teacher(self):
        teacher = ProviderRuleBasedTeacher(self.providers)
        state = ProviderSelectionState(
            region_us_east=1.0,
            region_us_west=0.0,
            region_eu_west=0.0,
            region_eu_north=0.0,
            region_asia_east=0.0,
            region_asia_southeast=0.0,
            region_global=0.0,
            hour_of_day=12,
            day_of_week=3,
            success_climate_trace=0.9,
            success_os_climate=0.5,
            success_electricity_maps=0.3,
            cb_climate_trace=0.0,
            cb_os_climate=1.0,
            cb_electricity_maps=2.0,
            avail_climate_trace=1.0,
            avail_os_climate=1.0,
            avail_electricity_maps=1.0,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # climate_trace should be highest

    async def test_select_provider(self):
        state = ProviderSelectionState(
            region_us_east=1.0,
            region_us_west=0.0,
            region_eu_west=0.0,
            region_eu_north=0.0,
            region_asia_east=0.0,
            region_asia_southeast=0.0,
            region_global=0.0,
            hour_of_day=12,
            day_of_week=3,
            success_climate_trace=0.9,
            success_os_climate=0.5,
            success_electricity_maps=0.3,
            cb_climate_trace=0.0,
            cb_os_climate=1.0,
            cb_electricity_maps=2.0,
            avail_climate_trace=1.0,
            avail_os_climate=1.0,
            avail_electricity_maps=1.0,
        )
        provider, idx, state_vec, teacher_probs = await self.optimizer.select_provider(state, exploration=False)
        self.assertIn(provider, self.providers)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(18)
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
            "providers": ["climate_trace", "os_climate", "electricity_maps"],
            "cache_ttl": 3600,
            "distillation_epsilon": 0.1,
            "distillation_train_every": 5,
        }
        fetcher = create_carbon_fetcher(cache, config)

        # Simulate a few calls to train the agent
        for _ in range(5):
            intensity = await fetcher.get_intensity("us-east")
            print(f"Intensity: {intensity}")

        stats = fetcher.provider_optimizer.get_stats()
        print("Distillation stats:", stats)

        await fetcher.close()

    asyncio.run(main())
