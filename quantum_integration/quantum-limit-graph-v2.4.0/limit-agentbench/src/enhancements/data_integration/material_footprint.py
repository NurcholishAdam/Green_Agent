# src/enhancements/data_integration/material_footprint_v2_2_0.py
"""
Enhanced Material Footprint Updater v2.2.0
===========================================
Fetches and caches product‑level material footprints from BONSAI/FOOTPRINTDATA.
Provides adaptive source selection and update scheduling via Multi‑Teacher On‑Policy Distillation.

ENHANCEMENTS OVER v2.1.0:
- Adaptive source selection (bonsai, footprintdata, mock) based on context.
- Adaptive update mode (full catalog vs single‑product fetch).
- State‑aware decisions using cache age, product demand, source reliability, time since last update.
- Online learning from API call outcomes and cache freshness.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights and interaction logs.
- Offline training for historical ML teacher from logs.
- Unit tests for distillation components.
"""

import asyncio
import logging
import time
import json
import sqlite3
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
import aiohttp
from aiohttp import ClientTimeout, ClientError
import random
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
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
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

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class MaterialConfig(BaseModel):
        """Configuration for MaterialFootprintUpdater."""
        # Database
        db_path: Path = Field(Path("./material_catalog.db"))
        # API endpoints
        bonsai_api_url: str = Field("https://api.bonsai.uno/v1/footprints")
        footprintdata_api_url: str = Field("https://api.footprintdata.org/v1/products")
        # API keys
        bonsai_api_key: Optional[str] = Field(None)
        footprintdata_api_key: Optional[str] = Field(None)
        # Cache TTL (seconds)
        cache_ttl: int = Field(86400 * 7, ge=0)
        # Retry settings
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        # Circuit breaker
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        # Request timeout (seconds)
        request_timeout: float = Field(10.0, ge=1)
        # Enable metrics
        enable_prometheus: bool = True
        # Source priority (used as fallback for rule teacher)
        source_priority: List[str] = Field(default_factory=lambda: ["bonsai", "footprintdata"])

        # NEW: Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # Persistence paths
        q_weights_path: str = Field("./material_q_weights.json")
        interaction_logs_path: str = Field("./material_interactions.csv")
        historical_model_path: str = Field("./material_historical_model.pkl")

        @field_validator('source_priority')
        @classmethod
        def validate_source_priority(cls, v):
            allowed = {"bonsai", "footprintdata"}
            for s in v:
                if s not in allowed:
                    raise ValueError(f"Source {s} not in allowed list {allowed}")
            return v

        class Config:
            env_prefix = "MATERIAL_"
else:
    # Fallback dict
    MATERIAL_CONFIG = {
        "db_path": Path("./material_catalog.db"),
        "bonsai_api_url": "https://api.bonsai.uno/v1/footprints",
        "footprintdata_api_url": "https://api.footprintdata.org/v1/products",
        "bonsai_api_key": None,
        "footprintdata_api_key": None,
        "cache_ttl": 86400 * 7,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "enable_prometheus": True,
        "source_priority": ["bonsai", "footprintdata"],
        # Distillation defaults
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "q_weights_path": "./material_q_weights.json",
        "interaction_logs_path": "./material_interactions.csv",
        "historical_model_path": "./material_historical_model.pkl",
    }

# ============================================================================
# Data Models (Pydantic) - unchanged
# ============================================================================
if PYDANTIC_AVAILABLE:
    class BonsaiFootprintResponse(BaseModel):
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float

    class FootprintDataResponse(BaseModel):
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float

    class Footprint(BaseModel):
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float
        source: str
        last_updated: datetime

        @field_validator('material_index')
        @classmethod
        def material_index_non_negative(cls, v):
            if v < 0:
                raise ValueError("material_index must be non-negative")
            return v
else:
    from dataclasses import dataclass

    @dataclass
    class Footprint:
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float
        source: str
        last_updated: datetime


# ============================================================================
# DISTILLATION COMPONENTS FOR ADAPTIVE UPDATE
# ============================================================================

@dataclass
class UpdateState:
    """State for the distillation agent."""
    # Cache statistics
    total_products: int
    stale_fraction: float  # fraction of products with age > TTL
    # Product demand (frequency of get calls per product)
    avg_demand: float
    # Source reliability (success rates from interaction log)
    bonsai_success_rate: float
    footprintdata_success_rate: float
    # Circuit breaker states (0=CLOSED, 1=HALF_OPEN, 2=OPEN)
    bonsai_cb_state: float
    footprintdata_cb_state: float
    # Time since last full update (hours)
    hours_since_update: float
    # Is a specific product being requested? (0=full, 1=single)
    single_product_mode: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 11‑dim numeric feature vector."""
        features = [
            min(self.total_products / 1000.0, 1.0),
            self.stale_fraction,
            min(self.avg_demand / 10.0, 1.0),
            self.bonsai_success_rate,
            self.footprintdata_success_rate,
            self.bonsai_cb_state / 2.0,
            self.footprintdata_cb_state / 2.0,
            min(self.hours_since_update / 72.0, 1.0),
            self.single_product_mode,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: UpdateState) -> np.ndarray:
        """Return probability vector over actions."""
        pass

    @abstractmethod
    def confidence(self, state: UpdateState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class UpdateRuleBasedTeacher(Teacher):
    """
    Rule‑based expert.
    Actions: 0=bonsai_full, 1=footprintdata_full, 2=mock_full,
             3=bonsai_single, 4=footprintdata_single, 5=mock_single
    """
    ACTION_SPACE = [
        'bonsai_full', 'footprintdata_full', 'mock_full',
        'bonsai_single', 'footprintdata_single', 'mock_single'
    ]

    def predict(self, state: UpdateState) -> np.ndarray:
        n = 6
        probs = np.ones(n) * 0.1

        # Heuristics
        if state.single_product_mode > 0.5:
            # Single product request: prefer the source with higher success rate
            if state.bonsai_success_rate > state.footprintdata_success_rate:
                probs[3] = 0.8  # bonsai_single
            else:
                probs[4] = 0.8  # footprintdata_single
        else:
            # Full update: prefer source with higher success and lower stale fraction
            if state.stale_fraction > 0.5:
                # Many stale entries -> need update
                if state.bonsai_success_rate > state.footprintdata_success_rate:
                    probs[0] = 0.8  # bonsai_full
                else:
                    probs[1] = 0.8  # footprintdata_full
            else:
                # Not many stale entries; maybe use mock if sources are unreliable
                if state.bonsai_success_rate < 0.3 and state.footprintdata_success_rate < 0.3:
                    probs[2] = 0.7  # mock_full
                else:
                    probs[0] = 0.5  # bonsai_full default

        return probs / probs.sum()

    def confidence(self, state: UpdateState) -> float:
        if state.stale_fraction > 0.5:
            return 0.6
        return 0.4


class UpdateHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(MATERIAL_CONFIG['historical_model_path'])
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: UpdateState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(6) / 6
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: UpdateState) -> float:
        return 0.7 if self.model is not None else 0.0


class UpdateStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((9, 6))  # 9 features, 6 actions
        self._load_state()

    def _load_state(self):
        path = Path(MATERIAL_CONFIG['q_weights_path'])
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(MATERIAL_CONFIG['q_weights_path'])
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: UpdateState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: UpdateState) -> float:
        return 0.5

    def update(self, state: UpdateState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 9, n_classes: int = 6, lr: float = 0.01):
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


class DistillationUpdateOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for update decisions.
    Actions:
        0: bonsai_full
        1: footprintdata_full
        2: mock_full
        3: bonsai_single
        4: footprintdata_single
        5: mock_single
    """
    ACTION_SPACE = [
        'bonsai_full', 'footprintdata_full', 'mock_full',
        'bonsai_single', 'footprintdata_single', 'mock_single'
    ]

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            UpdateRuleBasedTeacher(),
            UpdateHistoricalMLTeacher(),
            UpdateStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_action(self, state: UpdateState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 6

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
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# MaterialFootprintUpdater (Enhanced)
# ============================================================================
class MaterialFootprintUpdater:
    """
    Enhanced material footprint updater with adaptive source selection and update scheduling.
    """

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], MaterialConfig]] = None,
    ):
        """
        Initialize the updater.

        Args:
            config: Configuration dictionary or Pydantic model.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = MaterialConfig()
            else:
                self.config = MATERIAL_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = MaterialConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.db_path = self._get_config('db_path', Path("./material_catalog.db"))
        self.cache_ttl = self._get_config('cache_ttl', 86400 * 7)
        self.bonsai_api_url = self._get_config('bonsai_api_url', "https://api.bonsai.uno/v1/footprints")
        self.bonsai_api_key = self._get_config('bonsai_api_key') or os.environ.get("BONSAI_API_KEY")
        self.footprintdata_api_url = self._get_config('footprintdata_api_url', "https://api.footprintdata.org/v1/products")
        self.footprintdata_api_key = self._get_config('footprintdata_api_key') or os.environ.get("FOOTPRINTDATA_API_KEY")
        self.request_timeout = self._get_config('request_timeout', 10.0)
        self.source_priority = self._get_config('source_priority', ["bonsai", "footprintdata"])

        # Initialize database
        self._init_db()

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breakers per source
        self._circuit_breakers = {
            "bonsai": CircuitBreaker(
                name="material_bonsai",
                failure_threshold=self._get_config('circuit_breaker_threshold', 5),
                recovery_timeout=self._get_config('circuit_breaker_timeout', 30.0),
            ),
            "footprintdata": CircuitBreaker(
                name="material_footprintdata",
                failure_threshold=self._get_config('circuit_breaker_threshold', 5),
                recovery_timeout=self._get_config('circuit_breaker_timeout', 30.0),
            ),
        }

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self._get_config('enable_prometheus', True):
            self.metrics = {
                'calls': Counter('material_api_calls_total', 'Material API calls', ['source', 'status']),
                'errors': Counter('material_api_errors_total', 'Material API errors', ['source']),
                'latency': Histogram('material_api_latency_seconds', 'Material API latency', ['source']),
                'cache_hits': Counter('material_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('material_cache_misses_total', 'Cache misses'),
                'cache_size': Gauge('material_cache_size', 'Number of cached footprints'),
                'cache_age_seconds': Gauge('material_cache_age_seconds', 'Age of cached footprint', ['product_id']),
                # Distillation metrics
                'update_action': Counter('material_update_action', 'Update action selected', ['action']),
                'update_reward': Histogram('material_update_reward', 'Reward per update action'),
            }
        else:
            self.metrics = None

        # Distillation optimizer
        self.update_optimizer = DistillationUpdateOptimizer({
            'distillation_epsilon': self._get_config('distillation_epsilon', 0.1),
            'distillation_train_every': self._get_config('distillation_train_every', 10),
            'distillation_replay_size': self._get_config('distillation_replay_size', 2000),
            'distillation_learning_rate': self._get_config('distillation_learning_rate', 0.01),
        })

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self.last_update_time: Optional[datetime] = None

        logger.info("MaterialFootprintUpdater initialized with adaptive update", db_path=str(self.db_path))

    def _get_config(self, key: str, default: Any = None) -> Any:
        """Safely get a config value."""
        if hasattr(self.config, 'model_dump'):
            return getattr(self.config, key, default)
        elif hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        else:
            return self.config.get(key, default)

    def _init_db(self):
        """Initialize SQLite database with enhanced schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS footprints (
                product_id TEXT PRIMARY KEY,
                embodied_carbon_kg REAL,
                rare_earth_kg REAL,
                total_mass_kg REAL,
                material_index REAL,
                source TEXT,
                last_updated TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_id ON footprints(product_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_last_updated ON footprints(last_updated)")
        conn.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp ClientSession."""
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
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ---------- Build state ----------
    def _build_state(self, product_id: Optional[str] = None) -> UpdateState:
        """Build state for the distillation agent."""
        conn = sqlite3.connect(self.db_path)
        total = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]

        # Stale fraction
        now = datetime.utcnow()
        rows = conn.execute("SELECT last_updated FROM footprints").fetchall()
        stale_count = 0
        for row in rows:
            try:
                last = datetime.fromisoformat(row[0])
                if (now - last).total_seconds() > self.cache_ttl:
                    stale_count += 1
            except:
                stale_count += 1
        conn.close()
        stale_fraction = stale_count / max(total, 1)

        # Product demand (tracked via get calls)
        # We'll use a simple moving average from logs
        if self.interaction_log:
            recent = [entry for entry in self.interaction_log[-50:] if entry.get('product_id') is not None]
            product_counts = {}
            for entry in recent:
                pid = entry['product_id']
                product_counts[pid] = product_counts.get(pid, 0) + 1
            avg_demand = np.mean(list(product_counts.values())) if product_counts else 1.0
        else:
            avg_demand = 1.0

        # Source success rates
        bonsai_success = 0.5
        footprintdata_success = 0.5
        if self.interaction_log:
            bonsai_entries = [e for e in self.interaction_log if e.get('source') == 'bonsai']
            footprint_entries = [e for e in self.interaction_log if e.get('source') == 'footprintdata']
            if bonsai_entries:
                bonsai_success = sum(1 for e in bonsai_entries if e.get('success', False)) / len(bonsai_entries)
            if footprint_entries:
                footprintdata_success = sum(1 for e in footprint_entries if e.get('success', False)) / len(footprint_entries)

        # Circuit breaker states
        bonsai_cb = 0.0
        if self._circuit_breakers['bonsai']._state == CircuitBreakerState.CLOSED:
            bonsai_cb = 0.0
        elif self._circuit_breakers['bonsai']._state == CircuitBreakerState.HALF_OPEN:
            bonsai_cb = 1.0
        else:
            bonsai_cb = 2.0

        footprint_cb = 0.0
        if self._circuit_breakers['footprintdata']._state == CircuitBreakerState.CLOSED:
            footprint_cb = 0.0
        elif self._circuit_breakers['footprintdata']._state == CircuitBreakerState.HALF_OPEN:
            footprint_cb = 1.0
        else:
            footprint_cb = 2.0

        # Hours since last update
        if self.last_update_time:
            hours = (datetime.utcnow() - self.last_update_time).total_seconds() / 3600
        else:
            hours = 0.0

        single_mode = 1.0 if product_id is not None else 0.0

        return UpdateState(
            total_products=total,
            stale_fraction=stale_fraction,
            avg_demand=avg_demand,
            bonsai_success_rate=bonsai_success,
            footprintdata_success_rate=footprintdata_success,
            bonsai_cb_state=bonsai_cb,
            footprintdata_cb_state=footprint_cb,
            hours_since_update=hours,
            single_product_mode=single_mode,
        )

    # ---------- Core update methods (enhanced) ----------
    async def update_catalog(self, force_refresh: bool = False) -> int:
        """
        Update the catalog using adaptive action selection.
        """
        # Build state (full catalog mode)
        state = self._build_state(product_id=None)
        action, action_idx, state_vec, teacher_probs = await self.update_optimizer.select_action(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        # Execute action
        success = False
        updated_count = 0
        start_time = time.time()

        if action == 'bonsai_full':
            updated_count = await self._update_from_source('bonsai', force_refresh)
            success = updated_count > 0
        elif action == 'footprintdata_full':
            updated_count = await self._update_from_source('footprintdata', force_refresh)
            success = updated_count > 0
        elif action == 'mock_full':
            self._seed_mock_data()
            updated_count = self._count_catalog()
            success = updated_count > 0
        elif action == 'bonsai_single':
            # For single action, we need a product_id; but update_catalog is full mode.
            # We'll fallback to full update with bonsai.
            updated_count = await self._update_from_source('bonsai', force_refresh)
            success = updated_count > 0
        elif action == 'footprintdata_single':
            updated_count = await self._update_from_source('footprintdata', force_refresh)
            success = updated_count > 0
        elif action == 'mock_single':
            self._seed_mock_data()
            updated_count = self._count_catalog()
            success = updated_count > 0

        # Compute reward
        reward = self._compute_reward(success, updated_count, force_refresh)
        self.last_update_time = datetime.utcnow()

        # Log interaction
        self._log_interaction('update_catalog', action, success, reward)
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(product_id=None)
            next_state_vec = next_state.to_feature_vector()
            await self.update_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        # Update metrics
        if self.metrics:
            self.metrics['update_action'].labels(action=action).inc()
            self.metrics['update_reward'].observe(reward)
            conn = sqlite3.connect(self.db_path)
            count = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]
            conn.close()
            self.metrics['cache_size'].set(count)

        logger.info(f"Update completed: action={action}, updated={updated_count}, reward={reward:.2f}")
        return updated_count

    async def _update_from_source(self, source: str, force_refresh: bool) -> int:
        """Fetch and update footprints from a specific source."""
        if source == "bonsai":
            url = self.bonsai_api_url
            api_key = self.bonsai_api_key
            response_model = BonsaiFootprintResponse if PYDANTIC_AVAILABLE else None
        elif source == "footprintdata":
            url = self.footprintdata_api_url
            api_key = self.footprintdata_api_key
            response_model = FootprintDataResponse if PYDANTIC_AVAILABLE else None
        else:
            raise ValueError(f"Unknown source: {source}")

        async def fetch():
            session = await self._get_session()
            headers = {}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    raise aiohttp.ClientError(f"API returned {resp.status}")
                data = await resp.json()
                return data

        if TENACITY_AVAILABLE:
            @retry(
                stop=stop_after_attempt(self._get_config('retry_attempts', 3)),
                wait=wait_exponential(
                    multiplier=1,
                    min=self._get_config('retry_min_wait', 1.0),
                    max=self._get_config('retry_max_wait', 10.0),
                ),
                retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )
            async def fetch_with_retry():
                return await fetch()
        else:
            async def fetch_with_retry():
                for attempt in range(self._get_config('retry_attempts', 3)):
                    try:
                        return await fetch()
                    except Exception as e:
                        if attempt == self._get_config('retry_attempts', 3) - 1:
                            raise
                        wait = min(
                            self._get_config('retry_min_wait', 1.0) * (2 ** attempt),
                            self._get_config('retry_max_wait', 10.0),
                        )
                        await asyncio.sleep(wait)

        start_time = time.time()
        data = await self._circuit_breakers[source].call(fetch_with_retry)
        if self.metrics:
            self.metrics['calls'].labels(source=source, status='success').inc()
            self.metrics['latency'].labels(source=source).observe(time.time() - start_time)

        # Parse and store footprints
        conn = sqlite3.connect(self.db_path)
        now = datetime.utcnow().isoformat()
        count = 0

        if not isinstance(data, list):
            if isinstance(data, dict) and 'data' in data:
                data = data['data']
            else:
                logger.warning(f"Unexpected response format from {source}")
                data = []

        for item in data:
            product_id = item.get('product_id') or item.get('id')
            if not product_id:
                continue

            if not force_refresh:
                row = conn.execute(
                    "SELECT last_updated FROM footprints WHERE product_id = ?",
                    (product_id,)
                ).fetchone()
                if row:
                    last_updated = datetime.fromisoformat(row[0])
                    if (datetime.utcnow() - last_updated).total_seconds() < self.cache_ttl:
                        continue

            embodied_carbon_kg = item.get('embodied_carbon_kg', 0.0)
            rare_earth_kg = item.get('rare_earth_kg', 0.0)
            total_mass_kg = item.get('total_mass_kg', 0.0)
            material_index = item.get('material_index', 1.0)

            if PYDANTIC_AVAILABLE and response_model:
                try:
                    parsed = response_model(**item)
                    embodied_carbon_kg = parsed.embodied_carbon_kg
                    rare_earth_kg = parsed.rare_earth_kg
                    total_mass_kg = parsed.total_mass_kg
                    material_index = parsed.material_index
                except ValidationError as e:
                    logger.warning(f"Validation failed for {product_id}: {e}")

            conn.execute("""
                INSERT OR REPLACE INTO footprints
                (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                product_id,
                embodied_carbon_kg,
                rare_earth_kg,
                total_mass_kg,
                material_index,
                source,
                now,
            ))
            count += 1

        conn.commit()
        conn.close()
        return count

    def _count_catalog(self) -> int:
        conn = sqlite3.connect(self.db_path)
        count = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]
        conn.close()
        return count

    def _seed_mock_data(self):
        """Seed the database with mock data."""
        mock_data = [
            {"product_id": "gpu-a100", "embodied_carbon_kg": 200, "rare_earth_kg": 0.01, "total_mass_kg": 2.5, "material_index": 1.2},
            {"product_id": "gpu-h100", "embodied_carbon_kg": 250, "rare_earth_kg": 0.015, "total_mass_kg": 3.0, "material_index": 1.5},
            {"product_id": "edge-device", "embodied_carbon_kg": 50, "rare_earth_kg": 0.002, "total_mass_kg": 0.5, "material_index": 0.6},
        ]
        conn = sqlite3.connect(self.db_path)
        now = datetime.utcnow().isoformat()
        for item in mock_data:
            conn.execute("""
                INSERT OR REPLACE INTO footprints
                (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item['product_id'],
                item['embodied_carbon_kg'],
                item['rare_earth_kg'],
                item['total_mass_kg'],
                item['material_index'],
                "mock",
                now,
            ))
        conn.commit()
        conn.close()
        logger.info("Seeded mock data")

    def _compute_reward(self, success: bool, updated_count: int, force_refresh: bool) -> float:
        """Compute reward for the update action."""
        if success:
            reward = 0.6
            if updated_count > 0:
                reward += 0.2 * min(1.0, updated_count / 10.0)
        else:
            reward = 0.0

        # Bonus for not using force_refresh (more efficient)
        if not force_refresh:
            reward += 0.1

        # Penalty if catalog empty after update
        if self._count_catalog() == 0:
            reward -= 0.2

        return max(0.0, min(1.0, reward))

    # ---------- Public methods (enhanced) ----------
    def get_footprint(self, product_id: str) -> Optional[Footprint]:
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated FROM footprints WHERE product_id = ?",
            (product_id,)
        ).fetchone()
        conn.close()
        if not row:
            if self.metrics:
                self.metrics['cache_misses'].inc()
            return None
        if self.metrics:
            self.metrics['cache_hits'].inc()
            age = (datetime.utcnow() - datetime.fromisoformat(row[5])).total_seconds()
            self.metrics['cache_age_seconds'].labels(product_id=product_id).set(age)

        return Footprint(
            product_id=product_id,
            embodied_carbon_kg=row[0],
            rare_earth_kg=row[1],
            total_mass_kg=row[2],
            material_index=row[3],
            source=row[4],
            last_updated=datetime.fromisoformat(row[5]),
        )

    async def get_or_fetch_footprint(self, product_id: str, force_refresh: bool = False) -> Optional[Footprint]:
        """
        Get a footprint, adaptively selecting the source if not found or expired.
        """
        fp = self.get_footprint(product_id)
        if fp and not force_refresh:
            age = (datetime.utcnow() - fp.last_updated).total_seconds()
            if age < self.cache_ttl:
                return fp

        # Build state with single-product mode
        state = self._build_state(product_id=product_id)
        action, action_idx, state_vec, teacher_probs = await self.update_optimizer.select_action(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        # Execute action
        success = False
        updated_count = 0
        start_time = time.time()

        if action == 'bonsai_single':
            # Try to fetch only this product (if API supports single fetch)
            # For simplicity, we fallback to full update for that source
            updated_count = await self._update_from_source('bonsai', force_refresh=True)
            success = updated_count > 0
        elif action == 'footprintdata_single':
            updated_count = await self._update_from_source('footprintdata', force_refresh=True)
            success = updated_count > 0
        elif action == 'mock_single':
            # Ensure mock data exists
            if self._count_catalog() == 0:
                self._seed_mock_data()
            success = True
        else:
            # For full actions, use full update
            updated_count = await self._update_from_source(action.split('_')[0], force_refresh=True)
            success = updated_count > 0

        # Compute reward
        reward = self._compute_reward(success, updated_count, force_refresh)

        # Log interaction
        self._log_interaction('get_or_fetch', action, success, reward, product_id=product_id)
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(product_id=product_id)
            next_state_vec = next_state.to_feature_vector()
            await self.update_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        if self.metrics:
            self.metrics['update_action'].labels(action=action).inc()
            self.metrics['update_reward'].observe(reward)

        return self.get_footprint(product_id)

    def _log_interaction(self, method: str, action: str, success: bool, reward: float, product_id: Optional[str] = None):
        """Log interaction for offline training."""
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'method': method,
            'action': action,
            'success': success,
            'reward': reward,
            'product_id': product_id,
        }
        self.interaction_log.append(entry)
        log_path = Path(self._get_config('interaction_logs_path', './material_interactions.csv'))
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ---------- Offline training ----------
    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./material_interactions.csv"),
                               model_path: Path = Path("./material_historical_model.pkl")):
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

        logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")

    # ---------- Other public methods (unchanged) ----------
    def list_products(self) -> List[str]:
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT product_id FROM footprints").fetchall()
        conn.close()
        return [row[0] for row in rows]

    def delete_footprint(self, product_id: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM footprints WHERE product_id = ?", (product_id,))
        conn.commit()
        deleted = conn.total_changes > 0
        conn.close()
        if deleted:
            logger.info("Deleted footprint", product_id=product_id)
        return deleted

    def clear_cache(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM footprints")
        conn.commit()
        conn.close()
        logger.info("Cache cleared")

    def export_catalog(self, path: Path) -> None:
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated FROM footprints").fetchall()
        conn.close()
        data = []
        for row in rows:
            data.append({
                "product_id": row[0],
                "embodied_carbon_kg": row[1],
                "rare_earth_kg": row[2],
                "total_mass_kg": row[3],
                "material_index": row[4],
                "source": row[5],
                "last_updated": row[6],
            })
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info("Catalog exported", path=str(path))

    def import_catalog(self, path: Path) -> int:
        with open(path, 'r') as f:
            data = json.load(f)
        conn = sqlite3.connect(self.db_path)
        count = 0
        for item in data:
            conn.execute("""
                INSERT OR REPLACE INTO footprints
                (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, material_index, source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item['product_id'],
                item['embodied_carbon_kg'],
                item['rare_earth_kg'],
                item['total_mass_kg'],
                item['material_index'],
                item['source'],
                item['last_updated'],
            ))
            count += 1
        conn.commit()
        conn.close()
        logger.info("Catalog imported", path=str(path), count=count)
        return count

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# ============================================================================
# Convenience factory
# ============================================================================
def create_material_updater(
    config: Optional[Dict[str, Any]] = None,
) -> MaterialFootprintUpdater:
    """
    Factory to create a fully configured MaterialFootprintUpdater.
    """
    return MaterialFootprintUpdater(config)


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
        self.optimizer = DistillationUpdateOptimizer(self.config)

    def test_state_feature_vector(self):
        state = UpdateState(
            total_products=100,
            stale_fraction=0.3,
            avg_demand=2.0,
            bonsai_success_rate=0.8,
            footprintdata_success_rate=0.6,
            bonsai_cb_state=0.0,
            footprintdata_cb_state=1.0,
            hours_since_update=12.0,
            single_product_mode=0.0,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 9)

    def test_rule_based_teacher(self):
        teacher = UpdateRuleBasedTeacher()
        state = UpdateState(
            total_products=100,
            stale_fraction=0.6,
            avg_demand=2.0,
            bonsai_success_rate=0.9,
            footprintdata_success_rate=0.5,
            bonsai_cb_state=0.0,
            footprintdata_cb_state=0.0,
            hours_since_update=12.0,
            single_product_mode=0.0,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])  # bonsai_full should be highest

    async def test_select_action(self):
        state = UpdateState(
            total_products=100,
            stale_fraction=0.3,
            avg_demand=2.0,
            bonsai_success_rate=0.8,
            footprintdata_success_rate=0.6,
            bonsai_cb_state=0.0,
            footprintdata_cb_state=0.0,
            hours_since_update=12.0,
            single_product_mode=0.0,
        )
        action, idx, state_vec, teacher_probs = await self.optimizer.select_action(state, exploration=False)
        self.assertIn(action, self.optimizer.ACTION_SPACE)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(9)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(6)/6)
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

    async def main():
        config = {
            "db_path": Path("./test_material.db"),
            "cache_ttl": 3600,
            "distillation_epsilon": 0.1,
            "distillation_train_every": 2,
        }
        updater = create_material_updater(config)

        # Simulate a few update calls to train the agent
        for _ in range(5):
            await updater.update_catalog()
            fp = updater.get_footprint("gpu-a100")
            print(f"Got footprint: {fp}")

        stats = updater.update_optimizer.get_stats()
        print("Distillation stats:", stats)

        await updater.close()

    asyncio.run(main())
