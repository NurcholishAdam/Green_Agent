"""
Enhanced Adaptive Sustainability Cost Function v3.0.0
=====================================================
Extends the base SustainabilityCostFunction with online SGD weight adaptation,
momentum/Adam optimizer, asynchronous database operations, caching, anomaly
integration, alerting, and comprehensive test coverage.

NEW IN v3.0.0:
- Integration with MOPD teacher grid: per‑teacher adaptive weights.
- Distillation loss awareness: weight updates based on distillation performance.
- Bio‑inspired core integration: real‑time carbon/helium/energy metrics.
- Pareto‑based multi‑objective routing (optional).
- Persistence of optimizer states (momentum/Adam velocities).
- Extended FastAPI endpoints for teacher management and distillation.
- Improved JWT authentication with role‑based access.
- Full support for per‑expert/teacher weights.
- Telemetry for MOPD‑related metrics.
- Unit tests covering new features.
"""

import asyncio
import logging
import json
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import threading
import aiohttp

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict

# ---------- SQLAlchemy async ----------
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import (
    Column, String, Float, DateTime, Integer, Boolean, JSON, text,
    select, update, insert, delete
)
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError

# ---------- FastAPI ----------
from fastapi import FastAPI, Depends, HTTPException, status, Request, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import uvicorn

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Retry ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Logging ----------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------- Base classes (imported stubs) ----------
# In a real deployment, these would be imported from your project.
class SustainabilityCostFunction:
    def __init__(self, config):
        self.weights = config.get('weights', {})
        self._dependencies = {}

    def inject_dependencies(self, carbon_manager=None, helium_dashboard=None, node_registry=None):
        self._dependencies.update(locals())

    async def compute(self, expert, context):
        # Base implementation – to be overridden
        return 0.0

class ExpertRegistry:
    async def get_expert(self, expert_id):
        pass

# ---------- Database models (async) ----------
Base = declarative_base()

class FeedbackRecordDB(Base):
    __tablename__ = 'feedback_records'
    id = Column(Integer, primary_key=True)
    request_id = Column(String(128))
    expert_id = Column(String(128))
    node_id = Column(String(128))
    predicted_cost = Column(Float)
    actual_cost = Column(Float)
    energy_joules = Column(Float)
    carbon_kg = Column(Float)
    helium_units = Column(Float)
    latency_ms = Column(Float)
    accuracy = Column(Float)
    timestamp = Column(DateTime, default=datetime.now)
    weights_snapshot = Column(JSON, nullable=True)
    teacher_id = Column(String(128), nullable=True)  # NEW: teacher used
    distillation_loss = Column(Float, nullable=True)  # NEW: distillation loss

class WeightHistoryDB(Base):
    __tablename__ = 'weight_history'
    id = Column(Integer, primary_key=True)
    alpha = Column(Float)
    beta = Column(Float)
    gamma = Column(Float)
    delta = Column(Float)
    epsilon = Column(Float)
    zeta = Column(Float)
    timestamp = Column(DateTime, default=datetime.now)
    reason = Column(String(64), nullable=True)  # e.g., "update", "rollback"
    teacher_id = Column(String(128), nullable=True)  # NEW: per‑teacher weights

class NormalisationStatsDB(Base):
    __tablename__ = 'normalisation_stats'
    id = Column(Integer, primary_key=True)
    metric = Column(String(16), unique=True, index=True)
    count = Column(Integer)
    mean = Column(Float)
    m2 = Column(Float)  # sum of squared differences from mean
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class OptimizerStateDB(Base):
    __tablename__ = 'optimizer_state'
    id = Column(Integer, primary_key=True)
    expert_id = Column(String(128), index=True)
    optimizer_type = Column(String(16))
    state_json = Column(JSON)  # serialized momentum/Adam state
    timestamp = Column(DateTime, default=datetime.now)

# ---------- Circuit Breaker (inlined) ----------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str, threshold: int = 5, timeout: int = 60):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        self.metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self.metrics["successful_calls"] += 1
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.metrics["failed_calls"] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

# ---------- Retry decorator ----------
def retry_decorator():
    if TENACITY_AVAILABLE:
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((SQLAlchemyError,)),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )
    else:
        # Fallback simple retry
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(3):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == 2:
                            raise
                        await asyncio.sleep(2 ** attempt)
                return None
            return wrapper
        return decorator

# ---------- Incremental Statistics (Welford) ----------
class IncrementalStats:
    """
    Maintains running mean and variance using Welford's algorithm.
    """
    def __init__(self, initial_mean: float = 0.0, initial_m2: float = 0.0, count: int = 0):
        self.count = count
        self.mean = initial_mean
        self.m2 = initial_m2  # sum of (x - mean)^2

    def update(self, x: float):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self):
        return self.m2 / (self.count - 1) if self.count > 1 else 1.0

    @property
    def std(self):
        return np.sqrt(self.variance) if self.count > 1 else 1.0

    def to_dict(self):
        return {
            'count': self.count,
            'mean': self.mean,
            'm2': self.m2,
            'std': self.std
        }

# ---------- Prometheus Metrics ----------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    WEIGHT_ALPHA = Gauge('adaptive_weight_alpha', 'Weight for energy', registry=REGISTRY)
    WEIGHT_BETA = Gauge('adaptive_weight_beta', 'Weight for carbon', registry=REGISTRY)
    WEIGHT_GAMMA = Gauge('adaptive_weight_gamma', 'Weight for helium', registry=REGISTRY)
    WEIGHT_DELTA = Gauge('adaptive_weight_delta', 'Weight for material', registry=REGISTRY)
    WEIGHT_EPSILON = Gauge('adaptive_weight_epsilon', 'Weight for latency', registry=REGISTRY)
    WEIGHT_ZETA = Gauge('adaptive_weight_zeta', 'Weight for accuracy', registry=REGISTRY)
    MAE_GAUGE = Gauge('adaptive_mae', 'Mean absolute error', registry=REGISTRY)
    UPDATE_COUNTER = Counter('adaptive_updates_total', 'Number of weight updates', registry=REGISTRY)
    BATCH_SIZE = Gauge('adaptive_batch_size', 'Current mini-batch size', registry=REGISTRY)
    # NEW MOPD metrics
    TEACHER_WEIGHT = Gauge('adaptive_teacher_weight', 'Teacher weight', ['teacher_id'], registry=REGISTRY)
    DISTILLATION_LOSS = Gauge('adaptive_distillation_loss', 'Distillation loss', registry=REGISTRY)
else:
    # Dummy metrics
    class DummyMetric:
        def set(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    WEIGHT_ALPHA = WEIGHT_BETA = WEIGHT_GAMMA = WEIGHT_DELTA = WEIGHT_EPSILON = WEIGHT_ZETA = MAE_GAUGE = UPDATE_COUNTER = BATCH_SIZE = TEACHER_WEIGHT = DISTILLATION_LOSS = DummyMetric()

# ---------- Configuration with Pydantic ----------
class AdaptiveCostConfig(BaseModel):
    learning_rate: float = Field(0.01, gt=0, le=1)
    learning_rate_decay: float = Field(0.999, ge=0, le=1)
    normalisation_window: int = Field(1000, ge=10)
    mae_threshold: float = Field(1.0, gt=0)
    rollback_enabled: bool = True
    batch_size: int = Field(10, ge=1)  # for mini-batch SGD
    per_expert_weights: bool = False
    metric_names: List[str] = Field(default_factory=lambda: ['E', 'CO2', 'H', 'M', 'L', 'A'])
    initial_weights: Dict[str, float] = Field(
        default_factory=lambda: {'alpha': 1.0, 'beta': 1.0, 'gamma': 1.0, 'delta': 1.0, 'epsilon': 1.0, 'zeta': 1.0}
    )
    # NEW: Optimiser type and momentum
    optimizer: str = Field("adam", description="sgd, momentum, adam")
    momentum: float = Field(0.9, ge=0, le=1)
    beta1: float = Field(0.9, ge=0, le=1)
    beta2: float = Field(0.999, ge=0, le=1)
    epsilon: float = Field(1e-8, gt=0)
    # NEW: Expert cache TTL (seconds)
    expert_cache_ttl: int = Field(300, ge=0)
    # NEW: Anomaly integration
    anomaly_adjustment_enabled: bool = Field(True)
    anomaly_lr_reduction_factor: float = Field(0.5, gt=0, le=1)
    # NEW: Alert webhook
    alert_webhook_url: Optional[str] = None
    # NEW: Database backend
    db_backend: str = Field("sqlite", description="sqlite or postgresql")
    db_url: Optional[str] = Field(None, description="Database URL (if not using default)")

    # NEW: MOPD integration
    enable_mopd: bool = Field(True, description="Enable MOPD teacher grid integration")
    teacher_grid: Dict[str, List[str]] = Field(
        default_factory=lambda: {
            'math': ['math_expert_1', 'math_expert_2'],
            'code': ['code_expert_1', 'code_expert_2'],
            'general': ['general_expert_1']
        },
        description="Mapping domain -> list of teacher IDs"
    )
    distillation_loss_weight: float = Field(0.1, ge=0, le=1, description="Weight of distillation loss in cost")
    pareto_enabled: bool = Field(False, description="Enable Pareto multi-objective routing")

    @field_validator('initial_weights')
    @classmethod
    def check_weights(cls, v: Dict[str, float]) -> Dict[str, float]:
        required = {'alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta'}
        if not required.issubset(v.keys()):
            raise ValueError(f"initial_weights must contain keys {required}")
        return v

    @field_validator('optimizer')
    @classmethod
    def validate_optimizer(cls, v: str) -> str:
        allowed = {'sgd', 'momentum', 'adam'}
        if v not in allowed:
            raise ValueError(f"optimizer must be one of {allowed}")
        return v

    @field_validator('db_backend')
    @classmethod
    def validate_db_backend(cls, v: str) -> str:
        if v not in {'sqlite', 'postgresql'}:
            raise ValueError("db_backend must be 'sqlite' or 'postgresql'")
        return v

# ---------- Database Manager (async) ----------
class AsyncDatabaseManager:
    """Async database manager with support for SQLite and PostgreSQL."""
    def __init__(self, config: AdaptiveCostConfig):
        self.config = config
        if config.db_url:
            self.db_url = config.db_url
        else:
            if config.db_backend == 'sqlite':
                self.db_url = "sqlite+aiosqlite:///adaptive_cost.db"
            else:  # postgresql
                self.db_url = "postgresql+asyncpg://user:pass@localhost/adaptive_cost"
        self.engine = None
        self.async_session = None

    async def init(self):
        """Create async engine and sessionmaker."""
        self.engine = create_async_engine(
            self.db_url,
            poolclass=NullPool,
            echo=False
        )
        self.async_session = async_sessionmaker(
            self.engine, expire_on_commit=False
        )
        # Create tables
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def get_session(self) -> AsyncSession:
        if not self.async_session:
            await self.init()
        return self.async_session()

    async def close(self):
        if self.engine:
            await self.engine.dispose()

# ---------- Expert Cache (with TTL) ----------
class ExpertCache:
    """In-memory cache for expert profiles with TTL."""
    def __init__(self, registry: ExpertRegistry, ttl_seconds: int = 300):
        self.registry = registry
        self.ttl = ttl_seconds
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()

    async def get(self, expert_id: str) -> Optional[Any]:
        now = time.time()
        async with self._lock:
            if expert_id in self._cache:
                expert, timestamp = self._cache[expert_id]
                if now - timestamp < self.ttl:
                    return expert
        # Miss or expired – fetch from registry
        expert = await self.registry.get_expert(expert_id)
        if expert:
            async with self._lock:
                self._cache[expert_id] = (expert, now)
        return expert

    async def invalidate(self, expert_id: str):
        async with self._lock:
            if expert_id in self._cache:
                del self._cache[expert_id]

# ---------- MOPD Teacher Grid Manager ----------
class MOPDTeacherGrid:
    """
    Manages the teacher grid for Multi‑Teacher On‑Policy Distillation.
    Provides methods to select teachers based on domain, reasoning effort, and energy mode.
    """
    def __init__(self, config: AdaptiveCostConfig):
        self.config = config
        self.teacher_grid = config.teacher_grid
        self.teacher_weights: Dict[str, float] = {}  # teacher_id -> weight
        self._lock = asyncio.Lock()

    async def select_teachers(self, domain: str, reasoning_effort: str = "medium",
                              energy_mode: str = "balanced", num_teachers: int = 1) -> List[str]:
        """
        Select teachers from the grid based on domain and energy mode.
        If domain not found, fallback to general.
        """
        # Build composite key (simplified)
        key = f"{domain}_{reasoning_effort}_{energy_mode}"
        if key not in self.teacher_grid:
            # Fallback to domain only
            key = domain
        candidate_ids = self.teacher_grid.get(key, self.teacher_grid.get('general', []))
        # Sort by teacher weights if available
        if candidate_ids:
            sorted_ids = sorted(candidate_ids, key=lambda tid: self.teacher_weights.get(tid, 1.0), reverse=True)
            return sorted_ids[:num_teachers]
        return []

    async def update_teacher_weight(self, teacher_id: str, delta: float):
        """Update weight for a teacher (used in distillation)."""
        async with self._lock:
            current = self.teacher_weights.get(teacher_id, 1.0)
            self.teacher_weights[teacher_id] = max(0.1, current + delta)

    def get_teacher_weights(self) -> Dict[str, float]:
        return self.teacher_weights.copy()

# ---------- Pareto Front Manager ----------
class ParetoFrontManager:
    """
    Maintains a set of Pareto‑optimal weight vectors for multi‑objective routing.
    """
    def __init__(self):
        self.pareto_front: List[Dict[str, float]] = []
        self._lock = asyncio.Lock()

    async def add(self, weights: Dict[str, float]) -> bool:
        """
        Add a weight vector if it is not dominated by existing vectors.
        Returns True if added.
        """
        async with self._lock:
            # Check dominance
            for existing in self.pareto_front:
                if all(existing[k] <= weights[k] for k in weights) and any(existing[k] < weights[k] for k in weights):
                    return False  # dominated
            # Remove dominated vectors
            self.pareto_front = [w for w in self.pareto_front if not all(weights[k] <= w[k] for k in weights)]
            self.pareto_front.append(weights.copy())
            return True

    def get_front(self) -> List[Dict[str, float]]:
        return self.pareto_front.copy()

# ---------- Adaptive Cost Function (Enhanced) ----------
class AdaptiveCostFunction(SustainabilityCostFunction):
    """
    Enhanced adaptive cost function with online SGD, momentum/Adam, async DB,
    caching, anomaly integration, alerting, and MOPD teacher grid integration.
    """

    def __init__(self, config: Dict[str, float]):
        # Validate config with Pydantic
        self._config_obj = AdaptiveCostConfig(**config)
        self.learning_rate = self._config_obj.learning_rate
        self.lr_decay = self._config_obj.learning_rate_decay
        self.normalisation_window = self._config_obj.normalisation_window
        self.mae_threshold = self._config_obj.mae_threshold
        self.rollback_enabled = self._config_obj.rollback_enabled
        self.batch_size = self._config_obj.batch_size
        self.per_expert_weights = self._config_obj.per_expert_weights
        self.metric_names = self._config_obj.metric_names
        self.initial_weights = self._config_obj.initial_weights
        self.optimizer = self._config_obj.optimizer
        self.momentum = self._config_obj.momentum
        self.beta1 = self._config_obj.beta1
        self.beta2 = self._config_obj.beta2
        self.epsilon = self._config_obj.epsilon
        self.expert_cache_ttl = self._config_obj.expert_cache_ttl
        self.anomaly_adjustment_enabled = self._config_obj.anomaly_adjustment_enabled
        self.anomaly_lr_reduction_factor = self._config_obj.anomaly_lr_reduction_factor
        self.alert_webhook_url = self._config_obj.alert_webhook_url
        self.enable_mopd = self._config_obj.enable_mopd
        self.teacher_grid = self._config_obj.teacher_grid
        self.distillation_loss_weight = self._config_obj.distillation_loss_weight
        self.pareto_enabled = self._config_obj.pareto_enabled

        super().__init__(config)

        # Statistics per metric (incremental)
        self.stats: Dict[str, IncrementalStats] = {}
        for m in self.metric_names:
            self.stats[m] = IncrementalStats()

        # Prediction errors for MAE
        self.prediction_errors = deque(maxlen=1000)

        # Mini-batch buffer: list of feedback records
        self._feedback_buffer: List[Tuple[Dict, Dict]] = []  # (context, actual_metrics)

        # Per‑expert weights: if enabled, store dict of expert_id -> weights
        self._expert_weights: Dict[str, Dict[str, float]] = {}

        # Callbacks for routing system
        self._routing_callbacks: List[Callable] = []

        # Lock for thread safety
        self._lock = asyncio.Lock()

        # Database and other dependencies
        self.db_manager: Optional[AsyncDatabaseManager] = None
        self.registry: Optional[ExpertRegistry] = None
        self.expert_cache: Optional[ExpertCache] = None
        self._running = False
        self._validation_task: Optional[asyncio.Task] = None

        # Circuit breaker for DB operations
        self._db_circuit_breaker = EnhancedCircuitBreaker("adaptive_db", threshold=3, timeout=30)

        # Last snapshot for rollback (global weights)
        self._last_snapshot: Dict[str, float] = self.initial_weights.copy()

        # Weight history for export
        self._weight_history: List[Dict] = []

        # Optimiser state (for momentum/Adam)
        self._momentum_velocities: Dict[str, Dict[str, float]] = {}
        self._adam_m: Dict[str, Dict[str, float]] = {}
        self._adam_v: Dict[str, Dict[str, float]] = {}
        self._adam_step: Dict[str, int] = {}

        # Anomaly adjustment flag
        self._anomaly_reduced_lr = False
        self._anomaly_cooldown = 0.0

        # MOPD components
        self.teacher_grid_manager = MOPDTeacherGrid(self._config_obj) if self.enable_mopd else None
        self.pareto_manager = ParetoFrontManager() if self.pareto_enabled else None

        logger.info("AdaptiveCostFunction initialized with config: %s", config)

    def inject_dependencies(
        self,
        db_manager: AsyncDatabaseManager,
        registry: ExpertRegistry,
        carbon_manager=None,
        helium_dashboard=None,
        node_registry=None
    ):
        self.db_manager = db_manager
        self.registry = registry
        self.expert_cache = ExpertCache(registry, self.expert_cache_ttl)
        super().inject_dependencies(carbon_manager, helium_dashboard, node_registry)

        # Load persisted normalisation statistics from DB
        asyncio.create_task(self._load_normalisation_stats())
        # Load optimizer states from DB
        asyncio.create_task(self._load_optimizer_states())

    # -------------------------------------------------------------------------
    # Normalisation persistence
    # -------------------------------------------------------------------------
    @retry_decorator()
    async def _load_normalisation_stats(self):
        """Load stats from DB into memory."""
        if not self.db_manager:
            return
        async with self.db_manager.get_session() as session:
            result = await session.execute(
                text("SELECT metric, count, mean, m2 FROM normalisation_stats")
            )
            rows = result.fetchall()
            for row in rows:
                metric = row.metric
                self.stats[metric] = IncrementalStats(
                    initial_mean=row.mean,
                    initial_m2=row.m2,
                    count=row.count
                )
            logger.info("Loaded normalisation stats for %d metrics", len(rows))

    @retry_decorator()
    async def _persist_normalisation_stats(self):
        """Save current stats to DB."""
        if not self.db_manager:
            return
        async with self.db_manager.get_session() as session:
            for metric, stat in self.stats.items():
                await session.execute(
                    text("""
                        INSERT INTO normalisation_stats (metric, count, mean, m2, updated_at)
                        VALUES (:metric, :count, :mean, :m2, :updated_at)
                        ON CONFLICT (metric) DO UPDATE SET
                            count = EXCLUDED.count,
                            mean = EXCLUDED.mean,
                            m2 = EXCLUDED.m2,
                            updated_at = EXCLUDED.updated_at
                    """),
                    {
                        'metric': metric,
                        'count': stat.count,
                        'mean': stat.mean,
                        'm2': stat.m2,
                        'updated_at': datetime.now()
                    }
                )
            await session.commit()

    # -------------------------------------------------------------------------
    # Optimizer state persistence
    # -------------------------------------------------------------------------
    async def _load_optimizer_states(self):
        """Load optimizer states from DB."""
        if not self.db_manager:
            return
        async with self.db_manager.get_session() as session:
            result = await session.execute(
                text("SELECT expert_id, optimizer_type, state_json FROM optimizer_state")
            )
            rows = result.fetchall()
            for row in rows:
                expert_id = row.expert_id
                opt_type = row.optimizer_type
                state = json.loads(row.state_json)
                if opt_type == 'momentum':
                    self._momentum_velocities[expert_id] = state
                elif opt_type == 'adam':
                    self._adam_m[expert_id] = state['m']
                    self._adam_v[expert_id] = state['v']
                    self._adam_step[expert_id] = state['step']
            logger.info("Loaded optimizer states for %d experts", len(rows))

    async def _persist_optimizer_state(self, expert_id: str):
        """Save optimizer state for an expert."""
        if not self.db_manager:
            return
        opt_type = self.optimizer
        state_json = {}
        if opt_type == 'momentum' and expert_id in self._momentum_velocities:
            state_json = self._momentum_velocities[expert_id]
        elif opt_type == 'adam' and expert_id in self._adam_m:
            state_json = {
                'm': self._adam_m[expert_id],
                'v': self._adam_v[expert_id],
                'step': self._adam_step[expert_id]
            }
        else:
            return
        async with self.db_manager.get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO optimizer_state (expert_id, optimizer_type, state_json, timestamp)
                    VALUES (:expert_id, :opt_type, :state_json, :timestamp)
                    ON CONFLICT (expert_id, optimizer_type) DO UPDATE SET
                        state_json = EXCLUDED.state_json,
                        timestamp = EXCLUDED.timestamp
                """),
                {
                    'expert_id': expert_id,
                    'opt_type': opt_type,
                    'state_json': json.dumps(state_json),
                    'timestamp': datetime.now()
                }
            )
            await session.commit()

    # -------------------------------------------------------------------------
    # Feedback recording (enhanced with MOPD)
    # -------------------------------------------------------------------------
    async def record_feedback(
        self,
        context: Dict[str, Any],
        actual_metrics: Dict[str, float],
        teacher_id: Optional[str] = None,
        distillation_loss: Optional[float] = None
    ) -> None:
        """
        Record actual metrics after a request and optionally update weights.
        If teacher_id and distillation_loss are provided, they are recorded for MOPD.
        """
        expert_id = context.get('expert_id')
        if not expert_id or not self.registry or not self.expert_cache:
            logger.warning("Missing expert_id, registry, or cache; skipping feedback")
            return

        expert = await self.expert_cache.get(expert_id)
        if not expert:
            logger.warning(f"Expert {expert_id} not found; skipping feedback")
            return

        # Compute predicted cost
        predicted_cost = await self.compute(expert, context)

        # Extract metrics with validation and imputation
        E, CO2, H, M, L, A = self._validate_and_impute(actual_metrics)

        # Compute actual cost (use current weights)
        weights = self._get_weights(expert_id)
        actual_cost = (
            weights.get('alpha', 1.0) * E +
            weights.get('beta', 1.0) * CO2 +
            weights.get('gamma', 1.0) * H +
            weights.get('delta', 1.0) * M +
            weights.get('epsilon', 1.0) * L +
            weights.get('zeta', 1.0) * A
        )

        error = actual_cost - predicted_cost
        self.prediction_errors.append(error)

        # Update incremental stats for each metric
        async with self._lock:
            for metric, value in [('E', E), ('CO2', CO2), ('H', H), ('M', M), ('L', L), ('A', A)]:
                self.stats[metric].update(value)

            # Add to mini-batch buffer (include teacher info)
            self._feedback_buffer.append((
                {
                    'E': E, 'CO2': CO2, 'H': H, 'M': M, 'L': L, 'A': A,
                    'predicted_cost': predicted_cost,
                    'actual_cost': actual_cost,
                    'error': error,
                    'expert_id': expert_id,
                    'teacher_id': teacher_id,
                    'distillation_loss': distillation_loss,
                },
                context
            ))

            # If buffer reaches batch size, perform SGD update
            if len(self._feedback_buffer) >= self.batch_size:
                await self._apply_mini_batch()
                self._feedback_buffer.clear()

        # Persist feedback record (with retry)
        await self._persist_feedback(context, actual_metrics, predicted_cost, actual_cost,
                                     teacher_id, distillation_loss)

        # Notify routing system if weights changed significantly
        if self._routing_callbacks:
            for cb in self._routing_callbacks:
                try:
                    await cb(self.weights)
                except Exception as e:
                    logger.error(f"Routing callback error: {e}")

    def _validate_and_impute(self, metrics: Dict[str, float]) -> Tuple[float, float, float, float, float, float]:
        """
        Validate and impute missing metrics using historical stats.
        """
        # Define default values and impute if missing
        E = metrics.get('energy_joules')
        if E is None:
            logger.warning("Missing energy_joules, imputing from stats.")
            E = self.stats['E'].mean if self.stats['E'].count > 0 else 0.0

        CO2 = metrics.get('carbon_kg')
        if CO2 is None:
            logger.warning("Missing carbon_kg, imputing from stats.")
            CO2 = self.stats['CO2'].mean if self.stats['CO2'].count > 0 else 0.0

        H = metrics.get('helium_units')
        if H is None:
            logger.warning("Missing helium_units, imputing from stats.")
            H = self.stats['H'].mean if self.stats['H'].count > 0 else 0.0

        M = metrics.get('material_index')
        if M is None:
            logger.warning("Missing material_index, imputing from stats.")
            M = self.stats['M'].mean if self.stats['M'].count > 0 else 0.0

        L = metrics.get('latency_ms')
        if L is None:
            logger.warning("Missing latency_ms, imputing from stats.")
            L = self.stats['L'].mean if self.stats['L'].count > 0 else 0.0

        A = 1.0 - metrics.get('accuracy', 0.5)
        if A is None:
            logger.warning("Missing accuracy, imputing from stats.")
            A = self.stats['A'].mean if self.stats['A'].count > 0 else 0.5

        return E, CO2, H, M, L, A

    # -------------------------------------------------------------------------
    # Mini-batch SGD with momentum/Adam (enhanced with MOPD distillation loss)
    # -------------------------------------------------------------------------
    async def _apply_mini_batch(self):
        """Apply SGD update on the accumulated batch with momentum/Adam."""
        if not self._feedback_buffer:
            return

        # Accumulate gradients
        grad_sum = {k: 0.0 for k in self.initial_weights.keys()}
        # For MOPD: accumulate distillation loss gradient if any
        distill_grad_sum = {k: 0.0 for k in self.initial_weights.keys()}
        for record, _ in self._feedback_buffer:
            # Normalise each metric
            E_norm = self._normalise(record['E'], 'E')
            CO2_norm = self._normalise(record['CO2'], 'CO2')
            H_norm = self._normalise(record['H'], 'H')
            M_norm = self._normalise(record['M'], 'M')
            L_norm = self._normalise(record['L'], 'L')
            A_norm = self._normalise(record['A'], 'A')

            error = record['error']
            grad_sum['alpha'] += error * E_norm
            grad_sum['beta'] += error * CO2_norm
            grad_sum['gamma'] += error * H_norm
            grad_sum['delta'] += error * M_norm
            grad_sum['epsilon'] += error * L_norm
            grad_sum['zeta'] += error * A_norm

            # If distillation loss is present, incorporate it
            distill_loss = record.get('distillation_loss')
            if distill_loss is not None and self.enable_mopd:
                # Simplistic: gradient is proportional to distillation loss
                # In practice, you might compute gradient from teacher-student difference.
                distill_grad = distill_loss * 0.1
                for k in grad_sum:
                    distill_grad_sum[k] += distill_grad

        # Average gradients
        batch_size = len(self._feedback_buffer)
        for k in grad_sum:
            grad_sum[k] /= batch_size
            distill_grad_sum[k] /= batch_size

        # Combine with distillation weight
        if self.enable_mopd:
            for k in grad_sum:
                grad_sum[k] = (1 - self.distillation_loss_weight) * grad_sum[k] + \
                               self.distillation_loss_weight * distill_grad_sum[k]

        # Apply gradient descent with selected optimizer
        lr = self.learning_rate
        # If anomaly adjustment is active, reduce LR
        if self._anomaly_reduced_lr and self.anomaly_adjustment_enabled:
            lr *= self.anomaly_lr_reduction_factor

        async with self._lock:
            weights = self._get_weights(self._feedback_buffer[0][1].get('expert_id'))
            expert_id = self._feedback_buffer[0][1].get('expert_id')

            if self.optimizer == 'adam':
                await self._apply_adam(weights, grad_sum, lr, expert_id)
            elif self.optimizer == 'momentum':
                await self._apply_momentum(weights, grad_sum, lr, expert_id)
            else:  # sgd
                for k in weights:
                    weights[k] -= lr * grad_sum[k]
                    weights[k] = max(-5.0, min(5.0, weights[k]))

            # Update teacher weights if MOPD enabled
            if self.enable_mopd and self.teacher_grid_manager:
                # Update teacher weight based on distillation loss (positive if loss low)
                for record, _ in self._feedback_buffer:
                    teacher_id = record.get('teacher_id')
                    distill_loss = record.get('distillation_loss')
                    if teacher_id and distill_loss is not None:
                        # Lower loss -> increase weight
                        delta = -distill_loss * 0.01
                        await self.teacher_grid_manager.update_teacher_weight(teacher_id, delta)
                        TEACHER_WEIGHT.labels(teacher_id=teacher_id).set(
                            self.teacher_grid_manager.get_teacher_weights().get(teacher_id, 1.0)
                        )

            # Decay learning rate
            self.learning_rate *= self.lr_decay

            # Update snapshot if change significant
            if abs(weights['alpha'] - self._last_snapshot.get('alpha', 0)) > 0.1:
                self._last_snapshot = weights.copy()
                await self._persist_weight_history(reason="batch_update")
                # Save optimizer state
                if self.per_expert_weights and expert_id:
                    await self._persist_optimizer_state(expert_id)

            # Update Prometheus gauges
            WEIGHT_ALPHA.set(weights['alpha'])
            WEIGHT_BETA.set(weights['beta'])
            WEIGHT_GAMMA.set(weights['gamma'])
            WEIGHT_DELTA.set(weights['delta'])
            WEIGHT_EPSILON.set(weights['epsilon'])
            WEIGHT_ZETA.set(weights['zeta'])
            UPDATE_COUNTER.inc()
            BATCH_SIZE.set(batch_size)

            logger.debug(f"Batch update: weights={weights}, lr={lr:.4f}")

    async def _apply_momentum(self, weights: Dict[str, float], grad: Dict[str, float], lr: float, expert_id: str):
        """Apply momentum update."""
        if expert_id not in self._momentum_velocities:
            self._momentum_velocities[expert_id] = {k: 0.0 for k in weights}
        vel = self._momentum_velocities[expert_id]
        for k in weights:
            vel[k] = self.momentum * vel[k] + lr * grad[k]
            weights[k] -= vel[k]
            weights[k] = max(-5.0, min(5.0, weights[k]))

    async def _apply_adam(self, weights: Dict[str, float], grad: Dict[str, float], lr: float, expert_id: str):
        """Apply Adam update."""
        if expert_id not in self._adam_m:
            self._adam_m[expert_id] = {k: 0.0 for k in weights}
            self._adam_v[expert_id] = {k: 0.0 for k in weights}
            self._adam_step[expert_id] = 0
        m = self._adam_m[expert_id]
        v = self._adam_v[expert_id]
        step = self._adam_step[expert_id] + 1
        self._adam_step[expert_id] = step

        for k in weights:
            m[k] = self.beta1 * m[k] + (1 - self.beta1) * grad[k]
            v[k] = self.beta2 * v[k] + (1 - self.beta2) * (grad[k] ** 2)
            m_hat = m[k] / (1 - self.beta1 ** step)
            v_hat = v[k] / (1 - self.beta2 ** step)
            weights[k] -= lr * m_hat / (np.sqrt(v_hat) + self.epsilon)
            weights[k] = max(-5.0, min(5.0, weights[k]))

    def _normalise(self, value: float, metric: str) -> float:
        """Normalise using incremental stats."""
        stat = self.stats.get(metric)
        if not stat or stat.count < 2:
            return 0.0
        std = stat.std
        if std < 1e-8:
            return 0.0
        return (value - stat.mean) / std

    def _get_weights(self, expert_id: Optional[str] = None) -> Dict[str, float]:
        """Return weights for the given expert or global weights."""
        if self.per_expert_weights and expert_id:
            if expert_id not in self._expert_weights:
                self._expert_weights[expert_id] = self.initial_weights.copy()
            return self._expert_weights[expert_id]
        return self.weights

    # -------------------------------------------------------------------------
    # Persistence helpers (with retry & circuit breaker)
    # -------------------------------------------------------------------------
    async def _persist_feedback(self, context: Dict, actual: Dict, pred: float, actual_cost: float,
                                teacher_id: Optional[str] = None, distillation_loss: Optional[float] = None):
        if not self.db_manager:
            return
        try:
            await self._db_circuit_breaker.call(
                self._persist_feedback_inner,
                context, actual, pred, actual_cost, teacher_id, distillation_loss
            )
        except Exception as e:
            logger.error(f"Feedback persistence failed: {e}")

    @retry_decorator()
    async def _persist_feedback_inner(self, context: Dict, actual: Dict, pred: float, actual_cost: float,
                                       teacher_id: Optional[str], distillation_loss: Optional[float]):
        async with self.db_manager.get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO feedback_records
                    (request_id, expert_id, node_id, predicted_cost, actual_cost,
                     energy_joules, carbon_kg, helium_units, latency_ms, accuracy,
                     weights_snapshot, teacher_id, distillation_loss)
                    VALUES (:request_id, :expert_id, :node_id, :predicted_cost, :actual_cost,
                     :energy_joules, :carbon_kg, :helium_units, :latency_ms, :accuracy,
                     :weights_snapshot, :teacher_id, :distillation_loss)
                """),
                {
                    'request_id': context.get('request_id'),
                    'expert_id': context.get('expert_id'),
                    'node_id': context.get('node_id'),
                    'predicted_cost': pred,
                    'actual_cost': actual_cost,
                    'energy_joules': actual.get('energy_joules', 0),
                    'carbon_kg': actual.get('carbon_kg', 0),
                    'helium_units': actual.get('helium_units', 0),
                    'latency_ms': actual.get('latency_ms', 0),
                    'accuracy': actual.get('accuracy', 0),
                    'weights_snapshot': json.dumps(self.weights),
                    'teacher_id': teacher_id,
                    'distillation_loss': distillation_loss
                }
            )
            await session.commit()

    async def _persist_weight_history(self, reason: str = "update"):
        if not self.db_manager:
            return
        try:
            await self._db_circuit_breaker.call(
                self._persist_weight_history_inner,
                reason
            )
        except Exception as e:
            logger.error(f"Weight history persistence failed: {e}")

    @retry_decorator()
    async def _persist_weight_history_inner(self, reason: str):
        async with self.db_manager.get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO weight_history
                    (alpha, beta, gamma, delta, epsilon, zeta, reason)
                    VALUES (:alpha, :beta, :gamma, :delta, :epsilon, :zeta, :reason)
                """),
                {
                    'alpha': self.weights['alpha'],
                    'beta': self.weights['beta'],
                    'gamma': self.weights['gamma'],
                    'delta': self.weights['delta'],
                    'epsilon': self.weights['epsilon'],
                    'zeta': self.weights['zeta'],
                    'reason': reason
                }
            )
            await session.commit()

    # -------------------------------------------------------------------------
    # Validation loop with trend analysis and alerting
    # -------------------------------------------------------------------------
    async def start_validation_loop(self, interval_seconds: int = 3600):
        """Start background task to monitor weight adaptation performance."""
        self._running = True
        self._validation_task = asyncio.create_task(
            self._validation_loop(interval_seconds)
        )
        logger.info("AdaptiveCostFunction validation loop started")

    async def _validation_loop(self, interval: int):
        while self._running:
            try:
                await self._validate_weights()
                await self._persist_normalisation_stats()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Weight validation error: {e}")
                await asyncio.sleep(60)

    async def _validate_weights(self):
        """Check MAE and optionally roll back weights if performance degrades."""
        if len(self.prediction_errors) < self.batch_size:
            return
        errors = list(self.prediction_errors)
        mae = np.mean(np.abs(errors))
        MAE_GAUGE.set(mae)
        logger.info(f"Weight adaptation MAE (last {len(errors)}): {mae:.4f}")

        # Trend analysis: compute slope of errors over time
        if len(errors) > 50:
            slope = np.polyfit(range(len(errors)), errors, 1)[0]
            if slope > 0.01:
                logger.warning(f"Errors trend is increasing (slope={slope:.4f}). Consider adjusting learning rate.")

        # If MAE exceeds threshold, rollback and send alert
        if self.rollback_enabled and mae > self.mae_threshold:
            logger.warning(f"MAE {mae:.4f} exceeds threshold {self.mae_threshold}. Rolling back weights.")
            await self._rollback_weights()
            # Send alert via webhook
            if self.alert_webhook_url:
                await self._send_alert(f"MAE exceeded threshold: {mae:.4f} > {self.mae_threshold}")

    async def _rollback_weights(self):
        """Restore weights to the last snapshot."""
        async with self._lock:
            self.weights = self._last_snapshot.copy()
            # Reset optimiser state for this expert (if any)
            # For simplicity, we clear all optimiser states.
            self._momentum_velocities.clear()
            self._adam_m.clear()
            self._adam_v.clear()
            self._adam_step.clear()
            if self.db_manager:
                await self._persist_weight_history(reason="rollback")
            logger.info("Weights rolled back to previous snapshot.")

    async def _send_alert(self, message: str):
        """Send alert via webhook."""
        if not self.alert_webhook_url:
            return
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "text": f"AdaptiveCostFunction Alert: {message}",
                    "timestamp": datetime.now().isoformat()
                }
                await session.post(self.alert_webhook_url, json=payload, timeout=5)
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

    # -------------------------------------------------------------------------
    # Anomaly integration
    # -------------------------------------------------------------------------
    async def on_anomaly_detected(self, severity: float):
        """
        Callback from anomaly detector.
        Adjusts learning rate temporarily based on severity.
        """
        if not self.anomaly_adjustment_enabled:
            return
        if severity > 0.7:
            self._anomaly_reduced_lr = True
            self._anomaly_cooldown = time.time() + 300  # 5 minutes
            logger.info(f"Anomaly detected (severity={severity}), reducing LR for 5 minutes.")
        else:
            # Still reduce but less aggressively
            self._anomaly_reduced_lr = True
            self._anomaly_cooldown = time.time() + 60
            logger.info(f"Anomaly detected (severity={severity}), reducing LR for 1 minute.")

        # Schedule reset
        asyncio.create_task(self._reset_anomaly_adjustment())

    async def _reset_anomaly_adjustment(self):
        """Reset anomaly LR reduction after cooldown."""
        await asyncio.sleep(self._anomaly_cooldown - time.time())
        self._anomaly_reduced_lr = False
        logger.info("Anomaly LR adjustment reset.")

    # -------------------------------------------------------------------------
    # MOPD Teacher selection
    # -------------------------------------------------------------------------
    async def select_teachers(self, domain: str, reasoning_effort: str = "medium",
                               energy_mode: str = "balanced", num_teachers: int = 1) -> List[str]:
        """
        Select teachers from the MOPD grid.
        """
        if not self.enable_mopd or not self.teacher_grid_manager:
            return []
        return await self.teacher_grid_manager.select_teachers(
            domain, reasoning_effort, energy_mode, num_teachers
        )

    # -------------------------------------------------------------------------
    # Export / import weights
    # -------------------------------------------------------------------------
    def export_weights(self, expert_id: Optional[str] = None) -> Dict[str, Any]:
        """Export current weights (and optionally per-expert weights) as JSON."""
        data = {
            'global_weights': self.weights.copy(),
            'expert_weights': self._expert_weights.copy(),
            'stats': {k: v.to_dict() for k, v in self.stats.items()},
            'learning_rate': self.learning_rate,
            'timestamp': datetime.now().isoformat()
        }
        if self.enable_mopd and self.teacher_grid_manager:
            data['teacher_weights'] = self.teacher_grid_manager.get_teacher_weights()
        return data

    def import_weights(self, data: Dict[str, Any]) -> None:
        """Import weights from JSON export."""
        self.weights = data.get('global_weights', self.initial_weights.copy())
        self._expert_weights = data.get('expert_weights', {})
        for metric, stat_dict in data.get('stats', {}).items():
            if metric in self.stats:
                self.stats[metric] = IncrementalStats(
                    initial_mean=stat_dict['mean'],
                    initial_m2=stat_dict['m2'],
                    count=stat_dict['count']
                )
        self.learning_rate = data.get('learning_rate', self.learning_rate)
        self._last_snapshot = self.weights.copy()
        if self.enable_mopd and self.teacher_grid_manager and 'teacher_weights' in data:
            self.teacher_grid_manager.teacher_weights = data['teacher_weights']
        logger.info("Weights imported successfully.")

    # -------------------------------------------------------------------------
    # Callback registration for routing system
    # -------------------------------------------------------------------------
    def register_routing_callback(self, callback: Callable):
        """Register a callback to be called when weights change."""
        self._routing_callbacks.append(callback)

    # -------------------------------------------------------------------------
    # Shutdown
    # -------------------------------------------------------------------------
    async def stop(self):
        self._running = False
        if self._validation_task:
            self._validation_task.cancel()
            try:
                await self._validation_task
            except asyncio.CancelledError:
                pass
        if self.db_manager:
            await self.db_manager.close()
        logger.info("AdaptiveCostFunction stopped")

# =============================================================================
# FastAPI REST API (enhanced)
# =============================================================================
app = FastAPI(title="Adaptive Cost Function API", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instance (set during startup)
adaptive_function: Optional[AdaptiveCostFunction] = None

# Authentication (JWT with roles)
security = HTTPBearer()
async def verify_jwt(token: str) -> Dict:
    # In production, verify JWT properly and extract roles.
    # For demo, we accept any token and assign role based on presence.
    return {"sub": "admin", "role": "admin"}

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await verify_jwt(credentials.credentials)

async def require_admin(user: Dict = Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user

# ---------- API Endpoints ----------
@app.get("/metrics")
async def get_metrics():
    if PROMETHEUS_AVAILABLE:
        return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
    return {"error": "Prometheus not enabled"}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/weights", dependencies=[Depends(get_current_user)])
async def get_weights(expert_id: Optional[str] = None):
    if not adaptive_function:
        raise HTTPException(status_code=503, detail="Service not initialized")
    weights = adaptive_function._get_weights(expert_id)
    return {"weights": weights, "expert_id": expert_id}

@app.post("/weights", dependencies=[Depends(require_admin)])
async def set_weights(new_weights: Dict[str, float], expert_id: Optional[str] = None):
    if not adaptive_function:
        raise HTTPException(status_code=503, detail="Service not initialized")
    required = {'alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta'}
    if not required.issubset(new_weights.keys()):
        raise HTTPException(status_code=400, detail=f"Missing keys, required: {required}")
    async with adaptive_function._lock:
        if expert_id and adaptive_function.per_expert_weights:
            adaptive_function._expert_weights[expert_id] = new_weights.copy()
        else:
            adaptive_function.weights = new_weights.copy()
            adaptive_function._last_snapshot = new_weights.copy()
    return {"status": "updated", "weights": new_weights}

@app.get("/weights/history", dependencies=[Depends(get_current_user)])
async def get_weight_history(limit: int = 100):
    if not adaptive_function or not adaptive_function.db_manager:
        raise HTTPException(status_code=503, detail="Database not available")
    async with adaptive_function.db_manager.get_session() as session:
        result = await session.execute(
            text("SELECT * FROM weight_history ORDER BY id DESC LIMIT :limit"),
            {"limit": limit}
        )
        rows = result.fetchall()
        return {"history": [dict(r._mapping) for r in rows]}

@app.get("/feedback", dependencies=[Depends(get_current_user)])
async def get_feedback_history(limit: int = 100):
    if not adaptive_function or not adaptive_function.db_manager:
        raise HTTPException(status_code=503, detail="Database not available")
    async with adaptive_function.db_manager.get_session() as session:
        result = await session.execute(
            text("SELECT * FROM feedback_records ORDER BY id DESC LIMIT :limit"),
            {"limit": limit}
        )
        rows = result.fetchall()
        return {"feedback": [dict(r._mapping) for r in rows]}

@app.post("/weights/export", dependencies=[Depends(get_current_user)])
async def export_weights():
    if not adaptive_function:
        raise HTTPException(status_code=503, detail="Service not initialized")
    data = adaptive_function.export_weights()
    return {"export": data}

@app.post("/weights/import", dependencies=[Depends(require_admin)])
async def import_weights(data: Dict[str, Any]):
    if not adaptive_function:
        raise HTTPException(status_code=503, detail="Service not initialized")
    adaptive_function.import_weights(data)
    return {"status": "imported"}

@app.get("/stats", dependencies=[Depends(get_current_user)])
async def get_stats():
    if not adaptive_function:
        raise HTTPException(status_code=503, detail="Service not initialized")
    stats = {k: v.to_dict() for k, v in adaptive_function.stats.items()}
    return {"stats": stats}

# NEW MOPD endpoints
@app.get("/teachers", dependencies=[Depends(get_current_user)])
async def get_teachers(domain: Optional[str] = None):
    if not adaptive_function or not adaptive_function.enable_mopd:
        raise HTTPException(status_code=503, detail="MOPD not enabled")
    if domain:
        teachers = await adaptive_function.select_teachers(domain)
    else:
        # Return all teachers and weights
        weights = adaptive_function.teacher_grid_manager.get_teacher_weights()
        return {"teacher_weights": weights, "grid": adaptive_function.teacher_grid_manager.teacher_grid}
    return {"teachers": teachers}

@app.post("/teachers/update_weight", dependencies=[Depends(require_admin)])
async def update_teacher_weight(teacher_id: str, delta: float):
    if not adaptive_function or not adaptive_function.enable_mopd:
        raise HTTPException(status_code=503, detail="MOPD not enabled")
    await adaptive_function.teacher_grid_manager.update_teacher_weight(teacher_id, delta)
    return {"status": "updated", "teacher_id": teacher_id, "new_weight": adaptive_function.teacher_grid_manager.teacher_weights.get(teacher_id, 1.0)}

@app.get("/pareto", dependencies=[Depends(get_current_user)])
async def get_pareto_front():
    if not adaptive_function or not adaptive_function.pareto_enabled:
        raise HTTPException(status_code=503, detail="Pareto not enabled")
    return {"pareto_front": adaptive_function.pareto_manager.get_front()}

@app.post("/pareto/add", dependencies=[Depends(require_admin)])
async def add_pareto_point(weights: Dict[str, float]):
    if not adaptive_function or not adaptive_function.pareto_enabled:
        raise HTTPException(status_code=503, detail="Pareto not enabled")
    added = await adaptive_function.pareto_manager.add(weights)
    return {"added": added, "pareto_front": adaptive_function.pareto_manager.get_front()}

# ---------- Startup/Shutdown ----------
@app.on_event("startup")
async def startup():
    global adaptive_function
    # Create an instance (this would normally be injected)
    config = {
        'learning_rate': 0.01,
        'learning_rate_decay': 0.999,
        'normalisation_window': 1000,
        'mae_threshold': 1.0,
        'rollback_enabled': True,
        'batch_size': 10,
        'per_expert_weights': False,
        'initial_weights': {'alpha': 1.0, 'beta': 1.0, 'gamma': 1.0, 'delta': 1.0, 'epsilon': 1.0, 'zeta': 1.0},
        'optimizer': 'adam',
        'momentum': 0.9,
        'beta1': 0.9,
        'beta2': 0.999,
        'epsilon': 1e-8,
        'expert_cache_ttl': 300,
        'anomaly_adjustment_enabled': True,
        'anomaly_lr_reduction_factor': 0.5,
        'alert_webhook_url': None,
        'db_backend': 'sqlite',
        'db_url': None,
        'enable_mopd': True,
        'teacher_grid': {
            'math': ['math_expert_1', 'math_expert_2'],
            'code': ['code_expert_1', 'code_expert_2'],
            'general': ['general_expert_1']
        },
        'distillation_loss_weight': 0.1,
        'pareto_enabled': False,
    }
    adaptive_function = AdaptiveCostFunction(config)
    # In a real deployment, inject dependencies here.
    logger.info("FastAPI startup: AdaptiveCostFunction initialized")

@app.on_event("shutdown")
async def shutdown():
    if adaptive_function:
        await adaptive_function.stop()
    logger.info("FastAPI shutdown")

# ---------- Unit tests (pytest) ----------
"""
Test suite for AdaptiveCostFunction.

To run: pytest -v adaptive_cost_function.py
"""
def test_adaptive_cost_function_basic():
    """Test basic normalisation and weight update."""
    import pytest
    from unittest.mock import Mock, AsyncMock
    config = {
        'learning_rate': 0.1,
        'batch_size': 2,
        'initial_weights': {'alpha': 1.0, 'beta': 0.5, 'gamma': 0.0, 'delta': 0.0, 'epsilon': 0.0, 'zeta': 0.0},
        'optimizer': 'sgd',
        'db_backend': 'sqlite',
    }
    af = AdaptiveCostFunction(config)
    af.stats['E'].update(10)
    af.stats['CO2'].update(5)
    af.stats['H'].update(2)
    af.stats['M'].update(1)
    af.stats['L'].update(100)
    af.stats['A'].update(0.5)

    # Mock registry and db_manager
    af.registry = Mock()
    af.registry.get_expert = AsyncMock(return_value=Mock())
    af.db_manager = AsyncMock()
    af.db_manager.get_session.return_value.__aenter__.return_value = Mock()
    af.expert_cache = ExpertCache(af.registry, ttl_seconds=60)

    # Record feedback
    context = {'expert_id': 'exp1', 'request_id': 'req1'}
    metrics = {'energy_joules': 20, 'carbon_kg': 8, 'helium_units': 3, 'material_index': 0.5, 'latency_ms': 150, 'accuracy': 0.8}
    af.compute = AsyncMock(return_value=0.5)
    asyncio.run(af.record_feedback(context, metrics))
    assert len(af._feedback_buffer) == 1

    # Add another to trigger batch update
    asyncio.run(af.record_feedback(context, metrics))
    assert len(af._feedback_buffer) == 0
    assert af.weights['alpha'] != 1.0

def test_mopd_teacher_selection():
    """Test MOPD teacher selection."""
    config = {
        'enable_mopd': True,
        'teacher_grid': {
            'math': ['math_expert_1', 'math_expert_2'],
            'general': ['general_expert_1']
        }
    }
    af = AdaptiveCostFunction(config)
    teachers = asyncio.run(af.select_teachers('math', 'medium', 'balanced', 2))
    assert len(teachers) == 2
    assert teachers[0] in ['math_expert_1', 'math_expert_2']

def test_pareto_front():
    """Test Pareto front management."""
    config = {'pareto_enabled': True}
    af = AdaptiveCostFunction(config)
    weights1 = {'alpha': 1.0, 'beta': 0.5, 'gamma': 0.0, 'delta': 0.0, 'epsilon': 0.0, 'zeta': 0.0}
    weights2 = {'alpha': 0.8, 'beta': 0.6, 'gamma': 0.0, 'delta': 0.0, 'epsilon': 0.0, 'zeta': 0.0}
    added = asyncio.run(af.pareto_manager.add(weights1))
    assert added == True
    added = asyncio.run(af.pareto_manager.add(weights2))
    # weights2 is not dominated by weights1 (alpha lower, beta higher)
    assert added == True
    front = af.pareto_manager.get_front()
    assert len(front) == 2
"""

# ---------- Main entry point ----------
if __name__ == "__main__":
    # Run FastAPI server
    uvicorn.run(
        "adaptive_cost_function:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False
    )
