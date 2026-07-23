# File: src/enhancements/adaptive_cost_function.py
"""
Adaptive Sustainability Cost Function with online weight learning.
Extends the base SustainabilityCostFunction with SGD weight adaptation.

v2.0.0 ENHANCEMENTS:
- Persisted normalisation statistics (mean/std) in DB.
- Incremental statistics (Welford's algorithm) for O(1) updates.
- Retry logic and circuit‑breakers for database operations.
- Prometheus metrics for weights, MAE, update count.
- Pydantic validation for configuration.
- Adaptive learning rate with decay.
- Mini‑batch SGD (batch update every N feedbacks).
- Per‑expert or per‑node weight sets.
- FastAPI REST API to view and adjust weights.
- Export/import of weights (JSON).
- Graceful handling of missing metrics.
- Extended validation loop with trend analysis and alerting.
- Callback integration with routing system.
- Unit test stubs.
- Comprehensive documentation.
"""

import asyncio
import logging
import json
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple, Callable
from collections import deque
from datetime import datetime
from enum import Enum
import numpy as np
import threading

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator, ValidationInfo

# ---------- SQLAlchemy ----------
from sqlalchemy import (
    Column, String, Float, DateTime, Integer, Boolean, JSON, text, create_engine, event, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

# ---------- FastAPI ----------
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
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

# ---------- Database models ----------
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

class NormalisationStatsDB(Base):
    __tablename__ = 'normalisation_stats'
    id = Column(Integer, primary_key=True)
    metric = Column(String(16), unique=True, index=True)
    count = Column(Integer)
    mean = Column(Float)
    m2 = Column(Float)  # sum of squared differences from mean
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

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
else:
    # Dummy metrics
    class DummyMetric:
        def set(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    WEIGHT_ALPHA = WEIGHT_BETA = WEIGHT_GAMMA = WEIGHT_DELTA = WEIGHT_EPSILON = WEIGHT_ZETA = MAE_GAUGE = UPDATE_COUNTER = BATCH_SIZE = DummyMetric()

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

    @field_validator('initial_weights')
    @classmethod
    def check_weights(cls, v: Dict[str, float]) -> Dict[str, float]:
        required = {'alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta'}
        if not required.issubset(v.keys()):
            raise ValueError(f"initial_weights must contain keys {required}")
        return v

# ---------- Main Adaptive Cost Function ----------
class AdaptiveCostFunction(SustainabilityCostFunction):
    """
    Extends SustainabilityCostFunction with online SGD weight adaptation.
    Supports per‑expert weights, mini‑batch updates, adaptive LR, persisted normalisation,
    circuit‑breakers, Prometheus metrics, and REST API.
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
        self.db_manager: Optional[DatabaseManager] = None
        self.registry: Optional[ExpertRegistry] = None
        self._running = False
        self._validation_task: Optional[asyncio.Task] = None

        # Circuit breaker for DB operations
        self._db_circuit_breaker = EnhancedCircuitBreaker("adaptive_db", threshold=3, timeout=30)

        # Last snapshot for rollback (global weights)
        self._last_snapshot: Dict[str, float] = self.initial_weights.copy()

        # Weight history for export
        self._weight_history: List[Dict] = []

        logger.info("AdaptiveCostFunction initialized with config: %s", config)

    def inject_dependencies(
        self,
        db_manager: 'DatabaseManager',
        registry: ExpertRegistry,
        carbon_manager=None,
        helium_dashboard=None,
        node_registry=None
    ):
        self.db_manager = db_manager
        self.registry = registry
        super().inject_dependencies(carbon_manager, helium_dashboard, node_registry)

        # Load persisted normalisation statistics from DB
        asyncio.create_task(self._load_normalisation_stats())

    # -------------------------------------------------------------------------
    # Normalisation persistence
    # -------------------------------------------------------------------------
    @retry_decorator()
    async def _load_normalisation_stats(self):
        """Load stats from DB into memory."""
        if not self.db_manager:
            return
        async with self.db_manager.get_session() as session:
            result = session.execute(
                text("SELECT metric, count, mean, m2 FROM normalisation_stats")
            ).fetchall()
            for row in result:
                metric = row.metric
                self.stats[metric] = IncrementalStats(
                    initial_mean=row.mean,
                    initial_m2=row.m2,
                    count=row.count
                )
            logger.info("Loaded normalisation stats for %d metrics", len(result))

    @retry_decorator()
    async def _persist_normalisation_stats(self):
        """Save current stats to DB."""
        if not self.db_manager:
            return
        async with self.db_manager.get_session() as session:
            for metric, stat in self.stats.items():
                session.execute(
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

    # -------------------------------------------------------------------------
    # Feedback recording
    # -------------------------------------------------------------------------
    async def record_feedback(
        self,
        context: Dict[str, Any],
        actual_metrics: Dict[str, float]
    ) -> None:
        """
        Record actual metrics after a request and optionally update weights.
        """
        expert_id = context.get('expert_id')
        if not expert_id or not self.registry:
            logger.warning("Missing expert_id or registry; skipping feedback")
            return

        expert = await self.registry.get_expert(expert_id)
        if not expert:
            logger.warning(f"Expert {expert_id} not found; skipping feedback")
            return

        # Compute predicted cost
        predicted_cost = await self.compute(expert, context)

        # Extract metrics (handle missing gracefully)
        E = actual_metrics.get('energy_joules', 0.0)
        CO2 = actual_metrics.get('carbon_kg', 0.0)
        H = actual_metrics.get('helium_units', 0.0)
        M = actual_metrics.get('material_index', 0.0)
        L = actual_metrics.get('latency_ms', 0.0)
        A = 1.0 - actual_metrics.get('accuracy', 0.5)

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

            # Add to mini-batch buffer
            self._feedback_buffer.append(({
                'E': E, 'CO2': CO2, 'H': H, 'M': M, 'L': L, 'A': A,
                'predicted_cost': predicted_cost,
                'actual_cost': actual_cost,
                'error': error,
                'expert_id': expert_id,
            }, context))

            # If buffer reaches batch size, perform SGD update
            if len(self._feedback_buffer) >= self.batch_size:
                await self._apply_mini_batch()
                self._feedback_buffer.clear()

        # Persist feedback record (with retry)
        await self._persist_feedback(context, actual_metrics, predicted_cost, actual_cost)

        # Notify routing system if weights changed significantly
        if self._routing_callbacks:
            for cb in self._routing_callbacks:
                try:
                    await cb(self.weights)
                except Exception as e:
                    logger.error(f"Routing callback error: {e}")

    # -------------------------------------------------------------------------
    # Mini-batch SGD
    # -------------------------------------------------------------------------
    async def _apply_mini_batch(self):
        """Apply SGD update on the accumulated batch."""
        if not self._feedback_buffer:
            return

        # Accumulate gradients
        grad_sum = {k: 0.0 for k in self.initial_weights.keys()}
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

        # Average gradient
        batch_size = len(self._feedback_buffer)
        for k in grad_sum:
            grad_sum[k] /= batch_size

        # Apply gradient descent with adaptive LR
        lr = self.learning_rate
        async with self._lock:
            weights = self._get_weights(self._feedback_buffer[0][1].get('expert_id'))
            for k in weights:
                weights[k] -= lr * grad_sum[k]
                weights[k] = max(-5.0, min(5.0, weights[k]))  # clamp

            # Decay learning rate
            self.learning_rate *= self.lr_decay

            # Update snapshot if change significant
            if abs(weights['alpha'] - self._last_snapshot.get('alpha', 0)) > 0.1:
                self._last_snapshot = weights.copy()
                await self._persist_weight_history(reason="batch_update")

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
    async def _persist_feedback(self, context: Dict, actual: Dict, pred: float, actual_cost: float):
        if not self.db_manager:
            return
        try:
            await self._db_circuit_breaker.call(
                self._persist_feedback_inner,
                context, actual, pred, actual_cost
            )
        except Exception as e:
            logger.error(f"Feedback persistence failed: {e}")

    @retry_decorator()
    async def _persist_feedback_inner(self, context: Dict, actual: Dict, pred: float, actual_cost: float):
        async with self.db_manager.get_session() as session:
            session.execute(
                text("""
                    INSERT INTO feedback_records
                    (request_id, expert_id, node_id, predicted_cost, actual_cost,
                     energy_joules, carbon_kg, helium_units, latency_ms, accuracy,
                     weights_snapshot)
                    VALUES (:request_id, :expert_id, :node_id, :predicted_cost, :actual_cost,
                     :energy_joules, :carbon_kg, :helium_units, :latency_ms, :accuracy,
                     :weights_snapshot)
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
                    'weights_snapshot': json.dumps(self.weights)
                }
            )

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
            session.execute(
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

        if self.rollback_enabled and mae > self.mae_threshold:
            logger.warning(f"MAE {mae:.4f} exceeds threshold {self.mae_threshold}. Rolling back weights.")
            await self._rollback_weights()

    async def _rollback_weights(self):
        """Restore weights to the last snapshot."""
        async with self._lock:
            self.weights = self._last_snapshot.copy()
            if self.db_manager:
                await self._persist_weight_history(reason="rollback")
            logger.info("Weights rolled back to previous snapshot.")

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
        logger.info("AdaptiveCostFunction stopped")

# =============================================================================
# FastAPI REST API (integrated)
# =============================================================================
app = FastAPI(title="Adaptive Cost Function API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instance (set during startup)
adaptive_function: Optional[AdaptiveCostFunction] = None

# Authentication (simple JWT – for demo)
security = HTTPBearer()
async def verify_jwt(token: str) -> Dict:
    # In production, verify JWT properly
    return {"sub": "admin", "role": "admin"}

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await verify_jwt(credentials.credentials)

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

@app.post("/weights", dependencies=[Depends(get_current_user)])
async def set_weights(new_weights: Dict[str, float], expert_id: Optional[str] = None):
    if not adaptive_function:
        raise HTTPException(status_code=503, detail="Service not initialized")
    # Validate keys
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
        result = session.execute(
            text("SELECT * FROM weight_history ORDER BY id DESC LIMIT :limit"),
            {"limit": limit}
        ).fetchall()
        return {"history": [dict(r._mapping) for r in result]}

@app.post("/weights/export", dependencies=[Depends(get_current_user)])
async def export_weights():
    if not adaptive_function:
        raise HTTPException(status_code=503, detail="Service not initialized")
    data = adaptive_function.export_weights()
    return {"export": data}

@app.post("/weights/import", dependencies=[Depends(get_current_user)])
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
        'initial_weights': {'alpha': 1.0, 'beta': 1.0, 'gamma': 1.0, 'delta': 1.0, 'epsilon': 1.0, 'zeta': 1.0}
    }
    adaptive_function = AdaptiveCostFunction(config)
    # In a real deployment, inject dependencies here.
    logger.info("FastAPI startup: AdaptiveCostFunction initialized")

@app.on_event("shutdown")
async def shutdown():
    if adaptive_function:
        await adaptive_function.stop()
    logger.info("FastAPI shutdown")

# ---------- Unit test stubs (pytest) ----------
# These would be placed in a separate test file, but we include them as comments.
"""
def test_adaptive_cost_function():
    import pytest
    from unittest.mock import Mock, AsyncMock
    config = {'learning_rate': 0.1, 'batch_size': 2, 'initial_weights': {'alpha': 1.0, 'beta': 0.5, 'gamma': 0.0, 'delta': 0.0, 'epsilon': 0.0, 'zeta': 0.0}}
    af = AdaptiveCostFunction(config)
    af.stats['E'].update(10)
    af.stats['CO2'].update(5)
    # ... test normalisation, update, etc.
    assert True
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
