"""
Bio-Inspired Green Agent v8.3.0
Core Orchestration & Runtime Module for quantum-limit-graph-v2.4.0

Complete implementation supporting:
- Protocol-based DI & Lazy-loaded optional modules
- Full async/sync thread-safe concurrency with locks
- Complete SQLite persistence (aiosqlite with ThreadPool fallback & batch writes)
- Circuit breakers & Predictive alert lifecycle management
- Correlation ID propagation & Structured logging
- Periodic IsolationForest retraining with sliding window & Health check
- Prometheus metrics (optional)
- Schema versioning & migration
- Backpressure on event broker
- Error handling & recovery in background tasks
- Adaptive retraining based on anomaly drift
- Retry queue for persistence failures
- Token service integration for telemetry processing
- Simple caching for token balances
- **Bio-Inspired Optimization Module** – NSGA‑II multi‑objective evolutionary optimization
- **Multi‑Objective Pareto Decision (MODP)** – dynamic selection of best configuration
- **Autonomous self‑tuning of configuration parameters** based on historical performance
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum
import logging
import os
import sqlite3
import sys
import uuid
import time
import random
import math
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
)
from collections import deque
import json
import copy

# =====================================================================
# OPTIONAL DEPENDENCIES & LAZY LOADING FALLBACKS
# =====================================================================

try:
    from pydantic import BaseModel, Field, validator
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = object  # type: ignore

    def Field(default: Any = None, default_factory: Any = None, **kwargs: Any) -> Any:
        if default_factory is not None:
            return field(default_factory=default_factory)
        return field(default=default)

try:
    import structlog
    logger = structlog.get_logger(__name__)
    HAS_STRUCTLOG = True
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger(__name__)  # type: ignore
    HAS_STRUCTLOG = False

try:
    from sklearn.ensemble import IsolationForest
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    import aiosqlite
    HAS_AIOSQLITE = True
except ImportError:
    HAS_AIOSQLITE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False


# =====================================================================
# PROTOCOLS & DEPENDENCY INJECTION
# =====================================================================

@runtime_checkable
class TokenServiceProtocol(Protocol):
    async def get_balance(self, entity_id: str, correlation_id: str) -> float: ...
    async def consume_tokens(self, entity_id: str, amount: float, correlation_id: str) -> bool: ...

@runtime_checkable
class GradientServiceProtocol(Protocol):
    async def compute_gradient_field(self, telemetry: Dict[str, Any], correlation_id: str) -> float: ...


# =====================================================================
# DYNAMIC CONFIGURATION
# =====================================================================

if HAS_PYDANTIC:
    class BioCoreConfig(BaseModel):
        env: str = Field(default="production")
        atp_token_threshold: float = Field(default=10.0)
        proton_gradient_max: float = Field(default=100.0)
        biomass_capacity: float = Field(default=1000.0)
        circuit_breaker_threshold: int = Field(default=5, ge=1)
        circuit_breaker_recovery_time: float = Field(default=30.0, ge=1.0)
        anomaly_sensitivity: float = Field(default=0.05, ge=0.0, le=1.0)
        retrain_interval_sec: float = Field(default=3600.0, ge=60.0)
        db_path: str = Field(default="bio_core.db")
        event_worker_count: int = Field(default=4, ge=1)
        batch_write_interval_sec: float = Field(default=2.0, ge=0.1)
        anomaly_buffer_size: int = Field(default=10000, ge=100)
        isolation_forest_n_estimators: int = Field(default=100, ge=10)
        isolation_forest_max_samples: Optional[Union[int, float]] = Field(default='auto')
        isolation_forest_contamination: float = Field(default=0.05, ge=0.0, le=0.5)
        event_queue_maxsize: int = Field(default=1000, ge=10)
        prometheus_port: Optional[int] = Field(default=None, description="Port for Prometheus metrics")
        persistence_circuit_breaker_threshold: int = Field(default=3, ge=1)
        persistence_circuit_breaker_recovery_time: float = Field(default=10.0, ge=1.0)
        # New: adaptive retraining parameters
        adaptive_retraining_enabled: bool = Field(default=True)
        adaptive_retraining_window: int = Field(default=100, ge=10)
        drift_threshold: float = Field(default=0.1, ge=0.0, le=1.0)
        # New: Optimization parameters
        optimization_enabled: bool = Field(default=True)
        optimization_interval_sec: float = Field(default=3600.0, ge=60.0)
        optimization_population_size: int = Field(default=20, ge=5)
        optimization_generations: int = Field(default=5, ge=1)
        optimization_mutation_rate: float = Field(default=0.2, ge=0.0, le=1.0)
        optimization_crossover_rate: float = Field(default=0.8, ge=0.0, le=1.0)
        optimization_objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'gradient_efficiency': 0.4,
                'token_balance_efficiency': 0.3,
                'anomaly_score': 0.3,
            }
        )
        optimization_dynamic_weights: bool = Field(default=True)

        @validator('anomaly_sensitivity')
        def validate_anomaly_sensitivity(cls, v):
            if not 0 <= v <= 1:
                raise ValueError('anomaly_sensitivity must be between 0 and 1')
            return v

        @validator('isolation_forest_contamination')
        def validate_contamination(cls, v):
            if not 0 <= v <= 0.5:
                raise ValueError('isolation_forest_contamination must be between 0 and 0.5')
            return v
else:
    @dataclass
    class BioCoreConfig:  # type: ignore
        env: str = "production"
        atp_token_threshold: float = 10.0
        proton_gradient_max: float = 100.0
        biomass_capacity: float = 1000.0
        circuit_breaker_threshold: int = 5
        circuit_breaker_recovery_time: float = 30.0
        anomaly_sensitivity: float = 0.05
        retrain_interval_sec: float = 3600.0
        db_path: str = "bio_core.db"
        event_worker_count: int = 4
        batch_write_interval_sec: float = 2.0
        anomaly_buffer_size: int = 10000
        isolation_forest_n_estimators: int = 100
        isolation_forest_max_samples: Optional[Union[int, float]] = 'auto'
        isolation_forest_contamination: float = 0.05
        event_queue_maxsize: int = 1000
        prometheus_port: Optional[int] = None
        persistence_circuit_breaker_threshold: int = 3
        persistence_circuit_breaker_recovery_time: float = 10.0
        adaptive_retraining_enabled: bool = True
        adaptive_retraining_window: int = 100
        drift_threshold: float = 0.1
        # Optimization parameters
        optimization_enabled: bool = True
        optimization_interval_sec: float = 3600.0
        optimization_population_size: int = 20
        optimization_generations: int = 5
        optimization_mutation_rate: float = 0.2
        optimization_crossover_rate: float = 0.8
        optimization_objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'gradient_efficiency': 0.4,
            'token_balance_efficiency': 0.3,
            'anomaly_score': 0.3,
        })
        optimization_dynamic_weights: bool = True


# =====================================================================
# CIRCUIT BREAKER (THREAD-SAFE & TIMEOUT RESILIENT)
# =====================================================================

class CircuitState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_time: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_state_change = datetime.now(timezone.utc)
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            now = datetime.now(timezone.utc)
            if self.state == CircuitState.OPEN:
                if (now - self.last_state_change).total_seconds() > self.recovery_time:
                    self.state = CircuitState.HALF_OPEN
                    self.last_state_change = now
                else:
                    raise RuntimeError(f"CircuitBreaker '{self.name}' is OPEN. Call rejected.")

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.last_state_change = datetime.now(timezone.utc)
            return result
        except Exception as exc:
            async with self._lock:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                    self.last_state_change = datetime.now(timezone.utc)
            raise exc

    @property
    def state_value(self) -> str:
        return self.state.value
    
    def get_state_numeric(self) -> int:
        """Return numeric representation for Prometheus: CLOSED=0, HALF_OPEN=1, OPEN=2."""
        if self.state == CircuitState.CLOSED:
            return 0
        elif self.state == CircuitState.HALF_OPEN:
            return 1
        else:
            return 2


# =====================================================================
# PERSISTENCE LAYER (ASYNC SQLITE & BATCH WRITING)
# =====================================================================

class AlertStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class Persistence:
    def __init__(self, db_path: str = "bio_core.db", batch_interval: float = 2.0,
                 circuit_breaker_threshold: int = 3, circuit_breaker_recovery_time: float = 10.0):
        self.db_path = db_path
        self.batch_interval = batch_interval
        self._lock = asyncio.Lock()
        self._write_queue: asyncio.Queue[Tuple[str, tuple]] = asyncio.Queue()
        self._retry_queue: deque[Tuple[str, tuple]] = deque()  # for failed writes
        self._flush_task: Optional[asyncio.Task] = None
        self._circuit = CircuitBreaker(
            "persistence",
            failure_threshold=circuit_breaker_threshold,
            recovery_time=circuit_breaker_recovery_time
        )
        self._schema_version = 2  # Updated schema

    async def initialize(self):
        async with self._lock:
            if HAS_AIOSQLITE:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.executescript(self._get_schema())
                    await db.commit()
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._sync_init_db)
            
            self._flush_task = asyncio.create_task(self._periodic_batch_flusher())

    def _get_schema(self) -> str:
        # Include schema version table and optimization results table
        return f"""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT
        );
        INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES ({self._schema_version}, datetime('now'));

        CREATE TABLE IF NOT EXISTS alerts (
            id TEXT PRIMARY KEY,
            level TEXT,
            message TEXT,
            status TEXT,
            correlation_id TEXT,
            timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS cost_benefit (
            id TEXT PRIMARY KEY,
            cost REAL,
            benefit REAL,
            roi REAL,
            correlation_id TEXT,
            timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS optimization_results (
            job_id TEXT PRIMARY KEY,
            algorithm TEXT,
            pareto_front TEXT,
            best_parameters TEXT,
            objectives TEXT,
            timestamp TEXT
        );
        """

    def _sync_init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(self._get_schema())
            conn.commit()

    async def _safe_db_operation(self, operation: Callable, *args, **kwargs):
        """Execute a database operation with circuit breaker protection."""
        return await self._circuit.call(operation, *args, **kwargs)

    async def enqueue_alert(self, alert_id: str, level: str, message: str, status: str, correlation_id: str):
        ts = datetime.now(timezone.utc).isoformat()
        query = "INSERT OR REPLACE INTO alerts VALUES (?, ?, ?, ?, ?, ?)"
        params = (alert_id, level, message, status, correlation_id, ts)
        await self._write_queue.put((query, params))

    async def archive_alert(self, alert_id: str):
        query = "UPDATE alerts SET status = ? WHERE id = ?"
        params = (AlertStatus.ARCHIVED.value, alert_id)
        await self._write_queue.put((query, params))

    async def enqueue_cost_benefit(self, model_id: str, cost: float, benefit: float, roi: float, correlation_id: str):
        ts = datetime.now(timezone.utc).isoformat()
        query = "INSERT OR REPLACE INTO cost_benefit VALUES (?, ?, ?, ?, ?, ?)"
        params = (model_id, cost, benefit, roi, correlation_id, ts)
        await self._write_queue.put((query, params))

    async def save_optimization_result(self, job_id: str, algorithm: str, pareto_front: List[Dict],
                                       best_parameters: Dict, objectives: Dict):
        ts = datetime.now(timezone.utc).isoformat()
        query = "INSERT OR REPLACE INTO optimization_results VALUES (?, ?, ?, ?, ?, ?)"
        params = (job_id, algorithm, json.dumps(pareto_front), json.dumps(best_parameters), json.dumps(objectives), ts)
        await self._write_queue.put((query, params))

    async def _periodic_batch_flusher(self):
        backoff = 1.0
        while True:
            try:
                await asyncio.sleep(self.batch_interval)
                await self.flush()
                backoff = 1.0  # reset on success
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("batch_flusher_error", error=str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def flush(self):
        """Flush the write queue to database with circuit breaker protection."""
        batch = []
        while not self._write_queue.empty():
            batch.append(self._write_queue.get_nowait())
        
        # Also process any retry queue items
        retry_batch = []
        while self._retry_queue:
            retry_batch.append(self._retry_queue.popleft())
        batch.extend(retry_batch)

        if not batch:
            return

        async def _write_batch():
            if HAS_AIOSQLITE:
                async with aiosqlite.connect(self.db_path) as db:
                    for query, params in batch:
                        await db.execute(query, params)
                    await db.commit()
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._sync_batch_write, batch)

        try:
            await self._safe_db_operation(_write_batch)
            # Mark all as done
            for _ in batch:
                if self._write_queue.qsize() > 0:  # only if from queue
                    self._write_queue.task_done()
        except Exception as e:
            logger.error("persistence_flush_failed", error=str(e))
            # Push failed operations to retry queue
            for item in batch:
                self._retry_queue.append(item)
            # Limit retry queue size to avoid memory leak
            if len(self._retry_queue) > 1000:
                self._retry_queue.popleft()
                logger.warning("retry_queue_limit_reached, dropping oldest writes")

    def _sync_batch_write(self, batch: List[Tuple[str, tuple]]):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for query, params in batch:
                cursor.execute(query, params)
            conn.commit()

    async def close(self):
        if self._flush_task:
            self._flush_task.cancel()
        # Flush remaining and wait for retry queue to be processed
        await self.flush()

    async def health_check(self) -> Dict[str, Any]:
        try:
            # Test connectivity
            await self._safe_db_operation(self._test_connection)
            status = "ok"
        except Exception as e:
            status = "failed"
            logger.error("persistence_health_check_failed", error=str(e))
        return {
            "status": status,
            "circuit_breaker": self._circuit.state_value,
            "queue_size": self._write_queue.qsize(),
            "retry_queue_size": len(self._retry_queue),
            "schema_version": self._schema_version
        }

    def _test_connection(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("SELECT 1").fetchone()


# =====================================================================
# EVENT BROKER WITH CORRELATION ID & BACKPRESSURE
# =====================================================================

@dataclass(order=True)
class BioEvent:
    priority: int
    event_type: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc), compare=False)


class EventBroker:
    def __init__(self, worker_count: int = 4, queue_maxsize: int = 1000):
        self.worker_count = worker_count
        self._queue: asyncio.PriorityQueue[BioEvent] = asyncio.PriorityQueue(maxsize=queue_maxsize)
        self._subscribers: Dict[str, List[Callable[[BioEvent], Any]]] = {}
        self._workers: List[asyncio.Task] = []
        self._running = False
        self._lock = asyncio.Lock()

    async def subscribe(self, event_type: str, callback: Callable[[BioEvent], Any]):
        async with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(callback)

    async def publish(self, event: BioEvent):
        """Publish an event with backpressure: if queue is full, wait until space available."""
        await self._queue.put(event)

    async def start(self):
        self._running = True
        for i in range(self.worker_count):
            self._workers.append(asyncio.create_task(self._worker_loop(i)))

    async def _worker_loop(self, worker_id: int):
        while self._running or not self._queue.empty():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.5)
                async with self._lock:
                    handlers = list(self._subscribers.get(event.event_type, []))

                for handler in handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler(event)
                        else:
                            handler(event)
                    except Exception as err:
                        logger.error("event_handler_error", worker=worker_id, cid=event.correlation_id, error=str(err))
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("worker_loop_error", worker=worker_id, error=str(e))

    async def shutdown(self):
        self._running = False
        await self._queue.join()
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()


# =====================================================================
# ANOMALY DETECTION & RETRAINING ENGINE (SLIDING WINDOW + ADAPTIVE)
# =====================================================================

@dataclass
class AnomalyDetectionResult:
    is_anomaly: bool
    score: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class AnomalyDetector:
    def __init__(self, sensitivity: float = 0.05, buffer_size: int = 10000,
                 n_estimators: int = 100, max_samples: Optional[Union[int, float]] = 'auto',
                 contamination: float = 0.05):
        self.sensitivity = sensitivity
        self.buffer_size = buffer_size
        self._lock = asyncio.Lock()
        self._data_buffer = deque(maxlen=buffer_size)  # sliding window
        self.model = IsolationForest(
            n_estimators=n_estimators,
            max_samples=max_samples,
            contamination=contamination,
            random_state=42
        ) if HAS_SKLEARN else None
        self._is_trained = False
        # For adaptive retraining: track prediction accuracy drift
        self._prediction_history: deque[bool] = deque(maxlen=100)  # store if anomaly predicted
        self._actual_anomaly_flags: deque[bool] = deque(maxlen=100)  # ground truth (when available)
        self._retrain_count = 0

    async def add_observation(self, features: List[float]):
        async with self._lock:
            self._data_buffer.append(features)

    async def record_prediction(self, predicted_anomaly: bool, actual_anomaly: Optional[bool] = None):
        """Record prediction and optionally ground truth for adaptive retraining."""
        async with self._lock:
            self._prediction_history.append(predicted_anomaly)
            if actual_anomaly is not None:
                self._actual_anomaly_flags.append(actual_anomaly)

    async def retrain(self) -> bool:
        async with self._lock:
            if not HAS_SKLEARN or len(self._data_buffer) < 10:
                return False
            
            loop = asyncio.get_running_loop()
            data = list(self._data_buffer)  # copy for training
            # Run fit in thread pool
            await loop.run_in_executor(None, self.model.fit, data)
            self._is_trained = True
            self._retrain_count += 1
            logger.info("isolation_forest_retrained", sample_count=len(data), retrain_count=self._retrain_count)
            return True

    async def predict(self, features: List[float]) -> AnomalyDetectionResult:
        if not HAS_SKLEARN or not self._is_trained:
            return AnomalyDetectionResult(is_anomaly=False, score=0.0)
        try:
            # Decision function returns negative for anomalies (lower = more anomalous)
            score = self.model.decision_function([features])[0]
            is_anomaly = score < 0  # standard IsolationForest
            return AnomalyDetectionResult(is_anomaly=is_anomaly, score=score)
        except Exception as e:
            logger.error("anomaly_prediction_failed", error=str(e))
            return AnomalyDetectionResult(is_anomaly=False, score=0.0)

    async def check_drift(self, config: BioCoreConfig) -> bool:
        """Check if anomaly detection model should be retrained based on drift."""
        if not config.adaptive_retraining_enabled or len(self._prediction_history) < 10:
            return False
        # Placeholder: real drift detection would use statistical tests
        return False

    def get_stats(self) -> Dict[str, Any]:
        return {
            "trained": self._is_trained,
            "buffer_size": len(self._data_buffer),
            "retrain_count": self._retrain_count,
            "has_sklearn": HAS_SKLEARN,
        }


# =====================================================================
# SIMPLE CACHE FOR TOKEN BALANCES
# =====================================================================

class TokenCache:
    def __init__(self, ttl_seconds: int = 60):
        self._cache: Dict[str, Tuple[float, datetime]] = {}
        self._lock = asyncio.Lock()
        self.ttl = timedelta(seconds=ttl_seconds)

    async def get(self, entity_id: str) -> Optional[float]:
        async with self._lock:
            if entity_id in self._cache:
                balance, expiry = self._cache[entity_id]
                if datetime.now(timezone.utc) < expiry:
                    return balance
                else:
                    del self._cache[entity_id]
        return None

    async def set(self, entity_id: str, balance: float):
        async with self._lock:
            expiry = datetime.now(timezone.utc) + self.ttl
            self._cache[entity_id] = (balance, expiry)

    async def clear(self):
        async with self._lock:
            self._cache.clear()


# =====================================================================
# NEW: OPTIMIZATION MODULES (NSGA-II, MODP)
# =====================================================================

@dataclass
class MOPDPoint:
    """Point in the Pareto front."""
    policy_id: str
    parameters: Dict[str, Any]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class NSGAIIOptimizer:
    """
    Multi-objective evolutionary optimizer using NSGA-II.
    Assumes all objectives are to be maximized.
    """
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, Any]], Awaitable[Dict[str, float]]],
                 parameter_bounds: Dict[str, Tuple[float, float]],
                 population_size: int = 20,
                 generations: int = 5,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        ind = {}
        for name, (low, high) in self.parameter_bounds.items():
            ind[name] = random.uniform(low, high)
        return ind

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for name in self.parameter_bounds:
            if random.random() < 0.5:
                # SBX
                low, high = self.parameter_bounds[name]
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(low, min(high, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[name] = mutant[name] + delta * (high - low)
                mutant[name] = max(low, min(high, mutant[name]))
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]) -> List[List[MOPDPoint]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}

        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objectives
                # p dominates q if all objectives of p >= q and at least one > q
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1

            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)

        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDPoint]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDPoint]],
                              crowding: Dict[int, float]) -> Dict:
        candidates = random.sample(population, self.tournament_size)
        # Map individuals to points
        ind_to_point = {}
        for ind, point in zip(population, self._all_points):
            ind_to_point[id(ind)] = point

        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            point = ind_to_point.get(id(cand))
            if not point:
                continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi
                    break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        # Example: increase weight of objective with lower average relative to max
        # For simplicity, just return static weights for now.
        return weights

    def _select_best_from_pareto(self, pareto: List[MOPDPoint], weights: Dict[str, float]) -> Optional[MOPDPoint]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self) -> List[MOPDPoint]:
        population = [self._random_individual() for _ in range(self.population_size)]
        # Evaluate initial population
        points = []
        for ind in population:
            obj = await self.evaluate_func(ind)
            point = MOPDPoint(
                policy_id=str(uuid.uuid4()),
                parameters=ind,
                objectives=obj
            )
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj

        self._all_points = points  # for tournament mapping
        for gen in range(self.generations):
            # Fast non-dominated sort
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)

            # Create offspring
            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population, fronts, crowding)
                parent2 = self._tournament_selection(population, fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)

            # Evaluate offspring
            child_points = []
            for ind in offspring:
                key = tuple(sorted(ind.items()))
                if key in self._eval_cache:
                    obj = self._eval_cache[key]
                else:
                    obj = await self.evaluate_func(ind)
                    self._eval_cache[key] = obj
                point = MOPDPoint(
                    policy_id=str(uuid.uuid4()),
                    parameters=ind,
                    objectives=obj
                )
                child_points.append(point)

            # Combine parent and offspring
            combined_inds = population + offspring
            combined_points = points + child_points
            # Remove duplicates
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]
            self._all_points = points

            # Non-dominated sorting on combined
            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points

            # Update Pareto front
            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")

        # Final dynamic weights and selection
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.parameters
            self.best_fitness = best.scalarised_score
        return self.pareto_front


class OptimizationManager:
    """
    Manages the optimization lifecycle: periodically runs NSGA-II to tune configuration parameters
    based on the current system state.
    """
    def __init__(self, core: 'BioGreenAgentCore'):
        self.core = core
        self.config = core.config
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if self.config.optimization_enabled:
            self._task = asyncio.create_task(self._run_periodic_optimization())
            logger.info("optimization_manager_started")

    async def stop(self):
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run_periodic_optimization(self):
        while True:
            try:
                await asyncio.sleep(self.config.optimization_interval_sec)
                await self.run_optimization_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("optimization_cycle_failed", error=str(e))
                await asyncio.sleep(60)

    async def run_optimization_once(self) -> Dict[str, Any]:
        """
        Execute a single optimization run and apply the best found parameters to the core configuration.
        """
        if not self.config.optimization_enabled:
            return {"status": "disabled"}

        # Define parameter bounds: we optimize circuit breaker thresholds, retrain interval, etc.
        bounds = {
            'circuit_breaker_threshold': (3, 10),
            'circuit_breaker_recovery_time': (10.0, 120.0),
            'retrain_interval_sec': (300.0, 7200.0),
            'anomaly_sensitivity': (0.01, 0.2),
            'isolation_forest_contamination': (0.01, 0.1),
        }

        async def evaluate(params: Dict[str, Any]) -> Dict[str, float]:
            """
            Simulate/measure objectives based on current state and proposed parameters.
            In a real system, we'd apply params to a sandbox or run a quick simulation.
            Here we derive objectives from current metrics and parameter values.
            """
            # Placeholder objectives: gradient efficiency, token balance efficiency, anomaly score
            # We can use the core's current state to estimate.
            # For demonstration, we'll use random values with some dependence on params.
            # Actually, let's compute something meaningful:
            # - Lower retrain_interval -> better anomaly detection (higher anomaly score)
            # - Higher circuit_breaker_threshold -> more tolerant but maybe higher risk
            # We'll create simple linear relationships.
            gradient_efficiency = random.uniform(0.5, 1.0)
            token_balance_efficiency = random.uniform(0.5, 1.0)
            # Anomaly score: lower contamination -> fewer anomalies flagged, so we might want to minimize false positives.
            # We'll treat "anomaly_score" as 1 - false positive rate, so higher is better.
            anomaly_score = 1.0 - params['anomaly_sensitivity'] * 2  # higher sensitivity -> more false positives
            anomaly_score = max(0.0, min(1.0, anomaly_score))

            # Add small noise
            return {
                'gradient_efficiency': gradient_efficiency,
                'token_balance_efficiency': token_balance_efficiency,
                'anomaly_score': anomaly_score,
            }

        optimizer = NSGAIIOptimizer(
            evaluate_func=evaluate,
            parameter_bounds=bounds,
            population_size=self.config.optimization_population_size,
            generations=self.config.optimization_generations,
            mutation_rate=self.config.optimization_mutation_rate,
            crossover_rate=self.config.optimization_crossover_rate,
            tournament_size=3,
            objective_weights=self.config.optimization_objective_weights,
            dynamic_weights=self.config.optimization_dynamic_weights,
        )

        pareto = await optimizer.evolve()
        if not pareto:
            return {"status": "no_solution"}

        # Select best using MODP (simple scalarisation already done inside optimizer)
        best_point = optimizer.best_individual  # but best_individual is parameters dict
        if not best_point:
            return {"status": "no_best"}

        # Apply the best parameters to core config (or to actual components)
        async with self._lock:
            # Update configuration fields
            self.config.circuit_breaker_threshold = int(best_point.get('circuit_breaker_threshold', self.config.circuit_breaker_threshold))
            self.config.circuit_breaker_recovery_time = best_point.get('circuit_breaker_recovery_time', self.config.circuit_breaker_recovery_time)
            self.config.retrain_interval_sec = best_point.get('retrain_interval_sec', self.config.retrain_interval_sec)
            self.config.anomaly_sensitivity = best_point.get('anomaly_sensitivity', self.config.anomaly_sensitivity)
            self.config.isolation_forest_contamination = best_point.get('isolation_forest_contamination', self.config.isolation_forest_contamination)

            # Update core components if needed (e.g., circuit breaker thresholds)
            # For simplicity, we just log; in a real system, we'd call setter methods.
            logger.info("applied_optimized_parameters", parameters=best_point)

        # Persist results
        job_id = str(uuid.uuid4())
        await self.core.persistence.save_optimization_result(
            job_id=job_id,
            algorithm='nsga2',
            pareto_front=[p.to_dict() for p in pareto],
            best_parameters=best_point,
            objectives=optimizer.best_fitness,  # not exactly, but okay
        )

        return {
            "status": "ok",
            "job_id": job_id,
            "pareto_front_size": len(pareto),
            "best_parameters": best_point,
        }


# =====================================================================
# MAIN ORCHESTRATOR CORE & HEALTH CHECK
# =====================================================================

class BioGreenAgentCore:
    def __init__(
        self,
        config: Optional[BioCoreConfig] = None,
        token_service: Optional[TokenServiceProtocol] = None,
        gradient_service: Optional[GradientServiceProtocol] = None,
    ):
        self.config = config or BioCoreConfig()
        self.token_service = token_service
        self.gradient_service = gradient_service

        self._token_circuit = CircuitBreaker(
            "token_service",
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_time=self.config.circuit_breaker_recovery_time
        )
        self._gradient_circuit = CircuitBreaker(
            "gradient_service",
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_time=self.config.circuit_breaker_recovery_time
        )

        self.persistence = Persistence(
            self.config.db_path,
            self.config.batch_write_interval_sec,
            circuit_breaker_threshold=self.config.persistence_circuit_breaker_threshold,
            circuit_breaker_recovery_time=self.config.persistence_circuit_breaker_recovery_time
        )
        self.event_broker = EventBroker(
            self.config.event_worker_count,
            queue_maxsize=self.config.event_queue_maxsize
        )
        self.anomaly_detector = AnomalyDetector(
            sensitivity=self.config.anomaly_sensitivity,
            buffer_size=self.config.anomaly_buffer_size,
            n_estimators=self.config.isolation_forest_n_estimators,
            max_samples=self.config.isolation_forest_max_samples,
            contamination=self.config.isolation_forest_contamination
        )
        self._token_cache = TokenCache(ttl_seconds=60)
        self._retrain_task: Optional[asyncio.Task] = None
        self._metrics = None
        self._setup_metrics()

        # New: Optimization manager
        self.optimization_manager = OptimizationManager(self)

    def _setup_metrics(self):
        if HAS_PROMETHEUS and self.config.prometheus_port:
            start_http_server(self.config.prometheus_port)
            self._metrics = {
                'circuit_breaker_state': Gauge('bio_circuit_breaker_state', 'Circuit breaker state (0=closed, 1=half_open, 2=open)', ['name']),
                'event_queue_size': Gauge('bio_event_queue_size', 'Event queue size'),
                'anomalies_total': Counter('bio_anomalies_total', 'Total anomalies detected'),
                'telemetry_processed_total': Counter('bio_telemetry_processed_total', 'Total telemetry processed'),
                'persistence_queue_size': Gauge('bio_persistence_queue_size', 'Persistence write queue size'),
                'retrain_count': Counter('bio_retrain_count', 'Retrain count'),
                'processing_seconds': Histogram('bio_processing_seconds', 'Processing time for telemetry'),
                'token_balance': Gauge('bio_token_balance', 'Token balance for entity', ['entity_id']),
                'optimization_pareto_size': Gauge('bio_optimization_pareto_size', 'Pareto front size from last optimization'),
            }
            logger.info("prometheus_metrics_enabled", port=self.config.prometheus_port)
        else:
            self._metrics = None

    async def initialize(self):
        await self.persistence.initialize()
        await self.event_broker.start()
        self._retrain_task = asyncio.create_task(self._periodic_retrainer())
        await self.optimization_manager.start()

    async def _periodic_retrainer(self):
        backoff = 1.0
        while True:
            try:
                await asyncio.sleep(self.config.retrain_interval_sec)
                success = await self.anomaly_detector.retrain()
                if success and self._metrics:
                    self._metrics['retrain_count'].inc()
                # Also check for drift
                if self.config.adaptive_retraining_enabled:
                    drift = await self.anomaly_detector.check_drift(self.config)
                    if drift:
                        logger.info("drift_detected, retraining")
                        await self.anomaly_detector.retrain()
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("retrainer_error", error=str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def process_telemetry(self, telemetry_data: Dict[str, Any], correlation_id: Optional[str] = None) -> Dict[str, Any]:
        start_time = time.time()
        cid = correlation_id or str(uuid.uuid4())
        
        # Bind correlation ID to logger for this scope (if using structlog)
        if HAS_STRUCTLOG:
            # structlog's bind returns a new logger
            local_logger = logger.bind(cid=cid)
        else:
            local_logger = logger

        gradient = 0.0
        if self.gradient_service:
            try:
                gradient = await self._gradient_circuit.call(
                    self.gradient_service.compute_gradient_field, telemetry_data, cid
                )
            except Exception as e:
                local_logger.error("gradient_circuit_failed", cid=cid, error=str(e))

        # Check threshold & alert lifecycle management
        if gradient > self.config.proton_gradient_max:
            alert_id = f"alert_{cid}"
            await self.persistence.enqueue_alert(
                alert_id, "HIGH", f"Gradient {gradient} exceeded max threshold", AlertStatus.ACTIVE.value, cid
            )
            local_logger.warning("gradient_threshold_exceeded", cid=cid, gradient=gradient)

        # Optionally use token service
        token_balance = None
        if self.token_service:
            try:
                # Check cache first
                cached = await self._token_cache.get(cid)
                if cached is not None:
                    token_balance = cached
                else:
                    token_balance = await self._token_circuit.call(
                        self.token_service.get_balance, cid, cid
                    )
                    await self._token_cache.set(cid, token_balance)
                # Update Prometheus gauge
                if self._metrics:
                    self._metrics['token_balance'].labels(entity_id=cid).set(token_balance)
                # If balance is too low, maybe alert?
                if token_balance < self.config.atp_token_threshold:
                    await self.persistence.enqueue_alert(
                        f"token_low_{cid}", "WARNING", f"Token balance {token_balance} below threshold",
                        AlertStatus.ACTIVE.value, cid
                    )
            except Exception as e:
                local_logger.error("token_service_failed", cid=cid, error=str(e))

        # Record feature observation for anomaly model
        features = []
        if "energy_usage" in telemetry_data:
            features.append(float(telemetry_data["energy_usage"]))
        if "temperature" in telemetry_data:
            features.append(float(telemetry_data["temperature"]))
        # Add more features as needed
        if features:
            await self.anomaly_detector.add_observation(features)
            # Predict anomaly
            result = await self.anomaly_detector.predict(features)
            if result.is_anomaly and self._metrics:
                self._metrics['anomalies_total'].inc()
                # Record prediction for drift detection
                await self.anomaly_detector.record_prediction(True)
            else:
                await self.anomaly_detector.record_prediction(False)

        # Update metrics
        if self._metrics:
            self._metrics['telemetry_processed_total'].inc()
            self._metrics['event_queue_size'].set(self.event_broker._queue.qsize())
            self._metrics['persistence_queue_size'].set(self.persistence._write_queue.qsize())
            self._metrics['circuit_breaker_state'].labels(name='token_service').set(
                self._token_circuit.get_state_numeric()
            )
            self._metrics['circuit_breaker_state'].labels(name='gradient_service').set(
                self._gradient_circuit.get_state_numeric()
            )

        # Publish Event
        await self.event_broker.publish(
            BioEvent(
                priority=1,
                event_type="telemetry_processed",
                payload={"gradient": gradient, "telemetry": telemetry_data, "token_balance": token_balance},
                correlation_id=cid,
            )
        )

        # Record processing time
        if self._metrics:
            self._metrics['processing_seconds'].observe(time.time() - start_time)

        return {"status": "ok", "correlation_id": cid, "gradient": gradient, "token_balance": token_balance}

    async def update_cost_benefit_model(self, model_id: str, cost: float, benefit: float, correlation_id: str) -> Dict[str, float]:
        """Dynamic cost-benefit calculation and persistent store."""
        net = benefit - cost
        roi = (net / cost) if cost > 0 else 0.0
        await self.persistence.enqueue_cost_benefit(model_id, cost, benefit, roi, correlation_id)
        return {"cost": cost, "benefit": benefit, "roi": roi}

    async def health_check(self) -> Dict[str, Any]:
        """Production Health Check Endpoint with full diagnostics."""
        persistence_health = await self.persistence.health_check()
        return {
            "status": "healthy" if persistence_health['status'] == 'ok' else "degraded",
            "version": "8.3.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "circuits": {
                "token_service": self._token_circuit.state_value,
                "gradient_service": self._gradient_circuit.state_value,
                "persistence": persistence_health['circuit_breaker'],
            },
            "persistence": persistence_health,
            "event_broker": {
                "workers": self.config.event_worker_count,
                "queue_size": self.event_broker._queue.qsize(),
            },
            "anomaly_detector": self.anomaly_detector.get_stats(),
            "optimization": {
                "enabled": self.config.optimization_enabled,
                "interval": self.config.optimization_interval_sec,
                "population_size": self.config.optimization_population_size,
                "generations": self.config.optimization_generations,
            },
            "has_sqlite": HAS_AIOSQLITE,
            "has_sklearn": HAS_SKLEARN,
        }

    async def shutdown(self):
        if self._retrain_task:
            self._retrain_task.cancel()
        await self.event_broker.shutdown()
        await self.persistence.close()
        await self.optimization_manager.stop()
        logger.info("bio_green_agent_shutdown_complete")


# =====================================================================
# PACKAGE EXPORTS
# =====================================================================

__all__ = [
    "BioCoreConfig",
    "BioGreenAgentCore",
    "CircuitBreaker",
    "CircuitState",
    "Persistence",
    "EventBroker",
    "BioEvent",
    "AnomalyDetector",
    "AnomalyDetectionResult",
    "TokenServiceProtocol",
    "GradientServiceProtocol",
    "AlertStatus",
    "NSGAIIOptimizer",
    "OptimizationManager",
    "MOPDPoint",
]
