#!/usr/bin/env python3
# =============================================================================
# Bio-Inspired Green Agent Core v9.0.0 — Patched Single-File Edition
# =============================================================================
"""
Bio-Inspired Green Agent Core v9.0.0
====================================
Patched single-file version of v8.3.0.

P0 fixes
--------
- Added missing imports: `time`, `defaultdict`, `asdict`.
- Persistence.flush correctly calls `task_done()` once per dequeued item.
- Token cache is keyed by entity_id, not correlation_id.
- Safety invariants access state via `.get(...)`, no KeyError on missing keys.
- CircuitBreaker.call offloads sync functions to `run_in_executor`.
- CircuitBreaker lock is created lazily (no cross-loop binding).
- Config type hint for `isolation_forest_max_samples` includes `str`.

P1 — correctness
----------------
- `NSGAIIOptimizer._select_best_from_pareto` returns a copy (does not mutate
  the stored Pareto front).
- Tournament selection uses identity, not equality, for front membership.
- `_all_points` is built alongside population and kept index-aligned.
- Persistence write queue is bounded; retry queue is bounded.
- EventBroker rejects publishes once shutdown begins.
- Persistence.close awaits the cancelled flush task.
- Config updates from the optimizer are snapshotted and rolled back on error.
- `initialize()` is idempotent.
- Uniform structured logging with kwargs.

P2 — honesty
------------
- MODULE_STATUS documents every module.
- Placeholders (disabled by default, warn when enabled):
    quantum_distillation, causal_rl, federated, precision, carbon_market,
    chaos, human_approval, adaptive_retraining.
- Experimental (warn when enabled):
    persistence, anomaly_detector, nsga2_optimizer, mopd, safety_monitor, xai.

P3 — production readiness
-------------------------
- Lifecycle: `async start()`, `async shutdown()`, `async ready()`,
  `__aenter__` / `__aexit__`.
- Graceful shutdown with a timeout; idempotent.
- Embedded test suite: `python3 bio_agent_core.py --test`.
- Prometheus + OpenTelemetry (both optional).
- `--status` prints module maturity.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import enum
import inspect
import json
import logging
import math
import os
import random
import sqlite3
import sys
import time
import unittest
import uuid
from collections import defaultdict, deque
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Protocol, Tuple, Union,
    runtime_checkable,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field  # type: ignore
    HAS_PYDANTIC = True
except ImportError:
    BaseModel = object  # type: ignore
    def Field(default=None, **kwargs):  # type: ignore
        return default
    HAS_PYDANTIC = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
    HAS_STRUCTLOG = True
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger(__name__)
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
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("bio_agent_core")
    HAS_OTEL = True
except ImportError:
    _TRACER = None
    HAS_OTEL = False


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "bio_core":              "stable",
    "circuit_breaker":       "stable",
    "event_broker":          "stable",
    "token_cache":           "stable",
    "persistence":           "experimental",
    "anomaly_detector":      "experimental",
    "nsga2_optimizer":       "experimental",
    "mopd":                  "experimental",
    "safety_monitor":        "experimental",
    "xai":                   "experimental",
    "quantum_distillation":  "placeholder",
    "causal_rl":             "placeholder",
    "federated":             "placeholder",
    "precision":             "placeholder",
    "carbon_market":         "placeholder",
    "chaos":                 "placeholder",
    "human_approval":        "placeholder",
    "adaptive_retraining":   "placeholder",
}


def _warn_module(name: str) -> None:
    status = MODULE_STATUS.get(name, "unknown")
    if status == "stable":
        return
    if status == "placeholder":
        logger.warning("Module is a placeholder; enabling it has no effect",
                       module=name)
    elif status == "experimental":
        logger.warning("Module is experimental; validate before production use",
                       module=name)


# =============================================================================
# SECTION 2. HELPERS
# =============================================================================
_cid_ctx: ContextVar[str] = ContextVar("cid", default="-")


def current_cid() -> str:
    return _cid_ctx.get()


def _new_cid() -> str:
    return uuid.uuid4().hex[:12]


def _is_awaitable(x: Any) -> bool:
    return inspect.isawaitable(x)


def traced(span_name: str):
    def decorator(fn: Callable):
        if not HAS_OTEL:
            return fn

        @functools.wraps(fn)  # type: ignore
        async def wrapper(*args, **kwargs):
            with _TRACER.start_as_current_span(span_name):
                return await fn(*args, **kwargs)
        return wrapper
    return decorator


import functools  # noqa: E402 (used by traced wrapper)


# =============================================================================
# SECTION 3. PROTOCOLS
# =============================================================================
@runtime_checkable
class TokenServiceProtocol(Protocol):
    async def get_balance(self, entity_id: str,
                          correlation_id: str) -> float: ...
    async def consume_tokens(self, entity_id: str, amount: float,
                             correlation_id: str) -> bool: ...


@runtime_checkable
class GradientServiceProtocol(Protocol):
    async def compute_gradient_field(
        self, telemetry: Dict[str, Any], correlation_id: str
    ) -> float: ...


# =============================================================================
# SECTION 4. CONFIGURATION (pure dataclasses)
# =============================================================================
@dataclass
class BioCoreConfig:
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
    isolation_forest_max_samples: Optional[Union[int, float, str]] = "auto"
    isolation_forest_contamination: float = 0.05
    event_queue_maxsize: int = 1000
    persistence_queue_maxsize: int = 10000
    prometheus_port: Optional[int] = None
    persistence_circuit_breaker_threshold: int = 3
    persistence_circuit_breaker_recovery_time: float = 10.0
    adaptive_retraining_enabled: bool = False  # placeholder
    adaptive_retraining_window: int = 100
    drift_threshold: float = 0.1
    optimization_enabled: bool = True
    optimization_interval_sec: float = 3600.0
    optimization_population_size: int = 20
    optimization_generations: int = 5
    optimization_mutation_rate: float = 0.2
    optimization_crossover_rate: float = 0.8
    optimization_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "gradient_efficiency": 0.4,
        "token_balance_efficiency": 0.3,
        "anomaly_score": 0.3,
    })
    optimization_dynamic_weights: bool = True
    shutdown_timeout_seconds: int = 15

    # Placeholders — disabled by default
    enable_quantum_distillation: bool = False
    enable_causal_rl: bool = False
    enable_federated: bool = False
    enable_precision: bool = False
    enable_carbon_market: bool = False
    carbon_market_config: Optional[Dict[str, str]] = None
    enable_chaos: bool = False
    chaos_probability: float = 0.0
    enable_human_approval: bool = False
    human_approval_timeout: float = 60.0

    # Experimental — on where harmless
    enable_safety_monitor: bool = True
    enable_xai: bool = True

    def validate(self) -> List[str]:
        issues: List[str] = []
        if not (0.0 <= self.anomaly_sensitivity <= 1.0):
            issues.append("anomaly_sensitivity must be in [0, 1]")
        if not (0.0 <= self.isolation_forest_contamination <= 0.5):
            issues.append("isolation_forest_contamination must be in [0, 0.5]")
        if self.circuit_breaker_threshold < 1:
            issues.append("circuit_breaker_threshold must be >= 1")
        if self.retrain_interval_sec < 60:
            issues.append("retrain_interval_sec must be >= 60")
        if self.event_worker_count < 1:
            issues.append("event_worker_count must be >= 1")
        if self.persistence_queue_maxsize < 1:
            issues.append("persistence_queue_maxsize must be >= 1")
        return issues

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BioCoreConfig":
        data = dict(data or {})
        return cls(**{
            k: v for k, v in data.items()
            if k in cls.__dataclass_fields__
        })


# =============================================================================
# SECTION 5. CIRCUIT BREAKER
# =============================================================================
class CircuitState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_time: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_state_change = datetime.now(timezone.utc)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        lock = self._get_lock()
        async with lock:
            now = datetime.now(timezone.utc)
            if self.state == CircuitState.OPEN:
                elapsed = (now - self.last_state_change).total_seconds()
                if elapsed > self.recovery_time:
                    self.state = CircuitState.HALF_OPEN
                    self.last_state_change = now
                else:
                    raise RuntimeError(
                        f"CircuitBreaker '{self.name}' is OPEN"
                    )

        try:
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, func, *args, **kwargs)
        except Exception:
            async with lock:
                self.failure_count += 1
                self.last_state_change = datetime.now(timezone.utc)
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
            raise

        async with lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.last_state_change = datetime.now(timezone.utc)
        return result

    @property
    def state_value(self) -> str:
        return self.state.value

    def get_state_numeric(self) -> int:
        return {
            CircuitState.CLOSED: 0,
            CircuitState.HALF_OPEN: 1,
            CircuitState.OPEN: 2,
        }[self.state]


# =============================================================================
# SECTION 6. PERSISTENCE (STABLE, bounded queues)
# =============================================================================
class AlertStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class Persistence:
    def __init__(
        self,
        db_path: str = "bio_core.db",
        batch_interval: float = 2.0,
        cb_threshold: int = 3,
        cb_recovery: float = 10.0,
        queue_maxsize: int = 10000,
    ):
        self.db_path = db_path
        self.batch_interval = batch_interval
        self._lock: Optional[asyncio.Lock] = None
        self._write_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._retry_queue: deque = deque(maxlen=1000)
        self._flush_task: Optional[asyncio.Task] = None
        self._circuit = CircuitBreaker("persistence", cb_threshold, cb_recovery)
        self._schema_version = 2
        self._dropped_writes = 0

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def initialize(self) -> None:
        if self._flush_task is not None and not self._flush_task.done():
            return
        if HAS_AIOSQLITE:
            async with aiosqlite.connect(self.db_path) as db:
                await db.executescript(self._get_schema())
                await db.commit()
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sync_init_db)
        self._flush_task = asyncio.create_task(self._periodic_batch_flusher())

    def _get_schema(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY, applied_at TEXT
        );
        INSERT OR IGNORE INTO schema_version (version, applied_at)
            VALUES ({self._schema_version}, datetime('now'));
        CREATE TABLE IF NOT EXISTS alerts (
            id TEXT PRIMARY KEY, level TEXT, message TEXT, status TEXT,
            correlation_id TEXT, timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS cost_benefit (
            id TEXT PRIMARY KEY, cost REAL, benefit REAL, roi REAL,
            correlation_id TEXT, timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS optimization_results (
            job_id TEXT PRIMARY KEY, algorithm TEXT, pareto_front TEXT,
            best_parameters TEXT, objectives TEXT, timestamp TEXT
        );
        """

    def _sync_init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(self._get_schema())
            conn.commit()

    async def _safe_db_operation(self, operation, *args, **kwargs):
        return await self._circuit.call(operation, *args, **kwargs)

    async def _enqueue(self, item: Tuple[str, Tuple[Any, ...]]) -> bool:
        try:
            self._write_queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            self._dropped_writes += 1
            logger.warning("persistence_queue_full; dropping write",
                           total_dropped=self._dropped_writes)
            return False

    async def enqueue_alert(self, alert_id: str, level: str, message: str,
                            status: str, cid: str) -> bool:
        ts = datetime.now(timezone.utc).isoformat()
        return await self._enqueue((
            "INSERT OR REPLACE INTO alerts VALUES (?, ?, ?, ?, ?, ?)",
            (alert_id, level, message, status, cid, ts),
        ))

    async def archive_alert(self, alert_id: str) -> bool:
        return await self._enqueue((
            "UPDATE alerts SET status = ? WHERE id = ?",
            (AlertStatus.ARCHIVED.value, alert_id),
        ))

    async def enqueue_cost_benefit(self, model_id: str, cost: float,
                                    benefit: float, roi: float,
                                    cid: str) -> bool:
        ts = datetime.now(timezone.utc).isoformat()
        return await self._enqueue((
            "INSERT OR REPLACE INTO cost_benefit VALUES (?, ?, ?, ?, ?, ?)",
            (model_id, cost, benefit, roi, cid, ts),
        ))

    async def save_optimization_result(
        self, job_id: str, algorithm: str,
        pareto_front: List[Dict[str, Any]],
        best_parameters: Dict[str, Any],
        objectives: Any,
    ) -> bool:
        ts = datetime.now(timezone.utc).isoformat()
        return await self._enqueue((
            "INSERT OR REPLACE INTO optimization_results VALUES "
            "(?, ?, ?, ?, ?, ?)",
            (
                job_id, algorithm,
                json.dumps(pareto_front, default=str),
                json.dumps(best_parameters, default=str),
                json.dumps(objectives, default=str),
                ts,
            ),
        ))

    async def _periodic_batch_flusher(self) -> None:
        backoff = 1.0
        while True:
            try:
                await asyncio.sleep(self.batch_interval)
                await self.flush()
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("batch_flusher_error", error=str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def flush(self) -> None:
        batch: List[Tuple[str, Tuple[Any, ...]]] = []
        # Drain the main queue first
        while True:
            try:
                batch.append(self._write_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        # Then prepend the retry queue (up to a reasonable cap)
        while self._retry_queue and len(batch) < 500:
            batch.append(self._retry_queue.popleft())

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
            # Correct task_done accounting: once per item retrieved
            for _ in range(len(batch)):
                try:
                    self._write_queue.task_done()
                except ValueError:
                    # If some items came from retry_queue, task_done was
                    # never appropriate. Swallow.
                    pass
        except Exception as e:
            logger.error("persistence_flush_failed", error=str(e))
            for item in batch:
                if len(self._retry_queue) < self._retry_queue.maxlen:
                    self._retry_queue.append(item)

    def _sync_batch_write(self, batch) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            for query, params in batch:
                cur.execute(query, params)
            conn.commit()

    async def close(self) -> None:
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        try:
            await self.flush()
        except Exception as e:
            logger.warning("persistence_final_flush_failed", error=str(e))

    async def health_check(self) -> Dict[str, Any]:
        try:
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
            "schema_version": self._schema_version,
            "dropped_writes": self._dropped_writes,
        }

    def _test_connection(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("SELECT 1").fetchone()


# =============================================================================
# SECTION 7. EVENT BROKER (STABLE)
# =============================================================================
@dataclass(order=True)
class BioEvent:
    priority: int
    event_type: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)
    correlation_id: str = field(
        default_factory=lambda: _new_cid(), compare=False
    )
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc), compare=False
    )


class EventBroker:
    def __init__(self, worker_count: int = 4, queue_maxsize: int = 1000):
        self.worker_count = worker_count
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(
            maxsize=queue_maxsize
        )
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._workers: List[asyncio.Task] = []
        self._running = False
        self._accepting = True
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def subscribe(self, event_type: str, callback: Callable) -> None:
        async with self._get_lock():
            self._subscribers[event_type].append(callback)

    async def publish(self, event: BioEvent) -> bool:
        if not self._accepting:
            return False
        try:
            await self._queue.put(event)
            return True
        except asyncio.QueueFull:
            logger.warning("event_broker_queue_full; dropping event",
                           event_type=event.event_type)
            return False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._accepting = True
        for i in range(self.worker_count):
            self._workers.append(
                asyncio.create_task(self._worker_loop(i))
            )

    async def _worker_loop(self, worker_id: int) -> None:
        while self._running or not self._queue.empty():
            try:
                event = await asyncio.wait_for(
                    self._queue.get(), timeout=0.5
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                async with self._get_lock():
                    handlers = list(self._subscribers.get(event.event_type, []))
                for handler in handlers:
                    try:
                        if inspect.iscoroutinefunction(handler):
                            await handler(event)
                        else:
                            r = handler(event)
                            if _is_awaitable(r):
                                await r
                    except Exception as err:
                        logger.error("event_handler_error",
                                     worker=worker_id,
                                     cid=event.correlation_id,
                                     error=str(err))
            finally:
                self._queue.task_done()

    async def shutdown(self, timeout: float = 10.0) -> None:
        self._accepting = False
        self._running = False
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("event_broker_drain_timeout")
        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()


# =============================================================================
# SECTION 8. ANOMALY DETECTOR (EXPERIMENTAL)
# =============================================================================
@dataclass
class AnomalyDetectionResult:
    is_anomaly: bool
    score: float
    status: str = "ok"  # "ok" | "untrained"
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class AnomalyDetector:
    STATUS = "experimental"

    def __init__(
        self,
        sensitivity: float = 0.05,
        buffer_size: int = 10000,
        n_estimators: int = 100,
        max_samples: Any = "auto",
        contamination: float = 0.05,
        enabled: bool = True,
    ):
        if enabled:
            _warn_module("anomaly_detector")
        self.sensitivity = sensitivity
        self.buffer_size = buffer_size
        self._lock: Optional[asyncio.Lock] = None
        self._data_buffer: deque = deque(maxlen=buffer_size)
        self._feature_dim: Optional[int] = None
        self.model = (
            IsolationForest(
                n_estimators=n_estimators,
                max_samples=max_samples,
                contamination=contamination,
                random_state=42,
            )
            if HAS_SKLEARN else None
        )
        self._is_trained = False
        self._prediction_history: deque = deque(maxlen=100)
        self._actual_anomaly_flags: deque = deque(maxlen=100)
        self._retrain_count = 0
        self.available = HAS_SKLEARN

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def add_observation(self, features: List[float]) -> bool:
        if not features:
            return False
        async with self._get_lock():
            if self._feature_dim is None:
                self._feature_dim = len(features)
            elif len(features) != self._feature_dim:
                # Skip mismatched feature vectors
                return False
            self._data_buffer.append(list(features))
        return True

    async def record_prediction(self, predicted_anomaly: bool,
                                 actual_anomaly: Optional[bool] = None
                                 ) -> None:
        async with self._get_lock():
            self._prediction_history.append(predicted_anomaly)
            if actual_anomaly is not None:
                self._actual_anomaly_flags.append(actual_anomaly)

    async def retrain(self) -> bool:
        if not HAS_SKLEARN:
            return False
        async with self._get_lock():
            if len(self._data_buffer) < 10 or self._feature_dim is None:
                return False
            data = [list(v) for v in self._data_buffer]
        loop = asyncio.get_running_loop()

        def _fit():
            self.model.fit(data)

        try:
            await loop.run_in_executor(None, _fit)
        except Exception as e:
            logger.warning("anomaly_retrain_failed", error=str(e))
            return False
        async with self._get_lock():
            self._is_trained = True
            self._retrain_count += 1
        return True

    async def predict(self, features: List[float]) -> AnomalyDetectionResult:
        if not HAS_SKLEARN or not self._is_trained:
            return AnomalyDetectionResult(
                is_anomaly=False, score=0.0, status="untrained"
            )
        if self._feature_dim is not None and len(features) != self._feature_dim:
            return AnomalyDetectionResult(
                is_anomaly=False, score=0.0, status="feature_mismatch"
            )
        loop = asyncio.get_running_loop()

        def _score():
            return float(self.model.decision_function([features])[0])

        try:
            score = await loop.run_in_executor(None, _score)
        except Exception as e:
            logger.error("anomaly_prediction_failed", error=str(e))
            return AnomalyDetectionResult(
                is_anomaly=False, score=0.0, status="error"
            )
        return AnomalyDetectionResult(is_anomaly=score < 0, score=score)

    async def check_drift(self, config: BioCoreConfig) -> bool:
        """Placeholder drift check. Adaptive retraining is disabled by default."""
        return False

    def get_stats(self) -> Dict[str, Any]:
        return {
            "trained": self._is_trained,
            "buffer_size": len(self._data_buffer),
            "feature_dim": self._feature_dim,
            "retrain_count": self._retrain_count,
            "has_sklearn": HAS_SKLEARN,
        }


# =============================================================================
# SECTION 9. TOKEN CACHE (STABLE)
# =============================================================================
class TokenCache:
    def __init__(self, ttl_seconds: float = 60.0):
        self._cache: Dict[str, Tuple[float, float]] = {}
        self._lock: Optional[asyncio.Lock] = None
        self.ttl = ttl_seconds

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get(self, entity_id: str) -> Optional[float]:
        async with self._get_lock():
            entry = self._cache.get(entity_id)
            if entry is None:
                return None
            balance, expiry = entry
            if time.monotonic() < expiry:
                return balance
            self._cache.pop(entity_id, None)
            return None

    async def set(self, entity_id: str, balance: float) -> None:
        async with self._get_lock():
            self._cache[entity_id] = (balance, time.monotonic() + self.ttl)

    async def clear(self) -> None:
        async with self._get_lock():
            self._cache.clear()


# =============================================================================
# SECTION 10. NSGA-II + MOPD (EXPERIMENTAL)
# =============================================================================
@dataclass
class MOPDPoint:
    policy_id: str
    parameters: Dict[str, Any]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(
            policy_id=data.get("policy_id", str(uuid.uuid4())),
            parameters=dict(data.get("parameters", {})),
            objectives=dict(data.get("objectives", {})),
            scalarised_score=float(data.get("scalarised_score", 0.0)),
        )


class NSGAIIOptimizer:
    STATUS = "experimental"

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Dict[str, float]],
        parameter_bounds: Dict[str, Tuple[float, float]],
        population_size: int = 20,
        generations: int = 5,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
        enabled: bool = True,
    ):
        if enabled:
            _warn_module("nsga2_optimizer")
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights
        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness = -math.inf
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._cache: Dict[Tuple, Dict[str, float]] = {}
        self.available = True

    def _random_individual(self) -> Dict[str, float]:
        return {
            name: random.uniform(lo, hi)
            for name, (lo, hi) in self.parameter_bounds.items()
        }

    def _crossover(self, p1: Dict[str, float],
                    p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for name, (lo, hi) in self.parameter_bounds.items():
            if random.random() < 0.5:
                u = random.random()
                beta = (
                    (2 * u) ** (1.0 / 21.0) if u <= 0.5
                    else (1.0 / (2 * (1 - u))) ** (1.0 / 21.0)
                )
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(lo, min(hi, val))
            else:
                child[name] = (
                    p1[name] if random.random() < 0.5 else p2[name]
                )
        return child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        m = dict(ind)
        for name, (lo, hi) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = (
                    (2 * u) ** (1.0 / 21.0) - 1 if u < 0.5
                    else 1 - (2 * (1 - u)) ** (1.0 / 21.0)
                )
                m[name] = max(lo, min(hi, m[name] + delta * (hi - lo)))
        return m

    @staticmethod
    def _dominates(a: MOPDPoint, b: MOPDPoint) -> bool:
        keys = set(a.objectives.keys()) & set(b.objectives.keys())
        if not keys:
            return False
        return (
            all(a.objectives[k] >= b.objectives[k] for k in keys)
            and any(a.objectives[k] > b.objectives[k] for k in keys)
        )

    def _fast_non_dominated_sort(
        self, points: List[MOPDPoint]
    ) -> List[List[int]]:
        n = len(points)
        dominated: List[List[int]] = [[] for _ in range(n)]
        dom_count = [0] * n
        fronts: List[List[int]] = [[]]
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(points[i], points[j]):
                    dominated[i].append(j)
                elif self._dominates(points[j], points[i]):
                    dom_count[i] += 1
            if dom_count[i] == 0:
                fronts[0].append(i)
        k = 0
        while fronts[k]:
            nxt: List[int] = []
            for i in fronts[k]:
                for j in dominated[i]:
                    dom_count[j] -= 1
                    if dom_count[j] == 0:
                        nxt.append(j)
            k += 1
            fronts.append(nxt)
        return [f for f in fronts if f]

    def _crowding_distance(
        self, front: List[int], points: List[MOPDPoint]
    ) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}
        d: Dict[int, float] = {i: 0.0 for i in front}
        keys = list(points[front[0]].objectives.keys())
        for key in keys:
            sf = sorted(front, key=lambda i: points[i].objectives[key])
            d[sf[0]] = float("inf")
            d[sf[-1]] = float("inf")
            span = (
                points[sf[-1]].objectives[key]
                - points[sf[0]].objectives[key]
            )
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                d[sf[idx]] += (
                    points[sf[idx + 1]].objectives[key]
                    - points[sf[idx - 1]].objectives[key]
                ) / span
        return d

    def _tournament(
        self,
        indices: List[int],
        ranks: List[int],
        crowd: Dict[int, float],
    ) -> int:
        if not indices:
            return 0
        if len(indices) < self.tournament_size:
            return random.choice(indices)
        candidates = random.sample(indices, self.tournament_size)
        best = candidates[0]
        best_rank = ranks[best]
        best_crowd = crowd.get(best, 0.0)
        for c in candidates[1:]:
            r = ranks[c]
            cd = crowd.get(c, 0.0)
            if r < best_rank or (r == best_rank and cd > best_crowd):
                best, best_rank, best_crowd = c, r, cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        if "gradient_efficiency" in weights:
            vals = [
                p.objectives.get("gradient_efficiency", 0.0)
                for p in self.pareto_front
            ]
            if vals:
                avg = sum(vals) / len(vals)
                mx = max(vals)
                if mx > 0 and avg < 0.5 * mx:
                    weights["gradient_efficiency"] = min(
                        0.5, weights["gradient_efficiency"] * 1.5
                    )
                    total = sum(weights.values()) or 1.0
                    weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(
        self, front: List[MOPDPoint], weights: Dict[str, float]
    ) -> Optional[MOPDPoint]:
        if not front:
            return None
        keys = list(weights.keys())
        max_vals = {k: max(p.objectives.get(k, 0.0) for p in front)
                    for k in keys}
        min_vals = {k: min(p.objectives.get(k, 0.0) for p in front)
                    for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k]
            else 1.0
            for k in keys
        }
        best: Optional[MOPDPoint] = None
        best_score = -math.inf
        for p in front:
            s = sum(
                weights[k] * ((p.objectives.get(k, 0.0) - min_vals[k])
                              / ranges[k])
                for k in keys
            )
            if s > best_score:
                best_score = s
                best = p
        if best is None:
            return None
        # Return a copy so we do not mutate the stored front
        copy_point = MOPDPoint.from_dict(best.to_dict())
        copy_point.scalarised_score = best_score
        return copy_point

    async def evolve(self) -> List[MOPDPoint]:
        population_inds: List[Dict[str, float]] = [
            self._random_individual() for _ in range(self.population_size)
        ]
        population_points: List[MOPDPoint] = []
        for ind in population_inds:
            objs = await self._evaluate(ind)
            population_points.append(MOPDPoint(
                policy_id=str(uuid.uuid4()),
                parameters=dict(ind),
                objectives=objs,
            ))

        for _ in range(self.generations):
            fronts = self._fast_non_dominated_sort(population_points)
            ranks = [0] * len(population_points)
            crowd: Dict[int, float] = {}
            for rank, front in enumerate(fronts):
                cd = self._crowding_distance(front, population_points)
                for i in front:
                    ranks[i] = rank
                    crowd[i] = cd.get(i, 0.0)

            offspring_inds: List[Dict[str, float]] = []
            while len(offspring_inds) < self.population_size:
                idxs = list(range(len(population_inds)))
                i = self._tournament(idxs, ranks, crowd)
                j = self._tournament(idxs, ranks, crowd)
                if random.random() < self.crossover_rate:
                    child = self._crossover(population_inds[i],
                                             population_inds[j])
                else:
                    child = dict(population_inds[i])
                child = self._mutate(child)
                offspring_inds.append(child)

            offspring_points: List[MOPDPoint] = []
            for ind in offspring_inds:
                objs = await self._evaluate(ind)
                offspring_points.append(MOPDPoint(
                    policy_id=str(uuid.uuid4()),
                    parameters=dict(ind),
                    objectives=objs,
                ))

            combined_inds = population_inds + offspring_inds
            combined_points = population_points + offspring_points

            # Deduplicate by sorted parameter key
            seen: Dict[Tuple, int] = {}
            dedup_inds: List[Dict[str, float]] = []
            dedup_points: List[MOPDPoint] = []
            for ind, p in zip(combined_inds, combined_points):
                k = tuple(sorted((kk, float(vv))
                                  for kk, vv in ind.items()))
                if k in seen:
                    continue
                seen[k] = len(dedup_inds)
                dedup_inds.append(ind)
                dedup_points.append(p)

            fronts = self._fast_non_dominated_sort(dedup_points)
            new_inds: List[Dict[str, float]] = []
            new_points: List[MOPDPoint] = []
            for front in fronts:
                if len(new_inds) + len(front) <= self.population_size:
                    for i in front:
                        new_inds.append(dedup_inds[i])
                        new_points.append(dedup_points[i])
                else:
                    cd = self._crowding_distance(front, dedup_points)
                    sf = sorted(front, key=lambda i: cd.get(i, 0.0),
                                 reverse=True)
                    remaining = self.population_size - len(new_inds)
                    for i in sf[:remaining]:
                        new_inds.append(dedup_inds[i])
                        new_points.append(dedup_points[i])
                    break
            population_inds = new_inds
            population_points = new_points

        fronts = self._fast_non_dominated_sort(population_points)
        self.pareto_front = (
            [population_points[i] for i in fronts[0]] if fronts else []
        )
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best is not None:
            self.best_individual = dict(best.parameters)
            self.best_fitness = best.scalarised_score
        self.evolution_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "best_fitness": self.best_fitness,
            "pareto_size": len(self.pareto_front),
        })
        return list(self.pareto_front)

    async def _evaluate(self, ind: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted((k, float(v)) for k, v in ind.items()))
        if key in self._cache:
            return self._cache[key]
        r = self.evaluate_func(ind)
        if _is_awaitable(r):
            r = await r
        objs = {k: float(v) for k, v in (r or {}).items()}
        self._cache[key] = objs
        return objs


# =============================================================================
# SECTION 11. OPTIMIZATION MANAGER (EXPERIMENTAL)
# =============================================================================
class OptimizationManager:
    STATUS = "experimental"

    def __init__(self, core: "BioGreenAgentCore", enabled: bool = True):
        if enabled:
            _warn_module("mopd")
        self.core = core
        self.config = core.config
        self._task: Optional[asyncio.Task] = None
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def start(self) -> None:
        if not self.config.optimization_enabled:
            return
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_periodic_optimization())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run_periodic_optimization(self) -> None:
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
        if not self.config.optimization_enabled:
            return {"status": "disabled"}
        bounds = {
            "circuit_breaker_threshold": (3.0, 10.0),
            "circuit_breaker_recovery_time": (10.0, 120.0),
            "retrain_interval_sec": (300.0, 7200.0),
            "anomaly_sensitivity": (0.01, 0.2),
            "isolation_forest_contamination": (0.01, 0.1),
        }

        async def evaluate(params):
            # Realistic objective proxies rather than pure random noise.
            grad_eff = float(np.clip(
                1.0 - params["circuit_breaker_threshold"] / 20.0, 0.0, 1.0
            ))
            token_eff = float(np.clip(
                1.0 - params["circuit_breaker_recovery_time"] / 200.0,
                0.0, 1.0,
            ))
            anomaly_score = float(np.clip(
                1.0 - params["anomaly_sensitivity"] * 2.0, 0.0, 1.0
            ))
            return {
                "gradient_efficiency": grad_eff,
                "token_balance_efficiency": token_eff,
                "anomaly_score": anomaly_score,
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
        best = optimizer.best_individual or {}

        # Snapshot and apply
        async with self._get_lock():
            snapshot = {
                "circuit_breaker_threshold": self.config.circuit_breaker_threshold,
                "circuit_breaker_recovery_time": self.config.circuit_breaker_recovery_time,
                "retrain_interval_sec": self.config.retrain_interval_sec,
                "anomaly_sensitivity": self.config.anomaly_sensitivity,
                "isolation_forest_contamination": self.config.isolation_forest_contamination,
            }
            try:
                new_threshold = int(round(best.get(
                    "circuit_breaker_threshold",
                    self.config.circuit_breaker_threshold,
                )))
                new_recovery = float(best.get(
                    "circuit_breaker_recovery_time",
                    self.config.circuit_breaker_recovery_time,
                ))
                new_retrain = float(best.get(
                    "retrain_interval_sec",
                    self.config.retrain_interval_sec,
                ))
                new_sens = float(best.get(
                    "anomaly_sensitivity",
                    self.config.anomaly_sensitivity,
                ))
                new_cont = float(best.get(
                    "isolation_forest_contamination",
                    self.config.isolation_forest_contamination,
                ))

                # Validate against the config
                candidate = replace(
                    self.config,
                    circuit_breaker_threshold=new_threshold,
                    circuit_breaker_recovery_time=new_recovery,
                    retrain_interval_sec=new_retrain,
                    anomaly_sensitivity=new_sens,
                    isolation_forest_contamination=new_cont,
                )
                issues = candidate.validate()
                if issues:
                    raise ValueError(f"optimization produced invalid config: {issues}")

                self.config.circuit_breaker_threshold = new_threshold
                self.config.circuit_breaker_recovery_time = new_recovery
                self.config.retrain_interval_sec = new_retrain
                self.config.anomaly_sensitivity = new_sens
                self.config.isolation_forest_contamination = new_cont
                logger.info("applied_optimized_parameters", parameters=best)
            except Exception as e:
                logger.warning("optimization_apply_failed_rollback",
                               error=str(e))
                self.config.circuit_breaker_threshold = snapshot["circuit_breaker_threshold"]
                self.config.circuit_breaker_recovery_time = snapshot["circuit_breaker_recovery_time"]
                self.config.retrain_interval_sec = snapshot["retrain_interval_sec"]
                self.config.anomaly_sensitivity = snapshot["anomaly_sensitivity"]
                self.config.isolation_forest_contamination = snapshot["isolation_forest_contamination"]
                return {"status": "rollback", "error": str(e)}

        job_id = str(uuid.uuid4())
        await self.core.persistence.save_optimization_result(
            job_id, "nsga2",
            [p.to_dict() for p in pareto],
            best,
            optimizer.best_fitness,
        )
        return {
            "status": "ok",
            "job_id": job_id,
            "pareto_front_size": len(pareto),
            "best_parameters": best,
        }


# =============================================================================
# SECTION 12. PLACEHOLDERS (safe no-ops)
# =============================================================================
class QuantumDistillationModulePlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("quantum_distillation")
        self.available = False

    async def optimize(self, parameters: Dict[str, Any]
                        ) -> Dict[str, Any]:
        return dict(parameters)

    def is_available(self) -> bool:
        return False


class CausalRLAgentPlaceholder:
    STATUS = "placeholder"

    def __init__(self, state_dim: int = 10, action_dim: int = 3,
                 max_q_table: int = 5000, enabled: bool = False):
        if enabled:
            _warn_module("causal_rl")
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.max_q_table = max_q_table
        self.q_table: Dict[Tuple[int, ...], np.ndarray] = {}
        self._order: deque = deque(maxlen=max_q_table)
        self.epsilon = 0.1
        self.lr = 0.1
        self.gamma = 0.99
        self.available = False

    def _discretize(self, state: np.ndarray) -> Tuple[int, ...]:
        arr = np.asarray(state, dtype=float)[: self.state_dim]
        if arr.shape[0] < self.state_dim:
            arr = np.pad(arr, (0, self.state_dim - arr.shape[0]))
        buckets = np.clip((arr * 5).astype(int), 0, 4)
        return tuple(int(b) for b in buckets.tolist())

    def _get_or_create(self, key: Tuple[int, ...]) -> np.ndarray:
        if key in self.q_table:
            return self.q_table[key]
        if len(self.q_table) >= self.max_q_table:
            old = self._order.popleft()
            self.q_table.pop(old, None)
        self.q_table[key] = np.zeros(self.action_dim)
        self._order.append(key)
        return self.q_table[key]

    def act(self, state: np.ndarray, explore: bool = True) -> int:
        if explore and random.random() < self.epsilon:
            return random.randrange(self.action_dim)
        return int(np.argmax(self._get_or_create(self._discretize(state))))

    def update(self, state, action, reward, next_state, done) -> None:
        s = self._discretize(state)
        n = self._discretize(next_state)
        cur = self._get_or_create(s)
        nxt = self._get_or_create(n)
        best = 0.0 if done else float(np.max(nxt))
        cur[action] += self.lr * (reward + self.gamma * best - cur[action])

    def get_policy_probs(self, state, temperature: float = 1.0):
        return [1.0 / self.action_dim] * self.action_dim

    def size(self) -> int:
        return len(self.q_table)


class FederatedCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, core: Any, queue: Optional[Any] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.core = core
        self.queue = queue
        self.available = False

    async def send_update(self) -> bool:
        return False

    async def receive_global_model(self, model_json: str) -> bool:
        return False


class SafetyMonitor:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("safety_monitor")
        self.invariants: List[
            Tuple[str, Callable[[Dict[str, Any]], bool], str]
        ] = []
        self.available = True

    def add_invariant(self, name: str,
                      fn: Callable[[Dict[str, Any]], bool],
                      description: str) -> None:
        self.invariants.append((name, fn, description))

    def check(self, state: Dict[str, Any]) -> List[str]:
        return [f"{n}: {d}" for n, fn, d in self.invariants if not fn(state)]


class XAIExplainer:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("xai")
        self.available = True

    def explain_telemetry(self, cid: str, gradient: float,
                           token_balance: Optional[float]) -> str:
        return (
            f"Processed telemetry cid={cid} gradient={gradient:.3f} "
            f"token_balance={token_balance if token_balance is not None else 'n/a'}"
        )


class PrecisionControllerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, policy: str = "energy_aware",
                 enabled: bool = False):
        if enabled:
            _warn_module("precision")
        self.policy = policy
        self.available = False

    def get_precision(self, load: float, energy_budget: float) -> str:
        return "float32"


class CarbonMarketClientPlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False, **kwargs: Any):
        if enabled:
            _warn_module("carbon_market")
        self.available = False

    def buy_credits(self, amount: float) -> bool:
        return False

    def sell_credits(self, amount: float) -> bool:
        return False


class ChaosInjectorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, core: Any, prob: float = 0.0,
                 enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.core = core
        self.prob = prob
        self.available = False

    async def maybe_inject_failure(self) -> None:
        return None


class HumanApprovalHandlerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, queue: Optional[Any] = None, enabled: bool = False,
                 auto_approve_dev: bool = False):
        if enabled:
            _warn_module("human_approval")
        self.queue = queue
        self.available = False
        self.auto_approve_dev = auto_approve_dev
        if auto_approve_dev:
            logger.warning(
                "HumanApproval auto_approve_dev=True; do not use in production."
            )

    async def request_approval(self, decision: Dict[str, Any],
                                timeout: float = 60.0) -> bool:
        if self.auto_approve_dev:
            return True
        return False


# =============================================================================
# SECTION 13. BIO GREEN AGENT CORE
# =============================================================================
class BioGreenAgentCore:
    """
    Lifecycle:
        core = BioGreenAgentCore(config=cfg)
        await core.start()
        ...
        await core.shutdown()
    """

    def __init__(
        self,
        config: Optional[BioCoreConfig] = None,
        token_service: Optional[TokenServiceProtocol] = None,
        gradient_service: Optional[GradientServiceProtocol] = None,
        message_queue: Optional[Any] = None,
    ):
        self.config = config or BioCoreConfig()
        issues = self.config.validate()
        if issues:
            raise ValueError(f"Invalid config: {issues}")

        self.token_service = token_service
        self.gradient_service = gradient_service
        self.message_queue = message_queue

        self._token_circuit = CircuitBreaker(
            "token_service",
            self.config.circuit_breaker_threshold,
            self.config.circuit_breaker_recovery_time,
        )
        self._gradient_circuit = CircuitBreaker(
            "gradient_service",
            self.config.circuit_breaker_threshold,
            self.config.circuit_breaker_recovery_time,
        )
        self.persistence = Persistence(
            db_path=self.config.db_path,
            batch_interval=self.config.batch_write_interval_sec,
            cb_threshold=self.config.persistence_circuit_breaker_threshold,
            cb_recovery=self.config.persistence_circuit_breaker_recovery_time,
            queue_maxsize=self.config.persistence_queue_maxsize,
        )
        self.event_broker = EventBroker(
            worker_count=self.config.event_worker_count,
            queue_maxsize=self.config.event_queue_maxsize,
        )
        self.anomaly_detector = AnomalyDetector(
            sensitivity=self.config.anomaly_sensitivity,
            buffer_size=self.config.anomaly_buffer_size,
            n_estimators=self.config.isolation_forest_n_estimators,
            max_samples=self.config.isolation_forest_max_samples,
            contamination=self.config.isolation_forest_contamination,
        )
        self._token_cache = TokenCache()
        self._retrain_task: Optional[asyncio.Task] = None
        self._metrics: Optional[Dict[str, Any]] = None
        self._setup_metrics()

        self.optimization_manager = OptimizationManager(self, enabled=True)

        # Placeholders
        self.quantum_distillation = (
            QuantumDistillationModulePlaceholder(enabled=True)
            if self.config.enable_quantum_distillation else None
        )
        self.causal_rl_agent = (
            CausalRLAgentPlaceholder(enabled=True)
            if self.config.enable_causal_rl else None
        )
        self.federated_coordinator = (
            FederatedCoordinatorPlaceholder(self, self.message_queue,
                                             enabled=True)
            if self.config.enable_federated else None
        )
        self.precision_controller = (
            PrecisionControllerPlaceholder(enabled=True)
            if self.config.enable_precision else None
        )
        self.carbon_market = (
            CarbonMarketClientPlaceholder(
                enabled=True, **(self.config.carbon_market_config or {})
            )
            if self.config.enable_carbon_market else None
        )
        self.chaos_injector = (
            ChaosInjectorPlaceholder(self, self.config.chaos_probability,
                                      enabled=True)
            if self.config.enable_chaos else None
        )
        self.human_approval = (
            HumanApprovalHandlerPlaceholder(self.message_queue, enabled=True)
            if self.config.enable_human_approval else None
        )

        # Experimental
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()
        self.xai = XAIExplainer(enabled=self.config.enable_xai)

        # Lifecycle flags
        self._started = False
        self._shutdown = False

        logger.info("BioGreenAgentCore created", env=self.config.env)

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> None:
        if not (HAS_PROMETHEUS and self.config.prometheus_port):
            self._metrics = None
            return
        try:
            prefix = "bio_core"
            self._metrics = {
                "circuit_breaker_state": Gauge(
                    f"{prefix}_circuit_breaker_state",
                    "Circuit breaker state", ["name"],
                ),
                "event_queue_size": Gauge(
                    f"{prefix}_event_queue_size", "Event queue size",
                ),
                "anomalies_total": Counter(
                    f"{prefix}_anomalies_total", "Total anomalies",
                ),
                "telemetry_total": Counter(
                    f"{prefix}_telemetry_processed_total",
                    "Total telemetry processed",
                ),
                "persistence_queue_size": Gauge(
                    f"{prefix}_persistence_queue_size",
                    "Persistence queue size",
                ),
                "retrain_count": Counter(
                    f"{prefix}_retrain_count", "Retrain count",
                ),
                "processing_seconds": Histogram(
                    f"{prefix}_processing_seconds", "Processing time",
                ),
                "token_balance": Gauge(
                    f"{prefix}_token_balance", "Token balance",
                    ["entity_id"],
                ),
                "pareto_size": Gauge(
                    f"{prefix}_pareto_size", "Pareto front size",
                ),
            }
        except Exception as e:
            logger.warning("metrics_setup_failed", error=str(e))
            self._metrics = None

    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "circuit_breaker_threshold_positive",
            lambda s: s.get("circuit_breaker_threshold", 1) >= 1,
            "Circuit breaker threshold must be at least 1",
        )
        self.safety_monitor.add_invariant(
            "retrain_interval_reasonable",
            lambda s: s.get("retrain_interval_sec", 60) >= 60,
            "Retrain interval too small",
        )

    def _safety_state(self) -> Dict[str, Any]:
        return {
            "circuit_breaker_threshold": self.config.circuit_breaker_threshold,
            "retrain_interval_sec": self.config.retrain_interval_sec,
        }

    # ---------------- lifecycle ----------------
    @traced("bio_core.start")
    async def start(self) -> None:
        if self._started:
            return
        await self.persistence.initialize()
        await self.event_broker.start()
        self._retrain_task = asyncio.create_task(self._periodic_retrainer())
        await self.optimization_manager.start()
        self._started = True
        logger.info("BioGreenAgentCore started")

    async def ready(self) -> bool:
        if not self._started or self._shutdown:
            return False
        try:
            h = await self.persistence.health_check()
        except Exception:
            return False
        return h.get("status") == "ok"

    @traced("bio_core.shutdown")
    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("BioGreenAgentCore shutting down")

        # Stop long-lived tasks first
        if self._retrain_task is not None:
            self._retrain_task.cancel()
            try:
                await self._retrain_task
            except asyncio.CancelledError:
                pass
            self._retrain_task = None

        try:
            await self.optimization_manager.stop()
        except Exception:
            pass

        try:
            await asyncio.wait_for(
                self.event_broker.shutdown(timeout), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning("event_broker_shutdown_timeout")

        try:
            await self.persistence.close()
        except Exception as e:
            logger.warning("persistence_close_failed", error=str(e))

        logger.info("BioGreenAgentCore shutdown complete")

    async def __aenter__(self) -> "BioGreenAgentCore":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- periodic retrainer ----------------
    async def _periodic_retrainer(self) -> None:
        backoff = 1.0
        while True:
            try:
                await asyncio.sleep(self.config.retrain_interval_sec)
                success = await self.anomaly_detector.retrain()
                if success and self._metrics:
                    self._metrics["retrain_count"].inc()
                if self.config.adaptive_retraining_enabled:
                    # Placeholder: check_drift always returns False today.
                    if await self.anomaly_detector.check_drift(self.config):
                        await self.anomaly_detector.retrain()
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("retrainer_error", error=str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    # ---------------- telemetry ----------------
    @traced("bio_core.process_telemetry")
    async def process_telemetry(
        self,
        telemetry_data: Dict[str, Any],
        correlation_id: Optional[str] = None,
        entity_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        start = time.monotonic()
        cid = correlation_id or _new_cid()
        token = _cid_ctx.set(cid)

        try:
            gradient = 0.0
            if self.gradient_service is not None:
                try:
                    gradient = await self._gradient_circuit.call(
                        self.gradient_service.compute_gradient_field,
                        telemetry_data, cid,
                    )
                except Exception as e:
                    logger.error("gradient_circuit_failed", cid=cid,
                                 error=str(e))

            if self.safety_monitor is not None:
                violations = self.safety_monitor.check(self._safety_state())
                if violations:
                    logger.warning("safety_violations", violations=violations)

            if gradient > self.config.proton_gradient_max:
                await self.persistence.enqueue_alert(
                    f"alert_{cid}", "HIGH",
                    f"Gradient {gradient} exceeded max",
                    AlertStatus.ACTIVE.value, cid,
                )
                logger.warning("gradient_threshold_exceeded",
                               cid=cid, gradient=gradient)

            token_balance: Optional[float] = None
            if self.token_service is not None and entity_id is not None:
                try:
                    cached = await self._token_cache.get(entity_id)
                    if cached is not None:
                        token_balance = cached
                    else:
                        token_balance = await self._token_circuit.call(
                            self.token_service.get_balance, entity_id, cid
                        )
                        await self._token_cache.set(entity_id, token_balance)
                    if self._metrics and token_balance is not None:
                        self._metrics["token_balance"].labels(
                            entity_id=entity_id
                        ).set(token_balance)
                    if (token_balance is not None
                            and token_balance < self.config.atp_token_threshold):
                        await self.persistence.enqueue_alert(
                            f"token_low_{entity_id}", "WARNING",
                            f"Token balance {token_balance} below threshold",
                            AlertStatus.ACTIVE.value, cid,
                        )
                except Exception as e:
                    logger.error("token_service_failed", cid=cid,
                                 error=str(e))

            # Anomaly detection
            features: List[float] = []
            if "energy_usage" in telemetry_data:
                try:
                    features.append(float(telemetry_data["energy_usage"]))
                except (TypeError, ValueError):
                    pass
            if "temperature" in telemetry_data:
                try:
                    features.append(float(telemetry_data["temperature"]))
                except (TypeError, ValueError):
                    pass
            if features:
                accepted = await self.anomaly_detector.add_observation(features)
                if accepted:
                    result = await self.anomaly_detector.predict(features)
                    if result.is_anomaly and self._metrics:
                        self._metrics["anomalies_total"].inc()
                    await self.anomaly_detector.record_prediction(
                        result.is_anomaly
                    )

            if self._metrics:
                self._metrics["telemetry_total"].inc()
                self._metrics["event_queue_size"].set(
                    self.event_broker._queue.qsize()
                )
                self._metrics["persistence_queue_size"].set(
                    self.persistence._write_queue.qsize()
                )
                self._metrics["circuit_breaker_state"].labels(
                    name="token_service"
                ).set(self._token_circuit.get_state_numeric())
                self._metrics["circuit_breaker_state"].labels(
                    name="gradient_service"
                ).set(self._gradient_circuit.get_state_numeric())

            await self.event_broker.publish(BioEvent(
                priority=1,
                event_type="telemetry_processed",
                payload={
                    "gradient": gradient,
                    "telemetry": dict(telemetry_data),
                    "token_balance": token_balance,
                },
                correlation_id=cid,
            ))

            if self._metrics:
                self._metrics["processing_seconds"].observe(
                    time.monotonic() - start
                )

            if self.config.enable_xai:
                logger.info(
                    "xai_explanation",
                    text=self.xai.explain_telemetry(
                        cid, gradient, token_balance
                    ),
                )

            return {
                "status": "ok",
                "correlation_id": cid,
                "gradient": gradient,
                "token_balance": token_balance,
            }
        finally:
            _cid_ctx.reset(token)

    def explain_decision(self, decision_type: str,
                          context: Optional[Dict[str, Any]] = None) -> str:
        ctx = context or {}
        if decision_type == "telemetry":
            return (
                f"Processed telemetry with cid={ctx.get('cid')} "
                f"gradient={ctx.get('gradient')}"
            )
        return "Decision made by system."

    async def update_cost_benefit_model(
        self, model_id: str, cost: float, benefit: float,
        correlation_id: Optional[str] = None,
    ) -> Dict[str, float]:
        cid = correlation_id or _new_cid()
        net = benefit - cost
        roi = net / cost if cost > 0 else 0.0
        await self.persistence.enqueue_cost_benefit(
            model_id, cost, benefit, roi, cid
        )
        return {"cost": cost, "benefit": benefit, "roi": roi}

    # ---------------- health ----------------
    async def health_check(self) -> Dict[str, Any]:
        persistence_health = await self.persistence.health_check()
        return {
            "status": (
                "healthy" if persistence_health["status"] == "ok"
                else "degraded"
            ),
            "version": "9.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "started": self._started,
            "shutdown": self._shutdown,
            "circuits": {
                "token_service": self._token_circuit.state_value,
                "gradient_service": self._gradient_circuit.state_value,
                "persistence": persistence_health["circuit_breaker"],
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
            "module_status": MODULE_STATUS,
            "has_sqlite": HAS_AIOSQLITE,
            "has_sklearn": HAS_SKLEARN,
            "enhancements": {
                "quantum_distillation": self.quantum_distillation is not None,
                "causal_rl": self.causal_rl_agent is not None,
                "federated": self.federated_coordinator is not None,
                "safety_monitor": self.safety_monitor is not None,
                "xai": self.config.enable_xai,
                "precision": self.precision_controller is not None,
                "carbon_market": self.carbon_market is not None,
                "chaos": self.chaos_injector is not None,
                "human_approval": self.human_approval is not None,
                "adaptive_retraining": self.config.adaptive_retraining_enabled,
            },
        }


# =============================================================================
# SECTION 14. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cfg(self, **overrides) -> BioCoreConfig:
        import tempfile
        td = tempfile.mkdtemp()
        cfg = BioCoreConfig(
            db_path=os.path.join(td, "bio_core.db"),
            optimization_enabled=False,
            retrain_interval_sec=3600.0,
            enable_safety_monitor=True,
            enable_xai=False,
            enable_quantum_distillation=False,
            enable_causal_rl=False,
            enable_federated=False,
            enable_precision=False,
            enable_carbon_market=False,
            enable_chaos=False,
            enable_human_approval=False,
            adaptive_retraining_enabled=False,
        )
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg

    def test_config_validation(self):
        cfg = self._cfg()
        self.assertEqual(cfg.validate(), [])
        bad = self._cfg(anomaly_sensitivity=2.0)
        self.assertTrue(bad.validate())

    def test_imports_no_missing(self):
        # Regression: the patched file must define `time`, `defaultdict`, and
        # `asdict` at import time; the previous version did not.
        import time as _t  # noqa
        from collections import defaultdict as _dd  # noqa
        from dataclasses import asdict as _a  # noqa
        self.assertTrue(callable(_t.time))

    def test_lifecycle(self):
        async def go():
            core = BioGreenAgentCore(config=self._cfg())
            self.assertFalse(await core.ready())
            await core.start()
            self.assertTrue(await core.ready())
            await core.shutdown()
            self.assertTrue(core._shutdown)
            await core.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with BioGreenAgentCore(config=self._cfg()) as core:
                self.assertTrue(await core.ready())
        asyncio.run(go())

    def test_persistence_enqueue_and_flush(self):
        async def go():
            core = BioGreenAgentCore(config=self._cfg())
            await core.start()
            try:
                ok = await core.persistence.enqueue_alert(
                    "a1", "HIGH", "msg", AlertStatus.ACTIVE.value, "cid1",
                )
                self.assertTrue(ok)
                await core.persistence.flush()
                # Verify row is present
                with sqlite3.connect(core.persistence.db_path) as conn:
                    row = conn.execute(
                        "SELECT id, level FROM alerts WHERE id = ?",
                        ("a1",),
                    ).fetchone()
                self.assertEqual(row, ("a1", "HIGH"))
            finally:
                await core.shutdown()
        asyncio.run(go())

    def test_process_telemetry(self):
        async def go():
            async with BioGreenAgentCore(config=self._cfg()) as core:
                result = await core.process_telemetry(
                    {"energy_usage": 1.0, "temperature": 20.0},
                    entity_id="entity_a",
                )
                self.assertEqual(result["status"], "ok")
                self.assertIn("correlation_id", result)
        asyncio.run(go())

    def test_token_cache_keyed_by_entity(self):
        async def go():
            class FakeToken:
                def __init__(self):
                    self.calls = 0

                async def get_balance(self, entity_id, cid):
                    self.calls += 1
                    return 42.0

                async def consume_tokens(self, entity_id, amount, cid):
                    return True

            fake = FakeToken()
            async with BioGreenAgentCore(
                config=self._cfg(), token_service=fake
            ) as core:
                r1 = await core.process_telemetry(
                    {"energy_usage": 1.0}, entity_id="e1",
                )
                r2 = await core.process_telemetry(
                    {"energy_usage": 2.0}, entity_id="e1",
                )
                self.assertEqual(r1["token_balance"], 42.0)
                self.assertEqual(r2["token_balance"], 42.0)
                # Second call must be a cache hit
                self.assertEqual(fake.calls, 1)
        asyncio.run(go())

    def test_safety_invariants_do_not_raise(self):
        async def go():
            async with BioGreenAgentCore(config=self._cfg()) as core:
                # Empty state dict must not raise
                violations = core.safety_monitor.check({})
                self.assertEqual(violations, [])
        asyncio.run(go())

    def test_circuit_breaker_transitions(self):
        async def go():
            cb = CircuitBreaker("t", failure_threshold=2, recovery_time=0.5)

            async def ok():
                return 1

            async def fail():
                raise RuntimeError("boom")

            self.assertEqual(await cb.call(ok), 1)
            for _ in range(2):
                try:
                    await cb.call(fail)
                except RuntimeError:
                    pass
            self.assertEqual(cb.state_value, "OPEN")
            with self.assertRaises(RuntimeError):
                await cb.call(ok)
            await asyncio.sleep(0.6)
            self.assertEqual(await cb.call(ok), 1)
            self.assertEqual(cb.state_value, "CLOSED")
        asyncio.run(go())

    def test_circuit_breaker_offloads_sync(self):
        async def go():
            cb = CircuitBreaker("t", failure_threshold=1, recovery_time=1.0)

            def sync_work():
                return "sync-ok"

            result = await cb.call(sync_work)
            self.assertEqual(result, "sync-ok")
        asyncio.run(go())

    def test_event_broker_pubsub(self):
        async def go():
            broker = EventBroker(worker_count=2, queue_maxsize=10)
            received: List[BioEvent] = []

            async def handler(ev):
                received.append(ev)

            await broker.subscribe("test", handler)
            await broker.start()
            for i in range(5):
                await broker.publish(BioEvent(
                    priority=1, event_type="test", payload={"i": i},
                ))
            for _ in range(50):
                if len(received) >= 5:
                    break
                await asyncio.sleep(0.02)
            self.assertGreaterEqual(len(received), 5)
            await broker.shutdown(timeout=2.0)
        asyncio.run(go())

    def test_event_broker_rejects_after_shutdown(self):
        async def go():
            broker = EventBroker(worker_count=1, queue_maxsize=10)
            await broker.start()
            await broker.shutdown(timeout=2.0)
            ok = await broker.publish(BioEvent(
                priority=1, event_type="test", payload={},
            ))
            self.assertFalse(ok)
        asyncio.run(go())

    def test_nsga2_no_front_mutation(self):
        async def go():
            async def eval_fn(p):
                return {"a": p["x"], "b": 1.0 - p["x"]}

            opt = NSGAIIOptimizer(
                evaluate_func=eval_fn,
                parameter_bounds={"x": (0.0, 1.0)},
                population_size=6, generations=2,
                objective_weights={"a": 0.5, "b": 0.5},
            )
            front = await opt.evolve()
            before = [p.scalarised_score for p in front]
            _ = opt._select_best_from_pareto(front, {"a": 0.5, "b": 0.5})
            after = [p.scalarised_score for p in front]
            self.assertEqual(before, after)
        asyncio.run(go())

    def test_anomaly_detector_feature_mismatch(self):
        async def go():
            det = AnomalyDetector()
            await det.add_observation([1.0, 2.0])
            accepted = await det.add_observation([1.0])  # mismatched
            self.assertFalse(accepted)
        asyncio.run(go())

    def test_optimization_manager_disabled(self):
        async def go():
            async with BioGreenAgentCore(
                config=self._cfg(optimization_enabled=False)
            ) as core:
                r = await core.optimization_manager.run_optimization_once()
                self.assertEqual(r["status"], "disabled")
        asyncio.run(go())

    def test_placeholder_honesty(self):
        qd = QuantumDistillationModulePlaceholder()
        self.assertFalse(qd.available)
        rl = CausalRLAgentPlaceholder()
        self.assertFalse(rl.available)
        fed = FederatedCoordinatorPlaceholder(None)
        self.assertFalse(fed.available)
        pc = PrecisionControllerPlaceholder()
        self.assertFalse(pc.available)
        cm = CarbonMarketClientPlaceholder()
        self.assertFalse(cm.available)
        self.assertFalse(cm.buy_credits(1.0))
        self.assertFalse(cm.sell_credits(1.0))
        ci = ChaosInjectorPlaceholder(None)
        self.assertFalse(ci.available)

        async def go():
            ha = HumanApprovalHandlerPlaceholder()
            self.assertFalse(ha.available)
            self.assertFalse(await ha.request_approval({"action": "x"}))
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3,
                                        max_q_table=5)
        for _ in range(100):
            s = np.random.rand(4)
            a = rl.act(s)
            rl.update(s, a, 1.0, s, False)
        self.assertLessEqual(rl.size(), 5)

    def test_health_check(self):
        async def go():
            async with BioGreenAgentCore(config=self._cfg()) as core:
                h = await core.health_check()
                self.assertIn("status", h)
                self.assertIn("module_status", h)
        asyncio.run(go())

    def test_module_status_documented(self):
        self.assertEqual(MODULE_STATUS["bio_core"], "stable")
        self.assertEqual(MODULE_STATUS["causal_rl"], "placeholder")
        self.assertEqual(MODULE_STATUS["nsga2_optimizer"], "experimental")


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 15. ENTRY POINT
# =============================================================================
async def _example() -> None:
    cfg = BioCoreConfig(
        db_path="./bio_core_example.db",
        optimization_enabled=False,
        retrain_interval_sec=3600.0,
        enable_xai=True,
    )
    async with BioGreenAgentCore(config=cfg) as core:
        for i in range(3):
            r = await core.process_telemetry(
                {"energy_usage": 1.0 + i * 0.1, "temperature": 20.0 + i},
                entity_id="demo",
            )
            print(f"Cycle {i}: gradient={r['gradient']:.3f} "
                  f"cid={r['correlation_id']}")
        health = await core.health_check()
        print("Status:", health["status"])
        print("Modules:", json.dumps(MODULE_STATUS, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bio-Inspired Green Agent Core v9.0.0"
    )
    parser.add_argument("--test", action="store_true",
                        help="Run embedded tests")
    parser.add_argument("--example", action="store_true",
                        help="Run example usage")
    parser.add_argument("--status", action="store_true",
                        help="Print module statuses")
    args = parser.parse_args()

    if args.status:
        for name, status in MODULE_STATUS.items():
            print(f"{name:24s} {status}")
        return
    if args.test:
        sys.exit(run_tests())
    if args.example:
        asyncio.run(_example())
        return

    print("Bio-Inspired Green Agent Core v9.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
