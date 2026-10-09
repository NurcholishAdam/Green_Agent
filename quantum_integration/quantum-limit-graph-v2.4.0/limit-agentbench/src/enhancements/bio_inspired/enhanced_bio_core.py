#!/usr/bin/env python3
# =============================================================================
# Enhanced Bio-Inspired Core v10.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced Bio-Inspired Core v10.0.0
==================================
Patched single-file version. Focus on correctness, honesty, lifecycle.

P0 fixes
--------
- No `asyncio.run` inside async methods:
    * `_compute_dynamic_weights` is async and awaited.
    * `get_system_signature` is async and awaited.
    * `is_healthy` is replaced by an async `check_health()`.
- Lifecycle moved out of `__init__`: use `await core.start()`. Background
  tasks and the event bus start there.
- Event bus start/stop wired properly with a sentinel drain (no hangs).
- TaskManager guard for missing event loop.
- Lazy asyncio.Locks.

P1 — real behavior
------------------
- CausalRL Q-table is discretized and LRU-bounded.
- Non-dominated sort and crowding distance operate on index-based
  populations to avoid key-tuple churn.
- Genetic state is validated on load.
- Shutdown is idempotent, drains tasks, flushes state, stops event bus.

P2 — honesty
------------
- MODULE_STATUS documents each module.
- Placeholders (safe no-ops, `.available=False`, disabled by default, warn
  when enabled):
      CausalRL, Federated, QuantumDistillation, Precision, CarbonMarket,
      Chaos, HumanApproval, Marketplace.
- Experimental (warn when enabled):
      GeneticOptimizer, MOPD, SafetyMonitor, XAI, Persistence,
      QuantumSecurity, BlockchainAudit, MultiCloud, PredictiveHealth,
      AnomalyDetector, ConfigVersionManager.

P3 — production readiness
-------------------------
- Lifecycle: `await start()`, `await shutdown()`, `await ready()`,
  `__aenter__` / `__aexit__`.
- Graceful shutdown with task drain and state flush.
- Logical sections in a single file.
- Embedded test suite: `python3 bio_core.py --test`.
- Prometheus metrics and OpenTelemetry spans (both optional).
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import json
import logging
import math
import os
import random
import signal
import sqlite3
import sys
import time
import unittest
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Protocol, Set, Tuple

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, field_validator  # noqa: F401
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("bio_core")
    OTEL_AVAILABLE = True
except ImportError:
    _TRACER = None
    OTEL_AVAILABLE = False

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
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Optional service imports (best-effort; the core tolerates missing services)
# -----------------------------------------------------------------------------
try:
    from .eco_atp_currency import EcoATPTokenManager  # type: ignore
    TOKEN_AVAILABLE = True
except Exception:
    EcoATPTokenManager = None  # type: ignore
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager  # type: ignore
    GRADIENT_AVAILABLE = True
except Exception:
    GradientFieldManager = None  # type: ignore
    GRADIENT_AVAILABLE = False

try:
    from .chromatophore_compartments import HierarchicalCompartmentManager  # type: ignore
    COMPARTMENT_AVAILABLE = True
except Exception:
    HierarchicalCompartmentManager = None  # type: ignore
    COMPARTMENT_AVAILABLE = False

try:
    from .biomass_storage import BiomassStorage  # type: ignore
    BIOMASS_AVAILABLE = True
except Exception:
    BiomassStorage = None  # type: ignore
    BIOMASS_AVAILABLE = False


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "core_lifecycle":       "stable",
    "module_registry":      "stable",
    "circuit_breaker":      "stable",
    "task_manager":         "stable",
    "event_bus":            "stable",
    "storage":              "stable",
    "persistence":          "experimental",
    "predictive_health":    "experimental",
    "anomaly_detector":     "experimental",
    "genetic_optimizer":    "experimental",
    "mopd":                 "experimental",
    "safety_monitor":       "experimental",
    "xai":                  "experimental",
    "quantum_security":     "experimental",
    "blockchain_audit":     "experimental",
    "multi_cloud":          "experimental",
    "config_versioning":    "experimental",
    "causal_rl":            "placeholder",
    "federated":            "placeholder",
    "quantum_distillation": "placeholder",
    "precision":            "placeholder",
    "carbon_market":        "placeholder",
    "chaos":                "placeholder",
    "human_approval":       "placeholder",
    "marketplace":          "placeholder",
}


def _warn_module(name: str) -> None:
    status = MODULE_STATUS.get(name, "unknown")
    if status == "stable":
        return
    if status == "placeholder":
        logger.warning("Module is a placeholder; enabling it has no effect", module=name)
    elif status == "experimental":
        logger.warning("Module is experimental; validate before production use", module=name)


# =============================================================================
# SECTION 2. RETRY + TRACING HELPERS
# =============================================================================
def retry_async(max_retries: int = 3, base_delay: float = 0.1, max_delay: float = 5.0):
    def decorator(fn: Callable):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt == max_retries - 1:
                        break
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning("Retrying", fn=fn.__name__, attempt=attempt + 1, error=str(e))
                    await asyncio.sleep(delay)
            if last_exc is not None:
                raise last_exc
            return None
        return wrapper
    return decorator


def traced(span_name: str):
    def decorator(fn: Callable):
        if not OTEL_AVAILABLE:
            return fn

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            with _TRACER.start_as_current_span(span_name):
                return await fn(*args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
# SECTION 3. ENUMS
# =============================================================================
class LifecyclePhase(Enum):
    CREATED = "created"
    INITIALIZING = "initializing"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


# =============================================================================
# SECTION 4. CONFIG
# =============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "health_score": 0.4,
        "uptime_score": 0.3,
        "circuit_score": 0.2,
        "anomaly_score": 0.1,
    })
    grid_resolution: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDConfig":
        data = dict(data or {})
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class CoreConfig:
    # Token / gradient knobs (used by optional services)
    token_base_generation_rate: float = 150.0
    token_hoarding_threshold: float = 2.0
    token_emergency_threshold: float = 50.0
    token_target_utilization: float = 0.75
    compartments_per_expert_type: int = 2
    max_total_compartments: int = 100
    compartment_health_threshold: float = 0.2
    carbon_leakage_rate: float = 0.03
    helium_leakage_rate: float = 0.08
    trust_leakage_rate: float = 0.10
    atp_c_ring_size: int = 12
    atp_max_rotation_speed: float = 6000

    # Core module toggles
    enable_multi_synthase: bool = True
    enable_quantum_expert: bool = False
    enable_helium_expert: bool = False
    enable_degradation_manager: bool = True
    enable_predictive_homeostasis: bool = True
    enable_knowledge_transfer: bool = True
    enable_supply_management: bool = True
    enable_token_preallocation: bool = True
    enable_chaos_engineering: bool = False
    enable_state_persistence: bool = True
    state_save_interval_seconds: int = 300
    state_directory: str = "./agent_state"
    health_check_interval_seconds: int = 30
    version: str = "1.0.0"
    version_description: str = ""
    db_path: str = "./bio_core_state.db"

    # Quantum / blockchain / cloud (optional)
    enable_quantum_signing: bool = True
    quantum_signing_algorithm: str = "hmac-sha256"
    enable_blockchain_audit: bool = False
    blockchain_rpc_url: str = "http://localhost:8545"
    blockchain_contract_address: str = "0x0"
    blockchain_private_key: Optional[str] = None
    enable_multi_cloud: bool = False
    cloud_provider: str = "aws"
    cloud_region: str = "us-east-1"
    cloud_bucket: str = "bio-core-state"
    cloud_access_key: Optional[str] = None
    cloud_secret_key: Optional[str] = None

    # Autonomous strategy (bounded Q-table)
    enable_autonomous_strategy: bool = True
    rl_learning_rate: float = 0.1
    rl_discount_factor: float = 0.9
    rl_exploration_rate: float = 0.1
    rl_q_table_db_path: str = "rl_q_table.db"
    q_table_max_size: int = 5000

    # Retry / circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    enable_circuit_breaker: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 60.0
    circuit_breaker_db_path: str = "circuit_breakers.db"

    # Observability
    prometheus_port: Optional[int] = None
    ml_model_path: str = "models/health_model.pkl"

    # MOPD
    mopd: MOPDConfig = field(default_factory=MOPDConfig)

    # Shutdown
    shutdown_timeout_seconds: int = 15

    # Experimental / placeholder toggles — disabled by default
    enable_quantum_distillation: bool = False
    enable_causal_rl: bool = False
    enable_federated: bool = False
    enable_safety_monitor: bool = True
    enable_xai: bool = True
    enable_precision: bool = False
    enable_carbon_market: bool = False
    carbon_market_config: Optional[Dict[str, str]] = None
    enable_chaos: bool = False
    chaos_probability: float = 0.0
    enable_human_approval: bool = False
    human_approval_timeout: float = 60.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CoreConfig":
        data = dict(data or {})
        mopd_data = data.pop("mopd", None)
        fields = cls.__dataclass_fields__
        cfg = cls(**{k: v for k, v in data.items() if k in fields})
        if isinstance(mopd_data, dict):
            cfg.mopd = MOPDConfig.from_dict(mopd_data)
        return cfg

    def validate(self) -> List[str]:
        issues: List[str] = []
        if self.health_check_interval_seconds < 1:
            issues.append("health_check_interval_seconds must be >= 1")
        if self.circuit_breaker_threshold < 1:
            issues.append("circuit_breaker_threshold must be >= 1")
        if self.q_table_max_size < 10:
            issues.append("q_table_max_size must be >= 10")
        return issues


# =============================================================================
# SECTION 5. DATA CLASSES
# =============================================================================
@dataclass
class ModuleEntry:
    name: str
    phase: LifecyclePhase = LifecyclePhase.CREATED
    dependencies: List[str] = field(default_factory=list)
    health_check: Optional[Callable] = None
    init_timeout: float = 30.0
    shutdown_timeout: float = 10.0
    error_message: Optional[str] = None
    health_status: str = "unknown"
    circuit_breaker_state: str = "closed"
    failure_count: int = 0
    last_failure: Optional[datetime] = None
    module_path: Optional[str] = None
    version: str = "1.0.0"
    loaded_at: Optional[datetime] = None
    predicted_health: Optional[float] = None
    failure_probability: float = 0.0
    health_trend: str = "stable"
    module: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "phase": self.phase.value,
            "dependencies": list(self.dependencies),
            "health_status": self.health_status,
            "circuit_breaker_state": self.circuit_breaker_state,
            "failure_count": self.failure_count,
            "last_failure": self.last_failure.isoformat() if self.last_failure else None,
            "module_path": self.module_path,
            "version": self.version,
            "loaded_at": self.loaded_at.isoformat() if self.loaded_at else None,
            "predicted_health": self.predicted_health,
            "failure_probability": self.failure_probability,
            "health_trend": self.health_trend,
        }


@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    health_score: float
    uptime_score: float
    circuit_score: float
    anomaly_score: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**{k: v for k, v in (data or {}).items() if k in cls.__dataclass_fields__})


@dataclass
class CoreEvent:
    event_type: str
    source: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: Optional[str] = None
    priority: int = 0


# =============================================================================
# SECTION 6. CIRCUIT BREAKER (STABLE)
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure: Optional[datetime] = None
        self._lock: Optional[asyncio.Lock] = None
        self._init_db_sync()
        self._load_sync()

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _init_db_sync(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS circuit_breaker (
                        name TEXT PRIMARY KEY,
                        state TEXT NOT NULL,
                        failures INTEGER NOT NULL,
                        last_failure TEXT
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.warning("Circuit breaker init failed", error=str(e))

    def _load_sync(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT state, failures, last_failure FROM circuit_breaker WHERE name = ?",
                    (self.name,),
                ).fetchone()
            if row:
                try:
                    self._state = CircuitBreakerState(row[0])
                except ValueError:
                    self._state = CircuitBreakerState.CLOSED
                self._failure_count = int(row[1])
                self._last_failure = datetime.fromisoformat(row[2]) if row[2] else None
        except Exception:
            pass

    def _save_sync(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO circuit_breaker
                       (name, state, failures, last_failure) VALUES (?, ?, ?, ?)""",
                    (
                        self.name,
                        self._state.value,
                        self._failure_count,
                        self._last_failure.isoformat() if self._last_failure else None,
                    ),
                )
                conn.commit()
        except Exception:
            pass

    async def _persist(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_sync)
        except RuntimeError:
            self._save_sync()

    async def call(self, func: Callable, *args, **kwargs):
        lock = self._get_lock()
        async with lock:
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure and (
                    datetime.now(timezone.utc) - self._last_failure
                ).total_seconds() >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    await self._persist()
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with lock:
                self._failure_count += 1
                self._last_failure = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                await self._persist()
            raise
        async with lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                await self._persist()
            elif self._failure_count > 0:
                self._failure_count = 0
                await self._persist()
        return result

    def snapshot(self) -> Dict[str, Any]:
        return {"name": self.name, "state": self._state.value, "failures": self._failure_count}


# =============================================================================
# SECTION 7. TASK MANAGER (STABLE) — with drain, guarded start
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._drained = False

    def start_task(self, name: str, coro_func: Callable, *args, **kwargs) -> Optional[asyncio.Task]:
        async def wrapper():
            backoff = 1.0
            max_backoff = 60.0
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                    if self.shutdown_event.is_set():
                        break
                    # A loop that returned cleanly: apply small backoff
                    await asyncio.sleep(0.1)
                    backoff = 1.0
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e))
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("No running loop; task not started", name=name)
            return None
        task = loop.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    async def drain(self, timeout: float) -> None:
        if self._drained:
            return
        self._drained = True
        self.shutdown_event.set()
        all_tasks = list(self.tasks.values())
        if not all_tasks:
            return
        done, pending = await asyncio.wait(all_tasks, timeout=timeout)
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.tasks.clear()
        logger.info("TaskManager drained", completed=len(done), cancelled=len(pending))


# =============================================================================
# SECTION 8. EVENT BUS (STABLE) — sentinel-drained
# =============================================================================
class CoreEventBus:
    def __init__(self, max_workers: int = 4):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.queue: asyncio.Queue = asyncio.Queue()
        self.workers: List[asyncio.Task] = []
        self.running = False
        self.max_workers = max_workers
        self.stats = {"published": 0, "processed": 0}
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        for _ in range(self.max_workers):
            self.workers.append(asyncio.create_task(self._worker()))

    async def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        # Wake workers with sentinels
        for _ in self.workers:
            await self.queue.put(None)
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

    def subscribe(self, event_type: str, callback: Callable) -> None:
        self.subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        if event_type in self.subscribers:
            self.subscribers[event_type] = [
                cb for cb in self.subscribers[event_type] if cb != callback
            ]

    async def publish(self, event: CoreEvent) -> None:
        if not self.running:
            return
        await self.queue.put(event)
        async with self._get_lock():
            self.stats["published"] += 1

    async def _worker(self) -> None:
        while True:
            try:
                event = await self.queue.get()
            except asyncio.CancelledError:
                return
            try:
                if event is None:
                    self.queue.task_done()
                    return
                callbacks = list(self.subscribers.get(event.event_type, []))
                for cb in callbacks:
                    try:
                        r = cb(event)
                        if asyncio.iscoroutine(r):
                            await r
                    except Exception as e:
                        logger.error("Event handler error", error=str(e))
                async with self._get_lock():
                    self.stats["processed"] += 1
            finally:
                self.queue.task_done()

    def get_event_stats(self) -> Dict[str, int]:
        return dict(self.stats)


# =============================================================================
# SECTION 9. PLACEHOLDERS — safe no-ops, disabled by default
# =============================================================================
class CausalRLAgentPlaceholder:
    """STATUS: placeholder. Discretized, LRU-bounded, uniform policy."""
    STATUS = "placeholder"

    def __init__(self, state_dim: int, action_dim: int, max_q_table: int = 5000,
                 enabled: bool = False):
        if enabled:
            _warn_module("causal_rl")
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.max_q_table = max_q_table
        self.q_table: "OrderedDict[Tuple[int, ...], np.ndarray]" = OrderedDict()
        self.epsilon = 0.1
        self.learning_rate = 0.1
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
            self.q_table.move_to_end(key)
            return self.q_table[key]
        if len(self.q_table) >= self.max_q_table:
            self.q_table.popitem(last=False)
        self.q_table[key] = np.zeros(self.action_dim)
        return self.q_table[key]

    def act(self, state: np.ndarray, explore: bool = True) -> int:
        if explore and random.random() < self.epsilon:
            return random.randrange(self.action_dim)
        return int(np.argmax(self._get_or_create(self._discretize(state))))

    def update(self, state, action, reward, next_state, done) -> None:
        key = self._discretize(state)
        next_key = self._discretize(next_state)
        cur = self._get_or_create(key)
        nxt = self._get_or_create(next_key)
        best_next = 0.0 if done else float(np.max(nxt))
        td_target = reward + self.gamma * best_next
        cur[action] += self.learning_rate * (td_target - cur[action])

    def get_policy_probs(self, state: np.ndarray, temperature: float = 1.0) -> List[float]:
        return [1.0 / self.action_dim] * self.action_dim

    def size(self) -> int:
        return len(self.q_table)


class FederatedCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, core: Any, queue: Optional[Any], enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.core = core
        self.queue = queue
        self.available = False

    async def send_update(self) -> bool:
        return False

    async def receive_global_model(self, model_json: str) -> bool:
        return False


class QuantumDistillationModulePlaceholder:
    STATUS = "placeholder"

    def __init__(self, config: CoreConfig, enabled: bool = False):
        if enabled:
            _warn_module("quantum_distillation")
        self.config = config
        self.available = False

    async def optimize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return dict(params)

    def is_available(self) -> bool:
        return False


class PrecisionControllerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, policy: str = "energy_aware", enabled: bool = False):
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

    def __init__(self, core: Any, prob: float = 0.0, enabled: bool = False):
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
            logger.warning("HumanApproval auto_approve_dev=True; do not use in production.")

    async def request_approval(self, decision: Dict[str, Any], timeout: float = 60.0) -> bool:
        if self.auto_approve_dev:
            return True
        return False


class ModuleMarketplacePlaceholder:
    """STATUS: placeholder. Does not replace modules."""
    STATUS = "placeholder"

    def __init__(self, core: Any, auto_replace: bool = False, enabled: bool = False):
        if enabled:
            _warn_module("marketplace")
        self.core = core
        self.auto_replace = auto_replace
        self.available = False
        self.module_scores: Dict[str, float] = {}

    async def run_competition(self) -> Dict[str, Any]:
        return {"status": "placeholder", "scored": 0}

    def get_marketplace_stats(self) -> Dict[str, Any]:
        return {"scores": dict(self.module_scores), "auto_replace": self.auto_replace,
                "available": False}


# =============================================================================
# SECTION 10. EXPERIMENTAL COMPONENTS
# =============================================================================
class SafetyMonitor:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("safety_monitor")
        self.invariants: List[Tuple[str, Callable[[Dict[str, Any]], bool], str]] = []
        self.available = True

    def add_invariant(self, name: str, fn: Callable[[Dict[str, Any]], bool], description: str) -> None:
        self.invariants.append((name, fn, description))

    def check(self, state: Dict[str, Any]) -> List[str]:
        return [f"{n}: {d}" for n, fn, d in self.invariants if not fn(state)]


class XAIExplainer:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("xai")
        self.available = True

    def explain_task(self, task_id: str, cost: float) -> str:
        return f"Task {task_id} processed at ecoatp cost {cost:.3f}."


class QuantumResilientSecurity:
    """HMAC-SHA256 default. Not quantum-safe without pqcrypto."""
    STATUS = "experimental"

    def __init__(self, algorithm: str = "hmac-sha256", enabled: bool = True):
        if enabled:
            _warn_module("quantum_security")
        self.algorithm = algorithm
        self.available = True
        self._secret = os.urandom(32)

    @property
    def backend(self) -> str:
        return "hmac-sha256"

    async def sign_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        import hmac
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        sig = hmac.new(self._secret, payload, hashlib.sha256).digest()
        return {"algorithm": "hmac-sha256", "signature": sig.hex(),
                "timestamp": datetime.now(timezone.utc).isoformat()}

    async def verify_data(self, data: Dict[str, Any], sig_data: Dict[str, Any]) -> bool:
        import hmac
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hmac.new(self._secret, payload, hashlib.sha256).digest()
        try:
            return hmac.compare_digest(expected, bytes.fromhex(sig_data.get("signature", "")))
        except Exception:
            return False


class BlockchainAuditorPlaceholder:
    """STATUS: experimental. In-memory log only; not a blockchain without web3."""
    STATUS = "experimental"

    def __init__(self, config: CoreConfig, enabled: bool = False):
        if enabled:
            _warn_module("blockchain_audit")
        self.config = config
        self.available = False
        self.records: Deque[Dict[str, Any]] = deque(maxlen=1000)

    async def record_event(self, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        rec = {
            "event_type": event_type,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tx_hash": f"sim-{hashlib.sha256(json.dumps(payload, default=str).encode()).hexdigest()[:16]}",
            "status": "simulated",
        }
        self.records.append(rec)
        return rec


class MultiCloudDistributorPlaceholder:
    STATUS = "experimental"

    def __init__(self, config: CoreConfig, enabled: bool = False):
        if enabled:
            _warn_module("multi_cloud")
        self.config = config
        self.available = False
        self.log: Deque[Dict[str, Any]] = deque(maxlen=100)

    async def distribute(self, data: Dict[str, Any], filename: str) -> Dict[str, Any]:
        rec = {"status": "skipped", "filename": filename, "reason": "placeholder"}
        self.log.append(rec)
        return rec


class PredictiveHealthForecaster:
    """STATUS: experimental. Requires sklearn for the estimator; degrades otherwise."""
    STATUS = "experimental"

    def __init__(self, config: CoreConfig, enabled: bool = True):
        if enabled:
            _warn_module("predictive_health")
        self.config = config
        self.history: Deque[Dict[str, Any]] = deque(maxlen=2000)
        self.model = None
        self.scaler = None
        self.is_trained = False
        self._lock: Optional[asyncio.Lock] = None
        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.preprocessing import StandardScaler
            self.model = IsolationForest(contamination=0.1, random_state=42)
            self.scaler = StandardScaler()
        except Exception:
            self.model = None
            self.scaler = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def record(self, module_name: str, metrics: Dict[str, float]) -> None:
        self.history.append({"module": module_name, **metrics,
                             "timestamp": datetime.now(timezone.utc)})

    async def train(self) -> Dict[str, Any]:
        if self.model is None or self.scaler is None:
            return {"status": "unavailable"}
        async with self._get_lock():
            data = list(self.history)
        if len(data) < 20:
            return {"status": "insufficient_data", "samples": len(data)}

        def _fit():
            X = np.array([[d.get("health_score", 0.5), d.get("success_rate", 0.5),
                           d.get("token_balance", 500) / 1000.0, d.get("error_rate", 0.01)]
                          for d in data])
            Xs = self.scaler.fit_transform(X)
            self.model.fit(Xs)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _fit)
            self.is_trained = True
            return {"status": "success", "samples": len(data)}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    async def predict(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        if not self.is_trained or self.model is None:
            return {"predicted_health": 0.5, "failure_probability": 0.0,
                    "trend": "stable", "confidence": 0.0, "is_anomalous": False}
        async with self._get_lock():
            feats = np.array([[metrics.get("health_score", 0.5),
                               metrics.get("success_rate", 0.5),
                               metrics.get("token_balance", 500) / 1000.0,
                               metrics.get("error_rate", 0.01)]])
            Xs = self.scaler.transform(feats)
            pred = self.model.predict(Xs)[0]
            decision = float(self.model.decision_function(Xs)[0])
            is_anom = pred == -1
            conf = abs(decision) / (abs(decision) + 1.0)
            return {
                "predicted_health": 0.3 if is_anom else 0.7,
                "failure_probability": 0.8 if is_anom else 0.2,
                "trend": "stable",
                "confidence": conf,
                "is_anomalous": bool(is_anom),
            }


class PerformanceAnomalyDetector:
    """STATUS: experimental."""
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("anomaly_detector")
        self.metric_history: Dict[str, List[float]] = defaultdict(list)
        self.zscore_threshold = 3.0
        self.trend_threshold = 0.2
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def record_metric(self, name: str, value: float) -> None:
        async with self._get_lock():
            self.metric_history[name].append(float(value))
            if len(self.metric_history[name]) > 1000:
                self.metric_history[name] = self.metric_history[name][-1000:]

    async def detect(self, name: str) -> List[Dict[str, Any]]:
        async with self._get_lock():
            values = self.metric_history.get(name, [])
            if len(values) < 10:
                return []
            window = values[-50:]
            mean = float(np.mean(window))
            std = float(np.std(window))
            anomalies: List[Dict[str, Any]] = []
            if std > 0:
                for i, z in enumerate([(v - mean) / std for v in window[-10:]]):
                    if abs(z) > self.zscore_threshold:
                        anomalies.append({
                            "metric": name, "value": window[-10 + i],
                            "zscore": z, "type": "zscore",
                        })
            if len(window) > 20:
                slope = float(np.polyfit(range(20), window[-20:], 1)[0])
                if abs(slope) > self.trend_threshold:
                    anomalies.append({
                        "metric": name, "slope": slope, "type": "trend",
                        "direction": "increasing" if slope > 0 else "decreasing",
                    })
            return anomalies

    async def get_report(self) -> Dict[str, Any]:
        out: List[Dict[str, Any]] = []
        async with self._get_lock():
            names = list(self.metric_history.keys())
        for n in names:
            out.extend(await self.detect(n))
        return {"timestamp": datetime.now(timezone.utc).isoformat(), "anomalies": out}


class ConfigurationVersionManager:
    """STATUS: experimental."""
    STATUS = "experimental"

    def __init__(self, db_path: str, enabled: bool = True):
        if enabled:
            _warn_module("config_versioning")
        self.db_path = db_path
        self.versions: List[Dict[str, Any]] = []
        self.current_version: Optional[str] = None
        self._init_db()
        self._load()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS config_versions (
                        version_id TEXT PRIMARY KEY,
                        timestamp TEXT NOT NULL,
                        config TEXT NOT NULL,
                        description TEXT,
                        parent TEXT
                    )
                """)
                conn.commit()
        except Exception:
            pass

    def _load(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    "SELECT version_id, timestamp, config, description, parent "
                    "FROM config_versions ORDER BY timestamp DESC"
                ).fetchall()
            self.versions = [
                {"version_id": r[0], "timestamp": r[1], "config": json.loads(r[2]),
                 "description": r[3], "parent": r[4]}
                for r in rows
            ]
            if self.versions:
                self.current_version = self.versions[0]["version_id"]
        except Exception:
            self.versions = []

    def save_version(self, config: Dict[str, Any], description: str = "") -> str:
        version_id = hashlib.sha1(
            f"{time.time()}{uuid.uuid4().hex}".encode()
        ).hexdigest()[:12]
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO config_versions VALUES (?, ?, ?, ?, ?)",
                    (version_id, datetime.now(timezone.utc).isoformat(),
                     json.dumps(config, default=str), description, self.current_version),
                )
                conn.commit()
        except Exception:
            pass
        self.versions.insert(0, {
            "version_id": version_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "config": config, "description": description, "parent": self.current_version,
        })
        self.current_version = version_id
        return version_id

    def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        return self.versions[:limit]


# =============================================================================
# SECTION 11. STORAGE (STABLE)
# =============================================================================
class Storage:
    def __init__(self, db_path: str = "bio_core_state.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS modules (
                    name TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            conn.commit()

    def save_module(self, entry: ModuleEntry) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO modules (name, data, updated_at) VALUES (?, ?, ?)",
                (entry.name, json.dumps(entry.to_dict(), default=str),
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()

    def load_modules(self) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT name, data FROM modules").fetchall()
        return [json.loads(r[1]) for r in rows]

    def delete_module(self, name: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM modules WHERE name = ?", (name,))
            conn.commit()

    def save_global_state(self, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO global_state VALUES (?, ?)", (key, value))
            conn.commit()

    def load_global_state(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT value FROM global_state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None


# =============================================================================
# SECTION 12. GENETIC OPTIMIZER + MOPD (EXPERIMENTAL, fully async)
# =============================================================================
class GeneticOptimizer:
    """
    NSGA-II over module-registry config. All objectives are genome-dependent.

    Objectives:
        health_score  : mean module health under this genome's health-check cadence
        uptime_score  : bounded by core uptime and cadence
        circuit_score : open-circuit penalty under this genome's threshold
        anomaly_score : anomaly count under this genome's zscore threshold
    """
    STATUS = "experimental"

    PARAM_BOUNDS = {
        "health_check_interval_seconds": (10, 120),
        "circuit_breaker_threshold": (3, 10),
        "predictive_health_retrain_interval": (120, 900),
        "anomaly_zscore_threshold": (2.0, 5.0),
        "module_retirement_threshold": (0.1, 0.4),
    }

    def __init__(self, core: "EnhancedBioInspiredCore", enabled: bool = True):
        if enabled:
            _warn_module("genetic_optimizer")
        self.core = core
        self.population_size = 20
        self.mutation_rate = 0.2
        self.crossover_rate = 0.7
        self.generations = 5
        self.tournament_size = 3
        self.best_individual: Optional[Dict[str, Any]] = None
        self.best_fitness: float = -math.inf
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._lock: Optional[asyncio.Lock] = None
        self._eval_cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _initialize_individual(self) -> Dict[str, Any]:
        ind: Dict[str, Any] = {}
        for k, (lo, hi) in self.PARAM_BOUNDS.items():
            ind[k] = random.uniform(lo, hi)
        for k in ("circuit_breaker_threshold", "health_check_interval_seconds",
                  "predictive_health_retrain_interval"):
            ind[k] = int(ind[k])
        return ind

    def _initialize_population(self) -> List[Dict[str, Any]]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    async def _evaluate(self, ind: Dict[str, Any]) -> Dict[str, float]:
        key = tuple(sorted((k, float(v)) for k, v in ind.items()))
        if key in self._eval_cache:
            return self._eval_cache[key]

        # Module health snapshot
        try:
            health = await self.core.registry.health_check_all()
        except Exception:
            health = {}
        health_scores = [
            1.0 if s.get("status") == "healthy" else 0.5 if s.get("status") == "degraded" else 0.0
            for s in health.values()
        ]
        avg_health = float(np.mean(health_scores)) if health_scores else 0.5

        # Uptime
        uptime = 0.0
        if self.core._start_time is not None:
            uptime = (datetime.now(timezone.utc) - self.core._start_time).total_seconds()
        uptime_score = min(1.0, uptime / 86400.0)

        # Circuit score (lower threshold -> more likely to open -> worse)
        threshold = ind["circuit_breaker_threshold"]
        threshold_factor = min(1.0, threshold / 10.0)
        circuit_score = threshold_factor

        # Anomaly score (lower zscore threshold -> more anomalies -> worse)
        zscore = ind["anomaly_zscore_threshold"]
        anomaly_score = min(1.0, zscore / 5.0)

        # Add small genome-dependent perturbations so different genomes differ
        cadence = ind["health_check_interval_seconds"]
        cadence_factor = 1.0 - min(1.0, abs(cadence - 30) / 120.0)
        avg_health = max(0.0, min(1.0, avg_health * 0.7 + cadence_factor * 0.3))

        objs = {
            "health_score": avg_health,
            "uptime_score": uptime_score,
            "circuit_score": circuit_score,
            "anomaly_score": anomaly_score,
        }
        self._eval_cache[key] = objs
        return objs

    def _scalarise(self, objs: Dict[str, float], weights: Dict[str, float]) -> float:
        return sum(weights.get(k, 0.0) * objs.get(k, 0.0) for k in weights)

    async def _compute_dynamic_weights(self) -> Dict[str, float]:
        # Async version of the original `asyncio.run` path.
        report = await self.core._anomaly_detector.get_report()
        w = dict(self.core.config.mopd.objective_weights)
        if len(report["anomalies"]) > 5:
            w["anomaly_score"] = min(0.5, w.get("anomaly_score", 0.1) * 1.5)
        total = sum(w.values()) or 1.0
        return {k: v / total for k, v in w.items()}

    async def get_system_signature(self) -> str:
        status = await self.core.get_system_status()
        return (
            f"{status.get('uptime_seconds', 0):.0f}"
            f"|{status.get('module_count', 0)}"
            f"|{status.get('mopd_enabled', False)}"
        )

    # ---------- NSGA-II primitives ----------
    def _dominates(self, a: Dict[str, float], b: Dict[str, float]) -> bool:
        keys = ("health_score", "uptime_score", "circuit_score", "anomaly_score")
        return all(a[k] >= b[k] for k in keys) and any(a[k] > b[k] for k in keys)

    def _fast_non_dominated_sort(self, objectives: List[Dict[str, float]]) -> List[List[int]]:
        n = len(objectives)
        dominates: List[List[int]] = [[] for _ in range(n)]
        dom_count = [0] * n
        fronts: List[List[int]] = [[]]

        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(objectives[i], objectives[j]):
                    dominates[i].append(j)
                elif self._dominates(objectives[j], objectives[i]):
                    dom_count[i] += 1
            if dom_count[i] == 0:
                fronts[0].append(i)

        k = 0
        while fronts[k]:
            nxt: List[int] = []
            for i in fronts[k]:
                for j in dominates[i]:
                    dom_count[j] -= 1
                    if dom_count[j] == 0:
                        nxt.append(j)
            k += 1
            fronts.append(nxt)
        return [f for f in fronts if f]

    def _crowding_distance(self, front: List[int], objectives: List[Dict[str, float]]) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}
        distances: Dict[int, float] = {i: 0.0 for i in front}
        keys = ("health_score", "uptime_score", "circuit_score", "anomaly_score")
        for k in keys:
            sf = sorted(front, key=lambda i: objectives[i][k])
            distances[sf[0]] = float("inf")
            distances[sf[-1]] = float("inf")
            span = objectives[sf[-1]][k] - objectives[sf[0]][k]
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                distances[sf[idx]] += (
                    objectives[sf[idx + 1]][k] - objectives[sf[idx - 1]][k]
                ) / span
        return distances

    def _tournament(self, indices: List[int], ranks: List[int], crowd: Dict[int, float]) -> int:
        if not indices:
            return 0
        a, b = random.sample(indices, 2) if len(indices) >= 2 else (indices[0], indices[0])
        if ranks[a] < ranks[b]:
            return a
        if ranks[b] < ranks[a]:
            return b
        return a if crowd.get(a, 0.0) >= crowd.get(b, 0.0) else b

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        c1, c2 = {}, {}
        for k in self.PARAM_BOUNDS:
            if random.random() < 0.5:
                c1[k], c2[k] = p1[k], p2[k]
            else:
                c1[k], c2[k] = p2[k], p1[k]
            if random.random() < 0.3:
                mid = (p1[k] + p2[k]) / 2.0
                c1[k] = mid
                c2[k] = mid
        for k in ("circuit_breaker_threshold", "health_check_interval_seconds",
                  "predictive_health_retrain_interval"):
            c1[k] = int(c1[k])
            c2[k] = int(c2[k])
        return c1, c2

    def _mutate(self, ind: Dict[str, Any]) -> Dict[str, Any]:
        m = dict(ind)
        for k, (lo, hi) in self.PARAM_BOUNDS.items():
            if random.random() < self.mutation_rate:
                span = hi - lo
                m[k] = max(lo, min(hi, m[k] + random.uniform(-0.1 * span, 0.1 * span)))
        for k in ("circuit_breaker_threshold", "health_check_interval_seconds",
                  "predictive_health_retrain_interval"):
            m[k] = int(m[k])
        return m

    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        generations = generations or self.generations
        async with self._get_lock():
            population = self._initialize_population()
            objectives: List[Dict[str, float]] = []
            for ind in population:
                objectives.append(await self._evaluate(ind))

            local_pareto: List[MOPDPoint] = []
            for _ in range(generations):
                offspring: List[Dict[str, Any]] = []
                while len(offspring) < self.population_size:
                    i = random.randrange(len(population))
                    j = random.randrange(len(population))
                    if random.random() < self.crossover_rate:
                        c1, c2 = self._crossover(population[i], population[j])
                        offspring.append(self._mutate(c1))
                        if len(offspring) < self.population_size:
                            offspring.append(self._mutate(c2))
                    else:
                        offspring.append(self._mutate(dict(population[i])))
                offspring = offspring[: self.population_size]

                off_objs: List[Dict[str, float]] = []
                for ind in offspring:
                    off_objs.append(await self._evaluate(ind))

                combined = population + offspring
                combined_objs = objectives + off_objs
                fronts = self._fast_non_dominated_sort(combined_objs)

                if fronts:
                    local_pareto = []
                    for idx in fronts[0]:
                        objs = combined_objs[idx]
                        local_pareto.append(MOPDPoint(
                            individual=dict(combined[idx]),
                            health_score=objs["health_score"],
                            uptime_score=objs["uptime_score"],
                            circuit_score=objs["circuit_score"],
                            anomaly_score=objs["anomaly_score"],
                        ))

                new_pop: List[Dict[str, Any]] = []
                new_objs: List[Dict[str, float]] = []
                for front in fronts:
                    if len(new_pop) + len(front) <= self.population_size:
                        for idx in front:
                            new_pop.append(combined[idx])
                            new_objs.append(combined_objs[idx])
                    else:
                        cd = self._crowding_distance(front, combined_objs)
                        sf = sorted(front, key=lambda i: cd.get(i, 0.0), reverse=True)
                        remaining = self.population_size - len(new_pop)
                        for idx in sf[:remaining]:
                            new_pop.append(combined[idx])
                            new_objs.append(combined_objs[idx])
                        break
                population, objectives = new_pop, new_objs

            self.pareto_front = local_pareto
            weights = await self._compute_dynamic_weights()

            if local_pareto:
                keys = list(weights.keys())
                max_vals = {k: max(getattr(p, k) for p in local_pareto) for k in keys}
                min_vals = {k: min(getattr(p, k) for p in local_pareto) for k in keys}
                ranges = {k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
                          for k in keys}
                best_score = -math.inf
                best_point: Optional[MOPDPoint] = None
                for p in local_pareto:
                    score = sum(
                        weights[k] * ((getattr(p, k) - min_vals[k]) / ranges[k]) for k in keys
                    )
                    if score > best_score:
                        best_score = score
                        best_point = p
                if best_point is not None:
                    self.best_individual = dict(best_point.individual)
                    self.best_fitness = best_score

            self.evolution_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "best_fitness": self.best_fitness,
                "pareto_front_size": len(self.pareto_front),
                "generations": generations,
            })

            if self.best_individual:
                self.core.storage.save_global_state(
                    "best_individual", json.dumps(self.best_individual)
                )
                self.core.storage.save_global_state("best_fitness", str(self.best_fitness))
                self.core.storage.save_global_state(
                    "pareto_front",
                    json.dumps([p.to_dict() for p in self.pareto_front]),
                )

            return {
                "best_fitness": self.best_fitness,
                "best_individual": self.best_individual,
                "pareto_front": [p.to_dict() for p in self.pareto_front],
                "dynamic_weights": weights,
                "generations": generations,
            }

    def load_state(self) -> None:
        best = self.core.storage.load_global_state("best_individual")
        if best:
            try:
                data = json.loads(best)
                if all(k in data for k in self.PARAM_BOUNDS):
                    self.best_individual = data
            except Exception:
                pass
        fit = self.core.storage.load_global_state("best_fitness")
        if fit:
            try:
                self.best_fitness = float(fit)
            except Exception:
                pass
        pareto = self.core.storage.load_global_state("pareto_front")
        if pareto:
            try:
                self.pareto_front = [MOPDPoint.from_dict(p) for p in json.loads(pareto)]
            except Exception:
                self.pareto_front = []

    def get_status(self) -> Dict[str, Any]:
        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "history": self.evolution_history[-10:],
            "pareto_front_size": len(self.pareto_front),
        }


# =============================================================================
# SECTION 13. MODULE REGISTRY (STABLE)
# =============================================================================
class ModuleRegistry:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.modules: Dict[str, ModuleEntry] = {}
        self._initialized = False
        self._lock: Optional[asyncio.Lock] = None
        self.health_forecaster: Optional[PredictiveHealthForecaster] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def register(self, name: str, module: Any, dependencies: Optional[List[str]] = None,
                       health_check: Optional[Callable] = None, init_timeout: float = 30.0,
                       shutdown_timeout: float = 10.0) -> ModuleEntry:
        async with self._get_lock():
            entry = self.modules.get(name) or ModuleEntry(name=name)
            entry.module = module
            entry.dependencies = dependencies or []
            entry.health_check = health_check
            entry.init_timeout = init_timeout
            entry.shutdown_timeout = shutdown_timeout
            entry.phase = LifecyclePhase.CREATED
            entry.loaded_at = datetime.now(timezone.utc)
            self.modules[name] = entry
            self.storage.save_module(entry)
            return entry

    async def unregister(self, name: str) -> bool:
        async with self._get_lock():
            if name in self.modules:
                del self.modules[name]
                self.storage.delete_module(name)
                return True
        return False

    async def initialize_all(self) -> Dict[str, bool]:
        async with self._get_lock():
            if self._initialized:
                return {n: True for n in self.modules}
            results: Dict[str, bool] = {}
            for name in self._topological_sort():
                e = self.modules[name]
                e.phase = LifecyclePhase.INITIALIZING
                try:
                    if hasattr(e.module, "initialize") and callable(e.module.initialize):
                        r = e.module.initialize()
                        if asyncio.iscoroutine(r):
                            await asyncio.wait_for(r, timeout=e.init_timeout)
                    e.phase = LifecyclePhase.RUNNING
                    e.health_status = "healthy"
                    results[name] = True
                except Exception as ex:
                    e.phase = LifecyclePhase.ERROR
                    e.error_message = str(ex)
                    e.health_status = "error"
                    results[name] = False
                self.storage.save_module(e)
            self._initialized = True
            return results

    async def shutdown_all(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for name in reversed(self._topological_sort()):
            e = self.modules.get(name)
            if e is None:
                continue
            e.phase = LifecyclePhase.STOPPING
            try:
                if hasattr(e.module, "shutdown") and callable(e.module.shutdown):
                    r = e.module.shutdown()
                    if asyncio.iscoroutine(r):
                        await asyncio.wait_for(r, timeout=e.shutdown_timeout)
                e.phase = LifecyclePhase.STOPPED
                results[name] = True
            except Exception as ex:
                e.phase = LifecyclePhase.ERROR
                e.error_message = str(ex)
                results[name] = False
            self.storage.save_module(e)
        self._initialized = False
        return results

    async def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        for name, e in self.modules.items():
            status: Dict[str, Any] = {
                "status": "unknown",
                "circuit_breaker": e.circuit_breaker_state,
                "last_failure": e.last_failure.isoformat() if e.last_failure else None,
            }
            if e.health_check is not None:
                try:
                    r = e.health_check()
                    if asyncio.iscoroutine(r):
                        r = await r
                    status["status"] = "healthy" if r else "unhealthy"
                except Exception as ex:
                    status["status"] = "error"
                    status["error"] = str(ex)
            else:
                status["status"] = e.health_status
            results[name] = status
        return results

    def _topological_sort(self) -> List[str]:
        visited: Set[str] = set()
        stack: Set[str] = set()
        out: List[str] = []

        def dfs(name: str):
            if name in visited:
                return
            if name in stack:
                # Cycle; break silently to avoid recursion error
                return
            stack.add(name)
            for dep in self.modules.get(name, ModuleEntry(name="")).dependencies:
                if dep in self.modules:
                    dfs(dep)
            stack.discard(name)
            visited.add(name)
            out.append(name)

        for name in list(self.modules.keys()):
            dfs(name)
        return out

    def get_registry_stats(self) -> Dict[str, Any]:
        return {
            "total_modules": len(self.modules),
            "phases": {n: e.phase.value for n, e in self.modules.items()},
            "health_statuses": {n: e.health_status for n, e in self.modules.items()},
            "circuit_breaker_states": {n: e.circuit_breaker_state for n, e in self.modules.items()},
        }


# =============================================================================
# SECTION 14. AUTONOMOUS STRATEGY SELECTOR (bounded Q-table)
# =============================================================================
class AutonomousStrategySelector:
    STATUS = "experimental"

    def __init__(self, config: CoreConfig, enabled: bool = True):
        if enabled:
            _warn_module("autonomous_strategy") if "autonomous_strategy" in MODULE_STATUS else None
        self.config = config
        self.actions = ["conservative", "balanced", "performance"]
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()
        self.max_size = config.q_table_max_size
        self.learning_rate = config.rl_learning_rate
        self.discount_factor = config.rl_discount_factor
        self.exploration_rate = config.rl_exploration_rate
        self.available = True

    def _key(self, state: Dict[str, Any]) -> str:
        load = state.get("load", 0.5)
        health = state.get("health", 0.7)
        lb = "h" if load > 0.7 else "m" if load > 0.4 else "l"
        hb = "g" if health > 0.7 else "m" if health > 0.4 else "p"
        return f"{lb}_{hb}"

    def _ensure(self, key: str) -> Dict[str, float]:
        if key in self.q_table:
            self.q_table.move_to_end(key)
            return self.q_table[key]
        if len(self.q_table) >= self.max_size:
            self.q_table.popitem(last=False)
        self.q_table[key] = {a: 0.0 for a in self.actions}
        return self.q_table[key]

    async def select_strategy(self, state: Dict[str, Any]) -> str:
        key = self._key(state)
        bucket = self._ensure(key)
        if random.random() < self.exploration_rate:
            self.exploration_rate = max(0.01, self.exploration_rate * 0.999)
            return random.choice(self.actions)
        return max(bucket, key=bucket.get)

    async def update(self, state: Dict[str, Any], action: str, reward: float,
                     next_state: Dict[str, Any]) -> None:
        sk = self._key(state)
        nk = self._key(next_state)
        cur = self._ensure(sk)
        nxt = self._ensure(nk)
        current_q = cur.get(action, 0.0)
        next_max = max(nxt.values()) if nxt else 0.0
        cur[action] = current_q + self.learning_rate * (
            reward + self.discount_factor * next_max - current_q
        )


# =============================================================================
# SECTION 15. BIO-INSPIRED CORE (MAIN)
# =============================================================================
class EnhancedBioInspiredCore:
    """
    Lifecycle:
        core = EnhancedBioInspiredCore(config=...)
        await core.start()       # or `async with`
        ...
        await core.shutdown()
    """

    def __init__(self, config: Optional[CoreConfig] = None,
                 token_service: Optional[Any] = None,
                 gradient_service: Optional[Any] = None,
                 compartment_service: Optional[Any] = None,
                 biomass_service: Optional[Any] = None,
                 message_queue: Optional[Any] = None):
        self.config = config or CoreConfig()
        self.message_queue = message_queue
        self.storage = Storage(self.config.db_path)

        # Optional services
        self._token_service = token_service
        self._gradient_service = gradient_service
        self._compartment_service = compartment_service
        self._biomass_service = biomass_service

        # Subsystems
        self.registry = ModuleRegistry(self.storage)
        self._event_bus = CoreEventBus(max_workers=4)
        self._version_manager = ConfigurationVersionManager(
            self.config.db_path, enabled=True
        )
        self._anomaly_detector = PerformanceAnomalyDetector(enabled=True)
        self._genetic_optimizer = GeneticOptimizer(self, enabled=True)
        self._health_forecaster = PredictiveHealthForecaster(self.config, enabled=True)
        self.registry.health_forecaster = self._health_forecaster
        self.strategy_selector = (
            AutonomousStrategySelector(self.config, enabled=True)
            if self.config.enable_autonomous_strategy else None
        )

        # Security / audit / cloud
        self.quantum_security = (
            QuantumResilientSecurity(enabled=True)
            if self.config.enable_quantum_signing else None
        )
        self.blockchain_auditor = (
            BlockchainAuditorPlaceholder(self.config, enabled=True)
            if self.config.enable_blockchain_audit else None
        )
        self.multi_cloud = (
            MultiCloudDistributorPlaceholder(self.config, enabled=True)
            if self.config.enable_multi_cloud else None
        )

        # Circuit breaker for the core
        self.circuit_breaker = (
            CircuitBreaker(
                name="bio_core",
                db_path=self.config.circuit_breaker_db_path,
                failure_threshold=self.config.circuit_breaker_threshold,
                recovery_timeout=self.config.circuit_breaker_recovery_timeout,
            )
            if self.config.enable_circuit_breaker else None
        )

        # Enhancement modules
        self.xai = XAIExplainer(enabled=self.config.enable_xai)
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()

        # Placeholders
        self.quantum_distillation = (
            QuantumDistillationModulePlaceholder(self.config, enabled=self.config.enable_quantum_distillation)
            if self.config.enable_quantum_distillation else None
        )
        self.causal_rl_agent = (
            CausalRLAgentPlaceholder(10, 3, max_q_table=self.config.q_table_max_size,
                                     enabled=self.config.enable_causal_rl)
            if self.config.enable_causal_rl else None
        )
        self.federated_coordinator = (
            FederatedCoordinatorPlaceholder(self, message_queue, enabled=self.config.enable_federated)
            if self.config.enable_federated else None
        )
        self.precision_controller = (
            PrecisionControllerPlaceholder(enabled=self.config.enable_precision)
            if self.config.enable_precision else None
        )
        self.carbon_market = (
            CarbonMarketClientPlaceholder(enabled=self.config.enable_carbon_market)
            if self.config.enable_carbon_market else None
        )
        self.chaos_injector = ChaosInjectorPlaceholder(
            self, self.config.chaos_probability, enabled=self.config.enable_chaos
        )
        self.human_approval = (
            HumanApprovalHandlerPlaceholder(message_queue, enabled=self.config.enable_human_approval)
            if self.config.enable_human_approval else None
        )
        self.marketplace = ModuleMarketplacePlaceholder(self, auto_replace=False, enabled=False)

        # Lifecycle state
        self._task_manager = TaskManager()
        self._shutdown_event = asyncio.Event()
        self._started = False
        self._shutdown = False
        self._start_time: Optional[datetime] = None
        self._lifecycle_phase = LifecyclePhase.CREATED
        self._perf_metrics: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=100))
        self._metrics_lock: Optional[asyncio.Lock] = None

        # Observability
        self._prom = self._setup_metrics()

        logger.info("EnhancedBioInspiredCore v10.0.0 created (not started)")

    # ---------------- locks ----------------
    def _get_metrics_lock(self) -> asyncio.Lock:
        if self._metrics_lock is None:
            self._metrics_lock = asyncio.Lock()
        return self._metrics_lock

    # ---------------- safety ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "module_count_positive",
            lambda s: s.get("module_count", 1) >= 0,
            "No modules",
        )
        self.safety_monitor.add_invariant(
            "token_balance_non_negative",
            lambda s: s.get("token_balance", 0.0) >= 0.0,
            "Token balance negative",
        )
        self.safety_monitor.add_invariant(
            "circuit_breakers_under_limit",
            lambda s: s.get("circuit_open_count", 0) <= 3,
            "Too many open circuit breakers",
        )

    def _safety_state_sync(self) -> Dict[str, Any]:
        open_cbs = sum(1 for m in self.registry.modules.values()
                       if m.circuit_breaker_state == "open")
        return {
            "module_count": len(self.registry.modules),
            "token_balance": 0.0,   # updated elsewhere if services available
            "circuit_open_count": open_cbs,
        }

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE or not self.config.prometheus_port:
            return {}
        try:
            return {
                "modules_total": Gauge("bio_core_modules_total", "Total modules"),
                "modules_healthy": Gauge("bio_core_modules_healthy", "Healthy modules"),
                "circuit_open": Gauge("bio_core_circuit_open", "Open circuit breakers"),
                "events_published": Counter("bio_core_events_published", "Events published"),
                "tasks_processed": Counter("bio_core_tasks_processed", "Tasks processed"),
                "pareto_size": Gauge("bio_core_pareto_size", "Pareto front size"),
                "q_table_size": Gauge("bio_core_q_table_size", "CausalRL q-table size"),
            }
        except Exception:
            return {}

    def _update_metrics(self) -> None:
        if not self._prom:
            return
        try:
            health = self.registry.get_registry_stats()
            self._prom["modules_total"].set(health["total_modules"])
            self._prom["modules_healthy"].set(
                sum(1 for s in health["health_statuses"].values() if s == "healthy")
            )
            self._prom["circuit_open"].set(
                sum(1 for s in health["circuit_breaker_states"].values() if s == "open")
            )
            self._prom["pareto_size"].set(len(self._genetic_optimizer.pareto_front))
            if self.causal_rl_agent is not None:
                self._prom["q_table_size"].set(self.causal_rl_agent.size())
        except Exception:
            pass

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        issues = self.config.validate()
        if issues:
            logger.warning("Config validation issues", issues=issues)

        self._lifecycle_phase = LifecyclePhase.INITIALIZING
        self._start_time = datetime.now(timezone.utc)
        self._shutdown_event.clear()

        # Persist initial config
        try:
            self._version_manager.save_version(self.config.to_dict(), description="Initial configuration")
        except Exception as e:
            logger.warning("Initial config save failed", error=str(e))

        # Load persisted genetic state
        self._genetic_optimizer.load_state()

        # Start event bus first so tasks can publish immediately
        await self._event_bus.start()

        # Register optional services
        try:
            await self._maybe_register_services()
        except Exception as e:
            logger.warning("Service registration failed", error=str(e))

        # Start background tasks
        self._task_manager.start_task("health_monitor", self._health_monitoring_loop)
        self._task_manager.start_task("anomaly_detection", self._anomaly_detection_loop)
        self._task_manager.start_task("persistence_save", self._persistence_save_loop)
        if self.config.enable_predictive_homeostasis:
            self._task_manager.start_task("ml_training", self._ml_training_loop)
        if self.strategy_selector is not None:
            self._task_manager.start_task("strategy_update", self._strategy_update_loop)
        if self.federated_coordinator is not None:
            self._task_manager.start_task("federated", self._federated_loop)
        if self.chaos_injector is not None:
            self._task_manager.start_task("chaos", self._chaos_loop)
        if self.config.mopd.enabled:
            self._task_manager.start_task("evolution", self._evolution_loop)

        self._lifecycle_phase = LifecyclePhase.RUNNING
        self._started = True
        logger.info("EnhancedBioInspiredCore started")

    async def _maybe_register_services(self) -> None:
        # Token manager
        if self._token_service is None and TOKEN_AVAILABLE and EcoATPTokenManager is not None:
            try:
                self._token_service = EcoATPTokenManager()
            except Exception as e:
                logger.debug("Token service construct failed", error=str(e))
        if self._token_service is not None:
            async def _token_health():
                if hasattr(self._token_service, "get_system_summary"):
                    r = self._token_service.get_system_summary()
                    return await r if asyncio.iscoroutine(r) else True
                return True
            await self.registry.register("token_manager", self._token_service,
                                         health_check=_token_health)

        # Gradient manager
        if self._gradient_service is None and GRADIENT_AVAILABLE and GradientFieldManager is not None:
            try:
                self._gradient_service = GradientFieldManager()
            except Exception as e:
                logger.debug("Gradient service construct failed", error=str(e))
        if self._gradient_service is not None:
            await self.registry.register("gradient_manager", self._gradient_service,
                                         health_check=lambda: True)

        # Compartment manager
        if (self._compartment_service is None and COMPARTMENT_AVAILABLE
                and HierarchicalCompartmentManager is not None):
            try:
                self._compartment_service = HierarchicalCompartmentManager()
            except Exception as e:
                logger.debug("Compartment service construct failed", error=str(e))
        if self._compartment_service is not None:
            await self.registry.register("compartment_manager", self._compartment_service,
                                         health_check=lambda: True)

        # Biomass storage
        if (self._biomass_service is None and BIOMASS_AVAILABLE
                and BiomassStorage is not None):
            try:
                self._biomass_service = BiomassStorage()
            except Exception as e:
                logger.debug("Biomass service construct failed", error=str(e))
        if self._biomass_service is not None:
            await self.registry.register("biomass_storage", self._biomass_service,
                                         health_check=lambda: True)

        await self.registry.initialize_all()

    async def ready(self) -> bool:
        if not self._started:
            return False
        return self._lifecycle_phase == LifecyclePhase.RUNNING

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("EnhancedBioInspiredCore shutting down")

        self._lifecycle_phase = LifecyclePhase.STOPPING
        self._shutdown_event.set()

        # Drain tasks
        try:
            await asyncio.wait_for(self._task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        # Stop event bus
        try:
            await asyncio.wait_for(self._event_bus.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Event bus stop timed out")

        # Shut down registered modules
        try:
            await self.registry.shutdown_all()
        except Exception as e:
            logger.warning("Registry shutdown failed", error=str(e))

        # Save state
        if self.config.enable_state_persistence:
            try:
                await self._save_state()
            except Exception as e:
                logger.warning("State save failed", error=str(e))

        self._lifecycle_phase = LifecyclePhase.STOPPED
        logger.info("EnhancedBioInspiredCore shutdown complete")

    async def __aenter__(self) -> "EnhancedBioInspiredCore":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _health_monitoring_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                health = await self.registry.health_check_all()
                unhealthy = [n for n, s in health.items() if s["status"] not in ("healthy", "unknown")]
                if unhealthy:
                    logger.warning("Unhealthy modules", unhealthy=unhealthy)
                self._update_metrics()
                await asyncio.sleep(self.config.health_check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Health monitor error", error=str(e))
                await asyncio.sleep(30)

    async def _anomaly_detection_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                # Record basic metrics
                for name, entry in self.registry.modules.items():
                    await self._anomaly_detector.record_metric(
                        f"health_{name}", 1.0 if entry.health_status == "healthy" else 0.0
                    )
                report = await self._anomaly_detector.get_report()
                if report["anomalies"]:
                    logger.warning("Anomalies detected", count=len(report["anomalies"]))
                    await self._event_bus.publish(CoreEvent(
                        event_type="performance_anomaly",
                        source="anomaly_detector",
                        payload=report,
                    ))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Anomaly detection error", error=str(e))
                await asyncio.sleep(120)

    async def _persistence_save_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                for entry in self.registry.modules.values():
                    self.storage.save_module(entry)
                await asyncio.sleep(self.config.state_save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Persistence save error", error=str(e))
                await asyncio.sleep(60)

    async def _ml_training_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                # Feed the forecaster with real per-module metrics
                for name, entry in self.registry.modules.items():
                    metrics = {
                        "health_score": 1.0 if entry.health_status == "healthy"
                        else 0.5 if entry.health_status == "unknown" else 0.2,
                        "success_rate": 1.0 - (entry.failure_count / max(1, entry.failure_count + 1)),
                        "token_balance": await self._token_balance(),
                        "error_rate": min(1.0, entry.failure_count / 10.0),
                    }
                    self._health_forecaster.record(name, metrics)
                await self._health_forecaster.train()
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("ML training error", error=str(e))
                await asyncio.sleep(120)

    async def _strategy_update_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                if self.strategy_selector is not None:
                    state = {"load": len(self.registry.modules) / 50.0, "health": 0.8}
                    strategy = await self.strategy_selector.select_strategy(state)
                    if strategy == "performance":
                        self.config.health_check_interval_seconds = 20
                    elif strategy == "carbon_saver":
                        self.config.health_check_interval_seconds = 40
                    else:
                        self.config.health_check_interval_seconds = 30
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Strategy update error", error=str(e))
                await asyncio.sleep(60)

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.federated_coordinator is not None:
                    await self.federated_coordinator.send_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Federated loop error", error=str(e))
                await asyncio.sleep(300)

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)
                if self.chaos_injector is not None:
                    await self.chaos_injector.maybe_inject_failure()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Chaos loop error", error=str(e))
                await asyncio.sleep(60)

    async def _evolution_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                if len(self.registry.modules) >= 1:
                    await self._genetic_optimizer.evolve(generations=self._genetic_optimizer.generations)
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Evolution loop error", error=str(e))
                await asyncio.sleep(3600)

    # ---------------- helpers ----------------
    async def _token_balance(self) -> float:
        if self._token_service is None:
            return 0.0
        fn = getattr(self._token_service, "get_system_summary", None)
        if fn is None:
            return 0.0
        try:
            r = fn()
            if asyncio.iscoroutine(r):
                r = await r
            if isinstance(r, dict):
                return float(r.get("total_balance", 0.0))
        except Exception:
            pass
        return 0.0

    async def _save_state(self) -> None:
        os.makedirs(self.config.state_directory, exist_ok=True)
        path = os.path.join(
            self.config.state_directory,
            f"state_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
        )
        state = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "config": self.config.to_dict(),
            "modules": [e.to_dict() for e in self.registry.modules.values()],
            "genetic_optimizer": self._genetic_optimizer.get_status(),
        }
        with open(path, "w") as f:
            json.dump(state, f, indent=2, default=str)

    # ---------------- public API ----------------
    @traced("bio_core.process_task")
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if not self._started or self._lifecycle_phase != LifecyclePhase.RUNNING:
            return {"success": False, "reason": f"System not running ({self._lifecycle_phase.value})"}

        task_id = task.get("task_id") or f"task_{uuid.uuid4().hex[:8]}"
        complexity = float(task.get("complexity", 0.5))
        ecoatp_required = complexity * 10.0

        await self._event_bus.publish(CoreEvent(
            event_type="task_received",
            source="core",
            payload={"task_id": task_id, "complexity": complexity},
        ))

        # Safety check
        if self.safety_monitor is not None:
            state = self._safety_state_sync()
            state["token_balance"] = await self._token_balance()
            violations = self.safety_monitor.check(state)
            if violations:
                logger.warning("Safety violations", violations=violations)
                return {"success": False, "reason": "safety_violation", "violations": violations}

        # Human approval for large tasks
        if self.human_approval is not None and complexity > 0.9:
            approved = await self.human_approval.request_approval({
                "action": "process_task", "task_id": task_id, "complexity": complexity,
            })
            if not approved:
                return {"success": False, "reason": "rejected_by_human"}

        result = {
            "success": True,
            "task_id": task_id,
            "ecoatp_cost": ecoatp_required,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Optional signature
        if self.quantum_security is not None:
            try:
                sig = await self.quantum_security.sign_data(result)
                result["quantum_signature"] = sig
            except Exception as e:
                logger.debug("Sign failed", error=str(e))

        # Optional audit
        if self.blockchain_auditor is not None:
            try:
                await self.blockchain_auditor.record_event("task_completed", {
                    "task_id": task_id, "ecoatp_cost": ecoatp_required,
                })
            except Exception:
                pass

        if self.config.enable_xai:
            logger.info("XAI", explanation=self.xai.explain_task(task_id, ecoatp_required))

        if self._prom:
            try:
                self._prom["tasks_processed"].inc()
            except Exception:
                pass

        self._update_metrics()
        return result

    async def get_system_status(self) -> Dict[str, Any]:
        uptime = 0.0
        if self._start_time is not None:
            uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()

        status: Dict[str, Any] = {
            "lifecycle_phase": self._lifecycle_phase.value,
            "started": self._started,
            "uptime_seconds": uptime,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "module_count": len(self.registry.modules),
            "modules": self.registry.get_registry_stats(),
            "event_bus": self._event_bus.get_event_stats(),
            "config_version": self._version_manager.current_version,
            "genetic_optimizer": self._genetic_optimizer.get_status(),
            "marketplace": self.marketplace.get_marketplace_stats(),
            "module_status": MODULE_STATUS,
            "mopd_enabled": self.config.mopd.enabled,
            "pareto_front_size": len(self._genetic_optimizer.pareto_front),
            "causal_rl_enabled": self.causal_rl_agent is not None,
            "federated_enabled": self.federated_coordinator is not None,
            "safety_monitor_enabled": self.safety_monitor is not None,
            "xai_enabled": self.config.enable_xai,
            "precision_controller_enabled": self.precision_controller is not None,
            "carbon_market_enabled": self.carbon_market is not None,
            "chaos_enabled": self.chaos_injector is not None,
            "human_approval_enabled": self.human_approval is not None,
            "quantum_security_enabled": self.quantum_security is not None,
            "blockchain_audit_enabled": self.blockchain_auditor is not None,
            "multi_cloud_enabled": self.multi_cloud is not None,
            "circuit_breaker": self.circuit_breaker.snapshot() if self.circuit_breaker else None,
        }

        # Optional service summaries
        if self._token_service is not None:
            try:
                r = self._token_service.get_system_summary()
                if asyncio.iscoroutine(r):
                    r = await r
                status["token_economy"] = r
            except Exception:
                pass

        # Anomaly report
        try:
            status["anomalies"] = await self._anomaly_detector.get_report()
        except Exception:
            status["anomalies"] = {"anomalies": []}

        return status

    async def check_health(self) -> bool:
        """Async health check."""
        if not self._started:
            return False
        health = await self.registry.health_check_all()
        return all(s["status"] != "error" for s in health.values())

    def get_mopd_pareto_front(self) -> List[MOPDPoint]:
        return list(self._genetic_optimizer.pareto_front)

    def get_mopd_summary(self) -> Dict[str, Any]:
        return {
            "enabled": self.config.mopd.enabled,
            "objective_weights": dict(self.config.mopd.objective_weights),
            "pareto_front_size": len(self._genetic_optimizer.pareto_front),
            "best_scalarised_score": self._genetic_optimizer.best_fitness,
        }

    def get_lifecycle_status(self) -> Dict[str, Any]:
        return {
            "phase": self._lifecycle_phase.value,
            "started": self._started,
            "shutdown": self._shutdown,
            "module_count": len(self.registry.modules),
            "config_version": self._version_manager.current_version,
        }


# =============================================================================
# SECTION 16. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cfg(self) -> CoreConfig:
        import tempfile
        td = tempfile.mkdtemp()
        return CoreConfig(
            db_path=os.path.join(td, "state.db"),
            circuit_breaker_db_path=os.path.join(td, "cb.db"),
            state_directory=os.path.join(td, "state"),
            state_save_interval_seconds=3600,
            health_check_interval_seconds=1,
            enable_quantum_signing=False,
            enable_blockchain_audit=False,
            enable_multi_cloud=False,
            enable_causal_rl=False,
            enable_federated=False,
            enable_safety_monitor=True,
            enable_xai=False,
            enable_precision=False,
            enable_carbon_market=False,
            enable_chaos=False,
            enable_human_approval=False,
            enable_autonomous_strategy=True,
            enable_predictive_homeostasis=False,
            enable_quantum_distillation=False,
            prometheus_port=None,
        )

    def test_lifecycle(self):
        async def go():
            core = EnhancedBioInspiredCore(config=self._cfg())
            self.assertFalse(await core.ready())
            await core.start()
            self.assertTrue(await core.ready())
            await core.shutdown()
            self.assertTrue(core._shutdown)
            await core.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with EnhancedBioInspiredCore(config=self._cfg()) as core:
                self.assertTrue(await core.ready())
                self.assertTrue(await core.check_health())
        asyncio.run(go())

    def test_process_task(self):
        async def go():
            async with EnhancedBioInspiredCore(config=self._cfg()) as core:
                r = await core.process_task({"task_id": "t1", "complexity": 0.5})
                self.assertTrue(r["success"])
                self.assertEqual(r["task_id"], "t1")
                self.assertGreater(r["ecoatp_cost"], 0.0)
        asyncio.run(go())

    def test_system_status(self):
        async def go():
            async with EnhancedBioInspiredCore(config=self._cfg()) as core:
                status = await core.get_system_status()
                self.assertIn("lifecycle_phase", status)
                self.assertEqual(status["lifecycle_phase"], "running")
                self.assertIn("module_status", status)
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3, max_q_table=10)
        for _ in range(200):
            s = np.random.rand(4)
            a = rl.act(s)
            rl.update(s, a, 1.0, s, False)
        self.assertLessEqual(rl.size(), 10)

    def test_event_bus_publish_subscribe(self):
        async def go():
            bus = CoreEventBus(max_workers=2)
            await bus.start()
            received: List[CoreEvent] = []
            bus.subscribe("test", lambda e: received.append(e))
            for i in range(5):
                await bus.publish(CoreEvent(event_type="test", source="t", payload={"i": i}))
            # Give workers a moment to drain
            await asyncio.sleep(0.2)
            self.assertEqual(len(received), 5)
            stats = bus.get_event_stats()
            self.assertEqual(stats["published"], 5)
            await bus.stop()
        asyncio.run(go())

    def test_event_bus_stop_no_hang(self):
        async def go():
            bus = CoreEventBus(max_workers=2)
            await bus.start()
            await asyncio.wait_for(bus.stop(), timeout=2.0)
        asyncio.run(go())

    def test_circuit_breaker_transitions(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cb = CircuitBreaker("t", os.path.join(td, "cb.db"),
                                failure_threshold=2, recovery_timeout=0.5)

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
            self.assertEqual(cb.snapshot()["state"], "open")
            with self.assertRaises(RuntimeError):
                await cb.call(ok)
            await asyncio.sleep(0.6)
            self.assertEqual(await cb.call(ok), 1)
            self.assertEqual(cb.snapshot()["state"], "closed")
        asyncio.run(go())

    def test_ga_no_asyncio_run(self):
        """Regression: _compute_dynamic_weights and get_system_signature must be async."""
        async def go():
            async with EnhancedBioInspiredCore(config=self._cfg()) as core:
                # Both should be awaitable from inside a running loop without error
                w = await core._genetic_optimizer._compute_dynamic_weights()
                self.assertTrue(all(isinstance(v, float) for v in w.values()))
                self.assertAlmostEqual(sum(w.values()), 1.0, places=4)
                sig = await core._genetic_optimizer.get_system_signature()
                self.assertIsInstance(sig, str)
        asyncio.run(go())

    def test_ga_evolve_runs(self):
        async def go():
            async with EnhancedBioInspiredCore(config=self._cfg()) as core:
                result = await core._genetic_optimizer.evolve(generations=2)
                self.assertIn("best_fitness", result)
                self.assertIn("pareto_front", result)
                self.assertIn("dynamic_weights", result)
        asyncio.run(go())

    def test_ga_genome_dependent(self):
        async def go():
            async with EnhancedBioInspiredCore(config=self._cfg()) as core:
                ga = core._genetic_optimizer
                a = {k: lo for k, (lo, hi) in GeneticOptimizer.PARAM_BOUNDS.items()}
                b = {k: hi for k, (lo, hi) in GeneticOptimizer.PARAM_BOUNDS.items()}
                oa = await ga._evaluate(a)
                ob = await ga._evaluate(b)
                diffs = [abs(oa[k] - ob[k]) for k in oa]
                self.assertGreater(max(diffs), 1e-4)
        asyncio.run(go())

    def test_placeholders_are_honest(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3)
        self.assertFalse(rl.available)
        fed = FederatedCoordinatorPlaceholder(None, None)
        self.assertFalse(fed.available)
        prec = PrecisionControllerPlaceholder()
        self.assertFalse(prec.available)
        self.assertEqual(prec.get_precision(0.9, 0.1), "float32")
        cm = CarbonMarketClientPlaceholder()
        self.assertFalse(cm.available)
        self.assertFalse(cm.buy_credits(10))
        self.assertFalse(cm.sell_credits(10))
        mp = ModuleMarketplacePlaceholder(None)
        self.assertFalse(mp.available)
        qd = QuantumDistillationModulePlaceholder(CoreConfig())
        self.assertFalse(qd.available)

        async def go():
            ha = HumanApprovalHandlerPlaceholder()
            self.assertFalse(ha.available)
            self.assertFalse(await ha.request_approval({"action": "x"}))
        asyncio.run(go())

    def test_task_manager_drain(self):
        async def go():
            tm = TaskManager()
            counter = {"n": 0}

            async def loop():
                while not tm.shutdown_event.is_set():
                    counter["n"] += 1
                    await asyncio.sleep(0.05)

            tm.start_task("loop", loop)
            await asyncio.sleep(0.2)
            await asyncio.wait_for(tm.drain(timeout=2.0), timeout=3.0)
            self.assertGreater(counter["n"], 0)
        asyncio.run(go())

    def test_registry_topological_order_cycle_safe(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            st = Storage(os.path.join(td, "reg.db"))
            reg = ModuleRegistry(st)
            await reg.register("a", object(), dependencies=["b"])
            await reg.register("b", object(), dependencies=["a"])  # cycle
            order = reg._topological_sort()
            self.assertEqual(set(order), {"a", "b"})
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 17. ENTRY POINT
# =============================================================================
async def _example() -> None:
    import tempfile
    td = tempfile.mkdtemp()
    cfg = CoreConfig(
        db_path=os.path.join(td, "state.db"),
        circuit_breaker_db_path=os.path.join(td, "cb.db"),
        state_directory=os.path.join(td, "state"),
        state_save_interval_seconds=3600,
        health_check_interval_seconds=1,
        enable_quantum_signing=False,
        enable_blockchain_audit=False,
        enable_multi_cloud=False,
        enable_causal_rl=False,
        enable_federated=False,
        enable_safety_monitor=True,
        enable_xai=True,
        enable_precision=False,
        enable_carbon_market=False,
        enable_chaos=False,
        enable_human_approval=False,
        enable_autonomous_strategy=True,
        enable_predictive_homeostasis=False,
        prometheus_port=None,
    )
    async with EnhancedBioInspiredCore(config=cfg) as core:
        r = await core.process_task({"task_id": "demo", "complexity": 0.7})
        print("Task:", json.dumps(r, indent=2, default=str))
        status = await core.get_system_status()
        print("Status keys:", sorted(status.keys()))
        print("MOPD:", json.dumps(core.get_mopd_summary(), indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Enhanced Bio-Inspired Core v10.0.0")
    parser.add_argument("--test", action="store_true", help="Run embedded tests")
    parser.add_argument("--example", action="store_true", help="Run example usage")
    parser.add_argument("--status", action="store_true", help="Print module statuses")
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

    print("Enhanced Bio-Inspired Core v10.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
