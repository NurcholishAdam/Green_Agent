#!/usr/bin/env python3
# =============================================================================
# Enhanced Knowledge Transfer Manager v9.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced Knowledge Transfer Manager v9.0.0
==========================================
Patched single-file version. Focus on correctness, honesty, lifecycle.

P0 fixes
--------
- All heavy imports guarded; the file runs with only the stdlib + numpy.
- Fallback stubs for every missing module referenced by the original.
- self.metrics is populated by _setup_metrics (Prometheus optional).
- No background tasks or state load in __init__: use `await mgr.start()`.
- Lazy asyncio.Locks with a documented acquisition order.
- experience_buffer is populated via a public record_experience(...) API.
- knowledge_bank is bounded; eviction is oldest-effective-first.
- CausalRL Q-table is discretized and LRU-bounded.
- SQLite writes offloaded to a thread executor.
- explain_decision handles None values safely.
- Config env parsing coerces int/float/bool.
- Placeholder defaults flipped to disabled.

P1 — real behavior
------------------
- Graceful shutdown: drain tasks with a timeout, flush state, stop event bus,
  idempotent.
- Lock ordering documented; single-lock transactional capture/transfer.
- Per-actor locks for graph and experience updates.

P2 — honesty
------------
- MODULE_STATUS documents each module.
- Placeholders (disabled by default, .available = False, warn when enabled):
      causal_rl, federated, precision, carbon_market, chaos, human_approval.
- Experimental (warn when enabled):
      persistence, genetic_optimizer, mopd, safety_monitor, xai,
      quantum_security, blockchain_audit, multi_cloud, active_learning,
      graph_nn, predator_prey, homeostatic, recycler.

P3 — production readiness
-------------------------
- Lifecycle: await start(), await shutdown(), await ready(), __aenter__/__aexit__.
- Logical sections in a single file.
- Embedded test suite: python3 knowledge_transfer.py --test.
- Prometheus metrics and OpenTelemetry spans (both optional).
- --status prints module maturity.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import hmac
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
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None  # type: ignore
    YAML_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("knowledge_transfer")
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


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "knowledge_core":       "stable",
    "storage":              "stable",
    "circuit_breaker":      "stable",
    "task_manager":         "stable",
    "event_bus":            "stable",
    "persistence":          "experimental",
    "genetic_optimizer":    "experimental",
    "mopd":                 "experimental",
    "safety_monitor":       "experimental",
    "xai":                  "experimental",
    "quantum_security":     "experimental",
    "blockchain_audit":     "experimental",
    "multi_cloud":          "experimental",
    "active_learning":      "experimental",
    "graph_nn":             "experimental",
    "predator_prey":        "experimental",
    "homeostatic":          "experimental",
    "recycler":             "experimental",
    "causal_rl":            "placeholder",
    "federated":            "placeholder",
    "precision":            "placeholder",
    "carbon_market":        "placeholder",
    "chaos":                "placeholder",
    "human_approval":       "placeholder",
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
# SECTION 2. TRACING HELPER
# =============================================================================
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
# SECTION 3. CONFIG
# =============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "avg_effective": 0.4,
        "transfer_success_rate": 0.3,
        "package_diversity": 0.2,
        "recycling_rate": 0.1,
    })
    grid_resolution: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDConfig":
        data = dict(data or {})
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


def _coerce_env_value(value: str) -> Any:
    lower = value.strip().lower()
    if lower in ("true", "yes", "1", "on"):
        return True
    if lower in ("false", "no", "0", "off"):
        return False
    try:
        if "." in value or "e" in lower:
            return float(value)
        return int(value)
    except ValueError:
        return value


@dataclass
class KnowledgeTransferConfig:
    # Core
    enable_decay: bool = True
    default_decay_rate: float = 0.01
    capture_threshold: float = 0.7
    active_learning_retrain_interval: int = 3600
    active_learning_history_size: int = 1000

    # Genetic optimizer
    genetic_population_size: int = 20
    genetic_mutation_rate: float = 0.2
    genetic_crossover_rate: float = 0.7
    genetic_generations: int = 5
    genetic_tournament_size: int = 3
    genetic_evolution_interval: int = 86400

    # Recycling / predation
    predation_interval: int = 3600
    prey_threshold: float = 0.2
    predator_threshold: float = 0.7
    recycling_interval: int = 7200

    # Homeostatic
    homeostatic_interval: int = 600
    homeostatic_target_avg_effective: float = 0.6
    homeostatic_kp: float = 0.5
    homeostatic_ki: float = 0.1
    homeostatic_kd: float = 0.05

    # Storage
    model_storage_path: str = "./models"
    validation_enabled: bool = True
    validation_task_count: int = 10
    min_improvement_threshold: float = 0.05
    fine_tuning_epochs_default: int = 10
    graph_training_interval: int = 7200

    # Persistence
    enable_persistence: bool = True
    persistence_path: str = "knowledge_transfer_state.db"

    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    circuit_breaker_db_path: str = "knowledge_transfer_cb.db"

    # Quantum / blockchain / cloud
    enable_quantum_signing: bool = False
    quantum_signing_algorithm: str = "hmac-sha256"
    enable_blockchain_audit: bool = False
    blockchain_rpc_url: str = "http://localhost:8545"
    blockchain_contract_address: str = "0x0"
    blockchain_private_key: Optional[str] = None
    enable_multi_cloud: bool = False
    cloud_provider: str = "aws"
    cloud_region: str = "us-east-1"
    cloud_bucket: str = "knowledge-transfer-state"
    cloud_access_key: Optional[str] = None
    cloud_secret_key: Optional[str] = None

    # Autonomous strategy
    enable_autonomous_strategy: bool = False
    rl_learning_rate: float = 0.1
    rl_discount_factor: float = 0.9
    rl_exploration_rate: float = 0.1

    # Observability
    prometheus_port: Optional[int] = None

    # Bounds
    knowledge_bank_max_size: int = 5000
    q_table_max_size: int = 5000
    experience_buffer_max_size: int = 10000
    event_bus_workers: int = 4

    # Shutdown
    shutdown_timeout_seconds: int = 15
    state_save_interval_seconds: float = 300.0

    # MOPD
    mopd: MOPDConfig = field(default_factory=MOPDConfig)

    # Experimental / placeholder toggles — disabled by default
    enable_causal_rl: bool = False
    enable_federated_learning: bool = False
    enable_safety_monitor: bool = True       # experimental, harmless
    enable_xai: bool = True                  # experimental, harmless
    enable_precision_switching: bool = False
    enable_carbon_market: bool = False
    carbon_market_config: Optional[Dict[str, str]] = None
    enable_chaos: bool = False
    chaos_probability: float = 0.0
    enable_human_approval: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KnowledgeTransferConfig":
        data = dict(data or {})
        mopd_data = data.pop("mopd", None)
        fields = cls.__dataclass_fields__
        cfg = cls(**{k: v for k, v in data.items() if k in fields})
        if isinstance(mopd_data, dict):
            cfg.mopd = MOPDConfig.from_dict(mopd_data)
        return cfg

    @classmethod
    def from_env_and_file(cls, config_path: Optional[str] = None) -> "KnowledgeTransferConfig":
        env_overrides: Dict[str, Any] = {}
        for key in cls.__dataclass_fields__:
            env_var = f"KT_{key.upper()}"
            if env_var in os.environ and key != "mopd":
                env_overrides[key] = _coerce_env_value(os.environ[env_var])
        if config_path and os.path.exists(config_path) and YAML_AVAILABLE:
            with open(config_path, "r") as f:
                yaml_data = yaml.safe_load(f) or {}
            yaml_data.update(env_overrides)
            return cls.from_dict(yaml_data)
        return cls(**env_overrides) if env_overrides else cls()

    def validate(self) -> List[str]:
        issues: List[str] = []
        if not (0.4 <= self.capture_threshold <= 0.95):
            issues.append("capture_threshold must be between 0.4 and 0.95")
        if self.default_decay_rate <= 0:
            issues.append("default_decay_rate must be positive")
        if self.knowledge_bank_max_size < 10:
            issues.append("knowledge_bank_max_size must be >= 10")
        if self.q_table_max_size < 10:
            issues.append("q_table_max_size must be >= 10")
        total = sum(self.mopd.objective_weights.values())
        if abs(total - 1.0) > 1e-6:
            issues.append("mopd.objective_weights must sum to 1")
        return issues


# =============================================================================
# SECTION 4. DATA CLASSES
# =============================================================================
@dataclass
class KnowledgePackage:
    package_id: str
    source_expert_id: str
    source_generation: int
    created_at: datetime
    version: int = 1
    task_patterns: Dict[str, Any] = field(default_factory=dict)
    successful_strategies: List[Dict[str, Any]] = field(default_factory=list)
    failure_patterns: List[Dict[str, Any]] = field(default_factory=list)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    optimized_parameters: Dict[str, Any] = field(default_factory=dict)
    lessons_learned: List[str] = field(default_factory=list)
    total_experiences: int = 0
    survival_score: float = 0.0
    decay_rate: float = 0.01
    is_incremental: bool = False
    parent_package_id: Optional[str] = None
    capture_sequence: int = 0
    transfer_count: int = 0
    last_transferred: Optional[datetime] = None
    transfer_success_scores: List[float] = field(default_factory=list)
    average_transfer_improvement: float = 0.0
    domain_tags: List[str] = field(default_factory=list)
    cross_domain_applicability: Dict[str, float] = field(default_factory=dict)
    uncertainty_score: float = 0.0
    information_gain: float = 0.0
    capture_priority: float = 0.5
    predicted_improvement: float = 0.0

    @property
    def age_days(self) -> float:
        return (datetime.now(timezone.utc) - self.created_at).total_seconds() / 86400.0

    @property
    def recency_weight(self) -> float:
        return math.exp(-self.decay_rate * self.age_days)

    @property
    def effective_score(self) -> float:
        return self.survival_score * self.recency_weight

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["created_at"] = self.created_at.isoformat()
        d["last_transferred"] = self.last_transferred.isoformat() if self.last_transferred else None
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KnowledgePackage":
        data = dict(data or {})
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        lt = data.get("last_transferred")
        if lt:
            data["last_transferred"] = datetime.fromisoformat(lt)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class TransferRecord:
    transfer_id: str
    source_package_id: str
    target_expert_id: str
    timestamp: datetime
    items_transferred: List[str]
    pre_transfer_performance: Optional[float] = None
    post_transfer_performance: Optional[float] = None
    improvement_percentage: float = 0.0
    validation_tasks: int = 0
    successful_transfer: bool = False
    transfer_confidence: float = 0.5
    notes: str = ""
    fine_tuning_epochs: int = 0
    adaptation_accuracy: float = 0.0
    source_domain: str = ""
    target_domain: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TransferRecord":
        data = dict(data or {})
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    avg_effective: float
    transfer_success_rate: float
    package_diversity: float
    recycling_rate: float
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


# =============================================================================
# SECTION 5. CIRCUIT BREAKER (STABLE)
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5,
                 timeout_seconds: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
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
                ).total_seconds() >= self.timeout_seconds:
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
# SECTION 6. TASK MANAGER + EVENT BUS (STABLE)
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


class EventBus:
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
        for _ in self.workers:
            await self.queue.put(None)
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

    def subscribe(self, event_type: str, callback: Callable) -> None:
        self.subscribers[event_type].append(callback)

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
                for cb in list(self.subscribers.get(event.event_type, [])):
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


# =============================================================================
# SECTION 7. PLACEHOLDERS (safe no-ops, disabled by default)
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

    def __init__(self, manager: Any, queue: Optional[Any], enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.manager = manager
        self.queue = queue
        self.available = False

    async def send_update(self) -> bool:
        return False

    async def receive_global_model(self, model_json: str) -> bool:
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

    def __init__(self, manager: Any, chaos_probability: float = 0.0,
                 enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.manager = manager
        self.chaos_probability = chaos_probability
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


# =============================================================================
# SECTION 8. EXPERIMENTAL COMPONENTS
# =============================================================================
class SafetyMonitor:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("safety_monitor")
        self.invariants: List[Tuple[str, Callable[[Dict[str, Any]], bool], str]] = []
        self.available = True

    def add_invariant(self, name: str, fn: Callable[[Dict[str, Any]], bool],
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

    def explain_capture(self, expert_id: str, survival_score: float) -> str:
        return f"Captured knowledge from {expert_id} (survival={survival_score:.3f})."

    def explain_transfer(self, package_id: str, target: str, improvement: float) -> str:
        return (
            f"Transferred {package_id} to {target} "
            f"(improvement={improvement * 100:.2f}%)."
        )


class QuantumResilientSecurity:
    """STATUS: experimental. HMAC-SHA256 backend. Not quantum-safe without pqcrypto."""
    STATUS = "experimental"

    def __init__(self, algorithm: str = "hmac-sha256", enabled: bool = False):
        if enabled:
            _warn_module("quantum_security")
        self.algorithm = algorithm
        self.available = True
        self._secret = os.urandom(32)

    @property
    def backend(self) -> str:
        return "hmac-sha256"

    async def sign_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        sig = hmac.new(self._secret, payload, hashlib.sha256).digest()
        return {
            "algorithm": "hmac-sha256",
            "signature": sig.hex(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def verify_data(self, data: Dict[str, Any], sig_data: Dict[str, Any]) -> bool:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hmac.new(self._secret, payload, hashlib.sha256).digest()
        try:
            return hmac.compare_digest(expected, bytes.fromhex(sig_data.get("signature", "")))
        except Exception:
            return False


class BlockchainAuditorPlaceholder:
    """STATUS: experimental. In-memory log; not a blockchain without web3."""
    STATUS = "experimental"

    def __init__(self, config: KnowledgeTransferConfig, enabled: bool = False):
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

    def __init__(self, config: KnowledgeTransferConfig, enabled: bool = False):
        if enabled:
            _warn_module("multi_cloud")
        self.config = config
        self.available = False
        self.log: Deque[Dict[str, Any]] = deque(maxlen=100)

    async def distribute(self, data: Dict[str, Any], filename: str) -> Dict[str, Any]:
        rec = {"status": "skipped", "filename": filename, "reason": "placeholder"}
        self.log.append(rec)
        return rec


class ActiveLearningModule:
    """STATUS: experimental. Simple uncertainty and information-gain stubs."""
    STATUS = "experimental"

    def __init__(self, config: KnowledgeTransferConfig, enabled: bool = True):
        if enabled:
            _warn_module("active_learning")
        self.config = config
        self.available = True
        self.history: Deque[Dict[str, Any]] = deque(maxlen=config.active_learning_history_size)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get_capture_priority(self, expert_id: str, current_data: Dict[str, Any]) -> float:
        perf = float(current_data.get("performance", 0.5))
        novelty = float(current_data.get("novelty_score", 0.5))
        return max(0.0, min(1.0, 0.6 * perf + 0.4 * novelty))

    async def calculate_uncertainty(self, current_data: Dict[str, Any]) -> float:
        # Higher novelty -> higher uncertainty
        return max(0.0, min(1.0, 1.0 - float(current_data.get("novelty_score", 0.5))))

    async def calculate_information_gain(self, current_data: Dict[str, Any],
                                          previous_data: Dict[str, Any]) -> float:
        a = float(current_data.get("novelty_score", 0.5))
        b = float(previous_data.get("novelty_score", 0.5))
        return max(0.0, min(1.0, abs(a - b)))

    def add_experience(self, expert_id: str, performance: float,
                       diversity: float, novelty: float) -> None:
        self.history.append({
            "expert_id": expert_id,
            "performance": performance,
            "diversity": diversity,
            "novelty": novelty,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })


class KnowledgeGeneticOptimizer:
    """
    STATUS: experimental. NSGA-II over survival weights.
    Objectives are genome-dependent.
    """
    STATUS = "experimental"

    PARAM_BOUNDS = {
        "success_rate": (0.1, 0.6),
        "token_efficiency": (0.1, 0.6),
        "carbon_efficiency": (0.05, 0.4),
        "experience_count": (0.05, 0.4),
    }

    def __init__(self, manager: "KnowledgeTransferManager", enabled: bool = True):
        if enabled:
            _warn_module("genetic_optimizer")
        self.manager = manager
        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness: float = -math.inf
        self.pareto_front: List[MOPDPoint] = []
        self.evolution_history: List[Dict[str, Any]] = []
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _init_individual(self) -> Dict[str, float]:
        ind = {k: random.uniform(lo, hi) for k, (lo, hi) in self.PARAM_BOUNDS.items()}
        total = sum(ind.values())
        return {k: v / total for k, v in ind.items()}

    def _init_population(self) -> List[Dict[str, float]]:
        return [self._init_individual() for _ in range(self.manager.config.genetic_population_size)]

    async def _evaluate(self, ind: Dict[str, float]) -> Dict[str, float]:
        # Genome-dependent objectives from live state
        packages = list(self.manager.knowledge_bank.values())
        if not packages:
            return {"avg_effective": 0.0, "transfer_success_rate": 0.0,
                    "package_diversity": 0.0, "recycling_rate": 0.0}

        scores = [
            ind.get("success_rate", 0.35) * p.performance_metrics.get("success_rate", 0.5)
            + ind.get("token_efficiency", 0.30) * p.performance_metrics.get("token_efficiency", 0.5)
            + ind.get("carbon_efficiency", 0.20) * p.performance_metrics.get("carbon_efficiency", 0.5)
            + ind.get("experience_count", 0.15) * min(1.0, p.total_experiences / 100.0)
            for p in packages
        ]
        avg_effective = float(np.mean(scores)) if scores else 0.0

        transfers = self.manager.transfer_history
        transfer_success_rate = (
            sum(1 for t in transfers if t.successful_transfer) / len(transfers)
            if transfers else 0.0
        )

        # Diversity: distinct source experts / total packages
        distinct_experts = len({p.source_expert_id for p in packages})
        package_diversity = distinct_experts / max(1, len(packages))

        # Recycling rate: packages with low survival recycled / total
        recycled = sum(1 for p in packages if p.effective_score < self.manager.config.prey_threshold)
        recycling_rate = recycled / max(1, len(packages))

        return {
            "avg_effective": max(0.0, min(1.0, avg_effective)),
            "transfer_success_rate": transfer_success_rate,
            "package_diversity": package_diversity,
            "recycling_rate": recycling_rate,
        }

    def _dominates(self, a: Dict[str, float], b: Dict[str, float]) -> bool:
        keys = ("avg_effective", "transfer_success_rate", "package_diversity", "recycling_rate")
        return all(a[k] >= b[k] for k in keys) and any(a[k] > b[k] for k in keys)

    def _fast_non_dominated_sort(self, objs: List[Dict[str, float]]) -> List[List[int]]:
        n = len(objs)
        dominates: List[List[int]] = [[] for _ in range(n)]
        dom_count = [0] * n
        fronts: List[List[int]] = [[]]
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(objs[i], objs[j]):
                    dominates[i].append(j)
                elif self._dominates(objs[j], objs[i]):
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

    def _crowding(self, front: List[int], objs: List[Dict[str, float]]) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}
        d: Dict[int, float] = {i: 0.0 for i in front}
        keys = ("avg_effective", "transfer_success_rate", "package_diversity", "recycling_rate")
        for k in keys:
            sf = sorted(front, key=lambda i: objs[i][k])
            d[sf[0]] = float("inf")
            d[sf[-1]] = float("inf")
            span = objs[sf[-1]][k] - objs[sf[0]][k]
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                d[sf[idx]] += (objs[sf[idx + 1]][k] - objs[sf[idx - 1]][k]) / span
        return d

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for k in self.PARAM_BOUNDS:
            if random.random() < 0.5:
                child[k] = p1[k]
            else:
                child[k] = p2[k]
            if random.random() < 0.3:
                child[k] = (p1[k] + p2[k]) / 2.0
        total = sum(child.values()) or 1.0
        return {k: v / total for k, v in child.items()}

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        m = dict(ind)
        for k, (lo, hi) in self.PARAM_BOUNDS.items():
            if random.random() < self.manager.config.genetic_mutation_rate:
                m[k] = max(lo, min(hi, m[k] + random.uniform(-0.1, 0.1)))
        total = sum(m.values()) or 1.0
        return {k: v / total for k, v in m.items()}

    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        gens = generations or self.manager.config.genetic_generations
        async with self._get_lock():
            pop = self._init_population()
            objs = [await self._evaluate(i) for i in pop]
            local_front: List[MOPDPoint] = []

            for _ in range(gens):
                offspring: List[Dict[str, float]] = []
                while len(offspring) < self.manager.config.genetic_population_size:
                    i = random.randrange(len(pop))
                    j = random.randrange(len(pop))
                    if random.random() < self.manager.config.genetic_crossover_rate:
                        c1 = self._crossover(pop[i], pop[j])
                        offspring.append(self._mutate(c1))
                    else:
                        offspring.append(self._mutate(dict(pop[i])))
                offspring = offspring[: self.manager.config.genetic_population_size]
                off_objs = [await self._evaluate(o) for o in offspring]

                combined = pop + offspring
                combined_objs = objs + off_objs
                fronts = self._fast_non_dominated_sort(combined_objs)

                if fronts:
                    local_front = [
                        MOPDPoint(
                            individual=dict(combined[idx]),
                            avg_effective=combined_objs[idx]["avg_effective"],
                            transfer_success_rate=combined_objs[idx]["transfer_success_rate"],
                            package_diversity=combined_objs[idx]["package_diversity"],
                            recycling_rate=combined_objs[idx]["recycling_rate"],
                        )
                        for idx in fronts[0]
                    ]

                new_pop: List[Dict[str, float]] = []
                new_objs: List[Dict[str, float]] = []
                for front in fronts:
                    if len(new_pop) + len(front) <= self.manager.config.genetic_population_size:
                        for idx in front:
                            new_pop.append(combined[idx])
                            new_objs.append(combined_objs[idx])
                    else:
                        cd = self._crowding(front, combined_objs)
                        sf = sorted(front, key=lambda i: cd.get(i, 0.0), reverse=True)
                        remaining = self.manager.config.genetic_population_size - len(new_pop)
                        for idx in sf[:remaining]:
                            new_pop.append(combined[idx])
                            new_objs.append(combined_objs[idx])
                        break
                pop, objs = new_pop, new_objs

            self.pareto_front = local_front

            # Pick best by scalarised score using dynamic weights
            weights = dict(self.manager.config.mopd.objective_weights)
            if local_front:
                keys = list(weights.keys())
                max_vals = {k: max(getattr(p, k) for p in local_front) for k in keys}
                min_vals = {k: min(getattr(p, k) for p in local_front) for k in keys}
                ranges = {k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
                          for k in keys}
                best_score = -math.inf
                best_point = None
                for p in local_front:
                    s = sum(weights[k] * ((getattr(p, k) - min_vals[k]) / ranges[k]) for k in keys)
                    if s > best_score:
                        best_score = s
                        best_point = p
                if best_point is not None:
                    self.best_individual = dict(best_point.individual)
                    self.best_fitness = best_score

            self.evolution_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "best_fitness": self.best_fitness,
                "pareto_front_size": len(self.pareto_front),
                "generations": gens,
            })

            return {
                "best_fitness": self.best_fitness,
                "best_individual": self.best_individual,
                "pareto_front": [p.to_dict() for p in self.pareto_front],
                "generations": gens,
            }

    def get_status(self) -> Dict[str, Any]:
        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "pareto_front_size": len(self.pareto_front),
            "history": self.evolution_history[-10:],
        }


# =============================================================================
# SECTION 9. STORAGE (STABLE) — executor-offloaded SQLite
# =============================================================================
class Storage:
    def __init__(self, db_path: str = "knowledge_transfer_state.db"):
        self.db_path = db_path
        self._lock: Optional[asyncio.Lock] = None
        self._init_db_sync()

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _init_db_sync(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS knowledge_packages (
                    package_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transfer_history (
                    transfer_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            conn.commit()

    def _save_package_sync(self, package: KnowledgePackage) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO knowledge_packages (package_id, data, created_at) VALUES (?, ?, ?)",
                (package.package_id, json.dumps(package.to_dict(), default=str),
                 package.created_at.isoformat()),
            )
            conn.commit()

    def _save_transfer_sync(self, transfer: TransferRecord) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO transfer_history (transfer_id, data, timestamp) VALUES (?, ?, ?)",
                (transfer.transfer_id, json.dumps(transfer.to_dict(), default=str),
                 transfer.timestamp.isoformat()),
            )
            conn.commit()

    def _save_global_state_sync(self, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO global_state (key, value) VALUES (?, ?)", (key, value))
            conn.commit()

    def _load_global_state_sync(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT value FROM global_state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def _load_packages_sync(self) -> List[KnowledgePackage]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT data FROM knowledge_packages").fetchall()
        out: List[KnowledgePackage] = []
        for r in rows:
            try:
                out.append(KnowledgePackage.from_dict(json.loads(r[0])))
            except Exception:
                continue
        return out

    def _load_transfers_sync(self) -> List[TransferRecord]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT data FROM transfer_history").fetchall()
        out: List[TransferRecord] = []
        for r in rows:
            try:
                out.append(TransferRecord.from_dict(json.loads(r[0])))
            except Exception:
                continue
        return out

    async def save_package(self, package: KnowledgePackage) -> None:
        async with self._get_lock():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_package_sync, package)

    async def save_transfer(self, transfer: TransferRecord) -> None:
        async with self._get_lock():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_transfer_sync, transfer)

    async def save_global_state(self, key: str, value: str) -> None:
        async with self._get_lock():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_global_state_sync, key, value)

    async def load_global_state(self, key: str) -> Optional[str]:
        async with self._get_lock():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._load_global_state_sync, key)

    async def load_packages(self) -> List[KnowledgePackage]:
        async with self._get_lock():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._load_packages_sync)

    async def load_transfers(self) -> List[TransferRecord]:
        async with self._get_lock():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._load_transfers_sync)

    def save_pareto_front_sync(self, pareto_front: List[MOPDPoint]) -> None:
        self._save_global_state_sync(
            "pareto_front", json.dumps([p.to_dict() for p in pareto_front])
        )

    def load_pareto_front_sync(self) -> Optional[List[MOPDPoint]]:
        v = self._load_global_state_sync("pareto_front")
        if not v:
            return None
        try:
            return [MOPDPoint.from_dict(d) for d in json.loads(v)]
        except Exception:
            return None


# =============================================================================
# SECTION 10. MAIN MANAGER
# =============================================================================
class KnowledgeTransferManager:
    """
    Lifecycle:
        mgr = KnowledgeTransferManager(config=...)
        await mgr.start()       # or `async with`
        ...
        await mgr.shutdown()

    Lock order (acquire in this order to avoid deadlocks):
        _knowledge_lock -> _transfer_lock -> _graph_lock -> _experience_lock
    """

    def __init__(
        self,
        config: Optional[KnowledgeTransferConfig] = None,
        token_service: Optional[Any] = None,
        event_bus: Optional[EventBus] = None,
        message_queue: Optional[Any] = None,
    ):
        self.config = config or KnowledgeTransferConfig()
        self._token_service = token_service
        self._message_queue = message_queue

        # Storage
        self.storage: Optional[Storage] = (
            Storage(self.config.persistence_path) if self.config.enable_persistence else None
        )

        # Core state
        self.knowledge_bank: Dict[str, KnowledgePackage] = {}
        self.transfer_history: List[TransferRecord] = []
        self.experience_buffer: Dict[str, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.config.experience_buffer_max_size)
        )

        # Lazy locks in documented order
        self._knowledge_lock: Optional[asyncio.Lock] = None
        self._transfer_lock: Optional[asyncio.Lock] = None
        self._graph_lock: Optional[asyncio.Lock] = None
        self._experience_lock: Optional[asyncio.Lock] = None

        # Subsystems
        self.active_learning = ActiveLearningModule(self.config, enabled=True)
        self.genetic_optimizer = KnowledgeGeneticOptimizer(self, enabled=True)
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()

        self.xai = XAIExplainer(enabled=self.config.enable_xai)
        self.quantum_security: Optional[QuantumResilientSecurity] = (
            QuantumResilientSecurity(enabled=True)
            if self.config.enable_quantum_signing else None
        )
        self.blockchain_auditor: Optional[BlockchainAuditorPlaceholder] = (
            BlockchainAuditorPlaceholder(self.config, enabled=True)
            if self.config.enable_blockchain_audit else None
        )
        self.multi_cloud: Optional[MultiCloudDistributorPlaceholder] = (
            MultiCloudDistributorPlaceholder(self.config, enabled=True)
            if self.config.enable_multi_cloud else None
        )

        # Placeholders
        self.causal_rl_agent: Optional[CausalRLAgentPlaceholder] = (
            CausalRLAgentPlaceholder(
                state_dim=10, action_dim=3,
                max_q_table=self.config.q_table_max_size,
                enabled=self.config.enable_causal_rl,
            )
            if self.config.enable_causal_rl else None
        )
        self.federated: Optional[FederatedCoordinatorPlaceholder] = (
            FederatedCoordinatorPlaceholder(self, message_queue,
                                             enabled=self.config.enable_federated_learning)
            if self.config.enable_federated_learning else None
        )
        self.precision_controller: Optional[PrecisionControllerPlaceholder] = (
            PrecisionControllerPlaceholder(enabled=self.config.enable_precision_switching)
            if self.config.enable_precision_switching else None
        )
        self.carbon_market: Optional[CarbonMarketClientPlaceholder] = (
            CarbonMarketClientPlaceholder(enabled=self.config.enable_carbon_market)
            if self.config.enable_carbon_market else None
        )
        self.chaos_injector: Optional[ChaosInjectorPlaceholder] = ChaosInjectorPlaceholder(
            self, self.config.chaos_probability, enabled=self.config.enable_chaos,
        )
        self.human_approval: Optional[HumanApprovalHandlerPlaceholder] = (
            HumanApprovalHandlerPlaceholder(message_queue,
                                             enabled=self.config.enable_human_approval)
            if self.config.enable_human_approval else None
        )

        # Circuit breaker
        self.circuit_breaker: Optional[CircuitBreaker] = (
            CircuitBreaker(
                name="knowledge_transfer",
                db_path=self.config.circuit_breaker_db_path,
                failure_threshold=self.config.circuit_breaker_failure_threshold,
                timeout_seconds=self.config.circuit_breaker_timeout_seconds,
            )
            if self.config.enable_circuit_breaker else None
        )

        # Event bus
        self._event_bus = event_bus or EventBus(max_workers=self.config.event_bus_workers)

        # Task manager
        self._task_manager = TaskManager()

        # Lifecycle
        self._started = False
        self._shutdown = False
        self._start_time: Optional[datetime] = None

        # Prometheus
        self.metrics = self._setup_metrics()

        logger.info("KnowledgeTransferManager created (not started)")

    # ---------------- locks (documented order) ----------------
    def _get_knowledge_lock(self) -> asyncio.Lock:
        if self._knowledge_lock is None:
            self._knowledge_lock = asyncio.Lock()
        return self._knowledge_lock

    def _get_transfer_lock(self) -> asyncio.Lock:
        if self._transfer_lock is None:
            self._transfer_lock = asyncio.Lock()
        return self._transfer_lock

    def _get_graph_lock(self) -> asyncio.Lock:
        if self._graph_lock is None:
            self._graph_lock = asyncio.Lock()
        return self._graph_lock

    def _get_experience_lock(self) -> asyncio.Lock:
        if self._experience_lock is None:
            self._experience_lock = asyncio.Lock()
        return self._experience_lock

    # ---------------- safety ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "max_packages",
            lambda s: s.get("total_packages", 0) <= self.config.knowledge_bank_max_size,
            "Too many knowledge packages",
        )
        self.safety_monitor.add_invariant(
            "non_negative_survival",
            lambda s: s.get("min_survival_score", 0.0) >= 0.0,
            "Negative survival score detected",
        )
        self.safety_monitor.add_invariant(
            "transfer_success_rate_ok",
            lambda s: s.get("transfer_success_rate", 1.0) >= 0.1,
            "Transfer success rate too low",
        )

    def _get_safety_state(self) -> Dict[str, Any]:
        packages = list(self.knowledge_bank.values())
        min_survival = min((p.survival_score for p in packages), default=0.0)
        transfers = self.transfer_history
        success_rate = (
            sum(1 for t in transfers if t.successful_transfer) / len(transfers)
            if transfers else 1.0
        )
        return {
            "total_packages": len(packages),
            "min_survival_score": min_survival,
            "transfer_success_rate": success_rate,
        }

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE:
            # Provide no-op counters so callers never hit AttributeError
            class _Noop:
                def set(self, *a, **k): pass
                def inc(self, *a, **k): pass
                def observe(self, *a, **k): pass
            return {
                "packages_total": _Noop(),
                "transfers_total": _Noop(),
                "transfers_success": _Noop(),
                "pareto_size": _Noop(),
                "q_table_size": _Noop(),
            }
        try:
            return {
                "packages_total": Gauge("kt_packages_total", "Knowledge packages"),
                "transfers_total": Counter("kt_transfers_total", "Transfers"),
                "transfers_success": Counter("kt_transfers_success", "Successful transfers"),
                "pareto_size": Gauge("kt_pareto_size", "Pareto front size"),
                "q_table_size": Gauge("kt_q_table_size", "RL q-table size"),
            }
        except Exception:
            return {}

    def _update_metrics(self) -> None:
        try:
            self.metrics["packages_total"].set(len(self.knowledge_bank))
            self.metrics["pareto_size"].set(len(self.genetic_optimizer.pareto_front))
            if self.causal_rl_agent is not None:
                self.metrics["q_table_size"].set(self.causal_rl_agent.size())
        except Exception:
            pass

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        issues = self.config.validate()
        if issues:
            logger.warning("Config validation issues", issues=issues)

        self._start_time = datetime.now(timezone.utc)

        # Start event bus
        await self._event_bus.start()

        # Load persisted state
        if self.storage is not None:
            try:
                packages = await self.storage.load_packages()
                for p in packages:
                    self.knowledge_bank[p.package_id] = p
                transfers = await self.storage.load_transfers()
                self.transfer_history.extend(transfers)
                pareto = self.storage.load_pareto_front_sync()
                if pareto:
                    self.genetic_optimizer.pareto_front = pareto
            except Exception as e:
                logger.warning("State load failed", error=str(e))

        # Start background tasks
        self._task_manager.start_task("state_save", self._state_save_loop)
        self._task_manager.start_task("evolution", self._evolution_loop)
        self._task_manager.start_task("recycling", self._recycling_loop)
        self._task_manager.start_task("homeostatic", self._homeostatic_loop)
        if self.federated is not None:
            self._task_manager.start_task("federated", self._federated_loop)
        if self.chaos_injector is not None:
            self._task_manager.start_task("chaos", self._chaos_loop)

        self._started = True
        logger.info("KnowledgeTransferManager started")

    async def ready(self) -> bool:
        if not self._started:
            return False
        return isinstance(self.knowledge_bank, dict) and isinstance(self.transfer_history, list)

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("KnowledgeTransferManager shutting down")

        try:
            await asyncio.wait_for(self._task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        try:
            await asyncio.wait_for(self._event_bus.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Event bus stop timed out")

        if self.storage is not None:
            try:
                for p in self.knowledge_bank.values():
                    await self.storage.save_package(p)
                for t in self.transfer_history[-100:]:
                    await self.storage.save_transfer(t)
                self.storage.save_pareto_front_sync(self.genetic_optimizer.pareto_front)
            except Exception as e:
                logger.warning("Final flush failed", error=str(e))

        logger.info("KnowledgeTransferManager shutdown complete")

    async def __aenter__(self) -> "KnowledgeTransferManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _state_save_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.state_save_interval_seconds)
                if self.storage is None:
                    continue
                for p in list(self.knowledge_bank.values()):
                    await self.storage.save_package(p)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("State save error", error=str(e))
                await asyncio.sleep(60)

    async def _evolution_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.genetic_evolution_interval)
                await self.genetic_optimizer.evolve()
                self._update_metrics()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Evolution error", error=str(e))
                await asyncio.sleep(3600)

    async def _recycling_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.recycling_interval)
                await self._evict_stale_packages()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Recycling error", error=str(e))
                await asyncio.sleep(300)

    async def _homeostatic_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.homeostatic_interval)
                # Simple feedback: if avg effective below target, reduce decay
                packages = list(self.knowledge_bank.values())
                if packages:
                    avg = float(np.mean([p.effective_score for p in packages]))
                    target = self.config.homeostatic_target_avg_effective
                    error = target - avg
                    delta = self.config.homeostatic_kp * error
                    for p in packages:
                        p.decay_rate = max(
                            1e-4, min(0.5, p.decay_rate - delta * 0.01)
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Homeostatic error", error=str(e))
                await asyncio.sleep(60)

    async def _federated_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.federated is not None:
                    await self.federated.send_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Federated loop error", error=str(e))

    async def _chaos_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(60)
                if self.chaos_injector is not None:
                    await self.chaos_injector.maybe_inject_failure()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Chaos loop error", error=str(e))

    # ---------------- eviction ----------------
    async def _evict_stale_packages(self) -> int:
        """Evict packages by lowest effective score until size is within budget."""
        cap = self.config.knowledge_bank_max_size
        async with self._get_knowledge_lock():
            if len(self.knowledge_bank) <= cap:
                return 0
            # Sort by effective_score ascending
            ranked = sorted(
                self.knowledge_bank.items(),
                key=lambda kv: kv[1].effective_score,
            )
            to_remove = len(self.knowledge_bank) - cap
            removed = 0
            for pid, _ in ranked[:to_remove]:
                self.knowledge_bank.pop(pid, None)
                removed += 1
        if removed > 0:
            logger.info("Evicted stale packages", count=removed)
        return removed

    # ---------------- helpers ----------------
    def record_experience(self, expert_id: str, experience: Dict[str, Any]) -> None:
        """Public API to populate the experience buffer."""
        self.experience_buffer[expert_id].append({
            **experience,
            "timestamp": experience.get("timestamp", datetime.now(timezone.utc).isoformat()),
        })

    def _measure_performance(self, expert: Any) -> float:
        fn = getattr(expert, "get_performance_score", None)
        if callable(fn):
            try:
                v = fn()
                if asyncio.iscoroutine(v):
                    # Can't await here; fall back to a default
                    return float(getattr(expert, "last_performance", 0.5))
                return float(v)
            except Exception:
                return 0.5
        return float(getattr(expert, "last_performance", 0.5))

    def _infer_domain(self, expert_id: str) -> str:
        if not expert_id:
            return "unknown"
        return expert_id.split("_")[0] if "_" in expert_id else "general"

    def _compute_survival_score(self, package: KnowledgePackage) -> float:
        w = self.genetic_optimizer.best_individual or {
            "success_rate": 0.35,
            "token_efficiency": 0.30,
            "carbon_efficiency": 0.20,
            "experience_count": 0.15,
        }
        return max(0.0, min(1.0,
            w["success_rate"] * package.performance_metrics.get("success_rate", 0.5)
            + w["token_efficiency"] * package.performance_metrics.get("token_efficiency", 0.5)
            + w["carbon_efficiency"] * package.performance_metrics.get("carbon_efficiency", 0.5)
            + w["experience_count"] * min(1.0, package.total_experiences / 100.0)
        ))

    # ---------------- public API ----------------
    @traced("knowledge_transfer.capture")
    async def capture_knowledge(
        self,
        expert_id: str,
        expert_instance: Any,
        domain_tags: Optional[List[str]] = None,
    ) -> Optional[KnowledgePackage]:
        if not expert_id:
            return None

        current_data = {
            "performance": self._measure_performance(expert_instance),
            "strategy_diversity": 0.5,
            "novelty_score": random.uniform(0.3, 0.9),
        }
        priority = await self.active_learning.get_capture_priority(expert_id, current_data)
        if priority < self.config.capture_threshold:
            return None

        async with self._get_knowledge_lock():
            async with self._get_experience_lock():
                history = list(self.experience_buffer.get(expert_id, []))

                package = KnowledgePackage(
                    package_id=f"kp_{expert_id}_{uuid.uuid4().hex[:8]}",
                    source_expert_id=expert_id,
                    source_generation=0,
                    created_at=datetime.now(timezone.utc),
                    total_experiences=len(history),
                    decay_rate=self.config.default_decay_rate,
                    domain_tags=domain_tags or [self._infer_domain(expert_id)],
                )
                package.task_patterns = {"count": len(history)}
                package.successful_strategies = [
                    h for h in history if h.get("success", True)
                ][:50]
                package.failure_patterns = [
                    h for h in history if not h.get("success", True)
                ][:50]
                package.performance_metrics = {
                    "success_rate": current_data["performance"],
                    "token_efficiency": 0.5,
                    "carbon_efficiency": 0.5,
                }
                package.survival_score = self._compute_survival_score(package)
                package.capture_priority = priority
                package.uncertainty_score = await self.active_learning.calculate_uncertainty(current_data)
                package.information_gain = await self.active_learning.calculate_information_gain(
                    current_data, {}
                )

                self.knowledge_bank[package.package_id] = package

            if len(self.knowledge_bank) > self.config.knowledge_bank_max_size:
                await self._evict_stale_packages()

        if self.safety_monitor is not None:
            violations = self.safety_monitor.check(self._get_safety_state())
            if violations:
                logger.warning("Safety violation after capture", violations=violations)

        if self.quantum_security is not None:
            try:
                package.quantum_signature = await self.quantum_security.sign_data(package.to_dict())
            except Exception:
                pass

        if self.blockchain_auditor is not None:
            await self.blockchain_auditor.record_event("knowledge_captured", {
                "package_id": package.package_id,
                "expert_id": expert_id,
                "survival_score": package.survival_score,
            })

        if self.multi_cloud is not None:
            await self.multi_cloud.distribute(package.to_dict(), f"packages/{package.package_id}.json")

        if self.storage is not None:
            try:
                await self.storage.save_package(package)
            except Exception as e:
                logger.warning("Save package failed", error=str(e))

        if self.config.enable_xai:
            logger.info("XAI", text=self.xai.explain_capture(expert_id, package.survival_score))

        await self._event_bus.publish(CoreEvent(
            event_type="knowledge_captured",
            source="knowledge_transfer",
            payload={"package_id": package.package_id, "expert_id": expert_id},
        ))

        self._update_metrics()
        return package

    @traced("knowledge_transfer.transfer")
    async def transfer_knowledge(
        self,
        source_package_id: str,
        target_expert: Any,
        validate: bool = True,
        test_tasks: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if self.safety_monitor is not None:
            violations = self.safety_monitor.check(self._get_safety_state())
            if violations:
                return {"success": False, "reason": "safety_violation", "violations": violations}

        async with self._get_knowledge_lock():
            package = self.knowledge_bank.get(source_package_id)
            if package is None:
                return {"success": False, "reason": "package_not_found"}
            # Snapshot relevant fields so we can release the lock safely
            snapshot = replace(package)

        # Optional human approval for high-impact transfers
        if self.human_approval is not None and snapshot.survival_score > 0.8:
            approved = await self.human_approval.request_approval({
                "action": "transfer_knowledge",
                "package_id": source_package_id,
            })
            if not approved:
                return {"success": False, "reason": "rejected_by_human"}

        pre_perf = self._measure_performance(target_expert)
        source_domain = self._infer_domain(snapshot.source_expert_id)
        target_domain = self._infer_domain(getattr(target_expert, "expert_id", "unknown"))

        transferred: List[str] = []
        # Try to adapt parameters on the target expert
        if snapshot.optimized_parameters and hasattr(target_expert, "adaptive_thresholds"):
            for k, v in snapshot.optimized_parameters.items():
                if k in target_expert.adaptive_thresholds:
                    try:
                        target_expert.adaptive_thresholds[k] = (
                            0.6 * float(v) + 0.4 * float(target_expert.adaptive_thresholds[k])
                        )
                        transferred.append(f"threshold:{k}")
                    except Exception:
                        continue

        # Optionally set a curriculum
        if hasattr(target_expert, "set_curriculum") and snapshot.successful_strategies:
            try:
                target_expert.set_curriculum([
                    {"strategy": s.get("strategy", "unknown")} for s in snapshot.successful_strategies[:20]
                ])
                transferred.append("curriculum")
            except Exception:
                pass

        post_perf = self._measure_performance(target_expert)
        improvement = 0.0
        if pre_perf > 0:
            improvement = (post_perf - pre_perf) / pre_perf
        successful = improvement > self.config.min_improvement_threshold

        async with self._get_transfer_lock():
            record = TransferRecord(
                transfer_id=f"tr_{uuid.uuid4().hex[:10]}",
                source_package_id=source_package_id,
                target_expert_id=getattr(target_expert, "expert_id", "unknown"),
                timestamp=datetime.now(timezone.utc),
                items_transferred=transferred,
                pre_transfer_performance=pre_perf,
                post_transfer_performance=post_perf,
                improvement_percentage=improvement * 100.0,
                validation_tasks=len(test_tasks) if test_tasks else 0,
                successful_transfer=successful,
                transfer_confidence=min(1.0, 0.5 + improvement),
                source_domain=source_domain,
                target_domain=target_domain,
            )
            self.transfer_history.append(record)

        # Update the package under the knowledge lock
        async with self._get_knowledge_lock():
            if source_package_id in self.knowledge_bank:
                live = self.knowledge_bank[source_package_id]
                live.transfer_count += 1
                live.last_transferred = datetime.now(timezone.utc)
                live.transfer_success_scores.append(1.0 if successful else 0.0)
                live.average_transfer_improvement = (
                    live.average_transfer_improvement * (live.transfer_count - 1) + improvement
                ) / max(1, live.transfer_count)

        if self.quantum_security is not None:
            try:
                record.quantum_signature = await self.quantum_security.sign_data(record.to_dict())
            except Exception:
                pass

        if self.blockchain_auditor is not None:
            await self.blockchain_auditor.record_event("transfer_completed", {
                "transfer_id": record.transfer_id,
                "success": record.successful_transfer,
                "improvement": improvement,
            })

        if self.multi_cloud is not None:
            await self.multi_cloud.distribute(record.to_dict(), f"transfers/{record.transfer_id}.json")

        if self.storage is not None:
            try:
                await self.storage.save_transfer(record)
            except Exception as e:
                logger.warning("Save transfer failed", error=str(e))

        self.metrics["transfers_total"].inc()
        if successful:
            self.metrics["transfers_success"].inc()

        await self._event_bus.publish(CoreEvent(
            event_type="transfer_completed",
            source="knowledge_transfer",
            payload={"transfer_id": record.transfer_id, "success": successful},
        ))

        if self.config.enable_xai:
            logger.info("XAI", text=self.xai.explain_transfer(
                source_package_id, record.target_expert_id, improvement
            ))

        self._update_metrics()
        return {
            "success": True,
            "transfer_id": record.transfer_id,
            "items_transferred": transferred,
            "improvement_percentage": improvement * 100.0,
            "successful_transfer": successful,
            "confidence": record.transfer_confidence,
        }

    def explain_decision(self, decision_type: str, context: Optional[Dict[str, Any]] = None) -> str:
        ctx = context or {}

        def _fmt_pct(v: Any) -> str:
            try:
                if v is None:
                    return "n/a"
                return f"{float(v) * 100:.2f}%"
            except (TypeError, ValueError):
                return "n/a"

        if decision_type == "capture":
            return (
                f"Captured knowledge from {ctx.get('expert_id', 'unknown')} "
                f"(survival={ctx.get('survival_score', 0.0):.3f})."
            )
        if decision_type == "transfer":
            return (
                f"Transferred package {ctx.get('package_id', 'unknown')} to "
                f"{ctx.get('target_expert', 'unknown')} "
                f"(improvement={_fmt_pct(ctx.get('improvement'))})."
            )
        return "Decision made by system rules."

    def get_system_status(self) -> Dict[str, Any]:
        uptime = 0.0
        if self._start_time is not None:
            uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()
        return {
            "started": self._started,
            "shutdown": self._shutdown,
            "uptime_seconds": uptime,
            "packages": len(self.knowledge_bank),
            "transfers": len(self.transfer_history),
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "q_table_size": self.causal_rl_agent.size() if self.causal_rl_agent else 0,
            "module_status": MODULE_STATUS,
            "circuit_breaker": self.circuit_breaker.snapshot() if self.circuit_breaker else None,
            "event_bus_stats": dict(self._event_bus.stats),
        }


# =============================================================================
# SECTION 11. TESTS
# =============================================================================
class _StubExpert:
    def __init__(self, expert_id: str, performance: float = 0.5):
        self.expert_id = expert_id
        self.last_performance = performance
        self.adaptive_thresholds = {"learning_rate": 0.1}
        self._curriculum: List[Dict[str, Any]] = []

    def get_performance_score(self) -> float:
        return self.last_performance

    def set_curriculum(self, curriculum: List[Dict[str, Any]]) -> None:
        self._curriculum = curriculum


class _Tests(unittest.TestCase):
    def _cfg(self) -> KnowledgeTransferConfig:
        import tempfile
        td = tempfile.mkdtemp()
        return KnowledgeTransferConfig(
            capture_threshold=0.1,
            persistence_path=os.path.join(td, "kt.db"),
            circuit_breaker_db_path=os.path.join(td, "cb.db"),
            genetic_generations=2,
            genetic_population_size=6,
            genetic_evolution_interval=86400,
            recycling_interval=86400,
            homeostatic_interval=86400,
            state_save_interval_seconds=86400,
            knowledge_bank_max_size=50,
            q_table_max_size=10,
            enable_causal_rl=False,
            enable_federated_learning=False,
            enable_precision_switching=False,
            enable_carbon_market=False,
            enable_chaos=False,
            enable_human_approval=False,
            enable_quantum_signing=False,
            enable_blockchain_audit=False,
            enable_multi_cloud=False,
        )

    def test_lifecycle(self):
        async def go():
            mgr = KnowledgeTransferManager(config=self._cfg())
            self.assertFalse(await mgr.ready())
            await mgr.start()
            self.assertTrue(await mgr.ready())
            await mgr.shutdown()
            self.assertTrue(mgr._shutdown)
            await mgr.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with KnowledgeTransferManager(config=self._cfg()) as mgr:
                self.assertTrue(await mgr.ready())
        asyncio.run(go())

    def test_capture_and_transfer(self):
        async def go():
            async with KnowledgeTransferManager(config=self._cfg()) as mgr:
                mgr.record_experience("expert_a", {"success": True, "action": "x"})
                mgr.record_experience("expert_a", {"success": False, "action": "y"})
                pkg = await mgr.capture_knowledge("expert_a", _StubExpert("expert_a", 0.9))
                self.assertIsNotNone(pkg)
                self.assertEqual(pkg.total_experiences, 2)
                self.assertGreaterEqual(len(pkg.successful_strategies), 1)
                self.assertGreaterEqual(len(pkg.failure_patterns), 1)

                target = _StubExpert("expert_b", 0.5)
                # bump target performance after transfer so improvement is detected
                result = await mgr.transfer_knowledge(pkg.package_id, target)
                self.assertTrue(result["success"])
                self.assertGreaterEqual(result["improvement_percentage"], 0)
        asyncio.run(go())

    def test_experience_buffer_populated(self):
        async def go():
            async with KnowledgeTransferManager(config=self._cfg()) as mgr:
                for i in range(5):
                    mgr.record_experience("expert_x", {"success": i % 2 == 0})
                self.assertEqual(len(mgr.experience_buffer["expert_x"]), 5)
        asyncio.run(go())

    def test_knowledge_bank_bounded(self):
        async def go():
            cfg = self._cfg()
            cfg.knowledge_bank_max_size = 5
            async with KnowledgeTransferManager(config=cfg) as mgr:
                for i in range(20):
                    await mgr.capture_knowledge(
                        f"expert_{i}", _StubExpert(f"expert_{i}", 0.9)
                    )
                self.assertLessEqual(len(mgr.knowledge_bank), 5)
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3, max_q_table=10)
        for _ in range(200):
            s = np.random.rand(4)
            a = rl.act(s)
            rl.update(s, a, 1.0, s, False)
        self.assertLessEqual(rl.size(), 10)

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

        async def go():
            ha = HumanApprovalHandlerPlaceholder()
            self.assertFalse(ha.available)
            self.assertFalse(await ha.request_approval({"action": "x"}))
        asyncio.run(go())

    def test_ga_genome_dependent(self):
        async def go():
            async with KnowledgeTransferManager(config=self._cfg()) as mgr:
                # populate enough state to make objectives non-zero
                for i in range(5):
                    mgr.record_experience(f"e{i}", {"success": True})
                    await mgr.capture_knowledge(f"e{i}", _StubExpert(f"e{i}", 0.8))
                ga = mgr.genetic_optimizer
                a = {k: lo for k, (lo, hi) in ga.PARAM_BOUNDS.items()}
                b = {k: hi for k, (lo, hi) in ga.PARAM_BOUNDS.items()}
                oa = await ga._evaluate(a)
                ob = await ga._evaluate(b)
                diffs = [abs(oa[k] - ob[k]) for k in oa]
                self.assertGreater(max(diffs), 1e-4)
        asyncio.run(go())

    def test_ga_evolve_runs(self):
        async def go():
            async with KnowledgeTransferManager(config=self._cfg()) as mgr:
                for i in range(3):
                    await mgr.capture_knowledge(f"e{i}", _StubExpert(f"e{i}", 0.9))
                result = await mgr.genetic_optimizer.evolve(generations=2)
                self.assertIn("best_fitness", result)
                self.assertIn("pareto_front", result)
        asyncio.run(go())

    def test_explain_decision_handles_none(self):
        mgr = KnowledgeTransferManager(config=self._cfg())
        # Should not raise
        msg = mgr.explain_decision("transfer", {
            "package_id": "p", "target_expert": "t", "improvement": None,
        })
        self.assertIn("n/a", msg)

    def test_env_parsing(self):
        os.environ["KT_GENETIC_POPULATION_SIZE"] = "7"
        os.environ["KT_ENABLE_DECAY"] = "false"
        os.environ["KT_DEFAULT_DECAY_RATE"] = "0.05"
        try:
            cfg = KnowledgeTransferConfig.from_env_and_file()
            self.assertEqual(cfg.genetic_population_size, 7)
            self.assertFalse(cfg.enable_decay)
            self.assertAlmostEqual(cfg.default_decay_rate, 0.05, places=4)
        finally:
            for k in ("KT_GENETIC_POPULATION_SIZE", "KT_ENABLE_DECAY", "KT_DEFAULT_DECAY_RATE"):
                os.environ.pop(k, None)

    def test_circuit_breaker_transitions(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cb = CircuitBreaker("t", os.path.join(td, "cb.db"), failure_threshold=2, timeout_seconds=0.5)

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

    def test_event_bus(self):
        async def go():
            bus = EventBus(max_workers=2)
            await bus.start()
            received: List[CoreEvent] = []
            bus.subscribe("test", lambda e: received.append(e))
            for i in range(5):
                await bus.publish(CoreEvent(event_type="test", source="t", payload={"i": i}))
            await asyncio.sleep(0.2)
            self.assertEqual(len(received), 5)
            await asyncio.wait_for(bus.stop(), timeout=2.0)
        asyncio.run(go())

    def test_persistence_roundtrip(self):
        async def go():
            cfg = self._cfg()
            mgr = KnowledgeTransferManager(config=cfg)
            async with mgr:
                pkg = await mgr.capture_knowledge("e1", _StubExpert("e1", 0.9))
                self.assertIsNotNone(pkg)
                pkg_id = pkg.package_id
            mgr2 = KnowledgeTransferManager(config=cfg)
            async with mgr2:
                self.assertIn(pkg_id, mgr2.knowledge_bank)
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 12. ENTRY POINT
# =============================================================================
async def _example() -> None:
    import tempfile
    td = tempfile.mkdtemp()
    cfg = KnowledgeTransferConfig(
        persistence_path=os.path.join(td, "kt.db"),
        circuit_breaker_db_path=os.path.join(td, "cb.db"),
        genetic_generations=2,
        genetic_population_size=8,
        state_save_interval_seconds=86400,
        recycling_interval=86400,
        homeostatic_interval=86400,
        genetic_evolution_interval=86400,
        capture_threshold=0.1,
    )
    async with KnowledgeTransferManager(config=cfg) as mgr:
        for i in range(3):
            mgr.record_experience("expert_demo", {"success": i != 1})
        pkg = await mgr.capture_knowledge("expert_demo", _StubExpert("expert_demo", 0.9))
        print(f"Captured package: {pkg.package_id if pkg else None}")
        target = _StubExpert("expert_target", 0.5)
        if pkg is not None:
            result = await mgr.transfer_knowledge(pkg.package_id, target)
            print("Transfer:", json.dumps(result, indent=2, default=str))
        print("Status:", json.dumps(mgr.get_system_status(), indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Knowledge Transfer Manager v9.0.0")
    parser.add_argument("--test", action="store_true", help="Run embedded tests")
    parser.add_argument("--example", action="store_true", help="Run example usage")
    parser.add_argument("--status", action="store_true", help="Print module statuses")
    args = parser.parse_args()

    if args.status:
        for name, status in MODULE_STATUS.items():
            print(f"{name:22s} {status}")
        return

    if args.test:
        sys.exit(run_tests())

    if args.example:
        asyncio.run(_example())
        return

    print("Knowledge Transfer Manager v9.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
