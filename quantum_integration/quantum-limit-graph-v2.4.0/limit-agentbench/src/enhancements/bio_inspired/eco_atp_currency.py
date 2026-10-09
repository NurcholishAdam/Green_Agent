#!/usr/bin/env python3
# =============================================================================
# Enhanced Eco-ATP Currency System v11.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced Eco-ATP Currency System v11.0.0
========================================
Patched single-file version. Focus on correctness, honesty, lifecycle.

P0 fixes
--------
- `import yaml` added; safe fallback if not installed.
- No `asyncio.run` inside async contexts. `_get_safety_state` is async and
  computes the snapshot from in-memory fields.
- `AsyncPersistenceManager.initialize()` awaited from `start()`.
- Lifecycle moved to `await manager.start()`; no tasks or state load in
  `__init__`.
- Reentrant-lock deadlock fixed in GA (evolve → _evaluate_individual).
- NSGA-II primitives (non-dominated sort, crowding, tournament, crossover,
  mutate) fully implemented.
- All referenced loops and persistence methods implemented.
- QuantumResilientSecurity ECDSA path returns and consumes public_key.
- Lazy asyncio locks.

P1 — real behavior
------------------
- Bounded, discretized Q-table for the CausalRL placeholder.
- SQLite on the event loop moved to thread executor in the circuit breaker
  and to aiosqlite in the strategy selector.
- `market` account mutations guarded by the manager's account lock.
- ML predictor features are real signals; reads guarded by lock.
- Token decay accepts a half-life parameter.
- Cloud and blockchain I/O offloaded to threads.

P2 — honesty
------------
- MODULE_STATUS documents each module.
- Placeholders: CausalRL, Federated, Precision, CarbonMarket, Chaos,
  HumanApproval — safe no-ops, `.available=False`, disabled by default.
- Experimental: Persistence, GeneticOptimizer, MOPD, SafetyMonitor, XAI,
  QuantumSecurity, BlockchainAudit, MultiCloud, MLPredictor.

P3 — production readiness
-------------------------
- Lifecycle: `await start()`, `await shutdown()`, `await ready()`,
  `__aenter__` / `__aexit__`.
- Graceful shutdown: drain tasks, flush persistence, idempotent.
- Logical sections in a single file.
- Embedded test suite: `python3 eco_atp_currency.py --test`.
- Prometheus metrics and OpenTelemetry spans (both optional).
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import hmac
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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple, Union

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
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore
    AIOSQLITE_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("eco_atp_currency")
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
    "token_core":         "stable",
    "circuit_breaker":    "stable",
    "task_manager":       "stable",
    "persistence":        "experimental",
    "genetic_optimizer":  "experimental",
    "mopd":               "experimental",
    "safety_monitor":     "experimental",
    "xai":                "experimental",
    "quantum_security":   "experimental",
    "blockchain_audit":   "experimental",
    "multi_cloud":        "experimental",
    "ml_predictor":       "experimental",
    "causal_rl":          "placeholder",
    "federated":          "placeholder",
    "precision":          "placeholder",
    "carbon_market":      "placeholder",
    "chaos":              "placeholder",
    "human_approval":     "placeholder",
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
class EcoATPSource(Enum):
    RENEWABLE_ENERGY = "renewable_energy"
    CARBON_OFFSET = "carbon_offset"
    EFFICIENCY_GAIN = "efficiency_gain"
    WASTE_HEAT_RECOVERY = "waste_heat_recovery"
    COMPUTATION_SCAVENGING = "computation_scavenging"
    HELIUM_RECOVERY = "helium_recovery"
    EXTERNAL_TRADE = "external_trade"
    GRADIENT_CONVERSION = "gradient_conversion"
    EMERGENCY_SUBSTRATE = "emergency_substrate"
    QUANTUM_ADVANTAGE = "quantum_advantage"


class EcoATPConsumer(Enum):
    EXPERT_EXECUTION = "expert_execution"
    MODEL_TRAINING = "model_training"
    DATA_PROCESSING = "data_processing"
    QUANTUM_COMPUTING = "quantum_computing"
    NETWORK_TRANSFER = "network_transfer"
    COOLING_SYSTEM = "cooling_system"
    STORAGE_OPERATION = "storage_operation"
    MAINTENANCE = "maintenance"


class TokenState(Enum):
    GENERATED = "generated"
    AVAILABLE = "available"
    RESERVED = "reserved"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    RECOVERED = "recovered"
    TRADED = "traded"


# =============================================================================
# SECTION 4. CONFIGURATION
# =============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "efficiency": 0.4,
        "inflation": 0.3,
        "emergency": 0.3,
    })
    grid_resolution: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDConfig":
        data = dict(data or {})
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class EcoATPConfig:
    # Token parameters
    token_expiry_hours: float = 24.0
    token_half_life_hours: float = 24.0
    carbon_to_ecoatp_factor: float = 10.0
    helium_to_ecoatp_factor: float = 5.0
    energy_to_ecoatp_factor: float = 1000.0

    # Thresholds
    hoarding_threshold: float = 2.0
    tax_rate: float = 0.1
    emergency_threshold: float = 50.0
    rate_limit_multiplier_high: float = 0.5
    rate_limit_multiplier_low: float = 1.5

    # Redistribution
    redistribution_interval_minutes: int = 30

    # Emergency
    emergency_token_rate: float = 10.0
    emergency_reserve: float = 1000.0
    substrate_reserves_max: float = 1000.0
    substrate_reserves_min: float = 500.0

    # Tenant defaults
    default_max_tokens_per_minute: float = 100.0
    default_max_concurrent_tasks: int = 5
    default_min_priority_for_reservation: int = 2
    default_reservation_cooldown_seconds: float = 1.0

    # Suspicious detection
    suspicious_threshold: int = 5
    batch_size: int = 10

    # ML
    ml_retrain_interval_seconds: int = 60
    ml_history_size: int = 1000

    # Market
    market_matching_interval_seconds: int = 30
    market_order_expiry_minutes: int = 5

    # Genetic optimizer
    enable_genetic_optimizer: bool = True
    genetic_population_size: int = 20
    genetic_mutation_rate: float = 0.2
    genetic_crossover_rate: float = 0.7
    genetic_generations: int = 5
    genetic_tournament_size: int = 3
    genetic_evolution_interval_seconds: int = 86400

    # Recovery rates
    recovery_rates: Dict[float, float] = field(default_factory=lambda: {
        0.0: 0.0, 0.25: 0.125, 0.5: 0.25, 0.75: 0.6, 0.9: 0.8, 1.0: 0.95,
    })

    # Persistence
    enable_persistence: bool = True
    persistence_path: str = "eco_atp_state.db"

    # Retry
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0

    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 60.0
    circuit_breaker_db_path: str = "circuit_breakers.db"

    # Quantum signing
    enable_quantum_signing: bool = True
    quantum_signing_algorithm: str = "dilithium"

    # Blockchain audit
    enable_blockchain_audit: bool = False
    blockchain_rpc_url: str = "http://localhost:8545"
    blockchain_contract_address: str = "0x0"
    blockchain_private_key: Optional[str] = None

    # Autonomous strategy
    enable_autonomous_strategy: bool = True
    rl_learning_rate: float = 0.1
    rl_discount_factor: float = 0.9
    rl_exploration_rate: float = 0.1
    rl_q_table_db_path: str = "rl_q_table.db"
    q_table_max_size: int = 5000

    # Multi-cloud
    enable_multi_cloud: bool = False
    cloud_provider: str = "aws"
    cloud_region: str = "us-east-1"
    cloud_bucket: str = "eco-atp-state"
    cloud_access_key: Optional[str] = None
    cloud_secret_key: Optional[str] = None

    # Prometheus
    prometheus_port: Optional[int] = None

    # Health check
    enable_health_endpoint: bool = True
    health_endpoint_port: int = 8080

    # Model persistence paths
    ml_model_path: str = "models/ml_model.joblib"
    genetic_state_path: str = "models/genetic_state.json"

    # MOPD
    mopd: MOPDConfig = field(default_factory=MOPDConfig)

    # Shutdown
    shutdown_timeout_seconds: int = 15
    state_save_interval_seconds: float = 60.0

    # Experimental / placeholder toggles — disabled by default
    enable_causal_rl: bool = False
    enable_federated_learning: bool = False
    enable_safety_monitor: bool = True
    enable_xai: bool = True
    enable_precision_switching: bool = False
    enable_carbon_market: bool = False
    carbon_market_config: Optional[Dict[str, str]] = None
    enable_chaos: bool = False
    chaos_probability: float = 0.0
    enable_human_approval: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EcoATPConfig":
        data = dict(data or {})
        mopd_data = data.pop("mopd", None)
        fields = cls.__dataclass_fields__
        cfg = cls(**{k: v for k, v in data.items() if k in fields})
        if isinstance(mopd_data, dict):
            cfg.mopd = MOPDConfig.from_dict(mopd_data)
        return cfg

    @classmethod
    def from_env_and_file(cls, config_path: Optional[str] = None) -> "EcoATPConfig":
        env_overrides: Dict[str, Any] = {}
        for key in cls.__dataclass_fields__:
            env_var = f"ECOATP_{key.upper()}"
            if env_var in os.environ:
                env_overrides[key] = os.environ[env_var]
        if config_path and os.path.exists(config_path) and YAML_AVAILABLE:
            with open(config_path, "r") as f:
                yaml_data = yaml.safe_load(f) or {}
            yaml_data.update(env_overrides)
            return cls.from_dict(yaml_data)
        return cls(**env_overrides) if env_overrides else cls()


# =============================================================================
# SECTION 5. DATA CLASSES
# =============================================================================
@dataclass
class EcoATPToken:
    token_id: str
    value: float
    source: EcoATPSource
    generated_at: datetime
    expires_at: datetime
    state: TokenState = TokenState.AVAILABLE
    carbon_equivalent_kg: float = 0.0
    helium_equivalent_units: float = 0.0
    generation_efficiency: float = 1.0
    provenance_hash: str = ""
    quantum_signature: Optional[Dict[str, Any]] = None
    consumed_at: Optional[datetime] = None
    recovered_at: Optional[datetime] = None

    def __post_init__(self):
        if not self.provenance_hash:
            self.provenance_hash = self._compute_hash()
        if self.generated_at.tzinfo is None:
            self.generated_at = self.generated_at.replace(tzinfo=timezone.utc)
        if self.expires_at.tzinfo is None:
            self.expires_at = self.expires_at.replace(tzinfo=timezone.utc)

    def _compute_hash(self) -> str:
        data = f"{self.token_id}{self.value}{self.source.value}{self.generated_at.isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()

    def apply_decay(self, current_time: datetime, half_life_hours: float = 24.0) -> float:
        age_hours = (current_time - self.generated_at).total_seconds() / 3600.0
        half_life = max(half_life_hours, 1e-6)
        decay_factor = math.exp(-math.log(2) * age_hours / half_life)
        return self.value * decay_factor

    def is_expired(self, current_time: datetime) -> bool:
        return current_time > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token_id": self.token_id,
            "value": self.value,
            "source": self.source.value,
            "generated_at": self.generated_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "state": self.state.value,
            "carbon_equivalent_kg": self.carbon_equivalent_kg,
            "helium_equivalent_units": self.helium_equivalent_units,
            "generation_efficiency": self.generation_efficiency,
            "provenance_hash": self.provenance_hash,
            "consumed_at": self.consumed_at.isoformat() if self.consumed_at else None,
            "recovered_at": self.recovered_at.isoformat() if self.recovered_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EcoATPToken":
        data = dict(data or {})
        return cls(
            token_id=data["token_id"],
            value=float(data["value"]),
            source=EcoATPSource(data.get("source", "renewable_energy")),
            generated_at=datetime.fromisoformat(data["generated_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]),
            state=TokenState(data.get("state", "available")),
            carbon_equivalent_kg=float(data.get("carbon_equivalent_kg", 0.0)),
            helium_equivalent_units=float(data.get("helium_equivalent_units", 0.0)),
            generation_efficiency=float(data.get("generation_efficiency", 1.0)),
            provenance_hash=data.get("provenance_hash", ""),
            consumed_at=datetime.fromisoformat(data["consumed_at"]) if data.get("consumed_at") else None,
            recovered_at=datetime.fromisoformat(data["recovered_at"]) if data.get("recovered_at") else None,
        )


@dataclass
class EcoATPAccount:
    account_id: str
    balance: float = 0.0
    total_generated: float = 0.0
    total_consumed: float = 0.0
    total_recovered: float = 0.0
    total_expired: float = 0.0
    efficiency_rating: float = 1.0

    @property
    def net_balance(self) -> float:
        return self.balance

    @property
    def utilization_rate(self) -> float:
        if self.total_generated == 0:
            return 0.0
        return self.total_consumed / self.total_generated

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EcoATPAccount":
        data = dict(data or {})
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class MOPDPoint:
    individual: Dict[str, float]
    efficiency: float
    inflation: float
    emergency: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**{k: v for k, v in (data or {}).items() if k in cls.__dataclass_fields__})


@dataclass
class MarketOrder:
    order_id: str
    account_id: str
    amount: float
    price: float
    side: str
    status: str = "open"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    remaining: float = 0.0

    def __post_init__(self):
        if self.expires_at is None:
            self.expires_at = self.created_at + timedelta(minutes=5)
        if self.remaining == 0.0:
            self.remaining = self.amount


# =============================================================================
# SECTION 6. CIRCUIT BREAKER (STABLE) — lazy lock, executor I/O
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
        return {
            "name": self.name,
            "state": self._state.value,
            "failures": self._failure_count,
        }


# =============================================================================
# SECTION 7. TASK MANAGER (STABLE) — with drain
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
# SECTION 8. PLACEHOLDERS — safe no-ops, disabled by default
# =============================================================================
class CausalRLAgentPlaceholder:
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
        key = self._discretize(state)
        return int(np.argmax(self._get_or_create(key)))

    def update(self, state, action, reward, next_state, done) -> None:
        key = self._discretize(state)
        next_key = self._discretize(next_state)
        current = self._get_or_create(key)
        next_q = self._get_or_create(next_key)
        best_next = 0.0 if done else float(np.max(next_q))
        td_target = reward + self.gamma * best_next
        current[action] += self.learning_rate * (td_target - current[action])

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
# SECTION 9. EXPERIMENTAL COMPONENTS
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
        return [
            f"{name}: {desc}"
            for name, fn, desc in self.invariants
            if not fn(state)
        ]


class XAIExplainer:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("xai")
        self.available = True

    def explain_generation(self, carbon_saved_kg: float, helium_saved_units: float,
                            energy_saved_kwh: float, total: float) -> str:
        return (
            f"Generated {total:.2f} eco-ATP from carbon={carbon_saved_kg:.3f}kg, "
            f"helium={helium_saved_units:.3f}u, energy={energy_saved_kwh:.3f}kWh."
        )

    def explain_reservation(self, amount: float, consumer: str, priority: int) -> str:
        return f"Reserved {amount:.2f} tokens for {consumer} at priority {priority}."

    def explain_emergency(self, balance: float, threshold: float) -> str:
        return f"Emergency activated: balance {balance:.2f} < threshold {threshold:.2f}."


class QuantumResilientSecurity:
    """HMAC-SHA256 default. Optional ECDSA fallback. Not quantum-safe without pqcrypto."""
    STATUS = "experimental"

    def __init__(self, algorithm: str = "hmac-sha256", enabled: bool = True):
        if enabled:
            _warn_module("quantum_security")
        self.algorithm = algorithm
        self.available = True
        self._hmac_secret = os.urandom(32)
        self._ecdsa_private = None
        self._ecdsa_public_der: Optional[bytes] = None
        try:
            from cryptography.hazmat.primitives.asymmetric import ec
            self._ecdsa_private = ec.generate_private_key(ec.SECP256R1())
            from cryptography.hazmat.primitives import serialization
            self._ecdsa_public_der = self._ecdsa_private.public_key().public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        except Exception:
            pass

    @property
    def backend(self) -> str:
        return "ecdsa" if self._ecdsa_private is not None else "hmac-sha256"

    async def sign_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if self._ecdsa_private is not None:
            try:
                from cryptography.hazmat.primitives import hashes
                from cryptography.hazmat.primitives.asymmetric import ec
                signature = self._ecdsa_private.sign(payload, ec.ECDSA(hashes.SHA256()))
                return {
                    "algorithm": "ecdsa",
                    "signature": signature.hex(),
                    "public_key": self._ecdsa_public_der.hex() if self._ecdsa_public_der else "",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            except Exception:
                pass
        sig = hmac.new(self._hmac_secret, payload, hashlib.sha256).digest()
        return {
            "algorithm": "hmac-sha256",
            "signature": sig.hex(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def verify_data(self, data: Dict[str, Any], signature_data: Dict[str, Any]) -> bool:
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        algo = signature_data.get("algorithm")
        sig = bytes.fromhex(signature_data.get("signature", ""))
        if algo == "ecdsa" and self._ecdsa_public_der is not None:
            try:
                from cryptography.hazmat.primitives import hashes
                from cryptography.hazmat.primitives.asymmetric import ec
                from cryptography.hazmat.primitives.serialization import load_der_public_key
                pub = load_der_public_key(bytes.fromhex(signature_data.get("public_key", "")))
                pub.verify(sig, payload, ec.ECDSA(hashes.SHA256()))
                return True
            except Exception:
                return False
        if algo == "hmac-sha256":
            expected = hmac.new(self._hmac_secret, payload, hashlib.sha256).digest()
            return hmac.compare_digest(expected, sig)
        return False


class BlockchainAuditorPlaceholder:
    """Simulated in-memory audit log. Not a blockchain without web3."""
    STATUS = "experimental"

    def __init__(self, config: EcoATPConfig, enabled: bool = False):
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

    def __init__(self, config: EcoATPConfig, enabled: bool = False):
        if enabled:
            _warn_module("multi_cloud")
        self.config = config
        self.available = False
        self.log: Deque[Dict[str, Any]] = deque(maxlen=100)

    async def distribute(self, data: Dict[str, Any], filename: str) -> Dict[str, Any]:
        rec = {"status": "skipped", "filename": filename, "reason": "placeholder"}
        self.log.append(rec)
        return rec


class MLDemandPredictor:
    """Rolling-mean predictor. Available only if sklearn is present for the estimator."""
    STATUS = "experimental"

    def __init__(self, config: EcoATPConfig, enabled: bool = True):
        if enabled:
            _warn_module("ml_predictor")
        self.config = config
        self.history: Deque[Tuple[float, float, float, float]] = deque(maxlen=config.ml_history_size)
        self.last_trained: Optional[datetime] = None
        self.available = True
        self._lock: Optional[asyncio.Lock] = None
        self._model = None
        self._scaler = None
        try:
            from sklearn.ensemble import RandomForestRegressor
            from sklearn.preprocessing import StandardScaler
            self._model = RandomForestRegressor(n_estimators=10, random_state=42)
            self._scaler = StandardScaler()
        except Exception:
            self._model = None
            self._scaler = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def record_demand(self, account_id: str, amount: float, timestamp: datetime,
                             system_load: float = 0.5) -> None:
        # Feature set: hour, weekday, load, amount (target = amount, features drop amount)
        feats = (float(timestamp.hour), float(timestamp.weekday()), float(system_load), float(amount))
        async with self._get_lock():
            self.history.append(feats)

    async def train(self, force: bool = False) -> Dict[str, Any]:
        if self._model is None or self._scaler is None:
            return {"status": "unavailable"}
        now = datetime.now(timezone.utc)
        if not force and self.last_trained and (
            now - self.last_trained
        ).total_seconds() < self.config.ml_retrain_interval_seconds:
            return {"status": "skipped"}
        async with self._get_lock():
            data = list(self.history)
        if len(data) < 10:
            return {"status": "insufficient_data", "samples": len(data)}

        def _fit():
            X = np.array([[h, d, l] for h, d, l, _ in data])
            y = np.array([a for _, _, _, a in data])
            Xs = self._scaler.fit_transform(X)
            self._model.fit(Xs, y)

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _fit)
            self.last_trained = now
            return {"status": "success", "samples": len(data)}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def predict_demand(self, timestamp: datetime, system_load: float = 0.5) -> float:
        if self._model is None or self._scaler is None or self.last_trained is None:
            return 0.0
        try:
            X = np.array([[float(timestamp.hour), float(timestamp.weekday()), float(system_load)]])
            Xs = self._scaler.transform(X)
            return float(self._model.predict(Xs)[0])
        except Exception:
            return 0.0


# =============================================================================
# SECTION 10. GENETIC OPTIMIZER (EXPERIMENTAL) — NSGA-II implemented
# =============================================================================
class GeneticOptimizer:
    """
    Multi-objective NSGA-II over token thresholds.

    Objectives (all genome-dependent):
      - efficiency : utilization proxy derived from current metrics.
      - inflation  : 1 - |generated - consumed| / max(consumed, 1).
      - emergency  : 1 - emergency_mode.

    Genome: {hoarding_threshold, tax_rate, emergency_threshold,
             rate_limit_multiplier_high, rate_limit_multiplier_low}.
    """
    STATUS = "experimental"

    PARAM_BOUNDS = {
        "hoarding_threshold": (1.2, 4.0),
        "tax_rate": (0.05, 0.3),
        "emergency_threshold": (10.0, 100.0),
        "rate_limit_multiplier_high": (0.3, 0.7),
        "rate_limit_multiplier_low": (1.2, 2.0),
    }

    def __init__(self, manager: "EcoATPTokenManager", config: EcoATPConfig,
                 enabled: bool = True):
        if enabled:
            _warn_module("genetic_optimizer")
        self.manager = manager
        self.config = config
        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness: float = -math.inf
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._lock: Optional[asyncio.Lock] = None
        self._eval_cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _initialize_individual(self) -> Dict[str, float]:
        return {
            k: random.uniform(lo, hi) for k, (lo, hi) in self.PARAM_BOUNDS.items()
        }

    def _initialize_population(self) -> List[Dict[str, float]]:
        return [self._initialize_individual() for _ in range(self.config.genetic_population_size)]

    # ---------- genome-dependent objectives ----------
    async def _evaluate(self, individual: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted(individual.items()))
        if key in self._eval_cache:
            return self._eval_cache[key]

        # Compute metrics from current manager state without mutating config.
        metrics = self.manager._metrics_snapshot()
        total_gen = metrics["total_generated"]
        total_con = metrics["total_consumed"]
        efficiency = total_con / max(total_gen, 1.0)
        inflation_raw = (total_gen - total_con) / max(total_con, 1.0)
        inflation = 1.0 - min(abs(inflation_raw), 1.0)
        emergency = 0.0 if self.manager.emergency_mode else 1.0

        # Genome-dependent shaping: penalize extreme parameters
        hoard = individual["hoarding_threshold"]
        tax = individual["tax_rate"]
        em_thr = individual["emergency_threshold"]
        hi_mult = individual["rate_limit_multiplier_high"]
        lo_mult = individual["rate_limit_multiplier_low"]

        # Hoarding penalty: values near the middle are safer
        hoard_penalty = abs(hoard - 2.5) / 2.5
        # Tax penalty: taxes above 0.2 reduce efficiency slightly
        tax_penalty = max(0.0, (tax - 0.2)) * 0.5
        # Emergency threshold penalty: too low -> premature emergency
        em_penalty = max(0.0, (30.0 - em_thr)) / 30.0
        # Rate-limit balance penalty: hi + lo should be around 2.0
        balance_penalty = abs((hi_mult + lo_mult) - 2.0) / 2.0

        efficiency_g = max(0.0, min(1.0, efficiency - hoard_penalty * 0.05 - tax_penalty))
        inflation_g = max(0.0, min(1.0, inflation - em_penalty * 0.1))
        emergency_g = max(0.0, min(1.0, emergency - balance_penalty * 0.1))

        objs = {"efficiency": efficiency_g, "inflation": inflation_g, "emergency": emergency_g}
        self._eval_cache[key] = objs
        return objs

    def _scalarise(self, objs: Dict[str, float],
                   weights: Optional[Dict[str, float]] = None) -> float:
        w = weights or self.config.mopd.objective_weights
        return (
            w.get("efficiency", 0.4) * objs["efficiency"]
            + w.get("inflation", 0.3) * objs["inflation"]
            + w.get("emergency", 0.3) * objs["emergency"]
        )

    # ---------- NSGA-II primitives ----------
    def _dominates(self, a: Dict[str, float], b: Dict[str, float]) -> bool:
        keys = ("efficiency", "inflation", "emergency")
        return all(a[k] >= b[k] for k in keys) and any(a[k] > b[k] for k in keys)

    def _fast_non_dominated_sort(
        self,
        objectives: List[Dict[str, float]],
    ) -> List[List[int]]:
        n = len(objectives)
        dominates = [[] for _ in range(n)]
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

    def _crowding_distance(
        self,
        front: List[int],
        objectives: List[Dict[str, float]],
    ) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}

        distances: Dict[int, float] = {i: 0.0 for i in front}
        keys = ("efficiency", "inflation", "emergency")
        for k in keys:
            sorted_front = sorted(front, key=lambda i: objectives[i][k])
            distances[sorted_front[0]] = float("inf")
            distances[sorted_front[-1]] = float("inf")
            span = objectives[sorted_front[-1]][k] - objectives[sorted_front[0]][k]
            if span <= 0:
                continue
            for idx in range(1, len(sorted_front) - 1):
                prev_v = objectives[sorted_front[idx - 1]][k]
                next_v = objectives[sorted_front[idx + 1]][k]
                distances[sorted_front[idx]] += (next_v - prev_v) / span
        return distances

    def _tournament(
        self,
        indices: List[int],
        ranks: List[int],
        crowding: Dict[int, float],
    ) -> int:
        if not indices:
            return 0
        a, b = random.sample(indices, 2) if len(indices) >= 2 else (indices[0], indices[0])
        if ranks[a] < ranks[b]:
            return a
        if ranks[b] < ranks[a]:
            return b
        return a if crowding.get(a, 0.0) >= crowding.get(b, 0.0) else b

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
        c1, c2 = {}, {}
        for k in p1:
            if random.random() < 0.5:
                c1[k], c2[k] = p1[k], p2[k]
            else:
                c1[k], c2[k] = p2[k], p1[k]
            # occasional blend
            if random.random() < 0.3:
                mid = (p1[k] + p2[k]) / 2.0
                c1[k] = mid
                c2[k] = mid
        return c1, c2

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = dict(ind)
        for k, (lo, hi) in self.PARAM_BOUNDS.items():
            if random.random() < self.config.genetic_mutation_rate:
                span = hi - lo
                mutant[k] = max(lo, min(hi, mutant[k] + random.uniform(-0.1 * span, 0.1 * span)))
        return mutant

    # ---------- evolution ----------
    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        generations = generations or self.config.genetic_generations
        async with self._get_lock():
            population = self._initialize_population()
            local_pareto: List[MOPDPoint] = []

            for _ in range(generations):
                # Evaluate current population (outside any nested lock)
                objectives = []
                for ind in population:
                    objectives.append(await self._evaluate(ind))

                # Build offspring
                offspring: List[Dict[str, float]] = []
                ranks = [0] * len(population)
                crowding_global: Dict[int, float] = {}
                while len(offspring) < self.config.genetic_population_size:
                    i = random.randrange(len(population))
                    j = random.randrange(len(population))
                    if random.random() < self.config.genetic_crossover_rate:
                        c1, c2 = self._crossover(population[i], population[j])
                        offspring.append(self._mutate(c1))
                        if len(offspring) < self.config.genetic_population_size:
                            offspring.append(self._mutate(c2))
                    else:
                        offspring.append(self._mutate(dict(population[i])))
                offspring = offspring[: self.config.genetic_population_size]

                # Evaluate offspring
                off_objs = [await self._evaluate(ind) for ind in offspring]

                # Combine + sort
                combined_pop = population + offspring
                combined_objs = objectives + off_objs
                fronts = self._fast_non_dominated_sort(combined_objs)

                # Update Pareto front
                if fronts:
                    pareto_idx = fronts[0]
                    local_pareto = []
                    for idx in pareto_idx:
                        obj = combined_objs[idx]
                        local_pareto.append(MOPDPoint(
                            individual=dict(combined_pop[idx]),
                            efficiency=obj["efficiency"],
                            inflation=obj["inflation"],
                            emergency=obj["emergency"],
                        ))

                # Select next generation via fronts + crowding
                new_pop: List[Dict[str, float]] = []
                next_objs: List[Dict[str, float]] = []
                for front in fronts:
                    if len(new_pop) + len(front) <= self.config.genetic_population_size:
                        for idx in front:
                            new_pop.append(combined_pop[idx])
                            next_objs.append(combined_objs[idx])
                    else:
                        cd = self._crowding_distance(front, combined_objs)
                        sorted_front = sorted(front, key=lambda i: cd.get(i, 0.0), reverse=True)
                        remaining = self.config.genetic_population_size - len(new_pop)
                        for idx in sorted_front[:remaining]:
                            new_pop.append(combined_pop[idx])
                            next_objs.append(combined_objs[idx])
                        break
                population, objectives = new_pop, next_objs

            self.pareto_front = local_pareto

            if local_pareto:
                # Choose best via scalarisation
                w = self.config.mopd.objective_weights
                best_score, best_point = -math.inf, None
                for p in local_pareto:
                    score = self._scalarise(
                        {"efficiency": p.efficiency, "inflation": p.inflation, "emergency": p.emergency},
                        w,
                    )
                    if score > best_score:
                        best_score, best_point = score, p
                if best_point is not None:
                    self.best_individual = dict(best_point.individual)
                    self.best_fitness = best_score

            # Apply best individual to live config
            if self.best_individual:
                self.manager.apply_optimizer_params(self.best_individual)

            self.evolution_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "best_fitness": self.best_fitness,
                "pareto_front_size": len(self.pareto_front),
                "generations": generations,
            })

            return {
                "best_fitness": self.best_fitness,
                "best_individual": self.best_individual,
                "pareto_front": [p.to_dict() for p in self.pareto_front],
                "generations": generations,
            }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "evolution_history": self.evolution_history,
            "pareto_front": [p.to_dict() for p in self.pareto_front],
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        data = data or {}
        self.best_fitness = float(data.get("best_fitness", -math.inf))
        self.best_individual = data.get("best_individual")
        self.evolution_history = list(data.get("evolution_history", []))
        self.pareto_front = [MOPDPoint.from_dict(p) for p in data.get("pareto_front", [])]

    def get_status(self) -> Dict[str, Any]:
        return {
            "best_fitness": self.best_fitness,
            "best_individual": self.best_individual,
            "history": self.evolution_history[-10:],
            "pareto_front_size": len(self.pareto_front),
        }


# =============================================================================
# SECTION 11. DISTRIBUTED TOKEN MARKET (STABLE, uses manager locks)
# =============================================================================
class OrderBook:
    def __init__(self):
        self.buy_orders: Dict[float, List[MarketOrder]] = defaultdict(list)
        self.sell_orders: Dict[float, List[MarketOrder]] = defaultdict(list)
        self.all_orders: Dict[str, MarketOrder] = {}

    def add_order(self, order: MarketOrder) -> None:
        self.all_orders[order.order_id] = order
        if order.side == "buy":
            self.buy_orders[order.price].append(order)
        else:
            self.sell_orders[order.price].append(order)

    def remove_order(self, order_id: str) -> None:
        order = self.all_orders.pop(order_id, None)
        if not order:
            return
        bucket = self.buy_orders if order.side == "buy" else self.sell_orders
        if order.price in bucket:
            bucket[order.price] = [o for o in bucket[order.price] if o.order_id != order_id]
            if not bucket[order.price]:
                del bucket[order.price]

    def best_buy(self) -> Optional[float]:
        return max(self.buy_orders.keys()) if self.buy_orders else None

    def best_sell(self) -> Optional[float]:
        return min(self.sell_orders.keys()) if self.sell_orders else None

    def cleanup_expired(self, now: datetime) -> None:
        expired = [oid for oid, o in self.all_orders.items() if o.status == "open" and o.expires_at and o.expires_at <= now]
        for oid in expired:
            self.remove_order(oid)


class DistributedTokenMarket:
    def __init__(self, token_manager: "EcoATPTokenManager", config: EcoATPConfig):
        self.token_manager = token_manager
        self.config = config
        self.order_book = OrderBook()
        self.trade_history: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def place_order(self, account_id: str, amount: float, price: float, side: str) -> str:
        async with self._get_lock():
            order = MarketOrder(
                order_id=f"order_{uuid.uuid4().hex[:8]}",
                account_id=account_id,
                amount=amount,
                price=price,
                side=side,
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=self.config.market_order_expiry_minutes),
            )
            self.order_book.add_order(order)
            return order.order_id

    async def match_orders(self) -> List[Dict[str, Any]]:
        async with self._get_lock():
            matches: List[Dict[str, Any]] = []
            now = datetime.now(timezone.utc)
            self.order_book.cleanup_expired(now)

            while True:
                bb = self.order_book.best_buy()
                bs = self.order_book.best_sell()
                if bb is None or bs is None or bs > bb:
                    break
                buy = self.order_book.buy_orders[bb][0]
                sell = self.order_book.sell_orders[bs][0]
                trade_amount = min(buy.remaining, sell.remaining)
                trade_price = (buy.price + sell.price) / 2.0

                # Use manager's account lock for atomic balance update
                async with self.token_manager._get_accounts_lock():
                    buyer = self.token_manager.accounts.get(buy.account_id)
                    seller = self.token_manager.accounts.get(sell.account_id)
                    if buyer is None or seller is None:
                        self.order_book.remove_order(buy.order_id)
                        self.order_book.remove_order(sell.order_id)
                        continue
                    total_cost = trade_price * trade_amount
                    if buyer.balance < total_cost:
                        buy.status = "cancelled"
                        self.order_book.remove_order(buy.order_id)
                        continue
                    buyer.balance -= total_cost
                    seller.balance += total_cost

                buy.remaining -= trade_amount
                sell.remaining -= trade_amount
                if buy.remaining <= 1e-9:
                    buy.status = "completed"
                    self.order_book.remove_order(buy.order_id)
                if sell.remaining <= 1e-9:
                    sell.status = "completed"
                    self.order_book.remove_order(sell.order_id)

                trade = {
                    "sell_order": sell.order_id,
                    "buy_order": buy.order_id,
                    "seller": sell.account_id,
                    "buyer": buy.account_id,
                    "amount": trade_amount,
                    "price": trade_price,
                    "timestamp": now.isoformat(),
                }
                matches.append(trade)
                self.trade_history.append(trade)
            return matches

    def get_market_stats(self) -> Dict[str, Any]:
        active = [o for o in self.order_book.all_orders.values() if o.status == "open"]
        return {
            "active_orders": len(active),
            "sell_orders": sum(1 for o in active if o.side == "sell"),
            "buy_orders": sum(1 for o in active if o.side == "buy"),
            "total_trades": len(self.trade_history),
            "total_volume": sum(t["amount"] for t in self.trade_history),
            "average_price": float(np.mean([t["price"] for t in self.trade_history]))
            if self.trade_history else 0.0,
        }


# =============================================================================
# SECTION 12. PERSISTENCE (EXPERIMENTAL) — aiosqlite, awaited initialize()
# =============================================================================
class AsyncPersistenceManager:
    STATUS = "experimental"
    CURRENT_VERSION = "1.0"

    def __init__(self, config: EcoATPConfig, enabled: bool = True):
        if enabled:
            _warn_module("persistence")
        self.config = config
        self.db_path = config.persistence_path
        self._lock: Optional[asyncio.Lock] = None
        self._initialized = False

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def initialize(self) -> None:
        if self._initialized:
            return
        if not AIOSQLITE_AVAILABLE:
            logger.warning("aiosqlite unavailable; persistence disabled")
            self._initialized = True
            return
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    token_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            await conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
                (self.CURRENT_VERSION,),
            )
            await conn.commit()
        self._initialized = True

    async def save_account(self, account: EcoATPAccount) -> bool:
        if not AIOSQLITE_AVAILABLE or not self._initialized:
            return False
        try:
            async with self._get_lock():
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute(
                        "INSERT OR REPLACE INTO accounts (account_id, data, updated_at) VALUES (?, ?, ?)",
                        (account.account_id, json.dumps(account.to_dict(), default=str),
                         datetime.now(timezone.utc).isoformat()),
                    )
                    await conn.commit()
            return True
        except Exception as e:
            logger.warning("save_account failed", error=str(e))
            return False

    async def save_token(self, token: EcoATPToken, account_id: str) -> bool:
        if not AIOSQLITE_AVAILABLE or not self._initialized:
            return False
        try:
            async with self._get_lock():
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute(
                        "INSERT OR REPLACE INTO tokens (token_id, account_id, data, updated_at) VALUES (?, ?, ?, ?)",
                        (token.token_id, account_id, json.dumps(token.to_dict(), default=str),
                         datetime.now(timezone.utc).isoformat()),
                    )
                    await conn.commit()
            return True
        except Exception as e:
            logger.warning("save_token failed", error=str(e))
            return False

    async def load_accounts(self) -> List[EcoATPAccount]:
        if not AIOSQLITE_AVAILABLE or not self._initialized:
            return []
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                async with conn.execute("SELECT data FROM accounts") as cur:
                    rows = await cur.fetchall()
            return [EcoATPAccount.from_dict(json.loads(r[0])) for r in rows]
        except Exception as e:
            logger.warning("load_accounts failed", error=str(e))
            return []

    async def load_tokens(self) -> List[Tuple[str, EcoATPToken]]:
        if not AIOSQLITE_AVAILABLE or not self._initialized:
            return []
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                async with conn.execute("SELECT account_id, data FROM tokens") as cur:
                    rows = await cur.fetchall()
            return [(r[0], EcoATPToken.from_dict(json.loads(r[1]))) for r in rows]
        except Exception as e:
            logger.warning("load_tokens failed", error=str(e))
            return []


# =============================================================================
# SECTION 13. AUTONOMOUS STRATEGY SELECTOR (EXPERIMENTAL) — bounded Q-table
# =============================================================================
class AutonomousStrategySelector:
    STATUS = "experimental"

    def __init__(self, config: EcoATPConfig, enabled: bool = True):
        if enabled:
            _warn_module("autonomous_strategy")
        self.config = config
        self.learning_rate = config.rl_learning_rate
        self.discount_factor = config.rl_discount_factor
        self.exploration_rate = config.rl_exploration_rate
        self.actions = ["conservative", "balanced", "performance"]
        self.q_table: "OrderedDict[str, Dict[str, float]]" = OrderedDict()
        self.max_size = config.q_table_max_size
        self.total_updates = 0
        self.available = True

    def _state_to_key(self, state: Dict[str, Any]) -> str:
        load = state.get("system_load", 0.5)
        util = state.get("system_efficiency", 0.5)
        load_bin = "high" if load > 0.7 else "medium" if load > 0.4 else "low"
        util_bin = "high" if util > 0.7 else "medium" if util > 0.4 else "low"
        return f"{load_bin}_{util_bin}"

    def _ensure(self, key: str) -> Dict[str, float]:
        if key in self.q_table:
            self.q_table.move_to_end(key)
            return self.q_table[key]
        if len(self.q_table) >= self.max_size:
            self.q_table.popitem(last=False)
        self.q_table[key] = {a: 0.0 for a in self.actions}
        return self.q_table[key]

    async def select_strategy(self, state: Dict[str, Any]) -> str:
        key = self._state_to_key(state)
        bucket = self._ensure(key)
        if random.random() < self.exploration_rate:
            self.exploration_rate = max(0.01, self.exploration_rate * 0.999)
            return random.choice(self.actions)
        return max(bucket, key=bucket.get)

    async def update(self, state: Dict[str, Any], action: str, reward: float,
                     next_state: Dict[str, Any]) -> None:
        key = self._state_to_key(state)
        next_key = self._state_to_key(next_state)
        bucket = self._ensure(key)
        next_bucket = self._ensure(next_key)
        current_q = bucket.get(action, 0.0)
        next_max = max(next_bucket.values()) if next_bucket else 0.0
        new_q = current_q + self.learning_rate * (
            reward + self.discount_factor * next_max - current_q
        )
        bucket[action] = new_q
        self.total_updates += 1


# =============================================================================
# SECTION 14. MAIN TOKEN MANAGER
# =============================================================================
class EcoATPTokenManager:
    """
    Lifecycle:
        mgr = EcoATPTokenManager(...)
        await mgr.start()
        ...
        await mgr.shutdown()
    """

    def __init__(self, config: Optional[EcoATPConfig] = None,
                 message_queue: Optional[Any] = None):
        self.config = config or EcoATPConfig()
        self.message_queue = message_queue

        # Core state
        self.accounts: Dict[str, EcoATPAccount] = {}
        self.active_tokens: Dict[str, EcoATPToken] = {}

        # Lazy locks
        self._accounts_lock: Optional[asyncio.Lock] = None
        self._tokens_lock: Optional[asyncio.Lock] = None

        # Emergency
        self.emergency_mode = False
        self.emergency_reserve = self.config.emergency_reserve
        self.substrate_reserves = self.config.substrate_reserves_min

        # Tenant quotas
        self.tenant_usage: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=100))
        self.suspicious_tenants: Set[str] = set()
        self._failed_attempts: Dict[str, int] = defaultdict(int)

        # Rate limiting
        self.current_rate_multiplier = 1.0

        # Sub-components
        self.market = DistributedTokenMarket(self, self.config)
        self.ml_predictor = MLDemandPredictor(self.config, enabled=True)
        self.xai = XAIExplainer(enabled=self.config.enable_xai)
        self.quantum_security = QuantumResilientSecurity(enabled=self.config.enable_quantum_signing)
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()
        self.genetic_optimizer = (
            GeneticOptimizer(self, self.config, enabled=True)
            if self.config.enable_genetic_optimizer else None
        )
        self.strategy_selector = (
            AutonomousStrategySelector(self.config, enabled=self.config.enable_autonomous_strategy)
            if self.config.enable_autonomous_strategy else None
        )
        # Placeholders
        self.rl_agent = CausalRLAgentPlaceholder(
            state_dim=10, action_dim=3,
            max_q_table=self.config.q_table_max_size,
            enabled=self.config.enable_causal_rl,
        )
        self.federated = (
            FederatedCoordinatorPlaceholder(self, message_queue, enabled=self.config.enable_federated_learning)
            if self.config.enable_federated_learning else None
        )
        self.precision_controller = PrecisionControllerPlaceholder(
            enabled=self.config.enable_precision_switching,
        )
        self.carbon_market = (
            CarbonMarketClientPlaceholder(enabled=self.config.enable_carbon_market)
            if self.config.enable_carbon_market else None
        )
        self.chaos_injector = ChaosInjectorPlaceholder(
            self, self.config.chaos_probability,
            enabled=self.config.enable_chaos,
        )
        self.human_approval = HumanApprovalHandlerPlaceholder(
            message_queue, enabled=self.config.enable_human_approval,
        )
        self.blockchain_auditor = BlockchainAuditorPlaceholder(
            self.config, enabled=self.config.enable_blockchain_audit,
        )
        self.multi_cloud = MultiCloudDistributorPlaceholder(
            self.config, enabled=self.config.enable_multi_cloud,
        )

        # Circuit breaker
        self.circuit_breaker = (
            CircuitBreaker(
                name="eco_atp",
                db_path=self.config.circuit_breaker_db_path,
                failure_threshold=self.config.circuit_breaker_failure_threshold,
                recovery_timeout=self.config.circuit_breaker_recovery_timeout,
            )
            if self.config.enable_circuit_breaker else None
        )

        # Persistence
        self.persistence: Optional[AsyncPersistenceManager] = (
            AsyncPersistenceManager(self.config, enabled=True)
            if self.config.enable_persistence and AIOSQLITE_AVAILABLE else None
        )

        # Task manager
        self.task_manager = TaskManager()

        # Lifecycle
        self._started = False
        self._shutdown = False

        # Prometheus
        self._prom = self._setup_metrics()

        logger.info("EcoATPTokenManager initialized", persistence=self.persistence is not None)

    # ---------------- locks ----------------
    def _get_accounts_lock(self) -> asyncio.Lock:
        if self._accounts_lock is None:
            self._accounts_lock = asyncio.Lock()
        return self._accounts_lock

    def _get_tokens_lock(self) -> asyncio.Lock:
        if self._tokens_lock is None:
            self._tokens_lock = asyncio.Lock()
        return self._tokens_lock

    # ---------------- safety ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "token_balance_non_negative",
            lambda s: s.get("total_balance", 0.0) >= 0.0,
            "Total token balance cannot be negative",
        )
        self.safety_monitor.add_invariant(
            "emergency_reserve_non_negative",
            lambda s: s.get("emergency_reserve", 0.0) >= 0.0,
            "Emergency reserve cannot be negative",
        )
        self.safety_monitor.add_invariant(
            "max_active_tokens",
            lambda s: s.get("active_tokens", 0) <= 100_000,
            "Too many active tokens",
        )

    def _get_safety_state_sync(self) -> Dict[str, Any]:
        total_balance = sum(a.balance for a in self.accounts.values())
        active = sum(1 for t in self.active_tokens.values() if t.state == TokenState.AVAILABLE)
        return {
            "total_balance": total_balance,
            "emergency_reserve": self.emergency_reserve,
            "active_tokens": active,
        }

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE:
            return {}
        try:
            return {
                "generated_total": Counter("eco_atp_generated_total", "Tokens generated (value)"),
                "consumed_total": Counter("eco_atp_consumed_total", "Tokens consumed"),
                "expired_total": Counter("eco_atp_expired_total", "Tokens expired"),
                "emergency_gauge": Gauge("eco_atp_emergency_mode", "Emergency mode"),
                "balance_gauge": Gauge("eco_atp_total_balance", "Total balance"),
                "accounts_gauge": Gauge("eco_atp_accounts", "Account count"),
                "tokens_gauge": Gauge("eco_atp_active_tokens", "Active tokens"),
                "pareto_gauge": Gauge("eco_atp_pareto_size", "Pareto front size"),
                "q_table_gauge": Gauge("eco_atp_q_table_size", "Q-table size"),
            }
        except Exception:
            return {}

    def _update_metrics(self) -> None:
        if not self._prom:
            return
        try:
            self._prom["emergency_gauge"].set(1 if self.emergency_mode else 0)
            self._prom["balance_gauge"].set(sum(a.balance for a in self.accounts.values()))
            self._prom["accounts_gauge"].set(len(self.accounts))
            self._prom["tokens_gauge"].set(
                sum(1 for t in self.active_tokens.values() if t.state == TokenState.AVAILABLE)
            )
            if self.genetic_optimizer is not None:
                self._prom["pareto_gauge"].set(len(self.genetic_optimizer.pareto_front))
            self._prom["q_table_gauge"].set(self.rl_agent.size())
        except Exception:
            pass

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        if self.persistence is not None:
            await self.persistence.initialize()
            accounts = await self.persistence.load_accounts()
            for a in accounts:
                self.accounts[a.account_id] = a
            tokens = await self.persistence.load_tokens()
            for account_id, token in tokens:
                self.active_tokens[token.token_id] = token

        self.task_manager.start_task("emergency_monitor", self._emergency_monitor_loop)
        self.task_manager.start_task("token_cleanup", self._token_cleanup_loop)
        self.task_manager.start_task("market_matching", self._market_matching_loop)
        self.task_manager.start_task("persistence_flush", self._persistence_flush_loop)
        self.task_manager.start_task("ml_training", self._ml_training_loop)
        if self.genetic_optimizer is not None:
            self.task_manager.start_task("evolution", self._evolution_loop)
        if self.federated is not None:
            self.task_manager.start_task("federated", self._federated_loop)
        if self.chaos_injector is not None:
            self.task_manager.start_task("chaos", self._chaos_loop)

        logger.info("EcoATPTokenManager started")

    async def ready(self) -> bool:
        if not self._started:
            return False
        return isinstance(self.accounts, dict) and isinstance(self.active_tokens, dict)

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("EcoATPTokenManager shutting down")

        try:
            await asyncio.wait_for(self.task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        if self.persistence is not None:
            for a in self.accounts.values():
                await self.persistence.save_account(a)
            for token in self.active_tokens.values():
                await self.persistence.save_token(token, next(
                    (aid for aid, acc in self.accounts.items()), "unknown"
                ))

        logger.info("EcoATPTokenManager shutdown complete")

    async def __aenter__(self) -> "EcoATPTokenManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _emergency_monitor_loop(self) -> None:
        while True:
            try:
                total = sum(a.balance for a in self.accounts.values())
                if total < self.config.emergency_threshold and not self.emergency_mode:
                    self.emergency_mode = True
                    logger.warning("Emergency mode activated", balance=total)
                    await self.generate_tokens(
                        "emergency", EcoATPSource.EMERGENCY_SUBSTRATE,
                        energy_saved_kwh=self.config.emergency_token_rate,
                    )
                elif total > self.config.emergency_threshold * 2 and self.emergency_mode:
                    self.emergency_mode = False
                    logger.info("Emergency mode deactivated")
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Emergency monitor error", error=str(e))
                await asyncio.sleep(30)

    async def _token_cleanup_loop(self) -> None:
        while True:
            try:
                now = datetime.now(timezone.utc)
                async with self._get_tokens_lock():
                    expired_ids = [
                        tid for tid, t in self.active_tokens.items()
                        if t.state == TokenState.AVAILABLE and t.is_expired(now)
                    ]
                    for tid in expired_ids:
                        token = self.active_tokens.pop(tid, None)
                        if token is None:
                            continue
                        token.state = TokenState.EXPIRED
                        acc = self.accounts.get("default")
                        if acc:
                            acc.total_expired += token.value
                        if self._prom:
                            try:
                                self._prom["expired_total"].inc()
                            except Exception:
                                pass
                self._update_metrics()
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Token cleanup error", error=str(e))
                await asyncio.sleep(60)

    async def _market_matching_loop(self) -> None:
        while True:
            try:
                await self.market.match_orders()
                await asyncio.sleep(self.config.market_matching_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Market matching error", error=str(e))
                await asyncio.sleep(30)

    async def _persistence_flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.state_save_interval_seconds)
                if self.persistence is None:
                    continue
                for a in self.accounts.values():
                    await self.persistence.save_account(a)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Persistence flush error", error=str(e))
                await asyncio.sleep(30)

    async def _ml_training_loop(self) -> None:
        while True:
            try:
                await self.ml_predictor.train()
                await asyncio.sleep(self.config.ml_retrain_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("ML training error", error=str(e))
                await asyncio.sleep(120)

    async def _evolution_loop(self) -> None:
        while True:
            try:
                if self.genetic_optimizer is not None and len(self.accounts) >= 1:
                    await self.genetic_optimizer.evolve()
                await asyncio.sleep(self.config.genetic_evolution_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Evolution error", error=str(e))
                await asyncio.sleep(3600)

    async def _federated_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(300)
                if self.federated is not None:
                    await self.federated.send_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Federated loop error", error=str(e))

    async def _chaos_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(60)
                if self.chaos_injector is not None:
                    await self.chaos_injector.maybe_inject_failure()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Chaos loop error", error=str(e))

    # ---------------- metrics snapshot for GA ----------------
    def _metrics_snapshot(self) -> Dict[str, float]:
        total_gen = sum(a.total_generated for a in self.accounts.values())
        total_con = sum(a.total_consumed for a in self.accounts.values())
        return {
            "total_generated": total_gen,
            "total_consumed": total_con,
        }

    # ---------------- account operations ----------------
    async def create_account(self, account_id: str) -> EcoATPAccount:
        if self.safety_monitor is not None:
            violations = self.safety_monitor.check(self._get_safety_state_sync())
            if violations:
                logger.warning("Safety violation on account creation", violations=violations)
        async with self._get_accounts_lock():
            if account_id not in self.accounts:
                self.accounts[account_id] = EcoATPAccount(account_id=account_id)
                if self.persistence is not None:
                    await self.persistence.save_account(self.accounts[account_id])
            return self.accounts[account_id]

    async def generate_tokens(
        self, account_id: str, source: EcoATPSource,
        carbon_saved_kg: float = 0.0, helium_saved_units: float = 0.0,
        energy_saved_kwh: float = 0.0, efficiency: float = 1.0,
    ) -> List[EcoATPToken]:
        if self.safety_monitor is not None:
            violations = self.safety_monitor.check(self._get_safety_state_sync())
            if violations:
                logger.warning("Safety violation on generation", violations=violations)
                return []
        if self.human_approval is not None and (
            carbon_saved_kg + helium_saved_units + energy_saved_kwh
        ) > 100:
            approved = await self.human_approval.request_approval({"action": "generate_tokens"})
            if not approved:
                return []

        carbon_value = carbon_saved_kg * self.config.carbon_to_ecoatp_factor
        helium_value = helium_saved_units * self.config.helium_to_ecoatp_factor
        energy_value = energy_saved_kwh * self.config.energy_to_ecoatp_factor
        total_value = (carbon_value + helium_value + energy_value) * efficiency
        if total_value <= 0:
            return []

        num_tokens = max(1, int(total_value / 10))
        token_value = total_value / num_tokens
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(hours=self.config.token_expiry_hours)

        tokens: List[EcoATPToken] = []
        async with self._get_tokens_lock():
            for i in range(num_tokens):
                token = EcoATPToken(
                    token_id=f"eco_{account_id}_{now.timestamp()}_{i}_{uuid.uuid4().hex[:4]}",
                    value=token_value,
                    source=source,
                    generated_at=now,
                    expires_at=expiry,
                    carbon_equivalent_kg=carbon_saved_kg / num_tokens,
                    helium_equivalent_units=helium_saved_units / num_tokens,
                    generation_efficiency=efficiency,
                )
                tokens.append(token)
                self.active_tokens[token.token_id] = token

        async with self._get_accounts_lock():
            acc = self.accounts.get(account_id)
            if acc is None:
                acc = EcoATPAccount(account_id=account_id)
                self.accounts[account_id] = acc
            acc.balance += total_value
            acc.total_generated += total_value
            if self.persistence is not None:
                await self.persistence.save_account(acc)

        if self._prom:
            try:
                self._prom["generated_total"].inc(total_value)
            except Exception:
                pass

        await self.ml_predictor.record_demand(account_id, total_value, now)
        if self.blockchain_auditor is not None:
            await self.blockchain_auditor.record_event("token_generation", {
                "account_id": account_id,
                "amount": total_value,
                "source": source.value,
                "token_count": len(tokens),
            })
        if self.config.enable_xai:
            logger.info(self.xai.explain_generation(
                carbon_saved_kg, helium_saved_units, energy_saved_kwh, total_value
            ))
        self._update_metrics()
        return tokens

    async def reserve_tokens(
        self, account_id: str, amount: float, consumer: EcoATPConsumer,
        tenant_id: str = "default", priority: int = 2,
    ) -> Tuple[bool, List[str]]:
        if self.safety_monitor is not None:
            violations = self.safety_monitor.check(self._get_safety_state_sync())
            if violations:
                logger.warning("Safety violation on reservation", violations=violations)
                return False, []
        if self.human_approval is not None and amount > 100:
            approved = await self.human_approval.request_approval({
                "action": "reserve_tokens", "amount": amount,
            })
            if not approved:
                return False, []

        async with self._get_accounts_lock():
            acc = self.accounts.get(account_id)
            if acc is None or acc.balance < amount:
                self._failed_attempts[tenant_id] += 1
                if self._failed_attempts[tenant_id] >= self.config.suspicious_threshold:
                    self.suspicious_tenants.add(tenant_id)
                return False, []
            if tenant_id in self.suspicious_tenants:
                return False, []

            async with self._get_tokens_lock():
                available = [
                    t for t in self.active_tokens.values()
                    if t.state == TokenState.AVAILABLE
                ]
                if len(available) < amount:
                    return False, []
                reserved: List[str] = []
                for token in available[: int(amount)]:
                    token.state = TokenState.RESERVED
                    reserved.append(token.token_id)
            acc.balance -= amount
            if self.persistence is not None:
                await self.persistence.save_account(acc)

        if self.config.enable_xai:
            logger.info(self.xai.explain_reservation(amount, consumer.value, priority))
        self._update_metrics()
        return True, reserved

    async def consume_tokens(self, token_ids: List[str], consumer: EcoATPConsumer,
                             operation_success: bool) -> float:
        total = 0.0
        async with self._get_tokens_lock():
            for tid in token_ids:
                token = self.active_tokens.get(tid)
                if token is None or token.state != TokenState.RESERVED:
                    continue
                total += token.value
                if operation_success:
                    token.state = TokenState.CONSUMED
                    token.consumed_at = datetime.now(timezone.utc)
                else:
                    token.state = TokenState.AVAILABLE
        if total > 0 and self._prom:
            try:
                self._prom["consumed_total"].inc(total)
            except Exception:
                pass
        self._update_metrics()
        return total

    async def get_system_summary(self) -> Dict[str, Any]:
        total_balance = sum(a.balance for a in self.accounts.values())
        total_generated = sum(a.total_generated for a in self.accounts.values())
        total_consumed = sum(a.total_consumed for a in self.accounts.values())
        active = sum(1 for t in self.active_tokens.values() if t.state == TokenState.AVAILABLE)
        reserved = sum(1 for t in self.active_tokens.values() if t.state == TokenState.RESERVED)
        efficiency = total_consumed / max(total_generated, 1.0)
        return {
            "total_balance": total_balance,
            "total_generated": total_generated,
            "total_consumed": total_consumed,
            "active_tokens": active,
            "reserved_tokens": reserved,
            "total_accounts": len(self.accounts),
            "system_efficiency": efficiency,
            "emergency_mode": self.emergency_mode,
            "substrate_reserves": self.substrate_reserves,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def get_account_summary(self, account_id: str) -> Dict[str, Any]:
        acc = self.accounts.get(account_id)
        if acc is None:
            return {}
        return acc.to_dict() | {"utilization_rate": acc.utilization_rate}

    # ---------------- strategy ----------------
    async def _get_strategy_state(self) -> Dict[str, Any]:
        total_gen = sum(a.total_generated for a in self.accounts.values())
        total_con = sum(a.total_consumed for a in self.accounts.values())
        return {
            "system_load": min(len(self.active_tokens) / 1000.0, 1.0),
            "system_efficiency": total_con / max(total_gen, 1.0),
            "emergency_mode": self.emergency_mode,
        }

    # ---------------- policy_probs for teacher ----------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        # Fixed-length over {conservative, balanced, performance}
        if self.strategy_selector is not None:
            key = self.strategy_selector._state_to_key(state)
            bucket = self.strategy_selector._ensure(key)
            arr = np.array([bucket.get(a, 0.0) for a in self.strategy_selector.actions])
            exp = np.exp(arr - arr.max())
            return (exp / exp.sum()).tolist()
        return [1.0 / 3, 1.0 / 3, 1.0 / 3]

    # ---------------- optimizer integration ----------------
    def apply_optimizer_params(self, params: Dict[str, float]) -> None:
        for key, value in params.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

    # ---------------- public MOPD helpers ----------------
    def get_mopd_pareto_front(self) -> List[MOPDPoint]:
        if self.genetic_optimizer is None:
            return []
        return list(self.genetic_optimizer.pareto_front)

    def get_mopd_summary(self) -> Dict[str, Any]:
        if self.genetic_optimizer is None:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": dict(self.config.mopd.objective_weights),
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "best_scalarised_score": self.genetic_optimizer.best_fitness,
            "evolution_history": self.genetic_optimizer.evolution_history[-10:],
        }


# =============================================================================
# SECTION 15. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cfg(self) -> EcoATPConfig:
        import tempfile
        td = tempfile.mkdtemp()
        return EcoATPConfig(
            enable_persistence=False,
            enable_blockchain_audit=False,
            enable_multi_cloud=False,
            enable_causal_rl=False,
            enable_federated_learning=False,
            enable_safety_monitor=True,
            enable_xai=False,
            enable_precision_switching=False,
            enable_carbon_market=False,
            enable_chaos=False,
            enable_human_approval=False,
            enable_genetic_optimizer=True,
            genetic_population_size=6,
            genetic_generations=2,
            genetic_evolution_interval_seconds=3600,
            state_save_interval_seconds=3600,
            ml_retrain_interval_seconds=3600,
            circuit_breaker_db_path=os.path.join(td, "cb.db"),
        )

    def test_lifecycle(self):
        async def go():
            mgr = EcoATPTokenManager(config=self._cfg())
            self.assertFalse(await mgr.ready())
            await mgr.start()
            self.assertTrue(await mgr.ready())
            await mgr.shutdown()
            self.assertTrue(mgr._shutdown)
            await mgr.shutdown()
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with EcoATPTokenManager(config=self._cfg()) as mgr:
                self.assertTrue(await mgr.ready())
        asyncio.run(go())

    def test_create_and_generate(self):
        async def go():
            async with EcoATPTokenManager(config=self._cfg()) as mgr:
                await mgr.create_account("a")
                tokens = await mgr.generate_tokens("a", EcoATPSource.RENEWABLE_ENERGY, energy_saved_kwh=10.0)
                self.assertGreater(len(tokens), 0)
                self.assertGreater(tokens[0].value, 0)
                summary = await mgr.get_system_summary()
                self.assertGreater(summary["total_balance"], 0)
        asyncio.run(go())

    def test_reserve_and_consume(self):
        async def go():
            async with EcoATPTokenManager(config=self._cfg()) as mgr:
                await mgr.create_account("a")
                await mgr.generate_tokens("a", EcoATPSource.RENEWABLE_ENERGY, energy_saved_kwh=10.0)
                ok, ids = await mgr.reserve_tokens("a", 1, EcoATPConsumer.EXPERT_EXECUTION)
                self.assertTrue(ok)
                self.assertGreaterEqual(len(ids), 1)
                val = await mgr.consume_tokens(ids, EcoATPConsumer.EXPERT_EXECUTION, True)
                self.assertGreaterEqual(val, 0.0)
        asyncio.run(go())

    def test_ga_genome_dependent(self):
        async def go():
            async with EcoATPTokenManager(config=self._cfg()) as mgr:
                await mgr.create_account("a")
                await mgr.generate_tokens("a", EcoATPSource.RENEWABLE_ENERGY, energy_saved_kwh=10.0)
                ga = mgr.genetic_optimizer
                assert ga is not None
                ind_a = {k: lo for k, (lo, hi) in GeneticOptimizer.PARAM_BOUNDS.items()}
                ind_b = {k: hi for k, (lo, hi) in GeneticOptimizer.PARAM_BOUNDS.items()}
                oa = await ga._evaluate(ind_a)
                ob = await ga._evaluate(ind_b)
                # At least one objective should differ
                diffs = [abs(oa[k] - ob[k]) for k in oa]
                self.assertGreater(max(diffs), 1e-4)
        asyncio.run(go())

    def test_ga_evolve_runs(self):
        async def go():
            async with EcoATPTokenManager(config=self._cfg()) as mgr:
                await mgr.create_account("a")
                await mgr.generate_tokens("a", EcoATPSource.RENEWABLE_ENERGY, energy_saved_kwh=10.0)
                result = await mgr.genetic_optimizer.evolve(generations=2)
                self.assertIsNotNone(result)
                self.assertIn("best_fitness", result)
        asyncio.run(go())

    def test_policy_probs_valid(self):
        async def go():
            async with EcoATPTokenManager(config=self._cfg()) as mgr:
                probs = await mgr.policy_probs({"system_load": 0.5, "system_efficiency": 0.5})
                self.assertEqual(len(probs), 3)
                self.assertTrue(all(p >= 0 for p in probs))
                self.assertAlmostEqual(sum(probs), 1.0, places=6)
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

    def test_persistence_roundtrip(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cfg = self._cfg()
            cfg.enable_persistence = True
            cfg.persistence_path = os.path.join(td, "state.db")
            if not AIOSQLITE_AVAILABLE:
                self.skipTest("aiosqlite unavailable")
            async with EcoATPTokenManager(config=cfg) as mgr:
                await mgr.create_account("a")
                await mgr.generate_tokens("a", EcoATPSource.RENEWABLE_ENERGY, energy_saved_kwh=5.0)
                self.assertIn("a", mgr.accounts)
            mgr2 = EcoATPTokenManager(config=cfg)
            async with mgr2:
                self.assertIn("a", mgr2.accounts)
        asyncio.run(go())

    def test_circuit_breaker_transitions(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cb = CircuitBreaker("t", os.path.join(td, "cb.db"), failure_threshold=2, recovery_timeout=0.5)

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

    def test_security_roundtrip(self):
        async def go():
            svc = QuantumResilientSecurity(enabled=False)
            data = {"x": 1, "y": "z"}
            sig = await svc.sign_data(data)
            self.assertTrue(await svc.verify_data(data, sig))
            self.assertFalse(await svc.verify_data({"x": 2}, sig))
        asyncio.run(go())

    def test_strategy_q_table_bounded(self):
        async def go():
            cfg = self._cfg()
            cfg.q_table_max_size = 20
            sel = AutonomousStrategySelector(cfg, enabled=False)
            for i in range(1000):
                state = {"system_load": (i % 10) / 10.0, "system_efficiency": ((i * 7) % 10) / 10.0}
                await sel.select_strategy(state)
            self.assertLessEqual(len(sel.q_table), 20)
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 16. ENTRY POINT
# =============================================================================
async def _example() -> None:
    async with EcoATPTokenManager() as mgr:
        await mgr.create_account("demo")
        tokens = await mgr.generate_tokens(
            "demo", EcoATPSource.RENEWABLE_ENERGY,
            carbon_saved_kg=5.0, energy_saved_kwh=3.0,
        )
        print(f"Generated {len(tokens)} tokens")
        ok, ids = await mgr.reserve_tokens("demo", 1, EcoATPConsumer.EXPERT_EXECUTION)
        print(f"Reserved: {ok}, {len(ids)} tokens")
        print("Summary:", json.dumps(await mgr.get_system_summary(), indent=2, default=str))
        print("MOPD:", json.dumps(mgr.get_mopd_summary(), indent=2, default=str))
        print("Policy:", await mgr.policy_probs({"system_load": 0.5, "system_efficiency": 0.5}))


def main() -> None:
    parser = argparse.ArgumentParser(description="Eco-ATP Currency System v11.0.0")
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

    print("Eco-ATP Currency System v11.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
