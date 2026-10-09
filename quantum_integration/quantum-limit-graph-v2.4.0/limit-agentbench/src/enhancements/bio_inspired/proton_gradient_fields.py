#!/usr/bin/env python3
# =============================================================================
# Enhanced Photosynthetic Harvester v12.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced Photosynthetic Harvester v12.0.0
==========================================
Patched single-file version built from the v11.2.0 source.

P0 fixes
--------
- `self._state_lock` is now assigned (lazy). Harvest cycles no longer crash.
- `sense_environment` checks photoinhibition damage against the *raw*
  excitation *before* clamping, so damage actually accumulates.
- Config is pure dataclasses. Missing Pydantic no longer leaves the module
  in a broken state.
- Async token manager integration: `get_system_summary` and `generate_tokens`
  are awaited. Same for `create_account`.
- Background tasks and state load moved out of `__init__` into `async start()`.
- Every asyncio.Lock is created lazily.
- All optional imports are guarded.

P1 — Real behavior
------------------
- Persistence backends: MemoryBackend (bounded LRU) and FileBackend
  (thread-offloaded I/O, bounded cache).
- NSGA-II genetic optimizer with genome-dependent objectives.
- CausalRL Q-table is discretized and LRU-bounded (uses RLConfig bounds).
- Graceful shutdown: drain tasks with timeout, stop event bus, flush state,
  idempotent.
- Honest placeholders for: quantum_distillation, causal_rl, federated,
  precision, carbon_market, chaos, human_approval, websocket, swarm,
  competition_engine, security. All `.available = False`, disabled by
  default, warn on enable.

P2 — Honesty
------------
- MODULE_STATUS documents every module.
- Config sections: RLConfig / CompetitionConfig / FederatedConfig /
  SecurityConfig are declarative and consumed (or explicitly flagged as
  placeholder config).

P3 — Production readiness
-------------------------
- Lifecycle: await start(), await shutdown(), await ready(), __aenter__/__aexit__.
- Prometheus metrics and OpenTelemetry spans (both optional).
- Embedded test suite: python3 harvester.py --test.
- --status prints module maturity.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import inspect
import json
import math
import os
import random
import sys
import unittest
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies (guarded)
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
    _TRACER = trace.get_tracer("photosynthetic_harvester")
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
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger(__name__)

try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPSource  # type: ignore
    TOKEN_MANAGER_AVAILABLE = True
except ImportError:
    EcoATPTokenManager = None  # type: ignore

    class EcoATPSource:  # type: ignore
        RENEWABLE_ENERGY = "renewable_energy"
        GRADIENT_CONVERSION = "gradient_conversion"

    TOKEN_MANAGER_AVAILABLE = False


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "harvester_core":        "stable",
    "pigment_array":         "stable",
    "reaction_center":       "stable",
    "event_bus":             "stable",
    "task_manager":          "stable",
    "circuit_breaker":       "stable",
    "persistence":           "experimental",
    "genetic_optimizer":     "experimental",
    "mopd":                  "experimental",
    "safety_monitor":        "experimental",
    "xai":                   "experimental",
    "health_monitor":        "experimental",
    "self_healer":           "experimental",
    "quantum_distillation":  "placeholder",
    "causal_rl":             "placeholder",
    "rl":                    "placeholder",
    "federated":             "placeholder",
    "precision":             "placeholder",
    "carbon_market":         "placeholder",
    "chaos":                 "placeholder",
    "human_approval":        "placeholder",
    "websocket":             "placeholder",
    "swarm_coordinator":     "placeholder",
    "competition_engine":    "placeholder",
    "security":              "placeholder",
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
# SECTION 2. TRACING + ASYNC HELPERS
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


def _is_awaitable(x: Any) -> bool:
    return inspect.isawaitable(x)


async def _await_maybe(r: Any) -> Any:
    if _is_awaitable(r):
        return await r
    return r


# =============================================================================
# SECTION 3. EXCEPTIONS
# =============================================================================
class HarvesterError(Exception):
    pass


class ConfigError(HarvesterError):
    pass


class PersistenceError(HarvesterError):
    pass


class CircuitBreakerOpenError(HarvesterError):
    pass


# =============================================================================
# SECTION 4. ENUMS AND SHARED DATA
# =============================================================================
class HarvestingMode(Enum):
    FULL = "full"
    ADAPTIVE = "adaptive"
    MODULATED = "modulated"
    CONSERVATIVE = "conservative"
    MINIMAL = "minimal"
    SURVIVAL = "survival"


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class PigmentHealth:
    pigment_name: str
    state: str = "active"
    efficiency: float = 1.0
    damage_accumulation: float = 0.0
    repair_progress: float = 0.0
    total_excitations: int = 0
    recovery_rate: float = 0.01
    last_repair: Optional[datetime] = None

    def repair(self, rate: Optional[float] = None) -> None:
        r = rate if rate is not None else self.recovery_rate
        self.damage_accumulation = max(0.0, self.damage_accumulation - r)
        self.efficiency = max(0.1, 1.0 - self.damage_accumulation)
        self.last_repair = datetime.now(timezone.utc)

    def apply_damage(self, amount: float) -> None:
        self.damage_accumulation = min(1.0, self.damage_accumulation + amount)
        self.efficiency = max(0.1, 1.0 - self.damage_accumulation)


@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    energy_output: float
    pigment_health: float
    longterm_efficiency: float
    resource_usage: float
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
# SECTION 5. CONFIGURATION (pure dataclasses)
# =============================================================================
@dataclass
class PigmentConfig:
    default_repair_rate: float = 0.01
    damage_threshold: float = 0.8
    photoinhibition_rate: float = 0.01
    safe_excitation_level: float = 0.7
    saturation_threshold: float = 0.95
    prediction_cache_ttl_seconds: float = 30.0


@dataclass
class ReactionCenterConfig:
    base_quantum_efficiency: float = 0.85
    min_efficiency: float = 0.3
    max_efficiency: float = 0.98
    demand_modulation_enabled: bool = True
    token_abundance_threshold: float = 50000.0
    token_scarcity_threshold: float = 5000.0
    demand_response_factor: float = 0.5
    repair_rate: float = 0.005


@dataclass
class HealthConfig:
    efficiency_warning_threshold: float = 0.6
    efficiency_critical_threshold: float = 0.3
    damage_warning_threshold: float = 0.4
    damage_critical_threshold: float = 0.7
    max_healing_attempts: int = 3


@dataclass
class RLConfig:
    """STATUS: placeholder. Bounds for the CausalRL placeholder's Q-table."""
    enabled: bool = False
    state_dim: int = 12
    action_dim: int = 6
    learning_rate: float = 0.1
    gamma: float = 0.99
    epsilon: float = 0.1
    q_table_max_size: int = 5000


@dataclass
class CausalRLConfig:
    """STATUS: placeholder. Configuration for the CausalRL placeholder."""
    enabled: bool = False
    causal_mask: Optional[List[List[int]]] = None
    use_causal_discovery: bool = False


@dataclass
class GeneticConfig:
    population_size: int = 20
    mutation_rate: float = 0.2
    crossover_rate: float = 0.7
    generations: int = 5
    tournament_size: int = 3
    evolution_interval: int = 86400


@dataclass
class CompetitionConfig:
    """STATUS: placeholder. The competition engine is a no-op."""
    enabled: bool = False
    interval: int = 3600
    replacement_threshold: float = 0.3
    max_children: int = 10
    excitation_budget: float = 1000.0


@dataclass
class SwarmConfig:
    """STATUS: placeholder. The swarm coordinator is a no-op."""
    enabled: bool = False
    redis_url: Optional[str] = None
    update_interval: int = 120
    channel_prefix: str = "harvester_swarm"


@dataclass
class FederatedConfig:
    """STATUS: placeholder. Federated learning sends no updates."""
    enabled: bool = False
    model_keys: List[str] = field(default_factory=lambda: ["mopd_weights", "rl_q_table"])
    update_interval: int = 300
    aggregation_method: str = "fedavg"


@dataclass
class WebSocketConfig:
    """STATUS: placeholder. No WebSocket server is started."""
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8765
    auth_token: Optional[str] = None
    jwt_secret: Optional[str] = None
    tls_enabled: bool = False
    tls_cert: Optional[str] = None
    tls_key: Optional[str] = None
    rate_limit_per_minute: int = 60
    stream_interval: float = 1.0


@dataclass
class PersistenceConfig:
    enable: bool = True
    backend: str = "memory"  # memory | file
    retention_days: int = 30
    checkpoint_interval: int = 300
    base_dir: str = "./harvester_data"
    cache_max_entries: int = 1000


@dataclass
class SecurityConfig:
    """STATUS: placeholder. No live auth backend is wired."""
    level: str = "STANDARD"
    jwt_secret: Optional[str] = None
    rate_limit_max_requests: int = 100
    rate_limit_window: int = 60


@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "energy_output": 0.4,
        "pigment_health": 0.3,
        "longterm_efficiency": 0.2,
        "resource_usage": 0.1,
    })
    grid_resolution: int = 5


@dataclass
class QuantumConfig:
    """STATUS: placeholder. Quantum distillation is a no-op."""
    enabled: bool = False
    backend: str = "simulator"
    shots: int = 1024
    optimization_cycles: int = 10


@dataclass
class SafetyConfig:
    enabled: bool = True
    max_pigment_damage: float = 0.9
    min_efficiency: float = 0.1
    max_children: int = 20


@dataclass
class XAIConfig:
    enabled: bool = True


@dataclass
class PrecisionConfig:
    """STATUS: placeholder. Nothing consumes the returned precision."""
    enabled: bool = False
    policy: str = "energy_aware"


@dataclass
class CarbonMarketConfig:
    """STATUS: placeholder. No live carbon market is wired."""
    enabled: bool = False
    provider_url: Optional[str] = None
    contract_address: Optional[str] = None
    private_key: Optional[str] = None


@dataclass
class ChaosConfig:
    """STATUS: placeholder. Chaos injection is a no-op."""
    enabled: bool = False
    probability: float = 0.0


@dataclass
class HumanApprovalConfig:
    """STATUS: placeholder. Requests deny by default."""
    enabled: bool = False
    approval_timeout: float = 60.0


@dataclass
class HarvesterConfig:
    harvester_id: str = "primary"
    latitude: float = 0.0
    longitude: float = 0.0
    enable_prometheus: bool = False
    prometheus_port: int = 8000
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    circuit_breaker_half_open_attempts: int = 3
    shutdown_timeout_seconds: int = 15

    pigment: PigmentConfig = field(default_factory=PigmentConfig)
    reaction_center: ReactionCenterConfig = field(default_factory=ReactionCenterConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    rl: RLConfig = field(default_factory=RLConfig)
    causal_rl: CausalRLConfig = field(default_factory=CausalRLConfig)
    genetic: GeneticConfig = field(default_factory=GeneticConfig)
    competition: CompetitionConfig = field(default_factory=CompetitionConfig)
    swarm: SwarmConfig = field(default_factory=SwarmConfig)
    federated: FederatedConfig = field(default_factory=FederatedConfig)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    quantum: QuantumConfig = field(default_factory=QuantumConfig)
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    xai: XAIConfig = field(default_factory=XAIConfig)
    precision: PrecisionConfig = field(default_factory=PrecisionConfig)
    carbon_market: CarbonMarketConfig = field(default_factory=CarbonMarketConfig)
    chaos: ChaosConfig = field(default_factory=ChaosConfig)
    human_approval: HumanApprovalConfig = field(default_factory=HumanApprovalConfig)

    # Experimental toggles (harmless, on by default)
    enable_genetic_optimizer: bool = True
    enable_safety_monitor: bool = True

    def validate(self) -> List[str]:
        issues: List[str] = []
        if not (-90 <= self.latitude <= 90):
            issues.append("latitude must be between -90 and 90")
        if not (-180 <= self.longitude <= 180):
            issues.append("longitude must be between -180 and 180")
        total = sum(self.mopd.objective_weights.values())
        if abs(total - 1.0) > 1e-6:
            issues.append("mopd.objective_weights must sum to 1")
        if not (0.0 <= self.chaos.probability <= 1.0):
            issues.append("chaos.probability must be in [0, 1]")
        if self.pigment.saturation_threshold <= self.pigment.safe_excitation_level:
            issues.append("pigment.saturation_threshold must exceed safe_excitation_level")
        if self.rl.q_table_max_size < 10:
            issues.append("rl.q_table_max_size must be >= 10")
        if self.competition.max_children < 0:
            issues.append("competition.max_children must be >= 0")
        if self.security.rate_limit_max_requests < 1:
            issues.append("security.rate_limit_max_requests must be >= 1")
        return issues

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def _sub_types(cls) -> Dict[str, Any]:
        return {
            "pigment": PigmentConfig,
            "reaction_center": ReactionCenterConfig,
            "health": HealthConfig,
            "rl": RLConfig,
            "causal_rl": CausalRLConfig,
            "genetic": GeneticConfig,
            "competition": CompetitionConfig,
            "swarm": SwarmConfig,
            "federated": FederatedConfig,
            "websocket": WebSocketConfig,
            "persistence": PersistenceConfig,
            "security": SecurityConfig,
            "mopd": MOPDConfig,
            "quantum": QuantumConfig,
            "safety": SafetyConfig,
            "xai": XAIConfig,
            "precision": PrecisionConfig,
            "carbon_market": CarbonMarketConfig,
            "chaos": ChaosConfig,
            "human_approval": HumanApprovalConfig,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HarvesterConfig":
        data = dict(data or {})
        sub_types = cls._sub_types()
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k in sub_types and isinstance(v, dict):
                sub_cls = sub_types[k]
                kwargs[k] = sub_cls(**{
                    sk: sv for sk, sv in v.items() if sk in sub_cls.__dataclass_fields__
                })
            elif k in cls.__dataclass_fields__:
                kwargs[k] = v
        return cls(**kwargs)

    @classmethod
    def from_yaml(cls, path: str) -> "HarvesterConfig":
        if not YAML_AVAILABLE:
            raise ConfigError("PyYAML not installed")
        with open(path, "r") as f:
            return cls.from_dict(yaml.safe_load(f) or {})


# =============================================================================
# SECTION 6. CIRCUIT BREAKER (STABLE)
# =============================================================================
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0, half_open_attempts: int = 3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_attempts = half_open_attempts
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure: Optional[datetime] = None
        self._half_open_count = 0
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def call(self, func: Callable, *args, **kwargs):
        lock = self._get_lock()
        async with lock:
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure and (
                    datetime.now(timezone.utc) - self._last_failure
                ).total_seconds() > self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_count = 0
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                if self._half_open_count >= self.half_open_attempts:
                    self._state = CircuitBreakerState.OPEN
                    self._last_failure = datetime.now(timezone.utc)
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker {self.name} half-open exceeded"
                    )

        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with lock:
                self._failure_count += 1
                self._last_failure = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                elif self._state == CircuitBreakerState.HALF_OPEN:
                    self._half_open_count += 1
            raise

        async with lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0
        return result

    @property
    def state(self) -> CircuitBreakerState:
        return self._state

    def snapshot(self) -> Dict[str, Any]:
        return {"name": self.name, "state": self._state.value,
                "failures": self._failure_count}


# =============================================================================
# SECTION 7. TASK MANAGER + EVENT BUS (STABLE)
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._drained = False

    def start_task(self, name: str, coro_func: Callable,
                   *args, **kwargs) -> Optional[asyncio.Task]:
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
        self.stats = {"published": 0, "processed": 0, "errors": 0}
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
                for cb in list(self.subscribers.get(event.event_type, [])):
                    try:
                        r = cb(event)
                        if _is_awaitable(r):
                            await r
                    except Exception as e:
                        async with self._get_lock():
                            self.stats["errors"] += 1
                        logger.error("Event handler error", error=str(e))
                async with self._get_lock():
                    self.stats["processed"] += 1
            finally:
                self.queue.task_done()


# =============================================================================
# SECTION 8. PLACEHOLDERS (safe no-ops, disabled by default)
# =============================================================================
class CausalRLAgentPlaceholder:
    """STATUS: placeholder. Discretized, LRU-bounded, uniform policy."""
    STATUS = "placeholder"

    def __init__(self, state_dim: int, action_dim: int, max_q_table: int = 5000,
                 learning_rate: float = 0.1, gamma: float = 0.99,
                 epsilon: float = 0.1, enabled: bool = False):
        if enabled:
            _warn_module("causal_rl")
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.max_q_table = max_q_table
        self.q_table: "OrderedDict[Tuple[int, ...], np.ndarray]" = OrderedDict()
        self.epsilon = epsilon
        self.learning_rate = learning_rate
        self.gamma = gamma
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


class QuantumDistillationModulePlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("quantum_distillation")
        self.available = False

    async def optimize(self, parameters: Dict[str, float]) -> Dict[str, float]:
        return dict(parameters)

    def is_available(self) -> bool:
        return False


class FederatedCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, manager: Any, queue: Optional[Any] = None,
                 config: Optional[FederatedConfig] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.manager = manager
        self.queue = queue
        self.config = config or FederatedConfig()
        self.available = False

    async def send_update(self) -> bool:
        return False

    async def receive_global_model(self, model_json: str) -> bool:
        return False

    def describe(self) -> Dict[str, Any]:
        return {
            "available": False,
            "aggregation_method": self.config.aggregation_method,
            "model_keys": list(self.config.model_keys),
        }


class PrecisionControllerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, config: Optional[PrecisionConfig] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("precision")
        self.config = config or PrecisionConfig()
        self.policy = self.config.policy
        self.available = False

    def get_precision(self, load: float, energy_budget: float) -> str:
        return "float32"


class CarbonMarketClientPlaceholder:
    STATUS = "placeholder"

    def __init__(self, config: Optional[CarbonMarketConfig] = None,
                 enabled: bool = False, **kwargs: Any):
        if enabled:
            _warn_module("carbon_market")
        self.config = config or CarbonMarketConfig()
        self.available = False

    def buy_credits(self, amount: float) -> bool:
        return False

    def sell_credits(self, amount: float) -> bool:
        return False


class ChaosInjectorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, manager: Any, config: Optional[ChaosConfig] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.manager = manager
        self.config = config or ChaosConfig()
        self.chaos_probability = self.config.probability
        self.available = False

    async def maybe_inject_failure(self) -> None:
        return None


class HumanApprovalHandlerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, queue: Optional[Any] = None,
                 config: Optional[HumanApprovalConfig] = None,
                 enabled: bool = False, auto_approve_dev: bool = False):
        if enabled:
            _warn_module("human_approval")
        self.queue = queue
        self.config = config or HumanApprovalConfig()
        self.available = False
        self.auto_approve_dev = auto_approve_dev
        if auto_approve_dev:
            logger.warning(
                "HumanApproval auto_approve_dev=True; do not use in production."
            )

    async def request_approval(self, decision: Dict[str, Any],
                               timeout: Optional[float] = None) -> bool:
        if self.auto_approve_dev:
            return True
        return False


class WebSocketServerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, config: Optional[WebSocketConfig] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("websocket")
        self.config = config or WebSocketConfig()
        self.available = False
        self.running = False

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def broadcast(self, message: Dict[str, Any]) -> None:
        return None


class SwarmCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, config: Optional[SwarmConfig] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("swarm_coordinator")
        self.config = config or SwarmConfig()
        self.available = False

    async def share(self, data: Dict[str, Any]) -> None:
        return None


class CompetitionEnginePlaceholder:
    STATUS = "placeholder"

    def __init__(self, config: Optional[CompetitionConfig] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("competition_engine")
        self.config = config or CompetitionConfig()
        self.available = False

    async def run_competition(self) -> Dict[str, Any]:
        return {"replaced": 0, "available": False}


class SecurityServicePlaceholder:
    """STATUS: placeholder. No auth backend is active."""
    STATUS = "placeholder"

    def __init__(self, config: Optional[SecurityConfig] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("security")
        self.config = config or SecurityConfig()
        self.available = False
        self.level = self.config.level

    def check_token(self, token: Optional[str]) -> bool:
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

    def explain_harvest(self, eco_atp: float, efficiency: float, mode: str) -> str:
        return (
            f"Harvested {eco_atp:.3f} eco-ATP at efficiency {efficiency:.3f} "
            f"in mode {mode}."
        )

    def explain_mode(self, mode: str) -> str:
        return f"Mode set to {mode}."

    def explain_healing(self, issue_type: str, attempt: int) -> str:
        return f"Healing applied for {issue_type} (attempt {attempt})."


class HealthMonitor:
    STATUS = "experimental"

    def __init__(self, config: HarvesterConfig, harvester_id: str,
                 enabled: bool = True):
        if enabled:
            _warn_module("health_monitor")
        self.config = config
        self.harvester_id = harvester_id
        self.available = True
        self.metrics: Dict[str, Any] = {}
        self.recommendations: List[Dict[str, Any]] = []
        self._prom = self._setup_metrics()

    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE:
            class _Noop:
                def set(self, *a, **k): pass
                def inc(self, *a, **k): pass
                def observe(self, *a, **k): pass
            return {"harvesting_rate": _Noop(), "pigment_health": _Noop(),
                    "mode_transitions": _Noop()}
        try:
            hid = self.harvester_id
            return {
                "harvesting_rate": Gauge(f"harvester_rate_{hid}", "Harvesting rate"),
                "pigment_health": Gauge(f"pigment_health_{hid}", "Pigment health"),
                "mode_transitions": Counter(f"mode_transitions_{hid}",
                                             "Mode transitions"),
            }
        except Exception as e:
            logger.warning("Prometheus setup failed; using no-op metrics",
                           error=str(e))
            class _Noop:
                def set(self, *a, **k): pass
                def inc(self, *a, **k): pass
                def observe(self, *a, **k): pass
            return {"harvesting_rate": _Noop(), "pigment_health": _Noop(),
                    "mode_transitions": _Noop()}

    def collect_metrics(self, harvester_state: Dict[str, Any]) -> Dict[str, Any]:
        self.metrics = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "harvester_id": self.harvester_id,
            "total_harvested": harvester_state.get("total_harvested", 0.0),
            "harvest_cycles": harvester_state.get("harvest_cycles", 0),
            "efficiency": harvester_state.get("efficiency", 0.0),
            "mode": harvester_state.get("mode", "unknown"),
            "pigment_health": harvester_state.get("pigment_health", {}),
        }
        ph = self.metrics["pigment_health"]
        self.metrics["overall_health"] = (
            float(np.mean([h.get("efficiency", 1.0) for h in ph.values()]))
            if ph else 1.0
        )
        try:
            self._prom["harvesting_rate"].set(self.metrics["total_harvested"])
            self._prom["mode_transitions"].inc()
        except Exception:
            pass
        self.recommendations = self._generate_recommendations(harvester_state)
        return self.metrics.copy()

    def _generate_recommendations(self, harvester_state: Dict[str, Any]
                                   ) -> List[Dict[str, Any]]:
        recs: List[Dict[str, Any]] = []
        eff = harvester_state.get("efficiency", 1.0)
        if eff < self.config.health.efficiency_warning_threshold:
            recs.append({"type": "efficiency", "severity": "medium",
                         "message": "Efficiency below warning threshold"})
        if eff < self.config.health.efficiency_critical_threshold:
            recs.append({"type": "efficiency_collapse", "severity": "high",
                         "message": "Efficiency critical"})
        overall = self.metrics.get("overall_health", 1.0)
        if overall < self.config.health.damage_warning_threshold:
            recs.append({"type": "photoinhibition", "severity": "medium",
                         "message": "Pigment health below warning threshold"})
        if overall < self.config.health.damage_critical_threshold:
            recs.append({"type": "photoinhibition", "severity": "high",
                         "message": "Pigment health critical"})
        return recs

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.copy()

    def get_recommendations(self) -> List[Dict[str, Any]]:
        return self.recommendations.copy()


class SelfHealer:
    STATUS = "experimental"

    def __init__(self, harvester: Any, config: HarvesterConfig,
                 enabled: bool = True):
        if enabled:
            _warn_module("self_healer")
        self.harvester = harvester
        self.config = config
        self.available = True
        self.healing_attempts: Dict[str, int] = {}
        self.max_attempts = config.health.max_healing_attempts

    async def apply_healing(self, issue_type: str) -> bool:
        attempts = self.healing_attempts.get(issue_type, 0)
        if attempts >= self.max_attempts:
            return False
        try:
            if issue_type == "photoinhibition":
                for h in self.harvester.pigments.pigment_health.values():
                    h.recovery_rate *= 1.5
                    h.repair()
            elif issue_type == "efficiency_collapse":
                rc = self.harvester.reaction_center
                rc.cumulative_damage = max(0.0, rc.cumulative_damage - 0.1)
                rc.current_efficiency = float(np.clip(
                    rc.base_quantum_efficiency * (1.0 - rc.cumulative_damage),
                    rc.min_efficiency, rc.max_efficiency,
                ))
            else:
                return False
            self.healing_attempts[issue_type] = attempts + 1
            return True
        except Exception as e:
            logger.warning("Healing failed", issue_type=issue_type, error=str(e))
            return False


# =============================================================================
# SECTION 10. PERSISTENCE BACKENDS (STABLE)
# =============================================================================
class PersistenceBackend:
    async def save(self, key: str, data: Any) -> bool:
        raise NotImplementedError

    async def load(self, key: str) -> Optional[Any]:
        raise NotImplementedError

    async def delete(self, key: str) -> bool:
        raise NotImplementedError


class MemoryBackend(PersistenceBackend):
    def __init__(self, max_entries: int = 1000):
        self._store: "OrderedDict[str, Any]" = OrderedDict()
        self._max = max_entries

    async def save(self, key: str, data: Any) -> bool:
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = data
        if len(self._store) > self._max:
            self._store.popitem(last=False)
        return True

    async def load(self, key: str) -> Optional[Any]:
        return self._store.get(key)

    async def delete(self, key: str) -> bool:
        return self._store.pop(key, None) is not None


class FileBackend(PersistenceBackend):
    def __init__(self, base_dir: str, max_cache_entries: int = 1000):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._cache: "OrderedDict[str, Any]" = OrderedDict()
        self._max_cache = max_cache_entries
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _path(self, key: str) -> Path:
        return self.base_dir / f"{key}.json"

    def _json_default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if hasattr(obj, "__dict__"):
            return {k: v for k, v in obj.__dict__.items()
                    if not k.startswith("_")}
        return str(obj)

    def _save_sync(self, key: str, data: Any) -> None:
        with open(self._path(key), "w") as f:
            json.dump({"version": "1.0", "data": data}, f,
                      default=self._json_default)

    def _load_sync(self, key: str) -> Optional[Any]:
        p = self._path(key)
        if not p.exists():
            return None
        with open(p, "r") as f:
            return json.load(f).get("data")

    async def save(self, key: str, data: Any) -> bool:
        async with self._get_lock():
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._save_sync, key, data)
                self._cache[key] = data
                self._cache.move_to_end(key)
                if len(self._cache) > self._max_cache:
                    self._cache.popitem(last=False)
                return True
            except Exception as e:
                logger.error("File save failed", key=key, error=str(e))
                return False

    async def load(self, key: str) -> Optional[Any]:
        async with self._get_lock():
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            try:
                loop = asyncio.get_running_loop()
                data = await loop.run_in_executor(None, self._load_sync, key)
                if data is not None:
                    self._cache[key] = data
                    self._cache.move_to_end(key)
                    if len(self._cache) > self._max_cache:
                        self._cache.popitem(last=False)
                return data
            except Exception as e:
                logger.error("File load failed", key=key, error=str(e))
                return None

    async def delete(self, key: str) -> bool:
        async with self._get_lock():
            try:
                p = self._path(key)
                if p.exists():
                    p.unlink()
                self._cache.pop(key, None)
                return True
            except Exception:
                return False


# =============================================================================
# SECTION 11. GENETIC OPTIMIZER (NSGA-II, genome-dependent)
# =============================================================================
class HarvesterGeneticOptimizer:
    """
    NSGA-II over pigment and reaction-center parameters.

    Genome (bounded, all matter):
      sensitivity_multiplier   shapes pigment health under excitation
      repair_rate              shapes pigment recovery
      demand_response_factor   shapes long-term efficiency
      conversion_multiplier    shapes energy output

    Objectives:
      energy_output / pigment_health / longterm_efficiency / resource_usage
    """
    STATUS = "experimental"

    PARAM_BOUNDS = {
        "sensitivity_multiplier": (0.5, 2.0),
        "repair_rate": (0.001, 0.05),
        "demand_response_factor": (0.1, 1.0),
        "conversion_multiplier": (0.5, 1.5),
    }

    def __init__(self, harvester: Any, config: HarvesterConfig,
                 enabled: bool = True):
        if enabled:
            _warn_module("genetic_optimizer")
        self.harvester = harvester
        self.config = config
        self.available = True
        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness: float = -math.inf
        self.pareto_front: List[MOPDPoint] = []
        self.evolution_history: List[Dict[str, Any]] = []
        self._lock: Optional[asyncio.Lock] = None
        self._cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _init_individual(self) -> Dict[str, float]:
        return {k: random.uniform(lo, hi)
                for k, (lo, hi) in self.PARAM_BOUNDS.items()}

    def _init_population(self) -> List[Dict[str, float]]:
        return [self._init_individual()
                for _ in range(self.config.genetic.population_size)]

    async def _evaluate(self, ind: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted((k, float(v)) for k, v in ind.items()))
        if key in self._cache:
            return self._cache[key]

        eff = float(self.harvester.reaction_center.current_efficiency)
        damage = float(self.harvester.reaction_center.cumulative_damage)
        pigment_health_vals = [
            h.efficiency for h in self.harvester.pigments.pigment_health.values()
        ]
        mean_health = float(np.mean(pigment_health_vals)) if pigment_health_vals else 1.0

        sens_penalty = max(0.0, ind["sensitivity_multiplier"] - 1.0) * 0.2
        energy_output = float(np.clip(eff * ind["conversion_multiplier"], 0.0, 1.0))
        pigment_health = float(np.clip(
            mean_health + ind["repair_rate"] * 5.0 - sens_penalty, 0.0, 1.0
        ))
        longterm_efficiency = float(np.clip(
            eff * ind["demand_response_factor"], 0.0, 1.0
        ))
        resource_usage = float(np.clip(1.0 - damage - sens_penalty, 0.0, 1.0))

        objs = {
            "energy_output": energy_output,
            "pigment_health": pigment_health,
            "longterm_efficiency": longterm_efficiency,
            "resource_usage": resource_usage,
        }
        self._cache[key] = objs
        return objs

    def _dominates(self, a: Dict[str, float], b: Dict[str, float]) -> bool:
        keys = ("energy_output", "pigment_health",
                "longterm_efficiency", "resource_usage")
        return all(a[k] >= b[k] for k in keys) and any(a[k] > b[k] for k in keys)

    def _fast_non_dominated_sort(self, objs: List[Dict[str, float]]
                                  ) -> List[List[int]]:
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

    def _crowding(self, front: List[int],
                  objs: List[Dict[str, float]]) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}
        d: Dict[int, float] = {i: 0.0 for i in front}
        for k in ("energy_output", "pigment_health",
                  "longterm_efficiency", "resource_usage"):
            sf = sorted(front, key=lambda i: objs[i][k])
            d[sf[0]] = float("inf")
            d[sf[-1]] = float("inf")
            span = objs[sf[-1]][k] - objs[sf[0]][k]
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                d[sf[idx]] += (objs[sf[idx + 1]][k] - objs[sf[idx - 1]][k]) / span
        return d

    def _crossover(self, p1: Dict[str, float],
                   p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for k, (lo, hi) in self.PARAM_BOUNDS.items():
            if random.random() < 0.5:
                child[k] = p1[k]
            else:
                child[k] = p2[k]
            if random.random() < 0.3:
                child[k] = (p1[k] + p2[k]) / 2.0
            child[k] = max(lo, min(hi, child[k]))
        return child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        m = dict(ind)
        for k, (lo, hi) in self.PARAM_BOUNDS.items():
            if random.random() < self.config.genetic.mutation_rate:
                span = hi - lo
                m[k] = max(lo, min(hi,
                                    m[k] + random.uniform(-0.1 * span, 0.1 * span)))
        return m

    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        gens = generations or self.config.genetic.generations
        async with self._get_lock():
            pop = self._init_population()
            objs = [await self._evaluate(i) for i in pop]
            local_front: List[MOPDPoint] = []

            for _ in range(gens):
                offspring: List[Dict[str, float]] = []
                while len(offspring) < self.config.genetic.population_size:
                    i = random.randrange(len(pop))
                    j = random.randrange(len(pop))
                    if random.random() < self.config.genetic.crossover_rate:
                        c = self._crossover(pop[i], pop[j])
                    else:
                        c = dict(pop[i])
                    offspring.append(self._mutate(c))
                offspring = offspring[: self.config.genetic.population_size]
                off_objs = [await self._evaluate(o) for o in offspring]

                combined = pop + offspring
                combined_objs = objs + off_objs
                fronts = self._fast_non_dominated_sort(combined_objs)

                if fronts:
                    local_front = [
                        MOPDPoint(
                            individual=dict(combined[idx]),
                            energy_output=combined_objs[idx]["energy_output"],
                            pigment_health=combined_objs[idx]["pigment_health"],
                            longterm_efficiency=combined_objs[idx]["longterm_efficiency"],
                            resource_usage=combined_objs[idx]["resource_usage"],
                        )
                        for idx in fronts[0]
                    ]

                new_pop: List[Dict[str, float]] = []
                new_objs: List[Dict[str, float]] = []
                for front in fronts:
                    if (len(new_pop) + len(front)
                            <= self.config.genetic.population_size):
                        for idx in front:
                            new_pop.append(combined[idx])
                            new_objs.append(combined_objs[idx])
                    else:
                        cd = self._crowding(front, combined_objs)
                        sf = sorted(front, key=lambda i: cd.get(i, 0.0),
                                    reverse=True)
                        remaining = (self.config.genetic.population_size
                                     - len(new_pop))
                        for idx in sf[:remaining]:
                            new_pop.append(combined[idx])
                            new_objs.append(combined_objs[idx])
                        break
                pop, objs = new_pop, new_objs

            self.pareto_front = local_front

            weights = dict(self.config.mopd.objective_weights)
            if local_front:
                keys = list(weights.keys())
                max_vals = {k: max(getattr(p, k) for p in local_front)
                            for k in keys}
                min_vals = {k: min(getattr(p, k) for p in local_front)
                            for k in keys}
                ranges = {k: (max_vals[k] - min_vals[k])
                          if max_vals[k] != min_vals[k] else 1.0
                          for k in keys}
                best_score = -math.inf
                best_point: Optional[MOPDPoint] = None
                for p in local_front:
                    s = sum(weights[k] * ((getattr(p, k) - min_vals[k]) / ranges[k])
                            for k in keys)
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
            "available": self.available,
            "best_fitness": self.best_fitness,
            "pareto_front_size": len(self.pareto_front),
            "history": self.evolution_history[-10:],
        }


# =============================================================================
# SECTION 12. PIGMENT ARRAY (STABLE core)
# =============================================================================
class EnhancedPigmentArray:
    def __init__(self, config: HarvesterConfig, task_manager: TaskManager,
                 event_bus: EventBus):
        self.config = config
        self.task_manager = task_manager
        self.event_bus = event_bus

        self.pigments: Dict[str, Dict[str, Any]] = {
            "chlorophyll_a": {
                "target": "renewable_availability",
                "base_sensitivity": 1.0,
                "sensitivity": 1.0,
                "safe_excitation_level": config.pigment.safe_excitation_level,
                "saturation_threshold": config.pigment.saturation_threshold,
                "repair_rate": config.pigment.default_repair_rate,
                "energy_conversion_factor": 0.01,
                "specialization": "solar",
            },
            "chlorophyll_b": {
                "target": "carbon_intensity",
                "base_sensitivity": 0.8,
                "sensitivity": 0.8,
                "safe_excitation_level": 0.8,
                "saturation_threshold": 0.95,
                "repair_rate": config.pigment.default_repair_rate * 1.5,
                "energy_conversion_factor": 0.001,
                "specialization": "carbon",
            },
            "carotenoids": {
                "target": "waste_heat",
                "base_sensitivity": 0.6,
                "sensitivity": 0.6,
                "safe_excitation_level": 0.9,
                "saturation_threshold": 1.0,
                "repair_rate": config.pigment.default_repair_rate * 2.0,
                "energy_conversion_factor": 0.01,
                "specialization": "thermal",
            },
        }
        self._pigment_names = list(self.pigments.keys())
        self.pigment_health: Dict[str, PigmentHealth] = {
            name: PigmentHealth(
                pigment_name=name,
                recovery_rate=self.pigments[name]["repair_rate"],
            )
            for name in self._pigment_names
        }
        self.excitation_history: Dict[str, Deque[float]] = {
            name: deque(maxlen=500) for name in self._pigment_names
        }
        self._health_lock: Optional[asyncio.Lock] = None
        self._history_lock: Optional[asyncio.Lock] = None

    def _get_health_lock(self) -> asyncio.Lock:
        if self._health_lock is None:
            self._health_lock = asyncio.Lock()
        return self._health_lock

    def _get_history_lock(self) -> asyncio.Lock:
        if self._history_lock is None:
            self._history_lock = asyncio.Lock()
        return self._history_lock

    async def sense_environment(self, environmental_data: Dict[str, float]
                                 ) -> Dict[str, float]:
        excitations: Dict[str, float] = {}
        async with self._get_health_lock():
            for name in self._pigment_names:
                p = self.pigments[name]
                raw = float(environmental_data.get(p["target"], 0.0))
                health = self.pigment_health[name]
                effective_raw = raw * p["sensitivity"] * health.efficiency
                # --- P0 fix: check damage against RAW, before clipping ---
                if effective_raw > p["safe_excitation_level"]:
                    excess = effective_raw - p["safe_excitation_level"]
                    damage = excess * self.config.pigment.photoinhibition_rate
                    health.apply_damage(damage)
                else:
                    health.repair()
                reported = float(np.clip(
                    effective_raw, 0.0, p["saturation_threshold"]
                ))
                health.total_excitations += 1
                async with self._get_history_lock():
                    self.excitation_history[name].append(reported)
                excitations[name] = reported
        return excitations

    async def repair_loop(self) -> None:
        while True:
            try:
                async with self._get_health_lock():
                    for h in self.pigment_health.values():
                        if h.damage_accumulation > 0:
                            h.repair()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Repair loop error", error=str(e))
                await asyncio.sleep(30)

    def get_health_summary(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: {
                "damage": h.damage_accumulation,
                "efficiency": h.efficiency,
                "state": h.state,
            }
            for name, h in self.pigment_health.items()
        }

    def get_average_health(self) -> float:
        vals = [h.efficiency for h in self.pigment_health.values()]
        return float(np.mean(vals)) if vals else 1.0


# =============================================================================
# SECTION 13. REACTION CENTER (STABLE core)
# =============================================================================
class EnhancedReactionCenter:
    def __init__(self, config: HarvesterConfig, task_manager: TaskManager,
                 token_manager: Optional[Any] = None,
                 gradient_manager: Optional[Any] = None,
                 event_bus: Optional[EventBus] = None):
        self.config = config
        self.task_manager = task_manager
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.event_bus = event_bus

        self.base_quantum_efficiency = config.reaction_center.base_quantum_efficiency
        self.current_efficiency = config.reaction_center.base_quantum_efficiency
        self.min_efficiency = config.reaction_center.min_efficiency
        self.max_efficiency = config.reaction_center.max_efficiency
        self.demand_response_factor = config.reaction_center.demand_response_factor
        self.repair_rate = config.reaction_center.repair_rate
        self.cumulative_damage = 0.0
        self.conversion_history: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def _token_balance(self) -> float:
        if self.token_manager is None:
            return 10000.0
        fn = getattr(self.token_manager, "get_system_summary", None)
        if fn is None:
            return 10000.0
        try:
            r = fn()
            if _is_awaitable(r):
                r = await r
            if isinstance(r, dict):
                return float(r.get("total_balance", 10000.0))
        except Exception:
            pass
        return 10000.0

    async def modulate_efficiency(self) -> float:
        if not self.config.reaction_center.demand_modulation_enabled:
            return self.current_efficiency
        balance = await self._token_balance()
        if balance > self.config.reaction_center.token_abundance_threshold:
            modulation = 0.9
        elif balance < self.config.reaction_center.token_scarcity_threshold:
            modulation = 1.1
        else:
            modulation = 1.0
        eff = self.base_quantum_efficiency * modulation
        eff *= (1.0 - self.cumulative_damage * 0.5)
        return float(np.clip(eff, self.min_efficiency, self.max_efficiency))

    async def _generate_tokens_safe(self, account_id: str, total: float,
                                     efficiency: float) -> float:
        if self.token_manager is None:
            return total * efficiency * 0.5
        gen_fn = getattr(self.token_manager, "generate_tokens", None)
        if gen_fn is None:
            return total * efficiency * 0.5
        try:
            r = gen_fn(
                account_id=account_id,
                source=EcoATPSource.RENEWABLE_ENERGY,
                energy_saved_kwh=total * 0.01,
                efficiency=efficiency,
            )
            if _is_awaitable(r):
                r = await r
            if isinstance(r, list):
                return float(sum(getattr(t, "value", 0.0) for t in r))
        except Exception as e:
            logger.debug("Token generation failed; using local estimate",
                         error=str(e))
        return total * efficiency * 0.5

    async def convert_excitation(self, excitations: Dict[str, float],
                                  account_id: str) -> float:
        async with self._get_lock():
            total = float(sum(excitations.values()))
            if total < 0.1:
                return 0.0
            efficiency = await self.modulate_efficiency()
            if total > 0.8:
                self.cumulative_damage += 0.0005
            elif total < 0.3:
                self.cumulative_damage = max(0.0, self.cumulative_damage - 0.0001)
            self.cumulative_damage = min(1.0, max(0.0, self.cumulative_damage))
            self.current_efficiency = efficiency

            generated = await self._generate_tokens_safe(account_id, total, efficiency)
            self.conversion_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "generated": generated,
                "efficiency": efficiency,
            })

            if self.event_bus is not None:
                await self.event_bus.publish(CoreEvent(
                    event_type="harvest_completed",
                    source="reaction_center",
                    payload={
                        "total_excitation": total,
                        "efficiency": efficiency,
                        "generated": generated,
                    },
                ))
            return generated

    async def maintenance_loop(self) -> None:
        while True:
            try:
                async with self._get_lock():
                    if self.cumulative_damage > 0:
                        repair = min(self.cumulative_damage, self.repair_rate)
                        self.cumulative_damage -= repair
                        self.current_efficiency = self.base_quantum_efficiency * (
                            1.0 - self.cumulative_damage
                        )
                        self.current_efficiency = float(np.clip(
                            self.current_efficiency, self.min_efficiency,
                            self.max_efficiency,
                        ))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Maintenance loop error", error=str(e))
                await asyncio.sleep(60)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "current_efficiency": self.current_efficiency,
            "base_quantum_efficiency": self.base_quantum_efficiency,
            "cumulative_damage": self.cumulative_damage,
        }


# =============================================================================
# SECTION 14. MAIN HARVESTER
# =============================================================================
class EnhancedPhotosyntheticHarvester:
    """
    Lifecycle:
        harvester = EnhancedPhotosyntheticHarvester(config=cfg)
        await harvester.start()
        ...
        await harvester.shutdown()
    """

    def __init__(
        self,
        config: Optional[HarvesterConfig] = None,
        token_manager: Optional[Any] = None,
        gradient_manager: Optional[Any] = None,
        message_queue: Optional[Any] = None,
    ):
        self.config = config or HarvesterConfig()
        self.harvester_id = self.config.harvester_id
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.message_queue = message_queue

        # Core subsystems
        self.event_bus = EventBus()
        self._task_manager = TaskManager()
        self.pigments = EnhancedPigmentArray(self.config, self._task_manager,
                                              self.event_bus)
        self.reaction_center = EnhancedReactionCenter(
            self.config, self._task_manager, token_manager,
            gradient_manager, self.event_bus,
        )

        # Persistence
        self.persistence: Optional[PersistenceBackend] = None
        if self.config.persistence.enable:
            backend = self.config.persistence.backend
            if backend == "file":
                self.persistence = FileBackend(
                    self.config.persistence.base_dir,
                    max_cache_entries=self.config.persistence.cache_max_entries,
                )
            else:
                self.persistence = MemoryBackend(
                    max_entries=self.config.persistence.cache_max_entries
                )

        # Experimental
        self.health_monitor = HealthMonitor(self.config, self.harvester_id,
                                             enabled=True)
        self.self_healer = SelfHealer(self, self.config, enabled=True)
        self.xai = XAIExplainer(enabled=self.config.xai.enabled)
        self.genetic_optimizer = HarvesterGeneticOptimizer(
            self, self.config, enabled=self.config.enable_genetic_optimizer
        )
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()

        # Placeholders
        self.quantum_distillation = (
            QuantumDistillationModulePlaceholder(enabled=self.config.quantum.enabled)
            if self.config.quantum.enabled else None
        )
        self.causal_rl_agent = (
            CausalRLAgentPlaceholder(
                state_dim=self.config.rl.state_dim,
                action_dim=self.config.rl.action_dim,
                max_q_table=self.config.rl.q_table_max_size,
                learning_rate=self.config.rl.learning_rate,
                gamma=self.config.rl.gamma,
                epsilon=self.config.rl.epsilon,
                enabled=self.config.rl.enabled or self.config.causal_rl.enabled,
            )
            if (self.config.rl.enabled or self.config.causal_rl.enabled) else None
        )
        self.federated_coordinator = (
            FederatedCoordinatorPlaceholder(
                self, message_queue, config=self.config.federated,
                enabled=self.config.federated.enabled,
            )
            if self.config.federated.enabled else None
        )
        self.precision_controller = (
            PrecisionControllerPlaceholder(
                config=self.config.precision,
                enabled=self.config.precision.enabled,
            )
            if self.config.precision.enabled else None
        )
        self.carbon_market = (
            CarbonMarketClientPlaceholder(
                config=self.config.carbon_market,
                enabled=self.config.carbon_market.enabled,
            )
            if self.config.carbon_market.enabled else None
        )
        self.chaos_injector = ChaosInjectorPlaceholder(
            self, config=self.config.chaos,
            enabled=self.config.chaos.enabled,
        )
        self.human_approval = (
            HumanApprovalHandlerPlaceholder(
                message_queue, config=self.config.human_approval,
                enabled=self.config.human_approval.enabled,
            )
            if self.config.human_approval.enabled else None
        )
        self.websocket_server = WebSocketServerPlaceholder(
            config=self.config.websocket,
            enabled=self.config.websocket.enabled,
        )
        self.swarm_coordinator = SwarmCoordinatorPlaceholder(
            config=self.config.swarm,
            enabled=self.config.swarm.enabled,
        )
        self.competition_engine = CompetitionEnginePlaceholder(
            config=self.config.competition,
            enabled=self.config.competition.enabled,
        )
        self.security_service = SecurityServicePlaceholder(
            config=self.config.security,
            enabled=False,
        )

        # State
        self.mode = HarvestingMode.ADAPTIVE
        self.total_harvested = 0.0
        self.harvest_cycles = 0
        self.peak_harvest_rate = 0.0
        self.is_child = False
        self.child_harvesters: Dict[str, "EnhancedPhotosyntheticHarvester"] = {}

        # Locks (lazy)
        self._state_lock: Optional[asyncio.Lock] = None
        self._child_lock: Optional[asyncio.Lock] = None

        # Lifecycle
        self._started = False
        self._shutdown = False
        self._start_time: Optional[datetime] = None

        self.account_id = f"photosynthetic_{self.harvester_id}"

        logger.info("EnhancedPhotosyntheticHarvester created (not started)",
                    id=self.harvester_id)

    # ---------------- locks ----------------
    def _get_state_lock(self) -> asyncio.Lock:
        if self._state_lock is None:
            self._state_lock = asyncio.Lock()
        return self._state_lock

    def _get_child_lock(self) -> asyncio.Lock:
        if self._child_lock is None:
            self._child_lock = asyncio.Lock()
        return self._child_lock

    # ---------------- safety ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "max_damage",
            lambda s: s.get("max_damage", 0.0) <= self.config.safety.max_pigment_damage,
            "Pigment damage too high",
        )
        self.safety_monitor.add_invariant(
            "efficiency_floor",
            lambda s: s.get("efficiency", 1.0) >= self.config.safety.min_efficiency,
            "Efficiency below safe floor",
        )
        self.safety_monitor.add_invariant(
            "child_limit",
            lambda s: s.get("child_count", 0) <= self.config.safety.max_children,
            "Too many child harvesters",
        )

    def _get_safety_state(self) -> Dict[str, Any]:
        summary = self.pigments.get_health_summary()
        max_damage = max(
            (h.get("damage", 0.0) for h in summary.values()), default=0.0
        )
        return {
            "max_damage": max_damage,
            "efficiency": self.reaction_center.current_efficiency,
            "child_count": len(self.child_harvesters),
        }

    # ---------------- lifecycle ----------------
    @traced("harvester.start")
    async def start(self) -> None:
        if self._started:
            return
        issues = self.config.validate()
        if issues:
            raise ConfigError(f"Invalid config: {issues}")

        self._start_time = datetime.now(timezone.utc)
        await self.event_bus.start()

        if self.token_manager is not None:
            create_fn = getattr(self.token_manager, "create_account", None)
            if create_fn is not None:
                try:
                    r = create_fn(self.account_id)
                    if _is_awaitable(r):
                        await r
                except Exception as e:
                    logger.debug("create_account failed", error=str(e))

        if self.persistence is not None:
            try:
                state = await self.persistence.load(f"{self.harvester_id}:state")
                if isinstance(state, dict):
                    self.total_harvested = float(state.get("total_harvested", 0.0))
                    self.harvest_cycles = int(state.get("harvest_cycles", 0))
                    self.peak_harvest_rate = float(
                        state.get("peak_harvest_rate", 0.0)
                    )
                    try:
                        self.mode = HarvestingMode(
                            state.get("mode", self.mode.value)
                        )
                    except ValueError:
                        pass
            except Exception as e:
                logger.warning("State load failed", error=str(e))

        self._task_manager.start_task("pigment_repair",
                                       self.pigments.repair_loop)
        self._task_manager.start_task("rc_maintenance",
                                       self.reaction_center.maintenance_loop)
        self._task_manager.start_task("metrics_loop", self._metrics_loop)
        self._task_manager.start_task("checkpoint_loop", self._checkpoint_loop)
        if self.config.enable_genetic_optimizer:
            self._task_manager.start_task("genetic_evolution",
                                           self._genetic_evolution_loop)

        self._started = True
        logger.info("Harvester started", id=self.harvester_id)

    async def ready(self) -> bool:
        if not self._started:
            return False
        return isinstance(self.pigments.pigment_health, dict)

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("Harvester shutting down", id=self.harvester_id)

        try:
            await asyncio.wait_for(self._task_manager.drain(timeout),
                                    timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        try:
            await asyncio.wait_for(self.event_bus.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Event bus stop timed out")

        if self.persistence is not None:
            try:
                await self._save_state()
            except Exception as e:
                logger.warning("Final state save failed", error=str(e))

        async with self._get_child_lock():
            children = list(self.child_harvesters.values())
            self.child_harvesters.clear()
        for c in children:
            try:
                await c.shutdown(timeout=timeout)
            except Exception:
                pass

        logger.info("Harvester shutdown complete", id=self.harvester_id)

    async def __aenter__(self) -> "EnhancedPhotosyntheticHarvester":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _metrics_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                stats = await self.get_harvesting_stats()
                self.health_monitor.collect_metrics(stats)
                for rec in self.health_monitor.get_recommendations():
                    if rec["severity"] == "high":
                        await self.self_healer.apply_healing(rec["type"])
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Metrics loop error", error=str(e))
                await asyncio.sleep(30)

    async def _checkpoint_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.persistence.checkpoint_interval)
                if self.persistence is not None:
                    await self._save_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Checkpoint loop error", error=str(e))
                await asyncio.sleep(60)

    async def _genetic_evolution_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.genetic.evolution_interval)
                await self.genetic_optimizer.evolve()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Genetic evolution error", error=str(e))
                await asyncio.sleep(3600)

    # ---------------- persistence ----------------
    async def _save_state(self) -> None:
        if self.persistence is None:
            return
        async with self._get_state_lock():
            state = {
                "harvester_id": self.harvester_id,
                "total_harvested": self.total_harvested,
                "harvest_cycles": self.harvest_cycles,
                "mode": self.mode.value,
                "peak_harvest_rate": self.peak_harvest_rate,
                "pigment_health": self.pigments.get_health_summary(),
                "reaction_center": self.reaction_center.get_stats(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        await self.persistence.save(f"{self.harvester_id}:state", state)

    # ---------------- harvest ----------------
    @traced("harvester.harvest_cycle")
    async def harvest_cycle(self, environmental_data: Dict[str, float]
                             ) -> Dict[str, Any]:
        if not self._started:
            raise HarvesterError("Harvester not started")

        if self.safety_monitor is not None:
            violations = self.safety_monitor.check(self._get_safety_state())
            if violations:
                logger.warning("Safety violation before harvest",
                               violations=violations)

        excitations = await self.pigments.sense_environment(environmental_data)
        generated = await self.reaction_center.convert_excitation(
            excitations, self.account_id
        )

        # --- P0 fix: _state_lock is defined lazily ---
        async with self._get_state_lock():
            self.total_harvested += generated
            self.harvest_cycles += 1
            self.peak_harvest_rate = max(self.peak_harvest_rate, generated)

        if self.config.xai.enabled:
            logger.info("XAI", text=self.xai.explain_harvest(
                generated, self.reaction_center.current_efficiency,
                self.mode.value,
            ))

        if self.carbon_market is not None and generated > 10:
            try:
                self.carbon_market.sell_credits(generated * 0.01)
            except Exception:
                pass

        return {
            "harvester_id": self.harvester_id,
            "eco_atp_generated": generated,
            "total_harvested": self.total_harvested,
            "mode": self.mode.value,
            "efficiency": self.reaction_center.current_efficiency,
        }

    async def set_mode(self, mode: HarvestingMode) -> None:
        async with self._get_state_lock():
            self.mode = mode
        if self.config.xai.enabled:
            logger.info("XAI", text=self.xai.explain_mode(mode.value))

    # ---------------- stats ----------------
    async def get_harvesting_stats(self) -> Dict[str, Any]:
        async with self._get_state_lock():
            stats: Dict[str, Any] = {
                "harvester_id": self.harvester_id,
                "total_harvested": self.total_harvested,
                "harvest_cycles": self.harvest_cycles,
                "peak_harvest_rate": self.peak_harvest_rate,
                "mode": self.mode.value,
                "efficiency": self.reaction_center.current_efficiency,
                "pigment_health": self.pigments.get_health_summary(),
                "reaction_center": self.reaction_center.get_stats(),
                "is_child": self.is_child,
                "child_harvesters": len(self.child_harvesters),
                "module_status": MODULE_STATUS,
                "genetic_optimizer": self.genetic_optimizer.get_status(),
                "mopd_enabled": self.config.mopd.enabled,
                "pareto_front_size": len(self.genetic_optimizer.pareto_front),
                "rl_enabled": self.causal_rl_agent is not None,
                "causal_rl_enabled": self.causal_rl_agent is not None,
                "federated_enabled": self.federated_coordinator is not None,
                "safety_monitor_enabled": self.safety_monitor is not None,
                "xai_enabled": self.config.xai.enabled,
                "precision_controller_enabled": self.precision_controller is not None,
                "carbon_market_enabled": self.carbon_market is not None,
                "chaos_enabled": self.chaos_injector is not None,
                "human_approval_enabled": self.human_approval is not None,
                "quantum_distillation_enabled": self.quantum_distillation is not None,
                "websocket_enabled": self.websocket_server.available,
                "swarm_enabled": self.swarm_coordinator.available,
                "competition_enabled": self.competition_engine.available,
                "security_enabled": self.security_service.available,
            }
        return stats


# =============================================================================
# SECTION 15. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cfg(self, **overrides) -> HarvesterConfig:
        cfg = HarvesterConfig(
            harvester_id="test",
            persistence=PersistenceConfig(enable=False),
            genetic=GeneticConfig(population_size=6, generations=2,
                                   evolution_interval=86400),
            enable_genetic_optimizer=True,
            enable_safety_monitor=True,
        )
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg

    def test_lifecycle(self):
        async def go():
            h = EnhancedPhotosyntheticHarvester(config=self._cfg())
            self.assertFalse(await h.ready())
            await h.start()
            self.assertTrue(await h.ready())
            await h.shutdown()
            self.assertTrue(h._shutdown)
            await h.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with EnhancedPhotosyntheticHarvester(config=self._cfg()) as h:
                self.assertTrue(await h.ready())
        asyncio.run(go())

    def test_harvest_cycle_no_crash(self):
        """Regression: harvest_cycle must not crash on missing _state_lock."""
        async def go():
            async with EnhancedPhotosyntheticHarvester(config=self._cfg()) as h:
                env = {
                    "renewable_availability": 0.8,
                    "carbon_intensity": 200.0,
                    "waste_heat": 0.3,
                }
                result = await h.harvest_cycle(env)
                self.assertIn("eco_atp_generated", result)
                self.assertGreaterEqual(result["eco_atp_generated"], 0.0)
                stats = await h.get_harvesting_stats()
                self.assertEqual(stats["harvest_cycles"], 1)
        asyncio.run(go())

    def test_pigment_damage_accumulates(self):
        """Regression: damage check must be on raw, not clamped, excitation."""
        async def go():
            async with EnhancedPhotosyntheticHarvester(config=self._cfg()) as h:
                env = {
                    "renewable_availability": 5.0,
                    "carbon_intensity": 5000.0,
                    "waste_heat": 5.0,
                }
                for _ in range(20):
                    await h.harvest_cycle(env)
                summary = h.pigments.get_health_summary()
                total_damage = sum(v["damage"] for v in summary.values())
                self.assertGreater(total_damage, 0.0,
                                   "damage was never applied")
        asyncio.run(go())

    def test_set_mode_async(self):
        async def go():
            async with EnhancedPhotosyntheticHarvester(config=self._cfg()) as h:
                await h.set_mode(HarvestingMode.CONSERVATIVE)
                self.assertEqual(h.mode, HarvestingMode.CONSERVATIVE)
        asyncio.run(go())

    def test_ga_genome_dependent(self):
        async def go():
            async with EnhancedPhotosyntheticHarvester(config=self._cfg()) as h:
                ga = h.genetic_optimizer
                a = {k: lo for k, (lo, hi) in ga.PARAM_BOUNDS.items()}
                b = {k: hi for k, (lo, hi) in ga.PARAM_BOUNDS.items()}
                oa = await ga._evaluate(a)
                ob = await ga._evaluate(b)
                diffs = [abs(oa[k] - ob[k]) for k in oa]
                self.assertGreater(max(diffs), 1e-4)
        asyncio.run(go())

    def test_ga_evolve(self):
        async def go():
            async with EnhancedPhotosyntheticHarvester(config=self._cfg()) as h:
                result = await h.genetic_optimizer.evolve(generations=2)
                self.assertIn("best_fitness", result)
                self.assertIn("pareto_front", result)
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3, max_q_table=10)
        for _ in range(200):
            s = np.random.rand(4)
            a = rl.act(s)
            rl.update(s, a, 1.0, s, False)
        self.assertLessEqual(rl.size(), 10)

    def test_placeholders_honest(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3)
        self.assertFalse(rl.available)
        fed = FederatedCoordinatorPlaceholder(None)
        self.assertFalse(fed.available)
        prec = PrecisionControllerPlaceholder()
        self.assertFalse(prec.available)
        self.assertEqual(prec.get_precision(0.9, 0.1), "float32")
        cm = CarbonMarketClientPlaceholder()
        self.assertFalse(cm.available)
        self.assertFalse(cm.buy_credits(10))
        self.assertFalse(cm.sell_credits(10))
        qd = QuantumDistillationModulePlaceholder()
        self.assertFalse(qd.available)
        ws = WebSocketServerPlaceholder()
        self.assertFalse(ws.available)
        sw = SwarmCoordinatorPlaceholder()
        self.assertFalse(sw.available)
        ce = CompetitionEnginePlaceholder()
        self.assertFalse(ce.available)
        sec = SecurityServicePlaceholder()
        self.assertFalse(sec.available)
        self.assertFalse(sec.check_token("anything"))

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
            cfg.persistence = PersistenceConfig(
                enable=True, backend="file", base_dir=td,
                checkpoint_interval=86400,
            )
            h1 = EnhancedPhotosyntheticHarvester(config=cfg)
            async with h1:
                await h1.harvest_cycle({
                    "renewable_availability": 0.5,
                    "carbon_intensity": 100.0,
                    "waste_heat": 0.2,
                })
                self.assertEqual(h1.harvest_cycles, 1)

            cfg2 = self._cfg()
            cfg2.persistence = PersistenceConfig(
                enable=True, backend="file", base_dir=td,
                checkpoint_interval=86400,
            )
            h2 = EnhancedPhotosyntheticHarvester(config=cfg2)
            async with h2:
                self.assertEqual(h2.harvest_cycles, 1)
        asyncio.run(go())

    def test_event_bus(self):
        async def go():
            bus = EventBus(max_workers=2)
            await bus.start()
            received: List[CoreEvent] = []
            bus.subscribe("test", lambda e: received.append(e))
            for i in range(5):
                await bus.publish(CoreEvent(
                    event_type="test", source="t", payload={"i": i}
                ))
            await asyncio.sleep(0.2)
            self.assertEqual(len(received), 5)
            await asyncio.wait_for(bus.stop(), timeout=2.0)
        asyncio.run(go())

    def test_circuit_breaker(self):
        async def go():
            cb = CircuitBreaker("t", failure_threshold=2, recovery_timeout=0.5)

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
            self.assertEqual(cb.state, CircuitBreakerState.OPEN)
            with self.assertRaises(CircuitBreakerOpenError):
                await cb.call(ok)
            await asyncio.sleep(0.6)
            self.assertEqual(await cb.call(ok), 1)
            self.assertEqual(cb.state, CircuitBreakerState.CLOSED)
        asyncio.run(go())

    def test_config_validation(self):
        cfg = self._cfg()
        cfg.latitude = 200.0
        issues = cfg.validate()
        self.assertTrue(any("latitude" in i for i in issues))

    def test_token_manager_async_integration(self):
        class MockTokenManager:
            def __init__(self):
                self.created = 0
                self.generated = 0

            async def create_account(self, account_id):
                self.created += 1

            async def get_system_summary(self):
                return {"total_balance": 1000.0}

            async def generate_tokens(self, account_id, source, **kwargs):
                self.generated += 1

                class T:
                    def __init__(self, v): self.value = v
                return [T(1.0), T(2.0), T(3.0)]

        async def go():
            tm = MockTokenManager()
            async with EnhancedPhotosyntheticHarvester(
                config=self._cfg(), token_manager=tm
            ) as h:
                await h.harvest_cycle({
                    "renewable_availability": 0.5,
                    "carbon_intensity": 100.0,
                    "waste_heat": 0.2,
                })
                self.assertEqual(tm.created, 1)
                self.assertGreaterEqual(tm.generated, 0)
        asyncio.run(go())

    def test_module_status_is_documented(self):
        self.assertIn("harvester_core", MODULE_STATUS)
        self.assertEqual(MODULE_STATUS["harvester_core"], "stable")
        self.assertEqual(MODULE_STATUS["causal_rl"], "placeholder")
        self.assertEqual(MODULE_STATUS["genetic_optimizer"], "experimental")


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 16. ENTRY POINT
# =============================================================================
async def _example() -> None:
    cfg = HarvesterConfig(
        harvester_id="demo",
        persistence=PersistenceConfig(enable=False),
        genetic=GeneticConfig(population_size=10, generations=3),
        enable_genetic_optimizer=True,
    )
    async with EnhancedPhotosyntheticHarvester(config=cfg) as h:
        env = {
            "renewable_availability": 0.8,
            "carbon_intensity": 200.0,
            "waste_heat": 0.3,
        }
        for i in range(5):
            r = await h.harvest_cycle(env)
            print(f"Cycle {i}: {r['eco_atp_generated']:.3f} eco-ATP, "
                  f"efficiency={r['efficiency']:.3f}")
        stats = await h.get_harvesting_stats()
        print("Total harvested:", stats["total_harvested"])
        print("Modules:", json.dumps(MODULE_STATUS, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enhanced Photosynthetic Harvester v12.0.0"
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

    print("Enhanced Photosynthetic Harvester v12.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
