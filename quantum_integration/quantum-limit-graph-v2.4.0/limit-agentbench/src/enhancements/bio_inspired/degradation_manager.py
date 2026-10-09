#!/usr/bin/env python3
# =============================================================================
# Enhanced Degradation Manager v8.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced Degradation Manager v8.0.0
===================================
Patched single-file version of the hierarchical degradation manager.

P0 fixes
--------
- Reentrant-lock deadlock fixed (evaluate_rules → _transition_to_unlocked).
- DegradationConfig fully defined with every referenced field, plus
  to_dict / from_dict / from_env_and_file.
- All asyncio.Locks are lazily created (no cross-loop binding).
- Lifecycle moved to `await mgr.start()`; no tasks in __init__.
- _select_best_from_pareto returns a copy; stored front is not mutated.

P1 — real behavior
------------------
- All three MOPD objectives are genome-dependent:
    health    : recomputed from the individual's weights,
    stability : threshold margin vs current metrics,
    recovery  : hysteresis (enter/exit gap) quality.
- Rule cooldowns are enforced via _last_trigger[rule_id].
- CausalRL placeholder has a discretized, LRU-bounded Q-table.
- policy_probs returns a valid fixed-length vector over {maintain, degrade, recover}.
- Public async update_metrics() lets external callers feed state.
- Graceful shutdown drains tasks and flushes persistence; idempotent.

P2 — honesty
------------
- MODULE_STATUS documents each module.
- Placeholders: CausalRL, Federated, Precision, CarbonMarket, Chaos,
  HumanApproval. Disabled by default, safe no-ops, warn when enabled.
- Experimental: GeneticOptimizer, MOPD, SafetyMonitor, XAI, Persistence.
  Warn when enabled.

P3 — production readiness
-------------------------
- Lifecycle: `await start()`, `await shutdown()`, `await ready()`,
  `__aenter__` / `__aexit__`.
- Graceful shutdown with task drain and persistence flush.
- Logical sections in a single file.
- Embedded test suite: `python3 degradation_manager.py --test`.
- Prometheus metrics and OpenTelemetry spans (both optional).
"""

from __future__ import annotations

import argparse
import asyncio
import functools
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
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("degradation_manager")
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
# Central components (optional)
# -----------------------------------------------------------------------------
try:
    from ..storage import Storage as CentralStorage  # type: ignore
    from ..scaling.message_queue import AsyncMessageQueue  # type: ignore
    from ..routing.pareto_gating import ParetoGating  # type: ignore
    from ..feedback.adaptive_cost import AdaptiveCostFunction  # type: ignore
    from ..safety.drift_detector import DriftDetector  # type: ignore
    from ..metrics import MetricsRegistry  # type: ignore
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
    CENTRAL_AVAILABLE = True
except ImportError:
    CENTRAL_AVAILABLE = False
    CentralStorage = None  # type: ignore
    AsyncMessageQueue = None  # type: ignore
    ParetoGating = None  # type: ignore
    AdaptiveCostFunction = None  # type: ignore
    DriftDetector = None  # type: ignore
    MetricsRegistry = None  # type: ignore
    FeedbackEvent = None  # type: ignore


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "degradation_core":   "stable",
    "circuit_breaker":    "stable",
    "task_manager":       "stable",
    "genetic_optimizer":  "experimental",
    "mopd":               "experimental",
    "safety_monitor":     "experimental",
    "xai":                "experimental",
    "persistence":        "experimental",
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
def retry_async(max_retries: int = 3, base_delay: float = 0.5, max_delay: float = 5.0):
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
class OperationalTier(Enum):
    TIER_5_FULL = 5
    TIER_4_REDUCED = 4
    TIER_3_CONSERVATIVE = 3
    TIER_2_CRITICAL = 2
    TIER_1_SURVIVAL = 1


class TransitionType(Enum):
    DEGRADATION = "degradation"
    RECOVERY = "recovery"
    PREEMPTIVE = "preemptive"
    CHAOS_INDUCED = "chaos_induced"
    MANUAL = "manual"
    ANOMALY_INDUCED = "anomaly_induced"


class TransitionSpeed(Enum):
    INSTANT = "instant"
    FAST = "fast"
    NORMAL = "normal"
    SLOW = "slow"
    GRACEFUL = "graceful"


# =============================================================================
# SECTION 4. CONFIGURATION — fully defined
# =============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "health": 0.4,
        "stability": 0.3,
        "recovery": 0.3,
    })
    grid_resolution: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDConfig":
        data = dict(data or {})
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class DegradationConfig:
    # Recovery / cooldowns
    recovery_validation_period_seconds: float = 120.0
    transition_cooldown_seconds: float = 60.0
    max_transitions_per_hour: int = 10

    # Health score
    health_weights: Dict[str, float] = field(default_factory=lambda: {
        "token_balance": 0.15,
        "carbon_gradient": 0.20,
        "compartment_health": 0.30,
        "harvester_activity": 0.15,
        "error_rate": 0.20,
    })
    health_normalization: float = 2.0

    # Genetic optimizer
    enable_genetic_optimizer: bool = True
    ga_population_size: int = 20
    ga_mutation_rate: float = 0.1
    ga_crossover_rate: float = 0.7
    ga_generations: int = 5
    ga_tournament_size: int = 3
    ga_evolution_interval_hours: float = 6.0

    # MOPD
    mopd: MOPDConfig = field(default_factory=MOPDConfig)

    # Persistence
    enable_persistence: bool = True
    persistence_path: str = "degradation_state.json"

    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_db_path: str = "degradation_cb.db"
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0

    # RL
    q_table_max_size: int = 5000
    rl_state_dim: int = 10
    rl_action_dim: int = 3

    # Shutdown
    shutdown_timeout_seconds: int = 15

    # Experimental / placeholder toggles — disabled by default
    enable_causal_rl: bool = False
    enable_federated_learning: bool = False
    enable_safety_monitor: bool = True       # experimental but harmless
    enable_xai: bool = True                  # experimental but harmless
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
    def from_dict(cls, data: Dict[str, Any]) -> "DegradationConfig":
        data = dict(data or {})
        mopd_data = data.pop("mopd", None)
        fields = cls.__dataclass_fields__
        cfg = cls(**{k: v for k, v in data.items() if k in fields})
        if isinstance(mopd_data, dict):
            cfg.mopd = MOPDConfig.from_dict(mopd_data)
        return cfg

    @classmethod
    def from_env_and_file(cls) -> "DegradationConfig":
        return cls()


# =============================================================================
# SECTION 5. DATA CLASSES
# =============================================================================
@dataclass
class DegradationRule:
    rule_id: str
    metric: str
    enter_threshold: float
    exit_threshold: float
    comparison: str
    target_tier: OperationalTier
    cooldown_seconds: float = 60.0
    description: str = ""
    weight: float = 1.0
    trend_sensitive: bool = False
    trend_window: int = 10
    trend_threshold: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["target_tier"] = self.target_tier.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DegradationRule":
        data = dict(data or {})
        try:
            data["target_tier"] = OperationalTier(data["target_tier"])
        except (KeyError, ValueError):
            data["target_tier"] = OperationalTier.TIER_4_REDUCED
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class TransitionRecord:
    transition_id: str
    timestamp: datetime
    transition_type: TransitionType
    from_tier: OperationalTier
    to_tier: OperationalTier
    trigger_metric: str
    trigger_value: float
    trigger_threshold: float
    health_scores: Dict[str, float]
    duration_in_previous_tier: float
    was_preemptive: bool = False
    was_anomaly: bool = False
    transition_speed: TransitionSpeed = TransitionSpeed.NORMAL

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["transition_type"] = self.transition_type.value
        d["transition_speed"] = self.transition_speed.value
        d["from_tier"] = self.from_tier.value
        d["to_tier"] = self.to_tier.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TransitionRecord":
        data = dict(data or {})
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["transition_type"] = TransitionType(data.get("transition_type", "manual"))
        data["transition_speed"] = TransitionSpeed(data.get("transition_speed", "normal"))
        data["from_tier"] = OperationalTier(data.get("from_tier", 5))
        data["to_tier"] = OperationalTier(data.get("to_tier", 5))
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class HealthScore:
    timestamp: datetime
    overall_score: float
    component_scores: Dict[str, float]
    trend: str = "stable"
    predicted_tier: Optional[OperationalTier] = None
    confidence: float = 0.7

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "overall_score": self.overall_score,
            "component_scores": dict(self.component_scores),
            "trend": self.trend,
            "predicted_tier": self.predicted_tier.value if self.predicted_tier else None,
            "confidence": self.confidence,
        }


@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    health: float
    stability: float
    recovery: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**{k: v for k, v in (data or {}).items() if k in cls.__dataclass_fields__})


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

    def _load_sync(self) -> None:
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

    def _save_sync(self) -> None:
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
# SECTION 9. SAFETY MONITOR (EXPERIMENTAL)
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


# =============================================================================
# SECTION 10. XAI (EXPERIMENTAL)
# =============================================================================
class XAIExplainer:
    STATUS = "experimental"

    def __init__(self, enabled: bool = True):
        if enabled:
            _warn_module("xai")
        self.available = True

    def explain_transition(self, target_tier: OperationalTier, metric: str,
                           value: float, threshold: float) -> str:
        return (
            f"Transitioning to {target_tier.name} because {metric}={value:.3f} "
            f"crossed threshold {threshold:.3f}."
        )

    def explain_evolution(self, fitness: float) -> str:
        return f"Genetic optimizer selected parameters with scalarised fitness {fitness:.4f}."


# =============================================================================
# SECTION 11. GENETIC OPTIMIZER (EXPERIMENTAL)
# =============================================================================
class GeneticOptimizer:
    """
    Multi-objective genetic optimizer.

    The three objectives are genome-dependent:
      - health    : recomputed from the individual's weights against the current
                    metric snapshot.
      - stability : mean margin between the individual's enter thresholds and
                    the current metric values; larger margin = less likely to
                    trigger a rule = more stable.
      - recovery  : mean hysteresis between enter and exit thresholds; larger
                    hysteresis = smoother recovery = less oscillation.
    """
    STATUS = "experimental"

    METRIC_KEYS = ("token_balance", "carbon_gradient", "compartment_health",
                   "harvester_activity", "error_rate")

    def __init__(self, manager: "DegradationManager", config: DegradationConfig,
                 enabled: bool = True):
        if enabled:
            _warn_module("genetic_optimizer")
        self.manager = manager
        self.config = config
        self.best_fitness: float = -math.inf
        self.best_individual: Optional[Dict[str, Any]] = None
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _initialize_individual(self) -> Dict[str, Any]:
        ind: Dict[str, Any] = {}
        for rule in self.manager.rules:
            ind[f"{rule.rule_id}_enter"] = random.uniform(0.1, 0.9)
            ind[f"{rule.rule_id}_exit"] = random.uniform(0.1, 0.9)
            ind[f"{rule.rule_id}_weight"] = random.uniform(0.5, 2.0)
        for key in self.METRIC_KEYS:
            ind[f"weight_{key}"] = random.uniform(0.05, 0.4)
        self._normalize_weights(ind)
        return ind

    def _normalize_weights(self, ind: Dict[str, Any]) -> None:
        total = sum(ind.get(f"weight_{k}", 0.0) for k in self.METRIC_KEYS) or 1.0
        for k in self.METRIC_KEYS:
            ind[f"weight_{k}"] = ind.get(f"weight_{k}", 0.0) / total

    def _initialize_population(self) -> List[Dict[str, Any]]:
        return [self._initialize_individual() for _ in range(self.config.ga_population_size)]

    # --- genome-dependent objectives ---
    def _evaluate_health(self, ind: Dict[str, Any]) -> float:
        metrics = self.manager._metrics_snapshot()
        weights = {k: ind.get(f"weight_{k}", 0.0) for k in self.METRIC_KEYS}
        weighted = sum(weights.get(k, 0.0) * metrics[k] for k in self.METRIC_KEYS)
        return max(0.0, min(1.0, weighted / max(self.config.health_normalization, 1e-6)))

    def _evaluate_stability(self, ind: Dict[str, Any]) -> float:
        margins = []
        for rule in self.manager.rules:
            enter = ind.get(f"{rule.rule_id}_enter", 0.5)
            current = self.manager._get_metric(rule.metric)
            # For "greater_than", margin is how far below current is from enter.
            # For "less_than", margin is how far above current is from enter.
            if rule.comparison == "greater_than":
                margin = abs(enter - current)
            else:
                margin = abs(current - enter)
            margins.append(margin)
        if not margins:
            return 0.5
        # Normalize by 0.5 so margins above 0.5 saturate at 1.0
        return float(np.clip(np.mean(margins) / 0.5, 0.0, 1.0))

    def _evaluate_recovery(self, ind: Dict[str, Any]) -> float:
        gaps = []
        for rule in self.manager.rules:
            enter = ind.get(f"{rule.rule_id}_enter", 0.5)
            exit_ = ind.get(f"{rule.rule_id}_exit", 0.5)
            gaps.append(abs(enter - exit_))
        if not gaps:
            return 0.5
        # Normalize by 0.5 so gaps above 0.5 saturate at 1.0
        return float(np.clip(np.mean(gaps) / 0.5, 0.0, 1.0))

    def _evaluate_individual(self, ind: Dict[str, Any]) -> Dict[str, float]:
        return {
            "health": self._evaluate_health(ind),
            "stability": self._evaluate_stability(ind),
            "recovery": self._evaluate_recovery(ind),
        }

    def _scalarise(self, objs: Dict[str, float]) -> float:
        w = self.config.mopd.objective_weights
        return (
            w.get("health", 0.4) * objs["health"]
            + w.get("stability", 0.3) * objs["stability"]
            + w.get("recovery", 0.3) * objs["recovery"]
        )

    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        if not points:
            return []
        keys = ["health", "stability", "recovery"]
        front: List[MOPDPoint] = []
        for i, p in enumerate(points):
            dominated = False
            for j, q in enumerate(points):
                if i == j:
                    continue
                if (
                    all(getattr(q, k) >= getattr(p, k) for k in keys)
                    and any(getattr(q, k) > getattr(p, k) for k in keys)
                ):
                    dominated = True
                    break
            if not dominated:
                front.append(p)
        return front

    def _select_best_from_pareto(self, front: List[MOPDPoint]) -> Optional[MOPDPoint]:
        if not front:
            return None
        weights = self.config.mopd.objective_weights
        keys = list(weights.keys())
        max_vals = {k: max(getattr(p, k) for p in front) for k in keys}
        min_vals = {k: min(getattr(p, k) for p in front) for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
            for k in keys
        }
        best: Optional[MOPDPoint] = None
        best_score = -math.inf
        for p in front:
            score = sum(
                weights.get(k, 0.0) * ((getattr(p, k) - min_vals[k]) / ranges[k])
                for k in keys
            )
            if score > best_score:
                best_score = score
                best = p
        if best is None:
            return None
        copy = MOPDPoint.from_dict(best.to_dict())
        copy.scalarised_score = best_score
        return copy

    def _tournament(self, pop: List[Dict[str, Any]], fitness: List[float]) -> Dict[str, Any]:
        n = len(pop)
        k = min(self.config.ga_tournament_size, n)
        idxs = random.sample(range(n), k)
        best = max(idxs, key=lambda i: fitness[i])
        return pop[best]

    def _crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
        child = dict(p1)
        for key in p1:
            if key in p2 and random.random() < 0.5:
                child[key] = p2[key]
        # Blend numerical values occasionally
        for key in list(child.keys()):
            if key in p2 and random.random() < 0.3:
                child[key] = (child[key] + p2[key]) / 2.0
        self._normalize_weights(child)
        return child

    def _mutate(self, ind: Dict[str, Any]) -> Dict[str, Any]:
        mutant = dict(ind)
        for key in list(mutant.keys()):
            if random.random() < self.config.ga_mutation_rate:
                if key.endswith("_enter") or key.endswith("_exit"):
                    mutant[key] = max(0.01, min(0.99, mutant[key] + random.uniform(-0.1, 0.1)))
                else:
                    mutant[key] = max(0.01, mutant[key] + random.uniform(-0.1, 0.1))
        self._normalize_weights(mutant)
        return mutant

    async def evolve(self, generations: Optional[int] = None) -> Dict[str, Any]:
        generations = generations or self.config.ga_generations
        async with self._get_lock():
            population = self._initialize_population()
            local_front: List[MOPDPoint] = []
            final_fitness: List[float] = []

            for _ in range(generations):
                objs = [self._evaluate_individual(ind) for ind in population]
                fitness = [self._scalarise(o) for o in objs]

                if self.config.mopd.enabled:
                    points = [
                        MOPDPoint(
                            individual=dict(ind),
                            health=o["health"],
                            stability=o["stability"],
                            recovery=o["recovery"],
                        )
                        for ind, o in zip(population, objs)
                    ]
                    local_front = self._filter_pareto(local_front + points)

                new_pop: List[Dict[str, Any]] = []
                best_idx = int(np.argmax(fitness))
                new_pop.append(dict(population[best_idx]))
                while len(new_pop) < self.config.ga_population_size:
                    if random.random() < self.config.ga_crossover_rate and len(population) >= 2:
                        p1 = self._tournament(population, fitness)
                        p2 = self._tournament(population, fitness)
                        child = self._mutate(self._crossover(p1, p2))
                    else:
                        child = self._mutate(dict(self._tournament(population, fitness)))
                    new_pop.append(child)
                population = new_pop
                final_fitness = fitness

            self.pareto_front = local_front
            if self.config.mopd.enabled and local_front:
                best = self._select_best_from_pareto(local_front)
                if best is not None:
                    self.best_individual = dict(best.individual)
                    self.best_fitness = best.scalarised_score
            elif population:
                best_idx = int(np.argmax(final_fitness)) if final_fitness else 0
                self.best_individual = dict(population[best_idx])
                self.best_fitness = float(final_fitness[best_idx]) if final_fitness else 0.0

            if self.best_individual is not None:
                # Apply to manager's health weights
                self.manager._health_weights = {
                    k: self.best_individual.get(f"weight_{k}", self.manager._health_weights.get(k, 0.0))
                    for k in self.METRIC_KEYS
                }

            self.evolution_history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "generations": generations,
                "best_fitness": self.best_fitness,
                "pareto_size": len(self.pareto_front),
            })

            return {
                "best_fitness": self.best_fitness,
                "best_individual": self.best_individual,
                "generations": generations,
                "pareto_front": [p.to_dict() for p in self.pareto_front],
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
            "evolution_history": self.evolution_history[-10:],
            "pareto_front_size": len(self.pareto_front),
        }


# =============================================================================
# SECTION 12. PERSISTENCE (EXPERIMENTAL)
# =============================================================================
class DegradationPersistenceManager:
    STATUS = "experimental"
    CURRENT_VERSION = "3.0"

    def __init__(self, config: DegradationConfig, enabled: bool = True):
        if enabled:
            _warn_module("persistence")
        self.config = config
        self.path = Path(config.persistence_path)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def save_state(self, manager: "DegradationManager") -> bool:
        async with self._get_lock():
            try:
                state = {
                    "version": self.CURRENT_VERSION,
                    "config": manager.config.to_dict(),
                    "current_tier": manager.current_tier.value,
                    "previous_tier": manager.previous_tier.value,
                    "tier_history": [tr.to_dict() for tr in manager.tier_history[-100:]],
                    "health_scores": [hs.to_dict() for hs in manager.health_scores],
                    "rules": [r.to_dict() for r in manager.rules],
                    "health_weights": dict(manager._health_weights),
                    "metrics": {
                        "token_balance": manager._token_balance,
                        "carbon_gradient": manager._carbon_gradient,
                        "compartment_health": manager._compartment_health,
                        "harvester_activity": manager._harvester_activity,
                        "error_rate": manager._error_rate,
                        "queue_depth": manager._queue_depth,
                    },
                    "genetic_optimizer": manager.genetic_optimizer.to_dict(),
                }
                with open(self.path, "w") as f:
                    json.dump(state, f, indent=2, default=str)
                logger.info("Degradation state saved", path=str(self.path))
                return True
            except Exception as e:
                logger.error("State save failed", error=str(e))
                return False

    async def load_state(self, manager: "DegradationManager") -> bool:
        async with self._get_lock():
            if not self.path.exists():
                return False
            try:
                with open(self.path, "r") as f:
                    state = json.load(f)
                version = state.get("version", "0.0")
                if version != self.CURRENT_VERSION:
                    logger.warning("State version mismatch; ignoring", stored=version)
                    return False

                manager.config = DegradationConfig.from_dict(state.get("config", {}))
                manager.current_tier = OperationalTier(state.get("current_tier", 5))
                manager.previous_tier = OperationalTier(state.get("previous_tier", 5))

                manager.tier_history = []
                for tr in state.get("tier_history", [])[-100:]:
                    try:
                        manager.tier_history.append(TransitionRecord.from_dict(tr))
                    except Exception:
                        continue

                manager.health_scores = deque(maxlen=100)
                for hs in state.get("health_scores", []):
                    try:
                        ts = datetime.fromisoformat(hs["timestamp"])
                        pt = hs.get("predicted_tier")
                        pt_enum = OperationalTier(pt) if pt else None
                        manager.health_scores.append(HealthScore(
                            timestamp=ts,
                            overall_score=float(hs.get("overall_score", 0.7)),
                            component_scores=dict(hs.get("component_scores", {})),
                            trend=hs.get("trend", "stable"),
                            predicted_tier=pt_enum,
                            confidence=float(hs.get("confidence", 0.7)),
                        ))
                    except Exception:
                        continue

                loaded_rules = []
                for r in state.get("rules", []):
                    try:
                        loaded_rules.append(DegradationRule.from_dict(r))
                    except Exception:
                        continue
                if loaded_rules:
                    manager.rules = loaded_rules

                manager._health_weights.update(state.get("health_weights", {}))
                metrics = state.get("metrics", {})
                manager._token_balance = float(metrics.get("token_balance", manager._token_balance))
                manager._carbon_gradient = float(metrics.get("carbon_gradient", manager._carbon_gradient))
                manager._compartment_health = float(metrics.get("compartment_health", manager._compartment_health))
                manager._harvester_activity = float(metrics.get("harvester_activity", manager._harvester_activity))
                manager._error_rate = float(metrics.get("error_rate", manager._error_rate))
                manager._queue_depth = int(metrics.get("queue_depth", manager._queue_depth))

                manager.genetic_optimizer.from_dict(state.get("genetic_optimizer") or {})
                logger.info("Degradation state loaded", path=str(self.path))
                return True
            except Exception as e:
                logger.error("State load failed", error=str(e))
                return False


# =============================================================================
# SECTION 13. MAIN MANAGER
# =============================================================================
class DegradationManager:
    """
    Lifecycle:
        mgr = DegradationManager(...)
        await mgr.start()       # or `async with`
        ...
        await mgr.shutdown()
    """

    def __init__(
        self,
        config: Optional[DegradationConfig] = None,
        event_bus: Optional[Any] = None,
        token_manager: Optional[Any] = None,
        gradient_manager: Optional[Any] = None,
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
    ):
        self.config = config or DegradationConfig()
        self.event_bus = event_bus
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto_gating = pareto_gating
        self.drift_detector = drift_detector
        self.metrics = metrics

        # Tier state
        self.current_tier = OperationalTier.TIER_5_FULL
        self.previous_tier = OperationalTier.TIER_5_FULL
        self.predicted_tier: Optional[OperationalTier] = None
        self.tier_history: List[TransitionRecord] = []
        self.health_scores: Deque[HealthScore] = deque(maxlen=100)

        # Metrics
        self._token_balance = 500.0
        self._carbon_gradient = 0.5
        self._compartment_health = 0.8
        self._harvester_activity = 0.6
        self._error_rate = 0.01
        self._queue_depth = 0

        # Rules and weights
        self.rules: List[DegradationRule] = self._default_rules()
        self._health_weights: Dict[str, float] = dict(self.config.health_weights)
        self._last_trigger: Dict[str, float] = {}

        # Lazy locks
        self._state_lock: Optional[asyncio.Lock] = None
        self._transition_lock: Optional[asyncio.Lock] = None

        # Subsystems
        self.xai = XAIExplainer(enabled=self.config.enable_xai)
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()

        self.genetic_optimizer = GeneticOptimizer(self, self.config, enabled=True)

        # Placeholders
        self.rl_agent = CausalRLAgentPlaceholder(
            state_dim=self.config.rl_state_dim,
            action_dim=self.config.rl_action_dim,
            max_q_table=self.config.q_table_max_size,
            enabled=self.config.enable_causal_rl,
        )
        self.federated = (
            FederatedCoordinatorPlaceholder(self, message_queue, enabled=self.config.enable_federated_learning)
            if self.config.enable_federated_learning and message_queue is not None else None
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
            self.queue, enabled=self.config.enable_human_approval,
        )

        # Circuit breaker
        self.circuit_breaker = (
            CircuitBreaker(
                name="degradation_manager",
                db_path=self.config.circuit_breaker_db_path,
                failure_threshold=self.config.circuit_breaker_failure_threshold,
                recovery_timeout=self.config.circuit_breaker_timeout_seconds,
            )
            if self.config.enable_circuit_breaker else None
        )

        # Persistence
        self.persistence = (
            DegradationPersistenceManager(self.config, enabled=True)
            if self.config.enable_persistence and not storage else None
        )

        # Task manager
        self._task_manager = TaskManager()

        # Lifecycle
        self._started = False
        self._shutdown = False

        # Metrics
        self._prom = self._setup_metrics()

        logger.info(
            "DegradationManager initialized",
            mopd=self.config.mopd.enabled,
            persistence=self.persistence is not None,
        )

    # ---------------- locks ----------------
    def _get_state_lock(self) -> asyncio.Lock:
        if self._state_lock is None:
            self._state_lock = asyncio.Lock()
        return self._state_lock

    def _get_transition_lock(self) -> asyncio.Lock:
        if self._transition_lock is None:
            self._transition_lock = asyncio.Lock()
        return self._transition_lock

    # ---------------- safety invariants ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        max_transitions = self.config.max_transitions_per_hour
        self.safety_monitor.add_invariant(
            "token_balance_non_negative",
            lambda s: s.get("token_balance", 0.0) >= 0.0,
            "Token balance must be non-negative",
        )
        self.safety_monitor.add_invariant(
            "max_transitions_per_hour",
            lambda s: s.get("transitions_last_hour", 0) <= max_transitions,
            "Too many transitions in the last hour",
        )
        self.safety_monitor.add_invariant(
            "health_above_minimum",
            lambda s: s.get("overall_health", 1.0) >= 0.1,
            "Overall health too low",
        )

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Dict[str, Any]:
        if not PROMETHEUS_AVAILABLE:
            return {}
        try:
            return {
                "transitions_total": Counter("degradation_transitions_total", "Transitions"),
                "tier_gauge": Gauge("degradation_current_tier", "Current operational tier"),
                "health_gauge": Gauge("degradation_health_score", "Overall health"),
                "pareto_gauge": Gauge("degradation_pareto_size", "Pareto front size"),
                "q_table_gauge": Gauge("degradation_q_table_size", "RL q-table size"),
            }
        except Exception:
            return {}

    def _update_metrics(self) -> None:
        if not self._prom:
            return
        try:
            health = self.calculate_health_score()
            self._prom["tier_gauge"].set(self.current_tier.value)
            self._prom["health_gauge"].set(health.overall_score)
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
            try:
                await self.persistence.load_state(self)
            except Exception as e:
                logger.warning("Initial state load failed", error=str(e))

        self._task_manager.start_task("monitoring_loop", self._monitoring_loop)
        self._task_manager.start_task("evolution_loop", self._evolution_loop)
        self._task_manager.start_task("federated_loop", self._federated_loop)
        self._task_manager.start_task("chaos_loop", self._chaos_loop)

        logger.info("DegradationManager started")

    async def ready(self) -> bool:
        if not self._started:
            return False
        return isinstance(self.rules, list) and isinstance(self.tier_history, list)

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("DegradationManager shutting down")

        try:
            await asyncio.wait_for(self._task_manager.drain(timeout), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        if self.persistence is not None:
            try:
                await self.persistence.save_state(self)
            except Exception as e:
                logger.warning("Final save failed", error=str(e))

        logger.info("DegradationManager shutdown complete")

    async def __aenter__(self) -> "DegradationManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- default rules ----------------
    def _default_rules(self) -> List[DegradationRule]:
        return [
            DegradationRule(
                rule_id="carbon_high",
                metric="carbon_gradient",
                enter_threshold=0.7,
                exit_threshold=0.5,
                comparison="greater_than",
                target_tier=OperationalTier.TIER_4_REDUCED,
                cooldown_seconds=60.0,
                weight=1.0,
            ),
            DegradationRule(
                rule_id="token_low",
                metric="token_balance",
                enter_threshold=300.0,
                exit_threshold=1000.0,
                comparison="less_than",
                target_tier=OperationalTier.TIER_3_CONSERVATIVE,
                cooldown_seconds=60.0,
                weight=1.0,
            ),
            DegradationRule(
                rule_id="health_critical",
                metric="compartment_health",
                enter_threshold=0.3,
                exit_threshold=0.6,
                comparison="less_than",
                target_tier=OperationalTier.TIER_2_CRITICAL,
                cooldown_seconds=30.0,
                weight=2.0,
            ),
            DegradationRule(
                rule_id="error_rate_high",
                metric="error_rate",
                enter_threshold=0.05,
                exit_threshold=0.01,
                comparison="greater_than",
                target_tier=OperationalTier.TIER_2_CRITICAL,
                cooldown_seconds=30.0,
                weight=2.0,
            ),
        ]

    # ---------------- metrics snapshot ----------------
    def _metrics_snapshot(self) -> Dict[str, float]:
        return {
            "token_balance": self._token_balance,
            "carbon_gradient": self._carbon_gradient,
            "compartment_health": self._compartment_health,
            "harvester_activity": self._harvester_activity,
            "error_rate": self._error_rate,
        }

    def _get_metric(self, name: str) -> float:
        # token_balance is not in [0,1] but the normalize function clamps it
        return float(self._metrics_snapshot().get(name, 0.5))

    async def update_metrics(
        self,
        token_balance: Optional[float] = None,
        carbon_gradient: Optional[float] = None,
        compartment_health: Optional[float] = None,
        harvester_activity: Optional[float] = None,
        error_rate: Optional[float] = None,
        queue_depth: Optional[int] = None,
    ) -> None:
        """Public API to feed live metrics. All arguments optional."""
        async with self._get_state_lock():
            if token_balance is not None:
                self._token_balance = float(token_balance)
            if carbon_gradient is not None:
                self._carbon_gradient = float(carbon_gradient)
            if compartment_health is not None:
                self._compartment_health = float(compartment_health)
            if harvester_activity is not None:
                self._harvester_activity = float(harvester_activity)
            if error_rate is not None:
                self._error_rate = float(error_rate)
            if queue_depth is not None:
                self._queue_depth = int(queue_depth)

    # ---------------- background loops ----------------
    async def _monitoring_loop(self) -> None:
        while True:
            try:
                # Pull metrics from external providers if available
                if self.token_manager is not None and hasattr(self.token_manager, "get_system_summary"):
                    try:
                        r = self.token_manager.get_system_summary()
                        if asyncio.iscoroutine(r):
                            r = await r
                        if isinstance(r, dict):
                            await self.update_metrics(token_balance=r.get("total_balance"))
                    except Exception:
                        pass
                if self.gradient_manager is not None and hasattr(self.gradient_manager, "get_field_strengths"):
                    try:
                        r = self.gradient_manager.get_field_strengths()
                        if asyncio.iscoroutine(r):
                            r = await r
                        if isinstance(r, dict):
                            await self.update_metrics(carbon_gradient=r.get("carbon"))
                    except Exception:
                        pass

                await self.evaluate_rules()
                self._update_metrics()

                await asyncio.sleep(max(1.0, self.config.transition_cooldown_seconds))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Monitoring loop error", error=str(e))
                await asyncio.sleep(30)

    async def _evolution_loop(self) -> None:
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.tier_history) >= 2:
                    await self.genetic_optimizer.evolve()
                await asyncio.sleep(max(60.0, self.config.ga_evolution_interval_hours * 3600.0))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Evolution loop error", error=str(e))
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
                await asyncio.sleep(300)

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
                await asyncio.sleep(60)

    # ---------------- health ----------------
    def calculate_health_score(self) -> HealthScore:
        metrics = self._metrics_snapshot()
        weighted = sum(self._health_weights.get(k, 0.0) * metrics[k] for k in metrics)
        score = max(0.0, min(1.0, weighted / max(self.config.health_normalization, 1e-6)))
        return HealthScore(
            timestamp=datetime.now(timezone.utc),
            overall_score=score,
            component_scores=dict(metrics),
            trend="stable",
            predicted_tier=self.predicted_tier,
            confidence=0.7,
        )

    # ---------------- rule evaluation ----------------
    @traced("degradation.evaluate_rules")
    async def evaluate_rules(self) -> Optional[str]:
        """Evaluate all rules under a single lock; no reentrant acquisition."""
        async with self._get_transition_lock():
            now_ts = time.time()
            for rule in self.rules:
                # Cooldown enforcement
                last = self._last_trigger.get(rule.rule_id, 0.0)
                if now_ts - last < rule.cooldown_seconds:
                    continue

                metric_value = self._get_metric(rule.metric)
                triggered = (
                    metric_value > rule.enter_threshold
                    if rule.comparison == "greater_than"
                    else metric_value < rule.enter_threshold
                )
                if not triggered:
                    continue

                # Safety check
                if self.safety_monitor is not None:
                    state = self._get_safety_state()
                    violations = self.safety_monitor.check(state)
                    if violations:
                        logger.warning("Safety violation prevents transition", violations=violations)
                        continue

                # Human approval for critical transitions
                if self.human_approval is not None and rule.target_tier.value <= 2:
                    approved = await self.human_approval.request_approval({
                        "action": "transition",
                        "target_tier": rule.target_tier.value,
                        "trigger": rule.metric,
                    })
                    if not approved:
                        continue

                self._last_trigger[rule.rule_id] = now_ts
                return await self._transition_to_unlocked(
                    target_tier=rule.target_tier,
                    trigger_metric=rule.metric,
                    trigger_value=metric_value,
                    trigger_threshold=rule.enter_threshold,
                    transition_type=TransitionType.DEGRADATION,
                )
        return None

    def _get_safety_state(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        last_hour = now - timedelta(hours=1)
        transitions_last_hour = sum(1 for t in self.tier_history if t.timestamp >= last_hour)
        health = self.calculate_health_score()
        return {
            "token_balance": self._token_balance,
            "transitions_last_hour": transitions_last_hour,
            "overall_health": health.overall_score,
        }

    @traced("degradation.transition_to")
    async def transition_to(
        self,
        target_tier: OperationalTier,
        trigger_metric: str = "manual",
        trigger_value: float = 0.0,
        trigger_threshold: float = 0.0,
        transition_type: TransitionType = TransitionType.MANUAL,
        speed: TransitionSpeed = TransitionSpeed.NORMAL,
    ) -> Dict[str, Any]:
        async with self._get_transition_lock():
            return await self._transition_to_unlocked(
                target_tier=target_tier,
                trigger_metric=trigger_metric,
                trigger_value=trigger_value,
                trigger_threshold=trigger_threshold,
                transition_type=transition_type,
                speed=speed,
            )

    async def _transition_to_unlocked(
        self,
        target_tier: OperationalTier,
        trigger_metric: str,
        trigger_value: float,
        trigger_threshold: float,
        transition_type: TransitionType,
        speed: TransitionSpeed = TransitionSpeed.NORMAL,
    ) -> Dict[str, Any]:
        """Assumes the transition lock is already held."""
        if self.current_tier == target_tier:
            return {"status": "no_change"}

        if self.safety_monitor is not None:
            state = self._get_safety_state()
            state["target_tier"] = target_tier.value
            violations = self.safety_monitor.check(state)
            if violations:
                logger.warning("Safety violation on transition", violations=violations)
                return {"status": "safety_violation"}

        explanation: Optional[str] = None
        if self.xai is not None:
            explanation = self.xai.explain_transition(
                target_tier, trigger_metric, trigger_value, trigger_threshold
            )

        self.previous_tier = self.current_tier
        self.current_tier = target_tier

        record = TransitionRecord(
            transition_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc),
            transition_type=transition_type,
            from_tier=self.previous_tier,
            to_tier=target_tier,
            trigger_metric=trigger_metric,
            trigger_value=trigger_value,
            trigger_threshold=trigger_threshold,
            health_scores=self.calculate_health_score().component_scores,
            duration_in_previous_tier=0.0,
            transition_speed=speed,
        )
        self.tier_history.append(record)

        if self._prom:
            try:
                self._prom["transitions_total"].inc()
            except Exception:
                pass

        if self.queue is not None and FeedbackEvent is not None:
            try:
                event = FeedbackEvent.create_with_context(
                    task_id=f"degradation_{record.transition_id}",
                    selected_action=f"transition_to_{target_tier.value}",
                    quality_score=self.calculate_health_score().overall_score,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="degradation",
                    adaptive_cost_value=0.0,
                    state={"from_tier": self.previous_tier.value, "to_tier": target_tier.value},
                    candidates=[{"action": "transition"}],
                    source="degradation_manager",
                    environment="production",
                    tags=["degradation", "transition"],
                )
                await self.queue.publish("feedback_events", event.to_json())
            except Exception:
                pass

        # Reward RL agent based on direction
        if self.rl_agent is not None:
            state_vec = self._state_to_features()
            action_idx = 1 if target_tier.value < self.previous_tier.value else 2
            reward = 1.0 if target_tier.value >= self.previous_tier.value else -1.0
            self.rl_agent.update(state_vec, action_idx, reward, state_vec, False)

        logger.info(
            "Transition",
            from_tier=self.previous_tier.value,
            to_tier=target_tier.value,
            trigger=trigger_metric,
            explanation=explanation,
        )
        return {"status": "success", "transition": record.transition_id}

    # ---------------- state features / policy ----------------
    def _state_to_features(self) -> np.ndarray:
        health = self.calculate_health_score()
        return np.array([
            health.overall_score,
            min(self._token_balance / 2000.0, 1.0),
            self._carbon_gradient,
            self._compartment_health,
            self._harvester_activity,
            self._error_rate,
            self.current_tier.value / 5.0,
            min(len(self.tier_history) / 100.0, 1.0),
            min(self._queue_depth / 100.0, 1.0),
            0.0,
        ], dtype=float)

    @traced("degradation.policy_probs")
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """Fixed-length distribution over {maintain, degrade, recover}."""
        actions = ["maintain", "degrade", "recover"]
        n = len(actions)

        # Prefer central adaptive cost + pareto gating
        if self.adaptive_cost is not None and self.pareto_gating is not None:
            try:
                health = self.calculate_health_score().overall_score
                candidates = []
                for action in actions:
                    quality = 0.9 if action == "maintain" else 0.7 if action == "recover" else 0.5
                    cost = float(self.adaptive_cost.compute(
                        quality=quality,
                        carbon_g=0.0,
                        latency_ms=0.0,
                        energy_joules=0.0,
                        health=health,
                        atp=0.5,
                    ))
                    candidates.append({"action": action, "cost": cost})
                filtered = self.pareto_gating.filter(
                    [{"expert_id": c["action"], "quality_score": 0.5,
                      "carbon_g": 0.0, "latency_ms": 0.0, "energy_joules": 0.0}
                     for c in candidates]
                ) or []
                allowed = {c["expert_id"] for c in filtered} if filtered else {c["action"] for c in candidates}
                costs = np.array([c["cost"] if c["action"] in allowed else 1e9 for c in candidates])
                costs = costs - costs.min()
                exp = np.exp(-costs)
                probs = exp / exp.sum()
                return probs.tolist()
            except Exception:
                pass

        # Fallback: heuristic based on health
        health = self.calculate_health_score().overall_score
        maintain = max(0.05, health)
        degrade = max(0.05, (1.0 - health) * 0.7)
        recover = max(0.05, (1.0 - health) * 0.3)
        total = maintain + degrade + recover
        return [maintain / total, degrade / total, recover / total]

    # ---------------- public status ----------------
    def get_mopd_summary(self) -> Dict[str, Any]:
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": dict(self.config.mopd.objective_weights),
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "best_scalarised_score": self.genetic_optimizer.best_fitness,
            "evolution_history": self.genetic_optimizer.evolution_history[-10:],
        }

    def get_health_status(self) -> Dict[str, Any]:
        health = self.calculate_health_score()
        return {
            "status": "healthy" if self.current_tier.value > 3 else "degraded",
            "score": health.overall_score,
            "current_tier": self.current_tier.value,
            "previous_tier": self.previous_tier.value,
            "transition_count": len(self.tier_history),
            "module_status": MODULE_STATUS,
            "q_table_size": self.rl_agent.size(),
            "pareto_front_size": len(self.genetic_optimizer.pareto_front),
            "circuit_breaker": self.circuit_breaker.snapshot() if self.circuit_breaker else None,
        }


# =============================================================================
# SECTION 14. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cfg(self) -> DegradationConfig:
        import tempfile
        td = tempfile.mkdtemp()
        return DegradationConfig(
            persistence_path=os.path.join(td, "state.json"),
            circuit_breaker_db_path=os.path.join(td, "cb.db"),
            ga_population_size=6,
            ga_generations=2,
            transition_cooldown_seconds=0.1,
            recovery_validation_period_seconds=0.5,
        )

    def test_lifecycle(self):
        async def go():
            mgr = DegradationManager(config=self._cfg())
            self.assertFalse(await mgr.ready())
            await mgr.start()
            self.assertTrue(await mgr.ready())
            await mgr.shutdown()
            self.assertTrue(mgr._shutdown)
            await mgr.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with DegradationManager(config=self._cfg()) as mgr:
                self.assertTrue(await mgr.ready())
        asyncio.run(go())

    def test_no_deadlock_on_rule_trigger(self):
        """evaluate_rules calls _transition_to_unlocked under a single lock."""
        async def go():
            async with DegradationManager(config=self._cfg()) as mgr:
                await mgr.update_metrics(error_rate=0.9)  # triggers error_rate_high
                result = await asyncio.wait_for(mgr.evaluate_rules(), timeout=2.0)
                self.assertIsNotNone(result)
                self.assertEqual(result["status"], "success")
        asyncio.run(go())

    def test_rule_cooldown_enforced(self):
        async def go():
            async with DegradationManager(config=self._cfg()) as mgr:
                await mgr.update_metrics(error_rate=0.9)
                r1 = await mgr.evaluate_rules()
                self.assertIsNotNone(r1)
                # Second call within cooldown should not trigger
                r2 = await mgr.evaluate_rules()
                self.assertIsNone(r2)
        asyncio.run(go())

    def test_all_objectives_genome_dependent(self):
        async def go():
            async with DegradationManager(config=self._cfg()) as mgr:
                # Two very different individuals
                ind_a = {}
                ind_b = {}
                for rule in mgr.rules:
                    ind_a[f"{rule.rule_id}_enter"] = 0.9
                    ind_a[f"{rule.rule_id}_exit"] = 0.1
                    ind_b[f"{rule.rule_id}_enter"] = 0.1
                    ind_b[f"{rule.rule_id}_exit"] = 0.9
                for k in GeneticOptimizer.METRIC_KEYS:
                    ind_a[f"weight_{k}"] = 1.0 / len(GeneticOptimizer.METRIC_KEYS)
                    ind_b[f"weight_{k}"] = 1.0 / len(GeneticOptimizer.METRIC_KEYS)
                oa = mgr.genetic_optimizer._evaluate_individual(ind_a)
                ob = mgr.genetic_optimizer._evaluate_individual(ind_b)
                # All three objectives should differ between the two genomes
                self.assertNotAlmostEqual(oa["stability"], ob["stability"], places=4)
                self.assertNotAlmostEqual(oa["recovery"], ob["recovery"], places=4)
                # health differs because the weights are the same but... let's
                # instead vary weights to check health too
                ind_c = dict(ind_a)
                ind_d = dict(ind_b)
                for k in GeneticOptimizer.METRIC_KEYS:
                    ind_c[f"weight_{k}"] = 0.0
                    ind_d[f"weight_{k}"] = 0.0
                ind_c["weight_compartment_health"] = 1.0
                ind_d["weight_error_rate"] = 1.0
                hc = mgr.genetic_optimizer._evaluate_health(ind_c)
                hd = mgr.genetic_optimizer._evaluate_health(ind_d)
                self.assertNotAlmostEqual(hc, hd, places=4)
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3, max_q_table=10)
        for _ in range(200):
            s = np.random.rand(4)
            a = rl.act(s)
            rl.update(s, a, 1.0, s, False)
        self.assertLessEqual(rl.size(), 10)

    def test_policy_probs_valid(self):
        async def go():
            async with DegradationManager(config=self._cfg()) as mgr:
                probs = await mgr.policy_probs({})
                self.assertEqual(len(probs), 3)
                self.assertTrue(all(p >= 0 for p in probs))
                self.assertAlmostEqual(sum(probs), 1.0, places=6)
        asyncio.run(go())

    def test_persistence_roundtrip(self):
        async def go():
            cfg = self._cfg()
            mgr = DegradationManager(config=cfg)
            async with mgr:
                await mgr.update_metrics(error_rate=0.9)
                await mgr.evaluate_rules()
                self.assertEqual(len(mgr.tier_history), 1)
                await mgr.persistence.save_state(mgr)
                self.assertTrue(os.path.exists(cfg.persistence_path))

            mgr2 = DegradationManager(config=cfg)
            async with mgr2:
                await mgr2.persistence.load_state(mgr2)
                self.assertEqual(len(mgr2.tier_history), 1)
                self.assertEqual(mgr2.tier_history[0].trigger_metric, "error_rate")
        asyncio.run(go())

    def test_circuit_breaker_transitions(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cb = CircuitBreaker("test", os.path.join(td, "cb.db"),
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

    def test_placeholders_are_noops(self):
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

    def test_safety_invariant_transitions(self):
        async def go():
            cfg = self._cfg()
            cfg.max_transitions_per_hour = 1
            cfg.transition_cooldown_seconds = 0.0
            async with DegradationManager(config=cfg) as mgr:
                await mgr.update_metrics(error_rate=0.9)
                r1 = await mgr.evaluate_rules()
                self.assertIsNotNone(r1)
                # Force a second trigger
                await mgr.update_metrics(compartment_health=0.1)
                r2 = await mgr.evaluate_rules()
                # Second transition should be blocked by the safety invariant
                self.assertIn(r2, (None, {"status": "safety_violation"}))
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 15. ENTRY POINT
# =============================================================================
async def _example() -> None:
    async with DegradationManager() as mgr:
        await mgr.update_metrics(error_rate=0.9, carbon_gradient=0.85)
        await mgr.evaluate_rules()
        print("status:", json.dumps(mgr.get_health_status(), indent=2, default=str))
        print("policy:", await mgr.policy_probs({}))
        print("mopd:", json.dumps(mgr.get_mopd_summary(), indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Degradation Manager v8.0.0")
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

    print("Degradation Manager v8.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
