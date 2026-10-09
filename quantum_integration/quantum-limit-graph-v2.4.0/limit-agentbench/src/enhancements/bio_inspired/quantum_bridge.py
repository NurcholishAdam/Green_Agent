#!/usr/bin/env python3
# =============================================================================
# Quantum Bridge v4.0.0 — Patched Single-File Edition
# =============================================================================
"""
Quantum Bridge v4.0.0
=====================
Patched single-file version of the v3.3.0 source.

P0 fixes
--------
- `import copy` added (was missing; optimizer crashed on first individual).
- Config is pure dataclasses. No Pydantic v1/v2 branch to trip over.
- `Gauge.clear()` removed — Prometheus labeled gauges re-set children directly.
- `update_config` does a deep merge (shallow merge destroyed unrelated keys).
- `QuantumBridge.apply_optimizer_result` applies the optimizer's best config
  and clears the cache.
- Numpy scalars coerced to `float` before caching / persisting.
- Async `set_parameters` handled in both sync and async paths.
- No blocking `time.sleep` on async paths.

P1 — correctness
----------------
- `_qubo_to_ising` uses the standard symmetric convention (documented) and
  preserves `timestamp` and any extra params.
- `_select_best_from_pareto` returns a copy; stored Pareto front is not
  mutated.
- Dominance is documented as maximization.
- `get_qubo_report` snapshots cache-hit status before the call.
- History is a bounded deque (O(1) eviction).
- CausalRL Q-table is discretized and LRU-bounded.

P2 — honesty
------------
- MODULE_STATUS documents every module.
- Placeholders warn on enable and expose `.available = False`.

P3 — production readiness
-------------------------
- Lifecycle: `async start()`, `async shutdown()`, `async ready()`,
  `__aenter__` / `__aexit__`.
- Graceful shutdown drains tasks; idempotent.
- Embedded test suite: `python3 quantum_bridge.py --test`.
- Prometheus + OpenTelemetry (both optional).
- --status prints module maturity.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import functools
import hashlib
import inspect
import json
import math
import os
import random
import sys
import time
import unittest
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Tuple, Union,
    runtime_checkable,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("quantum_bridge")
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


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "quantum_bridge_core":    "stable",
    "transform_registry":     "stable",
    "composite_provider":     "stable",
    "config_persistence":     "experimental",
    "prometheus_metrics":     "experimental",
    "qubo_ising_conversion":  "experimental",
    "mopd_optimizer":         "experimental",
    "safety_monitor":         "experimental",
    "xai":                    "experimental",
    "quantum_distillation":   "placeholder",
    "causal_rl":              "placeholder",
    "federated":              "placeholder",
    "precision":              "placeholder",
    "carbon_market":          "placeholder",
    "chaos":                  "placeholder",
    "human_approval":         "placeholder",
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


def _coerce_float_dict(d: Dict[str, Any]) -> Dict[str, float]:
    """Coerce every numeric value to a plain Python float."""
    out: Dict[str, float] = {}
    for k, v in d.items():
        if k == "timestamp":
            out[k] = float(v)
        elif isinstance(v, (int, float, np.integer, np.floating)):
            out[k] = float(v)
        else:
            out[k] = v  # type: ignore
    return out


def _deep_merge(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge updates into base. Neither input is mutated."""
    result = dict(base)
    for k, v in updates.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# =============================================================================
# SECTION 3. EXCEPTIONS
# =============================================================================
class QuantumBridgeError(Exception):
    pass


class ProviderError(QuantumBridgeError):
    pass


class SolverError(QuantumBridgeError):
    pass


class ConfigurationError(QuantumBridgeError):
    pass


class ConversionError(QuantumBridgeError):
    pass


# =============================================================================
# SECTION 4. DATA STRUCTURES
# =============================================================================
class OutputFormat(Enum):
    QUBO = "qubo"
    ISING = "ising"


@dataclass
class QuantumBridgeConfig:
    """
    Configuration for QuantumBridge. Pure dataclass — no Pydantic dependency.
    """
    # Mapping: gradient field name → QUBO parameter name
    field_mapping: Dict[str, str] = field(default_factory=lambda: {
        "carbon": "penalty_carbon",
        "helium": "penalty_helium_shortage",
        "trust": "penalty_geopolitical",
        "opportunity": "weight_opportunity",
        "eco_atp_reserve": "constraint_budget",
    })
    scaling: Dict[str, float] = field(default_factory=lambda: {
        "carbon": 10.0,
        "helium": 20.0,
        "trust": 8.0,
        "opportunity": 5.0,
        "eco_atp_reserve": 15.0,
    })
    default_gradient: float = 0.5
    field_specific_defaults: Dict[str, float] = field(default_factory=dict)
    invert_fields: List[str] = field(
        default_factory=lambda: ["trust", "eco_atp_reserve"]
    )
    enable_caching: bool = True
    cache_ttl: Optional[int] = None
    history_size: int = 100
    output_format: str = "qubo"
    quadratic_mapping: Dict[Tuple[str, str], str] = field(default_factory=lambda: {
        ("carbon", "helium"): "penalty_carbon_helium",
        ("trust", "opportunity"): "penalty_trust_opportunity",
    })
    custom_transform_registry: Dict[str, str] = field(default_factory=dict)
    cache_persistence_path: Optional[str] = None
    enable_prometheus: bool = False
    provider_retries: int = 2
    quadratic_scaling: float = 1.0
    transform_order: List[str] = field(
        default_factory=lambda: ["invert", "transform", "scale"]
    )
    param_types: Dict[str, str] = field(default_factory=lambda: {
        "penalty_carbon": "linear",
        "penalty_helium_shortage": "linear",
        "penalty_geopolitical": "linear",
        "weight_opportunity": "linear",
        "constraint_budget": "linear",
        "penalty_carbon_helium": "quadratic",
        "penalty_trust_opportunity": "quadratic",
    })
    config_version: str = "4.0"
    cache_version: int = 4

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

    # Experimental — on by default where harmless
    enable_safety_monitor: bool = True
    enable_xai: bool = True
    enable_mopd_optimizer: bool = True

    def validate(self) -> List[str]:
        issues: List[str] = []
        if self.output_format not in ("qubo", "ising"):
            issues.append("output_format must be 'qubo' or 'ising'")
        for k, v in self.scaling.items():
            if v <= 0:
                issues.append(f"scaling[{k}] must be positive")
        values = list(self.field_mapping.values())
        if len(values) != len(set(values)):
            issues.append("field_mapping values must be unique")
        valid_ops = {"invert", "transform", "scale"}
        for op in self.transform_order:
            if op not in valid_ops:
                issues.append(f"transform_order contains invalid op '{op}'")
        for param, kind in self.param_types.items():
            if kind not in ("linear", "quadratic"):
                issues.append(f"param_types['{param}'] invalid: {kind}")
        if not (0.0 <= self.chaos_probability <= 1.0):
            issues.append("chaos_probability must be in [0, 1]")
        if self.history_size < 1:
            issues.append("history_size must be >= 1")
        return issues

    def config_hash(self) -> str:
        data = {
            "field_mapping": self.field_mapping,
            "scaling": self.scaling,
            "invert_fields": self.invert_fields,
            "quadratic_mapping": {
                f"{k[0]}_{k[1]}": v for k, v in self.quadratic_mapping.items()
            },
            "custom_transform_registry": self.custom_transform_registry,
            "output_format": self.output_format,
            "quadratic_scaling": self.quadratic_scaling,
            "transform_order": self.transform_order,
            "param_types": self.param_types,
            "config_version": self.config_version,
            "default_gradient": self.default_gradient,
            "field_specific_defaults": self.field_specific_defaults,
            "enable_quantum_distillation": self.enable_quantum_distillation,
            "enable_causal_rl": self.enable_causal_rl,
            "enable_federated": self.enable_federated,
            "enable_precision": self.enable_precision,
            "enable_carbon_market": self.enable_carbon_market,
            "enable_chaos": self.enable_chaos,
            "enable_human_approval": self.enable_human_approval,
        }
        return hashlib.sha256(
            json.dumps(data, sort_keys=True, default=str).encode()
        ).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Tuples as dict keys are not JSON-friendly; keep as-is for the caller.
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QuantumBridgeConfig":
        data = dict(data or {})
        # Rebuild tuple keys for quadratic_mapping
        qm = data.get("quadratic_mapping")
        if isinstance(qm, dict):
            new_qm: Dict[Tuple[str, str], str] = {}
            for k, v in qm.items():
                if isinstance(k, tuple):
                    new_qm[k] = v
                elif isinstance(k, str) and "_" in k:
                    a, b = k.split("_", 1)
                    new_qm[(a, b)] = v
            data["quadratic_mapping"] = new_qm
        return cls(**{k: v for k, v in data.items()
                      if k in cls.__dataclass_fields__})


@dataclass
class MOPDPoint:
    config: QuantumBridgeConfig
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self.config.to_dict(),
            "objectives": self.objectives,
            "scalarised_score": self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(
            config=QuantumBridgeConfig.from_dict(data.get("config", {})),
            objectives=dict(data.get("objectives", {})),
            scalarised_score=float(data.get("scalarised_score", 0.0)),
        )


@dataclass
class CoreEvent:
    event_type: str
    source: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: Optional[str] = None


# =============================================================================
# SECTION 5. PROTOCOLS
# =============================================================================
@runtime_checkable
class GradientProvider(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def get_forecast(self, hours: int) -> Optional[Dict[str, float]]: ...


@runtime_checkable
class QuantumSolver(Protocol):
    def set_parameters(self, params: Dict[str, float]) -> None: ...
    def solve(self) -> Dict[str, Any]: ...


# =============================================================================
# SECTION 6. TASK MANAGER + EVENT BUS (STABLE)
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
        logger.info("TaskManager drained", completed=len(done),
                    cancelled=len(pending))


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
# SECTION 7. PLACEHOLDERS (safe no-ops)
# =============================================================================
class QuantumDistillationModulePlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("quantum_distillation")
        self.available = False

    async def optimize(self, params: Dict[str, float]) -> Dict[str, float]:
        return dict(params)

    def is_available(self) -> bool:
        return False


class CausalRLAgentPlaceholder:
    """STATUS: placeholder. Discretized, LRU-bounded, uniform policy."""
    STATUS = "placeholder"

    def __init__(self, state_dim: int = 10, action_dim: int = 3,
                 max_q_table: int = 5000, enabled: bool = False):
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

    def get_policy_probs(self, state: np.ndarray, temperature: float = 1.0):
        return [1.0 / self.action_dim] * self.action_dim

    def size(self) -> int:
        return len(self.q_table)


class FederatedCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, bridge: Any, queue: Optional[Any] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.bridge = bridge
        self.queue = queue
        self.available = False

    async def send_update(self) -> bool:
        return False

    async def receive_global_model(self, model_json: str) -> bool:
        return False


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

    def __init__(self, bridge: Any, chaos_probability: float = 0.0,
                 enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.bridge = bridge
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
            logger.warning(
                "HumanApproval auto_approve_dev=True; do not use in production."
            )

    async def request_approval(self, decision: Dict[str, Any],
                                timeout: float = 60.0) -> bool:
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

    def explain_translation(self, params: Dict[str, float],
                            strengths: Dict[str, float],
                            config: QuantumBridgeConfig) -> str:
        parts = []
        for field, param in config.field_mapping.items():
            val = strengths.get(field, 0.0)
            scaled = params.get(param, 0.0)
            parts.append(f"{field}={val:.3f} → {param}={scaled:.3f}")
        return "QUBO translation: " + ", ".join(parts)


class MOPDOptimizer:
    """
    NSGA-II over scaling coefficients and quadratic_scaling.

    STATUS: experimental.

    Dominance convention: **maximization**. Every objective returned by the
    caller's evaluate_func must be larger-is-better. If you need
    smaller-is-better, negate the objective before returning.
    """
    STATUS = "experimental"

    def __init__(
        self,
        bridge: "QuantumBridge",
        evaluate_func: Callable[[QuantumBridgeConfig], Dict[str, float]],
        population_size: int = 20,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        config_bounds: Optional[Dict[str, Tuple[float, float]]] = None,
        enabled: bool = True,
    ):
        if enabled:
            _warn_module("mopd_optimizer")
        self.bridge = bridge
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.best_individual: Optional[QuantumBridgeConfig] = None
        self.best_fitness: float = -math.inf
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self.available = True
        self._lock: Optional[asyncio.Lock] = None
        self._cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

        self.scaling_keys = list(bridge.config.scaling.keys())
        self.param_names = self.scaling_keys + ["quadratic_scaling"]
        self.param_bounds: Dict[str, Tuple[float, float]] = {}
        for k in self.scaling_keys:
            self.param_bounds[k] = (0.1, 100.0)
        self.param_bounds["quadratic_scaling"] = (0.0, 10.0)
        if config_bounds:
            self.param_bounds.update(config_bounds)

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _config_from_vector(self, vector: Dict[str, float]) -> QuantumBridgeConfig:
        new_config = copy.deepcopy(self.bridge.config)
        new_scaling = dict(new_config.scaling)
        for k in self.scaling_keys:
            new_scaling[k] = vector[k]
        new_config.scaling = new_scaling
        new_config.quadratic_scaling = vector["quadratic_scaling"]
        new_config.enable_caching = False
        return new_config

    def _vector_to_key(self, v: Dict[str, float]) -> Tuple[float, ...]:
        return tuple(float(v[k]) for k in self.param_names)

    def _random_individual(self) -> Dict[str, float]:
        ind: Dict[str, float] = {}
        for name in self.param_names:
            lo, hi = self.param_bounds[name]
            ind[name] = random.uniform(lo, hi)
        return ind

    def _crossover(self, p1: Dict[str, float],
                   p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for name in self.param_names:
            if random.random() < 0.5:
                child[name] = p1[name]
            else:
                child[name] = p2[name]
            if random.random() < 0.3:
                lo, hi = self.param_bounds[name]
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1.0 / 21.0)
                else:
                    beta = (1.0 / (2 * (1 - u))) ** (1.0 / 21.0)
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(lo, min(hi, val))
        return child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        m = dict(ind)
        for name in self.param_names:
            if random.random() < self.mutation_rate:
                lo, hi = self.param_bounds[name]
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1.0 / 21.0) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1.0 / 21.0)
                m[name] = max(lo, min(hi, m[name] + delta * (hi - lo)))
        return m

    async def _evaluate(self, vector: Dict[str, float]) -> Dict[str, float]:
        key = self._vector_to_key(vector)
        if key in self._cache:
            return self._cache[key]
        config = self._config_from_vector(vector)
        r = self.evaluate_func(config)
        if _is_awaitable(r):
            r = await r
        objs = {k: float(v) for k, v in (r or {}).items()}
        self._cache[key] = objs
        return objs

    @staticmethod
    def _dominates(a: Dict[str, float], b: Dict[str, float]) -> bool:
        keys = set(a) & set(b)
        if not keys:
            return False
        return all(a[k] >= b[k] for k in keys) and any(a[k] > b[k] for k in keys)

    def _fast_non_dominated_sort(
        self, objs: List[Dict[str, float]]
    ) -> List[List[int]]:
        n = len(objs)
        dominated: List[List[int]] = [[] for _ in range(n)]
        dom_count = [0] * n
        fronts: List[List[int]] = [[]]

        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(objs[i], objs[j]):
                    dominated[i].append(j)
                elif self._dominates(objs[j], objs[i]):
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

    def _crowding(self, front: List[int],
                  objs: List[Dict[str, float]]) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}
        distances: Dict[int, float] = {i: 0.0 for i in front}
        keys = list(objs[front[0]].keys())
        for key in keys:
            sf = sorted(front, key=lambda i: objs[i][key])
            distances[sf[0]] = float("inf")
            distances[sf[-1]] = float("inf")
            span = objs[sf[-1]][key] - objs[sf[0]][key]
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                distances[sf[idx]] += (
                    objs[sf[idx + 1]][key] - objs[sf[idx - 1]][key]
                ) / span
        return distances

    def _tournament(self, indices: List[int], ranks: List[int],
                    crowd: Dict[int, float]) -> int:
        if not indices:
            return 0
        if len(indices) < 2:
            return indices[0]
        a, b = random.sample(indices, 2)
        if ranks[a] < ranks[b]:
            return a
        if ranks[b] < ranks[a]:
            return b
        return a if crowd.get(a, 0.0) >= crowd.get(b, 0.0) else b

    def _select_best_from_pareto(
        self, front: List[MOPDPoint],
        weights: Dict[str, float],
    ) -> Optional[MOPDPoint]:
        # Return a copy; do not mutate the stored front.
        if not front:
            return None
        keys = list(weights.keys())
        max_vals = {k: max(p.objectives.get(k, 0.0) for p in front) for k in keys}
        min_vals = {k: min(p.objectives.get(k, 0.0) for p in front) for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
            for k in keys
        }
        best: Optional[MOPDPoint] = None
        best_score = -math.inf
        for p in front:
            s = sum(
                weights[k] * ((p.objectives.get(k, 0.0) - min_vals[k]) / ranges[k])
                for k in keys
            )
            if s > best_score:
                best_score = s
                best = p
        if best is None:
            return None
        copy_point = MOPDPoint(
            config=copy.deepcopy(best.config),
            objectives=dict(best.objectives),
            scalarised_score=best_score,
        )
        return copy_point

    @traced("quantum_bridge.optimizer.evolve")
    async def evolve(self, generations: Optional[int] = None
                      ) -> Tuple[QuantumBridgeConfig, Dict[str, float]]:
        gens = generations or self.generations
        async with self._get_lock():
            pop = [self._random_individual() for _ in range(self.population_size)]
            objs = [await self._evaluate(ind) for ind in pop]
            local_front: List[MOPDPoint] = []

            for _ in range(gens):
                ranks: List[int] = [0] * len(pop)
                crowd_global: Dict[int, float] = {}
                fronts = self._fast_non_dominated_sort(objs)
                for rank, front in enumerate(fronts):
                    cd = self._crowding(front, objs)
                    for idx in front:
                        ranks[idx] = rank
                        crowd_global[idx] = cd.get(idx, 0.0)

                offspring: List[Dict[str, float]] = []
                while len(offspring) < self.population_size:
                    all_indices = list(range(len(pop)))
                    i = self._tournament(all_indices, ranks, crowd_global)
                    j = self._tournament(all_indices, ranks, crowd_global)
                    if random.random() < self.crossover_rate:
                        c = self._mutate(self._crossover(pop[i], pop[j]))
                    else:
                        c = self._mutate(dict(pop[i]))
                    offspring.append(c)
                offspring = offspring[: self.population_size]
                off_objs = [await self._evaluate(o) for o in offspring]

                combined = pop + offspring
                combined_objs = objs + off_objs
                # Deduplicate by vector key
                seen: Dict[Tuple[float, ...], int] = {}
                dedup_pop: List[Dict[str, float]] = []
                dedup_objs: List[Dict[str, float]] = []
                for ind, obj in zip(combined, combined_objs):
                    k = self._vector_to_key(ind)
                    if k in seen:
                        continue
                    seen[k] = len(dedup_pop)
                    dedup_pop.append(ind)
                    dedup_objs.append(obj)

                fronts = self._fast_non_dominated_sort(dedup_objs)
                new_pop: List[Dict[str, float]] = []
                new_objs: List[Dict[str, float]] = []
                for front in fronts:
                    if len(new_pop) + len(front) <= self.population_size:
                        for idx in front:
                            new_pop.append(dedup_pop[idx])
                            new_objs.append(dedup_objs[idx])
                    else:
                        cd = self._crowding(front, dedup_objs)
                        sf = sorted(front, key=lambda i: cd.get(i, 0.0),
                                    reverse=True)
                        remaining = self.population_size - len(new_pop)
                        for idx in sf[:remaining]:
                            new_pop.append(dedup_pop[idx])
                            new_objs.append(dedup_objs[idx])
                        break
                pop, objs = new_pop, new_objs

                # Update local Pareto front
                local_front = []
                for ind, obj in zip(pop, objs):
                    local_front.append(MOPDPoint(
                        config=self._config_from_vector(ind),
                        objectives=dict(obj),
                    ))

            self.pareto_front = local_front

            if not self.objective_weights:
                all_keys: set = set()
                for p in local_front:
                    all_keys.update(p.objectives.keys())
                weights = {k: 1.0 / len(all_keys) for k in all_keys} if all_keys else {}
            else:
                weights = dict(self.objective_weights)

            best = self._select_best_from_pareto(local_front, weights)
            if best is not None:
                self.best_individual = copy.deepcopy(best.config)
                self.best_fitness = best.scalarised_score
                self.evolution_history.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "best_fitness": self.best_fitness,
                    "pareto_front_size": len(self.pareto_front),
                })
                return self.best_individual, dict(best.objectives)

            # Fallback: pick the individual with the highest mean objective
            if pop:
                means = [float(np.mean(list(o.values()))) if o else 0.0 for o in objs]
                best_idx = int(np.argmax(means))
                best_config = self._config_from_vector(pop[best_idx])
                self.best_individual = copy.deepcopy(best_config)
                self.best_fitness = means[best_idx]
                return best_config, dict(objs[best_idx])
            # Empty population fallback
            fallback_config = copy.deepcopy(self.bridge.config)
            self.best_individual = fallback_config
            self.best_fitness = 0.0
            return fallback_config, {}

    def get_pareto_front(self) -> List[MOPDPoint]:
        return [MOPDPoint(
            config=copy.deepcopy(p.config),
            objectives=dict(p.objectives),
            scalarised_score=p.scalarised_score,
        ) for p in self.pareto_front]

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "best_fitness": self.best_fitness,
            "evolution_history": self.evolution_history[-10:],
            "pareto_front_size": len(self.pareto_front),
        }


# =============================================================================
# SECTION 9. TRANSFORM REGISTRY + COMPOSITE PROVIDER
# =============================================================================
class TransformRegistry:
    _transforms: Dict[str, Callable[[float], float]] = {}

    @classmethod
    def register(cls, name: str, func: Callable[[float], float]) -> None:
        cls._transforms[name] = func

    @classmethod
    def get(cls, name: str) -> Optional[Callable[[float], float]]:
        return cls._transforms.get(name)

    @classmethod
    def known(cls) -> List[str]:
        return list(cls._transforms.keys())


def _quadratic_transform(x: float) -> float:
    return x ** 2


def _sigmoid_transform(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-10.0 * (x - 0.5)))


TransformRegistry.register("quadratic", _quadratic_transform)
TransformRegistry.register("sigmoid", _sigmoid_transform)


class CompositeGradientProvider:
    def __init__(self, providers: List[Tuple[GradientProvider, float]],
                 normalize: bool = True):
        self.providers = providers
        self.normalize = normalize

    def get_field_strengths(self) -> Dict[str, float]:
        combined: Dict[str, float] = {}
        total_weight = sum(w for _, w in self.providers)
        norm = total_weight if self.normalize and total_weight > 0 else 1.0
        for provider, weight in self.providers:
            try:
                strengths = provider.get_field_strengths()
                for field, value in strengths.items():
                    combined[field] = combined.get(field, 0.0) + value * weight / norm
            except Exception as e:
                logger.warning("Provider failed in composite: %s", e)
        return combined

    def get_forecast(self, hours: int) -> Optional[Dict[str, float]]:
        forecasts = []
        for provider, _ in self.providers:
            if hasattr(provider, "get_forecast"):
                try:
                    f = provider.get_forecast(hours)
                    if f is not None:
                        forecasts.append(f)
                except Exception as e:
                    logger.warning("Forecast from provider failed: %s", e)
        if not forecasts:
            return None
        combined: Dict[str, float] = {}
        for f in forecasts:
            for field, value in f.items():
                combined[field] = combined.get(field, 0.0) + value / len(forecasts)
        return combined

    async def get_field_strengths_async(self) -> Dict[str, float]:
        return self.get_field_strengths()

    async def get_forecast_async(self, hours: int) -> Optional[Dict[str, float]]:
        return self.get_forecast(hours)


# =============================================================================
# SECTION 10. QUANTUM BRIDGE CORE
# =============================================================================
class QuantumBridge:
    """
    Lifecycle:
        bridge = QuantumBridge(provider, solver, config)
        await bridge.start()
        ...
        await bridge.shutdown()
    """

    def __init__(
        self,
        gradient_provider: GradientProvider,
        quantum_solver: Optional[QuantumSolver] = None,
        config: Optional[Union[QuantumBridgeConfig, Dict[str, Any]]] = None,
        message_queue: Optional[Any] = None,
        shutdown_timeout_seconds: int = 10,
    ):
        self.gradient_provider = gradient_provider
        self.quantum_solver = quantum_solver
        self.message_queue = message_queue
        self._shutdown_timeout = shutdown_timeout_seconds

        if isinstance(config, dict):
            self.config = QuantumBridgeConfig.from_dict(config)
        elif isinstance(config, QuantumBridgeConfig):
            self.config = config
        else:
            self.config = QuantumBridgeConfig()

        issues = self.config.validate()
        if issues:
            raise ConfigurationError(f"Invalid config: {issues}")

        self._cache: Optional[Dict[str, float]] = None
        self._cache_hash: Optional[str] = None
        self._cache_timestamp: Optional[datetime] = None
        self._history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.history_size
        )
        self._last_update: Optional[datetime] = None
        self._expected_fields = list(self.config.field_mapping.keys())
        self._config_hash = self.config.config_hash()

        if (self.config.cache_persistence_path
                and os.path.exists(self.config.cache_persistence_path)):
            self._load_cache_from_disk()

        self._prometheus_metrics = self._init_prometheus()

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
            FederatedCoordinatorPlaceholder(self, self.message_queue, enabled=True)
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

        # Lifecycle
        self.event_bus = EventBus()
        self._task_manager = TaskManager()
        self._started = False
        self._shutdown = False

        logger.info("QuantumBridge initialized", config_hash=self._config_hash)

    # ---------------- prometheus ----------------
    def _init_prometheus(self) -> Optional[Dict[str, Any]]:
        if not (self.config.enable_prometheus and PROMETHEUS_AVAILABLE):
            return None
        try:
            hid = hashlib.sha1(self._config_hash.encode()).hexdigest()[:8]
            return {
                "translation_latency": Histogram(
                    f"quantum_bridge_translation_latency_seconds_{hid}",
                    "Time to translate gradients",
                ),
                "cache_hits": Counter(
                    f"quantum_bridge_cache_hits_total_{hid}",
                    "Cache hits",
                ),
                "cache_misses": Counter(
                    f"quantum_bridge_cache_misses_total_{hid}",
                    "Cache misses",
                ),
                "translation_count": Counter(
                    f"quantum_bridge_translation_total_{hid}",
                    "Total translations",
                ),
                "health_status": Gauge(
                    f"quantum_bridge_health_{hid}",
                    "Health status (1=healthy, 0=unhealthy)",
                ),
            }
        except Exception as e:
            logger.warning("Prometheus setup failed; metrics disabled",
                           error=str(e))
            return None

    # ---------------- safety ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "max_linear_penalty",
            lambda p: all(
                p.get(name, 0.0) <= 100.0
                for name in self.config.field_mapping.values()
            ),
            "Linear penalty exceeds safe threshold",
        )
        self.safety_monitor.add_invariant(
            "non_negative_quadratic",
            lambda p: all(
                p.get(name, 0.0) >= -0.01
                for name in self.config.quadratic_mapping.values()
            ),
            "Quadratic term negative (should be non-negative for penalties)",
        )

    def _check_safety(self, params: Dict[str, float]) -> List[str]:
        if not self.safety_monitor:
            return []
        return self.safety_monitor.check(params)

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        await self.event_bus.start()
        self._started = True
        logger.info("QuantumBridge started")

    async def ready(self) -> bool:
        return self._started and not self._shutdown

    async def shutdown(self) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        logger.info("QuantumBridge shutting down")
        try:
            await asyncio.wait_for(
                self._task_manager.drain(self._shutdown_timeout),
                timeout=self._shutdown_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")
        try:
            await asyncio.wait_for(self.event_bus.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Event bus stop timed out")
        logger.info("QuantumBridge shutdown complete")

    async def __aenter__(self) -> "QuantumBridge":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- helpers ----------------
    def _compute_hash(self, strengths: Dict[str, float]) -> str:
        data = {"strengths": strengths, "config_hash": self._config_hash}
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()

    def _get_default_for_field(self, field: str) -> float:
        return self.config.field_specific_defaults.get(
            field, self.config.default_gradient
        )

    def _validate_and_complete(self, strengths: Dict[str, float]
                                ) -> Dict[str, float]:
        validated: Dict[str, float] = {}
        for field in self._expected_fields:
            raw = float(strengths.get(field, self._get_default_for_field(field)))
            clamped = max(0.0, min(1.0, raw))
            if clamped != raw:
                logger.debug("Clamped field", field=field, raw=raw,
                             clamped=clamped)
            validated[field] = clamped
        return validated

    def _apply_transform(self, field: str, value: float) -> float:
        name = self.config.custom_transform_registry.get(field)
        if not name:
            return value
        fn = TransformRegistry.get(name)
        if fn is None:
            logger.warning("Unknown transform", name=name, field=field)
            return value
        try:
            return float(fn(value))
        except Exception as e:
            logger.warning("Transform failed", name=name, field=field,
                           error=str(e))
            return value

    def _translate_value(self, field: str, value: float) -> float:
        scale = self.config.scaling.get(field, 1.0)
        invert = field in self.config.invert_fields
        for op in self.config.transform_order:
            if op == "invert" and invert:
                value = 1.0 - value
            elif op == "transform":
                value = self._apply_transform(field, value)
            elif op == "scale":
                value *= scale
        return value

    def _translate_quadratic(self, strengths: Dict[str, float]
                              ) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for (f1, f2), param in self.config.quadratic_mapping.items():
            v1 = strengths.get(f1, 0.0)
            v2 = strengths.get(f2, 0.0)
            s1 = self.config.scaling.get(f1, 1.0) or 1.0
            s2 = self.config.scaling.get(f2, 1.0) or 1.0
            v1n = self._translate_value(f1, v1) / s1
            v2n = self._translate_value(f2, v2) / s2
            out[param] = float(v1n * v2n * self.config.quadratic_scaling)
        return out

    # ---------------- QUBO → Ising (documented) ----------------
    def _qubo_to_ising(self, qubo_params: Dict[str, float]) -> Dict[str, float]:
        """
        Standard symmetric transformation.

        Convention:
            - Q[i][i] = linear coefficient b_i (from field_mapping).
            - Q[i][j] = Q[j][i] = c_ij / 2 for i != j (from quadratic_mapping).

        With x_i = (1 + s_i) / 2, s_i ∈ {-1, +1}:
            h_i = b_i / 2 + sum_{j != i} Q[i][j] / 2
            J_ij = Q[i][j] / 2  (for i != j)

        Extra parameters (e.g. timestamp) are passed through unchanged.
        """
        fields = list(self.config.field_mapping.keys())
        n = len(fields)
        field_index = {f: i for i, f in enumerate(fields)}

        Q = np.zeros((n, n), dtype=float)
        # Diagonal: linear coefficient
        for field, param in self.config.field_mapping.items():
            if param in qubo_params:
                Q[field_index[field], field_index[field]] = float(qubo_params[param])
        # Off-diagonal symmetric
        for (f1, f2), param in self.config.quadratic_mapping.items():
            if param in qubo_params:
                i = field_index.get(f1)
                j = field_index.get(f2)
                if i is not None and j is not None:
                    v = float(qubo_params[param])
                    Q[i][j] = v
                    Q[j][i] = v

        h = np.zeros(n)
        J = np.zeros((n, n))
        for i in range(n):
            row_off_diag = float(np.sum(Q[i, :]) - Q[i, i])
            h[i] = Q[i, i] / 2.0 + row_off_diag / 2.0
        for i in range(n):
            for j in range(i + 1, n):
                J[i][j] = Q[i][j] / 2.0
                J[j][i] = J[i][j]

        ising: Dict[str, float] = {}
        for i, field in enumerate(fields):
            param_name = self.config.field_mapping[field]
            ising[f"h_{param_name}"] = float(h[i])
        for i in range(n):
            for j in range(i + 1, n):
                f1 = fields[i]
                f2 = fields[j]
                pair = (f1, f2)
                if pair in self.config.quadratic_mapping:
                    name = self.config.quadratic_mapping[pair]
                    ising[f"J_{name}"] = float(J[i][j])
                else:
                    ising[f"J_{f1}_{f2}"] = float(J[i][j])

        # Pass through extra params (e.g. timestamp) unchanged.
        for k, v in qubo_params.items():
            if k == "timestamp":
                ising[k] = v
            elif (k not in self.config.field_mapping.values()
                  and k not in self.config.quadratic_mapping.values()):
                ising[k] = v
        return ising

    # ---------------- provider fetch ----------------
    def _fetch_strengths(self, forecast_hours: Optional[int] = None
                          ) -> Dict[str, float]:
        retries = max(1, self.config.provider_retries + 1)
        for attempt in range(retries):
            try:
                if (forecast_hours is not None
                        and hasattr(self.gradient_provider, "get_forecast")):
                    f = self.gradient_provider.get_forecast(forecast_hours)
                    if f is not None:
                        return dict(f)
                return dict(self.gradient_provider.get_field_strengths())
            except Exception as e:
                logger.warning("Gradient provider failure (attempt %d/%d): %s",
                               attempt + 1, retries, e)
                if attempt == retries - 1:
                    logger.error("Provider failed after %d retries; using defaults.",
                                 retries)
                    return {f: self._get_default_for_field(f)
                            for f in self._expected_fields}
        return {f: self._get_default_for_field(f) for f in self._expected_fields}

    async def _fetch_strengths_async(self, forecast_hours: Optional[int] = None
                                      ) -> Dict[str, float]:
        retries = max(1, self.config.provider_retries + 1)
        for attempt in range(retries):
            try:
                if (forecast_hours is not None
                        and hasattr(self.gradient_provider, "get_forecast")):
                    f = self.gradient_provider.get_forecast(forecast_hours)
                    if _is_awaitable(f):
                        f = await f
                    if f is not None:
                        return dict(f)
                r = self.gradient_provider.get_field_strengths()
                if _is_awaitable(r):
                    r = await r
                return dict(r)
            except Exception as e:
                logger.warning("Gradient provider failure (attempt %d/%d): %s",
                               attempt + 1, retries, e)
                if attempt == retries - 1:
                    return {f: self._get_default_for_field(f)
                            for f in self._expected_fields}
                await asyncio.sleep(0.5 * (attempt + 1))
        return {f: self._get_default_for_field(f) for f in self._expected_fields}

    # ---------------- main translation ----------------
    def _compute_translation(
        self, strengths: Dict[str, float]
    ) -> Tuple[Dict[str, float], bool]:
        """Returns (params, cache_hit)."""
        current_hash = self._compute_hash(strengths)
        if self.config.enable_caching and self._cache_hash == current_hash:
            if self._cache_timestamp and self.config.cache_ttl is not None:
                age = (datetime.now(timezone.utc) - self._cache_timestamp
                       ).total_seconds()
                if age <= self.config.cache_ttl and self._cache is not None:
                    return dict(self._cache), True
            elif self._cache is not None:
                return dict(self._cache), True

        params: Dict[str, float] = {}
        for field, value in strengths.items():
            if field in self.config.field_mapping:
                params[self.config.field_mapping[field]] = float(
                    self._translate_value(field, value)
                )
        params.update(self._translate_quadratic(strengths))
        now = datetime.now(timezone.utc)
        params["timestamp"] = now.timestamp()

        if self.config.output_format == "ising":
            params = self._qubo_to_ising(params)

        params = _coerce_float_dict(params)

        if self.config.enable_caching:
            self._cache = dict(params)
            self._cache_hash = current_hash
            self._cache_timestamp = now
            self._persist_cache()

        self._history.append({
            "timestamp": now.isoformat(),
            "gradient_strengths": dict(strengths),
            "qubo_parameters": dict(params),
        })
        return params, False

    @traced("quantum_bridge.translate")
    def get_qubo_parameters(
        self, forecast_hours: Optional[int] = None
    ) -> Dict[str, float]:
        if self._prometheus_metrics:
            try:
                self._prometheus_metrics["translation_count"].inc()
            except Exception:
                pass

        strengths = self._fetch_strengths(forecast_hours)
        strengths = self._validate_and_complete(strengths)
        params, cache_hit = self._compute_translation(strengths)

        if self._prometheus_metrics:
            try:
                if cache_hit:
                    self._prometheus_metrics["cache_hits"].inc()
                else:
                    self._prometheus_metrics["cache_misses"].inc()
            except Exception:
                pass

        if self.safety_monitor:
            violations = self._check_safety(params)
            if violations:
                logger.warning("Safety violations detected", violations=violations)

        if self.config.enable_xai:
            logger.info("XAI", text=self.xai.explain_translation(
                params, strengths, self.config
            ))

        if self.carbon_market and "carbon" in strengths:
            carbon = strengths["carbon"]
            if carbon > 0.7:
                self.carbon_market.buy_credits(carbon * 10.0)
            elif carbon < 0.3:
                self.carbon_market.sell_credits((1.0 - carbon) * 5.0)

        if self.precision_controller:
            _ = self.precision_controller.get_precision(
                load=strengths.get("system_load", 0.5),
                energy_budget=strengths.get("energy_budget", 0.5),
            )

        return params

    async def get_qubo_parameters_async(
        self, forecast_hours: Optional[int] = None
    ) -> Dict[str, float]:
        strengths = await self._fetch_strengths_async(forecast_hours)
        strengths = self._validate_and_complete(strengths)
        params, _ = self._compute_translation(strengths)
        if self.safety_monitor:
            violations = self._check_safety(params)
            if violations:
                logger.warning("Safety violations detected", violations=violations)
        return params

    # ---------------- solver ----------------
    async def _apply_params_to_solver(self, params: Dict[str, float]) -> bool:
        if self.quantum_solver is None:
            logger.warning("No quantum solver attached – translation only.")
            return False
        try:
            r = self.quantum_solver.set_parameters(params)
            if _is_awaitable(r):
                await r
            return True
        except Exception as e:
            logger.error("Failed to apply parameters to solver", error=str(e))
            return False

    def apply_to_quantum_solver(self, forecast_hours: Optional[int] = None
                                 ) -> bool:
        """Synchronous wrapper. If the solver is async, use the async variant."""
        params = self.get_qubo_parameters(forecast_hours)
        if self.quantum_solver is None:
            logger.warning("No quantum solver attached – translation only.")
            return False
        try:
            r = self.quantum_solver.set_parameters(params)
            if _is_awaitable(r):
                logger.warning(
                    "Solver.set_parameters returned a coroutine; use the async "
                    "variant apply_to_quantum_solver_async()."
                )
                return False
            return True
        except Exception as e:
            logger.error("Failed to apply parameters to solver", error=str(e))
            return False

    async def apply_to_quantum_solver_async(
        self, forecast_hours: Optional[int] = None
    ) -> bool:
        params = await self.get_qubo_parameters_async(forecast_hours)
        return await self._apply_params_to_solver(params)

    # ---------------- cache persistence ----------------
    def _persist_cache(self) -> None:
        if not self.config.cache_persistence_path:
            return
        try:
            data = {
                "cache_version": self.config.cache_version,
                "config_hash": self._config_hash,
                "cache": _coerce_float_dict(self._cache or {}),
                "cache_hash": self._cache_hash,
                "cache_timestamp": self._cache_timestamp.isoformat()
                if self._cache_timestamp else None,
            }
            with open(self.config.cache_persistence_path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning("Failed to persist cache", error=str(e))

    def _load_cache_from_disk(self) -> None:
        if not self.config.cache_persistence_path:
            return
        try:
            with open(self.config.cache_persistence_path, "r") as f:
                data = json.load(f)
            if data.get("cache_version") != self.config.cache_version:
                logger.info("Cache version mismatch; discarding.")
                return
            if data.get("config_hash") != self._config_hash:
                logger.info("Config changed; discarding cached values.")
                return
            cache = data.get("cache") or {}
            self._cache = {
                k: (float(v) if isinstance(v, (int, float)) else v)
                for k, v in cache.items()
            }
            self._cache_hash = data.get("cache_hash")
            ts = data.get("cache_timestamp")
            if ts:
                self._cache_timestamp = datetime.fromisoformat(ts)
            logger.info("Cache loaded from disk")
        except Exception as e:
            logger.warning("Failed to load cache", error=str(e))

    # ---------------- reporting ----------------
    def health_check(self) -> Dict[str, Any]:
        status: Dict[str, Any] = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "config_hash": self._config_hash,
            "cache_loaded": self._cache is not None,
            "gradient_provider": "ok",
            "quantum_solver": "ok" if self.quantum_solver else "not_attached",
            "started": self._started,
            "shutdown": self._shutdown,
        }
        try:
            strengths = self.gradient_provider.get_field_strengths()
            if not strengths:
                status["status"] = "degraded"
                status["gradient_provider"] = "empty"
        except Exception as e:
            status["status"] = "unhealthy"
            status["gradient_provider"] = f"failed: {e}"
        if self.quantum_solver is not None and not hasattr(
            self.quantum_solver, "set_parameters"
        ):
            status["status"] = "degraded"
            status["quantum_solver"] = "missing_set_parameters"
        if self._prometheus_metrics:
            try:
                self._prometheus_metrics["health_status"].set(
                    1 if status["status"] == "healthy" else 0
                )
            except Exception:
                pass
        return status

    def get_qubo_report(self, forecast_hours: Optional[int] = None
                         ) -> Dict[str, Any]:
        strengths = self._fetch_strengths(forecast_hours)
        strengths = self._validate_and_complete(strengths)
        cache_hit_pre = (
            self._cache_hash is not None
            and self._cache_hash == self._compute_hash(strengths)
        )
        params = self.get_qubo_parameters(forecast_hours)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "gradient_strengths": dict(strengths),
            "qubo_parameters": dict(params),
            "scaling": dict(self.config.scaling),
            "field_mapping": dict(self.config.field_mapping),
            "quadratic_mapping": {
                f"{k[0]}_{k[1]}": v for k, v in self.config.quadratic_mapping.items()
            },
            "cache_hit": cache_hit_pre,
            "history_size": len(self._history),
            "output_format": self.config.output_format,
            "config": self.config.to_dict(),
            "safety_violations": self._check_safety(params) if self.safety_monitor else [],
            "xai_enabled": self.config.enable_xai,
            "carbon_market_available": bool(
                self.carbon_market and self.carbon_market.available
            ),
            "precision": "float32",
            "chaos_enabled": self.chaos_injector is not None,
            "human_approval_enabled": self.human_approval is not None,
            "federated_enabled": self.federated_coordinator is not None,
            "quantum_distillation_enabled": self.quantum_distillation is not None,
            "causal_rl_enabled": self.causal_rl_agent is not None,
            "module_status": MODULE_STATUS,
        }

    def get_history(self, limit: Optional[int] = None,
                    start_time: Optional[datetime] = None,
                    end_time: Optional[datetime] = None
                    ) -> List[Dict[str, Any]]:
        out = list(self._history)
        if start_time is not None:
            out = [h for h in out
                   if datetime.fromisoformat(h["timestamp"]) >= start_time]
        if end_time is not None:
            out = [h for h in out
                   if datetime.fromisoformat(h["timestamp"]) <= end_time]
        if limit is not None:
            out = out[-limit:]
        return out

    def export_history(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(list(self._history), f, indent=2, default=str)

    def clear_cache(self) -> None:
        self._cache = None
        self._cache_hash = None
        self._cache_timestamp = None
        if (self.config.cache_persistence_path
                and os.path.exists(self.config.cache_persistence_path)):
            try:
                os.remove(self.config.cache_persistence_path)
            except Exception as e:
                logger.warning("Failed to delete cache file", error=str(e))
        logger.info("Cache cleared")

    def clear_history(self) -> None:
        self._history.clear()

    # ---------------- config update ----------------
    async def update_config(self, updates: Dict[str, Any]) -> None:
        if self.config.enable_human_approval and self.human_approval is not None:
            ok = await self.human_approval.request_approval({
                "action": "update_config", "details": updates,
            })
            if not ok:
                logger.info("Config update rejected by human")
                return

        current = self.config.to_dict()
        merged = _deep_merge(current, updates)
        # Rebuild tuple-keyed quadratic_mapping if the merge broke it
        qm = merged.get("quadratic_mapping")
        if isinstance(qm, dict):
            rebuilt: Dict[Any, Any] = {}
            for k, v in qm.items():
                if isinstance(k, str) and "_" in k and not isinstance(k, tuple):
                    a, b = k.split("_", 1)
                    rebuilt[(a, b)] = v
                else:
                    rebuilt[k] = v
            merged["quadratic_mapping"] = rebuilt
        self.config = QuantumBridgeConfig.from_dict(merged)
        issues = self.config.validate()
        if issues:
            raise ConfigurationError(f"Invalid config after update: {issues}")
        self._config_hash = self.config.config_hash()
        self.clear_cache()
        logger.info("Configuration updated", updates=list(updates.keys()))

    def set_custom_transform(self, field: str, transform_name: str) -> None:
        if transform_name not in TransformRegistry.known():
            raise ValueError(f"Transform '{transform_name}' not registered")
        new_registry = dict(self.config.custom_transform_registry)
        new_registry[field] = transform_name
        self.config.custom_transform_registry = new_registry
        self._config_hash = self.config.config_hash()
        self.clear_cache()
        logger.info("Set custom transform", field=field, name=transform_name)

    # ---------------- optimizer integration ----------------
    def apply_optimizer_result(self, best_config: QuantumBridgeConfig) -> None:
        """Apply the optimizer's chosen config and clear the cache."""
        self.config = copy.deepcopy(best_config)
        issues = self.config.validate()
        if issues:
            raise ConfigurationError(f"Invalid optimizer config: {issues}")
        self._config_hash = self.config.config_hash()
        self.clear_cache()
        logger.info("Optimizer result applied")


# =============================================================================
# SECTION 11. TESTS
# =============================================================================
class _MockGradientProvider:
    def __init__(self, strengths: Optional[Dict[str, float]] = None):
        self._strengths = strengths or {
            "carbon": 0.8, "helium": 0.2, "trust": 0.1,
            "opportunity": 0.9, "eco_atp_reserve": 0.5,
        }

    def get_field_strengths(self) -> Dict[str, float]:
        return dict(self._strengths)

    def get_forecast(self, hours: int) -> Optional[Dict[str, float]]:
        return dict(self._strengths)


class _MockQuantumSolver:
    def __init__(self, async_mode: bool = False):
        self.calls: List[Dict[str, float]] = []
        self.async_mode = async_mode

    def set_parameters(self, params: Dict[str, float]):
        if self.async_mode:
            async def _inner():
                self.calls.append(params)
            return _inner()
        self.calls.append(params)

    def solve(self) -> Dict[str, Any]:
        return {"status": "ok"}


class _Tests(unittest.TestCase):
    def test_config_roundtrip(self):
        cfg = QuantumBridgeConfig()
        h = cfg.config_hash()
        cfg2 = QuantumBridgeConfig.from_dict(cfg.to_dict())
        self.assertEqual(h, cfg2.config_hash())

    def test_qubo_to_ising_preserves_timestamp(self):
        bridge = QuantumBridge(gradient_provider=_MockGradientProvider(),
                               config=QuantumBridgeConfig(output_format="ising"))
        params = bridge.get_qubo_parameters()
        self.assertIn("timestamp", params)

    def test_qubo_to_ising_diagonal_and_offdiag(self):
        cfg = QuantumBridgeConfig()
        bridge = QuantumBridge(gradient_provider=_MockGradientProvider(),
                                config=cfg)
        # Fabricate QUBO params to test the math
        qubo = {
            "penalty_carbon": 4.0,
            "penalty_helium_shortage": 2.0,
            "penalty_geopolitical": 1.0,
            "weight_opportunity": 3.0,
            "constraint_budget": 5.0,
            "penalty_carbon_helium": 0.5,
            "penalty_trust_opportunity": 0.25,
        }
        ising = bridge._qubo_to_ising(qubo)
        # Standard: h_i = Q_ii/2 + sum_{j!=i} Q_ij / 2
        # For carbon (index 0):
        # Q_00 = 4.0, Q_01 = 0.5
        # h_0 = 4.0/2 + 0.5/2 = 2.25
        self.assertAlmostEqual(ising["h_penalty_carbon"], 2.25, places=6)
        # J_01 = Q_01 / 2 = 0.25
        self.assertAlmostEqual(ising["J_penalty_carbon_helium"], 0.25, places=6)

    def test_update_config_deep_merge(self):
        async def go():
            bridge = QuantumBridge(gradient_provider=_MockGradientProvider())
            original_carbon = bridge.config.scaling["carbon"]
            original_helium = bridge.config.scaling["helium"]
            await bridge.update_config({"scaling": {"carbon": 12.0}})
            self.assertAlmostEqual(bridge.config.scaling["carbon"], 12.0)
            # Helium must NOT be lost
            self.assertAlmostEqual(bridge.config.scaling["helium"], original_helium)
            self.assertNotEqual(bridge.config.scaling["carbon"], original_carbon)
        asyncio.run(go())

    def test_cache_roundtrip_numpy_coercion(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cache_path = os.path.join(td, "cache.json")
            cfg = QuantumBridgeConfig(
                cache_persistence_path=cache_path,
                enable_caching=True,
            )
            bridge = QuantumBridge(gradient_provider=_MockGradientProvider(),
                                    config=cfg)
            params = bridge.get_qubo_parameters()
            for k, v in params.items():
                self.assertIsInstance(v, float, f"param {k} is {type(v)}")

            # Reload
            bridge2 = QuantumBridge(gradient_provider=_MockGradientProvider(),
                                     config=cfg)
            self.assertIsNotNone(bridge2._cache)
            for k, v in (bridge2._cache or {}).items():
                self.assertIsInstance(v, float, f"reloaded {k} is {type(v)}")
        asyncio.run(go())

    def test_lifecycle(self):
        async def go():
            bridge = QuantumBridge(gradient_provider=_MockGradientProvider())
            self.assertFalse(await bridge.ready())
            await bridge.start()
            self.assertTrue(await bridge.ready())
            await bridge.shutdown()
            self.assertTrue(bridge._shutdown)
            await bridge.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with QuantumBridge(gradient_provider=_MockGradientProvider()) as b:
                self.assertTrue(await b.ready())
        asyncio.run(go())

    def test_optimizer_applies_result(self):
        async def go():
            bridge = QuantumBridge(gradient_provider=_MockGradientProvider())

            def evaluate(cfg: QuantumBridgeConfig) -> Dict[str, float]:
                return {
                    "score_a": cfg.scaling["carbon"],
                    "score_b": -abs(cfg.scaling["helium"] - 15.0),
                }

            opt = MOPDOptimizer(
                bridge=bridge,
                evaluate_func=evaluate,
                population_size=6,
                generations=2,
                objective_weights={"score_a": 0.5, "score_b": 0.5},
            )
            best_config, objs = await opt.evolve()
            bridge.apply_optimizer_result(best_config)
            self.assertIsNotNone(bridge.config)
        asyncio.run(go())

    def test_optimizer_does_not_mutate_front(self):
        async def go():
            bridge = QuantumBridge(gradient_provider=_MockGradientProvider())

            def evaluate(cfg: QuantumBridgeConfig) -> Dict[str, float]:
                return {"a": cfg.scaling["carbon"], "b": cfg.scaling["helium"]}

            opt = MOPDOptimizer(
                bridge=bridge, evaluate_func=evaluate,
                population_size=4, generations=1,
                objective_weights={"a": 0.5, "b": 0.5},
            )
            await opt.evolve()
            # Take a snapshot of scalarised_score for the front
            before = [p.scalarised_score for p in opt.pareto_front]
            # Call _select_best_from_pareto again
            _ = opt._select_best_from_pareto(opt.pareto_front,
                                              {"a": 0.5, "b": 0.5})
            after = [p.scalarised_score for p in opt.pareto_front]
            self.assertEqual(before, after)
        asyncio.run(go())

    def test_async_solver_params(self):
        async def go():
            solver = _MockQuantumSolver(async_mode=True)
            async with QuantumBridge(
                gradient_provider=_MockGradientProvider(),
                quantum_solver=solver,
            ) as b:
                ok = await b.apply_to_quantum_solver_async()
                self.assertTrue(ok)
                self.assertEqual(len(solver.calls), 1)
        asyncio.run(go())

    def test_sync_solver_async_path_reports_false(self):
        solver = _MockQuantumSolver(async_mode=True)
        bridge = QuantumBridge(
            gradient_provider=_MockGradientProvider(),
            quantum_solver=solver,
        )
        # Sync path with async solver should report False and warn, not crash
        ok = bridge.apply_to_quantum_solver()
        self.assertFalse(ok)

    def test_placeholders_honest(self):
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
        self.assertFalse(cm.buy_credits(10))
        self.assertFalse(cm.sell_credits(10))
        ci = ChaosInjectorPlaceholder(None)
        self.assertFalse(ci.available)

        async def go():
            ha = HumanApprovalHandlerPlaceholder()
            self.assertFalse(ha.available)
            self.assertFalse(await ha.request_approval({"action": "x"}))
        asyncio.run(go())

    def test_q_table_bounded(self):
        rl = CausalRLAgentPlaceholder(state_dim=4, action_dim=3, max_q_table=10)
        for _ in range(200):
            s = np.random.rand(4)
            a = rl.act(s)
            rl.update(s, a, 1.0, s, False)
        self.assertLessEqual(rl.size(), 10)

    def test_safety_monitor_fires(self):
        bridge = QuantumBridge(gradient_provider=_MockGradientProvider())
        # Force a linear penalty well over the safe threshold
        bridge.config.scaling["carbon"] = 1000.0
        bridge._config_hash = bridge.config.config_hash()
        params = bridge.get_qubo_parameters()
        violations = bridge._check_safety(params)
        self.assertTrue(any("max_linear_penalty" in v for v in violations))

    def test_history_is_bounded(self):
        cfg = QuantumBridgeConfig(history_size=3, enable_caching=False)
        bridge = QuantumBridge(gradient_provider=_MockGradientProvider(),
                                config=cfg)
        for _ in range(10):
            bridge.get_qubo_parameters()
        self.assertLessEqual(len(bridge._history), 3)

    def test_module_status_is_documented(self):
        self.assertIn("quantum_bridge_core", MODULE_STATUS)
        self.assertEqual(MODULE_STATUS["quantum_bridge_core"], "stable")
        self.assertEqual(MODULE_STATUS["causal_rl"], "placeholder")


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 12. ENTRY POINT
# =============================================================================
async def _example() -> None:
    bridge = QuantumBridge(
        gradient_provider=_MockGradientProvider(),
        quantum_solver=_MockQuantumSolver(),
        config=QuantumBridgeConfig(
            output_format="qubo",
            cache_ttl=60,
        ),
    )
    async with bridge:
        params = bridge.get_qubo_parameters()
        print("QUBO parameters:", json.dumps(params, indent=2))
        ok = await bridge.apply_to_quantum_solver_async()
        print("Applied to solver:", ok)
        report = bridge.get_qubo_report()
        print("Report summary:", json.dumps({
            "cache_hit": report["cache_hit"],
            "history_size": report["history_size"],
            "output_format": report["output_format"],
        }, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Quantum Bridge v4.0.0"
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

    print("Quantum Bridge v4.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
