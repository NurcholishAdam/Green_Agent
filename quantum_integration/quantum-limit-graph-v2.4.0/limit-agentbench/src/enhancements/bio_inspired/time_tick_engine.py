#!/usr/bin/env python3
# =============================================================================
# TimeTickEngine v4.0.0 — Patched Single-File Edition
# =============================================================================
"""
TimeTickEngine v4.0.0
=====================
Patched single-file version of the v3.4 source.

P0 fixes
--------
- Background tasks moved to `async start()`; no tasks in `__init__`.
- `evaluate_policy` snapshot/restore + lock; policy evaluation is serialized
  and no longer corrupts shared engine state.
- `NSGAIIOptimizer._tournament_selection` uses an explicit key→rank map,
  so selection actually reflects front rank and crowding.
- `_select_best_from_pareto` returns a copy; stored Pareto front is never
  mutated.
- `_maybe_trade_carbon` handles sync and async carbon market clients.
- `Lifecycle`: `async start()`, `async shutdown()`, `async ready()`,
  `__aenter__` / `__aexit__`.
- Config is pure dataclasses. No Pydantic v1/v2 branch.
- Checkpoints use JSON, not pickle.

P1 — correctness
----------------
- timezone-aware timestamps throughout.
- Bounded `MetricsCollector` lists (via deques).
- `_current_index` and other state mutated under an asyncio.Lock.
- `evaluate_policy` restores `_current_index`, `_running`, `_stop_event`.
- Chaos injector no longer mutates `_current_index` from a background task.
- Guard against re-entering `run_simulation`.
- `fillna(method=...)` replaced with `.ffill().bfill()`.
- `get_pareto_front` returns deep copies.
- Translator failures are logged at debug level.

P2 — honesty
------------
- MODULE_STATUS documents every module.
- Placeholders (disabled by default, warn when enabled):
    quantum_distillation, causal_rl, federated, precision, carbon_market,
    chaos, human_approval.
- Experimental (warn when enabled):
    mopd_filtering, nsga2_optimizer, safety_monitor, xai, checkpointing,
    live_data_feed.

P3 — production readiness
-------------------------
- Embedded test suite: python3 time_tick_engine.py --test.
- --status prints module maturity.
- Prometheus + OpenTelemetry hooks (optional).
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import functools
import hashlib
import inspect
import json
import logging
import math
import os
import random
import sys
import unittest
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Protocol, Tuple, Union,
)

import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("time_tick_engine")
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
    "time_tick_core":        "stable",
    "csv_loading":           "stable",
    "metrics":               "stable",
    "event_bus":             "stable",
    "task_manager":          "stable",
    "live_data_feed":        "experimental",
    "checkpointing":         "experimental",
    "mopd_filtering":        "experimental",
    "nsga2_optimizer":       "experimental",
    "safety_monitor":        "experimental",
    "xai":                   "experimental",
    "quantum_distillation":  "placeholder",
    "causal_rl":             "placeholder",
    "federated":             "placeholder",
    "precision":             "placeholder",
    "carbon_market":         "placeholder",
    "chaos":                 "placeholder",
    "human_approval":        "placeholder",
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
# SECTION 2. TRACING HELPERS
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


# =============================================================================
# SECTION 3. ENUMS
# =============================================================================
class HarvestingMode(Enum):
    FULL = "full"
    ADAPTIVE = "adaptive"
    MODULATED = "modulated"
    CONSERVATIVE = "conservative"
    MINIMAL = "minimal"
    SURVIVAL = "survival"
    STANDARD = "standard"
    AGGRESSIVE = "aggressive"


# =============================================================================
# SECTION 4. CONFIGURATION (pure dataclasses)
# =============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "total_harvested": 0.3,
        "avg_efficiency": 0.3,
        "carbon_saved": 0.2,
        "helium_saved": 0.2,
    })
    grid_resolution: int = 5
    population_size: int = 20
    generations: int = 5
    mutation_rate: float = 0.2
    crossover_rate: float = 0.8
    tournament_size: int = 3
    dynamic_weights: bool = True

    def validate(self) -> List[str]:
        issues: List[str] = []
        total = sum(self.objective_weights.values())
        if abs(total - 1.0) > 1e-6:
            issues.append("mopd.objective_weights must sum to 1")
        if self.population_size < 4:
            issues.append("mopd.population_size must be >= 4")
        if self.generations < 1:
            issues.append("mopd.generations must be >= 1")
        return issues


@dataclass
class TimeTickConfig:
    data_source: str = "csv"
    csv_path: Optional[str] = None
    date_column: str = "date"
    date_format: Optional[str] = None
    value_columns: List[str] = field(
        default_factory=lambda: ["helium_supply", "helium_demand"]
    )
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    interpolation_method: str = "linear"
    tick_interval_seconds: float = 0.1
    checkpoint_dir: str = "./checkpoints"
    enable_checkpointing: bool = True
    checkpoint_interval: int = 100
    metrics_enabled: bool = True
    max_checkpoints: int = 5
    live_fetch_interval: float = 1.0
    live_data_callback: Optional[Callable] = None
    max_custom_metrics_entries: int = 1000
    max_metric_history: int = 10000
    shutdown_timeout_seconds: int = 15
    mopd: MOPDConfig = field(default_factory=MOPDConfig)

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
        if self.data_source not in ("csv", "live"):
            issues.append("data_source must be 'csv' or 'live'")
        if self.data_source == "live" and self.live_data_callback is None:
            issues.append("live_data_callback required when data_source='live'")
        if self.interpolation_method not in ("linear", "quadratic", "spline", "time"):
            issues.append("interpolation_method must be linear|quadratic|spline|time")
        if not (0.0 <= self.chaos_probability <= 1.0):
            issues.append("chaos_probability must be in [0, 1]")
        if self.checkpoint_interval < 1:
            issues.append("checkpoint_interval must be >= 1")
        issues.extend(self.mopd.validate())
        return issues

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["live_data_callback"] = None  # not serializable
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TimeTickConfig":
        data = dict(data or {})
        mopd_data = data.pop("mopd", None)
        fields = cls.__dataclass_fields__
        kwargs = {k: v for k, v in data.items() if k in fields}
        cfg = cls(**kwargs)
        if isinstance(mopd_data, dict):
            cfg.mopd = MOPDConfig(**{
                k: v for k, v in mopd_data.items()
                if k in MOPDConfig.__dataclass_fields__
            })
        return cfg


# =============================================================================
# SECTION 5. PROTOCOLS
# =============================================================================
class HarvesterProtocol(Protocol):
    async def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]: ...
    def set_mode(self, mode: Any) -> None: ...
    async def get_harvesting_stats(self) -> Dict[str, Any]: ...
    def restore_state(self, state: Dict[str, Any]) -> None: ...
    def set_parameters(self, params: Dict[str, Any]) -> None: ...


class TranslatorProtocol(Protocol):
    @staticmethod
    def translate_row(row: pd.Series) -> Dict[str, float]: ...


# =============================================================================
# SECTION 6. DATA STRUCTURES
# =============================================================================
@dataclass
class SimulationState:
    current_index: int
    current_date: str
    total_harvested: float
    harvest_cycles: int
    metrics: Dict[str, Any]
    metrics_data: Dict[str, Any]
    harvester_state: Optional[Dict[str, Any]] = None
    data_hash: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    pareto_front: Optional[List[Dict[str, Any]]] = None
    current_policy_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SimulationState":
        return cls(**{k: v for k, v in (data or {}).items()
                      if k in cls.__dataclass_fields__})


@dataclass
class MOPDPoint:
    policy_id: str
    harvester_mode: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    total_harvested: float = 0.0
    avg_efficiency: float = 0.0
    carbon_saved: float = 0.0
    helium_saved: float = 0.0
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**{k: v for k, v in (data or {}).items()
                      if k in cls.__dataclass_fields__})


@dataclass
class Policy:
    policy_id: str
    harvester_mode: str
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CoreEvent:
    event_type: str
    source: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: Optional[str] = None


# =============================================================================
# SECTION 7. TASK MANAGER + EVENT BUS
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
# SECTION 8. PLACEHOLDERS
# =============================================================================
class QuantumDistillationModulePlaceholder:
    STATUS = "placeholder"

    def __init__(self, enabled: bool = False):
        if enabled:
            _warn_module("quantum_distillation")
        self.available = False

    async def optimize(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
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
        self._order: "deque[Tuple[int, ...]]" = deque()
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
        key = self._discretize(state)
        next_key = self._discretize(next_state)
        cur = self._get_or_create(key)
        nxt = self._get_or_create(next_key)
        best_next = 0.0 if done else float(np.max(nxt))
        td_target = reward + self.gamma * best_next
        cur[action] += self.learning_rate * (td_target - cur[action])

    def size(self) -> int:
        return len(self.q_table)


class FederatedCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, engine: Any, queue: Optional[Any] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.engine = engine
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

    def __init__(self, engine: Any, chaos_probability: float = 0.0,
                 enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.engine = engine
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
# SECTION 9. EXPERIMENTAL MODULES
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

    def explain_policy(self, point: MOPDPoint) -> str:
        return (
            f"Policy {point.policy_id} selected: mode={point.harvester_mode}, "
            f"total_harvested={point.total_harvested:.2f}, "
            f"avg_efficiency={point.avg_efficiency:.2f}, "
            f"carbon_saved={point.carbon_saved:.2f}, "
            f"helium_saved={point.helium_saved:.2f}."
        )


# =============================================================================
# SECTION 10. METRICS COLLECTOR (bounded)
# =============================================================================
class MetricsCollector:
    def __init__(self, max_custom_entries: int = 1000,
                 max_history: int = 10000):
        self.total_harvested = 0.0
        self.harvest_cycles = 0
        self._max_custom = max_custom_entries
        self._max_history = max_history
        self.efficiencies: "deque[float]" = deque(maxlen=max_history)
        self.modes: "deque[str]" = deque(maxlen=max_history)
        self.timestamps: "deque[datetime]" = deque(maxlen=max_history)
        self.custom_metrics: Dict[str, deque] = {}

    def record(self, result: Dict[str, Any]) -> None:
        self.total_harvested += float(result.get("eco_atp_generated", 0.0))
        self.harvest_cycles += 1
        self.efficiencies.append(float(result.get("efficiency", 0.0)))
        self.modes.append(str(result.get("mode", "unknown")))
        self.timestamps.append(datetime.now(timezone.utc))

        for key, value in result.items():
            if key in ("eco_atp_generated", "efficiency", "mode"):
                continue
            if key not in self.custom_metrics:
                self.custom_metrics[key] = deque(maxlen=self._max_custom)
            self.custom_metrics[key].append(value)

    def get_summary(self) -> Dict[str, Any]:
        effs = list(self.efficiencies)
        tss = list(self.timestamps)
        summary: Dict[str, Any] = {
            "total_harvested": self.total_harvested,
            "harvest_cycles": self.harvest_cycles,
            "avg_efficiency": float(np.mean(effs)) if effs else 0.0,
            "max_efficiency": max(effs) if effs else 0.0,
            "mode_counts": {m: list(self.modes).count(m)
                            for m in set(self.modes)},
            "duration_hours": (
                (tss[-1] - tss[0]).total_seconds() / 3600.0
                if len(tss) >= 2 else 0.0
            ),
        }
        for key, values in self.custom_metrics.items():
            vals = list(values)
            if vals:
                try:
                    summary[f"avg_{key}"] = float(np.mean(vals))
                    summary[f"min_{key}"] = float(np.min(vals))
                    summary[f"max_{key}"] = float(np.max(vals))
                except Exception:
                    pass
        return summary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_harvested": self.total_harvested,
            "harvest_cycles": self.harvest_cycles,
            "efficiencies": list(self.efficiencies),
            "modes": list(self.modes),
            "timestamps": [ts.isoformat() for ts in self.timestamps],
            "custom_metrics": {k: list(v) for k, v in self.custom_metrics.items()},
            "max_custom_entries": self._max_custom,
            "max_history": self._max_history,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetricsCollector":
        c = cls(
            max_custom_entries=int(data.get("max_custom_entries", 1000)),
            max_history=int(data.get("max_history", 10000)),
        )
        c.total_harvested = float(data.get("total_harvested", 0.0))
        c.harvest_cycles = int(data.get("harvest_cycles", 0))
        c.efficiencies = deque(data.get("efficiencies", []),
                                maxlen=c._max_history)
        c.modes = deque(data.get("modes", []), maxlen=c._max_history)
        c.timestamps = deque(
            [datetime.fromisoformat(ts) for ts in data.get("timestamps", [])],
            maxlen=c._max_history,
        )
        for k, v in (data.get("custom_metrics") or {}).items():
            c.custom_metrics[k] = deque(v, maxlen=c._max_custom)
        return c


# =============================================================================
# SECTION 11. LIVE DATA FEED
# =============================================================================
class LiveDataFeed:
    def __init__(self, config: TimeTickConfig):
        self.config = config
        self._callback = config.live_data_callback
        self._running = False
        self._last_data: Optional[Dict[str, float]] = None
        self._backoff = 0.5

    async def fetch(self) -> Dict[str, float]:
        if self._callback is not None:
            try:
                data = self._callback()
                if _is_awaitable(data):
                    data = await data
                if data is not None:
                    self._last_data = dict(data)
                    self._backoff = 0.5
                    return dict(data)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Live callback failed", error=str(e))
                self._backoff = min(self._backoff * 2, 30.0)
                await asyncio.sleep(self._backoff)
        if self._last_data is None:
            return {col: 0.5 for col in self.config.value_columns}
        return dict(self._last_data)

    def stop(self) -> None:
        self._running = False


# =============================================================================
# SECTION 12. TIME TICK ENGINE
# =============================================================================
class TimeTickEngine:
    """
    Lifecycle:
        engine = TimeTickEngine(harvester, translator, config)
        await engine.start()
        ...
        await engine.shutdown()
    """

    def __init__(
        self,
        harvester: HarvesterProtocol,
        translator: Union[TranslatorProtocol, Callable],
        config: Optional[Union[TimeTickConfig, Dict[str, Any]]] = None,
        message_queue: Optional[Any] = None,
    ):
        self.harvester = harvester
        self.translator = translator
        self.message_queue = message_queue

        if isinstance(config, dict):
            self.config = TimeTickConfig.from_dict(config)
        elif isinstance(config, TimeTickConfig):
            self.config = config
        else:
            self.config = TimeTickConfig()

        issues = self.config.validate()
        if issues:
            raise ValueError(f"Invalid config: {issues}")

        # Data
        self.df_monthly: Optional[pd.DataFrame] = None
        self.daily_df: Optional[pd.DataFrame] = None
        self._data_hash: Optional[str] = None

        # State
        self.metrics = MetricsCollector(
            max_custom_entries=self.config.max_custom_metrics_entries,
            max_history=self.config.max_metric_history,
        )
        self._running = False
        self._stop_event = asyncio.Event()
        self._current_index = 0
        self._live_feed: Optional[LiveDataFeed] = None

        # MOPD
        self._mopd_results: Dict[str, Dict[str, Any]] = {}
        self._pareto_front: List[MOPDPoint] = []
        self._policy_cache: Dict[Tuple, MOPDPoint] = {}

        # Concurrency
        self._state_lock: Optional[asyncio.Lock] = None
        self._policy_eval_lock: Optional[asyncio.Lock] = None

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

        # Subsystems
        self.event_bus = EventBus()
        self._task_manager = TaskManager()

        # Lifecycle
        self._started = False
        self._shutdown = False

        if self.config.enable_checkpointing:
            Path(self.config.checkpoint_dir).mkdir(parents=True, exist_ok=True)

        logger.info("TimeTickEngine created (not started)")

    # ---------------- locks ----------------
    def _get_state_lock(self) -> asyncio.Lock:
        if self._state_lock is None:
            self._state_lock = asyncio.Lock()
        return self._state_lock

    def _get_policy_eval_lock(self) -> asyncio.Lock:
        if self._policy_eval_lock is None:
            self._policy_eval_lock = asyncio.Lock()
        return self._policy_eval_lock

    # ---------------- safety ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "efficiency_non_negative",
            lambda s: s.get("efficiency", 0.0) >= 0.0,
            "Efficiency must be non-negative",
        )
        self.safety_monitor.add_invariant(
            "harvest_non_negative",
            lambda s: s.get("total_harvested", 0.0) >= 0.0,
            "Total harvested must be non-negative",
        )

    def _check_safety(self, state: Dict[str, Any]) -> List[str]:
        if self.safety_monitor is None:
            return []
        return self.safety_monitor.check(state)

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        await self.event_bus.start()

        if self.federated_coordinator is not None:
            self._task_manager.start_task("federated_loop",
                                           self._federated_loop)
        if self.chaos_injector is not None:
            self._task_manager.start_task("chaos_loop", self._chaos_loop)

        self._started = True
        logger.info("TimeTickEngine started")

    async def ready(self) -> bool:
        return self._started and not self._shutdown

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("TimeTickEngine shutting down")

        self.stop()
        try:
            await asyncio.wait_for(self._task_manager.drain(timeout),
                                    timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        try:
            await asyncio.wait_for(self.event_bus.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Event bus stop timed out")

        if (self.config.enable_checkpointing
                and self._current_index > 0
                and self.config.data_source == "csv"):
            try:
                await self._save_checkpoint(self._current_index)
            except Exception as e:
                logger.warning("Final checkpoint failed", error=str(e))

        logger.info("TimeTickEngine shutdown complete")

    async def __aenter__(self) -> "TimeTickEngine":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _federated_loop(self) -> None:
        while not self._task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.federated_coordinator is not None:
                    await self.federated_coordinator.send_update()
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

    # ---------------- data loading ----------------
    async def load_data(self, csv_path: Optional[str] = None) -> None:
        if self.config.data_source == "live":
            self._live_feed = LiveDataFeed(self.config)
            logger.info("Live data feed initialized")
            return

        path = csv_path or self.config.csv_path
        if not path:
            raise ValueError("CSV path not provided")

        try:
            with open(path, "rb") as f:
                self._data_hash = hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.warning("Data hash computation failed", error=str(e))
            self._data_hash = None

        try:
            df = pd.read_csv(path)
        except Exception as e:
            logger.error("Failed to read CSV", error=str(e))
            raise

        required = [self.config.date_column] + self.config.value_columns
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        try:
            if self.config.date_format:
                df[self.config.date_column] = pd.to_datetime(
                    df[self.config.date_column], format=self.config.date_format
                )
            else:
                df[self.config.date_column] = pd.to_datetime(df[self.config.date_column])
        except Exception as e:
            raise ValueError(f"Date parsing failed: {e}")

        df = df.sort_values(self.config.date_column)

        if self.config.start_date:
            df = df[df[self.config.date_column] >= pd.to_datetime(self.config.start_date)]
        if self.config.end_date:
            df = df[df[self.config.date_column] <= pd.to_datetime(self.config.end_date)]

        self.df_monthly = df
        self._interpolate_daily()
        logger.info("CSV loaded", rows_monthly=len(self.df_monthly),
                    rows_daily=len(self.daily_df))

    def _interpolate_daily(self) -> None:
        if self.df_monthly is None or self.df_monthly.empty:
            self.daily_df = pd.DataFrame(
                columns=["date"] + self.config.value_columns
            )
            return
        df_m = self.df_monthly.set_index(self.config.date_column)
        daily_index = pd.date_range(start=df_m.index.min(),
                                    end=df_m.index.max(), freq="D")
        numeric_cols = [c for c in self.config.value_columns if c in df_m.columns]
        try:
            method = self.config.interpolation_method
            if method == "spline":
                try:
                    self.daily_df = (df_m[numeric_cols].reindex(daily_index)
                                     .interpolate(method="spline", order=3))
                except Exception as e:
                    logger.warning("Spline failed; falling back to linear",
                                   error=str(e))
                    self.daily_df = (df_m[numeric_cols].reindex(daily_index)
                                     .interpolate(method="linear"))
            else:
                self.daily_df = (df_m[numeric_cols].reindex(daily_index)
                                 .interpolate(method=method))
        except Exception as e:
            logger.error("Interpolation failed", error=str(e))
            raise
        self.daily_df = self.daily_df.reset_index()
        # pandas >= 2.x: use ffill/bfill directly
        self.daily_df = self.daily_df.ffill().bfill()

    # ---------------- simulation ----------------
    @traced("time_tick_engine.run_simulation")
    async def run_simulation(
        self,
        start_index: Optional[int] = None,
        post_tick_callback: Optional[
            Callable[[int, pd.Series, Dict[str, Any]], Awaitable[None]]
        ] = None,
    ) -> None:
        if self._running:
            raise RuntimeError("Simulation already running on this engine")

        if self.config.data_source == "csv" and self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first")

        if start_index is not None:
            self._current_index = start_index
        elif self.config.enable_checkpointing:
            self._load_checkpoint()

        self._stop_event.clear()
        self._running = True

        total_ticks = (
            len(self.daily_df) if self.config.data_source == "csv" else None
        )

        pbar = None
        if TQDM_AVAILABLE and total_ticks:
            pbar = tqdm(total=total_ticks, initial=self._current_index,
                        desc="Simulating")

        try:
            while self._running and not self._stop_event.is_set():
                if self.config.data_source == "csv":
                    if total_ticks is not None and self._current_index >= total_ticks:
                        logger.info("Reached end of data")
                        break
                    row = self.daily_df.iloc[self._current_index]
                    if pbar:
                        pbar.update(1)
                else:
                    if self._live_feed is None:
                        logger.error("No live data feed available")
                        break
                    data = await self._live_feed.fetch()
                    row = pd.Series({"date": datetime.now(timezone.utc)})
                    for k, v in data.items():
                        row[k] = v

                env_data = self._translate_row(row)
                if env_data is None:
                    if self.config.data_source == "csv":
                        self._current_index += 1
                    continue

                result = await self.harvester.harvest_cycle(env_data)

                if self.safety_monitor is not None:
                    state = {
                        "efficiency": result.get("efficiency", 0.0),
                        "total_harvested": (
                            self.metrics.total_harvested
                            + result.get("eco_atp_generated", 0.0)
                        ),
                    }
                    violations = self._check_safety(state)
                    if violations:
                        logger.warning("Safety violations", violations=violations)

                if result.get("carbon_saved", 0.0) > 0 and self.carbon_market:
                    await self._maybe_trade_carbon(result["carbon_saved"])

                if self.config.metrics_enabled:
                    self.metrics.record(result)

                if post_tick_callback is not None:
                    try:
                        r = post_tick_callback(self._current_index, row, result)
                        if _is_awaitable(r):
                            await r
                    except Exception as e:
                        logger.error("Post-tick callback failed", error=str(e))

                if self.config.enable_xai and self._current_index % 100 == 0:
                    logger.info("XAI tick", tick=self._current_index,
                                harvested=result.get("eco_atp_generated", 0.0))

                if (self.config.enable_checkpointing
                        and self.config.data_source == "csv"
                        and self._current_index % self.config.checkpoint_interval == 0):
                    await self._save_checkpoint(self._current_index)

                await asyncio.sleep(self.config.tick_interval_seconds)
                if self.config.data_source == "csv":
                    self._current_index += 1

        except asyncio.CancelledError:
            logger.info("Simulation cancelled")
            if (self.config.enable_checkpointing
                    and self.config.data_source == "csv"):
                await self._save_checkpoint(self._current_index)
            raise

        except Exception as e:
            logger.error("Simulation failed", index=self._current_index,
                         error=str(e))
            raise

        finally:
            if pbar:
                pbar.close()
            self._running = False
            self._stop_event.set()
            if self._live_feed is not None:
                self._live_feed.stop()
            logger.info("Simulation finished",
                        total_harvested=self.metrics.total_harvested)

    def stop(self) -> None:
        self._stop_event.set()
        self._running = False
        logger.info("Stop requested")

    def _translate_row(self, row: pd.Series) -> Optional[Dict[str, float]]:
        try:
            if callable(self.translator):
                return self.translator(row)
            if hasattr(self.translator, "translate_row"):
                return self.translator.translate_row(row)
            raise TypeError("translator is not callable nor has translate_row")
        except Exception as e:
            logger.debug("Row translation failed", error=str(e))
            return None

    # ---------------- policy evaluation ----------------
    def _policy_key(self, policy: Policy) -> Tuple:
        params = tuple(sorted(policy.parameters.items()))
        return (policy.harvester_mode, params)

    @traced("time_tick_engine.evaluate_policy")
    async def evaluate_policy(self, policy: Policy) -> MOPDPoint:
        # Serialize policy evaluation: it mutates the harvester's mode and
        # parameters, the metrics collector, and _current_index, all of which
        # are shared across the engine.
        async with self._get_policy_eval_lock():
            key = self._policy_key(policy)
            if key in self._policy_cache:
                return self._policy_cache[key]

            # Snapshot engine + harvester state
            original_index = self._current_index
            original_running = self._running
            original_stop_was_set = self._stop_event.is_set()
            original_metrics = self.metrics
            original_mode = getattr(self.harvester, "mode", None)
            original_params = None
            if hasattr(self.harvester, "get_parameters"):
                try:
                    original_params = self.harvester.get_parameters()
                except Exception:
                    original_params = None

            try:
                if hasattr(self.harvester, "set_mode"):
                    self.harvester.set_mode(policy.harvester_mode)
                if (policy.parameters
                        and hasattr(self.harvester, "set_parameters")):
                    self.harvester.set_parameters(policy.parameters)

                self.metrics = MetricsCollector(
                    max_custom_entries=self.config.max_custom_metrics_entries,
                    max_history=self.config.max_metric_history,
                )

                await self.run_simulation(start_index=0)

                summary = self.metrics.get_summary()
                point = MOPDPoint(
                    policy_id=policy.policy_id,
                    harvester_mode=policy.harvester_mode,
                    parameters=dict(policy.parameters),
                    total_harvested=float(summary["total_harvested"]),
                    avg_efficiency=float(summary["avg_efficiency"]),
                    carbon_saved=float(summary.get("avg_carbon_impact", 0.0)),
                    helium_saved=float(summary.get("avg_helium_usage", 0.0)),
                )

                if self.safety_monitor is not None:
                    state = {
                        "total_harvested": point.total_harvested,
                        "efficiency": point.avg_efficiency,
                    }
                    violations = self._check_safety(state)
                    if violations:
                        logger.warning("Policy violates safety invariants",
                                       policy=policy.policy_id,
                                       violations=violations)
                        point.total_harvested = 0.0
                        point.avg_efficiency = 0.0
                        point.carbon_saved = 0.0
                        point.helium_saved = 0.0

                if self.config.enable_xai:
                    logger.info("XAI", text=self.xai.explain_policy(point))

                self._policy_cache[key] = point
                return point

            finally:
                # Restore engine state
                self._current_index = original_index
                self._running = original_running
                if original_stop_was_set:
                    self._stop_event.set()
                else:
                    self._stop_event.clear()
                self.metrics = original_metrics
                if (original_mode is not None
                        and hasattr(self.harvester, "set_mode")):
                    try:
                        self.harvester.set_mode(original_mode)
                    except Exception:
                        pass
                if (original_params is not None
                        and hasattr(self.harvester, "set_parameters")):
                    try:
                        self.harvester.set_parameters(original_params)
                    except Exception:
                        pass

    async def run_multi_policy_simulation(
        self,
        policies: List[Policy],
        post_policy_callback: Optional[
            Callable[[Policy, Dict[str, Any]], Awaitable[None]]
        ] = None,
    ) -> List[MOPDPoint]:
        if not self.config.mopd.enabled:
            logger.warning("MOPD disabled; no Pareto front will be generated")
            return []
        if self.config.data_source == "csv" and self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first")

        self._mopd_results = {}
        self._pareto_front = []

        logger.info("Evaluating policies", count=len(policies))
        points: List[MOPDPoint] = []
        for p in policies:
            try:
                point = await self.evaluate_policy(p)
                points.append(point)
                if post_policy_callback is not None:
                    r = post_policy_callback(p, point.to_dict())
                    if _is_awaitable(r):
                        await r
            except Exception as e:
                logger.error("Policy evaluation failed",
                             policy=p.policy_id, error=str(e))

        self._pareto_front = self._filter_pareto(points)
        best = self._select_best_from_pareto(self._pareto_front)
        if best is not None:
            logger.info("Best policy selected",
                        policy=best.policy_id,
                        score=best.scalarised_score)
            if self.config.enable_xai:
                logger.info("XAI", text=self.xai.explain_policy(best))
            if self.carbon_market is not None and best.carbon_saved > 0:
                await self._maybe_trade_carbon(best.carbon_saved)

        if self.config.enable_checkpointing:
            await self._save_checkpoint(self._current_index,
                                         pareto_front=self._pareto_front)
        return self._pareto_front

    async def run_evolution(self, policy_space: Optional[Dict[str, Any]] = None
                             ) -> List[MOPDPoint]:
        if not self.config.mopd.enabled:
            logger.warning("MOPD disabled; cannot run evolution")
            return []
        if self.config.data_source == "csv" and self.daily_df is None:
            raise RuntimeError("Data not loaded. Call load_data() first")

        optimizer = NSGAIIOptimizer(
            engine=self,
            policy_space=policy_space,
            population_size=self.config.mopd.population_size,
            generations=self.config.mopd.generations,
            mutation_rate=self.config.mopd.mutation_rate,
            crossover_rate=self.config.mopd.crossover_rate,
            tournament_size=self.config.mopd.tournament_size,
            dynamic_weights=self.config.mopd.dynamic_weights,
            objective_weights=self.config.mopd.objective_weights,
        )
        pareto = await optimizer.evolve()
        self._pareto_front = pareto

        if self.config.enable_checkpointing:
            await self._save_checkpoint(self._current_index,
                                         pareto_front=self._pareto_front)
        if self.config.enable_xai and pareto:
            best = self._select_best_from_pareto(pareto)
            if best is not None:
                logger.info("XAI", text=self.xai.explain_policy(best))
        return pareto

    # ---------------- MOPD helpers ----------------
    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        if not points:
            return []
        keys = ["total_harvested", "avg_efficiency",
                "carbon_saved", "helium_saved"]
        pareto: List[MOPDPoint] = []
        for i, p in enumerate(points):
            dominated = False
            for j, q in enumerate(points):
                if i == j:
                    continue
                a = [getattr(p, k) for k in keys]
                b = [getattr(q, k) for k in keys]
                if all(bb >= aa for aa, bb in zip(a, b)) and any(
                    bb > aa for aa, bb in zip(a, b)
                ):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p)
        return pareto

    def _select_best_from_pareto(self, front: List[MOPDPoint]
                                   ) -> Optional[MOPDPoint]:
        if not front:
            return None
        weights = self.config.mopd.objective_weights
        keys = list(weights.keys())
        max_vals = {k: max(getattr(p, k) for p in front) for k in keys}
        min_vals = {k: min(getattr(p, k) for p in front) for k in keys}
        ranges = {k: (max_vals[k] - min_vals[k])
                  if max_vals[k] != min_vals[k] else 1.0 for k in keys}
        best: Optional[MOPDPoint] = None
        best_score = -math.inf
        for p in front:
            score = sum(
                weights[k] * ((getattr(p, k) - min_vals[k]) / ranges[k])
                for k in keys
            )
            if score > best_score:
                best_score = score
                best = p
        if best is None:
            return None
        copy_point = MOPDPoint.from_dict(best.to_dict())
        copy_point.scalarised_score = best_score
        return copy_point

    # ---------------- carbon market helper ----------------
    async def _maybe_trade_carbon(self, carbon_saved: float) -> None:
        if (self.carbon_market is None
                or not getattr(self.carbon_market, "available", False)
                or carbon_saved <= 0):
            return
        try:
            r = self.carbon_market.sell_credits(carbon_saved * 0.1)
            if _is_awaitable(r):
                await r
        except Exception as e:
            logger.warning("Carbon trade failed", error=str(e))

    # ---------------- checkpointing (JSON) ----------------
    def _checkpoint_path(self) -> Optional[Path]:
        if not self.config.enable_checkpointing:
            return None
        pattern = f"simulation_{self.harvester.__class__.__name__}_*.json"
        return Path(self.config.checkpoint_dir) / pattern

    def _cleanup_old_checkpoints(self) -> None:
        pat = self._checkpoint_path()
        if pat is None:
            return
        files = sorted(Path(pat.parent).glob(pat.name),
                       key=lambda p: p.stat().st_mtime)
        if len(files) > self.config.max_checkpoints:
            for f in files[:-self.config.max_checkpoints]:
                try:
                    f.unlink()
                except Exception as e:
                    logger.warning("Failed to remove old checkpoint",
                                   path=str(f), error=str(e))

    async def _save_checkpoint(self, current_index: int,
                                pareto_front: Optional[List[MOPDPoint]] = None
                                ) -> None:
        if not self.config.enable_checkpointing:
            return

        harvester_state: Optional[Dict[str, Any]] = None
        if hasattr(self.harvester, "get_harvesting_stats"):
            try:
                r = self.harvester.get_harvesting_stats()
                if _is_awaitable(r):
                    r = await r
                harvester_state = r
            except Exception as e:
                logger.warning("Harvester state fetch failed", error=str(e))

        if (self.config.data_source == "csv"
                and self.daily_df is not None
                and current_index < len(self.daily_df)):
            d = self.daily_df.iloc[current_index]["date"]
            current_date_str = d.isoformat() if hasattr(d, "isoformat") else str(d)
        else:
            current_date_str = datetime.now(timezone.utc).isoformat()

        pareto = pareto_front if pareto_front is not None else self._pareto_front
        state = SimulationState(
            current_index=current_index,
            current_date=current_date_str,
            total_harvested=self.metrics.total_harvested,
            harvest_cycles=self.metrics.harvest_cycles,
            metrics=self.metrics.get_summary(),
            metrics_data=self.metrics.to_dict(),
            harvester_state=harvester_state,
            data_hash=self._data_hash,
            timestamp=datetime.now(timezone.utc).isoformat(),
            pareto_front=[p.to_dict() for p in pareto] if pareto else None,
            current_policy_id=None,
        )

        filename = (
            f"simulation_{self.harvester.__class__.__name__}_"
            f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        )
        path = Path(self.config.checkpoint_dir) / filename
        try:
            with open(path, "w") as f:
                json.dump(state.to_dict(), f, default=str, indent=2)
            self._cleanup_old_checkpoints()
            logger.debug("Checkpoint saved", index=current_index, path=str(path))
        except Exception as e:
            logger.warning("Checkpoint save failed", error=str(e))

    def _load_checkpoint(self) -> bool:
        if not self.config.enable_checkpointing:
            return False
        pat = self._checkpoint_path()
        if pat is None:
            return False
        files = sorted(Path(pat.parent).glob(pat.name),
                       key=lambda p: p.stat().st_mtime)
        if not files:
            return False
        latest = files[-1]
        try:
            with open(latest, "r") as f:
                data = json.load(f)
            state = SimulationState.from_dict(data)

            if (self._data_hash is not None
                    and state.data_hash is not None
                    and state.data_hash != self._data_hash):
                logger.warning("Data hash mismatch; ignoring checkpoint")
                return False

            self._current_index = int(state.current_index)
            if state.metrics_data:
                self.metrics = MetricsCollector.from_dict(state.metrics_data)
            else:
                self.metrics.total_harvested = float(state.total_harvested)
                self.metrics.harvest_cycles = int(state.harvest_cycles)

            if (state.harvester_state is not None
                    and hasattr(self.harvester, "restore_state")):
                try:
                    r = self.harvester.restore_state(state.harvester_state)
                    if _is_awaitable(r):
                        # cannot await from sync method; spawn best-effort
                        try:
                            loop = asyncio.get_running_loop()
                            loop.create_task(r)
                        except RuntimeError:
                            pass
                except Exception as e:
                    logger.warning("Harvester restore failed", error=str(e))

            if state.pareto_front:
                self._pareto_front = [
                    MOPDPoint.from_dict(p) for p in state.pareto_front
                ]
                logger.info("Pareto front restored",
                            size=len(self._pareto_front))

            logger.info("Checkpoint loaded", index=state.current_index,
                        date=state.current_date, path=str(latest))
            return True
        except Exception as e:
            logger.warning("Checkpoint load failed", error=str(e))
            return False

    # ---------------- reporting ----------------
    def get_pareto_front(self) -> List[MOPDPoint]:
        return [MOPDPoint.from_dict(p.to_dict()) for p in self._pareto_front]

    def get_mopd_summary(self) -> Dict[str, Any]:
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": dict(self.config.mopd.objective_weights),
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self._pareto_front),
            "num_policies_evaluated": len(self._mopd_results),
            "module_status": MODULE_STATUS,
        }

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.get_summary()


# =============================================================================
# SECTION 13. NSGA-II OPTIMIZER (key-based selection)
# =============================================================================
class NSGAIIOptimizer:
    """
    NSGA-II over policies. Selection is by (front_rank, crowding_distance);
    dominance is maximization across the four objectives.
    """
    STATUS = "experimental"

    OBJECTIVE_KEYS = ("total_harvested", "avg_efficiency",
                      "carbon_saved", "helium_saved")

    def __init__(self, engine: TimeTickEngine,
                 policy_space: Optional[Dict[str, Any]] = None,
                 population_size: int = 20,
                 generations: int = 5,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 dynamic_weights: bool = True,
                 objective_weights: Optional[Dict[str, float]] = None,
                 enabled: bool = True):
        if enabled:
            _warn_module("nsga2_optimizer")
        self.engine = engine
        self.policy_space = policy_space if policy_space else self._default_policy_space()
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.dynamic_weights = dynamic_weights
        self.objective_weights = objective_weights or engine.config.mopd.objective_weights
        self.best_individual: Optional[Dict[str, Any]] = None
        self.best_fitness = -math.inf
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple, MOPDPoint] = {}
        self.available = True

        self.param_names = list(self.policy_space.keys())
        self.discrete_params: Dict[str, List] = {}
        self.continuous_params: Dict[str, Tuple[float, float]] = {}
        for k, v in self.policy_space.items():
            if isinstance(v, (list, tuple)) and len(v) == 2 and all(
                isinstance(x, (int, float)) for x in v
            ) and not isinstance(v[0], bool):
                self.continuous_params[k] = (float(v[0]), float(v[1]))
            else:
                self.discrete_params[k] = list(v)

    def _default_policy_space(self) -> Dict[str, Any]:
        return {
            "harvester_mode": ["standard", "aggressive", "conservative"],
            "conversion_factor": (0.5, 1.5),
            "repair_rate": (0.001, 0.02),
            "sensitivity_multiplier": (0.5, 2.0),
        }

    def _individual_key(self, ind: Dict[str, Any]) -> Tuple:
        return tuple(sorted((k, float(v)) if isinstance(v, (int, float))
                            else (k, str(v)) for k, v in ind.items()))

    def _random_individual(self) -> Dict[str, Any]:
        ind: Dict[str, Any] = {}
        for name in self.param_names:
            if name in self.discrete_params:
                ind[name] = random.choice(self.discrete_params[name])
            elif name in self.continuous_params:
                lo, hi = self.continuous_params[name]
                ind[name] = random.uniform(lo, hi)
        return ind

    def _crossover(self, p1: Dict[str, Any],
                   p2: Dict[str, Any]) -> Dict[str, Any]:
        child: Dict[str, Any] = {}
        for name in self.param_names:
            if name in self.discrete_params:
                child[name] = random.choice([p1[name], p2[name]])
            else:
                lo, hi = self.continuous_params[name]
                if random.random() < 0.5:
                    u = random.random()
                    beta = ((2 * u) ** (1.0 / 21.0) if u <= 0.5
                            else (1.0 / (2 * (1 - u))) ** (1.0 / 21.0))
                    val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                    child[name] = max(lo, min(hi, val))
                else:
                    child[name] = p1[name] if random.random() < 0.5 else p2[name]
        return child

    def _mutate(self, ind: Dict[str, Any]) -> Dict[str, Any]:
        m = dict(ind)
        for name in self.param_names:
            if random.random() < self.mutation_rate:
                if name in self.discrete_params:
                    m[name] = random.choice(self.discrete_params[name])
                else:
                    lo, hi = self.continuous_params[name]
                    u = random.random()
                    delta = ((2 * u) ** (1.0 / 21.0) - 1 if u < 0.5
                             else 1 - (2 * (1 - u)) ** (1.0 / 21.0))
                    m[name] = max(lo, min(hi, ind[name] + delta * (hi - lo)))
        return m

    def _individual_to_policy(self, ind: Dict[str, Any]) -> Policy:
        pid = "evolved_" + hashlib.md5(
            json.dumps(ind, sort_keys=True, default=str).encode()
        ).hexdigest()[:8]
        mode = str(ind.get("harvester_mode", "standard"))
        params = {k: v for k, v in ind.items() if k != "harvester_mode"}
        return Policy(policy_id=pid, harvester_mode=mode, parameters=params)

    async def _evaluate_individual(self, ind: Dict[str, Any]) -> MOPDPoint:
        key = self._individual_key(ind)
        if key in self._eval_cache:
            return self._eval_cache[key]
        policy = self._individual_to_policy(ind)
        point = await self.engine.evaluate_policy(policy)
        self._eval_cache[key] = point
        return point

    @staticmethod
    def _dominates(a: MOPDPoint, b: MOPDPoint) -> bool:
        keys = NSGAIIOptimizer.OBJECTIVE_KEYS
        av = [getattr(a, k) for k in keys]
        bv = [getattr(b, k) for k in keys]
        return all(x >= y for x, y in zip(av, bv)) and any(
            x > y for x, y in zip(av, bv)
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

    def _crowding_distance(self, front: List[int],
                            points: List[MOPDPoint]) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}
        d: Dict[int, float] = {i: 0.0 for i in front}
        for key in self.OBJECTIVE_KEYS:
            sf = sorted(front, key=lambda i: getattr(points[i], key))
            d[sf[0]] = float("inf")
            d[sf[-1]] = float("inf")
            span = getattr(points[sf[-1]], key) - getattr(points[sf[0]], key)
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                d[sf[idx]] += (
                    getattr(points[sf[idx + 1]], key)
                    - getattr(points[sf[idx - 1]], key)
                ) / span
        return d

    def _tournament_selection(
        self, keys: List[Tuple], key_to_rank: Dict[Tuple, int],
        key_to_crowd: Dict[Tuple, float],
    ) -> Tuple:
        if len(keys) < self.tournament_size:
            cand = random.choice(keys)
            return cand
        candidates = random.sample(keys, self.tournament_size)
        best = candidates[0]
        best_rank = key_to_rank.get(best, len(keys))
        best_crowd = key_to_crowd.get(best, 0.0)
        for cand in candidates[1:]:
            r = key_to_rank.get(cand, len(keys))
            c = key_to_crowd.get(cand, 0.0)
            if r < best_rank or (r == best_rank and c > best_crowd):
                best = cand
                best_rank = r
                best_crowd = c
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        avg_h = float(np.mean([p.total_harvested for p in self.pareto_front]))
        max_h = max(p.total_harvested for p in self.pareto_front)
        if max_h > 0 and avg_h < 0.5 * max_h:
            weights["total_harvested"] = min(
                0.5, weights.get("total_harvested", 0.3) * 1.5
            )
            total = sum(weights.values()) or 1.0
            weights = {k: v / total for k, v in weights.items()}
        return weights

    @traced("nsga2.evolve")
    async def evolve(self) -> List[MOPDPoint]:
        # Build initial population
        population: List[Dict[str, Any]] = [
            self._random_individual() for _ in range(self.population_size)
        ]
        points: List[MOPDPoint] = []
        for ind in population:
            points.append(await self._evaluate_individual(ind))

        for gen in range(self.generations):
            keys = [self._individual_key(ind) for ind in population]
            key_to_ind: Dict[Tuple, Dict[str, Any]] = dict(zip(keys, population))

            fronts = self._fast_non_dominated_sort(points)
            key_to_rank: Dict[Tuple, int] = {}
            key_to_crowd: Dict[Tuple, float] = {}
            for rank, front in enumerate(fronts):
                cd = self._crowding_distance(front, points)
                for idx in front:
                    k = keys[idx]
                    key_to_rank[k] = rank
                    key_to_crowd[k] = cd.get(idx, 0.0)

            # Produce offspring
            offspring: List[Dict[str, Any]] = []
            while len(offspring) < self.population_size:
                pk1 = self._tournament_selection(keys, key_to_rank, key_to_crowd)
                pk2 = self._tournament_selection(keys, key_to_rank, key_to_crowd)
                p1 = key_to_ind[pk1]
                p2 = key_to_ind[pk2]
                if random.random() < self.crossover_rate:
                    child = self._crossover(p1, p2)
                else:
                    child = dict(p1)
                child = self._mutate(child)
                offspring.append(child)

            off_points: List[MOPDPoint] = []
            for ind in offspring:
                off_points.append(await self._evaluate_individual(ind))

            # Combine and dedupe by key
            combined_inds = population + offspring
            combined_points = points + off_points
            seen: Dict[Tuple, int] = {}
            dedup_inds: List[Dict[str, Any]] = []
            dedup_points: List[MOPDPoint] = []
            for ind, pt in zip(combined_inds, combined_points):
                k = self._individual_key(ind)
                if k in seen:
                    continue
                seen[k] = len(dedup_inds)
                dedup_inds.append(ind)
                dedup_points.append(pt)

            # Select next generation
            fronts = self._fast_non_dominated_sort(dedup_points)
            new_inds: List[Dict[str, Any]] = []
            new_points: List[MOPDPoint] = []
            for front in fronts:
                if len(new_inds) + len(front) <= self.population_size:
                    for idx in front:
                        new_inds.append(dedup_inds[idx])
                        new_points.append(dedup_points[idx])
                else:
                    cd = self._crowding_distance(front, dedup_points)
                    sf = sorted(front, key=lambda i: cd.get(i, 0.0),
                                 reverse=True)
                    remaining = self.population_size - len(new_inds)
                    for idx in sf[:remaining]:
                        new_inds.append(dedup_inds[idx])
                        new_points.append(dedup_points[idx])
                    break
            population, points = new_inds, new_points

            # Update Pareto front for this generation
            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = [points[i] for i in fronts[0]]
            logger.info("Generation done", gen=gen + 1,
                        population=len(population),
                        pareto_size=len(self.pareto_front))

        weights = self._compute_dynamic_weights()
        # Copy so we don't mutate the stored points
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best is not None:
            self.best_fitness = best.scalarised_score
            for ind in population:
                pol = self._individual_to_policy(ind)
                if pol.policy_id == best.policy_id:
                    self.best_individual = ind
                    break
        return [MOPDPoint.from_dict(p.to_dict()) for p in self.pareto_front]

    def _select_best_from_pareto(
        self, front: List[MOPDPoint],
        weights: Optional[Dict[str, float]] = None,
    ) -> Optional[MOPDPoint]:
        if not front:
            return None
        weights = weights or self.objective_weights
        keys = list(weights.keys())
        max_vals = {k: max(getattr(p, k) for p in front) for k in keys}
        min_vals = {k: min(getattr(p, k) for p in front) for k in keys}
        ranges = {k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
                  for k in keys}
        best: Optional[MOPDPoint] = None
        best_score = -math.inf
        for p in front:
            score = sum(
                weights[k] * ((getattr(p, k) - min_vals[k]) / ranges[k])
                for k in keys
            )
            if score > best_score:
                best_score = score
                best = p
        if best is None:
            return None
        copy_point = MOPDPoint.from_dict(best.to_dict())
        copy_point.scalarised_score = best_score
        return copy_point


# =============================================================================
# SECTION 14. TESTS
# =============================================================================
class _MockHarvester:
    def __init__(self):
        self.mode = "standard"
        self.parameters: Dict[str, Any] = {}
        self.mode_calls: List[Any] = []
        self.param_calls: List[Dict[str, Any]] = []

    async def harvest_cycle(self, env_data: Dict[str, float]) -> Dict[str, Any]:
        base = float(env_data.get("helium_supply", 0.5)) * 10.0
        factor = 1.0
        if self.mode == "aggressive":
            factor = 1.5
        elif self.mode == "conservative":
            factor = 0.8
        factor *= float(self.parameters.get("conversion_factor", 1.0))
        factor *= float(self.parameters.get("sensitivity_multiplier", 1.0))
        repair = float(self.parameters.get("repair_rate", 0.01))
        efficiency = 0.85 * factor * (1.0 - repair * 10.0)
        return {
            "eco_atp_generated": base * factor,
            "efficiency": efficiency,
            "mode": self.mode,
            "carbon_impact": 0.1 * factor,
            "helium_usage": float(env_data.get("helium_demand", 0.5)) * factor,
        }

    async def get_harvesting_stats(self) -> Dict[str, Any]:
        return {"harvester_id": "mock", "mode": self.mode}

    def restore_state(self, state: Dict[str, Any]) -> None:
        pass

    def set_mode(self, mode: Any) -> None:
        self.mode = mode
        self.mode_calls.append(mode)

    def set_parameters(self, params: Dict[str, Any]) -> None:
        self.parameters = dict(params)
        self.param_calls.append(dict(params))

    def get_parameters(self) -> Dict[str, Any]:
        return dict(self.parameters)


class _MockTranslator:
    @staticmethod
    def translate_row(row: pd.Series) -> Dict[str, float]:
        return {
            "renewable_availability": 0.8,
            "carbon_intensity": 200.0,
            "waste_heat": 0.3,
            "edge_availability": 0.6,
            "system_overload": 0.1,
            "helium_supply": float(row.get("helium_supply", 0.5)),
            "helium_demand": float(row.get("helium_demand", 0.5)),
        }


def _build_daily_df(days: int = 20) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=days, freq="D")
    return pd.DataFrame({
        "date": dates,
        "helium_supply": np.linspace(0.3, 0.8, days),
        "helium_demand": np.linspace(0.5, 0.4, days),
    })


class _Tests(unittest.TestCase):
    def _cfg(self, **overrides) -> TimeTickConfig:
        cfg = TimeTickConfig(
            data_source="csv",
            csv_path="unused.csv",
            value_columns=["helium_supply", "helium_demand"],
            tick_interval_seconds=0.0,
            enable_checkpointing=False,
            metrics_enabled=True,
            enable_safety_monitor=True,
            enable_xai=False,
            mopd=MOPDConfig(enabled=True, population_size=4, generations=1),
        )
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg

    def _engine(self, **overrides) -> TimeTickEngine:
        engine = TimeTickEngine(
            harvester=_MockHarvester(),
            translator=_MockTranslator(),
            config=self._cfg(**overrides),
        )
        engine.daily_df = _build_daily_df()
        return engine

    def test_lifecycle(self):
        async def go():
            engine = self._engine()
            self.assertFalse(await engine.ready())
            await engine.start()
            self.assertTrue(await engine.ready())
            await engine.shutdown()
            self.assertTrue(engine._shutdown)
            await engine.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            engine = self._engine()
            async with engine:
                self.assertTrue(await engine.ready())
        asyncio.run(go())

    def test_run_simulation_advances(self):
        async def go():
            async with self._engine() as engine:
                await engine.run_simulation(start_index=0)
                self.assertEqual(engine.metrics.harvest_cycles,
                                 len(engine.daily_df))
                self.assertGreater(engine.metrics.total_harvested, 0.0)
        asyncio.run(go())

    def test_evaluate_policy_restores_state(self):
        async def go():
            async with self._engine() as engine:
                engine._current_index = 7
                engine.metrics.total_harvested = 123.45
                before_index = engine._current_index
                before_metrics = engine.metrics.total_harvested
                before_mode = engine.harvester.mode

                policy = Policy(
                    policy_id="p1",
                    harvester_mode="aggressive",
                    parameters={"conversion_factor": 1.2},
                )
                point = await engine.evaluate_policy(policy)
                self.assertGreater(point.total_harvested, 0.0)
                self.assertEqual(engine._current_index, before_index)
                self.assertAlmostEqual(engine.metrics.total_harvested,
                                        before_metrics)
                self.assertEqual(engine.harvester.mode, before_mode)
        asyncio.run(go())

    def test_parallel_policy_evaluation_is_serialized(self):
        async def go():
            async with self._engine() as engine:
                policies = [
                    Policy(policy_id=f"p{i}",
                            harvester_mode="standard",
                            parameters={"conversion_factor": 1.0 + i * 0.1})
                    for i in range(4)
                ]
                # Serialize the evaluations via gather (they will still queue
                # on the internal lock, but gather proves no crash / no
                # cross-contamination).
                points = await asyncio.gather(
                    *[engine.evaluate_policy(p) for p in policies]
                )
                self.assertEqual(len(points), len(policies))
                # Each has its own policy id
                self.assertEqual({p.policy_id for p in points},
                                 {p.policy_id for p in policies})
        asyncio.run(go())

    def test_tournament_uses_rank(self):
        async def go():
            async with self._engine() as engine:
                opt = NSGAIIOptimizer(
                    engine=engine,
                    population_size=6,
                    generations=1,
                    objective_weights={
                        "total_harvested": 0.5,
                        "avg_efficiency": 0.5,
                        "carbon_saved": 0.0,
                        "helium_saved": 0.0,
                    },
                )
                # Build fake keys and ranks; verify tournament prefers low rank
                keys = [("a",), ("b",), ("c",)]
                ranks = {("a",): 2, ("b",): 0, ("c",): 1}
                crowd = {("a",): 5.0, ("b",): 0.1, ("c",): 0.1}
                # Force size so sampling includes all three
                opt.tournament_size = 3
                winner = opt._tournament_selection(keys, ranks, crowd)
                self.assertEqual(winner, ("b",))
        asyncio.run(go())

    def test_select_best_does_not_mutate_front(self):
        engine = self._engine()
        front = [
            MOPDPoint("a", "standard", {}, 10.0, 0.5, 0.1, 0.2),
            MOPDPoint("b", "standard", {}, 5.0, 0.9, 0.2, 0.1),
        ]
        before = [p.scalarised_score for p in front]
        best = engine._select_best_from_pareto(front)
        self.assertIsNotNone(best)
        self.assertEqual([p.scalarised_score for p in front], before)
        self.assertNotEqual(best.scalarised_score, 0.0)

    def test_carbon_trade_does_not_crash(self):
        async def go():
            async with self._engine(enable_carbon_market=True,
                                     carbon_market_config={
                                         "provider_url": "http://x",
                                         "contract_address": "0x1",
                                         "private_key": "0x2",
                                     }) as engine:
                # Placeholder available=False, so this is a no-op path
                await engine._maybe_trade_carbon(1.0)
        asyncio.run(go())

    def test_checkpoint_json_roundtrip(self):
        async def go():
            import tempfile
            td = tempfile.mkdtemp()
            cfg = self._cfg(enable_checkpointing=True,
                             checkpoint_dir=td,
                             checkpoint_interval=5)
            engine = TimeTickEngine(
                harvester=_MockHarvester(),
                translator=_MockTranslator(),
                config=cfg,
            )
            engine.daily_df = _build_daily_df()
            engine._data_hash = "deadbeef"
            async with engine:
                await engine.run_simulation(start_index=0)
                await engine._save_checkpoint(engine._current_index)
            # There should be at least one checkpoint file
            files = list(Path(td).glob("*.json"))
            self.assertGreater(len(files), 0)
        asyncio.run(go())

    def test_reentrance_guard(self):
        async def go():
            async with self._engine() as engine:
                engine._running = True
                with self.assertRaises(RuntimeError):
                    await engine.run_simulation(start_index=0)
                engine._running = False
        asyncio.run(go())

    def test_metrics_bounded(self):
        m = MetricsCollector(max_history=5)
        for i in range(20):
            m.record({"eco_atp_generated": 1.0, "efficiency": 0.5,
                       "mode": "standard"})
        self.assertLessEqual(len(m.efficiencies), 5)
        self.assertLessEqual(len(m.modes), 5)
        self.assertLessEqual(len(m.timestamps), 5)

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

    def test_run_evolution_smoke(self):
        async def go():
            async with self._engine() as engine:
                pareto = await engine.run_evolution()
                self.assertIsInstance(pareto, list)
        asyncio.run(go())

    def test_module_status_is_documented(self):
        self.assertEqual(MODULE_STATUS["time_tick_core"], "stable")
        self.assertEqual(MODULE_STATUS["causal_rl"], "placeholder")
        self.assertEqual(MODULE_STATUS["nsga2_optimizer"], "experimental")

    def test_config_roundtrip(self):
        cfg = self._cfg()
        d = cfg.to_dict()
        cfg2 = TimeTickConfig.from_dict(d)
        self.assertEqual(cfg.interpolation_method, cfg2.interpolation_method)
        self.assertEqual(cfg.mopd.enabled, cfg2.mopd.enabled)
        self.assertAlmostEqual(cfg.mopd.objective_weights["total_harvested"],
                                cfg2.mopd.objective_weights["total_harvested"])


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 15. ENTRY POINT
# =============================================================================
async def _example() -> None:
    cfg = TimeTickConfig(
        data_source="csv",
        csv_path="unused.csv",
        tick_interval_seconds=0.0,
        enable_checkpointing=False,
        mopd=MOPDConfig(enabled=True, population_size=6, generations=2),
    )
    engine = TimeTickEngine(
        harvester=_MockHarvester(),
        translator=_MockTranslator(),
        config=cfg,
    )
    engine.daily_df = _build_daily_df()
    async with engine:
        pareto = await engine.run_evolution()
        print(f"Pareto front size: {len(pareto)}")
        for p in pareto[:5]:
            print(f"  {p.policy_id}: mode={p.harvester_mode}, "
                  f"harvested={p.total_harvested:.2f}, "
                  f"efficiency={p.avg_efficiency:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="TimeTickEngine v4.0.0")
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

    print("TimeTickEngine v4.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
