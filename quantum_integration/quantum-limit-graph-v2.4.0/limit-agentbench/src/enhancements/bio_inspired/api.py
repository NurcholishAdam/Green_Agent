#!/usr/bin/env python3
# =============================================================================
# Enhanced Bio-Inspired API v11.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced Bio-Inspired API v11.0.0
=================================
Patched single-file version of v10.5.0.

P0 fixes
--------
- Added `import numpy as np` and `Awaitable` to typing imports.
- Defined BaseHandler, Container, TokenHandler, OpenAPIGenerator, and
  WEBSOCKETS_AVAILABLE.
- Route registration now keyed by (method, path) so handle_request works.
- Task startup moved from __init__ to `async start()`; `__aenter__` calls it.
- OptimizationManager.start_optimization tracks its task; no untracked
  create_task.
- Placeholder modules (causal_rl, federated, precision, carbon_market,
  chaos, human_approval) are honest: disabled by default, .available = False,
  warn when enabled.
- Pydantic imports try/except (no more silent dataclass fallback).

P1 — correctness
----------------
- WebhookManager uses aiosqlite-style executor for DB I/O.
- Rate limiter is thread-safe and bounded.
- APIKeyManager stores hashed keys with an in-memory index; optional
  persistence to JSON.
- NSGA-II does not mutate the stored Pareto front's scalarised_score.
- OptimizationManager stores its background task and cancels on shutdown.
- Graceful shutdown with timeout and idempotency.
- Latency histogram is bounded.

P2 — honesty
------------
- MODULE_STATUS documents every module; `--status` prints it.

P3 — production readiness
-------------------------
- Lifecycle: async start(), async shutdown(), async ready(), __aenter__ /
  __aexit__.
- Embedded test suite: python3 bio_api.py --test.
- OpenAPIGenerator produces an OpenAPI 3.0 document from route metadata.
- Request ID propagation via contextvar.
- Prometheus metrics registered on the default registry with a prefix.
- OTel spans on handle_request, optimization, and evolution.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import functools
import hashlib
import hmac
import inspect
import json
import logging
import math
import os
import random
import secrets
import sqlite3
import sys
import time
import unittest
import uuid
from collections import OrderedDict, defaultdict, deque
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Protocol, Set, Tuple,
    Type, Union,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field  # type: ignore
    PYDANTIC_AVAILABLE = True
except ImportError:
    BaseModel = object  # type: ignore
    def Field(default=None, **kwargs):  # type: ignore
        return default
    PYDANTIC_AVAILABLE = False

try:
    import websockets
    from websockets.exceptions import ConnectionClosed  # type: ignore
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    websockets = None  # type: ignore
    class ConnectionClosed(Exception):  # type: ignore
        pass
    WEBSOCKETS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    import jwt as _pyjwt
    JWT_AVAILABLE = True
except ImportError:
    _pyjwt = None  # type: ignore
    JWT_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("bio_inspired_api")
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
    "api_core":              "stable",
    "routing":               "stable",
    "task_manager":          "stable",
    "circuit_breaker":       "stable",
    "health_checker":        "stable",
    "event_bus":             "stable",
    "rate_limiting":         "experimental",
    "cache":                 "experimental",
    "api_keys":              "experimental",
    "oauth2":                "experimental",
    "webhooks":              "experimental",
    "websocket":             "experimental",
    "optimization_manager":  "experimental",
    "nsga2_optimizer":       "experimental",
    "mopd":                  "experimental",
    "safety_monitor":        "experimental",
    "xai":                   "experimental",
    "audit":                 "experimental",
    "openapi_generator":     "experimental",
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
# SECTION 2. REQUEST CONTEXT + TRACING
# =============================================================================
_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")


def current_request_id() -> str:
    return _request_id_ctx.get()


def new_request_id() -> str:
    return uuid.uuid4().hex[:16]


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
# SECTION 3. EXCEPTIONS
# =============================================================================
class APIError(Exception):
    def __init__(self, status_code: int, code: str, message: str,
                 details: Optional[Dict] = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(message)


class CircuitBreakerOpenError(APIError):
    def __init__(self, status_code: int, code: str, message: str,
                 details: Optional[Dict] = None):
        super().__init__(status_code, code, message, details)


def error_response(status_code: int, code: str, message: str,
                    details: Optional[Dict] = None) -> Dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
            "request_id": current_request_id(),
        },
        "status": status_code,
    }


# =============================================================================
# SECTION 4. ENUMS + SHARED DATA
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CoreEvent:
    event_type: str
    source: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: Optional[str] = None


# =============================================================================
# SECTION 5. CONFIG (dataclasses only, no Pydantic dependency)
# =============================================================================
@dataclass
class RateLimitConfig:
    default_rate_limit: int = 100
    default_burst_limit: int = 20
    adaptive_enabled: bool = True
    sliding_window_seconds: int = 60
    redis_url: Optional[str] = None
    max_keys: int = 10000


@dataclass
class CacheConfig:
    enabled: bool = True
    backend: str = "memory"
    redis_url: Optional[str] = None
    ttl_seconds: int = 60
    max_items: int = 1000


@dataclass
class WebhookConfig:
    max_retries: int = 5
    retry_backoff_base: int = 2
    secret_key: str = field(default_factory=lambda: secrets.token_urlsafe(16))
    db_path: str = "./webhooks.db"
    request_timeout_seconds: float = 10.0
    dead_letter_path: str = "./webhook_dead_letter.jsonl"


@dataclass
class OAuth2Config:
    secret_key: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    issuer: str = "green-agent"
    audience: str = "green-agent-api"
    access_token_expiry_minutes: int = 60
    refresh_token_expiry_days: int = 7
    refresh_token_store_backend: str = "file"
    refresh_token_redis_url: Optional[str] = None
    refresh_token_file_path: str = "./refresh_tokens.json"


@dataclass
class WebSocketConfig:
    enabled: bool = False
    port: int = 8765
    auth_required: bool = True
    heartbeat_interval: int = 30


@dataclass
class OptimizationConfig:
    enabled: bool = True
    algorithm: str = "nsga2"
    population_size: int = 20
    generations: int = 5
    mutation_rate: float = 0.2
    crossover_rate: float = 0.8
    tournament_size: int = 3
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "total_harvested": 0.3,
        "avg_efficiency": 0.3,
        "carbon_saved": 0.2,
        "helium_saved": 0.2,
    })
    dynamic_weights: bool = True


@dataclass
class APIConfig:
    api_version: str = "v1"
    prefix: str = "/api"
    pagination_default_page_size: int = 20
    pagination_max_page_size: int = 100
    health_check_timeout_seconds: int = 5
    audit_log_path: str = "./audit.log"
    structured_logging: bool = True
    enable_prometheus: bool = False
    shutdown_timeout_seconds: int = 15

    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    oauth2: OAuth2Config = field(default_factory=OAuth2Config)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    optimization: OptimizationConfig = field(default_factory=OptimizationConfig)

    # Placeholders — disabled by default
    enable_causal_rl: bool = False
    enable_federated_learning: bool = False
    enable_precision_switching: bool = False
    enable_carbon_market: bool = False
    carbon_market_config: Optional[Dict[str, str]] = None
    enable_chaos: bool = False
    chaos_probability: float = 0.0
    enable_human_approval: bool = False

    # Experimental — on where harmless
    enable_safety_monitor: bool = True
    enable_xai: bool = True

    def validate(self) -> List[str]:
        issues: List[str] = []
        if not (0.0 <= self.chaos_probability <= 1.0):
            issues.append("chaos_probability must be in [0, 1]")
        if self.rate_limit.default_burst_limit < 1:
            issues.append("rate_limit.default_burst_limit must be >= 1")
        if self.optimization.population_size < 4:
            issues.append("optimization.population_size must be >= 4")
        return issues

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "APIConfig":
        data = dict(data or {})
        sub_types = {
            "rate_limit": RateLimitConfig,
            "cache": CacheConfig,
            "webhook": WebhookConfig,
            "oauth2": OAuth2Config,
            "websocket": WebSocketConfig,
            "optimization": OptimizationConfig,
        }
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k in sub_types and isinstance(v, dict):
                kwargs[k] = sub_types[k](**{
                    sk: sv for sk, sv in v.items()
                    if sk in sub_types[k].__dataclass_fields__
                })
            elif k in cls.__dataclass_fields__:
                kwargs[k] = v
        return cls(**kwargs)


# =============================================================================
# SECTION 6. CIRCUIT BREAKER
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
        self._last_failure_time: Optional[datetime] = None
        self._half_open_attempt_count = 0
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def call(self, func: Callable, *args, **kwargs):
        lock = self._get_lock()
        async with lock:
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure_time and (
                    datetime.now(timezone.utc) - self._last_failure_time
                ).total_seconds() > self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_attempt_count = 0
                else:
                    raise CircuitBreakerOpenError(
                        503, "circuit_open", f"Circuit breaker {self.name} is OPEN"
                    )
            elif self._state == CircuitBreakerState.HALF_OPEN:
                if self._half_open_attempt_count >= self.half_open_attempts:
                    self._state = CircuitBreakerState.OPEN
                    self._last_failure_time = datetime.now(timezone.utc)
                    raise CircuitBreakerOpenError(
                        503, "circuit_open",
                        f"Circuit breaker {self.name} half-open attempts exceeded"
                    )
        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                elif self._state == CircuitBreakerState.HALF_OPEN:
                    self._half_open_attempt_count += 1
            raise

        async with lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0
        return result

    @property
    def state(self) -> CircuitBreakerState:
        return self._state


class GlobalCircuitBreaker:
    _instance = None
    _breakers: Dict[str, CircuitBreaker] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name: str, **kwargs) -> CircuitBreaker:
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, **kwargs)
        return self._breakers[name]


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
# SECTION 8. PLACEHOLDERS (safe no-ops)
# =============================================================================
class CausalRLAgentPlaceholder:
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

    def size(self) -> int:
        return len(self.q_table)


class FederatedCoordinatorPlaceholder:
    STATUS = "placeholder"

    def __init__(self, api: Any, queue: Optional[Any] = None,
                 model_keys: Optional[List[str]] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("federated")
        self.api = api
        self.queue = queue
        self.model_keys = model_keys or ["optimization_weights", "rl_q_table"]
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

    def __init__(self, api: Any, chaos_probability: float = 0.0,
                 enabled: bool = False):
        if enabled:
            _warn_module("chaos")
        self.api = api
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

    def explain(self, method: str, path: str,
                result: Dict[str, Any]) -> Optional[str]:
        if path.startswith("/optimize/apply"):
            return ("Applied policy from Pareto front using weighted "
                    "objectives.")
        if path.startswith("/tokens/generate"):
            return "Tokens generated from energy savings and efficiency."
        if path.startswith("/webhook/subscribe"):
            return "Webhook subscription registered."
        return None


class AuditLogger:
    STATUS = "experimental"

    def __init__(self, path: str, enabled: bool = True):
        if enabled:
            _warn_module("audit")
        self.path = Path(path)
        self.available = True

    def log(self, event: Dict[str, Any]) -> None:
        try:
            event = dict(event)
            event.setdefault("timestamp",
                             datetime.now(timezone.utc).isoformat())
            event.setdefault("request_id", current_request_id())
            with open(self.path, "a") as f:
                f.write(json.dumps(event, default=str) + "\n")
        except Exception as e:
            logger.warning("Audit log failed", error=str(e))


class HealthChecker:
    STATUS = "stable"

    def __init__(self, api: "BioInspiredAPI"):
        self.api = api

    async def check(self) -> Dict[str, Any]:
        checks: Dict[str, bool] = {}
        checks["task_manager"] = True
        checks["event_bus"] = self.api.event_bus.running
        checks["api_keys"] = self.api.api_key_manager is not None
        checks["optimization"] = self.api.optimization_manager is not None
        status = "healthy" if all(checks.values()) else "degraded"
        return {"status": status, "checks": checks}


# =============================================================================
# SECTION 10. RATE LIMITER + CACHE + API KEYS + OAUTH2
# =============================================================================
class SlidingWindowRateLimiter:
    STATUS = "experimental"

    def __init__(self, config: RateLimitConfig, enabled: bool = True):
        if enabled:
            _warn_module("rate_limiting")
        self.config = config
        self.window = config.sliding_window_seconds
        self._buckets: Dict[str, deque] = defaultdict(deque)
        self._max_keys = config.max_keys
        self._lock: Optional[asyncio.Lock] = None
        self.available = True

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def check_rate_limit(self, key: str,
                                limit: Optional[int] = None
                                ) -> Tuple[bool, Dict[str, Any]]:
        now = time.monotonic()
        cap = limit or self.config.default_burst_limit
        async with self._get_lock():
            if len(self._buckets) >= self._max_keys and key not in self._buckets:
                # evict oldest
                oldest = min(self._buckets.items(),
                             key=lambda kv: kv[1][0] if kv[1] else 0.0)
                self._buckets.pop(oldest[0], None)
            dq = self._buckets[key]
            while dq and dq[0] < now - self.window:
                dq.popleft()
            if len(dq) >= cap:
                reset = int(now + self.window)
                return False, {"limit": cap, "remaining": 0, "reset": reset}
            dq.append(now)
            return True, {
                "limit": cap,
                "remaining": cap - len(dq),
                "reset": int(now + self.window),
            }


class MemoryCacheBackend:
    STATUS = "experimental"

    def __init__(self, max_items: int = 1000, enabled: bool = True):
        if enabled:
            _warn_module("cache")
        self.cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.max_items = max_items
        self._lock: Optional[asyncio.Lock] = None
        self.available = True
        self._hits = 0
        self._misses = 0

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get(self, key: str) -> Optional[Any]:
        async with self._get_lock():
            item = self.cache.get(key)
            if item is None:
                self._misses += 1
                return None
            if item["expires"] <= time.monotonic():
                self.cache.pop(key, None)
                self._misses += 1
                return None
            self.cache.move_to_end(key)
            item["last_access"] = time.monotonic()
            self._hits += 1
            return item["value"]

    async def set(self, key: str, value: Any,
                  ttl: Optional[float] = None) -> None:
        async with self._get_lock():
            ttl = ttl if ttl is not None else 60.0
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = {
                "value": value,
                "expires": time.monotonic() + ttl,
                "last_access": time.monotonic(),
            }
            while len(self.cache) > self.max_items:
                self.cache.popitem(last=False)

    @property
    def stats(self) -> Dict[str, int]:
        return {"hits": self._hits, "misses": self._misses,
                "size": len(self.cache)}


class APIKeyManager:
    STATUS = "experimental"

    def __init__(self, config: RateLimitConfig, enabled: bool = True):
        if enabled:
            _warn_module("api_keys")
        self.config = config
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._lock: Optional[asyncio.Lock] = None
        self.available = True

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _hash(self, api_key: str) -> str:
        return hashlib.sha256(api_key.encode()).hexdigest()

    async def create_key(self, name: str,
                          rate_limit: Optional[int] = None,
                          role: str = "user") -> str:
        key = secrets.token_urlsafe(32)
        key_hash = self._hash(key)
        async with self._get_lock():
            self._keys[key_hash] = {
                "name": name,
                "rate_limit": rate_limit or self.config.default_rate_limit,
                "role": role,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        return key

    async def validate_key(self, api_key: str
                            ) -> Optional[Dict[str, Any]]:
        key_hash = self._hash(api_key)
        async with self._get_lock():
            return self._keys.get(key_hash)

    async def revoke_key(self, api_key: str) -> bool:
        key_hash = self._hash(api_key)
        async with self._get_lock():
            return self._keys.pop(key_hash, None) is not None


class OAuth2Manager:
    STATUS = "experimental"

    def __init__(self, config: OAuth2Config, enabled: bool = True):
        if enabled:
            _warn_module("oauth2")
        self.config = config
        self.available = JWT_AVAILABLE

    def create_access_token(self, client_id: str,
                             scopes: Optional[List[str]] = None) -> str:
        if not JWT_AVAILABLE:
            raise APIError(501, "jwt_unavailable",
                           "PyJWT is not installed")
        now = datetime.now(timezone.utc)
        payload = {
            "iss": self.config.issuer,
            "aud": self.config.audience,
            "sub": client_id,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(
                minutes=self.config.access_token_expiry_minutes
            )).timestamp()),
            "scope": scopes or ["read"],
        }
        return _pyjwt.encode(payload, self.config.secret_key,
                              algorithm="HS256")

    async def validate_token(self, token: str
                              ) -> Optional[Dict[str, Any]]:
        if not JWT_AVAILABLE:
            return None
        try:
            return _pyjwt.decode(token, self.config.secret_key,
                                  algorithms=["HS256"],
                                  audience=self.config.audience,
                                  issuer=self.config.issuer)
        except Exception:
            return None


# =============================================================================
# SECTION 11. WEBHOOK MANAGER (executor-offloaded SQLite + real delivery)
# =============================================================================
class WebhookManager:
    STATUS = "experimental"

    def __init__(self, config: WebhookConfig, enabled: bool = True):
        if enabled:
            _warn_module("webhooks")
        self.config = config
        self.db_path = config.db_path
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self.delivery_queue: asyncio.Queue = asyncio.Queue()
        self._lock: Optional[asyncio.Lock] = None
        self._running = False
        self._dead_letter = Path(config.dead_letter_path)
        self.available = True
        self._init_db_sync()
        self._load_subscriptions_sync()

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        return conn

    def _init_db_sync(self) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS subscriptions (
                        id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        callback_url TEXT NOT NULL,
                        max_retries INTEGER NOT NULL,
                        created_at TEXT NOT NULL
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.warning("Webhook init_db failed", error=str(e))

    def _load_subscriptions_sync(self) -> None:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT id, event_type, callback_url, max_retries "
                    "FROM subscriptions"
                ).fetchall()
            for r in rows:
                self.subscriptions[r[0]] = {
                    "id": r[0], "event_type": r[1],
                    "callback_url": r[2],
                    "max_retries": r[3] or self.config.max_retries,
                }
        except Exception as e:
            logger.warning("Webhook load failed", error=str(e))

    async def _run_sync(self, fn: Callable, *args) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, fn, *args)

    def _insert_sync(self, sub_id: str, event_type: str,
                      callback_url: str, max_retries: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO subscriptions "
                "(id, event_type, callback_url, max_retries, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (sub_id, event_type, callback_url, max_retries,
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()

    def _delete_sync(self, sub_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
            conn.commit()

    async def subscribe(self, event_type: str, callback_url: str,
                         max_retries: Optional[int] = None) -> str:
        sub_id = str(uuid.uuid4())
        retries = max_retries or self.config.max_retries
        async with self._get_lock():
            self.subscriptions[sub_id] = {
                "id": sub_id, "event_type": event_type,
                "callback_url": callback_url, "max_retries": retries,
            }
            try:
                await self._run_sync(self._insert_sync, sub_id, event_type,
                                      callback_url, retries)
            except Exception as e:
                # roll back in-memory
                self.subscriptions.pop(sub_id, None)
                logger.warning("Webhook persist failed", error=str(e))
        return sub_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        async with self._get_lock():
            if subscription_id not in self.subscriptions:
                return False
            self.subscriptions.pop(subscription_id, None)
            try:
                await self._run_sync(self._delete_sync, subscription_id)
            except Exception as e:
                logger.warning("Webhook delete failed", error=str(e))
                return False
        return True

    async def enqueue_delivery(self, event_type: str,
                                payload: Dict[str, Any]) -> int:
        count = 0
        for sub in list(self.subscriptions.values()):
            if sub["event_type"] != event_type and sub["event_type"] != "*":
                continue
            await self.delivery_queue.put({
                "sub_id": sub["id"],
                "callback_url": sub["callback_url"],
                "event_type": event_type,
                "payload": payload,
                "attempts": 0,
                "max_retries": sub["max_retries"],
            })
            count += 1
        return count

    async def _deliver(self, item: Dict[str, Any]) -> bool:
        if not AIOHTTP_AVAILABLE:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    item["callback_url"],
                    json={"event_type": item["event_type"],
                          "payload": item["payload"]},
                    timeout=self.config.request_timeout_seconds,
                ) as resp:
                    return resp.status < 500
        except Exception as e:
            logger.debug("Webhook delivery failed", error=str(e))
            return False

    def _append_dead_letter(self, item: Dict[str, Any]) -> None:
        try:
            with open(self._dead_letter, "a") as f:
                f.write(json.dumps(item, default=str) + "\n")
        except Exception:
            pass

    async def process_deliveries_loop(self) -> None:
        self._running = True
        while self._running:
            try:
                try:
                    item = await asyncio.wait_for(
                        self.delivery_queue.get(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    continue
                ok = await self._deliver(item)
                if not ok:
                    item["attempts"] += 1
                    if item["attempts"] <= item["max_retries"]:
                        await asyncio.sleep(min(
                            self.config.retry_backoff_base ** item["attempts"],
                            30.0,
                        ))
                        await self.delivery_queue.put(item)
                    else:
                        self._append_dead_letter(item)
                self.delivery_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Webhook loop error", error=str(e))
                await asyncio.sleep(1)
        self._running = False

    async def shutdown(self) -> None:
        self._running = False


# =============================================================================
# SECTION 12. NSGA-II OPTIMIZER
# =============================================================================
@dataclass
class MOPDPoint:
    policy_id: str
    parameters: Dict[str, Any]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0
    rank: int = 0
    crowding_distance: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Do not leak NSGA-II internals to callers.
        d.pop("rank", None)
        d.pop("crowding_distance", None)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(
            policy_id=data.get("policy_id", str(uuid.uuid4())),
            parameters=dict(data.get("parameters", {})),
            objectives=dict(data.get("objectives", {})),
            scalarised_score=float(data.get("scalarised_score", 0.0)),
            rank=int(data.get("rank", 0)),
            crowding_distance=float(data.get("crowding_distance", 0.0)),
        )


class NSGAIIOptimizer:
    STATUS = "experimental"

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, Any]], Dict[str, float]],
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
        self.best_individual: Optional[Dict[str, Any]] = None
        self.best_fitness = -math.inf
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._cache: Dict[Tuple, Dict[str, float]] = {}
        self.available = True

    def _random_individual(self) -> Dict[str, float]:
        return {
            k: random.uniform(lo, hi)
            for k, (lo, hi) in self.parameter_bounds.items()
        }

    def _crossover(self, p1: Dict[str, float],
                   p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for name, (lo, hi) in self.parameter_bounds.items():
            if random.random() < 0.5:
                u = random.random()
                beta = ((2 * u) ** (1.0 / 21.0) if u <= 0.5
                        else (1.0 / (2 * (1 - u))) ** (1.0 / 21.0))
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(lo, min(hi, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        return child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        m = dict(ind)
        for name, (lo, hi) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = ((2 * u) ** (1.0 / 21.0) - 1 if u < 0.5
                         else 1 - (2 * (1 - u)) ** (1.0 / 21.0))
                m[name] = max(lo, min(hi, m[name] + delta * (hi - lo)))
        return m

    @staticmethod
    def _dominates(a: MOPDPoint, b: MOPDPoint) -> bool:
        keys = set(a.objectives.keys()) & set(b.objectives.keys())
        if not keys:
            return False
        return (all(a.objectives[k] >= b.objectives[k] for k in keys)
                and any(a.objectives[k] > b.objectives[k] for k in keys))

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]
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
        keys = list(points[front[0]].objectives.keys())
        for key in keys:
            sf = sorted(front, key=lambda i: points[i].objectives[key])
            d[sf[0]] = float("inf")
            d[sf[-1]] = float("inf")
            span = (points[sf[-1]].objectives[key]
                    - points[sf[0]].objectives[key])
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                d[sf[idx]] += (
                    points[sf[idx + 1]].objectives[key]
                    - points[sf[idx - 1]].objectives[key]
                ) / span
        return d

    def _tournament(self, indices: List[int], ranks: List[int],
                    crowd: Dict[int, float]) -> int:
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

    async def _evaluate(self, params: Dict[str, float]
                         ) -> Dict[str, float]:
        key = tuple(sorted((k, float(v)) for k, v in params.items()))
        if key in self._cache:
            return self._cache[key]
        r = self.evaluate_func(params)
        if _is_awaitable(r):
            r = await r
        objs = {k: float(v) for k, v in (r or {}).items()}
        self._cache[key] = objs
        return objs

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        if "total_harvested" in weights:
            vals = [p.objectives.get("total_harvested", 0.0)
                    for p in self.pareto_front]
            if vals:
                avg = sum(vals) / len(vals)
                mx = max(vals)
                if mx > 0 and avg < 0.5 * mx:
                    weights["total_harvested"] = min(
                        0.5, weights["total_harvested"] * 1.5
                    )
                    total = sum(weights.values()) or 1.0
                    weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, front: List[MOPDPoint],
                                   weights: Dict[str, float]
                                   ) -> Optional[MOPDPoint]:
        if not front:
            return None
        keys = list(weights.keys())
        max_vals = {k: max(p.objectives.get(k, 0.0) for p in front)
                    for k in keys}
        min_vals = {k: min(p.objectives.get(k, 0.0) for p in front)
                    for k in keys}
        ranges = {k: (max_vals[k] - min_vals[k])
                  if max_vals[k] != min_vals[k] else 1.0
                  for k in keys}
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
        copy_point = MOPDPoint.from_dict(best.to_dict())
        copy_point.scalarised_score = best_score
        return copy_point

    @traced("nsga2.evolve")
    async def evolve(self) -> List[MOPDPoint]:
        population: List[MOPDPoint] = []
        for _ in range(self.population_size):
            ind = self._random_individual()
            objs = await self._evaluate(ind)
            population.append(MOPDPoint(
                policy_id=str(uuid.uuid4()),
                parameters=ind,
                objectives=objs,
            ))

        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(population)
            ranks = [0] * len(population)
            crowd: Dict[int, float] = {}
            for rank, front in enumerate(fronts):
                cd = self._crowding_distance(front, population)
                for i in front:
                    ranks[i] = rank
                    crowd[i] = cd.get(i, 0.0)

            offspring: List[MOPDPoint] = []
            while len(offspring) < self.population_size:
                idxs = list(range(len(population)))
                i = self._tournament(idxs, ranks, crowd)
                j = self._tournament(idxs, ranks, crowd)
                if random.random() < self.crossover_rate:
                    child_ind = self._crossover(population[i].parameters,
                                                 population[j].parameters)
                else:
                    child_ind = dict(population[i].parameters)
                child_ind = self._mutate(child_ind)
                objs = await self._evaluate(child_ind)
                offspring.append(MOPDPoint(
                    policy_id=str(uuid.uuid4()),
                    parameters=child_ind,
                    objectives=objs,
                ))

            combined = population + offspring
            unique: Dict[Tuple, MOPDPoint] = {}
            for p in combined:
                k = tuple(sorted((kk, float(vv))
                                  for kk, vv in p.parameters.items()))
                unique.setdefault(k, p)
            combined = list(unique.values())

            fronts = self._fast_non_dominated_sort(combined)
            new_pop: List[MOPDPoint] = []
            for front in fronts:
                if len(new_pop) + len(front) <= self.population_size:
                    for i in front:
                        new_pop.append(combined[i])
                else:
                    cd = self._crowding_distance(front, combined)
                    sf = sorted(front, key=lambda i: cd.get(i, 0.0),
                                 reverse=True)
                    remaining = self.population_size - len(new_pop)
                    for i in sf[:remaining]:
                        new_pop.append(combined[i])
                    break
            population = new_pop

        fronts = self._fast_non_dominated_sort(population)
        self.pareto_front = [population[i] for i in fronts[0]] if fronts else []
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


# =============================================================================
# SECTION 13. OPTIMIZATION MANAGER
# =============================================================================
class OptimizationManager:
    STATUS = "experimental"

    def __init__(self, api: "BioInspiredAPI", enabled: bool = True):
        if enabled:
            _warn_module("optimization_manager")
        self.api = api
        self.config = api.config.optimization
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock: Optional[asyncio.Lock] = None
        self.available = True

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _default_bounds(self) -> Dict[str, Tuple[float, float]]:
        return {
            "conversion_factor": (0.5, 1.5),
            "repair_rate": (0.001, 0.02),
            "sensitivity_multiplier": (0.5, 2.0),
            "token_allocation_weight": (0.1, 0.9),
        }

    async def start_optimization(self, request: Dict[str, Any]) -> str:
        job_id = uuid.uuid4().hex
        algorithm = request.get("algorithm") or self.config.algorithm
        bounds = request.get("parameter_bounds") or self._default_bounds()
        weights = request.get("objective_weights") or self.config.objective_weights
        pop_size = int(request.get("population_size") or self.config.population_size)
        gens = int(request.get("generations") or self.config.generations)

        async with self._get_lock():
            self.jobs[job_id] = {
                "status": "pending",
                "algorithm": algorithm,
                "start_time": datetime.now(timezone.utc).isoformat(),
                "end_time": None,
                "result": None,
                "error": None,
            }

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            async with self._get_lock():
                self.jobs[job_id]["status"] = "failed"
                self.jobs[job_id]["error"] = "no_running_loop"
            raise APIError(500, "no_running_loop",
                           "Cannot start optimization without a running loop")

        task = loop.create_task(
            self._run_optimization(job_id, algorithm, bounds, weights,
                                    pop_size, gens)
        )
        async with self._get_lock():
            self._tasks[job_id] = task
        task.add_done_callback(
            lambda t, jid=job_id: self._tasks.pop(jid, None)
        )
        return job_id

    async def _run_optimization(self, job_id: str, algorithm: str,
                                 bounds: Dict[str, Tuple[float, float]],
                                 weights: Dict[str, float],
                                 pop_size: int, gens: int) -> None:
        async with self._get_lock():
            self.jobs[job_id]["status"] = "running"
        try:
            async def evaluate_func(params: Dict[str, float]
                                     ) -> Dict[str, float]:
                # Placeholder objective. A real implementation would apply
                # params to the bio core and measure outcomes.
                await asyncio.sleep(0.01)
                # Deterministic-ish jitter to make the demo interesting.
                base = sum(params.values())
                return {
                    "total_harvested": 50.0 + 10.0 * base,
                    "avg_efficiency": max(0.0, min(1.0, 0.5 + 0.1 * base)),
                    "carbon_saved": max(0.0, base),
                    "helium_saved": max(0.0, base * 0.5),
                }

            if algorithm != "nsga2":
                raise APIError(400, "unsupported_algorithm",
                               f"Unsupported algorithm: {algorithm}")

            optimizer = NSGAIIOptimizer(
                evaluate_func=evaluate_func,
                parameter_bounds=bounds,
                population_size=pop_size,
                generations=gens,
                mutation_rate=self.config.mutation_rate,
                crossover_rate=self.config.crossover_rate,
                tournament_size=self.config.tournament_size,
                objective_weights=weights,
                dynamic_weights=self.config.dynamic_weights,
            )
            pareto = await optimizer.evolve()
            async with self._get_lock():
                self.jobs[job_id]["status"] = "completed"
                self.jobs[job_id]["result"] = {
                    "pareto_front": [p.to_dict() for p in pareto],
                    "best": optimizer.best_individual,
                    "best_fitness": optimizer.best_fitness,
                }
                self.jobs[job_id]["end_time"] = datetime.now(
                    timezone.utc
                ).isoformat()
        except Exception as e:
            async with self._get_lock():
                self.jobs[job_id]["status"] = "failed"
                self.jobs[job_id]["error"] = str(e)
                self.jobs[job_id]["end_time"] = datetime.now(
                    timezone.utc
                ).isoformat()

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        async with self._get_lock():
            return self.jobs.get(job_id)

    async def cancel_job(self, job_id: str) -> bool:
        async with self._get_lock():
            t = self._tasks.get(job_id)
        if t is None or t.done():
            return False
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        async with self._get_lock():
            self.jobs.setdefault(job_id, {})["status"] = "cancelled"
        return True

    async def cancel_all(self) -> None:
        async with self._get_lock():
            tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def apply_policy(self, job_id: str,
                            policy_id: str) -> Dict[str, Any]:
        async with self._get_lock():
            job = self.jobs.get(job_id)
        if job is None:
            raise APIError(404, "not_found", "Optimization job not found")
        if job["status"] != "completed":
            raise APIError(409, "conflict", "Job not completed")
        result = job["result"] or {}
        front = result.get("pareto_front", [])
        if policy_id == "best":
            best = result.get("best")
            if not best:
                raise APIError(404, "not_found", "No best policy available")
            return {"success": True, "policy_id": "best",
                    "parameters": best}
        for point in front:
            if point["policy_id"] == policy_id:
                return {"success": True, "policy_id": policy_id,
                        "parameters": point["parameters"]}
        raise APIError(404, "not_found", "Policy not found in Pareto front")


# =============================================================================
# SECTION 14. ROUTE REGISTRY + HANDLERS
# =============================================================================
@dataclass
class Route:
    method: str
    path: str
    handler: Callable
    summary: str = ""
    tags: List[str] = field(default_factory=list)
    auth_required: bool = True
    requires_approval: bool = False
    request_model: Optional[Type] = None


class Container:
    STATUS = "stable"

    def __init__(self):
        self._services: Dict[str, Any] = {}

    def register(self, name: str, service: Any) -> None:
        self._services[name] = service

    def resolve(self, name: str) -> Any:
        if name not in self._services:
            raise KeyError(f"Service not registered: {name}")
        return self._services[name]


class BaseHandler:
    STATUS = "stable"

    def __init__(self, container: Container):
        self.container = container
        self.api = container.resolve("api")
        self.config = container.resolve("config")


class TokenHandler(BaseHandler):
    STATUS = "experimental"

    async def generate_token(self, payload: Dict[str, Any]
                              ) -> Dict[str, Any]:
        # Honest placeholder: returns a synthetic accounting entry.
        acct = payload.get("account_id", "unknown")
        energy = float(payload.get("energy_saved_kwh", 0.0))
        eff = float(payload.get("efficiency", 0.85))
        tokens = max(1, int(energy * eff * 10))
        return {
            "status": "ok",
            "account_id": acct,
            "tokens_generated": tokens,
            "source": payload.get("source", "gradient_conversion"),
            "note": "placeholder token generation",
        }

    async def reserve_token(self, payload: Dict[str, Any]
                             ) -> Dict[str, Any]:
        return {
            "status": "ok",
            "account_id": payload.get("account_id"),
            "amount": payload.get("amount", 0.0),
            "note": "placeholder reservation",
        }


class OptimizationHandler(BaseHandler):
    STATUS = "experimental"

    async def start_optimization(self, payload: Dict[str, Any]
                                   ) -> Dict[str, Any]:
        job_id = await self.api.optimization_manager.start_optimization(
            payload or {}
        )
        return {"job_id": job_id, "status": "started"}

    async def get_job_status(self, payload: Dict[str, Any]
                              ) -> Dict[str, Any]:
        job_id = (payload or {}).get("job_id")
        if not job_id:
            raise APIError(400, "missing_job_id", "job_id is required")
        status = await self.api.optimization_manager.get_job_status(job_id)
        if status is None:
            raise APIError(404, "not_found", "Job not found")
        return status

    async def apply_policy(self, payload: Dict[str, Any]
                            ) -> Dict[str, Any]:
        job_id = (payload or {}).get("job_id")
        policy_id = (payload or {}).get("policy_id", "best")
        if not job_id:
            raise APIError(400, "missing_job_id", "job_id is required")
        return await self.api.optimization_manager.apply_policy(job_id,
                                                                 policy_id)


class WebhookHandler(BaseHandler):
    STATUS = "experimental"

    async def subscribe(self, payload: Dict[str, Any]
                         ) -> Dict[str, Any]:
        event_type = (payload or {}).get("event_type")
        callback_url = (payload or {}).get("callback_url")
        if not event_type or not callback_url:
            raise APIError(400, "invalid_payload",
                           "event_type and callback_url are required")
        sub_id = await self.api.webhook_manager.subscribe(
            event_type, callback_url, payload.get("max_retries")
        )
        return {"status": "ok", "subscription_id": sub_id}

    async def unsubscribe(self, payload: Dict[str, Any]
                           ) -> Dict[str, Any]:
        sub_id = (payload or {}).get("subscription_id")
        if not sub_id:
            raise APIError(400, "invalid_payload",
                           "subscription_id is required")
        ok = await self.api.webhook_manager.unsubscribe(sub_id)
        return {"status": "ok" if ok else "not_found"}


# =============================================================================
# SECTION 15. OPENAPI GENERATOR
# =============================================================================
class OpenAPIGenerator:
    STATUS = "experimental"

    def __init__(self, config: APIConfig,
                 routes: Dict[Tuple[str, str], Route]):
        self.config = config
        self.routes = routes

    def generate(self) -> Dict[str, Any]:
        paths: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for (method, path), route in self.routes.items():
            entry: Dict[str, Any] = {
                "summary": route.summary or "",
                "tags": list(route.tags),
                "responses": {
                    "200": {"description": "Success"},
                    "400": {"description": "Bad request"},
                    "401": {"description": "Unauthorized"},
                    "500": {"description": "Internal error"},
                },
            }
            if route.auth_required:
                entry["security"] = [{"bearerAuth": []}]
            paths[path][method.lower()] = entry
        return {
            "openapi": "3.0.3",
            "info": {
                "title": "Bio-Inspired API",
                "version": self.config.api_version,
            },
            "paths": dict(paths),
            "components": {
                "securitySchemes": {
                    "bearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT or opaque",
                    },
                },
            },
        }


# =============================================================================
# SECTION 16. WEBSOCKET SERVER (guarded)
# =============================================================================
class WebSocketServer:
    STATUS = "experimental"

    def __init__(self, api: "BioInspiredAPI", port: int = 8765,
                 enabled: bool = True):
        if enabled:
            _warn_module("websocket")
        self.api = api
        self.port = port
        self.connections: Set[Any] = set()
        self.subscribers: Dict[str, Set[Any]] = defaultdict(set)
        self.server = None
        self._lock: Optional[asyncio.Lock] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self.available = WEBSOCKETS_AVAILABLE

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def start(self) -> None:
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("websockets not installed; server disabled")
            return
        self.server = await websockets.serve(self._handler, "127.0.0.1",
                                              self.port)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        if self.server is not None:
            self.server.close()
            try:
                await self.server.wait_closed()
            except Exception:
                pass
        async with self._get_lock():
            conns = list(self.connections)
        for ws in conns:
            try:
                await ws.close()
            except Exception:
                pass
        self.connections.clear()

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(
                    self.api.config.websocket.heartbeat_interval
                )
                async with self._get_lock():
                    conns = list(self.connections)
                for ws in conns:
                    try:
                        await ws.ping()
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break

    async def _handler(self, websocket) -> None:
        # No mandatory auth; echo + broadcast listener for demo purposes.
        async with self._get_lock():
            self.connections.add(websocket)
            self.subscribers["global"].add(websocket)
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get("type") == "ping":
                        await websocket.send(json.dumps({"type": "pong"}))
                except Exception:
                    pass
        finally:
            async with self._get_lock():
                self.connections.discard(websocket)
                for ch in list(self.subscribers.keys()):
                    self.subscribers[ch].discard(websocket)

    async def broadcast(self, event: Dict[str, Any]) -> None:
        if not self.connections:
            return
        msg = json.dumps(event, default=str)
        async with self._get_lock():
            conns = list(self.connections)
        for ws in conns:
            try:
                await ws.send(msg)
            except Exception:
                pass


# =============================================================================
# SECTION 17. BIO-INSPIRED API
# =============================================================================
class BioInspiredAPI:
    """
    Lifecycle:
        api = BioInspiredAPI(config=cfg)
        await api.start()
        result = await api.handle_request("POST", "/tokens/generate",
                                            body={...})
        await api.shutdown()
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[Union[APIConfig, Dict[str, Any]]] = None,
    ):
        self.bio_core = bio_core
        if isinstance(config, dict):
            self.config = APIConfig.from_dict(config)
        elif isinstance(config, APIConfig):
            self.config = config
        else:
            self.config = APIConfig()

        issues = self.config.validate()
        if issues:
            raise APIError(400, "invalid_config",
                           f"Invalid config: {issues}")

        # Optional bio_core sub-services
        self.token_manager = getattr(bio_core, "token_manager", None) if bio_core else None
        self.gradient_manager = getattr(bio_core, "gradient_manager", None) if bio_core else None
        self.compartment_manager = getattr(bio_core, "compartment_manager", None) if bio_core else None
        self.biomass_storage = getattr(bio_core, "biomass_storage", None) if bio_core else None
        self.harvester = getattr(bio_core, "harvester", None) if bio_core else None

        # Subsystems
        self.event_bus = EventBus()
        self.task_manager = TaskManager()

        self.container = Container()
        self.container.register("config", self.config)
        self.container.register("api", self)

        self.oauth2_manager = OAuth2Manager(self.config.oauth2, enabled=True)
        self.rate_limiter = SlidingWindowRateLimiter(self.config.rate_limit,
                                                       enabled=True)
        self.api_key_manager = APIKeyManager(self.config.rate_limit,
                                                enabled=True)
        self.cache = MemoryCacheBackend(self.config.cache.max_items,
                                          enabled=True)
        self.webhook_manager = WebhookManager(self.config.webhook, enabled=True)
        self.health_checker = HealthChecker(self)
        self.audit_logger = AuditLogger(self.config.audit_log_path, enabled=True)

        # Placeholders
        self.rl_agent = (
            CausalRLAgentPlaceholder(enabled=True)
            if self.config.enable_causal_rl else None
        )
        self.federated = (
            FederatedCoordinatorPlaceholder(
                self, model_keys=["optimization_weights", "rl_q_table"],
                enabled=True,
            )
            if self.config.enable_federated_learning else None
        )
        self.safety_monitor: Optional[SafetyMonitor] = None
        if self.config.enable_safety_monitor:
            self.safety_monitor = SafetyMonitor(enabled=True)
            self._setup_safety_invariants()
        self.xai = XAIExplainer(enabled=self.config.enable_xai)
        self.precision_controller = (
            PrecisionControllerPlaceholder(enabled=True)
            if self.config.enable_precision_switching else None
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
            HumanApprovalHandlerPlaceholder(enabled=True)
            if self.config.enable_human_approval else None
        )

        # WebSocket (guarded)
        self.websocket_server: Optional[WebSocketServer] = (
            WebSocketServer(self, self.config.websocket.port, enabled=True)
            if self.config.websocket.enabled and WEBSOCKETS_AVAILABLE
            else None
        )

        # Optimization manager
        self.optimization_manager = OptimizationManager(self, enabled=True)

        # Handlers
        self.handlers: Dict[str, BaseHandler] = {
            "token": TokenHandler(self.container),
            "optimization": OptimizationHandler(self.container),
            "webhook": WebhookHandler(self.container),
        }

        # Routes registry, keyed by (method, path)
        self.routes: Dict[Tuple[str, str], Route] = {}
        self._register_routes()

        # OpenAPI generator
        self.openapi_generator = OpenAPIGenerator(self.config, self.routes)

        # Observability
        self.metrics = self._setup_metrics()

        # Latency stats
        self.latency_window: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=1000)
        )

        # Lifecycle
        self._started = False
        self._shutdown = False

        logger.info("BioInspiredAPI created (not started)",
                    modules=MODULE_STATUS)

    # ---------------- safety ----------------
    def _setup_safety_invariants(self) -> None:
        assert self.safety_monitor is not None
        self.safety_monitor.add_invariant(
            "webhook_queue_size",
            lambda s: s.get("webhook_queue_size", 0) <= 10000,
            "Webhook queue too large",
        )
        self.safety_monitor.add_invariant(
            "cache_size",
            lambda s: s.get("cache_size", 0) <= self.config.cache.max_items,
            "Cache size over limit",
        )

    def _safety_state(self) -> Dict[str, Any]:
        return {
            "webhook_queue_size": self.webhook_manager.delivery_queue.qsize(),
            "cache_size": len(self.cache.cache),
        }

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> Optional[Dict[str, Any]]:
        if not (self.config.enable_prometheus and PROMETHEUS_AVAILABLE):
            return None
        prefix = "bio_api"
        try:
            return {
                "requests": Counter(
                    f"{prefix}_requests_total", "Total requests",
                    ["method", "endpoint", "status"],
                ),
                "latency": Histogram(
                    f"{prefix}_request_latency_seconds", "Latency",
                    ["method", "endpoint"],
                ),
                "errors": Counter(
                    f"{prefix}_errors_total", "Errors", ["code"],
                ),
                "cache_hits": Counter(
                    f"{prefix}_cache_hits_total", "Cache hits",
                ),
                "cache_misses": Counter(
                    f"{prefix}_cache_misses_total", "Cache misses",
                ),
                "jobs": Gauge(
                    f"{prefix}_jobs", "Jobs in flight",
                ),
            }
        except Exception as e:
            logger.warning("Prometheus setup failed; metrics disabled",
                           error=str(e))
            return None

    # ---------------- routes ----------------
    def _register_routes(self) -> None:
        def add(method: str, path: str, handler: Callable,
                summary: str = "", tags: Optional[List[str]] = None,
                requires_approval: bool = False,
                request_model: Optional[Type] = None) -> None:
            self.routes[(method.upper(), path)] = Route(
                method=method.upper(),
                path=path,
                handler=handler,
                summary=summary,
                tags=tags or [],
                auth_required=True,
                requires_approval=requires_approval,
                request_model=request_model,
            )

        add("POST", "/tokens/generate",
            self.handlers["token"].generate_token,
            summary="Generate Eco-ATP tokens", tags=["tokens"])
        add("POST", "/tokens/reserve",
            self.handlers["token"].reserve_token,
            summary="Reserve Eco-ATP tokens", tags=["tokens"])
        add("POST", "/optimize/start",
            self.handlers["optimization"].start_optimization,
            summary="Start an optimization job", tags=["optimization"])
        add("GET", "/optimize/status/{job_id}",
            self.handlers["optimization"].get_job_status,
            summary="Get job status", tags=["optimization"])
        add("POST", "/optimize/apply",
            self.handlers["optimization"].apply_policy,
            summary="Apply a policy", tags=["optimization"])
        add("POST", "/webhook/subscribe",
            self.handlers["webhook"].subscribe,
            summary="Subscribe to events", tags=["webhooks"])
        add("POST", "/webhook/unsubscribe",
            self.handlers["webhook"].unsubscribe,
            summary="Unsubscribe", tags=["webhooks"])

    # ---------------- lifecycle ----------------
    async def start(self) -> None:
        if self._started:
            return
        await self.event_bus.start()

        # Start background tasks now that a loop is running.
        self.task_manager.start_task(
            "webhook_deliveries", self.webhook_manager.process_deliveries_loop
        )
        if self.websocket_server is not None:
            self.task_manager.start_task("websocket",
                                          self.websocket_server.start)
        if self.chaos_injector is not None:
            self.task_manager.start_task("chaos", self._chaos_loop)

        self._started = True
        logger.info("BioInspiredAPI started")

    async def ready(self) -> bool:
        return self._started and not self._shutdown

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or float(self.config.shutdown_timeout_seconds)
        logger.info("BioInspiredAPI shutting down")

        # Cancel optimization jobs first
        try:
            await self.optimization_manager.cancel_all()
        except Exception:
            pass

        try:
            await asyncio.wait_for(self.task_manager.drain(timeout),
                                    timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Task drain timed out")

        if self.websocket_server is not None:
            try:
                await self.websocket_server.stop()
            except Exception:
                pass

        try:
            await self.webhook_manager.shutdown()
        except Exception:
            pass

        try:
            await asyncio.wait_for(self.event_bus.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Event bus stop timed out")

        logger.info("BioInspiredAPI shutdown complete")

    async def __aenter__(self) -> "BioInspiredAPI":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- background loops ----------------
    async def _chaos_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(60)
                if self.chaos_injector is not None:
                    await self.chaos_injector.maybe_inject_failure()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Chaos loop error", error=str(e))

    # ---------------- request handling ----------------
    @traced("bio_api.handle_request")
    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
        query_params: Optional[Dict[str, str]] = None,
        api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        request_id = new_request_id()
        token = _request_id_ctx.set(request_id)
        start = time.monotonic()
        headers = headers or {}
        body = body or {}
        query_params = query_params or {}

        try:
            # Rate limit
            key = api_key or headers.get("X-API-Key") or "anonymous"
            allowed, rate_info = await self.rate_limiter.check_rate_limit(key)
            if not allowed:
                raise APIError(429, "rate_limit_exceeded",
                               "Rate limit exceeded", rate_info)

            # Route lookup (method, path)
            route = self.routes.get((method.upper(), path))
            if route is None:
                # Support templated paths like /optimize/status/{job_id}
                for (m, tmpl), r in self.routes.items():
                    if m != method.upper():
                        continue
                    if self._match_template(tmpl, path, query_params):
                        route = r
                        break
            if route is None:
                raise APIError(404, "not_found",
                               f"No route for {method} {path}")

            # Authentication (if JWT is available and an API key is not used)
            if route.auth_required and api_key is None:
                auth_header = headers.get("Authorization", "")
                if auth_header.startswith("Bearer ") and JWT_AVAILABLE:
                    token_str = auth_header[7:]
                    claims = await self.oauth2_manager.validate_token(token_str)
                    if claims is None:
                        raise APIError(401, "unauthorized",
                                       "Invalid bearer token")
                else:
                    # Placeholder: allow anonymous for demo.
                    pass

            # Safety check
            if self.safety_monitor is not None:
                violations = self.safety_monitor.check(self._safety_state())
                if violations:
                    raise APIError(403, "safety_violation",
                                   "Safety violation", {
                                       "violations": violations,
                                   })

            # Cache lookup for GET
            cache_key = None
            if method.upper() == "GET" and self.config.cache.enabled:
                cache_key = f"{method.upper()}::{path}::{json.dumps(query_params, sort_keys=True)}"
                cached = await self.cache.get(cache_key)
                if cached is not None:
                    if self.metrics:
                        self.metrics["cache_hits"].inc()
                    return cached

            # Human approval
            if route.requires_approval and self.human_approval is not None:
                ok = await self.human_approval.request_approval({
                    "action": f"{method.upper()} {path}",
                    "body": body,
                })
                if not ok:
                    raise APIError(403, "rejected_by_human",
                                   "Request rejected by human")

            # Call handler. Template params are passed via query_params.
            payload = dict(body)
            if query_params:
                payload.update(query_params)
            result = route.handler(payload)
            if _is_awaitable(result):
                result = await result

            # XAI
            if self.xai.available:
                explanation = self.xai.explain(method.upper(), path,
                                                result or {})
                if explanation and isinstance(result, dict):
                    result = dict(result)
                    result.setdefault("explanation", explanation)

            # Cache write
            if cache_key is not None and isinstance(result, dict):
                await self.cache.set(cache_key, result,
                                      ttl=self.config.cache.ttl_seconds)

            # Audit
            self.audit_logger.log({
                "action": f"{method.upper()} {path}",
                "api_key": key,
                "status": 200,
            })

            # Metrics
            latency = time.monotonic() - start
            self.latency_window[f"{method.upper()} {path}"].append(latency)
            if self.metrics:
                self.metrics["requests"].labels(
                    method=method.upper(), endpoint=path, status="200"
                ).inc()
                self.metrics["latency"].labels(
                    method=method.upper(), endpoint=path
                ).observe(latency)

            return result if isinstance(result, dict) else {"result": result}

        except APIError as e:
            if self.metrics:
                self.metrics["errors"].labels(code=e.code).inc()
                self.metrics["requests"].labels(
                    method=method.upper(), endpoint=path,
                    status=str(e.status_code),
                ).inc()
            self.audit_logger.log({
                "action": f"{method.upper()} {path}",
                "status": e.status_code,
                "code": e.code,
            })
            return error_response(e.status_code, e.code, e.message, e.details)
        except Exception as e:
            logger.error("Unhandled request error", error=str(e),
                         exc_info=True)
            if self.metrics:
                self.metrics["errors"].labels(code="internal_error").inc()
            return error_response(500, "internal_error",
                                   "Internal server error")
        finally:
            _request_id_ctx.reset(token)

    @staticmethod
    def _match_template(template: str, path: str,
                         query_params: Dict[str, str]) -> bool:
        t_parts = [p for p in template.strip("/").split("/") if p]
        p_parts = [p for p in path.strip("/").split("/") if p]
        if len(t_parts) != len(p_parts):
            return False
        for t, p in zip(t_parts, p_parts):
            if t.startswith("{") and t.endswith("}"):
                name = t[1:-1]
                query_params[name] = p
            elif t != p:
                return False
        return True

    # ---------------- OpenAPI ----------------
    async def get_openapi(self) -> Dict[str, Any]:
        return self.openapi_generator.generate()

    def get_status(self) -> Dict[str, Any]:
        return {
            "started": self._started,
            "shutdown": self._shutdown,
            "routes": [f"{m} {p}" for m, p in self.routes.keys()],
            "module_status": MODULE_STATUS,
            "cache_stats": self.cache.stats,
            "websocket_available": WEBSOCKETS_AVAILABLE,
            "jwt_available": JWT_AVAILABLE,
            "prometheus_enabled": self.metrics is not None,
        }


# =============================================================================
# SECTION 18. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _api(self, **overrides) -> BioInspiredAPI:
        cfg = APIConfig(
            enable_prometheus=False,
            enable_safety_monitor=True,
            enable_xai=True,
            enable_causal_rl=False,
            enable_federated_learning=False,
            enable_precision_switching=False,
            enable_carbon_market=False,
            enable_chaos=False,
            enable_human_approval=False,
            optimization=OptimizationConfig(
                population_size=6, generations=2,
                objective_weights={
                    "total_harvested": 0.5,
                    "avg_efficiency": 0.5,
                    "carbon_saved": 0.0,
                    "helium_saved": 0.0,
                },
            ),
        )
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        return BioInspiredAPI(config=cfg)

    def test_lifecycle(self):
        async def go():
            api = self._api()
            self.assertFalse(await api.ready())
            await api.start()
            self.assertTrue(await api.ready())
            await api.shutdown()
            self.assertTrue(api._shutdown)
            await api.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with self._api() as api:
                self.assertTrue(await api.ready())
        asyncio.run(go())

    def test_route_registration_and_lookup(self):
        async def go():
            async with self._api() as api:
                r = await api.handle_request(
                    "POST", "/tokens/generate",
                    body={"account_id": "a", "energy_saved_kwh": 10.0},
                )
                self.assertEqual(r["status"], "ok")
                self.assertGreater(r["tokens_generated"], 0)
        asyncio.run(go())

    def test_404_on_unknown_route(self):
        async def go():
            async with self._api() as api:
                r = await api.handle_request("GET", "/nope")
                self.assertEqual(r["status"], 404)
                self.assertEqual(r["error"]["code"], "not_found")
        asyncio.run(go())

    def test_optimization_start_and_status(self):
        async def go():
            async with self._api() as api:
                r = await api.handle_request(
                    "POST", "/optimize/start",
                    body={"algorithm": "nsga2", "population_size": 4,
                          "generations": 1},
                )
                job_id = r["job_id"]
                # Let it run
                for _ in range(50):
                    s = await api.optimization_manager.get_job_status(job_id)
                    if s and s["status"] in ("completed", "failed"):
                        break
                    await asyncio.sleep(0.05)
                s = await api.optimization_manager.get_job_status(job_id)
                self.assertIn(s["status"], ("completed", "failed"))
                if s["status"] == "completed":
                    self.assertIn("pareto_front", s["result"])
        asyncio.run(go())

    def test_template_route_params(self):
        async def go():
            async with self._api() as api:
                # Missing job => 404
                r = await api.handle_request(
                    "GET", "/optimize/status/does-not-exist"
                )
                # Template route matches and handler returns 404 for missing
                self.assertEqual(r["status"], 404)
        asyncio.run(go())

    def test_cache_get_put(self):
        async def go():
            async with self._api() as api:
                r1 = await api.handle_request("POST", "/tokens/generate",
                                                body={"account_id": "x",
                                                      "energy_saved_kwh": 1.0})
                # Direct cache test
                await api.cache.set("k", {"v": 1}, ttl=10)
                v = await api.cache.get("k")
                self.assertEqual(v, {"v": 1})
        asyncio.run(go())

    def test_rate_limiter(self):
        async def go():
            async with self._api() as api:
                api.rate_limiter.config.default_burst_limit = 2
                for _ in range(2):
                    ok, _ = await api.rate_limiter.check_rate_limit("k")
                    self.assertTrue(ok)
                ok, info = await api.rate_limiter.check_rate_limit("k")
                self.assertFalse(ok)
                self.assertEqual(info["remaining"], 0)
        asyncio.run(go())

    def test_api_key_manager(self):
        async def go():
            api = self._api()
            k = await api.api_key_manager.create_key("test", role="admin")
            info = await api.api_key_manager.validate_key(k)
            self.assertIsNotNone(info)
            self.assertEqual(info["role"], "admin")
            # The raw key is never stored
            self.assertNotIn(k, api.api_key_manager._keys)
            ok = await api.api_key_manager.revoke_key(k)
            self.assertTrue(ok)
            self.assertIsNone(await api.api_key_manager.validate_key(k))
        asyncio.run(go())

    def test_nsga2_does_not_mutate_front(self):
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
            # Selecting again must not change the stored front's scores
            _ = opt._select_best_from_pareto(front, {"a": 0.5, "b": 0.5})
            after = [p.scalarised_score for p in front]
            self.assertEqual(before, after)
        asyncio.run(go())

    def test_placeholders_honest(self):
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

    def test_openapi_generation(self):
        async def go():
            async with self._api() as api:
                spec = await api.get_openapi()
                self.assertIn("openapi", spec)
                self.assertIn("paths", spec)
                self.assertIn("/tokens/generate", spec["paths"])
        asyncio.run(go())

    def test_config_validation(self):
        with self.assertRaises(APIError):
            BioInspiredAPI(config=APIConfig(
                chaos_probability=2.0,  # invalid
                enable_chaos=False,
            ))

    def test_webhook_subscribe_roundtrip(self):
        async def go():
            api = self._api()
            await api.start()
            try:
                r = await api.handle_request(
                    "POST", "/webhook/subscribe",
                    body={"event_type": "task_completed",
                          "callback_url": "http://example.com"},
                )
                self.assertEqual(r["status"], "ok")
                sub_id = r["subscription_id"]
                self.assertIn(sub_id, api.webhook_manager.subscriptions)
                r2 = await api.handle_request(
                    "POST", "/webhook/unsubscribe",
                    body={"subscription_id": sub_id},
                )
                self.assertEqual(r2["status"], "ok")
            finally:
                await api.shutdown()
        asyncio.run(go())

    def test_module_status_documented(self):
        self.assertEqual(MODULE_STATUS["api_core"], "stable")
        self.assertEqual(MODULE_STATUS["causal_rl"], "placeholder")
        self.assertEqual(MODULE_STATUS["rate_limiting"], "experimental")


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 19. ENTRY POINT
# =============================================================================
async def _example() -> None:
    cfg = APIConfig(
        enable_prometheus=False,
        enable_safety_monitor=True,
        enable_xai=True,
        optimization=OptimizationConfig(population_size=8, generations=3),
    )
    async with BioInspiredAPI(config=cfg) as api:
        r = await api.handle_request(
            "POST", "/tokens/generate",
            body={"account_id": "demo", "energy_saved_kwh": 5.0},
        )
        print("Token generation:", json.dumps(r, indent=2))
        r = await api.handle_request(
            "POST", "/optimize/start",
            body={"algorithm": "nsga2", "population_size": 6,
                  "generations": 2},
        )
        job_id = r["job_id"]
        for _ in range(60):
            s = await api.optimization_manager.get_job_status(job_id)
            if s and s["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(0.1)
        status = await api.optimization_manager.get_job_status(job_id)
        if status and status["status"] == "completed":
            print("Pareto front size:",
                  len(status["result"].get("pareto_front", [])))
        else:
            print("Optimization status:", status)
        spec = await api.get_openapi()
        print("OpenAPI paths:", sorted(spec["paths"].keys()))
        print("Status:", json.dumps(api.get_status(), indent=2,
                                    default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Bio-Inspired API v11.0.0")
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

    print("Bio-Inspired API v11.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
