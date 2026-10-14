#!/usr/bin/env python3
"""
Enhanced Adaptive API Service (v2.0.0)
=======================================

FastAPI service for expert feedback, distillation, and multi-objective
learning.  This file is self-contained and can be started directly:

    uvicorn adaptive_api_service:app --host 0.0.0.0 --port 8000

Run the built-in test suite:

    python adaptive_api_service.py --test

CHANGES (v2.0.0 over the previous release):
- Pydantic v2-first imports (``pydantic_settings.BaseSettings``,
  ``field_validator``); v1 fallback retained.
- Fixed the env-prefix / env-name double-prefix bug; a single
  ``env_prefix`` is used for every field.
- ``TemporalSafetyMonitor`` and ``FeedbackEvent`` are imported with
  fallback stubs; ``FeedbackEvent`` is emitted from ``/feedback``.
- ``CircuitBreaker.call`` now awaits coroutines returned by *sync*
  callables (previously the bandit update coroutine was silently dropped).
- All profiler consumers are guarded against ``profiler is None``.
- Approval callbacks may be sync or async.
- ``DistillationFlexGenSelector`` and ``FlexGenController`` are created
  once at startup and shared across requests.
- SQL is dialect-agnostic (``INSERT ... ON CONFLICT`` via SQLAlchemy).
- ``feedback_records`` has indexes on ``request_id``, ``expert_id``,
  ``node_id``, and ``created_at``.
- Rate limiter uses ``time.monotonic``, evicts stale keys, and falls back
  to the in-memory backend when Redis is unavailable.
- ``/feedback/history`` supports pagination (``limit``, ``offset``) with
  hard caps.
- ``verify_jwt`` validates ``audience`` and ``issuer`` when configured.
- Unified error envelope: ``{"error": {"code", "message", "request_id"}}``.
- ``X-Forwarded-For`` is honoured behind a configurable trusted-proxy list.
- ``/metrics`` endpoint exposes Prometheus text format.
- Retraining actually trains the bandit when enough data is available.
- Every endpoint body is a Pydantic model.
- Chaos testing supports ``reset`` to clear injected faults.
- ``workload.metadata`` / ``node.metadata`` accesses are guarded.
- ``__version__``, ``__all__``, and an in-file test suite.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import numpy as np

# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
from fastapi import (
    BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request,
    Response, status,
)
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# ---------------------------------------------------------------------------
# Pydantic (v2 first, v1 fallback)
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore
    PYDANTIC_V2 = True
except ImportError:  # pragma: no cover
    from pydantic import BaseModel, Field, BaseSettings  # type: ignore
    def field_validator(*a, **k):  # type: ignore
        def deco(fn): return fn
        return deco
    class SettingsConfigDict(dict):  # type: ignore
        pass
    PYDANTIC_V2 = False

# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------
try:
    import jwt
    from jwt import PyJWTError
    JWT_AVAILABLE = True
except ImportError:  # pragma: no cover
    JWT_AVAILABLE = False

    class PyJWTError(Exception):
        pass

# ---------------------------------------------------------------------------
# SQLAlchemy
# ---------------------------------------------------------------------------
from sqlalchemy import (
    JSON, Column, DateTime, Float, Index, Integer, String, Text, text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import select

# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------
try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST, Counter, Gauge, Histogram,
        generate_latest, start_http_server,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False
    CONTENT_TYPE_LATEST = "text/plain"

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
try:
    import structlog  # type: ignore
    from structlog.processors import JSONRenderer, TimeStamper  # type: ignore
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
    STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis (optional)
# ---------------------------------------------------------------------------
try:
    import redis.asyncio as redis  # type: ignore
    REDIS_AVAILABLE = True
except ImportError:  # pragma: no cover
    redis = None  # type: ignore
    REDIS_AVAILABLE = False


__version__ = "2.0.0"
__all__ = [
    "__version__",
    "Settings",
    "get_settings",
    "CircuitBreaker",
    "RateLimiter",
    "TaskManager",
    "FeedbackRequest",
    "FeedbackResponse",
    "app",
    "main",
    "run_tests",
]


# =============================================================================
# Optional modules (with graceful fallbacks)
# =============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    from enhancements.gpu_profiler import GPUProfiler  # type: ignore
    from enhancements.metric_aggregator import MetricAggregator  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.reward_calculator import RewardCalculator  # type: ignore
    from enhancements.temporal_safety_monitor import TemporalSafetyMonitor  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:
        def __init__(self, *a, **k) -> None: pass
        def generate_policies(self, current_policies, n=2):
            return []

    class ExpertRouter:
        def __init__(self, *a, **k) -> None: pass
        def encode(self, context) -> List[float]:
            return [0.0] * 5

    class ParetoOptimizer:
        def __init__(self, *a, **k) -> None: pass
        def evaluate(self, objectives: Dict[str, float],
                     weights: Dict[str, float]) -> float:
            return float(sum(objectives.get(k, 0.0) * weights.get(k, 1.0)
                             for k in objectives))

    class ContextualBandit:
        def __init__(self, action_space, fallback_solver) -> None:
            self.actions = list(action_space)
            self.fallback = fallback_solver
            self.state: Dict[str, Any] = {}
            self._values: Dict[Tuple[Any, str], float] = {}
            self._counts: Dict[Tuple[Any, str], int] = {}
        def select_action(self, context) -> Tuple[str, float, str]:
            return self.actions[0], 0.5, "fallback"
        def peek_confidence(self, context) -> float:
            return 0.5
        def update(self, context, action, reward) -> None:
            pass
        def train_from_batch(self, rows) -> int:
            return 0

    class GPUProfiler:
        def __init__(self) -> None: pass
        def start(self) -> None: pass
        def stop(self) -> None: pass
        def get_current_metrics(self) -> Dict[str, Any]: return {}
        async def get_all_gpu_metrics(self) -> List[Dict[str, Any]]: return []
        def get_gpu_metrics(self) -> Dict[str, Any]: return {}

    class MetricAggregator:
        def __init__(self, profiler, executor) -> None: pass
        def run(self, task, policy): return {}
        def get_current_metrics(self): return {}

    class RewardCalculator:
        def __init__(self) -> None: pass
        def compute(self, metrics, constraints, carbon_intensity) -> float:
            return 0.5

    class TemporalSafetyMonitor:
        def __init__(self) -> None:
            self.rules: List[Any] = []
        def add_rule(self, *a, **k) -> None: pass
        async def check(self, state) -> List[str]:
            return []

try:
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController  # type: ignore
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel  # type: ignore
    from enhancements.gpu_optimization.flexgen_policy import FlexGenPolicy, generate_candidate_policies  # type: ignore
    from enhancements.gpu_optimization.flexgen_policy_selector import DistillationFlexGenSelector  # type: ignore
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector  # type: ignore
    from enhancements.schemas.node_descriptor import NodeDescriptor  # type: ignore
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
    FLEXGEN_AVAILABLE = True
except ImportError:  # pragma: no cover
    FLEXGEN_AVAILABLE = False

    class FlexGenPolicy: pass
    def generate_candidate_policies(n=20): return []

    class FlexGenController:
        def __init__(self, *a, **k) -> None: pass
        async def step(self) -> Dict[str, Any]: return {}

    class FlexGenCostModel:
        def __init__(self, *a, **k) -> None: pass

    class PolicyDriftDetector:
        def __init__(self, *a, **k) -> None: pass
        def get_stats(self) -> Dict[str, Any]: return {}

    class NodeDescriptor:  # type: ignore
        def __init__(self, **data) -> None:
            self.__dict__.update(data)

    class WorkloadDescriptor:  # type: ignore
        def __init__(self, **data) -> None:
            self.__dict__.update(data)

    class DistillationFlexGenSelector:
        def __init__(self, *a, **k) -> None: pass

# FeedbackEvent schema
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# =============================================================================
# Settings
# =============================================================================
class Settings(BaseSettings):
    """
    Application settings.

    Environment variables are read with the ``ADAPTIVE_API_`` prefix.
    Nested structures (dicts, lists) accept JSON strings.
    """

    # General
    api_version: str = "v1"
    debug: bool = False
    log_level: str = "INFO"

    # JWT
    auth_secret: str = Field(
        default="dev-secret-change-me",
        description="HS256 secret. MUST be overridden in production.",
    )
    auth_algorithm: str = "HS256"
    auth_audience: Optional[str] = None
    auth_issuer: Optional[str] = None
    auth_jwks_url: Optional[str] = None
    token_expiry_minutes: int = 60

    # Database
    db_url: str = "sqlite+aiosqlite:///./adaptive.db"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # Redis
    redis_url: Optional[str] = None

    # Rate limiting
    rate_limit_enabled: bool = True
    rate_limit_per_minute: int = 100
    rate_limit_burst: int = 20
    trusted_proxies: List[str] = Field(default_factory=list)

    # Prometheus
    prometheus_port: Optional[int] = None

    # MODP weights
    modp_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            "energy": 0.25, "carbon": 0.25, "latency": 0.20, "accuracy": 0.30,
        }
    )

    # Bandit action space
    expert_ids: List[str] = Field(
        default_factory=lambda: ["expert_a", "expert_b", "expert_c"]
    )

    # Retraining
    retrain_interval_seconds: int = 3600
    min_feedback_for_retrain: int = 50

    # FlexGen
    flexgen_carbon_intensity_default: float = 400.0
    flexgen_population_size: int = 50
    flexgen_generations: int = 10
    flexgen_use_real_executor: bool = False
    flexgen_selector_epsilon: float = 0.1
    flexgen_selector_epsilon_decay: float = 0.999

    # Human approval
    require_human_approval: bool = False
    high_carbon_threshold: float = 800.0

    # Pagination caps
    history_max_limit: int = 1000

    # ------------------------------------------------------------------
    if PYDANTIC_V2:
        model_config = SettingsConfigDict(
            env_prefix="ADAPTIVE_API_",
            case_sensitive=False,
            extra="ignore",
        )
    else:  # pragma: no cover
        class Config:
            env_prefix = "ADAPTIVE_API_"
            case_sensitive = False

    # ------------------------------------------------------------------
    @field_validator("modp_weights", mode="before")
    @classmethod
    def _parse_modp_weights(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(f"modp_weights must be valid JSON: {e}")
        return v

    @field_validator("expert_ids", "trusted_proxies", mode="before")
    @classmethod
    def _parse_string_list(cls, v):
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("["):
                try:
                    return json.loads(s)
                except json.JSONDecodeError:
                    pass
            return [x.strip() for x in s.split(",") if x.strip()]
        return v

    @field_validator("auth_algorithm")
    @classmethod
    def _check_alg(cls, v: str) -> str:
        allowed = {"HS256", "HS384", "HS512", "RS256", "RS384", "RS512"}
        if v not in allowed:
            raise ValueError(f"auth_algorithm must be one of {sorted(allowed)}")
        return v


_settings_singleton: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings_singleton
    if _settings_singleton is None:
        _settings_singleton = Settings()
    return _settings_singleton


def reset_settings_for_tests() -> None:
    global _settings_singleton
    _settings_singleton = None


# =============================================================================
# Database
# =============================================================================
Base = declarative_base()


class FeedbackRecord(Base):
    __tablename__ = "feedback_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(String(64), nullable=False, index=True)
    expert_id = Column(String(128), nullable=False, index=True)
    node_id = Column(String(128), nullable=False, index=True)
    predicted_cost = Column(Float, nullable=False, default=0.0)
    actual_cost = Column(Float, nullable=False, default=0.0)
    energy_joules = Column(Float, default=0.0)
    carbon_kg = Column(Float, default=0.0)
    helium_units = Column(Float, default=0.0)
    latency_ms = Column(Float, default=0.0)
    accuracy = Column(Float, default=0.0)
    weights_snapshot = Column(JSON, nullable=True)
    teacher_id = Column(String(128), nullable=True)
    distillation_loss = Column(Float, nullable=True)
    modp_utility = Column(Float, nullable=True)
    context_vector = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        index=True)

    __table_args__ = (
        Index("ix_feedback_created_desc", created_at.desc()),
    )


class SystemState(Base):
    __tablename__ = "system_state"
    key = Column(String(64), primary_key=True)
    value = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# =============================================================================
# Circuit breaker
# =============================================================================
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_time: float = 30.0) -> None:
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_time = float(recovery_time)
        self._state = "CLOSED"
        self._failure_count = 0
        self._last_failure_monotonic: Optional[float] = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> str:
        return self._state

    def force_open(self) -> None:
        self._state = "OPEN"
        self._last_failure_monotonic = time.monotonic()
        self._failure_count = self.failure_threshold

    def reset(self) -> None:
        self._state = "CLOSED"
        self._failure_count = 0
        self._last_failure_monotonic = None

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self._state == "OPEN":
                if (self._last_failure_monotonic is not None
                        and time.monotonic() - self._last_failure_monotonic
                        > self.recovery_time):
                    self._state = "HALF_OPEN"
                    self._failure_count = 0
                else:
                    raise RuntimeError(f"CircuitBreaker '{self.name}' is OPEN")

        try:
            result = func(*args, **kwargs) if not inspect.iscoroutinefunction(func) \
                     else await func(*args, **kwargs)
            if inspect.iscoroutine(result):
                result = await result
            async with self._lock:
                if self._state == "HALF_OPEN":
                    self._state = "CLOSED"
                self._failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_monotonic = time.monotonic()
                if self._failure_count >= self.failure_threshold:
                    self._state = "OPEN"
            raise


# =============================================================================
# Rate limiter
# =============================================================================
class RateLimiter:
    """
    Sliding-window rate limiter with an in-memory fallback.

    Uses ``time.monotonic`` for interval math and evicts stale keys lazily.
    """

    def __init__(self, redis_client: Optional[Any] = None,
                 max_keys: int = 100_000) -> None:
        self.redis = redis_client
        self._memory_store: Dict[str, List[float]] = {}
        self._lock = asyncio.Lock()
        self._max_keys = int(max_keys)
        self._last_evict = time.monotonic()

    async def check(self, key: str, limit: int, window: float
                    ) -> Tuple[bool, int, int]:
        if self.redis is not None:
            try:
                return await self._redis_check(key, limit, window)
            except Exception as e:
                logger.warning("Redis rate limiter unavailable; using memory: %s", e)
        async with self._lock:
            return await self._memory_check(key, limit, window)

    async def _redis_check(self, key: str, limit: int, window: float
                           ) -> Tuple[bool, int, int]:
        now = time.time()
        window_start = now - window
        await self.redis.zremrangebyscore(key, 0, window_start)
        count = await self.redis.zcard(key)
        if count >= limit:
            oldest = await self.redis.zrange(key, 0, 0, withscores=True)
            retry = int(oldest[0][1] + window - now) if oldest else 0
            return False, max(1, retry), 0
        await self.redis.zadd(key, {str(now): now})
        await self.redis.expire(key, int(window) + 10)
        remaining = limit - (count + 1)
        return True, 0, max(0, remaining)

    async def _memory_check(self, key: str, limit: int, window: float
                            ) -> Tuple[bool, int, int]:
        now = time.monotonic()
        bucket = self._memory_store.get(key, [])
        cutoff = now - window
        bucket = [t for t in bucket if t >= cutoff]
        if len(bucket) >= limit:
            retry = int(bucket[0] + window - now)
            return False, max(1, retry), 0
        bucket.append(now)
        self._memory_store[key] = bucket
        # Lazy eviction every 60 seconds.
        if now - self._last_evict > 60.0:
            self._evict_stale(cutoff)
            self._last_evict = now
        return True, 0, max(0, limit - len(bucket))

    def _evict_stale(self, cutoff: float) -> None:
        stale = [k for k, v in self._memory_store.items()
                 if not v or v[-1] < cutoff]
        for k in stale:
            self._memory_store.pop(k, None)
        # Hard cap.
        while len(self._memory_store) > self._max_keys:
            self._memory_store.pop(next(iter(self._memory_store)))


# =============================================================================
# Task manager
# =============================================================================
class TaskManager:
    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]],
                   *args: Any, **kwargs: Any) -> asyncio.Task:
        if name in self.tasks and not self.tasks[name].done():
            return self.tasks[name]

        async def wrapper() -> None:
            backoff = 1.0
            max_backoff = 300.0
            while not self._shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e),
                                 exc_info=True)
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(), timeout=backoff)
                        break
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(backoff * 2, max_backoff)

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    async def stop_all(self) -> None:
        self._shutdown_event.set()
        async with self._lock:
            for t in self.tasks.values():
                if not t.done():
                    t.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks.values(),
                                     return_exceptions=True)
            self.tasks.clear()

    def snapshot(self) -> Dict[str, str]:
        return {n: ("running" if not t.done() else "stopped")
                for n, t in self.tasks.items()}


# =============================================================================
# Correlation ID middleware
# =============================================================================
async def correlation_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id
    if STRUCTLOG_AVAILABLE:
        try:
            structlog.contextvars.bind_contextvars(request_id=request_id)
        except Exception:
            pass
    start = time.monotonic()
    try:
        response = await call_next(request)
    finally:
        if REQUEST_LATENCY is not None:
            try:
                REQUEST_LATENCY.labels(
                    method=request.method,
                    path=request.url.path,
                ).observe(time.monotonic() - start)
            except Exception:
                pass
    response.headers["X-Request-ID"] = request_id
    return response


# =============================================================================
# Authentication
# =============================================================================
security = HTTPBearer(auto_error=False)


async def verify_jwt(token: str, settings: Settings) -> Dict[str, Any]:
    if not JWT_AVAILABLE:
        raise HTTPException(status_code=501, detail="JWT support not installed")
    if not token:
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        options = {"require": ["exp"]}
        kwargs: Dict[str, Any] = {
            "algorithms": [settings.auth_algorithm],
            "options": options,
        }
        if settings.auth_audience:
            kwargs["audience"] = settings.auth_audience
        if settings.auth_issuer:
            kwargs["issuer"] = settings.auth_issuer
        payload = jwt.decode(token, settings.auth_secret, **kwargs)
    except PyJWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    roles = payload.get("roles") or payload.get("scope") or []
    if isinstance(roles, str):
        roles = roles.split()
    return {"sub": payload.get("sub"), "roles": list(roles), "claims": payload}


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> Dict[str, Any]:
    settings = get_settings()
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    return await verify_jwt(credentials.credentials, settings)


async def require_admin(user: Dict = Depends(get_current_user)) -> Dict:
    if "admin" not in (user.get("roles") or []):
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


async def require_trainer(user: Dict = Depends(get_current_user)) -> Dict:
    roles = user.get("roles") or []
    if "trainer" not in roles and "admin" not in roles:
        raise HTTPException(status_code=403, detail="Trainer role required")
    return user


# =============================================================================
# Rate-limit dependency
# =============================================================================
async def rate_limit(request: Request):
    settings = get_settings()
    if not settings.rate_limit_enabled:
        return
    client_ip = _client_ip(request, settings.trusted_proxies)
    key = f"rate_limit:{client_ip}"
    allowed, retry_after, remaining = await rate_limiter.check(
        key, settings.rate_limit_per_minute, 60.0,
    )
    request.state.rate_limit_remaining = remaining
    if not allowed:
        if RATE_LIMIT_HITS is not None:
            RATE_LIMIT_HITS.inc()
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(retry_after),
                     "X-RateLimit-Remaining": "0"},
        )


def _client_ip(request: Request, trusted_proxies: List[str]) -> str:
    if request.client and request.client.host in set(trusted_proxies or []):
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# =============================================================================
# Prometheus metrics
# =============================================================================
if PROMETHEUS_AVAILABLE:
    DISTILLATION_LOSS = Gauge(
        "adaptive_distillation_loss", "Most recent distillation loss")
    FEEDBACK_RECORDS = Counter(
        "adaptive_feedback_records_total", "Total feedback records")
    REQUEST_LATENCY = Histogram(
        "adaptive_request_latency_seconds", "Request latency",
        ["method", "path"])
    RATE_LIMIT_HITS = Counter(
        "adaptive_rate_limit_hits_total", "Rate limit hits")
    BANDIT_CONFIDENCE = Gauge(
        "adaptive_bandit_confidence", "Bandit confidence")
    DB_CIRCUIT_STATE = Gauge(
        "adaptive_db_circuit_state", "Database circuit breaker state",
        ["name"])
else:  # pragma: no cover
    DISTILLATION_LOSS = None
    FEEDBACK_RECORDS = None
    REQUEST_LATENCY = None
    RATE_LIMIT_HITS = None
    BANDIT_CONFIDENCE = None
    DB_CIRCUIT_STATE = None


# =============================================================================
# Global state containers
# =============================================================================
@dataclass
class AppState:
    """Bundle of lazily-initialised components."""
    engine: Any = None
    session_factory: Any = None
    profiler: Optional[Any] = None
    metric_aggregator: Optional[Any] = None
    reward_calc: Optional[Any] = None
    modp: Optional[Any] = None
    moe: Optional[Any] = None
    bio: Optional[Any] = None
    bandit: Optional[Any] = None
    safety_monitor: Optional[Any] = None
    flexgen_cost_model: Optional[Any] = None
    policy_drift_detector: Optional[Any] = None
    flexgen_selector: Optional[Any] = None
    flexgen_controller_factory: Optional[Callable[..., Any]] = None
    redis_client: Optional[Any] = None
    rate_limiter: Optional[RateLimiter] = None
    task_manager: Optional[TaskManager] = None
    db_circuit: Optional[CircuitBreaker] = None
    learning_circuit: Optional[CircuitBreaker] = None
    approval_callback: Optional[Callable[[Dict], Union[bool, Awaitable[bool]]]] = None


app_state = AppState()


def _state() -> AppState:
    return app_state


def _require(slot: str) -> Any:
    val = getattr(app_state, slot, None)
    if val is None:
        raise HTTPException(
            status_code=503,
            detail=f"Component '{slot}' is not available",
        )
    return val


# =============================================================================
# JWT helpers for tests / dev
# =============================================================================
def issue_token(sub: str, roles: List[str], settings: Optional[Settings] = None
                ) -> str:
    settings = settings or get_settings()
    if not JWT_AVAILABLE:
        raise RuntimeError("JWT not available")
    now = datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "sub": sub,
        "roles": roles,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=settings.token_expiry_minutes)).timestamp()),
    }
    if settings.auth_audience:
        payload["aud"] = settings.auth_audience
    if settings.auth_issuer:
        payload["iss"] = settings.auth_issuer
    return jwt.encode(payload, settings.auth_secret,
                      algorithm=settings.auth_algorithm)


# =============================================================================
# Pydantic request/response models
# =============================================================================
class FeedbackRequest(BaseModel):
    expert_id: str = Field(..., min_length=1, max_length=128)
    node_id: str = Field(..., min_length=1, max_length=128)
    predicted_cost: float = Field(..., ge=0)
    actual_cost: float = Field(..., ge=0)
    actual_metrics: Dict[str, float] = Field(default_factory=dict)
    teacher_id: Optional[str] = Field(None, max_length=128)
    distillation_loss: Optional[float] = Field(None, ge=0)
    context_features: Optional[Dict[str, Any]] = None


class FeedbackResponse(BaseModel):
    status: str
    request_id: str
    recorded: bool
    modp_utility: Optional[float] = None
    bandit_confidence: Optional[float] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str
    checks: Dict[str, Any]


class BestExpertRequest(BaseModel):
    context: Dict[str, Any] = Field(default_factory=dict)


class BestExpertResponse(BaseModel):
    expert_id: str
    confidence: float
    source: str
    explanation: str = ""


class ParetoRequest(BaseModel):
    objectives: Dict[str, float] = Field(default_factory=dict)


class ParetoResponse(BaseModel):
    objectives: Dict[str, float]
    utility: float


class GeneratePoliciesResponse(BaseModel):
    new_policies: List[Dict[str, Any]]


class FlexGenOptimizeRequest(BaseModel):
    workload: Dict[str, Any]
    node: Dict[str, Any]
    carbon_intensity: Optional[float] = None


class FlexGenOptimizeResponse(BaseModel):
    chosen_policy: Dict[str, Any]
    metrics: Dict[str, Any]
    reward: float
    pareto_count: int
    drift_detected: bool = False
    explanation: str = ""


class FlexGenStatusResponse(BaseModel):
    gpu: List[Dict[str, Any]]
    drift: Dict[str, Any]


class ChaosInjectRequest(BaseModel):
    fault_type: str = Field(..., pattern="^(database_failure|profiler_failure|high_carbon|reset_bandit|reset)$")


class ChaosReportResponse(BaseModel):
    circuit_breakers: Dict[str, str]
    profiler_available: bool
    rate_limiter: str
    bandit_state: Dict[str, Any]
    task_status: Dict[str, str]


class HistoryResponse(BaseModel):
    records: List[Dict[str, Any]]
    limit: int
    offset: int
    total: Optional[int] = None


# =============================================================================
# Lifespan / app factory
# =============================================================================
def _build_engine(settings: Settings):
    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {
        "echo": settings.debug,
        "pool_pre_ping": True,
    }
    # SQLite ignores pool_size / max_overflow; guard them.
    if "sqlite" not in settings.db_url.lower():
        engine_kwargs["pool_size"] = settings.db_pool_size
        engine_kwargs["max_overflow"] = settings.db_max_overflow
    return create_async_engine(settings.db_url, **engine_kwargs)


async def _persist_system_state(session: AsyncSession, key: str, value: str
                                ) -> None:
    """Dialect-agnostic upsert for ``system_state``."""
    dialect = session.bind.dialect.name if session.bind is not None else "sqlite"
    if dialect == "postgresql":
        stmt = pg_insert(SystemState).values(key=key, value=value,
                                             updated_at=datetime.now(timezone.utc))
        stmt = stmt.on_conflict_do_update(
            index_elements=[SystemState.key],
            set_={"value": value, "updated_at": datetime.now(timezone.utc)},
        )
    elif dialect == "sqlite":
        stmt = sqlite_insert(SystemState).values(
            key=key, value=value, updated_at=datetime.now(timezone.utc))
        stmt = stmt.on_conflict_do_update(
            index_elements=[SystemState.key],
            set_={"value": value, "updated_at": datetime.now(timezone.utc)},
        )
    else:
        # Best-effort merge for other dialects.
        await session.merge(SystemState(
            key=key, value=value, updated_at=datetime.now(timezone.utc)))
        return
    await session.execute(stmt)


async def load_learning_state(session: AsyncSession) -> None:
    try:
        result = await session.execute(
            select(SystemState).where(SystemState.key == "bandit_weights"))
        row = result.scalar_one_or_none()
        if row is not None:
            try:
                data = json.loads(row.value)
                if app_state.bandit is not None and hasattr(app_state.bandit, "state"):
                    app_state.bandit.state = data
            except Exception as e:
                logger.warning("Could not restore bandit state: %s", e)
    except Exception as e:
        logger.warning("load_learning_state failed: %s", e)


async def save_learning_state(session: AsyncSession) -> None:
    try:
        bandit_state = getattr(app_state.bandit, "state", {}) if app_state.bandit else {}
        await _persist_system_state(
            session, "bandit_weights", json.dumps(bandit_state, default=_json_default))
        await _persist_system_state(
            session, "modp_weights",
            json.dumps(get_settings().modp_weights, default=_json_default))
        await session.commit()
    except Exception as e:
        logger.warning("save_learning_state failed: %s", e)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    try:
        import torch  # type: ignore
        if isinstance(obj, torch.Tensor):
            return obj.detach().cpu().tolist()
    except Exception:
        pass
    return str(obj)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    # 1. Engine + tables
    app_state.engine = _build_engine(settings)
    app_state.session_factory = async_sessionmaker(
        app_state.engine, expire_on_commit=False)
    async with app_state.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 2. Components
    app_state.profiler = GPUProfiler() if ENHANCEMENTS_AVAILABLE else None
    app_state.metric_aggregator = MetricAggregator(
        app_state.profiler, executor_fn=lambda task, policy: {}) \
        if ENHANCEMENTS_AVAILABLE else None
    app_state.reward_calc = RewardCalculator() if ENHANCEMENTS_AVAILABLE else None
    app_state.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else ParetoOptimizer()
    app_state.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else ExpertRouter()
    app_state.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else GeneticPolicyGenerator()
    app_state.safety_monitor = TemporalSafetyMonitor()
    app_state.bandit = ContextualBandit(
        action_space=settings.expert_ids,
        fallback_solver=lambda ctx: settings.expert_ids[0],
    )

    # 3. FlexGen singletons
    if FLEXGEN_AVAILABLE:
        try:
            app_state.flexgen_cost_model = FlexGenCostModel(
                carbon_intensity_g_per_kwh=settings.flexgen_carbon_intensity_default,
            )
        except Exception as e:
            logger.warning("FlexGenCostModel init failed: %s", e)
        try:
            app_state.policy_drift_detector = PolicyDriftDetector()
        except Exception as e:
            logger.warning("PolicyDriftDetector init failed: %s", e)
        try:
            app_state.flexgen_selector = DistillationFlexGenSelector(
                n_candidates=20,
                config={
                    "epsilon": settings.flexgen_selector_epsilon,
                    "epsilon_decay": settings.flexgen_selector_epsilon_decay,
                },
            )
        except Exception as e:
            logger.warning("DistillationFlexGenSelector init failed: %s", e)

    # 4. Redis + rate limiter
    if settings.redis_url and REDIS_AVAILABLE:
        try:
            app_state.redis_client = redis.from_url(
                settings.redis_url, decode_responses=True)
        except Exception as e:
            logger.warning("Redis init failed: %s", e)
            app_state.redis_client = None
    app_state.rate_limiter = RateLimiter(app_state.redis_client)

    # 5. Circuit breakers + task manager
    app_state.db_circuit = CircuitBreaker("database")
    app_state.learning_circuit = CircuitBreaker("learning")
    app_state.task_manager = TaskManager()

    # 6. Restore learning state
    async with app_state.session_factory() as session:
        await load_learning_state(session)

    # 7. Start profiler + metrics + background retraining
    if app_state.profiler is not None:
        try:
            app_state.profiler.start()
        except Exception as e:
            logger.warning("Profiler start failed: %s", e)
    if PROMETHEUS_AVAILABLE and settings.prometheus_port:
        try:
            start_http_server(settings.prometheus_port)
            logger.info("Prometheus on port %s", settings.prometheus_port)
        except Exception as e:
            logger.warning("Prometheus start failed: %s", e)
    app_state.task_manager.start_task("retraining", retraining_loop)

    try:
        yield
    finally:
        # Shutdown
        await app_state.task_manager.stop_all()
        if app_state.profiler is not None:
            try:
                app_state.profiler.stop()
            except Exception:
                pass
        try:
            async with app_state.session_factory() as session:
                await save_learning_state(session)
        except Exception as e:
            logger.warning("save_learning_state on shutdown failed: %s", e)
        if app_state.engine is not None:
            await app_state.engine.dispose()


app = FastAPI(
    title="Adaptive API",
    version=__version__,
    description="Expert feedback, distillation, and multi-objective learning API.",
    lifespan=lifespan,
    openapi_url=None if os.getenv("ADAPTIVE_API_DEBUG", "false").lower() == "true"
                        else "/openapi.json",
)

app.middleware("http")(correlation_middleware)


def _cors_settings() -> Dict[str, Any]:
    settings = get_settings()
    origins_env = os.getenv("ADAPTIVE_API_CORS_ORIGINS", "")
    if settings.debug and not origins_env:
        origins = ["*"]
        allow_credentials = False
    else:
        origins = [o.strip() for o in origins_env.split(",") if o.strip()]
        allow_credentials = True
    return {"allow_origins": origins, "allow_credentials": allow_credentials,
            "allow_methods": ["*"], "allow_headers": ["*"]}


app.add_middleware(CORSMiddleware, **_cors_settings())


# =============================================================================
# Error handlers (unified envelope)
# =============================================================================
def _error_body(code: Union[int, str], message: str, request_id: Optional[str]
                ) -> Dict[str, Any]:
    return {"error": {"code": code, "message": message,
                      "request_id": request_id}}


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    rid = getattr(request.state, "request_id", None)
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body(exc.status_code, str(exc.detail), rid),
        headers=getattr(exc, "headers", None),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request,
                                       exc: RequestValidationError):
    rid = getattr(request.state, "request_id", None)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=_error_body("validation_error",
                            "; ".join(
                                f"{'.'.join(str(x) for x in e['loc'])}: {e['msg']}"
                                for e in exc.errors()
                            ), rid),
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    rid = getattr(request.state, "request_id", None)
    logger.exception("Unhandled exception", request_id=rid)
    return JSONResponse(
        status_code=500,
        content=_error_body("internal_server_error",
                            "Internal server error", rid),
    )


# =============================================================================
# Health / metrics
# =============================================================================
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(request: Request):
    checks: Dict[str, Any] = {}
    # Database
    try:
        async with app_state.session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = {"status": "ok"}
    except Exception as e:
        checks["database"] = {"status": "failed", "error": str(e)}
    # Rate limiter
    settings = get_settings()
    if settings.rate_limit_enabled:
        if app_state.redis_client is not None:
            try:
                await app_state.redis_client.ping()
                checks["rate_limiter"] = {"status": "ok", "backend": "redis"}
            except Exception:
                checks["rate_limiter"] = {"status": "degraded",
                                          "backend": "memory"}
        else:
            checks["rate_limiter"] = {"status": "ok", "backend": "memory"}
    else:
        checks["rate_limiter"] = {"status": "disabled"}
    # Enhancements
    checks["enhancements"] = {
        "status": "available" if ENHANCEMENTS_AVAILABLE else "stub"}
    checks["flexgen"] = {
        "status": "available" if FLEXGEN_AVAILABLE else "stub"}
    checks["feedback_event"] = {
        "status": "available" if FeedbackEvent is not None else "disabled"}
    # Circuit breakers
    if app_state.db_circuit:
        checks["circuit_breaker_db"] = {"status": app_state.db_circuit.state}
    # Tasks
    if app_state.task_manager:
        checks["tasks"] = app_state.task_manager.snapshot()

    hard_fail = any(v.get("status") == "failed" for v in checks.values())
    return HealthResponse(
        status="degraded" if hard_fail else "healthy",
        version=__version__,
        timestamp=datetime.now(timezone.utc).isoformat(),
        checks=checks,
    )


@app.get("/metrics", tags=["Health"])
async def metrics_endpoint():
    if not PROMETHEUS_AVAILABLE:
        return PlainTextResponse("# prometheus_client not installed\n",
                                 media_type="text/plain")
    return Response(content=generate_latest(),
                    media_type=CONTENT_TYPE_LATEST)


# =============================================================================
# Feedback
# =============================================================================
async def _emit_feedback_event(expert_id: str, request_id: str, reward: float,
                               context_vector: Any) -> None:
    if FeedbackEvent is None:
        return
    try:
        ctx = context_vector.tolist() if hasattr(context_vector, "tolist") \
            else list(context_vector or [])
        ev = FeedbackEvent(
            source="adaptive_api",
            feedback_type="routing",
            context={"task_id": request_id, "teacher_ids": []},
            action={"selected_action": expert_id, "selected_rank": 1,
                    "confidence_score": 0.5},
            performance={"quality_score": float(max(0.0, min(1.0, reward))),
                         "latency_ms": 0.0, "energy_joules": 0.0,
                         "carbon_g": 0.0, "helium_cost": 0.0,
                         "duration_ms": 0.0},
            adaptive_cost_value=float(reward),
            tags=["api", "feedback", expert_id],
            envelope={"trace_id": request_id},
            metadata={"context_vector_len": len(ctx)},
        )
        logger.debug("FeedbackEvent emitted: %s", getattr(ev, "event_id", "?"))
    except Exception as e:
        logger.warning("Failed to emit FeedbackEvent: %s", e)


@app.post("/feedback", response_model=FeedbackResponse, tags=["Feedback"])
async def record_feedback(
    feedback: FeedbackRequest,
    request: Request,
    user: Dict = Depends(get_current_user),
    _: None = Depends(rate_limit),
):
    settings = get_settings()
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))

    # 1. Hardware metrics (guarded).
    hardware_metrics: Dict[str, Any] = {}
    if app_state.profiler is not None:
        try:
            hardware_metrics = app_state.profiler.get_current_metrics() or {}
        except Exception as e:
            logger.warning("Profiler.get_current_metrics failed: %s", e)

    # 2. MoE context encoding.
    context = {
        "expert_id": feedback.expert_id,
        "node_id": feedback.node_id,
        "predicted_cost": feedback.predicted_cost,
        "actual_cost": feedback.actual_cost,
        "metrics": feedback.actual_metrics,
        "hardware": hardware_metrics,
        "user_context": feedback.context_features or {},
    }
    try:
        context_vector = app_state.moe.encode(context)
    except Exception as e:
        logger.warning("MoE encode failed: %s", e)
        context_vector = [0.0] * 5

    # 3. MODP utility.
    objectives = {
        "energy": feedback.actual_metrics.get("energy_joules", 0.0) / 1000.0,
        "carbon": feedback.actual_metrics.get("carbon_kg", 0.0) / 10.0,
        "latency": feedback.actual_metrics.get("latency_ms", 0.0) / 1000.0,
        "accuracy": feedback.actual_metrics.get("accuracy", 0.0),
    }
    try:
        modp_utility = float(app_state.modp.evaluate(objectives,
                                                     settings.modp_weights))
    except Exception as e:
        logger.warning("MODP evaluate failed: %s", e)
        modp_utility = 0.0

    # 4. Bandit update (awaited via the fixed circuit breaker).
    try:
        await app_state.learning_circuit.call(
            _bandit_update_callable, context_vector,
            feedback.expert_id, modp_utility,
        )
    except Exception as e:
        logger.warning("Bandit update failed: %s", e)

    # 5. Persist.
    async def _persist() -> None:
        async with app_state.session_factory() as session:
            record = FeedbackRecord(
                request_id=request_id,
                expert_id=feedback.expert_id,
                node_id=feedback.node_id,
                predicted_cost=feedback.predicted_cost,
                actual_cost=feedback.actual_cost,
                energy_joules=feedback.actual_metrics.get("energy_joules", 0.0),
                carbon_kg=feedback.actual_metrics.get("carbon_kg", 0.0),
                helium_units=feedback.actual_metrics.get("helium_units", 0.0),
                latency_ms=feedback.actual_metrics.get("latency_ms", 0.0),
                accuracy=feedback.actual_metrics.get("accuracy", 0.0),
                weights_snapshot={"extra_metrics": feedback.actual_metrics},
                teacher_id=feedback.teacher_id,
                distillation_loss=feedback.distillation_loss,
                modp_utility=modp_utility,
                context_vector=(context_vector.tolist()
                                if hasattr(context_vector, "tolist")
                                else list(context_vector)),
            )
            session.add(record)
            await session.commit()
        if FEEDBACK_RECORDS is not None:
            FEEDBACK_RECORDS.inc()

    try:
        await app_state.db_circuit.call(_persist)
    except Exception as e:
        logger.exception("Failed to persist feedback", request_id=request_id)
        raise HTTPException(status_code=500, detail="Feedback recording failed")

    if feedback.distillation_loss is not None and DISTILLATION_LOSS is not None:
        try:
            DISTILLATION_LOSS.set(feedback.distillation_loss)
        except Exception:
            pass

    # Peek confidence without mutating the bandit's exploration state.
    confidence = 0.0
    try:
        if hasattr(app_state.bandit, "peek_confidence"):
            confidence = float(app_state.bandit.peek_confidence(context_vector))
        else:
            _, confidence, _ = app_state.bandit.select_action(context_vector)
    except Exception as e:
        logger.warning("Bandit confidence read failed: %s", e)

    await _emit_feedback_event(feedback.expert_id, request_id,
                               modp_utility, context_vector)

    return FeedbackResponse(
        status="ok",
        request_id=request_id,
        recorded=True,
        modp_utility=modp_utility,
        bandit_confidence=confidence,
    )


async def _bandit_update_callable(context_vector: Any, expert_id: str,
                                  reward: float) -> None:
    """Returnable callable for the circuit breaker; may be sync or async."""
    bandit = app_state.bandit
    if bandit is None:
        return
    result = bandit.update(context_vector, expert_id, reward)
    if inspect.iscoroutine(result):
        await result


@app.get("/feedback/history", response_model=HistoryResponse,
         tags=["Feedback"])
async def get_feedback_history(
    request: Request,
    user: Dict = Depends(require_admin),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    settings = get_settings()
    limit = min(limit, settings.history_max_limit)
    async with app_state.session_factory() as session:
        result = await session.execute(
            select(FeedbackRecord)
            .order_by(FeedbackRecord.created_at.desc())
            .limit(limit).offset(offset)
        )
        rows = result.scalars().all()
    records = []
    for r in rows:
        records.append({
            "id": r.id,
            "request_id": r.request_id,
            "expert_id": r.expert_id,
            "node_id": r.node_id,
            "predicted_cost": r.predicted_cost,
            "actual_cost": r.actual_cost,
            "energy_joules": r.energy_joules,
            "carbon_kg": r.carbon_kg,
            "latency_ms": r.latency_ms,
            "accuracy": r.accuracy,
            "modp_utility": r.modp_utility,
            "distillation_loss": r.distillation_loss,
            "teacher_id": r.teacher_id,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        })
    return HistoryResponse(records=records, limit=limit, offset=offset)


# =============================================================================
# Optimization
# =============================================================================
@app.post("/optimization/best-expert", response_model=BestExpertResponse,
         tags=["Optimization"])
async def get_best_expert(
    req: BestExpertRequest,
    request: Request,
    user: Dict = Depends(get_current_user),
    _: None = Depends(rate_limit),
):
    try:
        context_vector = app_state.moe.encode(req.context)
    except Exception as e:
        logger.warning("MoE encode failed: %s", e)
        context_vector = [0.0] * 5
    try:
        expert, confidence, source = app_state.bandit.select_action(context_vector)
    except Exception as e:
        logger.warning("Bandit select failed: %s", e)
        expert = get_settings().expert_ids[0]
        confidence, source = 0.5, "fallback"
    if BANDIT_CONFIDENCE is not None:
        try:
            BANDIT_CONFIDENCE.set(float(confidence))
        except Exception:
            pass
    explanation = (
        f"Selected expert '{expert}' with confidence {confidence:.2f} "
        f"(source: {source})."
    )
    return BestExpertResponse(
        expert_id=expert, confidence=float(confidence),
        source=source, explanation=explanation,
    )


@app.post("/optimization/pareto", response_model=ParetoResponse,
         tags=["Optimization"])
async def evaluate_pareto(
    req: ParetoRequest,
    request: Request,
    user: Dict = Depends(get_current_user),
    _: None = Depends(rate_limit),
):
    settings = get_settings()
    try:
        utility = float(app_state.modp.evaluate(req.objectives,
                                                settings.modp_weights))
    except Exception as e:
        logger.warning("MODP evaluate failed: %s", e)
        utility = 0.0
    return ParetoResponse(objectives=req.objectives, utility=utility)


@app.post("/optimization/generate-policies",
         response_model=GeneratePoliciesResponse, tags=["Optimization"])
async def generate_new_policies(
    request: Request,
    user: Dict = Depends(require_admin),
    _: None = Depends(rate_limit),
):
    settings = get_settings()
    async with app_state.session_factory() as session:
        result = await session.execute(
            select(FeedbackRecord.expert_id,
                   FeedbackRecord.modp_utility)
            .order_by(FeedbackRecord.created_at.desc())
            .limit(200)
        )
        rows = result.all()
    if len(rows) < settings.min_feedback_for_retrain:
        raise HTTPException(status_code=400,
                            detail="Not enough feedback data for evolution")
    expert_utilities: Dict[str, List[float]] = {}
    for expert, utility in rows:
        expert_utilities.setdefault(expert, []).append(utility or 0.0)
    current_policies = list(expert_utilities.keys())
    try:
        new_policies = app_state.bio.generate_policies(current_policies, n=3)
    except Exception as e:
        logger.warning("Bio-inspired generation failed: %s", e)
        new_policies = []
    return GeneratePoliciesResponse(new_policies=new_policies)


@app.post("/optimization/retrain", tags=["Optimization"])
async def trigger_retraining(
    request: Request,
    background_tasks: BackgroundTasks,
    user: Dict = Depends(require_admin),
    _: None = Depends(rate_limit),
):
    background_tasks.add_task(retraining_task)
    return {"status": "retraining triggered",
            "request_id": getattr(request.state, "request_id", None)}


# =============================================================================
# FlexGen
# =============================================================================
def _descriptor_metadata(desc: Any) -> Dict[str, Any]:
    meta = getattr(desc, "metadata", None)
    return meta if isinstance(meta, dict) else {}


@app.post("/flexgen/optimize", response_model=FlexGenOptimizeResponse,
         tags=["FlexGen"])
async def flexgen_optimize(
    req: FlexGenOptimizeRequest,
    request: Request,
    user: Dict = Depends(get_current_user),
    _: None = Depends(rate_limit),
):
    settings = get_settings()
    if not FLEXGEN_AVAILABLE or app_state.flexgen_cost_model is None:
        raise HTTPException(status_code=501,
                            detail="FlexGen modules not available")

    # 1. Build descriptors (guarded).
    try:
        workload = WorkloadDescriptor(**req.workload)
        node = NodeDescriptor(**req.node)
    except Exception as e:
        raise HTTPException(status_code=422,
                            detail=f"Invalid descriptor: {e}")

    # 2. Carbon intensity.
    workload_meta = _descriptor_metadata(workload)
    node_meta = _descriptor_metadata(node)
    carbon_intensity = float(
        req.carbon_intensity
        or workload_meta.get("carbon_intensity",
                             settings.flexgen_carbon_intensity_default)
    )

    # 3. Safety.
    try:
        violations = await app_state.safety_monitor.check({
            "carbon_intensity": carbon_intensity,
            "workload_size": workload_meta.get("size", 0),
            "node_temperature": node_meta.get("temp", 0),
        })
    except Exception as e:
        logger.warning("Safety check failed: %s", e)
        violations = []
    if violations:
        raise HTTPException(status_code=409,
                            detail=f"Safety constraints violated: {violations}")

    # 4. Human approval.
    if settings.require_human_approval and \
            carbon_intensity > settings.high_carbon_threshold and \
            app_state.approval_callback is not None:
        try:
            approved = app_state.approval_callback({
                "action": "flexgen_optimize",
                "carbon_intensity": carbon_intensity,
            })
            if inspect.iscoroutine(approved):
                approved = await approved
        except Exception as e:
            logger.warning("Approval callback raised: %s", e)
            approved = False
        if not approved:
            raise HTTPException(
                status_code=403,
                detail="Human approval required for high carbon operation")

    # 5. Build controller using the shared selector and cost model.
    try:
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=carbon_intensity,
            use_real_executor=settings.flexgen_use_real_executor,
            executor=None,
            cost_model=app_state.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={
                "population_size": settings.flexgen_population_size,
                "generations": settings.flexgen_generations,
            },
            modp_planner=None,
            drift_detector=app_state.policy_drift_detector,
            gpu_profiler=app_state.profiler,
            selector=app_state.flexgen_selector,
        )
    except TypeError:
        # Fallback if the controller doesn't accept 'selector'.
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=carbon_intensity,
            use_real_executor=settings.flexgen_use_real_executor,
            executor=None,
            cost_model=app_state.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={
                "population_size": settings.flexgen_population_size,
                "generations": settings.flexgen_generations,
            },
            modp_planner=None,
            drift_detector=app_state.policy_drift_detector,
            gpu_profiler=app_state.profiler,
        )

    try:
        result = await controller.step()
    except Exception as e:
        logger.exception("FlexGen controller step failed: %s", e)
        raise HTTPException(status_code=500,
                            detail=f"FlexGen optimization failed: {e}")

    chosen = result.get("chosen_policy", {}) or {}
    explanation = (
        f"Selected FlexGen policy with reward "
        f"{float(result.get('reward', 0.0)):.2f}. "
        f"Pareto candidates: {int(result.get('pareto_count', 0))}."
    )
    return FlexGenOptimizeResponse(
        chosen_policy=chosen,
        metrics=result.get("metrics", {}) or {},
        reward=float(result.get("reward", 0.0)),
        pareto_count=int(result.get("pareto_count", 0)),
        drift_detected=bool(result.get("drift_detected", False)),
        explanation=explanation,
    )


@app.get("/flexgen/status", response_model=FlexGenStatusResponse,
         tags=["FlexGen"])
async def flexgen_status(
    request: Request,
    user: Dict = Depends(get_current_user),
):
    if not FLEXGEN_AVAILABLE:
        raise HTTPException(status_code=501,
                            detail="FlexGen modules not available")
    gpu_metrics: List[Dict[str, Any]] = []
    if app_state.profiler is not None:
        try:
            if hasattr(app_state.profiler, "get_all_gpu_metrics"):
                maybe = app_state.profiler.get_all_gpu_metrics()
                if inspect.iscoroutine(maybe):
                    gpu_metrics = await maybe
                else:
                    gpu_metrics = maybe or []
            elif hasattr(app_state.profiler, "get_gpu_metrics"):
                gpu_metrics = [app_state.profiler.get_gpu_metrics() or {}]
        except Exception as e:
            logger.warning("GPU metrics unavailable: %s", e)
    drift = {}
    if app_state.policy_drift_detector is not None:
        try:
            drift = app_state.policy_drift_detector.get_stats() or {}
        except Exception as e:
            logger.warning("Drift stats unavailable: %s", e)
    return FlexGenStatusResponse(gpu=list(gpu_metrics or []), drift=drift)


# =============================================================================
# Chaos
# =============================================================================
@app.post("/chaos/inject", tags=["Chaos Testing"])
async def inject_fault(
    payload: ChaosInjectRequest,
    request: Request,
    user: Dict = Depends(require_admin),
):
    fault = payload.fault_type

    if fault == "database_failure":
        app_state.db_circuit.force_open()
        return {"status": "injected", "fault": fault}
    if fault == "profiler_failure":
        app_state.profiler = None
        return {"status": "injected", "fault": fault}
    if fault == "high_carbon":
        return {"status": "injected", "fault": fault,
                "note": "Set carbon_intensity high in the next /flexgen/optimize call."}
    if fault == "reset_bandit":
        if app_state.bandit is not None and hasattr(app_state.bandit, "state"):
            app_state.bandit.state = {}
        return {"status": "injected", "fault": fault}
    if fault == "reset":
        app_state.db_circuit.reset()
        app_state.learning_circuit.reset()
        if ENHANCEMENTS_AVAILABLE:
            app_state.profiler = GPUProfiler()
            try:
                app_state.profiler.start()
            except Exception:
                pass
        return {"status": "reset"}
    raise HTTPException(status_code=400, detail="Unknown fault type")


@app.get("/chaos/report", response_model=ChaosReportResponse,
         tags=["Chaos Testing"])
async def chaos_report(request: Request, user: Dict = Depends(require_admin)):
    bandit_state: Dict[str, Any] = {}
    if app_state.bandit is not None:
        bandit_state = getattr(app_state.bandit, "state", {}) or {}
    return ChaosReportResponse(
        circuit_breakers={
            "database": app_state.db_circuit.state if app_state.db_circuit else "n/a",
            "learning": app_state.learning_circuit.state if app_state.learning_circuit else "n/a",
        },
        profiler_available=app_state.profiler is not None,
        rate_limiter="redis" if app_state.redis_client else "memory",
        bandit_state=bandit_state,
        task_status=app_state.task_manager.snapshot() if app_state.task_manager else {},
    )


# =============================================================================
# Background retraining
# =============================================================================
async def retraining_loop() -> None:
    settings = get_settings()
    while True:
        try:
            await asyncio.sleep(settings.retrain_interval_seconds)
            await retraining_task()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Retraining loop error: %s", e)


async def retraining_task() -> Dict[str, Any]:
    settings = get_settings()
    async with app_state.session_factory() as session:
        result = await session.execute(
            select(FeedbackRecord.expert_id,
                   FeedbackRecord.modp_utility,
                   FeedbackRecord.context_vector)
            .order_by(FeedbackRecord.created_at.desc())
            .limit(500)
        )
        rows = result.all()
    if len(rows) < settings.min_feedback_for_retrain:
        logger.info("Not enough feedback for retraining", count=len(rows))
        return {"status": "skipped", "records": len(rows)}
    trained = 0
    if app_state.bandit is not None and hasattr(app_state.bandit, "train_from_batch"):
        try:
            trained = int(app_state.bandit.train_from_batch(rows) or 0)
        except Exception as e:
            logger.warning("Bandit train_from_batch failed: %s", e)
    # Persist learned state.
    try:
        async with app_state.session_factory() as session:
            await save_learning_state(session)
    except Exception as e:
        logger.warning("Persist after retraining failed: %s", e)
    logger.info("Retraining completed", records_processed=len(rows),
                bandit_updates=trained)
    return {"status": "completed", "records": len(rows),
            "bandit_updates": trained}


# =============================================================================
# CLI + tests
# =============================================================================
def run_tests() -> None:
    import tempfile
    import httpx  # type: ignore  # FastAPI test client dependency
    from fastapi.testclient import TestClient

    # Configure test env before importing settings.
    tmp = tempfile.mkdtemp()
    os.environ["ADAPTIVE_API_DB_URL"] = f"sqlite+aiosqlite:///{tmp}/test.db"
    os.environ["ADAPTIVE_API_AUTH_SECRET"] = "test-secret"
    os.environ["ADAPTIVE_API_REQUIRE_HUMAN_APPROVAL"] = "false"
    os.environ["ADAPTIVE_API_RATE_LIMIT_ENABLED"] = "false"
    reset_settings_for_tests()

    with TestClient(app) as client:
        # --- health ---
        r = client.get("/health")
        assert r.status_code == 200, r.text
        assert r.json()["status"] in ("healthy", "degraded")

        # --- metrics ---
        r = client.get("/metrics")
        assert r.status_code == 200

        # --- JWT ---
        settings = get_settings()
        token_user = issue_token("alice", ["user"], settings)
        headers_user = {"Authorization": f"Bearer {token_user}"}
        token_admin = issue_token("root", ["admin"], settings)
        headers_admin = {"Authorization": f"Bearer {token_admin}"}

        # unauthenticated
        r = client.post("/feedback", json={
            "expert_id": "e1", "node_id": "n1",
            "predicted_cost": 0.1, "actual_cost": 0.2,
        })
        assert r.status_code == 401

        # --- feedback ---
        payload = {
            "expert_id": "expert_a", "node_id": "node_x",
            "predicted_cost": 0.1, "actual_cost": 0.15,
            "actual_metrics": {"energy_joules": 100.0, "carbon_kg": 1.0,
                               "latency_ms": 200.0, "accuracy": 0.9},
            "distillation_loss": 0.12,
        }
        r = client.post("/feedback", json=payload, headers=headers_user)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["recorded"] is True
        assert "modp_utility" in body

        # --- history (admin only) ---
        r = client.get("/feedback/history?limit=10", headers=headers_admin)
        assert r.status_code == 200, r.text
        hist = r.json()
        assert hist["limit"] == 10
        assert len(hist["records"]) >= 1

        # --- best expert ---
        r = client.post("/optimization/best-expert",
                        json={"context": {"x": 1}},
                        headers=headers_user)
        assert r.status_code == 200, r.text
        assert r.json()["expert_id"]

        # --- pareto ---
        r = client.post("/optimization/pareto",
                        json={"objectives": {"energy": 0.5, "accuracy": 0.9}},
                        headers=headers_user)
        assert r.status_code == 200, r.text

        # --- chaos reset ---
        r = client.post("/chaos/inject", json={"fault_type": "reset"},
                        headers=headers_admin)
        assert r.status_code == 200, r.text

        # --- chaos report ---
        r = client.get("/chaos/report", headers=headers_admin)
        assert r.status_code == 200, r.text
        assert "circuit_breakers" in r.json()

        # --- invalid payload ---
        r = client.post("/feedback", json={
            "expert_id": "", "node_id": "n", "predicted_cost": -1,
            "actual_cost": 0,
        }, headers=headers_user)
        assert r.status_code == 422

    print("All Adaptive API tests passed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Adaptive API Service")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, server_header=False)


if __name__ == "__main__":
    main()
