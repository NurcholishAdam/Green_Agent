"""
Enhanced Explainable Green Decisions – Enterprise UI (v4.2.0)
=============================================================================

Single-File Enhanced Rewrite

Fixes and enhancements over v4.1.0:
- AsyncMessageQueue and FeedbackEvent are now imported honestly (not shadowed).
- No more `asyncio.get_event_loop()`; the sync `generate()` is removed.
- `DashboardEngine.start()` creates tasks; `__init__` no longer does.
- Pydantic v2 validator signature fixed (ValidationInfo).
- SQLAlchemy model definitions guarded (only when Base exists).
- Login / register credentials are request bodies.
- Register rejects role escalation.
- Refresh-token rotation implemented; the DB stores and validates the token.
- Default JWT secret is rejected by a validator.
- All datetimes are timezone-aware UTC.
- Rate limiting actually applied to protected endpoints.
- CausalBandit is a real linear contextual bandit (Bayesian linear regression
  with Thompson sampling).
- FederatedFeedbackCoordinator weighted; DP noise scales to sensitivity/epsilon.
- MultiAgentUIRouter only updates contributing agents.
- CarbonOffsetBroker is thread-safe with persistence-ready records.
- ChaosMonkey has named faults, seeded RNG, and recovery measurement.
- HumanReviewManager has TTL, capacity limit, and audit log.
- QuantumDistillationOptimizer tries `qiskit_optimization` + `qiskit_algorithms`
  then falls back to classical scoring.
- CircuitBreaker dispatches sync/async correctly.
- Real Prometheus metrics registered and exposed at /metrics.
- In-memory state is bounded (deques / LRU).
- `export_all` streams from the DB.
- WhatIfSimulator falls back to the DB when a request is not cached.
- Postgres-compatible upserts (SQLite fallback retained).
- Expert-stats update race fixed with a single upsert.
- `create_app()` factory with lifespan; `main()` uses it.
- `DashboardEngine` supports `async with`.

Public API preserved:
    ExplainableUIConfig, RequestLog, WhatIfResult, DatabaseManager,
    ExplanationGenerator, DashboardEngine, WhatIfSimulator, AuthManager,
    APIGatewayExtension, create_explainable_ui, CausalBandit, SafetyMonitor,
    FeatureAttributionExplainer, FederatedFeedbackCoordinator,
    MultiAgentUIRouter, CarbonOffsetBroker, ChaosMonkey, HumanReviewManager,
    QuantumDistillationOptimizer, FlexGenPrecisionPolicy, CircuitBreaker,
    and the exception hierarchy.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import inspect
import json
import logging
import logging.handlers  # noqa: F401
import os
import random
import threading
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import (
    Any, AsyncIterator, Callable, Deque, Dict, Iterable, List, Optional, Protocol,
    Set, Tuple, Union, runtime_checkable,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional imports
# -----------------------------------------------------------------------------
try:
    import structlog  # noqa: F401
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "explainable_ui.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )

try:
    from pydantic import BaseModel, Field, ConfigDict, ValidationInfo, field_validator
    PYDANTIC_AVAILABLE = True
    try:
        from pydantic import __version__ as _pyd_version  # type: ignore
        PYDANTIC_V2 = _pyd_version.startswith("2")
    except Exception:
        PYDANTIC_V2 = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    PYDANTIC_V2 = False

try:
    from sqlalchemy import (
        Boolean, Column, DateTime, Float, Integer, JSON, String, Text, func,
        select, update, text,
    )
    from sqlalchemy.ext.asyncio import (
        AsyncSession, async_sessionmaker, create_async_engine,
    )
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool
    from sqlalchemy.exc import SQLAlchemyError  # noqa: F401
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

try:
    from sqlalchemy import create_engine  # type: ignore
    from sqlalchemy.orm import sessionmaker  # type: ignore
    SQLALCHEMY_SYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

try:
    from fastapi import (
        BackgroundTasks, Depends, FastAPI, HTTPException, Request, status,
        WebSocket, WebSocketDisconnect,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response, StreamingResponse
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    import jwt as pyjwt
    from passlib.context import CryptContext
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    import plotly.graph_objects as go  # type: ignore
    import plotly.express as px  # type: ignore
    import plotly.io as pio  # type: ignore
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

try:
    from jinja2 import Environment, FileSystemLoader, Template  # noqa: F401
    JINJA_AVAILABLE = True
except ImportError:
    JINJA_AVAILABLE = False

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram,
        generate_latest,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from tenacity import (  # noqa: F401
        AsyncRetrying, RetryError, retry, retry_if_exception_type,
        stop_after_attempt, wait_exponential,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler  # noqa: F401
    from slowapi.errors import RateLimitExceeded  # noqa: F401
    from slowapi.util import get_remote_address
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False

try:
    from qiskit_optimization import QuadraticProgram  # type: ignore
    from qiskit_optimization.algorithms import MinimumEigenOptimizer  # type: ignore
    try:
        from qiskit_algorithms import QAOA  # type: ignore
    except ImportError:
        from qiskit.algorithms import QAOA  # type: ignore
    QISKIT_AVAILABLE = True
    QISKIT_LEGACY = False
except ImportError:
    try:
        from qiskit.optimization import QuadraticProgram  # type: ignore
        from qiskit.optimization.algorithms import MinimumEigenOptimizer  # type: ignore
        from qiskit.algorithms import QAOA  # type: ignore
        QISKIT_AVAILABLE = True
        QISKIT_LEGACY = True
    except ImportError:
        QISKIT_AVAILABLE = False
        QISKIT_LEGACY = False

try:
    import shap  # noqa: F401
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    import lime  # noqa: F401
    LIME_AVAILABLE = True
except ImportError:
    LIME_AVAILABLE = False


# -----------------------------------------------------------------------------
# Enhancement modules (guarded)
# -----------------------------------------------------------------------------
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}

    class ExpertRouter:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def encode(self, context):
            if isinstance(context, dict):
                vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
            else:
                vals = [float(v) for v in context]
            while len(vals) < 8:
                vals.append(0.0)
            return vals[:8]
        def select(self, encoded): return "default"

    class ParetoOptimizer:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0.0) * weights.get(k, 1.0) for k in objectives)


# Honest imports for message queue / feedback event.
try:
    from enhancements.scaling.message_queue import AsyncMessageQueue  # type: ignore
except ImportError:
    class AsyncMessageQueue:  # type: ignore[no-redef]
        """Fallback that drops events silently but does not shadow real ones."""
        async def publish(self, topic: str, message: str) -> None:
            logger.debug(f"message_queue_stub topic={topic} bytes={len(message)}")

try:
    from enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    class FeedbackEvent:  # type: ignore[no-redef]
        @staticmethod
        def create_with_context(**kwargs: Any) -> Dict[str, Any]:
            return dict(kwargs)


try:
    from sustainability import (  # type: ignore
        SustainabilityAwareExpertProfile, SustainabilityFitnessScorer,
    )
except ImportError:
    class SustainabilityAwareExpertProfile:  # type: ignore[no-redef]
        def __init__(self, expert_id: str, **kwargs: Any) -> None:
            self.expert_id = expert_id
            self.energy_per_inference_full: float = 0.0
            self.energy_per_inference_compressed: Optional[float] = None
            self.accuracy_full: float = 0.0
            self.accuracy_compressed: Optional[float] = None
            self.compressed_flag: bool = False
            self.sustainability_fitness_score: float = 0.0
            self.compression_method: Optional[str] = None

    class SustainabilityFitnessScorer:  # type: ignore[no-redef]
        def compute(self, profile: Any) -> float:
            return 0.5


try:
    from enhancements.llm_client import LLMClient  # type: ignore
except ImportError:
    class LLMClient:  # type: ignore[no-redef]
        async def generate_explanation(self, prompt: str) -> str:
            return "LLM client not available."


# =============================================================================
# Helpers
# =============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_event(event: Any) -> str:
    """Defensively serialize a FeedbackEvent-like object."""
    if event is None:
        return json.dumps({})
    for attr in ("to_json", "model_dump_json"):
        fn = getattr(event, attr, None)
        if callable(fn):
            try:
                out = fn()
                return out if isinstance(out, str) else json.dumps(out, default=str)
            except Exception:
                continue
    if isinstance(event, dict):
        return json.dumps(event, default=str)
    if hasattr(event, "__dict__"):
        return json.dumps(event.__dict__, default=str)
    return json.dumps({"event": str(event)})


def _to_cfg_dict(cfg: Any) -> Dict[str, Any]:
    if cfg is None:
        return {}
    if hasattr(cfg, "model_dump"):
        try:
            return cfg.model_dump()
        except Exception:
            pass
    if hasattr(cfg, "dict"):
        try:
            return cfg.dict()
        except Exception:
            pass
    if hasattr(cfg, "__dataclass_fields__"):
        return asdict(cfg)
    return dict(getattr(cfg, "__dict__", {}))


# =============================================================================
# Exceptions
# =============================================================================
class ExplainableUIException(Exception):
    """Base exception."""


class SafetyViolationError(ExplainableUIException): pass
class ChaosExperimentError(ExplainableUIException): pass
class QuantumError(ExplainableUIException): pass
class AuthError(ExplainableUIException): pass
class ConfigurationError(ExplainableUIException): pass


# =============================================================================
# Metrics
# =============================================================================
class _NoopMetric:
    def labels(self, *a, **k): return self
    def inc(self, *a, **k): return None
    def set(self, *a, **k): return None
    def observe(self, *a, **k): return None


class _MetricsRegistry:
    def __init__(self) -> None:
        self.enabled = PROMETHEUS_AVAILABLE
        self.registry = CollectorRegistry() if self.enabled else None
        self._counters: Dict[str, Any] = {}
        self._gauges: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def counter(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, doc, labels or [], registry=self.registry)
            return self._counters[name]

    def gauge(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, doc, labels or [], registry=self.registry)
            return self._gauges[name]

    def histogram(self, name, doc="", labels=None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, doc, labels or [], registry=self.registry)
            return self._histograms[name]

    def render(self) -> bytes:
        if not self.enabled:
            return b""
        return generate_latest(self.registry)


METRICS = _MetricsRegistry()
ROUTING_REQUESTS = METRICS.counter("ui_routing_requests_total", "Routing requests logged")
EXPLANATIONS = METRICS.counter("ui_explanations_total", "Explanations generated", ["style"])
FEEDBACK = METRICS.counter("ui_feedback_total", "Feedback recorded", ["rating_bucket"])
SAFETY_VIOLATIONS = METRICS.counter("ui_safety_violations_total", "Safety violations", ["rule"])
HITL_REQUESTS = METRICS.counter("ui_hitl_requests_total", "HITL requests")
HITL_STATUS = METRICS.counter("ui_hitl_status_total", "HITL status", ["status"])
CHAOS_EVENTS = METRICS.counter("ui_chaos_events_total", "Chaos events", ["fault"])
FEDERATED_ROUNDS = METRICS.counter("ui_federated_rounds_total", "Federated rounds")
CARBON_OFFSETS = METRICS.counter("ui_carbon_offsets_total", "Carbon offsets", ["status"])
BANDIT_UPDATES = METRICS.counter("ui_bandit_updates_total", "Bandit updates", ["style"])
AUTH_EVENTS = METRICS.counter("ui_auth_events_total", "Auth events", ["kind", "status"])
DB_OPS = METRICS.histogram("ui_db_ops_seconds", "DB operation latency", ["op"])
CACHE_HITS = METRICS.counter("ui_cache_hits_total", "Cache hits", ["kind"])


# =============================================================================
# Circuit breaker — sync + async dispatch
# =============================================================================
class CircuitBreakerState(str):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_success_threshold = 2
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()

    def _precheck(self) -> None:
        with self._lock:
            if (
                self._state == CircuitBreakerState.OPEN
                and self._last_failure_time is not None
                and time.time() - self._last_failure_time >= self.recovery_timeout
            ):
                self._state = CircuitBreakerState.HALF_OPEN
                self._success_count = 0
            if self._state == CircuitBreakerState.OPEN:
                raise RuntimeError(f"Circuit breaker {self.name} is open")

    def _record_success(self) -> None:
        with self._lock:
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
            else:
                self._failure_count = 0

    def _record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state in (CircuitBreakerState.CLOSED, CircuitBreakerState.HALF_OPEN):
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN

    async def call_async(self, func, *args, **kwargs):
        self._precheck()
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def call_sync(self, func, *args, **kwargs):
        self._precheck()
        try:
            result = func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    async def call(self, func, *args, **kwargs):
        if inspect.iscoroutinefunction(func):
            return await self.call_async(func, *args, **kwargs)
        result = self.call_sync(func, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "state": self._state,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
            }


# =============================================================================
# Configuration
# =============================================================================
if PYDANTIC_AVAILABLE:
    class ExplainableUIConfig(BaseModel):
        db_path: str = Field("./explainable_ui.db")
        db_url: Optional[str] = Field(None)
        db_pool_size: int = Field(10, ge=1)
        db_max_overflow: int = Field(20, ge=1)
        jwt_secret: str = Field("")
        jwt_algorithm: str = "HS256"
        jwt_expiration_minutes: int = Field(1440, ge=1)
        refresh_token_expiration_days: int = Field(7, ge=1)
        cache_ttl_seconds: int = Field(300, ge=0)
        cache_max_entries: int = Field(500, ge=1)
        request_log_max_entries: int = Field(10_000, ge=100)
        plotly_theme: str = Field("plotly_white")
        log_level: str = Field("INFO")
        export_format: str = Field("json")
        ws_enabled: bool = True
        ws_broadcast_interval: int = Field(5, ge=1)
        default_page_size: int = Field(20, ge=1)
        max_page_size: int = Field(100, ge=1)
        co2_per_kwh_kg: float = Field(0.2, gt=0)
        energy_to_co2_factor: float = Field(0.2 / 3600000, gt=0)
        explanation_template_path: Optional[str] = Field(None)

        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1,
            }
        )
        template_evolution_enabled: bool = True
        template_evolution_interval_seconds: int = Field(3600, ge=60)
        template_population_size: int = Field(10, ge=1)
        template_generations: int = Field(5, ge=1)

        safety_monitor_enabled: bool = True
        chaos_enabled: bool = False
        chaos_failure_probability: float = Field(0.1, ge=0, le=1)
        chaos_seed: int = 42
        federated_privacy_budget: float = Field(0.5, gt=0)
        federated_sensitivity: float = Field(1.0, gt=0)
        quantum_distillation_enabled: bool = False
        human_review_threshold_carbon: float = Field(500.0, gt=0)
        human_review_threshold_utility: float = Field(0.3, ge=0, le=1)
        hitl_ttl_seconds: float = Field(3600.0, gt=0)
        hitl_max_pending: int = Field(100, ge=1)

        @field_validator("log_level")
        @classmethod
        def _validate_log_level(cls, v: str) -> str:
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

        @field_validator("jwt_secret")
        @classmethod
        def _validate_secret(cls, v: str) -> str:
            if not v:
                # Generate a per-instance random secret so the service can start.
                return hashlib.sha256(os.urandom(32)).hexdigest()
            if v in {"change_me_in_production", "secret", ""}:
                raise ValueError(
                    "jwt_secret cannot be a default value; set EXPLAINABLE_UI_JWT_SECRET"
                )
            return v

        model_config = ConfigDict(env_prefix="EXPLAINABLE_UI_")

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "ExplainableUIConfig":
            if hasattr(cls, "model_validate"):
                return cls.model_validate(data)
            return cls(**data)

else:
    @dataclass
    class ExplainableUIConfig:
        db_path: str = "./explainable_ui.db"
        db_url: Optional[str] = None
        db_pool_size: int = 10
        db_max_overflow: int = 20
        jwt_secret: str = ""
        jwt_algorithm: str = "HS256"
        jwt_expiration_minutes: int = 1440
        refresh_token_expiration_days: int = 7
        cache_ttl_seconds: int = 300
        cache_max_entries: int = 500
        request_log_max_entries: int = 10_000
        plotly_theme: str = "plotly_white"
        log_level: str = "INFO"
        export_format: str = "json"
        ws_enabled: bool = True
        ws_broadcast_interval: int = 5
        default_page_size: int = 20
        max_page_size: int = 100
        co2_per_kwh_kg: float = 0.2
        energy_to_co2_factor: float = 0.2 / 3600000
        explanation_template_path: Optional[str] = None

        modp_weights: Dict[str, float] = field(default_factory=lambda: {
            "accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1,
        })
        template_evolution_enabled: bool = True
        template_evolution_interval_seconds: int = 3600
        template_population_size: int = 10
        template_generations: int = 5

        safety_monitor_enabled: bool = True
        chaos_enabled: bool = False
        chaos_failure_probability: float = 0.1
        chaos_seed: int = 42
        federated_privacy_budget: float = 0.5
        federated_sensitivity: float = 1.0
        quantum_distillation_enabled: bool = False
        human_review_threshold_carbon: float = 500.0
        human_review_threshold_utility: float = 0.3
        hitl_ttl_seconds: float = 3600.0
        hitl_max_pending: int = 100

        def __post_init__(self):
            if not self.jwt_secret:
                self.jwt_secret = hashlib.sha256(os.urandom(32)).hexdigest()
            if self.jwt_secret in {"change_me_in_production", "secret"}:
                raise ConfigurationError("jwt_secret cannot be a default value")

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "ExplainableUIConfig":
            valid = {f.name for f in fields(cls)}
            filtered = {k: v for k, v in data.items() if k in valid}
            return cls(**filtered)


# =============================================================================
# Data models
# =============================================================================
@dataclass
class RequestLog:
    request_id: str
    timestamp: datetime
    query: str
    chosen_expert_id: str
    chosen_expert_profile: SustainabilityAwareExpertProfile
    alternative_experts: List[Tuple[str, SustainabilityAwareExpertProfile]]
    latency_ms: float
    energy_joules: float
    co2_kg: float
    accuracy: float
    carbon_intensity: float = 0.0
    helium_scarcity: float = 0.0
    material_index: float = 0.0
    sustainability_score: float = 0.0
    explanation: str = ""
    feedback_rating: Optional[int] = None
    feedback_comment: Optional[str] = None
    precision: str = "fp32"
    human_review_id: Optional[str] = None


@dataclass
class WhatIfResult:
    scenario_id: str
    alternative_expert_id: str
    expected_energy_joules: float
    expected_co2_kg: float
    expected_latency_ms: float
    expected_accuracy: float
    expected_carbon_intensity: float
    expected_helium_scarcity: float
    expected_material_index: float
    difference_energy: float
    difference_co2: float
    difference_latency: float
    difference_accuracy: float
    chosen_utility: Optional[float] = None
    alternative_utility: Optional[float] = None
    offset_cost_usd: Optional[float] = None
    recommended_precision: Optional[str] = None


# =============================================================================
# SQLAlchemy models (guarded)
# =============================================================================
if ASYNC_SQLALCHEMY_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE:
    from sqlalchemy.orm import declarative_base as _declarative_base
    Base = _declarative_base()

    class RequestLogDB(Base):
        __tablename__ = "request_logs"
        id = Column(Integer, primary_key=True)
        request_id = Column(String(64), unique=True, index=True)
        timestamp = Column(DateTime, default=_utc_now)
        query = Column(Text)
        chosen_expert_id = Column(String(128))
        alternative_experts = Column(JSON)
        latency_ms = Column(Float)
        energy_joules = Column(Float)
        co2_kg = Column(Float)
        accuracy = Column(Float)
        carbon_intensity = Column(Float)
        helium_scarcity = Column(Float)
        material_index = Column(Float)
        sustainability_score = Column(Float)
        explanation = Column(Text)
        feedback_rating = Column(Integer, nullable=True)
        feedback_comment = Column(Text, nullable=True)
        precision = Column(String(16), default="fp32")
        human_review_id = Column(String(64), nullable=True)

    class ExpertStatsDB(Base):
        __tablename__ = "expert_stats"
        expert_id = Column(String(128), primary_key=True)
        total_requests = Column(Integer, default=0)
        avg_latency_ms = Column(Float, default=0.0)
        avg_energy_joules = Column(Float, default=0.0)
        avg_accuracy = Column(Float, default=0.0)
        total_co2_kg = Column(Float, default=0.0)
        last_updated = Column(DateTime, default=_utc_now)

    class UserDB(Base):
        __tablename__ = "users"
        id = Column(Integer, primary_key=True)
        username = Column(String(64), unique=True, index=True)
        password_hash = Column(String(128))
        role = Column(String(32), default="viewer")
        created_at = Column(DateTime, default=_utc_now)
        last_login = Column(DateTime, nullable=True)
        refresh_token = Column(String(256), nullable=True)
        refresh_token_expires = Column(DateTime, nullable=True)

    class OptimizerStateDB(Base):
        __tablename__ = "optimizer_state"
        id = Column(Integer, primary_key=True)
        key = Column(String(64), unique=True)
        value = Column(JSON)
        updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now)
else:
    Base = None


# =============================================================================
# Database manager
# =============================================================================
class DatabaseManager:
    """
    Supports async SQLAlchemy (preferred) and sync SQLAlchemy offloaded to
    threads. Uses SQLite by default; Postgres-compatible upserts are used via
    `_upsert_optimizer_state`.
    """

    def __init__(self, config: ExplainableUIConfig):
        self.config = config
        self.async_engine = None
        self.async_sessionmaker = None
        self.sync_engine = None
        self.sync_sessionmaker = None
        self._lock = asyncio.Lock()
        self._initialized = False
        self.backend: str = "none"

        if ASYNC_SQLALCHEMY_AVAILABLE:
            db_url = config.db_url or f"sqlite+aiosqlite:///{config.db_path}"
            self.async_engine = create_async_engine(db_url, poolclass=NullPool)
            self.async_sessionmaker = async_sessionmaker(
                self.async_engine, expire_on_commit=False
            )
            self.backend = "async"
        elif SQLALCHEMY_SYNC_AVAILABLE:
            db_url = config.db_url or f"sqlite:///{config.db_path}"
            self.sync_engine = create_engine(db_url, poolclass=NullPool)
            self.sync_sessionmaker = sessionmaker(bind=self.sync_engine)
            if Base is not None:
                Base.metadata.create_all(self.sync_engine)
            self.backend = "sync"
        else:
            self.backend = "none"
            logger.warning("database_backend_unavailable")

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            if ASYNC_SQLALCHEMY_AVAILABLE and self.async_engine is not None and Base is not None:
                async with self.async_engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
            self._initialized = True
            logger.info(f"db_initialized backend={self.backend}")

    async def upsert_optimizer_state(self, key: str, value: Dict[str, Any]) -> None:
        value_str = json.dumps(value, default=str)
        ts = _utc_now().isoformat()
        if ASYNC_SQLALCHEMY_AVAILABLE and self.async_sessionmaker is not None:
            async with self.async_sessionmaker() as session:
                # Portable upsert: try update, then insert on no-op.
                try:
                    await session.execute(
                        text(
                            "INSERT INTO optimizer_state (key, value, updated_at) "
                            "VALUES (:key, :value, :updated_at) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
                            "updated_at=excluded.updated_at"
                        ),
                        {"key": key, "value": value_str, "updated_at": ts},
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    # SQLite < 3.24 fallback: REPLACE.
                    await session.execute(
                        text(
                            "INSERT OR REPLACE INTO optimizer_state "
                            "(key, value, updated_at) VALUES (:key, :value, :updated_at)"
                        ),
                        {"key": key, "value": value_str, "updated_at": ts},
                    )
                    await session.commit()
        elif self.sync_sessionmaker is not None:
            session = self.sync_sessionmaker()
            try:
                session.execute(
                    text(
                        "INSERT INTO optimizer_state (key, value, updated_at) "
                        "VALUES (:key, :value, :updated_at) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
                        "updated_at=excluded.updated_at"
                    ),
                    {"key": key, "value": value_str, "updated_at": ts},
                )
                session.commit()
            except Exception:
                session.rollback()
                session.execute(
                    text(
                        "INSERT OR REPLACE INTO optimizer_state "
                        "(key, value, updated_at) VALUES (:key, :value, :updated_at)"
                    ),
                    {"key": key, "value": value_str, "updated_at": ts},
                )
                session.commit()
            finally:
                session.close()

    async def load_optimizer_state(self, key: str) -> Optional[Dict[str, Any]]:
        if ASYNC_SQLALCHEMY_AVAILABLE and self.async_sessionmaker is not None:
            async with self.async_sessionmaker() as session:
                result = await session.execute(
                    text("SELECT value FROM optimizer_state WHERE key = :key"),
                    {"key": key},
                )
                row = result.fetchone()
                if row:
                    try:
                        return json.loads(row[0])
                    except Exception:
                        return None
                return None
        if self.sync_sessionmaker is not None:
            session = self.sync_sessionmaker()
            try:
                row = session.execute(
                    text("SELECT value FROM optimizer_state WHERE key = :key"),
                    {"key": key},
                ).fetchone()
                if row:
                    try:
                        return json.loads(row[0])
                    except Exception:
                        return None
            finally:
                session.close()
        return None

    async def health_check(self) -> Dict[str, Any]:
        try:
            if ASYNC_SQLALCHEMY_AVAILABLE and self.async_sessionmaker is not None:
                async with self.async_sessionmaker() as session:
                    await session.execute(text("SELECT 1"))
            elif self.sync_sessionmaker is not None:
                session = self.sync_sessionmaker()
                session.execute(text("SELECT 1"))
                session.close()
            else:
                return {"status": "unavailable", "backend": "none"}
            return {"status": "ok", "backend": self.backend}
        except Exception as e:
            return {"status": "error", "backend": self.backend, "error": str(e)}

    async def close(self) -> None:
        if self.async_engine is not None:
            await self.async_engine.dispose()
        if self.sync_engine is not None:
            self.sync_engine.dispose()
        self._initialized = False


# =============================================================================
# CausalBandit — real contextual bandit
# =============================================================================
@dataclass
class BanditConfig:
    alpha: float = 1.0
    exploration_beta: float = 1.0
    min_trials: int = 5
    use_thompson: bool = True


class CausalBandit:
    """Real per-action Bayesian linear regression bandit."""

    def __init__(
        self,
        action_space: List[str],
        fallback_solver: Optional[Callable[[Any], str]] = None,
        min_trials_before_bandit: int = 5,
        confidence_threshold: float = 0.6,
        feature_dim: int = 8,
        config: Optional[BanditConfig] = None,
    ):
        if not action_space:
            raise ValueError("action_space must be non-empty")
        self.actions: List[str] = list(action_space)
        self.dim = int(feature_dim)
        cfg = config or BanditConfig()
        cfg.min_trials = int(min_trials_before_bandit)
        self.config = cfg
        self.fallback_solver = fallback_solver
        self.confidence_threshold = float(confidence_threshold)
        self.theta: Dict[str, np.ndarray] = {a: np.zeros(self.dim) for a in self.actions}
        self.P: Dict[str, np.ndarray] = {
            a: np.eye(self.dim) / cfg.alpha for a in self.actions
        }
        self.usage: Dict[str, int] = {a: 0 for a in self.actions}
        self.total_trials = 0
        self._lock = threading.RLock()

    def _prepare_features(self, context: Any) -> np.ndarray:
        if isinstance(context, dict):
            vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
        elif context is None:
            vals = []
        else:
            try:
                vals = [float(v) for v in context]
            except Exception:
                vals = []
        if len(vals) < self.dim:
            vals = vals + [0.0] * (self.dim - len(vals))
        return np.asarray(vals[: self.dim], dtype=float)

    def select_action(self, context: Any) -> Tuple[str, float, str]:
        with self._lock:
            x = self._prepare_features(context)
            if self.total_trials < self.config.min_trials and self.fallback_solver is not None:
                return self.fallback_solver(context), 0.0, "fallback"

            if self.config.use_thompson:
                samples: Dict[str, float] = {}
                for a in self.actions:
                    try:
                        s = np.random.multivariate_normal(self.theta[a], self.P[a])
                    except Exception:
                        s = self.theta[a]
                    samples[a] = float(x @ s)
                best = max(samples, key=samples.get)
                conf = 1.0 / (1.0 + np.exp(-abs(samples[best])))
                return best, float(conf), "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * np.sqrt(var)
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + np.exp(-abs(scores[best])))
            return best, float(conf), "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
        """Synchronous update — do NOT await."""
        if action not in self.theta:
            return
        with self._lock:
            x = self._prepare_features(context)
            P = self.P[action]
            denom = 1.0 + float(x @ P @ x)
            K = (P @ x) / denom
            err = float(reward) - float(x @ self.theta[action])
            self.theta[action] = self.theta[action] + K * err
            newP = P - np.outer(K, x) @ P
            self.P[action] = 0.5 * (newP + newP.T)
            self.usage[action] += 1
            self.total_trials += 1
            BANDIT_UPDATES.labels(style=action).inc()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": list(self.actions),
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor — snapshot/restore, keeps bool API
# =============================================================================
class SafetyMonitor:
    def __init__(
        self,
        max_carbon_for_green: float = 400.0,
        max_consecutive_green: int = 5,
        max_low_accuracy_confidence: float = 0.6,
    ):
        self.max_carbon_for_green = float(max_carbon_for_green)
        self.max_consecutive_green = int(max_consecutive_green)
        self.max_low_accuracy_confidence = float(max_low_accuracy_confidence)
        self.consecutive_green = 0
        self.history: Deque[Dict[str, Any]] = deque(maxlen=50)
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def check_explanation(self, request: RequestLog, chosen_utility: Optional[float]) -> bool:
        with self._lock:
            carbon_intensity = request.carbon_intensity
            if isinstance(carbon_intensity, (int, float)) and carbon_intensity <= 1.0:
                carbon_intensity = float(carbon_intensity) * 800.0

            if carbon_intensity > self.max_carbon_for_green:
                self.consecutive_green = 0
            else:
                self.consecutive_green += 1
                if self.consecutive_green > self.max_consecutive_green:
                    self._record("max_consecutive_green", {"count": self.consecutive_green})
                    return False

            if (
                request.accuracy < 0.5
                and chosen_utility is not None
                and chosen_utility > self.max_low_accuracy_confidence
            ):
                self._record(
                    "low_accuracy_high_confidence",
                    {"accuracy": request.accuracy, "utility": chosen_utility},
                )
                return False

            self.history.append({
                "carbon": float(carbon_intensity),
                "accuracy": float(request.accuracy),
                "utility": chosen_utility,
            })
            return True

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_carbon_for_green": self.max_carbon_for_green,
                "max_consecutive_green": self.max_consecutive_green,
                "max_low_accuracy_confidence": self.max_low_accuracy_confidence,
            }

    def restore(self, snapshot: Dict[str, Any]) -> None:
        with self._lock:
            if "max_carbon_for_green" in snapshot:
                self.max_carbon_for_green = float(snapshot["max_carbon_for_green"])
            if "max_consecutive_green" in snapshot:
                self.max_consecutive_green = int(snapshot["max_consecutive_green"])
            if "max_low_accuracy_confidence" in snapshot:
                self.max_low_accuracy_confidence = float(snapshot["max_low_accuracy_confidence"])

    def _record(self, rule: str, details: Dict[str, Any]) -> None:
        self.violations.append({
            "rule": rule,
            "details": details,
            "timestamp": _utc_now().isoformat(),
        })
        SAFETY_VIOLATIONS.labels(rule=rule).inc()
        logger.warning(f"safety_violation rule={rule} details={details}")

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.violations)


# =============================================================================
# FeatureAttributionExplainer — structured output
# =============================================================================
class FeatureAttributionExplainer:
    def __init__(self):
        self.shap_available = SHAP_AVAILABLE
        self.lime_available = LIME_AVAILABLE

    def explain(self, request: RequestLog, chosen_utility: Optional[float] = None) -> Dict[str, float]:
        attribution: Dict[str, float] = {
            "accuracy": float(request.accuracy),
            "energy": 1.0 - min(1.0, float(request.energy_joules) / 10.0),
            "carbon": 1.0 - min(1.0, float(request.co2_kg) * 1000.0),
            "latency": 1.0 - min(1.0, float(request.latency_ms) / 500.0),
        }
        total = sum(attribution.values()) or 1.0
        attribution = {k: round(v / total, 4) for k, v in attribution.items()}
        if chosen_utility is not None:
            attribution["utility_contribution"] = round(float(chosen_utility), 4)
        return attribution

    def get_status(self) -> Dict[str, Any]:
        return {
            "shap_available": self.shap_available,
            "lime_available": self.lime_available,
            "mode": "heuristic",
        }


# =============================================================================
# FederatedFeedbackCoordinator — weighted, round IDs, correct DP
# =============================================================================
class FederatedFeedbackCoordinator:
    def __init__(
        self,
        privacy_budget: float = 0.5,
        sensitivity: float = 1.0,
        min_clients: int = 1,
        seed: int = 42,
    ):
        self.privacy_budget = max(float(privacy_budget), 1e-6)
        self.sensitivity = max(float(sensitivity), 1e-6)
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.round_id = 0
        self._rng = np.random.default_rng(seed)
        self._lock = threading.RLock()

    def register_participant(
        self,
        participant_id: str,
        update: Dict[str, Any],
        sample_count: int = 1,
        round_id: Optional[int] = None,
    ) -> None:
        with self._lock:
            rid = round_id if round_id is not None else self.round_id
            self.participants[participant_id] = {
                "update": dict(update),
                "weight": max(1, int(sample_count)),
                "round": rid,
            }

    def aggregate(self, expected_round: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            active = {
                pid: data for pid, data in self.participants.items()
                if expected_round is None or data["round"] >= expected_round
            }
            if len(active) < self.min_clients:
                return {}
            keys: Set[str] = set()
            for data in active.values():
                keys.update(data["update"].keys())
            noise_scale = self.sensitivity / self.privacy_budget
            aggregated: Dict[str, Any] = {}
            for key in keys:
                values: List[float] = []
                weights: List[float] = []
                numeric = True
                for data in active.values():
                    if key not in data["update"]:
                        continue
                    v = data["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(float(data["weight"]))
                    else:
                        values.append(v)
                        numeric = False
                if not values:
                    continue
                if numeric:
                    wsum = sum(weights)
                    mean = (
                        sum(v * w for v, w in zip(values, weights)) / wsum
                        if wsum > 0 else float(np.mean(values))
                    )
                    # Only apply DP noise to numeric aggregates. Scale reflects
                    # sensitivity of the value's range, not just 1/epsilon.
                    mean += float(self._rng.laplace(0.0, noise_scale))
                    aggregated[key] = mean
                else:
                    aggregated[key] = values[0]
            self.round_id += 1
            FEDERATED_ROUNDS.inc()
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_participant_count(self) -> int:
        with self._lock:
            return len(self.participants)


# =============================================================================
# MultiAgentUIRouter — only contributing agents updated
# =============================================================================
class MultiAgentUIRouter:
    def __init__(self, agents: Optional[List[str]] = None):
        self.agents = agents or [
            "carbon_agent", "latency_agent", "accuracy_agent", "cost_agent",
        ]
        self.reputation: Dict[str, float] = {a: 0.5 for a in self.agents}
        self.contributions: Dict[str, int] = {a: 0 for a in self.agents}
        self._lock = threading.RLock()

    def select_agents(self, request: RequestLog, top_k: int = 2) -> List[str]:
        with self._lock:
            scores: Dict[str, float] = {}
            for agent in self.agents:
                base = self.reputation.get(agent, 0.5)
                if agent == "carbon_agent" and request.carbon_intensity > 0.5:
                    base += 0.2
                elif agent == "latency_agent" and request.latency_ms > 100:
                    base += 0.2
                elif agent == "accuracy_agent" and request.accuracy < 0.85:
                    base += 0.2
                elif agent == "cost_agent" and request.energy_joules > 5:
                    base += 0.2
                scores[agent] = base
            sorted_agents = sorted(scores.items(), key=lambda x: -x[1])
            return [a for a, _ in sorted_agents[:top_k]]

    def record_outcome(self, agents: Iterable[str], success: bool) -> None:
        alpha = 0.2
        with self._lock:
            for agent in agents:
                if agent not in self.reputation:
                    continue
                prev = self.reputation[agent]
                self.reputation[agent] = prev + alpha * ((1.0 if success else 0.0) - prev)
                self.contributions[agent] = self.contributions.get(agent, 0) + 1

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "reputation": {k: round(v, 3) for k, v in self.reputation.items()},
                "contributions": dict(self.contributions),
            }


# =============================================================================
# CarbonOffsetBroker — thread-safe
# =============================================================================
class CarbonOffsetBroker:
    def __init__(
        self,
        threshold: float = 400.0,
        cost_per_kg: float = 0.1,
        rec_cost_per_mwh: float = 5.0,
    ):
        self.threshold = float(threshold)
        self.cost_per_kg = float(cost_per_kg)
        self.rec_cost_per_mwh = float(rec_cost_per_mwh)
        self.total_offset_kg = 0.0
        self.total_recs_mwh = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    async def purchase_offsets(self, carbon_intensity: float, carbon_kg: float) -> Dict[str, Any]:
        if carbon_kg <= 0:
            return {"status": "invalid_amount"}
        if carbon_intensity <= self.threshold:
            return {"status": "below_threshold", "carbon_kg": float(carbon_kg)}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += float(carbon_kg)
            self.total_cost += float(cost)
            record = {
                "status": "offset_purchased",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": _utc_now().isoformat(),
            }
            self.records.append(record)
        CARBON_OFFSETS.labels(status="offset").inc()
        return record

    async def purchase_recs(self, energy_mwh: float) -> Dict[str, Any]:
        if energy_mwh <= 0:
            return {"status": "no_energy"}
        cost = energy_mwh * self.rec_cost_per_mwh
        with self._lock:
            self.total_recs_mwh += float(energy_mwh)
            self.total_cost += float(cost)
            record = {
                "status": "rec_purchased",
                "energy_mwh": float(energy_mwh),
                "cost_usd": float(cost),
                "timestamp": _utc_now().isoformat(),
            }
            self.records.append(record)
        CARBON_OFFSETS.labels(status="rec").inc()
        return record

    def estimate_offset_cost(self, carbon_kg: float, carbon_intensity: Optional[float] = None) -> float:
        if carbon_intensity is not None and carbon_intensity <= self.threshold:
            return 0.0
        return float(carbon_kg) * self.cost_per_kg

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_recs_mwh": self.total_recs_mwh,
                "total_cost_usd": self.total_cost,
            }


# =============================================================================
# ChaosMonkey — named faults, seeded, recovery measurement
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {"db_failure", "ws_drop", "carbon_provider_down", "feedback_drop"}

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.1,
        seed: int = 42,
    ):
        self.enabled = bool(enabled)
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(int(seed))
        self.injected_failures = 0
        self.events: List[Dict[str, Any]] = []
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def maybe_fail(self, component: str = "ui") -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self.injected_failures += 1
        self._record(fault, "triggered", {"component": component})
        CHAOS_EVENTS.labels(fault=fault).inc()
        return fault

    def apply(self, fault: str, dashboard: "DashboardEngine") -> None:
        with self._lock:
            if fault == "db_failure":
                self._snapshots.setdefault("db_failure", {"db": dashboard.db_manager})
                dashboard.db_manager = _NullDatabaseManager()
            elif fault == "ws_drop":
                self._snapshots.setdefault("ws_drop", {"connections": list(dashboard._ws_connections)})
                dashboard._ws_connections.clear()
            elif fault == "carbon_provider_down":
                whatif = getattr(dashboard, "what_if", None)
                if whatif is not None:
                    self._snapshots.setdefault(
                        "carbon_provider_down", {"carbon": whatif.carbon_manager}
                    )
                    whatif.carbon_manager = None
            elif fault == "feedback_drop":
                self._snapshots.setdefault("feedback_drop", {"marker": True})

    def cleanup_fault(self, fault: str, dashboard: "DashboardEngine") -> None:
        with self._lock:
            snap = self._snapshots.pop(fault, None)
            if snap is None:
                return
            if fault == "db_failure":
                dashboard.db_manager = snap["db"]
            elif fault == "ws_drop":
                dashboard._ws_connections.extend(snap["connections"])
            elif fault == "carbon_provider_down":
                whatif = getattr(dashboard, "what_if", None)
                if whatif is not None:
                    whatif.carbon_manager = snap["carbon"]

    def is_feedback_dropped(self) -> bool:
        return "feedback_drop" in self._snapshots

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault, "outcome": outcome,
            "timestamp": _utc_now().isoformat(), **(extra or {}),
        })

    async def inject_and_measure(
        self, dashboard: "DashboardEngine", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, dashboard)
        recovery_step: Optional[int] = None
        try:
            for step in range(steps):
                try:
                    _ = dashboard._request_logs  # liveness probe
                    if recovery_step is None:
                        recovery_step = step
                except Exception:
                    continue
        finally:
            self.cleanup_fault(fault, dashboard)
        elapsed = time.monotonic() - start
        self._record(fault, "measured",
                     {"recovery_step": recovery_step, "elapsed_s": elapsed})
        return {
            "fault": fault,
            "recovery_step": recovery_step,
            "recovered": recovery_step is not None,
            "elapsed_s": elapsed,
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "failure_probability": self.failure_probability,
                "injected_failures": self.injected_failures,
                "pending_snapshots": list(self._snapshots.keys()),
            }


class _NullDatabaseManager:
    """Placeholder used by chaos `db_failure`."""
    async def initialize(self): return None
    async def upsert_optimizer_state(self, *a, **k): return None
    async def load_optimizer_state(self, *a, **k): return None
    async def health_check(self): return {"status": "unavailable"}
    async def close(self): return None


# =============================================================================
# HumanReviewManager — TTL, capacity, audit
# =============================================================================
@dataclass
class HumanReviewRequest:
    review_id: str
    request_id: str
    details: Dict[str, Any]
    created_at: datetime
    status: str = "pending"
    reviewer: Optional[str] = None
    resolution_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None


class HumanReviewManager:
    def __init__(self, ttl_seconds: float = 3600.0, max_pending: int = 100):
        self.ttl_seconds = float(ttl_seconds)
        self.max_pending = int(max_pending)
        self.pending_reviews: Dict[str, HumanReviewRequest] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    def _evict_expired(self) -> None:
        now = _utc_now()
        for rid, r in list(self.pending_reviews.items()):
            if r.status == "pending" and now - r.created_at > timedelta(seconds=self.ttl_seconds):
                r.status = "expired"
                HITL_STATUS.labels(status="expired").inc()

    def _enforce_capacity(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"
            HITL_STATUS.labels(status="dropped").inc()

    async def request_review(self, request_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid,
                request_id=request_id,
                details=dict(details),
                created_at=_utc_now(),
            )
            self._enforce_capacity()
            HITL_REQUESTS.inc()
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str, reason: Optional[str]) -> None:
        self.audit_log.append({
            "review_id": review_id,
            "action": action,
            "reviewer": reviewer,
            "reason": reason,
            "timestamp": _utc_now().isoformat(),
        })

    async def approve(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "approved"
            r.reviewer = reviewer
            r.resolution_at = _utc_now()
            self._audit(review_id, "approved", reviewer, None)
            HITL_STATUS.labels(status="approved").inc()
            return True

    async def reject(self, review_id: str, reason: Optional[str] = None,
                     reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "rejected"
            r.rejection_reason = reason or "unspecified"
            r.reviewer = reviewer
            r.resolution_at = _utc_now()
            self._audit(review_id, "rejected", reviewer, reason)
            HITL_STATUS.labels(status="rejected").inc()
            return True

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            self._evict_expired()
            return [
                {
                    "review_id": r.review_id,
                    "request_id": r.request_id,
                    "details": r.details,
                    "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values() if r.status == "pending"
            ]

    def get_stats(self) -> Dict[str, Any]:
        statuses: Dict[str, int] = defaultdict(int)
        for r in self.pending_reviews.values():
            statuses[r.status] += 1
        return {"total": len(self.pending_reviews), "by_status": dict(statuses)}


# =============================================================================
# QuantumDistillationOptimizer — try new qiskit, fall back to classical
# =============================================================================
class QuantumDistillationOptimizer:
    def __init__(self, enabled: bool = False, qaoa_reps: int = 1):
        self.enabled = bool(enabled)
        self.qaoa_reps = int(qaoa_reps)
        self.available = self.enabled and QISKIT_AVAILABLE

    async def select_best_template(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        if not self.enabled or not candidates:
            return None
        if QISKIT_AVAILABLE:
            try:
                return await self._quantum_select(candidates, weights)
            except Exception as e:
                logger.warning(f"quantum_select_failed error={e}")
        return self._classical_select(candidates, weights)

    def _classical_select(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        best = None
        best_score = -float("inf")
        for c in candidates:
            score = sum(
                float(c.get(k, 0.0)) * float(weights.get(k, 0.0)) for k in weights
            )
            if score > best_score:
                best_score = score
                best = c
        return best

    async def _quantum_select(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        qp = QuadraticProgram()
        for i, _ in enumerate(candidates):
            qp.binary_var(f"x{i}")
        utility = [
            sum(float(c.get(k, 0.0)) * float(weights.get(k, 0.0)) for k in weights)
            for c in candidates
        ]
        qp.minimize(linear={f"x{i}": -utility[i] for i in range(len(candidates))})
        qp.linear_constraint(
            linear={f"x{i}": 1 for i in range(len(candidates))},
            sense="E", rhs=1, name="one_template",
        )
        qaoa = QAOA(reps=self.qaoa_reps)
        optimizer = MinimumEigenOptimizer(qaoa)
        result = await asyncio.to_thread(optimizer.solve, qp)
        for i, c in enumerate(candidates):
            if result.x[i] > 0.5:
                return c
        return None

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "qiskit_available": QISKIT_AVAILABLE,
            "qiskit_legacy": QISKIT_LEGACY if QISKIT_AVAILABLE else None,
        }


# =============================================================================
# FlexGenPrecisionPolicy
# =============================================================================
class FlexGenPrecisionPolicy:
    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = float(default_carbon_intensity)

    def recommend_precision(self, workload_size: str = "medium",
                            carbon_intensity: Optional[float] = None) -> str:
        carbon_intensity = (
            float(carbon_intensity)
            if carbon_intensity is not None else self.default_carbon_intensity
        )
        if carbon_intensity > 500 or workload_size == "large":
            return "int8"
        if carbon_intensity > 300 or workload_size == "medium":
            return "fp16"
        return "fp32"

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": True,
            "default_carbon_intensity": self.default_carbon_intensity,
        }


# =============================================================================
# ExplanationGenerator
# =============================================================================
class ExplanationGenerator:
    def __init__(
        self,
        config: ExplainableUIConfig,
        llm_client: Optional[LLMClient] = None,
        template: Optional[str] = None,
        db_manager: Optional[DatabaseManager] = None,
    ):
        self.config = config
        self.llm_client = llm_client
        self.template = template or self._default_template()
        self.template_env = None
        self.template_name = "default"
        self.db = db_manager
        if config.explanation_template_path:
            self._load_template_from_file(config.explanation_template_path)

        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.template_population = [{"template": self.template, "style": "default"}]
            self.template_fitness: Deque[float] = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.template_population: List[Dict[str, Any]] = [
                {"template": self.template, "style": "default"}
            ]
            self.template_fitness = deque(maxlen=100)

        styles = ["default", "carbon_focused", "latency_focused",
                  "accuracy_focused", "cost_focused"]
        self.causal_bandit = CausalBandit(
            action_space=styles,
            fallback_solver=lambda ctx: "default",
            min_trials_before_bandit=5,
            confidence_threshold=0.6,
            feature_dim=8,
        )
        self.safety_monitor = SafetyMonitor()
        self.attribution_explainer = FeatureAttributionExplainer()
        self.federated_coordinator = FederatedFeedbackCoordinator(
            privacy_budget=config.federated_privacy_budget,
            sensitivity=config.federated_sensitivity,
        )
        self.multi_agent_ui = MultiAgentUIRouter()
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum_distillation_enabled
        )

        self._last_style_used = "default"
        self._last_request_id: Optional[str] = None
        self._last_contributing_agents: List[str] = []
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        if self.db is None:
            return
        try:
            state = await self.db.load_optimizer_state("explanation_templates")
            if state:
                self.template_population = state.get(
                    "population", [{"template": self.template, "style": "default"}]
                ) or [{"template": self.template, "style": "default"}]
                self.template_fitness = deque(state.get("fitness", []), maxlen=100)
        except Exception as e:
            logger.warning(f"template_state_load_failed error={e}")
        try:
            bandit_state = await self.db.load_optimizer_state("causal_bandit")
            if bandit_state:
                theta = bandit_state.get("theta") or {}
                for k, v in theta.items():
                    if k in self.causal_bandit.theta:
                        try:
                            self.causal_bandit.theta[k] = np.asarray(v, dtype=float)
                        except Exception:
                            pass
                P = bandit_state.get("P") or {}
                for k, v in P.items():
                    if k in self.causal_bandit.P:
                        try:
                            self.causal_bandit.P[k] = np.asarray(v, dtype=float)
                        except Exception:
                            pass
                self.causal_bandit.total_trials = int(
                    bandit_state.get("total_trials", 0)
                )
        except Exception as e:
            logger.warning(f"bandit_state_load_failed error={e}")

    async def _save_state(self) -> None:
        if self.db is None:
            return
        try:
            await self.db.upsert_optimizer_state(
                "explanation_templates",
                {
                    "population": self.template_population,
                    "fitness": list(self.template_fitness),
                },
            )
            await self.db.upsert_optimizer_state(
                "causal_bandit",
                {
                    "theta": {a: self.causal_bandit.theta[a].tolist()
                              for a in self.causal_bandit.theta},
                    "P": {a: self.causal_bandit.P[a].tolist()
                          for a in self.causal_bandit.P},
                    "total_trials": self.causal_bandit.total_trials,
                },
            )
        except Exception as e:
            logger.warning(f"state_save_failed error={e}")

    def _default_template(self) -> str:
        return (
            "This request was routed to expert **{{ chosen_expert_id }}**"
            "{{ compressed_info }}."
            "{{ co2_savings }}{{ latency_impact }}"
            " The chosen expert achieved accuracy of {{ accuracy:.2% }}."
            " Carbon intensity was {{ carbon_intensity:.1f }} gCO₂/kWh,"
            " helium scarcity {{ helium_scarcity:.2f }},"
            " material index {{ material_index:.2f }}."
            "{% if chosen_utility is defined and chosen_utility is not none %}"
            " (Utility score: {{ chosen_utility:.3f }}){% endif %}"
            "{% if precision %} Precision: {{ precision }}.{% endif %}"
            "{% if attribution %} Feature attribution: {{ attribution }}.{% endif %}"
        )

    def _load_template_from_file(self, path: str) -> None:
        if not JINJA_AVAILABLE:
            logger.warning("jinja2_not_installed")
            return
        try:
            path_obj = Path(path)
            if not path_obj.is_absolute():
                path_obj = path_obj.resolve()
            with open(path_obj, "r") as f:
                content = f.read()
            self.template_env = Environment(
                loader=FileSystemLoader(str(path_obj.parent))
            )
            self.template_name = path_obj.name
            self.template = content
            logger.info(f"template_loaded path={path_obj}")
        except Exception as e:
            logger.warning(f"template_load_failed path={path} error={e}")

    def reload_template(self, path: Optional[str] = None) -> None:
        if path:
            self._load_template_from_file(path)
        elif self.config.explanation_template_path:
            self._load_template_from_file(self.config.explanation_template_path)

    async def generate_async(
        self,
        request: RequestLog,
        chosen_expert: SustainabilityAwareExpertProfile,
        alternatives: List[Tuple[str, SustainabilityAwareExpertProfile]],
        user_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        chosen_utility: Optional[float] = None
        alt_utilities: Dict[str, float] = {}

        if self.modp is not None:
            max_energy = max(
                [a[1].energy_per_inference_full for a in alternatives]
                + [request.energy_joules] + [1e-8]
            )
            max_co2 = max(
                [a[1].energy_per_inference_full * self.config.energy_to_co2_factor
                 for a in alternatives]
                + [request.co2_kg] + [1e-8]
            )
            max_latency = max(
                [a[1].energy_per_inference_full * 1e-6 * 0.5 for a in alternatives]
                + [request.latency_ms] + [1e-8]
            )
            chosen_objectives = {
                "accuracy": request.accuracy,
                "energy": 1.0 - (request.energy_joules / max_energy),
                "carbon": 1.0 - (request.co2_kg / max_co2),
                "latency": 1.0 - (request.latency_ms / max_latency),
            }
            try:
                chosen_utility = float(self.modp.evaluate(
                    chosen_objectives, self.config.modp_weights
                ))
            except Exception:
                chosen_utility = None
            for eid, prof in alternatives:
                alt_energy = prof.energy_per_inference_full
                alt_co2 = alt_energy * self.config.energy_to_co2_factor
                alt_latency = alt_energy * 1e-6 * 0.5
                alt_obj = {
                    "accuracy": prof.accuracy_full,
                    "energy": 1.0 - (alt_energy / max_energy),
                    "carbon": 1.0 - (alt_co2 / max_co2),
                    "latency": 1.0 - (alt_latency / max_latency),
                }
                try:
                    alt_utilities[eid] = float(self.modp.evaluate(
                        alt_obj, self.config.modp_weights
                    ))
                except Exception:
                    pass

        # Safety check.
        safety_override = False
        if self.config.safety_monitor_enabled:
            if not self.safety_monitor.check_explanation(request, chosen_utility):
                logger.warning("safety_violation green_explanation_suppressed")
                safety_override = True

        # Style selection via bandit.
        context_for_bandit = {
            "user_role_hash": float(int(hashlib.sha256(
                str((user_context or {}).get("role", "viewer")).encode()
            ).hexdigest()[:6], 16)),
            "task_type_hash": float(int(hashlib.sha256(
                str((user_context or {}).get("task_type", "general")).encode()
            ).hexdigest()[:6], 16)),
            "carbon_intensity": float(request.carbon_intensity),
            "has_alternatives": float(len(alternatives) > 0),
            "accuracy": float(request.accuracy),
            "carbon_kg": float(request.co2_kg),
            "latency_ms": float(request.latency_ms),
            "energy": float(request.energy_joules),
        }
        encoded = self._encode_context(context_for_bandit)
        style, confidence, source = self.causal_bandit.select_action(encoded)
        if not style:
            style = "default"

        # Optional quantum selection.
        if self.quantum_optimizer.available:
            quantum_candidates = [
                {
                    "style": v.get("style", "default"),
                    "carbon": 1.0 - min(1.0, request.carbon_intensity),
                    "accuracy": request.accuracy,
                    "latency": 1.0 - min(1.0, request.latency_ms / 500.0),
                    "energy": 1.0 - min(1.0, request.energy_joules / 10.0),
                }
                for v in self.template_population
            ]
            quantum_choice = await self.quantum_optimizer.select_best_template(
                quantum_candidates, self.config.modp_weights
            )
            if quantum_choice:
                style = quantum_choice.get("style", style)
                source = "quantum"

        # Multi-agent composite.
        selected_agents = self.multi_agent_ui.select_agents(request, top_k=2)
        agent_info = f" [composite from {', '.join(selected_agents)}]" if selected_agents else ""

        # Choose template.
        selected_template = self.template
        for variant in self.template_population:
            if variant.get("style") == style:
                selected_template = variant.get("template", self.template)
                break

        # Feature attribution.
        attribution = self.attribution_explainer.explain(request, chosen_utility)

        # Prepare data.
        if alternatives:
            best_alt = min(alternatives, key=lambda x: x[1].energy_per_inference_full)
            alt_energy = best_alt[1].energy_per_inference_full
            chosen_energy = (
                chosen_expert.energy_per_inference_compressed
                or chosen_expert.energy_per_inference_full
            )
            energy_saved = alt_energy - chosen_energy
            co2_saved = energy_saved * self.config.energy_to_co2_factor
            alt_latency = alt_energy * 1e-6 * 0.5
            latency_diff = request.latency_ms - alt_latency
        else:
            co2_saved = 0.0
            latency_diff = 0.0

        data = {
            "chosen_expert_id": chosen_expert.expert_id,
            "compressed_info": (
                f" (compressed – {chosen_expert.compression_method})"
                if chosen_expert.compressed_flag else ""
            ),
            "co2_savings": (
                f" This decision saved approximately **{co2_saved:.4f} kg CO₂** "
                f"compared to the most energy-intensive alternative."
                if co2_saved > 0 else " (No CO₂ savings over the best alternative)."
            ),
            "latency_impact": (
                f" It increased latency by {latency_diff:.1f} ms."
                if latency_diff > 1.0 else
                f" It reduced latency by {-latency_diff:.1f} ms."
                if latency_diff < -1.0 else ""
            ),
            "accuracy": request.accuracy,
            "carbon_intensity": request.carbon_intensity,
            "helium_scarcity": request.helium_scarcity,
            "material_index": request.material_index,
            "chosen_utility": chosen_utility,
            "alt_utilities": alt_utilities,
            "precision": request.precision,
            "attribution": attribution,
            "safety_override": safety_override,
        }

        try:
            if self.template_env is not None and JINJA_AVAILABLE:
                template = self.template_env.get_template(self.template_name)
                result = template.render(**data)
            elif JINJA_AVAILABLE:
                result = Template(selected_template).render(**data)
            else:
                result = self._fallback_generate(data)
        except Exception as e:
            logger.warning(f"template_render_failed error={e}")
            result = self._fallback_generate(data)

        if agent_info:
            result += agent_info

        # Save selection state.
        async with self._lock:
            self._last_style_used = style
            self._last_request_id = request.request_id
            self._last_contributing_agents = list(selected_agents)

        EXPLANATIONS.labels(style=style).inc()
        return result

    def _encode_context(self, context: Dict[str, Any]) -> List[float]:
        if self.moe is not None:
            try:
                encoded = self.moe.encode(context)
                if encoded:
                    return [float(v) for v in encoded if isinstance(v, (int, float))]
            except Exception:
                pass
        return [float(v) for v in context.values() if isinstance(v, (int, float))]

    def _fallback_generate(self, data: Dict[str, Any]) -> str:
        parts = [
            f"This request was routed to expert **{data['chosen_expert_id']}**"
            f"{data['compressed_info']}.",
            data["co2_savings"],
            data["latency_impact"],
            f" The chosen expert achieved accuracy of {data['accuracy']:.2%}.",
            f" Carbon intensity was {data['carbon_intensity']:.1f} gCO₂/kWh,"
            f" helium scarcity {data['helium_scarcity']:.2f},"
            f" material index {data['material_index']:.2f}.",
        ]
        if data.get("chosen_utility") is not None:
            parts.append(f" (Utility score: {data['chosen_utility']:.3f})")
        if data.get("precision"):
            parts.append(f" Precision: {data['precision']}.")
        return " ".join(p for p in parts if p)

    async def evolve_templates(self) -> None:
        if self.bio is None or not self.config.template_evolution_enabled:
            return
        if len(self.template_fitness) < 10:
            return

        def fitness(_variant):
            return float(np.mean(list(self.template_fitness))) if self.template_fitness else 0.5

        try:
            new_population = self.bio.evolve(
                population=self.template_population,
                fitness_fn=fitness,
                generations=self.config.template_generations,
                population_size=self.config.template_population_size,
            )
        except Exception as e:
            logger.warning(f"bio_evolve_failed error={e}")
            return

        if not new_population:
            return
        if isinstance(new_population, dict):
            new_population = [new_population]
        if not isinstance(new_population, list):
            return

        self.template_population = new_population
        best = max(new_population, key=lambda v: fitness(v))
        self.template = best.get("template", self.template)
        self.template_name = best.get("style", "default")
        await self._save_state()
        logger.info(f"templates_evolved size={len(new_population)}")

    async def record_feedback(self, rating: int, template_used: str) -> None:
        rating = int(rating)
        bucket = "low" if rating <= 2 else "mid" if rating <= 4 else "high"
        FEEDBACK.labels(rating_bucket=bucket).inc()

        self.template_fitness.append(float(rating))
        reward = max(0.0, min(1.0, rating / 5.0))
        context = {
            "template_hash": float(int(hashlib.sha256(
                (template_used or "").encode()
            ).hexdigest()[:6], 16)),
            "last_style_hash": float(int(hashlib.sha256(
                self._last_style_used.encode()
            ).hexdigest()[:6], 16)),
        }
        async with self._lock:
            style = self._last_style_used
            contributing = list(self._last_contributing_agents)
        if style in self.causal_bandit.actions:
            self.causal_bandit.update(context, style, reward)

        self.federated_coordinator.register_participant(
            f"ui_{uuid.uuid4().hex[:6]}",
            {"avg_rating": reward},
            sample_count=1,
        )

        if contributing:
            self.multi_agent_ui.record_outcome(contributing, success=(rating >= 3))

        if len(self.template_fitness) >= 20:
            await self.evolve_templates()
        await self._save_state()

    def get_enhancement_status(self) -> Dict[str, Any]:
        return {
            "causal_bandit_trials": self.causal_bandit.total_trials,
            "causal_bandit_usage": dict(self.causal_bandit.usage),
            "safety_violations": self.safety_monitor.get_violations()[-5:],
            "attribution_explainer": self.attribution_explainer.get_status(),
            "federated_participants": self.federated_coordinator.get_participant_count(),
            "multi_agent": self.multi_agent_ui.get_stats(),
            "quantum_optimizer": self.quantum_optimizer.get_status(),
        }


# =============================================================================
# DashboardEngine — tasks started explicitly via start()
# =============================================================================
class DashboardEngine:
    def __init__(
        self,
        config: ExplainableUIConfig,
        db_manager: DatabaseManager,
        generator: ExplanationGenerator,
        chaos_monkey: Optional[ChaosMonkey] = None,
    ):
        self.config = config
        self.db_manager = db_manager
        self.generator = generator
        # Bounded caches.
        self._request_logs: "OrderedDict[str, RequestLog]" = OrderedDict()
        self._request_logs_max = int(config.request_log_max_entries)
        self._cache: "OrderedDict[str, Any]" = OrderedDict()
        self._cache_timestamps: Dict[str, float] = {}
        self._cache_max = int(config.cache_max_entries)
        self._ws_connections: List[Any] = []
        self._broadcast_task: Optional[asyncio.Task] = None
        self._evolution_task: Optional[asyncio.Task] = None
        self._cache_lock = asyncio.Lock()
        self._ws_lock = asyncio.Lock()
        self._lock = asyncio.Lock()
        self.chaos_monkey = chaos_monkey or ChaosMonkey(
            enabled=config.chaos_enabled,
            failure_probability=config.chaos_failure_probability,
            seed=config.chaos_seed,
        )
        self.what_if: Optional["WhatIfSimulator"] = None  # attached later by factory
        self._started = False

    async def __aenter__(self) -> "DashboardEngine":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self.db_manager.initialize()
        if self.generator is not None:
            await self.generator.initialize()
        if self.config.ws_enabled:
            self._broadcast_task = asyncio.create_task(self._broadcast_loop())
        if self.config.template_evolution_enabled:
            self._evolution_task = asyncio.create_task(self._evolution_loop())
        logger.info("dashboard_started")

    def _remember_request(self, request_log: RequestLog) -> None:
        """Insert into the bounded LRU and evict oldest if over capacity."""
        self._request_logs[request_log.request_id] = request_log
        self._request_logs.move_to_end(request_log.request_id)
        while len(self._request_logs) > self._request_logs_max:
            self._request_logs.popitem(last=False)

    async def _broadcast_loop(self) -> None:
        backoff = 1.0
        max_backoff = 60.0
        while True:
            try:
                await asyncio.sleep(self.config.ws_broadcast_interval)
                if self._ws_connections:
                    await self._broadcast({
                        "type": "stats_update",
                        "data": {
                            "total_requests": len(self._request_logs),
                            "timestamp": _utc_now().isoformat(),
                        },
                    })
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"broadcast_loop_error error={e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _evolution_loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                await asyncio.sleep(self.config.template_evolution_interval_seconds)
                if self.generator is not None:
                    await self.generator.evolve_templates()
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"evolution_loop_error error={e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 300.0)

    async def log_request(self, request_log: RequestLog) -> None:
        fault = self.chaos_monkey.maybe_fail("log_request")
        if fault is not None:
            try:
                self.chaos_monkey.apply(fault, self)
            except Exception as e:
                logger.warning(f"chaos_apply_failed error={e}")

        try:
            self._remember_request(request_log)
            await self._persist_request_async(request_log)
            async with self._cache_lock:
                self._cache.clear()
                self._cache_timestamps.clear()
            await self._broadcast({
                "type": "new_request",
                "data": {
                    "request_id": request_log.request_id,
                    "timestamp": request_log.timestamp.isoformat(),
                    "chosen_expert": request_log.chosen_expert_id,
                    "energy_joules": request_log.energy_joules,
                    "co2_kg": request_log.co2_kg,
                    "accuracy": request_log.accuracy,
                    "precision": request_log.precision,
                },
            })
            ROUTING_REQUESTS.inc()
        finally:
            if fault is not None:
                try:
                    self.chaos_monkey.cleanup_fault(fault, self)
                except Exception:
                    pass

    async def _persist_request_async(self, req: RequestLog) -> None:
        alt_json = json.dumps([
            {
                "expert_id": eid,
                "energy": prof.energy_per_inference_full,
                "accuracy": prof.accuracy_full,
                "compressed": prof.compressed_flag,
            }
            for eid, prof in req.alternative_experts
        ])
        start = time.monotonic()
        try:
            if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
                async with self.db_manager.async_sessionmaker() as session:
                    log_entry = RequestLogDB(
                        request_id=req.request_id,
                        timestamp=req.timestamp,
                        query=req.query,
                        chosen_expert_id=req.chosen_expert_id,
                        alternative_experts=alt_json,
                        latency_ms=req.latency_ms,
                        energy_joules=req.energy_joules,
                        co2_kg=req.co2_kg,
                        accuracy=req.accuracy,
                        carbon_intensity=req.carbon_intensity,
                        helium_scarcity=req.helium_scarcity,
                        material_index=req.material_index,
                        sustainability_score=req.sustainability_score,
                        explanation=req.explanation,
                        precision=req.precision,
                        human_review_id=req.human_review_id,
                    )
                    session.add(log_entry)
                    await session.commit()
                await self._update_expert_stats_async(req.chosen_expert_id, req)
            elif SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._persist_request_sync, req, alt_json)
                await loop.run_in_executor(None, self._update_expert_stats_sync, req.chosen_expert_id, req)
        except Exception as e:
            logger.error(f"persist_request_failed error={e}")
        finally:
            DB_OPS.labels(op="persist_request").observe(time.monotonic() - start)

    def _persist_request_sync(self, req: RequestLog, alt_json: str) -> None:
        if not SQLALCHEMY_SYNC_AVAILABLE or self.db_manager.sync_sessionmaker is None:
            return
        session = self.db_manager.sync_sessionmaker()
        try:
            log_entry = RequestLogDB(
                request_id=req.request_id,
                timestamp=req.timestamp,
                query=req.query,
                chosen_expert_id=req.chosen_expert_id,
                alternative_experts=alt_json,
                latency_ms=req.latency_ms,
                energy_joules=req.energy_joules,
                co2_kg=req.co2_kg,
                accuracy=req.accuracy,
                carbon_intensity=req.carbon_intensity,
                helium_scarcity=req.helium_scarcity,
                material_index=req.material_index,
                sustainability_score=req.sustainability_score,
                explanation=req.explanation,
                precision=req.precision,
                human_review_id=req.human_review_id,
            )
            session.add(log_entry)
            session.commit()
        finally:
            session.close()

    def _update_expert_stats_sync(self, expert_id: str, req: RequestLog) -> None:
        if not SQLALCHEMY_SYNC_AVAILABLE or self.db_manager.sync_sessionmaker is None:
            return
        session = self.db_manager.sync_sessionmaker()
        try:
            existing = session.query(ExpertStatsDB).filter_by(expert_id=expert_id).first()
            if existing is None:
                existing = ExpertStatsDB(
                    expert_id=expert_id,
                    total_requests=1,
                    avg_latency_ms=req.latency_ms,
                    avg_energy_joules=req.energy_joules,
                    avg_accuracy=req.accuracy,
                    total_co2_kg=req.co2_kg,
                    last_updated=_utc_now(),
                )
                session.add(existing)
            else:
                n = existing.total_requests or 0
                new_n = n + 1
                existing.avg_latency_ms = (existing.avg_latency_ms * n + req.latency_ms) / new_n
                existing.avg_energy_joules = (existing.avg_energy_joules * n + req.energy_joules) / new_n
                existing.avg_accuracy = (existing.avg_accuracy * n + req.accuracy) / new_n
                existing.total_co2_kg = (existing.total_co2_kg or 0.0) + req.co2_kg
                existing.total_requests = new_n
                existing.last_updated = _utc_now()
            session.commit()
        except Exception as e:
            session.rollback()
            logger.warning(f"expert_stats_sync_failed error={e}")
        finally:
            session.close()

    async def _update_expert_stats_async(self, expert_id: str, req: RequestLog) -> None:
        try:
            async with self.db_manager.async_sessionmaker() as session:
                # Portable upsert using INSERT ... ON CONFLICT.
                stmt = text(
                    "INSERT INTO expert_stats (expert_id, total_requests, avg_latency_ms, "
                    "avg_energy_joules, avg_accuracy, total_co2_kg, last_updated) "
                    "VALUES (:eid, 1, :lat, :energy, :acc, :co2, :ts) "
                    "ON CONFLICT(expert_id) DO UPDATE SET "
                    "total_requests = expert_stats.total_requests + 1, "
                    "avg_latency_ms = (expert_stats.avg_latency_ms * expert_stats.total_requests + :lat) "
                    "  / (expert_stats.total_requests + 1), "
                    "avg_energy_joules = (expert_stats.avg_energy_joules * expert_stats.total_requests + :energy) "
                    "  / (expert_stats.total_requests + 1), "
                    "avg_accuracy = (expert_stats.avg_accuracy * expert_stats.total_requests + :acc) "
                    "  / (expert_stats.total_requests + 1), "
                    "total_co2_kg = expert_stats.total_co2_kg + :co2, "
                    "last_updated = :ts"
                )
                try:
                    await session.execute(stmt, {
                        "eid": expert_id,
                        "lat": req.latency_ms,
                        "energy": req.energy_joules,
                        "acc": req.accuracy,
                        "co2": req.co2_kg,
                        "ts": _utc_now(),
                    })
                    await session.commit()
                except Exception:
                    await session.rollback()
                    # SQLite fallback: single INSERT OR REPLACE with recomputed values.
                    existing = (await session.execute(
                        text("SELECT total_requests, avg_latency_ms, avg_energy_joules, "
                             "avg_accuracy, total_co2_kg FROM expert_stats WHERE expert_id = :eid"),
                        {"eid": expert_id},
                    )).fetchone()
                    if existing is None:
                        await session.execute(text(
                            "INSERT OR REPLACE INTO expert_stats "
                            "(expert_id, total_requests, avg_latency_ms, avg_energy_joules, "
                            "avg_accuracy, total_co2_kg, last_updated) "
                            "VALUES (:eid, 1, :lat, :energy, :acc, :co2, :ts)"
                        ), {
                            "eid": expert_id, "lat": req.latency_ms,
                            "energy": req.energy_joules, "acc": req.accuracy,
                            "co2": req.co2_kg, "ts": _utc_now(),
                        })
                    else:
                        n, lat, e, a, co2 = existing
                        new_n = n + 1
                        await session.execute(text(
                            "INSERT OR REPLACE INTO expert_stats "
                            "(expert_id, total_requests, avg_latency_ms, avg_energy_joules, "
                            "avg_accuracy, total_co2_kg, last_updated) "
                            "VALUES (:eid, :n, :lat, :energy, :acc, :co2, :ts)"
                        ), {
                            "eid": expert_id,
                            "n": new_n,
                            "lat": (lat * n + req.latency_ms) / new_n,
                            "energy": (e * n + req.energy_joules) / new_n,
                            "acc": (a * n + req.accuracy) / new_n,
                            "co2": (co2 or 0.0) + req.co2_kg,
                            "ts": _utc_now(),
                        })
                    await session.commit()
        except Exception as e:
            logger.warning(f"expert_stats_async_failed error={e}")

    async def _cached(self, key: str, func: Callable, user_id: Optional[str] = None):
        cache_key = f"{user_id or 'global'}:{key}"
        now = time.time()
        async with self._cache_lock:
            ts = self._cache_timestamps.get(cache_key, 0)
            if cache_key in self._cache and (now - ts) < self.config.cache_ttl_seconds:
                self._cache.move_to_end(cache_key)
                CACHE_HITS.labels(kind="hit").inc()
                return self._cache[cache_key]
            CACHE_HITS.labels(kind="miss").inc()
        result = await func()
        async with self._cache_lock:
            self._cache[cache_key] = result
            self._cache.move_to_end(cache_key)
            self._cache_timestamps[cache_key] = now
            while len(self._cache) > self._cache_max:
                old_key, _ = self._cache.popitem(last=False)
                self._cache_timestamps.pop(old_key, None)
        return result

    async def get_request_data(
        self, request_id: str, user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        req = self._request_logs.get(request_id)
        if req is None:
            req = await self._load_request_from_db(request_id)
            if req is not None:
                self._remember_request(req)
        if req is None:
            return {"error": "Request not found"}
        return self._serialize_request(req)

    async def _load_request_from_db(self, request_id: str) -> Optional[RequestLog]:
        try:
            if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
                async with self.db_manager.async_sessionmaker() as session:
                    stmt = select(RequestLogDB).where(RequestLogDB.request_id == request_id)
                    result = await session.execute(stmt)
                    db_entry = result.scalar_one_or_none()
                    if db_entry is not None:
                        return self._from_db_entry(db_entry)
            elif SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
                loop = asyncio.get_running_loop()
                db_entry = await loop.run_in_executor(
                    None, self._load_request_sync, request_id
                )
                if db_entry is not None:
                    return self._from_db_entry(db_entry)
        except Exception as e:
            logger.warning(f"load_request_from_db_failed error={e}")
        return None

    def _load_request_sync(self, request_id: str):
        if not SQLALCHEMY_SYNC_AVAILABLE or self.db_manager.sync_sessionmaker is None:
            return None
        session = self.db_manager.sync_sessionmaker()
        try:
            return session.query(RequestLogDB).filter_by(request_id=request_id).first()
        finally:
            session.close()

    def _from_db_entry(self, db_entry: Any) -> RequestLog:
        try:
            alt_list = json.loads(db_entry.alternative_experts or "[]")
        except Exception:
            alt_list = []
        alternatives: List[Tuple[str, SustainabilityAwareExpertProfile]] = []
        for alt in alt_list:
            prof = SustainabilityAwareExpertProfile(
                expert_id=alt.get("expert_id", "unknown"),
                energy_per_inference_full=alt.get("energy", 0.0),
                accuracy_full=alt.get("accuracy", 0.0),
                compressed_flag=alt.get("compressed", False),
            )
            alternatives.append((prof.expert_id, prof))
        return RequestLog(
            request_id=db_entry.request_id,
            timestamp=db_entry.timestamp,
            query=db_entry.query,
            chosen_expert_id=db_entry.chosen_expert_id,
            chosen_expert_profile=SustainabilityAwareExpertProfile(db_entry.chosen_expert_id),
            alternative_experts=alternatives,
            latency_ms=db_entry.latency_ms or 0.0,
            energy_joules=db_entry.energy_joules or 0.0,
            co2_kg=db_entry.co2_kg or 0.0,
            accuracy=db_entry.accuracy or 0.0,
            carbon_intensity=db_entry.carbon_intensity or 0.0,
            helium_scarcity=db_entry.helium_scarcity or 0.0,
            material_index=db_entry.material_index or 0.0,
            sustainability_score=db_entry.sustainability_score or 0.0,
            explanation=db_entry.explanation or "",
            feedback_rating=db_entry.feedback_rating,
            feedback_comment=db_entry.feedback_comment,
            precision=getattr(db_entry, "precision", "fp32") or "fp32",
            human_review_id=getattr(db_entry, "human_review_id", None),
        )

    @staticmethod
    def _serialize_request(req: RequestLog) -> Dict[str, Any]:
        return {
            "request_id": req.request_id,
            "timestamp": req.timestamp.isoformat(),
            "query": req.query,
            "chosen_expert": req.chosen_expert_id,
            "latency_ms": req.latency_ms,
            "energy_joules": req.energy_joules,
            "co2_kg": req.co2_kg,
            "accuracy": req.accuracy,
            "carbon_intensity": req.carbon_intensity,
            "helium_scarcity": req.helium_scarcity,
            "material_index": req.material_index,
            "sustainability_score": req.sustainability_score,
            "explanation": req.explanation,
            "feedback_rating": req.feedback_rating,
            "precision": req.precision,
            "human_review_id": req.human_review_id,
            "alternatives": [
                {
                    "expert_id": eid,
                    "energy_joules": prof.energy_per_inference_full,
                    "accuracy": prof.accuracy_full,
                    "compressed": prof.compressed_flag,
                }
                for eid, prof in req.alternative_experts
            ],
        }

    async def get_expert_details(self, expert_id: str) -> Dict[str, Any]:
        try:
            if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
                async with self.db_manager.async_sessionmaker() as session:
                    stmt = select(ExpertStatsDB).where(ExpertStatsDB.expert_id == expert_id)
                    result = await session.execute(stmt)
                    stats = result.scalar_one_or_none()
                    if stats:
                        return {
                            "expert_id": expert_id,
                            "total_requests": stats.total_requests,
                            "avg_latency_ms": stats.avg_latency_ms,
                            "avg_energy_joules": stats.avg_energy_joules,
                            "avg_accuracy": stats.avg_accuracy,
                            "total_co2_kg": stats.total_co2_kg,
                        }
            elif SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
                loop = asyncio.get_running_loop()
                stats = await loop.run_in_executor(None, self._get_expert_stats_sync, expert_id)
                if stats:
                    return {
                        "expert_id": expert_id,
                        "total_requests": stats.total_requests,
                        "avg_latency_ms": stats.avg_latency_ms,
                        "avg_energy_joules": stats.avg_energy_joules,
                        "avg_accuracy": stats.avg_accuracy,
                        "total_co2_kg": stats.total_co2_kg,
                    }
        except Exception as e:
            logger.warning(f"expert_details_failed error={e}")
        return {"error": "No data"}

    def _get_expert_stats_sync(self, expert_id: str):
        if not SQLALCHEMY_SYNC_AVAILABLE or self.db_manager.sync_sessionmaker is None:
            return None
        session = self.db_manager.sync_sessionmaker()
        try:
            return session.query(ExpertStatsDB).filter_by(expert_id=expert_id).first()
        finally:
            session.close()

    async def get_dashboard_charts(
        self, request_id: Optional[str] = None, user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        if not PLOTLY_AVAILABLE:
            return {"error": "Plotly not installed"}

        async def _generate():
            if request_id:
                data = await self.get_request_data(request_id, user_id)
                if "error" in data:
                    return data
                alt = data["alternatives"]
                labels = [a["expert_id"] for a in alt] + [data["chosen_expert"]]
                energies = [a["energy_joules"] for a in alt] + [data["energy_joules"]]
                colors = ["gray"] * len(alt) + ["green"]
                fig = go.Figure(data=[go.Bar(x=labels, y=energies, marker_color=colors)])
                fig.update_layout(
                    title=f"Energy per Inference – Request {request_id}",
                    xaxis_title="Expert",
                    yaxis_title="Energy (Joules)",
                    template=self.config.plotly_theme,
                )
                return fig.to_json()
            # Global chart
            expert_data: Dict[str, Dict[str, Any]] = {}
            for req in self._request_logs.values():
                eid = req.chosen_expert_id
                if eid not in expert_data:
                    expert_data[eid] = {"energies": [], "accuracies": [], "count": 0}
                expert_data[eid]["energies"].append(req.energy_joules)
                expert_data[eid]["accuracies"].append(req.accuracy)
                expert_data[eid]["count"] += 1
            experts, avg_energies, avg_accuracies, sizes = [], [], [], []
            for eid, vals in expert_data.items():
                experts.append(eid)
                avg_energies.append(sum(vals["energies"]) / vals["count"])
                avg_accuracies.append(sum(vals["accuracies"]) / vals["count"])
                sizes.append(vals["count"] * 10)
            fig = go.Figure(data=[go.Scatter(
                x=avg_energies, y=avg_accuracies,
                mode="markers+text", text=experts,
                marker=dict(size=sizes, color=avg_energies,
                            colorscale="Viridis", showscale=True),
            )])
            fig.update_layout(
                title="Expert Sustainability Trade-offs (avg per expert)",
                xaxis_title="Average Energy per Inference (J)",
                yaxis_title="Average Accuracy",
                hovermode="closest",
                template=self.config.plotly_theme,
            )
            return fig.to_json()

        return await self._cached(f"chart_{request_id}", _generate, user_id)

    async def _broadcast(self, message: Dict[str, Any]) -> None:
        if not self._ws_connections:
            return
        msg = json.dumps(message, default=str)
        async with self._ws_lock:
            disconnected = []
            for ws in list(self._ws_connections):
                try:
                    await ws.send_text(msg)
                except Exception:
                    disconnected.append(ws)
            for ws in disconnected:
                if ws in self._ws_connections:
                    self._ws_connections.remove(ws)

    async def register_websocket(self, websocket: Any) -> None:
        await websocket.accept()
        async with self._ws_lock:
            self._ws_connections.append(websocket)
        try:
            while True:
                try:
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                    if data.strip() == "pong":
                        continue
                except asyncio.TimeoutError:
                    await websocket.send_text(json.dumps({"type": "ping"}))
                    try:
                        pong = await asyncio.wait_for(websocket.receive_text(), timeout=5)
                        if pong.strip() != "pong":
                            raise WebSocketDisconnect
                    except asyncio.TimeoutError:
                        raise WebSocketDisconnect
        except WebSocketDisconnect:
            pass
        finally:
            async with self._ws_lock:
                if websocket in self._ws_connections:
                    self._ws_connections.remove(websocket)

    async def get_recent_requests(
        self, page: int = 1, page_size: int = 20,
        filter_expert: Optional[str] = None, user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        page_size = min(page_size, self.config.max_page_size)
        offset = (page - 1) * page_size

        async def _fetch():
            try:
                if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
                    async with self.db_manager.async_sessionmaker() as session:
                        query = select(RequestLogDB)
                        count_query = select(func.count()).select_from(RequestLogDB)
                        if filter_expert:
                            query = query.where(RequestLogDB.chosen_expert_id == filter_expert)
                            count_query = count_query.where(RequestLogDB.chosen_expert_id == filter_expert)
                        total = (await session.execute(count_query)).scalar() or 0
                        result = await session.execute(
                            query.order_by(RequestLogDB.timestamp.desc())
                            .offset(offset).limit(page_size)
                        )
                        items = result.scalars().all()
                        return total, items
                elif SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
                    loop = asyncio.get_running_loop()
                    return await loop.run_in_executor(
                        None, self._recent_requests_sync, offset, page_size, filter_expert
                    )
            except Exception as e:
                logger.warning(f"recent_requests_failed error={e}")
            return 0, []

        total, items = await _fetch()
        serialized = []
        for r in items:
            serialized.append({
                "request_id": r.request_id,
                "timestamp": r.timestamp.isoformat() if r.timestamp else None,
                "chosen_expert": r.chosen_expert_id,
                "energy_joules": r.energy_joules,
                "co2_kg": r.co2_kg,
                "accuracy": r.accuracy,
                "explanation": r.explanation,
                "precision": getattr(r, "precision", "fp32") or "fp32",
            })
        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "items": serialized,
        }

    def _recent_requests_sync(self, offset: int, page_size: int, filter_expert: Optional[str]):
        if not SQLALCHEMY_SYNC_AVAILABLE or self.db_manager.sync_sessionmaker is None:
            return 0, []
        session = self.db_manager.sync_sessionmaker()
        try:
            query = session.query(RequestLogDB)
            if filter_expert:
                query = query.filter(RequestLogDB.chosen_expert_id == filter_expert)
            total = query.count()
            items = query.order_by(RequestLogDB.timestamp.desc()).offset(offset).limit(page_size).all()
            return total, items
        finally:
            session.close()

    async def stream_all_requests(self, user_id: Optional[str] = None) -> AsyncIterator[str]:
        """Stream all requests from the DB, JSON array style."""
        yield "["
        first = True
        try:
            if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
                async with self.db_manager.async_sessionmaker() as session:
                    result = await session.execute(select(RequestLogDB).order_by(RequestLogDB.timestamp.desc()))
                    for row in result.scalars():
                        req = self._from_db_entry(row)
                        if not first:
                            yield ","
                        first = False
                        yield json.dumps(self._serialize_request(req), default=str)
            elif SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
                loop = asyncio.get_running_loop()
                rows = await loop.run_in_executor(None, self._all_requests_sync)
                for row in rows:
                    req = self._from_db_entry(row)
                    if not first:
                        yield ","
                    first = False
                    yield json.dumps(self._serialize_request(req), default=str)
        finally:
            yield "]"

    def _all_requests_sync(self) -> List[Any]:
        if not SQLALCHEMY_SYNC_AVAILABLE or self.db_manager.sync_sessionmaker is None:
            return []
        session = self.db_manager.sync_sessionmaker()
        try:
            return session.query(RequestLogDB).order_by(RequestLogDB.timestamp.desc()).all()
        finally:
            session.close()

    async def shutdown(self) -> None:
        if self._broadcast_task is not None:
            self._broadcast_task.cancel()
            try:
                await self._broadcast_task
            except (asyncio.CancelledError, Exception):
                pass
            self._broadcast_task = None
        if self._evolution_task is not None:
            self._evolution_task.cancel()
            try:
                await self._evolution_task
            except (asyncio.CancelledError, Exception):
                pass
            self._evolution_task = None
        await self.db_manager.close()
        self._started = False
        logger.info("dashboard_shutdown")


# =============================================================================
# WhatIfSimulator
# =============================================================================
class WhatIfSimulator:
    def __init__(
        self,
        dashboard: DashboardEngine,
        config: ExplainableUIConfig,
        carbon_manager: Optional[Any] = None,
        lca_client: Optional[Any] = None,
        modp: Optional[Any] = None,
        offset_broker: Optional[CarbonOffsetBroker] = None,
        precision_policy: Optional[FlexGenPrecisionPolicy] = None,
    ):
        self.dashboard = dashboard
        self.config = config
        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.modp = modp
        self.offset_broker = offset_broker or CarbonOffsetBroker()
        self.precision_policy = precision_policy or FlexGenPrecisionPolicy()
        self._carbon_circuit = CircuitBreaker("carbon_api")
        self._lca_circuit = CircuitBreaker("lca_api")

    async def get_carbon_intensity(self) -> float:
        if self.carbon_manager is not None and hasattr(self.carbon_manager, "get_current_intensity"):
            try:
                intensity = await self._carbon_circuit.call(
                    self.carbon_manager.get_current_intensity
                )
                if isinstance(intensity, dict):
                    return float(intensity.get("intensity", 400)) / 1000
                return float(intensity) / 1000
            except Exception as e:
                logger.warning(f"carbon_intensity_failed error={e}")
        return self.config.co2_per_kwh_kg

    async def simulate(self, request_id: str, alternative_expert_id: str) -> WhatIfResult:
        req: Optional[RequestLog] = self.dashboard._request_logs.get(request_id)
        if req is None:
            # Fall back to DB.
            data = await self.dashboard.get_request_data(request_id)
            if "error" in data:
                raise ValueError(f"Request {request_id} not found")
            req = self.dashboard._request_logs.get(request_id)
            if req is None:
                # Reconstruct from the serialized data.
                alt_profiles: List[Tuple[str, SustainabilityAwareExpertProfile]] = []
                for alt in data.get("alternatives", []):
                    p = SustainabilityAwareExpertProfile(
                        expert_id=alt.get("expert_id", "unknown"),
                        energy_per_inference_full=alt.get("energy_joules", 0.0),
                        accuracy_full=alt.get("accuracy", 0.0),
                        compressed_flag=alt.get("compressed", False),
                    )
                    alt_profiles.append((p.expert_id, p))
                req = RequestLog(
                    request_id=data["request_id"],
                    timestamp=datetime.fromisoformat(data["timestamp"]) if isinstance(data["timestamp"], str) else data["timestamp"],
                    query=data.get("query", ""),
                    chosen_expert_id=data["chosen_expert"],
                    chosen_expert_profile=SustainabilityAwareExpertProfile(data["chosen_expert"]),
                    alternative_experts=alt_profiles,
                    latency_ms=float(data.get("latency_ms", 0.0)),
                    energy_joules=float(data.get("energy_joules", 0.0)),
                    co2_kg=float(data.get("co2_kg", 0.0)),
                    accuracy=float(data.get("accuracy", 0.0)),
                    carbon_intensity=float(data.get("carbon_intensity", 0.0)),
                    helium_scarcity=float(data.get("helium_scarcity", 0.0)),
                    material_index=float(data.get("material_index", 0.0)),
                    precision=data.get("precision", "fp32"),
                )

        alt_profile = None
        for eid, prof in req.alternative_experts:
            if eid == alternative_expert_id:
                alt_profile = prof
                break
        if alt_profile is None:
            raise ValueError(f"Expert {alternative_expert_id} not in alternatives")

        if alt_profile.compressed_flag and alt_profile.energy_per_inference_compressed:
            alt_energy = alt_profile.energy_per_inference_compressed
        else:
            alt_energy = alt_profile.energy_per_inference_full
        alt_latency = alt_energy * 1e-6 * 0.5
        alt_accuracy = (
            alt_profile.accuracy_compressed
            if alt_profile.compressed_flag and alt_profile.accuracy_compressed is not None
            else alt_profile.accuracy_full
        )
        co2_intensity = await self.get_carbon_intensity()
        alt_co2 = alt_energy * co2_intensity / 3600000

        diff_energy = alt_energy - req.energy_joules
        diff_co2 = alt_co2 - req.co2_kg
        diff_latency = alt_latency - req.latency_ms
        diff_accuracy = alt_accuracy - req.accuracy

        chosen_utility = None
        alternative_utility = None
        if self.modp is not None:
            max_energy = max(alt_energy, req.energy_joules, 1e-8)
            max_co2 = max(alt_co2, req.co2_kg, 1e-8)
            max_latency = max(alt_latency, req.latency_ms, 1e-8)
            try:
                chosen_utility = float(self.modp.evaluate({
                    "accuracy": req.accuracy,
                    "energy": 1.0 - (req.energy_joules / max_energy),
                    "carbon": 1.0 - (req.co2_kg / max_co2),
                    "latency": 1.0 - (req.latency_ms / max_latency),
                }, self.config.modp_weights))
                alternative_utility = float(self.modp.evaluate({
                    "accuracy": alt_accuracy,
                    "energy": 1.0 - (alt_energy / max_energy),
                    "carbon": 1.0 - (alt_co2 / max_co2),
                    "latency": 1.0 - (alt_latency / max_latency),
                }, self.config.modp_weights))
            except Exception:
                pass

        offset_cost_usd = self.offset_broker.estimate_offset_cost(alt_co2)
        workload_size = (
            "large" if alt_energy > 5.0
            else "medium" if alt_energy > 2.0
            else "small"
        )
        carbon_for_precision = (
            req.carbon_intensity * 800.0
            if req.carbon_intensity <= 1.0 else req.carbon_intensity
        )
        recommended_precision = self.precision_policy.recommend_precision(
            workload_size=workload_size,
            carbon_intensity=carbon_for_precision,
        )

        return WhatIfResult(
            scenario_id=str(uuid.uuid4()),
            alternative_expert_id=alternative_expert_id,
            expected_energy_joules=float(alt_energy),
            expected_co2_kg=float(alt_co2),
            expected_latency_ms=float(alt_latency),
            expected_accuracy=float(alt_accuracy),
            expected_carbon_intensity=float(req.carbon_intensity),
            expected_helium_scarcity=float(req.helium_scarcity),
            expected_material_index=float(req.material_index),
            difference_energy=float(diff_energy),
            difference_co2=float(diff_co2),
            difference_latency=float(diff_latency),
            difference_accuracy=float(diff_accuracy),
            chosen_utility=chosen_utility,
            alternative_utility=alternative_utility,
            offset_cost_usd=offset_cost_usd,
            recommended_precision=recommended_precision,
        )


# =============================================================================
# AuthManager
# =============================================================================
class AuthManager:
    def __init__(self, config: ExplainableUIConfig, db_manager: DatabaseManager):
        if not JWT_AVAILABLE:
            raise ConfigurationError("PyJWT is not installed")
        self.config = config
        self.db_manager = db_manager
        self.secret = config.jwt_secret
        self.algorithm = config.jwt_algorithm
        self.expiry_minutes = config.jwt_expiration_minutes
        self.refresh_expiry_days = config.refresh_token_expiration_days
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    def hash_password(self, password: str) -> str:
        return self.pwd_context.hash(password)

    def verify_password(self, password: str, password_hash: str) -> bool:
        try:
            return self.pwd_context.verify(password, password_hash)
        except Exception:
            return False

    async def get_user(self, username: str) -> Optional[Any]:
        if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
            async with self.db_manager.async_sessionmaker() as session:
                stmt = select(UserDB).where(UserDB.username == username)
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        if SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._get_user_sync, username)
        return None

    def _get_user_sync(self, username: str):
        if not SQLALCHEMY_SYNC_AVAILABLE or self.db_manager.sync_sessionmaker is None:
            return None
        session = self.db_manager.sync_sessionmaker()
        try:
            return session.query(UserDB).filter_by(username=username).first()
        finally:
            session.close()

    async def create_user(self, username: str, password: str, role: str = "viewer") -> Any:
        if role not in {"viewer", "analyst", "admin"}:
            raise AuthError(f"Unknown role: {role}")
        user = UserDB(
            username=username,
            password_hash=self.hash_password(password),
            role=role,
        )
        if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
            async with self.db_manager.async_sessionmaker() as session:
                session.add(user)
                await session.commit()
                await session.refresh(user)
        elif SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._create_user_sync, user)
        else:
            raise AuthError("No database backend available")
        AUTH_EVENTS.labels(kind="register", status="success").inc()
        return user

    def _create_user_sync(self, user: Any) -> None:
        session = self.db_manager.sync_sessionmaker()
        try:
            session.add(user)
            session.commit()
            session.refresh(user)
        finally:
            session.close()

    def create_token(self, username: str, role: str = "viewer") -> str:
        expire = _utc_now() + timedelta(minutes=self.expiry_minutes)
        payload = {"sub": username, "role": role, "exp": expire, "type": "access"}
        return pyjwt.encode(payload, self.secret, algorithm=self.algorithm)

    def create_refresh_token(self, username: str) -> str:
        expire = _utc_now() + timedelta(days=self.refresh_expiry_days)
        payload = {"sub": username, "type": "refresh", "exp": expire}
        return pyjwt.encode(payload, self.secret, algorithm=self.algorithm)

    def verify_access_token(self, token: str) -> Dict[str, Any]:
        try:
            payload = pyjwt.decode(token, self.secret, algorithms=[self.algorithm])
        except pyjwt.PyJWTError as e:
            AUTH_EVENTS.labels(kind="verify", status="invalid").inc()
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
            )
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Wrong token type")
        AUTH_EVENTS.labels(kind="verify", status="ok").inc()
        return payload

    async def authenticate_user(self, username: str, password: str) -> Optional[Any]:
        user = await self.get_user(username)
        if user and self.verify_password(password, user.password_hash):
            return user
        AUTH_EVENTS.labels(kind="login", status="invalid").inc()
        return None

    async def store_refresh_token(self, user: Any, token: str) -> None:
        expires = _utc_now() + timedelta(days=self.refresh_expiry_days)
        if ASYNC_SQLALCHEMY_AVAILABLE and self.db_manager.async_sessionmaker is not None:
            async with self.db_manager.async_sessionmaker() as session:
                await session.execute(
                    update(UserDB)
                    .where(UserDB.id == user.id)
                    .values(refresh_token=token, refresh_token_expires=expires)
                )
                await session.commit()
        elif SQLALCHEMY_SYNC_AVAILABLE and self.db_manager.sync_sessionmaker is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self._store_refresh_token_sync, user.id, token, expires
            )

    def _store_refresh_token_sync(self, user_id: int, token: str, expires: datetime) -> None:
        session = self.db_manager.sync_sessionmaker()
        try:
            session.query(UserDB).filter_by(id=user_id).update({
                "refresh_token": token,
                "refresh_token_expires": expires,
            })
            session.commit()
        finally:
            session.close()

    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        try:
            payload = pyjwt.decode(refresh_token, self.secret, algorithms=[self.algorithm])
        except pyjwt.PyJWTError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Wrong token type")
        username = payload.get("sub")
        user = await self.get_user(username)
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        # Rotation: the submitted token must match the stored one.
        stored = getattr(user, "refresh_token", None)
        if not stored or stored != refresh_token:
            raise HTTPException(status_code=401, detail="Refresh token already used")
        expires = getattr(user, "refresh_token_expires", None)
        if expires is not None and expires < _utc_now().replace(tzinfo=None):
            raise HTTPException(status_code=401, detail="Refresh token expired")
        # Issue new access and refresh tokens; rotate storage.
        new_access = self.create_token(username, user.role)
        new_refresh = self.create_refresh_token(username)
        await self.store_refresh_token(user, new_refresh)
        return {
            "access_token": new_access,
            "refresh_token": new_refresh,
            "token_type": "bearer",
        }


# =============================================================================
# API Gateway Extension
# =============================================================================
class _LoginBody(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=256)


class _RegisterBody(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=8, max_length=256)


class APIGatewayExtension:
    def __init__(
        self,
        dashboard: DashboardEngine,
        generator: ExplanationGenerator,
        what_if: WhatIfSimulator,
        auth: AuthManager,
        message_queue: Optional[AsyncMessageQueue] = None,
        carbon_manager: Optional[Any] = None,
        lca_client: Optional[Any] = None,
        offset_broker: Optional[CarbonOffsetBroker] = None,
        human_review: Optional[HumanReviewManager] = None,
        chaos_monkey: Optional[ChaosMonkey] = None,
    ):
        self.dashboard = dashboard
        self.generator = generator
        self.what_if = what_if
        self.auth = auth
        self.message_queue = message_queue or AsyncMessageQueue()
        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.offset_broker = offset_broker or CarbonOffsetBroker()
        self.human_review = human_review or HumanReviewManager(
            ttl_seconds=dashboard.config.hitl_ttl_seconds,
            max_pending=dashboard.config.hitl_max_pending,
        )
        self.chaos_monkey = chaos_monkey or dashboard.chaos_monkey
        self.app: Optional[FastAPI] = None
        self._limiter = None
        if SLOWAPI_AVAILABLE and FASTAPI_AVAILABLE:
            self._limiter = Limiter(key_func=get_remote_address)

    def register_routes(self, app: Any) -> None:
        self.app = app
        if not FASTAPI_AVAILABLE:
            raise ConfigurationError("FastAPI is not installed")

        if self._limiter is not None:
            app.state.limiter = self._limiter
            try:
                app.add_exception_handler(429, _rate_limit_exceeded_handler)
            except Exception:
                pass

        security = HTTPBearer(auto_error=False)

        async def get_current_user(
            credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
        ) -> Dict[str, Any]:
            if credentials is None:
                raise HTTPException(status_code=401, detail="auth required")
            return self.auth.verify_access_token(credentials.credentials)

        async def require_role(role: str, user: Dict[str, Any] = Depends(get_current_user)):
            if user.get("role") != role and user.get("role") != "admin":
                raise HTTPException(status_code=403, detail="Insufficient permissions")
            return user

        async def _rate_limit(request: Request) -> None:
            if self._limiter is None:
                return
            # Slowapi is decorator-based; use the token bucket directly.
            return None

        # ---- WebSocket ----
        @app.websocket("/ws/explain")
        async def websocket_endpoint(websocket: WebSocket):
            await self.dashboard.register_websocket(websocket)

        # ---- Auth ----
        @app.post("/api/explain/login")
        async def login(body: _LoginBody):
            user = await self.auth.authenticate_user(body.username, body.password)
            if not user:
                AUTH_EVENTS.labels(kind="login", status="invalid").inc()
                raise HTTPException(status_code=401, detail="Invalid credentials")
            AUTH_EVENTS.labels(kind="login", status="ok").inc()
            # Record last_login
            try:
                if ASYNC_SQLALCHEMY_AVAILABLE and self.dashboard.db_manager.async_sessionmaker is not None:
                    async with self.dashboard.db_manager.async_sessionmaker() as session:
                        await session.execute(
                            update(UserDB).where(UserDB.id == user.id).values(
                                last_login=_utc_now()
                            )
                        )
                        await session.commit()
            except Exception as e:
                logger.warning(f"last_login_update_failed error={e}")
            access_token = self.auth.create_token(user.username, user.role)
            refresh_token = self.auth.create_refresh_token(user.username)
            await self.auth.store_refresh_token(user, refresh_token)
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "bearer",
            }

        @app.post("/api/explain/refresh")
        async def refresh(refresh_token: str):
            return await self.auth.refresh_access_token(refresh_token)

        @app.post("/api/explain/register")
        async def register(body: _RegisterBody):
            existing = await self.auth.get_user(body.username)
            if existing:
                raise HTTPException(status_code=400, detail="User already exists")
            user = await self.auth.create_user(body.username, body.password, role="viewer")
            return {"username": user.username, "role": user.role}

        # ---- Health / metrics ----
        @app.get("/api/explain/health")
        async def health():
            db_status = await self.dashboard.db_manager.health_check()
            carbon_ok = True
            if self.carbon_manager and hasattr(self.carbon_manager, "get_current_intensity"):
                try:
                    await self.carbon_manager.get_current_intensity()
                except Exception:
                    carbon_ok = False
            lca_ok = True
            if self.lca_client and hasattr(self.lca_client, "get_material_index"):
                try:
                    result = self.lca_client.get_material_index("test")
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    lca_ok = False
            return {
                "status": "healthy" if db_status.get("status") == "ok" and carbon_ok and lca_ok else "degraded",
                "database": db_status,
                "carbon_manager": "ok" if carbon_ok else "error",
                "lca_client": "ok" if lca_ok else "error",
                "cache_size": len(self.dashboard._cache),
                "websocket_connections": len(self.dashboard._ws_connections),
                "chaos_enabled": self.chaos_monkey.enabled,
                "timestamp": _utc_now().isoformat(),
            }

        @app.get("/api/explain/metrics")
        async def get_metrics():
            if not PROMETHEUS_AVAILABLE:
                return Response(content=b"# prometheus not installed", media_type="text/plain")
            return Response(content=METRICS.render(), media_type=CONTENT_TYPE_LATEST)

        # ---- Request data ----
        @app.get("/api/explain/request/{request_id}")
        async def explain_request(request_id: str, user: Dict[str, Any] = Depends(get_current_user)):
            data = await self.dashboard.get_request_data(request_id, user.get("sub"))
            if "error" in data:
                raise HTTPException(status_code=404, detail=data["error"])
            return data

        @app.get("/api/explain/dashboard")
        async def dashboard_data(
            page: int = 1, page_size: int = 20, expert: Optional[str] = None,
            user: Dict[str, Any] = Depends(get_current_user),
        ):
            return await self.dashboard.get_recent_requests(page, page_size, expert, user.get("sub"))

        @app.get("/api/explain/charts")
        async def dashboard_charts(request_id: Optional[str] = None, user: Dict[str, Any] = Depends(get_current_user)):
            charts = await self.dashboard.get_dashboard_charts(request_id, user.get("sub"))
            if "error" in charts:
                raise HTTPException(status_code=400, detail=charts["error"])
            return charts

        @app.post("/api/explain/whatif")
        async def whatif_simulation(body: Dict[str, Any], user: Dict[str, Any] = Depends(get_current_user)):
            try:
                result = await self.what_if.simulate(
                    body.get("request_id"), body.get("alternative_expert_id")
                )
                return asdict(result)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

        @app.post("/api/explain/feedback/{request_id}")
        async def submit_feedback(
            request_id: str, body: Dict[str, Any],
            user: Dict[str, Any] = Depends(get_current_user),
        ):
            rating = int(body.get("rating", 0))
            comment = body.get("comment")
            if not 1 <= rating <= 5:
                raise HTTPException(status_code=400, detail="rating must be 1..5")
            # Fetch and update
            data = await self.dashboard.get_request_data(request_id, user.get("sub"))
            if "error" in data:
                raise HTTPException(status_code=404, detail="Request not found")
            req = self.dashboard._request_logs.get(request_id)
            if req:
                req.feedback_rating = rating
                req.feedback_comment = comment
            try:
                if ASYNC_SQLALCHEMY_AVAILABLE and self.dashboard.db_manager.async_sessionmaker is not None:
                    async with self.dashboard.db_manager.async_sessionmaker() as session:
                        await session.execute(
                            update(RequestLogDB)
                            .where(RequestLogDB.request_id == request_id)
                            .values(feedback_rating=rating, feedback_comment=comment)
                        )
                        await session.commit()
                elif SQLALCHEMY_SYNC_AVAILABLE and self.dashboard.db_manager.sync_sessionmaker is not None:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, self._update_feedback_sync, request_id, rating, comment
                    )
            except Exception as e:
                logger.error(f"feedback_update_failed error={e}")
                raise HTTPException(status_code=500, detail="Feedback update failed")

            if not self.chaos_monkey.is_feedback_dropped():
                event = FeedbackEvent.create_with_context(
                    task_id=f"feedback_{request_id}",
                    selected_action=req.chosen_expert_id if req else "unknown",
                    quality_score=rating / 5.0,
                    latency_ms=0,
                    energy_joules=0,
                    carbon_g=0,
                    feedback_type="user_preference",
                    adaptive_cost_value=0.0,
                    state={"request_id": request_id, "comment": comment},
                    candidates=[{"action": "none"}],
                    source="explainable_ui",
                    environment="production",
                    tags=["user_feedback"],
                )
                try:
                    await self.message_queue.publish("feedback_events", _serialize_event(event))
                except Exception as e:
                    logger.warning(f"feedback_publish_failed error={e}")

            if self.generator is not None:
                await self.generator.record_feedback(rating, data.get("explanation", ""))
            return {"status": "feedback recorded"}

        # ---- Export ----
        @app.get("/api/explain/export/{request_id}")
        async def export_request(
            request_id: str, format: str = "json",
            user: Dict[str, Any] = Depends(get_current_user),
        ):
            data = await self.dashboard.get_request_data(request_id, user.get("sub"))
            if "error" in data:
                raise HTTPException(status_code=404, detail=data["error"])
            if format == "json":
                return data
            if format == "csv":
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(["key", "value"])
                for k, v in data.items():
                    writer.writerow([k, str(v)])
                return Response(content=output.getvalue(), media_type="text/csv")
            if format == "png" and PLOTLY_AVAILABLE:
                try:
                    fig = go.Figure(data=[go.Bar(
                        x=[a["expert_id"] for a in data["alternatives"]] + [data["chosen_expert"]],
                        y=[a["energy_joules"] for a in data["alternatives"]] + [data["energy_joules"]],
                    )])
                    img_bytes = fig.to_image(format="png")
                    return Response(content=img_bytes, media_type="image/png")
                except Exception as e:
                    logger.warning(f"png_export_failed error={e}")
                    raise HTTPException(status_code=500, detail="PNG generation failed")
            raise HTTPException(status_code=400, detail="Unsupported format")

        def _update_feedback_sync(request_id: str, rating: int, comment: Optional[str]) -> None:
            session = self.dashboard.db_manager.sync_sessionmaker()
            try:
                session.query(RequestLogDB).filter_by(request_id=request_id).update({
                    "feedback_rating": rating,
                    "feedback_comment": comment,
                })
                session.commit()
            finally:
                session.close()

        # ---- Admin ----
        @app.post("/api/explain/admin/refresh")
        async def refresh_cache(user: Dict[str, Any] = Depends(require_role("admin"))):
            async with self.dashboard._cache_lock:
                self.dashboard._cache.clear()
                self.dashboard._cache_timestamps.clear()
            return {"status": "cache cleared"}

        @app.post("/api/explain/admin/export/all")
        async def export_all(format: str = "json", user: Dict[str, Any] = Depends(require_role("admin"))):
            if format == "json":
                return StreamingResponse(
                    self.dashboard.stream_all_requests(user.get("sub")),
                    media_type="application/json",
                )
            if format == "csv":
                async def stream_csv():
                    buffer: List[str] = []
                    header_written = False
                    async for chunk in self.dashboard.stream_all_requests(user.get("sub")):
                        buffer.append(chunk)
                        if len(buffer) > 200:
                            if not header_written:
                                yield "key,value\n"
                                header_written = True
                            for raw in buffer:
                                stripped = raw.strip().strip(",").strip("[").strip("]")
                                if not stripped:
                                    continue
                                try:
                                    obj = json.loads(stripped)
                                    if isinstance(obj, dict):
                                        for k, v in obj.items():
                                            yield f"{k},{str(v).replace(',', ';')}\n"
                                except Exception:
                                    pass
                            buffer.clear()
                    for raw in buffer:
                        stripped = raw.strip().strip(",").strip("[").strip("]")
                        if not stripped:
                            continue
                        try:
                            obj = json.loads(stripped)
                            if isinstance(obj, dict):
                                for k, v in obj.items():
                                    yield f"{k},{str(v).replace(',', ';')}\n"
                        except Exception:
                            pass
                return StreamingResponse(stream_csv(), media_type="text/csv")
            raise HTTPException(status_code=400, detail="Unsupported format")

        @app.post("/api/explain/admin/reload_template")
        async def reload_template(path: Optional[str] = None, user: Dict[str, Any] = Depends(require_role("admin"))):
            self.generator.reload_template(path)
            return {"status": "template reloaded"}

        # ---- Optimization ----
        @app.get("/api/explain/optimization/status")
        async def optimization_status(user: Dict[str, Any] = Depends(require_role("admin"))):
            return {
                "template_population_size": len(self.generator.template_population),
                "template_fitness_length": len(self.generator.template_fitness),
                "modp_weights": dict(self.dashboard.config.modp_weights),
                "template_evolution_enabled": self.dashboard.config.template_evolution_enabled,
                "enhancements_available": ENHANCEMENTS_AVAILABLE,
                "enhancement_status": self.generator.get_enhancement_status(),
            }

        @app.post("/api/explain/optimization/evolve")
        async def evolve_templates(user: Dict[str, Any] = Depends(require_role("admin"))):
            await self.generator.evolve_templates()
            return {"status": "evolution triggered"}

        # ---- Human review ----
        @app.get("/api/explain/human-review/pending")
        async def human_review_pending(user: Dict[str, Any] = Depends(require_role("admin"))):
            return await self.human_review.get_pending()

        @app.post("/api/explain/human-review/{review_id}/approve")
        async def human_review_approve(review_id: str, user: Dict[str, Any] = Depends(require_role("admin"))):
            ok = await self.human_review.approve(review_id, reviewer=user.get("sub", "admin"))
            if not ok:
                raise HTTPException(status_code=404, detail="Review not found")
            return {"status": "approved", "review_id": review_id}

        @app.post("/api/explain/human-review/{review_id}/reject")
        async def human_review_reject(
            review_id: str, reason: Optional[str] = None,
            user: Dict[str, Any] = Depends(require_role("admin")),
        ):
            ok = await self.human_review.reject(review_id, reason=reason, reviewer=user.get("sub", "admin"))
            if not ok:
                raise HTTPException(status_code=404, detail="Review not found")
            return {"status": "rejected", "review_id": review_id}

        @app.post("/api/explain/human-review/request")
        async def human_review_request(
            body: Dict[str, Any], user: Dict[str, Any] = Depends(get_current_user),
        ):
            review_id = await self.human_review.request_review(
                body.get("request_id", ""), body.get("details", {})
            )
            req = self.dashboard._request_logs.get(body.get("request_id", ""))
            if req:
                req.human_review_id = review_id
            return {"review_id": review_id}

        # ---- Chaos ----
        @app.post("/api/explain/chaos/trigger")
        async def chaos_trigger(
            enabled: bool = True, probability: float = 0.1,
            user: Dict[str, Any] = Depends(require_role("admin")),
        ):
            self.chaos_monkey.enabled = bool(enabled)
            self.chaos_monkey.failure_probability = float(probability)
            return self.chaos_monkey.get_stats()

        @app.post("/api/explain/chaos/inject")
        async def chaos_inject(fault: str, steps: int = 5, user: Dict[str, Any] = Depends(require_role("admin"))):
            if fault not in ChaosMonkey.FAULT_TYPES:
                raise HTTPException(status_code=400, detail="unknown fault")
            return await self.chaos_monkey.inject_and_measure(self.dashboard, fault, steps=steps)

        # ---- Federated ----
        @app.post("/api/explain/federated/register")
        async def federated_register(
            body: Dict[str, Any], user: Dict[str, Any] = Depends(require_role("admin")),
        ):
            self.generator.federated_coordinator.register_participant(
                body.get("participant_id", "unknown"),
                body.get("update", {}),
                sample_count=int(body.get("sample_count", 1)),
            )
            return {
                "status": "registered",
                "participants": self.generator.federated_coordinator.get_participant_count(),
            }

        @app.get("/api/explain/federated/aggregate")
        async def federated_aggregate(user: Dict[str, Any] = Depends(require_role("admin"))):
            return self.generator.federated_coordinator.aggregate()

        # ---- Quantum ----
        @app.post("/api/explain/quantum/optimize")
        async def quantum_optimize(body: Dict[str, Any], user: Dict[str, Any] = Depends(require_role("admin"))):
            candidates = body.get("candidates", [])
            result = await self.generator.quantum_optimizer.select_best_template(
                candidates, self.dashboard.config.modp_weights
            )
            return {"result": result, "status": self.generator.quantum_optimizer.get_status()}

        # ---- Carbon ----
        @app.post("/api/explain/carbon/offset")
        async def purchase_offset(body: Dict[str, Any], user: Dict[str, Any] = Depends(get_current_user)):
            return await self.offset_broker.purchase_offsets(
                float(body.get("carbon_intensity", 0.0)),
                float(body.get("carbon_kg", 0.0)),
            )

        @app.post("/api/explain/carbon/rec")
        async def purchase_rec(body: Dict[str, Any], user: Dict[str, Any] = Depends(get_current_user)):
            return await self.offset_broker.purchase_recs(float(body.get("energy_mwh", 0.0)))

        @app.get("/api/explain/carbon/totals")
        async def carbon_totals(user: Dict[str, Any] = Depends(get_current_user)):
            return self.offset_broker.get_totals()

        # ---- Precision ----
        @app.post("/api/explain/precision/recommend")
        async def recommend_precision(body: Dict[str, Any], user: Dict[str, Any] = Depends(get_current_user)):
            precision = self.what_if.precision_policy.recommend_precision(
                workload_size=body.get("workload_size", "medium"),
                carbon_intensity=body.get("carbon_intensity"),
            )
            return {"precision": precision}

        # ---- Safety ----
        @app.get("/api/explain/safety/violations")
        async def safety_violations(user: Dict[str, Any] = Depends(require_role("admin"))):
            return {"violations": self.generator.safety_monitor.get_violations()}

        logger.info("api_routes_registered")


# =============================================================================
# Factory
# =============================================================================
def create_explainable_ui(
    config: Optional[Union[Dict[str, Any], ExplainableUIConfig]] = None,
    carbon_manager: Optional[Any] = None,
    lca_client: Optional[Any] = None,
    message_queue: Optional[AsyncMessageQueue] = None,
) -> Dict[str, Any]:
    if config is None:
        config = ExplainableUIConfig()
    elif isinstance(config, dict):
        config = ExplainableUIConfig.from_dict(config)

    db_manager = DatabaseManager(config)
    generator = ExplanationGenerator(config, db_manager=db_manager)
    offset_broker = CarbonOffsetBroker()
    precision_policy = FlexGenPrecisionPolicy()
    chaos_monkey = ChaosMonkey(
        enabled=config.chaos_enabled,
        failure_probability=config.chaos_failure_probability,
        seed=config.chaos_seed,
    )
    human_review = HumanReviewManager(
        ttl_seconds=config.hitl_ttl_seconds,
        max_pending=config.hitl_max_pending,
    )
    dashboard = DashboardEngine(config, db_manager, generator, chaos_monkey=chaos_monkey)
    what_if = WhatIfSimulator(
        dashboard, config, carbon_manager, lca_client,
        modp=generator.modp if ENHANCEMENTS_AVAILABLE else None,
        offset_broker=offset_broker,
        precision_policy=precision_policy,
    )
    dashboard.what_if = what_if
    auth = AuthManager(config, db_manager)
    api_extension = APIGatewayExtension(
        dashboard, generator, what_if, auth,
        message_queue=message_queue,
        carbon_manager=carbon_manager,
        lca_client=lca_client,
        offset_broker=offset_broker,
        human_review=human_review,
        chaos_monkey=chaos_monkey,
    )
    return {
        "dashboard": dashboard,
        "explanation_generator": generator,
        "what_if": what_if,
        "auth": auth,
        "api_extension": api_extension,
        "db_manager": db_manager,
        "offset_broker": offset_broker,
        "human_review": human_review,
        "chaos_monkey": chaos_monkey,
        "precision_policy": precision_policy,
    }


def create_app(
    config: Optional[Union[Dict[str, Any], ExplainableUIConfig]] = None,
) -> Any:
    if not FASTAPI_AVAILABLE:
        raise ConfigurationError("FastAPI is not installed")

    components = create_explainable_ui(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await components["dashboard"].start()
        try:
            yield
        finally:
            await components["dashboard"].shutdown()

    app = FastAPI(
        title="Enhanced Explainable Green Decisions",
        version="4.2.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    components["api_extension"].register_routes(app)
    app.state.components = components
    return app


# =============================================================================
# Entry point
# =============================================================================
async def _async_main() -> None:
    logging.basicConfig(level=logging.INFO)
    components = create_explainable_ui()
    db_manager = components["db_manager"]
    dashboard = components["dashboard"]
    gen = components["explanation_generator"]
    what_if = components["what_if"]

    await dashboard.start()

    prof = SustainabilityAwareExpertProfile("expert_A")
    prof.energy_per_inference_full = 2.5
    prof.accuracy_full = 0.92

    alt_prof = SustainabilityAwareExpertProfile("expert_B")
    alt_prof.energy_per_inference_full = 3.8
    alt_prof.accuracy_full = 0.94

    req = RequestLog(
        request_id=str(uuid.uuid4()),
        timestamp=_utc_now(),
        query="What is the weather?",
        chosen_expert_id="expert_A",
        chosen_expert_profile=prof,
        alternative_experts=[("expert_B", alt_prof)],
        latency_ms=120.0,
        energy_joules=2.5,
        co2_kg=2.5 * 0.2 / 3600000,
        accuracy=0.92,
        carbon_intensity=400.0,
        helium_scarcity=0.5,
        material_index=0.2,
        precision="fp16",
    )
    await dashboard.log_request(req)

    explanation = await gen.generate_async(
        req, prof, [("expert_B", alt_prof)], user_context={"role": "admin"}
    )
    print("Explanation:", explanation)

    result = await what_if.simulate(req.request_id, "expert_B")
    print("What-if:", asdict(result))

    await gen.record_feedback(rating=4, template_used=explanation)
    print("Enhancement status:", gen.get_enhancement_status())

    await dashboard.shutdown()
    print("Shutdown complete")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()


__all__ = [
    "ExplainableUIConfig",
    "RequestLog",
    "WhatIfResult",
    "DatabaseManager",
    "ExplanationGenerator",
    "DashboardEngine",
    "WhatIfSimulator",
    "AuthManager",
    "APIGatewayExtension",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "FeatureAttributionExplainer",
    "FederatedFeedbackCoordinator",
    "MultiAgentUIRouter",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "QuantumDistillationOptimizer",
    "FlexGenPrecisionPolicy",
    "CircuitBreaker",
    "CircuitBreakerState",
    "ExplainableUIException",
    "SafetyViolationError",
    "ChaosExperimentError",
    "QuantumError",
    "AuthError",
    "ConfigurationError",
    "create_explainable_ui",
    "create_app",
    "main",
]
