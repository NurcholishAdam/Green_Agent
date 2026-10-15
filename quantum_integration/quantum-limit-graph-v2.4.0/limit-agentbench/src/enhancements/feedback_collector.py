#!/usr/bin/env python3
# File: src/enhancements/feedback_recorder_enhanced_v15_0.py
"""
Enhanced Feedback Recorder for Green Agent — v16.1 (Single-File Enhanced).

Public API preserved:
    FeedbackError, CircuitBreakerOpenError, CostFunctionUnavailableError,
    SafetyViolationError, ChaosExperimentError,
    FeedbackContext, FeedbackMetrics, FeedbackRecord,
    GeneralConfig, CostFunctionConfig, OptimizerConfig, FeedbackConfig,
    CausalBandit, BanditConfig,
    QuantumDistillationOptimizer, DifferentialPrivacy, PrivacyAccountant,
    FederatedFeedbackCoordinator, MultiAgentFeedbackCoordinator,
    FormalSafetyMonitor, XAIExplainer, FlexGenPrecisionPolicy,
    CarbonOffsetBroker, ChaosMonkey, RealHumanApprovalManager,
    HumanApprovalEntry, ActiveLearningRLHF,
    CircuitBreakerState, CircuitBreaker, GlobalCircuitBreaker, Bulkhead,
    FeedbackRecorder, build_components, create_app, get_feedback_recorder, main.

Fixes and enhancements over v16.0:
- Import-time crash fixed: `logging.handlers` now imported.
- `logger.addFilter` guarded with isinstance check (structlog-safe).
- `FeedbackRecorder.__init__` no longer creates asyncio tasks; the batch
  processor and evolution loop start via `async def start()`.
- `start()` is idempotent and awaits the batch queue initialization.
- `await self.bandit.update(...)` bug fixed (CausalBandit.update is sync).
- CausalBandit is a real per-action Bayesian linear regression bandit with
  Thompson sampling and UCB; supports `add_actions`.
- Real PrivacyAccountant tracking cumulative (ε, δ); DP refuses updates
  once the budget is exhausted.
- QuantumDistillationOptimizer: modern-qiskit-first import; correct arity
  for `select_best_policy(candidates, weights)`.
- RealHumanApprovalManager uses asyncio.Event per decision, TTL eviction,
  capacity limit, audit log. No more busy-wait.
- ActiveLearningRLHF does not penalize the string "unknown" and does not
  double-count updates.
- MultiAgentFeedbackCoordinator now updates reputation for actual
  contributors (agents), and votes are attributed correctly; the
  per-policy reward is tracked separately.
- FormalSafetyMonitor returns (safe, reason); result respected by the
  caller (forcing `delay` on violation).
- `FeedbackRecorder` uses a bounded queue (maxsize) with explicit drop
  accounting.
- `_send_batch` uses tenacity retry + bulkhead + circuit breaker; the
  bulkhead is applied inside the retry so queue time isn't counted twice.
- `health_check` no longer injects a synthetic feedback record; it pings
  the endpoint with an HTTP HEAD.
- `_load_state` / `_save_state` real implementation using an optional
  Storage injected via the config.
- `_last_policy_by_decision` tracked per decision.
- Real Prometheus metrics incremented on every send path.
- FastAPI moved to `create_app()` with lifespan; admin endpoints guarded.
- `__aenter__` / `__aexit__` on FeedbackRecorder.
- `_batch_queue` is created in `start()`; `record_feedback` auto-starts
  the recorder if the caller forgot.
- Shutdown flushes the queue and awaits the batch task.
- Structured logging via structlog when available.
"""

from __future__ import annotations

import asyncio
import base64  # noqa: F401
import contextlib
import contextvars
import hashlib
import json
import logging
import logging.handlers  # noqa: F401
import math
import os
import random
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# -----------------------------------------------------------------------------
# Optional / guarded imports
# -----------------------------------------------------------------------------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import structlog  # noqa: F401
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "feedback_recorder.log",
                    maxBytes=10 * 1024 * 1024,
                    backupCount=5,
                ),
                logging.StreamHandler(),
            ],
        )

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from tenacity import (
        before_sleep_log, retry, retry_if_exception_type, stop_after_attempt,
        wait_exponential,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator  # noqa: F401
    try:
        from pydantic_settings import BaseSettings, SettingsConfigDict  # noqa: F401
    except ImportError:
        from pydantic import BaseSettings  # type: ignore
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
    from fastapi import (
        BackgroundTasks, Depends, FastAPI, HTTPException, Request, status,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    import jwt as pyjwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

# Qiskit — modern layout first, then legacy
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


# -----------------------------------------------------------------------------
# Enhancement modules (guarded)
# -----------------------------------------------------------------------------
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    from enhancements.limit_graph import LimitGraph  # type: ignore
    from enhancements.rlhf import RLHFOptimizer  # type: ignore
    from enhancements.multi_teacher_policy_distillation import (  # type: ignore
        MultiTeacherDistiller,
    )
    ENHANCEMENTS_AVAILABLE = True
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = False

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
        def select(self, encoded): return "immediate"

    class ParetoOptimizer:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0.0) * weights.get(k, 1.0) for k in objectives)

    class ContextualBandit:  # type: ignore[no-redef]
        def __init__(self, action_space, fallback_solver, *a, **k):
            self.actions = list(action_space)
            self.fallback = fallback_solver
        def select_action(self, context):
            return self.fallback(context), 1.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

    class LimitGraph:  # type: ignore[no-redef]
        def __init__(self, *a, **k): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

    class RLHFOptimizer:  # type: ignore[no-redef]
        def __init__(self, action_space, *a, **k):
            self.actions = list(action_space)
            self.preferences = []
        def update(self, context, action, reward): pass
        def sample_action(self, context):
            return self.actions[0] if self.actions else None
        def add_human_feedback(self, context, chosen_action, rejected_action):
            self.preferences.append((context, chosen_action, rejected_action))

    class MultiTeacherDistiller:  # type: ignore[no-redef]
        def __init__(self, teachers, *a, **k): self.teachers = teachers
        def distill(self, context):
            if not self.teachers:
                return "immediate"
            votes = [t(context) for t in self.teachers]
            return max(set(votes), key=votes.count) if votes else "immediate"


# Guarded Storage import. Used only for optional state persistence.
try:
    from ..storage import Storage  # type: ignore
except Exception:
    class Storage:  # type: ignore[no-redef]
        def get_feedback_recorder_state(self): return None
        def save_feedback_recorder_state(self, state): return None


# =============================================================================
# Helpers
# =============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)


def _get_correlation_id() -> str:
    cid = correlation_id_var.get()
    if not cid:
        cid = str(uuid.uuid4())[:8]
        correlation_id_var.set(cid)
    return cid


class CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _get_correlation_id()
        return True


if isinstance(logger, logging.Logger):
    logger.addFilter(CorrelationIdFilter())

audit_logger = logging.getLogger("audit")
if not audit_logger.handlers:
    try:
        _ah = logging.handlers.RotatingFileHandler(
            "audit.log", maxBytes=10 * 1024 * 1024, backupCount=5
        )
    except Exception:
        _ah = logging.StreamHandler()
    _ah.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    audit_logger.addHandler(_ah)
audit_logger.setLevel(logging.INFO)


# =============================================================================
# Exceptions
# =============================================================================
class FeedbackError(Exception):
    """Base class for the feedback recorder."""


class CircuitBreakerOpenError(FeedbackError): pass
class CostFunctionUnavailableError(FeedbackError): pass
class SafetyViolationError(FeedbackError): pass
class ChaosExperimentError(FeedbackError): pass
class NotImplementedYetError(FeedbackError): pass


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
        from prometheus_client import generate_latest
        return generate_latest(self.registry)


METRICS = _MetricsRegistry()
FEEDBACK_RECORDS = METRICS.counter(
    "feedback_records_total", "Feedback records sent", ["status"]
)
FEEDBACK_LATENCY = METRICS.histogram("feedback_latency_seconds", "Feedback call latency")
CIRCUIT_BREAKER_STATE = METRICS.gauge(
    "feedback_circuit_breaker_state", "CB state", ["name"]
)
BATCH_SIZE = METRICS.gauge("feedback_batch_size", "Batch size")
QUEUE_SIZE = METRICS.gauge("feedback_queue_size", "Queue size")
QUEUE_DROPPED = METRICS.counter("feedback_queue_dropped_total", "Queue drop events")
SAFETY_VIOLATIONS = METRICS.counter(
    "feedback_safety_violations_total", "Safety violations", ["rule"]
)
CHAOS_EXPERIMENTS = METRICS.counter(
    "feedback_chaos_experiments_total", "Chaos events", ["fault"]
)
HUMAN_REVIEWS = METRICS.counter(
    "feedback_human_reviews_total", "Human reviews", ["status"]
)
XAI_DECISIONS = METRICS.counter(
    "feedback_xai_decisions_total", "XAI decisions", ["policy"]
)
CARBON_OFFSETS = METRICS.counter(
    "feedback_carbon_offsets_total", "Carbon offsets", ["status"]
)
PRECISION_SELECTIONS = METRICS.counter(
    "feedback_precision_selections_total", "Precision selections", ["precision"]
)
FEDERATED_SHARES = METRICS.counter(
    "feedback_federated_shares_total", "Federated shares", ["source"]
)
DP_CLIPPED = METRICS.counter("feedback_dp_clipped_total", "DP clipping events")
DP_EPSILON = METRICS.gauge(
    "feedback_dp_cumulative_epsilon", "Cumulative DP epsilon"
)
BULKHEAD_ACTIVE = METRICS.gauge("feedback_bulkhead_active", "Bulkhead active")
BULKHEAD_QUEUED = METRICS.gauge("feedback_bulkhead_queued", "Bulkhead queued")


# =============================================================================
# Privacy accountant
# =============================================================================
@dataclass
class PrivacyAccountant:
    """Sequential composition bound for the Gaussian mechanism."""

    delta: float = 1e-5
    cumulative_epsilon: float = 0.0
    updates: int = 0
    budget_epsilon: Optional[float] = None

    def step(self, noise_multiplier: float) -> float:
        sigma = max(float(noise_multiplier), 1e-6)
        eps_single = math.sqrt(2.0 * math.log(1.25 / self.delta)) / sigma
        self.cumulative_epsilon += eps_single
        self.updates += 1
        return eps_single

    def budget_exhausted(self) -> bool:
        return (
            self.budget_epsilon is not None
            and self.cumulative_epsilon >= self.budget_epsilon
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "delta": self.delta,
            "cumulative_epsilon": self.cumulative_epsilon,
            "updates": self.updates,
            "budget_epsilon": self.budget_epsilon,
        }


# =============================================================================
# Pydantic / dataclass models
# =============================================================================
if PYDANTIC_AVAILABLE:
    class FeedbackContext(BaseModel):
        request_id: str
        expert_id: str
        node_id: str
        task_type: str = "general"
        timestamp: datetime = Field(default_factory=_utc_now)

        @field_validator("request_id")
        @classmethod
        def _v_request_id(cls, v: str) -> str:
            if not v:
                raise ValueError("request_id cannot be empty")
            return v

        @field_validator("expert_id")
        @classmethod
        def _v_expert_id(cls, v: str) -> str:
            if not v:
                raise ValueError("expert_id cannot be empty")
            return v

    class FeedbackMetrics(BaseModel):
        predicted_cost: float
        actual_cost: float
        energy_joules: float = 0.0
        carbon_kg: float = 0.0
        helium_units: float = 0.0
        latency_ms: float = 0.0
        accuracy: float = 0.0
        teacher_id: Optional[str] = None
        distillation_loss: Optional[float] = None

        @field_validator("predicted_cost")
        @classmethod
        def _v_predicted_cost(cls, v: float) -> float:
            if v < 0:
                raise ValueError("predicted_cost cannot be negative")
            return v

        @field_validator("actual_cost")
        @classmethod
        def _v_actual_cost(cls, v: float) -> float:
            if v < 0:
                raise ValueError("actual_cost cannot be negative")
            return v

    class FeedbackRecord(BaseModel):
        context: FeedbackContext
        metrics: FeedbackMetrics
        timestamp: datetime = Field(default_factory=_utc_now)
        correlation_id: str = Field(default_factory=_get_correlation_id)

else:
    @dataclass
    class FeedbackContext:
        request_id: str
        expert_id: str
        node_id: str
        task_type: str = "general"
        timestamp: datetime = field(default_factory=_utc_now)

    @dataclass
    class FeedbackMetrics:
        predicted_cost: float
        actual_cost: float
        energy_joules: float = 0.0
        carbon_kg: float = 0.0
        helium_units: float = 0.0
        latency_ms: float = 0.0
        accuracy: float = 0.0
        teacher_id: Optional[str] = None
        distillation_loss: Optional[float] = None

    @dataclass
    class FeedbackRecord:
        context: FeedbackContext
        metrics: FeedbackMetrics
        timestamp: datetime = field(default_factory=_utc_now)
        correlation_id: str = field(default_factory=_get_correlation_id)


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict") and not isinstance(obj, dict):
        try:
            return obj.dict()
        except Exception:
            pass
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    return dict(obj) if isinstance(obj, dict) else {}


# =============================================================================
# Config
# =============================================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        max_retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        circuit_breaker_failure_threshold: int = Field(5, ge=1)
        circuit_breaker_recovery_timeout: int = Field(60, ge=1)
        circuit_breaker_half_open_attempts: int = Field(3, ge=1)
        bulkhead_max_concurrency: int = Field(10, ge=1)
        batch_interval_seconds: float = Field(2.0, ge=0.5)
        batch_max_size: int = Field(100, ge=1)
        batch_queue_maxsize: int = Field(1000, ge=1)
        health_check_interval: int = Field(60, ge=10)
        log_level: str = Field("INFO")
        human_review_threshold_queue_size: int = Field(80, ge=1)

        @field_validator("log_level")
        @classmethod
        def _v_log_level(cls, v: str) -> str:
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

    class CostFunctionConfig(BaseModel):
        endpoint_url: str = Field("http://localhost:8000/feedback")
        timeout_seconds: float = Field(10.0, gt=0)
        api_key: Optional[str] = None

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        batch_policies: List[str] = Field(
            default_factory=lambda: ["immediate", "small_batch", "large_batch", "delay"]
        )
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "importance": 0.4, "urgency": 0.3, "energy": 0.2, "latency": 0.1,
            }
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bandit_feature_dim: int = Field(8, ge=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        evolve_interval_seconds: int = Field(3600, ge=60)
        limit_graph_enabled: bool = True
        limit_graph_max_nodes: int = 100
        rlhf_enabled: bool = True
        rlhf_buffer_size: int = 1000
        distillation_enabled: bool = True
        distillation_update_interval: int = 600
        causal_bandit_enabled: bool = True
        enable_distillation: bool = False
        qaoa_reps: int = 1
        dp_clip_norm: float = 1.0
        dp_noise_multiplier: float = 0.1
        dp_delta: float = 1e-5
        dp_budget_epsilon: Optional[float] = None
        enable_dp: bool = True
        enable_federated: bool = True
        federated_privacy_budget: float = Field(0.5, gt=0)
        federated_sensitivity: float = Field(1.0, gt=0)
        enable_multi_agent: bool = True
        safety_max_queue_size: int = Field(50, ge=1)
        safety_max_consecutive_failures: int = Field(3, ge=1)
        enable_xai: bool = True
        enable_precision_switch: bool = True
        default_carbon_intensity: float = Field(400.0, gt=0)
        carbon_offset_threshold_kg: float = Field(1.0, gt=0)
        carbon_offset_cost_per_kg: float = Field(0.1, gt=0)
        chaos_enabled: bool = False
        chaos_failure_probability: float = Field(0.1, ge=0, le=1)
        chaos_seed: int = 42
        human_review_enabled: bool = True
        human_review_timeout_seconds: float = Field(30.0, gt=0)
        human_review_ttl_seconds: float = Field(300.0, gt=0)
        human_review_max_pending: int = Field(100, ge=1)

    class FeedbackConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="FEEDBACK_", case_sensitive=False, env_nested_delimiter="__",
        )
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        cost_function: CostFunctionConfig = Field(default_factory=CostFunctionConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        api_host: str = Field("0.0.0.0")
        api_port: int = Field(8001)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        require_auth_for_admin: bool = True

        def to_dict(self) -> Dict[str, Any]:
            return _model_to_dict(self)

else:
    @dataclass
    class GeneralConfig:
        max_retry_attempts: int = 3
        retry_wait_seconds: int = 2
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: int = 60
        circuit_breaker_half_open_attempts: int = 3
        bulkhead_max_concurrency: int = 10
        batch_interval_seconds: float = 2.0
        batch_max_size: int = 100
        batch_queue_maxsize: int = 1000
        health_check_interval: int = 60
        log_level: str = "INFO"
        human_review_threshold_queue_size: int = 80

    @dataclass
    class CostFunctionConfig:
        endpoint_url: str = "http://localhost:8000/feedback"
        timeout_seconds: float = 10.0
        api_key: Optional[str] = None

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        batch_policies: List[str] = field(
            default_factory=lambda: ["immediate", "small_batch", "large_batch", "delay"]
        )
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {
                "importance": 0.4, "urgency": 0.3, "energy": 0.2, "latency": 0.1,
            }
        )
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bandit_feature_dim: int = 8
        bio_generations: int = 10
        bio_population_size: int = 20
        evolve_interval_seconds: int = 3600
        limit_graph_enabled: bool = True
        limit_graph_max_nodes: int = 100
        rlhf_enabled: bool = True
        rlhf_buffer_size: int = 1000
        distillation_enabled: bool = True
        distillation_update_interval: int = 600
        causal_bandit_enabled: bool = True
        enable_distillation: bool = False
        qaoa_reps: int = 1
        dp_clip_norm: float = 1.0
        dp_noise_multiplier: float = 0.1
        dp_delta: float = 1e-5
        dp_budget_epsilon: Optional[float] = None
        enable_dp: bool = True
        enable_federated: bool = True
        federated_privacy_budget: float = 0.5
        federated_sensitivity: float = 1.0
        enable_multi_agent: bool = True
        safety_max_queue_size: int = 50
        safety_max_consecutive_failures: int = 3
        enable_xai: bool = True
        enable_precision_switch: bool = True
        default_carbon_intensity: float = 400.0
        carbon_offset_threshold_kg: float = 1.0
        carbon_offset_cost_per_kg: float = 0.1
        chaos_enabled: bool = False
        chaos_failure_probability: float = 0.1
        chaos_seed: int = 42
        human_review_enabled: bool = True
        human_review_timeout_seconds: float = 30.0
        human_review_ttl_seconds: float = 300.0
        human_review_max_pending: int = 100

    @dataclass
    class FeedbackConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        cost_function: CostFunctionConfig = field(default_factory=CostFunctionConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        api_host: str = "0.0.0.0"
        api_port: int = 8001
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        require_auth_for_admin: bool = True

        def to_dict(self) -> Dict[str, Any]:
            return asdict(self)


def _cfg_dict(cfg: Any) -> Dict[str, Any]:
    if cfg is None:
        return {}
    if hasattr(cfg, "to_dict"):
        try:
            return cfg.to_dict()
        except Exception:
            pass
    return _model_to_dict(cfg)


# =============================================================================
# CausalBandit — real linear contextual bandit
# =============================================================================
@dataclass
class BanditConfig:
    alpha: float = 1.0
    exploration_beta: float = 1.0
    min_trials: int = 5
    use_thompson: bool = True


class CausalBandit:
    """Per-action Bayesian linear regression bandit with Thompson sampling."""

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
                conf = 1.0 / (1.0 + math.exp(-abs(samples[best])))
                return best, float(conf), "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * math.sqrt(var)
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + math.exp(-abs(scores[best])))
            return best, float(conf), "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
        """Synchronous update. Do NOT await."""
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

    def add_actions(self, new_actions: List[str]) -> List[str]:
        added: List[str] = []
        with self._lock:
            for a in new_actions:
                if not isinstance(a, str) or not a or a in self.theta:
                    continue
                self.theta[a] = np.zeros(self.dim)
                self.P[a] = np.eye(self.dim) / self.config.alpha
                self.usage[a] = 0
                self.actions.append(a)
                added.append(a)
        return added

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": list(self.actions),
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# DifferentialPrivacy
# =============================================================================
class DifferentialPrivacy:
    """DP via L2 clipping + Gaussian noise, with budget accounting."""

    def __init__(
        self,
        clip_norm: float = 1.0,
        noise_multiplier: float = 0.1,
        delta: float = 1e-5,
        budget_epsilon: Optional[float] = None,
    ):
        self.clip_norm = float(clip_norm)
        self.noise_multiplier = float(noise_multiplier)
        self.accountant = PrivacyAccountant(delta=float(delta), budget_epsilon=budget_epsilon)
        self._lock = threading.RLock()

    def clip_and_noise(self, values: List[float]) -> List[float]:
        with self._lock:
            if self.accountant.budget_exhausted():
                raise SafetyViolationError(
                    f"DP budget exhausted (eps={self.accountant.cumulative_epsilon:.4f})"
                )
            norm = math.sqrt(sum(float(v) ** 2 for v in values)) if values else 0.0
            clip_factor = min(1.0, self.clip_norm / (norm + 1e-12))
            if clip_factor < 1.0:
                DP_CLIPPED.inc()
            noise_std = self.noise_multiplier * self.clip_norm
            out = [float(v) * clip_factor + random.gauss(0.0, noise_std) for v in values]
            self.accountant.step(self.noise_multiplier)
            DP_EPSILON.set(self.accountant.cumulative_epsilon)
            return out

    def get_accountant(self) -> PrivacyAccountant:
        return self.accountant


# =============================================================================
# QuantumDistillationOptimizer
# =============================================================================
class QuantumDistillationOptimizer:
    def __init__(self, enabled: bool = False, qaoa_reps: int = 1, max_items: int = 8):
        self.enabled = bool(enabled)
        self.qaoa_reps = int(qaoa_reps)
        self.max_items = int(max_items)
        self.available = self.enabled and QISKIT_AVAILABLE

    def select_best_policy(
        self,
        candidates: List[Dict[str, Any]],
        weights: Dict[str, float],
    ) -> Optional[Dict[str, Any]]:
        if not candidates:
            return None
        if not self.available:
            return self._classical_select(candidates, weights)
        try:
            return self._quantum_select(candidates, weights)
        except Exception as e:
            logger.warning(f"quantum_select_failed error={e}")
            return self._classical_select(candidates, weights)

    def _classical_select(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        best, best_score = None, -float("inf")
        for c in candidates:
            score = sum(
                float(c.get(k, 0.0)) * float(weights.get(k, 0.0)) for k in weights
            )
            if score > best_score:
                best_score = score
                best = c
        return best or candidates[0]

    def _quantum_select(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        candidates = candidates[: self.max_items]
        qp = QuadraticProgram()
        for i in range(len(candidates)):
            qp.binary_var(f"x{i}")
        linear: Dict[str, float] = {}
        for i, c in enumerate(candidates):
            utility = sum(
                float(c.get(k, 0.0)) * float(weights.get(k, 0.0)) for k in weights
            )
            linear[f"x{i}"] = -utility
        qp.minimize(linear=linear)
        qp.linear_constraint(
            linear={f"x{i}": 1 for i in range(len(candidates))},
            sense="E", rhs=1, name="one_policy",
        )
        qaoa = QAOA(reps=self.qaoa_reps)
        optimizer = MinimumEigenOptimizer(qaoa)
        result = optimizer.solve(qp)
        for i, c in enumerate(candidates):
            if result.x[i] > 0.5:
                return c
        return candidates[0]

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "qiskit_available": QISKIT_AVAILABLE,
            "qiskit_legacy": QISKIT_LEGACY if QISKIT_AVAILABLE else None,
        }


# =============================================================================
# FederatedFeedbackCoordinator
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
        self._rng = random.Random(seed)
        self._lock = threading.RLock()

    def register_participant(
        self,
        participant_id: str,
        update: Dict[str, Any],
        sample_count: int = 1,
    ) -> None:
        with self._lock:
            self.participants[participant_id] = {
                "update": dict(update),
                "weight": max(1, int(sample_count)),
            }
            FEDERATED_SHARES.labels(source=participant_id).inc()

    def aggregate(self) -> Dict[str, Any]:
        with self._lock:
            if len(self.participants) < self.min_clients:
                return {}
            keys: set = set()
            for p in self.participants.values():
                keys.update(p["update"].keys())
            noise_scale = self.sensitivity / self.privacy_budget
            avg: Dict[str, Any] = {}
            for key in keys:
                values: List[float] = []
                weights: List[float] = []
                numeric = True
                for p in self.participants.values():
                    if key not in p["update"]:
                        continue
                    v = p["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(float(p["weight"]))
                    else:
                        numeric = False
                        values.append(v)
                if not values:
                    continue
                if numeric:
                    wsum = sum(weights)
                    mean = (
                        sum(v * w for v, w in zip(values, weights)) / wsum
                        if wsum > 0 else float(sum(values) / len(values))
                    )
                    mean += self._rng.gauss(0.0, noise_scale)
                    avg[key] = mean
                else:
                    avg[key] = values[0]
            self.round_id += 1
            return avg

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_participant_count(self) -> int:
        with self._lock:
            return len(self.participants)


# =============================================================================
# MultiAgentFeedbackCoordinator
# =============================================================================
class MultiAgentFeedbackCoordinator:
    """
    Each agent has its own reputation. Votes are combined by reputation, and
    outcomes update the reputation of the agents that voted for the winning
    policy. Per-policy reward is tracked separately.
    """

    def __init__(self, agents: Optional[List[str]] = None):
        self.agents = agents or [
            "throughput_agent", "cost_agent", "carbon_agent", "reliability_agent",
        ]
        self.reputation: Dict[str, float] = {a: 0.5 for a in self.agents}
        self.contributions: Dict[str, int] = {a: 0 for a in self.agents}
        self.policy_rewards: Dict[str, float] = defaultdict(float)
        self.policy_usage: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()

    def _score_agent_policy(self, agent: str, policy: str, context: Dict[str, Any]) -> float:
        """Return the agent's preference score for the given policy."""
        queue = float(context.get("queue_size", 0))
        cb_open = context.get("circuit_breaker_state") == "open"
        if agent == "throughput_agent":
            return {"immediate": 0.5, "small_batch": 0.3}.get(policy, 0.0)
        if agent == "cost_agent":
            return {"small_batch": 0.5, "large_batch": 0.2}.get(policy, 0.0)
        if agent == "carbon_agent":
            # Under high load, delay is preferred; else large batch
            if queue > 50:
                return {"delay": 0.5}.get(policy, 0.0)
            return {"large_batch": 0.3}.get(policy, 0.0)
        if agent == "reliability_agent":
            if cb_open:
                return {"delay": 0.6}.get(policy, 0.0)
            return {"immediate": 0.3, "small_batch": 0.4}.get(policy, 0.0)
        return 0.0

    def vote(self, context: Dict[str, Any], policies: List[str]) -> Tuple[str, List[str]]:
        with self._lock:
            scores: Dict[str, float] = {p: 0.0 for p in policies}
            voters: Dict[str, List[str]] = {p: [] for p in policies}
            for agent in self.agents:
                rep = self.reputation.get(agent, 0.5)
                for p in policies:
                    s = self._score_agent_policy(agent, p, context)
                    if s > 0:
                        scores[p] += rep * s
                        voters[p].append(agent)
            if not scores:
                return policies[0], []
            best = max(scores, key=scores.get)
            for a in voters[best]:
                self.contributions[a] = self.contributions.get(a, 0) + 1
            return best, voters[best]

    def record_outcome(self, voters: List[str], policy: str, success: bool) -> None:
        alpha = 0.2
        with self._lock:
            for agent in voters:
                if agent not in self.reputation:
                    continue
                prev = self.reputation[agent]
                self.reputation[agent] = prev + alpha * ((1.0 if success else 0.0) - prev)
            self.policy_usage[policy] += 1
            n = self.policy_usage[policy]
            r = self.policy_rewards[policy]
            self.policy_rewards[policy] = r + ((1.0 if success else 0.0) - r) / n

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "reputation": {k: round(v, 3) for k, v in self.reputation.items()},
                "contributions": dict(self.contributions),
                "policy_rewards": dict(self.policy_rewards),
                "policy_usage": dict(self.policy_usage),
            }


# =============================================================================
# FormalSafetyMonitor
# =============================================================================
class FormalSafetyMonitor:
    """
    Temporal-logic-like invariants. Returns (safe, reason).

    Invariants:
      1. G(queue_size > max -> policy == immediate)
      2. G(circuit_open -> policy in {delay})
      3. G(consecutive_failures > threshold -> policy == delay)
    """

    def __init__(
        self,
        max_queue_size: int = 50,
        max_consecutive_failures: int = 3,
        max_violations: int = 1000,
    ):
        self.max_queue_size = int(max_queue_size)
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.consecutive_failures = 0
        self.violations: deque = deque(maxlen=int(max_violations))
        self._lock = threading.RLock()

    def check(self, context: Dict[str, Any], policy: str) -> Tuple[bool, str]:
        with self._lock:
            qs = float(context.get("queue_size", 0))
            if qs > self.max_queue_size and policy != "immediate":
                self._record("max_queue_size", {"queue_size": qs, "policy": policy})
                return False, "max_queue_size_requires_immediate"

            if context.get("circuit_breaker_state") == "open" and policy != "delay":
                self._record("circuit_open_no_send", {"policy": policy})
                return False, "circuit_open_requires_delay"

            if self.consecutive_failures > self.max_consecutive_failures and policy != "delay":
                self._record("consecutive_failures_delay",
                             {"failures": self.consecutive_failures, "policy": policy})
                return False, "consecutive_failures_requires_delay"

            return True, "ok"

    def record_failure(self) -> None:
        with self._lock:
            self.consecutive_failures += 1

    def record_success(self) -> None:
        with self._lock:
            self.consecutive_failures = 0

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
            return list(self.violations)[-10:]


# =============================================================================
# XAIExplainer
# =============================================================================
class XAIExplainer:
    def explain_policy(
        self,
        policy: str,
        context: Dict[str, Any],
        feature_weights: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        feature_weights = feature_weights or {}
        raw = {
            "queue_size": float(context.get("queue_size", 0)) * 0.1,
            "hour_proximity": abs(12 - float(context.get("hour", 12))) * 0.05,
            "day_of_week": float(context.get("day_of_week", 0)) * 0.02,
            "circuit_open": 0.5 if context.get("circuit_breaker_state") == "open" else 0.0,
        }
        contributions = {
            k: raw.get(k, 0.0) + float(feature_weights.get(k, 0.0))
            for k in set(raw) | set(feature_weights)
        }
        ranked = sorted(contributions.items(), key=lambda kv: -abs(kv[1]))[:3]
        narrative_parts = [f"Policy '{policy}' selected."]
        for name, value in ranked:
            sign = "+" if value >= 0 else "-"
            narrative_parts.append(f"{name}={sign}{abs(value):.3f}")
        narrative_parts.append(f"(total={sum(contributions.values()):.3f})")
        return {
            "policy": policy,
            "top_contributions": [{"feature": n, "value": v} for n, v in ranked],
            "total_attribution": float(sum(contributions.values())),
            "narrative": " | ".join(narrative_parts),
        }

    def explain_policy_str(
        self,
        policy: str,
        context: Dict[str, Any],
        feature_weights: Optional[Dict[str, float]] = None,
    ) -> str:
        return self.explain_policy(policy, context, feature_weights)["narrative"]


# =============================================================================
# FlexGenPrecisionPolicy
# =============================================================================
class FlexGenPrecisionPolicy:
    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = float(default_carbon_intensity)

    def recommend(
        self, workload_size: str = "medium", carbon_intensity: Optional[float] = None
    ) -> str:
        ci = float(carbon_intensity) if carbon_intensity is not None else self.default_carbon_intensity
        if ci > 500 or workload_size == "large":
            precision = "int8"
        elif ci > 300 or workload_size == "medium":
            precision = "fp16"
        else:
            precision = "fp32"
        PRECISION_SELECTIONS.labels(precision=precision).inc()
        return precision


# =============================================================================
# CarbonOffsetBroker
# =============================================================================
class CarbonOffsetBroker:
    def __init__(self, threshold_kg: float = 1.0, cost_per_kg: float = 0.1):
        self.threshold_kg = float(threshold_kg)
        self.cost_per_kg = float(cost_per_kg)
        self.total_offset_kg = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    async def purchase_offsets(self, carbon_kg: float) -> Dict[str, Any]:
        if carbon_kg <= 0:
            return {"status": "invalid_amount"}
        if carbon_kg < self.threshold_kg:
            return {"status": "below_threshold", "carbon_kg": float(carbon_kg)}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += float(carbon_kg)
            self.total_cost += float(cost)
            rec = {
                "status": "offset_purchased",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": _utc_now().isoformat(),
            }
            self.records.append(rec)
        CARBON_OFFSETS.labels(status="offset").inc()
        return rec

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_cost_usd": self.total_cost,
            }


# =============================================================================
# ChaosMonkey
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {
        "record_feedback", "batch_processor", "cost_function", "human_approval",
    }

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

    def maybe_fail(self, component: str = "feedback") -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        self.injected_failures += 1
        self.events.append({
            "component": component,
            "timestamp": _utc_now().isoformat(),
        })
        CHAOS_EXPERIMENTS.labels(fault=component).inc()
        raise ChaosExperimentError(f"Simulated chaos failure in {component}")

    def get_stats(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "failure_probability": self.failure_probability,
            "injected_failures": self.injected_failures,
            "recent_events": self.events[-5:],
        }


# =============================================================================
# RealHumanApprovalManager
# =============================================================================
@dataclass
class HumanApprovalEntry:
    decision_id: str
    details: Dict[str, Any]
    created_at: datetime
    event: asyncio.Event


class RealHumanApprovalManager:
    """Event-based HITL approval with TTL, capacity, and audit log."""

    def __init__(
        self,
        timeout_seconds: float = 30.0,
        ttl_seconds: float = 300.0,
        max_pending: int = 100,
    ):
        self.timeout_seconds = float(timeout_seconds)
        self.ttl_seconds = float(ttl_seconds)
        self.max_pending = int(max_pending)
        self._events: Dict[str, HumanApprovalEntry] = {}
        self._responses: Dict[str, bool] = {}
        self._audit: List[Dict[str, Any]] = []
        self._received = 0
        self._lock = asyncio.Lock()

    async def _evict_expired_locked(self) -> None:
        now = _utc_now()
        for did, entry in list(self._events.items()):
            if now - entry.created_at > timedelta(seconds=self.ttl_seconds):
                self._events.pop(did, None)
                self._responses.pop(did, None)

    async def request_approval(
        self,
        decision_id: str,
        details: Dict[str, Any],
        publisher: Optional[Callable[[str], Any]] = None,
    ) -> bool:
        async with self._lock:
            await self._evict_expired_locked()
            entry = self._events.get(decision_id)
            if entry is None:
                if len(self._events) >= self.max_pending:
                    HUMAN_REVIEWS.labels(status="rejected_capacity").inc()
                    return False
                entry = HumanApprovalEntry(
                    decision_id=decision_id,
                    details=dict(details),
                    created_at=_utc_now(),
                    event=asyncio.Event(),
                )
                self._events[decision_id] = entry
                HUMAN_REVIEWS.labels(status="pending").inc()

        if publisher is not None:
            try:
                await publisher(json.dumps({
                    "decision_id": decision_id, "details": details,
                }, default=str))
            except Exception as e:
                logger.warning(f"hitl_publish_failed error={e}")

        try:
            await asyncio.wait_for(entry.event.wait(), timeout=self.timeout_seconds)
        except asyncio.TimeoutError:
            async with self._lock:
                self._events.pop(decision_id, None)
                self._responses.pop(decision_id, None)
            HUMAN_REVIEWS.labels(status="timeout").inc()
            return False

        async with self._lock:
            approved = self._responses.pop(decision_id, False)
            self._events.pop(decision_id, None)
            self._audit.append({
                "decision_id": decision_id,
                "approved": bool(approved),
                "timestamp": _utc_now().isoformat(),
            })
        HUMAN_REVIEWS.labels(status="approved" if approved else "rejected").inc()
        return bool(approved)

    async def record_response(self, decision_id: str, approved: bool) -> bool:
        async with self._lock:
            entry = self._events.get(decision_id)
            if entry is None:
                return False
            self._responses[decision_id] = bool(approved)
            self._received += 1
            entry.event.set()
            return True

    def get_stats(self) -> Dict[str, Any]:
        return {
            "pending": len(self._events),
            "responses_received": self._received,
            "audit_entries": len(self._audit),
        }

    async def close(self) -> None:
        async with self._lock:
            for entry in self._events.values():
                entry.event.set()
            self._events.clear()
            self._responses.clear()


# =============================================================================
# ActiveLearningRLHF
# =============================================================================
class ActiveLearningRLHF:
    def __init__(self, rlhf: Optional[Any] = None):
        self.rlhf = rlhf
        self.feedback_count = 0
        self._lock = threading.RLock()

    def record_human_preference(
        self,
        context: Dict[str, Any],
        approved_action: str,
        rejected_action: Optional[str] = None,
    ) -> None:
        with self._lock:
            self.feedback_count += 1
        if self.rlhf is None:
            return
        has_pref = hasattr(self.rlhf, "add_human_feedback")
        has_upd = hasattr(self.rlhf, "update")

        if has_pref and rejected_action is not None and rejected_action != approved_action:
            try:
                self.rlhf.add_human_feedback(context, approved_action, rejected_action)
            except Exception as e:
                logger.warning(f"rlhf_add_human_feedback_failed error={e}")
            return

        if has_upd:
            try:
                self.rlhf.update(context, approved_action, 1.0)
                if rejected_action is not None and rejected_action != approved_action:
                    self.rlhf.update(context, rejected_action, -1.0)
            except Exception:
                pass

    def get_stats(self) -> Dict[str, Any]:
        return {"feedback_count": self.feedback_count}


# =============================================================================
# Circuit breaker + Bulkhead
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_success_threshold = 2
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
        self._metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    def _precheck(self) -> None:
        with self._lock:
            if (
                self._state == CircuitBreakerState.OPEN
                and self._last_failure_time is not None
                and time.time() - self._last_failure_time >= self.recovery_timeout
            ):
                self._state = CircuitBreakerState.HALF_OPEN
                self._success_count = 0
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0.5)
            if self._state == CircuitBreakerState.OPEN:
                raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")

    def _record_success(self) -> None:
        with self._lock:
            self._metrics["successful_calls"] += 1
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
            else:
                self._failure_count = 0

    def _record_failure(self) -> None:
        with self._lock:
            self._metrics["failed_calls"] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state in (CircuitBreakerState.CLOSED, CircuitBreakerState.HALF_OPEN):
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)

    async def call(self, func, *args, **kwargs):
        self._precheck()
        self._metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                **self._metrics,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
            }


class GlobalCircuitBreaker:
    _instance: Optional["GlobalCircuitBreaker"] = None
    _breakers: Dict[str, CircuitBreaker] = {}
    _lock = threading.RLock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name: str, **kwargs) -> CircuitBreaker:
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, **kwargs)
            return self._breakers[name]

    def all_metrics(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {n: cb.get_metrics() for n, cb in self._breakers.items()}


class Bulkhead:
    def __init__(self, max_concurrency: int = 10):
        self.max_concurrency = int(max_concurrency)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._lock = asyncio.Lock()
        self.active = 0
        self.queued = 0

    async def execute(self, func: Callable, *args, **kwargs):
        async with self._lock:
            self.queued += 1
        async with self.semaphore:
            async with self._lock:
                self.queued -= 1
                self.active += 1
                BULKHEAD_ACTIVE.set(self.active)
                BULKHEAD_QUEUED.set(self.queued)
            try:
                return await func(*args, **kwargs)
            finally:
                async with self._lock:
                    self.active -= 1
                    BULKHEAD_ACTIVE.set(self.active)

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "active": self.active,
            "queued": self.queued,
            "max_concurrency": self.max_concurrency,
        }


# =============================================================================
# FeedbackRecorder
# =============================================================================
class FeedbackRecorder:
    """Enhanced feedback recorder with all advanced enhancements."""

    def __init__(
        self,
        config: Optional[Union[FeedbackConfig, Dict[str, Any]]] = None,
        storage: Optional[Storage] = None,
    ):
        if config is None:
            self.config = FeedbackConfig()
        elif isinstance(config, FeedbackConfig):
            self.config = config
        else:
            self.config = FeedbackConfig(**config)

        self.storage = storage
        self.instance_id = str(uuid.uuid4())[:8]

        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "cost_function",
            failure_threshold=self.config.general.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.general.circuit_breaker_recovery_timeout,
        )
        self.bulkhead = Bulkhead(max_concurrency=self.config.general.bulkhead_max_concurrency)

        self._batch_queue: Optional[asyncio.Queue] = None
        self._batch_task: Optional[asyncio.Task] = None
        self._evolution_task: Optional[asyncio.Task] = None
        self._running = False
        self._shutdown_event: Optional[asyncio.Event] = None
        self._startup_lock: Optional[asyncio.Lock] = None

        self._health_cache: Optional[bool] = None
        self._health_cache_time: Optional[datetime] = None
        self._health_ttl = timedelta(seconds=30)

        self.last_context: Dict[str, Any] = {}
        self._last_policy: str = "immediate"
        self._last_policy_by_decision: Dict[str, str] = {}

        # Optimizer modules
        if ENHANCEMENTS_AVAILABLE and self.config.optimizer.enabled:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            if self.config.optimizer.causal_bandit_enabled:
                self.bandit = CausalBandit(
                    action_space=self.config.optimizer.batch_policies,
                    fallback_solver=lambda ctx: "immediate",
                    min_trials_before_bandit=self.config.optimizer.bandit_min_trials,
                    confidence_threshold=self.config.optimizer.bandit_confidence_threshold,
                    feature_dim=self.config.optimizer.bandit_feature_dim,
                )
            else:
                self.bandit = ContextualBandit(
                    action_space=self.config.optimizer.batch_policies,
                    fallback_solver=lambda ctx: "immediate",
                    min_trials_before_bandit=self.config.optimizer.bandit_min_trials,
                    confidence_threshold=self.config.optimizer.bandit_confidence_threshold,
                )
            self.param_population = [{
                "max_retry_attempts": self.config.general.max_retry_attempts,
                "circuit_breaker_failure_threshold": self.config.general.circuit_breaker_failure_threshold,
                "batch_interval_seconds": self.config.general.batch_interval_seconds,
            }]
            self.param_rewards: deque = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.param_population: List[Dict[str, Any]] = []
            self.param_rewards = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.config.optimizer.limit_graph_enabled:
            self.limit_graph = LimitGraph()
            self.limit_graph.build_graph([], [])
        else:
            self.limit_graph = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.config.optimizer.rlhf_enabled:
            self.rlhf = RLHFOptimizer(action_space=self.config.optimizer.batch_policies)
        else:
            self.rlhf = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._bandit_teacher, self._modp_teacher, self._static_teacher,
            ])
        else:
            self.distiller = None

        # Advanced modules
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=self.config.optimizer.enable_distillation,
            qaoa_reps=self.config.optimizer.qaoa_reps,
        )
        self.dp = (
            DifferentialPrivacy(
                clip_norm=self.config.optimizer.dp_clip_norm,
                noise_multiplier=self.config.optimizer.dp_noise_multiplier,
                delta=self.config.optimizer.dp_delta,
                budget_epsilon=self.config.optimizer.dp_budget_epsilon,
            ) if self.config.optimizer.enable_dp else None
        )
        self.federated_coordinator = (
            FederatedFeedbackCoordinator(
                privacy_budget=self.config.optimizer.federated_privacy_budget,
                sensitivity=self.config.optimizer.federated_sensitivity,
            ) if self.config.optimizer.enable_federated else None
        )
        self.multi_agent = (
            MultiAgentFeedbackCoordinator() if self.config.optimizer.enable_multi_agent else None
        )
        self.safety_monitor = FormalSafetyMonitor(
            max_queue_size=self.config.optimizer.safety_max_queue_size,
            max_consecutive_failures=self.config.optimizer.safety_max_consecutive_failures,
        )
        self.xai = XAIExplainer() if self.config.optimizer.enable_xai else None
        self.precision_policy = (
            FlexGenPrecisionPolicy(
                default_carbon_intensity=self.config.optimizer.default_carbon_intensity,
            ) if self.config.optimizer.enable_precision_switch else None
        )
        self.carbon_broker = CarbonOffsetBroker(
            threshold_kg=self.config.optimizer.carbon_offset_threshold_kg,
            cost_per_kg=self.config.optimizer.carbon_offset_cost_per_kg,
        )
        self.chaos_monkey = ChaosMonkey(
            enabled=self.config.optimizer.chaos_enabled,
            failure_probability=self.config.optimizer.chaos_failure_probability,
            seed=self.config.optimizer.chaos_seed,
        )
        self.human_approval = (
            RealHumanApprovalManager(
                timeout_seconds=self.config.optimizer.human_review_timeout_seconds,
                ttl_seconds=self.config.optimizer.human_review_ttl_seconds,
                max_pending=self.config.optimizer.human_review_max_pending,
            ) if self.config.optimizer.human_review_enabled else None
        )
        self.active_learning = ActiveLearningRLHF(rlhf=self.rlhf)

        self._load_state()
        logger.info(f"feedback_recorder_initialized version=16.1 instance={self.instance_id}")

    async def __aenter__(self) -> "FeedbackRecorder":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        if self._startup_lock is None:
            self._startup_lock = asyncio.Lock()
        async with self._startup_lock:
            if self._running:
                return
            self._batch_queue = asyncio.Queue(maxsize=self.config.general.batch_queue_maxsize)
            self._shutdown_event = asyncio.Event()
            self._running = True
            self._batch_task = asyncio.create_task(
                self._batch_processor_loop(), name="feedback_batch"
            )
            if ENHANCEMENTS_AVAILABLE:
                self._evolution_task = asyncio.create_task(
                    self._evolution_loop(), name="feedback_evolution"
                )
            logger.info(
                f"feedback_recorder_started tasks="
                f"{int(self._batch_task is not None) + int(self._evolution_task is not None)}"
            )

    async def shutdown(self) -> None:
        if not self._running:
            return
        logger.info("feedback_recorder_shutting_down")
        self._running = False
        if self._shutdown_event is not None:
            self._shutdown_event.set()

        for task in (self._batch_task, self._evolution_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._batch_task = None
        self._evolution_task = None

        # Flush remaining records
        if self._batch_queue is not None:
            remaining: List[FeedbackRecord] = []
            while not self._batch_queue.empty():
                try:
                    rec = self._batch_queue.get_nowait()
                    remaining.append(rec)
                    self._batch_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            if remaining:
                await self._send_batch(remaining, precision="fp32")

        if self.human_approval is not None:
            with contextlib.suppress(Exception):
                await self.human_approval.close()
        self._save_state()
        logger.info("feedback_recorder_shutdown_complete")

    # ------------------------------------------------------------------ #
    # State persistence                                                  #
    # ------------------------------------------------------------------ #
    def _load_state(self) -> None:
        if self.storage is None:
            return
        try:
            loader = getattr(self.storage, "get_feedback_recorder_state", None)
            if loader is None:
                return
            state = loader()
            if not state:
                return
            if "param_population" in state and isinstance(state["param_population"], list):
                self.param_population = state["param_population"]
            if "param_rewards" in state and isinstance(state["param_rewards"], list):
                self.param_rewards = deque(state["param_rewards"], maxlen=100)
        except Exception as e:
            logger.warning(f"feedback_recorder_state_load_failed error={e}")

    def _save_state(self) -> None:
        if self.storage is None:
            return
        try:
            saver = getattr(self.storage, "save_feedback_recorder_state", None)
            if saver is None:
                return
            saver({
                "param_population": list(self.param_population),
                "param_rewards": list(self.param_rewards),
            })
        except Exception as e:
            logger.warning(f"feedback_recorder_state_save_failed error={e}")

    # ------------------------------------------------------------------ #
    # Teachers                                                           #
    # ------------------------------------------------------------------ #
    def _bandit_teacher(self, context: Dict[str, Any]) -> str:
        if self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            policy, _, _ = self.bandit.select_action(encoded)
            return policy or "immediate"
        return "immediate"

    def _modp_teacher(self, context: Dict[str, Any]) -> str:
        if not self.modp:
            return "immediate"
        objectives = {
            "importance": 0.5,
            "urgency": 0.5,
            "energy": float(context.get("queue_size", 0)) / 1000.0,
            "latency": 1.0 if context.get("circuit_breaker_state") == "open" else 0.1,
        }
        utilities: Dict[str, float] = {}
        for policy in self.config.optimizer.batch_policies:
            if policy == "immediate":
                obj = {**objectives, "latency": 0.2}
            elif policy == "small_batch":
                obj = {**objectives, "latency": 0.5}
            elif policy == "large_batch":
                obj = {**objectives, "latency": 0.7}
            else:
                obj = {**objectives, "latency": 0.9}
            utilities[policy] = float(self.modp.evaluate(obj, self.config.optimizer.modp_weights))
        return max(utilities, key=utilities.get)

    def _static_teacher(self, context: Dict[str, Any]) -> str:
        return "immediate"

    # ------------------------------------------------------------------ #
    # Public feedback API                                                #
    # ------------------------------------------------------------------ #
    async def record_feedback(
        self,
        context: Union[Dict[str, Any], FeedbackContext],
        metrics: Union[Dict[str, Any], FeedbackMetrics],
        teacher_id: Optional[str] = None,
        distillation_loss: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> bool:
        # Auto-start if the caller forgot to call start().
        if not self._running:
            await self.start()

        # Chaos injection
        fault = self.chaos_monkey.maybe_fail("record_feedback")
        if fault is not None:
            logger.warning(f"chaos_injected component=record_feedback")

        # Coerce to models
        if isinstance(context, FeedbackContext):
            ctx = context
        else:
            ctx = FeedbackContext(**context)
        if isinstance(metrics, FeedbackMetrics):
            mets = metrics
        else:
            mets = FeedbackMetrics(**metrics)
        if teacher_id is not None:
            mets.teacher_id = teacher_id
        if distillation_loss is not None:
            mets.distillation_loss = distillation_loss

        record = FeedbackRecord(
            context=ctx,
            metrics=mets,
            correlation_id=correlation_id or _get_correlation_id(),
        )

        # Drop low-utility records when the queue is nearly full.
        if self.modp is not None:
            objectives = {
                "importance": 0.5,
                "urgency": 0.5,
                "energy": mets.energy_joules / 1000.0,
                "latency": mets.latency_ms / 1000.0,
            }
            utility = float(self.modp.evaluate(objectives, self.config.optimizer.modp_weights))
            if (
                utility < 0.2
                and self._batch_queue.qsize()
                > self.config.general.batch_max_size * 0.8
            ):
                logger.debug(f"dropping_low_utility_feedback utility={utility:.2f}")
                QUEUE_DROPPED.inc()
                return False

        try:
            self._batch_queue.put_nowait(record)
            QUEUE_SIZE.set(self._batch_queue.qsize())
            return True
        except asyncio.QueueFull:
            QUEUE_DROPPED.inc()
            logger.warning("feedback_queue_full_dropping")
            return False

    async def submit_human_feedback(self, decision_id: str, approved: bool) -> bool:
        if self.human_approval is None:
            return False
        accepted = await self.human_approval.record_response(decision_id, approved)
        if not accepted:
            logger.warning(f"hitl_unknown_decision decision_id={decision_id}")
            return False
        if approved:
            policy = self._last_policy_by_decision.get(decision_id, "immediate")
            self.active_learning.record_human_preference(
                context={"decision_id": decision_id},
                approved_action=policy,
                rejected_action=None,
            )
        return True

    # ------------------------------------------------------------------ #
    # Batch processor                                                    #
    # ------------------------------------------------------------------ #
    async def _batch_processor_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                context = {
                    "queue_size": self._batch_queue.qsize(),
                    "circuit_breaker_state": self.circuit_breaker._state.value,
                    "hour": datetime.now().hour,
                    "day_of_week": datetime.now().weekday(),
                }
                self.last_context = context

                policy, source, explanation, feature_weights = await self._select_policy(context)

                # Safety gate
                safe, reason = self.safety_monitor.check(context, policy)
                if not safe:
                    logger.warning(f"safety_override reason={reason}")
                    policy = "delay"
                    source = "safety_override"
                    explanation = f"Policy overridden by formal safety monitor: {reason}"

                # HITL gate
                if (
                    self.human_approval is not None
                    and context["queue_size"] > self.config.general.human_review_threshold_queue_size
                ):
                    decision_id = f"batch_{uuid.uuid4().hex[:8]}"
                    self._last_policy_by_decision[decision_id] = policy
                    approved = await self.human_approval.request_approval(
                        decision_id,
                        {
                            "policy": policy,
                            "context": context,
                            "explanation": explanation,
                        },
                    )
                    if not approved:
                        logger.info("hitl_denied_using_safe_default")
                        policy = "small_batch"
                        source = "human_override"

                # Precision
                precision = "fp32"
                if self.precision_policy is not None:
                    workload = (
                        "large" if context["queue_size"] > 50
                        else "medium" if context["queue_size"] > 10
                        else "small"
                    )
                    precision = self.precision_policy.recommend(workload_size=workload)

                batch_size, interval = self._policy_to_params(policy)

                if self.limit_graph:
                    limits = self.limit_graph.get_limits(context)
                    if limits.get("max_batch_size"):
                        batch_size = min(batch_size, int(limits["max_batch_size"]))
                    if limits.get("max_interval"):
                        interval = min(interval, int(limits["max_interval"]))
                    if limits.get("min_interval"):
                        interval = max(interval, int(limits["min_interval"]))

                # Drain up to batch_size records within 0.1s.
                records: List[FeedbackRecord] = []
                for _ in range(batch_size):
                    try:
                        rec = await asyncio.wait_for(self._batch_queue.get(), timeout=0.1)
                        records.append(rec)
                        self._batch_queue.task_done()
                    except asyncio.TimeoutError:
                        break

                if records:
                    BATCH_SIZE.set(len(records))
                    success = await self._send_batch(records, precision)

                    if success:
                        self.safety_monitor.record_success()
                    else:
                        self.safety_monitor.record_failure()

                    reward = 1.0 if success else -1.0
                    encoded = self.moe.encode(context) if self.moe else context
                    if self.bandit is not None:
                        # Sync update — do NOT await.
                        self.bandit.update(encoded, policy, reward)
                    if self.rlhf is not None:
                        self.rlhf.update(context, policy, reward)
                    if self.limit_graph is not None:
                        self.limit_graph.update_from_feedback({
                            "context": context,
                            "policy": policy,
                            "reward": reward,
                            "success": success,
                        })
                    if self.bio is not None:
                        self.param_rewards.append(reward)
                    if self.multi_agent is not None:
                        _, voters = self.multi_agent.vote(
                            context, self.config.optimizer.batch_policies
                        )
                        self.multi_agent.record_outcome(voters, policy, success)
                    if self.federated_coordinator is not None:
                        self.federated_coordinator.register_participant(
                            participant_id=f"{self.instance_id}_{int(time.time() * 1000)}",
                            update={
                                "policy_reward": reward,
                                "queue_size": context["queue_size"],
                                "batch_size": len(records),
                            },
                            sample_count=len(records),
                        )

                    self._last_policy = policy

                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"batch_processor_error error={e}")
                await asyncio.sleep(10)

    async def _select_policy(
        self, context: Dict[str, Any]
    ) -> Tuple[str, str, str, Dict[str, float]]:
        feature_weights: Dict[str, float] = {}
        policy = "immediate"
        source = "fixed"

        if self.distiller is not None:
            policy = self.distiller.distill(context) or "immediate"
            source = "distilled"
        elif self.rlhf is not None:
            policy = self.rlhf.sample_action(context) or "immediate"
            source = "rlhf"
        elif self.bandit is not None:
            encoded = self.moe.encode(context) if self.moe else context
            policy, confidence, source = self.bandit.select_action(encoded)
            policy = policy or "immediate"
            feature_weights["bandit_confidence"] = float(confidence)

        # Multi-agent composite vote
        if self.multi_agent is not None:
            agent_policy, _ = self.multi_agent.vote(
                context, self.config.optimizer.batch_policies
            )
            if agent_policy != policy:
                feature_weights["multi_agent_override"] = 1.0
                policy = agent_policy
                source = "multi_agent"

        # Quantum override
        if self.quantum_optimizer is not None and self.quantum_optimizer.available:
            candidates = []
            for p in self.config.optimizer.batch_policies:
                candidates.append({
                    "name": p,
                    "importance": 0.5,
                    "urgency": 1.0 - (context.get("queue_size", 0) / 100.0),
                    "energy": 1.0 - (context.get("queue_size", 0) / 100.0),
                    "latency": 1.0 if context.get("circuit_breaker_state") == "open" else 0.5,
                })
            quantum_choice = self.quantum_optimizer.select_best_policy(
                candidates, self.config.optimizer.modp_weights
            )
            if quantum_choice:
                policy = quantum_choice.get("name", policy)
                source = "quantum"

        explanation = ""
        if self.xai is not None:
            explanation = self.xai.explain_policy_str(policy, context, feature_weights)
            XAI_DECISIONS.labels(policy=policy).inc()

        return policy, source, explanation, feature_weights

    def _policy_to_params(self, policy: str) -> Tuple[int, float]:
        if policy == "immediate":
            return 1, 0.5
        if policy == "small_batch":
            return 10, 1.0
        if policy == "large_batch":
            return self.config.general.batch_max_size, 5.0
        return 0, 10.0  # delay

    # ------------------------------------------------------------------ #
    # Batch sending                                                      #
    # ------------------------------------------------------------------ #
    async def _send_batch(
        self, records: List[FeedbackRecord], precision: str = "fp32"
    ) -> bool:
        if not records:
            return True

        payload = [self._record_to_dict(r) for r in records]

        # DP on numeric metric fields
        if self.dp is not None:
            for entry in payload:
                m = entry.get("metrics", {})
                numeric_keys = [
                    "predicted_cost", "actual_cost", "energy_joules",
                    "carbon_kg", "helium_units", "latency_ms", "accuracy",
                ]
                vals = [float(m.get(k, 0.0) or 0.0) for k in numeric_keys]
                try:
                    noisy = self.dp.clip_and_noise(vals)
                except SafetyViolationError as e:
                    logger.warning(f"dp_budget_block error={e}")
                    return False
                for k, v in zip(numeric_keys, noisy):
                    m[k] = float(v)

        wrapper = {
            "records": payload,
            "precision": precision,
            "dp_enabled": self.dp is not None,
            "instance_id": self.instance_id,
        }

        async def _send() -> bool:
            if not AIOHTTP_AVAILABLE:
                raise CostFunctionUnavailableError("aiohttp is not installed")
            async with aiohttp.ClientSession() as session:
                headers = {"Content-Type": "application/json"}
                if self.config.cost_function.api_key:
                    headers["Authorization"] = f"Bearer {self.config.cost_function.api_key}"
                async with session.post(
                    self.config.cost_function.endpoint_url,
                    json=wrapper,
                    headers=headers,
                    timeout=self.config.cost_function.timeout_seconds,
                ) as resp:
                    if resp.status >= 400:
                        raise CostFunctionUnavailableError(
                            f"cost_function_returned_status status={resp.status}"
                        )
                    return True

        async def _send_with_bulkhead() -> bool:
            return await self.bulkhead.execute(_send)

        if TENACITY_AVAILABLE:
            @retry(
                stop=stop_after_attempt(self.config.general.max_retry_attempts),
                wait=wait_exponential(
                    multiplier=1,
                    min=self.config.general.retry_wait_seconds,
                    max=10,
                ),
                retry=retry_if_exception_type(
                    (CostFunctionUnavailableError, asyncio.TimeoutError)
                ),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )
            async def _send_with_retry() -> bool:
                return await _send_with_bulkhead()
        else:
            async def _send_with_retry() -> bool:
                return await _send_with_bulkhead()

        try:
            start = time.monotonic()
            result = await self.circuit_breaker.call(_send_with_retry)
            latency = time.monotonic() - start
            FEEDBACK_RECORDS.labels(status="success").inc(len(records))
            FEEDBACK_LATENCY.observe(latency)
            logger.info(
                f"feedback_batch_sent records={len(records)} precision={precision} "
                f"latency_s={latency:.3f}"
            )

            # Carbon offset for transmission emissions
            carbon_kg = len(records) * 0.0001
            with contextlib.suppress(Exception):
                await self.carbon_broker.purchase_offsets(carbon_kg)

            audit_logger.info(
                f"feedback_batch_sent records={len(records)} precision={precision}"
            )
            return bool(result)
        except CircuitBreakerOpenError:
            FEEDBACK_RECORDS.labels(status="circuit_open").inc(len(records))
            logger.error("circuit_breaker_open dropping_batch")
            return False
        except Exception as e:
            FEEDBACK_RECORDS.labels(status="error").inc(len(records))
            logger.error(f"feedback_batch_send_failed error={e}")
            return False

    def _record_to_dict(self, record: FeedbackRecord) -> Dict[str, Any]:
        return {
            "context": _model_to_dict(record.context),
            "metrics": _model_to_dict(record.metrics),
            "timestamp": record.timestamp.isoformat(),
            "correlation_id": record.correlation_id,
        }

    # ------------------------------------------------------------------ #
    # Bio-inspired evolution                                             #
    # ------------------------------------------------------------------ #
    async def _evolve_parameters(self) -> None:
        if not self.bio or not self.param_population:
            return
        if len(self.param_rewards) < 10:
            return

        def fitness(params: Dict[str, Any]) -> float:
            return float(sum(self.param_rewards) / len(self.param_rewards)) if self.param_rewards else 0.0

        new_population = self.bio.evolve(
            population=self.param_population,
            fitness_fn=fitness,
            generations=self.config.optimizer.bio_generations,
            population_size=self.config.optimizer.bio_population_size,
        )
        if new_population:
            self.param_population = new_population
            best = max(new_population, key=fitness)
            self.config.general.max_retry_attempts = int(
                best.get("max_retry_attempts", self.config.general.max_retry_attempts)
            )
            self.config.general.circuit_breaker_failure_threshold = int(
                best.get(
                    "circuit_breaker_failure_threshold",
                    self.config.general.circuit_breaker_failure_threshold,
                )
            )
            self.config.general.batch_interval_seconds = float(
                best.get("batch_interval_seconds", self.config.general.batch_interval_seconds)
            )
            self._save_state()
            logger.info(f"parameters_evolved best={best}")

    async def _evolution_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.optimizer.evolve_interval_seconds)
                await self._evolve_parameters()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"evolution_loop_error error={e}")

    # ------------------------------------------------------------------ #
    # Health and stats                                                   #
    # ------------------------------------------------------------------ #
    async def health_check(self) -> Dict[str, Any]:
        now = _utc_now()
        if (
            self._health_cache is not None
            and self._health_cache_time is not None
            and now - self._health_cache_time < self._health_ttl
        ):
            return {"healthy": self._health_cache, "cached": True}

        healthy = False
        if AIOHTTP_AVAILABLE:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(
                        self.config.cost_function.endpoint_url,
                        timeout=min(5.0, self.config.cost_function.timeout_seconds),
                    ) as resp:
                        healthy = resp.status < 500
            except Exception as e:
                logger.warning(f"health_check_failed error={e}")
                healthy = False
        self._health_cache = healthy
        self._health_cache_time = now
        return {"healthy": healthy, "cached": False}

    async def get_stats(self) -> Dict[str, Any]:
        queue_size = self._batch_queue.qsize() if self._batch_queue is not None else 0
        optimizer_stats: Dict[str, Any] = {
            "bandit": self.bandit.get_stats() if isinstance(self.bandit, CausalBandit) else None,
            "causal_bandit": isinstance(self.bandit, CausalBandit),
            "modp_weights": dict(self.config.optimizer.modp_weights),
            "param_population_size": len(self.param_population),
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "limit_graph_active": self.limit_graph is not None,
            "rlhf_active": self.rlhf is not None,
            "distillation_active": self.distiller is not None,
        }
        advanced: Dict[str, Any] = {
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "dp_enabled": self.dp is not None,
            "dp_accountant": self.dp.get_accountant().snapshot() if self.dp else None,
            "federated_participants": (
                self.federated_coordinator.get_participant_count()
                if self.federated_coordinator else 0
            ),
            "multi_agent": self.multi_agent.get_stats() if self.multi_agent else None,
            "safety_violations": self.safety_monitor.get_violations(),
            "xai_enabled": self.xai is not None,
            "precision_policy": (
                {"default_carbon_intensity": self.precision_policy.default_carbon_intensity}
                if self.precision_policy else None
            ),
            "carbon_broker": self.carbon_broker.get_totals(),
            "chaos_monkey": self.chaos_monkey.get_stats(),
            "human_approval": self.human_approval.get_stats() if self.human_approval else None,
            "active_learning": self.active_learning.get_stats(),
        }
        return {
            "instance_id": self.instance_id,
            "config": _cfg_dict(self.config),
            "circuit_breaker": self.circuit_breaker.get_metrics(),
            "bulkhead": self.bulkhead.get_metrics(),
            "queue_size": queue_size,
            "running": self._running,
            "last_policy": self._last_policy,
            "optimizer": optimizer_stats,
            "advanced_enhancements": advanced,
        }


# =============================================================================
# Component factory
# =============================================================================
def build_components(
    config: Optional[Union[FeedbackConfig, Dict[str, Any]]] = None,
    storage: Optional[Storage] = None,
) -> Dict[str, Any]:
    recorder = FeedbackRecorder(config=config, storage=storage)
    return {"recorder": recorder}


# =============================================================================
# FastAPI app factory
# =============================================================================
def create_app(config: Optional[Union[FeedbackConfig, Dict[str, Any]]] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config if isinstance(config, FeedbackConfig) else (
        FeedbackConfig(**config) if config else FeedbackConfig()
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[valid-type]
        recorder = FeedbackRecorder(cfg)
        app.state.recorder = recorder
        await recorder.start()
        try:
            yield
        finally:
            await recorder.shutdown()

    app = FastAPI(title="Feedback Recorder API", version="16.1", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer(auto_error=False)

    async def require_auth(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    ) -> Dict[str, Any]:
        if credentials is None or not JWT_AVAILABLE:
            raise HTTPException(status_code=401, detail="auth required")
        try:
            return pyjwt.decode(
                credentials.credentials, cfg.jwt_secret, algorithms=["HS256"]
            )
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid token: {e}")

    async def require_admin(user: Dict[str, Any] = Depends(require_auth)) -> Dict[str, Any]:
        if cfg.require_auth_for_admin and user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="admin required")
        return user

    @app.post("/feedback")
    async def record_feedback(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await recorder.record_feedback(
            context=body.get("context", {}),
            metrics=body.get("metrics", {}),
            teacher_id=body.get("teacher_id"),
            distillation_loss=body.get("distillation_loss"),
        )
        if not ok:
            raise HTTPException(status_code=503, detail="queue full or dropped")
        return {"status": "accepted"}

    @app.get("/health")
    async def health(request: Request) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        return await recorder.health_check()

    @app.get("/stats")
    async def stats(
        request: Request,
        user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        return await recorder.get_stats()

    @app.get("/metrics")
    async def metrics() -> Response:
        if not PROMETHEUS_AVAILABLE:
            return Response(content=b"# prometheus not installed", media_type="text/plain")
        return Response(content=METRICS.render(), media_type="text/plain")

    @app.get("/optimization/status")
    async def optimization_status(
        request: Request,
        user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        return (await recorder.get_stats()).get("optimizer", {})

    @app.post("/optimization/evolve")
    async def evolve_parameters(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        if ENHANCEMENTS_AVAILABLE and recorder.bio is not None:
            await recorder._evolve_parameters()
            return {"status": "evolution triggered"}
        return {"status": "evolution not available"}

    @app.post("/optimization/rlhf-update")
    async def rlhf_update(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        if recorder.rlhf is None:
            return {"status": "RLHF not available"}
        recorder.rlhf.update(
            body.get("context", {}),
            body.get("action", "unknown"),
            float(body.get("reward", 0.0)),
        )
        return {"status": "RLHF updated"}

    @app.post("/human-review/{decision_id}/approve")
    async def human_approve(
        decision_id: str,
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await recorder.submit_human_feedback(decision_id, approved=True)
        return {"status": "approved" if ok else "unknown_decision"}

    @app.post("/human-review/{decision_id}/reject")
    async def human_reject(
        decision_id: str,
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await recorder.submit_human_feedback(decision_id, approved=False)
        return {"status": "rejected" if ok else "unknown_decision"}

    @app.post("/chaos/trigger")
    async def chaos_trigger(
        request: Request,
        enabled: bool = True,
        probability: float = 0.1,
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        recorder.chaos_monkey.enabled = bool(enabled)
        recorder.chaos_monkey.failure_probability = float(probability)
        return recorder.chaos_monkey.get_stats()

    @app.get("/carbon/totals")
    async def carbon_totals(
        request: Request,
        user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        return recorder.carbon_broker.get_totals()

    @app.post("/precision/recommend")
    async def precision_recommend(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None or recorder.precision_policy is None:
            raise HTTPException(status_code=503, detail="precision policy not available")
        return {
            "precision": recorder.precision_policy.recommend(
                workload_size=body.get("workload_size", "medium"),
                carbon_intensity=body.get("carbon_intensity"),
            )
        }

    @app.get("/safety/violations")
    async def safety_violations(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None:
            raise HTTPException(status_code=503, detail="not initialized")
        return {"violations": recorder.safety_monitor.get_violations()}

    @app.post("/federated/register")
    async def federated_register(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None or recorder.federated_coordinator is None:
            raise HTTPException(status_code=503, detail="federated coordinator not available")
        recorder.federated_coordinator.register_participant(
            body.get("participant_id", "unknown"),
            body.get("update", {}),
            sample_count=int(body.get("sample_count", 1)),
        )
        return {"status": "registered"}

    @app.get("/federated/aggregate")
    async def federated_aggregate(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ) -> Dict[str, Any]:
        recorder = getattr(request.app.state, "recorder", None)
        if recorder is None or recorder.federated_coordinator is None:
            raise HTTPException(status_code=503, detail="federated coordinator not available")
        return recorder.federated_coordinator.aggregate()

    return app


# =============================================================================
# Singleton accessor
# =============================================================================
_recorder_instance: Optional[FeedbackRecorder] = None
_recorder_lock: Optional[asyncio.Lock] = None


async def get_feedback_recorder(
    config: Optional[Union[FeedbackConfig, Dict[str, Any]]] = None,
    storage: Optional[Storage] = None,
) -> FeedbackRecorder:
    global _recorder_instance, _recorder_lock
    if _recorder_lock is None:
        _recorder_lock = asyncio.Lock()
    if _recorder_instance is None:
        async with _recorder_lock:
            if _recorder_instance is None:
                _recorder_instance = FeedbackRecorder(config=config, storage=storage)
                await _recorder_instance.start()
    return _recorder_instance


# =============================================================================
# Main entry point
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Enhanced Feedback Recorder v16.1 — Single-File Enhanced")
    print("=" * 80)

    recorder = await get_feedback_recorder()
    print("\nEnhancements:")
    print(f"  Causal RL: {ENHANCEMENTS_AVAILABLE}")
    print(f"  Additional enhancements: {ADDITIONAL_ENHANCEMENTS_AVAILABLE}")
    print(f"  Pydantic: {PYDANTIC_AVAILABLE}")
    print(f"  Quantum optimizer: {recorder.quantum_optimizer.get_status()}")

    ok = await recorder.record_feedback(
        context={"request_id": "t1", "expert_id": "e1", "node_id": "n1"},
        metrics={"predicted_cost": 0.5, "actual_cost": 0.6},
    )
    print(f"\nFeedback accepted: {ok}")

    await asyncio.sleep(1.5)

    stats = await recorder.get_stats()
    print(f"Queue size: {stats['queue_size']}")
    print(f"Circuit breaker: {stats['circuit_breaker']['state']}")
    print(f"Optimizer: causal_bandit={stats['optimizer']['causal_bandit']}")
    print(f"Safety violations: {len(stats['advanced_enhancements']['safety_violations'])}")

    health = await recorder.health_check()
    print(f"Health: {health}")

    await recorder.shutdown()
    print("\nShutdown complete")


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()


__all__ = [
    "FeedbackError",
    "CircuitBreakerOpenError",
    "CostFunctionUnavailableError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "NotImplementedYetError",
    "FeedbackContext",
    "FeedbackMetrics",
    "FeedbackRecord",
    "GeneralConfig",
    "CostFunctionConfig",
    "OptimizerConfig",
    "FeedbackConfig",
    "CausalBandit",
    "BanditConfig",
    "QuantumDistillationOptimizer",
    "DifferentialPrivacy",
    "PrivacyAccountant",
    "FederatedFeedbackCoordinator",
    "MultiAgentFeedbackCoordinator",
    "FormalSafetyMonitor",
    "XAIExplainer",
    "FlexGenPrecisionPolicy",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "RealHumanApprovalManager",
    "HumanApprovalEntry",
    "ActiveLearningRLHF",
    "CircuitBreakerState",
    "CircuitBreaker",
    "GlobalCircuitBreaker",
    "Bulkhead",
    "FeedbackRecorder",
    "build_components",
    "create_app",
    "get_feedback_recorder",
    "main",
]
