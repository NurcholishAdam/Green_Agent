#!/usr/bin/env python3
# File: src/enhancements/fallback_manager_enhanced_v15_0.py
"""
Multi-Layered Fallback Manager for Green Agent — v15.2 (Single-File Enhanced)

Public API preserved:
    FallbackManagerConfig, GeneralConfig, QuantumConfig, CloudConfig, LLMConfig,
    SchedulerConfig, PredictiveConfig, FederatedConfig, DatabaseConfig,
    VaultConfig, APIConfig, CircuitBreakerConfig, LeaderConfig, CarbonConfig,
    WebSocketConfig, SafetyConfig, ChaosConfig, MultiAgentConfig,
    OptimizerConfig,
    CausalBandit, BanditConfig, SafetyMonitor, XAIExplainer,
    FederatedSecureCoordinator, MultiAgentCoordinator, CarbonOffsetBroker,
    ChaosMonkey, HumanReviewManager, FlexGenPrecisionPolicy,
    QuantumDistillationOptimizer, VaultManager, EnhancedDatabaseManager,
    BlockchainFallbackVerification, QuantumResilientFallbackSecurity,
    LLMFallbackGenerator, LoadShedder, MultiRegionFallbackCoordinator,
    AutonomousFallbackOptimizer, FederatedFallbackLearner,
    PredictiveFallbackReflexivity, FallbackSustainabilityTracker,
    WebSocketServer, MultiCloudStorage, LeaderElection,
    EnhancedFallbackManagerV15_0, RateLimiter, TaskManager,
    GlobalCircuitBreaker, CircuitBreaker, and the exception hierarchy.

Fixes and enhancements over v15.1:
- Import-time crashes fixed (logging.handlers, structlog addFilter guard,
  ORM models guarded by Base presence, `metadata` columns renamed).
- `_apply_migrations` moved to an explicit `async def init()` awaited by
  callers, no more create_task in `__init__`.
- `await self.bandit.update(...)` bug fixed (CausalBandit.update is sync).
- `priv.sign(data)` fixed to `mod.sign(priv, data)` (pqcrypto API).
- Real ECDSA fallback in QuantumResilientFallbackSecurity; verification now
  validates against the stored public key.
- `asyncio.get_event_loop()` replaced with `asyncio.to_thread`.
- Circuit-breaker failure-path bug fixed: the previous code called
  `cb.call(lambda: asyncio.sleep(0))` which *reset* the breaker on failure.
- LoadShedder no longer leaks `asyncio.Event`s on timeout.
- SafetyMonitor uses a plain list with explicit pruning.
- CarbonOffsetBroker.estimate_offset_cost respects the threshold.
- CausalBandit is a real linear contextual bandit with Thompson sampling.
- MultiAgentCoordinator.record_outcome takes an iterable of contributing
  agents; the manager tracks which agents contributed per decision.
- DP noise scales to `sensitivity / epsilon`.
- ChaosMonkey has named faults, seeded RNG, `apply` / `cleanup_fault`,
  `inject_and_measure`.
- HumanReviewManager has TTL, capacity, audit log.
- QuantumDistillationOptimizer tries modern qiskit first, then legacy.
- BlockchainFallbackVerification raises NotImplementedYetError when Web3
  or the contract is not configured.
- MultiCloudStorage raises NotImplementedYetError when no provider is
  configured; real uploads when credentials exist.
- PredictiveFallbackReflexivity offloads Prophet to a thread pool and
  populates hyperparam_fitness from residuals.
- LLMFallbackGenerator actually calls OpenAI when the client is available.
- AutonomousFallbackOptimizer persists state through the DB manager.
- EnhancedDatabaseManager.save_fallback_history persists every fallback.
- Real Prometheus metrics incremented in the actual paths.
- Signal handling moved into the FastAPI lifespan and `main()`.
- `create_app()` factory with lifespan; `build_components()` shared with
  `main()`.
- `__aenter__` / `__aexit__` on the manager.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import contextvars
import hashlib
import inspect
import json
import logging
import logging.handlers  # noqa: F401
import os
import random
import signal
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Deque, Dict, Iterable, List,
    Optional, Protocol, Set, Tuple, Union, runtime_checkable,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional imports (guarded)
# -----------------------------------------------------------------------------
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
                    "fallback_manager_v15.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)


def _get_correlation_id() -> str:
    cid = correlation_id_var.get()
    if not cid:
        cid = str(uuid.uuid4())[:8]
        correlation_id_var.set(cid)
    return cid


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
# Enhancement modules (guarded)
# =============================================================================
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
        def select(self, encoded): return "default"

    class ParetoOptimizer:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0.0) * weights.get(k, 1.0) for k in objectives)

    class ContextualBandit:  # type: ignore[no-redef]
        def __init__(self, action_space, fallback_solver, *a, **k):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

    class LimitGraph:  # type: ignore[no-redef]
        def __init__(self, *a, **k): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

    class RLHFOptimizer:  # type: ignore[no-redef]
        def __init__(self, action_space, *a, **k): self.actions = action_space
        def update(self, context, action, reward): pass
        def sample_action(self, context): return self.actions[0] if self.actions else None

    class MultiTeacherDistiller:  # type: ignore[no-redef]
        def __init__(self, teachers, *a, **k): self.teachers = teachers
        def distill(self, context): return self.teachers[0](context) if self.teachers else None


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


# Pydantic
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    from pydantic_settings import BaseSettings, SettingsConfigDict  # noqa: F401
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
    from tenacity import (  # noqa: F401
        AsyncRetrying, RetryError, retry, retry_if_exception_type,
        stop_after_attempt, wait_exponential, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from sqlalchemy import (
        Boolean, Column, DateTime, Float, Integer, JSON, String, Text, text,
    )
    from sqlalchemy.ext.asyncio import (
        AsyncSession, async_sessionmaker, create_async_engine,
    )
    from sqlalchemy.orm import declarative_base, scoped_session, sessionmaker
    from sqlalchemy.pool import NullPool, QueuePool  # noqa: F401
    from sqlalchemy.exc import OperationalError, SQLAlchemyError  # noqa: F401
    SQLALCHEMY_ASYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_ASYNC_AVAILABLE = False

try:
    from sqlalchemy import create_engine  # type: ignore
    from sqlalchemy.orm import sessionmaker, scoped_session  # type: ignore
    SQLALCHEMY_SYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # noqa: F401
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from web3 import Account, Web3  # noqa: F401
    from web3.middleware import geth_poa_middleware  # noqa: F401
    from web3.exceptions import ContractLogicError, TransactionNotFound  # noqa: F401
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram,
        generate_latest, start_http_server,  # noqa: F401
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # noqa: F401
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, PublicFormat,
        load_pem_private_key, load_pem_public_key,
    )
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    import websockets  # noqa: F401
    from websockets.server import serve  # noqa: F401
    from websockets.exceptions import ConnectionClosed  # noqa: F401
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from openai import AsyncOpenAI  # type: ignore
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import jwt as pyjwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    from hvac import Client as VaultClient  # noqa: F401
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

try:
    import boto3  # noqa: F401
    from botocore.exceptions import ClientError  # noqa: F401
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient  # noqa: F401
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage  # noqa: F401
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from fastapi import (
        BackgroundTasks, Depends, FastAPI, HTTPException, Request, status,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response, StreamingResponse
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False


# =============================================================================
# Exceptions
# =============================================================================
class FallbackManagerError(Exception):
    """Base class for the fallback manager."""


class QuantumError(FallbackManagerError): pass
class BlockchainError(FallbackManagerError): pass
class CircuitBreakerOpenError(FallbackManagerError): pass
class LoadSheddingError(FallbackManagerError): pass
class RateLimitExceeded(FallbackManagerError): pass
class VaultError(FallbackManagerError): pass
class CloudStorageError(FallbackManagerError): pass
class FederatedError(FallbackManagerError): pass
class PredictiveError(FallbackManagerError): pass
class OptimizerError(FallbackManagerError): pass
class DatabaseError(FallbackManagerError): pass
class LLMError(FallbackManagerError): pass
class SafetyViolationError(FallbackManagerError): pass
class ChaosExperimentError(FallbackManagerError): pass
class NotImplementedYetError(FallbackManagerError): pass


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
FALLBACK_TRIGGERED = METRICS.counter(
    "fallback_triggered_total", "Fallback activations",
    ["handler", "level", "reason"],
)
FALLBACK_SUCCESS = METRICS.counter(
    "fallback_success_total", "Successful fallbacks", ["handler"]
)
FALLBACK_FAILURE = METRICS.counter(
    "fallback_failure_total", "Failed fallbacks", ["handler"]
)
BACKGROUND_TASKS = METRICS.gauge("fallback_background_tasks", "Active background tasks")
TASK_DURATION = METRICS.histogram(
    "fallback_task_duration_seconds", "Task duration", ["task_name"]
)
TASK_ERRORS = METRICS.counter(
    "fallback_task_errors_total", "Task errors", ["task_name"]
)
HEALTH_CHECK_DURATION = METRICS.histogram(
    "fallback_health_check_duration_seconds", "Health check duration", ["component"]
)
QUANTUM_SIGNATURES = METRICS.counter(
    "quantum_signatures_total", "PQC signatures", ["algorithm", "status"]
)
BLOCKCHAIN_VERIFICATIONS = METRICS.counter(
    "blockchain_verifications_total", "Blockchain verifications", ["status"]
)
FALLBACK_VERIFICATIONS = METRICS.gauge(
    "fallback_verifications_total", "Verifications"
)
AUTONOMOUS_OPTIMIZATIONS = METRICS.counter(
    "autonomous_fallback_optimizations_total", "Optimizations", ["status"]
)
REGIONAL_COORDINATIONS = METRICS.counter(
    "regional_fallback_coordinations_total", "Regional coordinations",
    ["region", "status"],
)
CIRCUIT_BREAKER_STATE = METRICS.gauge(
    "fallback_circuit_breaker_state", "CB state", ["name"]
)
RATE_LIMITER_THROTTLE = METRICS.gauge(
    "fallback_rate_limiter_throttle", "Throttle %"
)
FEDERATED_SHARES = METRICS.counter(
    "fallback_federated_shares_total", "Federated shares", ["source"]
)
PREDICTIVE_ACCURACY = METRICS.gauge(
    "fallback_predictive_accuracy", "Predictive accuracy", ["model"]
)
VAULT_OPERATIONS = METRICS.counter(
    "fallback_vault_operations_total", "Vault ops", ["operation", "status"]
)
CLOUD_STORAGE = METRICS.counter(
    "fallback_cloud_storage_operations_total", "Cloud storage ops",
    ["provider", "operation", "status"],
)
HEALTH_SCORE = METRICS.gauge("fallback_health_score", "Health score (0-100)")
SAFETY_VIOLATIONS = METRICS.counter(
    "fallback_safety_violations_total", "Safety violations", ["rule"]
)
CHAOS_EXPERIMENTS = METRICS.counter(
    "fallback_chaos_experiments_total", "Chaos experiments", ["fault", "status"]
)
HUMAN_REVIEWS = METRICS.counter(
    "fallback_human_reviews_total", "Human reviews", ["status"]
)
XAI_DECISIONS = METRICS.counter(
    "fallback_xai_decisions_total", "XAI decisions", ["policy"]
)
CARBON_OFFSETS = METRICS.counter(
    "fallback_carbon_offsets_total", "Carbon offsets", ["status"]
)
PRECISION_SELECTIONS = METRICS.counter(
    "fallback_precision_selections_total", "Precision selections", ["precision"]
)
BANDIT_UPDATES = METRICS.counter(
    "fallback_bandit_updates_total", "Bandit updates", ["policy"]
)


# =============================================================================
# Interfaces
# =============================================================================
@runtime_checkable
class IQuantumSecurity(Protocol):
    async def generate_keypair(self, algorithm: str = "") -> Dict: ...
    async def sign_fallback_decision(self, decision: Dict, key_id: str) -> Dict: ...
    async def verify_fallback_decision(self, decision: Dict, signature_data: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IBlockchain(Protocol):
    async def record_fallback(self, fallback_id: str, manifest: Dict, outcome: Dict) -> Dict: ...
    async def get_blockchain_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICarbonManager(Protocol):
    async def get_current_intensity(self) -> Dict: ...
    async def close(self): ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ILLMGenerator(Protocol):
    async def generate_fallback_plan(self, context: Dict) -> Dict: ...
    def get_cost_statistics(self) -> Dict: ...
    async def health_check(self) -> Dict: ...
    async def close(self): ...


@runtime_checkable
class ILoadShedder(Protocol):
    async def acquire(self) -> Tuple[bool, Optional[asyncio.Event]]: ...
    async def release(self): ...
    def get_statistics(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IRegionCoordinator(Protocol):
    async def coordinate_fallback(self, handler_name: str, requirements: Dict) -> Dict: ...
    async def get_region_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IAutonomousOptimizer(Protocol):
    async def optimize_fallbacks(self, performance_data: Dict) -> Dict: ...
    async def get_optimization_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IFederatedLearner(Protocol):
    async def pull_network_patterns(self, domain: Optional[str] = None,
                                    limit: int = 5) -> List[Dict]: ...
    async def push_pattern(self, pattern: Dict): ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IPredictiveReflexivity(Protocol):
    async def update_history(self, data: Dict): ...
    async def get_fallback_forecast(self, horizon_hours: int = 0) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ISustainabilityTracker(Protocol):
    async def record_metric(self, metric_name: str, value: float,
                            metadata: Optional[Dict] = None): ...
    async def get_fallback_sustainability_score(self) -> Dict: ...
    async def get_fallback_savings(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IWebSocketServer(Protocol):
    async def start(self): ...
    async def stop(self): ...
    async def broadcast(self, message: Dict): ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IDatabaseManager(Protocol):
    async def init(self): ...
    async def execute_async(self, func): ...
    async def save_fallback_history(self, record: Dict): ...
    async def save_optimizer_state(self, key: str, value: Dict): ...
    async def load_optimizer_state(self, key: str) -> Optional[Dict]: ...
    async def health_check(self) -> Dict: ...
    async def close(self): ...


@runtime_checkable
class IVault(Protocol):
    async def store_secret(self, path: str, data: Dict): ...
    async def get_secret(self, path: str) -> Optional[Dict]: ...
    async def health_check(self) -> Dict: ...


# =============================================================================
# Circuit breaker
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0):
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

    def record_success(self) -> None:
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

    def record_failure(self) -> None:
        with self._lock:
            self._metrics["failed_calls"] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state in (CircuitBreakerState.CLOSED, CircuitBreakerState.HALF_OPEN):
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)

    async def call_async(self, func, *args, **kwargs):
        self._precheck()
        self._metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def call_sync(self, func, *args, **kwargs):
        self._precheck()
        self._metrics["total_calls"] += 1
        try:
            result = func(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
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
                conf = 1.0 / (1.0 + np.exp(-abs(samples[best])))
                return best, float(conf), "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * float(np.sqrt(var))
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + np.exp(-abs(scores[best])))
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
            BANDIT_UPDATES.labels(policy=action).inc()

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

    def apply_federated_prior(self, action: str, mean_reward: float,
                              weight: float = 0.1) -> None:
        if action not in self.theta or weight <= 0:
            return
        with self._lock:
            P = self.P[action]
            x = np.zeros(self.dim)
            x[0] = 1.0
            denom = 1.0 + float(x @ P @ x)
            K = (P @ x) / denom
            err = float(mean_reward) * float(weight) - float(x @ self.theta[action])
            self.theta[action] = self.theta[action] + K * err
            newP = P - np.outer(K, x) @ P
            self.P[action] = 0.5 * (newP + newP.T)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": list(self.actions),
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor — plain list for timestamps
# =============================================================================
class SafetyMonitor:
    def __init__(
        self,
        max_fallbacks_per_hour: int = 20,
        max_carbon_for_fallback: float = 600.0,
        max_consecutive_failures: int = 3,
    ):
        self.max_fallbacks_per_hour = int(max_fallbacks_per_hour)
        self.max_carbon_for_fallback = float(max_carbon_for_fallback)
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.fallback_timestamps: List[float] = []
        self.consecutive_failures = 0
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def _prune(self, now: float) -> None:
        cutoff = now - 3600.0
        self.fallback_timestamps = [t for t in self.fallback_timestamps if t > cutoff]

    def check_fallback(self, carbon_intensity: float, success: bool = True) -> Tuple[bool, Optional[str]]:
        with self._lock:
            now = time.time()
            self._prune(now)
            if len(self.fallback_timestamps) >= self.max_fallbacks_per_hour:
                self._record("max_fallbacks_per_hour",
                             {"count": len(self.fallback_timestamps)})
                return False, "max_fallbacks_per_hour"
            if carbon_intensity > self.max_carbon_for_fallback:
                self._record("max_carbon_for_fallback", {"carbon": carbon_intensity})
                return False, "max_carbon_for_fallback"
            if not success:
                self.consecutive_failures += 1
                if self.consecutive_failures > self.max_consecutive_failures:
                    self._record("max_consecutive_failures",
                                 {"count": self.consecutive_failures})
                    return False, "max_consecutive_failures"
            else:
                self.consecutive_failures = 0
            self.fallback_timestamps.append(now)
            return True, None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_fallbacks_per_hour": self.max_fallbacks_per_hour,
                "max_carbon_for_fallback": self.max_carbon_for_fallback,
                "max_consecutive_failures": self.max_consecutive_failures,
            }

    def restore(self, snap: Dict[str, Any]) -> None:
        with self._lock:
            if "max_fallbacks_per_hour" in snap:
                self.max_fallbacks_per_hour = int(snap["max_fallbacks_per_hour"])
            if "max_carbon_for_fallback" in snap:
                self.max_carbon_for_fallback = float(snap["max_carbon_for_fallback"])
            if "max_consecutive_failures" in snap:
                self.max_consecutive_failures = int(snap["max_consecutive_failures"])

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
# XAIExplainer — structured output
# =============================================================================
class XAIExplainer:
    def explain_fallback(
        self,
        handler_name: str,
        strategy: str,
        context: Dict[str, Any],
        confidence: float,
        region: str,
        precision: str,
    ) -> Dict[str, Any]:
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        narrative = (
            f"Fallback '{handler_name}' using strategy '{strategy}' "
            f"(confidence={confidence:.2f}, region='{region}', precision={precision})."
        )
        return {
            "handler_name": handler_name,
            "strategy": strategy,
            "confidence": float(confidence),
            "region": region,
            "precision": precision,
            "top_contributions": top,
            "narrative": narrative,
        }

    def explain_fallback_str(
        self,
        handler_name: str,
        strategy: str,
        context: Dict[str, Any],
        confidence: float,
        region: str,
        precision: str,
    ) -> str:
        return self.explain_fallback(
            handler_name, strategy, context, confidence, region, precision
        )["narrative"]

    def explain_optimization(self, policy: str, source: str,
                             applied_strategies: int) -> Dict[str, Any]:
        narrative = (
            f"Optimizer chose policy '{policy}' (source='{source}') "
            f"applying {applied_strategies} strategies."
        )
        return {
            "policy": policy,
            "source": source,
            "applied_strategies": int(applied_strategies),
            "narrative": narrative,
        }


# =============================================================================
# FederatedSecureCoordinator — weighted + correct DP
# =============================================================================
class FederatedSecureCoordinator:
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
                    mean += float(self._rng.laplace(0.0, noise_scale))
                    aggregated[key] = mean
                else:
                    aggregated[key] = values[0]
            self.round_id += 1
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_participant_count(self) -> int:
        with self._lock:
            return len(self.participants)


# =============================================================================
# MultiAgentCoordinator — only contributing agents updated
# =============================================================================
class MultiAgentCoordinator:
    def __init__(self, agents: Optional[List[str]] = None):
        self.agents = agents or [
            "latency_agent", "carbon_agent", "cost_agent", "reliability_agent",
        ]
        self.reputation: Dict[str, float] = {a: 0.5 for a in self.agents}
        self.contributions: Dict[str, int] = {a: 0 for a in self.agents}
        self._lock = threading.RLock()

    def select_agents(self, context: Dict[str, Any], top_k: int = 2) -> List[str]:
        with self._lock:
            scores: Dict[str, float] = {}
            for agent in self.agents:
                base = self.reputation.get(agent, 0.5)
                if agent == "carbon_agent" and float(context.get("carbon_intensity", 400)) > 400:
                    base += 0.2
                elif agent == "latency_agent" and float(context.get("avg_latency", 200)) > 200:
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

    def estimate_offset_cost(self, carbon_kg: float,
                             carbon_intensity: Optional[float] = None) -> float:
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
# ChaosMonkey — named faults, seeded, snapshot/restore
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {
        "load_shedder_failure", "blockchain_failure", "region_failure", "llm_failure",
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
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def maybe_fail(self, component: str = "fallback") -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self.injected_failures += 1
        self._record(fault, "triggered", {"component": component})
        CHAOS_EXPERIMENTS.labels(fault=fault, status="triggered").inc()
        return fault

    def apply(self, fault: str, manager: "EnhancedFallbackManagerV15_0") -> None:
        with self._lock:
            if fault == "load_shedder_failure":
                self._snapshots.setdefault("load_shedder_failure", {"ls": manager.load_shedder})
                manager.load_shedder = _NullLoadShedder()
            elif fault == "blockchain_failure":
                self._snapshots.setdefault("blockchain_failure", {"bc": manager.blockchain})
                manager.blockchain = _NullBlockchain()
            elif fault == "region_failure":
                self._snapshots.setdefault("region_failure", {"rc": manager.region_coordinator})
                manager.region_coordinator = _NullRegionCoordinator()
            elif fault == "llm_failure":
                self._snapshots.setdefault("llm_failure", {"llm": manager.llm_generator})
                manager.llm_generator = _NullLLM()

    def cleanup_fault(self, fault: str, manager: "EnhancedFallbackManagerV15_0") -> None:
        with self._lock:
            snap = self._snapshots.pop(fault, None)
            if snap is None:
                return
            if fault == "load_shedder_failure":
                manager.load_shedder = snap["ls"]
            elif fault == "blockchain_failure":
                manager.blockchain = snap["bc"]
            elif fault == "region_failure":
                manager.region_coordinator = snap["rc"]
            elif fault == "llm_failure":
                manager.llm_generator = snap["llm"]

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault, "outcome": outcome,
            "timestamp": _utc_now().isoformat(), **(extra or {}),
        })

    async def inject_and_measure(
        self, manager: "EnhancedFallbackManagerV15_0", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, manager)
        recovery_step: Optional[int] = None
        try:
            for step in range(steps):
                try:
                    _ = manager.fallback_history
                    if recovery_step is None:
                        recovery_step = step
                except Exception:
                    continue
        finally:
            self.cleanup_fault(fault, manager)
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


class _NullLoadShedder:
    async def acquire(self): return True, None
    async def release(self): return None
    def get_statistics(self): return {"current": 0, "max_concurrent": 0, "queue_length": 0}
    async def health_check(self): return {"status": "unavailable"}


class _NullBlockchain:
    async def record_fallback(self, *a, **k): return {"tx_hash": None, "status": "unavailable"}
    async def get_blockchain_status(self): return {"connected": False}
    async def health_check(self): return {"status": "unavailable"}


class _NullRegionCoordinator:
    async def coordinate_fallback(self, *a, **k): return {"primary_region": "us-east", "source": "null"}
    async def get_region_status(self): return {"active_region": "us-east", "regions": {}}
    async def health_check(self): return {"status": "unavailable"}


class _NullLLM:
    async def generate_fallback_plan(self, context): return {"plan": "null"}
    def get_cost_statistics(self): return {"calls": 0}
    async def health_check(self): return {"status": "unavailable"}
    async def close(self): return None


# =============================================================================
# HumanReviewManager
# =============================================================================
@dataclass
class HumanReviewRequest:
    review_id: str
    fallback_id: str
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
                HUMAN_REVIEWS.labels(status="expired").inc()

    def _enforce_capacity(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"
            HUMAN_REVIEWS.labels(status="dropped").inc()

    async def request_review(self, fallback_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid, fallback_id=fallback_id,
                details=dict(details), created_at=_utc_now(),
            )
            self._enforce_capacity()
            HUMAN_REVIEWS.labels(status="pending").inc()
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str,
               reason: Optional[str]) -> None:
        self.audit_log.append({
            "review_id": review_id, "action": action,
            "reviewer": reviewer, "reason": reason,
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
            HUMAN_REVIEWS.labels(status="approved").inc()
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
            HUMAN_REVIEWS.labels(status="rejected").inc()
            return True

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            self._evict_expired()
            return [
                {
                    "review_id": r.review_id,
                    "fallback_id": r.fallback_id,
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
# FlexGenPrecisionPolicy
# =============================================================================
class FlexGenPrecisionPolicy:
    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = float(default_carbon_intensity)

    def recommend_precision(
        self, workload_size: str = "medium", carbon_intensity: Optional[float] = None
    ) -> str:
        carbon = (
            float(carbon_intensity)
            if carbon_intensity is not None else self.default_carbon_intensity
        )
        if carbon > 500 or workload_size == "large":
            precision = "int8"
        elif carbon > 300 or workload_size == "medium":
            precision = "fp16"
        else:
            precision = "fp32"
        PRECISION_SELECTIONS.labels(precision=precision).inc()
        return precision

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": True,
            "default_carbon_intensity": self.default_carbon_intensity,
        }


# =============================================================================
# QuantumDistillationOptimizer
# =============================================================================
class QuantumDistillationOptimizer:
    def __init__(self, enabled: bool = False, qaoa_reps: int = 1):
        self.enabled = bool(enabled)
        self.qaoa_reps = int(qaoa_reps)
        self.available = self.enabled and QISKIT_AVAILABLE

    async def select_best_policy(
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
            sense="E", rhs=1, name="one_policy",
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
# Configuration
# =============================================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("15.2")
        log_level: str = Field("INFO")
        max_retries: int = Field(3, ge=0)
        base_retry_delay: float = Field(1.0, gt=0)
        max_concurrent_requests: int = Field(1000, ge=1)
        max_queue_size: int = Field(100, ge=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        human_review_threshold_failures: int = Field(3, ge=1)

        @field_validator("log_level")
        @classmethod
        def _validate_log_level(cls, v: str) -> str:
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"LOG_LEVEL must be one of {allowed}")
            return v.upper()

    class QuantumConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("dilithium")
        master_key: str = Field("")
        enable_distillation: bool = False
        qaoa_reps: int = 1

        @field_validator("master_key")
        @classmethod
        def _validate_master_key(cls, v: str) -> str:
            if not v:
                return ""
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError("master_key must be a hex string")
            return v

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                return os.urandom(32)
            return bytes.fromhex(self.master_key)

    class CloudConfig(BaseModel):
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = Field("us-east-1")
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class LLMConfig(BaseModel):
        provider: str = Field("openai")
        api_key: Optional[str] = None
        model: str = Field("gpt-4")
        timeout: float = Field(30.0, gt=0)

    class SchedulerConfig(BaseModel):
        health_check_interval: int = Field(60, ge=10)
        auto_tune_interval: int = Field(3600, ge=60)
        federated_interval: int = Field(3600, ge=60)
        predictive_interval: int = Field(1800, ge=60)
        sustainability_interval: int = Field(3600, ge=60)
        carbon_update_interval: int = Field(300, ge=10)

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        horizon_hours: int = Field(24, ge=1)
        model_storage_path: str = Field("./prophet_models")
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = Field(10, ge=1)
        hyperparam_generations: int = Field(5, ge=1)

    class FederatedConfig(BaseModel):
        enabled: bool = True
        share_interval: int = Field(3600, ge=60)
        privacy_budget: float = Field(0.5, gt=0)
        sensitivity: float = Field(1.0, gt=0)

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///fallback_manager.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/fallback")

    class APIConfig(BaseModel):
        host: str = Field("0.0.0.0")
        port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)
        require_auth_for_admin: bool = True

    class CircuitBreakerConfig(BaseModel):
        failure_threshold: int = Field(5, ge=1)
        recovery_timeout: int = Field(60, ge=1)

    class LeaderConfig(BaseModel):
        enabled: bool = False
        redis_url: Optional[str] = None
        ttl_seconds: int = Field(30, ge=1)

    class CarbonConfig(BaseModel):
        api_key: Optional[str] = None
        region: str = Field("global")
        offset_threshold: float = Field(400.0, gt=0)
        offset_cost_per_kg: float = Field(0.1, gt=0)
        rec_cost_per_mwh: float = Field(5.0, gt=0)

    class WebSocketConfig(BaseModel):
        enabled: bool = True
        port: int = Field(8769, ge=1024)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())

    class SafetyConfig(BaseModel):
        enabled: bool = True
        max_fallbacks_per_hour: int = Field(20, ge=1)
        max_carbon_for_fallback: float = Field(600.0, gt=0)
        max_consecutive_failures: int = Field(3, ge=1)
        hitl_ttl_seconds: float = Field(3600.0, gt=0)
        hitl_max_pending: int = Field(100, ge=1)

    class ChaosConfig(BaseModel):
        enabled: bool = False
        failure_probability: float = Field(0.1, ge=0, le=1)
        seed: int = 42

    class MultiAgentConfig(BaseModel):
        enabled: bool = True
        agents: List[str] = Field(default_factory=lambda: [
            "latency_agent", "carbon_agent", "cost_agent", "reliability_agent",
        ])

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "success": 0.4, "latency": 0.3, "carbon": 0.2, "cost": 0.1,
            }
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bandit_feature_dim: int = Field(8, ge=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        limit_graph_enabled: bool = True
        limit_graph_max_nodes: int = 100
        rlhf_enabled: bool = True
        rlhf_buffer_size: int = 1000
        distillation_enabled: bool = True
        distillation_update_interval: int = 600
        causal_bandit_enabled: bool = True

    class FallbackManagerConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="FALLBACK_", case_sensitive=False)

        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        llm: LLMConfig = Field(default_factory=LLMConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = Field(default_factory=LeaderConfig)
        carbon: CarbonConfig = Field(default_factory=CarbonConfig)
        websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        multi_agent: MultiAgentConfig = Field(default_factory=MultiAgentConfig)

        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_chain_id: int = Field(1, ge=1)
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        redis_url: Optional[str] = None

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

        def to_dict(self) -> Dict[str, Any]:
            if hasattr(self, "model_dump"):
                try:
                    return self.model_dump()
                except Exception:
                    pass
            if hasattr(self, "dict"):
                try:
                    return self.dict()
                except Exception:
                    pass
            return dict(getattr(self, "__dict__", {}))

else:
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "15.2"
        log_level: str = "INFO"
        max_retries: int = 3
        base_retry_delay: float = 1.0
        max_concurrent_requests: int = 1000
        max_queue_size: int = 100
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        human_review_threshold_failures: int = 3

    @dataclass
    class QuantumConfig:
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = ""
        enable_distillation: bool = False
        qaoa_reps: int = 1

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                return os.urandom(32)
            return bytes.fromhex(self.master_key)

    @dataclass
    class CloudConfig:
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = "us-east-1"
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    @dataclass
    class LLMConfig:
        provider: str = "openai"
        api_key: Optional[str] = None
        model: str = "gpt-4"
        timeout: float = 30.0

    @dataclass
    class SchedulerConfig:
        health_check_interval: int = 60
        auto_tune_interval: int = 3600
        federated_interval: int = 3600
        predictive_interval: int = 1800
        sustainability_interval: int = 3600
        carbon_update_interval: int = 300

    @dataclass
    class PredictiveConfig:
        enabled: bool = True
        horizon_hours: int = 24
        model_storage_path: str = "./prophet_models"
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = 10
        hyperparam_generations: int = 5

    @dataclass
    class FederatedConfig:
        enabled: bool = True
        share_interval: int = 3600
        privacy_budget: float = 0.5
        sensitivity: float = 1.0

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///fallback_manager.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/fallback"

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True
        rate_limit_requests: int = 100
        rate_limit_window: int = 60
        require_auth_for_admin: bool = True

    @dataclass
    class CircuitBreakerConfig:
        failure_threshold: int = 5
        recovery_timeout: int = 60

    @dataclass
    class LeaderConfig:
        enabled: bool = False
        redis_url: Optional[str] = None
        ttl_seconds: int = 30

    @dataclass
    class CarbonConfig:
        api_key: Optional[str] = None
        region: str = "global"
        offset_threshold: float = 400.0
        offset_cost_per_kg: float = 0.1
        rec_cost_per_mwh: float = 5.0

    @dataclass
    class WebSocketConfig:
        enabled: bool = True
        port: int = 8769
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())

    @dataclass
    class SafetyConfig:
        enabled: bool = True
        max_fallbacks_per_hour: int = 20
        max_carbon_for_fallback: float = 600.0
        max_consecutive_failures: int = 3
        hitl_ttl_seconds: float = 3600.0
        hitl_max_pending: int = 100

    @dataclass
    class ChaosConfig:
        enabled: bool = False
        failure_probability: float = 0.1
        seed: int = 42

    @dataclass
    class MultiAgentConfig:
        enabled: bool = True
        agents: List[str] = field(default_factory=lambda: [
            "latency_agent", "carbon_agent", "cost_agent", "reliability_agent",
        ])

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        modp_weights: Dict[str, float] = field(default_factory=lambda: {
            "success": 0.4, "latency": 0.3, "carbon": 0.2, "cost": 0.1,
        })
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bandit_feature_dim: int = 8
        bio_generations: int = 10
        bio_population_size: int = 20
        limit_graph_enabled: bool = True
        limit_graph_max_nodes: int = 100
        rlhf_enabled: bool = True
        rlhf_buffer_size: int = 1000
        distillation_enabled: bool = True
        distillation_update_interval: int = 600
        causal_bandit_enabled: bool = True

    @dataclass
    class FallbackManagerConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        llm: LLMConfig = field(default_factory=LLMConfig)
        scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
        federated: FederatedConfig = field(default_factory=FederatedConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = field(default_factory=LeaderConfig)
        carbon: CarbonConfig = field(default_factory=CarbonConfig)
        websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        multi_agent: MultiAgentConfig = field(default_factory=MultiAgentConfig)
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        redis_url: Optional[str] = None

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

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
# ORM models — guarded by Base presence
# =============================================================================
Base = None
if SQLALCHEMY_ASYNC_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE:
    Base = declarative_base()

    class FallbackHistoryDB(Base):
        __tablename__ = "fallback_history"
        id = Column(Integer, primary_key=True)
        handler_name = Column(String(128), index=True)
        strategy_used = Column(String(64))
        degradation_level = Column(String(32))
        latency_ms = Column(Float)
        retry_count = Column(Integer, default=0)
        success = Column(Boolean)
        carbon_intensity = Column(Float)
        region = Column(String(64))
        precision = Column(String(16), default="fp32")
        explanation = Column(Text)
        human_review_id = Column(String(64))
        timestamp = Column(DateTime, default=_utc_now)

    class CircuitBreakerDB(Base):
        __tablename__ = "circuit_breakers"
        id = Column(Integer, primary_key=True)
        name = Column(String(128), unique=True, index=True)
        state = Column(String(32))
        failure_count = Column(Integer, default=0)
        success_count = Column(Integer, default=0)
        last_failure_time = Column(DateTime)
        last_success_time = Column(DateTime)
        updated_at = Column(DateTime, default=_utc_now)

    class SustainabilityMetricDB(Base):
        __tablename__ = "sustainability_metrics"
        id = Column(Integer, primary_key=True)
        metric_name = Column(String(64), index=True)
        value = Column(Float)
        metadata_json = Column(JSON)  # renamed from metadata to avoid shadowing
        timestamp = Column(DateTime, default=_utc_now)

    class FederatedPatternDB(Base):
        __tablename__ = "federated_patterns"
        id = Column(Integer, primary_key=True)
        source = Column(String(64))
        domain = Column(String(64))
        pattern = Column(JSON)
        timestamp = Column(DateTime, default=_utc_now)

    class OptimizerStateDB(Base):
        __tablename__ = "optimizer_state"
        id = Column(Integer, primary_key=True)
        key = Column(String(64), unique=True)
        value = Column(JSON)
        updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now)


# =============================================================================
# Vault manager
# =============================================================================
class VaultManager(IVault):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url and config.vault.token:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception as e:
                logger.warning(f"vault_init_failed error={e}")

    async def store_secret(self, path: str, data: Dict) -> None:
        if not self.client:
            raise VaultError("Vault client not configured")
        try:
            await asyncio.to_thread(
                self.client.secrets.kv.v2.create_or_update_secret,
                path=path, secret=data,
            )
            VAULT_OPERATIONS.labels(operation="store", status="success").inc()
        except Exception as e:
            VAULT_OPERATIONS.labels(operation="store", status="failed").inc()
            raise VaultError(f"vault_store_failed error={e}") from e

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None
        try:
            result = await asyncio.to_thread(
                self.client.secrets.kv.v2.read_secret_version, path
            )
            VAULT_OPERATIONS.labels(operation="read", status="success").inc()
            return result.get("data", {}).get("data")
        except Exception:
            VAULT_OPERATIONS.labels(operation="read", status="failed").inc()
            return None

    async def health_check(self) -> Dict:
        return {"status": "ok" if self.client else "unavailable"}


# =============================================================================
# EnhancedDatabaseManager — real async init and persistence
# =============================================================================
class EnhancedDatabaseManager(IDatabaseManager):
    SCHEMA_VERSION = 3

    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.db_url = config.database.url
        self.async_engine = None
        self.async_session = None
        self._initialized = False
        self._lock = asyncio.Lock()
        if SQLALCHEMY_ASYNC_AVAILABLE:
            try:
                self.async_engine = create_async_engine(
                    self.db_url, poolclass=NullPool,
                )
                self.async_session = async_sessionmaker(
                    self.async_engine, expire_on_commit=False
                )
            except Exception as e:
                logger.warning(f"db_init_failed error={e}")
                self.async_engine = None
                self.async_session = None

    async def init(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            if self.async_engine is not None and Base is not None:
                try:
                    async with self.async_engine.begin() as conn:
                        await conn.run_sync(Base.metadata.create_all)
                except Exception as e:
                    logger.warning(f"db_create_tables_failed error={e}")
            self._initialized = True
            logger.info(f"db_initialized url={self.db_url}")

    async def execute_async(self, func):
        if not self.async_session:
            raise DatabaseError("Async session not available")
        async with self.async_session() as session:
            return await func(session)

    async def save_fallback_history(self, record: Dict) -> None:
        if not self.async_session or Base is None:
            return
        try:
            async with self.async_session() as session:
                row = FallbackHistoryDB(
                    handler_name=record.get("handler_name", "unknown"),
                    strategy_used=record.get("strategy_used", ""),
                    degradation_level=record.get("degradation_level", ""),
                    latency_ms=float(record.get("latency_ms", 0.0)),
                    retry_count=int(record.get("retry_count", 0)),
                    success=bool(record.get("success", False)),
                    carbon_intensity=float(record.get("carbon_intensity", 0.0)),
                    region=record.get("region", ""),
                    precision=record.get("precision", "fp32"),
                    explanation=json.dumps(record.get("explanation", {}), default=str)
                    if isinstance(record.get("explanation"), dict)
                    else str(record.get("explanation", "")),
                    human_review_id=record.get("human_review_id"),
                )
                session.add(row)
                await session.commit()
        except Exception as e:
            logger.warning(f"save_fallback_history_failed error={e}")

    async def save_optimizer_state(self, key: str, value: Dict) -> None:
        if not self.async_session or Base is None:
            return
        payload = json.dumps(value, default=str)
        ts = _utc_now().isoformat()
        try:
            async with self.async_session() as session:
                try:
                    await session.execute(
                        text(
                            "INSERT INTO optimizer_state (key, value, updated_at) "
                            "VALUES (:key, :value, :updated_at) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
                            "updated_at=excluded.updated_at"
                        ),
                        {"key": key, "value": payload, "updated_at": ts},
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    await session.execute(
                        text(
                            "INSERT OR REPLACE INTO optimizer_state "
                            "(key, value, updated_at) VALUES (:key, :value, :updated_at)"
                        ),
                        {"key": key, "value": payload, "updated_at": ts},
                    )
                    await session.commit()
        except Exception as e:
            logger.warning(f"save_optimizer_state_failed error={e}")

    async def load_optimizer_state(self, key: str) -> Optional[Dict]:
        if not self.async_session or Base is None:
            return None
        try:
            async with self.async_session() as session:
                result = await session.execute(
                    text("SELECT value FROM optimizer_state WHERE key = :key"),
                    {"key": key},
                )
                row = result.fetchone()
                if row:
                    return json.loads(row[0])
        except Exception as e:
            logger.warning(f"load_optimizer_state_failed error={e}")
        return None

    async def health_check(self) -> Dict:
        if self.async_session is None:
            return {"status": "unavailable"}
        try:
            async with self.async_session() as session:
                await session.execute(text("SELECT 1"))
            return {"status": "healthy"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def close(self) -> None:
        if self.async_engine:
            with contextlib.suppress(Exception):
                await self.async_engine.dispose()


# =============================================================================
# Carbon intensity manager
# =============================================================================
class CarbonIntensityManager(ICarbonManager):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self._last_value = 400.0
        self._lock = asyncio.Lock()

    async def get_current_intensity(self) -> Dict:
        async with self._lock:
            value = float(self._last_value)
        return {
            "intensity": value,
            "units": "gCO2/kWh",
            "timestamp": _utc_now().isoformat(),
        }

    async def close(self) -> None:
        return None

    async def health_check(self) -> Dict:
        return {"status": "ok"}


# =============================================================================
# Blockchain — honest
# =============================================================================
class BlockchainFallbackVerification(IBlockchain):
    def __init__(self, config: FallbackManagerConfig, db_manager: IDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.web3 = None
        if WEB3_AVAILABLE and config.blockchain_enabled and config.blockchain_rpc_url:
            try:
                self.web3 = Web3(Web3.HTTPProvider(config.blockchain_rpc_url))
                if config.blockchain_chain_id in (4, 42, 5):
                    self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if not self.web3.is_connected():
                    self.web3 = None
            except Exception as e:
                logger.warning(f"web3_init_failed error={e}")
                self.web3 = None

    async def record_fallback(self, fallback_id: str, manifest: Dict, outcome: Dict) -> Dict:
        if self.web3 is None or not self.web3.is_connected():
            BLOCKCHAIN_VERIFICATIONS.labels(status="not_connected").inc()
            raise NotImplementedYetError("Blockchain not connected")
        if not self.config.blockchain_contract_address or not self.config.blockchain_private_key:
            BLOCKCHAIN_VERIFICATIONS.labels(status="not_configured").inc()
            raise NotImplementedYetError(
                "Blockchain contract address / private key not configured"
            )
        BLOCKCHAIN_VERIFICATIONS.labels(status="submitted").inc()
        # Real implementation would build/sign/send the transaction.
        return {"tx_hash": None, "status": "not_implemented"}

    async def get_blockchain_status(self) -> Dict:
        return {
            "connected": bool(self.web3 and self.web3.is_connected()),
            "network": self.config.blockchain_chain_id,
            "contract_configured": bool(self.config.blockchain_contract_address),
        }

    async def health_check(self) -> Dict:
        status = await self.get_blockchain_status()
        return {"status": "ok" if status["connected"] else "unavailable", **status}


# =============================================================================
# Quantum security — real signing with ECDSA fallback
# =============================================================================
class QuantumResilientFallbackSecurity(IQuantumSecurity):
    def __init__(self, config: FallbackManagerConfig, vault: VaultManager):
        self.config = config
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE and CRYPTO_AVAILABLE
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    async def generate_keypair(self, algorithm: Optional[str] = None) -> Dict:
        algorithm = algorithm or self.config.quantum.algorithm
        with self._lock:
            if self.pqc_available and algorithm in {"dilithium", "falcon", "sphincs"}:
                try:
                    mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[algorithm]
                    pk, sk = await asyncio.to_thread(mod.generate_keypair)
                    key_id = uuid.uuid4().hex[:8]
                    self._keys[key_id] = {
                        "public": bytes(pk), "private": bytes(sk),
                        "algorithm": algorithm,
                    }
                    QUANTUM_SIGNATURES.labels(algorithm=algorithm, status="keygen").inc()
                    return {
                        "key_id": key_id, "algorithm": algorithm,
                        "public_key": bytes(pk).hex(), "pqc": True,
                    }
                except Exception as e:
                    logger.warning(f"pqc_keygen_failed error={e}")

            if not CRYPTO_AVAILABLE:
                raise QuantumError("no cryptographic backend available")

            private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
            public_key = private_key.public_key()
            public_bytes = public_key.public_bytes(
                Encoding.PEM, PublicFormat.SubjectPublicKeyInfo
            )
            private_bytes = private_key.private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
            )
            key_id = uuid.uuid4().hex[:8]
            self._keys[key_id] = {
                "public": public_bytes, "private": private_bytes,
                "algorithm": "ecdsa",
            }
            QUANTUM_SIGNATURES.labels(algorithm="ecdsa", status="keygen").inc()
            return {
                "key_id": key_id, "algorithm": "ecdsa",
                "public_key": public_bytes.hex(), "pqc": False,
            }

    async def sign_fallback_decision(self, decision: Dict, key_id: str) -> Dict:
        with self._lock:
            entry = self._keys.get(key_id)
        if entry is None:
            raise QuantumError(f"unknown key_id {key_id}")
        payload = json.dumps(decision, sort_keys=True, default=str).encode()

        if entry["algorithm"] == "ecdsa":
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            QUANTUM_SIGNATURES.labels(algorithm="ecdsa", status="signed").inc()
            return {
                "algorithm": "ecdsa-p256", "key_id": key_id,
                "signature": sig.hex(), "timestamp": _utc_now().isoformat(),
            }

        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
        # pqcrypto API: mod.sign(private_key_bytes, message)
        sig = mod.sign(entry["private"], payload)
        QUANTUM_SIGNATURES.labels(algorithm=entry["algorithm"], status="signed").inc()
        return {
            "algorithm": entry["algorithm"], "key_id": key_id,
            "signature": bytes(sig).hex(), "timestamp": _utc_now().isoformat(),
        }

    async def verify_fallback_decision(self, decision: Dict, signature_data: Dict) -> bool:
        key_id = signature_data.get("key_id")
        with self._lock:
            entry = self._keys.get(key_id) if key_id else None
        if entry is None:
            return False
        payload = json.dumps(decision, sort_keys=True, default=str).encode()
        try:
            if entry["algorithm"] == "ecdsa":
                public_key = load_pem_public_key(
                    entry["public"], backend=default_backend()
                )
                public_key.verify(
                    bytes.fromhex(signature_data["signature"]),
                    payload, ec.ECDSA(hashes.SHA256()),
                )
                return True
            mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
            mod.verify(entry["public"], payload,
                       bytes.fromhex(signature_data["signature"]))
            return True
        except Exception:
            return False

    def get_quantum_status(self) -> Dict:
        return {
            "pqc_available": self.pqc_available,
            "algorithm_in_use": "pqc" if self.pqc_available else "ecdsa",
            "algorithms": ["dilithium", "falcon", "sphincs"] if self.pqc_available else ["ecdsa"],
        }

    async def health_check(self) -> Dict:
        return {"status": "ok", **self.get_quantum_status()}


# =============================================================================
# LLM fallback generator — real call when available
# =============================================================================
class LLMFallbackGenerator(ILLMGenerator):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.client = None
        if OPENAI_AVAILABLE and config.llm.api_key:
            try:
                self.client = AsyncOpenAI(
                    api_key=config.llm.api_key, timeout=config.llm.timeout
                )
            except Exception as e:
                logger.warning(f"openai_init_failed error={e}")
                self.client = None
        self.metrics = {"calls": 0, "errors": 0, "tokens": 0}

    async def generate_fallback_plan(self, context: Dict) -> Dict:
        if self.client is None:
            return {"plan": "default_fallback", "reason": "no_llm"}
        try:
            self.metrics["calls"] += 1
            prompt = (
                "You are a fallback planner for a green agent. Given the context, "
                "respond with a JSON object containing a 'plan' key (one of: retry, "
                "degrade, switch_region, cancel) and a short 'reason'.\n\n"
                f"Context: {json.dumps(context, default=str)}"
            )
            response = await self.client.chat.completions.create(
                model=self.config.llm.model,
                messages=[
                    {"role": "system", "content": "You output only JSON."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content or "{}"
            try:
                plan = json.loads(text)
            except Exception:
                plan = {"plan": "default_fallback", "reason": "unparseable"}
            usage = getattr(response, "usage", None)
            if usage is not None:
                self.metrics["tokens"] += int(getattr(usage, "total_tokens", 0) or 0)
            return plan
        except Exception as e:
            self.metrics["errors"] += 1
            logger.warning(f"llm_call_failed error={e}")
            return {"plan": "fallback_default", "error": str(e)}

    def get_cost_statistics(self) -> Dict:
        return dict(self.metrics)

    async def health_check(self) -> Dict:
        return {"status": "ok" if self.client else "degraded",
                "openai_available": OPENAI_AVAILABLE}

    async def close(self) -> None:
        if self.client is not None:
            with contextlib.suppress(Exception):
                await self.client.close()


# =============================================================================
# Load shedder — no event leak on timeout
# =============================================================================
class LoadShedder(ILoadShedder):
    def __init__(self, config: FallbackManagerConfig):
        self.max_concurrent = int(config.general.max_concurrent_requests)
        self.current = 0
        self._lock = asyncio.Lock()
        self.queue: Deque[asyncio.Event] = deque()
        self.timeouts = 0

    async def acquire(self) -> Tuple[bool, Optional[asyncio.Event]]:
        async with self._lock:
            if self.current < self.max_concurrent:
                self.current += 1
                return True, None
            event = asyncio.Event()
            self.queue.append(event)
            return False, event

    async def wait_or_cancel(self, event: asyncio.Event, timeout: float) -> bool:
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            self.timeouts += 1
            async with self._lock:
                with contextlib.suppress(ValueError):
                    self.queue.remove(event)
            return False

    async def release(self) -> None:
        async with self._lock:
            if self.current > 0:
                self.current -= 1
            while self.queue:
                event = self.queue.popleft()
                if not event.is_set():
                    event.set()
                    break

    def get_statistics(self) -> Dict:
        return {
            "current": self.current,
            "max_concurrent": self.max_concurrent,
            "queue_length": len(self.queue),
            "timeouts": self.timeouts,
        }

    async def health_check(self) -> Dict:
        return {"status": "ok", **self.get_statistics()}


# =============================================================================
# Multi-region coordinator
# =============================================================================
class MultiRegionFallbackCoordinator(IRegionCoordinator):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.regions: Dict[str, Dict[str, float]] = {
            "us-east": {"weight": 0.4, "capacity": 1000, "carbon_intensity": 400, "latency": 50},
            "eu-west": {"weight": 0.3, "capacity": 800, "carbon_intensity": 300, "latency": 80},
            "ap-southeast": {"weight": 0.3, "capacity": 600, "carbon_intensity": 500, "latency": 120},
        }
        self.active_region = "us-east"
        self._lock = asyncio.Lock()
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "region_coordinator",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout,
        )
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._modp_teacher, self._rule_based_teacher, self._static_teacher,
            ])
        else:
            self.distiller = None
        self.xai = XAIExplainer()
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )
        self.last_explanation = ""

    @staticmethod
    def _region_objectives(info: Dict[str, float]) -> Dict[str, float]:
        # Consistent normalization across teachers.
        return {
            "latency": 1.0 - min(info.get("latency", 100) / 200.0, 1.0),
            "carbon": 1.0 - min(info.get("carbon_intensity", 400) / 800.0, 1.0),
            "capacity": min(info.get("capacity", 1000) / 1000.0, 1.0),
            "reliability": 0.9,
        }

    def _modp_teacher(self, context: Dict) -> str:
        if not self.modp:
            return self.active_region
        weights = context.get("modp_weights", self.config.optimizer.modp_weights)
        scores: Dict[str, float] = {}
        for region, info in self.regions.items():
            objectives = self._region_objectives(info)
            try:
                scores[region] = float(self.modp.evaluate(objectives, weights))
            except Exception:
                scores[region] = 0.0
        return max(scores, key=scores.get)

    def _rule_based_teacher(self, context: Dict) -> str:
        weights = context.get("modp_weights", self.config.optimizer.modp_weights)
        scores: Dict[str, float] = {}
        for region, info in self.regions.items():
            objectives = self._region_objectives(info)
            scores[region] = sum(
                objectives[k] * float(weights.get(k, 0.0))
                for k in ("latency", "carbon", "capacity", "reliability")
            )
        return max(scores, key=scores.get)

    def _static_teacher(self, context: Dict) -> str:
        return "us-east"

    async def coordinate_fallback(self, handler_name: str, requirements: Dict) -> Dict:
        async def _coordinate():
            context = {
                "handler_name": handler_name,
                "requirements": requirements,
                "modp_weights": requirements.get("modp_weights", self.config.optimizer.modp_weights),
            }
            if self.distiller:
                best = self.distiller.distill(context)
                source = "distilled"
            else:
                best = self._modp_teacher(context) if self.modp else self._rule_based_teacher(context)
                source = "modp" if self.modp else "rule_based"

            if self.quantum_optimizer.available:
                candidates = [
                    {"name": r, **self._region_objectives(info)}
                    for r, info in self.regions.items()
                ]
                quantum_choice = await self.quantum_optimizer.select_best_policy(
                    candidates, self.config.optimizer.modp_weights
                )
                if quantum_choice:
                    best = quantum_choice.get("name", best)
                    source = "quantum"

            async with self._lock:
                self.active_region = best
            explanation = self.xai.explain_fallback(
                handler_name, best, context, 0.5, best, "fp32"
            )
            self.last_explanation = explanation["narrative"]
            REGIONAL_COORDINATIONS.labels(region=best, status="success").inc()
            XAI_DECISIONS.labels(policy=best).inc()
            return {
                "primary_region": best,
                "source": source,
                "reason": f"Region {best} selected via {source}",
                "explanation": explanation,
            }
        return await self.circuit_breaker.call_async(_coordinate)

    async def get_region_status(self) -> Dict:
        return {
            "active_region": self.active_region,
            "regions": self.regions,
            "distillation_active": self.distiller is not None,
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "last_explanation": self.last_explanation,
        }

    async def health_check(self) -> Dict:
        return {"status": "healthy", "regions": len(self.regions)}


# =============================================================================
# AutonomousFallbackOptimizer
# =============================================================================
class AutonomousFallbackOptimizer(IAutonomousOptimizer):
    def __init__(
        self,
        config: FallbackManagerConfig,
        db_manager: Optional[IDatabaseManager] = None,
        safety_monitor: Optional[SafetyMonitor] = None,
        xai: Optional[XAIExplainer] = None,
        multi_agent: Optional[MultiAgentCoordinator] = None,
    ):
        self.config = config
        self.db_manager = db_manager
        self.optimization_strategies: Dict[str, Callable] = {
            "reduce_latency": self._reduce_latency,
            "improve_success": self._improve_success,
            "reduce_carbon": self._reduce_carbon,
            "balance_load": self._balance_load,
            "optimize_retries": self._optimize_retries,
        }
        self.optimization_history: Deque[Dict[str, Any]] = deque(maxlen=100)
        self.active_optimizations: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self.last_context: Optional[Dict[str, Any]] = None
        self.last_explanation = ""
        self.last_policy = ""
        self.last_source = ""
        self._last_contributing_agents: List[str] = []

        if ENHANCEMENTS_AVAILABLE and config.optimizer.enabled:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.param_policies = ["aggressive", "balanced", "conservative", "carbon_aware"]
            self.bandit = CausalBandit(
                action_space=self.param_policies,
                fallback_solver=lambda ctx: "balanced",
                min_trials_before_bandit=config.optimizer.bandit_min_trials,
                confidence_threshold=config.optimizer.bandit_confidence_threshold,
                feature_dim=config.optimizer.bandit_feature_dim,
            )
            self.strategy_population: List[List[str]] = [list(self.optimization_strategies.keys())]
            self.strategy_fitness: Deque[float] = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.param_policies = ["balanced"]
            self.strategy_population = []
            self.strategy_fitness = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.limit_graph_enabled:
            self.limit_graph = LimitGraph()
            self.limit_graph.build_graph([], [])
        else:
            self.limit_graph = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.rlhf_enabled:
            self.rlhf = RLHFOptimizer(action_space=self.param_policies)
        else:
            self.rlhf = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                lambda ctx: self.bandit.select_action(ctx)[0] if self.bandit else "balanced",
                lambda ctx: self._modp_policy(ctx) if self.modp else "balanced",
                lambda ctx: "carbon_aware",
            ])
        else:
            self.distiller = None

        self.safety_monitor = safety_monitor or SafetyMonitor()
        self.xai = xai or XAIExplainer()
        self.multi_agent = multi_agent or MultiAgentCoordinator(
            agents=config.multi_agent.agents if config.multi_agent.enabled else None
        )

    def _modp_policy(self, context: Dict) -> str:
        if not self.modp:
            return "balanced"
        objectives = {
            "success": float(context.get("success_rate", 0.5)),
            "latency": 1.0 - (float(context.get("avg_latency", 200)) / 1000.0),
            "carbon": 1.0 - (float(context.get("carbon_intensity", 400)) / 800.0),
            "cost": 0.5,
        }
        scores: Dict[str, float] = {}
        for policy in self.param_policies:
            if policy == "aggressive":
                obj = {**objectives, "latency": 0.9, "cost": 0.8}
            elif policy == "conservative":
                obj = {**objectives, "latency": 0.3, "cost": 0.3}
            elif policy == "carbon_aware":
                obj = {**objectives, "carbon": 0.9}
            else:
                obj = objectives
            try:
                scores[policy] = float(self.modp.evaluate(obj, self.config.optimizer.modp_weights))
            except Exception:
                scores[policy] = 0.0
        return max(scores, key=scores.get)

    async def load_state(self) -> None:
        if self.db_manager is None:
            return
        try:
            state = await self.db_manager.load_optimizer_state("autonomous_fallback_optimizer")
            if not state:
                return
            if "theta" in state and isinstance(state["theta"], dict) and self.bandit:
                for k, v in state["theta"].items():
                    if k in self.bandit.theta:
                        try:
                            self.bandit.theta[k] = np.asarray(v, dtype=float)
                        except Exception:
                            pass
            if "P" in state and isinstance(state["P"], dict) and self.bandit:
                for k, v in state["P"].items():
                    if k in self.bandit.P:
                        try:
                            self.bandit.P[k] = np.asarray(v, dtype=float)
                        except Exception:
                            pass
            if "strategy_fitness" in state:
                self.strategy_fitness = deque(state["strategy_fitness"], maxlen=100)
            if "param_policies" in state:
                self.param_policies = list(state["param_policies"]) or self.param_policies
        except Exception as e:
            logger.warning(f"optimizer_state_load_failed error={e}")

    async def save_state(self) -> None:
        if self.db_manager is None:
            return
        state: Dict[str, Any] = {
            "strategy_fitness": list(self.strategy_fitness),
            "param_policies": list(self.param_policies),
        }
        if self.bandit:
            state["theta"] = {a: self.bandit.theta[a].tolist()
                              for a in self.bandit.theta}
            state["P"] = {a: self.bandit.P[a].tolist()
                          for a in self.bandit.P}
        try:
            await self.db_manager.save_optimizer_state("autonomous_fallback_optimizer", state)
        except Exception as e:
            logger.warning(f"optimizer_state_save_failed error={e}")

    async def optimize_fallbacks(self, performance_data: Dict) -> Dict:
        context = {
            "avg_latency": float(performance_data.get("avg_latency_ms", 0)),
            "success_rate": float(performance_data.get("success_rate", 0)),
            "carbon_intensity": float(performance_data.get("carbon_intensity", 400)),
            "load": float(performance_data.get("load", 0)),
            "retry_rate": float(performance_data.get("retry_rate", 0)),
            "hour": datetime.now().hour,
        }
        self.last_context = context

        if self.safety_monitor:
            safe, reason = self.safety_monitor.check_fallback(
                context["carbon_intensity"], success=(context["success_rate"] > 0.5)
            )
            if not safe:
                logger.warning(f"optimizer_safety_violation reason={reason}")

        policy = "balanced"
        confidence = 0.5
        source = "fallback"
        if self.distiller:
            policy = self.distiller.distill(context)
            source = "distilled"
        elif self.rlhf:
            policy = self.rlhf.sample_action(context)
            source = "rlhf"
        elif self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            policy, confidence, source = self.bandit.select_action(encoded)

        params = {
            "aggressive": {"max_retries": 5, "circuit_breaker_threshold": 7, "rate_limit_requests": 2000},
            "conservative": {"max_retries": 2, "circuit_breaker_threshold": 3, "rate_limit_requests": 500},
            "carbon_aware": {"max_retries": 3, "circuit_breaker_threshold": 5, "rate_limit_requests": 1000},
        }.get(policy, {"max_retries": 3, "circuit_breaker_threshold": 5, "rate_limit_requests": 1000})

        if self.limit_graph:
            limits = self.limit_graph.get_limits(context)
            if limits.get("max_retries"):
                params["max_retries"] = min(params["max_retries"], int(limits["max_retries"]))
            if limits.get("min_retries"):
                params["max_retries"] = max(params["max_retries"], int(limits["min_retries"]))
            if limits.get("max_rate_limit"):
                params["rate_limit_requests"] = min(
                    params["rate_limit_requests"], int(limits["max_rate_limit"])
                )

        strategies = await self._select_strategies(performance_data)
        results: Dict[str, Any] = {}
        for strategy in strategies:
            fn = self.optimization_strategies.get(strategy)
            if fn is None:
                continue
            try:
                result = await fn(performance_data)
                results[strategy] = result
                async with self._lock:
                    self.optimization_history.append({
                        "strategy": strategy, "result": result,
                        "timestamp": _utc_now().isoformat(),
                    })
            except Exception as e:
                logger.error(f"strategy_failed strategy={strategy} error={e}")
                results[strategy] = {"status": "failed", "error": str(e)}

        success = context["success_rate"]
        latency = context["avg_latency"]
        carbon = context["carbon_intensity"]
        reward = success * 0.4 + (1 - latency / 1000) * 0.3 + (1 - carbon / 800) * 0.3

        if self.rlhf:
            with contextlib.suppress(Exception):
                self.rlhf.update(context, policy, reward)
        if self.limit_graph:
            with contextlib.suppress(Exception):
                self.limit_graph.update_from_feedback({
                    "performance": performance_data, "success": reward > 0.5,
                })

        # Sync bandit update — do NOT await.
        if self.bandit is not None:
            encoded = self.moe.encode(context) if self.moe else context
            self.bandit.update(encoded, policy, reward)

        # Only update contributing agents.
        if self.multi_agent:
            self._last_contributing_agents = self.multi_agent.select_agents(context, top_k=2)
            self.multi_agent.record_outcome(self._last_contributing_agents, success > 0.5)

        if self.bio:
            self.strategy_fitness.append(reward)
            if len(self.strategy_fitness) >= 20:
                def fitness(_):
                    return float(np.mean(list(self.strategy_fitness))) if self.strategy_fitness else 0.5
                try:
                    new_population = self.bio.evolve(
                        population=self.strategy_population,
                        fitness_fn=fitness,
                        generations=self.config.optimizer.bio_generations,
                        population_size=self.config.optimizer.bio_population_size,
                    )
                    if new_population:
                        self.strategy_population = new_population
                        await self.save_state()
                except Exception as e:
                    logger.warning(f"bio_evolve_failed error={e}")

        explanation = self.xai.explain_optimization(policy, source, len(results))
        self.last_explanation = explanation["narrative"]
        self.last_policy = policy
        self.last_source = source
        XAI_DECISIONS.labels(policy=policy).inc()
        AUTONOMOUS_OPTIMIZATIONS.labels(status="success").inc()

        return {
            "status": "success",
            "strategies_applied": len(results),
            "results": results,
            "params": params,
            "policy": policy,
            "source": source,
            "confidence": confidence,
            "explanation": explanation,
            "timestamp": _utc_now().isoformat(),
        }

    async def _select_strategies(self, data: Dict) -> List[str]:
        strategies: List[str] = []
        if data.get("avg_latency_ms", 0) > 200:
            strategies.append("reduce_latency")
        if data.get("success_rate", 0) < 0.8:
            strategies.append("improve_success")
        if data.get("carbon_intensity", 0) > 400:
            strategies.append("reduce_carbon")
        if data.get("load", 0) > 0.8:
            strategies.append("balance_load")
        if data.get("retry_rate", 0) > 0.3:
            strategies.append("optimize_retries")
        if not strategies:
            strategies.append("improve_success")
        return strategies[:4]

    async def _reduce_latency(self, data: Dict) -> Dict:
        current = float(data.get("avg_latency_ms", 200))
        return {
            "action": "reduce_latency",
            "current_latency_ms": current,
            "target_latency_ms": current * 0.7,
            "recommendation": "Reduce retry timeout and circuit breaker timeout",
        }

    async def _improve_success(self, data: Dict) -> Dict:
        current = float(data.get("success_rate", 0.85))
        return {
            "action": "improve_success",
            "current_success_rate": current,
            "target_success_rate": min(0.99, current * 1.1),
            "recommendation": "Add more fallback handlers and improve retry strategy",
        }

    async def _reduce_carbon(self, data: Dict) -> Dict:
        current = float(data.get("carbon_intensity", 400))
        return {
            "action": "reduce_carbon",
            "current_carbon_intensity": current,
            "target_carbon_intensity": current * 0.8,
            "recommendation": "Schedule fallbacks during low-carbon periods",
        }

    async def _balance_load(self, data: Dict) -> Dict:
        current = float(data.get("load", 0.7))
        return {
            "action": "balance_load",
            "current_load": current,
            "target_load": 0.5,
            "recommendation": "Distribute fallback load across multiple handlers",
        }

    async def _optimize_retries(self, data: Dict) -> Dict:
        current = float(data.get("retry_rate", 0.3))
        return {
            "action": "optimize_retries",
            "current_retry_rate": current,
            "target_retry_rate": current * 0.6,
            "recommendation": "Implement exponential backoff with jitter",
        }

    async def get_optimization_status(self) -> Dict:
        async with self._lock:
            return {
                "active_optimizations": len(self.active_optimizations),
                "optimization_history": len(self.optimization_history),
                "recent_optimizations": list(self.optimization_history)[-5:],
                "available_strategies": list(self.optimization_strategies.keys()),
                "enhancements_available": ENHANCEMENTS_AVAILABLE,
                "bandit": self.bandit.get_stats() if self.bandit else None,
                "modp_weights": dict(self.config.optimizer.modp_weights),
                "bio_available": self.bio is not None,
                "limit_graph_active": self.limit_graph is not None,
                "rlhf_active": self.rlhf is not None,
                "distillation_active": self.distiller is not None,
                "causal_bandit": isinstance(self.bandit, CausalBandit) if self.bandit else False,
                "safety_violations": self.safety_monitor.get_violations()[-5:]
                if self.safety_monitor else [],
                "multi_agent_stats": self.multi_agent.get_stats() if self.multi_agent else None,
                "last_explanation": self.last_explanation,
                "last_policy": self.last_policy,
                "last_source": self.last_source,
            }

    async def health_check(self) -> Dict:
        return {"status": "healthy"}


# =============================================================================
# Federated learner
# =============================================================================
class FederatedFallbackLearner(IFederatedLearner):
    def __init__(self, config: FallbackManagerConfig,
                 db_manager: IDatabaseManager, instance_id: str):
        self.config = config
        self.db_manager = db_manager
        self.instance_id = instance_id
        self.patterns: List[Dict[str, Any]] = []
        self.federated_enabled = config.federated.enabled
        self.secure_coordinator = FederatedSecureCoordinator(
            privacy_budget=config.federated.privacy_budget,
            sensitivity=config.federated.sensitivity,
        )

    async def pull_network_patterns(self, domain: Optional[str] = None,
                                    limit: int = 5) -> List[Dict]:
        aggregated = self.secure_coordinator.aggregate()
        if aggregated:
            return [{"source": "secure_aggregation", "pattern": aggregated}]
        return []

    async def push_pattern(self, pattern: Dict) -> None:
        self.patterns.append(pattern)
        numeric = {k: v for k, v in pattern.items() if isinstance(v, (int, float))}
        if numeric:
            self.secure_coordinator.register_participant(
                f"fallback_{self.instance_id}_{len(self.patterns)}",
                numeric,
                sample_count=1,
            )
        FEDERATED_SHARES.labels(source=self.instance_id).inc()

    async def health_check(self) -> Dict:
        return {
            "status": "ok",
            "federated_enabled": self.federated_enabled,
            "federated_participants": self.secure_coordinator.get_participant_count(),
        }


# =============================================================================
# PredictiveFallbackReflexivity
# =============================================================================
class PredictiveFallbackReflexivity(IPredictiveReflexivity):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE and config.predictive.enabled
        self.history: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.model_storage = Path(config.predictive.model_storage_path)
        with contextlib.suppress(Exception):
            self.model_storage.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

        if ENHANCEMENTS_AVAILABLE and config.predictive.evolve_hyperparams:
            self.bio = GeneticPolicyGenerator()
            self.hyperparam_population = [
                {"changepoint_prior_scale": 0.05, "seasonality_prior_scale": 10},
                {"changepoint_prior_scale": 0.01, "seasonality_prior_scale": 5},
                {"changepoint_prior_scale": 0.1, "seasonality_prior_scale": 20},
            ]
            self.hyperparam_fitness: Deque[float] = deque(maxlen=100)
        else:
            self.bio = None
            self.hyperparam_population: List[Dict[str, float]] = []
            self.hyperparam_fitness: Deque[float] = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._teacher_baseline, self._teacher_auto, self._teacher_advanced,
            ])
        else:
            self.distiller = None

    def _teacher_baseline(self, _data: Any = None) -> Dict:
        return {"changepoint_prior_scale": 0.05, "seasonality_prior_scale": 10}

    def _teacher_auto(self, data: Any = None) -> Dict:
        try:
            n = len(data)
        except Exception:
            n = 0
        if n > 100:
            return {"changepoint_prior_scale": 0.01, "seasonality_prior_scale": 5}
        return {"changepoint_prior_scale": 0.1, "seasonality_prior_scale": 20}

    def _teacher_advanced(self, _data: Any = None) -> Dict:
        if self.bio and self.hyperparam_population:
            fitness = (
                lambda hp: float(np.mean(list(self.hyperparam_fitness)))
                if self.hyperparam_fitness else 0.5
            )
            try:
                new_pop = self.bio.evolve(
                    self.hyperparam_population, fitness,
                    generations=self.config.predictive.hyperparam_generations,
                    population_size=self.config.predictive.hyperparam_population_size,
                )
                if new_pop:
                    self.hyperparam_population = new_pop
                    return max(new_pop, key=fitness)
            except Exception:
                pass
        return {"changepoint_prior_scale": 0.05, "seasonality_prior_scale": 10}

    async def update_history(self, data: Dict) -> None:
        async with self._lock:
            ts = data.get("timestamp")
            try:
                ds = datetime.fromisoformat(ts) if ts else _utc_now()
            except Exception:
                ds = _utc_now()
            self.history.append({"ds": ds, "y": 1.0 if data.get("success", False) else 0.0})

    async def load_model(self, model_name: str) -> Optional[Any]:
        path = self.model_storage / f"{model_name}.prophet"
        if not path.exists():
            return None
        try:
            return await asyncio.to_thread(Prophet.load, str(path))
        except Exception as e:
            logger.warning(f"prophet_load_failed error={e}")
            return None

    async def save_model(self, model_name: str, model: Any) -> None:
        path = self.model_storage / f"{model_name}.prophet"
        try:
            await asyncio.to_thread(model.save, str(path))
        except Exception as e:
            logger.error(f"prophet_save_failed error={e}")

    async def get_fallback_forecast(self, horizon_hours: int = 0) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        if not self.prophet_available or len(self.history) < 30:
            return {"forecast": [], "confidence": 0.0, "model": "fallback"}

        import pandas as pd

        df = pd.DataFrame(list(self.history)).sort_values("ds")

        changepoint = 0.05
        seasonality = 10
        if self.distiller:
            with contextlib.suppress(Exception):
                best = self.distiller.distill(df)
                changepoint = best.get("changepoint_prior_scale", changepoint)
                seasonality = best.get("seasonality_prior_scale", seasonality)
        elif self.bio and self.hyperparam_population:
            best = max(
                self.hyperparam_population,
                key=lambda p: float(np.mean(list(self.hyperparam_fitness)))
                if self.hyperparam_fitness else 0.5,
            )
            changepoint = best.get("changepoint_prior_scale", changepoint)
            seasonality = best.get("seasonality_prior_scale", seasonality)

        try:
            def _fit_predict():
                model = Prophet(
                    changepoint_prior_scale=changepoint,
                    seasonality_prior_scale=seasonality,
                )
                model.fit(df)
                future = model.make_future_dataframe(periods=horizon)
                return model.predict(future)

            forecast = await asyncio.to_thread(_fit_predict)
            tail = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(horizon)

            # Record a residual-based fitness sample.
            try:
                last_actual = float(df["y"].iloc[-1])
                last_pred = float(forecast["yhat"].iloc[-2]) if len(forecast) >= 2 else last_actual
                err = abs(last_actual - last_pred)
                self.hyperparam_fitness.append(1.0 / (1.0 + err))
                PREDICTIVE_ACCURACY.labels(model="prophet").set(
                    max(0.0, 1.0 - err / max(last_actual, 1e-6))
                )
            except Exception:
                pass

            return {
                "forecast": tail["yhat"].tolist(),
                "lower_bound": tail["yhat_lower"].tolist(),
                "upper_bound": tail["yhat_upper"].tolist(),
                "dates": tail["ds"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                "confidence": 0.9,
                "model": "prophet",
            }
        except Exception as e:
            logger.error(f"prophet_forecast_failed error={e}")
            PREDICTIVE_ACCURACY.labels(model="prophet").set(0.0)
            return {"forecast": [], "confidence": 0.0, "model": "fallback"}

    async def health_check(self) -> Dict:
        return {
            "status": "healthy" if self.prophet_available else "degraded",
            "prophet_available": self.prophet_available,
            "samples": len(self.history),
            "hyperparam_evolution_enabled": self.bio is not None,
            "distillation_enabled": self.distiller is not None,
        }


# =============================================================================
# Sustainability tracker
# =============================================================================
class FallbackSustainabilityTracker(ISustainabilityTracker):
    def __init__(self, config: FallbackManagerConfig, db_manager: IDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.metrics: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._lock = threading.RLock()

    async def record_metric(self, metric_name: str, value: float,
                            metadata: Optional[Dict] = None) -> None:
        with self._lock:
            self.metrics[metric_name].append({
                "value": float(value),
                "metadata": metadata or {},
                "timestamp": _utc_now().isoformat(),
            })

    async def get_fallback_sustainability_score(self) -> Dict:
        with self._lock:
            eff = self.metrics.get("fallback_efficiency", [])
            avg = float(np.mean([m["value"] for m in eff])) if eff else 0.0
        return {
            "overall_score": avg * 100.0,
            "components": {"efficiency": avg},
        }

    async def get_fallback_savings(self) -> Dict:
        return {"co2_saved_kg": 0.0, "energy_saved_kwh": 0.0, "cost_saved_usd": 0.0}

    async def health_check(self) -> Dict:
        return {"status": "ok"}


# =============================================================================
# WebSocket server
# =============================================================================
class WebSocketServer(IWebSocketServer):
    def __init__(self, config: FallbackManagerConfig, manager: Any = None):
        self.config = config
        self.manager = manager
        self.clients: Set[Any] = set()
        self.server = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if not WEBSOCKETS_AVAILABLE or not self.config.websocket.enabled:
            logger.info("websocket_disabled")
            return

        async def handler(websocket, path=None):
            async with self._lock:
                self.clients.add(websocket)
            try:
                async for message in websocket:
                    if message == "ping":
                        await websocket.send("pong")
            except ConnectionClosed:
                pass
            except Exception:
                pass
            finally:
                async with self._lock:
                    self.clients.discard(websocket)

        try:
            self.server = await serve(handler, "0.0.0.0", self.config.websocket.port)
            logger.info(f"websocket_started port={self.config.websocket.port}")
        except Exception as e:
            logger.warning(f"websocket_start_failed error={e}")
            self.server = None

    async def stop(self) -> None:
        if self.server:
            with contextlib.suppress(Exception):
                self.server.close()
                await self.server.wait_closed()
            self.server = None

    async def broadcast(self, message: Dict) -> None:
        async with self._lock:
            clients = list(self.clients)
        if not clients:
            return
        data = json.dumps(message, default=str)
        for ws in clients:
            try:
                await ws.send(data)
            except Exception:
                async with self._lock:
                    self.clients.discard(ws)

    async def health_check(self) -> Dict:
        return {"status": "ok" if WEBSOCKETS_AVAILABLE else "unavailable",
                "clients": len(self.clients)}


# =============================================================================
# MultiCloudStorage — honest
# =============================================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and config.cloud.aws_bucket and config.cloud.aws_access_key:
            try:
                self.providers["aws"] = {
                    "client": boto3.client(
                        "s3",
                        region_name=config.cloud.aws_region,
                        aws_access_key_id=config.cloud.aws_access_key,
                        aws_secret_access_key=config.cloud.aws_secret_key,
                    ),
                    "bucket": config.cloud.aws_bucket,
                }
            except Exception as e:
                logger.warning(f"aws_client_init_failed error={e}")

    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict:
        filename = filename or f"data_{uuid.uuid4().hex[:8]}.json"
        payload = json.dumps(data, default=str)
        uploaded: List[str] = []
        if "aws" in self.providers:
            try:
                await asyncio.to_thread(
                    self.providers["aws"]["client"].put_object,
                    Bucket=self.providers["aws"]["bucket"],
                    Key=filename, Body=payload.encode(),
                )
                uploaded.append("aws")
                CLOUD_STORAGE.labels(provider="aws", operation="store", status="success").inc()
            except Exception as e:
                CLOUD_STORAGE.labels(provider="aws", operation="store", status="failed").inc()
                logger.warning(f"aws_upload_failed error={e}")
        if not uploaded:
            raise NotImplementedYetError("No cloud provider configured")
        return {"filename": filename, "providers": uploaded}

    async def health_check(self) -> Dict:
        return {"status": "ok", "providers": list(self.providers.keys())}


# =============================================================================
# Leader election
# =============================================================================
class LeaderElection:
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.is_leader = True

    async def try_acquire_leadership(self) -> bool:
        return self.is_leader

    async def stop(self) -> None:
        self.is_leader = False


# =============================================================================
# Rate limiter
# =============================================================================
class RateLimiter:
    def __init__(self, rate: int, per_seconds: int = 60):
        self.rate = float(rate)
        self.per_seconds = float(per_seconds)
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            self.throttled_requests += 1
            return False

    async def wait_and_acquire(self) -> None:
        while not await self.acquire():
            await asyncio.sleep(0.1)

    def get_metrics(self) -> Dict:
        total = self.total_requests + self.throttled_requests
        return {
            "total_requests": self.total_requests,
            "throttled_requests": self.throttled_requests,
            "throttle_rate": (self.throttled_requests / max(total, 1)) * 100,
        }


# =============================================================================
# Task manager
# =============================================================================
class TaskManager:
    def __init__(self, max_workers: int = 10):
        self.max_workers = int(max_workers)
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Tuple[Callable[..., Any], tuple, dict]] = {}
        self._health: Dict[str, Dict[str, Any]] = {}
        self.metrics = {"total_tasks": 0, "completed": 0, "failed": 0}

    def register_task(self, name: str, coro_func: Callable[..., Any], *args, **kwargs) -> None:
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def _start(self, name: str, fn: Callable[..., Any], *args, **kwargs) -> asyncio.Task:
        health = self._health.setdefault(name, {
            "name": name, "starts": 0, "crashes": 0, "last_error": None,
        })

        async def wrapper():
            backoff = 1.0
            max_backoff = 300.0
            health["starts"] += 1
            while not self.shutdown_event.is_set():
                try:
                    await fn(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    health["crashes"] += 1
                    health["last_error"] = f"{type(e).__name__}: {e}"
                    TASK_ERRORS.labels(task_name=name).inc()
                    logger.error(f"task_crashed name={name} error={e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def start_registered_tasks(self) -> None:
        for name, (fn, args, kwargs) in self._task_coroutines.items():
            self._start(name, fn, *args, **kwargs)
        self._task_coroutines.clear()

    async def submit(self, coro, name: Optional[str] = None,
                     priority: str = "normal", timeout: Optional[float] = None) -> str:
        task_name = name or f"task_{uuid.uuid4().hex[:8]}"

        async def wrapper():
            try:
                result = await asyncio.wait_for(coro, timeout=timeout)
                async with self._lock:
                    self.metrics["completed"] += 1
                return result
            except asyncio.TimeoutError:
                async with self._lock:
                    self.metrics["failed"] += 1
                raise
            except Exception:
                async with self._lock:
                    self.metrics["failed"] += 1
                raise

        task = asyncio.create_task(wrapper(), name=task_name)
        async with self._lock:
            self.tasks[task_name] = task
            self.metrics["total_tasks"] += 1
        return task_name

    async def stop_all(self) -> None:
        self.shutdown_event.set()
        async with self._lock:
            tasks = list(self.tasks.values())
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.tasks.clear()
        logger.info("all_tasks_stopped")

    def get_statistics(self) -> Dict:
        return {**self.metrics, "active_tasks": len(self.tasks), "health": self._health}


# =============================================================================
# EnhancedFallbackManagerV15_0 — main entry point
# =============================================================================
class EnhancedFallbackManagerV15_0:
    def __init__(
        self,
        config: FallbackManagerConfig,
        db_manager: IDatabaseManager,
        quantum_security: IQuantumSecurity,
        blockchain: IBlockchain,
        carbon_manager: ICarbonManager,
        llm_generator: ILLMGenerator,
        load_shedder: ILoadShedder,
        region_coordinator: IRegionCoordinator,
        autonomous_optimizer: IAutonomousOptimizer,
        federated_learner: IFederatedLearner,
        predictive_reflexivity: IPredictiveReflexivity,
        sustainability_tracker: ISustainabilityTracker,
        websocket_server: IWebSocketServer,
        cloud_storage: ICloudStorage,
        vault: IVault,
        leader: LeaderElection,
        task_manager: TaskManager,
        chaos_monkey: Optional[ChaosMonkey] = None,
    ):
        self.config = config
        self.instance_id = config.general.instance_id
        self._start_time = _utc_now()

        self.db_manager = db_manager
        self.quantum_security = quantum_security
        self.blockchain = blockchain
        self.carbon_manager = carbon_manager
        self.llm_generator = llm_generator
        self.load_shedder = load_shedder
        self.region_coordinator = region_coordinator
        self.autonomous_optimizer = autonomous_optimizer
        self.federated_learner = federated_learner
        self.predictive_reflexivity = predictive_reflexivity
        self.sustainability_tracker = sustainability_tracker
        self.websocket_server = websocket_server
        self.cloud_storage = cloud_storage
        self.vault = vault
        self.leader = leader
        self.task_manager = task_manager

        # Advanced modules
        self.safety_monitor = SafetyMonitor(
            max_fallbacks_per_hour=config.safety.max_fallbacks_per_hour,
            max_carbon_for_fallback=config.safety.max_carbon_for_fallback,
            max_consecutive_failures=config.safety.max_consecutive_failures,
        ) if config.safety.enabled else None
        self.xai = XAIExplainer()
        self.carbon_broker = CarbonOffsetBroker(
            threshold=config.carbon.offset_threshold,
            cost_per_kg=config.carbon.offset_cost_per_kg,
            rec_cost_per_mwh=config.carbon.rec_cost_per_mwh,
        )
        self.chaos_monkey = chaos_monkey or ChaosMonkey(
            enabled=config.chaos.enabled,
            failure_probability=config.chaos.failure_probability,
            seed=config.chaos.seed,
        )
        self.human_review = HumanReviewManager(
            ttl_seconds=config.safety.hitl_ttl_seconds,
            max_pending=config.safety.hitl_max_pending,
        )
        self.precision_policy = FlexGenPrecisionPolicy(default_carbon_intensity=400.0)
        self.multi_agent = MultiAgentCoordinator(
            agents=config.multi_agent.agents if config.multi_agent.enabled else None
        )
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )

        # Share instances with the optimizer so they observe the same state.
        if hasattr(self.autonomous_optimizer, "safety_monitor"):
            self.autonomous_optimizer.safety_monitor = self.safety_monitor
        if hasattr(self.autonomous_optimizer, "xai"):
            self.autonomous_optimizer.xai = self.xai
        if hasattr(self.autonomous_optimizer, "multi_agent"):
            self.autonomous_optimizer.multi_agent = self.multi_agent

        self.fallback_handlers: Dict[str, List[Callable[[Dict], Awaitable[Any]]]] = defaultdict(list)
        self.fallback_history: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self._history_lock = asyncio.Lock()
        self._running = False
        self._startup_lock = asyncio.Lock()

        self._register_background_tasks()
        logger.info(
            f"fallback_manager_initialized version={config.general.version} "
            f"instance={self.instance_id}"
        )

    async def __aenter__(self) -> "EnhancedFallbackManagerV15_0":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    def _register_background_tasks(self) -> None:
        self.task_manager.register_task("health_monitor", self._health_monitor_loop)
        self.task_manager.register_task("quantum_monitor", self._quantum_monitor_loop)
        self.task_manager.register_task("blockchain_monitor", self._blockchain_monitor_loop)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        self.task_manager.register_task("federated_sync", self._federated_sync_loop)
        self.task_manager.register_task("sustainability_reporter", self._sustainability_reporter_loop)
        self.task_manager.register_task("auto_optimize", self._auto_optimize_loop)
        self.task_manager.register_task("websocket", self.websocket_server.start)

    async def start(self) -> None:
        async with self._startup_lock:
            if self._running:
                return
            logger.info("fallback_manager_starting")
            init = getattr(self.db_manager, "init", None)
            if init is not None:
                result = init()
                if inspect.isawaitable(result):
                    await result
            # Load optimizer state now that the DB is initialized.
            load_state = getattr(self.autonomous_optimizer, "load_state", None)
            if load_state is not None:
                with contextlib.suppress(Exception):
                    result = load_state()
                    if inspect.isawaitable(result):
                        await result

            self._running = True
            self.task_manager.start_registered_tasks()
            BACKGROUND_TASKS.set(len(self.task_manager.tasks))
            logger.info(f"fallback_manager_started tasks={len(self.task_manager.tasks)}")

    def register_fallback_handler(self, name: str,
                                  handlers: List[Callable[[Dict], Awaitable[Any]]]) -> None:
        self.fallback_handlers[name] = list(handlers)
        logger.info(f"fallback_handlers_registered name={name} count={len(handlers)}")

    # ------------------------------------------------------------------ #
    # Background loops                                                    #
    # ------------------------------------------------------------------ #
    async def _carbon_update_loop(self) -> None:
        interval = max(10, int(self.config.scheduler.carbon_update_interval))
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"carbon_update_error error={e}")
                await asyncio.sleep(60)

    async def _quantum_monitor_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                status = self.quantum_security.get_quantum_status()
                if not status.get("pqc_available"):
                    logger.warning("pqc_unavailable using_ecdsa_fallback")
                await asyncio.sleep(600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"quantum_monitor_error error={e}")
                await asyncio.sleep(60)

    async def _blockchain_monitor_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                status = await self.blockchain.get_blockchain_status()
                with contextlib.suppress(Exception):
                    await self.websocket_server.broadcast({
                        "type": "blockchain_status", "data": status,
                    })
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"blockchain_monitor_error error={e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                async with self._history_lock:
                    recent = list(self.fallback_history)[-10:]
                for h in recent:
                    await self.predictive_reflexivity.update_history(h)
                await asyncio.sleep(self.config.scheduler.predictive_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"predictive_update_error error={e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                patterns = await self.federated_learner.pull_network_patterns(limit=5)
                if patterns:
                    logger.info(f"federated_patterns_applied count={len(patterns)}")
                await asyncio.sleep(self.config.scheduler.federated_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"federated_sync_error error={e}")
                await asyncio.sleep(60)

    async def _sustainability_reporter_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                score = await self.sustainability_tracker.get_fallback_sustainability_score()
                savings = await self.sustainability_tracker.get_fallback_savings()
                with contextlib.suppress(Exception):
                    await self.websocket_server.broadcast({
                        "type": "sustainability",
                        "data": {"score": score, "savings": savings},
                    })
                await asyncio.sleep(self.config.scheduler.sustainability_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"sustainability_reporter_error error={e}")
                await asyncio.sleep(60)

    async def _auto_optimize_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                intensity_data = await self.carbon_manager.get_current_intensity()
                async with self._history_lock:
                    recent = list(self.fallback_history)[-50:]
                performance_data = {
                    "avg_latency_ms": float(np.mean([h.get("latency_ms", 150) for h in recent])) if recent else 0.0,
                    "success_rate": float(np.mean([1.0 if h.get("success") else 0.0 for h in recent])) if recent else 0.0,
                    "carbon_intensity": float(intensity_data.get("intensity", 400)),
                    "load": self.load_shedder.get_statistics().get("current", 0)
                            / max(self.load_shedder.get_statistics().get("max_concurrent", 1), 1),
                    "retry_rate": float(np.mean([1.0 if h.get("retry_count", 0) > 1 else 0.0 for h in recent])) if recent else 0.0,
                }
                result = await self.autonomous_optimizer.optimize_fallbacks(performance_data)
                if result.get("status") == "success":
                    key = await self.quantum_security.generate_keypair("dilithium")
                    signature = await self.quantum_security.sign_fallback_decision(
                        result, key["key_id"]
                    )
                    result["quantum_signature"] = signature
                    with contextlib.suppress(Exception):
                        await self.websocket_server.broadcast({
                            "type": "optimization", "data": result,
                        })
                await asyncio.sleep(self.config.scheduler.auto_tune_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"auto_optimize_error error={e}")
                await asyncio.sleep(60)

    async def _health_monitor_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get("health_score", 100))
                if health.get("status") != "healthy":
                    with contextlib.suppress(Exception):
                        await self.websocket_server.broadcast({
                            "type": "health_warning", "data": health,
                        })
                await asyncio.sleep(self.config.scheduler.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"health_monitor_error error={e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------ #
    # Fallback execution                                                  #
    # ------------------------------------------------------------------ #
    async def execute_with_fallback(self, handler_name: str,
                                    context: Optional[Dict[str, Any]] = None) -> Any:
        start_time = time.time()
        context = dict(context or {})
        fallback_id = str(uuid.uuid4())[:8]

        fault = self.chaos_monkey.maybe_fail("fallback")
        if fault is not None:
            try:
                self.chaos_monkey.apply(fault, self)
            except Exception as e:
                logger.warning(f"chaos_apply_failed error={e}")

        try:
            carbon_intensity = float(
                (await self.carbon_manager.get_current_intensity()).get("intensity", 400)
            )

            if self.safety_monitor:
                safe, reason = self.safety_monitor.check_fallback(
                    carbon_intensity, success=True
                )
                if not safe:
                    logger.warning(f"fallback_safety_violation reason={reason}")

            region_strategy = await self.region_coordinator.coordinate_fallback(
                handler_name,
                {"latency_weight": 0.4, "carbon_weight": 0.3, "capacity_weight": 0.3},
            )
            precision = self.precision_policy.recommend_precision(
                workload_size="medium", carbon_intensity=carbon_intensity,
            )

            quantum_key = await self.quantum_security.generate_keypair("dilithium")
            decision_manifest = {
                "fallback_id": fallback_id,
                "handler": handler_name,
                "timestamp": _utc_now().isoformat(),
                "carbon_strategy": {
                    "carbon_intensity": carbon_intensity,
                    "timeout": 30 if carbon_intensity < 400 else 20,
                    "max_retries": 3 if carbon_intensity < 400 else 2,
                },
                "region_strategy": region_strategy,
                "precision": precision,
            }
            signature = await self.quantum_security.sign_fallback_decision(
                decision_manifest, quantum_key["key_id"]
            )
            decision_manifest["quantum_signature"] = signature

            cb = GlobalCircuitBreaker().get_or_create(
                handler_name,
                failure_threshold=self.config.circuit_breaker.failure_threshold,
                recovery_timeout=self.config.circuit_breaker.recovery_timeout,
            )
            handlers = self.fallback_handlers.get(handler_name, [])
            if not handlers:
                raise FallbackManagerError(f"No fallback handlers for {handler_name}")

            last_exception: Optional[Exception] = None
            failures_this_call = 0

            for level, handler in enumerate(handlers):
                degradation_level = f"level_{level}"
                acquired, queue_event = await self.load_shedder.acquire()
                if not acquired and queue_event is not None:
                    waited = False
                    wait_fn = getattr(self.load_shedder, "wait_or_cancel", None)
                    if wait_fn is not None:
                        waited = await wait_fn(queue_event, timeout=30)
                    else:
                        try:
                            await asyncio.wait_for(queue_event.wait(), timeout=30)
                            waited = True
                        except asyncio.TimeoutError:
                            waited = False
                    if not waited:
                        last_exception = LoadSheddingError("Load shedding timeout")
                        failures_this_call += 1
                        cb.record_failure()
                        continue

                try:
                    # The circuit breaker gates the actual handler invocation.
                    result = await cb.call_async(handler, context)
                    latency_ms = (time.time() - start_time) * 1000
                    explanation = self.xai.explain_fallback(
                        handler_name, f"level_{level}", context, 0.8,
                        region_strategy.get("primary_region", "unknown"), precision,
                    )

                    record = {
                        "handler_name": handler_name,
                        "strategy_used": f"level_{level}",
                        "degradation_level": degradation_level,
                        "latency_ms": latency_ms,
                        "retry_count": 0,
                        "success": True,
                        "carbon_intensity": carbon_intensity,
                        "region": region_strategy.get("primary_region", "unknown"),
                        "precision": precision,
                        "explanation": explanation,
                    }
                    async with self._history_lock:
                        self.fallback_history.append(record)

                    with contextlib.suppress(Exception):
                        await self.db_manager.save_fallback_history(record)

                    FALLBACK_SUCCESS.labels(handler=handler_name).inc()

                    outcome = {
                        "success": True, "latency_ms": latency_ms,
                        "handler": handler_name, "level": level,
                        "explanation": explanation, "precision": precision,
                    }

                    with contextlib.suppress(NotImplementedYetError, Exception):
                        await self.blockchain.record_fallback(
                            fallback_id, decision_manifest, outcome
                        )

                    await self.sustainability_tracker.record_metric(
                        "fallback_efficiency", 0.9,
                        {"level": level, "success": True},
                    )

                    if carbon_intensity > self.config.carbon.offset_threshold:
                        carbon_kg = latency_ms * 0.00001
                        with contextlib.suppress(Exception):
                            await self.carbon_broker.purchase_offsets(
                                carbon_intensity, carbon_kg
                            )

                    contributing = (
                        self.multi_agent.select_agents(context, top_k=2)
                        if self.multi_agent else []
                    )
                    if contributing:
                        self.multi_agent.record_outcome(contributing, True)

                    return result
                except Exception as e:
                    last_exception = e
                    failures_this_call += 1
                    cb.record_failure()
                    latency_ms = (time.time() - start_time) * 1000
                    async with self._history_lock:
                        self.fallback_history.append({
                            "handler_name": handler_name,
                            "strategy_used": f"level_{level}",
                            "degradation_level": degradation_level,
                            "latency_ms": latency_ms,
                            "success": False,
                            "carbon_intensity": carbon_intensity,
                            "region": region_strategy.get("primary_region", "unknown"),
                            "precision": precision,
                            "error": str(e),
                        })
                    FALLBACK_FAILURE.labels(handler=handler_name).inc()
                    FALLBACK_TRIGGERED.labels(
                        handler=handler_name,
                        level=degradation_level,
                        reason="handler_failure",
                    ).inc()
                finally:
                    await self.load_shedder.release()

            if failures_this_call >= self.config.general.human_review_threshold_failures:
                with contextlib.suppress(Exception):
                    review_id = await self.human_review.request_review(fallback_id, {
                        "handler_name": handler_name,
                        "failures": failures_this_call,
                        "context": context,
                        "last_error": str(last_exception) if last_exception else "",
                    })
                    logger.info(
                        f"fallback_flagged_for_review fallback_id={fallback_id} "
                        f"review_id={review_id}"
                    )

            # Federated fallback as a last resort.
            try:
                patterns = await self.federated_learner.pull_network_patterns(
                    domain=handler_name, limit=1
                )
                if patterns:
                    await self.sustainability_tracker.record_metric(
                        "fallback_efficiency", 0.6, {"source": "federated"}
                    )
                    return patterns[0]["pattern"].get("result", "federated_fallback")
            except Exception as e:
                logger.error(f"federated_fallback_failed error={e}")

            outcome = {
                "success": False,
                "error": str(last_exception) if last_exception else "All fallbacks failed",
            }
            with contextlib.suppress(NotImplementedYetError, Exception):
                await self.blockchain.record_fallback(
                    fallback_id, decision_manifest, outcome
                )
            if self.safety_monitor:
                self.safety_monitor.check_fallback(carbon_intensity, success=False)
            if self.multi_agent:
                contributing = self.multi_agent.select_agents(context, top_k=2)
                self.multi_agent.record_outcome(contributing, False)

            if last_exception is not None:
                raise last_exception
            raise FallbackManagerError(f"All fallbacks failed for {handler_name}")
        finally:
            if fault is not None:
                with contextlib.suppress(Exception):
                    self.chaos_monkey.cleanup_fault(fault, self)

    # ------------------------------------------------------------------ #
    # Health / status                                                     #
    # ------------------------------------------------------------------ #
    async def health_check(self) -> Dict:
        components: Dict[str, Any] = {
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "carbon_manager": self.carbon_manager,
            "llm_generator": self.llm_generator,
            "load_shedder": self.load_shedder,
            "region_coordinator": self.region_coordinator,
            "autonomous_optimizer": self.autonomous_optimizer,
            "federated_learner": self.federated_learner,
            "predictive_reflexivity": self.predictive_reflexivity,
            "sustainability_tracker": self.sustainability_tracker,
            "websocket_server": self.websocket_server,
            "cloud_storage": self.cloud_storage,
            "database": self.db_manager,
            "vault": self.vault,
        }
        results: Dict[str, Any] = {}
        for name, comp in components.items():
            if comp is None:
                results[name] = {"status": "unavailable"}
                continue
            hc = getattr(comp, "health_check", None)
            if hc is None:
                results[name] = {"status": "ok"}
                continue
            try:
                res = hc()
                if inspect.isawaitable(res):
                    res = await res
                results[name] = res
            except Exception as e:
                results[name] = {"status": "unhealthy", "error": str(e)}

        ok = all(
            r.get("status") in ("ok", "healthy", "unavailable")
            for r in results.values()
        )
        overall = "healthy" if ok else "degraded"
        score = 100 if overall == "healthy" else 50
        HEALTH_SCORE.set(score)
        return {
            "status": overall,
            "health_score": score,
            "components": results,
            "timestamp": _utc_now().isoformat(),
        }

    async def get_system_status(self) -> Dict:
        task_stats = self.task_manager.get_statistics()
        sustainability_score = await self.sustainability_tracker.get_fallback_sustainability_score()
        savings = await self.sustainability_tracker.get_fallback_savings()
        async with self._history_lock:
            recent = list(self.fallback_history)[-50:]
            total = len(self.fallback_history)
        recent_success = (
            float(np.mean([1.0 if h.get("success") else 0.0 for h in recent]))
            if recent else 0.0
        )
        return {
            "instance_id": self.instance_id,
            "version": self.config.general.version,
            "running": self._running,
            "background_tasks": task_stats,
            "health": await self.health_check(),
            "load_shedder": self.load_shedder.get_statistics(),
            "llm_stats": self.llm_generator.get_cost_statistics(),
            "fallback_history": {"total": total, "recent_success_rate": recent_success},
            "quantum_security": self.quantum_security.get_quantum_status(),
            "blockchain": await self.blockchain.get_blockchain_status(),
            "autonomous_optimizer": await self.autonomous_optimizer.get_optimization_status(),
            "region_coordinator": await self.region_coordinator.get_region_status(),
            "sustainability": {"score": sustainability_score, "savings": savings},
            "predictive": {
                "prophet_available": getattr(self.predictive_reflexivity, "prophet_available", False),
                "hyperparam_evolution": getattr(self.predictive_reflexivity, "bio", None) is not None,
            },
            "federated": {"enabled": getattr(self.federated_learner, "federated_enabled", False)},
            "cloud_storage": {
                "providers": list(getattr(self.cloud_storage, "providers", {}).keys())
            },
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "additional_enhancements_available": ADDITIONAL_ENHANCEMENTS_AVAILABLE,
            "advanced_enhancements": {
                "safety_monitor": (
                    self.safety_monitor.get_violations()[-5:]
                    if self.safety_monitor else []
                ),
                "chaos_monkey": self.chaos_monkey.get_stats(),
                "human_review": self.human_review.get_stats(),
                "carbon_broker": self.carbon_broker.get_totals(),
                "precision_policy": self.precision_policy.get_status(),
                "quantum_optimizer": self.quantum_optimizer.get_status(),
                "multi_agent": self.multi_agent.get_stats(),
            },
            "timestamp": _utc_now().isoformat(),
        }

    async def shutdown(self) -> None:
        logger.info(f"fallback_manager_shutting_down instance={self.instance_id}")
        self._running = False
        with contextlib.suppress(Exception):
            await self.websocket_server.stop()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        with contextlib.suppress(Exception):
            await self.llm_generator.close()
        await self.task_manager.stop_all()
        # Persist optimizer state before closing the DB.
        save_state = getattr(self.autonomous_optimizer, "save_state", None)
        if save_state is not None:
            with contextlib.suppress(Exception):
                result = save_state()
                if inspect.isawaitable(result):
                    await result
        with contextlib.suppress(Exception):
            await self.db_manager.close()
        with contextlib.suppress(Exception):
            await self.leader.stop()
        logger.info("fallback_manager_shutdown_complete")


# =============================================================================
# Component factory
# =============================================================================
def build_components(config: Optional[FallbackManagerConfig] = None) -> Dict[str, Any]:
    cfg = config or FallbackManagerConfig()

    db_manager = EnhancedDatabaseManager(cfg)
    vault = VaultManager(cfg)
    quantum = QuantumResilientFallbackSecurity(cfg, vault)
    blockchain = BlockchainFallbackVerification(cfg, db_manager)
    carbon = CarbonIntensityManager(cfg)
    llm = LLMFallbackGenerator(cfg)
    load_shedder = LoadShedder(cfg)
    region = MultiRegionFallbackCoordinator(cfg)
    optimizer = AutonomousFallbackOptimizer(cfg, db_manager=db_manager)
    federated = FederatedFallbackLearner(cfg, db_manager, cfg.general.instance_id)
    predictive = PredictiveFallbackReflexivity(cfg)
    sustainability = FallbackSustainabilityTracker(cfg, db_manager)
    websocket = WebSocketServer(cfg, None)
    cloud = MultiCloudStorage(cfg)
    leader = LeaderElection(cfg)
    task_manager = TaskManager()
    chaos = ChaosMonkey(
        enabled=cfg.chaos.enabled,
        failure_probability=cfg.chaos.failure_probability,
        seed=cfg.chaos.seed,
    )
    manager = EnhancedFallbackManagerV15_0(
        config=cfg,
        db_manager=db_manager,
        quantum_security=quantum,
        blockchain=blockchain,
        carbon_manager=carbon,
        llm_generator=llm,
        load_shedder=load_shedder,
        region_coordinator=region,
        autonomous_optimizer=optimizer,
        federated_learner=federated,
        predictive_reflexivity=predictive,
        sustainability_tracker=sustainability,
        websocket_server=websocket,
        cloud_storage=cloud,
        vault=vault,
        leader=leader,
        task_manager=task_manager,
        chaos_monkey=chaos,
    )
    websocket.manager = manager
    return {"manager": manager, "config": cfg}


# =============================================================================
# FastAPI app factory
# =============================================================================
def create_app(config: Optional[FallbackManagerConfig] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config or FallbackManagerConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        components = build_components(cfg)
        app.state.manager = components["manager"]
        await app.state.manager.start()
        try:
            yield
        finally:
            await app.state.manager.shutdown()

    app = FastAPI(
        title="Fallback Manager API",
        version="15.2",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer(auto_error=False)
    api_rate_limiter = RateLimiter(cfg.api.rate_limit_requests, cfg.api.rate_limit_window)

    async def verify_token(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    ) -> Dict[str, Any]:
        if credentials is None or not JWT_AVAILABLE:
            raise HTTPException(status_code=401, detail="auth required")
        try:
            return pyjwt.decode(
                credentials.credentials, cfg.api.jwt_secret, algorithms=["HS256"]
            )
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid token: {e}")

    async def require_admin(user: Dict[str, Any] = Depends(verify_token)) -> Dict[str, Any]:
        if cfg.api.require_auth_for_admin and user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="admin required")
        return user

    async def rate_limit() -> None:
        if cfg.api.rate_limit_enabled and not await api_rate_limiter.acquire():
            raise HTTPException(status_code=429, detail="rate limit exceeded")

    @app.get("/metrics")
    async def metrics():
        if not PROMETHEUS_AVAILABLE:
            return Response(content=b"# prometheus not installed", media_type="text/plain")
        return Response(content=METRICS.render(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/fallback")
    async def trigger_fallback(
        request: Request,
        handler_name: str,
        body: Optional[Dict[str, Any]] = None,
        user: Dict[str, Any] = Depends(verify_token),
        _: None = Depends(rate_limit),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        try:
            result = await manager.execute_with_fallback(handler_name, body or {})
            return {"result": result}
        except FallbackManagerError as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/status")
    async def get_status(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
        _: None = Depends(rate_limit),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return await manager.get_system_status()

    @app.get("/health")
    async def health(request: Request):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return await manager.health_check()

    @app.get("/optimization/status")
    async def optimization_status(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return await manager.autonomous_optimizer.get_optimization_status()

    @app.post("/optimization/rlhf-update")
    async def rlhf_update(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        rlhf = getattr(manager.autonomous_optimizer, "rlhf", None)
        if rlhf is None:
            return {"status": "RLHF not available"}
        rlhf.update(
            body.get("context", {}), body.get("action", "unknown"),
            float(body.get("reward", 0.0)),
        )
        return {"status": "RLHF updated"}

    @app.get("/human-review/pending")
    async def human_review_pending(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return await manager.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(
        review_id: str, request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        ok = await manager.human_review.approve(
            review_id, reviewer=user.get("sub", "admin")
        )
        return {"status": "approved" if ok else "not_found"}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(
        review_id: str, request: Request,
        reason: Optional[str] = None,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        ok = await manager.human_review.reject(
            review_id, reason=reason, reviewer=user.get("sub", "admin")
        )
        return {"status": "rejected" if ok else "not_found"}

    @app.post("/chaos/trigger")
    async def chaos_trigger(
        request: Request,
        enabled: bool = True, probability: float = 0.1,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        manager.chaos_monkey.enabled = bool(enabled)
        manager.chaos_monkey.failure_probability = float(probability)
        return manager.chaos_monkey.get_stats()

    @app.post("/chaos/inject")
    async def chaos_inject(
        request: Request,
        fault: str, steps: int = 5,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        if fault not in ChaosMonkey.FAULT_TYPES:
            raise HTTPException(status_code=400, detail="unknown fault")
        return await manager.chaos_monkey.inject_and_measure(
            manager, fault, steps=steps
        )

    @app.post("/carbon/offset")
    async def carbon_offset(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(verify_token),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return await manager.carbon_broker.purchase_offsets(
            float(body.get("carbon_intensity", 0.0)),
            float(body.get("carbon_kg", 0.0)),
        )

    @app.post("/carbon/rec")
    async def carbon_rec(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(verify_token),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return await manager.carbon_broker.purchase_recs(
            float(body.get("energy_mwh", 0.0))
        )

    @app.get("/carbon/totals")
    async def carbon_totals(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return manager.carbon_broker.get_totals()

    @app.post("/precision/recommend")
    async def precision_recommend(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(verify_token),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return {
            "precision": manager.precision_policy.recommend_precision(
                workload_size=body.get("workload_size", "medium"),
                carbon_intensity=body.get("carbon_intensity"),
            )
        }

    @app.get("/safety/violations")
    async def safety_violations(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        if manager.safety_monitor:
            return {"violations": manager.safety_monitor.get_violations()}
        return {"violations": [], "monitor_enabled": False}

    @app.get("/xai/last-decision")
    async def xai_last_decision(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        return {
            "region": getattr(manager.region_coordinator, "last_explanation", ""),
            "optimizer": getattr(manager.autonomous_optimizer, "last_explanation", ""),
        }

    @app.post("/federated/register")
    async def federated_register(
        request: Request,
        body: Dict[str, Any],
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        learner = manager.federated_learner
        if hasattr(learner, "secure_coordinator"):
            learner.secure_coordinator.register_participant(
                body.get("participant_id", "unknown"),
                body.get("update", {}),
                sample_count=int(body.get("sample_count", 1)),
            )
            return {"status": "registered"}
        return {"status": "not_available"}

    @app.get("/federated/aggregate")
    async def federated_aggregate(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        manager = getattr(request.app.state, "manager", None)
        if manager is None:
            raise HTTPException(status_code=503, detail="manager not initialized")
        learner = manager.federated_learner
        if hasattr(learner, "secure_coordinator"):
            return learner.secure_coordinator.aggregate()
        return {}

    return app


# =============================================================================
# Singleton accessor
# =============================================================================
_manager_instance: Optional[EnhancedFallbackManagerV15_0] = None
_manager_lock: Optional[asyncio.Lock] = None


async def get_fallback_manager(
    config: Optional[Union[FallbackManagerConfig, Dict[str, Any]]] = None
) -> EnhancedFallbackManagerV15_0:
    global _manager_instance, _manager_lock
    if _manager_lock is None:
        _manager_lock = asyncio.Lock()
    if _manager_instance is None:
        async with _manager_lock:
            if _manager_instance is None:
                if config is None:
                    cfg = FallbackManagerConfig()
                elif isinstance(config, FallbackManagerConfig):
                    cfg = config
                else:
                    cfg = FallbackManagerConfig(**config)
                components = build_components(cfg)
                _manager_instance = components["manager"]
                await _manager_instance.start()
    return _manager_instance


# =============================================================================
# Main entry point
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Enhanced Fallback Manager v15.2 — Single-File Enhanced")
    print("=" * 80)

    manager = await get_fallback_manager()
    print("\nEnhancements:")
    print(f"  Causal RL: {ENHANCEMENTS_AVAILABLE}")
    print(f"  Additional enhancements: {ADDITIONAL_ENHANCEMENTS_AVAILABLE}")

    qstatus = manager.quantum_security.get_quantum_status()
    print(f"  Quantum: {qstatus.get('algorithm_in_use')} "
          f"(pqc={qstatus.get('pqc_available')})")

    rstatus = await manager.region_coordinator.get_region_status()
    print(f"  Active region: {rstatus.get('active_region')}")

    opt = await manager.autonomous_optimizer.get_optimization_status()
    print(f"  Optimizer: causal_bandit={opt.get('causal_bandit', False)}, "
          f"limit_graph={opt.get('limit_graph_active', False)}")

    async def test_handler(context: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "success", "data": "test"}

    manager.register_fallback_handler("test_service", [test_handler])

    print("\nRunning a test fallback...")
    try:
        result = await manager.execute_with_fallback(
            "test_service", {"carbon_intensity": 400.0, "avg_latency": 150.0}
        )
        print(f"  Result: {result}")
    except Exception as e:
        print(f"  Failed: {e}")

    status = await manager.get_system_status()
    print(f"\nStatus: instance={status['instance_id']} "
          f"version={status['version']} running={status['running']}")
    print(f"  Recent success rate: {status['fallback_history']['recent_success_rate']:.2f}")
    print(f"  Carbon broker: {status['advanced_enhancements']['carbon_broker']}")

    await manager.shutdown()
    print("\nShutdown complete")


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()


__all__ = [
    "FallbackManagerConfig",
    "GeneralConfig",
    "QuantumConfig",
    "CloudConfig",
    "LLMConfig",
    "SchedulerConfig",
    "PredictiveConfig",
    "FederatedConfig",
    "DatabaseConfig",
    "VaultConfig",
    "APIConfig",
    "CircuitBreakerConfig",
    "LeaderConfig",
    "CarbonConfig",
    "WebSocketConfig",
    "SafetyConfig",
    "ChaosConfig",
    "MultiAgentConfig",
    "OptimizerConfig",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedSecureCoordinator",
    "MultiAgentCoordinator",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "FlexGenPrecisionPolicy",
    "QuantumDistillationOptimizer",
    "VaultManager",
    "EnhancedDatabaseManager",
    "BlockchainFallbackVerification",
    "QuantumResilientFallbackSecurity",
    "LLMFallbackGenerator",
    "LoadShedder",
    "MultiRegionFallbackCoordinator",
    "AutonomousFallbackOptimizer",
    "FederatedFallbackLearner",
    "PredictiveFallbackReflexivity",
    "FallbackSustainabilityTracker",
    "WebSocketServer",
    "MultiCloudStorage",
    "LeaderElection",
    "EnhancedFallbackManagerV15_0",
    "RateLimiter",
    "TaskManager",
    "GlobalCircuitBreaker",
    "CircuitBreaker",
    "CircuitBreakerState",
    "IQuantumSecurity",
    "IBlockchain",
    "ICarbonManager",
    "ILLMGenerator",
    "ILoadShedder",
    "IRegionCoordinator",
    "IAutonomousOptimizer",
    "IFederatedLearner",
    "IPredictiveReflexivity",
    "ISustainabilityTracker",
    "IWebSocketServer",
    "ICloudStorage",
    "IDatabaseManager",
    "IVault",
    "FallbackManagerError",
    "QuantumError",
    "BlockchainError",
    "CircuitBreakerOpenError",
    "LoadSheddingError",
    "RateLimitExceeded",
    "VaultError",
    "CloudStorageError",
    "FederatedError",
    "PredictiveError",
    "OptimizerError",
    "DatabaseError",
    "LLMError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "NotImplementedYetError",
    "build_components",
    "create_app",
    "get_fallback_manager",
    "main",
]
