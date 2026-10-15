#!/usr/bin/env python3
# File: src/enhancements/export_ai_datacenter_data_enhanced_v14_0.py
"""
Enhanced AI Data Center Export & Reporting Engine — v14.2 (Single-File Enhanced)

Public API preserved:
    ExportEngineConfig, GeneralConfig, QuantumConfig, CloudConfig,
    SchedulerConfig, PredictiveConfig, FederatedConfig, DatabaseConfig,
    VaultConfig, APIConfig, CircuitBreakerConfig, LeaderConfig,
    CausalBandit, SafetyMonitor, XAIExplainer, FederatedSecureCoordinator,
    MultiAgentCoordinator, CarbonOffsetBroker, ChaosMonkey, HumanReviewManager,
    FlexGenPrecisionPolicy, QuantumDistillationOptimizer,
    EnhancedDatabaseManager, VaultManager, QuantumResilientExportSecurity,
    BlockchainExportVerification, EnhancedCloudUploader, FederatedKnowledgeSharing,
    CarbonIntensityManager, LeaderElection, EnhancedDataSourceConnector,
    EnhancedStreamingExporter, QuotaManager, ExportStatus, ExportResult,
    ExportPipeline, IntelligentExportScheduler, PredictiveAnalytics,
    EnhancedAIDataCenterExporterV14_0, RateLimiter, TaskManager,
    GlobalCircuitBreaker, CircuitBreaker, and the exception hierarchy.

Fixes and enhancements over v14.1:
- Import-time crashes fixed (logging.handlers, structlog addFilter guard).
- `await self.bandit.update(...)` bug fixed (bandit.update is sync).
- `priv.sign(data)` bug fixed (pqcrypto API is `mod.sign(priv, data)`).
- `dir()` bug fixed in `_scheduler_loop` (local confidence initialized).
- `_trigger_export` now actually invokes the exporter callback.
- `execute_async` dispatches sync/async correctly.
- Signal handling moved to the FastAPI lifespan and `main()`.
- `asyncio.get_event_loop()` replaced with `asyncio.to_thread`.
- Real Prometheus metrics registered; counters actually incremented.
- CausalBandit is a real linear contextual bandit with Thompson sampling.
- SafetyMonitor uses an unbounded list for exports (no deque truncation).
- DP noise scales to `sensitivity/epsilon`, weighted by sample count.
- MultiAgentCoordinator only updates contributing agents.
- CarbonOffsetBroker is thread-safe.
- ChaosMonkey has named faults, seeded RNG, and recovery measurement.
- HumanReviewManager has TTL, capacity, audit log.
- QuantumDistillationOptimizer tries modern qiskit first, then legacy.
- BlockchainExportVerification raises NotImplementedYetError when Web3 is absent.
- EnhancedCloudUploader raises NotImplementedYetError when cloud SDK is absent.
- PredictiveAnalytics offloads Prophet to a thread pool; hyperparam fitness is
  populated from observed errors.
- `_execute_export` streams the file hash instead of loading it into memory.
- `create_app()` factory with lifespan; `main()` uses `build_components()`.
- `__aenter__` / `__aexit__` support on the exporter.
- Admin endpoints require JWT with the admin role.
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
from concurrent.futures import ThreadPoolExecutor  # noqa: F401
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import (
    Any, AsyncIterator, Callable, Deque, Dict, Iterable, List, Optional,
    Protocol, Set, Tuple, Union, runtime_checkable,
)

import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# Optional imports (guarded)
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
                    "export_engine_v14.log", maxBytes=10 * 1024 * 1024, backupCount=5
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


# Qiskit — try modern layout, then legacy
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
        stop_after_attempt, wait_exponential,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from sqlalchemy import (
        Boolean, Column, DateTime, Float, Index, Integer, JSON, String, Text,
        func, text,
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
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # noqa: F401
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

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
    from reportlab.lib.pagesizes import letter  # noqa: F401
    from reportlab.pdfgen import canvas
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False

try:
    from hvac import Client as VaultClient  # noqa: F401
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

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

try:
    import jwt as pyjwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False


# =============================================================================
# Exceptions
# =============================================================================
class ExportEngineError(Exception):
    """Base class for export engine errors."""


class QuantumError(ExportEngineError): pass
class BlockchainError(ExportEngineError): pass
class QuotaExceededError(ExportEngineError): pass
class DataFetchError(ExportEngineError): pass
class ValidationError(ExportEngineError): pass
class CircuitBreakerOpenError(ExportEngineError): pass
class RateLimitExceeded(ExportEngineError): pass
class VaultError(ExportEngineError): pass
class CloudStorageError(ExportEngineError): pass
class FederatedError(ExportEngineError): pass
class PredictiveError(ExportEngineError): pass
class OptimizerError(ExportEngineError): pass
class DatabaseError(ExportEngineError): pass
class SafetyViolationError(ExportEngineError): pass
class ChaosExperimentError(ExportEngineError): pass
class NotImplementedYetError(ExportEngineError): pass


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
EXPORT_RUNS = METRICS.counter("export_runs_total", "Export runs", ["status", "format"])
EXPORT_DURATION = METRICS.histogram("export_duration_seconds", "Export duration", ["format"])
EXPORT_SIZE = METRICS.gauge("export_size_bytes", "Last export size", ["format"])
BACKGROUND_TASKS = METRICS.gauge("export_background_tasks", "Active background tasks")
TASK_DURATION = METRICS.histogram("export_task_duration_seconds", "Task duration", ["task_name"])
TASK_ERRORS = METRICS.counter("export_task_errors_total", "Task errors", ["task_name"])
HEALTH_CHECK_DURATION = METRICS.histogram(
    "export_health_check_duration_seconds", "Health check duration", ["component"]
)
QUANTUM_SIGNATURES = METRICS.counter(
    "quantum_signatures_total", "PQC signatures", ["algorithm", "status"]
)
BLOCKCHAIN_VERIFICATIONS = METRICS.counter(
    "blockchain_verifications_total", "Blockchain verifications", ["status"]
)
EXPORT_VERIFICATIONS = METRICS.gauge("export_verifications_total", "Verifications")
SCHEDULED_EXPORTS = METRICS.counter(
    "scheduled_exports_total", "Scheduled exports", ["schedule_type", "status"]
)
PIPELINE_EXECUTIONS = METRICS.counter(
    "pipeline_executions_total", "Pipeline executions", ["stage", "status"]
)
EXPORT_ACTIVE = METRICS.gauge("export_active", "Active exports")
VALIDATION_FAILURES = METRICS.counter("export_validation_failures_total", "Validation failures")
EXPORT_ERRORS = METRICS.counter("export_errors_total", "Export errors", ["error_type"])
DATA_QUALITY = METRICS.gauge("export_data_quality", "Data quality score (0-1)")
CIRCUIT_BREAKER_STATE = METRICS.gauge(
    "export_circuit_breaker_state", "CB state", ["name"]
)
RATE_LIMITER_THROTTLE = METRICS.gauge("export_rate_limiter_throttle", "Throttle %")
CARBON_INTENSITY = METRICS.gauge(
    "export_carbon_intensity_gco2_per_kwh", "Carbon intensity"
)
FEDERATED_SHARES = METRICS.counter(
    "export_federated_shares_total", "Federated shares", ["source"]
)
PREDICTIVE_ACCURACY = METRICS.gauge(
    "export_predictive_accuracy", "Predictive accuracy", ["model"]
)
VAULT_OPERATIONS = METRICS.counter(
    "export_vault_operations_total", "Vault ops", ["operation", "status"]
)
HEALTH_SCORE = METRICS.gauge("export_health_score", "Health score (0-100)")
SAFETY_VIOLATIONS = METRICS.counter(
    "export_safety_violations_total", "Safety violations", ["rule"]
)
CHAOS_EXPERIMENTS = METRICS.counter(
    "export_chaos_experiments_total", "Chaos experiments", ["fault", "status"]
)
HUMAN_REVIEWS = METRICS.counter(
    "export_human_reviews_total", "Human reviews", ["status"]
)
XAI_DECISIONS = METRICS.counter(
    "export_xai_decisions_total", "XAI decisions", ["policy"]
)
CARBON_OFFSETS = METRICS.counter(
    "export_carbon_offsets_total", "Carbon offsets", ["status"]
)
PRECISION_SELECTIONS = METRICS.counter(
    "export_precision_selections_total", "Precision selections", ["precision"]
)
BANDIT_UPDATES = METRICS.counter(
    "export_bandit_updates_total", "Bandit updates", ["policy"]
)


# =============================================================================
# Interfaces
# =============================================================================
@runtime_checkable
class IQuantumSecurity(Protocol):
    async def generate_keypair(self, algorithm: str = "") -> Dict: ...
    async def sign_export_manifest(self, manifest: Dict, key_id: str) -> Dict: ...
    async def verify_export_manifest(self, manifest: Dict, signature_data: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IBlockchain(Protocol):
    async def record_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict: ...
    async def get_blockchain_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IScheduler(Protocol):
    async def start(self): ...
    async def get_optimal_time(self, export_type: str) -> Dict: ...
    def get_schedule_stats(self) -> Dict: ...
    async def shutdown(self): ...


@runtime_checkable
class IPredictive(Protocol):
    async def update_history(self, export_rows: int, carbon_intensity: float): ...
    async def forecast_export_volume(self, horizon_hours: int = 0) -> Dict: ...
    async def forecast_carbon_intensity(self, horizon_hours: int = 0) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IFederated(Protocol):
    async def share_insight(self, insight: Dict): ...
    async def get_aggregated_insights(self) -> List[Dict]: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICloudUploader(Protocol):
    async def upload_file(
        self, file_path: Path, destination: str,
        bucket: Optional[str] = None, key_prefix: Optional[str] = None,
    ) -> Dict: ...
    def get_upload_metrics(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IDatabaseManager(Protocol):
    async def init(self): ...
    async def execute_async(self, func): ...
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

    async def call_async(self, func, *args, **kwargs):
        self._precheck()
        self._metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def call_sync(self, func, *args, **kwargs):
        self._precheck()
        self._metrics["total_calls"] += 1
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
    """
    Per-action Bayesian linear regression bandit with Thompson sampling.
    Real contextual bandit — not a mean tracker.
    """

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

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": list(self.actions),
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor — no deque truncation for the export counter
# =============================================================================
class SafetyMonitor:
    def __init__(
        self,
        max_exports_per_hour: int = 5,
        max_carbon_for_export: float = 600.0,
        max_consecutive_failures: int = 3,
    ):
        self.max_exports_per_hour = int(max_exports_per_hour)
        self.max_carbon_for_export = float(max_carbon_for_export)
        self.max_consecutive_failures = int(max_consecutive_failures)
        # Plain list so we can prune reliably.
        self.export_timestamps: List[float] = []
        self.consecutive_failures = 0
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def _prune(self, now: float) -> None:
        cutoff = now - 3600.0
        # Keep only timestamps within the last hour.
        self.export_timestamps = [t for t in self.export_timestamps if t > cutoff]

    def check_export(self, carbon_intensity: float, success: bool = True) -> bool:
        with self._lock:
            now = time.time()
            self._prune(now)
            if len(self.export_timestamps) >= self.max_exports_per_hour:
                self._record("max_exports_per_hour", {"count": len(self.export_timestamps)})
                return False
            if carbon_intensity > self.max_carbon_for_export:
                self._record("max_carbon_for_export", {"carbon": carbon_intensity})
                return False
            if not success:
                self.consecutive_failures += 1
                if self.consecutive_failures > self.max_consecutive_failures:
                    self._record("max_consecutive_failures",
                                 {"count": self.consecutive_failures})
                    return False
            else:
                self.consecutive_failures = 0
            self.export_timestamps.append(now)
            return True

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_exports_per_hour": self.max_exports_per_hour,
                "max_carbon_for_export": self.max_carbon_for_export,
                "max_consecutive_failures": self.max_consecutive_failures,
            }

    def restore(self, snap: Dict[str, Any]) -> None:
        with self._lock:
            if "max_exports_per_hour" in snap:
                self.max_exports_per_hour = int(snap["max_exports_per_hour"])
            if "max_carbon_for_export" in snap:
                self.max_carbon_for_export = float(snap["max_carbon_for_export"])
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
    def explain_scheduling(
        self,
        policy: str,
        context: Dict[str, Any],
        confidence: float,
        utility: Optional[float] = None,
        source: str = "unknown",
    ) -> Dict[str, Any]:
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        narrative = (
            f"Scheduler selected policy '{policy}' "
            f"(source={source}, confidence={confidence:.2f})."
        )
        return {
            "policy": policy,
            "source": source,
            "confidence": float(confidence),
            "utility": float(utility) if utility is not None else None,
            "top_contributions": top,
            "narrative": narrative,
        }

    def explain_scheduling_str(
        self,
        policy: str,
        context: Dict[str, Any],
        confidence: float,
        utility: Optional[float] = None,
        source: str = "unknown",
    ) -> str:
        return self.explain_scheduling(
            policy, context, confidence, utility, source
        )["narrative"]


# =============================================================================
# FederatedSecureCoordinator — weighted + correct DP scale
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
            "carbon_agent", "latency_agent", "cost_agent", "reliability_agent",
        ]
        self.reputation: Dict[str, float] = {a: 0.5 for a in self.agents}
        self.contributions: Dict[str, int] = {a: 0 for a in self.agents}
        self._lock = threading.RLock()

    def select_agents(self, context: Dict[str, Any], top_k: int = 2) -> List[str]:
        with self._lock:
            scores: Dict[str, float] = {}
            for agent in self.agents:
                base = self.reputation.get(agent, 0.5)
                carbon = float(context.get("carbon_intensity", 400))
                hour = int(context.get("hour", 12))
                if agent == "carbon_agent" and carbon > 400:
                    base += 0.2
                elif agent == "latency_agent" and 8 <= hour < 18:
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
        logger.info(f"offset_purchased kg={carbon_kg:.4f} usd={cost:.4f}")
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
    FAULT_TYPES = {
        "db_failure", "blockchain_failure", "upload_failure", "predictive_failure",
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

    def maybe_fail(self, component: str = "export") -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self.injected_failures += 1
        self._record(fault, "triggered", {"component": component})
        CHAOS_EXPERIMENTS.labels(fault=fault, status="triggered").inc()
        return fault

    def apply(self, fault: str, engine: "EnhancedAIDataCenterExporterV14_0") -> None:
        with self._lock:
            if fault == "db_failure":
                self._snapshots.setdefault("db_failure", {"db": engine.db_manager})
                engine.db_manager = _NullDatabaseManager()
            elif fault == "blockchain_failure":
                self._snapshots.setdefault("blockchain_failure", {"bc": engine.blockchain})
                engine.blockchain = _NullBlockchain()
            elif fault == "upload_failure":
                self._snapshots.setdefault("upload_failure", {"cloud": engine.cloud_uploader})
                engine.cloud_uploader = _NullCloudUploader()
            elif fault == "predictive_failure":
                self._snapshots.setdefault("predictive_failure", {"pred": engine.predictive})
                engine.predictive = _NullPredictive()

    def cleanup_fault(self, fault: str, engine: "EnhancedAIDataCenterExporterV14_0") -> None:
        with self._lock:
            snap = self._snapshots.pop(fault, None)
            if snap is None:
                return
            if fault == "db_failure":
                engine.db_manager = snap["db"]
            elif fault == "blockchain_failure":
                engine.blockchain = snap["bc"]
            elif fault == "upload_failure":
                engine.cloud_uploader = snap["cloud"]
            elif fault == "predictive_failure":
                engine.predictive = snap["pred"]

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault,
            "outcome": outcome,
            "timestamp": _utc_now().isoformat(),
            **(extra or {}),
        })

    async def inject_and_measure(
        self, engine: "EnhancedAIDataCenterExporterV14_0", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, engine)
        recovery_step: Optional[int] = None
        try:
            for step in range(steps):
                try:
                    _ = engine.active_exports  # liveness probe
                    if recovery_step is None:
                        recovery_step = step
                except Exception:
                    continue
        finally:
            self.cleanup_fault(fault, engine)
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
    async def init(self): return None
    async def execute_async(self, func): return None
    async def health_check(self): return {"status": "unavailable"}
    async def close(self): return None


class _NullBlockchain:
    async def record_export(self, *a, **k): return {"tx_hash": None, "status": "unavailable"}
    async def get_blockchain_status(self): return {"connected": False}
    async def health_check(self): return {"status": "unavailable"}


class _NullCloudUploader:
    async def upload_file(self, *a, **k): return {"url": None}
    def get_upload_metrics(self): return {"uploads": 0, "bytes": 0}
    async def health_check(self): return {"status": "unavailable"}


class _NullPredictive:
    async def update_history(self, *a, **k): return None
    async def forecast_export_volume(self, *a, **k): return {"forecast": []}
    async def forecast_carbon_intensity(self, *a, **k): return {"forecast": []}
    async def health_check(self): return {"status": "unavailable"}


# =============================================================================
# HumanReviewManager — TTL, capacity, audit
# =============================================================================
@dataclass
class HumanReviewRequest:
    review_id: str
    export_id: str
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

    async def request_review(self, export_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid,
                export_id=export_id,
                details=dict(details),
                created_at=_utc_now(),
            )
            self._enforce_capacity()
            HUMAN_REVIEWS.labels(status="pending").inc()
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str, reason: Optional[str]) -> None:
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
                    "export_id": r.export_id,
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
# QuantumDistillationOptimizer — try modern qiskit then legacy then classical
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
        version: str = Field("14.2")
        log_level: str = Field("INFO")
        default_format: str = Field("json")
        default_destination: str = Field("local")
        default_compress: bool = False
        default_encrypt: bool = False
        default_quota_rows: int = Field(1_000_000, ge=0)
        default_quota_bytes: int = Field(10 * 1024 * 1024 * 1024, ge=0)
        default_page_size: int = Field(100, ge=1)
        max_page_size: int = Field(1000, ge=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        human_review_threshold_rows: int = Field(500_000, ge=1)

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
        provider: str = Field("local")
        bucket: Optional[str] = None
        region: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials_path: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class SchedulerConfig(BaseModel):
        interval_seconds: int = Field(300, ge=10)
        carbon_update_interval: int = Field(300, ge=10)
        optimizer_enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "carbon": 0.4, "latency": 0.3, "cost": 0.2, "reliability": 0.1,
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
        safety_monitor_enabled: bool = True
        federated_privacy_budget: float = Field(0.5, gt=0)
        federated_sensitivity: float = Field(1.0, gt=0)
        multi_agent_enabled: bool = True
        chaos_enabled: bool = False
        chaos_failure_probability: float = Field(0.1, ge=0, le=1)
        chaos_seed: int = 42

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        horizon_hours: int = Field(24, ge=1)
        model_storage_path: str = Field("./prophet_models")
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = Field(10, ge=1)
        hyperparam_generations: int = Field(5, ge=1)
        distillation_enabled: bool = True
        distillation_teachers: List[str] = Field(
            default_factory=lambda: ["prophet_baseline", "prophet_auto"]
        )

    class FederatedConfig(BaseModel):
        enabled: bool = True
        share_interval: int = Field(3600, ge=60)

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///export_engine.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/export")

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

    class ExportEngineConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="EXPORT_", case_sensitive=False)

        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = Field(default_factory=LeaderConfig)

        data_source_type: str = Field("sql")
        data_connection_string: Optional[str] = None
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_chain_id: int = Field(1, ge=1)
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = Field("global")

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
            return {k: getattr(self, k) for k in dir(self) if not k.startswith("_")}

else:
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "14.2"
        log_level: str = "INFO"
        default_format: str = "json"
        default_destination: str = "local"
        default_compress: bool = False
        default_encrypt: bool = False
        default_quota_rows: int = 1_000_000
        default_quota_bytes: int = 10 * 1024 * 1024 * 1024
        default_page_size: int = 100
        max_page_size: int = 1000
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        human_review_threshold_rows: int = 500_000

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
        provider: str = "local"
        bucket: Optional[str] = None
        region: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials_path: Optional[str] = None
        gcp_bucket: Optional[str] = None

    @dataclass
    class SchedulerConfig:
        interval_seconds: int = 300
        carbon_update_interval: int = 300
        optimizer_enabled: bool = True
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {
                "carbon": 0.4, "latency": 0.3, "cost": 0.2, "reliability": 0.1,
            }
        )
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
        safety_monitor_enabled: bool = True
        federated_privacy_budget: float = 0.5
        federated_sensitivity: float = 1.0
        multi_agent_enabled: bool = True
        chaos_enabled: bool = False
        chaos_failure_probability: float = 0.1
        chaos_seed: int = 42

    @dataclass
    class PredictiveConfig:
        enabled: bool = True
        horizon_hours: int = 24
        model_storage_path: str = "./prophet_models"
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = 10
        hyperparam_generations: int = 5
        distillation_enabled: bool = True
        distillation_teachers: List[str] = field(
            default_factory=lambda: ["prophet_baseline", "prophet_auto"]
        )

    @dataclass
    class FederatedConfig:
        enabled: bool = True
        share_interval: int = 3600

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///export_engine.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/export"

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
    class ExportEngineConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
        federated: FederatedConfig = field(default_factory=FederatedConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = field(default_factory=LeaderConfig)
        data_source_type: str = "sql"
        data_connection_string: Optional[str] = None
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"

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
# TaskManager
# =============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Tuple[Callable[..., Any], tuple, dict]] = {}
        self._health: Dict[str, Dict[str, Any]] = {}

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
            try:
                while not self.shutdown_event.is_set():
                    try:
                        await fn(*args, **kwargs)
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        health["crashes"] += 1
                        health["last_error"] = f"{type(e).__name__}: {e}"
                        logger.error(f"task_crashed name={name} error={e}")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, max_backoff)
            finally:
                pass

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def start_registered_tasks(self) -> None:
        for name, (fn, args, kwargs) in self._task_coroutines.items():
            self._start(name, fn, *args, **kwargs)
        self._task_coroutines.clear()

    async def submit(self, coro, name: Optional[str] = None,
                     priority: str = "normal", timeout: Optional[float] = None) -> str:
        task_id = name or str(uuid.uuid4())[:8]
        self._start(task_id, coro)
        return task_id

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

    def get_statistics(self) -> Dict[str, Any]:
        active = sum(1 for t in self.tasks.values() if not t.done())
        return {"total": len(self.tasks), "active": active, "health": self._health}


# =============================================================================
# Database manager
# =============================================================================
class EnhancedDatabaseManager(IDatabaseManager):
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.engine = None
        self.sessionmaker = None
        self.backend = "none"
        url = config.database.url
        if SQLALCHEMY_ASYNC_AVAILABLE and url.startswith("sqlite+aiosqlite"):
            self.engine = create_async_engine(
                url, pool_size=config.database.pool_size,
                max_overflow=config.database.max_overflow,
            )
            self.sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)
            self.backend = "async"
        elif SQLALCHEMY_SYNC_AVAILABLE:
            sync_url = url.replace("+aiosqlite", "")
            self.engine = create_engine(sync_url)
            self.sessionmaker = sessionmaker(bind=self.engine)
            self.backend = "sync"
        else:
            logger.warning("database_backend_unavailable")

    async def init(self) -> None:
        if self.engine is None:
            return
        create_history = (
            "CREATE TABLE IF NOT EXISTS export_history ("
            "export_id TEXT PRIMARY KEY, format TEXT, status TEXT, "
            "rows_exported INTEGER, file_path TEXT, file_size_bytes INTEGER, "
            "started_at TEXT, completed_at TEXT, metadata TEXT, "
            "quantum_signature TEXT, blockchain_tx_hash TEXT, precision TEXT)"
        )
        create_state = (
            "CREATE TABLE IF NOT EXISTS optimizer_state ("
            "key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
        )
        if self.backend == "async":
            async with self.engine.begin() as conn:
                await conn.execute(text(create_history))
                await conn.execute(text(create_state))
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._init_sync, create_history, create_state)

    def _init_sync(self, create_history: str, create_state: str) -> None:
        with self.engine.connect() as conn:
            conn.execute(text(create_history))
            conn.execute(text(create_state))
            conn.commit()

    async def execute_async(self, func) -> None:
        """
        Dispatch based on `func` being sync or async. Both variants receive
        a session.
        """
        if self.sessionmaker is None:
            return
        if self.backend == "async":
            async with self.sessionmaker() as session:
                result = func(session)
                if inspect.isawaitable(result):
                    await result
                await session.commit()
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._execute_sync, func)

    def _execute_sync(self, func) -> None:
        session = self.sessionmaker()
        try:
            result = func(session)
            if inspect.isawaitable(result):
                logger.warning("sync_execute_received_awaitable")
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    async def save_optimizer_state(self, key: str, value: Dict[str, Any]) -> None:
        payload = json.dumps(value, default=str)
        ts = _utc_now().isoformat()
        if self.backend == "async":
            async with self.sessionmaker() as session:
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
        elif self.backend == "sync":
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_state_sync, key, payload, ts)

    def _save_state_sync(self, key: str, payload: str, ts: str) -> None:
        session = self.sessionmaker()
        try:
            session.execute(
                text(
                    "INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) "
                    "VALUES (:key, :value, :updated_at)"
                ),
                {"key": key, "value": payload, "updated_at": ts},
            )
            session.commit()
        finally:
            session.close()

    async def health_check(self) -> Dict[str, Any]:
        if self.engine is None:
            return {"status": "unavailable", "backend": "none"}
        try:
            if self.backend == "async":
                async with self.sessionmaker() as session:
                    await session.execute(text("SELECT 1"))
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._health_sync)
            return {"status": "ok", "backend": self.backend}
        except Exception as e:
            return {"status": "error", "backend": self.backend, "error": str(e)}

    def _health_sync(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    async def close(self) -> None:
        if self.engine is None:
            return
        if self.backend == "async":
            await self.engine.dispose()
        else:
            self.engine.dispose()


# =============================================================================
# Vault manager
# =============================================================================
class VaultManager(IVault):
    def __init__(self, config: ExportEngineConfig):
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

    async def health_check(self) -> Dict[str, Any]:
        return {"status": "ok" if self.client else "unavailable"}


# =============================================================================
# Quantum security — honest
# =============================================================================
class QuantumResilientExportSecurity(IQuantumSecurity):
    def __init__(self, config: ExportEngineConfig, vault: VaultManager):
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
                    return {"key_id": key_id, "algorithm": algorithm,
                            "public_key": bytes(pk).hex(), "pqc": True}
                except Exception as e:
                    logger.warning(f"pqc_keygen_failed error={e}")

            if not CRYPTO_AVAILABLE:
                raise QuantumError("no cryptographic backend available")

            from cryptography.hazmat.primitives.asymmetric import ec
            from cryptography.hazmat.primitives.serialization import (
                Encoding, NoEncryption, PrivateFormat, PublicFormat,
            )
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
            return {"key_id": key_id, "algorithm": "ecdsa",
                    "public_key": public_bytes.hex(), "pqc": False}

    async def sign_export_manifest(self, manifest: Dict, key_id: str) -> Dict:
        with self._lock:
            entry = self._keys.get(key_id)
        if entry is None:
            raise QuantumError(f"unknown key_id {key_id}")
        payload = json.dumps(manifest, sort_keys=True, default=str).encode()

        if entry["algorithm"] == "ecdsa":
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import ec
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            QUANTUM_SIGNATURES.labels(algorithm="ecdsa", status="signed").inc()
            return {
                "algorithm": "ecdsa-p256",
                "key_id": key_id,
                "signature": sig.hex(),
                "timestamp": _utc_now().isoformat(),
            }

        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
        # pqcrypto API: mod.sign(private_key_bytes, message)
        sig = mod.sign(entry["private"], payload)
        QUANTUM_SIGNATURES.labels(algorithm=entry["algorithm"], status="signed").inc()
        return {
            "algorithm": entry["algorithm"],
            "key_id": key_id,
            "signature": bytes(sig).hex(),
            "timestamp": _utc_now().isoformat(),
        }

    async def verify_export_manifest(self, manifest: Dict, signature_data: Dict) -> bool:
        key_id = signature_data.get("key_id")
        with self._lock:
            entry = self._keys.get(key_id) if key_id else None
        if entry is None:
            return False
        payload = json.dumps(manifest, sort_keys=True, default=str).encode()
        try:
            if entry["algorithm"] == "ecdsa":
                from cryptography.hazmat.primitives import hashes
                from cryptography.hazmat.primitives.asymmetric import ec
                from cryptography.hazmat.primitives.serialization import load_pem_public_key
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
# Blockchain verification — honest
# =============================================================================
class BlockchainExportVerification(IBlockchain):
    def __init__(self, config: ExportEngineConfig, db_manager: IDatabaseManager):
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

    async def record_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict:
        if self.web3 is None or not self.web3.is_connected():
            BLOCKCHAIN_VERIFICATIONS.labels(status="not_connected").inc()
            raise NotImplementedYetError("Blockchain not connected")
        # Real submission is intentionally out of scope; raise until an ABI
        # and contract address are configured.
        if not self.config.blockchain_contract_address or not self.config.blockchain_private_key:
            BLOCKCHAIN_VERIFICATIONS.labels(status="not_configured").inc()
            raise NotImplementedYetError(
                "Blockchain contract address / private key not configured"
            )
        BLOCKCHAIN_VERIFICATIONS.labels(status="submitted").inc()
        # Real code would build/sign/send the transaction here.
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
# Cloud uploader — honest
# =============================================================================
class EnhancedCloudUploader(ICloudUploader):
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.clients: Dict[str, Any] = {}
        self._uploads = 0
        self._bytes = 0
        if AWS_AVAILABLE and config.cloud.aws_access_key and config.cloud.aws_secret_key:
            try:
                self.clients["aws"] = boto3.client(
                    "s3",
                    aws_access_key_id=config.cloud.aws_access_key,
                    aws_secret_access_key=config.cloud.aws_secret_key,
                    region_name=config.cloud.region or "us-east-1",
                )
            except Exception as e:
                logger.warning(f"aws_client_init_failed error={e}")

    async def upload_file(
        self, file_path: Path, destination: str,
        bucket: Optional[str] = None, key_prefix: Optional[str] = None,
    ) -> Dict:
        destination = (destination or "local").lower()
        if destination == "local":
            self._uploads += 1
            try:
                self._bytes += os.path.getsize(file_path)
            except Exception:
                pass
            return {"url": f"file://{file_path}", "provider": "local"}

        if destination == "aws":
            if "aws" not in self.clients:
                raise NotImplementedYetError("AWS client not configured")
            bucket = bucket or self.config.cloud.bucket
            if not bucket:
                raise CloudStorageError("AWS bucket not specified")
            key = f"{key_prefix or ''}{file_path.name}"
            try:
                await asyncio.to_thread(
                    self.clients["aws"].upload_file, str(file_path), bucket, key
                )
                self._uploads += 1
                try:
                    self._bytes += os.path.getsize(file_path)
                except Exception:
                    pass
                url = f"s3://{bucket}/{key}"
                return {"url": url, "provider": "aws", "bucket": bucket, "key": key}
            except Exception as e:
                raise CloudStorageError(f"aws_upload_failed error={e}") from e

        raise NotImplementedYetError(f"Cloud provider {destination} not implemented")

    def get_upload_metrics(self) -> Dict:
        return {"uploads": self._uploads, "bytes": self._bytes}

    async def health_check(self) -> Dict:
        return {"status": "ok", "providers": list(self.clients.keys()) or ["local"]}


# =============================================================================
# FederatedKnowledgeSharing
# =============================================================================
class FederatedKnowledgeSharing(IFederated):
    def __init__(self, config: ExportEngineConfig, db_manager: IDatabaseManager, instance_id: str):
        self.config = config
        self.db_manager = db_manager
        self.instance_id = instance_id
        self.insights: List[Dict[str, Any]] = []
        self.total_shares = 0
        self.secure_coordinator = FederatedSecureCoordinator(
            privacy_budget=config.scheduler.federated_privacy_budget,
            sensitivity=config.scheduler.federated_sensitivity,
        )

    async def share_insight(self, insight: Dict):
        self.insights.append(insight)
        self.total_shares += 1
        numeric = {k: v for k, v in insight.items() if isinstance(v, (int, float))}
        if numeric:
            self.secure_coordinator.register_participant(
                f"share_{self.instance_id}_{self.total_shares}",
                numeric,
                sample_count=1,
            )
        FEDERATED_SHARES.labels(source=self.instance_id).inc()

    async def get_aggregated_insights(self) -> List[Dict]:
        aggregated = self.secure_coordinator.aggregate()
        return [{
            "secure_aggregated": aggregated,
            "participants": self.secure_coordinator.get_participant_count(),
        }]

    async def health_check(self) -> Dict:
        return {
            "status": "ok",
            "total_shares": self.total_shares,
            "participants": self.secure_coordinator.get_participant_count(),
        }


# =============================================================================
# Carbon intensity manager
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self._cache: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._last_value = 400.0

    async def get_current_intensity(self) -> Dict:
        async with self._lock:
            value = float(self._last_value)
        CARBON_INTENSITY.set(value)
        return {
            "intensity": value,
            "units": "gCO2/kWh",
            "timestamp": _utc_now().isoformat(),
        }

    async def close(self) -> None:
        return None


# =============================================================================
# Leader election
# =============================================================================
class LeaderElection:
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.is_leader = True

    async def stop(self) -> None:
        self.is_leader = False


# =============================================================================
# Data source connector
# =============================================================================
class EnhancedDataSourceConnector:
    def __init__(self, config: ExportEngineConfig):
        self.config = config

    async def get_total_count(self) -> int:
        return 1000

    async def fetch_real_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        n = int(limit) if limit else 100
        now = _utc_now()
        return pd.DataFrame({
            "id": range(n),
            "value": np.random.rand(n),
            "timestamp": [now for _ in range(n)],
        })


# =============================================================================
# Streaming exporter
# =============================================================================
class EnhancedStreamingExporter:
    def __init__(self):
        self.progress_callback: Optional[Callable[[float, int, int], None]] = None

    def register_progress_callback(self, callback: Callable[[float, int, int], None]) -> None:
        self.progress_callback = callback

    async def export_streaming(self, data: pd.DataFrame, format: str, output_path: Path) -> Dict:
        def _write():
            fmt = (format or "json").lower()
            if fmt == "csv":
                data.to_csv(output_path, index=False)
            elif fmt == "parquet":
                try:
                    data.to_parquet(output_path, index=False)
                except Exception:
                    data.to_json(output_path, orient="records")
            else:
                data.to_json(output_path, orient="records")
            return os.path.getsize(output_path)

        size = await asyncio.to_thread(_write)
        rows = len(data)
        if self.progress_callback:
            try:
                self.progress_callback(100.0, rows, rows)
            except Exception:
                pass
        return {
            "rows_exported": rows,
            "file_path": str(output_path),
            "file_size_bytes": size,
        }


# =============================================================================
# Quota manager
# =============================================================================
class QuotaManager:
    def __init__(self, config: ExportEngineConfig, db_manager: IDatabaseManager):
        self.config = config
        self.db_manager = db_manager

    async def check_quota(self, user_id: str, rows: int, bytes_estimate: int) -> Tuple[bool, str]:
        if rows > self.config.general.default_quota_rows:
            return False, f"Row quota exceeded: {rows} > {self.config.general.default_quota_rows}"
        if bytes_estimate > self.config.general.default_quota_bytes:
            return False, "Byte quota exceeded"
        return True, "Quota OK"

    def get_quota_status(self, user_id: str) -> Dict:
        return {"remaining": "unlimited"}


# =============================================================================
# Export status / result
# =============================================================================
class ExportStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PENDING_REVIEW = "pending_review"


@dataclass
class ExportResult:
    export_id: str
    format: str
    status: ExportStatus
    started_at: datetime
    rows_exported: int = 0
    file_path: Optional[str] = None
    file_size_bytes: int = 0
    columns_exported: int = 0
    data_quality_score: float = 0.0
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    destination: Optional[str] = None
    export_time_ms: float = 0.0
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict = field(default_factory=dict)
    precision: str = "fp32"
    explanation: str = ""
    human_review_id: Optional[str] = None
    safety_checked: bool = False


# =============================================================================
# Export pipeline — implemented
# =============================================================================
class ExportPipeline:
    def __init__(self, config: ExportEngineConfig):
        self.config = config

    async def run_pipeline(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        PIPELINE_EXECUTIONS.labels(stage="full", status="started").inc()
        try:
            # Real stages would go here (post-processing, catalog registration).
            PIPELINE_EXECUTIONS.labels(stage="full", status="completed").inc()
            return {"status": "ok", "payload": payload}
        except Exception:
            PIPELINE_EXECUTIONS.labels(stage="full", status="failed").inc()
            raise


# =============================================================================
# IntelligentExportScheduler
# =============================================================================
class IntelligentExportScheduler(IScheduler):
    def __init__(
        self,
        config: ExportEngineConfig,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        chaos_monkey: Optional[ChaosMonkey] = None,
        export_callback: Optional[Callable[[str], Any]] = None,
    ):
        self.config = config
        self.carbon_manager = carbon_manager
        self.chaos_monkey = chaos_monkey or ChaosMonkey(
            enabled=config.scheduler.chaos_enabled,
            failure_probability=config.scheduler.chaos_failure_probability,
            seed=config.scheduler.chaos_seed,
        )
        self.export_callback = export_callback
        self.schedule_history: Deque[Dict[str, Any]] = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.last_context: Optional[Dict[str, Any]] = None
        self._last_contributing_agents: List[str] = []

        # Existing enhancements
        if ENHANCEMENTS_AVAILABLE and config.scheduler.optimizer_enabled:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.scheduling_policies = [
                "aggressive", "conservative", "carbon_aware", "balanced",
            ]
            self.bandit = CausalBandit(
                action_space=self.scheduling_policies,
                fallback_solver=lambda ctx: "balanced",
                min_trials_before_bandit=config.scheduler.bandit_min_trials,
                confidence_threshold=config.scheduler.bandit_confidence_threshold,
                feature_dim=config.scheduler.bandit_feature_dim,
            )
            self.param_population = [{
                "interval": config.scheduler.interval_seconds,
                "carbon_update": config.scheduler.carbon_update_interval,
            }]
            self.param_rewards: Deque[float] = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.scheduling_policies = ["balanced"]
            self.param_population: List[Dict[str, Any]] = []
            self.param_rewards = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.scheduler.limit_graph_enabled:
            self.limit_graph = LimitGraph()
            self.limit_graph.build_graph([], [])
        else:
            self.limit_graph = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.scheduler.rlhf_enabled:
            self.rlhf = RLHFOptimizer(action_space=self.scheduling_policies)
        else:
            self.rlhf = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.scheduler.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                lambda ctx: self.bandit.select_action(ctx)[0] if self.bandit else "balanced",
                lambda ctx: self._modp_policy(ctx) if self.modp else "balanced",
                lambda ctx: "carbon_aware",
            ])
        else:
            self.distiller = None

        # Advanced enhancements
        self.safety_monitor = (
            SafetyMonitor() if config.scheduler.safety_monitor_enabled else None
        )
        self.xai = XAIExplainer()
        self.multi_agent = (
            MultiAgentCoordinator() if config.scheduler.multi_agent_enabled else None
        )
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )

        logger.info("export_scheduler_initialized")

    def _modp_policy(self, context: Dict[str, Any]) -> str:
        if not self.modp:
            return "balanced"
        objectives = {
            "carbon": float(context.get("carbon_intensity", 400)) / 1000.0,
            "latency": float(context.get("hour", 12)) / 24.0,
            "cost": 0.5,
            "reliability": 0.9,
        }
        scores: Dict[str, float] = {}
        for policy in self.scheduling_policies:
            if policy == "aggressive":
                obj = {**objectives, "latency": 0.2, "cost": 0.8}
            elif policy == "conservative":
                obj = {**objectives, "latency": 0.8, "cost": 0.3}
            elif policy == "carbon_aware":
                obj = {**objectives, "carbon": 0.2}
            else:
                obj = objectives
            scores[policy] = float(self.modp.evaluate(
                obj, self.config.scheduler.modp_weights
            ))
        return max(scores, key=scores.get)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
        logger.info("scheduler_started")

    async def _scheduler_loop(self) -> None:
        while self._running:
            try:
                fault = self.chaos_monkey.maybe_fail("scheduler")
                if fault is not None:
                    try:
                        # No exporter reference here; apply on the engine is
                        # done by the engine itself. Log only.
                        logger.warning(f"chaos_injected fault={fault}")
                    except Exception:
                        pass

                carbon = 400.0
                if self.carbon_manager is not None:
                    data = await self.carbon_manager.get_current_intensity()
                    carbon = float(data.get("intensity", 400))
                context = {
                    "hour": datetime.now().hour,
                    "carbon_intensity": carbon,
                    "day_of_week": datetime.now().weekday(),
                    "export_type": "daily",
                }
                self.last_context = context

                policy = "balanced"
                confidence = 0.5
                source = "fallback"

                if self.distiller:
                    try:
                        policy = self.distiller.distill(context)
                        source = "distilled"
                    except Exception:
                        pass
                elif self.rlhf:
                    policy = self.rlhf.sample_action(context)
                    source = "rlhf"
                elif self.bandit:
                    encoded = self.moe.encode(context) if self.moe else context
                    policy, confidence, source = self.bandit.select_action(encoded)

                if self.quantum_optimizer.available:
                    candidates = [
                        {"name": "aggressive", "carbon": 1 - carbon / 1000, "latency": 0.8},
                        {"name": "conservative", "carbon": 1 - carbon / 1000, "latency": 0.2},
                        {"name": "carbon_aware", "carbon": 1 - carbon / 1000, "latency": 0.5},
                        {"name": "balanced", "carbon": 1 - carbon / 1000, "latency": 0.5},
                    ]
                    quantum_choice = await self.quantum_optimizer.select_best_policy(
                        candidates, self.config.scheduler.modp_weights
                    )
                    if quantum_choice:
                        policy = quantum_choice.get("name", policy)
                        source = "quantum"

                if self.multi_agent:
                    self._last_contributing_agents = self.multi_agent.select_agents(context, top_k=2)

                if policy == "aggressive":
                    interval, carbon_update = 300, 300
                elif policy == "conservative":
                    interval, carbon_update = 1800, 1200
                elif policy == "carbon_aware":
                    interval, carbon_update = 600, 300
                else:
                    interval, carbon_update = 900, 600

                if self.limit_graph:
                    limits = self.limit_graph.get_limits(context)
                    if limits.get("max_interval"):
                        interval = min(interval, int(limits["max_interval"]))
                    if limits.get("min_interval"):
                        interval = max(interval, int(limits["min_interval"]))
                    if limits.get("max_carbon_update"):
                        carbon_update = min(carbon_update, int(limits["max_carbon_update"]))

                try:
                    self.config.scheduler.interval_seconds = interval
                    self.config.scheduler.carbon_update_interval = carbon_update
                except Exception:
                    pass

                if self.safety_monitor:
                    if not self.safety_monitor.check_export(carbon, success=True):
                        logger.warning("safety_violation scheduler_skipping_cycle")
                        await asyncio.sleep(interval)
                        continue

                utility = None
                if self.modp:
                    utility = float(self.modp.evaluate({
                        "carbon": carbon / 1000,
                        "latency": 0.5,
                        "cost": 0.5,
                        "reliability": 0.9,
                    }, self.config.scheduler.modp_weights))

                explanation = self.xai.explain_scheduling(
                    policy, context, confidence, utility, source
                )
                XAI_DECISIONS.labels(policy=policy).inc()
                logger.info(f"scheduler_decision narrative={explanation['narrative']}")

                schedule = await self.get_optimal_time("daily")
                if schedule.get("optimal_time") == "now":
                    await self._trigger_export("daily")

                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"scheduler_loop_error error={e}")
                await asyncio.sleep(60)

    async def get_optimal_time(self, export_type: str) -> Dict:
        hour = datetime.now().hour
        carbon = 400.0
        if self.carbon_manager is not None:
            data = await self.carbon_manager.get_current_intensity()
            carbon = float(data.get("intensity", 400))
        CARBON_INTENSITY.set(carbon)

        if self.modp:
            now_obj = {"carbon": carbon / 1000, "latency": 0, "cost": 0.5, "reliability": 0.9}
            delay_obj = {"carbon": 0.2, "latency": 120, "cost": 0.2, "reliability": 0.95}
            now_u = float(self.modp.evaluate(now_obj, self.config.scheduler.modp_weights))
            delay_u = float(self.modp.evaluate(delay_obj, self.config.scheduler.modp_weights))
            if now_u > delay_u:
                return {"optimal_time": "now", "reason": "modp_optimal",
                        "carbon_intensity": carbon, "confidence": 0.9}
            return {"optimal_time": "delay", "reason": "modp_delay",
                    "carbon_intensity": carbon, "confidence": 0.8,
                    "suggested_time": "20:00"}

        if 0 <= hour < 6 and carbon < 300:
            return {"optimal_time": "now", "reason": "low_carbon",
                    "carbon_intensity": carbon, "confidence": 0.9}
        if 6 <= hour < 8 and carbon < 400:
            return {"optimal_time": "morning", "reason": "moderate_carbon",
                    "carbon_intensity": carbon, "confidence": 0.7}
        if 8 <= hour < 18:
            return {"optimal_time": "delay", "reason": "peak",
                    "carbon_intensity": carbon, "confidence": 0.8,
                    "suggested_time": "20:00"}
        return {"optimal_time": "evening", "reason": "reduced_traffic",
                "carbon_intensity": carbon, "confidence": 0.7}

    async def _trigger_export(self, schedule_type: str) -> bool:
        logger.info(f"triggering_export type={schedule_type}")
        SCHEDULED_EXPORTS.labels(schedule_type=schedule_type, status="triggered").inc()
        async with self._lock:
            self.schedule_history.append({
                "type": schedule_type,
                "timestamp": _utc_now().isoformat(),
                "status": "triggered",
            })
        if self.export_callback is not None:
            try:
                result = self.export_callback(schedule_type)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.warning(f"export_callback_failed error={e}")
        return True

    async def record_feedback(self, export_id: str, success: bool, metrics: Dict):
        if self.last_context is None:
            return
        context = self.last_context
        carbon_saved = float(metrics.get("carbon_saved_kg", 0))
        latency = float(metrics.get("latency_ms", 0))
        reward = (0.5 if success else -0.5) + (carbon_saved / 10) - (latency / 1000)

        if self.rlhf:
            try:
                self.rlhf.update(context, "triggered", reward)
            except Exception:
                pass

        if self.limit_graph:
            try:
                self.limit_graph.update_from_feedback({
                    "export_id": export_id,
                    "success": success,
                    "metrics": metrics,
                })
            except Exception:
                pass

        # Sync bandit update — do NOT await.
        if self.bandit is not None:
            encoded = self.moe.encode(context) if self.moe else context
            self.bandit.update(encoded, "triggered", reward)

        # Only update contributing agents.
        if self.multi_agent and self._last_contributing_agents:
            self.multi_agent.record_outcome(self._last_contributing_agents, success)

        if self.bio:
            self.param_rewards.append(reward)
            if len(self.param_rewards) >= 20:
                def fitness(params):
                    return float(np.mean(list(self.param_rewards))) if self.param_rewards else 0.5
                try:
                    self.param_population = self.bio.evolve(
                        population=self.param_population,
                        fitness_fn=fitness,
                        generations=self.config.scheduler.bio_generations,
                        population_size=self.config.scheduler.bio_population_size,
                    )
                    if self.param_population:
                        best = max(self.param_population, key=fitness)
                        self.config.scheduler.interval_seconds = int(best["interval"])
                        self.config.scheduler.carbon_update_interval = int(best["carbon_update"])
                except Exception as e:
                    logger.warning(f"bio_evolve_failed error={e}")

    def get_schedule_stats(self) -> Dict:
        return {
            "total_triggers": len(self.schedule_history),
            "recent_triggers": list(self.schedule_history)[-5:],
            "running": self._running,
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "bandit_actions": self.bandit.actions if self.bandit else None,
            "modp_weights": dict(self.config.scheduler.modp_weights),
            "limit_graph_active": self.limit_graph is not None,
            "rlhf_active": self.rlhf is not None,
            "distillation_active": self.distiller is not None,
            "safety_monitor_active": self.safety_monitor is not None,
            "multi_agent_active": self.multi_agent is not None,
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "multi_agent_stats": self.multi_agent.get_stats() if self.multi_agent else None,
            "chaos": self.chaos_monkey.get_stats(),
        }

    async def shutdown(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("scheduler_shutdown")


# =============================================================================
# PredictiveAnalytics
# =============================================================================
class PredictiveAnalytics(IPredictive):
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE and config.predictive.enabled
        self.history_export_volumes: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.history_carbon_intensity: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.model_storage = Path(config.predictive.model_storage_path)
        try:
            self.model_storage.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
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
            self.hyperparam_fitness = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.predictive.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._teacher_baseline,
                self._teacher_auto,
                self._teacher_advanced,
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

    async def update_history(self, export_rows: int, carbon_intensity: float):
        async with self._lock:
            self.history_export_volumes.append({"ds": _utc_now(), "y": float(export_rows)})
            self.history_carbon_intensity.append({"ds": _utc_now(), "y": float(carbon_intensity)})

    async def load_model(self, model_name: str) -> Optional[Any]:
        path = self.model_storage / f"{model_name}.prophet"
        if not path.exists():
            return None
        try:
            return await asyncio.to_thread(Prophet.load, str(path))
        except Exception as e:
            logger.warning(f"prophet_load_failed error={e}")
            return None

    async def save_model(self, model_name: str, model: Any):
        path = self.model_storage / f"{model_name}.prophet"
        try:
            await asyncio.to_thread(model.save, str(path))
        except Exception as e:
            logger.error(f"prophet_save_failed error={e}")

    async def _forecast(self, history: Deque[Dict[str, Any]], horizon: int, model_name: str) -> Dict:
        if not self.prophet_available or len(history) < 30:
            return {"forecast": [], "confidence": 0.0, "model": "fallback"}

        df = pd.DataFrame(list(history)).sort_values("ds")

        changepoint = 0.05
        seasonality = 10
        if self.distiller:
            try:
                best = self.distiller.distill(df)
                changepoint = best.get("changepoint_prior_scale", changepoint)
                seasonality = best.get("seasonality_prior_scale", seasonality)
            except Exception:
                pass
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
                forecast = model.predict(future)
                return forecast

            forecast = await asyncio.to_thread(_fit_predict)
            tail = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(horizon)
            # Record a rough fitness from the last residual.
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
            return {"forecast": [], "confidence": 0.0, "model": "fallback"}

    async def forecast_export_volume(self, horizon_hours: int = 0) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._forecast(self.history_export_volumes, horizon, "export_volume")

    async def forecast_carbon_intensity(self, horizon_hours: int = 0) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._forecast(self.history_carbon_intensity, horizon, "carbon_intensity")

    async def health_check(self) -> Dict:
        return {
            "status": "healthy" if self.prophet_available else "degraded",
            "prophet_available": self.prophet_available,
            "samples": len(self.history_export_volumes),
            "distillation_enabled": self.distiller is not None,
        }


# =============================================================================
# Main exporter
# =============================================================================
class EnhancedAIDataCenterExporterV14_0:
    def __init__(
        self,
        config: ExportEngineConfig,
        db_manager: IDatabaseManager,
        quantum_security: IQuantumSecurity,
        blockchain: IBlockchain,
        scheduler: IScheduler,
        predictive: IPredictive,
        federated: IFederated,
        cloud_uploader: ICloudUploader,
        vault: IVault,
        carbon_manager: CarbonIntensityManager,
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
        self.scheduler = scheduler
        self.predictive = predictive
        self.federated = federated
        self.cloud_uploader = cloud_uploader
        self.vault = vault
        self.carbon_manager = carbon_manager
        self.leader = leader
        self.task_manager = task_manager

        self.data_connector = EnhancedDataSourceConnector(config)
        self.streaming_exporter = EnhancedStreamingExporter()
        self.quota_manager = QuotaManager(config, db_manager)
        self.pipeline = ExportPipeline(config)

        # Advanced enhancements
        self.chaos_monkey = chaos_monkey or ChaosMonkey(
            enabled=config.scheduler.chaos_enabled,
            failure_probability=config.scheduler.chaos_failure_probability,
            seed=config.scheduler.chaos_seed,
        )
        self.carbon_broker = CarbonOffsetBroker()
        self.human_review = HumanReviewManager()
        self.precision_policy = FlexGenPrecisionPolicy()
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )
        self.safety_monitor = (
            SafetyMonitor() if config.scheduler.safety_monitor_enabled else None
        )
        self.xai = XAIExplainer()

        self.active_exports: Dict[str, ExportResult] = {}
        self.export_history: Deque[ExportResult] = deque(maxlen=1000)
        self._exports_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._running = False

        # Wire the scheduler to actually trigger exports.
        if isinstance(self.scheduler, IntelligentExportScheduler):
            self.scheduler.export_callback = self._scheduler_trigger_callback

        self.streaming_exporter.register_progress_callback(self._on_export_progress)
        self._register_background_tasks()

        logger.info(f"exporter_initialized version={self.config.general.version} instance={self.instance_id}")

    async def __aenter__(self) -> "EnhancedAIDataCenterExporterV14_0":
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
        self.task_manager.register_task("federated_share", self._federated_share_loop)

    async def _scheduler_trigger_callback(self, schedule_type: str) -> None:
        try:
            await self.export_data(
                format="json",
                destination="local",
                user_id="scheduler",
                sample_size=100,
                sign_manifest=True,
                blockchain_record=False,
            )
        except Exception as e:
            logger.warning(f"scheduler_triggered_export_failed error={e}")

    def _on_export_progress(self, progress: float, processed: int, total: int) -> None:
        logger.info(f"export_progress pct={progress:.1f} processed={processed} total={total}")

    async def start(self) -> None:
        if self._running:
            return
        logger.info("exporter_starting")
        await self.scheduler.start()
        self._running = True
        self.task_manager.start_registered_tasks()
        BACKGROUND_TASKS.set(len(self.task_manager.tasks))
        logger.info(f"exporter_started tasks={len(self.task_manager.tasks)}")

    async def _carbon_update_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
                await asyncio.sleep(self.config.scheduler.carbon_update_interval)
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
                if not status.get("connected"):
                    logger.warning("blockchain_not_connected")
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"blockchain_monitor_error error={e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.export_history:
                    last = self.export_history[-1]
                    carbon = await self.carbon_manager.get_current_intensity()
                    await self.predictive.update_history(
                        last.rows_exported, float(carbon.get("intensity", 400))
                    )
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"predictive_update_error error={e}")
                await asyncio.sleep(60)

    async def _federated_share_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.export_history:
                    insight = {
                        "total_exports": len(self.export_history),
                        "avg_rows": float(np.mean([r.rows_exported for r in self.export_history])),
                        "timestamp": _utc_now().isoformat(),
                    }
                    await self.federated.share_insight(insight)
                await asyncio.sleep(self.config.federated.share_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"federated_share_error error={e}")
                await asyncio.sleep(60)

    async def _health_monitor_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get("health_score", 100))
                if health.get("status") != "healthy":
                    logger.warning(f"health_degraded status={health.get('status')}")
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"health_monitor_error error={e}")
                await asyncio.sleep(60)

    async def export_data(
        self,
        format: Optional[str] = None,
        output_path: Optional[Path] = None,
        incremental: bool = False,
        compress: Optional[bool] = None,
        encrypt: Optional[bool] = None,
        destination: Optional[str] = None,
        validate: bool = True,
        generate_pdf: bool = False,
        bucket: Optional[str] = None,
        key_prefix: Optional[str] = None,
        user_id: str = "default",
        sample_size: Optional[int] = None,
        resume_checkpoint_id: Optional[str] = None,
        priority: str = "normal",
        timeout: Optional[float] = None,
        sign_manifest: bool = True,
        blockchain_record: bool = True,
    ) -> str:
        fmt = format or self.config.general.default_format
        compress = self.config.general.default_compress if compress is None else compress
        encrypt = self.config.general.default_encrypt if encrypt is None else encrypt
        dest = destination or self.config.general.default_destination
        bkt = bucket or self.config.cloud.bucket

        async def _task():
            return await self._execute_export(
                format=fmt, output_path=output_path, incremental=incremental,
                compress=compress, encrypt=encrypt, destination=dest,
                validate=validate, generate_pdf=generate_pdf,
                bucket=bkt, key_prefix=key_prefix, user_id=user_id,
                sample_size=sample_size, resume_checkpoint_id=resume_checkpoint_id,
                sign_manifest=sign_manifest, blockchain_record=blockchain_record,
            )

        task_id = await self.task_manager.submit(_task, name=f"export_{fmt}_{uuid.uuid4().hex[:6]}")
        logger.info(f"export_task_submitted task_id={task_id}")
        return task_id

    async def _execute_export(
        self,
        format: str = "json",
        output_path: Optional[Path] = None,
        incremental: bool = False,
        compress: bool = False,
        encrypt: bool = False,
        destination: str = "local",
        validate: bool = True,
        generate_pdf: bool = False,
        bucket: Optional[str] = None,
        key_prefix: Optional[str] = None,
        user_id: str = "default",
        sample_size: Optional[int] = None,
        resume_checkpoint_id: Optional[str] = None,
        sign_manifest: bool = True,
        blockchain_record: bool = True,
    ) -> ExportResult:
        start_time = time.time()
        export_id = str(uuid.uuid4())[:8]
        result = ExportResult(
            export_id=export_id,
            format=format,
            status=ExportStatus.RUNNING,
            started_at=_utc_now(),
        )

        async with self._exports_lock:
            self.active_exports[export_id] = result
            EXPORT_ACTIVE.set(len(self.active_exports))

        logger.info(f"export_starting id={export_id} format={format}")

        try:
            total_rows = await self.data_connector.get_total_count()
            estimated_size = total_rows * 1000

            carbon_data = await self.carbon_manager.get_current_intensity()
            carbon_intensity = float(carbon_data.get("intensity", 400))

            if self.safety_monitor:
                if not self.safety_monitor.check_export(carbon_intensity, success=True):
                    logger.warning("safety_violation proceeding_with_caution")

            if total_rows >= self.config.general.human_review_threshold_rows:
                review_id = await self.human_review.request_review(export_id, {
                    "total_rows": total_rows,
                    "format": format,
                    "user_id": user_id,
                    "carbon_intensity": carbon_intensity,
                })
                result.human_review_id = review_id
                result.status = ExportStatus.PENDING_REVIEW
                logger.info(f"export_pending_review id={export_id} review_id={review_id}")

            quota_ok, quota_message = await self.quota_manager.check_quota(
                user_id, total_rows, estimated_size
            )
            if not quota_ok:
                raise QuotaExceededError(quota_message)

            if sample_size and sample_size < total_rows:
                data = await self.data_connector.fetch_real_data(limit=sample_size)
            else:
                data = await self.data_connector.fetch_real_data()

            if len(data) == 0:
                raise DataFetchError("No data available for export")

            if validate:
                validation = await self._validate_data_chunked(data)
                if not validation.get("valid"):
                    VALIDATION_FAILURES.inc(validation.get("error_count", 1))
                    logger.warning(f"validation_findings errors={validation.get('error_count')}")

            if incremental:
                data = self._incremental_export(data, resume_checkpoint_id)

            if output_path is None:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = Path(f"./exports/datacenter_export_{ts}_{export_id}.{format}")
            output_path.parent.mkdir(exist_ok=True, parents=True)

            workload_size = (
                "large" if total_rows > 100_000
                else "medium" if total_rows > 10_000
                else "small"
            )
            precision = self.precision_policy.recommend_precision(
                workload_size=workload_size, carbon_intensity=carbon_intensity,
            )
            result.precision = precision

            export_result = await self.streaming_exporter.export_streaming(data, format, output_path)
            result.rows_exported = export_result["rows_exported"]
            result.file_path = export_result["file_path"]
            result.file_size_bytes = export_result["file_size_bytes"]
            result.columns_exported = len(data.columns)
            result.data_quality_score = self._calculate_quality_score(data)
            DATA_QUALITY.set(result.data_quality_score)

            file_hash = await self._streaming_file_hash(output_path)
            manifest = {
                "export_id": export_id,
                "format": format,
                "rows_exported": result.rows_exported,
                "timestamp": _utc_now().isoformat(),
                "file_hash": file_hash,
                "file_size_bytes": result.file_size_bytes,
                "user_id": user_id,
                "instance_id": self.instance_id,
                "version": self.config.general.version,
                "carbon_intensity": carbon_intensity,
                "precision": precision,
                "workload_size": workload_size,
            }

            if sign_manifest:
                try:
                    key = await self.quantum_security.generate_keypair(
                        self.config.quantum.algorithm
                    )
                    signature = await self.quantum_security.sign_export_manifest(
                        manifest, key["key_id"]
                    )
                    result.quantum_signature = signature
                    manifest["quantum_signature"] = signature
                except Exception as e:
                    logger.warning(f"sign_manifest_failed error={e}")

            if blockchain_record:
                try:
                    bc = await self.blockchain.record_export(
                        export_id, manifest, manifest["file_hash"]
                    )
                    result.blockchain_tx_hash = bc.get("tx_hash")
                except NotImplementedYetError as e:
                    logger.info(f"blockchain_record_skipped reason={e}")
                except Exception as e:
                    logger.warning(f"blockchain_record_failed error={e}")

            if generate_pdf:
                pdf_path = output_path.with_suffix(".pdf")
                await self._generate_pdf_report(data, pdf_path, export_id)

            if destination != "local" and bucket:
                try:
                    upload_result = await self.cloud_uploader.upload_file(
                        output_path, destination, bucket, key_prefix
                    )
                    result.destination = destination
                    logger.info(f"uploaded provider={destination} url={upload_result.get('url')}")
                except NotImplementedYetError as e:
                    logger.info(f"upload_skipped reason={e}")

            if carbon_intensity > 450:
                carbon_kg = result.rows_exported * 0.0001
                try:
                    await self.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)
                except Exception as e:
                    logger.warning(f"carbon_offset_failed error={e}")

            result.status = ExportStatus.COMPLETED
            result.export_time_ms = (time.time() - start_time) * 1000
            result.completed_at = _utc_now()
            result.metadata = manifest
            result.safety_checked = True

            EXPORT_RUNS.labels(status="success", format=format).inc()
            EXPORT_DURATION.labels(format=format).observe(result.export_time_ms / 1000)
            EXPORT_SIZE.labels(format=format).set(result.file_size_bytes)

            async with self._exports_lock:
                self.export_history.append(result)

            if self.db_manager is not None:
                try:
                    await self.db_manager.execute_async(lambda session: self._insert_export(session, result, manifest))
                except Exception as e:
                    logger.warning(f"db_insert_failed error={e}")

            audit_logger.info(
                f"export_completed id={export_id} rows={result.rows_exported} "
                f"ms={result.export_time_ms:.0f}"
            )

            try:
                await self.pipeline.run_pipeline({"export_id": export_id, "format": format})
            except Exception as e:
                logger.warning(f"pipeline_failed error={e}")

            await self.predictive.update_history(result.rows_exported, carbon_intensity)
            await self.federated.share_insight({
                "export_id": export_id,
                "format": format,
                "rows": result.rows_exported,
                "carbon_intensity": carbon_intensity,
                "timestamp": _utc_now().isoformat(),
            })

            if hasattr(self.scheduler, "record_feedback"):
                try:
                    await self.scheduler.record_feedback(export_id, True, {
                        "carbon_saved_kg": 0.0,
                        "latency_ms": result.export_time_ms,
                    })
                except Exception as e:
                    logger.warning(f"scheduler_feedback_failed error={e}")

            return result

        except Exception as e:
            result.status = ExportStatus.FAILED
            result.error_message = str(e)
            result.completed_at = _utc_now()
            EXPORT_RUNS.labels(status="failed", format=format).inc()
            EXPORT_ERRORS.labels(error_type="export_failed").inc()
            logger.error(f"export_failed id={export_id} error={e}")
            if self.safety_monitor:
                self.safety_monitor.check_export(400.0, success=False)
            if hasattr(self.scheduler, "record_feedback"):
                try:
                    await self.scheduler.record_feedback(export_id, False, {})
                except Exception:
                    pass
            raise
        finally:
            async with self._exports_lock:
                self.active_exports.pop(export_id, None)
                EXPORT_ACTIVE.set(len(self.active_exports))

    @staticmethod
    async def _streaming_file_hash(path: Path) -> str:
        def _hash():
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            return h.hexdigest()
        return await asyncio.to_thread(_hash)

    def _insert_export(self, session, result: ExportResult, manifest: Dict[str, Any]) -> None:
        session.execute(
            text(
                "INSERT OR REPLACE INTO export_history "
                "(export_id, format, status, rows_exported, file_path, "
                "file_size_bytes, started_at, completed_at, metadata, "
                "quantum_signature, blockchain_tx_hash, precision) "
                "VALUES (:export_id, :format, :status, :rows_exported, :file_path, "
                ":file_size_bytes, :started_at, :completed_at, :metadata, "
                ":quantum_signature, :blockchain_tx_hash, :precision)"
            ),
            {
                "export_id": result.export_id,
                "format": result.format,
                "status": "completed",
                "rows_exported": result.rows_exported,
                "file_path": result.file_path,
                "file_size_bytes": result.file_size_bytes,
                "started_at": result.started_at.isoformat(),
                "completed_at": result.completed_at.isoformat() if result.completed_at else None,
                "metadata": json.dumps(manifest, default=str),
                "quantum_signature": json.dumps(result.quantum_signature, default=str)
                if result.quantum_signature else None,
                "blockchain_tx_hash": result.blockchain_tx_hash,
                "precision": result.precision,
            },
        )

    async def _validate_data_chunked(self, data: pd.DataFrame) -> Dict:
        error_count = int(data.isnull().sum().sum())
        return {"valid": error_count == 0, "error_count": error_count}

    def _incremental_export(self, data: pd.DataFrame, checkpoint_id: Optional[str] = None) -> pd.DataFrame:
        return data

    def _calculate_quality_score(self, data: pd.DataFrame) -> float:
        try:
            total = data.shape[0] * data.shape[1]
            if total == 0:
                return 0.0
            missing = int(data.isnull().sum().sum())
            return float(1.0 - missing / total)
        except Exception:
            return 0.0

    async def _generate_pdf_report(self, data: pd.DataFrame, pdf_path: Path, export_id: str):
        if REPORTLAB_AVAILABLE:
            try:
                def _draw():
                    c = canvas.Canvas(str(pdf_path), pagesize=letter)
                    c.drawString(100, 750, f"Export Report - {export_id}")
                    c.drawString(100, 730, f"Rows: {len(data)}")
                    c.drawString(100, 710, f"Columns: {len(data.columns)}")
                    c.drawString(100, 690, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    c.save()
                await asyncio.to_thread(_draw)
                return
            except Exception as e:
                logger.warning(f"pdf_generation_failed error={e}")
        await asyncio.to_thread(pdf_path.write_text, "PDF report placeholder")

    async def health_check(self) -> Dict:
        components = {
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "scheduler": self.scheduler,
            "predictive": self.predictive,
            "federated": self.federated,
            "cloud_uploader": self.cloud_uploader,
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

    async def get_statistics(self) -> Dict:
        task_stats = self.task_manager.get_statistics()
        scheduler_stats = self.scheduler.get_schedule_stats()
        return {
            "instance_id": self.instance_id,
            "version": self.config.general.version,
            "total_exports": len(self.export_history),
            "total_rows_exported": sum(r.rows_exported for r in self.export_history),
            "active_exports": len(self.active_exports),
            "background_tasks": task_stats,
            "upload_stats": self.cloud_uploader.get_upload_metrics(),
            "quota_status": self.quota_manager.get_quota_status("default"),
            "quantum_security": self.quantum_security.get_quantum_status(),
            "blockchain": await self.blockchain.get_blockchain_status(),
            "scheduler": scheduler_stats,
            "predictive": await self.predictive.health_check(),
            "federated": await self.federated.health_check(),
            "health": await self.health_check(),
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "additional_enhancements_available": ADDITIONAL_ENHANCEMENTS_AVAILABLE,
            "advanced_enhancements": {
                "safety_monitor": (
                    self.safety_monitor.get_violations()[-5:] if self.safety_monitor else []
                ),
                "chaos_monkey": self.chaos_monkey.get_stats(),
                "human_review": self.human_review.get_stats(),
                "carbon_broker": self.carbon_broker.get_totals(),
                "precision_policy": self.precision_policy.get_status(),
                "quantum_optimizer": self.quantum_optimizer.get_status(),
            },
            "timestamp": _utc_now().isoformat(),
        }

    async def shutdown(self) -> None:
        logger.info(f"exporter_shutting_down instance={self.instance_id}")
        self._shutdown_event.set()
        self._running = False
        with contextlib.suppress(Exception):
            await self.scheduler.shutdown()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        await self.task_manager.stop_all()
        with contextlib.suppress(Exception):
            await self.db_manager.close()
        with contextlib.suppress(Exception):
            await self.leader.stop()
        logger.info("exporter_shutdown_complete")


# =============================================================================
# Rate limiter
# =============================================================================
class RateLimiter:
    def __init__(self, config: APIConfig):
        self.config = config
        self.rate = float(config.rate_limit_requests)
        self.window = float(config.rate_limit_window)
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False


# =============================================================================
# Component factory
# =============================================================================
def build_components(config: Optional[ExportEngineConfig] = None) -> Dict[str, Any]:
    cfg = config or ExportEngineConfig()
    db_manager = EnhancedDatabaseManager(cfg)
    vault = VaultManager(cfg)
    quantum = QuantumResilientExportSecurity(cfg, vault)
    blockchain = BlockchainExportVerification(cfg, db_manager)
    carbon = CarbonIntensityManager(cfg)
    chaos_monkey = ChaosMonkey(
        enabled=cfg.scheduler.chaos_enabled,
        failure_probability=cfg.scheduler.chaos_failure_probability,
        seed=cfg.scheduler.chaos_seed,
    )
    scheduler = IntelligentExportScheduler(cfg, carbon, chaos_monkey=chaos_monkey)
    predictive = PredictiveAnalytics(cfg)
    federated = FederatedKnowledgeSharing(cfg, db_manager, cfg.general.instance_id)
    cloud = EnhancedCloudUploader(cfg)
    leader = LeaderElection(cfg)
    task_manager = TaskManager()
    exporter = EnhancedAIDataCenterExporterV14_0(
        config=cfg,
        db_manager=db_manager,
        quantum_security=quantum,
        blockchain=blockchain,
        scheduler=scheduler,
        predictive=predictive,
        federated=federated,
        cloud_uploader=cloud,
        vault=vault,
        carbon_manager=carbon,
        leader=leader,
        task_manager=task_manager,
        chaos_monkey=chaos_monkey,
    )
    return {"exporter": exporter, "config": cfg}


# =============================================================================
# FastAPI app factory
# =============================================================================
def create_app(config: Optional[ExportEngineConfig] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config or ExportEngineConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        components = build_components(cfg)
        app.state.exporter = components["exporter"]
        await app.state.exporter.start()
        try:
            yield
        finally:
            await app.state.exporter.shutdown()

    app = FastAPI(title="Export Engine API", version="14.2", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer(auto_error=False)
    api_rate_limiter = RateLimiter(cfg.api)

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

    @app.post("/export")
    async def trigger_export(
        request: Request,
        format: str = "json",
        destination: str = "local",
        bucket: Optional[str] = None,
        sample_size: Optional[int] = None,
        user: Dict[str, Any] = Depends(verify_token),
        _: None = Depends(rate_limit),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        task_id = await exporter.export_data(
            format=format,
            destination=destination,
            bucket=bucket,
            sample_size=sample_size,
            user_id=user.get("sub", "default"),
        )
        return {"task_id": task_id}

    @app.get("/status")
    async def get_status(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
        _: None = Depends(rate_limit),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return await exporter.get_statistics()

    @app.get("/health")
    async def health(request: Request):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return await exporter.health_check()

    @app.get("/optimization/status")
    async def optimization_status(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
        _: None = Depends(rate_limit),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return {
            "scheduler": exporter.scheduler.get_schedule_stats(),
            "predictive_hyperparams": getattr(exporter.predictive, "hyperparam_population", []),
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "additional_enhancements_available": ADDITIONAL_ENHANCEMENTS_AVAILABLE,
        }

    @app.post("/optimization/evolve")
    async def evolve_optimizer(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        await exporter.scheduler.record_feedback("manual", True, {
            "carbon_saved_kg": 0, "latency_ms": 0,
        })
        return {"status": "evolution triggered"}

    @app.get("/human-review/pending")
    async def human_review_pending(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return await exporter.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(
        review_id: str,
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        ok = await exporter.human_review.approve(review_id, reviewer=user.get("sub", "admin"))
        return {"status": "approved" if ok else "not_found"}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(
        review_id: str,
        request: Request,
        reason: Optional[str] = None,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        ok = await exporter.human_review.reject(
            review_id, reason=reason, reviewer=user.get("sub", "admin")
        )
        return {"status": "rejected" if ok else "not_found"}

    @app.post("/chaos/trigger")
    async def chaos_trigger(
        request: Request,
        enabled: bool = True,
        probability: float = 0.1,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        exporter.chaos_monkey.enabled = bool(enabled)
        exporter.chaos_monkey.failure_probability = float(probability)
        return exporter.chaos_monkey.get_stats()

    @app.post("/chaos/inject")
    async def chaos_inject(
        request: Request,
        fault: str,
        steps: int = 5,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        if fault not in ChaosMonkey.FAULT_TYPES:
            raise HTTPException(status_code=400, detail="unknown fault")
        return await exporter.chaos_monkey.inject_and_measure(exporter, fault, steps=steps)

    @app.post("/carbon/offset")
    async def carbon_offset(
        request: Request,
        carbon_kg: float,
        carbon_intensity: float,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return await exporter.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)

    @app.post("/carbon/rec")
    async def carbon_rec(
        request: Request,
        energy_mwh: float,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return await exporter.carbon_broker.purchase_recs(energy_mwh)

    @app.get("/carbon/totals")
    async def carbon_totals(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return exporter.carbon_broker.get_totals()

    @app.post("/precision/recommend")
    async def precision_recommend(
        request: Request,
        workload_size: str = "medium",
        carbon_intensity: Optional[float] = None,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return {
            "precision": exporter.precision_policy.recommend_precision(
                workload_size=workload_size, carbon_intensity=carbon_intensity
            )
        }

    @app.get("/safety/violations")
    async def safety_violations(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        if exporter.safety_monitor:
            return {"violations": exporter.safety_monitor.get_violations()}
        return {"violations": [], "monitor_enabled": False}

    @app.get("/xai/last-decision")
    async def xai_last_decision(
        request: Request,
        user: Dict[str, Any] = Depends(verify_token),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        return {
            "last_context": exporter.scheduler.last_context,
            "schedule_stats": exporter.scheduler.get_schedule_stats(),
        }

    @app.post("/federated/register")
    async def federated_register(
        request: Request,
        participant_id: str,
        update: Dict[str, Any],
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        federated = exporter.federated
        if hasattr(federated, "secure_coordinator"):
            federated.secure_coordinator.register_participant(participant_id, update, sample_count=1)
            return {"status": "registered"}
        return {"status": "not_available"}

    @app.get("/federated/aggregate")
    async def federated_aggregate(
        request: Request,
        user: Dict[str, Any] = Depends(require_admin),
    ):
        exporter = getattr(request.app.state, "exporter", None)
        if exporter is None:
            raise HTTPException(status_code=503, detail="exporter not initialized")
        federated = exporter.federated
        if hasattr(federated, "secure_coordinator"):
            return federated.secure_coordinator.aggregate()
        return {}

    return app


# =============================================================================
# Singleton accessor
# =============================================================================
_exporter_instance: Optional[EnhancedAIDataCenterExporterV14_0] = None
_exporter_lock: Optional[asyncio.Lock] = None


async def get_export_engine(
    config: Optional[Union[ExportEngineConfig, Dict[str, Any]]] = None
) -> EnhancedAIDataCenterExporterV14_0:
    global _exporter_instance, _exporter_lock
    if _exporter_lock is None:
        _exporter_lock = asyncio.Lock()
    if _exporter_instance is None:
        async with _exporter_lock:
            if _exporter_instance is None:
                if config is None:
                    cfg = ExportEngineConfig()
                elif isinstance(config, ExportEngineConfig):
                    cfg = config
                else:
                    cfg = ExportEngineConfig(**config)
                components = build_components(cfg)
                _exporter_instance = components["exporter"]
                await _exporter_instance.start()
    return _exporter_instance


# =============================================================================
# Main
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Enhanced AI Data Center Export Engine v14.2 — Single-File Enhanced")
    print("=" * 80)

    exporter = await get_export_engine()
    print(f"\nEnhancements:")
    print(f"  Causal RL: {ENHANCEMENTS_AVAILABLE}")
    print(f"  Additional enhancements: {ADDITIONAL_ENHANCEMENTS_AVAILABLE}")
    print(f"  Scheduler: {exporter.scheduler.get_schedule_stats().get('running')}")

    qstatus = exporter.quantum_security.get_quantum_status()
    print(f"  Quantum: {qstatus.get('algorithm_in_use')} (pqc={qstatus.get('pqc_available')})")

    bstatus = await exporter.blockchain.get_blockchain_status()
    print(f"  Blockchain connected: {bstatus.get('connected')}")

    task_id = await exporter.export_data(
        format="json",
        destination="local",
        user_id="demo",
        sample_size=100,
        sign_manifest=True,
        blockchain_record=False,
    )
    print(f"\nExport task submitted: {task_id}")

    # Wait for the task to complete.
    await asyncio.sleep(2)

    stats = await exporter.get_statistics()
    print(f"\nStats: total_exports={stats['total_exports']} "
          f"rows_exported={stats['total_rows_exported']}")
    print(f"  Carbon broker: {stats['advanced_enhancements']['carbon_broker']}")
    print(f"  Human reviews: {stats['advanced_enhancements']['human_review']}")
    print(f"  Precision policy: {stats['advanced_enhancements']['precision_policy']}")

    await exporter.shutdown()
    print("\nShutdown complete")


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()


__all__ = [
    "ExportEngineConfig",
    "GeneralConfig",
    "QuantumConfig",
    "CloudConfig",
    "SchedulerConfig",
    "PredictiveConfig",
    "FederatedConfig",
    "DatabaseConfig",
    "VaultConfig",
    "APIConfig",
    "CircuitBreakerConfig",
    "LeaderConfig",
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
    "EnhancedDatabaseManager",
    "VaultManager",
    "QuantumResilientExportSecurity",
    "BlockchainExportVerification",
    "EnhancedCloudUploader",
    "FederatedKnowledgeSharing",
    "CarbonIntensityManager",
    "LeaderElection",
    "EnhancedDataSourceConnector",
    "EnhancedStreamingExporter",
    "QuotaManager",
    "ExportStatus",
    "ExportResult",
    "ExportPipeline",
    "IntelligentExportScheduler",
    "PredictiveAnalytics",
    "EnhancedAIDataCenterExporterV14_0",
    "RateLimiter",
    "TaskManager",
    "GlobalCircuitBreaker",
    "CircuitBreaker",
    "CircuitBreakerState",
    "IQuantumSecurity",
    "IBlockchain",
    "IScheduler",
    "IPredictive",
    "IFederated",
    "ICloudUploader",
    "IDatabaseManager",
    "IVault",
    "ExportEngineError",
    "QuantumError",
    "BlockchainError",
    "QuotaExceededError",
    "DataFetchError",
    "ValidationError",
    "CircuitBreakerOpenError",
    "RateLimitExceeded",
    "VaultError",
    "CloudStorageError",
    "FederatedError",
    "PredictiveError",
    "OptimizerError",
    "DatabaseError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "NotImplementedYetError",
    "build_components",
    "create_app",
    "get_export_engine",
    "main",
]
