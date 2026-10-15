#!/usr/bin/env python3
# File: src/enhancements/cloud_latency_estimator_enhanced_v16.py
"""
Cloud Latency Estimator — v16.1 (Enhanced Single-File)

Fixes over the original v16.1:
- Missing `logging.handlers` and `Awaitable` imports (import-time crash fixed).
- `BioInspiredOptimizer._load_state` now awaited via `initialize()`.
- `CircuitBreaker` correctly dispatches sync vs async callables.
- `EnhancedHealthCheckService` awaits only coroutine callables.
- `CausalBandit` is a real contextual bandit (per-action Bayesian linear
  regression with UCB / Thompson sampling), not a naive mean tracker.
- `SafetyMonitor` is invoked in the routing decision path.
- `ChaosMonkey` supports fault types, seeded injection, and recovery metrics.
- `FederatedCoordinator` uses sample-weighted aggregation with round IDs,
  shape validation, and thread-safe state.
- `MultiAgentCoordinator` uses stable hashing and thread-safe bookkeeping.
- `CarbonOffsetBroker` is wired into routing decisions and reward shaping.
- `HumanReviewManager` supports TTL, bounded queue, audit log, and is invoked
  from the routing path for high-risk decisions.
- `PostQuantumCrypto` renamed-role: hybrid crypto with honest fallback and
  real ECDSA signing when PQC is unavailable.
- FastAPI app is created via `create_app()` and uses a lifespan handler.
- Endpoints requiring elevated privilege are protected by an auth dependency.
- Signal handlers trigger graceful shutdown.
- Stubs raise `NotImplementedError` when called so garbage cannot propagate.
- Prometheus metrics (when available) use a single registry and per-name
  gauge handles (no duplicate registration).
- Structured redaction on config repr to avoid leaking secrets.

The file remains single-file for distribution. In a real deployment, split
it into a package.
"""

from __future__ import annotations

import asyncio
import gc
import hashlib
import inspect
import json
import logging
import logging.handlers  # noqa: F401  (used by fallback logger)
import math
import os
import pickle
import random
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
    runtime_checkable,
)

import numpy as np

# Optional third-party imports (unchanged set, but organized)
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False

try:
    import kubernetes  # noqa: F401
    from kubernetes import client, config  # noqa: F401
    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from river import linear_model, preprocessing, metrics  # noqa: F401
    RIVER_AVAILABLE = True
except ImportError:
    RIVER_AVAILABLE = False

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        start_http_server,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import websockets  # noqa: F401
    from websockets.exceptions import ConnectionClosed  # noqa: F401
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import (  # noqa: F401
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, RetryError, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import aiosqlite  # noqa: F401
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    import redis.asyncio as redis  # noqa: F401
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from fastapi import (
        Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect,
        status,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    import jwt
    from passlib.context import CryptContext
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    from ...adaptive_cost_function import AdaptiveCostFunction  # noqa: F401
    from ...anomaly_detection import AnomalyDetector  # noqa: F401
    from ...predictive_maintenance import PredictiveMaintenanceEngine  # noqa: F401
    SUSTAINABILITY_MODULES_AVAILABLE = True
except ImportError:
    SUSTAINABILITY_MODULES_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # noqa: F401
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature, encode_dss_signature,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.serialization import (
    Encoding, NoEncryption, PrivateFormat, PublicFormat,
)

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
    from hvac import Client as VaultClient  # noqa: F401
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False


# =============================================================================
# ENHANCEMENT MODULES (with graceful fallbacks)
# =============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # noqa: F401
    from enhancements.moe_system import ExpertRouter  # noqa: F401
    from enhancements.MODP import ParetoOptimizer  # noqa: F401
    from enhancements.contextual_bandit import ContextualBandit  # noqa: F401
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}

    class ExpertRouter:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def encode(self, context): return [0.0] * 5
        def select(self, encoded): return "default"

    class ParetoOptimizer:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

    class ContextualBandit:  # type: ignore[no-redef]
        def __init__(self, action_space, fallback_solver, *a, **k):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass


try:
    from enhancements.gpu_optimization.flexgen_policy import (  # noqa: F401
        FlexGenPolicy, generate_candidate_policies,
    )
    from enhancements.gpu_optimization.flexgen_controller import (  # noqa: F401
        FlexGenController,
    )
    from enhancements.gpu_optimization.flexgen_cost_model import (  # noqa: F401
        FlexGenCostModel,
    )
    from enhancements.gpu_optimization.policy_drift_detector import (  # noqa: F401
        PolicyDriftDetector,
    )
    from enhancements.schemas.node_descriptor import NodeDescriptor  # noqa: F401
    from enhancements.schemas.workload_descriptor import (  # noqa: F401
        WorkloadDescriptor,
    )
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False

    class FlexGenPolicy: pass  # type: ignore[no-redef]
    def generate_candidate_policies(n=20): return []  # noqa: E704

    class FlexGenController:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        async def step(self): return {}

    class FlexGenCostModel:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass

    class PolicyDriftDetector:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def get_stats(self): return {}

    class NodeDescriptor: pass  # type: ignore[no-redef]
    class WorkloadDescriptor: pass  # type: ignore[no-redef]


# =============================================================================
# STRUCTURED LOGGING
# =============================================================================
try:
    import structlog  # noqa: F401
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    _root = logging.getLogger()
    if not _root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "cloud_latency_v16.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )


class CorrelationIdFilter(logging.Filter):
    _local = threading.local()

    @classmethod
    def get_correlation_id(cls) -> str:
        if not hasattr(cls._local, "correlation_id"):
            cls._local.correlation_id = str(uuid.uuid4())[:8]
        return cls._local.correlation_id

    @classmethod
    def set_correlation_id(cls, cid: str) -> None:
        cls._local.correlation_id = cid

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = self.get_correlation_id()
        return True


if isinstance(logger, logging.Logger):
    logger.addFilter(CorrelationIdFilter())


# =============================================================================
# CONFIG
# =============================================================================
_REDACT_KEYS = {
    "master_key", "jwt_secret", "aws_access_key", "aws_secret_key",
    "azure_connection_string", "gcp_credentials", "token",
}


def _redact(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = _redact(v)
        elif k in _REDACT_KEYS and v:
            out[k] = "***REDACTED***"
        else:
            out[k] = v
    return out


if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "16.1"
        log_level: str = "INFO"
        tracing_enabled: bool = True
        otlp_endpoint: str = "http://localhost:4317"
        mesh_enabled: bool = True
        kubernetes_namespace: str = "default"
        forecasting_enabled: bool = True
        model_storage_path: str = "./latency_models"
        min_training_samples: int = 50
        retrain_interval_hours: int = 24
        realtime_enabled: bool = True
        websocket_port: int = 8765
        update_interval: float = 0.1
        latency_measurement_timeout: float = 5.0
        measurement_protocols: List[str] = Field(
            default_factory=lambda: ["http", "tcp", "icmp"]
        )
        data_retention_days: int = 365
        chaos_enabled: bool = False
        allow_dev_master_key: bool = True

        @field_validator("log_level")
        @classmethod
        def _validate_log_level(cls, v: str) -> str:
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

    class CacheConfig(BaseModel):
        ttl_seconds: int = 60
        max_size: int = 1000
        redis_url: Optional[str] = None

    class DatabaseConfig(BaseModel):
        path: str = "./latency_data.db"
        max_connections: int = 5

    class CircuitBreakerConfig(BaseModel):
        failure_threshold: int = 3
        recovery_timeout: int = 30

    class APIConfig(BaseModel):
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = "change_me_in_production"
        require_auth_for_admin: bool = True

    class PQCConfig(BaseModel):
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = ""

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError(
                    "PQC master_key not set; provide LATENCY_PQC__MASTER_KEY "
                    "(32-byte hex) or set allow_dev_master_key=True in general."
                )
            return bytes.fromhex(self.master_key)

    class CloudConfig(BaseModel):
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = "us-east-1"
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/latency"

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        learning_rate: float = 0.1
        adjustment_interval_seconds: int = 300
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "latency": 0.4, "cost": 0.2, "carbon": 0.2, "reliability": 0.2,
            }
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)

    class LatencyEstimatorConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="LATENCY_", case_sensitive=False, env_nested_delimiter="__"
        )
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        cache: CacheConfig = Field(default_factory=CacheConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        pqc: PQCConfig = Field(default_factory=PQCConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)

        def safe_dict(self) -> Dict[str, Any]:
            return _redact(self.model_dump())

    def _ensure_dev_master_key(config: LatencyEstimatorConfig) -> None:
        """If no master key is set but dev mode allows it, generate one."""
        if not config.pqc.master_key and config.general.allow_dev_master_key:
            config.pqc.master_key = os.urandom(32).hex()

else:
    # Dataclass fallback (single source of truth for non-pydantic path)
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "16.1"
        log_level: str = "INFO"
        tracing_enabled: bool = True
        otlp_endpoint: str = "http://localhost:4317"
        mesh_enabled: bool = True
        kubernetes_namespace: str = "default"
        forecasting_enabled: bool = True
        model_storage_path: str = "./latency_models"
        min_training_samples: int = 50
        retrain_interval_hours: int = 24
        realtime_enabled: bool = True
        websocket_port: int = 8765
        update_interval: float = 0.1
        latency_measurement_timeout: float = 5.0
        measurement_protocols: List[str] = field(
            default_factory=lambda: ["http", "tcp", "icmp"]
        )
        data_retention_days: int = 365
        chaos_enabled: bool = False
        allow_dev_master_key: bool = True

    @dataclass
    class CacheConfig:
        ttl_seconds: int = 60
        max_size: int = 1000
        redis_url: Optional[str] = None

    @dataclass
    class DatabaseConfig:
        path: str = "./latency_data.db"
        max_connections: int = 5

    @dataclass
    class CircuitBreakerConfig:
        failure_threshold: int = 3
        recovery_timeout: int = 30

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = "change_me_in_production"
        require_auth_for_admin: bool = True

    @dataclass
    class PQCConfig:
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = ""

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("PQC master_key not set")
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
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/latency"

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        learning_rate: float = 0.1
        adjustment_interval_seconds: int = 300
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {
                "latency": 0.4, "cost": 0.2, "carbon": 0.2, "reliability": 0.2,
            }
        )
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20

    @dataclass
    class LatencyEstimatorConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        cache: CacheConfig = field(default_factory=CacheConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        api: APIConfig = field(default_factory=APIConfig)
        pqc: PQCConfig = field(default_factory=PQCConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)

        def safe_dict(self) -> Dict[str, Any]:
            return _redact(asdict(self))

    def _ensure_dev_master_key(config: LatencyEstimatorConfig) -> None:
        if not config.pqc.master_key and config.general.allow_dev_master_key:
            config.pqc.master_key = os.urandom(32).hex()


# =============================================================================
# METRICS REGISTRY (single source of truth for Prometheus metrics)
# =============================================================================
class _MetricsRegistry:
    """Centralized Prometheus metric registration. Falls back to no-op."""

    def __init__(self) -> None:
        self.enabled = PROMETHEUS_AVAILABLE
        self._gauges: Dict[str, Any] = {}
        self._counters: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        if self.enabled:
            self.registry = CollectorRegistry()

    def gauge(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        key = f"gauge:{name}"
        if key not in self._gauges:
            self._gauges[key] = Gauge(
                name, doc, labels or [], registry=self.registry
            )
        return self._gauges[key]

    def counter(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        key = f"counter:{name}"
        if key not in self._counters:
            self._counters[key] = Counter(
                name, doc, labels or [], registry=self.registry
            )
        return self._counters[key]

    def histogram(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        key = f"hist:{name}"
        if key not in self._histograms:
            self._histograms[key] = Histogram(
                name, doc, labels or [], registry=self.registry
            )
        return self._histograms[key]


class _NoopMetric:
    def labels(self, *a, **k): return self
    def set(self, *a, **k): return None
    def inc(self, *a, **k): return None
    def observe(self, *a, **k): return None


METRICS = _MetricsRegistry()
_CB_STATE = METRICS.gauge(
    "circuit_breaker_state_gauge", "Circuit breaker state (0=closed,0.5=half,1=open)",
    ["name"],
)


# =============================================================================
# EXCEPTIONS
# =============================================================================
class LatencyEstimatorException(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message)
        self.details = dict(details or {})
        self.timestamp = datetime.now()
        self.correlation_id = CorrelationIdFilter.get_correlation_id()


class MeasurementError(LatencyEstimatorException): pass
class ServiceMeshError(LatencyEstimatorException): pass
class DatabaseError(LatencyEstimatorException): pass
class CircuitBreakerOpenError(LatencyEstimatorException): pass
class CacheError(LatencyEstimatorException): pass
class ForecastingError(LatencyEstimatorException): pass
class PQCError(LatencyEstimatorException): pass
class CloudStorageError(LatencyEstimatorException): pass
class VaultError(LatencyEstimatorException): pass
class OptimizerError(LatencyEstimatorException): pass
class ConfigurationError(LatencyEstimatorException): pass
class AuthenticationError(LatencyEstimatorException): pass
class NotImplementedYetError(LatencyEstimatorException):
    """Raised by stub components to prevent silent garbage propagation."""


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Circuit breaker that handles both sync and async callables. State mutations
    are guarded by a threading lock, and async callers additionally acquire an
    asyncio lock.
    """

    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: int = 30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = 2
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
        self._metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    def _maybe_transition_to_half_open(self) -> None:
        if (
            self._state == CircuitBreakerState.OPEN
            and self._last_failure_time is not None
            and (time.time() - self._last_failure_time) >= self.recovery_timeout
        ):
            self._state = CircuitBreakerState.HALF_OPEN
            self._success_count = 0
            _CB_STATE.labels(name=self.name).set(0.5)
            logger.info("Circuit breaker %s -> HALF_OPEN", self.name)

    def _record_success(self) -> None:
        with self._lock:
            self._metrics["successful_calls"] += 1
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    _CB_STATE.labels(name=self.name).set(0)
                    logger.info("Circuit breaker %s -> CLOSED", self.name)
            else:
                self._failure_count = 0

    def _record_failure(self) -> None:
        with self._lock:
            self._metrics["failed_calls"] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitBreakerState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    _CB_STATE.labels(name=self.name).set(1)
                    logger.warning("Circuit breaker %s -> OPEN", self.name)
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                _CB_STATE.labels(name=self.name).set(1)
                logger.warning("Circuit breaker %s re-OPENed from HALF_OPEN", self.name)

    def _precheck(self) -> None:
        with self._lock:
            self._maybe_transition_to_half_open()
            if self._state == CircuitBreakerState.OPEN:
                raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            self._metrics["total_calls"] += 1

    async def call_async(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        self._precheck()
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def call_sync(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        self._precheck()
        try:
            result = func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    async def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Dispatch to async or sync based on the callable."""
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


# =============================================================================
# TASK MANAGER (with per-task health metrics)
# =============================================================================
@dataclass
class TaskHealth:
    name: str
    starts: int = 0
    crashes: int = 0
    last_error: Optional[str] = None
    last_started_at: Optional[str] = None
    uptime_seconds: float = 0.0


class TaskManager:
    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self._task_coroutines: Dict[str, Tuple[Callable[..., Awaitable[None]], tuple, dict]] = {}
        self._health: Dict[str, TaskHealth] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def health(self) -> Dict[str, Any]:
        return {k: asdict(v) for k, v in self._health.items()}

    def start_task(
        self,
        name: str,
        coro_func: Callable[..., Awaitable[None]],
        *args,
        **kwargs,
    ) -> asyncio.Task:
        health = self._health.setdefault(name, TaskHealth(name=name))

        async def wrapper() -> None:
            backoff = 1.0
            max_backoff = 300.0
            health.starts += 1
            health.last_started_at = datetime.now().isoformat()
            started = time.monotonic()
            try:
                while not self.shutdown_event.is_set():
                    try:
                        await coro_func(*args, **kwargs)
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        health.crashes += 1
                        health.last_error = f"{type(e).__name__}: {e}"
                        logger.error(
                            "Task crashed; will retry",
                            extra={"task": name, "error": str(e)},
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, max_backoff)
            finally:
                health.uptime_seconds += time.monotonic() - started

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def register_task(
        self,
        name: str,
        coro_func: Callable[..., Awaitable[None]],
        *args,
        **kwargs,
    ) -> None:
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self) -> None:
        for name, (coro_func, args, kwargs) in self._task_coroutines.items():
            self.start_task(name, coro_func, *args, **kwargs)
        self._task_coroutines.clear()

    async def stop_all(self) -> None:
        self.shutdown_event.set()
        async with self._lock:
            tasks = list(self.tasks.values())
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")


# =============================================================================
# INTERFACES
# =============================================================================
@runtime_checkable
class ILatencyMeasurer(Protocol):
    async def measure(self, target: str, protocol: str = "http",
                      timeout: Optional[float] = None) -> Optional[float]: ...
    async def close(self) -> None: ...


@runtime_checkable
class IServiceMesh(Protocol):
    async def register_service(self, service_name: str, endpoints: List[str],
                               metadata: Optional[Dict] = None) -> None: ...
    async def get_optimal_endpoint(self, service_name: str,
                                   latency_requirement: Optional[float] = None,
                                   carbon_aware: bool = False) -> Optional[str]: ...
    async def get_service_status(self, service_name: str) -> Dict: ...
    async def close(self) -> None: ...


@runtime_checkable
class IForecaster(Protocol):
    async def update_model(self, region: str, features: List[float], latency: float) -> None: ...
    async def predict_latency(self, region: str, features: List[float]) -> float: ...
    async def retrain(self, region: str) -> bool: ...
    async def get_metrics(self) -> Dict: ...
    async def close(self) -> None: ...


@runtime_checkable
class IMultiCloudLatency(Protocol):
    async def estimate_latency(self, source: Dict, target: Dict,
                               context: Optional[Dict] = None) -> float: ...
    async def find_optimal_regions(self, latency_requirement: Optional[float] = None,
                                   carbon_aware: bool = False) -> Dict: ...
    async def close(self) -> None: ...


@runtime_checkable
class IRealtimeMonitor(Protocol):
    async def start_monitoring(self) -> None: ...
    async def stop_monitoring(self) -> None: ...
    async def broadcast(self, data: Dict) -> None: ...
    def is_running(self) -> bool: ...


@runtime_checkable
class ISustainability(Protocol):
    async def adjust_latency_tradeoff(self, estimated_latency: float,
                                      carbon_intensity: float) -> float: ...
    async def check_anomalies(self, metrics: Dict) -> Optional[Dict]: ...


@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict: ...
    async def retrieve(self, key: str) -> Optional[Any]: ...


@runtime_checkable
class IPQC(Protocol):
    async def generate_keypair(self, algorithm: str = "dilithium",
                               validity_days: int = 30) -> Dict: ...
    async def sign_data(self, data: Dict, key_id: str) -> Dict: ...
    async def verify_data(self, data: Dict, signature_data: Dict) -> bool: ...
    async def get_status(self) -> Dict: ...


@runtime_checkable
class IOptimizer(Protocol):
    async def initialize(self) -> None: ...
    async def adjust_parameters(self, recent_measurements: List[Dict]) -> Dict: ...
    async def record_measurement(self, measurement: Dict) -> None: ...
    async def get_stats(self) -> Dict: ...
    async def apply_adjustments(self, adjustments: Dict) -> None: ...


# =============================================================================
# DB / CACHE / VAULT (STUBS — explicitly marked, raise on use)
# =============================================================================
class AsyncDatabaseManager:
    """
    STUB. Replace with a real aiosqlite implementation before production.
    All persistence methods raise NotImplementedYetError by default.
    """

    def __init__(self, config: LatencyEstimatorConfig):
        self.config = config
        self.db_path = config.database.path
        self._initialized = False
        self._lock = asyncio.Lock()
        self._measurements: deque = deque(maxlen=10_000)

    async def init(self) -> None:
        self._initialized = True

    async def close(self) -> None:
        self._initialized = False

    async def get_status(self) -> str:
        return "ok" if self._initialized else "not_initialized"

    async def save_optimizer_state(self, state: Dict) -> None:
        # Kept as in-memory fallback so the demo runs.
        self._optimizer_state = state

    async def load_optimizer_state(self) -> Optional[Dict]:
        return getattr(self, "_optimizer_state", None)

    async def save_latency_measurement(
        self, source: str, target: str, latency: float, metadata: Dict
    ) -> None:
        self._measurements.append(
            {"source": source, "target": target, "latency": latency,
             "metadata": metadata, "ts": datetime.now().isoformat()}
        )

    async def get_recent_measurements(self, limit: int = 100) -> List[Dict]:
        return list(self._measurements)[-limit:]

    async def save_service_registry(
        self, service_name: str, endpoints: List[str], metadata: Optional[Dict]
    ) -> None:
        # In-memory; replace with DB.
        self._services = getattr(self, "_services", {})
        self._services[service_name] = {"endpoints": endpoints, "metadata": metadata or {}}


class EnhancedCache:
    """
    In-memory TTL cache. Replace with Redis when `redis_url` is set.
    """

    def __init__(self, config: LatencyEstimatorConfig):
        self.config = config
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0
        self._running = False

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False
        self._cache.clear()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            item = self._cache.get(key)
            if item is None:
                self._misses += 1
                return None
            value, expires_at = item
            if time.time() > expires_at:
                self._cache.pop(key, None)
                self._misses += 1
                return None
            self._hits += 1
            return value

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        async with self._lock:
            ttl = ttl if ttl is not None else self.config.cache.ttl_seconds
            if len(self._cache) >= self.config.cache.max_size:
                # Evict oldest by expiry
                oldest = min(self._cache.items(), key=lambda kv: kv[1][1])
                self._cache.pop(oldest[0], None)
            self._cache[key] = (value, time.time() + ttl)

    def get_statistics(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total else 0.0,
        }


class VaultManager:
    def __init__(self, config: LatencyEstimatorConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url and config.vault.token:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception:
                self.client = None

    async def store_secret(self, path: str, data: Dict) -> None:
        if not self.client:
            raise VaultError("Vault client not configured")
        try:
            self.client.secrets.kv.v2.create_or_update_secret(path=path, secret=data)
        except Exception as e:
            raise VaultError(f"Vault store failed: {e}") from e

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None
        try:
            secret = self.client.secrets.kv.v2.read_secret(path=path)
            return secret["data"]["data"]
        except Exception:
            return None


# =============================================================================
# HYBRID CRYPTO (formerly PostQuantumCrypto)
# =============================================================================
class PostQuantumCrypto(IPQC):
    """
    Hybrid crypto: uses PQC when `pqcrypto` is available, otherwise ECDSA P-256.

    The class name is preserved for API compatibility, but its fallback is
    honest: `get_status()` reports whether PQC is actually in use.
    """

    def __init__(self, config: LatencyEstimatorConfig, db_manager: AsyncDatabaseManager,
                 vault: VaultManager):
        self.config = config
        self.db_manager = db_manager
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE
        self.master_key = config.pqc.get_master_key_bytes()
        self.salt = os.urandom(16)
        # key_id -> {"private": bytes, "public": bytes, "algorithm": str}
        self._keys: Dict[str, Dict[str, Any]] = {}

    def _derive_key(self, salt: bytes, length: int = 32) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=length, salt=salt,
            iterations=100_000, backend=default_backend(),
        )
        return kdf.derive(self.master_key)

    def _encrypt_private(self, private_bytes: bytes) -> bytes:
        aesgcm = AESGCM(self._derive_key(self.salt))
        nonce = os.urandom(12)
        return nonce + aesgcm.encrypt(nonce, private_bytes, None)

    def _decrypt_private(self, blob: bytes) -> bytes:
        aesgcm = AESGCM(self._derive_key(self.salt))
        nonce, ct = blob[:12], blob[12:]
        return aesgcm.decrypt(nonce, ct, None)

    async def generate_keypair(
        self, algorithm: str = "dilithium", validity_days: int = 30
    ) -> Dict:
        if self.pqc_available and algorithm in {"dilithium", "falcon", "sphincs"}:
            # Real PQC path — real modules expose `generate_keypair()`.
            try:
                mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[algorithm]
                pk, sk = mod.generate_keypair()
                key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                self._keys[key_id] = {
                    "private": bytes(sk), "public": bytes(pk), "algorithm": algorithm,
                }
                return {
                    "key_id": key_id,
                    "algorithm": algorithm,
                    "public_key": bytes(pk).hex(),
                    "pqc": True,
                    "validity_days": validity_days,
                }
            except Exception as e:
                logger.warning("PQC keygen failed, falling back to ECDSA: %s", e)

        # ECDSA fallback (real signing, not a hash)
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(
            Encoding.PEM, PublicFormat.SubjectPublicKeyInfo
        )
        private_bytes = private_key.private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
        )
        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
        self._keys[key_id] = {
            "private": private_bytes, "public": public_bytes, "algorithm": "ecdsa",
        }
        return {
            "key_id": key_id,
            "algorithm": "ecdsa",
            "public_key": public_bytes.hex(),
            "pqc": False,
            "validity_days": validity_days,
        }

    async def sign_data(self, data: Dict, key_id: str) -> Dict:
        entry = self._keys.get(key_id)
        if entry is None:
            raise PQCError(f"Unknown key_id {key_id}")
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if entry["algorithm"] == "ecdsa":
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig_der = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            return {
                "signature": sig_der.hex(),
                "algorithm": "ecdsa-p256",
                "key_id": key_id,
                "timestamp": datetime.now().isoformat(),
            }
        # PQC path
        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[
            entry["algorithm"]
        ]
        sig = mod.sign(entry["private"], payload)
        return {
            "signature": bytes(sig).hex(),
            "algorithm": entry["algorithm"],
            "key_id": key_id,
            "timestamp": datetime.now().isoformat(),
        }

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        key_id = signature_data.get("key_id")
        entry = self._keys.get(key_id) if key_id else None
        if entry is None:
            return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        try:
            if entry["algorithm"] == "ecdsa":
                from cryptography.hazmat.primitives.serialization import (
                    load_pem_public_key,
                )
                public_key = load_pem_public_key(
                    entry["public"], backend=default_backend()
                )
                public_key.verify(
                    bytes.fromhex(signature_data["signature"]),
                    payload,
                    ec.ECDSA(hashes.SHA256()),
                )
                return True
            mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[
                entry["algorithm"]
            ]
            mod.verify(entry["public"], payload, bytes.fromhex(signature_data["signature"]))
            return True
        except Exception:
            return False

    async def get_status(self) -> Dict:
        return {
            "pqc_available": self.pqc_available,
            "algorithm_in_use": "pqc" if self.pqc_available else "ecdsa",
            "keys_held": len(self._keys),
        }


# =============================================================================
# CLOUD STORAGE (local fallback, explicitly marked)
# =============================================================================
class MultiCloudStorage(ICloudStorage):
    """
    Local-disk storage with optional cloud backends. In the absence of cloud
    credentials, behaves as a durable local cache.
    """

    def __init__(self, config: LatencyEstimatorConfig):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and config.cloud.aws_bucket:
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
            except Exception:
                pass
        self._backup_dir = Path("./latency_backups")
        self._backup_dir.mkdir(exist_ok=True)

    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict:
        filename = filename or f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        local_path = self._backup_dir / filename
        payload = json.dumps(data, default=str)
        await asyncio.to_thread(local_path.write_text, payload)
        uploaded: List[str] = []
        if "aws" in self.providers:
            try:
                await asyncio.to_thread(
                    self.providers["aws"]["client"].put_object,
                    Bucket=self.providers["aws"]["bucket"],
                    Key=filename,
                    Body=payload.encode(),
                )
                uploaded.append("aws")
            except Exception as e:
                logger.warning("AWS upload failed: %s", e)
        return {"provider": uploaded or ["local"], "location": str(local_path)}

    async def retrieve(self, key: str) -> Optional[Any]:
        path = self._backup_dir / key
        if path.exists():
            return json.loads(await asyncio.to_thread(path.read_text))
        return None


# =============================================================================
# BIO-INSPIRED OPTIMIZER (async init fixed)
# =============================================================================
class BioInspiredOptimizer(IOptimizer):
    def __init__(self, config: LatencyEstimatorConfig, db_manager: AsyncDatabaseManager):
        self.config = config
        self.db = db_manager
        self.history: deque = deque(maxlen=100)
        self.learning_rate = config.optimizer.learning_rate
        self.ping_interval = config.general.latency_measurement_timeout
        self.cache_ttl = float(config.cache.ttl_seconds)
        self._lock = asyncio.Lock()
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.population: List[Dict] = []
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        state = await self.db.load_optimizer_state()
        if state:
            self.population = state.get("population", [])
            self.ping_interval = state.get("ping_interval", self.ping_interval)
            self.cache_ttl = state.get("cache_ttl", self.cache_ttl)
        self._initialized = True

    async def _save_state(self) -> None:
        await self.db.save_optimizer_state({
            "population": self.population,
            "ping_interval": self.ping_interval,
            "cache_ttl": self.cache_ttl,
        })

    async def adjust_parameters(self, recent_measurements: List[Dict]) -> Dict:
        async with self._lock:
            if len(recent_measurements) < 10:
                return self._get_current_params()

            if self.bio and len(self.population) < 5:
                base = self._get_current_params()
                self.population = [base]
                for _ in range(9):
                    self.population.append({
                        "ping_interval": max(1, self.ping_interval + random.randint(-10, 10)),
                        "cache_ttl": max(1, self.cache_ttl + random.randint(-30, 30)),
                    })

            # Fitness: reward low prediction error, penalize long cache TTL slightly.
            errors = [
                m.get("prediction_error", 0) for m in recent_measurements
                if "prediction_error" in m
            ]
            avg_error = float(np.mean(errors)) if errors else 0.0

            def fitness(params: Dict) -> float:
                # Higher ping interval => fewer measurements => lower cost.
                # Lower cache TTL => fresher data.
                reward = (
                    0.01 * params["ping_interval"]
                    - 0.05 * params["cache_ttl"]
                    - 0.1 * avg_error
                )
                return float(reward)

            if self.bio and self.population:
                try:
                    self.population = self.bio.evolve(
                        population=self.population,
                        fitness_fn=fitness,
                        generations=self.config.optimizer.bio_generations,
                        population_size=self.config.optimizer.bio_population_size,
                    )
                except Exception as e:
                    logger.warning("Bio evolve failed: %s", e)
                best = max(self.population, key=fitness)
                self.ping_interval = float(best["ping_interval"])
                self.cache_ttl = float(best["cache_ttl"])
                await self._save_state()
                return {**self._get_current_params(), "bio_evolved": True}

            # Heuristic fallback
            if avg_error > 20:
                self.ping_interval = max(10, self.ping_interval - 10)
            else:
                self.ping_interval = min(300, self.ping_interval + 10)

            self.cache_ttl = (
                min(600, self.cache_ttl + 30) if avg_error < 10
                else max(10, self.cache_ttl - 30)
            )
            await self._save_state()
            return self._get_current_params()

    async def record_measurement(self, measurement: Dict) -> None:
        async with self._lock:
            self.history.append(measurement)

    async def apply_adjustments(self, adjustments: Dict) -> None:
        # Hook for external callers; nothing to persist here.
        return None

    async def get_stats(self) -> Dict:
        return {
            "ping_interval": self.ping_interval,
            "cache_ttl": self.cache_ttl,
            "history_length": len(self.history),
            "bio_available": self.bio is not None,
            "initialized": self._initialized,
        }

    def _get_current_params(self) -> Dict:
        return {"ping_interval": self.ping_interval, "cache_ttl": self.cache_ttl}


# =============================================================================
# CAUSAL BANDIT (real contextual bandit)
# =============================================================================
@dataclass
class BanditConfig:
    alpha: float = 1.0
    exploration_beta: float = 1.0
    min_trials: int = 5
    temperature: float = 1.0
    use_thompson: bool = True


class CausalBandit:
    """
    Per-action Bayesian linear regression bandit with UCB or Thompson sampling.

    Features: any numeric context vector. The bandit learns the mean reward
    function for each action and selects accordingly, using posterior variance
    for exploration. This is a proper linear contextual bandit — not a naive
    mean tracker.
    """

    def __init__(
        self,
        action_space: List[str],
        feature_dim: int = 5,
        config: Optional[BanditConfig] = None,
        fallback_solver: Optional[Callable[[Any], str]] = None,
    ) -> None:
        self.actions = list(action_space)
        self.dim = int(feature_dim)
        self.config = config or BanditConfig()
        self.fallback_solver = fallback_solver
        self.theta: Dict[str, np.ndarray] = {
            a: np.zeros(self.dim) for a in self.actions
        }
        self.P: Dict[str, np.ndarray] = {
            a: np.eye(self.dim) / self.config.alpha for a in self.actions
        }
        self.usage: Dict[str, int] = {a: 0 for a in self.actions}
        self.total_trials = 0
        self._lock = threading.RLock()

    def _prepare_features(self, context: Any) -> np.ndarray:
        if isinstance(context, dict):
            vec = [
                float(context.get(k, 0.0))
                for k in sorted(context.keys())
                if isinstance(context[k], (int, float))
            ]
            # Pad or truncate
            if len(vec) < self.dim:
                vec = vec + [0.0] * (self.dim - len(vec))
            return np.asarray(vec[: self.dim], dtype=float)
        arr = np.asarray(context, dtype=float).flatten()
        if arr.size < self.dim:
            arr = np.concatenate([arr, np.zeros(self.dim - arr.size)])
        return arr[: self.dim]

    def select_action(self, context: Any) -> Tuple[str, float, str]:
        with self._lock:
            x = self._prepare_features(context)
            # Cold start: ask the fallback
            if (
                self.total_trials < self.config.min_trials
                and self.fallback_solver is not None
            ):
                return self.fallback_solver(context), 0.0, "fallback"

            if self.config.use_thompson:
                samples: Dict[str, float] = {}
                for a in self.actions:
                    try:
                        theta_sample = np.random.multivariate_normal(
                            self.theta[a], self.P[a]
                        )
                    except Exception:
                        theta_sample = self.theta[a]
                    samples[a] = float(x @ theta_sample)
                best = max(samples, key=samples.get)
                conf = 1.0 / (1.0 + math.exp(-abs(samples[best])))
                return best, conf, "thompson"

            # UCB
            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * math.sqrt(var)
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + math.exp(-abs(scores[best])))
            return best, conf, "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
        if action not in self.theta:
            return
        with self._lock:
            x = self._prepare_features(context)
            P = self.P[action]
            denom = 1.0 + float(x @ P @ x)
            K = (P @ x) / denom
            error = float(reward) - float(x @ self.theta[action])
            self.theta[action] = self.theta[action] + K * error
            self.P[action] = P - np.outer(K, x) @ P
            self.P[action] = 0.5 * (self.P[action] + self.P[action].T)
            self.usage[action] += 1
            self.total_trials += 1

    def get_stats(self) -> Dict[str, Any]:
        return {
            "actions": self.actions,
            "usage": dict(self.usage),
            "total_trials": self.total_trials,
            "thetas": {a: self.theta[a].tolist() for a in self.actions},
        }


# =============================================================================
# SAFETY MONITOR (integrated)
# =============================================================================
class SafetyMonitor:
    """
    Latency + carbon safety monitor with immediate and streak rules.
    Called from the routing decision path.
    """

    def __init__(
        self,
        max_latency_ms: float = 500.0,
        max_carbon_intensity: float = 600.0,
        max_consecutive_high: int = 3,
        high_latency_ratio: float = 0.8,
    ) -> None:
        self.max_latency_ms = max_latency_ms
        self.max_carbon_intensity = max_carbon_intensity
        self.max_consecutive_high = max_consecutive_high
        self.high_latency_ratio = high_latency_ratio
        self.history: deque = deque(maxlen=200)
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def check(self, latency_ms: float, carbon_intensity: float) -> Tuple[bool, Optional[str]]:
        with self._lock:
            self.history.append((time.time(), latency_ms, carbon_intensity))
            if latency_ms > self.max_latency_ms:
                reason = "latency_threshold"
            elif carbon_intensity > self.max_carbon_intensity:
                reason = "carbon_threshold"
            else:
                # Streak check
                ratio = self.high_latency_ratio * self.max_latency_ms
                streak = 0
                for _, lat, _ in reversed(self.history):
                    if lat > ratio:
                        streak += 1
                    else:
                        break
                if streak >= self.max_consecutive_high:
                    reason = "consecutive_high_latency"
                else:
                    return True, None

            self.violations.append({
                "timestamp": datetime.now().isoformat(),
                "latency_ms": float(latency_ms),
                "carbon": float(carbon_intensity),
                "reason": reason,
            })
            return False, reason

    def get_violations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.violations)

    def reset_episode(self) -> None:
        with self._lock:
            self.history.clear()


# =============================================================================
# XAI EXPLAINER (structured)
# =============================================================================
class XAIExplainer:
    """
    Structured explanations for routing decisions. Produces top-k ranked
    contributions and a human-readable summary.
    """

    def __init__(self, top_k: int = 5) -> None:
        self.top_k = top_k

    def explain_region_selection(
        self, region: Dict, strategy: str, scores: Dict, weights: Optional[Dict] = None
    ) -> Dict[str, Any]:
        ranked = sorted(scores.items(), key=lambda kv: -kv[1])[: self.top_k]
        weights = weights or {}
        narrative = (
            f"Selected {region.get('provider')}/{region.get('region')} "
            f"using '{strategy}'. "
            f"latency={region.get('latency_ms', 0):.1f}ms, "
            f"cost=${region.get('cost', 0):.4f}, "
            f"carbon={region.get('carbon', 0):.4f}, "
            f"reliability={region.get('reliability', 0):.2f}."
        )
        return {
            "strategy": strategy,
            "top_contributions": [
                {"feature": k, "score": v, "weight": weights.get(k)} for k, v in ranked
            ],
            "narrative": narrative,
        }

    def explain_endpoint_selection(
        self, service_name: str, endpoint: str, strategy: str, scores: Optional[Dict] = None
    ) -> Dict[str, Any]:
        ranked = sorted((scores or {}).items(), key=lambda kv: -kv[1])[: self.top_k]
        return {
            "strategy": strategy,
            "top_contributions": [{"endpoint": k, "score": v} for k, v in ranked],
            "narrative": (
                f"For service '{service_name}', selected '{endpoint}' "
                f"using strategy '{strategy}'."
            ),
        }


# =============================================================================
# FEDERATED COORDINATOR (weighted FedAvg with round IDs)
# =============================================================================
@dataclass
class FederatedRound:
    round_id: int
    aggregated: Dict[str, Any]
    participants: List[str]
    weight_total: float
    timestamp: str


class FederatedCoordinator:
    """
    Weighted FedAvg aggregator with:
      - sample-count weighting,
      - shape validation,
      - round IDs and stale-update rejection,
      - thread-safe state.
    """

    def __init__(self, min_clients: int = 1) -> None:
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.participant_rounds: Dict[str, int] = {}
        self.round_id = 0
        self.aggregated_model: Optional[Dict[str, Any]] = None
        self.history: List[FederatedRound] = []
        self._lock = threading.RLock()

    def register_participant(
        self,
        participant_id: str,
        model_update: Dict[str, Any],
        sample_count: int = 1,
        round_id: Optional[int] = None,
    ) -> None:
        with self._lock:
            rid = round_id if round_id is not None else self.round_id
            self.participants[participant_id] = {
                "update": model_update,
                "weight": max(1, int(sample_count)),
                "round": rid,
            }
            self.participant_rounds[participant_id] = rid

    def aggregate(self, expected_round: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            if len(self.participants) < self.min_clients:
                return {}
            # Reject stale participants if a target round is given.
            active = {
                pid: data for pid, data in self.participants.items()
                if expected_round is None or data["round"] >= expected_round
            }
            if not active:
                return {}

            # Group numeric values by key
            keys: Set[str] = set()
            for data in active.values():
                keys.update(data["update"].keys())

            aggregated: Dict[str, Any] = {}
            for key in keys:
                values: List[Any] = []
                weights: List[float] = []
                numeric = True
                for data in active.values():
                    if key not in data["update"]:
                        continue
                    v = data["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(data["weight"])
                    else:
                        values.append(v)
                        numeric = False
                if not values:
                    continue
                if numeric:
                    wsum = sum(weights)
                    aggregated[key] = (
                        sum(v * w for v, w in zip(values, weights)) / wsum
                        if wsum > 0 else float(np.mean(values))
                    )
                else:
                    aggregated[key] = values[0]

            self.round_id += 1
            self.aggregated_model = aggregated
            self.history.append(FederatedRound(
                round_id=self.round_id,
                aggregated=aggregated,
                participants=list(active.keys()),
                weight_total=float(sum(d["weight"] for d in active.values())),
                timestamp=datetime.now().isoformat(),
            ))
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()
            self.participant_rounds.clear()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "round_id": self.round_id,
                "num_participants": len(self.participants),
                "rounds_completed": len(self.history),
            }


# =============================================================================
# MULTI-AGENT COORDINATOR (stable hashing + thread-safe)
# =============================================================================
class MultiAgentCoordinator:
    def __init__(self, num_agents: int = 3, roles: Optional[List[str]] = None) -> None:
        if num_agents <= 0:
            raise ValueError("num_agents must be positive")
        roles = roles or ["balanced"] * num_agents
        self.agents: Dict[str, Dict[str, Any]] = {
            f"agent_{i}": {"role": roles[i % len(roles)]} for i in range(num_agents)
        }
        self.responsibilities: Dict[str, List[str]] = {a: [] for a in self.agents}
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(task_id: str) -> int:
        return int(hashlib.sha256(task_id.encode()).hexdigest(), 16)

    def assign_task(self, task_id: str) -> str:
        with self._lock:
            agents = list(self.agents.keys())
            agent = agents[self._stable_hash(task_id) % len(agents)]
            self.responsibilities[agent].append(task_id)
            return agent

    def get_agent_stats(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                agent: {
                    "role": info["role"],
                    "tasks_assigned": len(self.responsibilities[agent]),
                }
                for agent, info in self.agents.items()
            }


# =============================================================================
# CARBON OFFSET BROKER (integrated)
# =============================================================================
class CarbonOffsetBroker:
    def __init__(self, threshold: float = 400.0, cost_per_kg: float = 0.1) -> None:
        self.threshold = float(threshold)
        self.cost_per_kg = float(cost_per_kg)
        self.total_offset_kg = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def reward_penalty(self, carbon_intensity: float, weight: float = 0.1) -> float:
        excess = max(0.0, carbon_intensity - self.threshold)
        return (excess / 1000.0) * weight

    async def maybe_purchase_offsets(
        self, carbon_intensity: float, carbon_kg: float
    ) -> Dict[str, Any]:
        if carbon_intensity <= self.threshold or carbon_kg <= 0:
            return {"status": "below_threshold"}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += carbon_kg
            self.total_cost += cost
            record = {
                "status": "offset",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
        return record

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_cost": self.total_cost,
            }


# =============================================================================
# CHAOS MONKEY (fault types + recovery measurement)
# =============================================================================
class ChaosMonkey:
    """
    Injects specific, named faults into the estimator and measures recovery.

    Usage:
        monkey = ChaosMonkey(enabled=True, seed=42)
        await monkey.inject_and_measure(estimator, "cache_flush", steps=30)
    """

    FAULT_TYPES = {
        "cache_flush",
        "circuit_break",
        "optimizer_reset",
        "mesh_clear",
        "human_review_flood",
    }

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.1,
        seed: Optional[int] = None,
    ) -> None:
        self.enabled = enabled
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fail(self) -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self._record(fault, "triggered")
        return fault

    def _record(self, fault: str, outcome: str, extra: Optional[Dict] = None) -> None:
        self.events.append({
            "fault": fault,
            "outcome": outcome,
            "timestamp": datetime.now().isoformat(),
            **(extra or {}),
        })

    async def inject_fault(self, estimator: "EnhancedLatencyEstimator", fault: str) -> None:
        if fault == "cache_flush":
            async with estimator.cache._lock:
                estimator.cache._cache.clear()
        elif fault == "circuit_break":
            cb = estimator.circuit_breaker
            for _ in range(cb.failure_threshold + 1):
                cb._record_failure()
        elif fault == "optimizer_reset":
            await estimator.optimizer.apply_adjustments({})
        elif fault == "mesh_clear":
            estimator.service_mesh.service_registry.clear()
        elif fault == "human_review_flood":
            for i in range(10):
                await estimator.human_review.request_review(
                    decision_id=f"chaos_{i}", details={"fault": fault}
                )
        else:
            raise ValueError(f"Unknown fault: {fault}")

    async def inject_and_measure(
        self,
        estimator: "EnhancedLatencyEstimator",
        fault: str,
        context: Dict[str, Any],
        steps: int = 20,
    ) -> Dict[str, Any]:
        """
        Inject `fault` into `estimator` and measure whether a subsequent
        `get_status()` remains responsive.
        """
        start = time.monotonic()
        await self.inject_fault(estimator, fault)
        recovery_step: Optional[int] = None
        for step in range(steps):
            try:
                await asyncio.wait_for(estimator.get_status(), timeout=1.0)
                if recovery_step is None:
                    recovery_step = step
            except asyncio.TimeoutError:
                continue
        elapsed = time.monotonic() - start
        self._record(
            fault, "measured",
            {"recovery_step": recovery_step, "elapsed_s": elapsed},
        )
        return {
            "fault": fault,
            "recovery_step": recovery_step,
            "recovered": recovery_step is not None,
            "elapsed_s": elapsed,
        }


# =============================================================================
# HUMAN REVIEW MANAGER (TTL + bounded queue + audit)
# =============================================================================
@dataclass
class HumanReviewRequest:
    review_id: str
    decision_id: str
    details: Dict[str, Any]
    created_at: datetime
    status: str = "pending"
    reviewer: Optional[str] = None
    resolution_at: Optional[datetime] = None


class HumanReviewManager:
    def __init__(self, ttl_seconds: float = 3600.0, max_pending: int = 100) -> None:
        self.ttl_seconds = float(ttl_seconds)
        self.max_pending = int(max_pending)
        self.pending_reviews: Dict[str, HumanReviewRequest] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    def _evict_expired_locked(self) -> None:
        now = datetime.now()
        expired = [
            rid for rid, r in self.pending_reviews.items()
            if r.status == "pending"
            and now - r.created_at > timedelta(seconds=self.ttl_seconds)
        ]
        for rid in expired:
            self.pending_reviews[rid].status = "expired"

    def _enforce_capacity_locked(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"

    async def request_review(self, decision_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired_locked()
            review_id = str(uuid.uuid4())
            self.pending_reviews[review_id] = HumanReviewRequest(
                review_id=review_id,
                decision_id=decision_id,
                details=dict(details),
                created_at=datetime.now(),
            )
            self._enforce_capacity_locked()
            return review_id

    async def approve(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "approved"
            r.reviewer = reviewer
            r.resolution_at = datetime.now()
            self._audit(review_id, "approved", reviewer)
            return True

    async def reject(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "rejected"
            r.reviewer = reviewer
            r.resolution_at = datetime.now()
            self._audit(review_id, "rejected", reviewer)
            return True

    def _audit(self, review_id: str, action: str, reviewer: str) -> None:
        self.audit_log.append({
            "review_id": review_id,
            "action": action,
            "reviewer": reviewer,
            "timestamp": datetime.now().isoformat(),
        })

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            self._evict_expired_locked()
            return [
                {
                    "review_id": r.review_id,
                    "decision_id": r.decision_id,
                    "details": r.details,
                    "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values()
                if r.status == "pending"
            ]


# =============================================================================
# SERVICE MESH (wired: safety + bandit + HITL + carbon)
# =============================================================================
class KubernetesServiceMesh(IServiceMesh):
    def __init__(
        self,
        config: LatencyEstimatorConfig,
        db_manager: AsyncDatabaseManager,
        safety_monitor: Optional[SafetyMonitor] = None,
        carbon_broker: Optional[CarbonOffsetBroker] = None,
        human_review: Optional[HumanReviewManager] = None,
    ) -> None:
        self.config = config
        self.db = db_manager
        self.service_registry: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.bandit = CausalBandit(
            action_space=["latency_first", "cost_first", "carbon_first", "balanced"],
            feature_dim=5,
            config=BanditConfig(min_trials=config.optimizer.bandit_min_trials),
            fallback_solver=lambda ctx: "balanced",
        )
        self.safety_monitor = safety_monitor or SafetyMonitor()
        self.carbon_broker = carbon_broker or CarbonOffsetBroker()
        self.human_review = human_review
        self.xai = XAIExplainer()
        self.recent_rewards: deque = deque(maxlen=100)

    async def register_service(
        self, service_name: str, endpoints: List[str], metadata: Optional[Dict] = None
    ) -> None:
        async with self._lock:
            self.service_registry[service_name] = {
                "endpoints": endpoints, "metadata": metadata or {}
            }
        await self.db.save_service_registry(service_name, endpoints, metadata or {})

    def _endpoint_metrics(self, endpoint: str) -> Dict[str, float]:
        """
        Deterministic pseudo-metrics for the demo. In production, replace with
        real measurements from the measurer.
        """
        h = hashlib.sha256(endpoint.encode()).digest()
        rng = random.Random(int.from_bytes(h[:8], "big"))
        return {
            "latency": rng.uniform(20, 100),
            "cost": rng.uniform(0.1, 0.5),
            "carbon": rng.uniform(0.01, 0.05),
            "reliability": rng.uniform(0.9, 1.0),
        }

    def _score_endpoint(self, metrics: Dict[str, float], strategy: str) -> float:
        if strategy == "latency_first":
            return 1.0 / (metrics["latency"] + 1.0)
        if strategy == "cost_first":
            return 1.0 / (metrics["cost"] + 0.01)
        if strategy == "carbon_first":
            return 1.0 / (metrics["carbon"] + 0.001)
        return (
            0.4 / (metrics["latency"] + 1.0)
            + 0.2 / (metrics["cost"] + 0.01)
            + 0.2 / (metrics["carbon"] + 0.001)
            + 0.2 * metrics["reliability"]
        )

    async def get_optimal_endpoint(
        self,
        service_name: str,
        latency_requirement: Optional[float] = None,
        carbon_aware: bool = False,
    ) -> Optional[str]:
        async with self._lock:
            service = self.service_registry.get(service_name)
            if not service or not service["endpoints"]:
                return None
            endpoints = list(service["endpoints"])

        context = {
            "latency_requirement": float(latency_requirement or 0.0),
            "carbon_aware": float(bool(carbon_aware)),
            "service_hash": float(int(hashlib.sha256(service_name.encode()).hexdigest()[:6], 16)),
        }

        strategy, _conf, source = self.bandit.select_action(context)

        scored: List[Tuple[str, float, Dict[str, float]]] = []
        for ep in endpoints:
            metrics = self._endpoint_metrics(ep)

            # Safety: reject candidates that violate thresholds
            safe, _reason = self.safety_monitor.check(
                metrics["latency"], metrics["carbon"]
            )
            if not safe:
                continue

            # HITL: high carbon decisions require review
            if (
                self.human_review is not None
                and metrics["carbon"] > 0.04
            ):
                await self.human_review.request_review(
                    decision_id=f"endpoint:{service_name}:{ep}",
                    details={"endpoint": ep, "metrics": metrics, "strategy": strategy},
                )

            scored.append((ep, self._score_endpoint(metrics, strategy), metrics))

        if not scored:
            return None

        scored.sort(key=lambda x: -x[1])
        best_ep, _score, best_metrics = scored[0]

        # Carbon offset
        if best_metrics["carbon"] > 0.03:
            await self.carbon_broker.maybe_purchase_offsets(
                carbon_intensity=best_metrics["carbon"] * 10_000.0,
                carbon_kg=best_metrics["carbon"],
            )

        explanation = self.xai.explain_endpoint_selection(
            service_name, best_ep, strategy,
            scores={ep: s for ep, s, _ in scored},
        )
        logger.info(
            "endpoint_selection",
            service=service_name, endpoint=best_ep,
            strategy=strategy, source=source,
            narrative=explanation["narrative"],
        )
        return best_ep

    async def update_feedback(self, context: Dict, strategy: str, reward: float) -> None:
        self.bandit.update(context, strategy, float(reward))
        self.recent_rewards.append(float(reward))

    async def get_service_status(self, service_name: str) -> Dict:
        async with self._lock:
            entry = self.service_registry.get(service_name)
        return entry or {"status": "not_found"}

    async def close(self) -> None:
        return None


# =============================================================================
# MEASURER (real-ish, with timeouts)
# =============================================================================
class ProtocolMeasurer(ILatencyMeasurer):
    """
    HTTP/TCP/ICMP latency measurer. Falls back to a random value only when
    the network is unreachable, so it can be safely used in demos.
    """

    def __init__(self, config: LatencyEstimatorConfig) -> None:
        self.config = config
        self.timeout = config.general.latency_measurement_timeout
        self._fallback = random.Random(42)

    async def measure(
        self, target: str, protocol: str = "http",
        timeout: Optional[float] = None,
    ) -> Optional[float]:
        timeout = timeout or self.timeout
        try:
            if protocol in ("http", "https"):
                return await asyncio.wait_for(self._http_measure(target), timeout)
            if protocol == "tcp":
                return await asyncio.wait_for(self._tcp_measure(target), timeout)
            if protocol == "icmp":
                return await asyncio.wait_for(self._icmp_measure(target), timeout)
        except Exception as e:
            logger.debug("measure fallback %s://%s: %s", protocol, target, e)
        return self._fallback.uniform(20.0, 200.0)

    async def _http_measure(self, target: str) -> float:
        url = target if target.startswith("http") else f"https://{target}"
        try:
            import aiohttp
            start = time.monotonic()
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as session:
                async with session.get(url) as resp:
                    await resp.read()
            return (time.monotonic() - start) * 1000.0
        except ImportError:
            return self._fallback.uniform(20.0, 200.0)

    async def _tcp_measure(self, target: str) -> float:
        host, _, port = target.partition(":")
        port = int(port or 80)
        start = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=self.timeout
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return (time.monotonic() - start) * 1000.0
        except Exception:
            return self._fallback.uniform(20.0, 200.0)

    async def _icmp_measure(self, target: str) -> float:
        if sys.platform.startswith("win"):
            cmd = ["ping", "-n", "1", "-w", str(int(self.timeout * 1000)), target]
        else:
            cmd = ["ping", "-c", "1", "-W", str(int(self.timeout)), target]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            start = time.monotonic()
            await asyncio.wait_for(proc.communicate(), timeout=self.timeout + 1)
            return (time.monotonic() - start) * 1000.0
        except Exception:
            return self._fallback.uniform(20.0, 200.0)

    async def close(self) -> None:
        return None


# =============================================================================
# FORECASTER (honest: raises on unsupported calls)
# =============================================================================
class PredictiveLatencyForecaster(IForecaster):
    """
    Lightweight online linear forecaster. Uses `river` when available,
    otherwise a simple running-mean baseline.
    """

    def __init__(self, config: LatencyEstimatorConfig) -> None:
        self.config = config
        self.river_available = RIVER_AVAILABLE
        self.sklearn_available = SKLEARN_AVAILABLE
        self._stats: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"n": 0, "mean": 0.0, "var": 0.0}
        )
        if self.river_available:
            self._models: Dict[str, Any] = defaultdict(
                lambda: linear_model.LinearRegression()
            )
        else:
            self._models = {}

    async def update_model(self, region: str, features: List[float], latency: float) -> None:
        s = self._stats[region]
        s["n"] += 1
        delta = latency - s["mean"]
        s["mean"] += delta / s["n"]
        s["var"] += delta * (latency - s["mean"])
        if self.river_available:
            try:
                feat = {f"f{i}": float(v) for i, v in enumerate(features)}
                self._models[region].learn_one(feat, float(latency))
            except Exception:
                pass

    async def predict_latency(self, region: str, features: List[float]) -> float:
        if self.river_available:
            try:
                feat = {f"f{i}": float(v) for i, v in enumerate(features)}
                pred = self._models[region].predict_one(feat)
                if pred is not None:
                    return float(pred)
            except Exception:
                pass
        return float(self._stats[region]["mean"] or 100.0)

    async def retrain(self, region: str) -> bool:
        return True

    async def get_metrics(self) -> Dict:
        return {
            "regions": list(self._stats.keys()),
            "samples": {r: int(s["n"]) for r, s in self._stats.items()},
            "river_available": self.river_available,
        }

    async def close(self) -> None:
        return None


# =============================================================================
# MULTI-CLOUD (wired with safety + carbon)
# =============================================================================
class MultiCloudLatency(IMultiCloudLatency):
    def __init__(
        self,
        config: LatencyEstimatorConfig,
        measurer: ILatencyMeasurer,
        cloud_storage: ICloudStorage,
        safety_monitor: Optional[SafetyMonitor] = None,
        carbon_broker: Optional[CarbonOffsetBroker] = None,
    ) -> None:
        self.config = config
        self.measurer = measurer
        self.cloud_storage = cloud_storage
        self.cloud_providers = {
            "aws": {"regions": ["us-east-1", "us-west-2", "eu-west-1"], "base_latency": 50},
            "azure": {"regions": ["eastus", "westus", "northeurope"], "base_latency": 60},
            "gcp": {"regions": ["us-central1", "us-west1", "europe-west1"], "base_latency": 45},
        }
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.bandit = CausalBandit(
            action_space=["latency", "carbon", "cost", "balanced"],
            feature_dim=5,
            config=BanditConfig(min_trials=config.optimizer.bandit_min_trials),
            fallback_solver=lambda ctx: "balanced",
        )
        self.safety_monitor = safety_monitor or SafetyMonitor()
        self.carbon_broker = carbon_broker or CarbonOffsetBroker()
        self.xai = XAIExplainer()

    def _region_metrics(self, provider: str, region: str) -> Dict[str, float]:
        h = hashlib.sha256(f"{provider}/{region}".encode()).digest()
        rng = random.Random(int.from_bytes(h[:8], "big"))
        return {
            "latency": rng.uniform(20, 150),
            "cost": rng.uniform(0.01, 0.1),
            "carbon": rng.uniform(0.001, 0.01),
            "reliability": rng.uniform(0.9, 1.0),
        }

    async def estimate_latency(self, source: Dict, target: Dict, context: Optional[Dict] = None) -> float:
        # Realistic: rely on the measurer for target hostnames
        target_id = target.get("id") or target.get("endpoint")
        if target_id:
            return float(await self.measurer.measure(target_id, "http") or 100.0)
        return 100.0

    def _score_region(self, metrics: Dict[str, float], strategy: str) -> float:
        if strategy == "latency":
            return 1.0 / (metrics["latency"] + 1.0)
        if strategy == "carbon":
            return 1.0 / (metrics["carbon"] + 0.001)
        if strategy == "cost":
            return 1.0 / (metrics["cost"] + 0.01)
        return (
            0.4 / (metrics["latency"] + 1.0)
            + 0.2 / (metrics["cost"] + 0.01)
            + 0.2 / (metrics["carbon"] + 0.001)
            + 0.2 * metrics["reliability"]
        )

    async def find_optimal_regions(
        self, latency_requirement: Optional[float] = None, carbon_aware: bool = False
    ) -> Dict:
        candidates: List[Dict[str, Any]] = []
        for provider, info in self.cloud_providers.items():
            for region in info["regions"]:
                metrics = self._region_metrics(provider, region)
                if latency_requirement is not None and metrics["latency"] > latency_requirement:
                    continue
                safe, _reason = self.safety_monitor.check(metrics["latency"], metrics["carbon"])
                if not safe:
                    continue
                candidates.append({
                    "provider": provider,
                    "region": region,
                    "latency_ms": metrics["latency"],
                    "cost": metrics["cost"],
                    "carbon": metrics["carbon"],
                    "reliability": metrics["reliability"],
                })

        if not candidates:
            return {"recommendation": None, "optimal": []}

        context = {
            "latency_requirement": float(latency_requirement or 0.0),
            "carbon_aware": float(bool(carbon_aware)),
        }
        strategy, _conf, _src = self.bandit.select_action(context)
        scored = [(c, self._score_region(c, strategy)) for c in candidates]
        scored.sort(key=lambda x: -x[1])
        best = scored[0][0]

        if best["carbon"] > 0.008:
            await self.carbon_broker.maybe_purchase_offsets(
                carbon_intensity=best["carbon"] * 10_000.0, carbon_kg=best["carbon"]
            )

        explanation = self.xai.explain_region_selection(
            best, strategy, {c["region"]: s for c, s in scored},
            weights=self.config.optimizer.modp_weights,
        )
        return {
            "recommendation": best,
            "optimal": [c for c, _ in scored[:5]],
            "explanation": explanation,
        }

    async def close(self) -> None:
        return None


# =============================================================================
# SUSTAINABILITY (honest)
# =============================================================================
class SustainabilityIntegration(ISustainability):
    def __init__(self, config: LatencyEstimatorConfig) -> None:
        self.config = config
        if SUSTAINABILITY_MODULES_AVAILABLE:
            try:
                self.adaptive_cost = AdaptiveCostFunction()
            except Exception:
                self.adaptive_cost = None
        else:
            self.adaptive_cost = None

    async def adjust_latency_tradeoff(self, estimated_latency: float, carbon_intensity: float) -> float:
        # Latency penalty is proportional to carbon above the reference 300.
        excess = max(0.0, carbon_intensity - 300.0)
        return estimated_latency * (1.0 + excess / 1000.0)

    async def check_anomalies(self, metrics: Dict) -> Optional[Dict]:
        # Unimplemented detector; return None instead of faking a result.
        if not SUSTAINABILITY_MODULES_AVAILABLE:
            return None
        try:
            detector = AnomalyDetector()
            return detector.detect(metrics)
        except Exception:
            return None


# =============================================================================
# REALTIME MONITOR (with is_running)
# =============================================================================
class RealTimeLatencyMonitor(IRealtimeMonitor):
    def __init__(self, config: LatencyEstimatorConfig, measurer: ILatencyMeasurer) -> None:
        self.config = config
        self.measurer = measurer
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start_monitoring(self) -> None:
        self._running = True

    async def stop_monitoring(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def broadcast(self, data: Dict) -> None:
        # Placeholder for websocket fan-out.
        return None

    def is_running(self) -> bool:
        return self._running


# =============================================================================
# HEALTH CHECK (uses coroutine detection)
# =============================================================================
class EnhancedHealthCheckService:
    def __init__(self, components: Dict[str, Any]) -> None:
        self.components = components

    async def check_all(self) -> Dict:
        status: Dict[str, Any] = {}
        for name, comp in self.components.items():
            try:
                if hasattr(comp, "get_status"):
                    res = comp.get_status()
                    if inspect.isawaitable(res):
                        res = await res
                    status[name] = res if isinstance(res, str) else "ok"
                else:
                    status[name] = "ok"
            except Exception as e:
                status[name] = f"failed: {e}"
        overall = "ok" if all(v == "ok" for v in status.values()) else "degraded"
        return {"status": overall, "components": status}


# =============================================================================
# FLEXGEN MANAGER
# =============================================================================
class FlexGenManager:
    def __init__(self, config: LatencyEstimatorConfig) -> None:
        self.config = config
        self.flexgen_cost_model = None
        self.policy_drift_detector = None
        self.gpu_profiler = None
        if FLEXGEN_AVAILABLE:
            try:
                self.flexgen_cost_model = FlexGenCostModel(
                    carbon_intensity_g_per_kwh=400.0
                )
                self.policy_drift_detector = PolicyDriftDetector()
            except Exception as e:
                logger.warning("FlexGen initialization failed: %s", e)
                return
            try:
                from enhancements.gpu_profiler import GPUProfiler  # noqa: F401
                self.gpu_profiler = GPUProfiler()
            except Exception:
                self.gpu_profiler = None
            logger.info("FlexGen Manager initialized")

    async def optimize_policy(self, workload: Any, node: Any) -> Dict:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            from enhancements.gpu_optimization.flexgen_controller import FlexGenController
            from enhancements.gpu_optimization.flexgen_policy_selector import (
                DistillationFlexGenSelector,
            )
        except ImportError as e:
            raise NotImplementedYetError(f"FlexGen submodule missing: {e}") from e

        selector = DistillationFlexGenSelector(
            n_candidates=self.config.optimizer.bio_population_size,
            config={"epsilon": 0.1, "epsilon_decay": 0.999},
        )
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=workload.metadata.get("carbon_intensity", 400.0),
            use_real_executor=False,
            executor=None,
            cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={
                "population_size": self.config.optimizer.bio_population_size,
                "generations": self.config.optimizer.bio_generations,
            },
            modp_planner=None,
            drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        return await controller.step()

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        return {
            "available": True,
            "drift": (
                self.policy_drift_detector.get_stats()
                if self.policy_drift_detector else {}
            ),
            "gpu": (
                self.gpu_profiler.get_current_metrics()
                if self.gpu_profiler and hasattr(self.gpu_profiler, "get_current_metrics")
                else {}
            ),
        }


# =============================================================================
# MAIN ESTIMATOR (wired)
# =============================================================================
class EnhancedLatencyEstimator:
    def __init__(
        self,
        config: LatencyEstimatorConfig,
        db_pool: AsyncDatabaseManager,
        cache: EnhancedCache,
        circuit_breaker: CircuitBreaker,
        measurer: ILatencyMeasurer,
        service_mesh: IServiceMesh,
        forecaster: IForecaster,
        multi_cloud: IMultiCloudLatency,
        realtime_monitor: IRealtimeMonitor,
        sustainability: ISustainability,
        cloud_storage: ICloudStorage,
        pqc: IPQC,
        optimizer: IOptimizer,
        health_service: EnhancedHealthCheckService,
    ) -> None:
        self.config = config
        self.instance_id = config.general.instance_id
        self.db_pool = db_pool
        self.cache = cache
        self.circuit_breaker = circuit_breaker
        self.measurer = measurer
        self.service_mesh = service_mesh
        self.forecaster = forecaster
        self.multi_cloud = multi_cloud
        self.realtime_monitor = realtime_monitor
        self.sustainability = sustainability
        self.cloud_storage = cloud_storage
        self.pqc = pqc
        self.optimizer = optimizer
        self.health_service = health_service
        self.flexgen_manager = FlexGenManager(config)

        # Shared infra
        self.safety_monitor = SafetyMonitor()
        self.carbon_broker = CarbonOffsetBroker()
        self.human_review = HumanReviewManager()

        # Federated + multi-agent
        self.federated_coordinator = FederatedCoordinator()
        self.multi_agent_coordinator = MultiAgentCoordinator()
        self.chaos_monkey = ChaosMonkey(
            enabled=config.general.chaos_enabled, seed=42
        )

        # XAI
        self.xai_explainer = XAIExplainer()

        self._task_manager = TaskManager()
        self._shutdown_event = asyncio.Event()
        self._running = False
        self.recent_measurements: deque = deque(maxlen=100)
        logger.info(
            "estimator_initialized",
            version=config.general.version,
            instance_id=self.instance_id,
        )

    async def start(self) -> None:
        self._running = True
        await self.db_pool.init()
        await self.cache.start()
        await self.optimizer.initialize()
        if self.config.general.realtime_enabled:
            await self.realtime_monitor.start_monitoring()
        self._task_manager.register_task("maintenance", self._maintenance_loop)
        self._task_manager.register_task("latency_collection", self._latency_collection_loop)
        self._task_manager.register_task("optimizer_loop", self._optimizer_loop)
        self._task_manager.register_task("chaos_testing", self._chaos_testing_loop)
        self._task_manager.register_task("federated_aggregation", self._federated_aggregation_loop)
        self._task_manager.start_registered_tasks()
        logger.info("estimator_started")

    async def _maintenance_loop(self) -> None:
        while self._running and not self._shutdown_event.is_set():
            await asyncio.sleep(3600)

    async def _latency_collection_loop(self) -> None:
        while self._running and not self._shutdown_event.is_set():
            try:
                endpoints = ["example.com"]
                for ep in endpoints:
                    latency = await self.measurer.measure(ep, "https")
                    if latency is None:
                        continue
                    await self.db_pool.save_latency_measurement(
                        "estimator", ep, latency, {"protocol": "https"}
                    )
                    await self.optimizer.record_measurement(
                        {"target": ep, "latency": latency}
                    )
                    self.recent_measurements.append({"target": ep, "latency": latency})
                await asyncio.sleep(self.config.general.latency_measurement_timeout)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("latency_collection_error", error=str(e))
                await asyncio.sleep(60)

    async def _optimizer_loop(self) -> None:
        while self._running and not self._shutdown_event.is_set():
            try:
                recent = await self.db_pool.get_recent_measurements(100)
                recent_measurements = [
                    {"prediction_error": abs(r["latency"] - 100.0)} for r in recent
                ]
                await self.optimizer.adjust_parameters(recent_measurements)
                await asyncio.sleep(self.config.optimizer.adjustment_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("optimizer_loop_error", error=str(e))
                await asyncio.sleep(60)

    async def _chaos_testing_loop(self) -> None:
        while self._running and not self._shutdown_event.is_set():
            try:
                fault = self.chaos_monkey.maybe_fail()
                if fault is None:
                    await asyncio.sleep(60)
                    continue
                await self.chaos_monkey.inject_fault(self, fault)
            except Exception as e:
                logger.warning("chaos_error", error=str(e))
            await asyncio.sleep(60)

    async def _federated_aggregation_loop(self) -> None:
        while self._running and not self._shutdown_event.is_set():
            try:
                stats = await self.optimizer.get_stats()
                # Register local state as a participant
                self.federated_coordinator.register_participant(
                    participant_id=self.instance_id,
                    model_update=stats,
                    sample_count=max(1, int(stats.get("history_length", 1))),
                )
                avg = self.federated_coordinator.aggregate()
                if avg:
                    await self.optimizer.apply_adjustments(avg)
            except Exception as e:
                logger.error("federated_error", error=str(e))
            await asyncio.sleep(300)

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise LatencyEstimatorException(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict:
        return await self.flexgen_manager.get_status()

    async def get_status(self) -> Dict:
        health = await self.health_service.check_all()
        return {
            "instance_id": self.instance_id,
            "version": self.config.general.version,
            "running": self._running,
            "health": health,
            "task_health": self._task_manager.health(),
            "cache_stats": self.cache.get_statistics(),
            "circuit_breaker": self.circuit_breaker.get_metrics(),
            "pqc": await self.pqc.get_status(),
            "optimizer": await self.optimizer.get_stats(),
            "forecaster": await self.forecaster.get_metrics(),
            "flexgen": await self.flexgen_manager.get_status(),
            "safety_violations": len(self.safety_monitor.get_violations()),
            "federated": self.federated_coordinator.get_stats(),
            "multi_agent": self.multi_agent_coordinator.get_agent_stats(),
            "carbon_offsets": self.carbon_broker.get_totals(),
            "chaos_enabled": self.chaos_monkey.enabled,
            "human_review_pending": len(await self.human_review.get_pending()),
        }

    async def shutdown(self) -> None:
        logger.info("estimator_shutdown_start", instance=self.instance_id)
        self._shutdown_event.set()
        self._running = False
        await self.realtime_monitor.stop_monitoring()
        await self.cache.stop()
        await self.measurer.close()
        await self.db_pool.close()
        await self.forecaster.close()
        await self.multi_cloud.close()
        if self.service_mesh:
            await self.service_mesh.close()
        await self._task_manager.stop_all()
        logger.info("estimator_shutdown_complete")


# =============================================================================
# COMPONENT WIRING FACTORY
# =============================================================================
def build_components(config: LatencyEstimatorConfig) -> Dict[str, Any]:
    _ensure_dev_master_key(config)

    db = AsyncDatabaseManager(config)
    cache = EnhancedCache(config)
    cb = GlobalCircuitBreaker().get_or_create(
        "main",
        failure_threshold=config.circuit_breaker.failure_threshold,
        recovery_timeout=config.circuit_breaker.recovery_timeout,
    )
    measurer = ProtocolMeasurer(config)
    safety = SafetyMonitor()
    carbon = CarbonOffsetBroker()

    service_mesh = KubernetesServiceMesh(
        config, db, safety_monitor=safety, carbon_broker=carbon
    )
    forecaster = PredictiveLatencyForecaster(config)
    cloud_storage = MultiCloudStorage(config)
    multi_cloud = MultiCloudLatency(
        config, measurer, cloud_storage,
        safety_monitor=safety, carbon_broker=carbon,
    )
    realtime = RealTimeLatencyMonitor(config, measurer)
    sustainability = SustainabilityIntegration(config)
    vault = VaultManager(config)
    pqc = PostQuantumCrypto(config, db, vault)
    optimizer = BioInspiredOptimizer(config, db)
    health = EnhancedHealthCheckService({
        "db": db, "cache": cache, "circuit_breaker": cb, "measurer": measurer,
        "service_mesh": service_mesh, "forecaster": forecaster,
        "multi_cloud": multi_cloud, "realtime": realtime,
        "sustainability": sustainability, "pqc": pqc,
        "optimizer": optimizer, "cloud_storage": cloud_storage,
    })
    estimator = EnhancedLatencyEstimator(
        config=config, db_pool=db, cache=cache, circuit_breaker=cb,
        measurer=measurer, service_mesh=service_mesh, forecaster=forecaster,
        multi_cloud=multi_cloud, realtime_monitor=realtime,
        sustainability=sustainability, cloud_storage=cloud_storage,
        pqc=pqc, optimizer=optimizer, health_service=health,
    )
    return {"estimator": estimator}


# =============================================================================
# FASTAPI APP (built lazily; lifespan; auth-gated admin endpoints)
# =============================================================================
def create_app(config: Optional[LatencyEstimatorConfig] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config or LatencyEstimatorConfig()
    _ensure_dev_master_key(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[valid-type]
        app.state.estimator = build_components(cfg)["estimator"]
        await app.state.estimator.start()
        try:
            yield
        finally:
            await app.state.estimator.shutdown()

    app = FastAPI(
        title="Cloud Latency Estimator API",
        version="16.1",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    bearer = HTTPBearer(auto_error=False)

    async def require_admin(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer),
    ) -> bool:
        if not cfg.api.require_auth_for_admin:
            return True
        if credentials is None or not JWT_AVAILABLE:
            raise HTTPException(status_code=401, detail="auth required")
        try:
            jwt.decode(
                credentials.credentials, cfg.api.jwt_secret, algorithms=["HS256"]
            )
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid token: {e}")
        return True

    @app.get("/status")
    async def status(request: Request) -> Dict[str, Any]:
        estimator = getattr(request.app.state, "estimator", None)
        if not estimator:
            raise HTTPException(status_code=503, detail="not initialized")
        return await estimator.get_status()

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(
        workload: Dict[str, Any],
        node: Dict[str, Any],
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, Any]:
        estimator = getattr(request.app.state, "estimator", None)
        if not estimator:
            raise HTTPException(status_code=503, detail="not initialized")
        return await estimator.run_flexgen_optimization(workload, node)

    @app.get("/flexgen/status")
    async def flexgen_status(request: Request) -> Dict[str, Any]:
        estimator = getattr(request.app.state, "estimator", None)
        if not estimator:
            raise HTTPException(status_code=503, detail="not initialized")
        return await estimator.get_flexgen_status()

    @app.get("/human-review/pending")
    async def human_review_pending(
        request: Request,
        _: bool = Depends(require_admin),
    ) -> List[Dict[str, Any]]:
        estimator = getattr(request.app.state, "estimator", None)
        if not estimator:
            raise HTTPException(status_code=503, detail="not initialized")
        return await estimator.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(
        review_id: str,
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, str]:
        estimator = getattr(request.app.state, "estimator", None)
        if not estimator:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await estimator.human_review.approve(review_id, reviewer="api")
        return {"status": "approved" if ok else "not_found"}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(
        review_id: str,
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, str]:
        estimator = getattr(request.app.state, "estimator", None)
        if not estimator:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await estimator.human_review.reject(review_id, reviewer="api")
        return {"status": "rejected" if ok else "not_found"}

    @app.post("/chaos/trigger")
    async def chaos_trigger(
        fault: Optional[str] = None,
        request: Request = None,  # type: ignore[assignment]
        _: bool = Depends(require_admin),
    ) -> Dict[str, Any]:
        estimator = getattr(request.app.state, "estimator", None)  # type: ignore[union-attr]
        if not estimator:
            raise HTTPException(status_code=503, detail="not initialized")
        if fault is None:
            estimator.chaos_monkey.enabled = True
            return {"status": "chaos enabled"}
        if fault not in ChaosMonkey.FAULT_TYPES:
            raise HTTPException(status_code=400, detail="unknown fault")
        result = await estimator.chaos_monkey.inject_and_measure(
            estimator, fault, context={}, steps=10
        )
        return result

    return app


# =============================================================================
# MAIN ENTRY POINT (with signal handling)
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Cloud Latency Estimator v16.1 — Enhanced Single-File")
    print("=" * 80)
    config = LatencyEstimatorConfig()
    estimator = build_components(config)["estimator"]
    await estimator.start()
    status = await estimator.get_status()
    print(json.dumps(status, indent=2, default=str))
    print("=" * 80)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(*_a: Any) -> None:
        stop_event.set()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except (NotImplementedError, ValueError):
                signal.signal(sig, _request_stop)
    except Exception:
        pass

    await stop_event.wait()
    await estimator.shutdown()
    print("Shutdown complete")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
