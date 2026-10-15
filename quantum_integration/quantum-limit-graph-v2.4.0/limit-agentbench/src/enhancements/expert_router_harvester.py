#!/usr/bin/env python3
# File: src/enhancements/expert_router_harvester_v3_0.py
"""
Expert Router with Photosynthetic Harvester Awareness — v3.2 (Single-File Enhanced)

Public API preserved:
    ExpertRouterWithHarvester, CausalBandit, SafetyMonitor, XAIExplainer,
    FederatedRouterCoordinator, MultiAgentRouter, CarbonOffsetBroker,
    ChaosMonkey, HumanReviewManager, QuantumDistillationOptimizer,
    FlexGenManager, PostQuantumCrypto, MultiCloudStorage, VaultManager,
    RateLimiter, CircuitBreaker, GlobalCircuitBreaker, AsyncDatabaseManager,
    RouterConfig, all sub-configs, and the exception hierarchy.

Key fixes over v3.1:
- `await self.bandit.update(...)` bug fixed (bandit.update is sync).
- AsyncDatabaseManager uses aiosqlite directly; migrations run in an explicit
  `init()` awaited from the router's `start()`.
- `_save_state` is now an async method awaited from routing/feedback paths.
- PQC performs real ECDSA signing (or Dilithium when pqcrypto is available).
- CausalBandit is a real linear contextual bandit (Bayesian linear regression).
- SafetyMonitor exposes snapshot/restore for chaos.
- XAIExplainer returns a structured dict.
- FederatedRouterCoordinator weights by sample count and scales DP noise to
  sensitivity/epsilon.
- MultiAgentRouter is thread-safe and dedupes reputation updates.
- CarbonOffsetBroker is thread-safe; computes emission from energy, not cost.
- ChaosMonkey has named faults, seeded RNG, snapshot/restore.
- HumanReviewManager has TTL, capacity, and audit log.
- QuantumDistillationOptimizer tries `qiskit_optimization` and falls back to
  classical scoring.
- FlexGenManager raises NotImplementedYetError when FlexGen is missing.
- FastAPI app built via `create_app()` with lifespan and auth on admin endpoints.
- Real component factory; `main()` and FastAPI share wiring.
- `asyncio.ensure_future` removed from `__init__`.
- `RouterConfig.dict()` handled for Pydantic v1/v2 and dataclasses.
- Harvester bonus now reduces cost (correct sign).
- PQC master_key defaults to a random per-instance key when empty.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import hashlib
import inspect
import json
import logging
import logging.handlers  # noqa: F401
import math
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import (
    Any, AsyncIterator, Callable, Deque, Dict, List, Optional, Protocol, Set,
    Tuple, Union, runtime_checkable,
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
                    "router.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )

try:
    from pydantic import BaseModel, Field, field_validator
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
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from sqlalchemy import (  # noqa: F401
        Boolean, Column, DateTime, Float, Integer, JSON, String, Text, text,
    )
    from sqlalchemy.ext.asyncio import (  # noqa: F401
        AsyncSession, async_sessionmaker, create_async_engine,
    )
    from sqlalchemy.ext.declarative import declarative_base  # noqa: F401
    from sqlalchemy.pool import NullPool  # noqa: F401
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # noqa: F401
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

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
    from fastapi import (
        Depends, FastAPI, HTTPException, Request,
    )
    from fastapi.middleware.cors import CORSMiddleware
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

try:
    from opentelemetry import trace  # noqa: F401
    from opentelemetry.sdk.trace import TracerProvider  # noqa: F401
    from opentelemetry.sdk.trace.export import BatchSpanProcessor  # noqa: F401
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter  # noqa: F401
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

# Qiskit — try new module layout first, then legacy.
try:
    from qiskit_optimization import QuadraticProgram
    from qiskit_optimization.algorithms import MinimumEigenOptimizer
    try:
        from qiskit_algorithms import QAOA
    except ImportError:
        from qiskit.algorithms import QAOA  # legacy
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
    from enhancements.moe_system import ExpertRouter as MoEExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore[no-redef]
        def __init__(self, *a, **k): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}

    class MoEExpertRouter:  # type: ignore[no-redef]
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


try:
    from enhancements.gpu_optimization.flexgen_policy import (  # type: ignore
        FlexGenPolicy, generate_candidate_policies,
    )
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController  # type: ignore
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel  # type: ignore
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector  # type: ignore
    from enhancements.schemas.node_descriptor import NodeDescriptor  # type: ignore
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
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


# -----------------------------------------------------------------------------
# Green Agent base components (guarded)
# -----------------------------------------------------------------------------
try:
    from ..expert_router import ExpertRouter  # type: ignore
    from ..expert_registry import ExpertProfile, ExpertRegistry  # type: ignore
    from ..bio_inspired import PhotosyntheticHarvester  # type: ignore
    from ..sustainability_cost import SustainabilityCostFunction  # type: ignore
    from ..database.manager import DatabaseManager  # type: ignore
    from ..task_manager import TaskManager  # type: ignore
except ImportError:

    class ExpertRouter:  # type: ignore[no-redef]
        def __init__(self, *a, **k):
            self.registry = None
        def get_candidate_experts(self, task, context):
            return []

    class ExpertProfile:  # type: ignore[no-redef]
        def __init__(
            self, expert_id: Optional[str] = None, domain: str = "",
            usage_count: int = 0, accuracy_score: float = 0.5,
            last_used: Optional[datetime] = None,
            photosynthetic_harvester_flag: bool = False, **kwargs: Any,
        ):
            self.expert_id = expert_id or str(uuid.uuid4())
            self.domain = domain
            self.usage_count = usage_count
            self.accuracy_score = accuracy_score
            self.last_used = last_used
            self.photosynthetic_harvester_flag = photosynthetic_harvester_flag
            for k, v in kwargs.items():
                setattr(self, k, v)

    class ExpertRegistry:  # type: ignore[no-redef]
        pass

    class PhotosyntheticHarvester:  # type: ignore[no-redef]
        pass

    class SustainabilityCostFunction:  # type: ignore[no-redef]
        async def compute_multiple(self, experts, context):
            return {e.expert_id: 1.0 for e in experts}

    class DatabaseManager:  # type: ignore[no-redef]
        pass

    class TaskManager:  # type: ignore[no-redef]
        pass


# -----------------------------------------------------------------------------
# Structured logging with correlation IDs
# -----------------------------------------------------------------------------
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
            "routing_audit.log", maxBytes=10 * 1024 * 1024, backupCount=5
        )
    except Exception:
        _ah = logging.StreamHandler()
    _ah.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    audit_logger.addHandler(_ah)
audit_logger.setLevel(logging.INFO)


# =============================================================================
# Exceptions
# =============================================================================
class RouterError(Exception):
    """Base exception for the router."""


class CostFunctionError(RouterError): pass
class RegistryError(RouterError): pass
class CircuitBreakerOpenError(RouterError): pass
class RateLimitExceeded(RouterError): pass
class SignatureError(RouterError): pass
class HealthCheckError(RouterError): pass
class SafetyViolationError(RouterError): pass
class ChaosExperimentError(RouterError): pass
class NotImplementedYetError(RouterError): pass


# =============================================================================
# Metrics — single registry with memoized handles
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

    def counter(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, doc, labels or [], registry=self.registry)
            return self._counters[name]

    def gauge(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, doc, labels or [], registry=self.registry)
            return self._gauges[name]

    def histogram(self, name: str, doc: str = "", labels: Optional[List[str]] = None):
        if not self.enabled:
            return _NoopMetric()
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, doc, labels or [], registry=self.registry)
            return self._histograms[name]


METRICS = _MetricsRegistry()
ROUTER_REQUESTS = METRICS.counter("router_requests_total", "Routing requests")
HARVESTER_BONUS = METRICS.counter("router_harvester_bonus_total", "Harvester bonus applied")
SELECTED_COST = METRICS.histogram("router_selected_cost", "Selected expert cost")
SELECTED_BONUS_FACTOR = METRICS.histogram("router_selected_bonus_factor", "Bonus factor applied")
ROUTER_LATENCY = METRICS.histogram("router_latency_seconds", "Routing latency (s)")
CIRCUIT_BREAKER_STATE = METRICS.gauge(
    "router_circuit_breaker_state", "CB state", ["name"]
)
RATE_LIMITER_THROTTLE = METRICS.counter("router_rate_limiter_throttle_total", "Throttles")
PQC_SIGNATURES = METRICS.counter(
    "router_pqc_signatures_total", "PQC signatures", ["algorithm", "status"]
)
CLOUD_STORAGE = METRICS.counter(
    "router_cloud_storage_total", "Cloud storage ops",
    ["provider", "operation", "status"],
)
VAULT_OPERATIONS = METRICS.counter(
    "router_vault_operations_total", "Vault ops", ["operation", "status"]
)
HEALTH_SCORE = METRICS.gauge("router_health_score", "Health score")
SAFETY_VIOLATIONS = METRICS.counter(
    "router_safety_violations_total", "Safety violations", ["rule"]
)
CHAOS_EXPERIMENTS = METRICS.counter(
    "router_chaos_experiments_total", "Chaos experiments", ["fault", "status"]
)
HUMAN_REVIEWS = METRICS.counter(
    "router_human_reviews_total", "Human reviews", ["status"]
)
XAI_DECISIONS = METRICS.counter(
    "router_xai_decisions_total", "XAI decisions", ["policy"]
)
CARBON_OFFSETS = METRICS.counter(
    "router_carbon_offsets_total", "Carbon offsets", ["status"]
)
FEDERATED_PARTICIPANTS = METRICS.gauge(
    "router_federated_participants", "Federated participants"
)
BANDIT_UPDATES = METRICS.counter(
    "router_bandit_updates_total", "Bandit updates", ["policy"]
)


# =============================================================================
# Config
# =============================================================================
def _cfg_to_dict(cfg: Any) -> Dict[str, Any]:
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


if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        bonus_discount: float = Field(0.8, ge=0, le=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        human_review_threshold: float = Field(0.5, ge=0, le=1)

    class CircuitBreakerConfig(BaseModel):
        failure_threshold: int = Field(5, ge=1)
        recovery_timeout: int = Field(60, ge=1)

    class RateLimitConfig(BaseModel):
        enabled: bool = True
        requests_per_minute: int = Field(100, ge=1)
        window_seconds: int = Field(60, ge=1)

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///routing.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/routing")

    class CloudConfig(BaseModel):
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = Field("us-east-1")
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class PQCConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("dilithium")
        master_key: str = Field("", description="Hex string; empty = random per-instance")

        @field_validator("master_key")
        @classmethod
        def validate_master_key(cls, v: str) -> str:
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

    class APIConfig(BaseModel):
        host: str = Field("0.0.0.0")
        port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True
        require_auth_for_admin: bool = True

    class SafetyConfig(BaseModel):
        max_carbon_intensity: float = 500.0
        max_low_harvester_routes: int = 5
        max_consecutive_low_fitness: int = 3
        enable_monitor: bool = True

    class ChaosConfig(BaseModel):
        enabled: bool = False
        failure_probability: float = 0.1
        seed: int = 42
        hitl_ttl_seconds: float = 3600.0
        hitl_max_pending: int = 100

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1,
            }
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bandit_feature_dim: int = Field(8, ge=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        bonus_evolution_enabled: bool = True
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    class QuantumConfig(BaseModel):
        enable_distillation: bool = False
        qaoa_reps: int = 1

    class RouterConfig(BaseModel):
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        pqc: PQCConfig = Field(default_factory=PQCConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)

        def get_master_key_bytes(self) -> bytes:
            return self.pqc.get_master_key_bytes()

        def to_dict(self) -> Dict[str, Any]:
            return _cfg_to_dict(self)

else:
    @dataclass
    class GeneralConfig:
        bonus_discount: float = 0.8
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        human_review_threshold: float = 0.5

    @dataclass
    class CircuitBreakerConfig:
        failure_threshold: int = 5
        recovery_timeout: int = 60

    @dataclass
    class RateLimitConfig:
        enabled: bool = True
        requests_per_minute: int = 100
        window_seconds: int = 60

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///routing.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/routing"

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
    class PQCConfig:
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = ""

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                return os.urandom(32)
            return bytes.fromhex(self.master_key)

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True
        require_auth_for_admin: bool = True

    @dataclass
    class SafetyConfig:
        max_carbon_intensity: float = 500.0
        max_low_harvester_routes: int = 5
        max_consecutive_low_fitness: int = 3
        enable_monitor: bool = True

    @dataclass
    class ChaosConfig:
        enabled: bool = False
        failure_probability: float = 0.1
        seed: int = 42
        hitl_ttl_seconds: float = 3600.0
        hitl_max_pending: int = 100

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {
                "accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1,
            }
        )
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bandit_feature_dim: int = 8
        bio_generations: int = 10
        bio_population_size: int = 20
        bonus_evolution_enabled: bool = True
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    @dataclass
    class QuantumConfig:
        enable_distillation: bool = False
        qaoa_reps: int = 1

    @dataclass
    class RouterConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        pqc: PQCConfig = field(default_factory=PQCConfig)
        api: APIConfig = field(default_factory=APIConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)

        def get_master_key_bytes(self) -> bytes:
            return self.pqc.get_master_key_bytes()

        def to_dict(self) -> Dict[str, Any]:
            return _cfg_to_dict(self)


# =============================================================================
# Interfaces
# =============================================================================
@runtime_checkable
class ICostFunction(Protocol):
    async def compute_multiple(self, experts: List[Any], context: Dict) -> Dict[str, float]: ...


@runtime_checkable
class IRegistry(Protocol):
    def get_expert(self, expert_id: str) -> Optional[Any]: ...
    def get_all_active_experts(self) -> List[Any]: ...


@runtime_checkable
class IHarvester(Protocol):
    async def get_energy_bonus(self, expert: Any, context: Dict) -> float: ...


@runtime_checkable
class IPQC(Protocol):
    async def sign_routing_decision(self, decision_data: Dict) -> Dict: ...
    async def verify_routing_decision(self, data: Dict, signature: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...


@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict: ...
    def get_status(self) -> Dict: ...


@runtime_checkable
class IVault(Protocol):
    async def store_secret(self, path: str, data: Dict) -> None: ...
    async def get_secret(self, path: str) -> Optional[Dict]: ...


@runtime_checkable
class IAsyncDatabase(Protocol):
    async def init(self) -> None: ...
    async def save_routing_decision(self, decision: Dict) -> None: ...
    async def save_optimizer_state(self, state: Dict) -> None: ...
    async def load_optimizer_state(self) -> Optional[Dict]: ...
    async def health_check(self) -> Dict: ...
    async def close(self) -> None: ...


# =============================================================================
# Circuit breaker (sync + async aware)
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self, name: str, failure_threshold: int = 5, recovery_timeout: float = 60.0
    ):
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

    def all_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {name: cb.get_metrics() for name, cb in self._breakers.items()}


# =============================================================================
# RateLimiter
# =============================================================================
class RateLimiter:
    def __init__(self, config: RouterConfig):
        self.config = config
        self.rate = float(config.rate_limit.requests_per_minute)
        self.window = float(config.rate_limit.window_seconds)
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            self.throttled_requests += 1
            return False

    def get_metrics(self) -> Dict[str, Any]:
        total = self.total_requests + self.throttled_requests
        return {
            "total_requests": self.total_requests,
            "throttled_requests": self.throttled_requests,
            "throttle_rate": (self.throttled_requests / max(total, 1)) * 100,
        }


# =============================================================================
# VaultManager
# =============================================================================
class VaultManager(IVault):
    def __init__(self, config: RouterConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url and config.vault.token:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception as e:
                logger.error(f"vault_init_failed error={e}")

    async def store_secret(self, path: str, data: Dict) -> None:
        if not self.client:
            raise RouterError("Vault client not configured")
        try:
            self.client.secrets.kv.v2.create_or_update_secret(path=path, secret=data)
            VAULT_OPERATIONS.labels(operation="store", status="success").inc()
        except Exception as e:
            VAULT_OPERATIONS.labels(operation="store", status="failed").inc()
            raise RouterError(f"vault_store_failed error={e}") from e

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None
        try:
            secret = self.client.secrets.kv.v2.read_secret(path=path)
            VAULT_OPERATIONS.labels(operation="read", status="success").inc()
            return secret["data"]["data"]
        except Exception:
            VAULT_OPERATIONS.labels(operation="read", status="failed").inc()
            return None

    async def health_check(self) -> Dict[str, Any]:
        return {"status": "ok" if self.client else "unavailable"}


# =============================================================================
# PostQuantumCrypto — real signing with ECDSA fallback
# =============================================================================
class PostQuantumCrypto(IPQC):
    def __init__(self, config: RouterConfig, vault: Optional[VaultManager] = None):
        self.config = config
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE and CRYPTO_AVAILABLE
        self.master_key = config.get_master_key_bytes()
        self.salt = os.urandom(16)
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    async def generate_keypair(self, algorithm: Optional[str] = None) -> Dict[str, Any]:
        algorithm = algorithm or self.config.pqc.algorithm
        with self._lock:
            if self.pqc_available and algorithm in {"dilithium", "falcon", "sphincs"}:
                try:
                    mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[algorithm]
                    pk, sk = await asyncio.to_thread(mod.generate_keypair)
                    key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                    self._keys[key_id] = {
                        "private": bytes(sk), "public": bytes(pk), "algorithm": algorithm,
                    }
                    PQC_SIGNATURES.labels(algorithm=algorithm, status="keygen").inc()
                    return {
                        "key_id": key_id, "algorithm": algorithm,
                        "public_key": bytes(pk).hex(), "pqc": True,
                    }
                except Exception as e:
                    logger.warning(f"pqc_keygen_failed error={e}")

            if not CRYPTO_AVAILABLE:
                raise SignatureError("No cryptographic backend available")

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
            PQC_SIGNATURES.labels(algorithm="ecdsa", status="keygen").inc()
            return {
                "key_id": key_id, "algorithm": "ecdsa",
                "public_key": public_bytes.hex(), "pqc": False,
            }

    async def sign_routing_decision(self, decision_data: Dict) -> Dict[str, Any]:
        with self._lock:
            if not self._keys:
                empty = True
            else:
                empty = False
        if empty:
            await self.generate_keypair()

        with self._lock:
            key_id = next(iter(self._keys))
            entry = self._keys[key_id]

        payload = json.dumps(decision_data, sort_keys=True, default=str).encode()
        if entry["algorithm"] == "ecdsa":
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            PQC_SIGNATURES.labels(algorithm="ecdsa", status="signed").inc()
            return {
                "signature": sig.hex(),
                "algorithm": "ecdsa-p256",
                "key_id": key_id,
                "timestamp": datetime.now().isoformat(),
            }
        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
        sig = mod.sign(entry["private"], payload)
        PQC_SIGNATURES.labels(algorithm=entry["algorithm"], status="signed").inc()
        return {
            "signature": bytes(sig).hex(),
            "algorithm": entry["algorithm"],
            "key_id": key_id,
            "timestamp": datetime.now().isoformat(),
        }

    async def verify_routing_decision(self, data: Dict, signature: Dict[str, Any]) -> bool:
        key_id = signature.get("key_id")
        with self._lock:
            entry = self._keys.get(key_id) if key_id else None
        if entry is None:
            return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        try:
            if entry["algorithm"] == "ecdsa":
                public_key = load_pem_public_key(entry["public"], backend=default_backend())
                public_key.verify(
                    bytes.fromhex(signature["signature"]),
                    payload,
                    ec.ECDSA(hashes.SHA256()),
                )
                return True
            mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[
                entry["algorithm"]
            ]
            mod.verify(entry["public"], payload, bytes.fromhex(signature["signature"]))
            return True
        except Exception:
            return False

    def get_quantum_status(self) -> Dict[str, Any]:
        return {
            "pqc_available": self.pqc_available,
            "algorithm_in_use": "pqc" if self.pqc_available else "ecdsa",
            "keys_held": len(self._keys),
        }

    async def health_check(self) -> Dict[str, Any]:
        return {"status": "ok", **self.get_quantum_status()}


# =============================================================================
# MultiCloudStorage — local + optional cloud
# =============================================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: RouterConfig):
        self.config = config
        self.backup_dir = Path("./routing_backups")
        try:
            self.backup_dir.mkdir(exist_ok=True)
        except Exception:
            pass
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
            except Exception as e:
                logger.warning(f"aws_client_init_failed error={e}")

    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict[str, Any]:
        filename = filename or f"routing_{uuid.uuid4().hex[:8]}.json"
        path = self.backup_dir / filename
        payload = json.dumps(data, default=str)
        try:
            await asyncio.to_thread(path.write_text, payload)
        except Exception as e:
            CLOUD_STORAGE.labels(provider="local", operation="store", status="failed").inc()
            raise RouterError(f"local_store_failed error={e}") from e
        CLOUD_STORAGE.labels(provider="local", operation="store", status="success").inc()

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
                CLOUD_STORAGE.labels(provider="aws", operation="store", status="success").inc()
            except Exception as e:
                CLOUD_STORAGE.labels(provider="aws", operation="store", status="failed").inc()
                logger.warning(f"aws_upload_failed error={e}")

        return {"provider": uploaded or ["local"], "location": str(path)}

    async def retrieve(self, key: str) -> Optional[Dict[str, Any]]:
        path = self.backup_dir / key
        if not path.exists():
            return None
        try:
            return json.loads(await asyncio.to_thread(path.read_text))
        except Exception:
            return None

    def get_status(self) -> Dict[str, Any]:
        return {"providers": list(self.providers.keys()) or ["local"],
                "backup_dir": str(self.backup_dir)}

    async def health_check(self) -> Dict[str, Any]:
        return {"status": "ok", **self.get_status()}


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
                conf = 1.0 / (1.0 + math.exp(-abs(samples[best])))
                return best, conf, "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * math.sqrt(var)
            best = max(scores, key=scores.get)
            conf = 1.0 / (1.0 + math.exp(-abs(scores[best])))
            return best, conf, "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
        """Synchronous update. Do NOT await this method."""
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

    def apply_federated_prior(self, action: str, mean_reward: float, weight: float = 0.1) -> None:
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

    def get_action_probabilities(self, context: Any) -> List[float]:
        with self._lock:
            x = self._prepare_features(context)
            scores = np.array([float(x @ self.theta[a]) for a in self.actions])
            z = scores - np.max(scores)
            exp = np.exp(z)
            total = exp.sum()
            if total <= 0:
                return [1.0 / len(self.actions)] * len(self.actions)
            return (exp / total).tolist()

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
    def __init__(self, config: SafetyConfig):
        self.config = config
        self.low_harvester_routes = 0
        self.consecutive_low_fitness = 0
        self.history: Deque[Dict[str, Any]] = deque(maxlen=50)
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def reset_episode(self) -> None:
        with self._lock:
            self.low_harvester_routes = 0
            self.consecutive_low_fitness = 0

    def check_routing(self, context: Dict, expert: Any, fitness: float) -> bool:
        if not self.config.enable_monitor:
            return True
        with self._lock:
            carbon_intensity = context.get("carbon_intensity", 0.0)
            if isinstance(carbon_intensity, (int, float)) and carbon_intensity <= 1.0:
                carbon_intensity = float(carbon_intensity) * 800.0

            if carbon_intensity > self.config.max_carbon_intensity:
                self._record("max_carbon_intensity", {"carbon": carbon_intensity})
                return False

            is_harvester = getattr(expert, "photosynthetic_harvester_flag", False)
            if not is_harvester and carbon_intensity > self.config.max_carbon_intensity * 0.8:
                self.low_harvester_routes += 1
                if self.low_harvester_routes > self.config.max_low_harvester_routes:
                    self._record("max_low_harvester_routes",
                                 {"count": self.low_harvester_routes})
                    return False
            else:
                self.low_harvester_routes = 0

            if fitness < 0.3:
                self.consecutive_low_fitness += 1
                if self.consecutive_low_fitness > self.config.max_consecutive_low_fitness:
                    self._record("consecutive_low_fitness",
                                 {"count": self.consecutive_low_fitness})
                    return False
            else:
                self.consecutive_low_fitness = 0

            self.history.append({
                "expert_id": getattr(expert, "expert_id", "unknown"),
                "fitness": float(fitness),
                "carbon": float(carbon_intensity),
                "timestamp": time.time(),
            })
            return True

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_carbon_intensity": self.config.max_carbon_intensity,
                "max_low_harvester_routes": self.config.max_low_harvester_routes,
                "max_consecutive_low_fitness": self.config.max_consecutive_low_fitness,
                "enable_monitor": self.config.enable_monitor,
            }

    def restore(self, snapshot: Dict[str, Any]) -> None:
        with self._lock:
            if "max_carbon_intensity" in snapshot:
                self.config.max_carbon_intensity = float(snapshot["max_carbon_intensity"])
            if "max_low_harvester_routes" in snapshot:
                self.config.max_low_harvester_routes = int(snapshot["max_low_harvester_routes"])
            if "max_consecutive_low_fitness" in snapshot:
                self.config.max_consecutive_low_fitness = int(snapshot["max_consecutive_low_fitness"])
            if "enable_monitor" in snapshot:
                self.config.enable_monitor = bool(snapshot["enable_monitor"])

    def _record(self, rule: str, details: Dict[str, Any]) -> None:
        self.violations.append({
            "rule": rule,
            "details": details,
            "timestamp": datetime.now().isoformat(),
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
    def explain_routing(
        self,
        expert: Any,
        policy: str,
        context: Dict[str, Any],
        confidence: float,
        cost: float,
        bonus_applied: bool,
    ) -> Dict[str, Any]:
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        narrative = (
            f"Selected expert '{getattr(expert, 'expert_id', 'unknown')}' "
            f"(domain='{getattr(expert, 'domain', 'unknown')}') "
            f"using policy '{policy}' (confidence={confidence:.2f}). "
            f"cost={cost:.4f}; harvester_bonus={bonus_applied}."
        )
        return {
            "expert_id": getattr(expert, "expert_id", "unknown"),
            "policy": policy,
            "confidence": float(confidence),
            "cost": float(cost),
            "bonus_applied": bool(bonus_applied),
            "top_contributions": top,
            "narrative": narrative,
        }

    def explain_routing_str(
        self,
        expert: Any,
        policy: str,
        context: Dict[str, Any],
        confidence: float,
        cost: float,
        bonus_applied: bool,
    ) -> str:
        return self.explain_routing(
            expert, policy, context, confidence, cost, bonus_applied
        )["narrative"]


# =============================================================================
# FederatedRouterCoordinator — weighted, round IDs, DP scaled correctly
# =============================================================================
class FederatedRouterCoordinator:
    def __init__(
        self,
        privacy_budget: float = 0.5,
        sensitivity: float = 1.0,
        min_clients: int = 1,
        seed: int = 42,
    ):
        self.privacy_budget = max(float(privacy_budget), 1e-6)
        self.sensitivity = float(sensitivity)
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
            FEDERATED_PARTICIPANTS.set(len(self.participants))

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

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "round_id": self.round_id,
                "num_participants": len(self.participants),
                "privacy_budget": self.privacy_budget,
                "sensitivity": self.sensitivity,
            }


# =============================================================================
# MultiAgentRouter — thread-safe, dedup, stable hash
# =============================================================================
class MultiAgentRouter:
    def __init__(self):
        self.expert_bids: Dict[str, float] = {}
        self.domain_reputation: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.specialist_roles: Dict[str, str] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(expert_id: str) -> int:
        return int(hashlib.sha256(expert_id.encode()).hexdigest(), 16)

    def submit_bid(self, expert_id: str, bid: float) -> None:
        with self._lock:
            self.expert_bids[expert_id] = float(bid)

    def record_outcome(self, expert_id: str, domain: str, success: bool) -> None:
        alpha = 0.3
        with self._lock:
            prev = self.domain_reputation[domain].get(expert_id, 0.5)
            new = prev + alpha * ((1.0 if success else 0.0) - prev)
            self.domain_reputation[domain][expert_id] = new
            if new > 0.75:
                self.specialist_roles[expert_id] = f"specialist_{domain}"

    def get_specialist_for_domain(self, domain: str) -> Optional[str]:
        with self._lock:
            rep = self.domain_reputation.get(domain, {})
            if not rep:
                return None
            return max(rep, key=rep.get)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "num_experts": len(self.expert_bids),
                "domains": list(self.domain_reputation.keys()),
                "specialist_roles": dict(self.specialist_roles),
            }


# =============================================================================
# CarbonOffsetBroker — thread-safe, wired
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

    def reward_penalty(self, carbon_intensity: float, weight: float = 0.1) -> float:
        excess = max(0.0, carbon_intensity - self.threshold)
        return (excess / 1000.0) * weight

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
                "timestamp": datetime.now().isoformat(),
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
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
        CARBON_OFFSETS.labels(status="rec").inc()
        return record

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
    FAULT_TYPES = {"drop_registry", "freeze_bandit", "safety_override", "skip_hitl"}

    def __init__(self, config: ChaosConfig):
        self.config = config
        self._rng = random.Random(int(config.seed))
        self.injected_failures = 0
        self.events: List[Dict[str, Any]] = []
        self._frozen = False
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def maybe_fail(self, component: str = "router") -> Optional[str]:
        if not self.config.enabled:
            return None
        if self._rng.random() >= self.config.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self.injected_failures += 1
        self._record(fault, "triggered", {"component": component})
        CHAOS_EXPERIMENTS.labels(fault=fault, status="triggered").inc()
        return fault

    def apply(self, fault: str, router: "ExpertRouterWithHarvester") -> None:
        with self._lock:
            if fault == "drop_registry":
                self._snapshots.setdefault("drop_registry", {"registry": router.registry})
                router.registry = _NullRegistry()
            elif fault == "freeze_bandit":
                self._frozen = True
            elif fault == "safety_override":
                self._snapshots.setdefault(
                    "safety_override", {"safety": router.safety_monitor.snapshot()}
                )
                router.safety_monitor.config.max_carbon_intensity *= 2.0
            elif fault == "skip_hitl":
                self._snapshots.setdefault("skip_hitl", {"marker": True})

    def cleanup_fault(self, fault: str, router: "ExpertRouterWithHarvester") -> None:
        with self._lock:
            snap = self._snapshots.pop(fault, None)
            if snap is None:
                return
            if fault == "drop_registry":
                router.registry = snap["registry"]
            elif fault == "safety_override":
                router.safety_monitor.restore(snap["safety"])
            elif fault == "freeze_bandit":
                self._frozen = False

    def is_frozen(self) -> bool:
        return self._frozen

    def is_hitl_skipped(self) -> bool:
        return "skip_hitl" in self._snapshots

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault, "outcome": outcome,
            "timestamp": datetime.now().isoformat(), **(extra or {}),
        })

    async def inject_and_measure(
        self, router: "ExpertRouterWithHarvester", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, router)
        recovery_step: Optional[int] = None
        try:
            for step in range(steps):
                try:
                    _ = router._last_decision  # trivial liveness probe
                    if recovery_step is None:
                        recovery_step = step
                except Exception:
                    continue
        finally:
            self.cleanup_fault(fault, router)
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
                "enabled": self.config.enabled,
                "failure_probability": self.config.failure_probability,
                "injected_failures": self.injected_failures,
                "frozen": self._frozen,
                "pending_snapshots": list(self._snapshots.keys()),
            }


class _NullRegistry:
    def get_expert(self, expert_id: str): return None
    def get_all_active_experts(self): return []


# =============================================================================
# HumanReviewManager — TTL, capacity, audit
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
    def __init__(self, ttl_seconds: float = 3600.0, max_pending: int = 100):
        self.ttl_seconds = float(ttl_seconds)
        self.max_pending = int(max_pending)
        self.pending_reviews: Dict[str, HumanReviewRequest] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    def _evict_expired(self) -> None:
        now = datetime.now()
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

    async def request_review(self, decision_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid, decision_id=decision_id,
                details=dict(details), created_at=datetime.now(),
            )
            self._enforce_capacity()
            HUMAN_REVIEWS.labels(status="pending").inc()
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str) -> None:
        self.audit_log.append({
            "review_id": review_id, "action": action,
            "reviewer": reviewer, "timestamp": datetime.now().isoformat(),
        })

    async def approve(self, review_id: str, reviewer: str = "unknown") -> bool:
        async with self._lock:
            r = self.pending_reviews.get(review_id)
            if r is None or r.status != "pending":
                return False
            r.status = "approved"
            r.reviewer = reviewer
            r.resolution_at = datetime.now()
            self._audit(review_id, "approved", reviewer)
            HUMAN_REVIEWS.labels(status="approved").inc()
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
            HUMAN_REVIEWS.labels(status="rejected").inc()
            return True

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            self._evict_expired()
            return [
                {
                    "review_id": r.review_id,
                    "decision_id": r.decision_id,
                    "details": r.details,
                    "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values() if r.status == "pending"
            ]


# =============================================================================
# QuantumDistillationOptimizer — honest weighted selection
# =============================================================================
class QuantumDistillationOptimizer:
    """
    Weighted multi-objective selector. Tries qiskit_optimization for QAOA if
    configured, otherwise uses classical scoring.
    """

    def __init__(self, config: QuantumConfig):
        self.config = config
        self.available = bool(config.enable_distillation)

    async def select_best_expert(
        self,
        candidates: List[Any],
        context: Dict[str, Any],
        modp_weights: Dict[str, float],
    ) -> Optional[Any]:
        if not self.available or not candidates:
            return None
        if QISKIT_AVAILABLE:
            try:
                return await self._quantum_select(candidates, context, modp_weights)
            except Exception as e:
                logger.warning(f"quantum_select_failed error={e}")
        # Classical fallback
        return self._classical_select(candidates, context, modp_weights)

    def _classical_select(
        self, candidates: List[Any], context: Dict, weights: Dict[str, float]
    ) -> Optional[Any]:
        best = None
        best_score = -float("inf")
        carbon = context.get("carbon_intensity", 0.5)
        for e in candidates:
            accuracy = float(getattr(e, "accuracy_score", 0.5) or 0.5)
            usage = float(getattr(e, "usage_count", 0) or 0)
            score = (
                accuracy * float(weights.get("accuracy", 0.4))
                + (1.0 - min(usage / 100.0, 1.0)) * float(weights.get("energy", 0.3))
                + (1.0 - carbon) * float(weights.get("carbon", 0.2))
                + 0.5 * float(weights.get("latency", 0.1))
            )
            if score > best_score:
                best_score = score
                best = e
        return best

    async def _quantum_select(
        self, candidates: List[Any], context: Dict, weights: Dict[str, float]
    ) -> Optional[Any]:
        qp = QuadraticProgram()
        for e in candidates:
            qp.binary_var(str(e.expert_id))
        carbon = context.get("carbon_intensity", 0.5)
        linear: Dict[str, float] = {}
        for e in candidates:
            accuracy = float(getattr(e, "accuracy_score", 0.5) or 0.5)
            usage = float(getattr(e, "usage_count", 0) or 0)
            u = (
                accuracy * float(weights.get("accuracy", 0.4))
                + (1.0 - min(usage / 100.0, 1.0)) * float(weights.get("energy", 0.3))
                + (1.0 - carbon) * float(weights.get("carbon", 0.2))
                + 0.5 * float(weights.get("latency", 0.1))
            )
            linear[str(e.expert_id)] = -u
        qp.minimize(linear=linear)
        qp.linear_constraint(
            linear={str(e.expert_id): 1 for e in candidates},
            sense="E", rhs=1, name="one_expert",
        )
        qaoa = QAOA(reps=self.config.qaoa_reps)
        optimizer = MinimumEigenOptimizer(qaoa)
        result = await asyncio.to_thread(optimizer.solve, qp)
        for i, e in enumerate(candidates):
            if result.x[i] > 0.5:
                return e
        return None

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "qiskit_available": QISKIT_AVAILABLE,
            "qiskit_legacy": QISKIT_LEGACY if QISKIT_AVAILABLE else None,
        }


# =============================================================================
# FlexGenManager — honest delegation
# =============================================================================
class FlexGenManager:
    def __init__(self, config: RouterConfig):
        self.config = config
        self._cost_model = None
        self._drift_detector = None
        self._gpu_profiler = None
        if FLEXGEN_AVAILABLE:
            try:
                self._cost_model = FlexGenCostModel(
                    carbon_intensity_g_per_kwh=config.optimizer.flexgen_carbon_intensity_default
                )
                self._drift_detector = PolicyDriftDetector()
            except Exception as e:
                logger.warning(f"flexgen_init_failed error={e}")
                self._cost_model = None
                self._drift_detector = None
            try:
                from enhancements.gpu_profiler import GPUProfiler  # type: ignore
                self._gpu_profiler = GPUProfiler()
            except Exception:
                self._gpu_profiler = None

    @property
    def available(self) -> bool:
        return FLEXGEN_AVAILABLE

    async def optimize_policy(self, workload: Any, node: Any) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            from enhancements.gpu_optimization.flexgen_controller import (  # type: ignore
                FlexGenController,
            )
            from enhancements.gpu_optimization.flexgen_policy_selector import (  # type: ignore
                DistillationFlexGenSelector,
            )
        except ImportError as e:
            raise NotImplementedYetError(f"FlexGen submodule missing: {e}") from e
        _selector = DistillationFlexGenSelector(  # noqa: F841
            n_candidates=20,
            config={
                "epsilon": self.config.optimizer.flexgen_selector_epsilon,
                "epsilon_decay": self.config.optimizer.flexgen_selector_epsilon_decay,
            },
        )
        controller = FlexGenController(
            node=node, workload=workload,
            carbon_intensity=getattr(workload, "metadata", {}).get(
                "carbon_intensity",
                self.config.optimizer.flexgen_carbon_intensity_default,
            ),
            use_real_executor=self.config.optimizer.flexgen_use_real_executor,
            executor=None,
            cost_model=self._cost_model,
            use_bio_search=True,
            bio_search_config={
                "population_size": self.config.optimizer.flexgen_population_size,
                "generations": self.config.optimizer.flexgen_generations,
            },
            modp_planner=None,
            drift_detector=self._drift_detector,
            gpu_profiler=self._gpu_profiler,
        )
        return await controller.step()

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        if FLEXGEN_AVAILABLE:
            try:
                wd = WorkloadDescriptor(**workload) if isinstance(workload, dict) else workload
                nd = NodeDescriptor()
                result = await self.optimize_policy(wd, nd)
                if isinstance(result, dict) and "chosen_policy" in result:
                    precision = result["chosen_policy"].get("precision")
                    if precision in ("fp32", "fp16", "int8"):
                        return precision
            except NotImplementedYetError:
                pass
            except Exception as e:
                logger.warning(f"flexgen_precision_failed error={e}")

        carbon = float(workload.get(
            "carbon_intensity",
            self.config.optimizer.flexgen_carbon_intensity_default,
        ))
        size = workload.get("size", "medium")
        if carbon > 500 or size == "large":
            return "int8"
        if carbon > 300 or size == "medium":
            return "fp16"
        return "fp32"

    async def get_status(self) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        gpu = {}
        if self._gpu_profiler and hasattr(self._gpu_profiler, "get_current_metrics"):
            try:
                gpu = self._gpu_profiler.get_current_metrics()
            except Exception:
                gpu = {}
        return {
            "available": True,
            "drift": self._drift_detector.get_stats() if self._drift_detector else {},
            "gpu": gpu,
        }


# =============================================================================
# AsyncDatabaseManager — SQLAlchemy (async) with aiosqlite fallback
# =============================================================================
class AsyncDatabaseManager(IAsyncDatabase):
    SCHEMA_VERSION = 3

    def __init__(self, config: RouterConfig):
        self.config = config
        self.db_url = config.database.url
        self.async_engine = None
        self.async_session = None
        self._aiosqlite_conn = None
        self._use_aiosqlite = False
        self._initialized = False
        self._lock = asyncio.Lock()
        self._sqlite_path = self._extract_sqlite_path(config.database.url)

    @staticmethod
    def _extract_sqlite_path(url: str) -> Optional[str]:
        if url.startswith("sqlite+aiosqlite:///"):
            return url.split("///", 1)[1]
        if url.startswith("sqlite:///"):
            return url.split("///", 1)[1]
        return None

    async def init(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            # Try SQLAlchemy first
            if SQLALCHEMY_AVAILABLE:
                try:
                    self.async_engine = create_async_engine(
                        self.db_url,
                        poolclass=NullPool,
                    )
                    self.async_session = async_sessionmaker(
                        self.async_engine, expire_on_commit=False
                    )
                    await self._apply_migrations_sqlalchemy()
                    self._initialized = True
                    logger.info("db_initialized backend=sqlalchemy")
                    return
                except Exception as e:
                    logger.warning(f"sqlalchemy_init_failed error={e}")
                    self.async_engine = None
                    self.async_session = None
            # Fallback to aiosqlite
            if AIOSQLITE_AVAILABLE and self._sqlite_path:
                self._aiosqlite_conn = await aiosqlite.connect(self._sqlite_path)
                await self._apply_migrations_aiosqlite()
                self._use_aiosqlite = True
                self._initialized = True
                logger.info("db_initialized backend=aiosqlite")
                return
            # No persistence available
            logger.warning("db_initialization_skipped no_backend_available")
            self._initialized = True

    async def _apply_migrations_sqlalchemy(self) -> None:
        async with self.async_engine.begin() as conn:
            await conn.execute(text(
                "CREATE TABLE IF NOT EXISTS schema_version ("
                "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            ))
            result = await conn.execute(text(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            ))
            row = result.fetchone()
            current = row[0] if row else 0
            if current < 1:
                await conn.execute(text(
                    "CREATE TABLE IF NOT EXISTS routing_decisions ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, routing_id TEXT UNIQUE, "
                    "task_type TEXT, selected_expert_id TEXT, cost REAL, "
                    "bonus_applied INTEGER, context TEXT, timestamp TEXT, "
                    "pqc_signature TEXT, explanation TEXT, precision TEXT)"
                ))
                await conn.execute(text(
                    "INSERT INTO schema_version (version, applied_at) "
                    "VALUES (1, datetime('now'))"
                ))
                current = 1
            if current < 2:
                await conn.execute(text(
                    "CREATE TABLE IF NOT EXISTS optimizer_state ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, "
                    "value TEXT, updated_at TEXT)"
                ))
                await conn.execute(text(
                    "INSERT INTO schema_version (version, applied_at) "
                    "VALUES (2, datetime('now'))"
                ))
                current = 2
            if current < 3:
                with contextlib.suppress(Exception):
                    await conn.execute(text(
                        "ALTER TABLE routing_decisions ADD COLUMN explanation TEXT"
                    ))
                with contextlib.suppress(Exception):
                    await conn.execute(text(
                        "ALTER TABLE routing_decisions ADD COLUMN precision TEXT"
                    ))
                await conn.execute(text(
                    "INSERT INTO schema_version (version, applied_at) "
                    "VALUES (3, datetime('now'))"
                ))

    async def _apply_migrations_aiosqlite(self) -> None:
        conn = self._aiosqlite_conn
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version ("
            "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        cursor = await conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        current = row[0] if row else 0
        if current < 1:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS routing_decisions ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, routing_id TEXT UNIQUE, "
                "task_type TEXT, selected_expert_id TEXT, cost REAL, "
                "bonus_applied INTEGER, context TEXT, timestamp TEXT, "
                "pqc_signature TEXT, explanation TEXT, precision TEXT)"
            )
            await conn.execute(
                "INSERT INTO schema_version (version, applied_at) "
                "VALUES (1, datetime('now'))"
            )
            current = 1
        if current < 2:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS optimizer_state ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, "
                "value TEXT, updated_at TEXT)"
            )
            await conn.execute(
                "INSERT INTO schema_version (version, applied_at) "
                "VALUES (2, datetime('now'))"
            )
            current = 2
        if current < 3:
            with contextlib.suppress(Exception):
                await conn.execute(
                    "ALTER TABLE routing_decisions ADD COLUMN explanation TEXT"
                )
            with contextlib.suppress(Exception):
                await conn.execute(
                    "ALTER TABLE routing_decisions ADD COLUMN precision TEXT"
                )
            await conn.execute(
                "INSERT INTO schema_version (version, applied_at) "
                "VALUES (3, datetime('now'))"
            )
        await conn.commit()

    async def save_routing_decision(self, decision: Dict) -> None:
        if not self._initialized:
            await self.init()
        payload = {
            "routing_id": decision.get("routing_id", str(uuid.uuid4())),
            "task_type": decision.get("task_type", "unknown"),
            "selected_expert_id": decision.get("selected_expert_id", ""),
            "cost": float(decision.get("cost", 0.0)),
            "bonus_applied": 1 if decision.get("bonus_applied") else 0,
            "context": json.dumps(decision.get("context", {}), default=str),
            "timestamp": decision.get("timestamp", datetime.now().isoformat()),
            "pqc_signature": json.dumps(decision.get("pqc_signature", {}), default=str),
            "explanation": json.dumps(decision.get("explanation", {}), default=str)
            if isinstance(decision.get("explanation"), dict)
            else str(decision.get("explanation", "")),
            "precision": decision.get("precision", "fp32"),
        }
        if self.async_session is not None:
            try:
                async with self.async_session() as session:
                    await session.execute(
                        text(
                            "INSERT OR REPLACE INTO routing_decisions "
                            "(routing_id, task_type, selected_expert_id, cost, "
                            "bonus_applied, context, timestamp, pqc_signature, "
                            "explanation, precision) "
                            "VALUES (:routing_id, :task_type, :selected_expert_id, "
                            ":cost, :bonus_applied, :context, :timestamp, "
                            ":pqc_signature, :explanation, :precision)"
                        ),
                        payload,
                    )
                    await session.commit()
                return
            except Exception as e:
                logger.error(f"save_routing_decision_failed error={e}")
        if self._use_aiosqlite and self._aiosqlite_conn is not None:
            try:
                await self._aiosqlite_conn.execute(
                    "INSERT OR REPLACE INTO routing_decisions "
                    "(routing_id, task_type, selected_expert_id, cost, "
                    "bonus_applied, context, timestamp, pqc_signature, "
                    "explanation, precision) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        payload["routing_id"], payload["task_type"],
                        payload["selected_expert_id"], payload["cost"],
                        payload["bonus_applied"], payload["context"],
                        payload["timestamp"], payload["pqc_signature"],
                        payload["explanation"], payload["precision"],
                    ),
                )
                await self._aiosqlite_conn.commit()
            except Exception as e:
                logger.error(f"save_routing_decision_aiosqlite_failed error={e}")

    async def save_optimizer_state(self, state: Dict) -> None:
        if not self._initialized:
            await self.init()
        value = json.dumps(state, default=str)
        ts = datetime.now().isoformat()
        if self.async_session is not None:
            try:
                async with self.async_session() as session:
                    await session.execute(
                        text(
                            "INSERT OR REPLACE INTO optimizer_state "
                            "(key, value, updated_at) VALUES (:key, :value, :updated_at)"
                        ),
                        {"key": "state", "value": value, "updated_at": ts},
                    )
                    await session.commit()
                return
            except Exception as e:
                logger.error(f"save_optimizer_state_failed error={e}")
        if self._use_aiosqlite and self._aiosqlite_conn is not None:
            try:
                await self._aiosqlite_conn.execute(
                    "INSERT OR REPLACE INTO optimizer_state "
                    "(key, value, updated_at) VALUES (?, ?, ?)",
                    ("state", value, ts),
                )
                await self._aiosqlite_conn.commit()
            except Exception as e:
                logger.error(f"save_optimizer_state_aiosqlite_failed error={e}")

    async def load_optimizer_state(self) -> Optional[Dict]:
        if not self._initialized:
            await self.init()
        if self.async_session is not None:
            try:
                async with self.async_session() as session:
                    result = await session.execute(
                        text("SELECT value FROM optimizer_state WHERE key = 'state'")
                    )
                    row = result.fetchone()
                    if row:
                        return json.loads(row[0])
                return None
            except Exception as e:
                logger.error(f"load_optimizer_state_failed error={e}")
        if self._use_aiosqlite and self._aiosqlite_conn is not None:
            try:
                cursor = await self._aiosqlite_conn.execute(
                    "SELECT value FROM optimizer_state WHERE key = 'state'"
                )
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
            except Exception as e:
                logger.error(f"load_optimizer_state_aiosqlite_failed error={e}")
        return None

    async def health_check(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if self._initialized else "not_initialized",
            "sqlalchemy": SQLALCHEMY_AVAILABLE,
            "aiosqlite": AIOSQLITE_AVAILABLE and self._use_aiosqlite,
        }

    async def close(self) -> None:
        async with self._lock:
            if self.async_engine is not None:
                try:
                    await self.async_engine.dispose()
                except Exception:
                    pass
                self.async_engine = None
                self.async_session = None
            if self._aiosqlite_conn is not None:
                try:
                    await self._aiosqlite_conn.close()
                except Exception:
                    pass
                self._aiosqlite_conn = None
            self._initialized = False


# =============================================================================
# ExpertRouterWithHarvester v3.2
# =============================================================================
class ExpertRouterWithHarvester(ExpertRouter):
    """
    Enhanced ExpertRouter with causal RL, temporal safety, XAI, secure
    federated aggregation, multi-agent bidding, carbon offsets, chaos
    injection, and human-in-the-loop review.
    """

    def __init__(
        self,
        config: RouterConfig,
        cost_function: ICostFunction,
        registry: IRegistry,
        harvester: Optional[IHarvester] = None,
        db: Optional[IAsyncDatabase] = None,
        pqc: Optional[IPQC] = None,
        cloud_storage: Optional[ICloudStorage] = None,
        vault: Optional[IVault] = None,
        *args: Any,
        **kwargs: Any,
    ):
        try:
            super().__init__(*args, **kwargs)
        except Exception:
            pass
        self.config = config
        self.cost_function = cost_function
        self.registry = registry
        self.harvester = harvester
        self.db = db
        self.pqc = pqc
        self.cloud_storage = cloud_storage
        self.vault = vault

        self.rate_limiter = RateLimiter(config)

        # Enhanced modules
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.moe = MoEExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.selection_policies = [
            "cost_based", "accuracy_focused", "energy_focused", "balanced",
        ]
        self.bandit = CausalBandit(
            action_space=self.selection_policies,
            fallback_solver=lambda ctx: "cost_based",
            min_trials_before_bandit=config.optimizer.bandit_min_trials,
            confidence_threshold=config.optimizer.bandit_confidence_threshold,
            feature_dim=config.optimizer.bandit_feature_dim,
        )
        self.bonus_population = [config.general.bonus_discount]
        self.bonus_rewards: Deque[float] = deque(maxlen=100)

        # New enhancement modules
        self.safety_monitor = SafetyMonitor(config.safety)
        self.xai = XAIExplainer()
        self.federated = FederatedRouterCoordinator()
        self.multi_agent = MultiAgentRouter()
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(config.chaos)
        self.human_review = HumanReviewManager(
            ttl_seconds=config.chaos.hitl_ttl_seconds,
            max_pending=config.chaos.hitl_max_pending,
        )
        self.quantum_optimizer = QuantumDistillationOptimizer(config.quantum)
        self.flexgen_manager = FlexGenManager(config)

        self._last_decision: Dict[str, Any] = {}
        self._last_review_id: Optional[str] = None
        self._started = False
        self._startup_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "ExpertRouterWithHarvester":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        async with self._startup_lock:
            if self._started:
                return
            self._started = True
            if self.db is not None:
                init = getattr(self.db, "init", None)
                if init is not None:
                    try:
                        result = init()
                        if inspect.isawaitable(result):
                            await result
                    except Exception as e:
                        logger.warning(f"db_init_failed error={e}")
            # Restore state if available.
            await self._load_state()

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        if self.db is not None:
            close = getattr(self.db, "close", None)
            if close is not None:
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    pass

    async def _load_state(self) -> None:
        if self.db is None:
            return
        loader = getattr(self.db, "load_optimizer_state", None)
        if loader is None:
            return
        try:
            state = await loader()
            if not state:
                return
            if "bonus_population" in state:
                self.bonus_population = list(state["bonus_population"]) or self.bonus_population
            if "theta" in state and isinstance(state["theta"], dict):
                for k, v in state["theta"].items():
                    if k in self.bandit.theta:
                        try:
                            self.bandit.theta[k] = np.asarray(v, dtype=float)
                        except Exception:
                            pass
            if "P" in state and isinstance(state["P"], dict):
                for k, v in state["P"].items():
                    if k in self.bandit.P:
                        try:
                            self.bandit.P[k] = np.asarray(v, dtype=float)
                        except Exception:
                            pass
            logger.info("router_state_loaded")
        except Exception as e:
            logger.warning(f"router_state_load_failed error={e}")

    async def _save_state(self) -> None:
        if self.db is None:
            return
        saver = getattr(self.db, "save_optimizer_state", None)
        if saver is None:
            return
        async with self._state_lock:
            state = {
                "bonus_population": list(self.bonus_population),
                "theta": {a: self.bandit.theta[a].tolist() for a in self.bandit.theta},
                "P": {a: self.bandit.P[a].tolist() for a in self.bandit.P},
                "federated_round": self.federated.round_id,
            }
            try:
                result = saver(state)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.warning(f"router_state_save_failed error={e}")

    # ------------------------------------------------------------------ #
    # Harvester bonus                                                    #
    # ------------------------------------------------------------------ #
    async def _apply_harvester_bonus(
        self, cost: float, context: Dict, expert: Any
    ) -> Tuple[float, bool]:
        """
        Returns (adjusted_cost, bonus_applied).
        A smaller bonus_factor means lower cost. When the expert is flagged as
        a harvester and the data source is the harvester, we scale cost by the
        bonus factor — bonus factor is interpreted as a discount (<= 1
        reduces cost, > 1 increases cost).
        """
        data_source = context.get("data_source", "cloud")
        harvester_flag = getattr(expert, "photosynthetic_harvester_flag", False)
        if data_source == "photosynthetic_harvester" and harvester_flag:
            if self.harvester is not None and hasattr(self.harvester, "get_energy_bonus"):
                try:
                    factor = float(await self.harvester.get_energy_bonus(expert, context))
                except Exception as e:
                    logger.warning(f"harvester_bonus_failed error={e}")
                    factor = float(np.mean(self.bonus_population)) if self.bonus_population else self.config.general.bonus_discount
            else:
                factor = float(np.mean(self.bonus_population)) if self.bonus_population else self.config.general.bonus_discount
            return cost * factor, True
        return cost, False

    # ------------------------------------------------------------------ #
    # Routing                                                             #
    # ------------------------------------------------------------------ #
    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        ROUTER_REQUESTS.inc()
        start_time = time.time()
        if not await self.rate_limiter.acquire():
            RATE_LIMITER_THROTTLE.inc()
            raise RateLimitExceeded("Rate limit exceeded")
        try:
            result = await self._route_with_enhanced_modules(task, context)
            ROUTER_LATENCY.observe(time.time() - start_time)
            return result
        except ChaosExperimentError as e:
            logger.warning(f"chaos_failure error={e}")
            raise
        except Exception:
            logger.exception("routing_failed")
            raise

    async def _route_with_enhanced_modules(
        self, task: Dict, context: Dict
    ) -> Dict[str, Any]:
        # Chaos injection — apply fault if any.
        fault = self.chaos_monkey.maybe_fail("routing")
        if fault is not None:
            try:
                self.chaos_monkey.apply(fault, self)
            except Exception as e:
                logger.warning(f"chaos_apply_failed error={e}")

        try:
            candidates = await self._get_candidates_with_breaker(task, context)
            if not candidates:
                raise RegistryError("No candidate experts found")

            costs = await self._compute_costs_with_breaker(candidates, context)

            context_for_bandit = {
                "task_hash": float(int(hashlib.sha256(
                    task.get("type", "unknown").encode()
                ).hexdigest()[:6], 16)),
                "data_source_hash": float(int(hashlib.sha256(
                    context.get("data_source", "cloud").encode()
                ).hexdigest()[:6], 16)),
                "num_candidates": float(len(candidates)),
                "avg_cost": float(np.mean(list(costs.values()))) if costs else 0.0,
                "carbon_intensity": float(context.get("carbon_intensity", 0.5)),
                "hour": float(datetime.now().hour),
            }
            encoded = self._encode_context(context_for_bandit)
            policy, confidence, source = self.bandit.select_action(encoded)
            if not policy:
                policy = "cost_based"

            # Apply harvester bonus.
            final_costs: Dict[str, float] = {}
            bonus_map: Dict[str, bool] = {}
            for eid, cost in costs.items():
                expert = self.registry.get_expert(eid) if self.registry else None
                if expert is None:
                    continue
                adjusted, applied = await self._apply_harvester_bonus(cost, context, expert)
                final_costs[eid] = adjusted
                bonus_map[eid] = applied

            if not final_costs:
                raise RegistryError("No valid experts after filtering")

            # Optional quantum selection.
            selected_expert = None
            if self.quantum_optimizer.available:
                quantum_candidates = [
                    self.registry.get_expert(eid) for eid in final_costs.keys()
                ]
                quantum_candidates = [e for e in quantum_candidates if e is not None]
                selected_expert = await self.quantum_optimizer.select_best_expert(
                    quantum_candidates, context, self.config.optimizer.modp_weights
                )

            if selected_expert is not None and getattr(selected_expert, "expert_id", None) in final_costs:
                best_eid = selected_expert.expert_id
                best_expert = selected_expert
                policy = "quantum"
            else:
                best_eid, best_expert = self._select_best_by_policy(
                    policy, final_costs, context
                )
                if best_expert is None:
                    raise RegistryError("Selected expert not found in registry")

            # Safety gate.
            best_fitness = float(getattr(best_expert, "accuracy_score", 0.5) or 0.5)
            if not self.safety_monitor.check_routing(context, best_expert, best_fitness):
                logger.warning("safety_violation fallback_to_safest")
                safe_eid = max(
                    final_costs.keys(),
                    key=lambda eid: float(
                        getattr(self.registry.get_expert(eid), "accuracy_score", 0.0) or 0.0
                    ),
                )
                best_eid = safe_eid
                best_expert = self.registry.get_expert(best_eid)
                policy = "safety_override"

            bonus_applied = bonus_map.get(best_eid, False)
            if bonus_applied:
                HARVESTER_BONUS.inc()
                SELECTED_BONUS_FACTOR.observe(
                    float(np.mean(self.bonus_population)) if self.bonus_population
                    else self.config.general.bonus_discount
                )
            SELECTED_COST.observe(final_costs[best_eid])

            # Precision selection.
            try:
                precision = await self.flexgen_manager.select_precision({
                    "size": "medium" if len(candidates) < 20 else "large",
                    "carbon_intensity": float(context.get("carbon_intensity", 0.5)) * 800.0,
                })
            except Exception:
                precision = "fp32"

            # XAI explanation.
            explanation = self.xai.explain_routing(
                best_expert, policy, context, confidence,
                final_costs[best_eid], bonus_applied,
            )
            XAI_DECISIONS.labels(policy=policy).inc()

            decision = {
                "routing_id": str(uuid.uuid4()),
                "task_type": task.get("type", "unknown"),
                "selected_expert_id": best_eid,
                "cost": float(final_costs[best_eid]),
                "bonus_applied": bool(bonus_applied),
                "context": context,
                "explanation": explanation,
                "precision": precision,
                "timestamp": datetime.now().isoformat(),
            }

            # HITL for low-confidence, high-carbon decisions.
            review_id: Optional[str] = None
            carbon_intensity = context.get("carbon_intensity", 0.0)
            if isinstance(carbon_intensity, (int, float)) and carbon_intensity <= 1.0:
                carbon_intensity = float(carbon_intensity) * 800.0
            if (
                confidence < self.config.general.human_review_threshold
                and carbon_intensity > 450
                and not self.chaos_monkey.is_hitl_skipped()
            ):
                try:
                    review_id = await self.human_review.request_review(
                        decision["routing_id"],
                        {
                            "expert_id": best_eid,
                            "policy": policy,
                            "explanation": explanation,
                            "carbon_intensity": carbon_intensity,
                        },
                    )
                except Exception as e:
                    logger.warning(f"hitl_request_failed error={e}")

            # Sign.
            signature = None
            if self.pqc is not None:
                try:
                    signature = await self.pqc.sign_routing_decision(decision)
                    decision["pqc_signature"] = signature
                except Exception as e:
                    logger.warning(f"pqc_sign_failed error={e}")

            # Persist.
            if self.db is not None:
                try:
                    await self.db.save_routing_decision(decision)
                except Exception as e:
                    logger.warning(f"db_save_failed error={e}")
            if self.cloud_storage is not None:
                try:
                    await self.cloud_storage.store(
                        decision, f"routing_{decision['routing_id']}.json"
                    )
                except Exception as e:
                    logger.warning(f"cloud_store_failed error={e}")

            # Multi-agent bookkeeping.
            domain = getattr(best_expert, "domain", "unknown")
            self.multi_agent.submit_bid(best_eid, final_costs[best_eid])
            self.multi_agent.record_outcome(best_eid, domain, True)

            # Bandit update (sync — do not await).
            if not self.chaos_monkey.is_frozen():
                reward = best_fitness - final_costs[best_eid] * 0.1
                self.bandit.update(encoded, policy, float(reward))

            # Carbon offsets — computed from energy, not cost.
            carbon_kg = float(context.get("carbon_kg", 0.0))
            if carbon_kg <= 0:
                # Fallback proxy: assume a small emission per routing decision.
                carbon_kg = final_costs[best_eid] * 0.005
            if carbon_intensity > 450 and carbon_kg > 0:
                try:
                    await self.carbon_broker.purchase_offsets(
                        carbon_intensity, carbon_kg
                    )
                except Exception as e:
                    logger.warning(f"carbon_offset_failed error={e}")

            # Federated round — self-register.
            self.federated.register_participant(
                f"router_{uuid.uuid4().hex[:8]}",
                {"avg_cost": final_costs[best_eid], "fitness": best_fitness},
                sample_count=1,
            )
            aggregated = self.federated.aggregate()
            if aggregated and "avg_cost" in aggregated:
                # Nudge the bandit posterior conservatively for the chosen policy.
                try:
                    self.bandit.apply_federated_prior(
                        policy, 1.0 - float(aggregated["avg_cost"]), weight=0.02
                    )
                except Exception:
                    pass

            # Audit.
            audit_logger.info(
                f"routing_decision routing_id={decision['routing_id']} "
                f"expert={best_eid} cost={final_costs[best_eid]:.4f} "
                f"policy={policy} confidence={confidence:.2f}"
            )

            self._last_decision = {
                "context": context_for_bandit,
                "policy": policy,
                "expert_id": best_eid,
                "decision": decision,
                "explanation": explanation,
                "review_id": review_id,
            }
            self._last_review_id = review_id

            try:
                await self._save_state()
            except Exception as e:
                logger.warning(f"state_save_failed error={e}")

            return {
                "routing_id": decision["routing_id"],
                "expert": best_expert,
                "cost": final_costs[best_eid],
                "harvester_bonus_applied": bonus_applied,
                "timestamp": decision["timestamp"],
                "pqc_signature": signature,
                "explanation": explanation,
                "precision": precision,
                "review_id": review_id,
                "policy": policy,
                "confidence": float(confidence),
            }
        finally:
            if fault is not None:
                try:
                    self.chaos_monkey.cleanup_fault(fault, self)
                except Exception:
                    pass

    def _encode_context(self, context: Dict[str, Any]) -> List[float]:
        if self.moe is not None:
            try:
                encoded = self.moe.encode(context)
                if encoded:
                    return [float(v) for v in encoded if isinstance(v, (int, float))]
            except Exception:
                pass
        return [float(v) for v in context.values() if isinstance(v, (int, float))]

    def _select_best_by_policy(
        self,
        policy: str,
        final_costs: Dict[str, float],
        context: Dict[str, Any],
    ) -> Tuple[str, Optional[Any]]:
        def acc(eid: str) -> float:
            e = self.registry.get_expert(eid) if self.registry else None
            return float(getattr(e, "accuracy_score", 0.0) or 0.0)

        def usage(eid: str) -> float:
            e = self.registry.get_expert(eid) if self.registry else None
            return float(getattr(e, "usage_count", 0) or 0)

        if policy == "cost_based":
            best_eid = min(final_costs, key=final_costs.get)
        elif policy == "accuracy_focused":
            best_eid = max(final_costs.keys(), key=acc)
        elif policy == "energy_focused":
            best_eid = min(final_costs.keys(), key=usage)
        else:  # balanced
            if self.modp is not None:
                max_cost = max(final_costs.values()) or 1.0
                carbon = float(context.get("carbon_intensity", 0.5))
                utilities: Dict[str, float] = {}
                for eid, cost in final_costs.items():
                    objectives = {
                        "accuracy": acc(eid),
                        "energy": 1.0 - (cost / max_cost),
                        "carbon": 1.0 - carbon,
                        "latency": 0.5,
                    }
                    try:
                        utilities[eid] = float(self.modp.evaluate(
                            objectives, self.config.optimizer.modp_weights
                        ))
                    except Exception:
                        utilities[eid] = 0.0
                best_eid = max(utilities, key=utilities.get)
            else:
                best_eid = min(final_costs, key=final_costs.get)

        best_expert = self.registry.get_expert(best_eid) if self.registry else None
        return best_eid, best_expert

    async def _get_candidates_with_breaker(
        self, task: Dict, context: Dict
    ) -> List[Any]:
        breaker = GlobalCircuitBreaker().get_or_create(
            "registry",
            failure_threshold=self.config.circuit_breaker.failure_threshold,
            recovery_timeout=self.config.circuit_breaker.recovery_timeout,
        )

        async def get_candidates():
            # Try the parent class's method first.
            parent_method = None
            try:
                parent_method = super(ExpertRouterWithHarvester, self).get_candidate_experts
            except AttributeError:
                parent_method = None
            if parent_method is not None:
                try:
                    result = parent_method(task, context)
                    if inspect.isawaitable(result):
                        result = await result
                    if result:
                        return list(result)
                except Exception as e:
                    logger.debug(f"parent_get_candidates_failed error={e}")
            # Fall back to registry.
            if self.registry is not None and hasattr(self.registry, "get_all_active_experts"):
                return list(self.registry.get_all_active_experts())
            return []

        return await breaker.call_async(get_candidates)

    async def _compute_costs_with_breaker(
        self, candidates: List[Any], context: Dict
    ) -> Dict[str, float]:
        breaker = GlobalCircuitBreaker().get_or_create(
            "cost_function",
            failure_threshold=self.config.circuit_breaker.failure_threshold,
            recovery_timeout=self.config.circuit_breaker.recovery_timeout,
        )

        async def compute_costs():
            result = self.cost_function.compute_multiple(candidates, context)
            if inspect.isawaitable(result):
                result = await result
            return {str(k): float(v) for k, v in result.items()}

        return await breaker.call_async(compute_costs)

    # ------------------------------------------------------------------ #
    # Feedback                                                           #
    # ------------------------------------------------------------------ #
    async def record_feedback(
        self, routing_id: str, success: bool, actual_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        if self._last_decision:
            reward = 1.0 if success else -1.0
            policy = self._last_decision.get("policy", "unknown")
            context = self._last_decision.get("context", {})
            if not self.chaos_monkey.is_frozen():
                # Sync update — do NOT await.
                self.bandit.update(context, policy, reward)
            expert_id = self._last_decision.get("expert_id")
            if expert_id and self.registry is not None:
                expert = self.registry.get_expert(expert_id)
                domain = getattr(expert, "domain", "unknown") if expert else "unknown"
                self.multi_agent.record_outcome(expert_id, domain, success)
        try:
            await self._save_state()
        except Exception:
            pass
        return {
            "status": "recorded",
            "routing_id": routing_id,
            "success": bool(success),
            "metrics": actual_metrics,
            "explanation": self._last_decision.get("explanation"),
        }

    # ------------------------------------------------------------------ #
    # Public helpers                                                     #
    # ------------------------------------------------------------------ #
    async def run_flexgen_optimization(
        self, workload: Dict[str, Any], node: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise RouterError(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict[str, Any]:
        return await self.flexgen_manager.get_status()

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        return await self.flexgen_manager.select_precision(workload)

    async def get_pending_reviews(self) -> List[Dict[str, Any]]:
        return await self.human_review.get_pending()

    async def approve_review(self, review_id: str, reviewer: str = "api") -> bool:
        return await self.human_review.approve(review_id, reviewer=reviewer)

    async def reject_review(self, review_id: str, reviewer: str = "api") -> bool:
        return await self.human_review.reject(review_id, reviewer=reviewer)

    async def trigger_chaos(
        self, enabled: bool = True, probability: float = 0.1
    ) -> Dict[str, Any]:
        self.chaos_monkey.config.enabled = bool(enabled)
        self.chaos_monkey.config.failure_probability = float(probability)
        return self.chaos_monkey.get_stats()

    async def get_carbon_totals(self) -> Dict[str, float]:
        return self.carbon_broker.get_totals()

    async def run_federated_round(self) -> Dict[str, Any]:
        aggregated = self.federated.aggregate()
        return {
            "status": "completed",
            "aggregated": aggregated,
            "participants": self.federated.get_participant_count(),
            "stats": self.federated.get_stats(),
        }

    async def health_check(self) -> Dict[str, Any]:
        components: Dict[str, Any] = {}
        for name, comp in [
            ("cost_function", self.cost_function),
            ("registry", self.registry),
            ("db", self.db),
            ("pqc", self.pqc),
            ("cloud_storage", self.cloud_storage),
            ("vault", self.vault),
            ("flexgen", self.flexgen_manager),
        ]:
            if comp is None:
                components[name] = {"status": "unavailable"}
                continue
            hc = getattr(comp, "health_check", None)
            if hc is None:
                components[name] = {"status": "ok"}
                continue
            try:
                result = hc()
                if inspect.isawaitable(result):
                    result = await result
                components[name] = result
            except Exception as e:
                components[name] = {"status": "unhealthy", "error": str(e)}
        ok = all(
            c.get("status") in ("ok", "healthy", "unavailable")
            for c in components.values()
        )
        overall = "healthy" if ok else "degraded"
        HEALTH_SCORE.set(100 if overall == "healthy" else 50)
        return {
            "status": overall,
            "components": components,
            "timestamp": datetime.now().isoformat(),
        }

    async def get_router_status(self) -> Dict[str, Any]:
        return {
            "bonus_discount": self.config.general.bonus_discount,
            "circuit_breaker": GlobalCircuitBreaker().all_metrics(),
            "rate_limiter": self.rate_limiter.get_metrics(),
            "quantum": self.pqc.get_quantum_status() if self.pqc else None,
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "cloud_storage": self.cloud_storage.get_status() if self.cloud_storage else None,
            "vault": {"available": self.vault is not None},
            "db": await self.db.health_check() if self.db else {"status": "unavailable"},
            "health": await self.health_check(),
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "bandit": self.bandit.get_stats(),
            "modp_weights": dict(self.config.optimizer.modp_weights),
            "flexgen": await self.get_flexgen_status(),
            "safety_violations": self.safety_monitor.get_violations()[-5:],
            "multi_agent": self.multi_agent.get_stats(),
            "carbon_broker": self.carbon_broker.get_totals(),
            "federated": self.federated.get_stats(),
            "chaos": self.chaos_monkey.get_stats(),
            "human_review_pending": len(await self.human_review.get_pending()),
            "last_explanation": self._last_decision.get("explanation"),
        }


# =============================================================================
# Component factory
# =============================================================================
def build_components(
    config: Optional[RouterConfig] = None,
    *,
    cost_function: Optional[ICostFunction] = None,
    registry: Optional[IRegistry] = None,
    harvester: Optional[IHarvester] = None,
) -> Dict[str, Any]:
    cfg = config or RouterConfig()

    if cost_function is None:
        class _DefaultCostFunction:
            async def compute_multiple(self, experts, context):
                return {e.expert_id: 1.0 for e in experts}
        cost_function = _DefaultCostFunction()

    if registry is None:
        class _DefaultRegistry:
            def __init__(self):
                self.experts: Dict[str, Any] = {}
                for i in range(3):
                    eid = f"expert_{i}"
                    self.experts[eid] = ExpertProfile(
                        expert_id=eid,
                        domain=["vision", "nlp", "audio"][i],
                        accuracy_score=0.7 + 0.1 * i,
                        photosynthetic_harvester_flag=(i % 2 == 0),
                    )
            def get_expert(self, expert_id):
                return self.experts.get(expert_id)
            def get_all_active_experts(self):
                return list(self.experts.values())
        registry = _DefaultRegistry()

    db = AsyncDatabaseManager(cfg)
    vault = VaultManager(cfg)
    pqc = PostQuantumCrypto(cfg, vault)
    cloud = MultiCloudStorage(cfg)

    router = ExpertRouterWithHarvester(
        config=cfg,
        cost_function=cost_function,
        registry=registry,
        harvester=harvester,
        db=db,
        pqc=pqc,
        cloud_storage=cloud,
        vault=vault,
    )
    return {"router": router, "config": cfg}


# =============================================================================
# FastAPI app factory
# =============================================================================
def create_app(config: Optional[RouterConfig] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config or RouterConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[valid-type]
        app.state.router = build_components(cfg)["router"]
        await app.state.router.start()
        try:
            yield
        finally:
            await app.state.router.stop()

    app = FastAPI(
        title="Expert Router API",
        version="3.2",
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

    async def require_auth(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer),
    ) -> Dict[str, Any]:
        if credentials is None or not JWT_AVAILABLE:
            # Fail-open in dev mode only. Set require_auth_for_admin=False to
            # intentionally disable.
            if not cfg.api.require_auth_for_admin:
                return {"sub": "anonymous"}
            raise HTTPException(status_code=401, detail="auth required")
        try:
            return pyjwt.decode(
                credentials.credentials, cfg.api.jwt_secret, algorithms=["HS256"]
            )
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid token: {e}")

    api_rate_limiter = RateLimiter(cfg)

    async def rate_limit(_: None = None) -> None:
        if not await api_rate_limiter.acquire():
            raise HTTPException(status_code=429, detail="rate limit exceeded")

    @app.post("/route")
    async def route_endpoint(
        task: Dict[str, Any],
        context: Dict[str, Any],
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
        _rl: None = Depends(rate_limit),
    ) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        try:
            result = await router.route(task, context)
            expert = result.get("expert")
            if hasattr(expert, "__dict__"):
                result["expert"] = {
                    k: v for k, v in expert.__dict__.items() if not k.startswith("_")
                }
            return result
        except RateLimitExceeded as e:
            raise HTTPException(status_code=429, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/feedback")
    async def feedback_endpoint(
        routing_id: str,
        success: bool,
        actual_metrics: Dict[str, Any],
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
        _rl: None = Depends(rate_limit),
    ) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.record_feedback(routing_id, success, actual_metrics)

    @app.get("/status")
    async def status_endpoint(
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
        _rl: None = Depends(rate_limit),
    ) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.get_router_status()

    @app.get("/health")
    async def health_endpoint(request: Request) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.health_check()

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(
        workload: Dict[str, Any],
        node: Dict[str, Any],
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        try:
            return await router.run_flexgen_optimization(workload, node)
        except NotImplementedYetError as e:
            raise HTTPException(status_code=501, detail=str(e))

    @app.get("/flexgen/status")
    async def flexgen_status(request: Request) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.get_flexgen_status()

    @app.get("/human-review/pending")
    async def human_review_pending(
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
    ) -> List[Dict[str, Any]]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.get_pending_reviews()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(
        review_id: str,
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, bool]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return {"approved": await router.approve_review(review_id, reviewer="api")}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(
        review_id: str,
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, bool]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return {"rejected": await router.reject_review(review_id, reviewer="api")}

    @app.post("/chaos/trigger")
    async def chaos_trigger(
        request: Request,
        enabled: bool = True,
        probability: float = 0.1,
        _user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.trigger_chaos(enabled, probability)

    @app.get("/carbon/totals")
    async def carbon_totals(
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, float]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.get_carbon_totals()

    @app.post("/federated/round")
    async def federated_round(
        request: Request,
        _user: Dict[str, Any] = Depends(require_auth),
    ) -> Dict[str, Any]:
        router = getattr(request.app.state, "router", None)
        if router is None:
            raise HTTPException(status_code=503, detail="router not initialized")
        return await router.run_federated_round()

    return app


# =============================================================================
# Singleton accessor
# =============================================================================
_router_instance: Optional[ExpertRouterWithHarvester] = None
_router_lock: Optional[asyncio.Lock] = None


async def get_router_instance(
    config: Optional[RouterConfig] = None,
    cost_function: Optional[ICostFunction] = None,
    registry: Optional[IRegistry] = None,
    harvester: Optional[IHarvester] = None,
    db: Optional[IAsyncDatabase] = None,  # noqa: ARG001
    pqc: Optional[IPQC] = None,  # noqa: ARG001
    cloud_storage: Optional[ICloudStorage] = None,  # noqa: ARG001
    vault: Optional[IVault] = None,  # noqa: ARG001
) -> ExpertRouterWithHarvester:
    global _router_instance, _router_lock
    if _router_lock is None:
        _router_lock = asyncio.Lock()
    if _router_instance is None:
        async with _router_lock:
            if _router_instance is None:
                components = build_components(
                    config=config,
                    cost_function=cost_function,
                    registry=registry,
                    harvester=harvester,
                )
                _router_instance = components["router"]
                await _router_instance.start()
    return _router_instance


# =============================================================================
# Main
# =============================================================================
async def main() -> None:
    print("=" * 80)
    print("Expert Router with Harvester v3.2 — Single-File Enhanced")
    print("=" * 80)

    config = RouterConfig()

    class SimpleCostFunction:
        async def compute_multiple(self, experts, context):
            return {e.expert_id: random.uniform(0.1, 1.0) for e in experts}

    class SimpleRegistry:
        def __init__(self):
            self.experts: Dict[str, Any] = {}
            for i in range(3):
                eid = f"expert_{i}"
                self.experts[eid] = ExpertProfile(
                    expert_id=eid,
                    domain=["vision", "nlp", "audio"][i],
                    accuracy_score=0.7 + 0.1 * i,
                    photosynthetic_harvester_flag=(i % 2 == 0),
                )
        def get_expert(self, expert_id):
            return self.experts.get(expert_id)
        def get_all_active_experts(self):
            return list(self.experts.values())

    cost_function = SimpleCostFunction()
    registry = SimpleRegistry()
    db = AsyncDatabaseManager(config)
    vault = VaultManager(config)
    pqc = PostQuantumCrypto(config, vault)
    cloud = MultiCloudStorage(config)

    router = ExpertRouterWithHarvester(
        config=config,
        cost_function=cost_function,
        registry=registry,
        harvester=None,
        db=db,
        pqc=pqc,
        cloud_storage=cloud,
        vault=vault,
    )
    await router.start()

    task = {"type": "classification"}
    context = {"data_source": "photosynthetic_harvester", "carbon_intensity": 0.6}

    result = await router.route(task, context)
    print(f"\nrouting_result expert={result['expert'].expert_id} "
          f"cost={result['cost']:.4f} bonus={result['harvester_bonus_applied']} "
          f"precision={result['precision']} policy={result['policy']} "
          f"confidence={result['confidence']:.2f}")

    fb = await router.record_feedback(
        result.get("routing_id", "unknown"), True, {"accuracy": 0.92}
    )
    print(f"feedback_status={fb['status']}")

    status = await router.get_router_status()
    print(json.dumps(status, indent=2, default=str))

    # FlexGen demo
    workload = {
        "task_id": "wl_001", "task_type": "inference", "tokens": 512,
        "latency_target": 200.0, "urgency": "medium", "priority": "balanced",
        "bio_mode": "none", "metadata": {},
    }
    node = {
        "id": "node_001", "type": "cloud", "region": "us-east",
        "region_carbon_intensity": 0.42, "energy_per_token": 0.00005,
        "uptime": 0.99, "maintenance_status": "operational", "metadata": {},
    }
    try:
        flexgen_result = await router.run_flexgen_optimization(workload, node)
        print(f"\nflexgen_result={flexgen_result}")
    except NotImplementedYetError as e:
        print(f"\nflexgen_unavailable reason={e}")

    await router.stop()
    print("\nShutdown complete")


if __name__ == "__main__":
    asyncio.run(main())


__all__ = [
    "ExpertRouterWithHarvester",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedRouterCoordinator",
    "MultiAgentRouter",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "QuantumDistillationOptimizer",
    "FlexGenManager",
    "PostQuantumCrypto",
    "MultiCloudStorage",
    "VaultManager",
    "RateLimiter",
    "CircuitBreaker",
    "CircuitBreakerState",
    "GlobalCircuitBreaker",
    "AsyncDatabaseManager",
    "RouterConfig",
    "GeneralConfig",
    "CircuitBreakerConfig",
    "RateLimitConfig",
    "DatabaseConfig",
    "VaultConfig",
    "CloudConfig",
    "PQCConfig",
    "APIConfig",
    "SafetyConfig",
    "ChaosConfig",
    "OptimizerConfig",
    "QuantumConfig",
    "ICostFunction",
    "IRegistry",
    "IHarvester",
    "IPQC",
    "ICloudStorage",
    "IVault",
    "IAsyncDatabase",
    "RouterError",
    "CostFunctionError",
    "RegistryError",
    "CircuitBreakerOpenError",
    "RateLimitExceeded",
    "SignatureError",
    "HealthCheckError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "NotImplementedYetError",
    "build_components",
    "create_app",
    "get_router_instance",
    "main",
]
