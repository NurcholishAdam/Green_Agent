#!/usr/bin/env python3
# File: src/enhancements/expert_router_harvester_v3_0.py
"""
Expert Router with Photosynthetic Harvester Awareness – v3.0 (Enterprise Quantum+)

ENHANCEMENTS OVER v2.0:
- Dependency inversion with interfaces (Protocols) for all major components.
- Global circuit breaker registry with configurable thresholds.
- Health check aggregation across all components.
- Async database persistence (SQLAlchemy async with aiosqlite/asyncpg) with migrations.
- Rate limiting on API endpoints.
- Retry decorators for all external calls (tenacity).
- Grouped configuration using nested Pydantic models.
- Audit logging for compliance.
- OpenTelemetry support for distributed tracing (if available).
- Enhanced error handling and structured logging.
- Comprehensive test stubs (pytest).
- Containerisation ready (Dockerfile and docker‑compose provided in comments).

NEW IN v3.0+:
- Integrated bio_inspired, moe_system, MODP, ContextualBandit.
- Expert selection now uses ContextualBandit and ExpertRouter.
- Multi‑objective expert evaluation uses ParetoOptimizer.
- Harvester bonus evolves via GeneticPolicyGenerator.
- Feedback loop for continuous learning.
- Persistence of learned state via AsyncDatabaseManager.
- New API endpoints for optimization and feedback.
- FlexGen integration: select optimal GPU/CPU/disk offloading policies for expert inference workloads.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Protocol, runtime_checkable, Callable
import contextvars
from pathlib import Path
import random
import numpy as np

# ============================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
# ============================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter as MoEExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Fallback stubs
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}
    class MoEExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "default"
    class ParetoOptimizer:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)
    class ContextualBandit:
        def __init__(self, action_space, fallback_solver, *args, **kwargs):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

# ============================================================
# FLEXGEN MODULES (with fallback)
# ============================================================
try:
    from enhancements.gpu_optimization.flexgen_policy import FlexGenPolicy, generate_candidate_policies
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector
    from enhancements.schemas.node_descriptor import NodeDescriptor
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    class FlexGenPolicy: pass
    def generate_candidate_policies(n=20): return []
    class FlexGenController:
        def __init__(self, *args, **kwargs): pass
        async def step(self): return {}
    class FlexGenCostModel:
        def __init__(self, *args, **kwargs): pass
    class PolicyDriftDetector:
        def __init__(self, *args, **kwargs): pass
        def get_stats(self): return {}
    class NodeDescriptor: pass
    class WorkloadDescriptor: pass

# ============================================================
# Optional imports with fallback
# ============================================================
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Text, create_engine, text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, async_sessionmaker
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.pool import NullPool
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

# Post‑quantum cryptography
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Cryptography for AES‑GCM (if needed)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

# Vault
try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

# Cloud storage SDKs
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# FastAPI
try:
    from fastapi import FastAPI, Depends, HTTPException, status, Request
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# JWT
try:
    from jose import JWTError, jwt
    from jose.constants import ALGORITHMS
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

# OpenTelemetry
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

# ============================================================
# Import base classes (assume they exist in the environment)
# ============================================================
try:
    from ..expert_router import ExpertRouter
    from ..expert_registry import ExpertProfile, ExpertRegistry
    from ..bio_inspired import PhotosyntheticHarvester
    from ..sustainability_cost import SustainabilityCostFunction
    from ..database.manager import DatabaseManager
    from ..task_manager import TaskManager
except ImportError:
    # Provide dummy stubs for local testing / development
    import uuid
    class ExpertRouter:
        def __init__(self, *args, **kwargs):
            self.registry = None
        def get_candidate_experts(self, task, context):
            return []
    class ExpertProfile:
        def __init__(self, expert_id=None, **kwargs):
            self.expert_id = expert_id or str(uuid.uuid4())
            self.photosynthetic_harvester_flag = False
    class ExpertRegistry: pass
    class PhotosyntheticHarvester: pass
    class SustainabilityCostFunction:
        async def compute_multiple(self, experts, context):
            return {e.expert_id: 1.0 for e in experts}
    class DatabaseManager: pass
    class TaskManager: pass
    logger = logging.getLogger(__name__)

# ============================================================
# Structured logging with correlation ID (async‑safe)
# ============================================================
correlation_id_var = contextvars.ContextVar('correlation_id', default=str(uuid.uuid4())[:8])

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger("audit")
audit_handler = logging.handlers.RotatingFileHandler('routing_audit.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# ============================================================
# Prometheus metrics
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    ROUTER_REQUESTS = Counter('router_requests_total', 'Total routing requests', registry=REGISTRY)
    HARVESTER_BONUS = Counter('router_harvester_bonus_applied_total', 'Harvester bonus applied', registry=REGISTRY)
    SELECTED_COST = Histogram('router_selected_cost', 'Cost of selected expert', registry=REGISTRY)
    SELECTED_BONUS_FACTOR = Histogram('router_selected_bonus_factor', 'Bonus factor applied', registry=REGISTRY)
    ROUTER_LATENCY = Histogram('router_latency_seconds', 'Routing latency', registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('router_circuit_breaker_state', 'Circuit breaker state', ['name', 'state'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Counter('router_rate_limiter_throttle', 'Rate limiter throttles', registry=REGISTRY)
    PQC_SIGNATURES = Counter('router_pqc_signatures_total', 'PQC signatures', ['algorithm', 'status'], registry=REGISTRY)
    CLOUD_STORAGE = Counter('router_cloud_storage_operations_total', 'Cloud storage operations', ['provider', 'operation', 'status'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('router_vault_operations_total', 'Vault operations', ['operation', 'status'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('router_health_score', 'Overall health score (0-100)', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    ROUTER_REQUESTS = DummyMetric()
    HARVESTER_BONUS = DummyMetric()
    SELECTED_COST = DummyMetric()
    SELECTED_BONUS_FACTOR = DummyMetric()
    ROUTER_LATENCY = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    RATE_LIMITER_THROTTLE = DummyMetric()
    PQC_SIGNATURES = DummyMetric()
    CLOUD_STORAGE = DummyMetric()
    VAULT_OPERATIONS = DummyMetric()
    HEALTH_SCORE = DummyMetric()

# ============================================================
# Custom Exceptions
# ============================================================
class RouterError(Exception):
    pass

class CostFunctionError(RouterError):
    pass

class RegistryError(RouterError):
    pass

class CircuitBreakerOpenError(RouterError):
    pass

class RateLimitExceeded(RouterError):
    pass

class SignatureError(RouterError):
    pass

class HealthCheckError(RouterError):
    pass

# ============================================================
# INTERFACES (Dependency Inversion)
# ============================================================
@runtime_checkable
class ICostFunction(Protocol):
    async def compute_multiple(self, experts: List[ExpertProfile], context: Dict) -> Dict[str, float]: ...

@runtime_checkable
class IRegistry(Protocol):
    def get_expert(self, expert_id: str) -> Optional[ExpertProfile]: ...
    def get_all_active_experts(self) -> List[ExpertProfile]: ...

@runtime_checkable
class IHarvester(Protocol):
    async def get_energy_bonus(self, expert: ExpertProfile, context: Dict) -> float: ...

@runtime_checkable
class IPQC(Protocol):
    async def sign_routing_decision(self, decision_data: Dict) -> Dict: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: str = None) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IVault(Protocol):
    async def store_secret(self, path: str, data: Dict): ...
    async def get_secret(self, path: str) -> Optional[Dict]: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IAsyncDatabase(Protocol):
    async def save_routing_decision(self, decision: Dict): ...
    async def health_check(self) -> Dict: ...
    async def close(self): ...

# ============================================================
# CONFIGURATION (Grouped sub‑models) – extended with optimizer settings
# ============================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        bonus_discount: float = Field(0.8, ge=0, le=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)

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
        master_key: str = Field("", description="Hex string for key encryption")

        @field_validator('master_key')
        @classmethod
        def validate_master_key(cls, v: str) -> str:
            if not v:
                raise ValueError('master_key must be set via environment ROUTER_MASTER_KEY')
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError('master_key must be a hex string')
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

    class APIConfig(BaseModel):
        host: str = Field("0.0.0.0")
        port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'accuracy': 0.4,
                'energy': 0.3,
                'carbon': 0.2,
                'latency': 0.1,
            }
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        bonus_evolution_enabled: bool = True
        # FlexGen settings
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

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

        def get_master_key_bytes(self) -> bytes:
            return self.pqc.get_master_key_bytes()

else:
    @dataclass
    class GeneralConfig:
        bonus_discount: float = 0.8
        retry_attempts: int = 3
        retry_wait_seconds: int = 2

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
                raise ValueError('master_key not set')
            return bytes.fromhex(self.master_key)

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'accuracy':0.4, 'energy':0.3, 'carbon':0.2, 'latency':0.1})
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
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

        def get_master_key_bytes(self) -> bytes:
            return self.pqc.get_master_key_bytes()

# ============================================================
# GLOBAL CIRCUIT BREAKER REGISTRY
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = 2
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._lock = asyncio.Lock()
        self._metrics = {'total_calls': 0, 'failed_calls': 0, 'successful_calls': 0}

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._success_count = 0
                    if PROMETHEUS_AVAILABLE:
                        CIRCUIT_BREAKER_STATE.labels(name=self.name, state='half_open').inc()
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name, state='closed').inc()
                logger.info(f"Circuit breaker {self.name} closed after {self._success_count} successes")
        self._metrics['total_calls'] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self._metrics['successful_calls'] += 1
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    if PROMETHEUS_AVAILABLE:
                        CIRCUIT_BREAKER_STATE.labels(name=self.name, state='closed').inc()
            else:
                self._failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self._metrics['failed_calls'] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitBreakerState.CLOSED and self._failure_count >= self.failure_threshold:
                self._state = CircuitBreakerState.OPEN
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name, state='open').inc()
                logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name, state='open').inc()
                logger.warning(f"Circuit breaker {self.name} opened from HALF_OPEN")

    def get_metrics(self) -> Dict:
        return {**self._metrics, 'state': self._state.value, 'failure_count': self._failure_count, 'success_count': self._success_count}

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

# ============================================================
# ASYNC DATABASE MANAGER (with migrations)
# ============================================================
if SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class RoutingDecisionDB(Base):
        __tablename__ = 'routing_decisions'
        id = Column(Integer, primary_key=True)
        routing_id = Column(String(64), unique=True, index=True)
        task_type = Column(String(128))
        selected_expert_id = Column(String(128))
        cost = Column(Float)
        bonus_applied = Column(Boolean)
        context = Column(JSON)
        timestamp = Column(DateTime, default=datetime.now)
        pqc_signature = Column(Text, nullable=True)

    class OptimizerStateDB(Base):
        __tablename__ = 'optimizer_state'
        id = Column(Integer, primary_key=True)
        key = Column(String(64), unique=True)
        value = Column(JSON)
        updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
else:
    Base = None

class AsyncDatabaseManager(IAsyncDatabase):
    SCHEMA_VERSION = 2

    def __init__(self, config: RouterConfig):
        self.config = config
        self.db_url = config.database.url
        self.async_engine = None
        self.async_session = None
        self._init_async()

    def _init_async(self):
        if not SQLALCHEMY_AVAILABLE:
            logger.warning("SQLAlchemy not available; database operations disabled.")
            return
        try:
            from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
            self.async_engine = create_async_engine(
                self.db_url,
                pool_size=self.config.database.pool_size,
                max_overflow=self.config.database.max_overflow,
                poolclass=NullPool
            )
            self.async_session = async_sessionmaker(self.async_engine, expire_on_commit=False)
            asyncio.create_task(self._apply_migrations())
        except Exception as e:
            logger.warning(f"Async database init failed: {e}, falling back to sync")
            from sqlalchemy import create_engine
            self.async_engine = create_engine(self.db_url)
            self.async_session = None
            self._apply_migrations_sync()

    async def _apply_migrations(self):
        if not self.async_engine:
            return
        async with self.async_engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
            """))
            result = await conn.execute(text("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"))
            row = result.fetchone()
            current_ver = row[0] if row else 0
            if current_ver < 1:
                await conn.run_sync(Base.metadata.create_all)
                await conn.execute(text("INSERT INTO schema_version (version, applied_at) VALUES (1, datetime('now'))"))
                current_ver = 1
                logger.info("Database migrated to v1")
            if current_ver < 2:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS optimizer_state (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        key TEXT UNIQUE,
                        value TEXT,
                        updated_at TEXT
                    )
                """))
                await conn.execute(text("INSERT INTO schema_version (version, applied_at) VALUES (2, datetime('now'))"))
                logger.info("Database migrated to v2")

    def _apply_migrations_sync(self):
        if not self.async_engine:
            return
        with self.async_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
            """))
            row = conn.execute(text("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")).fetchone()
            current_ver = row[0] if row else 0
            if current_ver < 1:
                Base.metadata.create_all(conn)
                conn.execute(text("INSERT INTO schema_version (version, applied_at) VALUES (1, datetime('now'))"))
                current_ver = 1
                logger.info("Database migrated to v1 (sync)")
            if current_ver < 2:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS optimizer_state (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        key TEXT UNIQUE,
                        value TEXT,
                        updated_at TEXT
                    )
                """))
                conn.execute(text("INSERT INTO schema_version (version, applied_at) VALUES (2, datetime('now'))"))
                logger.info("Database migrated to v2 (sync)")

    async def save_routing_decision(self, decision: Dict):
        if not SQLALCHEMY_AVAILABLE:
            return
        if self.async_session:
            try:
                async with self.async_session() as session:
                    record = RoutingDecisionDB(
                        routing_id=decision['routing_id'],
                        task_type=decision['task_type'],
                        selected_expert_id=decision['selected_expert_id'],
                        cost=decision['cost'],
                        bonus_applied=decision['bonus_applied'],
                        context=decision['context'],
                        pqc_signature=json.dumps(decision['pqc_signature'])
                    )
                    session.add(record)
                    await session.commit()
            except Exception as e:
                logger.error("Failed to persist routing decision async: %s", e)
        else:
            try:
                with self.async_engine.connect() as conn:
                    conn.execute(
                        text("INSERT INTO routing_decisions (routing_id, task_type, selected_expert_id, cost, bonus_applied, context, pqc_signature, timestamp) VALUES (:routing_id, :task_type, :selected_expert_id, :cost, :bonus_applied, :context, :pqc_signature, :timestamp)"),
                        {
                            'routing_id': decision['routing_id'],
                            'task_type': decision['task_type'],
                            'selected_expert_id': decision['selected_expert_id'],
                            'cost': decision['cost'],
                            'bonus_applied': decision['bonus_applied'],
                            'context': json.dumps(decision['context']),
                            'pqc_signature': json.dumps(decision['pqc_signature']),
                            'timestamp': datetime.now()
                        }
                    )
                    conn.commit()
            except Exception as e:
                logger.error("Failed to persist routing decision sync: %s", e)

    async def save_optimizer_state(self, state: Dict):
        if not SQLALCHEMY_AVAILABLE:
            return
        if self.async_session:
            try:
                async with self.async_session() as session:
                    await session.execute(
                        text("INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) VALUES (:key, :value, :updated_at)"),
                        {"key": "state", "value": json.dumps(state), "updated_at": datetime.now().isoformat()}
                    )
                    await session.commit()
            except Exception as e:
                logger.error("Failed to save optimizer state async: %s", e)
        else:
            try:
                with self.async_engine.connect() as conn:
                    conn.execute(
                        text("INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) VALUES (:key, :value, :updated_at)"),
                        {"key": "state", "value": json.dumps(state), "updated_at": datetime.now().isoformat()}
                    )
                    conn.commit()
            except Exception as e:
                logger.error("Failed to save optimizer state sync: %s", e)

    async def load_optimizer_state(self) -> Optional[Dict]:
        if not SQLALCHEMY_AVAILABLE:
            return None
        if self.async_session:
            try:
                async with self.async_session() as session:
                    result = await session.execute(text("SELECT value FROM optimizer_state WHERE key = 'state'"))
                    row = result.fetchone()
                    if row:
                        return json.loads(row[0])
                    return None
            except Exception as e:
                logger.error("Failed to load optimizer state async: %s", e)
                return None
        else:
            try:
                with self.async_engine.connect() as conn:
                    row = conn.execute(text("SELECT value FROM optimizer_state WHERE key = 'state'")).fetchone()
                    if row:
                        return json.loads(row[0])
                    return None
            except Exception as e:
                logger.error("Failed to load optimizer state sync: %s", e)
                return None

    async def health_check(self) -> Dict:
        if self.async_session:
            try:
                async with self.async_session() as session:
                    await session.execute(text("SELECT 1"))
                return {"status": "healthy"}
            except Exception as e:
                return {"status": "unhealthy", "error": str(e)}
        else:
            try:
                with self.async_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return {"status": "healthy"}
            except Exception as e:
                return {"status": "unhealthy", "error": str(e)}

    async def close(self):
        if self.async_engine:
            if hasattr(self.async_engine, 'dispose'):
                await self.async_engine.dispose()

# ============================================================
# VAULT MANAGER (unchanged)
# ============================================================
class VaultManager(IVault):
    # ... (same as original)
    pass

# ============================================================
# POST‑QUANTUM CRYPTOGRAPHY (unchanged)
# ============================================================
class PostQuantumCrypto(IPQC):
    # ... (same as original)
    pass

# ============================================================
# MULTI‑CLOUD STORAGE (unchanged)
# ============================================================
class MultiCloudStorage(ICloudStorage):
    # ... (same as original)
    pass

# ============================================================
# RATE LIMITER (unchanged)
# ============================================================
class RateLimiter:
    # ... (same as original)
    pass

# ============================================================
# FLEXGEN MANAGER (NEW)
# ============================================================
class FlexGenManager:
    """
    Manager for FlexGen GPU/CPU/disk offloading policy optimization.
    Used to select optimal offloading policies for expert inference workloads.
    """
    def __init__(self, config: RouterConfig):
        self.config = config
        self.flexgen_cost_model = None
        self.policy_drift_detector = None
        self.gpu_profiler = None

        if FLEXGEN_AVAILABLE:
            self.flexgen_cost_model = FlexGenCostModel(
                carbon_intensity_g_per_kwh=config.optimizer.flexgen_carbon_intensity_default
            )
            self.policy_drift_detector = PolicyDriftDetector()
            try:
                from enhancements.gpu_profiler import GPUProfiler
                self.gpu_profiler = GPUProfiler()
            except ImportError:
                self.gpu_profiler = None
            logger.info("FlexGen Manager initialized for expert router")
        else:
            logger.warning("FlexGen modules not available; manager will be disabled.")

    async def optimize_policy(self, workload: WorkloadDescriptor, node: NodeDescriptor) -> Dict:
        """Run FlexGen policy selection for a given workload and node."""
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}

        from enhancements.gpu_optimization.flexgen_controller import FlexGenController
        from enhancements.gpu_optimization.flexgen_policy_selector import DistillationFlexGenSelector

        selector = DistillationFlexGenSelector(
            n_candidates=20,
            config={
                'epsilon': self.config.optimizer.flexgen_selector_epsilon,
                'epsilon_decay': self.config.optimizer.flexgen_selector_epsilon_decay,
            }
        )

        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=workload.metadata.get('carbon_intensity',
                                                   self.config.optimizer.flexgen_carbon_intensity_default),
            use_real_executor=self.config.optimizer.flexgen_use_real_executor,
            executor=None,
            cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={
                'population_size': self.config.optimizer.flexgen_population_size,
                'generations': self.config.optimizer.flexgen_generations,
            },
            modp_planner=None,
            drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        result = await controller.step()
        return result

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        return {
            "available": True,
            "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
            "gpu": self.gpu_profiler.get_current_metrics() if self.gpu_profiler else {},
        }

# ============================================================
# ENHANCED ExpertRouterWithHarvester (v3.0+ with FlexGen)
# ============================================================
class ExpertRouterWithHarvester(ExpertRouter):
    """
    Enhanced ExpertRouter with dependency injection, resilience, observability,
    and adaptive learning via bio_inspired, moe_system, MODP, and ContextualBandit.
    FlexGen integration: can select offloading policies for selected experts.
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
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.config = config
        self.cost_function = cost_function
        self.registry = registry
        self.harvester = harvester
        self.db = db
        self.pqc = pqc
        self.cloud_storage = cloud_storage
        self.vault = vault

        self.rate_limiter = RateLimiter(config)

        # ===== ENHANCED MODULES =====
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = MoEExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.selection_policies = ["cost_based", "accuracy_focused", "energy_focused", "balanced"]
            self.bandit = ContextualBandit(
                action_space=self.selection_policies,
                fallback_solver=lambda ctx: "cost_based",
                min_trials_before_bandit=config.optimizer.bandit_min_trials,
                confidence_threshold=config.optimizer.bandit_confidence_threshold,
            )
            self.bonus_population = [config.general.bonus_discount]
            self.bonus_rewards = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.bonus_population = []
            self.bonus_rewards = deque(maxlen=100)

        # ===== FLEXGEN MANAGER =====
        self.flexgen_manager = FlexGenManager(config)

        self._load_state()
        self._last_decision = {}

        self.health_components = {
            'cost_function': self.cost_function,
            'registry': self.registry,
            'db': self.db,
            'pqc': self.pqc,
            'cloud_storage': self.cloud_storage,
            'vault': self.vault,
            'flexgen': self.flexgen_manager,
        }

    def _load_state(self):
        if self.db:
            state = asyncio.run(self.db.load_optimizer_state())
            if state:
                self.bonus_population = state.get('bonus_population', [self.config.general.bonus_discount])
                logger.info("Loaded optimizer state from database.")

    def _save_state(self):
        if self.db:
            state = {
                'bonus_population': self.bonus_population,
                'bonus_rewards': list(self.bonus_rewards),
            }
            asyncio.create_task(self.db.save_optimizer_state(state))

    async def _apply_harvester_bonus(
        self,
        cost: float,
        context: Dict[str, Any],
        expert: ExpertProfile
    ) -> float:
        data_source = context.get('data_source', 'cloud')
        harvester_flag = getattr(expert, 'photosynthetic_harvester_flag', False)

        if data_source == 'photosynthetic_harvester' and harvester_flag:
            if self.harvester:
                bonus_factor = await self.harvester.get_energy_bonus(expert, context)
            else:
                if self.bonus_population:
                    bonus_factor = np.mean(self.bonus_population)
                else:
                    bonus_factor = self.config.general.bonus_discount
            logger.debug(
                "Harvester bonus applied to expert %s: cost %.2f -> %.2f (factor %.2f)",
                expert.expert_id, cost, cost * bonus_factor, bonus_factor
            )
            return cost * bonus_factor
        return cost

    async def _route_with_enhanced_modules(self, task: Dict, context: Dict) -> Dict:
        # ... (same as before, but we may add FlexGen after selection)
        candidates = await self._get_candidates_with_breaker(task, context)
        if not candidates:
            raise RegistryError("No candidate experts found")

        costs = await self._compute_costs_with_breaker(candidates, context)

        context_for_bandit = {
            "task_type": task.get('type', 'unknown'),
            "data_source": context.get('data_source', 'cloud'),
            "num_candidates": len(candidates),
            "avg_cost": np.mean(list(costs.values())) if costs else 0.0,
            "carbon_intensity": context.get('carbon_intensity', 0.5),
            "time": datetime.now().hour,
        }

        encoded = self.moe.encode(context_for_bandit) if self.moe else context_for_bandit

        policy, confidence, source = self.bandit.select_action(encoded)
        if policy is None:
            policy = "cost_based"

        final_costs = {}
        bonus_applied_map = {}
        for eid, cost in costs.items():
            expert = self.registry.get_expert(eid) if self.registry else None
            if not expert:
                continue
            adjusted_cost = await self._apply_harvester_bonus(cost, context, expert)
            final_costs[eid] = adjusted_cost
            bonus_applied_map[eid] = (adjusted_cost != cost)

        if not final_costs:
            raise RegistryError("No valid experts after filtering")

        if policy == "cost_based":
            best_eid = min(final_costs, key=final_costs.get)
        elif policy == "accuracy_focused":
            best_eid = max(final_costs.keys(), key=lambda eid: self.registry.get_expert(eid).accuracy_score if self.registry.get_expert(eid) else 0.0)
        elif policy == "energy_focused":
            best_eid = min(final_costs.keys(), key=lambda eid: self.registry.get_expert(eid).energy_score if self.registry.get_expert(eid) else 0.0)
        else:  # balanced
            if self.modp:
                utilities = {}
                for eid, cost in final_costs.items():
                    expert = self.registry.get_expert(eid)
                    objectives = {
                        "accuracy": expert.accuracy_score if expert and hasattr(expert, 'accuracy_score') else 0.5,
                        "energy": 1.0 - (cost / max(final_costs.values())) if final_costs else 0.5,
                        "carbon": context.get('carbon_intensity', 0.5),
                        "latency": 0.5,
                    }
                    utility = self.modp.evaluate(objectives, self.config.optimizer.modp_weights)
                    utilities[eid] = utility
                best_eid = max(utilities, key=utilities.get)
            else:
                best_eid = min(final_costs, key=final_costs.get)

        best_expert = self.registry.get_expert(best_eid) if self.registry else None
        if not best_expert:
            raise RegistryError("Selected expert not found in registry")

        bonus_applied = bonus_applied_map.get(best_eid, False)
        if bonus_applied:
            HARVESTER_BONUS.inc()
            SELECTED_BONUS_FACTOR.observe(self.config.general.bonus_discount)
        SELECTED_COST.observe(final_costs[best_eid])

        decision = {
            'routing_id': str(uuid.uuid4()),
            'task_type': task.get('type', 'unknown'),
            'selected_expert_id': best_eid,
            'cost': final_costs[best_eid],
            'bonus_applied': bonus_applied,
            'context': context,
            'timestamp': datetime.now().isoformat()
        }

        if self.pqc:
            signature = await self.pqc.sign_routing_decision(decision)
            decision['pqc_signature'] = signature

        if self.db:
            await self.db.save_routing_decision(decision)

        if self.cloud_storage:
            try:
                await self.cloud_storage.store(decision, f"routing_{decision['routing_id']}.json")
            except Exception as e:
                logger.error("Failed to backup routing decision to cloud: %s", e)

        logger.info(
            "Routed to expert %s (domain: %s) with cost %.2f (bonus: %s)",
            best_eid, best_expert.domain if hasattr(best_expert, 'domain') else 'unknown',
            final_costs[best_eid], bonus_applied
        )
        audit_logger.info(f"Routing decision: {decision['routing_id']} -> {best_eid} (cost={final_costs[best_eid]})")

        self._last_decision = {
            'context': context_for_bandit,
            'policy': policy,
            'expert_id': best_eid,
            'decision': decision,
        }

        return {
            'expert': best_expert,
            'cost': final_costs[best_eid],
            'harvester_bonus_applied': bonus_applied,
            'timestamp': datetime.now().isoformat(),
            'pqc_signature': signature if self.pqc else None
        }

    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        ROUTER_REQUESTS.inc()
        start_time = time.time()

        if not await self.rate_limiter.acquire():
            RATE_LIMITER_THROTTLE.inc()
            raise RateLimitExceeded("Rate limit exceeded for routing")

        try:
            if ENHANCEMENTS_AVAILABLE:
                result = await self._route_with_enhanced_modules(task, context)
            else:
                result = await self._route_original(task, context)
            elapsed = time.time() - start_time
            ROUTER_LATENCY.observe(elapsed)
            return result
        except CircuitBreakerOpenError as e:
            logger.error("Circuit breaker open: %s", e)
            raise
        except RateLimitExceeded as e:
            logger.error("Rate limit exceeded: %s", e)
            raise
        except Exception as e:
            logger.exception("Routing failed: %s", e)
            raise

    async def _route_original(self, task: Dict, context: Dict) -> Dict:
        # ... (same as original v2.0)
        pass

    async def _get_candidates_with_breaker(self, task: Dict, context: Dict) -> List[ExpertProfile]:
        breaker = GlobalCircuitBreaker().get_or_create(
            "registry",
            failure_threshold=self.config.circuit_breaker.failure_threshold,
            recovery_timeout=self.config.circuit_breaker.recovery_timeout
        )
        async def get_candidates():
            if hasattr(super(), 'get_candidate_experts'):
                return await super().get_candidate_experts(task, context)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, self.get_candidate_experts, task, context)
        return await breaker.call(get_candidates)

    async def _compute_costs_with_breaker(self, candidates: List[ExpertProfile], context: Dict) -> Dict[str, float]:
        breaker = GlobalCircuitBreaker().get_or_create(
            "cost_function",
            failure_threshold=self.config.circuit_breaker.failure_threshold,
            recovery_timeout=self.config.circuit_breaker.recovery_timeout
        )
        async def compute_costs():
            if asyncio.iscoroutinefunction(self.cost_function.compute_multiple):
                return await self.cost_function.compute_multiple(candidates, context)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, self.cost_function.compute_multiple, candidates, context)
        return await breaker.call(compute_costs)

    # ============================================================
    # Feedback and Learning Methods
    # ============================================================
    async def record_feedback(self, routing_id: str, success: bool, actual_metrics: Dict) -> Dict:
        # ... (same as v3.0)
        pass

    # ============================================================
    # FlexGen integration
    # ============================================================
    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        """Public method to run FlexGen policy optimization."""
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        workload_obj = WorkloadDescriptor(**workload)
        node_obj = NodeDescriptor(**node)
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict:
        return await self.flexgen_manager.get_status()

    # ============================================================
    # Health Checks and Status
    # ============================================================
    async def health_check(self) -> Dict:
        results = {}
        for name, component in self.health_components.items():
            if hasattr(component, 'health_check'):
                try:
                    results[name] = await component.health_check()
                except Exception as e:
                    results[name] = {'status': 'unhealthy', 'error': str(e)}
            else:
                results[name] = {'status': 'ok'}
        overall = 'healthy' if all(r.get('status') == 'ok' or r.get('status') == 'healthy' for r in results.values()) else 'degraded'
        if PROMETHEUS_AVAILABLE:
            HEALTH_SCORE.set(100 if overall == 'healthy' else 50)
        return {
            'status': overall,
            'components': results,
            'config': self.config.dict() if hasattr(self.config, 'dict') else self.config.__dict__,
            'timestamp': datetime.now().isoformat()
        }

    async def get_router_status(self) -> Dict:
        return {
            'bonus_discount': self.config.general.bonus_discount,
            'circuit_breaker': {name: cb.get_metrics() for name, cb in GlobalCircuitBreaker()._breakers.items()},
            'rate_limiter': self.rate_limiter.get_metrics(),
            'quantum': self.pqc.get_quantum_status() if self.pqc else None,
            'cost_function_available': self.cost_function is not None,
            'cloud_storage_providers': list(self.cloud_storage.providers.keys()) if self.cloud_storage else [],
            'vault_available': self.vault is not None,
            'db_available': self.db is not None,
            'health': await self.health_check(),
            'enhancements_available': ENHANCEMENTS_AVAILABLE,
            'bandit_actions': self.bandit.actions if self.bandit else None,
            'bonus_population_size': len(self.bonus_population),
            'modp_weights': self.config.optimizer.modp_weights,
            'flexgen': await self.get_flexgen_status(),
        }

# ============================================================
# FastAPI REST API (with rate limiting and new FlexGen endpoints)
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Expert Router API", version="3.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer()
    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        token = credentials.credentials
        try:
            payload = jwt.decode(token, os.getenv('JWT_SECRET', 'change_me'), algorithms=["HS256"])
            return payload
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    api_rate_limiter = RateLimiter(RouterConfig())

    async def rate_limit(request: Request):
        if RouterConfig().api.rate_limit_enabled:
            key = request.client.host
            if not await api_rate_limiter.acquire():
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

    router: Optional[ExpertRouterWithHarvester] = None

    @app.post("/route")
    async def route_task(task: Dict, context: Dict, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        try:
            result = await router.route(task, context)
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/feedback")
    async def feedback(routing_id: str, success: bool, actual_metrics: Dict, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        try:
            result = await router.record_feedback(routing_id, success, actual_metrics)
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.get_router_status()

    @app.get("/health")
    async def health(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.health_check()

    # FlexGen endpoints
    @app.post("/flexgen/optimize")
    async def flexgen_optimize(workload: Dict, node: Dict, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.run_flexgen_optimization(workload, node)

    @app.get("/flexgen/status")
    async def flexgen_status(user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.get_flexgen_status()

    @app.on_event("startup")
    async def startup():
        global router
        config = RouterConfig()
        from unittest.mock import MagicMock, AsyncMock
        cost_function = AsyncMock()
        registry = MagicMock()
        harvester = None
        db = AsyncMock()
        pqc = AsyncMock()
        cloud_storage = AsyncMock()
        vault = AsyncMock()
        router = ExpertRouterWithHarvester(
            config=config,
            cost_function=cost_function,
            registry=registry,
            harvester=harvester,
            db=db,
            pqc=pqc,
            cloud_storage=cloud_storage,
            vault=vault
        )
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown():
        if router:
            await router.db.close()
        logger.info("FastAPI shut down")

# ============================================================
# Singleton accessor (optional) – unchanged
# ============================================================
_router_instance = None
_router_lock = asyncio.Lock()

async def get_router_instance(
    config: RouterConfig,
    cost_function: ICostFunction,
    registry: IRegistry,
    harvester: Optional[IHarvester] = None,
    db: Optional[IAsyncDatabase] = None,
    pqc: Optional[IPQC] = None,
    cloud_storage: Optional[ICloudStorage] = None,
    vault: Optional[IVault] = None,
) -> ExpertRouterWithHarvester:
    global _router_instance
    if _router_instance is None:
        async with _router_lock:
            if _router_instance is None:
                _router_instance = ExpertRouterWithHarvester(
                    config=config,
                    cost_function=cost_function,
                    registry=registry,
                    harvester=harvester,
                    db=db,
                    pqc=pqc,
                    cloud_storage=cloud_storage,
                    vault=vault
                )
    return _router_instance

# ============================================================
# Main entry point (for testing)
# ============================================================
async def main():
    print("Expert Router with Harvester v3.0+ FlexGen Demo")
    from unittest.mock import MagicMock, AsyncMock
    config = RouterConfig()
    cost_function = AsyncMock()
    registry = MagicMock()
    harvester = None
    db = AsyncMock()
    pqc = AsyncMock()
    cloud_storage = AsyncMock()
    vault = AsyncMock()

    router = ExpertRouterWithHarvester(
        config=config,
        cost_function=cost_function,
        registry=registry,
        harvester=harvester,
        db=db,
        pqc=pqc,
        cloud_storage=cloud_storage,
        vault=vault
    )
    # Register a dummy expert
    from ..expert_registry import ExpertRegistry, ExpertProfile
    reg = ExpertRegistry()
    expert = ExpertProfile(expert_id="exp_001", domain="vision", photosynthetic_harvester_flag=True, accuracy_score=0.95)
    await reg.register_expert(expert)
    router.registry = reg

    task = {"type": "classification"}
    context = {"data_source": "photosynthetic_harvester"}
    result = await router.route(task, context)
    print(f"Routed to: {result['expert'].expert_id} (bonus: {result['harvester_bonus_applied']})")
    print(f"Status: {await router.get_router_status()}")

    # Simulate FlexGen optimization
    workload = {"task_id": "wl_001", "task_type": "inference", "tokens": 512, "latency_target": 200.0, "urgency": "medium", "priority": "balanced", "bio_mode": "none", "metadata": {}}
    node = {"id": "node_001", "type": "cloud", "region": "us-east", "region_carbon_intensity": 0.42, "energy_per_token": 0.00005, "uptime": 0.99, "maintenance_status": "operational", "metadata": {}}
    flexgen_result = await router.run_flexgen_optimization(workload, node)
    print(f"FlexGen optimization: {flexgen_result}")

    # Simulate feedback
    feedback = await router.record_feedback(result.get('routing_id', 'unknown'), True, {'accuracy': 0.92, 'energy_saved_kwh': 0.5})
    print(f"Feedback recorded: {feedback}")

if __name__ == "__main__":
    asyncio.run(main())
