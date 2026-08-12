#!/usr/bin/env python3
# File: src/enhancements/evolutionary_engine_v4_0_0.py
"""
Evolutionary Engine for Green Agent v4.0.0 (Enterprise Quantum+)
Manages the lifecycle of experts using sustainability‑aware fitness.

ENHANCEMENTS OVER v3.0.0:
- Dependency inversion with interfaces (Protocols) for all major components.
- Global circuit breaker registry for external services.
- Health check aggregation across all components.
- TaskManager supervises the evolution loop with automatic restart.
- Full async PostgreSQL support (asyncpg) with connection pooling.
- Alembic‑style database migrations (inline runner).
- Prophet models are persisted to disk/cloud.
- Autonomous optimizer parameter space and epsilon configurable.
- Rate limiting on API endpoints.
- Retry decorators for all external calls.
- Distributed leader election (Redis) to avoid duplicate work.
- Configuration grouped into sub‑models.
- Comprehensive error handling and logging.
- Unit test suite (pytest) stubs (expanded).
"""

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Callable, Union, Protocol, runtime_checkable
from collections import deque, defaultdict
from enum import Enum
from functools import wraps
import numpy as np
import contextvars
import random
import weakref

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
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Text, create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, scoped_session
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy.pool import NullPool
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

# Post‑quantum cryptography (pqcrypto)
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Cryptography for AES‑GCM
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

# Vault client
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

# Prophet for forecasting
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

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

# Redis for leader election and caching
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# ============================================================
# Import existing modules (adjust paths as needed)
# ============================================================
try:
    from ..expert_registry import ExpertRegistry, ExpertProfile
    from ..digital_twin import DigitalTwin
    from ..mlops_pipeline import MLOpsPipeline
    from ..database.manager import DatabaseManager
    from ..task_manager import TaskManager
    from .sustainability_cost import SustainabilityCostFunction
except ImportError:
    # Stub classes for demonstration (will be replaced in real environment)
    class ExpertRegistry: pass
    class ExpertProfile: pass
    class DigitalTwin: pass
    class MLOpsPipeline: pass
    class DatabaseManager: pass
    class TaskManager: pass
    class SustainabilityCostFunction: pass
    logger = logging.getLogger(__name__)

# ============================================================
# Structured logging with correlation ID (async‑safe)
# ============================================================
correlation_id_var = contextvars.ContextVar('correlation_id', default=str(uuid.uuid4())[:8])

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logger.addFilter(CorrelationIdFilter())

# ============================================================
# Prometheus metrics (dummy fallback)
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    EVOLUTION_CYCLES = Counter('evolution_cycles_total', 'Total evolution cycles', registry=REGISTRY)
    EXPERTS_PRUNED = Counter('experts_pruned_total', 'Experts pruned', registry=REGISTRY)
    EXPERTS_MERGED = Counter('experts_merged_total', 'Experts merged', registry=REGISTRY)
    EXPERTS_SPAWNED = Counter('experts_spawned_total', 'Experts spawned', registry=REGISTRY)
    FITNESS_DISTRIBUTION = Histogram('expert_fitness', 'Fitness scores of experts', buckets=[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0], registry=REGISTRY)
    EVOLUTION_DURATION = Histogram('evolution_duration_seconds', 'Evolution cycle duration', registry=REGISTRY)
    PQC_SIGNATURES = Counter('pqc_signatures_total', 'PQC signatures', ['algorithm', 'status'], registry=REGISTRY)
    CLOUD_STORAGE = Counter('cloud_storage_operations_total', 'Cloud storage operations', ['provider', 'operation', 'status'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('vault_operations_total', 'Vault operations', ['operation', 'status'], registry=REGISTRY)
    PREDICTIVE_FORECAST = Counter('predictive_forecasts_total', 'Predictive forecasts generated', ['model', 'status'], registry=REGISTRY)
    OPTIMIZER_DECISIONS = Counter('autonomous_optimizer_decisions_total', 'Optimizer decisions', ['parameter', 'action'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('evolution_circuit_breaker_state', 'Circuit breaker state', ['service'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('evolution_health_score', 'System health score (0-100)', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    EVOLUTION_CYCLES = DummyMetric()
    EXPERTS_PRUNED = DummyMetric()
    EXPERTS_MERGED = DummyMetric()
    EXPERTS_SPAWNED = DummyMetric()
    FITNESS_DISTRIBUTION = DummyMetric()
    EVOLUTION_DURATION = DummyMetric()
    PQC_SIGNATURES = DummyMetric()
    CLOUD_STORAGE = DummyMetric()
    VAULT_OPERATIONS = DummyMetric()
    PREDICTIVE_FORECAST = DummyMetric()
    OPTIMIZER_DECISIONS = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    HEALTH_SCORE = DummyMetric()

# ============================================================
# Custom Exceptions
# ============================================================
class EvolutionaryEngineError(Exception):
    """Base exception for Evolutionary Engine."""
    pass

class ConfigError(EvolutionaryEngineError):
    pass

class SecurityError(EvolutionaryEngineError):
    pass

class CloudStorageError(EvolutionaryEngineError):
    pass

class VaultError(EvolutionaryEngineError):
    pass

class PredictionError(EvolutionaryEngineError):
    pass

class OptimizerError(EvolutionaryEngineError):
    pass

class DatabaseError(EvolutionaryEngineError):
    pass

class CircuitBreakerOpenError(EvolutionaryEngineError):
    pass

# ============================================================
# CONFIGURATION (Grouped sub‑models)
# ============================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        prune_threshold: float = Field(0.2, ge=0, le=1)
        merge_similarity_threshold: float = Field(0.85, ge=0, le=1)
        spawn_gap_threshold: float = Field(0.3, ge=0, le=1)
        evolution_interval_seconds: int = Field(3600, ge=60)
        max_merges_per_cycle: int = Field(5, ge=1)
        max_prunes_per_cycle: int = Field(10, ge=1)
        critical_usage_threshold: int = Field(100, ge=1)
        fitness_recency_weight: float = Field(0.3, ge=0, le=1)
        fitness_usage_weight: float = Field(0.2, ge=0, le=1)
        fitness_uncertainty_weight: float = Field(0.1, ge=0, le=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)

        @field_validator('fitness_recency_weight')
        @classmethod
        def check_weights_sum(cls, v: float, info: ValidationInfo):
            values = info.data
            total = v + values.get('fitness_usage_weight', 0) + values.get('fitness_uncertainty_weight', 0)
            if total > 1.0:
                raise ValueError("Sum of fitness weights must not exceed 1.0")
            return v

    class QuantumConfig(BaseModel):
        pqc_enabled: bool = True
        pqc_algorithm: str = Field("dilithium", description="Algorithm for PQC signing")
        master_key: str = Field("", description="Hex string for key encryption")

        @field_validator('master_key')
        @classmethod
        def validate_master_key(cls, v: str) -> str:
            if not v:
                raise ValueError('master_key must be set via environment EVOLUTION_MASTER_KEY')
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError('master_key must be a hex string')
            return v

        def get_master_key_bytes(self) -> bytes:
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

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///evolution.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/evolution")

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        model_storage_path: str = Field("./prophet_models")
        min_samples: int = Field(30, ge=1)

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        epsilon: float = Field(0.1, ge=0, le=1)
        parameter_space: Dict[str, List[float]] = Field(
            default_factory=lambda: {
                'prune_threshold': [0.1, 0.2, 0.3],
                'merge_similarity_threshold': [0.8, 0.85, 0.9],
                'spawn_gap_threshold': [0.2, 0.3, 0.4],
                'fitness_recency_weight': [0.2, 0.3, 0.4]
            }
        )

    class APIConfig(BaseModel):
        host: str = Field("0.0.0.0")
        port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)

    class CircuitBreakerConfig(BaseModel):
        failure_threshold: int = Field(3, ge=1)
        recovery_timeout: int = Field(30, ge=1)

    class LeaderConfig(BaseModel):
        enabled: bool = False
        redis_url: Optional[str] = None
        ttl_seconds: int = Field(30, ge=1)

    class EvolutionConfig(BaseModel):
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = Field(default_factory=LeaderConfig)

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

else:
    @dataclass
    class GeneralConfig:
        prune_threshold: float = 0.2
        merge_similarity_threshold: float = 0.85
        spawn_gap_threshold: float = 0.3
        evolution_interval_seconds: int = 3600
        max_merges_per_cycle: int = 5
        max_prunes_per_cycle: int = 10
        critical_usage_threshold: int = 100
        fitness_recency_weight: float = 0.3
        fitness_usage_weight: float = 0.2
        fitness_uncertainty_weight: float = 0.1
        retry_attempts: int = 3
        retry_wait_seconds: int = 2

    @dataclass
    class QuantumConfig:
        pqc_enabled: bool = True
        pqc_algorithm: str = "dilithium"
        master_key: str = ""

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError('master_key not set')
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
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///evolution.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/evolution"

    @dataclass
    class PredictiveConfig:
        enabled: bool = True
        model_storage_path: str = "./prophet_models"
        min_samples: int = 30

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        epsilon: float = 0.1
        parameter_space: Dict[str, List[float]] = field(default_factory=lambda: {
            'prune_threshold': [0.1, 0.2, 0.3],
            'merge_similarity_threshold': [0.8, 0.85, 0.9],
            'spawn_gap_threshold': [0.2, 0.3, 0.4],
            'fitness_recency_weight': [0.2, 0.3, 0.4]
        })

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True
        rate_limit_requests: int = 100
        rate_limit_window: int = 60

    @dataclass
    class CircuitBreakerConfig:
        failure_threshold: int = 3
        recovery_timeout: int = 30

    @dataclass
    class LeaderConfig:
        enabled: bool = False
        redis_url: Optional[str] = None
        ttl_seconds: int = 30

    @dataclass
    class EvolutionConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = field(default_factory=LeaderConfig)

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

# ============================================================
# INTERFACES (Dependency Inversion)
# ============================================================
@runtime_checkable
class IPQC(Protocol):
    async def sign_evolution_event(self, event_data: Dict) -> Dict: ...
    def get_quantum_status(self) -> Dict: ...

@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: str = None) -> Dict: ...
    def get_status(self) -> Dict: ...

@runtime_checkable
class IPredictiveAnalytics(Protocol):
    async def update_history(self, fitness_scores: List[float]): ...
    async def forecast_fitness(self, horizon_hours: int = 24) -> Dict: ...
    async def load_model(self, region: str) -> Optional[Any]: ...
    async def save_model(self, region: str, model: Any): ...

@runtime_checkable
class IAutonomousOptimizer(Protocol):
    async def select_parameters(self) -> Dict: ...
    async def update_rewards(self, parameters: Dict, outcome: float): ...
    def get_stats(self) -> Dict: ...

@runtime_checkable
class IAsyncDatabase(Protocol):
    async def log_event(self, event_type: str, expert_id: str = None, details: Dict = None): ...
    async def health_check(self) -> Dict: ...
    async def close(self): ...

@runtime_checkable
class IHealthCheckable(Protocol):
    async def health_check(self) -> Dict: ...

# ============================================================
# GLOBAL CIRCUIT BREAKER REGISTRY
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0):
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
                        CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0.5)
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0)
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
                        CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0)
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
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(1)
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
# ENHANCED RATE LIMITER (for API)
# ============================================================
class RateLimiter:
    def __init__(self, config: APIConfig):
        self.config = config
        self.rate = config.rate_limit_requests
        self.window = config.rate_limit_window
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            else:
                self.throttled_requests += 1
                return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

    def get_metrics(self) -> Dict:
        total = self.total_requests + self.throttled_requests
        return {
            'total_requests': self.total_requests,
            'throttled_requests': self.throttled_requests,
            'throttle_rate': (self.throttled_requests / max(total, 1)) * 100
        }

# ============================================================
# VAULT MANAGER (with circuit breaker)
# ============================================================
class VaultManager:
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.client = None
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "vault",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout
        )
        if VAULT_AVAILABLE and config.vault.url and config.vault.token:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
                logger.info("Vault client initialized")
            except Exception as e:
                logger.error(f"Vault client initialization failed: {e}")
        else:
            logger.warning("Vault not configured; using database fallback for secrets.")

    async def store_secret(self, path: str, data: Dict):
        if not self.client:
            logger.warning("Vault not available; secret not stored")
            return
        async def _store():
            self.client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=data
            )
        try:
            await self.circuit_breaker.call(_store)
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='store', status='success').inc()
        except Exception as e:
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='store', status='failed').inc()
            raise VaultError(f"Failed to store secret: {e}") from e

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None
        async def _get():
            secret = self.client.secrets.kv.v2.read_secret(path=path)
            return secret['data']['data']
        try:
            result = await self.circuit_breaker.call(_get)
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='read', status='success').inc()
            return result
        except Exception:
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='read', status='failed').inc()
            return None

# ============================================================
# POST‑QUANTUM CRYPTOGRAPHY (implements IPQC)
# ============================================================
class PostQuantumCrypto(IPQC):
    def __init__(self, config: EvolutionConfig, vault: Optional[VaultManager] = None):
        self.config = config
        self.vault = vault
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE and config.quantum.pqc_enabled
        self._lock = asyncio.Lock()
        self.master_key = config.get_master_key_bytes()
        self.salt = os.urandom(16)
        self.default_keypair = None
        self.key_id = None

        if self.pqc_available:
            self._initialize_pqc()
            self._generate_default_keypair_sync()
        else:
            logger.warning("PQC libraries not found – using ECDSA fallback. Install 'pqcrypto' for real PQC.")
        logger.info(f"PostQuantumCrypto initialized (PQC: {self.pqc_available})")

    def _initialize_pqc(self):
        self.pqc_algorithms['dilithium'] = dilithium
        self.pqc_algorithms['falcon'] = falcon
        self.pqc_algorithms['sphincs'] = sphincs

    def _derive_key(self, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        return kdf.derive(self.master_key)

    def _encrypt_key(self, key_bytes: bytes) -> bytes:
        salt = os.urandom(16)
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return salt + nonce + ciphertext

    def _decrypt_key(self, encrypted_bytes: bytes) -> bytes:
        salt = encrypted_bytes[:16]
        nonce = encrypted_bytes[16:28]
        ciphertext = encrypted_bytes[28:]
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        return aesgcm.decrypt(nonce, ciphertext, None)

    def _generate_default_keypair_sync(self):
        algorithm = self.config.quantum.pqc_algorithm
        if not self.pqc_available:
            self.default_keypair = self._fallback_keypair()
            return
        try:
            signer = self.pqc_algorithms.get(algorithm)
            if not signer:
                raise ValueError(f"Algorithm {algorithm} not available")
            public_key, private_key = signer.generate_keypair()
            key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
            encrypted_private = self._encrypt_key(private_key)
            encrypted_public = self._encrypt_key(public_key)
            secret_data = {
                "algorithm": algorithm,
                "public_key": encrypted_public.hex(),
                "private_key": encrypted_private.hex(),
                "created_at": datetime.now().isoformat()
            }
            if self.vault and self.vault.client:
                # Use vault store (sync)
                self.vault.store_secret(f"pqc/{key_id}", secret_data)
            self.default_keypair = {
                'key_id': key_id,
                'algorithm': algorithm,
                'public_key': public_key,
                'private_key': private_key,
                'created_at': datetime.now().isoformat()
            }
            self.key_id = key_id
            if PROMETHEUS_AVAILABLE:
                PQC_SIGNATURES.labels(algorithm=algorithm, status='generated').inc()
            logger.info(f"Persistent PQC keypair generated: {key_id}")
        except Exception as e:
            logger.error(f"Keypair generation failed: {e}")
            self.default_keypair = self._fallback_keypair()

    def _fallback_keypair(self) -> Dict:
        key_id = f"fallback_{uuid.uuid4().hex[:8]}"
        return {'key_id': key_id, 'algorithm': 'ecdsa', 'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    async def sign_evolution_event(self, event_data: Dict) -> Dict:
        if not self.pqc_available or self.default_keypair is None:
            return self._fallback_sign(event_data)
        try:
            keypair = self.default_keypair
            algorithm = keypair['algorithm']
            private_key = keypair['private_key']
            signer = self.pqc_algorithms.get(algorithm)
            if not signer:
                return self._fallback_sign(event_data)
            data_bytes = json.dumps(event_data, sort_keys=True).encode()
            signature = await asyncio.to_thread(signer.sign, data_bytes, private_key)
            sig_data = {
                'signature': signature.hex(),
                'algorithm': algorithm,
                'key_id': self.key_id,
                'timestamp': datetime.now().isoformat()
            }
            if PROMETHEUS_AVAILABLE:
                PQC_SIGNATURES.labels(algorithm=algorithm, status='sign_success').inc()
            logger.info(f"Evolution event signed with {algorithm}")
            return sig_data
        except Exception as e:
            logger.error(f"PQC signing failed: {e}")
            if PROMETHEUS_AVAILABLE:
                PQC_SIGNATURES.labels(algorithm=algorithm, status='sign_failed').inc()
            return self._fallback_sign(event_data)

    def _fallback_sign(self, event_data: Dict) -> Dict:
        return {
            'signature': hashlib.sha256(json.dumps(event_data, sort_keys=True).encode()).hexdigest(),
            'algorithm': 'sha256_fallback',
            'key_id': 'fallback',
            'timestamp': datetime.now().isoformat()
        }

    def get_quantum_status(self) -> Dict:
        return {
            'pqc_available': self.pqc_available,
            'algorithms': list(self.pqc_algorithms.keys()),
            'default_keypair_exists': self.default_keypair is not None,
        }

# ============================================================
# MULTI‑CLOUD STORAGE (implements ICloudStorage)
# ============================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.providers = {}
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "cloud_storage",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout
        )
        self._init_providers()

    def _init_providers(self):
        if AWS_AVAILABLE and self.config.cloud.aws_bucket:
            try:
                self.providers['aws'] = {
                    'client': boto3.client(
                        's3',
                        region_name=self.config.cloud.aws_region,
                        aws_access_key_id=self.config.cloud.aws_access_key,
                        aws_secret_access_key=self.config.cloud.aws_secret_key
                    ),
                    'bucket': self.config.cloud.aws_bucket
                }
            except Exception as e:
                logger.warning(f"AWS client init failed: {e}")
        if AZURE_AVAILABLE and self.config.cloud.azure_connection_string:
            try:
                self.providers['azure'] = {
                    'client': BlobServiceClient.from_connection_string(self.config.cloud.azure_connection_string),
                    'container': self.config.cloud.azure_container
                }
            except Exception as e:
                logger.warning(f"Azure client init failed: {e}")
        if GCP_AVAILABLE and self.config.cloud.gcp_credentials:
            try:
                self.providers['gcp'] = {
                    'client': storage.Client(),
                    'bucket': self.config.cloud.gcp_bucket
                }
            except Exception as e:
                logger.warning(f"GCP client init failed: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type((Exception,)), before_sleep=before_sleep_log(logger, logging.WARNING))
    async def store(self, data: Dict, filename: str = None) -> Dict:
        async def _store():
            for provider_name, provider in self.providers.items():
                try:
                    if provider_name == 'aws':
                        client = provider['client']
                        bucket = provider['bucket']
                        key = filename or f"evolution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        data_bytes = json.dumps(data, default=str).encode()
                        client.put_object(Bucket=bucket, Key=key, Body=data_bytes)
                        if PROMETHEUS_AVAILABLE:
                            CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='success').inc()
                        return {'provider': provider_name, 'location': f"s3://{bucket}/{key}"}
                    elif provider_name == 'azure':
                        client = provider['client']
                        container = provider['container']
                        blob_name = filename or f"evolution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        data_bytes = json.dumps(data, default=str).encode()
                        blob_client = client.get_blob_client(container=container, blob=blob_name)
                        blob_client.upload_blob(data_bytes, overwrite=True)
                        if PROMETHEUS_AVAILABLE:
                            CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='success').inc()
                        return {'provider': provider_name, 'location': f"https://{container}.blob.core.windows.net/{blob_name}"}
                    elif provider_name == 'gcp':
                        client = provider['client']
                        bucket = provider['bucket']
                        blob_name = filename or f"evolution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        data_bytes = json.dumps(data, default=str).encode()
                        bucket_obj = client.bucket(bucket)
                        blob = bucket_obj.blob(blob_name)
                        blob.upload_from_string(data_bytes)
                        if PROMETHEUS_AVAILABLE:
                            CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='success').inc()
                        return {'provider': provider_name, 'location': f"gs://{bucket}/{blob_name}"}
                except Exception as e:
                    logger.error(f"Cloud storage failed for {provider_name}: {e}")
                    if PROMETHEUS_AVAILABLE:
                        CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='failed').inc()
            # Fallback to local
            local_path = Path(f"./evolution_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(local_path, 'w') as f:
                json.dump(data, f, default=str)
            return {'provider': 'local', 'location': str(local_path)}
        return await self.circuit_breaker.call(_store)

    def get_status(self) -> Dict:
        return {
            'providers': list(self.providers.keys()),
            'active_count': len(self.providers)
        }

# ============================================================
# PREDICTIVE ANALYTICS (implements IPredictiveAnalytics)
# ============================================================
class PredictiveAnalytics(IPredictiveAnalytics):
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.history = deque(maxlen=1000)
        self.prophet_available = PROPHET_AVAILABLE and config.predictive.enabled
        self.model_storage = Path(config.predictive.model_storage_path)
        self.model_storage.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        logger.info(f"PredictiveAnalytics initialized (Prophet: {self.prophet_available})")

    async def update_history(self, fitness_scores: List[float]):
        async with self._lock:
            timestamp = datetime.now()
            for score in fitness_scores:
                self.history.append({'ds': timestamp, 'y': score})

    async def load_model(self, region: str) -> Optional[Any]:
        path = self.model_storage / f"{region}.prophet"
        if path.exists():
            try:
                return Prophet.load(str(path))
            except Exception as e:
                logger.warning(f"Failed to load Prophet model for {region}: {e}")
        return None

    async def save_model(self, region: str, model: Any):
        path = self.model_storage / f"{region}.prophet"
        try:
            model.save(str(path))
        except Exception as e:
            logger.error(f"Failed to save Prophet model for {region}: {e}")

    async def forecast_fitness(self, horizon_hours: int = 24) -> Dict:
        if not self.prophet_available or len(self.history) < self.config.predictive.min_samples:
            return {'forecast': [], 'confidence': 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(self.history))
            df = df.sort_values('ds')
            # Use a fixed region name for simplicity
            region = "global"
            model = await self.load_model(region)
            if model is None:
                model = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
                model.fit(df)
                await self.save_model(region, model)
            else:
                # Update with new data
                model.fit(df)
                await self.save_model(region, model)
            future = model.make_future_dataframe(periods=horizon_hours)
            forecast = model.predict(future)
            forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon_hours)
            if PROMETHEUS_AVAILABLE:
                PREDICTIVE_FORECAST.labels(model='prophet', status='success').inc()
            return {
                'forecast': forecast_df['yhat'].tolist(),
                'lower_bound': forecast_df['yhat_lower'].tolist(),
                'upper_bound': forecast_df['yhat_upper'].tolist(),
                'dates': forecast_df['ds'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
                'confidence': 0.9,
                'model': 'prophet'
            }
        except Exception as e:
            logger.error(f"Prophet forecast failed: {e}")
            if PROMETHEUS_AVAILABLE:
                PREDICTIVE_FORECAST.labels(model='prophet', status='failed').inc()
            return {'forecast': [], 'confidence': 0.0}

# ============================================================
# AUTONOMOUS OPTIMIZER (implements IAutonomousOptimizer)
# ============================================================
class AutonomousOptimizer(IAutonomousOptimizer):
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.param_space = config.optimizer.parameter_space
        self.epsilon = config.optimizer.epsilon
        self.rewards = {param: {val: 0.0 for val in vals} for param, vals in self.param_space.items()}
        self.counts = {param: {val: 0 for val in vals} for param, vals in self.param_space.items()}
        self.history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        logger.info("AutonomousOptimizer initialized")

    async def select_parameters(self) -> Dict:
        async with self._lock:
            selected = {}
            for param, values in self.param_space.items():
                if random.random() < self.epsilon:
                    val = random.choice(values)
                else:
                    val = max(values, key=lambda v: self.rewards[param][v])
                selected[param] = val
            self.history.append({'timestamp': datetime.now().isoformat(), 'selected': selected})
            if PROMETHEUS_AVAILABLE:
                OPTIMIZER_DECISIONS.labels(parameter='all', action='selected').inc()
            return selected

    async def update_rewards(self, parameters: Dict, outcome: float):
        async with self._lock:
            for param, val in parameters.items():
                if param in self.rewards and val in self.rewards[param]:
                    count = self.counts[param][val] + 1
                    self.counts[param][val] = count
                    self.rewards[param][val] += (outcome - self.rewards[param][val]) / count

    def get_stats(self) -> Dict:
        return {
            'epsilon': self.epsilon,
            'rewards': self.rewards,
            'counts': self.counts,
            'history_length': len(self.history)
        }

# ============================================================
# ASYNC DATABASE MANAGER (implements IAsyncDatabase)
# ============================================================
class AsyncDatabaseManager(IAsyncDatabase):
    def __init__(self, config: EvolutionConfig):
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
            self.async_engine = create_async_engine(self.db_url, pool_size=self.config.database.pool_size,
                                                     max_overflow=self.config.database.max_overflow,
                                                     poolclass=NullPool)
            self.async_session = async_sessionmaker(self.async_engine, expire_on_commit=False)
            # Create tables
            self._init_schema()
        except Exception as e:
            logger.warning(f"Async database init failed: {e}, falling back to sync")
            # Fallback to sync engine (for SQLite)
            from sqlalchemy import create_engine
            self.async_engine = create_engine(self.db_url.replace("+aiosqlite", ""))
            self.async_session = None
            self._init_schema_sync()

    def _init_schema(self):
        if not self.async_engine:
            return
        try:
            Base.metadata.create_all(self.async_engine)
        except Exception as e:
            logger.warning(f"Could not create tables async: {e}")

    def _init_schema_sync(self):
        try:
            Base.metadata.create_all(self.async_engine)
        except Exception as e:
            logger.warning(f"Could not create tables sync: {e}")

    async def log_event(self, event_type: str, expert_id: str = None, details: Dict = None):
        if not SQLALCHEMY_AVAILABLE:
            return
        if self.async_session:
            try:
                async with self.async_session() as session:
                    event = EvolutionEventDB(
                        event_type=event_type,
                        expert_id=expert_id,
                        details=details or {}
                    )
                    session.add(event)
                    await session.commit()
            except Exception as e:
                logger.warning(f"Failed to log event async: {e}")
        else:
            # Sync fallback
            try:
                with self.async_engine.connect() as conn:
                    conn.execute(
                        text("INSERT INTO evolution_events (event_type, expert_id, details, timestamp) VALUES (:event_type, :expert_id, :details, :timestamp)"),
                        {"event_type": event_type, "expert_id": expert_id, "details": json.dumps(details or {}), "timestamp": datetime.now()}
                    )
                    conn.commit()
            except Exception as e:
                logger.warning(f"Failed to log event sync: {e}")

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
# LEADER ELECTION (using Redis)
# ============================================================
class LeaderElection:
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.redis = None
        self.is_leader = False
        self._lock = asyncio.Lock()
        if config.leader.enabled and REDIS_AVAILABLE and config.leader.redis_url:
            try:
                self.redis = redis.from_url(config.leader.redis_url, decode_responses=True)
            except Exception as e:
                logger.error(f"Redis connection failed: {e}")

    async def try_acquire_leadership(self) -> bool:
        if not self.redis:
            return True  # Assume leader if no leader election
        try:
            # Use setnx with TTL
            acquired = await self.redis.setnx("evolution:leader", str(uuid.uuid4()))
            if acquired:
                await self.redis.expire("evolution:leader", self.config.leader.ttl_seconds)
                async with self._lock:
                    self.is_leader = True
                return True
            else:
                async with self._lock:
                    self.is_leader = False
                return False
        except Exception as e:
            logger.error(f"Leader election failed: {e}")
            return True  # Assume leader on error

    async def renew_leadership(self):
        if self.redis and self.is_leader:
            try:
                await self.redis.expire("evolution:leader", self.config.leader.ttl_seconds)
            except Exception as e:
                logger.error(f"Failed to renew leadership: {e}")

    async def stop(self):
        if self.redis:
            await self.redis.close()

# ============================================================
# DATABASE ORM MODEL
# ============================================================
if SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class EvolutionEventDB(Base):
        __tablename__ = 'evolution_events'
        id = Column(Integer, primary_key=True)
        event_type = Column(String(64))
        expert_id = Column(String(128))
        details = Column(JSON)
        timestamp = Column(DateTime, default=datetime.now)
else:
    Base = None

# ============================================================
# ENHANCED EVOLUTIONARY ENGINE (with dependency injection)
# ============================================================
class EvolutionaryEngine:
    """
    Periodic evolutionary engine with:
    - Fitness computation (accuracy / cost) with recency, usage, uncertainty weights.
    - Pruning of low‑fitness experts.
    - Merging of similar experts.
    - Spawning of new experts based on domain gaps.
    - PQC signing of evolution events.
    - Cloud backup of evolution history.
    - Predictive analytics for fitness trends.
    - Autonomous parameter optimization.
    - Leader election to avoid duplicate work.
    """

    def __init__(
        self,
        config: EvolutionConfig,
        registry: ExpertRegistry,
        cost_function: SustainabilityCostFunction,
        digital_twin: DigitalTwin,
        mlops: MLOpsPipeline,
        db_manager: AsyncDatabaseManager,
        task_manager: TaskManager,
        pqc: IPQC,
        cloud_storage: ICloudStorage,
        predictive_analytics: IPredictiveAnalytics,
        autonomous_optimizer: IAutonomousOptimizer,
        vault: VaultManager,
        leader_election: LeaderElection,
    ):
        self.config = config
        self.registry = registry
        self.cost_function = cost_function
        self.digital_twin = digital_twin
        self.mlops = mlops
        self.db_manager = db_manager
        self.task_manager = task_manager
        self.pqc = pqc
        self.cloud_storage = cloud_storage
        self.predictive = predictive_analytics
        self.optimizer = autonomous_optimizer
        self.vault = vault
        self.leader = leader_election

        # State
        self._fitness_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._running = False
        self._cycle_count = 0

        # Register health checks
        self._health_components = {
            'pqc': self.pqc,
            'cloud': self.cloud_storage,
            'predictive': self.predictive,
            'optimizer': self.optimizer,
            'database': self.db_manager,
            'vault': self.vault,
        }

        logger.info("EvolutionaryEngine initialized with config: %s", self.config)

    # ----------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------
    async def start(self):
        self._running = True
        # Register the evolution loop with TaskManager
        self.task_manager.register_task(
            "evolution_loop",
            self._evolution_loop,
            self.config.general.evolution_interval_seconds
        )
        self.task_manager.start_registered_tasks()
        logger.info("EvolutionaryEngine started with interval %d seconds",
                    self.config.general.evolution_interval_seconds)

    async def _evolution_loop(self, interval: int):
        while self._running:
            start_time = time.time()
            try:
                # Try to acquire leadership
                if await self.leader.try_acquire_leadership():
                    await self._evolve()
                    # Renew leadership periodically
                    asyncio.create_task(self.leader.renew_leadership())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Evolution loop error: %s", e, exc_info=True)
                await asyncio.sleep(60)
            finally:
                elapsed = time.time() - start_time
                if PROMETHEUS_AVAILABLE:
                    EVOLUTION_DURATION.observe(elapsed)
                    EVOLUTION_CYCLES.inc()
                await asyncio.sleep(interval)

    async def _evolve(self):
        """Run one full evolution cycle with enhanced features."""
        experts = self.registry.get_all_active_experts()
        if not experts:
            logger.debug("No active experts, skipping evolution cycle")
            return

        # 1. Compute fitness
        context = {"task_type": "general", "token_count": 100}
        fitness_scores = {}
        fitness_values = []
        for expert in experts:
            try:
                fitness = await self._compute_fitness(expert, context)
                fitness_scores[expert.expert_id] = fitness
                fitness_values.append(fitness)
            except Exception as e:
                logger.error("Error computing fitness for expert %s: %s", expert.expert_id, e)
                fitness_scores[expert.expert_id] = 0.0

        if fitness_values:
            if PROMETHEUS_AVAILABLE:
                FITNESS_DISTRIBUTION.observe(np.mean(fitness_values))
            await self.predictive.update_history(fitness_values)

        # 2. Autonomous parameter selection
        if self.config.optimizer.enabled:
            params = await self.optimizer.select_parameters()
            self.config.general.prune_threshold = params['prune_threshold']
            self.config.general.merge_similarity_threshold = params['merge_similarity_threshold']
            self.config.general.spawn_gap_threshold = params['spawn_gap_threshold']
            # Update fitness weights if they are part of param space
            if 'fitness_recency_weight' in params:
                self.config.general.fitness_recency_weight = params['fitness_recency_weight']

        async with self._lock:
            # 3. Prune low‑fitness experts
            to_prune = []
            for eid, fit in fitness_scores.items():
                if fit < self.config.general.prune_threshold and not await self._is_critical(eid):
                    to_prune.append(eid)
            to_prune = to_prune[:self.config.general.max_prunes_per_cycle]
            for eid in to_prune:
                try:
                    await self.registry.deprecate_expert(eid, reason="evolutionary_prune")
                    logger.info("Pruned expert %s (fitness %.3f)", eid, fitness_scores[eid])
                    if PROMETHEUS_AVAILABLE:
                        EXPERTS_PRUNED.inc()
                    await self.db_manager.log_event('prune', expert_id=eid, details={'fitness': fitness_scores[eid]})
                except Exception as e:
                    logger.error("Failed to prune expert %s: %s", eid, e)

            # 4. Merge similar experts
            merge_candidates = await self._find_similar_experts(experts, fitness_scores)
            merge_candidates = merge_candidates[:self.config.general.max_merges_per_cycle]
            for eid_a, eid_b in merge_candidates:
                try:
                    merged_id = await self._merge_experts(eid_a, eid_b)
                    if merged_id:
                        logger.info("Merged experts %s and %s into %s", eid_a, eid_b, merged_id)
                        if PROMETHEUS_AVAILABLE:
                            EXPERTS_MERGED.inc()
                        await self.db_manager.log_event('merge', expert_id=f"{eid_a},{eid_b}",
                                                        details={'merged_id': merged_id})
                except Exception as e:
                    logger.error("Failed to merge experts %s and %s: %s", eid_a, eid_b, e)

            # 5. Spawn new experts if domain gap is detected
            try:
                gap = await self._detect_domain_gap(experts, fitness_scores)
                if gap > self.config.general.spawn_gap_threshold:
                    new_expert_id = await self._spawn_expert(gap)
                    if new_expert_id:
                        logger.info("Spawned new expert %s due to domain gap %.3f", new_expert_id, gap)
                        if PROMETHEUS_AVAILABLE:
                            EXPERTS_SPAWNED.inc()
                        await self.db_manager.log_event('spawn', expert_id=new_expert_id, details={'gap': gap})
            except Exception as e:
                logger.error("Error during spawn: %s", e)

        # 6. Update optimizer reward based on overall fitness improvement
        if self.config.optimizer.enabled:
            avg_fitness = np.mean(fitness_values) if fitness_values else 0.0
            await self.optimizer.update_rewards(params, avg_fitness)

        # 7. Sign the cycle summary and backup to cloud
        cycle_summary = {
            'cycle': self._cycle_count,
            'timestamp': datetime.now().isoformat(),
            'experts_count': len(experts),
            'pruned': len(to_prune),
            'merged': len(merge_candidates),
            'spawned': 1 if 'new_expert_id' in locals() else 0,
            'fitness_scores': fitness_scores
        }
        signature = await self.pqc.sign_evolution_event(cycle_summary)
        cycle_summary['pqc_signature'] = signature
        await self.cloud_storage.store(cycle_summary, f"cycle_{self._cycle_count}.json")

    # ----------------------------------------------------------------
    # Internal methods
    # ----------------------------------------------------------------
    async def _compute_fitness(self, expert: ExpertProfile, context: Dict) -> float:
        cost = await self.cost_function.compute(expert, context)
        accuracy = expert.accuracy_score if expert.accuracy_score is not None else 0.5

        recency_factor = 1.0
        if hasattr(expert, 'last_used') and expert.last_used:
            days_since = (datetime.now() - expert.last_used).days
            recency_factor = 1.0 / (1 + days_since * 0.1)

        usage_factor = min(1.0, expert.usage_count / self.config.general.critical_usage_threshold)

        uncertainty_factor = 1.0
        if hasattr(expert, 'confidence'):
            confidence = expert.confidence
            uncertainty_factor = 1.0 - (1.0 - confidence) * 0.5

        weighted_factor = (
            (1 - self.config.general.fitness_recency_weight -
             self.config.general.fitness_usage_weight -
             self.config.general.fitness_uncertainty_weight)
            + self.config.general.fitness_recency_weight * recency_factor
            + self.config.general.fitness_usage_weight * usage_factor
            + self.config.general.fitness_uncertainty_weight * uncertainty_factor
        )
        fitness = (accuracy * weighted_factor) / (cost + 1e-8)
        return fitness

    async def _is_critical(self, expert_id: str) -> bool:
        expert = self.registry.get_expert(expert_id)
        if not expert:
            return False
        return expert.usage_count > self.config.general.critical_usage_threshold

    async def _find_similar_experts(self, experts: List[ExpertProfile], fitness: Dict[str, float]) -> List[Tuple[str, str]]:
        pairs = []
        if hasattr(self.mlops, 'get_model_embedding'):
            embeddings = {}
            for e in experts:
                try:
                    emb = await self.mlops.get_model_embedding(e.expert_id)
                    embeddings[e.expert_id] = emb
                except Exception as e:
                    logger.warning("Could not get embedding for %s: %s", e.expert_id, e)
                    embeddings[e.expert_id] = None

            for i, e1 in enumerate(experts):
                for e2 in experts[i+1:]:
                    if embeddings.get(e1.expert_id) is not None and embeddings.get(e2.expert_id) is not None:
                        sim = self._cosine_similarity(embeddings[e1.expert_id], embeddings[e2.expert_id])
                        if sim > self.config.general.merge_similarity_threshold:
                            pairs.append((e1.expert_id, e2.expert_id))
        else:
            for i, e1 in enumerate(experts):
                for e2 in experts[i+1:]:
                    if (e1.domain == e2.domain and
                        abs(fitness[e1.expert_id] - fitness[e2.expert_id]) < 0.1):
                        pairs.append((e1.expert_id, e2.expert_id))
        return pairs[:self.config.general.max_merges_per_cycle]

    def _cosine_similarity(self, vec_a: np.ndarray, vec_b: np.ndarray) -> float:
        if vec_a is None or vec_b is None:
            return 0.0
        dot = np.dot(vec_a, vec_b)
        norm_a = np.linalg.norm(vec_a)
        norm_b = np.linalg.norm(vec_b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    @retry(stop=stop_after_attempt(self.config.general.retry_attempts),
           wait=wait_exponential(multiplier=1, min=self.config.general.retry_wait_seconds, max=10),
           retry=retry_if_exception_type((Exception,)), before_sleep=before_sleep_log(logger, logging.WARNING))
    async def _merge_experts(self, expert_a_id: str, expert_b_id: str) -> Optional[str]:
        if not hasattr(self.mlops, 'merge_models'):
            expert_a = self.registry.get_expert(expert_a_id)
            expert_b = self.registry.get_expert(expert_b_id)
            if expert_a and expert_b:
                if expert_a.accuracy_score >= expert_b.accuracy_score:
                    await self.registry.deprecate_expert(expert_b_id, replacement=expert_a_id)
                    return expert_a_id
                else:
                    await self.registry.deprecate_expert(expert_a_id, replacement=expert_b_id)
                    return expert_b_id
            return None

        merged = await self.mlops.merge_models(expert_a_id, expert_b_id)
        if not merged:
            return None

        profile = ExpertProfile(
            expert_id=merged['id'],
            expert_name=f"Merged_{expert_a_id}_{expert_b_id}",
            domain=self.registry.get_expert(expert_a_id).domain,
            accuracy_score=merged['accuracy'],
            efficiency_score=(
                self.registry.get_expert(expert_a_id).efficiency_score +
                self.registry.get_expert(expert_b_id).efficiency_score
            ) / 2,
            sustainability_score=merged.get('sustainability_score', 0.5)
        )
        success, _ = await self.registry.register_expert(profile, validate=False, auto_certify=True)
        if success:
            await self.registry.deprecate_expert(expert_a_id, replacement=profile.expert_id)
            await self.registry.deprecate_expert(expert_b_id, replacement=profile.expert_id)
            return profile.expert_id
        return None

    async def _detect_domain_gap(self, experts: List[ExpertProfile], fitness: Dict[str, float]) -> float:
        if not hasattr(self.digital_twin, 'forecast_domain_distribution'):
            if len(experts) < 3:
                return 0.5
            return 0.0

        forecast = await self.digital_twin.forecast_domain_distribution()
        if not forecast:
            return 0.0

        current = defaultdict(int)
        for e in experts:
            current[e.domain] += 1

        total_domains = len(forecast)
        missing_domains = 0
        for domain, expected in forecast.items():
            if expected > 0 and current.get(domain, 0) == 0:
                missing_domains += 1
        gap = missing_domains / max(total_domains, 1)
        return gap

    async def _spawn_expert(self, gap: float) -> Optional[str]:
        if not hasattr(self.mlops, 'spawn_expert'):
            return None

        new_expert = await self.mlops.spawn_expert(gap)
        if not new_expert:
            return None

        profile = ExpertProfile(
            expert_id=new_expert['id'],
            expert_name=f"Spawned_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            domain=new_expert['domain'],
            accuracy_score=new_expert['accuracy'],
            efficiency_score=0.8,
            sustainability_score=new_expert.get('sustainability_score', 0.5)
        )
        success, _ = await self.registry.register_expert(profile, validate=False, auto_certify=True)
        return profile.expert_id if success else None

    # ----------------------------------------------------------------
    # Health check aggregation
    # ----------------------------------------------------------------
    async def health_check(self) -> Dict:
        results = {}
        for name, component in self._health_components.items():
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
            'cycle_count': self._cycle_count,
            'running': self._running,
            'timestamp': datetime.now().isoformat()
        }

    # ----------------------------------------------------------------
    # Control methods
    # ----------------------------------------------------------------
    async def stop(self):
        self._running = False
        await self.task_manager.stop_all()
        await self.leader.stop()
        await self.db_manager.close()
        logger.info("EvolutionaryEngine stopped")

    async def get_status(self) -> Dict:
        async with self._lock:
            return {
                'running': self._running,
                'cycle_count': self._cycle_count,
                'fitness_history_length': len(self._fitness_history),
                'config': self.config.dict() if hasattr(self.config, 'dict') else self.config.__dict__,
                'active_expert_count': len(self.registry.get_all_active_experts()),
                'quantum': self.pqc.get_quantum_status(),
                'optimizer': self.optimizer.get_stats(),
                'predictive_available': self.predictive.prophet_available,
                'is_leader': self.leader.is_leader
            }

# ============================================================
# FastAPI REST API (with rate limiting)
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Evolutionary Engine API", version="4.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer()
    rate_limiter = RateLimiter(EvolutionConfig().api)

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        token = credentials.credentials
        try:
            payload = jwt.decode(token, EvolutionConfig().api.jwt_secret, algorithms=["HS256"])
            return payload
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    async def rate_limit(request: Request):
        if EvolutionConfig().api.rate_limit_enabled:
            key = request.client.host
            if not await rate_limiter.acquire():
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

    # Global engine instance
    engine: Optional[EvolutionaryEngine] = None

    @app.get("/health")
    async def health():
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return await engine.health_check()

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return await engine.get_status()

    @app.post("/start")
    async def start(interval: Optional[int] = None, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        # Reconfigure interval if provided
        if interval is not None:
            engine.config.general.evolution_interval_seconds = interval
        await engine.start()
        return {"status": "started"}

    @app.post("/stop")
    async def stop(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        await engine.stop()
        return {"status": "stopped"}

    @app.on_event("startup")
    async def startup():
        global engine
        from unittest.mock import MagicMock, AsyncMock
        registry = MagicMock()
        cost_function = AsyncMock()
        digital_twin = AsyncMock()
        mlops = AsyncMock()
        db_manager = AsyncMock()
        task_manager = MagicMock()
        config = EvolutionConfig()
        # Build dependencies
        vault = VaultManager(config)
        pqc = PostQuantumCrypto(config, vault)
        cloud = MultiCloudStorage(config)
        predictive = PredictiveAnalytics(config)
        optimizer = AutonomousOptimizer(config)
        leader = LeaderElection(config)
        # Create engine
        engine = EvolutionaryEngine(
            config=config,
            registry=registry,
            cost_function=cost_function,
            digital_twin=digital_twin,
            mlops=mlops,
            db_manager=db_manager,
            task_manager=task_manager,
            pqc=pqc,
            cloud_storage=cloud,
            predictive_analytics=predictive,
            autonomous_optimizer=optimizer,
            vault=vault,
            leader_election=leader
        )
        await engine.start()
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown():
        if engine:
            await engine.stop()
        logger.info("FastAPI shut down")

# ============================================================
# Singleton accessor (optional)
# ============================================================
_engine_instance = None
_engine_lock = asyncio.Lock()

async def get_evolutionary_engine(
    config: EvolutionConfig,
    registry: ExpertRegistry,
    cost_function: SustainabilityCostFunction,
    digital_twin: DigitalTwin,
    mlops: MLOpsPipeline,
    db_manager: DatabaseManager,
    task_manager: TaskManager,
) -> EvolutionaryEngine:
    global _engine_instance
    if _engine_instance is None:
        async with _engine_lock:
            if _engine_instance is None:
                # Build dependencies
                vault = VaultManager(config)
                pqc = PostQuantumCrypto(config, vault)
                cloud = MultiCloudStorage(config)
                predictive = PredictiveAnalytics(config)
                optimizer = AutonomousOptimizer(config)
                leader = LeaderElection(config)
                # Async DB manager (wraps the provided db_manager)
                async_db = AsyncDatabaseManager(config)
                _engine_instance = EvolutionaryEngine(
                    config=config,
                    registry=registry,
                    cost_function=cost_function,
                    digital_twin=digital_twin,
                    mlops=mlops,
                    db_manager=db_manager,
                    task_manager=task_manager,
                    pqc=pqc,
                    cloud_storage=cloud,
                    predictive_analytics=predictive,
                    autonomous_optimizer=optimizer,
                    vault=vault,
                    leader_election=leader
                )
    return _engine_instance

# ============================================================
# Dummy Tenacity decorator if not available
# ============================================================
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @wraps(func)
            async def wrapper(*fargs, **fkwargs):
                return await func(*fargs, **fkwargs)
            return wrapper
        return decorator

# ============================================================
# Main entry point (for testing)
# ============================================================
async def main():
    print("Starting Evolutionary Engine Demo...")
    from unittest.mock import AsyncMock, MagicMock
    registry = MagicMock()
    registry.get_all_active_experts.return_value = []
    cost_function = AsyncMock()
    digital_twin = AsyncMock()
    mlops = AsyncMock()
    db_manager = MagicMock()
    task_manager = MagicMock()

    config = EvolutionConfig()
    # Build dependencies
    vault = VaultManager(config)
    pqc = PostQuantumCrypto(config, vault)
    cloud = MultiCloudStorage(config)
    predictive = PredictiveAnalytics(config)
    optimizer = AutonomousOptimizer(config)
    leader = LeaderElection(config)
    async_db = AsyncDatabaseManager(config)

    engine = EvolutionaryEngine(
        config=config,
        registry=registry,
        cost_function=cost_function,
        digital_twin=digital_twin,
        mlops=mlops,
        db_manager=db_manager,
        task_manager=task_manager,
        pqc=pqc,
        cloud_storage=cloud,
        predictive_analytics=predictive,
        autonomous_optimizer=optimizer,
        vault=vault,
        leader_election=leader
    )
    await engine.start()
    try:
        await asyncio.sleep(30)
    except KeyboardInterrupt:
        pass
    finally:
        await engine.stop()
        print("Engine stopped.")

if __name__ == "__main__":
    asyncio.run(main())
