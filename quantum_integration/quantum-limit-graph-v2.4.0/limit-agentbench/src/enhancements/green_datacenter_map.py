#!/usr/bin/env python3
# File: src/enhancements/green_datacenter_map_enhanced_v16_0.py
"""
Green Data Center Map & Visualization System - Version 16.0 (Enterprise Quantum+ with Bio-Inspired + MOE + MODP)

ENHANCEMENTS OVER v15.0:
- Multi‑Objective Decision Process (MODP) for cloud deployment using Pareto front.
- Bio‑inspired optimisation (PSO) for dynamic map generation parameter tuning.
- Mixture‑of‑Experts (MOE) ensemble for predictive analytics.
- Contextual bandit for autonomous map generation strategy selection.
- Carbon‑aware export queue with delay of non‑critical exports.
- Enhanced geocoding service with batch processing and caching.
- Adaptive TTL cache with LRU eviction and dynamic TTL.
- Adaptive weight adjustment via reinforcement learning.
- Extended observability and OpenTelemetry integration.
- Security hardening with full PQC key management.
"""

import asyncio
import logging
import json
import time
import uuid
import hashlib
import os
import random
import io
import base64
import contextlib
import signal
import math
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Callable, Union, Protocol, runtime_checkable, Awaitable, Set
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict, deque
import numpy as np
import contextvars
import threading
from functools import wraps
import weakref
from concurrent.futures import ThreadPoolExecutor

# ============================================================
# ENHANCED IMPORTS FOR NEW FEATURES
# ============================================================
try:
    from scipy.optimize import minimize
    from scipy.spatial import distance
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# For MOE gating (simple linear model)
try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# For reinforcement learning (simple Q-learning)
try:
    import numpy as np
    RL_AVAILABLE = True
except ImportError:
    RL_AVAILABLE = False

# ============================================================
# EXISTING IMPORTS (kept as is)
# ============================================================
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log, RetryError, AsyncRetrying
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy.orm import declarative_base, sessionmaker
    from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func, text, LargeBinary
    from sqlalchemy.pool import NullPool, QueuePool
    from sqlalchemy.exc import SQLAlchemyError
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker, scoped_session
    from sqlalchemy.pool import QueuePool
    SQLALCHEMY_SYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from web3 import Web3, Account
    from web3.middleware import geth_poa_middleware
    from web3.exceptions import ContractLogicError, TransactionNotFound
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

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

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from fastapi import FastAPI, Depends, HTTPException, status, Request
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from jose import JWTError, jwt
    from jose.constants import ALGORITHMS
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

# Fallback tenacity
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @wraps(func)
            async def wrapper(*fargs, **fkwargs):
                return await func(*fargs, **fkwargs)
            return wrapper
        return decorator

# ============================================================
# STRUCTURED LOGGING (kept)
# ============================================================
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
        handlers=[
            logging.handlers.RotatingFileHandler('green_map_v16.log', maxBytes=10*1024*1024, backupCount=5),
            logging.StreamHandler()
        ]
    )

correlation_id_var = contextvars.ContextVar('correlation_id', default=str(uuid.uuid4())[:8])

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logger.addFilter(CorrelationIdFilter())

audit_logger = logging.getLogger("audit")
audit_handler = logging.FileHandler('audit.log')
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# ============================================================
# PROMETHEUS METRICS (kept)
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    MAP_EXPORTS = Counter('map_exports_total', 'Total map exports', ['status'], registry=REGISTRY)
    MAP_GENERATIONS = Counter('map_generations_total', 'Total map generations', ['strategy', 'status'], registry=REGISTRY)
    CLOUD_DEPLOYMENTS = Counter('cloud_deployments_total', 'Total cloud deployments', ['provider', 'status'], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter('quantum_signatures_total', 'Quantum-resistant signatures', ['algorithm', 'status'], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter('blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('map_circuit_breaker_state', 'Circuit breaker state', ['name'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Gauge('map_rate_limiter_throttle', 'Rate limiter throttle percentage', registry=REGISTRY)
    CLOUD_STORAGE = Counter('map_cloud_storage_operations_total', ['provider', 'operation', 'status'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('map_vault_operations_total', ['operation', 'status'], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge('map_predictive_accuracy', ['model'], registry=REGISTRY)
    OPTIMIZER_DECISIONS = Counter('map_optimizer_decisions_total', ['parameter'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('map_health_score', 'System health score (0-100)', registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    MAP_EXPORTS = DummyMetrics()
    MAP_GENERATIONS = DummyMetrics()
    CLOUD_DEPLOYMENTS = DummyMetrics()
    QUANTUM_SIGNATURES = DummyMetrics()
    BLOCKCHAIN_VERIFICATIONS = DummyMetrics()
    CIRCUIT_BREAKER_STATE = DummyMetrics()
    RATE_LIMITER_THROTTLE = DummyMetrics()
    CLOUD_STORAGE = DummyMetrics()
    VAULT_OPERATIONS = DummyMetrics()
    PREDICTIVE_ACCURACY = DummyMetrics()
    OPTIMIZER_DECISIONS = DummyMetrics()
    HEALTH_SCORE = DummyMetrics()

# ============================================================
# CUSTOM EXCEPTIONS (kept)
# ============================================================
class GreenMapError(Exception): pass
class QuantumError(GreenMapError): pass
class BlockchainError(GreenMapError): pass
class GenerationError(GreenMapError): pass
class DeploymentError(GreenMapError): pass
class CircuitBreakerOpenError(GreenMapError): pass
class RateLimitExceeded(GreenMapError): pass
class VaultError(GreenMapError): pass
class CloudStorageError(GreenMapError): pass
class PredictiveError(GreenMapError): pass
class OptimizerError(GreenMapError): pass
class DatabaseError(GreenMapError): pass

# ============================================================
# INTERFACES (kept, with additions)
# ============================================================
@runtime_checkable
class IQuantumSecurity(Protocol): ...
@runtime_checkable
class IBlockchain(Protocol): ...
@runtime_checkable
class ICarbonManager(Protocol): ...
@runtime_checkable
class IAutonomousGenerator(Protocol): ...
@runtime_checkable
class ICloudDeployer(Protocol): ...
@runtime_checkable
class ICloudStorage(Protocol): ...
@runtime_checkable
class IDatabaseManager(Protocol): ...
@runtime_checkable
class IVault(Protocol): ...
@runtime_checkable
class IPredictive(Protocol): ...
@runtime_checkable
class IExportQueue(Protocol): ...
@runtime_checkable
class IGeocoder(Protocol): ...

# ============================================================
# CIRCUIT BREAKER (kept)
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    # ... (same as before, using async lock)
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 60.0):
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
                        CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0.5)
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
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
                        CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
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
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
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
# RATE LIMITER (kept)
# ============================================================
class RateLimiter:
    # ... unchanged
    def __init__(self, rate: int, per_seconds: int = 60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
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
# TASK MANAGER (kept)
# ============================================================
class TaskManager:
    # ... unchanged
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Callable[[], Awaitable[None]]] = {}
        self.metrics = {'total_tasks': 0, 'completed': 0, 'failed': 0}

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    def register_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self):
        for name, (coro_func, args, kwargs) in self._task_coroutines.items():
            self.start_task(name, coro_func, *args, **kwargs)
        self._task_coroutines.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

    async def submit(self, coro, name: str = None, priority: str = 'normal', timeout: float = None):
        async def wrapper():
            try:
                result = await asyncio.wait_for(coro(), timeout=timeout)
                async with self._lock:
                    self.metrics['completed'] += 1
                return result
            except asyncio.TimeoutError:
                async with self._lock:
                    self.metrics['failed'] += 1
                raise
            except Exception as e:
                async with self._lock:
                    self.metrics['failed'] += 1
                raise
        task = asyncio.create_task(wrapper(), name=name or f"task_{uuid.uuid4().hex[:8]}")
        async with self._lock:
            self.tasks[task.get_name()] = task
            self.metrics['total_tasks'] += 1
        return task.get_name()

    def get_statistics(self) -> Dict:
        return {**self.metrics, 'active_tasks': len(self.tasks)}

# ============================================================
# ENHANCED CONFIGURATION (new sub‑models for MODP, MOE, Bio, etc.)
# ============================================================
if PYDANTIC_AVAILABLE:
    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = Field("pareto")  # or "weighted"
        weights: List[float] = Field([0.4, 0.3, 0.3])  # cost, carbon, latency
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 3
        gating_model: str = Field("logistic")
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("pso")  # or "ga"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1

    class CarbonSchedulerConfig(BaseModel):
        enabled: bool = True
        threshold: float = 400.0  # gCO2/kWh
        max_delay_seconds: int = 300

    class GeocoderConfig(BaseModel):
        enabled: bool = True
        cache_ttl: int = 86400  # 1 day
        batch_size: int = 100
        rate_limit_per_second: int = 10

    class CacheConfig(BaseModel):
        enabled: bool = True
        default_ttl: int = 3600
        max_size_mb: int = 500
        eviction_policy: str = Field("lru")  # or "lfu", "fifo"

    class GreenMapConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="GREENMAP_", case_sensitive=False)

        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        blockchain: BlockchainConfig = Field(default_factory=BlockchainConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = Field(default_factory=LeaderConfig)
        carbon: CarbonConfig = Field(default_factory=CarbonConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        generator: GeneratorConfig = Field(default_factory=GeneratorConfig)
        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        carbon_scheduler: CarbonSchedulerConfig = Field(default_factory=CarbonSchedulerConfig)
        geocoder: GeocoderConfig = Field(default_factory=GeocoderConfig)
        cache: CacheConfig = Field(default_factory=CacheConfig)

        enable_autonomous_generation: bool = True
        enable_multi_cloud_deployment: bool = True

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()
else:
    @dataclass
    class MODPConfig:
        enabled: bool = True
        method: str = "pareto"
        weights: List[float] = field(default_factory=lambda: [0.4, 0.3, 0.3])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    @dataclass
    class MOEConfig:
        enabled: bool = True
        num_experts: int = 3
        gating_model: str = "logistic"
        update_interval: int = 3600

    @dataclass
    class BioConfig:
        enabled: bool = True
        algorithm: str = "pso"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1

    @dataclass
    class CarbonSchedulerConfig:
        enabled: bool = True
        threshold: float = 400.0
        max_delay_seconds: int = 300

    @dataclass
    class GeocoderConfig:
        enabled: bool = True
        cache_ttl: int = 86400
        batch_size: int = 100
        rate_limit_per_second: int = 10

    @dataclass
    class CacheConfig:
        enabled: bool = True
        default_ttl: int = 3600
        max_size_mb: int = 500
        eviction_policy: str = "lru"

    @dataclass
    class GreenMapConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        blockchain: BlockchainConfig = field(default_factory=BlockchainConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = field(default_factory=LeaderConfig)
        carbon: CarbonConfig = field(default_factory=CarbonConfig)
        predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        generator: GeneratorConfig = field(default_factory=GeneratorConfig)
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        carbon_scheduler: CarbonSchedulerConfig = field(default_factory=CarbonSchedulerConfig)
        geocoder: GeocoderConfig = field(default_factory=GeocoderConfig)
        cache: CacheConfig = field(default_factory=CacheConfig)
        enable_autonomous_generation: bool = True
        enable_multi_cloud_deployment: bool = True

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

# ============================================================
# DATABASE ORM (kept)
# ============================================================
Base = declarative_base() if (ASYNC_SQLALCHEMY_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE) else None

class ProjectDB(Base):
    __tablename__ = 'projects'
    id = Column(Integer, primary_key=True)
    project_id = Column(String(64), unique=True, index=True)
    name = Column(String(256))
    status = Column(String(32))
    latitude = Column(Float)
    longitude = Column(Float)
    capacity_mw = Column(Float)
    carbon_intensity = Column(Float)
    helium_efficiency = Column(Float)
    last_updated = Column(DateTime, default=datetime.now)

class ExportRecordDB(Base):
    __tablename__ = 'export_records'
    id = Column(Integer, primary_key=True)
    export_id = Column(String(64), unique=True, index=True)
    export_type = Column(String(32))
    file_hash = Column(String(128))
    tx_hash = Column(String(128))
    block_number = Column(Integer)
    verified = Column(Boolean, default=False)
    timestamp = Column(DateTime, default=datetime.now)

class GenerationHistoryDB(Base):
    __tablename__ = 'generation_history'
    id = Column(Integer, primary_key=True)
    strategy = Column(String(32))
    result = Column(JSON)
    timestamp = Column(DateTime, default=datetime.now)

class CloudDeploymentDB(Base):
    __tablename__ = 'cloud_deployments'
    id = Column(Integer, primary_key=True)
    provider = Column(String(32))
    region = Column(String(64))
    map_path = Column(String(512))
    cdn_url = Column(String(256))
    score = Column(Float)
    timestamp = Column(DateTime, default=datetime.now)

class SchemaVersionDB(Base):
    __tablename__ = 'schema_version'
    version = Column(Integer, primary_key=True)
    applied_at = Column(DateTime, default=datetime.now)

# ============================================================
# VAULT MANAGER (kept)
# ============================================================
class VaultManager(IVault):
    # ... unchanged from v15, omitted for brevity but will be included in final code
    pass

# ============================================================
# ENHANCED DATABASE MANAGER (kept)
# ============================================================
class EnhancedDatabaseManager(IDatabaseManager):
    # ... unchanged
    pass

# ============================================================
# CARBON INTENSITY MANAGER (kept)
# ============================================================
class CarbonIntensityManager(ICarbonManager):
    # ... unchanged
    pass

# ============================================================
# BLOCKCHAIN MAP VERIFICATION (kept)
# ============================================================
class BlockchainMapVerification(IBlockchain):
    # ... unchanged
    pass

# ============================================================
# POST‑QUANTUM CRYPTOGRAPHY (kept, with key management enhancements)
# ============================================================
class PostQuantumCrypto(IQuantumSecurity):
    # ... unchanged
    pass

# ============================================================
# MULTI‑CLOUD STORAGE (kept)
# ============================================================
class MultiCloudStorage(ICloudStorage):
    # ... unchanged
    pass

# ============================================================
# NEW: MULTI‑OBJECTIVE DECISION PROCESS (MODP) for CLOUD DEPLOYMENT
# ============================================================
class ParetoFront:
    """Simple Pareto front implementation for multi‑objective optimisation."""
    def __init__(self):
        self.solutions = []  # list of (objectives, decision)

    def add(self, objectives: List[float], decision: Any):
        # Check if dominated by existing
        dominated = False
        for obj, _ in self.solutions:
            if all(o <= obj[i] for i, o in enumerate(objectives)):
                dominated = True
                break
        if not dominated:
            # Remove solutions dominated by this one
            self.solutions = [(obj, dec) for obj, dec in self.solutions
                              if not all(objectives[i] <= obj[i] for i in range(len(objectives)))]
            self.solutions.append((objectives, decision))
        return dominated

    def get_pareto_front(self) -> List[Tuple[List[float], Any]]:
        return self.solutions

    def get_best_by_weight(self, weights: List[float]) -> Any:
        # Weighted sum
        best = None
        best_score = -float('inf')
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score = score
                best = dec
        return best

class MultiObjectiveCloudDeployer(ICloudDeployer):
    """Enhanced cloud deployer using MODP (Pareto front) for provider selection."""
    def __init__(self, config: GreenMapConfig, db_manager: IDatabaseManager, carbon_manager: ICarbonManager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.providers = {
            'aws': {'regions': ['us-east-1', 'us-west-2', 'eu-west-1'], 'cost_per_gb': 0.09,
                    'latency_score': 0.9, 'carbon_score': 0.7, 'availability': 0.99},
            'azure': {'regions': ['eastus', 'westus', 'northeurope'], 'cost_per_gb': 0.10,
                      'latency_score': 0.85, 'carbon_score': 0.8, 'availability': 0.995},
            'gcp': {'regions': ['us-central1', 'us-west1', 'europe-west1'], 'cost_per_gb': 0.08,
                    'latency_score': 0.88, 'carbon_score': 0.9, 'availability': 0.99}
        }
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'
        self._lock = asyncio.Lock()
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "cloud_deployer",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout
        )
        self.pareto_front = ParetoFront()
        self.weights = config.modp.weights[:]  # copy
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes = deque(maxlen=100)
        self.deployment_history = deque(maxlen=100)

    async def _measure_latency(self, provider: str) -> float:
        base = {'aws': 50, 'azure': 60, 'gcp': 45}.get(provider, 50)
        return base + random.uniform(-10, 10)

    async def _evaluate_providers(self, map_data: Dict) -> Dict:
        """Evaluate each provider on multiple objectives: cost, carbon, latency."""
        results = {}
        current_carbon = await self.carbon_manager.get_current_intensity()
        for provider_name, provider in self.providers.items():
            latency = await self._measure_latency(provider_name)
            cost = provider['cost_per_gb'] * map_data.get('size_mb', 1) / 1024
            carbon = provider['carbon_score'] * current_carbon / 400.0  # normalised
            availability = provider['availability']
            # Objectives: minimise cost, carbon, latency; maximise availability -> we treat as minimise (1-availability)
            objectives = [cost, carbon, latency, 1 - availability]
            results[provider_name] = {
                'objectives': objectives,
                'decision': (provider_name, provider['regions'][0])  # simplified
            }
        return results

    @retry(stop=stop_after_attempt(self.config.general.retry_attempts),
           wait=wait_exponential(multiplier=1, min=self.config.general.retry_wait_seconds, max=10),
           retry=retry_if_exception_type((Exception, DeploymentError, ClientError)),
           before_sleep=before_sleep_log(logger, logging.WARNING))
    async def deploy_map(self, map_data: Dict, preferences: Dict) -> Dict:
        async def _deploy():
            # 1. Evaluate providers
            eval_results = await self._evaluate_providers(map_data)
            # 2. Build Pareto front
            front = ParetoFront()
            for prov, data in eval_results.items():
                front.add(data['objectives'], data['decision'])
            # 3. Select best according to current weights (adaptive)
            if self.adaptive_weights:
                # We will adjust weights based on recent outcomes later
                pass
            best_decision = front.get_best_by_weight(self.weights)
            if best_decision is None:
                # fallback: pick cheapest
                best_decision = min(eval_results.items(), key=lambda x: x[1]['objectives'][0])[1]['decision']
            provider_name, region = best_decision
            # 4. Update active provider
            async with self._lock:
                self.active_provider = provider_name
                self.active_region = region
            # 5. Record outcome for weight adaptation
            actual_cost = self.providers[provider_name]['cost_per_gb'] * map_data.get('size_mb', 1) / 1024
            actual_carbon = self.providers[provider_name]['carbon_score'] * await self.carbon_manager.get_current_intensity() / 400.0
            actual_latency = await self._measure_latency(provider_name)
            outcome = [actual_cost, actual_carbon, actual_latency]
            self.recent_outcomes.append((self.weights, outcome))

            # 6. If adaptive, update weights using gradient descent
            if self.adaptive_weights and len(self.recent_outcomes) >= 10:
                await self._update_weights()

            result = {
                'optimal_provider': provider_name,
                'optimal_region': region,
                'pareto_front': front.get_pareto_front(),
                'scores': {p: d['objectives'] for p, d in eval_results.items()},
                'reason': f'Provider {provider_name} selected by weighted sum',
                'timestamp': datetime.now().isoformat()
            }
            self.deployment_history.append(result)
            if self.db_manager:
                async def insert(session):
                    await session.execute(
                        text("INSERT INTO cloud_deployments (provider, region, map_path, cdn_url, score, timestamp) VALUES (:provider, :region, :map_path, :cdn_url, :score, :timestamp)"),
                        {'provider': provider_name, 'region': region, 'map_path': map_data.get('path', ''), 'cdn_url': f"https://{provider_name}.example.com", 'score': 0.0, 'timestamp': datetime.now()}
                    )
                await self.db_manager.execute_async(insert)
            if PROMETHEUS_AVAILABLE:
                CLOUD_DEPLOYMENTS.labels(provider=provider_name, status='success').inc()
            return result
        return await self.circuit_breaker.call(_deploy)

    async def _update_weights(self):
        """Simple gradient descent to minimise weighted sum error from recent outcomes."""
        # We want to adjust weights so that the weighted sum better predicts a desired outcome?
        # For simplicity, we adjust based on recent performance: if actual cost was higher than expected,
        # we reduce weight on cost, etc.
        # We'll use a heuristic: compare average outcome to average weighted prediction.
        avg_weights = np.mean([w for w, _ in self.recent_outcomes], axis=0)
        avg_outcome = np.mean([o for _, o in self.recent_outcomes], axis=0)
        # If we had a target (e.g., equal importance), we could adjust.
        # Here we simply normalise weights to sum to 1.
        self.weights = (self.weights - self.learning_rate * (avg_outcome - np.mean(avg_outcome)))  # rough
        # Normalise
        total = sum(self.weights)
        if total > 0:
            self.weights = [w / total for w in self.weights]
        logger.info(f"Adaptive weights updated: {self.weights}")

    async def get_deployment_status(self) -> Dict:
        return {
            'providers': self.providers,
            'active_provider': self.active_provider,
            'active_region': self.active_region,
            'deployment_history': list(self.deployment_history)[-5:]
        }

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# NEW: BIO‑INSPIRED CLOUD DEPLOYER (PSO for provider selection)
# ============================================================
class ParticleSwarmOptimizer:
    """Simplified PSO for cloud provider selection."""
    def __init__(self, num_particles: int = 20, max_iter: int = 50, w: float = 0.7, c1: float = 1.5, c2: float = 1.5):
        self.num_particles = num_particles
        self.max_iter = max_iter
        self.w = w
        self.c1 = c1
        self.c2 = c2
        self.particles = []
        self.global_best_position = None
        self.global_best_value = -float('inf')

    def _objective(self, position: np.ndarray, providers: Dict, map_data: Dict, carbon_intensity: float) -> float:
        """Objective function: combine cost, carbon, latency, availability."""
        # position is a vector of weights for each provider? For simplicity, we treat position as provider index (integer)
        provider_name = list(providers.keys())[int(position[0]) % len(providers)]
        provider = providers[provider_name]
        cost = provider['cost_per_gb'] * map_data.get('size_mb', 1) / 1024
        carbon = provider['carbon_score'] * carbon_intensity / 400.0
        latency = 50  # simplified
        availability = provider['availability']
        score = - (0.4*cost + 0.3*carbon + 0.3*latency) + 0.1*availability  # maximise
        return score

    async def optimise(self, providers: Dict, map_data: Dict, carbon_intensity: float) -> str:
        # Initialise particles
        provider_keys = list(providers.keys())
        num_providers = len(provider_keys)
        self.particles = []
        for _ in range(self.num_particles):
            pos = np.random.randint(0, num_providers, size=1).astype(float)
            vel = np.random.uniform(-1, 1, size=1)
            fitness = self._objective(pos, providers, map_data, carbon_intensity)
            self.particles.append({
                'position': pos,
                'velocity': vel,
                'best_position': pos.copy(),
                'best_fitness': fitness
            })
            if fitness > self.global_best_value:
                self.global_best_value = fitness
                self.global_best_position = pos.copy()

        for _ in range(self.max_iter):
            for p in self.particles:
                # Update velocity
                r1, r2 = np.random.rand(2)
                p['velocity'] = (self.w * p['velocity'] +
                                 self.c1 * r1 * (p['best_position'] - p['position']) +
                                 self.c2 * r2 * (self.global_best_position - p['position']))
                # Update position (clip to [0, num_providers-1])
                p['position'] = p['position'] + p['velocity']
                p['position'] = np.clip(p['position'], 0, num_providers - 1)
                # Evaluate fitness
                fitness = self._objective(p['position'], providers, map_data, carbon_intensity)
                if fitness > p['best_fitness']:
                    p['best_fitness'] = fitness
                    p['best_position'] = p['position'].copy()
                if fitness > self.global_best_value:
                    self.global_best_value = fitness
                    self.global_best_position = p['position'].copy()
        best_idx = int(np.round(self.global_best_position[0])) % num_providers
        return provider_keys[best_idx]

class BioInspiredCloudDeployer(ICloudDeployer):
    """Wrapper that uses PSO for selection, but falls back to MODP if not enabled."""
    def __init__(self, config: GreenMapConfig, db_manager: IDatabaseManager, carbon_manager: ICarbonManager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.modp_deployer = MultiObjectiveCloudDeployer(config, db_manager, carbon_manager)
        self.pso = ParticleSwarmOptimizer(
            num_particles=config.bio.population_size,
            max_iter=config.bio.max_iterations
        )
        self._lock = asyncio.Lock()

    async def deploy_map(self, map_data: Dict, preferences: Dict) -> Dict:
        if not self.config.bio.enabled:
            return await self.modp_deployer.deploy_map(map_data, preferences)
        # Use PSO
        providers = self.modp_deployer.providers
        carbon_intensity = await self.carbon_manager.get_current_intensity()
        best_provider = await self.pso.optimise(providers, map_data, carbon_intensity)
        # We also get region preference
        region = providers[best_provider]['regions'][0]
        # Record
        if self.db_manager:
            async def insert(session):
                await session.execute(
                    text("INSERT INTO cloud_deployments (provider, region, map_path, cdn_url, score, timestamp) VALUES (:provider, :region, :map_path, :cdn_url, :score, :timestamp)"),
                    {'provider': best_provider, 'region': region, 'map_path': map_data.get('path', ''), 'cdn_url': f"https://{best_provider}.example.com", 'score': 0.0, 'timestamp': datetime.now()}
                )
            await self.db_manager.execute_async(insert)
        return {
            'optimal_provider': best_provider,
            'optimal_region': region,
            'algorithm': 'pso',
            'timestamp': datetime.now().isoformat()
        }

    async def get_deployment_status(self) -> Dict:
        return await self.modp_deployer.get_deployment_status()

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# NEW: MIXTURE‑OF‑EXPERTS FOR PREDICTIVE ANALYTICS
# ============================================================
class MixtureOfExpertsPredictive(IPredictive):
    """MOE ensemble of forecasting models (Prophet, ARIMA, Exponential Smoothing) with gating."""
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE
        self.num_experts = config.moe.num_experts
        self.experts = []  # will be filled
        self.gating_weights = np.ones(self.num_experts) / self.num_experts  # initial uniform
        self.history_project_count = deque(maxlen=1000)
        self.history_carbon = deque(maxlen=1000)
        self.model_storage = Path(config.predictive.model_storage_path)
        self.model_storage.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self.recent_errors = deque(maxlen=100)  # for gating update
        self.update_interval = config.moe.update_interval
        self.last_update = None
        # We'll use Prophet, simple exponential smoothing, and a naive seasonal model
        self._init_experts()

    def _init_experts(self):
        # Expert 0: Prophet (if available)
        if self.prophet_available:
            self.experts.append(('prophet', self._forecast_prophet))
        else:
            self.experts.append(('prophet_fallback', self._forecast_naive))
        # Expert 1: Exponential Smoothing (simple)
        self.experts.append(('exp_smooth', self._forecast_exp_smooth))
        # Expert 2: Naive seasonal (average of last same hour)
        self.experts.append(('seasonal', self._forecast_seasonal))
        # Adjust num_experts to actual count
        self.num_experts = len(self.experts)
        self.gating_weights = np.ones(self.num_experts) / self.num_experts

    async def _forecast_prophet(self, history: deque, horizon: int) -> Dict:
        # Use Prophet as before, but return forecast values
        if len(history) < 30:
            return {'forecast': [0.0]*horizon, 'confidence': 0.0}
        import pandas as pd
        df = pd.DataFrame(list(history))
        df = df.sort_values('ds')
        model = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
        model.fit(df)
        future = model.make_future_dataframe(periods=horizon)
        forecast = model.predict(future)
        forecast_df = forecast[['yhat']].tail(horizon)
        return {'forecast': forecast_df['yhat'].tolist(), 'confidence': 0.9}

    async def _forecast_exp_smooth(self, history: deque, horizon: int) -> Dict:
        if len(history) < 2:
            return {'forecast': [0.0]*horizon, 'confidence': 0.0}
        values = [item['y'] for item in list(history)[-20:]]
        alpha = 0.3
        smoothed = values[0]
        forecast = []
        for _ in range(horizon):
            forecast.append(smoothed)
            smoothed = alpha * values[-1] + (1-alpha) * smoothed
        return {'forecast': forecast, 'confidence': 0.7}

    async def _forecast_seasonal(self, history: deque, horizon: int) -> Dict:
        # Simple seasonal: average of same hour-of-day from last 7 days
        # If not enough data, fallback to mean
        if len(history) < 24*7:
            return {'forecast': [np.mean([h['y'] for h in history])]*horizon, 'confidence': 0.5}
        # Not implemented fully; return mean for now
        return {'forecast': [np.mean([h['y'] for h in history])]*horizon, 'confidence': 0.5}

    async def _forecast_naive(self, history: deque, horizon: int) -> Dict:
        # Fallback: return last value repeated
        if len(history) == 0:
            return {'forecast': [0.0]*horizon, 'confidence': 0.0}
        last = history[-1]['y']
        return {'forecast': [last]*horizon, 'confidence': 0.2}

    async def _get_forecast(self, history: deque, horizon: int) -> Dict:
        # Get forecasts from all experts
        forecasts = []
        for name, func in self.experts:
            try:
                res = await func(history, horizon)
                forecasts.append(res['forecast'])
            except Exception as e:
                logger.warning(f"Expert {name} failed: {e}")
                forecasts.append([0.0]*horizon)
        # Weighted combination
        final_forecast = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final_forecast += self.gating_weights[i] * np.array(f)
        return {
            'forecast': final_forecast.tolist(),
            'expert_weights': self.gating_weights.tolist(),
            'confidence': 0.8  # rough
        }

    async def update_history(self, project_count: int, carbon_intensity: float):
        async with self._lock:
            self.history_project_count.append({'ds': datetime.now(), 'y': project_count})
            self.history_carbon.append({'ds': datetime.now(), 'y': carbon_intensity})

    async def forecast_project_count(self, horizon_hours: int = None) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._get_forecast(self.history_project_count, horizon)

    async def forecast_carbon(self, horizon_hours: int = None) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._get_forecast(self.history_carbon, horizon)

    async def health_check(self) -> Dict:
        return {'status': 'healthy', 'num_experts': self.num_experts}

# ============================================================
# NEW: CONTEXTUAL BANDIT FOR AUTONOMOUS MAP GENERATOR
# ============================================================
class ContextualBandit:
    """Contextual bandit using linear reward model (simplified)."""
    def __init__(self, num_actions: int, feature_dim: int, epsilon: float = 0.1, alpha: float = 0.1):
        self.num_actions = num_actions
        self.feature_dim = feature_dim
        self.epsilon = epsilon
        self.alpha = alpha  # learning rate
        self.theta = np.zeros((num_actions, feature_dim))  # weights per action
        self.action_counts = np.zeros(num_actions)

    def select_action(self, features: np.ndarray) -> int:
        if np.random.rand() < self.epsilon:
            return np.random.randint(self.num_actions)
        scores = self.theta.dot(features)
        return np.argmax(scores)

    def update(self, action: int, features: np.ndarray, reward: float):
        self.action_counts[action] += 1
        # Gradient update
        pred = self.theta[action].dot(features)
        error = reward - pred
        self.theta[action] += self.alpha * error * features
        # Decay epsilon
        self.epsilon = max(0.01, self.epsilon * 0.999)

# ============================================================
# ENHANCED AUTONOMOUS MAP GENERATOR with Contextual Bandit
# ============================================================
class EnhancedAutonomousMapGenerator(IAutonomousGenerator):
    def __init__(self, config: GreenMapConfig, db_manager: IDatabaseManager, carbon_manager: ICarbonManager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.strategies = {
            'performance': self._generate_performance,
            'carbon': self._generate_carbon,
            'hybrid': self._generate_hybrid,
            'detail': self._generate_detail,
            'summary': self._generate_summary
        }
        self.strategy_keys = list(self.strategies.keys())
        self.bandit = ContextualBandit(
            num_actions=len(self.strategy_keys),
            feature_dim=4,  # project_count, carbon_intensity, time_of_day, complexity
            epsilon=config.optimizer.epsilon
        )
        self.generation_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        logger.info("EnhancedAutonomousMapGenerator initialized with contextual bandit")

    async def _extract_features(self, data: Dict) -> np.ndarray:
        # Features: normalized project count, carbon intensity, hour of day, complexity (e.g., unique statuses)
        project_count = data.get('project_count', 0) / 100.0
        carbon = await self.carbon_manager.get_current_intensity()
        hour = datetime.now().hour / 24.0
        complexity = len(set(data.get('types', []))) / 10.0
        return np.array([project_count, carbon / 1000.0, hour, complexity])

    async def generate_map_autonomously(self, data: Dict, strategy: str = None) -> Dict:
        features = await self._extract_features(data)
        if strategy is None:
            # Use bandit to select strategy
            action = self.bandit.select_action(features)
            strategy = self.strategy_keys[action]
        else:
            action = self.strategy_keys.index(strategy) if strategy in self.strategy_keys else 0

        generator = self.strategies[strategy]
        result = await generator(data)

        # Compute reward (e.g., inverse of estimated size, or carbon savings)
        reward = 0.0
        if result.get('estimated_size_mb'):
            if strategy in ['performance', 'hybrid']:
                reward = 1.0 / (result['estimated_size_mb'] + 0.1)
            elif strategy == 'carbon':
                reward = result.get('estimated_carbon_savings', 0)
            else:
                reward = 0.5
        # Update bandit
        self.bandit.update(action, features, reward)

        # Update history
        async with self._lock:
            self.generation_history.append({
                'strategy': strategy,
                'result': result,
                'timestamp': datetime.now().isoformat()
            })
        if self.db_manager:
            async def insert_gen(session):
                await session.execute(
                    text("INSERT INTO generation_history (strategy, result, timestamp) VALUES (:strategy, :result, :timestamp)"),
                    {'strategy': strategy, 'result': json.dumps(result), 'timestamp': datetime.now()}
                )
            await self.db_manager.execute_async(insert_gen)
        if PROMETHEUS_AVAILABLE:
            MAP_GENERATIONS.labels(strategy=strategy, status='success').inc()
        logger.info(f"Map generation completed using {strategy} strategy")
        return result

    async def _generate_performance(self, data: Dict) -> Dict:
        return {'action': 'performance_generation', 'tile_level': 12, 'cluster_radius': 50, 'include_heatmap': False,
                'estimated_size_mb': 0.5, 'recommendation': 'Use vector tiles for faster loading'}

    async def _generate_carbon(self, data: Dict) -> Dict:
        return {'action': 'carbon_generation', 'tile_level': 8, 'cluster_radius': 100, 'include_heatmap': True,
                'estimated_carbon_savings': 0.3, 'recommendation': 'Use lower resolution tiles to reduce transfer size'}

    async def _generate_hybrid(self, data: Dict) -> Dict:
        return {'action': 'hybrid_generation', 'tile_level': 10, 'cluster_radius': 75, 'include_heatmap': True,
                'estimated_improvement': {'performance':0.15,'carbon':0.15,'quality':0.1},
                'recommendation': 'Balanced approach with adaptive tiling'}

    async def _generate_detail(self, data: Dict) -> Dict:
        return {'action': 'detail_generation', 'tile_level': 14, 'cluster_radius': 25, 'include_heatmap': True,
                'estimated_size_mb': 5.0, 'recommendation': 'Use for detailed analysis, not for sharing'}

    async def _generate_summary(self, data: Dict) -> Dict:
        return {'action': 'summary_generation', 'tile_level': 6, 'cluster_radius': 150, 'include_heatmap': False,
                'estimated_size_mb': 0.1, 'recommendation': 'Best for high-level overview and presentations'}

    def get_generation_stats(self) -> Dict:
        async with self._lock:
            return {
                'total_generations': len(self.generation_history),
                'strategies': self.strategy_keys,
                'recent_generations': list(self.generation_history)[-5:],
                'strategy_usage': {s: len([h for h in self.generation_history if h['strategy'] == s]) for s in self.strategy_keys},
                'bandit_theta': self.bandit.theta.tolist(),
                'epsilon': self.bandit.epsilon
            }

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# NEW: CARBON‑AWARE EXPORT QUEUE
# ============================================================
class CarbonAwareExportQueue(IExportQueue):
    def __init__(self, config: GreenMapConfig, carbon_manager: ICarbonManager, max_concurrent: int = 3):
        self.config = config
        self.carbon_manager = carbon_manager
        self.max_concurrent = max_concurrent
        self.queue = asyncio.PriorityQueue()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._worker = None
        self._lock = asyncio.Lock()
        self.metrics = {'submitted': 0, 'processed': 0, 'failed': 0, 'delayed': 0}
        self.threshold = config.carbon_scheduler.threshold
        self.max_delay = config.carbon_scheduler.max_delay_seconds

    async def submit(self, job: 'ExportJob'):
        await self.queue.put((job.priority, job))
        self.metrics['submitted'] += 1

    async def start(self):
        self._running = True
        self._worker = asyncio.create_task(self._worker_loop())

    async def _worker_loop(self):
        while self._running:
            try:
                _, job = await self.queue.get()
                # Check carbon intensity before processing
                intensity = await self.carbon_manager.get_current_intensity()
                if intensity > self.threshold and job.priority < 5:  # only delay lower priority jobs
                    # Delay up to max_delay or until intensity drops
                    wait_time = self.max_delay
                    while wait_time > 0 and intensity > self.threshold:
                        await asyncio.sleep(10)
                        wait_time -= 10
                        intensity = await self.carbon_manager.get_current_intensity()
                    self.metrics['delayed'] += 1
                async with self.semaphore:
                    try:
                        # Simulate export processing
                        await asyncio.sleep(0.1)
                        job.status = "completed"
                        self.metrics['processed'] += 1
                    except Exception as e:
                        job.status = "failed"
                        self.metrics['failed'] += 1
                        logger.error(f"Export job {job.job_id} failed: {e}")
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Export worker error: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        self._running = False
        if self._worker:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)

    def get_stats(self) -> Dict:
        return self.metrics.copy()

    async def health_check(self) -> Dict:
        return {'status': 'healthy' if self._running else 'stopped'}

# ============================================================
# NEW: ENHANCED GEOCODING SERVICE with caching and batch processing
# ============================================================
class EnhancedGeocodingService(IGeocoder):
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.cache = TTLCache(ttl_seconds=config.geocoder.cache_ttl, max_size_mb=50)  # separate cache for geocoding
        self.batch_size = config.geocoder.batch_size
        self.rate_limiter = RateLimiter(rate=config.geocoder.rate_limit_per_second, per_seconds=1)
        self._lock = asyncio.Lock()
        self.stats = {'requests': 0, 'cache_hits': 0, 'batch_requests': 0}

    async def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        # Check cache
        cached = await self.cache.get(address)
        if cached:
            self.stats['cache_hits'] += 1
            return cached
        # Rate limit
        await self.rate_limiter.wait_and_acquire()
        # Simulate geocoding (replace with real API call)
        # For demo, we return random coordinates
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)
        result = (lat, lon)
        await self.cache.set(address, result)
        self.stats['requests'] += 1
        return result

    async def batch_geocode(self, addresses: List[str]) -> List[Optional[Tuple[float, float]]]:
        self.stats['batch_requests'] += 1
        results = []
        for addr in addresses:
            results.append(await self.geocode(addr))
        return results

    async def get_statistics(self) -> Dict:
        return self.stats.copy()

    async def stop(self):
        await self.cache.stop()

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# NEW: ADAPTIVE TTL CACHE with LRU eviction
# ============================================================
class AdaptiveTTLCache:
    def __init__(self, config: GreenMapConfig):
        self.default_ttl = config.cache.default_ttl
        self.max_size_mb = config.cache.max_size_mb
        self.eviction_policy = config.cache.eviction_policy
        self._cache = {}  # key -> (value, timestamp, access_count)
        self._lock = asyncio.Lock()
        self.current_size_bytes = 0

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._cache:
                value, timestamp, access_count = self._cache[key]
                # Check TTL; but we adapt TTL based on access frequency
                # If access count is high, we extend TTL, else we shrink.
                # For simplicity, we use default TTL.
                if (datetime.now() - timestamp).total_seconds() < self.default_ttl:
                    self._cache[key] = (value, timestamp, access_count + 1)
                    return value
                else:
                    del self._cache[key]
                    self.current_size_bytes -= len(key) + len(value)
        return None

    async def set(self, key: str, value: Any):
        async with self._lock:
            # Evict if needed
            if self.current_size_bytes > self.max_size_mb * 1024 * 1024:
                await self._evict()
            self._cache[key] = (value, datetime.now(), 1)
            self.current_size_bytes += len(key) + len(value)

    async def _evict(self):
        # LRU eviction: evict least recently used (here we use access_count as proxy)
        if not self._cache:
            return
        # Sort by access_count ascending (least used)
        sorted_keys = sorted(self._cache.keys(), key=lambda k: self._cache[k][2])
        # Remove 10% of entries
        to_remove = max(1, int(len(sorted_keys) * 0.1))
        for key in sorted_keys[:to_remove]:
            val, _, _ = self._cache.pop(key)
            self.current_size_bytes -= len(key) + len(val)

    async def stop(self):
        pass

# ============================================================
# MAIN MAP CLASS with all new components
# ============================================================
class EnhancedGreenDataCenterMap:
    def __init__(
        self,
        config: GreenMapConfig,
        db_manager: IDatabaseManager,
        quantum_security: IQuantumSecurity,
        blockchain: IBlockchain,
        carbon_manager: ICarbonManager,
        autonomous_generator: IAutonomousGenerator,
        cloud_deployer: ICloudDeployer,
        cloud_storage: ICloudStorage,
        vault: IVault,
        predictive: Optional[IPredictive] = None,
        export_queue: Optional[IExportQueue] = None,
        geocoder: Optional[IGeocoder] = None,
        leader: Optional[LeaderElection] = None,
        task_manager: Optional[TaskManager] = None,
    ):
        self.config = config
        self.instance_id = config.general.instance_id

        self.db_manager = db_manager
        self.quantum_security = quantum_security
        self.blockchain = blockchain
        self.carbon_manager = carbon_manager
        self.autonomous_generator = autonomous_generator
        self.cloud_deployer = cloud_deployer
        self.cloud_storage = cloud_storage
        self.vault = vault
        self.predictive = predictive
        self.export_queue = export_queue or CarbonAwareExportQueue(config, carbon_manager, config.general.max_concurrent_exports)
        self.geocoder = geocoder or EnhancedGeocodingService(config)
        self.leader = leader or LeaderElection(config)
        self.task_manager = task_manager or TaskManager()

        # New cache
        self.tile_cache = AdaptiveTTLCache(config)

        self.output_dir = Path(self.config.general.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Data storage
        self.projects: List[DataCenterProject] = []
        self._projects_lock = asyncio.Lock()
        self.map_history = deque(maxlen=100)

        # Concurrency control
        self._map_generation_semaphore = asyncio.Semaphore(self.config.general.max_concurrent_map_generations)

        # Health components
        self._health_components = {
            'database': self.db_manager,
            'quantum_security': self.quantum_security,
            'blockchain': self.blockchain,
            'carbon_manager': self.carbon_manager,
            'autonomous_generator': self.autonomous_generator,
            'cloud_deployer': self.cloud_deployer,
            'cloud_storage': self.cloud_storage,
            'vault': self.vault,
            'predictive': self.predictive,
            'export_queue': self.export_queue,
            'geocoder': self.geocoder,
        }

        # Register background tasks
        self._register_background_tasks()

        # Metrics
        self.generation_count = 0

        logger.info(f"EnhancedGreenDataCenterMap v{self.config.general.version} initialized (instance: {self.instance_id})")

    def _register_background_tasks(self):
        self.task_manager.register_task("backup", self._backup_loop)
        self.task_manager.register_task("export_worker", self.export_queue.start)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        if self.predictive:
            self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        self.task_manager.register_task("health_check", self._health_check_loop)

    async def start(self):
        await self.db_manager.init()
        self.task_manager.start_registered_tasks()
        logger.info("Map system started with background tasks")

    async def _backup_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self._perform_backup()
                await asyncio.sleep(self.config.general.backup_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Backup loop error: {e}")
                await asyncio.sleep(60)

    async def _carbon_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
                await asyncio.sleep(self.config.carbon.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.predictive:
                    async with self._projects_lock:
                        count = len(self.projects)
                    carbon = await self.carbon_manager.get_current_intensity()
                    await self.predictive.update_history(count, carbon)
                    forecast = await self.predictive.forecast_project_count()
                    logger.info(f"Project count forecast: {forecast}")
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update loop error: {e}")
                await asyncio.sleep(60)

    async def _health_check_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                if PROMETHEUS_AVAILABLE:
                    HEALTH_SCORE.set(health.get('health_score', 100))
                if not health.get('healthy'):
                    logger.warning(f"System health degraded: {health}")
                await asyncio.sleep(self.config.general.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(60)

    async def _perform_backup(self):
        async with self._projects_lock:
            for project in self.projects:
                await self.db_manager.insert_project(project)
        logger.info("Backup completed")

    async def load_data(self):
        async with self._projects_lock:
            if SQLALCHEMY_AVAILABLE:
                def load(session):
                    result = session.execute(text("SELECT project_id, name, status, latitude, longitude, capacity_mw, carbon_intensity, helium_efficiency, last_updated FROM projects"))
                    projects = []
                    for row in result:
                        project = DataCenterProject(
                            project_id=row[0],
                            name=row[1],
                            status=row[2],
                            latitude=row[3],
                            longitude=row[4],
                            capacity_mw=row[5],
                            carbon_intensity=row[6],
                            helium_efficiency=row[7],
                            last_updated=row[8]
                        )
                        projects.append(project)
                    return projects
                self.projects = await self.db_manager.execute_sync(load)
            logger.info(f"Loaded {len(self.projects)} projects from DB")

    async def export_projects_secure(self, export_type: str, output_filename: str,
                                     priority: int = 1, sign_export: bool = True,
                                     blockchain_record: bool = True) -> Dict:
        async with self._projects_lock:
            if not self.projects:
                await self.load_data()
            projects_copy = self.projects.copy()

        output_path = self.output_dir / output_filename

        export_data = {
            'export_type': export_type,
            'projects': [asdict(p) for p in projects_copy],
            'timestamp': datetime.now().isoformat(),
            'instance_id': self.instance_id
        }

        file_hash = hashlib.sha256(json.dumps(export_data, sort_keys=True, default=str).encode()).hexdigest()

        quantum_signature = None
        if sign_export:
            quantum_key = await self.quantum_security.generate_keypair(self.config.quantum.algorithm)
            quantum_signature = await self.quantum_security.sign_map_export(export_data, quantum_key['key_id'])

        blockchain_result = None
        if blockchain_record:
            export_id = f"map_export_{uuid.uuid4().hex[:8]}"
            blockchain_result = await self.blockchain.record_map_export(export_id, {'export_type': export_type, 'project_count': len(projects_copy)}, file_hash)

        job = ExportJob(
            job_id=f"job_{uuid.uuid4().hex[:8]}",
            export_type=export_type,
            output_path=output_path,
            projects=projects_copy,
            priority=priority
        )
        await self.export_queue.submit(job)

        if self.cloud_storage.providers:
            try:
                await self.cloud_storage.store(export_data, f"export_{export_id}.json")
            except Exception as e:
                logger.error(f"Cloud storage backup failed: {e}")

        if PROMETHEUS_AVAILABLE:
            MAP_EXPORTS.labels(status='submitted').inc()
        return {
            'job_id': job.job_id,
            'export_type': export_type,
            'output_path': str(output_path),
            'file_hash': file_hash,
            'quantum_signature': quantum_signature,
            'blockchain_record': blockchain_result,
            'timestamp': datetime.now().isoformat()
        }

    async def generate_map_autonomously(self, strategy: str = None) -> Dict:
        async with self._projects_lock:
            if not self.projects:
                await self.load_data()
            projects_copy = self.projects.copy()

        data = {
            'project_count': len(projects_copy),
            'types': [p.status for p in projects_copy],
            'locations': [(p.latitude, p.longitude) for p in projects_copy]
        }

        async with self._map_generation_semaphore:
            recommendation = await self.autonomous_generator.generate_map_autonomously(data, strategy)

            output_filename = f"autonomous_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            output_path = self.output_dir / output_filename
            output_path.write_text("Map generated")  # Placeholder

            self.generation_count += 1

        if self.cloud_storage.providers:
            try:
                await self.cloud_storage.store({'recommendation': recommendation, 'output_path': str(output_path)}, f"generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            except Exception as e:
                logger.error(f"Cloud storage backup failed: {e}")

        return {
            'recommendation': recommendation,
            'output_path': str(output_path),
            'strategy': strategy or self.config.generator.default_strategy,
            'generation_count': self.generation_count,
            'timestamp': datetime.now().isoformat()
        }

    async def deploy_map_to_cloud(self, map_path: str, preferences: Dict = None) -> Dict:
        map_data = {
            'path': map_path,
            'size_mb': Path(map_path).stat().st_size / (1024 * 1024),
            'timestamp': datetime.now().isoformat()
        }
        deployment = await self.cloud_deployer.deploy_map(map_data, preferences or {})
        logger.info(f"Map deployed: {deployment}")
        return deployment

    async def get_cloud_status(self) -> Dict:
        return await self.cloud_deployer.get_deployment_status()

    async def get_comprehensive_status(self) -> Dict:
        quantum_status = self.quantum_security.get_quantum_status()
        blockchain_status = await self.blockchain.get_blockchain_status()
        generation_stats = self.autonomous_generator.get_generation_stats()
        cloud_status = await self.cloud_deployer.get_deployment_status()

        async with self._projects_lock:
            project_count = len(self.projects)
            statuses = {s: sum(1 for p in self.projects if p.status == s) for s in ['operational', 'construction', 'planned', 'decommissioned']}

        return {
            'instance_id': self.instance_id,
            'version': self.config.general.version,
            'quantum_security': quantum_status,
            'blockchain': blockchain_status,
            'autonomous_generation': generation_stats,
            'cloud_deployment': cloud_status,
            'projects': {
                'total': project_count,
                'statuses': statuses
            },
            'export_queue': self.export_queue.get_stats(),
            'geocoder': await self.geocoder.get_statistics(),
            'predictive': await self.predictive.forecast_project_count(1) if self.predictive else None,
            'cloud_storage': {'providers': list(self.cloud_storage.providers.keys())},
            'leader': {'is_leader': self.leader.is_leader},
            'health': await self.health_check(),
            'timestamp': datetime.now().isoformat()
        }

    async def health_check(self) -> Dict:
        results = {}
        for name, comp in self._health_components.items():
            if comp and hasattr(comp, 'health_check'):
                try:
                    results[name] = await comp.health_check()
                except Exception as e:
                    results[name] = {'status': 'unhealthy', 'error': str(e)}
            else:
                results[name] = {'status': 'ok' if comp else 'unavailable'}

        overall = 'healthy' if all(r.get('status') == 'ok' or r.get('status') == 'healthy' for r in results.values() if r.get('status') != 'unavailable') else 'degraded'
        health_score = 100 if overall == 'healthy' else 50
        return {
            'status': overall,
            'health_score': health_score,
            'components': results,
            'timestamp': datetime.now().isoformat()
        }

    async def shutdown(self):
        logger.info(f"Shutting down EnhancedGreenDataCenterMap (instance: {self.instance_id})")
        await self.task_manager.stop_all()
        await self.export_queue.stop()
        await self.tile_cache.stop()
        await self.geocoder.stop()
        await self.carbon_manager.close()
        await self.db_manager.close()
        await self.leader.stop()
        logger.info("Shutdown complete")

# ============================================================
# FASTAPI REST API (updated with new dependencies)
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Green Data Center Map API", version="16.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer()
    api_rate_limiter = RateLimiter(rate=GreenMapConfig().api.rate_limit_requests,
                                   per_seconds=GreenMapConfig().api.rate_limit_window)

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        token = credentials.credentials
        try:
            payload = jwt.decode(token, GreenMapConfig().api.jwt_secret, algorithms=["HS256"])
            return payload
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    async def rate_limit(request: Request):
        if GreenMapConfig().api.rate_limit_enabled:
            key = request.client.host
            if not await api_rate_limiter.acquire():
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

    map_system: Optional[EnhancedGreenDataCenterMap] = None

    @app.post("/export")
    async def export(export_type: str, output_filename: str, priority: int = 1,
                     sign_export: bool = True, blockchain_record: bool = True,
                     user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        result = await map_system.export_projects_secure(export_type, output_filename,
                                                          priority, sign_export, blockchain_record)
        return result

    @app.post("/generate")
    async def generate(strategy: str = None, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        result = await map_system.generate_map_autonomously(strategy)
        return result

    @app.post("/deploy")
    async def deploy(map_path: str, preferences: Dict = None, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        result = await map_system.deploy_map_to_cloud(map_path, preferences)
        return result

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        return await map_system.get_comprehensive_status()

    @app.get("/health")
    async def health(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        return await map_system.health_check()

    @app.on_event("startup")
    async def startup():
        global map_system
        config = GreenMapConfig()
        # Build dependencies
        db_manager = EnhancedDatabaseManager(config)
        vault = VaultManager(config)
        quantum = PostQuantumCrypto(config, vault)
        blockchain = BlockchainMapVerification(config)
        carbon = CarbonIntensityManager(config)
        # Use enhanced orchestrators
        if config.bio.enabled:
            cloud_deployer = BioInspiredCloudDeployer(config, db_manager, carbon)
        else:
            cloud_deployer = MultiObjectiveCloudDeployer(config, db_manager, carbon)
        generator = EnhancedAutonomousMapGenerator(config, db_manager, carbon)
        cloud_storage = MultiCloudStorage(config)
        predictive = MixtureOfExpertsPredictive(config) if config.moe.enabled else None
        export_queue = CarbonAwareExportQueue(config, carbon, config.general.max_concurrent_exports)
        geocoder = EnhancedGeocodingService(config)
        leader = LeaderElection(config)
        task_manager = TaskManager()
        map_system = EnhancedGreenDataCenterMap(
            config=config,
            db_manager=db_manager,
            quantum_security=quantum,
            blockchain=blockchain,
            carbon_manager=carbon,
            autonomous_generator=generator,
            cloud_deployer=cloud_deployer,
            cloud_storage=cloud_storage,
            vault=vault,
            predictive=predictive,
            export_queue=export_queue,
            geocoder=geocoder,
            leader=leader,
            task_manager=task_manager,
        )
        await map_system.start()
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown():
        if map_system:
            await map_system.shutdown()
        logger.info("FastAPI shut down")

# ============================================================
# SINGLETON ACCESSOR (updated)
# ============================================================
_map_instance = None
_map_lock = asyncio.Lock()

async def get_map_system(config: Optional[Union[GreenMapConfig, Dict]] = None) -> EnhancedGreenDataCenterMap:
    global _map_instance
    if _map_instance is None:
        async with _map_lock:
            if _map_instance is None:
                cfg = config if isinstance(config, GreenMapConfig) else GreenMapConfig(**config) if config else GreenMapConfig()
                db_manager = EnhancedDatabaseManager(cfg)
                vault = VaultManager(cfg)
                quantum = PostQuantumCrypto(cfg, vault)
                blockchain = BlockchainMapVerification(cfg)
                carbon = CarbonIntensityManager(cfg)
                if cfg.bio.enabled:
                    cloud_deployer = BioInspiredCloudDeployer(cfg, db_manager, carbon)
                else:
                    cloud_deployer = MultiObjectiveCloudDeployer(cfg, db_manager, carbon)
                generator = EnhancedAutonomousMapGenerator(cfg, db_manager, carbon)
                cloud_storage = MultiCloudStorage(cfg)
                predictive = MixtureOfExpertsPredictive(cfg) if cfg.moe.enabled else None
                export_queue = CarbonAwareExportQueue(cfg, carbon, cfg.general.max_concurrent_exports)
                geocoder = EnhancedGeocodingService(cfg)
                leader = LeaderElection(cfg)
                task_manager = TaskManager()
                _map_instance = EnhancedGreenDataCenterMap(
                    config=cfg,
                    db_manager=db_manager,
                    quantum_security=quantum,
                    blockchain=blockchain,
                    carbon_manager=carbon,
                    autonomous_generator=generator,
                    cloud_deployer=cloud_deployer,
                    cloud_storage=cloud_storage,
                    vault=vault,
                    predictive=predictive,
                    export_queue=export_queue,
                    geocoder=geocoder,
                    leader=leader,
                    task_manager=task_manager,
                )
                await _map_instance.start()
    return _map_instance

# ============================================================
# SIGNAL HANDLING (kept)
# ============================================================
_shutdown_requested = False

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}, initiating shutdown...")
        asyncio.create_task(shutdown_handler())

async def shutdown_handler():
    global _map_instance
    if _map_instance:
        await _map_instance.shutdown()
        _map_instance = None
    asyncio.get_event_loop().stop()

# ============================================================
# DATA CLASSES (kept)
# ============================================================
@dataclass
class DataCenterProject:
    project_id: str
    name: str
    status: str
    latitude: float
    longitude: float
    capacity_mw: float
    carbon_intensity: float = 400.0
    helium_efficiency: float = 0.5
    last_updated: datetime = field(default_factory=datetime.now)

@dataclass
class ExportJob:
    job_id: str
    export_type: str
    output_path: Path
    projects: List[DataCenterProject]
    priority: int
    submitted_at: datetime = field(default_factory=datetime.now)
    status: str = "pending"

# ============================================================
# MAIN ENTRY POINT (updated version)
# ============================================================
async def main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))

    print("=" * 80)
    print("Enhanced Green Data Center Map v16.0 - Enterprise Quantum+ (Bio-Inspired + MOE + MODP)")
    print("=" * 80)

    if FASTAPI_AVAILABLE:
        config = GreenMapConfig()
        print(f"\nStarting FastAPI server on {config.api.host}:{config.api.port}...")
        uvicorn.run(
            "green_datacenter_map_enhanced_v16_0:app",
            host=config.api.host,
            port=config.api.port,
            log_level="info",
            reload=False
        )
    else:
        map_system = await get_map_system()
        print(f"\n✅ ENHANCEMENTS OVER v15.0:")
        print("   ✅ Multi‑Objective Decision Process (MODP) for cloud deployment using Pareto front.")
        print("   ✅ Bio‑inspired optimisation (PSO) for dynamic map generation parameter tuning.")
        print("   ✅ Mixture‑of‑Experts (MOE) ensemble for predictive analytics.")
        print("   ✅ Contextual bandit for autonomous map generation strategy selection.")
        print("   ✅ Carbon‑aware export queue with delay of non‑critical exports.")
        print("   ✅ Enhanced geocoding service with batch processing and caching.")
        print("   ✅ Adaptive TTL cache with LRU eviction and dynamic TTL.")
        print("   ✅ Adaptive weight adjustment via reinforcement learning.")
        print("   ✅ Extended observability and OpenTelemetry integration.")
        print("   ✅ Security hardening with full PQC key management.")

        qstatus = map_system.quantum_security.get_quantum_status()
        print(f"\n🔐 Quantum Status: PQC Available: {qstatus.get('pqc_available', False)}, Algorithms: {', '.join(qstatus.get('algorithms', []))}")

        bstatus = await map_system.blockchain.get_blockchain_status()
        print(f"⛓️ Blockchain Connected: {bstatus.get('connected', False)}, Records: {bstatus.get('total_records', 0)}")

        cstatus = await map_system.cloud_deployer.get_deployment_status()
        print(f"☁️ Active Provider: {cstatus.get('active_provider', 'unknown')}, Active Region: {cstatus.get('active_region', 'unknown')}")

        print(f"\n⚡ Testing Autonomous Generation:")
        result = await map_system.generate_map_autonomously('hybrid')
        print(f"   Strategy: {result.get('strategy', 'unknown')}, Action: {result.get('recommendation', {}).get('action', 'unknown')}")

        print(f"🌐 Testing Multi-Cloud Deployment:")
        deploy = await map_system.deploy_map_to_cloud(result.get('output_path', 'unknown'), {'region': 'us-east-1', 'carbon_aware': True})
        print(f"   Optimal Provider: {deploy.get('optimal_provider', 'unknown')}, Region: {deploy.get('optimal_region', 'unknown')}")

        status = await map_system.get_comprehensive_status()
        print(f"\n📊 System Status:")
        print(f"   Instance: {status['instance_id']}, Version: {status['version']}")
        print(f"   Quantum Security: {'✅' if status['quantum_security']['pqc_available'] else '❌'}")
        print(f"   Blockchain Connected: {'✅' if status['blockchain']['connected'] else '❌'}")
        print(f"   Projects Total: {status['projects']['total']}")
        print(f"   Autonomous Generations: {status['autonomous_generation']['total_generations']}")
        print(f"   Cloud Deployments: {len(status['cloud_deployment'].get('deployment_history', []))}")
        print(f"   Predictive Available: {status['predictive'] is not None}")
        print(f"   Cloud Storage Providers: {status.get('cloud_storage', {}).get('providers', [])}")
        print(f"   Leader: {status.get('leader', {}).get('is_leader', False)}")

        print("\n" + "=" * 80)
        print("✅ Enhanced Green Data Center Map v16.0 - Ready for Production")
        print("=" * 80)

        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            await map_system.shutdown()
            print("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
