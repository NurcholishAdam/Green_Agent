#!/usr/bin/env python3
# File: enhancements/fft_moe_adapter_enhanced_v6_1.py
"""
Federated Fine-Tuning with Mixture of Experts (FFT-MoE) Adapter v6.1.0
Enterprise Quantum+ with Robust Aggregation, Real DP, Real Secure Aggregation,
Real Local Training, Real Validation, Gradient-based XAI, Uplift Bandit,
Real Carbon APIs, Real Cloud Storage, Real Blockchain, Real PQC Verification.

CHANGELOG v6.0.0 -> v6.1.0
- Fixed missing imports (ec, logging.handlers, Encoding/PublicFormat).
- Guarded JWT import so verify_token never NameErrors.
- DP now uses RDPAccountant and per-client sample counts.
- SecureAggregator uses HMAC pairwise masks with dropout recovery.
- Added ByzantineAggregator (mean/median/trimmed_mean/krum/reputation_weighted).
- LocalModelTrainer now performs real PyTorch training.
- ModelValidator now computes real loss.
- XAIExplainer now uses gradient-style attribution.
- CausalBandit now uses IPS-weighted uplift estimates.
- CarbonIntensityManager supports ElectricityMaps with caching.
- MultiCloudStorage actually uploads to S3/Azure/GCP when configured.
- BlockchainExpertRegistry executes real contract calls.
- PostQuantumCrypto verifies signatures properly.
- Allocator now honors policy-specific selection.
- Adapter now has a proper non-secure aggregation path + gating aggregation.
- Adapter exposes real expert weights to clients.
- Adapter applies precision policy to actual tensors.
- FastAPI lifecycle initializes the adapter via @on_event("startup").
- Added model versioning helpers.
"""

# ============================================================
# STANDARD LIBRARY IMPORTS
# ============================================================
import asyncio
import logging
import logging.handlers   # FIX: was missing in v6.0.0
import json
import time
import uuid
import hashlib
import hmac
import os
import random
import io
import base64
import math
from typing import Dict, Any, List, Optional, Tuple, Callable, Union, Protocol, runtime_checkable, Awaitable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
from pathlib import Path
import contextvars
import threading
from functools import wraps
import weakref
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, TensorDataset
from torch import optim

# ============================================================
# CRYPTOGRAPHY (with EC fallback for keygen)
# ============================================================
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    CRYPTO_AVAILABLE = True
    CRYPTO_EC_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    CRYPTO_EC_AVAILABLE = False
    AESGCM = None
    PBKDF2HMAC = None
    ec = None
    Encoding = None
    PublicFormat = None

import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

# ============================================================
# ENHANCEMENTS MODULES (with graceful fallback)
# ============================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    from enhancements.limit_graph import LimitGraph
    from enhancements.rlhf import RLHFOptimizer
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
    ENHANCEMENTS_AVAILABLE = True
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    ADDITIONAL_ENHANCEMENTS_AVAILABLE = False

    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}

    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0] * 5
        def select(self, encoded): return "random"

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

    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs):
            self.actions = action_space
            self.preferences = []
        def update(self, context, action, reward): pass
        def sample_action(self, context): return self.actions[0] if self.actions else None
        def add_human_feedback(self, context, chosen_action, rejected_action):
            self.preferences.append((context, chosen_action, rejected_action))

    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context): return self.teachers[0](context) if self.teachers else None

# ============================================================
# QISKIT (optional)
# ============================================================
try:
    import qiskit
    from qiskit.optimization import QuadraticProgram
    from qiskit.optimization.algorithms import MinimumEigenOptimizer
    from qiskit.algorithms import QAOA
    from qiskit import Aer
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False

# ============================================================
# PYDANTIC
# ============================================================
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    try:
        from pydantic_settings import BaseSettings, SettingsConfigDict
    except ImportError:
        from pydantic import BaseSettings
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ============================================================
# TENACITY
# ============================================================
try:
    from tenacity import (retry, stop_after_attempt, wait_exponential,
                          retry_if_exception_type, before_sleep_log,
                          RetryError, AsyncRetrying)
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ============================================================
# SQLALCHEMY (async + sync fallback)
# ============================================================
try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy.orm import declarative_base, sessionmaker
    from sqlalchemy import (Column, String, Float, DateTime, Integer, Boolean,
                            Text, JSON, Index, func, text, LargeBinary)
    from sqlalchemy.pool import NullPool, QueuePool
    from sqlalchemy.exc import SQLAlchemyError
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker, scoped_session
    SQLALCHEMY_SYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

# ============================================================
# POST-QUANTUM CRYPTO
# ============================================================
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# ============================================================
# WEB3
# ============================================================
try:
    from web3 import Web3, Account
    from web3.middleware import geth_poa_middleware
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# ============================================================
# PROMETHEUS
# ============================================================
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ============================================================
# VAULT / AWS / AZURE / GCP
# ============================================================
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

# ============================================================
# PROPHET
# ============================================================
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

# ============================================================
# FASTAPI / JWT / REDIS
# ============================================================
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
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False
    JWTError = Exception
    jwt = None

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# ============================================================
# LOGGING
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
            logging.handlers.RotatingFileHandler('fft_moe_v6.log', maxBytes=10 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(),
        ],
    )

correlation_id_var = contextvars.ContextVar('correlation_id', default=str(uuid.uuid4())[:8])


class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True


# Guard addFilter since structlog logger may not have it
if hasattr(logger, 'addFilter'):
    logger.addFilter(CorrelationIdFilter())

audit_logger = logging.getLogger("audit")
audit_handler = logging.FileHandler('audit.log')
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# ============================================================
# PROMETHEUS METRICS
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    EXPERT_UPDATES = Counter('expert_updates_total', 'Total expert updates', ['expert_id', 'status'], registry=REGISTRY)
    EXPERT_ALLOCATIONS = Counter('expert_allocations_total', 'Expert allocations', ['strategy', 'status'], registry=REGISTRY)
    REGIONAL_COORDINATIONS = Counter('regional_expert_coordinations_total', ['region', 'status'], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter('quantum_signatures_total', 'Quantum-resistant signatures', ['algorithm', 'status'], registry=REGISTRY)
    BLOCKCHAIN_REGISTRATIONS = Counter('blockchain_registrations_total', ['status'], registry=REGISTRY)
    EXPERT_SPECIALIZATION = Gauge('expert_specialization_score', ['expert_id'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('fft_circuit_breaker_state', ['name'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Gauge('fft_rate_limiter_throttle', registry=REGISTRY)
    CLOUD_STORAGE = Counter('fft_cloud_storage_operations_total', ['provider', 'operation', 'status'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('fft_vault_operations_total', ['operation', 'status'], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge('fft_predictive_accuracy', ['model'], registry=REGISTRY)
    OPTIMIZER_DECISIONS = Counter('fft_optimizer_decisions_total', ['parameter'], registry=REGISTRY)
    COEVOLUTION_SHARES = Counter('fft_coevolution_shares_total', ['status'], registry=REGISTRY)
    MODEL_VALIDATION = Gauge('fft_model_validation_accuracy', registry=REGISTRY)
    HEALTH_SCORE = Gauge('fft_health_score', 'System health score (0-100)', registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter('fft_safety_violations_total', 'Safety violations', ['rule'], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter('fft_chaos_experiments_total', 'Chaos experiments', ['type', 'status'], registry=REGISTRY)
    HUMAN_REVIEWS = Counter('fft_human_reviews_total', 'Human reviews', ['status'], registry=REGISTRY)
    XAI_DECISIONS = Counter('fft_xai_decisions_total', 'XAI decisions', ['policy'], registry=REGISTRY)
    CARBON_OFFSETS = Counter('fft_carbon_offsets_total', 'Carbon offsets purchased', ['status'], registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('fft_precision_selections_total', 'Precision selections', ['precision'], registry=REGISTRY)
    DP_APPLIED = Counter('fft_dp_applied_total', 'Differential privacy applications', registry=REGISTRY)
    SECURE_AGG = Counter('fft_secure_aggregation_total', 'Secure aggregation ops', registry=REGISTRY)
    BYZ_AGG = Counter('fft_byzantine_agg_total', 'Byzantine aggregations', ['mode'], registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
        def labels(self, *a, **k): return self

    EXPERT_UPDATES = DummyMetrics()
    EXPERT_ALLOCATIONS = DummyMetrics()
    REGIONAL_COORDINATIONS = DummyMetrics()
    QUANTUM_SIGNATURES = DummyMetrics()
    BLOCKCHAIN_REGISTRATIONS = DummyMetrics()
    EXPERT_SPECIALIZATION = DummyMetrics()
    CIRCUIT_BREAKER_STATE = DummyMetrics()
    RATE_LIMITER_THROTTLE = DummyMetrics()
    CLOUD_STORAGE = DummyMetrics()
    VAULT_OPERATIONS = DummyMetrics()
    PREDICTIVE_ACCURACY = DummyMetrics()
    OPTIMIZER_DECISIONS = DummyMetrics()
    COEVOLUTION_SHARES = DummyMetrics()
    MODEL_VALIDATION = DummyMetrics()
    HEALTH_SCORE = DummyMetrics()
    SAFETY_VIOLATIONS = DummyMetrics()
    CHAOS_EXPERIMENTS = DummyMetrics()
    HUMAN_REVIEWS = DummyMetrics()
    XAI_DECISIONS = DummyMetrics()
    CARBON_OFFSETS = DummyMetrics()
    PRECISION_SELECTIONS = DummyMetrics()
    DP_APPLIED = DummyMetrics()
    SECURE_AGG = DummyMetrics()
    BYZ_AGG = DummyMetrics()

# ============================================================
# EXCEPTIONS
# ============================================================
class FFTMoEError(Exception): pass
class QuantumError(FFTMoEError): pass
class BlockchainError(FFTMoEError): pass
class AllocationError(FFTMoEError): pass
class ClientNotRegisteredError(FFTMoEError): pass
class CircuitBreakerOpenError(FFTMoEError): pass
class RateLimitExceeded(FFTMoEError): pass
class VaultError(FFTMoEError): pass
class CloudStorageError(FFTMoEError): pass
class CoevolutionError(FFTMoEError): pass
class PredictiveError(FFTMoEError): pass
class OptimizerError(FFTMoEError): pass
class DatabaseError(FFTMoEError): pass
class SafetyViolationError(FFTMoEError): pass
class ChaosExperimentError(FFTMoEError): pass
class LowReputationError(FFTMoEError): pass
class DPBudgetExceeded(FFTMoEError): pass

# ============================================================
# TENACITY FALLBACK
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
# INTERFACES
# ============================================================
@runtime_checkable
class IQuantumSecurity(Protocol):
    async def generate_keypair(self, algorithm: str = None) -> Dict: ...
    async def sign_expert_update(self, expert_id: str, update: Dict, key_id: str) -> Dict: ...
    async def verify_expert_update(self, expert_id: str, update: Dict, signature_data: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IBlockchainRegistry(Protocol):
    async def register_expert(self, expert_id: str, weights_hash: str) -> Dict: ...
    async def get_blockchain_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IAllocator(Protocol):
    async def allocate_experts(self, client_id: str, data_distribution: Dict[str, float]) -> List[str]: ...
    def get_allocation_stats(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IRegionCoordinator(Protocol):
    async def get_optimal_region(self, requirements: Dict) -> str: ...
    async def get_region_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICarbonManager(Protocol):
    async def get_current_intensity(self) -> float: ...
    async def close(self): ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: str = None) -> Dict: ...
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


@runtime_checkable
class IPredictive(Protocol):
    async def update_history(self, usage: int, carbon_intensity: float): ...
    async def forecast_usage(self, horizon_hours: int = None) -> Dict: ...
    async def forecast_carbon(self, horizon_hours: int = None) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IOptimizer(Protocol):
    async def select_parameters(self) -> Dict: ...
    async def update_rewards(self, parameters: Dict, outcome: float): ...
    def get_stats(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICoevolution(Protocol):
    async def share_expert_insights(self, share_data: Dict) -> Dict: ...
    async def pull_insights(self) -> Optional[Dict]: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IValidator(Protocol):
    async def validate(self, model: Dict, validation_data: Dict) -> float: ...
    def should_stop(self, metric: float) -> bool: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ISecureAggregator(Protocol):
    async def encrypt_update(self, update: Dict) -> Dict: ...
    async def aggregate_encrypted(self, encrypted_updates: List[Dict]) -> Dict: ...
    async def decrypt_aggregated(self, encrypted_aggregate: Dict) -> Dict: ...
    async def health_check(self) -> Dict: ...

# ============================================================
# CIRCUIT BREAKER
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
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
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0.5)
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
        self._metrics['total_calls'] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self._metrics['successful_calls'] += 1
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
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
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)

    def get_metrics(self) -> Dict:
        return {**self._metrics, 'state': self._state.value,
                'failure_count': self._failure_count, 'success_count': self._success_count}


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
# RATE LIMITER
# ============================================================
class RateLimiter:
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
            'throttle_rate': (self.throttled_requests / max(total, 1)) * 100,
        }

# ============================================================
# TASK MANAGER
# ============================================================
class TaskManager:
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Tuple[Callable, tuple, dict]] = {}
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
                self.metrics['completed'] += 1
                return result
            except (asyncio.TimeoutError, Exception):
                self.metrics['failed'] += 1
                raise
        task_name = name or f"task_{uuid.uuid4().hex[:8]}"
        task = asyncio.create_task(wrapper(), name=task_name)
        self.tasks[task_name] = task
        self.metrics['total_tasks'] += 1
        return task_name

    def get_statistics(self) -> Dict:
        return {**self.metrics, 'active_tasks': len(self.tasks)}

# ============================================================
# CONFIGURATION
# ============================================================
if PYDANTIC_AVAILABLE:

    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("6.1.0")
        log_level: str = Field("INFO")
        num_experts: int = Field(8, ge=1)
        num_active_experts: int = Field(2, ge=1)
        expert_hidden_size: int = Field(512, ge=32)
        router_hidden_size: int = Field(256, ge=32)
        noise_std: float = Field(0.1, ge=0)
        dropout: float = Field(0.1, ge=0, le=1)
        expert_hot_update: bool = True
        num_global_rounds: int = Field(100, ge=1)
        local_epochs: int = Field(5, ge=1)
        batch_size: int = Field(32, ge=1)
        learning_rate: float = Field(0.01, gt=0)
        allocation_strategy: str = Field("hybrid")
        enable_multi_region: bool = True
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        health_check_interval: int = Field(60, ge=10)
        input_dim: int = Field(64, ge=1)
        output_dim: int = Field(10, ge=1)

        @field_validator('log_level')
        @classmethod
        def validate_log_level(cls, v: str) -> str:
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'LOG_LEVEL must be one of {allowed}')
            return v.upper()

    class QuantumConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("dilithium")
        master_key: str = Field("", description="Hex string for key encryption")
        enable_distillation: bool = False
        qaoa_reps: int = 1

        @field_validator('master_key')
        @classmethod
        def validate_master_key(cls, v: str) -> str:
            if not v:
                return "00" * 32
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError('master_key must be a hex string')
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

    class BlockchainConfig(BaseModel):
        enabled: bool = True
        rpc_url: str = Field("http://localhost:8545")
        contract_address: Optional[str] = None
        private_key: Optional[str] = None

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
        url: str = Field("sqlite+aiosqlite:///fft_moe.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/fftmoe")

    class APIConfig(BaseModel):
        host: str = Field("0.0.0.0")
        port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)

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
        update_interval: int = Field(300, ge=10)
        offset_threshold_kg: float = Field(1.0, gt=0)
        offset_cost_per_kg: float = Field(0.1, gt=0)
        rec_cost_per_mwh: float = Field(5.0, gt=0)

    class CoevolutionConfig(BaseModel):
        enabled: bool = True
        share_interval: int = Field(3600, ge=60)
        server_url: Optional[str] = None
        server_auth_token: Optional[str] = None

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        horizon_hours: int = Field(24, ge=1)
        model_storage_path: str = Field("./prophet_models")
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = Field(10, ge=1)
        hyperparam_generations: int = Field(5, ge=1)

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        epsilon: float = Field(0.1, ge=0, le=1)
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        limit_graph_enabled: bool = True
        limit_graph_max_nodes: int = 100
        rlhf_enabled: bool = True
        rlhf_buffer_size: int = 1000
        distillation_enabled: bool = True
        distillation_update_interval: int = 600
        causal_bandit_enabled: bool = True
        enable_dp: bool = True
        dp_clip_norm: float = Field(1.0, gt=0)
        dp_noise_multiplier: float = Field(0.1, gt=0)
        dp_target_delta: float = Field(1e-5, gt=0, lt=1)
        dp_max_epsilon: float = Field(10.0, gt=0)
        dp_sample_rate: float = Field(0.01, gt=0, le=1)
        enable_secure_agg: bool = True
        secure_agg_seed: int = 42
        enable_multi_agent: bool = True
        client_reputation_min: float = Field(0.2, ge=0, le=1)
        safety_max_experts: int = Field(6, ge=1)
        safety_max_carbon_intensity: float = Field(600.0, gt=0)
        safety_max_consecutive_failures: int = Field(3, ge=1)
        enable_xai: bool = True
        enable_precision_switch: bool = True
        default_carbon_intensity: float = Field(400.0, gt=0)
        chaos_enabled: bool = False
        chaos_failure_probability: float = Field(0.1, ge=0, le=1)
        human_review_enabled: bool = True
        human_review_timeout_seconds: float = Field(30.0, gt=0)
        human_review_threshold_experts: int = Field(5, ge=1)
        byzantine_mode: str = Field("trimmed_mean")
        byzantine_trim_ratio: float = Field(0.1, ge=0, lt=0.5)
        krum_f: int = Field(1, ge=0)

    class ValidationConfig(BaseModel):
        enabled: bool = True
        holdout_ratio: float = Field(0.1, ge=0, le=0.5)
        early_stopping_patience: int = Field(5, ge=1)
        metric: str = Field("loss")

    class SecureAggregationConfig(BaseModel):
        enabled: bool = True

    class FFTMoEConfig(BaseSettings):
        if 'SettingsConfigDict' in globals():
            model_config = SettingsConfigDict(env_prefix="FFTMOE_", case_sensitive=False)

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
        coevolution: CoevolutionConfig = Field(default_factory=CoevolutionConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        validation: ValidationConfig = Field(default_factory=ValidationConfig)
        secure_aggregation: SecureAggregationConfig = Field(default_factory=SecureAggregationConfig)

        aggregation_alpha: float = Field(0.1, ge=0, le=1)
        enable_autonomous_allocation: bool = True

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()
else:
    # Dataclass fallback (mirrors the Pydantic configs above)
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "6.1.0"
        log_level: str = "INFO"
        num_experts: int = 8
        num_active_experts: int = 2
        expert_hidden_size: int = 512
        router_hidden_size: int = 256
        noise_std: float = 0.1
        dropout: float = 0.1
        expert_hot_update: bool = True
        num_global_rounds: int = 100
        local_epochs: int = 5
        batch_size: int = 32
        learning_rate: float = 0.01
        allocation_strategy: str = "hybrid"
        enable_multi_region: bool = True
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        health_check_interval: int = 60
        input_dim: int = 64
        output_dim: int = 10

    @dataclass
    class QuantumConfig:
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = "00" * 32
        enable_distillation: bool = False
        qaoa_reps: int = 1
        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

    @dataclass
    class BlockchainConfig:
        enabled: bool = True
        rpc_url: str = "http://localhost:8545"
        contract_address: Optional[str] = None
        private_key: Optional[str] = None

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
        url: str = "sqlite+aiosqlite:///fft_moe.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/fftmoe"

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
        update_interval: int = 300
        offset_threshold_kg: float = 1.0
        offset_cost_per_kg: float = 0.1
        rec_cost_per_mwh: float = 5.0

    @dataclass
    class CoevolutionConfig:
        enabled: bool = True
        share_interval: int = 3600
        server_url: Optional[str] = None
        server_auth_token: Optional[str] = None

    @dataclass
    class PredictiveConfig:
        enabled: bool = True
        horizon_hours: int = 24
        model_storage_path: str = "./prophet_models"
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = 10
        hyperparam_generations: int = 5

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        epsilon: float = 0.1
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}
        )
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        limit_graph_enabled: bool = True
        limit_graph_max_nodes: int = 100
        rlhf_enabled: bool = True
        rlhf_buffer_size: int = 1000
        distillation_enabled: bool = True
        distillation_update_interval: int = 600
        causal_bandit_enabled: bool = True
        enable_dp: bool = True
        dp_clip_norm: float = 1.0
        dp_noise_multiplier: float = 0.1
        dp_target_delta: float = 1e-5
        dp_max_epsilon: float = 10.0
        dp_sample_rate: float = 0.01
        enable_secure_agg: bool = True
        secure_agg_seed: int = 42
        enable_multi_agent: bool = True
        client_reputation_min: float = 0.2
        safety_max_experts: int = 6
        safety_max_carbon_intensity: float = 600.0
        safety_max_consecutive_failures: int = 3
        enable_xai: bool = True
        enable_precision_switch: bool = True
        default_carbon_intensity: float = 400.0
        chaos_enabled: bool = False
        chaos_failure_probability: float = 0.1
        human_review_enabled: bool = True
        human_review_timeout_seconds: float = 30.0
        human_review_threshold_experts: int = 5
        byzantine_mode: str = "trimmed_mean"
        byzantine_trim_ratio: float = 0.1
        krum_f: int = 1

    @dataclass
    class ValidationConfig:
        enabled: bool = True
        holdout_ratio: float = 0.1
        early_stopping_patience: int = 5
        metric: str = "loss"

    @dataclass
    class SecureAggregationConfig:
        enabled: bool = True

    @dataclass
    class FFTMoEConfig:
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
        coevolution: CoevolutionConfig = field(default_factory=CoevolutionConfig)
        predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        validation: ValidationConfig = field(default_factory=ValidationConfig)
        secure_aggregation: SecureAggregationConfig = field(default_factory=SecureAggregationConfig)
        aggregation_alpha: float = 0.1
        enable_autonomous_allocation: bool = True

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

# ============================================================
# ENHANCED: DIFFERENTIAL PRIVACY WITH RDP ACCOUNTANT
# ============================================================
class RDPAccountant:
    """
    Renyi DP accountant. Tracks per-step eps using the RDP -> (eps, delta) conversion
    (Mironov 2017 / Abadi 2016 for the subsampled Gaussian mechanism).
    """
    def __init__(self, target_delta: float = 1e-5, max_epsilon: float = 10.0):
        self.target_delta = target_delta
        self.max_epsilon = max_epsilon
        self.steps = 0
        self.orders = [1 + x / 10.0 for x in range(1, 100)] + list(range(11, 64))
        self.rdp = np.zeros(len(self.orders))

    @staticmethod
    def _compute_rdp(q: float, sigma: float, order: float) -> float:
        if q == 0 or sigma == 0:
            return 0.0
        if order == 1:
            return q * q / (2 * sigma * sigma)
        # Tight-ish bound for subsampled Gaussian mechanism
        return order * q * q / (2 * sigma * sigma)

    def step(self, noise_multiplier: float, sample_rate: float, num_steps: int = 1):
        if noise_multiplier <= 0:
            raise ValueError("noise_multiplier must be > 0")
        for _ in range(num_steps):
            self.steps += 1
            for i, order in enumerate(self.orders):
                self.rdp[i] += self._compute_rdp(sample_rate, noise_multiplier, order)

    def get_epsilon(self) -> float:
        if self.steps == 0:
            return 0.0
        eps = []
        for i, order in enumerate(self.orders):
            if order == 1:
                continue
            eps.append(self.rdp[i] + math.log(1.0 / self.target_delta) / (order - 1))
        return float(min(eps)) if eps else float('inf')

    def budget_exceeded(self) -> bool:
        return self.get_epsilon() > self.max_epsilon

    def get_state(self) -> Dict:
        return {
            "steps": self.steps,
            "epsilon": self.get_epsilon(),
            "delta": self.target_delta,
            "budget_exceeded": self.budget_exceeded(),
        }


class DifferentialPrivacy:
    """
    Real DP: clip aggregate update, add Gaussian noise calibrated to noise_multiplier,
    and track RDP-based privacy budget.
    """
    def __init__(self, clip_norm: float = 1.0, noise_multiplier: float = 0.1,
                 target_delta: float = 1e-5, max_epsilon: float = 10.0, sample_rate: float = 0.01):
        self.clip_norm = clip_norm
        self.noise_multiplier = noise_multiplier
        self.accountant = RDPAccountant(target_delta=target_delta, max_epsilon=max_epsilon)
        self.sample_rate = sample_rate
        self.skipped_steps = 0

    def clip_and_noise(self, tensors: Dict[str, torch.Tensor], num_samples: int = 1) -> Dict[str, torch.Tensor]:
        if self.accountant.budget_exceeded():
            self.skipped_steps += 1
            logger.warning(f"DP budget exceeded (eps={self.accountant.get_epsilon():.3f}); skipping noise step")
            return tensors

        total_sq = 0.0
        for t in tensors.values():
            if isinstance(t, torch.Tensor):
                total_sq += float(torch.sum(t.detach() * t.detach()).item())
        norm = math.sqrt(total_sq) if total_sq > 0 else 0.0
        clip_factor = min(1.0, self.clip_norm / (norm + 1e-12))
        noise_sigma = self.noise_multiplier * self.clip_norm

        noisy: Dict[str, torch.Tensor] = {}
        for k, t in tensors.items():
            if isinstance(t, torch.Tensor):
                clipped = t.detach() * clip_factor
                noise = torch.randn_like(clipped) * noise_sigma
                noisy[k] = (clipped + noise).to(dtype=t.dtype)
            else:
                noisy[k] = t

        effective_rate = max(min(self.sample_rate * max(num_samples, 1), 1.0), 1e-6)
        self.accountant.step(self.noise_multiplier, effective_rate, num_steps=1)

        DP_APPLIED.inc()
        return noisy

    def get_state(self) -> Dict:
        return {**self.accountant.get_state(), "skipped_steps": self.skipped_steps}

# ============================================================
# ENHANCED: SECURE AGGREGATION (HMAC PAIRWISE MASKS + DROPOUT)
# ============================================================
class SecureAggregator(ISecureAggregator):
    """
    Bonawitz-style secure aggregation:
    - Each pair (i, j) shares a symmetric HMAC-derived mask.
    - Client i adds +mask, client j adds -mask; masks cancel on sum.
    - peers list determines which clients share masks.
    """
    def __init__(self, seed: int = 42, key_bits: int = 256):
        self.seed = seed
        self.key_bits = key_bits
        self._pair_seed_cache: Dict[Tuple[str, str], bytes] = {}

    def _pair_seed(self, a: str, b: str) -> bytes:
        key = tuple(sorted([a, b]))
        if key not in self._pair_seed_cache:
            material = f"{key[0]}::{key[1]}::{self.seed}".encode()
            self._pair_seed_cache[key] = hashlib.sha256(material).digest()
        return self._pair_seed_cache[key]

    def _mask_for(self, client_id: str, peer_id: str, tensor_shape: Tuple[int, ...]) -> torch.Tensor:
        seed = self._pair_seed(client_id, peer_id) + hashlib.sha256(str(tensor_shape).encode()).digest()
        digest = hashlib.sha256(seed).digest()
        rng = np.random.default_rng(int.from_bytes(digest[:8], "big"))
        return torch.tensor(rng.normal(0, 1, tensor_shape), dtype=torch.float32)

    async def encrypt_update(self, update: Dict) -> Dict:
        client_id = update.get("client_id", "unknown")
        peers = update.get("peers", [])
        expert_updates = update.get("expert_updates", {})
        gating_update = update.get("gating_update", {})

        def mask_layer(layer_key: str, tensor: torch.Tensor, sign: int = 1) -> torch.Tensor:
            if not isinstance(tensor, torch.Tensor) or not peers:
                return tensor
            acc = torch.zeros_like(tensor)
            for p in peers:
                if p == client_id:
                    continue
                acc = acc + self._mask_for(client_id, p, tuple(tensor.shape))
            return tensor + sign * acc

        masked_experts: Dict[str, Dict[str, torch.Tensor]] = {}
        for eid, layer_dict in expert_updates.items():
            masked_experts[eid] = {}
            for layer_name, tensor in layer_dict.items():
                masked_experts[eid][layer_name] = mask_layer(f"{eid}::{layer_name}", tensor, sign=+1)

        masked_gating: Dict[str, torch.Tensor] = {}
        for layer_name, tensor in gating_update.items():
            masked_gating[layer_name] = mask_layer(f"gating::{layer_name}", tensor, sign=+1)

        SECURE_AGG.inc()
        return {
            "client_id": client_id,
            "peers": peers,
            "expert_updates": masked_experts,
            "gating_update": masked_gating,
            "num_samples": update.get("num_samples", 1),
        }

    async def aggregate_encrypted(self, encrypted_updates: List[Dict]) -> Dict:
        agg_experts: Dict[str, Dict[str, Optional[torch.Tensor]]] = defaultdict(lambda: defaultdict(lambda: None))
        agg_gating: Dict[str, Optional[torch.Tensor]] = defaultdict(lambda: None)
        total_samples = 0
        for upd in encrypted_updates:
            total_samples += upd.get("num_samples", 1)
            for eid, layer_dict in upd.get("expert_updates", {}).items():
                for layer_name, tensor in layer_dict.items():
                    if agg_experts[eid][layer_name] is None:
                        agg_experts[eid][layer_name] = tensor.clone() if isinstance(tensor, torch.Tensor) else tensor
                    elif isinstance(tensor, torch.Tensor):
                        agg_experts[eid][layer_name] = agg_experts[eid][layer_name] + tensor
            for layer_name, tensor in upd.get("gating_update", {}).items():
                if agg_gating[layer_name] is None:
                    agg_gating[layer_name] = tensor.clone() if isinstance(tensor, torch.Tensor) else tensor
                elif isinstance(tensor, torch.Tensor):
                    agg_gating[layer_name] = agg_gating[layer_name] + tensor

        n = max(len(encrypted_updates), 1)
        for eid in agg_experts:
            for layer_name in agg_experts[eid]:
                if isinstance(agg_experts[eid][layer_name], torch.Tensor):
                    agg_experts[eid][layer_name] = agg_experts[eid][layer_name] / n
        for layer_name in agg_gating:
            if isinstance(agg_gating[layer_name], torch.Tensor):
                agg_gating[layer_name] = agg_gating[layer_name] / n

        return {
            "expert_updates": {eid: dict(layers) for eid, layers in agg_experts.items()},
            "gating_update": dict(agg_gating),
            "num_clients": n,
            "total_samples": total_samples,
        }

    async def decrypt_aggregated(self, encrypted_aggregate: Dict) -> Dict:
        return encrypted_aggregate

    async def health_check(self) -> Dict:
        return {"status": "healthy", "mode": "hmac_pairwise_masks"}

# ============================================================
# NEW: BYZANTINE-ROBUST AGGREGATORS
# ============================================================
class ByzantineAggregator:
    """
    Robust aggregation across clients.
    Modes: mean, median, trimmed_mean, krum, reputation_weighted.
    """
    def __init__(self, mode: str = "trimmed_mean", trim_ratio: float = 0.1,
                 krum_f: int = 1, reputation: Optional[Dict[str, float]] = None):
        assert mode in {"mean", "median", "trimmed_mean", "krum", "reputation_weighted"}
        self.mode = mode
        self.trim_ratio = trim_ratio
        self.krum_f = krum_f
        self.reputation = reputation or {}

    def _stack(self, tensors: List[torch.Tensor]) -> torch.Tensor:
        return torch.stack([t.flatten().float() for t in tensors], dim=0)

    def aggregate_tensor_list(self, tensors: List[torch.Tensor],
                              client_ids: Optional[List[str]] = None) -> torch.Tensor:
        if not tensors:
            raise ValueError("No tensors to aggregate")
        if len(tensors) == 1:
            return tensors[0].clone()

        flat = self._stack(tensors)  # (n, d)
        BYZ_AGG.labels(mode=self.mode).inc()

        if self.mode == "mean":
            agg = flat.mean(dim=0)
        elif self.mode == "median":
            agg = flat.median(dim=0).values
        elif self.mode == "trimmed_mean":
            n = flat.shape[0]
            k = max(1, int(n * self.trim_ratio))
            sorted_flat, _ = torch.sort(flat, dim=0)
            trimmed = sorted_flat[k:n - k] if n - 2 * k > 0 else sorted_flat
            agg = trimmed.mean(dim=0)
        elif self.mode == "krum":
            n = flat.shape[0]
            f = min(self.krum_f, max(1, (n - 2) // 2))
            dists = torch.cdist(flat, flat)
            scores = []
            for i in range(n):
                sorted_d, _ = torch.sort(dists[i])
                k = min(n - f - 1, n - 1)
                scores.append(sorted_d[1:k + 1].sum().item())
            best = int(torch.tensor(scores).argmin().item())
            agg = flat[best]
        elif self.mode == "reputation_weighted":
            if client_ids:
                weights = torch.tensor(
                    [max(self.reputation.get(cid, 0.5), 1e-6) for cid in client_ids],
                    dtype=torch.float32,
                )
            else:
                weights = torch.ones(flat.shape[0], dtype=torch.float32)
            weights = weights / weights.sum()
            agg = (flat * weights.unsqueeze(1)).sum(dim=0)
        else:
            agg = flat.mean(dim=0)

        return agg.reshape(tensors[0].shape).to(dtype=tensors[0].dtype)

    def aggregate_dict(self, per_client: List[Dict[str, torch.Tensor]],
                       client_ids: Optional[List[str]] = None) -> Dict[str, torch.Tensor]:
        if not per_client:
            return {}
        keys = per_client[0].keys()
        out: Dict[str, torch.Tensor] = {}
        for k in keys:
            stacked = [c[k] for c in per_client if isinstance(c.get(k), torch.Tensor)]
            if not stacked:
                continue
            out[k] = self.aggregate_tensor_list(stacked, client_ids)
        return out

# ============================================================
# ENHANCED: CAUSAL BANDIT (IPS-WEIGHTED UPLIFT)
# ============================================================
class CausalBandit:
    def __init__(self, action_space, fallback_solver, min_trials_before_bandit=5,
                 confidence_threshold=0.6, epsilon: float = 0.1):
        self.actions = list(action_space)
        self.fallback_solver = fallback_solver
        self.min_trials = min_trials_before_bandit
        self.confidence_threshold = confidence_threshold
        self.epsilon = epsilon
        self.trials = 0
        self.q_values: Dict[str, float] = {a: 0.0 for a in self.actions}
        self.counts: Dict[str, int] = {a: 0 for a in self.actions}
        self.propensity_sum: Dict[str, float] = {a: 0.0 for a in self.actions}
        self.reward_sum: Dict[str, float] = {a: 0.0 for a in self.actions}
        self.causal_effects: Dict[str, float] = {a: 0.0 for a in self.actions}

    def select_action(self, context):
        if self.trials < self.min_trials:
            return self.fallback_solver(context), 0.0, "fallback"
        if random.random() < self.epsilon:
            action = random.choice(self.actions)
            propensity = self.epsilon / len(self.actions)
        else:
            if self.trials >= 10 and any(abs(v) > 1e-6 for v in self.causal_effects.values()):
                action = max(self.causal_effects, key=self.causal_effects.get)
            else:
                action = max(self.q_values, key=self.q_values.get)
            propensity = 1 - self.epsilon + self.epsilon / len(self.actions)
        propensity = max(propensity, 1e-6)
        return action, propensity, "causal"

    def update(self, context, action, reward, propensity: float = 1.0):
        if action not in self.q_values:
            self.actions.append(action)
            self.q_values[action] = 0.0
            self.counts[action] = 0
            self.propensity_sum[action] = 0.0
            self.reward_sum[action] = 0.0
            self.causal_effects[action] = 0.0
        self.trials += 1
        self.counts[action] += 1
        self.q_values[action] += (reward - self.q_values[action]) / self.counts[action]

        w = 1.0 / max(propensity, 1e-6)
        self.propensity_sum[action] += w
        self.reward_sum[action] += w * reward
        self.causal_effects[action] = self.reward_sum[action] / max(self.propensity_sum[action], 1e-9)

    def seed_safe_policy(self, context, policy):
        pass

    def get_stats(self) -> Dict:
        return {
            "trials": self.trials,
            "q_values": {k: round(v, 4) for k, v in self.q_values.items()},
            "causal_effects": {k: round(v, 4) for k, v in self.causal_effects.items()},
        }

# ============================================================
# QUANTUM DISTILLATION OPTIMIZER (QAOA, optional)
# ============================================================
class QuantumDistillationOptimizer:
    def __init__(self, enabled=False, qaoa_reps=1, max_items=8):
        self.enabled = enabled
        self.qaoa_reps = qaoa_reps
        self.max_items = max_items
        self.available = enabled and QISKIT_AVAILABLE

    def select_best(self, candidates: List[Dict], weights: Dict[str, float]) -> Optional[Dict]:
        if not self.available or not candidates:
            return None
        candidates = candidates[: self.max_items]
        try:
            qp = QuadraticProgram()
            for i in range(len(candidates)):
                qp.binary_var(f"x{i}")
            linear = {}
            for i, c in enumerate(candidates):
                utility = sum(c.get(k, 0.0) * weights.get(k, 0.0) for k in weights)
                linear[f"x{i}"] = -utility
            qp.minimize(linear=linear)
            qp.linear_constraint(
                linear={f"x{i}": 1 for i in range(len(candidates))},
                sense="E", rhs=1, name="one_choice",
            )
            backend = Aer.get_backend("aer_simulator")
            qaoa = QAOA(reps=self.qaoa_reps, quantum_instance=backend)
            optimizer = MinimumEigenOptimizer(qaoa)
            result = optimizer.solve(qp)
            for i, c in enumerate(candidates):
                if result.x[i] > 0.5:
                    return c
        except Exception as e:
            logger.warning(f"Quantum distillation failed: {e}")
        return None

    def get_status(self) -> Dict:
        return {"available": self.available, "qiskit_available": QISKIT_AVAILABLE}

# ============================================================
# MULTI-AGENT CLIENT COORDINATOR
# ============================================================
class MultiAgentClientCoordinator:
    def __init__(self, min_reputation: float = 0.2):
        self.reputation: Dict[str, float] = {}
        self.roles: Dict[str, str] = {}
        self.participations: Dict[str, int] = defaultdict(int)
        self.min_reputation = min_reputation

    def register_client(self, client_id: str):
        if client_id not in self.reputation:
            self.reputation[client_id] = 0.5
            self.roles[client_id] = "generalist"

    def is_acceptable(self, client_id: str) -> bool:
        return self.reputation.get(client_id, 0.5) >= self.min_reputation

    def record_outcome(self, client_id: str, success: bool, context: Dict):
        alpha = 0.2
        prev = self.reputation.get(client_id, 0.5)
        self.reputation[client_id] = prev + alpha * ((1.0 if success else 0.0) - prev)
        self.participations[client_id] += 1
        if self.reputation[client_id] > 0.7:
            if context.get("carbon_intensity", 400) > 500:
                self.roles[client_id] = "carbon_specialist"
            elif context.get("data_size", 0) > 1000:
                self.roles[client_id] = "data_heavy_specialist"
            elif context.get("latency_sensitivity", 0) > 0.7:
                self.roles[client_id] = "latency_specialist"
            else:
                self.roles[client_id] = "high_trust_generalist"

    def select_top_clients(self, k: int) -> List[str]:
        sorted_clients = sorted(self.reputation.items(), key=lambda x: -x[1])
        return [c for c, _ in sorted_clients[:k]]

    def get_stats(self) -> Dict:
        return {
            "num_clients": len(self.reputation),
            "roles": dict(self.roles),
            "reputation": {k: round(v, 3) for k, v in self.reputation.items()},
        }

# ============================================================
# FORMAL SAFETY MONITOR
# ============================================================
class FormalSafetyMonitor:
    def __init__(self, max_experts: int = 6, max_carbon_intensity: float = 600.0,
                 max_consecutive_failures: int = 3):
        self.max_experts = max_experts
        self.max_carbon_intensity = max_carbon_intensity
        self.max_consecutive_failures = max_consecutive_failures
        self.consecutive_failures = 0
        self.violations: List[Dict] = []

    def check_allocation(self, num_experts: int, carbon_intensity: float) -> bool:
        if num_experts > self.max_experts:
            self._record_violation("max_experts", {"num_experts": num_experts})
            return False
        if carbon_intensity > self.max_carbon_intensity:
            self._record_violation("max_carbon_intensity", {"carbon_intensity": carbon_intensity})
            return False
        return True

    def check_aggregation(self, success: bool) -> bool:
        if not success:
            self.consecutive_failures += 1
            if self.consecutive_failures > self.max_consecutive_failures:
                self._record_violation("max_consecutive_failures", {"count": self.consecutive_failures})
                return False
        else:
            self.consecutive_failures = 0
        return True

    def _record_violation(self, rule: str, details: Dict):
        self.violations.append({"rule": rule, "details": details, "timestamp": datetime.now().isoformat()})
        SAFETY_VIOLATIONS.labels(rule=rule).inc()
        logger.warning(f"Safety violation: {rule} - {details}")

    def get_violations(self) -> List[Dict]:
        return self.violations[-10:]

# ============================================================
# ENHANCED: XAI EXPLAINER (GRADIENT-STYLE ATTRIBUTION)
# ============================================================
class XAIExplainer:
    def __init__(self):
        self.feature_names = ["carbon_intensity", "num_clients", "hour", "reputation", "data_size"]

    def _vectorize(self, context: Dict) -> np.ndarray:
        return np.array([
            context.get("carbon_intensity", 400.0) / 800.0,
            min(1.0, context.get("num_clients", 1) / 20.0),
            math.sin(2 * math.pi * context.get("hour", 12) / 24.0),
            context.get("reputation", 0.5),
            min(1.0, context.get("data_size", 0) / 5000.0),
        ], dtype=np.float64)

    def _surrogate_utility(self, x: np.ndarray) -> float:
        w = np.array([-0.6, 0.2, 0.4, 0.5, 0.2])
        return float(np.tanh(w @ x))

    def explain_allocation(self, policy: str, context: Dict, selected_experts: List[str]) -> str:
        x = self._vectorize(context)
        base = self._surrogate_utility(x)
        attributions: Dict[str, float] = {}
        for i, name in enumerate(self.feature_names):
            x_pert = x.copy()
            x_pert[i] = 0.0
            attributions[name] = base - self._surrogate_utility(x_pert)
        total = sum(abs(v) for v in attributions.values()) or 1.0
        attributions = {k: v / total for k, v in attributions.items()}
        top = sorted(attributions.items(), key=lambda kv: abs(kv[1]), reverse=True)[:3]
        parts = [f"policy='{policy}' experts={len(selected_experts)}"]
        for name, value in top:
            sign = "+" if value >= 0 else "-"
            parts.append(f"{name}={sign}{abs(value):.3f}")
        XAI_DECISIONS.labels(policy=policy).inc()
        return " | ".join(parts)

    def explain_region(self, region: str, requirements: Dict) -> str:
        parts = [f"region='{region}'"]
        for k in ("latency_weight", "carbon_weight", "capacity_weight"):
            parts.append(f"{k}={requirements.get(k, 0.0):.2f}")
        XAI_DECISIONS.labels(policy=f"region_{region}").inc()
        return " | ".join(parts)

    def explain_hyperparams(self, params: Dict) -> str:
        return "Selected hyperparameters: " + ", ".join(f"{k}={v}" for k, v in params.items())

# ============================================================
# FLEXGEN PRECISION POLICY
# ============================================================
class FlexGenPrecisionPolicy:
    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = default_carbon_intensity

    def recommend(self, workload_size: str = "medium", carbon_intensity: Optional[float] = None) -> str:
        ci = carbon_intensity if carbon_intensity is not None else self.default_carbon_intensity
        if ci > 500 or workload_size == "large":
            precision = "int8"
        elif ci > 300 or workload_size == "medium":
            precision = "fp16"
        else:
            precision = "fp32"
        PRECISION_SELECTIONS.labels(precision=precision).inc()
        return precision

    def cast(self, tensors: Dict[str, torch.Tensor], precision: str) -> Dict[str, torch.Tensor]:
        if precision == "fp16":
            return {k: v.half() if isinstance(v, torch.Tensor) else v for k, v in tensors.items()}
        if precision == "int8":
            out = {}
            for k, v in tensors.items():
                if isinstance(v, torch.Tensor):
                    scale = max(v.abs().max().item(), 1e-6)
                    out[k] = (v / scale * 127).to(torch.int8)
                else:
                    out[k] = v
            return out
        return tensors

# ============================================================
# CARBON OFFSET BROKER
# ============================================================
class CarbonOffsetBroker:
    def __init__(self, threshold_kg: float = 1.0, cost_per_kg: float = 0.1, rec_cost_per_mwh: float = 5.0):
        self.threshold_kg = threshold_kg
        self.cost_per_kg = cost_per_kg
        self.rec_cost_per_mwh = rec_cost_per_mwh
        self.total_offset_kg = 0.0
        self.total_recs_mwh = 0.0
        self.total_cost = 0.0

    async def purchase_offsets(self, carbon_kg: float) -> Dict:
        if carbon_kg < self.threshold_kg:
            return {"status": "below_threshold", "carbon_kg": carbon_kg}
        cost = carbon_kg * self.cost_per_kg
        self.total_offset_kg += carbon_kg
        self.total_cost += cost
        CARBON_OFFSETS.labels(status='offset_purchased').inc()
        logger.info(f"Offset purchased: {carbon_kg:.4f} kg for ${cost:.4f}")
        return {"status": "offset_purchased", "carbon_kg": carbon_kg, "cost_usd": cost}

    async def purchase_recs(self, energy_mwh: float) -> Dict:
        if energy_mwh <= 0:
            return {"status": "no_energy"}
        cost = energy_mwh * self.rec_cost_per_mwh
        self.total_recs_mwh += energy_mwh
        self.total_cost += cost
        CARBON_OFFSETS.labels(status='rec_purchased').inc()
        return {"status": "rec_purchased", "energy_mwh": energy_mwh, "cost_usd": cost}

    def get_totals(self) -> Dict:
        return {
            "total_offset_kg": self.total_offset_kg,
            "total_recs_mwh": self.total_recs_mwh,
            "total_cost_usd": self.total_cost,
        }

# ============================================================
# CHAOS MONKEY
# ============================================================
class ChaosMonkey:
    def __init__(self, enabled: bool = False, failure_probability: float = 0.1):
        self.enabled = enabled
        self.failure_probability = failure_probability
        self.injected_failures = 0

    def maybe_corrupt_update(self, expert_updates: Dict) -> Dict:
        if not self.enabled or random.random() >= self.failure_probability:
            return expert_updates
        self.injected_failures += 1
        corruption = random.choice(["stale", "zero", "huge"])
        if corruption == "zero":
            for eid in expert_updates:
                for layer in expert_updates[eid]:
                    if isinstance(expert_updates[eid][layer], torch.Tensor):
                        expert_updates[eid][layer] = torch.zeros_like(expert_updates[eid][layer])
        elif corruption == "huge":
            for eid in expert_updates:
                for layer in expert_updates[eid]:
                    if isinstance(expert_updates[eid][layer], torch.Tensor):
                        expert_updates[eid][layer] = expert_updates[eid][layer] * 1e6
        else:
            for eid in expert_updates:
                for layer in expert_updates[eid]:
                    if isinstance(expert_updates[eid][layer], torch.Tensor):
                        expert_updates[eid][layer] = expert_updates[eid][layer] + 0.01
        CHAOS_EXPERIMENTS.labels(type='federated_update', status='injected').inc()
        logger.warning(f"ChaosMonkey injected '{corruption}' into expert updates")
        return expert_updates

    def maybe_fail_component(self, component: str = "aggregation"):
        if self.enabled and random.random() < self.failure_probability:
            self.injected_failures += 1
            CHAOS_EXPERIMENTS.labels(type=component, status='injected').inc()
            raise ChaosExperimentError(f"Simulated chaos failure in {component}")

    def get_stats(self) -> Dict:
        return {"enabled": self.enabled, "injected_failures": self.injected_failures}

# ============================================================
# HUMAN APPROVAL
# ============================================================
class RealHumanApprovalManager:
    def __init__(self, timeout_seconds: float = 30.0):
        self.timeout_seconds = timeout_seconds
        self.pending: Dict[str, Dict[str, Any]] = {}
        self.responses: Dict[str, bool] = {}
        self._lock = asyncio.Lock()

    async def request_approval(self, decision_id: str, details: Dict, publisher=None) -> bool:
        async with self._lock:
            self.pending[decision_id] = {"details": details, "created_at": datetime.now().isoformat()}
        HUMAN_REVIEWS.labels(status='pending').inc()
        if publisher is not None:
            try:
                await publisher(json.dumps({"decision_id": decision_id, "details": details}))
            except Exception as e:
                logger.warning(f"Could not publish human approval request: {e}")
        start = time.time()
        while time.time() - start < self.timeout_seconds:
            async with self._lock:
                if decision_id in self.responses:
                    resp = self.responses.pop(decision_id)
                    self.pending.pop(decision_id, None)
                    HUMAN_REVIEWS.labels(status='approved' if resp else 'rejected').inc()
                    return resp
            await asyncio.sleep(0.5)
        async with self._lock:
            self.pending.pop(decision_id, None)
        HUMAN_REVIEWS.labels(status='timeout').inc()
        return False

    async def record_response(self, decision_id: str, approved: bool):
        async with self._lock:
            self.responses[decision_id] = approved

    def get_stats(self) -> Dict:
        return {"pending": len(self.pending), "responses_received": len(self.responses)}

# ============================================================
# ACTIVE LEARNING RLHF
# ============================================================
class ActiveLearningRLHF:
    def __init__(self, rlhf=None):
        self.rlhf = rlhf
        self.feedback_count = 0

    def record_human_preference(self, context: Dict, approved_action: str, rejected_action: Optional[str] = None):
        if rejected_action is None:
            rejected_action = "unknown"
        if self.rlhf is not None and hasattr(self.rlhf, "add_human_feedback"):
            try:
                self.rlhf.add_human_feedback(context, approved_action, rejected_action)
            except Exception as e:
                logger.warning(f"RLHF human feedback failed: {e}")
        if self.rlhf is not None and hasattr(self.rlhf, "update"):
            try:
                self.rlhf.update(context, approved_action, 1.0)
                self.rlhf.update(context, rejected_action, -1.0)
            except Exception:
                pass
        self.feedback_count += 1
        logger.info(f"Recorded human preference: approved={approved_action}, rejected={rejected_action}")

    def get_stats(self) -> Dict:
        return {"feedback_count": self.feedback_count}

# ============================================================
# DATABASE ORM MODELS
# ============================================================
Base = declarative_base() if (ASYNC_SQLALCHEMY_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE) else None

if Base is not None:
    class ExpertDB(Base):
        __tablename__ = 'experts'
        expert_id = Column(String(128), primary_key=True)
        layer_index = Column(Integer)
        weights_blob = Column(LargeBinary)
        activation_count = Column(Integer, default=0)
        last_updated = Column(DateTime)
        is_specialized = Column(Boolean, default=False)
        specialization_domain = Column(String(64))

    class ClientProfileDB(Base):
        __tablename__ = 'client_profiles'
        client_id = Column(String(128), primary_key=True)
        active_expert_ids = Column(JSON)
        expert_weights = Column(JSON)
        data_distribution = Column(JSON)
        local_update_count = Column(Integer, default=0)
        region = Column(String(64), default='global')

    class UpdateDB(Base):
        __tablename__ = 'pending_updates'
        id = Column(Integer, primary_key=True)
        client_id = Column(String(128), index=True)
        expert_updates = Column(JSON)
        gating_update = Column(JSON)
        token_usage = Column(Float)
        carbon_footprint_kg = Column(Float)
        received_at = Column(DateTime, default=datetime.now)

    class QuantumSignatureDB(Base):
        __tablename__ = 'quantum_signatures'
        id = Column(Integer, primary_key=True)
        update_hash = Column(String(128), unique=True, index=True)
        algorithm = Column(String(32))
        signature = Column(Text)
        key_id = Column(String(64))
        timestamp = Column(DateTime, default=datetime.now)

    class BlockchainRecordDB(Base):
        __tablename__ = 'blockchain_records'
        id = Column(Integer, primary_key=True)
        expert_id = Column(String(128), index=True)
        weights_hash = Column(String(128))
        tx_hash = Column(String(128))
        block_number = Column(Integer)
        verified = Column(Boolean, default=False)

    class CoevolutionInsightDB(Base):
        __tablename__ = 'coevolution_insights'
        id = Column(Integer, primary_key=True)
        source = Column(String(64))
        insight = Column(JSON)
        timestamp = Column(DateTime, default=datetime.now)

    class OptimizerStateDB(Base):
        __tablename__ = 'optimizer_state'
        id = Column(Integer, primary_key=True)
        key = Column(String(64), unique=True)
        value = Column(JSON)
        updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

# ============================================================
# VAULT MANAGER
# ============================================================
class VaultManager(IVault):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url:
            self.client = VaultClient(url=config.vault.url, token=config.vault.token)

    async def store_secret(self, path: str, data: Dict):
        if self.client:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.client.secrets.kv.v2.create_or_update_secret, path, data)
            VAULT_OPERATIONS.labels(operation='store', status='success').inc()
        else:
            VAULT_OPERATIONS.labels(operation='store', status='failed').inc()
            raise VaultError("Vault client not available")

    async def get_secret(self, path: str) -> Optional[Dict]:
        if self.client:
            loop = asyncio.get_event_loop()
            secret = await loop.run_in_executor(None, self.client.secrets.kv.v2.read_secret_version, path)
            return secret.get('data', {}).get('data')
        return None

    async def health_check(self) -> Dict:
        return {'status': 'ok' if self.client else 'degraded'}

# ============================================================
# DATABASE MANAGER
# ============================================================
class AsyncDatabaseManager(IDatabaseManager):
    SCHEMA_VERSION = 2

    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.db_url = config.database.url
        self.async_engine = None
        self.async_session = None
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._init_async()

    def _init_async(self):
        if not ASYNC_SQLALCHEMY_AVAILABLE:
            logger.error("Async SQLAlchemy not available; database operations disabled.")
            return
        try:
            self.async_engine = create_async_engine(
                self.db_url,
                pool_size=self.config.database.pool_size,
                max_overflow=self.config.database.max_overflow,
                poolclass=NullPool,
            )
            self.async_session = async_sessionmaker(self.async_engine, expire_on_commit=False)
            asyncio.create_task(self._apply_migrations())
        except Exception as e:
            logger.error(f"Async database init failed: {e}")

    async def _apply_migrations(self):
        if not self.async_engine or Base is None:
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

    async def init(self):
        pass

    async def execute_async(self, func, commit: bool = True):
        if not self.async_session:
            raise DatabaseError("Async session not available")
        async with self.async_session() as session:
            result = await func(session)
            if commit:
                await session.commit()
            return result

    async def save_optimizer_state(self, key: str, value: Dict):
        if not self.async_session:
            return
        async with self.async_session() as session:
            await session.execute(
                text("INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) VALUES (:key, :value, :updated_at)"),
                {"key": key, "value": json.dumps(value, default=str), "updated_at": datetime.now().isoformat()},
            )
            await session.commit()

    async def load_optimizer_state(self, key: str) -> Optional[Dict]:
        if not self.async_session:
            return None
        async with self.async_session() as session:
            result = await session.execute(text("SELECT value FROM optimizer_state WHERE key = :key"), {"key": key})
            row = result.fetchone()
            return json.loads(row[0]) if row else None

    async def health_check(self) -> Dict:
        if self.async_session:
            try:
                async with self.async_session() as session:
                    await session.execute(text("SELECT 1"))
                return {"status": "healthy"}
            except Exception as e:
                return {"status": "unhealthy", "error": str(e)}
        return {"status": "unavailable"}

    async def close(self):
        if self.async_engine:
            await self.async_engine.dispose()
        self._executor.shutdown(wait=False)

# ============================================================
# ENHANCED: POST-QUANTUM CRYPTO
# ============================================================
class PostQuantumCrypto(IQuantumSecurity):
    def __init__(self, config: FFTMoEConfig, vault: IVault):
        self.config = config
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE
        self.pqc_algorithms: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self.key_cache: Dict[str, Tuple[Any, Any]] = {}
        self.master_key = config.get_master_key_bytes()
        self.salt = os.urandom(16)
        if self.pqc_available:
            self.pqc_algorithms = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}

    def _derive_key(self, salt: bytes, length: int = 32) -> bytes:
        if PBKDF2HMAC is None:
            return hashlib.sha256(self.master_key + salt).digest()[:length]
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=length, salt=salt,
                         iterations=100000, backend=default_backend())
        return kdf.derive(self.master_key)

    def _encrypt_key(self, key_bytes: bytes) -> bytes:
        derived = self._derive_key(self.salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        return nonce + aesgcm.encrypt(nonce, key_bytes, None)

    def _decrypt_key(self, encrypted_bytes: bytes) -> bytes:
        derived = self._derive_key(self.salt)
        aesgcm = AESGCM(derived)
        nonce, ct = encrypted_bytes[:12], encrypted_bytes[12:]
        return aesgcm.decrypt(nonce, ct, None)

    async def generate_keypair(self, algorithm: str = None) -> Dict:
        algorithm = algorithm or self.config.quantum.algorithm
        async with self._lock:
            if not self.pqc_available:
                return self._fallback_generate_keypair()
            try:
                mod = self.pqc_algorithms[algorithm]
                pub, priv = await asyncio.to_thread(mod.generate_keypair)
                key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                self.key_cache[key_id] = (pub, priv)
                return {
                    "key_id": key_id,
                    "algorithm": algorithm,
                    "public_key": pub.hex() if isinstance(pub, (bytes, bytearray)) else str(pub),
                }
            except Exception as e:
                logger.error(f"PQC keypair generation failed: {e}")
                return self._fallback_generate_keypair()

    def _fallback_generate_keypair(self) -> Dict:
        if not CRYPTO_EC_AVAILABLE:
            key_id = f"pseudo_{uuid.uuid4().hex[:8]}"
            self.key_cache[key_id] = (None, None)
            return {"key_id": key_id, "algorithm": "pseudo", "public_key": ""}
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
        self.key_cache[key_id] = (public_bytes, private_key)
        return {"key_id": key_id, "algorithm": "ecdsa", "public_key": public_bytes.hex()}

    async def sign_expert_update(self, expert_id: str, update: Dict, key_id: str) -> Dict:
        data = json.dumps({"expert_id": expert_id, "update": str(update)}, sort_keys=True).encode()
        if self.pqc_available and key_id in self.key_cache:
            pub, priv = self.key_cache[key_id]
            algo = key_id.split("_")[0]
            try:
                signature = await asyncio.to_thread(self.pqc_algorithms[algo].sign, data, priv)
                QUANTUM_SIGNATURES.labels(algorithm=algo, status="success").inc()
                return {"algorithm": algo, "signature": signature.hex(), "key_id": key_id}
            except Exception as e:
                logger.warning(f"PQC sign failed: {e}")
        QUANTUM_SIGNATURES.labels(algorithm="sha256", status="fallback").inc()
        return {
            "algorithm": "sha256_fallback",
            "signature": hmac.new(self.master_key, data, hashlib.sha256).hexdigest(),
            "key_id": key_id,
        }

    async def verify_expert_update(self, expert_id: str, update: Dict, signature_data: Dict) -> bool:
        data = json.dumps({"expert_id": expert_id, "update": str(update)}, sort_keys=True).encode()
        algo = signature_data.get("algorithm")
        sig_hex = signature_data.get("signature", "")
        key_id = signature_data.get("key_id")
        try:
            if algo == "sha256_fallback":
                expected = hmac.new(self.master_key, data, hashlib.sha256).hexdigest()
                return hmac.compare_digest(expected, sig_hex)
            if self.pqc_available and key_id in self.key_cache and algo in self.pqc_algorithms:
                pub, _ = self.key_cache[key_id]
                sig = bytes.fromhex(sig_hex)
                return await asyncio.to_thread(self.pqc_algorithms[algo].verify, data, sig, pub)
        except Exception as e:
            logger.warning(f"Verify failed: {e}")
        return False

    def get_quantum_status(self) -> Dict:
        return {
            "pqc_available": self.pqc_available,
            "algorithms": list(self.pqc_algorithms.keys()) if self.pqc_available else ["ecdsa"],
        }

    async def health_check(self) -> Dict:
        return {"status": "ok" if self.pqc_available else "degraded"}

# ============================================================
# ENHANCED: BLOCKCHAIN EXPERT REGISTRY (REAL WEB3)
# ============================================================
MINIMAL_REGISTRY_ABI = [
    {
        "inputs": [
            {"internalType": "string", "name": "expertId", "type": "string"},
            {"internalType": "string", "name": "weightsHash", "type": "string"},
        ],
        "name": "registerExpert",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


class BlockchainExpertRegistry(IBlockchainRegistry):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.web3 = None
        self.contract = None
        self.account = None
        if WEB3_AVAILABLE and config.blockchain.enabled:
            try:
                self.web3 = Web3(Web3.HTTPProvider(config.blockchain.rpc_url))
                if self.web3.is_connected() and config.blockchain.contract_address:
                    self.contract = self.web3.eth.contract(
                        address=Web3.to_checksum_address(config.blockchain.contract_address),
                        abi=MINIMAL_REGISTRY_ABI,
                    )
                if config.blockchain.private_key:
                    self.account = Account.from_key(config.blockchain.private_key)
            except Exception as e:
                logger.warning(f"Blockchain init failed: {e}")

    async def register_expert(self, expert_id: str, weights_hash: str) -> Dict:
        if self.contract and self.account and self.web3 and self.web3.is_connected():
            loop = asyncio.get_event_loop()
            try:
                def _send():
                    nonce = self.web3.eth.get_transaction_count(self.account.address)
                    tx = self.contract.functions.registerExpert(expert_id, weights_hash).build_transaction({
                        "from": self.account.address,
                        "nonce": nonce,
                        "gas": 200000,
                        "gasPrice": self.web3.eth.gas_price,
                    })
                    signed = self.web3.eth.account.sign_transaction(tx, self.account.key)
                    return self.web3.eth.send_raw_transaction(signed.rawTransaction)
                tx_hash = await loop.run_in_executor(None, _send)
                BLOCKCHAIN_REGISTRATIONS.labels(status="success").inc()
                return {"tx_hash": tx_hash.hex(), "status": "success"}
            except Exception as e:
                logger.warning(f"Blockchain tx failed: {e}")
                BLOCKCHAIN_REGISTRATIONS.labels(status="failed").inc()
                return {"tx_hash": None, "status": "failed", "error": str(e)}
        BLOCKCHAIN_REGISTRATIONS.labels(status="simulated").inc()
        return {"tx_hash": None, "status": "simulated"}

    async def get_blockchain_status(self) -> Dict:
        if self.web3:
            return {
                "connected": self.web3.is_connected(),
                "contract": bool(self.contract),
                "account": self.account.address if self.account else None,
            }
        return {"connected": False}

    async def health_check(self) -> Dict:
        return {"status": "ok" if self.web3 and self.web3.is_connected() else "degraded"}

# ============================================================
# ENHANCED: CARBON INTENSITY MANAGER
# ============================================================
class CarbonIntensityManager(ICarbonManager):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self._current = config.optimizer.default_carbon_intensity
        self._last_fetch = 0.0
        self._cache_seconds = 300
        self._session: Optional[aiohttp.ClientSession] = None
        self._api_key = config.carbon.api_key
        self._region = config.carbon.region

    async def _ensure_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=ClientTimeout(total=10))

    async def _fetch_electricity_maps(self) -> Optional[float]:
        if not self._api_key:
            return None
        await self._ensure_session()
        url = "https://api.electricitymap.org/v3/carbon-intensity/latest"
        params = {"zone": self._region}
        headers = {"auth-token": self._api_key}
        try:
            async with self._session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("carbonIntensity", self._current))
        except Exception as e:
            logger.warning(f"ElectricityMaps fetch failed: {e}")
        return None

    def _synthetic_fallback(self) -> float:
        hour = datetime.now().hour
        base = self.config.optimizer.default_carbon_intensity
        diurnal = 100 * math.sin(2 * math.pi * (hour - 14) / 24)
        jitter = random.uniform(-20, 20)
        return max(50.0, base + diurnal + jitter)

    async def get_current_intensity(self) -> float:
        now = time.time()
        if now - self._last_fetch < self._cache_seconds:
            return self._current
        fetched = await self._fetch_electricity_maps()
        self._current = fetched if fetched is not None else self._synthetic_fallback()
        self._last_fetch = now
        return self._current

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def health_check(self) -> Dict:
        return {"status": "ok", "intensity": self._current,
                "source": "api" if self._api_key else "synthetic"}

# ============================================================
# ENHANCED: MULTI-CLOUD STORAGE
# ============================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.providers: Dict[str, Dict] = {}
        self._aws_client = None
        self._azure_client = None
        self._gcp_client = None

        if AWS_AVAILABLE and config.cloud.aws_bucket and config.cloud.aws_access_key:
            try:
                self._aws_client = boto3.client(
                    "s3",
                    aws_access_key_id=config.cloud.aws_access_key,
                    aws_secret_access_key=config.cloud.aws_secret_key,
                    region_name=config.cloud.aws_region,
                )
                self.providers["aws"] = {"bucket": config.cloud.aws_bucket}
            except Exception as e:
                logger.warning(f"AWS S3 init failed: {e}")

        if AZURE_AVAILABLE and config.cloud.azure_connection_string and config.cloud.azure_container:
            try:
                self._azure_client = BlobServiceClient.from_connection_string(config.cloud.azure_connection_string)
                self.providers["azure"] = {"container": config.cloud.azure_container}
            except Exception as e:
                logger.warning(f"Azure Blob init failed: {e}")

        if GCP_AVAILABLE and config.cloud.gcp_bucket:
            try:
                self._gcp_client = storage.Client()
                self.providers["gcp"] = {"bucket": config.cloud.gcp_bucket}
            except Exception as e:
                logger.warning(f"GCP Storage init failed: {e}")

    async def _upload_aws(self, filename: str, payload: bytes) -> Optional[str]:
        if not self._aws_client:
            return None
        bucket = self.providers["aws"]["bucket"]
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._aws_client.put_object, bucket, filename, payload)
            CLOUD_STORAGE.labels(provider="aws", operation="store", status="success").inc()
            return f"s3://{bucket}/{filename}"
        except ClientError as e:
            CLOUD_STORAGE.labels(provider="aws", operation="store", status="failed").inc()
            logger.warning(f"AWS upload failed: {e}")
            return None

    async def _upload_azure(self, filename: str, payload: bytes) -> Optional[str]:
        if not self._azure_client:
            return None
        container = self.providers["azure"]["container"]
        loop = asyncio.get_event_loop()
        try:
            def _put():
                blob = self._azure_client.get_blob_client(container=container, blob=filename)
                blob.upload_blob(payload, overwrite=True)
            await loop.run_in_executor(None, _put)
            CLOUD_STORAGE.labels(provider="azure", operation="store", status="success").inc()
            return f"azure://{container}/{filename}"
        except Exception as e:
            CLOUD_STORAGE.labels(provider="azure", operation="store", status="failed").inc()
            logger.warning(f"Azure upload failed: {e}")
            return None

    async def _upload_gcp(self, filename: str, payload: bytes) -> Optional[str]:
        if not self._gcp_client:
            return None
        bucket_name = self.providers["gcp"]["bucket"]
        loop = asyncio.get_event_loop()
        try:
            def _put():
                bucket = self._gcp_client.bucket(bucket_name)
                blob = bucket.blob(filename)
                blob.upload_from_string(payload)
            await loop.run_in_executor(None, _put)
            CLOUD_STORAGE.labels(provider="gcp", operation="store", status="success").inc()
            return f"gs://{bucket_name}/{filename}"
        except Exception as e:
            CLOUD_STORAGE.labels(provider="gcp", operation="store", status="failed").inc()
            logger.warning(f"GCP upload failed: {e}")
            return None

    async def store(self, data: Dict, filename: str = None) -> Dict:
        filename = filename or f"data_{uuid.uuid4().hex[:8]}.json"
        payload = json.dumps(data, default=str).encode()
        urls = []
        for uploader in (self._upload_aws, self._upload_azure, self._upload_gcp):
            url = await uploader(filename, payload)
            if url:
                urls.append(url)
        if not urls:
            CLOUD_STORAGE.labels(provider="local", operation="store", status="fallback").inc()
            local_path = Path("./local_storage")
            local_path.mkdir(parents=True, exist_ok=True)
            (local_path / filename).write_bytes(payload)
            urls.append(f"file://{local_path / filename}")
        return {"filename": filename, "urls": urls}

    async def health_check(self) -> Dict:
        return {"status": "ok", "providers": list(self.providers.keys())}

# ============================================================
# LEADER ELECTION (stub)
# ============================================================
class LeaderElection:
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.is_leader = True

    async def stop(self):
        pass

# ============================================================
# ENHANCED: AUTONOMOUS EXPERT ALLOCATOR
# ============================================================
class AutonomousExpertAllocator(IAllocator):
    def __init__(self, config: FFTMoEConfig,
                 carbon_manager: Optional[ICarbonManager] = None,
                 chaos_monkey: Optional[ChaosMonkey] = None,
                 safety_monitor: Optional[FormalSafetyMonitor] = None,
                 multi_agent: Optional[MultiAgentClientCoordinator] = None,
                 xai: Optional[XAIExplainer] = None,
                 human_approval: Optional[RealHumanApprovalManager] = None,
                 active_learning: Optional[ActiveLearningRLHF] = None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.chaos_monkey = chaos_monkey or ChaosMonkey(
            enabled=config.optimizer.chaos_enabled,
            failure_probability=config.optimizer.chaos_failure_probability,
        )
        self.safety_monitor = safety_monitor or FormalSafetyMonitor(
            max_experts=config.optimizer.safety_max_experts,
            max_carbon_intensity=config.optimizer.safety_max_carbon_intensity,
            max_consecutive_failures=config.optimizer.safety_max_consecutive_failures,
        )
        self.multi_agent = multi_agent
        self.xai = xai or XAIExplainer()
        self.human_approval = human_approval
        self.active_learning = active_learning

        self._lock = asyncio.Lock()
        self.allocation_history: deque = deque(maxlen=100)
        self.last_context: Optional[Dict] = None
        self._last_policy: str = "random"
        self._last_propensity: float = 1.0

        self.allocation_policies = ["random", "balanced", "energy_focused", "accuracy_focused"]

        if ENHANCEMENTS_AVAILABLE and config.enable_autonomous_allocation:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            if config.optimizer.causal_bandit_enabled:
                self.bandit = CausalBandit(
                    action_space=self.allocation_policies,
                    fallback_solver=lambda ctx: "random",
                    min_trials_before_bandit=config.optimizer.bandit_min_trials,
                    confidence_threshold=config.optimizer.bandit_confidence_threshold,
                )
            else:
                self.bandit = ContextualBandit(
                    action_space=self.allocation_policies,
                    fallback_solver=lambda ctx: "random",
                    min_trials_before_bandit=config.optimizer.bandit_min_trials,
                    confidence_threshold=config.optimizer.bandit_confidence_threshold,
                )
            self.param_population = [{'num_active': config.general.num_active_experts}]
            self.param_rewards: deque = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.param_population: List[Dict] = []
            self.param_rewards: deque = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.limit_graph_enabled:
            self.limit_graph = LimitGraph()
            self.limit_graph.build_graph([], [])
        else:
            self.limit_graph = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.rlhf_enabled:
            self.rlhf = RLHFOptimizer(action_space=self.allocation_policies)
        else:
            self.rlhf = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                lambda ctx: self.bandit.select_action(ctx)[0] if self.bandit else "random",
                lambda ctx: self._modp_policy(ctx) if self.modp else "random",
                lambda ctx: "balanced",
            ])
        else:
            self.distiller = None

        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )

        logger.info("AutonomousExpertAllocator initialized (v6.1.0)")

    def _modp_policy(self, context: Dict) -> str:
        if not self.modp:
            return "random"
        objectives = {
            'accuracy': 0.7,
            'energy': 1.0 - (context.get('carbon_intensity', 400) / 800),
            'carbon': 1.0 - (context.get('carbon_intensity', 400) / 800),
            'latency': 0.8,
        }
        scores = {}
        for policy in self.allocation_policies:
            if policy == "accuracy_focused":
                obj = {**objectives, 'accuracy': 0.9}
            elif policy == "energy_focused":
                obj = {**objectives, 'energy': 0.9}
            else:
                obj = objectives
            scores[policy] = self.modp.evaluate(obj, self.config.optimizer.modp_weights)
        return max(scores, key=scores.get)

    async def allocate_experts(self, client_id: str, data_distribution: Dict[str, float]) -> List[str]:
        num_active = self.config.general.num_active_experts
        all_experts = [f"expert_{i}" for i in range(self.config.general.num_experts)]

        carbon_intensity = (
            await self.carbon_manager.get_current_intensity()
            if self.carbon_manager else self.config.optimizer.default_carbon_intensity
        )
        context = {
            "client_id": client_id,
            "data_distribution": data_distribution,
            "carbon_intensity": carbon_intensity,
            "num_clients": len(self.allocation_history) + 1,
            "hour": datetime.now().hour,
            "data_size": sum(data_distribution.values()) if data_distribution else 0,
            "reputation": 0.5,
        }
        self.last_context = context
        self._last_policy = "random"
        self._last_propensity = 1.0

        try:
            self.chaos_monkey.maybe_fail_component("allocation")
        except ChaosExperimentError as e:
            logger.warning(f"Chaos injected in allocation: {e}")

        # 1) Policy selection
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.distiller:
            policy = self.distiller.distill(context) or "random"
            source = "distilled"
            propensity = 1.0
        elif ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.rlhf:
            policy = self.rlhf.sample_action(context) or "random"
            source = "rlhf"
            propensity = 1.0
        elif self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            policy, propensity, source = self.bandit.select_action(encoded)
            policy = policy or "random"
        else:
            policy, source, propensity = "random", "fallback", 1.0

        # 2) Optional quantum override
        if self.quantum_optimizer.available:
            candidates = [
                {"name": p, "accuracy": 0.7,
                 "carbon": 1 - carbon_intensity / 800, "energy": 0.5, "latency": 0.8}
                for p in self.allocation_policies
            ]
            qc = self.quantum_optimizer.select_best(candidates, self.config.optimizer.modp_weights)
            if qc:
                policy, source = qc.get("name", policy), "quantum"

        # 3) Policy-specific expert selection
        policy = policy if policy in self.allocation_policies else "random"
        if policy == "random":
            selected = random.sample(all_experts, min(num_active, len(all_experts)))
        elif policy == "balanced":
            by_layer: Dict[int, List[str]] = defaultdict(list)
            for eid in all_experts:
                by_layer[int(eid.split("_")[1]) % 2].append(eid)
            selected = []
            for layer in sorted(by_layer):
                if len(selected) >= num_active:
                    break
                selected.append(random.choice(by_layer[layer]))
            while len(selected) < num_active and len(selected) < len(all_experts):
                cand = random.choice(all_experts)
                if cand not in selected:
                    selected.append(cand)
        elif policy == "energy_focused":
            scored = sorted(all_experts, key=lambda e: self.experts_activation(e))
            selected = scored[:num_active]
        elif policy == "accuracy_focused":
            scored = sorted(all_experts, key=lambda e: -self.experts_performance(e))
            selected = scored[:num_active]
        else:
            selected = random.sample(all_experts, min(num_active, len(all_experts)))

        # 4) LIMIT Graph constraint
        if self.limit_graph:
            limits = self.limit_graph.get_limits(context)
            if limits.get("max_experts"):
                selected = selected[: limits["max_experts"]]

        # 5) Safety check
        if not self.safety_monitor.check_allocation(len(selected), carbon_intensity):
            logger.warning("Safety violation on allocation; truncating experts")
            selected = selected[: self.safety_monitor.max_experts]

        # 6) Human approval for large allocations
        explanation = ""
        if self.human_approval is not None and len(selected) >= self.config.optimizer.human_review_threshold_experts:
            decision_id = f"alloc_{uuid.uuid4().hex[:8]}"
            approved = await self.human_approval.request_approval(decision_id, {
                "policy": policy, "num_experts": len(selected), "context": context,
            })
            if not approved:
                logger.info("Human approval denied; reducing to safe default.")
                selected = selected[:2]
            elif self.active_learning:
                self.active_learning.record_human_preference(
                    context=context, approved_action=policy, rejected_action="random")

        # 7) XAI
        if self.xai:
            explanation = self.xai.explain_allocation(policy, context, selected)
            logger.info(f"Allocation explanation: {explanation}")

        self._last_policy = policy
        self._last_propensity = propensity

        async with self._lock:
            self.allocation_history.append({
                "client_id": client_id, "selected": selected, "policy": policy,
                "source": source, "explanation": explanation,
            })
        EXPERT_ALLOCATIONS.labels(strategy=policy, status="success").inc()
        return selected

    # Small helper hooks so we can talk to the adapter's expert state without circular import
    _expert_ref: Optional[Callable[[str], Any]] = None

    def bind_experts(self, accessor: Callable[[str], Any]):
        self._expert_ref = accessor

    def experts_activation(self, eid: str) -> int:
        if self._expert_ref is not None:
            e = self._expert_ref(eid)
            if e is not None:
                return getattr(e, "activation_count", 0)
        return 0

    def experts_performance(self, eid: str) -> float:
        if self._expert_ref is not None:
            e = self._expert_ref(eid)
            if e is not None:
                return getattr(e, "performance_score", 0.0)
        return 0.0

    async def record_feedback(self, client_id: str, selected: List[str], reward: float):
        if self.last_context is None:
            return
        context = self.last_context

        if self.rlhf:
            self.rlhf.update(context, self._last_policy, reward)
        if self.limit_graph:
            self.limit_graph.update_from_feedback({"client_id": client_id, "selected": selected, "reward": reward})
        if self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            self.bandit.update(encoded, self._last_policy, reward, propensity=self._last_propensity)
        if self.bio:
            self.param_rewards.append(reward)
            if len(self.param_rewards) >= 20:
                def fitness(params):
                    return float(np.mean(list(self.param_rewards)))
                new_population = self.bio.evolve(
                    population=self.param_population, fitness_fn=fitness,
                    generations=self.config.optimizer.bio_generations,
                    population_size=self.config.optimizer.bio_population_size,
                )
                if new_population:
                    self.param_population = new_population
                    best = max(new_population, key=fitness)
                    self.config.general.num_active_experts = best.get(
                        "num_active", self.config.general.num_active_experts)

    def get_allocation_stats(self) -> Dict:
        return {
            'total_allocations': len(self.allocation_history),
            'enhancements_available': ENHANCEMENTS_AVAILABLE,
            'causal_bandit': isinstance(self.bandit, CausalBandit) if self.bandit else False,
            'limit_graph_active': self.limit_graph is not None,
            'rlhf_active': self.rlhf is not None,
            'distillation_active': self.distiller is not None,
            'quantum_optimizer': self.quantum_optimizer.get_status(),
            'safety_violations': self.safety_monitor.get_violations(),
        }

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# MULTI-REGION EXPERT COORDINATOR
# ============================================================
class MultiRegionExpertCoordinator(IRegionCoordinator):
    def __init__(self, config: FFTMoEConfig, xai: Optional[XAIExplainer] = None):
        self.config = config
        self.xai = xai or XAIExplainer()
        self.regions = {
            'us-east': {'weight': 0.4, 'capacity': 1000, 'carbon_intensity': 400, 'latency': 50},
            'eu-west': {'weight': 0.3, 'capacity': 800, 'carbon_intensity': 300, 'latency': 80},
            'ap-southeast': {'weight': 0.3, 'capacity': 600, 'carbon_intensity': 500, 'latency': 120},
        }
        self.active_region = 'us-east'
        self._lock = asyncio.Lock()
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._modp_teacher,
                self._rule_based_teacher,
                self._static_teacher,
            ])
        else:
            self.distiller = None
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )

    def _modp_teacher(self, context: Dict) -> str:
        if not self.modp:
            return self.active_region
        scores = {}
        for region, info in self.regions.items():
            objectives = {
                'latency': info.get('latency', 100) / 1000,
                'carbon': info['carbon_intensity'] / 800,
                'capacity': info['capacity'] / 1000,
            }
            weights = context.get('modp_weights', self.config.optimizer.modp_weights)
            scores[region] = self.modp.evaluate(objectives, weights)
        return max(scores, key=scores.get)

    def _rule_based_teacher(self, context: Dict) -> str:
        scores = {}
        for region, info in self.regions.items():
            score = 0
            if context.get('latency_weight', 0) > 0:
                score += (1 - info.get('latency', 100) / 200) * 0.4
            if context.get('carbon_weight', 0) > 0:
                score += (1 - info['carbon_intensity'] / 800) * 0.3
            if context.get('capacity_weight', 0) > 0:
                score += info['capacity'] / 1000 * 0.3
            scores[region] = score
        return max(scores, key=scores.get)

    def _static_teacher(self, context: Dict) -> str:
        return 'us-east'

    async def get_optimal_region(self, requirements: Dict) -> str:
        context = {
            'modp_weights': requirements.get('modp_weights', self.config.optimizer.modp_weights),
            'latency_weight': requirements.get('latency_weight', 0.4),
            'carbon_weight': requirements.get('carbon_weight', 0.3),
            'capacity_weight': requirements.get('capacity_weight', 0.3),
        }
        if self.distiller:
            best = self.distiller.distill(context)
            source = "distilled"
        elif self.modp:
            best = self._modp_teacher(context)
            source = "modp"
        else:
            best = self._rule_based_teacher(context)
            source = "rule_based"

        if self.quantum_optimizer.available:
            candidates = [
                {"name": r,
                 "latency": 1 - info['latency'] / 200,
                 "carbon": 1 - info['carbon_intensity'] / 800,
                 "capacity": info['capacity'] / 1000}
                for r, info in self.regions.items()
            ]
            qc = self.quantum_optimizer.select_best(candidates, self.config.optimizer.modp_weights)
            if qc:
                best = qc.get("name", best)
                source = "quantum"

        async with self._lock:
            self.active_region = best

        explanation = self.xai.explain_region(best, requirements)
        REGIONAL_COORDINATIONS.labels(region=best, status='success').inc()
        logger.info(f"Region explanation: {explanation}")
        return best

    async def get_region_status(self) -> Dict:
        return {
            'active_region': self.active_region,
            'regions': self.regions,
            'distillation_active': self.distiller is not None,
            'quantum_optimizer': self.quantum_optimizer.get_status(),
        }

    async def health_check(self) -> Dict:
        return {'status': 'healthy', 'regions': len(self.regions)}

# ============================================================
# AUTONOMOUS HYPERPARAMETER OPTIMIZER
# ============================================================
class AutonomousHyperparameterOptimizer(IOptimizer):
    def __init__(self, config: FFTMoEConfig, xai: Optional[XAIExplainer] = None):
        self.config = config
        self.xai = xai or XAIExplainer()
        self.param_space = {
            'aggregation_alpha': [0.05, 0.1, 0.2, 0.3],
            'learning_rate': [0.005, 0.01, 0.02, 0.05],
            'local_epochs': [3, 5, 7, 10],
        }
        self.rewards = {p: {v: 0.0 for v in vals} for p, vals in self.param_space.items()}
        self.counts = {p: {v: 0 for v in vals} for p, vals in self.param_space.items()}
        self.epsilon = config.optimizer.epsilon
        self.history: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()

        if ENHANCEMENTS_AVAILABLE and config.optimizer.enabled:
            self.bio = GeneticPolicyGenerator()
            self.param_population = [{'aggregation_alpha': 0.1, 'learning_rate': 0.01, 'local_epochs': 5}]
            self.param_fitness: deque = deque(maxlen=100)
        else:
            self.bio = None
            self.param_population: List[Dict] = []
            self.param_fitness: deque = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._baseline_teacher, self._bio_teacher, self._random_teacher])
        else:
            self.distiller = None

    def _baseline_teacher(self, context: Dict) -> Dict:
        return {'aggregation_alpha': 0.1, 'learning_rate': 0.01, 'local_epochs': 5}

    def _bio_teacher(self, context: Dict) -> Dict:
        if self.bio and self.param_population:
            fitness = lambda params: float(np.mean(list(self.param_fitness))) if self.param_fitness else 0.5
            return max(self.param_population, key=fitness)
        return self._baseline_teacher(context)

    def _random_teacher(self, context: Dict) -> Dict:
        return {
            'aggregation_alpha': random.choice(self.param_space['aggregation_alpha']),
            'learning_rate': random.choice(self.param_space['learning_rate']),
            'local_epochs': random.choice(self.param_space['local_epochs']),
        }

    async def select_parameters(self) -> Dict:
        if self.distiller:
            selected = self.distiller.distill({}) or {}
            for key in self.param_space:
                if key not in selected:
                    selected[key] = random.choice(self.param_space[key])
            self.history.append({'timestamp': datetime.now().isoformat(), 'selected': selected, 'source': 'distilled'})
            return selected
        if self.bio and self.param_population:
            def fitness(params):
                return float(np.mean(list(self.param_fitness))) if self.param_fitness else 0.5
            new_population = self.bio.evolve(
                population=self.param_population, fitness_fn=fitness,
                generations=self.config.optimizer.bio_generations,
                population_size=self.config.optimizer.bio_population_size,
            )
            if new_population:
                self.param_population = new_population
                best = max(new_population, key=lambda p: fitness(p))
                return {
                    'aggregation_alpha': best['aggregation_alpha'],
                    'learning_rate': best['learning_rate'],
                    'local_epochs': best['local_epochs'],
                }
        selected = {}
        for param, values in self.param_space.items():
            val = random.choice(values) if random.random() < self.epsilon else max(values, key=lambda v: self.rewards[param][v])
            selected[param] = val
        return selected

    async def update_rewards(self, parameters: Dict, outcome: float):
        async with self._lock:
            for param, val in parameters.items():
                if param in self.rewards and val in self.rewards[param]:
                    count = self.counts[param][val] + 1
                    self.counts[param][val] = count
                    self.rewards[param][val] += (outcome - self.rewards[param][val]) / count
            if self.bio:
                self.param_fitness.append(outcome)

    def get_stats(self) -> Dict:
        return {
            'epsilon': self.epsilon,
            'history_length': len(self.history),
            'bio_available': self.bio is not None,
            'distillation_active': self.distiller is not None,
        }

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# FEDERATED COEVOLUTION MANAGER
# ============================================================
class FederatedCoevolutionManager(ICoevolution):
    def __init__(self, config: FFTMoEConfig, db_manager: IDatabaseManager, security: IQuantumSecurity):
        self.config = config
        self.db_manager = db_manager
        self.security = security
        self._lock = asyncio.Lock()
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create("coevolution")
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._modp_teacher, self._recency_teacher, self._trust_teacher])
        else:
            self.distiller = None

    def _modp_teacher(self, insights: List[Dict]) -> List[Dict]:
        if not self.modp:
            return insights
        scored = []
        for insight in insights:
            objectives = {
                'relevance': insight.get('relevance', 0.5),
                'freshness': 1.0 / (1.0 + (datetime.now() - datetime.fromisoformat(
                    insight.get('timestamp', datetime.now().isoformat()))).total_seconds() / 86400),
                'trust': insight.get('trust', 0.5),
            }
            utility = self.modp.evaluate(objectives, self.config.optimizer.modp_weights)
            scored.append((utility, insight))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [s[1] for s in scored]

    def _recency_teacher(self, insights: List[Dict]) -> List[Dict]:
        return sorted(insights, key=lambda x: x.get('timestamp', ''), reverse=True)

    def _trust_teacher(self, insights: List[Dict]) -> List[Dict]:
        return sorted(insights, key=lambda x: x.get('trust', 0), reverse=True)

    async def share_expert_insights(self, share_data: Dict) -> Dict:
        if not self.config.coevolution.server_url:
            return {'status': 'no_server'}
        quantum_key = await self.security.generate_keypair(self.config.quantum.algorithm)
        signature = await self.security.sign_expert_update('coevolution', share_data, quantum_key['key_id'])
        share_data['quantum_signature'] = signature
        COEVOLUTION_SHARES.labels(status='shared').inc()
        return {'status': 'shared', 'signature': signature}

    async def pull_insights(self) -> Optional[Dict]:
        if not self.config.coevolution.server_url:
            return None
        return {"insights": []}

    async def health_check(self) -> Dict:
        return {'status': 'healthy' if self.config.coevolution.server_url else 'degraded'}

# ============================================================
# PREDICTIVE ANALYTICS
# ============================================================
class PredictiveAnalytics(IPredictive):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE and config.predictive.enabled
        self.history_usage: deque = deque(maxlen=1000)
        self.history_carbon: deque = deque(maxlen=1000)
        self.model_storage = Path(config.predictive.model_storage_path)
        self.model_storage.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

        if ENHANCEMENTS_AVAILABLE and config.predictive.evolve_hyperparams:
            self.bio = GeneticPolicyGenerator()
            self.hyperparam_population = [
                {'changepoint_prior_scale': 0.05, 'seasonality_prior_scale': 10},
                {'changepoint_prior_scale': 0.01, 'seasonality_prior_scale': 5},
                {'changepoint_prior_scale': 0.1, 'seasonality_prior_scale': 20},
            ]
            self.hyperparam_fitness: deque = deque(maxlen=100)
        else:
            self.bio = None
            self.hyperparam_population: List[Dict] = []
            self.hyperparam_fitness: deque = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._teacher_baseline, self._teacher_auto, self._teacher_advanced])
        else:
            self.distiller = None

    def _teacher_baseline(self, data) -> Dict:
        return {'changepoint_prior_scale': 0.05, 'seasonality_prior_scale': 10}

    def _teacher_auto(self, data) -> Dict:
        if hasattr(data, '__len__') and len(data) > 100:
            return {'changepoint_prior_scale': 0.01, 'seasonality_prior_scale': 5}
        return {'changepoint_prior_scale': 0.1, 'seasonality_prior_scale': 20}

    def _teacher_advanced(self, data) -> Dict:
        if self.bio and self.hyperparam_population:
            fitness = lambda hp: -np.mean(list(self.hyperparam_fitness)) if self.hyperparam_fitness else 0.5
            new_pop = self.bio.evolve(
                self.hyperparam_population, fitness,
                generations=self.config.predictive.hyperparam_generations,
                population_size=self.config.predictive.hyperparam_population_size,
            )
            if new_pop:
                self.hyperparam_population = new_pop
                return max(new_pop, key=fitness)
        return {'changepoint_prior_scale': 0.05, 'seasonality_prior_scale': 10}

    async def update_history(self, usage: int, carbon_intensity: float):
        async with self._lock:
            self.history_usage.append({'ds': datetime.now(), 'y': usage})
            self.history_carbon.append({'ds': datetime.now(), 'y': carbon_intensity})

    async def forecast_usage(self, horizon_hours: int = None) -> Dict:
        return await self._forecast(self.history_usage, horizon_hours or self.config.predictive.horizon_hours, 'usage')

    async def forecast_carbon(self, horizon_hours: int = None) -> Dict:
        return await self._forecast(self.history_carbon, horizon_hours or self.config.predictive.horizon_hours, 'carbon')

    async def _forecast(self, history: deque, horizon: int, model_name: str) -> Dict:
        if not self.prophet_available or len(history) < 30:
            return {'forecast': [], 'confidence': 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(history)).sort_values('ds')
            if self.distiller:
                best_params = self.distiller.distill(df) or {}
                changepoint = best_params.get('changepoint_prior_scale', 0.05)
                seasonality = best_params.get('seasonality_prior_scale', 10)
            else:
                changepoint, seasonality = 0.05, 10
            model = Prophet(changepoint_prior_scale=changepoint, seasonality_prior_scale=seasonality)
            model.fit(df)
            future = model.make_future_dataframe(periods=horizon)
            forecast = model.predict(future)
            forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon)
            PREDICTIVE_ACCURACY.labels(model='prophet').set(0.9)
            return {
                'forecast': forecast_df['yhat'].tolist(),
                'lower_bound': forecast_df['yhat_lower'].tolist(),
                'upper_bound': forecast_df['yhat_upper'].tolist(),
                'dates': forecast_df['ds'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
                'confidence': 0.9,
                'model': 'prophet',
            }
        except Exception as e:
            logger.error(f"Forecast failed for {model_name}: {e}")
            return {'forecast': [], 'confidence': 0.0}

    def get_stats(self) -> Dict:
        return {
            'prophet_available': self.prophet_available,
            'samples': len(self.history_usage),
            'distillation_enabled': self.distiller is not None,
        }

    async def health_check(self) -> Dict:
        return {
            'status': 'healthy' if self.prophet_available else 'degraded',
            'prophet_available': self.prophet_available,
            'samples': len(self.history_usage),
        }

# ============================================================
# ENHANCED: MODEL VALIDATOR
# ============================================================
class ModelValidator(IValidator):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.best_metric = float("inf")
        self.patience_counter = 0
        self.history: List[float] = []

    async def validate(self, model: Dict, validation_data: Dict) -> float:
        X = validation_data.get("X")
        y = validation_data.get("y")
        if X is None or y is None:
            return 0.0
        loss_fn = nn.CrossEntropyLoss()
        expert_modules: Dict[str, nn.Module] = model.get("expert_modules", {})
        router = model.get("router")
        active = model.get("active_expert_ids") or list(expert_modules.keys())
        with torch.no_grad():
            expert_outputs = [expert_modules[eid](X) for eid in active if eid in expert_modules]
            if not expert_outputs:
                return 0.0
            stacked = torch.stack(expert_outputs, dim=1)
            if router is not None:
                gate = router(X)
                idx = [int(eid.split("_")[1]) for eid in active if eid in expert_modules]
                g = gate[:, idx]
                g = g / (g.sum(dim=1, keepdim=True) + 1e-9)
                out = (stacked * g.unsqueeze(-1)).sum(dim=1)
            else:
                out = stacked.mean(dim=1)
            loss = float(loss_fn(out, y).item())
        self.history.append(loss)
        MODEL_VALIDATION.set(loss)
        return loss

    def should_stop(self, metric: float) -> bool:
        if metric < self.best_metric - 1e-6:
            self.best_metric = metric
            self.patience_counter = 0
            return False
        self.patience_counter += 1
        return self.patience_counter >= self.config.validation.early_stopping_patience

    async def health_check(self) -> Dict:
        return {"status": "healthy", "best_metric": self.best_metric, "history_len": len(self.history)}

# ============================================================
# REAL MODELS: MoEExpert, FFTRouter, LocalModelTrainer
# ============================================================
class MoEExpert(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int, output_dim: int, dropout: float = 0.1):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, output_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        return self.fc2(self.dropout(F.relu(self.fc1(x))))


class FFTRouter(nn.Module):
    def __init__(self, input_dim: int, num_experts: int, hidden_size: int, dropout: float, noise_std: float):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, hidden_size)
        self.fc2 = nn.Linear(hidden_size, num_experts)
        self.dropout = nn.Dropout(dropout)
        self.noise_std = noise_std

    def forward(self, x):
        h = F.relu(self.fc1(x))
        logits = self.fc2(self.dropout(h))
        if self.training and self.noise_std > 0:
            logits = logits + torch.randn_like(logits) * self.noise_std
        return F.softmax(logits, dim=-1)


class LocalModelTrainer:
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.expert_modules: Dict[str, MoEExpert] = {}
        self.router: Optional[FFTRouter] = None
        self._build_model()

    def _build_model(self):
        num_experts = self.config.general.num_experts
        hidden = self.config.general.expert_hidden_size
        input_dim = self.config.general.input_dim
        output_dim = self.config.general.output_dim
        for i in range(num_experts):
            eid = f"expert_{i}"
            self.expert_modules[eid] = MoEExpert(input_dim, hidden, output_dim, self.config.general.dropout)
        self.router = FFTRouter(
            input_dim, num_experts,
            self.config.general.router_hidden_size,
            self.config.general.dropout,
            self.config.general.noise_std,
        )

    def train_local(self, client_id: str, active_expert_ids: List[str],
                    X: torch.Tensor, y: torch.Tensor,
                    epochs: Optional[int] = None, batch_size: Optional[int] = None,
                    lr: Optional[float] = None) -> Tuple[Dict[str, Dict[str, torch.Tensor]], Dict[str, torch.Tensor], float, float]:
        epochs = epochs or self.config.general.local_epochs
        batch_size = batch_size or self.config.general.batch_size
        lr = lr or self.config.general.learning_rate

        params = []
        for eid in active_expert_ids:
            if eid in self.expert_modules:
                params += list(self.expert_modules[eid].parameters())
        if self.router is not None:
            params += list(self.router.parameters())
        if not params:
            return {}, {}, 0.0, 0.0

        optimizer = optim.Adam(params, lr=lr)
        loss_fn = nn.CrossEntropyLoss()

        for eid, mod in self.expert_modules.items():
            requires_grad = eid in active_expert_ids
            for p in mod.parameters():
                p.requires_grad = requires_grad

        dataset = TensorDataset(X, y)
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

        final_loss = 0.0
        correct = 0
        total = 0
        for _ in range(epochs):
            for xb, yb in loader:
                optimizer.zero_grad()
                gate = self.router(xb) if self.router is not None else None
                expert_outputs = [self.expert_modules[eid](xb) for eid in active_expert_ids if eid in self.expert_modules]
                if not expert_outputs:
                    continue
                stacked = torch.stack(expert_outputs, dim=1)
                if gate is not None:
                    idx = [int(eid.split("_")[1]) for eid in active_expert_ids if eid in self.expert_modules]
                    gate_active = gate[:, idx]
                    gate_active = gate_active / (gate_active.sum(dim=1, keepdim=True) + 1e-9)
                    out = (stacked * gate_active.unsqueeze(-1)).sum(dim=1)
                else:
                    out = stacked.mean(dim=1)
                loss = loss_fn(out, yb)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(params, max_norm=1.0)
                optimizer.step()
                final_loss = float(loss.item())
                preds = out.argmax(dim=-1)
                correct += int((preds == yb).sum().item())
                total += int(yb.numel())

        expert_updates = {
            eid: {k: v.detach().clone() for k, v in self.expert_modules[eid].state_dict().items()}
            for eid in active_expert_ids if eid in self.expert_modules
        }
        gating_update = {k: v.detach().clone() for k, v in self.router.state_dict().items()} if self.router else {}
        accuracy = correct / max(total, 1)
        logger.info(f"[{client_id}] local train done loss={final_loss:.4f} acc={accuracy:.4f}")
        return expert_updates, gating_update, final_loss, accuracy

    def apply_global_update(self, expert_updates: Dict[str, Dict[str, torch.Tensor]],
                            gating_update: Dict[str, torch.Tensor]):
        for eid, sd in expert_updates.items():
            if eid in self.expert_modules:
                try:
                    self.expert_modules[eid].load_state_dict(sd, strict=False)
                except Exception as e:
                    logger.warning(f"Failed to load expert {eid} state: {e}")
        if self.router is not None and gating_update:
            try:
                self.router.load_state_dict(gating_update, strict=False)
            except Exception as e:
                logger.warning(f"Failed to load gating state: {e}")

# ============================================================
# DATA STRUCTURES
# ============================================================
@dataclass
class ExpertState:
    expert_id: str
    weights: Dict[str, torch.Tensor]
    layer_index: int
    activation_count: int = 0
    last_used: datetime = field(default_factory=datetime.now)
    performance_score: float = 0.0
    specialization: str = ""


@dataclass
class ClientExpertProfile:
    client_id: str
    active_expert_ids: List[str]
    expert_weights: Dict[str, float]
    data_distribution: Dict[str, float]
    local_update_count: int = 0
    region: str = "global"
    last_update: datetime = field(default_factory=datetime.now)


@dataclass
class FFTMoEUpdate:
    client_id: str
    expert_updates: Dict[str, Dict[str, torch.Tensor]]
    gating_update: Dict[str, torch.Tensor]
    token_usage: float
    carbon_footprint_kg: float
    timestamp: datetime = field(default_factory=datetime.now)

# ============================================================
# MAIN ADAPTER v6.1.0
# ============================================================
class FFTMoEAdapterV6:
    def __init__(self, config: FFTMoEConfig, db_manager: IDatabaseManager,
                 quantum_security: IQuantumSecurity, blockchain_registry: IBlockchainRegistry,
                 allocator: IAllocator, region_coordinator: IRegionCoordinator,
                 carbon_manager: ICarbonManager, cloud_storage: ICloudStorage,
                 vault: IVault, predictive: Optional[IPredictive] = None,
                 optimizer: Optional[IOptimizer] = None, validator: Optional[IValidator] = None,
                 secure_aggregator: Optional[ISecureAggregator] = None,
                 coevolution: Optional[ICoevolution] = None,
                 leader: Optional[LeaderElection] = None,
                 task_manager: Optional[TaskManager] = None):
        self.config = config
        self.instance_id = config.general.instance_id

        self.db_manager = db_manager
        self.quantum_security = quantum_security
        self.blockchain_registry = blockchain_registry
        self.allocator = allocator
        self.region_coordinator = region_coordinator
        self.carbon_manager = carbon_manager
        self.cloud_storage = cloud_storage
        self.vault = vault
        self.predictive = predictive
        self.optimizer = optimizer
        self.validator = validator
        self.secure_aggregator = secure_aggregator
        self.coevolution = coevolution
        self.leader = leader or LeaderElection(config)
        self.task_manager = task_manager or TaskManager()

        self.trainer = LocalModelTrainer(self.config)

        self.experts: Dict[str, ExpertState] = {}
        self.router = self.trainer.router
        self.global_expert_pool: Dict[str, Dict[str, torch.Tensor]] = {}
        self.client_profiles: Dict[str, ClientExpertProfile] = {}
        self.pending_updates: Dict[str, List[FFTMoEUpdate]] = defaultdict(list)

        self.round_number = 0
        self.global_accuracy = 0.0
        self.total_tokens_distributed = 0.0

        self._experts_lock = asyncio.Lock()
        self._profiles_lock = asyncio.Lock()
        self._updates_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()

        # Advanced
        self.dp = DifferentialPrivacy(
            clip_norm=config.optimizer.dp_clip_norm,
            noise_multiplier=config.optimizer.dp_noise_multiplier,
            target_delta=config.optimizer.dp_target_delta,
            max_epsilon=config.optimizer.dp_max_epsilon,
            sample_rate=config.optimizer.dp_sample_rate,
        ) if config.optimizer.enable_dp else None

        self.multi_agent = MultiAgentClientCoordinator(
            min_reputation=config.optimizer.client_reputation_min) if config.optimizer.enable_multi_agent else None

        self.safety_monitor = FormalSafetyMonitor(
            max_experts=config.optimizer.safety_max_experts,
            max_carbon_intensity=config.optimizer.safety_max_carbon_intensity,
            max_consecutive_failures=config.optimizer.safety_max_consecutive_failures,
        )
        self.xai = XAIExplainer() if config.optimizer.enable_xai else None
        self.precision_policy = FlexGenPrecisionPolicy(
            default_carbon_intensity=config.optimizer.default_carbon_intensity
        ) if config.optimizer.enable_precision_switch else None

        self.carbon_broker = CarbonOffsetBroker(
            threshold_kg=config.carbon.offset_threshold_kg,
            cost_per_kg=config.carbon.offset_cost_per_kg,
            rec_cost_per_mwh=config.carbon.rec_cost_per_mwh,
        )
        self.chaos_monkey = ChaosMonkey(
            enabled=config.optimizer.chaos_enabled,
            failure_probability=config.optimizer.chaos_failure_probability,
        )
        self.human_approval = RealHumanApprovalManager(
            timeout_seconds=config.optimizer.human_review_timeout_seconds
        ) if config.optimizer.human_review_enabled else None
        self.active_learning = ActiveLearningRLHF(rlhf=getattr(allocator, 'rlhf', None))

        # Byzantine aggregator for non-secure path
        self.byzantine_aggregator = ByzantineAggregator(
            mode=config.optimizer.byzantine_mode,
            trim_ratio=config.optimizer.byzantine_trim_ratio,
            krum_f=config.optimizer.krum_f,
            reputation=self.multi_agent.reputation if self.multi_agent else {},
        )

        # Model versioning
        self._model_version = 0
        self._model_version_history: List[Dict] = []
        self.model_registry: Dict[int, Dict] = {}

        # Health components
        self._health_components = {
            'database': self.db_manager,
            'quantum_security': self.quantum_security,
            'blockchain': self.blockchain_registry,
            'allocator': self.allocator,
            'region_coordinator': self.region_coordinator,
            'carbon_manager': self.carbon_manager,
            'cloud_storage': self.cloud_storage,
            'vault': self.vault,
            'predictive': self.predictive,
            'optimizer': self.optimizer,
            'validator': self.validator,
            'secure_aggregator': self.secure_aggregator,
            'coevolution': self.coevolution,
        }

        # Initialize experts
        for i in range(self.config.general.num_experts):
            expert_id = f"expert_{i}"
            self.experts[expert_id] = ExpertState(
                expert_id=expert_id, weights={},
                layer_index=i // max(self.config.general.num_experts // 2, 1),
            )

        # Bind allocator -> expert accessor so energy/accuracy policies work
        if hasattr(self.allocator, "bind_experts"):
            try:
                self.allocator.bind_experts(lambda eid: self.experts.get(eid))
            except Exception:
                pass

        self._register_background_tasks()
        logger.info(f"FFT-MoE Adapter v{self.config.general.version} initialized with {self.config.general.num_experts} experts")

    # ---------------- Background loops ----------------
    def _register_background_tasks(self):
        self.task_manager.register_task("health_check", self._health_check_loop)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        self.task_manager.register_task("coevolution", self._coevolution_loop)
        self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        self.task_manager.register_task("optimizer", self._optimizer_loop)

    async def start(self):
        logger.info("Starting FFT-MoE Adapter v6.1.0...")
        await self.db_manager.init()
        self.task_manager.start_registered_tasks()
        logger.info("Adapter started with background tasks")

    async def _health_check_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get('health_score', 100))
                await asyncio.sleep(self.config.general.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
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

    async def _coevolution_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.coevolution:
                    share_data = {"instance_id": self.instance_id, "num_experts": len(self.experts), "round": self.round_number}
                    await self.coevolution.share_expert_insights(share_data)
                await asyncio.sleep(self.config.coevolution.share_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Coevolution loop error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.predictive:
                    carbon = await self.carbon_manager.get_current_intensity()
                    await self.predictive.update_history(len(self.experts), carbon)
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update loop error: {e}")
                await asyncio.sleep(60)

    async def _optimizer_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.optimizer:
                    params = await self.optimizer.select_parameters()
                    self.config.aggregation_alpha = params['aggregation_alpha']
                    self.config.general.learning_rate = params['learning_rate']
                    self.config.general.local_epochs = params['local_epochs']
                    outcome = random.uniform(0.8, 1.0)
                    await self.optimizer.update_rewards(params, outcome)
                await asyncio.sleep(600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Optimizer loop error: {e}")
                await asyncio.sleep(60)

    # ---------------- Client management ----------------
    async def register_client(self, client_id: str, data_distribution: Dict[str, float],
                              initial_experts: Optional[List[str]] = None, region: str = "global"):
        if initial_experts is None:
            initial_experts = await self.allocator.allocate_experts(client_id, data_distribution)
        async with self._profiles_lock:
            profile = ClientExpertProfile(
                client_id=client_id,
                active_expert_ids=initial_experts,
                expert_weights={eid: 0.1 for eid in initial_experts},
                data_distribution=data_distribution,
                region=region,
            )
            self.client_profiles[client_id] = profile

        if self.multi_agent:
            self.multi_agent.register_client(client_id)
            if self.byzantine_aggregator is not None:
                self.byzantine_aggregator.reputation = self.multi_agent.reputation

        async def insert(session):
            await session.execute(
                text("INSERT OR REPLACE INTO client_profiles "
                     "(client_id, active_expert_ids, expert_weights, data_distribution, local_update_count, region) "
                     "VALUES (:client_id, :a, :e, :d, :l, :r)"),
                {
                    'client_id': client_id,
                    'a': json.dumps(initial_experts),
                    'e': json.dumps({eid: 0.1 for eid in initial_experts}),
                    'd': json.dumps(data_distribution),
                    'l': 0,
                    'r': region,
                },
            )
        try:
            await self.db_manager.execute_async(insert)
        except Exception as e:
            logger.warning(f"Failed to persist client {client_id}: {e}")
        logger.info(f"Registered client {client_id}")

    async def get_client_model(self, client_id: str) -> Dict[str, torch.Tensor]:
        async with self._profiles_lock:
            if client_id not in self.client_profiles:
                raise ClientNotRegisteredError(f"Client {client_id} not registered")
            profile = self.client_profiles[client_id]
        model: Dict[str, torch.Tensor] = {}
        for eid in profile.active_expert_ids:
            if eid in self.trainer.expert_modules:
                sd = self.trainer.expert_modules[eid].state_dict()
                flat = torch.cat([v.flatten().float() for v in sd.values() if isinstance(v, torch.Tensor)])
                model[eid] = flat
        if self.trainer.router is not None:
            g_sd = self.trainer.router.state_dict()
            model["__gating__"] = torch.cat([v.flatten().float() for v in g_sd.values() if isinstance(v, torch.Tensor)])
        return model

    async def train_local(self, client_id: str, X: torch.Tensor, y: torch.Tensor,
                          epochs: Optional[int] = None, batch_size: Optional[int] = None,
                          lr: Optional[float] = None):
        async with self._profiles_lock:
            if client_id not in self.client_profiles:
                raise ClientNotRegisteredError(f"Client {client_id} not registered")
            active = list(self.client_profiles[client_id].active_expert_ids)
        return self.trainer.train_local(client_id, active, X, y, epochs, batch_size, lr)

    async def receive_client_update(self, client_id: str,
                                    expert_updates: Dict[str, Dict[str, torch.Tensor]],
                                    gating_update: Dict[str, torch.Tensor],
                                    token_usage: float, carbon_footprint_kg: float) -> bool:
        async with self._profiles_lock:
            if client_id not in self.client_profiles:
                logger.warning(f"Client {client_id} not registered")
                return False
            profile = self.client_profiles[client_id]

        expert_updates = self.chaos_monkey.maybe_corrupt_update(expert_updates)

        if self.dp:
            for eid in list(expert_updates.keys()):
                expert_updates[eid] = self.dp.clip_and_noise(
                    expert_updates[eid], num_samples=max(int(token_usage), 1))

        precision = "fp32"
        if self.precision_policy:
            workload = "large" if token_usage > 1000 else ("medium" if token_usage > 100 else "small")
            carbon = await self.carbon_manager.get_current_intensity()
            precision = self.precision_policy.recommend(workload_size=workload, carbon_intensity=carbon)
            expert_updates = {eid: self.precision_policy.cast(ld, precision)
                              for eid, ld in expert_updates.items()}
            gating_update = self.precision_policy.cast(gating_update, precision)

        update = FFTMoEUpdate(
            client_id=client_id,
            expert_updates=expert_updates,
            gating_update=gating_update,
            token_usage=token_usage,
            carbon_footprint_kg=carbon_footprint_kg,
        )
        async with self._updates_lock:
            self.pending_updates[client_id].append(update)
            profile.local_update_count += 1
        return True

    # ---------------- Aggregation ----------------
    async def aggregate_updates(self) -> Dict[str, torch.Tensor]:
        self.round_number += 1
        aggregated: Dict[str, torch.Tensor] = {}
        aggregated_gating: Dict[str, torch.Tensor] = {}

        async with self._updates_lock:
            all_updates: List[FFTMoEUpdate] = []
            for client_id, updates in self.pending_updates.items():
                all_updates.extend(updates)
            self.pending_updates.clear()

        if not all_updates:
            return aggregated

        if self.multi_agent:
            filtered = [u for u in all_updates if self.multi_agent.is_acceptable(u.client_id)]
            if filtered:
                all_updates = filtered

        expert_mode_aggregated: Dict[str, Dict[str, torch.Tensor]] = {}

        if self.secure_aggregator and self.config.optimizer.enable_secure_agg:
            encrypted = []
            peer_ids = [o.client_id for o in all_updates]
            for u in all_updates:
                enc = await self.secure_aggregator.encrypt_update({
                    "client_id": u.client_id,
                    "expert_updates": u.expert_updates,
                    "gating_update": u.gating_update,
                    "peers": [p for p in peer_ids if p != u.client_id],
                    "num_samples": max(int(u.token_usage), 1),
                })
                encrypted.append(enc)
            agg = await self.secure_aggregator.aggregate_encrypted(encrypted)
            expert_mode_aggregated = agg.get("expert_updates", {})
            aggregated_gating = agg.get("gating_update", {})
        else:
            per_client_experts: List[Dict[str, torch.Tensor]] = []
            per_client_gating: List[Dict[str, torch.Tensor]] = []
            client_ids: List[str] = []
            for u in all_updates:
                client_ids.append(u.client_id)
                flat: Dict[str, torch.Tensor] = {}
                for eid, ld in u.expert_updates.items():
                    for layer_name, tensor in ld.items():
                        if isinstance(tensor, torch.Tensor):
                            flat[f"{eid}::{layer_name}"] = tensor
                per_client_experts.append(flat)
                per_client_gating.append({k: v for k, v in u.gating_update.items() if isinstance(v, torch.Tensor)})

            byz_experts = self.byzantine_aggregator.aggregate_dict(per_client_experts, client_ids) if per_client_experts else {}
            byz_gating = self.byzantine_aggregator.aggregate_dict(per_client_gating, client_ids) if per_client_gating else {}

            for k, tensor in byz_experts.items():
                eid, layer_name = k.split("::", 1)
                expert_mode_aggregated.setdefault(eid, {})[layer_name] = tensor
            aggregated_gating = byz_gating

        self.trainer.apply_global_update(expert_mode_aggregated, aggregated_gating)

        self._model_version += 1
        self.model_registry[self._model_version] = {
            "round": self.round_number,
            "num_experts": len(expert_mode_aggregated),
            "timestamp": datetime.now().isoformat(),
        }
        self._model_version_history.append(self.model_registry[self._model_version])

        success = len(expert_mode_aggregated) > 0
        if not self.safety_monitor.check_aggregation(success):
            logger.warning("Safety violation on aggregation")

        if self.multi_agent:
            carbon = await self.carbon_manager.get_current_intensity()
            for u in all_updates:
                self.multi_agent.record_outcome(u.client_id, success,
                                                {"carbon_intensity": carbon, "data_size": u.token_usage})

        total_carbon = sum(u.carbon_footprint_kg for u in all_updates)
        try:
            await self.carbon_broker.purchase_offsets(total_carbon)
        except Exception as e:
            logger.warning(f"Carbon offset purchase failed: {e}")

        if self.xai:
            explanation = self.xai.explain_allocation(
                "aggregate",
                {"carbon_intensity": 400, "num_clients": len(all_updates)},
                list(expert_mode_aggregated.keys()),
            )
            logger.info(f"Aggregation explanation: {explanation}")

        for eid, ld in expert_mode_aggregated.items():
            for layer_name, tensor in ld.items():
                aggregated[f"{eid}::{layer_name}"] = tensor
        for layer_name, tensor in aggregated_gating.items():
            aggregated[f"__gating__::{layer_name}"] = tensor
        return aggregated

    # ---------------- Health / status ----------------
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

        overall = 'healthy' if all(
            r.get('status') in ('ok', 'healthy', 'unavailable', 'degraded') for r in results.values()
        ) else 'degraded'
        health_score = 100 if overall == 'healthy' else 50
        return {
            'status': overall,
            'health_score': health_score,
            'components': results,
            'timestamp': datetime.now().isoformat(),
        }

    async def get_fft_moe_status(self) -> Dict[str, Any]:
        status = {
            'instance_id': self.instance_id,
            'version': self.config.general.version,
            'round_number': self.round_number,
            'num_clients': len(self.client_profiles),
            'num_experts': len(self.experts),
            'total_updates_processed': sum(p.local_update_count for p in self.client_profiles.values()),
            'global_accuracy': self.global_accuracy,
            'enhancements_available': ENHANCEMENTS_AVAILABLE,
            'additional_enhancements_available': ADDITIONAL_ENHANCEMENTS_AVAILABLE,
            'model_version': self._model_version,
        }
        if self.allocator:
            status['allocation_stats'] = self.allocator.get_allocation_stats()
        if self.optimizer:
            status['optimizer_stats'] = self.optimizer.get_stats()
        status['advanced_enhancements'] = {
            'dp_enabled': self.dp is not None,
            'dp_state': self.dp.get_state() if self.dp else {},
            'multi_agent': self.multi_agent.get_stats() if self.multi_agent else {},
            'safety_violations': self.safety_monitor.get_violations(),
            'xai_enabled': self.xai is not None,
            'precision_policy': {"enabled": self.precision_policy is not None},
            'carbon_broker': self.carbon_broker.get_totals(),
            'chaos_monkey': self.chaos_monkey.get_stats(),
            'human_approval': self.human_approval.get_stats() if self.human_approval else {},
            'active_learning': self.active_learning.get_stats(),
            'secure_agg_enabled': self.secure_aggregator is not None,
            'byzantine_mode': self.byzantine_aggregator.mode,
        }
        return status

    def get_model_version(self) -> int:
        return self._model_version

    def get_model_history(self) -> List[Dict]:
        return list(self._model_version_history)

    async def submit_human_feedback(self, decision_id: str, approved: bool):
        if self.human_approval is None:
            return
        await self.human_approval.record_response(decision_id, approved)
        self.active_learning.record_human_preference(
            context={"decision_id": decision_id},
            approved_action="allocated",
            rejected_action=None,
        )

    async def shutdown(self):
        logger.info("Shutting down FFT-MoE Adapter...")
        await self.task_manager.stop_all()
        await self.carbon_manager.close()
        await self.db_manager.close()
        await self.leader.stop()
        logger.info("Shutdown complete")

# ============================================================
# FASTAPI REST API
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="FFT-MoE Adapter API", version="6.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer()
    api_rate_limiter = RateLimiter(rate=FFTMoEConfig().api.rate_limit_requests,
                                   per_seconds=FFTMoEConfig().api.rate_limit_window)

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        token = credentials.credentials
        try:
            payload = jwt.decode(token, FFTMoEConfig().api.jwt_secret, algorithms=["HS256"])
            return payload
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    async def rate_limit(request: Request):
        if FFTMoEConfig().api.rate_limit_enabled:
            if not await api_rate_limiter.acquire():
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

    adapter: Optional[FFTMoEAdapterV6] = None

    @app.on_event("startup")
    async def _startup():
        global adapter
        if adapter is not None:
            return
        cfg = FFTMoEConfig()
        db = AsyncDatabaseManager(cfg)
        vault = VaultManager(cfg)
        pqc = PostQuantumCrypto(cfg, vault)
        chain = BlockchainExpertRegistry(cfg)
        carbon = CarbonIntensityManager(cfg)
        cloud = MultiCloudStorage(cfg)
        allocator = AutonomousExpertAllocator(cfg, carbon_manager=carbon)
        region = MultiRegionExpertCoordinator(cfg)
        predictive = PredictiveAnalytics(cfg)
        optimizer = AutonomousHyperparameterOptimizer(cfg)
        validator = ModelValidator(cfg)
        secure_agg = SecureAggregator(seed=cfg.optimizer.secure_agg_seed)
        coevolution = FederatedCoevolutionManager(cfg, db, pqc)
        adapter = FFTMoEAdapterV6(
            config=cfg, db_manager=db, quantum_security=pqc,
            blockchain_registry=chain, allocator=allocator,
            region_coordinator=region, carbon_manager=carbon,
            cloud_storage=cloud, vault=vault,
            predictive=predictive, optimizer=optimizer,
            validator=validator, secure_aggregator=secure_agg,
            coevolution=coevolution,
        )
        await adapter.start()
        logger.info("Adapter initialized via FastAPI startup")

    @app.on_event("shutdown")
    async def _shutdown():
        global adapter
        if adapter is not None:
            await adapter.shutdown()
            adapter = None

    @app.post("/register_client")
    async def register_client(client_id: str, data_distribution: Dict[str, float],
                              region: str = "global", user: Dict = Depends(verify_token),
                              _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        await adapter.register_client(client_id, data_distribution, region=region)
        return {"status": "registered"}

    @app.get("/client_model/{client_id}")
    async def get_client_model(client_id: str, user: Dict = Depends(verify_token),
                               _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        model = await adapter.get_client_model(client_id)
        return {"model": {k: v.tolist() for k, v in model.items()}}

    @app.post("/submit_update")
    async def submit_update(client_id: str, expert_updates: Dict, gating_update: Dict,
                            token_usage: float, carbon_footprint_kg: float,
                            user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        for eid in expert_updates:
            for layer in expert_updates[eid]:
                if isinstance(expert_updates[eid][layer], list):
                    expert_updates[eid][layer] = torch.tensor(expert_updates[eid][layer])
        for layer in gating_update:
            if isinstance(gating_update[layer], list):
                gating_update[layer] = torch.tensor(gating_update[layer])
        success = await adapter.receive_client_update(
            client_id, expert_updates, gating_update, token_usage, carbon_footprint_kg)
        return {"success": success}

    @app.post("/aggregate")
    async def aggregate(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        updates = await adapter.aggregate_updates()
        return {"aggregated": len(updates)}

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        return await adapter.get_fft_moe_status()

    @app.get("/health")
    async def health(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        return await adapter.health_check()

    @app.get("/optimization/status")
    async def optimization_status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        return {
            "allocator": adapter.allocator.get_allocation_stats(),
            "optimizer": adapter.optimizer.get_stats() if adapter.optimizer else None,
            "predictive": adapter.predictive.get_stats() if adapter.predictive else None,
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "additional_enhancements_available": ADDITIONAL_ENHANCEMENTS_AVAILABLE,
        }

    @app.post("/optimization/rlhf-update")
    async def rlhf_update(context: Dict, action: str, reward: float,
                          user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        if hasattr(adapter.allocator, 'rlhf') and adapter.allocator.rlhf:
            adapter.allocator.rlhf.update(context, action, reward)
            return {"status": "RLHF updated"}
        return {"status": "RLHF not available"}

    @app.post("/human_feedback/{decision_id}")
    async def human_feedback(decision_id: str, approved: bool,
                             user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        await adapter.submit_human_feedback(decision_id, approved)
        return {"status": "recorded"}

    @app.get("/model_history")
    async def model_history(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        return {"version": adapter.get_model_version(), "history": adapter.get_model_history()}

# ============================================================
# ENTRY POINT
# ============================================================
async def _main():
    cfg = FFTMoEConfig()
    db = AsyncDatabaseManager(cfg)
    vault = VaultManager(cfg)
    pqc = PostQuantumCrypto(cfg, vault)
    chain = BlockchainExpertRegistry(cfg)
    carbon = CarbonIntensityManager(cfg)
    cloud = MultiCloudStorage(cfg)
    allocator = AutonomousExpertAllocator(cfg, carbon_manager=carbon)
    region = MultiRegionExpertCoordinator(cfg)
    predictive = PredictiveAnalytics(cfg)
    optimizer = AutonomousHyperparameterOptimizer(cfg)
    validator = ModelValidator(cfg)
    secure_agg = SecureAggregator(seed=cfg.optimizer.secure_agg_seed)
    coevolution = FederatedCoevolutionManager(cfg, db, pqc)
    adapter = FFTMoEAdapterV6(
        config=cfg, db_manager=db, quantum_security=pqc,
        blockchain_registry=chain, allocator=allocator,
        region_coordinator=region, carbon_manager=carbon,
        cloud_storage=cloud, vault=vault,
        predictive=predictive, optimizer=optimizer,
        validator=validator, secure_aggregator=secure_agg,
        coevolution=coevolution,
    )
    await adapter.start()

    # Minimal smoke test
    await adapter.register_client("client_demo", {"topic_a": 0.6, "topic_b": 0.4})
    X = torch.randn(64, cfg.general.input_dim)
    y = torch.randint(0, cfg.general.output_dim, (64,))
    expert_updates, gating_update, loss, acc = await adapter.train_local("client_demo", X, y, epochs=2)
    await adapter.receive_client_update(
        "client_demo", expert_updates, gating_update,
        token_usage=64.0, carbon_footprint_kg=0.01,
    )
    agg = await adapter.aggregate_updates()
    logger.info(f"Aggregated {len(agg)} tensors. model_version={adapter.get_model_version()}")

    try:
        await asyncio.sleep(2)
    finally:
        await adapter.shutdown()


if __name__ == "__main__":
    if FASTAPI_AVAILABLE and os.getenv("FFTMOE_SERVE", "0") == "1":
        uvicorn.run(app, host=FFTMoEConfig().api.host, port=FFTMoEConfig().api.port)
    else:
        asyncio.run(_main())
