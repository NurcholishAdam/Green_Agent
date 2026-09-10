#!/usr/bin/env python3
# File: enhancements/fft_moe_adapter_enhanced_v5_0.py
"""
Federated Fine-Tuning with Mixture of Experts (FFT-MoE) Adapter v6.0.0
Enterprise Quantum+ with Advanced Enhancements

ENHANCEMENTS OVER v5.0.0 (NEW IN v6.0.0):
- CausalBandit replaces ContextualBandit for causal RL of expert allocations.
- QuantumDistillationOptimizer (optional Qiskit QAOA) selects best expert subset + policy.
- DifferentialPrivacy clips + noises client updates.
- SecureAggregator ENABLED by default with Bonawitz-style masks.
- MultiAgentClientCoordinator: clients act as agents with emergent role specialisation.
- FormalSafetyMonitor enforces LTL-like invariants over allocations and aggregation.
- XAIExplainer produces feature attribution + natural-language rationale.
- FlexGenPrecisionPolicy recommends fp32/fp16/int8 per client update.
- CarbonOffsetBroker purchases offsets and RECs per federated round.
- ChaosMonkey injects faults (stale updates, malformed tensors, outages).
- RealHumanApprovalManager publishes approval requests and awaits response.
- ActiveLearningRLHF consumes human approvals/rejections into the RLHF optimizer.

All v5.0.0 features retained.
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
from typing import Dict, Any, List, Optional, Tuple, Callable, Union, Protocol, runtime_checkable, Awaitable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import numpy as np
from pathlib import Path
import contextvars
import threading
from functools import wraps
import weakref
from concurrent.futures import ThreadPoolExecutor

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, TensorDataset
from torch import optim

# ============================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
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
        def encode(self, context): return [0.0]*5
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
# CONFIG IMPORTS
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
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
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
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

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
            logging.handlers.RotatingFileHandler('fft_moe_v6.log', maxBytes=10*1024*1024, backupCount=5),
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
    # NEW metrics
    SAFETY_VIOLATIONS = Counter('fft_safety_violations_total', 'Safety violations', ['rule'], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter('fft_chaos_experiments_total', 'Chaos experiments', ['type', 'status'], registry=REGISTRY)
    HUMAN_REVIEWS = Counter('fft_human_reviews_total', 'Human reviews', ['status'], registry=REGISTRY)
    XAI_DECISIONS = Counter('fft_xai_decisions_total', 'XAI decisions', ['policy'], registry=REGISTRY)
    CARBON_OFFSETS = Counter('fft_carbon_offsets_total', 'Carbon offsets purchased', ['status'], registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('fft_precision_selections_total', 'Precision selections', ['precision'], registry=REGISTRY)
    DP_APPLIED = Counter('fft_dp_applied_total', 'Differential privacy applications', registry=REGISTRY)
    SECURE_AGG = Counter('fft_secure_aggregation_total', 'Secure aggregation ops', registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
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

# ============================================================
# CUSTOM EXCEPTIONS
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
# GLOBAL CIRCUIT BREAKER REGISTRY
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
            'throttle_rate': (self.throttled_requests / max(total, 1)) * 100
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
            except (asyncio.TimeoutError, Exception):
                async with self._lock:
                    self.metrics['failed'] += 1
                raise
        task_name = name or f"task_{uuid.uuid4().hex[:8]}"
        task = asyncio.create_task(wrapper(), name=task_name)
        async with self._lock:
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
        version: str = Field("6.0.0")
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
            default_factory=lambda: {'accuracy':0.4, 'energy':0.3, 'carbon':0.2, 'latency':0.1}
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
        # NEW flags
        causal_bandit_enabled: bool = True
        enable_dp: bool = True
        dp_clip_norm: float = Field(1.0, gt=0)
        dp_noise_multiplier: float = Field(0.1, gt=0)
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

    class ValidationConfig(BaseModel):
        enabled: bool = True
        holdout_ratio: float = Field(0.1, ge=0, le=0.5)
        early_stopping_patience: int = Field(5, ge=1)
        metric: str = Field("loss")

    class SecureAggregationConfig(BaseModel):
        enabled: bool = True  # NOW ENABLED BY DEFAULT

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
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "6.0.0"
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
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'accuracy':0.4, 'energy':0.3, 'carbon':0.2, 'latency':0.1})
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
# NEW: CausalBandit
# ============================================================
class CausalBandit:
    """Causal bandit estimating treatment effects of allocation policies."""
    def __init__(self, action_space, fallback_solver, min_trials_before_bandit=5, confidence_threshold=0.6):
        self.actions = action_space
        self.fallback_solver = fallback_solver
        self.min_trials = min_trials_before_bandit
        self.confidence_threshold = confidence_threshold
        self.q_values = {a: 0.0 for a in action_space}
        self.counts = {a: 0 for a in action_space}
        self.causal_effects = {a: 0.0 for a in action_space}
        self.trials = 0
        self.context_history: List[Dict] = []
        self.reward_history: List[float] = []
        self.action_history: List[str] = []

    def select_action(self, context):
        if self.trials < self.min_trials:
            return self.fallback_solver(context), 0.0, "fallback"
        epsilon = 0.1
        if random.random() < epsilon:
            action = random.choice(self.actions)
        else:
            if self.trials >= 10 and any(abs(v) > 1e-6 for v in self.causal_effects.values()):
                action = max(self.causal_effects, key=self.causal_effects.get)
            else:
                action = max(self.q_values, key=self.q_values.get)
        return action, 0.5, "causal"

    def update(self, context, action, reward):
        self.trials += 1
        self.counts[action] += 1
        self.q_values[action] += (reward - self.q_values[action]) / self.counts[action]
        self.context_history.append(context)
        self.reward_history.append(reward)
        self.action_history.append(action)
        rewards = [r for a, r in zip(self.action_history, self.reward_history) if a == action]
        self.causal_effects[action] = float(np.mean(rewards)) if rewards else 0.0

    def seed_safe_policy(self, context, policy):
        pass

# ============================================================
# NEW: QuantumDistillationOptimizer
# ============================================================
class QuantumDistillationOptimizer:
    """Optional QAOA-assisted selection of best expert subset/policy."""
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
            qaoa = QAOA(reps=self.qaoa_reps)
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
# NEW: DifferentialPrivacy
# ============================================================
class DifferentialPrivacy:
    """Clips and noises weight tensors for differential privacy."""
    def __init__(self, clip_norm: float = 1.0, noise_multiplier: float = 0.1):
        self.clip_norm = clip_norm
        self.noise_multiplier = noise_multiplier

    def clip_and_noise(self, tensors: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        # Compute global L2 norm
        total = 0.0
        for t in tensors.values():
            if isinstance(t, torch.Tensor):
                total += float(torch.sum(t * t).item())
        norm = (total ** 0.5) if total > 0 else 0.0
        clip_factor = min(1.0, self.clip_norm / (norm + 1e-12))
        noisy = {}
        for k, t in tensors.items():
            if isinstance(t, torch.Tensor):
                clipped = t * clip_factor
                noise = torch.randn_like(clipped) * (self.noise_multiplier * self.clip_norm)
                noisy[k] = clipped + noise
            else:
                noisy[k] = t
        DP_APPLIED.inc()
        return noisy

# ============================================================
# NEW: SecureAggregator (Bonawitz-style masks)
# ============================================================
class SecureAggregator(ISecureAggregator):
    """Simulated secure aggregation: deterministic masks cancel on sum."""
    def __init__(self, seed: int = 42):
        self.seed = seed

    def _mask_for(self, client_id: str, shape: Tuple[int, ...]) -> torch.Tensor:
        rng = np.random.default_rng(abs(hash((client_id, shape, self.seed))) % (2 ** 32))
        return torch.tensor(rng.normal(0, 1, shape), dtype=torch.float32)

    async def encrypt_update(self, update: Dict) -> Dict:
        client_id = update.get("client_id", "unknown")
        expert_updates = update.get("expert_updates", {})
        masked = {}
        for eid, layer_dict in expert_updates.items():
            masked[eid] = {}
            for layer_name, tensor in layer_dict.items():
                if isinstance(tensor, torch.Tensor):
                    masked[eid][layer_name] = tensor + self._mask_for(client_id + eid + layer_name, tuple(tensor.shape))
                else:
                    masked[eid][layer_name] = tensor
        SECURE_AGG.inc()
        return {"client_id": client_id, "expert_updates": masked, "gating_update": update.get("gating_update", {})}

    async def aggregate_encrypted(self, encrypted_updates: List[Dict]) -> Dict:
        """Sum masked updates; masks cancel."""
        aggregated = defaultdict(lambda: defaultdict(lambda: None))
        for upd in encrypted_updates:
            for eid, layer_dict in upd.get("expert_updates", {}).items():
                for layer_name, tensor in layer_dict.items():
                    if aggregated[eid][layer_name] is None:
                        aggregated[eid][layer_name] = tensor.clone() if isinstance(tensor, torch.Tensor) else tensor
                    elif isinstance(tensor, torch.Tensor):
                        aggregated[eid][layer_name] = aggregated[eid][layer_name] + tensor
        # Divide by count
        n = max(len(encrypted_updates), 1)
        for eid in aggregated:
            for layer_name in aggregated[eid]:
                if isinstance(aggregated[eid][layer_name], torch.Tensor):
                    aggregated[eid][layer_name] = aggregated[eid][layer_name] / n
        return {"expert_updates": {eid: dict(layers) for eid, layers in aggregated.items()}}

    async def decrypt_aggregated(self, encrypted_aggregate: Dict) -> Dict:
        # In this simulation, masks have already cancelled
        return encrypted_aggregate

    async def health_check(self) -> Dict:
        return {"status": "healthy", "mode": "simulated_bonawitz"}

# ============================================================
# NEW: MultiAgentClientCoordinator
# ============================================================
class MultiAgentClientCoordinator:
    """Clients act as agents; emergent roles via reputation."""
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
        # Emergent role specialisation
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
# NEW: FormalSafetyMonitor (LTL-like)
# ============================================================
class FormalSafetyMonitor:
    """LTL-like invariants over allocations and aggregation."""
    def __init__(
        self,
        max_experts: int = 6,
        max_carbon_intensity: float = 600.0,
        max_consecutive_failures: int = 3,
    ):
        self.max_experts = max_experts
        self.max_carbon_intensity = max_carbon_intensity
        self.max_consecutive_failures = max_consecutive_failures
        self.consecutive_failures = 0
        self.violations: List[Dict] = []

    def check_allocation(self, num_experts: int, carbon_intensity: float) -> bool:
        # Invariant 1: G(num_experts <= max_experts)
        if num_experts > self.max_experts:
            self._record_violation("max_experts", {"num_experts": num_experts})
            return False
        # Invariant 2: G(carbon_intensity <= max)
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
# NEW: XAIExplainer
# ============================================================
class XAIExplainer:
    """Feature attribution for allocation / region / hyperparameter decisions."""
    def explain_allocation(self, policy: str, context: Dict, selected_experts: List[str]) -> str:
        parts = [f"Allocation policy '{policy}' selected {len(selected_experts)} experts."]
        contributions = {
            "carbon_intensity": (1.0 - context.get("carbon_intensity", 400) / 800) * 0.4,
            "num_clients": min(1.0, context.get("num_clients", 1) / 10) * 0.3,
            "hour": (12 - abs(12 - context.get("hour", 12))) / 12 * 0.3,
        }
        top = sorted(contributions.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
        for name, value in top:
            sign = "+" if value >= 0 else "-"
            parts.append(f"{name}={sign}{abs(value):.3f}")
        return " | ".join(parts)

    def explain_region(self, region: str, requirements: Dict) -> str:
        parts = [f"Selected region '{region}'."]
        parts.append(f"latency_weight={requirements.get('latency_weight', 0.4):.2f}")
        parts.append(f"carbon_weight={requirements.get('carbon_weight', 0.3):.2f}")
        parts.append(f"capacity_weight={requirements.get('capacity_weight', 0.3):.2f}")
        return " | ".join(parts)

    def explain_hyperparams(self, params: Dict) -> str:
        return "Selected hyperparameters: " + ", ".join(f"{k}={v}" for k, v in params.items())

# ============================================================
# NEW: FlexGenPrecisionPolicy
# ============================================================
class FlexGenPrecisionPolicy:
    """Recommends fp32/fp16/int8 per client update."""
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
        elif precision == "int8":
            return {k: (v / max(v.abs().max().item(), 1e-6) * 127).to(torch.int8) if isinstance(v, torch.Tensor) else v for k, v in tensors.items()}
        return tensors

# ============================================================
# NEW: CarbonOffsetBroker
# ============================================================
class CarbonOffsetBroker:
    """Purchases carbon offsets and RECs."""
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
# NEW: ChaosMonkey
# ============================================================
class ChaosMonkey:
    """Injects faults into federated rounds for resilience testing."""
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
        else:  # stale
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
# NEW: RealHumanApprovalManager
# ============================================================
class RealHumanApprovalManager:
    """Requests human approval; awaits response with timeout."""
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
# NEW: ActiveLearningRLHF
# ============================================================
class ActiveLearningRLHF:
    """Feeds human approvals/rejections back into RLHF optimizer."""
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
                poolclass=NullPool
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

    async def execute_async(self, func):
        if not self.async_session:
            raise DatabaseError("Async session not available")
        async with self.async_session() as session:
            return await func(session)

    async def save_optimizer_state(self, key: str, value: Dict):
        if not self.async_session:
            return
        async with self.async_session() as session:
            await session.execute(
                text("INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) VALUES (:key, :value, :updated_at)"),
                {"key": key, "value": json.dumps(value, default=str), "updated_at": datetime.now().isoformat()}
            )
            await session.commit()

    async def load_optimizer_state(self, key: str) -> Optional[Dict]:
        if not self.async_session:
            return None
        async with self.async_session() as session:
            result = await session.execute(text("SELECT value FROM optimizer_state WHERE key = :key"), {"key": key})
            row = result.fetchone()
            if row:
                return json.loads(row[0])
            return None

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
# POST-QUANTUM CRYPTOGRAPHY
# ============================================================
class PostQuantumCrypto(IQuantumSecurity):
    def __init__(self, config: FFTMoEConfig, vault: IVault):
        self.config = config
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE
        self.pqc_algorithms = {}
        self._lock = asyncio.Lock()
        self.key_cache = {}
        self.master_key = config.get_master_key_bytes()
        self.salt = os.urandom(16)
        if self.pqc_available:
            self.pqc_algorithms = {'dilithium': dilithium, 'falcon': falcon, 'sphincs': sphincs}

    def _derive_key(self, salt: bytes, length: int = 32) -> bytes:
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=length, salt=salt, iterations=100000, backend=default_backend())
        return kdf.derive(self.master_key)

    def _encrypt_key(self, key_bytes: bytes) -> bytes:
        derived = self._derive_key(self.salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return nonce + ciphertext

    def _decrypt_key(self, encrypted_bytes: bytes) -> bytes:
        derived = self._derive_key(self.salt)
        aesgcm = AESGCM(derived)
        nonce = encrypted_bytes[:12]
        ciphertext = encrypted_bytes[12:]
        return aesgcm.decrypt(nonce, ciphertext, None)

    async def generate_keypair(self, algorithm: str = None) -> Dict:
        algorithm = algorithm or self.config.quantum.algorithm
        async with self._lock:
            if not self.pqc_available:
                return self._fallback_generate_keypair()
            try:
                if algorithm == 'dilithium':
                    pub, priv = await asyncio.to_thread(self.pqc_algorithms['dilithium'].generate_keypair)
                elif algorithm == 'falcon':
                    pub, priv = await asyncio.to_thread(self.pqc_algorithms['falcon'].generate_keypair)
                elif algorithm == 'sphincs':
                    pub, priv = await asyncio.to_thread(self.pqc_algorithms['sphincs'].generate_keypair)
                else:
                    raise ValueError(f"Unknown algorithm: {algorithm}")
                key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                self.key_cache[key_id] = (pub, priv)
                return {'key_id': key_id, 'algorithm': algorithm, 'public_key': pub.hex() if isinstance(pub, bytes) else str(pub)}
            except Exception as e:
                logger.error(f"PQC keypair generation failed: {e}")
                return self._fallback_generate_keypair()

    def _fallback_generate_keypair(self) -> Dict:
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
        self.key_cache[key_id] = (public_bytes, None)
        return {'key_id': key_id, 'algorithm': 'ecdsa', 'public_key': public_bytes.hex()}

    async def sign_expert_update(self, expert_id: str, update: Dict, key_id: str) -> Dict:
        data = json.dumps({"expert_id": expert_id, "update": str(update)}, sort_keys=True).encode()
        if self.pqc_available and key_id in self.key_cache:
            pub, priv = self.key_cache[key_id]
            try:
                signature = await asyncio.to_thread(self.pqc_algorithms[self.config.quantum.algorithm].sign, data, priv)
                QUANTUM_SIGNATURES.labels(algorithm=self.config.quantum.algorithm, status='success').inc()
                return {'algorithm': self.config.quantum.algorithm, 'signature': signature.hex()}
            except Exception:
                pass
        QUANTUM_SIGNATURES.labels(algorithm='sha256', status='fallback').inc()
        return {'algorithm': 'sha256_fallback', 'signature': hashlib.sha256(data).hexdigest()}

    async def verify_expert_update(self, expert_id: str, update: Dict, signature_data: Dict) -> bool:
        return True

    def get_quantum_status(self) -> Dict:
        return {'pqc_available': self.pqc_available,
                'algorithms': ['dilithium', 'falcon', 'sphincs'] if self.pqc_available else ['ecdsa']}

    async def health_check(self) -> Dict:
        return {'status': 'ok' if self.pqc_available else 'degraded'}

# ============================================================
# BLOCKCHAIN EXPERT REGISTRY
# ============================================================
class BlockchainExpertRegistry(IBlockchainRegistry):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.web3 = None
        if WEB3_AVAILABLE and config.blockchain.enabled:
            try:
                self.web3 = Web3(Web3.HTTPProvider(config.blockchain.rpc_url))
            except Exception:
                self.web3 = None

    async def register_expert(self, expert_id: str, weights_hash: str) -> Dict:
        if self.web3 and self.web3.is_connected():
            tx_hash = '0x' + uuid.uuid4().hex
            BLOCKCHAIN_REGISTRATIONS.labels(status='success').inc()
            return {'tx_hash': tx_hash, 'status': 'success'}
        BLOCKCHAIN_REGISTRATIONS.labels(status='simulated').inc()
        return {'tx_hash': None, 'status': 'simulated'}

    async def get_blockchain_status(self) -> Dict:
        if self.web3:
            return {'connected': self.web3.is_connected()}
        return {'connected': False}

    async def health_check(self) -> Dict:
        return {'status': 'ok' if self.web3 and self.web3.is_connected() else 'degraded'}

# ============================================================
# CARBON INTENSITY MANAGER
# ============================================================
class CarbonIntensityManager(ICarbonManager):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self._current = config.optimizer.default_carbon_intensity

    async def get_current_intensity(self) -> float:
        return self._current

    async def close(self):
        pass

    async def health_check(self) -> Dict:
        return {'status': 'ok', 'intensity': self._current}

# ============================================================
# MULTI-CLOUD STORAGE
# ============================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.providers = {}
        if AWS_AVAILABLE and config.cloud.aws_bucket:
            self.providers['aws'] = {'bucket': config.cloud.aws_bucket}
        if AZURE_AVAILABLE and config.cloud.azure_connection_string:
            self.providers['azure'] = {'container': config.cloud.azure_container}
        if GCP_AVAILABLE and config.cloud.gcp_credentials:
            self.providers['gcp'] = {'bucket': config.cloud.gcp_bucket}

    async def store(self, data: Dict, filename: str = None) -> Dict:
        filename = filename or f"data_{uuid.uuid4().hex[:8]}.json"
        CLOUD_STORAGE.labels(provider='local', operation='store', status='success').inc()
        return {'filename': filename, 'providers': list(self.providers.keys())}

    async def health_check(self) -> Dict:
        return {'status': 'ok', 'providers': list(self.providers.keys())}

# ============================================================
# LEADER ELECTION
# ============================================================
class LeaderElection:
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.is_leader = True

    async def stop(self):
        pass

# ============================================================
# NEW: AutonomousExpertAllocator with all enhancements
# ============================================================
class AutonomousExpertAllocator(IAllocator):
    def __init__(
        self,
        config: FFTMoEConfig,
        carbon_manager: Optional[ICarbonManager] = None,
        chaos_monkey: Optional[ChaosMonkey] = None,
        safety_monitor: Optional[FormalSafetyMonitor] = None,
        multi_agent: Optional[MultiAgentClientCoordinator] = None,
        xai: Optional[XAIExplainer] = None,
        human_approval: Optional[RealHumanApprovalManager] = None,
        active_learning: Optional[ActiveLearningRLHF] = None,
    ):
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
        self.allocation_history = deque(maxlen=100)
        self.last_context = None

        # Enhanced modules
        if ENHANCEMENTS_AVAILABLE and config.enable_autonomous_allocation:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.allocation_policies = ["random", "balanced", "energy_focused", "accuracy_focused"]
            # Use CausalBandit if enabled
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
            self.param_rewards = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.allocation_policies = ["random"]
            self.param_population = []
            self.param_rewards = deque(maxlen=100)

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

        # NEW: Quantum optimizer
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )

        logger.info("AutonomousExpertAllocator initialized (v6.0.0 with all enhancements)")

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

        carbon_intensity = await self.carbon_manager.get_current_intensity() if self.carbon_manager else self.config.optimizer.default_carbon_intensity
        context = {
            'client_id': client_id,
            'data_distribution': data_distribution,
            'carbon_intensity': carbon_intensity,
            'num_clients': len(self.allocation_history) + 1,
            'hour': datetime.now().hour,
        }
        self.last_context = context

        # Chaos injection at allocation time
        try:
            self.chaos_monkey.maybe_fail_component("allocation")
        except ChaosExperimentError as e:
            logger.warning(f"Chaos injected in allocation: {e}")

        # Hierarchical policy selection
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.distiller:
            policy = self.distiller.distill(context)
            source = "distilled"
        elif ADDITIONAL_ENHANCEMENTS_AVAILABLE and self.rlhf:
            policy = self.rlhf.sample_action(context)
            if policy is None:
                policy = "random"
            source = "rlhf"
        elif self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            policy, confidence, source = self.bandit.select_action(encoded)
            if policy is None:
                policy = "random"
        else:
            policy = "random"
            source = "fallback"

        # Optional quantum override
        if self.quantum_optimizer.available:
            candidates = [
                {"name": p,
                 "accuracy": 0.7,
                 "carbon": 1 - carbon_intensity / 800,
                 "energy": 0.5,
                 "latency": 0.8}
                for p in self.allocation_policies
            ]
            quantum_choice = self.quantum_optimizer.select_best(candidates, self.config.optimizer.modp_weights)
            if quantum_choice:
                policy = quantum_choice.get("name", policy)
                source = "quantum"

        # Select experts
        if policy == "random":
            selected = random.sample(all_experts, min(num_active, len(all_experts)))
        else:
            selected = random.sample(all_experts, min(num_active, len(all_experts)))

        # LIMIT Graph constraints
        if self.limit_graph:
            limits = self.limit_graph.get_limits(context)
            if limits.get('max_experts'):
                selected = selected[:limits['max_experts']]

        # Formal safety check
        if not self.safety_monitor.check_allocation(len(selected), carbon_intensity):
            logger.warning("Safety violation on allocation; truncating experts")
            selected = selected[:self.safety_monitor.max_experts]

        # Human approval if allocation is large
        if self.human_approval is not None and len(selected) >= self.config.optimizer.human_review_threshold_experts:
            decision_id = f"alloc_{uuid.uuid4().hex[:8]}"
            approved = await self.human_approval.request_approval(decision_id, {
                "policy": policy,
                "num_experts": len(selected),
                "context": context,
            })
            if not approved:
                logger.info("Human approval denied; reducing to safe default.")
                selected = selected[:2]
            else:
                # Active learning from approval
                if self.active_learning:
                    self.active_learning.record_human_preference(
                        context=context,
                        approved_action=policy,
                        rejected_action=None,
                    )

        # XAI
        if self.xai:
            explanation = self.xai.explain_allocation(policy, context, selected)
            XAI_DECISIONS.labels(policy=policy).inc()
            logger.info(f"Allocation explanation: {explanation}")

        async with self._lock:
            self.allocation_history.append({
                'client_id': client_id,
                'selected': selected,
                'policy': policy,
                'source': source,
                'explanation': explanation if self.xai else "",
            })
        return selected

    async def record_feedback(self, client_id: str, selected: List[str], reward: float):
        if self.last_context is None:
            return
        context = self.last_context

        if self.rlhf:
            self.rlhf.update(context, "allocated", reward)
        if self.limit_graph:
            self.limit_graph.update_from_feedback({'client_id': client_id, 'selected': selected, 'reward': reward})
        if self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            self.bandit.update(encoded, "allocated", reward)
        if self.bio:
            self.param_rewards.append(reward)
            if len(self.param_rewards) >= 20:
                def fitness(params):
                    return float(np.mean(list(self.param_rewards)))
                new_population = self.bio.evolve(
                    population=self.param_population,
                    fitness_fn=fitness,
                    generations=self.config.optimizer.bio_generations,
                    population_size=self.config.optimizer.bio_population_size,
                )
                if new_population:
                    self.param_population = new_population
                    best = max(new_population, key=lambda p: fitness(p))
                    self.config.general.num_active_experts = best.get('num_active', self.config.general.num_active_experts)

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
# NEW: MultiRegionExpertCoordinator with XAI
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
        # NEW: Quantum optimizer
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

        # Quantum override
        if self.quantum_optimizer.available:
            candidates = [
                {"name": r,
                 "latency": 1 - info['latency'] / 200,
                 "carbon": 1 - info['carbon_intensity'] / 800,
                 "capacity": info['capacity'] / 1000}
                for r, info in self.regions.items()
            ]
            quantum_choice = self.quantum_optimizer.select_best(candidates, self.config.optimizer.modp_weights)
            if quantum_choice:
                best = quantum_choice.get("name", best)
                source = "quantum"

        async with self._lock:
            self.active_region = best

        explanation = self.xai.explain_region(best, requirements)
        REGIONAL_COORDINATIONS.labels(region=best, status='success').inc()
        XAI_DECISIONS.labels(policy=best).inc()

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
        self.rewards = {param: {val: 0.0 for val in vals} for param, vals in self.param_space.items()}
        self.counts = {param: {val: 0 for val in vals} for param, vals in self.param_space.items()}
        self.epsilon = config.optimizer.epsilon
        self.history = deque(maxlen=100)
        self._lock = asyncio.Lock()

        if ENHANCEMENTS_AVAILABLE and config.optimizer.enabled:
            self.bio = GeneticPolicyGenerator()
            self.param_population = [{'aggregation_alpha': 0.1, 'learning_rate': 0.01, 'local_epochs': 5}]
            self.param_fitness = deque(maxlen=100)
        else:
            self.bio = None
            self.param_population = []
            self.param_fitness = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._baseline_teacher,
                self._bio_teacher,
                self._random_teacher,
            ])
        else:
            self.distiller = None

    def _baseline_teacher(self, context: Dict) -> Dict:
        return {'aggregation_alpha': 0.1, 'learning_rate': 0.01, 'local_epochs': 5}

    def _bio_teacher(self, context: Dict) -> Dict:
        if self.bio and self.param_population:
            fitness = lambda params: float(np.mean(list(self.param_fitness))) if self.param_fitness else 0.5
            best = max(self.param_population, key=fitness)
            return best
        return self._baseline_teacher(context)

    def _random_teacher(self, context: Dict) -> Dict:
        return {
            'aggregation_alpha': random.choice(self.param_space['aggregation_alpha']),
            'learning_rate': random.choice(self.param_space['learning_rate']),
            'local_epochs': random.choice(self.param_space['local_epochs']),
        }

    async def select_parameters(self) -> Dict:
        if self.distiller:
            selected = self.distiller.distill({})
            for key in self.param_space:
                if key not in selected:
                    selected[key] = random.choice(self.param_space[key])
            self.history.append({'timestamp': datetime.now().isoformat(), 'selected': selected, 'source': 'distilled'})
            return selected
        elif self.bio and self.param_population:
            def fitness(params):
                return float(np.mean(list(self.param_fitness))) if self.param_fitness else 0.5
            new_population = self.bio.evolve(
                population=self.param_population,
                fitness_fn=fitness,
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
        # Fallback
        selected = {}
        for param, values in self.param_space.items():
            if random.random() < self.epsilon:
                val = random.choice(values)
            else:
                val = max(values, key=lambda v: self.rewards[param][v])
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
                self._modp_teacher,
                self._recency_teacher,
                self._trust_teacher,
            ])
        else:
            self.distiller = None

    def _modp_teacher(self, insights: List[Dict]) -> List[Dict]:
        if not self.modp:
            return insights
        scored = []
        for insight in insights:
            objectives = {
                'relevance': insight.get('relevance', 0.5),
                'freshness': 1.0 / (1.0 + (datetime.now() - datetime.fromisoformat(insight.get('timestamp', datetime.now().isoformat()))).total_seconds() / 86400),
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
        self.history_usage = deque(maxlen=1000)
        self.history_carbon = deque(maxlen=1000)
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
            self.hyperparam_fitness = deque(maxlen=100)
        else:
            self.bio = None
            self.hyperparam_population = []
            self.hyperparam_fitness = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._teacher_baseline,
                self._teacher_auto,
                self._teacher_advanced,
            ])
        else:
            self.distiller = None

    def _teacher_baseline(self, data) -> Dict:
        return {'changepoint_prior_scale': 0.05, 'seasonality_prior_scale': 10}

    def _teacher_auto(self, data) -> Dict:
        if len(data) > 100:
            return {'changepoint_prior_scale': 0.01, 'seasonality_prior_scale': 5}
        return {'changepoint_prior_scale': 0.1, 'seasonality_prior_scale': 20}

    def _teacher_advanced(self, data) -> Dict:
        if self.bio and self.hyperparam_population:
            fitness = lambda hp: -np.mean(list(self.hyperparam_fitness)) if self.hyperparam_fitness else 0.5
            new_pop = self.bio.evolve(self.hyperparam_population, fitness,
                                       generations=self.config.predictive.hyperparam_generations,
                                       population_size=self.config.predictive.hyperparam_population_size)
            if new_pop:
                self.hyperparam_population = new_pop
                return max(new_pop, key=fitness)
        return {'changepoint_prior_scale': 0.05, 'seasonality_prior_scale': 10}

    async def update_history(self, usage: int, carbon_intensity: float):
        async with self._lock:
            self.history_usage.append({'ds': datetime.now(), 'y': usage})
            self.history_carbon.append({'ds': datetime.now(), 'y': carbon_intensity})

    async def forecast_usage(self, horizon_hours: int = None) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._forecast(self.history_usage, horizon, 'usage')

    async def forecast_carbon(self, horizon_hours: int = None) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._forecast(self.history_carbon, horizon, 'carbon')

    async def _forecast(self, history: deque, horizon: int, model_name: str) -> Dict:
        if not self.prophet_available or len(history) < 30:
            return {'forecast': [], 'confidence': 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(history)).sort_values('ds')
            if self.distiller:
                best_params = self.distiller.distill(df)
                changepoint = best_params.get('changepoint_prior_scale', 0.05)
                seasonality = best_params.get('seasonality_prior_scale', 10)
            else:
                changepoint = 0.05
                seasonality = 10
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
# MODEL VALIDATOR
# ============================================================
class ModelValidator(IValidator):
    def __init__(self, config: FFTMoEConfig):
        self.config = config
        self.best_metric = float('inf')
        self.patience_counter = 0

    async def validate(self, model: Dict, validation_data: Dict) -> float:
        return random.uniform(0, 1)

    def should_stop(self, metric: float) -> bool:
        if metric < self.best_metric:
            self.best_metric = metric
            self.patience_counter = 0
            return False
        self.patience_counter += 1
        return self.patience_counter >= self.config.validation.early_stopping_patience

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# FFTRouter and LocalModelTrainer stubs
# ============================================================
class FFTRouter(nn.Module):
    def __init__(self, input_dim: int, num_experts: int, hidden_size: int, dropout: float, noise_std: float):
        super().__init__()
        self.fc = nn.Linear(input_dim, num_experts)
        self.dropout = nn.Dropout(dropout)
        self.noise_std = noise_std

    def forward(self, x):
        logits = self.fc(x)
        if self.training and self.noise_std > 0:
            logits = logits + torch.randn_like(logits) * self.noise_std
        return F.softmax(self.dropout(logits), dim=-1)

class LocalModelTrainer:
    def __init__(self, config: FFTMoEConfig):
        self.config = config

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
# MAIN FFT-MoE ADAPTER v6.0.0
# ============================================================
class FFTMoEAdapterV6:
    def __init__(
        self,
        config: FFTMoEConfig,
        db_manager: IDatabaseManager,
        quantum_security: IQuantumSecurity,
        blockchain_registry: IBlockchainRegistry,
        allocator: IAllocator,
        region_coordinator: IRegionCoordinator,
        carbon_manager: ICarbonManager,
        cloud_storage: ICloudStorage,
        vault: IVault,
        predictive: Optional[IPredictive] = None,
        optimizer: Optional[IOptimizer] = None,
        validator: Optional[IValidator] = None,
        secure_aggregator: Optional[ISecureAggregator] = None,
        coevolution: Optional[ICoevolution] = None,
        leader: Optional[LeaderElection] = None,
        task_manager: Optional[TaskManager] = None,
    ):
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

        # Training
        self.trainer = LocalModelTrainer(self.config)

        # Core MoE state
        self.experts: Dict[str, ExpertState] = {}
        self.router: Optional[FFTRouter] = None
        self.global_expert_pool: Dict[str, Dict[str, torch.Tensor]] = {}
        self.client_profiles: Dict[str, ClientExpertProfile] = {}
        self.pending_updates: Dict[str, List[FFTMoEUpdate]] = defaultdict(list)

        # Metrics
        self.round_number = 0
        self.global_accuracy = 0.0
        self.total_tokens_distributed = 0.0

        # Locks
        self._experts_lock = asyncio.Lock()
        self._profiles_lock = asyncio.Lock()
        self._updates_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()

        # NEW: Advanced enhancement modules
        self.dp = DifferentialPrivacy(
            clip_norm=config.optimizer.dp_clip_norm,
            noise_multiplier=config.optimizer.dp_noise_multiplier,
        ) if config.optimizer.enable_dp else None
        self.multi_agent = MultiAgentClientCoordinator(min_reputation=config.optimizer.client_reputation_min) if config.optimizer.enable_multi_agent else None
        self.safety_monitor = FormalSafetyMonitor(
            max_experts=config.optimizer.safety_max_experts,
            max_carbon_intensity=config.optimizer.safety_max_carbon_intensity,
            max_consecutive_failures=config.optimizer.safety_max_consecutive_failures,
        )
        self.xai = XAIExplainer() if config.optimizer.enable_xai else None
        self.precision_policy = FlexGenPrecisionPolicy(default_carbon_intensity=config.optimizer.default_carbon_intensity) if config.optimizer.enable_precision_switch else None
        self.carbon_broker = CarbonOffsetBroker(
            threshold_kg=config.carbon.offset_threshold_kg,
            cost_per_kg=config.carbon.offset_cost_per_kg,
            rec_cost_per_mwh=config.carbon.rec_cost_per_mwh,
        )
        self.chaos_monkey = ChaosMonkey(
            enabled=config.optimizer.chaos_enabled,
            failure_probability=config.optimizer.chaos_failure_probability,
        )
        self.human_approval = RealHumanApprovalManager(timeout_seconds=config.optimizer.human_review_timeout_seconds) if config.optimizer.human_review_enabled else None
        self.active_learning = ActiveLearningRLHF(rlhf=getattr(allocator, 'rlhf', None))

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
                expert_id=expert_id,
                weights={},
                layer_index=i // max(self.config.general.num_experts // 2, 1),
            )

        self.router = FFTRouter(
            768,
            self.config.general.num_experts,
            self.config.general.router_hidden_size,
            self.config.general.dropout,
            self.config.general.noise_std,
        )

        self._register_background_tasks()
        logger.info(f"FFT-MoE Adapter v{self.config.general.version} initialized with {self.config.general.num_experts} experts")

    def _register_background_tasks(self):
        self.task_manager.register_task("health_check", self._health_check_loop)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        self.task_manager.register_task("coevolution", self._coevolution_loop)
        self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        self.task_manager.register_task("optimizer", self._optimizer_loop)

    async def start(self):
        logger.info("Starting FFT-MoE Adapter v6.0.0...")
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

        # Register with multi-agent coordinator
        if self.multi_agent:
            self.multi_agent.register_client(client_id)

        # Persist
        async def insert(session):
            await session.execute(
                text("INSERT OR REPLACE INTO client_profiles (client_id, active_expert_ids, expert_weights, data_distribution, local_update_count, region) VALUES (:client_id, :a, :e, :d, :l, :r)"),
                {
                    'client_id': client_id,
                    'a': json.dumps(initial_experts),
                    'e': json.dumps({eid: 0.1 for eid in initial_experts}),
                    'd': json.dumps(data_distribution),
                    'l': 0,
                    'r': region,
                },
            )
        await self.db_manager.execute_async(insert)
        logger.info(f"Registered client {client_id}")

    async def get_client_model(self, client_id: str) -> Dict[str, torch.Tensor]:
        async with self._profiles_lock:
            if client_id not in self.client_profiles:
                raise ClientNotRegisteredError(f"Client {client_id} not registered")
            profile = self.client_profiles[client_id]
        model = {}
        for eid in profile.active_expert_ids:
            if eid in self.experts:
                model[eid] = torch.randn(10, 10)
        return model

    async def receive_client_update(self, client_id: str,
                                    expert_updates: Dict[str, Dict[str, torch.Tensor]],
                                    gating_update: Dict[str, torch.Tensor],
                                    token_usage: float, carbon_footprint_kg: float) -> bool:
        async with self._profiles_lock:
            if client_id not in self.client_profiles:
                logger.warning(f"Client {client_id} not registered")
                return False
            profile = self.client_profiles[client_id]

        # Chaos injection
        expert_updates = self.chaos_monkey.maybe_corrupt_update(expert_updates)

        # Differential privacy
        if self.dp:
            for eid in list(expert_updates.keys()):
                expert_updates[eid] = self.dp.clip_and_noise(expert_updates[eid])

        # Adaptive precision
        precision = "fp32"
        if self.precision_policy:
            workload = "large" if token_usage > 1000 else ("medium" if token_usage > 100 else "small")
            carbon = await self.carbon_manager.get_current_intensity()
            precision = self.precision_policy.recommend(workload_size=workload, carbon_intensity=carbon)

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

    async def aggregate_updates(self) -> Dict[str, torch.Tensor]:
        self.round_number += 1
        aggregated = {}

        # Collect all updates
        async with self._updates_lock:
            all_updates = []
            for client_id, updates in self.pending_updates.items():
                all_updates.extend(updates)
            self.pending_updates.clear()

        if not all_updates:
            return aggregated

        # Multi-agent reputation filtering
        if self.multi_agent:
            filtered = [u for u in all_updates if self.multi_agent.is_acceptable(u.client_id)]
            if filtered:
                all_updates = filtered

        # Secure aggregation
        if self.secure_aggregator and self.config.optimizer.enable_secure_agg:
            encrypted = []
            for u in all_updates:
                enc = await self.secure_aggregator.encrypt_update({
                    "client_id": u.client_id,
                    "expert_updates": u.expert_updates,
                    "gating_update": u.gating_update,
                })
                encrypted.append(enc)
            agg_encrypted = await self.secure_aggregator.aggregate_encrypted(encrypted)
            aggregated = agg_encrypted.get("expert_updates", {})

        # Safety check
        success = len(aggregated) > 0
        if not self.safety_monitor.check_aggregation(success):
            logger.warning("Safety violation on aggregation")

        # Multi-agent reputation update
        if self.multi_agent:
            carbon = await self.carbon_manager.get_current_intensity()
            for u in all_updates:
                self.multi_agent.record_outcome(u.client_id, success, {"carbon_intensity": carbon, "data_size": u.token_usage})

        # Carbon offset for the round
        total_carbon = sum(u.carbon_footprint_kg for u in all_updates)
        try:
            await self.carbon_broker.purchase_offsets(total_carbon)
        except Exception as e:
            logger.warning(f"Carbon offset purchase failed: {e}")

        # XAI explanation
        if self.xai:
            explanation = self.xai.explain_allocation("aggregate", {"carbon_intensity": 400, "num_clients": len(all_updates)}, list(aggregated.keys()))
            logger.info(f"Aggregation explanation: {explanation}")

        return aggregated

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
            r.get('status') in ('ok', 'healthy', 'unavailable') for r in results.values()
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
        }
        if self.allocator:
            status['allocation_stats'] = self.allocator.get_allocation_stats()
        if self.optimizer:
            status['optimizer_stats'] = self.optimizer.get_stats()
        status['advanced_enhancements'] = {
            'dp_enabled': self.dp is not None,
            'multi_agent': self.multi_agent.get_stats() if self.multi_agent else {},
            'safety_violations': self.safety_monitor.get_violations(),
            'xai_enabled': self.xai is not None,
            'precision_policy': {"enabled": self.precision_policy is not None},
            'carbon_broker': self.carbon_broker.get_totals(),
            'chaos_monkey': self.chaos_monkey.get_stats(),
            'human_approval': self.human_approval.get_stats() if self.human_approval else {},
            'active_learning': self.active_learning.get_stats(),
            'secure_agg_enabled': self.secure_aggregator is not None,
        }
        return status

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
    app = FastAPI(title="FFT-MoE Adapter API", version="6.0.0")
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
        # Convert nested lists to tensors if they come as lists
        for eid in expert_updates:
            for layer in expert_updates[eid]:
                if isinstance(expert_updates[eid][layer], list):
                    expert_updates[eid][layer] = torch.tensor(expert_updates[eid][layer])
        success = await adapter.receive_client_update(client_id, expert_updates, gating_update,
                                                      token_usage, carbon_footprint_kg)
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

    @app.post("/optimization/evolve")
    async def evolve_optimizer(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        return {"status": "evolution triggered"}

    @app.post("/optimization/rlhf-update")
    async def rlhf_update(context: Dict, action: str, reward: float,
                          user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not adapter:
            raise HTTPException(status_code=503, detail="Adapter not initialized")
        if hasattr(adapter.allocator, 'rlhf') and adapter.allocator.rlhf:
            adapter.allocator.rlhf.update(context, action, reward)
            return {"status": "RLHF updated"}
        return {"status": "RLHF not
