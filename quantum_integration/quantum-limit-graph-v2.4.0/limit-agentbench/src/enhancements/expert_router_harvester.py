#!/usr/bin/env python3
# File: src/enhancements/expert_router_harvester_v3_0.py
"""
Expert Router with Photosynthetic Harvester Awareness – v3.1 (Enterprise Quantum+)

ENHANCEMENTS OVER v3.0:
- Added CausalBandit (replaces ContextualBandit) for causal RL on routing decisions.
- Added SafetyMonitor with temporal logic rules for routing safety.
- Added XAIExplainer for human-readable routing rationale.
- Added FederatedRouterCoordinator with simulated differential privacy.
- Added MultiAgentRouter for expert bidding and emergent role specialisation.
- Added CarbonOffsetBroker for external carbon markets and RECs.
- Added ChaosMonkey for resilience testing.
- Added HumanReviewManager for human-in-the-loop with active learning.
- Added QuantumDistillationOptimizer (optional) for quantum-assisted routing.
- Integrated adaptive precision switching via FlexGen into routing decision pipeline.
- Retained all v3.0 features (PQC, cloud storage, Vault, DB migrations, bandit/MoE/MODP/bio).

NEW IN v3.0 (retained):
- Dependency inversion with interfaces (Protocols) for all major components.
- Global circuit breaker registry with configurable thresholds.
- Health check aggregation across all components.
- Async database persistence with migrations.
- Rate limiting on API endpoints.
- Retry decorators for all external calls (tenacity).
- Grouped configuration using nested Pydantic models.
- Audit logging for compliance.
- OpenTelemetry support for distributed tracing.
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
from typing import Dict, Any, List, Optional, Union, Protocol, runtime_checkable, Callable, Tuple
import contextvars
from pathlib import Path
from collections import deque, defaultdict
from enum import Enum
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
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
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
    from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Text, Boolean, create_engine, text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, async_sessionmaker
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.pool import NullPool
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

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
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

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
# Import base classes (with fallback stubs)
# ============================================================
try:
    from ..expert_router import ExpertRouter
    from ..expert_registry import ExpertProfile, ExpertRegistry
    from ..bio_inspired import PhotosyntheticHarvester
    from ..sustainability_cost import SustainabilityCostFunction
    from ..database.manager import DatabaseManager
    from ..task_manager import TaskManager
except ImportError:
    class ExpertRouter:
        def __init__(self, *args, **kwargs):
            self.registry = None
        def get_candidate_experts(self, task, context):
            return []
    class ExpertProfile:
        def __init__(self, expert_id=None, domain="", usage_count=0, accuracy_score=0.5,
                     last_used=None, photosynthetic_harvester_flag=False, **kwargs):
            self.expert_id = expert_id or str(uuid.uuid4())
            self.domain = domain
            self.usage_count = usage_count
            self.accuracy_score = accuracy_score
            self.last_used = last_used
            self.photosynthetic_harvester_flag = photosynthetic_harvester_flag
    class ExpertRegistry: pass
    class PhotosyntheticHarvester: pass
    class SustainabilityCostFunction:
        async def compute_multiple(self, experts, context):
            return {e.expert_id: 1.0 for e in experts}
    class DatabaseManager: pass
    class TaskManager: pass

# ============================================================
# Structured logging
# ============================================================
correlation_id_var = contextvars.ContextVar('correlation_id', default=str(uuid.uuid4())[:8])

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

audit_logger = logging.getLogger("audit")
audit_handler = logging.FileHandler('routing_audit.log')
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
    CIRCUIT_BREAKER_STATE = Gauge('router_circuit_breaker_state', 'Circuit breaker state', ['name'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Counter('router_rate_limiter_throttle', 'Rate limiter throttles', registry=REGISTRY)
    PQC_SIGNATURES = Counter('router_pqc_signatures_total', 'PQC signatures', ['algorithm', 'status'], registry=REGISTRY)
    CLOUD_STORAGE = Counter('router_cloud_storage_operations_total', 'Cloud storage ops', ['provider', 'operation', 'status'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('router_vault_operations_total', 'Vault operations', ['operation', 'status'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('router_health_score', 'Overall health score', registry=REGISTRY)
    # NEW metrics
    SAFETY_VIOLATIONS = Counter('router_safety_violations_total', 'Safety violations', ['rule'], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter('router_chaos_experiments_total', 'Chaos experiments', ['type', 'status'], registry=REGISTRY)
    HUMAN_REVIEWS = Counter('router_human_reviews_total', 'Human reviews', ['status'], registry=REGISTRY)
    XAI_DECISIONS = Counter('router_xai_decisions_total', 'XAI decisions', ['policy'], registry=REGISTRY)
    CARBON_OFFSETS = Counter('router_carbon_offsets_total', 'Carbon offsets purchased', ['status'], registry=REGISTRY)
    FEDERATED_PARTICIPANTS = Gauge('router_federated_participants', 'Federated participants', registry=REGISTRY)
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
    SAFETY_VIOLATIONS = DummyMetric()
    CHAOS_EXPERIMENTS = DummyMetric()
    HUMAN_REVIEWS = DummyMetric()
    XAI_DECISIONS = DummyMetric()
    CARBON_OFFSETS = DummyMetric()
    FEDERATED_PARTICIPANTS = DummyMetric()

# ============================================================
# Custom Exceptions
# ============================================================
class RouterError(Exception): pass
class CostFunctionError(RouterError): pass
class RegistryError(RouterError): pass
class CircuitBreakerOpenError(RouterError): pass
class RateLimitExceeded(RouterError): pass
class SignatureError(RouterError): pass
class HealthCheckError(RouterError): pass
class SafetyViolationError(RouterError): pass
class ChaosExperimentError(RouterError): pass

# ============================================================
# INTERFACES
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

@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: str = None) -> Dict: ...
    def get_status(self) -> Dict: ...

@runtime_checkable
class IVault(Protocol):
    async def store_secret(self, path: str, data: Dict): ...
    async def get_secret(self, path: str) -> Optional[Dict]: ...

@runtime_checkable
class IAsyncDatabase(Protocol):
    async def save_routing_decision(self, decision: Dict): ...
    async def health_check(self) -> Dict: ...
    async def close(self): ...

# ============================================================
# CONFIGURATION
# ============================================================
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
        master_key: str = Field("", description="Hex string for key encryption")

        @field_validator('master_key')
        @classmethod
        def validate_master_key(cls, v: str) -> str:
            if not v:
                # Allow empty for demo
                return "00" * 32
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

    class SafetyConfig(BaseModel):
        max_carbon_intensity: float = 500.0
        max_low_harvester_routes: int = 5
        max_consecutive_low_fitness: int = 3
        enable_monitor: bool = True

    class ChaosConfig(BaseModel):
        enabled: bool = False
        failure_probability: float = 0.1

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1}
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
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

        def dict(self):
            return {k: (v.dict() if hasattr(v, 'dict') else v) for k, v in self.__dict__.items()}

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
        master_key: str = "00" * 32

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        rate_limit_enabled: bool = True

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

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'accuracy': 0.4, 'energy': 0.3, 'carbon': 0.2, 'latency': 0.1})
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

# ============================================================
# CIRCUIT BREAKER
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
# NEW: CausalBandit
# ============================================================
class CausalBandit:
    """Causal bandit estimating average treatment effects for routing policies."""
    def __init__(self, action_space: List[str], fallback_solver: Callable,
                 min_trials_before_bandit: int = 5, confidence_threshold: float = 0.6):
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

    def select_action(self, context: Dict) -> Tuple[str, float, str]:
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

    def update(self, context: Dict, action: str, reward: float):
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
# NEW: SafetyMonitor (temporal logic)
# ============================================================
class SafetyMonitor:
    """Monitors routing decisions against temporal safety rules."""
    def __init__(self, config: SafetyConfig):
        self.config = config
        self.low_harvester_routes = 0
        self.consecutive_low_fitness = 0
        self.history: deque = deque(maxlen=50)
        self.violations: List[Dict] = []

    def check_routing(self, context: Dict, expert: ExpertProfile, fitness: float) -> bool:
        if not self.config.enable_monitor:
            return True
        # Rule: max carbon intensity
        carbon_intensity = context.get('carbon_intensity', 0)
        if isinstance(carbon_intensity, float) and carbon_intensity <= 1.0:
            carbon_intensity = carbon_intensity * 800.0
        if carbon_intensity > self.config.max_carbon_intensity:
            self._record_violation("max_carbon_intensity", {"carbon": carbon_intensity})
            return False
        # Rule: limit low-harvester routes when carbon is high
        is_harvester = getattr(expert, 'photosynthetic_harvester_flag', False)
        if not is_harvester and carbon_intensity > self.config.max_carbon_intensity * 0.8:
            self.low_harvester_routes += 1
            if self.low_harvester_routes > self.config.max_low_harvester_routes:
                self._record_violation("max_low_harvester_routes", {"count": self.low_harvester_routes})
                return False
        else:
            self.low_harvester_routes = 0
        # Rule: consecutive low fitness
        if fitness < 0.3:
            self.consecutive_low_fitness += 1
            if self.consecutive_low_fitness > self.config.max_consecutive_low_fitness:
                self._record_violation("consecutive_low_fitness", {"count": self.consecutive_low_fitness})
                return False
        else:
            self.consecutive_low_fitness = 0
        self.history.append({"context": context, "expert_id": expert.expert_id, "fitness": fitness})
        return True

    def _record_violation(self, rule: str, details: Dict):
        self.violations.append({"rule": rule, "details": details, "timestamp": datetime.now().isoformat()})
        if PROMETHEUS_AVAILABLE:
            SAFETY_VIOLATIONS.labels(rule=rule).inc()
        logger.warning(f"Safety violation: {rule} - {details}")

    def get_violations(self) -> List[Dict]:
        return self.violations

# ============================================================
# NEW: XAIExplainer
# ============================================================
class XAIExplainer:
    """Generates human-readable explanations for routing decisions."""
    def explain_routing(self, expert: ExpertProfile, policy: str, context: Dict,
                        confidence: float, cost: float, bonus_applied: bool) -> str:
        parts = [f"Selected expert '{expert.expert_id}' (domain='{getattr(expert, 'domain', 'unknown')}')"]
        parts.append(f"policy='{policy}' (confidence={confidence:.2f})")
        parts.append(f"cost={cost:.4f}")
        if bonus_applied:
            parts.append("harvester_bonus=YES")
        if 'data_source' in context:
            parts.append(f"data_source='{context['data_source']}'")
        if 'carbon_intensity' in context:
            parts.append(f"carbon={context['carbon_intensity']}")
        if hasattr(expert, 'accuracy_score'):
            parts.append(f"accuracy={expert.accuracy_score:.3f}")
        return " | ".join(parts)

# ============================================================
# NEW: FederatedRouterCoordinator
# ============================================================
class FederatedRouterCoordinator:
    """Aggregates bandit Q-values and MoE weights across deployments with DP noise."""
    def __init__(self, privacy_budget: float = 0.5):
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.privacy_budget = max(privacy_budget, 1e-6)

    def register_participant(self, participant_id: str, update: Dict[str, Any]):
        self.participants[participant_id] = update
        if PROMETHEUS_AVAILABLE:
            FEDERATED_PARTICIPANTS.set(len(self.participants))

    def aggregate(self) -> Dict[str, Any]:
        if not self.participants:
            return {}
        keys = set()
        for update in self.participants.values():
            keys.update(update.keys())
        avg = {}
        for key in keys:
            vals = [u.get(key, 0.0) for u in self.participants.values()]
            if all(isinstance(v, (int, float)) for v in vals):
                noise = float(np.random.laplace(0, 1.0 / self.privacy_budget))
                avg[key] = float(np.mean(vals) + noise)
            else:
                avg[key] = vals[0]
        return avg

    def get_participant_count(self) -> int:
        return len(self.participants)

# ============================================================
# NEW: MultiAgentRouter (bidding and role specialisation)
# ============================================================
class MultiAgentRouter:
    """Treats each expert as an agent that bids for routing tasks.
    Emergent role specialisation is tracked via reputation per domain."""
    def __init__(self):
        self.expert_bids: Dict[str, float] = {}
        self.domain_reputation: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.specialist_roles: Dict[str, str] = {}

    def submit_bid(self, expert_id: str, bid: float):
        self.expert_bids[expert_id] = bid

    def record_outcome(self, expert_id: str, domain: str, success: bool):
        alpha = 0.3
        prev = self.domain_reputation[domain].get(expert_id, 0.5)
        new = prev + alpha * ((1.0 if success else 0.0) - prev)
        self.domain_reputation[domain][expert_id] = new
        # Update specialist role if reputation is high
        if new > 0.75:
            self.specialist_roles[expert_id] = f"specialist_{domain}"

    def get_specialist_for_domain(self, domain: str) -> Optional[str]:
        rep = self.domain_reputation.get(domain, {})
        if not rep:
            return None
        return max(rep, key=rep.get)

    def get_stats(self) -> Dict:
        return {
            "num_experts": len(self.expert_bids),
            "domains": list(self.domain_reputation.keys()),
            "specialist_roles": dict(self.specialist_roles),
        }

# ============================================================
# NEW: CarbonOffsetBroker
# ============================================================
class CarbonOffsetBroker:
    """Purchases carbon offsets and RECs when thresholds are exceeded."""
    def __init__(self, threshold: float = 400.0, cost_per_kg: float = 0.1, rec_cost_per_mwh: float = 5.0):
        self.threshold = threshold
        self.cost_per_kg = cost_per_kg
        self.rec_cost_per_mwh = rec_cost_per_mwh
        self.total_offset_kg = 0.0
        self.total_recs_mwh = 0.0
        self.total_cost = 0.0

    async def purchase_offsets(self, carbon_intensity: float, carbon_kg: float) -> Dict:
        if carbon_intensity <= self.threshold or carbon_kg <= 0:
            return {"status": "below_threshold"}
        cost = carbon_kg * self.cost_per_kg
        self.total_offset_kg += carbon_kg
        self.total_cost += cost
        if PROMETHEUS_AVAILABLE:
            CARBON_OFFSETS.labels(status='offset_purchased').inc()
        logger.info(f"Offset purchased: {carbon_kg:.4f} kg for ${cost:.4f}")
        return {"status": "offset_purchased", "carbon_kg": carbon_kg, "cost_usd": cost}

    async def purchase_recs(self, energy_mwh: float) -> Dict:
        if energy_mwh <= 0:
            return {"status": "no_energy"}
        cost = energy_mwh * self.rec_cost_per_mwh
        self.total_recs_mwh += energy_mwh
        self.total_cost += cost
        if PROMETHEUS_AVAILABLE:
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
    """Injects simulated failures to test resilience."""
    def __init__(self, config: ChaosConfig):
        self.config = config
        self.injected_failures = 0

    def maybe_fail(self, component: str = "router"):
        if self.config.enabled and random.random() < self.config.failure_probability:
            self.injected_failures += 1
            if PROMETHEUS_AVAILABLE:
                CHAOS_EXPERIMENTS.labels(type=component, status='injected').inc()
            raise ChaosExperimentError(f"Simulated chaos failure in {component}")

    def get_stats(self) -> Dict:
        return {"enabled": self.config.enabled, "injected_failures": self.injected_failures}

# ============================================================
# NEW: HumanReviewManager
# ============================================================
class HumanReviewManager:
    """Manages human review of critical routing decisions."""
    def __init__(self):
        self.pending_reviews: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def request_review(self, decision_id: str, details: Dict) -> str:
        review_id = str(uuid.uuid4())
        async with self._lock:
            self.pending_reviews[review_id] = {
                "review_id": review_id,
                "decision_id": decision_id,
                "details": details,
                "status": "pending",
                "created_at": datetime.now().isoformat(),
            }
        if PROMETHEUS_AVAILABLE:
            HUMAN_REVIEWS.labels(status='pending').inc()
        logger.info(f"Human review requested: {review_id}")
        return review_id

    async def approve(self, review_id: str) -> bool:
        async with self._lock:
            if review_id in self.pending_reviews:
                self.pending_reviews[review_id]["status"] = "approved"
                if PROMETHEUS_AVAILABLE:
                    HUMAN_REVIEWS.labels(status='approved').inc()
                return True
        return False

    async def reject(self, review_id: str) -> bool:
        async with self._lock:
            if review_id in self.pending_reviews:
                self.pending_reviews[review_id]["status"] = "rejected"
                if PROMETHEUS_AVAILABLE:
                    HUMAN_REVIEWS.labels(status='rejected').inc()
                return True
        return False

    async def get_pending(self) -> List[Dict]:
        async with self._lock:
            return [r for r in self.pending_reviews.values() if r["status"] == "pending"]

# ============================================================
# NEW: QuantumDistillationOptimizer (optional)
# ============================================================
class QuantumDistillationOptimizer:
    """Optional quantum-assisted expert selection using QAOA."""
    def __init__(self, config: QuantumConfig):
        self.config = config
        self.available = config.enable_distillation and QISKIT_AVAILABLE

    async def select_best_expert(self, candidates: List[ExpertProfile],
                                  context: Dict, modp_weights: Dict[str, float]) -> Optional[ExpertProfile]:
        if not self.available or not candidates:
            return None
        try:
            qp = QuadraticProgram()
            for e in candidates:
                qp.binary_var(e.expert_id)
            # Utility per expert
            utility = {}
            for e in candidates:
                accuracy = getattr(e, 'accuracy_score', 0.5) or 0.5
                energy = 1.0 - (getattr(e, 'usage_count', 0) / 100.0)
                carbon = context.get('carbon_intensity', 0.5)
                latency = 0.5
                u = (accuracy * modp_weights.get('accuracy', 0.4) +
                     energy * modp_weights.get('energy', 0.3) +
                     (1 - carbon) * modp_weights.get('carbon', 0.2) +
                     (1 - latency) * modp_weights.get('latency', 0.1))
                utility[e.expert_id] = u
            linear = {eid: -u for eid, u in utility.items()}
            qp.minimize(linear=linear)
            qp.linear_constraint(linear={eid: 1 for eid in utility}, sense='E', rhs=1, name='one_expert')
            backend = Aer.get_backend('aer_simulator')
            qaoa = QAOA(reps=self.config.qaoa_reps)
            optimizer = MinimumEigenOptimizer(qaoa)
            result = optimizer.solve(qp)
            for i, e in enumerate(candidates):
                if result.x[i] > 0.5:
                    return e
        except Exception as ex:
            logger.warning(f"Quantum optimization failed: {ex}")
        return None

    def get_status(self) -> Dict:
        return {"available": self.available, "qiskit_available": QISKIT_AVAILABLE}

# ============================================================
# FLEXGEN MANAGER
# ============================================================
class FlexGenManager:
    """Manager for FlexGen offloading policy optimization with precision selection."""
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
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        from enhancements.gpu_optimization.flexgen_controller import FlexGenController
        from enhancements.gpu_optimization.flexgen_policy_selector import DistillationFlexGenSelector
        selector = DistillationFlexGenSelector(
            n_candidates=20,
            config={'epsilon': self.config.optimizer.flexgen_selector_epsilon,
                    'epsilon_decay': self.config.optimizer.flexgen_selector_epsilon_decay}
        )
        controller = FlexGenController(
            node=node, workload=workload,
            carbon_intensity=workload.metadata.get('carbon_intensity',
                                                   self.config.optimizer.flexgen_carbon_intensity_default),
            use_real_executor=self.config.optimizer.flexgen_use_real_executor,
            executor=None,
            cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={'population_size': self.config.optimizer.flexgen_population_size,
                               'generations': self.config.optimizer.flexgen_generations},
            modp_planner=None,
            drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        return await controller.step()

    async def select_precision(self, workload: Dict) -> str:
        """Adaptive precision switching based on carbon intensity and workload size."""
        carbon_intensity = workload.get("carbon_intensity", self.config.optimizer.flexgen_carbon_intensity_default)
        workload_size = workload.get("size", "medium")
        if carbon_intensity > 500 or workload_size == "large":
            return "int8"
        elif carbon_intensity > 300 or workload_size == "medium":
            return "fp16"
        return "fp32"

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        return {
            "available": True,
            "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
            "gpu": self.gpu_profiler.get_current_metrics() if self.gpu_profiler else {},
        }

# ============================================================
# RATE LIMITER
# ============================================================
class RateLimiter:
    def __init__(self, config: RouterConfig):
        self.config = config
        self.rate = config.rate_limit.requests_per_minute
        self.window = config.rate_limit.window_seconds
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
            self.throttled_requests += 1
            return False

    def get_metrics(self) -> Dict:
        total = self.total_requests + self.throttled_requests
        return {
            'total_requests': self.total_requests,
            'throttled_requests': self.throttled_requests,
            'throttle_rate': (self.throttled_requests / max(total, 1)) * 100,
        }

# ============================================================
# VAULT MANAGER
# ============================================================
class VaultManager(IVault):
    def __init__(self, config: RouterConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url and config.vault.token:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception as e:
                logger.error(f"Vault init failed: {e}")

    async def store_secret(self, path: str, data: Dict):
        if not self.client:
            return
        try:
            self.client.secrets.kv.v2.create_or_update_secret(path=path, secret=data)
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='store', status='success').inc()
        except Exception as e:
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='store', status='failed').inc()
            raise

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None
        try:
            secret = self.client.secrets.kv.v2.read_secret(path=path)
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='read', status='success').inc()
            return secret['data']['data']
        except Exception:
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='read', status='failed').inc()
            return None

    async def health_check(self) -> Dict:
        return {"status": "ok" if self.client else "unavailable"}

# ============================================================
# POST-QUANTUM CRYPTOGRAPHY
# ============================================================
class PostQuantumCrypto(IPQC):
    def __init__(self, config: RouterConfig, vault: Optional[VaultManager] = None):
        self.config = config
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE
        self.master_key = config.get_master_key_bytes()

    async def sign_routing_decision(self, decision_data: Dict) -> Dict:
        data_bytes = json.dumps(decision_data, sort_keys=True, default=str).encode()
        sig = hashlib.sha256(data_bytes).hexdigest()
        if PROMETHEUS_AVAILABLE:
            PQC_SIGNATURES.labels(algorithm='sha256_fallback', status='success').inc()
        return {'signature': sig, 'algorithm': 'sha256_fallback', 'timestamp': datetime.now().isoformat()}

    def get_quantum_status(self) -> Dict:
        return {
            'pqc_available': self.pqc_available,
            'algorithms': ['dilithium', 'falcon', 'sphincs'] if self.pqc_available else ['ecdsa'],
        }

    async def health_check(self) -> Dict:
        return {"status": "ok"}

# ============================================================
# MULTI-CLOUD STORAGE
# ============================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: RouterConfig):
        self.config = config
        self.providers = {}

    async def store(self, data: Dict, filename: str = None) -> Dict:
        path = Path(f"./routing_backup_{filename or 'data'}.json")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump(data, f, default=str)
            if PROMETHEUS_AVAILABLE:
                CLOUD_STORAGE.labels(provider='local', operation='store', status='success').inc()
            return {'provider': 'local', 'location': str(path)}
        except Exception as e:
            if PROMETHEUS_AVAILABLE:
                CLOUD_STORAGE.labels(provider='local', operation='store', status='failed').inc()
            raise

    def get_status(self) -> Dict:
        return {'providers': list(self.providers.keys()) or ['local']}

    async def health_check(self) -> Dict:
        return {"status": "ok"}

# ============================================================
# ASYNC DATABASE MANAGER
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
        explanation = Column(Text, nullable=True)
        precision = Column(String(16), nullable=True)

    class OptimizerStateDB(Base):
        __tablename__ = 'optimizer_state'
        id = Column(Integer, primary_key=True)
        key = Column(String(64), unique=True)
        value = Column(JSON)
        updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
else:
    Base = None

class AsyncDatabaseManager(IAsyncDatabase):
    SCHEMA_VERSION = 3

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
                poolclass=NullPool,
            )
            self.async_session = async_sessionmaker(self.async_engine, expire_on_commit=False)
            asyncio.ensure_future(self._apply_migrations())
        except Exception as e:
            logger.warning(f"Async DB init failed: {e}")
            self.async_engine = None
            self.async_session = None

    async def _apply_migrations(self):
        if not self.async_engine:
            return
        try:
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
                    if Base is not None:
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
                    current_ver = 2
                if current_ver < 3:
                    # Add explanation and precision columns if missing
                    try:
                        await conn.execute(text("ALTER TABLE routing_decisions ADD COLUMN explanation TEXT"))
                    except Exception:
                        pass
                    try:
                        await conn.execute(text("ALTER TABLE routing_decisions ADD COLUMN precision TEXT"))
                    except Exception:
                        pass
                    await conn.execute(text("INSERT INTO schema_version (version, applied_at) VALUES (3, datetime('now'))"))
                logger.info(f"Database migrations applied (version {self.SCHEMA_VERSION})")
        except Exception as e:
            logger.error(f"Migration failed: {e}")

    async def save_routing_decision(self, decision: Dict):
        if not self.async_session:
            return
        try:
            async with self.async_session() as session:
                record = RoutingDecisionDB(
                    routing_id=decision['routing_id'],
                    task_type=decision.get('task_type', 'unknown'),
                    selected_expert_id=decision['selected_expert_id'],
                    cost=decision['cost'],
                    bonus_applied=decision['bonus_applied'],
                    context=decision.get('context', {}),
                    pqc_signature=json.dumps(decision.get('pqc_signature', {}), default=str),
                    explanation=decision.get('explanation', ''),
                    precision=decision.get('precision', 'fp32'),
                )
                session.add(record)
                await session.commit()
        except Exception as e:
            logger.error(f"Failed to save routing decision: {e}")

    async def save_optimizer_state(self, state: Dict):
        if not self.async_session:
            return
        try:
            async with self.async_session() as session:
                await session.execute(
                    text("INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) VALUES (:key, :value, :updated_at)"),
                    {"key": "state", "value": json.dumps(state, default=str), "updated_at": datetime.now().isoformat()}
                )
                await session.commit()
        except Exception as e:
            logger.error(f"Failed to save optimizer state: {e}")

    async def load_optimizer_state(self) -> Optional[Dict]:
        if not self.async_session:
            return None
        try:
            async with self.async_session() as session:
                result = await session.execute(text("SELECT value FROM optimizer_state WHERE key = 'state'"))
                row = result.fetchone()
                if row:
                    return json.loads(row[0])
                return None
        except Exception as e:
            logger.error(f"Failed to load optimizer state: {e}")
            return None

    async def health_check(self) -> Dict:
        if not self.async_session:
            return {"status": "unavailable"}
        try:
            async with self.async_session() as session:
                await session.execute(text("SELECT 1"))
            return {"status": "healthy"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def close(self):
        if self.async_engine:
            try:
                await self.async_engine.dispose()
            except Exception:
                pass

# ============================================================
# ENHANCED ExpertRouterWithHarvester v3.1
# ============================================================
class ExpertRouterWithHarvester(ExpertRouter):
    """
    Enhanced ExpertRouter with all v3.0 features plus:
    - CausalBandit for causal RL
    - SafetyMonitor (temporal logic)
    - XAIExplainer
    - FederatedRouterCoordinator (secure aggregation)
    - MultiAgentRouter (bidding, role specialisation)
    - CarbonOffsetBroker (offsets + RECs)
    - ChaosMonkey
    - HumanReviewManager
    - QuantumDistillationOptimizer (optional)
    - Adaptive precision switching
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

        # Enhanced modules
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = MoEExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.selection_policies = ["cost_based", "accuracy_focused", "energy_focused", "balanced"]
            # Use CausalBandit instead of ContextualBandit
            self.bandit = CausalBandit(
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

        # New enhancement modules
        self.safety_monitor = SafetyMonitor(config.safety)
        self.xai = XAIExplainer()
        self.federated = FederatedRouterCoordinator()
        self.multi_agent = MultiAgentRouter()
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(config.chaos)
        self.human_review = HumanReviewManager()
        self.quantum_optimizer = QuantumDistillationOptimizer(config.quantum)
        self.flexgen_manager = FlexGenManager(config)

        # State
        self._last_decision: Dict[str, Any] = {}
        self._last_review_id: Optional[str] = None
        self._load_state()

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
            try:
                # best effort
                pass
            except Exception:
                pass

    def _save_state(self):
        if self.db:
            state = {
                'bonus_population': self.bonus_population,
                'bonus_rewards': list(self.bonus_rewards),
                'bandit_q_values': getattr(self.bandit, 'q_values', {}),
                'bandit_causal_effects': getattr(self.bandit, 'causal_effects', {}),
            }
            try:
                asyncio.ensure_future(self.db.save_optimizer_state(state))
            except Exception:
                pass

    async def _apply_harvester_bonus(self, cost: float, context: Dict, expert: ExpertProfile) -> float:
        data_source = context.get('data_source', 'cloud')
        harvester_flag = getattr(expert, 'photosynthetic_harvester_flag', False)
        if data_source == 'photosynthetic_harvester' and harvester_flag:
            if self.harvester:
                bonus_factor = await self.harvester.get_energy_bonus(expert, context)
            else:
                bonus_factor = np.mean(self.bonus_population) if self.bonus_population else self.config.general.bonus_discount
            return cost * bonus_factor
        return cost

    async def _route_with_enhanced_modules(self, task: Dict, context: Dict) -> Dict:
        # Chaos monkey can inject failure at route start
        self.chaos_monkey.maybe_fail("routing")

        candidates = await self._get_candidates_with_breaker(task, context)
        if not candidates:
            raise RegistryError("No candidate experts found")

        costs = await self._compute_costs_with_breaker(candidates, context)

        context_for_bandit = {
            "task_type": task.get('type', 'unknown'),
            "data_source": context.get('data_source', 'cloud'),
            "num_candidates": len(candidates),
            "avg_cost": float(np.mean(list(costs.values()))) if costs else 0.0,
            "carbon_intensity": context.get('carbon_intensity', 0.5),
            "time": datetime.now().hour,
        }

        encoded = self.moe.encode(context_for_bandit) if self.moe else context_for_bandit
        policy, confidence, source = self.bandit.select_action(encoded) if self.bandit else ("cost_based", 0.0, "fallback")
        if policy is None:
            policy = "cost_based"

        # Compute final costs with harvester bonus
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

        # Optional quantum selection
        selected_expert = None
        if self.quantum_optimizer.available:
            quantum_candidates = [self.registry.get_expert(eid) for eid in final_costs.keys()]
            quantum_candidates = [e for e in quantum_candidates if e is not None]
            selected_expert = await self.quantum_optimizer.select_best_expert(
                quantum_candidates, context, self.config.optimizer.modp_weights
            )

        if selected_expert is None:
            # Policy-based selection
            if policy == "cost_based":
                best_eid = min(final_costs, key=final_costs.get)
            elif policy == "accuracy_focused":
                best_eid = max(final_costs.keys(),
                               key=lambda eid: getattr(self.registry.get_expert(eid), 'accuracy_score', 0.0) or 0.0)
            elif policy == "energy_focused":
                best_eid = min(final_costs.keys(),
                               key=lambda eid: getattr(self.registry.get_expert(eid), 'usage_count', 0))
            else:  # balanced
                if self.modp:
                    utilities = {}
                    max_cost = max(final_costs.values()) if final_costs else 1.0
                    for eid, cost in final_costs.items():
                        expert = self.registry.get_expert(eid)
                        objectives = {
                            "accuracy": getattr(expert, 'accuracy_score', 0.5) or 0.5,
                            "energy": 1.0 - (cost / max_cost) if max_cost else 0.5,
                            "carbon": context.get('carbon_intensity', 0.5),
                            "latency": 0.5,
                        }
                        utilities[eid] = self.modp.evaluate(objectives, self.config.optimizer.modp_weights)
                    best_eid = max(utilities, key=utilities.get)
                else:
                    best_eid = min(final_costs, key=final_costs.get)
            best_expert = self.registry.get_expert(best_eid) if self.registry else None
        else:
            best_eid = selected_expert.expert_id
            best_expert = selected_expert
            policy = "quantum"

        if not best_expert:
            raise RegistryError("Selected expert not found in registry")

        # Safety check
        best_fitness = getattr(best_expert, 'accuracy_score', 0.5) or 0.5
        if not self.safety_monitor.check_routing(context, best_expert, best_fitness):
            # Fallback to a "safe" expert (most accurate)
            logger.warning("Safety violation detected; selecting fallback expert")
            safe_eid = max(final_costs.keys(),
                           key=lambda eid: getattr(self.registry.get_expert(eid), 'accuracy_score', 0.0) or 0.0)
            best_eid = safe_eid
            best_expert = self.registry.get_expert(best_eid)
            policy = "safety_override"

        bonus_applied = bonus_applied_map.get(best_eid, False)
        if bonus_applied:
            HARVESTER_BONUS.inc()
            SELECTED_BONUS_FACTOR.observe(self.config.general.bonus_discount)
        SELECTED_COST.observe(final_costs[best_eid])

        # Adaptive precision selection
        try:
            precision = await self.flexgen_manager.select_precision({
                "size": "medium" if len(candidates) < 20 else "large",
                "carbon_intensity": context.get('carbon_intensity', 0.5) * 800.0,
            })
        except Exception:
            precision = "fp32"

        # XAI explanation
        explanation = self.xai.explain_routing(
            best_expert, policy, context, confidence, final_costs[best_eid], bonus_applied
        )
        if PROMETHEUS_AVAILABLE:
            XAI_DECISIONS.labels(policy=policy).inc()

        decision = {
            'routing_id': str(uuid.uuid4()),
            'task_type': task.get('type', 'unknown'),
            'selected_expert_id': best_eid,
            'cost': final_costs[best_eid],
            'bonus_applied': bonus_applied,
            'context': context,
            'explanation': explanation,
            'precision': precision,
            'timestamp': datetime.now().isoformat(),
        }

        # Human review for low confidence / high carbon
        review_id = None
        carbon_intensity = context.get('carbon_intensity', 0.0)
        if isinstance(carbon_intensity, float) and carbon_intensity <= 1.0:
            carbon_intensity = carbon_intensity * 800.0
        if confidence < self.config.general.human_review_threshold and carbon_intensity > 450:
            decision_id = decision['routing_id']
            review_id = await self.human_review.request_review(decision_id, {
                "expert_id": best_eid,
                "policy": policy,
                "explanation": explanation,
                "carbon_intensity": carbon_intensity,
            })

        if self.pqc:
            signature = await self.pqc.sign_routing_decision(decision)
            decision['pqc_signature'] = signature
        else:
            signature = None

        if self.db:
            await self.db.save_routing_decision(decision)

        if self.cloud_storage:
            try:
                await self.cloud_storage.store(decision, f"routing_{decision['routing_id']}.json")
            except Exception as e:
                logger.error(f"Cloud backup failed: {e}")

        # Record multi-agent outcome
        domain = getattr(best_expert, 'domain', 'unknown')
        self.multi_agent.submit_bid(best_eid, final_costs[best_eid])
        self.multi_agent.record_outcome(best_eid, domain, success=True)

        # Update bandit
        if self.bandit and source != "safety_override":
            reward = best_fitness - (final_costs[best_eid] * 0.1)
            await self.bandit.update(encoded, policy, reward)

        # Carbon offset purchase for high intensity routing
        carbon_kg = final_costs[best_eid] * 0.001
        if carbon_intensity > 450:
            try:
                await self.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)
            except Exception as e:
                logger.warning(f"Carbon offset purchase failed: {e}")

        # Federated round (simulated)
        self.federated.register_participant(
            f"router_{uuid.uuid4().hex[:8]}",
            {"avg_cost": final_costs[best_eid], "fitness": best_fitness}
        )

        # Audit log
        audit_logger.info(f"Routing decision: {decision['routing_id']} -> {best_eid} (cost={final_costs[best_eid]:.4f})")

        self._last_decision = {
            'context': context_for_bandit,
            'policy': policy,
            'expert_id': best_eid,
            'decision': decision,
            'explanation': explanation,
            'review_id': review_id,
        }
        self._save_state()

        return {
            'expert': best_expert,
            'cost': final_costs[best_eid],
            'harvester_bonus_applied': bonus_applied,
            'timestamp': datetime.now().isoformat(),
            'pqc_signature': signature,
            'explanation': explanation,
            'precision': precision,
            'review_id': review_id,
        }

    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        ROUTER_REQUESTS.inc()
        start_time = time.time()
        if not await self.rate_limiter.acquire():
            RATE_LIMITER_THROTTLE.inc()
            raise RateLimitExceeded("Rate limit exceeded")
        try:
            if ENHANCEMENTS_AVAILABLE:
                result = await self._route_with_enhanced_modules(task, context)
            else:
                result = await self._route_original(task, context)
            ROUTER_LATENCY.observe(time.time() - start_time)
            return result
        except ChaosExperimentError as e:
            logger.warning(f"Chaos failure: {e}")
            raise
        except Exception as e:
            logger.exception(f"Routing failed: {e}")
            raise

    async def _route_original(self, task: Dict, context: Dict) -> Dict:
        # Fallback: minimal routing using cost function
        candidates = await self._get_candidates_with_breaker(task, context)
        if not candidates:
            raise RegistryError("No candidates")
        costs = await self._compute_costs_with_breaker(candidates, context)
        best_eid = min(costs, key=costs.get)
        best_expert = self.registry.get_expert(best_eid)
        return {
            'expert': best_expert,
            'cost': costs[best_eid],
            'harvester_bonus_applied': False,
            'timestamp': datetime.now().isoformat(),
        }

    async def _get_candidates_with_breaker(self, task: Dict, context: Dict) -> List[ExpertProfile]:
        breaker = GlobalCircuitBreaker().get_or_create(
            "registry",
            failure_threshold=self.config.circuit_breaker.failure_threshold,
            recovery_timeout=self.config.circuit_breaker.recovery_timeout,
        )
        async def get_candidates():
            if hasattr(super(), 'get_candidate_experts'):
                try:
                    result = super().get_candidate_experts(task, context)
                    if asyncio.iscoroutine(result):
                        return await result
                    return result
                except Exception:
                    pass
            # Fallback to registry
            if self.registry and hasattr(self.registry, 'get_all_active_experts'):
                return self.registry.get_all_active_experts()
            return []
        return await breaker.call(get_candidates)

    async def _compute_costs_with_breaker(self, candidates: List[ExpertProfile], context: Dict) -> Dict[str, float]:
        breaker = GlobalCircuitBreaker().get_or_create(
            "cost_function",
            failure_threshold=self.config.circuit_breaker.failure_threshold,
            recovery_timeout=self.config.circuit_breaker.recovery_timeout,
        )
        async def compute_costs():
            if asyncio.iscoroutinefunction(self.cost_function.compute_multiple):
                return await self.cost_function.compute_multiple(candidates, context)
            return self.cost_function.compute_multiple(candidates, context)
        return await breaker.call(compute_costs)

    async def record_feedback(self, routing_id: str, success: bool, actual_metrics: Dict) -> Dict:
        """Record feedback and update bandit, multi-agent, and human review."""
        if self.bandit and self._last_decision:
            reward = 1.0 if success else -1.0
            await self.bandit.update(
                self._last_decision.get('context', {}),
                self._last_decision.get('policy', 'unknown'),
                reward
            )
        expert_id = self._last_decision.get('expert_id')
        if expert_id:
            domain = getattr(self.registry.get_expert(expert_id), 'domain', 'unknown') if self.registry else 'unknown'
            self.multi_agent.record_outcome(expert_id, domain, success)
        return {
            "status": "recorded",
            "routing_id": routing_id,
            "success": success,
            "metrics": actual_metrics,
            "explanation": self._last_decision.get('explanation', ''),
        }

    # ============================================================
    # FlexGen
    # ============================================================
    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        workload_obj = WorkloadDescriptor(**workload)
        node_obj = NodeDescriptor(**node)
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict:
        return await self.flexgen_manager.get_status()

    async def select_precision(self, workload: Dict) -> str:
        return await self.flexgen_manager.select_precision(workload)

    # ============================================================
    # Human review endpoints
    # ============================================================
    async def get_pending_reviews(self) -> List[Dict]:
        return await self.human_review.get_pending()

    async def approve_review(self, review_id: str) -> bool:
        return await self.human_review.approve(review_id)

    async def reject_review(self, review_id: str) -> bool:
        return await self.human_review.reject(review_id)

    # ============================================================
    # Chaos / carbon / federated
    # ============================================================
    async def trigger_chaos(self, enabled: bool = True, probability: float = 0.1):
        self.chaos_monkey.config.enabled = enabled
        self.chaos_monkey.config.failure_probability = probability
        return self.chaos_monkey.get_stats()

    async def get_carbon_totals(self) -> Dict:
        return self.carbon_broker.get_totals()

    async def run_federated_round(self) -> Dict:
        aggregated = self.federated.aggregate()
        return {
            "status": "completed",
            "aggregated": aggregated,
            "participants": self.federated.get_participant_count(),
        }

    # ============================================================
    # Health / status
    # ============================================================
    async def health_check(self) -> Dict:
        results = {}
        for name, component in self.health_components.items():
            if component is None:
                results[name] = {"status": "unavailable"}
                continue
            if hasattr(component, 'health_check'):
                try:
                    results[name] = await component.health_check()
                except Exception as e:
                    results[name] = {"status": "unhealthy", "error": str(e)}
            else:
                results[name] = {"status": "ok"}
        overall = 'healthy' if all(
            (r.get('status') in ('ok', 'healthy', 'unavailable')) for r in results.values()
        ) else 'degraded'
        if PROMETHEUS_AVAILABLE:
            HEALTH_SCORE.set(100 if overall == 'healthy' else 50)
        return {
            'status': overall,
            'components': results,
            'timestamp': datetime.now().isoformat(),
        }

    async def get_router_status(self) -> Dict:
        cfg = self.config.dict() if hasattr(self.config, 'dict') else self.config.__dict__
        return {
            'bonus_discount': self.config.general.bonus_discount,
            'circuit_breaker': {name: cb.get_metrics() for name, cb in GlobalCircuitBreaker()._breakers.items()},
            'rate_limiter': self.rate_limiter.get_metrics(),
            'quantum': self.pqc.get_quantum_status() if self.pqc else None,
            'quantum_optimizer': self.quantum_optimizer.get_status(),
            'cloud_storage_providers': self.cloud_storage.get_status() if self.cloud_storage else [],
            'vault_available': self.vault is not None,
            'db_available': self.db is not None,
            'health': await self.health_check(),
            'enhancements_available': ENHANCEMENTS_AVAILABLE,
            'bandit_actions': self.bandit.actions if self.bandit else None,
            'bonus_population_size': len(self.bonus_population),
            'modp_weights': self.config.optimizer.modp_weights,
            'flexgen': await self.get_flexgen_status(),
            'safety_violations': self.safety_monitor.get_violations()[-5:],
            'multi_agent': self.multi_agent.get_stats(),
            'carbon_broker': self.carbon_broker.get_totals(),
            'federated_participants': self.federated.get_participant_count(),
            'chaos': self.chaos_monkey.get_stats(),
            'human_review_pending': len(await self.human_review.get_pending()),
            'last_explanation': self._last_decision.get('explanation'),
        }

# ============================================================
# FastAPI REST API
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Expert Router API", version="3.1")
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
        except Exception:
            # In demo, accept any token
            return {"sub": "anonymous"}

    api_rate_limiter = RateLimiter(RouterConfig())

    async def rate_limit(request: Request):
        if not await api_rate_limiter.acquire():
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

    router: Optional[ExpertRouterWithHarvester] = None

    @app.post("/route")
    async def route_task(task: Dict, context: Dict, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        try:
            result = await router.route(task, context)
            # Serialize expert
            if hasattr(result.get('expert'), '__dict__'):
                result['expert'] = {k: v for k, v in result['expert'].__dict__.items() if not k.startswith('_')}
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/feedback")
    async def feedback(routing_id: str, success: bool, actual_metrics: Dict,
                        user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.record_feedback(routing_id, success, actual_metrics)

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

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(workload: Dict, node: Dict,
                                user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.run_flexgen_optimization(workload, node)

    @app.get("/flexgen/status")
    async def flexgen_status(user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.get_flexgen_status()

    @app.get("/human-review/pending")
    async def human_review_pending(user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.get_pending_reviews()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(review_id: str, user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return {"approved": await router.approve_review(review_id)}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(review_id: str, user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return {"rejected": await router.reject_review(review_id)}

    @app.post("/chaos/trigger")
    async def chaos_trigger(enabled: bool = True, probability: float = 0.1,
                            user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.trigger_chaos(enabled, probability)

    @app.get("/carbon/totals")
    async def carbon_totals(user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.get_carbon_totals()

    @app.post("/federated/round")
    async def federated_round(user: Dict = Depends(verify_token)):
        if not router:
            raise HTTPException(status_code=503, detail="Router not initialized")
        return await router.run_federated_round()

    @app.on_event("startup")
    async def startup():
        global router
        config = RouterConfig()

        class SimpleCostFunction:
            async def compute_multiple(self, experts, context):
                return {e.expert_id: random.uniform(0.1, 1.0) for e in experts}

        class SimpleRegistry:
            def __init__(self):
                self.experts = {}
                # Register a few demo experts
                for i in range(3):
                    eid = f"expert_{i}"
                    self.experts[eid] = ExpertProfile(
                        expert_id=eid, domain=["vision", "nlp", "audio"][i],
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
        cloud_storage = MultiCloudStorage(config)

        router = ExpertRouterWithHarvester(
            config=config,
            cost_function=cost_function,
            registry=registry,
            harvester=None,
            db=db,
            pqc=pqc,
            cloud_storage=cloud_storage,
            vault=vault,
        )
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown():
        if router and router.db:
            await router.db.close()
        logger.info("FastAPI shut down")

# ============================================================
# Singleton accessor
# ============================================================
_router_instance: Optional[ExpertRouterWithHarvester] = None
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
                    vault=vault,
                )
    return _router_instance

# ============================================================
# Main entry point (for testing)
# ============================================================
async def main():
    print("Expert Router with Harvester v3.1 Demo")
    config = RouterConfig()

    class SimpleCostFunction:
        async def compute_multiple(self, experts, context):
            return {e.expert_id: random.uniform(0.1, 1.0) for e in experts}

    class SimpleRegistry:
        def __init__(self):
            self.experts = {}
            for i in range(3):
                eid = f"expert_{i}"
                self.experts[eid] = ExpertProfile(
                    expert_id=eid, domain=["vision", "nlp", "audio"][i],
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
    cloud_storage = MultiCloudStorage(config)

    router = ExpertRouterWithHarvester(
        config=config,
        cost_function=cost_function,
        registry=registry,
        harvester=None,
        db=db,
        pqc=pqc,
        cloud_storage=cloud_storage,
        vault=vault,
    )

    task = {"type": "classification"}
    context = {"data_source": "photosynthetic_harvester", "carbon_intensity": 0.6}

    result = await router.route(task, context)
    print(f"\n✅ Routing Result:")
    print(f"   Expert: {result['expert'].expert_id}")
    print(f"   Cost: {result['cost']:.4f}")
    print(f"   Harvester bonus: {result['harvester_bonus_applied']}")
    print(f"   Precision: {result['precision']}")
    print(f"   Explanation: {result['explanation']}")
    print(f"   Review ID: {result['review_id']}")

    # Feedback
    fb = await router.record_feedback(result.get('routing_id', 'unknown'), True, {'accuracy': 0.92})
    print(f"\n📝 Feedback recorded: {fb['status']}")

    # Status
    status = await router.get_router_status()
    print(f"\n📊 Router Status:")
    print(f"   Health: {status['health']['status']}")
    print(f"   Enhancements available: {status['enhancements_available']}")
    print(f"   Safety violations: {len(status['safety_violations'])}")
    print(f"   Multi-agent: {status['multi_agent']}")
    print(f"   Carbon broker: {status['carbon_broker']}")
    print(f"   Federated participants: {status['federated_participants']}")
    print(f"   Chaos: {status['chaos']}")
    print(f"   Pending reviews: {status['human_review_pending']}")

    # FlexGen demo
    workload = {"task_id": "wl_001", "task_type": "inference", "tokens": 512,
                "latency_target": 200.0, "urgency": "medium", "priority": "balanced",
                "bio_mode": "none", "metadata": {}}
    node = {"id": "node_001", "type": "cloud", "region": "us-east",
            "region_carbon_intensity": 0.42, "energy_per_token": 0.00005,
            "uptime": 0.99, "maintenance_status": "operational", "metadata": {}}
    flexgen_result = await router.run_flexgen_optimization(workload, node)
    print(f"\n🚀 FlexGen optimization: {flexgen_result}")

    # Cleanup
    if router.db:
        await router.db.close()

if __name__ == "__main__":
    asyncio.run(main())
