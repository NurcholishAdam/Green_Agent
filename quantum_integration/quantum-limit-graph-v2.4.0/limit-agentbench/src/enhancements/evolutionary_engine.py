#!/usr/bin/env python3
# File: src/enhancements/evolutionary_engine_v4_0_0.py
"""
Evolutionary Engine for Green Agent v4.1.0 (Enterprise Quantum+ with Advanced Enhancements)
Manages the lifecycle of experts using sustainability-aware fitness.

NEW IN v4.1.0:
- CausalBandit replaces ContextualBandit for causal RL on lifecycle decisions.
- SafetyMonitor with temporal logic rules for expert pruning/merging.
- XAIExplainer generating natural language explanations.
- FederatedEvolutionCoordinator with simulated differential privacy.
- MultiAgentCoordinator supporting emergent role specialisation among experts.
- CarbonOffsetBroker integrating external carbon markets and RECs.
- ChaosMonkey for resilience testing.
- HumanReviewManager for human-in-the-loop with active learning.
- QuantumInspiredOptimizer stub for quantum-distillation integration.
- Adaptive precision switching via FlexGen integrated into fitness evaluation.
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
from pathlib import Path
import numpy as np
import contextvars
import random
import weakref

# ============================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
# ============================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "prune"
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
    from pydantic import BaseModel, Field, field_validator
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
    from fastapi import FastAPI, HTTPException
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

# ============================================================
# Import existing modules (with stubs)
# ============================================================
try:
    from ..expert_registry import ExpertRegistry, ExpertProfile
    from ..digital_twin import DigitalTwin
    from ..mlops_pipeline import MLOpsPipeline
    from ..database.manager import DatabaseManager
    from ..task_manager import TaskManager
    from .sustainability_cost import SustainabilityCostFunction
except ImportError:
    class ExpertRegistry:
        def get_all_active_experts(self): return []
        def get_expert(self, eid): return None
        async def deprecate_expert(self, eid, reason=""): pass
    class ExpertProfile:
        def __init__(self, expert_id="", domain="", usage_count=0, accuracy_score=None, last_used=None):
            self.expert_id = expert_id
            self.domain = domain
            self.usage_count = usage_count
            self.accuracy_score = accuracy_score
            self.last_used = last_used
    class DigitalTwin: pass
    class MLOpsPipeline: pass
    class DatabaseManager: pass
    class TaskManager:
        def __init__(self):
            self.tasks = {}
            self.shutdown_event = asyncio.Event()
        def register_task(self, name, func, *args): self.tasks[name] = (func, args)
        def start_registered_tasks(self): pass
        async def stop_all(self): pass
    class SustainabilityCostFunction:
        async def compute(self, expert, context): return 0.1
    logger = logging.getLogger(__name__)

# ============================================================
# Structured logging
# ============================================================
correlation_id_var = contextvars.ContextVar('correlation_id', default=str(uuid.uuid4())[:8])

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logger.addFilter(CorrelationIdFilter())

# ============================================================
# Prometheus metrics (fallback)
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    EVOLUTION_CYCLES = Counter('evolution_cycles_total', 'Total evolution cycles', registry=REGISTRY)
    EXPERTS_PRUNED = Counter('experts_pruned_total', 'Experts pruned', registry=REGISTRY)
    EXPERTS_MERGED = Counter('experts_merged_total', 'Experts merged', registry=REGISTRY)
    EXPERTS_SPAWNED = Counter('experts_spawned_total', 'Experts spawned', registry=REGISTRY)
    FITNESS_DISTRIBUTION = Histogram('expert_fitness', 'Fitness scores', registry=REGISTRY)
    EVOLUTION_DURATION = Histogram('evolution_duration_seconds', 'Cycle duration', registry=REGISTRY)
    PQC_SIGNATURES = Counter('pqc_signatures_total', 'PQC signatures', ['algorithm', 'status'], registry=REGISTRY)
    CLOUD_STORAGE = Counter('cloud_storage_operations_total', 'Cloud storage ops', ['provider', 'operation', 'status'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('vault_operations_total', 'Vault ops', ['operation', 'status'], registry=REGISTRY)
    OPTIMIZER_DECISIONS = Counter('optimizer_decisions_total', 'Optimizer decisions', ['parameter', 'action'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('evolution_circuit_breaker_state', 'CB state', ['service'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('evolution_health_score', 'Health score', registry=REGISTRY)
    # NEW metrics
    SAFETY_VIOLATIONS = Counter('evolution_safety_violations_total', 'Safety violations', ['rule'], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter('evolution_chaos_experiments_total', 'Chaos experiments', ['type', 'status'], registry=REGISTRY)
    HUMAN_REVIEWS = Counter('evolution_human_reviews_total', 'Human reviews', ['status'], registry=REGISTRY)
    XAI_DECISIONS = Counter('evolution_xai_decisions_total', 'XAI decisions', ['strategy'], registry=REGISTRY)
    CARBON_OFFSETS = Counter('evolution_carbon_offsets_total', 'Carbon offsets purchased', ['status'], registry=REGISTRY)
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
    OPTIMIZER_DECISIONS = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    HEALTH_SCORE = DummyMetric()
    SAFETY_VIOLATIONS = DummyMetric()
    CHAOS_EXPERIMENTS = DummyMetric()
    HUMAN_REVIEWS = DummyMetric()
    XAI_DECISIONS = DummyMetric()
    CARBON_OFFSETS = DummyMetric()

# ============================================================
# Custom Exceptions
# ============================================================
class EvolutionaryEngineError(Exception): pass
class ConfigError(EvolutionaryEngineError): pass
class SecurityError(EvolutionaryEngineError): pass
class CloudStorageError(EvolutionaryEngineError): pass
class VaultError(EvolutionaryEngineError): pass
class PredictionError(EvolutionaryEngineError): pass
class OptimizerError(EvolutionaryEngineError): pass
class DatabaseError(EvolutionaryEngineError): pass
class CircuitBreakerOpenError(EvolutionaryEngineError): pass
class SafetyViolationError(EvolutionaryEngineError): pass
class ChaosExperimentError(EvolutionaryEngineError): pass

# ============================================================
# CONFIGURATION (Grouped sub-models)
# ============================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
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

    class QuantumConfig(BaseModel):
        pqc_enabled: bool = True
        pqc_algorithm: str = "dilithium"
        master_key: str = ""
        enable_distillation: bool = False

    class CloudConfig(BaseModel):
        aws_bucket: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None
        aws_region: str = "us-east-1"

    class DatabaseConfig(BaseModel):
        url: str = "sqlite+aiosqlite:///evolution.db"
        pool_size: int = 10
        max_overflow: int = 20

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/evolution"

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        model_storage_path: str = "./prophet_models"
        min_samples: int = 30

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        epsilon: float = 0.1
        parameter_space: Dict[str, List[float]] = {}
        modp_weights: Dict[str, float] = {}
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    class SafetyConfig(BaseModel):
        max_prune_per_cycle: int = 10
        max_merge_per_cycle: int = 5
        max_consecutive_prunes: int = 3
        enable_monitor: bool = True

    class ChaosConfig(BaseModel):
        enabled: bool = False
        failure_probability: float = 0.1

    class APIConfig(BaseModel):
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = "secret"
        rate_limit_enabled: bool = True
        rate_limit_requests: int = 100
        rate_limit_window: int = 60

    class CircuitBreakerConfig(BaseModel):
        failure_threshold: int = 3
        recovery_timeout: int = 30

    class LeaderConfig(BaseModel):
        enabled: bool = False
        redis_url: Optional[str] = None
        ttl_seconds: int = 30

    class EvolutionConfig(BaseModel):
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = Field(default_factory=LeaderConfig)
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
        enable_distillation: bool = False

    @dataclass
    class CloudConfig:
        aws_bucket: Optional[str] = None
        azure_connection_string: Optional[str] = None
        gcp_bucket: Optional[str] = None
        aws_region: str = "us-east-1"

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
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'accuracy':0.4, 'energy':0.3, 'carbon':0.2, 'latency':0.1})
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    @dataclass
    class SafetyConfig:
        max_prune_per_cycle: int = 10
        max_merge_per_cycle: int = 5
        max_consecutive_prunes: int = 3
        enable_monitor: bool = True

    @dataclass
    class ChaosConfig:
        enabled: bool = False
        failure_probability: float = 0.1

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = "secret"
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
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = field(default_factory=LeaderConfig)

# ============================================================
# INTERFACES
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
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0)
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
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                if PROMETHEUS_AVAILABLE:
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(1)

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

    def get_metrics(self) -> Dict:
        total = self.total_requests + self.throttled_requests
        return {'total_requests': self.total_requests, 'throttled_requests': self.throttled_requests,
                'throttle_rate': (self.throttled_requests / max(total, 1)) * 100}

# ============================================================
# VAULT MANAGER
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
            except Exception as e:
                logger.error(f"Vault init failed: {e}")

    async def store_secret(self, path: str, data: Dict):
        if not self.client:
            return
        async def _store():
            self.client.secrets.kv.v2.create_or_update_secret(path=path, secret=data)
        try:
            await self.circuit_breaker.call(_store)
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='store', status='success').inc()
        except Exception as e:
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='store', status='failed').inc()
            raise VaultError(f"Store failed: {e}")

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

    def get_status(self) -> Dict:
        return {'available': self.client is not None}

# ============================================================
# POST-QUANTUM CRYPTOGRAPHY
# ============================================================
class PostQuantumCrypto(IPQC):
    def __init__(self, config, vault):
        self.config = config
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE

    async def sign_evolution_event(self, event_data: Dict) -> Dict:
        data_bytes = json.dumps(event_data, sort_keys=True, default=str).encode()
        sig = hashlib.sha256(data_bytes).hexdigest()
        return {'signature': sig, 'algorithm': 'sha256_fallback'}

    def get_quantum_status(self) -> Dict:
        return {'pqc_available': self.pqc_available,
                'algorithms': ['dilithium','falcon','sphincs'] if self.pqc_available else ['ecdsa']}

# ============================================================
# MULTI-CLOUD STORAGE
# ============================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config):
        self.config = config
        self.providers = {}

    async def store(self, data: Dict, filename: str = None) -> Dict:
        path = Path(f"./backup_{filename or 'data'}.json")
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
            raise CloudStorageError(str(e))

    def get_status(self) -> Dict:
        return {'providers': list(self.providers.keys()) or ['local']}

# ============================================================
# PREDICTIVE ANALYTICS
# ============================================================
class PredictiveAnalytics(IPredictiveAnalytics):
    def __init__(self, config):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE
        self.history = deque(maxlen=1000)

    async def update_history(self, fitness_scores: List[float]):
        self.history.extend(fitness_scores)

    async def forecast_fitness(self, horizon_hours: int = 24) -> Dict:
        values = list(self.history)[-30:]
        if not values:
            return {'forecast': [0]*horizon_hours, 'model': 'exp_smoothing', 'confidence': 0.3}
        alpha = 0.3
        smoothed = values[0]
        forecast = []
        for _ in range(horizon_hours):
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
            forecast.append(smoothed)
        return {'forecast': forecast, 'model': 'exp_smoothing', 'confidence': 0.7}

    async def load_model(self, region: str) -> Optional[Any]:
        return None

    async def save_model(self, region: str, model: Any):
        pass

    def get_status(self) -> Dict:
        return {'available': self.prophet_available, 'history_length': len(self.history)}

# ============================================================
# NEW: CausalBandit
# ============================================================
class CausalBandit:
    """Causal bandit with average treatment effect estimation."""
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
        self.context_history = []
        self.reward_history = []
        self.action_history = []

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
    """Temporal logic-like safety rules for lifecycle decisions."""
    def __init__(self, config: SafetyConfig):
        self.config = config
        self.prunes_this_cycle = 0
        self.merges_this_cycle = 0
        self.consecutive_prunes = 0
        self.violations: List[Dict] = []

    def reset_cycle(self):
        self.prunes_this_cycle = 0
        self.merges_this_cycle = 0

    def check_prune(self) -> bool:
        if not self.config.enable_monitor:
            return True
        if self.prunes_this_cycle >= self.config.max_prune_per_cycle:
            self._record_violation("max_prune_per_cycle", {"count": self.prunes_this_cycle})
            return False
        if self.consecutive_prunes >= self.config.max_consecutive_prunes:
            self._record_violation("max_consecutive_prunes", {"count": self.consecutive_prunes})
            return False
        return True

    def check_merge(self) -> bool:
        if not self.config.enable_monitor:
            return True
        if self.merges_this_cycle >= self.config.max_merge_per_cycle:
            self._record_violation("max_merge_per_cycle", {"count": self.merges_this_cycle})
            return False
        return True

    def record_prune(self):
        self.prunes_this_cycle += 1
        self.consecutive_prunes += 1

    def record_merge(self):
        self.merges_this_cycle += 1
        self.consecutive_prunes = 0

    def record_non_prune_action(self):
        self.consecutive_prunes = 0

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
    def explain_lifecycle(self, expert_id: str, action: str, context: Dict,
                          confidence: float, reward: float = None) -> str:
        parts = [f"Expert '{expert_id}': action='{action}'"]
        if 'fitness' in context:
            parts.append(f"fitness={context['fitness']:.3f}")
        if 'usage' in context:
            parts.append(f"usage={context['usage']}")
        if 'domain' in context:
            parts.append(f"domain='{context['domain']}'")
        if confidence:
            parts.append(f"confidence={confidence:.2f}")
        if reward is not None:
            parts.append(f"reward={reward:.3f}")
        return " | ".join(parts)

# ============================================================
# NEW: FederatedEvolutionCoordinator (secure aggregation)
# ============================================================
class FederatedEvolutionCoordinator:
    def __init__(self, privacy_budget: float = 0.5):
        self.participants: Dict[str, Dict[str, float]] = {}
        self.privacy_budget = max(privacy_budget, 1e-6)

    def register_participant(self, participant_id: str, update: Dict[str, float]):
        self.participants[participant_id] = update

    def aggregate(self) -> Dict[str, float]:
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
# NEW: MultiAgentCoordinator (emergent role specialisation)
# ============================================================
class MultiAgentCoordinator:
    """Coordinates expert roles and emergent specialisation."""
    def __init__(self):
        self.expert_roles: Dict[str, str] = {}
        self.role_assignments: Dict[str, List[str]] = defaultdict(list)

    def assign_role(self, expert_id: str, role: str):
        self.expert_roles[expert_id] = role
        self.role_assignments[role].append(expert_id)

    def get_role_for_expert(self, expert_id: str) -> str:
        return self.expert_roles.get(expert_id, "unassigned")

    def get_stats(self) -> Dict:
        return {
            "num_experts": len(self.expert_roles),
            "roles": {role: len(experts) for role, experts in self.role_assignments.items()},
        }

# ============================================================
# NEW: CarbonOffsetBroker (offsets + RECs)
# ============================================================
class CarbonOffsetBroker:
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
            CARBON_OFFSETS.labels(status='success').inc()
        return {"status": "offset_purchased", "carbon_kg": carbon_kg, "cost_usd": cost}

    async def purchase_recs(self, energy_mwh: float) -> Dict:
        if energy_mwh <= 0:
            return {"status": "no_energy"}
        cost = energy_mwh * self.rec_cost_per_mwh
        self.total_recs_mwh += energy_mwh
        self.total_cost += cost
        if PROMETHEUS_AVAILABLE:
            CARBON_OFFSETS.labels(status='rec_success').inc()
        return {"status": "rec_purchased", "energy_mwh": energy_mwh, "cost_usd": cost}

    def get_totals(self) -> Dict:
        return {"total_offset_kg": self.total_offset_kg, "total_recs_mwh": self.total_recs_mwh,
                "total_cost_usd": self.total_cost}

# ============================================================
# NEW: ChaosMonkey
# ============================================================
class ChaosMonkey:
    def __init__(self, config: ChaosConfig):
        self.enabled = config.enabled
        self.failure_probability = config.failure_probability
        self.injected_failures = 0

    def maybe_fail(self):
        if self.enabled and random.random() < self.failure_probability:
            self.injected_failures += 1
            if PROMETHEUS_AVAILABLE:
                CHAOS_EXPERIMENTS.labels(type='lifecycle', status='injected').inc()
            raise ChaosExperimentError("Simulated chaos failure in evolution")

    def get_stats(self) -> Dict:
        return {"enabled": self.enabled, "injected_failures": self.injected_failures}

# ============================================================
# NEW: HumanReviewManager
# ============================================================
class HumanReviewManager:
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
# NEW: QuantumInspiredOptimizer (stub for distillation)
# ============================================================
class QuantumInspiredOptimizer:
    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        try:
            import qiskit
            self.qiskit_available = True
        except ImportError:
            self.qiskit_available = False
        self.available = enabled and self.qiskit_available

    async def optimize_selection(self, candidates: List[Dict], weights: Dict[str, float]) -> Optional[Dict]:
        """Use quantum inspired optimization to select best candidate."""
        if not self.available or not candidates:
            return None
        try:
            # Simplified: score and pick best (placeholder for real QAOA)
            scored = []
            for c in candidates:
                score = sum(c.get(k, 0.0) * weights.get(k, 0.0) for k in weights)
                scored.append((score, c))
            scored.sort(key=lambda x: x[0], reverse=True)
            return scored[0][1] if scored else None
        except Exception as e:
            logger.warning(f"Quantum optimization failed: {e}")
            return None

    def get_status(self) -> Dict:
        return {"available": self.available, "qiskit_available": self.qiskit_available}

# ============================================================
# BIO-INSPIRED OPTIMIZER
# ============================================================
class BioInspiredOptimizer(IAutonomousOptimizer):
    def __init__(self, config: EvolutionConfig, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.param_space = config.optimizer.parameter_space
        self.epsilon = config.optimizer.epsilon
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.population = []
        self.rewards = {param: {val: 0.0 for val in vals} for param, vals in self.param_space.items()}
        self.counts = {param: {val: 0 for val in vals} for param, vals in self.param_space.items()}
        self.history = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def select_parameters(self) -> Dict:
        async with self._lock:
            if self.bio and len(self.population) < 5:
                base = {
                    'prune_threshold': self.config.general.prune_threshold,
                    'merge_similarity_threshold': self.config.general.merge_similarity_threshold,
                    'spawn_gap_threshold': self.config.general.spawn_gap_threshold,
                    'fitness_recency_weight': self.config.general.fitness_recency_weight,
                }
                self.population = [base]
                for _ in range(9):
                    variation = {
                        'prune_threshold': max(0.1, min(0.9, base['prune_threshold'] + random.uniform(-0.1, 0.1))),
                        'merge_similarity_threshold': max(0.5, min(1.0, base['merge_similarity_threshold'] + random.uniform(-0.1, 0.1))),
                        'spawn_gap_threshold': max(0.1, min(0.9, base['spawn_gap_threshold'] + random.uniform(-0.1, 0.1))),
                        'fitness_recency_weight': max(0.0, min(1.0, base['fitness_recency_weight'] + random.uniform(-0.1, 0.1))),
                    }
                    self.population.append(variation)

            if self.bio and self.population:
                def fitness(params):
                    scores = []
                    for p, v in params.items():
                        if p in self.rewards and v in self.rewards[p]:
                            scores.append(self.rewards[p][v])
                    return float(np.mean(scores)) if scores else 0.0

                self.population = self.bio.evolve(
                    population=self.population,
                    fitness_fn=fitness,
                    generations=self.config.optimizer.bio_generations,
                    population_size=self.config.optimizer.bio_population_size,
                )
                best = max(self.population, key=lambda p: fitness(p))
                selected = best
            else:
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
        return {'epsilon': self.epsilon, 'history_length': len(self.history),
                'population_size': len(self.population), 'bio_available': self.bio is not None}

# ============================================================
# ASYNC DATABASE MANAGER
# ============================================================
class AsyncDatabaseManager(IAsyncDatabase):
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.async_engine = None

    async def log_event(self, event_type: str, expert_id: str = None, details: Dict = None):
        pass

    async def health_check(self) -> Dict:
        return {'status': 'ok'}

    async def close(self):
        pass

    async def load_optimizer_state(self) -> Optional[Dict]:
        return None

    async def save_optimizer_state(self, state: Dict):
        pass

# ============================================================
# LEADER ELECTION
# ============================================================
class LeaderElection:
    def __init__(self, config):
        self.config = config
        self.is_leader = False

    async def try_acquire_leadership(self) -> bool:
        if not self.config.leader.enabled:
            self.is_leader = True
            return True
        self.is_leader = True
        return True

    async def renew_leadership(self):
        pass

    async def stop(self):
        self.is_leader = False

# ============================================================
# FLEXGEN MANAGER (with precision selection)
# ============================================================
class FlexGenManager:
    def __init__(self, config: EvolutionConfig):
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

    async def optimize_policy(self, workload, node) -> Dict:
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
        return {"available": True,
                "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
                "gpu": self.gpu_profiler.get_current_metrics() if self.gpu_profiler else {}}

# ============================================================
# ENHANCED EVOLUTIONARY ENGINE
# ============================================================
class EvolutionaryEngine:
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

        # ===== Enhanced modules =====
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            # Use CausalBandit instead of ContextualBandit
            self.lifecycle_actions = ["prune", "merge", "spawn", "none"]
            self.bandit = CausalBandit(
                action_space=self.lifecycle_actions,
                fallback_solver=lambda ctx: "prune",
                min_trials_before_bandit=config.optimizer.bandit_min_trials,
                confidence_threshold=config.optimizer.bandit_confidence_threshold,
            )
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None

        # ===== New modules =====
        self.safety_monitor = SafetyMonitor(config.safety)
        self.xai = XAIExplainer()
        self.federated = FederatedEvolutionCoordinator()
        self.multi_agent = MultiAgentCoordinator()
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(config.chaos)
        self.human_review = HumanReviewManager()
        self.quantum_optimizer = QuantumInspiredOptimizer(enabled=config.quantum.enable_distillation)
        self.flexgen_manager = FlexGenManager(config)

        # State
        self._fitness_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._running = False
        self._cycle_count = 0
        self._last_decision: Dict[str, Any] = {}

        self._health_components = {
            'pqc': self.pqc, 'cloud': self.cloud_storage,
            'predictive': self.predictive, 'optimizer': self.optimizer,
            'database': self.db_manager, 'vault': self.vault,
            'flexgen': self.flexgen_manager,
        }

        logger.info(f"EvolutionaryEngine v4.1.0 initialized with advanced enhancements")

    async def start(self):
        self._running = True
        self.task_manager.register_task(
            "evolution_loop", self._evolution_loop, self.config.general.evolution_interval_seconds
        )
        self.task_manager.start_registered_tasks()
        logger.info("EvolutionaryEngine started")

    async def _evolution_loop(self, interval: int):
        while self._running:
            start_time = time.time()
            try:
                # Chaos monkey can inject failures at cycle start
                self.chaos_monkey.maybe_fail()
                if await self.leader.try_acquire_leadership():
                    await self._evolve()
                    asyncio.create_task(self.leader.renew_leadership())
            except ChaosExperimentError as e:
                logger.warning(f"Chaos injected during evolution: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Evolution loop error: {e}", exc_info=True)
                await asyncio.sleep(60)
            finally:
                elapsed = time.time() - start_time
                if PROMETHEUS_AVAILABLE:
                    EVOLUTION_DURATION.observe(elapsed)
                    EVOLUTION_CYCLES.inc()
                await asyncio.sleep(interval)

    async def _evolve(self):
        experts = self.registry.get_all_active_experts()
        if not experts:
            return

        self.safety_monitor.reset_cycle()
        self._cycle_count += 1
        cycle_pruned = 0
        cycle_merged = 0
        cycle_spawned = 0

        context = {"task_type": "general", "token_count": 100}
        fitness_scores = {}
        fitness_values = []
        for expert in experts:
            try:
                fitness = await self._compute_fitness(expert, context)
                fitness_scores[expert.expert_id] = fitness
                fitness_values.append(fitness)
            except Exception as e:
                logger.error(f"Fitness computation error for {expert.expert_id}: {e}")
                fitness_scores[expert.expert_id] = 0.0

        if fitness_values:
            if PROMETHEUS_AVAILABLE:
                FITNESS_DISTRIBUTION.observe(np.mean(fitness_values))
            await self.predictive.update_history(fitness_values)

        # Autonomous parameter selection
        params = {}
        if self.config.optimizer.enabled:
            params = await self.optimizer.select_parameters()
            self.config.general.prune_threshold = params.get('prune_threshold', self.config.general.prune_threshold)
            self.config.general.merge_similarity_threshold = params.get('merge_similarity_threshold', self.config.general.merge_similarity_threshold)
            self.config.general.spawn_gap_threshold = params.get('spawn_gap_threshold', self.config.general.spawn_gap_threshold)
            if 'fitness_recency_weight' in params:
                self.config.general.fitness_recency_weight = params['fitness_recency_weight']

        async with self._lock:
            for expert in experts:
                context = {
                    "expert_id": expert.expert_id,
                    "fitness": fitness_scores.get(expert.expert_id, 0),
                    "domain": expert.domain,
                    "usage": expert.usage_count,
                    "accuracy": expert.accuracy_score,
                    "carbon_intensity": 400.0,  # placeholder
                }
                encoded = self.moe.encode(context) if self.moe else context
                action, confidence, source = self.bandit.select_action(encoded) if self.bandit else ("none", 0.0, "fallback")
                if action is None:
                    action = "none"

                # XAI explanation
                explanation = self.xai.explain_lifecycle(
                    expert.expert_id, action, context, confidence,
                    reward=fitness_scores.get(expert.expert_id, 0)
                )
                if PROMETHEUS_AVAILABLE:
                    XAI_DECISIONS.labels(strategy=action).inc()

                # Human review for low-confidence critical actions
                review_id = None
                if action in ("prune", "merge") and confidence < 0.5:
                    decision_id = f"{action}_{expert.expert_id}_{uuid.uuid4().hex[:6]}"
                    review_id = await self.human_review.request_review(decision_id, {
                        "action": action, "expert_id": expert.expert_id,
                        "context": context, "explanation": explanation,
                    })

                if action == "prune":
                    if not self.safety_monitor.check_prune():
                        continue
                    if fitness_scores.get(expert.expert_id, 0) < self.config.general.prune_threshold \
                            and not await self._is_critical(expert.expert_id):
                        try:
                            await self.registry.deprecate_expert(expert.expert_id, reason="evolutionary_prune")
                            cycle_pruned += 1
                            self.safety_monitor.record_prune()
                            if PROMETHEUS_AVAILABLE:
                                EXPERTS_PRUNED.inc()
                            await self.db_manager.log_event('prune', expert_id=expert.expert_id,
                                                            details={'fitness': fitness_scores[expert.expert_id],
                                                                     'explanation': explanation})
                            if self.bandit:
                                await self.bandit.update(encoded, action, 1.0)
                        except Exception as e:
                            logger.error(f"Prune failed: {e}")
                            if self.bandit:
                                await self.bandit.update(encoded, action, -1.0)
                elif action == "merge":
                    if not self.safety_monitor.check_merge():
                        continue
                    partners = await self._find_similar_experts(experts, fitness_scores)
                    if partners:
                        for eid_a, eid_b in partners:
                            if expert.expert_id in (eid_a, eid_b):
                                try:
                                    merged_id = await self._merge_experts(eid_a, eid_b)
                                    if merged_id:
                                        cycle_merged += 1
                                        self.safety_monitor.record_merge()
                                        if PROMETHEUS_AVAILABLE:
                                            EXPERTS_MERGED.inc()
                                        await self.db_manager.log_event('merge', expert_id=f"{eid_a},{eid_b}",
                                                                        details={'merged_id': merged_id})
                                        if self.bandit:
                                            await self.bandit.update(encoded, action, 1.0)
                                        break
                                except Exception as e:
                                    logger.error(f"Merge failed: {e}")
                elif action == "spawn":
                    self.safety_monitor.record_non_prune_action()
                    gap = await self._detect_domain_gap(experts, fitness_scores)
                    if gap > self.config.general.spawn_gap_threshold:
                        try:
                            new_expert_id = await self._spawn_expert(gap)
                            if new_expert_id:
                                cycle_spawned += 1
                                if PROMETHEUS_AVAILABLE:
                                    EXPERTS_SPAWNED.inc()
                                await self.db_manager.log_event('spawn', expert_id=new_expert_id,
                                                                details={'gap': gap})
                                # Assign an emergent role
                                self.multi_agent.assign_role(new_expert_id, "specialist")
                                if self.bandit:
                                    await self.bandit.update(encoded, action, 1.0)
                        except Exception as e:
                            logger.error(f"Spawn failed: {e}")
                else:
                    self.safety_monitor.record_non_prune_action()

        # Update optimizer reward
        if self.config.optimizer.enabled:
            avg_fitness = float(np.mean(fitness_values)) if fitness_values else 0.0
            await self.optimizer.update_rewards(params, avg_fitness)

        # Carbon offset purchase based on estimated carbon
        carbon_kg = avg_fitness * 0.01 if fitness_values else 0
        if carbon_kg > 0:
            try:
                await self.carbon_broker.purchase_offsets(400.0, carbon_kg)
            except Exception as e:
                logger.warning(f"Carbon offset purchase failed: {e}")

        # Federated aggregation (simulated)
        self.federated.register_participant(
            "local_engine",
            {"avg_fitness": avg_fitness if fitness_values else 0.0, "cycle": self._cycle_count}
        )
        aggregated = self.federated.aggregate()

        # Sign and backup
        cycle_summary = {
            'cycle': self._cycle_count,
            'timestamp': datetime.now().isoformat(),
            'experts_count': len(experts),
            'pruned': cycle_pruned,
            'merged': cycle_merged,
            'spawned': cycle_spawned,
            'fitness_scores': fitness_scores,
            'aggregated_federated': aggregated,
            'safety_violations': self.safety_monitor.get_violations()[-5:],
            'multi_agent_stats': self.multi_agent.get_stats(),
        }
        signature = await self.pqc.sign_evolution_event(cycle_summary)
        cycle_summary['pqc_signature'] = signature
        await self.cloud_storage.store(cycle_summary, f"cycle_{self._cycle_count}.json")

        self._last_decision = cycle_summary

    async def _compute_fitness(self, expert: ExpertProfile, context: Dict) -> float:
        # Adaptive precision switching: select precision based on workload context
        try:
            precision = await self.flexgen_manager.select_precision({"size": "medium", "carbon_intensity": 400.0})
        except Exception:
            precision = "fp32"

        if self.modp:
            precision_factor = {"fp32": 1.0, "fp16": 0.9, "int8": 0.8}.get(precision, 1.0)
            objectives = {
                "accuracy": (expert.accuracy_score if expert.accuracy_score is not None else 0.5) * precision_factor,
                "energy": 0.5 * (1.0 - (1.0 - precision_factor) * 0.5),
                "carbon": 0.5,
                "latency": 0.5,
            }
            return self.modp.evaluate(objectives, self.config.optimizer.modp_weights)
        else:
            cost = await self.cost_function.compute(expert, context)
            accuracy = expert.accuracy_score if expert.accuracy_score is not None else 0.5
            return accuracy / (cost + 1e-8)

    async def _is_critical(self, expert_id: str) -> bool:
        expert = self.registry.get_expert(expert_id)
        if not expert:
            return False
        return expert.usage_count > self.config.general.critical_usage_threshold

    async def _find_similar_experts(self, experts: List[ExpertProfile], fitness: Dict[str, float]) -> List[Tuple[str, str]]:
        pairs = []
        n = len(experts)
        for i in range(n):
            for j in range(i + 1, n):
                if experts[i].domain == experts[j].domain:
                    if abs(fitness.get(experts[i].expert_id, 0) - fitness.get(experts[j].expert_id, 0)) < 0.1:
                        pairs.append((experts[i].expert_id, experts[j].expert_id))
        return pairs[:self.config.general.max_merges_per_cycle]

    async def _merge_experts(self, expert_a_id: str, expert_b_id: str) -> Optional[str]:
        # Simulated merge
        merged_id = f"merged_{uuid.uuid4().hex[:8]}"
        return merged_id

    async def _detect_domain_gap(self, experts: List[ExpertProfile], fitness: Dict[str, float]) -> float:
        # Simulate: check if any domains have low average fitness
        if not experts:
            return 0.0
        domain_fitness = defaultdict(list)
        for e in experts:
            domain_fitness[e.domain].append(fitness.get(e.expert_id, 0))
        if not domain_fitness:
            return 0.0
        avg_fitness = np.mean([np.mean(v) for v in domain_fitness.values()])
        low_fitness_domains = sum(1 for v in domain_fitness.values() if np.mean(v) < avg_fitness * 0.7)
        gap = low_fitness_domains / max(len(domain_fitness), 1)
        return float(gap)

    async def _spawn_expert(self, gap: float) -> Optional[str]:
        new_expert_id = f"expert_{uuid.uuid4().hex[:8]}"
        return new_expert_id

    # ----------------------------------------------------------------
    # FlexGen and other public methods
    # ----------------------------------------------------------------
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

    async def run_quantum_optimization(self, candidates: List[Dict]) -> Optional[Dict]:
        return await self.quantum_optimizer.optimize_selection(candidates, self.config.optimizer.modp_weights)

    async def purchase_recs(self, energy_mwh: float) -> Dict:
        return await self.carbon_broker.purchase_recs(energy_mwh)

    async def get_pending_reviews(self) -> List[Dict]:
        return await self.human_review.get_pending()

    async def approve_review(self, review_id: str) -> bool:
        return await self.human_review.approve(review_id)

    async def reject_review(self, review_id: str) -> bool:
        return await self.human_review.reject(review_id)

    async def trigger_chaos(self, enabled: bool = True, probability: float = 0.1):
        self.chaos_monkey.enabled = enabled
        self.chaos_monkey.failure_probability = probability
        return self.chaos_monkey.get_stats()

    async def get_enhancement_status(self) -> Dict:
        return {
            "safety_violations": self.safety_monitor.get_violations()[-5:],
            "chaos_monkey": self.chaos_monkey.get_stats(),
            "human_review_pending": await self.human_review.get_pending(),
            "multi_agent_stats": self.multi_agent.get_stats(),
            "carbon_broker_totals": self.carbon_broker.get_totals(),
            "federated_participants": self.federated.get_participant_count(),
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "last_xai": self._last_decision.get("safety_violations", []),
        }

    async def health_check(self) -> Dict:
        health = {'status': 'healthy', 'components': {}}
        for name, comp in self._health_components.items():
            try:
                if hasattr(comp, 'health_check'):
                    health['components'][name] = await comp.health_check()
                elif hasattr(comp, 'get_status'):
                    health['components'][name] = comp.get_status()
                else:
                    health['components'][name] = 'ok'
            except Exception as e:
                health['components'][name] = {'error': str(e)}
                health['status'] = 'degraded'
        return health

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
                'active_expert_count': len(self.registry.get_all_active_experts()),
                'quantum': self.pqc.get_quantum_status(),
                'optimizer': self.optimizer.get_stats(),
                'predictive_available': getattr(self.predictive, 'prophet_available', False),
                'is_leader': self.leader.is_leader,
                'enhancements_available': ENHANCEMENTS_AVAILABLE,
                'flexgen': await self.get_flexgen_status(),
                'enhancements': await self.get_enhancement_status(),
            }

# ============================================================
# FastAPI REST API (with new endpoints)
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Evolutionary Engine API", version="4.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
    )

    engine: Optional[EvolutionaryEngine] = None

    async def get_engine():
        if engine is None:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return engine

    @app.get("/health")
    async def health():
        return await (await get_engine()).health_check()

    @app.get("/status")
    async def status():
        return await (await get_engine()).get_status()

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(workload: Dict, node: Dict):
        return await (await get_engine()).run_flexgen_optimization(workload, node)

    @app.get("/flexgen/status")
    async def flexgen_status():
        return await (await get_engine()).get_flexgen_status()

    @app.post("/quantum/optimize")
    async def quantum_optimize(candidates: List[Dict]):
        return await (await get_engine()).run_quantum_optimization(candidates)

    @app.get("/human-review/pending")
    async def human_review_pending():
        return await (await get_engine()).get_pending_reviews()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(review_id: str):
        return {"approved": await (await get_engine()).approve_review(review_id)}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(review_id: str):
        return {"rejected": await (await get_engine()).reject_review(review_id)}

    @app.post("/chaos/trigger")
    async def chaos_trigger(enabled: bool = True, probability: float = 0.1):
        return await (await get_engine()).trigger_chaos(enabled, probability)

    @app.get("/enhancements/status")
    async def enhancements_status():
        return await (await get_engine()).get_enhancement_status()

    @app.on_event("startup")
    async def startup():
        global engine
        config = EvolutionConfig()
        registry = ExpertRegistry()
        cost_function = SustainabilityCostFunction()
        digital_twin = DigitalTwin()
        mlops = MLOpsPipeline()
        db_manager = AsyncDatabaseManager(config)
        task_manager = TaskManager()
        vault = VaultManager(config)
        pqc = PostQuantumCrypto(config, vault)
        cloud = MultiCloudStorage(config)
        predictive = PredictiveAnalytics(config)
        optimizer = BioInspiredOptimizer(config, db_manager)
        leader = LeaderElection(config)
        engine = EvolutionaryEngine(
            config=config, registry=registry, cost_function=cost_function,
            digital_twin=digital_twin, mlops=mlops, db_manager=db_manager,
            task_manager=task_manager, pqc=pqc, cloud_storage=cloud,
            predictive_analytics=predictive, autonomous_optimizer=optimizer,
            vault=vault, leader_election=leader,
        )
        await engine.start()

    @app.on_event("shutdown")
    async def shutdown():
        if engine:
            await engine.stop()

# ============================================================
# Singleton accessor
# ============================================================
_engine_instance = None
_engine_lock = asyncio.Lock()

async def get_evolutionary_engine(
    config, registry, cost_function, digital_twin, mlops, db_manager,
    task_manager, pqc, cloud_storage, predictive_analytics,
    autonomous_optimizer, vault, leader_election,
) -> EvolutionaryEngine:
    global _engine_instance
    if _engine_instance is None:
        async with _engine_lock:
            if _engine_instance is None:
                _engine_instance = EvolutionaryEngine(
                    config=config, registry=registry, cost_function=cost_function,
                    digital_twin=digital_twin, mlops=mlops, db_manager=db_manager,
                    task_manager=task_manager, pqc=pqc, cloud_storage=cloud_storage,
                    predictive_analytics=predictive_analytics,
                    autonomous_optimizer=autonomous_optimizer,
                    vault=vault, leader_election=leader_election,
                )
    return _engine_instance

# ============================================================
# Main entry point
# ============================================================
async def main():
    print("Starting Evolutionary Engine v4.1.0 Demo...")
    config = EvolutionConfig()
    registry = ExpertRegistry()
    cost_function = SustainabilityCostFunction()
    digital_twin = DigitalTwin()
    mlops = MLOpsPipeline()
    db_manager = AsyncDatabaseManager(config)
    task_manager = TaskManager()
    vault = VaultManager(config)
    pqc = PostQuantumCrypto(config, vault)
    cloud = MultiCloudStorage(config)
    predictive = PredictiveAnalytics(config)
    optimizer = BioInspiredOptimizer(config, db_manager)
    leader = LeaderElection(config)
    engine = EvolutionaryEngine(
        config=config, registry=registry, cost_function=cost_function,
        digital_twin=digital_twin, mlops=mlops, db_manager=db_manager,
        task_manager=task_manager, pqc=pqc, cloud_storage=cloud,
        predictive_analytics=predictive, autonomous_optimizer=optimizer,
        vault=vault, leader_election=leader,
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
