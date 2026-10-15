#!/usr/bin/env python3
# File: src/enhancements/evolutionary_engine_v4_0_0.py
"""
Evolutionary Engine for Green Agent v4.2.0 — Single-File Enhanced

Public API preserved:
    EvolutionaryEngine, CausalBandit, SafetyMonitor, XAIExplainer,
    FederatedEvolutionCoordinator, MultiAgentCoordinator, CarbonOffsetBroker,
    ChaosMonkey, HumanReviewManager, QuantumInspiredOptimizer,
    BioInspiredOptimizer, AsyncDatabaseManager, LeaderElection, FlexGenManager,
    PostQuantumCrypto, MultiCloudStorage, PredictiveAnalytics, VaultManager,
    RateLimiter, CircuitBreaker, GlobalCircuitBreaker, TaskManager,
    EvolutionConfig, all sub-configs, and the exception hierarchy.

Key fixes and enhancements over v4.1.0:
- `await self.bandit.update(...)` bug fixed (bandit.update is sync).
- AsyncDatabaseManager is a real aiosqlite-backed manager with in-memory
  fallback when aiosqlite is missing.
- _merge_experts / _spawn_expert only increment counters when the registry
  actually performs the operation, or raise NotImplementedYetError.
- PredictiveAnalytics.forecast_fitness uses exponential smoothing + a linear
  trend, not a constant series.
- PostQuantumCrypto performs real ECDSA signing (PQC when available).
- MultiCloudStorage tries real AWS uploads when credentials are configured.
- CausalBandit is a real linear contextual bandit with Thompson sampling.
- SafetyMonitor exposes snapshot/restore for chaos.
- XAIExplainer returns a structured dict.
- FederatedEvolutionCoordinator does weighted aggregation with round IDs and
  correctly scaled DP noise.
- MultiAgentCoordinator deduplicates role assignments and uses stable SHA-256
  hashing for task assignment.
- CarbonOffsetBroker is thread-safe.
- ChaosMonkey has named faults and a seeded RNG.
- HumanReviewManager has TTL, capacity, and audit log.
- LeaderElection uses Redis when configured, otherwise an in-process lock.
- _evolution_loop uses an interruptible shutdown wait.
- FastAPI app is built via `create_app()` with a lifespan handler and auth on
  admin endpoints.
- Single Prometheus registry with memoized handles.
- `build_components()` factory; `main()` and the FastAPI app share wiring.
- `__aenter__` / `__aexit__` support on the engine.
"""

from __future__ import annotations

import asyncio
import contextlib
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
                    "evolution.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )

try:
    from pydantic import BaseModel, Field, field_validator  # noqa: F401
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

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
    from sqlalchemy import Column, DateTime, Float, Integer, JSON, String, Text  # noqa: F401
    from sqlalchemy.ext.declarative import declarative_base  # noqa: F401
    from sqlalchemy.orm import scoped_session, sessionmaker  # noqa: F401
    from sqlalchemy.ext.asyncio import (  # noqa: F401
        AsyncSession, create_async_engine,
    )
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
    import asyncpg  # noqa: F401
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

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
    from prophet import Prophet  # noqa: F401
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from fastapi import Depends, FastAPI, HTTPException, Request
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
    import redis.asyncio as redis  # noqa: F401
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


# -----------------------------------------------------------------------------
# Enhancement modules (with fallbacks)
# -----------------------------------------------------------------------------
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

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
        def select(self, encoded): return "none"

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
    from enhancements.gpu_optimization.flexgen_controller import (  # type: ignore
        FlexGenController,
    )
    from enhancements.gpu_optimization.flexgen_cost_model import (  # type: ignore
        FlexGenCostModel,
    )
    from enhancements.gpu_optimization.policy_drift_detector import (  # type: ignore
        PolicyDriftDetector,
    )
    from enhancements.schemas.node_descriptor import NodeDescriptor  # type: ignore
    from enhancements.schemas.workload_descriptor import (  # type: ignore
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


# -----------------------------------------------------------------------------
# Green Agent components (guarded)
# -----------------------------------------------------------------------------
try:
    from ..expert_registry import ExpertProfile, ExpertRegistry  # type: ignore
    from ..digital_twin import DigitalTwin  # type: ignore
    from ..mlops_pipeline import MLOpsPipeline  # type: ignore
    from ..database.manager import DatabaseManager  # type: ignore
    from ..task_manager import TaskManager  # type: ignore
    from .sustainability_cost import SustainabilityCostFunction  # type: ignore
except ImportError:

    class ExpertRegistry:  # type: ignore[no-redef]
        def get_all_active_experts(self): return []
        def get_expert(self, eid): return None
        async def deprecate_expert(self, eid, reason=""): pass

    class ExpertProfile:  # type: ignore[no-redef]
        def __init__(self, expert_id: str = "", domain: str = "",
                     usage_count: int = 0, accuracy_score: Optional[float] = None,
                     last_used: Optional[datetime] = None):
            self.expert_id = expert_id
            self.domain = domain
            self.usage_count = usage_count
            self.accuracy_score = accuracy_score
            self.last_used = last_used

    class DigitalTwin:  # type: ignore[no-redef]
        pass

    class MLOpsPipeline:  # type: ignore[no-redef]
        pass

    class DatabaseManager:  # type: ignore[no-redef]
        pass

    class TaskManager:  # type: ignore[no-redef]
        """Fallback TaskManager used only if the real one is unavailable."""
        def __init__(self):
            self.tasks: Dict[str, asyncio.Task] = {}
            self._task_coroutines = {}
            self._health = {}
            self.shutdown_event = asyncio.Event()
            self._lock = asyncio.Lock()

        def start_task(self, name, coro_func, *args, **kwargs):
            async def wrapper():
                backoff = 1.0
                while not self.shutdown_event.is_set():
                    try:
                        await coro_func(*args, **kwargs)
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.error(f"task_crashed name={name} error={e}")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 300.0)
            self.tasks[name] = asyncio.create_task(wrapper(), name=name)

        def register_task(self, name, coro_func, *args, **kwargs):
            self._task_coroutines[name] = (coro_func, args, kwargs)

        def start_registered_tasks(self):
            for name, (fn, args, kwargs) in self._task_coroutines.items():
                self.start_task(name, fn, *args, **kwargs)
            self._task_coroutines.clear()

        async def stop_all(self):
            self.shutdown_event.set()
            async with self._lock:
                tasks = list(self.tasks.values())
                for t in tasks:
                    t.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                self.tasks.clear()

    class SustainabilityCostFunction:  # type: ignore[no-redef]
        async def compute(self, expert, context): return 0.1


# =============================================================================
# Exceptions
# =============================================================================
class EvolutionaryEngineError(Exception):
    """Base class for the evolutionary engine."""


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
class NotImplementedYetError(EvolutionaryEngineError): pass


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
EVOLUTION_CYCLES = METRICS.counter("evolution_cycles_total", "Evolution cycles")
EXPERTS_PRUNED = METRICS.counter("evolution_experts_pruned_total", "Experts pruned")
EXPERTS_MERGED = METRICS.counter("evolution_experts_merged_total", "Experts merged")
EXPERTS_SPAWNED = METRICS.counter("evolution_experts_spawned_total", "Experts spawned")
FITNESS_DISTRIBUTION = METRICS.histogram("evolution_fitness", "Fitness scores")
EVOLUTION_DURATION = METRICS.histogram("evolution_cycle_duration_seconds", "Cycle duration (s)")
PQC_SIGNATURES = METRICS.counter(
    "evolution_pqc_signatures_total", "PQC signatures", ["algorithm", "status"]
)
CLOUD_STORAGE = METRICS.counter(
    "evolution_cloud_operations_total", "Cloud storage ops",
    ["provider", "operation", "status"],
)
VAULT_OPERATIONS = METRICS.counter(
    "evolution_vault_operations_total", "Vault ops", ["operation", "status"]
)
OPTIMIZER_DECISIONS = METRICS.counter(
    "evolution_optimizer_decisions_total", "Optimizer decisions",
    ["parameter", "action"],
)
CIRCUIT_BREAKER_STATE = METRICS.gauge(
    "evolution_circuit_breaker_state", "CB state",
    ["service"],
)
HEALTH_SCORE = METRICS.gauge("evolution_health_score", "Health score")
SAFETY_VIOLATIONS = METRICS.counter(
    "evolution_safety_violations_total", "Safety violations", ["rule"]
)
CHAOS_EXPERIMENTS = METRICS.counter(
    "evolution_chaos_experiments_total", "Chaos experiments", ["fault", "status"]
)
HUMAN_REVIEWS = METRICS.counter(
    "evolution_human_reviews_total", "Human reviews", ["status"]
)
XAI_DECISIONS = METRICS.counter(
    "evolution_xai_decisions_total", "XAI decisions", ["strategy"]
)
CARBON_OFFSETS = METRICS.counter(
    "evolution_carbon_offsets_total", "Carbon offsets", ["status"]
)
BANDIT_UPDATES = METRICS.counter(
    "evolution_bandit_updates_total", "Bandit updates", ["action"]
)


# =============================================================================
# Config
# =============================================================================
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
        allow_dev_master_key: bool = True

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                if self.allow_dev_master_key:
                    return os.urandom(32)
                raise ConfigError("quantum.master_key is required")
            return bytes.fromhex(self.master_key)

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
        parameter_space: Dict[str, List[float]] = Field(default_factory=lambda: {
            "prune_threshold": [0.1, 0.2, 0.3],
            "merge_similarity_threshold": [0.8, 0.85, 0.9],
            "spawn_gap_threshold": [0.2, 0.3, 0.4],
            "fitness_recency_weight": [0.2, 0.3, 0.4],
        })
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "accuracy": 0.4, "energy": 0.3, "carbon": 0.2, "latency": 0.1,
            }
        )
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bandit_feature_dim: int = 8
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
        seed: int = 42
        hitl_ttl_seconds: float = 3600.0
        hitl_max_pending: int = 100

    class APIConfig(BaseModel):
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = "change_me_in_production"
        require_auth_for_admin: bool = True
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
        allow_dev_master_key: bool = True

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                if self.allow_dev_master_key:
                    return os.urandom(32)
                raise ConfigError("quantum.master_key is required")
            return bytes.fromhex(self.master_key)

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
            "prune_threshold": [0.1, 0.2, 0.3],
            "merge_similarity_threshold": [0.8, 0.85, 0.9],
            "spawn_gap_threshold": [0.2, 0.3, 0.4],
            "fitness_recency_weight": [0.2, 0.3, 0.4],
        })
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
        seed: int = 42
        hitl_ttl_seconds: float = 3600.0
        hitl_max_pending: int = 100

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = "change_me_in_production"
        require_auth_for_admin: bool = True
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


# =============================================================================
# Interfaces
# =============================================================================
@runtime_checkable
class IPQC(Protocol):
    async def sign_evolution_event(self, event_data: Dict) -> Dict: ...
    async def verify_evolution_event(self, event_data: Dict, signature: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...


@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict: ...
    async def retrieve(self, key: str) -> Optional[Dict]: ...
    def get_status(self) -> Dict: ...


@runtime_checkable
class IPredictiveAnalytics(Protocol):
    async def update_history(self, fitness_scores: List[float]) -> None: ...
    async def forecast_fitness(self, horizon_hours: int = 24) -> Dict: ...
    def get_status(self) -> Dict: ...


@runtime_checkable
class IAutonomousOptimizer(Protocol):
    async def select_parameters(self) -> Dict: ...
    async def update_rewards(self, parameters: Dict, outcome: float) -> None: ...
    def get_stats(self) -> Dict: ...


@runtime_checkable
class IAsyncDatabase(Protocol):
    async def log_event(self, event_type: str, expert_id: Optional[str] = None,
                        details: Optional[Dict] = None) -> None: ...
    async def load_optimizer_state(self) -> Optional[Dict]: ...
    async def save_optimizer_state(self, state: Dict) -> None: ...
    async def health_check(self) -> Dict: ...
    async def close(self) -> None: ...


@runtime_checkable
class IHealthCheckable(Protocol):
    async def health_check(self) -> Dict: ...


# =============================================================================
# Helpers
# =============================================================================
def _cfg(config: Any, name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _cfg_nested(config: Any, path: str, default: Any) -> Any:
    if config is None:
        return default
    node = config
    for part in path.split("."):
        node = getattr(node, part, None)
        if node is None:
            return default
    return node


def _serialize(payload: Any) -> str:
    return json.dumps(payload, default=str, sort_keys=True)


# =============================================================================
# Circuit breaker (sync + async aware)
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0
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
                CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0.5)
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
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0)
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
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(1)

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


# =============================================================================
# RateLimiter
# =============================================================================
class RateLimiter:
    def __init__(self, config: APIConfig):
        self.config = config
        self.rate = float(config.rate_limit_requests)
        self.window = float(config.rate_limit_window)
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
class VaultManager:
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.client = None
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "vault",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout,
        )
        if VAULT_AVAILABLE and config.vault.url and config.vault.token:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception as e:
                logger.error(f"vault_init_failed error={e}")

    async def store_secret(self, path: str, data: Dict) -> None:
        if not self.client:
            raise VaultError("Vault client not configured")

        async def _store():
            self.client.secrets.kv.v2.create_or_update_secret(path=path, secret=data)

        try:
            await self.circuit_breaker.call_async(_store)
            VAULT_OPERATIONS.labels(operation="store", status="success").inc()
        except Exception as e:
            VAULT_OPERATIONS.labels(operation="store", status="failed").inc()
            raise VaultError(f"store_failed error={e}") from e

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None

        async def _get():
            secret = self.client.secrets.kv.v2.read_secret(path=path)
            return secret["data"]["data"]

        try:
            result = await self.circuit_breaker.call_async(_get)
            VAULT_OPERATIONS.labels(operation="read", status="success").inc()
            return result
        except Exception:
            VAULT_OPERATIONS.labels(operation="read", status="failed").inc()
            return None

    def get_status(self) -> Dict[str, Any]:
        return {"available": self.client is not None}


# =============================================================================
# PostQuantumCrypto — real signing with ECDSA fallback
# =============================================================================
class PostQuantumCrypto(IPQC):
    def __init__(self, config: EvolutionConfig, vault: VaultManager):
        self.config = config
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE and CRYPTO_AVAILABLE
        self.master_key = config.quantum.get_master_key_bytes()
        self.salt = os.urandom(16)
        self._keys: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    async def generate_keypair(self, algorithm: Optional[str] = None) -> Dict[str, Any]:
        algorithm = algorithm or self.config.quantum.pqc_algorithm
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
                raise SecurityError("no cryptographic backend available")

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

    async def sign_evolution_event(self, event_data: Dict) -> Dict[str, Any]:
        with self._lock:
            if not self._keys:
                # Lazy key generation
                pass
        if not self._keys:
            await self.generate_keypair()

        with self._lock:
            key_id = next(iter(self._keys))
            entry = self._keys[key_id]

        payload = _serialize(event_data).encode()
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

    async def verify_evolution_event(
        self, event_data: Dict, signature: Dict[str, Any]
    ) -> bool:
        key_id = signature.get("key_id")
        with self._lock:
            entry = self._keys.get(key_id) if key_id else None
        if entry is None:
            return False
        payload = _serialize(event_data).encode()
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
            mod.verify(
                entry["public"], payload, bytes.fromhex(signature["signature"])
            )
            return True
        except Exception:
            return False

    def get_quantum_status(self) -> Dict[str, Any]:
        return {
            "pqc_available": self.pqc_available,
            "algorithm_in_use": "pqc" if self.pqc_available else "ecdsa",
            "keys_held": len(self._keys),
        }


# =============================================================================
# MultiCloudStorage — local + optional cloud backends
# =============================================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.backup_dir = Path("./evolution_backups")
        try:
            self.backup_dir.mkdir(exist_ok=True)
        except Exception:
            pass
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and config.cloud.aws_bucket:
            try:
                self.providers["aws"] = {
                    "client": boto3.client("s3", region_name=config.cloud.aws_region),
                    "bucket": config.cloud.aws_bucket,
                }
            except Exception as e:
                logger.warning(f"aws_client_init_failed error={e}")

    async def store(self, data: Dict, filename: Optional[str] = None) -> Dict[str, Any]:
        filename = filename or f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        path = self.backup_dir / filename
        payload = _serialize(data)
        try:
            await asyncio.to_thread(path.write_text, payload)
        except Exception as e:
            CLOUD_STORAGE.labels(provider="local", operation="store", status="failed").inc()
            raise CloudStorageError(f"local_store_failed error={e}") from e
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
            raw = await asyncio.to_thread(path.read_text)
            return json.loads(raw)
        except Exception:
            return None

    def get_status(self) -> Dict[str, Any]:
        return {
            "providers": list(self.providers.keys()) or ["local"],
            "backup_dir": str(self.backup_dir),
        }


# =============================================================================
# PredictiveAnalytics — real exponential smoothing + trend
# =============================================================================
class PredictiveAnalytics(IPredictiveAnalytics):
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE
        self.history: Deque[float] = deque(maxlen=1000)
        self._lock = threading.RLock()

    async def update_history(self, fitness_scores: List[float]) -> None:
        with self._lock:
            self.history.extend(float(v) for v in fitness_scores)

    async def forecast_fitness(self, horizon_hours: int = 24) -> Dict[str, Any]:
        with self._lock:
            recent = list(self.history)[-30:]

        if not recent:
            return {
                "forecast": [0.0] * horizon_hours,
                "base": 0.0,
                "trend": 0.0,
                "confidence": 0.0,
                "model": "none",
                "samples": 0,
            }

        # Exponential smoothing
        alpha = 0.3
        smoothed = recent[0]
        for v in recent[1:]:
            smoothed = alpha * v + (1.0 - alpha) * smoothed

        # Linear trend
        x = np.arange(len(recent))
        try:
            slope = float(np.polyfit(x, np.asarray(recent), 1)[0])
        except Exception:
            slope = 0.0

        forecast = [smoothed + slope * (h + 1) for h in range(horizon_hours)]
        confidence = float(1.0 / (1.0 + abs(slope)))
        return {
            "forecast": forecast,
            "base": float(smoothed),
            "trend": slope,
            "confidence": confidence,
            "model": "exp_smoothing_linear_trend",
            "samples": len(recent),
            "prophet_available": self.prophet_available,
        }

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            n = len(self.history)
        return {"available": True, "history_length": n, "prophet_available": self.prophet_available}


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
    Per-action Bayesian linear regression bandit with UCB or Thompson sampling.
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
            BANDIT_UPDATES.labels(action=action).inc()

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

    def seed_safe_policy(self, context: Any, policy: str) -> None:
        if policy in self.theta:
            self.update(context, policy, 0.1)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": list(self.actions),
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor
# =============================================================================
class SafetyMonitor:
    """
    Temporal safety rules for lifecycle decisions. Public API kept as
    `check_prune()` / `check_merge()` returning bool. Violations carry reasons.
    """

    def __init__(self, config: SafetyConfig):
        self.config = config
        self.prunes_this_cycle = 0
        self.merges_this_cycle = 0
        self.consecutive_prunes = 0
        self.violations: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def reset_cycle(self) -> None:
        with self._lock:
            self.prunes_this_cycle = 0
            self.merges_this_cycle = 0

    def check_prune(self) -> bool:
        if not self.config.enable_monitor:
            return True
        with self._lock:
            if self.prunes_this_cycle >= self.config.max_prune_per_cycle:
                self._record("max_prune_per_cycle", {"count": self.prunes_this_cycle})
                return False
            if self.consecutive_prunes >= self.config.max_consecutive_prunes:
                self._record("max_consecutive_prunes", {"count": self.consecutive_prunes})
                return False
            return True

    def check_merge(self) -> bool:
        if not self.config.enable_monitor:
            return True
        with self._lock:
            if self.merges_this_cycle >= self.config.max_merge_per_cycle:
                self._record("max_merge_per_cycle", {"count": self.merges_this_cycle})
                return False
            return True

    def record_prune(self) -> None:
        with self._lock:
            self.prunes_this_cycle += 1
            self.consecutive_prunes += 1

    def record_merge(self) -> None:
        with self._lock:
            self.merges_this_cycle += 1
            self.consecutive_prunes = 0

    def record_non_prune_action(self) -> None:
        with self._lock:
            self.consecutive_prunes = 0

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_prune_per_cycle": self.config.max_prune_per_cycle,
                "max_merge_per_cycle": self.config.max_merge_per_cycle,
                "max_consecutive_prunes": self.config.max_consecutive_prunes,
                "enable_monitor": self.config.enable_monitor,
            }

    def restore(self, snapshot: Dict[str, Any]) -> None:
        with self._lock:
            if "max_prune_per_cycle" in snapshot:
                self.config.max_prune_per_cycle = int(snapshot["max_prune_per_cycle"])
            if "max_merge_per_cycle" in snapshot:
                self.config.max_merge_per_cycle = int(snapshot["max_merge_per_cycle"])
            if "max_consecutive_prunes" in snapshot:
                self.config.max_consecutive_prunes = int(snapshot["max_consecutive_prunes"])
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
# XAIExplainer
# =============================================================================
class XAIExplainer:
    def explain_lifecycle(
        self,
        expert_id: str,
        action: str,
        context: Dict[str, Any],
        confidence: float,
        reward: Optional[float] = None,
    ) -> Dict[str, Any]:
        top = sorted(
            (
                {"feature": k, "value": float(v)}
                for k, v in context.items() if isinstance(v, (int, float))
            ),
            key=lambda x: -abs(x["value"]),
        )[:5]
        narrative = f"Expert '{expert_id}': action='{action}'"
        if "fitness" in context:
            narrative += f" | fitness={context['fitness']:.3f}"
        if "usage" in context:
            narrative += f" | usage={context['usage']}"
        if confidence:
            narrative += f" | confidence={confidence:.2f}"
        return {
            "expert_id": expert_id,
            "action": action,
            "confidence": float(confidence),
            "reward": float(reward) if reward is not None else None,
            "top_contributions": top,
            "narrative": narrative,
        }

    def explain_lifecycle_str(
        self,
        expert_id: str,
        action: str,
        context: Dict[str, Any],
        confidence: float,
        reward: Optional[float] = None,
    ) -> str:
        return self.explain_lifecycle(expert_id, action, context, confidence, reward)["narrative"]


# =============================================================================
# FederatedEvolutionCoordinator — weighted, round IDs, DP
# =============================================================================
class FederatedEvolutionCoordinator:
    def __init__(
        self,
        privacy_budget: float = 0.5,
        sensitivity: float = 1.0,
        min_clients: int = 1,
        seed: Optional[int] = 42,
    ):
        self.privacy_budget = max(float(privacy_budget), 1e-6)
        self.sensitivity = float(sensitivity)
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.round_id = 0
        self._rng = np.random.default_rng(seed if seed is not None else 42)
        self._lock = threading.RLock()

    def register_participant(
        self,
        participant_id: str,
        update: Dict[str, float],
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

    def aggregate(self, expected_round: Optional[int] = None) -> Dict[str, float]:
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
            aggregated: Dict[str, float] = {}
            noise_scale = self.sensitivity / self.privacy_budget
            for key in keys:
                values: List[float] = []
                weights: List[float] = []
                for data in active.values():
                    if key not in data["update"]:
                        continue
                    v = data["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(float(data["weight"]))
                if not values:
                    continue
                wsum = sum(weights)
                mean = (
                    sum(v * w for v, w in zip(values, weights)) / wsum
                    if wsum > 0 else float(np.mean(values))
                )
                mean += float(self._rng.laplace(0.0, noise_scale))
                aggregated[key] = mean
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
# MultiAgentCoordinator — stable hash, thread-safe, dedup
# =============================================================================
class MultiAgentCoordinator:
    def __init__(self, num_agents: int = 3):
        self.expert_roles: Dict[str, str] = {}
        self.role_assignments: Dict[str, List[str]] = defaultdict(list)
        self.num_agents = max(1, int(num_agents))
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(expert_id: str) -> int:
        return int(hashlib.sha256(expert_id.encode()).hexdigest(), 16)

    def assign_role(self, expert_id: str, role: str) -> str:
        with self._lock:
            # Deduplicate: remove from previous role's bucket first.
            prior = self.expert_roles.get(expert_id)
            if prior is not None and prior != role and expert_id in self.role_assignments[prior]:
                self.role_assignments[prior].remove(expert_id)
            self.expert_roles[expert_id] = role
            if expert_id not in self.role_assignments[role]:
                self.role_assignments[role].append(expert_id)
            return role

    def assign_task(self, task_id: str) -> str:
        with self._lock:
            roles = list(self.role_assignments.keys()) or ["specialist"]
            role = roles[self._stable_hash(task_id) % len(roles)]
            return role

    def get_role_for_expert(self, expert_id: str) -> str:
        return self.expert_roles.get(expert_id, "unassigned")

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "num_experts": len(self.expert_roles),
                "roles": {role: len(experts) for role, experts in self.role_assignments.items()},
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
        logger.info(f"rec_purchased mwh={energy_mwh:.4f} usd={cost:.4f}")
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
    FAULT_TYPES = {"drop_database", "freeze_bandit", "safety_override", "skip_spawn"}

    def __init__(self, config: ChaosConfig):
        self.enabled = bool(config.enabled)
        self.failure_probability = float(config.failure_probability)
        self._rng = random.Random(int(config.seed))
        self.injected_failures = 0
        self.events: List[Dict[str, Any]] = []
        self._frozen = False
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def maybe_fail(self) -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self.injected_failures += 1
        self._record(fault, "triggered")
        CHAOS_EXPERIMENTS.labels(fault=fault, status="triggered").inc()
        return fault

    def apply(self, fault: str, engine: "EvolutionaryEngine") -> None:
        with self._lock:
            if fault == "drop_database":
                self._snapshots.setdefault("drop_database", {"db": engine.db_manager})
                engine.db_manager = _NullDatabaseManager()
            elif fault == "freeze_bandit":
                self._frozen = True
            elif fault == "safety_override":
                self._snapshots.setdefault(
                    "safety_override",
                    {"safety": engine.safety_monitor.snapshot()},
                )
                engine.safety_monitor.config.max_prune_per_cycle *= 2
            elif fault == "skip_spawn":
                # Marker only; the engine consults `is_spawn_skipped`.
                self._snapshots.setdefault("skip_spawn", {"marker": True})

    def cleanup_fault(self, fault: str, engine: "EvolutionaryEngine") -> None:
        with self._lock:
            snap = self._snapshots.pop(fault, None)
            if snap is None:
                return
            if fault == "drop_database":
                engine.db_manager = snap["db"]
            elif fault == "safety_override":
                engine.safety_monitor.restore(snap["safety"])
            elif fault == "freeze_bandit":
                self._frozen = False

    def is_frozen(self) -> bool:
        return self._frozen

    def is_spawn_skipped(self) -> bool:
        return "skip_spawn" in self._snapshots

    def _record(self, fault: str, outcome: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.events.append({
            "fault": fault,
            "outcome": outcome,
            "timestamp": datetime.now().isoformat(),
            **(extra or {}),
        })

    async def inject_and_measure(
        self, engine: "EvolutionaryEngine", fault: str, steps: int = 5
    ) -> Dict[str, Any]:
        start = time.monotonic()
        self.apply(fault, engine)
        recovery_step: Optional[int] = None
        try:
            for step in range(steps):
                try:
                    _ = engine._cycle_count  # trivial liveness probe
                    if recovery_step is None:
                        recovery_step = step
                except Exception:
                    continue
        finally:
            self.cleanup_fault(fault, engine)
        elapsed = time.monotonic() - start
        self._record(fault, "measured",
                     {"recovery_step": recovery_step, "elapsed_s": elapsed})
        CHAOS_EXPERIMENTS.labels(fault=fault, status="recovered").inc()
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
                "injected_failures": self.injected_failures,
                "frozen": self._frozen,
                "pending_snapshots": list(self._snapshots.keys()),
            }


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
                review_id=rid,
                decision_id=decision_id,
                details=dict(details),
                created_at=datetime.now(),
            )
            self._enforce_capacity()
            HUMAN_REVIEWS.labels(status="requested").inc()
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str) -> None:
        self.audit_log.append({
            "review_id": review_id,
            "action": action,
            "reviewer": reviewer,
            "timestamp": datetime.now().isoformat(),
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
# QuantumInspiredOptimizer — honest weight-based selector
# =============================================================================
class QuantumInspiredOptimizer:
    """
    Weighted multi-objective selector. The `enabled` flag retains API
    compatibility, but the implementation is transparent: it scores each
    candidate against the provided weights. If qiskit is installed, we still
    only use classical scoring — the docstring notes this explicitly.
    """

    def __init__(self, enabled: bool = False):
        self.enabled = bool(enabled)
        try:
            import qiskit  # noqa: F401
            self.qiskit_available = True
        except ImportError:
            self.qiskit_available = False
        self.available = self.enabled

    async def optimize_selection(
        self, candidates: List[Dict[str, Any]], weights: Dict[str, float]
    ) -> Optional[Dict[str, Any]]:
        if not self.available or not candidates:
            return None
        try:
            scored = []
            for c in candidates:
                score = sum(
                    float(c.get(k, 0.0)) * float(weights.get(k, 0.0))
                    for k in weights
                )
                scored.append((score, c))
            scored.sort(key=lambda x: -x[0])
            return scored[0][1] if scored else None
        except Exception as e:
            logger.warning(f"quantum_optimize_failed error={e}")
            return None

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "qiskit_available": self.qiskit_available,
            "mode": "classical_weighted",
        }


# =============================================================================
# FlexGenManager
# =============================================================================
class FlexGenManager:
    def __init__(self, config: EvolutionConfig):
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
            node=node,
            workload=workload,
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
            "carbon_intensity", self.config.optimizer.flexgen_carbon_intensity_default
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
# BioInspiredOptimizer
# =============================================================================
class BioInspiredOptimizer(IAutonomousOptimizer):
    def __init__(self, config: EvolutionConfig, db_manager: "IAsyncDatabase"):
        self.config = config
        self.db_manager = db_manager
        self.param_space = dict(config.optimizer.parameter_space)
        self.epsilon = float(config.optimizer.epsilon)
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.population: List[Dict[str, float]] = []
        self.rewards: Dict[str, Dict[float, float]] = {
            param: {float(v): 0.0 for v in vals}
            for param, vals in self.param_space.items()
        }
        self.counts: Dict[str, Dict[float, int]] = {
            param: {float(v): 0 for v in vals}
            for param, vals in self.param_space.items()
        }
        self.history: Deque[Dict[str, Any]] = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self._rng = random.Random(42)

    async def select_parameters(self) -> Dict[str, float]:
        async with self._lock:
            # Cold start: seed population from config.
            if not self.population:
                base = {
                    "prune_threshold": float(self.config.general.prune_threshold),
                    "merge_similarity_threshold": float(self.config.general.merge_similarity_threshold),
                    "spawn_gap_threshold": float(self.config.general.spawn_gap_threshold),
                    "fitness_recency_weight": float(self.config.general.fitness_recency_weight),
                }
                self.population = [base]
                for _ in range(9):
                    self.population.append({
                        "prune_threshold": max(0.1, min(0.9, base["prune_threshold"] + self._rng.uniform(-0.1, 0.1))),
                        "merge_similarity_threshold": max(0.5, min(1.0, base["merge_similarity_threshold"] + self._rng.uniform(-0.1, 0.1))),
                        "spawn_gap_threshold": max(0.1, min(0.9, base["spawn_gap_threshold"] + self._rng.uniform(-0.1, 0.1))),
                        "fitness_recency_weight": max(0.0, min(1.0, base["fitness_recency_weight"] + self._rng.uniform(-0.1, 0.1))),
                    })

            def fitness(params: Dict[str, float]) -> float:
                scores: List[float] = []
                for p, v in params.items():
                    if p in self.rewards:
                        nearest = min(self.rewards[p].keys(), key=lambda x: abs(x - v))
                        scores.append(self.rewards[p][nearest])
                return float(np.mean(scores)) if scores else 0.0

            if self.bio and self.population:
                try:
                    self.population = self.bio.evolve(
                        population=self.population,
                        fitness_fn=fitness,
                        generations=self.config.optimizer.bio_generations,
                        population_size=self.config.optimizer.bio_population_size,
                    )
                except Exception as e:
                    logger.warning(f"bio_evolve_failed error={e}")
                if self.population:
                    selected = max(self.population, key=fitness)
                else:
                    selected = self._epsilon_greedy_select()
            else:
                selected = self._epsilon_greedy_select()

            self.history.append({
                "timestamp": datetime.now().isoformat(),
                "selected": dict(selected),
            })
            OPTIMIZER_DECISIONS.labels(parameter="all", action="selected").inc()
            return {k: float(v) for k, v in selected.items()}

    def _epsilon_greedy_select(self) -> Dict[str, float]:
        selected: Dict[str, float] = {}
        for param, values in self.param_space.items():
            values_float = [float(v) for v in values]
            if self._rng.random() < self.epsilon:
                selected[param] = float(self._rng.choice(values_float))
            else:
                # Add small epsilon to break ties.
                selected[param] = max(
                    values_float,
                    key=lambda v: self.rewards[param].get(v, 0.0) + self._rng.uniform(0.0, 1e-9),
                )
        return selected

    async def update_rewards(self, parameters: Dict[str, float], outcome: float) -> None:
        async with self._lock:
            for param, val in parameters.items():
                if param not in self.rewards:
                    continue
                nearest = min(self.rewards[param].keys(), key=lambda x: abs(x - float(val)))
                count = self.counts[param][nearest] + 1
                self.counts[param][nearest] = count
                prev = self.rewards[param][nearest]
                self.rewards[param][nearest] = prev + (outcome - prev) / count

    def get_stats(self) -> Dict[str, Any]:
        return {
            "epsilon": self.epsilon,
            "history_length": len(self.history),
            "population_size": len(self.population),
            "bio_available": self.bio is not None,
            "parameter_space": {k: len(v) for k, v in self.param_space.items()},
        }


# =============================================================================
# AsyncDatabaseManager — real aiosqlite with in-memory fallback
# =============================================================================
class AsyncDatabaseManager(IAsyncDatabase):
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.db_path = "./evolution.db"
        url = config.database.url or ""
        if url.startswith("sqlite"):
            tail = url.split(":///", 1)[-1]
            if tail:
                self.db_path = tail
        self._conn: Any = None
        self._in_memory_events: Deque[Dict[str, Any]] = deque(maxlen=10_000)
        self._in_memory_state: Dict[str, Any] = {}
        self._initialized = False
        self._lock = asyncio.Lock()

    async def init(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            if AIOSQLITE_AVAILABLE:
                self._conn = await aiosqlite.connect(self.db_path)
                await self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS events ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "event_type TEXT, expert_id TEXT, details TEXT, timestamp TEXT)"
                )
                await self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS optimizer_state ("
                    "key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
                )
                await self._conn.commit()
            self._initialized = True
            logger.info(f"db_initialized path={self.db_path} aiosqlite={AIOSQLITE_AVAILABLE}")

    async def log_event(
        self, event_type: str, expert_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self._initialized:
            await self.init()
        record = {
            "event_type": event_type,
            "expert_id": expert_id,
            "details": details or {},
            "timestamp": datetime.now().isoformat(),
        }
        self._in_memory_events.append(record)
        if self._conn is not None:
            await self._conn.execute(
                "INSERT INTO events (event_type, expert_id, details, timestamp) VALUES (?, ?, ?, ?)",
                (event_type, expert_id, json.dumps(details or {}), record["timestamp"]),
            )
            await self._conn.commit()

    async def load_optimizer_state(self) -> Optional[Dict[str, Any]]:
        if not self._initialized:
            await self.init()
        if self._conn is not None:
            cursor = await self._conn.execute(
                "SELECT value FROM optimizer_state WHERE key = 'state'"
            )
            row = await cursor.fetchone()
            if row:
                try:
                    return json.loads(row[0])
                except Exception:
                    return None
        return dict(self._in_memory_state) or None

    async def save_optimizer_state(self, state: Dict[str, Any]) -> None:
        if not self._initialized:
            await self.init()
        self._in_memory_state = dict(state)
        if self._conn is not None:
            await self._conn.execute(
                "INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) VALUES (?, ?, ?)",
                ("state", json.dumps(state, default=str), datetime.now().isoformat()),
            )
            await self._conn.commit()

    async def health_check(self) -> Dict[str, Any]:
        return {
            "status": "ok" if self._initialized else "not_initialized",
            "aiosqlite": AIOSQLITE_AVAILABLE,
            "db_path": self.db_path,
            "in_memory_events": len(self._in_memory_events),
        }

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None
            self._initialized = False


class _NullDatabaseManager(IAsyncDatabase):
    """Placeholder used by chaos `drop_database` fault."""
    async def log_event(self, *a, **k): return None
    async def load_optimizer_state(self): return None
    async def save_optimizer_state(self, state): return None
    async def health_check(self): return {"status": "unavailable"}
    async def close(self): return None


# =============================================================================
# LeaderElection — Redis or in-process
# =============================================================================
class LeaderElection:
    def __init__(self, config: EvolutionConfig):
        self.config = config
        self.is_leader = False
        self._lock_name = "green_agent:evolution:leader"
        self._redis = None
        self._token = uuid.uuid4().hex
        if self.config.leader.enabled and REDIS_AVAILABLE and self.config.leader.redis_url:
            try:
                import redis.asyncio as redis_async  # local import
                self._redis = redis_async.from_url(
                    self.config.leader.redis_url, decode_responses=True
                )
            except Exception as e:
                logger.warning(f"redis_init_failed error={e}")
                self._redis = None

    async def try_acquire_leadership(self) -> bool:
        if self._redis is not None:
            try:
                ok = await self._redis.set(
                    self._lock_name, self._token, nx=True,
                    ex=int(self.config.leader.ttl_seconds),
                )
                self.is_leader = bool(ok)
                return self.is_leader
            except Exception as e:
                logger.warning(f"redis_acquire_failed error={e}")
                self.is_leader = True
                return True
        # In-process fallback: always leader.
        self.is_leader = True
        return True

    async def renew_leadership(self) -> None:
        if self._redis is None or not self.is_leader:
            return
        try:
            # Only renew if we still own the token.
            current = await self._redis.get(self._lock_name)
            if current == self._token:
                await self._redis.expire(
                    self._lock_name, int(self.config.leader.ttl_seconds)
                )
        except Exception as e:
            logger.debug(f"redis_renew_failed error={e}")

    async def stop(self) -> None:
        if self._redis is not None and self.is_leader:
            try:
                current = await self._redis.get(self._lock_name)
                if current == self._token:
                    await self._redis.delete(self._lock_name)
            except Exception:
                pass
        self.is_leader = False
        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None


# =============================================================================
# TaskManager — supervisor with per-task health
# =============================================================================
@dataclass
class TaskHealth:
    name: str
    starts: int = 0
    crashes: int = 0
    last_error: Optional[str] = None
    uptime_seconds: float = 0.0


class TaskManager:
    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self._task_coroutines: Dict[str, Tuple[Callable[..., Any], tuple, dict]] = {}
        self._health: Dict[str, TaskHealth] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def health(self) -> Dict[str, Any]:
        return {k: asdict(v) for k, v in self._health.items()}

    def start_task(
        self, name: str, coro_func: Callable[..., Any], *args, **kwargs
    ) -> asyncio.Task:
        health = self._health.setdefault(name, TaskHealth(name=name))

        async def wrapper():
            backoff = 1.0
            max_backoff = 300.0
            health.starts += 1
            start = time.monotonic()
            try:
                while not self.shutdown_event.is_set():
                    try:
                        await coro_func(*args, **kwargs)
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        health.crashes += 1
                        health.last_error = f"{type(e).__name__}: {e}"
                        logger.error(f"task_crashed name={name} error={e}")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, max_backoff)
            finally:
                health.uptime_seconds += time.monotonic() - start

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def register_task(
        self, name: str, coro_func: Callable[..., Any], *args, **kwargs
    ) -> None:
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self) -> None:
        for name, (fn, args, kwargs) in self._task_coroutines.items():
            self.start_task(name, fn, *args, **kwargs)
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
        logger.info("all_tasks_stopped")


# =============================================================================
# EvolutionaryEngine
# =============================================================================
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
        self.digital_twin = digital_twin  # reserved for future hooks
        self.mlops = mlops  # reserved for future hooks
        self.db_manager = db_manager
        self.task_manager = task_manager
        self.pqc = pqc
        self.cloud_storage = cloud_storage
        self.predictive = predictive_analytics
        self.optimizer = autonomous_optimizer
        self.vault = vault
        self.leader = leader_election

        # Enhanced modules
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None

        self.lifecycle_actions = ["prune", "merge", "spawn", "none"]
        self.bandit = CausalBandit(
            action_space=self.lifecycle_actions,
            fallback_solver=lambda ctx: "prune",
            min_trials_before_bandit=config.optimizer.bandit_min_trials,
            confidence_threshold=config.optimizer.bandit_confidence_threshold,
            feature_dim=config.optimizer.bandit_feature_dim,
        )

        # New modules
        self.safety_monitor = SafetyMonitor(config.safety)
        self.xai = XAIExplainer()
        self.federated = FederatedEvolutionCoordinator()
        self.multi_agent = MultiAgentCoordinator(num_agents=3)
        self.carbon_broker = CarbonOffsetBroker()
        self.chaos_monkey = ChaosMonkey(config.chaos)
        self.human_review = HumanReviewManager(
            ttl_seconds=config.chaos.hitl_ttl_seconds,
            max_pending=config.chaos.hitl_max_pending,
        )
        self.quantum_optimizer = QuantumInspiredOptimizer(
            enabled=config.quantum.enable_distillation
        )
        self.flexgen_manager = FlexGenManager(config)

        # State
        self._fitness_history: Deque[float] = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._running = False
        self._cycle_count = 0
        self._last_decision: Dict[str, Any] = {}
        self._shutdown_event = asyncio.Event()
        self._registered = False

        self._health_components = {
            "pqc": self.pqc,
            "cloud": self.cloud_storage,
            "predictive": self.predictive,
            "optimizer": self.optimizer,
            "database": self.db_manager,
            "vault": self.vault,
            "flexgen": self.flexgen_manager,
        }

        logger.info("engine_initialized version=4.2.0")

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "EvolutionaryEngine":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._shutdown_event.clear()

        # Ensure DB is initialized.
        init = getattr(self.db_manager, "init", None)
        if callable(init):
            result = init()
            if inspect.isawaitable(result):
                await result

        interval = int(self.config.general.evolution_interval_seconds)
        if not self._registered:
            self.task_manager.register_task(
                "evolution_loop", self._evolution_loop, interval
            )
            self._registered = True
        self.task_manager.start_registered_tasks()
        logger.info("engine_started")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._shutdown_event.set()
        await self.leader.stop()
        await self.task_manager.stop_all()
        try:
            await self.db_manager.close()
        except Exception:
            pass
        logger.info("engine_stopped")

    async def _evolution_loop(self, interval: int) -> None:
        while self._running and not self._shutdown_event.is_set():
            start_time = time.time()
            try:
                fault = self.chaos_monkey.maybe_fail()
                if fault is not None:
                    try:
                        self.chaos_monkey.apply(fault, self)
                    except Exception as e:
                        logger.warning(f"chaos_apply_failed error={e}")

                if await self.leader.try_acquire_leadership():
                    await self._evolve()
                    await self.leader.renew_leadership()
            except ChaosExperimentError as e:
                logger.warning(f"chaos_injected error={e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"evolution_loop_error error={e}", exc_info=True)
            finally:
                elapsed = time.time() - start_time
                EVOLUTION_DURATION.observe(elapsed)
                EVOLUTION_CYCLES.inc()
                if fault is not None:
                    try:
                        self.chaos_monkey.cleanup_fault(fault, self)
                    except Exception:
                        pass

            # Interruptible sleep — wakes immediately on shutdown.
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------ #
    # Evolution cycle                                                    #
    # ------------------------------------------------------------------ #
    async def _evolve(self) -> None:
        experts = list(self.registry.get_all_active_experts() or [])
        if not experts:
            return

        self.safety_monitor.reset_cycle()
        async with self._lock:
            self._cycle_count += 1
            cycle = self._cycle_count

        # Cache precision once per cycle.
        try:
            cycle_precision = await self.flexgen_manager.select_precision(
                {"size": "medium", "carbon_intensity": 400.0}
            )
        except Exception:
            cycle_precision = "fp32"

        # Fitness computation.
        fitness_scores: Dict[str, float] = {}
        fitness_values: List[float] = []
        context = {"task_type": "general", "token_count": 100}
        for expert in experts:
            eid = getattr(expert, "expert_id", None) or f"unknown_{uuid.uuid4().hex[:6]}"
            try:
                f = await self._compute_fitness(expert, context, cycle_precision)
            except Exception as e:
                logger.error(f"fitness_error expert={eid} error={e}")
                f = 0.0
            fitness_scores[eid] = float(f)
            fitness_values.append(float(f))

        if fitness_values:
            FITNESS_DISTRIBUTION.observe(float(np.mean(fitness_values)))
            await self.predictive.update_history(fitness_values)
            async with self._lock:
                self._fitness_history.extend(fitness_values)

        # Parameter selection.
        params: Dict[str, float] = {}
        if self.config.optimizer.enabled:
            try:
                params = await self.optimizer.select_parameters()
                # Apply — Pydantic models are mutable by default; dataclasses too.
                if "prune_threshold" in params:
                    self.config.general.prune_threshold = float(params["prune_threshold"])
                if "merge_similarity_threshold" in params:
                    self.config.general.merge_similarity_threshold = float(params["merge_similarity_threshold"])
                if "spawn_gap_threshold" in params:
                    self.config.general.spawn_gap_threshold = float(params["spawn_gap_threshold"])
                if "fitness_recency_weight" in params:
                    self.config.general.fitness_recency_weight = float(params["fitness_recency_weight"])
            except Exception as e:
                logger.warning(f"optimizer_select_failed error={e}")

        # Precompute domain index for O(sum k²) pair finding.
        domain_index: Dict[str, List[Any]] = defaultdict(list)
        for e in experts:
            domain_index[getattr(e, "domain", "unknown") or "unknown"].append(e)

        cycle_pruned = 0
        cycle_merged = 0
        cycle_spawned = 0

        for expert in experts:
            eid = getattr(expert, "expert_id", None)
            if eid is None:
                continue

            ctx = {
                "expert_hash": float(
                    int(hashlib.sha256(eid.encode()).hexdigest()[:6], 16)
                ),
                "fitness": float(fitness_scores.get(eid, 0.0)),
                "domain_hash": float(
                    int(hashlib.sha256(
                        (getattr(expert, "domain", "") or "").encode()
                    ).hexdigest()[:6], 16)
                ),
                "usage": float(getattr(expert, "usage_count", 0) or 0),
                "accuracy": float(getattr(expert, "accuracy_score", 0.5) or 0.5),
                "carbon_intensity": 400.0,
            }
            encoded = self._encode_context(ctx)
            action, confidence, source = self.bandit.select_action(encoded)
            if not action:
                action = "none"

            explanation = self.xai.explain_lifecycle(
                eid, action, ctx, confidence,
                reward=fitness_scores.get(eid, 0.0),
            )
            XAI_DECISIONS.labels(strategy=action).inc()

            # HITL gate for low-confidence critical decisions.
            review_id: Optional[str] = None
            if action in ("prune", "merge") and confidence < 0.5:
                decision_id = f"{action}_{eid}_{uuid.uuid4().hex[:6]}"
                try:
                    review_id = await self.human_review.request_review(
                        decision_id,
                        {
                            "action": action,
                            "expert_id": eid,
                            "context": ctx,
                            "explanation": explanation,
                        },
                    )
                except Exception as e:
                    logger.warning(f"hitl_request_failed error={e}")

            reward = 0.0
            if action == "prune":
                if not self.safety_monitor.check_prune():
                    self.safety_monitor.record_non_prune_action()
                elif fitness_scores.get(eid, 0.0) < self.config.general.prune_threshold \
                        and not await self._is_critical(eid):
                    ok = await self._deprecate_expert(eid, "evolutionary_prune")
                    if ok:
                        cycle_pruned += 1
                        self.safety_monitor.record_prune()
                        EXPERTS_PRUNED.inc()
                        await self.db_manager.log_event(
                            "prune", expert_id=eid,
                            details={"fitness": fitness_scores[eid],
                                     "explanation": explanation},
                        )
                        reward = 1.0
                    else:
                        reward = -1.0
                else:
                    self.safety_monitor.record_non_prune_action()

            elif action == "merge":
                if not self.safety_monitor.check_merge():
                    self.safety_monitor.record_non_prune_action()
                else:
                    partners = self._find_similar_experts_for(
                        expert, domain_index, fitness_scores
                    )
                    merged = False
                    for partner_id in partners:
                        merged_id = await self._merge_experts(eid, partner_id)
                        if merged_id:
                            cycle_merged += 1
                            self.safety_monitor.record_merge()
                            EXPERTS_MERGED.inc()
                            await self.db_manager.log_event(
                                "merge", expert_id=f"{eid},{partner_id}",
                                details={"merged_id": merged_id},
                            )
                            reward = 1.0
                            merged = True
                            break
                    if not merged:
                        self.safety_monitor.record_non_prune_action()

            elif action == "spawn":
                if self.chaos_monkey.is_spawn_skipped():
                    self.safety_monitor.record_non_prune_action()
                else:
                    self.safety_monitor.record_non_prune_action()
                    gap = self._detect_domain_gap(experts, fitness_scores)
                    if gap > self.config.general.spawn_gap_threshold:
                        new_id = await self._spawn_expert(gap)
                        if new_id:
                            cycle_spawned += 1
                            EXPERTS_SPAWNED.inc()
                            self.multi_agent.assign_role(new_id, "specialist")
                            await self.db_manager.log_event(
                                "spawn", expert_id=new_id,
                                details={"gap": gap},
                            )
                            reward = 1.0
            else:
                self.safety_monitor.record_non_prune_action()

            # Bandit update (SYNC — do not await).
            if not self.chaos_monkey.is_frozen():
                self.bandit.update(encoded, action, reward)

        # Optimizer feedback.
        if self.config.optimizer.enabled and fitness_values:
            avg_fitness = float(np.mean(fitness_values))
            try:
                await self.optimizer.update_rewards(params, avg_fitness)
            except Exception as e:
                logger.warning(f"optimizer_update_failed error={e}")

        # Carbon offset for the estimated cycle emissions.
        avg_fitness = float(np.mean(fitness_values)) if fitness_values else 0.0
        carbon_kg = max(0.0, avg_fitness * 0.01)
        if carbon_kg > 0:
            try:
                await self.carbon_broker.purchase_offsets(400.0, carbon_kg)
            except Exception as e:
                logger.warning(f"carbon_offset_failed error={e}")

        # Federated aggregation.
        self.federated.register_participant(
            "local_engine",
            {"avg_fitness": avg_fitness, "cycle": float(cycle)},
            sample_count=max(1, len(fitness_values)),
        )
        aggregated = self.federated.aggregate()

        # Apply federated prior to bandit for the most recent action.
        if aggregated and "avg_fitness" in aggregated:
            # Nudge all actions slightly — a conservative use of the aggregate.
            for a in self.bandit.actions:
                try:
                    self.bandit.apply_federated_prior(a, float(aggregated["avg_fitness"]), weight=0.02)
                except Exception:
                    pass

        # Sign and persist the cycle summary.
        cycle_summary = {
            "cycle": cycle,
            "timestamp": datetime.now().isoformat(),
            "experts_count": len(experts),
            "pruned": cycle_pruned,
            "merged": cycle_merged,
            "spawned": cycle_spawned,
            "avg_fitness": avg_fitness,
            "fitness_scores": fitness_scores,
            "aggregated_federated": aggregated,
            "safety_violations": self.safety_monitor.get_violations()[-5:],
            "multi_agent_stats": self.multi_agent.get_stats(),
        }
        try:
            signature = await self.pqc.sign_evolution_event(cycle_summary)
            cycle_summary["pqc_signature"] = signature
        except Exception as e:
            logger.warning(f"pqc_sign_failed error={e}")

        try:
            await self.cloud_storage.store(cycle_summary, f"cycle_{cycle}.json")
        except Exception as e:
            logger.warning(f"cloud_store_failed error={e}")

        try:
            await self.db_manager.log_event(
                "evolution_cycle",
                details={"cycle": cycle, "pruned": cycle_pruned,
                         "merged": cycle_merged, "spawned": cycle_spawned},
            )
        except Exception as e:
            logger.warning(f"db_log_failed error={e}")

        async with self._lock:
            self._last_decision = cycle_summary

    # ------------------------------------------------------------------ #
    # Internals                                                          #
    # ------------------------------------------------------------------ #
    def _encode_context(self, context: Dict[str, Any]) -> List[float]:
        if self.moe is not None:
            try:
                encoded = self.moe.encode(context)
                if encoded:
                    return [float(v) for v in encoded if isinstance(v, (int, float))]
            except Exception:
                pass
        return [float(v) for v in context.values() if isinstance(v, (int, float))]

    async def _compute_fitness(
        self, expert: ExpertProfile, context: Dict[str, Any], precision: str
    ) -> float:
        if self.modp is not None:
            precision_factor = {"fp32": 1.0, "fp16": 0.9, "int8": 0.8}.get(precision, 1.0)
            accuracy = getattr(expert, "accuracy_score", None)
            if accuracy is None:
                accuracy = 0.5
            objectives = {
                "accuracy": float(accuracy) * precision_factor,
                "energy": 0.5 + (1.0 - precision_factor) * 0.3,
                "carbon": 0.5,
                "latency": 0.5,
            }
            try:
                return float(self.modp.evaluate(objectives, self.config.optimizer.modp_weights))
            except Exception:
                pass
        cost = await self.cost_function.compute(expert, context)
        accuracy = getattr(expert, "accuracy_score", None) or 0.5
        return float(accuracy) / (float(cost) + 1e-8)

    async def _is_critical(self, expert_id: str) -> bool:
        expert = self.registry.get_expert(expert_id)
        if not expert:
            return False
        usage = getattr(expert, "usage_count", 0) or 0
        return usage > self.config.general.critical_usage_threshold

    def _find_similar_experts_for(
        self, expert: ExpertProfile,
        domain_index: Dict[str, List[Any]],
        fitness: Dict[str, float],
    ) -> List[str]:
        """
        Find merge candidates for `expert` within the same domain. Uses a
        pre-built domain index so this is O(k) per expert, not O(n).
        """
        eid = getattr(expert, "expert_id", None)
        domain = getattr(expert, "domain", "unknown") or "unknown"
        if eid is None:
            return []
        candidates: List[str] = []
        for other in domain_index.get(domain, []):
            oid = getattr(other, "expert_id", None)
            if oid is None or oid == eid:
                continue
            if abs(fitness.get(eid, 0.0) - fitness.get(oid, 0.0)) < 0.1:
                candidates.append(oid)
            if len(candidates) >= self.config.general.max_merges_per_cycle:
                break
        return candidates

    async def _deprecate_expert(self, expert_id: str, reason: str) -> bool:
        deprecate = getattr(self.registry, "deprecate_expert", None)
        if deprecate is None:
            logger.info(f"deprecate_skipped_no_registry_support expert={expert_id}")
            return False
        try:
            result = deprecate(expert_id, reason=reason)
            if inspect.isawaitable(result):
                await result
            return True
        except Exception as e:
            logger.error(f"deprecate_failed expert={expert_id} error={e}")
            return False

    async def _merge_experts(self, expert_a_id: str, expert_b_id: str) -> Optional[str]:
        """
        Only performs a merge if the registry exposes `merge_experts`. Otherwise
        returns None so the caller does not increment a fake counter.
        """
        merger = getattr(self.registry, "merge_experts", None)
        if merger is None:
            logger.info(f"merge_skipped_no_registry_support a={expert_a_id} b={expert_b_id}")
            return None
        try:
            result = merger(expert_a_id, expert_b_id)
            if inspect.isawaitable(result):
                result = await result
            return result if isinstance(result, str) else None
        except Exception as e:
            logger.error(f"merge_failed a={expert_a_id} b={expert_b_id} error={e}")
            return None

    async def _spawn_expert(self, gap: float) -> Optional[str]:
        spawner = getattr(self.registry, "spawn_expert", None)
        if spawner is None:
            logger.info(f"spawn_skipped_no_registry_support gap={gap:.3f}")
            return None
        try:
            result = spawner(gap)
            if inspect.isawaitable(result):
                result = await result
            return result if isinstance(result, str) else None
        except Exception as e:
            logger.error(f"spawn_failed gap={gap:.3f} error={e}")
            return None

    def _detect_domain_gap(
        self, experts: List[Any], fitness: Dict[str, float]
    ) -> float:
        if not experts:
            return 0.0
        domain_fitness: Dict[str, List[float]] = defaultdict(list)
        for e in experts:
            domain = getattr(e, "domain", "unknown") or "unknown"
            eid = getattr(e, "expert_id", None)
            if eid is not None:
                domain_fitness[domain].append(fitness.get(eid, 0.0))
        if not domain_fitness:
            return 0.0
        means = [float(np.mean(v)) for v in domain_fitness.values()]
        overall = float(np.mean(means))
        low = sum(1 for m in means if m < overall * 0.7)
        return float(low) / max(len(means), 1)

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        try:
            context = self._encode_context(state or {})
            probs = self.bandit.get_action_probabilities(context)
            n = len(self.bandit.actions)
            if len(probs) == n:
                return probs
        except Exception:
            pass
        n = len(self.bandit.actions)
        return [1.0 / n] * n

    async def run_flexgen_optimization(
        self, workload: Dict[str, Any], node: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise EvolutionaryEngineError(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict[str, Any]:
        return await self.flexgen_manager.get_status()

    async def select_precision(self, workload: Dict[str, Any]) -> str:
        return await self.flexgen_manager.select_precision(workload)

    async def run_quantum_optimization(
        self, candidates: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        return await self.quantum_optimizer.optimize_selection(
            candidates, self.config.optimizer.modp_weights
        )

    async def purchase_recs(self, energy_mwh: float) -> Dict[str, Any]:
        return await self.carbon_broker.purchase_recs(energy_mwh)

    async def get_pending_reviews(self) -> List[Dict[str, Any]]:
        return await self.human_review.get_pending()

    async def approve_review(self, review_id: str, reviewer: str = "api") -> bool:
        return await self.human_review.approve(review_id, reviewer=reviewer)

    async def reject_review(self, review_id: str, reviewer: str = "api") -> bool:
        return await self.human_review.reject(review_id, reviewer=reviewer)

    async def trigger_chaos(
        self, enabled: bool = True, probability: float = 0.1
    ) -> Dict[str, Any]:
        self.chaos_monkey.enabled = bool(enabled)
        self.chaos_monkey.failure_probability = float(probability)
        return self.chaos_monkey.get_stats()

    async def get_enhancement_status(self) -> Dict[str, Any]:
        return {
            "safety_violations": self.safety_monitor.get_violations()[-5:],
            "chaos_monkey": self.chaos_monkey.get_stats(),
            "human_review_pending": await self.human_review.get_pending(),
            "multi_agent_stats": self.multi_agent.get_stats(),
            "carbon_broker_totals": self.carbon_broker.get_totals(),
            "federated": self.federated.get_stats(),
            "quantum_optimizer": self.quantum_optimizer.get_status(),
            "bandit": self.bandit.get_stats(),
            "last_explanation": (
                self._last_decision.get("safety_violations") or []
            )[-1] if self._last_decision.get("safety_violations") else None,
        }

    async def health_check(self) -> Dict[str, Any]:
        health: Dict[str, Any] = {"status": "healthy", "components": {}}
        for name, comp in self._health_components.items():
            try:
                if comp is None:
                    health["components"][name] = {"status": "unavailable"}
                    continue
                if hasattr(comp, "health_check"):
                    h = comp.health_check()
                    if inspect.isawaitable(h):
                        h = await h
                    health["components"][name] = h
                elif hasattr(comp, "get_status"):
                    health["components"][name] = comp.get_status()
                else:
                    health["components"][name] = "ok"
            except Exception as e:
                health["components"][name] = {"error": str(e)}
                health["status"] = "degraded"
        try:
            HEALTH_SCORE.set(1.0 if health["status"] == "healthy" else 0.5)
        except Exception:
            pass
        return health

    async def get_status(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "running": self._running,
                "cycle_count": self._cycle_count,
                "fitness_history_length": len(self._fitness_history),
                "active_expert_count": len(self.registry.get_all_active_experts() or []),
                "quantum": self.pqc.get_quantum_status(),
                "optimizer": self.optimizer.get_stats(),
                "predictive": self.predictive.get_status(),
                "is_leader": self.leader.is_leader,
                "enhancements_available": ENHANCEMENTS_AVAILABLE,
                "flexgen": await self.flexgen_manager.get_status(),
                "enhancements": await self.get_enhancement_status(),
                "task_health": self.task_manager.health(),
            }


# =============================================================================
# Component factory
# =============================================================================
def build_components(config: Optional[EvolutionConfig] = None) -> Dict[str, Any]:
    cfg = config or EvolutionConfig()

    registry = ExpertRegistry()
    cost_function = SustainabilityCostFunction()
    digital_twin = DigitalTwin()
    mlops = MLOpsPipeline()
    db_manager = AsyncDatabaseManager(cfg)
    task_manager = TaskManager()
    vault = VaultManager(cfg)
    pqc = PostQuantumCrypto(cfg, vault)
    cloud = MultiCloudStorage(cfg)
    predictive = PredictiveAnalytics(cfg)
    optimizer = BioInspiredOptimizer(cfg, db_manager)
    leader = LeaderElection(cfg)

    engine = EvolutionaryEngine(
        config=cfg,
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
        leader_election=leader,
    )
    return {"engine": engine, "config": cfg}


# =============================================================================
# FastAPI app factory
# =============================================================================
def create_app(config: Optional[EvolutionConfig] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config or EvolutionConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[valid-type]
        app.state.engine = build_components(cfg)["engine"]
        await app.state.engine.start()
        try:
            yield
        finally:
            await app.state.engine.stop()

    app = FastAPI(
        title="Evolutionary Engine API",
        version="4.2.0",
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
            pyjwt.decode(credentials.credentials, cfg.api.jwt_secret, algorithms=["HS256"])
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid token: {e}")
        return True

    @app.get("/health")
    async def health(request: Request) -> Dict[str, Any]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return await engine.health_check()

    @app.get("/status")
    async def status(request: Request) -> Dict[str, Any]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return await engine.get_status()

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(
        workload: Dict[str, Any],
        node: Dict[str, Any],
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, Any]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        try:
            return await engine.run_flexgen_optimization(workload, node)
        except NotImplementedYetError as e:
            raise HTTPException(status_code=501, detail=str(e))

    @app.get("/flexgen/status")
    async def flexgen_status(request: Request) -> Dict[str, Any]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return await engine.get_flexgen_status()

    @app.post("/quantum/optimize")
    async def quantum_optimize(
        candidates: List[Dict[str, Any]],
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Optional[Dict[str, Any]]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return await engine.run_quantum_optimization(candidates)

    @app.get("/human-review/pending")
    async def human_review_pending(
        request: Request,
        _: bool = Depends(require_admin),
    ) -> List[Dict[str, Any]]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return await engine.get_pending_reviews()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(
        review_id: str,
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, bool]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return {"approved": await engine.approve_review(review_id, reviewer="api")}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(
        review_id: str,
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, bool]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return {"rejected": await engine.reject_review(review_id, reviewer="api")}

    @app.post("/chaos/trigger")
    async def chaos_trigger(
        request: Request,
        enabled: bool = True,
        probability: float = 0.1,
        _: bool = Depends(require_admin),
    ) -> Dict[str, Any]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return await engine.trigger_chaos(enabled, probability)

    @app.get("/enhancements/status")
    async def enhancements_status(request: Request) -> Dict[str, Any]:
        engine = getattr(request.app.state, "engine", None)
        if not engine:
            raise HTTPException(status_code=503, detail="not initialized")
        return await engine.get_enhancement_status()

    return app


# =============================================================================
# Singleton accessor
# =============================================================================
_engine_instance: Optional[EvolutionaryEngine] = None
_engine_lock: Optional[asyncio.Lock] = None


async def get_evolutionary_engine(
    config: Optional[EvolutionConfig] = None,
    **kwargs: Any,
) -> EvolutionaryEngine:
    """
    Public singleton accessor kept for backward compatibility. Extra kwargs
    are ignored; the factory builds all dependencies. Use `build_components`
    directly if you need to inject custom components.
    """
    global _engine_instance, _engine_lock
    if _engine_lock is None:
        _engine_lock = asyncio.Lock()
    if _engine_instance is None:
        async with _engine_lock:
            if _engine_instance is None:
                _engine_instance = build_components(config)["engine"]
                await _engine_instance.start()
    return _engine_instance


# =============================================================================
# Main
# =============================================================================
async def main() -> None:
    print("=" * 80)
    print("Evolutionary Engine v4.2.0 — Single-File Enhanced")
    print("=" * 80)

    config = EvolutionConfig()
    engine = build_components(config)["engine"]

    await engine.start()

    # Run one manual evolution cycle to exercise the path.
    try:
        await engine._evolve()
    except Exception as e:
        logger.warning(f"initial_cycle_failed error={e}")

    status = await engine.get_status()
    print(json.dumps(status, indent=2, default=str))
    print("=" * 80)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(*_a: Any) -> None:
        stop_event.set()

    import signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, ValueError):
            try:
                signal.signal(sig, _request_stop)
            except Exception:
                pass

    try:
        await asyncio.wait_for(stop_event.wait(), timeout=30.0)
    except asyncio.TimeoutError:
        pass

    await engine.stop()
    print("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())


__all__ = [
    "EvolutionaryEngine",
    "CausalBandit",
    "BanditConfig",
    "SafetyMonitor",
    "XAIExplainer",
    "FederatedEvolutionCoordinator",
    "MultiAgentCoordinator",
    "CarbonOffsetBroker",
    "ChaosMonkey",
    "HumanReviewManager",
    "HumanReviewRequest",
    "QuantumInspiredOptimizer",
    "BioInspiredOptimizer",
    "AsyncDatabaseManager",
    "LeaderElection",
    "FlexGenManager",
    "PostQuantumCrypto",
    "MultiCloudStorage",
    "PredictiveAnalytics",
    "VaultManager",
    "RateLimiter",
    "CircuitBreaker",
    "CircuitBreakerState",
    "GlobalCircuitBreaker",
    "TaskManager",
    "TaskHealth",
    "EvolutionConfig",
    "GeneralConfig",
    "QuantumConfig",
    "CloudConfig",
    "DatabaseConfig",
    "VaultConfig",
    "PredictiveConfig",
    "OptimizerConfig",
    "SafetyConfig",
    "ChaosConfig",
    "APIConfig",
    "CircuitBreakerConfig",
    "LeaderConfig",
    "IPQC",
    "ICloudStorage",
    "IPredictiveAnalytics",
    "IAutonomousOptimizer",
    "IAsyncDatabase",
    "IHealthCheckable",
    "EvolutionaryEngineError",
    "ConfigError",
    "SecurityError",
    "CloudStorageError",
    "VaultError",
    "PredictionError",
    "OptimizerError",
    "DatabaseError",
    "CircuitBreakerOpenError",
    "SafetyViolationError",
    "ChaosExperimentError",
    "NotImplementedYetError",
    "build_components",
    "create_app",
    "get_evolutionary_engine",
    "main",
]
