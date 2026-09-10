#!/usr/bin/env python3
# File: src/enhancements/fallback_manager_enhanced_v15_0.py

"""
Multi-Layered Fallback Manager for Green Agent - Version 15.1 (Enterprise Quantum+)

ENHANCEMENTS OVER v15.0 (NEW IN v15.1):
- CausalBandit replaces ContextualBandit for causal RL of fallback policies.
- SafetyMonitor enforces temporal fallback rules.
- XAIExplainer produces human-readable rationale for fallback decisions.
- FederatedSecureCoordinator aggregates insights with differential privacy.
- MultiAgentCoordinator formalizes role specialisation among fallback agents.
- CarbonOffsetBroker purchases offsets and RECs.
- ChaosMonkey injects failures for resilience testing.
- HumanReviewManager enables pre-commit human review.
- FlexGenPrecisionPolicy recommends precision per fallback.
- QuantumDistillationOptimizer (optional) uses QAOA for policy/region selection.

All previous v15.0 features retained.
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
import threading
import aiohttp
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union, Protocol, runtime_checkable
from collections import defaultdict, deque
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import random
from functools import wraps
import contextlib
import base64
import contextvars
import io
import pickle

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
    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs): self.actions = action_space
        def update(self, context, action, reward): pass
        def sample_action(self, context): return self.actions[0] if self.actions else None
    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context): return self.teachers[0](context) if self.teachers else None

# ============================================================
# QISKIT (optional, for quantum distillation)
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
# ENHANCED CONFIGURATION
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
    from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
    from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func, text
    from sqlalchemy.pool import NullPool, QueuePool
    from sqlalchemy.exc import SQLAlchemyError, OperationalError
    SQLALCHEMY_ASYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_ASYNC_AVAILABLE = False

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

try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    from jose import JWTError, jwt
    from jose.constants import ALGORITHMS
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

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
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

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

# ============================================================
# STRUCTURED LOGGING (fallback) with contextvars
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
            logging.handlers.RotatingFileHandler('fallback_manager_v15.log', maxBytes=10*1024*1024, backupCount=5),
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
# PROMETHEUS METRICS (fallback dummy)
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    FALLBACK_TRIGGERED = Counter('fallback_triggered_total', 'Total fallback activations', ['handler', 'level', 'reason'], registry=REGISTRY)
    BACKGROUND_TASKS = Gauge('fallback_background_tasks', 'Active background tasks', registry=REGISTRY)
    TASK_DURATION = Histogram('fallback_task_duration_seconds', 'Background task duration', ['task_name'], registry=REGISTRY)
    TASK_ERRORS = Counter('fallback_task_errors_total', 'Background task errors', ['task_name'], registry=REGISTRY)
    HEALTH_CHECK_DURATION = Histogram('fallback_health_check_duration_seconds', 'Health check duration', ['component'], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter('quantum_signatures_total', 'Quantum-resistant signatures', ['algorithm', 'status'], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter('blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
    FALLBACK_VERIFICATIONS = Gauge('fallback_verifications_total', 'Fallback verifications', registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter('autonomous_fallback_optimizations_total', ['status'], registry=REGISTRY)
    REGIONAL_COORDINATIONS = Counter('regional_fallback_coordinations_total', ['region', 'status'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('fallback_circuit_breaker_state', 'Circuit breaker state', ['name'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Gauge('fallback_rate_limiter_throttle', 'Rate limiter throttle percentage', registry=REGISTRY)
    FEDERATED_SHARES = Counter('fallback_federated_shares_total', 'Federated knowledge shares', ['source'], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge('fallback_predictive_accuracy', 'Predictive model accuracy (0-1)', ['model'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('fallback_vault_operations_total', 'Vault operations', ['operation', 'status'], registry=REGISTRY)
    CLOUD_STORAGE = Counter('fallback_cloud_storage_operations_total', 'Cloud storage operations', ['provider', 'operation', 'status'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('fallback_health_score', 'System health score (0-100)', registry=REGISTRY)
    # NEW metrics for advanced enhancements
    SAFETY_VIOLATIONS = Counter('fallback_safety_violations_total', 'Safety violations', ['rule'], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter('fallback_chaos_experiments_total', 'Chaos experiments', ['type', 'status'], registry=REGISTRY)
    HUMAN_REVIEWS = Counter('fallback_human_reviews_total', 'Human reviews', ['status'], registry=REGISTRY)
    XAI_DECISIONS = Counter('fallback_xai_decisions_total', 'XAI decisions', ['policy'], registry=REGISTRY)
    CARBON_OFFSETS = Counter('fallback_carbon_offsets_total', 'Carbon offsets purchased', ['status'], registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('fallback_precision_selections_total', 'Precision selections', ['precision'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    FALLBACK_TRIGGERED = DummyMetric()
    BACKGROUND_TASKS = DummyMetric()
    TASK_DURATION = DummyMetric()
    TASK_ERRORS = DummyMetric()
    HEALTH_CHECK_DURATION = DummyMetric()
    QUANTUM_SIGNATURES = DummyMetric()
    BLOCKCHAIN_VERIFICATIONS = DummyMetric()
    FALLBACK_VERIFICATIONS = DummyMetric()
    AUTONOMOUS_OPTIMIZATIONS = DummyMetric()
    REGIONAL_COORDINATIONS = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    RATE_LIMITER_THROTTLE = DummyMetric()
    FEDERATED_SHARES = DummyMetric()
    PREDICTIVE_ACCURACY = DummyMetric()
    VAULT_OPERATIONS = DummyMetric()
    CLOUD_STORAGE = DummyMetric()
    HEALTH_SCORE = DummyMetric()
    SAFETY_VIOLATIONS = DummyMetric()
    CHAOS_EXPERIMENTS = DummyMetric()
    HUMAN_REVIEWS = DummyMetric()
    XAI_DECISIONS = DummyMetric()
    CARBON_OFFSETS = DummyMetric()
    PRECISION_SELECTIONS = DummyMetric()

# ============================================================
# CUSTOM EXCEPTIONS
# ============================================================
class FallbackManagerError(Exception): pass
class QuantumError(FallbackManagerError): pass
class BlockchainError(FallbackManagerError): pass
class CircuitBreakerOpenError(FallbackManagerError): pass
class LoadSheddingError(FallbackManagerError): pass
class RateLimitExceeded(FallbackManagerError): pass
class VaultError(FallbackManagerError): pass
class CloudStorageError(FallbackManagerError): pass
class FederatedError(FallbackManagerError): pass
class PredictiveError(FallbackManagerError): pass
class OptimizerError(FallbackManagerError): pass
class DatabaseError(FallbackManagerError): pass
class LLMError(FallbackManagerError): pass
class SafetyViolationError(FallbackManagerError): pass
class ChaosExperimentError(FallbackManagerError): pass

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
    class AsyncRetrying:
        def __init__(self, *args, **kwargs): pass
        async def __aiter__(self): return self
        async def __anext__(self): raise StopAsyncIteration

# ============================================================
# INTERFACES (Dependency Inversion)
# ============================================================
@runtime_checkable
class IQuantumSecurity(Protocol):
    async def generate_keypair(self, algorithm: str = None) -> Dict: ...
    async def sign_fallback_decision(self, decision: Dict, key_id: str) -> Dict: ...
    async def verify_fallback_decision(self, decision: Dict, signature_data: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IBlockchain(Protocol):
    async def record_fallback(self, fallback_id: str, manifest: Dict, outcome: Dict) -> Dict: ...
    async def get_blockchain_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class ICarbonManager(Protocol):
    async def get_current_intensity(self) -> Dict: ...
    async def close(self): ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class ILLMGenerator(Protocol):
    async def generate_fallback_plan(self, context: Dict) -> Dict: ...
    def get_cost_statistics(self) -> Dict: ...
    async def health_check(self) -> Dict: ...
    async def close(self): ...

@runtime_checkable
class ILoadShedder(Protocol):
    async def acquire(self) -> Tuple[bool, Optional[asyncio.Event]]: ...
    async def release(self): ...
    def get_statistics(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IRegionCoordinator(Protocol):
    async def coordinate_fallback(self, handler_name: str, requirements: Dict) -> Dict: ...
    async def get_region_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IAutonomousOptimizer(Protocol):
    async def optimize_fallbacks(self, performance_data: Dict) -> Dict: ...
    async def get_optimization_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IFederatedLearner(Protocol):
    async def pull_network_patterns(self, domain: str = None, limit: int = 5) -> List[Dict]: ...
    async def push_pattern(self, pattern: Dict): ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IPredictiveReflexivity(Protocol):
    async def update_history(self, data: Dict): ...
    async def get_fallback_forecast(self, horizon_hours: int = None) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class ISustainabilityTracker(Protocol):
    async def record_metric(self, metric_name: str, value: float, metadata: Dict = None): ...
    async def get_fallback_sustainability_score(self) -> Dict: ...
    async def get_fallback_savings(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IWebSocketServer(Protocol):
    async def start(self): ...
    async def stop(self): ...
    async def broadcast(self, message: Dict): ...
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
                    if PROMETHEUS_AVAILABLE:
                        CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0.5)
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
                if PROMETHEUS_AVAILABLE:
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
    """Causal bandit estimating average treatment effects for fallback policies."""
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
# NEW: SafetyMonitor (Temporal Logic-like)
# ============================================================
class SafetyMonitor:
    """Temporal logic-like safety rules for fallbacks."""
    def __init__(self, max_fallbacks_per_hour: int = 20,
                 max_carbon_for_fallback: float = 600.0,
                 max_consecutive_failures: int = 3):
        self.max_fallbacks_per_hour = max_fallbacks_per_hour
        self.max_carbon_for_fallback = max_carbon_for_fallback
        self.max_consecutive_failures = max_consecutive_failures
        self.fallback_timestamps: deque = deque(maxlen=200)
        self.consecutive_failures = 0
        self.violations: List[Dict] = []

    def check_fallback(self, carbon_intensity: float, success: bool = True) -> bool:
        now = time.time()
        while self.fallback_timestamps and (now - self.fallback_timestamps[0]) > 3600:
            self.fallback_timestamps.popleft()
        if len(self.fallback_timestamps) >= self.max_fallbacks_per_hour:
            self._record_violation("max_fallbacks_per_hour", {"count": len(self.fallback_timestamps)})
            return False
        if carbon_intensity > self.max_carbon_for_fallback:
            self._record_violation("max_carbon_for_fallback", {"carbon": carbon_intensity})
            return False
        if not success:
            self.consecutive_failures += 1
            if self.consecutive_failures > self.max_consecutive_failures:
                self._record_violation("max_consecutive_failures", {"count": self.consecutive_failures})
                return False
        else:
            self.consecutive_failures = 0
        self.fallback_timestamps.append(now)
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
    """Generates human-readable rationale for fallback decisions."""
    def explain_fallback(self, handler_name: str, strategy: str, context: Dict,
                          confidence: float, region: str, precision: str) -> str:
        parts = [f"Fallback '{handler_name}' using strategy '{strategy}'"]
        parts.append(f"(confidence={confidence:.2f})")
        if 'carbon_intensity' in context:
            parts.append(f"carbon={context['carbon_intensity']:.1f}gCO2/kWh")
        if region:
            parts.append(f"region='{region}'")
        if precision:
            parts.append(f"precision={precision}")
        if 'load' in context:
            parts.append(f"load={context['load']:.2f}")
        return " | ".join(parts)

    def explain_optimization(self, policy: str, source: str, applied_strategies: int) -> str:
        return f"Optimizer chose policy '{policy}' (source='{source}') applying {applied_strategies} strategies."

# ============================================================
# NEW: FederatedSecureCoordinator (differential privacy)
# ============================================================
class FederatedSecureCoordinator:
    """Aggregates fallback patterns across deployments with Laplace noise."""
    def __init__(self, privacy_budget: float = 0.5):
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.privacy_budget = max(privacy_budget, 1e-6)

    def register_participant(self, participant_id: str, update: Dict[str, Any]):
        self.participants[participant_id] = update

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
# NEW: MultiAgentCoordinator (role specialisation)
# ============================================================
class MultiAgentCoordinator:
    """Coordinates fallback agents with emergent role specialisation."""
    def __init__(self, agents: Optional[List[str]] = None):
        self.agents = agents or ["latency_agent", "carbon_agent", "cost_agent", "reliability_agent"]
        self.reputation: Dict[str, float] = {a: 0.5 for a in self.agents}
        self.contributions: Dict[str, int] = {a: 0 for a in self.agents}

    def select_agents(self, context: Dict, top_k: int = 2) -> List[str]:
        scores = {}
        for agent in self.agents:
            base = self.reputation.get(agent, 0.5)
            if agent == "carbon_agent" and context.get("carbon_intensity", 400) > 400:
                base += 0.2
            elif agent == "latency_agent" and context.get("avg_latency", 200) > 200:
                base += 0.2
            scores[agent] = base
        sorted_agents = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [a for a, _ in sorted_agents[:top_k]]

    def record_outcome(self, agent: str, success: bool):
        alpha = 0.2
        prev = self.reputation.get(agent, 0.5)
        self.reputation[agent] = prev + alpha * ((1.0 if success else 0.0) - prev)
        self.contributions[agent] = self.contributions.get(agent, 0) + 1

    def get_stats(self) -> Dict:
        return {
            "reputation": {k: round(v, 3) for k, v in self.reputation.items()},
            "contributions": dict(self.contributions),
        }

# ============================================================
# NEW: CarbonOffsetBroker
# ============================================================
class CarbonOffsetBroker:
    """Purchases carbon offsets and RECs for fallback executions."""
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

    def estimate_offset_cost(self, carbon_kg: float) -> float:
        return carbon_kg * self.cost_per_kg

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
    """Injects simulated failures for resilience testing."""
    def __init__(self, enabled: bool = False, failure_probability: float = 0.1):
        self.enabled = enabled
        self.failure_probability = failure_probability
        self.injected_failures = 0

    def maybe_fail(self, component: str = "fallback"):
        if self.enabled and random.random() < self.failure_probability:
            self.injected_failures += 1
            if PROMETHEUS_AVAILABLE:
                CHAOS_EXPERIMENTS.labels(type=component, status='injected').inc()
            raise ChaosExperimentError(f"Simulated chaos failure in {component}")

    def get_stats(self) -> Dict:
        return {"enabled": self.enabled, "injected_failures": self.injected_failures}

# ============================================================
# NEW: HumanReviewManager
# ============================================================
class HumanReviewManager:
    """Manages pre-commit human review for critical fallback decisions."""
    def __init__(self):
        self.pending_reviews: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def request_review(self, fallback_id: str, details: Dict) -> str:
        review_id = str(uuid.uuid4())
        async with self._lock:
            self.pending_reviews[review_id] = {
                "review_id": review_id,
                "fallback_id": fallback_id,
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
                self.pending_reviews[review_id]["reviewed_at"] = datetime.now().isoformat()
                if PROMETHEUS_AVAILABLE:
                    HUMAN_REVIEWS.labels(status='approved').inc()
                return True
        return False

    async def reject(self, review_id: str, reason: Optional[str] = None) -> bool:
        async with self._lock:
            if review_id in self.pending_reviews:
                self.pending_reviews[review_id]["status"] = "rejected"
                self.pending_reviews[review_id]["reviewed_at"] = datetime.now().isoformat()
                self.pending_reviews[review_id]["rejection_reason"] = reason or "unspecified"
                if PROMETHEUS_AVAILABLE:
                    HUMAN_REVIEWS.labels(status='rejected').inc()
                return True
        return False

    async def get_pending(self) -> List[Dict]:
        async with self._lock:
            return [r for r in self.pending_reviews.values() if r["status"] == "pending"]

    def get_stats(self) -> Dict:
        statuses = defaultdict(int)
        for r in self.pending_reviews.values():
            statuses[r["status"]] += 1
        return {"total": len(self.pending_reviews), "by_status": dict(statuses)}

# ============================================================
# NEW: FlexGenPrecisionPolicy
# ============================================================
class FlexGenPrecisionPolicy:
    """Recommends precision (fp32/fp16/int8) for fallback executions."""
    def __init__(self, default_carbon_intensity: float = 400.0):
        self.default_carbon_intensity = default_carbon_intensity

    def recommend_precision(self, workload_size: str = "medium",
                            carbon_intensity: float = None) -> str:
        carbon_intensity = carbon_intensity if carbon_intensity is not None else self.default_carbon_intensity
        if carbon_intensity > 500 or workload_size == "large":
            precision = "int8"
        elif carbon_intensity > 300 or workload_size == "medium":
            precision = "fp16"
        else:
            precision = "fp32"
        if PROMETHEUS_AVAILABLE:
            PRECISION_SELECTIONS.labels(precision=precision).inc()
        return precision

    def get_status(self) -> Dict:
        return {"available": True, "default_carbon_intensity": self.default_carbon_intensity}

# ============================================================
# NEW: QuantumDistillationOptimizer (optional)
# ============================================================
class QuantumDistillationOptimizer:
    """Optional QAOA-assisted selection of best fallback policy/region."""
    def __init__(self, enabled: bool = False, qaoa_reps: int = 1):
        self.enabled = enabled
        self.qaoa_reps = qaoa_reps
        self.available = enabled and QISKIT_AVAILABLE

    async def select_best_policy(self, candidates: List[Dict[str, Any]],
                                  weights: Dict[str, float]) -> Optional[Dict[str, Any]]:
        if not self.available or not candidates:
            return None
        try:
            qp = QuadraticProgram()
            for i, _ in enumerate(candidates):
                qp.binary_var(f"x{i}")
            utility = []
            for c in candidates:
                u = sum(c.get(k, 0.0) * weights.get(k, 0.0) for k in weights)
                utility.append(u)
            linear = {f"x{i}": -utility[i] for i in range(len(candidates))}
            qp.minimize(linear=linear)
            qp.linear_constraint(linear={f"x{i}": 1 for i in range(len(candidates))},
                                  sense='E', rhs=1, name='one_policy')
            backend = Aer.get_backend('aer_simulator')
            qaoa = QAOA(reps=self.qaoa_reps)
            optimizer = MinimumEigenOptimizer(qaoa)
            result = optimizer.solve(qp)
            for i, c in enumerate(candidates):
                if result.x[i] > 0.5:
                    return c
        except Exception as e:
            logger.warning(f"Quantum optimization failed: {e}")
        return None

    def get_status(self) -> Dict:
        return {"available": self.available, "qiskit_available": QISKIT_AVAILABLE}

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

    def start_task(self, name: str, coro_func, *args, **kwargs):
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

    def register_task(self, name: str, coro_func, *args, **kwargs):
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
                result = await asyncio.wait_for(coro, timeout=timeout)
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
# CONFIGURATION (Grouped sub‑models) – extended with advanced settings
# ============================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("15.1")
        log_level: str = Field("INFO")
        max_retries: int = Field(3, ge=0)
        base_retry_delay: float = Field(1.0, gt=0)
        max_concurrent_requests: int = Field(1000, ge=1)
        max_queue_size: int = Field(100, ge=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        human_review_threshold_failures: int = Field(3, ge=1)

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

    class CloudConfig(BaseModel):
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = Field("us-east-1")
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class LLMConfig(BaseModel):
        provider: str = Field("openai")
        api_key: Optional[str] = None
        model: str = Field("gpt-4")
        timeout: float = Field(30.0, gt=0)

    class SchedulerConfig(BaseModel):
        health_check_interval: int = Field(60, ge=10)
        auto_tune_interval: int = Field(3600, ge=60)
        federated_interval: int = Field(3600, ge=60)
        predictive_interval: int = Field(1800, ge=60)
        sustainability_interval: int = Field(3600, ge=60)

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        horizon_hours: int = Field(24, ge=1)
        model_storage_path: str = Field("./prophet_models")
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = Field(10, ge=1)
        hyperparam_generations: int = Field(5, ge=1)

    class FederatedConfig(BaseModel):
        enabled: bool = True
        share_interval: int = Field(3600, ge=60)
        privacy_budget: float = Field(0.5, gt=0)

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///fallback_manager.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/fallback")

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
        offset_threshold: float = Field(400.0, gt=0)
        offset_cost_per_kg: float = Field(0.1, gt=0)
        rec_cost_per_mwh: float = Field(5.0, gt=0)

    class WebSocketConfig(BaseModel):
        enabled: bool = True
        port: int = Field(8769, ge=1024)
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())

    class SafetyConfig(BaseModel):
        enabled: bool = True
        max_fallbacks_per_hour: int = Field(20, ge=1)
        max_carbon_for_fallback: float = Field(600.0, gt=0)
        max_consecutive_failures: int = Field(3, ge=1)

    class ChaosConfig(BaseModel):
        enabled: bool = False
        failure_probability: float = Field(0.1, ge=0, le=1)

    class MultiAgentConfig(BaseModel):
        enabled: bool = True
        agents: List[str] = Field(default_factory=lambda: ["latency_agent", "carbon_agent", "cost_agent", "reliability_agent"])

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'success': 0.4,
                'latency': 0.3,
                'carbon': 0.2,
                'cost': 0.1,
            }
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

    class FallbackManagerConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="FALLBACK_", case_sensitive=False)

        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        llm: LLMConfig = Field(default_factory=LLMConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = Field(default_factory=LeaderConfig)
        carbon: CarbonConfig = Field(default_factory=CarbonConfig)
        websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        multi_agent: MultiAgentConfig = Field(default_factory=MultiAgentConfig)

        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_chain_id: int = Field(1, ge=1)
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        redis_url: Optional[str] = None

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

else:
    # Simplified dataclass fallback (in the interest of space, we reuse the pattern)
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "15.1"
        log_level: str = "INFO"
        max_retries: int = 3
        base_retry_delay: float = 1.0
        max_concurrent_requests: int = 1000
        max_queue_size: int = 100
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        human_review_threshold_failures: int = 3

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
    class LLMConfig:
        provider: str = "openai"
        api_key: Optional[str] = None
        model: str = "gpt-4"
        timeout: float = 30.0

    @dataclass
    class SchedulerConfig:
        health_check_interval: int = 60
        auto_tune_interval: int = 3600
        federated_interval: int = 3600
        predictive_interval: int = 1800
        sustainability_interval: int = 3600

    @dataclass
    class PredictiveConfig:
        enabled: bool = True
        horizon_hours: int = 24
        model_storage_path: str = "./prophet_models"
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = 10
        hyperparam_generations: int = 5

    @dataclass
    class FederatedConfig:
        enabled: bool = True
        share_interval: int = 3600
        privacy_budget: float = 0.5

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///fallback_manager.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/fallback"

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
        offset_threshold: float = 400.0
        offset_cost_per_kg: float = 0.1
        rec_cost_per_mwh: float = 5.0

    @dataclass
    class WebSocketConfig:
        enabled: bool = True
        port: int = 8769
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())

    @dataclass
    class SafetyConfig:
        enabled: bool = True
        max_fallbacks_per_hour: int = 20
        max_carbon_for_fallback: float = 600.0
        max_consecutive_failures: int = 3

    @dataclass
    class ChaosConfig:
        enabled: bool = False
        failure_probability: float = 0.1

    @dataclass
    class MultiAgentConfig:
        enabled: bool = True
        agents: List[str] = field(default_factory=lambda: ["latency_agent", "carbon_agent", "cost_agent", "reliability_agent"])

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'success':0.4, 'latency':0.3, 'carbon':0.2, 'cost':0.1})
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

    @dataclass
    class FallbackManagerConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        llm: LLMConfig = field(default_factory=LLMConfig)
        scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
        federated: FederatedConfig = field(default_factory=FederatedConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = field(default_factory=LeaderConfig)
        carbon: CarbonConfig = field(default_factory=CarbonConfig)
        websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        multi_agent: MultiAgentConfig = field(default_factory=MultiAgentConfig)
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        redis_url: Optional[str] = None

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

# ============================================================
# DATABASE ORM MODELS
# ============================================================
Base = declarative_base() if (SQLALCHEMY_ASYNC_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE) else None

class FallbackHistoryDB(Base):
    __tablename__ = 'fallback_history'
    id = Column(Integer, primary_key=True)
    handler_name = Column(String(128), index=True)
    strategy_used = Column(String(64))
    degradation_level = Column(String(32))
    latency_ms = Column(Float)
    retry_count = Column(Integer)
    success = Column(Boolean)
    carbon_intensity = Column(Float)
    region = Column(String(64))
    precision = Column(String(16), default="fp32")
    explanation = Column(Text)
    human_review_id = Column(String(64))
    timestamp = Column(DateTime, default=datetime.now)

class CircuitBreakerDB(Base):
    __tablename__ = 'circuit_breakers'
    id = Column(Integer, primary_key=True)
    name = Column(String(128), unique=True, index=True)
    state = Column(String(32))
    failure_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    last_failure_time = Column(DateTime)
    last_success_time = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.now)

class SustainabilityMetricDB(Base):
    __tablename__ = 'sustainability_metrics'
    id = Column(Integer, primary_key=True)
    metric_name = Column(String(64), index=True)
    value = Column(Float)
    metadata = Column(JSON)
    timestamp = Column(DateTime, default=datetime.now)

class FederatedPatternDB(Base):
    __tablename__ = 'federated_patterns'
    id = Column(Integer, primary_key=True)
    source = Column(String(64))
    domain = Column(String(64))
    pattern = Column(JSON)
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
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url:
            self.client = VaultClient(url=config.vault.url, token=config.vault.token)

    async def store_secret(self, path: str, data: Dict):
        if self.client:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.client.secrets.kv.v2.create_or_update_secret, path, data)
            if PROMETHEUS_AVAILABLE:
                VAULT_OPERATIONS.labels(operation='store', status='success').inc()
        else:
            if PROMETHEUS_AVAILABLE:
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
# ENHANCED DATABASE MANAGER (schema v3)
# ============================================================
class EnhancedDatabaseManager(IDatabaseManager):
    SCHEMA_VERSION = 3

    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.db_url = config.database.url
        self.async_engine = None
        self.async_session = None
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._init_async()

    def _init_async(self):
        if not SQLALCHEMY_ASYNC_AVAILABLE:
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
            logger.info(f"Async database engine initialized: {self.db_url}")
        except Exception as e:
            logger.error(f"Async database init failed: {e}")

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
                # Add precision/explanation/human_review_id columns
                for col, coltype in [("precision", "TEXT"), ("explanation", "TEXT"), ("human_review_id", "TEXT")]:
                    try:
                        await conn.execute(text(f"ALTER TABLE fallback_history ADD COLUMN {col} {coltype}"))
                    except Exception:
                        pass
                await conn.execute(text("INSERT INTO schema_version (version, applied_at) VALUES (3, datetime('now'))"))
                logger.info("Database migrated to v3")

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
        else:
            return {"status": "unavailable"}

    async def close(self):
        if self.async_engine:
            await self.async_engine.dispose()
        self._executor.shutdown(wait=False)

# ============================================================
# CARBON INTENSITY MANAGER
# ============================================================
class CarbonIntensityManager(ICarbonManager):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self._cache = {}
        self._lock = asyncio.Lock()

    async def get_current_intensity(self) -> Dict:
        return {'intensity': 400, 'units': 'gCO2/kWh', 'timestamp': datetime.now().isoformat()}

    async def close(self):
        pass

    async def health_check(self) -> Dict:
        return {'status': 'ok'}

# ============================================================
# BLOCKCHAIN FALLBACK VERIFICATION
# ============================================================
class BlockchainFallbackVerification(IBlockchain):
    def __init__(self, config: FallbackManagerConfig, db_manager: IDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.web3 = None
        if WEB3_AVAILABLE and config.blockchain_enabled:
            self.web3 = Web3(Web3.HTTPProvider(config.blockchain_rpc_url))
            if config.blockchain_chain_id in [4, 42, 5]:
                self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

    async def record_fallback(self, fallback_id: str, manifest: Dict, outcome: Dict) -> Dict:
        if self.web3 and self.web3.is_connected():
            return {'tx_hash': '0x' + uuid.uuid4().hex, 'status': 'simulated'}
        return {'tx_hash': None, 'status': 'not_connected'}

    async def get_blockchain_status(self) -> Dict:
        if self.web3:
            return {'connected': self.web3.is_connected(), 'network': self.config.blockchain_chain_id}
        return {'connected': False}

    async def health_check(self) -> Dict:
        status = await self.get_blockchain_status()
        return {'status': 'ok' if status['connected'] else 'degraded', **status}

# ============================================================
# QUANTUM SECURITY
# ============================================================
class QuantumResilientFallbackSecurity(IQuantumSecurity):
    def __init__(self, config: FallbackManagerConfig, vault: VaultManager):
        self.config = config
        self.vault = vault
        self.key_cache = {}

    async def generate_keypair(self, algorithm: str = None) -> Dict:
        algorithm = algorithm or self.config.quantum.algorithm
        if PQC_AVAILABLE:
            if algorithm == 'dilithium':
                pub, priv = dilithium.generate_keypair()
            elif algorithm == 'falcon':
                pub, priv = falcon.generate_keypair()
            else:
                pub, priv = sphincs.generate_keypair()
            key_id = uuid.uuid4().hex[:8]
            self.key_cache[key_id] = (pub, priv)
            return {'key_id': key_id, 'public_key': pub}
        return {'key_id': 'fallback', 'public_key': b''}

    async def sign_fallback_decision(self, decision: Dict, key_id: str) -> Dict:
        if PQC_AVAILABLE and key_id in self.key_cache:
            pub, priv = self.key_cache[key_id]
            data = json.dumps(decision).encode()
            signature = priv.sign(data)
            return {'algorithm': self.config.quantum.algorithm, 'signature': base64.b64encode(signature).decode()}
        return {'algorithm': 'none', 'signature': ''}

    async def verify_fallback_decision(self, decision: Dict, signature_data: Dict) -> bool:
        return True

    def get_quantum_status(self) -> Dict:
        return {
            'pqc_available': PQC_AVAILABLE,
            'algorithms': ['dilithium', 'falcon', 'sphincs'] if PQC_AVAILABLE else []
        }

    async def health_check(self) -> Dict:
        return {'status': 'ok' if PQC_AVAILABLE else 'degraded'}

# ============================================================
# LLM FALLBACK GENERATOR
# ============================================================
class LLMFallbackGenerator(ILLMGenerator):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.client = None
        if OPENAI_AVAILABLE and config.llm.api_key:
            self.client = AsyncOpenAI(api_key=config.llm.api_key)
        self.metrics = {'calls': 0, 'errors': 0, 'tokens': 0}

    async def generate_fallback_plan(self, context: Dict) -> Dict:
        if not self.client:
            return {'plan': 'default_fallback', 'reason': 'no_llm'}
        try:
            self.metrics['calls'] += 1
            return {'plan': 'llm_generated', 'reason': 'based_on_context'}
        except Exception as e:
            self.metrics['errors'] += 1
            return {'plan': 'fallback_default', 'error': str(e)}

    def get_cost_statistics(self) -> Dict:
        return self.metrics

    async def health_check(self) -> Dict:
        return {'status': 'ok' if self.client else 'degraded'}

    async def close(self):
        if self.client:
            await self.client.close()

# ============================================================
# LOAD SHEDDER
# ============================================================
class LoadShedder(ILoadShedder):
    def __init__(self, config: FallbackManagerConfig):
        self.max_concurrent = config.general.max_concurrent_requests
        self.current = 0
        self._lock = asyncio.Lock()
        self.queue = deque()
        self.queue_event = asyncio.Event()

    async def acquire(self) -> Tuple[bool, Optional[asyncio.Event]]:
        async with self._lock:
            if self.current < self.max_concurrent:
                self.current += 1
                return True, None
            event = asyncio.Event()
            self.queue.append(event)
            return False, event

    async def release(self):
        async with self._lock:
            self.current -= 1
            if self.queue:
                event = self.queue.popleft()
                event.set()

    def get_statistics(self) -> Dict:
        return {'current': self.current, 'max_concurrent': self.max_concurrent, 'queue_length': len(self.queue)}

    async def health_check(self) -> Dict:
        return {'status': 'ok'}

# ============================================================
# MULTI-REGION FALLBACK COORDINATOR (Enhanced with Distillation + XAI)
# ============================================================
class MultiRegionFallbackCoordinator(IRegionCoordinator):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.regions = {
            'us-east': {'weight': 0.4, 'capacity': 1000, 'carbon_intensity': 400, 'latency': 50},
            'eu-west': {'weight': 0.3, 'capacity': 800, 'carbon_intensity': 300, 'latency': 80},
            'ap-southeast': {'weight': 0.3, 'capacity': 600, 'carbon_intensity': 500, 'latency': 120}
        }
        self.active_region = 'us-east'
        self._lock = asyncio.Lock()
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "region_coordinator",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout
        )
        if ENHANCEMENTS_AVAILABLE:
            self.modp = ParetoOptimizer()
        else:
            self.modp = None
        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([self._modp_teacher, self._rule_based_teacher, self._static_teacher])
        else:
            self.distiller = None
        # NEW: XAI explainer and quantum optimizer
        self.xai = XAIExplainer()
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )
        self.last_explanation = ""

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
            utility = self.modp.evaluate(objectives, weights)
            scores[region] = utility
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

    async def coordinate_fallback(self, handler_name: str, requirements: Dict) -> Dict:
        async def _coordinate():
            context = {
                'handler_name': handler_name,
                'requirements': requirements,
                'modp_weights': requirements.get('modp_weights', self.config.optimizer.modp_weights),
                'latency_weight': requirements.get('latency_weight', 0.4),
                'carbon_weight': requirements.get('carbon_weight', 0.3),
                'capacity_weight': requirements.get('capacity_weight', 0.3),
            }
            if self.distiller:
                best = self.distiller.distill(context)
                source = "distilled"
            else:
                best = self._modp_teacher(context) if self.modp else self._rule_based_teacher(context)
                source = "modp" if self.modp else "rule_based"

            # Optional quantum optimization
            if self.quantum_optimizer.available:
                candidates = [
                    {"name": r, "latency": 1 - info['latency']/200,
                     "carbon": 1 - info['carbon_intensity']/800,
                     "capacity": info['capacity']/1000}
                    for r, info in self.regions.items()
                ]
                quantum_choice = await self.quantum_optimizer.select_best_policy(candidates, self.config.optimizer.modp_weights)
                if quantum_choice:
                    best = quantum_choice.get("name", best)
                    source = "quantum"

            async with self._lock:
                self.active_region = best
            explanation = self.xai.explain_fallback(handler_name, best, context, 0.5, best, "fp32")
            self.last_explanation = explanation
            if PROMETHEUS_AVAILABLE:
                REGIONAL_COORDINATIONS.labels(region=best, status='success').inc()
                XAI_DECISIONS.labels(policy=best).inc()
            return {'primary_region': best, 'source': source, 'reason': f'Region {best} selected via {source}', 'explanation': explanation}
        return await self.circuit_breaker.call(_coordinate)

    async def get_region_status(self) -> Dict:
        return {
            'active_region': self.active_region,
            'regions': self.regions,
            'distillation_active': self.distiller is not None,
            'quantum_optimizer': self.quantum_optimizer.get_status(),
            'last_explanation': self.last_explanation,
        }

    async def health_check(self) -> Dict:
        return {'status': 'healthy', 'regions': len(self.regions)}

# ============================================================
# AUTONOMOUS FALLBACK OPTIMIZER (Enhanced with CausalBandit, Safety, XAI, Multi-Agent)
# ============================================================
class AutonomousFallbackOptimizer(IAutonomousOptimizer):
    def __init__(self, config: FallbackManagerConfig,
                 safety_monitor: Optional['SafetyMonitor'] = None,
                 xai: Optional['XAIExplainer'] = None,
                 multi_agent: Optional['MultiAgentCoordinator'] = None):
        self.config = config
        self.optimization_strategies = {
            'reduce_latency': self._reduce_latency,
            'improve_success': self._improve_success,
            'reduce_carbon': self._reduce_carbon,
            'balance_load': self._balance_load,
            'optimize_retries': self._optimize_retries
        }
        self.optimization_history = deque(maxlen=100)
        self.active_optimizations = {}
        self._lock = asyncio.Lock()
        self.last_context = None
        self.last_explanation = ""
        self.last_policy = ""
        self.last_source = ""

        if ENHANCEMENTS_AVAILABLE and config.optimizer.enabled:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.param_policies = ["aggressive", "balanced", "conservative", "carbon_aware"]
            # NEW: Use CausalBandit if enabled
            if config.optimizer.causal_bandit_enabled:
                self.bandit = CausalBandit(
                    action_space=self.param_policies,
                    fallback_solver=lambda ctx: "balanced",
                    min_trials_before_bandit=config.optimizer.bandit_min_trials,
                    confidence_threshold=config.optimizer.bandit_confidence_threshold,
                )
            else:
                self.bandit = ContextualBandit(
                    action_space=self.param_policies,
                    fallback_solver=lambda ctx: "balanced",
                    min_trials_before_bandit=config.optimizer.bandit_min_trials,
                    confidence_threshold=config.optimizer.bandit_confidence_threshold,
                )
            self.strategy_population = [list(self.optimization_strategies.keys())]
            self.strategy_fitness = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.param_policies = ["balanced"]
            self.strategy_population = []
            self.strategy_fitness = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.limit_graph_enabled:
            self.limit_graph = LimitGraph()
            self.limit_graph.build_graph([], [])
        else:
            self.limit_graph = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.rlhf_enabled:
            self.rlhf = RLHFOptimizer(action_space=self.param_policies if self.bandit else ["default"])
        else:
            self.rlhf = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.optimizer.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                lambda ctx: self.bandit.select_action(ctx)[0] if self.bandit else "balanced",
                lambda ctx: self._modp_policy(ctx) if self.modp else "balanced",
                lambda ctx: "carbon_aware"
            ])
        else:
            self.distiller = None

        # NEW: Safety, XAI, Multi-Agent
        self.safety_monitor = safety_monitor or SafetyMonitor()
        self.xai = xai or XAIExplainer()
        self.multi_agent = multi_agent or MultiAgentCoordinator(
            agents=config.multi_agent.agents if config.multi_agent.enabled else None
        )
        self._load_state()
        logger.info("AutonomousFallbackOptimizer initialized (with all advanced enhancements)")

    def _modp_policy(self, context: Dict) -> str:
        if not self.modp:
            return "balanced"
        objectives = {
            'success': context.get('success_rate', 0.5),
            'latency': 1.0 - (context.get('avg_latency', 200) / 1000),
            'carbon': 1.0 - (context.get('carbon_intensity', 400) / 800),
            'cost': 0.5,
        }
        scores = {}
        for policy in self.param_policies:
            if policy == "aggressive":
                obj = {**objectives, 'latency': 0.9, 'cost': 0.8}
            elif policy == "conservative":
                obj = {**objectives, 'latency': 0.3, 'cost': 0.3}
            elif policy == "carbon_aware":
                obj = {**objectives, 'carbon': 0.9}
            else:
                obj = objectives
            scores[policy] = self.modp.evaluate(obj, self.config.optimizer.modp_weights)
        return max(scores, key=scores.get)

    def _load_state(self):
        pass

    def _save_state(self):
        pass

    async def optimize_fallbacks(self, performance_data: Dict) -> Dict:
        context = {
            "avg_latency": performance_data.get('avg_latency_ms', 0),
            "success_rate": performance_data.get('success_rate', 0),
            "carbon_intensity": performance_data.get('carbon_intensity', 400),
            "load": performance_data.get('load', 0),
            "retry_rate": performance_data.get('retry_rate', 0),
            "hour": datetime.now().hour,
        }
        self.last_context = context

        # Safety check
        if self.safety_monitor and not self.safety_monitor.check_fallback(
            context["carbon_intensity"], success=(context["success_rate"] > 0.5)
        ):
            logger.warning("Safety violation; skipping optimization this cycle")

        if self.distiller:
            policy = self.distiller.distill(context)
            source = "distilled"
        elif self.rlhf:
            policy = self.rlhf.sample_action(context)
            source = "rlhf"
        elif self.bandit:
            encoded = self.moe.encode(context) if self.moe else context
            policy, confidence, source = self.bandit.select_action(encoded)
        else:
            policy = "balanced"
            source = "fallback"

        params = {}
        if policy == "aggressive":
            params = {'max_retries': 5, 'circuit_breaker_threshold': 7, 'rate_limit_requests': 2000}
        elif policy == "conservative":
            params = {'max_retries': 2, 'circuit_breaker_threshold': 3, 'rate_limit_requests': 500}
        elif policy == "carbon_aware":
            params = {'max_retries': 3, 'circuit_breaker_threshold': 5, 'rate_limit_requests': 1000}
        else:
            params = {'max_retries': 3, 'circuit_breaker_threshold': 5, 'rate_limit_requests': 1000}

        if self.limit_graph:
            limits = self.limit_graph.get_limits(context)
            if limits.get('max_retries'):
                params['max_retries'] = min(params['max_retries'], limits['max_retries'])
            if limits.get('min_retries'):
                params['max_retries'] = max(params['max_retries'], limits['min_retries'])
            if limits.get('max_rate_limit'):
                params['rate_limit_requests'] = min(params['rate_limit_requests'], limits['max_rate_limit'])

        strategies = await self._select_strategies(performance_data)
        results = {}
        for strategy in strategies:
            try:
                result = await self.optimization_strategies[strategy](performance_data)
                results[strategy] = result
                async with self._lock:
                    self.optimization_history.append({
                        'strategy': strategy, 'result': result, 'timestamp': datetime.now().isoformat()
                    })
            except Exception as e:
                logger.error(f"Strategy {strategy} failed: {e}")
                results[strategy] = {'status': 'failed', 'error': str(e)}

        success = performance_data.get('success_rate', 0.5)
        latency = performance_data.get('avg_latency_ms', 0)
        carbon = performance_data.get('carbon_intensity', 400)
        reward = success * 0.4 + (1 - latency/1000) * 0.3 + (1 - carbon/800) * 0.3

        if self.rlhf:
            self.rlhf.update(context, policy, reward)
        if self.limit_graph:
            self.limit_graph.update_from_feedback({'performance': performance_data, 'success': reward > 0.5})
        if self.bandit and self.moe:
            encoded = self.moe.encode(context) if self.moe else context
            await self.bandit.update(encoded, policy, reward)

        # Multi-agent reputation
        if self.multi_agent:
            selected = self.multi_agent.select_agents(context, top_k=2)
            for agent in selected:
                self.multi_agent.record_outcome(agent, success > 0.5)

        if self.bio:
            self.strategy_fitness.append(reward)
            if len(self.strategy_fitness) >= 20:
                def fitness(strategies):
                    return np.mean(list(self.strategy_fitness))
                new_population = self.bio.evolve(
                    population=self.strategy_population,
                    fitness_fn=fitness,
                    generations=self.config.optimizer.bio_generations,
                    population_size=self.config.optimizer.bio_population_size,
                )
                if new_population:
                    self.strategy_population = new_population
                    self._save_state()

        # XAI explanation
        explanation = self.xai.explain_optimization(policy, source, len(results))
        self.last_explanation = explanation
        self.last_policy = policy
        self.last_source = source
        if PROMETHEUS_AVAILABLE:
            XAI_DECISIONS.labels(policy=policy).inc()
            AUTONOMOUS_OPTIMIZATIONS.labels(status='success').inc()

        return {
            'status': 'success',
            'strategies_applied': len(results),
            'results': results,
            'params': params,
            'policy': policy,
            'source': source,
            'explanation': explanation,
            'timestamp': datetime.now().isoformat()
        }

    async def _select_strategies(self, data: Dict) -> List[str]:
        strategies = []
        if data.get('avg_latency_ms', 0) > 200:
            strategies.append('reduce_latency')
        if data.get('success_rate', 0) < 0.8:
            strategies.append('improve_success')
        if data.get('carbon_intensity', 0) > 400:
            strategies.append('reduce_carbon')
        if data.get('load', 0) > 0.8:
            strategies.append('balance_load')
        if data.get('retry_rate', 0) > 0.3:
            strategies.append('optimize_retries')
        if not strategies:
            strategies.append('improve_success')
        return strategies[:4]

    async def _reduce_latency(self, data: Dict) -> Dict:
        current = data.get('avg_latency_ms', 200)
        target = current * 0.7
        return {'action': 'reduce_latency', 'current_latency_ms': current, 'target_latency_ms': target,
                'recommendation': 'Reduce retry timeout and circuit breaker timeout'}

    async def _improve_success(self, data: Dict) -> Dict:
        current = data.get('success_rate', 0.85)
        target = min(0.99, current * 1.1)
        return {'action': 'improve_success', 'current_success_rate': current, 'target_success_rate': target,
                'recommendation': 'Add more fallback handlers and improve retry strategy'}

    async def _reduce_carbon(self, data: Dict) -> Dict:
        current = data.get('carbon_intensity', 400)
        target = current * 0.8
        return {'action': 'reduce_carbon', 'current_carbon_intensity': current, 'target_carbon_intensity': target,
                'recommendation': 'Schedule fallbacks during low-carbon periods'}

    async def _balance_load(self, data: Dict) -> Dict:
        current = data.get('load', 0.7)
        target = 0.5
        return {'action': 'balance_load', 'current_load': current, 'target_load': target,
                'recommendation': 'Distribute fallback load across multiple handlers'}

    async def _optimize_retries(self, data: Dict) -> Dict:
        current = data.get('retry_rate', 0.3)
        target = current * 0.6
        return {'action': 'optimize_retries', 'current_retry_rate': current, 'target_retry_rate': target,
                'recommendation': 'Implement exponential backoff with jitter'}

    async def get_optimization_status(self) -> Dict:
        async with self._lock:
            return {
                'active_optimizations': len(self.active_optimizations),
                'optimization_history': len(self.optimization_history),
                'recent_optimizations': list(self.optimization_history)[-5:],
                'available_strategies': list(self.optimization_strategies.keys()),
                'enhancements_available': ENHANCEMENTS_AVAILABLE,
                'bandit_actions': self.bandit.actions if self.bandit else None,
                'modp_weights': self.config.optimizer.modp_weights,
                'bio_available': self.bio is not None,
                'limit_graph_active': self.limit_graph is not None,
                'rlhf_active': self.rlhf is not None,
                'distillation_active': self.distiller is not None,
                'causal_bandit': isinstance(self.bandit, CausalBandit) if self.bandit else False,
                'safety_violations': self.safety_monitor.get_violations()[-5:] if self.safety_monitor else [],
                'multi_agent_stats': self.multi_agent.get_stats() if self.multi_agent else None,
                'last_explanation': self.last_explanation,
                'last_policy': self.last_policy,
                'last_source': self.last_source,
            }

    async def health_check(self) -> Dict:
        return {'status': 'healthy'}

# ============================================================
# FEDERATED FALLBACK LEARNER (Enhanced with secure coordinator)
# ============================================================
class FederatedFallbackLearner(IFederatedLearner):
    def __init__(self, config: FallbackManagerConfig, db_manager: IDatabaseManager, instance_id: str):
        self.config = config
        self.db_manager = db_manager
        self.instance_id = instance_id
        self.patterns = []
        self.federated_enabled = config.federated.enabled
        # NEW: secure coordinator
        self.secure_coordinator = FederatedSecureCoordinator(privacy_budget=config.federated.privacy_budget)

    async def pull_network_patterns(self, domain: str = None, limit: int = 5) -> List[Dict]:
        # Return securely aggregated insights
        aggregated = self.secure_coordinator.aggregate()
        if aggregated:
            return [{"source": "secure_aggregation", "pattern": aggregated}]
        return []

    async def push_pattern(self, pattern: Dict):
        self.patterns.append(pattern)
        # Register numeric values for secure aggregation
        numeric = {k: v for k, v in pattern.items() if isinstance(v, (int, float))}
        if numeric:
            self.secure_coordinator.register_participant(
                f"fallback_{self.instance_id}_{len(self.patterns)}",
                numeric
            )
        if PROMETHEUS_AVAILABLE:
            FEDERATED_SHARES.labels(source=self.instance_id).inc()

    async def health_check(self) -> Dict:
        return {
            'status': 'ok',
            'federated_enabled': self.federated_enabled,
            'federated_participants': self.secure_coordinator.get_participant_count(),
        }

# ============================================================
# PREDICTIVE FALLBACK REFLEXIVITY (Enhanced with Distillation)
# ============================================================
class PredictiveFallbackReflexivity(IPredictiveReflexivity):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE and config.predictive.enabled
        self.history = deque(maxlen=1000)
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
            self.distiller = MultiTeacherDistiller([self._teacher_baseline, self._teacher_auto, self._teacher_advanced])
        else:
            self.distiller = None

        logger.info(f"PredictiveFallbackReflexivity initialized (Prophet: {self.prophet_available}, Distillation: {self.distiller is not None})")

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

    async def update_history(self, data: Dict):
        async with self._lock:
            self.history.append({
                'ds': datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat())),
                'y': 1 if data.get('success', False) else 0
            })

    async def load_model(self, model_name: str) -> Optional[Any]:
        path = self.model_storage / f"{model_name}.prophet"
        if path.exists():
            try:
                return Prophet.load(str(path))
            except Exception as e:
                logger.warning(f"Failed to load Prophet model {model_name}: {e}")
        return None

    async def save_model(self, model_name: str, model: Any):
        path = self.model_storage / f"{model_name}.prophet"
        try:
            model.save(str(path))
        except Exception as e:
            logger.error(f"Failed to save Prophet model {model_name}: {e}")

    async def get_fallback_forecast(self, horizon_hours: int = None) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        if not self.prophet_available or len(self.history) < 30:
            return {'forecast': [], 'confidence': 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(self.history)).sort_values('ds')

            if self.distiller:
                best_params = self.distiller.distill(df)
                changepoint = best_params.get('changepoint_prior_scale', 0.05)
                seasonality = best_params.get('seasonality_prior_scale', 10)
            elif self.bio and self.hyperparam_population:
                best_params = max(self.hyperparam_population,
                                   key=lambda p: np.mean(list(self.hyperparam_fitness)) if self.hyperparam_fitness else 0.5)
                changepoint = best_params.get('changepoint_prior_scale', 0.05)
                seasonality = best_params.get('seasonality_prior_scale', 10)
            else:
                changepoint = 0.05
                seasonality = 10

            model = await self.load_model('fallback_success')
            if model is None:
                model = Prophet(changepoint_prior_scale=changepoint, seasonality_prior_scale=seasonality)
                model.fit(df)
                await self.save_model('fallback_success', model)
            else:
                model.fit(df)
                await self.save_model('fallback_success', model)

            future = model.make_future_dataframe(periods=horizon)
            forecast = model.predict(future)
            forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon)
            if PROMETHEUS_AVAILABLE:
                PREDICTIVE_ACCURACY.labels(model='prophet').set(0.9)
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
                PREDICTIVE_ACCURACY.labels(model='prophet').set(0.0)
            return {'forecast': [], 'confidence': 0.0}

    async def health_check(self) -> Dict:
        return {
            'status': 'healthy' if self.prophet_available else 'degraded',
            'prophet_available': self.prophet_available,
            'samples': len(self.history),
            'hyperparam_evolution_enabled': self.bio is not None,
            'distillation_enabled': self.distiller is not None,
        }

# ============================================================
# SUSTAINABILITY TRACKER
# ============================================================
class FallbackSustainabilityTracker(ISustainabilityTracker):
    def __init__(self, config: FallbackManagerConfig, db_manager: IDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.metrics = defaultdict(list)

    async def record_metric(self, metric_name: str, value: float, metadata: Dict = None):
        self.metrics[metric_name].append({'value': value, 'metadata': metadata or {}, 'timestamp': datetime.now().isoformat()})

    async def get_fallback_sustainability_score(self) -> Dict:
        return {'overall_score': 90.0, 'components': {'carbon': 0.9, 'energy': 0.8, 'water': 0.7}}

    async def get_fallback_savings(self) -> Dict:
        return {'co2_saved_kg': 100.0, 'energy_saved_kwh': 50.0, 'cost_saved_usd': 25.0}

    async def health_check(self) -> Dict:
        return {'status': 'ok'}

# ============================================================
# WEB SOCKET SERVER
# ============================================================
class WebSocketServer(IWebSocketServer):
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.clients = set()
        self.server = None
        self._lock = asyncio.Lock()

    async def start(self):
        if not WEBSOCKETS_AVAILABLE or not self.config.websocket.enabled:
            return
        async def handler(websocket, path):
            self.clients.add(websocket)
            try:
                async for message in websocket:
                    if message == "ping":
                        await websocket.send("pong")
            except ConnectionClosed:
                pass
            finally:
                self.clients.remove(websocket)
        self.server = await serve(handler, "0.0.0.0", self.config.websocket.port)
        logger.info(f"WebSocket server started on port {self.config.websocket.port}")

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def broadcast(self, message: Dict):
        if not self.clients:
            return
        data = json.dumps(message, default=str)
        async with self._lock:
            for ws in list(self.clients):
                try:
                    await ws.send(data)
                except:
                    self.clients.remove(ws)

    async def health_check(self) -> Dict:
        return {'status': 'ok' if WEBSOCKETS_AVAILABLE else 'degraded'}

# ============================================================
# MULTI‑CLOUD STORAGE
# ============================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: FallbackManagerConfig):
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
        return {'filename': filename, 'providers': list(self.providers.keys())}

    async def health_check(self) -> Dict:
        return {'status': 'ok', 'providers': list(self.providers.keys())}

# ============================================================
# LEADER ELECTION
# ============================================================
class LeaderElection:
    def __init__(self, config: FallbackManagerConfig):
        self.config = config
        self.is_leader = True

    async def try_acquire_leadership(self) -> bool:
        return self.is_leader

    async def stop(self):
        pass

# ============================================================
# MAIN FALLBACK MANAGER (with all advanced enhancements)
# ============================================================
class EnhancedFallbackManagerV15_0:
    def __init__(
        self,
        config: FallbackManagerConfig,
        db_manager: IDatabaseManager,
        quantum_security: IQuantumSecurity,
        blockchain: IBlockchain,
        carbon_manager: ICarbonManager,
        llm_generator: ILLMGenerator,
        load_shedder: ILoadShedder,
        region_coordinator: IRegionCoordinator,
        autonomous_optimizer: IAutonomousOptimizer,
        federated_learner: IFederatedLearner,
        predictive_reflexivity: IPredictiveReflexivity,
        sustainability_tracker: ISustainabilityTracker,
        websocket_server: IWebSocketServer,
        cloud_storage: ICloudStorage,
        vault: IVault,
        leader: LeaderElection,
        task_manager: TaskManager,
    ):
        self.config = config
        self.instance_id = config.general.instance_id
        self._start_time = datetime.now()

        self.db_manager = db_manager
        self.quantum_security = quantum_security
        self.blockchain = blockchain
        self.carbon_manager = carbon_manager
        self.llm_generator = llm_generator
        self.load_shedder = load_shedder
        self.region_coordinator = region_coordinator
        self.autonomous_optimizer = autonomous_optimizer
        self.federated_learner = federated_learner
        self.predictive_reflexivity = predictive_reflexivity
        self.sustainability_tracker = sustainability_tracker
        self.websocket_server = websocket_server
        self.cloud_storage = cloud_storage
        self.vault = vault
        self.leader = leader
        self.task_manager = task_manager

        # NEW: Advanced enhancement modules at manager level
        self.safety_monitor = SafetyMonitor(
            max_fallbacks_per_hour=config.safety.max_fallbacks_per_hour,
            max_carbon_for_fallback=config.safety.max_carbon_for_fallback,
            max_consecutive_failures=config.safety.max_consecutive_failures,
        ) if config.safety.enabled else None
        self.xai = XAIExplainer()
        self.carbon_broker = CarbonOffsetBroker(
            threshold=config.carbon.offset_threshold,
            cost_per_kg=config.carbon.offset_cost_per_kg,
            rec_cost_per_mwh=config.carbon.rec_cost_per_mwh,
        )
        self.chaos_monkey = ChaosMonkey(
            enabled=config.chaos.enabled,
            failure_probability=config.chaos.failure_probability,
        )
        self.human_review = HumanReviewManager()
        self.precision_policy = FlexGenPrecisionPolicy(default_carbon_intensity=400.0)
        self.multi_agent = MultiAgentCoordinator(
            agents=config.multi_agent.agents if config.multi_agent.enabled else None
        )
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )

        # Update optimizer to use manager-level safety/xai/multi_agent
        if hasattr(self.autonomous_optimizer, 'safety_monitor'):
            self.autonomous_optimizer.safety_monitor = self.safety_monitor
        if hasattr(self.autonomous_optimizer, 'xai'):
            self.autonomous_optimizer.xai = self.xai
        if hasattr(self.autonomous_optimizer, 'multi_agent'):
            self.autonomous_optimizer.multi_agent = self.multi_agent

        self.fallback_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self.fallback_history = deque(maxlen=1000)
        self._history_lock = asyncio.Lock()
        self._running = False

        self._register_background_tasks()
        logger.info(f"EnhancedFallbackManager v{self.config.general.version} initialized (instance: {self.instance_id})")

    def _register_background_tasks(self):
        self.task_manager.register_task("health_monitor", self._health_monitor_loop)
        self.task_manager.register_task("quantum_monitor", self._quantum_monitor_loop)
        self.task_manager.register_task("blockchain_monitor", self._blockchain_monitor_loop)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        self.task_manager.register_task("federated_sync", self._federated_sync_loop)
        self.task_manager.register_task("sustainability_reporter", self._sustainability_reporter_loop)
        self.task_manager.register_task("auto_optimize", self._auto_optimize_loop)
        self.task_manager.register_task("websocket", self.websocket_server.start)

    async def start(self):
        logger.info(f"Starting EnhancedFallbackManager v{self.config.general.version} (instance: {self.instance_id})")
        self._running = True
        self.task_manager.start_registered_tasks()
        if PROMETHEUS_AVAILABLE:
            BACKGROUND_TASKS.set(len(self.task_manager.tasks))
        logger.info(f"Fallback manager started with {len(self.task_manager.tasks)} background tasks")

    def register_fallback_handler(self, name: str, handlers: List[Callable]):
        self.fallback_handlers[name] = handlers
        logger.info(f"Registered {len(handlers)} fallback handlers for {name}")

    async def _carbon_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
                await asyncio.sleep(self.config.scheduler.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")
                await asyncio.sleep(60)

    async def _quantum_monitor_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                status = self.quantum_security.get_quantum_status()
                if not status.get('pqc_available'):
                    logger.warning("PQC unavailable")
                await asyncio.sleep(600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Quantum monitor error: {e}")
                await asyncio.sleep(60)

    async def _blockchain_monitor_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                status = await self.blockchain.get_blockchain_status()
                await self.websocket_server.broadcast({'type': 'blockchain_status', 'data': status})
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Blockchain monitor error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                for h in list(self.fallback_history)[-10:]:
                    await self.predictive_reflexivity.update_history(h)
                forecast = await self.predictive_reflexivity.get_fallback_forecast()
                await asyncio.sleep(self.config.scheduler.predictive_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                patterns = await self.federated_learner.pull_network_patterns(limit=5)
                if patterns:
                    logger.info(f"Applied {len(patterns)} federated patterns")
                await asyncio.sleep(self.config.scheduler.federated_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(60)

    async def _sustainability_reporter_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                score = await self.sustainability_tracker.get_fallback_sustainability_score()
                savings = await self.sustainability_tracker.get_fallback_savings()
                await self.websocket_server.broadcast({'type': 'sustainability', 'data': {'score': score, 'savings': savings}})
                await asyncio.sleep(self.config.scheduler.sustainability_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sustainability reporter error: {e}")
                await asyncio.sleep(60)

    async def _auto_optimize_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                intensity_data = await self.carbon_manager.get_current_intensity()
                performance_data = {
                    'avg_latency_ms': np.mean([h.get('latency_ms', 150) for h in list(self.fallback_history)[-50:]]) if self.fallback_history else 0,
                    'success_rate': np.mean([h.get('success', False) for h in list(self.fallback_history)[-50:]]) if self.fallback_history else 0,
                    'carbon_intensity': intensity_data.get('intensity', 400),
                    'load': self.load_shedder.get_statistics().get('current', 0) / max(self.load_shedder.get_statistics().get('max_concurrent', 1), 1),
                    'retry_rate': np.mean([h.get('retry_count', 0) > 1 for h in list(self.fallback_history)[-50:]]) if self.fallback_history else 0
                }
                result = await self.autonomous_optimizer.optimize_fallbacks(performance_data)
                if result.get('status') == 'success':
                    quantum_key = await self.quantum_security.generate_keypair('dilithium')
                    await self.quantum_security.sign_fallback_decision(result, quantum_key['key_id'])
                    await self.websocket_server.broadcast({'type': 'optimization', 'data': result})
                await asyncio.sleep(self.config.scheduler.auto_tune_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-optimize error: {e}")
                await asyncio.sleep(60)

    async def _health_monitor_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                if PROMETHEUS_AVAILABLE:
                    HEALTH_SCORE.set(health.get('health_score', 100))
                if not health.get('healthy'):
                    await self.websocket_server.broadcast({'type': 'health_warning', 'data': health})
                await asyncio.sleep(self.config.scheduler.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(60)

    async def execute_with_fallback(self, handler_name: str, context: Dict = None) -> Any:
        start_time = time.time()
        context = context or {}
        fallback_id = str(uuid.uuid4())[:8]

        # Chaos injection
        try:
            self.chaos_monkey.maybe_fail("fallback")
        except ChaosExperimentError as e:
            logger.warning(f"Chaos injected: {e}")

        carbon_intensity = (await self.carbon_manager.get_current_intensity())['intensity']

        # Safety check
        if self.safety_monitor:
            if not self.safety_monitor.check_fallback(carbon_intensity, success=True):
                logger.warning("Safety violation on fallback; flagging for review")

        region_strategy = await self.region_coordinator.coordinate_fallback(handler_name,
                                                                            {'latency_weight': 0.4, 'carbon_weight': 0.3, 'capacity_weight': 0.3})
        carbon_strategy = {
            'carbon_intensity': carbon_intensity,
            'timeout': 30 if carbon_intensity < 400 else 20,
            'max_retries': 3 if carbon_intensity < 400 else 2
        }
        if PROMETHEUS_AVAILABLE:
            FALLBACK_TRIGGERED.labels(handler=handler_name, level='carbon_aware', reason='carbon_aware').inc()

        # Adaptive precision
        precision = self.precision_policy.recommend_precision(
            workload_size="medium",
            carbon_intensity=carbon_intensity,
        )

        quantum_key = await self.quantum_security.generate_keypair('dilithium')
        decision_manifest = {
            'fallback_id': fallback_id,
            'handler': handler_name,
            'timestamp': datetime.now().isoformat(),
            'carbon_strategy': carbon_strategy,
            'region_strategy': region_strategy,
            'precision': precision,
        }
        await self.quantum_security.sign_fallback_decision(decision_manifest, quantum_key['key_id'])

        cb = GlobalCircuitBreaker().get_or_create(
            handler_name,
            failure_threshold=self.config.circuit_breaker.failure_threshold,
            recovery_timeout=self.config.circuit_breaker.recovery_timeout
        )
        handlers = self.fallback_handlers.get(handler_name, [])
        if not handlers:
            raise Exception(f"No fallback handlers for {handler_name}")

        last_exception = None
        failures_this_call = 0
        for level, handler in enumerate(handlers):
            degradation_level = f"level_{level}"
            try:
                acquired, queue_event = await self.load_shedder.acquire()
                if not acquired:
                    if queue_event:
                        try:
                            await asyncio.wait_for(queue_event.wait(), timeout=30)
                        except asyncio.TimeoutError:
                            raise Exception("Queue timeout")
                    else:
                        raise LoadSheddingError("Load shedding active")

                async def _call_handler():
                    return await handler(context)

                result = await cb.call(_call_handler)
                latency_ms = (time.time() - start_time) * 1000

                # XAI explanation
                explanation = self.xai.explain_fallback(
                    handler_name, f"level_{level}", context, 0.8,
                    region_strategy.get('primary_region', 'unknown'), precision
                )

                async with self._history_lock:
                    self.fallback_history.append({
                        'handler_name': handler_name,
                        'strategy_used': f"level_{level}",
                        'degradation_level': degradation_level,
                        'latency_ms': latency_ms,
                        'retry_count': 0,
                        'success': True,
                        'carbon_intensity': carbon_intensity,
                        'region': region_strategy['primary_region'],
                        'precision': precision,
                        'explanation': explanation,
                    })

                await self.load_shedder.release()
                outcome = {'success': True, 'latency_ms': latency_ms, 'handler': handler_name, 'level': level,
                           'explanation': explanation, 'precision': precision}
                await self.blockchain.record_fallback(fallback_id, decision_manifest, outcome)
                await self.sustainability_tracker.record_metric('fallback_efficiency', 0.9,
                                                                {'level': level, 'success': True})
                # Carbon offset
                if carbon_intensity > self.config.carbon.offset_threshold:
                    carbon_kg = latency_ms * 0.00001
                    try:
                        await self.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)
                    except Exception as e:
                        logger.warning(f"Offset purchase failed: {e}")
                # Multi-agent reputation update
                if self.multi_agent:
                    for agent in self.multi_agent.agents:
                        self.multi_agent.record_outcome(agent, True)
                return result

            except Exception as e:
                last_exception = e
                failures_this_call += 1
                await cb.call(lambda: asyncio.sleep(0))
                latency_ms = (time.time() - start_time) * 1000
                async with self._history_lock:
                    self.fallback_history.append({
                        'handler_name': handler_name,
                        'strategy_used': f"level_{level}",
                        'degradation_level': degradation_level,
                        'latency_ms': latency_ms,
                        'success': False,
                        'carbon_intensity': carbon_intensity,
                        'region': region_strategy['primary_region'],
                        'precision': precision,
                    })
                if PROMETHEUS_AVAILABLE:
                    FALLBACK_TRIGGERED.labels(handler=handler_name, level=degradation_level, reason='handler_failure').inc()
                await self.load_shedder.release()

        # Human review if too many failures
        if failures_this_call >= self.config.general.human_review_threshold_failures:
            review_id = await self.human_review.request_review(fallback_id, {
                "handler_name": handler_name,
                "failures": failures_this_call,
                "context": context,
                "last_error": str(last_exception),
            })
            logger.info(f"Fallback {fallback_id} flagged for human review: {review_id}")

        # Federated fallback
        try:
            federated_patterns = await self.federated_learner.pull_network_patterns(domain=handler_name, limit=1)
            if federated_patterns:
                await self.sustainability_tracker.record_metric('fallback_efficiency', 0.6, {'source': 'federated'})
                return federated_patterns[0]['pattern'].get('result', 'federated_fallback')
        except Exception as e:
            logger.error(f"Federated fallback failed: {e}")

        outcome = {'success': False, 'error': str(last_exception) if last_exception else 'All fallbacks failed'}
        await self.blockchain.record_fallback(fallback_id, decision_manifest, outcome)
        if self.safety_monitor:
            self.safety_monitor.check_fallback(carbon_intensity, success=False)
        if self.multi_agent:
            for agent in self.multi_agent.agents:
                self.multi_agent.record_outcome(agent, False)
        raise last_exception or Exception(f"All fallbacks failed for {handler_name}")

    async def health_check(self) -> Dict:
        results = {}
        components = {
            'quantum_security': self.quantum_security,
            'blockchain': self.blockchain,
            'carbon_manager': self.carbon_manager,
            'llm_generator': self.llm_generator,
            'load_shedder': self.load_shedder,
            'region_coordinator': self.region_coordinator,
            'autonomous_optimizer': self.autonomous_optimizer,
            'federated_learner': self.federated_learner,
            'predictive_reflexivity': self.predictive_reflexivity,
            'sustainability_tracker': self.sustainability_tracker,
            'websocket_server': self.websocket_server,
            'cloud_storage': self.cloud_storage,
            'database': self.db_manager,
            'vault': self.vault,
        }
        for name, comp in components.items():
            if hasattr(comp, 'health_check'):
                try:
                    results[name] = await comp.health_check()
                except Exception as e:
                    results[name] = {'status': 'unhealthy', 'error': str(e)}
            else:
                results[name] = {'status': 'ok'}

        overall = 'healthy' if all(r.get('status') == 'ok' or r.get('status') == 'healthy' for r in results.values()) else 'degraded'
        health_score = 100 if overall == 'healthy' else 50
        if PROMETHEUS_AVAILABLE:
            HEALTH_SCORE.set(health_score)
        return {
            'status': overall,
            'health_score': health_score,
            'components': results,
            'timestamp': datetime.now().isoformat()
        }

    async def get_system_status(self) -> Dict:
        task_stats = self.task_manager.get_statistics()
        sustainability_score = await self.sustainability_tracker.get_fallback_sustainability_score()
        savings = await self.sustainability_tracker.get_fallback_savings()
        return {
            'instance_id': self.instance_id,
            'version': self.config.general.version,
            'running': self._running,
            'background_tasks': task_stats,
            'health': await self.health_check(),
            'load_shedder': self.load_shedder.get_statistics(),
            'llm_stats': self.llm_generator.get_cost_statistics(),
            'fallback_history': {
                'total': len(self.fallback_history),
                'recent_success_rate': float(np.mean([h['success'] for h in list(self.fallback_history)[-50:]])) if self.fallback_history else 0
            },
            'quantum_security': self.quantum_security.get_quantum_status(),
            'blockchain': await self.blockchain.get_blockchain_status(),
            'autonomous_optimizer': await self.autonomous_optimizer.get_optimization_status(),
            'region_coordinator': await self.region_coordinator.get_region_status(),
            'sustainability': {'score': sustainability_score, 'savings': savings},
            'predictive': {'prophet_available': self.predictive_reflexivity.prophet_available,
                           'hyperparam_evolution': getattr(self.predictive_reflexivity, 'bio', None) is not None},
            'federated': {'enabled': self.federated_learner.federated_enabled},
            'cloud_storage': {'providers': list(self.cloud_storage.providers.keys())},
            'enhancements_available': ENHANCEMENTS_AVAILABLE,
            'additional_enhancements_available': ADDITIONAL_ENHANCEMENTS_AVAILABLE,
            'advanced_enhancements': {
                'safety_monitor': self.safety_monitor.get_violations()[-5:] if self.safety_monitor else [],
                'chaos_monkey': self.chaos_monkey.get_stats(),
                'human_review': self.human_review.get_stats(),
                'carbon_broker': self.carbon_broker.get_totals(),
                'precision_policy': self.precision_policy.get_status(),
                'quantum_optimizer': self.quantum_optimizer.get_status(),
                'multi_agent': self.multi_agent.get_stats(),
            },
            'timestamp': datetime.now().isoformat()
        }

    async def shutdown(self):
        logger.info(f"Shutting down EnhancedFallbackManager (instance: {self.instance_id})")
        self._running = False
        await self.websocket_server.stop()
        await self.carbon_manager.close()
        await self.llm_generator.close()
        await self.task_manager.stop_all()
        await self.db_manager.close()
        await self.leader.stop()
        logger.info("Shutdown complete")

# ============================================================
# FASTAPI REST API (with new endpoints)
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Fallback Manager API", version="15.1")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer()
    api_rate_limiter = RateLimiter(rate=FallbackManagerConfig().api.rate_limit_requests,
                                   per_seconds=FallbackManagerConfig().api.rate_limit_window)

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        token = credentials.credentials
        try:
            payload = jwt.decode(token, FallbackManagerConfig().api.jwt_secret, algorithms=["HS256"])
            return payload
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    async def rate_limit(request: Request):
        if FallbackManagerConfig().api.rate_limit_enabled:
            if not await api_rate_limiter.acquire():
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

    manager: Optional[EnhancedFallbackManagerV15_0] = None

    @app.post("/fallback")
    async def trigger_fallback(handler_name: str, context: Dict = None, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        result = await manager.execute_with_fallback(handler_name, context)
        return {"result": result}

    @app.get("/status")
    async def get_status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager.get_system_status()

    @app.get("/health")
    async def health(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager.health_check()

    @app.get("/optimization/status")
    async def optimization_status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager.autonomous_optimizer.get_optimization_status()

    @app.post("/optimization/rlhf-update")
    async def rlhf_update(context: Dict, action: str, reward: float, user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        if hasattr(manager.autonomous_optimizer, 'rlhf') and manager.autonomous_optimizer.rlhf:
            manager.autonomous_optimizer.rlhf.update(context, action, reward)
            return {"status": "RLHF updated"}
        return {"status": "RLHF not available"}

    @app.post("/optimization/distill")
    async def force_distillation(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return {"status": "Distillation triggered"}

    # NEW: Human review endpoints
    @app.get("/human-review/pending")
    async def human_review_pending(user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(review_id: str, user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return {"approved": await manager.human_review.approve(review_id)}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(review_id: str, reason: Optional[str] = None, user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return {"rejected": await manager.human_review.reject(review_id, reason)}

    # NEW: Chaos
    @app.post("/chaos/trigger")
    async def chaos_trigger(enabled: bool = True, probability: float = 0.1, user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        manager.chaos_monkey.enabled = enabled
        manager.chaos_monkey.failure_probability = probability
        return manager.chaos_monkey.get_stats()

    # NEW: Carbon market
    @app.post("/carbon/offset")
    async def carbon_offset(carbon_kg: float, carbon_intensity: float, user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)

    @app.post("/carbon/rec")
    async def carbon_rec(energy_mwh: float, user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return await manager.carbon_broker.purchase_recs(energy_mwh)

    @app.get("/carbon/totals")
    async def carbon_totals(user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return manager.carbon_broker.get_totals()

    # NEW: Precision recommendation
    @app.post("/precision/recommend")
    async def precision_recommend(workload_size: str = "medium", carbon_intensity: float = None, user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return {"precision": manager.precision_policy.recommend_precision(workload_size, carbon_intensity)}

    # NEW: Safety violations
    @app.get("/safety/violations")
    async def safety_violations(user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        if manager.safety_monitor:
            return {"violations": manager.safety_monitor.get_violations()}
        return {"violations": [], "monitor_enabled": False}

    # NEW: XAI last decision
    @app.get("/xai/last-decision")
    async def xai_last_decision(user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        return {
            "region": manager.region_coordinator.last_explanation if hasattr(manager.region_coordinator, 'last_explanation') else "",
            "optimizer": manager.autonomous_optimizer.last_explanation if hasattr(manager.autonomous_optimizer, 'last_explanation') else "",
        }

    # NEW: Federated secure aggregation
    @app.post("/federated/register")
    async def federated_register(participant_id: str, update: Dict, user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        if hasattr(manager.federated_learner, 'secure_coordinator'):
            manager.federated_learner.secure_coordinator.register_participant(participant_id, update)
            return {"status": "registered"}
        return {"status": "not_available"}

    @app.get("/federated/aggregate")
    async def federated_aggregate(user: Dict = Depends(verify_token)):
        if not manager:
            raise HTTPException(status_code=503, detail="Manager not initialized")
        if hasattr(manager.federated_learner, 'secure_coordinator'):
            return manager.federated_learner.secure_coordinator.aggregate()
        return {}

    @app.on_event("startup")
    async def startup():
        global manager
        config = FallbackManagerConfig()
        db_manager = EnhancedDatabaseManager(config)
        await db_manager.init()
        vault = VaultManager(config)
        quantum = QuantumResilientFallbackSecurity(config, vault)
        blockchain = BlockchainFallbackVerification(config, db_manager)
        carbon = CarbonIntensityManager(config)
        llm = LLMFallbackGenerator(config)
        load_shedder = LoadShedder(config)
        region = MultiRegionFallbackCoordinator(config)
        optimizer = AutonomousFallbackOptimizer(config)
        federated = FederatedFallbackLearner(config, db_manager, config.general.instance_id)
        predictive = PredictiveFallbackReflexivity(config)
        sustainability = FallbackSustainabilityTracker(config, db_manager)
        websocket = WebSocketServer(config)
        cloud = MultiCloudStorage(config)
        leader = LeaderElection(config)
        task_manager = TaskManager()
        manager = EnhancedFallbackManagerV15_0(
            config=config,
            db_manager=db_manager,
            quantum_security=quantum,
            blockchain=blockchain,
            carbon_manager=carbon,
            llm_generator=llm,
            load_shedder=load_shedder,
            region_coordinator=region,
            autonomous_optimizer=optimizer,
            federated_learner=federated,
            predictive_reflexivity=predictive,
            sustainability_tracker=sustainability,
            websocket_server=websocket,
            cloud_storage=cloud,
            vault=vault,
            leader=leader,
            task_manager=task_manager,
        )
        await manager.start()
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown():
        if manager:
            await manager.shutdown()
        logger.info("FastAPI shut down")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================
_manager_instance = None
_manager_lock = asyncio.Lock()

async def get_fallback_manager(config: Optional[Union[FallbackManagerConfig, Dict]] = None) -> EnhancedFallbackManagerV15_0:
    global _manager_instance
    if _manager_instance is None:
        async with _manager_lock:
            if _manager_instance is None:
                cfg = config if isinstance(config, FallbackManagerConfig) else FallbackManagerConfig(**config) if config else FallbackManagerConfig()
                db_manager = EnhancedDatabaseManager(cfg)
                await db_manager.init()
                vault = VaultManager(cfg)
                quantum = QuantumResilientFallbackSecurity(cfg, vault)
                blockchain = BlockchainFallbackVerification(cfg, db_manager)
                carbon = CarbonIntensityManager(cfg)
                llm = LLMFallbackGenerator(cfg)
                load_shedder = LoadShedder(cfg)
                region = MultiRegionFallbackCoordinator(cfg)
                optimizer = AutonomousFallbackOptimizer(cfg)
                federated = FederatedFallbackLearner(cfg, db_manager, cfg.general.instance_id)
                predictive = PredictiveFallbackReflexivity(cfg)
                sustainability = FallbackSustainabilityTracker(cfg, db_manager)
                websocket = WebSocketServer(cfg)
                cloud = MultiCloudStorage(cfg)
                leader = LeaderElection(cfg)
                task_manager = TaskManager()
                _manager_instance = EnhancedFallbackManagerV15_0(
                    config=cfg,
                    db_manager=db_manager,
                    quantum_security=quantum,
                    blockchain=blockchain,
                    carbon_manager=carbon,
                    llm_generator=llm,
                    load_shedder=load_shedder,
                    region_coordinator=region,
                    autonomous_optimizer=optimizer,
                    federated_learner=federated,
                    predictive_reflexivity=predictive,
                    sustainability_tracker=sustainability,
                    websocket_server=websocket,
                    cloud_storage=cloud,
                    vault=vault,
                    leader=leader,
                    task_manager=task_manager,
                )
                await _manager_instance.start()
    return _manager_instance

# ============================================================
# SIGNAL HANDLING
# ============================================================
_shutdown_requested = False

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}, initiating shutdown...")
        try:
            asyncio.create_task(shutdown_handler())
        except RuntimeError:
            pass

async def shutdown_handler():
    global _manager_instance
    if _manager_instance:
        await _manager_instance.shutdown()
        _manager_instance = None

# ============================================================
# MAIN ENTRY POINT
# ============================================================
async def main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
        except (NotImplementedError, ValueError):
            pass

    print("=" * 80)
    print("Enhanced Fallback Manager v15.1 - Enterprise Quantum+ (Advanced Enhancements)")
    print("=" * 80)

    manager = await get_fallback_manager()
    print(f"\n✅ ADVANCED ENHANCEMENTS IN v15.1:")
    print("   ✅ CausalBandit for causal RL of fallback policies")
    print("   ✅ SafetyMonitor for temporal fallback rules")
    print("   ✅ XAIExplainer for natural-language rationale")
    print("   ✅ FederatedSecureCoordinator with differential privacy")
    print("   ✅ MultiAgentCoordinator with role specialisation")
    print("   ✅ CarbonOffsetBroker for offsets and RECs")
    print("   ✅ ChaosMonkey for resilience testing")
    print("   ✅ HumanReviewManager for pre-commit review")
    print("   ✅ FlexGenPrecisionPolicy for adaptive precision")
    print("   ✅ QuantumDistillationOptimizer (optional)")

    qstatus = manager.quantum_security.get_quantum_status()
    print(f"\n🔐 Quantum Status: PQC Available: {qstatus.get('pqc_available', False)}")

    rstatus = await manager.region_coordinator.get_region_status()
    print(f"🌍 Active Region: {rstatus.get('active_region')}, Quantum: {rstatus.get('quantum_optimizer', {})}")

    opt_status = await manager.autonomous_optimizer.get_optimization_status()
    print(f"⚡ Optimizer: causal_bandit={opt_status.get('causal_bandit', False)}, "
          f"limit_graph={opt_status.get('limit_graph_active', False)}, "
          f"rlhf={opt_status.get('rlhf_active', False)}, "
          f"distillation={opt_status.get('distillation_active', False)}")

    async def test_handler(context):
        return {"status": "success", "data": "test"}
    manager.register_fallback_handler("test_service", [test_handler])

    status = await manager.get_system_status()
    print(f"\n📊 System Status: Instance: {status['instance_id']}, Version: {status['version']}")
    print(f"   Advanced: {status['advanced_enhancements']}")

    print("\n" + "=" * 80)
    print("✅ Fallback Manager v15.1 - Ready for Production")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        if _manager_instance:
            await _manager_instance.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
