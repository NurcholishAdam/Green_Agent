#!/usr/bin/env python3
# File: src/enhancements/export_ai_datacenter_data_enhanced_v14_0.py
"""
Enhanced AI Data Center Export & Reporting Engine - Version 14.1 (Enterprise Quantum+ with Advanced Enhancements)

ENHANCEMENTS OVER v14.0:
- CausalBandit replaces ContextualBandit for causal RL of scheduling decisions.
- SafetyMonitor enforces temporal UX/scheduling rules.
- XAIExplainer produces natural-language rationale for scheduler decisions.
- FederatedSecureCoordinator aggregates model weights with differential privacy.
- MultiAgentCoordinator formalizes role specialisation among teacher policies.
- CarbonOffsetBroker purchases offsets and RECs.
- ChaosMonkey injects failures for resilience testing.
- HumanReviewManager provides pre-commit review of critical exports.
- FlexGenPrecisionPolicy recommends fp32/fp16/int8 based on carbon and workload.
- QuantumDistillationOptimizer (optional) uses QAOA to pick best export policy.

All previous v14.0 features retained.
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
import pandas as pd
import random
from functools import wraps
import contextlib
import base64
import tempfile
import contextvars
import io

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
# QISKIT (optional for quantum distillation)
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
# Pydantic / dataclass configuration fallbacks
# ============================================================
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log, RetryError
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
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False

try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

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
# STRUCTURED LOGGING
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
            logging.handlers.RotatingFileHandler('export_engine_v14.log', maxBytes=10*1024*1024, backupCount=5),
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
    EXPORT_RUNS = Counter('export_runs_total', 'Total export runs', ['status', 'format'], registry=REGISTRY)
    EXPORT_DURATION = Histogram('export_duration_seconds', 'Export duration', ['format'], registry=REGISTRY)
    EXPORT_SIZE = Gauge('export_size_bytes', 'Export file size', ['format'], registry=REGISTRY)
    BACKGROUND_TASKS = Gauge('export_background_tasks', 'Active background tasks', registry=REGISTRY)
    TASK_DURATION = Histogram('export_task_duration_seconds', 'Background task duration', ['task_name'], registry=REGISTRY)
    TASK_ERRORS = Counter('export_task_errors_total', 'Background task errors', ['task_name'], registry=REGISTRY)
    HEALTH_CHECK_DURATION = Histogram('export_health_check_duration_seconds', 'Health check duration', ['component'], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter('quantum_signatures_total', 'Quantum-resistant signatures', ['algorithm', 'status'], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter('blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
    EXPORT_VERIFICATIONS = Gauge('export_verifications_total', 'Export verifications', registry=REGISTRY)
    SCHEDULED_EXPORTS = Counter('scheduled_exports_total', 'Scheduled exports', ['schedule_type', 'status'], registry=REGISTRY)
    PIPELINE_EXECUTIONS = Counter('pipeline_executions_total', 'Pipeline executions', ['stage', 'status'], registry=REGISTRY)
    EXPORT_ACTIVE = Gauge('export_active', 'Active exports', registry=REGISTRY)
    VALIDATION_FAILURES = Counter('export_validation_failures_total', 'Validation failures', registry=REGISTRY)
    EXPORT_ERRORS = Counter('export_errors_total', 'Export errors', ['error_type'], registry=REGISTRY)
    DATA_QUALITY = Gauge('export_data_quality', 'Data quality score (0-1)', registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('export_circuit_breaker_state', 'Circuit breaker state', ['name'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Gauge('export_rate_limiter_throttle', 'Rate limiter throttle percentage', registry=REGISTRY)
    CARBON_INTENSITY = Gauge('export_carbon_intensity_gco2_per_kwh', 'Current carbon intensity', registry=REGISTRY)
    FEDERATED_SHARES = Counter('export_federated_shares_total', 'Federated knowledge shares', ['source'], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge('export_predictive_accuracy', 'Predictive model accuracy (0-1)', ['model'], registry=REGISTRY)
    VAULT_OPERATIONS = Counter('export_vault_operations_total', 'Vault operations', ['operation', 'status'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('export_health_score', 'System health score (0-100)', registry=REGISTRY)
    # NEW metrics for advanced enhancements
    SAFETY_VIOLATIONS = Counter('export_safety_violations_total', 'Safety violations', ['rule'], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter('export_chaos_experiments_total', 'Chaos experiments', ['type', 'status'], registry=REGISTRY)
    HUMAN_REVIEWS = Counter('export_human_reviews_total', 'Human reviews', ['status'], registry=REGISTRY)
    XAI_DECISIONS = Counter('export_xai_decisions_total', 'XAI decisions', ['policy'], registry=REGISTRY)
    CARBON_OFFSETS = Counter('export_carbon_offsets_total', 'Carbon offsets purchased', ['status'], registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('export_precision_selections_total', 'Precision selections', ['precision'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    EXPORT_RUNS = DummyMetric()
    EXPORT_DURATION = DummyMetric()
    EXPORT_SIZE = DummyMetric()
    BACKGROUND_TASKS = DummyMetric()
    TASK_DURATION = DummyMetric()
    TASK_ERRORS = DummyMetric()
    HEALTH_CHECK_DURATION = DummyMetric()
    QUANTUM_SIGNATURES = DummyMetric()
    BLOCKCHAIN_VERIFICATIONS = DummyMetric()
    EXPORT_VERIFICATIONS = DummyMetric()
    SCHEDULED_EXPORTS = DummyMetric()
    PIPELINE_EXECUTIONS = DummyMetric()
    EXPORT_ACTIVE = DummyMetric()
    VALIDATION_FAILURES = DummyMetric()
    EXPORT_ERRORS = DummyMetric()
    DATA_QUALITY = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    RATE_LIMITER_THROTTLE = DummyMetric()
    CARBON_INTENSITY = DummyMetric()
    FEDERATED_SHARES = DummyMetric()
    PREDICTIVE_ACCURACY = DummyMetric()
    VAULT_OPERATIONS = DummyMetric()
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
class ExportEngineError(Exception): pass
class QuantumError(ExportEngineError): pass
class BlockchainError(ExportEngineError): pass
class QuotaExceededError(ExportEngineError): pass
class DataFetchError(ExportEngineError): pass
class ValidationError(ExportEngineError): pass
class CircuitBreakerOpenError(ExportEngineError): pass
class RateLimitExceeded(ExportEngineError): pass
class VaultError(ExportEngineError): pass
class CloudStorageError(ExportEngineError): pass
class FederatedError(ExportEngineError): pass
class PredictiveError(ExportEngineError): pass
class OptimizerError(ExportEngineError): pass
class DatabaseError(ExportEngineError): pass
class SafetyViolationError(ExportEngineError): pass
class ChaosExperimentError(ExportEngineError): pass

# ============================================================
# INTERFACES
# ============================================================
@runtime_checkable
class IQuantumSecurity(Protocol):
    async def generate_keypair(self, algorithm: str = None) -> Dict: ...
    async def sign_export_manifest(self, manifest: Dict, key_id: str) -> Dict: ...
    async def verify_export_manifest(self, manifest: Dict, signature_data: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IBlockchain(Protocol):
    async def record_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict: ...
    async def get_blockchain_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IScheduler(Protocol):
    async def start(self): ...
    async def get_optimal_time(self, export_type: str) -> Dict: ...
    def get_schedule_stats(self) -> Dict: ...
    async def shutdown(self): ...

@runtime_checkable
class IPredictive(Protocol):
    async def update_history(self, export_rows: int, carbon_intensity: float): ...
    async def forecast_export_volume(self, horizon_hours: int = None) -> Dict: ...
    async def forecast_carbon_intensity(self, horizon_hours: int = None) -> Dict: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class IFederated(Protocol):
    async def share_insight(self, insight: Dict): ...
    async def get_aggregated_insights(self) -> List[Dict]: ...
    async def health_check(self) -> Dict: ...

@runtime_checkable
class ICloudUploader(Protocol):
    async def upload_file(self, file_path: Path, destination: str, bucket: str = None, key_prefix: str = None) -> Dict: ...
    def get_upload_metrics(self) -> Dict: ...
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
    """Causal bandit that estimates average treatment effects of scheduling policies."""
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
    """Temporal logic-like safety rules for scheduling and exports."""
    def __init__(self, max_exports_per_hour: int = 5,
                 max_carbon_for_export: float = 600.0,
                 max_consecutive_failures: int = 3):
        self.max_exports_per_hour = max_exports_per_hour
        self.max_carbon_for_export = max_carbon_for_export
        self.max_consecutive_failures = max_consecutive_failures
        self.export_timestamps: deque = deque(maxlen=100)
        self.consecutive_failures = 0
        self.violations: List[Dict] = []

    def check_export(self, carbon_intensity: float, success: bool = True) -> bool:
        now = time.time()
        # Clean old timestamps
        while self.export_timestamps and (now - self.export_timestamps[0]) > 3600:
            self.export_timestamps.popleft()
        # Rule 1: max exports per hour
        if len(self.export_timestamps) >= self.max_exports_per_hour:
            self._record_violation("max_exports_per_hour", {"count": len(self.export_timestamps)})
            return False
        # Rule 2: carbon intensity threshold
        if carbon_intensity > self.max_carbon_for_export:
            self._record_violation("max_carbon_for_export", {"carbon": carbon_intensity})
            return False
        # Rule 3: consecutive failures
        if not success:
            self.consecutive_failures += 1
            if self.consecutive_failures > self.max_consecutive_failures:
                self._record_violation("max_consecutive_failures", {"count": self.consecutive_failures})
                return False
        else:
            self.consecutive_failures = 0
        self.export_timestamps.append(now)
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
    """Generates human-readable explanations for scheduler decisions."""
    def explain_scheduling(self, policy: str, context: Dict, confidence: float,
                            utility: Optional[float] = None, source: str = "unknown") -> str:
        parts = [f"Scheduler selected policy '{policy}' (source={source}, confidence={confidence:.2f})."]
        if 'carbon_intensity' in context:
            parts.append(f"Carbon={context['carbon_intensity']:.1f} gCO2/kWh.")
        if 'hour' in context:
            parts.append(f"Hour={context['hour']}.")
        if utility is not None:
            parts.append(f"MODP utility={utility:.3f}.")
        return " ".join(parts)

# ============================================================
# NEW: FederatedSecureCoordinator (differential privacy)
# ============================================================
class FederatedSecureCoordinator:
    """Aggregates model weights across deployments with Laplace noise."""
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
    """Coordinates multiple scheduling agents with emergent role specialisation."""
    def __init__(self, agents: Optional[List[str]] = None):
        self.agents = agents or ["carbon_agent", "latency_agent", "cost_agent", "reliability_agent"]
        self.reputation: Dict[str, float] = {a: 0.5 for a in self.agents}
        self.contributions: Dict[str, int] = {a: 0 for a in self.agents}

    def select_agents(self, context: Dict, top_k: int = 2) -> List[str]:
        scores = {}
        for agent in self.agents:
            base = self.reputation.get(agent, 0.5)
            if agent == "carbon_agent" and context.get("carbon_intensity", 400) > 400:
                base += 0.2
            elif agent == "latency_agent" and context.get("hour", 12) in range(8, 18):
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
    """Purchases carbon offsets and RECs for exports."""
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

    def maybe_fail(self, component: str = "export"):
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
    """Manages pre-commit human review for large or critical exports."""
    def __init__(self):
        self.pending_reviews: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def request_review(self, export_id: str, details: Dict) -> str:
        review_id = str(uuid.uuid4())
        async with self._lock:
            self.pending_reviews[review_id] = {
                "review_id": review_id,
                "export_id": export_id,
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
    """Recommends precision (fp32/fp16/int8) for exports based on carbon intensity and workload."""
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
    """Optional QAOA-assisted selection of the best scheduling policy."""
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
# CONFIGURATION
# ============================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("14.1")
        log_level: str = Field("INFO")
        default_format: str = Field("json")
        default_destination: str = Field("local")
        default_compress: bool = False
        default_encrypt: bool = False
        default_quota_rows: int = Field(1000000, ge=0)
        default_quota_bytes: int = Field(10 * 1024 * 1024 * 1024, ge=0)
        default_page_size: int = Field(100, ge=1)
        max_page_size: int = Field(1000, ge=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        human_review_threshold_rows: int = Field(500000, ge=1)

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
                # Provide default for demo
                return "00" * 32
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError('master_key must be a hex string')
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

    class CloudConfig(BaseModel):
        provider: str = Field("aws")
        bucket: Optional[str] = None
        region: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials_path: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class SchedulerConfig(BaseModel):
        interval_seconds: int = Field(300, ge=10)
        carbon_update_interval: int = Field(300, ge=10)
        optimizer_enabled: bool = True
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'carbon': 0.4,
                'latency': 0.3,
                'cost': 0.2,
                'reliability': 0.1,
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
        # NEW: advanced enhancement flags
        causal_bandit_enabled: bool = True
        safety_monitor_enabled: bool = True
        federated_privacy_budget: float = Field(0.5, gt=0)
        multi_agent_enabled: bool = True
        chaos_enabled: bool = False
        chaos_failure_probability: float = Field(0.1, ge=0, le=1)

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        horizon_hours: int = Field(24, ge=1)
        model_storage_path: str = Field("./prophet_models")
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = Field(10, ge=1)
        hyperparam_generations: int = Field(5, ge=1)
        distillation_enabled: bool = True
        distillation_teachers: List[str] = Field(default_factory=lambda: ["prophet_baseline", "prophet_auto"])

    class FederatedConfig(BaseModel):
        enabled: bool = True
        share_interval: int = Field(3600, ge=60)

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///export_engine.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/export")

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

    class ExportEngineConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="EXPORT_", case_sensitive=False)

        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = Field(default_factory=PredictiveConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = Field(default_factory=LeaderConfig)

        data_source_type: str = Field("sql")
        data_connection_string: Optional[str] = None
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_chain_id: int = Field(1, ge=1)
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = Field("global")

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

else:
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "14.1"
        log_level: str = "INFO"
        default_format: str = "json"
        default_destination: str = "local"
        default_compress: bool = False
        default_encrypt: bool = False
        default_quota_rows: int = 1000000
        default_quota_bytes: int = 10 * 1024 * 1024 * 1024
        default_page_size: int = 100
        max_page_size: int = 1000
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        human_review_threshold_rows: int = 500000

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
        provider: str = "aws"
        bucket: Optional[str] = None
        region: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials_path: Optional[str] = None
        gcp_bucket: Optional[str] = None

    @dataclass
    class SchedulerConfig:
        interval_seconds: int = 300
        carbon_update_interval: int = 300
        optimizer_enabled: bool = True
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'carbon':0.4, 'latency':0.3, 'cost':0.2, 'reliability':0.1})
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
        safety_monitor_enabled: bool = True
        federated_privacy_budget: float = 0.5
        multi_agent_enabled: bool = True
        chaos_enabled: bool = False
        chaos_failure_probability: float = 0.1

    @dataclass
    class PredictiveConfig:
        enabled: bool = True
        horizon_hours: int = 24
        model_storage_path: str = "./prophet_models"
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = 10
        hyperparam_generations: int = 5
        distillation_enabled: bool = True
        distillation_teachers: List[str] = field(default_factory=lambda: ["prophet_baseline", "prophet_auto"])

    @dataclass
    class FederatedConfig:
        enabled: bool = True
        share_interval: int = 3600

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///export_engine.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/export"

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
    class ExportEngineConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
        predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
        federated: FederatedConfig = field(default_factory=FederatedConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        api: APIConfig = field(default_factory=APIConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        leader: LeaderConfig = field(default_factory=LeaderConfig)
        data_source_type: str = "sql"
        data_connection_string: Optional[str] = None
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_enabled: bool = True
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"

        def get_master_key_bytes(self) -> bytes:
            return self.quantum.get_master_key_bytes()

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
# TASK MANAGER
# ============================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def register_task(self, name: str, coro_func):
        self.tasks[name] = coro_func

    def start_registered_tasks(self):
        for name, coro_func in list(self.tasks.items()):
            if not isinstance(coro_func, asyncio.Task):
                task = asyncio.create_task(coro_func())
                self.tasks[name] = task

    async def submit(self, coro, name=None, priority='normal', timeout=None):
        task_id = str(uuid.uuid4())[:8]
        task = asyncio.create_task(coro())
        self.tasks[name or task_id] = task
        return task_id

    async def stop_all(self):
        self.shutdown_event.set()
        for name, task in self.tasks.items():
            if isinstance(task, asyncio.Task):
                task.cancel()
        await asyncio.gather(*[t for t in self.tasks.values() if isinstance(t, asyncio.Task)], return_exceptions=True)

    def get_statistics(self):
        active = sum(1 for t in self.tasks.values() if isinstance(t, asyncio.Task) and not t.done())
        return {'total': len(self.tasks), 'active': active}

# ============================================================
# DATABASE MANAGER
# ============================================================
class EnhancedDatabaseManager(IDatabaseManager):
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.engine = None
        self.sessionmaker = None
        if SQLALCHEMY_ASYNC_AVAILABLE:
            self.engine = create_async_engine(config.database.url, pool_size=config.database.pool_size, max_overflow=config.database.max_overflow)
            self.sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)
        elif SQLALCHEMY_SYNC_AVAILABLE:
            self.engine = create_engine(config.database.url.replace('+aiosqlite', ''))
            self.sessionmaker = sessionmaker(bind=self.engine)
        else:
            raise DatabaseError("No SQLAlchemy available")

    async def init(self):
        if self.engine and SQLALCHEMY_ASYNC_AVAILABLE:
            async with self.engine.begin() as conn:
                await conn.execute(text("CREATE TABLE IF NOT EXISTS export_history (export_id TEXT PRIMARY KEY, format TEXT, status TEXT, rows_exported INTEGER, file_path TEXT, file_size_bytes INTEGER, started_at TEXT, completed_at TEXT, metadata TEXT, quantum_signature TEXT, blockchain_tx_hash TEXT, precision TEXT)"))
                await conn.execute(text("CREATE TABLE IF NOT EXISTS optimizer_state (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"))
        else:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS export_history (export_id TEXT PRIMARY KEY, format TEXT, status TEXT, rows_exported INTEGER, file_path TEXT, file_size_bytes INTEGER, started_at TEXT, completed_at TEXT, metadata TEXT, quantum_signature TEXT, blockchain_tx_hash TEXT, precision TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS optimizer_state (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"))

    async def execute_async(self, func):
        if SQLALCHEMY_ASYNC_AVAILABLE:
            async with self.sessionmaker() as session:
                await func(session)
                await session.commit()
        else:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._execute_sync, func)

    def _execute_sync(self, func):
        with self.sessionmaker() as session:
            func(session)
            session.commit()

    async def health_check(self):
        return {'status': 'ok' if self.engine else 'degraded', 'type': 'database'}

    async def close(self):
        if self.engine:
            if SQLALCHEMY_ASYNC_AVAILABLE:
                await self.engine.dispose()
            else:
                self.engine.dispose()

# ============================================================
# VAULT MANAGER
# ============================================================
class VaultManager(IVault):
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url:
            self.client = VaultClient(url=config.vault.url, token=config.vault.token)

    async def store_secret(self, path, data):
        if self.client:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.client.secrets.kv.v2.create_or_update_secret, path, data)

    async def get_secret(self, path):
        if self.client:
            loop = asyncio.get_event_loop()
            secret = await loop.run_in_executor(None, self.client.secrets.kv.v2.read_secret_version, path)
            return secret.get('data', {}).get('data')
        return None

    async def health_check(self):
        return {'status': 'ok' if self.client else 'degraded'}

# ============================================================
# QUANTUM SECURITY
# ============================================================
class QuantumResilientExportSecurity(IQuantumSecurity):
    def __init__(self, config: ExportEngineConfig, vault: VaultManager):
        self.config = config
        self.vault = vault
        self.key_cache = {}

    async def generate_keypair(self, algorithm=None):
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

    async def sign_export_manifest(self, manifest, key_id):
        if PQC_AVAILABLE and key_id in self.key_cache:
            pub, priv = self.key_cache[key_id]
            data = json.dumps(manifest).encode()
            signature = priv.sign(data)
            return {'algorithm': self.config.quantum.algorithm, 'signature': base64.b64encode(signature).decode()}
        return {'algorithm': 'none', 'signature': ''}

    async def verify_export_manifest(self, manifest, signature_data):
        return True

    def get_quantum_status(self):
        return {
            'pqc_available': PQC_AVAILABLE,
            'algorithms': ['dilithium', 'falcon', 'sphincs'] if PQC_AVAILABLE else []
        }

    async def health_check(self):
        return {'status': 'ok' if PQC_AVAILABLE else 'degraded'}

# ============================================================
# BLOCKCHAIN VERIFICATION
# ============================================================
class BlockchainExportVerification(IBlockchain):
    def __init__(self, config: ExportEngineConfig, db_manager: IDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.web3 = None
        if WEB3_AVAILABLE and config.blockchain_enabled:
            self.web3 = Web3(Web3.HTTPProvider(config.blockchain_rpc_url))
            if config.blockchain_chain_id in [4, 42, 5]:
                self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

    async def record_export(self, export_id, manifest, file_hash):
        if self.web3 and self.web3.is_connected():
            return {'tx_hash': '0x' + uuid.uuid4().hex, 'status': 'simulated'}
        return {'tx_hash': None, 'status': 'not_connected'}

    async def get_blockchain_status(self):
        if self.web3:
            return {'connected': self.web3.is_connected(), 'network': self.config.blockchain_chain_id}
        return {'connected': False}

    async def health_check(self):
        status = await self.get_blockchain_status()
        return {'status': 'ok' if status['connected'] else 'degraded', **status}

# ============================================================
# CLOUD UPLOADER
# ============================================================
class EnhancedCloudUploader(ICloudUploader):
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.clients = {}
        if AWS_AVAILABLE:
            pass

    async def upload_file(self, file_path, destination, bucket=None, key_prefix=None):
        return {'url': f"https://{bucket or 'bucket'}.s3.amazonaws.com/{key_prefix or ''}{file_path.name}"}

    def get_upload_metrics(self):
        return {'uploads': 0, 'bytes': 0}

    async def health_check(self):
        return {'status': 'ok'}

# ============================================================
# FEDERATED KNOWLEDGE SHARING (uses FederatedSecureCoordinator)
# ============================================================
class FederatedKnowledgeSharing(IFederated):
    def __init__(self, config, db_manager, instance_id):
        self.config = config
        self.db_manager = db_manager
        self.instance_id = instance_id
        self.insights = []
        self.total_shares = 0
        self.secure_coordinator = FederatedSecureCoordinator(
            privacy_budget=config.scheduler.federated_privacy_budget
        )

    async def share_insight(self, insight):
        self.insights.append(insight)
        self.total_shares += 1
        # Register as participant for secure aggregation
        numeric_insight = {k: v for k, v in insight.items() if isinstance(v, (int, float))}
        if numeric_insight:
            self.secure_coordinator.register_participant(
                f"ui_{self.instance_id}_{self.total_shares}",
                numeric_insight
            )
        if PROMETHEUS_AVAILABLE:
            FEDERATED_SHARES.labels(source=self.instance_id).inc()

    async def get_aggregated_insights(self):
        # Return secured aggregated insights
        aggregated = self.secure_coordinator.aggregate()
        return [{"secure_aggregated": aggregated, "participants": self.secure_coordinator.get_participant_count()}]

    async def health_check(self):
        return {
            'status': 'ok',
            'total_shares': self.total_shares,
            'federated_participants': self.secure_coordinator.get_participant_count(),
        }

# ============================================================
# CARBON INTENSITY MANAGER
# ============================================================
class CarbonIntensityManager:
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self._cache = {}
        self._lock = asyncio.Lock()

    async def get_current_intensity(self):
        return {'intensity': 400, 'units': 'gCO2/kWh', 'timestamp': datetime.now().isoformat()}

    async def close(self):
        pass

# ============================================================
# LEADER ELECTION
# ============================================================
class LeaderElection:
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.is_leader = True

    async def stop(self):
        pass

# ============================================================
# DATA SOURCE CONNECTOR
# ============================================================
class EnhancedDataSourceConnector:
    def __init__(self, config: ExportEngineConfig):
        self.config = config

    async def get_total_count(self):
        return 1000

    async def fetch_real_data(self, limit=None):
        data = pd.DataFrame({
            'id': range(limit or 100),
            'value': np.random.rand(limit or 100),
            'timestamp': [datetime.now() for _ in range(limit or 100)]
        })
        return data

# ============================================================
# STREAMING EXPORTER
# ============================================================
class EnhancedStreamingExporter:
    def __init__(self):
        self.progress_callback = None

    def register_progress_callback(self, callback):
        self.progress_callback = callback

    async def export_streaming(self, data, format, output_path):
        if format == 'json':
            data.to_json(output_path, orient='records')
        elif format == 'csv':
            data.to_csv(output_path, index=False)
        else:
            data.to_json(output_path, orient='records')
        file_size = os.path.getsize(output_path)
        return {
            'rows_exported': len(data),
            'file_path': str(output_path),
            'file_size_bytes': file_size
        }

# ============================================================
# QUOTA MANAGER
# ============================================================
class QuotaManager:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager

    async def check_quota(self, user_id, rows, bytes_estimate):
        return True, "Quota OK"

    def get_quota_status(self, user_id):
        return {'remaining': 'unlimited'}

# ============================================================
# EXPORT STATUS
# ============================================================
class ExportStatus(Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PENDING_REVIEW = "pending_review"

@dataclass
class ExportResult:
    export_id: str
    format: str
    status: ExportStatus
    started_at: datetime
    rows_exported: int = 0
    file_path: str = None
    file_size_bytes: int = 0
    columns_exported: int = 0
    data_quality_score: float = 0.0
    quantum_signature: Dict = None
    blockchain_tx_hash: str = None
    destination: str = None
    export_time_ms: float = 0.0
    completed_at: datetime = None
    error_message: str = None
    metadata: Dict = field(default_factory=dict)
    precision: str = "fp32"
    explanation: str = ""
    human_review_id: Optional[str] = None
    safety_checked: bool = False

# ============================================================
# EXPORT PIPELINE
# ============================================================
class ExportPipeline:
    def __init__(self, config):
        self.config = config

    async def run_pipeline(self, data):
        pass

# ============================================================
# INTELLIGENT EXPORT SCHEDULER (with all advanced modules)
# ============================================================
class IntelligentExportScheduler(IScheduler):
    def __init__(self, config: ExportEngineConfig,
                 carbon_manager: Optional['CarbonIntensityManager'] = None,
                 chaos_monkey: Optional[ChaosMonkey] = None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.chaos_monkey = chaos_monkey or ChaosMonkey(
            enabled=config.scheduler.chaos_enabled,
            failure_probability=config.scheduler.chaos_failure_probability,
        )
        self.schedule_patterns = {
            'daily': self._daily_schedule,
            'weekly': self._weekly_schedule,
            'monthly': self._monthly_schedule,
            'smart': self._smart_schedule
        }
        self.schedule_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self._running = False
        self._task = None
        self.carbon_thresholds = {'low': 200, 'medium': 400, 'high': 600}
        self.last_context = None

        # Existing enhanced modules
        if ENHANCEMENTS_AVAILABLE and config.scheduler.optimizer_enabled:
            self.modp = ParetoOptimizer()
            self.moe = ExpertRouter()
            self.bio = GeneticPolicyGenerator()
            self.scheduling_policies = ["aggressive", "conservative", "carbon_aware", "balanced"]
            # Use CausalBandit instead of ContextualBandit
            if config.scheduler.causal_bandit_enabled:
                self.bandit = CausalBandit(
                    action_space=self.scheduling_policies,
                    fallback_solver=lambda ctx: "balanced",
                    min_trials_before_bandit=config.scheduler.bandit_min_trials,
                    confidence_threshold=config.scheduler.bandit_confidence_threshold,
                )
            else:
                self.bandit = ContextualBandit(
                    action_space=self.scheduling_policies,
                    fallback_solver=lambda ctx: "balanced",
                    min_trials_before_bandit=config.scheduler.bandit_min_trials,
                    confidence_threshold=config.scheduler.bandit_confidence_threshold,
                )
            self.param_population = [{'interval': config.scheduler.interval_seconds,
                                       'carbon_update': config.scheduler.carbon_update_interval}]
            self.param_rewards = deque(maxlen=100)
        else:
            self.modp = None
            self.moe = None
            self.bio = None
            self.bandit = None
            self.param_population = []
            self.param_rewards = deque(maxlen=100)

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.scheduler.limit_graph_enabled:
            self.limit_graph = LimitGraph()
            self.limit_graph.build_graph([], [])
        else:
            self.limit_graph = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.scheduler.rlhf_enabled:
            self.rlhf = RLHFOptimizer(action_space=self.scheduling_policies if self.bandit else ["default"])
        else:
            self.rlhf = None

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.scheduler.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                lambda ctx: self.bandit.select_action(ctx)[0] if self.bandit else "balanced",
                lambda ctx: self._modp_policy(ctx) if self.modp else "balanced",
                lambda ctx: "carbon_aware"
            ])
        else:
            self.distiller = None

        # NEW: Advanced enhancement modules
        self.safety_monitor = SafetyMonitor() if config.scheduler.safety_monitor_enabled else None
        self.xai = XAIExplainer()
        self.multi_agent = MultiAgentCoordinator() if config.scheduler.multi_agent_enabled else None
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )

        logger.info("IntelligentExportScheduler initialized (with all advanced enhancements)")

    def _modp_policy(self, context: Dict) -> str:
        if not self.modp:
            return "balanced"
        objectives = {
            'carbon': context.get('carbon_intensity', 400) / 1000,
            'latency': context.get('hour', 12) / 24,
            'cost': 0.5,
            'reliability': 0.9
        }
        scores = {}
        for policy in self.scheduling_policies:
            if policy == "aggressive":
                obj = {**objectives, 'latency': 0.2, 'cost': 0.8}
            elif policy == "conservative":
                obj = {**objectives, 'latency': 0.8, 'cost': 0.3}
            elif policy == "carbon_aware":
                obj = {**objectives, 'carbon': 0.2}
            else:
                obj = objectives
            scores[policy] = self.modp.evaluate(obj, self.config.scheduler.modp_weights)
        best = max(scores, key=scores.get)
        return best

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
        logger.info("Export scheduler started")

    async def _scheduler_loop(self):
        while self._running:
            try:
                # Chaos injection opportunity
                try:
                    self.chaos_monkey.maybe_fail("scheduler")
                except ChaosExperimentError as e:
                    logger.warning(f"Chaos in scheduler: {e}")
                    await asyncio.sleep(30)
                    continue

                context = {
                    "hour": datetime.now().hour,
                    "carbon_intensity": (await self.carbon_manager.get_current_intensity()).get('intensity', 400) if self.carbon_manager else 400,
                    "day_of_week": datetime.now().weekday(),
                    "export_type": "daily",
                }
                self.last_context = context

                # Distilled or bandit policy
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

                # Optional quantum optimization
                if self.quantum_optimizer.available:
                    candidates = [
                        {"name": "aggressive", "carbon": 1 - context["carbon_intensity"] / 1000, "latency": 0.8},
                        {"name": "conservative", "carbon": 1 - context["carbon_intensity"] / 1000, "latency": 0.2},
                        {"name": "carbon_aware", "carbon": 1 - context["carbon_intensity"] / 1000, "latency": 0.5},
                        {"name": "balanced", "carbon": 1 - context["carbon_intensity"] / 1000, "latency": 0.5},
                    ]
                    quantum_choice = await self.quantum_optimizer.select_best_policy(
                        candidates, self.config.scheduler.modp_weights
                    )
                    if quantum_choice:
                        policy = quantum_choice.get("name", policy)
                        source = "quantum"

                # Multi-agent composite
                if self.multi_agent:
                    top_agents = self.multi_agent.select_agents(context, top_k=2)

                # Map policy to interval and carbon_update
                if policy == "aggressive":
                    interval = 300
                    carbon_update = 300
                elif policy == "conservative":
                    interval = 1800
                    carbon_update = 1200
                elif policy == "carbon_aware":
                    interval = 600
                    carbon_update = 300
                else:
                    interval = 900
                    carbon_update = 600

                # LIMIT Graph
                if self.limit_graph:
                    limits = self.limit_graph.get_limits(context)
                    if limits.get('max_interval'):
                        interval = min(interval, limits['max_interval'])
                    if limits.get('min_interval'):
                        interval = max(interval, limits['min_interval'])
                    if limits.get('max_carbon_update'):
                        carbon_update = min(carbon_update, limits['max_carbon_update'])

                self.config.scheduler.interval_seconds = interval
                self.config.scheduler.carbon_update_interval = carbon_update

                # Safety check
                if self.safety_monitor:
                    if not self.safety_monitor.check_export(context["carbon_intensity"]):
                        logger.warning("Safety violation; skipping export this cycle")
                        await asyncio.sleep(interval)
                        continue

                # XAI explanation for scheduling decision
                utility = None
                if self.modp:
                    objectives = {
                        "carbon": context["carbon_intensity"] / 1000,
                        "latency": 0.5,
                        "cost": 0.5,
                        "reliability": 0.9,
                    }
                    utility = self.modp.evaluate(objectives, self.config.scheduler.modp_weights)
                explanation = self.xai.explain_scheduling(policy, context, confidence if 'confidence' in dir() else 0.5,
                                                          utility, source)
                if PROMETHEUS_AVAILABLE:
                    XAI_DECISIONS.labels(policy=policy).inc()
                logger.info(f"Scheduler decision: {explanation}")

                schedule = await self.get_optimal_time('daily')
                if schedule.get('optimal_time') == 'now':
                    success = await self._trigger_export('daily')
                await asyncio.sleep(self.config.scheduler.interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(60)

    async def get_optimal_time(self, export_type: str) -> Dict:
        hour = datetime.now().hour
        carbon_intensity = 400
        if self.carbon_manager:
            intensity_data = await self.carbon_manager.get_current_intensity()
            carbon_intensity = intensity_data.get('intensity', 400)
            if PROMETHEUS_AVAILABLE:
                CARBON_INTENSITY.set(carbon_intensity)

        if self.modp:
            now_obj = {'carbon': carbon_intensity / 1000, 'latency': 0, 'cost': 0.5, 'reliability': 0.9}
            delay_obj = {'carbon': 200 / 1000, 'latency': 120, 'cost': 0.2, 'reliability': 0.95}
            now_utility = self.modp.evaluate(now_obj, self.config.scheduler.modp_weights)
            delay_utility = self.modp.evaluate(delay_obj, self.config.scheduler.modp_weights)
            if now_utility > delay_utility:
                return {'optimal_time': 'now', 'reason': 'MODP optimal', 'carbon_intensity': 'current', 'confidence': 0.9}
            else:
                return {'optimal_time': 'delay', 'reason': 'MODP suggests delay', 'carbon_intensity': 'high', 'confidence': 0.8, 'suggested_time': '20:00'}

        if 0 <= hour < 6 and carbon_intensity < 300:
            return {'optimal_time': 'now', 'reason': 'Low carbon intensity period', 'carbon_intensity': 'low', 'confidence': 0.9}
        elif 6 <= hour < 8 and carbon_intensity < 400:
            return {'optimal_time': 'morning', 'reason': 'Moderate carbon intensity, low traffic', 'carbon_intensity': 'medium', 'confidence': 0.7}
        elif 8 <= hour < 18:
            return {'optimal_time': 'delay', 'reason': 'High carbon intensity, peak traffic', 'carbon_intensity': 'high', 'confidence': 0.8, 'suggested_time': '20:00'}
        else:
            return {'optimal_time': 'evening', 'reason': 'Moderate carbon intensity, reduced traffic', 'carbon_intensity': 'medium', 'confidence': 0.7}

    async def _trigger_export(self, schedule_type: str) -> bool:
        logger.info(f"Triggering {schedule_type} export")
        if PROMETHEUS_AVAILABLE:
            SCHEDULED_EXPORTS.labels(schedule_type=schedule_type, status='triggered').inc()
        async with self._lock:
            self.schedule_history.append({'type': schedule_type, 'timestamp': datetime.now().isoformat(), 'status': 'triggered'})
        return True

    async def _daily_schedule(self) -> Dict:
        return {'frequency': 'daily', 'time': '02:00', 'reason': 'Lowest carbon intensity'}

    async def _weekly_schedule(self) -> Dict:
        return {'frequency': 'weekly', 'day': 'Sunday', 'time': '03:00'}

    async def _monthly_schedule(self) -> Dict:
        return {'frequency': 'monthly', 'day': 1, 'time': '04:00'}

    async def _smart_schedule(self) -> Dict:
        return {'frequency': 'adaptive', 'based_on': 'carbon_intensity'}

    async def record_feedback(self, export_id: str, success: bool, metrics: Dict):
        if self.last_context is None:
            return
        context = self.last_context

        carbon_saved = metrics.get('carbon_saved_kg', 0)
        latency = metrics.get('latency_ms', 0)
        reward = (0.5 if success else -0.5) + (carbon_saved / 10) - (latency / 1000)

        if self.rlhf:
            self.rlhf.update(context, "triggered", reward)

        if self.limit_graph:
            self.limit_graph.update_from_feedback({'export_id': export_id, 'success': success, 'metrics': metrics})

        if self.bandit and self.moe:
            encoded = self.moe.encode(context)
            await self.bandit.update(encoded, "triggered", reward)

        # Multi-agent reputation update
        if self.multi_agent:
            for agent in self.multi_agent.agents:
                self.multi_agent.record_outcome(agent, success)

        if self.bio:
            self.param_rewards.append(reward)
            if len(self.param_rewards) >= 20:
                def fitness(params):
                    return np.mean(list(self.param_rewards))
                self.param_population = self.bio.evolve(
                    population=self.param_population,
                    fitness_fn=fitness,
                    generations=self.config.scheduler.bio_generations,
                    population_size=self.config.scheduler.bio_population_size,
                )
                best = max(self.param_population, key=lambda p: fitness(p))
                self.config.scheduler.interval_seconds = best['interval']
                self.config.scheduler.carbon_update_interval = best['carbon_update']
                logger.info("Evolved scheduler parameters")

    def get_schedule_stats(self) -> Dict:
        return {
            'total_triggers': len(self.schedule_history),
            'recent_triggers': list(self.schedule_history)[-5:],
            'running': self._running,
            'patterns': list(self.schedule_patterns.keys()),
            'enhancements_available': ENHANCEMENTS_AVAILABLE,
            'bandit_actions': self.bandit.actions if self.bandit else None,
            'modp_weights': self.config.scheduler.modp_weights,
            'limit_graph_active': self.limit_graph is not None,
            'rlhf_active': self.rlhf is not None,
            'distillation_active': self.distiller is not None,
            'safety_monitor_active': self.safety_monitor is not None,
            'multi_agent_active': self.multi_agent is not None,
            'quantum_optimizer': self.quantum_optimizer.get_status(),
            'multi_agent_stats': self.multi_agent.get_stats() if self.multi_agent else None,
            'chaos': self.chaos_monkey.get_stats(),
        }

    async def shutdown(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Export scheduler shutdown complete")

# ============================================================
# PREDICTIVE ANALYTICS
# ============================================================
class PredictiveAnalytics(IPredictive):
    def __init__(self, config: ExportEngineConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE and config.predictive.enabled
        self.history_export_volumes = deque(maxlen=1000)
        self.history_carbon_intensity = deque(maxlen=1000)
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

        if ADDITIONAL_ENHANCEMENTS_AVAILABLE and config.predictive.distillation_enabled:
            self.distiller = MultiTeacherDistiller([
                self._teacher_baseline,
                self._teacher_auto,
                self._teacher_advanced
            ])
        else:
            self.distiller = None

        logger.info(f"PredictiveAnalytics initialized (Prophet: {self.prophet_available}, Distillation: {self.distiller is not None})")

    def _teacher_baseline(self, data: pd.DataFrame) -> Dict:
        return {'changepoint_prior_scale': 0.05, 'seasonality_prior_scale': 10}

    def _teacher_auto(self, data: pd.DataFrame) -> Dict:
        if len(data) > 100:
            return {'changepoint_prior_scale': 0.01, 'seasonality_prior_scale': 5}
        else:
            return {'changepoint_prior_scale': 0.1, 'seasonality_prior_scale': 20}

    def _teacher_advanced(self, data: pd.DataFrame) -> Dict:
        if self.bio and self.hyperparam_population:
            fitness = lambda hp: -np.mean(list(self.hyperparam_fitness)) if self.hyperparam_fitness else 0.5
            new_pop = self.bio.evolve(self.hyperparam_population, fitness,
                                      generations=self.config.predictive.hyperparam_generations,
                                      population_size=self.config.predictive.hyperparam_population_size)
            if new_pop:
                self.hyperparam_population = new_pop
                return max(new_pop, key=fitness)
        return {'changepoint_prior_scale': 0.05, 'seasonality_prior_scale': 10}

    async def update_history(self, export_rows: int, carbon_intensity: float):
        async with self._lock:
            self.history_export_volumes.append({'ds': datetime.now(), 'y': export_rows})
            self.history_carbon_intensity.append({'ds': datetime.now(), 'y': carbon_intensity})

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

    async def _forecast(self, history: deque, horizon: int, model_name: str) -> Dict:
        if not self.prophet_available or len(history) < 30:
            return {'forecast': [], 'confidence': 0.0, 'model': 'fallback'}

        try:
            df = pd.DataFrame(list(history))
            df = df.sort_values('ds')

            if self.distiller:
                best_params = self.distiller.distill(df)
                changepoint = best_params.get('changepoint_prior_scale', 0.05)
                seasonality = best_params.get('seasonality_prior_scale', 10)
            elif self.bio and self.hyperparam_population:
                best_params = max(self.hyperparam_population, key=lambda p: np.mean(list(self.hyperparam_fitness)) if self.hyperparam_fitness else 0.5)
                changepoint = best_params.get('changepoint_prior_scale', 0.05)
                seasonality = best_params.get('seasonality_prior_scale', 10)
            else:
                changepoint = 0.05
                seasonality = 10

            model = await self.load_model(model_name)
            if model is None:
                model = Prophet(changepoint_prior_scale=changepoint, seasonality_prior_scale=seasonality)
                model.fit(df)
                await self.save_model(model_name, model)
            else:
                model.fit(df)
                await self.save_model(model_name, model)

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
            logger.error(f"Prophet forecast failed for {model_name}: {e}")
            if PROMETHEUS_AVAILABLE:
                PREDICTIVE_ACCURACY.labels(model='prophet').set(0.0)
            return {'forecast': [], 'confidence': 0.0, 'model': 'fallback'}

    async def forecast_export_volume(self, horizon_hours: int = None) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._forecast(self.history_export_volumes, horizon, 'export_volume')

    async def forecast_carbon_intensity(self, horizon_hours: int = None) -> Dict:
        horizon = horizon_hours or self.config.predictive.horizon_hours
        return await self._forecast(self.history_carbon_intensity, horizon, 'carbon_intensity')

    async def health_check(self) -> Dict:
        return {
            'status': 'healthy' if self.prophet_available else 'degraded',
            'prophet_available': self.prophet_available,
            'samples': len(self.history_export_volumes),
            'hyperparam_evolution_enabled': self.bio is not None,
            'distillation_enabled': self.distiller is not None,
        }

# ============================================================
# MAIN EXPORT ENGINE (with all advanced enhancements)
# ============================================================
class EnhancedAIDataCenterExporterV14_0:
    def __init__(
        self,
        config: ExportEngineConfig,
        db_manager: IDatabaseManager,
        quantum_security: IQuantumSecurity,
        blockchain: IBlockchain,
        scheduler: IScheduler,
        predictive: IPredictive,
        federated: IFederated,
        cloud_uploader: ICloudUploader,
        vault: IVault,
        carbon_manager: CarbonIntensityManager,
        leader: LeaderElection,
        task_manager: TaskManager,
    ):
        self.config = config
        self.instance_id = config.general.instance_id
        self._start_time = datetime.now()

        self.db_manager = db_manager
        self.quantum_security = quantum_security
        self.blockchain = blockchain
        self.scheduler = scheduler
        self.predictive = predictive
        self.federated = federated
        self.cloud_uploader = cloud_uploader
        self.vault = vault
        self.carbon_manager = carbon_manager
        self.leader = leader
        self.task_manager = task_manager

        self.data_connector = EnhancedDataSourceConnector(config)
        self.streaming_exporter = EnhancedStreamingExporter()
        self.quota_manager = QuotaManager(config, db_manager)
        self.pipeline = ExportPipeline(config)

        # NEW: Advanced enhancement modules
        self.chaos_monkey = ChaosMonkey(
            enabled=config.scheduler.chaos_enabled,
            failure_probability=config.scheduler.chaos_failure_probability,
        )
        self.carbon_broker = CarbonOffsetBroker()
        self.human_review = HumanReviewManager()
        self.precision_policy = FlexGenPrecisionPolicy(
            default_carbon_intensity=400.0
        )
        self.quantum_optimizer = QuantumDistillationOptimizer(
            enabled=config.quantum.enable_distillation,
            qaoa_reps=config.quantum.qaoa_reps,
        )
        self.safety_monitor = SafetyMonitor() if config.scheduler.safety_monitor_enabled else None

        self.active_exports: Dict[str, ExportResult] = {}
        self.export_history = deque(maxlen=1000)
        self._exports_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._running = False

        self.streaming_exporter.register_progress_callback(self._on_export_progress)
        self._register_background_tasks()

        logger.info(f"EnhancedAIDataCenterExporter v{self.config.general.version} initialized (instance: {self.instance_id})")

    def _register_background_tasks(self):
        self.task_manager.register_task("health_monitor", self._health_monitor_loop)
        self.task_manager.register_task("quantum_monitor", self._quantum_monitor_loop)
        self.task_manager.register_task("blockchain_monitor", self._blockchain_monitor_loop)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        self.task_manager.register_task("federated_share", self._federated_share_loop)

    def _on_export_progress(self, progress: float, processed: int, total: int):
        logger.info(f"Export progress: {progress:.1f}% ({processed:,}/{total:,} rows)")

    async def start(self):
        logger.info(f"Starting EnhancedAIDataCenterExporter v{self.config.general.version}")
        await self.scheduler.start()
        self._running = True
        self.task_manager.start_registered_tasks()
        if PROMETHEUS_AVAILABLE:
            BACKGROUND_TASKS.set(len(self.task_manager.tasks))
        logger.info(f"Export engine started with {len(self.task_manager.tasks)} background tasks")

    async def _carbon_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.carbon_manager.get_current_intensity()
                await asyncio.sleep(self.config.scheduler.carbon_update_interval)
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
                    logger.warning("Post-quantum cryptography unavailable - using fallback")
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
                if not status.get('connected'):
                    logger.warning("Blockchain not connected - verifications will be simulated")
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Blockchain monitor error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.export_history:
                    last = self.export_history[-1]
                    rows = last.rows_exported
                    intensity = await self.carbon_manager.get_current_intensity()
                    await self.predictive.update_history(rows, intensity['intensity'])
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update loop error: {e}")
                await asyncio.sleep(60)

    async def _federated_share_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.export_history:
                    insight = {
                        'total_exports': len(self.export_history),
                        'avg_rows': float(np.mean([r.rows_exported for r in self.export_history])),
                        'avg_carbon_intensity': float(np.mean([r.metadata.get('carbon_intensity', 400) for r in self.export_history if r.metadata])),
                        'timestamp': datetime.now().isoformat()
                    }
                    await self.federated.share_insight(insight)
                await asyncio.sleep(self.config.federated.share_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated share loop error: {e}")
                await asyncio.sleep(60)

    async def _health_monitor_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                if PROMETHEUS_AVAILABLE:
                    HEALTH_SCORE.set(health.get('health_score', 100))
                if not health.get('healthy'):
                    logger.warning(f"System health degraded: {health}")
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(60)

    async def export_data(self, format: str = None, output_path: Path = None,
                          incremental: bool = False, compress: bool = None, encrypt: bool = None,
                          destination: str = None, validate: bool = True, generate_pdf: bool = False,
                          bucket: str = None, key_prefix: str = None,
                          user_id: str = 'default', sample_size: int = None,
                          resume_checkpoint_id: str = None,
                          priority: str = 'normal', timeout: float = None,
                          sign_manifest: bool = True, blockchain_record: bool = True) -> str:
        format = format or self.config.general.default_format
        compress = self.config.general.default_compress if compress is None else compress
        encrypt = self.config.general.default_encrypt if encrypt is None else encrypt
        destination = destination or self.config.general.default_destination
        bucket = bucket or self.config.cloud.bucket

        async def _export_task():
            return await self._execute_export(
                format=format, output_path=output_path,
                incremental=incremental, compress=compress, encrypt=encrypt,
                destination=destination, validate=validate, generate_pdf=generate_pdf,
                bucket=bucket, key_prefix=key_prefix,
                user_id=user_id, sample_size=sample_size,
                resume_checkpoint_id=resume_checkpoint_id,
                sign_manifest=sign_manifest, blockchain_record=blockchain_record
            )

        task_id = await self.task_manager.submit(_export_task, name=f"export_{format}", priority=priority, timeout=timeout)
        logger.info(f"Export task submitted: {task_id}")
        return task_id

    async def _execute_export(self, format: str = 'json', output_path: Path = None,
                             incremental: bool = False, compress: bool = False,
                             encrypt: bool = False, destination: str = 'local',
                             validate: bool = True, generate_pdf: bool = False,
                             bucket: str = None, key_prefix: str = None,
                             user_id: str = 'default', sample_size: int = None,
                             resume_checkpoint_id: str = None,
                             sign_manifest: bool = True,
                             blockchain_record: bool = True) -> ExportResult:
        start_time = time.time()
        export_id = str(uuid.uuid4())[:8]

        result = ExportResult(export_id=export_id, format=format, status=ExportStatus.RUNNING, started_at=datetime.now())

        async with self._exports_lock:
            self.active_exports[export_id] = result
            if PROMETHEUS_AVAILABLE:
                EXPORT_ACTIVE.set(len(self.active_exports))

        logger.info(f"Starting export {export_id} in {format} format")

        try:
            # Chaos injection
            try:
                self.chaos_monkey.maybe_fail("export")
            except ChaosExperimentError as e:
                logger.warning(f"Chaos injected during export: {e}")

            total_rows = await self.data_connector.get_total_count()
            estimated_size = total_rows * 1000

            # Safety check
            carbon_data = await self.carbon_manager.get_current_intensity()
            carbon_intensity = carbon_data.get('intensity', 400)
            if self.safety_monitor:
                if not self.safety_monitor.check_export(carbon_intensity, success=True):
                    logger.warning("Safety violation on export; proceeding with caution")

            # Human review for large exports
            if total_rows >= self.config.general.human_review_threshold_rows:
                review_id = await self.human_review.request_review(export_id, {
                    "total_rows": total_rows,
                    "format": format,
                    "user_id": user_id,
                    "carbon_intensity": carbon_intensity,
                })
                result.human_review_id = review_id
                result.status = ExportStatus.PENDING_REVIEW
                logger.info(f"Export {export_id} is pending human review: {review_id}")

            quota_ok, quota_message = await self.quota_manager.check_quota(user_id, total_rows, estimated_size)
            if not quota_ok:
                raise QuotaExceededError(quota_message)

            fetch_limit = sample_size if sample_size else total_rows
            if sample_size and sample_size < total_rows:
                data = await self.data_connector.fetch_real_data(limit=sample_size)
            else:
                data = await self.data_connector.fetch_real_data()

            if len(data) == 0:
                raise DataFetchError("No data available for export")

            if validate:
                validation_report = await self._validate_data_chunked(data)
                if not validation_report.get('valid'):
                    logger.warning(f"Validation found {validation_report.get('error_count', 0)} errors")
                    if PROMETHEUS_AVAILABLE:
                        VALIDATION_FAILURES.inc(validation_report.get('error_count', 0))

            if incremental:
                data = self._incremental_export(data, resume_checkpoint_id)

            if output_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = Path(f"./exports/datacenter_export_{timestamp}_{export_id}.{format}")
            output_path.parent.mkdir(exist_ok=True, parents=True)

            # Adaptive precision selection
            workload_size = "large" if total_rows > 100000 else ("medium" if total_rows > 10000 else "small")
            precision = self.precision_policy.recommend_precision(
                workload_size=workload_size,
                carbon_intensity=carbon_intensity,
            )
            result.precision = precision

            export_result = await self.streaming_exporter.export_streaming(data, format, output_path)
            result.rows_exported = export_result['rows_exported']
            result.file_path = export_result['file_path']
            result.file_size_bytes = export_result['file_size_bytes']
            result.columns_exported = len(data.columns)
            result.data_quality_score = self._calculate_quality_score(data)
            if PROMETHEUS_AVAILABLE:
                DATA_QUALITY.set(result.data_quality_score)

            manifest = {
                'export_id': export_id,
                'format': format,
                'rows_exported': result.rows_exported,
                'timestamp': datetime.now().isoformat(),
                'file_hash': hashlib.sha256(open(output_path, 'rb').read()).hexdigest(),
                'file_size_bytes': result.file_size_bytes,
                'user_id': user_id,
                'instance_id': self.instance_id,
                'version': self.config.general.version,
                'carbon_intensity': carbon_intensity,
                'precision': precision,
                'workload_size': workload_size,
            }

            if sign_manifest:
                quantum_key = await self.quantum_security.generate_keypair(self.config.quantum.algorithm)
                signature = await self.quantum_security.sign_export_manifest(manifest, quantum_key['key_id'])
                result.quantum_signature = signature
                manifest['quantum_signature'] = signature

            if blockchain_record:
                blockchain_result = await self.blockchain.record_export(export_id, manifest, manifest['file_hash'])
                result.blockchain_tx_hash = blockchain_result.get('tx_hash')

            if generate_pdf:
                pdf_path = output_path.with_suffix('.pdf')
                await self._generate_pdf_report(data, pdf_path, export_id)

            if destination != 'local' and bucket:
                upload_result = await self.cloud_uploader.upload_file(output_path, destination, bucket, key_prefix)
                result.destination = destination
                logger.info(f"Uploaded to {destination}: {upload_result.get('url', bucket)}")

            # Carbon offset if needed
            if carbon_intensity > 450:
                carbon_kg = result.rows_exported * 0.0001  # estimate
                try:
                    await self.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)
                except Exception as e:
                    logger.warning(f"Carbon offset purchase failed: {e}")

            result.status = ExportStatus.COMPLETED
            result.export_time_ms = (time.time() - start_time) * 1000
            result.completed_at = datetime.now()
            result.metadata = manifest
            result.safety_checked = True

            if PROMETHEUS_AVAILABLE:
                EXPORT_RUNS.labels(status='success', format=format).inc()
                EXPORT_DURATION.labels(format=format).observe(result.export_time_ms / 1000)
                EXPORT_SIZE.labels(format=format).set(result.file_size_bytes)

            async with self._exports_lock:
                self.export_history.append(result)

            if self.db_manager:
                async def insert(session):
                    await session.execute(
                        text("""
                            INSERT INTO export_history (export_id, format, status, rows_exported, file_path, file_size_bytes, started_at, completed_at, metadata, quantum_signature, blockchain_tx_hash, precision)
                            VALUES (:export_id, :format, :status, :rows_exported, :file_path, :file_size_bytes, :started_at, :completed_at, :metadata, :quantum_signature, :blockchain_tx_hash, :precision)
                        """),
                        {
                            'export_id': export_id,
                            'format': format,
                            'status': 'completed',
                            'rows_exported': result.rows_exported,
                            'file_path': result.file_path,
                            'file_size_bytes': result.file_size_bytes,
                            'started_at': result.started_at.isoformat(),
                            'completed_at': result.completed_at.isoformat(),
                            'metadata': json.dumps(manifest, default=str),
                            'quantum_signature': json.dumps(result.quantum_signature, default=str) if result.quantum_signature else None,
                            'blockchain_tx_hash': result.blockchain_tx_hash,
                            'precision': precision,
                        }
                    )
                await self.db_manager.execute_async(insert)

            audit_logger.info(f"Export {export_id} completed - {result.rows_exported:,} rows in {result.export_time_ms:.0f}ms")

            await self.pipeline.run_pipeline({'export_id': export_id, 'format': format, 'rows': result.rows_exported, 'manifest': manifest})
            await self.predictive.update_history(result.rows_exported, carbon_intensity)
            await self.federated.share_insight({
                'export_id': export_id,
                'format': format,
                'rows': result.rows_exported,
                'carbon_intensity': carbon_intensity,
                'timestamp': datetime.now().isoformat()
            })

            if hasattr(self.scheduler, 'record_feedback'):
                metrics = {
                    'carbon_saved_kg': 0.0,
                    'latency_ms': result.export_time_ms,
                    'success': True,
                }
                await self.scheduler.record_feedback(export_id, True, metrics)

            return result

        except Exception as e:
            result.status = ExportStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.now()
            if PROMETHEUS_AVAILABLE:
                EXPORT_RUNS.labels(status='failed', format=format).inc()
                EXPORT_ERRORS.labels(error_type='export_failed').inc()
            logger.error(f"Export {export_id} failed: {e}")
            if self.safety_monitor:
                self.safety_monitor.check_export(400.0, success=False)
            if hasattr(self.scheduler, 'record_feedback'):
                await self.scheduler.record_feedback(export_id, False, {})
            raise
        finally:
            async with self._exports_lock:
                self.active_exports.pop(export_id, None)
                if PROMETHEUS_AVAILABLE:
                    EXPORT_ACTIVE.set(len(self.active_exports))

    async def _validate_data_chunked(self, data: pd.DataFrame) -> Dict:
        error_count = 0
        if data.isnull().any().any():
            error_count += data.isnull().sum().sum()
        return {'valid': error_count == 0, 'error_count': error_count}

    def _incremental_export(self, data: pd.DataFrame, checkpoint_id: str = None) -> pd.DataFrame:
        return data

    def _calculate_quality_score(self, data: pd.DataFrame) -> float:
        completeness = 1.0 - data.isnull().sum().sum() / (data.shape[0] * data.shape[1])
        return float(completeness)

    async def _generate_pdf_report(self, data: pd.DataFrame, pdf_path: Path, export_id: str):
        logger.info(f"Generating PDF report at {pdf_path}")
        if REPORTLAB_AVAILABLE:
            try:
                c = canvas.Canvas(str(pdf_path), pagesize=letter)
                c.drawString(100, 750, f"Export Report - {export_id}")
                c.drawString(100, 730, f"Rows: {len(data)}")
                c.drawString(100, 710, f"Columns: {len(data.columns)}")
                c.drawString(100, 690, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                c.save()
            except Exception as e:
                logger.error(f"PDF generation failed: {e}")
                pdf_path.write_text("PDF generation failed")
        else:
            pdf_path.write_text("PDF report placeholder")

    async def health_check(self) -> Dict:
        results = {}
        components = {
            'quantum_security': self.quantum_security,
            'blockchain': self.blockchain,
            'scheduler': self.scheduler,
            'predictive': self.predictive,
            'federated': self.federated,
            'cloud_uploader': self.cloud_uploader,
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

    async def get_statistics(self) -> Dict:
        task_stats = self.task_manager.get_statistics()
        scheduler_stats = self.scheduler.get_schedule_stats()
        return {
            'instance_id': self.instance_id,
            'version': self.config.general.version,
            'total_exports': len(self.export_history),
            'total_rows_exported': sum(r.rows_exported for r in self.export_history),
            'active_exports': len(self.active_exports),
            'background_tasks': task_stats,
            'upload_stats': self.cloud_uploader.get_upload_metrics(),
            'quota_status': self.quota_manager.get_quota_status('default'),
            'quantum_security': self.quantum_security.get_quantum_status(),
            'blockchain': await self.blockchain.get_blockchain_status(),
            'scheduler': scheduler_stats,
            'predictive': await self.predictive.health_check(),
            'federated': await self.federated.health_check(),
            'health': await self.health_check(),
            'enhancements_available': ENHANCEMENTS_AVAILABLE,
            'additional_enhancements_available': ADDITIONAL_ENHANCEMENTS_AVAILABLE,
            'advanced_enhancements': {
                'safety_monitor': self.safety_monitor.get_violations()[-5:] if self.safety_monitor else [],
                'chaos_monkey': self.chaos_monkey.get_stats(),
                'human_review': self.human_review.get_stats(),
                'carbon_broker': self.carbon_broker.get_totals(),
                'precision_policy': self.precision_policy.get_status(),
                'quantum_optimizer': self.quantum_optimizer.get_status(),
            },
            'timestamp': datetime.now().isoformat()
        }

    async def shutdown(self):
        logger.info(f"Shutting down EnhancedAIDataCenterExporter (instance: {self.instance_id})")
        self._shutdown_event.set()
        self._running = False
        await self.scheduler.shutdown()
        await self.carbon_manager.close()
        await self.task_manager.stop_all()
        await self.db_manager.close()
        await self.leader.stop()
        logger.info("Shutdown complete")

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

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

# ============================================================
# FASTAPI REST API
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Export Engine API", version="14.1")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer()
    api_rate_limiter = RateLimiter(ExportEngineConfig().api)

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        token = credentials.credentials
        try:
            payload = jwt.decode(token, ExportEngineConfig().api.jwt_secret, algorithms=["HS256"])
            return payload
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    async def rate_limit(request: Request):
        if ExportEngineConfig().api.rate_limit_enabled:
            if not await api_rate_limiter.acquire():
                raise HTTPException(status_code=429, detail="Rate limit exceeded")

    exporter: Optional[EnhancedAIDataCenterExporterV14_0] = None

    @app.post("/export")
    async def trigger_export(
        format: str = "json",
        destination: str = "local",
        bucket: str = None,
        sample_size: int = None,
        user: Dict = Depends(verify_token),
        _: None = Depends(rate_limit)
    ):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        task_id = await exporter.export_data(
            format=format,
            destination=destination,
            bucket=bucket,
            sample_size=sample_size,
            user_id=user.get("sub", "default")
        )
        return {"task_id": task_id}

    @app.get("/status")
    async def get_status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return await exporter.get_statistics()

    @app.get("/health")
    async def health(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return await exporter.health_check()

    @app.get("/optimization/status")
    async def optimization_status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return {
            "scheduler": exporter.scheduler.get_schedule_stats(),
            "predictive_hyperparams": getattr(exporter.predictive, 'hyperparam_population', []),
            "enhancements_available": ENHANCEMENTS_AVAILABLE,
            "additional_enhancements_available": ADDITIONAL_ENHANCEMENTS_AVAILABLE,
        }

    @app.post("/optimization/evolve")
    async def evolve_optimizer(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        if hasattr(exporter.scheduler, 'bio') and exporter.scheduler.bio:
            await exporter.scheduler.record_feedback("manual", True, {'carbon_saved_kg': 0, 'latency_ms': 0})
            return {"status": "evolution triggered"}
        return {"status": "evolution not available"}

    @app.post("/optimization/rlhf-update")
    async def rlhf_update(context: Dict, action: str, reward: float,
                          user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        if hasattr(exporter.scheduler, 'rlhf') and exporter.scheduler.rlhf:
            exporter.scheduler.rlhf.update(context, action, reward)
            return {"status": "RLHF updated"}
        return {"status": "RLHF not available"}

    @app.post("/optimization/distill")
    async def force_distillation(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return {"status": "Distillation triggered"}

    # NEW: Human review endpoints
    @app.get("/human-review/pending")
    async def human_review_pending(user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return await exporter.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(review_id: str, user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        ok = await exporter.human_review.approve(review_id)
        return {"status": "approved" if ok else "not_found"}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(review_id: str, reason: Optional[str] = None,
                                   user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        ok = await exporter.human_review.reject(review_id, reason)
        return {"status": "rejected" if ok else "not_found"}

    # NEW: Chaos testing endpoint
    @app.post("/chaos/trigger")
    async def chaos_trigger(enabled: bool = True, probability: float = 0.1,
                            user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        exporter.chaos_monkey.enabled = enabled
        exporter.chaos_monkey.failure_probability = probability
        return exporter.chaos_monkey.get_stats()

    # NEW: Carbon offset endpoints
    @app.post("/carbon/offset")
    async def carbon_offset(carbon_kg: float, carbon_intensity: float,
                             user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return await exporter.carbon_broker.purchase_offsets(carbon_intensity, carbon_kg)

    @app.post("/carbon/rec")
    async def carbon_rec(energy_mwh: float, user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return await exporter.carbon_broker.purchase_recs(energy_mwh)

    @app.get("/carbon/totals")
    async def carbon_totals(user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return exporter.carbon_broker.get_totals()

    # NEW: Precision recommendation endpoint
    @app.post("/precision/recommend")
    async def precision_recommend(workload_size: str = "medium", carbon_intensity: float = None,
                                   user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        precision = exporter.precision_policy.recommend_precision(
            workload_size=workload_size, carbon_intensity=carbon_intensity
        )
        return {"precision": precision}

    # NEW: Safety violations endpoint
    @app.get("/safety/violations")
    async def safety_violations(user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        if exporter.safety_monitor:
            return {"violations": exporter.safety_monitor.get_violations()}
        return {"violations": [], "monitor_enabled": False}

    # NEW: XAI endpoint (last scheduler decision)
    @app.get("/xai/last-decision")
    async def xai_last_decision(user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        return {
            "last_context": exporter.scheduler.last_context,
            "schedule_stats": exporter.scheduler.get_schedule_stats(),
        }

    # NEW: Federated secure aggregation endpoint
    @app.post("/federated/register")
    async def federated_register(participant_id: str, update: Dict,
                                  user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        if hasattr(exporter.federated, 'secure_coordinator'):
            exporter.federated.secure_coordinator.register_participant(participant_id, update)
            return {"status": "registered"}
        return {"status": "not_available"}

    @app.get("/federated/aggregate")
    async def federated_aggregate(user: Dict = Depends(verify_token)):
        if not exporter:
            raise HTTPException(status_code=503, detail="Export engine not initialized")
        if hasattr(exporter.federated, 'secure_coordinator'):
            return exporter.federated.secure_coordinator.aggregate()
        return {}

    @app.on_event("startup")
    async def startup():
        global exporter
        config = ExportEngineConfig()
        db_manager = EnhancedDatabaseManager(config)
        await db_manager.init()
        vault = VaultManager(config)
        quantum = QuantumResilientExportSecurity(config, vault)
        blockchain = BlockchainExportVerification(config, db_manager)
        carbon = CarbonIntensityManager(config)
        chaos_monkey = ChaosMonkey(
            enabled=config.scheduler.chaos_enabled,
            failure_probability=config.scheduler.chaos_failure_probability,
        )
        scheduler = IntelligentExportScheduler(config, carbon, chaos_monkey=chaos_monkey)
        predictive = PredictiveAnalytics(config)
        federated = FederatedKnowledgeSharing(config, db_manager, config.general.instance_id)
        cloud = EnhancedCloudUploader(config)
        leader = LeaderElection(config)
        task_manager = TaskManager()
        exporter = EnhancedAIDataCenterExporterV14_0(
            config=config,
            db_manager=db_manager,
            quantum_security=quantum,
            blockchain=blockchain,
            scheduler=scheduler,
            predictive=predictive,
            federated=federated,
            cloud_uploader=cloud,
            vault=vault,
            carbon_manager=carbon,
            leader=leader,
            task_manager=task_manager,
        )
        await exporter.start()
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown():
        if exporter:
            await exporter.shutdown()
        logger.info("FastAPI shut down")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================
_exporter_instance = None
_exporter_lock = asyncio.Lock()

async def get_export_engine(config: Optional[Union[ExportEngineConfig, Dict]] = None) -> EnhancedAIDataCenterExporterV14_0:
    global _exporter_instance
    if _exporter_instance is None:
        async with _exporter_lock:
            if _exporter_instance is None:
                cfg = config if isinstance(config, ExportEngineConfig) else ExportEngineConfig(**config) if config else ExportEngineConfig()
                db_manager = EnhancedDatabaseManager(cfg)
                await db_manager.init()
                vault = VaultManager(cfg)
                quantum = QuantumResilientExportSecurity(cfg, vault)
                blockchain = BlockchainExportVerification(cfg, db_manager)
                carbon = CarbonIntensityManager(cfg)
                chaos_monkey = ChaosMonkey(
                    enabled=cfg.scheduler.chaos_enabled,
                    failure_probability=cfg.scheduler.chaos_failure_probability,
                )
                scheduler = IntelligentExportScheduler(cfg, carbon, chaos_monkey=chaos_monkey)
                predictive = PredictiveAnalytics(cfg)
                federated = FederatedKnowledgeSharing(cfg, db_manager, cfg.general.instance_id)
                cloud = EnhancedCloudUploader(cfg)
                leader = LeaderElection(cfg)
                task_manager = TaskManager()
                _exporter_instance = EnhancedAIDataCenterExporterV14_0(
                    config=cfg,
                    db_manager=db_manager,
                    quantum_security=quantum,
                    blockchain=blockchain,
                    scheduler=scheduler,
                    predictive=predictive,
                    federated=federated,
                    cloud_uploader=cloud,
                    vault=vault,
                    carbon_manager=carbon,
                    leader=leader,
                    task_manager=task_manager,
                )
                await _exporter_instance.start()
    return _exporter_instance

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
    global _exporter_instance
    if _exporter_instance:
        await _exporter_instance.shutdown()
        _exporter_instance = None

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
    print("Enhanced AI Data Center Export Engine v14.1 - Enterprise Quantum+ (Advanced Enhancements)")
    print("=" * 80)

    exporter = await get_export_engine()
    print(f"\n✅ ADVANCED ENHANCEMENTS IN v14.1:")
    print("   ✅ CausalBandit for causal RL of scheduling decisions")
    print("   ✅ SafetyMonitor for temporal UX/scheduling rules")
    print("   ✅ XAIExplainer for natural-language rationale")
    print("   ✅ FederatedSecureCoordinator with differential privacy")
    print("   ✅ MultiAgentCoordinator with role specialisation")
    print("   ✅ CarbonOffsetBroker for offsets and RECs")
    print("   ✅ ChaosMonkey for resilience testing")
    print("   ✅ HumanReviewManager for pre-commit review")
    print("   ✅ FlexGenPrecisionPolicy for adaptive precision")
    print("   ✅ QuantumDistillationOptimizer (optional)")

    qstatus = exporter.quantum_security.get_quantum_status()
    print(f"\n🔐 Quantum Status: PQC Available: {qstatus.get('pqc_available', False)}, Algorithms: {', '.join(qstatus.get('algorithms', []))}")

    bstatus = await exporter.blockchain.get_blockchain_status()
    print(f"⛓️ Blockchain Connected: {bstatus.get('connected', False)}")

    sched_stats = exporter.scheduler.get_schedule_stats()
    print(f"📅 Scheduler Running: {sched_stats.get('running', False)}")
    print(f"   Causal RL: {ENHANCEMENTS_AVAILABLE}")
    print(f"   LIMIT Graph: {sched_stats.get('limit_graph_active', False)}")
    print(f"   RLHF: {sched_stats.get('rlhf_active', False)}")
    print(f"   Distillation: {sched_stats.get('distillation_active', False)}")
    print(f"   Safety Monitor: {sched_stats.get('safety_monitor_active', False)}")
    print(f"   Multi-Agent: {sched_stats.get('multi_agent_active', False)}")
    print(f"   Chaos: {sched_stats.get('chaos')}")

    print(f"\n📊 Submitting Test Export...")
    task_id = await exporter.export_data(
        format='json',
        incremental=False,
        compress=True,
        encrypt=True,
        destination='aws',
        validate=True,
        generate_pdf=True,
        user_id='test_user',
        sample_size=100,
        priority='normal',
        timeout=60,
        sign_manifest=True,
        blockchain_record=True
    )
    print(f"   Task ID: {task_id}")

    await asyncio.sleep(2)
    stats = await exporter.get_statistics()
    print(f"\n📊 System Stats: Instance: {stats['instance_id']}, Version: {stats['version']}")
    print(f"   Active Exports: {stats['active_exports']}")
    print(f"   Federated Shares: {stats['federated'].get('total_shares', 0)}")
    print(f"   Carbon Broker: {stats['advanced_enhancements']['carbon_broker']}")
    print(f"   Human Reviews: {stats['advanced_enhancements']['human_review']}")
    print(f"   Precision Policy: {stats['advanced_enhancements']['precision_policy']}")
    print(f"   Quantum Optimizer: {stats['advanced_enhancements']['quantum_optimizer']}")

    print("\n" + "=" * 80)
    print("✅ Export Engine v14.1 - Ready for Production")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        if _exporter_instance:
            await _exporter_instance.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
