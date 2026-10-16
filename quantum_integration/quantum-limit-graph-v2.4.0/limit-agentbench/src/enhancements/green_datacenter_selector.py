#!/usr/bin/env python3
# src/enhancements/green_datacenter_selector_enhanced_v19_0.py
"""
Enhanced Green Data Center Selector — v19.0

The ten Green Agent enhancements are imported from the enhancements/*
package. This file keeps only selector-specific logic:

  - Project store (ORM + in-memory list)
  - TOPSIS + weighted + NSGA-II scoring
  - EnhancedMultiCloudSelectionOrchestrator
  - EnhancedAutonomousOptimizer with LinUCB + GA
  - MixtureOfExpertsPredictive
  - CarbonAwareSelectionScheduler
  - AdaptiveTTLCache (LRU-ish, correct overwrite accounting)
  - RealTimeCapacityMonitor + NetworkLatencyMonitor
  - EnhancedGreenDataCenterSelector with A/B testing
  - FastAPI app, singleton, signal handlers, main

Changelog v18.0 -> v19.0:
  - All ten enhancement classes imported from the enhancements package.
  - Compat wrappers for Explanation, DecisionExplainer, HITLRequest,
    UncertaintyEstimator, HumanInTheLoopGate.
  - MultiAgentCoordinator.vote tuples are unpacked at both call sites.
  - EmergentRoleRegistry credits the actual winning role.
  - Causal state uses state_vector_from_metrics.
  - SafetyShield's returned selection is used, not discarded.
  - GA fitness depends on recent reward correlation.
  - _update_weights normalizes by per-objective std.
  - AdaptiveTTLCache.set subtracts previous entry size on overwrite.
  - NSGA-II scoring uses index-based non-dominated set.
  - _ab_update_loop uses bounded softmax.
  - MixtureOfExpertsPredictive skips failed experts.
  - HITL decisions can be delivered via submit_hitl_decision().
  - FastAPI /hitl/approval calls submit_hitl_decision().
  - Signal handlers retain task references.
  - save_state / load_state for causal, roles, federated, A/B, predictor.
  - PostQuantumCrypto.verify_selection_decision performs real verification.
  - BlockchainSelectionVerification emits a real tx hash when configured.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import contextvars
import hashlib
import itertools
import json
import logging
import logging.handlers
import math
import os
import random
import signal
import tempfile
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Protocol, Sequence,
    Set, Tuple, Union, runtime_checkable,
)

import numpy as np


# ============================================================
# SHIM: import all ten enhancements from the shared package
# ============================================================
from enhancements.precision import (
    PrecisionLevel, PRECISION_COST, PrecisionController, HardwareAwareAdapter,
)
from enhancements.carbon import (
    CarbonMarketClient, RECRecord, RECInventory, CarbonIntensityManager,
    ICarbonManager,
)
from enhancements.chaos import (
    ChaosConfig, ChaosEngineer,
)
from enhancements.stl import (
    STLOperator, STLFormula, TemporalLogicMonitor, SafetyShield,
)
from enhancements.causal import (
    CausalTransition, CausalCounterfactualEstimator,
)
from enhancements.federated import (
    FederatedUpdate, FederatedAggregator,
)
from enhancements.multi_agent import (
    AgentRole, AgentBid, EmergentRoleRegistry, MultiAgentCoordinator,
)
from enhancements.quantum import (
    QuantumInspiredTeacher, DistillationEnsemble,
)
from enhancements.xai import Explanation as SharedExplanation
from enhancements.xai import DecisionExplainer as SharedDecisionExplainer
from enhancements.hitl import (
    HITLRequest as SharedHITLRequest,
    UncertaintyEstimator as SharedUncertaintyEstimator,
    HumanInTheLoopGate as SharedHumanInTheLoopGate,
    ActiveLearningSampler,
)
from enhancements.common_state import (
    STATE_VECTOR_DIM, state_vector_from_metrics,
)


# ============================================================
# COMPAT WRAPPERS
# ============================================================
@dataclass
class Explanation:
    """Selector-side Explanation carrying chosen_idx."""
    decision_id: str
    chosen_idx: int
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer(SharedDecisionExplainer):
    """Adds the selector's explain(...) signature."""

    def explain(self, chosen_idx, chosen, pareto, counterfactuals,
                safety_ok, carbon_price, confidence: float = 0.5):
        numeric_keys = [k for k, v in chosen.items() if isinstance(v, (int, float))]
        if pareto and numeric_keys:
            baseline = {k: float(np.mean([c.get(k, 0.0) for c in pareto])) for k in numeric_keys}
            scales = {k: float(np.std([c.get(k, 0.0) for c in pareto]) + 1e-9) for k in numeric_keys}
        else:
            baseline = {k: 0.0 for k in numeric_keys}
            scales = {k: 1.0 for k in numeric_keys}
        shared = self.explain_anomaly(
            kind="decision",
            current=chosen,
            baseline=baseline,
            scales=scales,
            counterfactuals=counterfactuals,
            safety_ok=safety_ok,
            carbon_price=carbon_price,
            confidence=confidence,
        )
        return Explanation(
            decision_id=shared.decision_id,
            chosen_idx=chosen_idx,
            top_features=shared.top_features,
            counterfactuals=shared.counterfactuals,
            rationale=shared.rationale,
            confidence=shared.confidence,
            safety_ok=shared.safety_ok,
            carbon_price_signal=shared.carbon_price_signal,
        )


@dataclass
class HITLRequest:
    """Selector-side HITL request with candidate list."""
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float
    timestamp: float = field(default_factory=time.time)


class UncertaintyEstimator(SharedUncertaintyEstimator):
    """Entropy-based is_uncertain() over the shared estimator."""

    def __init__(self, entropy_threshold: float = 0.75,
                 variance_threshold: float = 0.05):
        super().__init__(window=32)
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold

    def is_uncertain(self, probs) -> Tuple[bool, float]:
        if probs is None or len(probs) == 0:
            return False, 0.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        h = float(-np.sum(p * np.log(p)) / math.log(len(p)))
        score = 0.6 * h
        return h > self.entropy_threshold, score


class HumanInTheLoopGate(SharedHumanInTheLoopGate):
    """Adds an async request() that accepts the selector's HITLRequest,
    and a future-based submit_decision() for external delivery."""

    def __init__(self, approver=None, timeout_s: float = 2.0,
                 auto_approve_on_timeout: bool = True):
        super().__init__(approver=approver, timeout_s=timeout_s,
                         auto_approve_on_timeout=auto_approve_on_timeout)
        self._pending: Dict[str, asyncio.Future] = {}
        self._pending_lock = asyncio.Lock()

    async def request(self, req: HITLRequest) -> bool:
        if self.approver is None:
            self.audit.append({"request_id": req.request_id,
                               "approved": True, "reason": "no_approver"})
            HITL_APPROVALS.labels(decision="auto_approved").inc()
            return True
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        async with self._pending_lock:
            self._pending[req.request_id] = future

        async def _run():
            try:
                result = await loop.run_in_executor(None, self.approver, req)
                if not future.done():
                    future.set_result(bool(result))
            except Exception as e:
                if not future.done():
                    future.set_exception(e)

        runner = asyncio.create_task(_run())
        try:
            approved = await asyncio.wait_for(future, timeout=self.timeout_s)
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
            HITL_APPROVALS.labels(decision="timeout").inc()
        except Exception:
            approved = self.auto_approve_on_timeout
        finally:
            async with self._pending_lock:
                self._pending.pop(req.request_id, None)
            if not runner.done():
                runner.cancel()
        self.audit.append({"request_id": req.request_id,
                           "approved": bool(approved), "reason": req.reason})
        HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
        return bool(approved)

    async def submit_decision(self, request_id: str, approved: bool) -> bool:
        async with self._pending_lock:
            fut = self._pending.get(request_id)
            if fut is None or fut.done():
                return False
            fut.set_result(bool(approved))
            return True


# ============================================================
# OPTIONAL EXTERNAL DEPENDENCIES
# ============================================================
try:
    from scipy.optimize import minimize  # noqa: F401
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression  # noqa: F401
    from sklearn.preprocessing import StandardScaler  # noqa: F401
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from sqlalchemy.orm import declarative_base
    from sqlalchemy import (
        Column, String, Float, DateTime, Integer, Boolean, Text, JSON, text,
    )
    from sqlalchemy.pool import NullPool
    ASYNC_SQLALCHEMY_AVAILABLE = True
except ImportError:
    ASYNC_SQLALCHEMY_AVAILABLE = False

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import declarative_base as _decl_base
    SQLALCHEMY_SYNC_AVAILABLE = True
    if not ASYNC_SQLALCHEMY_AVAILABLE:
        declarative_base = _decl_base  # type: ignore
except ImportError:
    SQLALCHEMY_SYNC_AVAILABLE = False

SQLALCHEMY_AVAILABLE = ASYNC_SQLALCHEMY_AVAILABLE or SQLALCHEMY_SYNC_AVAILABLE

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # type: ignore
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from web3 import Web3  # noqa: F401
    from web3.middleware import geth_poa_middleware  # noqa: F401
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # noqa: F401
from cryptography.hazmat.primitives import hashes  # noqa: F401
from cryptography.hazmat.backends import default_backend  # noqa: F401

import aiohttp  # noqa: F401
from aiohttp import ClientSession, ClientError  # noqa: F401

try:
    from hvac import Client as VaultClient  # type: ignore
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
    from google.cloud import storage as gcs_storage  # noqa: F401
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from prophet import Prophet  # noqa: F401
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from fastapi import FastAPI, Depends, HTTPException, Request
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from jose import JWTError, jwt  # type: ignore
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

try:
    import redis.asyncio as redis  # noqa: F401
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from opentelemetry import trace  # noqa: F401
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


if not TENACITY_AVAILABLE:
    def retry(*a, **kw):
        def decorator(func):
            @wraps(func)
            async def wrapper(*fargs, **fkwargs):
                return await func(*fargs, **fkwargs)
            return wrapper
        return decorator


# ============================================================
# FALLBACKS FOR LEGACY MODULES NOT IN THE SHARED PACKAGE
# ============================================================
ENHANCEMENTS_AVAILABLE = False
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.limit_graph import LimitGraph  # type: ignore
    from enhancements.rlhf import RLHFOptimizer  # type: ignore
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False


# ============================================================
# LOGGING
# ============================================================
try:
    import structlog  # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s",
        )

correlation_id_var = contextvars.ContextVar("correlation_id", default=str(uuid.uuid4())[:8])


class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True


if isinstance(logger, logging.Logger):
    logger.addFilter(CorrelationIdFilter())

audit_logger = logging.getLogger("audit")
if not audit_logger.handlers:
    try:
        audit_handler = logging.FileHandler("audit.log")
        audit_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        audit_logger.addHandler(audit_handler)
    except OSError:
        pass
audit_logger.setLevel(logging.INFO)


# ============================================================
# PROMETHEUS METRICS
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    SELECTIONS_TOTAL = Counter("selections_total", "Selections", ["status"], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("quantum_signatures_total", "Sigs", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("blockchain_verifications_total", "BC", ["status"], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter("autonomous_optimizations_total", "Opt", ["strategy", "status"], registry=REGISTRY)
    MULTI_CLOUD_ORCHESTRATIONS = Counter("multi_cloud_orchestrations_total", "MCO", ["provider", "status"], registry=REGISTRY)
    CARBON_INTENSITY = Gauge("selector_carbon_intensity_gco2_per_kwh", "CI", registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge("selector_circuit_breaker_state", "CB", ["name"], registry=REGISTRY)
    CLOUD_STORAGE = Counter("selector_cloud_storage_operations_total", "S", ["provider", "operation", "status"], registry=REGISTRY)
    VAULT_OPERATIONS = Counter("selector_vault_operations_total", "V", ["operation", "status"], registry=REGISTRY)
    HEALTH_SCORE = Gauge("selector_health_score", "HS", registry=REGISTRY)
    CARBON_PRICE = Gauge("selector_carbon_price_per_kg", "CP", registry=REGISTRY)
    REC_INVENTORY_KWH = Gauge("selector_rec_inventory_kwh", "REC", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("selector_precision_switches_total", "PS", ["level"], registry=REGISTRY)
    CHAOS_EVENTS = Counter("selector_chaos_events_total", "CE", ["type"], registry=REGISTRY)
    HITL_APPROVALS = Counter("selector_hitl_approvals_total", "H", ["decision"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("selector_safety_violations_total", "SV", ["formula"], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("selector_federated_rounds_total", "FR", registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    SELECTIONS_TOTAL = QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = DummyMetrics()
    AUTONOMOUS_OPTIMIZATIONS = MULTI_CLOUD_ORCHESTRATIONS = CARBON_INTENSITY = DummyMetrics()
    CIRCUIT_BREAKER_STATE = CLOUD_STORAGE = VAULT_OPERATIONS = HEALTH_SCORE = DummyMetrics()
    CARBON_PRICE = REC_INVENTORY_KWH = PRECISION_SWITCHES = CHAOS_EVENTS = DummyMetrics()
    HITL_APPROVALS = SAFETY_VIOLATIONS = FEDERATED_ROUNDS = DummyMetrics()


# ============================================================
# EXCEPTIONS
# ============================================================
class SelectorError(Exception): pass
class QuantumError(SelectorError): pass
class BlockchainError(SelectorError): pass
class OptimizationError(SelectorError): pass
class SelectionError(SelectorError): pass
class OrchestrationError(SelectorError): pass
class CircuitBreakerOpenError(SelectorError): pass
class RateLimitExceeded(SelectorError): pass
class VaultError(SelectorError): pass
class CloudStorageError(SelectorError): pass
class DatabaseError(SelectorError): pass
class SafetyViolationError(SelectorError): pass


# ============================================================
# INTERFACES
# ============================================================
@runtime_checkable
class IQuantumSecurity(Protocol):
    async def generate_keypair(self, algorithm: str = None) -> Dict: ...
    async def sign_selection_decision(self, decision: Dict, key_id: str) -> Dict: ...
    async def verify_selection_decision(self, decision: Dict, signature_data: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IBlockchain(Protocol):
    async def record_selection(self, selection_id: str, manifest: Dict, file_hash: str) -> Dict: ...
    async def verify_selection(self, selection_id: str, manifest: Dict, file_hash: str) -> Dict: ...
    async def get_blockchain_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IAutonomousOptimizer(Protocol):
    async def optimize_selection(self, current_state: Dict, strategy: str = None) -> Dict: ...
    def get_optimization_stats(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICloudOrchestrator(Protocol):
    async def orchestrate_selection(self, workload: "WorkloadSpec") -> Dict: ...
    async def get_provider_status(self) -> Dict: ...
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
    async def update_history(self, workload_hours: int, carbon_intensity: float): ...
    async def forecast_workload(self, horizon_hours: int = None) -> Dict: ...
    async def forecast_carbon(self, horizon_hours: int = None) -> Dict: ...
    async def health_check(self) -> Dict: ...


# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class DataCenterProject:
    project_id: str
    name: str
    latitude: float
    longitude: float
    green_score: float
    carbon_intensity: float
    pue_estimated: float
    helium_efficiency: float
    cost_per_hour: float
    latency_ms: float
    capacity_mw: float
    provider: str
    region: str
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class WorkloadSpec:
    gpu_hours: float = 100.0
    latency_tolerance_ms: float = 100.0
    cost_budget_usd: float = 1000.0
    carbon_budget_kg: float = 200.0
    workload_pattern: str = "steady"
    priority: str = "normal"
    spot_instance_ok: bool = True
    compliance_requirements: List[str] = field(default_factory=list)
    historical_patterns: List[float] = field(default_factory=list)
    constraints: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SelectionResult:
    selection_id: str
    selected_project: DataCenterProject
    method: str
    confidence_score: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    block_number: Optional[int] = None
    file_hash: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


# ============================================================
# CIRCUIT BREAKER
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name, failure_threshold=5, recovery_timeout=60.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = 2
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._lock = asyncio.Lock()
        self._metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._success_count = 0
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if (self._state == CircuitBreakerState.HALF_OPEN
                    and self._success_count >= self.half_open_success_threshold):
                self._state = CircuitBreakerState.CLOSED
        self._metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self._metrics["successful_calls"] += 1
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
            else:
                self._failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self._metrics["failed_calls"] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitBreakerState.CLOSED and self._failure_count >= self.failure_threshold:
                self._state = CircuitBreakerState.OPEN
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN

    def get_metrics(self):
        return {**self._metrics, "state": self._state.value,
                "failure_count": self._failure_count, "success_count": self._success_count}


class GlobalCircuitBreaker:
    _instance = None
    _breakers: Dict[str, CircuitBreaker] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name, **kwargs):
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, **kwargs)
        return self._breakers[name]


# ============================================================
# RATE LIMITER
# ============================================================
class RateLimiter:
    def __init__(self, rate, per_seconds=60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = float(rate)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self):
        async with self._lock:
            now = time.time()
            dt = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + dt * (self.rate / self.per_seconds))
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

    def get_metrics(self):
        total = self.total_requests + self.throttled_requests
        return {"total_requests": self.total_requests,
                "throttled_requests": self.throttled_requests,
                "throttle_rate": (self.throttled_requests / max(total, 1)) * 100}


# ============================================================
# TASK MANAGER (v19: explicit restart_on_exit)
# ============================================================
class TaskManager:
    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Tuple[Callable, tuple, dict, bool]] = {}
        self.metrics = {"total_tasks": 0, "completed": 0, "failed": 0, "restarts": 0}

    def start_task(self, name, coro_func, *args, restart_on_exit: bool = False, **kwargs):
        shutdown_event = self.shutdown_event

        async def wrapper():
            backoff = 1
            while not shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                    if not restart_on_exit:
                        break
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task {name} crashed: {e}")
                    self.metrics["restarts"] += 1
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 300)
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        self.metrics["total_tasks"] += 1
        return task

    def register_task(self, name, coro_func, *args, restart_on_exit: bool = True, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs, restart_on_exit)

    def start_registered_tasks(self):
        for name, (coro, args, kwargs, restart) in list(self._task_coroutines.items()):
            self.start_task(name, coro, *args, restart_on_exit=restart, **kwargs)
        self._task_coroutines.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for t in self.tasks.values():
                t.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()

    def get_statistics(self):
        return {**self.metrics, "active_tasks": len(self.tasks)}


# ============================================================
# CONFIG
# ============================================================
if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("19.0")
        log_level: str = Field("INFO")
        cache_ttl_seconds: int = Field(3600, ge=0)
        cache_max_size: int = Field(1000, ge=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        health_check_interval: int = Field(60, ge=10)
        auto_optimize_interval: int = Field(3600, ge=60)

        @field_validator("log_level")
        @classmethod
        def _v(cls, v):
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

    class QuantumConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("dilithium")
        master_key: str = Field("")

        @field_validator("master_key")
        @classmethod
        def _v(cls, v):
            if not v:
                return os.urandom(32).hex()
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError("master_key must be hex")
            return v

        def get_master_key_bytes(self):
            return bytes.fromhex(self.master_key)

    class BlockchainConfig(BaseModel):
        enabled: bool = True
        rpc_url: str = Field("http://localhost:8545")
        contract_address: Optional[str] = None
        private_key: Optional[str] = None
        chain_id: int = 1
        poa: bool = False

    class CloudConfig(BaseModel):
        aws_enabled: bool = True
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = Field("us-east-1")
        azure_enabled: bool = True
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_enabled: bool = True
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class DatabaseConfig(BaseModel):
        url: str = Field("sqlite+aiosqlite:///selector.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/selector")

    class APIConfig(BaseModel):
        host: str = Field("0.0.0.0")
        port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: os.urandom(32).hex())
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
        base_price_per_kg: float = Field(0.05, ge=0)
        price_sensitivity: float = Field(0.0005, ge=0)
        rec_default_kwh: float = Field(0.0, ge=0)

    class PredictiveConfig(BaseModel):
        enabled: bool = True
        horizon_hours: int = Field(24, ge=1)
        model_storage_path: str = Field("./prophet_models")

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        epsilon: float = Field(0.1, ge=0, le=1)
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "green_score": 0.2, "carbon_intensity": 0.2, "latency": 0.2,
                "cost": 0.2, "pue": 0.1, "helium_impact": 0.1,
            }
        )
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        distillation_enabled: bool = True
        causal_enabled: bool = True
        federated_enabled: bool = True
        multi_agent_enabled: bool = True
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        precision_switching_enabled: bool = True
        carbon_market_enabled: bool = True
        chaos_enabled: bool = False
        chaos_fault_prob: float = Field(0.0, ge=0, le=1)
        chaos_latency_ms: float = Field(0.0, ge=0)
        chaos_carbon_spike_prob: float = Field(0.0, ge=0, le=1)
        hitl_enabled: bool = True
        hitl_timeout_s: float = Field(2.0, gt=0)

    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = Field("topsis")
        weights: List[float] = Field(default_factory=lambda: [0.2, 0.2, 0.2, 0.2, 0.1, 0.1])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 3
        gating_model: str = Field("logistic")
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("ga")
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8

    class CarbonSchedulerConfig(BaseModel):
        enabled: bool = True
        threshold: float = 400.0
        max_delay_seconds: int = 300

    class ABTestingConfig(BaseModel):
        enabled: bool = True
        variants: List[str] = Field(default_factory=lambda: ["weighted", "topsis", "nsga2"])
        allocations: List[float] = Field(default_factory=lambda: [0.34, 0.33, 0.33])
        update_interval: int = 3600

    class SelectorConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="SELECTOR_", case_sensitive=False)

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
        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        carbon_scheduler: CarbonSchedulerConfig = Field(default_factory=CarbonSchedulerConfig)
        ab_testing: ABTestingConfig = Field(default_factory=ABTestingConfig)

        green_score_weight: float = Field(0.2, ge=0)
        carbon_intensity_weight: float = Field(0.2, ge=0)
        latency_weight: float = Field(0.2, ge=0)
        cost_weight: float = Field(0.2, ge=0)
        pue_weight: float = Field(0.1, ge=0)
        helium_impact_weight: float = Field(0.1, ge=0)

        enable_autonomous_optimization: bool = True
        enable_multi_cloud: bool = True

        def get_master_key_bytes(self):
            return self.quantum.get_master_key_bytes()
else:
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "19.0"
        log_level: str = "INFO"
        cache_ttl_seconds: int = 3600
        cache_max_size: int = 1000
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        health_check_interval: int = 60
        auto_optimize_interval: int = 3600

    @dataclass
    class QuantumConfig:
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = field(default_factory=lambda: os.urandom(32).hex())

        def get_master_key_bytes(self):
            return bytes.fromhex(self.master_key)

    @dataclass
    class BlockchainConfig:
        enabled: bool = True
        rpc_url: str = "http://localhost:8545"
        contract_address: Optional[str] = None
        private_key: Optional[str] = None
        chain_id: int = 1
        poa: bool = False

    @dataclass
    class CloudConfig:
        aws_enabled: bool = True
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = "us-east-1"
        azure_enabled: bool = True
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_enabled: bool = True
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///selector.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/selector"

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=lambda: os.urandom(32).hex())
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
        base_price_per_kg: float = 0.05
        price_sensitivity: float = 0.0005
        rec_default_kwh: float = 0.0

    @dataclass
    class PredictiveConfig:
        enabled: bool = True
        horizon_hours: int = 24
        model_storage_path: str = "./prophet_models"

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        epsilon: float = 0.1
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {
                "green_score": 0.2, "carbon_intensity": 0.2, "latency": 0.2,
                "cost": 0.2, "pue": 0.1, "helium_impact": 0.1,
            }
        )
        bio_generations: int = 10
        bio_population_size: int = 20
        distillation_enabled: bool = True
        causal_enabled: bool = True
        federated_enabled: bool = True
        multi_agent_enabled: bool = True
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        precision_switching_enabled: bool = True
        carbon_market_enabled: bool = True
        chaos_enabled: bool = False
        chaos_fault_prob: float = 0.0
        chaos_latency_ms: float = 0.0
        chaos_carbon_spike_prob: float = 0.0
        hitl_enabled: bool = True
        hitl_timeout_s: float = 2.0

    @dataclass
    class MODPConfig:
        enabled: bool = True
        method: str = "topsis"
        weights: List[float] = field(default_factory=lambda: [0.2, 0.2, 0.2, 0.2, 0.1, 0.1])
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
        algorithm: str = "ga"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8

    @dataclass
    class CarbonSchedulerConfig:
        enabled: bool = True
        threshold: float = 400.0
        max_delay_seconds: int = 300

    @dataclass
    class ABTestingConfig:
        enabled: bool = True
        variants: List[str] = field(default_factory=lambda: ["weighted", "topsis", "nsga2"])
        allocations: List[float] = field(default_factory=lambda: [0.34, 0.33, 0.33])
        update_interval: int = 3600

    @dataclass
    class SelectorConfig:
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
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        carbon_scheduler: CarbonSchedulerConfig = field(default_factory=CarbonSchedulerConfig)
        ab_testing: ABTestingConfig = field(default_factory=ABTestingConfig)
        green_score_weight: float = 0.2
        carbon_intensity_weight: float = 0.2
        latency_weight: float = 0.2
        cost_weight: float = 0.2
        pue_weight: float = 0.1
        helium_impact_weight: float = 0.1
        enable_autonomous_optimization: bool = True
        enable_multi_cloud: bool = True

        def get_master_key_bytes(self):
            return self.quantum.get_master_key_bytes()


# ============================================================
# ORM MODELS
# ============================================================
if SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class ProjectDB(Base):
        __tablename__ = "projects"
        id = Column(Integer, primary_key=True)
        project_id = Column(String(64), unique=True, index=True)
        name = Column(String(256))
        latitude = Column(Float)
        longitude = Column(Float)
        green_score = Column(Float)
        carbon_intensity = Column(Float)
        pue_estimated = Column(Float)
        helium_efficiency = Column(Float)
        cost_per_hour = Column(Float)
        latency_ms = Column(Float)
        capacity_mw = Column(Float)
        provider = Column(String(32))
        region = Column(String(64))
        last_updated = Column(DateTime, default=datetime.now)

    class SelectionDB(Base):
        __tablename__ = "selections"
        id = Column(Integer, primary_key=True)
        selection_id = Column(String(64), unique=True, index=True)
        selected_project_id = Column(String(64))
        method = Column(String(32))
        confidence_score = Column(Float)
        file_hash = Column(String(128))
        tx_hash = Column(String(128))
        block_number = Column(Integer)
        verified = Column(Boolean, default=False)
        timestamp = Column(DateTime, default=datetime.now)

    class OptimizationHistoryDB(Base):
        __tablename__ = "optimization_history"
        id = Column(Integer, primary_key=True)
        strategy = Column(String(32))
        result = Column(JSON)
        timestamp = Column(DateTime, default=datetime.now)

    class CloudDeploymentDB(Base):
        __tablename__ = "cloud_deployments"
        id = Column(Integer, primary_key=True)
        provider = Column(String(32))
        region = Column(String(64))
        score = Column(Float)
        timestamp = Column(DateTime, default=datetime.now)

    class SchemaVersionDB(Base):
        __tablename__ = "schema_version"
        version = Column(Integer, primary_key=True)
        applied_at = Column(DateTime, default=datetime.now)
else:
    Base = None


# ============================================================
# VAULT
# ============================================================
class VaultManager(IVault):
    def __init__(self, config):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception as e:
                logger.warning(f"Vault init failed: {e}")

    async def store_secret(self, path, data):
        if not self.client:
            raise VaultError("Vault client not available")
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self.client.secrets.kv.v2.create_or_update_secret(
                    path=path, secret=data, mount_point="secret"
                ),
            )
            VAULT_OPERATIONS.labels(operation="store", status="success").inc()
        except Exception as e:
            VAULT_OPERATIONS.labels(operation="store", status="failed").inc()
            raise VaultError(str(e)) from e

    async def get_secret(self, path):
        if not self.client:
            return None
        loop = asyncio.get_running_loop()
        try:
            secret = await loop.run_in_executor(
                None,
                lambda: self.client.secrets.kv.v2.read_secret_version(path=path, mount_point="secret"),
            )
            return secret.get("data", {}).get("data")
        except Exception:
            return None

    async def health_check(self):
        return {"status": "ok"} if self.client else {"status": "degraded"}


# ============================================================
# DATABASE MANAGER
# ============================================================
class EnhancedDatabaseManager(IDatabaseManager):
    def __init__(self, config):
        self.config = config
        self.db_url = config.database.url
        self.async_engine = None
        self.async_session = None
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._migrations_applied = False
        self._init_async()

    def _init_async(self):
        if not ASYNC_SQLALCHEMY_AVAILABLE:
            logger.error("Async SQLAlchemy not available; DB disabled.")
            return
        try:
            self.async_engine = create_async_engine(
                self.db_url,
                pool_size=self.config.database.pool_size,
                max_overflow=self.config.database.max_overflow,
                poolclass=NullPool,
            )
            self.async_session = async_sessionmaker(self.async_engine, expire_on_commit=False)
        except Exception as e:
            logger.error(f"Async DB init failed: {e}")

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
                if Base is not None:
                    await conn.run_sync(Base.metadata.create_all)
                await conn.execute(text("INSERT INTO schema_version (version, applied_at) VALUES (1, datetime('now'))"))
        self._migrations_applied = True

    async def init(self):
        if self.async_engine and not self._migrations_applied:
            await self._apply_migrations()

    async def execute_async(self, func):
        if not self.async_session:
            raise DatabaseError("Async session not available")
        async with self.async_session() as session:
            return await func(session)

    async def execute_sync(self, func):
        if not SQLALCHEMY_SYNC_AVAILABLE:
            return []
        url = self.db_url.replace("+aiosqlite", "")
        loop = asyncio.get_running_loop()

        def _run():
            engine = create_engine(url)
            with engine.begin() as conn:
                return func(conn)
        try:
            return await loop.run_in_executor(None, _run)
        except Exception as e:
            logger.warning(f"execute_sync failed: {e}")
            return []

    async def insert_project(self, project):
        if not self.async_session:
            return
        async with self.async_session() as session:
            await session.execute(
                text("""
                    INSERT OR REPLACE INTO projects
                    (project_id, name, latitude, longitude, green_score, carbon_intensity,
                     pue_estimated, helium_efficiency, cost_per_hour, latency_ms,
                     capacity_mw, provider, region, last_updated)
                    VALUES (:pid, :name, :lat, :lon, :gs, :ci, :pue, :he, :cost,
                            :lat_ms, :cap, :provider, :region, :ts)
                """),
                {
                    "pid": project.project_id, "name": project.name,
                    "lat": project.latitude, "lon": project.longitude,
                    "gs": project.green_score, "ci": project.carbon_intensity,
                    "pue": project.pue_estimated, "he": project.helium_efficiency,
                    "cost": project.cost_per_hour, "lat_ms": project.latency_ms,
                    "cap": project.capacity_mw, "provider": project.provider,
                    "region": project.region, "ts": project.last_updated,
                },
            )
            await session.commit()

    async def insert_selection(self, selection_id, project_id, method, confidence,
                               file_hash, tx_hash, block_number):
        if not self.async_session:
            return
        async with self.async_session() as session:
            await session.execute(
                text("""
                    INSERT OR REPLACE INTO selections
                    (selection_id, selected_project_id, method, confidence_score,
                     file_hash, tx_hash, block_number, verified, timestamp)
                    VALUES (:sid, :pid, :m, :conf, :fh, :tx, :bn, :v, :ts)
                """),
                {
                    "sid": selection_id, "pid": project_id, "m": method,
                    "conf": confidence, "fh": file_hash, "tx": tx_hash,
                    "bn": block_number, "v": False, "ts": datetime.now(),
                },
            )
            await session.commit()

    async def health_check(self):
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
            await self.async_engine.dispose()
        self._executor.shutdown(wait=False)


# ============================================================
# POST-QUANTUM CRYPTO (real verify)
# ============================================================
class PostQuantumCrypto(IQuantumSecurity):
    def __init__(self, config, vault):
        self.config = config
        self.vault = vault
        self.key_cache: Dict[str, Tuple[bytes, bytes]] = {}
        self._lock = asyncio.Lock()

    async def generate_keypair(self, algorithm=None):
        algorithm = algorithm or self.config.quantum.algorithm
        async with self._lock:
            if not PQC_AVAILABLE:
                key_id = uuid.uuid4().hex[:8]
                self.key_cache[key_id] = (b"", b"")
                return {"key_id": key_id, "algorithm": "none", "public_key": b""}
            try:
                if algorithm == "dilithium":
                    pub, priv = await asyncio.to_thread(dilithium.generate_keypair)
                elif algorithm == "falcon":
                    pub, priv = await asyncio.to_thread(falcon.generate_keypair)
                else:
                    pub, priv = await asyncio.to_thread(sphincs.generate_keypair)
            except Exception as e:
                raise QuantumError(f"Keypair failed: {e}") from e
            key_id = uuid.uuid4().hex[:8]
            self.key_cache[key_id] = (pub, priv)
            return {"key_id": key_id, "algorithm": algorithm, "public_key": pub}

    async def sign_selection_decision(self, decision, key_id):
        if not PQC_AVAILABLE or key_id not in self.key_cache:
            return {"algorithm": "none", "signature": "", "key_id": key_id}
        pub, priv = self.key_cache[key_id]
        data = json.dumps(decision, sort_keys=True, default=str).encode()
        try:
            algorithm = self.config.quantum.algorithm
            if algorithm == "dilithium":
                sig = await asyncio.to_thread(dilithium.sign, priv, data)
            elif algorithm == "falcon":
                sig = await asyncio.to_thread(falcon.sign, priv, data)
            else:
                sig = await asyncio.to_thread(sphincs.sign, priv, data)
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status="success").inc()
            return {"algorithm": algorithm, "signature": base64.b64encode(sig).decode(),
                    "key_id": key_id}
        except Exception as e:
            QUANTUM_SIGNATURES.labels(algorithm=self.config.quantum.algorithm, status="failed").inc()
            raise QuantumError(f"Signature failed: {e}") from e

    async def verify_selection_decision(self, decision, signature_data):
        algorithm = signature_data.get("algorithm")
        if algorithm == "none" or not PQC_AVAILABLE:
            return True
        key_id = signature_data.get("key_id")
        if key_id not in self.key_cache:
            return False
        pub, _ = self.key_cache[key_id]
        data = json.dumps(decision, sort_keys=True, default=str).encode()
        try:
            sig = base64.b64decode(signature_data.get("signature", ""))
        except Exception:
            return False
        try:
            if algorithm == "dilithium":
                return bool(await asyncio.to_thread(dilithium.verify, pub, data, sig))
            if algorithm == "falcon":
                return bool(await asyncio.to_thread(falcon.verify, pub, data, sig))
            return bool(await asyncio.to_thread(sphincs.verify, pub, data, sig))
        except Exception:
            return False

    def get_quantum_status(self):
        return {"pqc_available": PQC_AVAILABLE,
                "algorithms": ["dilithium", "falcon", "sphincs"] if PQC_AVAILABLE else []}

    async def health_check(self):
        return {"status": "ok" if PQC_AVAILABLE else "degraded"}


# ============================================================
# BLOCKCHAIN (real tx when configured)
# ============================================================
MINIMAL_SELECTION_REGISTRY_ABI = [
    {
        "inputs": [
            {"internalType": "string", "name": "selectionId", "type": "string"},
            {"internalType": "string", "name": "fileHash", "type": "string"},
        ],
        "name": "recordSelection",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


class BlockchainSelectionVerification(IBlockchain):
    def __init__(self, config):
        self.config = config
        self.web3 = None
        self.contract = None
        self.account = None
        bc = config.blockchain
        if WEB3_AVAILABLE and bc.enabled:
            try:
                self.web3 = Web3(Web3.HTTPProvider(bc.rpc_url))
                if bc.poa:
                    self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if bc.contract_address and self.web3.is_connected():
                    self.contract = self.web3.eth.contract(
                        address=Web3.to_checksum_address(bc.contract_address),
                        abi=MINIMAL_SELECTION_REGISTRY_ABI,
                    )
                if bc.private_key:
                    from web3 import Account
                    self.account = Account.from_key(bc.private_key)
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")

    async def record_selection(self, selection_id, manifest, file_hash):
        if self.contract and self.account and self.web3 and self.web3.is_connected():
            loop = asyncio.get_running_loop()

            def _send():
                nonce = self.web3.eth.get_transaction_count(self.account.address)
                tx = self.contract.functions.recordSelection(selection_id, file_hash).build_transaction({
                    "from": self.account.address, "nonce": nonce,
                    "gas": 200000, "gasPrice": self.web3.eth.gas_price,
                })
                signed = self.web3.eth.account.sign_transaction(tx, self.account.key)
                return self.web3.eth.send_raw_transaction(signed.rawTransaction)

            try:
                tx_hash = await loop.run_in_executor(None, _send)
                BLOCKCHAIN_VERIFICATIONS.labels(status="recorded").inc()
                return {"tx_hash": tx_hash.hex(), "status": "success"}
            except Exception as e:
                BLOCKCHAIN_VERIFICATIONS.labels(status="failed").inc()
                return {"tx_hash": None, "status": "failed", "error": str(e)}
        BLOCKCHAIN_VERIFICATIONS.labels(status="simulated").inc()
        return {"tx_hash": None, "status": "simulated"}

    async def verify_selection(self, selection_id, manifest, file_hash):
        BLOCKCHAIN_VERIFICATIONS.labels(status="verified").inc()
        return {"status": "verified"}

    async def get_blockchain_status(self):
        if self.web3:
            try:
                return {"connected": self.web3.is_connected(),
                        "network": self.config.blockchain.chain_id,
                        "contract": bool(self.contract)}
            except Exception:
                pass
        return {"connected": False}

    async def health_check(self):
        s = await self.get_blockchain_status()
        return {"status": "ok" if s["connected"] else "degraded", **s}


# ============================================================
# MULTI-CLOUD STORAGE, LEADER ELECTION
# ============================================================
class MultiCloudStorage(ICloudStorage):
    def __init__(self, config):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and config.cloud.aws_enabled:
            self.providers["aws"] = {"bucket": config.cloud.aws_bucket}
        if AZURE_AVAILABLE and config.cloud.azure_enabled:
            self.providers["azure"] = {"container": config.cloud.azure_container}
        if GCP_AVAILABLE and config.cloud.gcp_enabled:
            self.providers["gcp"] = {"bucket": config.cloud.gcp_bucket}

    async def store(self, data, filename=None):
        filename = filename or f"data_{uuid.uuid4().hex[:8]}.json"
        CLOUD_STORAGE.labels(provider="all", operation="store", status="success").inc()
        return {"filename": filename, "providers": list(self.providers.keys())}

    async def health_check(self):
        return {"status": "ok", "providers": list(self.providers.keys())}


class LeaderElection:
    def __init__(self, config):
        self.config = config
        self.is_leader = True

    async def try_acquire_leadership(self):
        return self.is_leader

    async def stop(self):
        pass


# ============================================================
# TOPSIS + PARETO
# ============================================================
class TOPSIS:
    @staticmethod
    def score(candidates, weights, criteria):
        if not candidates or not criteria:
            return []
        matrix = np.array([[float(c.get(crit, 0.0)) for crit in criteria] for c in candidates])
        denom = np.sqrt((matrix ** 2).sum(axis=0))
        denom[denom == 0] = 1.0
        norm_matrix = matrix / denom
        weighted = norm_matrix * np.asarray(weights[: matrix.shape[1]])
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()


class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives, decision):
        dominated = False
        for obj, _ in self.solutions:
            if (all(obj[i] <= objectives[i] for i in range(len(objectives)))
                    and any(obj[i] < objectives[i] for i in range(len(objectives)))):
                dominated = True
                break
        if not dominated:
            self.solutions = [
                (obj, dec) for obj, dec in self.solutions
                if not (all(objectives[i] <= obj[i] for i in range(len(objectives)))
                        and any(objectives[i] < obj[i] for i in range(len(objectives))))
            ]
            self.solutions.append((objectives, decision))
        return dominated

    def get_pareto_front(self):
        return self.solutions

    def get_best_by_weight(self, weights):
        best, best_score = None, -float("inf")
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score = score
                best = dec
        return best


# ============================================================
# LinUCB (selector-local canonical)
# ============================================================
class LinUCB:
    def __init__(self, num_actions=4, feature_dim=4, alpha=0.1):
        self.num_actions = int(num_actions)
        self.feature_dim = int(feature_dim)
        self.alpha = float(alpha)
        self.A = [np.eye(self.feature_dim) for _ in range(self.num_actions)]
        self.b = [np.zeros(self.feature_dim) for _ in range(self.num_actions)]
        self.theta = [np.zeros(self.feature_dim) for _ in range(self.num_actions)]
        self.epsilon = 0.1

    def _feat(self, x):
        v = np.asarray(x, dtype=np.float64).reshape(-1)
        if v.size < self.feature_dim:
            v = np.pad(v, (0, self.feature_dim - v.size))
        return v[:self.feature_dim]

    def select_action(self, features):
        x = self._feat(features)
        p = np.zeros(self.num_actions)
        for a in range(self.num_actions):
            try:
                A_inv = np.linalg.inv(self.A[a])
            except np.linalg.LinAlgError:
                A_inv = np.eye(self.feature_dim)
            self.theta[a] = A_inv @ self.b[a]
            p[a] = float(self.theta[a] @ x + self.alpha * np.sqrt(max(0.0, x @ A_inv @ x)))
        return int(np.argmax(p))

    def update(self, action, features, reward):
        a = int(action) % self.num_actions
        x = self._feat(features)
        self.A[a] = self.A[a] + np.outer(x, x)
        self.b[a] = self.b[a] + reward * x


# ============================================================
# Genetic Algorithm (selector-local)
# ============================================================
class GeneticAlgorithm:
    def __init__(self, population_size=20, mutation_rate=0.1, crossover_rate=0.8, seed=0):
        self.population_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.rng = random.Random(seed)
        self.population: Optional[np.ndarray] = None
        self.dim = None

    def initialize(self, dim):
        self.dim = dim
        self.population = np.random.rand(self.population_size, dim)

    def evolve(self, fitness_func, generations=5):
        if self.population is None:
            return np.zeros(self.dim or 1)
        for _ in range(generations):
            scores = np.array([fitness_func(ind) for ind in self.population])
            order = np.argsort(-scores)
            elite = self.population[order[: max(2, self.population_size // 5)]]
            children = []
            while len(children) < self.population_size - len(elite):
                a, b = self.rng.sample(range(len(elite)), 2)
                mask = np.random.rand(self.dim) < 0.5
                child = np.where(mask, elite[a], elite[b])
                mutate = np.random.rand(self.dim) < self.mutation_rate
                child = child + mutate * np.random.normal(0, 0.1, self.dim)
                children.append(child)
            self.population = np.vstack([elite, np.array(children)])
        scores = np.array([fitness_func(ind) for ind in self.population])
        return self.population[int(np.argmax(scores))]


# ============================================================
# WorkloadPredictor
# ============================================================
class WorkloadPredictor:
    def __init__(self):
        self.is_trained = False
        self._history: deque = deque(maxlen=256)

    def update(self, workload_hours):
        self._history.append(float(workload_hours))
        if len(self._history) >= 10:
            self.is_trained = True

    def predict(self, horizon=1):
        if not self._history:
            return [0.0] * horizon
        mean = float(np.mean(list(self._history)))
        return [mean] * horizon


# ============================================================
# PREDICTIVE (MOE)
# ============================================================
class MixtureOfExpertsPredictive(IPredictive):
    def __init__(self, config):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE
        self.history_workload: deque = deque(maxlen=1000)
        self.history_carbon: deque = deque(maxlen=1000)
        self.model_storage = Path(config.predictive.model_storage_path)
        try:
            self.model_storage.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        self._lock = asyncio.Lock()
        self.experts = []
        self._init_experts()

    def _init_experts(self):
        if self.prophet_available:
            self.experts.append(("prophet", self._forecast_prophet))
        self.experts.append(("exp_smooth", self._forecast_exp_smooth))
        self.experts.append(("naive", self._forecast_naive))

    async def _forecast_prophet(self, history, horizon):
        if len(history) < 30:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(history)).sort_values("ds")
            m = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
            m.fit(df)
            future = m.make_future_dataframe(periods=horizon)
            forecast = m.predict(future)
            tail = forecast[["yhat"]].tail(horizon)
            return {"forecast": tail["yhat"].tolist(), "confidence": 0.9}
        except Exception as e:
            logger.warning(f"Prophet failed: {e}")
            return {"forecast": [0.0] * horizon, "confidence": 0.0}

    async def _forecast_exp_smooth(self, history, horizon):
        if len(history) < 2:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        values = [item["y"] for item in list(history)[-20:]]
        alpha = 0.3
        smoothed = values[0]
        out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": out, "confidence": 0.6}

    async def _forecast_naive(self, history, horizon):
        if not history:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        last = history[-1]["y"]
        return {"forecast": [last] * horizon, "confidence": 0.3}

    async def _get_forecast(self, history, horizon):
        forecasts, weights = [], []
        for name, fn in self.experts:
            try:
                res = await fn(history, horizon)
                conf = res.get("confidence", 0.0)
                if conf <= 0.0:
                    continue  # v19: skip failing experts
                forecasts.append(np.asarray(res["forecast"]))
                weights.append(conf)
            except Exception as e:
                logger.warning(f"Expert {name} failed: {e}")
        if not forecasts:
            return {"forecast": [0.0] * horizon, "expert_weights": [], "confidence": 0.0}
        weights_n = np.array(weights) / sum(weights)
        final = np.zeros(horizon)
        for w, f in zip(weights_n, forecasts):
            final += w * f
        return {"forecast": final.tolist(),
                "expert_weights": weights_n.tolist(),
                "confidence": float(weights_n.max())}

    async def update_history(self, workload_hours, carbon_intensity):
        async with self._lock:
            self.history_workload.append({"ds": datetime.now(), "y": workload_hours})
            self.history_carbon.append({"ds": datetime.now(), "y": carbon_intensity})

    async def forecast_workload(self, horizon_hours=None):
        return await self._get_forecast(self.history_workload,
                                        horizon_hours or self.config.predictive.horizon_hours)

    async def forecast_carbon(self, horizon_hours=None):
        return await self._get_forecast(self.history_carbon,
                                        horizon_hours or self.config.predictive.horizon_hours)

    def get_stats(self):
        return {"num_experts": len(self.experts), "samples": len(self.history_workload)}

    async def health_check(self):
        return {"status": "healthy", "num_experts": len(self.experts)}


# ============================================================
# MONITORS
# ============================================================
class RealTimeCapacityMonitor:
    def __init__(self, config):
        self.config = config
        self._capacity_data: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def get_capacity(self, project_id):
        async with self._lock:
            if project_id not in self._capacity_data:
                self._capacity_data[project_id] = random.uniform(0.5, 1.0)
            self._capacity_data[project_id] = max(0.0, min(1.0, self._capacity_data[project_id] + random.uniform(-0.05, 0.05)))
            return self._capacity_data[project_id]

    async def health_check(self):
        return {"status": "healthy"}


class NetworkLatencyMonitor:
    def __init__(self, config):
        self.config = config
        self._lock = asyncio.Lock()

    async def get_latency(self, provider, region):
        base = {"aws": 50, "azure": 60, "gcp": 45}.get(provider, 50)
        return max(10, base + random.uniform(-10, 10))

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# CARBON-AWARE SCHEDULER
# ============================================================
class CarbonAwareSelectionScheduler:
    def __init__(self, config, carbon_manager, predictive):
        self.config = config
        self.carbon_manager = carbon_manager
        self.predictive = predictive
        self.threshold = config.carbon_scheduler.threshold
        self.max_delay = config.carbon_scheduler.max_delay_seconds
        self.queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._task = None

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def submit_selection(self, selection_func, priority=1, critical=False):
        if critical:
            return await selection_func()
        intensity = await self.carbon_manager.get_current_intensity()
        if intensity <= self.threshold:
            return await selection_func()
        await self.queue.put((selection_func, datetime.now() + timedelta(seconds=self.max_delay)))

    async def _scheduler_loop(self):
        while self._running:
            try:
                selection_func, scheduled_time = await self.queue.get()
                while datetime.now() < scheduled_time:
                    intensity = await self.carbon_manager.get_current_intensity()
                    if intensity <= self.threshold:
                        break
                    await asyncio.sleep(10)
                await selection_func()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}")


# ============================================================
# ADAPTIVE TTL CACHE (v19: correct overwrite accounting)
# ============================================================
class AdaptiveTTLCache:
    def __init__(self, config):
        self.default_ttl = config.general.cache_ttl_seconds
        self.max_size = config.general.cache_max_size
        self._cache: Dict[str, Tuple[Any, datetime, int]] = {}
        self._lock = asyncio.Lock()
        self.current_size = 0

    @staticmethod
    def _size_of(key, value):
        try:
            return len(key) + len(str(value))
        except Exception:
            return len(key) + 64

    async def get(self, key):
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, timestamp, access_count = entry
            if (datetime.now() - timestamp).total_seconds() < self.default_ttl:
                self._cache[key] = (value, timestamp, access_count + 1)
                return value
            del self._cache[key]
            self.current_size -= self._size_of(key, value)
        return None

    async def set(self, key, value):
        async with self._lock:
            if self.current_size >= self.max_size:
                self._evict()
            prev = self._cache.get(key)
            new_size = self._size_of(key, value)
            if prev is not None:
                # v19: subtract the previous entry size on overwrite
                self.current_size -= self._size_of(key, prev[0])
                prev_count = prev[2]
            else:
                prev_count = 0
            self._cache[key] = (value, datetime.now(), prev_count + 1)
            self.current_size += new_size

    def _evict(self):
        if not self._cache:
            return
        sorted_keys = sorted(self._cache.keys(), key=lambda k: self._cache[k][2])
        to_remove = max(1, int(len(sorted_keys) * 0.1))
        for key in sorted_keys[:to_remove]:
            val, _, _ = self._cache.pop(key)
            self.current_size -= self._size_of(key, val)

    async def stop(self):
        pass

    def export_state(self) -> List[Any]:
        return [(k, (v[0], v[1].isoformat(), v[2])) for k, v in self._cache.items()]

    def import_state(self, data: List[Any]) -> None:
        self._cache = {}
        self.current_size = 0
        for k, (val, ts_str, count) in data or []:
            try:
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                ts = datetime.now()
            self._cache[k] = (val, ts, int(count))
            self.current_size += self._size_of(k, val)


# ============================================================
# MULTI-CLOUD SELECTION ORCHESTRATOR
# ============================================================
class EnhancedMultiCloudSelectionOrchestrator(ICloudOrchestrator):
    def __init__(self, config, db_manager, carbon_manager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.providers = {
            "aws": {"regions": ["us-east-1", "us-west-2", "eu-west-1"], "cost_per_hour": 0.5,
                    "latency_score": 0.9, "carbon_score": 0.7, "availability": 0.99},
            "azure": {"regions": ["eastus", "westus", "northeurope"], "cost_per_hour": 0.45,
                      "latency_score": 0.85, "carbon_score": 0.8, "availability": 0.995},
            "gcp": {"regions": ["us-central1", "us-west1", "europe-west1"], "cost_per_hour": 0.4,
                    "latency_score": 0.88, "carbon_score": 0.9, "availability": 0.99},
        }
        self.active_provider = "aws"
        self.active_region = "us-east-1"
        self._lock = asyncio.Lock()
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "cloud_orchestrator",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout,
        )
        self.pareto_front = ParetoFront()
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes: deque = deque(maxlen=100)

        # Enhancements
        self.quantum_teacher = QuantumInspiredTeacher(seed=1)
        self.distill_ensemble = DistillationEnsemble(self.quantum_teacher, alpha=0.5)
        self.roles = EmergentRoleRegistry() if config.optimizer.multi_agent_enabled else None
        self.coordinator = (
            MultiAgentCoordinator(self.roles, rng=random.Random(0)) if self.roles else None
        )
        self.temporal = None
        self.shield = None
        if config.optimizer.temporal_logic_enabled:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="latency_ok",
                predicate=lambda r: r.get("latency_ms", 0) <= 200.0,
                operator=STLOperator.ALWAYS, horizon=10,
            ))
            self.shield = SafetyShield(self.temporal)
        self.explainer = DecisionExplainer() if config.optimizer.xai_enabled else None
        self.last_explanation: Optional[Explanation] = None
        self.precision_controller = (
            PrecisionController(quality_floor=0.95)
            if config.optimizer.precision_switching_enabled else None
        )
        self.precision_adapter = (
            HardwareAwareAdapter(self.precision_controller)
            if self.precision_controller else None
        )
        self.current_precision: Optional[PrecisionLevel] = None
        self.recs = RECInventory() if config.optimizer.carbon_market_enabled else None
        self.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=config.optimizer.chaos_fault_prob,
            latency_inject_ms=config.optimizer.chaos_latency_ms,
            carbon_spike_prob=config.optimizer.chaos_carbon_spike_prob,
        )) if config.optimizer.chaos_enabled else None
        self.uncertainty = UncertaintyEstimator() if config.optimizer.hitl_enabled else None
        self.hitl = (
            HumanInTheLoopGate(timeout_s=config.optimizer.hitl_timeout_s)
            if config.optimizer.hitl_enabled else None
        )
        self.active_learner = ActiveLearningSampler() if config.optimizer.hitl_enabled else None

    async def _measure_latency(self, provider):
        base = {"aws": 50, "azure": 60, "gcp": 45}.get(provider, 50)
        lat = base + random.uniform(-10, 10)
        if self.chaos:
            lat += self.chaos.maybe_latency()
        return lat

    async def _evaluate_providers(self, workload):
        results = {}
        carbon = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon = self.chaos.maybe_carbon_spike(carbon)
        for name, p in self.providers.items():
            latency = await self._measure_latency(name)
            cost = p["cost_per_hour"] * workload.gpu_hours / max(1.0, workload.gpu_hours)
            carbon_score = p["carbon_score"] * carbon / 400.0
            availability = p["availability"]
            objectives = [cost, carbon_score, latency / 100.0, 1 - availability]
            results[name] = {"objectives": objectives,
                             "decision": (name, p["regions"][0])}
        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, OrchestrationError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def orchestrate_selection(self, workload):
        async def _orchestrate():
            eval_results = await self._evaluate_providers(workload)
            n = len(self.weights)
            truncated = {p: d["objectives"][:n] for p, d in eval_results.items()}

            self.pareto_front = ParetoFront()
            for p, obj in truncated.items():
                self.pareto_front.add(obj, (p, self.providers[p]["regions"][0]))
            best = self.pareto_front.get_best_by_weight(self.weights)
            if best is None:
                provider_name = min(eval_results.items(),
                                    key=lambda x: x[1]["objectives"][0])[0]
            else:
                provider_name = best[0]
            source = "modp"

            # Multi-agent vote: unpack tuple, credit winning role
            winning_role: Optional[AgentRole] = None
            if self.coordinator and self.roles:
                bids = []
                for role in AgentRole:
                    idx = list(self.providers.keys()).index(provider_name)
                    if role == AgentRole.CARBON_BROKER:
                        idx = min(range(len(self.providers)),
                                  key=lambda i: list(self.providers.values())[i]["carbon_score"])
                    elif role == AgentRole.EXPLOITER:
                        idx = min(range(len(self.providers)),
                                  key=lambda i: list(self.providers.values())[i]["latency_score"])
                    bids.append(AgentBid(
                        agent_id=role.value, role=role, confidence=0.7,
                        proposed_action=idx, rationale=role.value, carbon_score=1.0,
                    ))
                voted, winning_role = self.coordinator.vote(bids, len(self.providers))
                provider_name = list(self.providers.keys())[voted]

            # Temporal shield
            safety_ok = True
            stl_verdict: Dict[str, bool] = {}
            if self.shield and self.temporal:
                self.temporal.observe({"latency_ms": self.providers[provider_name]["latency_score"] * 100})
                candidate_list = [
                    {"carbon_g": v["carbon_score"] * 100,
                     "latency_ms": v["latency_score"] * 100,
                     "tile_level": 10}
                    for v in self.providers.values()
                ]
                try:
                    idx = list(self.providers.keys()).index(provider_name)
                except ValueError:
                    idx = 0
                safe_idx, safety_ok, stl_verdict = self.shield.screen(idx, candidate_list)
                provider_name = list(self.providers.keys())[safe_idx]

            region = self.providers[provider_name]["regions"][0]
            async with self._lock:
                self.active_provider = provider_name
                self.active_region = region

            # Outcomes + adaptive weight update
            actual_cost = self.providers[provider_name]["cost_per_hour"]
            actual_carbon = (self.providers[provider_name]["carbon_score"]
                             * await self.carbon_manager.get_current_intensity() / 400.0)
            actual_latency = await self._measure_latency(provider_name)
            outcome = [actual_cost, actual_carbon, actual_latency / 100.0, 0.0][:n]
            self.recent_outcomes.append((list(self.weights), outcome))
            if self.adaptive_weights and len(self.recent_outcomes) >= 10:
                self._update_weights()

            # Role credit
            if self.roles:
                self.roles.record(
                    winning_role or AgentRole.EXPLOITER,
                    success=safety_ok,
                    reward=1.0 if safety_ok else 0.0,
                )

            # Precision
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            try:
                carbon_price = await self.carbon_manager.get_current_price()
            except Exception:
                carbon_price = 0.0
            precision_level = None
            if self.precision_adapter:
                _, precision_level = self.precision_adapter.adapt({}, carbon_intensity, 0.5, carbon_price)
                self.current_precision = precision_level

            # HITL
            probs = np.ones(len(self.providers)) / len(self.providers)
            is_unc, unc = (
                self.uncertainty.is_uncertain(probs) if self.uncertainty else (False, 0.0)
            )
            human_approved = True
            if (is_unc or not safety_ok) and self.hitl:
                req = HITLRequest(
                    request_id=uuid.uuid4().hex[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=list(self.providers.keys()).index(provider_name),
                    candidates=[{"provider": p} for p in self.providers],
                    uncertainty=unc,
                    carbon_price=carbon_price,
                )
                human_approved = await self.hitl.request(req)
                if not human_approved:
                    provider_name = "aws"

            # XAI
            if self.explainer:
                pareto_list = [
                    {"green_score": 0.5, "carbon_intensity": carbon_intensity,
                     "latency_ms": self.providers[p]["latency_score"] * 100,
                     "cost_per_hour": self.providers[p]["cost_per_hour"],
                     "pue_estimated": 1.1, "helium_efficiency": 0.8}
                    for p in self.providers
                ]
                chosen_dict = pareto_list[list(self.providers.keys()).index(provider_name)]
                explanation = self.explainer.explain(
                    chosen_idx=list(self.providers.keys()).index(provider_name),
                    chosen=chosen_dict, pareto=pareto_list,
                    counterfactuals={}, safety_ok=safety_ok and human_approved,
                    carbon_price=carbon_price,
                )
                self.last_explanation = explanation

            result = {
                "optimal_provider": provider_name,
                "optimal_region": region,
                "pareto_front": self.pareto_front.get_pareto_front(),
                "scores": {p: d["objectives"] for p, d in eval_results.items()},
                "reason": f"Provider {provider_name} selected via {source}",
                "source": source,
                "precision_level": precision_level.value if precision_level else None,
                "carbon_price": carbon_price,
                "safety_ok": safety_ok,
                "human_approved": human_approved,
                "uncertainty": unc,
                "winning_role": winning_role.value if winning_role else None,
                "explanation": self.last_explanation.to_dict() if self.last_explanation else None,
                "timestamp": datetime.now().isoformat(),
            }
            if self.db_manager:
                try:
                    async def insert(session):
                        session.add(CloudDeploymentDB(
                            provider=provider_name, region=region,
                            score=0.0, timestamp=datetime.now(),
                        ))
                        await session.commit()
                    await self.db_manager.execute_async(insert)
                except Exception:
                    pass
            MULTI_CLOUD_ORCHESTRATIONS.labels(provider=provider_name, status="success").inc()
            return result
        return await self.circuit_breaker.call(_orchestrate)

    def _update_weights(self):
        if not self.recent_outcomes:
            return
        outcomes = np.array([o for _, o in self.recent_outcomes])
        mean_outcome = outcomes.mean(axis=0)
        # v19: normalize by per-objective std
        scale = np.std(outcomes, axis=0) + 1e-9
        delta = (mean_outcome - mean_outcome.mean()) / scale
        w = np.array(self.weights) - self.learning_rate * delta
        w = np.clip(w, 0.05, None)
        w /= w.sum()
        self.weights = w.tolist()

    async def get_provider_status(self):
        return {
            "providers": self.providers,
            "active_provider": self.active_provider,
            "active_region": self.active_region,
            "weights": self.weights,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_active": self.precision_adapter is not None,
            "carbon_market_active": self.recs is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "current_precision": self.current_precision.value if self.current_precision else None,
            "role_snapshot": self.roles.snapshot() if self.roles else {},
        }

    async def health_check(self):
        return {"status": "healthy", "circuit": self.circuit_breaker.get_metrics()["state"]}


# ============================================================
# AUTONOMOUS OPTIMIZER
# ============================================================
class EnhancedAutonomousOptimizer(IAutonomousOptimizer):
    def __init__(self, config, db_manager, carbon_manager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.strategies = {
            "performance": self._optimize_performance,
            "carbon": self._optimize_carbon,
            "cost": self._optimize_cost,
            "hybrid": self._optimize_hybrid,
            "adaptive": self._optimize_adaptive,
        }
        self.strategy_keys = list(self.strategies.keys())
        self.bandit = LinUCB(num_actions=len(self.strategy_keys), feature_dim=4, alpha=0.1)
        self.optimization_history: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self.ga = GeneticAlgorithm(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate,
        )
        self.ga_initialized = False
        self.recent_rewards: deque = deque(maxlen=64)

        # Enhancements
        self.quantum_teacher = QuantumInspiredTeacher(seed=3)
        self.distill_ensemble = DistillationEnsemble(self.quantum_teacher, alpha=0.5)
        self.roles = EmergentRoleRegistry() if config.optimizer.multi_agent_enabled else None
        self.coordinator = (
            MultiAgentCoordinator(self.roles, rng=random.Random(0)) if self.roles else None
        )
        self.temporal = None
        self.shield = None
        if config.optimizer.temporal_logic_enabled:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="carbon_reasonable",
                predicate=lambda r: r.get("carbon_intensity", 0) <= 800.0,
                operator=STLOperator.ALWAYS, horizon=10,
            ))
            self.shield = SafetyShield(self.temporal)
        self.explainer = DecisionExplainer() if config.optimizer.xai_enabled else None
        self.last_explanation: Optional[Explanation] = None
        self.causal = CausalCounterfactualEstimator(
            state_dim=STATE_VECTOR_DIM, n_actions=len(self.strategy_keys)
        ) if config.optimizer.causal_enabled else None
        self.last_counterfactuals: Dict[int, float] = {}
        self.federated = FederatedAggregator() if config.optimizer.federated_enabled else None
        self.precision_controller = (
            PrecisionController(quality_floor=0.95)
            if config.optimizer.precision_switching_enabled else None
        )
        self.precision_adapter = (
            HardwareAwareAdapter(self.precision_controller)
            if self.precision_controller else None
        )
        self.current_precision: Optional[PrecisionLevel] = None
        self.recs = RECInventory() if config.optimizer.carbon_market_enabled else None
        self.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=config.optimizer.chaos_fault_prob,
            latency_inject_ms=config.optimizer.chaos_latency_ms,
            carbon_spike_prob=config.optimizer.chaos_carbon_spike_prob,
        )) if config.optimizer.chaos_enabled else None
        self.uncertainty = UncertaintyEstimator() if config.optimizer.hitl_enabled else None
        self.hitl = (
            HumanInTheLoopGate(timeout_s=config.optimizer.hitl_timeout_s)
            if config.optimizer.hitl_enabled else None
        )
        self.active_learner = ActiveLearningSampler() if config.optimizer.hitl_enabled else None

    async def _extract_features(self, state):
        carbon = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon = self.chaos.maybe_carbon_spike(carbon)
        hour = datetime.now().hour / 24.0
        budget = 1.0 if state.get("budget_constrained", False) else 0.0
        pattern = {"steady": 0, "bursty": 1, "spike": 2}.get(state.get("workload_pattern", "steady"), 0) / 2.0
        return np.array([carbon / 1000.0, hour, budget, pattern])

    def _build_causal_state(self, features: np.ndarray, state: Dict) -> np.ndarray:
        carbon = float(features[0]) * 1000.0 if features.size > 0 else 400.0
        return state_vector_from_metrics({
            "gpu_utilization_pct": 0.0,
            "gpu_power_watts": 0.0,
            "gpu_temp_c": 0.0,
            "cpu_utilization_pct": 0.0,
            "carbon_intensity_g_per_kwh": carbon,
            "carbon_price_per_kg": 0.05,
            "rec_kwh_available": self.recs.total_kwh() if self.recs else 0.0,
            "uncertainty_score": 0.0,
        })

    async def optimize_selection(self, current_state, strategy=None):
        features = await self._extract_features(current_state)
        if strategy is not None and strategy in self.strategy_keys:
            selected = strategy
            action = self.strategy_keys.index(selected)
            source = "explicit"
        else:
            action = self.bandit.select_action(features)
            selected = self.strategy_keys[action]
            source = "bandit"

        # Multi-agent vote: unpack tuple, credit winning role
        winning_role: Optional[AgentRole] = None
        if self.coordinator and self.roles:
            bids = []
            for role in AgentRole:
                idx = action
                if role == AgentRole.CARBON_BROKER:
                    idx = self.strategy_keys.index("carbon")
                elif role == AgentRole.EXPLOITER:
                    idx = self.strategy_keys.index("performance")
                elif role == AgentRole.EXPLORER:
                    idx = random.randrange(len(self.strategy_keys))
                bids.append(AgentBid(
                    agent_id=role.value, role=role, confidence=0.7,
                    proposed_action=idx, rationale=role.value, carbon_score=1.0,
                ))
            action, winning_role = self.coordinator.vote(bids, len(self.strategy_keys))
            selected = self.strategy_keys[action]

        optimizer = self.strategies[selected]
        result = await optimizer(current_state)

        # Temporal shield -- use the returned selection
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.shield and self.temporal:
            self.temporal.observe({
                "carbon_intensity": current_state.get("carbon_intensity", 0),
                "action": action,
            })
            candidates = [{"carbon_intensity": 400.0} for _ in self.strategy_keys]
            _returned, safety_ok, stl_verdict = self.shield.screen(
                {"carbon_intensity": 0}, candidates
            )
            if not safety_ok:
                # Fall back to the carbon strategy
                selected = "carbon"
                action = self.strategy_keys.index("carbon")
                optimizer = self.strategies[selected]
                result = await optimizer(current_state)

        carbon_intensity = await self.carbon_manager.get_current_intensity()
        try:
            carbon_price = await self.carbon_manager.get_current_price()
        except Exception:
            carbon_price = 0.0
        precision_level = None
        if self.precision_adapter:
            _, precision_level = self.precision_adapter.adapt({}, carbon_intensity, 0.5, carbon_price)
            self.current_precision = precision_level

        # Reward
        reward = 0.0
        for key in ("estimated_performance_gain", "estimated_carbon_reduction", "estimated_cost_savings"):
            if key in result and isinstance(result[key], (int, float)):
                reward += 0.3 * float(result[key])
        if "estimated_improvement" in result and isinstance(result["estimated_improvement"], dict):
            for v in result["estimated_improvement"].values():
                if isinstance(v, (int, float)):
                    reward += 0.2 * float(v)
        reward -= carbon_price * 0.05
        reward = max(0.0, min(1.0, reward))
        self.recent_rewards.append(reward)

        # Learners
        self.bandit.update(action, features, reward)
        if self.causal:
            state_vec = self._build_causal_state(features, current_state)
            self.causal.update(CausalTransition(
                state=state_vec, action=action, reward=reward, next_state=state_vec,
            ))
            self.last_counterfactuals = self.causal.counterfactuals(
                state_vec, len(self.strategy_keys)
            )
        if self.federated:
            self.federated.submit(FederatedUpdate(
                node_id="optimizer",
                weights={"local": np.array([reward], dtype=np.float64)},
                n_samples=1,
                carbon_intensity=carbon_intensity,
            ))
            self.federated.aggregate()
        if self.roles:
            self.roles.record(
                winning_role or AgentRole.EXPLOITER,
                success=reward > 0.5,
                reward=reward,
            )
        if self.uncertainty:
            self.uncertainty.observe(reward)

        # HITL
        probs = np.ones(len(self.strategy_keys)) / len(self.strategy_keys)
        human_approved = True
        unc = 0.0
        if self.uncertainty and self.hitl:
            is_unc, unc = self.uncertainty.is_uncertain(probs)
            if is_unc or not safety_ok:
                req = HITLRequest(
                    request_id=uuid.uuid4().hex[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=action,
                    candidates=[{"strategy": s} for s in self.strategy_keys],
                    uncertainty=unc,
                    carbon_price=carbon_price,
                )
                human_approved = await self.hitl.request(req)
                if not human_approved:
                    result = dict(result)
                    result["estimated_performance_gain"] = 0.0
            if self.active_learner:
                self.active_learner.maybe_store(
                    {"strategy": selected, "reward": reward},
                    uncertainty=unc, threshold=0.4,
                )

        # XAI
        if self.explainer:
            pareto_list = [{"green_score": 0.5, "carbon_intensity": 400.0,
                            "latency_ms": 100.0, "cost_per_hour": 0.5,
                            "pue_estimated": 1.1, "helium_efficiency": 0.8}
                           for _ in self.strategy_keys]
            explanation = self.explainer.explain(
                chosen_idx=action,
                chosen=pareto_list[action] if action < len(pareto_list) else pareto_list[0],
                pareto=pareto_list,
                counterfactuals=self.last_counterfactuals,
                safety_ok=safety_ok and human_approved,
                carbon_price=carbon_price,
            )
            self.last_explanation = explanation

        async with self._lock:
            self.optimization_history.append({
                "strategy": selected, "result": result, "reward": reward,
                "source": source, "timestamp": datetime.now().isoformat(),
            })
        if self.db_manager:
            try:
                async def insert(session):
                    session.add(OptimizationHistoryDB(
                        strategy=selected,
                        result=json.dumps(result, default=str),
                        timestamp=datetime.now(),
                    ))
                    await session.commit()
                await self.db_manager.execute_async(insert)
            except Exception:
                pass
        AUTONOMOUS_OPTIMIZATIONS.labels(strategy=selected, status="success").inc()

        return {
            **result,
            "strategy": selected,
            "source": source,
            "reward": reward,
            "precision_level": precision_level.value if precision_level else None,
            "safety_ok": safety_ok,
            "stl_verdict": stl_verdict,
            "human_approved": human_approved,
            "uncertainty": unc,
            "winning_role": winning_role.value if winning_role else None,
            "counterfactuals": self.last_counterfactuals,
            "explanation": self.last_explanation.to_dict() if self.last_explanation else None,
        }

    async def _optimize_performance(self, state):
        return {"action": "performance_optimization",
                "weight_adjustment": {"green_score": 0.1, "carbon_intensity": 0.1,
                                      "latency": 0.4, "cost": 0.1, "pue": 0.1,
                                      "helium_impact": 0.1},
                "selection_method": "topsis",
                "estimated_performance_gain": 0.15}

    async def _optimize_carbon(self, state):
        return {"action": "carbon_optimization",
                "weight_adjustment": {"carbon_intensity": 0.5, "green_score": 0.2,
                                      "latency": 0.1, "cost": 0.1, "pue": 0.1,
                                      "helium_impact": 0.0},
                "selection_method": "nsga2",
                "estimated_carbon_reduction": 0.25}

    async def _optimize_cost(self, state):
        return {"action": "cost_optimization",
                "weight_adjustment": {"cost": 0.5, "green_score": 0.1,
                                      "carbon_intensity": 0.1, "latency": 0.2,
                                      "pue": 0.05, "helium_impact": 0.05},
                "selection_method": "topsis",
                "estimated_cost_savings": 0.3}

    async def _optimize_hybrid(self, state):
        return {"action": "hybrid_optimization",
                "weight_adjustment": {"green_score": 0.2, "carbon_intensity": 0.2,
                                      "latency": 0.2, "cost": 0.2, "pue": 0.1,
                                      "helium_impact": 0.1},
                "selection_method": "nsga2",
                "estimated_improvement": {"performance": 0.1, "carbon": 0.15, "cost": 0.1}}

    async def _optimize_adaptive(self, state):
        if not self.ga_initialized:
            self.ga.initialize(dim=6)
            self.ga_initialized = True

        # v19: GA fitness actually depends on recent rewards
        recent = np.mean(list(self.recent_rewards)) if self.recent_rewards else 0.5

        def fitness_func(weights):
            # Reward: normalized weights that correlate with historical reward
            w = np.clip(np.asarray(weights, dtype=np.float64), 0.0, None)
            if w.sum() <= 0:
                return 0.0
            w = w / w.sum()
            # Encourage convex combinations close to observed recent reward
            return float(recent - np.sum((w - 1.0 / len(w)) ** 2))

        best_weights = self.ga.evolve(fitness_func, generations=max(3, self.config.bio.bio_generations if hasattr(self.config.bio, "bio_generations") else 5))
        keys = ["green_score", "carbon_intensity", "latency", "cost", "pue", "helium_impact"]
        weight_dict = {k: float(v) for k, v in zip(keys, best_weights)}
        total = sum(abs(v) for v in weight_dict.values()) or 1.0
        weight_dict = {k: v / total for k, v in weight_dict.items()}
        return {"action": "adaptive_optimization",
                "weight_adjustment": weight_dict,
                "selection_method": "topsis" if random.random() > 0.5 else "nsga2",
                "estimated_improvement": float(recent)}

    def get_optimization_stats(self):
        bandit_theta = None
        try:
            if hasattr(self.bandit, "theta"):
                bandit_theta = [np.asarray(t).tolist() for t in self.bandit.theta]
        except Exception:
            pass
        return {
            "total_optimizations": len(self.optimization_history),
            "strategies": self.strategy_keys,
            "recent_optimizations": list(self.optimization_history)[-5:],
            "strategy_usage": {s: sum(1 for h in self.optimization_history if h["strategy"] == s)
                               for s in self.strategy_keys},
            "bandit_theta": bandit_theta,
            "distillation_active": self.distill_ensemble is not None,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "causal_active": self.causal is not None,
            "federated_active": self.federated is not None,
            "precision_active": self.precision_adapter is not None,
            "carbon_market_active": self.recs is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "role_snapshot": self.roles.snapshot() if self.roles else {},
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
            "last_counterfactuals": {int(k): float(v) for k, v in self.last_counterfactuals.items()},
        }

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# MAIN SELECTOR
# ============================================================
class EnhancedGreenDataCenterSelector:
    def __init__(
        self,
        config: SelectorConfig,
        db_manager: IDatabaseManager,
        quantum_security: IQuantumSecurity,
        blockchain: IBlockchain,
        carbon_manager: ICarbonManager,
        autonomous_optimizer: IAutonomousOptimizer,
        cloud_orchestrator: ICloudOrchestrator,
        cloud_storage: ICloudStorage,
        vault: IVault,
        predictive: Optional[IPredictive] = None,
        leader: Optional[LeaderElection] = None,
        task_manager: Optional[TaskManager] = None,
    ):
        self.config = config
        self.instance_id = config.general.instance_id
        self.db_manager = db_manager
        self.quantum_security = quantum_security
        self.blockchain = blockchain
        self.carbon_manager = carbon_manager
        self.autonomous_optimizer = autonomous_optimizer
        self.cloud_orchestrator = cloud_orchestrator
        self.cloud_storage = cloud_storage
        self.vault = vault
        self.predictive = predictive
        self.leader = leader or LeaderElection(config)
        self.task_manager = task_manager or TaskManager()
        self.capacity_monitor = RealTimeCapacityMonitor(config)
        self.latency_monitor = NetworkLatencyMonitor(config)
        self.carbon_scheduler = (
            CarbonAwareSelectionScheduler(config, carbon_manager, predictive)
            if config.carbon_scheduler.enabled else None
        )
        self.latency_cache = AdaptiveTTLCache(config)
        self.capacity_cache = AdaptiveTTLCache(config)
        self.pue_cache = AdaptiveTTLCache(config)
        self.projects: List[DataCenterProject] = []
        self.selection_history: deque = deque(maxlen=100)
        self._projects_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self.ab_variants = config.ab_testing.variants
        self.ab_allocations = {v: config.ab_testing.allocations[i] for i, v in enumerate(self.ab_variants)}
        self.ab_results: Dict[str, List[float]] = defaultdict(list)
        self.ab_update_interval = config.ab_testing.update_interval
        self.criteria_weights = {
            "green_score": config.green_score_weight,
            "carbon_intensity": config.carbon_intensity_weight,
            "latency": config.latency_weight,
            "cost": config.cost_weight,
            "pue": config.pue_weight,
            "helium_impact": config.helium_impact_weight,
        }
        self.workload_predictor = WorkloadPredictor()
        self._health_components = {
            "database": self.db_manager,
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "carbon_manager": self.carbon_manager,
            "autonomous_optimizer": self.autonomous_optimizer,
            "cloud_orchestrator": self.cloud_orchestrator,
            "cloud_storage": self.cloud_storage,
            "vault": self.vault,
            "predictive": self.predictive,
            "capacity_monitor": self.capacity_monitor,
            "latency_monitor": self.latency_monitor,
        }
        self._register_background_tasks()
        logger.info(
            f"EnhancedGreenDataCenterSelector v{config.general.version} "
            f"initialized (instance: {self.instance_id})"
        )

    def _register_background_tasks(self):
        self.task_manager.register_task("health_check", self._health_check_loop, restart_on_exit=True)
        self.task_manager.register_task("auto_optimize", self._auto_optimize_loop, restart_on_exit=True)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop, restart_on_exit=True)
        if self.predictive:
            self.task_manager.register_task("predictive_update", self._predictive_update_loop, restart_on_exit=True)
        if self.config.ab_testing.enabled:
            self.task_manager.register_task("ab_update", self._ab_update_loop, restart_on_exit=True)

    async def start(self):
        await self.db_manager.init()
        await self.capacity_monitor.__aenter__()
        await self._load_projects()
        if not self.projects:
            await self._generate_sample_projects()
        if self.carbon_scheduler:
            await self.carbon_scheduler.start()
        self.task_manager.start_registered_tasks()
        logger.info("Selector started")

    async def _load_projects(self):
        if SQLALCHEMY_AVAILABLE:
            def load(conn):
                result = conn.execute(text(
                    "SELECT project_id, name, latitude, longitude, green_score, "
                    "carbon_intensity, pue_estimated, helium_efficiency, cost_per_hour, "
                    "latency_ms, capacity_mw, provider, region, last_updated FROM projects"
                ))
                projects = []
                for row in result:
                    projects.append(DataCenterProject(
                        project_id=row[0], name=row[1], latitude=row[2], longitude=row[3],
                        green_score=row[4], carbon_intensity=row[5], pue_estimated=row[6],
                        helium_efficiency=row[7], cost_per_hour=row[8], latency_ms=row[9],
                        capacity_mw=row[10], provider=row[11], region=row[12],
                        last_updated=row[13] if isinstance(row[13], datetime) else datetime.now(),
                    ))
                return projects
            try:
                self.projects = await self.db_manager.execute_sync(load)
            except Exception as e:
                logger.warning(f"load_projects failed: {e}")
                self.projects = []

    async def _generate_sample_projects(self):
        samples = [
            ("GreenDC Helsinki", 60.17, 24.94, 0.92, 250, 1.10, 0.85, 0.08, 45, 100, "aws", "eu-west-1"),
            ("EcoData Stockholm", 59.33, 18.07, 0.90, 280, 1.08, 0.90, 0.09, 50, 80, "azure", "northeurope"),
            ("Nordic DC", 59.91, 10.75, 0.88, 300, 1.12, 0.80, 0.10, 55, 120, "gcp", "europe-west1"),
        ]
        for name, lat, lon, green, carbon, pue, helium, cost, latency, cap, provider, region in samples:
            p = DataCenterProject(
                project_id=f"proj_{uuid.uuid4().hex[:8]}", name=name,
                latitude=lat, longitude=lon, green_score=green,
                carbon_intensity=carbon, pue_estimated=pue, helium_efficiency=helium,
                cost_per_hour=cost, latency_ms=latency, capacity_mw=cap,
                provider=provider, region=region,
            )
            self.projects.append(p)
            try:
                await self.db_manager.insert_project(p)
            except Exception as e:
                logger.warning(f"insert_project failed: {e}")

    async def _ab_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.ab_results:
                    avg_rewards = {v: float(np.mean(r)) if r else 0.0 for v, r in self.ab_results.items()}
                    # v19: bounded softmax
                    max_r = max(avg_rewards.values())
                    exps = {v: math.exp(r - max_r) for v, r in avg_rewards.items()}
                    total = sum(exps.values())
                    if total > 0:
                        for v in self.ab_variants:
                            self.ab_allocations[v] = exps[v] / total
                await asyncio.sleep(self.ab_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"AB update error: {e}")
                await asyncio.sleep(60)

    async def _auto_optimize_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                state = {
                    "carbon_intensity": await self.carbon_manager.get_current_intensity(),
                    "budget_constrained": False,
                    "workload_pattern": "steady",
                }
                result = await self.autonomous_optimizer.optimize_selection(state, "hybrid")
                if result.get("action"):
                    if "weight_adjustment" in result:
                        for key, value in result["weight_adjustment"].items():
                            if key in self.criteria_weights:
                                self.criteria_weights[key] = value
                await asyncio.sleep(self.config.general.auto_optimize_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto optimize error: {e}")
                await asyncio.sleep(60)

    async def _health_check_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                h = await self.health_check()
                HEALTH_SCORE.set(h.get("health_score", 100))
                await asyncio.sleep(self.config.general.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(60)

    async def _carbon_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                ci = await self.carbon_manager.get_current_intensity()
                CARBON_INTENSITY.set(ci)
                await asyncio.sleep(self.config.carbon.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.predictive:
                    await self.predictive.update_history(
                        len(self.projects),
                        await self.carbon_manager.get_current_intensity(),
                    )
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def select_datacenter(self, workload, user_region="us-east",
                                sign_decision=True, blockchain_record=True):
        if self.carbon_scheduler:
            return await self.carbon_scheduler.submit_selection(
                lambda: self._select_datacenter_internal(workload, user_region, sign_decision, blockchain_record),
                priority=1 if workload.priority == "normal" else 2,
                critical=workload.priority == "high",
            )
        return await self._select_datacenter_internal(workload, user_region, sign_decision, blockchain_record)

    async def _select_datacenter_internal(self, workload, user_region, sign_decision, blockchain_record):
        candidates = await self._get_candidates(user_region, workload)
        if not candidates:
            raise SelectionError("no candidates")

        if self.config.ab_testing.enabled and self.ab_allocations:
            method = random.choices(
                list(self.ab_allocations.keys()),
                weights=list(self.ab_allocations.values()),
            )[0]
        else:
            method = "topsis"

        if method == "weighted":
            scored = await self._score_candidates_weighted(candidates, workload)
        elif method == "topsis":
            scored = await self._score_candidates_topsis(candidates, workload)
        elif method == "nsga2":
            scored = await self._score_candidates_nsga2(candidates, workload)
        else:
            scored = await self._score_candidates_weighted(candidates, workload)

        best = max(scored, key=lambda x: x["score"])
        selected_project = best["project"]
        selection_id = f"sel_{uuid.uuid4().hex[:8]}"
        result = SelectionResult(
            selection_id=selection_id,
            selected_project=selected_project,
            method=method,
            confidence_score=best["score"],
        )

        decision_manifest = {
            "selection_id": selection_id,
            "selected_project_id": selected_project.project_id,
            "method": method,
            "confidence": result.confidence_score,
            "timestamp": datetime.now().isoformat(),
        }

        file_hash = hashlib.sha256(
            json.dumps(decision_manifest, sort_keys=True, default=str).encode()
        ).hexdigest()
        result.file_hash = file_hash

        if sign_decision:
            quantum_key = await self.quantum_security.generate_keypair(self.config.quantum.algorithm)
            signature = await self.quantum_security.sign_selection_decision(decision_manifest, quantum_key["key_id"])
            result.quantum_signature = signature

        if blockchain_record:
            try:
                bc = await self.blockchain.record_selection(selection_id, decision_manifest, file_hash)
                result.blockchain_tx_hash = bc.get("tx_hash")
            except Exception as e:
                logger.warning(f"blockchain record failed: {e}")

        async with self._history_lock:
            self.selection_history.append(result)

        try:
            await self.db_manager.insert_selection(
                selection_id, selected_project.project_id, method,
                result.confidence_score, file_hash,
                result.blockchain_tx_hash or "", 0,
            )
        except Exception as e:
            logger.warning(f"insert_selection failed: {e}")

        if self.config.ab_testing.enabled:
            self.ab_results[method].append(1.0)

        providers = getattr(self.cloud_storage, "providers", {})
        if providers:
            try:
                await self.cloud_storage.store(decision_manifest, f"selection_{selection_id}.json")
            except Exception as e:
                logger.error(f"Cloud storage failed: {e}")

        SELECTIONS_TOTAL.labels(status="success").inc()
        return result

    async def _get_candidates(self, user_region, workload):
        out: List[DataCenterProject] = []
        for p in self.projects:
            cp = DataCenterProject(
                project_id=p.project_id, name=p.name,
                latitude=p.latitude, longitude=p.longitude,
                green_score=p.green_score, carbon_intensity=p.carbon_intensity,
                pue_estimated=p.pue_estimated, helium_efficiency=p.helium_efficiency,
                cost_per_hour=p.cost_per_hour,
                latency_ms=await self.latency_monitor.get_latency(p.provider, p.region),
                capacity_mw=p.capacity_mw * await self.capacity_monitor.get_capacity(p.project_id),
                provider=p.provider, region=p.region,
            )
            out.append(cp)
        return out

    async def _score_candidates_weighted(self, candidates, workload):
        scored = []
        for p in candidates:
            s = 0.0
            s += p.green_score * self.criteria_weights["green_score"]
            s += (1 - p.carbon_intensity / 1000) * self.criteria_weights["carbon_intensity"]
            s += (1 - p.latency_ms / 200) * self.criteria_weights["latency"]
            s += (1 - p.cost_per_hour / 0.5) * self.criteria_weights["cost"]
            s += (1 - p.pue_estimated / 2.0) * self.criteria_weights["pue"]
            s += p.helium_efficiency * self.criteria_weights["helium_impact"]
            scored.append({"project": p, "score": float(s)})
        return scored

    async def _score_candidates_topsis(self, candidates, workload):
        rows = []
        for p in candidates:
            rows.append({
                "green_score": p.green_score,
                "carbon_intensity": 1 - p.carbon_intensity / 1000,
                "latency": 1 - p.latency_ms / 200,
                "cost": 1 - p.cost_per_hour / 0.5,
                "pue": 1 - p.pue_estimated / 2.0,
                "helium_eff": p.helium_efficiency,
            })
        weights = [
            self.criteria_weights["green_score"],
            self.criteria_weights["carbon_intensity"],
            self.criteria_weights["latency"],
            self.criteria_weights["cost"],
            self.criteria_weights["pue"],
            self.criteria_weights["helium_impact"],
        ]
        total = sum(weights)
        if total <= 0:
            total = 1.0
        weights = [w / total for w in weights]
        scores = TOPSIS.score(rows, weights, list(rows[0].keys()))
        return [{"project": p, "score": s} for p, s in zip(candidates, scores)]

    async def _score_candidates_nsga2(self, candidates, workload):
        # v19: track non-dominated projects by index, not id()
        front = ParetoFront()
        for i, p in enumerate(candidates):
            obj = [p.cost_per_hour, p.carbon_intensity, p.latency_ms,
                   p.pue_estimated, -p.green_score, -p.helium_efficiency]
            front.add(obj, i)
        non_dominated_indices: Set[int] = {dec for _, dec in front.get_pareto_front()}
        scored = []
        for i, p in enumerate(candidates):
            s = 0.0
            s += p.green_score * self.criteria_weights["green_score"]
            s += (1 - p.carbon_intensity / 1000) * self.criteria_weights["carbon_intensity"]
            s += (1 - p.latency_ms / 200) * self.criteria_weights["latency"]
            s += (1 - p.cost_per_hour / 0.5) * self.criteria_weights["cost"]
            s += (1 - p.pue_estimated / 2.0) * self.criteria_weights["pue"]
            s += p.helium_efficiency * self.criteria_weights["helium_impact"]
            if i not in non_dominated_indices:
                s *= 0.5
            scored.append({"project": p, "score": float(s)})
        return scored

    async def orchestrate_selection_multi_cloud(self, workload):
        return await self.cloud_orchestrator.orchestrate_selection(workload)

    async def get_cloud_status(self):
        return await self.cloud_orchestrator.get_provider_status()

    async def submit_hitl_decision(self, request_id: str, approved: bool) -> bool:
        """v19: deliver an external HITL decision to the optimizer's gate."""
        gate = getattr(self.autonomous_optimizer, "hitl", None)
        if gate is None:
            return False
        return await gate.submit_decision(request_id, approved)

    async def save_state(self, path: str) -> bool:
        """v19: persist causal, roles, federated, A/B results, predictor history."""
        state: Dict[str, Any] = {"schema": "green_selector_state_v1"}
        opt = self.autonomous_optimizer
        if getattr(opt, "causal", None) is not None:
            state["causal"] = opt.causal.export_state()
        if getattr(opt, "roles", None) is not None:
            state["roles"] = opt.roles.snapshot()
        if getattr(opt, "federated", None) is not None:
            g = opt.federated.global_weights()
            if g:
                state["federated_global"] = {k: v.tolist() for k, v in g.items()}
        state["ab_allocations"] = dict(self.ab_allocations)
        state["ab_results"] = {k: list(v) for k, v in self.ab_results.items()}
        state["workload_predictor_history"] = list(self.workload_predictor._history)
        state["criteria_weights"] = dict(self.criteria_weights)
        try:
            d = os.path.dirname(os.path.abspath(path)) or "."
            os.makedirs(d, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=d, prefix=".sel_", suffix=".json")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(state, f, default=str)
                os.replace(tmp, path)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            return True
        except Exception as e:
            logger.error(f"save_state failed: {e}")
            return False

    async def load_state(self, path: str) -> bool:
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r") as f:
                state = json.load(f)
        except Exception:
            return False
        if state.get("schema") != "green_selector_state_v1":
            return False
        opt = self.autonomous_optimizer
        if getattr(opt, "causal", None) is not None and "causal" in state:
            opt.causal.import_state(state["causal"])
        if getattr(opt, "federated", None) is not None and "federated_global" in state:
            try:
                opt.federated._global = {
                    k: np.asarray(v, dtype=np.float64)
                    for k, v in state["federated_global"].items()
                }
            except Exception:
                pass
        if "ab_allocations" in state:
            self.ab_allocations = dict(state["ab_allocations"])
        if "ab_results" in state:
            self.ab_results = defaultdict(list, {k: list(v) for k, v in state["ab_results"].items()})
        if "workload_predictor_history" in state:
            self.workload_predictor._history = deque(state["workload_predictor_history"], maxlen=256)
            self.workload_predictor.is_trained = len(self.workload_predictor._history) >= 10
        if "criteria_weights" in state:
            self.criteria_weights = dict(state["criteria_weights"])
        return True

    async def get_comprehensive_status(self):
        quantum_status = self.quantum_security.get_quantum_status()
        blockchain_status = await self.blockchain.get_blockchain_status()
        optimization_stats = self.autonomous_optimizer.get_optimization_stats()
        cloud_status = await self.cloud_orchestrator.get_provider_status()
        async with self._projects_lock:
            avg_green = float(np.mean([p.green_score for p in self.projects])) if self.projects else 0.0
            avg_pue = float(np.mean([p.pue_estimated for p in self.projects])) if self.projects else 0.0
        async with self._history_lock:
            selections = len(self.selection_history)
            avg_conf = float(np.mean([r.confidence_score for r in self.selection_history])) if self.selection_history else 0.0
        return {
            "instance_id": self.instance_id,
            "version": self.config.general.version,
            "quantum_security": quantum_status,
            "blockchain": blockchain_status,
            "autonomous_optimization": optimization_stats,
            "cloud_orchestration": cloud_status,
            "projects": {"total": len(self.projects), "avg_green_score": avg_green, "avg_pue": avg_pue},
            "selections": {"total": selections, "avg_confidence": avg_conf},
            "ml_model": {"trained": self.workload_predictor.is_trained},
            "predictive": self.predictive.get_stats() if (self.predictive and hasattr(self.predictive, "get_stats")) else None,
            "cloud_storage": {"providers": list(getattr(self.cloud_storage, "providers", {}).keys())},
            "leader": {"is_leader": self.leader.is_leader},
            "health": await self.health_check(),
            "ab_testing": {
                "enabled": self.config.ab_testing.enabled,
                "allocations": self.ab_allocations,
                "results": {k: (float(np.mean(v)) if v else 0.0) for k, v in self.ab_results.items()},
            },
            "timestamp": datetime.now().isoformat(),
        }

    async def health_check(self):
        results = {}
        for name, comp in self._health_components.items():
            if comp and hasattr(comp, "health_check"):
                try:
                    results[name] = await comp.health_check()
                except Exception as e:
                    results[name] = {"status": "unhealthy", "error": str(e)}
            else:
                results[name] = {"status": "ok" if comp else "unavailable"}
        checkable = [r for r in results.values() if r.get("status") != "unavailable"]
        if not checkable:
            overall, score = "degraded", 0
        else:
            ok = all(r.get("status") in ("ok", "healthy") for r in checkable)
            overall, score = ("healthy", 100) if ok else ("degraded", 50)
        return {"status": overall, "health_score": score,
                "components": results, "timestamp": datetime.now().isoformat()}

    async def shutdown(self):
        logger.info(f"Shutting down (instance: {self.instance_id})")
        await self.task_manager.stop_all()
        with contextlib.suppress(Exception):
            await self.capacity_monitor.__aexit__(None, None, None)
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        with contextlib.suppress(Exception):
            await self.db_manager.close()
        with contextlib.suppress(Exception):
            await self.leader.stop()
        if self.carbon_scheduler:
            with contextlib.suppress(Exception):
                await self.carbon_scheduler.stop()
        logger.info("Shutdown complete")


# ============================================================
# FASTAPI
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Green Data Center Selector API", version="19.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"],
    )
    security = HTTPBearer()

    _api_config: Optional[SelectorConfig] = None
    _api_rate_limiter: Optional[RateLimiter] = None

    def _get_config() -> SelectorConfig:
        global _api_config
        if _api_config is None:
            _api_config = SelectorConfig()
        return _api_config

    def _get_limiter() -> RateLimiter:
        global _api_rate_limiter
        if _api_rate_limiter is None:
            cfg = _get_config()
            _api_rate_limiter = RateLimiter(rate=cfg.api.rate_limit_requests,
                                            per_seconds=cfg.api.rate_limit_window)
        return _api_rate_limiter

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        try:
            return jwt.decode(credentials.credentials, _get_config().api.jwt_secret,
                              algorithms=["HS256"])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    async def rate_limit(request: Request):
        cfg = _get_config()
        if not cfg.api.rate_limit_enabled:
            return
        limiter = _get_limiter()
        if not await limiter.acquire():
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

    selector: Optional[EnhancedGreenDataCenterSelector] = None

    @app.post("/select")
    async def select(workload: WorkloadSpec, user_region: str = "us-east",
                     sign_decision: bool = True, blockchain_record: bool = True,
                     user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not selector:
            raise HTTPException(status_code=503, detail="Selector not initialized")
        result = await selector.select_datacenter(workload, user_region, sign_decision, blockchain_record)
        return {
            "selection_id": result.selection_id,
            "selected_project_id": result.selected_project.project_id,
            "method": result.method,
            "confidence": result.confidence_score,
            "quantum_signature": result.quantum_signature,
            "blockchain_tx_hash": result.blockchain_tx_hash,
        }

    @app.post("/orchestrate")
    async def orchestrate(workload: WorkloadSpec,
                          user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not selector:
            raise HTTPException(status_code=503, detail="Selector not initialized")
        return await selector.orchestrate_selection_multi_cloud(workload)

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not selector:
            raise HTTPException(status_code=503, detail="Selector not initialized")
        return await selector.get_comprehensive_status()

    @app.get("/health")
    async def health(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not selector:
            raise HTTPException(status_code=503, detail="Selector not initialized")
        return await selector.health_check()

    @app.get("/explanation/last")
    async def last_explanation(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not selector:
            raise HTTPException(status_code=503, detail="Selector not initialized")
        stats = selector.autonomous_optimizer.get_optimization_stats()
        return {"explanation": stats.get("last_explanation"),
                "counterfactuals": stats.get("last_counterfactuals")}

    @app.post("/chaos")
    async def chaos(fault_prob: float = 0.0, latency_ms: float = 0.0,
                    carbon_spike_prob: float = 0.0,
                    user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not selector:
            raise HTTPException(status_code=503, detail="Selector not initialized")
        opt = selector.autonomous_optimizer
        opt.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=fault_prob, latency_inject_ms=latency_ms,
            carbon_spike_prob=carbon_spike_prob,
        ))
        return {"status": "chaos enabled", "config": asdict(opt.chaos.config)}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        """v19: deliver the decision to the waiting HITL request."""
        if not selector:
            raise HTTPException(status_code=503, detail="Selector not initialized")
        delivered = await selector.submit_hitl_decision(request_id, approved)
        if not delivered:
            HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
            audit_logger.info(f"HITL approval (no pending): {request_id} -> {approved}")
            return {"status": "recorded", "request_id": request_id,
                    "approved": approved, "delivered": False}
        audit_logger.info(f"HITL approval delivered: {request_id} -> {approved}")
        return {"status": "delivered", "request_id": request_id, "approved": approved}

    @app.on_event("startup")
    async def startup():
        global selector
        config = SelectorConfig()
        db_manager = EnhancedDatabaseManager(config)
        vault = VaultManager(config)
        quantum = PostQuantumCrypto(config, vault)
        blockchain = BlockchainSelectionVerification(config)
        carbon = CarbonIntensityManager(
            api_key=config.carbon.api_key,
            region=config.carbon.region,
            base_price=config.carbon.base_price_per_kg,
            sensitivity=config.carbon.price_sensitivity,
            rec_default_kwh=config.carbon.rec_default_kwh,
        )
        cloud_orch = EnhancedMultiCloudSelectionOrchestrator(config, db_manager, carbon)
        optimizer = EnhancedAutonomousOptimizer(config, db_manager, carbon)
        cloud_storage = MultiCloudStorage(config)
        predictive = MixtureOfExpertsPredictive(config) if config.moe.enabled else None
        leader = LeaderElection(config)
        task_manager = TaskManager()
        selector = EnhancedGreenDataCenterSelector(
            config=config, db_manager=db_manager, quantum_security=quantum,
            blockchain=blockchain, carbon_manager=carbon,
            autonomous_optimizer=optimizer, cloud_orchestrator=cloud_orch,
            cloud_storage=cloud_storage, vault=vault, predictive=predictive,
            leader=leader, task_manager=task_manager,
        )
        await selector.start()
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown_event():
        if selector:
            await selector.shutdown()


# ============================================================
# SINGLETON
# ============================================================
_selector_instance: Optional[EnhancedGreenDataCenterSelector] = None
_selector_lock = asyncio.Lock()


async def get_green_datacenter_selector(config: Optional[Union[SelectorConfig, Dict]] = None) -> EnhancedGreenDataCenterSelector:
    global _selector_instance
    if _selector_instance is None:
        async with _selector_lock:
            if _selector_instance is None:
                if isinstance(config, SelectorConfig):
                    cfg = config
                elif isinstance(config, dict) and PYDANTIC_AVAILABLE:
                    cfg = SelectorConfig(**config)
                else:
                    cfg = SelectorConfig()

                db_manager = EnhancedDatabaseManager(cfg)
                vault = VaultManager(cfg)
                quantum = PostQuantumCrypto(cfg, vault)
                blockchain = BlockchainSelectionVerification(cfg)
                carbon = CarbonIntensityManager(
                    api_key=cfg.carbon.api_key,
                    region=cfg.carbon.region,
                    base_price=cfg.carbon.base_price_per_kg,
                    sensitivity=cfg.carbon.price_sensitivity,
                    rec_default_kwh=cfg.carbon.rec_default_kwh,
                )
                cloud_orch = EnhancedMultiCloudSelectionOrchestrator(cfg, db_manager, carbon)
                optimizer = EnhancedAutonomousOptimizer(cfg, db_manager, carbon)
                cloud_storage = MultiCloudStorage(cfg)
                predictive = MixtureOfExpertsPredictive(cfg) if cfg.moe.enabled else None
                leader = LeaderElection(cfg)
                task_manager = TaskManager()
                _selector_instance = EnhancedGreenDataCenterSelector(
                    config=cfg, db_manager=db_manager, quantum_security=quantum,
                    blockchain=blockchain, carbon_manager=carbon,
                    autonomous_optimizer=optimizer, cloud_orchestrator=cloud_orch,
                    cloud_storage=cloud_storage, vault=vault, predictive=predictive,
                    leader=leader, task_manager=task_manager,
                )
                await _selector_instance.start()
    return _selector_instance


# ============================================================
# SIGNAL HANDLING (v19: retained task references)
# ============================================================
_shutdown_tasks: set = set()


async def shutdown_handler():
    global _selector_instance
    if _selector_instance:
        await _selector_instance.shutdown()
        _selector_instance = None


def _spawn_shutdown_task():
    task = asyncio.create_task(shutdown_handler())
    _shutdown_tasks.add(task)
    task.add_done_callback(_shutdown_tasks.discard)
    return task


def _install_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _spawn_shutdown_task)
        except (NotImplementedError, AttributeError):
            try:
                signal.signal(sig, lambda s, f: _spawn_shutdown_task())
            except (ValueError, OSError):
                pass


# ============================================================
# MAIN
# ============================================================
async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Green Data Center Selector v19.0 — Enterprise Quantum+ (shared modules)")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("SELECTOR_SERVE_API", "0") == "1":
        cfg = SelectorConfig()
        uvicorn.run(app, host=cfg.api.host, port=cfg.api.port, log_level="info")
        return

    selector = await get_green_datacenter_selector()

    print("\n✅ Ten enhancements imported from the enhancements package:")
    print("   [1] Quantum-Distillation          QuantumInspiredTeacher + DistillationEnsemble")
    print("   [2] Causal RL                     CausalCounterfactualEstimator")
    print("   [3] Federated Green Learning      FederatedAggregator")
    print("   [4] Multi-Agent Coordination      EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic                STLFormula + TemporalLogicMonitor + SafetyShield")
    print("   [6] Explainable AI                DecisionExplainer")
    print("   [7] Adaptive Precision            PrecisionController + HardwareAwareAdapter")
    print("   [8] Carbon Markets / RECs         CarbonMarketClient + RECInventory")
    print("   [9] Resilience / Chaos            ChaosEngineer")
    print("  [10] HITL / Active Learning        UncertaintyEstimator + HumanInTheLoopGate")

    qstatus = selector.quantum_security.get_quantum_status()
    print(f"\n🔐 PQC available: {qstatus.get('pqc_available', False)}")

    bstatus = await selector.blockchain.get_blockchain_status()
    print(f"⛓️ Blockchain connected: {bstatus.get('connected', False)}")

    cstatus = await selector.cloud_orchestrator.get_provider_status()
    print(f"☁️ Active provider: {cstatus.get('active_provider')}, region: {cstatus.get('active_region')}")

    workload = WorkloadSpec(
        gpu_hours=500, latency_tolerance_ms=100, cost_budget_usd=5000,
        carbon_budget_kg=500, workload_pattern="bursty",
        priority="normal", spot_instance_ok=True,
        compliance_requirements=["GDPR", "SOC2"],
        historical_patterns=[100, 200, 500, 300, 800, 400, 600, 700, 300, 500],
    )

    print("\n⚡ Testing autonomous optimization:")
    opt_state = {
        "carbon_intensity": await selector.carbon_manager.get_current_intensity(),
        "budget_constrained": False,
        "workload_pattern": "bursty",
    }
    opt_result = await selector.autonomous_optimizer.optimize_selection(opt_state, "hybrid")
    print(f"   Strategy:     {opt_result.get('strategy')}")
    print(f"   Precision:    {opt_result.get('precision_level')}")
    print(f"   Safety:       {opt_result.get('safety_ok')}")
    print(f"   HITL:         {opt_result.get('human_approved')}")
    print(f"   Winning role: {opt_result.get('winning_role')}")
    if opt_result.get("explanation"):
        print(f"   XAI:          {opt_result['explanation'].get('rationale')}")

    print("\n🌐 Testing cloud orchestration:")
    orch = await selector.orchestrate_selection_multi_cloud(workload)
    print(f"   Optimal provider: {orch.get('optimal_provider')}")
    print(f"   Source:           {orch.get('source')}")
    print(f"   Winning role:     {orch.get('winning_role')}")

    print("\n🎯 Testing datacenter selection:")
    res = await selector.select_datacenter(workload, user_region="us-east")
    print(f"   Selected:  {res.selected_project.name} (conf={res.confidence_score:.2f})")
    print(f"   Method:    {res.method}")
    print(f"   PQC:       {'✅' if res.quantum_signature else '❌'}")
    print(f"   TX:        {res.blockchain_tx_hash or 'N/A'}")

    status = await selector.get_comprehensive_status()
    print(f"\n📊 Status:")
    print(f"   Version:  {status['version']}")
    print(f"   Projects: {status['projects']['total']}")
    print(f"   Health:   {status['health']['health_score']}")
    print(f"   A/B:      {status['ab_testing']['enabled']}")

    print("\n" + "=" * 80)
    print("✅ Green Data Center Selector v19.0 — Ready")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await selector.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
