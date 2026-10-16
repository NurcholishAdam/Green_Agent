#!/usr/bin/env python3
# src/enhancements/green_datacenter_map_enhanced_v19_0.py
"""
Green Data Center Map & Visualization System — v19.0

The ten Green Agent enhancements are imported from the enhancements/*
package. This file keeps only map-specific logic:

  - Project store (ORM + in-memory list)
  - CarbonAwareExportQueue with monotonic tie-break
  - AdaptiveTTLCache with LRU-ish eviction
  - EnhancedGeocodingService
  - ContextualBandit (map-local canonical) + ParetoFront
  - ParticleSwarmOptimizer + BioInspiredCloudDeployer
  - MultiObjectiveCloudDeployer
  - MixtureOfExpertsPredictive
  - EnhancedAutonomousMapGenerator
  - EnhancedGreenDataCenterMap
  - FastAPI app, singleton, signal handlers, main

Changelog v18.0 -> v19.0:
  - All ten enhancement classes imported from enhancements/*.
  - Compat wrappers for Explanation, DecisionExplainer, HITLRequest,
    UncertaintyEstimator, HumanInTheLoopGate.
  - MultiAgentCoordinator.vote tuples are unpacked at both call sites.
  - EmergentRoleRegistry credits the actual winning role.
  - Causal state uses state_vector_from_metrics.
  - ParticleSwarmOptimizer guards against zero particles.
  - MultiObjectiveCloudDeployer._update_weights normalizes by per-objective std.
  - AdaptiveTTLCache bumps access_count on set() as well as get().
  - HITL requests can be delivered externally via submit_hitl_decision().
  - FastAPI /hitl/approval calls submit_hitl_decision().
  - Signal handlers retain task references.
  - save_state / load_state for causal, roles, federated, generation history.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import contextvars
import hashlib
import heapq
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
import weakref
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol,
    Sequence, Set, Tuple, Union, runtime_checkable,
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
    """Map-side Explanation carrying chosen_idx."""
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
    """Adds the map's explain(...) signature."""

    def explain(self, chosen_idx, chosen, pareto, teacher_probs,
                counterfactuals, safety_ok, carbon_price, confidence: float = 0.5):
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
    """Map-side HITL request with candidate list."""
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
    """Adds an async request() that accepts the map's HITLRequest,
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
    from sqlalchemy import create_engine  # noqa: F401
    from sqlalchemy.orm import declarative_base as _decl_base  # noqa: F401
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
# FALLBACKS FOR LEGACY ENHANCEMENT MODULES NOT IN THE PACKAGE
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

    class ParetoOptimizer:
        def __init__(self, *a, **kw): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0.0) * weights.get(k, 1.0) for k in objectives)

    class MultiTeacherDistiller:
        def __init__(self, teachers, weights=None, *a, **kw):
            self.teachers = list(teachers)
            self.weights = list(weights) if weights else [1.0] * len(self.teachers)

        def distill(self, context):
            votes: Dict[Any, float] = {}
            for t, w in zip(self.teachers, self.weights):
                try:
                    choice = t(context)
                    if asyncio.iscoroutine(choice):
                        choice.close()
                        continue
                    votes[choice] = votes.get(choice, 0.0) + float(w)
                except Exception:
                    continue
            if not votes:
                return None
            return max(votes, key=votes.get)


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
    MAP_EXPORTS = Counter("map_exports_total", "Exports", ["status"], registry=REGISTRY)
    MAP_GENERATIONS = Counter("map_generations_total", "Gens", ["strategy", "status"], registry=REGISTRY)
    CLOUD_DEPLOYMENTS = Counter("cloud_deployments_total", "Deploys", ["provider", "status"], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("quantum_signatures_total", "Sigs", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("blockchain_verifications_total", "BC", ["status"], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge("map_circuit_breaker_state", "CB", ["name"], registry=REGISTRY)
    CLOUD_STORAGE = Counter("map_cloud_storage_operations_total", "S", ["provider", "operation", "status"], registry=REGISTRY)
    VAULT_OPERATIONS = Counter("map_vault_operations_total", "V", ["operation", "status"], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge("map_predictive_accuracy", "PA", ["model"], registry=REGISTRY)
    HEALTH_SCORE = Gauge("map_health_score", "HS", registry=REGISTRY)
    CARBON_PRICE = Gauge("map_carbon_price_per_kg", "CP", registry=REGISTRY)
    REC_INVENTORY_KWH = Gauge("map_rec_inventory_kwh", "REC", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("map_precision_switches_total", "PS", ["level"], registry=REGISTRY)
    CHAOS_EVENTS = Counter("map_chaos_events_total", "CE", ["type"], registry=REGISTRY)
    HITL_APPROVALS = Counter("map_hitl_approvals_total", "H", ["decision"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("map_safety_violations_total", "SV", ["formula"], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("map_federated_rounds_total", "FR", registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    MAP_EXPORTS = MAP_GENERATIONS = CLOUD_DEPLOYMENTS = DummyMetrics()
    QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = CIRCUIT_BREAKER_STATE = DummyMetrics()
    CLOUD_STORAGE = VAULT_OPERATIONS = PREDICTIVE_ACCURACY = DummyMetrics()
    HEALTH_SCORE = CARBON_PRICE = REC_INVENTORY_KWH = DummyMetrics()
    PRECISION_SWITCHES = CHAOS_EVENTS = HITL_APPROVALS = SAFETY_VIOLATIONS = FEDERATED_ROUNDS = DummyMetrics()


# ============================================================
# EXCEPTIONS
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
class DatabaseError(GreenMapError): pass
class SafetyViolationError(GreenMapError): pass


# ============================================================
# INTERFACES
# ============================================================
@runtime_checkable
class IQuantumSecurity(Protocol):
    async def generate_keypair(self, algorithm: str = None) -> Dict: ...
    async def sign_map_export(self, export_data: Dict, key_id: str) -> Dict: ...
    async def verify_map_export(self, export_data: Dict, signature_data: Dict) -> bool: ...
    def get_quantum_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IBlockchain(Protocol):
    async def record_map_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict: ...
    async def verify_map_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict: ...
    async def get_blockchain_status(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IAutonomousGenerator(Protocol):
    async def generate_map_autonomously(self, data: Dict, strategy: str = None) -> Dict: ...
    def get_generation_stats(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class ICloudDeployer(Protocol):
    async def deploy_map(self, map_data: Dict, preferences: Dict) -> Dict: ...
    async def get_deployment_status(self) -> Dict: ...
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
    async def update_history(self, count: int, carbon_intensity: float): ...
    async def forecast_project_count(self, horizon_hours: int = None) -> Dict: ...
    async def forecast_carbon(self, horizon_hours: int = None) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IExportQueue(Protocol):
    async def submit(self, job): ...
    async def start(self): ...
    async def stop(self): ...
    def get_stats(self) -> Dict: ...
    async def health_check(self) -> Dict: ...


@runtime_checkable
class IGeocoder(Protocol):
    async def geocode(self, address: str) -> Optional[Tuple[float, float]]: ...
    async def batch_geocode(self, addresses: List[str]) -> List[Optional[Tuple[float, float]]]: ...
    async def get_statistics(self) -> Dict: ...
    async def stop(self): ...
    async def health_check(self) -> Dict: ...


# ============================================================
# DATA CLASSES
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
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
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
    def __init__(self, rate: int, per_seconds: int = 60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = float(rate)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
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
# TASK MANAGER
# ============================================================
class TaskManager:
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Tuple[Callable, tuple, dict, bool]] = {}
        self.metrics = {"total_tasks": 0, "completed": 0, "failed": 0, "restarts": 0}

    def start_task(self, name: str, coro_func, *args,
                   restart_on_exit: bool = False, **kwargs):
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

    def register_task(self, name, coro_func, *args,
                      restart_on_exit: bool = True, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs, restart_on_exit)

    def start_registered_tasks(self):
        for name, (coro, args, kwargs, restart) in list(self._task_coroutines.items()):
            self.start_task(name, coro, *args, restart_on_exit=restart, **kwargs)
        self._task_coroutines.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
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
        output_dir: str = Field("./maps")
        backup_interval: int = Field(86400, ge=60)
        max_concurrent_map_generations: int = Field(5, ge=1)
        max_concurrent_exports: int = Field(3, ge=1)
        retry_attempts: int = Field(3, ge=0)
        retry_wait_seconds: int = Field(2, ge=1)
        health_check_interval: int = Field(60, ge=10)

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
                raise ValueError("master_key must be a hex string")
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
        url: str = Field("sqlite+aiosqlite:///green_map.db")
        pool_size: int = Field(10, ge=1)
        max_overflow: int = Field(20, ge=0)

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = Field("secret/map")

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
            default_factory=lambda: {"performance": 0.4, "carbon": 0.3, "cost": 0.3}
        )
        bio_population_size: int = Field(20, ge=2)
        bio_generations: int = Field(10, ge=1)
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

    class GeneratorConfig(BaseModel):
        enabled: bool = True
        default_strategy: str = Field("hybrid")

    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = Field("pareto")
        weights: List[float] = Field(default_factory=lambda: [0.4, 0.3, 0.3])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 3
        gating_model: str = Field("logistic")
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("pso")
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1

    class CarbonSchedulerConfig(BaseModel):
        enabled: bool = True
        threshold: float = 400.0
        max_delay_seconds: int = 300

    class GeocoderConfig(BaseModel):
        enabled: bool = True
        cache_ttl: int = 86400
        batch_size: int = 100
        rate_limit_per_second: int = 10

    class CacheConfig(BaseModel):
        enabled: bool = True
        default_ttl: int = 3600
        max_size_mb: int = 500
        eviction_policy: str = Field("lru")

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

        def get_master_key_bytes(self):
            return self.quantum.get_master_key_bytes()

else:
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "19.0"
        log_level: str = "INFO"
        output_dir: str = "./maps"
        backup_interval: int = 86400
        max_concurrent_map_generations: int = 5
        max_concurrent_exports: int = 3
        retry_attempts: int = 3
        retry_wait_seconds: int = 2
        health_check_interval: int = 60

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
        url: str = "sqlite+aiosqlite:///green_map.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/map"

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
            default_factory=lambda: {"performance": 0.4, "carbon": 0.3, "cost": 0.3}
        )
        bio_population_size: int = 20
        bio_generations: int = 10
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
    class GeneratorConfig:
        enabled: bool = True
        default_strategy: str = "hybrid"

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
        status = Column(String(32))
        latitude = Column(Float)
        longitude = Column(Float)
        capacity_mw = Column(Float)
        carbon_intensity = Column(Float)
        helium_efficiency = Column(Float)
        last_updated = Column(DateTime, default=datetime.now)

    class ExportRecordDB(Base):
        __tablename__ = "export_records"
        id = Column(Integer, primary_key=True)
        export_id = Column(String(64), unique=True, index=True)
        export_type = Column(String(32))
        file_hash = Column(String(128))
        tx_hash = Column(String(128))
        block_number = Column(Integer)
        verified = Column(Boolean, default=False)
        timestamp = Column(DateTime, default=datetime.now)

    class GenerationHistoryDB(Base):
        __tablename__ = "generation_history"
        id = Column(Integer, primary_key=True)
        strategy = Column(String(32))
        result = Column(JSON)
        timestamp = Column(DateTime, default=datetime.now)

    class CloudDeploymentDB(Base):
        __tablename__ = "cloud_deployments"
        id = Column(Integer, primary_key=True)
        provider = Column(String(32))
        region = Column(String(64))
        map_path = Column(String(512))
        cdn_url = Column(String(256))
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
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault.url:
            try:
                self.client = VaultClient(url=config.vault.url, token=config.vault.token)
            except Exception as e:
                logger.warning(f"Vault init failed: {e}")

    async def store_secret(self, path: str, data: Dict):
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

    async def get_secret(self, path: str):
        if not self.client:
            return None
        loop = asyncio.get_running_loop()
        try:
            secret = await loop.run_in_executor(
                None,
                lambda: self.client.secrets.kv.v2.read_secret_version(
                    path=path, mount_point="secret"
                ),
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
    SCHEMA_VERSION = 1

    def __init__(self, config: GreenMapConfig):
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
            result = await conn.execute(text(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            ))
            row = result.fetchone()
            current_ver = row[0] if row else 0
            if current_ver < 1:
                if Base is not None:
                    await conn.run_sync(Base.metadata.create_all)
                await conn.execute(text(
                    "INSERT INTO schema_version (version, applied_at) VALUES (1, datetime('now'))"
                ))
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
        if not self.async_engine:
            return []
        loop = asyncio.get_running_loop()

        def _run():
            engine = create_engine(self.db_url.replace("+aiosqlite", ""))
            with engine.begin() as conn:
                return func(conn)
        try:
            return await loop.run_in_executor(None, _run)
        except Exception as e:
            logger.warning(f"execute_sync failed: {e}")
            return []

    async def insert_project(self, project: DataCenterProject):
        if not self.async_session:
            return
        async with self.async_session() as session:
            await session.execute(
                text("""
                    INSERT OR REPLACE INTO projects
                    (project_id, name, status, latitude, longitude, capacity_mw,
                     carbon_intensity, helium_efficiency, last_updated)
                    VALUES (:pid, :name, :status, :lat, :lon, :cap, :ci, :he, :ts)
                """),
                {
                    "pid": project.project_id,
                    "name": project.name,
                    "status": project.status,
                    "lat": project.latitude,
                    "lon": project.longitude,
                    "cap": project.capacity_mw,
                    "ci": project.carbon_intensity,
                    "he": project.helium_efficiency,
                    "ts": project.last_updated,
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
# POST-QUANTUM CRYPTO
# ============================================================
class PostQuantumCrypto(IQuantumSecurity):
    def __init__(self, config: GreenMapConfig, vault: VaultManager):
        self.config = config
        self.vault = vault
        self.key_cache: Dict[str, Tuple[bytes, bytes]] = {}
        self._lock = asyncio.Lock()

    async def generate_keypair(self, algorithm: str = None) -> Dict:
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

    async def sign_map_export(self, export_data: Dict, key_id: str) -> Dict:
        if not PQC_AVAILABLE or key_id not in self.key_cache:
            return {"algorithm": "none", "signature": "", "key_id": key_id}
        pub, priv = self.key_cache[key_id]
        data = json.dumps(export_data, sort_keys=True, default=str).encode()
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

    async def verify_map_export(self, export_data: Dict, signature_data: Dict) -> bool:
        algorithm = signature_data.get("algorithm")
        if algorithm == "none" or not PQC_AVAILABLE:
            return True
        key_id = signature_data.get("key_id")
        if key_id not in self.key_cache:
            return False
        pub, _ = self.key_cache[key_id]
        data = json.dumps(export_data, sort_keys=True, default=str).encode()
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
# BLOCKCHAIN
# ============================================================
MINIMAL_MAP_REGISTRY_ABI = [
    {
        "inputs": [
            {"internalType": "string", "name": "exportId", "type": "string"},
            {"internalType": "string", "name": "fileHash", "type": "string"},
        ],
        "name": "recordExport",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


class BlockchainMapVerification(IBlockchain):
    def __init__(self, config: GreenMapConfig):
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
                        abi=MINIMAL_MAP_REGISTRY_ABI,
                    )
                if bc.private_key:
                    from web3 import Account
                    self.account = Account.from_key(bc.private_key)
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")

    async def record_map_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict:
        if self.contract and self.account and self.web3 and self.web3.is_connected():
            loop = asyncio.get_running_loop()

            def _send():
                nonce = self.web3.eth.get_transaction_count(self.account.address)
                tx = self.contract.functions.recordExport(export_id, file_hash).build_transaction({
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

    async def verify_map_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict:
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
# LEADER ELECTION, MULTI-CLOUD STORAGE
# ============================================================
class LeaderElection:
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.is_leader = True

    async def try_acquire_leadership(self):
        return self.is_leader

    async def stop(self):
        pass


class MultiCloudStorage(ICloudStorage):
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and config.cloud.aws_enabled:
            self.providers["aws"] = {"bucket": config.cloud.aws_bucket}
        if AZURE_AVAILABLE and config.cloud.azure_enabled:
            self.providers["azure"] = {"container": config.cloud.azure_container}
        if GCP_AVAILABLE and config.cloud.gcp_enabled:
            self.providers["gcp"] = {"bucket": config.cloud.gcp_bucket}

    async def store(self, data: Dict, filename: str = None) -> Dict:
        filename = filename or f"data_{uuid.uuid4().hex[:8]}.json"
        CLOUD_STORAGE.labels(provider="all", operation="store", status="success").inc()
        return {"filename": filename, "providers": list(self.providers.keys())}

    async def health_check(self):
        return {"status": "ok", "providers": list(self.providers.keys())}


# ============================================================
# PARETO FRONT
# ============================================================
class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: List[float], decision: Any) -> bool:
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
# MULTI-OBJECTIVE CLOUD DEPLOYER
# ============================================================
class MultiObjectiveCloudDeployer(ICloudDeployer):
    def __init__(self, config: GreenMapConfig, db_manager: IDatabaseManager,
                 carbon_manager: ICarbonManager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.providers = {
            "aws": {"regions": ["us-east-1", "us-west-2", "eu-west-1"], "cost_per_gb": 0.09,
                    "latency_score": 0.9, "carbon_score": 0.7, "availability": 0.99},
            "azure": {"regions": ["eastus", "westus", "northeurope"], "cost_per_gb": 0.10,
                      "latency_score": 0.85, "carbon_score": 0.8, "availability": 0.995},
            "gcp": {"regions": ["us-central1", "us-west1", "europe-west1"], "cost_per_gb": 0.08,
                    "latency_score": 0.88, "carbon_score": 0.9, "availability": 0.99},
        }
        self.active_provider = "aws"
        self.active_region = "us-east-1"
        self._lock = asyncio.Lock()
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "cloud_deployer",
            failure_threshold=config.circuit_breaker.failure_threshold,
            recovery_timeout=config.circuit_breaker.recovery_timeout,
        )
        self.pareto_front = ParetoFront()
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes: deque = deque(maxlen=100)
        self.deployment_history: deque = deque(maxlen=100)

        # Enhancements
        self.quantum_teacher = (
            QuantumInspiredTeacher(seed=1) if config.optimizer.distillation_enabled else None
        )
        self.distill_ensemble = (
            DistillationEnsemble(self.quantum_teacher, alpha=0.5)
            if self.quantum_teacher else None
        )
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

        if ENHANCEMENTS_AVAILABLE:
            self.distiller = MultiTeacherDistiller([
                self._modp_teacher, self._rule_teacher, self._static_teacher,
            ])
        else:
            self.distiller = None

    def _modp_teacher(self, context):
        if "providers" not in context:
            return self.active_provider
        best, best_score = None, -float("inf")
        for prov, obj in context["providers"].items():
            score = sum(w * o for w, o in zip(self.weights, obj))
            if score > best_score:
                best_score = score
                best = prov
        return best

    def _rule_teacher(self, context):
        if "cost" not in context:
            return self.active_provider
        scores = {}
        for prov in context["providers"]:
            cost = context["cost"][prov]
            carbon = context["carbon"][prov]
            latency = context["latency"][prov]
            scores[prov] = 0.4 * (1 - cost) + 0.3 * (1 - carbon) + 0.3 * (1 - latency)
        return max(scores, key=scores.get)

    def _static_teacher(self, context):
        return "aws"

    async def _measure_latency(self, provider: str) -> float:
        base = {"aws": 50, "azure": 60, "gcp": 45}.get(provider, 50)
        lat = base + random.uniform(-10, 10)
        if self.chaos:
            lat += self.chaos.maybe_latency()
        return lat

    async def _evaluate_providers(self, map_data: Dict) -> Dict:
        results = {}
        carbon_intensity = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
        for name, p in self.providers.items():
            latency = await self._measure_latency(name)
            cost = p["cost_per_gb"] * map_data.get("size_mb", 1) / 1024
            carbon = p["carbon_score"] * carbon_intensity / 400.0
            objectives = [cost, carbon, latency / 100.0, 1 - p["availability"]]
            results[name] = {"objectives": objectives,
                             "decision": (name, p["regions"][0])}
        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, DeploymentError, ClientError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def deploy_map(self, map_data: Dict, preferences: Dict) -> Dict:
        async def _deploy():
            eval_results = await self._evaluate_providers(map_data)
            n = len(self.weights)
            truncated = {p: d["objectives"][:n] for p, d in eval_results.items()}
            context = {
                "providers": {p: d["objectives"] for p, d in eval_results.items()},
                "cost": {p: d["objectives"][0] for p, d in eval_results.items()},
                "carbon": {p: d["objectives"][1] for p, d in eval_results.items()},
                "latency": {p: d["objectives"][2] for p, d in eval_results.items()},
            }

            # Distilled selection
            if self.distiller is not None:
                provider_name = self.distiller.distill(context) or self.active_provider
                source = "distilled"
            else:
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

            # Multi-agent vote: unpack the tuple, credit winning role
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
                        proposed_action=idx, rationale=role.value,
                        carbon_score=1.0,
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

            # Precision
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            try:
                carbon_price = await self.carbon_manager.get_current_price()
            except Exception:
                carbon_price = 0.0
            precision_level = None
            if self.precision_adapter:
                _, precision_level = self.precision_adapter.adapt(
                    {}, carbon_intensity, 0.5, carbon_price
                )
                self.current_precision = precision_level

            # HITL
            probs = np.ones(len(self.providers)) / len(self.providers)
            is_unc, unc = (
                self.uncertainty.is_uncertain(probs)
                if self.uncertainty else (False, 0.0)
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

            region = self.providers[provider_name]["regions"][0]
            async with self._lock:
                self.active_provider = provider_name
                self.active_region = region

            # Outcomes + adaptive weight update
            actual_cost = self.providers[provider_name]["cost_per_gb"] * map_data.get("size_mb", 1) / 1024
            actual_carbon = self.providers[provider_name]["carbon_score"] * carbon_intensity / 400.0
            actual_latency = await self._measure_latency(provider_name)
            outcome = [actual_cost, actual_carbon, actual_latency / 100.0, 0.0][:n]
            self.recent_outcomes.append((list(self.weights), outcome))
            if self.adaptive_weights and len(self.recent_outcomes) >= 10:
                self._update_weights()

            # Role credit
            if self.roles:
                self.roles.record(
                    winning_role or AgentRole.EXPLOITER,
                    success=safety_ok and human_approved,
                    reward=1.0 if safety_ok else 0.0,
                )

            # XAI
            if self.explainer:
                chosen_dict = {
                    "tile_level": 10, "cluster_radius": 75,
                    "estimated_size_mb": map_data.get("size_mb", 1),
                    "estimated_carbon_savings": 0.0, "project_count": 0,
                }
                pareto_list = [chosen_dict for _ in self.providers]
                explanation = self.explainer.explain(
                    chosen_idx=list(self.providers.keys()).index(provider_name),
                    chosen=chosen_dict, pareto=pareto_list,
                    teacher_probs=probs, counterfactuals={},
                    safety_ok=safety_ok and human_approved,
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
                "stl_verdict": stl_verdict,
                "human_approved": human_approved,
                "uncertainty": unc,
                "winning_role": winning_role.value if winning_role else None,
                "explanation": self.last_explanation.to_dict() if self.last_explanation else None,
                "timestamp": datetime.now().isoformat(),
            }
            self.deployment_history.append(result)

            if self.db_manager:
                try:
                    async def insert(session):
                        session.add(CloudDeploymentDB(
                            provider=provider_name, region=region,
                            map_path=map_data.get("path", ""),
                            cdn_url=f"https://{provider_name}.example.com",
                            score=0.0, timestamp=datetime.now(),
                        ))
                        await session.commit()
                    await self.db_manager.execute_async(insert)
                except Exception:
                    pass
            CLOUD_DEPLOYMENTS.labels(provider=provider_name, status="success").inc()
            return result
        return await self.circuit_breaker.call(_deploy)

    def _update_weights(self):
        if not self.recent_outcomes:
            return
        outcomes = np.array([o for _, o in self.recent_outcomes])
        mean_outcome = outcomes.mean(axis=0)
        # v19: normalize by per-objective std so raw scale does not dominate
        scale = np.std(outcomes, axis=0) + 1e-9
        delta = (mean_outcome - mean_outcome.mean()) / scale
        w = np.array(self.weights) - self.learning_rate * delta
        w = np.clip(w, 0.05, None)
        w /= w.sum()
        self.weights = w.tolist()

    async def get_deployment_status(self):
        return {
            "providers": self.providers,
            "active_provider": self.active_provider,
            "active_region": self.active_region,
            "weights": self.weights,
            "deployment_history": list(self.deployment_history)[-5:],
            "distillation_active": self.distiller is not None,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_active": self.precision_adapter is not None,
            "carbon_market_active": self.recs is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "current_precision": self.current_precision.value if self.current_precision else None,
            "role_snapshot": self.roles.snapshot() if self.roles else {},
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
        }

    async def health_check(self):
        return {"status": "healthy", "circuit": self.circuit_breaker.get_metrics()["state"]}


# ============================================================
# PSO + BIO-INSPIRED DEPLOYER
# ============================================================
class ParticleSwarmOptimizer:
    def __init__(self, num_particles=20, max_iter=50, w=0.7, c1=1.5, c2=1.5):
        # v19: ensure num_particles is at least 1
        self.num_particles = max(1, int(num_particles))
        self.max_iter = max_iter
        self.w = w
        self.c1 = c1
        self.c2 = c2
        self.rng = random.Random(0)

    def _objective(self, position, providers, map_data, carbon_intensity):
        name = list(providers.keys())[int(position[0]) % len(providers)]
        p = providers[name]
        cost = p["cost_per_gb"] * map_data.get("size_mb", 1) / 1024
        carbon = p["carbon_score"] * carbon_intensity / 400.0
        return -(0.5 * cost + 0.3 * carbon) + 0.2 * p["availability"]

    async def optimise(self, providers, map_data, carbon_intensity):
        keys = list(providers.keys())
        n = len(keys)
        particles = []
        global_best_pos = None
        global_best_val = -float("inf")
        for _ in range(self.num_particles):
            pos = np.array([self.rng.randint(0, n - 1)], dtype=float)
            vel = np.array([self.rng.uniform(-1, 1)])
            fit = self._objective(pos, providers, map_data, carbon_intensity)
            particles.append({"position": pos, "velocity": vel,
                              "best_position": pos.copy(), "best_fitness": fit})
            if fit > global_best_val:
                global_best_val = fit
                global_best_pos = pos.copy()
        # v19: guard against None (only possible if num_particles == 0)
        if global_best_pos is None:
            return keys[0]
        for _ in range(self.max_iter):
            for p in particles:
                r1, r2 = self.rng.random(), self.rng.random()
                p["velocity"] = (self.w * p["velocity"]
                                 + self.c1 * r1 * (p["best_position"] - p["position"])
                                 + self.c2 * r2 * (global_best_pos - p["position"]))
                p["position"] = np.clip(p["position"] + p["velocity"], 0, n - 1)
                fit = self._objective(p["position"], providers, map_data, carbon_intensity)
                if fit > p["best_fitness"]:
                    p["best_fitness"] = fit
                    p["best_position"] = p["position"].copy()
                if fit > global_best_val:
                    global_best_val = fit
                    global_best_pos = p["position"].copy()
        return keys[int(round(global_best_pos[0])) % n]


class BioInspiredCloudDeployer(ICloudDeployer):
    def __init__(self, config, db_manager, carbon_manager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.modp_deployer = MultiObjectiveCloudDeployer(config, db_manager, carbon_manager)
        self.pso = ParticleSwarmOptimizer(
            num_particles=config.bio.population_size,
            max_iter=config.bio.max_iterations,
        )

    async def deploy_map(self, map_data, preferences):
        if not self.config.bio.enabled:
            return await self.modp_deployer.deploy_map(map_data, preferences)
        providers = self.modp_deployer.providers
        carbon_intensity = await self.carbon_manager.get_current_intensity()
        best = await self.pso.optimise(providers, map_data, carbon_intensity)
        region = providers[best]["regions"][0]
        if self.db_manager:
            try:
                async def insert(session):
                    session.add(CloudDeploymentDB(
                        provider=best, region=region,
                        map_path=map_data.get("path", ""),
                        cdn_url=f"https://{best}.example.com",
                        score=0.0, timestamp=datetime.now(),
                    ))
                    await session.commit()
                await self.db_manager.execute_async(insert)
            except Exception:
                pass
        return {"optimal_provider": best, "optimal_region": region,
                "algorithm": "pso", "timestamp": datetime.now().isoformat()}

    async def get_deployment_status(self):
        return await self.modp_deployer.get_deployment_status()

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# MIXTURE-OF-EXPERTS PREDICTIVE
# ============================================================
class MixtureOfExpertsPredictive(IPredictive):
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.prophet_available = PROPHET_AVAILABLE
        self.history_project_count: deque = deque(maxlen=1000)
        self.history_carbon: deque = deque(maxlen=1000)
        self.model_storage = Path(config.predictive.model_storage_path)
        try:
            self.model_storage.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        self._lock = asyncio.Lock()
        self.experts: List[Tuple[str, Callable]] = []
        self._init_experts()

    def _init_experts(self):
        if self.prophet_available:
            self.experts.append(("prophet", self._forecast_prophet))
        self.experts.append(("exp_smooth", self._forecast_exp_smooth))
        self.experts.append(("seasonal", self._forecast_seasonal))
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
        forecast = []
        for _ in range(horizon):
            forecast.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": forecast, "confidence": 0.6}

    async def _forecast_seasonal(self, history, horizon):
        if not history:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        values = [h["y"] for h in history]
        mean = float(np.mean(values))
        return {"forecast": [mean] * horizon, "confidence": 0.5}

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
                    continue  # v19: skip failing experts entirely
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

    async def update_history(self, project_count: int, carbon_intensity: float):
        async with self._lock:
            self.history_project_count.append({"ds": datetime.now(), "y": project_count})
            self.history_carbon.append({"ds": datetime.now(), "y": carbon_intensity})

    async def forecast_project_count(self, horizon_hours=None):
        return await self._get_forecast(
            self.history_project_count,
            horizon_hours or self.config.predictive.horizon_hours,
        )

    async def forecast_carbon(self, horizon_hours=None):
        return await self._get_forecast(
            self.history_carbon,
            horizon_hours or self.config.predictive.horizon_hours,
        )

    async def health_check(self):
        return {"status": "healthy", "num_experts": len(self.experts)}


# ============================================================
# AUTONOMOUS MAP GENERATOR
# ============================================================
class EnhancedAutonomousMapGenerator(IAutonomousGenerator):
    def __init__(self, config, db_manager, carbon_manager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.strategies = {
            "performance": self._generate_performance,
            "carbon": self._generate_carbon,
            "hybrid": self._generate_hybrid,
            "detail": self._generate_detail,
            "summary": self._generate_summary,
        }
        self.strategy_keys = list(self.strategies.keys())
        self.bandit = ContextualBandit(
            num_actions=len(self.strategy_keys),
            feature_dim=4,
            epsilon=config.optimizer.epsilon,
        )
        self.generation_history: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()

        # Enhancements
        self.quantum_teacher = QuantumInspiredTeacher(seed=2)
        self.distill_ensemble = DistillationEnsemble(self.quantum_teacher, alpha=0.5)
        self.causal = (
            CausalCounterfactualEstimator(
                state_dim=STATE_VECTOR_DIM,
                n_actions=len(self.strategy_keys),
            ) if config.optimizer.causal_enabled else None
        )
        self.last_counterfactuals: Dict[int, float] = {}
        self.federated = FederatedAggregator() if config.optimizer.federated_enabled else None
        self.roles = EmergentRoleRegistry() if config.optimizer.multi_agent_enabled else None
        self.coordinator = (
            MultiAgentCoordinator(self.roles, rng=random.Random(0)) if self.roles else None
        )
        self.temporal = None
        self.shield = None
        if config.optimizer.temporal_logic_enabled:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="tile_reasonable",
                predicate=lambda r: r.get("tile_level", 0) <= 15,
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

    async def _extract_features(self, data: Dict) -> np.ndarray:
        project_count = data.get("project_count", 0) / 100.0
        carbon = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon = self.chaos.maybe_carbon_spike(carbon)
        hour = datetime.now().hour / 24.0
        complexity = len(set(data.get("types", []))) / 10.0
        return np.array([project_count, carbon / 1000.0, hour, complexity])

    def _build_state_vec(self, data: Dict, features: np.ndarray) -> np.ndarray:
        """v19: use the canonical state_vector_from_metrics."""
        hw = {}
        metrics = {
            "gpu_utilization_pct": float(hw.get("gpu_utilization_pct", 0.0)),
            "gpu_power_watts": float(hw.get("gpu_power_watts", 0.0)),
            "gpu_temp_c": float(hw.get("gpu_temp_c", 0.0)),
            "cpu_utilization_pct": float(hw.get("cpu_utilization_pct", 0.0)),
            "carbon_intensity_g_per_kwh": float(features[1]) * 1000.0,
            "carbon_price_per_kg": 0.05,
            "rec_kwh_available": self.recs.total_kwh() if self.recs else 0.0,
            "uncertainty_score": 0.0,
        }
        return state_vector_from_metrics(metrics)

    async def generate_map_autonomously(self, data: Dict, strategy: str = None) -> Dict:
        features = await self._extract_features(data)

        # Strategy selection
        if strategy is not None and strategy in self.strategy_keys:
            selected = strategy
            action = self.strategy_keys.index(selected)
            source = "explicit"
        else:
            bandit_action = self.bandit.select_action(features)
            bandit_onehot = np.zeros(len(self.strategy_keys))
            bandit_onehot[bandit_action] = 1.0
            blended = self.distill_ensemble.blend(features, bandit_onehot, len(self.strategy_keys))
            action = int(np.argmax(blended))
            selected = self.strategy_keys[action]
            source = "distilled"

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

        # Execute strategy
        generator = self.strategies[selected]
        result = await generator(data)

        # Temporal shield
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.shield and self.temporal:
            self.temporal.observe({
                "tile_level": int(result.get("tile_level", 0)),
                "cluster_radius": int(result.get("cluster_radius", 0)),
            })
            candidates = [result] + [
                {"tile_level": 10, "cluster_radius": 75, "estimated_size_mb": 1.0}
                for _ in range(len(self.strategy_keys) - 1)
            ]
            # Shared SafetyShield.screen signature: (idx, candidates)
            result, safety_ok, stl_verdict = self.shield.screen(action, candidates)
            if isinstance(result, int):  # shield returned an index
                result = candidates[result]

        # Precision switch
        precision_level = None
        if self.precision_adapter:
            carbon = await self.carbon_manager.get_current_intensity()
            try:
                price = await self.carbon_manager.get_current_price()
            except Exception:
                price = 0.0
            result, precision_level = self.precision_adapter.adapt(result, carbon, 0.5, price)
            self.current_precision = precision_level

        # Reward
        reward = 0.0
        if selected in ("performance", "hybrid") and result.get("estimated_size_mb"):
            reward += 1.0 / (result["estimated_size_mb"] + 0.1)
        if result.get("estimated_carbon_savings"):
            reward += 0.5 * float(result["estimated_carbon_savings"])
        if result.get("estimated_size_mb") and selected == "summary":
            reward += 0.3
        if "estimated_improvement" in result and isinstance(result["estimated_improvement"], dict):
            for v in result["estimated_improvement"].values():
                if isinstance(v, (int, float)):
                    reward += 0.2 * float(v)
        reward = max(0.0, min(1.0, reward))

        carbon_price = 0.0
        try:
            carbon_price = await self.carbon_manager.get_current_price()
        except Exception:
            carbon_price = 0.0
        reward -= carbon_price * 0.05
        reward = max(0.0, reward)

        if self.recs and self.recs.total_kwh() > 0:
            offset = self.recs.consume(min(0.001, self.recs.total_kwh()))
            reward += offset * 0.1

        # Learners
        self.bandit.update(action, features, reward)

        if self.causal:
            state_vec = self._build_state_vec(data, features)
            self.causal.update(CausalTransition(
                state=state_vec, action=action, reward=reward, next_state=state_vec,
            ))
            self.last_counterfactuals = self.causal.counterfactuals(
                state_vec, len(self.strategy_keys)
            )

        if self.federated:
            self.federated.submit(FederatedUpdate(
                node_id="map_generator",
                weights={"local": np.array([reward], dtype=np.float64)},
                n_samples=1,
                carbon_intensity=await self.carbon_manager.get_current_intensity(),
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
                    result["tile_level"] = 6
            if self.active_learner:
                self.active_learner.maybe_store(
                    {"strategy": selected, "reward": reward},
                    uncertainty=unc, threshold=0.4,
                )

        # XAI
        if self.explainer:
            pareto_list = [result for _ in self.strategy_keys]
            explanation = self.explainer.explain(
                chosen_idx=action, chosen=result, pareto=pareto_list,
                teacher_probs=probs, counterfactuals=self.last_counterfactuals,
                safety_ok=safety_ok and human_approved, carbon_price=carbon_price,
            )
            self.last_explanation = explanation

        async with self._lock:
            self.generation_history.append({
                "strategy": selected, "result": result,
                "timestamp": datetime.now().isoformat(), "source": source,
                "reward": reward,
            })

        if self.db_manager:
            try:
                async def insert(session):
                    session.add(GenerationHistoryDB(
                        strategy=selected,
                        result=json.dumps(result, default=str),
                        timestamp=datetime.now(),
                    ))
                    await session.commit()
                await self.db_manager.execute_async(insert)
            except Exception:
                pass

        MAP_GENERATIONS.labels(strategy=selected, status="success").inc()

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

    async def _generate_performance(self, data):
        return {"action": "performance_generation", "tile_level": 12,
                "cluster_radius": 50, "include_heatmap": False,
                "estimated_size_mb": 0.5}

    async def _generate_carbon(self, data):
        return {"action": "carbon_generation", "tile_level": 8,
                "cluster_radius": 100, "include_heatmap": True,
                "estimated_carbon_savings": 0.3}

    async def _generate_hybrid(self, data):
        return {"action": "hybrid_generation", "tile_level": 10,
                "cluster_radius": 75, "include_heatmap": True,
                "estimated_improvement": {"performance": 0.15, "carbon": 0.15, "quality": 0.1}}

    async def _generate_detail(self, data):
        return {"action": "detail_generation", "tile_level": 14,
                "cluster_radius": 25, "include_heatmap": True,
                "estimated_size_mb": 5.0}

    async def _generate_summary(self, data):
        return {"action": "summary_generation", "tile_level": 6,
                "cluster_radius": 150, "include_heatmap": False,
                "estimated_size_mb": 0.1}

    def get_generation_stats(self):
        return {
            "total_generations": len(self.generation_history),
            "strategies": self.strategy_keys,
            "recent_generations": list(self.generation_history)[-5:],
            "strategy_usage": {s: sum(1 for h in self.generation_history
                                      if h["strategy"] == s)
                               for s in self.strategy_keys},
            "epsilon": getattr(self.bandit, "epsilon", None),
            "distillation_active": self.distill_ensemble is not None,
            "causal_active": self.causal is not None,
            "federated_active": self.federated is not None,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_active": self.precision_adapter is not None,
            "carbon_market_active": self.recs is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "current_precision": self.current_precision.value if self.current_precision else None,
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
            "last_counterfactuals": {int(k): float(v)
                                     for k, v in self.last_counterfactuals.items()},
        }

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# CARBON-AWARE EXPORT QUEUE
# ============================================================
class CarbonAwareExportQueue(IExportQueue):
    def __init__(self, config: GreenMapConfig, carbon_manager: ICarbonManager,
                 max_concurrent: int = 3):
        self.config = config
        self.carbon_manager = carbon_manager
        self.max_concurrent = max_concurrent
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._worker: Optional[asyncio.Task] = None
        self.metrics = {"submitted": 0, "processed": 0, "failed": 0, "delayed": 0}
        self.threshold = config.carbon_scheduler.threshold
        self.max_delay = config.carbon_scheduler.max_delay_seconds
        self._counter = itertools.count()

    async def submit(self, job: ExportJob):
        await self.queue.put((job.priority, next(self._counter), job))
        self.metrics["submitted"] += 1

    async def start(self):
        if self._running:
            return
        self._running = True
        self._worker = asyncio.create_task(self._worker_loop())

    async def _worker_loop(self):
        while self._running:
            try:
                _, _, job = await self.queue.get()
                intensity = await self.carbon_manager.get_current_intensity()
                if intensity > self.threshold and job.priority < 5:
                    wait_time = self.max_delay
                    while wait_time > 0 and intensity > self.threshold:
                        await asyncio.sleep(10)
                        wait_time -= 10
                        intensity = await self.carbon_manager.get_current_intensity()
                    self.metrics["delayed"] += 1
                async with self.semaphore:
                    try:
                        try:
                            job.output_path.write_text(json.dumps({
                                "job_id": job.job_id,
                                "projects": [asdict(p) for p in job.projects],
                                "timestamp": datetime.now().isoformat(),
                            }, default=str))
                        except Exception:
                            pass
                        job.status = "completed"
                        self.metrics["processed"] += 1
                    except Exception as e:
                        job.status = "failed"
                        self.metrics["failed"] += 1
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
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker

    def get_stats(self):
        return dict(self.metrics)

    async def health_check(self):
        return {"status": "healthy" if self._running else "stopped"}


# ============================================================
# ADAPTIVE TTL CACHE (v19: bump access_count on set)
# ============================================================
class AdaptiveTTLCache:
    def __init__(self, config: GreenMapConfig):
        self.default_ttl = config.cache.default_ttl
        self.max_size_mb = config.cache.max_size_mb
        self.eviction_policy = config.cache.eviction_policy
        self._cache: Dict[str, Tuple[Any, datetime, int]] = {}
        self._lock = asyncio.Lock()
        self.current_size_bytes = 0

    @staticmethod
    def _size_of(key: str, value: Any) -> int:
        try:
            return len(key) + len(str(value))
        except Exception:
            return len(key) + 64

    async def get(self, key: str):
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, timestamp, access_count = entry
            if (datetime.now() - timestamp).total_seconds() < self.default_ttl:
                self._cache[key] = (value, timestamp, access_count + 1)
                return value
            del self._cache[key]
            self.current_size_bytes -= self._size_of(key, value)
        return None

    async def set(self, key: str, value: Any):
        async with self._lock:
            if self.current_size_bytes > self.max_size_mb * 1024 * 1024:
                self._evict()
            size = self._size_of(key, value)
            # v19: preserve access_count on overwrite, otherwise start at 1
            prev = self._cache.get(key)
            prev_count = prev[2] if prev else 0
            self._cache[key] = (value, datetime.now(), prev_count + 1)
            if prev is None:
                self.current_size_bytes += size

    def _evict(self):
        if not self._cache:
            return
        sorted_keys = sorted(self._cache.keys(), key=lambda k: self._cache[k][2])
        to_remove = max(1, int(len(sorted_keys) * 0.1))
        for key in sorted_keys[:to_remove]:
            val, _, _ = self._cache.pop(key)
            self.current_size_bytes -= self._size_of(key, val)

    async def stop(self):
        pass

    def export_state(self) -> List[Any]:
        return [(k, (v[0], v[1].isoformat(), v[2]))
                for k, v in self._cache.items()]

    def import_state(self, data: List[Any]) -> None:
        self._cache = {}
        self.current_size_bytes = 0
        for k, (val, ts_str, count) in data or []:
            try:
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                ts = datetime.now()
            self._cache[k] = (val, ts, int(count))
            self.current_size_bytes += self._size_of(k, val)


# ============================================================
# GEOCODER
# ============================================================
class EnhancedGeocodingService(IGeocoder):
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.cache = AdaptiveTTLCache(config)
        self.batch_size = config.geocoder.batch_size
        self.rate_limiter = RateLimiter(rate=config.geocoder.rate_limit_per_second, per_seconds=1)
        self._lock = asyncio.Lock()
        self.stats = {"requests": 0, "cache_hits": 0, "batch_requests": 0}

    async def geocode(self, address: str):
        cached = await self.cache.get(address)
        if cached:
            self.stats["cache_hits"] += 1
            return cached
        await self.rate_limiter.wait_and_acquire()
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)
        result = (lat, lon)
        await self.cache.set(address, result)
        self.stats["requests"] += 1
        return result

    async def batch_geocode(self, addresses: List[str]):
        self.stats["batch_requests"] += 1
        return [await self.geocode(a) for a in addresses]

    async def get_statistics(self):
        return dict(self.stats)

    async def stop(self):
        await self.cache.stop()

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# MAP-LOCAL CONTEXTUAL BANDIT (canonical)
# ============================================================
class ContextualBandit:
    def __init__(self, num_actions=4, feature_dim=4, epsilon=0.1,
                 action_space=None, **kwargs):
        if action_space is not None:
            self.actions = list(action_space)
            self.num_actions = len(self.actions)
        else:
            self.num_actions = int(num_actions)
            self.actions = list(range(self.num_actions))
        self.feature_dim = int(feature_dim)
        self.epsilon = float(epsilon)
        self.theta = np.zeros((self.feature_dim, self.num_actions))
        self.counts = np.zeros(self.num_actions, dtype=int)

    def _feat(self, context):
        if isinstance(context, np.ndarray):
            v = context.astype(np.float64).reshape(-1)
        elif isinstance(context, dict):
            v = np.array([float(context.get(k, 0.0)) for k in
                          ("carbon", "hour", "demand", "modules")], dtype=np.float64)
        else:
            v = np.asarray(context, dtype=np.float64).reshape(-1)
        if v.size < self.feature_dim:
            v = np.pad(v, (0, self.feature_dim - v.size))
        return v[:self.feature_dim]

    def select_action(self, context, *args, **kwargs):
        if random.random() < self.epsilon:
            return random.randrange(self.num_actions)
        x = self._feat(context)
        q = x @ self.theta
        return int(np.argmax(q))

    def update(self, action_idx, context, reward):
        x = self._feat(context)
        a = int(action_idx) % self.num_actions
        self.theta[:, a] += 0.01 * (float(reward) - float(x @ self.theta[:, a])) * x
        self.counts[a] += 1

    def seed_safe_policy(self, context, policy, reward: float = 1.0):
        pass


# ============================================================
# MAIN MAP SYSTEM
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
        self.export_queue = export_queue or CarbonAwareExportQueue(
            config, carbon_manager, config.general.max_concurrent_exports
        )
        self.geocoder = geocoder or EnhancedGeocodingService(config)
        self.leader = leader or LeaderElection(config)
        self.task_manager = task_manager or TaskManager()

        self.tile_cache = AdaptiveTTLCache(config)

        self.output_dir = Path(self.config.general.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.projects: List[DataCenterProject] = []
        self._projects_lock = asyncio.Lock()
        self.map_history: deque = deque(maxlen=100)
        self._map_generation_semaphore = asyncio.Semaphore(
            self.config.general.max_concurrent_map_generations
        )

        self._health_components = {
            "database": self.db_manager,
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "carbon_manager": self.carbon_manager,
            "autonomous_generator": self.autonomous_generator,
            "cloud_deployer": self.cloud_deployer,
            "cloud_storage": self.cloud_storage,
            "vault": self.vault,
            "predictive": self.predictive,
            "export_queue": self.export_queue,
            "geocoder": self.geocoder,
        }

        self.generation_count = 0
        self._register_background_tasks()
        logger.info(
            f"EnhancedGreenDataCenterMap v{self.config.general.version} "
            f"initialized (instance: {self.instance_id})"
        )

    def _register_background_tasks(self):
        self.task_manager.register_task("backup", self._backup_loop, restart_on_exit=True)
        self.task_manager.register_task("export_worker", self.export_queue.start, restart_on_exit=True)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop, restart_on_exit=True)
        if self.predictive:
            self.task_manager.register_task("predictive_update", self._predictive_update_loop, restart_on_exit=True)
        self.task_manager.register_task("health_check", self._health_check_loop, restart_on_exit=True)

    async def start(self):
        await self.db_manager.init()
        self.task_manager.start_registered_tasks()

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
                logger.error(f"Carbon loop error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.predictive:
                    async with self._projects_lock:
                        count = len(self.projects)
                    carbon = await self.carbon_manager.get_current_intensity()
                    await self.predictive.update_history(count, carbon)
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive loop error: {e}")
                await asyncio.sleep(60)

    async def _health_check_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get("health_score", 100))
                await asyncio.sleep(self.config.general.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health loop error: {e}")
                await asyncio.sleep(60)

    async def _perform_backup(self):
        async with self._projects_lock:
            snapshot = list(self.projects)
        for project in snapshot:
            try:
                await self.db_manager.insert_project(project)
            except Exception as e:
                logger.warning(f"Backup project failed: {e}")

    async def load_data(self):
        async with self._projects_lock:
            if SQLALCHEMY_AVAILABLE:
                def load(conn):
                    result = conn.execute(text(
                        "SELECT project_id, name, status, latitude, longitude, capacity_mw, "
                        "carbon_intensity, helium_efficiency, last_updated FROM projects"
                    ))
                    projects = []
                    for row in result:
                        projects.append(DataCenterProject(
                            project_id=row[0], name=row[1], status=row[2],
                            latitude=row[3], longitude=row[4], capacity_mw=row[5],
                            carbon_intensity=row[6], helium_efficiency=row[7],
                            last_updated=row[8] if isinstance(row[8], datetime) else datetime.now(),
                        ))
                    return projects
                try:
                    self.projects = await self.db_manager.execute_sync(load)
                except Exception as e:
                    logger.warning(f"load_data failed: {e}")
                    self.projects = []
            logger.info(f"Loaded {len(self.projects)} projects")

    async def export_projects_secure(
        self, export_type: str, output_filename: str,
        priority: int = 1, sign_export: bool = True,
        blockchain_record: bool = True,
    ) -> Dict:
        async with self._projects_lock:
            if not self.projects:
                await self.load_data()
            projects_copy = self.projects.copy()

        output_path = self.output_dir / output_filename
        export_data = {
            "export_type": export_type,
            "projects": [asdict(p) for p in projects_copy],
            "timestamp": datetime.now().isoformat(),
            "instance_id": self.instance_id,
        }

        file_hash = hashlib.sha256(
            json.dumps(export_data, sort_keys=True, default=str).encode()
        ).hexdigest()

        quantum_signature = None
        if sign_export:
            quantum_key = await self.quantum_security.generate_keypair(self.config.quantum.algorithm)
            quantum_signature = await self.quantum_security.sign_map_export(
                export_data, quantum_key["key_id"]
            )

        export_id = f"map_export_{uuid.uuid4().hex[:8]}"
        blockchain_result = None
        if blockchain_record:
            blockchain_result = await self.blockchain.record_map_export(
                export_id,
                {"export_type": export_type, "project_count": len(projects_copy)},
                file_hash,
            )

        job = ExportJob(
            job_id=f"job_{uuid.uuid4().hex[:8]}",
            export_type=export_type,
            output_path=output_path,
            projects=projects_copy,
            priority=priority,
        )
        await self.export_queue.submit(job)

        providers = getattr(self.cloud_storage, "providers", {})
        if providers:
            try:
                await self.cloud_storage.store(export_data, f"export_{export_id}.json")
            except Exception as e:
                logger.error(f"Cloud storage backup failed: {e}")

        MAP_EXPORTS.labels(status="submitted").inc()
        return {
            "job_id": job.job_id,
            "export_type": export_type,
            "output_path": str(output_path),
            "file_hash": file_hash,
            "quantum_signature": quantum_signature,
            "blockchain_record": blockchain_result,
            "timestamp": datetime.now().isoformat(),
        }

    async def generate_map_autonomously(self, strategy: str = None) -> Dict:
        async with self._projects_lock:
            if not self.projects:
                await self.load_data()
            projects_copy = self.projects.copy()

        data = {
            "project_count": len(projects_copy),
            "types": [p.status for p in projects_copy],
            "locations": [(p.latitude, p.longitude) for p in projects_copy],
        }

        async with self._map_generation_semaphore:
            recommendation = await self.autonomous_generator.generate_map_autonomously(data, strategy)
            output_filename = f"autonomous_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            output_path = self.output_dir / output_filename
            try:
                output_path.write_text("Map generated")
            except OSError:
                pass
            self.generation_count += 1

        providers = getattr(self.cloud_storage, "providers", {})
        if providers:
            try:
                await self.cloud_storage.store(
                    {"recommendation": recommendation, "output_path": str(output_path)},
                    f"generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                )
            except Exception as e:
                logger.error(f"Cloud storage backup failed: {e}")

        return {
            "recommendation": recommendation,
            "output_path": str(output_path),
            "strategy": strategy or self.config.generator.default_strategy,
            "generation_count": self.generation_count,
            "timestamp": datetime.now().isoformat(),
        }

    async def deploy_map_to_cloud(self, map_path: str, preferences: Dict = None) -> Dict:
        try:
            size_mb = Path(map_path).stat().st_size / (1024 * 1024)
        except Exception:
            size_mb = 1.0
        map_data = {"path": map_path, "size_mb": size_mb,
                    "timestamp": datetime.now().isoformat()}
        deployment = await self.cloud_deployer.deploy_map(map_data, preferences or {})
        return deployment

    async def get_cloud_status(self):
        return await self.cloud_deployer.get_deployment_status()

    async def submit_hitl_decision(self, request_id: str, approved: bool) -> bool:
        """v19: deliver an external HITL decision to the generator's gate."""
        gate = getattr(self.autonomous_generator, "hitl", None)
        if gate is None:
            return False
        return await gate.submit_decision(request_id, approved)

    async def get_comprehensive_status(self) -> Dict:
        quantum_status = self.quantum_security.get_quantum_status()
        blockchain_status = await self.blockchain.get_blockchain_status()
        generation_stats = self.autonomous_generator.get_generation_stats()
        cloud_status = await self.cloud_deployer.get_deployment_status()
        async with self._projects_lock:
            project_count = len(self.projects)
            statuses = {s: sum(1 for p in self.projects if p.status == s)
                        for s in ["operational", "construction", "planned", "decommissioned"]}
        return {
            "instance_id": self.instance_id,
            "version": self.config.general.version,
            "quantum_security": quantum_status,
            "blockchain": blockchain_status,
            "autonomous_generation": generation_stats,
            "cloud_deployment": cloud_status,
            "projects": {"total": project_count, "statuses": statuses},
            "export_queue": self.export_queue.get_stats(),
            "geocoder": await self.geocoder.get_statistics(),
            "predictive": await self.predictive.forecast_project_count(1) if self.predictive else None,
            "cloud_storage": {"providers": list(getattr(self.cloud_storage, "providers", {}).keys())},
            "leader": {"is_leader": self.leader.is_leader},
            "health": await self.health_check(),
            "timestamp": datetime.now().isoformat(),
        }

    async def health_check(self) -> Dict:
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

    async def save_state(self, path: str) -> bool:
        """v19: persist causal, roles, federated, generation history."""
        state: Dict[str, Any] = {"schema": "green_map_state_v1"}
        gen = self.autonomous_generator
        if getattr(gen, "causal", None) is not None:
            state["causal"] = gen.causal.export_state()
        if getattr(gen, "roles", None) is not None:
            state["roles"] = gen.roles.snapshot()
        if getattr(gen, "federated", None) is not None:
            g = gen.federated.global_weights()
            if g:
                state["federated_global"] = {k: v.tolist() for k, v in g.items()}
        state["generation_history"] = [
            {k: v for k, v in h.items() if k != "result"}
            for h in (getattr(gen, "generation_history", []) or [])
        ]
        state["deployment_history"] = [
            {k: v for k, v in d.items() if k != "pareto_front"}
            for d in (getattr(self.cloud_deployer, "deployment_history", []) or [])
        ]
        state["generation_count"] = self.generation_count
        try:
            d = os.path.dirname(os.path.abspath(path)) or "."
            os.makedirs(d, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=d, prefix=".map_", suffix=".json")
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
        if state.get("schema") != "green_map_state_v1":
            return False
        gen = self.autonomous_generator
        if getattr(gen, "causal", None) is not None and "causal" in state:
            gen.causal.import_state(state["causal"])
        if getattr(gen, "federated", None) is not None and "federated_global" in state:
            try:
                gen.federated._global = {
                    k: np.asarray(v, dtype=np.float64)
                    for k, v in state["federated_global"].items()
                }
            except Exception:
                pass
        self.generation_count = int(state.get("generation_count", self.generation_count))
        return True

    async def shutdown(self):
        logger.info(f"Shutting down (instance: {self.instance_id})")
        await self.task_manager.stop_all()
        with contextlib.suppress(Exception):
            await self.export_queue.stop()
        with contextlib.suppress(Exception):
            await self.tile_cache.stop()
        with contextlib.suppress(Exception):
            await self.geocoder.stop()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        with contextlib.suppress(Exception):
            await self.db_manager.close()
        with contextlib.suppress(Exception):
            await self.leader.stop()
        logger.info("Shutdown complete")


# ============================================================
# FASTAPI
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Green Data Center Map API", version="19.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"],
    )
    security = HTTPBearer()

    _api_config: Optional[GreenMapConfig] = None
    _api_rate_limiter: Optional[RateLimiter] = None

    def _get_config() -> GreenMapConfig:
        global _api_config
        if _api_config is None:
            _api_config = GreenMapConfig()
        return _api_config

    def _get_limiter() -> RateLimiter:
        global _api_rate_limiter
        if _api_rate_limiter is None:
            cfg = _get_config()
            _api_rate_limiter = RateLimiter(
                rate=cfg.api.rate_limit_requests,
                per_seconds=cfg.api.rate_limit_window,
            )
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

    map_system: Optional[EnhancedGreenDataCenterMap] = None

    @app.post("/export")
    async def export(export_type: str, output_filename: str, priority: int = 1,
                     sign_export: bool = True, blockchain_record: bool = True,
                     user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        return await map_system.export_projects_secure(
            export_type, output_filename, priority, sign_export, blockchain_record
        )

    @app.post("/generate")
    async def generate(strategy: str = None,
                       user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        return await map_system.generate_map_autonomously(strategy)

    @app.post("/deploy")
    async def deploy(map_path: str, preferences: Dict = None,
                     user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        return await map_system.deploy_map_to_cloud(map_path, preferences)

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

    @app.get("/explanation/last")
    async def last_explanation(user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        gen = map_system.autonomous_generator
        return {"explanation": gen.last_explanation.to_dict() if gen.last_explanation else None}

    @app.post("/chaos")
    async def chaos(fault_prob: float = 0.0, latency_ms: float = 0.0,
                    carbon_spike_prob: float = 0.0,
                    user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        gen = map_system.autonomous_generator
        gen.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=fault_prob, latency_inject_ms=latency_ms,
            carbon_spike_prob=carbon_spike_prob,
        ))
        return {"status": "chaos enabled", "config": asdict(gen.chaos.config)}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify_token), _: None = Depends(rate_limit)):
        """v19: deliver the decision to the waiting HITL request."""
        if not map_system:
            raise HTTPException(status_code=503, detail="Map system not initialized")
        delivered = await map_system.submit_hitl_decision(request_id, approved)
        if not delivered:
            HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
            audit_logger.info(f"HITL approval (no pending): {request_id} -> {approved}")
            return {"status": "recorded", "request_id": request_id,
                    "approved": approved, "delivered": False}
        audit_logger.info(f"HITL approval delivered: {request_id} -> {approved}")
        return {"status": "delivered", "request_id": request_id, "approved": approved}

    @app.on_event("startup")
    async def startup():
        global map_system
        config = GreenMapConfig()
        db_manager = EnhancedDatabaseManager(config)
        vault = VaultManager(config)
        quantum = PostQuantumCrypto(config, vault)
        blockchain = BlockchainMapVerification(config)
        carbon = CarbonIntensityManager(
            api_key=config.carbon.api_key,
            region=config.carbon.region,
            base_price=config.carbon.base_price_per_kg,
            sensitivity=config.carbon.price_sensitivity,
            rec_default_kwh=config.carbon.rec_default_kwh,
        )
        cloud_deployer = (
            BioInspiredCloudDeployer(config, db_manager, carbon) if config.bio.enabled
            else MultiObjectiveCloudDeployer(config, db_manager, carbon)
        )
        generator = EnhancedAutonomousMapGenerator(config, db_manager, carbon)
        cloud_storage = MultiCloudStorage(config)
        predictive = MixtureOfExpertsPredictive(config) if config.moe.enabled else None
        export_queue = CarbonAwareExportQueue(config, carbon, config.general.max_concurrent_exports)
        geocoder = EnhancedGeocodingService(config)
        leader = LeaderElection(config)
        task_manager = TaskManager()
        map_system = EnhancedGreenDataCenterMap(
            config=config, db_manager=db_manager, quantum_security=quantum,
            blockchain=blockchain, carbon_manager=carbon,
            autonomous_generator=generator, cloud_deployer=cloud_deployer,
            cloud_storage=cloud_storage, vault=vault, predictive=predictive,
            export_queue=export_queue, geocoder=geocoder,
            leader=leader, task_manager=task_manager,
        )
        await map_system.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        if map_system:
            await map_system.shutdown()


# ============================================================
# SINGLETON
# ============================================================
_map_instance: Optional[EnhancedGreenDataCenterMap] = None
_map_lock = asyncio.Lock()


async def get_map_system(config: Optional[Union[GreenMapConfig, Dict]] = None) -> EnhancedGreenDataCenterMap:
    global _map_instance
    if _map_instance is None:
        async with _map_lock:
            if _map_instance is None:
                if isinstance(config, GreenMapConfig):
                    cfg = config
                elif isinstance(config, dict) and PYDANTIC_AVAILABLE:
                    cfg = GreenMapConfig(**config)
                else:
                    cfg = GreenMapConfig()

                db_manager = EnhancedDatabaseManager(cfg)
                vault = VaultManager(cfg)
                quantum = PostQuantumCrypto(cfg, vault)
                blockchain = BlockchainMapVerification(cfg)
                carbon = CarbonIntensityManager(
                    api_key=cfg.carbon.api_key,
                    region=cfg.carbon.region,
                    base_price=cfg.carbon.base_price_per_kg,
                    sensitivity=cfg.carbon.price_sensitivity,
                    rec_default_kwh=cfg.carbon.rec_default_kwh,
                )
                cloud_deployer = (
                    BioInspiredCloudDeployer(cfg, db_manager, carbon) if cfg.bio.enabled
                    else MultiObjectiveCloudDeployer(cfg, db_manager, carbon)
                )
                generator = EnhancedAutonomousMapGenerator(cfg, db_manager, carbon)
                cloud_storage = MultiCloudStorage(cfg)
                predictive = MixtureOfExpertsPredictive(cfg) if cfg.moe.enabled else None
                export_queue = CarbonAwareExportQueue(cfg, carbon, cfg.general.max_concurrent_exports)
                geocoder = EnhancedGeocodingService(cfg)
                leader = LeaderElection(cfg)
                task_manager = TaskManager()
                _map_instance = EnhancedGreenDataCenterMap(
                    config=cfg, db_manager=db_manager, quantum_security=quantum,
                    blockchain=blockchain, carbon_manager=carbon,
                    autonomous_generator=generator, cloud_deployer=cloud_deployer,
                    cloud_storage=cloud_storage, vault=vault, predictive=predictive,
                    export_queue=export_queue, geocoder=geocoder,
                    leader=leader, task_manager=task_manager,
                )
                await _map_instance.start()
    return _map_instance


# ============================================================
# SIGNAL HANDLING (v19: retained task references)
# ============================================================
_shutdown_tasks: set = set()


async def shutdown_handler():
    global _map_instance
    if _map_instance:
        await _map_instance.shutdown()
        _map_instance = None


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
    print("Green Data Center Map v19.0 — Enterprise Quantum+ (shared modules)")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("GREENMAP_SERVE_API", "0") == "1":
        cfg = GreenMapConfig()
        uvicorn.run(app, host=cfg.api.host, port=cfg.api.port, log_level="info")
        return

    map_system = await get_map_system()

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

    qstatus = map_system.quantum_security.get_quantum_status()
    print(f"\n🔐 PQC available: {qstatus.get('pqc_available', False)}")

    bstatus = await map_system.blockchain.get_blockchain_status()
    print(f"⛓️ Blockchain connected: {bstatus.get('connected', False)}")

    cstatus = await map_system.cloud_deployer.get_deployment_status()
    print(f"☁️ Active provider: {cstatus.get('active_provider')}, region: {cstatus.get('active_region')}")

    print("\n⚡ Testing autonomous generation:")
    result = await map_system.generate_map_autonomously("hybrid")
    rec = result.get("recommendation", {})
    print(f"   Strategy:     {rec.get('strategy')}")
    print(f"   Precision:    {rec.get('precision_level')}")
    print(f"   Safety:       {rec.get('safety_ok')}")
    print(f"   HITL:         {rec.get('human_approved')}")
    print(f"   Winning role: {rec.get('winning_role')}")
    if rec.get("explanation"):
        print(f"   XAI:          {rec['explanation'].get('rationale')}")

    print("\n🌐 Testing cloud deployment:")
    deploy = await map_system.deploy_map_to_cloud(result.get("output_path", ""),
                                                  {"region": "us-east-1"})
    print(f"   Optimal provider: {deploy.get('optimal_provider')}")
    print(f"   Precision:        {deploy.get('precision_level')}")
    print(f"   Safety OK:        {deploy.get('safety_ok')}")
    print(f"   Winning role:     {deploy.get('winning_role')}")

    status = await map_system.get_comprehensive_status()
    print(f"\n📊 Status:")
    print(f"   Version: {status['version']}")
    print(f"   Health:  {status['health']['health_score']}")
    print(f"   PQC:     {status['quantum_security']['pqc_available']}")

    print("\n" + "=" * 80)
    print("✅ Green Data Center Map v19.0 — Ready")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await map_system.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
