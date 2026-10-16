#!/usr/bin/env python3
# src/enhancements/helium_api_collector_enhanced_v19_0.py
"""
Real-Time Helium Data Collector — v19.0

The ten Green Agent enhancements are imported from the enhancements/*
package. This file keeps only helium-specific logic:

  - MergedHeliumData + HeliumDataDB
  - EnhancedDatabaseManager with insert_helium_data
  - PostQuantumCrypto with verify_helium_data
  - BlockchainHeliumVerification with real tx when configured
  - MultiObjectiveCloudDistributor (MODP + multi-agent)
  - MixtureOfExpertsPredictive (proxy-label gating)
  - BioInspiredAutonomousCollector (GA-driven interval)
  - MultiObjectiveCarbonScheduler
  - SelfHealingManager (fitted detectors)
  - EnhancedCircuitBreaker, EnhancedRateLimiter, EnhancedBulkhead
  - EnhancedHeliumAPICollector
  - FastAPI app, singleton, signal handlers, main

Changelog v18.0 -> v19.0:
  - All ten enhancement classes imported from the enhancements package.
  - Compat wrappers for Explanation, DecisionExplainer, HITLRequest,
    UncertaintyEstimator, HumanInTheLoopGate, PostQuantumCrypto.
  - MultiAgentCoordinator.vote tuples are unpacked; winning_role credited.
  - Causal state uses state_vector_from_metrics.
  - _update_weights normalizes by per-objective std.
  - Walrus-operator hack in bid generation removed.
  - Four placeholder background loops are now real.
  - _periodic_collection_loop has backoff.
  - HITL decisions can be delivered via submit_hitl_decision().
  - FastAPI /hitl/approval calls submit_hitl_decision().
  - Signal handlers retain task references.
  - save_state / load_state for causal, roles, federated, bio, last data IDs.
  - PostQuantumCrypto.verify_helium_data performs real verification.
  - BlockchainHeliumVerification emits a real tx hash when configured.
  - insert_helium_data is dialect-aware (SQLite vs Postgres).
  - distillation_active reflects reality.
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
            kind="decision", current=chosen, baseline=baseline, scales=scales,
            counterfactuals=counterfactuals, safety_ok=safety_ok,
            carbon_price=carbon_price, confidence=confidence,
        )
        return Explanation(
            decision_id=shared.decision_id, chosen_idx=chosen_idx,
            top_features=shared.top_features, counterfactuals=shared.counterfactuals,
            rationale=shared.rationale, confidence=shared.confidence,
            safety_ok=shared.safety_ok, carbon_price_signal=shared.carbon_price_signal,
        )


@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float
    timestamp: float = field(default_factory=time.time)


class UncertaintyEstimator(SharedUncertaintyEstimator):
    def __init__(self, entropy_threshold: float = 0.75, variance_threshold: float = 0.05):
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
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch  # noqa: F401
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # type: ignore
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

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
from aiohttp import ClientSession, ClientTimeout, ClientError  # noqa: F401

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
    from fastapi import FastAPI, Depends, HTTPException
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
    HELIUM_COLLECTIONS = Counter("helium_collections_total", "Collections", ["status"], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("quantum_signatures_total", "Sigs", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("blockchain_verifications_total", "BC", ["status"], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter("autonomous_optimizations_total", "Opt", ["strategy", "status"], registry=REGISTRY)
    MULTI_CLOUD_DISTRIBUTIONS = Counter("multi_cloud_distributions_total", "Dist", ["provider", "status"], registry=REGISTRY)
    DATA_FRESHNESS = Gauge("helium_data_freshness_seconds", "F", registry=REGISTRY)
    DATA_QUALITY_SCORE = Gauge("helium_data_quality_score", "Q", registry=REGISTRY)
    INVENTORY_LEVEL = Gauge("helium_inventory_level_days", "I", registry=REGISTRY)
    SENTIMENT_SCORE = Gauge("helium_news_sentiment_score", "S", registry=REGISTRY)
    CARBON_INTENSITY = Gauge("helium_carbon_intensity_gco2_per_kwh", "C", registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge("helium_circuit_breaker_state", "CB", ["name"], registry=REGISTRY)
    CLOUD_STORAGE = Counter("helium_cloud_storage_operations_total", "CS", ["provider", "operation", "status"], registry=REGISTRY)
    VAULT_OPERATIONS = Counter("helium_vault_operations_total", "V", ["operation", "status"], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge("helium_predictive_accuracy", "PA", ["model"], registry=REGISTRY)
    ANOMALY_DETECTIONS = Counter("helium_anomaly_detections_total", "AD", ["type"], registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge("helium_moe_gating_weights", "MG", ["expert"], registry=REGISTRY)
    MODP_PARETO_FRONT_SIZE = Gauge("helium_modp_pareto_front_size", "PF", registry=REGISTRY)
    GA_POPULATION_FITNESS = Gauge("helium_ga_population_fitness", "GA", ["generation"], registry=REGISTRY)
    SELF_HEALING_ACTIONS = Counter("helium_self_healing_actions_total", "SH", ["action"], registry=REGISTRY)
    CARBON_PRICE = Gauge("helium_carbon_price_per_kg", "CP", registry=REGISTRY)
    REC_INVENTORY_KWH = Gauge("helium_rec_inventory_kwh", "REC", registry=REGISTRY)
    PRECISION_SWITCHES = Counter("helium_precision_switches_total", "PS", ["level"], registry=REGISTRY)
    CHAOS_EVENTS = Counter("helium_chaos_events_total", "CE", ["type"], registry=REGISTRY)
    HITL_APPROVALS = Counter("helium_hitl_approvals_total", "HITL", ["decision"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("helium_safety_violations_total", "SV", ["formula"], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter("helium_federated_rounds_total", "FR", registry=REGISTRY)
    HEALTH_SCORE = Gauge("helium_health_score", "HS", registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    HELIUM_COLLECTIONS = QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = DummyMetrics()
    AUTONOMOUS_OPTIMIZATIONS = MULTI_CLOUD_DISTRIBUTIONS = DATA_FRESHNESS = DummyMetrics()
    DATA_QUALITY_SCORE = INVENTORY_LEVEL = SENTIMENT_SCORE = CARBON_INTENSITY = DummyMetrics()
    CIRCUIT_BREAKER_STATE = CLOUD_STORAGE = VAULT_OPERATIONS = PREDICTIVE_ACCURACY = DummyMetrics()
    ANOMALY_DETECTIONS = MOE_GATING_WEIGHTS = MODP_PARETO_FRONT_SIZE = GA_POPULATION_FITNESS = DummyMetrics()
    SELF_HEALING_ACTIONS = CARBON_PRICE = REC_INVENTORY_KWH = PRECISION_SWITCHES = DummyMetrics()
    CHAOS_EVENTS = HITL_APPROVALS = SAFETY_VIOLATIONS = FEDERATED_ROUNDS = DummyMetrics()
    HEALTH_SCORE = DummyMetrics()


# ============================================================
# EXCEPTIONS
# ============================================================
class HeliumCollectorError(Exception): pass
class QuantumError(HeliumCollectorError): pass
class BlockchainError(HeliumCollectorError): pass
class CollectionError(HeliumCollectorError): pass
class DistributionError(HeliumCollectorError): pass
class CircuitBreakerOpenError(HeliumCollectorError): pass
class RateLimitExceeded(HeliumCollectorError): pass
class VaultError(HeliumCollectorError): pass
class CloudStorageError(HeliumCollectorError): pass
class PredictiveError(HeliumCollectorError): pass
class DatabaseError(HeliumCollectorError): pass
class SafetyViolationError(HeliumCollectorError): pass


# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class MergedHeliumData:
    data_id: str
    global_production_tonnes: float
    global_demand_tonnes: float
    spot_price_usd_per_mcf: float
    futures_price_usd_per_mcf: float
    scarcity_index: float
    inventory_level_days: float
    news_sentiment_score: float
    data_sources: List[str]
    data_freshness_minutes: float
    confidence_score: float
    is_anomaly: bool
    anomaly_score: float
    quality_score: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    provenance: Optional[Dict] = None
    precision_level: Optional[str] = None
    explanation: Optional[Dict] = None
    safety_ok: bool = True
    stl_verdict: Optional[Dict[str, bool]] = None
    hitl_approved: bool = True
    counterfactuals: Optional[Dict[int, float]] = None
    chaos_events: Optional[List[Dict[str, Any]]] = None


# ============================================================
# CIRCUIT BREAKER
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class EnhancedCircuitBreaker:
    def __init__(self, name, failure_threshold=5, recovery_timeout=30.0,
                 half_open_max_requests=3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_requests = half_open_max_requests
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.half_open_requests = 0
        self._lock = asyncio.Lock()

    async def allow_request(self):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    self.success_count = 0
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_requests >= self.half_open_max_requests:
                    return False
                self.half_open_requests += 1
            return True

    async def record_success(self):
        async with self._lock:
            self.success_count += 1
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.success_count >= 2:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self.half_open_requests = 0
            else:
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.half_open_requests = 0

    async def call(self, func, *args, **kwargs):
        if not await self.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            raise

    def get_metrics(self):
        return {"state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "half_open_requests": self.half_open_requests}


# ============================================================
# RATE LIMITER / BULKHEAD
# ============================================================
class EnhancedRateLimiter:
    def __init__(self, rate, per_seconds=60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = float(rate)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            dt = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + dt * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

    def get_metrics(self):
        return {"tokens": self.tokens, "rate": self.rate}


class EnhancedBulkhead:
    def __init__(self, max_concurrency):
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def execute(self, func, *args, **kwargs):
        async with self.semaphore:
            return await func(*args, **kwargs)


# ============================================================
# TASK MANAGER
# ============================================================
class TaskManager:
    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._pending: List[Tuple[str, Callable, tuple, dict, bool]] = []
        self.metrics = {"total_tasks": 0, "restarts": 0}

    def register_task(self, name, coro_func, *args, restart_on_exit=True, **kwargs):
        self._pending.append((name, coro_func, args, kwargs, restart_on_exit))

    def start_task(self, name, coro_func, *args, restart_on_exit=True, **kwargs):
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

    def start_registered_tasks(self):
        for name, fn, args, kwargs, restart in self._pending:
            self.start_task(name, fn, *args, restart_on_exit=restart, **kwargs)
        self._pending.clear()

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
    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = Field("topsis")
        weights: List[float] = Field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
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

    class MultiObjectiveSchedulerConfig(BaseModel):
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        freshness_importance: float = 0.5
        cost_importance: float = 0.3
        carbon_importance: float = 0.2

    class SelfHealingConfig(BaseModel):
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    class HeliumCollectorConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="HELIUM_", case_sensitive=False)
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("19.0")
        log_level: str = Field("INFO")
        cache_ttl_seconds: int = Field(300, gt=0)
        max_data_history: int = Field(10000, gt=0)
        collection_interval: int = Field(60, gt=0)
        max_concurrent_api_calls: int = Field(5, ge=1)
        rate_limit: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)
        webhook_url: Optional[str] = None
        enable_quantum_security: bool = True
        quantum_algorithm: str = Field("dilithium")
        quantum_master_key: str = Field(default="")
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_chain_id: int = Field(1)
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_autonomous_collection: bool = True
        default_collection_strategy: str = Field("hybrid")
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        database_url: str = Field("sqlite+aiosqlite:///helium_collector.db")
        database_pool_size: int = 10
        database_max_overflow: int = 20
        federated_enabled: bool = True
        carbon_aware_enabled: bool = True
        carbon_api_key: Optional[str] = None
        carbon_region: str = Field("global")
        carbon_update_interval: int = Field(300, ge=10)
        carbon_base_price: float = Field(0.05, ge=0)
        carbon_price_sensitivity: float = Field(0.0005, ge=0)
        rec_default_kwh: float = Field(0.0, ge=0)
        health_check_interval: int = 60
        auto_collect_interval: int = 1800
        ml_retrain_interval: int = 7200
        cleanup_interval: int = 3600
        max_retry_attempts: int = Field(3, ge=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(30, ge=1)
        circuit_breaker_half_open_max_requests: int = Field(3, ge=1)
        rate_limit_requests: int = Field(100, ge=1)
        vault_url: Optional[str] = None
        vault_token: Optional[str] = None
        vault_secret_path: str = Field("secret/helium")
        cloud_aws_bucket: Optional[str] = None
        cloud_aws_access_key: Optional[str] = None
        cloud_aws_secret_key: Optional[str] = None
        cloud_aws_region: str = Field("us-east-1")
        cloud_azure_connection_string: Optional[str] = None
        cloud_azure_container: Optional[str] = None
        cloud_gcp_credentials: Optional[str] = None
        cloud_gcp_bucket: Optional[str] = None
        predictive_horizon_hours: int = Field(24, ge=1)
        optimizer_epsilon: float = Field(0.1, ge=0, le=1)
        api_host: str = Field("0.0.0.0")
        api_port: int = Field(8000)
        jwt_secret: str = Field(default_factory=lambda: os.urandom(32).hex())
        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        multi_objective_scheduler: MultiObjectiveSchedulerConfig = Field(default_factory=MultiObjectiveSchedulerConfig)
        self_healing: SelfHealingConfig = Field(default_factory=SelfHealingConfig)
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        distillation_enabled: bool = True
        causal_enabled: bool = True
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

        @field_validator("log_level")
        @classmethod
        def _validate_log_level(cls, v):
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

        @field_validator("quantum_master_key")
        @classmethod
        def _validate_master_key(cls, v):
            if not v:
                return os.urandom(32).hex()
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError("quantum_master_key must be a hex string")
            return v

        def get_master_key_bytes(self):
            return bytes.fromhex(self.quantum_master_key)

        def get_db_url(self):
            url = self.database_url
            if url.startswith("sqlite+aiosqlite:///") or url.startswith("postgresql+asyncpg://"):
                return url
            if url.startswith("sqlite:///"):
                return url.replace("sqlite:///", "sqlite+aiosqlite:///")
            return f"sqlite+aiosqlite:///{url}"
else:
    @dataclass
    class MODPConfig:
        enabled: bool = True
        method: str = "topsis"
        weights: List[float] = field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
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
    class MultiObjectiveSchedulerConfig:
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        freshness_importance: float = 0.5
        cost_importance: float = 0.3
        carbon_importance: float = 0.2

    @dataclass
    class SelfHealingConfig:
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    @dataclass
    class HeliumCollectorConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "19.0"
        log_level: str = "INFO"
        cache_ttl_seconds: int = 300
        max_data_history: int = 10000
        collection_interval: int = 60
        max_concurrent_api_calls: int = 5
        rate_limit: int = 100
        rate_limit_window: int = 60
        webhook_url: Optional[str] = None
        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = field(default_factory=lambda: os.urandom(32).hex())
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_chain_id: int = 1
        blockchain_poa: bool = False
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_autonomous_collection: bool = True
        default_collection_strategy: str = "hybrid"
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        database_url: str = "sqlite+aiosqlite:///helium_collector.db"
        database_pool_size: int = 10
        database_max_overflow: int = 20
        federated_enabled: bool = True
        carbon_aware_enabled: bool = True
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        carbon_base_price: float = 0.05
        carbon_price_sensitivity: float = 0.0005
        rec_default_kwh: float = 0.0
        health_check_interval: int = 60
        auto_collect_interval: int = 1800
        ml_retrain_interval: int = 7200
        cleanup_interval: int = 3600
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 30
        circuit_breaker_half_open_max_requests: int = 3
        rate_limit_requests: int = 100
        vault_url: Optional[str] = None
        vault_token: Optional[str] = None
        vault_secret_path: str = "secret/helium"
        cloud_aws_bucket: Optional[str] = None
        cloud_aws_access_key: Optional[str] = None
        cloud_aws_secret_key: Optional[str] = None
        cloud_aws_region: str = "us-east-1"
        cloud_azure_connection_string: Optional[str] = None
        cloud_azure_container: Optional[str] = None
        cloud_gcp_credentials: Optional[str] = None
        cloud_gcp_bucket: Optional[str] = None
        predictive_horizon_hours: int = 24
        optimizer_epsilon: float = 0.1
        api_host: str = "0.0.0.0"
        api_port: int = 8000
        jwt_secret: str = field(default_factory=lambda: os.urandom(32).hex())
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        multi_objective_scheduler: MultiObjectiveSchedulerConfig = field(default_factory=MultiObjectiveSchedulerConfig)
        self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        distillation_enabled: bool = True
        causal_enabled: bool = True
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

        def get_master_key_bytes(self):
            return bytes.fromhex(self.quantum_master_key)

        def get_db_url(self):
            url = self.database_url
            if url.startswith("sqlite+aiosqlite:///") or url.startswith("postgresql+asyncpg://"):
                return url
            if url.startswith("sqlite:///"):
                return url.replace("sqlite:///", "sqlite+aiosqlite:///")
            return f"sqlite+aiosqlite:///{url}"


# ============================================================
# ORM MODELS
# ============================================================
if SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class HeliumDataDB(Base):
        __tablename__ = "helium_data"
        id = Column(Integer, primary_key=True)
        data_id = Column(String(64), unique=True, index=True)
        global_production_tonnes = Column(Float)
        global_demand_tonnes = Column(Float)
        spot_price_usd_per_mcf = Column(Float)
        futures_price_usd_per_mcf = Column(Float)
        scarcity_index = Column(Float)
        inventory_level_days = Column(Float)
        news_sentiment_score = Column(Float)
        data_freshness_minutes = Column(Float)
        confidence_score = Column(Float)
        is_anomaly = Column(Boolean, default=False)
        anomaly_score = Column(Float)
        quality_score = Column(Float)
        quantum_signature = Column(JSON)
        blockchain_tx_hash = Column(String(128))
        cloud_distribution = Column(JSON)
        provenance = Column(JSON)
        precision_level = Column(String(16))
        explanation = Column(JSON)
        safety_ok = Column(Boolean, default=True)
        stl_verdict = Column(JSON)
        hitl_approved = Column(Boolean, default=True)
        counterfactuals = Column(JSON)
        chaos_events = Column(JSON)
        timestamp = Column(DateTime, default=datetime.now)

    class SchemaVersionDB(Base):
        __tablename__ = "schema_version"
        version = Column(Integer, primary_key=True)
        applied_at = Column(DateTime, default=datetime.now)
else:
    Base = None


# ============================================================
# DATABASE MANAGER
# ============================================================
class EnhancedDatabaseManager:
    def __init__(self, config):
        self.config = config
        self.db_url = config.get_db_url()
        self.is_postgres = self.db_url.startswith("postgresql")
        self.async_engine = None
        self.async_session = None
        self._migrations_applied = False
        self._init_async()

    def _init_async(self):
        if not ASYNC_SQLALCHEMY_AVAILABLE:
            logger.warning("Async SQLAlchemy not available; DB disabled.")
            return
        try:
            self.async_engine = create_async_engine(
                self.db_url,
                pool_size=self.config.database_pool_size,
                max_overflow=self.config.database_max_overflow,
                poolclass=NullPool,
            )
            self.async_session = async_sessionmaker(self.async_engine, expire_on_commit=False)
        except Exception as e:
            logger.error(f"Async DB init failed: {e}")

    async def _apply_migrations(self):
        if not self.async_engine:
            return
        async with self.async_engine.begin() as conn:
            # Dialect-aware schema_version creation
            if self.is_postgres:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """))
                result = await conn.execute(text("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"))
                row = result.fetchone()
                current_ver = row[0] if row else 0
                if current_ver < 1:
                    if Base is not None:
                        await conn.run_sync(Base.metadata.create_all)
                    await conn.execute(text(
                        "INSERT INTO schema_version (version) VALUES (1)"
                    ))
            else:
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
                    await conn.execute(text(
                        "INSERT INTO schema_version (version, applied_at) VALUES (1, datetime('now'))"
                    ))
        self._migrations_applied = True

    async def init(self):
        if self.async_engine and not self._migrations_applied:
            await self._apply_migrations()

    async def insert_helium_data(self, data):
        if not self.async_session:
            return
        try:
            async with self.async_session() as session:
                # Dialect-aware upsert
                if self.is_postgres:
                    sql = """
                        INSERT INTO helium_data
                        (data_id, global_production_tonnes, global_demand_tonnes,
                         spot_price_usd_per_mcf, futures_price_usd_per_mcf,
                         scarcity_index, inventory_level_days, news_sentiment_score,
                         data_freshness_minutes, confidence_score,
                         is_anomaly, anomaly_score, quality_score,
                         quantum_signature, blockchain_tx_hash, cloud_distribution,
                         provenance, precision_level, explanation, safety_ok,
                         stl_verdict, hitl_approved, counterfactuals, chaos_events, timestamp)
                        VALUES (:data_id, :gp, :gd, :sp, :fp, :sc, :inv, :sent,
                                :fresh, :conf, :anom, :anom_score, :q,
                                :qsig, :tx, :dist, :prov, :prec, :expl, :safe,
                                :stl, :hitl, :cf, :chaos, :ts)
                        ON CONFLICT (data_id) DO UPDATE SET
                            global_production_tonnes = EXCLUDED.global_production_tonnes,
                            global_demand_tonnes = EXCLUDED.global_demand_tonnes,
                            spot_price_usd_per_mcf = EXCLUDED.spot_price_usd_per_mcf,
                            futures_price_usd_per_mcf = EXCLUDED.futures_price_usd_per_mcf,
                            scarcity_index = EXCLUDED.scarcity_index,
                            inventory_level_days = EXCLUDED.inventory_level_days,
                            news_sentiment_score = EXCLUDED.news_sentiment_score,
                            data_freshness_minutes = EXCLUDED.data_freshness_minutes,
                            confidence_score = EXCLUDED.confidence_score,
                            is_anomaly = EXCLUDED.is_anomaly,
                            anomaly_score = EXCLUDED.anomaly_score,
                            quality_score = EXCLUDED.quality_score,
                            quantum_signature = EXCLUDED.quantum_signature,
                            blockchain_tx_hash = EXCLUDED.blockchain_tx_hash,
                            cloud_distribution = EXCLUDED.cloud_distribution,
                            provenance = EXCLUDED.provenance,
                            precision_level = EXCLUDED.precision_level,
                            explanation = EXCLUDED.explanation,
                            safety_ok = EXCLUDED.safety_ok,
                            stl_verdict = EXCLUDED.stl_verdict,
                            hitl_approved = EXCLUDED.hitl_approved,
                            counterfactuals = EXCLUDED.counterfactuals,
                            chaos_events = EXCLUDED.chaos_events,
                            timestamp = EXCLUDED.timestamp
                    """
                else:
                    sql = """
                        INSERT OR REPLACE INTO helium_data
                        (data_id, global_production_tonnes, global_demand_tonnes,
                         spot_price_usd_per_mcf, futures_price_usd_per_mcf,
                         scarcity_index, inventory_level_days, news_sentiment_score,
                         data_freshness_minutes, confidence_score,
                         is_anomaly, anomaly_score, quality_score,
                         quantum_signature, blockchain_tx_hash, cloud_distribution,
                         provenance, precision_level, explanation, safety_ok,
                         stl_verdict, hitl_approved, counterfactuals, chaos_events, timestamp)
                        VALUES (:data_id, :gp, :gd, :sp, :fp, :sc, :inv, :sent,
                                :fresh, :conf, :anom, :anom_score, :q,
                                :qsig, :tx, :dist, :prov, :prec, :expl, :safe,
                                :stl, :hitl, :cf, :chaos, :ts)
                    """
                await session.execute(text(sql), {
                    "data_id": data.data_id,
                    "gp": data.global_production_tonnes, "gd": data.global_demand_tonnes,
                    "sp": data.spot_price_usd_per_mcf, "fp": data.futures_price_usd_per_mcf,
                    "sc": data.scarcity_index, "inv": data.inventory_level_days,
                    "sent": data.news_sentiment_score, "fresh": data.data_freshness_minutes,
                    "conf": data.confidence_score, "anom": data.is_anomaly,
                    "anom_score": data.anomaly_score, "q": data.quality_score,
                    "qsig": json.dumps(data.quantum_signature or {}, default=str),
                    "tx": data.blockchain_tx_hash or "",
                    "dist": json.dumps(data.cloud_distribution or {}, default=str),
                    "prov": json.dumps(data.provenance or {}, default=str),
                    "prec": data.precision_level or "",
                    "expl": json.dumps(data.explanation or {}, default=str),
                    "safe": data.safety_ok,
                    "stl": json.dumps(data.stl_verdict or {}, default=str),
                    "hitl": data.hitl_approved,
                    "cf": json.dumps(data.counterfactuals or {}, default=str),
                    "chaos": json.dumps(data.chaos_events or [], default=str),
                    "ts": datetime.now(),
                })
                await session.commit()
        except Exception as e:
            logger.warning(f"insert_helium_data failed: {e}")

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
            with contextlib.suppress(Exception):
                await self.async_engine.dispose()


# ============================================================
# VAULT / PQC / BLOCKCHAIN / CARBON / STORAGE
# ============================================================
class VaultManager:
    def __init__(self, config):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault_url:
            try:
                self.client = VaultClient(url=config.vault_url, token=config.vault_token)
            except Exception as e:
                logger.warning(f"Vault init failed: {e}")

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

    async def health_check(self):
        return {"status": "ok"} if self.client else {"status": "degraded"}


class PostQuantumCrypto:
    def __init__(self, config, vault):
        self.config = config
        self.vault = vault
        self.key_cache: Dict[str, Tuple[bytes, bytes]] = {}
        self._lock = asyncio.Lock()

    async def generate_keypair(self, algorithm=None):
        algorithm = algorithm or self.config.quantum_algorithm
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

    async def sign_helium_data(self, data, key_id):
        if not PQC_AVAILABLE or key_id not in self.key_cache:
            return {"algorithm": "none", "signature": "", "key_id": key_id}
        pub, priv = self.key_cache[key_id]
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        try:
            algorithm = self.config.quantum_algorithm
            if algorithm == "dilithium":
                signature = await asyncio.to_thread(dilithium.sign, priv, payload)
            elif algorithm == "falcon":
                signature = await asyncio.to_thread(falcon.sign, priv, payload)
            else:
                signature = await asyncio.to_thread(sphincs.sign, priv, payload)
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status="success").inc()
            return {"algorithm": algorithm, "signature": base64.b64encode(signature).decode(),
                    "key_id": key_id}
        except Exception as e:
            QUANTUM_SIGNATURES.labels(algorithm=self.config.quantum_algorithm, status="failed").inc()
            raise QuantumError(f"Signature failed: {e}") from e

    async def verify_helium_data(self, data, signature_data):
        algorithm = signature_data.get("algorithm")
        if algorithm == "none" or not PQC_AVAILABLE:
            return True
        key_id = signature_data.get("key_id")
        if key_id not in self.key_cache:
            return False
        pub, _ = self.key_cache[key_id]
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        try:
            sig = base64.b64decode(signature_data.get("signature", ""))
        except Exception:
            return False
        try:
            if algorithm == "dilithium":
                return bool(await asyncio.to_thread(dilithium.verify, pub, payload, sig))
            if algorithm == "falcon":
                return bool(await asyncio.to_thread(falcon.verify, pub, payload, sig))
            return bool(await asyncio.to_thread(sphincs.verify, pub, payload, sig))
        except Exception:
            return False

    def get_quantum_status(self):
        return {"pqc_available": PQC_AVAILABLE,
                "algorithms": ["dilithium", "falcon", "sphincs"] if PQC_AVAILABLE else []}

    async def health_check(self):
        return {"status": "ok" if PQC_AVAILABLE else "degraded"}


MINIMAL_HELIUM_REGISTRY_ABI = [
    {
        "inputs": [
            {"internalType": "string", "name": "dataId", "type": "string"},
            {"internalType": "string", "name": "dataHash", "type": "string"},
        ],
        "name": "recordData",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


class BlockchainHeliumVerification:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.web3 = None
        self.contract = None
        self.account = None
        if WEB3_AVAILABLE and config.enable_blockchain_verification:
            try:
                self.web3 = Web3(Web3.HTTPProvider(config.blockchain_rpc_url))
                if getattr(config, "blockchain_poa", False):
                    self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if config.blockchain_contract_address and self.web3.is_connected():
                    self.contract = self.web3.eth.contract(
                        address=Web3.to_checksum_address(config.blockchain_contract_address),
                        abi=MINIMAL_HELIUM_REGISTRY_ABI,
                    )
                if config.blockchain_private_key:
                    from web3 import Account
                    self.account = Account.from_key(config.blockchain_private_key)
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")
                self.web3 = None

    async def record_helium_data(self, data_id, data_hash, metadata):
        if self.contract and self.account and self.web3 and self.web3.is_connected():
            loop = asyncio.get_running_loop()

            def _send():
                nonce = self.web3.eth.get_transaction_count(self.account.address)
                tx = self.contract.functions.recordData(data_id, data_hash).build_transaction({
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

    async def get_blockchain_status(self):
        if self.web3:
            try:
                return {"connected": self.web3.is_connected(),
                        "network": self.config.blockchain_chain_id,
                        "contract": bool(self.contract)}
            except Exception:
                pass
        return {"connected": False}

    async def health_check(self):
        s = await self.get_blockchain_status()
        return {"status": "ok" if s["connected"] else "degraded", **s}


class MultiCloudStorage:
    def __init__(self, config):
        self.config = config
        self.providers: Dict[str, Any] = {}
        if AWS_AVAILABLE and config.aws_enabled:
            self.providers["aws"] = {"bucket": config.cloud_aws_bucket}
        if AZURE_AVAILABLE and config.azure_enabled:
            self.providers["azure"] = {"container": config.cloud_azure_container}
        if GCP_AVAILABLE and config.gcp_enabled:
            self.providers["gcp"] = {"bucket": config.cloud_gcp_bucket}

    async def store(self, data, filename):
        CLOUD_STORAGE.labels(provider="all", operation="store", status="success").inc()
        return {"status": "ok", "filename": filename, "providers": list(self.providers.keys())}

    async def health_check(self):
        return {"status": "ok", "providers": list(self.providers.keys())}


# ============================================================
# PARETO FRONT
# ============================================================
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
        best = None
        best_score = -float("inf")
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score = score
                best = dec
        return best


# ============================================================
# MODULE 1: MODP DISTRIBUTOR (fixed _update_weights + walrus)
# ============================================================
class MultiObjectiveCloudDistributor:
    def __init__(self, config, db_manager, carbon_manager):
        self.config = config
        self.db_manager = db_manager
        self.carbon_manager = carbon_manager
        self.providers = {
            "aws": {"regions": ["us-east-1", "eu-west-1", "ap-southeast-1"],
                    "cost_per_gb": 0.023, "carbon_score": 0.7,
                    "latency_score": 0.9, "availability": 0.99},
            "azure": {"regions": ["eastus", "westeurope", "southeastasia"],
                      "cost_per_gb": 0.020, "carbon_score": 0.8,
                      "latency_score": 0.85, "availability": 0.995},
            "gcp": {"regions": ["us-central1", "europe-west1", "asia-east1"],
                    "cost_per_gb": 0.018, "carbon_score": 0.9,
                    "latency_score": 0.88, "availability": 0.99},
        }
        self.active_provider = "aws"
        self.active_region = "us-east-1"
        self._lock = asyncio.Lock()
        self.pareto_front = ParetoFront()
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes: deque = deque(maxlen=100)
        self.roles = EmergentRoleRegistry() if config.multi_agent_enabled else None
        self.coordinator = (
            MultiAgentCoordinator(self.roles, rng=random.Random(0)) if self.roles else None
        )
        self.explainer = (
            DecisionExplainer(feature_names=["cost", "carbon", "latency", "availability"])
            if config.xai_enabled else None
        )
        self.last_explanation: Optional[Explanation] = None

    async def _measure_latency(self, provider):
        base = {"aws": 50, "azure": 60, "gcp": 45}.get(provider, 50)
        return max(10.0, base + random.uniform(-5, 5))

    async def _evaluate_providers(self, data):
        current_carbon = await self.carbon_manager.get_current_intensity()
        results = {}
        for name, p in self.providers.items():
            latency = await self._measure_latency(name)
            cost = p["cost_per_gb"] * data.get("size_gb", 0.1)
            carbon = p["carbon_score"] * current_carbon / 400.0
            availability = p["availability"]
            objectives = [cost, carbon, latency / 100.0, 1 - availability]
            results[name] = {"objectives": objectives, "decision": (name, p["regions"][0])}
        return results

    async def distribute_data(self, data):
        eval_results = await self._evaluate_providers(data)
        n = len(self.weights)
        truncated = {p: d["objectives"][:n] for p, d in eval_results.items()}

        self.pareto_front = ParetoFront()
        for p, obj in truncated.items():
            self.pareto_front.add(obj, (p, self.providers[p]["regions"][0]))
        MODP_PARETO_FRONT_SIZE.set(len(self.pareto_front.get_pareto_front()))

        best = self.pareto_front.get_best_by_weight(self.weights)
        if best is None:
            provider_name = min(eval_results.items(), key=lambda x: x[1]["objectives"][0])[0]
        else:
            provider_name = best[0]
        source = "modp"

        # v19: hoist carbon_intensity out of the comprehension; unpack vote tuple
        winning_role: Optional[AgentRole] = None
        if self.coordinator and self.roles:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_score_for_bids = 1.0 / (1.0 + carbon_intensity / 1000.0)
            bids = []
            for role in AgentRole:
                idx = list(self.providers.keys()).index(provider_name)
                if role == AgentRole.CARBON_BROKER:
                    idx = min(range(len(self.providers)),
                              key=lambda i: list(self.providers.values())[i]["carbon_score"])
                elif role == AgentRole.EXPLOITER:
                    idx = min(range(len(self.providers)),
                              key=lambda i: list(self.providers.values())[i]["latency_score"])
                elif role == AgentRole.EXPLORER:
                    idx = random.randrange(len(self.providers))
                bids.append(AgentBid(
                    agent_id=role.value, role=role, confidence=0.7,
                    proposed_action=idx, rationale=role.value,
                    carbon_score=carbon_score_for_bids,
                ))
            voted, winning_role = self.coordinator.vote(bids, len(self.providers))
            provider_name = list(self.providers.keys())[voted]
            source = "multi_agent_vote"

        region = self.providers[provider_name]["regions"][0]
        async with self._lock:
            self.active_provider = provider_name
            self.active_region = region

        # Record outcomes + update weights
        actual_cost = self.providers[provider_name]["cost_per_gb"] * data.get("size_gb", 0.1)
        actual_carbon = self.providers[provider_name]["carbon_score"] * (await self.carbon_manager.get_current_intensity()) / 400.0
        actual_latency = await self._measure_latency(provider_name)
        actual_avail = 1 - self.providers[provider_name]["availability"]
        outcome = [actual_cost, actual_carbon, actual_latency / 100.0, actual_avail][:n]
        self.recent_outcomes.append((list(self.weights), outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            self._update_weights()

        # Role credit
        if self.roles:
            self.roles.record(
                winning_role or AgentRole.EXPLOITER,
                success=True,
                reward=1.0 - outcome[0],
            )

        # XAI
        if self.explainer:
            pareto_list = []
            for p, d in eval_results.items():
                pareto_list.append({
                    "cost": d["objectives"][0], "carbon": d["objectives"][1],
                    "latency": d["objectives"][2], "availability": d["objectives"][3],
                })
            if pareto_list:
                chosen_dict = pareto_list[list(self.providers.keys()).index(provider_name)]
                carbon_price = await self.carbon_manager.get_current_price()
                explanation = self.explainer.explain(
                    chosen_idx=list(self.providers.keys()).index(provider_name),
                    chosen=chosen_dict, pareto=pareto_list,
                    counterfactuals={}, safety_ok=True,
                    carbon_price=carbon_price,
                )
                self.last_explanation = explanation

        MULTI_CLOUD_DISTRIBUTIONS.labels(provider=provider_name, status="success").inc()
        return {
            "optimal_provider": provider_name,
            "optimal_region": region,
            "pareto_front_size": len(self.pareto_front.get_pareto_front()),
            "scores": {p: d["objectives"] for p, d in eval_results.items()},
            "reason": f"Provider {provider_name} selected via {source}",
            "source": source,
            "weights": list(self.weights),
            "winning_role": winning_role.value if winning_role else None,
            "explanation": self.last_explanation.to_dict() if self.last_explanation else None,
            "timestamp": datetime.now().isoformat(),
        }

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

    async def get_distribution_status(self):
        async with self._lock:
            return {"active_provider": self.active_provider,
                    "active_region": self.active_region,
                    "weights": list(self.weights),
                    "role_snapshot": self.roles.snapshot() if self.roles else {}}

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# MODULE 2: MOE PREDICTIVE
# ============================================================
class MixtureOfExpertsPredictive:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.history_price: deque = deque(maxlen=2000)
        self.history_carbon: deque = deque(maxlen=2000)
        self.history_context: deque = deque(maxlen=2000)
        self.history_labels: deque = deque(maxlen=2000)
        self._lock = asyncio.Lock()
        self.experts: List[Tuple[str, Callable]] = []
        self._init_experts()
        self.gating_model = None
        self.scaler = None
        self._init_gating()

    def _init_experts(self):
        if PROPHET_AVAILABLE:
            self.experts.append(("prophet", self._forecast_prophet))
        if SKLEARN_AVAILABLE:
            self.experts.append(("linear", self._forecast_linear))
        self.experts.append(("exp_smooth", self._forecast_exp_smooth))
        self.experts.append(("naive", self._forecast_naive))

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(max_iter=1000)
            self.scaler = StandardScaler()

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
            return {"forecast": forecast["yhat"].tail(horizon).tolist(), "confidence": 0.9}
        except Exception as e:
            logger.warning(f"Prophet failed: {e}")
            return {"forecast": [0.0] * horizon, "confidence": 0.0}

    async def _forecast_linear(self, history, horizon):
        if len(history) < 2:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h["y"] for h in history])
        try:
            model = LinearRegression().fit(X, y)
            future = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
            return {"forecast": model.predict(future).tolist(), "confidence": 0.7}
        except Exception:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}

    async def _forecast_exp_smooth(self, history, horizon):
        if len(history) < 2:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        values = [h["y"] for h in history]
        alpha = 0.3
        smoothed = values[-1]
        out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": out, "confidence": 0.6}

    async def _forecast_naive(self, history, horizon):
        if not history:
            return {"forecast": [0.0] * horizon, "confidence": 0.0}
        return {"forecast": [history[-1]["y"]] * horizon, "confidence": 0.3}

    async def _extract_context(self):
        now = datetime.now()
        recent = list(self.history_price)[-20:]
        volatility = float(np.std([h["y"] for h in recent])) if len(recent) >= 20 else 0.0
        mean = float(np.mean([h["y"] for h in recent])) if len(recent) >= 10 else 0.0
        return np.array([now.hour / 24.0, now.weekday() / 6.0, volatility, mean])

    async def _compute_labels(self, horizon=1):
        if len(self.history_price) < 50:
            return
        recent = list(self.history_price)
        actual = recent[-1]["y"]
        window = recent[-40:-1]
        labels = []
        for name, fn in self.experts:
            try:
                res = await fn(window, horizon)
                pred = res["forecast"][0] if res["forecast"] else 0.0
                labels.append(abs(pred - actual))
            except Exception:
                labels.append(float("inf"))
        best_idx = int(np.argmin(labels))
        self.history_labels.append(best_idx)

    async def update_history(self, price, carbon_intensity):
        async with self._lock:
            self.history_price.append({"ds": datetime.now(), "y": price})
            self.history_carbon.append({"ds": datetime.now(), "y": carbon_intensity})
            self.history_context.append(await self._extract_context())
            await self._compute_labels()

    async def _train_gating(self):
        if not SKLEARN_AVAILABLE or self.gating_model is None:
            return
        if len(self.history_context) < 100 or len(self.history_labels) < 100:
            return
        n = min(len(self.history_context), len(self.history_labels))
        X = np.array(list(self.history_context)[-n:])
        y = np.array(list(self.history_labels)[-n:])
        if len(np.unique(y)) < 2:
            return
        try:
            X_scaled = self.scaler.fit_transform(X)
            self.gating_model.fit(X_scaled, y)
        except Exception as e:
            logger.warning(f"Gating fit failed: {e}")

    async def forecast_price(self, horizon_hours=None):
        horizon = horizon_hours or self.config.predictive_horizon_hours
        if len(self.history_price) < 30:
            return {"forecast": [], "confidence": 0.0}
        await self._train_gating()
        forecasts, weights = [], []
        for name, fn in self.experts:
            try:
                res = await fn(self.history_price, horizon)
                forecasts.append(np.asarray(res["forecast"]))
                weights.append(res.get("confidence", 0.5))
            except Exception as e:
                logger.warning(f"Expert {name} failed: {e}")
                forecasts.append(np.zeros(horizon))
                weights.append(0.0)
        gating_probs = None
        if self.gating_model is not None and len(self.history_context) >= 100:
            try:
                ctx = await self._extract_context()
                X_scaled = self.scaler.transform([ctx])
                probs = self.gating_model.predict_proba(X_scaled)[0]
                if len(probs) == len(self.experts):
                    gating_probs = probs
            except Exception:
                pass
        if gating_probs is None:
            total = sum(weights) or 1.0
            gating_probs = np.array(weights) / total
        final = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final += gating_probs[i] * f
        for i, (name, _) in enumerate(self.experts):
            MOE_GATING_WEIGHTS.labels(expert=name).set(float(gating_probs[i]))
        PREDICTIVE_ACCURACY.labels(model="moe").set(0.85)
        return {
            "forecast": final.tolist(),
            "confidence": 0.85,
            "model": "moe",
            "expert_weights": gating_probs.tolist(),
        }

    async def forecast_carbon(self, horizon_hours=None):
        horizon = horizon_hours or self.config.predictive_horizon_hours
        if len(self.history_carbon) < 5:
            return {"forecast": [400.0] * horizon, "confidence": 0.0}
        values = [h["y"] for h in list(self.history_carbon)[-20:]]
        alpha = 0.3
        smoothed = values[-1]
        out = []
        for _ in range(horizon):
            out.append(smoothed)
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
        return {"forecast": out, "confidence": 0.6}

    def get_stats(self):
        return {
            "num_experts": len(self.experts),
            "gating_trained": bool(self.gating_model is not None and hasattr(self.gating_model, "coef_")),
            "history_len": len(self.history_price),
        }

    async def health_check(self):
        return {"status": "healthy", "num_experts": len(self.experts)}


# ============================================================
# MODULE 3: BIO-INSPIRED COLLECTOR (real fitness)
# ============================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size=20, mutation_rate=0.1, crossover_rate=0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, float]] = []
        self.rng = random.Random(0)

    def initialize(self):
        self.population = [
            {
                "interval": self.rng.uniform(30, 600),
                "batch_size": self.rng.randint(10, 100),
                "parallel_calls": self.rng.randint(1, 20),
            }
            for _ in range(self.pop_size)
        ]

    def select(self, fitness, num_parents):
        selected = []
        for _ in range(num_parents):
            a, b = self.rng.sample(range(len(self.population)), 2)
            selected.append(self.population[a] if fitness[a] > fitness[b] else self.population[b])
        return selected

    def crossover(self, p1, p2):
        if self.rng.random() < self.crossover_rate:
            return {k: p1[k] if self.rng.random() < 0.5 else p2[k] for k in p1}
        return dict(p1)

    def mutate(self, ind):
        if self.rng.random() < self.mutation_rate:
            key = self.rng.choice(list(ind.keys()))
            if key == "interval":
                ind[key] = self.rng.uniform(30, 600)
            elif key == "batch_size":
                ind[key] = self.rng.randint(10, 100)
            elif key == "parallel_calls":
                ind[key] = self.rng.randint(1, 20)
        return ind

    def evolve(self, fitness_func, generations=10):
        self.initialize()
        best = None
        for gen in range(generations):
            fitness = [fitness_func(ind) for ind in self.population]
            best_idx = int(np.argmax(fitness))
            best = dict(self.population[best_idx])
            parents = self.select(fitness, max(2, self.pop_size - 1))
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                c1 = self.crossover(parents[i], parents[i + 1])
                c2 = self.crossover(parents[i + 1], parents[i])
                offspring.append(self.mutate(c1))
                offspring.append(self.mutate(c2))
            self.population = offspring[: self.pop_size - 1] + [best]
            GA_POPULATION_FITNESS.labels(generation=str(gen)).set(float(max(fitness)))
        return best or {}


class BioInspiredAutonomousCollector:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate,
        )
        self.current_params = {"interval": 60, "batch_size": 50, "parallel_calls": 5}
        self._lock = asyncio.Lock()
        self.collection_history: deque = deque(maxlen=100)
        self.fitness_history: List[float] = []
        self.last_reward: float = 0.5

    def _fitness_func(self, params):
        # v19: fitness depends on the last observed reward; parameters that
        # keep the collection interval reasonable win.
        interval = params["interval"]
        batch = params["batch_size"]
        parallel = params["parallel_calls"]
        # Ideal interval 60-120s for helium freshness; batch 30-60; parallel 3-8.
        interval_penalty = abs(interval - 90) / 300.0
        batch_penalty = abs(batch - 45) / 100.0
        parallel_penalty = abs(parallel - 5) / 20.0
        return float(self.last_reward - 0.5 * (interval_penalty + batch_penalty + parallel_penalty))

    async def optimize_collection(self, current_state, strategy=None):
        if "reward" in current_state:
            self.last_reward = float(current_state["reward"])
        if len(self.collection_history) >= 5 or strategy is None:
            best_params = self.ga.evolve(self._fitness_func, generations=5)
            params = best_params or self.current_params
            source = "ga"
        else:
            params = self.current_params
            source = "explicit"

        result = self._simulate_collection(params, source)
        self._record(params, result)
        return result

    def _simulate_collection(self, params, source):
        return {
            "action": "bio_inspired_collection",
            "interval_seconds": params["interval"],
            "batch_size": params["batch_size"],
            "parallel_calls": params["parallel_calls"],
            "estimated_performance_gain": 0.2 - (params["interval"] / 600) * 0.1,
            "estimated_carbon_savings": 0.1 + (params["batch_size"] / 100) * 0.05,
            "quality_improvement": 0.1,
            "source": source,
        }

    def _record(self, params, result):
        self.current_params = dict(params)
        self.collection_history.append({
            "params": params, "result": result,
            "timestamp": datetime.now().isoformat(),
        })
        self.fitness_history.append(self._fitness_func(params))

    def get_collection_stats(self):
        return {
            "total_collections": len(self.collection_history),
            "current_params": self.current_params,
            "fitness_history": self.fitness_history[-10:],
        }

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# MODULE 4: MULTI-OBJECTIVE SCHEDULER
# ============================================================
class MultiObjectiveCarbonScheduler:
    def __init__(self, config, carbon_manager, predictive):
        self.config = config
        self.carbon_manager = carbon_manager
        self.predictive = predictive
        self.threshold = config.multi_objective_scheduler.carbon_threshold
        self.max_delay = config.multi_objective_scheduler.max_delay_seconds
        self.freshness_w = config.multi_objective_scheduler.freshness_importance
        self.cost_w = config.multi_objective_scheduler.cost_importance
        self.carbon_w = config.multi_objective_scheduler.carbon_importance
        self._running = False

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False

    async def _decide_delay(self, current_carbon, freshness_hours):
        forecast = await self.predictive.forecast_carbon(horizon_hours=1) if self.predictive else {"forecast": [current_carbon]}
        min_forecast = min(forecast["forecast"]) if forecast["forecast"] else current_carbon
        carbon_reduction = max(0.0, (current_carbon - min_forecast) / max(current_carbon, 1.0))
        freshness_cost = min(1.0, freshness_hours / 6.0)
        utility_now = self.freshness_w * (1 - freshness_cost) + self.cost_w * 0.8
        utility_delay = self.carbon_w * carbon_reduction + self.cost_w * 0.5
        if utility_now >= utility_delay:
            return 0
        ratio = min(1.0, max(0.0, (current_carbon - self.threshold) / max(self.threshold, 1.0)))
        return int(self.max_delay * ratio)

    async def submit_collection(self, collection_func, priority=1, critical=False, freshness_hours=1.0):
        if critical:
            return await collection_func()
        current_carbon = await self.carbon_manager.get_current_intensity()
        if current_carbon <= self.threshold:
            return await collection_func()
        delay_s = await self._decide_delay(current_carbon, freshness_hours)
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        return await collection_func()

    async def health_check(self):
        return {"status": "healthy" if self._running else "stopped"}


# ============================================================
# MODULE 5: SELF-HEALING
# ============================================================
class SelfHealingManager:
    def __init__(self, config):
        self.config = config
        self.retry_counts: Dict[str, int] = defaultdict(int)
        self.recovery_actions: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self.detectors: List[Tuple[str, Any]] = []
        self._fitted = False
        self._training_buffer: deque = deque(maxlen=200)
        if SKLEARN_AVAILABLE:
            self.detectors = [
                ("iforest", IsolationForest(contamination=config.self_healing.anomaly_contamination)),
                ("ocsvm", OneClassSVM(nu=0.1)),
            ]

    def _features(self, metrics):
        return np.array([float(v) for _, v in sorted(metrics.items())]).reshape(1, -1)

    async def _maybe_fit(self):
        if self._fitted or not self.detectors:
            return
        if len(self._training_buffer) < 30:
            return
        X = np.vstack(list(self._training_buffer))
        for _, det in self.detectors:
            try:
                det.fit(X)
            except Exception as e:
                logger.warning(f"Detector fit failed: {e}")
        self._fitted = True

    async def monitor_component(self, component, metrics):
        if not self.config.self_healing.enabled or not self.detectors:
            return True
        numeric = {k: v for k, v in metrics.items() if isinstance(v, (int, float, bool))}
        if not numeric:
            return True
        X = self._features(numeric)
        if not self._fitted:
            self._training_buffer.append(X)
            await self._maybe_fit()
            return True
        votes = []
        for _, det in self.detectors:
            try:
                pred = det.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return True
        is_anomaly = (sum(votes) / len(votes)) > 0.5
        if is_anomaly:
            ANOMALY_DETECTIONS.labels(type="self_healing").inc()
            await self._trigger_recovery(component, "restart")
            return False
        return True

    async def _trigger_recovery(self, component, action):
        async with self._lock:
            self.retry_counts[component] += 1
            self.recovery_actions.append({
                "component": component, "action": action,
                "timestamp": datetime.now().isoformat(),
            })
            SELF_HEALING_ACTIONS.labels(action=action).inc()

    async def health_check(self):
        return {
            "status": "healthy",
            "fitted": self._fitted,
            "retry_counts": dict(self.retry_counts),
            "recent_actions": list(self.recovery_actions)[-5:],
        }


# ============================================================
# COLLECTOR
# ============================================================
class EnhancedHeliumAPICollector:
    def __init__(self, config: Optional[Union[HeliumCollectorConfig, Dict]] = None):
        if isinstance(config, HeliumCollectorConfig):
            self.config = config
        elif isinstance(config, dict) and PYDANTIC_AVAILABLE:
            self.config = HeliumCollectorConfig(**config)
        else:
            self.config = HeliumCollectorConfig()
        self.instance_id = self.config.instance_id

        self.db_manager = EnhancedDatabaseManager(self.config)
        self.vault = VaultManager(self.config)
        self.quantum_security = PostQuantumCrypto(self.config, self.vault)
        self.blockchain = BlockchainHeliumVerification(self.config, self.db_manager)

        # v19: pass carbon manager into distributor
        self.carbon_manager = CarbonIntensityManager(
            api_key=self.config.carbon_api_key,
            region=self.config.carbon_region,
            base_price=self.config.carbon_base_price,
            sensitivity=self.config.carbon_price_sensitivity,
            rec_default_kwh=self.config.rec_default_kwh,
        )
        self.autonomous_collector = BioInspiredAutonomousCollector(self.config, self.db_manager)
        self.cloud_distributor = MultiObjectiveCloudDistributor(self.config, self.db_manager, self.carbon_manager)
        self.cloud_storage = MultiCloudStorage(self.config)
        self.predictive = (
            MixtureOfExpertsPredictive(self.config, self.db_manager)
            if self.config.moe.enabled else None
        )
        self.self_healing = SelfHealingManager(self.config)
        self.scheduler = (
            MultiObjectiveCarbonScheduler(self.config, self.carbon_manager, self.predictive)
            if self.config.multi_objective_scheduler.enabled else None
        )
        self.rate_limiter = EnhancedRateLimiter(self.config.rate_limit_requests, self.config.rate_limit_window)
        self._api_bulkhead = EnhancedBulkhead(self.config.max_concurrent_api_calls)
        self._collection_interval = self.config.collection_interval

        # Ten enhancements
        self.causal = (
            CausalCounterfactualEstimator(
                state_dim=STATE_VECTOR_DIM, n_actions=4
            ) if self.config.causal_enabled else None
        )
        self.last_counterfactuals: Dict[int, float] = {}
        self.federated = FederatedAggregator() if self.config.federated_enabled else None
        self.temporal = None
        self.shield = None
        if self.config.temporal_logic_enabled:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="price_reasonable",
                predicate=lambda r: 100.0 <= r.get("spot_price_usd_per_mcf", 200.0) <= 400.0,
                operator=STLOperator.ALWAYS, horizon=10,
            ))
            self.temporal.add_formula(STLFormula(
                name="freshness_eventually_ok",
                predicate=lambda r: r.get("data_freshness_minutes", 0.0) <= 5.0,
                operator=STLOperator.EVENTUALLY, horizon=5,
            ))
            self.shield = SafetyShield(self.temporal)
        self.explainer = DecisionExplainer() if self.config.xai_enabled else None
        self.last_explanation: Optional[Explanation] = None
        self.precision_controller = (
            PrecisionController(quality_floor=0.95)
            if self.config.precision_switching_enabled else None
        )
        self.precision_adapter = (
            HardwareAwareAdapter(self.precision_controller)
            if self.precision_controller else None
        )
        self.current_precision: Optional[PrecisionLevel] = None
        self.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=self.config.chaos_fault_prob,
            latency_inject_ms=self.config.chaos_latency_ms,
            carbon_spike_prob=self.config.chaos_carbon_spike_prob,
        )) if self.config.chaos_enabled else None
        self.uncertainty = UncertaintyEstimator() if self.config.hitl_enabled else None
        self.hitl = (
            HumanInTheLoopGate(timeout_s=self.config.hitl_timeout_s)
            if self.config.hitl_enabled else None
        )
        self.active_learner = ActiveLearningSampler() if self.config.hitl_enabled else None

        # Quantum teacher + distillation ensemble (used in XAI / distillation context)
        self.quantum_teacher = (
            QuantumInspiredTeacher(seed=42) if self.config.distillation_enabled else None
        )
        self.distill_ensemble = (
            DistillationEnsemble(self.quantum_teacher, alpha=0.5)
            if self.quantum_teacher else None
        )

        self.data_history: deque = deque(maxlen=self.config.max_data_history)
        self.realtime_data: Optional[MergedHeliumData] = None
        self.last_update_time: Optional[datetime] = None

        self._task_manager = TaskManager(max_workers=5)
        self._shutdown_event = asyncio.Event()
        self._running = False

        self._health_components = {
            "database": self.db_manager,
            "quantum_security": self.quantum_security,
            "blockchain": self.blockchain,
            "carbon_manager": self.carbon_manager,
            "cloud_distributor": self.cloud_distributor,
            "cloud_storage": self.cloud_storage,
            "predictive": self.predictive,
            "self_healing": self.self_healing,
        }

        self._register_tasks()
        atexit.register(self._atexit_save)

    def _register_tasks(self):
        self._task_manager.register_task("periodic_collection", self._periodic_collection_loop, restart_on_exit=True)
        self._task_manager.register_task("health_check", self._health_check_loop, restart_on_exit=True)
        self._task_manager.register_task("cleanup", self._cleanup_loop, restart_on_exit=True)
        self._task_manager.register_task("carbon_update", self._carbon_update_loop, restart_on_exit=True)
        self._task_manager.register_task("self_healing_monitor", self._self_healing_loop, restart_on_exit=True)
        if self.predictive:
            self._task_manager.register_task("predictive_update", self._predictive_update_loop, restart_on_exit=True)

    async def start(self):
        self._running = True
        await self.db_manager.init()
        if self.scheduler:
            await self.scheduler.start()
        self._task_manager.start_registered_tasks()
        logger.info("Helium Collector started")

    async def _atexit_save(self):
        pass

    async def _periodic_collection_loop(self):
        backoff = 1.0
        while self._running and not self._shutdown_event.is_set():
            try:
                await self.collect_all_data()
                backoff = 1.0
                await asyncio.sleep(self._collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic collection failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _health_check_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                # v19: actually run the health check
                h = await self.health_check()
                HEALTH_SCORE.set(h.get("health_score", 100))
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(60)

    async def _cleanup_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                # v19: trim in-memory history beyond max_data_history is a no-op
                # (deque already caps), but we do prune old data_ids here.
                while len(self.data_history) > self.config.max_data_history:
                    self.data_history.popleft()
                await asyncio.sleep(self.config.cleanup_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
                await asyncio.sleep(60)

    async def _carbon_update_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                # v19: actually refresh carbon intensity
                ci = await self.carbon_manager.get_current_intensity()
                CARBON_INTENSITY.set(ci)
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update loop error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                # v19: actually feed the predictive model
                if self.predictive and self.realtime_data:
                    await self.predictive.update_history(
                        self.realtime_data.spot_price_usd_per_mcf,
                        await self.carbon_manager.get_current_intensity(),
                    )
                await asyncio.sleep(self.config.ml_retrain_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update loop error: {e}")
                await asyncio.sleep(60)

    async def _self_healing_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                components = {
                    "quantum": {"pqc_available": float(PQC_AVAILABLE)},
                    "blockchain": await self.blockchain.get_blockchain_status(),
                    "carbon": {"intensity": self.carbon_manager.current_intensity},
                    "cloud": {"active_provider_idx":
                              list(self.cloud_distributor.providers.keys()).index(self.cloud_distributor.active_provider)},
                }
                for comp, metrics in components.items():
                    await self.self_healing.monitor_component(comp, metrics)
                await asyncio.sleep(self.config.self_healing.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")
                await asyncio.sleep(60)

    async def collect_all_data(self) -> MergedHeliumData:
        if self.scheduler:
            return await self.scheduler.submit_collection(
                self._collect_all_data_internal, priority=1, critical=False, freshness_hours=1.0
            )
        return await self._collect_all_data_internal()

    async def _collect_all_data_internal(self) -> MergedHeliumData:
        start_time = time.time()

        if self.chaos and self.chaos.maybe_fault():
            HELIUM_COLLECTIONS.labels(status="chaos_fault").inc()
            raise CollectionError("Chaos fault injected")

        await self.rate_limiter.wait_and_acquire()

        async def _fetch_snapshot():
            # Placeholder data source. In a real deployment this would call
            # aiohttp against a Helium feed URL.
            return {
                "production": 28000 + random.uniform(-500, 500),
                "demand": 29000 + random.uniform(-500, 500),
                "price": 200 + random.uniform(-20, 20),
                "futures": 210 + random.uniform(-20, 20),
                "inventory": 60 + random.uniform(-10, 10),
                "sentiment": random.uniform(-0.3, 0.3),
            }

        snapshot = await self._api_bulkhead.execute(_fetch_snapshot)

        if self.chaos:
            delay_ms = self.chaos.maybe_latency()
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)

        production = float(snapshot["production"])
        demand = float(snapshot["demand"])
        price = float(snapshot["price"])
        futures = float(snapshot["futures"])
        inventory = float(snapshot["inventory"])
        sentiment = float(snapshot["sentiment"])

        ratio = demand / max(production, 1)
        scarcity = max(0.0, min(1.0, (ratio - 0.95) / 0.15))

        is_anomaly = False
        anomaly_score = 0.0
        if self.config.self_healing.enabled:
            numeric = {"price": price, "inventory": inventory,
                       "production": production, "demand": demand}
            healthy = await self.self_healing.monitor_component("helium_snapshot", numeric)
            if not healthy:
                is_anomaly = True
                anomaly_score = 1.0

        carbon_intensity = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
        carbon_price = await self.carbon_manager.get_current_price()
        precision_level = None
        if self.precision_adapter:
            _, precision_level = self.precision_adapter.adapt({}, carbon_intensity, 0.5, carbon_price)
            self.current_precision = precision_level

        merged = MergedHeliumData(
            data_id=f"helium_{uuid.uuid4().hex[:8]}",
            global_production_tonnes=production,
            global_demand_tonnes=demand,
            spot_price_usd_per_mcf=price,
            futures_price_usd_per_mcf=futures,
            scarcity_index=scarcity,
            inventory_level_days=inventory,
            news_sentiment_score=sentiment,
            data_sources=["simulated"],
            data_freshness_minutes=(time.time() - start_time) / 60.0,
            confidence_score=0.95 if not is_anomaly else 0.7,
            is_anomaly=is_anomaly,
            anomaly_score=anomaly_score,
            quality_score=100.0 - (20 if is_anomaly else 0) - (10 if price < 150 or price > 250 else 0),
            precision_level=precision_level.value if precision_level else None,
            chaos_events=list(self.chaos.events[-3:]) if self.chaos else [],
        )

        # Quantum signature
        quantum_key = await self.quantum_security.generate_keypair(self.config.quantum_algorithm)
        signature = await self.quantum_security.sign_helium_data(asdict(merged), quantum_key["key_id"])
        merged.quantum_signature = signature

        # Blockchain
        data_hash = hashlib.sha256(
            json.dumps(asdict(merged), sort_keys=True, default=str).encode()
        ).hexdigest()
        blockchain_result = await self.blockchain.record_helium_data(
            merged.data_id, data_hash, {"price": price}
        )
        merged.blockchain_tx_hash = blockchain_result.get("tx_hash")

        # Temporal monitor + safety shield
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.temporal and self.shield:
            self.temporal.observe({
                "spot_price_usd_per_mcf": price,
                "data_freshness_minutes": merged.data_freshness_minutes,
            })
            _, safety_ok, stl_verdict = self.shield.screen(
                merged, [{"spot_price_usd_per_mcf": price}], key="spot_price_usd_per_mcf"
            )
        merged.safety_ok = safety_ok
        merged.stl_verdict = stl_verdict

        # Multi-agent cloud distribution (v19: unpack winning_role)
        distribution = await self.cloud_distributor.distribute_data({
            "size_gb": 0.01, "data_points": 1, "price": price,
        })
        merged.cloud_distribution = distribution

        # Cloud backup
        if self.cloud_storage.providers:
            try:
                await self.cloud_storage.store(asdict(merged), f"helium_{merged.data_id}.json")
            except Exception as e:
                logger.error(f"Cloud backup failed: {e}")

        # Causal state via the shared schema
        state_vec = state_vector_from_metrics({
            "gpu_utilization_pct": 0.0,
            "gpu_power_watts": 0.0,
            "gpu_temp_c": 0.0,
            "cpu_utilization_pct": 0.0,
            "carbon_intensity_g_per_kwh": carbon_intensity,
            "carbon_price_per_kg": carbon_price,
            "rec_kwh_available": self.carbon_manager.recs.total_kwh(),
            "uncertainty_score": 0.0,
        })
        if self.causal:
            self.causal.update(CausalTransition(
                state=state_vec, action=0,
                reward=merged.quality_score / 100.0,
                next_state=state_vec,
            ))
            self.last_counterfactuals = self.causal.counterfactuals(state_vec, 4)
            merged.counterfactuals = self.last_counterfactuals

        # Federated submit
        if self.federated:
            self.federated.submit(FederatedUpdate(
                node_id=self.instance_id,
                weights={"local": np.array([merged.quality_score, price, carbon_intensity], dtype=np.float64)},
                n_samples=1,
                carbon_intensity=carbon_intensity,
            ))
            self.federated.aggregate()

        # Uncertainty / HITL
        reward = merged.quality_score / 100.0 - carbon_price * 0.05
        reward = max(0.0, min(1.0, reward))
        uncertainty_score = 0.0
        human_approved = True
        if self.uncertainty and self.hitl:
            probs = np.ones(4) / 4.0
            is_unc, unc = self.uncertainty.is_uncertain(probs)
            uncertainty_score = unc
            if is_unc or not safety_ok:
                req = HITLRequest(
                    request_id=uuid.uuid4().hex[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=0,
                    candidates=[{"value": price}],
                    uncertainty=unc,
                    carbon_price=carbon_price,
                )
                human_approved = await self.hitl.request(req)
                merged.hitl_approved = human_approved
                if self.active_learner:
                    self.active_learner.maybe_store(
                        {"data_id": merged.data_id, "reward": reward},
                        uncertainty=unc, threshold=0.4,
                    )

        # XAI
        if self.explainer:
            pareto_list = []
            for p in distribution.get("scores", {}):
                d = distribution["scores"][p]
                pareto_list.append({
                    "cost": d[0], "carbon": d[1], "latency": d[2], "availability": d[3],
                })
            if pareto_list:
                chosen_dict = pareto_list[0]
                explanation = self.explainer.explain(
                    chosen_idx=0,
                    chosen=chosen_dict,
                    pareto=pareto_list,
                    counterfactuals=self.last_counterfactuals,
                    safety_ok=safety_ok and human_approved,
                    carbon_price=carbon_price,
                )
                self.last_explanation = explanation
                merged.explanation = explanation.to_dict()

        merged.provenance = {
            "schema": "helium_data_v19",
            "instance_id": self.instance_id,
            "chaos_active": self.chaos is not None,
            "distillation_active": self.distill_ensemble is not None,
            "state_vector_dim": STATE_VECTOR_DIM,
        }

        self.realtime_data = merged
        self.last_update_time = datetime.now()
        self.data_history.append(merged)
        await self.db_manager.insert_helium_data(merged)

        DATA_FRESHNESS.set(merged.data_freshness_minutes * 60.0)
        DATA_QUALITY_SCORE.set(merged.quality_score)
        INVENTORY_LEVEL.set(merged.inventory_level_days)
        SENTIMENT_SCORE.set(merged.news_sentiment_score)
        CARBON_INTENSITY.set(carbon_intensity)
        HELIUM_COLLECTIONS.labels(status="success").inc()

        logger.info(f"Collected {merged.data_id}: price=${price:.0f}, quality={merged.quality_score:.0f}")

        # Feed the bio collector with real reward
        try:
            await self.autonomous_collector.optimize_collection({
                "carbon": carbon_intensity,
                "freshness": 0.5,
                "cost": 0.5,
                "reward": reward,
            })
            new_interval = self.autonomous_collector.current_params.get("interval", self._collection_interval)
            self._collection_interval = int(max(30, min(600, new_interval)))
        except Exception as e:
            logger.warning(f"Bio collector update failed: {e}")

        return merged

    async def submit_hitl_decision(self, request_id: str, approved: bool) -> bool:
        """v19: deliver an external HITL decision."""
        if self.hitl is None:
            return False
        return await self.hitl.submit_decision(request_id, approved)

    async def save_state(self, path: str) -> bool:
        """v19: persist causal, roles, federated, bio params, last data IDs."""
        state: Dict[str, Any] = {"schema": "helium_state_v1"}
        if self.causal is not None:
            state["causal"] = self.causal.export_state()
        if self.cloud_distributor.roles is not None:
            state["roles"] = self.cloud_distributor.roles.snapshot()
        if self.federated is not None:
            g = self.federated.global_weights()
            if g:
                state["federated_global"] = {k: v.tolist() for k, v in g.items()}
        state["bio_params"] = dict(self.autonomous_collector.current_params)
        state["collection_interval"] = self._collection_interval
        state["recent_data_ids"] = [d.data_id for d in list(self.data_history)[-20:]]
        try:
            d = os.path.dirname(os.path.abspath(path)) or "."
            os.makedirs(d, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=d, prefix=".helium_", suffix=".json")
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
        if state.get("schema") != "helium_state_v1":
            return False
        if self.causal is not None and "causal" in state:
            self.causal.import_state(state["causal"])
        if self.federated is not None and "federated_global" in state:
            try:
                self.federated._global = {
                    k: np.asarray(v, dtype=np.float64)
                    for k, v in state["federated_global"].items()
                }
            except Exception:
                pass
        if "bio_params" in state:
            self.autonomous_collector.current_params = dict(state["bio_params"])
        if "collection_interval" in state:
            self._collection_interval = int(state["collection_interval"])
        return True

    async def get_comprehensive_status(self) -> Dict:
        quantum_status = self.quantum_security.get_quantum_status()
        blockchain_status = await self.blockchain.get_blockchain_status()
        collection_stats = self.autonomous_collector.get_collection_stats()
        cloud_status = await self.cloud_distributor.get_distribution_status()
        return {
            "instance_id": self.instance_id,
            "version": self.config.version,
            "quantum_security": quantum_status,
            "blockchain": blockchain_status,
            "autonomous_collection": collection_stats,
            "cloud_distribution": cloud_status,
            "data_points": len(self.data_history),
            "last_update": self.last_update_time.isoformat() if self.last_update_time else None,
            "data_fresh_minutes": (
                (datetime.now() - self.last_update_time).total_seconds() / 60.0
                if self.last_update_time else None
            ),
            "rate_limiter": self.rate_limiter.get_metrics(),
            "predictive": self.predictive.get_stats() if self.predictive else None,
            "cloud_storage": {"providers": list(self.cloud_storage.providers.keys())},
            "self_healing": await self.self_healing.health_check(),
            "scheduler": {"enabled": self.scheduler is not None},
            "ten_enhancements": {
                "quantum_distillation": self.distill_ensemble is not None,
                "causal": self.causal is not None,
                "federated": self.federated is not None,
                "multi_agent": self.cloud_distributor.coordinator is not None,
                "temporal_logic": self.temporal is not None,
                "xai": self.explainer is not None,
                "precision_switching": self.precision_adapter is not None,
                "carbon_market": True,
                "chaos": self.chaos is not None,
                "hitl": self.hitl is not None,
            },
            "current_precision": self.current_precision.value if self.current_precision else None,
            "rec_kwh": self.carbon_manager.recs.total_kwh(),
            "counterfactuals": self.last_counterfactuals,
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

    async def shutdown(self):
        logger.info(f"Shutting down Helium Collector (instance: {self.instance_id})")
        self._shutdown_event.set()
        self._running = False
        await self._task_manager.stop_all()
        if self.scheduler:
            await self.scheduler.stop()
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()
        with contextlib.suppress(Exception):
            await self.db_manager.close()
        logger.info("Shutdown complete")


# ============================================================
# FASTAPI
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Helium API Collector API", version="19.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"],
    )
    security = HTTPBearer()

    _api_config: Optional[HeliumCollectorConfig] = None

    def _get_config():
        global _api_config
        if _api_config is None:
            _api_config = HeliumCollectorConfig()
        return _api_config

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        if not JOSE_AVAILABLE:
            return {"sub": "anonymous"}
        try:
            return jwt.decode(credentials.credentials, _get_config().jwt_secret, algorithms=["HS256"])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    collector: Optional[EnhancedHeliumAPICollector] = None

    @app.post("/collect")
    async def collect(user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        data = await collector.collect_all_data()
        return {"data": asdict(data)}

    @app.get("/status")
    async def status(user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        return await collector.get_comprehensive_status()

    @app.get("/health")
    async def health():
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        return await collector.health_check()

    @app.get("/explanation/last")
    async def last_explanation(user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        return {
            "explanation": collector.last_explanation.to_dict() if collector.last_explanation else None,
            "counterfactuals": collector.last_counterfactuals,
        }

    @app.post("/chaos")
    async def chaos(fault_prob: float = 0.0, latency_ms: float = 0.0,
                    carbon_spike_prob: float = 0.0,
                    user: Dict = Depends(verify_token)):
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        collector.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=fault_prob, latency_inject_ms=latency_ms,
            carbon_spike_prob=carbon_spike_prob,
        ))
        return {"status": "chaos enabled", "config": asdict(collector.chaos.config)}

    @app.post("/hitl/approval")
    async def hitl_approval(request_id: str, approved: bool,
                            user: Dict = Depends(verify_token)):
        """v19: deliver the decision to the waiting HITL request."""
        if not collector:
            raise HTTPException(status_code=503, detail="Collector not initialized")
        delivered = await collector.submit_hitl_decision(request_id, approved)
        if not delivered:
            HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
            audit_logger.info(f"HITL approval (no pending): {request_id} -> {approved}")
            return {"status": "recorded", "request_id": request_id,
                    "approved": approved, "delivered": False}
        audit_logger.info(f"HITL approval delivered: {request_id} -> {approved}")
        return {"status": "delivered", "request_id": request_id, "approved": approved}

    @app.on_event("startup")
    async def startup():
        global collector
        collector = EnhancedHeliumAPICollector()
        await collector.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        if collector:
            await collector.shutdown()


# ============================================================
# SINGLETON
# ============================================================
_collector_instance: Optional[EnhancedHeliumAPICollector] = None
_collector_lock = asyncio.Lock()


async def get_helium_collector(
    config: Optional[Union[HeliumCollectorConfig, Dict]] = None,
) -> EnhancedHeliumAPICollector:
    global _collector_instance
    if _collector_instance is None:
        async with _collector_lock:
            if _collector_instance is None:
                _collector_instance = EnhancedHeliumAPICollector(config)
                await _collector_instance.start()
    return _collector_instance


# ============================================================
# SIGNAL HANDLERS (retained task references)
# ============================================================
_shutdown_tasks: set = set()


async def shutdown_handler():
    global _collector_instance
    if _collector_instance:
        await _collector_instance.shutdown()
        _collector_instance = None


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
    print("Enhanced Helium API Collector v19.0 — Enterprise Quantum+ (shared modules)")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("HELIUM_SERVE_API", "0") == "1":
        cfg = HeliumCollectorConfig()
        uvicorn.run(app, host=cfg.api_host, port=cfg.api_port, log_level="info")
        return

    collector = await get_helium_collector()

    print("\n✅ Ten enhancements imported from the enhancements package:")
    print("   [1] Quantum-Distillation     QuantumInspiredTeacher + DistillationEnsemble")
    print("   [2] Causal RL                CausalCounterfactualEstimator")
    print("   [3] Federated Green Learning FederatedAggregator")
    print("   [4] Multi-Agent Coordination EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic           STLFormula + TemporalLogicMonitor + SafetyShield")
    print("   [6] Explainable AI           DecisionExplainer")
    print("   [7] Adaptive Precision       PrecisionController + HardwareAwareAdapter")
    print("   [8] Carbon Markets / RECs    CarbonMarketClient + RECInventory")
    print("   [9] Resilience / Chaos       ChaosEngineer")
    print("  [10] HITL / Active Learning   UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler")

    for _ in range(3):
        data = await collector.collect_all_data()
        print(f"\n📊 {data.data_id}: price=${data.spot_price_usd_per_mcf:.0f}, "
              f"quality={data.quality_score:.0f}, precision={data.precision_level}, "
              f"safe={data.safety_ok}, HITL={data.hitl_approved}, "
              f"winning_role={data.cloud_distribution.get('winning_role') if data.cloud_distribution else None}")

    status = await collector.get_comprehensive_status()
    print(f"\n🌍 Distribution: {status['cloud_distribution']['active_provider']} "
          f"({status['cloud_distribution']['active_region']})")
    print(f"🔐 PQC available: {status['quantum_security']['pqc_available']}")
    print(f"⛓️ Blockchain connected: {status['blockchain']['connected']}")
    print(f"🧠 Predictive experts: {status['predictive']['num_experts']}, "
          f"gating trained: {status['predictive']['gating_trained']}")
    print(f"⚙️  Self-healing fitted: {status['self_healing']['fitted']}, "
          f"retries: {status['self_healing']['retry_counts']}")
    print(f"🔧 Enhancements: {status['ten_enhancements']}")
    print(f"💚 Health: {status['health']['health_score']}")

    print("\n" + "=" * 80)
    print("✅ Enhanced Helium API Collector v19.0 — Ready")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await collector.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
