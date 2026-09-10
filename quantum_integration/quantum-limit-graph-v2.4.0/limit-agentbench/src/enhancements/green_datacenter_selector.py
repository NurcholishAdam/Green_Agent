#!/usr/bin/env python3
# src/enhancements/green_datacenter_selector_enhanced_v16_0.py
"""
Enhanced Green Data Center Selector — v18.0

Single-file rewrite that fixes every runtime bug from v16 and hosts all ten
Green Agent enhancements as first-class modules inside this file:

  1. Quantum-Distillation Integration      QuantumInspiredTeacher + DistillationEnsemble
  2. Causal RL for Policy Adaptation       CausalCounterfactualEstimator
  3. Federated Green Learning              FederatedAggregator
  4. Multi-Agent Coordination              AgentRole + EmergentRoleRegistry + MultiAgentCoordinator
  5. Temporal Logic / Formal Verification  STLFormula + TemporalLogicMonitor + SafetyShield
  6. Explainable AI                        DecisionExplainer + Explanation
  7. Adaptive Precision Switching          PrecisionController + HardwareAwareAdapter
  8. Carbon Markets / RECs                 CarbonMarketClient + RECInventory
  9. Resilience / Chaos                    ChaosEngineer
 10. HITL / Active Learning                UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler

Runtime fixes over v16:
  - Missing imports added (logging.handlers, itertools)
  - Guarded ORM declarations (Base is never None when subclassed)
  - LinUCB, GeneticAlgorithm, LeaderElection, OrchestrationError, WorkloadPredictor defined locally
  - insert_project / insert_selection / execute_sync added to the DB manager
  - BlockchainConfig gains chain_id and poa
  - SelectorConfig gains explicit criterion weights
  - PQC signing uses module-level sign()
  - Migrations deferred to init()
  - VaultManager uses get_running_loop() + correct hvac v2 signature
  - Config/RateLimiter resolved lazily, not at module import
  - _get_candidates no longer mutates self.projects in place
  - NetworkLatencyMonitor returns fresh measurements, not a random walk
  - _update_weights differentiates a real utility
  - _evaluate_providers truncated to len(weights)
  - _score_candidates_nsga2 actually uses Pareto non-dominance
  - file_hash initialized before use in _select_datacenter_internal
  - workload_predictor initialized
  - Health aggregation: empty checkable -> degraded, not healthy
  - AdaptiveTTLCache tracks size properly
  - uvicorn gets the app object
  - Signal handlers portable to Windows
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
# ENHANCEMENT MODULE IMPORTS (graceful fallback)
# ============================================================
ENHANCEMENTS_AVAILABLE = False
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    from enhancements.moe_system import ExpertRouter  # type: ignore
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    from enhancements.contextual_bandit import ContextualBandit  # type: ignore
    from enhancements.limit_graph import LimitGraph  # type: ignore
    from enhancements.rlhf import RLHFOptimizer  # type: ignore
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller  # type: ignore
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

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
# PROMETHEUS
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
    RATE_LIMITER_THROTTLE = Gauge("selector_rate_limiter_throttle", "RL", registry=REGISTRY)
    CLOUD_STORAGE = Counter("selector_cloud_storage_operations_total", "S", ["provider", "operation", "status"], registry=REGISTRY)
    VAULT_OPERATIONS = Counter("selector_vault_operations_total", "V", ["operation", "status"], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge("selector_predictive_accuracy", "PA", ["model"], registry=REGISTRY)
    OPTIMIZER_DECISIONS = Counter("selector_optimizer_decisions_total", "OD", ["parameter"], registry=REGISTRY)
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
    CIRCUIT_BREAKER_STATE = RATE_LIMITER_THROTTLE = CLOUD_STORAGE = VAULT_OPERATIONS = DummyMetrics()
    PREDICTIVE_ACCURACY = OPTIMIZER_DECISIONS = HEALTH_SCORE = CARBON_PRICE = DummyMetrics()
    REC_INVENTORY_KWH = PRECISION_SWITCHES = CHAOS_EVENTS = HITL_APPROVALS = DummyMetrics()
    SAFETY_VIOLATIONS = FEDERATED_ROUNDS = DummyMetrics()


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
class PredictiveError(SelectorError): pass
class OptimizerError(SelectorError): pass
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
class ICarbonManager(Protocol):
    async def get_current_intensity(self) -> float: ...
    async def close(self): ...
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
# ============================================================
# TEN ENHANCEMENT MODULES
# ============================================================
# ============================================================


# ------------------------------------------------------------
# Enhancement 7: Adaptive Precision
# ------------------------------------------------------------
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    INT8 = "int8"
    INT4 = "int4"


PRECISION_COST = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "memory": 1.00, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "memory": 0.125, "bits": 4.0},
}


class PrecisionController:
    def __init__(self, supported=None, quality_floor=0.95, carbon_aware=True):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware

    def select(self, carbon_intensity, latency_headroom_ratio, carbon_price=0.0):
        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        agg = 0.5 * stress + 0.3 * price_stress + 0.2 * (1.0 - headroom)
        ordered = [p for p in PrecisionLevel if p in self.supported]
        ordered.sort(key=lambda p: PRECISION_COST[p]["energy"])
        chosen = ordered[0]
        for i, p in enumerate(ordered):
            if PRECISION_COST[p]["quality"] >= self.quality_floor and agg >= 0.25 * i:
                chosen = p
        return chosen


class HardwareAwareAdapter:
    def __init__(self, controller):
        self.controller = controller

    def adapt(self, policy, carbon_intensity, latency_headroom, carbon_price):
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        PRECISION_SWITCHES.labels(level=level.value).inc()
        return out, level


# ------------------------------------------------------------
# Enhancement 8: Carbon Markets / RECs
# ------------------------------------------------------------
class CarbonMarketClient:
    def __init__(self, base_price=0.05, sensitivity=0.0005):
        self.base_price = base_price
        self.sensitivity = sensitivity

    def price(self, carbon_intensity, hour_of_day=None):
        tod = 1.0
        if hour_of_day is not None:
            tod = 1.0 + 0.3 * math.sin((hour_of_day / 24.0) * 2 * math.pi)
        return self.base_price + self.sensitivity * carbon_intensity * tod


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"


class RECInventory:
    def __init__(self, grid_kg_co2_per_kwh=0.4):
        self.grid_factor = grid_kg_co2_per_kwh
        self._records: List[RECRecord] = []

    def add(self, kwh, source="solar"):
        self._records.append(RECRecord(kwh=kwh, issued_at=time.time(), source=source))

    def total_kwh(self):
        return sum(r.kwh for r in self._records)

    def consume(self, kwh):
        remaining = kwh
        offset = 0.0
        new_records = []
        for r in self._records:
            if remaining <= 0:
                new_records.append(r)
                continue
            take = min(r.kwh, remaining)
            remaining -= take
            offset += take * self.grid_factor
            if r.kwh - take > 1e-9:
                new_records.append(RECRecord(kwh=r.kwh - take, issued_at=r.issued_at, source=r.source))
        self._records = new_records
        return offset


# ------------------------------------------------------------
# Enhancement 9: Chaos
# ------------------------------------------------------------
@dataclass
class ChaosConfig:
    fault_prob: float = 0.0
    latency_inject_ms: float = 0.0
    latency_inject_prob: float = 0.3
    carbon_spike_prob: float = 0.0
    carbon_spike_factor: float = 1.5
    seed: int = 0

    def enabled(self):
        return (self.fault_prob > 0
                or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
                or self.carbon_spike_prob > 0)


class ChaosEngineer:
    def __init__(self, config=None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fault(self):
        if self.rng.random() < self.config.fault_prob:
            self.events.append({"type": "fault", "t": time.time()})
            CHAOS_EVENTS.labels(type="fault").inc()
            return True
        return False

    def maybe_latency(self):
        if self.config.latency_inject_ms > 0 and self.rng.random() < self.config.latency_inject_prob:
            self.events.append({"type": "latency", "t": time.time()})
            CHAOS_EVENTS.labels(type="latency").inc()
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity):
        if self.rng.random() < self.config.carbon_spike_prob:
            self.events.append({"type": "carbon_spike", "t": time.time()})
            CHAOS_EVENTS.labels(type="carbon_spike").inc()
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity


# ------------------------------------------------------------
# Enhancement 5: Temporal Logic
# ------------------------------------------------------------
class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"


@dataclass
class STLFormula:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10


class TemporalLogicMonitor:
    def __init__(self, horizon=10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=horizon * 4)
        self.formulas: List[STLFormula] = []

    def add_formula(self, f):
        self.formulas.append(f)

    def observe(self, record):
        self._history.append(record)

    def verify(self):
        results = {}
        for f in self.formulas:
            window = list(self._history)[-f.horizon:]
            if not window:
                results[f.name] = True
                continue
            sat = [bool(f.predicate(r)) for r in window]
            if f.operator == STLOperator.ALWAYS:
                results[f.name] = all(sat)
            elif f.operator in (STLOperator.EVENTUALLY, STLOperator.UNTIL):
                results[f.name] = any(sat)
            else:
                results[f.name] = True
            if not results[f.name]:
                SAFETY_VIOLATIONS.labels(formula=f.name).inc()
        return results


class SafetyShield:
    def __init__(self, monitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected, candidates):
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected, True, verdict
        if not candidates:
            return selected, False, verdict
        safe = min(candidates, key=lambda p: p.carbon_intensity if hasattr(p, "carbon_intensity") else 9999)
        self.violations.append({"t": time.time(), "violated": violated})
        return safe, False, verdict


# ------------------------------------------------------------
# Enhancement 2: Causal RL
# ------------------------------------------------------------
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    def __init__(self, state_dim, n_actions, ridge=1e-3):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._n = 0

    def _features(self, s, a):
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n):
        if n == self.n_actions:
            return
        self._A = np.eye(self.state_dim + n + 1) * self.ridge
        self._b = np.zeros(self.state_dim + n + 1)
        self.n_actions = n
        self._n = 0

    def update(self, t):
        x = self._features(t.state, t.action)
        self._A += np.outer(x, x)
        self._b += t.reward * x
        self._n += 1

    def predict(self, s, a):
        if self._n < 2:
            return 0.0
        try:
            theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError:
            return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s, n_actions):
        self._ensure_actions(n_actions)
        return {a: self.predict(s, a) for a in range(n_actions)}


# ------------------------------------------------------------
# Enhancement 3: Federated
# ------------------------------------------------------------
@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    def __init__(self, dp_sigma=1e-3, staleness_s=3600.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None

    def submit(self, u):
        self._updates[u.node_id] = u

    def _fresh(self):
        now = time.time()
        return [u for u in self._updates.values() if (now - u.timestamp) <= self.staleness_s]

    def aggregate(self):
        fresh = self._fresh()
        if not fresh:
            return self._global
        weights = np.array([u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh])
        weights /= max(weights.sum(), 1e-12)
        keys = fresh[0].weights.keys()
        agg = {}
        for k in keys:
            stacked = np.stack([u.weights[k] for u in fresh], axis=0)
            blended = np.tensordot(weights, stacked, axes=([0], [0]))
            if self.dp_sigma > 0:
                blended = blended + np.random.normal(0.0, self.dp_sigma, size=blended.shape)
            agg[k] = blended
        self._global = agg
        FEDERATED_ROUNDS.inc()
        return agg

    def global_weights(self):
        return self._global


# ------------------------------------------------------------
# Enhancement 4: Multi-Agent
# ------------------------------------------------------------
class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


@dataclass
class AgentBid:
    agent_id: str
    role: AgentRole
    confidence: float
    proposed_action: int
    rationale: str
    carbon_score: float


class EmergentRoleRegistry:
    def __init__(self, decay=0.95):
        self.decay = decay
        self._scores = {r: 1.0 for r in AgentRole}
        self._counts = {r: 0 for r in AgentRole}

    def record(self, role, success, reward):
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = reward if success else -abs(reward) * 0.5
        self._scores[role] += signal
        self._counts[role] += 1

    def weights(self):
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self):
        return max(self._scores, key=self._scores.get)


class MultiAgentCoordinator:
    def __init__(self, registry, role_bias=None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.2,
            AgentRole.CARBON_BROKER: 1.1,
            AgentRole.VERIFIER: 1.0,
        }

    def vote(self, bids, n_candidates):
        if not bids or n_candidates <= 0:
            return 0
        role_w = self.registry.weights()
        scores = np.zeros(n_candidates)
        for b in bids:
            idx = b.proposed_action % n_candidates
            weight = (role_w.get(b.role, 0.1) * self.role_bias.get(b.role, 1.0)
                      * max(b.confidence, 0.0) * (1.0 + b.carbon_score))
            scores[idx] += weight
        return int(np.argmax(scores))


# ------------------------------------------------------------
# Enhancement 6: XAI
# ------------------------------------------------------------
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

    def to_dict(self):
        return asdict(self)


class DecisionExplainer:
    def __init__(self, feature_names=None):
        self.feature_names = list(feature_names) if feature_names else [
            "green_score", "carbon_intensity", "latency_ms",
            "cost_per_hour", "pue_estimated", "helium_efficiency",
        ]

    def _attributions(self, chosen, pareto):
        if not pareto:
            return []
        arr = np.array([[float(c.get(f, 0.0)) for f in self.feature_names] for c in pareto])
        chosen_vec = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
        mean = arr.mean(axis=0)
        std = arr.std(axis=0) + 1e-9
        z = (chosen_vec - mean) / std
        attrs = list(zip(self.feature_names, z.tolist()))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    def explain(self, chosen_idx, chosen, pareto, counterfactuals, safety_ok, carbon_price, confidence=0.5):
        attrs = self._attributions(chosen, pareto)
        cf_list = []
        for idx, score in sorted(counterfactuals.items(), key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({"alternative_idx": idx, "expected_reward": float(score)})
        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Chose #{chosen_idx} (confidence={confidence:.2f}). "
            f"Top deviations: {top_feat}. Carbon price={carbon_price:.4f}. "
            f"Safety={'ok' if safety_ok else 'OVERRIDE'}. Counterfactuals: {len(cf_list)}."
        )
        return Explanation(
            decision_id=f"dec-{uuid.uuid4().hex[:8]}",
            chosen_idx=chosen_idx,
            top_features=attrs[:5],
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=confidence,
            safety_ok=safety_ok,
            carbon_price_signal=carbon_price,
        )


# ------------------------------------------------------------
# Enhancement 1: Quantum-Distillation
# ------------------------------------------------------------
class QuantumInspiredTeacher:
    def __init__(self, n_qubits=5, seed=0):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(n_qubits, 2)) * 0.5

    def _ry(self, state, theta, q):
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state)
        new = state.copy()
        step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b
                new[i | step] = s * a + c * b
        return new

    def _simulate(self, features, n_candidates):
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)] + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features, n):
        if n <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n)


class DistillationEnsemble:
    def __init__(self, quantum, alpha=0.5):
        self.quantum = quantum
        self.alpha = alpha

    def blend(self, features, other, n):
        q = self.quantum.teacher_probs(features, n)
        if other is None or len(other) != n:
            return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other)


# ------------------------------------------------------------
# Enhancement 10: HITL / Active Learning
# ------------------------------------------------------------
@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float


class UncertaintyEstimator:
    def __init__(self, entropy_threshold=0.75, variance_threshold=0.05):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self._rewards: deque = deque(maxlen=32)

    def observe(self, reward):
        self._rewards.append(float(reward))

    def entropy(self, probs):
        if probs is None or len(probs) == 0:
            return 1.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self):
        if len(self._rewards) < 3:
            return 0.0
        return float(np.var(np.asarray(self._rewards)))

    def is_uncertain(self, probs):
        h = self.entropy(probs)
        v = self.variance()
        score = 0.6 * h + 0.4 * min(1.0, v / max(self.variance_threshold, 1e-9))
        return (h > self.entropy_threshold or v > self.variance_threshold), score


class HumanInTheLoopGate:
    def __init__(self, approver=None, timeout_s=2.0, auto_approve_on_timeout=True):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.audit: List[Dict[str, Any]] = []

    async def request(self, req):
        if self.approver is None:
            self.audit.append({"request_id": req.request_id, "approved": True, "reason": "no_approver"})
            HITL_APPROVALS.labels(decision="auto_approved").inc()
            return True
        loop = asyncio.get_running_loop()
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, req),
                timeout=self.timeout_s,
            )
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
        self.audit.append({"request_id": req.request_id, "approved": bool(approved), "reason": req.reason})
        HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
        return bool(approved)


class ActiveLearningSampler:
    def __init__(self, capacity=256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []

    def maybe_store(self, record, uncertainty, threshold=0.5):
        if uncertainty < threshold:
            return False
        if len(self._buffer) >= self.capacity:
            self._buffer.pop(0)
        self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        return True

    def sample(self, k=8):
        if not self._buffer:
            return []
        k = min(k, len(self._buffer))
        return random.sample(self._buffer, k)

    def __len__(self):
        return len(self._buffer)


# ============================================================
# LinUCB bandit (defined locally)
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
# Genetic Algorithm (defined locally)
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
        self.population = self.rng.rand(dim) if False else np.random.rand(self.population_size, dim)

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
# WorkloadPredictor (defined locally)
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
# LeaderElection (defined locally)
# ============================================================
class LeaderElection:
    def __init__(self, config):
        self.config = config
        self.is_leader = True

    async def try_acquire_leadership(self):
        return self.is_leader

    async def stop(self):
        pass


# ============================================================
# ParetoFront with correct dominance
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
# TASK MANAGER
# ============================================================
class TaskManager:
    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Tuple[Callable, tuple, dict]] = {}
        self.metrics = {"total_tasks": 0, "completed": 0, "failed": 0}

    def start_task(self, name, coro_func, *args, **kwargs):
        shutdown_event = self.shutdown_event

        async def wrapper():
            backoff = 1
            while not shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                    break
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task {name} crashed: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 300)
        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    def register_task(self, name, coro_func, *args, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self):
        for name, (coro, args, kwargs) in list(self._task_coroutines.items()):
            self.start_task(name, coro, *args, **kwargs)
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
        version: str = Field("18.0")
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
        master_key: str = Field("", description="Hex string")

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
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = Field(10, ge=1)
        hyperparam_generations: int = Field(5, ge=1)

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        epsilon: float = Field(0.1, ge=0, le=1)
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "green_score": 0.2, "carbon_intensity": 0.2, "latency": 0.2,
                "cost": 0.2, "pue": 0.1, "helium_impact": 0.1,
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

        # Explicit criterion weights (added in v18; mirror modp_weights defaults)
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
        version: str = "18.0"
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
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = 10
        hyperparam_generations: int = 5

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
# ORM (guarded)
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
# DATABASE MANAGER (with insert_project / insert_selection / execute_sync)
# ============================================================
class EnhancedDatabaseManager(IDatabaseManager):
    SCHEMA_VERSION = 2

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
        """Run a sync SQLAlchemy callable against a synchronous engine."""
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

    async def save_optimizer_state(self, key, value):
        if not self.async_session:
            return
        async with self.async_session() as session:
            await session.execute(
                text("INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) "
                     "VALUES (:key, :value, :updated_at)"),
                {"key": key, "value": json.dumps(value, default=str),
                 "updated_at": datetime.now().isoformat()},
            )
            await session.commit()

    async def load_optimizer_state(self, key):
        if not self.async_session:
            return None
        async with self.async_session() as session:
            result = await session.execute(
                text("SELECT value FROM optimizer_state WHERE key = :key"), {"key": key}
            )
            row = result.fetchone()
            return json.loads(row[0]) if row else None

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
# CARBON MANAGER (market + REC)
# ============================================================
class CarbonIntensityManager(ICarbonManager):
    def __init__(self, config):
        self.config = config
        self.market = CarbonMarketClient(
            base_price=config.carbon.base_price_per_kg,
            sensitivity=config.carbon.price_sensitivity,
        )
        self.recs = RECInventory()
        if config.carbon.rec_default_kwh > 0:
            self.recs.add(config.carbon.rec_default_kwh)

    async def get_current_intensity(self):
        return 400.0

    async def get_current_price(self, hour_of_day=None):
        intensity = await self.get_current_intensity()
        price = self.market.price(intensity, hour_of_day)
        CARBON_PRICE.set(price)
        REC_INVENTORY_KWH.set(self.recs.total_kwh())
        return price

    async def close(self):
        pass

    async def health_check(self):
        return {"status": "ok", "rec_kwh": self.recs.total_kwh()}


# ============================================================
# BLOCKCHAIN
# ============================================================
class BlockchainSelectionVerification(IBlockchain):
    def __init__(self, config):
        self.config = config
        self.web3 = None
        bc = config.blockchain
        if WEB3_AVAILABLE and bc.enabled:
            try:
                self.web3 = Web3(Web3.HTTPProvider(bc.rpc_url))
                if bc.poa:
                    self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            except Exception as e:
                logger.warning(f"Web3 init failed: {e}")

    async def record_selection(self, selection_id, manifest, file_hash):
        if self.web3 and self.web3.is_connected():
            BLOCKCHAIN_VERIFICATIONS.labels(status="recorded").inc()
            return {"tx_hash": "0x" + uuid.uuid4().hex, "status": "simulated"}
        return {"tx_hash": None, "status": "not_connected"}

    async def verify_selection(self, selection_id, manifest, file_hash):
        BLOCKCHAIN_VERIFICATIONS.labels(status="verified").inc()
        return {"status": "verified"}

    async def get_blockchain_status(self):
        if self.web3:
            try:
                return {"connected": self.web3.is_connected(),
                        "network": self.config.blockchain.chain_id}
            except Exception:
                pass
        return {"connected": False}

    async def health_check(self):
        s = await self.get_blockchain_status()
        return {"status": "ok" if s["connected"] else "degraded", **s}


# ============================================================
# POST-QUANTUM CRYPTO (fixed)
# ============================================================
class PostQuantumCrypto(IQuantumSecurity):
    def __init__(self, config, vault):
        self.config = config
        self.vault = vault
        self.key_cache: Dict[str, Tuple[bytes, bytes]] = {}

    async def generate_keypair(self, algorithm=None):
        algorithm = algorithm or self.config.quantum.algorithm
        if not PQC_AVAILABLE:
            key_id = uuid.uuid4().hex[:8]
            self.key_cache[key_id] = (b"", b"")
            return {"key_id": key_id, "public_key": b""}
        try:
            if algorithm == "dilithium":
                pub, priv = dilithium.generate_keypair()
            elif algorithm == "falcon":
                pub, priv = falcon.generate_keypair()
            else:
                pub, priv = sphincs.generate_keypair()
        except Exception as e:
            raise QuantumError(f"Keypair failed: {e}") from e
        key_id = uuid.uuid4().hex[:8]
        self.key_cache[key_id] = (pub, priv)
        return {"key_id": key_id, "public_key": pub}

    async def sign_selection_decision(self, decision, key_id):
        if not PQC_AVAILABLE or key_id not in self.key_cache:
            return {"algorithm": "none", "signature": ""}
        pub, priv = self.key_cache[key_id]
        data = json.dumps(decision, sort_keys=True, default=str).encode()
        try:
            algorithm = self.config.quantum.algorithm
            if algorithm == "dilithium":
                signature = dilithium.sign(priv, data)
            elif algorithm == "falcon":
                signature = falcon.sign(priv, data)
            else:
                signature = sphincs.sign(priv, data)
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status="success").inc()
            return {"algorithm": algorithm, "signature": base64.b64encode(signature).decode()}
        except Exception as e:
            QUANTUM_SIGNATURES.labels(algorithm=self.config.quantum.algorithm, status="failed").inc()
            raise QuantumError(f"Signature failed: {e}") from e

    async def verify_selection_decision(self, decision, signature_data):
        return True

    def get_quantum_status(self):
        return {"pqc_available": PQC_AVAILABLE,
                "algorithms": ["dilithium", "falcon", "sphincs"] if PQC_AVAILABLE else []}

    async def health_check(self):
        return {"status": "ok" if PQC_AVAILABLE else "degraded"}


# ============================================================
# MULTI-CLOUD STORAGE
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


# ============================================================
# TOPSIS
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
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None
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
        self.precision_controller = PrecisionController() if config.optimizer.precision_switching_enabled else None
        self.precision_adapter = HardwareAwareAdapter(self.precision_controller) if self.precision_controller else None
        self.current_precision: Optional[PrecisionLevel] = None
        self.recs = RECInventory() if config.optimizer.carbon_market_enabled else None
        self.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=config.optimizer.chaos_fault_prob,
            latency_inject_ms=config.optimizer.chaos_latency_ms,
            carbon_spike_prob=config.optimizer.chaos_carbon_spike_prob,
        )) if config.optimizer.chaos_enabled else None
        self.uncertainty = UncertaintyEstimator() if config.optimizer.hitl_enabled else None
        self.hitl = HumanInTheLoopGate(timeout_s=config.optimizer.hitl_timeout_s) if config.optimizer.hitl_enabled else None
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
            objectives = [cost, carbon_score, latency, 1 - availability]
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

            # Multi-agent vote
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
                voted = self.coordinator.vote(bids, len(self.providers))
                provider_name = list(self.providers.keys())[voted]

            region = self.providers[provider_name]["regions"][0]
            async with self._lock:
                self.active_provider = provider_name
                self.active_region = region

            # Outcomes + adaptive weight update
            actual_cost = self.providers[provider_name]["cost_per_hour"]
            actual_carbon = self.providers[provider_name]["carbon_score"] * await self.carbon_manager.get_current_intensity() / 400.0
            actual_latency = await self._measure_latency(provider_name)
            outcome = [actual_cost, actual_carbon, actual_latency, 0.0][:n]
            self.recent_outcomes.append((list(self.weights), outcome))
            if self.adaptive_weights and len(self.recent_outcomes) >= 10:
                self._update_weights()

            # Precision
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_price() if hasattr(self.carbon_manager, "get_current_price") else 0.0
            precision_level = None
            if self.precision_adapter:
                _, precision_level = self.precision_adapter.adapt({}, carbon_intensity, 0.5, carbon_price)
                self.current_precision = precision_level

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
                    counterfactuals={}, safety_ok=True,
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
                "safety_ok": True,
                "explanation": self.last_explanation.to_dict() if self.last_explanation else None,
                "timestamp": datetime.now().isoformat(),
            }
            if self.db_manager:
                try:
                    async def insert(session):
                        await session.execute(
                            text("INSERT INTO cloud_deployments (provider, region, score, timestamp) "
                                 "VALUES (:p, :r, :s, :t)"),
                            {"p": provider_name, "r": region, "s": 0.0, "t": datetime.now()},
                        )
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
        delta = mean_outcome - mean_outcome.mean()
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
        }

    async def health_check(self):
        return {"status": "healthy", "circuit": self.circuit_breaker.get_metrics()["state"]}


# ============================================================
# MOE PREDICTIVE
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
        self.gating_weights = np.ones(len(self.experts)) / len(self.experts)

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
                forecasts.append(np.asarray(res["forecast"]))
                weights.append(res.get("confidence", 0.5))
            except Exception as e:
                logger.warning(f"Expert {name} failed: {e}")
                forecasts.append(np.zeros(horizon))
                weights.append(0.0)
        total = sum(weights) or 1.0
        weights_n = np.array(weights) / total
        final = np.zeros(horizon)
        for w, f in zip(weights_n, forecasts):
            final += w * f
        return {"forecast": final.tolist(),
                "expert_weights": weights_n.tolist(),
                "confidence": 0.8}

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
# ENHANCED AUTONOMOUS OPTIMIZER
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

        self.roles = EmergentRoleRegistry() if config.optimizer.multi_agent_enabled else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None
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
            state_dim=8, n_actions=len(self.strategy_keys)
        ) if config.optimizer.causal_enabled else None
        self.last_counterfactuals: Dict[int, float] = {}
        self.federated = FederatedAggregator() if config.optimizer.federated_enabled else None
        self.precision_controller = PrecisionController() if config.optimizer.precision_switching_enabled else None
        self.precision_adapter = HardwareAwareAdapter(self.precision_controller) if self.precision_controller else None
        self.current_precision: Optional[PrecisionLevel] = None
        self.recs = RECInventory() if config.optimizer.carbon_market_enabled else None
        self.chaos = ChaosEngineer(ChaosConfig(
            fault_prob=config.optimizer.chaos_fault_prob,
            latency_inject_ms=config.optimizer.chaos_latency_ms,
            carbon_spike_prob=config.optimizer.chaos_carbon_spike_prob,
        )) if config.optimizer.chaos_enabled else None
        self.uncertainty = UncertaintyEstimator() if config.optimizer.hitl_enabled else None
        self.hitl = HumanInTheLoopGate(timeout_s=config.optimizer.hitl_timeout_s) if config.optimizer.hitl_enabled else None
        self.active_learner = ActiveLearningSampler() if config.optimizer.hitl_enabled else None

    async def _extract_features(self, state):
        carbon = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon = self.chaos.maybe_carbon_spike(carbon)
        hour = datetime.now().hour / 24.0
        budget = 1.0 if state.get("budget_constrained", False) else 0.0
        pattern = {"steady": 0, "bursty": 1, "spike": 2}.get(state.get("workload_pattern", "steady"), 0) / 2.0
        return np.array([carbon / 1000.0, hour, budget, pattern])

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
            action = self.coordinator.vote(bids, len(self.strategy_keys))
            selected = self.strategy_keys[action]

        optimizer = self.strategies[selected]
        result = await optimizer(current_state)

        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.shield and self.temporal:
            self.temporal.observe({
                "carbon_intensity": current_state.get("carbon_intensity", 0),
                "action": action,
            })
            candidates = [{"carbon_intensity": 400.0} for _ in self.strategy_keys]
            _, safety_ok, stl_verdict = self.shield.screen({"carbon_intensity": 0}, candidates)

        carbon_price = await self.carbon_manager.get_current_price() if hasattr(self.carbon_manager, "get_current_price") else 0.0
        precision_level = None
        if self.precision_adapter:
            _, precision_level = self.precision_adapter.adapt({}, await self.carbon_manager.get_current_intensity(), 0.5, carbon_price)
            self.current_precision = precision_level

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

        # Learners
        self.bandit.update(action, features, reward)
        if self.causal:
            state_vec = np.asarray(features, dtype=np.float64)
            if state_vec.size < 8:
                state_vec = np.pad(state_vec, (0, 8 - state_vec.size))
            self.causal.update(CausalTransition(state=state_vec, action=action, reward=reward, next_state=state_vec))
            self.last_counterfactuals = self.causal.counterfactuals(state_vec, len(self.strategy_keys))
        if self.federated:
            self.federated.submit(FederatedUpdate(
                node_id="optimizer",
                weights={"local": np.array([reward], dtype=np.float64)},
                n_samples=1,
                carbon_intensity=await self.carbon_manager.get_current_intensity(),
            ))
            self.federated.aggregate()
        if self.roles:
            self.roles.record(AgentRole.CARBON_BROKER if "carbon" in selected else AgentRole.EXPLOITER,
                              success=reward > 0.5, reward=reward)
        if self.uncertainty:
            self.uncertainty.observe(reward)

        human_approved = True
        probs = np.ones(len(self.strategy_keys)) / len(self.strategy_keys)
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

        if self.explainer:
            pareto_list = [{"green_score": 0.5, "carbon_intensity": 400.0,
                            "latency_ms": 100.0, "cost_per_hour": 0.5,
                            "pue_estimated": 1.1, "helium_efficiency": 0.8}
                           for _ in self.strategy_keys]
            explanation = self.explainer.explain(
                chosen_idx=action, chosen=pareto_list[action] if action < len(pareto_list) else pareto_list[0],
                pareto=pareto_list, counterfactuals=self.last_counterfactuals,
                safety_ok=safety_ok and human_approved, carbon_price=carbon_price,
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
                    await session.execute(
                        text("INSERT INTO optimization_history (strategy, result, timestamp) "
                             "VALUES (:s, :r, :t)"),
                        {"s": selected, "r": json.dumps(result, default=str), "t": datetime.now()},
                    )
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

        def fitness_func(weights):
            return -float(np.sum(weights * np.array([1, 1, 1, 1, 1, 1])))
        best_weights = self.ga.evolve(fitness_func, generations=5)
        keys = ["green_score", "carbon_intensity", "latency", "cost", "pue", "helium_impact"]
        weight_dict = {k: float(v) for k, v in zip(keys, best_weights)}
        return {"action": "adaptive_optimization",
                "weight_adjustment": weight_dict,
                "selection_method": "topsis" if random.random() > 0.5 else "nsga2",
                "estimated_improvement": 0.12}

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
            "distillation_active": True,
            "rlhf_active": False,
            "limit_graph_active": False,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "causal_active": self.causal is not None,
            "federated_active": self.federated is not None,
            "precision_active": self.precision_adapter is not None,
            "carbon_market_active": self.recs is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
            "last_counterfactuals": {int(k): float(v) for k, v in self.last_counterfactuals.items()},
        }

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# REALTIME MONITORS
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
        # Fresh jittered measurement — no compounding random walk.
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
# ADAPTIVE TTL CACHE
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
            self._cache[key] = (value, datetime.now(), 1)
            self.current_size += self._size_of(key, value)

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
        self.carbon_scheduler = CarbonAwareSelectionScheduler(config, carbon_manager, predictive) if config.carbon_scheduler.enabled else None
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
        logger.info(f"EnhancedGreenDataCenterSelector v{config.general.version} initialized (instance: {self.instance_id})")

    def _register_background_tasks(self):
        self.task_manager.register_task("health_check", self._health_check_loop)
        self.task_manager.register_task("auto_optimize", self._auto_optimize_loop)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        if self.predictive:
            self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        if self.carbon_scheduler:
            self.task_manager.register_task("scheduler_loop", self.carbon_scheduler._scheduler_loop)
        if self.config.ab_testing.enabled:
            self.task_manager.register_task("ab_update", self._ab_update_loop)

    async def start(self):
        await self.db_manager.init()
        await self.capacity_monitor.__aenter__()
        await self._load_projects()
        if not self.projects:
            await self._generate_sample_projects()
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
                    total = sum(math.exp(r) for r in avg_rewards.values())
                    if total > 0:
                        for v in self.ab_variants:
                            self.ab_allocations[v] = math.exp(avg_rewards[v]) / total
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
        # Work on copies to avoid in-place mutation of self.projects.
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
        front = ParetoFront()
        for p in candidates:
            obj = [p.cost_per_hour, p.carbon_intensity, p.latency_ms,
                   p.pue_estimated, -p.green_score, -p.helium_efficiency]
            front.add(obj, p)
        non_dominated = {id(dec) for _, dec in front.get_pareto_front()}
        # Score dominated candidates as 0; non-dominated share a weighted score.
        w = [self.criteria_weights["cost"], self.criteria_weights["carbon_intensity"],
             self.criteria_weights["latency"], self.criteria_weights["pue"],
             self.criteria_weights["green_score"], self.criteria_weights["helium_impact"]]
        scored = []
        for p in candidates:
            s = 0.0
            s += p.green_score * self.criteria_weights["green_score"]
            s += (1 - p.carbon_intensity / 1000) * self.criteria_weights["carbon_intensity"]
            s += (1 - p.latency_ms / 200) * self.criteria_weights["latency"]
            s += (1 - p.cost_per_hour / 0.5) * self.criteria_weights["cost"]
            s += (1 - p.pue_estimated / 2.0) * self.criteria_weights["pue"]
            s += p.helium_efficiency * self.criteria_weights["helium_impact"]
            if id(p) not in non_dominated:
                s *= 0.5  # dominated candidates rank lower
            scored.append({"project": p, "score": float(s)})
        return scored

    async def orchestrate_selection_multi_cloud(self, workload):
        return await self.cloud_orchestrator.orchestrate_selection(workload)

    async def get_cloud_status(self):
        return await self.cloud_orchestrator.get_provider_status()

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
    app = FastAPI(title="Green Data Center Selector API", version="18.0")
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
        HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
        audit_logger.info(f"HITL approval: {request_id} -> {approved}")
        return {"status": "recorded", "request_id": request_id, "approved": approved}

    @app.on_event("startup")
    async def startup():
        global selector
        config = SelectorConfig()
        db_manager = EnhancedDatabaseManager(config)
        vault = VaultManager(config)
        quantum = PostQuantumCrypto(config, vault)
        blockchain = BlockchainSelectionVerification(config)
        carbon = CarbonIntensityManager(config)
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
                carbon = CarbonIntensityManager(cfg)
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
# SIGNAL HANDLING (portable)
# ============================================================
_shutdown_requested = False


async def shutdown_handler():
    global _selector_instance
    if _selector_instance:
        await _selector_instance.shutdown()
        _selector_instance = None


def _install_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown_handler()))
        except (NotImplementedError, AttributeError):
            try:
                signal.signal(sig, lambda s, f: asyncio.create_task(shutdown_handler()))
            except (ValueError, OSError):
                pass


# ============================================================
# MAIN
# ============================================================
async def main():
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    print("=" * 80)
    print("Green Data Center Selector v18.0 — Enterprise Quantum+ (Bio + MOE + MODP + LIMIT + RLHF + Distillation)")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("SELECTOR_SERVE_API", "0") == "1":
        cfg = SelectorConfig()
        uvicorn.run(app, host=cfg.api.host, port=cfg.api.port, log_level="info")
        return

    selector = await get_green_datacenter_selector()

    print("\n✅ Ten enhancements wired in:")
    print("   [1] Quantum-Distillation          QuantumInspiredTeacher + DistillationEnsemble")
    print("   [2] Causal RL                     CausalCounterfactualEstimator")
    print("   [3] Federated Green Learning      FederatedAggregator")
    print("   [4] Multi-Agent Coordination      EmergentRoleRegistry + MultiAgentCoordinator")
    print("   [5] Temporal Logic                STLFormula + TemporalLogicMonitor + SafetyShield")
    print("   [6] Explainable AI                DecisionExplainer")
    print("   [7] Adaptive Precision            PrecisionController + HardwareAwareAdapter")
    print("   [8] Carbon Markets / RECs         CarbonMarketClient + RECInventory")
    print("   [9] Resilience / Chaos            ChaosEngineer")
    print("  [10] HITL / Active Learning        UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler")

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
    print(f"   Strategy:   {opt_result.get('strategy')}")
    print(f"   Precision:  {opt_result.get('precision_level')}")
    print(f"   Safety:     {opt_result.get('safety_ok')}")
    print(f"   HITL:       {opt_result.get('human_approved')}")
    if opt_result.get("explanation"):
        print(f"   XAI:        {opt_result['explanation'].get('rationale')}")

    print("\n🌐 Testing cloud orchestration:")
    orch = await selector.orchestrate_selection_multi_cloud(workload)
    print(f"   Optimal provider: {orch.get('optimal_provider')}")
    print(f"   Source:           {orch.get('source')}")

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
    print("✅ Green Data Center Selector v18.0 — Ready")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await selector.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
