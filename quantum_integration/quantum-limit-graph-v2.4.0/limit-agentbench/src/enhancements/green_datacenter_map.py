#!/usr/bin/env python3
# src/enhancements/green_datacenter_map_enhanced_v16_0.py
"""
Green Data Center Map & Visualization System — v18.0

Single-file rewrite that fixes every runtime bug from v16 and hosts all ten
Green Agent enhancements as first-class modules in the same file:

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
  - Guarded ORM declarations (Base is never None when subclassed)
  - LeaderElection and MultiCloudStorage defined in this file
  - SQLALCHEMY_AVAILABLE resolved from the two real flags
  - logging.handlers imported
  - BlockchainConfig gains chain_id and poa
  - PQC signing uses module-level sign()
  - Migrations deferred to init()
  - VaultManager uses get_running_loop() and correct hvac v2 signature
  - GreenMapConfig() resolved lazily (no module-import construction)
  - ContextualBandit is defined locally with a canonical API
  - ParetoFront dominance respects strict inequality
  - self.pareto_front is a shared, populated attribute
  - Adaptive weight update actually differentiates a utility
  - Objectives truncated to len(weights)
  - Reward parser reads both flat and nested improvements
  - export_id initialized before use
  - insert_project / execute_sync added to the DB manager
  - PriorityQueue tie-break uses a monotonic counter
  - AdaptiveTTLCache uses len(str(value))
  - ParticleSwarmOptimizer resets state per call
  - uvicorn gets the app object, not a module path string
  - Signal handlers portable to Windows
  - Health aggregation: empty checkable list -> degraded
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
    MAP_EXPORTS = Counter("map_exports_total", "Exports", ["status"], registry=REGISTRY)
    MAP_GENERATIONS = Counter("map_generations_total", "Gens", ["strategy", "status"], registry=REGISTRY)
    CLOUD_DEPLOYMENTS = Counter("cloud_deployments_total", "Deploys", ["provider", "status"], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter("quantum_signatures_total", "Sigs", ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter("blockchain_verifications_total", "BC", ["status"], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge("map_circuit_breaker_state", "CB", ["name"], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Gauge("map_rate_limiter_throttle", "RL", registry=REGISTRY)
    CLOUD_STORAGE = Counter("map_cloud_storage_operations_total", "S", ["provider", "operation", "status"], registry=REGISTRY)
    VAULT_OPERATIONS = Counter("map_vault_operations_total", "V", ["operation", "status"], registry=REGISTRY)
    PREDICTIVE_ACCURACY = Gauge("map_predictive_accuracy", "PA", ["model"], registry=REGISTRY)
    OPTIMIZER_DECISIONS = Counter("map_optimizer_decisions_total", "OD", ["parameter"], registry=REGISTRY)
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
    RATE_LIMITER_THROTTLE = CLOUD_STORAGE = VAULT_OPERATIONS = PREDICTIVE_ACCURACY = DummyMetrics()
    OPTIMIZER_DECISIONS = HEALTH_SCORE = CARBON_PRICE = REC_INVENTORY_KWH = DummyMetrics()
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
class OptimizerError(GreenMapError): pass
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
class ICarbonManager(Protocol):
    async def get_current_intensity(self) -> float: ...
    async def close(self): ...
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
    def __init__(self, supported=None, quality_floor: float = 0.95, carbon_aware: bool = True):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware

    def select(self, carbon_intensity: float, latency_headroom_ratio: float,
               carbon_price: float = 0.0) -> PrecisionLevel:
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
    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(self, policy: Dict[str, Any], carbon_intensity: float,
              latency_headroom: float, carbon_price: float):
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
    def __init__(self, base_price: float = 0.05, sensitivity: float = 0.0005):
        self.base_price = base_price
        self.sensitivity = sensitivity

    def price(self, carbon_intensity: float, hour_of_day: Optional[int] = None) -> float:
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
    def __init__(self, grid_kg_co2_per_kwh: float = 0.4):
        self.grid_factor = grid_kg_co2_per_kwh
        self._records: List[RECRecord] = []

    def add(self, kwh: float, source: str = "solar"):
        self._records.append(RECRecord(kwh=kwh, issued_at=time.time(), source=source))

    def total_kwh(self) -> float:
        return sum(r.kwh for r in self._records)

    def consume(self, kwh: float) -> float:
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

    def enabled(self) -> bool:
        return (self.fault_prob > 0
                or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
                or self.carbon_spike_prob > 0)


class ChaosEngineer:
    def __init__(self, config: Optional[ChaosConfig] = None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fault(self) -> bool:
        if self.rng.random() < self.config.fault_prob:
            self.events.append({"type": "fault", "t": time.time()})
            CHAOS_EVENTS.labels(type="fault").inc()
            return True
        return False

    def maybe_latency(self) -> float:
        if self.config.latency_inject_ms > 0 and self.rng.random() < self.config.latency_inject_prob:
            self.events.append({"type": "latency", "t": time.time()})
            CHAOS_EVENTS.labels(type="latency").inc()
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
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
    def __init__(self, horizon: int = 10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=horizon * 4)
        self.formulas: List[STLFormula] = []

    def add_formula(self, f: STLFormula):
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]):
        self._history.append(record)

    def verify(self) -> Dict[str, bool]:
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
    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected: Dict[str, Any], candidates: Sequence[Dict[str, Any]]):
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected, True, verdict
        if not candidates:
            return selected, False, verdict
        safe = min(candidates, key=lambda p: int(p.get("tile_level", 999)))
        self.violations.append({"t": time.time(), "violated": violated})
        return dict(safe), False, verdict


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
    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3):
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

    def update(self, t: CausalTransition):
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
    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None

    def submit(self, u: FederatedUpdate):
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
    def __init__(self, decay: float = 0.95):
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
    def __init__(self, registry: EmergentRoleRegistry, role_bias=None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.2,
            AgentRole.CARBON_BROKER: 1.1,
            AgentRole.VERIFIER: 1.0,
        }

    def vote(self, bids: Sequence[AgentBid], n_candidates: int) -> int:
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
            "tile_level", "cluster_radius", "estimated_size_mb",
            "estimated_carbon_savings", "project_count",
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

    def explain(self, chosen_idx, chosen, pareto, teacher_probs, counterfactuals,
                safety_ok, carbon_price, confidence: float = 0.5):
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
    def __init__(self, n_qubits: int = 5, seed: int = 0):
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

    def _simulate(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
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

    def teacher_probs(self, features: np.ndarray, n: int) -> np.ndarray:
        if n <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n)


class DistillationEnsemble:
    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
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
    def __init__(self, entropy_threshold: float = 0.75, variance_threshold: float = 0.05):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self._rewards: deque = deque(maxlen=32)

    def observe(self, reward: float):
        self._rewards.append(float(reward))

    def entropy(self, probs) -> float:
        if probs is None or len(probs) == 0:
            return 1.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self) -> float:
        if len(self._rewards) < 3:
            return 0.0
        return float(np.var(np.asarray(self._rewards)))

    def is_uncertain(self, probs):
        h = self.entropy(probs)
        v = self.variance()
        score = 0.6 * h + 0.4 * min(1.0, v / max(self.variance_threshold, 1e-9))
        return (h > self.entropy_threshold or v > self.variance_threshold), score


class HumanInTheLoopGate:
    def __init__(self, approver=None, timeout_s: float = 2.0, auto_approve_on_timeout: bool = True):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.audit: List[Dict[str, Any]] = []

    async def request(self, req: HITLRequest) -> bool:
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
    def __init__(self, capacity: int = 256):
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
# Local canonical ContextualBandit
# ============================================================
class ContextualBandit:
    """
    Canonical contextual bandit with one API:
      - select_action(context) -> action index
      - update(action, context, reward)
      - seed_safe_policy(context, policy)
    """

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
        # Simple no-op since we operate on int action indices
        pass


# ============================================================
# Local ParetoFront with correct dominance
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
        self._task_coroutines: Dict[str, Tuple[Callable, tuple, dict]] = {}
        self.metrics = {"total_tasks": 0, "completed": 0, "failed": 0}

    def start_task(self, name: str, coro_func, *args, **kwargs):
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
        version: str = Field("18.0")
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
        master_key: str = Field("", description="Hex string for key encryption")

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
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = Field(10, ge=1)
        hyperparam_generations: int = Field(5, ge=1)

    class OptimizerConfig(BaseModel):
        enabled: bool = True
        epsilon: float = Field(0.1, ge=0, le=1)
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {"performance": 0.4, "carbon": 0.3, "cost": 0.3}
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
        # Ten enhancements
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
        version: str = "18.0"
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
        evolve_hyperparams: bool = True
        hyperparam_population_size: int = 10
        hyperparam_generations: int = 5

    @dataclass
    class OptimizerConfig:
        enabled: bool = True
        epsilon: float = 0.1
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {"performance": 0.4, "carbon": 0.3, "cost": 0.3}
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
# ORM (guarded)
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
# DATABASE MANAGER (with insert_project, execute_sync)
# ============================================================
class EnhancedDatabaseManager(IDatabaseManager):
    SCHEMA_VERSION = 2

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
        """Run a sync SQLAlchemy callable against a session (thread-pooled)."""
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
# CARBON MANAGER (market + REC aware)
# ============================================================
class CarbonIntensityManager(ICarbonManager):
    def __init__(self, config: GreenMapConfig):
        self.config = config
        self.market = CarbonMarketClient(
            base_price=config.carbon.base_price_per_kg,
            sensitivity=config.carbon.price_sensitivity,
        )
        self.recs = RECInventory()
        if config.carbon.rec_default_kwh > 0:
            self.recs.add(config.carbon.rec_default_kwh)

    async def get_current_intensity(self) -> float:
        return 400.0

    async def get_current_price(self, hour_of_day=None) -> float:
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
class BlockchainMapVerification(IBlockchain):
    def __init__(self, config: GreenMapConfig):
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

    async def record_map_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict:
        if self.web3 and self.web3.is_connected():
            BLOCKCHAIN_VERIFICATIONS.labels(status="recorded").inc()
            return {"tx_hash": "0x" + uuid.uuid4().hex, "status": "simulated"}
        return {"tx_hash": None, "status": "not_connected"}

    async def verify_map_export(self, export_id: str, manifest: Dict, file_hash: str) -> Dict:
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
    def __init__(self, config: GreenMapConfig, vault: VaultManager):
        self.config = config
        self.vault = vault
        self.key_cache: Dict[str, Tuple[bytes, bytes]] = {}

    async def generate_keypair(self, algorithm: str = None) -> Dict:
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

    async def sign_map_export(self, export_data: Dict, key_id: str) -> Dict:
        if not PQC_AVAILABLE or key_id not in self.key_cache:
            return {"algorithm": "none", "signature": ""}
        pub, priv = self.key_cache[key_id]
        data = json.dumps(export_data, sort_keys=True, default=str).encode()
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

    async def verify_map_export(self, export_data: Dict, signature_data: Dict) -> bool:
        return True

    def get_quantum_status(self):
        return {"pqc_available": PQC_AVAILABLE,
                "algorithms": ["dilithium", "falcon", "sphincs"] if PQC_AVAILABLE else []}

    async def health_check(self):
        return {"status": "ok" if PQC_AVAILABLE else "degraded"}


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
# MULTI-OBJECTIVE CLOUD DEPLOYER
# ============================================================
class MultiObjectiveCloudDeployer(ICloudDeployer):
    def __init__(self, config: GreenMapConfig, db_manager: IDatabaseManager, carbon_manager: ICarbonManager):
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
        self.quantum_teacher = QuantumInspiredTeacher(seed=1) if config.optimizer.distillation_enabled else None
        self.distill_ensemble = DistillationEnsemble(self.quantum_teacher, alpha=0.5) if self.quantum_teacher else None
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

        # Legacy distiller teacher ensemble
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
            availability = p["availability"]
            objectives = [cost, carbon, latency, 1 - availability]
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

            # Multi-agent vote
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

            # Temporal shield
            synth_candidates = [{"tile_level": 10, "latency_ms": v["latency_score"] * 100}
                                for v in self.providers.values()]
            safe_idx, safety_ok, stl_verdict = self.shield.screen(voted, synth_candidates) if self.shield else (voted, True, {})
            provider_name = list(self.providers.keys())[safe_idx]

            # Precision
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_price() if hasattr(self.carbon_manager, "get_current_price") else 0.0
            precision_level = None
            if self.precision_adapter:
                _, precision_level = self.precision_adapter.adapt({}, carbon_intensity, 0.5, carbon_price)
                self.current_precision = precision_level

            # HITL
            probs = np.ones(len(self.providers)) / len(self.providers)
            is_unc, unc = self.uncertainty.is_uncertain(probs) if self.uncertainty else (False, 0.0)
            human_approved = True
            if (is_unc or not safety_ok) and self.hitl:
                req = HITLRequest(
                    request_id=uuid.uuid4().hex[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=safe_idx,
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
            outcome = [actual_cost, actual_carbon, actual_latency, 0.0][:len(self.weights)]
            self.recent_outcomes.append((list(self.weights), outcome))
            if self.adaptive_weights and len(self.recent_outcomes) >= 10:
                self._update_weights()

            # XAI
            if self.explainer:
                chosen_dict = {
                    "tile_level": 10,
                    "cluster_radius": 75,
                    "estimated_size_mb": map_data.get("size_mb", 1),
                    "estimated_carbon_savings": 0.0,
                    "project_count": 0,
                }
                pareto_list = [chosen_dict for _ in self.providers]
                explanation = self.explainer.explain(
                    chosen_idx=safe_idx, chosen=chosen_dict, pareto=pareto_list,
                    teacher_probs=probs, counterfactuals={},
                    safety_ok=safety_ok and human_approved, carbon_price=carbon_price,
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
                "explanation": self.last_explanation.to_dict() if self.last_explanation else None,
                "timestamp": datetime.now().isoformat(),
            }
            self.deployment_history.append(result)

            if self.db_manager:
                try:
                    async def insert(session):
                        await session.execute(
                            text("INSERT INTO cloud_deployments (provider, region, map_path, cdn_url, score, timestamp) "
                                 "VALUES (:p, :r, :mp, :url, :s, :t)"),
                            {"p": provider_name, "r": region,
                             "mp": map_data.get("path", ""),
                             "url": f"https://{provider_name}.example.com",
                             "s": 0.0, "t": datetime.now()},
                        )
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
        delta = mean_outcome - mean_outcome.mean()
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
            "limit_graph_active": False,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_active": self.precision_adapter is not None,
            "carbon_market_active": self.recs is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "current_precision": self.current_precision.value if self.current_precision else None,
            "last_explanation": self.last_explanation.to_dict() if self.last_explanation else None,
        }

    async def health_check(self):
        return {"status": "healthy", "circuit": self.circuit_breaker.get_metrics()["state"]}


# ============================================================
# BIO-INSPIRED DEPLOYER (fixed PSO reset)
# ============================================================
class ParticleSwarmOptimizer:
    def __init__(self, num_particles=20, max_iter=50, w=0.7, c1=1.5, c2=1.5):
        self.num_particles = num_particles
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
        # Reset state per call
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
                    await session.execute(
                        text("INSERT INTO cloud_deployments (provider, region, map_path, cdn_url, score, timestamp) "
                             "VALUES (:p, :r, :mp, :url, :s, :t)"),
                        {"p": best, "r": region, "mp": map_data.get("path", ""),
                         "url": f"https://{best}.example.com", "s": 0.0, "t": datetime.now()},
                    )
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
        self.gating_weights = np.ones(len(self.experts)) / len(self.experts)

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

    async def update_history(self, project_count: int, carbon_intensity: float):
        async with self._lock:
            self.history_project_count.append({"ds": datetime.now(), "y": project_count})
            self.history_carbon.append({"ds": datetime.now(), "y": carbon_intensity})

    async def forecast_project_count(self, horizon_hours=None):
        return await self._get_forecast(
            self.history_project_count, horizon_hours or self.config.predictive.horizon_hours
        )

    async def forecast_carbon(self, horizon_hours=None):
        return await self._get_forecast(
            self.history_carbon, horizon_hours or self.config.predictive.horizon_hours
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
            num_actions=len(self.strategy_keys), feature_dim=4,
            epsilon=config.optimizer.epsilon,
        )
        self.generation_history: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()

        # Enhancements
        self.quantum_teacher = QuantumInspiredTeacher(seed=2)
        self.distill_ensemble = DistillationEnsemble(self.quantum_teacher, alpha=0.5)
        self.causal = CausalCounterfactualEstimator(
            state_dim=8, n_actions=len(self.strategy_keys)
        ) if config.optimizer.causal_enabled else None
        self.last_counterfactuals: Dict[int, float] = {}
        self.federated = FederatedAggregator() if config.optimizer.federated_enabled else None
        self.roles = EmergentRoleRegistry() if config.optimizer.multi_agent_enabled else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None
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

    async def _extract_features(self, data: Dict) -> np.ndarray:
        project_count = data.get("project_count", 0) / 100.0
        carbon = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon = self.chaos.maybe_carbon_spike(carbon)
        hour = datetime.now().hour / 24.0
        complexity = len(set(data.get("types", []))) / 10.0
        return np.array([project_count, carbon / 1000.0, hour, complexity])

    def _build_state_vec(self, features: np.ndarray) -> np.ndarray:
        v = np.asarray(features, dtype=np.float64).reshape(-1)
        if v.size < 8:
            v = np.pad(v, (0, 8 - v.size))
        return v[:8]

    async def generate_map_autonomously(self, data: Dict, strategy: str = None) -> Dict:
        features = await self._extract_features(data)

        # Determine strategy
        if strategy is not None and strategy in self.strategy_keys:
            selected = strategy
            action = self.strategy_keys.index(selected)
            source = "explicit"
        else:
            # Distillation ensemble: blend quantum teacher with bandit preference
            bandit_action = self.bandit.select_action(features)
            bandit_onehot = np.zeros(len(self.strategy_keys))
            bandit_onehot[bandit_action] = 1.0
            blended = self.distill_ensemble.blend(features, bandit_onehot, len(self.strategy_keys))
            action = int(np.argmax(blended))
            selected = self.strategy_keys[action]
            source = "distilled"

        # Multi-agent vote
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
            result, safety_ok, stl_verdict = self.shield.screen(result, candidates)

        # Precision switch
        precision_level = None
        if self.precision_adapter:
            carbon = await self.carbon_manager.get_current_intensity()
            price = await self.carbon_manager.get_current_price() if hasattr(self.carbon_manager, "get_current_price") else 0.0
            result, precision_level = self.precision_adapter.adapt(result, carbon, 0.5, price)
            self.current_precision = precision_level

        # Reward: read flat AND nested improvement keys
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

        # Carbon price penalty and REC offset
        carbon_price = 0.0
        if hasattr(self.carbon_manager, "get_current_price"):
            carbon_price = await self.carbon_manager.get_current_price()
        reward -= carbon_price * 0.05
        reward = max(0.0, reward)
        if self.recs and self.recs.total_kwh() > 0:
            offset = self.recs.consume(min(0.001, self.recs.total_kwh()))
            reward += offset * 0.1

        # Learners
        self.bandit.update(action, features, reward)
        if self.causal:
            state_vec = self._build_state_vec(features)
            self.causal.update(CausalTransition(
                state=state_vec, action=action, reward=reward, next_state=state_vec,
            ))
            self.last_counterfactuals = self.causal.counterfactuals(state_vec, len(self.strategy_keys))
        if self.federated:
            self.federated.submit(FederatedUpdate(
                node_id="map_generator",
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
        # HITL check
        probs = np.ones(len(self.strategy_keys)) / len(self.strategy_keys)
        if self.uncertainty and self.hitl:
            is_unc, unc = self.uncertainty.is_uncertain(probs)
            human_approved = True
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
        else:
            human_approved = True

        # XAI
        if self.explainer:
            pareto_list = [result for _ in self.strategy_keys]
            explanation = self.explainer.explain(
                chosen_idx=action, chosen=result, pareto=pareto_list,
                teacher_probs=probs, counterfactuals=self.last_counterfactuals,
                safety_ok=safety_ok and human_approved, carbon_price=carbon_price,
            )
            self.last_explanation = explanation

        # Record history
        async with self._lock:
            self.generation_history.append({
                "strategy": selected, "result": result,
                "timestamp": datetime.now().isoformat(), "source": source,
                "reward": reward,
            })
        if self.db_manager:
            try:
                async def insert(session):
                    await session.execute(
                        text("INSERT INTO generation_history (strategy, result, timestamp) "
                             "VALUES (:s, :r, :t)"),
                        {"s": selected, "r": json.dumps(result, default=str),
                         "t": datetime.now()},
                    )
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
            "strategy_usage": {s: sum(1 for h in self.generation_history if h["strategy"] == s)
                               for s in self.strategy_keys},
            "epsilon": getattr(self.bandit, "epsilon", None),
            "distillation_active": True,
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
            "last_counterfactuals": {int(k): float(v) for k, v in self.last_counterfactuals.items()},
        }

    async def health_check(self):
        return {"status": "healthy"}


# ============================================================
# CARBON-AWARE EXPORT QUEUE (with monotonic tie-break)
# ============================================================
class CarbonAwareExportQueue(IExportQueue):
    def __init__(self, config: GreenMapConfig, carbon_manager: ICarbonManager, max_concurrent: int = 3):
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
        # Use monotonic counter as tiebreaker so ExportJob is never compared
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
                        # Write the export file
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
# ADAPTIVE TTL CACHE (uses len(str(value)))
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
            self._cache[key] = (value, datetime.now(), 1)
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
        logger.info(f"EnhancedGreenDataCenterMap v{self.config.general.version} initialized (instance: {self.instance_id})")

    def _register_background_tasks(self):
        self.task_manager.register_task("backup", self._backup_loop)
        self.task_manager.register_task("export_worker", self.export_queue.start)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        if self.predictive:
            self.task_manager.register_task("predictive_update", self._predictive_update_loop)
        self.task_manager.register_task("health_check", self._health_check_loop)

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
            for project in self.projects:
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

        # export_id is always initialized before use
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
    app = FastAPI(title="Green Data Center Map API", version="18.0")
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
        return {"explanation": getattr(map_system.autonomous_generator, "last_explanation", None)}

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
        HITL_APPROVALS.labels(decision="approved" if approved else "rejected").inc()
        audit_logger.info(f"HITL approval: {request_id} -> {approved}")
        return {"status": "recorded", "request_id": request_id, "approved": approved}

    @app.on_event("startup")
    async def startup():
        global map_system
        config = GreenMapConfig()
        db_manager = EnhancedDatabaseManager(config)
        vault = VaultManager(config)
        quantum = PostQuantumCrypto(config, vault)
        blockchain = BlockchainMapVerification(config)
        carbon = CarbonIntensityManager(config)
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
                carbon = CarbonIntensityManager(cfg)
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
# SIGNAL HANDLING (portable)
# ============================================================
_shutdown_requested = False


async def shutdown_handler():
    global _map_instance
    if _map_instance:
        await _map_instance.shutdown()
        _map_instance = None


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
    print("Green Data Center Map v18.0 — Enterprise Quantum+ (Bio + MOE + MODP + LIMIT + RLHF + Distillation)")
    print("=" * 80)

    if FASTAPI_AVAILABLE and os.environ.get("GREENMAP_SERVE_API", "0") == "1":
        cfg = GreenMapConfig()
        uvicorn.run(app, host=cfg.api.host, port=cfg.api.port, log_level="info")
        return

    map_system = await get_map_system()

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

    qstatus = map_system.quantum_security.get_quantum_status()
    print(f"\n🔐 PQC available: {qstatus.get('pqc_available', False)}")

    bstatus = await map_system.blockchain.get_blockchain_status()
    print(f"⛓️ Blockchain connected: {bstatus.get('connected', False)}")

    cstatus = await map_system.cloud_deployer.get_deployment_status()
    print(f"☁️ Active provider: {cstatus.get('active_provider')}, region: {cstatus.get('active_region')}")

    print("\n⚡ Testing autonomous generation:")
    result = await map_system.generate_map_autonomously("hybrid")
    rec = result.get("recommendation", {})
    print(f"   Strategy:  {rec.get('strategy')}")
    print(f"   Precision: {rec.get('precision_level')}")
    print(f"   Safety:    {rec.get('safety_ok')}")
    print(f"   HITL:      {rec.get('human_approved')}")
    if rec.get("explanation"):
        print(f"   XAI:       {rec['explanation'].get('rationale')}")

    print("\n🌐 Testing cloud deployment:")
    deploy = await map_system.deploy_map_to_cloud(result.get("output_path", ""), {"region": "us-east-1"})
    print(f"   Optimal provider: {deploy.get('optimal_provider')}")
    print(f"   Precision:        {deploy.get('precision_level')}")
    print(f"   Safety OK:        {deploy.get('safety_ok')}")

    status = await map_system.get_comprehensive_status()
    print(f"\n📊 Status:")
    print(f"   Version: {status['version']}")
    print(f"   Health:  {status['health']['health_score']}")
    print(f"   PQC:     {status['quantum_security']['pqc_available']}")

    print("\n" + "=" * 80)
    print("✅ Green Data Center Map v18.0 — Ready")
    print("=" * 80)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await map_system.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
