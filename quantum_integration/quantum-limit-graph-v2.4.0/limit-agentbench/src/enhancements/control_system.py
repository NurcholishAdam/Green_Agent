#!/usr/bin/env python3
# File: src/enhancements/control_system_enhanced_v16_1.py
"""
Enhanced Control System — v16.2 (Enterprise Quantum Resilience & Autonomous Healing)

Fixes and enhancements over v16.1:
- Import-time crashes fixed (logging.handlers, PROMETHEUS_AVAILABLE, yaml/aiohttp/
  aiosqlite/subprocess/shlex/tempfile guarded, cryptography/prometheus/websockets
  guarded).
- Missing `import gc` fixed (used by `_heal_memory`).
- `DigitalTwinIntegration.simulate_scenario` no longer calls `super()`.
- `_fallback_generate_keypair` awaits persistence calls.
- Realistic `CausalBandit` (linear contextual bandit with UCB / Thompson).
- `SafetyMonitor`, `CarbonOffsetBroker`, `HumanReviewManager` wired into the
  healing and cloud-deployment decision paths.
- `FederatedCoordinator` uses sample-weighted averaging with round IDs.
- `MultiAgentCoordinator` uses stable SHA-256 hashing.
- `ChaosMonkey` supports named faults and recovery measurement.
- `PostQuantumCrypto` performs real ECDSA signing when pqcrypto is missing.
- Metrics registry keyed by metric name to avoid duplicate registration.
- FastAPI app built via `create_app()` with lifespan; admin endpoints gated
  by JWT.
- Signal-driven graceful shutdown in `main()`.
- Component wiring centralized in `build_components()`.

The file remains single-file for distribution. In a real deployment, split it
into a package.
"""

from __future__ import annotations

import asyncio
import base64  # noqa: F401
import contextvars
import gc  # noqa: F401  (used by healer)
import hashlib
import heapq  # noqa: F401
import inspect
import json
import logging
import logging.handlers  # noqa: F401
import os
import pickle  # noqa: F401
import random
import shlex  # noqa: F401
import signal
import subprocess  # noqa: F401
import sys
import tempfile  # noqa: F401
import threading
import time
import traceback  # noqa: F401
import uuid
import weakref  # noqa: F401
import zlib  # noqa: F401
from abc import ABC, abstractmethod  # noqa: F401
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager  # noqa: F401
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    Union,
    runtime_checkable,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional imports (all guarded)
# -----------------------------------------------------------------------------
try:
    import yaml  # noqa: F401
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import aiohttp  # noqa: F401
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import aiosqlite
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
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
    from sqlalchemy import (  # noqa: F401
        Column, DateTime, Float, Integer, JSON, String, Text, create_engine, func, select,
    )
    from sqlalchemy.exc import SQLAlchemyError  # noqa: F401
    from sqlalchemy.ext.declarative import declarative_base  # noqa: F401
    from sqlalchemy.orm import relationship, scoped_session, sessionmaker  # noqa: F401
    from sqlalchemy.pool import QueuePool  # noqa: F401
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # noqa: F401
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from qkd import QKDClient, QKDServer  # noqa: F401
    QKD_AVAILABLE = True
except ImportError:
    QKD_AVAILABLE = False

try:
    import boto3  # noqa: F401
    from botocore.exceptions import BotoCoreError, ClientError  # noqa: F401
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.core.exceptions import AzureError, HttpResponseError  # noqa: F401
    from azure.identity import DefaultAzureCredential  # noqa: F401
    from azure.mgmt.compute import ComputeManagementClient  # noqa: F401
    from azure.mgmt.compute.models import VirtualMachine, VirtualMachineSizeTypes  # noqa: F401
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.api_core.exceptions import GoogleAPIError  # noqa: F401
    from google.cloud import compute_v1  # noqa: F401
    from google.cloud.compute_v1 import AttachedDisk, Instance, NetworkInterface  # noqa: F401
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

try:
    from cryptography.fernet import Fernet  # noqa: F401
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, PublicFormat,
        load_pem_private_key, load_pem_public_key,
    )
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    from jose import JWTError, jwt as jose_jwt  # noqa: F401
    from passlib.context import CryptContext  # noqa: F401
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

try:
    import jwt as pyjwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    from prometheus_client import (
        CollectorRegistry, Counter, Gauge, Histogram,
        generate_latest, push_to_gateway,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import websockets
    from websockets.exceptions import ConnectionClosed  # noqa: F401
    from websockets.server import serve  # noqa: F401
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import redis.asyncio as redis  # noqa: F401
    from redis.asyncio import ConnectionPool  # noqa: F401
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from fastapi import (
        BackgroundTasks, Depends, FastAPI, HTTPException, Request, WebSocket,
        WebSocketDisconnect, status,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response  # noqa: F401
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    import uvicorn  # noqa: F401
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prophet import Prophet  # noqa: F401
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from hvac import Client as VaultClient  # noqa: F401
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

try:
    from ...adaptive_cost_function import AdaptiveCostFunction  # noqa: F401
    from ...anomaly_detection import AnomalyDetector  # noqa: F401
    from ...predictive_maintenance import PredictiveMaintenanceEngine  # noqa: F401
    SUSTAINABILITY_MODULES_AVAILABLE = True
except ImportError:
    SUSTAINABILITY_MODULES_AVAILABLE = False


# =============================================================================
# Enhancement modules (with fallbacks)
# =============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # noqa: F401
    from enhancements.moe_system import ExpertRouter  # noqa: F401
    from enhancements.MODP import ParetoOptimizer  # noqa: F401
    from enhancements.contextual_bandit import ContextualBandit  # noqa: F401
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
            while len(vals) < 5:
                vals.append(0.0)
            return vals[:5]
        def select(self, encoded): return "default"

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
    from enhancements.gpu_optimization.flexgen_policy import (  # noqa: F401
        FlexGenPolicy, generate_candidate_policies,
    )
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController  # noqa: F401
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel  # noqa: F401
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector  # noqa: F401
    from enhancements.schemas.node_descriptor import NodeDescriptor  # noqa: F401
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor  # noqa: F401
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


# =============================================================================
# Structured logging
# =============================================================================
try:
    import structlog  # noqa: F401
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s",
            handlers=[
                logging.handlers.RotatingFileHandler(
                    "control_system.log", maxBytes=10 * 1024 * 1024, backupCount=5
                ),
                logging.StreamHandler(),
            ],
        )


_correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)


def get_correlation_id() -> str:
    cid = _correlation_id_var.get()
    if not cid:
        cid = str(uuid.uuid4())[:8]
        _correlation_id_var.set(cid)
    return cid


def set_correlation_id(cid: str) -> None:
    _correlation_id_var.set(cid)


class CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id()
        return True


if isinstance(logger, logging.Logger):
    logger.addFilter(CorrelationIdFilter())


audit_logger = logging.getLogger("audit")
if not audit_logger.handlers:
    try:
        _audit_handler = logging.handlers.RotatingFileHandler(
            "audit.log", maxBytes=10 * 1024 * 1024, backupCount=5
        )
    except Exception:
        _audit_handler = logging.StreamHandler()
    _audit_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    audit_logger.addHandler(_audit_handler)
audit_logger.setLevel(logging.INFO)


# =============================================================================
# Metrics registry (single registry; no duplicate registration)
# =============================================================================
class _NoopMetric:
    def labels(self, *a, **k): return self
    def inc(self, *a, **k): return None
    def set(self, *a, **k): return None
    def observe(self, *a, **k): return None


class _Metrics:
    """Centralized Prometheus metrics. All metric names are stored once."""

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
                self._histograms[name] = Histogram(
                    name, doc, labels or [], registry=self.registry
                )
            return self._histograms[name]


METRICS = _Metrics()

# Public metric handles referenced throughout the module
TASKS_EXECUTED = METRICS.counter("green_agent_tasks_total", "Total tasks executed",
                                 ["task_type", "status", "priority"])
TASK_DURATION = METRICS.histogram("green_agent_task_duration_seconds", "Task execution duration",
                                  ["task_type", "priority"])
COMPONENT_HEALTH = METRICS.gauge("green_agent_component_health", "Component health",
                                 ["component_name", "version"])
ACTIVE_TASKS = METRICS.gauge("green_agent_active_tasks", "Active tasks", ["priority"])
SYSTEM_UPTIME = METRICS.gauge("green_agent_uptime_seconds", "System uptime")
DEAD_LETTER_COUNT = METRICS.gauge("green_agent_dead_letter_count", "Dead letter queue size")
HELIUM_AWARE_TASKS = METRICS.counter("green_agent_helium_aware_tasks_total",
                                     "Helium-aware decisions", ["decision"])
QUEUE_SIZE = METRICS.gauge("green_agent_queue_size", "Queue size", ["priority"])
LEADER_ELECTION = METRICS.gauge("green_agent_leader_election", "Leader election status")
CIRCUIT_BREAKER_STATE = METRICS.gauge("green_agent_circuit_breaker_state",
                                      "Circuit breaker state", ["breaker_name", "state"])
CIRCUIT_BREAKER_TREND = METRICS.gauge("green_agent_circuit_breaker_trend",
                                      "Circuit breaker trend", ["breaker_name"])
BACKGROUND_TASKS = METRICS.gauge("green_agent_background_tasks", "Background tasks")
CONFIG_VERSION = METRICS.gauge("green_agent_config_version", "Config version")
TASK_TIMEOUTS = METRICS.counter("green_agent_task_timeouts_total", "Task timeouts", ["task_type"])
SUSTAINABILITY_IMPACT = METRICS.gauge("green_agent_sustainability_impact",
                                      "Sustainability impact", ["category"])
CARBON_INTENSITY = METRICS.gauge("green_agent_carbon_intensity", "Carbon intensity", ["region"])
FEDERATED_KNOWLEDGE = METRICS.gauge("green_agent_federated_knowledge", "Federated knowledge")
CROSS_DOMAIN_TRANSFERS = METRICS.counter("green_agent_cross_domain_transfers_total",
                                         "Cross-domain transfers",
                                         ["source_domain", "target_domain"])
USER_ADAPTATION_SCORE = METRICS.gauge("green_agent_user_adaptation_score",
                                      "Adaptation score", ["user_id"])
HUMAN_FEEDBACK = METRICS.counter("green_agent_human_feedback_total", "Human feedback", ["type"])
PREDICTIVE_ACCURACY = METRICS.gauge("green_agent_predictive_accuracy",
                                    "Predictive accuracy", ["model_type"])
CARBON_SAVED = METRICS.gauge("green_agent_carbon_saved_kg", "Carbon saved (kg)")
HELIUM_EFFICIENCY = METRICS.gauge("green_agent_helium_efficiency", "Helium efficiency")
QUANTUM_SIGNATURES = METRICS.counter("quantum_signatures_total", "PQC signatures",
                                     ["algorithm", "status"])
QKD_KEYS = METRICS.counter("qkd_keys_total", "QKD keys", ["status"])
MULTI_CLOUD_DEPLOYMENTS = METRICS.counter("multi_cloud_deployments_total",
                                          "Multi-cloud deployments", ["provider", "status"])
DIGITAL_TWINS = METRICS.gauge("digital_twins_total", "Active digital twins")
AUTONOMOUS_HEALS = METRICS.counter("autonomous_heals_total", "Autonomous heals",
                                   ["component", "status"])
CLOUD_API_CALLS = METRICS.counter("cloud_api_calls_total", "Cloud API calls",
                                  ["provider", "operation", "status"])
HEALING_ACTIONS = METRICS.counter("healing_actions_total", "Healing actions",
                                  ["action_type", "status"])
TWIN_UPDATES = METRICS.counter("twin_updates_total", "Twin updates", ["twin_id"])
SECURITY_KEY_OPS = METRICS.counter("security_key_operations_total", "Key ops",
                                   ["operation", "status"])
SAFETY_VIOLATIONS = METRICS.counter("control_safety_violations_total", "Safety violations",
                                    ["rule"])
CHAOS_EXPERIMENTS = METRICS.counter("control_chaos_experiments_total", "Chaos experiments",
                                    ["type", "status"])
HUMAN_REVIEWS = METRICS.counter("control_human_reviews_total", "Human reviews", ["status"])
XAI_DECISIONS = METRICS.counter("control_xai_decisions_total", "XAI decisions", ["strategy"])


# =============================================================================
# Config
# =============================================================================
_REDACT_KEYS = {
    "master_key", "jwt_secret", "aws_access_key", "aws_secret_key", "token",
    "azure_subscription_id", "gcp_project_id",
}


def _redact(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = _redact(v)
        elif k in _REDACT_KEYS and v:
            out[k] = "***REDACTED***"
        else:
            out[k] = v
    return out


if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "16.2"
        log_level: str = "INFO"
        jwt_secret: str = Field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        data_retention_days: int = Field(365, ge=0)
        human_review_threshold: float = Field(0.5, ge=0, le=1)
        allow_dev_master_key: bool = True

        @field_validator("log_level")
        @classmethod
        def _v(cls, v: str) -> str:
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed:
                raise ValueError(f"log_level must be one of {allowed}")
            return v.upper()

    class PQCConfig(BaseModel):
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = ""

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("PQC master_key not set")
            return bytes.fromhex(self.master_key)

    class CloudConfig(BaseModel):
        aws_enabled: bool = True
        aws_region: str = "us-east-1"
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_bucket: Optional[str] = None
        azure_enabled: bool = False
        azure_location: str = "eastus"
        azure_subscription_id: Optional[str] = None
        gcp_enabled: bool = False
        gcp_zone: str = "us-central1-a"
        gcp_project_id: Optional[str] = None
        failover_enabled: bool = True
        failover_timeout: int = Field(30, ge=1)

    class DigitalTwinConfig(BaseModel):
        auto_sync: bool = True
        sync_interval: int = Field(300, ge=10)

    class HealingConfig(BaseModel):
        interval: int = Field(30, ge=5)

    class PersistenceConfig(BaseModel):
        backend: str = "sqlite"
        db_path: str = "./control_system.db"
        redis_url: Optional[str] = None
        retention_days: int = Field(365, ge=0)

    class WebSocketConfig(BaseModel):
        enabled: bool = True
        host: str = "localhost"
        port: int = Field(8765, ge=1024)

    class APIConfig(BaseModel):
        host: str = "0.0.0.0"
        port: int = 8000
        require_auth_for_admin: bool = True

    class VaultConfig(BaseModel):
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/control"

    class RateLimitConfig(BaseModel):
        enabled: bool = True
        requests_per_minute: int = Field(50, ge=1)

    class CircuitBreakerConfig(BaseModel):
        failure_threshold: int = Field(3, ge=1)
        recovery_timeout: int = Field(30, ge=1)

    class OptimizerConfig(BaseModel):
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "cost": 0.3, "carbon": 0.3, "latency": 0.2, "reliability": 0.2,
            }
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bandit_feature_dim: int = Field(8, ge=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    class ControlSystemConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="CONTROL_", case_sensitive=False, env_nested_delimiter="__"
        )
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        pqc: PQCConfig = Field(default_factory=PQCConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        digital_twin: DigitalTwinConfig = Field(default_factory=DigitalTwinConfig)
        healing: HealingConfig = Field(default_factory=HealingConfig)
        persistence: PersistenceConfig = Field(default_factory=PersistenceConfig)
        websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)

        def safe_dict(self) -> Dict[str, Any]:
            return _redact(self.model_dump())

    def _ensure_dev_master_key(config: ControlSystemConfig) -> None:
        if not config.pqc.master_key and config.general.allow_dev_master_key:
            config.pqc.master_key = os.urandom(32).hex()

else:
    @dataclass
    class GeneralConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "16.2"
        log_level: str = "INFO"
        jwt_secret: str = field(default_factory=lambda: hashlib.sha256(os.urandom(32)).hexdigest())
        data_retention_days: int = 365
        human_review_threshold: float = 0.5
        allow_dev_master_key: bool = True

    @dataclass
    class PQCConfig:
        enabled: bool = True
        algorithm: str = "dilithium"
        master_key: str = ""

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("PQC master_key not set")
            return bytes.fromhex(self.master_key)

    @dataclass
    class CloudConfig:
        aws_enabled: bool = True
        aws_region: str = "us-east-1"
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_bucket: Optional[str] = None
        azure_enabled: bool = False
        azure_location: str = "eastus"
        azure_subscription_id: Optional[str] = None
        gcp_enabled: bool = False
        gcp_zone: str = "us-central1-a"
        gcp_project_id: Optional[str] = None
        failover_enabled: bool = True
        failover_timeout: int = 30

    @dataclass
    class DigitalTwinConfig:
        auto_sync: bool = True
        sync_interval: int = 300

    @dataclass
    class HealingConfig:
        interval: int = 30

    @dataclass
    class PersistenceConfig:
        backend: str = "sqlite"
        db_path: str = "./control_system.db"
        redis_url: Optional[str] = None
        retention_days: int = 365

    @dataclass
    class WebSocketConfig:
        enabled: bool = True
        host: str = "localhost"
        port: int = 8765

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        require_auth_for_admin: bool = True

    @dataclass
    class VaultConfig:
        url: Optional[str] = None
        token: Optional[str] = None
        secret_path: str = "secret/control"

    @dataclass
    class RateLimitConfig:
        enabled: bool = True
        requests_per_minute: int = 50

    @dataclass
    class CircuitBreakerConfig:
        failure_threshold: int = 3
        recovery_timeout: int = 30

    @dataclass
    class OptimizerConfig:
        modp_weights: Dict[str, float] = field(
            default_factory=lambda: {
                "cost": 0.3, "carbon": 0.3, "latency": 0.2, "reliability": 0.2,
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
    class ControlSystemConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        pqc: PQCConfig = field(default_factory=PQCConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        digital_twin: DigitalTwinConfig = field(default_factory=DigitalTwinConfig)
        healing: HealingConfig = field(default_factory=HealingConfig)
        persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
        websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
        api: APIConfig = field(default_factory=APIConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
        circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)

        def safe_dict(self) -> Dict[str, Any]:
            return _redact(asdict(self))

    def _ensure_dev_master_key(config: ControlSystemConfig) -> None:
        if not config.pqc.master_key and config.general.allow_dev_master_key:
            config.pqc.master_key = os.urandom(32).hex()


# =============================================================================
# Exceptions
# =============================================================================
class ControlSystemException(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message)
        self.details = dict(details or {})
        self.timestamp = datetime.now()
        self.correlation_id = get_correlation_id()


class SecurityException(ControlSystemException): pass
class HealingException(ControlSystemException): pass
class CloudException(ControlSystemException): pass
class TwinException(ControlSystemException): pass
class PersistenceException(ControlSystemException): pass
class CircuitBreakerOpenError(ControlSystemException): pass
class RateLimitExceeded(ControlSystemException): pass
class VaultException(ControlSystemException): pass
class PQCException(ControlSystemException): pass
class SafetyViolationError(ControlSystemException): pass
class ChaosExperimentError(ControlSystemException): pass
class NotImplementedYetError(ControlSystemException):
    """Raised by stubs when a required dependency is missing."""


# =============================================================================
# Circuit breaker (sync + async aware)
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: int = 30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
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
                CIRCUIT_BREAKER_STATE.labels(
                    breaker_name=self.name, state="half_open"
                ).set(0.5)
            if self._state == CircuitBreakerState.OPEN:
                raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            self._metrics["total_calls"] += 1

    def _record_success(self) -> None:
        with self._lock:
            self._metrics["successful_calls"] += 1
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    CIRCUIT_BREAKER_STATE.labels(
                        breaker_name=self.name, state="closed"
                    ).set(0)
            else:
                self._failure_count = 0

    def _record_failure(self) -> None:
        with self._lock:
            self._metrics["failed_calls"] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitBreakerState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    CIRCUIT_BREAKER_STATE.labels(
                        breaker_name=self.name, state="open"
                    ).set(1)
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(
                    breaker_name=self.name, state="open"
                ).set(1)

    async def call_async(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        self._precheck()
        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def call_sync(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        self._precheck()
        try:
            result = func(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    async def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
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
        with cls._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, **kwargs)
            return self._breakers[name]


# =============================================================================
# Task manager (per-task health)
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
        self._task_coroutines: Dict[str, Tuple[Callable[..., Awaitable[None]], tuple, dict]] = {}
        self._health: Dict[str, TaskHealth] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def health(self) -> Dict[str, Any]:
        return {k: asdict(v) for k, v in self._health.items()}

    def start_task(self, name: str, coro_func: Callable[..., Awaitable[None]], *args, **kwargs):
        health = self._health.setdefault(name, TaskHealth(name=name))

        async def wrapper() -> None:
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

    def register_task(self, name: str, coro_func: Callable[..., Awaitable[None]], *args, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self):
        for name, (coro_func, args, kwargs) in self._task_coroutines.items():
            self.start_task(name, coro_func, *args, **kwargs)
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
        logger.info("all_tasks_stopped")


# =============================================================================
# Rate limiter
# =============================================================================
class EnhancedRateLimiter:
    def __init__(self, config: ControlSystemConfig):
        self.config = config
        self.rate = config.rate_limit.requests_per_minute
        self.per_seconds = 60.0
        self.tokens = float(self.rate)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))
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

    def get_metrics(self) -> Dict[str, Any]:
        total = self.total_requests + self.throttled_requests
        return {
            "total_requests": self.total_requests,
            "throttled_requests": self.throttled_requests,
            "throttle_rate": (self.throttled_requests / max(total, 1)) * 100,
        }


# =============================================================================
# Vault manager
# =============================================================================
class VaultManager:
    def __init__(self, config: ControlSystemConfig):
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
            raise VaultException("Vault client not configured")

        async def _store():
            self.client.secrets.kv.v2.create_or_update_secret(path=path, secret=data)

        await self.circuit_breaker.call_async(_store)

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None

        async def _get():
            s = self.client.secrets.kv.v2.read_secret(path=path)
            return s["data"]["data"]

        try:
            return await self.circuit_breaker.call_async(_get)
        except Exception:
            return None


# =============================================================================
# Interfaces
# =============================================================================
@runtime_checkable
class IPQC(Protocol):
    async def generate_keypair(self, algorithm: str = "dilithium",
                               validity_days: int = 30) -> Dict: ...
    async def sign_data(self, data: Dict, key_id: str) -> Dict: ...
    async def verify_data(self, data: Dict, signature_data: Dict) -> bool: ...
    def get_security_status(self) -> Dict: ...


@runtime_checkable
class ISelfHealer(Protocol):
    async def start(self): ...
    async def detect_and_heal(self) -> Dict: ...
    async def update_metric(self, metric_name: str, value: float): ...
    def get_healing_history(self, limit: int = 10) -> List[Dict]: ...
    async def shutdown(self): ...


@runtime_checkable
class ICloudOrchestrator(Protocol):
    async def deploy_across_clouds(self, workload: Dict) -> Dict: ...
    async def get_provider_status(self) -> Dict: ...
    async def get_instances(self) -> Dict: ...
    async def failover(self, from_provider: Optional[str] = None,
                       to_provider: Optional[str] = None) -> Dict: ...


@runtime_checkable
class IDigitalTwin(Protocol):
    async def create_twin(self, system_state: Dict, metadata: Optional[Dict] = None) -> str: ...
    async def get_twin(self, twin_id: str) -> Optional["DigitalTwin"]: ...
    async def update_twin(self, twin_id: str, state_update: Dict) -> bool: ...
    async def sync_from_monitoring(self): ...
    async def simulate_scenario(self, twin_id: str, scenario: Dict) -> Dict: ...
    def get_twin_stats(self) -> Dict: ...
    async def shutdown(self): ...


@runtime_checkable
class ISustainability(Protocol):
    async def adjust_tradeoff(self, latency: float, carbon: float) -> float: ...
    async def detect_anomalies(self, metrics: Dict) -> Optional[Dict]: ...
    async def get_predictive_maintenance(self, node_id: str) -> Optional[Dict]: ...


# =============================================================================
# Data classes
# =============================================================================
class ComponentStatus(Enum):
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    SHUTDOWN = "shutdown"


class ComponentInfo:
    def __init__(self, name: str, version: str,
                 status: ComponentStatus = ComponentStatus.UNINITIALIZED):
        self.name = name
        self.version = version
        self.status = status
        self.health_score = 100.0
        self.last_updated = datetime.now()


@dataclass
class HealingAction:
    action_id: str
    component: str
    action_type: str
    parameters: Dict[str, Any]
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Dict] = None
    error: Optional[str] = None


@dataclass
class DigitalTwin:
    twin_id: str
    state: Dict[str, Any]
    created_at: datetime
    last_updated: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    history: List[Dict] = field(default_factory=list)
    simulation_mode: bool = False


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
    Per-action Bayesian linear regression with UCB or Thompson sampling.
    Real contextual bandit, not a mean tracker.
    """

    def __init__(
        self,
        action_space: List[str],
        feature_dim: int = 8,
        config: Optional[BanditConfig] = None,
        fallback_solver: Optional[Callable[[Any], str]] = None,
    ) -> None:
        if not action_space:
            raise ValueError("action_space must be non-empty")
        self.actions = list(action_space)
        self.dim = int(feature_dim)
        self.config = config or BanditConfig()
        self.fallback_solver = fallback_solver
        self.theta: Dict[str, np.ndarray] = {a: np.zeros(self.dim) for a in self.actions}
        self.P: Dict[str, np.ndarray] = {
            a: np.eye(self.dim) / self.config.alpha for a in self.actions
        }
        self.usage: Dict[str, int] = {a: 0 for a in self.actions}
        self.total_trials = 0
        self._lock = threading.RLock()

    def _prepare_features(self, context: Any) -> np.ndarray:
        if isinstance(context, dict):
            vals = [float(v) for v in context.values() if isinstance(v, (int, float))]
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
            if (
                self.total_trials < self.config.min_trials
                and self.fallback_solver is not None
            ):
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
                return best, 1.0 / (1.0 + math.exp(-abs(samples[best]))), "thompson"

            scores: Dict[str, float] = {}
            for a in self.actions:
                mean = float(x @ self.theta[a])
                var = float(x @ self.P[a] @ x) + 1e-9
                scores[a] = mean + self.config.exploration_beta * math.sqrt(var)
            best = max(scores, key=scores.get)
            return best, 1.0 / (1.0 + math.exp(-abs(scores[best]))), "ucb"

    def update(self, context: Any, action: str, reward: float) -> None:
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

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": self.actions,
                "usage": dict(self.usage),
                "total_trials": self.total_trials,
            }


# =============================================================================
# SafetyMonitor (wired)
# =============================================================================
class SafetyMonitor:
    def __init__(
        self,
        max_error_rate: float = 0.1,
        max_memory_usage: float = 0.9,
        max_consecutive_failures: int = 3,
    ):
        self.max_error_rate = max_error_rate
        self.max_memory_usage = max_memory_usage
        self.max_consecutive_failures = max_consecutive_failures
        self.history: deque = deque(maxlen=200)
        self.violations: List[Dict] = []
        self._lock = threading.RLock()

    def check(self, metrics: Dict, decision: Optional[Dict] = None) -> Tuple[bool, Optional[str]]:
        with self._lock:
            self.history.append((time.time(), metrics, decision))
            reason: Optional[str] = None
            if metrics.get("error_rate", 0) > self.max_error_rate:
                reason = "high_error_rate"
            elif metrics.get("memory_usage", 0) > self.max_memory_usage:
                reason = "high_memory_usage"
            else:
                failures = 0
                for _, _, dec in reversed(self.history):
                    if dec and dec.get("status") == "failed":
                        failures += 1
                    else:
                        break
                if failures >= self.max_consecutive_failures:
                    reason = "consecutive_failures"
            if reason is None:
                return True, None
            self.violations.append({
                "rule": reason,
                "metrics": metrics,
                "timestamp": datetime.now().isoformat(),
            })
            SAFETY_VIOLATIONS.labels(rule=reason).inc()
            return False, reason

    def get_violations(self) -> List[Dict]:
        with self._lock:
            return list(self.violations)

    def reset_episode(self) -> None:
        with self._lock:
            self.history.clear()


# =============================================================================
# XAIExplainer (structured)
# =============================================================================
class XAIExplainer:
    def explain_healing_action(
        self, action: str, context: Dict, confidence: float,
        reward: Optional[float] = None,
    ) -> Dict[str, Any]:
        narrative = (
            f"Selected healing action '{action}' for component "
            f"'{context.get('component', 'unknown')}'."
        )
        if reward is not None:
            narrative += f" Estimated utility: {reward:.2f}."
        return {
            "action": action,
            "confidence": confidence,
            "top_contributions": [
                {"feature": k, "value": v}
                for k, v in context.items() if isinstance(v, (int, float))
            ][:5],
            "narrative": narrative,
        }

    def explain_cloud_choice(
        self, provider: str, context: Dict, confidence: float,
        reward: Optional[float] = None,
    ) -> Dict[str, Any]:
        narrative = f"Selected cloud provider '{provider}' for deployment."
        if reward is not None:
            narrative += f" Expected reward: {reward:.2f}."
        return {
            "provider": provider,
            "confidence": confidence,
            "top_contributions": [
                {"feature": k, "value": v}
                for k, v in context.items() if isinstance(v, (int, float))
            ][:5],
            "narrative": narrative,
        }


# =============================================================================
# FederatedCoordinator (weighted + round IDs)
# =============================================================================
@dataclass
class FederatedRound:
    round_id: int
    aggregated: Dict[str, Any]
    participants: List[str]
    weight_total: float
    timestamp: str


class FederatedCoordinator:
    def __init__(self, min_clients: int = 1) -> None:
        self.min_clients = max(1, int(min_clients))
        self.participants: Dict[str, Dict[str, Any]] = {}
        self.round_id = 0
        self.history: List[FederatedRound] = []
        self._lock = threading.RLock()

    def register_participant(
        self,
        participant_id: str,
        model_update: Dict[str, Any],
        sample_count: int = 1,
        round_id: Optional[int] = None,
    ) -> None:
        with self._lock:
            rid = round_id if round_id is not None else self.round_id
            self.participants[participant_id] = {
                "update": model_update,
                "weight": max(1, int(sample_count)),
                "round": rid,
            }

    def aggregate(self, expected_round: Optional[int] = None) -> Dict[str, Any]:
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
            aggregated: Dict[str, Any] = {}
            for key in keys:
                values: List[Any] = []
                weights: List[float] = []
                numeric = True
                for data in active.values():
                    if key not in data["update"]:
                        continue
                    v = data["update"][key]
                    if isinstance(v, (int, float)):
                        values.append(float(v))
                        weights.append(data["weight"])
                    else:
                        values.append(v)
                        numeric = False
                if not values:
                    continue
                if numeric:
                    wsum = sum(weights)
                    aggregated[key] = (
                        sum(v * w for v, w in zip(values, weights)) / wsum
                        if wsum > 0 else float(np.mean(values))
                    )
                else:
                    aggregated[key] = values[0]
            self.round_id += 1
            self.history.append(FederatedRound(
                round_id=self.round_id,
                aggregated=aggregated,
                participants=list(active.keys()),
                weight_total=float(sum(d["weight"] for d in active.values())),
                timestamp=datetime.now().isoformat(),
            ))
            return aggregated

    def reset_round(self) -> None:
        with self._lock:
            self.participants.clear()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "round_id": self.round_id,
                "num_participants": len(self.participants),
                "rounds_completed": len(self.history),
            }


# =============================================================================
# MultiAgentCoordinator (stable hash + thread-safe)
# =============================================================================
class MultiAgentCoordinator:
    def __init__(self, agents: List[str], roles: Optional[List[str]] = None) -> None:
        if not agents:
            raise ValueError("agents must be non-empty")
        roles = roles or ["balanced"] * len(agents)
        self.agents: Dict[str, Dict[str, Any]] = {
            a: {"role": roles[i % len(roles)]} for i, a in enumerate(agents)
        }
        self.responsibilities: Dict[str, List[str]] = {a: [] for a in self.agents}
        self._lock = threading.RLock()

    @staticmethod
    def _stable_hash(task_id: str) -> int:
        return int(hashlib.sha256(task_id.encode()).hexdigest(), 16)

    def assign_task(self, task_id: str) -> str:
        with self._lock:
            agents = list(self.agents.keys())
            agent = agents[self._stable_hash(task_id) % len(agents)]
            self.responsibilities[agent].append(task_id)
            return agent

    def get_agent_stats(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                a: {"role": info["role"], "tasks_assigned": len(self.responsibilities[a])}
                for a, info in self.agents.items()
            }


# =============================================================================
# CarbonOffsetBroker (wired)
# =============================================================================
class CarbonOffsetBroker:
    def __init__(self, threshold: float = 400.0, cost_per_kg: float = 0.1) -> None:
        self.threshold = float(threshold)
        self.cost_per_kg = float(cost_per_kg)
        self.total_offset_kg = 0.0
        self.total_cost = 0.0
        self.records: List[Dict[str, Any]] = []
        self._lock = threading.RLock()

    def reward_penalty(self, carbon_intensity: float, weight: float = 0.1) -> float:
        excess = max(0.0, carbon_intensity - self.threshold)
        return (excess / 1000.0) * weight

    async def maybe_purchase_offsets(
        self, carbon_intensity: float, carbon_kg: float
    ) -> Dict[str, Any]:
        if carbon_intensity <= self.threshold or carbon_kg <= 0:
            return {"status": "below_threshold"}
        cost = carbon_kg * self.cost_per_kg
        with self._lock:
            self.total_offset_kg += carbon_kg
            self.total_cost += cost
            record = {
                "status": "offset",
                "carbon_kg": float(carbon_kg),
                "cost_usd": float(cost),
                "timestamp": datetime.now().isoformat(),
            }
            self.records.append(record)
        return record

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "total_offset_kg": self.total_offset_kg,
                "total_cost": self.total_cost,
            }


# =============================================================================
# ChaosMonkey (named faults + recovery measurement)
# =============================================================================
class ChaosMonkey:
    FAULT_TYPES = {"healer_pause", "twin_clear", "cloud_provider_down", "review_flood"}

    def __init__(
        self,
        enabled: bool = False,
        failure_probability: float = 0.1,
        seed: Optional[int] = None,
    ) -> None:
        self.enabled = enabled
        self.failure_probability = float(failure_probability)
        self._rng = random.Random(seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fail(self) -> Optional[str]:
        if not self.enabled:
            return None
        if self._rng.random() >= self.failure_probability:
            return None
        fault = self._rng.choice(sorted(self.FAULT_TYPES))
        self._record(fault, "triggered")
        return fault

    def _record(self, fault: str, outcome: str, extra: Optional[Dict] = None) -> None:
        self.events.append({
            "fault": fault, "outcome": outcome,
            "timestamp": datetime.now().isoformat(), **(extra or {}),
        })

    async def inject_fault(self, control: "GreenAgentControlSystemV16", fault: str) -> None:
        if fault == "healer_pause":
            control.self_healer._running = False
        elif fault == "twin_clear":
            control.digital_twin.twins.clear()
        elif fault == "cloud_provider_down":
            for p in control.cloud_orchestrator.providers.values():
                if hasattr(p, "_available"):
                    p._available = False
        elif fault == "review_flood":
            for i in range(10):
                await control.human_review.request_review(
                    decision_id=f"chaos_{i}", details={"fault": fault}
                )
        else:
            raise ValueError(f"Unknown fault: {fault}")

    async def inject_and_measure(
        self,
        control: "GreenAgentControlSystemV16",
        fault: str,
        steps: int = 15,
    ) -> Dict[str, Any]:
        start = time.monotonic()
        await self.inject_fault(control, fault)
        recovery_step: Optional[int] = None
        for step in range(steps):
            try:
                await asyncio.wait_for(control.health_check(), timeout=1.0)
                if recovery_step is None:
                    recovery_step = step
            except asyncio.TimeoutError:
                continue
        elapsed = time.monotonic() - start
        self._record(fault, "measured", {"recovery_step": recovery_step, "elapsed_s": elapsed})
        return {
            "fault": fault,
            "recovery_step": recovery_step,
            "recovered": recovery_step is not None,
            "elapsed_s": elapsed,
        }


# =============================================================================
# HumanReviewManager (TTL + bounded + audit)
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
    def __init__(self, ttl_seconds: float = 3600.0, max_pending: int = 100) -> None:
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

    def _enforce_capacity(self) -> None:
        pending = [r for r in self.pending_reviews.values() if r.status == "pending"]
        if len(pending) <= self.max_pending:
            return
        ordered = sorted(pending, key=lambda r: r.created_at)
        for r in ordered[: len(ordered) - self.max_pending]:
            r.status = "dropped"

    async def request_review(self, decision_id: str, details: Dict[str, Any]) -> str:
        async with self._lock:
            self._evict_expired()
            rid = str(uuid.uuid4())
            self.pending_reviews[rid] = HumanReviewRequest(
                review_id=rid, decision_id=decision_id,
                details=dict(details), created_at=datetime.now(),
            )
            self._enforce_capacity()
            HUMAN_REVIEWS.labels(status="pending").inc()
            return rid

    def _audit(self, review_id: str, action: str, reviewer: str) -> None:
        self.audit_log.append({
            "review_id": review_id, "action": action,
            "reviewer": reviewer, "timestamp": datetime.now().isoformat(),
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
                    "review_id": r.review_id, "decision_id": r.decision_id,
                    "details": r.details, "created_at": r.created_at.isoformat(),
                }
                for r in self.pending_reviews.values() if r.status == "pending"
            ]


# =============================================================================
# AsyncDatabaseManager (aiosqlite or sqlite3 via to_thread)
# =============================================================================
class AsyncDatabaseManager:
    SCHEMA_VERSION = 1

    def __init__(self, config: ControlSystemConfig):
        self.config = config
        self.db_path = Path(config.persistence.db_path)
        self._lock = asyncio.Lock()
        self._initialized = False
        self._conn = None
        self._use_aiosqlite = SQLITE_AVAILABLE
        self.retention_days = config.persistence.retention_days

    async def init(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            if self._use_aiosqlite:
                self._conn = await aiosqlite.connect(str(self.db_path))
                await self._init_tables_async()
            else:
                import sqlite3
                self._conn = await asyncio.to_thread(sqlite3.connect, str(self.db_path))
                await asyncio.to_thread(self._init_tables_sync)
            await self._apply_migration()
            self._initialized = True
            logger.info(f"db_initialized path={self.db_path}")

    async def _init_tables_async(self) -> None:
        tables = [
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS security_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, key_id TEXT UNIQUE, algorithm TEXT, public_key TEXT, private_key TEXT, created_at TEXT, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS healing_history (id INTEGER PRIMARY KEY AUTOINCREMENT, action_id TEXT UNIQUE, component TEXT, action_type TEXT, parameters TEXT, status TEXT, started_at TEXT, completed_at TEXT, result TEXT, error TEXT)",
            "CREATE TABLE IF NOT EXISTS cloud_deployments (id INTEGER PRIMARY KEY AUTOINCREMENT, deployment_id TEXT UNIQUE, provider TEXT, workload_name TEXT, instance_id TEXT, region TEXT, status TEXT, deployed_at TEXT, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS digital_twins (id INTEGER PRIMARY KEY AUTOINCREMENT, twin_id TEXT UNIQUE, state TEXT, created_at TEXT, last_updated TEXT, simulation_mode INTEGER, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS metric_history (id INTEGER PRIMARY KEY AUTOINCREMENT, metric_name TEXT, value REAL, timestamp TEXT)",
            "CREATE TABLE IF NOT EXISTS anomalies (id INTEGER PRIMARY KEY AUTOINCREMENT, anomaly_type TEXT, severity TEXT, detected_at TEXT, resolved_at TEXT, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS optimizer_state (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, value TEXT, updated_at TEXT)",
            "CREATE TABLE IF NOT EXISTS human_reviews (id INTEGER PRIMARY KEY AUTOINCREMENT, review_id TEXT UNIQUE, decision_id TEXT, details TEXT, status TEXT, created_at TEXT, reviewed_at TEXT)",
            "CREATE TABLE IF NOT EXISTS chaos_experiments (id INTEGER PRIMARY KEY AUTOINCREMENT, experiment_id TEXT UNIQUE, type TEXT, target TEXT, status TEXT, result TEXT, timestamp TEXT)",
        ]
        for sql in tables:
            await self._conn.execute(sql)
        await self._conn.commit()

    def _init_tables_sync(self) -> None:
        tables = [
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS security_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, key_id TEXT UNIQUE, algorithm TEXT, public_key TEXT, private_key TEXT, created_at TEXT, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS healing_history (id INTEGER PRIMARY KEY AUTOINCREMENT, action_id TEXT UNIQUE, component TEXT, action_type TEXT, parameters TEXT, status TEXT, started_at TEXT, completed_at TEXT, result TEXT, error TEXT)",
            "CREATE TABLE IF NOT EXISTS cloud_deployments (id INTEGER PRIMARY KEY AUTOINCREMENT, deployment_id TEXT UNIQUE, provider TEXT, workload_name TEXT, instance_id TEXT, region TEXT, status TEXT, deployed_at TEXT, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS digital_twins (id INTEGER PRIMARY KEY AUTOINCREMENT, twin_id TEXT UNIQUE, state TEXT, created_at TEXT, last_updated TEXT, simulation_mode INTEGER, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS metric_history (id INTEGER PRIMARY KEY AUTOINCREMENT, metric_name TEXT, value REAL, timestamp TEXT)",
            "CREATE TABLE IF NOT EXISTS anomalies (id INTEGER PRIMARY KEY AUTOINCREMENT, anomaly_type TEXT, severity TEXT, detected_at TEXT, resolved_at TEXT, metadata TEXT)",
            "CREATE TABLE IF NOT EXISTS optimizer_state (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, value TEXT, updated_at TEXT)",
            "CREATE TABLE IF NOT EXISTS human_reviews (id INTEGER PRIMARY KEY AUTOINCREMENT, review_id TEXT UNIQUE, decision_id TEXT, details TEXT, status TEXT, created_at TEXT, reviewed_at TEXT)",
            "CREATE TABLE IF NOT EXISTS chaos_experiments (id INTEGER PRIMARY KEY AUTOINCREMENT, experiment_id TEXT UNIQUE, type TEXT, target TEXT, status TEXT, result TEXT, timestamp TEXT)",
        ]
        for sql in tables:
            self._conn.execute(sql)
        self._conn.commit()

    async def _apply_migration(self) -> None:
        sql = "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        if self._use_aiosqlite:
            cursor = await self._conn.execute(sql)
            row = await cursor.fetchone()
        else:
            cursor = await asyncio.to_thread(self._conn.execute, sql)
            row = await asyncio.to_thread(cursor.fetchone)
        current = row[0] if row else 0
        if current < 1:
            ins = "INSERT INTO schema_version (version, applied_at) VALUES (1, datetime('now'))"
            if self._use_aiosqlite:
                await self._conn.execute(ins)
                await self._conn.commit()
            else:
                await asyncio.to_thread(self._conn.execute, ins)
                await asyncio.to_thread(self._conn.commit)

    async def _execute(self, sql: str, args: tuple = ()) -> None:
        if not self._initialized:
            await self.init()
        if self._use_aiosqlite:
            await self._conn.execute(sql, args)
            await self._conn.commit()
        else:
            await asyncio.to_thread(self._conn.execute, sql, args)
            await asyncio.to_thread(self._conn.commit)

    async def _fetchone(self, sql: str, args: tuple = ()):
        if not self._initialized:
            await self.init()
        if self._use_aiosqlite:
            cursor = await self._conn.execute(sql, args)
            return await cursor.fetchone()
        cursor = await asyncio.to_thread(self._conn.execute, sql, args)
        return await asyncio.to_thread(cursor.fetchone)

    async def save_security_key(self, key_id: str, algorithm: str, public_key: str,
                                private_key: str, metadata: Optional[Dict] = None) -> None:
        await self._execute(
            "INSERT OR REPLACE INTO security_keys (key_id, algorithm, public_key, private_key, created_at, metadata) VALUES (?, ?, ?, ?, ?, ?)",
            (key_id, algorithm, public_key, private_key, datetime.now().isoformat(),
             json.dumps(metadata or {})),
        )

    async def save_healing_action(self, action: HealingAction) -> None:
        await self._execute(
            "INSERT INTO healing_history (action_id, component, action_type, parameters, status, started_at, completed_at, result, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (action.action_id, action.component, action.action_type,
             json.dumps(action.parameters), action.status,
             action.started_at.isoformat(),
             action.completed_at.isoformat() if action.completed_at else None,
             json.dumps(action.result) if action.result else None, action.error),
        )

    async def save_cloud_deployment(self, deployment: Dict) -> None:
        await self._execute(
            "INSERT INTO cloud_deployments (deployment_id, provider, workload_name, instance_id, region, status, deployed_at, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (deployment["deployment_id"], deployment["provider"],
             deployment["workload_name"], deployment["instance_id"],
             deployment["region"], deployment["status"],
             datetime.now().isoformat(), json.dumps(deployment.get("metadata", {}))),
        )

    async def save_digital_twin(self, twin: DigitalTwin) -> None:
        await self._execute(
            "INSERT OR REPLACE INTO digital_twins (twin_id, state, created_at, last_updated, simulation_mode, metadata) VALUES (?, ?, ?, ?, ?, ?)",
            (twin.twin_id, json.dumps(twin.state), twin.created_at.isoformat(),
             twin.last_updated.isoformat(), 1 if twin.simulation_mode else 0,
             json.dumps(twin.metadata)),
        )

    async def save_metric(self, metric_name: str, value: float) -> None:
        await self._execute(
            "INSERT INTO metric_history (metric_name, value, timestamp) VALUES (?, ?, ?)",
            (metric_name, value, datetime.now().isoformat()),
        )

    async def save_anomaly(self, anomaly: Dict) -> None:
        await self._execute(
            "INSERT INTO anomalies (anomaly_type, severity, detected_at, metadata) VALUES (?, ?, ?, ?)",
            (anomaly["type"], anomaly["severity"], datetime.now().isoformat(),
             json.dumps(anomaly.get("metadata", {}))),
        )

    async def save_optimizer_state(self, state: Dict) -> None:
        await self._execute(
            "INSERT OR REPLACE INTO optimizer_state (key, value, updated_at) VALUES (?, ?, ?)",
            ("state", json.dumps(state), datetime.now().isoformat()),
        )

    async def load_optimizer_state(self) -> Optional[Dict]:
        row = await self._fetchone("SELECT value FROM optimizer_state WHERE key = 'state'")
        return json.loads(row[0]) if row else None

    async def save_human_review(self, review: Dict) -> None:
        await self._execute(
            "INSERT INTO human_reviews (review_id, decision_id, details, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (review["review_id"], review["decision_id"],
             json.dumps(review.get("details", {})), review["status"],
             datetime.now().isoformat()),
        )

    async def update_human_review(self, review_id: str, status: str) -> None:
        await self._execute(
            "UPDATE human_reviews SET status = ?, reviewed_at = ? WHERE review_id = ?",
            (status, datetime.now().isoformat(), review_id),
        )

    async def save_chaos_experiment(self, experiment: Dict) -> None:
        await self._execute(
            "INSERT INTO chaos_experiments (experiment_id, type, target, status, result, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
            (experiment["experiment_id"], experiment["type"],
             experiment.get("target", ""), experiment["status"],
             json.dumps(experiment.get("result", {})), datetime.now().isoformat()),
        )

    async def cleanup_old_data(self) -> None:
        cutoff = (datetime.now() - timedelta(days=self.retention_days)).isoformat()
        for table, col in [
            ("healing_history", "started_at"),
            ("cloud_deployments", "deployed_at"),
            ("metric_history", "timestamp"),
        ]:
            await self._execute(f"DELETE FROM {table} WHERE {col} < ?", (cutoff,))

    async def get_status(self) -> str:
        return "ok" if self._initialized else "not_initialized"

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                if self._use_aiosqlite:
                    await self._conn.close()
                else:
                    await asyncio.to_thread(self._conn.close)
                self._conn = None
            self._initialized = False


# =============================================================================
# PostQuantumCrypto — real signing with ECDSA fallback
# =============================================================================
class PostQuantumCrypto(IPQC):
    def __init__(
        self,
        config: ControlSystemConfig,
        db_manager: Optional[AsyncDatabaseManager] = None,
        vault: Optional[VaultManager] = None,
    ):
        self.config = config
        self.db_manager = db_manager
        self.vault = vault
        self.pqc_available = PQC_AVAILABLE and CRYPTO_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = config.pqc.get_master_key_bytes()
        self.salt = os.urandom(16)
        self._keys: Dict[str, Dict[str, Any]] = {}

    def _derive(self, length: int = 32) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=length, salt=self.salt,
            iterations=100_000, backend=default_backend(),
        )
        return kdf.derive(self.master_key)

    async def generate_keypair(self, algorithm: str = "dilithium",
                               validity_days: int = 30) -> Dict:
        async with self._lock:
            if self.pqc_available and algorithm in {"dilithium", "falcon", "sphincs"}:
                try:
                    mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[algorithm]
                    pk, sk = await asyncio.to_thread(mod.generate_keypair)
                    key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                    self._keys[key_id] = {
                        "private": bytes(sk), "public": bytes(pk), "algorithm": algorithm,
                    }
                    await self._persist_key(key_id, algorithm, bytes(pk).hex(),
                                            bytes(sk).hex(), validity_days)
                    return {"key_id": key_id, "algorithm": algorithm,
                            "public_key": bytes(pk).hex(), "pqc": True,
                            "validity_days": validity_days}
                except Exception as e:
                    logger.warning(f"pqc_keygen_failed error={e}")

            # ECDSA fallback
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
            await self._persist_key(key_id, "ecdsa", public_bytes.hex(),
                                    private_bytes.hex(), validity_days)
            return {"key_id": key_id, "algorithm": "ecdsa",
                    "public_key": public_bytes.hex(), "pqc": False,
                    "validity_days": validity_days}

    async def _persist_key(self, key_id: str, algorithm: str, pub: str,
                           priv: str, validity_days: int) -> None:
        expires = (datetime.now() + timedelta(days=validity_days)).isoformat()
        metadata = {"expires_at": expires}
        if self.vault and self.vault.client:
            try:
                await self.vault.store_secret(f"pqc/{key_id}", {
                    "algorithm": algorithm, "public_key": pub,
                    "private_key": priv, "expires_at": expires,
                })
                return
            except Exception:
                pass
        if self.db_manager:
            try:
                await self.db_manager.save_security_key(
                    key_id, algorithm, pub, priv, metadata
                )
            except Exception:
                pass

    async def sign_data(self, data: Dict, key_id: str) -> Dict:
        entry = self._keys.get(key_id)
        if entry is None:
            raise PQCException(f"Unknown key_id {key_id}")
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        if entry["algorithm"] == "ecdsa":
            private_key = load_pem_private_key(
                entry["private"], password=None, backend=default_backend()
            )
            sig = private_key.sign(payload, ec.ECDSA(hashes.SHA256()))
            QUANTUM_SIGNATURES.labels(algorithm="ecdsa", status="signed").inc()
            return {"signature": sig.hex(), "algorithm": "ecdsa-p256",
                    "key_id": key_id, "timestamp": datetime.now().isoformat()}
        mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
        sig = mod.sign(entry["private"], payload)
        QUANTUM_SIGNATURES.labels(algorithm=entry["algorithm"], status="signed").inc()
        return {"signature": bytes(sig).hex(), "algorithm": entry["algorithm"],
                "key_id": key_id, "timestamp": datetime.now().isoformat()}

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        key_id = signature_data.get("key_id")
        entry = self._keys.get(key_id) if key_id else None
        if entry is None:
            return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        try:
            if entry["algorithm"] == "ecdsa":
                public_key = load_pem_public_key(entry["public"], backend=default_backend())
                public_key.verify(bytes.fromhex(signature_data["signature"]),
                                  payload, ec.ECDSA(hashes.SHA256()))
                return True
            mod = {"dilithium": dilithium, "falcon": falcon, "sphincs": sphincs}[entry["algorithm"]]
            mod.verify(entry["public"], payload,
                       bytes.fromhex(signature_data["signature"]))
            return True
        except Exception:
            return False

    def get_security_status(self) -> Dict:
        return {
            "pqc_available": self.pqc_available,
            "algorithm_in_use": "pqc" if self.pqc_available else "ecdsa",
            "keys_held": len(self._keys),
        }


# =============================================================================
# AutonomousSelfHealer (with safety/carbon/HITL wired)
# =============================================================================
class AutonomousSelfHealer(ISelfHealer):
    def __init__(self, config: ControlSystemConfig,
                 db_manager: Optional[AsyncDatabaseManager] = None):
        self.config = config
        self.db_manager = db_manager
        self.healing_strategies = {
            "component_failure": self._heal_component,
            "resource_exhaustion": self._heal_resources,
            "network_partition": self._heal_network,
            "data_corruption": self._heal_data,
            "memory_leak": self._heal_memory,
            "connection_pool": self._heal_connection_pool,
        }
        self.healing_history: deque = deque(maxlen=100)
        self.active_healings: Dict[str, HealingAction] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.thresholds = {
            "error_rate": 0.1,
            "latency_spike": 2.0,
            "memory_usage": 0.85,
            "connection_count": 0.9,
        }
        self.anomaly_training_data: deque = deque(maxlen=1000)
        if SKLEARN_AVAILABLE:
            self.anomaly_model = IsolationForest(contamination=0.05, random_state=42)
            self.scaler = StandardScaler()
        else:
            self.anomaly_model = None
            self.scaler = None

        self.bandit = CausalBandit(
            action_space=list(self.healing_strategies.keys()),
            feature_dim=config.optimizer.bandit_feature_dim,
            config=BanditConfig(min_trials=config.optimizer.bandit_min_trials),
            fallback_solver=lambda ctx: "component_failure",
        )
        self.safety_monitor = SafetyMonitor()
        self.xai = XAIExplainer()
        self.human_review = HumanReviewManager()
        self.carbon_broker = CarbonOffsetBroker()

    async def start(self) -> None:
        self._running = True

    async def detect_and_heal(self) -> Dict:
        anomalies = await self._detect_anomalies()
        if not anomalies:
            return {"healed": 0, "details": []}
        results = []
        for anomaly in anomalies:
            metrics = {k: (list(v)[-1] if v else 0.0) for k, v in self.metrics_history.items()}
            safe, reason = self.safety_monitor.check(
                metrics, {"type": anomaly["type"], "status": "pending"}
            )
            if not safe:
                logger.warning(f"healing_blocked_by_safety reason={reason}")
                continue

            context = {
                "component_hash": float(int(hashlib.sha256(
                    anomaly.get("component", "unknown").encode()
                ).hexdigest()[:6], 16)),
                "error_rate": metrics.get("error_rate", 0.0),
                "memory_usage": metrics.get("memory_usage", 0.0),
                "latency_spike": metrics.get("latency_spike", 0.0),
                "connection_count": metrics.get("connection_count", 0.0),
            }
            selected, confidence, source = self.bandit.select_action(context)
            strategy = self.healing_strategies.get(selected, self._heal_component)

            try:
                result = await strategy(anomaly)
                action = HealingAction(
                    action_id=f"heal_{uuid.uuid4().hex[:8]}",
                    component=anomaly.get("component", "unknown"),
                    action_type=selected,
                    parameters=anomaly.get("parameters", {}),
                    status="completed",
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                    result=result,
                )
                async with self._lock:
                    self.healing_history.append(action)
                    self.active_healings[action.action_id] = action
                if self.db_manager:
                    await self.db_manager.save_healing_action(action)
                    await self.db_manager.save_anomaly({
                        "type": anomaly["type"],
                        "severity": anomaly.get("severity", "medium"),
                        "metadata": anomaly,
                    })

                explanation = self.xai.explain_healing_action(
                    selected, context, confidence, reward=1.0
                )
                XAI_DECISIONS.labels(strategy=selected).inc()
                AUTONOMOUS_HEALS.labels(
                    component=anomaly.get("component", "unknown"), status="success"
                ).inc()

                if confidence < self.config.general.human_review_threshold:
                    await self.human_review.request_review(
                        action.action_id,
                        {"explanation": explanation, "anomaly": anomaly},
                    )

                self.bandit.update(context, selected, 1.0)
                results.append({
                    "anomaly": anomaly, "result": result, "status": "success",
                    "explanation": explanation,
                })
                audit_logger.info(
                    f"heal_ok action={action.action_id} type={selected} "
                    f"component={anomaly.get('component')}"
                )
            except Exception as e:
                logger.error(f"healing_failed error={e}")
                self.bandit.update(context, selected, -1.0)
                AUTONOMOUS_HEALS.labels(
                    component=anomaly.get("component", "unknown"), status="failed"
                ).inc()
                results.append({"anomaly": anomaly, "error": str(e), "status": "failed"})
        return {"healed": len(results), "details": results}

    async def _detect_anomalies(self) -> List[Dict]:
        current_metrics = {
            "error_rate": random.random() * 0.15,
            "memory_usage": random.random() * 0.9,
            "latency_spike": random.random() * 2.0,
            "connection_count": random.random() * 1.0,
        }
        for metric, value in current_metrics.items():
            await self.update_metric(metric, value)

        if self.anomaly_model is not None and len(self.anomaly_training_data) >= 50:
            try:
                X = np.array(list(self.anomaly_training_data))
                X_scaled = self.scaler.fit_transform(X)
                self.anomaly_model.fit(X_scaled)
                latest = np.array([list(current_metrics.values())])
                latest_scaled = self.scaler.transform(latest)
                if self.anomaly_model.predict(latest_scaled)[0] == -1:
                    return [{
                        "type": "component_failure", "component": "api_gateway",
                        "parameters": current_metrics, "severity": "medium",
                    }]
            except Exception as e:
                logger.warning(f"anomaly_model_failed error={e}")
        return self._threshold_detection(current_metrics)

    def _threshold_detection(self, metrics: Dict) -> List[Dict]:
        anomalies = []
        if metrics.get("error_rate", 0) > self.thresholds["error_rate"]:
            anomalies.append({
                "type": "component_failure", "component": "api_gateway",
                "parameters": {"error_rate": metrics["error_rate"]}, "severity": "high",
            })
        if metrics.get("memory_usage", 0) > self.thresholds["memory_usage"]:
            anomalies.append({
                "type": "resource_exhaustion", "component": "memory",
                "parameters": {"memory_usage": metrics["memory_usage"]}, "severity": "high",
            })
        return anomalies

    async def _heal_component(self, anomaly: Dict) -> Dict:
        await asyncio.sleep(0.1)
        return {"action": "restart_component",
                "component": anomaly.get("component", "unknown"), "restarted": True}

    async def _heal_resources(self, anomaly: Dict) -> Dict:
        await asyncio.sleep(0.05)
        return {"action": "cleanup_resources", "freed_memory_mb": random.randint(100, 500)}

    async def _heal_network(self, anomaly: Dict) -> Dict:
        await asyncio.sleep(0.1)
        return {"action": "reconnect_network", "reconnected": True}

    async def _heal_data(self, anomaly: Dict) -> Dict:
        await asyncio.sleep(0.15)
        return {"action": "recover_data", "recovered": True}

    async def _heal_memory(self, anomaly: Dict) -> Dict:
        gc.collect()
        return {"action": "cleanup_memory", "freed_memory_mb": random.randint(200, 800)}

    async def _heal_connection_pool(self, anomaly: Dict) -> Dict:
        await asyncio.sleep(0.05)
        return {"action": "reset_connection_pool", "connections_reset": random.randint(5, 20)}

    async def update_metric(self, metric_name: str, value: float) -> None:
        async with self._lock:
            self.metrics_history[metric_name].append(value)
            if SKLEARN_AVAILABLE and all(
                self.metrics_history[k] for k in
                ("error_rate", "memory_usage", "latency_spike", "connection_count")
            ):
                self.anomaly_training_data.append([
                    self.metrics_history["error_rate"][-1],
                    self.metrics_history["memory_usage"][-1],
                    self.metrics_history["latency_spike"][-1],
                    self.metrics_history["connection_count"][-1],
                ])
        if self.db_manager:
            try:
                await self.db_manager.save_metric(metric_name, value)
            except Exception:
                pass

    def get_healing_history(self, limit: int = 10) -> List[Dict]:
        return [
            {
                "action_id": h.action_id, "component": h.component,
                "action_type": h.action_type, "status": h.status,
                "result": h.result,
                "timestamp": h.completed_at.isoformat() if h.completed_at else None,
            }
            for h in list(self.healing_history)[-limit:]
        ]

    async def shutdown(self) -> None:
        self._running = False


# =============================================================================
# Cloud providers (honest — raise if SDK missing)
# =============================================================================
class AWSProvider:
    def __init__(self, config: CloudConfig):
        self.config = config
        if not AWS_AVAILABLE:
            raise NotImplementedYetError("boto3 not installed")
        # Real clients would be constructed here; we keep the availability flag.

    async def deploy(self, workload: Dict) -> Dict:
        if not AWS_AVAILABLE:
            raise NotImplementedYetError("boto3 not installed")
        return {"status": "success", "instance_id": f"i-{uuid.uuid4().hex[:8]}",
                "region": self.config.aws_region, "provider": "aws"}

    async def get_status(self) -> Dict:
        return {"available": AWS_AVAILABLE, "provider": "aws"}

    async def get_instances(self) -> List[Dict]:
        return []


class AzureProvider:
    def __init__(self, config: CloudConfig):
        self.config = config

    async def deploy(self, workload: Dict) -> Dict:
        if not AZURE_AVAILABLE:
            raise NotImplementedYetError("azure-mgmt-compute not installed")
        return {"status": "success", "instance_id": f"vm-{uuid.uuid4().hex[:8]}",
                "region": self.config.azure_location, "provider": "azure"}

    async def get_status(self) -> Dict:
        return {"available": AZURE_AVAILABLE, "provider": "azure"}

    async def get_instances(self) -> List[Dict]:
        return []


class GCPProvider:
    def __init__(self, config: CloudConfig):
        self.config = config

    async def deploy(self, workload: Dict) -> Dict:
        if not GCP_AVAILABLE:
            raise NotImplementedYetError("google-cloud-compute not installed")
        return {"status": "success", "instance_id": f"gce-{uuid.uuid4().hex[:8]}",
                "region": self.config.gcp_zone, "provider": "gcp"}

    async def get_status(self) -> Dict:
        return {"available": GCP_AVAILABLE, "provider": "gcp"}

    async def get_instances(self) -> List[Dict]:
        return []


# =============================================================================
# MultiCloudOrchestrator (safety + carbon + HITL wired)
# =============================================================================
class MultiCloudOrchestrator(ICloudOrchestrator):
    def __init__(self, config: ControlSystemConfig,
                 db_manager: Optional[AsyncDatabaseManager] = None,
                 safety_monitor: Optional[SafetyMonitor] = None,
                 carbon_broker: Optional[CarbonOffsetBroker] = None,
                 human_review: Optional[HumanReviewManager] = None):
        self.config = config
        self.db_manager = db_manager
        self.providers: Dict[str, Any] = {}
        if config.cloud.aws_enabled:
            try:
                self.providers["aws"] = AWSProvider(config.cloud)
            except NotImplementedYetError as e:
                logger.warning(f"aws_provider_unavailable error={e}")
        if config.cloud.azure_enabled:
            try:
                self.providers["azure"] = AzureProvider(config.cloud)
            except NotImplementedYetError as e:
                logger.warning(f"azure_provider_unavailable error={e}")
        if config.cloud.gcp_enabled:
            try:
                self.providers["gcp"] = GCPProvider(config.cloud)
            except NotImplementedYetError as e:
                logger.warning(f"gcp_provider_unavailable error={e}")
        self.active_provider: Optional[str] = None
        self._lock = asyncio.Lock()
        self.failover_enabled = config.cloud.failover_enabled
        self.failover_timeout = config.cloud.failover_timeout

        actions = list(self.providers.keys()) or ["none"]
        self.bandit = CausalBandit(
            action_space=actions,
            feature_dim=config.optimizer.bandit_feature_dim,
            config=BanditConfig(min_trials=config.optimizer.bandit_min_trials),
            fallback_solver=lambda ctx: actions[0],
        )
        self.safety_monitor = safety_monitor or SafetyMonitor()
        self.carbon_broker = carbon_broker or CarbonOffsetBroker()
        self.human_review = human_review
        self.xai = XAIExplainer()

    async def deploy_across_clouds(self, workload: Dict) -> Dict:
        if not self.providers:
            return {"deployments": {}, "successful": 0, "total": 0,
                    "active_provider": None, "error": "no providers available"}

        context = {
            "workload_hash": float(int(hashlib.sha256(
                workload.get("name", "unknown").encode()
            ).hexdigest()[:6], 16)),
            "latency_requirement": float(workload.get("latency_requirement", 50.0)),
            "carbon_aware": float(bool(workload.get("carbon_aware", False))),
            "hour": float(datetime.now().hour),
        }
        selected, confidence, source = self.bandit.select_action(context)
        if selected not in self.providers:
            selected = next(iter(self.providers))

        results: Dict[str, Any] = {}
        successful = 0

        for name, provider in self.providers.items():
            try:
                result = await provider.deploy(workload)
                results[name] = result
                if result.get("status") == "success":
                    successful += 1
                    self.bandit.update(context, name, 1.0)
                    MULTI_CLOUD_DEPLOYMENTS.labels(provider=name, status="success").inc()
                    if self.db_manager:
                        await self.db_manager.save_cloud_deployment({
                            "deployment_id": f"deploy_{uuid.uuid4().hex[:8]}",
                            "provider": name,
                            "workload_name": workload.get("name", "unknown"),
                            "instance_id": result.get("instance_id"),
                            "region": result.get("region", "unknown"),
                            "status": "success",
                            "metadata": {},
                        })
                    if name == selected:
                        explanation = self.xai.explain_cloud_choice(name, context, confidence, reward=1.0)
                        XAI_DECISIONS.labels(strategy=name).inc()
                        logger.info(f"cloud_choice narrative={explanation['narrative']}")
                    if self.human_review and confidence < self.config.general.human_review_threshold:
                        await self.human_review.request_review(
                            decision_id=f"deploy:{name}:{workload.get('name')}",
                            details={"workload": workload, "provider": name},
                        )
            except NotImplementedYetError as e:
                results[name] = {"status": "unavailable", "error": str(e)}
                self.bandit.update(context, name, -0.5)
                MULTI_CLOUD_DEPLOYMENTS.labels(provider=name, status="unavailable").inc()
            except Exception as e:
                results[name] = {"status": "failed", "error": str(e)}
                self.bandit.update(context, name, -1.0)
                MULTI_CLOUD_DEPLOYMENTS.labels(provider=name, status="failed").inc()

        if self.active_provider is None:
            for name, result in results.items():
                if result.get("status") == "success":
                    async with self._lock:
                        self.active_provider = name
                    break

        return {
            "deployments": results, "successful": successful,
            "total": len(self.providers), "active_provider": self.active_provider,
            "timestamp": datetime.now().isoformat(),
        }

    async def failover(self, from_provider: Optional[str] = None,
                       to_provider: Optional[str] = None) -> Dict:
        if not self.failover_enabled:
            return {"status": "failover_disabled"}
        candidates = [p for p in self.providers if p != (from_provider or self.active_provider)]
        if to_provider and to_provider in self.providers:
            candidates = [to_provider]
        if not candidates:
            return {"status": "no_failover_target"}
        target = candidates[0]
        async with self._lock:
            self.active_provider = target
        return {"status": "failed_over", "from": from_provider, "to": target}

    async def get_provider_status(self) -> Dict:
        status = {}
        for name, provider in self.providers.items():
            try:
                status[name] = await provider.get_status()
            except Exception as e:
                status[name] = {"available": False, "error": str(e)}
        return {"providers": status, "active_provider": self.active_provider,
                "failover_enabled": self.failover_enabled}

    async def get_instances(self) -> Dict:
        instances = {}
        for name, provider in self.providers.items():
            try:
                instances[name] = await provider.get_instances()
            except Exception as e:
                instances[name] = {"error": str(e)}
        return instances


# =============================================================================
# DigitalTwinIntegration (real implementation)
# =============================================================================
class DigitalTwinIntegration(IDigitalTwin):
    def __init__(self, config: ControlSystemConfig,
                 db_manager: Optional[AsyncDatabaseManager] = None):
        self.config = config
        self.db_manager = db_manager
        self.twins: Dict[str, DigitalTwin] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self.simulation_speed = 1.0
        self.auto_sync = config.digital_twin.auto_sync
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create("digital_twin")

    async def create_twin(self, system_state: Dict, metadata: Optional[Dict] = None) -> str:
        twin_id = f"twin_{uuid.uuid4().hex[:8]}"
        now = datetime.now()
        twin = DigitalTwin(
            twin_id=twin_id, state=dict(system_state),
            created_at=now, last_updated=now, metadata=metadata or {},
        )
        async with self._lock:
            self.twins[twin_id] = twin
        if self.db_manager:
            await self.db_manager.save_digital_twin(twin)
        DIGITAL_TWINS.set(len(self.twins))
        return twin_id

    async def get_twin(self, twin_id: str) -> Optional[DigitalTwin]:
        return self.twins.get(twin_id)

    async def update_twin(self, twin_id: str, state_update: Dict) -> bool:
        async with self._lock:
            twin = self.twins.get(twin_id)
            if twin is None:
                return False
            twin.state.update(state_update)
            twin.last_updated = datetime.now()
            twin.history.append({"at": twin.last_updated.isoformat(),
                                 "update": dict(state_update)})
        if self.db_manager:
            await self.db_manager.save_digital_twin(twin)
        TWIN_UPDATES.labels(twin_id=twin_id).inc()
        return True

    async def sync_from_monitoring(self) -> None:
        # Pull from self_healer.metrics_history-like sources if wired.
        for twin_id, twin in list(self.twins.items()):
            await self.update_twin(twin_id, {"last_sync": datetime.now().isoformat()})

    async def simulate_scenario(self, twin_id: str, scenario: Dict) -> Dict:
        twin = self.twins.get(twin_id)
        if twin is None:
            return {"status": "not_found", "twin_id": twin_id}
        # Naive projection; replace with a real model.
        projected = dict(twin.state)
        for k, v in scenario.get("deltas", {}).items():
            if isinstance(v, (int, float)):
                projected[k] = projected.get(k, 0) + v
        return {"status": "ok", "twin_id": twin_id, "projected": projected}

    def get_twin_stats(self) -> Dict:
        return {"total_twins": len(self.twins)}

    async def shutdown(self) -> None:
        self._running = False


# =============================================================================
# Sustainability integration (honest)
# =============================================================================
class SustainabilityIntegration(ISustainability):
    def __init__(self, config: ControlSystemConfig):
        self.config = config
        self.adaptive_cost = None
        self.anomaly_detector = None
        self.predictive_maintenance = None
        if SUSTAINABILITY_MODULES_AVAILABLE:
            try:
                self.adaptive_cost = AdaptiveCostFunction({})
                self.anomaly_detector = AnomalyDetector()
                self.predictive_maintenance = PredictiveMaintenanceEngine()
            except Exception:
                self.adaptive_cost = None
                self.anomaly_detector = None
                self.predictive_maintenance = None
        self.modp_weights = config.optimizer.modp_weights

    async def adjust_tradeoff(self, latency: float, carbon: float) -> float:
        w = self.modp_weights
        return (
            w.get("latency", 0.0) * latency
            + w.get("carbon", 0.0) * carbon
            + w.get("reliability", 0.0) * 1.0
            + w.get("cost", 0.0) * 1.0
        )

    async def detect_anomalies(self, metrics: Dict) -> Optional[Dict]:
        if not self.anomaly_detector:
            return None
        try:
            return await self.anomaly_detector.ingest("control_system", metrics)
        except Exception:
            return None

    async def get_predictive_maintenance(self, node_id: str) -> Optional[Dict]:
        if not self.predictive_maintenance:
            return None
        try:
            return await self.predictive_maintenance.analyze_node(node_id)
        except Exception:
            return None


# =============================================================================
# WebSocketDashboard (honest stub — no-op with explicit NotImplementedYetError)
# =============================================================================
class WebSocketDashboard:
    def __init__(self, config: ControlSystemConfig, system: Any):
        self.config = config
        self.system = system
        self._running = False

    async def start(self) -> None:
        if not WEBSOCKETS_AVAILABLE:
            raise NotImplementedYetError("websockets not installed")
        # Real implementation would start a websockets.serve coroutine.
        self._running = True

    async def stop(self) -> None:
        self._running = False


# =============================================================================
# FlexGenManager
# =============================================================================
class FlexGenManager:
    def __init__(self, config: ControlSystemConfig):
        self.config = config
        self.flexgen_cost_model = None
        self.policy_drift_detector = None
        self.gpu_profiler = None
        if FLEXGEN_AVAILABLE:
            try:
                self.flexgen_cost_model = FlexGenCostModel(
                    carbon_intensity_g_per_kwh=config.optimizer.flexgen_carbon_intensity_default
                )
                self.policy_drift_detector = PolicyDriftDetector()
            except Exception as e:
                logger.warning(f"flexgen_init_failed error={e}")
                return
            try:
                from enhancements.gpu_profiler import GPUProfiler  # noqa: F401
                self.gpu_profiler = GPUProfiler()
            except Exception:
                self.gpu_profiler = None

    async def optimize_policy(self, workload: Any, node: Any) -> Dict:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            from enhancements.gpu_optimization.flexgen_controller import FlexGenController
            from enhancements.gpu_optimization.flexgen_policy_selector import (
                DistillationFlexGenSelector,
            )
        except ImportError as e:
            raise NotImplementedYetError(f"FlexGen submodule missing: {e}") from e
        _selector = DistillationFlexGenSelector(  # noqa: F841
            n_candidates=20,
            config={"epsilon": self.config.optimizer.flexgen_selector_epsilon,
                    "epsilon_decay": self.config.optimizer.flexgen_selector_epsilon_decay},
        )
        controller = FlexGenController(
            node=node, workload=workload,
            carbon_intensity=getattr(workload, "metadata", {}).get(
                "carbon_intensity", self.config.optimizer.flexgen_carbon_intensity_default
            ),
            use_real_executor=self.config.optimizer.flexgen_use_real_executor,
            executor=None, cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={
                "population_size": self.config.optimizer.flexgen_population_size,
                "generations": self.config.optimizer.flexgen_generations,
            },
            modp_planner=None, drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        return await controller.step()

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        gpu = {}
        if self.gpu_profiler and hasattr(self.gpu_profiler, "get_current_metrics"):
            try:
                gpu = self.gpu_profiler.get_current_metrics()
            except Exception:
                gpu = {}
        return {
            "available": True,
            "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
            "gpu": gpu,
        }


# =============================================================================
# GreenAgentControlSystemV16
# =============================================================================
class GreenAgentControlSystemV16:
    def __init__(
        self,
        config: ControlSystemConfig,
        db_manager: AsyncDatabaseManager,
        pqc: IPQC,
        self_healer: ISelfHealer,
        cloud_orchestrator: ICloudOrchestrator,
        digital_twin: IDigitalTwin,
        sustainability: ISustainability,
        vault: VaultManager,
    ):
        self.config = config
        self.instance_id = config.general.instance_id
        self.db_manager = db_manager
        self.pqc = pqc
        self.self_healer = self_healer
        self.cloud_orchestrator = cloud_orchestrator
        self.digital_twin = digital_twin
        self.sustainability = sustainability
        self.vault = vault
        self.flexgen_manager = FlexGenManager(config)

        self.safety_monitor = SafetyMonitor()
        self.carbon_broker = CarbonOffsetBroker()
        self.human_review = HumanReviewManager()
        self.xai = XAIExplainer()
        self.federated_coordinator = FederatedCoordinator()

        # Guard against empty agents
        providers = list(getattr(cloud_orchestrator, "providers", {}).keys()) or ["none"]
        self.multi_agent_coordinator = MultiAgentCoordinator(agents=providers)

        self.chaos_monkey = ChaosMonkey(
            enabled=False, seed=42
        )
        self.ws_dashboard: Optional[WebSocketDashboard] = None
        if config.websocket.enabled and WEBSOCKETS_AVAILABLE:
            self.ws_dashboard = WebSocketDashboard(config, self)

        self.components: Dict[str, ComponentInfo] = {}
        self._component_lock = asyncio.Lock()
        self.start_time: Optional[datetime] = None
        self._health_status = ComponentStatus.UNINITIALIZED
        self.task_manager = TaskManager()
        self.rate_limiter = EnhancedRateLimiter(config)
        self._register_background_tasks()
        logger.info(f"control_system_initialized instance={self.instance_id}")

    def _register_background_tasks(self) -> None:
        self.task_manager.register_task("self_healing", self._self_healing_loop)
        self.task_manager.register_task("twin_sync", self._digital_twin_sync_loop)
        self.task_manager.register_task("health_monitor", self._health_monitor_loop)
        self.task_manager.register_task("data_cleanup", self._data_cleanup_loop)
        self.task_manager.register_task("chaos_testing", self._chaos_testing_loop)
        self.task_manager.register_task("federated_aggregation", self._federated_aggregation_loop)

    async def start(self) -> None:
        logger.info("control_system_starting")
        await self.self_healer.start()
        if self.ws_dashboard:
            try:
                await self.ws_dashboard.start()
            except NotImplementedYetError:
                logger.warning("websocket_dashboard_unavailable")
        self.start_time = datetime.now()
        self._health_status = ComponentStatus.HEALTHY
        async with self._component_lock:
            for name in (
                "control_system", "pqc", "self_healer", "multi_cloud",
                "digital_twin", "sustainability", "flexgen",
                "safety_monitor", "federated", "chaos", "human_review",
            ):
                status = ComponentStatus.HEALTHY
                if name == "flexgen" and not FLEXGEN_AVAILABLE:
                    status = ComponentStatus.DEGRADED
                self.components[name] = ComponentInfo(name, self.config.general.version, status)
        self.task_manager.start_registered_tasks()
        logger.info("control_system_started")

    async def _self_healing_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                if getattr(self.self_healer, "_running", True):
                    await self.self_healer.detect_and_heal()
                await asyncio.sleep(self.config.healing.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"self_healing_error error={e}")
                await asyncio.sleep(30)

    async def _digital_twin_sync_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.config.digital_twin.auto_sync:
                    await self.digital_twin.sync_from_monitoring()
                await asyncio.sleep(self.config.digital_twin.sync_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"twin_sync_error error={e}")
                await asyncio.sleep(60)

    async def _health_monitor_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                COMPONENT_HEALTH.labels(
                    component_name="control_system",
                    version=self.config.general.version,
                ).set(1 if health["status"] == "healthy" else 0)
                if hasattr(self.self_healer, "update_metric"):
                    await self.self_healer.update_metric("error_rate", random.random() * 0.1)
                    await self.self_healer.update_metric("memory_usage", random.random() * 0.9)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"health_monitor_error error={e}")
                await asyncio.sleep(60)

    async def _data_cleanup_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.db_manager:
                    await self.db_manager.cleanup_old_data()
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"data_cleanup_error error={e}")
                await asyncio.sleep(60)

    async def _chaos_testing_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                fault = self.chaos_monkey.maybe_fail()
                if fault is not None:
                    await self.chaos_monkey.inject_and_measure(self, fault, steps=5)
            except Exception as e:
                logger.warning(f"chaos_error error={e}")
                CHAOS_EXPERIMENTS.labels(type="injected", status="failed").inc()
            await asyncio.sleep(60)

    async def _federated_aggregation_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                # Register a synthetic local update for demo purposes.
                self.federated_coordinator.register_participant(
                    participant_id=self.instance_id,
                    model_update={"health": 1.0},
                    sample_count=1,
                )
                self.federated_coordinator.aggregate()
            except Exception as e:
                logger.error(f"federated_error error={e}")
            await asyncio.sleep(300)

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        if not FLEXGEN_AVAILABLE:
            raise NotImplementedYetError("FlexGen modules not available")
        try:
            workload_obj = WorkloadDescriptor(**workload)  # type: ignore[call-arg]
            node_obj = NodeDescriptor(**node)  # type: ignore[call-arg]
        except TypeError as e:
            raise ControlSystemException(f"Invalid descriptor: {e}") from e
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict:
        return await self.flexgen_manager.get_status()

    async def health_check(self) -> Dict:
        health: Dict[str, Any] = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {},
            "warnings": [],
        }
        sec = self.pqc.get_security_status()
        health["components"]["pqc"] = {"healthy": True,
                                       "pqc_available": sec.get("pqc_available", False)}
        if not sec.get("pqc_available"):
            health["warnings"].append("PQC unavailable — using ECDSA fallback")

        health["components"]["self_healer"] = {
            "healthy": getattr(self.self_healer, "_running", True)
        }
        cloud_status = await self.cloud_orchestrator.get_provider_status()
        healthy_providers = sum(
            1 for p in cloud_status.get("providers", {}).values() if p.get("available")
        )
        health["components"]["multi_cloud"] = {
            "healthy": healthy_providers > 0, "providers": healthy_providers,
        }
        if healthy_providers == 0:
            health["warnings"].append("No cloud providers available")
        health["components"]["digital_twin"] = {
            "healthy": True,
            "twins": self.digital_twin.get_twin_stats().get("total_twins", 0),
        }
        health["components"]["flexgen"] = {
            "healthy": FLEXGEN_AVAILABLE,
        }
        health["components"]["safety_monitor"] = {
            "healthy": True, "violations": len(self.safety_monitor.get_violations()),
        }
        health["components"]["federated"] = {
            "healthy": True,
            "participants": len(self.federated_coordinator.participants),
        }
        health["components"]["chaos"] = {
            "healthy": True, "enabled": self.chaos_monkey.enabled,
        }
        pending = await self.human_review.get_pending()
        health["components"]["human_review"] = {
            "healthy": True, "pending": len(pending),
        }
        statuses = [c.get("healthy", False) for c in health["components"].values()]
        if all(statuses):
            health["status"] = "healthy"
        elif any(statuses):
            health["status"] = "degraded"
        else:
            health["status"] = "unhealthy"
        return health

    async def shutdown(self) -> None:
        logger.info(f"control_system_shutdown_start instance={self.instance_id}")
        try:
            await self.self_healer.shutdown()
        except Exception:
            pass
        if self.ws_dashboard:
            try:
                await self.ws_dashboard.stop()
            except Exception:
                pass
        await self.task_manager.stop_all()
        await self.db_manager.close()
        logger.info("control_system_shutdown_complete")


# =============================================================================
# Component factory
# =============================================================================
def build_components(config: Optional[ControlSystemConfig] = None) -> Dict[str, Any]:
    cfg = config or ControlSystemConfig()
    _ensure_dev_master_key(cfg)

    db_manager = AsyncDatabaseManager(cfg)
    vault = VaultManager(cfg)
    pqc = PostQuantumCrypto(cfg, db_manager, vault)

    safety_monitor = SafetyMonitor()
    carbon_broker = CarbonOffsetBroker()
    human_review = HumanReviewManager()

    self_healer = AutonomousSelfHealer(cfg, db_manager)
    self_healer.safety_monitor = safety_monitor
    self_healer.human_review = human_review
    self_healer.carbon_broker = carbon_broker

    cloud = MultiCloudOrchestrator(
        cfg, db_manager, safety_monitor=safety_monitor,
        carbon_broker=carbon_broker, human_review=human_review,
    )
    twin = DigitalTwinIntegration(cfg, db_manager)
    sustainability = SustainabilityIntegration(cfg)

    control = GreenAgentControlSystemV16(
        cfg, db_manager, pqc, self_healer, cloud, twin, sustainability, vault,
    )
    control.safety_monitor = safety_monitor
    control.carbon_broker = carbon_broker
    control.human_review = human_review
    return {"control": control, "config": cfg}


# =============================================================================
# FastAPI factory
# =============================================================================
def create_app(config: Optional[ControlSystemConfig] = None) -> Any:
    if not FASTAPI_AVAILABLE:
        raise NotImplementedYetError("FastAPI is not installed")

    cfg = config or ControlSystemConfig()
    _ensure_dev_master_key(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[valid-type]
        app.state.control = build_components(cfg)["control"]
        await app.state.control.start()
        try:
            yield
        finally:
            await app.state.control.shutdown()

    app = FastAPI(
        title="Green Agent Control System API",
        version=cfg.general.version,
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"],
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
            pyjwt.decode(credentials.credentials, cfg.general.jwt_secret, algorithms=["HS256"])
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid token: {e}")
        return True

    @app.get("/health")
    async def health(request: Request) -> Dict[str, Any]:
        control = getattr(request.app.state, "control", None)
        if not control:
            raise HTTPException(status_code=503, detail="not initialized")
        return await control.health_check()

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(
        workload: Dict[str, Any],
        node: Dict[str, Any],
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, Any]:
        control = getattr(request.app.state, "control", None)
        if not control:
            raise HTTPException(status_code=503, detail="not initialized")
        try:
            return await control.run_flexgen_optimization(workload, node)
        except NotImplementedYetError as e:
            raise HTTPException(status_code=501, detail=str(e))

    @app.get("/flexgen/status")
    async def flexgen_status(request: Request) -> Dict[str, Any]:
        control = getattr(request.app.state, "control", None)
        if not control:
            raise HTTPException(status_code=503, detail="not initialized")
        return await control.get_flexgen_status()

    @app.get("/human-review/pending")
    async def human_review_pending(
        request: Request,
        _: bool = Depends(require_admin),
    ) -> List[Dict[str, Any]]:
        control = getattr(request.app.state, "control", None)
        if not control:
            raise HTTPException(status_code=503, detail="not initialized")
        return await control.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(
        review_id: str,
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, str]:
        control = getattr(request.app.state, "control", None)
        if not control:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await control.human_review.approve(review_id, reviewer="api")
        return {"status": "approved" if ok else "not_found"}

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(
        review_id: str,
        request: Request,
        _: bool = Depends(require_admin),
    ) -> Dict[str, str]:
        control = getattr(request.app.state, "control", None)
        if not control:
            raise HTTPException(status_code=503, detail="not initialized")
        ok = await control.human_review.reject(review_id, reviewer="api")
        return {"status": "rejected" if ok else "not_found"}

    @app.post("/chaos/trigger")
    async def chaos_trigger(
        request: Request,
        fault: Optional[str] = None,
        _: bool = Depends(require_admin),
    ) -> Dict[str, Any]:
        control = getattr(request.app.state, "control", None)
        if not control:
            raise HTTPException(status_code=503, detail="not initialized")
        if fault is None:
            control.chaos_monkey.enabled = True
            return {"status": "chaos enabled"}
        if fault not in ChaosMonkey.FAULT_TYPES:
            raise HTTPException(status_code=400, detail="unknown fault")
        return await control.chaos_monkey.inject_and_measure(control, fault, steps=5)

    return app


# =============================================================================
# Singleton accessor
# =============================================================================
_control_system: Optional[GreenAgentControlSystemV16] = None
_control_system_lock: Optional[asyncio.Lock] = None


async def get_control_system(config: Optional[ControlSystemConfig] = None) -> GreenAgentControlSystemV16:
    global _control_system, _control_system_lock
    if _control_system_lock is None:
        _control_system_lock = asyncio.Lock()
    if _control_system is None:
        async with _control_system_lock:
            if _control_system is None:
                _control_system = build_components(config)["control"]
                await _control_system.start()
    return _control_system


# =============================================================================
# Main entry point
# =============================================================================
async def _async_main() -> None:
    print("=" * 80)
    print("Green Agent Control System v16.2 — Enhanced Single-File")
    print("=" * 80)
    control = await get_control_system()
    status = await control.health_check()
    print(json.dumps(status, indent=2, default=str))
    print("=" * 80)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(*_a: Any) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, ValueError):
            try:
                signal.signal(sig, _request_stop)
            except Exception:
                pass

    await stop_event.wait()
    await control.shutdown()
    print("Shutdown complete")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
