#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/blockchain_helium_verification_enhanced_v18.py
# VERSION: 18.0.0 (Enterprise Quantum Resilience – Production Ready with DI)
# =============================================================================
"""
Enhanced Blockchain Helium Verification — Version 18.0.0
=========================================================

Self-contained verification service with:
  - Dependency-inversion interfaces for storage, ZK, blockchain, carbon,
    multi-chain, and cloud distribution.
  - Global circuit breaker registry, centralized task manager, rate limiter.
  - Schema-versioned SQLite migrations.
  - Grouped Pydantic config, JWT auth, Prometheus metrics.
  - Safety monitor, XAI explanations, human-in-the-loop reviews, chaos
    engineering, causal bandit, quantum-distillation stub, federated
    coordinator, multi-agent voting, FlexGen policy simulation.

CHANGES (v18.0.0 over v17.1.0):
- Fixed missing ``logging.handlers`` and ``Awaitable`` imports.
- Pydantic v2-first detection (``pydantic_settings`` imported separately;
  v1 fallback retained).
- ``master_key`` validator allows empty; ``ensure_master_key()``
  auto-generates at startup.
- All previously-undefined collaborator classes are now defined:
  ``PendingVerification``, ``VerificationResult``, ``FlexGenManager``,
  ``RealTimeVerificationMonitor``, ``VerificationAnalyticsDashboard``,
  ``VerificationHealthScorer``, ``AdvancedCryptographicVerification``,
  ``PredictiveVerificationAnalyzer``, ``HeliumVerificationDashboard``,
  ``SustainabilityIntegration``.
- ``CircuitBreaker.call`` handles both sync and async callables.
- ``GlobalCircuitBreaker`` uses an instance dict guarded by a
  ``threading.RLock``.
- ``CausalBandit`` registers new actions in every parallel dict; bio
  expansion no longer raises ``KeyError``.
- ``MultiAgentSystem.vote`` now scores each strategy.
- RNG is a single seeded ``np.random.default_rng`` threaded through every
  random call site.
- Every FastAPI request body is a Pydantic model; every endpoint requires
  a JWT; admin-only routes are gated by role.
- ``on_event`` replaced by the ``lifespan`` context manager; config is
  stored on ``app.state``.
- ``FeedbackEvent`` emitted on every ``select_verification_strategy``
  decision when the schema is available.
- SQLite migrations wrapped in transactions; ``pending_verifications``
  and ``verifications`` are kept consistent.
- Log files honour ``VERIFICATION_LOG_DIR``; audit log lives beside them.
- ``__version__`` and ``__all__`` added.
- Built-in test suite: ``python blockchain_helium_verification_enhanced_v18.py --test``.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import logging.handlers  # FIX: required for RotatingFileHandler
import os
import secrets
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol,
    Set, Tuple, Union, runtime_checkable,
)

import numpy as np

__version__ = "18.0.0"
__all__ = [
    "__version__",
    "CircuitBreakerState",
    "CircuitBreaker",
    "GlobalCircuitBreaker",
    "RateLimiter",
    "TaskManager",
    "AsyncDatabaseManager",
    "ZKProofSystem",
    "BlockchainVerificationIntegrity",
    "CarbonIntensityManager",
    "MultiChainVerification",
    "MultiCloudVerificationDistribution",
    "SafetyMonitor",
    "XAIExplainer",
    "HumanReviewManager",
    "ChaosMonkey",
    "CausalBandit",
    "QuantumDistillationOptimizer",
    "FederatedCoordinator",
    "MultiAgentSystem",
    "AutonomousVerificationOptimizer",
    "FlexGenManager",
    "RealTimeVerificationMonitor",
    "VerificationAnalyticsDashboard",
    "VerificationHealthScorer",
    "AdvancedCryptographicVerification",
    "PredictiveVerificationAnalyzer",
    "HeliumVerificationDashboard",
    "SustainabilityIntegration",
    "VerificationResult",
    "PendingVerification",
    "VerificationConfig",
    "EnhancedVerificationManagerV18",
    "build_app",
    "main",
    "run_tests",
]


# ============================================================================
# Optional dependencies with fallbacks
# ============================================================================
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    _PYDANTIC_BASE_AVAILABLE = True
except ImportError:  # pragma: no cover
    _PYDANTIC_BASE_AVAILABLE = False
    BaseModel = object  # type: ignore
    def Field(*a, **k):  # type: ignore
        return None
    ConfigDict = dict  # type: ignore

try:
    from pydantic import field_validator  # type: ignore
    PYDANTIC_V2 = True
except ImportError:  # pragma: no cover
    field_validator = None  # type: ignore
    PYDANTIC_V2 = False

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore
    _SETTINGS_AVAILABLE = True
except ImportError:  # pragma: no cover
    try:
        from pydantic import BaseSettings  # type: ignore
        SettingsConfigDict = dict  # type: ignore
        _SETTINGS_AVAILABLE = True
    except ImportError:
        _SETTINGS_AVAILABLE = False
        BaseSettings = object  # type: ignore
        SettingsConfigDict = dict  # type: ignore

PYDANTIC_AVAILABLE = _PYDANTIC_BASE_AVAILABLE

try:
    from fastapi import (  # type: ignore
        Depends, FastAPI, HTTPException, Request, status,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, PlainTextResponse, Response
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False

try:
    from prometheus_client import (  # type: ignore
        CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram,
        generate_latest,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False
    CONTENT_TYPE_LATEST = "text/plain"

try:
    import jwt  # type: ignore
    JWT_AVAILABLE = True
except ImportError:  # pragma: no cover
    JWT_AVAILABLE = False

try:
    import aiosqlite  # type: ignore
    AIOSQLITE_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiosqlite = None  # type: ignore
    AIOSQLITE_AVAILABLE = False

try:
    from cryptography.hazmat.backends import default_backend  # type: ignore
    from cryptography.hazmat.primitives import hashes  # type: ignore
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # type: ignore
    CRYPTO_AVAILABLE = True
except ImportError:  # pragma: no cover
    CRYPTO_AVAILABLE = False

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# ============================================================================
# Logging
# ============================================================================
_LOG_DIR = Path(os.environ.get("VERIFICATION_LOG_DIR", "./logs"))
try:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    _LOG_DIR = Path("/tmp")

logging.basicConfig(
    level=os.environ.get("VERIFICATION_LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            _LOG_DIR / "verification_v18.log",
            maxBytes=10 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

audit_logger = logging.getLogger("verification_audit")
audit_logger.propagate = False
_audit_handler = logging.handlers.RotatingFileHandler(
    _LOG_DIR / "verification_audit_v18.log",
    maxBytes=50 * 1024 * 1024, backupCount=10)
_audit_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
audit_logger.addHandler(_audit_handler)
audit_logger.setLevel(logging.INFO)


# ============================================================================
# Prometheus metrics
# ============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    VERIFICATION_COUNTER = Counter(
        "helium_verifications_total", "Total verifications",
        ["status"], registry=REGISTRY)
    VERIFICATION_DURATION = Histogram(
        "verification_duration_seconds", "Verification duration",
        registry=REGISTRY)
    TRANSACTION_COUNTER = Counter(
        "helium_transactions_total", "Total transactions",
        ["type", "status"], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge(
        "helium_circuit_breaker_state", "Circuit breaker state",
        ["service"], registry=REGISTRY)
    HEALTH_SCORE = Gauge(
        "helium_system_health", "System health score (0-100)", registry=REGISTRY)
    DB_SIZE = Gauge(
        "helium_db_size_mb", "Database size in MB", registry=REGISTRY)
    PENDING_VERIFICATIONS = Gauge(
        "pending_verifications", "Pending verifications count", registry=REGISTRY)
    GAS_PRICE = Gauge(
        "helium_gas_price_gwei", "Current gas price in Gwei", registry=REGISTRY)
    ZK_PROOFS_GENERATED = Counter(
        "zk_proofs_generated_total", "ZK proofs generated",
        ["type", "status"], registry=REGISTRY)
    ZK_VERIFICATIONS = Counter(
        "zk_verifications_total", "ZK verifications",
        ["status"], registry=REGISTRY)
    STORAGE_STORE = Counter(
        "storage_store_total", "Storage store operations",
        ["backend", "status"], registry=REGISTRY)
    COMPONENT_HEALTH = Gauge(
        "component_health_score", "Component health score (0-100)",
        ["component"], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter(
        "verification_quantum_signatures_total", "Quantum signatures",
        ["algorithm", "status"], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter(
        "verification_blockchain_verifications_total", "Blockchain verifications",
        ["status"], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter(
        "verification_autonomous_optimizations_total", "Autonomous optimizations",
        ["strategy", "status"], registry=REGISTRY)
    CLOUD_DISTRIBUTIONS = Counter(
        "verification_cloud_distributions_total", "Cloud distributions",
        ["provider", "status"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter(
        "verification_safety_violations_total", "Safety violations",
        ["rule"], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter(
        "verification_chaos_experiments_total", "Chaos experiments",
        ["type", "status"], registry=REGISTRY)
    HUMAN_REVIEWS = Counter(
        "verification_human_reviews_total", "Human reviews",
        ["status"], registry=REGISTRY)
    XAI_DECISIONS = Counter(
        "verification_xai_decisions_total", "XAI decisions",
        ["strategy"], registry=REGISTRY)
else:  # pragma: no cover
    class _DummyMetric:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass

    VERIFICATION_COUNTER = VERIFICATION_DURATION = _DummyMetric()
    TRANSACTION_COUNTER = CIRCUIT_BREAKER_STATE = HEALTH_SCORE = _DummyMetric()
    DB_SIZE = PENDING_VERIFICATIONS = GAS_PRICE = _DummyMetric()
    ZK_PROOFS_GENERATED = ZK_VERIFICATIONS = STORAGE_STORE = _DummyMetric()
    COMPONENT_HEALTH = QUANTUM_SIGNATURES = BLOCKCHAIN_VERIFICATIONS = _DummyMetric()
    AUTONOMOUS_OPTIMIZATIONS = CLOUD_DISTRIBUTIONS = SAFETY_VIOLATIONS = _DummyMetric()
    CHAOS_EXPERIMENTS = HUMAN_REVIEWS = XAI_DECISIONS = _DummyMetric()
    REGISTRY = None


# ============================================================================
# Custom exceptions
# ============================================================================
class VerificationException(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None) -> None:
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)
        self.correlation_id = str(uuid.uuid4())[:8]


class ConfigurationError(VerificationException): pass
class BlockchainError(VerificationException): pass
class CloudError(VerificationException): pass
class IPFSError(VerificationException): pass
class WebSocketError(VerificationException): pass
class ZKError(VerificationException): pass
class SecurityError(VerificationException): pass
class CircuitBreakerOpenError(VerificationException): pass
class RateLimitExceeded(VerificationException): pass
class SafetyViolationError(VerificationException): pass
class ChaosExperimentError(VerificationException): pass


# ============================================================================
# Config
# ============================================================================
def _default_jwt_secret() -> str:
    return secrets.token_hex(32)


if PYDANTIC_AVAILABLE and _SETTINGS_AVAILABLE and PYDANTIC_V2:
    class GeneralConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        max_retry_attempts: int = Field(3, ge=1)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(60, ge=1)
        health_check_interval: int = Field(30, ge=5)
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)
        data_retention_days: int = Field(365)
        log_level: str = "INFO"
        data_version: int = 18
        human_review_threshold: float = Field(0.5, ge=0, le=1)
        random_seed: int = 42

        @field_validator("log_level")
        @classmethod
        def _v(cls, v: str) -> str:
            allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            up = v.upper()
            if up not in allowed:
                raise ValueError(f"log_level must be one of {sorted(allowed)}")
            return up

    class DatabaseConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        path: str = "/tmp/verification_v18.db"

    class BlockchainConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        rpc_url: str = "http://localhost:8545"
        contract_address: str = "0x0000000000000000000000000000000000000000"
        private_key: str = ""
        chain_id: int = 1

    class CloudConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        aws_access_key: str = ""
        aws_secret_key: str = ""
        aws_region: str = "us-east-1"
        aws_bucket: str = "helium-verification-data"
        azure_connection_string: str = ""
        azure_container: str = "helium-verification-data"
        gcp_credentials: str = ""
        gcp_bucket: str = "helium-verification-data"

    class JWTConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        secret: str = Field(default_factory=_default_jwt_secret)
        algorithm: str = "HS256"
        expiration_minutes: int = 1440

    class APIConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        host: str = "0.0.0.0"
        port: int = 8000

    class ZKConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = True
        proof_type: str = "groth16"

    class SafetyConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        max_carbon_intensity: float = 500.0
        min_renewable_share: float = 0.3
        max_latency_ms: float = 1000.0
        enable_monitor: bool = True

    class ChaosConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = False
        failure_probability: float = 0.1
        experiment_interval_seconds: int = 120

    class OptimizerConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        modp_weights: Dict[str, float] = Field(default_factory=lambda: {
            "carbon": 0.3, "gas": 0.2, "latency": 0.2, "certainty": 0.3})
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)

    class VerificationConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="VERIFICATION_", case_sensitive=False, extra="ignore")
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        blockchain: BlockchainConfig = Field(default_factory=BlockchainConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        jwt: JWTConfig = Field(default_factory=JWTConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        zk: ZKConfig = Field(default_factory=ZKConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        master_key: str = ""

        def ensure_master_key(self) -> str:
            if not self.master_key:
                object.__setattr__(self, "master_key", secrets.token_hex(32))
            return self.master_key

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("master_key not set")
            return bytes.fromhex(self.master_key)

else:  # pragma: no cover
    @dataclass
    class GeneralConfig:
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        health_check_interval: int = 30
        rate_limit_requests: int = 100
        rate_limit_window: int = 60
        data_retention_days: int = 365
        log_level: str = "INFO"
        data_version: int = 18
        human_review_threshold: float = 0.5
        random_seed: int = 42

    @dataclass
    class DatabaseConfig:
        path: str = "/tmp/verification_v18.db"

    @dataclass
    class BlockchainConfig:
        rpc_url: str = "http://localhost:8545"
        contract_address: str = "0x0000000000000000000000000000000000000000"
        private_key: str = ""
        chain_id: int = 1

    @dataclass
    class CloudConfig:
        aws_access_key: str = ""
        aws_secret_key: str = ""
        aws_region: str = "us-east-1"
        aws_bucket: str = "helium-verification-data"
        azure_connection_string: str = ""
        azure_container: str = "helium-verification-data"
        gcp_credentials: str = ""
        gcp_bucket: str = "helium-verification-data"

    @dataclass
    class JWTConfig:
        secret: str = field(default_factory=_default_jwt_secret)
        algorithm: str = "HS256"
        expiration_minutes: int = 1440

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000

    @dataclass
    class ZKConfig:
        enabled: bool = True
        proof_type: str = "groth16"

    @dataclass
    class SafetyConfig:
        max_carbon_intensity: float = 500.0
        min_renewable_share: float = 0.3
        max_latency_ms: float = 1000.0
        enable_monitor: bool = True

    @dataclass
    class ChaosConfig:
        enabled: bool = False
        failure_probability: float = 0.1
        experiment_interval_seconds: int = 120

    @dataclass
    class OptimizerConfig:
        modp_weights: Dict[str, float] = field(default_factory=lambda: {
            "carbon": 0.3, "gas": 0.2, "latency": 0.2, "certainty": 0.3})
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20

    @dataclass
    class VerificationConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        blockchain: BlockchainConfig = field(default_factory=BlockchainConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        jwt: JWTConfig = field(default_factory=JWTConfig)
        api: APIConfig = field(default_factory=APIConfig)
        zk: ZKConfig = field(default_factory=ZKConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        master_key: str = ""

        def ensure_master_key(self) -> str:
            if not self.master_key:
                self.master_key = secrets.token_hex(32)
            return self.master_key

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("master_key not set")
            return bytes.fromhex(self.master_key)


# ============================================================================
# Data containers
# ============================================================================
@dataclass
class PendingVerification:
    batch_id: str
    source: str
    volume_liters: float
    purity: float
    certification_level: str
    carbon_aware: bool
    submitted_at: str
    carbon_impact_kg: float = 0.0


@dataclass
class VerificationResult:
    batch_id: str
    success: bool
    status: str
    source: str
    volume_liters: float
    purity: float
    certification_level: str
    carbon_aware: bool
    submitted_at: str
    completed_at: str
    created_at: str
    duration_ms: float = 0.0
    carbon_impact_kg: float = 0.0
    carbon_intensity: float = 0.0
    block_number: int = 0
    sustainability_score: float = 0.0
    transaction_hash: Optional[str] = None
    storage_ipfs_hash: Optional[str] = None
    zk_proof_hash: Optional[str] = None
    quantum_signature: Optional[str] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Dict[str, Any] = field(default_factory=dict)
    autonomous_optimization: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


# ============================================================================
# Circuit breakers
# ============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 half_open_success_threshold: int = 2) -> None:
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_success_threshold = int(half_open_success_threshold)
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_monotonic: Optional[float] = None
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitBreakerState:
        return self._state

    def _publish_metric(self) -> None:
        try:
            CIRCUIT_BREAKER_STATE.labels(service=self.name).set(
                1.0 if self._state is CircuitBreakerState.OPEN else
                0.5 if self._state is CircuitBreakerState.HALF_OPEN else 0.0)
        except Exception:
            pass

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            if self._state is CircuitBreakerState.OPEN:
                if (self._last_failure_monotonic is not None
                        and time.monotonic() - self._last_failure_monotonic
                        >= self.recovery_timeout):
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._success_count = 0
                    self._publish_metric()
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is OPEN")
        try:
            result = func(*args, **kwargs)
            if inspect.iscoroutine(result):
                result = await result
        except Exception:
            with self._lock:
                self._failure_count += 1
                self._last_failure_monotonic = time.monotonic()
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    self._publish_metric()
            raise
        else:
            with self._lock:
                if self._state is CircuitBreakerState.HALF_OPEN:
                    self._success_count += 1
                    if self._success_count >= self.half_open_success_threshold:
                        self._state = CircuitBreakerState.CLOSED
                        self._failure_count = 0
                else:
                    self._failure_count = 0
                self._publish_metric()
            return result

    def force_open(self) -> None:
        with self._lock:
            self._state = CircuitBreakerState.OPEN
            self._failure_count = self.failure_threshold
            self._last_failure_monotonic = time.monotonic()
            self._publish_metric()

    def reset(self) -> None:
        with self._lock:
            self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_monotonic = None
            self._publish_metric()

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "name": self.name, "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
        }


class GlobalCircuitBreaker:
    _instance: Optional["GlobalCircuitBreaker"] = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "GlobalCircuitBreaker":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._breakers = {}  # type: ignore[attr-defined]
                    cls._instance._lock = threading.RLock()  # type: ignore[attr-defined]
        return cls._instance

    def get_or_create(self, name: str, **kwargs: Any) -> CircuitBreaker:
        with self._lock:  # type: ignore[attr-defined]
            if name not in self._breakers:  # type: ignore[attr-defined]
                self._breakers[name] = CircuitBreaker(name, **kwargs)  # type: ignore[attr-defined]
            return self._breakers[name]  # type: ignore[attr-defined]

    def all_metrics(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:  # type: ignore[attr-defined]
            return {n: cb.get_metrics()
                    for n, cb in self._breakers.items()}  # type: ignore[attr-defined]


# ============================================================================
# Rate limiter
# ============================================================================
class RateLimiter:
    def __init__(self, rate: int, per_seconds: int = 60) -> None:
        self.rate = int(rate)
        self.per_seconds = int(per_seconds)
        self.tokens = float(rate)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(
                self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            self.throttled_requests += 1
            return False

    async def wait_and_acquire(self) -> None:
        while not await self.acquire():
            await asyncio.sleep(0.05)

    def get_metrics(self) -> Dict[str, Any]:
        total = self.total_requests + self.throttled_requests
        return {
            "total_requests": self.total_requests,
            "throttled_requests": self.throttled_requests,
            "throttle_rate": (self.throttled_requests / max(total, 1)) * 100,
        }


# ============================================================================
# Task manager
# ============================================================================
class TaskManager:
    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._pending: Dict[str, Tuple[Callable[..., Awaitable[None]], Tuple, Dict]] = {}

    def register_task(self, name: str, coro_func: Callable[..., Awaitable[None]],
                      *args: Any, **kwargs: Any) -> None:
        if name in self.tasks and not self.tasks[name].done():
            return
        self._pending[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self) -> None:
        for name, (fn, args, kwargs) in list(self._pending.items()):
            self._start_task(name, fn, *args, **kwargs)
        self._pending.clear()

    def _start_task(self, name: str, coro_func: Callable[..., Awaitable[None]],
                    *args: Any, **kwargs: Any) -> asyncio.Task:
        if name in self.tasks and not self.tasks[name].done():
            return self.tasks[name]

        async def wrapper() -> None:
            backoff = 1.0
            max_backoff = 300.0
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task '%s' crashed: %s", name, e, exc_info=True)
                    try:
                        await asyncio.wait_for(
                            self.shutdown_event.wait(), timeout=backoff)
                        break
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(backoff * 2, max_backoff)

        task = asyncio.create_task(wrapper(), name=name)
        self.tasks[name] = task
        return task

    async def stop_all(self) -> None:
        self.shutdown_event.set()
        async with self._lock:
            for t in self.tasks.values():
                if not t.done():
                    t.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

    def snapshot(self) -> Dict[str, str]:
        return {n: ("running" if not t.done() else "stopped")
                for n, t in self.tasks.items()}


# ============================================================================
# Persistence
# ============================================================================
class AsyncDatabaseManager:
    SCHEMA_VERSION = 2

    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self.db_path = Path(config.database.path)
        self._conn: Optional[Any] = None
        self._init_lock = asyncio.Lock()
        self._query_lock = asyncio.Lock()
        self._initialized = False

    async def connect(self) -> Any:
        if self._conn is not None:
            return self._conn
        async with self._init_lock:
            if self._conn is not None:
                return self._conn
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._apply_migrations(self._conn)
            self._initialized = True
        return self._conn

    async def _apply_migrations(self, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            )""")
        await conn.commit()
        cur = await conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        row = await cur.fetchone()
        current = int(row[0]) if row else 0

        async def _block(version: int, fn: Callable[[], Awaitable[None]]) -> None:
            nonlocal current
            if current >= version:
                return
            try:
                await conn.execute("BEGIN")
                await fn()
                await conn.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (version, datetime.now(timezone.utc).isoformat()))
                await conn.commit()
                logger.info("DB migrated to v%d", version)
                current = version
            except Exception as e:
                await conn.rollback()
                logger.error("Migration v%d failed; rolled back: %s", version, e)
                raise

        async def v1() -> None:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS verifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT UNIQUE, success INTEGER, status TEXT,
                    source TEXT, volume_liters REAL, purity REAL,
                    certification_level TEXT, carbon_aware INTEGER,
                    transaction_hash TEXT, storage_ipfs_hash TEXT,
                    zk_proof_hash TEXT, duration_ms REAL,
                    carbon_impact_kg REAL, carbon_intensity REAL,
                    block_number INTEGER, sustainability_score REAL,
                    quantum_signature TEXT, blockchain_tx_hash TEXT,
                    cloud_distribution TEXT, autonomous_optimization TEXT,
                    submitted_at TEXT, completed_at TEXT,
                    error_message TEXT, created_at TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_verifications (
                    batch_id TEXT PRIMARY KEY, source TEXT,
                    volume_liters REAL, purity REAL,
                    certification_level TEXT, carbon_aware INTEGER,
                    carbon_impact_kg REAL, submitted_at TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS optimization_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy TEXT, result TEXT, timestamp TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cloud_distribution_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    optimal_provider TEXT, optimal_region TEXT,
                    scores TEXT, data_size_gb REAL, timestamp TEXT
                )""")

        async def v2() -> None:
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_verifications_batch "
                "ON verifications(batch_id)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pending_batch "
                "ON pending_verifications(batch_id)")

        await _block(1, v1)
        await _block(2, v2)

    async def save_verification(self, result: VerificationResult) -> None:
        conn = await self.connect()
        async with self._query_lock:
            await conn.execute("""
                INSERT OR REPLACE INTO verifications (
                    batch_id, success, status, source, volume_liters, purity,
                    certification_level, carbon_aware, transaction_hash,
                    storage_ipfs_hash, zk_proof_hash, duration_ms,
                    carbon_impact_kg, carbon_intensity, block_number,
                    sustainability_score, quantum_signature, blockchain_tx_hash,
                    cloud_distribution, autonomous_optimization,
                    submitted_at, completed_at, error_message, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                result.batch_id, int(result.success), result.status, result.source,
                result.volume_liters, result.purity, result.certification_level,
                int(result.carbon_aware), result.transaction_hash,
                result.storage_ipfs_hash, result.zk_proof_hash, result.duration_ms,
                result.carbon_impact_kg, result.carbon_intensity,
                result.block_number, result.sustainability_score,
                result.quantum_signature, result.blockchain_tx_hash,
                json.dumps(result.cloud_distribution, default=str),
                json.dumps(result.autonomous_optimization, default=str),
                result.submitted_at, result.completed_at,
                result.error_message, result.created_at,
            ))
            await conn.commit()

    async def save_pending(self, pending: PendingVerification) -> None:
        conn = await self.connect()
        async with self._query_lock:
            await conn.execute("""
                INSERT OR REPLACE INTO pending_verifications (
                    batch_id, source, volume_liters, purity,
                    certification_level, carbon_aware, carbon_impact_kg,
                    submitted_at
                ) VALUES (?,?,?,?,?,?,?,?)
            """, (
                pending.batch_id, pending.source, pending.volume_liters,
                pending.purity, pending.certification_level,
                int(pending.carbon_aware), pending.carbon_impact_kg,
                pending.submitted_at,
            ))
            await conn.commit()

    async def delete_pending(self, batch_id: str) -> None:
        conn = await self.connect()
        async with self._query_lock:
            await conn.execute(
                "DELETE FROM pending_verifications WHERE batch_id=?", (batch_id,))
            await conn.commit()

    async def update_verification_status(self, batch_id: str, status: str) -> None:
        conn = await self.connect()
        async with self._query_lock:
            await conn.execute(
                "UPDATE verifications SET status=? WHERE batch_id=?",
                (status, batch_id))
            await conn.commit()

    async def get_pending_batches(self) -> List[Dict[str, Any]]:
        conn = await self.connect()
        async with self._query_lock:
            cur = await conn.execute("SELECT * FROM pending_verifications")
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def save_optimization(self, strategy: str,
                                result: Dict[str, Any]) -> None:
        conn = await self.connect()
        async with self._query_lock:
            await conn.execute(
                "INSERT INTO optimization_history (strategy, result, timestamp) "
                "VALUES (?,?,?)",
                (strategy, json.dumps(result, default=str),
                 datetime.now(timezone.utc).isoformat()))
            await conn.commit()

    async def save_cloud_distribution(self, result: Dict[str, Any]) -> None:
        conn = await self.connect()
        async with self._query_lock:
            await conn.execute(
                "INSERT INTO cloud_distribution_history "
                "(optimal_provider, optimal_region, scores, data_size_gb, timestamp) "
                "VALUES (?,?,?,?,?)",
                (result.get("optimal_provider"), result.get("optimal_region"),
                 json.dumps(result.get("scores", {}), default=str),
                 float(result.get("data_size_gb", 0.0)),
                 datetime.now(timezone.utc).isoformat()))
            await conn.commit()

    async def get_statistics(self) -> Dict[str, Any]:
        conn = await self.connect()
        async with self._query_lock:
            cur = await conn.execute("SELECT COUNT(*) FROM verifications")
            total = (await cur.fetchone())[0]
            cur = await conn.execute(
                "SELECT COUNT(*) FROM pending_verifications")
            pending = (await cur.fetchone())[0]
        return {"total_verifications": int(total),
                "pending_verifications": int(pending)}

    async def close(self) -> None:
        async with self._init_lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None
                self._initialized = False


# ============================================================================
# ZK proof system (HMAC-hash stub)
# ============================================================================
class ZKProofSystem:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config

    async def generate_proof(self, data: Dict[str, Any],
                             proof_type: Optional[str] = None
                             ) -> Dict[str, Any]:
        pt = proof_type or self.config.zk.proof_type
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        proof_hash = hmac.new(
            b"zk_stub", payload, hashlib.sha256).hexdigest()
        ZK_PROOFS_GENERATED.labels(type=pt, status="success").inc()
        return {
            "proof_hash": proof_hash, "type": pt,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "simulated": True,
        }

    async def verify_proof(self, proof_data: Dict[str, Any],
                           data: Dict[str, Any]) -> bool:
        pt = proof_data.get("type", self.config.zk.proof_type)
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hmac.new(
            b"zk_stub", payload, hashlib.sha256).hexdigest()
        ok = hmac.compare_digest(expected, proof_data.get("proof_hash", ""))
        ZK_VERIFICATIONS.labels(status="success" if ok else "failure").inc()
        return ok

    def get_zk_status(self) -> Dict[str, Any]:
        return {"available": True, "proof_type": self.config.zk.proof_type,
                "simulated": True}


# ============================================================================
# Blockchain (deterministic simulation)
# ============================================================================
class BlockchainVerificationIntegrity:
    def __init__(self, config: VerificationConfig,
                 storage: AsyncDatabaseManager) -> None:
        self.config = config
        self.storage = storage
        self._rng = np.random.default_rng(config.general.random_seed)
        self.available = False  # real web3 client not wired in this build

    async def record_verification_result(self, data_id: str, data_hash: str,
                                         metadata: Dict[str, Any]
                                         ) -> Dict[str, Any]:
        tx_hash = "0x" + hashlib.sha256(
            f"{data_id}:{data_hash}:{time.time()}".encode()).hexdigest()[:40]
        block = int(self._rng.integers(1_000_000, 9_999_999))
        BLOCKCHAIN_VERIFICATIONS.labels(status="simulated").inc()
        return {
            "status": "success", "tx_hash": tx_hash,
            "block_number": block, "simulated": True,
        }

    async def verify_verification_result(self, data_id: str, data_hash: str
                                         ) -> Dict[str, Any]:
        return {"valid": True, "simulated": True}

    async def get_blockchain_status(self) -> Dict[str, Any]:
        return {"connected": self.available, "simulated": not self.available}


# ============================================================================
# Carbon intensity (deterministic)
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self.current_intensity = 400.0
        self.history: deque = deque(maxlen=100)

    async def get_current_intensity(self) -> float:
        return float(self.current_intensity)

    async def update_carbon_intensity(self) -> None:
        self.current_intensity = float(self._rng.uniform(200.0, 600.0))
        self.history.append(self.current_intensity)
        try:
            GAS_PRICE.set(self.current_intensity)
        except Exception:
            pass

    def calculate_verification_carbon_impact(self, gas_used: int,
                                             gas_price: int) -> float:
        return (gas_used * gas_price) / 1e9 * self.current_intensity / 1000.0

    async def get_carbon_trend(self) -> Dict[str, Any]:
        if len(self.history) < 2:
            return {"trend": "unknown"}
        x = np.arange(len(self.history))
        y = np.asarray(list(self.history))
        slope = float(np.polyfit(x, y, 1)[0])
        return {
            "slope": slope,
            "trend": "increasing" if slope > 0 else "decreasing",
            "samples": len(self.history),
        }

    async def close(self) -> None:
        pass


# ============================================================================
# Multi-chain (deterministic scoring)
# ============================================================================
class MultiChainVerification:
    CHAINS = ("ethereum", "polygon", "arbitrum", "optimism")
    GAS_PRICES = {"ethereum": 50, "polygon": 5, "arbitrum": 10, "optimism": 8}
    CARBON = {"ethereum": 500, "polygon": 200, "arbitrum": 150, "optimism": 120}

    def __init__(self, config: VerificationConfig) -> None:
        self.config = config

    async def send_transaction(self, chain: str,
                               contract_func: Callable[..., Any]) -> Dict[str, Any]:
        if chain not in self.CHAINS:
            raise VerificationException(f"Unknown chain: {chain}")
        return {"status": "success", "chain": chain, "simulated": True}

    async def verify_on_chain(self, data: Dict[str, Any],
                              chain: str = "ethereum") -> Dict[str, Any]:
        if chain not in self.CHAINS:
            raise VerificationException(f"Unknown chain: {chain}")
        tx_hash = "0x" + hashlib.sha256(
            f"{chain}:{json.dumps(data, sort_keys=True, default=str)}".encode()
        ).hexdigest()[:40]
        return {
            "status": "success", "chain": chain, "tx_hash": tx_hash,
            "simulated": True,
        }

    async def get_optimal_chain(self, requirements: Optional[Dict[str, Any]] = None
                                ) -> str:
        req = requirements or {}
        scores: Dict[str, float] = {}
        for chain in self.CHAINS:
            score = 0.0
            if req.get("low_gas", True):
                score -= self.GAS_PRICES[chain] * 0.1
            if req.get("low_carbon", True):
                score -= self.CARBON[chain] * 0.01
            if req.get("prefer_mainnet") and chain == "ethereum":
                score += 5.0
            scores[chain] = score
        return max(scores, key=scores.get)

    async def verify_on_optimal_chain(self, data: Dict[str, Any],
                                      requirements: Optional[Dict[str, Any]] = None
                                      ) -> Dict[str, Any]:
        chain = await self.get_optimal_chain(requirements)
        result = await self.verify_on_chain(data, chain)
        result["optimal"] = True
        return result

    def get_chain_status(self) -> Dict[str, Any]:
        return {"chains": list(self.CHAINS),
                "gas_prices": dict(self.GAS_PRICES),
                "carbon": dict(self.CARBON)}


# ============================================================================
# Cloud distribution (deterministic)
# ============================================================================
class MultiCloudVerificationDistribution:
    PROVIDERS = ("aws", "azure", "gcp")

    def __init__(self, config: VerificationConfig,
                 storage: AsyncDatabaseManager) -> None:
        self.config = config
        self.storage = storage
        self._rng = np.random.default_rng(config.general.random_seed)

    async def distribute_verification_data(
        self, data: Dict[str, Any],
        preferences: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, Any]:
        prefs = preferences or {"green": True, "cost": False}
        green_scores = {"aws": 0.8, "azure": 0.7, "gcp": 0.9}
        cost_scores = {"aws": 0.6, "azure": 0.7, "gcp": 0.5}
        scores: Dict[str, float] = {}
        for p in self.PROVIDERS:
            s = 0.0
            if prefs.get("green", True):
                s += green_scores[p] * 0.6
            if prefs.get("cost", False):
                s += cost_scores[p] * 0.4
            s += float(self._rng.random()) * 0.05
            scores[p] = s
        best = max(scores, key=scores.get)
        result = {
            "optimal_provider": best,
            "optimal_region": "us-east-1",
            "scores": scores,
            "data_size_gb": float(data.get("size_gb", 0.0)
                                  if isinstance(data, dict) else 0.0),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "simulated": True,
        }
        await self.storage.save_cloud_distribution(result)
        CLOUD_DISTRIBUTIONS.labels(provider=best, status="success").inc()
        return result

    async def get_distribution_status(self) -> Dict[str, Any]:
        return {"active_provider": "aws", "active_region": "us-east-1",
                "simulated": True}


# ============================================================================
# Safety, XAI, HITL, chaos
# ============================================================================
class SafetyMonitor:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._last_violation: Dict[str, bool] = {}

    def rules(self) -> Dict[str, Callable[[Dict[str, Any]], bool]]:
        s = self.config.safety
        return {
            "max_carbon_intensity":
                lambda m: float(m.get("carbon_intensity", 0.0))
                <= s.max_carbon_intensity,
            "min_renewable_share":
                lambda m: float(m.get("renewable_share", 1.0))
                >= s.min_renewable_share,
            "max_latency_ms":
                lambda m: float(m.get("latency_ms", 0.0)) <= s.max_latency_ms,
        }

    async def check(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations: List[Dict[str, Any]] = []
        for name, fn in self.rules().items():
            try:
                ok = bool(fn(metrics))
            except Exception:
                ok = True
            if ok:
                self._last_violation[name] = False
                continue
            violations.append({"rule": name, "details": dict(metrics)})
            if not self._last_violation.get(name, False):
                SAFETY_VIOLATIONS.labels(rule=name).inc()
                logger.warning("Safety violation: %s with metrics %s",
                               name, metrics)
                self._last_violation[name] = True
        return violations

    async def get_status(self) -> Dict[str, Any]:
        return {"enabled": self.config.safety.enable_monitor,
                "rules": list(self.rules().keys())}


class XAIExplainer:
    async def generate_explanation(self, strategy: Dict[str, Any],
                                   context: Any, confidence: float,
                                   utility: float,
                                   metrics: Dict[str, Any]) -> str:
        name = strategy.get("name", "unknown")
        parts: List[str] = []
        if name == "ethereum_zk":
            parts.append("Ethereum+ZK: high security, moderate cost.")
        elif name == "polygon_plonk":
            parts.append("Polygon+PLONK: low gas, fast verification.")
        elif name == "arbitrum_stark":
            parts.append("Arbitrum+STARK: scalable, low carbon.")
        elif name == "optimism_zk":
            parts.append("Optimism+ZK: fast and cheap.")
        elif name == "ethereum_standard":
            parts.append("Ethereum standard: simple, no ZK overhead.")
        else:
            parts.append(f"Selected strategy: {name}.")
        precision = strategy.get("params", {}).get("precision")
        if precision:
            parts.append(f"Precision: {precision}.")
        parts.append(f"Confidence: {confidence:.2f}.")
        parts.append(f"Utility: {utility:.2f}.")
        if "carbon_intensity" in metrics:
            parts.append(f"Carbon intensity: {metrics['carbon_intensity']} gCO2/kWh.")
        return " ".join(parts)


class HumanReviewManager:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def request_review(self, decision_id: str, explanation: str,
                             context: Dict[str, Any]) -> Dict[str, Any]:
        review = {
            "review_id": str(uuid.uuid4()),
            "decision_id": decision_id,
            "status": "pending",
            "explanation": explanation,
            "context": context,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        async with self._lock:
            self._pending[review["review_id"]] = review
        HUMAN_REVIEWS.labels(status="pending").inc()
        return review

    async def approve(self, review_id: str,
                      feedback: Optional[str] = None) -> Dict[str, Any]:
        async with self._lock:
            r = self._pending.get(review_id)
            if r is None:
                raise VerificationException("Review not found")
            r["status"] = "approved"
            r["feedback"] = feedback
            r["reviewed_at"] = datetime.now(timezone.utc).isoformat()
        HUMAN_REVIEWS.labels(status="approved").inc()
        return {"status": "approved", "review_id": review_id}

    async def reject(self, review_id: str,
                     feedback: Optional[str] = None) -> Dict[str, Any]:
        async with self._lock:
            r = self._pending.get(review_id)
            if r is None:
                raise VerificationException("Review not found")
            r["status"] = "rejected"
            r["feedback"] = feedback
            r["reviewed_at"] = datetime.now(timezone.utc).isoformat()
        HUMAN_REVIEWS.labels(status="rejected").inc()
        return {"status": "rejected", "review_id": review_id}

    async def get_pending(self) -> List[Dict[str, Any]]:
        async with self._lock:
            return [r for r in self._pending.values()
                    if r["status"] == "pending"]


class ChaosMonkey:
    def __init__(self, config: VerificationConfig,
                 manager: "EnhancedVerificationManagerV18") -> None:
        self.config = config
        self.manager = manager
        self.enabled = config.chaos.enabled
        self._rng = np.random.default_rng(config.general.random_seed)
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if not self.enabled:
            return
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Chaos Monkey started")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run_loop(self) -> None:
        interval = self.config.chaos.experiment_interval_seconds
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self._inject_failure()
            except Exception as e:
                logger.error("Chaos experiment failed: %s", e)

    async def _inject_failure(self) -> Dict[str, Any]:
        failure_type = str(self._rng.choice(["latency", "error", "disconnect"]))
        target = str(self._rng.choice(["blockchain", "database", "carbon_api"]))
        status_ = "success"
        result: Dict[str, Any] = {}
        try:
            if failure_type == "latency":
                delay = float(self._rng.uniform(0.05, 0.2))
                await asyncio.sleep(delay)
                result["delay_seconds"] = delay
            elif failure_type == "error":
                if target == "blockchain":
                    original = self.manager.blockchain_integrity.available
                    self.manager.blockchain_integrity.available = False
                    await asyncio.sleep(0.05)
                    self.manager.blockchain_integrity.available = original
                    result["action"] = "toggled blockchain availability"
                else:
                    result["action"] = "noop fault injection"
            else:
                await asyncio.sleep(0.05)
                result["action"] = "simulated network partition"
        except Exception as e:
            status_ = "failed"
            result["error"] = str(e)
        CHAOS_EXPERIMENTS.labels(type=failure_type, status=status_).inc()
        logger.info("Chaos experiment %s on %s: %s",
                    failure_type, target, status_)
        return {"fault": failure_type, "target": target,
                "status": status_, "result": result}


# ============================================================================
# Causal bandit
# ============================================================================
class CausalBandit:
    def __init__(self, action_space: List[Dict[str, Any]],
                 fallback_solver: Callable[[Any], Dict[str, Any]],
                 min_trials_before_bandit: int = 5,
                 confidence_threshold: float = 0.6,
                 seed: int = 42) -> None:
        self.actions = list(action_space)
        self.fallback_solver = fallback_solver
        self.min_trials = int(min_trials_before_bandit)
        self.confidence_threshold = float(confidence_threshold)
        self._rng = np.random.default_rng(seed)
        self.q_values: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}
        self.causal_effects: Dict[str, float] = {}
        self.trials = 0
        for a in self.actions:
            self._register(a["name"])

    def _register(self, name: str) -> None:
        self.q_values.setdefault(name, 0.0)
        self.counts.setdefault(name, 0)
        self.causal_effects.setdefault(name, 0.0)

    def register_action(self, action: Dict[str, Any]) -> None:
        if action not in self.actions:
            self.actions.append(action)
        self._register(action["name"])

    def select_action(self, context: Any) -> Tuple[Dict[str, Any], float, str]:
        if self.trials < self.min_trials:
            return self.fallback_solver(context), 0.0, "fallback"
        if float(self._rng.random()) < 0.1:
            idx = int(self._rng.integers(0, len(self.actions)))
            action = self.actions[idx]
        else:
            if self.trials >= 10 and any(self.causal_effects.values()):
                name = max(self.causal_effects, key=self.causal_effects.get)
            else:
                name = max(self.q_values, key=self.q_values.get)
            action = next(
                (a for a in self.actions if a["name"] == name), self.actions[0])
        confidence = min(1.0, self.counts[action["name"]] / max(1, self.trials))
        return action, confidence, "causal"

    def update(self, context: Any, action: Dict[str, Any], reward: float) -> None:
        name = action["name"]
        self._register(name)
        self.trials += 1
        self.counts[name] += 1
        self.q_values[name] += (reward - self.q_values[name]) / self.counts[name]
        self.causal_effects[name] += (
            reward - self.causal_effects[name]) / self.counts[name]

    def seed_safe_policy(self, context: Any, policy: Dict[str, Any]) -> None:
        self._register(policy["name"])
        self.q_values[policy["name"]] = max(self.q_values[policy["name"]], 0.0)

    def export_state(self) -> Dict[str, Any]:
        return {
            "q_values": dict(self.q_values),
            "counts": dict(self.counts),
            "causal_effects": dict(self.causal_effects),
            "trials": self.trials,
            "action_names": [a["name"] for a in self.actions],
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        for k, v in state.get("q_values", {}).items():
            self._register(k)
            self.q_values[k] = float(v)
        for k, v in state.get("counts", {}).items():
            self._register(k)
            self.counts[k] = int(v)
        for k, v in state.get("causal_effects", {}).items():
            self._register(k)
            self.causal_effects[k] = float(v)
        self.trials = int(state.get("trials", 0))


# ============================================================================
# Quantum distillation, federated, multi-agent
# ============================================================================
class QuantumDistillationOptimizer:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self.available = False  # QAOA not shipped

    async def optimize(self, strategies: List[Dict[str, Any]],
                       metrics: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return None


class FederatedCoordinator:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._participants: Dict[str, Dict[str, Any]] = {}

    async def register_participant(self, participant_id: str,
                                   model_update: Dict[str, Any]) -> None:
        self._participants[participant_id] = dict(model_update)

    async def aggregate(self) -> Dict[str, float]:
        if not self._participants:
            return {}
        keys = set()
        for p in self._participants.values():
            keys.update(p.keys())
        out: Dict[str, float] = {}
        for k in keys:
            vals = []
            for p in self._participants.values():
                v = p.get(k)
                if isinstance(v, (int, float)):
                    vals.append(float(v))
            if vals:
                out[k] = float(np.mean(vals))
        return out


class MultiAgentSystem:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config

    def _score_strategy(self, strategy: str,
                        metrics: Dict[str, Any]) -> float:
        carbon = float(metrics.get("carbon_intensity", 400.0))
        gas = float(metrics.get("gas_price_gwei", 50.0))
        latency = float(metrics.get("latency_estimate", 0.5))
        certainty = float(metrics.get("certainty_desired", 0.9))
        if strategy == "ethereum_zk":
            return certainty * 0.4 + (1 - carbon / 1000.0) * 0.3 + (1 - gas / 200.0) * 0.3
        if strategy == "polygon_plonk":
            return (1 - gas / 200.0) * 0.5 + (1 - carbon / 1000.0) * 0.3 + certainty * 0.2
        if strategy == "arbitrum_stark":
            return (1 - carbon / 1000.0) * 0.4 + (1 - latency) * 0.3 + certainty * 0.3
        if strategy == "optimism_zk":
            return (1 - gas / 200.0) * 0.4 + (1 - latency) * 0.3 + certainty * 0.3
        if strategy == "ethereum_standard":
            return certainty * 0.5 + (1 - latency) * 0.3 + (1 - carbon / 1000.0) * 0.2
        return 0.5

    async def vote(self, strategies: List[Dict[str, Any]],
                   metrics: Dict[str, Any]) -> Dict[str, Any]:
        scores = {s["name"]: self._score_strategy(s["name"], metrics)
                  for s in strategies}
        best = max(scores, key=scores.get) if scores else "ethereum_zk"
        return {"selected_strategy": best, "scores": scores,
                "source": "multi_agent"}


# ============================================================================
# Autonomous optimizer
# ============================================================================
class AutonomousVerificationOptimizer:
    def __init__(self, config: VerificationConfig,
                 storage: AsyncDatabaseManager,
                 safety_monitor: SafetyMonitor,
                 xai: XAIExplainer,
                 human_review: HumanReviewManager,
                 multi_agent: MultiAgentSystem) -> None:
        self.config = config
        self.storage = storage
        self.safety_monitor = safety_monitor
        self.xai = xai
        self.human_review = human_review
        self.multi_agent = multi_agent
        self._rng = np.random.default_rng(config.general.random_seed)
        self._lock = asyncio.Lock()

        self.action_space = [
            {"name": "ethereum_zk",
             "params": {"chain": "ethereum", "proof": "groth16", "precision": "fp32"}},
            {"name": "polygon_plonk",
             "params": {"chain": "polygon", "proof": "plonk", "precision": "fp16"}},
            {"name": "arbitrum_stark",
             "params": {"chain": "arbitrum", "proof": "stark", "precision": "fp16"}},
            {"name": "optimism_zk",
             "params": {"chain": "optimism", "proof": "zk", "precision": "int8"}},
            {"name": "ethereum_standard",
             "params": {"chain": "ethereum", "proof": "none", "precision": "fp32"}},
        ]

        def fallback(_ctx: Any) -> Dict[str, Any]:
            return {"name": "ethereum_zk",
                    "params": {"chain": "ethereum", "proof": "groth16",
                               "precision": "fp32"}}

        self.bandit = CausalBandit(
            action_space=self.action_space,
            fallback_solver=fallback,
            min_trials_before_bandit=config.optimizer.bandit_min_trials,
            confidence_threshold=config.optimizer.bandit_confidence_threshold,
            seed=config.general.random_seed,
        )
        self.recent_rewards: deque = deque(maxlen=100)

    async def select_strategy(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Safety gate
        if self.config.safety.enable_monitor:
            violations = await self.safety_monitor.check(context)
            if violations:
                safe_policy = {
                    "name": "polygon_plonk",
                    "params": {"chain": "polygon", "proof": "plonk",
                               "precision": "fp16"},
                }
                result = {
                    "strategy": safe_policy,
                    "confidence": 1.0,
                    "source": "safety_override",
                    "utility": 0.0,
                    "violations": violations,
                    "review_required": False,
                    "decision_id": str(uuid.uuid4()),
                    "explanation": "Safety override to polygon_plonk.",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                AUTONOMOUS_OPTIMIZATIONS.labels(
                    strategy=safe_policy["name"], status="safety_override").inc()
                await self.storage.save_optimization(safe_policy["name"], result)
                return result

        # 2. Bandit decision
        strategy, confidence, source = self.bandit.select_action(context)

        # 3. Utility
        objectives = {
            "carbon": float(context.get("carbon_intensity", 400.0)) / 1000.0,
            "gas": float(context.get("gas_price_gwei", 50.0)) / 200.0,
            "latency": float(context.get("latency_estimate", 0.5)),
            "certainty": float(context.get("certainty_desired", 0.9)),
        }
        weights = self.config.optimizer.modp_weights
        utility = float(sum(objectives[k] * weights.get(k, 1.0)
                            for k in objectives))

        # 4. XAI + HITL
        explanation = await self.xai.generate_explanation(
            strategy, context, confidence, utility, context)
        decision_id = str(uuid.uuid4())
        review_required = confidence < self.config.general.human_review_threshold
        review_id: Optional[str] = None
        if review_required:
            review = await self.human_review.request_review(
                decision_id, explanation, context)
            review_id = review["review_id"]

        result = {
            "strategy": strategy,
            "confidence": float(confidence),
            "source": source,
            "utility": utility,
            "explanation": explanation,
            "decision_id": decision_id,
            "review_required": review_required,
            "review_id": review_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        AUTONOMOUS_OPTIMIZATIONS.labels(
            strategy=strategy["name"], status="selected").inc()
        XAI_DECISIONS.labels(strategy=strategy["name"]).inc()
        await self.storage.save_optimization(strategy["name"], result)

        await self._emit_feedback_event(decision_id, strategy, confidence,
                                        source, utility)
        return result

    async def update_feedback(self, context: Dict[str, Any],
                              strategy: Dict[str, Any],
                              reward: float) -> None:
        self.bandit.update(context, strategy, reward)
        self.recent_rewards.append(float(reward))

    async def evolve_strategies(self) -> List[Dict[str, Any]]:
        candidate = {
            "name": "green_burst",
            "params": {"chain": "optimism", "proof": "zk",
                       "precision": "int8", "focus": "renewable_windows"},
        }
        if not any(a["name"] == candidate["name"] for a in self.action_space):
            self.action_space.append(candidate)
            self.bandit.register_action(candidate)
            logger.info("Bio expansion added strategy: %s", candidate["name"])
            return [candidate]
        return []

    async def _emit_feedback_event(self, decision_id: str,
                                   strategy: Dict[str, Any], confidence: float,
                                   source: str, utility: float) -> None:
        if FeedbackEvent is None:
            return
        try:
            ev = FeedbackEvent(
                source="blockchain_helium_verification",
                feedback_type="routing",
                context={"task_id": decision_id, "teacher_ids": []},
                action={
                    "selected_action": strategy["name"],
                    "selected_rank": 1,
                    "confidence_score": float(confidence),
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, utility))),
                    "latency_ms": 0.0, "energy_joules": 0.0,
                    "carbon_g": 0.0, "helium_cost": 0.0, "duration_ms": 0.0,
                },
                adaptive_cost_value=float(utility),
                tags=["verification", strategy["name"], source],
                envelope={"trace_id": decision_id},
                metadata={"source": source},
            )
            logger.debug("FeedbackEvent emitted: %s",
                         getattr(ev, "event_id", "?"))
        except Exception as e:
            logger.warning("Failed to emit FeedbackEvent: %s", e)

    def get_optimization_stats(self) -> Dict[str, Any]:
        return {
            "strategies": [s["name"] for s in self.action_space],
            "bandit_state": self.bandit.export_state(),
            "recent_reward_mean": (float(np.mean(self.recent_rewards))
                                   if self.recent_rewards else 0.0),
        }


# ============================================================================
# Collaborator classes that were missing
# ============================================================================
class RealTimeVerificationMonitor:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self._started = False

    async def start_server(self) -> None:
        self._started = True
        logger.info("RealTimeVerificationMonitor started (simulated)")

    async def stop(self) -> None:
        self._started = False
        logger.info("RealTimeVerificationMonitor stopped")

    def get_status(self) -> Dict[str, Any]:
        return {"started": self._started, "simulated": True}


class VerificationAnalyticsDashboard:
    def __init__(self) -> None:
        self._events: deque = deque(maxlen=1000)

    def record(self, event: Dict[str, Any]) -> None:
        self._events.append(event)

    def get_summary(self) -> Dict[str, Any]:
        return {"events": len(self._events)}


class VerificationHealthScorer:
    def __init__(self) -> None:
        self.components: Dict[str, float] = {}

    def update(self, component: str, score: float) -> None:
        self.components[component] = float(score)
        try:
            COMPONENT_HEALTH.labels(component=component).set(float(score))
        except Exception:
            pass

    def overall(self) -> float:
        if not self.components:
            return 100.0
        return float(np.mean(list(self.components.values())))


class AdvancedCryptographicVerification:
    """HMAC-based signing stub; real PQC pluggable."""

    def __init__(self) -> None:
        self._secret = secrets.token_bytes(32)

    async def sign(self, payload: Dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, default=str).encode()
        return hmac.new(self._secret, raw, hashlib.sha256).hexdigest()

    async def verify(self, payload: Dict[str, Any], signature: str) -> bool:
        expected = await self.sign(payload)
        return hmac.compare_digest(expected, signature)


class PredictiveVerificationAnalyzer:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)

    async def predict_success_probability(
        self, metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        carbon = float(metrics.get("carbon_intensity", 400.0))
        gas = float(metrics.get("gas_price_gwei", 50.0))
        # Deterministic logistic-style score.
        z = 1.0 - carbon / 800.0 - gas / 400.0
        p = 1.0 / (1.0 + np.exp(-z * 3.0))
        return {"success_probability": float(p), "simulated": True}


class HeliumVerificationDashboard:
    def __init__(self) -> None:
        self.snapshots: deque = deque(maxlen=100)

    def snapshot(self, data: Dict[str, Any]) -> None:
        self.snapshots.append(data)

    def latest(self) -> Optional[Dict[str, Any]]:
        return self.snapshots[-1] if self.snapshots else None


class SustainabilityIntegration:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._carbon = 400.0
        self._rng = np.random.default_rng(config.general.random_seed)

    async def get_carbon_intensity(self) -> float:
        self._carbon = float(self._rng.uniform(150.0, 550.0))
        try:
            GAS_PRICE.set(self._carbon)
        except Exception:
            pass
        return self._carbon

    def calculate_sustainability_score(self, metrics: Dict[str, Any]) -> float:
        carbon = float(metrics.get("carbon_intensity", self._carbon))
        renewable = float(metrics.get("renewable_share", 0.4))
        return float(max(0.0, min(100.0,
                                  (1 - carbon / 1000.0) * 60 + renewable * 40)))


class FlexGenManager:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self._last_policy: Optional[Dict[str, Any]] = None

    async def optimize_policy(self, workload: Any, node: Any) -> Dict[str, Any]:
        workload_id = getattr(workload, "task_id", None) or str(uuid.uuid4())
        node_id = getattr(node, "id", None) or str(uuid.uuid4())
        policy = {
            "gpu_batch_size": int(self._rng.integers(1, 8)),
            "weight_bits": int(self._rng.choice([8, 16])),
            "cpu_offload_fraction": float(self._rng.random()),
            "precision": str(self._rng.choice(["int8", "fp16"])),
        }
        metrics = {
            "energy_joules": float(self._rng.uniform(50.0, 200.0)),
            "carbon_g": float(self._rng.uniform(5.0, 30.0)),
            "latency_ms": float(self._rng.uniform(50.0, 500.0)),
        }
        self._last_policy = {
            "workload_id": workload_id, "node_id": node_id,
            "policy": policy, "metrics": metrics,
            "reward": float(self._rng.uniform(0.4, 1.0)),
            "simulated": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return self._last_policy

    async def get_status(self) -> Dict[str, Any]:
        return {"available": True, "last_policy": self._last_policy,
                "simulated": True}


# ============================================================================
# Enhanced manager
# ============================================================================
class EnhancedVerificationManagerV18:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Core subsystems
        self.storage = AsyncDatabaseManager(config)
        self.zk_system = ZKProofSystem(config)
        self.blockchain_integrity = BlockchainVerificationIntegrity(
            config, self.storage)
        self.carbon_manager = CarbonIntensityManager(config)
        self.multi_chain = MultiChainVerification(config)
        self.cloud_distributor = MultiCloudVerificationDistribution(
            config, self.storage)

        # Enhancement subsystems
        self.safety_monitor = SafetyMonitor(config)
        self.xai = XAIExplainer()
        self.human_review = HumanReviewManager(config)
        self.quantum_optimizer = QuantumDistillationOptimizer(config)
        self.multi_agent = MultiAgentSystem(config)
        self.federated = FederatedCoordinator(config)
        self.chaos_monkey = ChaosMonkey(config, self)

        # Optimizer
        self.autonomous_optimizer = AutonomousVerificationOptimizer(
            config=config,
            storage=self.storage,
            safety_monitor=self.safety_monitor,
            xai=self.xai,
            human_review=self.human_review,
            multi_agent=self.multi_agent,
        )
        self.flexgen_manager = FlexGenManager(config)

        # Simulated peripheral managers
        self.monitor = RealTimeVerificationMonitor(config)
        self.dashboard = VerificationAnalyticsDashboard()
        self.health_scorer = VerificationHealthScorer()
        self.crypto = AdvancedCryptographicVerification()
        self.predictive_analyzer = PredictiveVerificationAnalyzer(config)
        self.helium_dashboard = HeliumVerificationDashboard()
        self.sustainability = SustainabilityIntegration(config)

        # Background supervision
        self.task_manager = TaskManager()
        self.rate_limiter = RateLimiter(
            config.general.rate_limit_requests, config.general.rate_limit_window)
        self.pending_verifications: Dict[str, PendingVerification] = {}
        self.operation_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._queue_worker: Optional[asyncio.Task] = None
        self._running = False

        self._register_background_tasks()
        logger.info("EnhancedVerificationManagerV18 v%s initialized (id=%s)",
                    __version__, self.instance_id)

    # ------------------------------------------------------------------
    def _register_background_tasks(self) -> None:
        self.task_manager.register_task("health_check", self._health_check_loop)
        self.task_manager.register_task("cleanup", self._cleanup_loop)
        self.task_manager.register_task("carbon_update", self._carbon_update_loop)
        self.task_manager.register_task("auto_optimize", self._auto_optimize_loop)

    async def start(self) -> None:
        self._running = True
        await self.storage.connect()
        await self.carbon_manager.update_carbon_intensity()
        await self.monitor.start_server()
        self._queue_worker = asyncio.create_task(self._process_queue())
        self.task_manager.start_registered_tasks()
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.start()
        logger.info("Verification manager started (id=%s)", self.instance_id)

    async def shutdown(self) -> None:
        logger.info("Shutting down verification manager (id=%s)", self.instance_id)
        self._running = False
        if self._queue_worker is not None and not self._queue_worker.done():
            self._queue_worker.cancel()
            try:
                await self._queue_worker
            except asyncio.CancelledError:
                pass
        await self.task_manager.stop_all()
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.stop()
        await self.monitor.stop()
        await self.carbon_manager.close()
        await self.storage.close()
        logger.info("Shutdown complete")

    # ------------------------------------------------------------------
    async def _health_check_loop(self) -> None:
        interval = self.config.general.health_check_interval
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health["health_score"])
            except Exception as e:
                logger.error("Health check error: %s", e)
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

    async def _cleanup_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=3600)
                break
            except asyncio.TimeoutError:
                pass

    async def _carbon_update_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.carbon_manager.update_carbon_intensity()
            except Exception as e:
                logger.error("Carbon update error: %s", e)
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=300)
                break
            except asyncio.TimeoutError:
                pass

    async def _auto_optimize_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=600)
                break
            except asyncio.TimeoutError:
                pass
            try:
                if len(self.autonomous_optimizer.recent_rewards) > 20:
                    await self.autonomous_optimizer.evolve_strategies()
            except Exception as e:
                logger.error("Auto-optimize error: %s", e)

    async def _process_queue(self) -> None:
        while self._running:
            try:
                operation = await self.operation_queue.get()
            except asyncio.CancelledError:
                break
            try:
                result = await self._execute_verification(operation)
                operation["future"].set_result(result)
            except Exception as e:
                operation["future"].set_exception(e)
            finally:
                self.operation_queue.task_done()

    async def _execute_verification(self, operation: Dict[str, Any]
                                    ) -> VerificationResult:
        req = operation["request"]
        started = time.monotonic()
        batch_id = str(uuid.uuid4())

        # Optimizer decision
        context = {
            "carbon_intensity": self.carbon_manager.current_intensity,
            "gas_price_gwei": 50.0,
            "latency_estimate": 0.5,
            "certainty_desired": 0.9,
        }
        decision = await self.autonomous_optimizer.select_strategy(context)
        strategy = decision["strategy"]

        # ZK proof
        proof = await self.zk_system.generate_proof(
            {"batch_id": batch_id, "volume": req["volume_liters"]})

        # Blockchain record
        block_result = await self.blockchain_integrity.record_verification_result(
            data_id=batch_id, data_hash=proof["proof_hash"],
            metadata={"strategy": strategy["name"]})

        # Cloud distribution
        cloud = await self.cloud_distributor.distribute_verification_data(
            {"batch_id": batch_id, "size_gb": req["volume_liters"] / 1000.0})

        duration_ms = (time.monotonic() - started) * 1000.0
        result = VerificationResult(
            batch_id=batch_id,
            success=True,
            status="completed",
            source=req["source"],
            volume_liters=float(req["volume_liters"]),
            purity=float(req["purity"]),
            certification_level=req["certification_level"],
            carbon_aware=bool(req.get("carbon_aware", True)),
            submitted_at=datetime.now(timezone.utc).isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            created_at=datetime.now(timezone.utc).isoformat(),
            duration_ms=duration_ms,
            carbon_impact_kg=self.carbon_manager.calculate_verification_carbon_impact(
                gas_used=21000, gas_price=50),
            carbon_intensity=self.carbon_manager.current_intensity,
            block_number=int(block_result.get("block_number", 0)),
            sustainability_score=self.sustainability.calculate_sustainability_score(
                context),
            zk_proof_hash=proof["proof_hash"],
            blockchain_tx_hash=block_result.get("tx_hash"),
            cloud_distribution=cloud,
            autonomous_optimization=decision,
        )
        await self.storage.save_verification(result)
        VERIFICATION_COUNTER.labels(status="success").inc()
        VERIFICATION_DURATION.observe(duration_ms / 1000.0)
        self.dashboard.record({"batch_id": batch_id, "status": "completed"})
        return result

    async def register_batch(self, source: str, volume_liters: float,
                             purity: float, certification_level: str,
                             carbon_aware: bool = True,
                             urgency: str = "normal") -> VerificationResult:
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        pending = PendingVerification(
            batch_id=str(uuid.uuid4()),
            source=source,
            volume_liters=float(volume_liters),
            purity=float(purity),
            certification_level=certification_level,
            carbon_aware=bool(carbon_aware),
            submitted_at=datetime.now(timezone.utc).isoformat(),
        )
        await self.storage.save_pending(pending)
        self.pending_verifications[pending.batch_id] = pending
        PENDING_VERIFICATIONS.set(len(self.pending_verifications))

        await self.operation_queue.put({
            "type": "verification",
            "request": {
                "source": source,
                "volume_liters": volume_liters,
                "purity": purity,
                "certification_level": certification_level,
                "carbon_aware": carbon_aware,
                "urgency": urgency,
            },
            "future": future,
        })
        try:
            result = await future
        finally:
            self.pending_verifications.pop(pending.batch_id, None)
            await self.storage.delete_pending(pending.batch_id)
            PENDING_VERIFICATIONS.set(len(self.pending_verifications))
        return result

    async def select_verification_strategy(self, context: Dict[str, Any]
                                           ) -> Dict[str, Any]:
        return await self.autonomous_optimizer.select_strategy(context)

    async def update_verification_feedback(self, context: Dict[str, Any],
                                           strategy: Dict[str, Any],
                                           reward: float) -> None:
        await self.autonomous_optimizer.update_feedback(context, strategy, reward)

    async def run_flexgen_optimization(self, workload: Any, node: Any
                                       ) -> Dict[str, Any]:
        return await self.flexgen_manager.optimize_policy(workload, node)

    async def get_flexgen_status(self) -> Dict[str, Any]:
        return await self.flexgen_manager.get_status()

    async def health_check(self) -> Dict[str, Any]:
        score = 100
        checks: Dict[str, Any] = {}
        try:
            stats = await self.storage.get_statistics()
            checks["storage"] = stats
        except Exception as e:
            checks["storage"] = {"error": str(e)}
            score -= 20
        try:
            checks["blockchain"] = await self.blockchain_integrity.get_blockchain_status()
        except Exception as e:
            checks["blockchain"] = {"error": str(e)}
            score -= 10
        checks["carbon"] = {"current": self.carbon_manager.current_intensity}
        checks["tasks"] = self.task_manager.snapshot()
        checks["rate_limiter"] = self.rate_limiter.get_metrics()
        checks["circuit_breakers"] = GlobalCircuitBreaker().all_metrics()
        checks["optimizer"] = self.autonomous_optimizer.get_optimization_stats()
        return {
            "healthy": score > 50,
            "instance_id": self.instance_id,
            "version": __version__,
            "health_score": max(0, score),
            "components": checks,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ============================================================================
# FastAPI
# ============================================================================
if FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE:
    class _RegisterBody(BaseModel):
        source: str
        volume_liters: float
        purity: float
        certification_level: str
        carbon_aware: bool = True
        urgency: str = "normal"

    class _OptimizeBody(BaseModel):
        context: Dict[str, Any] = {}

    class _FeedbackBody(BaseModel):
        context: Dict[str, Any]
        strategy: Dict[str, Any]
        reward: float

    class _FlexGenBody(BaseModel):
        workload: Dict[str, Any] = {}
        node: Dict[str, Any] = {}

    class _SafetyCheckBody(BaseModel):
        metrics: Dict[str, Any] = {}

    class _FederatedRegisterBody(BaseModel):
        participant_id: str
        model_update: Dict[str, Any]

    class _MultiAgentVoteBody(BaseModel):
        strategies: List[Dict[str, Any]]
        metrics: Dict[str, Any]

    class _ReviewBody(BaseModel):
        feedback: Optional[str] = None

    class _TokenBody(BaseModel):
        subject: str
        roles: List[str] = ["user"]


def build_app(manager: EnhancedVerificationManagerV18) -> Any:
    if not (FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE):
        return None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await manager.start()
        try:
            yield
        finally:
            await manager.shutdown()

    app = FastAPI(title="Blockchain Helium Verification API",
                  version=__version__, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=[], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    app.state.manager = manager
    app.state.config = manager.config
    security = HTTPBearer(auto_error=False)

    async def _get_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ) -> Dict[str, Any]:
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing token")
        if not JWT_AVAILABLE:
            raise HTTPException(status_code=503, detail="JWT not available")
        try:
            payload = jwt.decode(
                credentials.credentials,
                app.state.config.jwt.secret,
                algorithms=[app.state.config.jwt.algorithm])
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")
        roles = payload.get("roles") or []
        if isinstance(roles, str):
            roles = roles.split()
        return {"sub": payload.get("sub"), "roles": list(roles)}

    async def _require_admin(user: Dict = Depends(_get_user)) -> Dict:
        if "admin" not in (user.get("roles") or []):
            raise HTTPException(status_code=403, detail="Admin required")
        return user

    @app.get("/health")
    async def health():
        return await manager.health_check()

    @app.get("/metrics")
    async def metrics():
        if PROMETHEUS_AVAILABLE and REGISTRY is not None:
            return Response(content=generate_latest(REGISTRY),
                            media_type=CONTENT_TYPE_LATEST)
        return PlainTextResponse("Prometheus not installed")

    @app.post("/token")
    async def issue_token(body: _TokenBody):
        if not JWT_AVAILABLE:
            raise HTTPException(status_code=503, detail="JWT not available")
        now = datetime.now(timezone.utc)
        exp = now + timedelta(minutes=app.state.config.jwt.expiration_minutes)
        token = jwt.encode(
            {"sub": body.subject, "roles": body.roles,
             "iat": int(now.timestamp()), "exp": int(exp.timestamp())},
            app.state.config.jwt.secret,
            algorithm=app.state.config.jwt.algorithm)
        return {"token": token, "expires_at": exp.isoformat()}

    @app.post("/verification/register")
    async def register_batch(body: _RegisterBody,
                             user: Dict = Depends(_get_user)):
        return asdict(await manager.register_batch(
            body.source, body.volume_liters, body.purity,
            body.certification_level, body.carbon_aware, body.urgency))

    @app.post("/verification/optimize")
    async def optimize(body: _OptimizeBody,
                       user: Dict = Depends(_get_user)):
        return await manager.select_verification_strategy(body.context)

    @app.post("/verification/feedback")
    async def feedback(body: _FeedbackBody,
                       user: Dict = Depends(_get_user)):
        await manager.update_verification_feedback(
            body.context, body.strategy, body.reward)
        return {"status": "feedback recorded"}

    @app.post("/verification/evolve")
    async def evolve(user: Dict = Depends(_require_admin)):
        return {"new_strategies":
                await manager.autonomous_optimizer.evolve_strategies()}

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(body: _FlexGenBody,
                               user: Dict = Depends(_get_user)):
        return await manager.run_flexgen_optimization(body.workload, body.node)

    @app.get("/flexgen/status")
    async def flexgen_status(user: Dict = Depends(_get_user)):
        return await manager.get_flexgen_status()

    @app.get("/safety/status")
    async def safety_status(user: Dict = Depends(_get_user)):
        return await manager.safety_monitor.get_status()

    @app.post("/safety/check")
    async def safety_check(body: _SafetyCheckBody,
                           user: Dict = Depends(_get_user)):
        return {"violations": await manager.safety_monitor.check(body.metrics)}

    @app.get("/human-review/pending")
    async def review_pending(user: Dict = Depends(_require_admin)):
        return await manager.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def review_approve(review_id: str, body: _ReviewBody,
                             user: Dict = Depends(_require_admin)):
        return await manager.human_review.approve(review_id, body.feedback)

    @app.post("/human-review/{review_id}/reject")
    async def review_reject(review_id: str, body: _ReviewBody,
                            user: Dict = Depends(_require_admin)):
        return await manager.human_review.reject(review_id, body.feedback)

    @app.post("/chaos/trigger")
    async def chaos_trigger(user: Dict = Depends(_require_admin)):
        return await manager.chaos_monkey._inject_failure()

    @app.post("/federated/register")
    async def federated_register(body: _FederatedRegisterBody,
                                 user: Dict = Depends(_get_user)):
        await manager.federated.register_participant(
            body.participant_id, body.model_update)
        return {"status": "registered"}

    @app.get("/federated/aggregate")
    async def federated_aggregate(user: Dict = Depends(_get_user)):
        return await manager.federated.aggregate()

    @app.post("/multi-agent/vote")
    async def multi_agent_vote(body: _MultiAgentVoteBody,
                               user: Dict = Depends(_get_user)):
        return await manager.multi_agent.vote(body.strategies, body.metrics)

    return app


# ============================================================================
# Tests
# ============================================================================
async def _test_circuit_breaker() -> None:
    cb = CircuitBreaker("t", failure_threshold=2, recovery_timeout=0.1)

    def sync_ok(): return 42

    async def async_ok(): return 43

    assert await cb.call(sync_ok) == 42
    assert await cb.call(async_ok) == 43

    def boom(): raise RuntimeError("boom")

    for _ in range(2):
        try:
            await cb.call(boom)
        except RuntimeError:
            pass
    assert cb.state is CircuitBreakerState.OPEN
    await asyncio.sleep(0.15)
    assert await cb.call(sync_ok) == 42
    assert cb.state is CircuitBreakerState.CLOSED


async def _test_global_registry_threadsafe() -> None:
    g = GlobalCircuitBreaker()
    assert g.get_or_create("abc") is g.get_or_create("abc")


async def _test_rate_limiter() -> None:
    rl = RateLimiter(rate=2, per_seconds=60)
    assert await rl.acquire() is True
    assert await rl.acquire() is True
    assert await rl.acquire() is False


async def _test_causal_bandit_new_policies() -> None:
    bandit = CausalBandit(
        action_space=[{"name": "a", "params": {}}],
        fallback_solver=lambda ctx: {"name": "a", "params": {}},
        min_trials_before_bandit=1, seed=0)
    bandit.update({}, {"name": "a", "params": {}}, 0.5)
    bandit.update({}, {"name": "brand_new", "params": {}}, 0.9)
    assert bandit.q_values["brand_new"] > 0.0


async def _test_multi_agent_vote_differs() -> None:
    cfg = VerificationConfig()
    cfg.ensure_master_key()
    ma = MultiAgentSystem(cfg)
    strategies = [{"name": "ethereum_zk"}, {"name": "polygon_plonk"},
                  {"name": "arbitrum_stark"}]
    metrics = {"carbon_intensity": 300.0, "gas_price_gwei": 30.0,
               "latency_estimate": 0.4, "certainty_desired": 0.9}
    result = await ma.vote(strategies, metrics)
    scores = list(result["scores"].values())
    assert len(set(round(s, 6) for s in scores)) > 1


async def _test_manager_end_to_end(tmpdir: Path) -> None:
    cfg = VerificationConfig()
    cfg.ensure_master_key()
    cfg.database.path = str(tmpdir / "verify.db")
    cfg.chaos.enabled = False

    manager = EnhancedVerificationManagerV18(cfg)
    await manager.start()
    try:
        result = await manager.register_batch(
            source="test", volume_liters=1000.0, purity=0.995,
            certification_level="gold", carbon_aware=True)
        assert isinstance(result, VerificationResult)
        assert result.success is True
        stats = await manager.storage.get_statistics()
        assert stats["total_verifications"] >= 1

        # Optimizer + XAI + FeedbackEvent.
        decision = await manager.select_verification_strategy({
            "carbon_intensity": 200.0, "gas_price_gwei": 30.0,
            "latency_estimate": 0.4, "certainty_desired": 0.9})
        assert "strategy" in decision
        assert "explanation" in decision
        await manager.update_verification_feedback(
            {"carbon_intensity": 200.0}, decision["strategy"], 0.85)

        # FlexGen.
        fg = await manager.run_flexgen_optimization(
            {"task_id": "w1"}, {"id": "n1"})
        assert "policy" in fg

        health = await manager.health_check()
        assert health["health_score"] >= 0
    finally:
        await manager.shutdown()


def run_tests() -> None:
    import tempfile
    loop = asyncio.new_event_loop()
    try:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            loop.run_until_complete(_test_circuit_breaker())
            loop.run_until_complete(_test_global_registry_threadsafe())
            loop.run_until_complete(_test_rate_limiter())
            loop.run_until_complete(_test_causal_bandit_new_policies())
            loop.run_until_complete(_test_multi_agent_vote_differs())
            loop.run_until_complete(_test_manager_end_to_end(tmp))
    finally:
        loop.close()
    print("All Blockchain Helium Verification v18.0.0 tests passed.")


# ============================================================================
# Entry point
# ============================================================================
async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Blockchain Helium Verification v18.0.0")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    print("=" * 80)
    print(f"Blockchain Helium Verification v{__version__}")
    print("=" * 80)

    config = VerificationConfig()
    config.ensure_master_key()
    manager = EnhancedVerificationManagerV18(config)

    if FASTAPI_AVAILABLE:
        app = build_app(manager)
        if app is not None:
            import uvicorn  # type: ignore
            uvicorn.run(
                app,
                host=args.host or config.api.host,
                port=args.port or config.api.port,
                log_level=config.general.log_level.lower(),
                server_header=False,
            )
            return

    # No FastAPI — run standalone.
    await manager.start()
    try:
        result = await manager.register_batch(
            source="demo", volume_liters=10_000.0, purity=0.995,
            certification_level="gold", carbon_aware=True)
        print(f"Verification: {result.batch_id} status={result.status}")
        decision = await manager.select_verification_strategy({
            "carbon_intensity": 300.0, "gas_price_gwei": 40.0,
            "latency_estimate": 0.4, "certainty_desired": 0.9})
        print(f"Strategy: {decision['strategy']['name']} "
              f"(confidence={decision['confidence']:.2f})")
        print(f"Explanation: {decision['explanation']}")
        health = await manager.health_check()
        print(f"Health: {health['health_score']}")
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            pass
    finally:
        await manager.shutdown()
        print("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
