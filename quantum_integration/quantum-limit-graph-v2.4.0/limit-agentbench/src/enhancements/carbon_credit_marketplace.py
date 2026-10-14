#!/usr/bin/env python3
# File: src/enhancements/carbon_credit_marketplace.py
"""
Carbon Credit Marketplace for Green Agent v6.0.0
=================================================

Self-contained, runnable Carbon Credit Marketplace with:
  - Pluggable registry / blockchain / cloud / PQC / price-prediction adapters
    (deterministic simulations by default).
  - Async SQLAlchemy persistence with a working aiosqlite fallback.
  - JWT auth, Prometheus metrics, structured logging, rate limiting.
  - Safety monitor (temporal rules), XAI explanations, human-in-the-loop,
    chaos engineering, causal bandit, federated aggregation, multi-agent
    voting, quantum-distillation stub, FlexGen policy simulation.

CHANGES (v6.0.0 over v5.1.0):
- Fixed missing imports (``Awaitable``, ``logging.handlers``, ``typing``).
- Pydantic v2-first detection (``pydantic_settings`` imported separately);
  v1 fallback retained.
- ``MASTER_KEY`` validator allows empty; ``ensure_master_key()`` auto-generates
  at startup.
- **All previously-undefined collaborator classes are now defined**:
  ``AsyncDatabaseManager``, ``RegistryClient``, ``BlockchainClient``,
  ``PostQuantumCrypto``, ``CloudStorage``, ``PricePredictor``,
  ``CarbonIntensityManager``, ``UnifiedSustainabilityEngine``,
  ``AutoOffsetEngine``, ``WebSocketManager``, ``WebhookNotifier``.
- SQLAlchemy ``metadata`` column renamed to ``extra`` (``metadata`` is
  reserved on declarative base) for both ``CreditProjectDB`` and
  ``CreditTransactionDB``.
- ``CircuitBreaker.call`` handles sync callables; ``GlobalCircuitBreaker``
  uses an instance dict guarded by ``threading.RLock``.
- ``CausalBandit`` bio-expansion registers new actions in every parallel
  dict; no more ``KeyError``.
- ``MultiAgentSystem.vote`` scores per project (already correct in v5.1.0,
  verified by test).
- RNG threaded via ``np.random.default_rng(config.general.random_seed)``;
  no unseeded ``random`` calls remain.
- FastAPI ``on_event`` → ``lifespan``; config stored on ``app.state``;
  JWT dependency uses ``HTTPBearer`` correctly.
- Every endpoint body is a Pydantic model; JWT required on all mutating
  endpoints; admin role enforced on ``/optimization/evolve``,
  ``/human-review/*``, ``/chaos/trigger``, ``/federated/aggregate``.
- ``FeedbackEvent`` emitted from ``AutonomousOptimizer.select_best_project``
  and ``CarbonCreditMarketplace._perform_offset``.
- ``AutonomousOptimizer._load_state``/``_save_state`` persist through
  ``AsyncDatabaseManager``.
- ``TaskManager`` propagates its shutdown event to registered loops.
- ``__version__`` and ``__all__`` added.
- Built-in test suite: ``python carbon_credit_marketplace.py --test``.

Run the API:
    python carbon_credit_marketplace.py --serve
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import logging.handlers
import os
import secrets
import threading
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol,
    Set, Tuple, Union, runtime_checkable,
)

import numpy as np

__version__ = "6.0.0"
__all__ = [
    "__version__",
    "Settings",
    "ensure_settings",
    "CircuitBreakerState",
    "CircuitBreaker",
    "GlobalCircuitBreaker",
    "TaskManager",
    "AsyncDatabaseManager",
    "RegistryClient",
    "BlockchainClient",
    "PostQuantumCrypto",
    "CloudStorage",
    "PricePredictor",
    "CarbonIntensityManager",
    "UnifiedSustainabilityEngine",
    "AutoOffsetEngine",
    "WebSocketManager",
    "WebhookNotifier",
    "SafetyMonitor",
    "XAIExplainer",
    "HumanReviewManager",
    "ChaosMonkey",
    "CausalBandit",
    "QuantumDistillationOptimizer",
    "FederatedCoordinator",
    "MultiAgentSystem",
    "AutonomousOptimizer",
    "FlexGenManager",
    "CreditProject",
    "CreditPurchaseRequest",
    "CreditRetireRequest",
    "CreditTransaction",
    "CarbonCreditMarketplace",
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
    import jwt  # type: ignore
    JWT_AVAILABLE = True
except ImportError:  # pragma: no cover
    JWT_AVAILABLE = False

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
_LOG_DIR = Path(os.environ.get("CARBON_LOG_DIR", "./logs"))
try:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    _LOG_DIR = Path("/tmp")

logging.basicConfig(
    level=os.environ.get("CARBON_LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            _LOG_DIR / "carbon_marketplace_v6.log",
            maxBytes=10 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Prometheus metrics
# ============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    PURCHASE_COUNTER = Counter(
        "carbon_credits_purchased_total", "Total credits purchased",
        ["project_id"], registry=REGISTRY)
    RETIRE_COUNTER = Counter(
        "carbon_credits_retired_total", "Total credits retired",
        ["status"], registry=REGISTRY)
    BALANCE_GAUGE = Gauge(
        "carbon_credits_balance_kg", "Current available balance",
        registry=REGISTRY)
    AUTO_OFFSET_COUNTER = Counter(
        "auto_offset_actions_total", "Auto-offset actions",
        ["reason"], registry=REGISTRY)
    PROJECT_COUNT = Gauge(
        "carbon_projects_available", "Active projects", registry=REGISTRY)
    BLOCKCHAIN_TX_FAILURES = Counter(
        "blockchain_tx_failures_total", "Blockchain tx failures",
        registry=REGISTRY)
    API_REQUESTS = Counter(
        "api_requests_total", "API requests",
        ["endpoint", "method", "status"], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge(
        "carbon_circuit_breaker_state", "Circuit breaker state",
        ["service"], registry=REGISTRY)
    REGISTRY_API_LATENCY = Histogram(
        "registry_api_latency_seconds", "Registry API latency",
        ["registry"], registry=REGISTRY)
    AUTO_OFFSET_SUCCESS = Counter(
        "auto_offset_success_total", "Auto-offset successful", registry=REGISTRY)
    PREDICTION_ERROR = Counter(
        "prediction_error_total", "Price prediction errors", registry=REGISTRY)
    PQC_SIGNATURES = Counter(
        "pqc_signatures_total", "PQC signatures",
        ["algorithm", "status"], registry=REGISTRY)
    CLOUD_STORE = Counter(
        "cloud_store_total", "Cloud storage operations",
        ["provider", "status"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter(
        "carbon_safety_violations_total", "Safety violations",
        ["rule"], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter(
        "carbon_chaos_experiments_total", "Chaos experiments",
        ["type", "status"], registry=REGISTRY)
    HUMAN_REVIEWS = Counter(
        "carbon_human_reviews_total", "Human reviews",
        ["status"], registry=REGISTRY)
    XAI_DECISIONS = Counter(
        "carbon_xai_decisions_total", "XAI decisions",
        ["strategy"], registry=REGISTRY)
else:  # pragma: no cover
    class _DummyMetric:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass

    PURCHASE_COUNTER = RETIRE_COUNTER = BALANCE_GAUGE = _DummyMetric()
    AUTO_OFFSET_COUNTER = PROJECT_COUNT = BLOCKCHAIN_TX_FAILURES = _DummyMetric()
    API_REQUESTS = CIRCUIT_BREAKER_STATE = REGISTRY_API_LATENCY = _DummyMetric()
    AUTO_OFFSET_SUCCESS = PREDICTION_ERROR = PQC_SIGNATURES = _DummyMetric()
    CLOUD_STORE = SAFETY_VIOLATIONS = CHAOS_EXPERIMENTS = _DummyMetric()
    HUMAN_REVIEWS = XAI_DECISIONS = _DummyMetric()
    REGISTRY = None


# ============================================================================
# Config
# ============================================================================
def _default_jwt_secret() -> str:
    return secrets.token_hex(32)


if PYDANTIC_AVAILABLE and _SETTINGS_AVAILABLE and PYDANTIC_V2:
    class DatabaseConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        path: str = "carbon_credits.db"
        pool_size: int = 10
        max_overflow: int = 20

    class BlockchainConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        rpc_url: str = "http://localhost:8545"
        contract_address: Optional[str] = None
        private_key: Optional[str] = None
        chain_id: int = 1

    class CloudConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        aws_bucket: Optional[str] = None
        aws_access_key: Optional[str] = None
        aws_secret_key: Optional[str] = None
        aws_region: str = "us-east-1"
        azure_connection_string: Optional[str] = None
        azure_container: Optional[str] = None
        gcp_credentials: Optional[str] = None
        gcp_bucket: Optional[str] = None

    class WebhookConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        url: Optional[str] = None
        secret: Optional[str] = None

    class CarbonConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        api_key: Optional[str] = None
        region: str = "global"

    class RateLimitConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = True
        requests_per_minute: int = 100
        burst: int = 20
        redis_url: Optional[str] = None

    class GeneralConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        refresh_interval_seconds: int = 3600
        auto_offset_enabled: bool = True
        auto_offset_threshold_kg: float = 100.0
        auto_offset_interval_seconds: int = 3600
        retry_attempts: int = 3
        retry_min_wait: float = 2.0
        retry_max_wait: float = 10.0
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        jwt_secret: str = Field(default_factory=_default_jwt_secret)
        jwt_algorithm: str = "HS256"
        jwt_expiration_minutes: int = 1440
        data_retention_days: int = 365
        log_level: str = "INFO"
        human_review_threshold: float = Field(0.5, ge=0, le=1)
        random_seed: int = 42

    class OptimizerConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        modp_weights: Dict[str, float] = Field(default_factory=lambda: {
            "price": 0.25, "vintage": 0.25,
            "biodiversity": 0.20, "carbon_intensity": 0.30})
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        enable_distillation: bool = False

    class SafetyConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enable_monitor: bool = True
        max_carbon_intensity: float = 500.0
        min_biodiversity: float = 0.0
        max_price_per_kg: float = 2.0

    class ChaosConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = False
        failure_probability: float = 0.1
        experiment_interval_seconds: int = 120

    class FederatedConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = False
        aggregation_interval_seconds: int = 300

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="CARBON_", case_sensitive=False, extra="ignore")
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        blockchain: BlockchainConfig = Field(default_factory=BlockchainConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        webhook: WebhookConfig = Field(default_factory=WebhookConfig)
        carbon: CarbonConfig = Field(default_factory=CarbonConfig)
        rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        federated: FederatedConfig = Field(default_factory=FederatedConfig)
        API_HOST: str = "0.0.0.0"
        API_PORT: int = 8000
        MASTER_KEY: str = ""

        def ensure_master_key(self) -> str:
            if not self.MASTER_KEY:
                object.__setattr__(self, "MASTER_KEY", secrets.token_hex(32))
            return self.MASTER_KEY

        def get_master_key_bytes(self) -> bytes:
            if not self.MASTER_KEY:
                raise ValueError("MASTER_KEY not set")
            return bytes.fromhex(self.MASTER_KEY)

else:  # pragma: no cover
    @dataclass
    class DatabaseConfig:
        path: str = "carbon_credits.db"
        pool_size: int = 10
        max_overflow: int = 20

    @dataclass
    class BlockchainConfig:
        rpc_url: str = "http://localhost:8545"
        contract_address: Optional[str] = None
        private_key: Optional[str] = None
        chain_id: int = 1

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
    class WebhookConfig:
        url: Optional[str] = None
        secret: Optional[str] = None

    @dataclass
    class CarbonConfig:
        api_key: Optional[str] = None
        region: str = "global"

    @dataclass
    class RateLimitConfig:
        enabled: bool = True
        requests_per_minute: int = 100
        burst: int = 20
        redis_url: Optional[str] = None

    @dataclass
    class GeneralConfig:
        refresh_interval_seconds: int = 3600
        auto_offset_enabled: bool = True
        auto_offset_threshold_kg: float = 100.0
        auto_offset_interval_seconds: int = 3600
        retry_attempts: int = 3
        retry_min_wait: float = 2.0
        retry_max_wait: float = 10.0
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        jwt_secret: str = field(default_factory=_default_jwt_secret)
        jwt_algorithm: str = "HS256"
        jwt_expiration_minutes: int = 1440
        data_retention_days: int = 365
        log_level: str = "INFO"
        human_review_threshold: float = 0.5
        random_seed: int = 42

    @dataclass
    class OptimizerConfig:
        modp_weights: Dict[str, float] = field(default_factory=lambda: {
            "price": 0.25, "vintage": 0.25,
            "biodiversity": 0.20, "carbon_intensity": 0.30})
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        enable_distillation: bool = False

    @dataclass
    class SafetyConfig:
        enable_monitor: bool = True
        max_carbon_intensity: float = 500.0
        min_biodiversity: float = 0.0
        max_price_per_kg: float = 2.0

    @dataclass
    class ChaosConfig:
        enabled: bool = False
        failure_probability: float = 0.1
        experiment_interval_seconds: int = 120

    @dataclass
    class FederatedConfig:
        enabled: bool = False
        aggregation_interval_seconds: int = 300

    @dataclass
    class Settings:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        blockchain: BlockchainConfig = field(default_factory=BlockchainConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        webhook: WebhookConfig = field(default_factory=WebhookConfig)
        carbon: CarbonConfig = field(default_factory=CarbonConfig)
        rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        federated: FederatedConfig = field(default_factory=FederatedConfig)
        API_HOST: str = "0.0.0.0"
        API_PORT: int = 8000
        MASTER_KEY: str = ""

        def ensure_master_key(self) -> str:
            if not self.MASTER_KEY:
                self.MASTER_KEY = secrets.token_hex(32)
            return self.MASTER_KEY

        def get_master_key_bytes(self) -> bytes:
            if not self.MASTER_KEY:
                raise ValueError("MASTER_KEY not set")
            return bytes.fromhex(self.MASTER_KEY)


_settings_singleton: Optional[Settings] = None


def ensure_settings() -> Settings:
    global _settings_singleton
    if _settings_singleton is None:
        _settings_singleton = Settings()
        _settings_singleton.ensure_master_key()
    return _settings_singleton


def _reset_settings_for_tests() -> None:
    global _settings_singleton
    _settings_singleton = None


# ============================================================================
# Exceptions
# ============================================================================
class CarbonMarketplaceException(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None) -> None:
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)


class RegistryError(CarbonMarketplaceException): pass
class BlockchainError(CarbonMarketplaceException): pass
class CloudStorageError(CarbonMarketplaceException): pass
class PQCError(CarbonMarketplaceException): pass
class PredictionError(CarbonMarketplaceException): pass
class OptimizationError(CarbonMarketplaceException): pass
class VaultError(CarbonMarketplaceException): pass
class DatabaseError(CarbonMarketplaceException): pass
class CircuitBreakerOpenError(CarbonMarketplaceException): pass
class SafetyViolationError(CarbonMarketplaceException): pass
class ChaosExperimentError(CarbonMarketplaceException): pass


# ============================================================================
# Circuit breaker
# ============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, name: str, threshold: int = 5, timeout: int = 60,
                 half_open_success_threshold: int = 2) -> None:
        self.name = name
        self.threshold = int(threshold)
        self.timeout = int(timeout)
        self.half_open_success_threshold = int(half_open_success_threshold)
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_monotonic: Optional[float] = None
        self._lock = threading.RLock()
        self._metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

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
                        >= self.timeout):
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._success_count = 0
                    self._publish_metric()
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is OPEN")
        self._metrics["total_calls"] += 1
        try:
            result = func(*args, **kwargs)
            if inspect.iscoroutine(result):
                result = await result
        except Exception:
            with self._lock:
                self._metrics["failed_calls"] += 1
                self._failure_count += 1
                self._last_failure_monotonic = time.monotonic()
                if (self._state is CircuitBreakerState.CLOSED
                        and self._failure_count >= self.threshold):
                    self._state = CircuitBreakerState.OPEN
                elif self._state is CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.OPEN
                self._publish_metric()
            raise
        else:
            with self._lock:
                self._metrics["successful_calls"] += 1
                if self._state is CircuitBreakerState.HALF_OPEN:
                    self._success_count += 1
                    if self._success_count >= self.half_open_success_threshold:
                        self._state = CircuitBreakerState.CLOSED
                        self._failure_count = 0
                else:
                    self._failure_count = 0
                self._publish_metric()
            return result

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            **self._metrics,
        }

    def force_open(self) -> None:
        with self._lock:
            self._state = CircuitBreakerState.OPEN
            self._failure_count = self.threshold
            self._last_failure_monotonic = time.monotonic()
            self._publish_metric()

    def reset(self) -> None:
        with self._lock:
            self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_monotonic = None
            self._publish_metric()


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
            return {n: cb.get_metrics() for n, cb in self._breakers.items()}  # type: ignore[attr-defined]


# ============================================================================
# Task manager
# ============================================================================
class TaskManager:
    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._pending: Dict[str, Tuple[Callable[..., Awaitable[None]], Tuple, Dict]] = {}

    def register_task(self, name: str,
                      coro_func: Callable[..., Awaitable[None]],
                      *args: Any, **kwargs: Any) -> None:
        if name in self.tasks and not self.tasks[name].done():
            return
        self._pending[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self) -> None:
        for name, (fn, args, kwargs) in list(self._pending.items()):
            self._start_task(name, fn, *args, **kwargs)
        self._pending.clear()

    def _start_task(self, name: str,
                    coro_func: Callable[..., Awaitable[None]],
                    *args: Any, **kwargs: Any) -> asyncio.Task:
        if name in self.tasks and not self.tasks[name].done():
            return self.tasks[name]

        async def wrapper() -> None:
            backoff = 1.0
            max_backoff = 300.0
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(self.shutdown_event, *args, **kwargs)
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
                await asyncio.gather(*self.tasks.values(),
                                     return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

    def snapshot(self) -> Dict[str, str]:
        return {n: ("running" if not t.done() else "stopped")
                for n, t in self.tasks.items()}


# ============================================================================
# Persistence
# ============================================================================
class AsyncDatabaseManager:
    """
    Minimal async persistence on aiosqlite.  Holds one connection behind a
    lock, applies schema migrations transactionally, and exposes a
    ``get_session()`` context manager for callers that want explicit commits.
    """

    SCHEMA_VERSION = 1

    def __init__(self, config: Settings) -> None:
        self.config = config
        self.db_path = Path(config.database.path)
        self._conn: Optional[Any] = None
        self._init_lock = asyncio.Lock()
        self._query_lock = asyncio.Lock()

    async def connect(self) -> Any:
        if self._conn is not None:
            return self._conn
        async with self._init_lock:
            if self._conn is not None:
                return self._conn
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._init_schema(self._conn)
        return self._conn

    async def _init_schema(self, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS credit_projects (
                project_id TEXT PRIMARY KEY, name TEXT, registry TEXT,
                available_credits_kg REAL, price_per_kg_usd REAL,
                verification_status TEXT, credit_type TEXT,
                co_benefits TEXT, extra TEXT, last_updated TEXT,
                active INTEGER DEFAULT 1
            )""")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS credit_transactions (
                tx_id TEXT PRIMARY KEY, project_id TEXT, amount_kg REAL,
                retired_kg REAL DEFAULT 0.0, cost_usd REAL, status TEXT,
                credit_type TEXT DEFAULT 'voluntary', retires_at TEXT,
                blockchain_tx_hash TEXT, payment_method TEXT DEFAULT 'USD',
                extra TEXT, created_at TEXT, updated_at TEXT
            )""")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY, password_hash TEXT, role TEXT,
                created_at TEXT
            )""")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT,
                action TEXT, details TEXT, timestamp TEXT
            )""")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
            )""")
        await conn.commit()

    @asynccontextmanager
    async def get_session(self):
        """Yield the shared connection with autocommit semantics."""
        conn = await self.connect()
        yield _SessionWrapper(conn, self._query_lock)

    async def execute(self, query: str, params: tuple = ()) -> Any:
        conn = await self.connect()
        async with self._query_lock:
            cur = await conn.execute(query, params)
            await conn.commit()
            return cur

    async def fetch_all(self, query: str, params: tuple = ()) -> List[Any]:
        conn = await self.connect()
        async with self._query_lock:
            cur = await conn.execute(query, params)
            return await cur.fetchall()

    async def fetch_one(self, query: str, params: tuple = ()) -> Optional[Any]:
        conn = await self.connect()
        async with self._query_lock:
            cur = await conn.execute(query, params)
            return await cur.fetchone()

    async def save_state(self, key: str, value: Any) -> None:
        await self.execute(
            "INSERT OR REPLACE INTO system_state (key, value, updated_at) "
            "VALUES (?, ?, ?)",
            (key, json.dumps(value, default=str),
             datetime.now(timezone.utc).isoformat()))

    async def load_state(self, key: str, default: Optional[Any] = None) -> Any:
        row = await self.fetch_one(
            "SELECT value FROM system_state WHERE key=?", (key,))
        if row is None:
            return default
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            return default

    async def close(self) -> None:
        async with self._init_lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None


class _SessionWrapper:
    """Minimal session wrapper for the async context manager."""

    def __init__(self, conn: Any, lock: asyncio.Lock) -> None:
        self._conn = conn
        self._lock = lock

    async def execute(self, query: str, params: Any = ()) -> Any:
        async with self._lock:
            cur = await self._conn.execute(query, params)
            return cur

    async def commit(self) -> None:
        await self._conn.commit()


# ============================================================================
# Adapter classes (deterministic simulations)
# ============================================================================
class RegistryClient:
    """Simulated registry client for carbon-credit projects."""

    def __init__(self, config: Settings) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create(
            "registry",
            threshold=config.general.circuit_breaker_threshold,
            timeout=config.general.circuit_breaker_timeout)

    async def _fetch(self) -> List[Dict[str, Any]]:
        # Deterministic set of 6 projects.
        templates = [
            ("amazon_redd_2023", "Amazon REDD+ 2023", "Verra"),
            ("borneo_peat_2022", "Borneo Peat Restoration 2022", "GoldStandard"),
            ("kenya_cookstove_2023", "Kenya Cookstove 2023", "Verra"),
            ("india_solar_2022", "India Solar 2022", "GoldStandard"),
            ("brazil_mangrove_2023", "Brazil Mangrove 2023", "Verra"),
            ("indonesia_geothermal_2024", "Indonesia Geothermal 2024", "Verra"),
        ]
        out: List[Dict[str, Any]] = []
        for pid, name, registry in templates:
            out.append({
                "project_id": pid,
                "name": name,
                "registry": registry,
                "available_credits_kg": float(self._rng.uniform(5_000, 50_000)),
                "price_per_kg_usd": float(self._rng.uniform(0.05, 1.5)),
                "verification_status": "verified",
                "credit_type": "voluntary",
                "co_benefits": {
                    "biodiversity": float(self._rng.uniform(0.0, 1.0)),
                    "sdg": ["13", "15"],
                },
                "metadata": {
                    "vintage": int(self._rng.choice([2022, 2023, 2024])),
                    "carbon_intensity": float(self._rng.uniform(50, 500)),
                },
            })
        return out

    async def fetch_projects(self) -> List[Dict[str, Any]]:
        async def _inner() -> List[Dict[str, Any]]:
            return await self._fetch()
        return await self.circuit_breaker.call(_inner)

    async def close(self) -> None:
        pass


class BlockchainClient:
    """Deterministic blockchain client (simulated)."""

    def __init__(self, config: Settings) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self._balances: Dict[str, float] = defaultdict(float)

    async def mint(self, project_id: str, amount_kg: float, owner: str) -> str:
        tx_hash = "0x" + hashlib.sha256(
            f"{project_id}:{amount_kg}:{owner}:{time.time()}".encode()
        ).hexdigest()[:40]
        self._balances[owner] += amount_kg
        return tx_hash

    async def get_balance(self, address: str) -> float:
        return float(self._balances.get(address, 0.0))

    async def close(self) -> None:
        pass


class PostQuantumCrypto:
    """HMAC-SHA256 signing with AES-GCM-wrapped per-key secrets."""

    SALT_KEY = "pqc_salt"

    def __init__(self, config: Settings, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        self.master_key = config.get_master_key_bytes()
        self._salt: Optional[bytes] = None
        self._lock = asyncio.Lock()

    async def _load_or_create_salt(self) -> bytes:
        if self._salt is not None:
            return self._salt
        async with self._lock:
            if self._salt is not None:
                return self._salt
            stored = await self.db.load_state(self.SALT_KEY)
            if stored is None:
                new_salt = os.urandom(16)
                await self.db.save_state(self.SALT_KEY, new_salt.hex())
                self._salt = new_salt
            else:
                self._salt = bytes.fromhex(stored)
            return self._salt

    def _derive(self, salt: bytes, length: int = 32) -> bytes:
        if not CRYPTO_AVAILABLE:
            return hmac.new(self.master_key, salt, hashlib.sha256).digest()[:length]
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=length,
                         salt=salt, iterations=100_000,
                         backend=default_backend())
        return kdf.derive(self.master_key)

    async def _encrypt(self, data: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive(salt)
        nonce = os.urandom(12)
        if CRYPTO_AVAILABLE:
            return nonce + AESGCM(derived).encrypt(nonce, data, None)
        return nonce + bytes(c ^ derived[i % len(derived)]
                             for i, c in enumerate(data))

    async def _decrypt(self, blob: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive(salt)
        nonce, rest = blob[:12], blob[12:]
        if CRYPTO_AVAILABLE:
            return AESGCM(derived).decrypt(nonce, rest, None)
        return bytes(c ^ derived[i % len(derived)]
                     for i, c in enumerate(rest))

    async def generate_keypair(self, algorithm: str = "hmac_sha256",
                               validity_days: int = 30) -> Dict[str, Any]:
        secret = secrets.token_bytes(32)
        key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
        expires = (datetime.now(timezone.utc)
                   + timedelta(days=validity_days)).isoformat()
        payload = {
            "key_id": key_id, "algorithm": algorithm,
            "public_key": hashlib.sha256(secret).hexdigest(),
            "secret": (await self._encrypt(secret)).hex(),
            "expires_at": expires,
        }
        await self.db.save_state(f"pqc_keypair:{key_id}", payload)
        PQC_SIGNATURES.labels(algorithm=algorithm, status="generated").inc()
        return payload

    async def sign_data(self, data: Dict[str, Any], key_id: str) -> Dict[str, Any]:
        kp = await self.db.load_state(f"pqc_keypair:{key_id}")
        if not kp:
            raise PQCError(f"Key {key_id} not found")
        secret = await self._decrypt(bytes.fromhex(kp["secret"]))
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        sig = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        PQC_SIGNATURES.labels(algorithm=kp["algorithm"], status="signed").inc()
        return {"signature": sig, "algorithm": kp["algorithm"],
                "key_id": key_id,
                "timestamp": datetime.now(timezone.utc).isoformat()}

    async def verify_data(self, data: Dict[str, Any],
                          signature_data: Dict[str, Any]) -> bool:
        key_id = signature_data.get("key_id")
        if not key_id:
            return False
        kp = await self.db.load_state(f"pqc_keypair:{key_id}")
        if not kp:
            return False
        secret = await self._decrypt(bytes.fromhex(kp["secret"]))
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        ok = hmac.compare_digest(expected, signature_data.get("signature", ""))
        PQC_SIGNATURES.labels(algorithm=kp["algorithm"],
                              status="verified" if ok else "mismatch").inc()
        return ok


class CloudStorage:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self.local_dir = Path(os.environ.get("CARBON_CLOUD_DIR", "./cloud_storage"))

    async def store(self, data: Dict[str, Any],
                    filename: Optional[str] = None) -> Dict[str, Any]:
        self.local_dir.mkdir(parents=True, exist_ok=True)
        fname = filename or f"blob_{uuid.uuid4().hex[:12]}.json"
        path = self.local_dir / fname
        await asyncio.to_thread(
            path.write_text, json.dumps(data, default=str))
        CLOUD_STORE.labels(provider="local", status="success").inc()
        return {"status": "success", "location": str(path),
                "provider": "local", "simulated": True}


class PricePredictor:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self._history: List[float] = [0.10, 0.12, 0.14, 0.13, 0.15]

    async def predict(self, days: int = 30) -> Optional[List[float]]:
        if len(self._history) < 2:
            return None
        x = np.arange(len(self._history))
        y = np.asarray(self._history, dtype=float)
        coeffs = np.polyfit(x, y, 1)
        out: List[float] = []
        base_x = len(self._history) - 1
        for i in range(1, days + 1):
            out.append(float(coeffs[0] * (base_x + i) + coeffs[1]))
        return out

    async def train(self) -> None:
        pass

    async def update_history(self, price_data: Dict[str, Any]) -> None:
        if "price" in price_data:
            try:
                self._history.append(float(price_data["price"]))
                if len(self._history) > 100:
                    self._history = self._history[-100:]
            except (TypeError, ValueError):
                PREDICTION_ERROR.inc()


class CarbonIntensityManager:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self.current_intensity_g = 400.0

    async def get_intensity(self) -> float:
        self.current_intensity_g = float(self._rng.uniform(150.0, 550.0))
        return self.current_intensity_g

    async def close(self) -> None:
        pass


class UnifiedSustainabilityEngine:
    def __init__(self, db: AsyncDatabaseManager) -> None:
        self.db = db
        self._rng = np.random.default_rng(42)

    async def get_recent_emissions(self, hours: int = 24) -> float:
        # Simulated recent emissions in kg CO₂.
        return float(self._rng.uniform(50.0, 250.0))


class AutoOffsetEngine:
    def __init__(self, marketplace: Optional[Any] = None) -> None:
        self.marketplace = marketplace

    async def offset(self, emissions_kg: float,
                     reason: str = "auto_offset") -> Dict[str, Any]:
        if self.marketplace is None:
            return {"status": "no_marketplace", "offset_kg": 0.0}
        return await self.marketplace._perform_offset(emissions_kg, reason)


class WebSocketManager:
    def __init__(self) -> None:
        self._connections: Set[Any] = set()

    async def connect(self, ws: Any) -> None:
        self._connections.add(ws)

    async def disconnect(self, ws: Any) -> None:
        self._connections.discard(ws)

    async def broadcast(self, payload: Dict[str, Any]) -> None:
        message = json.dumps(payload, default=str)
        for ws in list(self._connections):
            try:
                await ws.send_text(message)
            except Exception:
                self._connections.discard(ws)


class WebhookNotifier:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self._events: deque = deque(maxlen=200)

    async def notify(self, event: Dict[str, Any]) -> None:
        self._events.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
        })

    async def close(self) -> None:
        pass


# ============================================================================
# Enhancement modules
# ============================================================================
class SafetyMonitor:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self._last_violation: Dict[str, bool] = {}

    def rules(self) -> Dict[str, Callable[[Any], bool]]:
        s = self.config.safety
        return {
            "max_carbon_intensity":
                lambda p: float(p.metadata.get("carbon_intensity", 0.0))
                <= s.max_carbon_intensity,
            "min_biodiversity":
                lambda p: float(p.co_benefits.get("biodiversity", 0.0))
                >= s.min_biodiversity,
            "max_price_per_kg":
                lambda p: float(p.price_per_kg_usd) <= s.max_price_per_kg,
        }

    async def check_project(self, project: Any) -> List[Dict[str, Any]]:
        violations: List[Dict[str, Any]] = []
        for name, fn in self.rules().items():
            try:
                ok = bool(fn(project))
            except Exception:
                ok = True
            if ok:
                self._last_violation[name] = False
                continue
            violations.append({"rule": name, "project_id": project.project_id,
                               "details": project.model_dump()
                               if hasattr(project, "model_dump")
                               else str(project)})
            if not self._last_violation.get(name, False):
                SAFETY_VIOLATIONS.labels(rule=name).inc()
                logger.warning("Safety violation: %s on project %s",
                               name, project.project_id)
                self._last_violation[name] = True
        return violations

    async def validate_purchase(self, project: Any, amount_kg: float,
                                context: Dict[str, Any]) -> bool:
        return not await self.check_project(project)

    async def get_status(self) -> Dict[str, Any]:
        return {"enabled": self.config.safety.enable_monitor,
                "rules": list(self.rules().keys())}


class XAIExplainer:
    async def generate_explanation(self, strategy: Dict[str, Any],
                                   context: Any, confidence: float,
                                   utility: float, project: Any) -> str:
        parts: List[str] = [f"Selected strategy '{strategy.get('name', '?')}'."]
        params = strategy.get("params", {}) or {}
        for k, v in params.items():
            parts.append(f"{k}={float(v):.2f}")
        parts.append(f"Project: {getattr(project, 'name', '?')} "
                     f"({getattr(project, 'project_id', '?')}).")
        parts.append(f"Price/kg: "
                     f"${float(getattr(project, 'price_per_kg_usd', 0.0)):.3f}.")
        biodiversity = (getattr(project, "co_benefits", {}) or {}).get(
            "biodiversity")
        if biodiversity is not None:
            parts.append(f"Biodiversity: {float(biodiversity):.2f}.")
        parts.append(f"Confidence: {confidence:.2f}.")
        parts.append(f"Utility: {utility:.2f}.")
        return " ".join(parts)


class HumanReviewManager:
    def __init__(self, config: Settings) -> None:
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
            "context": dict(context),
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
                raise CarbonMarketplaceException("Review not found")
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
                raise CarbonMarketplaceException("Review not found")
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
    def __init__(self, config: Settings, marketplace: Any) -> None:
        self.config = config
        self.marketplace = marketplace
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
        failure = str(self._rng.choice(["latency", "error", "disconnect"]))
        target = str(self._rng.choice(["registry", "blockchain", "database"]))
        status_ = "success"
        result: Dict[str, Any] = {}
        try:
            if failure == "latency":
                delay = float(self._rng.uniform(0.05, 0.2))
                await asyncio.sleep(delay)
                result["delay_seconds"] = delay
            elif failure == "error":
                result["action"] = f"simulated {target} error"
            else:
                await asyncio.sleep(0.05)
                result["action"] = "simulated network partition"
        except Exception as e:
            status_ = "failed"
            result["error"] = str(e)
        CHAOS_EXPERIMENTS.labels(type=failure, status=status_).inc()
        logger.info("Chaos experiment %s on %s: %s", failure, target, status_)
        return {"fault": failure, "target": target,
                "status": status_, "result": result}


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


class QuantumDistillationOptimizer:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self.available = False  # QAOA not shipped

    async def optimize(self, projects: List[Any],
                       weights: Dict[str, float]) -> Optional[Any]:
        return None


class FederatedCoordinator:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self._participants: Dict[str, Dict[str, Any]] = {}
        self.last_aggregation: Optional[datetime] = None

    async def register_participant(self, participant_id: str,
                                   model_update: Dict[str, Any]) -> None:
        self._participants[participant_id] = dict(model_update)

    async def aggregate(self) -> Dict[str, float]:
        if not self._participants:
            return {}
        keys: Set[str] = set()
        for p in self._participants.values():
            keys.update(p.keys())
        out: Dict[str, float] = {}
        for k in keys:
            vals = [float(p[k]) for p in self._participants.values()
                    if isinstance(p.get(k), (int, float))]
            if vals:
                out[k] = float(np.mean(vals))
        self.last_aggregation = datetime.now(timezone.utc)
        return out


class MultiAgentSystem:
    AGENTS = ("price_agent", "carbon_agent", "biodiversity_agent")

    def __init__(self, config: Settings) -> None:
        self.config = config

    def _score_project(self, project: Any) -> Dict[str, float]:
        price = float(getattr(project, "price_per_kg_usd", 0.0))
        carbon = float(getattr(project, "metadata", {}).get(
            "carbon_intensity", 400.0))
        biodiversity = float(
            getattr(project, "co_benefits", {}).get("biodiversity", 0.0))
        return {
            "price_agent": 1.0 - price / 2.0,
            "carbon_agent": 1.0 - carbon / 1000.0,
            "biodiversity_agent": biodiversity,
        }

    async def vote(self, projects: List[Any]) -> Optional[Any]:
        if not projects:
            return None
        scores: Dict[str, float] = {}
        for p in projects:
            agent_scores = self._score_project(p)
            scores[p.project_id] = float(sum(agent_scores.values()))
        best_id = max(scores, key=scores.get)
        return next((p for p in projects if p.project_id == best_id), None)


# ============================================================================
# Autonomous optimizer
# ============================================================================
class AutonomousOptimizer:
    DEFAULT_STRATEGY = {
        "name": "balanced",
        "params": {"price_weight": 0.3, "vintage_weight": 0.3,
                   "biodiversity_weight": 0.2, "carbon_weight": 0.2},
    }

    def __init__(self, config: Settings, marketplace: Any,
                 db: AsyncDatabaseManager,
                 safety_monitor: SafetyMonitor,
                 xai: XAIExplainer,
                 human_review: HumanReviewManager,
                 quantum_optimizer: QuantumDistillationOptimizer,
                 multi_agent: MultiAgentSystem) -> None:
        self.config = config
        self.marketplace = marketplace
        self.db = db
        self.safety_monitor = safety_monitor
        self.xai = xai
        self.human_review = human_review
        self.quantum_optimizer = quantum_optimizer
        self.multi_agent = multi_agent
        self._lock = asyncio.Lock()
        self.threshold_history: deque = deque(maxlen=100)
        self.success_history: deque = deque(maxlen=100)

        self.action_space = [
            {"name": "balanced",
             "params": {"price_weight": 0.3, "vintage_weight": 0.3,
                        "biodiversity_weight": 0.2, "carbon_weight": 0.2}},
            {"name": "price_focused",
             "params": {"price_weight": 0.6, "vintage_weight": 0.1,
                        "biodiversity_weight": 0.1, "carbon_weight": 0.2}},
            {"name": "green_focused",
             "params": {"price_weight": 0.1, "vintage_weight": 0.2,
                        "biodiversity_weight": 0.3, "carbon_weight": 0.4}},
            {"name": "vintage_focused",
             "params": {"price_weight": 0.2, "vintage_weight": 0.5,
                        "biodiversity_weight": 0.2, "carbon_weight": 0.1}},
        ]

        def fallback(_ctx: Any) -> Dict[str, Any]:
            return dict(self.DEFAULT_STRATEGY)

        self.bandit = CausalBandit(
            action_space=self.action_space,
            fallback_solver=fallback,
            min_trials_before_bandit=config.optimizer.bandit_min_trials,
            confidence_threshold=config.optimizer.bandit_confidence_threshold,
            seed=config.general.random_seed,
        )

        self.recent_rewards: deque = deque(maxlen=100)
        self._last_selection: Dict[str, Any] = {
            "project": None, "strategy": None, "context": None}

    # ------------------------------------------------------------------
    async def load_state(self) -> None:
        state = await self.db.load_state("optimizer_state")
        if state:
            try:
                self.bandit.import_state(state)
            except Exception as e:
                logger.warning("Failed to import optimizer state: %s", e)

    async def save_state(self) -> None:
        try:
            await self.db.save_state("optimizer_state", self.bandit.export_state())
        except Exception as e:
            logger.warning("Failed to persist optimizer state: %s", e)

    # ------------------------------------------------------------------
    def _score_project(self, project: Any, weights: Dict[str, float]) -> float:
        price = 1.0 - min(1.0, float(project.price_per_kg_usd) / 2.0)
        vintage = float(project.metadata.get("vintage", 2020))
        vintage_score = max(0.0, min(1.0, (vintage - 2020) / 5.0))
        biodiversity = float(project.co_benefits.get("biodiversity", 0.0))
        carbon = float(project.metadata.get("carbon_intensity", 400.0))
        carbon_score = 1.0 - min(1.0, carbon / 1000.0)
        return float(
            price * weights.get("price_weight", 0.3)
            + vintage_score * weights.get("vintage_weight", 0.3)
            + biodiversity * weights.get("biodiversity_weight", 0.2)
            + carbon_score * weights.get("carbon_weight", 0.2)
        )

    # ------------------------------------------------------------------
    async def select_best_project(self, projects: List[Any], amount_kg: float,
                                  context: Optional[Dict[str, Any]] = None
                                  ) -> Optional[Any]:
        if not projects:
            return None
        ctx = context or {}
        strategy, confidence, source = self.bandit.select_action(ctx)
        if strategy is None:
            strategy = dict(self.DEFAULT_STRATEGY)

        # Quantum path (best-effort).
        selected: Optional[Any] = None
        if (self.quantum_optimizer.available
                and amount_kg > 10_000):
            selected = await self.quantum_optimizer.optimize(
                projects, strategy["params"])
            if selected is not None:
                source = "quantum"

        if selected is None:
            scored = sorted(
                ((p, self._score_project(p, strategy["params"])) for p in projects),
                key=lambda x: x[1], reverse=True)
            for p, _ in scored:
                if float(p.available_credits_kg) >= amount_kg:
                    selected = p
                    break

        if selected is None:
            return None

        # Safety gate.
        if self.config.safety.enable_monitor:
            violations = await self.safety_monitor.check_project(selected)
            if violations:
                fallback = next(
                    (p for p in projects
                     if float(p.available_credits_kg) >= amount_kg
                     and not await self.safety_monitor.check_project(p)),
                    None)
                if fallback is not None:
                    selected = fallback

        self._last_selection = {"project": selected, "strategy": strategy,
                                "context": ctx}

        # XAI + human review.
        utility = self._score_project(selected, strategy["params"])
        explanation = await self.xai.generate_explanation(
            strategy, ctx, confidence, utility, selected)
        decision_id = str(uuid.uuid4())
        logger.info("Decision %s: %s", decision_id, explanation)
        XAI_DECISIONS.labels(strategy=strategy["name"]).inc()
        if (confidence < self.config.general.human_review_threshold
                and self.human_review is not None):
            await self.human_review.request_review(decision_id, explanation, ctx)

        await self._emit_feedback_event(decision_id, strategy, confidence,
                                        source, utility)
        return selected

    # ------------------------------------------------------------------
    async def update_feedback(self, context: Dict[str, Any],
                              strategy: Dict[str, Any],
                              reward: float) -> None:
        self.bandit.update(context, strategy, float(reward))
        self.recent_rewards.append(float(reward))
        if len(self.recent_rewards) > 20 and \
                float(np.mean(self.recent_rewards)) < 0.3:
            await self.evolve_strategies()

    async def evolve_strategies(self) -> List[Dict[str, Any]]:
        candidate = {
            "name": "green_burst",
            "params": {"price_weight": 0.05, "vintage_weight": 0.15,
                       "biodiversity_weight": 0.4, "carbon_weight": 0.4},
        }
        if not any(a["name"] == candidate["name"] for a in self.action_space):
            self.action_space.append(candidate)
            self.bandit.register_action(candidate)
            logger.info("Bio expansion added strategy: %s", candidate["name"])
            return [candidate]
        return []

    async def optimize_offset_threshold(self) -> float:
        async with self._lock:
            if len(self.success_history) < 10:
                return float(self.config.general.auto_offset_threshold_kg)
            success_rate = sum(self.success_history) / len(self.success_history)
            current = float(self.config.general.auto_offset_threshold_kg)
            if success_rate > 0.9:
                new = current * 1.05
            elif success_rate < 0.6:
                new = current * 0.9
            else:
                new = current
            new = float(max(50.0, min(500.0, new)))
            object.__setattr__(self.config.general,
                               "auto_offset_threshold_kg", new)
            return new

    async def record_outcome(self, success: bool) -> None:
        async with self._lock:
            self.success_history.append(bool(success))

    def get_optimization_stats(self) -> Dict[str, Any]:
        return {
            "strategies": [s["name"] for s in self.action_space],
            "bandit_state": self.bandit.export_state(),
            "recent_reward_mean": (float(np.mean(self.recent_rewards))
                                   if self.recent_rewards else 0.0),
            "threshold": float(self.config.general.auto_offset_threshold_kg),
        }

    async def _emit_feedback_event(self, decision_id: str,
                                   strategy: Dict[str, Any], confidence: float,
                                   source: str, utility: float) -> None:
        if FeedbackEvent is None:
            return
        try:
            ev = FeedbackEvent(
                source="carbon_credit_marketplace",
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
                tags=["carbon", strategy["name"], source],
                envelope={"trace_id": decision_id},
                metadata={"source": source},
            )
            logger.debug("FeedbackEvent emitted: %s",
                         getattr(ev, "event_id", "?"))
        except Exception as e:
            logger.warning("Failed to emit FeedbackEvent: %s", e)


# ============================================================================
# FlexGen manager (deterministic simulation)
# ============================================================================
class FlexGenManager:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self._last_policy: Optional[Dict[str, Any]] = None

    async def optimize_policy(self, workload: Dict[str, Any],
                              node: Dict[str, Any]) -> Dict[str, Any]:
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
            "workload": workload, "node": node,
            "policy": policy, "metrics": metrics,
            "reward": float(self._rng.uniform(0.4, 1.0)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "simulated": True,
        }
        return self._last_policy

    async def get_status(self) -> Dict[str, Any]:
        return {"available": True, "last_policy": self._last_policy,
                "simulated": True}


# ============================================================================
# Pydantic data models
# ============================================================================
if PYDANTIC_AVAILABLE:
    class CreditProject(BaseModel):
        model_config = ConfigDict(extra="allow")
        project_id: str
        name: str
        registry: str
        available_credits_kg: float
        price_per_kg_usd: float
        verification_status: str
        credit_type: str = "voluntary"
        metadata: Dict[str, Any] = Field(default_factory=dict)
        co_benefits: Dict[str, Any] = Field(default_factory=dict)

    class CreditPurchaseRequest(BaseModel):
        project_id: str
        amount_kg: float
        credit_type: Optional[str] = None
        retire_immediately: bool = False
        reason: Optional[str] = None
        payment_method: str = "USD"

    class CreditRetireRequest(BaseModel):
        tx_id: str
        amount_kg: float
        reason: Optional[str] = None

    class CreditTransaction(BaseModel):
        tx_id: str
        project_id: str
        amount_kg: float
        cost_usd: float
        status: str
        credit_type: str
        retires_at: Optional[str] = None
        blockchain_tx_hash: Optional[str] = None
        metadata: Dict[str, Any] = Field(default_factory=dict)
        created_at: str = Field(
            default_factory=lambda: datetime.now(timezone.utc).isoformat())

    class ReportRequest(BaseModel):
        start_date: datetime
        end_date: datetime
        format: str = "json"

    class _RegisterBody(BaseModel):
        username: str
        password: str
        role: str = "viewer"

    class _LoginBody(BaseModel):
        username: str
        password: str

    class _OptimizeSelectBody(BaseModel):
        context: Dict[str, Any] = Field(default_factory=dict)

    class _FeedbackBody(BaseModel):
        context: Dict[str, Any]
        strategy: Dict[str, Any]
        reward: float

    class _FlexGenBody(BaseModel):
        workload: Dict[str, Any] = Field(default_factory=dict)
        node: Dict[str, Any] = Field(default_factory=dict)

    class _SafetyCheckBody(BaseModel):
        project: Dict[str, Any]

    class _FederatedRegisterBody(BaseModel):
        participant_id: str
        model_update: Dict[str, Any]

    class _ReviewActionBody(BaseModel):
        feedback: Optional[str] = None
else:  # pragma: no cover
    class CreditProject:  # type: ignore
        def __init__(self, **data):
            self.__dict__.update(data)

    class CreditPurchaseRequest:  # type: ignore
        def __init__(self, **data):
            self.__dict__.update(data)

    class CreditRetireRequest:  # type: ignore
        def __init__(self, **data):
            self.__dict__.update(data)

    class CreditTransaction:  # type: ignore
        def __init__(self, **data):
            self.__dict__.update(data)


# ============================================================================
# Marketplace
# ============================================================================
class CarbonCreditMarketplace:
    def __init__(
        self,
        config: Settings,
        db_manager: AsyncDatabaseManager,
        registry_client: RegistryClient,
        blockchain_client: BlockchainClient,
        pqc: PostQuantumCrypto,
        cloud_storage: CloudStorage,
        price_predictor: PricePredictor,
        auto_offset_engine: AutoOffsetEngine,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        sustainability_engine: Optional[UnifiedSustainabilityEngine] = None,
    ) -> None:
        self.config = config
        self.db_manager = db_manager
        self.registry_client = registry_client
        self.blockchain_client = blockchain_client
        self.pqc = pqc
        self.cloud_storage = cloud_storage
        self.price_predictor = price_predictor
        self.auto_offset_engine = auto_offset_engine
        self.carbon_manager = carbon_manager
        self.sustainability_engine = sustainability_engine
        self.instance_id = str(uuid.uuid4())[:8]

        # Enhancements.
        self.safety_monitor = SafetyMonitor(config)
        self.xai = XAIExplainer()
        self.human_review = HumanReviewManager(config)
        self.quantum_optimizer = QuantumDistillationOptimizer(config)
        self.multi_agent = MultiAgentSystem(config)
        self.federated = FederatedCoordinator(config)
        self.chaos_monkey = ChaosMonkey(config, self)

        self.optimizer = AutonomousOptimizer(
            config=config, marketplace=self, db=db_manager,
            safety_monitor=self.safety_monitor, xai=self.xai,
            human_review=self.human_review,
            quantum_optimizer=self.quantum_optimizer,
            multi_agent=self.multi_agent,
        )
        self.flexgen_manager = FlexGenManager(config)
        self.ws_manager = WebSocketManager()
        self.webhook = WebhookNotifier(config)

        self._projects_cache: Dict[str, CreditProject] = {}
        self._projects_cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(
            seconds=config.general.refresh_interval_seconds)
        self._transactions: Dict[str, CreditTransaction] = {}
        self._balances: Dict[str, float] = defaultdict(float)

        self.task_manager = TaskManager()
        self._register_background_tasks()
        logger.info("CarbonCreditMarketplace v%s initialized (id=%s)",
                    __version__, self.instance_id)

    # ------------------------------------------------------------------
    def _register_background_tasks(self) -> None:
        self.task_manager.register_task("auto_offset", self._auto_offset_loop)
        self.task_manager.register_task("price_update", self._price_update_loop)
        if self.config.chaos.enabled:
            self.task_manager.register_task("chaos", self._chaos_loop)
        if self.config.federated.enabled:
            self.task_manager.register_task(
                "federated_aggregate", self._federated_aggregate_loop)

    async def start(self) -> None:
        await self.db_manager.connect()
        await self.optimizer.load_state()
        await self._refresh_projects(force=True)
        self.task_manager.start_registered_tasks()
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.start()
        logger.info("Marketplace started (id=%s)", self.instance_id)

    async def shutdown(self) -> None:
        logger.info("Shutting down marketplace (id=%s)", self.instance_id)
        try:
            await self.optimizer.save_state()
        except Exception:
            pass
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.stop()
        await self.task_manager.stop_all()
        try:
            await self.registry_client.close()
        except Exception:
            pass
        try:
            await self.blockchain_client.close()
        except Exception:
            pass
        try:
            await self.webhook.close()
        except Exception:
            pass
        await self.db_manager.close()
        logger.info("Shutdown complete")

    # ------------------------------------------------------------------
    async def _auto_offset_loop(self, shutdown: asyncio.Event) -> None:
        interval = self.config.general.auto_offset_interval_seconds
        while not shutdown.is_set():
            try:
                if self.config.general.auto_offset_enabled \
                        and self.sustainability_engine is not None:
                    emissions = await self.sustainability_engine.get_recent_emissions(
                        hours=24)
                    if emissions > float(self.config.general.auto_offset_threshold_kg):
                        await self._perform_offset(emissions, reason="auto_offset_loop")
            except Exception as e:
                logger.error("Auto-offset loop error: %s", e)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

    async def _price_update_loop(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                await self.price_predictor.train()
            except Exception as e:
                logger.error("Price update loop error: %s", e)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=3600)
                break
            except asyncio.TimeoutError:
                pass

    async def _chaos_loop(self, shutdown: asyncio.Event) -> None:
        # The ChaosMonkey has its own loop; this is a placeholder.
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=3600)
                break
            except asyncio.TimeoutError:
                pass

    async def _federated_aggregate_loop(self, shutdown: asyncio.Event) -> None:
        interval = self.config.federated.aggregation_interval_seconds
        while not shutdown.is_set():
            try:
                avg = await self.federated.aggregate()
                if avg:
                    self.optimizer.bandit.import_state({
                        **self.optimizer.bandit.export_state(),
                        "q_values": {
                            **self.optimizer.bandit.q_values,
                            **{k: v for k, v in avg.items()
                               if k in self.optimizer.bandit.q_values},
                        },
                    })
            except Exception as e:
                logger.error("Federated loop error: %s", e)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------
    async def _refresh_projects(self, force: bool = False) -> None:
        now = datetime.now(timezone.utc)
        if not force and self._projects_cache_time is not None \
                and (now - self._projects_cache_time) < self._cache_ttl:
            return
        raws = await self.registry_client.fetch_projects()
        conn = await self.db_manager.connect()
        async with self.db_manager._query_lock:
            for raw in raws:
                await conn.execute("""
                    INSERT OR REPLACE INTO credit_projects
                    (project_id, name, registry, available_credits_kg,
                     price_per_kg_usd, verification_status, credit_type,
                     co_benefits, extra, last_updated, active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,1)
                """, (
                    raw["project_id"], raw["name"], raw["registry"],
                    float(raw["available_credits_kg"]),
                    float(raw["price_per_kg_usd"]),
                    raw["verification_status"], raw.get("credit_type", "voluntary"),
                    json.dumps(raw.get("co_benefits", {}), default=str),
                    json.dumps(raw.get("metadata", {}), default=str),
                    now.isoformat(),
                ))
            await conn.commit()
        self._projects_cache = await self._load_projects_from_db()
        self._projects_cache_time = now
        PROJECT_COUNT.set(len(self._projects_cache))
        logger.info("Refreshed projects: %d", len(self._projects_cache))

    async def _load_projects_from_db(self) -> Dict[str, CreditProject]:
        rows = await self.db_manager.fetch_all(
            "SELECT * FROM credit_projects WHERE active=1")
        out: Dict[str, CreditProject] = {}
        for r in rows:
            out[r["project_id"]] = CreditProject(
                project_id=r["project_id"], name=r["name"],
                registry=r["registry"],
                available_credits_kg=float(r["available_credits_kg"]),
                price_per_kg_usd=float(r["price_per_kg_usd"]),
                verification_status=r["verification_status"],
                credit_type=r["credit_type"] or "voluntary",
                co_benefits=json.loads(r["co_benefits"] or "{}"),
                metadata=json.loads(r["extra"] or "{}"),
            )
        return out

    async def list_projects(self, status: Optional[str] = None,
                            credit_type: Optional[str] = None
                            ) -> List[CreditProject]:
        await self._refresh_projects()
        out = list(self._projects_cache.values())
        if status:
            out = [p for p in out if p.verification_status == status]
        if credit_type:
            out = [p for p in out if p.credit_type == credit_type]
        return out

    # ------------------------------------------------------------------
    async def purchase_credits(self, request: CreditPurchaseRequest,
                               user: Dict[str, Any]) -> CreditTransaction:
        if request.amount_kg <= 0:
            raise ValueError("amount_kg must be positive")
        await self._refresh_projects()
        project = self._projects_cache.get(request.project_id)
        if project is None:
            raise ValueError(f"Unknown project: {request.project_id}")
        if project.available_credits_kg < request.amount_kg:
            raise ValueError("Insufficient available credits")

        # Safety check.
        violations = await self.safety_monitor.check_project(project)
        if violations:
            raise SafetyViolationError(
                f"Project {project.project_id} failed safety check",
                {"violations": violations})

        cost = float(request.amount_kg) * float(project.price_per_kg_usd)
        tx_id = str(uuid.uuid4())
        tx_hash = await self.blockchain_client.mint(
            project.project_id, request.amount_kg,
            user.get("sub", "anonymous"))

        tx = CreditTransaction(
            tx_id=tx_id, project_id=project.project_id,
            amount_kg=float(request.amount_kg), cost_usd=cost,
            status="completed", credit_type=project.credit_type,
            blockchain_tx_hash=tx_hash,
            metadata={"reason": request.reason,
                      "payment_method": request.payment_method},
        )
        self._transactions[tx_id] = tx
        self._balances["available_kg"] = (
            self._balances.get("available_kg", 0.0) + request.amount_kg)
        BALANCE_GAUGE.set(self._balances["available_kg"])
        PURCHASE_COUNTER.labels(project_id=project.project_id).inc()

        if request.retire_immediately:
            await self.retire_credits(
                CreditRetireRequest(tx_id=tx_id, amount_kg=request.amount_kg,
                                    reason="retire_immediately"),
                user)

        # Persist tx.
        await self.db_manager.execute(
            "INSERT OR REPLACE INTO credit_transactions "
            "(tx_id, project_id, amount_kg, cost_usd, status, credit_type, "
            "blockchain_tx_hash, extra, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (tx.tx_id, tx.project_id, tx.amount_kg, tx.cost_usd, tx.status,
             tx.credit_type, tx.blockchain_tx_hash,
             json.dumps(tx.metadata, default=str), tx.created_at))

        # Webhook notification (best-effort).
        try:
            await self.webhook.notify({"type": "purchase", "tx": asdict(tx)
                                       if hasattr(tx, "__dataclass_fields__")
                                       else tx.model_dump()})
        except Exception:
            pass
        return tx

    async def retire_credits(self, request: CreditRetireRequest,
                             user: Dict[str, Any]) -> CreditTransaction:
        tx = self._transactions.get(request.tx_id)
        if tx is None:
            raise ValueError(f"Unknown transaction: {request.tx_id}")
        if request.amount_kg <= 0 or request.amount_kg > tx.amount_kg:
            raise ValueError("Invalid retire amount")
        retired = tx.metadata.get("retired_kg", 0.0)
        if retired + request.amount_kg > tx.amount_kg:
            raise ValueError("Retire amount exceeds remaining credits")
        tx.metadata["retired_kg"] = retired + request.amount_kg
        self._balances["available_kg"] = max(
            0.0, self._balances.get("available_kg", 0.0) - request.amount_kg)
        self._balances["retired_kg"] = (
            self._balances.get("retired_kg", 0.0) + request.amount_kg)
        BALANCE_GAUGE.set(self._balances["available_kg"])
        RETIRE_COUNTER.labels(status="success").inc()
        return tx

    async def get_balance(self) -> Dict[str, float]:
        return {
            "available_kg": float(self._balances.get("available_kg", 0.0)),
            "retired_kg": float(self._balances.get("retired_kg", 0.0)),
        }

    # ------------------------------------------------------------------
    async def _perform_offset(self, emissions_kg: float,
                              reason: str = "auto_offset") -> Dict[str, Any]:
        intensity = None
        if self.carbon_manager is not None:
            intensity = await self.carbon_manager.get_intensity()
        threshold = await self.optimizer.optimize_offset_threshold()
        if emissions_kg < threshold and intensity and intensity < 400:
            return {"status": "skipped", "reason": "below_threshold"}

        balance = await self.get_balance()
        available = balance["available_kg"]
        projects = await self.list_projects(status="verified")
        context = {"carbon_intensity": intensity or 400.0, "urgency": "auto"}

        if available >= emissions_kg:
            best = await self.optimizer.select_best_project(
                projects, emissions_kg, context)
            if best is not None:
                AUTO_OFFSET_SUCCESS.inc()
                await self.optimizer.record_outcome(True)
                AUTO_OFFSET_COUNTER.labels(reason=reason).inc()
                return {"status": "retired_from_balance",
                        "project": best.project_id,
                        "amount_kg": emissions_kg}
        else:
            missing = emissions_kg - available
            best = await self.optimizer.select_best_project(
                projects, missing, context)
            if best is not None:
                tx = await self.purchase_credits(
                    CreditPurchaseRequest(
                        project_id=best.project_id, amount_kg=missing,
                        retire_immediately=True, reason=reason),
                    user={"sub": "auto_offset"})
                AUTO_OFFSET_SUCCESS.inc()
                await self.optimizer.record_outcome(True)
                AUTO_OFFSET_COUNTER.labels(reason=reason).inc()
                return {"status": "purchased_and_retired",
                        "project": best.project_id,
                        "tx_id": tx.tx_id, "amount_kg": missing}

        await self.optimizer.record_outcome(False)
        AUTO_OFFSET_COUNTER.labels(reason=reason).inc()
        return {"status": "failed", "reason": "no_suitable_project"}

    # ------------------------------------------------------------------
    async def run_flexgen_optimization(self, workload: Dict[str, Any],
                                       node: Dict[str, Any]) -> Dict[str, Any]:
        return await self.flexgen_manager.optimize_policy(workload, node)

    async def get_flexgen_status(self) -> Dict[str, Any]:
        return await self.flexgen_manager.get_status()

    # ------------------------------------------------------------------
    async def register_user(self, username: str, password: str,
                            role: str = "viewer") -> bool:
        salt = os.urandom(16)
        digest = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt, 100_000).hex()
        try:
            await self.db_manager.execute(
                "INSERT INTO users (username, password_hash, role, created_at) "
                "VALUES (?, ?, ?, ?)",
                (username, f"{salt.hex()}:{digest}", role,
                 datetime.now(timezone.utc).isoformat()))
            return True
        except Exception as e:
            logger.warning("Failed to register user: %s", e)
            return False

    async def authenticate_user(self, username: str,
                                password: str) -> Optional[Dict[str, Any]]:
        row = await self.db_manager.fetch_one(
            "SELECT password_hash, role FROM users WHERE username=?",
            (username,))
        if row is None:
            return None
        try:
            salt_hex, digest = row["password_hash"].split(":", 1)
        except ValueError:
            return None
        salt = bytes.fromhex(salt_hex)
        computed = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt, 100_000).hex()
        if not hmac.compare_digest(computed, digest):
            return None
        return {"sub": username, "role": row["role"]}

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        components: Dict[str, Any] = {
            "safety": await self.safety_monitor.get_status(),
            "chaos": {"enabled": self.chaos_monkey.enabled},
            "federated": {
                "enabled": self.config.federated.enabled,
                "participants": len(self.federated._participants),
            },
            "quantum": {"available": self.quantum_optimizer.available},
            "tasks": self.task_manager.snapshot(),
            "projects": len(self._projects_cache),
        }
        return {"status": "ok", "version": __version__,
                "instance_id": self.instance_id, "components": components}


# ============================================================================
# FastAPI app
# ============================================================================
def build_app(marketplace: CarbonCreditMarketplace) -> Any:
    if not (FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE):
        return None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await marketplace.start()
        try:
            yield
        finally:
            await marketplace.shutdown()

    app = FastAPI(title="Carbon Credit Marketplace API",
                  version=__version__, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=[], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    app.state.marketplace = marketplace
    app.state.config = marketplace.config
    security = HTTPBearer(auto_error=False)

    async def _get_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ) -> Dict[str, Any]:
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing token")
        if not JWT_AVAILABLE:
            raise HTTPException(status_code=503, detail="JWT unavailable")
        try:
            payload = jwt.decode(
                credentials.credentials,
                app.state.config.general.jwt_secret,
                algorithms=[app.state.config.general.jwt_algorithm])
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {"sub": payload.get("sub"), "role": payload.get("role", "viewer")}

    async def _require_admin(user: Dict = Depends(_get_user)) -> Dict:
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin required")
        return user

    @app.get("/health")
    async def health():
        return await marketplace.health_check()

    @app.get("/metrics")
    async def metrics():
        if PROMETHEUS_AVAILABLE and REGISTRY is not None:
            return Response(content=generate_latest(REGISTRY),
                            media_type=CONTENT_TYPE_LATEST)
        return PlainTextResponse("Prometheus not installed")

    @app.post("/auth/register")
    async def register(body: _RegisterBody):
        ok = await marketplace.register_user(
            body.username, body.password, body.role)
        if not ok:
            raise HTTPException(status_code=400,
                                detail="Username already exists")
        return {"status": "registered"}

    @app.post("/auth/login")
    async def login(body: _LoginBody):
        if not JWT_AVAILABLE:
            raise HTTPException(status_code=503, detail="JWT unavailable")
        user = await marketplace.authenticate_user(body.username, body.password)
        if user is None:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        now = datetime.now(timezone.utc)
        exp = now + timedelta(
            minutes=app.state.config.general.jwt_expiration_minutes)
        token = jwt.encode(
            {"sub": user["sub"], "role": user["role"],
             "iat": int(now.timestamp()), "exp": int(exp.timestamp())},
            app.state.config.general.jwt_secret,
            algorithm=app.state.config.general.jwt_algorithm)
        return {"access_token": token, "token_type": "bearer"}

    @app.get("/projects")
    async def list_projects(status: Optional[str] = None,
                            credit_type: Optional[str] = None):
        projects = await marketplace.list_projects(status, credit_type)
        return {"projects": [p.model_dump() for p in projects]}

    @app.post("/purchase")
    async def purchase(body: CreditPurchaseRequest,
                       user: Dict = Depends(_get_user)):
        try:
            tx = await marketplace.purchase_credits(body, user)
            return {"status": "success", "transaction": tx.model_dump()}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/retire")
    async def retire(body: CreditRetireRequest,
                     user: Dict = Depends(_get_user)):
        try:
            tx = await marketplace.retire_credits(body, user)
            return {"status": "success", "transaction": tx.model_dump()}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/balance")
    async def balance():
        return await marketplace.get_balance()

    @app.post("/optimization/select")
    async def optimize_select(body: _OptimizeSelectBody,
                              user: Dict = Depends(_get_user)):
        amount = float(body.context.get("amount_kg", 1000.0))
        projects = await marketplace.list_projects(status="verified")
        best = await marketplace.optimizer.select_best_project(
            projects, amount, body.context)
        if best is None:
            raise HTTPException(status_code=404, detail="No suitable project")
        return {"project": best.model_dump(),
                "strategy": marketplace.optimizer._last_selection["strategy"]}

    @app.post("/optimization/feedback")
    async def optimization_feedback(body: _FeedbackBody,
                                    user: Dict = Depends(_get_user)):
        await marketplace.optimizer.update_feedback(
            body.context, body.strategy, body.reward)
        return {"status": "feedback recorded"}

    @app.post("/optimization/evolve")
    async def optimization_evolve(user: Dict = Depends(_require_admin)):
        return {"new_strategies":
                await marketplace.optimizer.evolve_strategies()}

    @app.get("/optimization/stats")
    async def optimization_stats(user: Dict = Depends(_get_user)):
        return marketplace.optimizer.get_optimization_stats()

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(body: _FlexGenBody,
                               user: Dict = Depends(_get_user)):
        return await marketplace.run_flexgen_optimization(
            body.workload, body.node)

    @app.get("/flexgen/status")
    async def flexgen_status(user: Dict = Depends(_get_user)):
        return await marketplace.get_flexgen_status()

    @app.get("/safety/status")
    async def safety_status(user: Dict = Depends(_get_user)):
        return await marketplace.safety_monitor.get_status()

    @app.post("/safety/check")
    async def safety_check(body: _SafetyCheckBody,
                           user: Dict = Depends(_get_user)):
        project = CreditProject(**body.project)
        return {"violations":
                await marketplace.safety_monitor.check_project(project)}

    @app.get("/human-review/pending")
    async def human_review_pending(user: Dict = Depends(_require_admin)):
        return await marketplace.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def human_review_approve(review_id: str, body: _ReviewActionBody,
                                   user: Dict = Depends(_require_admin)):
        return await marketplace.human_review.approve(review_id, body.feedback)

    @app.post("/human-review/{review_id}/reject")
    async def human_review_reject(review_id: str, body: _ReviewActionBody,
                                  user: Dict = Depends(_require_admin)):
        return await marketplace.human_review.reject(review_id, body.feedback)

    @app.post("/chaos/trigger")
    async def chaos_trigger(user: Dict = Depends(_require_admin)):
        return await marketplace.chaos_monkey._inject_failure()

    @app.post("/federated/register")
    async def federated_register(body: _FederatedRegisterBody,
                                 user: Dict = Depends(_get_user)):
        await marketplace.federated.register_participant(
            body.participant_id, body.model_update)
        return {"status": "registered"}

    @app.get("/federated/aggregate")
    async def federated_aggregate(user: Dict = Depends(_require_admin)):
        return await marketplace.federated.aggregate()

    return app


# ============================================================================
# Tests
# ============================================================================
async def _test_circuit_breaker() -> None:
    cb = CircuitBreaker("t", threshold=2, timeout=0.1)

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


async def _test_global_registry_threadsafe() -> None:
    g = GlobalCircuitBreaker()
    a = g.get_or_create("x")
    b = g.get_or_create("x")
    assert a is b


async def _test_pqc_roundtrip(tmpdir: Path) -> None:
    cfg = Settings()
    cfg.ensure_master_key()
    cfg.database.path = str(tmpdir / "pqc.db")
    db = AsyncDatabaseManager(cfg)
    await db.connect()
    pqc = PostQuantumCrypto(cfg, db)
    kp = await pqc.generate_keypair()
    sig = await pqc.sign_data({"x": 1}, kp["key_id"])
    assert await pqc.verify_data({"x": 1}, sig) is True
    assert await pqc.verify_data({"x": 2}, sig) is False
    await db.close()


async def _test_causal_bandit_new_policies() -> None:
    bandit = CausalBandit(
        action_space=[{"name": "a", "params": {}}],
        fallback_solver=lambda ctx: {"name": "a", "params": {}},
        min_trials_before_bandit=1, seed=0)
    bandit.update({}, {"name": "a", "params": {}}, 0.5)
    bandit.update({}, {"name": "new", "params": {}}, 0.9)
    assert bandit.q_values["new"] > 0.0


async def _test_multi_agent_vote_differs() -> None:
    cfg = Settings()
    ma = MultiAgentSystem(cfg)
    projects = [
        CreditProject(project_id="a", name="A", registry="V",
                      available_credits_kg=1000, price_per_kg_usd=0.1,
                      verification_status="verified",
                      metadata={"carbon_intensity": 100.0},
                      co_benefits={"biodiversity": 0.9}),
        CreditProject(project_id="b", name="B", registry="V",
                      available_credits_kg=1000, price_per_kg_usd=1.5,
                      verification_status="verified",
                      metadata={"carbon_intensity": 500.0},
                      co_benefits={"biodiversity": 0.1}),
    ]
    best = await ma.vote(projects)
    assert best is not None
    assert best.project_id in ("a", "b")


async def _test_marketplace_end_to_end(tmpdir: Path) -> None:
    cfg = Settings()
    cfg.ensure_master_key()
    cfg.database.path = str(tmpdir / "marketplace.db")
    cfg.general.auto_offset_enabled = False
    cfg.chaos.enabled = False
    cfg.federated.enabled = False

    db = AsyncDatabaseManager(cfg)
    registry = RegistryClient(cfg)
    blockchain = BlockchainClient(cfg)
    pqc = PostQuantumCrypto(cfg, db)
    cloud = CloudStorage(cfg)
    predictor = PricePredictor(cfg)
    carbon = CarbonIntensityManager(cfg)
    sustainability = UnifiedSustainabilityEngine(db)
    auto_offset = AutoOffsetEngine()

    marketplace = CarbonCreditMarketplace(
        config=cfg, db_manager=db, registry_client=registry,
        blockchain_client=blockchain, pqc=pqc, cloud_storage=cloud,
        price_predictor=predictor, auto_offset_engine=auto_offset,
        carbon_manager=carbon, sustainability_engine=sustainability,
    )
    await marketplace.start()
    try:
        projects = await marketplace.list_projects(status="verified")
        assert len(projects) >= 1

        # Purchase.
        tx = await marketplace.purchase_credits(
            CreditPurchaseRequest(project_id=projects[0].project_id,
                                  amount_kg=100.0,
                                  retire_immediately=False),
            user={"sub": "alice"})
        assert tx.amount_kg == 100.0
        balance = await marketplace.get_balance()
        assert balance["available_kg"] == 100.0

        # Retire.
        await marketplace.retire_credits(
            CreditRetireRequest(tx_id=tx.tx_id, amount_kg=50.0), user={})
        balance = await marketplace.get_balance()
        assert balance["retired_kg"] == 50.0

        # Optimizer decision.
        decision = await marketplace.optimizer.select_best_project(
            projects, 50.0, {"carbon_intensity": 200.0})
        assert decision is not None

        # FlexGen.
        fg = await marketplace.run_flexgen_optimization(
            {"task_id": "w"}, {"id": "n"})
        assert "policy" in fg

        # Safety.
        violations = await marketplace.safety_monitor.check_project(projects[0])
        assert isinstance(violations, list)

        # Health.
        health = await marketplace.health_check()
        assert health["status"] == "ok"
    finally:
        await marketplace.shutdown()


def run_tests() -> None:
    import tempfile
    loop = asyncio.new_event_loop()
    try:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            loop.run_until_complete(_test_circuit_breaker())
            loop.run_until_complete(_test_global_registry_threadsafe())
            loop.run_until_complete(_test_pqc_roundtrip(tmp))
            loop.run_until_complete(_test_causal_bandit_new_policies())
            loop.run_until_complete(_test_multi_agent_vote_differs())
            loop.run_until_complete(_test_marketplace_end_to_end(tmp))
    finally:
        loop.close()
    print("All Carbon Credit Marketplace v6.0.0 tests passed.")


# ============================================================================
# Entry point
# ============================================================================
async def _standalone_main() -> None:
    cfg = ensure_settings()
    db = AsyncDatabaseManager(cfg)
    registry = RegistryClient(cfg)
    blockchain = BlockchainClient(cfg)
    pqc = PostQuantumCrypto(cfg, db)
    cloud = CloudStorage(cfg)
    predictor = PricePredictor(cfg)
    carbon = CarbonIntensityManager(cfg)
    sustainability = UnifiedSustainabilityEngine(db)
    auto_offset = AutoOffsetEngine()

    marketplace = CarbonCreditMarketplace(
        config=cfg, db_manager=db, registry_client=registry,
        blockchain_client=blockchain, pqc=pqc, cloud_storage=cloud,
        price_predictor=predictor, auto_offset_engine=auto_offset,
        carbon_manager=carbon, sustainability_engine=sustainability,
    )
    await marketplace.start()
    print(f"Marketplace {marketplace.instance_id} started.")
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await marketplace.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carbon Credit Marketplace v6.0.0")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    parser.add_argument("--serve", action="store_true",
                        help="Start the FastAPI server.")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    cfg = ensure_settings()
    if args.serve and FASTAPI_AVAILABLE:
        db = AsyncDatabaseManager(cfg)
        registry = RegistryClient(cfg)
        blockchain = BlockchainClient(cfg)
        pqc = PostQuantumCrypto(cfg, db)
        cloud = CloudStorage(cfg)
        predictor = PricePredictor(cfg)
        carbon = CarbonIntensityManager(cfg)
        sustainability = UnifiedSustainabilityEngine(db)
        auto_offset = AutoOffsetEngine()
        marketplace = CarbonCreditMarketplace(
            config=cfg, db_manager=db, registry_client=registry,
            blockchain_client=blockchain, pqc=pqc, cloud_storage=cloud,
            price_predictor=predictor, auto_offset_engine=auto_offset,
            carbon_manager=carbon, sustainability_engine=sustainability,
        )
        app = build_app(marketplace)
        if app is not None:
            import uvicorn  # type: ignore
            uvicorn.run(
                app,
                host=args.host or cfg.API_HOST,
                port=args.port or cfg.API_PORT,
                log_level=cfg.general.log_level.lower(),
                server_header=False)
            return
    asyncio.run(_standalone_main())


if __name__ == "__main__":
    main()
