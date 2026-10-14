#!/usr/bin/env python3
# ============================================================================
# Green Agent Base Classes - Version 15.0 (Enhanced)
# ============================================================================
"""
Green Agent Base Classes — orchestration layer for the Green Agent ecosystem.

CHANGES (v15.0 over v14.5):
- Import fixes: ``logging.handlers``, tenacity fallback stubs, Pydantic v2
  detection split from ``pydantic_settings``.
- Master-key flow fixed: validator allows empty value; auto-generation occurs
  once at system start.
- ``EnhancedCircuitBreaker.call`` awaits coroutines only.
- ``get_optimization_stats`` is async; the FastAPI endpoint awaits it.
- ``CausalBandit.update`` no longer raises KeyError for bio-generated policies.
- ``PostQuantumCrypto`` salt is persisted to disk and reused.
- ``_fallback_generate_keypair`` returns a coroutine instead of
  ``asyncio.create_task``.
- JWT authentication added to FastAPI endpoints; role checks on admin routes.
- ``FeedbackEvent`` emitted from optimizer decisions.
- ``SafetyMonitor`` instance is shared across components.
- Single long-lived aiosqlite connection behind a lock.
- Migrations are wrapped in transactions with rollback on failure.
- FastAPI request bodies are Pydantic models.
- Deterministic RNG seeded from config; no unseeded ``random`` calls.
- ``__version__`` and ``__all__`` added.
- Built-in test suite: ``python green_agent_base_v15_0.py --test``.

Run the full system:

    python green_agent_base_v15_0.py
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import hmac
import inspect
import json
import logging
import logging.handlers  # FIX: required for RotatingFileHandler
import os
import pickle  # noqa: F401 (kept for forward compatibility)
import secrets
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol,
    Set, Tuple, Type, Union, runtime_checkable,
)

import numpy as np

try:
    import pandas as pd  # noqa: F401
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

__version__ = "15.0.0"
__all__ = [
    "__version__",
    "GreenAgentConfig",
    "GeneralConfig",
    "DatabaseConfig",
    "BlockchainConfig",
    "AnalyticsConfig",
    "EdgeConfig",
    "CloudConfig",
    "NLPConfig",
    "APIConfig",
    "SafetyConfig",
    "ChaosConfig",
    "EventBus",
    "CircuitBreakerState",
    "EnhancedCircuitBreaker",
    "GlobalCircuitBreaker",
    "EnhancedRateLimiter",
    "AsyncDatabaseManager",
    "PostQuantumCrypto",
    "BlockchainIntegration",
    "AdvancedPredictiveAnalytics",
    "MultiCloudDistribution",
    "DataLakeIntegration",
    "RealTimeMonitoring",
    "MLOpsPipeline",
    "MultiRegionManager",
    "EdgeComputing",
    "SustainableNLP",
    "CausalBandit",
    "SafetyMonitor",
    "ChaosMonkey",
    "EnhancedAutonomousOptimizer",
    "GeospatialIntelligence",
    "FinancialModeler",
    "EnvironmentalImpactAnalyzer",
    "APIGateway",
    "GreenAgentSystem",
    "main",
    "run_tests",
]


# ============================================================================
# Optional dependencies with graceful fallbacks
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
    from pydantic import field_validator, model_validator  # type: ignore
    PYDANTIC_V2 = True
except ImportError:  # pragma: no cover
    field_validator = None  # type: ignore
    model_validator = None  # type: ignore
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

# Tenacity — real or stubbed.
try:
    from tenacity import (  # type: ignore
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:  # pragma: no cover
    TENACITY_AVAILABLE = False

    def retry(*a, **k):
        def deco(f): return f
        return deco
    def stop_after_attempt(_n): return None
    def wait_exponential(**k): return None
    def retry_if_exception_type(_t): return None
    def before_sleep_log(*a, **k): return None

try:
    import aiosqlite  # type: ignore
    AIOSQLITE_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiosqlite = None  # type: ignore
    AIOSQLITE_AVAILABLE = False

try:
    from fastapi import (  # type: ignore
        FastAPI, Depends, HTTPException, Header, Request, status,
    )
    from fastapi.responses import JSONResponse, Response, PlainTextResponse
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False

try:
    from prometheus_client import (  # type: ignore
        Counter, Gauge, Histogram, CollectorRegistry,
        generate_latest, CONTENT_TYPE_LATEST,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

try:
    from jose import JWTError, jwt  # type: ignore
    JOSE_AVAILABLE = True
except ImportError:  # pragma: no cover
    JOSE_AVAILABLE = False

try:
    from cryptography.fernet import Fernet  # type: ignore
    from cryptography.hazmat.backends import default_backend  # type: ignore
    from cryptography.hazmat.primitives import hashes  # type: ignore
    from cryptography.hazmat.primitives.asymmetric import ec  # type: ignore
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # type: ignore
    CRYPTO_AVAILABLE = True
except ImportError:  # pragma: no cover
    CRYPTO_AVAILABLE = False

# Optional FeedbackEvent integration.
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# ============================================================================
# Structured logging
# ============================================================================
try:
    import structlog  # type: ignore
    logger = structlog.get_logger(__name__)
    STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    STRUCTLOG_AVAILABLE = False
    _log_dir = Path(os.environ.get("GREEN_AGENT_LOG_DIR", "./logs"))
    _log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=os.environ.get("GREEN_AGENT_LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.handlers.RotatingFileHandler(
                _log_dir / "green_agent.log", maxBytes=10 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger(__name__)


# ============================================================================
# Prometheus metrics
# ============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    MODEL_PREDICTIONS = Counter(
        "model_predictions_total", "Total model predictions",
        ["model_name", "version", "status"], registry=REGISTRY)
    MODEL_PREDICTION_LATENCY = Histogram(
        "model_prediction_duration_seconds", "Prediction duration",
        ["model_name", "version"], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge(
        "circuit_breaker_state", "Circuit breaker state", ["name"], registry=REGISTRY)
    HEALTH_SCORE = Gauge(
        "component_health_score", "Component health score (0-100)",
        ["component"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter(
        "safety_violations_total", "Safety violations", ["rule"], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter(
        "chaos_experiments_total", "Chaos experiments", ["type", "status"],
        registry=REGISTRY)
    API_REQUESTS = Counter(
        "api_requests_total", "API requests",
        ["endpoint", "method", "status"], registry=REGISTRY)
    CARBON_INTENSITY = Gauge(
        "carbon_intensity_gco2_per_kwh", "Real-time carbon intensity",
        registry=REGISTRY)
    OPTIMIZER_DECISIONS = Counter(
        "optimizer_decisions_total", "Optimizer decisions",
        ["strategy", "source"], registry=REGISTRY)
else:  # pragma: no cover
    class _DummyMetric:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    MODEL_PREDICTIONS = _DummyMetric()
    MODEL_PREDICTION_LATENCY = _DummyMetric()
    CIRCUIT_BREAKER_STATE = _DummyMetric()
    HEALTH_SCORE = _DummyMetric()
    SAFETY_VIOLATIONS = _DummyMetric()
    CHAOS_EXPERIMENTS = _DummyMetric()
    API_REQUESTS = _DummyMetric()
    CARBON_INTENSITY = _DummyMetric()
    OPTIMIZER_DECISIONS = _DummyMetric()
    REGISTRY = None


# ============================================================================
# Custom exceptions
# ============================================================================
class GreenAgentException(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)
        self.correlation_id = str(uuid.uuid4())[:8]


class QuantumError(GreenAgentException): pass
class BlockchainError(GreenAgentException): pass
class DataLakeError(GreenAgentException): pass
class EdgeDeviceError(GreenAgentException): pass
class MLOpsError(GreenAgentException): pass
class APIGatewayError(GreenAgentException): pass
class CircuitBreakerOpenError(GreenAgentException): pass
class AuthenticationError(GreenAgentException): pass
class SecurityError(GreenAgentException): pass
class SafetyViolationError(GreenAgentException): pass
class ChaosExperimentError(GreenAgentException): pass


# ============================================================================
# Config
# ============================================================================
def _default_jwt_secret() -> str:
    return secrets.token_hex(32)


if PYDANTIC_AVAILABLE and _SETTINGS_AVAILABLE and PYDANTIC_V2:
    class GeneralConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        max_prediction_history: int = 10_000
        max_cache_size: int = 1_000
        cache_ttl_seconds: int = 300
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        health_check_timeout: int = 10
        rate_limit_requests: int = 1_000
        rate_limit_window: int = 60
        data_version: int = 15
        log_level: str = "INFO"
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        human_review_threshold: float = 0.5
        random_seed: int = 42

    class DatabaseConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        path: str = "./green_agent.db"

    class BlockchainConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        rpc_url: str = "http://localhost:8545"
        chain_id: int = 1337
        private_key: Optional[str] = None
        contract_address: str = "0x0000000000000000000000000000000000000000"

    class AnalyticsConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        prophet_changepoint_prior_scale: float = 0.05
        prophet_seasonality_prior_scale: float = 10.0
        lstm_units: int = 50
        lstm_epochs: int = 10
        lstm_batch_size: int = 32
        ensemble_weights: Optional[List[float]] = None
        modp_weights: Dict[str, float] = Field(default_factory=lambda: {
            "energy": 0.25, "carbon": 0.25, "latency": 0.20, "accuracy": 0.30})
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    class EdgeConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        mqtt_broker: str = "localhost"
        mqtt_port: int = 1883

    class CloudConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        s3_bucket: str = "green-agent-data-lake"
        s3_prefix: str = "sustainability/"
        athena_database: str = "green_agent"
        athena_table: str = "sustainability_metrics"

    class NLPConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        model_name: str = "distilgpt2"

    class APIConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = Field(default_factory=_default_jwt_secret)
        jwt_algorithm: str = "HS256"
        token_expiry_minutes: int = 60

    class SafetyConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        max_carbon_intensity: float = 500.0
        min_renewable_share: float = 0.3
        max_latency_ms: float = 1000.0

    class ChaosConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = False
        failure_probability: float = 0.1
        experiment_interval_seconds: int = 120

    class GreenAgentConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="GREEN_AGENT_", case_sensitive=False, extra="ignore")
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        blockchain: BlockchainConfig = Field(default_factory=BlockchainConfig)
        analytics: AnalyticsConfig = Field(default_factory=AnalyticsConfig)
        edge: EdgeConfig = Field(default_factory=EdgeConfig)
        cloud: CloudConfig = Field(default_factory=CloudConfig)
        nlp: NLPConfig = Field(default_factory=NLPConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        quantum_backend: str = "aer_simulator"
        quantum_n_qubits: int = 4
        quantum_qaoa_reps: int = 1
        master_key: str = ""

        @field_validator("master_key")
        @classmethod
        def _check_master_key(cls, v: str) -> str:
            # Allow empty so main() can auto-generate before the system starts.
            if v and len(v) < 32:
                raise ValueError("master_key must be at least 32 hex chars when set")
            return v

        def ensure_master_key(self) -> str:
            """Ensure master_key is set; generate if empty. Called at startup."""
            if not self.master_key:
                object.__setattr__(self, "master_key", secrets.token_hex(32))
            return self.master_key

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("master_key not set")
            return bytes.fromhex(self.master_key)

else:  # pragma: no cover - fallback path
    @dataclass
    class GeneralConfig:
        max_prediction_history: int = 10_000
        max_cache_size: int = 1_000
        cache_ttl_seconds: int = 300
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        health_check_timeout: int = 10
        rate_limit_requests: int = 1_000
        rate_limit_window: int = 60
        data_version: int = 15
        log_level: str = "INFO"
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        human_review_threshold: float = 0.5
        random_seed: int = 42

    @dataclass
    class DatabaseConfig:
        path: str = "./green_agent.db"

    @dataclass
    class BlockchainConfig:
        rpc_url: str = "http://localhost:8545"
        chain_id: int = 1337
        private_key: Optional[str] = None
        contract_address: str = "0x0000000000000000000000000000000000000000"

    @dataclass
    class AnalyticsConfig:
        prophet_changepoint_prior_scale: float = 0.05
        prophet_seasonality_prior_scale: float = 10.0
        lstm_units: int = 50
        lstm_epochs: int = 10
        lstm_batch_size: int = 32
        ensemble_weights: Optional[List[float]] = None
        modp_weights: Dict[str, float] = field(default_factory=lambda: {
            "energy": 0.25, "carbon": 0.25, "latency": 0.20, "accuracy": 0.30})
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    @dataclass
    class EdgeConfig:
        mqtt_broker: str = "localhost"
        mqtt_port: int = 1883

    @dataclass
    class CloudConfig:
        s3_bucket: str = "green-agent-data-lake"
        s3_prefix: str = "sustainability/"
        athena_database: str = "green_agent"
        athena_table: str = "sustainability_metrics"

    @dataclass
    class NLPConfig:
        model_name: str = "distilgpt2"

    @dataclass
    class APIConfig:
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = field(default_factory=_default_jwt_secret)
        jwt_algorithm: str = "HS256"
        token_expiry_minutes: int = 60

    @dataclass
    class SafetyConfig:
        max_carbon_intensity: float = 500.0
        min_renewable_share: float = 0.3
        max_latency_ms: float = 1000.0

    @dataclass
    class ChaosConfig:
        enabled: bool = False
        failure_probability: float = 0.1
        experiment_interval_seconds: int = 120

    @dataclass
    class GreenAgentConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        blockchain: BlockchainConfig = field(default_factory=BlockchainConfig)
        analytics: AnalyticsConfig = field(default_factory=AnalyticsConfig)
        edge: EdgeConfig = field(default_factory=EdgeConfig)
        cloud: CloudConfig = field(default_factory=CloudConfig)
        nlp: NLPConfig = field(default_factory=NLPConfig)
        api: APIConfig = field(default_factory=APIConfig)
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        quantum_backend: str = "aer_simulator"
        quantum_n_qubits: int = 4
        quantum_qaoa_reps: int = 1
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
# Event bus
# ============================================================================
class EventBus:
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def subscribe(self, event_type: str, callback: Callable) -> None:
        self._subscribers[event_type].append(callback)

    async def publish(self, event_type: str, data: Any) -> None:
        callbacks = list(self._subscribers.get(event_type, []))
        for cb in callbacks:
            try:
                result = cb(data)
                if inspect.iscoroutine(result):
                    await result
            except Exception as e:
                logger.warning("EventBus callback failed: %s", e)


# ============================================================================
# Circuit breakers
# ============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class EnhancedCircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 half_open_success_threshold: int = 2) -> None:
        self.name = name
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = int(recovery_timeout)
        self.half_open_success_threshold = int(half_open_success_threshold)
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()
        self.metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    def _update_metric(self) -> None:
        try:
            CIRCUIT_BREAKER_STATE.labels(name=self.name).set(
                1 if self.state is CircuitBreakerState.OPEN else
                0.5 if self.state is CircuitBreakerState.HALF_OPEN else 0)
        except Exception:
            pass

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state is CircuitBreakerState.OPEN:
                if (self.last_failure_time is not None
                        and time.monotonic() - self.last_failure_time
                        >= self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    self._update_metric()
                    logger.info("Circuit breaker %s -> HALF_OPEN", self.name)
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker {self.name} is OPEN")
            if (self.state is CircuitBreakerState.HALF_OPEN
                    and self.success_count >= self.half_open_success_threshold):
                self.state = CircuitBreakerState.CLOSED
                self._update_metric()

        self.metrics["total_calls"] += 1
        try:
            result = func(*args, **kwargs)
            if inspect.iscoroutine(result):
                result = await result
            await self._record_success()
            return result
        except Exception:
            await self._record_failure()
            raise

    async def _record_success(self) -> None:
        async with self._lock:
            self.metrics["successful_calls"] += 1
            self.success_count += 1
            if self.state is CircuitBreakerState.HALF_OPEN:
                if self.success_count >= self.half_open_success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self._update_metric()
                    logger.info("Circuit breaker %s -> CLOSED", self.name)
            else:
                self.failure_count = 0

    async def _record_failure(self) -> None:
        async with self._lock:
            self.metrics["failed_calls"] += 1
            self.failure_count += 1
            self.last_failure_time = time.monotonic()
            if (self.state is CircuitBreakerState.CLOSED
                    and self.failure_count >= self.failure_threshold):
                self.state = CircuitBreakerState.OPEN
                self._update_metric()
                logger.warning("Circuit breaker %s -> OPEN after %d failures",
                               self.name, self.failure_count)
            elif self.state is CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self._update_metric()

    def get_metrics(self) -> Dict[str, Any]:
        return {
            **self.metrics,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
        }

    def force_open(self) -> None:
        self.state = CircuitBreakerState.OPEN
        self.failure_count = self.failure_threshold
        self.last_failure_time = time.monotonic()
        self._update_metric()

    def reset(self) -> None:
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self._update_metric()


class GlobalCircuitBreaker:
    _instance: Optional["GlobalCircuitBreaker"] = None

    def __new__(cls) -> "GlobalCircuitBreaker":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._breakers = {}  # type: ignore[attr-defined]
        return cls._instance

    def get_or_create(self, name: str, **kwargs: Any) -> EnhancedCircuitBreaker:
        if name not in self._breakers:  # type: ignore[attr-defined]
            self._breakers[name] = EnhancedCircuitBreaker(name, **kwargs)  # type: ignore[attr-defined]
        return self._breakers[name]  # type: ignore[attr-defined]


# ============================================================================
# Rate limiter
# ============================================================================
class EnhancedRateLimiter:
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
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))
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
# Database
# ============================================================================
class AsyncDatabaseManager:
    SCHEMA_VERSION = 5

    def __init__(self, config: GreenAgentConfig) -> None:
        self.config = config
        self.db_path = Path(getattr(config.database, "path", "./green_agent.db"))
        self._conn: Optional[Any] = None
        self._init_lock = asyncio.Lock()
        self._query_lock = asyncio.Lock()
        self._initialized = False

    async def _connect(self) -> Any:
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

    # ------------------------------------------------------------------
    async def _apply_migrations(self, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
        """)
        await conn.commit()

        cur = await conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        row = await cur.fetchone()
        current = int(row[0]) if row else 0

        async def _migration_block(version: int, fn: Callable[[], Awaitable[None]]) -> None:
            nonlocal current
            if current >= version:
                return
            try:
                await conn.execute("BEGIN")
                await fn()
                await conn.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (version, datetime.now(timezone.utc).isoformat()),
                )
                await conn.commit()
                logger.info("Database migrated to v%d", version)
                current = version
            except Exception as e:
                await conn.rollback()
                logger.error("Migration to v%d failed; rolled back: %s", version, e)
                raise

        async def v1() -> None:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS model_registry (
                    model_id TEXT PRIMARY KEY, name TEXT, version TEXT,
                    metadata TEXT, registered_at TEXT, is_active INTEGER,
                    prediction_count INTEGER, error_count INTEGER,
                    avg_latency_ms REAL, created_at TEXT, updated_at TEXT,
                    version_number INTEGER
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS model_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, model_id TEXT,
                    metric_type TEXT, metric_value REAL, timestamp TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS blockchain_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, tx_hash TEXT,
                    tx_type TEXT, amount REAL, project_id TEXT,
                    timestamp TEXT, status TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    id TEXT PRIMARY KEY, alert_name TEXT, severity TEXT,
                    status TEXT, created_at TEXT, resolved_at TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS edge_devices (
                    device_id TEXT PRIMARY KEY, config TEXT, status TEXT,
                    last_seen TEXT, last_data TEXT, registered_at TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pqc_key_pairs (
                    key_id TEXT PRIMARY KEY, algorithm TEXT NOT NULL,
                    public_key BLOB NOT NULL, private_key BLOB NOT NULL,
                    created_at TEXT NOT NULL, expires_at TEXT NOT NULL
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS optimisation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT NOT NULL,
                    result TEXT, timestamp TEXT NOT NULL
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS distribution_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    optimal_provider TEXT NOT NULL, optimal_region TEXT NOT NULL,
                    scores TEXT, data_size_gb REAL, timestamp TEXT NOT NULL
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    project_id TEXT PRIMARY KEY, name TEXT, company TEXT,
                    city TEXT, country TEXT, lat REAL, lon REAL,
                    capacity_mw REAL, status TEXT, green_score REAL,
                    pue REAL, renewable_share REAL, data TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS system_state (
                    key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
                )""")

        async def v2() -> None:
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status)")

        async def v3() -> None:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS flexgen_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, workload_id TEXT,
                    node_id TEXT, policy_json TEXT, metrics_json TEXT,
                    reward REAL, timestamp TEXT
                )""")

        async def v4() -> None:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, metric_name TEXT NOT NULL,
                    value REAL NOT NULL, timestamp TEXT NOT NULL, metadata TEXT
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sustainability_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT,
                    score REAL, timestamp TEXT, details TEXT
                )""")

        async def v5() -> None:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS explanations (
                    id TEXT PRIMARY KEY, decision_id TEXT NOT NULL,
                    explanation TEXT NOT NULL, model_dump TEXT,
                    timestamp TEXT NOT NULL
                )""")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_explanations_decision "
                "ON explanations(decision_id)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS human_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, decision_id TEXT NOT NULL,
                    status TEXT NOT NULL, feedback TEXT, reviewer TEXT,
                    created_at TEXT, reviewed_at TEXT
                )""")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reviews_decision "
                "ON human_reviews(decision_id)")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS safety_violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT NOT NULL,
                    details TEXT, timestamp TEXT NOT NULL
                )""")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS chaos_experiments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    experiment_type TEXT NOT NULL, target_component TEXT,
                    status TEXT NOT NULL, result TEXT, timestamp TEXT NOT NULL
                )""")

        await _migration_block(1, v1)
        await _migration_block(2, v2)
        await _migration_block(3, v3)
        await _migration_block(4, v4)
        await _migration_block(5, v5)

    # ------------------------------------------------------------------
    async def _execute(self, query: str, params: tuple = ()) -> Any:
        conn = await self._connect()
        async with self._query_lock:
            cur = await conn.execute(query, params)
            await conn.commit()
            return cur

    async def _fetch_all(self, query: str, params: tuple = ()) -> List[Any]:
        conn = await self._connect()
        async with self._query_lock:
            cur = await conn.execute(query, params)
            return await cur.fetchall()

    async def _fetch_one(self, query: str, params: tuple = ()) -> Optional[Any]:
        conn = await self._connect()
        async with self._query_lock:
            cur = await conn.execute(query, params)
            return await cur.fetchone()

    # CRUD methods
    async def save_model_registry(self, model_id: str, name: str, version: str,
                                  metadata: Dict[str, Any], is_active: bool = True) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._execute("""
            INSERT OR REPLACE INTO model_registry
            (model_id, name, version, metadata, registered_at, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (model_id, name, version, json.dumps(metadata, default=str),
              1 if is_active else 0, now, now))

    async def save_blockchain_transaction(self, tx_hash: str, tx_type: str,
                                          amount: float, project_id: str,
                                          status: str = "success") -> None:
        await self._execute("""
            INSERT INTO blockchain_transactions
            (tx_hash, tx_type, amount, project_id, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (tx_hash, tx_type, amount, project_id,
              datetime.now(timezone.utc).isoformat(), status))

    async def save_edge_device(self, device_id: str, config: Dict[str, Any],
                               status: str, last_seen: Optional[datetime] = None,
                               last_data: Optional[Dict[str, Any]] = None) -> None:
        await self._execute("""
            INSERT OR REPLACE INTO edge_devices
            (device_id, config, status, last_seen, last_data, registered_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (device_id, json.dumps(config, default=str), status,
              last_seen.isoformat() if last_seen else None,
              json.dumps(last_data or {}, default=str),
              datetime.now(timezone.utc).isoformat()))

    async def save_pqc_keypair(self, key_id: str, algorithm: str,
                               public_key: bytes, private_key: bytes,
                               expires_at: str) -> None:
        await self._execute("""
            INSERT OR REPLACE INTO pqc_key_pairs
            (key_id, algorithm, public_key, private_key, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (key_id, algorithm, public_key, private_key,
              datetime.now(timezone.utc).isoformat(), expires_at))

    async def get_pqc_keypair(self, key_id: str) -> Optional[Dict[str, Any]]:
        row = await self._fetch_one(
            "SELECT algorithm, public_key, private_key, created_at, expires_at "
            "FROM pqc_key_pairs WHERE key_id=?", (key_id,))
        if row is None:
            return None
        return {
            "algorithm": row[0], "public_key": row[1], "private_key": row[2],
            "created_at": row[3], "expires_at": row[4],
        }

    async def save_optimisation(self, strategy: str, result: Dict[str, Any]) -> None:
        await self._execute(
            "INSERT INTO optimisation_history (strategy, result, timestamp) "
            "VALUES (?, ?, ?)",
            (strategy, json.dumps(result, default=str),
             datetime.now(timezone.utc).isoformat()))

    async def get_recent_optimisations(self, limit: int = 10) -> List[Dict[str, Any]]:
        rows = await self._fetch_all(
            "SELECT strategy, result, timestamp FROM optimisation_history "
            "ORDER BY id DESC LIMIT ?", (limit,))
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                out.append({
                    "strategy": r[0], "result": json.loads(r[1]),
                    "timestamp": r[2],
                })
            except json.JSONDecodeError:
                continue
        return out

    async def save_distribution(self, result: Dict[str, Any]) -> None:
        await self._execute(
            "INSERT INTO distribution_history "
            "(optimal_provider, optimal_region, scores, data_size_gb, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (result["optimal_provider"], result["optimal_region"],
             json.dumps(result.get("scores", {}), default=str),
             float(result.get("data_size_gb", 0.0)), result["timestamp"]))

    async def save_project(self, project: Dict[str, Any]) -> None:
        await self._execute(
            "INSERT OR REPLACE INTO projects VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (project["project_id"], project["project_name"], project["company"],
             project["location_city"], project["location_country"],
             project["latitude"], project["longitude"],
             project["planned_power_capacity_mw"], project["status"],
             project["green_score"],
             project["sustainability"].get("pue_estimated", 0.0),
             project["sustainability"].get("renewable_share_pct", 0.0),
             json.dumps(project, default=str)))

    async def save_flexgen_decision(self, workload_id: str, node_id: str,
                                    policy_json: str, metrics_json: str,
                                    reward: float) -> None:
        await self._execute("""
            INSERT INTO flexgen_decisions
            (workload_id, node_id, policy_json, metrics_json, reward, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (workload_id, node_id, policy_json, metrics_json, reward,
              datetime.now(timezone.utc).isoformat()))

    async def save_metric_history(self, metric_name: str, value: float,
                                  metadata: Optional[Dict[str, Any]] = None) -> None:
        await self._execute(
            "INSERT INTO metric_history (metric_name, value, timestamp, metadata) "
            "VALUES (?, ?, ?, ?)",
            (metric_name, value, datetime.now(timezone.utc).isoformat(),
             json.dumps(metadata or {}, default=str)))

    async def get_metric_history(self, metric_name: str,
                                 limit: int = 100) -> List[Dict[str, Any]]:
        rows = await self._fetch_all(
            "SELECT metric_name, value, timestamp, metadata FROM metric_history "
            "WHERE metric_name=? ORDER BY id DESC LIMIT ?",
            (metric_name, limit))
        return [{
            "metric_name": r[0], "value": r[1], "timestamp": r[2],
            "metadata": json.loads(r[3]) if r[3] else {},
        } for r in rows]

    async def save_explanation(self, decision_id: str, explanation: str,
                               model_dump: Optional[Dict[str, Any]] = None) -> str:
        expl_id = str(uuid.uuid4())
        await self._execute(
            "INSERT INTO explanations (id, decision_id, explanation, model_dump, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (expl_id, decision_id, explanation,
             json.dumps(model_dump, default=str) if model_dump else None,
             datetime.now(timezone.utc).isoformat()))
        return expl_id

    async def get_explanation(self, decision_id: str) -> Optional[Dict[str, Any]]:
        row = await self._fetch_one(
            "SELECT id, decision_id, explanation, model_dump, timestamp "
            "FROM explanations WHERE decision_id=? ORDER BY timestamp DESC LIMIT 1",
            (decision_id,))
        if row is None:
            return None
        return {
            "id": row[0], "decision_id": row[1], "explanation": row[2],
            "model_dump": json.loads(row[3]) if row[3] else None,
            "timestamp": row[4],
        }

    async def save_human_review(self, decision_id: str, status: str = "pending",
                                feedback: Optional[str] = None,
                                reviewer: Optional[str] = None) -> int:
        cur = await self._execute(
            "INSERT INTO human_reviews (decision_id, status, feedback, reviewer, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (decision_id, status, feedback, reviewer,
             datetime.now(timezone.utc).isoformat()))
        return cur.lastrowid or 0

    async def update_human_review(self, review_id: int, status: str,
                                  feedback: Optional[str] = None) -> None:
        await self._execute(
            "UPDATE human_reviews SET status=?, feedback=?, reviewed_at=? WHERE id=?",
            (status, feedback, datetime.now(timezone.utc).isoformat(), review_id))

    async def get_pending_reviews(self) -> List[Dict[str, Any]]:
        rows = await self._fetch_all(
            "SELECT id, decision_id, status, feedback, reviewer, created_at "
            "FROM human_reviews WHERE status='pending' ORDER BY created_at DESC")
        return [{
            "id": r[0], "decision_id": r[1], "status": r[2],
            "feedback": r[3], "reviewer": r[4], "created_at": r[5],
        } for r in rows]

    async def save_safety_violation(self, rule_name: str,
                                    details: Dict[str, Any]) -> None:
        await self._execute(
            "INSERT INTO safety_violations (rule_name, details, timestamp) "
            "VALUES (?, ?, ?)",
            (rule_name, json.dumps(details, default=str),
             datetime.now(timezone.utc).isoformat()))

    async def save_chaos_experiment(self, experiment_type: str,
                                    target_component: str, status: str,
                                    result: Optional[Dict[str, Any]] = None) -> None:
        await self._execute(
            "INSERT INTO chaos_experiments "
            "(experiment_type, target_component, status, result, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (experiment_type, target_component, status,
             json.dumps(result, default=str) if result else None,
             datetime.now(timezone.utc).isoformat()))

    async def save_system_state(self, key: str, value: Any) -> None:
        await self._execute(
            "INSERT OR REPLACE INTO system_state (key, value, updated_at) "
            "VALUES (?, ?, ?)",
            (key, json.dumps(value, default=str),
             datetime.now(timezone.utc).isoformat()))

    async def get_system_state(self, key: str,
                               default: Optional[Any] = None) -> Optional[Any]:
        row = await self._fetch_one(
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
                self._initialized = False


# ============================================================================
# Post-quantum security (HMAC fallback with persisted salt)
# ============================================================================
class PostQuantumCrypto:
    SALT_KEY = "pqc_salt"

    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        self.master_key = config.get_master_key_bytes()
        self.salt: Optional[bytes] = None
        self._lock = asyncio.Lock()

    async def _load_or_create_salt(self) -> bytes:
        if self.salt is not None:
            return self.salt
        async with self._lock:
            if self.salt is not None:
                return self.salt
            stored = await self.db.get_system_state(self.SALT_KEY)
            if stored is None:
                new_salt = os.urandom(16)
                await self.db.save_system_state(self.SALT_KEY, new_salt.hex())
                self.salt = new_salt
            else:
                self.salt = bytes.fromhex(stored)
            return self.salt

    def _derive_key(self, salt: bytes, length: int = 32) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=length, salt=salt,
            iterations=100_000, backend=default_backend())
        return kdf.derive(self.master_key)

    async def _encrypt_key(self, key_bytes: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        return nonce + aesgcm.encrypt(nonce, key_bytes, None)

    async def _decrypt_key(self, encrypted: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        return aesgcm.decrypt(encrypted[:12], encrypted[12:], None)

    async def generate_keypair(self, algorithm: str = "hmac_sha256",
                               validity_days: int = 30) -> Dict[str, Any]:
        secret = secrets.token_bytes(32)
        public_key = hashlib.sha256(secret).digest()
        key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
        expires_at = (datetime.now(timezone.utc)
                      + timedelta(days=validity_days)).isoformat()
        enc_pub = await self._encrypt_key(public_key)
        enc_priv = await self._encrypt_key(secret)
        await self.db.save_pqc_keypair(key_id, algorithm, enc_pub, enc_priv, expires_at)
        return {
            "key_id": key_id, "algorithm": algorithm,
            "public_key": public_key.hex(), "expires_at": expires_at,
        }

    async def sign_data(self, data: Dict[str, Any], key_id: str) -> Dict[str, Any]:
        kp = await self.db.get_pqc_keypair(key_id)
        if kp is None:
            raise ValueError(f"Key {key_id} not found")
        secret = await self._decrypt_key(kp["private_key"])
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        signature = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        return {
            "signature": signature, "algorithm": kp["algorithm"],
            "key_id": key_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def verify_data(self, data: Dict[str, Any],
                          signature_data: Dict[str, Any]) -> bool:
        key_id = signature_data.get("key_id")
        if not key_id:
            return False
        kp = await self.db.get_pqc_keypair(key_id)
        if kp is None:
            return False
        secret = await self._decrypt_key(kp["private_key"])
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature_data.get("signature", ""))

    async def get_status(self) -> Dict[str, Any]:
        rows = await self.db._fetch_all("SELECT COUNT(*) FROM pqc_key_pairs")
        return {
            "algorithms": ["hmac_sha256", "ecdsa_compat"],
            "keypairs_count": int(rows[0][0]) if rows else 0,
        }


# ============================================================================
# Blockchain integration (deterministic simulation)
# ============================================================================
class BlockchainIntegration:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        self.web3_available = False
        self._rng = np.random.default_rng(
            getattr(config.general, "random_seed", 42))

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=2, max=10))
    async def tokenize_carbon_credit(self, amount_kg: float,
                                     project_id: str) -> Dict[str, Any]:
        tx_hash = "0x" + hashlib.sha256(
            f"{amount_kg}:{project_id}:{time.time()}".encode()).hexdigest()
        block_number = int(self._rng.integers(1_000_000, 2_000_000))
        await self.db.save_blockchain_transaction(
            tx_hash, "tokenize", amount_kg, project_id, "success")
        return {
            "status": "success", "tx_hash": tx_hash,
            "block_number": block_number, "simulated": not self.web3_available,
        }

    async def verify_helium_savings(self, liters: float,
                                    component_id: str) -> Dict[str, Any]:
        tx_hash = "0x" + hashlib.sha256(
            f"{liters}:{component_id}:{time.time()}".encode()).hexdigest()
        await self.db.save_blockchain_transaction(
            tx_hash, "helium_verify", liters, component_id, "success")
        return {"status": "success", "tx_hash": tx_hash, "verified": True}

    async def get_transaction_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = await self.db._fetch_all(
            "SELECT tx_hash, tx_type, amount, project_id, timestamp, status "
            "FROM blockchain_transactions ORDER BY id DESC LIMIT ?", (limit,))
        return [{
            "tx_hash": r[0], "tx_type": r[1], "amount": r[2],
            "project_id": r[3], "timestamp": r[4], "status": r[5],
        } for r in rows]

    async def get_status(self) -> Dict[str, Any]:
        return {
            "connected": self.web3_available,
            "rpc_url": self.config.blockchain.rpc_url,
            "simulated": not self.web3_available,
        }


# ============================================================================
# Analytics
# ============================================================================
class AdvancedPredictiveAnalytics:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db

    async def multi_horizon_forecast(self, data: Dict[str, Any],
                                     horizons: List[int]) -> Dict[str, Any]:
        series = data.get("series", [])
        if not series:
            raise ValueError("Missing 'series' in data")
        if len(series) < 2:
            mean_val = float(np.mean(series)) if series else 0.0
            return {"forecast": {h: mean_val for h in horizons}, "method": "naive_mean"}
        window = min(len(series), 10)
        x = np.arange(window)
        y = np.asarray(series[-window:], dtype=float)
        coeffs = np.polyfit(x, y, 1)
        last_x = window - 1
        forecast = {
            h: float(coeffs[0] * (last_x + h) + coeffs[1]) for h in horizons
        }
        return {"forecast": forecast, "method": "linear_regression"}

    async def detect_anomalies(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        anomalies: List[Dict[str, Any]] = []
        thresholds = {
            "carbon_intensity": 500.0,
            "energy_joules": 100_000.0,
            "latency_ms": 5_000.0,
        }
        for name, value in metrics.items():
            if isinstance(value, (int, float)) and name in thresholds:
                if value > thresholds[name]:
                    anomalies.append({
                        "metric": name, "value": value, "type": "high",
                        "severity": 0.7, "threshold": thresholds[name],
                    })
        return anomalies

    async def calculate_green_trend(self, projects: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not projects:
            return {"trend": "unknown", "slope": 0.0, "significance": 0.0}
        scores = [p.get("green_score", 0) for p in projects]
        if len(scores) < 2:
            return {"trend": "insufficient_data", "slope": 0.0, "significance": 0.0}
        x = np.arange(len(scores))
        y = np.asarray(scores, dtype=float)
        coeffs = np.polyfit(x, y, 1)
        slope = float(coeffs[0])
        corr = np.corrcoef(x, y)[0, 1]
        significance = float(abs(corr)) if not np.isnan(corr) else 0.0
        if slope > 0.05 and significance > 0.5:
            trend = "improving"
        elif slope < -0.05 and significance > 0.5:
            trend = "declining"
        else:
            trend = "stable"
        return {
            "trend": trend, "slope": slope,
            "significance": significance, "num_projects": len(projects),
        }


# ============================================================================
# Cloud distribution
# ============================================================================
class MultiCloudDistribution:
    def __init__(self, db: AsyncDatabaseManager) -> None:
        self.db = db
        self.providers = {
            "aws": {"green": 0.7, "latency": 50, "cost": 0.8},
            "azure": {"green": 0.65, "latency": 60, "cost": 0.75},
            "gcp": {"green": 0.8, "latency": 55, "cost": 0.85},
        }

    async def distribute_loader_data(self, data: Dict[str, Any],
                                     preferences: Optional[Dict[str, bool]] = None
                                     ) -> Dict[str, Any]:
        prefs = preferences or {"green": True, "cost": False, "performance": True}
        scores: Dict[str, float] = {}
        for provider, info in self.providers.items():
            score = 0.0
            if prefs.get("green"):
                score += info["green"] * 0.4
            if prefs.get("cost"):
                score += (1 - info["cost"]) * 0.3
            if prefs.get("performance"):
                score += (1 - info["latency"] / 100) * 0.3
            scores[provider] = score
        best = max(scores, key=scores.get)
        result = {
            "optimal_provider": best,
            "optimal_region": "global",
            "scores": scores,
            "data_size_gb": float(data.get("size_gb", 0.0)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.db.save_distribution(result)
        return result

    async def get_distribution_status(self) -> Dict[str, Any]:
        rows = await self.db._fetch_all(
            "SELECT optimal_provider, optimal_region, timestamp "
            "FROM distribution_history ORDER BY id DESC LIMIT 1")
        if rows:
            return {
                "active_provider": rows[0][0], "active_region": rows[0][1],
                "timestamp": rows[0][2],
            }
        return {
            "active_provider": "aws", "active_region": "us-east-1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ============================================================================
# Data lake (local fallback)
# ============================================================================
class DataLakeIntegration:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        self.local_dir = Path(os.environ.get("GREEN_AGENT_DATA_LAKE", "./data_lake"))

    async def store_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        self.local_dir.mkdir(parents=True, exist_ok=True)
        fname = (f"metrics_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
                 f"_{uuid.uuid4().hex[:8]}.json")
        path = self.local_dir / fname
        await asyncio.to_thread(
            path.write_text, json.dumps(metrics, default=str))
        return {"status": "success", "location": str(path), "simulated": True}

    async def query_data_warehouse(self, query: str) -> List[Dict[str, Any]]:
        if "metric_history" in query.lower():
            rows = await self.db._fetch_all(
                "SELECT metric_name, value, timestamp FROM metric_history LIMIT 10")
            return [{
                "metric_name": r[0], "value": r[1], "timestamp": r[2],
            } for r in rows]
        return []


# ============================================================================
# Monitoring
# ============================================================================
class RealTimeMonitoring:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        self.metrics_buffer: deque = deque(maxlen=1000)

    async def record_metric(self, metric: Dict[str, Any]) -> None:
        self.metrics_buffer.append(metric)
        if "name" in metric and "value" in metric:
            await self.db.save_metric_history(
                metric["name"], metric["value"], metric.get("metadata"))

    async def get_latest_metrics(self) -> List[Dict[str, Any]]:
        return list(self.metrics_buffer)


# ============================================================================
# MLOps
# ============================================================================
class MLOpsPipeline:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        self.models: Dict[str, Dict[str, Any]] = {}

    async def register_model(self, model_id: str, name: str, version: str,
                             metadata: Dict[str, Any]) -> None:
        await self.db.save_model_registry(model_id, name, version, metadata)
        self.models[model_id] = {
            "name": name, "version": version, "metadata": metadata,
        }

    async def get_model(self, model_id: str) -> Optional[Dict[str, Any]]:
        row = await self.db._fetch_one(
            "SELECT model_id, name, version, metadata FROM model_registry "
            "WHERE model_id=?", (model_id,))
        if row is None:
            return None
        return {
            "model_id": row[0], "name": row[1], "version": row[2],
            "metadata": json.loads(row[3]) if row[3] else {},
        }

    async def list_models(self) -> List[Dict[str, Any]]:
        rows = await self.db._fetch_all("SELECT model_id FROM model_registry")
        out: List[Dict[str, Any]] = []
        for r in rows:
            m = await self.get_model(r[0])
            if m is not None:
                out.append(m)
        return out


# ============================================================================
# Multi-region
# ============================================================================
class MultiRegionManager:
    REGIONS = ("us-east-1", "eu-west-1", "ap-southeast-1")

    async def get_optimal_region(self, criteria: Dict[str, Any]) -> str:
        latency = criteria.get("latency_ms")
        if latency is None:
            return self.REGIONS[0]
        if latency < 50:
            return "us-east-1"
        if latency < 100:
            return "eu-west-1"
        return "ap-southeast-1"


# ============================================================================
# Edge computing
# ============================================================================
class EdgeComputing:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        self.devices: Dict[str, Dict[str, Any]] = {}

    async def register_device(self, device_id: str,
                              config: Dict[str, Any]) -> None:
        await self.db.save_edge_device(device_id, config, "active")
        self.devices[device_id] = {
            "config": config, "status": "active",
            "last_seen": datetime.now(timezone.utc),
        }

    async def update_device_status(self, device_id: str, status: str) -> None:
        device = self.devices.get(device_id, {})
        await self.db.save_edge_device(
            device_id, device.get("config", {}), status)
        if device_id in self.devices:
            self.devices[device_id]["status"] = status

    async def get_device_status(self, device_id: str) -> Optional[Dict[str, Any]]:
        if device_id in self.devices:
            return self.devices[device_id]
        row = await self.db._fetch_one(
            "SELECT config, status, last_seen FROM edge_devices WHERE device_id=?",
            (device_id,))
        if row is None:
            return None
        return {
            "config": json.loads(row[0]) if row[0] else {},
            "status": row[1], "last_seen": row[2],
        }


# ============================================================================
# Sustainable NLP (rule-based fallback)
# ============================================================================
class SustainableNLP:
    def __init__(self, config: GreenAgentConfig) -> None:
        self.config = config
        self.model_name = getattr(config.nlp, "model_name", "distilgpt2")

    async def generate_sustainability_summary(self, metrics: Dict[str, Any]) -> str:
        parts: List[str] = []
        ci = metrics.get("carbon_intensity")
        if isinstance(ci, (int, float)):
            if ci < 200:
                parts.append("Carbon intensity is low.")
            elif ci < 400:
                parts.append("Carbon intensity is moderate.")
            else:
                parts.append("Carbon intensity is high.")
        e = metrics.get("energy_joules")
        if isinstance(e, (int, float)):
            parts.append(f"Energy usage: {e / 3.6e6:.2f} kWh.")
        rs = metrics.get("renewable_share")
        if isinstance(rs, (int, float)):
            parts.append(f"Renewable share: {rs * 100:.1f}%.")
        return " ".join(parts) if parts else "No significant sustainability metrics found."


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
            name = a["name"]
            self.q_values[name] = 0.0
            self.counts[name] = 0
            self.causal_effects[name] = 0.0

    def _register(self, name: str) -> None:
        self.q_values.setdefault(name, 0.0)
        self.counts.setdefault(name, 0)
        self.causal_effects.setdefault(name, 0.0)

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
        self.causal_effects[name] += (reward - self.causal_effects[name]) / self.counts[name]

    def seed_safe_policy(self, context: Any, policy: Dict[str, Any]) -> None:
        name = policy["name"]
        self._register(name)
        self.q_values[name] = max(self.q_values[name], 0.0)

    def append_action(self, action: Dict[str, Any]) -> None:
        if action not in self.actions:
            self.actions.append(action)
        self._register(action["name"])

    def export_state(self) -> Dict[str, Any]:
        return {
            "q_values": dict(self.q_values),
            "counts": dict(self.counts),
            "causal_effects": dict(self.causal_effects),
            "trials": self.trials,
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        for k in state.get("q_values", {}):
            self._register(k)
            self.q_values[k] = float(state["q_values"][k])
        for k in state.get("counts", {}):
            self._register(k)
            self.counts[k] = int(state["counts"][k])
        for k in state.get("causal_effects", {}):
            self._register(k)
            self.causal_effects[k] = float(state["causal_effects"][k])
        self.trials = int(state.get("trials", 0))


# ============================================================================
# Safety monitor
# ============================================================================
class SafetyMonitor:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager) -> None:
        self.config = config
        self.db = db
        safety_cfg = getattr(config, "safety", None)
        self.max_carbon = float(getattr(safety_cfg, "max_carbon_intensity", 500.0))
        self.min_renewable = float(getattr(safety_cfg, "min_renewable_share", 0.3))
        self.max_latency = float(getattr(safety_cfg, "max_latency_ms", 1000.0))
        self._last_violation: Dict[str, bool] = {}

    def rules(self) -> Dict[str, Callable[[Dict[str, Any]], bool]]:
        return {
            "max_carbon_intensity":
                lambda m: float(m.get("carbon_intensity", 0.0)) <= self.max_carbon,
            "min_renewable_share":
                lambda m: float(m.get("renewable_share", 1.0)) >= self.min_renewable,
            "max_latency_ms":
                lambda m: float(m.get("latency_ms", 0.0)) <= self.max_latency,
        }

    async def check(self, metrics: Dict[str, Any],
                    context: Optional[Dict[str, Any]] = None
                    ) -> List[Dict[str, Any]]:
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
            # Only persist on a clean -> violation transition.
            if not self._last_violation.get(name, False):
                try:
                    await self.db.save_safety_violation(name, metrics)
                    SAFETY_VIOLATIONS.labels(rule=name).inc()
                except Exception as e:
                    logger.warning("Failed to persist safety violation: %s", e)
                self._last_violation[name] = True
        return violations


# ============================================================================
# Chaos monkey
# ============================================================================
class ChaosMonkey:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager,
                 system: "GreenAgentSystem") -> None:
        self.config = config
        self.db = db
        self.system = system
        chaos = getattr(config, "chaos", None)
        self.enabled = bool(getattr(chaos, "enabled", False))
        self.failure_probability = float(getattr(chaos, "failure_probability", 0.1))
        self.interval = int(getattr(chaos, "experiment_interval_seconds", 120))
        self._rng = np.random.default_rng(
            getattr(config.general, "random_seed", 42))
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
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self._inject_failure()
            except Exception as e:
                logger.error("Chaos experiment failed: %s", e)

    async def _inject_failure(self) -> None:
        failure_type = str(self._rng.choice(["latency", "error", "disconnect"]))
        target = str(self._rng.choice(
            ["blockchain", "database", "analytics", "flexgen"]))
        status = "success"
        result: Dict[str, Any] = {}
        try:
            if failure_type == "latency":
                delay = float(self._rng.uniform(0.05, 0.2))
                await asyncio.sleep(delay)
                result["delay_seconds"] = delay
            elif failure_type == "error":
                if target == "blockchain":
                    original = self.system.blockchain.web3_available
                    self.system.blockchain.web3_available = False
                    await asyncio.sleep(0.05)
                    self.system.blockchain.web3_available = original
                    result["action"] = "toggled blockchain availability"
                else:
                    result["action"] = "noop fault injection"
            else:
                await asyncio.sleep(0.05)
                result["action"] = "simulated network partition"
        except Exception as e:
            status = "failed"
            result["error"] = str(e)
        await self.db.save_chaos_experiment(failure_type, target, status, result)
        CHAOS_EXPERIMENTS.labels(type=failure_type, status=status).inc()


# ============================================================================
# Autonomous optimizer
# ============================================================================
class EnhancedAutonomousOptimizer:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager,
                 event_bus: EventBus, safety_monitor: SafetyMonitor) -> None:
        self.config = config
        self.db = db
        self.event_bus = event_bus
        self.safety_monitor = safety_monitor
        self.general = config.general
        self.analytics = config.analytics
        self._rng = np.random.default_rng(
            getattr(self.general, "random_seed", 42))

        self.action_space = [
            {"name": "performance", "params": {"focus": "throughput", "precision": "fp32"}},
            {"name": "carbon", "params": {"focus": "low_carbon", "precision": "int8"}},
            {"name": "cost", "params": {"focus": "min_cost", "precision": "int8"}},
            {"name": "hybrid", "params": {"focus": "balance", "precision": "fp16"}},
            {"name": "adaptive", "params": {"focus": "auto", "precision": "fp16"}},
        ]

        def fallback(_ctx: Any) -> Dict[str, Any]:
            return {"name": "hybrid", "params": {"focus": "balance", "precision": "fp16"}}

        self.bandit = CausalBandit(
            action_space=self.action_space,
            fallback_solver=fallback,
            min_trials_before_bandit=self.general.bandit_min_trials,
            confidence_threshold=self.general.bandit_confidence_threshold,
            seed=getattr(self.general, "random_seed", 42),
        )
        self.recent_rewards: deque = deque(maxlen=100)

    async def load_state(self) -> None:
        try:
            state = await self.db.get_system_state("bandit_state")
            if state:
                self.bandit.import_state(state)
        except Exception as e:
            logger.warning("Failed to load bandit state: %s", e)

    async def save_state(self) -> None:
        try:
            await self.db.save_system_state("bandit_state", self.bandit.export_state())
        except Exception as e:
            logger.warning("Failed to save bandit state: %s", e)

    # ------------------------------------------------------------------
    async def optimize(self, current_state: Dict[str, Any],
                       metrics: Dict[str, Any]) -> Dict[str, Any]:
        context = {
            "state": current_state, "metrics": metrics,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        policy, confidence, source = self.bandit.select_action(context)

        # Safety gate.
        violations = await self.safety_monitor.check(metrics)
        if violations:
            policy = {"name": "carbon", "params": {"focus": "low_carbon", "precision": "int8"}}
            source = "safety_override"
            confidence = 1.0

        objectives = {
            "energy": float(metrics.get("energy_joules", 0.0)) / 1000.0,
            "carbon": float(metrics.get("carbon_kg", 0.0)) / 10.0,
            "latency": float(metrics.get("latency_ms", 0.0)) / 1000.0,
            "accuracy": float(metrics.get("accuracy", 0.0)),
        }
        weights = getattr(self.analytics, "modp_weights",
                          {"energy": 0.25, "carbon": 0.25,
                           "latency": 0.20, "accuracy": 0.30})
        utility = float(sum(objectives[k] * weights.get(k, 1.0) for k in objectives))

        result = {
            "action": f"{policy['name']}_optimization",
            "selected_strategy": policy["name"],
            "confidence": float(confidence),
            "source": source,
            "utility": utility,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.db.save_optimisation(policy["name"], result)
        OPTIMIZER_DECISIONS.labels(strategy=policy["name"], source=source).inc()

        # Reward update when the strategy was chosen by the bandit.
        if source in ("causal", "bandit"):
            normalized = max(0.0, min(1.0, utility))
            self.bandit.update(context, policy, normalized)
            self.recent_rewards.append(normalized)

        # Bio-inspired expansion under sustained under-performance.
        if (len(self.recent_rewards) > 20
                and float(np.mean(self.recent_rewards)) < 0.3):
            self._expand_action_space()

        # XAI + persistence.
        decision_id = str(uuid.uuid4())
        explanation = self._generate_explanation(policy, confidence, utility)
        await self.db.save_explanation(
            decision_id, explanation,
            {"policy": policy, "context": context})
        result["explanation_id"] = decision_id
        result["explanation"] = explanation

        # Human-in-the-loop flag.
        needs_review = (
            confidence < float(self.general.human_review_threshold)
            or source == "safety_override"
        )
        if needs_review:
            review_id = await self.db.save_human_review(
                decision_id, status="pending", feedback=explanation)
            result["review_required"] = True
            result["review_id"] = review_id
        else:
            result["review_required"] = False

        await self.save_state()
        await self._emit_feedback_event(decision_id, policy, confidence,
                                        source, utility)

        return result

    # ------------------------------------------------------------------
    def _expand_action_space(self) -> None:
        # Deterministic expansion — adds a "green_burst" alternative.
        candidate = {
            "name": "green_burst",
            "params": {"focus": "renewable_windows", "precision": "int8"},
        }
        if any(a["name"] == candidate["name"] for a in self.action_space):
            return
        self.action_space.append(candidate)
        self.bandit.append_action(candidate)
        logger.info("Bio expansion added strategy: %s", candidate["name"])

    def _generate_explanation(self, policy: Dict[str, Any], confidence: float,
                              utility: float) -> str:
        focus = policy["params"].get("focus", "balanced")
        precision = policy["params"].get("precision", "unknown")
        return (
            f"Selected '{policy['name']}' strategy "
            f"(focus={focus}, precision={precision}); "
            f"confidence={confidence:.2f}, utility={utility:.2f}."
        )

    async def _emit_feedback_event(self, decision_id: str, policy: Dict[str, Any],
                                   confidence: float, source: str,
                                   utility: float) -> None:
        if FeedbackEvent is None:
            return
        try:
            ev = FeedbackEvent(
                source="green_agent_optimizer",
                feedback_type="routing",
                context={"task_id": decision_id, "teacher_ids": []},
                action={
                    "selected_action": policy["name"],
                    "selected_rank": 1,
                    "confidence_score": float(confidence),
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, utility))),
                    "latency_ms": 0.0, "energy_joules": 0.0,
                    "carbon_g": 0.0, "helium_cost": 0.0, "duration_ms": 0.0,
                },
                adaptive_cost_value=float(utility),
                tags=["optimizer", policy["name"], source],
                envelope={"trace_id": decision_id},
                metadata={"source": source},
            )
            logger.debug("FeedbackEvent emitted: %s",
                         getattr(ev, "event_id", "?"))
        except Exception as e:
            logger.warning("Failed to emit FeedbackEvent: %s", e)

    # ------------------------------------------------------------------
    async def get_optimization_stats(self) -> Dict[str, Any]:
        history = await self.db.get_recent_optimisations(1000)
        return {
            "total_optimizations": len(history),
            "strategies": [s["name"] for s in self.action_space],
            "recent": history[:5],
            "bandit_state": self.bandit.export_state(),
        }


# ============================================================================
# Geospatial / financial / environmental
# ============================================================================
class GeospatialIntelligence:
    LOCATIONS = [
        {"name": "Iceland", "renewable": 1.0, "latency": 30, "cost": 0.9},
        {"name": "Norway", "renewable": 0.98, "latency": 40, "cost": 0.8},
        {"name": "US Pacific Northwest", "renewable": 0.7, "latency": 60, "cost": 0.7},
        {"name": "Singapore", "renewable": 0.2, "latency": 20, "cost": 0.6},
        {"name": "Germany", "renewable": 0.5, "latency": 50, "cost": 0.75},
    ]

    async def find_optimal_locations(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        scored = []
        for loc in self.LOCATIONS:
            score = 0.0
            if criteria.get("renewable_energy"):
                score += loc["renewable"] * 0.5
            if criteria.get("low_latency"):
                score += (1 - loc["latency"] / 100) * 0.3
            if criteria.get("cost_budget"):
                score += (1 - loc["cost"]) * 0.2
            scored.append({"location": loc["name"], "score": score, "details": loc})
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored


class FinancialModeler:
    async def calculate_roi(self, project: Dict[str, Any],
                            timeframe_years: int = 10,
                            discount_rate: float = 0.05) -> Dict[str, Any]:
        capex = float(project.get("capex_usd", 0.0))
        opex = float(project.get("opex_usd_per_year", 0.0))
        revenue = float(project.get("revenue_usd_per_year", 0.0))
        net_cash = revenue - opex
        npv = -capex + sum(net_cash / ((1 + discount_rate) ** t)
                           for t in range(1, timeframe_years + 1))
        total_cost = capex + opex * timeframe_years
        total_revenue = revenue * timeframe_years
        roi = (total_revenue - total_cost) / total_cost if total_cost > 0 else 0.0
        payback = capex / net_cash if net_cash > 0 else None
        return {
            "roi": roi, "npv": npv,
            "total_cost": total_cost, "total_revenue": total_revenue,
            "payback_period_years": payback,
            "timeframe_years": timeframe_years,
            "discount_rate": discount_rate,
        }


class EnvironmentalImpactAnalyzer:
    async def calculate_lifecycle_emissions(self, project: Dict[str, Any]) -> Dict[str, Any]:
        capacity_mw = float(project.get("capacity_mw", 0.0))
        lifetime = int(project.get("lifetime_years", 25))
        emission_factor = float(project.get("emission_factor", 0.4))
        annual_mwh = capacity_mw * 0.3 * 8760
        total_mwh = annual_mwh * lifetime
        emissions_tco2 = total_mwh * emission_factor
        return {
            "emissions_tco2": emissions_tco2,
            "energy_per_year_mwh": annual_mwh,
            "lifetime_years": lifetime,
            "emission_factor": emission_factor,
        }


# ============================================================================
# API gateway
# ============================================================================
if FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE:
    from pydantic import BaseModel as _BM

    class _ForecastBody(_BM):
        series: List[float]
        horizons: List[int] = [1, 3, 6]

    class _OptimizeBody(_BM):
        state: Dict[str, Any] = {}
        metrics: Dict[str, Any] = {}

    class _TokenizeBody(_BM):
        amount_kg: float
        project_id: str

    class _SignBody(_BM):
        data: Dict[str, Any]
        key_id: str

    class _VerifyBody(_BM):
        data: Dict[str, Any]
        signature_data: Dict[str, Any]

    class _DistributeBody(_BM):
        data: Dict[str, Any]
        preferences: Optional[Dict[str, bool]] = None

    class _NLPBody(_BM):
        metrics: Dict[str, Any]

    class _ROIBody(_BM):
        project: Dict[str, Any]
        timeframe_years: int = 10

    class _ImpactBody(_BM):
        project: Dict[str, Any]

    class _ParetoBody(_BM):
        objectives: Dict[str, float]
        weights: Optional[Dict[str, float]] = None

    class _TokenBody(_BM):
        subject: str
        roles: List[str] = ["user"]

    class _ReviewActionBody(_BM):
        feedback: Optional[str] = None


class APIGateway:
    def __init__(self, config: GreenAgentConfig, system: "GreenAgentSystem") -> None:
        self.config = config
        self.system = system
        self.rate_limiter = EnhancedRateLimiter(
            config.general.rate_limit_requests, config.general.rate_limit_window)
        self.fastapi_app: Optional[Any] = None
        if FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE:
            self._init_fastapi()

    # ------------------------------------------------------------------
    def _init_fastapi(self) -> None:
        app = FastAPI(title="Green Agent API", version=__version__)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self.fastapi_app = app

        security = HTTPBearer(auto_error=False)

        async def _get_user(
            credentials: HTTPAuthorizationCredentials = Depends(security),
        ) -> Dict[str, Any]:
            if credentials is None:
                raise HTTPException(status_code=401, detail="Missing token")
            if not JOSE_AVAILABLE:
                raise HTTPException(status_code=503, detail="JWT not available")
            try:
                payload = jwt.decode(
                    credentials.credentials,
                    self.config.api.jwt_secret,
                    algorithms=[self.config.api.jwt_algorithm],
                )
            except JWTError:
                raise HTTPException(status_code=401, detail="Invalid token")
            roles = payload.get("roles") or []
            if isinstance(roles, str):
                roles = roles.split()
            return {"sub": payload.get("sub"), "roles": list(roles)}

        async def _require_admin(user: Dict = Depends(_get_user)) -> Dict:
            if "admin" not in (user.get("roles") or []):
                raise HTTPException(status_code=403, detail="Admin role required")
            return user

        @app.middleware("http")
        async def _rate_limit_middleware(request: Request, call_next):
            if not await self.rate_limiter.acquire():
                API_REQUESTS.labels(
                    endpoint=request.url.path, method=request.method,
                    status="429").inc()
                return JSONResponse(
                    status_code=429, content={"error": "Rate limit exceeded"})
            response = await call_next(request)
            API_REQUESTS.labels(
                endpoint=request.url.path, method=request.method,
                status=str(response.status_code)).inc()
            return response

        @app.get("/health")
        async def health() -> Dict[str, Any]:
            return await self.system.health_check()

        @app.get("/metrics")
        async def metrics():
            if PROMETHEUS_AVAILABLE and REGISTRY is not None:
                return Response(content=generate_latest(REGISTRY),
                                media_type=CONTENT_TYPE_LATEST)
            return PlainTextResponse("Prometheus not installed")

        @app.post("/token")
        async def issue_token(body: _TokenBody) -> Dict[str, Any]:
            if not JOSE_AVAILABLE:
                raise HTTPException(status_code=503, detail="JWT not available")
            now = datetime.now(timezone.utc)
            exp = now + timedelta(minutes=self.config.api.token_expiry_minutes)
            payload = {
                "sub": body.subject,
                "roles": body.roles,
                "iat": int(now.timestamp()),
                "exp": int(exp.timestamp()),
            }
            token = jwt.encode(payload, self.config.api.jwt_secret,
                              algorithm=self.config.api.jwt_algorithm)
            return {"token": token, "expires_at": exp.isoformat()}

        @app.post("/tokenize_carbon")
        async def tokenize_carbon(body: _TokenizeBody,
                                  user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.blockchain.tokenize_carbon_credit(
                body.amount_kg, body.project_id)

        @app.post("/forecast")
        async def forecast(body: _ForecastBody,
                           user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.analytics.multi_horizon_forecast(
                {"series": body.series}, body.horizons)

        @app.post("/optimize")
        async def optimize(body: _OptimizeBody,
                           user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.autonomous_optimizer.optimize(
                body.state, body.metrics)

        @app.get("/optimizer/stats")
        async def optimizer_stats(user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.autonomous_optimizer.get_optimization_stats()

        @app.post("/pqc/sign")
        async def pqc_sign(body: _SignBody,
                           user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.pqc.sign_data(body.data, body.key_id)

        @app.post("/pqc/verify")
        async def pqc_verify(body: _VerifyBody,
                             user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            valid = await self.system.pqc.verify_data(body.data, body.signature_data)
            return {"valid": valid}

        @app.post("/cloud/distribute")
        async def cloud_distribute(body: _DistributeBody,
                                   user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.cloud_distributor.distribute_loader_data(
                body.data, body.preferences)

        @app.get("/human-review/pending")
        async def pending_reviews(user: Dict = Depends(_require_admin)) -> Dict[str, Any]:
            return {"pending_reviews": await self.system.db.get_pending_reviews()}

        @app.post("/human-review/{review_id}/approve")
        async def approve_review(review_id: int, body: _ReviewActionBody,
                                 user: Dict = Depends(_require_admin)) -> Dict[str, Any]:
            await self.system.db.update_human_review(
                review_id, "approved", body.feedback)
            return {"status": "approved", "review_id": review_id}

        @app.post("/human-review/{review_id}/reject")
        async def reject_review(review_id: int, body: _ReviewActionBody,
                                user: Dict = Depends(_require_admin)) -> Dict[str, Any]:
            await self.system.db.update_human_review(
                review_id, "rejected", body.feedback)
            return {"status": "rejected", "review_id": review_id}

        @app.post("/chaos/trigger")
        async def trigger_chaos(user: Dict = Depends(_require_admin)) -> Dict[str, Any]:
            if not self.system.chaos_monkey:
                raise HTTPException(status_code=400, detail="Chaos Monkey disabled")
            await self.system.chaos_monkey._inject_failure()
            return {"status": "chaos experiment triggered"}

        @app.get("/geo/optimal")
        async def geo_optimal(user: Dict = Depends(_get_user)) -> List[Dict[str, Any]]:
            return await self.system.geo_intelligence.find_optimal_locations({})

        @app.post("/financial/roi")
        async def roi(body: _ROIBody,
                      user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.financial_modeler.calculate_roi(
                body.project, body.timeframe_years)

        @app.post("/environmental/impact")
        async def impact(body: _ImpactBody,
                         user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return await self.system.environmental_analyzer.calculate_lifecycle_emissions(
                body.project)

        @app.post("/nlp/summary")
        async def nlp_summary(body: _NLPBody,
                              user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            return {"summary": await self.system.nlp.generate_sustainability_summary(
                body.metrics)}

        @app.post("/optimization/pareto")
        async def pareto(body: _ParetoBody,
                         user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            weights = body.weights or self.system.config.analytics.modp_weights
            utility = float(sum(body.objectives[k] * weights.get(k, 1.0)
                                for k in body.objectives))
            return {"utility": utility}

        @app.get("/optimization/explain/{decision_id}")
        async def explain(decision_id: str,
                          user: Dict = Depends(_get_user)) -> Dict[str, Any]:
            expl = await self.system.db.get_explanation(decision_id)
            if expl is None:
                raise HTTPException(status_code=404, detail="Explanation not found")
            return expl


# ============================================================================
# Main orchestrator
# ============================================================================
class GreenAgentSystem:
    def __init__(self, config: GreenAgentConfig) -> None:
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]
        self._running = False
        self._shutdown_event = asyncio.Event()
        self.background_tasks: Set[asyncio.Task] = set()
        self.event_bus = EventBus()

        # Core services
        self.db = AsyncDatabaseManager(config)
        self.pqc = PostQuantumCrypto(config, self.db)
        self.blockchain = BlockchainIntegration(config, self.db)
        self.analytics = AdvancedPredictiveAnalytics(config, self.db)
        self.cloud_distributor = MultiCloudDistribution(self.db)
        self.data_lake = DataLakeIntegration(config, self.db)
        self.monitoring = RealTimeMonitoring(config, self.db)
        self.mlops = MLOpsPipeline(config, self.db)
        self.multi_region = MultiRegionManager()
        self.edge = EdgeComputing(config, self.db)
        self.nlp = SustainableNLP(config)

        # Safety, chaos, optimizer
        self.safety_monitor = SafetyMonitor(config, self.db)
        self.chaos_monkey = ChaosMonkey(config, self.db, self)
        self.autonomous_optimizer = EnhancedAutonomousOptimizer(
            config, self.db, self.event_bus, self.safety_monitor)

        # Analytics-adjacent
        self.geo_intelligence = GeospatialIntelligence()
        self.financial_modeler = FinancialModeler()
        self.environmental_analyzer = EnvironmentalImpactAnalyzer()

        # API gateway
        self.api_gateway = APIGateway(config, self)

        # Aggregated views
        self.components = {
            "pqc": self.pqc,
            "blockchain": self.blockchain,
            "analytics": self.analytics,
            "cloud": self.cloud_distributor,
            "data_lake": self.data_lake,
            "monitoring": self.monitoring,
            "mlops": self.mlops,
            "multi_region": self.multi_region,
            "edge": self.edge,
            "nlp": self.nlp,
            "optimizer": self.autonomous_optimizer,
            "safety_monitor": self.safety_monitor,
        }

        logger.info("GreenAgentSystem initialized (id=%s)", self.instance_id)

    # ------------------------------------------------------------------
    async def start(self) -> None:
        self.config.ensure_master_key()
        self._running = True
        await self.autonomous_optimizer.load_state()
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.start()
        self.background_tasks.add(
            asyncio.create_task(self._health_check_loop()))
        self.background_tasks.add(
            asyncio.create_task(self._monitoring_loop()))
        for t in list(self.background_tasks):
            t.add_done_callback(self.background_tasks.discard)
        if self.api_gateway.fastapi_app is not None:
            try:
                import uvicorn  # type: ignore
                config = uvicorn.Config(
                    self.api_gateway.fastapi_app,
                    host=self.config.api.host,
                    port=self.config.api.port,
                    log_level="info",
                    server_header=False,
                )
                server = uvicorn.Server(config)
                self._fastapi_task = asyncio.create_task(server.serve())
                logger.info("FastAPI server started on %s:%d",
                            self.config.api.host, self.config.api.port)
            except Exception as e:
                logger.warning("Failed to start FastAPI server: %s", e)
        logger.info("GreenAgentSystem started")

    async def _health_check_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.labels(component="system").set(health["health_score"])
            except Exception as e:
                logger.error("Health check error: %s", e)
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass

    async def _monitoring_loop(self) -> None:
        rng = np.random.default_rng(self.config.general.random_seed)
        while not self._shutdown_event.is_set():
            try:
                carbon = float(rng.uniform(200.0, 500.0))
                await self.monitoring.record_metric(
                    {"name": "carbon_intensity", "value": carbon})
                CARBON_INTENSITY.set(carbon)
            except Exception as e:
                logger.error("Monitoring loop error: %s", e)
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=300.0)
                break
            except asyncio.TimeoutError:
                pass

    async def health_check(self) -> Dict[str, Any]:
        statuses: Dict[str, Any] = {}
        penalties = 0
        for name, comp in self.components.items():
            try:
                if hasattr(comp, "get_status"):
                    status = await comp.get_status()
                    statuses[name] = status
                    if isinstance(status, dict) and status.get("connected") is False:
                        penalties += 10
            except Exception as e:
                statuses[name] = {"error": str(e)}
                penalties += 20
        score = max(0, 100 - penalties)
        return {
            "healthy": score > 50,
            "instance_id": self.instance_id,
            "version": __version__,
            "health_score": score,
            "components": statuses,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def shutdown(self) -> None:
        logger.info("Shutting down (id=%s)", self.instance_id)
        self._shutdown_event.set()
        self._running = False
        if self.chaos_monkey:
            try:
                await self.chaos_monkey.stop()
            except Exception:
                pass
        if hasattr(self, "_fastapi_task"):
            self._fastapi_task.cancel()
            try:
                await self._fastapi_task
            except (asyncio.CancelledError, Exception):
                pass
        for t in list(self.background_tasks):
            if not t.done():
                t.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        await self.autonomous_optimizer.save_state()
        await self.db.close()
        logger.info("Shutdown complete")


# ============================================================================
# Tests
# ============================================================================
async def _test_circuit_breaker_sync_async() -> None:
    cb = EnhancedCircuitBreaker("test", failure_threshold=2, recovery_timeout=0.1)

    def sync_ok(): return 42

    async def async_ok(): return 43

    assert await cb.call(sync_ok) == 42
    assert await cb.call(async_ok) == 43

    def boom(): raise RuntimeError("fail")

    for _ in range(2):
        try:
            await cb.call(boom)
        except RuntimeError:
            pass
    assert cb.state is CircuitBreakerState.OPEN
    try:
        await cb.call(sync_ok)
    except CircuitBreakerOpenError:
        pass
    else:
        raise AssertionError("open breaker should raise")
    await asyncio.sleep(0.15)
    assert await cb.call(sync_ok) == 42
    assert cb.state is CircuitBreakerState.CLOSED


async def _test_pqc_roundtrip(tmpdir: Path) -> None:
    cfg = GreenAgentConfig()
    cfg.ensure_master_key()
    cfg.database = DatabaseConfig(path=str(tmpdir / "pqc.db"))
    db = AsyncDatabaseManager(cfg)
    await db._connect()
    pqc = PostQuantumCrypto(cfg, db)
    kp = await pqc.generate_keypair()
    data = {"x": 1}
    sig = await pqc.sign_data(data, kp["key_id"])
    assert await pqc.verify_data(data, sig) is True
    assert await pqc.verify_data({"x": 2}, sig) is False
    await db.close()


async def _test_pqc_salt_persistence(tmpdir: Path) -> None:
    cfg = GreenAgentConfig()
    cfg.ensure_master_key()
    cfg.database = DatabaseConfig(path=str(tmpdir / "salt.db"))
    db = AsyncDatabaseManager(cfg)
    await db._connect()
    pqc1 = PostQuantumCrypto(cfg, db)
    kp = await pqc1.generate_keypair()
    sig = await pqc1.sign_data({"x": 1}, kp["key_id"])
    await db.close()

    # New process / new connection / new PQC instance.
    db2 = AsyncDatabaseManager(cfg)
    await db2._connect()
    pqc2 = PostQuantumCrypto(cfg, db2)
    assert await pqc2.verify_data({"x": 1}, sig) is True
    await db2.close()


async def _test_bandit_handles_new_policies() -> None:
    bandit = CausalBandit(
        action_space=[{"name": "a", "params": {}}],
        fallback_solver=lambda ctx: {"name": "a", "params": {}},
        min_trials_before_bandit=1,
        seed=0,
    )
    bandit.update({}, {"name": "a", "params": {}}, 0.5)
    bandit.update({}, {"name": "brand_new", "params": {}}, 0.9)
    assert bandit.q_values["brand_new"] > 0.0


async def _test_safety_monitor_transitions(tmpdir: Path) -> None:
    cfg = GreenAgentConfig()
    cfg.database = DatabaseConfig(path=str(tmpdir / "safety.db"))
    db = AsyncDatabaseManager(cfg)
    await db._connect()
    sm = SafetyMonitor(cfg, db)
    v1 = await sm.check({"carbon_intensity": 900.0})
    assert v1 and v1[0]["rule"] == "max_carbon_intensity"
    # Second call is a violation but does not re-persist.
    v2 = await sm.check({"carbon_intensity": 900.0})
    assert v2
    # Clean state resets.
    v3 = await sm.check({"carbon_intensity": 100.0})
    assert v3 == []
    await db.close()


async def _test_optimizer_and_feedback(tmpdir: Path) -> None:
    cfg = GreenAgentConfig()
    cfg.ensure_master_key()
    cfg.database = DatabaseConfig(path=str(tmpdir / "optimizer.db"))
    db = AsyncDatabaseManager(cfg)
    await db._connect()
    bus = EventBus()
    safety = SafetyMonitor(cfg, db)
    opt = EnhancedAutonomousOptimizer(cfg, db, bus, safety)
    result = await opt.optimize({}, {"energy_joules": 1000.0, "accuracy": 0.9})
    assert "selected_strategy" in result
    assert "explanation_id" in result
    stats = await opt.get_optimization_stats()
    assert stats["total_optimizations"] >= 1
    await db.close()


async def _test_migrations_and_state(tmpdir: Path) -> None:
    cfg = GreenAgentConfig()
    cfg.database = DatabaseConfig(path=str(tmpdir / "migrate.db"))
    db = AsyncDatabaseManager(cfg)
    await db._connect()
    # Fresh DB should be at schema v5.
    row = await db._fetch_one(
        "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
    assert row is not None and int(row[0]) == 5
    await db.close()


async def _test_system_startup(tmpdir: Path) -> None:
    cfg = GreenAgentConfig()
    cfg.database = DatabaseConfig(path=str(tmpdir / "system.db"))
    cfg.chaos = ChaosConfig(enabled=False)
    # Silence API startup during tests.
    cfg.api.host = "127.0.0.1"
    cfg.api.port = 0
    cfg.ensure_master_key()
    system = GreenAgentSystem(cfg)
    # Skip API startup path by clearing the fastapi_app temporarily.
    saved_app = system.api_gateway.fastapi_app
    system.api_gateway.fastapi_app = None
    try:
        await system.start()
        assert system._running
        health = await system.health_check()
        assert "health_score" in health
    finally:
        system.api_gateway.fastapi_app = saved_app
        await system.shutdown()


def run_tests() -> None:
    import tempfile
    loop = asyncio.new_event_loop()
    try:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            loop.run_until_complete(_test_circuit_breaker_sync_async())
            loop.run_until_complete(_test_pqc_roundtrip(tmp))
            loop.run_until_complete(_test_pqc_salt_persistence(tmp))
            loop.run_until_complete(_test_bandit_handles_new_policies())
            loop.run_until_complete(_test_safety_monitor_transitions(tmp))
            loop.run_until_complete(_test_optimizer_and_feedback(tmp))
            loop.run_until_complete(_test_migrations_and_state(tmp))
            loop.run_until_complete(_test_system_startup(tmp))
    finally:
        loop.close()
    print("All Green Agent v15.0.0 tests passed.")


# ============================================================================
# Entry point
# ============================================================================
async def main() -> None:
    parser = argparse.ArgumentParser(description="Green Agent v15.0.0")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    logging.basicConfig(
        level=os.environ.get("GREEN_AGENT_LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    config = GreenAgentConfig()
    config.ensure_master_key()

    print("=" * 80)
    print(f"Green Agent Base Classes v{__version__}")
    print("=" * 80)

    system = GreenAgentSystem(config)
    await system.start()
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await system.shutdown()
        print("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
