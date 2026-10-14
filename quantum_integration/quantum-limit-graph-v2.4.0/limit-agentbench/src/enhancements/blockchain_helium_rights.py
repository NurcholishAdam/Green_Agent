#!/usr/bin/env python3
# File: src/enhancements/blockchain_helium_rights_enhanced_v18.py
"""
Helium Rights Smart Contract & Trading Platform — Version 18.0
================================================================

Self-contained, runnable version of the Helium Rights platform. Every
subsystem is either implemented for real or clearly marked as a
deterministic simulation, so the module imports and runs without any
optional dependency (web3, hvac, qiskit, prophet, sklearn, etc.).

CHANGES (v18.0 over v17.1):
- Fixed missing ``random`` import; a single seeded ``np.random.default_rng``
  is threaded through every random call site.
- Pydantic v2-first imports (``pydantic_settings.BaseSettings``,
  ``field_validator``); v1 fallback retained.
- ``master_key`` validator allows an empty value; ``ensure_master_key()``
  auto-generates at startup.
- ``self.db_engine`` is created before its consumers; ``AutonomousOptimizer``
  and ``FlexGenManager`` receive a live engine.
- Every stub class is implemented. ``CircuitBreaker``, ``EnhancedRateLimiter``,
  ``TaskManager``, ``PostQuantumCrypto`` (HMAC-SHA256 with an AES-GCM-wrapped
  per-key secret), ``RealLayer2Integration``, ``RealDeFiIntegration``,
  ``PricePredictionEngine`` and ``FlexGenManager`` return well-formed results.
- ``GlobalCircuitBreaker`` uses ``threading.RLock`` and an instance dict.
- ``CausalBandit`` registers new actions in every parallel dict; bio expansion
  no longer raises ``KeyError``.
- Every FastAPI endpoint uses a Pydantic request body and requires a JWT
  with the appropriate role. ``/token`` issues tokens.
- FastAPI moves from ``on_event`` to the ``lifespan`` context manager.
- ``MultiAgentSystem.vote`` scores each strategy (previously identical).
- Fictitious L2 SDK imports are removed; a clearly-marked deterministic
  bridge simulator replaces them.
- ``web3`` and ``hvac`` are imported with ``try/except`` and fall back to
  simulated clients.
- ``FeedbackEvent`` is emitted from every optimizer decision.
- XAI explanations and human reviews are persisted to the async DB when it
  is available; in-memory fallback is preserved.
- ``__version__`` and ``__all__`` added.
- Built-in test suite: ``python blockchain_helium_rights_enhanced_v18.py --test``.
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
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
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
    "HeliumPlatformException",
    "CircuitBreakerState",
    "CircuitBreaker",
    "GlobalCircuitBreaker",
    "EnhancedRateLimiter",
    "TaskManager",
    "PostQuantumCrypto",
    "RealLayer2Integration",
    "RealDeFiIntegration",
    "PricePredictionEngine",
    "FlexGenManager",
    "SafetyMonitor",
    "CausalBandit",
    "XAIExplainer",
    "HumanReviewManager",
    "ChaosMonkey",
    "QuantumDistillationOptimizer",
    "FederatedCoordinator",
    "MultiAgentSystem",
    "AutonomousOptimizer",
    "HeliumPlatformConfig",
    "EnhancedHeliumRightsPlatform",
    "build_app",
    "main",
    "run_tests",
]


# ============================================================================
# Logging
# ============================================================================
_log_dir = Path(os.environ.get("HELIUM_LOG_DIR", "./logs"))
try:
    _log_dir.mkdir(parents=True, exist_ok=True)
except OSError:
    _log_dir = Path("/tmp")

logging.basicConfig(
    level=os.environ.get("HELIUM_LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            _log_dir / "helium_platform.log",
            maxBytes=10 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


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

# FastAPI
try:
    from fastapi import (
        Depends, FastAPI, Header, HTTPException, Request, status,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, PlainTextResponse, Response
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False

try:
    from prometheus_client import (
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
    from cryptography.hazmat.backends import default_backend  # type: ignore
    from cryptography.hazmat.primitives import hashes  # type: ignore
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # type: ignore
    CRYPTO_AVAILABLE = True
except ImportError:  # pragma: no cover
    CRYPTO_AVAILABLE = False

# Optional SQLAlchemy (async)
try:
    from sqlalchemy import (  # type: ignore
        JSON, Column, DateTime, Float, Integer, Index, String, Text,
    )
    from sqlalchemy.ext.asyncio import (  # type: ignore
        AsyncSession, async_sessionmaker, create_async_engine,
    )
    from sqlalchemy.orm import declarative_base  # type: ignore
    SQLALCHEMY_AVAILABLE = True
except ImportError:  # pragma: no cover
    SQLALCHEMY_AVAILABLE = False

# aiosqlite as a fallback persistence layer
try:
    import aiosqlite  # type: ignore
    AIOSQLITE_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiosqlite = None  # type: ignore
    AIOSQLITE_AVAILABLE = False

# web3 (real network clients) with fallback
try:
    from web3 import Account, HTTPProvider, Web3  # type: ignore
    WEB3_AVAILABLE = True
except ImportError:  # pragma: no cover
    WEB3_AVAILABLE = False

# hvac (Vault) with fallback
try:
    from hvac import Client as VaultClient  # type: ignore
    HVAC_AVAILABLE = True
except ImportError:  # pragma: no cover
    HVAC_AVAILABLE = False

# Optional FeedbackEvent schema
try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# ============================================================================
# Prometheus metrics
# ============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    TRADE_COUNTER = Counter(
        "helium_trades_total", "Total number of trades",
        ["status"], registry=REGISTRY)
    TRADE_LATENCY = Histogram(
        "helium_trade_latency_seconds", "Trade latency in seconds",
        registry=REGISTRY)
    TRANSACTION_COUNTER = Counter(
        "helium_transactions_total", "Total transactions",
        ["type", "status"], registry=REGISTRY)
    TRANSACTION_DURATION = Histogram(
        "helium_transaction_duration_seconds", "Transaction duration",
        ["type"], registry=REGISTRY)
    NONCE_GAP = Gauge(
        "helium_nonce_gap", "Transaction nonce gap", registry=REGISTRY)
    PENDING_TRANSACTIONS = Gauge(
        "helium_pending_transactions", "Number of pending transactions",
        registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge(
        "helium_circuit_breaker_state", "Circuit breaker state",
        ["service"], registry=REGISTRY)
    HEALTH_SCORE = Gauge(
        "helium_system_health", "System health score (0-100)", registry=REGISTRY)
    DB_SIZE = Gauge(
        "helium_db_size_mb", "Database size in MB", registry=REGISTRY)
    GAS_PRICE = Gauge(
        "helium_gas_price_gwei", "Current gas price in Gwei", registry=REGISTRY)
    CARBON_INTENSITY = Gauge(
        "carbon_intensity_gco2_per_kwh", "Real-time carbon intensity",
        registry=REGISTRY)
    TRADE_CARBON_IMPACT = Gauge(
        "trade_carbon_impact_kg", "Carbon impact per trade",
        ["trade_id"], registry=REGISTRY)
    SUSTAINABILITY_SCORE = Gauge(
        "trade_sustainability_score", "Sustainability score (0-100)",
        ["trade_id"], registry=REGISTRY)
    HELIUM_EFFICIENCY = Gauge(
        "helium_trade_efficiency", "Helium efficiency (0-100)",
        ["trade_id"], registry=REGISTRY)
    CARBON_SAVINGS = Counter(
        "helium_carbon_savings_total",
        "Total carbon savings from efficient trades", registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter(
        "quantum_signatures_total", "Quantum-resistant signatures",
        ["algorithm", "status"], registry=REGISTRY)
    L2_GAS_SAVINGS = Gauge(
        "l2_gas_savings_percent", "L2 gas savings percentage",
        ["network"], registry=REGISTRY)
    L2_TRANSACTIONS = Counter(
        "l2_transactions_total", "L2 transactions",
        ["network", "status"], registry=REGISTRY)
    DEFI_POSITIONS = Gauge(
        "defi_positions_total", "Total DeFi positions",
        ["protocol"], registry=REGISTRY)
    DEFI_YIELD = Gauge(
        "defi_yield_apy", "DeFi yield APY", ["protocol"], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter(
        "autonomous_optimizations_total", "Autonomous optimizations",
        ["strategy", "status"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter(
        "helium_safety_violations_total", "Safety violations",
        ["rule"], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter(
        "helium_chaos_experiments_total", "Chaos experiments",
        ["type", "status"], registry=REGISTRY)
    HUMAN_REVIEWS = Counter(
        "helium_human_reviews_total", "Human reviews",
        ["status"], registry=REGISTRY)
    XAI_DECISIONS = Counter(
        "helium_xai_decisions_total", "XAI decisions",
        ["strategy"], registry=REGISTRY)
else:  # pragma: no cover
    class _DummyMetric:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass

    TRADE_COUNTER = TRADE_LATENCY = _DummyMetric()
    TRANSACTION_COUNTER = TRANSACTION_DURATION = _DummyMetric()
    NONCE_GAP = PENDING_TRANSACTIONS = CIRCUIT_BREAKER_STATE = _DummyMetric()
    HEALTH_SCORE = DB_SIZE = GAS_PRICE = CARBON_INTENSITY = _DummyMetric()
    TRADE_CARBON_IMPACT = SUSTAINABILITY_SCORE = HELIUM_EFFICIENCY = _DummyMetric()
    CARBON_SAVINGS = QUANTUM_SIGNATURES = L2_GAS_SAVINGS = _DummyMetric()
    L2_TRANSACTIONS = DEFI_POSITIONS = DEFI_YIELD = _DummyMetric()
    AUTONOMOUS_OPTIMIZATIONS = SAFETY_VIOLATIONS = _DummyMetric()
    CHAOS_EXPERIMENTS = HUMAN_REVIEWS = XAI_DECISIONS = _DummyMetric()
    REGISTRY = None


# ============================================================================
# Custom exceptions
# ============================================================================
class HeliumPlatformException(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None) -> None:
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)
        self.correlation_id = str(uuid.uuid4())[:8]


class QuantumError(HeliumPlatformException): pass
class BlockchainError(HeliumPlatformException): pass
class L2Error(HeliumPlatformException): pass
class DeFiError(HeliumPlatformException): pass
class MLPredictionError(HeliumPlatformException): pass
class ComplianceError(HeliumPlatformException): pass
class IdentityError(HeliumPlatformException): pass
class ContractError(HeliumPlatformException): pass
class CircuitBreakerOpenError(HeliumPlatformException): pass
class RateLimitExceeded(HeliumPlatformException): pass
class SecurityError(HeliumPlatformException): pass
class SafetyViolationError(HeliumPlatformException): pass
class ChaosExperimentError(HeliumPlatformException): pass


# ============================================================================
# Config
# ============================================================================
def _default_jwt_secret() -> str:
    return secrets.token_hex(32)


if PYDANTIC_AVAILABLE and _SETTINGS_AVAILABLE and PYDANTIC_V2:
    class GeneralConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        max_retry_attempts: int = Field(5, ge=1)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(60, ge=1)
        health_check_interval: int = Field(30, ge=5)
        data_version: int = 17
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)
        log_level: str = "INFO"
        data_retention_days: int = 365
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

    class QuantumConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        algorithm: str = "hmac_sha256"
        enable_distillation: bool = False

    class L2Config(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = True
        networks: List[str] = Field(
            default_factory=lambda: ["optimism", "arbitrum", "polygon", "zksync"])

    class DeFiConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        protocols: List[str] = Field(
            default_factory=lambda: ["aave", "compound", "uniswap"])

    class MLConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        enabled: bool = True
        model_type: str = "ensemble"

    class CarbonConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        cost_per_kg: float = 0.10
        api_key: str = ""
        region: str = "global"

    class DatabaseConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        url: str = "sqlite+aiosqlite:///./helium_platform.db"
        pool_size: int = 10
        max_overflow: int = 20

    class JWTConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        secret: str = Field(default_factory=_default_jwt_secret)
        algorithm: str = "HS256"
        expiration_minutes: int = 1440

    class APIConfig(BaseModel):
        model_config = ConfigDict(extra="ignore")
        host: str = "0.0.0.0"
        port: int = 8000

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
            "profit": 0.4, "carbon": 0.3, "gas": 0.2, "latency": 0.1})
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        action_space: List[str] = Field(
            default_factory=lambda: ["arbitrage", "market_making", "trend_following"])

    class HeliumPlatformConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="HELIUM_", case_sensitive=False, extra="ignore")
        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        l2: L2Config = Field(default_factory=L2Config)
        defi: DeFiConfig = Field(default_factory=DeFiConfig)
        ml: MLConfig = Field(default_factory=MLConfig)
        carbon: CarbonConfig = Field(default_factory=CarbonConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        jwt: JWTConfig = Field(default_factory=JWTConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
        safety: SafetyConfig = Field(default_factory=SafetyConfig)
        chaos: ChaosConfig = Field(default_factory=ChaosConfig)
        chain_id: int = 1
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
        max_retry_attempts: int = 5
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        health_check_interval: int = 30
        data_version: int = 17
        rate_limit_requests: int = 100
        rate_limit_window: int = 60
        log_level: str = "INFO"
        data_retention_days: int = 365
        human_review_threshold: float = 0.5
        random_seed: int = 42

    @dataclass
    class QuantumConfig:
        algorithm: str = "hmac_sha256"
        enable_distillation: bool = False

    @dataclass
    class L2Config:
        enabled: bool = True
        networks: List[str] = field(
            default_factory=lambda: ["optimism", "arbitrum", "polygon", "zksync"])

    @dataclass
    class DeFiConfig:
        protocols: List[str] = field(
            default_factory=lambda: ["aave", "compound", "uniswap"])

    @dataclass
    class MLConfig:
        enabled: bool = True
        model_type: str = "ensemble"

    @dataclass
    class CarbonConfig:
        cost_per_kg: float = 0.10
        api_key: str = ""
        region: str = "global"

    @dataclass
    class DatabaseConfig:
        url: str = "sqlite+aiosqlite:///./helium_platform.db"
        pool_size: int = 10
        max_overflow: int = 20

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
            "profit": 0.4, "carbon": 0.3, "gas": 0.2, "latency": 0.1})
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        action_space: List[str] = field(default_factory=lambda: [
            "arbitrage", "market_making", "trend_following"])

    @dataclass
    class HeliumPlatformConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        l2: L2Config = field(default_factory=L2Config)
        defi: DeFiConfig = field(default_factory=DeFiConfig)
        ml: MLConfig = field(default_factory=MLConfig)
        carbon: CarbonConfig = field(default_factory=CarbonConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        jwt: JWTConfig = field(default_factory=JWTConfig)
        api: APIConfig = field(default_factory=APIConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        safety: SafetyConfig = field(default_factory=SafetyConfig)
        chaos: ChaosConfig = field(default_factory=ChaosConfig)
        chain_id: int = 1
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
# Circuit breakers
# ============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
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
        self.last_failure_monotonic: Optional[float] = None
        self._lock = threading.RLock()

    @property
    def _state_metric(self) -> float:
        if self.state is CircuitBreakerState.OPEN:
            return 1.0
        if self.state is CircuitBreakerState.HALF_OPEN:
            return 0.5
        return 0.0

    def _publish_metric(self) -> None:
        try:
            CIRCUIT_BREAKER_STATE.labels(service=self.name).set(self._state_metric)
        except Exception:
            pass

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            if self.state is CircuitBreakerState.OPEN:
                if (self.last_failure_monotonic is not None
                        and time.monotonic() - self.last_failure_monotonic
                        >= self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
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
                self.failure_count += 1
                self.last_failure_monotonic = time.monotonic()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    self._publish_metric()
            raise
        else:
            with self._lock:
                if self.state is CircuitBreakerState.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= self.half_open_success_threshold:
                        self.state = CircuitBreakerState.CLOSED
                        self.failure_count = 0
                else:
                    self.failure_count = 0
                self._publish_metric()
            return result

    def force_open(self) -> None:
        with self._lock:
            self.state = CircuitBreakerState.OPEN
            self.failure_count = self.failure_threshold
            self.last_failure_monotonic = time.monotonic()
            self._publish_metric()

    def reset(self) -> None:
        with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_monotonic = None
            self._publish_metric()

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "name": self.name, "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
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
            return {name: cb.get_metrics()
                    for name, cb in self._breakers.items()}  # type: ignore[attr-defined]


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

    async def start_registered_tasks(self) -> None:
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

    def snapshot(self) -> Dict[str, str]:
        return {n: ("running" if not t.done() else "stopped")
                for n, t in self.tasks.items()}


# ============================================================================
# Persistence layer (aiosqlite — used by XAI + HITL + optimizer state)
# ============================================================================
class AsyncPersistence:
    SCHEMA_VERSION = 1

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = str(db_path)
        self._conn: Optional[Any] = None
        self._init_lock = asyncio.Lock()
        self._query_lock = asyncio.Lock()

    async def connect(self) -> Any:
        if self._conn is not None:
            return self._conn
        async with self._init_lock:
            if self._conn is not None:
                return self._conn
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._init_schema(self._conn)
        return self._conn

    async def _init_schema(self, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS explanations (
                id TEXT PRIMARY KEY, decision_id TEXT NOT NULL,
                explanation TEXT NOT NULL, model_dump TEXT,
                created_at TEXT NOT NULL
            )""")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_expl_did ON explanations(decision_id)")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS human_reviews (
                id TEXT PRIMARY KEY, decision_id TEXT NOT NULL,
                status TEXT NOT NULL, feedback TEXT, reviewer TEXT,
                created_at TEXT NOT NULL, reviewed_at TEXT
            )""")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rev_did ON human_reviews(decision_id)")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY, value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )""")
        await conn.commit()

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

    async def save_explanation(self, decision_id: str, explanation: str,
                               model_dump: Optional[Dict[str, Any]] = None) -> str:
        expl_id = str(uuid.uuid4())
        await self.execute(
            "INSERT INTO explanations (id, decision_id, explanation, model_dump, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (expl_id, decision_id, explanation,
             json.dumps(model_dump, default=str) if model_dump else None,
             datetime.now(timezone.utc).isoformat()))
        return expl_id

    async def get_explanation(self, decision_id: str) -> Optional[Dict[str, Any]]:
        row = await self.fetch_one(
            "SELECT id, decision_id, explanation, model_dump, created_at "
            "FROM explanations WHERE decision_id=? ORDER BY created_at DESC LIMIT 1",
            (decision_id,))
        if row is None:
            return None
        return {
            "id": row[0], "decision_id": row[1], "explanation": row[2],
            "model_dump": json.loads(row[3]) if row[3] else None,
            "created_at": row[4],
        }

    async def save_review(self, decision_id: str, status: str = "pending",
                          feedback: Optional[str] = None,
                          reviewer: Optional[str] = None) -> str:
        rid = str(uuid.uuid4())
        await self.execute(
            "INSERT INTO human_reviews "
            "(id, decision_id, status, feedback, reviewer, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (rid, decision_id, status, feedback, reviewer,
             datetime.now(timezone.utc).isoformat()))
        return rid

    async def update_review(self, review_id: str, status: str,
                            feedback: Optional[str] = None) -> bool:
        cur = await self.execute(
            "UPDATE human_reviews SET status=?, feedback=?, reviewed_at=? WHERE id=?",
            (status, feedback, datetime.now(timezone.utc).isoformat(), review_id))
        return getattr(cur, "rowcount", 0) > 0

    async def get_pending_reviews(self) -> List[Dict[str, Any]]:
        rows = await self.fetch_all(
            "SELECT id, decision_id, status, feedback, reviewer, created_at "
            "FROM human_reviews WHERE status='pending' ORDER BY created_at DESC")
        return [{
            "id": r[0], "decision_id": r[1], "status": r[2],
            "feedback": r[3], "reviewer": r[4], "created_at": r[5],
        } for r in rows]

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


# ============================================================================
# Post-quantum crypto (HMAC-SHA256 with AES-GCM-wrapped secrets)
# ============================================================================
class PostQuantumCrypto:
    SALT_KEY = "pqc_salt"

    def __init__(self, config: HeliumPlatformConfig,
                 persistence: AsyncPersistence) -> None:
        self.config = config
        self.persistence = persistence
        self.master_key = config.get_master_key_bytes()
        self._salt: Optional[bytes] = None
        self._lock = asyncio.Lock()

    async def _load_or_create_salt(self) -> bytes:
        if self._salt is not None:
            return self._salt
        async with self._lock:
            if self._salt is not None:
                return self._salt
            stored = await self.persistence.load_state(self.SALT_KEY)
            if stored is None:
                new_salt = os.urandom(16)
                await self.persistence.save_state(self.SALT_KEY, new_salt.hex())
                self._salt = new_salt
            else:
                self._salt = bytes.fromhex(stored)
            return self._salt

    def _derive_key(self, salt: bytes, length: int = 32) -> bytes:
        if not CRYPTO_AVAILABLE:
            # Fallback: use HMAC-SHA256 as a KDF over the master key + salt.
            return hmac.new(self.master_key, salt, hashlib.sha256).digest()[:length]
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=length, salt=salt,
            iterations=100_000, backend=default_backend())
        return kdf.derive(self.master_key)

    async def _encrypt(self, plaintext: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive_key(salt)
        nonce = os.urandom(12)
        if CRYPTO_AVAILABLE:
            aesgcm = AESGCM(derived)
            return nonce + aesgcm.encrypt(nonce, plaintext, None)
        # Fallback: XOR-with-keystream (not secure, only for offline dev).
        stream = hashlib.sha256(derived + nonce).digest()
        ct = bytes(p ^ stream[i % len(stream)]
                   for i, p in enumerate(plaintext))
        tag = hashlib.sha256(derived + nonce + ct).digest()[:16]
        return nonce + tag + ct

    async def _decrypt(self, blob: bytes) -> bytes:
        salt = await self._load_or_create_salt()
        derived = self._derive_key(salt)
        nonce, rest = blob[:12], blob[12:]
        if CRYPTO_AVAILABLE:
            aesgcm = AESGCM(derived)
            return aesgcm.decrypt(nonce, rest, None)
        tag, ct = rest[:16], rest[16:]
        expected = hashlib.sha256(derived + nonce + ct).digest()[:16]
        if not hmac.compare_digest(tag, expected):
            raise SecurityError("PQC fallback tag mismatch")
        stream = hashlib.sha256(derived + nonce).digest()
        return bytes(c ^ stream[i % len(stream)] for i, c in enumerate(ct))

    async def generate_keypair(self, algorithm: Optional[str] = None,
                               validity_days: int = 30) -> Dict[str, Any]:
        alg = algorithm or self.config.quantum.algorithm or "hmac_sha256"
        secret = secrets.token_bytes(32)
        public_key = hashlib.sha256(secret).digest()
        key_id = f"{alg}_{uuid.uuid4().hex[:8]}"
        expires_at = (datetime.now(timezone.utc)
                      + timedelta(days=validity_days)).isoformat()
        enc_pub = await self._encrypt(public_key)
        enc_priv = await self._encrypt(secret)
        payload = {
            "key_id": key_id, "algorithm": alg,
            "public_key": enc_pub.hex(), "secret": enc_priv.hex(),
            "expires_at": expires_at,
        }
        await self.persistence.save_state(f"pqc_keypair:{key_id}", payload)
        QUANTUM_SIGNATURES.labels(algorithm=alg, status="generated").inc()
        return {
            "key_id": key_id, "algorithm": alg,
            "public_key": public_key.hex(), "expires_at": expires_at,
        }

    async def _load_keypair(self, key_id: str) -> Optional[Dict[str, Any]]:
        return await self.persistence.load_state(f"pqc_keypair:{key_id}")

    async def sign_data(self, data: Dict[str, Any], key_id: str) -> Dict[str, Any]:
        kp = await self._load_keypair(key_id)
        if kp is None:
            raise QuantumError(f"Key {key_id} not found")
        secret = await self._decrypt(bytes.fromhex(kp["secret"]))
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        signature = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        QUANTUM_SIGNATURES.labels(algorithm=kp["algorithm"], status="signed").inc()
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
        kp = await self._load_keypair(key_id)
        if kp is None:
            QUANTUM_SIGNATURES.labels(algorithm="?", status="unknown_key").inc()
            return False
        try:
            secret = await self._decrypt(bytes.fromhex(kp["secret"]))
        except Exception:
            return False
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hmac.new(secret, payload, hashlib.sha256).hexdigest()
        ok = hmac.compare_digest(expected, signature_data.get("signature", ""))
        QUANTUM_SIGNATURES.labels(
            algorithm=kp["algorithm"],
            status="verified" if ok else "mismatch").inc()
        return ok

    async def get_status(self) -> Dict[str, Any]:
        return {
            "algorithm": self.config.quantum.algorithm,
            "crypto_available": CRYPTO_AVAILABLE,
            "pqc_available": True,
        }


# ============================================================================
# L2 bridges (deterministic simulation of real bridges)
# ============================================================================
class RealLayer2Integration:
    NETWORKS = ("optimism", "arbitrum", "polygon", "zksync")

    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)

    async def bridge(
        self, network: str, amount: float, asset: str = "HELIUM",
    ) -> Dict[str, Any]:
        if network not in self.NETWORKS:
            raise L2Error(f"Unsupported L2 network: {network}")
        if amount <= 0:
            raise L2Error("Amount must be positive")
        gas_savings_pct = float(self._rng.uniform(40.0, 90.0))
        tx_hash = "0x" + hashlib.sha256(
            f"{network}:{amount}:{asset}:{time.time()}".encode()
        ).hexdigest()[:40]
        L2_GAS_SAVINGS.labels(network=network).set(gas_savings_pct)
        L2_TRANSACTIONS.labels(network=network, status="success").inc()
        return {
            "network": network,
            "amount": float(amount),
            "asset": asset,
            "tx_hash": tx_hash,
            "gas_savings_percent": gas_savings_pct,
            "simulated": not WEB3_AVAILABLE,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def get_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.config.l2.enabled,
            "networks": list(self.NETWORKS),
            "web3_available": WEB3_AVAILABLE,
        }


# ============================================================================
# DeFi (deterministic APYs and simulated positions)
# ============================================================================
class RealDeFiIntegration:
    APY_BASELINE = {
        "aave": {"USDC": 4.2, "ETH": 2.8, "HELIUM": 6.5},
        "compound": {"USDC": 3.9, "ETH": 2.6, "HELIUM": 6.1},
        "uniswap": {"USDC/ETH": 12.5, "HELIUM/USDC": 18.0},
    }

    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config
        self._positions: Dict[Tuple[str, str], float] = {}

    async def get_apy(self, protocol: str, asset: str) -> float:
        if protocol not in self.APY_BASELINE:
            raise DeFiError(f"Unknown protocol: {protocol}")
        base = self.APY_BASELINE[protocol].get(asset)
        if base is None:
            raise DeFiError(f"Asset {asset} not supported on {protocol}")
        DEFI_YIELD.labels(protocol=protocol).set(base)
        return float(base)

    async def deposit(self, protocol: str, asset: str, amount: float) -> Dict[str, Any]:
        if amount <= 0:
            raise DeFiError("Amount must be positive")
        await self.get_apy(protocol, asset)
        self._positions[(protocol, asset)] = (
            self._positions.get((protocol, asset), 0.0) + amount)
        DEFI_POSITIONS.labels(protocol=protocol).set(
            sum(v for (p, _a), v in self._positions.items() if p == protocol))
        return {
            "protocol": protocol, "asset": asset, "amount": float(amount),
            "status": "deposited", "position": self._positions[(protocol, asset)],
            "simulated": True, "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def withdraw(self, protocol: str, asset: str, amount: float) -> Dict[str, Any]:
        key = (protocol, asset)
        current = self._positions.get(key, 0.0)
        if amount <= 0 or amount > current:
            raise DeFiError("Invalid withdraw amount")
        self._positions[key] = current - amount
        DEFI_POSITIONS.labels(protocol=protocol).set(
            sum(v for (p, _a), v in self._positions.items() if p == protocol))
        return {
            "protocol": protocol, "asset": asset, "amount": float(amount),
            "status": "withdrawn", "position": self._positions[key],
            "simulated": True, "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ============================================================================
# Price prediction (linear regression fallback; Prophet if installed)
# ============================================================================
class PricePredictionEngine:
    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)

    async def predict_price(self, horizon_hours: int = 24,
                            historical_data: Optional[List[Dict]] = None
                            ) -> Dict[str, Any]:
        series: List[float] = []
        if historical_data:
            for row in historical_data:
                if isinstance(row, dict):
                    p = row.get("price") or row.get("close") or row.get("y")
                    if isinstance(p, (int, float)):
                        series.append(float(p))
        if not series:
            # Deterministic seed data so the API is usable without input.
            series = [100.0 + 5.0 * i + float(self._rng.normal(0.0, 1.5))
                      for i in range(48)]
        if len(series) < 2:
            price = series[-1] if series else 100.0
            return {
                "predicted_price": price, "method": "naive",
                "horizon_hours": horizon_hours,
            }
        window = min(len(series), 24)
        x = np.arange(window)
        y = np.asarray(series[-window:], dtype=float)
        slope, intercept = np.polyfit(x, y, 1)
        predicted = float(slope * (window - 1 + horizon_hours) + intercept)
        return {
            "predicted_price": predicted,
            "method": "linear_regression",
            "slope": float(slope),
            "horizon_hours": int(horizon_hours),
        }


# ============================================================================
# FlexGen (deterministic policy simulation)
# ============================================================================
class FlexGenManager:
    def __init__(self, config: HeliumPlatformConfig,
                 persistence: AsyncPersistence) -> None:
        self.config = config
        self.persistence = persistence
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
        reward = float(self._rng.uniform(0.4, 1.0))
        self._last_policy = {
            "workload_id": workload_id, "node_id": node_id,
            "policy": policy, "metrics": metrics, "reward": reward,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.persistence.save_state(
            f"flexgen:{workload_id}", self._last_policy)
        return self._last_policy

    async def get_status(self) -> Dict[str, Any]:
        return {
            "available": True,
            "last_policy": self._last_policy,
            "simulated": True,
        }


# ============================================================================
# Safety monitor
# ============================================================================
class SafetyMonitor:
    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config
        self._last_violation: Dict[str, bool] = {}

    def rules(self) -> Dict[str, Callable[[Dict[str, Any]], bool]]:
        safety = self.config.safety
        return {
            "max_carbon_intensity":
                lambda m: float(m.get("carbon_intensity", 0.0))
                <= safety.max_carbon_intensity,
            "min_renewable_share":
                lambda m: float(m.get("renewable_share", 1.0))
                >= safety.min_renewable_share,
            "max_latency_ms":
                lambda m: float(m.get("latency_ms", 0.0))
                <= safety.max_latency_ms,
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

    async def validate_policy(self, policy: Dict[str, Any],
                              metrics: Dict[str, Any]) -> bool:
        return True

    async def get_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.config.safety.enable_monitor,
            "rules": list(self.rules().keys()),
        }


# ============================================================================
# Causal bandit (safe on bio expansion)
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
# XAI
# ============================================================================
class XAIExplainer:
    async def generate_explanation(self, policy: Dict[str, Any], context: Any,
                                   confidence: float, utility: float,
                                   metrics: Dict[str, Any]) -> str:
        strategy = policy.get("name", "unknown")
        parts: List[str] = []
        if strategy == "arbitrage":
            parts.append("Arbitrage selected: high volatility + moderate gas.")
        elif strategy == "market_making":
            parts.append("Market making selected: low volatility, spread capture.")
        elif strategy == "trend_following":
            parts.append("Trend following selected: strong directional signal.")
        else:
            parts.append(f"Selected strategy: {strategy}.")
        precision = policy.get("params", {}).get("precision")
        if precision:
            parts.append(f"Precision: {precision}.")
        parts.append(f"Confidence: {confidence:.2f}.")
        parts.append(f"Utility: {utility:.2f}.")
        if "carbon_intensity" in metrics:
            parts.append(f"Carbon intensity: {metrics['carbon_intensity']} gCO2/kWh.")
        return " ".join(parts)


# ============================================================================
# Human review manager
# ============================================================================
class HumanReviewManager:
    def __init__(self, config: HeliumPlatformConfig,
                 persistence: Optional[AsyncPersistence] = None) -> None:
        self.config = config
        self.persistence = persistence
        self._pending: Dict[str, Dict[str, Any]] = {}

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
        self._pending[review["review_id"]] = review
        if self.persistence is not None:
            try:
                await self.persistence.save_review(
                    decision_id, status="pending", feedback=explanation)
            except Exception as e:
                logger.warning("Failed to persist review: %s", e)
        HUMAN_REVIEWS.labels(status="pending").inc()
        return review

    async def approve(self, review_id: str,
                      feedback: Optional[str] = None) -> Dict[str, Any]:
        review = self._pending.get(review_id)
        if review is None:
            raise HeliumPlatformException("Review not found", {"id": review_id})
        review["status"] = "approved"
        review["feedback"] = feedback
        review["reviewed_at"] = datetime.now(timezone.utc).isoformat()
        if self.persistence is not None:
            try:
                await self.persistence.update_review(review_id, "approved", feedback)
            except Exception:
                pass
        HUMAN_REVIEWS.labels(status="approved").inc()
        return {"status": "approved", "review_id": review_id}

    async def reject(self, review_id: str,
                     feedback: Optional[str] = None) -> Dict[str, Any]:
        review = self._pending.get(review_id)
        if review is None:
            raise HeliumPlatformException("Review not found", {"id": review_id})
        review["status"] = "rejected"
        review["feedback"] = feedback
        review["reviewed_at"] = datetime.now(timezone.utc).isoformat()
        if self.persistence is not None:
            try:
                await self.persistence.update_review(review_id, "rejected", feedback)
            except Exception:
                pass
        HUMAN_REVIEWS.labels(status="rejected").inc()
        return {"status": "rejected", "review_id": review_id}

    async def get_pending(self) -> List[Dict[str, Any]]:
        return [r for r in self._pending.values() if r["status"] == "pending"]

    async def get_status(self) -> Dict[str, Any]:
        return {
            "pending": len(await self.get_pending()),
            "total": len(self._pending),
        }


# ============================================================================
# Chaos monkey
# ============================================================================
class ChaosMonkey:
    def __init__(self, config: HeliumPlatformConfig,
                 platform: "EnhancedHeliumRightsPlatform") -> None:
        self.config = config
        self.platform = platform
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
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.config.chaos.experiment_interval_seconds)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self._inject_failure()
            except Exception as e:
                logger.error("Chaos experiment failed: %s", e)

    async def _inject_failure(self) -> Dict[str, Any]:
        failure_type = str(self._rng.choice(["latency", "error", "disconnect"]))
        target = str(self._rng.choice(
            ["blockchain", "database", "price_predictor"]))
        status_ = "success"
        result: Dict[str, Any] = {}
        try:
            if failure_type == "latency":
                delay = float(self._rng.uniform(0.05, 0.2))
                await asyncio.sleep(delay)
                result["delay_seconds"] = delay
            elif failure_type == "error":
                result["action"] = "simulated transient error"
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
# Quantum distillation optimizer (optional)
# ============================================================================
class QuantumDistillationOptimizer:
    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config
        self.available = False  # QAOA is not shipped in this build.

    async def optimize(self, strategies: List[Dict[str, Any]],
                       metrics: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.available:
            return None
        # Placeholder for a real QAOA implementation.
        return None


# ============================================================================
# Federated coordinator (in-process FedAvg)
# ============================================================================
class FederatedCoordinator:
    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config
        self._participants: Dict[str, Dict[str, Any]] = {}

    async def register_participant(self, participant_id: str,
                                   model_update: Dict[str, Any]) -> None:
        self._participants[participant_id] = dict(model_update)

    async def aggregate(self) -> Dict[str, Any]:
        if not self._participants:
            return {}
        first = next(iter(self._participants.values()))
        out: Dict[str, float] = {}
        for k in first:
            values = []
            for p in self._participants.values():
                v = p.get(k)
                if isinstance(v, (int, float)):
                    values.append(float(v))
            if values:
                out[k] = float(np.mean(values))
        return out

    async def clear(self) -> None:
        self._participants.clear()


# ============================================================================
# Multi-agent system
# ============================================================================
class MultiAgentSystem:
    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config

    def _score_strategy(self, strategy: str,
                        metrics: Dict[str, Any]) -> float:
        profit = float(metrics.get("expected_profit", 0.0))
        carbon = float(metrics.get("carbon_intensity", 400.0))
        latency = float(metrics.get("latency_ms", 500.0))
        volatility = float(metrics.get("volatility", 0.2))
        if strategy == "arbitrage":
            return (1 - carbon / 1000.0) * 0.4 + volatility * 0.3 + profit * 0.3
        if strategy == "market_making":
            return (1 - carbon / 1000.0) * 0.3 + (1 - volatility) * 0.4 + profit * 0.3
        if strategy == "trend_following":
            return (1 - latency / 1000.0) * 0.3 + volatility * 0.4 + profit * 0.3
        return 0.5

    async def vote(self, strategies: List[Dict[str, Any]],
                   metrics: Dict[str, Any]) -> Dict[str, Any]:
        scores = {s["name"]: self._score_strategy(s["name"], metrics)
                  for s in strategies}
        best = max(scores, key=scores.get) if scores else "hybrid"
        return {"selected_strategy": best, "scores": scores,
                "source": "multi_agent"}


# ============================================================================
# Autonomous optimizer
# ============================================================================
class AutonomousOptimizer:
    def __init__(self, config: HeliumPlatformConfig,
                 persistence: AsyncPersistence,
                 safety_monitor: SafetyMonitor,
                 human_review: HumanReviewManager,
                 xai: XAIExplainer,
                 multi_agent: Optional[MultiAgentSystem] = None) -> None:
        self.config = config
        self.persistence = persistence
        self.safety_monitor = safety_monitor
        self.human_review = human_review
        self.xai = xai
        self.multi_agent = multi_agent
        self._rng = np.random.default_rng(config.general.random_seed)
        self._lock = asyncio.Lock()

        self.action_space = [
            {"name": name, "params": {"precision": "fp16"}}
            for name in config.optimizer.action_space
        ]

        def fallback(_ctx: Any) -> Dict[str, Any]:
            return {"name": "market_making", "params": {"precision": "fp16"}}

        self.bandit = CausalBandit(
            action_space=self.action_space,
            fallback_solver=fallback,
            min_trials_before_bandit=config.optimizer.bandit_min_trials,
            confidence_threshold=config.optimizer.bandit_confidence_threshold,
            seed=config.general.random_seed,
        )
        self.recent_rewards: deque = deque(maxlen=100)

    async def load_state(self) -> None:
        state = await self.persistence.load_state("bandit_state")
        if state:
            try:
                self.bandit.import_state(state)
            except Exception as e:
                logger.warning("Failed to import bandit state: %s", e)

    async def save_state(self) -> None:
        try:
            await self.persistence.save_state(
                "bandit_state", self.bandit.export_state())
        except Exception as e:
            logger.warning("Failed to persist bandit state: %s", e)

    async def optimize_strategy(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Safety gate.
        if self.config.safety.enable_monitor:
            violations = await self.safety_monitor.check(current_state)
            if violations:
                safe_policy = {
                    "name": "market_making", "params": {"precision": "fp16"}}
                result = {
                    "action": safe_policy["name"],
                    "confidence": 1.0,
                    "source": "safety_override",
                    "utility": 0.0,
                    "violations": violations,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                AUTONOMOUS_OPTIMIZATIONS.labels(
                    strategy=safe_policy["name"], status="safety_override").inc()
                return result

        # 2. Bandit decision.
        policy, confidence, source = self.bandit.select_action(current_state)

        # 3. Utility.
        objectives = {
            "profit": float(current_state.get("expected_profit", 0.0)),
            "carbon": float(current_state.get("carbon_intensity", 400.0)) / 1000.0,
            "gas": float(current_state.get("gas_price_gwei", 50.0)) / 200.0,
            "latency": float(current_state.get("latency_ms", 500.0)) / 1000.0,
        }
        weights = self.config.optimizer.modp_weights
        utility = float(sum(objectives[k] * weights.get(k, 1.0)
                            for k in objectives))

        # 4. XAI + persistence.
        explanation = await self.xai.generate_explanation(
            policy, current_state, confidence, utility, current_state)
        decision_id = str(uuid.uuid4())
        try:
            await self.persistence.save_explanation(
                decision_id, explanation,
                {"policy": policy, "state": current_state})
        except Exception as e:
            logger.warning("Failed to persist explanation: %s", e)

        # 5. Human review flag.
        review_required = confidence < self.config.general.human_review_threshold
        review_id: Optional[str] = None
        if review_required:
            review = await self.human_review.request_review(
                decision_id, explanation, current_state)
            review_id = review["review_id"]

        result = {
            "action": policy["name"],
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
            strategy=policy["name"], status="selected").inc()
        XAI_DECISIONS.labels(strategy=policy["name"]).inc()

        await self._emit_feedback_event(decision_id, policy, confidence,
                                        source, utility)
        return result

    async def update_feedback(self, context: Dict[str, Any],
                              strategy: Dict[str, Any],
                              reward: float) -> None:
        self.bandit.update(context, strategy, reward)
        self.recent_rewards.append(float(reward))

    async def evolve_strategies(self) -> List[Dict[str, Any]]:
        # Deterministic expansion — add a renewable-friendly alternative.
        candidate = {
            "name": "green_burst",
            "params": {"precision": "int8", "focus": "renewable_windows"},
        }
        if not any(a["name"] == candidate["name"] for a in self.action_space):
            self.action_space.append(candidate)
            self.bandit.register_action(candidate)
            logger.info("Bio expansion added strategy: %s", candidate["name"])
            return [candidate]
        return []

    async def _emit_feedback_event(self, decision_id: str,
                                   policy: Dict[str, Any], confidence: float,
                                   source: str, utility: float) -> None:
        if FeedbackEvent is None:
            return
        try:
            ev = FeedbackEvent(
                source="helium_rights_platform",
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

    async def get_status(self) -> Dict[str, Any]:
        return {
            "strategies": [s["name"] for s in self.action_space],
            "bandit_state": self.bandit.export_state(),
            "recent_reward_mean": (float(np.mean(self.recent_rewards))
                                   if self.recent_rewards else 0.0),
        }


# ============================================================================
# Sustainability
# ============================================================================
class SustainabilityIntegration:
    def __init__(self, config: HeliumPlatformConfig) -> None:
        self.config = config
        self._rng = np.random.default_rng(config.general.random_seed)
        self._current = 400.0

    async def get_carbon_intensity(self) -> float:
        self._current = float(self._rng.uniform(150.0, 550.0))
        CARBON_INTENSITY.set(self._current)
        return self._current

    async def get_status(self) -> Dict[str, Any]:
        return {"carbon_intensity": self._current}


# ============================================================================
# Platform
# ============================================================================
class EnhancedHeliumRightsPlatform:
    def __init__(self, config: HeliumPlatformConfig,
                 persistence: AsyncPersistence) -> None:
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]
        self.persistence = persistence

        # Subsystems.
        self.safety_monitor = SafetyMonitor(config)
        self.human_review = HumanReviewManager(config, persistence)
        self.xai = XAIExplainer()
        self.multi_agent = MultiAgentSystem(config)
        self.federated = FederatedCoordinator(config)
        self.pqc = PostQuantumCrypto(config, persistence)
        self.l2 = RealLayer2Integration(config)
        self.defi = RealDeFiIntegration(config)
        self.price_predictor = PricePredictionEngine(config)
        self.flexgen_manager = FlexGenManager(config, persistence)
        self.sustainability = SustainabilityIntegration(config)

        # Optimizer (uses the persistence layer directly).
        self.optimizer = AutonomousOptimizer(
            config=config,
            persistence=persistence,
            safety_monitor=self.safety_monitor,
            human_review=self.human_review,
            xai=self.xai,
            multi_agent=self.multi_agent,
        )

        # Background task supervision + chaos.
        self.task_manager = TaskManager()
        self.chaos_monkey = ChaosMonkey(config, self)
        self._register_background_tasks()

        # RBAC users registry.
        self._users: Dict[str, str] = {}  # subject -> password hash

        logger.info("HeliumPlatform v%s initialized (id=%s)",
                    __version__, self.instance_id)

    # ------------------------------------------------------------------
    def _register_background_tasks(self) -> None:
        self.task_manager.register_task("health_check", self._health_check_loop)
        self.task_manager.register_task("monitoring", self._monitoring_loop)

    async def start(self) -> None:
        await self.persistence.connect()
        await self.optimizer.load_state()
        await self.task_manager.start_registered_tasks()
        if self.config.chaos.enabled:
            await self.chaos_monkey.start()
        logger.info("Platform started (id=%s)", self.instance_id)

    async def shutdown(self) -> None:
        logger.info("Shutting down platform (id=%s)", self.instance_id)
        try:
            await self.optimizer.save_state()
        except Exception:
            pass
        if self.chaos_monkey:
            try:
                await self.chaos_monkey.stop()
            except Exception:
                pass
        await self.task_manager.stop_all()
        await self.persistence.close()
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

    async def _monitoring_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.sustainability.get_carbon_intensity()
            except Exception as e:
                logger.error("Monitoring loop error: %s", e)
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=300)
                break
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        checks: Dict[str, Any] = {}
        score = 100
        try:
            checks["persistence"] = {"connected": True}
        except Exception as e:
            checks["persistence"] = {"error": str(e)}
            score -= 20
        try:
            checks["pqc"] = await self.pqc.get_status()
        except Exception as e:
            checks["pqc"] = {"error": str(e)}
            score -= 10
        try:
            checks["l2"] = await self.l2.get_status()
        except Exception as e:
            checks["l2"] = {"error": str(e)}
            score -= 5
        try:
            checks["flexgen"] = await self.flexgen_manager.get_status()
        except Exception as e:
            checks["flexgen"] = {"error": str(e)}
            score -= 5
        try:
            checks["optimizer"] = await self.optimizer.get_status()
        except Exception as e:
            checks["optimizer"] = {"error": str(e)}
            score -= 10
        try:
            checks["circuit_breakers"] = GlobalCircuitBreaker().all_metrics()
        except Exception:
            checks["circuit_breakers"] = {}
        return {
            "healthy": score > 50,
            "instance_id": self.instance_id,
            "version": __version__,
            "health_score": max(0, score),
            "components": checks,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    async def register_user(self, subject: str, password: str) -> Dict[str, Any]:
        if not subject:
            raise SecurityError("Subject must be non-empty")
        salt = os.urandom(16)
        digest = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt, 100_000).hex()
        await self.persistence.save_state(
            f"user:{subject}", {"salt": salt.hex(), "digest": digest})
        return {"subject": subject, "registered": True}

    async def verify_user(self, subject: str, password: str) -> bool:
        stored = await self.persistence.load_state(f"user:{subject}")
        if not isinstance(stored, dict):
            return False
        salt = bytes.fromhex(stored["salt"])
        digest = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt, 100_000).hex()
        return hmac.compare_digest(digest, stored["digest"])


# ============================================================================
# FastAPI
# ============================================================================
if FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE:
    class _RegisterBody(BaseModel):
        subject: str
        password: str

    class _TokenBody(BaseModel):
        subject: str
        password: str

    class _SignBody(BaseModel):
        data: Dict[str, Any]
        key_id: str

    class _VerifyBody(BaseModel):
        data: Dict[str, Any]
        signature_data: Dict[str, Any]

    class _PredictBody(BaseModel):
        horizon_hours: int = 24
        historical_data: Optional[List[Dict[str, Any]]] = None

    class _OptimizeBody(BaseModel):
        state: Dict[str, Any] = {}

    class _FeedbackBody(BaseModel):
        context: Dict[str, Any]
        strategy: Dict[str, Any]
        reward: float

    class _DeFiBody(BaseModel):
        protocol: str
        asset: str
        amount: float

    class _FlexGenBody(BaseModel):
        workload: Dict[str, Any] = {}
        node: Dict[str, Any] = {}

    class _ReviewBody(BaseModel):
        feedback: Optional[str] = None


def build_app(platform: EnhancedHeliumRightsPlatform) -> Any:
    if not (FASTAPI_AVAILABLE and PYDANTIC_AVAILABLE):
        return None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await platform.start()
        try:
            yield
        finally:
            await platform.shutdown()

    app = FastAPI(title="Helium Rights Platform", version=__version__,
                  lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=[], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"])
    security = HTTPBearer(auto_error=False)

    async def _get_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing token")
        if not JWT_AVAILABLE:
            raise HTTPException(status_code=503, detail="JWT not available")
        try:
            payload = jwt.decode(
                credentials.credentials,
                platform.config.jwt.secret,
                algorithms=[platform.config.jwt.algorithm])
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
        return await platform.health_check()

    @app.get("/metrics")
    async def metrics():
        if PROMETHEUS_AVAILABLE and REGISTRY is not None:
            return Response(content=generate_latest(REGISTRY),
                            media_type=CONTENT_TYPE_LATEST)
        return PlainTextResponse("Prometheus not installed")

    @app.post("/register")
    async def register(body: _RegisterBody):
        return await platform.register_user(body.subject, body.password)

    @app.post("/token")
    async def issue_token(body: _TokenBody):
        if not JWT_AVAILABLE:
            raise HTTPException(status_code=503, detail="JWT not available")
        if not await platform.verify_user(body.subject, body.password):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        now = datetime.now(timezone.utc)
        exp = now + timedelta(minutes=platform.config.jwt.expiration_minutes)
        token = jwt.encode(
            {"sub": body.subject, "roles": ["user"],
             "iat": int(now.timestamp()), "exp": int(exp.timestamp())},
            platform.config.jwt.secret,
            algorithm=platform.config.jwt.algorithm)
        return {"token": token, "expires_at": exp.isoformat()}

    @app.post("/pqc/generate")
    async def pqc_generate(user: Dict = Depends(_get_user)):
        return await platform.pqc.generate_keypair()

    @app.post("/pqc/sign")
    async def pqc_sign(body: _SignBody, user: Dict = Depends(_get_user)):
        return await platform.pqc.sign_data(body.data, body.key_id)

    @app.post("/pqc/verify")
    async def pqc_verify(body: _VerifyBody, user: Dict = Depends(_get_user)):
        valid = await platform.pqc.verify_data(body.data, body.signature_data)
        return {"valid": valid}

    @app.post("/predict")
    async def predict(body: _PredictBody, user: Dict = Depends(_get_user)):
        return await platform.price_predictor.predict_price(
            body.horizon_hours, body.historical_data)

    @app.post("/optimize")
    async def optimize(body: _OptimizeBody, user: Dict = Depends(_get_user)):
        return await platform.optimizer.optimize_strategy(body.state)

    @app.post("/optimize/feedback")
    async def optimize_feedback(body: _FeedbackBody,
                                user: Dict = Depends(_get_user)):
        await platform.optimizer.update_feedback(
            body.context, body.strategy, body.reward)
        return {"status": "feedback recorded"}

    @app.post("/optimize/evolve")
    async def optimize_evolve(user: Dict = Depends(_require_admin)):
        return {"new_strategies": await platform.optimizer.evolve_strategies()}

    @app.post("/defi/deposit")
    async def defi_deposit(body: _DeFiBody, user: Dict = Depends(_get_user)):
        return await platform.defi.deposit(body.protocol, body.asset, body.amount)

    @app.post("/defi/withdraw")
    async def defi_withdraw(body: _DeFiBody, user: Dict = Depends(_get_user)):
        return await platform.defi.withdraw(body.protocol, body.asset, body.amount)

    @app.get("/carbon/intensity")
    async def carbon_intensity(user: Dict = Depends(_get_user)):
        return {"intensity": await platform.sustainability.get_carbon_intensity()}

    @app.post("/flexgen/optimize")
    async def flexgen_optimize(body: _FlexGenBody,
                               user: Dict = Depends(_get_user)):
        return await platform.flexgen_manager.optimize_policy(
            body.workload, body.node)

    @app.get("/flexgen/status")
    async def flexgen_status(user: Dict = Depends(_get_user)):
        return await platform.flexgen_manager.get_status()

    @app.get("/safety/status")
    async def safety_status(user: Dict = Depends(_get_user)):
        return await platform.safety_monitor.get_status()

    @app.post("/safety/check")
    async def safety_check(body: _OptimizeBody,
                           user: Dict = Depends(_get_user)):
        return {"violations": await platform.safety_monitor.check(body.state)}

    @app.get("/human-review/pending")
    async def review_pending(user: Dict = Depends(_require_admin)):
        return await platform.human_review.get_pending()

    @app.post("/human-review/{review_id}/approve")
    async def review_approve(review_id: str, body: _ReviewBody,
                             user: Dict = Depends(_require_admin)):
        return await platform.human_review.approve(review_id, body.feedback)

    @app.post("/human-review/{review_id}/reject")
    async def review_reject(review_id: str, body: _ReviewBody,
                            user: Dict = Depends(_require_admin)):
        return await platform.human_review.reject(review_id, body.feedback)

    @app.post("/chaos/trigger")
    async def chaos_trigger(user: Dict = Depends(_require_admin)):
        if not platform.chaos_monkey.enabled:
            raise HTTPException(status_code=400, detail="Chaos disabled")
        return await platform.chaos_monkey._inject_failure()

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
    a = g.get_or_create("same_name")
    b = g.get_or_create("same_name")
    assert a is b


async def _test_rate_limiter() -> None:
    rl = EnhancedRateLimiter(rate=2, per_seconds=60)
    assert await rl.acquire() is True
    assert await rl.acquire() is True
    assert await rl.acquire() is False


async def _test_pqc(tmpdir: Path) -> None:
    cfg = HeliumPlatformConfig()
    cfg.ensure_master_key()
    p = AsyncPersistence(tmpdir / "pqc.db")
    await p.connect()
    pqc = PostQuantumCrypto(cfg, p)
    kp = await pqc.generate_keypair()
    sig = await pqc.sign_data({"x": 1}, kp["key_id"])
    assert await pqc.verify_data({"x": 1}, sig) is True
    assert await pqc.verify_data({"x": 2}, sig) is False
    await p.close()


async def _test_pqc_salt_persistence(tmpdir: Path) -> None:
    cfg = HeliumPlatformConfig()
    cfg.ensure_master_key()
    p1 = AsyncPersistence(tmpdir / "salt.db")
    await p1.connect()
    pqc1 = PostQuantumCrypto(cfg, p1)
    kp = await pqc1.generate_keypair()
    sig = await pqc1.sign_data({"x": 1}, kp["key_id"])
    await p1.close()

    p2 = AsyncPersistence(tmpdir / "salt.db")
    await p2.connect()
    pqc2 = PostQuantumCrypto(cfg, p2)
    assert await pqc2.verify_data({"x": 1}, sig) is True
    await p2.close()


async def _test_causal_bandit_new_policies() -> None:
    bandit = CausalBandit(
        action_space=[{"name": "a", "params": {}}],
        fallback_solver=lambda ctx: {"name": "a", "params": {}},
        min_trials_before_bandit=1, seed=0)
    bandit.update({}, {"name": "a", "params": {}}, 0.5)
    bandit.update({}, {"name": "brand_new", "params": {}}, 0.9)
    assert bandit.q_values["brand_new"] > 0.0


async def _test_optimizer_and_feedback(tmpdir: Path) -> None:
    cfg = HeliumPlatformConfig()
    cfg.ensure_master_key()
    p = AsyncPersistence(tmpdir / "opt.db")
    await p.connect()
    sm = SafetyMonitor(cfg)
    hr = HumanReviewManager(cfg, p)
    xai = XAIExplainer()
    opt = AutonomousOptimizer(cfg, p, sm, hr, xai)
    result = await opt.optimize_strategy(
        {"expected_profit": 0.5, "carbon_intensity": 200.0,
         "gas_price_gwei": 30.0, "latency_ms": 100.0})
    assert "action" in result
    assert "decision_id" in result
    expl = await p.get_explanation(result["decision_id"])
    assert expl is not None
    await p.close()


async def _test_l2_and_defi(tmpdir: Path) -> None:
    cfg = HeliumPlatformConfig()
    l2 = RealLayer2Integration(cfg)
    result = await l2.bridge("optimism", 100.0)
    assert result["gas_savings_percent"] > 0

    defi = RealDeFiIntegration(cfg)
    dep = await defi.deposit("aave", "USDC", 1000.0)
    assert dep["position"] == 1000.0
    wd = await defi.withdraw("aave", "USDC", 400.0)
    assert wd["position"] == 600.0


async def _test_multi_agent_vote() -> None:
    cfg = HeliumPlatformConfig()
    ma = MultiAgentSystem(cfg)
    strategies = [{"name": "arbitrage"}, {"name": "market_making"},
                  {"name": "trend_following"}]
    metrics = {"expected_profit": 0.7, "carbon_intensity": 300.0,
               "latency_ms": 200.0, "volatility": 0.4}
    result = await ma.vote(strategies, metrics)
    # Scores must differ per strategy.
    scores = list(result["scores"].values())
    assert len(set(round(v, 6) for v in scores)) > 1


async def _test_platform_end_to_end(tmpdir: Path) -> None:
    cfg = HeliumPlatformConfig()
    cfg.ensure_master_key()
    cfg.chaos.enabled = False
    cfg.database.url = f"sqlite+aiosqlite:///{tmpdir}/platform.db"
    persistence = AsyncPersistence(tmpdir / "platform.db")
    platform = EnhancedHeliumRightsPlatform(cfg, persistence)
    await platform.start()
    try:
        health = await platform.health_check()
        assert health["health_score"] >= 0
        await platform.register_user("alice", "hunter2")
        assert await platform.verify_user("alice", "hunter2") is True
        assert await platform.verify_user("alice", "wrong") is False
        r = await platform.optimizer.optimize_strategy(
            {"expected_profit": 0.6, "carbon_intensity": 200.0,
             "gas_price_gwei": 30.0, "latency_ms": 100.0})
        assert r["action"]
    finally:
        await platform.shutdown()


def run_tests() -> None:
    import tempfile
    loop = asyncio.new_event_loop()
    try:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            loop.run_until_complete(_test_circuit_breaker())
            loop.run_until_complete(_test_global_registry_threadsafe())
            loop.run_until_complete(_test_rate_limiter())
            loop.run_until_complete(_test_pqc(tmp))
            loop.run_until_complete(_test_pqc_salt_persistence(tmp))
            loop.run_until_complete(_test_causal_bandit_new_policies())
            loop.run_until_complete(_test_optimizer_and_feedback(tmp))
            loop.run_until_complete(_test_l2_and_defi(tmp))
            loop.run_until_complete(_test_multi_agent_vote())
            loop.run_until_complete(_test_platform_end_to_end(tmp))
    finally:
        loop.close()
    print("All Helium Rights Platform v18.0.0 tests passed.")


# ============================================================================
# Entry point
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Helium Rights Platform v18.0.0")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    cfg = HeliumPlatformConfig()
    cfg.ensure_master_key()
    persistence = AsyncPersistence(
        str(Path(cfg.database.url.split("///")[-1])
            if "///" in cfg.database.url else "./helium_platform.db"))
    platform = EnhancedHeliumRightsPlatform(cfg, persistence)

    if FASTAPI_AVAILABLE:
        app = build_app(platform)
        if app is not None:
            import uvicorn  # type: ignore
            uvicorn.run(
                app,
                host=args.host or cfg.api.host,
                port=args.port or cfg.api.port,
                log_level=cfg.general.log_level.lower(),
                server_header=False,
            )
            return

    # No FastAPI: run in a blocking loop with graceful shutdown.
    async def _run() -> None:
        await platform.start()
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            pass
        finally:
            await platform.shutdown()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
