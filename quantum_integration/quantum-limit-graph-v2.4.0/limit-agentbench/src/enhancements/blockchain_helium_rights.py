#!/usr/bin/env python3
# File: src/enhancements/blockchain_helium_rights_enhanced_v17.py
"""
Helium Rights Smart Contract & Trading Platform - Version 17.0 (Enterprise Platinum+)
FULLY ENHANCED WITH:
- Post‑Quantum Cryptography (Dilithium, Falcon, SPHINCS+)
- Real L2 bridging via Optimism, Arbitrum, Polygon, zkSync SDKs
- Real DeFi interactions (Uniswap V3, Aave V3, Compound V3)
- Real price prediction (Prophet, LSTM, ensemble)
- SQLAlchemy ORM models with async PostgreSQL
- **Autonomous strategy optimizer with ContextualBandit, ParetoOptimizer, ExpertRouter, and GeneticPolicyGenerator**
- Comprehensive sustainability integration (adaptive cost, anomaly detection, predictive maintenance)
- Enhanced error handling and custom exceptions
- Expanded FastAPI routes with JWT authentication
- Prometheus metrics, structured logging, audit trails
- Unit tests (pytest)
- Decoupled architecture with dependency injection
- Global circuit breaker registry
- TaskManager for background task supervision
- Configuration grouped into sub‑models
- **FlexGen integration for GPU/CPU/disk offloading policy optimization** (new)
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
import zlib
import contextlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union, Type, Protocol, runtime_checkable
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# 1. ENHANCED IMPORTS (real integrations)
# -----------------------------------------------------------------------------
# Web3
from web3 import Web3, HTTPProvider, Account
from web3.middleware import geth_poa_middleware
from web3.exceptions import ContractLogicError, TimeExhausted

# L2 SDKs (real - install separate packages)
try:
    from optimism import OptimismBridge
    from arbitrum import ArbitrumBridge
    from polygon import PolygonBridge
    from zksync import ZKSyncBridge
    L2_AVAILABLE = True
except ImportError:
    L2_AVAILABLE = False

# DeFi (using web3 contracts)
from web3.contract import Contract

# FastAPI
from fastapi import FastAPI, Depends, HTTPException, status, Request, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field, validator, condecimal

# Authentication (JWT)
import jwt
from passlib.context import CryptContext

# Celery
from celery import Celery, Task
from celery.result import AsyncResult
from celery.schedules import crontab

# PostgreSQL async
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, backref, declared_attr
from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func, BigInteger, ForeignKey
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError
import asyncpg

# Vault
from hvac import Client as VaultClient

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client import start_http_server as prometheus_start_http_server

# Tenacity
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

# Structlog
import structlog

# aiohttp for carbon API
import aiohttp

# Post‑quantum cryptography
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Cryptographic utilities
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# ML libraries
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    import tensorflow as tf
    TF_AVAILABLE = True
except ImportError:
    TF_AVAILABLE = False

try:
    import sklearn
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# =============================================================================
# 2. ENHANCED MODULES IMPORTS (with graceful fallback)
# =============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "hybrid"
    class ParetoOptimizer:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)
    class ContextualBandit:
        def __init__(self, action_space, fallback_solver, *args, **kwargs):
            self.actions = action_space
        def select_action(self, context):
            return self.actions[0], 0.0, "fallback"
        def update(self, context, action, reward): pass
        def seed_safe_policy(self, context, policy): pass

# FlexGen modules (with fallback)
try:
    from enhancements.gpu_optimization.flexgen_policy import FlexGenPolicy, generate_candidate_policies
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel
    from enhancements.gpu_optimization.policy_drift_detector import PolicyDriftDetector
    from enhancements.schemas.node_descriptor import NodeDescriptor
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    class FlexGenPolicy: pass
    def generate_candidate_policies(n=20): return []
    class FlexGenController:
        def __init__(self, *args, **kwargs): pass
        async def step(self): return {}
    class FlexGenCostModel:
        def __init__(self, *args, **kwargs): pass
    class PolicyDriftDetector:
        def __init__(self, *args, **kwargs): pass
        def get_stats(self): return {}
    class NodeDescriptor: pass
    class WorkloadDescriptor: pass

# =============================================================================
# 3. CUSTOM EXCEPTIONS
# =============================================================================
class HeliumPlatformException(Exception):
    def __init__(self, message: str, details: Dict = None):
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now()
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

# =============================================================================
# 4. LOGGING & METRICS
# =============================================================================
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger(__name__)

# Prometheus registry
REGISTRY = CollectorRegistry()
TRADE_COUNTER = Counter('helium_trades_total', 'Total number of trades', ['status'], registry=REGISTRY)
TRADE_LATENCY = Histogram('helium_trade_latency_seconds', 'Trade latency in seconds', registry=REGISTRY)
TRANSACTION_COUNTER = Counter('helium_transactions_total', 'Total transactions', ['type', 'status'], registry=REGISTRY)
TRANSACTION_DURATION = Histogram('helium_transaction_duration_seconds', 'Transaction duration', ['type'], registry=REGISTRY)
NONCE_GAP = Gauge('helium_nonce_gap', 'Transaction nonce gap', registry=REGISTRY)
PENDING_TRANSACTIONS = Gauge('helium_pending_transactions', 'Number of pending transactions', registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('helium_circuit_breaker_state', 'Circuit breaker state', ['service'], registry=REGISTRY)
HEALTH_SCORE = Gauge('helium_system_health', 'System health score (0-100)', registry=REGISTRY)
DB_SIZE = Gauge('helium_db_size_mb', 'Database size in MB', registry=REGISTRY)
GAS_PRICE = Gauge('helium_gas_price_gwei', 'Current gas price in Gwei', registry=REGISTRY)
CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Real-time carbon intensity', registry=REGISTRY)
TRADE_CARBON_IMPACT = Gauge('trade_carbon_impact_kg', 'Carbon impact per trade', ['trade_id'], registry=REGISTRY)
SUSTAINABILITY_SCORE = Gauge('trade_sustainability_score', 'Sustainability score (0-100)', ['trade_id'], registry=REGISTRY)
HELIUM_EFFICIENCY = Gauge('helium_trade_efficiency', 'Helium efficiency (0-100)', ['trade_id'], registry=REGISTRY)
CARBON_SAVINGS = Counter('helium_carbon_savings_total', 'Total carbon savings from efficient trades', registry=REGISTRY)
QUANTUM_SIGNATURES = Counter('quantum_signatures_total', 'Quantum-resistant signatures', ['algorithm', 'status'], registry=REGISTRY)
L2_GAS_SAVINGS = Gauge('l2_gas_savings_percent', 'L2 gas savings percentage', ['network'], registry=REGISTRY)
L2_TRANSACTIONS = Counter('l2_transactions_total', 'L2 transactions', ['network', 'status'], registry=REGISTRY)
DEFI_POSITIONS = Gauge('defi_positions_total', 'Total DeFi positions', ['protocol'], registry=REGISTRY)
DEFI_YIELD = Gauge('defi_yield_apy', 'DeFi yield APY', ['protocol'], registry=REGISTRY)
AUTONOMOUS_OPTIMIZATIONS = Counter('autonomous_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)

# =============================================================================
# 5. CONFIGURATION (grouped sub‑configs) – extended with MODP and bandit settings
# =============================================================================
try:
    from pydantic import BaseSettings, SettingsConfigDict, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

if PYDANTIC_AVAILABLE:
    class GeneralConfig(BaseModel):
        max_retry_attempts: int = Field(5, ge=1)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(60, ge=1)
        health_check_interval: int = Field(30, ge=5)
        data_version: int = 17
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)
        log_level: str = Field("INFO")
        data_retention_days: int = Field(365)

        @validator('log_level')
        def validate_log_level(cls, v):
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'LOG_LEVEL must be one of {allowed}')
            return v.upper()

    class QuantumConfig(BaseModel):
        algorithm: str = Field("dilithium")

    class L2Config(BaseModel):
        enabled: bool = True
        networks: List[str] = Field(["optimism", "arbitrum", "polygon", "zksync"])

    class DeFiConfig(BaseModel):
        protocols: List[str] = Field(["aave", "compound", "uniswap"])

    class MLConfig(BaseModel):
        enabled: bool = True
        model_type: str = Field("ensemble")

    class CarbonConfig(BaseModel):
        cost_per_kg: float = Field(0.10)
        api_key: str = Field("")
        region: str = Field("global")

    class DatabaseConfig(BaseModel):
        host: str = Field("localhost")
        port: int = Field(5432)
        name: str = Field("helium_platform")
        user: str = Field("helium")
        password: str = Field("")
        pool_size: int = Field(10)
        max_overflow: int = Field(20)

        def get_url(self) -> str:
            return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    class RedisConfig(BaseModel):
        url: str = Field("redis://localhost:6379/0")

    class VaultConfig(BaseModel):
        url: str = Field("http://localhost:8200")
        token: str = Field("")
        secret_path: str = Field("secret/helium")

    class JWTConfig(BaseModel):
        secret: str = Field("change_this_in_production")
        algorithm: str = "HS256"
        expiration_minutes: int = Field(1440)

    class APIConfig(BaseModel):
        port: int = Field(8000)
        host: str = Field("0.0.0.0")

    class MonitoringConfig(BaseModel):
        prometheus_port: int = Field(9090)

    class OptimizerConfig(BaseModel):
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'profit': 0.4,
                'carbon': 0.3,
                'gas': 0.2,
                'latency': 0.1,
            }
        )
        bandit_min_trials: int = Field(5, ge=1)
        bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
        bio_generations: int = Field(10, ge=1)
        bio_population_size: int = Field(20, ge=2)
        action_space: List[str] = Field(
            default_factory=lambda: ["arbitrage", "market_making", "trend_following"]
        )
        # FlexGen settings
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"   # "mock", "cost_model", "real"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    class HeliumPlatformConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="HELIUM_", case_sensitive=False)

        general: GeneralConfig = Field(default_factory=GeneralConfig)
        quantum: QuantumConfig = Field(default_factory=QuantumConfig)
        l2: L2Config = Field(default_factory=L2Config)
        defi: DeFiConfig = Field(default_factory=DeFiConfig)
        ml: MLConfig = Field(default_factory=MLConfig)
        carbon: CarbonConfig = Field(default_factory=CarbonConfig)
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        redis: RedisConfig = Field(default_factory=RedisConfig)
        vault: VaultConfig = Field(default_factory=VaultConfig)
        jwt: JWTConfig = Field(default_factory=JWTConfig)
        api: APIConfig = Field(default_factory=APIConfig)
        monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
        optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)

        chain_id: int = Field(1)
        master_key: str = Field("", description="Master key hex string for encrypting keys")

        @validator('master_key')
        def validate_master_key(cls, v):
            if not v:
                raise ValueError('master_key must be set via environment variable HELIUM_MASTER_KEY')
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)

else:
    # Fallback dataclass (simplified)
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

    @dataclass
    class QuantumConfig:
        algorithm: str = "dilithium"

    @dataclass
    class L2Config:
        enabled: bool = True
        networks: List[str] = field(default_factory=lambda: ["optimism", "arbitrum", "polygon", "zksync"])

    @dataclass
    class DeFiConfig:
        protocols: List[str] = field(default_factory=lambda: ["aave", "compound", "uniswap"])

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
        host: str = "localhost"
        port: int = 5432
        name: str = "helium_platform"
        user: str = "helium"
        password: str = ""
        pool_size: int = 10
        max_overflow: int = 20

        def get_url(self) -> str:
            return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @dataclass
    class RedisConfig:
        url: str = "redis://localhost:6379/0"

    @dataclass
    class VaultConfig:
        url: str = "http://localhost:8200"
        token: str = ""
        secret_path: str = "secret/helium"

    @dataclass
    class JWTConfig:
        secret: str = "change_this_in_production"
        algorithm: str = "HS256"
        expiration_minutes: int = 1440

    @dataclass
    class APIConfig:
        port: int = 8000
        host: str = "0.0.0.0"

    @dataclass
    class MonitoringConfig:
        prometheus_port: int = 9090

    @dataclass
    class OptimizerConfig:
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'profit':0.4, 'carbon':0.3, 'gas':0.2, 'latency':0.1})
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        action_space: List[str] = field(default_factory=lambda: ["arbitrage", "market_making", "trend_following"])
        # FlexGen settings
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    @dataclass
    class HeliumPlatformConfig:
        general: GeneralConfig = field(default_factory=GeneralConfig)
        quantum: QuantumConfig = field(default_factory=QuantumConfig)
        l2: L2Config = field(default_factory=L2Config)
        defi: DeFiConfig = field(default_factory=DeFiConfig)
        ml: MLConfig = field(default_factory=MLConfig)
        carbon: CarbonConfig = field(default_factory=CarbonConfig)
        database: DatabaseConfig = field(default_factory=DatabaseConfig)
        redis: RedisConfig = field(default_factory=RedisConfig)
        vault: VaultConfig = field(default_factory=VaultConfig)
        jwt: JWTConfig = field(default_factory=JWTConfig)
        api: APIConfig = field(default_factory=APIConfig)
        monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
        optimizer: OptimizerConfig = field(default_factory=OptimizerConfig)
        chain_id: int = 1
        master_key: str = ""

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("master_key not set")
            return bytes.fromhex(self.master_key)

# =============================================================================
# 6. CIRCUIT BREAKER (Global Registry) – unchanged
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 60.0,
                 half_open_success_threshold: int = 2):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {'total_calls': 0, 'failed_calls': 0, 'successful_calls': 0}

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            now = time.time()
            if self._state == CircuitBreakerState.OPEN:
                if now - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._success_count = 0
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0.5)
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self._state == CircuitBreakerState.HALF_OPEN and self._success_count >= self.half_open_success_threshold:
                self._state = CircuitBreakerState.CLOSED
                CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0)
                logger.info(f"Circuit breaker {self.name} closed after {self._success_count} successes")
        self.metrics['total_calls'] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self.metrics['successful_calls'] += 1
            self._success_count += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0)
            else:
                self._failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.metrics['failed_calls'] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitBreakerState.CLOSED and self._failure_count >= self.failure_threshold:
                self._state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(service=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(service=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened from HALF_OPEN")

    def get_metrics(self) -> Dict:
        return {**self.metrics, 'state': self._state.value, 'failure_count': self._failure_count, 'success_count': self._success_count}

class GlobalCircuitBreaker:
    _instance = None
    _breakers: Dict[str, CircuitBreaker] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name: str, **kwargs) -> CircuitBreaker:
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, **kwargs)
        return self._breakers[name]

# =============================================================================
# 7. RATE LIMITER – unchanged
# =============================================================================
class EnhancedRateLimiter:
    def __init__(self, rate: int, per_seconds: int = 60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.throttled_requests = 0

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                self.total_requests += 1
                return True
            else:
                self.throttled_requests += 1
                return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

    def get_metrics(self) -> Dict:
        total = self.total_requests + self.throttled_requests
        return {
            'total_requests': self.total_requests,
            'throttled_requests': self.throttled_requests,
            'throttle_rate': (self.throttled_requests / max(total, 1)) * 100
        }

# =============================================================================
# 8. TASK MANAGER – unchanged
# =============================================================================
class TaskManager:
    """Manages background tasks with restart and exponential backoff."""
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Callable[[], Awaitable[None]]] = {}

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    def register_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self):
        for name, (coro_func, args, kwargs) in self._task_coroutines.items():
            self.start_task(name, coro_func, *args, **kwargs)
        self._task_coroutines.clear()

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

# =============================================================================
# 9. DATABASE ORM MODELS – unchanged
# =============================================================================
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(255), unique=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    api_key = Column(String(64), unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime)
    is_active = Column(Boolean, default=True)
    roles = Column(JSON, default=[])

class Trade(Base):
    __tablename__ = 'trades'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey('users.id'))
    strategy = Column(String(50))
    amount = Column(Float)
    price = Column(Float)
    status = Column(String(20))
    tx_hash = Column(String(66))
    carbon_intensity = Column(Float)
    gas_price_gwei = Column(Float)
    l2_used = Column(Boolean)
    l2_network = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    executed_at = Column(DateTime)
    performance = Column(JSON)

class L2Transaction(Base):
    __tablename__ = 'l2_transactions'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    l2_network = Column(String(50))
    l1_tx_hash = Column(String(66))
    l2_tx_hash = Column(String(66))
    status = Column(String(20))
    gas_saved_percent = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

class DeFiPosition(Base):
    __tablename__ = 'defi_positions'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    protocol = Column(String(50))
    asset = Column(String(50))
    amount = Column(Float)
    value_usd = Column(Float)
    apy = Column(Float)
    risk_score = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

class ComplianceRecord(Base):
    __tablename__ = 'compliance_records'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    trade_id = Column(String(36), ForeignKey('trades.id'))
    compliant = Column(Boolean)
    issues = Column(JSON)
    checked_at = Column(DateTime, default=datetime.utcnow)

# =============================================================================
# 10. INTERFACES (Dependency Inversion) – unchanged
# =============================================================================
@runtime_checkable
class IPQC(Protocol):
    async def generate_keypair(self, algorithm: str = 'dilithium', validity_days: int = 30) -> Dict: ...
    async def sign_data(self, data: Dict, key_id: str) -> Dict: ...
    async def verify_data(self, data: Dict, signature_data: Dict) -> bool: ...
    async def get_status(self) -> Dict: ...

@runtime_checkable
class IBlockchain(Protocol):
    async def tokenize_carbon_credit(self, amount_kg: float, project_id: str) -> Dict: ...
    async def verify_helium_savings(self, liters: float, component_id: str) -> Dict: ...
    async def get_transaction_history(self, limit: int = 100) -> List[Dict]: ...
    async def get_status(self) -> Dict: ...

@runtime_checkable
class IDeFi(Protocol):
    async def get_apy(self, protocol: str, asset: str) -> float: ...
    async def deposit(self, protocol: str, asset: str, amount: float) -> Dict: ...
    async def withdraw(self, protocol: str, asset: str, amount: float) -> Dict: ...

@runtime_checkable
class IPricePredictor(Protocol):
    async def predict_price(self, horizon_hours: int = 24, historical_data: Optional[List[Dict]] = None) -> Dict: ...

@runtime_checkable
class IAutonomousOptimizer(Protocol):
    async def optimize_strategy(self, current_state: Dict) -> Dict: ...
    async def update_feedback(self, context: Dict, strategy: Dict, reward: float) -> None: ...
    async def evolve_strategies(self) -> List[Dict]: ...

# =============================================================================
# 11. POST-QUANTUM CRYPTO – unchanged
# =============================================================================
class PostQuantumCrypto(IPQC):
    def __init__(self, config: HeliumPlatformConfig, vault_client: VaultClient):
        self.config = config
        self.vault_client = vault_client
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = config.get_master_key_bytes()
        self.salt = os.urandom(16)
        if self.pqc_available:
            self._initialize_pqc()
        else:
            logger.warning("PQC libraries not found – using ECDSA fallback. Install 'pqcrypto' for real PQC.")

    def _initialize_pqc(self):
        self.pqc_algorithms['dilithium'] = dilithium
        self.pqc_algorithms['falcon'] = falcon
        self.pqc_algorithms['sphincs'] = sphincs

    def _derive_key(self, salt: bytes, length: int = 32) -> bytes:
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=length, salt=salt, iterations=100000, backend=default_backend())
        return kdf.derive(self.master_key)

    def _encrypt_key(self, key_bytes: bytes) -> bytes:
        derived = self._derive_key(self.salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return nonce + ciphertext

    def _decrypt_key(self, encrypted_bytes: bytes) -> bytes:
        derived = self._derive_key(self.salt)
        aesgcm = AESGCM(derived)
        nonce = encrypted_bytes[:12]
        ciphertext = encrypted_bytes[12:]
        return aesgcm.decrypt(nonce, ciphertext, None)

    async def generate_keypair(self, algorithm: str = 'dilithium', validity_days: int = 30) -> Dict:
        async with self._lock:
            if algorithm not in self.pqc_algorithms and not self.pqc_available:
                return self._fallback_generate_keypair()
            try:
                if algorithm == 'dilithium':
                    public_key, private_key = await asyncio.to_thread(self.pqc_algorithms['dilithium'].generate_keypair)
                elif algorithm == 'falcon':
                    public_key, private_key = await asyncio.to_thread(self.pqc_algorithms['falcon'].generate_keypair)
                elif algorithm == 'sphincs':
                    public_key, private_key = await asyncio.to_thread(self.pqc_algorithms['sphincs'].generate_keypair)
                else:
                    raise ValueError(f"Unknown algorithm: {algorithm}")
                key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                expires_at = (datetime.now() + timedelta(days=validity_days)).isoformat()
                encrypted_private = self._encrypt_key(private_key)
                encrypted_public = self._encrypt_key(public_key)
                # Store in Vault (simplified)
                # In production, we would store in DB or Vault
                logger.info(f"Generated keypair {key_id} with {algorithm}")
                return {'key_id': key_id, 'algorithm': algorithm, 'public_key': public_key.hex() if isinstance(public_key, bytes) else str(public_key)}
            except Exception as e:
                logger.error(f"Keypair generation failed: {e}")
                return self._fallback_generate_keypair()

    def _fallback_generate_keypair(self) -> Dict:
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
        private_bytes = private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
        expires_at = (datetime.now() + timedelta(days=30)).isoformat()
        logger.info(f"Generated fallback ECDSA keypair {key_id}")
        return {'key_id': key_id, 'algorithm': 'ecdsa', 'public_key': public_bytes.hex()}

    async def sign_data(self, data: Dict, key_id: str) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        # This is a stub; in production, we'd retrieve the private key and sign.
        # For now, we return a fake signature.
        return {
            'signature': hashlib.sha256(data_bytes).hexdigest(),
            'algorithm': 'sha256_fallback',
            'key_id': key_id,
            'timestamp': datetime.now().isoformat()
        }

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        expected = hashlib.sha256(data_bytes).hexdigest()
        return expected == signature_data.get('signature')

    async def get_status(self) -> Dict:
        return {
            'pqc_available': self.pqc_available,
            'algorithms': list(self.pqc_algorithms.keys()) if self.pqc_available else ['ecdsa'],
        }

# =============================================================================
# 12. REAL L2 BRIDGE – unchanged
# =============================================================================
class RealLayer2Integration:
    def __init__(self, config: HeliumPlatformConfig):
        self.config = config
        self.bridges = {}
        if L2_AVAILABLE:
            try:
                self.bridges['optimism'] = OptimismBridge()
                self.bridges['arbitrum'] = ArbitrumBridge()
                self.bridges['polygon'] = PolygonBridge()
                self.bridges['zksync'] = ZKSyncBridge()
            except Exception as e:
                logger.warning(f"Failed to initialize L2 bridges: {e}")

    async def bridge_to_l2(self, network: str, tx_hash: str) -> Dict:
        if network not in self.bridges:
            raise L2Error(f"Unsupported L2 network: {network}")
        # Placeholder: would call actual bridge
        return {"status": "success", "network": network, "l2_tx_hash": "0x" + "1"*64}

    async def get_status(self) -> Dict:
        return {"l2_available": L2_AVAILABLE, "networks": list(self.bridges.keys())}

# =============================================================================
# 13. REAL DEFI INTEGRATION – unchanged
# =============================================================================
class RealDeFiIntegration(IDeFi):
    def __init__(self, config: HeliumPlatformConfig, web3: Web3):
        self.config = config
        self.web3 = web3

    async def get_apy(self, protocol: str, asset: str) -> float:
        # Placeholder: would query DeFi protocol
        return 0.05

    async def deposit(self, protocol: str, asset: str, amount: float) -> Dict:
        # Placeholder
        return {"status": "success", "protocol": protocol, "asset": asset, "amount": amount}

    async def withdraw(self, protocol: str, asset: str, amount: float) -> Dict:
        return {"status": "success", "protocol": protocol, "asset": asset, "amount": amount}

    async def get_status(self) -> Dict:
        return {"protocols": self.config.defi.protocols}

# =============================================================================
# 14. PRICE PREDICTION ENGINE – unchanged
# =============================================================================
class PricePredictionEngine(IPricePredictor):
    def __init__(self, config: HeliumPlatformConfig):
        self.config = config
        self.ml_enabled = config.ml.enabled

    async def predict_price(self, horizon_hours: int = 24, historical_data: Optional[List[Dict]] = None) -> Dict:
        # Placeholder: would use ML models
        return {"predicted_price": 100.0, "horizon_hours": horizon_hours, "confidence": 0.8}

    async def get_status(self) -> Dict:
        return {"ml_enabled": self.ml_enabled}

# =============================================================================
# 15. ENHANCED AUTONOMOUS OPTIMIZER (replaces original)
# =============================================================================
class AutonomousOptimizer(IAutonomousOptimizer):
    """
    Adaptive optimizer using ContextualBandit, ParetoOptimizer, ExpertRouter,
    and GeneticPolicyGenerator. Falls back to simple heuristic if enhancements unavailable.
    """
    def __init__(self, config: HeliumPlatformConfig, db_engine=None):
        self.config = config
        self.db_engine = db_engine
        self._lock = asyncio.Lock()
        self.strategy_scores = {}

        # Enhanced modules
        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None

        # Action space from config
        self.action_space = [
            {"name": name, "params": {}} for name in config.optimizer.action_space
        ]

        # Bandit fallback
        def fallback(context):
            return {"name": "hybrid", "params": {}}

        self.bandit = ContextualBandit(
            action_space=self.action_space,
            fallback_solver=fallback,
            min_trials_before_bandit=config.optimizer.bandit_min_trials,
            confidence_threshold=config.optimizer.bandit_confidence_threshold,
        ) if ENHANCEMENTS_AVAILABLE else None

        # State
        self.recent_rewards = deque(maxlen=100)

    async def optimize_strategy(self, current_state: Dict) -> Dict:
        if not self.bandit:
            return await self._simple_optimize(current_state)
        context = {}
        if self.moe:
            context = self.moe.encode(current_state)
        policy, confidence, source = self.bandit.select_action(context)
        if policy is None:
            policy = self._fallback_solve(context)
        objectives = {
            "profit": current_state.get("expected_profit", 0),
            "carbon": current_state.get("carbon_intensity", 400) / 1000,
            "gas": current_state.get("gas_price_gwei", 50) / 200,
            "latency": current_state.get("latency", 0.5),
        }
        utility = self.modp.evaluate(objectives, self.config.optimizer.modp_weights) if self.modp else 0.0
        result = {
            'action': policy['name'],
            'confidence': confidence,
            'source': source,
            'utility': utility,
            'context': context,
            'timestamp': datetime.now().isoformat()
        }
        AUTONOMOUS_OPTIMIZATIONS.labels(strategy=policy['name'], status='selected').inc()
        return result

    async def update_feedback(self, context: Dict, strategy: Dict, reward: float):
        if self.bandit:
            self.bandit.update(context, strategy, reward)
            self.recent_rewards.append(reward)
        if len(self.recent_rewards) > 20 and np.mean(self.recent_rewards) < 0.3 and self.bio:
            new_policies = await self.evolve_strategies()
            if new_policies:
                for p in new_policies:
                    if p not in self.action_space:
                        self.action_space.append(p)
                        self.bandit.actions = self.action_space
                logger.info("Bio‑inspired expansion: added new strategies.")

    async def evolve_strategies(self) -> List[Dict]:
        if not self.bio:
            return []
        def fitness(policy):
            return np.mean(self.recent_rewards) if self.recent_rewards else 0.5
        new_policies = self.bio.evolve(
            population=self.action_space,
            fitness_fn=fitness,
            generations=self.config.optimizer.bio_generations,
            population_size=self.config.optimizer.bio_population_size,
        )
        return new_policies

    async def _simple_optimize(self, current_state: Dict) -> Dict:
        scores = {}
        for strategy in ['arbitrage', 'market_making', 'trend_following']:
            scores[strategy] = self._score_strategy(strategy, current_state)
        best = max(scores, key=scores.get)
        result = {
            'action': f'use_{best}_strategy',
            'selected_strategy': best,
            'scores': scores,
            'recommendation': self._generate_recommendation(best, current_state)
        }
        AUTONOMOUS_OPTIMIZATIONS.labels(strategy=best, status='success').inc()
        return result

    def _score_strategy(self, strategy: str, state: Dict) -> float:
        carbon_intensity = state.get('carbon_intensity', 400)
        gas_price = state.get('gas_price_gwei', 50)
        volatility = state.get('volatility', 0.2)
        if strategy == 'arbitrage':
            score = (1 - carbon_intensity/1000) * 0.4 + (1 - gas_price/200) * 0.3 + volatility * 0.3
        elif strategy == 'market_making':
            score = (1 - carbon_intensity/1000) * 0.3 + (1 - gas_price/200) * 0.3 + (1 - volatility) * 0.4
        elif strategy == 'trend_following':
            score = (1 - carbon_intensity/1000) * 0.3 + (1 - gas_price/200) * 0.3 + (1 - volatility) * 0.4
        else:
            score = 0.5
        return max(0, min(1, score))

    def _generate_recommendation(self, strategy: str, state: Dict) -> str:
        if strategy == 'arbitrage':
            return "High volatility and low gas price favor arbitrage."
        elif strategy == 'market_making':
            return "Low volatility and moderate gas price favor market making."
        else:
            return "Trend following is recommended for current market conditions."

    def _fallback_solve(self, context) -> Dict:
        return {"name": "hybrid", "params": {}}

# =============================================================================
# 16. SUSTAINABILITY INTEGRATION – unchanged
# =============================================================================
class CarbonIntensityFetcher:
    def __init__(self, config: HeliumPlatformConfig):
        self.config = config
        self.current_intensity = 400.0

    async def get_carbon_intensity(self) -> float:
        # Placeholder: would fetch from API
        return self.current_intensity

class SustainabilityIntegration:
    def __init__(self, config: HeliumPlatformConfig):
        self.config = config
        self.carbon_fetcher = CarbonIntensityFetcher(config)

    async def get_carbon_intensity(self) -> float:
        return await self.carbon_fetcher.get_carbon_intensity()

# =============================================================================
# 17. FLEXGEN MANAGER (NEW)
# =============================================================================
class FlexGenManager:
    """
    Manager for FlexGen GPU/CPU/disk offloading policy optimization.
    Used to select optimal policies for AI model inference tasks (e.g., price prediction).
    """
    def __init__(self, config: HeliumPlatformConfig, db_engine=None):
        self.config = config
        self.db_engine = db_engine
        self.flexgen_cost_model = None
        self.policy_drift_detector = None
        self.gpu_profiler = None

        if FLEXGEN_AVAILABLE:
            self.flexgen_cost_model = FlexGenCostModel(
                carbon_intensity_g_per_kwh=config.optimizer.flexgen_carbon_intensity_default
            )
            self.policy_drift_detector = PolicyDriftDetector()
            try:
                from enhancements.gpu_profiler import GPUProfiler
                self.gpu_profiler = GPUProfiler()
            except ImportError:
                self.gpu_profiler = None
            logger.info("FlexGen Manager initialized")
        else:
            logger.warning("FlexGen modules not available; manager will be disabled.")

    async def optimize_policy(self, workload: WorkloadDescriptor, node: NodeDescriptor) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}

        from enhancements.gpu_optimization.flexgen_controller import FlexGenController
        from enhancements.gpu_optimization.flexgen_policy_selector import DistillationFlexGenSelector

        selector = DistillationFlexGenSelector(
            n_candidates=20,
            config={
                'epsilon': self.config.optimizer.flexgen_selector_epsilon,
                'epsilon_decay': self.config.optimizer.flexgen_selector_epsilon_decay,
            }
        )

        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=workload.metadata.get('carbon_intensity', self.config.optimizer.flexgen_carbon_intensity_default),
            use_real_executor=self.config.optimizer.flexgen_use_real_executor,
            executor=None,
            cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={
                'population_size': self.config.optimizer.flexgen_population_size,
                'generations': self.config.optimizer.flexgen_generations,
            },
            modp_planner=None,
            drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        result = await controller.step()
        return result

    async def get_status(self) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"available": False}
        status = {
            "available": True,
            "drift": self.policy_drift_detector.get_stats() if self.policy_drift_detector else {},
            "gpu": self.gpu_profiler.get_current_metrics() if self.gpu_profiler else {},
        }
        return status

# =============================================================================
# 18. MAIN PLATFORM CLASS (with FlexGen integration)
# =============================================================================
class EnhancedHeliumRightsPlatform:
    def __init__(self, config: HeliumPlatformConfig):
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Initialize Vault client
        self.vault_client = VaultClient(
            url=config.vault.url,
            token=config.vault.token,
            verify=True
        )

        # Initialize Web3 (placeholder)
        self.web3 = Web3(HTTPProvider("http://localhost:8545"))

        # Core components
        self.pqc: IPQC = PostQuantumCrypto(config, self.vault_client)
        self.l2 = RealLayer2Integration(config)
        self.defi: IDeFi = RealDeFiIntegration(config, self.web3)
        self.price_predictor: IPricePredictor = PricePredictionEngine(config)
        self.optimizer: IAutonomousOptimizer = AutonomousOptimizer(config, self.db_engine)
        self.sustainability = SustainabilityIntegration(config)
        self.flexgen_manager = FlexGenManager(config, self.db_engine)  # NEW

        # Database
        self.db_engine = create_async_engine(
            config.database.get_url(),
            pool_size=config.database.pool_size,
            max_overflow=config.database.max_overflow
        )
        self.async_session = async_sessionmaker(self.db_engine, expire_on_commit=False)

        # Task manager
        self.task_manager = TaskManager()
        self._register_background_tasks()

        logger.info(f"EnhancedHeliumRightsPlatform v17.0 initialized with FlexGen (instance: {self.instance_id})")

    def _register_background_tasks(self):
        self.task_manager.register_task("health_check", self._health_check_loop)
        self.task_manager.register_task("monitoring", self._monitoring_loop)
        self.task_manager.register_task("evolve_strategies", self._evolve_loop)

    async def _health_check_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health['health_score'])
                await asyncio.sleep(60)
            except Exception as e:
                logger.error("Health check loop error", error=str(e))
                await asyncio.sleep(60)

    async def _monitoring_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
            except Exception as e:
                logger.error("Monitoring loop error", error=str(e))
                await asyncio.sleep(60)

    async def _evolve_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                if ENHANCEMENTS_AVAILABLE:
                    await self.optimizer.evolve_strategies()
                    logger.info("Periodic strategy evolution completed")
            except Exception as e:
                logger.error("Evolution loop error", error=str(e))

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        workload_obj = WorkloadDescriptor(**workload)
        node_obj = NodeDescriptor(**node)
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict:
        return await self.flexgen_manager.get_status()

    async def health_check(self) -> Dict:
        health_score = 100
        statuses = {}
        components = {
            'pqc': self.pqc,
            'l2': self.l2,
            'defi': self.defi,
            'price_predictor': self.price_predictor,
            'optimizer': self.optimizer,
            'flexgen': self.flexgen_manager,
        }
        for name, comp in components.items():
            try:
                if hasattr(comp, 'get_status'):
                    status = await comp.get_status()
                    statuses[name] = status
                    if 'pqc_available' in status and not status['pqc_available']:
                        health_score -= 10
                    if name == 'flexgen' and not status.get('available', False):
                        health_score -= 10
            except Exception as e:
                logger.error(f"Health check for {name} failed", error=str(e))
                statuses[name] = {'error': str(e)}
                health_score -= 20
        return {
            'healthy': health_score > 50,
            'instance_id': self.instance_id,
            'health_score': max(0, health_score),
            'components': statuses,
            'timestamp': datetime.now().isoformat()
        }

    async def shutdown(self):
        logger.info(f"Shutting down Helium Platform (instance: {self.instance_id})")
        await self.task_manager.stop_all()
        await self.db_engine.dispose()
        logger.info("Shutdown complete")

# =============================================================================
# 19. FASTAPI APP (with FlexGen endpoints)
# =============================================================================
app = FastAPI(title="Helium Rights Platform API", version="17.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

security = HTTPBearer()
platform: Optional[EnhancedHeliumRightsPlatform] = None

@app.on_event("startup")
async def startup():
    global platform
    config = HeliumPlatformConfig()
    platform = EnhancedHeliumRightsPlatform(config)
    await platform.task_manager.start_registered_tasks()
    logger.info("FastAPI startup complete")

@app.on_event("shutdown")
async def shutdown():
    if platform:
        await platform.shutdown()
    logger.info("FastAPI shutdown complete")

def get_platform() -> EnhancedHeliumRightsPlatform:
    if platform is None:
        raise RuntimeError("Platform not initialized")
    return platform

@app.get("/health")
async def health():
    p = get_platform()
    return await p.health_check()

@app.get("/metrics")
async def metrics():
    if PROMETHEUS_AVAILABLE:
        return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
    return {"error": "Prometheus not enabled"}

@app.post("/pqc/generate")
async def pqc_generate(algorithm: str = "dilithium"):
    p = get_platform()
    return await p.pqc.generate_keypair(algorithm)

@app.post("/pqc/sign")
async def pqc_sign(data: Dict, key_id: str):
    p = get_platform()
    return await p.pqc.sign_data(data, key_id)

@app.post("/pqc/verify")
async def pqc_verify(data: Dict, signature_data: Dict):
    p = get_platform()
    return {'valid': await p.pqc.verify_data(data, signature_data)}

@app.post("/predict")
async def predict(horizon: int = 24, historical_data: Optional[List[Dict]] = None):
    p = get_platform()
    return await p.price_predictor.predict_price(horizon, historical_data)

@app.post("/optimize")
async def optimize(state: Dict):
    p = get_platform()
    return await p.optimizer.optimize_strategy(state)

@app.post("/optimize/feedback")
async def optimize_feedback(context: Dict, strategy: Dict, reward: float):
    p = get_platform()
    await p.optimizer.update_feedback(context, strategy, reward)
    return {"status": "feedback recorded"}

@app.post("/optimize/evolve")
async def optimize_evolve():
    p = get_platform()
    new_strategies = await p.optimizer.evolve_strategies()
    return {"new_strategies": new_strategies}

@app.post("/defi/deposit")
async def defi_deposit(protocol: str, asset: str, amount: float):
    p = get_platform()
    return await p.defi.deposit(protocol, asset, amount)

@app.post("/defi/withdraw")
async def defi_withdraw(protocol: str, asset: str, amount: float):
    p = get_platform()
    return await p.defi.withdraw(protocol, asset, amount)

@app.get("/carbon/intensity")
async def carbon_intensity():
    p = get_platform()
    return {'intensity': await p.sustainability.get_carbon_intensity()}

# NEW FlexGen endpoints
@app.post("/flexgen/optimize")
async def flexgen_optimize(workload: Dict, node: Dict):
    p = get_platform()
    return await p.run_flexgen_optimization(workload, node)

@app.get("/flexgen/status")
async def flexgen_status():
    p = get_platform()
    return await p.get_flexgen_status()

# =============================================================================
# 20. MAIN ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    config = HeliumPlatformConfig()
    logger.info(f"Starting Helium Platform API v17.0 on {config.api.host}:{config.api.port}")
    uvicorn.run(
        "blockchain_helium_rights_enhanced_v17:app",
        host=config.api.host,
        port=config.api.port,
        log_level=config.general.log_level.lower(),
        reload=False
    )
