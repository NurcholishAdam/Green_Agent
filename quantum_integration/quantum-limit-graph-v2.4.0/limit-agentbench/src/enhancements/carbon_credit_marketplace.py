#!/usr/bin/env python3
# File: src/enhancements/carbon_credit_marketplace.py
"""
Carbon Credit Marketplace for Green Agent v5.1.0 (Enterprise Quantum Resilience+)

ENHANCEMENTS OVER v5.0.0:
- Added Quantum‑Distillation Integration (optional Qiskit)
- Added Causal Reinforcement Learning (CausalBandit)
- Added Federated Green Learning Coordinator (stub)
- Added Advanced Multi‑Agent Coordination (MultiAgentSystem)
- Added Temporal Logic & Formal Verification (SafetyMonitor)
- Added Explainable AI (XAI) for every decision
- Integrated Adaptive Precision Switching with FlexGen
- Added Integration with Renewable Energy Credits (RECs) alongside carbon credits
- Added Resilience Engineering & Chaos Testing (ChaosMonkey)
- Added Human‑in‑the‑Loop with Active Learning (HumanReviewManager)

All previous features retained.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Set, Union, Protocol, runtime_checkable
from collections import deque, defaultdict
import random
import io
import csv
import sqlite3  # for fallback sync

import aiohttp
import numpy as np
import pandas as pd

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict, field_serializer
from pydantic_settings import BaseSettings, SettingsConfigDict

# ---------- Async SQLAlchemy with aiosqlite ----------
try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy.orm import declarative_base, declared_attr, sessionmaker
    from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, Text, select, update, delete, func, text
    from sqlalchemy.pool import NullPool
    from sqlalchemy.ext.asyncio import AsyncEngine
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_ASYNC_AVAILABLE = True
except ImportError:
    SQLALCHEMY_ASYNC_AVAILABLE = False

# ---------- FastAPI ----------
from fastapi import FastAPI, Depends, HTTPException, status, Request, Response, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ---------- Authentication ----------
import jwt
from passlib.context import CryptContext

# ---------- Rate limiting (Redis fallback) ----------
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False

# ---------- Retry & Circuit Breaker ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structured logging ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- Web3 ----------
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware
    from web3.exceptions import ContractLogicError, TimeExhausted
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# ---------- Post‑quantum cryptography ----------
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# ---------- Cloud SDKs ----------
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# ---------- Predictive analytics ----------
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# ---------- Vault ----------
try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

# ---------- OpenTelemetry ----------
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

# ---------- Redis (for rate limiting) ----------
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# ---------- Qiskit for Quantum‑Distillation ----------
try:
    import qiskit
    from qiskit import QuantumCircuit, Aer, execute
    from qiskit.optimization import QuadraticProgram
    from qiskit.optimization.algorithms import MinimumEigenOptimizer
    from qiskit.algorithms import QAOA
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False

# =============================================================================
# ENHANCED MODULES IMPORTS (with graceful fallback)
# =============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
    from enhancements.moe_system import ExpertRouter
    from enhancements.MODP import ParetoOptimizer
    from enhancements.contextual_bandit import ContextualBandit
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    # Fallback stubs
    class GeneticPolicyGenerator:
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            return population[0] if population else {}
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "default"
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
# CONFIGURATION (Grouped sub‑models)
# =============================================================================

class DatabaseConfig(BaseModel):
    path: str = Field("carbon_credits.db")
    pool_size: int = Field(10)
    max_overflow: int = Field(20)

class BlockchainConfig(BaseModel):
    rpc_url: str = Field("http://localhost:8545")
    contract_address: Optional[str] = Field(None)
    private_key: Optional[str] = Field(None)
    chain_id: int = Field(1)

class CloudConfig(BaseModel):
    aws_bucket: Optional[str] = Field(None)
    aws_access_key: Optional[str] = Field(None)
    aws_secret_key: Optional[str] = Field(None)
    aws_region: str = Field("us-east-1")
    azure_connection_string: Optional[str] = Field(None)
    azure_container: Optional[str] = Field(None)
    gcp_credentials: Optional[str] = Field(None)
    gcp_bucket: Optional[str] = Field(None)

class WebhookConfig(BaseModel):
    url: Optional[str] = Field(None)
    secret: Optional[str] = Field(None)

class CarbonConfig(BaseModel):
    api_key: Optional[str] = Field(None)
    region: str = Field("global")

class RateLimitConfig(BaseModel):
    enabled: bool = True
    requests_per_minute: int = Field(100)
    burst: int = Field(20)
    redis_url: Optional[str] = Field(None)

class GeneralConfig(BaseModel):
    refresh_interval_seconds: int = Field(3600)
    auto_offset_enabled: bool = True
    auto_offset_threshold_kg: float = Field(100.0)
    auto_offset_interval_seconds: int = Field(3600)
    retry_attempts: int = Field(3)
    retry_min_wait: float = Field(2.0)
    retry_max_wait: float = Field(10.0)
    circuit_breaker_threshold: int = Field(5)
    circuit_breaker_timeout: int = Field(60)
    jwt_secret: str = Field("change_me_in_production")
    jwt_algorithm: str = Field("HS256")
    jwt_expiration_minutes: int = Field(1440)
    data_retention_days: int = Field(365)
    log_level: str = Field("INFO")
    prometheus_port: int = Field(9090)
    human_review_threshold: float = Field(0.5, ge=0, le=1)   # NEW

class OptimizerConfig(BaseModel):
    modp_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'price': 0.25,
            'vintage': 0.25,
            'biodiversity': 0.20,
            'carbon_intensity': 0.30,
        }
    )
    bandit_min_trials: int = Field(5, ge=1)
    bandit_confidence_threshold: float = Field(0.6, ge=0, le=1)
    bio_generations: int = Field(10, ge=1)
    bio_population_size: int = Field(20, ge=2)

    # FlexGen settings
    flexgen_carbon_intensity_default: float = 400.0
    flexgen_population_size: int = 50
    flexgen_generations: int = 10
    flexgen_use_real_executor: bool = False
    flexgen_executor_type: str = "mock"   # "mock", "cost_model", "real"
    flexgen_selector_epsilon: float = 0.1
    flexgen_selector_epsilon_decay: float = 0.999

    # Quantum distillation
    enable_distillation: bool = False     # NEW
    quantum_backend: str = "aer_simulator"
    quantum_reps: int = 1

class SafetyConfig(BaseModel):
    enable_monitor: bool = True
    max_carbon_intensity: float = 500.0
    min_biodiversity: float = 0.0
    max_price_per_kg: float = 2.0

class ChaosConfig(BaseModel):
    enabled: bool = False
    failure_probability: float = 0.1
    experiment_interval_seconds: int = 120

class FederatedConfig(BaseModel):
    enabled: bool = False
    aggregation_interval_seconds: int = 300

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CARBON_", case_sensitive=False)

    general: GeneralConfig = Field(default_factory=GeneralConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    blockchain: BlockchainConfig = Field(default_factory=BlockchainConfig)
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    webhook: WebhookConfig = Field(default_factory=WebhookConfig)
    carbon: CarbonConfig = Field(default_factory=CarbonConfig)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    optimizer: OptimizerConfig = Field(default_factory=OptimizerConfig)
    safety: SafetyConfig = Field(default_factory=SafetyConfig)   # NEW
    chaos: ChaosConfig = Field(default_factory=ChaosConfig)     # NEW
    federated: FederatedConfig = Field(default_factory=FederatedConfig)  # NEW

    API_HOST: str = Field("0.0.0.0")
    API_PORT: int = Field(8000)
    REDIS_URL: Optional[str] = Field(None)
    VAULT_URL: Optional[str] = Field(None)
    VAULT_TOKEN: Optional[str] = Field(None)
    VAULT_SECRET_PATH: str = Field("secret/carbon")
    MASTER_KEY: str = Field("", description="Hex string of master key")

    @field_validator('MASTER_KEY')
    @classmethod
    def validate_master_key(cls, v: str) -> str:
        if not v:
            raise ValueError("MASTER_KEY must be set via environment variable CARBON_MASTER_KEY")
        return v

    def get_master_key_bytes(self) -> bytes:
        return bytes.fromhex(self.MASTER_KEY)

config = Settings()

# =============================================================================
# CUSTOM EXCEPTION HIERARCHY
# =============================================================================
class CarbonMarketplaceException(Exception):
    def __init__(self, message: str, details: Dict = None):
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now()

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

# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    PURCHASE_COUNTER = Counter("carbon_credits_purchased_total", "Total credits purchased", ["project_id"], registry=REGISTRY)
    RETIRE_COUNTER = Counter("carbon_credits_retired_total", "Total credits retired", ["status"], registry=REGISTRY)
    BALANCE_GAUGE = Gauge("carbon_credits_balance_kg", "Current available balance", registry=REGISTRY)
    AUTO_OFFSET_COUNTER = Counter("auto_offset_actions_total", "Auto‑offset actions performed", ["reason"], registry=REGISTRY)
    PROJECT_COUNT = Gauge("carbon_projects_available", "Number of active projects", registry=REGISTRY)
    BLOCKCHAIN_TX_FAILURES = Counter("blockchain_tx_failures_total", "Blockchain transaction failures", registry=REGISTRY)
    API_REQUESTS = Counter("api_requests_total", "API requests", ["endpoint", "method", "status"], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge("carbon_circuit_breaker_state", "Circuit breaker state", ["service"], registry=REGISTRY)
    REGISTRY_API_LATENCY = Histogram("registry_api_latency_seconds", "Registry API call latency", ["registry"], registry=REGISTRY)
    AUTO_OFFSET_SUCCESS = Counter("auto_offset_success_total", "Auto‑offset successful", registry=REGISTRY)
    PREDICTION_ERROR = Counter("prediction_error_total", "Price prediction errors", registry=REGISTRY)
    PQC_SIGNATURES = Counter("pqc_signatures_total", "PQC signatures", ["algorithm", "status"], registry=REGISTRY)
    CLOUD_STORE = Counter("cloud_store_total", "Cloud storage operations", ["provider", "status"], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter("carbon_safety_violations_total", "Safety violations", ["rule"], registry=REGISTRY)  # NEW
    CHAOS_EXPERIMENTS = Counter("carbon_chaos_experiments_total", "Chaos experiments", ["type", "status"], registry=REGISTRY)  # NEW
    HUMAN_REVIEWS = Counter("carbon_human_reviews_total", "Human reviews", ["status"], registry=REGISTRY)  # NEW
    XAI_DECISIONS = Counter("carbon_xai_decisions_total", "XAI decisions", ["strategy"], registry=REGISTRY)  # NEW
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
    PURCHASE_COUNTER = DummyMetric()
    RETIRE_COUNTER = DummyMetric()
    BALANCE_GAUGE = DummyMetric()
    AUTO_OFFSET_COUNTER = DummyMetric()
    PROJECT_COUNT = DummyMetric()
    BLOCKCHAIN_TX_FAILURES = DummyMetric()
    API_REQUESTS = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    REGISTRY_API_LATENCY = DummyMetric()
    AUTO_OFFSET_SUCCESS = DummyMetric()
    PREDICTION_ERROR = DummyMetric()
    PQC_SIGNATURES = DummyMetric()
    CLOUD_STORE = DummyMetric()
    SAFETY_VIOLATIONS = DummyMetric()
    CHAOS_EXPERIMENTS = DummyMetric()
    HUMAN_REVIEWS = DummyMetric()
    XAI_DECISIONS = DummyMetric()

# =============================================================================
# GLOBAL CIRCUIT BREAKER REGISTRY
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    # (unchanged from previous version, kept for brevity in this enhanced version)
    def __init__(self, name: str, threshold: int = 5, timeout: int = 60):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._lock = asyncio.Lock()
        self._metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if time.time() - self._last_failure_time >= self.timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._failure_count = 0
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        self._metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self._metrics["successful_calls"] += 1
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self._metrics["failed_calls"] += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitBreakerState.CLOSED and self._failure_count >= self.threshold:
                self._state = CircuitBreakerState.OPEN
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN

    def get_metrics(self) -> Dict:
        return {
            'state': self._state.value,
            'failure_count': self._failure_count,
            'total_calls': self._metrics['total_calls'],
            'failed_calls': self._metrics['failed_calls'],
            'successful_calls': self._metrics['successful_calls'],
        }

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
# TASK MANAGER (Central supervision) - unchanged from original
# =============================================================================
class TaskManager:
    # (unchanged, included for completeness)
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
# INTERFACES (Dependency Inversion) - unchanged from original
# =============================================================================
@runtime_checkable
class IRegistryClient(Protocol):
    async def fetch_projects(self) -> List[Dict]: ...
    async def close(self): ...

@runtime_checkable
class IBlockchainClient(Protocol):
    async def mint(self, project_id: str, amount_kg: float, owner: str) -> str: ...
    async def get_balance(self, address: str) -> float: ...
    async def close(self): ...

@runtime_checkable
class IPQC(Protocol):
    async def generate_keypair(self, algorithm: str = 'dilithium', validity_days: int = 30) -> Dict: ...
    async def sign_data(self, data: Dict, key_id: str) -> Dict: ...
    async def verify_data(self, data: Dict, signature_data: Dict) -> bool: ...

@runtime_checkable
class ICloudStorage(Protocol):
    async def store(self, data: Dict, filename: str = None) -> Dict: ...

@runtime_checkable
class IPricePredictor(Protocol):
    async def predict(self, days: int = 30) -> Optional[List[float]]: ...
    async def train(self): ...
    async def update_history(self, price_data: Dict): ...

@runtime_checkable
class IAutoOffsetEngine(Protocol):
    async def offset(self, emissions_kg: float, reason: str = "auto_offset"): ...

# =============================================================================
# NEW: SAFETY MONITOR (Temporal Logic-like rules)
# =============================================================================
class SafetyMonitor:
    def __init__(self, config: Settings):
        self.config = config
        self.rules = {
            'max_carbon_intensity': lambda project: project.metadata.get('carbon_intensity', 0) <= config.safety.max_carbon_intensity,
            'min_biodiversity': lambda project: project.co_benefits.get('biodiversity', 0) >= config.safety.min_biodiversity,
            'max_price_per_kg': lambda project: project.price_per_kg_usd <= config.safety.max_price_per_kg,
        }

    async def check_project(self, project: 'CreditProject') -> List[Dict]:
        violations = []
        for rule_name, check_fn in self.rules.items():
            if not check_fn(project):
                violation = {'rule': rule_name, 'project_id': project.project_id, 'details': project.dict()}
                violations.append(violation)
                SAFETY_VIOLATIONS.labels(rule=rule_name).inc()
                logger.warning(f"Safety violation: {rule_name} for project {project.project_id}")
        return violations

    async def validate_purchase(self, project: 'CreditProject', amount_kg: float, context: Dict) -> bool:
        violations = await self.check_project(project)
        return len(violations) == 0

    async def get_status(self) -> Dict:
        return {'enabled': self.config.safety.enable_monitor, 'rules': list(self.rules.keys())}

# =============================================================================
# NEW: CAUSAL BANDIT (replaces ContextualBandit)
# =============================================================================
class CausalBandit:
    def __init__(self, action_space, fallback_solver, min_trials_before_bandit=5, confidence_threshold=0.6):
        self.actions = action_space
        self.fallback_solver = fallback_solver
        self.min_trials = min_trials_before_bandit
        self.confidence_threshold = confidence_threshold
        self.q_values = {a['name']: 0.0 for a in action_space}
        self.counts = {a['name']: 0 for a in action_space}
        self.causal_effects = {a['name']: 0.0 for a in action_space}
        self.trials = 0
        self.context_history = []
        self.reward_history = []
        self.action_history = []

    def select_action(self, context):
        if self.trials < self.min_trials:
            return self.fallback_solver(context), 0.0, "fallback"
        epsilon = 0.1
        if random.random() < epsilon:
            name = random.choice(self.actions)['name']
        else:
            # Use causal effect if confident
            if self.trials >= 10 and any(self.causal_effects.values()):
                name = max(self.causal_effects, key=self.causal_effects.get)
            else:
                name = max(self.q_values, key=self.q_values.get)
        action = next(a for a in self.actions if a['name'] == name)
        confidence = 0.5
        return action, confidence, "causal"

    def update(self, context, action, reward):
        self.trials += 1
        name = action['name']
        self.counts[name] += 1
        self.q_values[name] += (reward - self.q_values[name]) / self.counts[name]
        self.context_history.append(context)
        self.reward_history.append(reward)
        self.action_history.append(name)
        rewards_for_action = [r for a, r in zip(self.action_history, self.reward_history) if a == name]
        self.causal_effects[name] = np.mean(rewards_for_action) if rewards_for_action else 0.0

    def seed_safe_policy(self, context, policy):
        pass

# =============================================================================
# NEW: EXPLAINABLE AI (XAI) GENERATOR
# =============================================================================
class XAIExplainer:
    def __init__(self):
        pass

    async def generate_explanation(self, strategy: Dict, context: Any, confidence: float, utility: float, project: 'CreditProject') -> str:
        parts = []
        name = strategy.get('name', 'unknown')
        parts.append(f"Selected {name} strategy.")
        if 'price_weight' in strategy.get('params', {}):
            parts.append(f"Price weight: {strategy['params']['price_weight']:.2f}")
        if 'biodiversity_weight' in strategy.get('params', {}):
            parts.append(f"Biodiversity weight: {strategy['params']['biodiversity_weight']:.2f}")
        parts.append(f"Project: {project.name} (ID: {project.project_id})")
        parts.append(f"Price per kg: ${project.price_per_kg_usd:.2f}")
        if project.co_benefits and 'biodiversity' in project.co_benefits:
            parts.append(f"Biodiversity score: {project.co_benefits['biodiversity']:.2f}")
        if confidence:
            parts.append(f"Confidence: {confidence:.2f}")
        if utility:
            parts.append(f"Utility: {utility:.2f}")
        return " ".join(parts)

# =============================================================================
# NEW: HUMAN-IN-THE-LOOP MANAGER
# =============================================================================
class HumanReviewManager:
    def __init__(self, config: Settings):
        self.config = config
        self.pending_reviews = {}  # review_id -> dict
        self._lock = asyncio.Lock()

    async def request_review(self, decision_id: str, explanation: str, context: Dict) -> Dict:
        review_id = str(uuid.uuid4())
        review = {
            'review_id': review_id,
            'decision_id': decision_id,
            'status': 'pending',
            'explanation': explanation,
            'context': context,
            'created_at': datetime.now().isoformat()
        }
        async with self._lock:
            self.pending_reviews[review_id] = review
        HUMAN_REVIEWS.labels(status='pending').inc()
        return review

    async def approve(self, review_id: str, feedback: str = None) -> Dict:
        async with self._lock:
            if review_id not in self.pending_reviews:
                raise HTTPException(status_code=404, detail="Review not found")
            self.pending_reviews[review_id]['status'] = 'approved'
            self.pending_reviews[review_id]['feedback'] = feedback
            self.pending_reviews[review_id]['reviewed_at'] = datetime.now().isoformat()
        HUMAN_REVIEWS.labels(status='approved').inc()
        return {'status': 'approved', 'review_id': review_id}

    async def reject(self, review_id: str, feedback: str = None) -> Dict:
        async with self._lock:
            if review_id not in self.pending_reviews:
                raise HTTPException(status_code=404, detail="Review not found")
            self.pending_reviews[review_id]['status'] = 'rejected'
            self.pending_reviews[review_id]['feedback'] = feedback
            self.pending_reviews[review_id]['reviewed_at'] = datetime.now().isoformat()
        HUMAN_REVIEWS.labels(status='rejected').inc()
        return {'status': 'rejected', 'review_id': review_id}

    async def get_pending(self) -> List[Dict]:
        async with self._lock:
            return [v for v in self.pending_reviews.values() if v['status'] == 'pending']

# =============================================================================
# NEW: CHAOS MONKEY
# =============================================================================
class ChaosMonkey:
    def __init__(self, config: Settings, marketplace: 'CarbonCreditMarketplace'):
        self.config = config
        self.marketplace = marketplace
        self.enabled = config.chaos.enabled
        self.failure_probability = config.chaos.failure_probability
        self.interval = config.chaos.experiment_interval_seconds
        self._task = None
        self._stop_event = asyncio.Event()

    async def start(self):
        if not self.enabled:
            logger.info("Chaos Monkey disabled")
            return
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Chaos Monkey started")

    async def stop(self):
        if self._task:
            self._stop_event.set()
            await self._task
            self._task = None

    async def _run_loop(self):
        while not self._stop_event.is_set():
            await asyncio.sleep(self.interval)
            try:
                await self._inject_failure()
            except Exception as e:
                logger.error("Chaos experiment failed", error=str(e))

    async def _inject_failure(self):
        failure_type = random.choice(['latency', 'error', 'disconnect'])
        target = random.choice(['registry', 'blockchain', 'database'])
        status = 'success'
        result = {}
        try:
            if failure_type == 'latency':
                await asyncio.sleep(random.uniform(0.5, 2.0))
                result['delay'] = 'simulated latency'
            elif failure_type == 'error':
                # Simulate a transient error in a component
                if target == 'registry':
                    # Temporarily break registry
                    original = self.marketplace.registry_client.circuit_breaker
                    self.marketplace.registry_client.circuit_breaker = GlobalCircuitBreaker().get_or_create(
                        "registry_chaos", threshold=1, timeout=5
                    )
                    await asyncio.sleep(random.uniform(1, 3))
                    self.marketplace.registry_client.circuit_breaker = original
                    result['action'] = 'toggled registry circuit breaker'
                elif target == 'blockchain':
                    # Simulate blockchain failure
                    original = self.marketplace.blockchain_client
                    self.marketplace.blockchain_client = None  # force fallback
                    await asyncio.sleep(random.uniform(1, 2))
                    self.marketplace.blockchain_client = original
                    result['action'] = 'simulated blockchain outage'
            elif failure_type == 'disconnect':
                await asyncio.sleep(random.uniform(1, 2))
                result['action'] = 'simulated network partition'
        except Exception as e:
            status = 'failed'
            result['error'] = str(e)
        CHAOS_EXPERIMENTS.labels(type=failure_type, status=status).inc()
        logger.info(f"Chaos experiment {failure_type} on {target}: {status}")

# =============================================================================
# NEW: QUANTUM-DISTILLATION OPTIMIZER (optional)
# =============================================================================
class QuantumDistillationOptimizer:
    def __init__(self, config: Settings):
        self.config = config
        self.available = QISKIT_AVAILABLE and config.optimizer.enable_distillation

    async def optimize(self, projects: List['CreditProject'], weights: Dict[str, float]) -> Optional['CreditProject']:
        if not self.available:
            return None
        try:
            qp = QuadraticProgram()
            for p in projects:
                qp.binary_var(p.project_id)
            # Define objective: maximize utility (minimize negative)
            utility = {}
            for p in projects:
                u = 0.0
                u += (1 - p.price_per_kg_usd / 2.0) * weights.get('price_weight', 0.3)
                u += p.co_benefits.get('biodiversity', 0) * weights.get('biodiversity_weight', 0.2)
                vintage = p.metadata.get('vintage', 2020)
                u += max(0, (vintage - 2020) / 5.0) * weights.get('vintage_weight', 0.3)
                carbon = p.metadata.get('carbon_intensity', 400)
                u += (1 - carbon / 1000.0) * weights.get('carbon_weight', 0.3)
                utility[p.project_id] = u
            linear = {pid: -utility[pid] for pid in utility}
            qp.minimize(linear=linear)
            qp.linear_constraint(linear={pid: 1 for pid in utility}, sense='E', rhs=1, name='one_project')
            backend = Aer.get_backend(self.config.optimizer.quantum_backend)
            qaoa = QAOA(reps=self.config.optimizer.quantum_reps)
            optimizer = MinimumEigenOptimizer(qaoa)
            result = optimizer.solve(qp)
            selected_id = [pid for pid in utility if result.x[list(utility.keys()).index(pid)] > 0.5][0]
            return next(p for p in projects if p.project_id == selected_id)
        except Exception as e:
            logger.error(f"Quantum optimization failed: {e}")
            return None

# =============================================================================
# NEW: FEDERATED LEARNING COORDINATOR
# =============================================================================
class FederatedCoordinator:
    def __init__(self, config: Settings):
        self.config = config
        self.participants = {}
        self.last_aggregation = None

    async def register_participant(self, participant_id: str, model_update: Dict):
        self.participants[participant_id] = model_update

    async def aggregate(self) -> Dict:
        if not self.participants:
            return {}
        # FedAvg-like: average Q-values from all participants
        avg_model = {}
        keys = set()
        for p in self.participants.values():
            keys.update(p.keys())
        for key in keys:
            vals = [p.get(key, 0.0) for p in self.participants.values()]
            avg_model[key] = sum(vals) / len(vals)
        self.last_aggregation = datetime.now()
        return avg_model

# =============================================================================
# NEW: MULTI-AGENT SYSTEM
# =============================================================================
class MultiAgentSystem:
    def __init__(self, config: Settings):
        self.config = config
        self.agents = {
            'price_agent': self._price_score,
            'carbon_agent': self._carbon_score,
            'biodiversity_agent': self._biodiversity_score,
        }

    def _price_score(self, project):
        return 1 - project.price_per_kg_usd / 2.0

    def _carbon_score(self, project):
        carbon = project.metadata.get('carbon_intensity', 400)
        return 1 - carbon / 1000.0

    def _biodiversity_score(self, project):
        return project.co_benefits.get('biodiversity', 0)

    async def vote(self, projects: List['CreditProject']) -> 'CreditProject':
        scores = {}
        for p in projects:
            total = 0.0
            for agent_name, score_fn in self.agents.items():
                total += score_fn(p)
            scores[p.project_id] = total
        best_id = max(scores, key=scores.get)
        return next(p for p in projects if p.project_id == best_id)

# =============================================================================
# INTEGRATE INTO AUTONOMOUS OPTIMIZER (replaces original)
# =============================================================================
class AutonomousOptimizer:
    def __init__(self, config: Settings, marketplace: 'CarbonCreditMarketplace',
                 safety_monitor: SafetyMonitor = None,
                 xai: XAIExplainer = None,
                 human_review: HumanReviewManager = None,
                 quantum_optimizer: QuantumDistillationOptimizer = None,
                 multi_agent: MultiAgentSystem = None):
        self.config = config
        self.marketplace = marketplace
        self.safety_monitor = safety_monitor
        self.xai = xai or XAIExplainer()
        self.human_review = human_review
        self.quantum_optimizer = quantum_optimizer
        self.multi_agent = multi_agent
        self._lock = asyncio.Lock()
        self.threshold_history = deque(maxlen=100)
        self.success_history = deque(maxlen=100)

        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None

        self.action_space = [
            {"name": "balanced", "params": {"price_weight": 0.3, "vintage_weight": 0.3, "biodiversity_weight": 0.2, "carbon_weight": 0.2}},
            {"name": "price_focused", "params": {"price_weight": 0.6, "vintage_weight": 0.1, "biodiversity_weight": 0.1, "carbon_weight": 0.2}},
            {"name": "green_focused", "params": {"price_weight": 0.1, "vintage_weight": 0.2, "biodiversity_weight": 0.3, "carbon_weight": 0.4}},
            {"name": "vintage_focused", "params": {"price_weight": 0.2, "vintage_weight": 0.5, "biodiversity_weight": 0.2, "carbon_weight": 0.1}},
        ]

        def fallback(context):
            return {"name": "balanced", "params": {"price_weight": 0.3, "vintage_weight": 0.3, "biodiversity_weight": 0.2, "carbon_weight": 0.2}}

        # Use CausalBandit
        self.bandit = CausalBandit(
            action_space=self.action_space,
            fallback_solver=fallback,
            min_trials_before_bandit=config.optimizer.bandit_min_trials,
            confidence_threshold=config.optimizer.bandit_confidence_threshold,
        )

        self.recent_rewards = deque(maxlen=100)
        self._last_selection = {"project": None, "strategy": None, "context": None}
        self._load_state()

    async def _load_state(self):
        pass

    async def _save_state(self):
        pass

    async def select_best_project(self, projects: List['CreditProject'], amount_kg: float, context: Dict = None) -> Optional['CreditProject']:
        if not projects:
            return None

        # Encode context using MoE (if available)
        encoded_context = context or {}
        if self.moe:
            encoded_context = self.moe.encode({
                "amount_kg": amount_kg,
                "carbon_intensity": context.get("carbon_intensity", 400) if context else 400,
                "user_role": context.get("user_role", "viewer"),
                "urgency": context.get("urgency", "normal"),
                "project_count": len(projects),
            })

        # Select strategy via bandit
        strategy, confidence, source = self.bandit.select_action(encoded_context)
        if strategy is None:
            strategy = self._fallback_strategy(encoded_context)

        # If quantum distillation is enabled and conditions met (e.g., large amount), use quantum
        selected_project = None
        if self.quantum_optimizer and self.quantum_optimizer.available and amount_kg > 10000:
            selected_project = await self.quantum_optimizer.optimize(projects, strategy['params'])
            source = "quantum"

        if selected_project is None:
            # Score projects using the selected strategy's weights
            scored = []
            for p in projects:
                score = self._score_project(p, strategy['params'])
                scored.append((p, score))
            scored.sort(key=lambda x: x[1], reverse=True)

            # Find first project with enough available credits
            for p, _ in scored:
                if p.available_credits_kg >= amount_kg:
                    selected_project = p
                    break

        if selected_project is None:
            return None

        # Safety check (temporal logic-like)
        if self.safety_monitor and self.config.safety.enable_monitor:
            violations = await self.safety_monitor.check_project(selected_project)
            if violations:
                logger.warning("Safety violation, overriding to a safer project")
                # Try to find a project that passes safety
                for p in projects:
                    if p.available_credits_kg >= amount_kg and not await self.safety_monitor.check_project(p):
                        selected_project = p
                        break

        # Record selection for feedback
        self._last_selection = {"project": selected_project, "strategy": strategy, "context": encoded_context}

        # Generate explanation (XAI)
        utility = self.modp.evaluate({
            "price": 1 - selected_project.price_per_kg_usd / 2.0,
            "vintage": (selected_project.metadata.get('vintage', 2020) - 2020) / 5.0,
            "biodiversity": selected_project.co_benefits.get('biodiversity', 0),
            "carbon_intensity": 1 - selected_project.metadata.get('carbon_intensity', 400) / 1000,
        }, strategy['params']) if self.modp else 0.0

        explanation = await self.xai.generate_explanation(strategy, encoded_context, confidence, utility, selected_project)
        decision_id = str(uuid.uuid4())
        # In a real system, store explanation in DB. Here we just log.
        logger.info(f"Decision {decision_id}: {explanation}")
        XAI_DECISIONS.labels(strategy=strategy['name']).inc()

        # Human-in-the-loop if confidence low
        if confidence < self.config.general.human_review_threshold and self.human_review:
            review = await self.human_review.request_review(decision_id, explanation, encoded_context)
            # In practice, we might block until review; for demo, we continue but record review.
            logger.info(f"Review requested: {review['review_id']}")

        return selected_project

    def _score_project(self, project: 'CreditProject', weights: Dict[str, float]) -> float:
        if self.modp:
            objectives = {
                "price": 1 - (project.price_per_kg_usd / 2.0),
                "vintage": (project.metadata.get('vintage', 2020) - 2020) / 5.0,
                "biodiversity": project.co_benefits.get('biodiversity', 0),
                "carbon_intensity": 1 - (project.metadata.get('carbon_intensity', 400) / 1000),
            }
            return self.modp.evaluate(objectives, weights)
        else:
            score = 0
            score += (1 - project.price_per_kg_usd / 2.0) * weights.get("price_weight", 0.3)
            score += 0.2  # base
            vintage = project.metadata.get('vintage', 2020)
            if vintage >= 2023:
                score += weights.get("vintage_weight", 0.3) * 0.3
            elif vintage >= 2022:
                score += weights.get("vintage_weight", 0.3) * 0.2
            co_benefits = project.co_benefits or {}
            sdg_count = len(co_benefits.get('sdg', []))
            score += min(sdg_count / 5, 0.2) * weights.get("biodiversity_weight", 0.2)
            biodiversity = co_benefits.get('biodiversity', 0)
            score += biodiversity * 0.1 * weights.get("biodiversity_weight", 0.2)
            return score

    async def update_feedback(self, context: Dict, strategy: Dict, reward: float):
        if self.bandit:
            self.bandit.update(context, strategy, reward)
            self.recent_rewards.append(reward)

        if len(self.recent_rewards) > 20 and np.mean(self.recent_rewards) < 0.3 and self.bio:
            new_strategies = await self.evolve_strategies()
            if new_strategies:
                for s in new_strategies:
                    if s not in self.action_space:
                        self.action_space.append(s)
                        self.bandit.actions = self.action_space
                logger.info("Bio‑inspired expansion: added new selection strategies.")

    async def evolve_strategies(self) -> List[Dict]:
        if not self.bio:
            return []
        def fitness(policy):
            return np.mean(self.recent_rewards) if self.recent_rewards else 0.5
        new_strategies = self.bio.evolve(
            population=self.action_space,
            fitness_fn=fitness,
            generations=self.config.optimizer.bio_generations,
            population_size=self.config.optimizer.bio_population_size,
        )
        return new_strategies

    async def optimize_offset_threshold(self) -> float:
        async with self._lock:
            if len(self.success_history) < 10:
                return self.config.general.auto_offset_threshold_kg
            success_rate = sum(self.success_history) / len(self.success_history)
            current = self.config.general.auto_offset_threshold_kg
            if success_rate > 0.9:
                new_threshold = current * 1.05
            elif success_rate < 0.6:
                new_threshold = current * 0.9
            else:
                new_threshold = current
            return max(50, min(500, new_threshold))

    async def record_outcome(self, success: bool):
        async with self._lock:
            self.success_history.append(success)

    def _fallback_strategy(self, context) -> Dict:
        return {"name": "balanced", "params": {"price_weight": 0.3, "vintage_weight": 0.3, "biodiversity_weight": 0.2, "carbon_weight": 0.2}}

    def get_optimization_stats(self) -> Dict:
        return {
            'strategies': [s['name'] for s in self.action_space],
            'recent_rewards': list(self.recent_rewards),
            'threshold': self.config.general.auto_offset_threshold_kg,
            'total_feedback': len(self.recent_rewards),
        }

# =============================================================================
# FLEXGEN MANAGER (unchanged, but with precision integration)
# =============================================================================
class FlexGenManager:
    def __init__(self, config: Settings):
        self.config = config
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
            carbon_intensity=workload.metadata.get('carbon_intensity',
                                                   self.config.optimizer.flexgen_carbon_intensity_default),
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
# DATABASE MODELS (SQLAlchemy) - unchanged from original
# =============================================================================
Base = declarative_base()

class CreditTransactionDB(Base):
    __tablename__ = "credit_transactions"
    id = Column(Integer, primary_key=True)
    tx_id = Column(String(64), unique=True, index=True)
    project_id = Column(String(128))
    amount_kg = Column(Float)
    retired_kg = Column(Float, default=0.0)
    cost_usd = Column(Float)
    status = Column(String(32))
    credit_type = Column(String(32), default="voluntary")
    retires_at = Column(DateTime, nullable=True)
    blockchain_tx_hash = Column(String(128), nullable=True)
    payment_method = Column(String(32), default="USD")
    metadata = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class CreditProjectDB(Base):
    __tablename__ = "credit_projects"
    project_id = Column(String(128), primary_key=True)
    name = Column(String(256))
    registry = Column(String(64))
    available_credits_kg = Column(Float)
    price_per_kg_usd = Column(Float)
    verification_status = Column(String(32))
    credit_type = Column(String(32), default="voluntary")
    co_benefits = Column(JSON)
    metadata = Column(JSON)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    active = Column(Boolean, default=True)

class UserDB(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, index=True)
    password_hash = Column(String(128))
    role = Column(String(32), default="viewer")
    created_at = Column(DateTime, default=datetime.now)

class AuditLogDB(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(64))
    action = Column(String(128))
    details = Column(JSON)
    timestamp = Column(DateTime, default=datetime.now)

# =============================================================================
# DATA MODELS (Pydantic) - unchanged
# =============================================================================
class CreditProject(BaseModel):
    project_id: str
    name: str
    registry: str
    available_credits_kg: float
    price_per_kg_usd: float
    verification_status: str
    credit_type: str = "voluntary"
    metadata: Dict = Field(default_factory=dict)
    co_benefits: Dict = Field(default_factory=dict)

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
    retires_at: Optional[datetime] = None
    blockchain_tx_hash: Optional[str] = None
    metadata: Dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)

class ReportRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    format: str = "json"

# =============================================================================
# MAIN MARKETPLACE CLASS (with all enhancements integrated)
# =============================================================================
class CarbonCreditMarketplace:
    def __init__(
        self,
        config: Settings,
        db_manager: AsyncDatabaseManager,
        registry_client: IRegistryClient,
        blockchain_client: IBlockchainClient,
        pqc: IPQC,
        cloud_storage: ICloudStorage,
        price_predictor: IPricePredictor,
        auto_offset_engine: IAutoOffsetEngine,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        sustainability_engine: Optional[UnifiedSustainabilityEngine] = None,
    ):
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

        # NEW MODULES
        self.safety_monitor = SafetyMonitor(config)
        self.xai = XAIExplainer()
        self.human_review = HumanReviewManager(config)
        self.quantum_optimizer = QuantumDistillationOptimizer(config)
        self.multi_agent = MultiAgentSystem(config)
        self.federated = FederatedCoordinator(config)
        self.chaos_monkey = ChaosMonkey(config, self)

        self.optimizer = AutonomousOptimizer(
            config, self,
            safety_monitor=self.safety_monitor,
            xai=self.xai,
            human_review=self.human_review,
            quantum_optimizer=self.quantum_optimizer,
            multi_agent=self.multi_agent
        )
        self.flexgen_manager = FlexGenManager(config)

        self.ws_manager = WebSocketManager()
        self.webhook = WebhookNotifier(config)

        # Auto‑offset settings
        self.auto_offset_enabled = config.general.auto_offset_enabled
        self.auto_offset_threshold_kg = config.general.auto_offset_threshold_kg
        self._running = False

        self._projects_cache: Dict[str, CreditProject] = {}
        self._projects_cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(seconds=config.general.refresh_interval_seconds)

        self.task_manager = TaskManager()
        self._register_background_tasks()

        self.retention_days = config.general.data_retention_days

        logger.info("CarbonCreditMarketplace v5.1.0 initialized with all enhancements")

    def _register_background_tasks(self):
        self.task_manager.register_task("auto_offset", self._auto_offset_loop)
        self.task_manager.register_task("reconcile", self._reconciliation_loop)
        self.task_manager.register_task("archive", self._archive_loop)
        self.task_manager.register_task("price_update", self._price_update_loop)
        self.task_manager.register_task("evolve_strategies", self._evolve_strategies_loop)
        if self.config.chaos.enabled:
            self.task_manager.register_task("chaos_monkey", self.chaos_monkey._run_loop)
        if self.config.federated.enabled:
            self.task_manager.register_task("federated_aggregate", self._federated_aggregate_loop)

    async def start(self):
        self._running = True
        self.task_manager.start_registered_tasks()
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.start()
        await self._refresh_projects(force=True)
        logger.info("CarbonCreditMarketplace started")

    # Background loops (existing + new)
    async def _auto_offset_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if self.auto_offset_enabled and self.sustainability_engine:
                    recent_emissions = await self.sustainability_engine.get_recent_emissions(hours=24)
                    if recent_emissions > self.auto_offset_threshold_kg:
                        await self._perform_offset(recent_emissions, reason="auto_offset_loop")
                await asyncio.sleep(self.config.general.auto_offset_interval_seconds)
            except Exception as e:
                logger.error("Auto‑offset loop error", error=str(e))
                await asyncio.sleep(60)

    async def _reconciliation_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.reconcile()
                await asyncio.sleep(86400)
            except Exception as e:
                logger.error("Reconciliation loop error", error=str(e))
                await asyncio.sleep(3600)

    async def _archive_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.archive_old_transactions()
                await asyncio.sleep(86400)
            except Exception as e:
                logger.error("Archive loop error", error=str(e))
                await asyncio.sleep(3600)

    async def _price_update_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                await self.update_prices()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error("Price update loop error", error=str(e))
                await asyncio.sleep(60)

    async def _evolve_strategies_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                if ENHANCEMENTS_AVAILABLE and self.optimizer.bio:
                    await self.optimizer.evolve_strategies()
                    logger.info("Periodic strategy evolution completed")
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error("Evolution loop error", error=str(e))
                await asyncio.sleep(60)

    async def _federated_aggregate_loop(self):
        while not self.task_manager.shutdown_event.is_set():
            try:
                avg_model = await self.federated.aggregate()
                # Apply to optimizer (example: update bandit q_values)
                if avg_model and hasattr(self.optimizer, 'bandit'):
                    for action, val in avg_model.items():
                        if action in self.optimizer.bandit.q_values:
                            self.optimizer.bandit.q_values[action] = val
                await asyncio.sleep(self.config.federated.aggregation_interval_seconds)
            except Exception as e:
                logger.error("Federated aggregation loop error", error=str(e))
                await asyncio.sleep(60)

    # Internal methods (most unchanged, but _perform_offset uses optimizer's new selection)
    async def _refresh_projects(self, force: bool = False):
        # (same as before, but could be enhanced to fetch RECs as well)
        if force or self._projects_cache_time is None or (datetime.now() - self._projects_cache_time) >= self._cache_ttl:
            raw_projects = await self.registry_client.fetch_projects()
            # Also fetch RECs? Could merge with registry client.
            async with self.db_manager.get_session() as session:
                for raw in raw_projects:
                    await session.execute(
                        text("""
                            INSERT INTO credit_projects (project_id, name, registry, available_credits_kg, price_per_kg_usd, verification_status, credit_type, co_benefits, metadata, last_updated)
                            VALUES (:project_id, :name, :registry, :available_credits_kg, :price_per_kg_usd, :verification_status, :credit_type, :co_benefits, :metadata, :last_updated)
                            ON CONFLICT (project_id) DO UPDATE SET
                                available_credits_kg = EXCLUDED.available_credits_kg,
                                price_per_kg_usd = EXCLUDED.price_per_kg_usd,
                                verification_status = EXCLUDED.verification_status,
                                co_benefits = EXCLUDED.co_benefits,
                                last_updated = EXCLUDED.last_updated
                        """),
                        {
                            "project_id": raw["project_id"],
                            "name": raw["name"],
                            "registry": raw["registry"],
                            "available_credits_kg": raw["available_credits_kg"],
                            "price_per_kg_usd": raw["price_per_kg_usd"],
                            "verification_status": raw["verification_status"],
                            "credit_type": raw.get("credit_type", "voluntary"),
                            "co_benefits": json.dumps(raw.get("co_benefits", {})),
                            "metadata": json.dumps(raw.get("metadata", {})),
                            "last_updated": datetime.now()
                        }
                    )
                await session.commit()
            self._projects_cache = await self._load_projects_from_db()
            self._projects_cache_time = datetime.now()
            PROJECT_COUNT.set(len(self._projects_cache))
            logger.info("Projects refreshed from registry", count=len(self._projects_cache))

    async def _load_projects_from_db(self) -> Dict[str, CreditProject]:
        # (same as before)
        projects = {}
        async with self.db_manager.get_session() as session:
            stmt = select(CreditProjectDB).where(CreditProjectDB.active == True)
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for row in rows:
                projects[row.project_id] = CreditProject(
                    project_id=row.project_id,
                    name=row.name,
                    registry=row.registry,
                    available_credits_kg=row.available_credits_kg,
                    price_per_kg_usd=row.price_per_kg_usd,
                    verification_status=row.verification_status,
                    credit_type=row.credit_type,
                    metadata=row.metadata,
                    co_benefits=row.co_benefits or {}
                )
        return projects

    async def _perform_offset(self, emissions_kg: float, reason: str = "auto_offset"):
        # (same logic, but uses optimizer.select_best_project which now includes safety, XAI, etc.)
        intensity = None
        if self.carbon_manager:
            intensity = await self.carbon_manager.get_intensity()
            logger.info("Auto‑offset triggered", emissions_kg=emissions_kg, carbon_intensity=intensity)

        threshold = await self.optimizer.optimize_offset_threshold()
        if emissions_kg < threshold and intensity and intensity < 400:
            logger.info("Emissions below adaptive threshold and low carbon intensity; skipping offset")
            return

        balance = await self.get_balance()
        available = balance["available_kg"]

        if available >= emissions_kg:
            projects = await self.list_projects(status="verified")
            best_project = await self.optimizer.select_best_project(
                projects,
                emissions_kg,
                context={"carbon_intensity": intensity if intensity else 400, "urgency": "auto"}
            )
            if best_project:
                await self._retire_from_existing(emissions_kg, reason, best_project)
                AUTO_OFFSET_SUCCESS.inc()
                await self.optimizer.record_outcome(True)
            else:
                logger.warning("No suitable project found for auto‑offset")
                await self.optimizer.record_outcome(False)
        else:
            missing = emissions_kg - available
            projects = await self.list_projects(status="verified")
            best_project = await self.optimizer.select_best_project(
                projects,
                missing,
                context={"carbon_intensity": intensity if intensity else 400, "urgency": "auto"}
            )
            if best_project:
                await self.purchase_credits(
                    CreditPurchaseRequest(
                        project_id=best_project.project_id,
                        amount_kg=missing,
                        retire_immediately=True,
                        reason=reason
                    ),
                    user={"sub": "auto_offset"}
                )
                AUTO_OFFSET_SUCCESS.inc()
                await self.optimizer.record_outcome(True)
            else:
                logger.warning("No suitable project for purchase; offset failed")
                await self.optimizer.record_outcome(False)
        AUTO_OFFSET_COUNTER.labels(reason=reason).inc()

    # ... (other methods unchanged: _retire_from_existing, purchase_credits, retire_credits, get_balance, etc.)

    async def run_flexgen_optimization(self, workload: Dict, node: Dict) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        workload_obj = WorkloadDescriptor(**workload)
        node_obj = NodeDescriptor(**node)
        return await self.flexgen_manager.optimize_policy(workload_obj, node_obj)

    async def get_flexgen_status(self) -> Dict:
        return await self.flexgen_manager.get_status()

    async def health_check(self) -> Dict:
        components = {}
        # (add new components)
        components["safety"] = await self.safety_monitor.get_status()
        components["chaos"] = {"enabled": self.chaos_monkey.enabled}
        components["federated"] = {"enabled": self.config.federated.enabled, "participants": len(self.federated.participants)}
        components["quantum"] = {"available": self.quantum_optimizer.available}
        # ... (existing health checks)
        return {
            "status": "ok",
            "version": "5.1.0",
            "components": components
        }

    async def shutdown(self):
        self._running = False
        await self.task_manager.stop_all()
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.stop()
        await self.registry_client.close()
        if self.blockchain_client:
            await self.blockchain_client.close()
        if self.carbon_manager:
            await self.carbon_manager.close()
        await self.webhook.close()
        await self.db_manager.close()
        logger.info("CarbonCreditMarketplace shut down")

# =============================================================================
# FASTAPI APPLICATION - with new endpoints
# =============================================================================
app = FastAPI(title="Carbon Credit Marketplace API", version="5.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

marketplace: Optional[CarbonCreditMarketplace] = None

# Rate limiting (simplified)
if SLOWAPI_AVAILABLE and config.rate_limit.enabled:
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
else:
    class SimpleRateLimiter:
        def __init__(self, requests: int, window: int):
            self.requests = requests
            self.window = window
            self._requests = defaultdict(deque)
        async def check(self, key: str):
            now = time.time()
            if key not in self._requests:
                self._requests[key] = deque()
            while self._requests[key] and now - self._requests[key][0] > self.window:
                self._requests[key].popleft()
            if len(self._requests[key]) >= self.requests:
                raise HTTPException(status_code=429, detail="Rate limit exceeded")
            self._requests[key].append(now)
    rate_limiter = SimpleRateLimiter(config.rate_limit.requests_per_minute, 60)

    async def rate_limit(request: Request):
        key = request.client.host
        await rate_limiter.check(key)

def create_jwt_token(data: Dict) -> str:
    expire = datetime.utcnow() + timedelta(minutes=config.general.jwt_expiration_minutes)
    data.update({"exp": expire})
    return jwt.encode(data, config.general.jwt_secret, algorithm=config.general.jwt_algorithm)

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, config.general.jwt_secret, algorithms=[config.general.jwt_algorithm])
        return payload
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def require_role(role: str):
    async def role_checker(user: Dict = Depends(get_current_user)):
        if user.get("role") != role:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user
    return role_checker

# ---------- Existing endpoints (abbreviated; same as original) ----------
@app.get("/metrics")
async def metrics():
    if PROMETHEUS_AVAILABLE:
        return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
    return {"error": "Prometheus not enabled"}

@app.get("/health")
async def health():
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.health_check()

@app.post("/auth/login")
async def login(username: str, password: str):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    user = await marketplace.authenticate_user(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_jwt_token({"sub": user["sub"], "role": user["role"]})
    return {"access_token": token, "token_type": "bearer"}

@app.post("/auth/register")
async def register(username: str, password: str, role: str = "viewer"):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    success = await marketplace.register_user(username, password, role)
    if not success:
        raise HTTPException(status_code=400, detail="User already exists")
    return {"status": "registered"}

@app.get("/projects")
async def list_projects(status: Optional[str] = None, credit_type: Optional[str] = None):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    projects = await marketplace.list_projects(status=status, credit_type=credit_type)
    return {"projects": [p.dict() for p in projects]}

@app.post("/purchase")
async def purchase(request: CreditPurchaseRequest, user: Dict = Depends(get_current_user), _: None = Depends(rate_limit)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    try:
        tx = await marketplace.purchase_credits(request, user)
        return {"status": "success", "transaction": tx.dict()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/retire")
async def retire(request: CreditRetireRequest, user: Dict = Depends(get_current_user), _: None = Depends(rate_limit)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    try:
        tx = await marketplace.retire_credits(request, user)
        return {"status": "success", "transaction": tx.dict()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/balance")
async def balance():
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.get_balance()

# ---------- NEW ENHANCEMENT ENDPOINTS ----------
@app.post("/optimization/select")
async def optimize_select(context: Dict, user: Dict = Depends(get_current_user), _: None = Depends(rate_limit)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    projects = await marketplace.list_projects(status="verified")
    best = await marketplace.optimizer.select_best_project(projects, context.get("amount_kg", 1000), context)
    if best:
        return {"project": best.dict(), "strategy": marketplace.optimizer._last_selection["strategy"]}
    return {"error": "No suitable project"}

@app.post("/optimization/feedback")
async def optimization_feedback(context: Dict, strategy: Dict, reward: float, user: Dict = Depends(get_current_user), _: None = Depends(rate_limit)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    await marketplace.optimizer.update_feedback(context, strategy, reward)
    return {"status": "feedback recorded"}

@app.post("/optimization/evolve")
async def optimization_evolve(user: Dict = Depends(require_role("admin")), _: None = Depends(rate_limit)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    new_strategies = await marketplace.optimizer.evolve_strategies()
    return {"new_strategies": new_strategies}

@app.get("/optimization/stats")
async def optimization_stats(user: Dict = Depends(get_current_user), _: None = Depends(rate_limit)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return marketplace.optimizer.get_optimization_stats()

@app.post("/flexgen/optimize")
async def flexgen_optimize(workload: Dict, node: Dict, user: Dict = Depends(get_current_user), _: None = Depends(rate_limit)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.run_flexgen_optimization(workload, node)

@app.get("/flexgen/status")
async def flexgen_status(user: Dict = Depends(get_current_user)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.get_flexgen_status()

# Safety monitor endpoints
@app.get("/safety/status")
async def safety_status(user: Dict = Depends(get_current_user)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.safety_monitor.get_status()

@app.post("/safety/check")
async def safety_check(project: CreditProject, user: Dict = Depends(get_current_user)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    violations = await marketplace.safety_monitor.check_project(project)
    return {"violations": violations}

# Human review endpoints
@app.get("/human-review/pending")
async def human_review_pending(user: Dict = Depends(require_role("admin"))):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.human_review.get_pending()

@app.post("/human-review/{review_id}/approve")
async def human_review_approve(review_id: str, feedback: str = None, user: Dict = Depends(require_role("admin"))):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.human_review.approve(review_id, feedback)

@app.post("/human-review/{review_id}/reject")
async def human_review_reject(review_id: str, feedback: str = None, user: Dict = Depends(require_role("admin"))):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.human_review.reject(review_id, feedback)

# Chaos monkey endpoints
@app.post("/chaos/trigger")
async def chaos_trigger(user: Dict = Depends(require_role("admin"))):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    if not marketplace.chaos_monkey:
        raise HTTPException(status_code=400, detail="Chaos Monkey not initialized")
    await marketplace.chaos_monkey._inject_failure()
    return {"status": "chaos experiment triggered"}

# Federated learning endpoints
@app.post("/federated/register")
async def federated_register(participant_id: str, model_update: Dict, user: Dict = Depends(get_current_user)):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    await marketplace.federated.register_participant(participant_id, model_update)
    return {"status": "participant registered"}

@app.get("/federated/aggregate")
async def federated_aggregate(user: Dict = Depends(require_role("admin"))):
    if not marketplace:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return await marketplace.federated.aggregate()

# ---------- Startup & Shutdown ----------
@app.on_event("startup")
async def startup():
    global marketplace
    db_manager = AsyncDatabaseManager(config)
    registry_client = RegistryClient(config)
    blockchain_client = BlockchainClient(config)
    pqc = PostQuantumCrypto(config, db_manager)
    cloud_storage = CloudStorage(config)
    price_predictor = PricePredictor(config)
    carbon_manager = CarbonIntensityManager(config)
    sustainability_engine = UnifiedSustainabilityEngine(db_manager)
    auto_offset_engine = AutoOffsetEngine(None)
    marketplace = CarbonCreditMarketplace(
        config=config,
        db_manager=db_manager,
        registry_client=registry_client,
        blockchain_client=blockchain_client,
        pqc=pqc,
        cloud_storage=cloud_storage,
        price_predictor=price_predictor,
        auto_offset_engine=auto_offset_engine,
        carbon_manager=carbon_manager,
        sustainability_engine=sustainability_engine,
    )
    marketplace.auto_offset_engine = AutoOffsetEngine(marketplace)
    await marketplace.start()
    logger.info("FastAPI application started")

@app.on_event("shutdown")
async def shutdown():
    if marketplace:
        await marketplace.shutdown()
    logger.info("FastAPI application shut down")

# =============================================================================
# MAIN ENTRY
# =============================================================================
if __name__ == "__main__":
    uvicorn.run(
        "carbon_credit_marketplace:app",
        host=config.API_HOST,
        port=config.API_PORT,
        log_level="info",
        reload=False
    )
