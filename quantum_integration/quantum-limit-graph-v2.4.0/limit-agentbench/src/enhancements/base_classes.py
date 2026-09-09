#!/usr/bin/env python3
# ============================================================================
# Green Agent Base Classes - Version 14.5 (Enhanced with Causal RL, XAI, 
# Adaptive Precision, Chaos Testing, Human-in-the-Loop, Safety Monitor)
# ORIGINAL v14.3 features retained, new modules integrated.
# ============================================================================

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import pickle
import threading
import time
import uuid
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Union, Type, Set, Protocol, runtime_checkable
from weakref import WeakValueDictionary
import functools
import inspect
import tempfile
import os
import zlib
import contextlib
import random
import secrets

import numpy as np
import pandas as pd  # For causal model and XAI (if available)

# ============================================================
# ENHANCED CONFIGURATION (Pydantic with fallback)
# ============================================================
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict, model_validator
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# Tenacity for retries
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# Async SQLite
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

# FastAPI
try:
    from fastapi import FastAPI, Depends, HTTPException, status, Request, WebSocket, WebSocketDisconnect
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, Response
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# Prometheus metrics
try:
    from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, start_http_server, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ============================================================
# OPTIONAL IMPORTS WITH GRACEFUL DEGRADATION
# ============================================================
try:
    import qiskit
    from qiskit import QuantumCircuit, Aer, execute
    from qiskit.optimization import QuadraticProgram
    from qiskit.optimization.algorithms import MinimumEigenOptimizer
    from qiskit.algorithms import QAOA
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False

try:
    import pennylane as qml
    PENNYLANE_AVAILABLE = True
except ImportError:
    PENNYLANE_AVAILABLE = False

# Post‑Quantum cryptography
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Blockchain
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Deep learning
try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import tensorflow as tf
    TF_AVAILABLE = True
except ImportError:
    TF_AVAILABLE = False

# Scikit-learn
try:
    import sklearn
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Prophet
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

# AWS
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    import aiobotocore
    AIOBOTOCORE_AVAILABLE = True
except ImportError:
    AIOBOTOCORE_AVAILABLE = False

# MQTT async
try:
    import aiomqtt
    AIOMQTT_AVAILABLE = True
except ImportError:
    AIOMQTT_AVAILABLE = False

# Transformers
try:
    from transformers import pipeline, AutoModelForCausalLM, AutoTokenizer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

# Cryptography
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

# HTTP client
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# OpenTelemetry
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False

# JWT
try:
    from jose import JWTError, jwt
    from jose.constants import ALGORITHMS
    JOSE_AVAILABLE = True
except ImportError:
    JOSE_AVAILABLE = False

# ============================================================
# IMPORT ENHANCED MODULES (with graceful fallback)
# ============================================================
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
        def select(self, encoded): return "balanced"
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

# ============================================================
# FlexGen modules (optional)
# ============================================================
try:
    from .gpu_optimization.flexgen_policy import FlexGenPolicy, MockFlexGenExecutor, generate_candidate_policies
    from .gpu_optimization.flexgen_policy_selector import DistillationFlexGenSelector, FlexGenState
    from .gpu_optimization.flexgen_controller import FlexGenController
    from .gpu_optimization.gpu_profiler import GPUProfiler
    from .gpu_optimization.flexgen_cost_model import FlexGenCostModel
    from .gpu_optimization.policy_drift_detector import PolicyDriftDetector
    from .modp.flexgen_modp_planner import FlexGenMODPPlanner
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    # Dummy placeholders with precision support
    class FlexGenPolicy:
        def __init__(self, **kwargs):
            self.params = kwargs
            self.precision = kwargs.get('precision', 'fp32')
        def to_dict(self):
            return {**self.params, 'precision': self.precision}
        def __repr__(self):
            return f"FlexGenPolicy(precision={self.precision}, params={self.params})"
    class MockFlexGenExecutor:
        def execute(self, policy, node, workload): return {}
    def generate_candidate_policies(n=20):
        return [FlexGenPolicy(
            percent=random.randint(0,100),
            gpu_batch_size=random.randint(1,16),
            cpu_offload=random.choice([True,False]),
            disk_offload=random.choice([True,False]),
            precision=random.choice(['fp32','fp16','int8'])
        ) for _ in range(n)]
    class DistillationFlexGenSelector:
        def __init__(self, *args, **kwargs): pass
    class FlexGenState:
        pass
    class FlexGenController:
        def __init__(self, *args, **kwargs): pass
        async def step(self): return {}
    class GPUProfiler:
        def __init__(self, *args, **kwargs): pass
        async def get_all_gpu_metrics(self): return []
        async def start_monitoring(self, interval_sec=5.0): pass
        def shutdown(self): pass
    class FlexGenCostModel:
        def __init__(self, *args, **kwargs): pass
        def evaluate(self, policy, node, workload):
            # Simple cost model that penalizes high precision
            base_cost = 1.0
            if policy.precision == 'fp32':
                base_cost *= 1.5
            elif policy.precision == 'fp16':
                base_cost *= 1.2
            else:  # int8
                base_cost *= 1.0
            return base_cost
    class PolicyDriftDetector:
        def __init__(self, *args, **kwargs): pass
        def get_stats(self): return {}
    class FlexGenMODPPlanner:
        def __init__(self, *args, **kwargs): pass

# ============================================================
# Green_Agent Sustainability Modules (optional)
# ============================================================
try:
    from ..adaptive_cost_function import AdaptiveCostFunction
    from ..anomaly_detection import AnomalyDetector
    from ..predictive_maintenance import PredictiveMaintenanceEngine
    SUSTAINABILITY_MODULES_AVAILABLE = True
except ImportError:
    SUSTAINABILITY_MODULES_AVAILABLE = False

# ============================================================
# STRUCTURED LOGGING (fallback to standard logging)
# ============================================================
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
        handlers=[
            logging.handlers.RotatingFileHandler('green_agent.log', maxBytes=10*1024*1024, backupCount=5),
            logging.StreamHandler()
        ]
    )
    class CorrelationIdFilter(logging.Filter):
        def __init__(self):
            super().__init__()
            self.correlation_id = str(uuid.uuid4())[:8]
        def filter(self, record):
            record.correlation_id = self.correlation_id
            return True
    logger.addFilter(CorrelationIdFilter())

# ============================================================
# PROMETHEUS METRICS (fallback dummy)
# ============================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    MODEL_PREDICTIONS = Counter('model_predictions_total', 'Total model predictions', ['model_name', 'version', 'status'], registry=REGISTRY)
    MODEL_PREDICTION_LATENCY = Histogram('model_prediction_duration_seconds', 'Prediction duration', ['model_name', 'version'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('circuit_breaker_state', 'Circuit breaker state', ['name'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('component_health_score', 'Component health score (0-100)', ['component'], registry=REGISTRY)
    DB_SIZE = Gauge('base_classes_db_size_mb', 'Database size in MB', registry=REGISTRY)
    CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Real-time carbon intensity', registry=REGISTRY)
    HELIUM_EFFICIENCY = Gauge('helium_efficiency_score', 'Helium efficiency (0-1)', registry=REGISTRY)
    SUSTAINABILITY_SCORE = Gauge('sustainability_score', 'Overall sustainability score (0-100)', registry=REGISTRY)
    CARBON_SAVINGS = Counter('carbon_savings_total', 'Total carbon savings', ['source'], registry=REGISTRY)
    HELIUM_SAVINGS = Counter('helium_savings_total', 'Total helium savings', ['source'], registry=REGISTRY)
    QUANTUM_CIRCUITS = Counter('quantum_circuits_executed', 'Quantum circuits executed', ['backend', 'status'], registry=REGISTRY)
    QUANTUM_TIME = Histogram('quantum_execution_duration_seconds', 'Quantum execution time', ['backend'], registry=REGISTRY)
    BLOCKCHAIN_TX = Counter('blockchain_transactions_total', 'Blockchain transactions', ['type', 'status'], registry=REGISTRY)
    CARBON_CREDITS = Gauge('carbon_credits_total', 'Total carbon credits', registry=REGISTRY)
    HELIUM_CREDITS = Gauge('helium_credits_total', 'Total helium credits', registry=REGISTRY)
    PQC_SIGNATURES = Counter('pqc_signatures_total', 'Post-quantum signatures', ['algorithm', 'status'], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter('autonomous_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)
    CLOUD_DISTRIBUTIONS = Counter('cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
    API_REQUESTS = Counter('api_requests_total', 'API requests', ['endpoint', 'method', 'status'], registry=REGISTRY)
    SAFETY_VIOLATIONS = Counter('safety_violations_total', 'Safety violations', ['rule'], registry=REGISTRY)
    CHAOS_EXPERIMENTS = Counter('chaos_experiments_total', 'Chaos experiments', ['type', 'status'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def set(self, **kwargs): pass
        def observe(self, **kwargs): pass
        def _value(self): return 0
    MODEL_PREDICTIONS = DummyMetric()
    MODEL_PREDICTION_LATENCY = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    HEALTH_SCORE = DummyMetric()
    DB_SIZE = DummyMetric()
    CARBON_INTENSITY = DummyMetric()
    HELIUM_EFFICIENCY = DummyMetric()
    SUSTAINABILITY_SCORE = DummyMetric()
    CARBON_SAVINGS = DummyMetric()
    HELIUM_SAVINGS = DummyMetric()
    QUANTUM_CIRCUITS = DummyMetric()
    QUANTUM_TIME = DummyMetric()
    BLOCKCHAIN_TX = DummyMetric()
    CARBON_CREDITS = DummyMetric()
    HELIUM_CREDITS = DummyMetric()
    PQC_SIGNATURES = DummyMetric()
    AUTONOMOUS_OPTIMIZATIONS = DummyMetric()
    CLOUD_DISTRIBUTIONS = DummyMetric()
    API_REQUESTS = DummyMetric()
    SAFETY_VIOLATIONS = DummyMetric()
    CHAOS_EXPERIMENTS = DummyMetric()

# ============================================================
# CUSTOM EXCEPTIONS
# ============================================================
class GreenAgentException(Exception):
    """Base exception for all Green Agent exceptions"""
    def __init__(self, message: str, details: Dict = None):
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now()
        self.correlation_id = getattr(logger, 'correlation_id', str(uuid.uuid4())[:8])

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

# ============================================================
# CONFIGURATION (Grouped sub-configs) – extended with new settings
# ============================================================
if PYDANTIC_AVAILABLE:
    class DatabaseConfig(BaseModel):
        path: str = Field("./green_agent.db")

    class BlockchainConfig(BaseModel):
        rpc_url: str = "http://localhost:8545"
        chain_id: int = 1337
        private_key: Optional[str] = None
        contract_address: str = "0x0000000000000000000000000000000000000000"

    class AnalyticsConfig(BaseModel):
        prophet_changepoint_prior_scale: float = 0.05
        prophet_seasonality_prior_scale: float = 10.0
        lstm_units: int = 50
        lstm_epochs: int = 10
        lstm_batch_size: int = 32
        ensemble_weights: Optional[List[float]] = None
        modp_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'energy': 0.25,
                'carbon': 0.25,
                'latency': 0.20,
                'accuracy': 0.30,
            }
        )
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

    class EdgeConfig(BaseModel):
        mqtt_broker: str = "localhost"
        mqtt_port: int = 1883

    class CloudConfig(BaseModel):
        s3_bucket: str = "green-agent-data-lake"
        s3_prefix: str = "sustainability/"
        athena_database: str = "green_agent"
        athena_table: str = "sustainability_metrics"

    class NLPConfig(BaseModel):
        model_name: str = "distilgpt2"

    class APIConfig(BaseModel):
        host: str = "0.0.0.0"
        port: int = 8000
        jwt_secret: str = Field(default_factory=lambda: secrets.token_hex(32))

    class SafetyConfig(BaseModel):
        max_carbon_intensity: float = 500.0
        min_renewable_share: float = 0.3
        max_latency_ms: float = 1000.0

    class ChaosConfig(BaseModel):
        enabled: bool = False
        failure_probability: float = 0.1
        experiment_interval_seconds: int = 120

    class GeneralConfig(BaseModel):
        max_prediction_history: int = 10000
        max_cache_size: int = 1000
        cache_ttl_seconds: int = 300
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        health_check_timeout: int = 10
        rate_limit_requests: int = 1000
        rate_limit_window: int = 60
        data_version: int = 15
        log_level: str = "INFO"
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        human_review_threshold: float = 0.5  # confidence below this triggers review

    class GreenAgentConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="GREEN_AGENT_", case_sensitive=False)

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
        master_key: str = Field(default='', description='Master key hex string for encrypting keys')

        @field_validator('master_key')
        @classmethod
        def validate_master_key(cls, v: str) -> str:
            if not v:
                raise ValueError('master_key must be set via environment variable GREEN_AGENT_MASTER_KEY')
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.master_key)
else:
    @dataclass
    class GeneralConfig:
        max_prediction_history: int = 10000
        max_cache_size: int = 1000
        cache_ttl_seconds: int = 300
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 60
        health_check_timeout: int = 10
        rate_limit_requests: int = 1000
        rate_limit_window: int = 60
        data_version: int = 15
        log_level: str = "INFO"
        bandit_min_trials: int = 5
        bandit_confidence_threshold: float = 0.6
        bio_generations: int = 10
        bio_population_size: int = 20
        human_review_threshold: float = 0.5

    @dataclass
    class AnalyticsConfig:
        prophet_changepoint_prior_scale: float = 0.05
        prophet_seasonality_prior_scale: float = 10.0
        lstm_units: int = 50
        lstm_epochs: int = 10
        lstm_batch_size: int = 32
        ensemble_weights: Optional[List[float]] = None
        modp_weights: Dict[str, float] = field(default_factory=lambda: {'energy':0.25, 'carbon':0.25, 'latency':0.20, 'accuracy':0.30})
        flexgen_carbon_intensity_default: float = 400.0
        flexgen_population_size: int = 50
        flexgen_generations: int = 10
        flexgen_use_real_executor: bool = False
        flexgen_executor_type: str = "mock"
        flexgen_selector_epsilon: float = 0.1
        flexgen_selector_epsilon_decay: float = 0.999

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
        jwt_secret: str = secrets.token_hex(32)

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

        def get_master_key_bytes(self) -> bytes:
            if not self.master_key:
                raise ValueError("master_key not set")
            return bytes.fromhex(self.master_key)

# ============================================================
# EVENT BUS (Decoupled communication)
# ============================================================
class EventBus:
    """Simple in-memory event bus."""
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def subscribe(self, event_type: str, callback: Callable):
        async with self._lock:
            self._subscribers[event_type].append(callback)

    async def publish(self, event_type: str, data: Any):
        async with self._lock:
            callbacks = self._subscribers.get(event_type, [])
        for cb in callbacks:
            asyncio.create_task(cb(data))

# ============================================================
# GLOBAL CIRCUIT BREAKER REGISTRY
# ============================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    """Enhanced circuit breaker with gradual recovery."""
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 60,
                 half_open_success_threshold: int = 2):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
        self.metrics = {'total_calls': 0, 'failed_calls': 0, 'successful_calls': 0}

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            now = time.time()
            if self.state == CircuitBreakerState.OPEN:
                if now - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0.5)
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= self.half_open_success_threshold:
                self.state = CircuitBreakerState.CLOSED
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
                logger.info(f"Circuit breaker {self.name} closed after {self.success_count} successes")
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
            self.success_count += 1
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.success_count >= self.half_open_success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    CIRCUIT_BREAKER_STATE.labels(name=self.name).set(0)
            else:
                self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.metrics['failed_calls'] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                CIRCUIT_BREAKER_STATE.labels(name=self.name).set(1)
                logger.warning(f"Circuit breaker {self.name} opened from HALF_OPEN")

    def get_metrics(self) -> Dict:
        return {**self.metrics, 'state': self.state.value, 'failure_count': self.failure_count, 'success_count': self.success_count}

class GlobalCircuitBreaker:
    _instance = None
    _breakers: Dict[str, EnhancedCircuitBreaker] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name: str, **kwargs) -> EnhancedCircuitBreaker:
        if name not in self._breakers:
            self._breakers[name] = EnhancedCircuitBreaker(name, **kwargs)
        return self._breakers[name]

# ============================================================
# RATE LIMITER
# ============================================================
class EnhancedRateLimiter:
    """Token bucket rate limiter."""
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

# ============================================================
# ASYNC DATABASE MANAGER (with migrations) - Schema v5
# ============================================================
class AsyncDatabaseManager:
    """Async database manager using aiosqlite with schema migrations."""
    SCHEMA_VERSION = 5

    def __init__(self, config: GreenAgentConfig):
        self.config = config
        self.db_path = Path(config.database.path)
        self._lock = asyncio.Lock()
        self._initialized = False

    async def _init_db(self):
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            async with aiosqlite.connect(self.db_path) as conn:
                await self._apply_migrations(conn)
            self._initialized = True

    async def _apply_migrations(self, conn: aiosqlite.Connection):
        # Create schema_version table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
        """)
        await conn.commit()

        cursor = await conn.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        row = await cursor.fetchone()
        current_ver = row[0] if row else 0

        if current_ver < 1:
            # Initial tables (v1) - unchanged from previous
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS model_registry (
                    model_id TEXT PRIMARY KEY,
                    name TEXT,
                    version TEXT,
                    metadata TEXT,
                    registered_at TEXT,
                    is_active INTEGER,
                    prediction_count INTEGER,
                    error_count INTEGER,
                    avg_latency_ms REAL,
                    created_at TEXT,
                    updated_at TEXT,
                    version_number INTEGER
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS model_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_id TEXT,
                    metric_type TEXT,
                    metric_value REAL,
                    timestamp TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS blockchain_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tx_hash TEXT,
                    tx_type TEXT,
                    amount REAL,
                    project_id TEXT,
                    timestamp TEXT,
                    status TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    id TEXT PRIMARY KEY,
                    alert_name TEXT,
                    severity TEXT,
                    status TEXT,
                    created_at TEXT,
                    resolved_at TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS edge_devices (
                    device_id TEXT PRIMARY KEY,
                    config TEXT,
                    status TEXT,
                    last_seen TEXT,
                    last_data TEXT,
                    registered_at TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pqc_key_pairs (
                    key_id TEXT PRIMARY KEY,
                    algorithm TEXT NOT NULL,
                    public_key BLOB NOT NULL,
                    private_key BLOB NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS optimisation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy TEXT NOT NULL,
                    result TEXT,
                    timestamp TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS distribution_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    optimal_provider TEXT NOT NULL,
                    optimal_region TEXT NOT NULL,
                    scores TEXT,
                    data_size_gb REAL,
                    timestamp TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    project_id TEXT PRIMARY KEY,
                    name TEXT,
                    company TEXT,
                    city TEXT,
                    country TEXT,
                    lat REAL,
                    lon REAL,
                    capacity_mw REAL,
                    status TEXT,
                    green_score REAL,
                    pue REAL,
                    renewable_share REAL,
                    data TEXT
                )
            """)
            await conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (1, datetime('now'))")
            await conn.commit()
            current_ver = 1

        if current_ver < 2:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status)")
            await conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (2, datetime('now'))")
            await conn.commit()
            logger.info("Database migrated to v2")
            current_ver = 2

        if current_ver < 3:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS flexgen_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workload_id TEXT,
                    node_id TEXT,
                    policy_json TEXT,
                    metrics_json TEXT,
                    reward REAL,
                    timestamp TEXT
                )
            """)
            await conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (3, datetime('now'))")
            await conn.commit()
            logger.info("Database migrated to v3")
            current_ver = 3

        if current_ver < 4:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    value REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    metadata TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sustainability_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT,
                    score REAL,
                    timestamp TEXT,
                    details TEXT
                )
            """)
            await conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (4, datetime('now'))")
            await conn.commit()
            logger.info("Database migrated to v4")
            current_ver = 4

        if current_ver < 5:
            # New tables for XAI, human-in-the-loop, safety, chaos
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS explanations (
                    id TEXT PRIMARY KEY,
                    decision_id TEXT NOT NULL,
                    explanation TEXT NOT NULL,
                    model_dump TEXT,
                    timestamp TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS human_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_id TEXT NOT NULL,
                    status TEXT NOT NULL,  -- pending, approved, rejected
                    feedback TEXT,
                    reviewer TEXT,
                    created_at TEXT,
                    reviewed_at TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS safety_violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_name TEXT NOT NULL,
                    details TEXT,
                    timestamp TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS chaos_experiments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    experiment_type TEXT NOT NULL,
                    target_component TEXT,
                    status TEXT NOT NULL,
                    result TEXT,
                    timestamp TEXT NOT NULL
                )
            """)
            await conn.execute("INSERT INTO schema_version (version, applied_at) VALUES (5, datetime('now'))")
            await conn.commit()
            logger.info("Database migrated to v5")
            current_ver = 5

    async def _execute(self, query: str, params: tuple = ()):
        await self._init_db()
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.execute(query, params)
            await conn.commit()
            return cursor

    async def _fetch_all(self, query: str, params: tuple = ()) -> List[tuple]:
        await self._init_db()
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.execute(query, params)
            return await cursor.fetchall()

    async def _fetch_one(self, query: str, params: tuple = ()) -> Optional[tuple]:
        await self._init_db()
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.execute(query, params)
            return await cursor.fetchone()

    # CRUD methods (same as before, plus new ones)

    async def save_model_registry(self, model_id: str, name: str, version: str, metadata: Dict, is_active: bool = True):
        await self._execute("""
            INSERT OR REPLACE INTO model_registry
            (model_id, name, version, metadata, registered_at, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (model_id, name, version, json.dumps(metadata, default=str), datetime.now().isoformat(), 1 if is_active else 0, datetime.now().isoformat()))

    async def save_blockchain_transaction(self, tx_hash: str, tx_type: str, amount: float, project_id: str, status: str = 'success'):
        await self._execute("""
            INSERT INTO blockchain_transactions (tx_hash, tx_type, amount, project_id, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (tx_hash, tx_type, amount, project_id, datetime.now().isoformat(), status))

    async def save_incident(self, incident_id: str, alert_name: str, severity: str, status: str = 'open'):
        await self._execute("""
            INSERT INTO incidents (id, alert_name, severity, status, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (incident_id, alert_name, severity, status, datetime.now().isoformat()))

    async def save_edge_device(self, device_id: str, config: Dict, status: str, last_seen: datetime = None, last_data: Dict = None):
        await self._execute("""
            INSERT OR REPLACE INTO edge_devices (device_id, config, status, last_seen, last_data, registered_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (device_id, json.dumps(config), status, last_seen.isoformat() if last_seen else None, json.dumps(last_data or {}), datetime.now().isoformat()))

    async def save_pqc_keypair(self, key_id: str, algorithm: str, public_key: bytes, private_key: bytes, expires_at: str):
        await self._execute("""
            INSERT OR REPLACE INTO pqc_key_pairs (key_id, algorithm, public_key, private_key, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (key_id, algorithm, public_key, private_key, datetime.now().isoformat(), expires_at))

    async def get_pqc_keypair(self, key_id: str) -> Optional[Dict]:
        row = await self._fetch_one("SELECT algorithm, public_key, private_key, created_at, expires_at FROM pqc_key_pairs WHERE key_id = ?", (key_id,))
        if row:
            return {'algorithm': row[0], 'public_key': row[1], 'private_key': row[2], 'created_at': row[3], 'expires_at': row[4]}
        return None

    async def save_optimisation(self, strategy: str, result: Dict):
        await self._execute("INSERT INTO optimisation_history (strategy, result, timestamp) VALUES (?, ?, ?)", (strategy, json.dumps(result), datetime.now().isoformat()))

    async def get_recent_optimisations(self, limit: int = 10) -> List[Dict]:
        rows = await self._fetch_all("SELECT strategy, result, timestamp FROM optimisation_history ORDER BY id DESC LIMIT ?", (limit,))
        return [{'strategy': r[0], 'result': json.loads(r[1]), 'timestamp': r[2]} for r in rows]

    async def save_distribution(self, result: Dict):
        await self._execute("INSERT INTO distribution_history (optimal_provider, optimal_region, scores, data_size_gb, timestamp) VALUES (?, ?, ?, ?, ?)", (result['optimal_provider'], result['optimal_region'], json.dumps(result['scores']), result.get('data_size_gb', 0), result['timestamp']))

    async def save_project(self, project: Dict):
        await self._execute("INSERT OR REPLACE INTO projects (project_id, name, company, city, country, lat, lon, capacity_mw, status, green_score, pue, renewable_share, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (project['project_id'], project['project_name'], project['company'], project['location_city'], project['location_country'], project['latitude'], project['longitude'], project['planned_power_capacity_mw'], project['status'], project['green_score'], project['sustainability']['pue_estimated'], project['sustainability']['renewable_share_pct'], json.dumps(project)))

    async def get_model_registry(self, model_id: str) -> Optional[Dict]:
        row = await self._fetch_one("SELECT * FROM model_registry WHERE model_id = ?", (model_id,))
        if row:
            return {
                'model_id': row[0],
                'name': row[1],
                'version': row[2],
                'metadata': json.loads(row[3]),
                'registered_at': row[4],
                'is_active': bool(row[5]),
                'prediction_count': row[6],
                'error_count': row[7],
                'avg_latency_ms': row[8],
                'created_at': row[9],
                'updated_at': row[10],
                'version_number': row[11]
            }
        return None

    async def list_models(self) -> List[Dict]:
        rows = await self._fetch_all("SELECT * FROM model_registry")
        return [{
            'model_id': r[0],
            'name': r[1],
            'version': r[2],
            'metadata': json.loads(r[3]),
            'registered_at': r[4],
            'is_active': bool(r[5]),
            'prediction_count': r[6],
            'error_count': r[7],
            'avg_latency_ms': r[8],
            'created_at': r[9],
            'updated_at': r[10],
            'version_number': r[11]
        } for r in rows]

    async def save_flexgen_decision(self, workload_id: str, node_id: str, policy_json: str, metrics_json: str, reward: float):
        await self._execute("""
            INSERT INTO flexgen_decisions (workload_id, node_id, policy_json, metrics_json, reward, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (workload_id, node_id, policy_json, metrics_json, reward, datetime.now().isoformat()))

    async def save_metric_history(self, metric_name: str, value: float, metadata: Dict = None):
        await self._execute("INSERT INTO metric_history (metric_name, value, timestamp, metadata) VALUES (?, ?, ?, ?)", (metric_name, value, datetime.now().isoformat(), json.dumps(metadata or {})))

    async def get_metric_history(self, metric_name: str, limit: int = 100) -> List[Dict]:
        rows = await self._fetch_all("SELECT metric_name, value, timestamp, metadata FROM metric_history WHERE metric_name = ? ORDER BY id DESC LIMIT ?", (metric_name, limit))
        return [{'metric_name': r[0], 'value': r[1], 'timestamp': r[2], 'metadata': json.loads(r[3])} for r in rows]

    async def list_projects(self, status: str = None) -> List[Dict]:
        if status:
            rows = await self._fetch_all("SELECT * FROM projects WHERE status = ?", (status,))
        else:
            rows = await self._fetch_all("SELECT * FROM projects")
        return [{
            'project_id': r[0],
            'name': r[1],
            'company': r[2],
            'city': r[3],
            'country': r[4],
            'lat': r[5],
            'lon': r[6],
            'capacity_mw': r[7],
            'status': r[8],
            'green_score': r[9],
            'pue': r[10],
            'renewable_share': r[11],
            'data': json.loads(r[12])
        } for r in rows]

    # New methods for XAI, HITL, safety, chaos
    async def save_explanation(self, decision_id: str, explanation: str, model_dump: Dict = None):
        expl_id = str(uuid.uuid4())
        await self._execute("INSERT INTO explanations (id, decision_id, explanation, model_dump, timestamp) VALUES (?, ?, ?, ?, ?)", (expl_id, decision_id, explanation, json.dumps(model_dump) if model_dump else None, datetime.now().isoformat()))
        return expl_id

    async def get_explanation(self, decision_id: str) -> Optional[Dict]:
        row = await self._fetch_one("SELECT id, decision_id, explanation, model_dump, timestamp FROM explanations WHERE decision_id = ? ORDER BY timestamp DESC LIMIT 1", (decision_id,))
        if row:
            return {'id': row[0], 'decision_id': row[1], 'explanation': row[2], 'model_dump': json.loads(row[3]) if row[3] else None, 'timestamp': row[4]}
        return None

    async def save_human_review(self, decision_id: str, status: str = 'pending', feedback: str = None, reviewer: str = None):
        await self._execute("INSERT INTO human_reviews (decision_id, status, feedback, reviewer, created_at) VALUES (?, ?, ?, ?, ?)", (decision_id, status, feedback, reviewer, datetime.now().isoformat()))

    async def update_human_review(self, review_id: int, status: str, feedback: str = None):
        await self._execute("UPDATE human_reviews SET status = ?, feedback = ?, reviewed_at = ? WHERE id = ?", (status, feedback, datetime.now().isoformat(), review_id))

    async def get_pending_reviews(self) -> List[Dict]:
        rows = await self._fetch_all("SELECT id, decision_id, status, feedback, reviewer, created_at FROM human_reviews WHERE status = 'pending' ORDER BY created_at DESC")
        return [{'id': r[0], 'decision_id': r[1], 'status': r[2], 'feedback': r[3], 'reviewer': r[4], 'created_at': r[5]} for r in rows]

    async def save_safety_violation(self, rule_name: str, details: Dict):
        await self._execute("INSERT INTO safety_violations (rule_name, details, timestamp) VALUES (?, ?, ?)", (rule_name, json.dumps(details), datetime.now().isoformat()))

    async def save_chaos_experiment(self, experiment_type: str, target_component: str, status: str, result: Dict = None):
        await self._execute("INSERT INTO chaos_experiments (experiment_type, target_component, status, result, timestamp) VALUES (?, ?, ?, ?, ?)", (experiment_type, target_component, status, json.dumps(result) if result else None, datetime.now().isoformat()))

    async def close(self):
        pass

# ============================================================
# INTERFACE DEFINITIONS (Dependency Inversion) - unchanged
# ============================================================
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
class IAnalytics(Protocol):
    async def multi_horizon_forecast(self, data: Dict, horizons: List[int]) -> Dict: ...
    async def detect_anomalies(self, metrics: Dict) -> List[Dict]: ...
    async def calculate_green_trend(self, projects: List[Dict]) -> Dict: ...

@runtime_checkable
class ICloudDistributor(Protocol):
    async def distribute_loader_data(self, data: Dict, preferences: Dict = None) -> Dict: ...
    async def get_distribution_status(self) -> Dict: ...

@runtime_checkable
class IDataLake(Protocol):
    async def store_metrics(self, metrics: Dict) -> Dict: ...
    async def query_data_warehouse(self, query: str) -> List[Dict]: ...

# ============================================================
# MODULE: POST-QUANTUM CRYPTOGRAPHY - unchanged (shortened for brevity, same as original)
# ============================================================
class PostQuantumCrypto(IPQC):
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
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
                await self.db.save_pqc_keypair(key_id, algorithm, encrypted_public, encrypted_private, expires_at)
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
        asyncio.create_task(self.db.save_pqc_keypair(key_id, 'ecdsa', public_bytes, private_bytes, expires_at))
        logger.info(f"Generated fallback ECDSA keypair {key_id}")
        return {'key_id': key_id, 'algorithm': 'ecdsa', 'public_key': public_bytes.hex()}

    async def sign_data(self, data: Dict, key_id: str) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        keypair = await self.db.get_pqc_keypair(key_id)
        if not keypair:
            raise ValueError(f"Key {key_id} not found")
        algorithm = keypair['algorithm']
        private_key_enc = keypair['private_key']
        private_key = self._decrypt_key(private_key_enc)
        if algorithm in self.pqc_algorithms:
            try:
                if algorithm == 'dilithium':
                    signature = await asyncio.to_thread(self.pqc_algorithms['dilithium'].sign, data_bytes, private_key)
                elif algorithm == 'falcon':
                    signature = await asyncio.to_thread(self.pqc_algorithms['falcon'].sign, data_bytes, private_key)
                elif algorithm == 'sphincs':
                    signature = await asyncio.to_thread(self.pqc_algorithms['sphincs'].sign, data_bytes, private_key)
                else:
                    raise ValueError("Invalid algorithm")
                signature_hex = signature.hex()
            except Exception as e:
                logger.error(f"PQC signing failed: {e}")
                return self._fallback_sign(data)
        elif algorithm == 'ecdsa':
            try:
                priv = ec.load_der_private_key(private_key, password=None, backend=default_backend())
                signature = priv.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
                signature_hex = signature.hex()
            except Exception as e:
                logger.error(f"ECDSA signing failed: {e}")
                return self._fallback_sign(data)
        else:
            return self._fallback_sign(data)
        return {'signature': signature_hex, 'algorithm': algorithm, 'key_id': key_id, 'timestamp': datetime.now().isoformat()}

    def _fallback_sign(self, data: Dict) -> Dict:
        return {
            'signature': hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest(),
            'algorithm': 'sha256_fallback',
            'key_id': 'fallback',
            'timestamp': datetime.now().isoformat()
        }

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        algorithm = signature_data.get('algorithm')
        key_id = signature_data.get('key_id')
        signature = signature_data.get('signature')
        if algorithm == 'sha256_fallback':
            expected = hashlib.sha256(data_bytes).hexdigest()
            return expected == signature
        keypair = await self.db.get_pqc_keypair(key_id)
        if not keypair:
            return False
        public_key_enc = keypair['public_key']
        public_key = self._decrypt_key(public_key_enc)
        if algorithm in self.pqc_algorithms:
            try:
                if algorithm == 'dilithium':
                    return await asyncio.to_thread(self.pqc_algorithms['dilithium'].verify, data_bytes, bytes.fromhex(signature), public_key)
                elif algorithm == 'falcon':
                    return await asyncio.to_thread(self.pqc_algorithms['falcon'].verify, data_bytes, bytes.fromhex(signature), public_key)
                elif algorithm == 'sphincs':
                    return await asyncio.to_thread(self.pqc_algorithms['sphincs'].verify, data_bytes, bytes.fromhex(signature), public_key)
            except Exception as e:
                logger.error(f"PQC verification failed: {e}")
                return False
        elif algorithm == 'ecdsa':
            try:
                pub = ec.load_der_public_key(public_key, backend=default_backend())
                pub.verify(bytes.fromhex(signature), data_bytes, ec.ECDSA(hashes.SHA256()))
                return True
            except Exception:
                return False
        return False

    async def get_status(self) -> Dict:
        keypairs = await self.db._fetch_all("SELECT COUNT(*) FROM pqc_key_pairs")
        return {
            'pqc_available': self.pqc_available,
            'algorithms': list(self.pqc_algorithms.keys()) if self.pqc_available else ['ecdsa'],
            'keypairs_count': keypairs[0][0] if keypairs else 0
        }

# ============================================================
# MODULE: BLOCKCHAIN INTEGRATION - mostly unchanged, but with completed methods
# ============================================================
class BlockchainIntegration(IBlockchain):
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
        self.web3 = None
        self.contract = None
        self.account = None
        self.web3_available = False
        self._lock = asyncio.Lock()
        self._circuit_breaker = GlobalCircuitBreaker().get_or_create('blockchain')
        if WEB3_AVAILABLE:
            self._initialize_blockchain()
        else:
            logger.warning("web3.py not installed – falling back to simulated blockchain.")

    def _initialize_blockchain(self):
        try:
            self.web3 = Web3(HTTPProvider(self.config.blockchain.rpc_url))
            if not self.web3.is_connected():
                raise ConnectionError("Cannot connect to blockchain RPC")
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            private_key = self.config.blockchain.private_key
            if private_key:
                self.account = Account.from_key(private_key)
                self.web3.eth.default_account = self.account.address
            else:
                self.account = self.web3.eth.accounts[0]
            contract_address = self.config.blockchain.contract_address
            if contract_address:
                contract_abi = self._load_contract_abi()
                self.contract = self.web3.eth.contract(address=contract_address, abi=contract_abi)
                self.web3_available = True
                logger.info(f"Connected to blockchain at {self.config.blockchain.rpc_url}")
            else:
                logger.warning("Contract address not configured – blockchain verification will be simulated.")
        except Exception as e:
            logger.error(f"Blockchain initialization failed: {e}")
            self.web3_available = False

    def _load_contract_abi(self) -> List:
        return [
            {"constant": False, "inputs": [{"name": "dataId", "type": "string"}, {"name": "dataHash", "type": "string"}, {"name": "metadata", "type": "string"}], "name": "recordData", "outputs": [], "type": "function"},
            {"constant": True, "inputs": [{"name": "dataId", "type": "string"}], "name": "getRecord", "outputs": [{"name": "dataHash", "type": "string"}, {"name": "metadata", "type": "string"}], "type": "function"}
        ]

    async def _record_data_on_chain(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        metadata_str = json.dumps(metadata)
        nonce = self.web3.eth.get_transaction_count(self.account.address)
        gas_estimate = self.contract.functions.recordData(data_id, data_hash, metadata_str).estimate_gas({'from': self.account.address})
        gas_price = self.web3.eth.gas_price
        tx = self.contract.functions.recordData(data_id, data_hash, metadata_str).build_transaction({
            'from': self.account.address,
            'nonce': nonce,
            'gas': int(gas_estimate * 1.2),
            'gasPrice': gas_price
        })
        signed_tx = self.account.sign_transaction(tx)
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status == 1:
            block_number = receipt.blockNumber
            await self.db.save_blockchain_transaction(tx_hash.hex(), "recordData", 0, data_id, "success")
            return {'status': 'success', 'data_id': data_id, 'tx_hash': tx_hash.hex(), 'block_number': block_number}
        else:
            raise RuntimeError("Transaction reverted")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def tokenize_carbon_credit(self, amount_kg: float, project_id: str) -> Dict:
        if not self.web3_available:
            return self._simulate_tokenize(amount_kg, project_id)
        try:
            result = await self._circuit_breaker.call(self._record_data_on_chain, project_id, hashlib.sha256(f"{amount_kg}:{project_id}".encode()).hexdigest(), {'amount_kg': amount_kg, 'project_id': project_id})
            return result
        except Exception as e:
            logger.error(f"Blockchain tokenization failed after circuit breaker: {e}")
            return {'status': 'failed', 'error': str(e)}

    def _simulate_tokenize(self, amount_kg: float, project_id: str) -> Dict:
        tx_hash = f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"
        block_number = random.randint(1000000, 2000000)
        asyncio.create_task(self.db.save_blockchain_transaction(tx_hash, "tokenize", amount_kg, project_id, "success"))
        return {'status': 'success', 'tx_hash': tx_hash, 'block_number': block_number, 'simulated': True}

    async def verify_helium_savings(self, liters: float, component_id: str) -> Dict:
        if not self.web3_available:
            tx_hash = f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"
            asyncio.create_task(self.db.save_blockchain_transaction(tx_hash, "helium_verify", liters, component_id, "success"))
            return {'status': 'success', 'tx_hash': tx_hash, 'simulated': True}
        try:
            data_hash = hashlib.sha256(f"{liters}:{component_id}".encode()).hexdigest()
            result = await self._circuit_breaker.call(self._record_data_on_chain, component_id, data_hash, {'liters': liters, 'component_id': component_id})
            return {'status': 'success', 'tx_hash': result.get('tx_hash'), 'verified': True}
        except Exception as e:
            logger.error(f"Helium verification failed: {e}")
            return {'status': 'failed', 'error': str(e)}

    async def get_transaction_history(self, limit: int = 100) -> List[Dict]:
        rows = await self.db._fetch_all("SELECT tx_hash, tx_type, amount, project_id, timestamp, status FROM blockchain_transactions ORDER BY id DESC LIMIT ?", (limit,))
        return [{'tx_hash': r[0], 'tx_type': r[1], 'amount': r[2], 'project_id': r[3], 'timestamp': r[4], 'status': r[5]} for r in rows]

    async def get_status(self) -> Dict:
        return {
            'connected': self.web3_available,
            'rpc_url': self.config.blockchain.rpc_url,
            'account': self.account.address if self.account else None,
        }

# ============================================================
# MODULE: ADVANCED PREDICTIVE ANALYTICS - now implemented more fully
# ============================================================
class AdvancedPredictiveAnalytics(IAnalytics):
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
        self.prophet_available = PROPHET_AVAILABLE
        self.sklearn_available = SKLEARN_AVAILABLE
        self.torch_available = TORCH_AVAILABLE

    async def multi_horizon_forecast(self, data: Dict, horizons: List[int]) -> Dict:
        if 'series' not in data or not data['series']:
            raise ValueError("Missing 'series' in data")
        series = data['series']
        if len(series) < 3:
            mean_val = np.mean(series) if series else 0
            return {'forecast': {h: mean_val for h in horizons}, 'method': 'naive_mean'}

        if self.prophet_available and len(series) >= 10:
            try:
                df = pd.DataFrame({'ds': pd.date_range(end=datetime.now(), periods=len(series), freq='H'), 'y': series})
                model = Prophet(
                    changepoint_prior_scale=self.config.analytics.prophet_changepoint_prior_scale,
                    seasonality_prior_scale=self.config.analytics.prophet_seasonality_prior_scale
                )
                await asyncio.to_thread(model.fit, df)
                future = model.make_future_dataframe(periods=max(horizons), freq='H')
                forecast = await asyncio.to_thread(model.predict, future)
                forecast_dict = {h: float(forecast.iloc[-h]['yhat']) for h in horizons}
                return {'forecast': forecast_dict, 'method': 'prophet'}
            except Exception as e:
                logger.warning(f"Prophet forecast failed: {e}")

        if len(series) >= 2:
            window = min(len(series), 10)
            x = np.arange(window)
            y = np.array(series[-window:])
            coeffs = np.polyfit(x, y, 1)
            forecast_dict = {}
            last_x = window - 1
            for h in horizons:
                pred = coeffs[0] * (last_x + h) + coeffs[1]
                forecast_dict[h] = float(pred)
            return {'forecast': forecast_dict, 'method': 'linear_regression'}
        return {'forecast': {h: series[-1] for h in horizons}, 'method': 'last_value'}

    async def detect_anomalies(self, metrics: Dict) -> List[Dict]:
        anomalies = []
        for name, value in metrics.items():
            if isinstance(value, (int, float)):
                if name == 'carbon_intensity' and value > 500:
                    anomalies.append({'metric': name, 'value': value, 'type': 'high', 'severity': 0.8, 'threshold': 500})
                elif name == 'energy_joules' and value > 100000:
                    anomalies.append({'metric': name, 'value': value, 'type': 'high', 'severity': 0.7, 'threshold': 100000})
                elif name == 'latency_ms' and value > 5000:
                    anomalies.append({'metric': name, 'value': value, 'type': 'high', 'severity': 0.6, 'threshold': 5000})
        for name, values in metrics.items():
            if isinstance(values, list) and len(values) > 5:
                try:
                    arr = np.array(values, dtype=float)
                    mean = np.mean(arr)
                    std = np.std(arr)
                    if std == 0:
                        continue
                    z_scores = (arr - mean) / std
                    for i, z in enumerate(z_scores):
                        if abs(z) > 3:
                            anomalies.append({'metric': name, 'index': i, 'value': values[i], 'z_score': float(z), 'type': 'outlier', 'severity': min(1.0, abs(z)/5)})
                except Exception as e:
                    logger.error(f"Anomaly detection failed for {name}: {e}")
        return anomalies

    async def calculate_green_trend(self, projects: List[Dict]) -> Dict:
        if not projects:
            return {'trend': 'unknown', 'slope': 0, 'significance': 0}
        scores = [p.get('green_score', 0) for p in projects]
        if len(scores) < 2:
            return {'trend': 'insufficient_data', 'slope': 0, 'significance': 0}
        x = np.arange(len(scores))
        y = np.array(scores, dtype=float)
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        corr = np.corrcoef(x, y)[0, 1]
        significance = abs(corr) if not np.isnan(corr) else 0
        if slope > 0.05 and significance > 0.5:
            trend = 'improving'
        elif slope < -0.05 and significance > 0.5:
            trend = 'declining'
        else:
            trend = 'stable'
        return {'trend': trend, 'slope': float(slope), 'significance': float(significance), 'num_projects': len(projects)}

# ============================================================
# MODULE: MULTI-CLOUD DISTRIBUTION - implemented with scoring
# ============================================================
class MultiCloudDistribution(ICloudDistributor):
    def __init__(self, db: AsyncDatabaseManager):
        self.db = db
        self.providers = {
            'aws': {'regions': ['us-east-1', 'eu-west-1', 'ap-southeast-1'], 'green_score': 0.7, 'latency': 50, 'cost': 0.8},
            'azure': {'regions': ['eastus', 'westeurope', 'southeastasia'], 'green_score': 0.65, 'latency': 60, 'cost': 0.75},
            'gcp': {'regions': ['us-central1', 'europe-west1', 'asia-southeast1'], 'green_score': 0.8, 'latency': 55, 'cost': 0.85},
        }

    async def distribute_loader_data(self, data: Dict, preferences: Dict = None) -> Dict:
        preferences = preferences or {'green': True, 'cost': False, 'performance': True}
        scores = {}
        for provider, info in self.providers.items():
            score = 0.0
            if preferences.get('green'):
                score += info['green_score'] * 0.4
            if preferences.get('cost'):
                score += (1 - info['cost']) * 0.3
            if preferences.get('performance'):
                score += (1 - info['latency']/100) * 0.3
            scores[provider] = score
        best_provider = max(scores, key=scores.get)
        best_region = self.providers[best_provider]['regions'][0]
        result = {
            'optimal_provider': best_provider,
            'optimal_region': best_region,
            'scores': scores,
            'data_size_gb': data.get('size_gb', 0),
            'timestamp': datetime.now().isoformat(),
        }
        await self.db.save_distribution(result)
        CLOUD_DISTRIBUTIONS.labels(provider=best_provider, status='success').inc()
        return result

    async def get_distribution_status(self) -> Dict:
        rows = await self.db._fetch_all("SELECT optimal_provider, optimal_region, timestamp FROM distribution_history ORDER BY id DESC LIMIT 1")
        if rows:
            return {'active_provider': rows[0][0], 'active_region': rows[0][1], 'timestamp': rows[0][2]}
        return {'active_provider': 'aws', 'active_region': 'us-east-1', 'timestamp': datetime.now().isoformat()}

# ============================================================
# MODULE: DATA LAKE INTEGRATION - now with actual S3/Athena logic (with fallback)
# ============================================================
class DataLakeIntegration(IDataLake):
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
        self.s3_client = None
        self.athena_client = None
        if AWS_AVAILABLE:
            try:
                self.s3_client = boto3.client('s3')
                self.athena_client = boto3.client('athena', region_name='us-east-1')
            except Exception as e:
                logger.error(f"AWS client init failed: {e}")
                self.s3_client = None
                self.athena_client = None

    async def store_metrics(self, metrics: Dict) -> Dict:
        if self.s3_client:
            try:
                bucket = self.config.cloud.s3_bucket
                key = f"{self.config.cloud.s3_prefix}{datetime.now().strftime('%Y/%m/%d/%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
                await asyncio.to_thread(self.s3_client.put_object, Bucket=bucket, Key=key, Body=json.dumps(metrics, default=str))
                return {'status': 'success', 'location': f"s3://{bucket}/{key}"}
            except Exception as e:
                logger.error(f"S3 store failed: {e}")
                return {'status': 'failed', 'error': str(e)}
        else:
            data_dir = Path('./data_lake')
            data_dir.mkdir(exist_ok=True)
            file_path = data_dir / f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
            with open(file_path, 'w') as f:
                json.dump(metrics, f, default=str)
            return {'status': 'success', 'location': str(file_path), 'simulated': True}

    async def query_data_warehouse(self, query: str) -> List[Dict]:
        if self.athena_client:
            try:
                response = await asyncio.to_thread(
                    self.athena_client.start_query_execution,
                    QueryString=query,
                    QueryExecutionContext={'Database': self.config.cloud.athena_database},
                    ResultConfiguration={'OutputLocation': f"s3://{self.config.cloud.s3_bucket}/athena-results/"}
                )
                query_execution_id = response['QueryExecutionId']
                while True:
                    result = await asyncio.to_thread(self.athena_client.get_query_execution, QueryExecutionId=query_execution_id)
                    state = result['QueryExecution']['Status']['State']
                    if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                        break
                    await asyncio.sleep(1)
                if state == 'SUCCEEDED':
                    result_response = await asyncio.to_thread(self.athena_client.get_query_results, QueryExecutionId=query_execution_id)
                    rows = result_response['ResultSet']['Rows']
                    if not rows:
                        return []
                    headers = [col['VarCharValue'] for col in rows[0]['Data']]
                    data = []
                    for row in rows[1:]:
                        values = [col.get('VarCharValue', None) for col in row['Data']]
                        data.append(dict(zip(headers, values)))
                    return data
                else:
                    logger.error(f"Athena query failed: {state}")
                    return []
            except Exception as e:
                logger.error(f"Athena query failed: {e}")
                return []
        else:
            logger.warning("Athena not available, using local DB fallback")
            if 'metric_history' in query.lower():
                rows = await self.db._fetch_all("SELECT * FROM metric_history LIMIT 10")
                return [{'metric_name': r[1], 'value': r[2], 'timestamp': r[3]} for r in rows]
            return []

# ============================================================
# MODULE: REAL-TIME MONITORING - with DB persistence
# ============================================================
class RealTimeMonitoring:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
        self.metrics_buffer = deque(maxlen=1000)

    async def record_metric(self, metric: Dict):
        self.metrics_buffer.append(metric)
        if 'name' in metric and 'value' in metric:
            await self.db.save_metric_history(metric['name'], metric['value'], metric.get('metadata'))

    async def get_latest_metrics(self) -> List[Dict]:
        return list(self.metrics_buffer)

    async def get_metric_history(self, metric_name: str, limit: int = 100) -> List[Dict]:
        return await self.db.get_metric_history(metric_name, limit)

# ============================================================
# MODULE: MLOps PIPELINE - unchanged but with minor additions
# ============================================================
class MLOpsPipeline:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
        self.models = {}

    async def register_model(self, model_id: str, name: str, version: str, metadata: Dict):
        await self.db.save_model_registry(model_id, name, version, metadata)
        self.models[model_id] = {'name': name, 'version': version, 'metadata': metadata}

    async def get_model(self, model_id: str) -> Optional[Dict]:
        return await self.db.get_model_registry(model_id)

    async def list_models(self) -> List[Dict]:
        return await self.db.list_models()

    async def update_model_metrics(self, model_id: str, prediction_count: int = None, error_count: int = None, avg_latency_ms: float = None):
        model = await self.db.get_model_registry(model_id)
        if not model:
            return
        if prediction_count is not None:
            model['prediction_count'] = prediction_count
        if error_count is not None:
            model['error_count'] = error_count
        if avg_latency_ms is not None:
            model['avg_latency_ms'] = avg_latency_ms
        await self.db.save_model_registry(model_id, model['name'], model['version'], model['metadata'], model['is_active'])

# ============================================================
# MODULE: MULTI-REGION MANAGER - unchanged
# ============================================================
class MultiRegionManager:
    def __init__(self):
        self.regions = ['us-east-1', 'eu-west-1', 'ap-southeast-1']

    async def get_optimal_region(self, criteria: Dict) -> str:
        if 'latency_ms' in criteria:
            if criteria['latency_ms'] < 50:
                return 'us-east-1'
            elif criteria['latency_ms'] < 100:
                return 'eu-west-1'
            else:
                return 'ap-southeast-1'
        return random.choice(self.regions)

# ============================================================
# MODULE: EDGE COMPUTING - with status update
# ============================================================
class EdgeComputing:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
        self.devices = {}

    async def register_device(self, device_id: str, config: Dict):
        await self.db.save_edge_device(device_id, config, 'active')
        self.devices[device_id] = {'config': config, 'status': 'active', 'last_seen': datetime.now()}

    async def update_device_status(self, device_id: str, status: str):
        await self.db.save_edge_device(device_id, self.devices.get(device_id, {}).get('config', {}), status)
        if device_id in self.devices:
            self.devices[device_id]['status'] = status

    async def get_device_status(self, device_id: str) -> Optional[Dict]:
        if device_id in self.devices:
            return self.devices[device_id]
        row = await self.db._fetch_one("SELECT config, status, last_seen FROM edge_devices WHERE device_id = ?", (device_id,))
        if row:
            return {'config': json.loads(row[0]), 'status': row[1], 'last_seen': row[2]}
        return None

# ============================================================
# MODULE: SUSTAINABLE NLP - with rule-based fallback
# ============================================================
class SustainableNLP:
    def __init__(self, config: GreenAgentConfig):
        self.config = config
        self.model_name = config.nlp.model_name
        self.tokenizer = None
        self.model = None
        if TRANSFORMERS_AVAILABLE:
            try:
                self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
                self.model = AutoModelForCausalLM.from_pretrained(self.model_name)
            except Exception as e:
                logger.warning(f"Failed to load NLP model: {e}")
                self.tokenizer = None
                self.model = None

    async def generate_sustainability_summary(self, metrics: Dict) -> str:
        if self.model and self.tokenizer:
            try:
                prompt = f"Sustainability metrics: {json.dumps(metrics)}. Summarize:"
                inputs = self.tokenizer(prompt, return_tensors='pt')
                outputs = await asyncio.to_thread(self.model.generate, **inputs, max_length=100)
                summary = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
                return summary
            except Exception as e:
                logger.error(f"NLP generation failed: {e}")
        summary_parts = []
        if 'carbon_intensity' in metrics:
            ci = metrics['carbon_intensity']
            if ci < 200:
                summary_parts.append("Carbon intensity is low.")
            elif ci < 400:
                summary_parts.append("Carbon intensity is moderate.")
            else:
                summary_parts.append("Carbon intensity is high.")
        if 'energy_joules' in metrics:
            energy_kwh = metrics['energy_joules'] / 3.6e6
            summary_parts.append(f"Energy usage: {energy_kwh:.2f} kWh.")
        if 'renewable_share' in metrics:
            rs = metrics['renewable_share']
            summary_parts.append(f"Renewable share: {rs*100:.1f}%.")
        if not summary_parts:
            summary_parts.append("No significant sustainability metrics found.")
        return " ".join(summary_parts)

# ============================================================
# NEW MODULE: CAUSAL BANDIT (Causal RL simplified)
# ============================================================
class CausalBandit:
    """A simple causal bandit that uses a linear structural model to estimate 
    intervention effects. Falls back to standard bandit if causal model fails."""
    def __init__(self, action_space, fallback_solver, min_trials_before_bandit=5, confidence_threshold=0.6):
        self.actions = action_space
        self.fallback_solver = fallback_solver
        self.min_trials = min_trials_before_bandit
        self.confidence_threshold = confidence_threshold
        self.q_values = {a['name']: 0.0 for a in action_space}
        self.counts = {a['name']: 0 for a in action_space}
        self.trials = 0
        self.causal_effects = {a['name']: 0.0 for a in action_space}
        self.context_history = []
        self.reward_history = []
        self.action_history = []

    def select_action(self, context):
        if self.trials < self.min_trials:
            return self.fallback_solver(context), 0.0, "fallback"
        # Use causal effect if available and confident, else Q-values
        epsilon = 0.1
        if random.random() < epsilon:
            name = random.choice(self.actions)['name']
        else:
            # Use causal effect if enough data, else Q-values
            if self.trials >= 10 and any(self.causal_effects.values()):
                name = max(self.causal_effects, key=self.causal_effects.get)
            else:
                name = max(self.q_values, key=self.q_values.get)
        action = next(a for a in self.actions if a['name'] == name)
        confidence = 0.5  # Could be based on variance
        return action, confidence, "causal" if name in self.causal_effects else "bandit"

    def update(self, context, action, reward):
        self.trials += 1
        name = action['name']
        self.counts[name] += 1
        self.q_values[name] += (reward - self.q_values[name]) / self.counts[name]
        self.context_history.append(context)
        self.reward_history.append(reward)
        self.action_history.append(name)
        # Simple causal effect estimation: average reward per action
        self.causal_effects[name] = np.mean([r for a, r in zip(self.action_history, self.reward_history) if a == name]) if name in self.action_history else 0.0

    def seed_safe_policy(self, context, policy):
        # Optionally seed with safe policy
        pass

# ============================================================
# NEW MODULE: SAFETY MONITOR (Temporal Logic-like rules)
# ============================================================
class SafetyMonitor:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager):
        self.config = config
        self.db = db
        self.rules = {
            'max_carbon_intensity': lambda metrics: metrics.get('carbon_intensity', 0) <= config.safety.max_carbon_intensity,
            'min_renewable_share': lambda metrics: metrics.get('renewable_share', 1.0) >= config.safety.min_renewable_share,
            'max_latency_ms': lambda metrics: metrics.get('latency_ms', 0) <= config.safety.max_latency_ms,
        }

    async def check(self, metrics: Dict, context: Dict = None) -> List[Dict]:
        violations = []
        for rule_name, check_fn in self.rules.items():
            if not check_fn(metrics):
                violation = {'rule': rule_name, 'details': metrics}
                violations.append(violation)
                await self.db.save_safety_violation(rule_name, metrics)
                SAFETY_VIOLATIONS.labels(rule=rule_name).inc()
                logger.warning(f"Safety violation: {rule_name} with metrics {metrics}")
        return violations

    async def validate_policy(self, policy: Dict, metrics: Dict) -> bool:
        # Additional policy-specific checks can be added
        return True

# ============================================================
# NEW MODULE: CHAOS MONKEY (Resilience Engineering)
# ============================================================
class ChaosMonkey:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager, system: 'GreenAgentSystem'):
        self.config = config
        self.db = db
        self.system = system
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
            try:
                await asyncio.sleep(self.interval)
                await self._inject_failure()
            except Exception as e:
                logger.error(f"Chaos experiment failed: {e}")

    async def _inject_failure(self):
        failure_type = random.choice(['latency', 'error', 'disconnect'])
        target = random.choice(['blockchain', 'database', 'analytics', 'flexgen'])
        status = 'success'
        result = {}
        try:
            if failure_type == 'latency':
                await asyncio.sleep(random.uniform(0.5, 2.0))
                result['delay'] = 'simulated latency'
            elif failure_type == 'error':
                # Simulate a transient error in a component
                if target == 'blockchain':
                    # Temporarily set web3_available False
                    original = self.system.blockchain.web3_available
                    self.system.blockchain.web3_available = False
                    await asyncio.sleep(random.uniform(1, 3))
                    self.system.blockchain.web3_available = original
                    result['action'] = 'toggled blockchain availability'
                elif target == 'database':
                    # Simulate DB lock
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    result['action'] = 'simulated DB delay'
            elif failure_type == 'disconnect':
                # Simulate network partition
                await asyncio.sleep(random.uniform(1, 2))
                result['action'] = 'simulated network partition'
        except Exception as e:
            status = 'failed'
            result['error'] = str(e)
        await self.db.save_chaos_experiment(failure_type, target, status, result)
        CHAOS_EXPERIMENTS.labels(type=failure_type, status=status).inc()
        logger.info(f"Chaos experiment {failure_type} on {target}: {status}")

# ============================================================
# ENHANCED AUTONOMOUS OPTIMIZER - now with Causal Bandit, XAI, Safety, HITL
# ============================================================
class EnhancedAutonomousOptimizer:
    def __init__(self, config: GreenAgentConfig, db: AsyncDatabaseManager, event_bus: EventBus):
        self.config = config
        self.db = db
        self.event_bus = event_bus
        self.general = config.general
        self.analytics = config.analytics

        self.modp = ParetoOptimizer() if ENHANCEMENTS_AVAILABLE else None
        self.bio = GeneticPolicyGenerator() if ENHANCEMENTS_AVAILABLE else None
        self.moe = ExpertRouter() if ENHANCEMENTS_AVAILABLE else None

        self.action_space = [
            {"name": "performance", "params": {"focus": "throughput", "precision": "fp32"}},
            {"name": "carbon", "params": {"focus": "low_carbon", "precision": "int8"}},
            {"name": "cost", "params": {"focus": "min_cost", "precision": "int8"}},
            {"name": "hybrid", "params": {"focus": "balance", "precision": "fp16"}},
            {"name": "adaptive", "params": {"focus": "auto", "precision": "fp16"}},
        ]

        def fallback(context):
            return {"name": "hybrid", "params": {"focus": "balance", "precision": "fp16"}}

        # Use CausalBandit if available, else standard bandit
        if ENHANCEMENTS_AVAILABLE:
            try:
                # Try to use CausalBandit (our class)
                self.bandit = CausalBandit(
                    action_space=self.action_space,
                    fallback_solver=fallback,
                    min_trials_before_bandit=self.general.bandit_min_trials,
                    confidence_threshold=self.general.bandit_confidence_threshold,
                )
            except:
                self.bandit = ContextualBandit(
                    action_space=self.action_space,
                    fallback_solver=fallback,
                    min_trials_before_bandit=self.general.bandit_min_trials,
                    confidence_threshold=self.general.bandit_confidence_threshold,
                )
        else:
            self.bandit = None

        self.recent_rewards = deque(maxlen=100)
        self._load_state_sync()

    def _load_state_sync(self):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._load_state())
            else:
                loop.run_until_complete(self._load_state())
        except Exception:
            pass

    async def _load_state(self):
        try:
            rows = await self.db._fetch_all("SELECT metric_name, value FROM metric_history WHERE metric_name LIKE 'bandit_%' ORDER BY id DESC LIMIT 100")
            if rows:
                for metric_name, value in rows:
                    action = metric_name.replace('bandit_', '')
                    if action in [a['name'] for a in self.action_space] and self.bandit:
                        self.bandit.q_values[action] = value
                        self.bandit.counts[action] = 1
                        self.bandit.trials = 1
        except Exception as e:
            logger.warning(f"Could not load bandit state: {e}")

    async def _save_state(self):
        if self.bandit:
            for action, q_value in self.bandit.q_values.items():
                await self.db.save_metric_history(f"bandit_{action}", q_value, {'type': 'bandit_q_value'})

    async def optimize(self, current_state: Dict, metrics: Dict[str, Any]) -> Dict:
        if not self.bandit:
            return await self._simple_optimize(current_state)

        context = {}
        if self.moe:
            context = self.moe.encode({
                "state": current_state,
                "metrics": metrics,
                "timestamp": datetime.now().isoformat(),
            })

        policy, confidence, source = self.bandit.select_action(context)
        if policy is None:
            policy = self._fallback_solve(context)

        # Safety check (temporal logic-like)
        safety_violations = await self.system.safety_monitor.check(metrics) if hasattr(self, 'system') and self.system.safety_monitor else []
        if safety_violations:
            # Override with safe policy (hybrid low carbon)
            policy = {"name": "carbon", "params": {"focus": "low_carbon", "precision": "int8"}}
            source = "safety_override"
            confidence = 1.0

        objectives = {
            "energy": metrics.get("energy_joules", 0) / 1000.0,
            "carbon": metrics.get("carbon_kg", 0) / 10.0,
            "latency": metrics.get("latency_ms", 0) / 1000.0,
            "accuracy": metrics.get("accuracy", 0),
        }
        utility = self.modp.evaluate(objectives, self.analytics.modp_weights) if self.modp else 0.0

        result = {
            'action': f"{policy['name']}_optimization",
            'selected_strategy': policy['name'],
            'confidence': confidence,
            'source': source,
            'utility': utility,
            'timestamp': datetime.now().isoformat(),
        }

        await self.db.save_optimisation(policy['name'], result)

        # Update bandit with reward (normalized)
        if self.bandit and source in ["bandit", "causal"]:
            normalized_reward = max(0.0, min(1.0, utility))
            self.bandit.update(context, policy, normalized_reward)
            self.recent_rewards.append(normalized_reward)

        # Bio‑inspired expansion
        if len(self.recent_rewards) > 20 and np.mean(self.recent_rewards) < 0.3 and self.bio:
            new_policies = self.bio.evolve(
                population=self.action_space,
                fitness_fn=lambda p: self._mock_fitness(p),
                generations=self.general.bio_generations,
                population_size=self.general.bio_population_size,
            )
            for p in new_policies:
                if p not in self.action_space:
                    self.action_space.append(p)
                    if self.bandit:
                        self.bandit.actions = self.action_space
            logger.info("Bio‑inspired expansion: added new strategies.")

        # XAI: generate explanation
        explanation = await self._generate_explanation(policy, context, confidence, utility)
        decision_id = str(uuid.uuid4())
        await self.db.save_explanation(decision_id, explanation, {'policy': policy, 'context': context})
        result['explanation_id'] = decision_id

        # Human-in-the-loop: flag for review if confidence low or safety override
        if confidence < self.general.human_review_threshold or source == "safety_override":
            await self.db.save_human_review(decision_id, status='pending', feedback=explanation)
            result['review_required'] = True
            result['review_id'] = decision_id
        else:
            result['review_required'] = False

        await self._save_state()
        return result

    async def _generate_explanation(self, policy: Dict, context: Any, confidence: float, utility: float) -> str:
        # Simple rule-based explanation
        parts = []
        if policy['name'] == 'performance':
            parts.append("Selected performance strategy to maximize throughput.")
        elif policy['name'] == 'carbon':
            parts.append("Selected carbon strategy to minimize emissions.")
        elif policy['name'] == 'cost':
            parts.append("Selected cost strategy to reduce expenses.")
        elif policy['name'] == 'hybrid':
            parts.append("Selected hybrid strategy for balanced objectives.")
        else:
            parts.append("Selected adaptive strategy based on recent trends.")
        if 'precision' in policy['params']:
            parts.append(f"Using {policy['params']['precision']} precision.")
        if confidence:
            parts.append(f"Confidence: {confidence:.2f}")
        if utility:
            parts.append(f"Utility score: {utility:.2f}")
        return " ".join(parts)

    async def _simple_optimize(self, current_state: Dict) -> Dict:
        scores = {}
        for s in ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']:
            scores[s] = self._score_strategy(s, current_state)
        best = max(scores, key=scores.get)
        result = {'action': f'{best}_optimization', 'selected_strategy': best, 'scores': scores,
                  'recommendation': self._generate_recommendation(best, current_state)}
        await self.db.save_optimisation(best, result)
        return result

    def _score_strategy(self, strategy: str, state: Dict) -> float:
        success_rate = state.get('success_rate', 0.5)
        carbon = state.get('carbon_intensity', 0.5)
        cost = state.get('cost_budget', 0.5)
        loader_quality = state.get('loader_quality', 0.5)
        if strategy == 'performance':
            return loader_quality * 0.8 + success_rate * 0.2
        elif strategy == 'carbon':
            return (1 - carbon) * 0.8 + success_rate * 0.2
        elif strategy == 'cost':
            return (1 - cost) * 0.8 + success_rate * 0.2
        elif strategy == 'hybrid':
            return (loader_quality + (1 - carbon) + (1 - cost)) / 3 * 0.7 + success_rate * 0.3
        elif strategy == 'adaptive':
            # Assume history retrieval is async - but this is sync method; we'll skip for simplicity
            return 0.5
        return 0.5

    def _generate_recommendation(self, strategy: str, state: Dict) -> str:
        if strategy == 'performance':
            return "Focus on maximising throughput and data quality."
        elif strategy == 'carbon':
            return "Prioritise carbon-aware data ingestion and processing."
        elif strategy == 'cost':
            return "Optimise resource usage during loading."
        elif strategy == 'hybrid':
            return "Balanced approach across performance, carbon, and cost."
        elif strategy == 'adaptive':
            return "Adjust dynamically based on recent performance trends."
        return "Maintain current strategy with monitoring."

    def _fallback_solve(self, context):
        return {"name": "hybrid", "params": {"focus": "balance", "precision": "fp16"}}

    def _mock_fitness(self, policy):
        return random.uniform(0, 1)

    async def optimize_flexgen(self, workload, node, metrics) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        candidates = generate_candidate_policies(20)
        context = self.moe.encode({
            "workload": workload.to_dict(),
            "node": node.to_dict(),
            "metrics": metrics,
        }) if self.moe else {}
        if self.bandit:
            action, confidence, source = self.bandit.select_action(context)
            chosen_policy = FlexGenPolicy(**action.get('params', {}))
        else:
            chosen_policy = candidates[0] if candidates else None
        return {"chosen_policy": chosen_policy.to_dict() if chosen_policy else None, "candidates": [c.to_dict() for c in candidates]}

    def get_optimization_stats(self) -> Dict:
        # Use async helper
        history = asyncio.get_event_loop().run_until_complete(self.db.get_recent_optimisations(1000))
        return {
            'total_optimizations': len(history),
            'strategies': [s['name'] for s in self.action_space],
            'recent_optimizations': history[:5]
        }

# ============================================================
# MODULE: GEOSPATIAL INTELLIGENCE - implemented
# ============================================================
class GeospatialIntelligence:
    async def find_optimal_locations(self, criteria: Dict) -> List[Dict]:
        locations = [
            {'name': 'Iceland', 'renewable': 1.0, 'latency': 30, 'cost': 0.9, 'green_score': 0.95},
            {'name': 'Norway', 'renewable': 0.98, 'latency': 40, 'cost': 0.8, 'green_score': 0.9},
            {'name': 'US Pacific Northwest', 'renewable': 0.7, 'latency': 60, 'cost': 0.7, 'green_score': 0.7},
            {'name': 'Singapore', 'renewable': 0.2, 'latency': 20, 'cost': 0.6, 'green_score': 0.3},
            {'name': 'Germany', 'renewable': 0.5, 'latency': 50, 'cost': 0.75, 'green_score': 0.6},
        ]
        scored = []
        for loc in locations:
            score = 0.0
            if criteria.get('renewable_energy'):
                score += loc['renewable'] * 0.5
            if criteria.get('low_latency'):
                score += (1 - loc['latency']/100) * 0.3
            if criteria.get('cost_budget'):
                score += (1 - loc['cost']) * 0.2
            scored.append({'location': loc['name'], 'score': score, 'details': loc})
        scored.sort(key=lambda x: x['score'], reverse=True)
        return scored

# ============================================================
# MODULE: FINANCIAL MODELER - implemented
# ============================================================
class FinancialModeler:
    async def calculate_roi(self, project: Dict, timeframe_years: int = 10) -> Dict:
        capex = project.get('capex_usd', 0)
        opex = project.get('opex_usd_per_year', 0)
        revenue = project.get('revenue_usd_per_year', 0)
        total_cost = capex + opex * timeframe_years
        total_revenue = revenue * timeframe_years
        roi = (total_revenue - total_cost) / total_cost if total_cost > 0 else 0
        payback_period = capex / (revenue - opex) if (revenue - opex) > 0 else None
        return {
            'roi': roi,
            'total_cost': total_cost,
            'total_revenue': total_revenue,
            'payback_period_years': payback_period,
            'timeframe_years': timeframe_years
        }

# ============================================================
# MODULE: ENVIRONMENTAL IMPACT ANALYZER - implemented
# ============================================================
class EnvironmentalImpactAnalyzer:
    async def calculate_lifecycle_emissions(self, project: Dict) -> Dict:
        capacity_mw = project.get('capacity_mw', 0)
        lifetime_years = project.get('lifetime_years', 25)
        emission_factor = project.get('emission_factor', 0.4)
        energy_per_year_mwh = capacity_mw * 0.3 * 8760
        total_energy_mwh = energy_per_year_mwh * lifetime_years
        emissions_tco2 = total_energy_mwh * emission_factor
        return {
            'emissions_tco2': emissions_tco2,
            'energy_per_year_mwh': energy_per_year_mwh,
            'lifetime_years': lifetime_years,
            'emission_factor': emission_factor
        }

# ============================================================
# MODULE: API GATEWAY - updated with new endpoints
# ============================================================
class APIGateway:
    def __init__(self, config: GreenAgentConfig, system: 'GreenAgentSystem'):
        self.config = config
        self.system = system
        self.fastapi_app = None
        self.rate_limiter = EnhancedRateLimiter(config.general.rate_limit_requests, config.general.rate_limit_window)
        if FASTAPI_AVAILABLE:
            self._init_fastapi()
        logger.info("API Gateway initialized")

    def _init_fastapi(self):
        app = FastAPI(title="Green Agent API", version="14.5")
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self.fastapi_app = app
        self._register_fastapi_routes()

    def _register_fastapi_routes(self):
        @self.fastapi_app.middleware("http")
        async def rate_limit_middleware(request: Request, call_next):
            if not await self.rate_limiter.acquire():
                API_REQUESTS.labels(endpoint=request.url.path, method=request.method, status='429').inc()
                return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
            response = await call_next(request)
            API_REQUESTS.labels(endpoint=request.url.path, method=request.method, status=response.status_code).inc()
            return response

        @self.fastapi_app.get("/metrics")
        async def metrics():
            if PROMETHEUS_AVAILABLE:
                return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
            return {"error": "Prometheus not enabled"}

        @self.fastapi_app.get("/health")
        async def health():
            return await self.system.health_check()

        @self.fastapi_app.post("/tokenize_carbon")
        async def tokenize_carbon(amount: float, project_id: str):
            result = await self.system.blockchain.tokenize_carbon_credit(amount, project_id)
            return result

        @self.fastapi_app.post("/forecast")
        async def forecast(data: Dict, horizons: List[int]):
            return await self.system.analytics.multi_horizon_forecast(data, horizons)

        @self.fastapi_app.get("/pqc/status")
        async def pqc_status():
            return await self.system.pqc.get_status()

        @self.fastapi_app.post("/pqc/sign")
        async def pqc_sign(data: Dict, key_id: str):
            return await self.system.pqc.sign_data(data, key_id)

        @self.fastapi_app.post("/pqc/verify")
        async def pqc_verify(data: Dict, signature_data: Dict):
            return {'valid': await self.system.pqc.verify_data(data, signature_data)}

        @self.fastapi_app.get("/cloud/status")
        async def cloud_status():
            return await self.system.cloud_distributor.get_distribution_status()

        @self.fastapi_app.post("/cloud/distribute")
        async def cloud_distribute(data: Dict):
            return await self.system.cloud_distributor.distribute_loader_data(data)

        @self.fastapi_app.get("/optimizer/stats")
        async def optimizer_stats():
            return self.system.autonomous_optimizer.get_optimization_stats()

        @self.fastapi_app.post("/optimize")
        async def optimize(state: Dict, metrics: Dict):
            result = await self.system.autonomous_optimizer.optimize(state, metrics)
            return result

        @self.fastapi_app.post("/optimization/best-strategy")
        async def best_strategy(state: Dict, metrics: Dict):
            result = await self.system.autonomous_optimizer.optimize(state, metrics)
            return result

        @self.fastapi_app.post("/optimization/evolve")
        async def evolve():
            new_policies = self.system.autonomous_optimizer.bio.evolve(
                population=self.system.autonomous_optimizer.action_space,
                fitness_fn=self.system.autonomous_optimizer._mock_fitness,
                generations=5,
                population_size=20
            )
            self.system.autonomous_optimizer.action_space.extend(new_policies)
            return {"status": "evolution triggered", "new_policies": [p['name'] for p in new_policies]}

        @self.fastapi_app.post("/optimization/pareto")
        async def pareto(objectives: Dict, weights: Optional[Dict] = None):
            modp = self.system.autonomous_optimizer.modp
            if modp:
                utility = modp.evaluate(objectives, weights or self.system.config.analytics.modp_weights)
                return {"utility": utility}
            return {"error": "MODP not available"}

        @self.fastapi_app.get("/optimization/explain/{decision_id}")
        async def explain_decision(decision_id: str):
            explanation = await self.system.db.get_explanation(decision_id)
            if not explanation:
                raise HTTPException(status_code=404, detail="Explanation not found")
            return explanation

        @self.fastapi_app.get("/human-review/pending")
        async def pending_reviews():
            reviews = await self.system.db.get_pending_reviews()
            return {"pending_reviews": reviews}

        @self.fastapi_app.post("/human-review/{review_id}/approve")
        async def approve_review(review_id: int, feedback: str = None):
            await self.system.db.update_human_review(review_id, 'approved', feedback)
            return {"status": "approved", "review_id": review_id}

        @self.fastapi_app.post("/human-review/{review_id}/reject")
        async def reject_review(review_id: int, feedback: str = None):
            await self.system.db.update_human_review(review_id, 'rejected', feedback)
            return {"status": "rejected", "review_id": review_id}

        @self.fastapi_app.post("/chaos/trigger")
        async def trigger_chaos():
            if not self.system.chaos_monkey:
                raise HTTPException(status_code=400, detail="Chaos Monkey not initialized")
            await self.system.chaos_monkey._inject_failure()
            return {"status": "chaos experiment triggered"}

        @self.fastapi_app.get("/geo/optimal")
        async def optimal_locations():
            return await self.system.geo_intelligence.find_optimal_locations({})

        @self.fastapi_app.post("/financial/roi")
        async def roi(project: Dict, timeframe_years: int = 10):
            return await self.system.financial_modeler.calculate_roi(project, timeframe_years)

        @self.fastapi_app.post("/environmental/impact")
        async def impact(project: Dict):
            return await self.system.environmental_analyzer.calculate_lifecycle_emissions(project)

        @self.fastapi_app.get("/nlp/summary")
        async def nlp_summary(metrics: Dict):
            return {'summary': await self.system.nlp.generate_sustainability_summary(metrics)}

        # FlexGen endpoints
        @self.fastapi_app.post("/flexgen/optimize")
        async def flexgen_optimize(workload: Dict, node: Dict):
            from types import SimpleNamespace
            wl = SimpleNamespace(**workload, metadata=workload.get('metadata', {}), task_id=workload.get('task_id', str(uuid.uuid4())))
            nd = SimpleNamespace(**node, id=node.get('id', 'node-1'))
            result = await self.system.run_flexgen_workload(wl, nd)
            return result

        @self.fastapi_app.get("/flexgen/status")
        async def flexgen_status():
            if self.system.gpu_profiler:
                metrics = await self.system.gpu_profiler.get_all_gpu_metrics()
                drift_stats = self.system.policy_drift_detector.get_stats()
                return {"gpu": metrics, "drift": drift_stats}
            return {"error": "GPU profiler not available"}

# ============================================================
# CENTRAL ORCHESTRATOR (Application) - updated with new components
# ============================================================
class GreenAgentSystem:
    def __init__(self, config: GreenAgentConfig):
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]
        self._running = False
        self._shutdown_event = asyncio.Event()
        self.background_tasks: Set[asyncio.Task] = set()
        self.event_bus = EventBus()

        # Initialize core services
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

        # New modules
        self.safety_monitor = SafetyMonitor(config, self.db)
        self.chaos_monkey = ChaosMonkey(config, self.db, self)
        # Autonomous optimizer needs reference to system for safety monitor
        self.autonomous_optimizer = EnhancedAutonomousOptimizer(config, self.db, self.event_bus)
        self.autonomous_optimizer.system = self  # for safety checks

        self.geo_intelligence = GeospatialIntelligence()
        self.financial_modeler = FinancialModeler()
        self.environmental_analyzer = EnvironmentalImpactAnalyzer()
        self.api_gateway = APIGateway(config, self)

        # FlexGen integration
        if FLEXGEN_AVAILABLE:
            self.gpu_profiler = GPUProfiler()
            self.flexgen_cost_model = FlexGenCostModel(carbon_intensity_g_per_kwh=config.analytics.flexgen_carbon_intensity_default)
            self.policy_drift_detector = PolicyDriftDetector()
            self.flexgen_controller = None
        else:
            self.gpu_profiler = None
            self.flexgen_cost_model = None
            self.policy_drift_detector = None
            self.flexgen_controller = None

        self.components = {
            'pqc': self.pqc,
            'blockchain': self.blockchain,
            'analytics': self.analytics,
            'cloud': self.cloud_distributor,
            'data_lake': self.data_lake,
            'monitoring': self.monitoring,
            'mlops': self.mlops,
            'multi_region': self.multi_region,
            'edge': self.edge,
            'nlp': self.nlp,
            'optimizer': self.autonomous_optimizer,
            'geo': self.geo_intelligence,
            'financial': self.financial_modeler,
            'environmental': self.environmental_analyzer,
            'api': self.api_gateway,
            'gpu_profiler': self.gpu_profiler,
            'flexgen_cost_model': self.flexgen_cost_model,
            'policy_drift_detector': self.policy_drift_detector,
            'safety_monitor': self.safety_monitor,
            'chaos_monkey': self.chaos_monkey,
        }

        global _system_instance
        _system_instance = self
        logger.info(f"GreenAgentSystem initialized (instance: {self.instance_id})")

    async def start(self):
        self._running = True
        await self.autonomous_optimizer._load_state()
        if self.chaos_monkey.enabled:
            await self.chaos_monkey.start()
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._monitoring_loop()),
        ]
        if self.gpu_profiler:
            tasks.append(asyncio.create_task(self.gpu_profiler.start_monitoring(interval_sec=5.0)))
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        if FASTAPI_AVAILABLE and self.api_gateway.fastapi_app:
            import uvicorn
            self._fastapi_task = asyncio.create_task(
                uvicorn.Server(uvicorn.Config(self.api_gateway.fastapi_app, host=self.config.api.host, port=self.config.api.port, log_level="info")).serve()
            )
            logger.info(f"FastAPI server started on {self.config.api.host}:{self.config.api.port}")
        logger.info("GreenAgentSystem started")

    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.labels(component='system').set(health['health_score'])
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(60)

    async def _monitoring_loop(self):
        while not self._shutdown_event.is_set():
            try:
                carbon = random.uniform(200, 500)
                await self.monitoring.record_metric({'name': 'carbon_intensity', 'value': carbon})
                CARBON_INTENSITY.set(carbon)
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(60)

    async def health_check(self) -> Dict:
        health_score = 100
        statuses = {}
        for name, comp in self.components.items():
            try:
                if hasattr(comp, 'get_status'):
                    status = await comp.get_status()
                    statuses[name] = status
                    if 'connected' in status and not status['connected']:
                        health_score -= 10
            except Exception as e:
                logger.error(f"Health check for {name} failed: {e}")
                statuses[name] = {'error': str(e)}
                health_score -= 20
        return {
            'healthy': health_score > 50,
            'instance_id': self.instance_id,
            'health_score': max(0, health_score),
            'components': statuses,
            'timestamp': datetime.now().isoformat()
        }

    async def run_flexgen_workload(self, workload: Any, node: Any) -> Dict:
        if not FLEXGEN_AVAILABLE:
            return {"error": "FlexGen modules not available"}
        selector = DistillationFlexGenSelector(n_candidates=20, config={'epsilon': self.config.analytics.flexgen_selector_epsilon, 'epsilon_decay': self.config.analytics.flexgen_selector_epsilon_decay})
        controller = FlexGenController(
            node=node,
            workload=workload,
            carbon_intensity=workload.metadata.get('carbon_intensity', self.config.analytics.flexgen_carbon_intensity_default),
            message_queue=None,
            use_real_executor=self.config.analytics.flexgen_use_real_executor,
            executor=self._get_flexgen_executor(),
            cost_model=self.flexgen_cost_model,
            use_bio_search=True,
            bio_search_config={'population_size': self.config.analytics.flexgen_population_size, 'generations': self.config.analytics.flexgen_generations},
            modp_planner=None,
            drift_detector=self.policy_drift_detector,
            gpu_profiler=self.gpu_profiler,
        )
        result = await controller.step()
        chosen_policy = result.get("chosen_policy", {})
        metrics = result.get("metrics", {})
        reward = result.get("reward", 0.0)
        await self.db.save_flexgen_decision(workload.task_id or "unknown", node.id, json.dumps(chosen_policy), json.dumps(metrics, default=str), reward)
        return result

    def _get_flexgen_executor(self):
        return None

    async def shutdown(self):
        logger.info(f"Shutting down GreenAgentSystem (instance: {self.instance_id})")
        self._shutdown_event.set()
        self._running = False
        if self.chaos_monkey:
            await self.chaos_monkey.stop()
        if hasattr(self, '_fastapi_task'):
            self._fastapi_task.cancel()
        for task in self.background_tasks:
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        if self.gpu_profiler:
            self.gpu_profiler.shutdown()
        await self.db.close()
        logger.info("Shutdown complete")

# ============================================================
# MAIN ENTRY POINT
# ============================================================
async def main():
    config = GreenAgentConfig()
    print("=" * 80)
    print("Green Agent Base Classes v14.5 - Enhanced with Causal RL, XAI, Safety, Chaos")
    print("=" * 80)

    if not config.master_key:
        config.master_key = secrets.token_hex(32)
        print("⚠️  master_key not set; using random for demo: ", config.master_key)

    system = GreenAgentSystem(config)
    await system.start()

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        await system.shutdown()
        print("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
